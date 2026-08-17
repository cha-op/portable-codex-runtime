import assert from "node:assert/strict";
import childProcess, { execFileSync } from "node:child_process";
import crypto from "node:crypto";
import {
  closeSync,
  constants as fsConstants,
  existsSync,
  openSync,
  readFileSync,
  writeFileSync,
} from "node:fs";
import {
  chmod,
  mkdir,
  mkdtemp,
  readFile,
  readdir,
  realpath,
  rm,
  symlink,
  writeFile,
} from "node:fs/promises";
import { syncBuiltinESMExports } from "node:module";
import { tmpdir } from "node:os";
import path, { join } from "node:path";
import test from "node:test";
import { setTimeout as delay } from "node:timers/promises";

import {
  PODMAN_WRITER_SUPERVISOR_CONTRACT_VERSION,
  PodmanWriterSupervisorError,
  createPodmanWriterSupervisor,
} from "../src/podman-writer-supervisor.mjs";
import {
  createPodmanWriterSupervisorState,
} from "../src/podman-writer-supervisor-state.mjs";

const DIGEST = `sha256:${"a".repeat(64)}`;
const CONFIG_DIGEST = `sha256:${"b".repeat(64)}`;
const CODEX_BINARY_SHA256 = "c".repeat(64);
const REQUEST_SHA256 = "d".repeat(64);
const CONTAINER_ID = "e".repeat(64);
const IMAGE_REFERENCE = `localhost/portable-codex@${DIGEST}`;
const SESSION_ID = "00000000-0000-4000-8000-000000000001";
const SUPERVISOR_ID = "podman-supervisor-001";
const HELD_MOUNT_SOURCE = "/proc/4242/fd/9";

function shellQuote(value) {
  return `'${value.replaceAll("'", `'"'"'`)}'`;
}

async function waitForPath(path) {
  for (let attempt = 0; attempt < 500; attempt += 1) {
    if (existsSync(path)) return;
    await delay(10);
  }
  assert.fail(`timed out waiting for ${path}`);
}

function processExists(pid) {
  try {
    process.kill(pid, 0);
    return true;
  } catch (error) {
    if (error?.code === "ENOENT") return false;
    if (error?.code === "ESRCH") return false;
    throw error;
  }
}

function closedStdioDescendantPodmanScript({
  descendantExitPath,
  descendantPidPath,
  descendantReadyPath,
  failureMode,
  sourceAttemptedPath,
  sourceMissingPath,
  sourceReleasedPath,
  sourceVisiblePath,
}) {
  const image = JSON.stringify([{
    Architecture: "amd64",
    Config: { User: "1000:1000" },
    Digest: DIGEST,
    Os: "linux",
  }]);
  const failureBody = failureMode === "nonzero"
    ? "exit 42"
    : failureMode === "stdout-overflow"
      ? `printf '%2048s' x\nexec /bin/sleep 30`
      : failureMode === "stderr-overflow"
        ? `printf '%2048s' x >&2\nexec /bin/sleep 30`
        : "exec /bin/sleep 30";
  const descendantAfterDirect = failureMode === "nonzero"
    ? `/bin/sleep 0.1
      if test -d "$source_path"; then
        : > ${shellQuote(sourceVisiblePath)}
      else
        : > ${shellQuote(sourceMissingPath)}
      fi
      : > ${shellQuote(sourceAttemptedPath)}
      while ! test -f ${shellQuote(descendantExitPath)}; do
        /bin/sleep 0.01
      done
      exit 0`
    : `while ! test -f ${shellQuote(sourceReleasedPath)}; do
        /bin/sleep 0.01
      done
      if test -d "$source_path"; then
        : > ${shellQuote(sourceVisiblePath)}
      else
        : > ${shellQuote(sourceMissingPath)}
      fi
      : > ${shellQuote(sourceAttemptedPath)}
      exec /bin/sleep 30`;
  return `#!/bin/sh
set -eu
test "$1" = "--remote=false"
shift
case "$1" in
  unshare)
    test "$2" = "/usr/bin/true"
    ;;
  image)
    printf '%s\\n' ${shellQuote(image)}
    ;;
  create)
    mount_spec=
    while [ "$#" -gt 0 ]; do
      if [ "$1" = "--mount" ]; then
        shift
        mount_spec=$1
        break
      fi
      shift
    done
    source_path=\${mount_spec#type=bind,source=}
    source_path=\${source_path%%,target=/session,*}
    test -d "$source_path"
    direct_pid=$$
    (
      : > ${shellQuote(descendantReadyPath)}
      while kill -0 "$direct_pid" 2>/dev/null; do
        /bin/sleep 0.01
      done
      ${descendantAfterDirect}
    ) </dev/null >/dev/null 2>/dev/null &
    descendant_pid=$!
    printf '%s\\n' "$descendant_pid" > ${shellQuote(descendantPidPath)}
    while ! test -f ${shellQuote(descendantReadyPath)}; do
      /bin/sleep 0.01
    done
    ${failureBody}
    ;;
  *)
    exit 64
    ;;
esac
`;
}

function detachedStartPodmanScript({
  containerId = CONTAINER_ID,
  holdingPath,
  namePath,
  releasePath,
  releasedPath,
}) {
  const image = JSON.stringify([{
    Architecture: "amd64",
    Config: { User: "1000:1000" },
    Digest: DIGEST,
    Os: "linux",
  }]);
  return `#!/bin/sh
set -eu
test "$1" = "--remote=false"
shift
case "$1" in
  unshare)
    test "$2" = "/usr/bin/true"
    ;;
  image)
    printf '%s\\n' ${shellQuote(image)}
    ;;
  create)
    shift
    while [ "$#" -gt 0 ]; do
      if [ "$1" = "--name" ]; then
        shift
        printf '%s\\n' "$1" > ${shellQuote(namePath)}
        break
      fi
      shift
    done
    printf '%s\\n' ${shellQuote(containerId)}
    ;;
  start)
    (
      : > ${shellQuote(holdingPath)}
      while ! test -f ${shellQuote(releasePath)}; do
        /bin/sleep 0.01
      done
      : > ${shellQuote(releasedPath)}
    ) &
    while ! test -f ${shellQuote(holdingPath)}; do
      /bin/sleep 0.01
    done
    printf '%2048s' x
    printf '%2048s' x >&2
    printf '%s\\n' ${shellQuote(containerId)}
    exit 0
    ;;
  container)
    name=$(cat ${shellQuote(namePath)})
    if test -f ${shellQuote(holdingPath)}; then
      pid=42001
      running=true
      status=running
    else
      pid=0
      running=false
      status=created
    fi
    printf '[{"Id":"%s","ImageDigest":"%s","Mounts":[{"Destination":"/session","Propagation":"rprivate","RW":true,"Source":"%s","Type":"bind"}],"Name":"%s","State":{"Pid":%s,"Running":%s,"Status":"%s"}}]\\n' \\
      ${shellQuote(containerId)} ${shellQuote(DIGEST)} \\
      ${shellQuote(HELD_MOUNT_SOURCE)} "$name" "$pid" "$running" "$status"
    ;;
  *)
    exit 64
    ;;
esac
`;
}

function blockingStartPodmanScript({
  namePath,
  pidPath,
  releasePath,
  startExitCode = 0,
  startedPath,
}) {
  const image = JSON.stringify([{
    Architecture: "amd64",
    Config: { User: "1000:1000" },
    Digest: DIGEST,
    Os: "linux",
  }]);
  return `#!/bin/sh
set -eu
test "$1" = "--remote=false"
shift
case "$1" in
  unshare)
    test "$2" = "/usr/bin/true"
    ;;
  image)
    printf '%s\\n' ${shellQuote(image)}
    ;;
  create)
    shift
    while [ "$#" -gt 0 ]; do
      if [ "$1" = "--name" ]; then
        shift
        printf '%s\\n' "$1" > ${shellQuote(namePath)}
        break
      fi
      shift
    done
    printf '%s\\n' ${shellQuote(CONTAINER_ID)}
    ;;
  container)
    name=$(cat ${shellQuote(namePath)})
    printf '[{"Id":"%s","ImageDigest":"%s","Mounts":[{"Destination":"/session","Propagation":"rprivate","RW":true,"Source":"%s","Type":"bind"}],"Name":"%s","State":{"Pid":0,"Running":false,"Status":"created"}}]\\n' \\
      ${shellQuote(CONTAINER_ID)} ${shellQuote(DIGEST)} \\
      ${shellQuote(HELD_MOUNT_SOURCE)} "$name"
    ;;
  start)
    printf '%s\\n' "$$" > ${shellQuote(pidPath)}
    : > ${shellQuote(startedPath)}
    while ! test -f ${shellQuote(releasePath)}; do
      /bin/sleep 0.01
    done
    exit ${startExitCode}
    ;;
  *)
    exit 64
    ;;
esac
`;
}

function defaultAuthorityPodmanScript({
  createMarker,
  createMutation = "",
  holderArgvPath = "/dev/null",
  mountPath,
  namePath,
  startedPath,
}) {
  const image = JSON.stringify([{
    Architecture: "amd64",
    Config: { User: "1000:1000" },
    Digest: DIGEST,
    Os: "linux",
  }]);
  return `#!/bin/sh
set -eu
test "$1" = "--remote=false"
shift
case "$1" in
  unshare)
    if test "$2" = "/usr/bin/true"; then
      exit 0
    fi
    printf '%s\\n' "$@" > ${shellQuote(holderArgvPath)}
    shift
    exec "$@"
    ;;
  image)
    printf '%s\\n' ${shellQuote(image)}
    ;;
  create)
    shift
    while test "$#" -gt 0; do
      case "$1" in
        --name)
          shift
          printf '%s\\n' "$1" > ${shellQuote(namePath)}
          ;;
        --mount)
          shift
          source_path=\${1#type=bind,source=}
          source_path=\${source_path%%,target=/session,*}
          test -d "$source_path"
          printf '%s\\n' "$source_path" > ${shellQuote(mountPath)}
          ${createMutation}
          ;;
      esac
      shift
    done
    : > ${shellQuote(createMarker)}
    printf '%s\\n' ${shellQuote(CONTAINER_ID)}
    ;;
  start)
    : > ${shellQuote(startedPath)}
    ;;
  container)
    name=$(cat ${shellQuote(namePath)})
    source_path=$(cat ${shellQuote(mountPath)})
    if test -f ${shellQuote(startedPath)}; then
      pid=$$
      running=true
      status=running
    else
      pid=0
      running=false
      status=created
    fi
    printf '[{"Id":"%s","ImageDigest":"%s","Mounts":[{"Destination":"/session","Propagation":"rprivate","RW":true,"Source":"%s","Type":"bind"}],"Name":"%s","State":{"Pid":%s,"Running":%s,"Status":"%s"}}]\\n' \\
      ${shellQuote(CONTAINER_ID)} ${shellQuote(DIGEST)} "$source_path" \\
      "$name" "$pid" "$running" "$status"
    ;;
  *)
    exit 64
    ;;
esac
`;
}

function failingHolderPodmanScript({
  mode,
  pidPath,
  rootlessProofReadyPath = null,
  rootlessProofReleasePath = null,
}) {
  const image = JSON.stringify([{
    Architecture: "amd64",
    Config: { User: "1000:1000" },
    Digest: DIGEST,
    Os: "linux",
  }]);
  const dev = mode === "malformed" ? "00" : "0";
  const holder = mode === "timeout"
    ? `printf '%s\\n' "$$" > ${shellQuote(pidPath)}\nexec /bin/sleep 30`
    : mode === "invalid-json"
      ? `printf '%s\\n' "$$" > ${shellQuote(pidPath)}
printf '{invalid-json\\n'
exec /bin/sleep 30`
    : `printf '%s\\n' "$$" > ${shellQuote(pidPath)}
printf '{"attachment":{"dev":"${dev}","fd":0,"ino":"0"},"configured":{"dev":"${dev}","fd":0,"ino":"0"},"contractVersion":1,"pid":%s,"status":"ready"}\\n' "$$"
exec /bin/sleep 30`;
  const rootlessProofBarrier =
    rootlessProofReadyPath === null || rootlessProofReleasePath === null
      ? ""
      : `: > ${shellQuote(rootlessProofReadyPath)}
while ! test -f ${shellQuote(rootlessProofReleasePath)}; do
  /bin/sleep 0.01
done`;
  return `#!/bin/sh
set -eu
test "$1" = "--remote=false"
shift
case "$1" in
  unshare)
    if test "$2" = "/usr/bin/true"; then
      ${rootlessProofBarrier}
      exit 0
    fi
    ${holder}
    ;;
  image)
    printf '%s\\n' ${shellQuote(image)}
    ;;
  *)
    exit 64
    ;;
esac
`;
}

function descendantHolderPodmanScript({
  attachmentRoot,
  authorityReleasedPath,
  configuredRoot,
  descendantExitPath,
  descendantPidPath,
  descendantReadyPath,
  leaderExitRequestedPath,
  mode,
  sourceAfterReleasePath,
  sourceRetainedPath,
}) {
  const image = JSON.stringify([{
    Architecture: "amd64",
    Config: { User: "1000:1000" },
    Digest: DIGEST,
    Os: "linux",
  }]);
  const markLeaderExitRequest = `: > ${shellQuote(leaderExitRequestedPath)}`;
  const afterReady = mode === "leader-error" || mode === "exit-before-close"
    ? `IFS= read -r command
test "$command" = verify
${markLeaderExitRequest}
exit 42`
    : `while IFS= read -r command; do
  case "$command" in
    verify)
      printf '%s\\n' verified
      ;;
    close)
      ${markLeaderExitRequest}
      exit 0
      ;;
    *)
      exit 65
      ;;
  esac
done
${markLeaderExitRequest}
exit 66`;
  const ready = mode === "timeout"
    ? "exec /bin/sleep 30"
    : `printf '{"attachment":{"dev":"%s","fd":7,"ino":"%s"},"configured":{"dev":"%s","fd":8,"ino":"%s"},"contractVersion":1,"pid":%s,"status":"ready"}\\n' \\
  "$attachment_dev" "$attachment_ino" "$configured_dev" "$configured_ino" "$$"
${afterReady}`;
  const descendantRedirection = mode === "exit-before-close"
    ? "</dev/null 2>/dev/null"
    : "</dev/null >/dev/null 2>/dev/null";
  return `#!/bin/sh
set -eu
test "$1" = "--remote=false"
shift
case "$1" in
  unshare)
    if test "$2" = "/usr/bin/true"; then
      exit 0
    fi
    IFS= read -r acquisition
    exec 7<${shellQuote(attachmentRoot)}
    exec 8<${shellQuote(configuredRoot)}
    attachment_dev=$(/usr/bin/stat -Lc %d /proc/self/fd/7)
    attachment_ino=$(/usr/bin/stat -Lc %i /proc/self/fd/7)
    configured_dev=$(/usr/bin/stat -Lc %d /proc/self/fd/8)
    configured_ino=$(/usr/bin/stat -Lc %i /proc/self/fd/8)
    (
      : > ${shellQuote(descendantReadyPath)}
      while ! test -f ${shellQuote(leaderExitRequestedPath)}; do
        /bin/sleep 0.01
      done
      if test -d /proc/self/fd/7; then
        : > ${shellQuote(sourceRetainedPath)}
      fi
      while :; do
        if test -f ${shellQuote(authorityReleasedPath)}; then
          if test -d /proc/self/fd/7; then
            : > ${shellQuote(sourceAfterReleasePath)}
          fi
          exit 0
        fi
        if test -f ${shellQuote(descendantExitPath)}; then
          exit 0
        fi
        /bin/sleep 0.01
      done
    ) ${descendantRedirection} &
    printf '%s\\n' "$!" > ${shellQuote(descendantPidPath)}
    while ! test -f ${shellQuote(descendantReadyPath)}; do
      /bin/sleep 0.01
    done
    ${ready}
    ;;
  image)
    printf '%s\\n' ${shellQuote(image)}
    ;;
  create)
    printf '%s\\n' short
    ;;
  *)
    exit 64
    ;;
esac
`;
}

function exact(value) {
  return Object.freeze(Object.assign(Object.create(null), value));
}

function measuredImage() {
  return exact({
    projection: exact({
      codexSandbox: "workspace-write",
      codexVersion: "1.2.3",
      platformImage: exact({
        architecture: "amd64",
        config: exact({
          digest: CONFIG_DIGEST,
          mediaType: "application/vnd.oci.image.config.v1+json",
          size: 1024,
        }),
        digest: DIGEST,
        mediaType: "application/vnd.oci.image.manifest.v1+json",
        os: "linux",
        size: 4096,
      }),
    }),
    runtimeIdentity: exact({
      codexBinaryPath: "/provider/runtime/codex",
      codexBinarySha256: CODEX_BINARY_SHA256,
      codexVersion: "1.2.3",
      platformImageDigest: DIGEST,
    }),
  });
}

function launchInput(attachmentRoot, options = {}) {
  const launchAttemptId = options.launchAttemptId ?? "launch-attempt-001";
  const signal = options.signal ?? new AbortController().signal;
  const launchMeasuredImage = options.measuredImage ?? measuredImage();
  const attachment = exact({
    attachmentId: `attachment-${launchAttemptId}`,
    backendId: "filesystem-backend",
    contractVersion: 1,
    fencingEpoch: "7",
    holderId: "writer-holder",
    kind: "directory",
    leaseId: "lease-001",
    mode: "read-write",
    operationId: "attachment-operation-001",
    proofId: "attachment-proof-001",
    rootPath: attachmentRoot,
    sessionId: SESSION_ID,
    storageId: "storage-001",
  });
  const lease = exact({
    contractVersion: 1,
    expiresAt: "2030-01-01T00:00:00.000Z",
    fencingEpoch: "7",
    holderId: "writer-holder",
    leaseId: "lease-001",
    sessionId: SESSION_ID,
  });
  const generationReference = exact({
    bindingSha256: "1".repeat(64),
    checkpointId: "checkpoint-001",
    claimedAt: "2026-08-14T09:00:00.000Z",
    committedAt: "2026-08-14T09:01:00.000Z",
    documentSha256: "2".repeat(64),
    generationId: "generation-001",
    operationId: "generation-operation-001",
    sessionId: SESSION_ID,
    state: "committed",
  });
  const request = exact({
    attachment,
    contractVersion: 1,
    fencingEpoch: "7",
    generation: generationReference,
    lease,
    measuredImage: launchMeasuredImage,
    supervisor: exact({ contractVersion: 1, supervisorId: SUPERVISOR_ID }),
  });
  const sessionDocument = exact({
    activeOperation: exact({ operationId: launchAttemptId }),
    attachment,
    backendCapabilities: exact({ exclusiveWriterAttachment: true }),
    documentVersion: 3,
    lastOperation: null,
    launch: null,
    lease,
    lifecycle: "ATTACHED",
    manifest: exact({
      runtime: exact({ imageDigest: DIGEST }),
    }),
    recovery: null,
    storageRef: exact({ storageId: "storage-001" }),
    writerEpoch: "7",
  });
  const session = exact({
    createdAt: "2026-08-14T08:00:00.000Z",
    document: sessionDocument,
    revision: "10",
    sessionId: SESSION_ID,
    updatedAt: "2026-08-14T10:00:00.000Z",
  });
  const attempt = exact({
    contractVersion: 1,
    launchAttemptId,
    request,
    result: null,
    state: "starting",
  });
  const operation = exact({
    conflictClass: "session-mutation",
    createdAt: "2026-08-14T09:59:00.000Z",
    expectedSession: session,
    kind: "writer-launch-attempt-v1",
    operationId: launchAttemptId,
    request,
    requestSha256: REQUEST_SHA256,
    result: null,
    retiredAt: null,
    revision: "1",
    sessionId: SESSION_ID,
    state: "starting",
    updatedAt: "2026-08-14T10:00:00.000Z",
  });
  const reservation = exact({
    conflictClass: "session-mutation",
    createdAt: "2026-08-14T09:59:00.000Z",
    expectedSessionRevision: "9",
    expiresAt: null,
    kind: "writer-launch-attempt-v1",
    operationId: launchAttemptId,
    releasedAt: null,
    requestSha256: REQUEST_SHA256,
    reservationId: `reservation-${launchAttemptId}`,
    sessionId: SESSION_ID,
    state: "starting",
    updatedAt: "2026-08-14T10:00:00.000Z",
  });
  const generation = exact({
    binding: exact({
      imageProviderPath: "/provider/image/authority",
      volumeDevicePath: "/dev/mapper/private-session",
    }),
    checkpointId: generationReference.checkpointId,
    claimedAt: generationReference.claimedAt,
    committedAt: generationReference.committedAt,
    document: exact({ status: "committed" }),
    generationId: generationReference.generationId,
    operationId: generationReference.operationId,
    sessionId: generationReference.sessionId,
    state: "committed",
  });
  return exact({
    attempt,
    authorityNow: "2026-08-14T10:00:01.000Z",
    consumedImage: request.measuredImage,
    contractVersion: PODMAN_WRITER_SUPERVISOR_CONTRACT_VERSION,
    generation,
    invocation: exact({}),
    operation,
    reservation,
    session,
    signal,
  });
}

function reconcileInput(input, signal = new AbortController().signal) {
  const attempt = exact({ ...input.attempt, state: "uncertain" });
  const operation = exact({
    ...input.operation,
    revision: "2",
    state: "uncertain",
  });
  const reservation = exact({ ...input.reservation, state: "uncertain" });
  return exact({
    attempt,
    contractVersion: PODMAN_WRITER_SUPERVISOR_CONTRACT_VERSION,
    invocation: exact({}),
    launch: null,
    operation,
    reservation,
    session: input.session,
    signal,
  });
}

function stopInput(input, receipt, options = {}) {
  return exact({
    attachment: input.attempt.request.attachment,
    contractVersion: PODMAN_WRITER_SUPERVISOR_CONTRACT_VERSION,
    invocation: exact({}),
    processIncarnationId: receipt.evidence.processIncarnationId,
    signal: options.signal ?? new AbortController().signal,
    stopOperationId: options.stopOperationId ?? "stop-operation-001",
    writerFence: exact({
      contractVersion: 1,
      fencingEpoch: input.attempt.request.lease.fencingEpoch,
      holderId: input.attempt.request.lease.holderId,
      leaseId: input.attempt.request.lease.leaseId,
      sessionId: input.attempt.request.lease.sessionId,
    }),
    writerIncarnationId: receipt.evidence.writerIncarnationId,
  });
}

function inspection(mountSource, running, status) {
  return {
    Id: CONTAINER_ID,
    ImageDigest: DIGEST,
    Mounts: [
      {
        Destination: "/session",
        Propagation: "rprivate",
        RW: true,
        Source: mountSource,
        Type: "bind",
      },
    ],
    Name: "__CONTAINER_NAME__",
    State: {
      Pid: running ? 42001 : 0,
      Running: running,
      Status: status ?? (running ? "running" : "exited"),
    },
  };
}

function successfulRunner(attachmentRoot, events, settings = {}) {
  let exists = false;
  let hasStarted = false;
  let running = false;
  let name = null;
  let mountSource = settings.inspectionSource ?? attachmentRoot;
  return async function commandRunner(executable, arguments_, options) {
    assert.equal(Object.isFrozen(arguments_), true);
    assert.equal(arguments_[0], "--remote=false");
    const rawArguments_ = arguments_;
    arguments_ = rawArguments_.slice(1);
    events.push({
      arguments_,
      executable,
      options,
      rawArguments_,
      receiver: this,
    });
    if (arguments_[0] === "unshare") {
      assert.deepEqual(arguments_, ["unshare", "/usr/bin/true"]);
      await settings.onUnshare?.();
      if (settings.unshareReject) {
        throw new Error("simulated rootless proof failure");
      }
      return { stderr: "", stdout: "" };
    }
    if (arguments_[0] === "image") {
      return {
        stderr: "",
        stdout: `${JSON.stringify(settings.imageInspection ?? [
          {
            Architecture: "amd64",
            Config: { User: settings.imageUser ?? "1000:1000" },
            Digest: settings.inspectedDigest ?? DIGEST,
            Os: "linux",
          },
        ])}\n`,
      };
    }
    if (arguments_[0] === "create") {
      name = arguments_[arguments_.indexOf("--name") + 1];
      const mount = arguments_[arguments_.indexOf("--mount") + 1];
      const sourcePrefix = "type=bind,source=";
      mountSource = mount.slice(
        sourcePrefix.length,
        mount.indexOf(",target=/session"),
      );
      await settings.onCreate?.();
      if (settings.createReject) {
        exists = settings.createExistsOnReject ?? false;
        throw new Error("simulated ambiguous create");
      }
      exists = true;
      hasStarted = false;
      if (settings.createOutput !== undefined) {
        return { stderr: "", stdout: settings.createOutput };
      }
      return { stderr: "", stdout: `${CONTAINER_ID}\n` };
    }
    if (arguments_[0] === "start") {
      if (settings.startReject) throw new Error("simulated ambiguous start");
      hasStarted = true;
      running = true;
      return { stderr: "", stdout: `${CONTAINER_ID}\n` };
    }
    if (arguments_[0] === "stop") {
      running = false;
      return { stderr: "", stdout: `${CONTAINER_ID}\n` };
    }
    if (arguments_[0] === "wait") {
      return { stderr: "", stdout: "0\n" };
    }
    if (arguments_[0] === "rm") {
      await settings.onRm?.();
      exists = false;
      hasStarted = false;
      running = false;
      if (settings.rmRejectAfterRemove) {
        settings.rmRejectAfterRemove = false;
        throw new Error("simulated ambiguous rm");
      }
      return {
        stderr: "",
        stdout: settings.rmOutput ?? `${CONTAINER_ID}\n`,
      };
    }
    if (arguments_[0] === "ps") {
      const filter = arguments_[arguments_.indexOf("--filter") + 1];
      const filteredName = filter.startsWith("name=^")
        ? filter.slice("name=^".length, -1)
        : name;
      name ??= filteredName;
      const entries = typeof settings.psEntries === "function"
        ? settings.psEntries(filteredName)
        : settings.psEntries ?? (exists
          ? [{
            Id: CONTAINER_ID,
            Names: [name],
            State: running ? "running" : "exited",
          }]
          : []);
      return {
        stderr: "",
        stdout: `${JSON.stringify(entries)}\n`,
      };
    }
    if (arguments_[0] === "container" && arguments_[1] === "inspect") {
      const value = inspection(
        mountSource,
        settings.forceRunning
          ? true
          : settings.forceStopped
            ? false
            : running,
        settings.inspectionStatus ??
          (exists && !running && !hasStarted ? "created" : undefined),
      );
      value.Name = name ?? settings.containerName;
      settings.mutateInspection?.(value);
      return { stderr: "", stdout: `${JSON.stringify([value])}\n` };
    }
    throw new Error(`unexpected command: ${arguments_.join(" ")}`);
  };
}

function successfulFilesystemAuthority(
  configuredAttachmentRoot,
  attachmentRoot,
  events,
  settings = {},
) {
  let nextHandle = 0;
  const openHandles = new Set();
  return exact({
    contractVersion: 1,
    async acquire(input) {
      assert.equal(input.configuredAttachmentRoot, configuredAttachmentRoot);
      assert.equal(input.attachment.rootPath, attachmentRoot);
      assert.equal(input.attachment.proofId, "attachment-proof-001");
      assert.equal(input.attachment.storageId, "storage-001");
      events.push({ input, operation: "acquire", receiver: this });
      settings.onAcquire?.(input);
      if (settings.acquireError !== undefined) throw settings.acquireError;
      const handle = exact({
        attachmentId: input.attachment.attachmentId,
        id: nextHandle += 1,
        proofId: input.attachment.proofId,
        storageId: input.attachment.storageId,
      });
      openHandles.add(handle);
      return exact({
        handle,
        mountSource: typeof settings.mountSource === "function"
          ? settings.mountSource(input)
          : settings.mountSource ?? HELD_MOUNT_SOURCE,
      });
    },
    async verifyCurrent(input) {
      assert.equal(openHandles.has(input.handle), true);
      assert.equal(input.configuredAttachmentRoot, configuredAttachmentRoot);
      assert.equal(input.attachment.rootPath, attachmentRoot);
      assert.equal(input.attachment.proofId, "attachment-proof-001");
      assert.equal(input.attachment.attachmentId, input.handle.attachmentId);
      assert.equal(input.attachment.proofId, input.handle.proofId);
      assert.equal(input.attachment.storageId, input.handle.storageId);
      events.push({ input, operation: "verify-current", receiver: this });
      settings.onVerifyCurrent?.(input);
      return settings.currentResult ?? true;
    },
    async verifyRunningMount(input) {
      assert.equal(openHandles.has(input.handle), true);
      assert.equal(input.configuredAttachmentRoot, configuredAttachmentRoot);
      assert.equal(input.attachment.rootPath, attachmentRoot);
      assert.equal(input.attachment.proofId, "attachment-proof-001");
      assert.equal(input.attachment.attachmentId, input.handle.attachmentId);
      assert.equal(input.attachment.proofId, input.handle.proofId);
      assert.equal(input.attachment.storageId, input.handle.storageId);
      events.push({ input, operation: "verify-running-mount", receiver: this });
      settings.onVerifyRunningMount?.(input);
      return settings.runningMountResult ?? true;
    },
    async close(input) {
      assert.equal(openHandles.delete(input.handle), true);
      events.push({ input, operation: "close", receiver: this });
      settings.onClose?.(input);
      return true;
    },
  });
}

async function fixture(t, settings = {}) {
  const parent = await mkdtemp(
    join(await realpath(tmpdir()), "podman-writer-supervisor-test-"),
  );
  const attachmentRoot = join(parent, "attachment");
  const stateRoot = join(parent, "state");
  await mkdir(attachmentRoot, { mode: 0o700 });
  t.after(() => rm(parent, { force: true, recursive: true }));
  const events = [];
  const filesystemEvents = [];
  const state = createPodmanWriterSupervisorState(exact({ root: stateRoot }));
  let podmanExecutable = settings.podmanExecutable ?? "/usr/bin/podman";
  if (settings.defaultCommandRunnerScript !== undefined) {
    podmanExecutable = join(parent, "podman-fixture");
    await writeFile(
      podmanExecutable,
      settings.defaultCommandRunnerScript({ attachmentRoot, parent }),
      { mode: 0o700 },
    );
    await chmod(podmanExecutable, 0o700);
  }
  const optionValues = {
    commandTimeoutMilliseconds: settings.commandTimeoutMilliseconds ?? 10_000,
    configuredAttachmentRoot: parent,
    images: exact({
      [DIGEST]: exact({
        architecture: "amd64",
        codexVersion: "1.2.3",
        imageReference: IMAGE_REFERENCE,
        os: "linux",
      }),
    }),
    maxOutputBytes: settings.maxOutputBytes ?? 64 * 1024,
    podmanEnvironment: exact({
      HOME: "/var/empty/podman",
      XDG_RUNTIME_DIR: "/run/user/1000",
    }),
    podmanExecutable,
    state,
    stopTimeoutSeconds: settings.stopTimeoutSeconds ?? 7,
    supervisorId: SUPERVISOR_ID,
    writerCommand: settings.writerCommand ??
      Object.freeze(["/usr/local/bin/codex", "app-server"]),
    writerEnvironment: exact({
      CODEX_HOME: "/session/.codex",
      LANG: "C.UTF-8",
    }),
  };
  if (settings.useDefaultCommandRunner !== true) {
    optionValues.commandRunner = settings.commandRunner ??
      successfulRunner(attachmentRoot, events, settings);
  }
  if (settings.useDefaultFilesystemAuthority !== true) {
    optionValues.filesystemAuthority = settings.filesystemAuthority ??
      successfulFilesystemAuthority(
        parent,
        attachmentRoot,
        filesystemEvents,
        settings.filesystemSettings,
      );
  }
  const options = exact(optionValues);
  return {
    attachmentRoot,
    events,
    filesystemEvents,
    input: launchInput(attachmentRoot),
    options,
    parent,
    state,
    stateRoot,
    supervisor: createPodmanWriterSupervisor(options),
  };
}

function assertSupervisorError(code) {
  return (error) =>
    error instanceof PodmanWriterSupervisorError && error.code === code;
}

async function preparingFixture(t) {
  const base = await fixture(t, { createReject: true });
  await assert.rejects(
    base.supervisor.launchWriter(base.input),
    assertSupervisorError("podman_writer_supervisor_outcome_uncertain"),
  );
  const durable = await base.state.read(
    exact({ launchAttemptId: "launch-attempt-001" }),
  );
  assert.equal(durable.status, "preparing");
  const name = base.events.find((event) => event.arguments_[0] === "create")
    .arguments_[2];
  return { ...base, name };
}

test("launches with the fixed rootless digest-pinned argv and joins the container on stop", async (t) => {
  const base = await fixture(t);
  const { supervisor } = base;
  assert.throws(
    () => createPodmanWriterSupervisor(exact({
      ...base.options,
      filesystemAuthority: undefined,
    })),
    assertSupervisorError("invalid_podman_writer_supervisor_options"),
  );
  assert.throws(
    () => createPodmanWriterSupervisor(exact({
      ...base.options,
      podmanEnvironment: exact({
        ...base.options.podmanEnvironment,
        PATH: "/tmp/untrusted-podman-tools",
      }),
    })),
    assertSupervisorError("invalid_podman_writer_supervisor_options"),
  );
  assert.equal(Object.getPrototypeOf(supervisor), null);
  assert.equal(Object.isFrozen(supervisor), true);
  assert.deepEqual(Reflect.ownKeys(supervisor), [
    "contractVersion",
    "supervisorId",
    "launchWriter",
    "reconcileWriterLaunch",
  ]);
  assert.equal(supervisor.contractVersion, 2);
  assert.equal(supervisor.launchWriter.length, 1);
  assert.equal(supervisor.reconcileWriterLaunch.length, 1);

  const pending = supervisor.launchWriter(base.input);
  assert.equal(Object.getPrototypeOf(pending), Promise.prototype);
  const receipt = await pending;
  assert.equal(receipt.evidence.status, "started");
  assert.equal(receipt.stopWriter.length, 1);
  assert.equal(Object.isFrozen(receipt.stopWriter), true);
  const stopRequest = stopInput(base.input, receipt);
  const stopPending = receipt.stopWriter(stopRequest);
  assert.equal(Object.getPrototypeOf(stopPending), Promise.prototype);
  const stopped = await stopPending;
  assert.deepEqual(stopped, exact({ contractVersion: 2, status: "stopped" }));

  assert.deepEqual(base.events[0].arguments_, ["unshare", "/usr/bin/true"]);
  assert.equal(
    base.events.some((event) => event.arguments_[0] === "info"),
    false,
  );
  for (const event of base.events) {
    assert.equal(event.rawArguments_[0], "--remote=false");
    assert.equal(event.rawArguments_.includes("--connection"), false);
    assert.equal(event.rawArguments_.includes("--context"), false);
    assert.equal(event.rawArguments_.includes("--url"), false);
  }
  const create = base.events.find((event) => event.arguments_[0] === "create");
  assert.equal(create.executable, "/usr/bin/podman");
  assert.equal(create.receiver, undefined);
  assert.deepEqual(create.options.environment, exact({
    HOME: "/var/empty/podman",
    PATH: "/usr/bin:/bin",
    XDG_RUNTIME_DIR: "/run/user/1000",
  }));
  assert.equal(Object.isFrozen(create.options.environment), true);
  assert.deepEqual(create.arguments_, [
    "create",
    "--name",
    create.arguments_[2],
    "--pull=never",
    "--image-volume=ignore",
    "--log-driver=none",
    "--read-only",
    "--read-only-tmpfs=false",
    "--security-opt=no-new-privileges",
    "--cap-drop=all",
    "--userns=keep-id:uid=1000,gid=1000",
    "--restart=no",
    "--mount",
    `type=bind,source=${HELD_MOUNT_SOURCE},target=/session,rw,bind-propagation=rprivate`,
    "--workdir",
    "/session",
    "--entrypoint",
    "/usr/local/bin/codex",
    "--env",
    "CODEX_HOME=/session/.codex",
    "--env",
    "LANG=C.UTF-8",
    IMAGE_REFERENCE,
    "app-server",
  ]);
  assert.deepEqual(
    base.events.slice(-6)
      .map((event) => event.arguments_),
    [
      ["stop", "--ignore", "--time", "7", CONTAINER_ID],
      ["wait", "--condition=stopped", CONTAINER_ID],
      ["container", "inspect", "--format=json", CONTAINER_ID],
      ["rm", "--ignore", CONTAINER_ID],
      [
        "ps",
        "-a",
        "--no-trunc",
        "--filter",
        `name=^${create.arguments_[2]}$`,
        "--format=json",
      ],
      [
        "ps",
        "-a",
        "--no-trunc",
        "--filter",
        `id=${CONTAINER_ID}`,
        "--format=json",
      ],
    ],
  );
  const stopIndex = base.events.findIndex((event) => event.arguments_[0] === "stop");
  for (let index = 0; index < base.events.length; index += 1) {
    const event = base.events[index];
    assert.equal(
      event.options.signal,
      index >= stopIndex ? stopRequest.signal : base.input.signal,
    );
    assert.equal(Object.getPrototypeOf(event.options), null);
    assert.equal(Object.isFrozen(event.options), true);
  }
});

test("a single writer executable fully overrides the image command", async (t) => {
  const writerCommand = Object.freeze(["/usr/local/bin/writer"]);
  const base = await fixture(t, { writerCommand });
  const receipt = await base.supervisor.launchWriter(base.input);
  const create = base.events.find((event) => event.arguments_[0] === "create");
  const entrypointIndex = create.arguments_.indexOf("--entrypoint");
  const imageIndex = create.arguments_.indexOf(IMAGE_REFERENCE);
  assert.deepEqual(
    create.arguments_.slice(entrypointIndex, entrypointIndex + 2),
    ["--entrypoint", writerCommand[0]],
  );
  assert.deepEqual(create.arguments_.slice(imageIndex), [IMAGE_REFERENCE]);
  await receipt.stopWriter(stopInput(base.input, receipt));
});

test("writer arguments preserve lossless UTF-8 at the code-unit boundary", async (t) => {
  const base = await fixture(t);
  const boundaryArgument = `${"x".repeat(4_094)}\u{1f680}`;
  assert.equal(boundaryArgument.length, 4_096);
  assert.equal(Buffer.byteLength(boundaryArgument, "utf8"), 4_098);

  const supervisor = createPodmanWriterSupervisor(exact({
    ...base.options,
    writerCommand: Object.freeze([
      "/usr/local/bin/codex",
      boundaryArgument,
    ]),
  }));
  const receipt = await supervisor.launchWriter(base.input);
  const create = base.events.find((event) => event.arguments_[0] === "create");
  assert.equal(create.arguments_.at(-1), boundaryArgument);
  await receipt.stopWriter(stopInput(base.input, receipt));

  for (const invalidArgument of [
    `${"x".repeat(4_095)}\ud800`,
    `${"x".repeat(4_095)}\udc00`,
    "x".repeat(4_097),
  ]) {
    assert.throws(
      () => createPodmanWriterSupervisor(exact({
        ...base.options,
        writerCommand: Object.freeze([
          "/usr/local/bin/codex",
          invalidArgument,
        ]),
      })),
      assertSupervisorError("invalid_podman_writer_supervisor_options"),
    );
  }
});

test("environment values preserve lossless UTF-8 at the code-unit boundary", async (t) => {
  const base = await fixture(t);
  const boundaryValue = `${"x".repeat(4_094)}\u{1f680}`;
  assert.equal(boundaryValue.length, 4_096);
  assert.equal(Buffer.byteLength(boundaryValue, "utf8"), 4_098);

  for (const environmentName of [
    "writerEnvironment",
    "podmanEnvironment",
  ]) {
    for (const invalidValue of [
      `${"x".repeat(4_095)}\ud800`,
      `${"x".repeat(4_095)}\udc00`,
      "x".repeat(4_097),
    ]) {
      assert.throws(
        () => createPodmanWriterSupervisor(exact({
          ...base.options,
          [environmentName]: exact({
            ...base.options[environmentName],
            LANG: invalidValue,
          }),
        })),
        assertSupervisorError("invalid_podman_writer_supervisor_options"),
      );
    }
  }
  assert.equal(base.events.length, 0);
  assert.equal(base.filesystemEvents.length, 0);
  assert.equal(existsSync(base.stateRoot), false);

  const supervisor = createPodmanWriterSupervisor(exact({
    ...base.options,
    podmanEnvironment: exact({
      ...base.options.podmanEnvironment,
      LANG: boundaryValue,
    }),
    writerEnvironment: exact({
      ...base.options.writerEnvironment,
      LANG: boundaryValue,
    }),
  }));
  const receipt = await supervisor.launchWriter(base.input);
  const create = base.events.find((event) => event.arguments_[0] === "create");
  assert.equal(create.options.environment.LANG, boundaryValue);
  assert.equal(
    create.arguments_[create.arguments_.indexOf(`LANG=${boundaryValue}`)],
    `LANG=${boundaryValue}`,
  );
  await receipt.stopWriter(stopInput(base.input, receipt));
});

test("exact replay and supervisor reconstruction never launch a second container", async (t) => {
  const base = await fixture(t);
  const first = await base.supervisor.launchWriter(base.input);
  const count = base.events.length;
  const replay = await base.supervisor.launchWriter(base.input);
  assert.deepEqual(replay.evidence, first.evidence);
  assert.equal(base.events.length, count);

  const reconstructed = createPodmanWriterSupervisor(base.options);
  const recovered = await reconstructed.launchWriter(base.input);
  assert.deepEqual(recovered.evidence, first.evidence);
  assert.equal(base.events.length, count);

  const changedRoot = join(base.parent, "other-attachment");
  await mkdir(changedRoot, { mode: 0o700 });
  await assert.rejects(
    base.supervisor.launchWriter(launchInput(changedRoot)),
    assertSupervisorError("podman_writer_state_conflict"),
  );
  assert.equal(base.events.length, count);
});

test("stop is exact-owner idempotent and persists complete-stopped reconciliation", async (t) => {
  const base = await fixture(t);
  const launch = await base.supervisor.launchWriter(base.input);
  const request = stopInput(base.input, launch);
  await launch.stopWriter(request);
  const count = base.events.length;
  assert.deepEqual(await launch.stopWriter(request), exact({ contractVersion: 2, status: "stopped" }));
  assert.deepEqual(
    base.events.slice(count).map((event) => event.arguments_[0]),
    ["rm", "ps", "ps"],
  );
  const afterReplay = base.events.length;
  await assert.rejects(
    launch.stopWriter(stopInput(base.input, launch, { stopOperationId: "stop-operation-002" })),
    assertSupervisorError("podman_writer_state_conflict"),
  );
  const reconciled = await base.supervisor.reconcileWriterLaunch(
    reconcileInput(base.input),
  );
  assert.equal(reconciled.evidence.status, "complete-stopped");
  assert.deepEqual(
    base.events.slice(afterReplay).map((event) => event.arguments_[0]),
    [],
  );
});

test("durable stopped cleanup retries after rm acknowledgement loss on cold replay", async (t) => {
  let state;
  const base = await fixture(t, {
    async onRm() {
      const record = await state.read(
        exact({ launchAttemptId: "launch-attempt-001" }),
      );
      assert.equal(record.status, "stopped");
    },
    rmRejectAfterRemove: true,
  });
  state = base.state;
  const launch = await base.supervisor.launchWriter(base.input);
  await assert.rejects(
    launch.stopWriter(stopInput(base.input, launch)),
    assertSupervisorError("podman_writer_supervisor_outcome_uncertain"),
  );
  const durable = await base.state.read(
    exact({ launchAttemptId: "launch-attempt-001" }),
  );
  assert.equal(durable.status, "stopped");
  const replayStart = base.events.length;
  const restarted = createPodmanWriterSupervisor(base.options);
  const replay = await restarted.launchWriter(base.input);
  assert.equal(replay.evidence.status, "complete-stopped");
  assert.equal(replay.stopWriter, null);
  assert.deepEqual(
    base.events.slice(replayStart).map((event) => event.arguments_[0]),
    ["rm", "ps", "ps"],
  );
});

test("successive launch attempts retire every durable stopped container", async (t) => {
  const base = await fixture(t);
  for (let index = 1; index <= 3; index += 1) {
    const input = launchInput(base.attachmentRoot, {
      launchAttemptId: `launch-attempt-${index}`,
    });
    const launch = await base.supervisor.launchWriter(input);
    await launch.stopWriter(stopInput(input, launch, {
      stopOperationId: `stop-operation-${index}`,
    }));
  }
  assert.equal(
    base.events.filter((event) => event.arguments_[0] === "create").length,
    3,
  );
  assert.equal(
    base.events.filter((event) => event.arguments_[0] === "rm").length,
    3,
  );
  const absent = launchInput(base.attachmentRoot, {
    launchAttemptId: "launch-attempt-4",
  });
  const reconciled = await base.supervisor.reconcileWriterLaunch(
    reconcileInput(absent),
  );
  assert.equal(reconciled.evidence.status, "not-started");
});

test("reconcile is a repeatable stopped-only observation", async (t) => {
  const missing = await fixture(t);
  const other = launchInput(missing.attachmentRoot, {
    launchAttemptId: "launch-attempt-missing",
  });
  const observed = await missing.supervisor.reconcileWriterLaunch(reconcileInput(other));
  assert.equal(observed.evidence.status, "not-started");
  assert.equal(observed.evidence.processIncarnationId, null);
  assert.deepEqual(missing.events.map((event) => event.arguments_[0]), ["ps"]);

  const running = await fixture(t);
  await running.supervisor.launchWriter(running.input);
  await assert.rejects(
    running.supervisor.reconcileWriterLaunch(reconcileInput(running.input)),
    assertSupervisorError("podman_writer_supervisor_outcome_uncertain"),
  );

  const observedExitEvents = [];
  const runningName = running.events.find((event) => event.arguments_[0] === "create")
    .arguments_[2];
  const observedExit = createPodmanWriterSupervisor(exact({
    ...running.options,
    commandRunner: successfulRunner(running.attachmentRoot, observedExitEvents, {
      containerName: runningName,
      forceStopped: true,
    }),
  }));
  const observedStopped = await observedExit.reconcileWriterLaunch(
    reconcileInput(running.input),
  );
  assert.equal(observedStopped.evidence.status, "complete-stopped");
  assert.equal(
    (await running.state.read(exact({ launchAttemptId: "launch-attempt-001" })))
      .status,
    "started",
  );
  assert.deepEqual(
    observedExitEvents.map((event) => event.arguments_[0]),
    ["container"],
  );

  const crashed = await fixture(t, { startReject: true });
  await assert.rejects(
    crashed.supervisor.launchWriter(crashed.input),
    assertSupervisorError("podman_writer_supervisor_outcome_uncertain"),
  );
  const recoveryEvents = [];
  const recoveryOptions = exact({
    ...crashed.options,
    commandRunner: successfulRunner(crashed.attachmentRoot, recoveryEvents, {
      containerName: crashed.events.find((event) => event.arguments_[0] === "create")
        .arguments_[2],
      forceStopped: true,
    }),
  });
  const recovery = createPodmanWriterSupervisor(recoveryOptions);
  const recovered = await recovery.reconcileWriterLaunch(reconcileInput(crashed.input));
  assert.equal(recovered.evidence.status, "complete-stopped");
  const durable = await crashed.state.read(exact({ launchAttemptId: "launch-attempt-001" }));
  assert.equal(durable.status, "created");
  assert.deepEqual(
    recoveryEvents.map((event) => event.arguments_[0]),
    ["container"],
  );
});

test("missing-state reconciliation probes the exact name and fails closed on live or conflicting evidence", async (t) => {
  const live = await fixture(t, {
    forceRunning: true,
    psEntries: (name) => [{ Id: CONTAINER_ID, Names: [name], State: "running" }],
  });
  await assert.rejects(
    live.supervisor.reconcileWriterLaunch(reconcileInput(live.input)),
    assertSupervisorError("podman_writer_supervisor_outcome_uncertain"),
  );
  assert.deepEqual(
    live.events.map((event) => event.arguments_[0]),
    ["ps", "container", "container"],
  );
  assert.equal(
    live.events.some((event) => ["create", "rm"].includes(event.arguments_[0])),
    false,
  );
  assert.deepEqual(
    live.filesystemEvents.map((event) => event.operation),
    ["acquire", "verify-running-mount", "close"],
  );
  assert.equal(
    await live.state.read(exact({ launchAttemptId: "launch-attempt-001" })),
    null,
  );

  const created = await fixture(t, {
    forceStopped: true,
    inspectionStatus: "created",
    psEntries: (name) => [{ Id: CONTAINER_ID, Names: [name], State: "created" }],
  });
  await assert.rejects(
    created.supervisor.reconcileWriterLaunch(reconcileInput(created.input)),
    assertSupervisorError("podman_writer_supervisor_outcome_uncertain"),
  );
  assert.deepEqual(created.events.map((event) => event.arguments_[0]), ["ps", "container"]);
  assert.equal(created.filesystemEvents.length, 0);

  const conflicting = await fixture(t, {
    forceStopped: true,
    psEntries: (name) => [{
      Id: "f".repeat(64),
      Names: [name],
      State: "exited",
    }],
  });
  await assert.rejects(
    conflicting.supervisor.reconcileWriterLaunch(reconcileInput(conflicting.input)),
    assertSupervisorError("podman_writer_supervisor_outcome_uncertain"),
  );
  assert.equal(
    await conflicting.state.read(exact({ launchAttemptId: "launch-attempt-001" })),
    null,
  );

  const malformed = await fixture(t, { psEntries: exact({}) });
  await assert.rejects(
    malformed.supervisor.reconcileWriterLaunch(reconcileInput(malformed.input)),
    assertSupervisorError("podman_writer_supervisor_outcome_uncertain"),
  );
});

test("missing-state stopped evidence replays identically without premature retirement", async (t) => {
  const stopped = await fixture(t, {
    forceStopped: true,
    psEntries: (name) => [{ Id: CONTAINER_ID, Names: [name], State: "exited" }],
  });
  const first = await stopped.supervisor.reconcileWriterLaunch(
    reconcileInput(stopped.input),
  );
  const replay = await stopped.supervisor.reconcileWriterLaunch(
    reconcileInput(stopped.input),
  );
  assert.equal(first.evidence.status, "complete-stopped");
  assert.deepEqual(replay.evidence, first.evidence);
  assert.deepEqual(
    stopped.events.map((event) => event.arguments_[0]),
    ["ps", "container", "ps", "container"],
  );
  assert.equal(
    stopped.events.some((event) => ["create", "rm"].includes(event.arguments_[0])),
    false,
  );
  assert.equal(
    await stopped.state.read(exact({ launchAttemptId: "launch-attempt-001" })),
    null,
  );
});

test("preparing reconciliation proves absence by exact read-only enumeration", async (t) => {
  const base = await preparingFixture(t);
  const events = [];
  const supervisor = createPodmanWriterSupervisor(exact({
    ...base.options,
    commandRunner: successfulRunner(base.attachmentRoot, events, {
      psEntries: [],
    }),
  }));
  const receipt = await supervisor.reconcileWriterLaunch(
    reconcileInput(base.input),
  );
  assert.equal(receipt.evidence.status, "not-started");
  assert.deepEqual(events.map((event) => event.arguments_[0]), ["ps"]);
  assert.deepEqual(events[0].arguments_, [
    "ps",
    "-a",
    "--no-trunc",
    "--filter",
    `name=^${base.name}$`,
    "--format=json",
  ]);
  assert.equal(
    (await base.state.read(exact({ launchAttemptId: "launch-attempt-001" })))
      .status,
    "preparing",
  );
});

test("preparing reconciliation observes stopped state without mutation", async (t) => {
  const base = await preparingFixture(t);
  const events = [];
  const supervisor = createPodmanWriterSupervisor(exact({
    ...base.options,
    commandRunner: successfulRunner(base.attachmentRoot, events, {
      containerName: base.name,
      forceStopped: true,
      psEntries: [{ Id: CONTAINER_ID, Names: [base.name], State: "exited" }],
    }),
  }));
  const first = await supervisor.reconcileWriterLaunch(reconcileInput(base.input));
  const replay = await supervisor.reconcileWriterLaunch(reconcileInput(base.input));
  assert.equal(first.evidence.status, "complete-stopped");
  assert.deepEqual(replay.evidence, first.evidence);
  assert.deepEqual(
    events.map((event) => event.arguments_[0]),
    ["ps", "container", "ps", "container"],
  );
  assert.equal(
    events.some((event) => event.arguments_[0] === "rm"),
    false,
  );
  assert.equal(
    (await base.state.read(exact({ launchAttemptId: "launch-attempt-001" })))
      .status,
    "preparing",
  );

  const configured = await preparingFixture(t);
  const configuredEvents = [];
  const configuredSupervisor = createPodmanWriterSupervisor(exact({
    ...configured.options,
    commandRunner: successfulRunner(
      configured.attachmentRoot,
      configuredEvents,
      {
        containerName: configured.name,
        forceStopped: true,
        inspectionStatus: "created",
        psEntries: [{
          Id: CONTAINER_ID,
          Names: [configured.name],
          State: "created",
        }],
      },
    ),
  }));
  await assert.rejects(
    configuredSupervisor.reconcileWriterLaunch(reconcileInput(configured.input)),
    assertSupervisorError("podman_writer_supervisor_outcome_uncertain"),
  );
  assert.deepEqual(
    configuredEvents.map((event) => event.arguments_[0]),
    ["ps", "container"],
  );
  assert.equal(
    (await configured.state.read(
      exact({ launchAttemptId: "launch-attempt-001" }),
    )).status,
    "preparing",
  );
});
test("preparing reconciliation rejects running, duplicate, or mismatched candidates", async (t) => {
  const running = await preparingFixture(t);
  const runningEvents = [];
  const runningSupervisor = createPodmanWriterSupervisor(exact({
    ...running.options,
    commandRunner: successfulRunner(running.attachmentRoot, runningEvents, {
      containerName: running.name,
      forceRunning: true,
      psEntries: [{ Id: CONTAINER_ID, Names: [running.name], State: "running" }],
    }),
  }));
  await assert.rejects(
    runningSupervisor.reconcileWriterLaunch(reconcileInput(running.input)),
    assertSupervisorError("podman_writer_supervisor_outcome_uncertain"),
  );
  assert.deepEqual(
    runningEvents.map((event) => event.arguments_[0]),
    ["ps", "container", "container"],
  );

  const raced = await preparingFixture(t);
  const racedEvents = [];
  const racedSupervisor = createPodmanWriterSupervisor(exact({
    ...raced.options,
    commandRunner: successfulRunner(raced.attachmentRoot, racedEvents, {
      containerName: raced.name,
      forceRunning: true,
      psEntries: [{ Id: CONTAINER_ID, Names: [raced.name], State: "exited" }],
    }),
  }));
  await assert.rejects(
    racedSupervisor.reconcileWriterLaunch(reconcileInput(raced.input)),
    assertSupervisorError("podman_writer_supervisor_outcome_uncertain"),
  );
  assert.deepEqual(
    racedEvents.map((event) => event.arguments_[0]),
    ["ps", "container", "container"],
  );

  const duplicate = await preparingFixture(t);
  const duplicateEvents = [];
  const duplicateSupervisor = createPodmanWriterSupervisor(exact({
    ...duplicate.options,
    commandRunner: successfulRunner(duplicate.attachmentRoot, duplicateEvents, {
      psEntries: [
        { Id: CONTAINER_ID, Names: [duplicate.name], State: "exited" },
        { Id: "f".repeat(64), Names: [duplicate.name], State: "exited" },
      ],
    }),
  }));
  await assert.rejects(
    duplicateSupervisor.reconcileWriterLaunch(reconcileInput(duplicate.input)),
    assertSupervisorError("podman_writer_supervisor_outcome_uncertain"),
  );
  assert.deepEqual(duplicateEvents.map((event) => event.arguments_[0]), ["ps"]);

  const mismatched = await preparingFixture(t);
  const mismatchedSupervisor = createPodmanWriterSupervisor(exact({
    ...mismatched.options,
    commandRunner: successfulRunner(mismatched.attachmentRoot, [], {
      psEntries: [{ Id: CONTAINER_ID, Names: ["foreign"], State: "exited" }],
    }),
  }));
  await assert.rejects(
    mismatchedSupervisor.reconcileWriterLaunch(reconcileInput(mismatched.input)),
    assertSupervisorError("podman_writer_supervisor_outcome_uncertain"),
  );
});

test("held attachment authority survives pathname ABA and benign child churn until live proof", async (t) => {
  let attachmentRoot;
  let pathnameObject = "original";
  let childChurnObserved = false;
  const base = await fixture(t, {
    async onCreate() {
      pathnameObject = "replacement";
      pathnameObject = "original";
      await writeFile(join(attachmentRoot, "child-entry"), "changed\n", {
        mode: 0o600,
      });
      childChurnObserved = true;
    },
    filesystemSettings: {
      onVerifyRunningMount() {
        assert.equal(pathnameObject, "original");
        assert.equal(childChurnObserved, true);
      },
    },
  });
  attachmentRoot = base.attachmentRoot;
  const receipt = await base.supervisor.launchWriter(base.input);
  assert.equal(receipt.evidence.status, "started");
  assert.deepEqual(
    base.filesystemEvents.map((event) => event.operation),
    [
      "acquire",
      "verify-current",
      "verify-current",
      "verify-running-mount",
      "close",
    ],
  );
  for (const event of base.filesystemEvents) {
    assert.equal(event.receiver, undefined);
    assert.equal(Object.getPrototypeOf(event.input), null);
    assert.equal(Object.isFrozen(event.input), true);
  }
  assert.deepEqual(
    Reflect.ownKeys(base.filesystemEvents[0].input),
    ["attachment", "configuredAttachmentRoot"],
  );
  assert.equal(
    base.filesystemEvents[0].input.attachment.attachmentId,
    "attachment-launch-attempt-001",
  );
  assert.equal(
    base.filesystemEvents[0].input.attachment.proofId,
    "attachment-proof-001",
  );
  assert.equal(
    base.filesystemEvents[0].input.attachment.storageId,
    "storage-001",
  );
  const create = base.events.find((event) => event.arguments_[0] === "create");
  assert.equal(create.arguments_.join("\n").includes(attachmentRoot), false);
  assert.equal(create.arguments_.join("\n").includes(HELD_MOUNT_SOURCE), true);
});

test("attachment replacement, access-policy change, and unreadable revalidation fail distinctly", async (t) => {
  const replaced = await fixture(t, {
    filesystemSettings: { currentResult: false },
  });
  await assert.rejects(
    replaced.supervisor.launchWriter(replaced.input),
    assertSupervisorError("podman_writer_attachment_mismatch"),
  );
  assert.equal(
    replaced.events.some((event) => event.arguments_[0] === "create"),
    false,
  );
  assert.deepEqual(
    replaced.filesystemEvents.map((event) => event.operation),
    ["acquire", "verify-current", "close"],
  );

  const policyChanged = await fixture(t, {
    filesystemSettings: { runningMountResult: false },
  });
  await assert.rejects(
    policyChanged.supervisor.launchWriter(policyChanged.input),
    assertSupervisorError("podman_writer_attachment_mismatch"),
  );
  assert.deepEqual(
    policyChanged.filesystemEvents.map((event) => event.operation),
    [
      "acquire",
      "verify-current",
      "verify-current",
      "verify-running-mount",
      "close",
    ],
  );

  const unreadableError = new Error("simulated EACCES");
  unreadableError.code = "EACCES";
  const unreadable = await fixture(t, {
    filesystemSettings: { acquireError: unreadableError },
  });
  await assert.rejects(
    unreadable.supervisor.launchWriter(unreadable.input),
    assertSupervisorError("podman_writer_attachment_revalidation_failed"),
  );
  assert.equal(unreadable.filesystemEvents.at(-1).operation, "acquire");
});

test("native pathname ingress accepts 4095 UTF-8 bytes and rejects 4096", async (t) => {
  const base = await fixture(t);
  const maximumPath = `/${"é".repeat(2_047)}`;
  const oversizedPath = `${maximumPath}a`;
  assert.equal(Buffer.byteLength(maximumPath, "utf8"), 4_095);
  assert.equal(Buffer.byteLength(oversizedPath, "utf8"), 4_096);

  assert.doesNotThrow(() => createPodmanWriterSupervisor(exact({
    ...base.options,
    configuredAttachmentRoot: maximumPath,
  })));
  assert.doesNotThrow(() => createPodmanWriterSupervisor(exact({
    ...base.options,
    podmanExecutable: maximumPath,
  })));
  assert.doesNotThrow(() => createPodmanWriterSupervisor(exact({
    ...base.options,
    writerCommand: Object.freeze([maximumPath]),
  })));
  assert.throws(
    () => createPodmanWriterSupervisor(exact({
      ...base.options,
      configuredAttachmentRoot: oversizedPath,
    })),
    assertSupervisorError("invalid_podman_writer_supervisor_options"),
  );
  assert.throws(
    () => createPodmanWriterSupervisor(exact({
      ...base.options,
      podmanExecutable: oversizedPath,
    })),
    assertSupervisorError("invalid_podman_writer_supervisor_options"),
  );
  for (const writerExecutable of [
    oversizedPath,
    `/${"x".repeat(4_095)}`,
    "/usr/local/../bin/writer",
    "/usr/local/bin/\ud800",
  ]) {
    assert.throws(
      () => createPodmanWriterSupervisor(exact({
        ...base.options,
        writerCommand: Object.freeze([writerExecutable]),
      })),
      assertSupervisorError("invalid_podman_writer_supervisor_options"),
    );
  }
  assert.equal(base.events.length, 0);
  assert.equal(base.filesystemEvents.length, 0);
  assert.equal(existsSync(base.stateRoot), false);

  const prefix = `${base.parent}/`;
  const maximumRequestedRoot =
    `${prefix}${"x".repeat(4_095 - Buffer.byteLength(prefix, "utf8"))}`;
  const oversizedRequestedRoot = `${maximumRequestedRoot}x`;
  assert.equal(Buffer.byteLength(maximumRequestedRoot, "utf8"), 4_095);
  assert.equal(Buffer.byteLength(oversizedRequestedRoot, "utf8"), 4_096);
  const acceptedFilesystemEvents = [];
  const acceptedRequestSupervisor = createPodmanWriterSupervisor(exact({
    ...base.options,
    filesystemAuthority: successfulFilesystemAuthority(
      base.parent,
      maximumRequestedRoot,
      acceptedFilesystemEvents,
      { acquireError: new Error("stop after pathname validation") },
    ),
  }));
  await assert.rejects(
    acceptedRequestSupervisor.launchWriter(launchInput(maximumRequestedRoot)),
    assertSupervisorError("podman_writer_attachment_revalidation_failed"),
  );
  assert.deepEqual(
    acceptedFilesystemEvents.map((event) => event.operation),
    ["acquire"],
  );
  const runnerCallsBeforeRejection = base.events.length;
  const filesystemCallsBeforeRejection = base.filesystemEvents.length;
  await assert.rejects(
    base.supervisor.launchWriter(launchInput(oversizedRequestedRoot)),
    assertSupervisorError("invalid_podman_writer_supervisor_request"),
  );
  assert.equal(base.events.length, runnerCallsBeforeRejection);
  assert.equal(
    base.filesystemEvents.length,
    filesystemCallsBeforeRejection,
  );
});

test("measured runtime paths use the native pathname domain before dispatch", async (t) => {
  const base = await fixture(t);
  const maximumPath = `/${"é".repeat(2_047)}`;
  const oversizedPath = `${maximumPath}a`;
  assert.equal(Buffer.byteLength(maximumPath, "utf8"), 4_095);
  assert.equal(Buffer.byteLength(oversizedPath, "utf8"), 4_096);

  const acceptedMeasuredImage = exact({
    ...measuredImage(),
    runtimeIdentity: exact({
      ...measuredImage().runtimeIdentity,
      codexBinaryPath: maximumPath,
    }),
  });
  const acceptedInput = launchInput(base.attachmentRoot, {
    measuredImage: acceptedMeasuredImage,
  });
  const receipt = await base.supervisor.launchWriter(acceptedInput);
  await receipt.stopWriter(stopInput(acceptedInput, receipt));
  const runnerCallsBeforeRejection = base.events.length;
  const filesystemCallsBeforeRejection = base.filesystemEvents.length;

  for (const codexBinaryPath of [
    oversizedPath,
    "/provider/runtime/\ud800",
    "relative/codex",
    "/provider/runtime/../codex",
  ]) {
    const invalidMeasuredImage = exact({
      ...measuredImage(),
      runtimeIdentity: exact({
        ...measuredImage().runtimeIdentity,
        codexBinaryPath,
      }),
    });
    await assert.rejects(
      base.supervisor.launchWriter(
        launchInput(base.attachmentRoot, {
          measuredImage: invalidMeasuredImage,
        }),
      ),
      assertSupervisorError("invalid_podman_writer_supervisor_request"),
    );
  }
  assert.equal(base.events.length, runnerCallsBeforeRejection);
  assert.equal(base.filesystemEvents.length, filesystemCallsBeforeRejection);
});

test(
  "measured runtime canonicality uses captured node:path intrinsics",
  { concurrency: false },
  async (t) => {
    const base = await fixture(t);
    const invalidMeasuredImage = exact({
      ...measuredImage(),
      runtimeIdentity: exact({
        ...measuredImage().runtimeIdentity,
        codexBinaryPath: "relative/codex",
      }),
    });
    const invalidInput = launchInput(base.attachmentRoot, {
      measuredImage: invalidMeasuredImage,
    });
    const isAbsoluteDescriptor = Object.getOwnPropertyDescriptor(
      path,
      "isAbsolute",
    );
    const resolveDescriptor = Object.getOwnPropertyDescriptor(path, "resolve");
    let poisonCalls = 0;
    path.isAbsolute = () => {
      poisonCalls += 1;
      return true;
    };
    path.resolve = (value) => {
      poisonCalls += 1;
      return value;
    };
    syncBuiltinESMExports();
    try {
      await assert.rejects(
        base.supervisor.launchWriter(invalidInput),
        assertSupervisorError("invalid_podman_writer_supervisor_request"),
      );
      assert.equal(poisonCalls, 0);
      assert.equal(base.events.length, 0);
      assert.equal(base.filesystemEvents.length, 0);
    } finally {
      Object.defineProperty(path, "isAbsolute", isAbsoluteDescriptor);
      Object.defineProperty(path, "resolve", resolveDescriptor);
      syncBuiltinESMExports();
    }
  },
);

test(
  "durable writer identities use the captured createHash after builtin export sync",
  { concurrency: false },
  async (t) => {
    const base = await fixture(t);
    const createHashDescriptor = Object.getOwnPropertyDescriptor(
      crypto,
      "createHash",
    );
    assert.equal(typeof createHashDescriptor?.value, "function");
    const originalCreateHash = createHashDescriptor.value;
    let poisonCalls = 0;
    let launch;

    try {
      Object.defineProperty(crypto, "createHash", {
        ...createHashDescriptor,
        value(...arguments_) {
          poisonCalls += 1;
          const hash = Reflect.apply(originalCreateHash, undefined, arguments_);
          hash.update("hostile-supervisor-identity-prefix\0", "utf8");
          return hash;
        },
      });
      syncBuiltinESMExports();
      launch = await base.supervisor.launchWriter(base.input);
    } finally {
      Object.defineProperty(crypto, "createHash", createHashDescriptor);
      syncBuiltinESMExports();
    }

    assert.equal(poisonCalls, 0);
    const eventCount = base.events.length;
    const reconstructed = createPodmanWriterSupervisor(base.options);
    const replay = await reconstructed.launchWriter(base.input);
    assert.deepEqual(replay.evidence, launch.evidence);
    assert.equal(base.events.length, eventCount);
    await replay.stopWriter(stopInput(base.input, replay));
  },
);

test("held mount sources use the 4095-byte native pathname domain", async (t) => {
  const sourcePrefix = "/proc/";
  const sourceSuffix = "/fd/7";
  const maximumSource = `${sourcePrefix}${"1".repeat(
    4_095 - sourcePrefix.length - sourceSuffix.length,
  )}${sourceSuffix}`;
  const oversizedSource = `${sourcePrefix}${"1".repeat(
    4_096 - sourcePrefix.length - sourceSuffix.length,
  )}${sourceSuffix}`;
  assert.equal(Buffer.byteLength(maximumSource, "utf8"), 4_095);
  assert.equal(Buffer.byteLength(oversizedSource, "utf8"), 4_096);

  const accepted = await fixture(t, {
    filesystemSettings: { mountSource: maximumSource },
  });
  const receipt = await accepted.supervisor.launchWriter(accepted.input);
  assert.equal(receipt.evidence.status, "started");
  await receipt.stopWriter(stopInput(accepted.input, receipt));

  const rejected = await fixture(t, {
    filesystemSettings: { mountSource: oversizedSource },
  });
  await assert.rejects(
    rejected.supervisor.launchWriter(rejected.input),
    assertSupervisorError("podman_writer_attachment_revalidation_failed"),
  );
  assert.equal(
    rejected.events.some((event) => event.arguments_[0] === "create"),
    false,
  );
});

test("configured attachment root containment and held-source shape are enforced outside the collaborator", async (t) => {
  const base = await fixture(t);
  const outsideRoot = await mkdtemp(
    join(await realpath(tmpdir()), "podman-writer-outside-root-"),
  );
  t.after(() => rm(outsideRoot, { force: true, recursive: true }));
  await assert.rejects(
    base.supervisor.launchWriter(launchInput(outsideRoot)),
    assertSupervisorError("podman_writer_attachment_mismatch"),
  );
  assert.equal(base.filesystemEvents.length, 0);
  assert.equal(base.events.length, 0);

  const invalidSource = await fixture(t, {
    filesystemSettings: { mountSource: "/tmp/not-a-held-fd" },
  });
  await assert.rejects(
    invalidSource.supervisor.launchWriter(invalidSource.input),
    assertSupervisorError("podman_writer_attachment_revalidation_failed"),
  );
  assert.equal(
    invalidSource.events.some((event) => event.arguments_[0] === "create"),
    false,
  );
});

test("running reconciliation requires the live session bind to match the current canonical root", async (t) => {
  const base = await fixture(t);
  await base.supervisor.launchWriter(base.input);
  const name = base.events.find((event) => event.arguments_[0] === "create")
    .arguments_[2];
  const commandEvents = [];
  const filesystemEvents = [];
  const supervisor = createPodmanWriterSupervisor(exact({
    ...base.options,
    commandRunner: successfulRunner(base.attachmentRoot, commandEvents, {
      containerName: name,
      forceRunning: true,
    }),
    filesystemAuthority: successfulFilesystemAuthority(
      base.parent,
      base.attachmentRoot,
      filesystemEvents,
      { runningMountResult: false },
    ),
  }));
  await assert.rejects(
    supervisor.reconcileWriterLaunch(reconcileInput(base.input)),
    assertSupervisorError("podman_writer_attachment_mismatch"),
  );
  assert.deepEqual(commandEvents.map((event) => event.arguments_[0]), ["container"]);
  assert.deepEqual(
    filesystemEvents.map((event) => event.operation),
    ["acquire", "verify-running-mount", "close"],
  );
});

test("running mount proof is bracketed by stable exact-container PID inspections", async (t) => {
  let runningInspections = 0;
  const base = await fixture(t, {
    mutateInspection(value) {
      if (value.State.Running) {
        runningInspections += 1;
        if (runningInspections === 2) value.State.Pid += 1;
      }
    },
  });
  await assert.rejects(
    base.supervisor.launchWriter(base.input),
    assertSupervisorError("podman_writer_output_invalid"),
  );
  assert.equal(runningInspections, 2);
  assert.equal(
    (await base.state.read(
      exact({ launchAttemptId: "launch-attempt-001" }),
    )).status,
    "created",
  );
});

test("pre-start inspection requires Podman's exact external created state", async (t) => {
  for (const inspectionStatus of ["configured", "exited"]) {
    await t.test(inspectionStatus, async (t) => {
      const base = await fixture(t, { inspectionStatus });
      await assert.rejects(
        base.supervisor.launchWriter(base.input),
        assertSupervisorError("podman_writer_output_invalid"),
      );
      assert.equal(
        base.events.some((event) => event.arguments_[0] === "start"),
        false,
      );
      assert.equal(
        (await base.state.read(
          exact({ launchAttemptId: "launch-attempt-001" }),
        )).status,
        "created",
      );
    });
  }
});

test("Linux default authority holds a rootless helper procfd and reaps it on close", {
  skip: process.platform !== "linux" || !existsSync("/usr/bin/getfacl"),
}, async (t) => {
  let createMarker;
  let holderArgvPath;
  let mountPath;
  const base = await fixture(t, {
    defaultCommandRunnerScript({ parent }) {
      createMarker = join(parent, "holder-create");
      holderArgvPath = join(parent, "holder-argv");
      mountPath = join(parent, "holder-mount");
      return defaultAuthorityPodmanScript({
        createMarker,
        createMutation: ': > "$source_path/child-entry"',
        holderArgvPath,
        mountPath,
        namePath: join(parent, "holder-name"),
        startedPath: join(parent, "holder-started"),
      });
    },
    useDefaultCommandRunner: true,
    useDefaultFilesystemAuthority: true,
  });
  const iteratorDescriptor = Object.getOwnPropertyDescriptor(
    Array.prototype,
    Symbol.iterator,
  );
  const toJsonDescriptor = Object.getOwnPropertyDescriptor(
    Object.prototype,
    "toJSON",
  );
  const originalIterator = iteratorDescriptor.value;
  const reflectApply = Reflect.apply;
  Object.defineProperty(Array.prototype, Symbol.iterator, {
    ...iteratorDescriptor,
    value: function guardedIterator() {
      let promiseOnly = this.length === 2 || this.length === 3;
      for (let index = 0; promiseOnly && index < this.length; index += 1) {
        const descriptor = Object.getOwnPropertyDescriptor(this, String(index));
        promiseOnly = descriptor !== undefined &&
          Object.hasOwn(descriptor, "value") &&
          Object.getPrototypeOf(descriptor.value) === Promise.prototype;
      }
      if (promiseOnly) throw new Error("poisoned promise iterable");
      return reflectApply(originalIterator, this, []);
    },
  });
  Object.defineProperty(Object.prototype, "toJSON", {
    configurable: true,
    value: function guardedToJSON() {
      if (
        Object.hasOwn(this, "attachmentRoot") &&
        Object.hasOwn(this, "configuredRoot") &&
        Object.hasOwn(this, "contractVersion")
      ) {
        throw new Error("poisoned acquisition toJSON");
      }
      return this;
    },
  });
  try {
    await assert.rejects(
      base.supervisor.launchWriter(base.input),
      assertSupervisorError("podman_writer_attachment_revalidation_failed"),
    );
  } finally {
    Object.defineProperty(Array.prototype, Symbol.iterator, iteratorDescriptor);
    if (toJsonDescriptor === undefined) delete Object.prototype.toJSON;
    else Object.defineProperty(Object.prototype, "toJSON", toJsonDescriptor);
  }
  assert.equal(existsSync(createMarker), true);
  assert.equal(existsSync(join(base.attachmentRoot, "child-entry")), true);
  const source = (await readFile(mountPath, "utf8")).trim();
  const holderArgv = await readFile(holderArgvPath, "utf8");
  assert.equal(holderArgv.includes(base.parent), false);
  assert.equal(holderArgv.includes(base.attachmentRoot), false);
  const match = /^\/proc\/([1-9][0-9]*)\/fd\/[0-9]+$/u.exec(source);
  assert.notEqual(match, null);
  assert.equal(existsSync(`/proc/${match[1]}`), false);
});

test("Linux holder waits for its closed-stdio process group before authority release", {
  skip: process.platform !== "linux" || !existsSync("/usr/bin/getfacl"),
}, async (t) => {
  for (const mode of ["normal", "leader-error", "exit-before-close", "timeout"]) {
    await t.test(mode, async (t) => {
      let authorityReleasedPath;
      let descendantExitPath;
      let descendantPidPath;
      let descendantReadyPath;
      let leaderExitRequestedPath;
      let sourceAfterReleasePath;
      let sourceRetainedPath;
      t.after(async () => {
        if (
          typeof descendantExitPath !== "string" ||
          typeof descendantPidPath !== "string"
        ) return;
        try {
          await writeFile(descendantExitPath, "cleanup\n", { mode: 0o600 });
        } catch {
          // A setup failure may leave no writable fixture root. The bounded
          // absence check below still reports any recorded survivor.
        }
        let descendantPid = null;
        try {
          descendantPid = Number(
            (await readFile(descendantPidPath, "utf8")).trim(),
          );
        } catch {
          return;
        }
        for (
          let attempt = 0;
          attempt < 100 && processExists(descendantPid);
          attempt += 1
        ) {
          await delay(10);
        }
        assert.equal(
          processExists(descendantPid),
          false,
          "holder descendant ignored its test-owned cleanup marker",
        );
      });
      const base = await fixture(t, {
        commandTimeoutMilliseconds: 2_000,
        defaultCommandRunnerScript({ attachmentRoot, parent }) {
          authorityReleasedPath = join(parent, "holder-authority-released");
          descendantExitPath = join(parent, "holder-descendant-exit");
          descendantPidPath = join(parent, "holder-descendant-pid");
          descendantReadyPath = join(parent, "holder-descendant-ready");
          leaderExitRequestedPath = join(
            parent,
            "holder-leader-exit-requested",
          );
          sourceAfterReleasePath = join(parent, "holder-source-after-release");
          sourceRetainedPath = join(parent, "holder-source-retained");
          return descendantHolderPodmanScript({
            attachmentRoot,
            authorityReleasedPath,
            configuredRoot: parent,
            descendantExitPath,
            descendantPidPath,
            descendantReadyPath,
            leaderExitRequestedPath,
            mode,
            sourceAfterReleasePath,
            sourceRetainedPath,
          });
        },
        stopTimeoutSeconds: 1,
        useDefaultCommandRunner: true,
        useDefaultFilesystemAuthority: true,
      });

      let settled = false;
      const pending = base.supervisor.launchWriter(base.input).then(
        () => {
          settled = true;
          assert.fail("holder failure unexpectedly launched a writer");
        },
        (error) => {
          settled = true;
          return error;
        },
      );
      await waitForPath(descendantReadyPath);
      const descendantPid = Number(readFileSync(descendantPidPath, "utf8").trim());
      let error;
      try {
        if (mode === "timeout") {
          error = await pending;
        } else {
          // This marker says only that the wrapper is about to exit. The
          // production childExited/PGID barriers, not the fixture marker,
          // establish waitpid and process-group quiescence.
          await waitForPath(leaderExitRequestedPath);
          await waitForPath(sourceRetainedPath);
          await delay(100);
          assert.equal(settled, false);
          assert.equal(processExists(descendantPid), true);
          await writeFile(descendantExitPath, "exit\n", { mode: 0o600 });
          error = await pending;
        }
      } finally {
        if (mode !== "timeout") {
          await writeFile(descendantExitPath, "exit\n", { mode: 0o600 })
            .catch(() => {});
        }
      }
      assert.equal(error instanceof PodmanWriterSupervisorError, true);
      assert.equal(processExists(descendantPid), false);
      await writeFile(authorityReleasedPath, "released\n", { mode: 0o600 });
      await delay(100);
      assert.equal(existsSync(sourceAfterReleasePath), false);
    });
  }
});

test("Linux default authority rejects malformed, mismatched, and timed-out holders", {
  skip: process.platform !== "linux" || !existsSync("/usr/bin/getfacl"),
}, async (t) => {
  for (const mode of ["invalid-json", "malformed", "mismatch", "timeout"]) {
    let pidPath;
    const base = await fixture(t, {
      commandTimeoutMilliseconds: 2_000,
      defaultCommandRunnerScript({ parent }) {
        pidPath = join(parent, `holder-${mode}-pid`);
        return failingHolderPodmanScript({ mode, pidPath });
      },
      stopTimeoutSeconds: 1,
      useDefaultCommandRunner: true,
      useDefaultFilesystemAuthority: true,
    });
    await assert.rejects(
      base.supervisor.launchWriter(base.input),
      assertSupervisorError(
        mode === "mismatch"
          ? "podman_writer_attachment_mismatch"
          : "podman_writer_supervisor_outcome_uncertain",
      ),
    );
    const pid = (await readFile(pidPath, "utf8")).trim();
    assert.equal(existsSync(`/proc/${pid}`), false);
  }
});

test("Linux holder uses captured AbortSignal listener methods and reaps on abort", {
  skip: process.platform !== "linux" || !existsSync("/usr/bin/getfacl"),
}, async (t) => {
  const controller = new AbortController();
  let addCalls = 0;
  let captureReads = 0;
  let pidPath;
  let removeCalls = 0;
  let rootlessProofReadyPath;
  let rootlessProofReleasePath;
  const signalAddEventListener = EventTarget.prototype.addEventListener;
  const signalRemoveEventListener = EventTarget.prototype.removeEventListener;
  // Node's default runner legitimately reaches the signal's own methods for
  // the two pre-holder child processes, but it supplies a null-prototype
  // listener-options record. A holder regression would instead reach this
  // method with its ordinary inherited-options dictionary and fail here.
  Object.defineProperties(controller.signal, {
    addEventListener: {
      configurable: true,
      value(type, listener, options) {
        addCalls += 1;
        if (
          options === null ||
          typeof options !== "object" ||
          Object.getPrototypeOf(options) !== null
        ) {
          throw new Error("poisoned addEventListener");
        }
        return Reflect.apply(signalAddEventListener, this, [
          type,
          listener,
          options,
        ]);
      },
    },
    removeEventListener: {
      configurable: true,
      value() {
        removeCalls += 1;
        return Reflect.apply(signalRemoveEventListener, this, arguments);
      },
    },
  });
  const base = await fixture(t, {
    commandTimeoutMilliseconds: 2_000,
    defaultCommandRunnerScript({ parent }) {
      pidPath = join(parent, "holder-poisoned-listener-pid");
      rootlessProofReadyPath = join(parent, "rootless-proof-ready");
      rootlessProofReleasePath = join(parent, "rootless-proof-release");
      return failingHolderPodmanScript({
        mode: "timeout",
        pidPath,
        rootlessProofReadyPath,
        rootlessProofReleasePath,
      });
    },
    stopTimeoutSeconds: 1,
    useDefaultCommandRunner: true,
    useDefaultFilesystemAuthority: true,
  });
  const captureDescriptor = Object.getOwnPropertyDescriptor(
    Object.prototype,
    "capture",
  );
  let observedError = null;
  const pending = base.supervisor.launchWriter(
    launchInput(base.attachmentRoot, { signal: controller.signal }),
  );
  void pending.catch(() => {});
  let pid = null;
  let poisonInstalled = false;
  try {
    await waitForPath(rootlessProofReadyPath);
    assert.equal(addCalls, 1);
    assert.equal(removeCalls, 0);
    Object.defineProperty(Object.prototype, "capture", {
      configurable: true,
      get() {
        captureReads += 1;
        throw new Error("poisoned inherited capture option");
      },
    });
    poisonInstalled = true;
    await writeFile(rootlessProofReleasePath, "release\n", { mode: 0o600 });
    await waitForPath(pidPath);
    controller.abort();
    try {
      await pending;
    } catch (error) {
      observedError = error;
    }
    pid = Number((await readFile(pidPath, "utf8")).trim());
    for (let attempt = 0; attempt < 100 && processExists(pid); attempt += 1) {
      await delay(10);
    }
  } finally {
    try {
      await writeFile(rootlessProofReleasePath, "release\n", { mode: 0o600 })
        .catch(() => {});
      controller.abort();
      await pending.catch(() => {});
    } finally {
      if (poisonInstalled && captureDescriptor === undefined) {
        delete Object.prototype.capture;
      } else if (poisonInstalled) {
        Object.defineProperty(Object.prototype, "capture", captureDescriptor);
      }
    }
  }
  assert.equal(
    assertSupervisorError("podman_writer_supervisor_aborted")(observedError),
    true,
  );
  assert.equal(addCalls, 2);
  assert.equal(captureReads, 0);
  assert.equal(removeCalls, 2);
  assert.equal(processExists(pid), false);
});

test("Linux default authority blocks start after holder exit or configured-source drift", {
  skip: process.platform !== "linux" || !existsSync("/usr/bin/getfacl"),
}, async (t) => {
  for (const mode of ["holder-exit", "source-drift"]) {
    let startMarker;
    const base = await fixture(t, {
      defaultCommandRunnerScript({ parent }) {
        const mountPath = join(parent, `${mode}-mount`);
        startMarker = join(parent, `${mode}-started`);
        const createMutation = mode === "holder-exit"
          ? 'holder_pid=${source_path#/proc/}; holder_pid=${holder_pid%%/*}; kill -KILL "$holder_pid"'
          : `printf '%s\\n' /proc/1/fd/0 > ${shellQuote(mountPath)}`;
        return defaultAuthorityPodmanScript({
          createMarker: join(parent, `${mode}-create`),
          createMutation,
          mountPath,
          namePath: join(parent, `${mode}-name`),
          startedPath: startMarker,
        });
      },
      useDefaultCommandRunner: true,
      useDefaultFilesystemAuthority: true,
    });
    await assert.rejects(
      base.supervisor.launchWriter(base.input),
      assertSupervisorError(
        mode === "holder-exit"
          ? "podman_writer_supervisor_outcome_uncertain"
          : "podman_writer_output_invalid",
      ),
    );
    assert.equal(existsSync(startMarker), false);
  }
});

test("Linux default authority distinguishes child churn from replacement and policy change", {
  skip: process.platform !== "linux" || !existsSync("/usr/bin/getfacl"),
}, async (t) => {
  for (const mode of ["replacement", "policy-change"]) {
    let startMarker;
    const base = await fixture(t, {
      defaultCommandRunnerScript({ attachmentRoot, parent }) {
        startMarker = join(parent, `${mode}-started`);
        const createMutation = mode === "replacement"
          ? `mv ${shellQuote(attachmentRoot)} ${shellQuote(`${attachmentRoot}-old`)}; mkdir -m 700 ${shellQuote(attachmentRoot)}`
          : 'chmod 0777 "$source_path"';
        return defaultAuthorityPodmanScript({
          createMarker: join(parent, `${mode}-create`),
          createMutation,
          mountPath: join(parent, `${mode}-mount`),
          namePath: join(parent, `${mode}-name`),
          startedPath: startMarker,
        });
      },
      useDefaultCommandRunner: true,
      useDefaultFilesystemAuthority: true,
    });
    await assert.rejects(
      base.supervisor.launchWriter(base.input),
      assertSupervisorError("podman_writer_attachment_mismatch"),
    );
    assert.equal(existsSync(startMarker), false);
  }
});

test("secure Linux authority rejects missing roots and final or ancestor symlink aliases", {
  skip: process.platform !== "linux" ||
    !existsSync("/usr/bin/getfacl") ||
    !existsSync("/usr/bin/setfacl"),
}, async (t) => {
  let createMarker;
  const base = await fixture(t, {
    defaultCommandRunnerScript({ parent }) {
      createMarker = join(parent, "default-authority-create");
      return defaultAuthorityPodmanScript({
        createMarker,
        mountPath: join(parent, "default-authority-mount"),
        namePath: join(parent, "default-authority-name"),
        startedPath: join(parent, "default-authority-started"),
      });
    },
    useDefaultCommandRunner: true,
    useDefaultFilesystemAuthority: true,
  });

  const missingRoot = join(base.parent, "missing-attachment");
  await assert.rejects(
    base.supervisor.launchWriter(launchInput(missingRoot)),
    assertSupervisorError("podman_writer_attachment_missing"),
  );

  const finalAlias = join(base.parent, "attachment-alias");
  await symlink(base.attachmentRoot, finalAlias, "dir");
  await assert.rejects(
    base.supervisor.launchWriter(launchInput(finalAlias)),
    assertSupervisorError("podman_writer_attachment_mismatch"),
  );

  const unsafeMode = join(base.parent, "unsafe-mode-attachment");
  await mkdir(unsafeMode, { mode: 0o700 });
  await chmod(unsafeMode, 0o777);
  await assert.rejects(
    base.supervisor.launchWriter(launchInput(unsafeMode)),
    assertSupervisorError("podman_writer_attachment_mismatch"),
  );

  const extendedAcl = join(base.parent, "extended-acl-attachment");
  await mkdir(extendedAcl, { mode: 0o700 });
  execFileSync(
    "/usr/bin/setfacl",
    ["-m", "u:12345:---,m::---", "--", extendedAcl],
    { stdio: "ignore" },
  );
  await assert.rejects(
    base.supervisor.launchWriter(launchInput(extendedAcl)),
    assertSupervisorError("podman_writer_attachment_mismatch"),
  );

  const realParent = join(base.parent, "real-parent");
  const nestedRoot = join(realParent, "nested-attachment");
  const parentAlias = join(base.parent, "parent-alias");
  await mkdir(nestedRoot, { mode: 0o700, recursive: true });
  await symlink(realParent, parentAlias, "dir");
  await assert.rejects(
    base.supervisor.launchWriter(
      launchInput(join(parentAlias, "nested-attachment")),
    ),
    assertSupervisorError("podman_writer_attachment_mismatch"),
  );

  const originalSpawnSync = childProcess.spawnSync;
  const inheritedSpawnResult = Object.assign(Object.create({
    error: new Error("inherited spawn result poison"),
  }), {
    signal: null,
    status: 0,
    stderr: Buffer.alloc(0),
    stdout: Buffer.from("user::rwx\ngroup::---\nother::---\n\n", "utf8"),
  });
  childProcess.spawnSync = () => inheritedSpawnResult;
  syncBuiltinESMExports();
  try {
    await assert.rejects(
      base.supervisor.launchWriter(base.input),
      assertSupervisorError("podman_writer_attachment_revalidation_failed"),
    );
  } finally {
    childProcess.spawnSync = originalSpawnSync;
    syncBuiltinESMExports();
  }
  // The fake running PID has no live /session path. Reaching create proves
  // inherited spawnSync fields did not replace own getfacl result fields.
  assert.equal(existsSync(createMarker), true);
});

test("revalidates the non-root process identity before every Podman dispatch", async (t) => {
  const originalGetUserId = process.getuid;
  const originalGetEffectiveUserId = process.geteuid;
  let realUserId = 1000;
  let effectiveUserId = 1000;
  let userScopedModule;
  try {
    process.getuid = () => realUserId;
    process.geteuid = () => effectiveUserId;
    userScopedModule = await import(
      "../src/podman-writer-supervisor.mjs?uid-revalidation-test",
    );
  } finally {
    process.getuid = originalGetUserId;
    process.geteuid = originalGetEffectiveUserId;
  }
  assert.equal(process.getuid, originalGetUserId);
  assert.equal(process.geteuid, originalGetEffectiveUserId);

  const transitioned = await fixture(t, {
    onUnshare() {
      effectiveUserId = 0;
    },
  });
  const transitionedSupervisor = userScopedModule.createPodmanWriterSupervisor(
    transitioned.options,
  );
  await assert.rejects(
    transitionedSupervisor.launchWriter(transitioned.input),
    (error) => error?.code === "podman_writer_rootless_required",
  );
  assert.deepEqual(
    transitioned.events.map((event) => event.arguments_),
    [["unshare", "/usr/bin/true"]],
  );

  realUserId = 0;
  effectiveUserId = 0;
  const root = await fixture(t);
  const rootSupervisor = userScopedModule.createPodmanWriterSupervisor(
    root.options,
  );
  await assert.rejects(
    rootSupervisor.launchWriter(root.input),
    (error) => error?.code === "podman_writer_rootless_required",
  );
  assert.equal(root.events.length, 0);
});

test("enforces local rootless execution, exact digest, and bounded unambiguous output", async (t) => {
  const unprovedRootless = await fixture(t, { unshareReject: true });
  await assert.rejects(
    unprovedRootless.supervisor.launchWriter(unprovedRootless.input),
    assertSupervisorError("podman_writer_supervisor_outcome_uncertain"),
  );
  assert.deepEqual(
    unprovedRootless.events.map((event) => event.arguments_),
    [["unshare", "/usr/bin/true"]],
  );
  assert.equal(
    unprovedRootless.events.some((event) => event.arguments_[0] === "create"),
    false,
  );

  const wrongImage = await fixture(t, {
    inspectedDigest: `sha256:${"f".repeat(64)}`,
  });
  await assert.rejects(
    wrongImage.supervisor.launchWriter(wrongImage.input),
    assertSupervisorError("podman_writer_image_mismatch"),
  );
  assert.equal(wrongImage.events.some((event) => event.arguments_[0] === "create"), false);

  for (const imageUser of ["1000", "0:1000", "1000:0", "01000:1000", "x:y"]) {
    const wrongUser = await fixture(t, { imageUser });
    await assert.rejects(
      wrongUser.supervisor.launchWriter(wrongUser.input),
      assertSupervisorError("podman_writer_image_mismatch"),
    );
    assert.equal(
      wrongUser.events.some((event) => event.arguments_[0] === "create"),
      false,
    );
  }
  const arrayConfig = await fixture(t, {
    imageInspection: [{
      Architecture: "amd64",
      Config: Object.assign([], { User: "1000:1000" }),
      Digest: DIGEST,
      Os: "linux",
    }],
  });
  await assert.rejects(
    arrayConfig.supervisor.launchWriter(arrayConfig.input),
    assertSupervisorError("podman_writer_image_mismatch"),
  );

  const malformed = await fixture(t, { createOutput: `${CONTAINER_ID}\nextra\n` });
  await assert.rejects(
    malformed.supervisor.launchWriter(malformed.input),
    assertSupervisorError("podman_writer_output_invalid"),
  );
  const createCount = malformed.events.filter((event) => event.arguments_[0] === "create").length;
  await assert.rejects(
    malformed.supervisor.launchWriter(malformed.input),
    assertSupervisorError("podman_writer_supervisor_outcome_uncertain"),
  );
  assert.equal(
    malformed.events.filter((event) => event.arguments_[0] === "create").length,
    createCount,
  );
});

test("Linux default runner kills closed-stdio descendants before releasing authority", {
  skip: process.platform !== "linux",
}, async (t) => {
  for (const failureMode of [
    "nonzero",
    "timeout",
    "abort",
    "stdout-overflow",
    "stderr-overflow",
  ]) {
    await t.test(failureMode, async (t) => {
      const controller = new AbortController();
      let authorityClosedPath;
      let closeCount = 0;
      let descendantLiveAtClose = null;
      let descendantExitPath;
      let descendantPidPath;
      let descendantReadyPath;
      let heldFd = null;
      let settlementCount = 0;
      let sourceAttemptedPath;
      let sourceMissingPath;
      let sourceVisiblePath;
      const base = await fixture(t, {
        commandTimeoutMilliseconds: failureMode === "timeout" ? 2_000 : 10_000,
        defaultCommandRunnerScript({ parent }) {
          authorityClosedPath = join(parent, "filesystem-authority-closed");
          descendantExitPath = join(parent, "podman-descendant-exit");
          descendantPidPath = join(parent, "podman-descendant-pid");
          descendantReadyPath = join(parent, "podman-descendant-ready");
          sourceAttemptedPath = join(parent, "podman-descendant-source-attempted");
          sourceMissingPath = join(parent, "podman-descendant-source-missing");
          sourceVisiblePath = join(parent, "podman-descendant-source-visible");
          return closedStdioDescendantPodmanScript({
            descendantExitPath,
            descendantPidPath,
            descendantReadyPath,
            failureMode,
            sourceAttemptedPath,
            sourceMissingPath,
            sourceReleasedPath: authorityClosedPath,
            sourceVisiblePath,
          });
        },
        filesystemSettings: {
          mountSource() {
            return `/proc/${process.pid}/fd/${heldFd}`;
          },
          onAcquire(input) {
            heldFd = openSync(
              input.attachment.rootPath,
              fsConstants.O_RDONLY |
                fsConstants.O_DIRECTORY |
                fsConstants.O_NOFOLLOW,
            );
          },
          onClose() {
            closeCount += 1;
            const descendantPid = Number(readFileSync(descendantPidPath, "utf8").trim());
            descendantLiveAtClose = processExists(descendantPid);
            closeSync(heldFd);
            heldFd = null;
            writeFileSync(authorityClosedPath, "closed\n", { mode: 0o600 });
          },
        },
        maxOutputBytes: 1_024,
        stopTimeoutSeconds: 1,
        useDefaultCommandRunner: true,
      });
      t.after(() => {
        if (heldFd !== null) closeSync(heldFd);
      });

      const pending = base.supervisor.launchWriter(
        launchInput(base.attachmentRoot, { signal: controller.signal }),
      ).then(
        () => {
          settlementCount += 1;
          assert.fail("failed Podman launch unexpectedly succeeded");
        },
        (error) => {
          settlementCount += 1;
          return error;
        },
      );
      await waitForPath(descendantReadyPath);
      if (failureMode === "abort") controller.abort();

      let error;
      try {
        if (failureMode === "nonzero") {
          await waitForPath(sourceAttemptedPath);
          assert.equal(existsSync(sourceVisiblePath), true);
          assert.equal(existsSync(sourceMissingPath), false);
          assert.equal(settlementCount, 0);
          assert.equal(closeCount, 0);
          assert.equal(heldFd !== null, true);
          await writeFile(descendantExitPath, "exit\n", { mode: 0o600 });
        }
        error = await pending;
      } finally {
        if (failureMode === "nonzero") {
          await writeFile(descendantExitPath, "exit\n", { mode: 0o600 })
            .catch(() => {});
        }
      }
      assert.equal(error instanceof PodmanWriterSupervisorError, true);
      assert.equal(
        error.code,
        failureMode === "abort"
          ? "podman_writer_supervisor_aborted"
          : "podman_writer_supervisor_outcome_uncertain",
      );
      const descendantPid = Number(readFileSync(descendantPidPath, "utf8").trim());
      await delay(100);
      assert.equal(processExists(descendantPid), false);
      assert.equal(descendantLiveAtClose, false);
      assert.equal(closeCount, 1);
      assert.equal(settlementCount, 1);
      assert.equal(heldFd, null);
      assert.equal(existsSync(authorityClosedPath), true);
      assert.equal(existsSync(sourceAttemptedPath), failureMode === "nonzero");
      assert.equal(existsSync(sourceMissingPath), false);
      assert.equal(existsSync(sourceVisiblePath), failureMode === "nonzero");
    });
  }
});

test("default runner reaps detached Podman start without waiting for container pipes", async (t) => {
  let holdingPath;
  let namePath;
  let releasePath;
  let releasedPath;
  const base = await fixture(t, {
    commandTimeoutMilliseconds: 2_000,
    defaultCommandRunnerScript({ parent }) {
      holdingPath = join(parent, "podman-start-descendant-holding");
      namePath = join(parent, "podman-start-container-name");
      releasePath = join(parent, "podman-start-descendant-release");
      releasedPath = join(parent, "podman-start-descendant-released");
      return detachedStartPodmanScript({
        holdingPath,
        namePath,
        releasePath,
        releasedPath,
      });
    },
    maxOutputBytes: 1_024,
    stopTimeoutSeconds: 1,
    useDefaultCommandRunner: true,
  });

  const pending = base.supervisor.launchWriter(base.input);
  await waitForPath(holdingPath);
  try {
    const receipt = await pending;
    assert.equal(receipt.evidence.status, "started");
    assert.equal(existsSync(releasedPath), false);
  } finally {
    await writeFile(releasePath, "release\n", { mode: 0o600 });
  }
  await waitForPath(releasedPath);
});

test("rejects short new create identities before Podman start dispatch", async (t) => {
  const shortContainerId = "e".repeat(63);
  let holdingPath;
  let namePath;
  let releasePath;
  let releasedPath;
  const base = await fixture(t, {
    commandTimeoutMilliseconds: 2_000,
    defaultCommandRunnerScript({ parent }) {
      holdingPath = join(parent, "podman-short-start-descendant-holding");
      namePath = join(parent, "podman-short-start-container-name");
      releasePath = join(parent, "podman-short-start-descendant-release");
      releasedPath = join(parent, "podman-short-start-descendant-released");
      return detachedStartPodmanScript({
        containerId: shortContainerId,
        holdingPath,
        namePath,
        releasePath,
        releasedPath,
      });
    },
    maxOutputBytes: 1_024,
    stopTimeoutSeconds: 1,
    useDefaultCommandRunner: true,
  });

  await assert.rejects(
    base.supervisor.launchWriter(base.input),
    assertSupervisorError("podman_writer_output_invalid"),
  );
  assert.equal(existsSync(holdingPath), false);
  assert.equal(existsSync(releasePath), false);
  assert.equal(existsSync(releasedPath), false);
});

test("exact Podman start abort and timeout hold authority until natural CLI close", async (t) => {
  const controller = new AbortController();
  let closeCount = 0;
  let namePath;
  let pidPath;
  let releasePath;
  let startedPath;
  const base = await fixture(t, {
    commandTimeoutMilliseconds: 2_000,
    defaultCommandRunnerScript({ parent }) {
      namePath = join(parent, "podman-aborted-container-name");
      pidPath = join(parent, "podman-aborted-start-pid");
      releasePath = join(parent, "podman-aborted-start-release");
      startedPath = join(parent, "podman-aborted-started");
      return blockingStartPodmanScript({ namePath, pidPath, releasePath, startedPath });
    },
    filesystemSettings: {
      onClose() {
        closeCount += 1;
      },
    },
    stopTimeoutSeconds: 1,
    useDefaultCommandRunner: true,
  });

  let settled = false;
  const pending = base.supervisor.launchWriter(
    launchInput(base.attachmentRoot, { signal: controller.signal }),
  ).finally(() => {
    settled = true;
  });
  void pending.catch(() => {});
  await waitForPath(startedPath);
  controller.abort();
  await delay(2_100);
  const pid = Number(readFileSync(pidPath, "utf8").trim());
  assert.equal(processExists(pid), true);
  assert.equal(settled, false);
  assert.equal(closeCount, 0);

  await writeFile(releasePath, "release\n", { mode: 0o600 });
  await assert.rejects(pending, assertSupervisorError("podman_writer_supervisor_aborted"));
  assert.equal(settled, true);
  assert.equal(closeCount, 1);
});

test("failed spawned exact Podman start fail-stops with authority held", async (t) => {
  let closeCount = 0;
  let namePath;
  let pidPath;
  let releasePath;
  let startedPath;
  const base = await fixture(t, {
    commandTimeoutMilliseconds: 2_000,
    defaultCommandRunnerScript({ parent }) {
      namePath = join(parent, "podman-failed-start-container-name");
      pidPath = join(parent, "podman-failed-start-pid");
      releasePath = join(parent, "podman-failed-start-release");
      startedPath = join(parent, "podman-failed-started");
      return blockingStartPodmanScript({
        namePath,
        pidPath,
        releasePath,
        startExitCode: 42,
        startedPath,
      });
    },
    filesystemSettings: {
      onClose() {
        closeCount += 1;
      },
    },
    stopTimeoutSeconds: 1,
    useDefaultCommandRunner: true,
  });

  let settled = false;
  void base.supervisor.launchWriter(base.input).then(
    () => {
      settled = true;
    },
    () => {
      settled = true;
    },
  );
  await waitForPath(startedPath);
  await writeFile(releasePath, "release\n", { mode: 0o600 });
  const pid = Number(readFileSync(pidPath, "utf8").trim());
  for (let attempt = 0; attempt < 100 && processExists(pid); attempt += 1) {
    await delay(10);
  }
  assert.equal(processExists(pid), false);
  await delay(100);
  assert.equal(settled, false);
  assert.equal(closeCount, 0);
});

test("preserves AbortSignal and rejects bad callback arity or non-native runner promises", async (t) => {
  const aborted = await fixture(t);
  const controller = new AbortController();
  controller.abort();
  const input = launchInput(aborted.attachmentRoot, { signal: controller.signal });
  await assert.rejects(
    aborted.supervisor.launchWriter(input),
    assertSupervisorError("podman_writer_supervisor_aborted"),
  );
  assert.equal(aborted.events.length, 0);
  await assert.rejects(
    aborted.supervisor.launchWriter(input, "extra"),
    assertSupervisorError("invalid_podman_writer_supervisor_request"),
  );

  const midRunController = new AbortController();
  let midRunCalls = 0;
  const midRun = await fixture(t, {
    async commandRunner() {
      midRunCalls += 1;
      midRunController.abort();
      return {
        stderr: "",
        stdout: "",
      };
    },
  });
  await assert.rejects(
    midRun.supervisor.launchWriter(
      launchInput(midRun.attachmentRoot, { signal: midRunController.signal }),
    ),
    assertSupervisorError("podman_writer_supervisor_aborted"),
  );
  assert.equal(midRunCalls, 1);
  assert.equal(await midRun.state.read(exact({ launchAttemptId: "launch-attempt-001" })), null);

  const thenable = await fixture(t, {
    commandRunner() {
      return { then() {} };
    },
  });
  await assert.rejects(
    thenable.supervisor.launchWriter(thenable.input),
    assertSupervisorError("podman_writer_supervisor_outcome_uncertain"),
  );
});

test("does not leak raw image, device, provider, or request authority", async (t) => {
  const base = await fixture(t);
  await base.supervisor.launchWriter(base.input);
  const commandText = base.events
    .flatMap((event) => event.arguments_)
    .join("\n");
  assert.equal(commandText.includes("/provider/runtime/codex"), false);
  assert.equal(commandText.includes("/provider/image/authority"), false);
  assert.equal(commandText.includes("/dev/mapper/private-session"), false);
  assert.equal(commandText.includes(base.attachmentRoot), false);
  assert.equal(commandText.includes(HELD_MOUNT_SOURCE), true);
  const stateBytes = (
    await Promise.all(
      (await readdir(base.stateRoot)).map((entry) =>
        readFile(join(base.stateRoot, entry), "utf8"),
      ),
    )
  ).join("\n");
  assert.equal(stateBytes.includes(base.attachmentRoot), false);
  assert.equal(stateBytes.includes(IMAGE_REFERENCE), false);
  assert.equal(stateBytes.includes("/provider/"), false);
});

test("request and state boundaries ignore poisoned ambient inspection intrinsics", async (t) => {
  const base = await fixture(t);
  const originals = {
    arrayEvery: Array.prototype.every,
    arrayIncludes: Array.prototype.includes,
    arraySort: Array.prototype.sort,
    getOwnPropertyDescriptor: Object.getOwnPropertyDescriptor,
    getPrototypeOf: Object.getPrototypeOf,
    hasOwn: Object.hasOwn,
    regexpTest: RegExp.prototype.test,
  };
  const poison = () => {
    throw new Error("poisoned ambient intrinsic");
  };
  let receipt;
  try {
    Array.prototype.every = poison;
    Array.prototype.includes = poison;
    Array.prototype.sort = poison;
    Object.getOwnPropertyDescriptor = poison;
    Object.getPrototypeOf = poison;
    Object.hasOwn = poison;
    RegExp.prototype.test = poison;
    receipt = await base.supervisor.launchWriter(base.input);
  } finally {
    Array.prototype.every = originals.arrayEvery;
    Array.prototype.includes = originals.arrayIncludes;
    Array.prototype.sort = originals.arraySort;
    Object.getOwnPropertyDescriptor = originals.getOwnPropertyDescriptor;
    Object.getPrototypeOf = originals.getPrototypeOf;
    Object.hasOwn = originals.hasOwn;
    RegExp.prototype.test = originals.regexpTest;
  }
  assert.equal(receipt.evidence.status, "started");
});

test("Podman JSON requires own data fields despite Object and Array prototype pollution", async (t) => {
  const installed = [];
  const install = (prototype, key, value) => {
    installed.push({
      descriptor: Object.getOwnPropertyDescriptor(prototype, key),
      key,
      prototype,
    });
    Object.defineProperty(prototype, key, {
      configurable: true,
      value,
      writable: true,
    });
  };
  const restore = () => {
    while (installed.length > 0) {
      const entry = installed.pop();
      if (entry.descriptor === undefined) delete entry.prototype[entry.key];
      else Object.defineProperty(entry.prototype, entry.key, entry.descriptor);
    }
  };

  try {
    install(Object.prototype, "Digest", DIGEST);
    install(Object.prototype, "Architecture", "amd64");
    install(Object.prototype, "Os", "linux");
    const inheritedImage = await fixture(t, { imageInspection: [{}] });
    await assert.rejects(
      inheritedImage.supervisor.launchWriter(inheritedImage.input),
      assertSupervisorError("podman_writer_image_mismatch"),
    );
    restore();

    install(Object.prototype, "Id", CONTAINER_ID);
    install(Object.prototype, "Names", ["inherited-name"]);
    install(Object.prototype, "State", "exited");
    const inheritedPs = await fixture(t, { psEntries: [{}] });
    await assert.rejects(
      inheritedPs.supervisor.reconcileWriterLaunch(
        reconcileInput(inheritedPs.input),
      ),
      assertSupervisorError("podman_writer_supervisor_outcome_uncertain"),
    );
    restore();

    install(Array.prototype, "Running", true);
    install(Array.prototype, "Pid", 42001);
    install(Array.prototype, "Status", "running");
    const inheritedInspect = await fixture(t, {
      mutateInspection(value) {
        value.State = [];
      },
    });
    await assert.rejects(
      inheritedInspect.supervisor.launchWriter(inheritedInspect.input),
      assertSupervisorError("podman_writer_output_invalid"),
    );
  } finally {
    restore();
  }
});
