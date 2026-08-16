import assert from "node:assert/strict";
import { execFileSync } from "node:child_process";
import {
  closeSync,
  constants as fsConstants,
  existsSync,
  openSync,
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
import { tmpdir } from "node:os";
import { join } from "node:path";
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

function delayedReapPodmanScript({
  mode,
  phase,
  reapedPath,
  releasePath,
  startedPath,
  terminatedPath,
}) {
  const info = JSON.stringify({ host: { security: { rootless: true } } });
  const image = JSON.stringify([{
    Architecture: "amd64",
    Digest: DIGEST,
    Os: "linux",
  }]);
  const failureBody = mode === "stdout-overflow"
    ? `printf '%2048s' x\nexec /bin/sleep 30`
    : mode === "stderr-overflow"
      ? `printf '%2048s' x >&2\nexec /bin/sleep 30`
      : "exec /bin/sleep 30";
  const targetBody = `
    target_pid=$$
    (
      while kill -0 "$target_pid" 2>/dev/null; do
        /bin/sleep 0.01
      done
      : > ${shellQuote(terminatedPath)}
      while ! test -f ${shellQuote(releasePath)}; do
        /bin/sleep 0.01
      done
      printf '%s\\n' reaped > ${shellQuote(reapedPath)}
    ) &
    : > ${shellQuote(startedPath)}
    ${failureBody}`;
  const createBody = phase === "create"
    ? targetBody
    : `printf '%s\\n' ${shellQuote(CONTAINER_ID)}`;
  const startBody = phase === "start"
    ? targetBody
    : `printf '%s\\n' ${shellQuote(CONTAINER_ID)}`;
  return `#!/bin/sh
set -eu
case "$1" in
  info)
    printf '%s\\n' ${shellQuote(info)}
    ;;
  image)
    printf '%s\\n' ${shellQuote(image)}
    ;;
  create)
    ${createBody}
    ;;
  start)
    ${startBody}
    ;;
  *)
    exit 64
    ;;
esac
`;
}

function heldFdReapPodmanScript({
  missingPath,
  reapedPath,
  releasePath,
  startedPath,
  terminatedPath,
  visiblePath,
}) {
  const info = JSON.stringify({ host: { security: { rootless: true } } });
  const image = JSON.stringify([{
    Architecture: "amd64",
    Digest: DIGEST,
    Os: "linux",
  }]);
  return `#!/bin/sh
set -eu
case "$1" in
  info)
    printf '%s\\n' ${shellQuote(info)}
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
    target_pid=$$
    (
      while kill -0 "$target_pid" 2>/dev/null; do
        /bin/sleep 0.01
      done
      if test -d "$source_path"; then
        printf '%s\\n' visible > ${shellQuote(visiblePath)}
      else
        printf '%s\\n' missing > ${shellQuote(missingPath)}
      fi
      : > ${shellQuote(terminatedPath)}
      while ! test -f ${shellQuote(releasePath)}; do
        /bin/sleep 0.01
      done
      printf '%s\\n' reaped > ${shellQuote(reapedPath)}
    ) &
    : > ${shellQuote(startedPath)}
    printf '%2048s' x
    exec /bin/sleep 30
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
  const info = JSON.stringify({ host: { security: { rootless: true } } });
  const image = JSON.stringify([{
    Architecture: "amd64",
    Digest: DIGEST,
    Os: "linux",
  }]);
  return `#!/bin/sh
set -eu
case "$1" in
  info)
    printf '%s\\n' ${shellQuote(info)}
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
    printf '[{"Id":"%s","ImageDigest":"%s","Mounts":[{"Destination":"/session","Propagation":"rprivate","RW":true,"Source":"%s","Type":"bind"}],"Name":"%s","State":{"Pid":42001,"Running":true,"Status":"running"}}]\\n' \\
      ${shellQuote(containerId)} ${shellQuote(DIGEST)} \\
      ${shellQuote(HELD_MOUNT_SOURCE)} "$name"
    ;;
  *)
    exit 64
    ;;
esac
`;
}

function blockingStartPodmanScript({ pidPath, startedPath }) {
  const info = JSON.stringify({ host: { security: { rootless: true } } });
  const image = JSON.stringify([{
    Architecture: "amd64",
    Digest: DIGEST,
    Os: "linux",
  }]);
  return `#!/bin/sh
set -eu
case "$1" in
  info)
    printf '%s\\n' ${shellQuote(info)}
    ;;
  image)
    printf '%s\\n' ${shellQuote(image)}
    ;;
  create)
    printf '%s\\n' ${shellQuote(CONTAINER_ID)}
    ;;
  start)
    printf '%s\\n' "$$" > ${shellQuote(pidPath)}
    : > ${shellQuote(startedPath)}
    exec /bin/sleep 30
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
    measuredImage: measuredImage(),
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
  let running = false;
  let name = null;
  let mountSource = settings.inspectionSource ?? attachmentRoot;
  return async function commandRunner(executable, arguments_, options) {
    events.push({ arguments_, executable, options, receiver: this });
    if (arguments_[0] === "info") {
      return {
        stderr: "",
        stdout: `${JSON.stringify(settings.infoValue ?? {
          host: { security: { rootless: settings.rootless ?? true } },
        })}\n`,
      };
    }
    if (arguments_[0] === "image") {
      return {
        stderr: "",
        stdout: `${JSON.stringify(settings.imageInspection ?? [
          {
            Architecture: "amd64",
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
      if (settings.createOutput !== undefined) {
        return { stderr: "", stdout: settings.createOutput };
      }
      return { stderr: "", stdout: `${CONTAINER_ID}\n` };
    }
    if (arguments_[0] === "start") {
      if (settings.startReject) throw new Error("simulated ambiguous start");
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
        settings.inspectionStatus,
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
    writerCommand: Object.freeze(["/usr/local/bin/codex", "app-server"]),
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

  const create = base.events.find((event) => event.arguments_[0] === "create");
  assert.equal(create.executable, "/usr/bin/podman");
  assert.equal(create.receiver, undefined);
  assert.deepEqual(create.arguments_, [
    "create",
    "--name",
    create.arguments_[2],
    "--pull=never",
    "--read-only",
    "--security-opt=no-new-privileges",
    "--cap-drop=all",
    "--userns=keep-id",
    "--restart=no",
    "--mount",
    `type=bind,source=${HELD_MOUNT_SOURCE},target=/session,rw,bind-propagation=rprivate`,
    "--workdir",
    "/session",
    "--env",
    "CODEX_HOME=/session/.codex",
    "--env",
    "LANG=C.UTF-8",
    IMAGE_REFERENCE,
    "/usr/local/bin/codex",
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
  assert.deepEqual(live.events.map((event) => event.arguments_[0]), ["ps", "container"]);
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
    inspectionStatus: "configured",
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
        inspectionStatus: "configured",
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
    ["ps", "container"],
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
    ["ps", "container"],
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
    ["acquire", "verify-current", "verify-running-mount", "close"],
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
    ["acquire", "verify-current", "verify-running-mount", "close"],
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

test("oversized configured and requested roots fail before filesystem or Podman dispatch", async (t) => {
  const base = await fixture(t);
  const oversizedRoot = `/${"x".repeat(1_000_000)}`;
  assert.throws(
    () => createPodmanWriterSupervisor(exact({
      ...base.options,
      configuredAttachmentRoot: oversizedRoot,
    })),
    assertSupervisorError("invalid_podman_writer_supervisor_options"),
  );
  assert.equal(base.events.length, 0);
  assert.equal(base.filesystemEvents.length, 0);

  await assert.rejects(
    base.supervisor.launchWriter(launchInput(oversizedRoot)),
    assertSupervisorError("invalid_podman_writer_supervisor_request"),
  );
  assert.equal(base.events.length, 0);
  assert.equal(base.filesystemEvents.length, 0);
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

test("secure Linux authority rejects missing roots and final or ancestor symlink aliases", {
  skip: process.platform !== "linux" ||
    !existsSync("/usr/bin/getfacl") ||
    !existsSync("/usr/bin/setfacl"),
}, async (t) => {
  const base = await fixture(t, { useDefaultFilesystemAuthority: true });

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

  const inheritedError = Object.getOwnPropertyDescriptor(
    Object.prototype,
    "error",
  );
  Object.defineProperty(Object.prototype, "error", {
    configurable: true,
    value: new Error("inherited spawn result poison"),
    writable: true,
  });
  let claimReached = false;
  const poisonProbeState = exact({
    contractVersion: base.state.contractVersion,
    async claim() {
      claimReached = true;
      throw new Error("stop after default authority acquisition");
    },
    async read() {
      return null;
    },
    async transition() {
      throw new Error("unexpected transition");
    },
  });
  const poisonProbe = createPodmanWriterSupervisor(exact({
    ...base.options,
    state: poisonProbeState,
  }));
  try {
    await assert.rejects(
      poisonProbe.launchWriter(base.input),
      assertSupervisorError("podman_writer_supervisor_outcome_uncertain"),
    );
  } finally {
    if (inheritedError === undefined) delete Object.prototype.error;
    else Object.defineProperty(Object.prototype, "error", inheritedError);
  }
  // Reaching the state boundary proves inherited spawnSync result fields did
  // not replace the own getfacl status/stdout/stderr snapshot. Stop there so
  // unrelated filesystem calls do not run while Object.prototype is poisoned.
  assert.equal(claimReached, true);
});

test("enforces rootless execution, exact local digest, and bounded unambiguous output", async (t) => {
  const rootful = await fixture(t, { rootless: false });
  await assert.rejects(
    rootful.supervisor.launchWriter(rootful.input),
    assertSupervisorError("podman_writer_rootless_required"),
  );
  assert.equal(rootful.events.some((event) => event.arguments_[0] === "create"), false);

  const wrongImage = await fixture(t, {
    inspectedDigest: `sha256:${"f".repeat(64)}`,
  });
  await assert.rejects(
    wrongImage.supervisor.launchWriter(wrongImage.input),
    assertSupervisorError("podman_writer_image_mismatch"),
  );
  assert.equal(wrongImage.events.some((event) => event.arguments_[0] === "create"), false);

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

test("default runner holds filesystem authority until failed Podman children close", async (t) => {
  const scenarios = [
    { mode: "timeout", phase: "create" },
    { mode: "abort", phase: "create" },
    { mode: "stdout-overflow", phase: "create" },
    { mode: "stderr-overflow", phase: "create" },
  ];

  for (const scenario of scenarios) {
    await t.test(`${scenario.mode} during ${scenario.phase}`, async (t) => {
      let closeCount = 0;
      let reapedPath;
      let releasePath;
      let startedPath;
      let terminatedPath;
      const controller = new AbortController();
      const base = await fixture(t, {
        commandTimeoutMilliseconds: scenario.mode === "timeout" ? 2_000 : 10_000,
        defaultCommandRunnerScript({ parent }) {
          reapedPath = join(parent, "podman-child-reaped");
          releasePath = join(parent, "podman-child-release");
          startedPath = join(parent, "podman-child-started");
          terminatedPath = join(parent, "podman-child-terminated");
          return delayedReapPodmanScript({
            mode: scenario.mode,
            phase: scenario.phase,
            reapedPath,
            releasePath,
            startedPath,
            terminatedPath,
          });
        },
        filesystemSettings: {
          onClose() {
            closeCount += 1;
            assert.equal(existsSync(reapedPath), true);
          },
        },
        maxOutputBytes: 1_024,
        stopTimeoutSeconds: 1,
        useDefaultCommandRunner: true,
      });
      let settled = false;
      const pending = base.supervisor.launchWriter(
        launchInput(base.attachmentRoot, { signal: controller.signal }),
      ).then(
        () => {
          settled = true;
          assert.fail("failed Podman launch unexpectedly succeeded");
        },
        (error) => {
          settled = true;
          return error;
        },
      );

      await waitForPath(startedPath);
      if (scenario.mode === "abort") controller.abort();
      try {
        await waitForPath(terminatedPath);
        assert.equal(existsSync(reapedPath), false);
        assert.equal(settled, false);
        assert.equal(closeCount, 0);
      } finally {
        await writeFile(releasePath, "release\n", { mode: 0o600 });
      }

      const error = await pending;
      assert.equal(error instanceof PodmanWriterSupervisorError, true);
      assert.equal(
        error.code,
        scenario.mode === "abort"
          ? "podman_writer_supervisor_aborted"
          : "podman_writer_supervisor_outcome_uncertain",
      );
      assert.equal(existsSync(reapedPath), true);
      assert.equal(closeCount, 1);
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

test("default runner captures output for non-full Podman start identities", async (t) => {
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

  const pending = base.supervisor.launchWriter(base.input);
  await waitForPath(holdingPath);
  try {
    await writeFile(releasePath, "release\n", { mode: 0o600 });
    await assert.rejects(
      pending,
      assertSupervisorError("podman_writer_supervisor_outcome_uncertain"),
    );
  } finally {
    if (!existsSync(releasePath)) {
      await writeFile(releasePath, "release\n", { mode: 0o600 });
    }
  }
  await waitForPath(releasedPath);
});

test("default runner reaps an aborted outputless Podman start before closing authority", async (t) => {
  const controller = new AbortController();
  let closeCount = 0;
  let pidPath;
  let startedPath;
  const base = await fixture(t, {
    commandTimeoutMilliseconds: 10_000,
    defaultCommandRunnerScript({ parent }) {
      pidPath = join(parent, "podman-aborted-start-pid");
      startedPath = join(parent, "podman-aborted-started");
      return blockingStartPodmanScript({ pidPath, startedPath });
    },
    filesystemSettings: {
      onClose() {
        closeCount += 1;
        const pid = Number(execFileSync("/bin/cat", [pidPath], {
          encoding: "utf8",
        }).trim());
        assert.throws(
          () => process.kill(pid, 0),
          (error) => error?.code === "ESRCH",
        );
      },
    },
    stopTimeoutSeconds: 1,
    useDefaultCommandRunner: true,
  });

  const pending = base.supervisor.launchWriter(
    launchInput(base.attachmentRoot, { signal: controller.signal }),
  );
  await waitForPath(startedPath);
  controller.abort();
  await assert.rejects(
    pending,
    assertSupervisorError("podman_writer_supervisor_aborted"),
  );
  assert.equal(closeCount, 1);
});

test("Linux default runner keeps the real attachment fd visible through reap", {
  skip: process.platform !== "linux",
}, async (t) => {
  let heldFd = null;
  let missingPath;
  let reapedPath;
  let releasePath;
  let startedPath;
  let terminatedPath;
  let visiblePath;
  const base = await fixture(t, {
    defaultCommandRunnerScript({ parent }) {
      missingPath = join(parent, "attachment-fd-missing");
      reapedPath = join(parent, "podman-child-reaped");
      releasePath = join(parent, "podman-child-release");
      startedPath = join(parent, "podman-child-started");
      terminatedPath = join(parent, "podman-child-terminated");
      visiblePath = join(parent, "attachment-fd-visible");
      return heldFdReapPodmanScript({
        missingPath,
        reapedPath,
        releasePath,
        startedPath,
        terminatedPath,
        visiblePath,
      });
    },
    filesystemSettings: {
      mountSource(input) {
        assert.equal(input.attachment.rootPath.endsWith("/attachment"), true);
        return `/proc/${process.pid}/fd/${heldFd}`;
      },
      onAcquire(input) {
        heldFd = openSync(
          input.attachment.rootPath,
          fsConstants.O_RDONLY | fsConstants.O_DIRECTORY | fsConstants.O_NOFOLLOW,
        );
      },
      onClose() {
        assert.equal(existsSync(reapedPath), true);
        assert.equal(existsSync(visiblePath), true);
        assert.equal(existsSync(missingPath), false);
        closeSync(heldFd);
        heldFd = null;
      },
    },
    maxOutputBytes: 1_024,
    stopTimeoutSeconds: 1,
    useDefaultCommandRunner: true,
  });
  t.after(() => {
    if (heldFd !== null) closeSync(heldFd);
  });

  let settled = false;
  const pending = base.supervisor.launchWriter(base.input).then(
    () => {
      settled = true;
      assert.fail("overflowing Podman launch unexpectedly succeeded");
    },
    (error) => {
      settled = true;
      return error;
    },
  );
  await waitForPath(startedPath);
  try {
    await waitForPath(terminatedPath);
    assert.equal(settled, false);
    assert.equal(heldFd !== null, true);
    assert.equal(existsSync(visiblePath), true);
    assert.equal(existsSync(missingPath), false);
    assert.equal(existsSync(reapedPath), false);
  } finally {
    await writeFile(releasePath, "release\n", { mode: 0o600 });
  }

  const error = await pending;
  assert.equal(error instanceof PodmanWriterSupervisorError, true);
  assert.equal(error.code, "podman_writer_supervisor_outcome_uncertain");
  assert.equal(existsSync(visiblePath), true);
  assert.equal(existsSync(missingPath), false);
  assert.equal(heldFd, null);
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
        stdout: `${JSON.stringify({ host: { security: { rootless: true } } })}\n`,
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
    install(Object.prototype, "rootless", true);
    const inheritedInfo = await fixture(t, {
      infoValue: { host: { security: {} } },
    });
    await assert.rejects(
      inheritedInfo.supervisor.launchWriter(inheritedInfo.input),
      assertSupervisorError("podman_writer_output_invalid"),
    );
    restore();

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
