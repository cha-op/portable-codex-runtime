import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { writeSync } from "node:fs";
import { lstat, mkdir, readFile, rm } from "node:fs/promises";
import { setTimeout as delay } from "node:timers/promises";
import { promisify } from "node:util";
import test from "node:test";

import {
  PODMAN_WRITER_SUPERVISOR_CONTRACT_VERSION,
  createPodmanWriterSupervisor,
} from "../src/podman-writer-supervisor.mjs";
import {
  createPodmanWriterSupervisorState,
} from "../src/podman-writer-supervisor-state.mjs";

const execFileAsync = promisify(execFile);
const DIGEST = process.env.PODMAN_WRITER_IMAGE_DIGEST;
const IMAGE_REFERENCE = process.env.PODMAN_WRITER_IMAGE_REFERENCE;
const ROOT = process.env.PODMAN_WRITER_TEST_ROOT;
const PODMAN = process.env.PODMAN_EXECUTABLE ?? "/usr/bin/podman";
const JOURNALCTL = "/usr/bin/journalctl";
const SUDO = "/usr/bin/sudo";
const SESSION_ID = "019f2100-0000-7000-8000-000000000001";
const SUPERVISOR_ID = "podman-linux-integration-v1";
const TEST_ROOT_PREFIX = "/var/tmp/portable-codex-runtime-podman-";
const WATCHDOG_DIAGNOSTIC_MILLISECONDS = 45_000;
const WATCHDOG_HARD_BACKSTOP_MILLISECONDS = 65_000;
const WATCHDOG_JOURNAL_BYTES = 64 * 1024;
const WATCHDOG_PROBE_MILLISECONDS = 5_000;

function exact(value) {
  return Object.freeze(Object.assign(Object.create(null), value));
}

function measuredImage() {
  return exact({
    projection: exact({
      codexSandbox: "workspace-write",
      codexVersion: "1.0.0",
      platformImage: exact({
        architecture: "amd64",
        config: exact({
          digest: `sha256:${"b".repeat(64)}`,
          mediaType: "application/vnd.oci.image.config.v1+json",
          size: 1,
        }),
        digest: DIGEST,
        mediaType: "application/vnd.oci.image.manifest.v1+json",
        os: "linux",
        size: 1,
      }),
    }),
    runtimeIdentity: exact({
      codexBinaryPath: "/usr/local/bin/writer",
      codexBinarySha256: "c".repeat(64),
      codexVersion: "1.0.0",
      platformImageDigest: DIGEST,
    }),
  });
}

function launchInput(attachmentRoot) {
  const attachment = exact({
    attachmentId: "podman-attachment-001",
    backendId: "linux-ext4-physical-v1",
    contractVersion: 1,
    fencingEpoch: "1",
    holderId: "podman-holder-001",
    kind: "directory",
    leaseId: "podman-lease-001",
    mode: "read-write",
    operationId: "podman-attachment-operation-001",
    proofId: "podman-attachment-proof-001",
    rootPath: attachmentRoot,
    sessionId: SESSION_ID,
    storageId: "podman-storage-001",
  });
  const lease = exact({
    contractVersion: 1,
    expiresAt: "2030-01-01T00:00:00.000Z",
    fencingEpoch: "1",
    holderId: attachment.holderId,
    leaseId: attachment.leaseId,
    sessionId: SESSION_ID,
  });
  const generationReference = exact({
    bindingSha256: "1".repeat(64),
    checkpointId: "podman-checkpoint-001",
    claimedAt: "2026-08-14T09:00:00.000Z",
    committedAt: "2026-08-14T09:01:00.000Z",
    documentSha256: "2".repeat(64),
    generationId: "podman-generation-001",
    operationId: "podman-generation-operation-001",
    sessionId: SESSION_ID,
    state: "committed",
  });
  const request = exact({
    attachment,
    contractVersion: 1,
    fencingEpoch: "1",
    generation: generationReference,
    lease,
    measuredImage: measuredImage(),
    supervisor: exact({ contractVersion: 1, supervisorId: SUPERVISOR_ID }),
  });
  const session = exact({
    createdAt: "2026-08-14T08:00:00.000Z",
    document: exact({
      activeOperation: exact({ operationId: "podman-launch-attempt-001" }),
      attachment,
      backendCapabilities: exact({ exclusiveWriterAttachment: true }),
      documentVersion: 3,
      lastOperation: null,
      launch: null,
      lease,
      lifecycle: "ATTACHED",
      manifest: exact({ runtime: exact({ imageDigest: DIGEST }) }),
      recovery: null,
      storageRef: exact({ storageId: attachment.storageId }),
      writerEpoch: "1",
    }),
    revision: "10",
    sessionId: SESSION_ID,
    updatedAt: "2026-08-14T10:00:00.000Z",
  });
  const attempt = exact({
    contractVersion: 1,
    launchAttemptId: "podman-launch-attempt-001",
    request,
    result: null,
    state: "starting",
  });
  const operation = exact({
    conflictClass: "session-mutation",
    createdAt: "2026-08-14T09:59:00.000Z",
    expectedSession: session,
    kind: "writer-launch-attempt-v1",
    operationId: attempt.launchAttemptId,
    request,
    requestSha256: "d".repeat(64),
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
    kind: operation.kind,
    operationId: operation.operationId,
    releasedAt: null,
    requestSha256: operation.requestSha256,
    reservationId: "podman-reservation-001",
    sessionId: SESSION_ID,
    state: "starting",
    updatedAt: "2026-08-14T10:00:00.000Z",
  });
  return exact({
    attempt,
    authorityNow: "2026-08-14T10:00:01.000Z",
    consumedImage: request.measuredImage,
    contractVersion: PODMAN_WRITER_SUPERVISOR_CONTRACT_VERSION,
    generation: exact({
      binding: exact({ provider: "podman-integration" }),
      checkpointId: generationReference.checkpointId,
      claimedAt: generationReference.claimedAt,
      committedAt: generationReference.committedAt,
      document: exact({ status: "committed" }),
      generationId: generationReference.generationId,
      operationId: generationReference.operationId,
      sessionId: SESSION_ID,
      state: "committed",
    }),
    invocation: exact({}),
    operation,
    reservation,
    session,
    signal: new AbortController().signal,
  });
}

function stopInput(input, receipt) {
  return exact({
    attachment: input.attempt.request.attachment,
    contractVersion: PODMAN_WRITER_SUPERVISOR_CONTRACT_VERSION,
    invocation: exact({}),
    processIncarnationId: receipt.evidence.processIncarnationId,
    signal: new AbortController().signal,
    stopOperationId: "podman-stop-operation-001",
    writerFence: exact({
      contractVersion: 1,
      fencingEpoch: input.attempt.request.lease.fencingEpoch,
      holderId: input.attempt.request.lease.holderId,
      leaseId: input.attempt.request.lease.leaseId,
      sessionId: SESSION_ID,
    }),
    writerIncarnationId: receipt.evidence.writerIncarnationId,
  });
}

function reconcileInput(input) {
  return exact({
    attempt: exact({ ...input.attempt, state: "uncertain" }),
    contractVersion: PODMAN_WRITER_SUPERVISOR_CONTRACT_VERSION,
    invocation: exact({}),
    launch: null,
    operation: exact({ ...input.operation, revision: "2", state: "uncertain" }),
    reservation: exact({ ...input.reservation, state: "uncertain" }),
    session: input.session,
    signal: new AbortController().signal,
  });
}

async function waitForReadyMarker(path) {
  const deadline = Date.now() + 10_000;
  while (true) {
    try {
      const contents = await readFile(path, "utf8");
      assert.equal(contents, "ready\n");
      return;
    } catch (error) {
      if (error?.code !== "ENOENT" || Date.now() >= deadline) throw error;
    }
    await delay(50);
  }
}

async function observeCreatedContainer() {
  try {
    const observed = await execFileAsync(
      PODMAN,
      [
        "--remote=false",
        "ps",
        "-a",
        "--filter",
        `ancestor=${IMAGE_REFERENCE}`,
        "--format=json",
      ],
      {
        cwd: "/",
        env: process.env,
        killSignal: "SIGKILL",
        maxBuffer: 64 * 1024,
        timeout: 5_000,
      },
    );
    const parsed = JSON.parse(observed.stdout);
    if (!Array.isArray(parsed)) return "unreadable";
    if (parsed.length === 0) return "absent";
    if (parsed.length !== 1) return "ambiguous";
    const descriptor = Object.getOwnPropertyDescriptor(parsed[0], "State");
    if (
      descriptor === undefined ||
      !Object.hasOwn(descriptor, "value") ||
      typeof descriptor.value !== "string"
    ) {
      return "unreadable";
    }
    return descriptor.value === "created" ? "created" : "other-state";
  } catch {
    return "unreadable";
  }
}

async function observeDurableStatus(supervisorState) {
  if (supervisorState === null) return "unavailable";
  try {
    const record = await supervisorState.read(exact({
      launchAttemptId: "podman-launch-attempt-001",
    }));
    if (record === null) return "absent";
    switch (record.status) {
      case "preparing":
      case "created":
      case "started":
      case "stopping":
      case "stopped":
        return record.status;
      default:
        return "unreadable";
    }
  } catch {
    return "unreadable";
  }
}

function containerObservation(
  ps,
  inspect = "not-run",
  running = "unreadable",
  pid = "unreadable",
  stateError = "unreadable",
  exitCode = "unreadable",
  conmonPid = "unreadable",
  errorRuntime = "unreadable",
  errorOperation = "unreadable",
  errorErrno = "unreadable",
  ociConfig = "unreadable",
  ociRuntime = "unreadable",
  cgroupManager = "unreadable",
  conmonOutcome = "unreadable",
  conmonStage = "unreadable",
  conmonErrno = "unreadable",
) {
  return {
    cgroupManager,
    conmonErrno,
    conmonOutcome,
    conmonPid,
    conmonStage,
    errorErrno,
    errorOperation,
    errorRuntime,
    exitCode,
    inspect,
    ociConfig,
    ociRuntime,
    pid,
    ps,
    running,
    stateError,
  };
}

function classifyStateError(value) {
  if (typeof value !== "string") return "unreadable";
  if (value === "") return "empty";
  const normalized = value.toLowerCase();
  const mentionsProcFd = /\/proc\/(?:self|[1-9][0-9]*)\/fd\/(?:0|[1-9][0-9]*)/u
    .test(value);
  const accessDenied = normalized.includes("permission denied") ||
    normalized.includes("operation not permitted") ||
    normalized.includes("access denied");
  const missing = normalized.includes("no such file or directory") ||
    normalized.includes("not a directory") ||
    normalized.includes("does not exist");
  const ociRuntimeToken = /(?:^|[^a-z0-9_])(?:crun|runc)(?:$|[^a-z0-9_])/u
    .test(normalized);
  const ociContext = ociRuntimeToken ||
    normalized.includes("oci runtime") ||
    normalized.includes("conmon") ||
    normalized.includes("container create") ||
    normalized.includes("mount") ||
    normalized.includes("/session");
  if (mentionsProcFd && accessDenied) return "procfd-access-denied";
  if (mentionsProcFd && missing) return "procfd-missing";
  if (mentionsProcFd) return "procfd-other";
  if (normalized.includes("container creation timeout")) return "oci-timeout";
  if (
    normalized.includes("conmon") &&
    (normalized.includes("no log") || normalized.includes("did not provide"))
  ) {
    return "conmon-no-logs";
  }
  if (accessDenied && ociContext) return "oci-access-denied";
  if (missing && ociContext) return "oci-missing";
  if (ociContext) return "oci-other";
  return "non-oci";
}

function classifyExitCode(value) {
  if (
    !Number.isSafeInteger(value) ||
    value < -2_147_483_648 ||
    value > 2_147_483_647
  ) {
    return "unreadable";
  }
  return value === 0 ? "zero" : "nonzero";
}

function classifyProcessIdentifier(value) {
  if (!Number.isSafeInteger(value) || value < 0) return "unreadable";
  return value === 0 ? "zero" : "positive";
}

function classifyOciRuntime(value) {
  if (typeof value !== "string" || value === "") return "unreadable";
  const normalized = value.toLowerCase();
  if (/(?:^|[^a-z0-9_])crun(?:$|[^a-z0-9_])/u.test(normalized)) {
    return "crun";
  }
  if (/(?:^|[^a-z0-9_])runc(?:$|[^a-z0-9_])/u.test(normalized)) {
    return "runc";
  }
  return "other";
}

function classifyCgroupManager(value) {
  if (typeof value !== "string" || value === "") return "unreadable";
  if (value === "systemd" || value === "cgroupfs") return value;
  return "other";
}

function classifyConmonOutcome(value) {
  if (typeof value !== "string") return "unreadable";
  const normalized = value.toLowerCase();
  if (normalized.includes("configuring conmon env:")) return "env";
  if (normalized.includes("fork/exec") && normalized.includes("conmon")) {
    return "spawn-failed";
  }
  if (normalized.endsWith("conmon failed: exit status 1")) {
    return "wait-exit-one";
  }
  if (/conmon failed: exit status [1-9][0-9]*$/u.test(normalized)) {
    return "wait-exit-other";
  }
  if (normalized.endsWith("conmon failed: signal: killed")) {
    return "wait-sigkill";
  }
  if (
    normalized.endsWith("conmon failed: signal: segmentation fault") ||
    normalized.endsWith(
      "conmon failed: signal: segmentation fault (core dumped)",
    )
  ) {
    return "wait-sigsegv";
  }
  if (normalized.includes("conmon failed: signal:")) {
    return "wait-signal-other";
  }
  return normalized.includes("conmon failed:") ? "wait-other" : "other";
}

function classifyConmonFatalErrno(value) {
  const normalized = value.toLowerCase();
  if (normalized.endsWith(" permission denied")) return "eacces";
  if (normalized.endsWith(" operation not permitted")) return "eperm";
  if (normalized.endsWith(" no such file or directory")) return "enoent";
  if (normalized.endsWith(" not a directory")) return "enotdir";
  if (normalized.endsWith(" invalid argument")) return "einval";
  if (normalized.endsWith(" bad file descriptor")) return "ebadf";
  if (normalized.endsWith(" too many open files in system")) return "enfile";
  if (normalized.endsWith(" too many open files")) return "emfile";
  if (normalized.endsWith(" resource temporarily unavailable")) return "eagain";
  if (normalized.endsWith(" cannot allocate memory")) return "enomem";
  if (normalized.endsWith(" no space left on device")) return "enospc";
  if (normalized.endsWith(" read-only file system")) return "erofs";
  return "unknown";
}

function classifyConmonFatalMessage(value, containerId) {
  if (
    typeof value !== "string" ||
    typeof containerId !== "string" ||
    !/^[0-9a-f]{64}$/u.test(containerId)
  ) {
    return null;
  }
  const prefix = `conmon ${containerId.slice(0, 20)} <error>:`;
  if (!value.startsWith(prefix)) return null;
  const normalized = value.slice(prefix.length).trim().toLowerCase();
  let conmonStage = "unknown";
  let noErrno = false;
  if (
    normalized === "container uuid not provided. use --cuuid" ||
    normalized === "cannot use 'exec' and 'restore' at the same time" ||
    normalized === "attach can only be specified with exec" ||
    normalized === "attach can only be specified for a non-legacy exec session" ||
    normalized === "exec process spec path not provided. use --exec-process-spec" ||
    normalized === "delay before invoking exit command must be greater than or equal to 0"
  ) {
    conmonStage = "cli";
    noErrno = true;
  } else if (
    normalized === "runtime path not provided. use --runtime"
  ) {
    conmonStage = "runtime-path";
    noErrno = true;
  } else if (
    normalized.startsWith("runtime path ") && normalized.includes(" is not valid")
  ) {
    conmonStage = "runtime-path";
  } else if (normalized === "failed to get working directory") {
    conmonStage = "cwd";
    noErrno = true;
  } else if (
    normalized === "log driver not provided. use --log-path" ||
    normalized === "k8s-file doesn't support --log-tag" ||
    normalized === "include journald in compilation path to log to systemd journal" ||
    normalized === "container id must be provided and of the correct length" ||
    normalized === "container id must be longer than 12 characters" ||
    normalized === "log-path must not be empty" ||
    normalized === "k8s-file requires a filename" ||
    normalized.startsWith("no such log level ") ||
    normalized.startsWith("no such log driver ")
  ) {
    conmonStage = "log-config";
    noErrno = true;
  } else if (normalized.startsWith("failed to open log file ")) {
    conmonStage = "log-open";
  } else if (
    normalized.startsWith("unable to parse _oci_startpipe") ||
    normalized.startsWith("unable to make _oci_startpipe cloexec") ||
    normalized.startsWith("start-pipe read failed")
  ) {
    conmonStage = "start-pipe";
  } else if (normalized.startsWith("failed to open /dev/null ")) {
    conmonStage = "devnull";
  } else if (normalized.startsWith("failed to fork the create command ")) {
    conmonStage = "fork";
  } else if (normalized.startsWith("failed to write conmon pidfile:")) {
    conmonStage = "pidfile";
  }
  return exact({
    conmonErrno: noErrno ? "none" : classifyConmonFatalErrno(normalized),
    conmonStage,
  });
}

function classifyConmonJournalOutput(value, containerId, expectedUid) {
  if (
    typeof value !== "string" ||
    !Number.isSafeInteger(expectedUid) ||
    expectedUid < 1
  ) {
    return exact({ conmonErrno: "unreadable", conmonStage: "unreadable" });
  }
  const classifiedRecords = [];
  const lines = value.split("\n");
  for (const line of lines) {
    if (line === "") continue;
    let record;
    try {
      record = JSON.parse(line);
    } catch {
      return exact({ conmonErrno: "unreadable", conmonStage: "unreadable" });
    }
    if (record === null || typeof record !== "object" || Array.isArray(record)) {
      return exact({ conmonErrno: "unreadable", conmonStage: "unreadable" });
    }
    const comm = Object.getOwnPropertyDescriptor(record, "_COMM");
    const transport = Object.getOwnPropertyDescriptor(record, "_TRANSPORT");
    const uid = Object.getOwnPropertyDescriptor(record, "_UID");
    const message = Object.getOwnPropertyDescriptor(record, "MESSAGE");
    const syslogIdentifier = Object.getOwnPropertyDescriptor(
      record,
      "SYSLOG_IDENTIFIER",
    );
    if (
      transport === undefined ||
      !Object.hasOwn(transport, "value") ||
      transport.value !== "syslog" ||
      uid === undefined ||
      !Object.hasOwn(uid, "value") ||
      uid.value !== String(expectedUid) ||
      message === undefined ||
      !Object.hasOwn(message, "value") ||
      typeof message.value !== "string" ||
      syslogIdentifier === undefined ||
      !Object.hasOwn(syslogIdentifier, "value") ||
      syslogIdentifier.value !== "conmon"
    ) {
      return exact({ conmonErrno: "unreadable", conmonStage: "unreadable" });
    }
    if (
      comm !== undefined &&
      (!Object.hasOwn(comm, "value") || comm.value !== "conmon")
    ) {
      return exact({ conmonErrno: "unreadable", conmonStage: "unreadable" });
    }
    const classified = classifyConmonFatalMessage(message.value, containerId);
    if (classified === null) continue;
    classifiedRecords.push(classified);
  }
  if (classifiedRecords.length === 0) {
    return exact({ conmonErrno: "absent", conmonStage: "absent" });
  }
  const pidfileRecords = classifiedRecords.filter(
    (record) => record.conmonStage === "pidfile",
  );
  const selectedRecords = pidfileRecords.length > 0
    ? pidfileRecords
    : classifiedRecords;
  const stages = new Set(selectedRecords.map((record) => record.conmonStage));
  const errnos = new Set(selectedRecords.map((record) => record.conmonErrno));
  return exact({
    conmonErrno: errnos.size === 1 ? [...errnos][0] : "ambiguous",
    conmonStage: stages.size === 1 ? [...stages][0] : "ambiguous",
  });
}

function classifyStateErrorAxes(value) {
  if (typeof value !== "string") {
    return {
      errorErrno: "unreadable",
      errorOperation: "unreadable",
      errorRuntime: "unreadable",
    };
  }
  const normalized = value.toLowerCase();
  const crunToken = /(?:^|[^a-z0-9_])crun(?:$|[^a-z0-9_])/u.test(normalized);
  const runcToken = /(?:^|[^a-z0-9_])runc(?:$|[^a-z0-9_])/u.test(normalized);
  let errorRuntime = "unknown";
  if (crunToken) {
    errorRuntime = "crun";
  } else if (runcToken) {
    errorRuntime = "runc";
  } else if (normalized.includes("conmon")) {
    errorRuntime = "conmon";
  } else if (normalized.includes("oci runtime")) {
    errorRuntime = "oci";
  } else if (
    normalized.includes("container creation timeout") ||
    normalized.includes("container create failed")
  ) {
    errorRuntime = "podman";
  }

  const mentionsProcFd = /\/proc\/(?:self|[1-9][0-9]*)\/fd\/(?:0|[1-9][0-9]*)/u
    .test(value);
  let errorOperation = "unknown";
  if (mentionsProcFd) {
    errorOperation = "procfd";
  } else if (
    normalized.includes("cgroup") ||
    normalized.includes("controller") ||
    normalized.includes("sd-bus") ||
    normalized.includes("systemd unit")
  ) {
    errorOperation = "cgroup";
  } else if (
    normalized.includes("user namespace") ||
    normalized.includes("userns") ||
    normalized.includes("uid_map") ||
    normalized.includes("gid_map") ||
    normalized.includes("newuidmap") ||
    normalized.includes("newgidmap") ||
    normalized.includes("clone_newuser") ||
    normalized.includes("setresuid") ||
    normalized.includes("setresgid") ||
    normalized.includes("setgroups") ||
    normalized.includes("uid mapping") ||
    normalized.includes("gid mapping") ||
    normalized.includes("invalid mapping specified") ||
    normalized.includes("relative mapping") ||
    normalized.includes("no mappings found")
  ) {
    errorOperation = "userns";
  } else if (
    /(?:^|[^a-z0-9_])setns(?:$|[^a-z0-9_])/u.test(normalized) ||
    /(?:^|[^a-z0-9_])unshare(?:$|[^a-z0-9_])/u.test(normalized) ||
    normalized.includes("invalid namespace type")
  ) {
    errorOperation = "namespace";
  } else if (
    normalized.includes("seccomp") ||
    normalized.includes("apparmor") ||
    normalized.includes("selinux") ||
    normalized.includes("keyring") ||
    normalized.includes("no-new-privileges") ||
    normalized.includes("no new privs") ||
    normalized.includes("capset") ||
    normalized.includes("capabilit") ||
    normalized.includes("/attr/")
  ) {
    errorOperation = "security";
  } else if (
    normalized.includes("network namespace") ||
    normalized.includes("netns") ||
    normalized.includes("slirp") ||
    normalized.includes("pasta") ||
    normalized.includes("netavark") ||
    normalized.includes("cni")
  ) {
    errorOperation = "network";
  } else if (
    normalized.includes("exec container process") ||
    normalized.includes("chdir to `/session`") ||
    normalized.includes("executable") ||
    normalized.includes("working directory") ||
    normalized.includes("oom_score_adj")
  ) {
    errorOperation = "process";
  } else if (
    normalized.includes("rootfs") ||
    normalized.includes("pivot_root") ||
    normalized.includes("chroot") ||
    normalized.includes("fchdir") ||
    normalized.includes("chdir to ") ||
    normalized.includes("oldroot")
  ) {
    errorOperation = "rootfs";
  } else if (
    /(?:^|[^a-z0-9_])mount(?:$|[^a-z0-9_])/u.test(normalized) ||
    normalized.includes("set propagation for") ||
    normalized.includes("rootfs propagation") ||
    normalized.includes("mount_setattr") ||
    normalized.includes("mounting ") ||
    normalized.includes("remount") ||
    /(?:^|[^a-z0-9_])umount(?:$|[^a-z0-9_])/u.test(normalized) ||
    (normalized.includes("make `") && normalized.includes("` private")) ||
    normalized.includes("open_tree") ||
    normalized.includes("fsmount") ||
    normalized.includes("fsopen") ||
    normalized.includes("move_mount")
  ) {
    errorOperation = "mount";
  } else if (
    normalized.includes("openat2") ||
    normalized.includes("cannot resolve") ||
    normalized.includes("statfs") ||
    normalized.includes("open mount target")
  ) {
    errorOperation = "path";
  } else if (
    normalized.includes("no logs from conmon") ||
    normalized.includes("conmon bytes") ||
    normalized.includes("sync pipe") ||
    normalized.includes("init process")
  ) {
    errorOperation = "conmon-sync";
  } else if (
    normalized.includes("container create") ||
    normalized.includes("container creation")
  ) {
    errorOperation = "create";
  }

  let errorErrno = normalized === "" ? "none" : "unknown";
  if (normalized.includes("permission denied")) {
    errorErrno = "eacces";
  } else if (normalized.includes("operation not permitted")) {
    errorErrno = "eperm";
  } else if (normalized.includes("no such file or directory")) {
    errorErrno = "enoent";
  } else if (normalized.includes("not a directory")) {
    errorErrno = "enotdir";
  } else if (normalized.includes("invalid argument")) {
    errorErrno = "einval";
  } else if (normalized.includes("bad file descriptor")) {
    errorErrno = "ebadf";
  } else if (normalized.includes("device or resource busy")) {
    errorErrno = "ebusy";
  } else if (normalized.includes("read-only file system")) {
    errorErrno = "erofs";
  } else if (normalized.includes("operation not supported")) {
    errorErrno = "enotsup";
  } else if (normalized.includes("no space left on device")) {
    errorErrno = "enospc";
  } else if (normalized.includes("function not implemented")) {
    errorErrno = "enosys";
  } else if (normalized.includes("invalid cross-device link")) {
    errorErrno = "exdev";
  } else if (normalized.includes("too many levels of symbolic links")) {
    errorErrno = "eloop";
  } else if (normalized.includes("cannot allocate memory")) {
    errorErrno = "enomem";
  } else if (normalized.includes("too many open files in system")) {
    errorErrno = "enfile";
  } else if (normalized.includes("too many open files")) {
    errorErrno = "emfile";
  } else if (normalized.includes("resource temporarily unavailable")) {
    errorErrno = "eagain";
  } else if (normalized.includes("text file busy")) {
    errorErrno = "etxtbsy";
  } else if (normalized.includes("exec format error")) {
    errorErrno = "enoexec";
  } else if (normalized.includes("container creation timeout")) {
    errorErrno = "timeout";
  }
  return { errorErrno, errorOperation, errorRuntime };
}

async function captureConmonJournalBoundary() {
  const sinceEpochSeconds = Math.max(0, Math.floor(Date.now() / 1000) - 1);
  try {
    const captured = await execFileAsync(
      SUDO,
      [
        "--non-interactive",
        JOURNALCTL,
        "--boot",
        "--no-pager",
        "--quiet",
        "--show-cursor",
        "--lines=0",
      ],
      {
        cwd: "/",
        env: process.env,
        killSignal: "SIGKILL",
        maxBuffer: 4 * 1024,
        timeout: WATCHDOG_PROBE_MILLISECONDS,
      },
    );
    const match = /^-- cursor: ([!-~]{1,2048})\r?\n?$/u.exec(captured.stdout);
    return exact({
      cursor: match === null ? null : match[1],
      sinceEpochSeconds,
    });
  } catch {
    return exact({ cursor: null, sinceEpochSeconds });
  }
}

async function observeConmonJournal(boundary, containerId) {
  const journalArguments = [
    "--non-interactive",
    JOURNALCTL,
    "--boot",
    "--no-pager",
    "--quiet",
    "--output=json",
    "--output-fields=MESSAGE,_COMM,_TRANSPORT,_UID,SYSLOG_IDENTIFIER",
    "--priority=err",
    "--lines=128",
  ];
  if (boundary.cursor === null) {
    journalArguments.push(`--since=@${boundary.sinceEpochSeconds}`);
    journalArguments.push(`--until=@${Math.floor(Date.now() / 1000) + 1}`);
  } else {
    journalArguments.push(`--after-cursor=${boundary.cursor}`);
  }
  const expectedUid = process.getuid();
  journalArguments.push(`_UID=${expectedUid}`);
  journalArguments.push("_TRANSPORT=syslog");
  journalArguments.push("SYSLOG_IDENTIFIER=conmon");
  try {
    const observed = await execFileAsync(SUDO, journalArguments, {
      cwd: "/",
      env: process.env,
      killSignal: "SIGKILL",
      maxBuffer: WATCHDOG_JOURNAL_BYTES,
      timeout: WATCHDOG_PROBE_MILLISECONDS,
    });
    return classifyConmonJournalOutput(
      observed.stdout,
      containerId,
      expectedUid,
    );
  } catch {
    return exact({ conmonErrno: "unreadable", conmonStage: "unreadable" });
  }
}

async function observeUniqueImageContainer(journalBoundary) {
  let listed;
  try {
    listed = await execFileAsync(
      PODMAN,
      [
        "--remote=false",
        "ps",
        "-a",
        "--no-trunc",
        "--filter",
        `ancestor=${IMAGE_REFERENCE}`,
        "--format=json",
      ],
      {
        cwd: "/",
        env: process.env,
        killSignal: "SIGKILL",
        maxBuffer: 64 * 1024,
        timeout: WATCHDOG_PROBE_MILLISECONDS,
      },
    );
  } catch {
    return containerObservation("unreadable");
  }
  let containers;
  try {
    containers = JSON.parse(listed.stdout);
  } catch {
    return containerObservation("unreadable");
  }
  if (!Array.isArray(containers)) {
    return containerObservation("unreadable");
  }
  if (containers.length === 0) return containerObservation("absent");
  if (containers.length !== 1) {
    return containerObservation("ambiguous");
  }
  const container = containers[0];
  if (
    container === null ||
    typeof container !== "object" ||
    Array.isArray(container)
  ) {
    return containerObservation("unique", "unreadable");
  }
  const idDescriptor = Object.getOwnPropertyDescriptor(container, "Id");
  if (
    idDescriptor === undefined ||
    !Object.hasOwn(idDescriptor, "value") ||
    typeof idDescriptor.value !== "string" ||
    !/^[0-9a-f]{64}$/u.test(idDescriptor.value)
  ) {
    return containerObservation("unique", "unreadable");
  }
  let inspected;
  try {
    inspected = await execFileAsync(
      PODMAN,
      [
        "--remote=false",
        "container",
        "inspect",
        "--format=json",
        idDescriptor.value,
      ],
      {
        cwd: "/",
        env: process.env,
        killSignal: "SIGKILL",
        maxBuffer: 64 * 1024,
        timeout: WATCHDOG_PROBE_MILLISECONDS,
      },
    );
  } catch {
    return containerObservation("unique", "unreadable");
  }
  let inspection;
  try {
    inspection = JSON.parse(inspected.stdout);
  } catch {
    return containerObservation("unique", "unreadable");
  }
  if (!Array.isArray(inspection) || inspection.length !== 1) {
    return containerObservation("unique", "unreadable");
  }
  const inspectedContainer = inspection[0];
  if (
    inspectedContainer === null ||
    typeof inspectedContainer !== "object" ||
    Array.isArray(inspectedContainer)
  ) {
    return containerObservation("unique", "unreadable");
  }
  const stateDescriptor = Object.getOwnPropertyDescriptor(
    inspectedContainer,
    "State",
  );
  if (
    stateDescriptor === undefined ||
    !Object.hasOwn(stateDescriptor, "value") ||
    stateDescriptor.value === null ||
    typeof stateDescriptor.value !== "object" ||
    Array.isArray(stateDescriptor.value)
  ) {
    return containerObservation("unique", "unreadable");
  }
  const ociConfigPathDescriptor = Object.getOwnPropertyDescriptor(
    inspectedContainer,
    "OCIConfigPath",
  );
  const ociRuntimeDescriptor = Object.getOwnPropertyDescriptor(
    inspectedContainer,
    "OCIRuntime",
  );
  const hostConfigDescriptor = Object.getOwnPropertyDescriptor(
    inspectedContainer,
    "HostConfig",
  );
  const statusDescriptor = Object.getOwnPropertyDescriptor(
    stateDescriptor.value,
    "Status",
  );
  const runningDescriptor = Object.getOwnPropertyDescriptor(
    stateDescriptor.value,
    "Running",
  );
  const pidDescriptor = Object.getOwnPropertyDescriptor(
    stateDescriptor.value,
    "Pid",
  );
  const errorDescriptor = Object.getOwnPropertyDescriptor(
    stateDescriptor.value,
    "Error",
  );
  const exitCodeDescriptor = Object.getOwnPropertyDescriptor(
    stateDescriptor.value,
    "ExitCode",
  );
  const conmonPidDescriptor = Object.getOwnPropertyDescriptor(
    stateDescriptor.value,
    "ConmonPid",
  );
  const running = runningDescriptor !== undefined &&
      Object.hasOwn(runningDescriptor, "value") &&
      typeof runningDescriptor.value === "boolean"
    ? String(runningDescriptor.value)
    : "unreadable";
  const pid = pidDescriptor !== undefined &&
      Object.hasOwn(pidDescriptor, "value") &&
      Number.isSafeInteger(pidDescriptor.value) &&
      pidDescriptor.value >= 0
    ? (pidDescriptor.value === 0 ? "zero" : "positive")
    : "unreadable";
  const statusReadable = statusDescriptor !== undefined &&
    Object.hasOwn(statusDescriptor, "value") &&
    typeof statusDescriptor.value === "string";
  let inspect = "unreadable";
  switch (statusReadable ? statusDescriptor.value : null) {
    case "created":
      inspect = "created";
      break;
    case "initialized":
      inspect = "initialized";
      break;
    case "running":
      inspect = "running";
      break;
    case "exited":
    case "stopped":
      inspect = "stopped";
      break;
    default:
      if (statusReadable) inspect = "other-state";
  }
  const stateErrorValue = errorDescriptor !== undefined &&
      Object.hasOwn(errorDescriptor, "value")
    ? errorDescriptor.value
    : undefined;
  const stateError = classifyStateError(stateErrorValue);
  const stateErrorAxes = classifyStateErrorAxes(stateErrorValue);
  const conmonOutcome = errorDescriptor === undefined
    ? "absent"
    : classifyConmonOutcome(stateErrorValue);
  const exitCode = exitCodeDescriptor !== undefined &&
      Object.hasOwn(exitCodeDescriptor, "value")
    ? classifyExitCode(exitCodeDescriptor.value)
    : "unreadable";
  // Podman 4.9.3 declares ConmonPid with `omitempty`. Preserve absence as a
  // diagnostic category instead of treating a reusable numeric PID as process
  // authority or silently conflating a malformed present value with zero.
  const conmonPid = conmonPidDescriptor === undefined
    ? "absent"
    : (Object.hasOwn(conmonPidDescriptor, "value")
      ? classifyProcessIdentifier(conmonPidDescriptor.value)
      : "unreadable");
  const ociConfig = ociConfigPathDescriptor === undefined
    ? "absent"
    : (Object.hasOwn(ociConfigPathDescriptor, "value") &&
        typeof ociConfigPathDescriptor.value === "string" &&
        ociConfigPathDescriptor.value !== ""
      ? "present"
      : "unreadable");
  const ociRuntime = ociRuntimeDescriptor === undefined
    ? "absent"
    : (Object.hasOwn(ociRuntimeDescriptor, "value")
      ? classifyOciRuntime(ociRuntimeDescriptor.value)
      : "unreadable");
  let cgroupManager = "unreadable";
  if (
    hostConfigDescriptor !== undefined &&
    Object.hasOwn(hostConfigDescriptor, "value") &&
    hostConfigDescriptor.value !== null &&
    typeof hostConfigDescriptor.value === "object" &&
    !Array.isArray(hostConfigDescriptor.value)
  ) {
    const managerDescriptor = Object.getOwnPropertyDescriptor(
      hostConfigDescriptor.value,
      "CgroupManager",
    );
    cgroupManager = managerDescriptor === undefined
      ? "absent"
      : (Object.hasOwn(managerDescriptor, "value")
        ? classifyCgroupManager(managerDescriptor.value)
        : "unreadable");
  }
  const conmonJournal =
    conmonOutcome === "wait-exit-one" ||
    conmonOutcome === "wait-exit-other"
      ? await observeConmonJournal(journalBoundary, idDescriptor.value)
      : exact({ conmonErrno: "not-applicable", conmonStage: "not-applicable" });
  return containerObservation(
    "unique",
    inspect,
    running,
    pid,
    stateError,
    exitCode,
    conmonPid,
    stateErrorAxes.errorRuntime,
    stateErrorAxes.errorOperation,
    stateErrorAxes.errorErrno,
    ociConfig,
    ociRuntime,
    cgroupManager,
    conmonOutcome,
    conmonJournal.conmonStage,
    conmonJournal.conmonErrno,
  );
}

function fixedPhaseLabel(phase) {
  switch (phase) {
    case "setup":
    case "launch":
    case "ready-marker":
    case "stop":
    case "reconcile":
    case "external-ps":
    case "complete":
      return phase;
    default:
      return "unknown";
  }
}

function writeWatchdogAndExit(line) {
  try {
    writeSync(2, `${line}\n`);
  } finally {
    process.exit(124);
  }
}

test("watchdog state-error classification stays fixed and redacted", () => {
  const stateErrorCases = [
    ["", "empty"],
    [
      "crun: mount /proc/123/fd/7 to /session: permission denied",
      "procfd-access-denied",
    ],
    [
      "crun: mount /proc/123/fd/7 to /session: no such file or directory",
      "procfd-missing",
    ],
    ["crun: mount /proc/123/fd/7: invalid argument", "procfd-other"],
    ["container creation timeout: internal error", "oci-timeout"],
    ["container creation timeout for /proc/123/fd/7", "procfd-other"],
    ["container create failed (no logs from conmon)", "conmon-no-logs"],
    ["crun: mount /session: permission denied", "oci-access-denied"],
    ["output truncated", "non-oci"],
    ["unrelated permission denied", "non-oci"],
    ["unrelated failure", "non-oci"],
    [null, "unreadable"],
  ];
  for (const [inputValue, expectedCategory] of stateErrorCases) {
    assert.equal(classifyStateError(inputValue), expectedCategory);
  }
  assert.equal(classifyExitCode(-1), "nonzero");
  assert.equal(classifyExitCode(0), "zero");
  assert.equal(classifyExitCode(1), "nonzero");
  assert.equal(classifyExitCode(2_147_483_648), "unreadable");
  assert.equal(classifyOciRuntime("/usr/bin/crun"), "crun");
  assert.equal(classifyOciRuntime("runc"), "runc");
  assert.equal(classifyOciRuntime("output truncated"), "other");
  assert.equal(classifyCgroupManager("systemd"), "systemd");
  assert.equal(classifyCgroupManager("cgroupfs"), "cgroupfs");
  assert.equal(classifyCgroupManager("custom"), "other");
  const conmonOutcomeCases = [
    ["conmon failed: exit status 1", "wait-exit-one"],
    ["conmon failed: exit status 70", "wait-exit-other"],
    ["conmon failed: signal: killed", "wait-sigkill"],
    ["conmon failed: signal: segmentation fault", "wait-sigsegv"],
    [
      "conmon failed: signal: segmentation fault (core dumped)",
      "wait-sigsegv",
    ],
    ["conmon failed: signal: aborted", "wait-signal-other"],
    ["configuring conmon env: invalid runtime directory", "env"],
    ["fork/exec /secret/conmon: permission denied", "spawn-failed"],
    ["container create failed (no logs from conmon): EOF", "other"],
    ["conmon failed: unexpected wait status", "wait-other"],
    ["unrelated error", "other"],
    [null, "unreadable"],
  ];
  for (const [inputValue, expected] of conmonOutcomeCases) {
    assert.equal(classifyConmonOutcome(inputValue), expected);
  }
  const axisCases = [
    [
      "crun: mount /proc/123/fd/7 to /session: invalid argument: OCI runtime error",
      ["crun", "procfd", "einval"],
    ],
    [
      "crun: mount /safe/source to /session: invalid argument",
      ["crun", "mount", "einval"],
    ],
    [
      "crun: set propagation for /session: invalid argument",
      ["crun", "mount", "einval"],
    ],
    [
      "crun: cannot setresuid to 1000: invalid argument",
      ["crun", "userns", "einval"],
    ],
    [
      "crun: invalid mapping specified: invalid argument",
      ["crun", "userns", "einval"],
    ],
    [
      "crun: unshare (CLONE_NEWUSER): invalid argument",
      ["crun", "userns", "einval"],
    ],
    [
      "crun: setgroups failed: operation not permitted",
      ["crun", "userns", "eperm"],
    ],
    ["crun: setns failed", ["crun", "namespace", "unknown"]],
    [
      "crun: requested cgroup controller cpu is not available",
      ["crun", "cgroup", "unknown"],
    ],
    [
      "crun: create keyring: operation not permitted",
      ["crun", "security", "eperm"],
    ],
    [
      "crun: capset failed: operation not permitted",
      ["crun", "security", "eperm"],
    ],
    [
      "runc: exec container process: permission denied",
      ["runc", "process", "eacces"],
    ],
    [
      "crun: chdir to `/session`: not a directory",
      ["crun", "process", "enotdir"],
    ],
    [
      "crun: chdir to newroot: not a directory",
      ["crun", "rootfs", "enotdir"],
    ],
    [
      "container create failed (no logs from conmon): EOF",
      ["conmon", "conmon-sync", "unknown"],
    ],
    ["container creation timeout: internal error", ["podman", "create", "timeout"]],
    ["OCI runtime error", ["oci", "unknown", "unknown"]],
    ["output truncated", ["unknown", "unknown", "unknown"]],
    ["crun: feature is not supported", ["crun", "unknown", "unknown"]],
    [
      "crun: cgroup mount /proc/123/fd/7: invalid argument",
      ["crun", "procfd", "einval"],
    ],
    ["crun: cgroup mount failed", ["crun", "cgroup", "unknown"]],
    ["crun: move_mount: function not implemented", ["crun", "mount", "enosys"]],
    ["crun: remount: invalid cross-device link", ["crun", "mount", "exdev"]],
    ["fork/exec /usr/bin/conmon: resource temporarily unavailable", ["conmon", "unknown", "eagain"]],
    ["fork/exec /usr/bin/conmon: too many open files", ["conmon", "unknown", "emfile"]],
    ["fork/exec /usr/bin/conmon: exec format error", ["conmon", "unknown", "enoexec"]],
    [
      "crun: make `/private/root` private: invalid argument",
      ["crun", "mount", "einval"],
    ],
    [
      "crun: umount /private/root: device or resource busy",
      ["crun", "mount", "ebusy"],
    ],
    ["", ["unknown", "unknown", "none"]],
  ];
  for (const [inputValue, expected] of axisCases) {
    const axes = classifyStateErrorAxes(inputValue);
    assert.deepEqual(
      [axes.errorRuntime, axes.errorOperation, axes.errorErrno],
      expected,
    );
  }
  const fakeContainerId = "d".repeat(64);
  const journalRecord = (message, overrides = {}) => JSON.stringify({
    _COMM: "conmon",
    _TRANSPORT: "syslog",
    _UID: "1001",
    MESSAGE: message,
    SYSLOG_IDENTIFIER: "conmon",
    ...overrides,
  });
  const pidfileMessage =
    `conmon ${fakeContainerId.slice(0, 20)} <error>: ` +
    "Failed to write conmon pidfile: /secret/conmon.pid Permission denied";
  assert.deepEqual(
    classifyConmonJournalOutput(
      `${journalRecord(pidfileMessage)}\n`,
      fakeContainerId,
      1001,
    ),
    exact({ conmonErrno: "eacces", conmonStage: "pidfile" }),
  );
  assert.deepEqual(
    classifyConmonJournalOutput(
      `${journalRecord(pidfileMessage, { _COMM: undefined })}\n`,
      fakeContainerId,
      1001,
    ),
    exact({ conmonErrno: "eacces", conmonStage: "pidfile" }),
  );
  assert.deepEqual(
    classifyConmonJournalOutput(
      `${journalRecord(pidfileMessage, { _COMM: "not-conmon" })}\n`,
      fakeContainerId,
      1001,
    ),
    exact({ conmonErrno: "unreadable", conmonStage: "unreadable" }),
  );
  assert.deepEqual(
    classifyConmonJournalOutput(
      `${journalRecord("conmon abcdef <error>: unrelated")}\n`,
      fakeContainerId,
      1001,
    ),
    exact({ conmonErrno: "absent", conmonStage: "absent" }),
  );
  const forkMessage =
    `conmon ${fakeContainerId.slice(0, 20)} <error>: ` +
    "Failed to fork the create command Resource temporarily unavailable";
  assert.deepEqual(
    classifyConmonJournalOutput(
      `${journalRecord(pidfileMessage)}\n${journalRecord(forkMessage)}\n`,
      fakeContainerId,
      1001,
    ),
    exact({ conmonErrno: "eacces", conmonStage: "pidfile" }),
  );
  const logOpenMessage =
    `conmon ${fakeContainerId.slice(0, 20)} <error>: ` +
    "Failed to open log file Read-only file system";
  assert.deepEqual(
    classifyConmonJournalOutput(
      `${journalRecord(forkMessage)}\n${journalRecord(logOpenMessage)}\n`,
      fakeContainerId,
      1001,
    ),
    exact({ conmonErrno: "ambiguous", conmonStage: "ambiguous" }),
  );
  const runtimeMissingMessage =
    `conmon ${fakeContainerId.slice(0, 20)} <error>: ` +
    "Runtime path not provided. Use --runtime";
  assert.deepEqual(
    classifyConmonJournalOutput(
      `${journalRecord(runtimeMissingMessage)}\n`,
      fakeContainerId,
      1001,
    ),
    exact({ conmonErrno: "none", conmonStage: "runtime-path" }),
  );
  assert.deepEqual(
    classifyConmonJournalOutput("{not-json}\n", fakeContainerId, 1001),
    exact({ conmonErrno: "unreadable", conmonStage: "unreadable" }),
  );
  const sensitiveError =
    `crun: mount /proc/123/fd/7 to /session for ${"a".repeat(64)}: invalid argument`;
  const sensitiveAxes = classifyStateErrorAxes(sensitiveError);
  const redactedObservation = containerObservation(
    "unique",
    "created",
    "false",
    "zero",
    classifyStateError(
      "crun: mount /proc/123/fd/7 to /session: permission denied",
    ),
    "nonzero",
    "positive",
    sensitiveAxes.errorRuntime,
    sensitiveAxes.errorOperation,
    sensitiveAxes.errorErrno,
    "present",
    "crun",
    "systemd",
    "wait-exit-one",
    "pidfile",
    "eacces",
  );
  const serializedObservation = JSON.stringify(redactedObservation);
  assert.equal(serializedObservation.includes("/proc/"), false);
  assert.equal(serializedObservation.includes("/session"), false);
  assert.equal(serializedObservation.includes("a".repeat(64)), false);
  assert.equal(serializedObservation.includes("invalid argument"), false);
  assert.equal(
    JSON.stringify(
      classifyConmonJournalOutput(
        `${journalRecord(pidfileMessage)}\n`,
        fakeContainerId,
        1001,
      ),
    ).includes("/secret/"),
    false,
  );
  assert.equal(serializedObservation.includes(fakeContainerId), false);
});

test("rootless Podman launches, writes through the sole bind, stops, and reconciles", async () => {
  assert.match(DIGEST, /^sha256:[0-9a-f]{64}$/u);
  assert.equal(IMAGE_REFERENCE, `localhost/portable-codex-writer@${DIGEST}`);
  assert.match(PODMAN, /^\/(?:[^/\0]+\/)*[^/\0]+$/u);
  assert.match(ROOT, /^\/var\/tmp\/portable-codex-runtime-podman-[1-9][0-9]*$/u);
  assert.equal(ROOT.startsWith(TEST_ROOT_PREFIX), true);
  assert.match(process.env.HOME, /^\//u);
  assert.match(process.env.XDG_RUNTIME_DIR, /^\//u);
  const attachmentRoot = `${ROOT}/attachment`;
  const stateRoot = `${ROOT}/state`;
  let containerId = null;
  let phase = "setup";
  let supervisorState = null;
  let watchdogActive = false;
  let watchdogDiagnosticTimer = null;
  let watchdogHardBackstopTimer = null;
  const clearLaunchWatchdog = () => {
    watchdogActive = false;
    if (watchdogDiagnosticTimer !== null) {
      clearTimeout(watchdogDiagnosticTimer);
      watchdogDiagnosticTimer = null;
    }
    if (watchdogHardBackstopTimer !== null) {
      clearTimeout(watchdogHardBackstopTimer);
      watchdogHardBackstopTimer = null;
    }
  };
  const startLaunchWatchdog = (journalBoundary) => {
    watchdogActive = true;
    watchdogDiagnosticTimer = setTimeout(() => {
      void (async () => {
        try {
          const durableStatus = await observeDurableStatus(supervisorState);
          const observed = await observeUniqueImageContainer(journalBoundary);
          if (!watchdogActive) return;
          writeWatchdogAndExit(
            `podman-integration-watchdog phase=${fixedPhaseLabel(phase)} ` +
              `durableStatus=${durableStatus} imagePs=${observed.ps} ` +
              `imageInspect=${observed.inspect} running=${observed.running} ` +
              `pid=${observed.pid} stateError=${observed.stateError} ` +
              `errorRuntime=${observed.errorRuntime} ` +
              `errorOperation=${observed.errorOperation} ` +
              `errorErrno=${observed.errorErrno} ` +
              `ociConfig=${observed.ociConfig} ` +
              `ociRuntime=${observed.ociRuntime} ` +
              `cgroupManager=${observed.cgroupManager} ` +
              `conmonOutcome=${observed.conmonOutcome} ` +
              `conmonStage=${observed.conmonStage} ` +
              `conmonErrno=${observed.conmonErrno} ` +
              `exitCode=${observed.exitCode} conmonPid=${observed.conmonPid}`,
          );
        } catch {
          if (!watchdogActive) return;
          writeWatchdogAndExit(
            `podman-integration-watchdog phase=${fixedPhaseLabel(phase)} ` +
              "durableStatus=probe-error imagePs=probe-error " +
              "imageInspect=probe-error running=unreadable pid=unreadable " +
              "stateError=unreadable errorRuntime=unreadable " +
              "errorOperation=unreadable errorErrno=unreadable " +
              "ociConfig=unreadable ociRuntime=unreadable " +
              "cgroupManager=unreadable " +
              "conmonOutcome=unreadable conmonStage=unreadable " +
              "conmonErrno=unreadable " +
              "exitCode=unreadable conmonPid=unreadable",
          );
        }
      })();
    }, WATCHDOG_DIAGNOSTIC_MILLISECONDS);
    watchdogHardBackstopTimer = setTimeout(() => {
      if (!watchdogActive) return;
      writeWatchdogAndExit(
        `podman-integration-watchdog phase=${fixedPhaseLabel(phase)} ` +
          "durableStatus=probe-timeout imagePs=probe-timeout " +
          "imageInspect=probe-timeout running=unreadable pid=unreadable " +
          "stateError=unreadable errorRuntime=unreadable " +
          "errorOperation=unreadable errorErrno=unreadable " +
          "ociConfig=unreadable ociRuntime=unreadable " +
          "cgroupManager=unreadable " +
          "conmonOutcome=unreadable conmonStage=unreadable " +
          "conmonErrno=unreadable " +
          "exitCode=unreadable conmonPid=unreadable",
      );
    }, WATCHDOG_HARD_BACKSTOP_MILLISECONDS);
  };
  try {
    const podmanEnvironment = exact({
      HOME: process.env.HOME,
      LANG: "C.UTF-8",
      XDG_RUNTIME_DIR: process.env.XDG_RUNTIME_DIR,
    });
    await mkdir(ROOT, { mode: 0o700 });
    await mkdir(attachmentRoot, { mode: 0o700 });
    const state = createPodmanWriterSupervisorState(exact({ root: stateRoot }));
    supervisorState = state;
    const options = exact({
      commandTimeoutMilliseconds: 30_000,
      configuredAttachmentRoot: ROOT,
      images: exact({
        [DIGEST]: exact({
          architecture: "amd64",
          codexVersion: "1.0.0",
          imageReference: IMAGE_REFERENCE,
          os: "linux",
        }),
      }),
      maxOutputBytes: 1024 * 1024,
      podmanEnvironment,
      podmanExecutable: PODMAN,
      state,
      stopTimeoutSeconds: 10,
      supervisorId: SUPERVISOR_ID,
      writerCommand: Object.freeze(["/usr/local/bin/writer"]),
      writerEnvironment: exact({ LANG: "C.UTF-8" }),
    });
    const input = launchInput(attachmentRoot);
    const supervisor = createPodmanWriterSupervisor(options);
    const journalBoundary = await captureConmonJournalBoundary();
    phase = "launch";
    startLaunchWatchdog(journalBoundary);
    const receipt = await supervisor.launchWriter(input);
    containerId = receipt.evidence.processIncarnationId.slice("podman-process:".length);
    phase = "ready-marker";
    const markerPath = `${attachmentRoot}/podman-writer-ready`;
    await waitForReadyMarker(markerPath);
    const markerStat = await lstat(markerPath);
    assert.equal(markerStat.isFile(), true);
    assert.equal(markerStat.uid, process.getuid());
    assert.equal(markerStat.gid, process.getgid());
    assert.equal(markerStat.mode & 0o7777, 0o600);
    phase = "stop";
    assert.deepEqual(await receipt.stopWriter(stopInput(input, receipt)), exact({
      contractVersion: 2,
      status: "stopped",
    }));
    const restarted = createPodmanWriterSupervisor(options);
    phase = "reconcile";
    const reconciled = await restarted.reconcileWriterLaunch(reconcileInput(input));
    assert.equal(reconciled.evidence.status, "complete-stopped");
    phase = "external-ps";
    const retired = await execFileAsync(
      PODMAN,
      [
        "--remote=false",
        "ps",
        "-a",
        "--filter",
        `id=${containerId}`,
        "--format=json",
      ],
      { env: process.env, timeout: 30_000 },
    );
    assert.deepEqual(JSON.parse(retired.stdout), []);
    containerId = null;
    phase = "complete";
    clearLaunchWatchdog();
  } catch (error) {
    clearLaunchWatchdog();
    let durableStatus = "unreadable";
    const createObservation = await observeCreatedContainer();
    if (supervisorState !== null) {
      try {
        const record = await supervisorState.read(exact({
          launchAttemptId: "podman-launch-attempt-001",
        }));
        durableStatus = record === null ? "absent" : record.status;
      } catch {
        // Preserve the primary integration failure while keeping diagnostics
        // restricted to fixed lifecycle labels.
      }
    }
    console.error(
      `podman-integration-failure phase=${phase} durableStatus=${durableStatus} ` +
        `createObservation=${createObservation}`,
    );
    throw error;
  } finally {
    clearLaunchWatchdog();
    if (containerId !== null) {
      await execFileAsync(PODMAN, [
        "--remote=false",
        "rm",
        "--force",
        containerId,
      ], {
        env: process.env,
        timeout: 30_000,
      }).catch(() => {});
    }
    await rm(ROOT, { force: true, recursive: true });
  }
});
