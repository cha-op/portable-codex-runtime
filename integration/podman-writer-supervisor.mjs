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
const SESSION_ID = "019f2100-0000-7000-8000-000000000001";
const SUPERVISOR_ID = "podman-linux-integration-v1";
const TEST_ROOT_PREFIX = "/var/tmp/portable-codex-runtime-podman-";
const WATCHDOG_DIAGNOSTIC_MILLISECONDS = 45_000;
const WATCHDOG_HARD_BACKSTOP_MILLISECONDS = 60_000;
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
) {
  return { conmonPid, exitCode, inspect, pid, ps, running, stateError };
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

async function observeUniqueImageContainer() {
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
  const stateError = errorDescriptor !== undefined &&
      Object.hasOwn(errorDescriptor, "value")
    ? classifyStateError(errorDescriptor.value)
    : "unreadable";
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
  return containerObservation(
    "unique",
    inspect,
    running,
    pid,
    stateError,
    exitCode,
    conmonPid,
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
  );
  assert.equal(JSON.stringify(redactedObservation).includes("/proc/"), false);
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
  const startLaunchWatchdog = () => {
    watchdogActive = true;
    watchdogDiagnosticTimer = setTimeout(() => {
      void (async () => {
        try {
          const durableStatus = await observeDurableStatus(supervisorState);
          const observed = await observeUniqueImageContainer();
          if (!watchdogActive) return;
          writeWatchdogAndExit(
            `podman-integration-watchdog phase=${fixedPhaseLabel(phase)} ` +
              `durableStatus=${durableStatus} imagePs=${observed.ps} ` +
              `imageInspect=${observed.inspect} running=${observed.running} ` +
              `pid=${observed.pid} stateError=${observed.stateError} ` +
              `exitCode=${observed.exitCode} conmonPid=${observed.conmonPid}`,
          );
        } catch {
          if (!watchdogActive) return;
          writeWatchdogAndExit(
            `podman-integration-watchdog phase=${fixedPhaseLabel(phase)} ` +
              "durableStatus=probe-error imagePs=probe-error " +
              "imageInspect=probe-error running=unreadable pid=unreadable " +
              "stateError=unreadable exitCode=unreadable conmonPid=unreadable",
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
          "stateError=unreadable exitCode=unreadable conmonPid=unreadable",
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
    phase = "launch";
    startLaunchWatchdog();
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
