import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { mkdir, readFile, rm } from "node:fs/promises";
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
const PODMAN_EXECUTION_PATH = "/usr/bin:/bin";

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
  try {
    await mkdir(ROOT, { mode: 0o700 });
    await mkdir(attachmentRoot, { mode: 0o700 });
    const state = createPodmanWriterSupervisorState(exact({ root: stateRoot }));
    supervisorState = state;
    const podmanEnvironment = exact({
      HOME: process.env.HOME,
      LANG: "C.UTF-8",
      XDG_RUNTIME_DIR: process.env.XDG_RUNTIME_DIR,
    });
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
    const preflightOptions = {
      cwd: "/",
      encoding: "utf8",
      env: exact({
        ...podmanEnvironment,
        PATH: PODMAN_EXECUTION_PATH,
      }),
      killSignal: "SIGKILL",
      maxBuffer: 1024 * 1024,
      timeout: 5_000,
    };
    phase = "preflight-info";
    await execFileAsync(PODMAN, ["info", "--format=json"], preflightOptions);
    phase = "preflight-image-inspect";
    await execFileAsync(
      PODMAN,
      ["image", "inspect", "--format=json", IMAGE_REFERENCE],
      preflightOptions,
    );
    const input = launchInput(attachmentRoot);
    const supervisor = createPodmanWriterSupervisor(options);
    phase = "launch";
    const receipt = await supervisor.launchWriter(input);
    containerId = receipt.evidence.processIncarnationId.slice("podman-process:".length);
    phase = "ready-marker";
    await waitForReadyMarker(`${attachmentRoot}/podman-writer-ready`);
    phase = "stop";
    assert.deepEqual(await receipt.stopWriter(stopInput(input, receipt)), exact({
      contractVersion: 2,
      status: "stopped",
    }));
    const restarted = createPodmanWriterSupervisor(options);
    phase = "reconcile";
    const reconciled = await restarted.reconcileWriterLaunch(reconcileInput(input));
    assert.equal(reconciled.evidence.status, "complete-stopped");
    assert.equal(reconciled.stopWriter, null);
    phase = "external-ps";
    const retired = await execFileAsync(
      PODMAN,
      ["ps", "-a", "--filter", `id=${containerId}`, "--format=json"],
      { env: process.env, timeout: 30_000 },
    );
    assert.deepEqual(JSON.parse(retired.stdout), []);
    containerId = null;
    phase = "complete";
  } catch (error) {
    let durableStatus = "unreadable";
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
      `podman-integration-failure phase=${phase} durableStatus=${durableStatus}`,
    );
    throw error;
  } finally {
    if (containerId !== null) {
      await execFileAsync(PODMAN, ["rm", "--force", containerId], {
        env: process.env,
        timeout: 30_000,
      }).catch(() => {});
    }
    await rm(ROOT, { force: true, recursive: true });
  }
});
