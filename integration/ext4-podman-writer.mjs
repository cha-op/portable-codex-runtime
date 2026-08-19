import assert from "node:assert/strict";
import { lstat, readFile } from "node:fs/promises";
import { setTimeout as delay } from "node:timers/promises";

import {
  PODMAN_WRITER_SUPERVISOR_CONTRACT_VERSION,
  createPodmanWriterSupervisor,
} from "../src/podman-writer-supervisor.mjs";
import {
  createPodmanWriterSupervisorState,
} from "../src/podman-writer-supervisor-state.mjs";

const READY_MARKER = "ready\n";
const SUPERVISOR_ID = "ext4-podman-composition-v1";

function exact(value) {
  return Object.freeze(Object.assign(Object.create(null), value));
}

function measuredImage(imageDigest) {
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
        digest: imageDigest,
        mediaType: "application/vnd.oci.image.manifest.v1+json",
        os: "linux",
        size: 1,
      }),
    }),
    runtimeIdentity: exact({
      codexBinaryPath: "/usr/local/bin/writer",
      codexBinarySha256: "c".repeat(64),
      codexVersion: "1.0.0",
      platformImageDigest: imageDigest,
    }),
  });
}

function launchInput(attachment, imageDigest) {
  const lease = exact({
    contractVersion: 1,
    expiresAt: "2030-01-01T00:00:00.000Z",
    fencingEpoch: attachment.fencingEpoch,
    holderId: attachment.holderId,
    leaseId: attachment.leaseId,
    sessionId: attachment.sessionId,
  });
  const generationReference = exact({
    bindingSha256: "1".repeat(64),
    checkpointId: "ext4-podman-checkpoint-001",
    claimedAt: "2026-08-14T09:00:00.000Z",
    committedAt: "2026-08-14T09:01:00.000Z",
    documentSha256: "2".repeat(64),
    generationId: "ext4-podman-generation-001",
    operationId: "ext4-podman-generation-operation-001",
    sessionId: attachment.sessionId,
    state: "committed",
  });
  const request = exact({
    attachment,
    contractVersion: 1,
    fencingEpoch: attachment.fencingEpoch,
    generation: generationReference,
    lease,
    measuredImage: measuredImage(imageDigest),
    supervisor: exact({ contractVersion: 1, supervisorId: SUPERVISOR_ID }),
  });
  const launchAttemptId = "ext4-podman-launch-attempt-001";
  const session = exact({
    createdAt: "2026-08-14T08:00:00.000Z",
    document: exact({
      activeOperation: exact({ operationId: launchAttemptId }),
      attachment,
      backendCapabilities: exact({ exclusiveWriterAttachment: true }),
      documentVersion: 3,
      lastOperation: null,
      launch: null,
      lease,
      lifecycle: "ATTACHED",
      manifest: exact({ runtime: exact({ imageDigest }) }),
      recovery: null,
      storageRef: exact({ storageId: attachment.storageId }),
      writerEpoch: attachment.fencingEpoch,
    }),
    revision: "10",
    sessionId: attachment.sessionId,
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
    requestSha256: "d".repeat(64),
    result: null,
    retiredAt: null,
    revision: "1",
    sessionId: attachment.sessionId,
    state: "starting",
    updatedAt: "2026-08-14T10:00:00.000Z",
  });
  const reservation = exact({
    conflictClass: "session-mutation",
    createdAt: "2026-08-14T09:59:00.000Z",
    expectedSessionRevision: "9",
    expiresAt: null,
    kind: operation.kind,
    operationId: launchAttemptId,
    releasedAt: null,
    requestSha256: operation.requestSha256,
    reservationId: "ext4-podman-reservation-001",
    sessionId: attachment.sessionId,
    state: "starting",
    updatedAt: "2026-08-14T10:00:00.000Z",
  });
  return exact({
    attempt,
    authorityNow: "2026-08-14T10:00:01.000Z",
    consumedImage: request.measuredImage,
    contractVersion: PODMAN_WRITER_SUPERVISOR_CONTRACT_VERSION,
    generation: exact({
      binding: exact({ provider: "ext4-podman-integration" }),
      checkpointId: generationReference.checkpointId,
      claimedAt: generationReference.claimedAt,
      committedAt: generationReference.committedAt,
      document: exact({ status: "committed" }),
      generationId: generationReference.generationId,
      operationId: generationReference.operationId,
      sessionId: generationReference.sessionId,
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
    stopOperationId: "ext4-podman-stop-operation-001",
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

async function waitForReadyMarker(path) {
  const deadline = Date.now() + 10_000;
  while (true) {
    try {
      assert.equal(await readFile(path, "utf8"), READY_MARKER);
      return;
    } catch (error) {
      if (error?.code !== "ENOENT" || Date.now() >= deadline) throw error;
    }
    await delay(50);
  }
}

export async function runExt4PodmanWriterIntegration({
  attachment,
  configuredAttachmentRoot,
  filesystemAuthority,
  imageDigest,
  imageReference,
  podmanEnvironment,
  podmanExecutable,
  stateRoot,
}) {
  assert.equal(process.platform, "linux");
  assert.notEqual(process.getuid(), 0);
  assert.match(imageDigest, /^sha256:[0-9a-f]{64}$/u);
  assert.equal(imageReference, `localhost/portable-codex-writer@${imageDigest}`);
  assert.match(podmanExecutable, /^\/(?:[^/\0]+\/)*[^/\0]+$/u);
  assert.match(podmanEnvironment.HOME, /^\//u);
  assert.match(podmanEnvironment.XDG_RUNTIME_DIR, /^\//u);
  const servicePid = process.pid;
  const serviceUid = process.getuid();
  const state = createPodmanWriterSupervisorState(exact({ root: stateRoot }));
  const supervisor = createPodmanWriterSupervisor(exact({
    commandTimeoutMilliseconds: 30_000,
    configuredAttachmentRoot,
    filesystemAuthority,
    images: exact({
      [imageDigest]: exact({
        architecture: "amd64",
        codexVersion: "1.0.0",
        imageReference,
        os: "linux",
      }),
    }),
    maxOutputBytes: 1024 * 1024,
    podmanEnvironment,
    podmanExecutable,
    state,
    stopTimeoutSeconds: 10,
    supervisorId: SUPERVISOR_ID,
    writerCommand: Object.freeze(["/usr/local/bin/writer"]),
    writerEnvironment: exact({ LANG: "C.UTF-8" }),
  }));
  const input = launchInput(attachment, imageDigest);
  const receipt = await supervisor.launchWriter(input);
  assert.equal(process.pid, servicePid);
  assert.equal(process.getuid(), serviceUid);
  const markerPath = `${attachment.rootPath}/podman-writer-ready`;
  assert.equal(typeof receipt.stopWriter, "function");
  try {
    await waitForReadyMarker(markerPath);
    const marker = await lstat(markerPath);
    assert.equal(marker.isFile(), true);
    assert.equal(marker.uid, serviceUid);
    assert.equal(marker.gid, process.getgid());
    assert.equal(marker.mode & 0o7777, 0o600);
  } finally {
    assert.deepEqual(await receipt.stopWriter(stopInput(input, receipt)), exact({
      contractVersion: PODMAN_WRITER_SUPERVISOR_CONTRACT_VERSION,
      status: "stopped",
    }));
  }
  assert.equal(process.pid, servicePid);
  assert.equal(process.getuid(), serviceUid);
  return exact({ markerPath, servicePid, serviceUid });
}

Object.freeze(runExt4PodmanWriterIntegration);
