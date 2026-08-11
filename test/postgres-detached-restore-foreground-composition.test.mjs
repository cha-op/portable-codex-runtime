import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import test from "node:test";

import {
  PostgresDetachedRestoreForegroundCompositionError,
  POSTGRES_DETACHED_RESTORE_FLEET_CONFIRMED,
  createPostgresDetachedRestoreForegroundComposition,
  isPostgresDetachedRestoreForegroundComposition,
} from "../src/postgres-detached-restore-foreground-composition.mjs";
import {
  createPostgresDetachedRestorePlan,
} from "../src/postgres-detached-restore-plan.mjs";
import {
  derivePostgresLogicalWriterStopOperationId,
} from "../src/postgres-logical-writer-launcher.mjs";
import {
  PostgresOperationGuard,
} from "../src/postgres-operation-guard.mjs";
import {
  createPostgresRestoreLifecycleGuard,
} from "../src/postgres-restore-lifecycle-guard.mjs";
import {
  CHECKPOINT_CAPTURE_OPERATION_KIND,
  SESSION_AUTHORITY_DOCUMENT_VERSION,
  SESSION_OPERATION_CONFLICT_CLASS,
  WRITER_LAUNCH_STOP_OPERATION_KIND,
  WRITER_LEASE_RENEW_OPERATION_KIND,
  PostgresSessionAuthorityError,
  assertSessionAuthoritySnapshot,
  assertSessionOperationBinding,
  createCheckpointCaptureOperationRequest,
  createRestoreDestinationGenerationOperationRequest,
  createWriterLaunchAttemptOperationRequest,
} from "../src/postgres-session-authority.mjs";
import {
  assertRestoreAttachmentActivationRequest,
  assertRestoreAttachmentActivationResult,
  createSessionManifest,
} from "../src/session-storage-contracts.mjs";

const SESSION_ID = "019f7f40-0000-7000-8000-000000000001";
const THREAD_ID = "019f7f40-0000-7000-8000-000000000002";
const BASE_TIME = "2026-08-11T08:00:00.000Z";
const IMAGE_DIGEST = `sha256:${"a".repeat(64)}`;

function deepFreeze(value) {
  if (value === null || typeof value !== "object" || Object.isFrozen(value)) {
    return value;
  }
  for (const key of Reflect.ownKeys(value)) deepFreeze(value[key]);
  return Object.freeze(value);
}

function sha256Json(value) {
  return createHash("sha256")
    .update(JSON.stringify(value), "utf8")
    .digest("hex");
}

function sha256Text(value) {
  return createHash("sha256").update(value, "utf8").digest("hex");
}

function canonicalData(value) {
  if (Array.isArray(value)) return deepFreeze(value.map(canonicalData));
  if (value === null || typeof value !== "object") return value;
  const result = Object.create(null);
  for (const key of Object.keys(value).sort()) {
    result[key] = canonicalData(value[key]);
  }
  return deepFreeze(result);
}

function restoreMaterialization(publicationId) {
  return deepFreeze({
    artifactManifestDigest: "1".repeat(64),
    contractVersion: 1,
    coordinatorBindingSha256: "2".repeat(64),
    modeledDigest: "3".repeat(64),
    publicationId,
    publicationKind: "restore-destination",
    stagedRoot: {
      filesystemId: "filesystem-001",
      objectIdentityScheme: "provider-object-id-v1",
      objectId: "destination-object-001",
    },
    treeIdentityDigest: "4".repeat(64),
  });
}

function manifest() {
  return createSessionManifest({
    sessionId: SESSION_ID,
    codex: {
      ephemeral: false,
      historyMode: "paginated",
      rootThreadId: THREAD_ID,
      sessionId: THREAD_ID,
    },
    runtime: {
      codexSandbox: "danger-full-access",
      codexVersion: "codex-cli 0.142.4",
      imageDigest: IMAGE_DIGEST,
      imageMediaType: "application/vnd.oci.image.manifest.v1+json",
      platform: "linux/arm64",
    },
  });
}

function lease(expiresAt = "2027-08-11T08:00:00.000Z") {
  return {
    contractVersion: 1,
    expiresAt,
    fencingEpoch: "42",
    holderId: "writer-holder-001",
    leaseId: "writer-lease-001",
    sessionId: SESSION_ID,
  };
}

function attachment() {
  return {
    attachmentId: "attachment-001",
    backendId: "storage-backend-001",
    contractVersion: 1,
    fencingEpoch: "42",
    holderId: "writer-holder-001",
    kind: "directory",
    leaseId: "writer-lease-001",
    mode: "read-write",
    operationId: "writer-attach-001",
    proofId: "writer-attach-proof-001",
    rootPath: "/var/lib/portable-codex/sessions/session-001",
    sessionId: SESSION_ID,
    storageId: "storage-001",
  };
}

function terminalPointer(overrides = {}) {
  return {
    conflictClass: SESSION_OPERATION_CONFLICT_CLASS,
    expectedSessionRevision: "5",
    kind: "writer-attachment-acquire-v1",
    operationId: "writer-attach-001",
    operationRevision: "2",
    requestSha256: "b".repeat(64),
    reservationId: "reservation-writer-attach-001",
    resultSha256: "c".repeat(64),
    state: "committed",
    ...overrides,
  };
}

function baseSession(overrides = {}) {
  const document = {
    activeOperation: null,
    attachment: attachment(),
    backendCapabilities: {
      atomicPointInTimeCheckpoint: true,
      exclusiveWriterAttachment: true,
      fencing: "epoch-enforced",
      normalDirectoryAttachment: true,
    },
    documentVersion: SESSION_AUTHORITY_DOCUMENT_VERSION,
    lastOperation: terminalPointer(),
    launch: null,
    lease: lease(),
    lifecycle: "ATTACHED",
    manifest: manifest(),
    recovery: null,
    storageRef: {
      backendId: "storage-backend-001",
      contractVersion: 1,
      sessionId: SESSION_ID,
      storageId: "storage-001",
    },
    writerEpoch: "42",
    ...overrides.document,
  };
  return assertSessionAuthoritySnapshot(
    deepFreeze({
      createdAt: BASE_TIME,
      document,
      revision: overrides.revision ?? "8",
      sessionId: SESSION_ID,
      updatedAt: overrides.updatedAt ?? BASE_TIME,
    }),
  );
}

function runningSession(overrides = {}) {
  const launchAttemptId = "current-launch-001";
  return baseSession({
    ...overrides,
    document: {
      lastOperation: terminalPointer({
        kind: "writer-launch-attempt-v1",
        operationId: launchAttemptId,
        reservationId: "reservation-current-launch-001",
      }),
      launch: {
        attachmentId: "attachment-001",
        attachmentSha256: "1".repeat(64),
        contractVersion: 1,
        fencingEpoch: "42",
        generation: {
          bindingSha256: "2".repeat(64),
          checkpointId: "current-checkpoint-001",
          claimedAt: "2026-08-11T07:50:00.000Z",
          committedAt: "2026-08-11T07:51:00.000Z",
          documentSha256: "3".repeat(64),
          generationId: "current-generation-001",
          operationId: "current-generation-operation-001",
          sessionId: SESSION_ID,
          state: "committed",
        },
        launchAttemptId,
        launchResultSha256: "4".repeat(64),
        leaseId: "writer-lease-001",
        leaseSha256: "5".repeat(64),
        measuredImageSha256: "6".repeat(64),
        processIncarnationId: "process-incarnation-001",
        startedAt: "2026-08-11T07:55:00.000Z",
        supervisorId: "supervisor-001",
        supervisorProofId: "supervisor-proof-001",
        writerIncarnationId: "writer-incarnation-001",
      },
      ...overrides.document,
    },
  });
}

function restoreRequest() {
  return {
    backendId: "storage-backend-001",
    contractVersion: 1,
    fencingEpoch: "42",
    holderId: "writer-holder-001",
    leaseId: "writer-lease-001",
    operation: "restore",
    operationId: "restore-root-001",
    sessionId: SESSION_ID,
    storageId: "storage-001",
    target: {
      artifactId: "source-artifact-001",
      checkpointId: "source-checkpoint-001",
      kind: "checkpoint",
    },
  };
}

function fixturePlan() {
  return createPostgresDetachedRestorePlan({
    request: restoreRequest(),
    plan: {
      captureCreatedAt: "2026-08-11T09:00:00.000Z",
      destinationDirectory: "/var/lib/portable-codex/restores/restore-001",
      destinationOwnedRoot: "/var/lib/portable-codex/restores",
      detachMode: "release",
      holderId: "restore-writer-001",
      imagePlanId: "image-plan-001",
      leaseDurationMilliseconds: 600_000,
      sourceArtifactDirectory: "/var/lib/portable-codex/artifacts/source-001",
      sourceArtifactOwnedRoot: "/var/lib/portable-codex/artifacts",
    },
  });
}

function admission() {
  return deepFreeze({
    checkpoint: {
      artifactId: "source-artifact-001",
      backendId: "storage-backend-001",
      checkpointClass: "clean",
      checkpointId: "source-checkpoint-001",
      codexSessionId: THREAD_ID,
      codexThreadId: THREAD_ID,
      contractVersion: 1,
      createdAt: "2026-08-11T07:59:00.000Z",
      imageDigest: IMAGE_DIGEST,
      sessionId: SESSION_ID,
      sourceFencingEpoch: "41",
      storageId: "storage-001",
    },
    request: restoreRequest(),
  });
}

class LockManager {
  constructor() {
    this.held = new Map();
    this.queries = [];
  }

  acquire(key, client, mode) {
    const record = this.held.get(key) ?? { exclusive: null, shared: new Set() };
    if (mode === "shared" && record.exclusive !== null) return false;
    if (mode === "exclusive" && (record.exclusive !== null || record.shared.size > 0)) {
      return false;
    }
    if (mode === "shared") record.shared.add(client);
    else record.exclusive = client;
    this.held.set(key, record);
    return true;
  }

  contains(key, client, mode) {
    const record = this.held.get(key);
    return mode === "shared"
      ? record?.shared.has(client) === true
      : record?.exclusive === client;
  }

  release(key, client, mode) {
    const record = this.held.get(key);
    if (record === undefined) return false;
    let released;
    if (mode === "shared") released = record.shared.delete(client);
    else {
      released = record.exclusive === client;
      if (released) record.exclusive = null;
    }
    if (record.exclusive === null && record.shared.size === 0) this.held.delete(key);
    return released;
  }

  releaseAll(client) {
    for (const [key, record] of this.held) {
      record.shared.delete(client);
      if (record.exclusive === client) record.exclusive = null;
      if (record.exclusive === null && record.shared.size === 0) this.held.delete(key);
    }
  }
}

class GuardClient {
  constructor(manager, pid) {
    this.manager = manager;
    this.pid = pid;
  }

  query(config) {
    this.manager.queries.push(config.text);
    const callback = config.callback;
    const text = config.text;
    if (text === "DISCARD ALL") {
      this.manager.releaseAll(this);
      callback(null, { command: "DISCARD", rows: [] });
      return undefined;
    }
    const key = config.values[0];
    const shared =
      text.includes("_shared") || text.includes("mode = 'ShareLock'");
    const mode = shared ? "shared" : "exclusive";
    if (text.includes("pg_try_advisory_lock")) {
      callback(null, {
        command: "SELECT",
        rows: [{ acquired: this.manager.acquire(key, this, mode), backend_pid: this.pid }],
      });
      return undefined;
    }
    if (text.includes("FROM pg_catalog.pg_locks")) {
      callback(null, {
        command: "SELECT",
        rows: [{ backend_pid: this.pid, lock_held: this.manager.contains(key, this, mode) }],
      });
      return undefined;
    }
    if (text.includes("pg_advisory_unlock")) {
      callback(null, {
        command: "SELECT",
        rows: [{ backend_pid: this.pid, unlocked: this.manager.release(key, this, mode) }],
      });
      return undefined;
    }
    callback(new Error(`unexpected guard query: ${text}`));
    return undefined;
  }

  release() {
    this.manager.releaseAll(this);
    return undefined;
  }
}

function guardPool(manager, pid) {
  const client = new GuardClient(manager, pid);
  const release = Object.freeze((...args) => {
    assert.equal(args.length <= 1, true);
    return client.release(...args);
  });
  return {
    connect(callback) {
      callback(null, client, release);
      return undefined;
    },
  };
}

function guards() {
  const manager = new LockManager();
  const foregroundPool = guardPool(manager, 1001);
  const foregroundOperationGuard = new PostgresOperationGuard({
    dedicatedPool: foregroundPool,
  });
  const recoveryOperationGuard = new PostgresOperationGuard({
    dedicatedPool: guardPool(manager, 1002),
  });
  return {
    foregroundOperationGuard,
    lifecycleGuard: createPostgresRestoreLifecycleGuard({
      foregroundOperationGuard,
      recoveryOperationGuard,
    }),
    operationGuard: new PostgresOperationGuard({
      dedicatedPool: guardPool(manager, 1003),
    }),
    queries: manager.queries,
    recoveryOperationGuard,
    sameForegroundPoolOperationGuard: new PostgresOperationGuard({
      dedicatedPool: foregroundPool,
    }),
  };
}

function captureBackend() {
  const operation = async () => undefined;
  return {
    backendId: "storage-backend-001",
    capabilities: {
      atomicPointInTimeCheckpoint: true,
      exclusiveWriterAttachment: true,
      fencing: "epoch-enforced",
      normalDirectoryAttachment: true,
    },
    captureCheckpoint: operation,
    captureReconciliationContractVersion: 1,
    contractVersion: 1,
    destroySession: operation,
    detachAttachment: operation,
    forceFence: operation,
    prepareWritableAttachment: operation,
    preparedCheckpointCaptureContractVersion: 1,
    provisionSession: operation,
    async reconcileCheckpointCapture() {
      throw new Error("unexpected reconcile");
    },
    async restoreCheckpoint() {
      throw new Error("unexpected restore");
    },
    async resumePreparedCheckpointCapture() {
      throw new Error("unexpected resume");
    },
  };
}

function safetyCheckpoint(plan) {
  return deepFreeze({
    artifactId: plan.captureArtifactId,
    backendId: "storage-backend-001",
    checkpointClass: "clean",
    checkpointId: plan.captureCheckpointId,
    codexSessionId: THREAD_ID,
    codexThreadId: THREAD_ID,
    contractVersion: 1,
    createdAt: plan.captureCreatedAt,
    imageDigest: IMAGE_DIGEST,
    sessionId: SESSION_ID,
    sourceFencingEpoch: "42",
    storageId: "storage-001",
  });
}

function safetyRequest(plan) {
  return deepFreeze({
    backendId: "storage-backend-001",
    contractVersion: 1,
    fencingEpoch: "42",
    holderId: "writer-holder-001",
    leaseId: "writer-lease-001",
    operation: "checkpoint",
    operationId: plan.captureOperationId,
    sessionId: SESSION_ID,
    storageId: "storage-001",
    target: {
      artifactId: plan.captureArtifactId,
      checkpointId: plan.captureCheckpointId,
      kind: "checkpoint",
    },
  });
}

function authorityOperation({
  createdAt,
  expectedSession,
  kind,
  operationId,
  request,
  result = null,
  revision,
  state,
  updatedAt,
}) {
  const binding = assertSessionOperationBinding({
    expectedSession,
    kind,
    operationId,
    request,
  });
  return deepFreeze({
    conflictClass: SESSION_OPERATION_CONFLICT_CLASS,
    createdAt: createdAt ?? updatedAt,
    expectedSession,
    kind,
    operationId,
    request,
    requestSha256: binding.requestSha256,
    result,
    retiredAt: state === "committed" ? updatedAt : null,
    revision,
    sessionId: SESSION_ID,
    state,
    updatedAt,
  });
}

function operationReservation(operation, state = operation.state) {
  const binding = assertSessionOperationBinding({
    expectedSession: operation.expectedSession,
    kind: operation.kind,
    operationId: operation.operationId,
    request: operation.request,
  });
  return deepFreeze({
    conflictClass: SESSION_OPERATION_CONFLICT_CLASS,
    createdAt: operation.createdAt,
    expectedSessionRevision: operation.expectedSession.revision,
    expiresAt: null,
    kind: operation.kind,
    operationId: operation.operationId,
    releasedAt: state === "released" ? operation.updatedAt : null,
    requestSha256: operation.requestSha256,
    reservationId: binding.reservationId,
    sessionId: SESSION_ID,
    state,
    updatedAt: operation.updatedAt,
  });
}

function sessionAfterOperation(
  previous,
  operation,
  revision,
  documentOverrides = {},
) {
  const reservation = operationReservation(operation, "released");
  return assertSessionAuthoritySnapshot(
    deepFreeze({
      createdAt: previous.createdAt,
      document: {
        ...previous.document,
        activeOperation: null,
        lastOperation: {
          conflictClass: SESSION_OPERATION_CONFLICT_CLASS,
          expectedSessionRevision: operation.expectedSession.revision,
          kind: operation.kind,
          operationId: operation.operationId,
          operationRevision: operation.revision,
          requestSha256: operation.requestSha256,
          reservationId: reservation.reservationId,
          resultSha256: sha256Json(operation.result),
          state: "committed",
        },
        ...documentOverrides,
      },
      revision,
      sessionId: previous.sessionId,
      updatedAt: operation.updatedAt,
    }),
  );
}

function sessionDuringOperation(previous, operation, revision, documentOverrides = {}) {
  const reservation = operationReservation(operation, operation.state);
  return assertSessionAuthoritySnapshot(
    deepFreeze({
      createdAt: previous.createdAt,
      document: {
        ...previous.document,
        activeOperation: {
          conflictClass: SESSION_OPERATION_CONFLICT_CLASS,
          expectedSessionRevision: operation.expectedSession.revision,
          kind: operation.kind,
          operationId: operation.operationId,
          operationRevision: operation.revision,
          requestSha256: operation.requestSha256,
          reservationId: reservation.reservationId,
          state: operation.state,
        },
        ...documentOverrides,
      },
      revision,
      sessionId: previous.sessionId,
      updatedAt: operation.updatedAt,
    }),
  );
}

function revisionAfterOperation(operation) {
  return String(
    BigInt(operation.expectedSession.revision) +
      BigInt(operation.revision) +
      1n,
  );
}

function measuredImage() {
  return deepFreeze({
    projection: {
      codexSandbox: "danger-full-access",
      codexVersion: "codex-cli 0.142.4",
      platformImage: {
        architecture: "arm64",
        config: {
          digest: `sha256:${"b".repeat(64)}`,
          mediaType: "application/vnd.oci.image.config.v1+json",
          size: 128,
        },
        digest: IMAGE_DIGEST,
        mediaType: "application/vnd.oci.image.manifest.v1+json",
        os: "linux",
        size: 256,
      },
    },
    runtimeIdentity: {
      codexBinaryPath: "/usr/local/bin/codex",
      codexBinarySha256: "c".repeat(64),
      codexVersion: "codex-cli 0.142.4",
      platformImageDigest: IMAGE_DIGEST,
    },
  });
}

function terminalLaunchSession(plan, generation, detached) {
  const launchOperation = authorityOperation({
    expectedSession: detached,
    kind: "writer-launch-attempt-v1",
    operationId: plan.launchAttemptId,
    request: deepFreeze({ launch: "prepared" }),
    result: deepFreeze({ outcome: "writer-launch-started" }),
    revision: "2",
    state: "committed",
    updatedAt: "2026-08-11T09:00:07.000Z",
  });
  const attached = {
    ...attachment(),
    operationId: plan.activationOperationId,
    proofId: "restore-attachment-proof-001",
    rootPath: plan.destinationDirectory,
  };
  const writerLease = {
    ...lease("2027-08-11T09:00:07.000Z"),
    holderId: plan.holderId,
    leaseId: "restore-writer-lease-001",
  };
  attached.holderId = writerLease.holderId;
  attached.leaseId = writerLease.leaseId;
  return sessionAfterOperation(detached, launchOperation, "25", {
    attachment: attached,
    launch: {
      attachmentId: attached.attachmentId,
      attachmentSha256: "1".repeat(64),
      contractVersion: 1,
      fencingEpoch: writerLease.fencingEpoch,
      generation: {
        bindingSha256: "2".repeat(64),
        checkpointId: generation.checkpointId,
        claimedAt: generation.claimedAt,
        committedAt: generation.committedAt,
        documentSha256: "3".repeat(64),
        generationId: generation.generationId,
        operationId: generation.operationId,
        sessionId: SESSION_ID,
        state: "committed",
      },
      launchAttemptId: plan.launchAttemptId,
      launchResultSha256: "4".repeat(64),
      leaseId: writerLease.leaseId,
      leaseSha256: "5".repeat(64),
      measuredImageSha256: "6".repeat(64),
      processIncarnationId: "restore-process-001",
      startedAt: launchOperation.updatedAt,
      supervisorId: "supervisor-001",
      supervisorProofId: "restore-supervisor-proof-001",
      writerIncarnationId: "restore-writer-incarnation-001",
    },
    lease: writerLease,
    lifecycle: "ATTACHED",
    writerEpoch: writerLease.fencingEpoch,
  });
}

function absent(code) {
  throw new PostgresSessionAuthorityError(code);
}

function facadeFixture({
  captureBackendValue,
  gate,
  readCheckpointCaptureAttempt,
  readSession = async () => runningSession(),
  renewWriterLease,
  selectOperationGuard,
  transformAuthority,
} = {}) {
  const plan = fixturePlan();
  const calls = { gate: 0, renew: 0, stop: 0 };
  const trace = [];
  const authority = {
    async claimRestoreAttachmentActivationDispatch() {
      throw new Error("unexpected activation claim");
    },
    async claimRestoreDestinationGenerationDispatch() {
      throw new Error("unexpected generation claim");
    },
    async finalizeRestoreDestinationGeneration() {
      throw new Error("unexpected generation finalize");
    },
    async readCheckpointCaptureAttempt() {
      trace.push("capture-read");
      if (readCheckpointCaptureAttempt !== undefined) {
        return readCheckpointCaptureAttempt();
      }
      return absent("checkpoint_capture_not_authorized");
    },
    async readRestoreAttachmentActivation() {
      return absent("restore_attachment_activation_not_authorized");
    },
    async readRestoreDestinationGeneration() {
      trace.push("generation-read");
      return absent("restore_generation_not_authorized");
    },
    readSession(...args) {
      trace.push("session-read");
      return Reflect.apply(readSession, this, args);
    },
    async readWriterLaunchAttempt() {
      return absent("writer_launch_attempt_not_authorized");
    },
    async renewWriterLease(input) {
      calls.renew += 1;
      if (renewWriterLease !== undefined) return renewWriterLease(input);
      throw new Error("unexpected renewal");
    },
    async reserveOperation() {
      throw new Error("unexpected reserve");
    },
  };
  const guardFixture = guards();
  const facade = createPostgresDetachedRestoreForegroundComposition({
    authority:
      transformAuthority === undefined
        ? authority
        : transformAuthority(authority),
    captureBackend: captureBackendValue ?? captureBackend(),
    durableStopCapture: {
      async runPreparedCapture() {
        calls.stop += 1;
        throw new Error("unexpected stop");
      },
    },
    fleetCapabilityGate(input) {
      calls.gate += 1;
      return gate?.(input);
    },
    lifecycleGuard: guardFixture.lifecycleGuard,
    launcher: {
      async prepareLaunchIntent() {
        throw new Error("unexpected launch intent");
      },
      async runPreparedLaunch() {
        throw new Error("unexpected launch");
      },
    },
    operationGuard:
      selectOperationGuard === undefined
        ? guardFixture.operationGuard
        : selectOperationGuard(guardFixture),
    prepareImageReservation() {
      throw new Error("unexpected image preparation");
    },
    resolveStablePlan({ admission: observed }) {
      trace.push("plan-resolve");
      assert.equal(observed.request.operationId, admission().request.operationId);
      return plan;
    },
    restoreActivationCoordinator: {
      async reconcileRestoreAttachmentActivation() {
        throw new Error("unexpected activation reconciliation");
      },
    },
    writerDetach: {
      async detachWriter() {
        throw new Error("unexpected detach");
      },
      async forceFenceWriter() {
        throw new Error("unexpected force fence");
      },
    },
  });
  return { calls, facade, guardQueries: guardFixture.queries, plan, trace };
}

function happyFacadeFixture({
  beforeLaunchReturn = null,
  captureColdState = null,
  crossedGeneration = false,
  crossedGenerationClaim = false,
  crossedLaunchRunRequest = false,
  crossedLaunchRunReservation = false,
  crossedPreparedCapture = null,
  crossedRenewalReservation = false,
} = {}) {
  const plan = fixturePlan();
  const events = [];
  let captureDurable = false;
  const initial = runningSession();
  const renewedLease = deepFreeze({
    contractVersion: initial.document.lease.contractVersion,
    sessionId: initial.document.lease.sessionId,
    leaseId: initial.document.lease.leaseId,
    holderId: initial.document.lease.holderId,
    fencingEpoch: initial.document.lease.fencingEpoch,
    expiresAt: "2027-08-11T08:05:00.000Z",
  });
  const renewalOperation = authorityOperation({
    expectedSession: initial,
    kind: WRITER_LEASE_RENEW_OPERATION_KIND,
    operationId: plan.renewalOperationId,
    request: deepFreeze({
      contractVersion: 1,
      leaseDurationMilliseconds: plan.leaseDurationMilliseconds,
    }),
    result: deepFreeze({
      resultVersion: 1,
      outcome: "writer-lease-renewed",
      lease: renewedLease,
      attachment: initial.document.attachment,
    }),
    revision: "0",
    state: "committed",
    updatedAt: "2026-08-11T09:00:01.000Z",
  });
  const renewed = sessionAfterOperation(
    initial,
    renewalOperation,
    revisionAfterOperation(renewalOperation),
    { lease: renewedLease },
  );
  const checkpoint = safetyCheckpoint(plan);
  const request = safetyRequest(plan);
  const stopOperationId = derivePostgresLogicalWriterStopOperationId({
    attachment: initial.document.attachment,
    checkpoint,
    launchAttemptId: initial.document.launch.launchAttemptId,
    request,
  });
  const captureAdmission = deepFreeze({
    attachment: initial.document.attachment,
    captureAttemptId: "019f7f40-0000-7000-8000-000000000003",
    checkpoint,
    processIncarnationId: initial.document.launch.processIncarnationId,
    request,
    stopOperationId,
    writerIncarnationId: initial.document.launch.writerIncarnationId,
  });
  const stopped = assertSessionAuthoritySnapshot(
    deepFreeze({
      createdAt: renewed.createdAt,
      document: {
        ...renewed.document,
        lastOperation: terminalPointer({
          expectedSessionRevision: renewed.revision,
          kind: WRITER_LAUNCH_STOP_OPERATION_KIND,
          operationId: stopOperationId,
          operationRevision: "2",
          requestSha256: "d".repeat(64),
          reservationId: "reservation-current-stop-001",
          resultSha256: "e".repeat(64),
        }),
        launch: null,
      },
      revision: String(BigInt(renewed.revision) + 3n),
      sessionId: renewed.sessionId,
      updatedAt: "2026-08-11T09:00:01.500Z",
    }),
  );
  const captureAttemptId = captureAdmission.captureAttemptId;
  const captureOperationRequest = createCheckpointCaptureOperationRequest({
    admission: captureAdmission,
    expectedSession: stopped,
  });
  const captureOperation = authorityOperation({
    expectedSession: stopped,
    kind: CHECKPOINT_CAPTURE_OPERATION_KIND,
    operationId: plan.captureOperationId,
    request: captureOperationRequest,
    result: deepFreeze({
      captureAttemptId,
      catalogueSha256: "7".repeat(64),
      checkpointId: plan.captureCheckpointId,
      outcome: "checkpoint-captured",
      resultVersion: 1,
    }),
    revision: "2",
    state: "committed",
    updatedAt: "2026-08-11T09:00:02.000Z",
  });
  const captured = sessionAfterOperation(
    stopped,
    captureOperation,
    revisionAfterOperation(captureOperation),
  );
  const catalogue = deepFreeze({
    document: { artifactProof: { proofId: "source-artifact-proof-001" } },
  });
  const captureReceipt = deepFreeze({
    attempt: null,
    catalogue: deepFreeze({ document: { artifactProof: null } }),
    operation: captureOperation,
    reservation: operationReservation(captureOperation, "released"),
    session: captured,
    status: "committed",
  });
  let coldCaptureReceipt = null;
  if (captureColdState !== null) {
    const revision = {
      prepared: "0",
      starting: "1",
      uncertain: "2",
      committed: "2",
    }[captureColdState];
    assert.notEqual(revision, undefined);
    if (captureColdState === "committed") {
      coldCaptureReceipt = captureReceipt;
    } else {
      const operation = authorityOperation({
        expectedSession: stopped,
        kind: CHECKPOINT_CAPTURE_OPERATION_KIND,
        operationId: plan.captureOperationId,
        request: captureOperationRequest,
        revision,
        state: captureColdState,
        updatedAt: "2026-08-11T09:00:02.000Z",
      });
      let reservation = operationReservation(operation, captureColdState);
      let captureSession = sessionDuringOperation(
        stopped,
        operation,
        revisionAfterOperation(operation),
      );
      if (captureColdState === "prepared") {
        if (crossedPreparedCapture === "reservation") {
          reservation = deepFreeze({
            ...reservation,
            reservationId: "crossed-prepared-capture-reservation-001",
          });
        } else if (crossedPreparedCapture === "active-pointer") {
          captureSession = assertSessionAuthoritySnapshot(
            deepFreeze({
              ...captureSession,
              document: {
                ...captureSession.document,
                activeOperation: {
                  ...captureSession.document.activeOperation,
                  reservationId: "crossed-prepared-capture-pointer-001",
                },
              },
            }),
          );
        } else {
          assert.equal(crossedPreparedCapture, null);
        }
      }
      coldCaptureReceipt = deepFreeze({
        attempt: null,
        catalogue: deepFreeze({ document: { artifactProof: null } }),
        operation,
        reservation,
        session: captureSession,
        status: captureColdState,
      });
    }
  }
  const captureProviderResult = deepFreeze({
    checkpoint,
    mutation: {
      ...request,
      proofId: "capture-proof-001",
      status: "checkpoint-created",
    },
  });
  let generationReceipt = null;
  let activationReceipt = null;
  let launchReceipt = null;
  let generationPrepared = null;
  let activationPrepared = null;
  let publishCompletion = null;

  const generationSnapshot = (state, operation, completion = null) =>
    deepFreeze({
      binding: {
        attachment: initial.document.attachment,
        captureAttemptId: "019f7f40-0000-7000-8000-000000000003",
      },
      checkpointId: admission().checkpoint.checkpointId,
      claimedAt: "2026-08-11T09:00:03.000Z",
      committedAt:
        state === "committed" ? "2026-08-11T09:00:04.000Z" : null,
      document:
        state === "committed"
          ? {
              materialization: completion.materialization,
              result: completion.result,
            }
          : null,
      generationId: plan.generationId,
      operationId: plan.request.operationId,
      sessionId: SESSION_ID,
      state,
    });

  if (crossedGeneration) {
    const crossedCaptureTerminal = assertSessionAuthoritySnapshot(
      deepFreeze({
        ...captured,
        document: {
          ...captured.document,
          lastOperation: {
            ...captured.document.lastOperation,
            reservationId: "crossed-capture-reservation-001",
          },
        },
      }),
    );
    const generationRequest = createRestoreDestinationGenerationOperationRequest({
      admission: admission(),
      expectedSession: crossedCaptureTerminal,
    });
    const operation = authorityOperation({
      expectedSession: crossedCaptureTerminal,
      kind: "restore-destination-generation-v1",
      operationId: plan.request.operationId,
      request: generationRequest,
      revision: "1",
      state: "starting",
      updatedAt: "2026-08-11T09:00:03.000Z",
    });
    generationReceipt = deepFreeze({
      catalogue,
      generation: generationSnapshot("authorized", operation),
      operation,
      reservation: operationReservation(operation, "starting"),
      session: crossedCaptureTerminal,
    });
  }

  const authority = {
    async claimRestoreAttachmentActivationDispatch(input) {
      events.push("activation-claim");
      const operation = authorityOperation({
        expectedSession: input.expectedSession,
        kind: input.kind,
        operationId: input.operationId,
        request: input.request,
        revision: "1",
        state: "starting",
        updatedAt: "2026-08-11T09:00:06.000Z",
      });
      activationReceipt = deepFreeze({
        activationRequest: { providerRequestId: "provider-request-001" },
        dispatchGranted: true,
        generation: generationReceipt.generation,
        operation,
        reservation: operationReservation(operation, "starting"),
        session: input.expectedSession,
      });
      return activationReceipt;
    },
    async claimRestoreDestinationGenerationDispatch(input) {
      events.push("generation-claim");
      const operation = authorityOperation({
        expectedSession: input.expectedSession,
        kind: input.kind,
        operationId: input.operationId,
        request: input.request,
        revision: "1",
        state: "starting",
        updatedAt: "2026-08-11T09:00:03.000Z",
      });
      let claimSession = sessionDuringOperation(
        captured,
        operation,
        revisionAfterOperation(operation),
      );
      if (crossedGenerationClaim) {
        claimSession = assertSessionAuthoritySnapshot(
          deepFreeze({
            ...claimSession,
            document: {
              ...claimSession.document,
              activeOperation: {
                ...claimSession.document.activeOperation,
                reservationId: "crossed-generation-reservation-001",
              },
            },
          }),
        );
      }
      generationReceipt = deepFreeze({
        catalogue,
        dispatchGranted: true,
        generation: generationSnapshot("authorized", operation),
        operation,
        reservation: operationReservation(operation, "starting"),
        session: claimSession,
      });
      return generationReceipt;
    },
    async finalizeRestoreDestinationGeneration(input) {
      events.push("generation-finalize");
      assert.equal(input.completion, publishCompletion);
      const terminalRevision =
        input.expectedOperationRevision === "2" ? "3" : "2";
      const operation = authorityOperation({
        createdAt: generationReceipt.operation.createdAt,
        expectedSession: input.expectedSession,
        kind: input.kind,
        operationId: input.operationId,
        request: input.request,
        result: input.completion.result,
        revision: terminalRevision,
        state: "committed",
        updatedAt: "2026-08-11T09:00:04.000Z",
      });
      const session = sessionAfterOperation(
        captured,
        operation,
        revisionAfterOperation(operation),
      );
      generationReceipt = deepFreeze({
        catalogue,
        generation: generationSnapshot("committed", operation, input.completion),
        operation,
        reservation: operationReservation(operation, "released"),
        session,
      });
      return generationReceipt;
    },
    async readCheckpointCaptureAttempt() {
      if (
        captureColdState !== null &&
        !events.includes("resume") &&
        !events.includes("reconcile")
      ) {
        return coldCaptureReceipt;
      }
      if (
        crossedGeneration ||
        captureColdState !== null ||
        captureDurable
      ) {
        return captureReceipt;
      }
      return absent("checkpoint_capture_not_authorized");
    },
    async readRestoreAttachmentActivation() {
      if (activationReceipt !== null) return activationReceipt;
      return absent("restore_attachment_activation_not_authorized");
    },
    async readRestoreDestinationGeneration() {
      if (generationReceipt !== null) return generationReceipt;
      return absent("restore_generation_not_authorized");
    },
    async readSession() {
      return initial;
    },
    async readWriterLaunchAttempt() {
      if (launchReceipt !== null) return launchReceipt;
      return absent("writer_launch_attempt_not_authorized");
    },
    async renewWriterLease(input) {
      events.push("renew");
      assert.equal(input.operationId, plan.renewalOperationId);
      const reservation = operationReservation(renewalOperation, "released");
      return deepFreeze({
        operation: renewalOperation,
        reservation: crossedRenewalReservation
          ? {
              ...reservation,
              reservationId: "crossed-renewal-reservation-001",
            }
          : reservation,
        session: renewed,
      });
    },
    async reserveOperation(input) {
      if (input.kind === "restore-destination-generation-v1") {
        events.push("generation-reserve");
        const operation = authorityOperation({
          expectedSession: input.expectedSession,
          kind: input.kind,
          operationId: input.operationId,
          request: input.request,
          revision: "0",
          state: "prepared",
          updatedAt: "2026-08-11T09:00:03.000Z",
        });
        generationPrepared = deepFreeze({
          operation,
          reservation: operationReservation(operation, "prepared"),
          session: captured,
        });
        return generationPrepared;
      }
      events.push("activation-reserve");
      const operation = authorityOperation({
        expectedSession: input.expectedSession,
        kind: input.kind,
        operationId: input.operationId,
        request: input.request,
        revision: "0",
        state: "prepared",
        updatedAt: "2026-08-11T09:00:06.000Z",
      });
      activationPrepared = deepFreeze({
        operation,
        reservation: operationReservation(operation, "prepared"),
        session: input.expectedSession,
      });
      return activationPrepared;
    },
  };

  const guardFixture = guards();
  const imageReservation = deepFreeze({
    configBytes: "config-bytes",
    descriptor: { digest: IMAGE_DIGEST },
    inspectCodex: Object.freeze(async () => undefined),
    reservation: Object.freeze(Object.create(null)),
  });
  const launchIntent = deepFreeze({
    launchAttemptId: plan.launchAttemptId,
    measuredImage: measuredImage(),
    supervisor: { contractVersion: 1, supervisorId: "supervisor-001" },
  });
  let detached = null;
  const facade = createPostgresDetachedRestoreForegroundComposition({
    authority,
    captureBackend: {
      ...captureBackend(),
      async reconcileCheckpointCapture() {
        events.push("reconcile");
        return captureProviderResult;
      },
      async resumePreparedCheckpointCapture() {
        events.push("resume");
        return captureProviderResult;
      },
    },
    durableStopCapture: {
      async runPreparedCapture(input) {
        events.push("stop");
        captureDurable = true;
        assert.equal(input.request.operationId, plan.captureOperationId);
        return captureProviderResult;
      },
    },
    fleetCapabilityGate() {
      events.push("gate");
      return POSTGRES_DETACHED_RESTORE_FLEET_CONFIRMED;
    },
    lifecycleGuard: guardFixture.lifecycleGuard,
    launcher: {
      async prepareLaunchIntent(input) {
        events.push("prepare-intent");
        assert.equal(input.launchAttemptId, plan.launchAttemptId);
        return launchIntent;
      },
      async runPreparedLaunch(input) {
        events.push("launch");
        assert.notEqual(launchReceipt, null);
        const expectedLaunchOperation = launchReceipt.operation;
        const expectedSession = expectedLaunchOperation.expectedSession;
        const requestValue = crossedLaunchRunRequest
          ? deepFreeze({
              ...expectedLaunchOperation.request,
              supervisor: {
                ...expectedLaunchOperation.request.supervisor,
                supervisorId: "crossed-terminal-supervisor-001",
              },
            })
          : expectedLaunchOperation.request;
        const evidence = deepFreeze({
          contractVersion: 1,
          launchAttemptId: plan.launchAttemptId,
          processIncarnationId: "restore-process-001",
          proofId: "restore-supervisor-proof-001",
          status: "started",
          supervisorId: requestValue.supervisor.supervisorId,
          writerIncarnationId: "restore-writer-incarnation-001",
        });
        const terminalRevision =
          expectedLaunchOperation.state === "uncertain" ? "3" : "2";
        const operation = authorityOperation({
          createdAt: expectedLaunchOperation.createdAt,
          expectedSession,
          kind: "writer-launch-attempt-v1",
          operationId: plan.launchAttemptId,
          request: requestValue,
          result: {
            evidence,
            outcome: "writer-launch-started",
            resultVersion: 1,
          },
          revision: terminalRevision,
          state: "committed",
          updatedAt: "2026-08-11T09:00:07.000Z",
        });
        const launchPointer = {
          attachmentId: requestValue.attachment.attachmentId,
          attachmentSha256: sha256Json(requestValue.attachment),
          contractVersion: 1,
          fencingEpoch: requestValue.fencingEpoch,
          generation: requestValue.generation,
          launchAttemptId: plan.launchAttemptId,
          launchResultSha256: sha256Json(operation.result),
          leaseId: requestValue.lease.leaseId,
          leaseSha256: sha256Json(requestValue.lease),
          measuredImageSha256: sha256Json(requestValue.measuredImage),
          processIncarnationId: evidence.processIncarnationId,
          startedAt: operation.updatedAt,
          supervisorId: evidence.supervisorId,
          supervisorProofId: evidence.proofId,
          writerIncarnationId: evidence.writerIncarnationId,
        };
        const session = sessionAfterOperation(
          expectedSession,
          operation,
          revisionAfterOperation(operation),
          { launch: launchPointer },
        );
        let terminalReservation = operationReservation(operation, "released");
        let terminalSession = session;
        if (crossedLaunchRunReservation) {
          terminalReservation = deepFreeze({
            ...terminalReservation,
            reservationId: "crossed-terminal-reservation-001",
          });
          terminalSession = assertSessionAuthoritySnapshot(
            deepFreeze({
              ...session,
              document: {
                ...session.document,
                lastOperation: {
                  ...session.document.lastOperation,
                  reservationId: terminalReservation.reservationId,
                },
              },
            }),
          );
        }
        launchReceipt = deepFreeze({
          attempt: {
            launchAttemptId: plan.launchAttemptId,
            request: operation.request,
            result: operation.result,
            state: "committed",
          },
          launch: terminalSession.document.launch,
          operation,
          reservation: terminalReservation,
          session: terminalSession,
        });
        assert.equal(input.imageReservation.reservation, imageReservation.reservation);
        if (beforeLaunchReturn !== null) beforeLaunchReturn();
        return deepFreeze({
          attempt: launchReceipt.attempt,
          contractVersion: 1,
          evidence,
          launch: launchReceipt.launch,
          operation,
          reservation: launchReceipt.reservation,
          session: launchReceipt.session,
          status: "started",
          writer: deepFreeze(Object.create(null)),
        });
      },
    },
    operationGuard: guardFixture.operationGuard,
    prepareImageReservation() {
      events.push("image");
      return imageReservation;
    },
    resolveStablePlan() {
      return plan;
    },
    restoreActivationCoordinator: {
      async reconcileRestoreAttachmentActivation(candidate) {
        events.push("activation-coordinator");
        assert.equal(candidate.request, activationReceipt.operation.request);
        const operationId = plan.activationOperationId;
        const writerLease = deepFreeze({
          contractVersion: 1,
          sessionId: SESSION_ID,
          leaseId: `lease-${sha256Text(`writer-lease:${operationId}`)}`,
          holderId: plan.holderId,
          fencingEpoch: "43",
          expiresAt: "2027-08-11T09:00:06.000Z",
        });
        const attachmentId =
          `attachment-${sha256Text(`writer-attachment:${operationId}`)}`;
        const materialization = generationReceipt.generation.document.materialization;
        const publication = deepFreeze({
          artifactManifestDigest: materialization.artifactManifestDigest,
          coordinatorBindingSha256: materialization.coordinatorBindingSha256,
          modeledDigest: materialization.modeledDigest,
          publicationId: materialization.publicationId,
          publicationKind: materialization.publicationKind,
          root: {
            filesystemId: materialization.stagedRoot.filesystemId,
            objectIdentityScheme:
              materialization.stagedRoot.objectIdentityScheme,
            objectId: materialization.stagedRoot.objectId,
            rootPath: plan.destinationDirectory,
          },
          treeIdentityDigest: materialization.treeIdentityDigest,
        });
        const mutationRequest = deepFreeze({
          backendId: detached.document.storageRef.backendId,
          contractVersion: 1,
          fencingEpoch: writerLease.fencingEpoch,
          holderId: writerLease.holderId,
          leaseId: writerLease.leaseId,
          operation: "attach",
          operationId,
          sessionId: SESSION_ID,
          storageId: detached.document.storageRef.storageId,
          target: { attachmentId, kind: "attachment" },
        });
        const providerRequest = assertRestoreAttachmentActivationRequest({
          contractVersion: 1,
          lease: writerLease,
          manifest: detached.document.manifest,
          mutationRequest,
          publication,
          storageRef: detached.document.storageRef,
        });
        const mutationResult = deepFreeze({
          ...mutationRequest,
          proofId: "restore-attachment-proof-001",
          status: "attached",
        });
        const attached = deepFreeze({
          contractVersion: 1,
          backendId: detached.document.storageRef.backendId,
          storageId: detached.document.storageRef.storageId,
          sessionId: SESSION_ID,
          attachmentId,
          leaseId: writerLease.leaseId,
          holderId: writerLease.holderId,
          fencingEpoch: writerLease.fencingEpoch,
          operationId,
          proofId: mutationResult.proofId,
          kind: "directory",
          rootPath: plan.destinationDirectory,
          mode: "read-write",
        });
        const providerResult = assertRestoreAttachmentActivationResult(
          {
            attachment: attached,
            contractVersion: 1,
            mutationResult,
            publication,
          },
          { request: providerRequest },
        );
        const storedProviderRequest = canonicalData(providerRequest);
        const storedProviderResult = canonicalData(providerResult);
        const activationOperation = authorityOperation({
          expectedSession: activationReceipt.operation.expectedSession,
          kind: "restore-attachment-activation-v1",
          operationId: plan.activationOperationId,
          request: activationReceipt.operation.request,
          result: {
            activationRequest: storedProviderRequest,
            activationResult: storedProviderResult,
            outcome: "restore-attachment-activated",
            resultVersion: 1,
          },
          revision: "2",
          state: "committed",
          updatedAt: "2026-08-11T09:00:06.000Z",
        });
        const activationTerminal = sessionAfterOperation(
          detached,
          activationOperation,
          revisionAfterOperation(activationOperation),
          {
            attachment: providerResult.attachment,
            launch: null,
            lease: providerRequest.lease,
            lifecycle: "ATTACHED",
            writerEpoch: providerRequest.lease.fencingEpoch,
          },
        );
        const launchRequest = createWriterLaunchAttemptOperationRequest({
          expectedSession: activationTerminal,
          generation: generationReceipt.generation,
          measuredImage: launchIntent.measuredImage,
          supervisor: launchIntent.supervisor,
        });
        const launchOperation = authorityOperation({
          expectedSession: activationTerminal,
          kind: "writer-launch-attempt-v1",
          operationId: plan.launchAttemptId,
          request: launchRequest,
          revision: "0",
          state: "prepared",
          updatedAt: "2026-08-11T09:00:06.000Z",
        });
        const activationReservation = operationReservation(
          activationOperation,
          "released",
        );
        const launchReservation = operationReservation(
          launchOperation,
          "prepared",
        );
        const launchActive = sessionDuringOperation(
          activationTerminal,
          launchOperation,
          revisionAfterOperation(launchOperation),
        );
        activationReceipt = deepFreeze({
          activationRequest: providerRequest,
          generation: generationReceipt.generation,
          operation: activationOperation,
          reservation: activationReservation,
          session: activationTerminal,
        });
        launchReceipt = deepFreeze({
          attempt: {
            launchAttemptId: plan.launchAttemptId,
            request: launchOperation.request,
            result: null,
            state: "prepared",
          },
          launch: null,
          operation: launchOperation,
          reservation: launchReservation,
          session: launchActive,
        });
        return deepFreeze({
          activation: {
            finalized: true,
            operation: activationOperation,
            reservation: activationReservation,
          },
          generation: generationReceipt.generation,
          launch: {
            attempt: launchReceipt.attempt,
            operation: launchOperation,
            reservation: launchReservation,
          },
          session: launchActive,
          status: "prepared",
        });
      },
    },
    writerDetach: {
      async detachWriter(input) {
        events.push("detach");
        const requestValue = deepFreeze({
          contractVersion: 1,
          target: input.target,
        });
        const operation = authorityOperation({
          expectedSession: input.expectedSession,
          kind: "writer-release-v1",
          operationId: plan.detachOperationId,
          request: requestValue,
          result: {
            resultVersion: 1,
            outcome: "writer-released",
            lease: input.expectedSession.document.lease,
            attachment: input.expectedSession.document.attachment,
            mutationResult: {
              contractVersion: 1,
              backendId: input.expectedSession.document.storageRef.backendId,
              storageId: input.expectedSession.document.storageRef.storageId,
              sessionId: SESSION_ID,
              leaseId: input.expectedSession.document.lease.leaseId,
              holderId: input.expectedSession.document.lease.holderId,
              fencingEpoch: input.expectedSession.document.lease.fencingEpoch,
              operation: "detach",
              operationId: plan.detachOperationId,
              target: input.target,
              proofId: "writer-release-proof-001",
              status: "detached",
            },
          },
          revision: "2",
          state: "committed",
          updatedAt: "2026-08-11T09:00:05.000Z",
        });
        detached = sessionAfterOperation(
          generationReceipt.session,
          operation,
          revisionAfterOperation(operation),
          {
            attachment: null,
            launch: null,
            lease: null,
            lifecycle: "DETACHED",
          },
        );
        return deepFreeze({
          operation,
          reservation: operationReservation(operation, "released"),
          session: detached,
        });
      },
      async forceFenceWriter() {
        assert.fail("force fence called on release plan");
      },
    },
  });
  return {
    corruptReplayReceipt(kind) {
      if (kind === "activation-generation") {
        activationReceipt = deepFreeze({
          ...activationReceipt,
          generation: {
            ...activationReceipt.generation,
            generationId: "crossed-generation-001",
          },
        });
        return;
      }
      if (kind === "launch-request") {
        const request = deepFreeze({
          ...launchReceipt.operation.request,
          supervisor: {
            ...launchReceipt.operation.request.supervisor,
            supervisorId: "crossed-supervisor-001",
          },
        });
        launchReceipt = deepFreeze({
          ...launchReceipt,
          attempt: { ...launchReceipt.attempt, request },
          operation: { ...launchReceipt.operation, request },
        });
        return;
      }
      assert.equal(kind, "launch-reservation");
      launchReceipt = deepFreeze({
        ...launchReceipt,
        reservation: {
          ...launchReceipt.reservation,
          reservationId: "crossed-launch-reservation-001",
        },
      });
    },
    events,
    facade,
    setActivationReplayState(state) {
      assert.ok(
        ["prepared", "starting", "uncertain", "committed"].includes(state),
      );
      assert.equal(activationReceipt?.operation.state, "committed");
      assert.notEqual(detached, null);
      launchReceipt = null;
      if (state === "committed") return;
      const committed = activationReceipt;
      const revision = { prepared: "0", starting: "1", uncertain: "2" }[
        state
      ];
      const operation = authorityOperation({
        createdAt: committed.operation.createdAt,
        expectedSession: committed.operation.expectedSession,
        kind: committed.operation.kind,
        operationId: committed.operation.operationId,
        request: committed.operation.request,
        revision,
        state,
        updatedAt: committed.operation.createdAt,
      });
      activationReceipt = deepFreeze({
        activationRequest: committed.activationRequest,
        generation: generationReceipt.generation,
        operation,
        reservation: operationReservation(operation, state),
        session: sessionDuringOperation(
          detached,
          operation,
          revisionAfterOperation(operation),
        ),
      });
    },
    setGenerationReplayState(state) {
      assert.ok(["starting", "uncertain", "committed"].includes(state));
      assert.equal(generationReceipt?.operation.state, "committed");
      activationReceipt = null;
      launchReceipt = null;
      if (state === "committed") return;
      const committed = generationReceipt;
      const revision = state === "starting" ? "1" : "2";
      const operation = authorityOperation({
        createdAt: committed.operation.createdAt,
        expectedSession: committed.operation.expectedSession,
        kind: committed.operation.kind,
        operationId: committed.operation.operationId,
        request: committed.operation.request,
        revision,
        state,
        updatedAt:
          state === "starting"
            ? committed.operation.createdAt
            : "2026-08-11T09:00:03.500Z",
      });
      generationReceipt = deepFreeze({
        catalogue,
        generation: generationSnapshot("authorized", operation),
        operation,
        reservation: operationReservation(operation, state),
        session: sessionDuringOperation(
          captured,
          operation,
          revisionAfterOperation(operation),
        ),
      });
    },
    setLaunchReplayState(state) {
      assert.ok(["starting", "uncertain"].includes(state));
      assert.equal(activationReceipt?.operation.state, "committed");
      assert.equal(launchReceipt?.operation.state, "committed");
      const committed = launchReceipt;
      const revision = state === "starting" ? "1" : "2";
      const operation = authorityOperation({
        createdAt: committed.operation.createdAt,
        expectedSession: committed.operation.expectedSession,
        kind: committed.operation.kind,
        operationId: committed.operation.operationId,
        request: committed.operation.request,
        revision,
        state,
        updatedAt:
          state === "starting"
            ? committed.operation.createdAt
            : "2026-08-11T09:00:06.500Z",
      });
      launchReceipt = deepFreeze({
        attempt: {
          launchAttemptId: plan.launchAttemptId,
          request: operation.request,
          result: null,
          state,
        },
        launch: null,
        operation,
        reservation: operationReservation(operation, state),
        session: sessionDuringOperation(
          operation.expectedSession,
          operation,
          revisionAfterOperation(operation),
        ),
      });
    },
    get publishCompletion() {
      return publishCompletion;
    },
    set publishCompletion(value) {
      publishCompletion = value;
    },
  };
}

async function seedCommittedRestore(fixture, publicationId) {
  let completion;
  const result = await fixture.facade.runRestore(
    admission(),
    async (context) => {
      fixture.events.push("publish");
      completion = deepFreeze({
        materialization: restoreMaterialization(publicationId),
        replayed: false,
        result: context.result,
      });
      fixture.publishCompletion = completion;
      return completion;
    },
  );
  assert.equal(result, completion);
  return completion;
}

async function rejectsWithCode(promise, code) {
  await assert.rejects(promise, (error) => {
    assert.equal(
      error instanceof PostgresDetachedRestoreForegroundCompositionError,
      true,
      `${error?.name}:${error?.code}`,
    );
    assert.equal(error.code, code);
    assert.equal(Object.isFrozen(error), true);
    return true;
  });
}

test("factory returns an exact branded frozen V3 facade", () => {
  const { facade } = facadeFixture({ gate: () => null });
  assert.deepEqual(Reflect.ownKeys(facade), [
    "restoreContextContractVersion",
    "runRestore",
  ]);
  assert.equal(facade.restoreContextContractVersion, 3);
  assert.equal(Object.getPrototypeOf(facade), null);
  assert.equal(Object.isFrozen(facade), true);
  assert.equal(Object.isFrozen(facade.runRestore), true);
  assert.equal(isPostgresDetachedRestoreForegroundComposition(facade), true);
  assert.equal(
    isPostgresDetachedRestoreForegroundComposition(
      Object.freeze({ ...facade }),
    ),
    false,
  );
});

test("factory rejects a proxy in a collaborator prototype chain without traps", () => {
  let trapCalls = 0;
  const prototype = new Proxy(Object.create(null), {
    getOwnPropertyDescriptor() {
      trapCalls += 1;
      throw new Error("prototype descriptor trap must not run");
    },
    getPrototypeOf() {
      trapCalls += 1;
      throw new Error("prototype traversal trap must not run");
    },
  });
  assert.throws(
    () =>
      facadeFixture({
        gate: () => null,
        transformAuthority: () => Object.create(prototype),
      }),
    (error) =>
      error instanceof PostgresDetachedRestoreForegroundCompositionError &&
      error.code ===
        "invalid_postgres_detached_restore_foreground_composition_options",
  );
  assert.equal(trapCalls, 0);
});

test("factory preflights the complete capture backend prototype chain without traps", () => {
  const trapCalls = { get: 0, getOwnPropertyDescriptor: 0, getPrototypeOf: 0 };
  const prototype = new Proxy(Object.create(null), {
    get() {
      trapCalls.get += 1;
      throw new Error("capture backend get trap must not run");
    },
    getOwnPropertyDescriptor() {
      trapCalls.getOwnPropertyDescriptor += 1;
      throw new Error("capture backend descriptor trap must not run");
    },
    getPrototypeOf() {
      trapCalls.getPrototypeOf += 1;
      throw new Error("capture backend prototype trap must not run");
    },
  });
  const captureBackendValue = Object.assign(
    Object.create(prototype),
    captureBackend(),
  );
  let businessCalls = 0;
  captureBackendValue.reconcileCheckpointCapture = async () => {
    businessCalls += 1;
  };
  captureBackendValue.resumePreparedCheckpointCapture = async () => {
    businessCalls += 1;
  };
  let queries;
  assert.throws(
    () =>
      facadeFixture({
        captureBackendValue,
        gate: () => null,
        selectOperationGuard: (guardFixture) => {
          queries = guardFixture.queries;
          return guardFixture.operationGuard;
        },
      }),
    (error) =>
      error instanceof PostgresDetachedRestoreForegroundCompositionError &&
      error.code ===
        "invalid_postgres_detached_restore_foreground_composition_options",
  );
  assert.deepEqual(trapCalls, {
    get: 0,
    getOwnPropertyDescriptor: 0,
    getPrototypeOf: 0,
  });
  assert.equal(businessCalls, 0);
  assert.deepEqual(queries, []);
});

for (const field of [
  "contractVersion",
  "backendId",
  "capabilities",
  "captureCheckpoint",
]) {
  test(`factory rejects a capture backend ${field} accessor without invoking it`, () => {
    const captureBackendValue = captureBackend();
    const originalValue = captureBackendValue[field];
    let getterCalls = 0;
    let businessCalls = 0;
    Object.defineProperty(captureBackendValue, field, {
      configurable: true,
      enumerable: true,
      get() {
        getterCalls += 1;
        return originalValue;
      },
    });
    captureBackendValue.reconcileCheckpointCapture = async () => {
      businessCalls += 1;
    };
    captureBackendValue.resumePreparedCheckpointCapture = async () => {
      businessCalls += 1;
    };
    let queries;
    assert.throws(
      () =>
        facadeFixture({
          captureBackendValue,
          gate: () => null,
          selectOperationGuard: (guardFixture) => {
            queries = guardFixture.queries;
            return guardFixture.operationGuard;
          },
        }),
      (error) =>
        error instanceof PostgresDetachedRestoreForegroundCompositionError &&
        error.code ===
          "invalid_postgres_detached_restore_foreground_composition_options",
    );
    assert.equal(getterCalls, 0);
    assert.equal(businessCalls, 0);
    assert.deepEqual(queries, []);
  });
}

for (const key of ["foregroundOperationGuard", "recoveryOperationGuard"]) {
  test(`factory rejects an operation guard reused from the lifecycle ${key}`, () => {
    let queries;
    assert.throws(
      () =>
        facadeFixture({
          gate: () => null,
          selectOperationGuard: (guardFixture) => {
            queries = guardFixture.queries;
            return guardFixture[key];
          },
        }),
      (error) =>
        error instanceof PostgresDetachedRestoreForegroundCompositionError &&
        error.code ===
          "invalid_postgres_detached_restore_foreground_composition_options",
    );
    assert.deepEqual(queries, []);
  });
}

test("factory rejects a different operation guard backed by the lifecycle pool", () => {
  let queries;
  assert.throws(
    () =>
      facadeFixture({
        gate: () => null,
        selectOperationGuard: (guardFixture) => {
          queries = guardFixture.queries;
          assert.notEqual(
            guardFixture.sameForegroundPoolOperationGuard,
            guardFixture.foregroundOperationGuard,
          );
          return guardFixture.sameForegroundPoolOperationGuard;
        },
      }),
    (error) =>
      error instanceof PostgresDetachedRestoreForegroundCompositionError &&
      error.code ===
        "invalid_postgres_detached_restore_foreground_composition_options",
  );
  assert.deepEqual(queries, []);
});

test("fresh restore runs every effect once and preserves publication identity", async () => {
  const fixture = happyFacadeFixture();
  let completion;
  const result = await fixture.facade.runRestore(admission(), async (context) => {
    fixture.events.push("publish");
    assert.equal(context.publicationMode, "fresh-or-exact-replay");
    assert.equal(
      context.artifactDirectory,
      "/var/lib/portable-codex/artifacts/source-001",
    );
    assert.equal(
      context.destinationDirectory,
      "/var/lib/portable-codex/restores/restore-001",
    );
    completion = deepFreeze({
      materialization: restoreMaterialization("materialization-001"),
      replayed: false,
      result: context.result,
    });
    fixture.publishCompletion = completion;
    return completion;
  });

  assert.equal(result, completion);
  assert.deepEqual(fixture.events, [
    "gate",
    "renew",
    "stop",
    "generation-reserve",
    "generation-claim",
    "publish",
    "generation-finalize",
    "detach",
    "image",
    "prepare-intent",
    "activation-reserve",
    "activation-claim",
    "activation-coordinator",
    "launch",
  ]);
});

test("renewal receipt must prove the exact reservation before writer stop", async () => {
  const fixture = happyFacadeFixture({ crossedRenewalReservation: true });
  await rejectsWithCode(
    fixture.facade.runRestore(
      admission(),
      async () => assert.fail("invalid renewal must not publish"),
    ),
    "postgres_detached_restore_foreground_composition_outcome_uncertain",
  );
  assert.deepEqual(fixture.events, ["gate", "renew"]);
});

test("launcher terminal must remain bound to the prepared launch request", async () => {
  const fixture = happyFacadeFixture({ crossedLaunchRunRequest: true });
  await rejectsWithCode(
    fixture.facade.runRestore(admission(), async (context) => {
      fixture.events.push("publish");
      const completion = deepFreeze({
        materialization: restoreMaterialization("crossed-launch-terminal-001"),
        replayed: false,
        result: context.result,
      });
      fixture.publishCompletion = completion;
      return completion;
    }),
    "postgres_detached_restore_foreground_composition_outcome_uncertain",
  );
  assert.equal(fixture.events.at(-1), "launch");
  assert.equal(
    fixture.events.filter((entry) => entry === "launch").length,
    1,
  );
});

test("launcher reservation binding ignores callback-time iterator poisoning", async () => {
  const originalIterator = Object.getOwnPropertyDescriptor(
    Array.prototype,
    Symbol.iterator,
  );
  let poisoned = false;
  const restoreIterator = () => {
    if (!poisoned) return;
    poisoned = false;
    if (originalIterator === undefined) delete Array.prototype[Symbol.iterator];
    else {
      Object.defineProperty(
        Array.prototype,
        Symbol.iterator,
        originalIterator,
      );
    }
  };
  const fixture = happyFacadeFixture({
    beforeLaunchReturn() {
      Object.defineProperty(Array.prototype, Symbol.iterator, {
        configurable: true,
        enumerable: false,
        value() {
          return {
            next() {
              return { done: true, value: undefined };
            },
          };
        },
        writable: true,
      });
      poisoned = true;
    },
    crossedLaunchRunReservation: true,
  });
  let observed;
  let releaseObservation;
  const observation = new Promise((resolve) => {
    releaseObservation = resolve;
  });
  try {
    const pending = fixture.facade.runRestore(admission(), async (context) => {
      fixture.events.push("publish");
      const completion = deepFreeze({
        materialization: restoreMaterialization("iterator-poison-launch-001"),
        replayed: false,
        result: context.result,
      });
      fixture.publishCompletion = completion;
      return completion;
    });
    pending.then(
      (value) => {
        observed = { status: "fulfilled", value };
        restoreIterator();
        releaseObservation();
      },
      (error) => {
        observed = { error, status: "rejected" };
        restoreIterator();
        releaseObservation();
      },
    );
    await observation;
  } finally {
    restoreIterator();
  }
  assert.equal(observed.status, "rejected");
  assert.equal(
    observed.error instanceof PostgresDetachedRestoreForegroundCompositionError,
    true,
  );
  assert.equal(
    observed.error.code,
    "postgres_detached_restore_foreground_composition_outcome_uncertain",
  );
  assert.equal(fixture.events.at(-1), "launch");
});

test("gate-closed committed replay verifies publication and adopts the durable launch", async () => {
  const fixture = happyFacadeFixture();
  let firstCompletion;
  await fixture.facade.runRestore(admission(), async (context) => {
    fixture.events.push("publish");
    firstCompletion = deepFreeze({
      materialization: restoreMaterialization("committed-replay-001"),
      replayed: false,
      result: context.result,
    });
    fixture.publishCompletion = firstCompletion;
    return firstCompletion;
  });

  fixture.events.length = 0;
  let replayCompletion;
  const result = await fixture.facade.runRestore(admission(), async (context) => {
    fixture.events.push("publish");
    assert.equal(context.publicationMode, "committed-only");
    replayCompletion = deepFreeze({
      materialization: firstCompletion.materialization,
      replayed: true,
      result: context.result,
    });
    return replayCompletion;
  });

  assert.equal(result, replayCompletion);
  assert.deepEqual(fixture.events, ["publish", "image", "launch"]);
});

for (const state of ["starting", "uncertain", "committed"]) {
  test(`existing generation ${state} uses committed-only publication routing`, async () => {
    const fixture = happyFacadeFixture();
    const seed = await seedCommittedRestore(
      fixture,
      `generation-route-seed-${state}`,
    );
    fixture.setGenerationReplayState(state);
    fixture.events.length = 0;
    let completion;
    const result = await fixture.facade.runRestore(
      admission(),
      async (context) => {
        fixture.events.push("publish");
        assert.equal(context.publicationMode, "committed-only");
        completion = deepFreeze({
          materialization:
            state === "committed"
              ? seed.materialization
              : restoreMaterialization(`generation-route-${state}`),
          replayed: true,
          result: context.result,
        });
        fixture.publishCompletion = completion;
        return completion;
      },
    );
    assert.equal(result, completion);
    assert.equal(
      fixture.events.filter((entry) => entry === "generation-finalize").length,
      state === "committed" ? 0 : 1,
    );
    for (const forbidden of [
      "gate",
      "renew",
      "stop",
      "generation-reserve",
      "generation-claim",
    ]) {
      assert.equal(fixture.events.includes(forbidden), false);
    }
  });
}

for (const state of ["prepared", "starting", "uncertain", "committed"]) {
  test(`existing activation ${state} without launch routes through the coordinator`, async () => {
    const fixture = happyFacadeFixture();
    const seed = await seedCommittedRestore(
      fixture,
      `activation-route-seed-${state}`,
    );
    fixture.setActivationReplayState(state);
    fixture.events.length = 0;
    let completion;
    const result = await fixture.facade.runRestore(
      admission(),
      async (context) => {
        fixture.events.push("publish");
        assert.equal(context.publicationMode, "committed-only");
        completion = deepFreeze({
          materialization: seed.materialization,
          replayed: true,
          result: context.result,
        });
        return completion;
      },
    );
    assert.equal(result, completion);
    assert.equal(
      fixture.events.filter((entry) => entry === "activation-claim").length,
      state === "prepared" ? 1 : 0,
    );
    assert.equal(
      fixture.events.filter((entry) => entry === "activation-coordinator").length,
      1,
    );
    assert.equal(fixture.events.filter((entry) => entry === "launch").length, 1);
    for (const forbidden of [
      "detach",
      "prepare-intent",
      "activation-reserve",
    ]) {
      assert.equal(fixture.events.includes(forbidden), false);
    }
  });
}

for (const state of ["starting", "uncertain"]) {
  test(`existing launch ${state} runs the prepared-launch continuation once`, async () => {
    const fixture = happyFacadeFixture();
    const seed = await seedCommittedRestore(
      fixture,
      `launch-route-seed-${state}`,
    );
    fixture.setLaunchReplayState(state);
    fixture.events.length = 0;
    let completion;
    const result = await fixture.facade.runRestore(
      admission(),
      async (context) => {
        fixture.events.push("publish");
        assert.equal(context.publicationMode, "committed-only");
        completion = deepFreeze({
          materialization: seed.materialization,
          replayed: true,
          result: context.result,
        });
        return completion;
      },
    );
    assert.equal(result, completion);
    assert.deepEqual(fixture.events, ["publish", "image", "launch"]);
  });
}

for (const corruption of [
  "activation-generation",
  "launch-request",
  "launch-reservation",
]) {
  test(`committed replay rejects crossed ${corruption} before image preparation`, async () => {
    const fixture = happyFacadeFixture();
    let committedCompletion;
    await fixture.facade.runRestore(admission(), async (context) => {
      fixture.events.push("publish");
      committedCompletion = deepFreeze({
        materialization: restoreMaterialization(`seed-${corruption}`),
        replayed: false,
        result: context.result,
      });
      fixture.publishCompletion = committedCompletion;
      return committedCompletion;
    });

    fixture.corruptReplayReceipt(corruption);
    fixture.events.length = 0;
    let replayCompletion;
    await rejectsWithCode(
      fixture.facade.runRestore(admission(), async (context) => {
        fixture.events.push("publish");
        replayCompletion = deepFreeze({
          materialization: committedCompletion.materialization,
          replayed: true,
          result: context.result,
        });
        return replayCompletion;
      }),
      "postgres_detached_restore_foreground_composition_outcome_uncertain",
    );
    assert.deepEqual(fixture.events, ["publish"]);
  });
}

test("an existing generation must descend from the exact safety capture terminal", async () => {
  const fixture = happyFacadeFixture({ crossedGeneration: true });
  let publishCalls = 0;
  await rejectsWithCode(
    fixture.facade.runRestore(admission(), async () => {
      publishCalls += 1;
      assert.fail("crossed generation must not publish");
    }),
    "postgres_detached_restore_foreground_composition_outcome_uncertain",
  );
  assert.equal(publishCalls, 0);
  assert.deepEqual(fixture.events, []);
});

test("fresh generation dispatch requires the exact active authority pointer", async () => {
  const fixture = happyFacadeFixture({ crossedGenerationClaim: true });
  let publishCalls = 0;
  await rejectsWithCode(
    fixture.facade.runRestore(admission(), async () => {
      publishCalls += 1;
      assert.fail("crossed generation claim must not publish");
    }),
    "postgres_detached_restore_foreground_composition_outcome_uncertain",
  );
  assert.equal(publishCalls, 0);
  assert.deepEqual(fixture.events, [
    "gate",
    "renew",
    "stop",
    "generation-reserve",
    "generation-claim",
  ]);
});

for (const state of ["prepared", "starting", "uncertain", "committed"]) {
  test(`cold capture ${state} uses the typed continuation without fresh stop`, async () => {
    const fixture = happyFacadeFixture({ captureColdState: state });
    let completion;
    const result = await fixture.facade.runRestore(
      admission(),
      async (context) => {
        fixture.events.push("publish");
        completion = deepFreeze({
          materialization: restoreMaterialization(`materialization-${state}`),
          replayed: false,
          result: context.result,
        });
        fixture.publishCompletion = completion;
        return completion;
      },
    );
    assert.equal(result, completion);
    assert.equal(fixture.events.includes("gate"), false);
    assert.equal(fixture.events.includes("renew"), false);
    assert.equal(fixture.events.includes("stop"), false);
    assert.equal(fixture.events[0], state === "prepared" ? "resume" : "reconcile");
    assert.equal(
      fixture.events.filter((entry) => entry === "resume").length,
      state === "prepared" ? 1 : 0,
    );
    assert.equal(
      fixture.events.filter((entry) => entry === "reconcile").length,
      state === "prepared" ? 0 : 1,
    );
  });
}

for (const corruption of ["reservation", "active-pointer"]) {
  test(`cold prepared capture rejects a crossed ${corruption} before resume`, async () => {
    const fixture = happyFacadeFixture({
      captureColdState: "prepared",
      crossedPreparedCapture: corruption,
    });
    let publishCalls = 0;
    await rejectsWithCode(
      fixture.facade.runRestore(admission(), async () => {
        publishCalls += 1;
        assert.fail("crossed prepared capture must not publish");
      }),
      "postgres_detached_restore_foreground_composition_outcome_uncertain",
    );
    assert.equal(publishCalls, 0);
    assert.deepEqual(fixture.events, []);
  });
}

test("publication completion with a polluted inherited then fails before finalization", async () => {
  const fixture = happyFacadeFixture();
  const thenDescriptor = Object.getOwnPropertyDescriptor(
    Object.prototype,
    "then",
  );
  let observed = null;
  let inheritedThenGets = 0;
  let releaseObservation;
  const observation = new Promise((resolve) => {
    releaseObservation = resolve;
  });
  const restore = () => {
    if (thenDescriptor === undefined) delete Object.prototype.then;
    else Object.defineProperty(Object.prototype, "then", thenDescriptor);
  };
  try {
    const root = fixture.facade.runRestore(admission(), (context) => {
      fixture.events.push("publish");
      const completion = deepFreeze({
        materialization: { materializationId: "materialization-poison-001" },
        replayed: false,
        result: context.result,
      });
      const pending = Promise.resolve(completion);
      Object.defineProperty(Object.prototype, "then", {
        configurable: true,
        enumerable: false,
        get() {
          if (this !== completion) return undefined;
          inheritedThenGets += 1;
          return (onFulfilled) => {
            if (typeof onFulfilled === "function") {
              onFulfilled("forged-completion");
            }
          };
        },
      });
      return pending;
    });
    root.then(
      (value) => {
        observed = { status: "fulfilled", value };
        setTimeout(() => {
          restore();
          releaseObservation();
        }, 0);
      },
      (error) => {
        observed = { error, status: "rejected" };
        setTimeout(() => {
          restore();
          releaseObservation();
        }, 0);
      },
    );
    await observation;
  } finally {
    restore();
  }
  assert.equal(observed.status, "rejected");
  assert.equal(
    observed.error instanceof PostgresDetachedRestoreForegroundCompositionError,
    true,
  );
  assert.equal(
    observed.error.code,
    "postgres_detached_restore_foreground_composition_outcome_uncertain",
  );
  assert.equal(inheritedThenGets, 0);
  assert.deepEqual(fixture.events, [
    "gate",
    "renew",
    "stop",
    "generation-reserve",
    "generation-claim",
    "publish",
  ]);
});

test("fresh work is default-deny before renewal or stop", async () => {
  const { calls, facade, guardQueries, trace } = facadeFixture({ gate: () => null });
  const error = await facade
    .runRestore(admission(), async () => assert.fail("publish called"))
    .catch((caught) => caught);
  assert.equal(
    trace.join(","),
    "session-read,plan-resolve,capture-read,generation-read",
    guardQueries.join("\n"),
  );
  assert.deepEqual(calls, { gate: 1, renew: 0, stop: 0 }, trace.join(","));
  assert.equal(error.code, "postgres_detached_restore_fleet_capability_required");
});

test("an expired renewed lease is rejected at the stop effect boundary", async () => {
  const initialUpdatedAt = new Date(Date.now() - 10_000).toISOString();
  const initialExpiresAt = new Date(Date.now() - 5_000).toISOString();
  const renewedUpdatedAt = new Date(Date.now() - 2_000).toISOString();
  const renewedExpiresAt = new Date(Date.now() - 1_000).toISOString();
  const initial = runningSession({
    updatedAt: initialUpdatedAt,
    document: { lease: lease(initialExpiresAt) },
  });
  const plan = fixturePlan();
  const renewed = runningSession({
    revision: "10",
    updatedAt: renewedUpdatedAt,
    document: {
      lastOperation: terminalPointer({
        expectedSessionRevision: "8",
        kind: WRITER_LEASE_RENEW_OPERATION_KIND,
        operationId: plan.renewalOperationId,
        operationRevision: "1",
        reservationId: `reservation-${plan.renewalOperationId}`,
      }),
      lease: lease(renewedExpiresAt),
    },
  });
  const fixture = facadeFixture({
    gate: () => POSTGRES_DETACHED_RESTORE_FLEET_CONFIRMED,
    readSession: async () => initial,
    renewWriterLease(input) {
      return deepFreeze({
        operation: {
          conflictClass: SESSION_OPERATION_CONFLICT_CLASS,
          createdAt: renewedUpdatedAt,
          expectedSession: input.expectedSession,
          kind: WRITER_LEASE_RENEW_OPERATION_KIND,
          operationId: plan.renewalOperationId,
          request: input.request,
          requestSha256: "7".repeat(64),
          result: {
            contractVersion: 1,
            expiresAt: renewedExpiresAt,
            outcome: "writer-lease-renewed",
          },
          retiredAt: renewedUpdatedAt,
          revision: "1",
          sessionId: SESSION_ID,
          state: "committed",
          updatedAt: renewedUpdatedAt,
        },
        reservation: {
          reservationId: `reservation-${plan.renewalOperationId}`,
        },
        session: renewed,
      });
    },
  });
  await rejectsWithCode(
    fixture.facade.runRestore(
      admission(),
      async () => assert.fail("publish called"),
    ),
    "postgres_detached_restore_foreground_composition_outcome_uncertain",
  );
  assert.deepEqual(fixture.calls, { gate: 1, renew: 1, stop: 0 });
});

for (const capability of [
  null,
  POSTGRES_DETACHED_RESTORE_FLEET_CONFIRMED,
]) {
  test(`renewal-only cold cut fails closed with gate ${capability === null ? "closed" : "open"}`, async () => {
    const plan = fixturePlan();
    const session = baseSession({
      revision: "10",
      updatedAt: "2026-08-11T08:01:00.000Z",
      document: {
        lastOperation: terminalPointer({
          expectedSessionRevision: "8",
          kind: WRITER_LEASE_RENEW_OPERATION_KIND,
          operationId: plan.renewalOperationId,
          operationRevision: "1",
          reservationId: `reservation-${plan.renewalOperationId}`,
        }),
      },
    });
    const fixture = facadeFixture({
      gate: () => capability,
      readSession: async () => session,
    });
    await rejectsWithCode(
      fixture.facade.runRestore(
        admission(),
        async () => assert.fail("publish called"),
      ),
      "postgres_detached_restore_foreground_composition_outcome_uncertain",
    );
    assert.deepEqual(fixture.calls, { gate: 0, renew: 0, stop: 0 });
  });
}

test("an active stop is never dispatched again", async () => {
  const session = baseSession({
    revision: "10",
    updatedAt: "2026-08-11T08:01:00.000Z",
    document: {
      activeOperation: {
        conflictClass: SESSION_OPERATION_CONFLICT_CLASS,
        expectedSessionRevision: "8",
        kind: WRITER_LAUNCH_STOP_OPERATION_KIND,
        operationId: "writer-stop-existing-001",
        operationRevision: "1",
        requestSha256: "d".repeat(64),
        reservationId: "reservation-writer-stop-existing-001",
        state: "starting",
      },
    },
  });
  const fixture = facadeFixture({
    gate: () => POSTGRES_DETACHED_RESTORE_FLEET_CONFIRMED,
    readSession: async () => session,
  });
  await rejectsWithCode(
    fixture.facade.runRestore(
      admission(),
      async () => assert.fail("publish called"),
    ),
    "postgres_detached_restore_foreground_composition_outcome_uncertain",
  );
  assert.deepEqual(fixture.calls, { gate: 0, renew: 0, stop: 0 });
});

test("safe-species adoption ignores hostile own then and callback-time species poison", async () => {
  const promiseSpeciesDescriptor = Object.getOwnPropertyDescriptor(
    Promise,
    Symbol.species,
  );
  const speciesHolder = Object.freeze(
    Object.create(null, {
      [Symbol.species]: {
        configurable: false,
        enumerable: false,
        value: Promise,
        writable: false,
      },
    }),
  );
  const fixture = facadeFixture({
    gate() {
      Object.defineProperty(Promise, Symbol.species, {
        configurable: true,
        get() {
          throw new Error("poisoned species");
        },
      });
      return null;
    },
    readSession() {
      const pending = Promise.resolve(runningSession());
      Object.defineProperties(pending, {
        constructor: {
          configurable: false,
          enumerable: false,
          value: speciesHolder,
          writable: false,
        },
        then: {
          configurable: false,
          enumerable: false,
          value() {
            throw new Error("hostile own then");
          },
          writable: false,
        },
      });
      return pending;
    },
  });
  try {
    await rejectsWithCode(
      fixture.facade.runRestore(
        admission(),
        async () => assert.fail("publish called"),
      ),
      "postgres_detached_restore_fleet_capability_required",
    );
  } finally {
    Object.defineProperty(Promise, Symbol.species, promiseSpeciesDescriptor);
  }
  assert.deepEqual(fixture.calls, { gate: 1, renew: 0, stop: 0 });
});

test("public child reactions cannot be forged by callback-time Promise poisoning", async () => {
  let resolveSessionRead;
  const sessionRead = new Promise((resolve) => {
    resolveSessionRead = resolve;
  });
  const fixture = facadeFixture({
    gate: () => null,
    readSession: () => sessionRead,
  });
  const root = fixture.facade.runRestore(
    admission(),
    async () => assert.fail("publish called"),
  );
  for (const key of ["constructor", "then", "catch", "finally"]) {
    const descriptor = Object.getOwnPropertyDescriptor(root, key);
    assert.equal(descriptor.configurable, false);
    assert.equal(descriptor.enumerable, false);
    assert.equal(descriptor.writable, false);
  }
  assert.equal(root.constructor, Promise);

  const never = new Promise(() => {});
  const thenDescriptor = Object.getOwnPropertyDescriptor(
    Promise.prototype,
    "then",
  );
  const speciesDescriptor = Object.getOwnPropertyDescriptor(
    Promise,
    Symbol.species,
  );
  let reactionRan = false;
  let chainSettled = false;
  let poisonInstalled = false;
  let releaseObservation;
  const observation = new Promise((resolve) => {
    releaseObservation = resolve;
  });
  const chain = root.catch((error) => {
    assert.equal(
      error.code,
      "postgres_detached_restore_fleet_capability_required",
    );
    reactionRan = true;
    Object.defineProperty(Promise.prototype, "then", {
      ...thenDescriptor,
      value(onFulfilled) {
        if (typeof onFulfilled === "function") onFulfilled("forged-chain");
        return undefined;
      },
    });
    Object.defineProperty(Promise, Symbol.species, {
      configurable: true,
      get() {
        throw new Error("poisoned Promise species");
      },
    });
    poisonInstalled = true;
    return never;
  });
  chain.then(
    () => {
      chainSettled = true;
    },
    () => {
      chainSettled = true;
    },
  );
  const restoreTimer = setTimeout(() => {
    if (poisonInstalled) {
      Object.defineProperty(Promise.prototype, "then", thenDescriptor);
      Object.defineProperty(Promise, Symbol.species, speciesDescriptor);
      poisonInstalled = false;
    }
    releaseObservation();
  }, 25);
  try {
    resolveSessionRead(runningSession());
    await observation;
    await Promise.resolve();
  } finally {
    clearTimeout(restoreTimer);
    if (poisonInstalled) {
      Object.defineProperty(Promise.prototype, "then", thenDescriptor);
      Object.defineProperty(Promise, Symbol.species, speciesDescriptor);
    }
  }
  assert.equal(reactionRan, true);
  assert.equal(chainSettled, false);
  assert.deepEqual(fixture.calls, { gate: 1, renew: 0, stop: 0 });
});

test("public reactions adopt foreign protected native promises by intrinsic bridge", async () => {
  const fixture = facadeFixture({ gate: () => null });
  const foreignLifecycle = guards().lifecycleGuard;
  const result = await fixture.facade
    .runRestore(admission(), async () => assert.fail("publish called"))
    .catch(() =>
      foreignLifecycle.runForeground((_lease, complete) =>
        complete("foreign-completion"),
      ),
    );
  assert.equal(result, "foreign-completion");
  assert.deepEqual(fixture.calls, { gate: 1, renew: 0, stop: 0 });
});

test("public finally adopts a foreign protected native promise and preserves rejection", async () => {
  const fixture = facadeFixture({ gate: () => null });
  const foreignLifecycle = guards().lifecycleGuard;
  await rejectsWithCode(
    fixture.facade
      .runRestore(admission(), async () => assert.fail("publish called"))
      .finally(() =>
        foreignLifecycle.runForeground((_lease, complete) =>
          complete("foreign-finally-completion"),
        ),
      ),
    "postgres_detached_restore_fleet_capability_required",
  );
  assert.deepEqual(fixture.calls, { gate: 1, renew: 0, stop: 0 });
});

test("frozen receipt arrays are rejected before fresh effects", async () => {
  const fixture = facadeFixture({
    gate: () => POSTGRES_DETACHED_RESTORE_FLEET_CONFIRMED,
    readCheckpointCaptureAttempt: () => Object.freeze([]),
  });
  await rejectsWithCode(
    fixture.facade.runRestore(
      admission(),
      async () => assert.fail("publish called"),
    ),
    "postgres_detached_restore_foreground_composition_outcome_uncertain",
  );
  assert.deepEqual(fixture.calls, { gate: 0, renew: 0, stop: 0 });
});

test("an external preconstructed facade error cannot escape callback mapping", async () => {
  const external = new PostgresDetachedRestoreForegroundCompositionError(
    "postgres_detached_restore_foreground_composition_outcome_uncertain",
  );
  const fixture = facadeFixture({
    gate() {
      throw external;
    },
  });
  await rejectsWithCode(
    fixture.facade.runRestore(
      admission(),
      async () => assert.fail("publish called"),
    ),
    "postgres_detached_restore_fleet_capability_required",
  );
  assert.deepEqual(fixture.calls, { gate: 1, renew: 0, stop: 0 });
});

test("ordinary promises with an own reaction fail before later effects", async () => {
  const fixture = facadeFixture({
    gate: () => POSTGRES_DETACHED_RESTORE_FLEET_CONFIRMED,
    readSession() {
      const pending = Promise.resolve(baseSession());
      Object.defineProperty(pending, "then", {
        configurable: false,
        enumerable: false,
        value() {
          throw new Error("must not run");
        },
        writable: false,
      });
      return pending;
    },
  });
  await rejectsWithCode(
    fixture.facade.runRestore(
      admission(),
      async () => assert.fail("publish called"),
    ),
    "postgres_detached_restore_foreground_composition_outcome_uncertain",
  );
  assert.deepEqual(fixture.calls, { gate: 0, renew: 0, stop: 0 });
});

test("capture kind constant remains the typed grandfather boundary", () => {
  assert.equal(CHECKPOINT_CAPTURE_OPERATION_KIND, "checkpoint-capture-v1");
});
