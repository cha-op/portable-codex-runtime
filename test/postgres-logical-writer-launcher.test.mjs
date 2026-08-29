import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import test from "node:test";

import {
  POSTGRES_DETACHED_RESTORE_IMAGE_PLAN_PROVIDER_CONTRACT_VERSION,
  createPostgresDetachedRestoreImagePlanBinding,
} from "../src/postgres-detached-restore-image-plan-binding.mjs";
import {
  createPhysicalCollaboratorSettlement,
} from "../src/physical-collaborator-settlement.mjs";
import {
  createPostgresDetachedRestorePlan,
} from "../src/postgres-detached-restore-plan.mjs";
import {
  PostgresOperationGuard,
} from "../src/postgres-operation-guard.mjs";
import {
  RESTORE_ATTACHMENT_ACTIVATION_OPERATION_KIND,
  RESTORE_DESTINATION_GENERATION_OPERATION_KIND,
  SESSION_AUTHORITY_DOCUMENT_VERSION,
  SESSION_OPERATION_CONFLICT_CLASS,
  WRITER_FORCE_FENCE_OPERATION_KIND,
  WRITER_LEASE_RENEW_OPERATION_KIND,
  WRITER_LAUNCH_ATTEMPT_OPERATION_KIND,
  WRITER_LAUNCH_PRE_DISPATCH_CANCELLATION_REASON,
  WRITER_LAUNCH_STOP_OPERATION_KIND,
  WRITER_RELEASE_OPERATION_KIND,
  assertCommittedWriterLaunchStopTransitionProof,
  createWriterLaunchAttemptOperationRequest,
} from "../src/postgres-session-authority.mjs";
import {
  LVM_ATOMIC_CRASH_CAPTURE_BINDING_KIND,
  LVM_ATOMIC_CRASH_CAPTURE_DRIVER_CONTRACT_VERSION,
  LVM_ATOMIC_CRASH_CAPTURE_PROVIDER_CONTRACT_VERSION,
} from "../src/lvm-atomic-crash-capture-provider.mjs";
import {
  PostgresLvmAtomicCrashCaptureCompositionError,
  createPostgresLvmAtomicCrashCaptureComposition,
} from "../src/postgres-atomic-crash-capture-composition.mjs";
import * as postgresLogicalWriterLauncherModule from "../src/postgres-logical-writer-launcher.mjs";
import {
  LOGICAL_WRITER_LAUNCH_CONTRACT_VERSION,
  LOGICAL_WRITER_RECONCILE_RECEIPT_VERSION,
  LOGICAL_WRITER_SUPERVISOR_CONTRACT_VERSION,
  PostgresLogicalWriterLauncherError,
  createPostgresLogicalWriterAtomicCrashCaptureOwner,
  createPostgresLogicalWriterLauncher,
  derivePostgresLogicalWriterAtomicCrashCaptureStopOperationId,
  derivePostgresLogicalWriterStopOperationId,
} from "../src/postgres-logical-writer-launcher.mjs";
import {
  createPostgresDurableStopCaptureComposition,
} from "../src/postgres-durable-stop-capture-composition.mjs";
import {
  ATOMIC_CRASH_CAPTURE_CONTRACT_VERSION,
  createSessionManifest,
  serializeSessionManifest,
} from "../src/session-storage-contracts.mjs";
import {
  STOPPED_WRITER_STOP_CONFIRMED,
  StoppedWriterCapabilityCoordinator,
  StoppedWriterCapabilityError,
} from "../src/stopped-writer-capability.mjs";

const SESSION_ID = "019f3d80-0000-7000-8000-000000000001";
const THREAD_ID = "019f3d80-0000-7000-8000-000000000002";
const LAUNCH_ATTEMPT_ID = "writer-launch-attempt-001";
const RESTORE_OPERATION_ID = "restore-generation-operation-001";
const RESTORE_ACTIVATION_OPERATION_ID = "restore-activation-operation-001";
const GENERATION_ID = "restore-generation-001";
const CHECKPOINT_ID = "checkpoint-001";
const ARTIFACT_ID = "artifact-001";
const CAPTURE_OPERATION_ID = "checkpoint-capture-operation-001";
const SUPERVISOR_ID = "supervisor-001";
const STATE_OWNER_ID = `state-owner:${"a".repeat(64)}`;
const OTHER_STATE_OWNER_ID = `state-owner:${"b".repeat(64)}`;
const PROCESS_INCARNATION_ID = "process-incarnation-001";
const WRITER_INCARNATION_ID = "writer-incarnation-001";
const PROOF_ID = "supervisor-proof-001";
const CONTAINER_ID = "1".repeat(64);
const CONTAINER_NAME = "portable-codex-writer-001";
const BACKEND_ID = "single-attach-test";
const STORAGE_ID = "volume-001";
const OCI_MANIFEST_MEDIA_TYPE =
  "application/vnd.oci.image.manifest.v1+json";
const OCI_CONFIG_MEDIA_TYPE = "application/vnd.oci.image.config.v1+json";
const OCI_LAYER_MEDIA_TYPE = "application/vnd.oci.image.layer.v1.tar+gzip";
const CODEX_VERSION = "codex-cli 0.142.4";
const DIFF_ID = `sha256:${"d".repeat(64)}`;
const LAYER_DIGEST = `sha256:${"c".repeat(64)}`;
const BASE_TIME = "2026-08-04T12:00:00.000Z";
const PREPARED_TIME = "2026-08-04T12:00:01.000Z";
const STARTING_TIME = "2026-08-04T12:00:02.000Z";
const UNCERTAIN_TIME = "2026-08-04T12:00:03.000Z";
const COMMITTED_TIME = "2026-08-04T12:00:04.000Z";
const STOP_PREPARED_TIME = "2026-08-04T12:00:05.000Z";
const STOP_STARTING_TIME = "2026-08-04T12:00:06.000Z";
const STOP_UNCERTAIN_TIME = "2026-08-04T12:00:07.000Z";
const STOP_COMMITTED_TIME = "2026-08-04T12:00:08.000Z";
const jsonStringify = JSON.stringify;
const objectCreate = Object.create;
const objectFreeze = Object.freeze;
const ignoreSettlementFatal = objectFreeze(() => undefined);

function createImagePlanProviderSettlement() {
  const options = objectFreeze({
    deadlineMilliseconds: 30_000,
    onFatal: ignoreSettlementFatal,
    settlementGraceMilliseconds: 1_000,
  });
  return objectFreeze({
    inspectCodex: createPhysicalCollaboratorSettlement(options),
    resolveImagePlan: createPhysicalCollaboratorSettlement(options),
  });
}

function createTestImagePlanBinding(provider) {
  return createPostgresDetachedRestoreImagePlanBinding(
    objectFreeze({
      provider,
      settlement: createImagePlanProviderSettlement(),
    }),
  );
}

function safeProviderCarrier(value) {
  return objectFreeze(Object.assign(objectCreate(null), value));
}

function detachedRestorePlan() {
  return createPostgresDetachedRestorePlan({
    request: {
      backendId: BACKEND_ID,
      contractVersion: 1,
      fencingEpoch: "11",
      holderId: "restore-holder-001",
      leaseId: "restore-lease-001",
      operation: "restore",
      operationId: "restore-operation-001",
      sessionId: SESSION_ID,
      storageId: STORAGE_ID,
      target: {
        artifactId: ARTIFACT_ID,
        checkpointId: CHECKPOINT_ID,
        kind: "checkpoint",
      },
    },
    plan: {
      captureCreatedAt: "2026-08-04T11:00:00.000Z",
      destinationDirectory: "/var/lib/portable-codex/restores/launcher-001",
      destinationOwnedRoot: "/var/lib/portable-codex/restores",
      detachMode: "release",
      holderId: "restored-writer-holder-001",
      imagePlanId: "launcher-image-plan-001",
      leaseDurationMilliseconds: 600_000,
      sourceArtifactDirectory:
        "/var/lib/portable-codex/artifacts/launcher-source-001",
      sourceArtifactOwnedRoot: "/var/lib/portable-codex/artifacts",
    },
  });
}

function callbackOnlyOperationGuardPool() {
  const state = {
    connectCallbacks: 0,
    connectCalls: 0,
    lockHeld: false,
    queryCallbacks: 0,
    queryCalls: 0,
    releaseCalls: 0,
  };
  const client = {
    query(config) {
      state.queryCalls += 1;
      assert.equal(config.queryMode, "extended");
      assert.equal(Object.isFrozen(config.callback), true);
      let result;
      if (config.text === "DISCARD ALL") {
        result = { command: "DISCARD", rows: [] };
      } else if (config.text.includes("pg_try_advisory_lock(")) {
        state.lockHeld = true;
        result = {
          command: "SELECT",
          rows: [{ acquired: true, backend_pid: 9002 }],
        };
      } else if (config.text.includes("FROM pg_catalog.pg_locks")) {
        result = {
          command: "SELECT",
          rows: [{ backend_pid: 9002, lock_held: state.lockHeld }],
        };
      } else if (config.text.includes("pg_advisory_unlock(")) {
        const unlocked = state.lockHeld;
        state.lockHeld = false;
        result = {
          command: "SELECT",
          rows: [{ backend_pid: 9002, unlocked }],
        };
      } else {
        assert.fail(`unexpected operation guard query: ${config.text}`);
      }
      state.queryCallbacks += 1;
      assert.equal(config.callback(null, result), undefined);
      return undefined;
    },
    release(...args) {
      state.releaseCalls += 1;
      assert.equal(args.length, 0);
      state.lockHeld = false;
      return undefined;
    },
  };
  const pool = {
    connect(callback) {
      state.connectCalls += 1;
      state.connectCallbacks += 1;
      assert.equal(callback(null, client), undefined);
      return undefined;
    },
  };
  return { pool, state };
}

function digest(bytes) {
  return `sha256:${createHash("sha256").update(bytes).digest("hex")}`;
}

function jsonBytes(value) {
  return Buffer.from(jsonStringify(value), "utf8");
}

function jsonSha256(value) {
  return createHash("sha256").update(jsonStringify(value)).digest("hex");
}

function textSha256(value) {
  return createHash("sha256").update(value, "utf8").digest("hex");
}

function stopClaimSha256(claimToken) {
  return textSha256(
    `portable-codex-runtime:writer-launch-stop-claim:v1\0${claimToken}`,
  );
}

function imageFixture() {
  const configBytes = jsonBytes({
    architecture: "arm64",
    config: { Env: ["PATH=/usr/local/bin:/usr/bin:/bin"] },
    os: "linux",
    rootfs: { type: "layers", diff_ids: [DIFF_ID] },
  });
  const manifestDocument = {
    schemaVersion: 2,
    mediaType: OCI_MANIFEST_MEDIA_TYPE,
    config: {
      mediaType: OCI_CONFIG_MEDIA_TYPE,
      digest: digest(configBytes),
      size: configBytes.byteLength,
    },
    layers: [
      {
        mediaType: OCI_LAYER_MEDIA_TYPE,
        digest: LAYER_DIGEST,
        size: 1024,
      },
    ],
  };
  const descriptorBytes = jsonBytes(manifestDocument);
  const descriptor = {
    bytes: descriptorBytes,
    digest: digest(descriptorBytes),
    mediaType: OCI_MANIFEST_MEDIA_TYPE,
    size: descriptorBytes.byteLength,
  };
  const manifest = createSessionManifest({
    sessionId: SESSION_ID,
    codex: {
      rootThreadId: THREAD_ID,
      sessionId: THREAD_ID,
      ephemeral: false,
      historyMode: "paginated",
    },
    runtime: {
      imageDigest: descriptor.digest,
      imageMediaType: descriptor.mediaType,
      platform: "linux/arm64",
      codexVersion: CODEX_VERSION,
      codexSandbox: "danger-full-access",
    },
  });
  return { configBytes, descriptor, manifest };
}

function expectedImageMeasurement(image) {
  const platformImage = objectFreeze({
    architecture: "arm64",
    config: objectFreeze({
      digest: digest(image.configBytes),
      mediaType: OCI_CONFIG_MEDIA_TYPE,
      size: image.configBytes.byteLength,
    }),
    digest: image.descriptor.digest,
    mediaType: OCI_MANIFEST_MEDIA_TYPE,
    os: "linux",
    size: image.descriptor.size,
  });
  return objectFreeze({
    projection: objectFreeze({
      codexSandbox: "danger-full-access",
      codexVersion: CODEX_VERSION,
      platformImage,
    }),
    runtimeIdentity: objectFreeze({
      codexBinaryPath: "/opt/portable-codex/bin/codex",
      codexBinarySha256: "b".repeat(64),
      codexVersion: CODEX_VERSION,
      platformImageDigest: image.descriptor.digest,
    }),
  });
}

function lease(overrides = {}) {
  return {
    contractVersion: 1,
    sessionId: SESSION_ID,
    leaseId: "lease-001",
    holderId: "host-001",
    fencingEpoch: "11",
    expiresAt: "2027-08-04T12:00:00.000Z",
    ...overrides,
  };
}

function attachment(writerLease = lease(), overrides = {}) {
  return {
    contractVersion: 1,
    backendId: BACKEND_ID,
    storageId: STORAGE_ID,
    sessionId: SESSION_ID,
    attachmentId: "attachment-001",
    leaseId: writerLease.leaseId,
    holderId: writerLease.holderId,
    fencingEpoch: writerLease.fencingEpoch,
    operationId: "operation-attach-001",
    proofId: "proof-attachment-001",
    kind: "directory",
    rootPath: "/var/lib/portable-codex/session-001",
    mode: "read-write",
    ...overrides,
  };
}

function storageRef() {
  return {
    contractVersion: 1,
    backendId: BACKEND_ID,
    storageId: STORAGE_ID,
    sessionId: SESSION_ID,
  };
}

function backendCapabilities() {
  return {
    atomicPointInTimeCheckpoint: false,
    exclusiveWriterAttachment: true,
    fencing: "manual",
    normalDirectoryAttachment: true,
  };
}

function sessionDocument(manifest, overrides = {}) {
  const writerLease = lease();
  const previousOperation = {
    conflictClass: SESSION_OPERATION_CONFLICT_CLASS,
    expectedSessionRevision: "5",
    kind: "writer-attachment-acquire-v1",
    operationId: "operation-attach-001",
    operationRevision: "2",
    requestSha256: "8".repeat(64),
    reservationId: "reservation-operation-attach-001",
    resultSha256: "9".repeat(64),
    state: "committed",
  };
  return {
    documentVersion: SESSION_AUTHORITY_DOCUMENT_VERSION,
    manifest: JSON.parse(serializeSessionManifest(manifest)),
    storageRef: storageRef(),
    backendCapabilities: backendCapabilities(),
    lifecycle: "ATTACHED",
    writerEpoch: writerLease.fencingEpoch,
    lease: writerLease,
    attachment: attachment(writerLease),
    activeOperation: null,
    lastOperation: previousOperation,
    recovery: null,
    launch: null,
    ...overrides,
  };
}

function sessionSnapshot(manifest, overrides = {}) {
  return {
    sessionId: SESSION_ID,
    revision: "8",
    document: sessionDocument(manifest),
    createdAt: BASE_TIME,
    updatedAt: BASE_TIME,
    ...overrides,
  };
}

function generationSnapshot(overrides = {}) {
  return {
    binding: {
      generationBinding: "binding-001",
    },
    checkpointId: CHECKPOINT_ID,
    claimedAt: "2026-08-04T11:59:58.000Z",
    committedAt: "2026-08-04T11:59:59.000Z",
    document: {
      generationDocument: "document-001",
    },
    generationId: GENERATION_ID,
    operationId: RESTORE_OPERATION_ID,
    sessionId: SESSION_ID,
    state: "committed",
    ...overrides,
  };
}

function clone(value) {
  return structuredClone(value);
}

function canonicalAuthorityOperationPointer(value, terminal) {
  if (value === null) return null;
  const pointer = {
    conflictClass: value.conflictClass,
    expectedSessionRevision: value.expectedSessionRevision,
    kind: value.kind,
    operationId: value.operationId,
    operationRevision: value.operationRevision,
    requestSha256: value.requestSha256,
    reservationId: value.reservationId,
  };
  if (terminal) pointer.resultSha256 = value.resultSha256;
  pointer.state = value.state;
  return pointer;
}

function canonicalAuthoritySession(value) {
  const document = value.document;
  return {
    sessionId: value.sessionId,
    revision: value.revision,
    document: {
      documentVersion: document.documentVersion,
      manifest: clone(document.manifest),
      storageRef: clone(document.storageRef),
      backendCapabilities: clone(document.backendCapabilities),
      lifecycle: document.lifecycle,
      writerEpoch: document.writerEpoch,
      lease: clone(document.lease),
      attachment: clone(document.attachment),
      activeOperation: canonicalAuthorityOperationPointer(
        document.activeOperation,
        false,
      ),
      lastOperation: canonicalAuthorityOperationPointer(
        document.lastOperation,
        true,
      ),
      recovery: null,
      launch: clone(document.launch),
    },
    createdAt: value.createdAt,
    updatedAt: value.updatedAt,
  };
}

function operationRequestSha256(expectedSession, request) {
  return jsonSha256({
    requestVersion: 1,
    conflictClass: SESSION_OPERATION_CONFLICT_CLASS,
    expectedSession: canonicalAuthoritySession(expectedSession),
    payload: request,
  });
}

function activePointer(operation, reservation) {
  return {
    conflictClass: SESSION_OPERATION_CONFLICT_CLASS,
    expectedSessionRevision: operation.expectedSession.revision,
    kind: operation.kind,
    operationId: operation.operationId,
    operationRevision: operation.revision,
    requestSha256: operation.requestSha256,
    reservationId: reservation.reservationId,
    state: operation.state,
  };
}

function terminalPointer(operation, reservation) {
  return {
    ...activePointer(operation, reservation),
    resultSha256: jsonSha256(operation.result),
  };
}

function evidence(attemptId, status, overrides = {}) {
  return {
    contractVersion: 1,
    launchAttemptId: attemptId,
    processIncarnationId:
      status === "not-started" ? null : PROCESS_INCARNATION_ID,
    proofId: PROOF_ID,
    status,
    supervisorId: SUPERVISOR_ID,
    writerIncarnationId:
      status === "not-started" ? null : WRITER_INCARNATION_ID,
    ...overrides,
  };
}

function terminalResult(launchEvidence) {
  const outcomes = {
    "complete-stopped": "writer-launch-complete-stopped",
    "not-started": "writer-launch-not-started",
    started: "writer-launch-started",
  };
  return {
    evidence: clone(launchEvidence),
    outcome: outcomes[launchEvidence.status],
    resultVersion: 1,
  };
}

function supervisorTerminalRecord({
  launchAttemptId = LAUNCH_ATTEMPT_ID,
  processIncarnationId = PROCESS_INCARNATION_ID,
  proofId = PROOF_ID,
  requestSha256 = "a".repeat(64),
  stopOperationId = "writer-launch-stop-operation-001",
  stopProofId = PROOF_ID,
  writerIncarnationId = WRITER_INCARNATION_ID,
} = {}) {
  return {
    containerId: CONTAINER_ID,
    containerName: CONTAINER_NAME,
    contractVersion: 1,
    launchAttemptId,
    processIncarnationId,
    proofId,
    requestSha256,
    revision: 4,
    status: "stopped",
    stopOperationId,
    stopProofId,
    writerIncarnationId,
  };
}

function supervisorStateGcAuthorization({
  launchAttemptId = LAUNCH_ATTEMPT_ID,
  sessionId = SESSION_ID,
  terminalKind,
  terminalOperationId,
  terminalRecord,
}) {
  const terminalRecordSha256 = jsonSha256(terminalRecord);
  const authorization = {
    authorizedAt: STOP_COMMITTED_TIME,
    contractVersion: 2,
    launchAttemptId,
    sessionId,
    stateOwnerId: STATE_OWNER_ID,
    terminalKind,
    terminalOperationId,
    terminalRecord: clone(terminalRecord),
    terminalRecordSha256,
  };
  return {
    authorizationSha256: textSha256(
      `portable-codex-runtime:writer-supervisor-state-gc-authorization:v2\0${jsonStringify(
        authorization,
      )}\n`,
    ),
    ...authorization,
  };
}

class MemoryLaunchAuthority {
  constructor({ events, expectedSession, generation }) {
    this.events = events;
    this.expectedSession = clone(expectedSession);
    this.generation = clone(generation);
    this.stateOwnerId = STATE_OWNER_ID;
    this.baseInput = null;
    this.state = "absent";
    this.result = null;
    this.terminalRevision = null;
    this.activeOperationOverride = undefined;
    this.finalizationLastOperationMutation = null;
    this.launchPointerMutation = null;
    this.lastOperationOverride = undefined;
    this.sessionRevisionOverride = undefined;
    this.sessionUpdatedAtOverride = undefined;
    this.currentLeaseOverride = undefined;
    this.behaviour = Object.create(null);
    this.claimReceiptMutation = null;
    this.calls = {
      cancel: 0,
      claim: 0,
      finalizeStarted: 0,
      finalizeStopped: 0,
      finalizeWriterStopped: 0,
      finalizeWriterStoppedAndCapture: 0,
      markUncertain: 0,
      read: 0,
      reserve: 0,
      stopClaim: 0,
      stopReconcile: 0,
    };
    this.lastClaimInput = null;
    this.lastCancelInput = null;
    this.readReceiptMutation = null;
    this.beforeFinalize = null;
    this.stopBaseInput = null;
    this.stopState = "absent";
    this.stopResult = null;
    this.stopTerminalRevision = null;
    this.stopFinalizationMutation = null;
    this.stopReconcileMutation = null;
    this.supervisorStateGcAuthorizations = new Map();
  }

  beginNextAttempt({ expectedSession, generation }) {
    this.expectedSession = clone(expectedSession);
    this.generation = clone(generation);
    this.baseInput = null;
    this.state = "absent";
    this.result = null;
    this.terminalRevision = null;
    this.activeOperationOverride = undefined;
    this.finalizationLastOperationMutation = null;
    this.launchPointerMutation = null;
    this.lastOperationOverride = undefined;
    this.sessionRevisionOverride = undefined;
    this.sessionUpdatedAtOverride = undefined;
    this.currentLeaseOverride = undefined;
    this.behaviour = Object.create(null);
    this.claimReceiptMutation = null;
    this.lastClaimInput = null;
    this.lastCancelInput = null;
    this.readReceiptMutation = null;
    this.beforeFinalize = null;
    this.stopBaseInput = null;
    this.stopState = "absent";
    this.stopResult = null;
    this.stopTerminalRevision = null;
    this.stopFinalizationMutation = null;
    this.stopReconcileMutation = null;
    this.supervisorStateGcAuthorizations = new Map();
  }

  seed(request, state, launchEvidence = null) {
    this.baseInput = {
      expectedSession: clone(this.expectedSession),
      kind: WRITER_LAUNCH_ATTEMPT_OPERATION_KIND,
      operationId: LAUNCH_ATTEMPT_ID,
      request: clone(request),
    };
    this.state = state;
    if (state === "committed") {
      if (launchEvidence === null) {
        this.result = {
          resultVersion: 1,
          outcome: "cancelled-before-dispatch",
          reason: WRITER_LAUNCH_PRE_DISPATCH_CANCELLATION_REASON,
        };
        this.terminalRevision = "1";
      } else {
        this.result = terminalResult(launchEvidence);
        this.terminalRevision = "2";
      }
    }
  }

  operation() {
    assert.notEqual(this.baseInput, null);
    const revision =
      this.state === "prepared"
        ? "0"
        : this.state === "starting"
          ? "1"
          : this.state === "uncertain"
            ? "2"
            : this.terminalRevision;
    const updatedAt =
      this.state === "prepared"
        ? PREPARED_TIME
        : this.state === "starting"
          ? STARTING_TIME
          : this.state === "uncertain"
            ? UNCERTAIN_TIME
            : COMMITTED_TIME;
    return {
      operationId: this.baseInput.operationId,
      sessionId: this.expectedSession.sessionId,
      kind: this.baseInput.kind,
      conflictClass: SESSION_OPERATION_CONFLICT_CLASS,
      expectedSession: clone(this.baseInput.expectedSession),
      request: clone(this.baseInput.request),
      requestSha256: "a".repeat(64),
      state: this.state,
      revision,
      result: clone(this.result),
      createdAt: PREPARED_TIME,
      updatedAt,
      retiredAt: this.state === "committed" ? updatedAt : null,
    };
  }

  reservation(operation = this.operation()) {
    return {
      reservationId: "reservation-writer-launch-attempt-001",
      operationId: operation.operationId,
      sessionId: operation.sessionId,
      kind: operation.kind,
      expectedSessionRevision: operation.expectedSession.revision,
      state: operation.state === "committed" ? "released" : operation.state,
      conflictClass: SESSION_OPERATION_CONFLICT_CLASS,
      requestSha256: operation.requestSha256,
      createdAt: PREPARED_TIME,
      updatedAt: operation.updatedAt,
      expiresAt: null,
      releasedAt: operation.state === "committed" ? operation.updatedAt : null,
    };
  }

  attempt(operation = this.operation()) {
    return {
      contractVersion: 1,
      launchAttemptId: operation.operationId,
      request: clone(operation.request),
      result: clone(operation.result),
      state: operation.state,
    };
  }

  launchPointer(operation = this.operation()) {
    if (
      operation.state !== "committed" ||
      operation.result?.outcome !== "writer-launch-started"
    ) {
      return null;
    }
    const launchEvidence = operation.result.evidence;
    const launch = {
      attachmentId: operation.request.attachment.attachmentId,
      attachmentSha256: jsonSha256(operation.request.attachment),
      contractVersion: 1,
      fencingEpoch: operation.request.fencingEpoch,
      generation: clone(operation.request.generation),
      launchAttemptId: operation.operationId,
      launchResultSha256: jsonSha256(operation.result),
      leaseId: operation.request.lease.leaseId,
      leaseSha256: jsonSha256(operation.request.lease),
      measuredImageSha256: jsonSha256(operation.request.measuredImage),
      processIncarnationId: launchEvidence.processIncarnationId,
      startedAt: operation.updatedAt,
      supervisorId: launchEvidence.supervisorId,
      supervisorProofId: launchEvidence.proofId,
      writerIncarnationId: launchEvidence.writerIncarnationId,
    };
    return this.launchPointerMutation === null
      ? launch
      : this.launchPointerMutation(clone(launch));
  }

  session(operation = this.operation(), reservation = this.reservation(operation)) {
    const document = clone(this.expectedSession.document);
    if (this.currentLeaseOverride !== undefined) {
      document.lease = clone(this.currentLeaseOverride);
    }
    document.documentVersion = SESSION_AUTHORITY_DOCUMENT_VERSION;
    if (operation.state === "committed") {
      document.activeOperation =
        this.activeOperationOverride === undefined
          ? null
          : clone(this.activeOperationOverride);
      document.lastOperation =
        this.lastOperationOverride === undefined
          ? terminalPointer(operation, reservation)
          : clone(this.lastOperationOverride);
      document.launch = this.launchPointer(operation);
    } else {
      document.activeOperation =
        this.activeOperationOverride === undefined
          ? activePointer(operation, reservation)
          : clone(this.activeOperationOverride);
      document.launch = null;
    }
    return {
      sessionId: this.expectedSession.sessionId,
      revision:
        this.sessionRevisionOverride ??
        (
          BigInt(this.expectedSession.revision) +
          BigInt(operation.revision) +
          1n
        ).toString(),
      document,
      createdAt: this.expectedSession.createdAt,
      updatedAt: this.sessionUpdatedAtOverride ?? operation.updatedAt,
    };
  }

  receipt() {
    const operation = this.operation();
    const reservation = this.reservation(operation);
    return {
      attempt: this.attempt(operation),
      launch: this.launchPointer(operation),
      operation,
      reservation,
      session: this.session(operation, reservation),
      status: operation.state,
    };
  }

  stopOperation() {
    assert.notEqual(this.stopBaseInput, null);
    const revision =
      this.stopState === "prepared"
        ? "0"
        : this.stopState === "starting"
          ? "1"
          : this.stopState === "uncertain"
            ? "2"
            : this.stopTerminalRevision;
    const updatedAt =
      this.stopState === "prepared"
        ? STOP_PREPARED_TIME
        : this.stopState === "starting"
          ? STOP_STARTING_TIME
          : this.stopState === "uncertain"
            ? STOP_UNCERTAIN_TIME
            : STOP_COMMITTED_TIME;
    return {
      operationId: this.stopBaseInput.operationId,
      sessionId: this.stopBaseInput.expectedSession.sessionId,
      kind: this.stopBaseInput.kind,
      conflictClass: SESSION_OPERATION_CONFLICT_CLASS,
      expectedSession: clone(this.stopBaseInput.expectedSession),
      request: clone(this.stopBaseInput.request),
      requestSha256: operationRequestSha256(
        this.stopBaseInput.expectedSession,
        this.stopBaseInput.request,
      ),
      state: this.stopState,
      revision,
      result: clone(this.stopResult),
      createdAt: STOP_PREPARED_TIME,
      updatedAt,
      retiredAt: this.stopState === "committed" ? updatedAt : null,
    };
  }

  stopReservation(operation = this.stopOperation()) {
    return {
      reservationId: `reservation-${textSha256(operation.operationId)}`,
      operationId: operation.operationId,
      sessionId: operation.sessionId,
      kind: operation.kind,
      expectedSessionRevision: operation.expectedSession.revision,
      state:
        operation.state === "committed" ? "released" : operation.state,
      conflictClass: SESSION_OPERATION_CONFLICT_CLASS,
      requestSha256: operation.requestSha256,
      createdAt: STOP_PREPARED_TIME,
      updatedAt: operation.updatedAt,
      expiresAt: null,
      releasedAt:
        operation.state === "committed" ? operation.updatedAt : null,
    };
  }

  stopRecord(operation = this.stopOperation()) {
    return {
      contractVersion: this.stopBaseInput.request.contractVersion,
      launchAttemptId: this.stopBaseInput.request.launch.launchAttemptId,
      request: clone(this.stopBaseInput.request),
      result: clone(operation.result),
      state: operation.state,
      stopOperationId: operation.operationId,
    };
  }

  stopSession(
    operation = this.stopOperation(),
    reservation = this.stopReservation(operation),
  ) {
    const session = clone(this.stopBaseInput.expectedSession);
    session.document.documentVersion = SESSION_AUTHORITY_DOCUMENT_VERSION;
    if (operation.state === "committed") {
      session.document.activeOperation = null;
      session.document.lastOperation = terminalPointer(operation, reservation);
      session.document.launch = null;
    } else {
      session.document.activeOperation = activePointer(operation, reservation);
    }
    session.revision = (
      BigInt(this.stopBaseInput.expectedSession.revision) +
      BigInt(operation.revision) +
      1n
    ).toString();
    session.updatedAt = operation.updatedAt;
    return session;
  }

  stopTerminalSessionForCapture() {
    const operation = this.stopOperation();
    const reservation = this.stopReservation(operation);
    const session = clone(this.stopBaseInput.expectedSession);
    session.document.documentVersion = SESSION_AUTHORITY_DOCUMENT_VERSION;
    session.document.activeOperation = null;
    session.document.lastOperation = terminalPointer(operation, reservation);
    session.document.launch = null;
    session.revision = (
      BigInt(this.stopBaseInput.expectedSession.revision) +
      BigInt(operation.revision) +
      1n
    ).toString();
    session.updatedAt = operation.updatedAt;
    return session;
  }

  captureOperation() {
    const expectedSession = this.stopTerminalSessionForCapture();
    const request = clone(this.stopBaseInput.request.captureIntent);
    return {
      operationId: request.admission.request.operationId,
      sessionId: expectedSession.sessionId,
      kind: "checkpoint-capture-v1",
      conflictClass: SESSION_OPERATION_CONFLICT_CLASS,
      expectedSession,
      request,
      requestSha256: operationRequestSha256(expectedSession, request),
      state: "prepared",
      revision: "0",
      result: null,
      createdAt: STOP_COMMITTED_TIME,
      updatedAt: STOP_COMMITTED_TIME,
      retiredAt: null,
    };
  }

  captureReservation(operation = this.captureOperation()) {
    return {
      reservationId: `reservation-${textSha256(operation.operationId)}`,
      operationId: operation.operationId,
      sessionId: operation.sessionId,
      kind: operation.kind,
      expectedSessionRevision: operation.expectedSession.revision,
      state: "prepared",
      conflictClass: SESSION_OPERATION_CONFLICT_CLASS,
      requestSha256: operation.requestSha256,
      createdAt: STOP_COMMITTED_TIME,
      updatedAt: STOP_COMMITTED_TIME,
      expiresAt: null,
      releasedAt: null,
    };
  }

  captureSession(
    operation = this.captureOperation(),
    reservation = this.captureReservation(operation),
  ) {
    const session = clone(operation.expectedSession);
    session.document.activeOperation = activePointer(operation, reservation);
    session.revision = (BigInt(operation.expectedSession.revision) + 1n).toString();
    session.updatedAt = operation.updatedAt;
    return session;
  }

  stopCaptureHandoffReceipt(finalized, claimTokenMatched) {
    const stopOperation = this.stopOperation();
    const stopReservation = this.stopReservation(stopOperation);
    const captureOperation = this.captureOperation();
    const captureReservation = this.captureReservation(captureOperation);
    return {
      ...(claimTokenMatched === undefined ? {} : { claimTokenMatched }),
      capture: {
        operation: captureOperation,
        reservation: captureReservation,
      },
      session: this.captureSession(captureOperation, captureReservation),
      status: "prepared",
      stop: {
        finalized,
        operation: stopOperation,
        record: this.stopRecord(stopOperation),
        reservation: stopReservation,
      },
    };
  }

  async readSession() {
    this.events.push("authority.read-session");
    if (this.behaviour.readSessionThrows) {
      throw new Error("session read unavailable");
    }
    if (this.stopState !== "absent") return this.stopSession();
    if (this.state === "committed") return this.receipt().session;
    return clone(this.expectedSession);
  }

  async reserveOperation(input) {
    this.calls.reserve += 1;
    this.events.push("authority.reserve");
    if (input.kind === WRITER_LAUNCH_STOP_OPERATION_KIND) {
      if (this.stopState === "absent") {
        if (this.behaviour.stopSessionSupersededBeforeReserve !== undefined) {
          const supersession =
            this.behaviour.stopSessionSupersededBeforeReserve;
          this.behaviour.stopSessionSupersededBeforeReserve = undefined;
          const currentSession = this.receipt().session;
          this.currentLeaseOverride = clone(supersession.lease);
          this.sessionRevisionOverride = (
            BigInt(currentSession.revision) + 1n
          ).toString();
          this.sessionUpdatedAtOverride = supersession.updatedAt;
          this.lastOperationOverride = {
            conflictClass: SESSION_OPERATION_CONFLICT_CLASS,
            expectedSessionRevision: currentSession.revision,
            kind: WRITER_LEASE_RENEW_OPERATION_KIND,
            operationId: "writer-lease-renewal-001",
            operationRevision: "0",
            requestSha256: "e".repeat(64),
            reservationId: "reservation-writer-lease-renewal-001",
            resultSha256: "f".repeat(64),
            state: "committed",
          };
          throw new Error("stop session precondition superseded");
        }
        const expectedSession = canonicalAuthoritySession(
          input.expectedSession,
        );
        this.stopBaseInput = {
          expectedSession,
          kind: input.kind,
          operationId: input.operationId,
          request: clone(input.request),
        };
        this.stopState = "prepared";
        if (this.behaviour.stopReserveThrowAfterCommit) {
          throw new Error("lost stop reserve acknowledgement");
        }
        const operation = this.stopOperation();
        const reservation = this.stopReservation(operation);
        return {
          acquired: true,
          operation,
          reservation,
          session: this.stopSession(operation, reservation),
          status: "prepared",
        };
      }
      const operation = this.stopOperation();
      const reservation = this.stopReservation(operation);
      return {
        acquired: false,
        operation,
        reservation,
        session: this.stopSession(operation, reservation),
        status: operation.state,
      };
    }
    if (this.state === "absent") {
      this.baseInput = clone(input);
      this.state = "prepared";
      if (this.behaviour.reserveThrowAfterCommit) throw new Error("lost reserve ack");
      const operation = this.operation();
      const reservation = this.reservation(operation);
      return {
        acquired: true,
        operation,
        reservation,
        session: this.session(operation, reservation),
        status: "prepared",
      };
    }
    const operation = this.operation();
    const reservation = this.reservation(operation);
    return {
      acquired: false,
      operation,
      reservation,
      session: this.session(operation, reservation),
      status: operation.state,
    };
  }

  async reconcileWriterLaunchStopOperation(input) {
    this.calls.stopReconcile += 1;
    this.events.push("authority.reconcile-stop");
    if (this.behaviour.stopReconcileThrowsOnce) {
      this.behaviour.stopReconcileThrowsOnce = false;
      throw new Error("stop reconcile unavailable");
    }
    assert.equal(input.kind, WRITER_LAUNCH_STOP_OPERATION_KIND);
    if (
      this.behaviour.stopForeignClaimBeforeReconcile &&
      this.stopState === "prepared"
    ) {
      this.stopState = "starting";
    }
    if (this.stopState === "absent") {
      const expectedSession = canonicalAuthoritySession(
        input.expectedSession,
      );
      const session = canonicalAuthoritySession(
        this.state === "committed"
          ? this.receipt().session
          : this.expectedSession,
      );
      return {
        claimTokenMatched: false,
        expectedSessionMatched:
          jsonStringify(session) === jsonStringify(expectedSession),
        operation: null,
        reservation: null,
        session,
        status: "absent",
      };
    }
    const { claimToken, ...baseInput } = input;
    assert.deepEqual(
      JSON.parse(jsonStringify(baseInput)),
      JSON.parse(jsonStringify(this.stopBaseInput)),
    );
    const claimTokenMatched =
      !this.behaviour.stopForeignClaimBeforeReconcile &&
      stopClaimSha256(claimToken) ===
        this.stopBaseInput.request.dispatchClaimSha256;
    const operation = this.stopOperation();
    const reservation = this.stopReservation(operation);
    const receipt = {
      claimTokenMatched,
      operation,
      reservation,
      session: this.stopSession(operation, reservation),
      status: operation.state,
    };
    if (
      this.stopState === "committed" &&
      this.stopBaseInput.request.contractVersion === 3
    ) {
      return this.stopCaptureHandoffReceipt(false, claimTokenMatched);
    }
    return this.stopReconcileMutation === null
      ? receipt
      : this.stopReconcileMutation(clone(receipt));
  }

  async claimWriterLaunchAttemptDispatch(input) {
    this.calls.claim += 1;
    this.events.push("authority.claim");
    this.lastClaimInput = clone(input);
    assert.deepEqual(
      JSON.parse(jsonStringify(input)),
      JSON.parse(
        jsonStringify({
          ...this.baseInput,
          expectedOperationRevision: "0",
          stateOwnerId: STATE_OWNER_ID,
        }),
      ),
    );
    if (this.state !== "prepared") {
      const receipt = this.receipt();
      const claimReceipt = {
        attempt: receipt.attempt,
        dispatchGranted: false,
        generation:
          this.state === "starting" ||
          this.state === "uncertain" ||
          receipt.operation.result?.outcome !==
            "cancelled-before-dispatch"
            ? clone(this.generation)
            : null,
        operation: receipt.operation,
        reservation: receipt.reservation,
        session: receipt.session,
        status: receipt.operation.state,
      };
      return this.claimReceiptMutation === null
        ? claimReceipt
        : this.claimReceiptMutation(clone(claimReceipt));
    }
    if (this.behaviour.claimThrowBeforeCommit) {
      throw new Error("claim unavailable before commit");
    }
    this.state = "starting";
    if (this.behaviour.claimThrowAfterCommit) throw new Error("lost claim ack");
    const operation = this.operation();
    const reservation = this.reservation(operation);
    const claimReceipt = {
      attempt: this.attempt(operation),
      authorityNow: "2026-08-04T12:00:02.500Z",
      dispatchGranted: true,
      generation: clone(this.generation),
      operation,
      reservation,
      session: this.session(operation, reservation),
      status: "starting",
    };
    return this.claimReceiptMutation === null
      ? claimReceipt
      : this.claimReceiptMutation(clone(claimReceipt));
  }

  async claimWriterLaunchStopDispatch(input) {
    this.calls.stopClaim += 1;
    this.events.push("authority.claim-stop");
    const { claimToken, ...transitionInput } = input;
    assert.deepEqual(
      JSON.parse(jsonStringify(transitionInput)),
      JSON.parse(
        jsonStringify({
          ...this.stopBaseInput,
          expectedOperationRevision: "0",
        }),
      ),
    );
    const claimTokenMatched =
      stopClaimSha256(claimToken) ===
      this.stopBaseInput.request.dispatchClaimSha256;
    if (this.stopState !== "prepared") {
      const operation = this.stopOperation();
      const reservation = this.stopReservation(operation);
      return {
        claimTokenMatched,
        dispatchGranted: false,
        launch: clone(this.stopBaseInput.request.launch),
        operation,
        reservation,
        session: this.stopSession(operation, reservation),
        status: operation.state,
        stop: this.stopRecord(operation),
      };
    }
    if (this.behaviour.stopClaimThrowBeforeCommit) {
      throw new Error("stop claim unavailable before commit");
    }
    if (!claimTokenMatched) {
      const operation = this.stopOperation();
      const reservation = this.stopReservation(operation);
      return {
        claimTokenMatched: false,
        dispatchGranted: false,
        launch: clone(this.stopBaseInput.request.launch),
        operation,
        reservation,
        session: this.stopSession(operation, reservation),
        status: "prepared",
        stop: this.stopRecord(operation),
      };
    }
    if (this.behaviour.stopClaimLosesPreparedRace) {
      this.stopState = "starting";
      const operation = this.stopOperation();
      const reservation = this.stopReservation(operation);
      return {
        claimTokenMatched: false,
        dispatchGranted: false,
        launch: clone(this.stopBaseInput.request.launch),
        operation,
        reservation,
        session: this.stopSession(operation, reservation),
        status: "starting",
        stop: this.stopRecord(operation),
      };
    }
    this.stopState = "starting";
    if (this.behaviour.stopClaimThrowAfterCommit) {
      throw new Error("lost stop claim acknowledgement");
    }
    const operation = this.stopOperation();
    const reservation = this.stopReservation(operation);
    return {
      claimTokenMatched,
      dispatchGranted: true,
      launch: clone(this.stopBaseInput.request.launch),
      operation,
      reservation,
      session: this.stopSession(operation, reservation),
      status: "starting",
      stop: this.stopRecord(operation),
    };
  }

  async readWriterLaunchAttempt(input) {
    this.calls.read += 1;
    this.events.push("authority.read-attempt");
    assert.deepEqual(Reflect.ownKeys(input), ["operationId", "stateOwnerId"]);
    assert.equal(input.operationId, LAUNCH_ATTEMPT_ID);
    if (input.stateOwnerId !== this.stateOwnerId) {
      throw new Error("writer launch attempt belongs to another state owner");
    }
    if (this.behaviour.readThrows) throw new Error("read unavailable");
    if (this.state === "absent") throw new Error("attempt absent");
    const receipt = this.receipt();
    if (this.stopState === "committed") {
      receipt.launch = null;
      receipt.session = this.stopSession();
    }
    return this.readReceiptMutation === null
      ? receipt
      : this.readReceiptMutation(clone(receipt));
  }

  async markOperationUncertain(input) {
    this.calls.markUncertain += 1;
    this.events.push("authority.mark-uncertain");
    if (input.kind === WRITER_LAUNCH_STOP_OPERATION_KIND) {
      if (this.stopState === "starting") this.stopState = "uncertain";
      if (this.behaviour.markThrowsAfterCommit) {
        throw new Error("lost uncertain ack");
      }
      const operation = this.stopOperation();
      const reservation = this.stopReservation(operation);
      return {
        changed: true,
        operation,
        reservation,
        session: this.stopSession(operation, reservation),
        status: operation.state,
      };
    }
    if (this.state === "starting") this.state = "uncertain";
    if (this.behaviour.markThrowsAfterCommit) {
      throw new Error("lost uncertain ack");
    }
    const operation = this.operation();
    const reservation = this.reservation(operation);
    return {
      changed: true,
      operation,
      reservation,
      session: this.session(operation, reservation),
      status: operation.state,
    };
  }

  async cancelPreparedOperation(options) {
    this.calls.cancel += 1;
    this.events.push("authority.cancel");
    assert.equal(this.state, "prepared");
    this.lastCancelInput = clone(options);
    assert.equal(
      options.reason,
      WRITER_LAUNCH_PRE_DISPATCH_CANCELLATION_REASON,
    );
    if (this.behaviour.cancelPreparedHandoffConflict) {
      throw new Error("prepared handoff cancellation conflict");
    }
    this.state = "committed";
    this.terminalRevision = "1";
    this.result = {
      resultVersion: 1,
      outcome: "cancelled-before-dispatch",
      reason: WRITER_LAUNCH_PRE_DISPATCH_CANCELLATION_REASON,
    };
    const operation = this.operation();
    const reservation = this.reservation(operation);
    return {
      cancelled: true,
      operation,
      reservation,
      session: this.session(operation, reservation),
      status: "committed",
    };
  }

  async #finalize(launchEvidence, kind) {
    this.events.push(`authority.finalize-${kind}`);
    this.beforeFinalize?.();
    const predecessor = this.state;
    assert.ok(predecessor === "starting" || predecessor === "uncertain");
    if (
      kind === "started" &&
      this.behaviour.finalizeStartedThrowBeforeCommit
    ) {
      throw new Error("started finalization unavailable");
    }
    this.state = "committed";
    this.terminalRevision = predecessor === "starting" ? "2" : "3";
    this.result = terminalResult(launchEvidence);
    if (this.behaviour.finalizeThrowAfterCommit) {
      throw new Error("lost finalize ack");
    }
    const receipt = this.receipt();
    if (this.finalizationLastOperationMutation !== null) {
      receipt.session.document.lastOperation =
        this.finalizationLastOperationMutation(
          clone(receipt.session.document.lastOperation),
        );
    }
    return {
      attempt: receipt.attempt,
      finalized: true,
      launch: receipt.launch,
      operation: receipt.operation,
      reservation: receipt.reservation,
      session: receipt.session,
      status: "committed",
    };
  }

  async finalizeWriterLaunchAttemptStarted(input) {
    this.calls.finalizeStarted += 1;
    return this.#finalize(input.evidence, "started");
  }

  async finalizeWriterLaunchAttemptStopped(input) {
    this.calls.finalizeStopped += 1;
    return this.#finalize(input.evidence, "stopped");
  }

  rememberSupervisorStateGcAuthorization({
    launchAttemptId,
    sessionId,
    terminalKind,
    terminalOperationId,
    terminalRecord,
  }) {
    const authorization = supervisorStateGcAuthorization({
      launchAttemptId,
      sessionId,
      terminalKind,
      terminalOperationId,
      terminalRecord,
    });
    this.supervisorStateGcAuthorizations.set(
      terminalOperationId,
      clone(authorization),
    );
    return authorization;
  }

  async readWriterSupervisorStateGcAuthorization(input) {
    assert.deepEqual(Reflect.ownKeys(input), [
      "stateOwnerId",
      "terminalOperationId",
    ]);
    assert.equal(input.stateOwnerId, STATE_OWNER_ID);
    const authorization = this.supervisorStateGcAuthorizations.get(
      input.terminalOperationId,
    );
    if (authorization === undefined) {
      throw new Error("supervisor state GC authorization absent");
    }
    return clone(authorization);
  }

  async finalizeWriterLaunchAttemptStoppedAndAuthorizeSupervisorStateGc(
    input,
  ) {
    const { terminalRecord, ...finalizationInput } = input;
    const remember = () =>
      this.rememberSupervisorStateGcAuthorization({
        launchAttemptId: input.operationId,
        sessionId: input.expectedSession.sessionId,
        terminalKind: WRITER_LAUNCH_ATTEMPT_OPERATION_KIND,
        terminalOperationId: input.operationId,
        terminalRecord,
      });
    let receipt;
    try {
      receipt = await this.finalizeWriterLaunchAttemptStopped(
        finalizationInput,
      );
    } catch (error) {
      if (this.state === "committed") remember();
      throw error;
    }
    return {
      ...receipt,
      supervisorStateGcAuthorization: remember(),
    };
  }

  async finalizeWriterLaunchStopped(input) {
    this.calls.finalizeWriterStopped += 1;
    this.events.push("authority.finalize-writer-stopped");
    if (
      this.behaviour.stopMarkUncertainBeforeFinalize &&
      this.stopState === "starting"
    ) {
      this.behaviour.stopMarkUncertainBeforeFinalize = false;
      this.stopState = "uncertain";
    }
    const expectedOperationRevision =
      this.stopState === "uncertain" || this.stopTerminalRevision === "3"
        ? "2"
        : "1";
    assert.deepEqual(
      JSON.parse(jsonStringify(input)),
      JSON.parse(
        jsonStringify({
          ...this.stopBaseInput,
          evidence: input.evidence,
          expectedOperationRevision,
        }),
      ),
    );
    if (this.stopState === "committed") {
      if (this.behaviour.stopFinalizeThrowAfterCommitAlways) {
        throw new Error("lost stop finalization acknowledgement");
      }
      const operation = this.stopOperation();
      const reservation = this.stopReservation(operation);
      const replay = {
        finalized: false,
        launch: null,
        operation,
        reservation,
        session: this.stopSession(operation, reservation),
        status: "committed",
        stop: this.stopRecord(operation),
      };
      return this.stopFinalizationMutation === null
        ? replay
        : this.stopFinalizationMutation(clone(replay));
    }
    assert.ok(
      this.stopState === "starting" || this.stopState === "uncertain",
    );
    if (this.behaviour.stopFinalizeThrowBeforeCommit) {
      throw new Error("stop finalization unavailable before commit");
    }
    const predecessor = this.stopState;
    this.stopState = "committed";
    this.stopTerminalRevision = predecessor === "starting" ? "2" : "3";
    this.stopResult = {
      evidence: clone(input.evidence),
      outcome: "writer-launch-stopped",
      resultVersion: 1,
    };
    if (
      this.behaviour.stopFinalizeThrowAfterCommit ||
      this.behaviour.stopFinalizeThrowAfterCommitAlways
    ) {
      this.behaviour.stopFinalizeThrowAfterCommit = false;
      throw new Error("lost stop finalization acknowledgement");
    }
    const operation = this.stopOperation();
    const reservation = this.stopReservation(operation);
    const receipt = {
      finalized: true,
      launch: null,
      operation,
      reservation,
      session: this.stopSession(operation, reservation),
      status: "committed",
      stop: this.stopRecord(operation),
    };
    return this.stopFinalizationMutation === null
      ? receipt
      : this.stopFinalizationMutation(clone(receipt));
  }

  async finalizeWriterLaunchStoppedAndAuthorizeSupervisorStateGc(input) {
    const { terminalRecord, ...finalizationInput } = input;
    const remember = () =>
      this.rememberSupervisorStateGcAuthorization({
        launchAttemptId: input.request.launch.launchAttemptId,
        sessionId: input.expectedSession.sessionId,
        terminalKind: WRITER_LAUNCH_STOP_OPERATION_KIND,
        terminalOperationId: input.operationId,
        terminalRecord,
      });
    let receipt;
    try {
      receipt = await this.finalizeWriterLaunchStopped(finalizationInput);
    } catch (error) {
      if (this.stopState === "committed") remember();
      throw error;
    }
    return {
      ...receipt,
      supervisorStateGcAuthorization: remember(),
    };
  }

  async finalizeWriterLaunchStoppedAndReserveCheckpointCapture(input) {
    this.calls.finalizeWriterStoppedAndCapture += 1;
    this.events.push("authority.finalize-writer-stopped-and-capture");
    const expectedOperationRevision =
      this.stopState === "uncertain" || this.stopTerminalRevision === "3"
        ? "2"
        : "1";
    assert.deepEqual(
      JSON.parse(jsonStringify(input)),
      JSON.parse(
        jsonStringify({
          ...this.stopBaseInput,
          evidence: input.evidence,
          expectedOperationRevision,
        }),
      ),
    );
    if (this.stopState === "committed") {
      return this.stopCaptureHandoffReceipt(false);
    }
    assert.ok(
      this.stopState === "starting" || this.stopState === "uncertain",
    );
    if (this.behaviour.stopFinalizeThrowBeforeCommit) {
      throw new Error("atomic stop-capture finalization unavailable");
    }
    const predecessor = this.stopState;
    this.stopState = "committed";
    this.stopTerminalRevision = predecessor === "starting" ? "2" : "3";
    this.stopResult = {
      evidence: clone(input.evidence),
      outcome: "writer-launch-stopped",
      resultVersion: 1,
    };
    if (
      this.behaviour.stopFinalizeThrowAfterCommit ||
      this.behaviour.stopFinalizeThrowAfterCommitAlways
    ) {
      this.behaviour.stopFinalizeThrowAfterCommit = false;
      throw new Error("lost atomic stop-capture finalization acknowledgement");
    }
    return this.stopCaptureHandoffReceipt(true);
  }

  async finalizeWriterLaunchStoppedAndReserveCheckpointCaptureAndAuthorizeSupervisorStateGc(
    input,
  ) {
    const { terminalRecord, ...finalizationInput } = input;
    const remember = () =>
      this.rememberSupervisorStateGcAuthorization({
        launchAttemptId: input.request.launch.launchAttemptId,
        sessionId: input.expectedSession.sessionId,
        terminalKind: WRITER_LAUNCH_STOP_OPERATION_KIND,
        terminalOperationId: input.operationId,
        terminalRecord,
      });
    let receipt;
    try {
      receipt =
        await this.finalizeWriterLaunchStoppedAndReserveCheckpointCapture(
          finalizationInput,
        );
    } catch (error) {
      if (this.stopState === "committed") remember();
      throw error;
    }
    return {
      ...receipt,
      supervisorStateGcAuthorization: remember(),
    };
  }
}

class MemoryOperationGuard {
  constructor(events) {
    this.events = events;
    this.calls = 0;
    this.assertions = 0;
    this.failBeforeCallback = false;
    this.tails = new Map();
  }

  async runExclusive(operationId, callback) {
    this.calls += 1;
    if (this.failBeforeCallback) {
      throw new Error("operation guard unavailable");
    }
    const predecessor = this.tails.get(operationId) ?? Promise.resolve();
    let release;
    const tail = new Promise((resolve) => {
      release = resolve;
    });
    this.tails.set(operationId, tail);
    await predecessor;
    this.events.push(`guard.enter:${operationId}`);
    try {
      let completionCalls = 0;
      let completionCarrier;
      let completionValue;
      const complete = objectFreeze((value) => {
        completionCalls += 1;
        assert.equal(completionCalls, 1);
        completionValue = value;
        completionCarrier = objectFreeze(objectCreate(null));
        return completionCarrier;
      });
      const carrier = await callback(
        Object.freeze({
          assertHeld: async () => {
            this.assertions += 1;
            this.events.push("guard.assert-held");
          },
        }),
        complete,
      );
      assert.equal(completionCalls, 1);
      assert.strictEqual(carrier, completionCarrier);
      return completionValue;
    } finally {
      this.events.push(`guard.exit:${operationId}`);
      release();
      if (this.tails.get(operationId) === tail) {
        this.tails.delete(operationId);
      }
    }
  }
}

let hostileRegisterWriterCalls = 0;
let hostileConsumeCapabilityCalls = 0;
let hostileLaunchAdmissionCalls = 0;
let hostileRetireWriterCalls = 0;
let hostileRevokeWriterCalls = 0;
let hostileStopWriterCalls = 0;

class HostileStoppedWriterCoordinator extends StoppedWriterCapabilityCoordinator {
  assertWriterLaunchAvailable() {
    hostileLaunchAdmissionCalls += 1;
    throw new Error("subclass override must not run");
  }

  registerWriter() {
    hostileRegisterWriterCalls += 1;
    throw new Error("subclass override must not run");
  }

  async consumeCapability() {
    hostileConsumeCapabilityCalls += 1;
    throw new Error("subclass override must not run");
  }

  retireWriter() {
    hostileRetireWriterCalls += 1;
    throw new Error("subclass override must not run");
  }

  revokeWriter() {
    hostileRevokeWriterCalls += 1;
    throw new Error("subclass override must not run");
  }

  async stopAndIssueCapability() {
    hostileStopWriterCalls += 1;
    throw new Error("subclass override must not run");
  }
}

function checkpoint(imageDigest) {
  return {
    contractVersion: 1,
    checkpointId: CHECKPOINT_ID,
    artifactId: ARTIFACT_ID,
    backendId: BACKEND_ID,
    storageId: STORAGE_ID,
    sessionId: SESSION_ID,
    codexThreadId: THREAD_ID,
    codexSessionId: THREAD_ID,
    imageDigest,
    sourceFencingEpoch: lease().fencingEpoch,
    checkpointClass: "clean",
    createdAt: "2026-08-04T11:00:00.000Z",
  };
}

function captureRequest() {
  const writerLease = lease();
  return {
    contractVersion: 1,
    backendId: BACKEND_ID,
    storageId: STORAGE_ID,
    sessionId: SESSION_ID,
    leaseId: writerLease.leaseId,
    holderId: writerLease.holderId,
    fencingEpoch: writerLease.fencingEpoch,
    operation: "checkpoint",
    operationId: CAPTURE_OPERATION_ID,
    target: {
      artifactId: ARTIFACT_ID,
      checkpointId: CHECKPOINT_ID,
      kind: "checkpoint",
    },
  };
}

function captureResult(imageDigest) {
  const request = captureRequest();
  return {
    checkpoint: checkpoint(imageDigest),
    mutation: {
      ...request,
      proofId: "checkpoint-proof-001",
      status: "checkpoint-created",
    },
  };
}

function atomicCrashCaptureRequest(value, overrides = {}) {
  const capture = resolverInput(value);
  return {
    captureAttemptId: "atomic-capture-attempt-001",
    checkpoint: {
      ...capture.checkpoint,
      checkpointClass: "crash-prefix",
    },
    contractVersion: ATOMIC_CRASH_CAPTURE_CONTRACT_VERSION,
    mutationRequest: capture.request,
    sourceAttachment: capture.attachment,
    storageRef: storageRef(),
    ...overrides,
  };
}

function atomicCrashCaptureResult(request, overrides = {}) {
  const { artifact: artifactOverrides = {}, ...resultOverrides } = overrides;
  return {
    artifact: {
      byteLength: "4096",
      contentSha256: "e".repeat(64),
      objectId: "SNAPSHOT-1234567890",
      objectIdentityScheme: "lvm-lv-uuid-v1",
      readOnly: true,
      ...artifactOverrides,
    },
    artifactId: request.checkpoint.artifactId,
    backendId: request.storageRef.backendId,
    captureAttemptId: request.captureAttemptId,
    checkpointId: request.checkpoint.checkpointId,
    contractVersion: ATOMIC_CRASH_CAPTURE_CONTRACT_VERSION,
    operationId: request.mutationRequest.operationId,
    proofId: "atomic-capture-proof-001",
    sessionId: request.storageRef.sessionId,
    sourceFencingEpoch: request.checkpoint.sourceFencingEpoch,
    status: "committed",
    storageId: request.storageRef.storageId,
    ...resultOverrides,
  };
}

function exactRecord(values) {
  return objectFreeze(Object.assign(objectCreate(null), values));
}

function higherEpochWriterBinding() {
  const canonicalLease = lease({
    fencingEpoch: "12",
    leaseId: "lease-002",
  });
  return {
    attachment: attachment(canonicalLease, {
      operationId: "operation-attach-002",
      proofId: "proof-attachment-002",
    }),
    canonicalLease,
  };
}

function canonicalJsonValue(value) {
  if (value === null || typeof value !== "object") return value;
  if (Array.isArray(value)) return value.map(canonicalJsonValue);
  const result = {};
  for (const key of Object.keys(value).sort()) {
    result[key] = canonicalJsonValue(value[key]);
  }
  return result;
}

function assertWriterLaunchBlocked(coordinator) {
  assert.throws(
    () => coordinator.assertWriterLaunchAvailable(higherEpochWriterBinding()),
    (error) =>
      error instanceof StoppedWriterCapabilityError &&
      error.code === "writer_state_conflict",
  );
}

function atomicCompositionCollaborators(
  request,
  {
    captureGate = null,
    captureThrows = false,
    commitAcknowledgementLoss = false,
    committed = false,
    committedReadVisible = true,
    onCaptureStart = null,
  } = {},
) {
  const calls = {
    capture: 0,
    claim: 0,
    commit: 0,
    mark: 0,
    read: 0,
    resolve: 0,
    verify: 0,
  };
  const binding = exactRecord({
    bindingKind: LVM_ATOMIC_CRASH_CAPTURE_BINDING_KIND,
    contractVersion: LVM_ATOMIC_CRASH_CAPTURE_PROVIDER_CONTRACT_VERSION,
    originLvUuid: "ORIGIN-1234567890",
    snapshotName: "pcr-snapshot-001",
    snapshotSizeBytes: "4096",
    snapshotTag: "pcr.atomic.snapshot-001",
  });
  const dispatchClaim = exactRecord({});
  let state = committed ? "committed" : "empty";
  let exposeCommittedRead = committedReadVisible;
  let storedBinding = committed ? binding : null;
  let storedResult = committed ? atomicCrashCaptureResult(request) : null;
  const catalogue = exactRecord({
    async claimStarting(input) {
      calls.claim += 1;
      if (state === "empty") {
        state = "starting";
        storedBinding = input.providerBinding;
        return exactRecord({ dispatchClaim, outcome: "dispatch" });
      }
      if (state === "committed") {
        return exactRecord({
          outcome: "committed",
          providerBinding: storedBinding,
          result: storedResult,
        });
      }
      return exactRecord({ outcome: "unknown" });
    },
    async commitResult(input) {
      calls.commit += 1;
      assert.strictEqual(input.dispatchClaim, dispatchClaim);
      state = "committed";
      storedResult = input.result;
      if (commitAcknowledgementLoss) {
        throw new Error("commit acknowledgement lost");
      }
      return exactRecord({
        outcome: "committed",
        providerBinding: storedBinding,
        result: storedResult,
      });
    },
    contractVersion: LVM_ATOMIC_CRASH_CAPTURE_PROVIDER_CONTRACT_VERSION,
    async markUncertain(input) {
      calls.mark += 1;
      assert.strictEqual(input.dispatchClaim, dispatchClaim);
      state = "uncertain";
      return exactRecord({ outcome: "uncertain" });
    },
    async readCommitted() {
      calls.read += 1;
      return state === "committed" && exposeCommittedRead
        ? exactRecord({
            outcome: "committed",
            providerBinding: storedBinding,
            result: storedResult,
          })
        : exactRecord({ outcome: "unknown" });
    },
  });
  const driver = exactRecord({
    async captureSnapshot(input) {
      calls.capture += 1;
      if (onCaptureStart !== null) {
        onCaptureStart();
      }
      if (captureThrows) {
        throw new Error("snapshot failed after dispatch");
      }
      if (captureGate !== null) {
        await captureGate;
      }
      return atomicCrashCaptureResult(input.request);
    },
    contractVersion: LVM_ATOMIC_CRASH_CAPTURE_DRIVER_CONTRACT_VERSION,
    async resolveProviderBinding() {
      calls.resolve += 1;
      return binding;
    },
    async verifySnapshot() {
      calls.verify += 1;
      return true;
    },
  });
  const baseBackend = {
    backendId: BACKEND_ID,
    capabilities: backendCapabilities(),
    contractVersion: 1,
  };
  for (const name of [
    "captureCheckpoint",
    "destroySession",
    "detachAttachment",
    "forceFence",
    "prepareWritableAttachment",
    "provisionSession",
    "restoreCheckpoint",
  ]) {
    baseBackend[name] = function atomicCompositionBaseMethod() {};
  }
  return {
    baseBackend,
    calls,
    catalogue,
    driver,
    setCommittedReadVisible(value) {
      exposeCommittedRead = value;
    },
  };
}

function assertLauncherError(code, retryable = false) {
  return (error) => {
    assert.ok(error instanceof PostgresLogicalWriterLauncherError);
    assert.equal(error.code, code);
    assert.equal(error.retryable, retryable);
    assert.equal(Object.isFrozen(error), true);
    assert.equal(Object.hasOwn(error, "cause"), false);
    return true;
  };
}

function assertAtomicCompositionError(code) {
  return (error) => {
    assert.ok(
      error instanceof PostgresLvmAtomicCrashCaptureCompositionError,
    );
    assert.equal(error.code, code);
    assert.equal(error.retryable, false);
    assert.equal(Object.isFrozen(error), true);
    return true;
  };
}

function assertProtectedPromise(promise) {
  assert.equal(promise instanceof Promise, true);
  for (const name of ["then", "catch", "finally", "constructor"]) {
    const descriptor = Object.getOwnPropertyDescriptor(promise, name);
    assert.equal(descriptor?.configurable, false);
    assert.equal(descriptor?.writable, false);
  }
  const chained = promise.then((value) => value);
  assert.notStrictEqual(chained, promise);
  assert.equal(Object.hasOwn(chained, "then"), true);
  void chained.catch(() => undefined);
  return promise;
}

async function fixture({
  launchStatus = "started",
  operationGuard: providedOperationGuard = null,
  reconcileStatus = "not-started",
  reconcileTerminalRecord = false,
  stoppedWriterCoordinator: providedStoppedWriterCoordinator = null,
  supervisorStopThrows = false,
} = {}) {
  const events = [];
  const image = imageFixture();
  const expectedSession = sessionSnapshot(image.manifest);
  const generation = generationSnapshot();
  const authority = new MemoryLaunchAuthority({
    events,
    expectedSession,
    generation,
  });
  const operationGuard =
    providedOperationGuard ?? new MemoryOperationGuard(events);
  const imagePlan = detachedRestorePlan();
  let inspectionCount = 0;
  let inspectionFailureAt = null;
  const inspectCodex = async () => {
    inspectionCount += 1;
    events.push(`image.inspect:${inspectionCount}`);
    if (inspectionCount === inspectionFailureAt) {
      throw new Error("image inspection unavailable");
    }
    return safeProviderCarrier({
      codexBinaryPath: "/opt/portable-codex/bin/codex",
      codexBinarySha256: "b".repeat(64),
      codexVersion: CODEX_VERSION,
    });
  };
  const imagePlanProvider = objectFreeze({
    contractVersion:
      POSTGRES_DETACHED_RESTORE_IMAGE_PLAN_PROVIDER_CONTRACT_VERSION,
    imagePlanProviderId: "launcher-image-provider-001",
    inspectCodex,
    async resolveImagePlan() {
      return safeProviderCarrier({
        configBytes: image.configBytes,
        descriptor: objectFreeze({ ...image.descriptor }),
      });
    },
  });
  const imagePlanBinding = createTestImagePlanBinding(imagePlanProvider);
  const imageReservation = await imagePlanBinding.prepareImageReservation(
    objectFreeze({
      plan: imagePlan,
      sessionManifest: image.manifest,
    }),
  );
  const reserved = expectedImageMeasurement(image);
  events.length = 0;
  const stoppedWriterCoordinator =
    providedStoppedWriterCoordinator ??
    new StoppedWriterCapabilityCoordinator();
  let launchCalls = 0;
  let reconcileCalls = 0;
  let supervisorStopCalls = 0;
  let launchContext = null;
  let reconcileContext = null;
  const supervisorStopWriter = async (binding) => {
    supervisorStopCalls += 1;
    events.push("supervisor.stop");
    assert.equal(Object.isFrozen(binding), true);
    if (supervisorStopThrows) throw new Error("supervisor stop unavailable");
    return {
      confirmation: STOPPED_WRITER_STOP_CONFIRMED,
      contractVersion: LOGICAL_WRITER_SUPERVISOR_CONTRACT_VERSION,
      terminalRecord: supervisorTerminalRecord({
        launchAttemptId:
          launchContext?.attempt.launchAttemptId ?? LAUNCH_ATTEMPT_ID,
        processIncarnationId: binding.processIncarnationId,
        requestSha256:
          launchContext?.operation.requestSha256 ?? "a".repeat(64),
        stopOperationId: binding.stopOperationId,
        stopProofId: binding.stopOperationId,
        writerIncarnationId: binding.writerIncarnationId,
      }),
    };
  };
  const launchWriter = async (context) => {
    launchCalls += 1;
    events.push("supervisor.launch");
    launchContext = context;
    const launchEvidence = evidence(
      context.attempt.launchAttemptId,
      launchStatus,
    );
    return {
      receiptVersion: 2,
      evidence: launchEvidence,
      stopWriter: launchStatus === "started" ? supervisorStopWriter : null,
      terminalRecord:
        launchStatus === "complete-stopped"
          ? supervisorTerminalRecord({
              launchAttemptId: context.attempt.launchAttemptId,
              proofId: launchEvidence.proofId,
              requestSha256: context.operation.requestSha256,
              stopProofId: launchEvidence.proofId,
            })
          : null,
    };
  };
  const reconcileWriterLaunch = async (context) => {
    reconcileCalls += 1;
    events.push("supervisor.reconcile");
    reconcileContext = context;
    const reconcileEvidence = evidence(LAUNCH_ATTEMPT_ID, reconcileStatus);
    return {
      receiptVersion: LOGICAL_WRITER_RECONCILE_RECEIPT_VERSION,
      evidence: reconcileEvidence,
      terminalRecord:
        reconcileTerminalRecord && reconcileStatus === "complete-stopped"
          ? supervisorTerminalRecord({
              launchAttemptId: context.attempt.launchAttemptId,
              processIncarnationId: reconcileEvidence.processIncarnationId,
              proofId: reconcileEvidence.proofId,
              requestSha256: context.operation.requestSha256,
              stopProofId: reconcileEvidence.proofId,
              writerIncarnationId: reconcileEvidence.writerIncarnationId,
            })
          : null,
    };
  };
  const supervisor = {
    contractVersion: LOGICAL_WRITER_SUPERVISOR_CONTRACT_VERSION,
    stateOwnerId: STATE_OWNER_ID,
    supervisorId: SUPERVISOR_ID,
    launchWriter,
    reconcileWriterLaunch,
  };
  const atomicCrashCaptureOwner =
    createPostgresLogicalWriterAtomicCrashCaptureOwner({
      authority,
      imagePlanBinding,
      operationGuard,
      stoppedWriterCoordinator,
      supervisor,
    });
  const facade = atomicCrashCaptureOwner.launcher;
  return {
    atomicCrashCaptureAssembler:
      atomicCrashCaptureOwner.atomicCrashCaptureAssembler,
    atomicCrashCaptureOwner,
    authority,
    events,
    expectedSession,
    facade,
    failInspectionAt(count) {
      inspectionFailureAt = count;
    },
    generation,
    get inspectionCount() {
      return inspectionCount;
    },
    image,
    imagePlan,
    imagePlanBinding,
    imagePlanProvider,
    imageReservation,
    get launchCalls() {
      return launchCalls;
    },
    get launchContext() {
      return launchContext;
    },
    operationGuard,
    get reconcileCalls() {
      return reconcileCalls;
    },
    get reconcileContext() {
      return reconcileContext;
    },
    reserved,
    stoppedWriterCoordinator,
    supervisor,
    get supervisorStopCalls() {
      return supervisorStopCalls;
    },
  };
}

function runInput(value, overrides = {}) {
  return {
    generation: value.generation,
    imageReservation: value.imageReservation,
    launchAttemptId: LAUNCH_ATTEMPT_ID,
    ...overrides,
  };
}

function prepareIntentInput(value, overrides = {}) {
  return {
    expectedSession: value.expectedSession,
    imageReservation: value.imageReservation,
    launchAttemptId: LAUNCH_ATTEMPT_ID,
    ...overrides,
  };
}

function cleanDetachedPrepareSession(value, overrides = {}) {
  const expectedSession = clone(value.expectedSession);
  expectedSession.revision = "10";
  expectedSession.document = {
    ...expectedSession.document,
    activeOperation: null,
    attachment: null,
    documentVersion: SESSION_AUTHORITY_DOCUMENT_VERSION,
    lastOperation: {
      conflictClass: SESSION_OPERATION_CONFLICT_CLASS,
      expectedSessionRevision: "7",
      kind: WRITER_RELEASE_OPERATION_KIND,
      operationId: "writer-release-operation-001",
      operationRevision: "2",
      requestSha256: "6".repeat(64),
      reservationId: "reservation-writer-release-operation-001",
      resultSha256: "7".repeat(64),
      state: "committed",
    },
    launch: null,
    lease: null,
    lifecycle: "DETACHED",
    recovery: null,
    ...overrides,
  };
  return expectedSession;
}

function preparedRunInput(value, overrides = {}) {
  return {
    imageReservation: value.imageReservation,
    launchAttemptId: LAUNCH_ATTEMPT_ID,
    ...overrides,
  };
}

function preparedLaunchExpectedSession(value, generation = value.generation) {
  const expectedSession = clone(value.expectedSession);
  expectedSession.revision = "11";
  expectedSession.updatedAt = generation.committedAt;
  expectedSession.document.lastOperation = {
    conflictClass: SESSION_OPERATION_CONFLICT_CLASS,
    expectedSessionRevision: "8",
    kind: RESTORE_DESTINATION_GENERATION_OPERATION_KIND,
    operationId: generation.operationId,
    operationRevision: "2",
    requestSha256: "4".repeat(64),
    reservationId: `reservation-${generation.operationId}`,
    resultSha256: "5".repeat(64),
    state: "committed",
  };
  return expectedSession;
}

function seedPreparedLaunchHandoff(
  value,
  {
    generation: suppliedGeneration = value.generation,
    measuredImage = {
      projection: value.reserved.projection,
      runtimeIdentity: value.reserved.runtimeIdentity,
    },
    launchEvidence = null,
    state = "prepared",
    supervisor = {
      contractVersion: LOGICAL_WRITER_LAUNCH_CONTRACT_VERSION,
      supervisorId: SUPERVISOR_ID,
    },
  } = {},
) {
  const generation = {
    ...clone(suppliedGeneration),
    claimedAt: BASE_TIME,
    committedAt: PREPARED_TIME,
  };
  const expectedSession = preparedLaunchExpectedSession(value, generation);
  value.authority.beginNextAttempt({ expectedSession, generation });
  value.authority.behaviour.cancelPreparedHandoffConflict = true;
  const request = createWriterLaunchAttemptOperationRequest({
    expectedSession,
    generation,
    measuredImage,
    supervisor,
  });
  value.authority.seed(request, state, launchEvidence);
  return { expectedSession, generation, request };
}

function seedPreparedActivationLaunchHandoff(
  value,
  {
    generationCommittedAt = BASE_TIME,
    launchEvidence = null,
    measuredImage = {
      projection: value.reserved.projection,
      runtimeIdentity: value.reserved.runtimeIdentity,
    },
    state = "prepared",
    supervisor = {
      contractVersion: LOGICAL_WRITER_LAUNCH_CONTRACT_VERSION,
      supervisorId: SUPERVISOR_ID,
    },
  } = {},
) {
  const generation = {
    ...clone(value.generation),
    claimedAt: "2026-08-04T11:59:58.000Z",
    committedAt: generationCommittedAt,
  };
  const expectedSession = preparedLaunchExpectedSession(value, generation);
  expectedSession.updatedAt = PREPARED_TIME;
  expectedSession.document.attachment.operationId =
    RESTORE_ACTIVATION_OPERATION_ID;
  expectedSession.document.lastOperation = {
    ...expectedSession.document.lastOperation,
    kind: RESTORE_ATTACHMENT_ACTIVATION_OPERATION_KIND,
    operationId: RESTORE_ACTIVATION_OPERATION_ID,
  };
  value.authority.beginNextAttempt({ expectedSession, generation });
  value.authority.behaviour.cancelPreparedHandoffConflict = true;
  const request = createWriterLaunchAttemptOperationRequest({
    expectedSession,
    generation,
    measuredImage,
    supervisor,
  });
  value.authority.seed(request, state, launchEvidence);
  return { expectedSession, generation, request };
}

function resolverInput(value, overrides = {}) {
  return {
    attachment: attachment(),
    checkpoint: checkpoint(value.image.manifest.runtime.imageDigest),
    request: captureRequest(),
    ...overrides,
  };
}

async function prepareLaunchCycle(
  value,
  index,
  { attachmentOverrides = {} } = {},
) {
  const ordinal = String(index + 1).padStart(3, "0");
  const launchAttemptId = `writer-launch-attempt-${ordinal}`;
  const writerLease = lease({
    fencingEpoch: String(11 + index),
    leaseId: `lease-${ordinal}`,
  });
  const mounted = attachment(writerLease, {
    operationId: `operation-attach-${ordinal}`,
    proofId: `proof-attachment-${ordinal}`,
    ...attachmentOverrides,
  });
  const generation = generationSnapshot({
    generationId: `restore-generation-${ordinal}`,
    operationId: `restore-generation-operation-${ordinal}`,
  });
  let imageReservation = value.imageReservation;

  if (index > 0) {
    const revision = 8 + index * 3;
    const expectedSession = sessionSnapshot(value.image.manifest, {
      document: sessionDocument(value.image.manifest, {
        attachment: mounted,
        lastOperation: {
          conflictClass: SESSION_OPERATION_CONFLICT_CLASS,
          expectedSessionRevision: String(revision - 3),
          kind: "writer-attachment-acquire-v1",
          operationId: mounted.operationId,
          operationRevision: "2",
          requestSha256: "8".repeat(64),
          reservationId: `reservation-${mounted.operationId}`,
          resultSha256: "9".repeat(64),
          state: "committed",
        },
        lease: writerLease,
        storageRef: {
          ...storageRef(),
          backendId: mounted.backendId,
          storageId: mounted.storageId,
        },
        writerEpoch: writerLease.fencingEpoch,
      }),
      revision: String(revision),
    });
    value.authority.beginNextAttempt({ expectedSession, generation });
    imageReservation = await value.imagePlanBinding.prepareImageReservation(
      objectFreeze({
        plan: value.imagePlan,
        sessionManifest: value.image.manifest,
      }),
    );
  }

  return {
    generation,
    imageReservation,
    launchAttemptId,
    mounted,
    ordinal,
    writerLease,
  };
}

function cycleResolverInput(value, cycle) {
  return {
    attachment: cycle.mounted,
    checkpoint: {
      ...checkpoint(value.image.manifest.runtime.imageDigest),
      sourceFencingEpoch: cycle.writerLease.fencingEpoch,
    },
    request: {
      ...captureRequest(),
      fencingEpoch: cycle.writerLease.fencingEpoch,
      leaseId: cycle.writerLease.leaseId,
      operationId: `checkpoint-capture-operation-${cycle.ordinal}`,
    },
  };
}

test("exports one exact frozen facade and starts/registers before durable finalization", async () => {
  const value = await fixture();
  assert.equal(LOGICAL_WRITER_LAUNCH_CONTRACT_VERSION, 1);
  assert.equal(LOGICAL_WRITER_SUPERVISOR_CONTRACT_VERSION, 4);
  assert.deepEqual(Reflect.ownKeys(value.facade).sort(), [
    "prepareLaunchIntent",
    "reconcileLaunchAttempt",
    "resolveStoppedWriter",
    "retirePreparedCapture",
    "retireStoppedWriter",
    "runLaunch",
    "runPreparedLaunch",
    "stopWriterForCapture",
    "stopWriterForPreparedCapture",
  ]);
  assert.equal(Object.getPrototypeOf(value.facade), null);
  assert.equal(Object.isFrozen(value.facade), true);
  for (const method of Reflect.ownKeys(value.facade)) {
    assert.equal(Object.isFrozen(value.facade[method]), true);
  }

  let resolverVisibleDuringFinalize = true;
  value.authority.beforeFinalize = () => {
    assert.throws(
      () => value.facade.resolveStoppedWriter(resolverInput(value)),
      assertLauncherError("invalid_logical_writer_launch_request"),
    );
    resolverVisibleDuringFinalize = false;
  };
  const pending = value.facade.runLaunch(runInput(value));
  assertProtectedPromise(pending);
  const result = await pending;

  assert.equal(resolverVisibleDuringFinalize, false);
  assert.equal(result.contractVersion, 1);
  assert.equal(result.status, "started");
  assert.equal(result.evidence.status, "started");
  assert.equal(result.writer !== null, true);
  assert.equal(Object.isFrozen(result), true);
  assert.deepEqual(Reflect.ownKeys(result), [
    "contractVersion",
    "attempt",
    "evidence",
    "launch",
    "operation",
    "reservation",
    "session",
    "status",
    "writer",
  ]);
  assert.deepEqual(Reflect.ownKeys(value.launchContext), [
    "contractVersion",
    "attempt",
    "authorityNow",
    "consumedImage",
    "generation",
    "operation",
    "reservation",
    "session",
  ]);
  assert.equal(Object.isFrozen(value.launchContext), true);
  assert.equal(Object.isFrozen(value.launchContext.consumedImage), true);
  assert.deepEqual(JSON.parse(JSON.stringify(value.launchContext.consumedImage)), {
    projection: value.reserved.projection,
    runtimeIdentity: value.reserved.runtimeIdentity,
  });
  assert.equal(value.launchCalls, 1);
  assert.equal(value.reconcileCalls, 0);
  assert.equal(value.inspectionCount, 3);
  assert.equal(value.authority.calls.finalizeStarted, 1);
  assert.equal(value.authority.calls.markUncertain, 0);
  assert.ok(
    value.events.indexOf("authority.claim") <
      value.events.indexOf("image.inspect:3"),
  );
  assert.ok(
    value.events.indexOf("image.inspect:3") <
      value.events.indexOf("supervisor.launch"),
  );
  assert.ok(
    value.events.indexOf("supervisor.launch") <
      value.events.indexOf("authority.finalize-started"),
  );

  const resolved = value.facade.resolveStoppedWriter(resolverInput(value));
  assert.strictEqual(resolved.writer, result.writer);
  assert.deepEqual(
    resolved.canonicalLeaseAtRegistration,
    result.attempt.request.lease,
  );
  assert.match(resolved.stopOperationId, /^writer-stop:[0-9a-f]{64}$/u);
  assert.equal(Object.isFrozen(resolved), true);
  const repeated = value.facade.resolveStoppedWriter(resolverInput(value));
  assert.equal(repeated.stopOperationId, resolved.stopOperationId);
  assert.strictEqual(repeated.writer, result.writer);

  const stopped = await value.facade.stopWriterForCapture(
    resolverInput(value),
  );
  assert.equal(value.supervisorStopCalls, 1);
  assert.equal(Object.isFrozen(stopped.capability), true);
  assert.equal(Object.isFrozen(stopped), true);
  assert.deepEqual(Reflect.ownKeys(stopped), [
    "capability",
    "evidence",
    "resolution",
    "stop",
  ]);
  assert.deepEqual(stopped.resolution, resolved);
  assert.equal(stopped.evidence.status, "complete-stopped");
  assert.equal(stopped.evidence.proofId, resolved.stopOperationId);
  assert.equal(stopped.stop.status, "committed");
  assert.deepEqual(Reflect.ownKeys(stopped.stop.operation.request), [
    "contractVersion",
    "dispatchClaimSha256",
    "launch",
  ]);
  assert.match(
    stopped.stop.operation.request.dispatchClaimSha256,
    /^[0-9a-f]{64}$/u,
  );
  assert.equal(
    Object.hasOwn(stopped.stop.operation.request, "claimToken"),
    false,
  );
  assertCommittedWriterLaunchStopTransitionProof({
    after: stopped.stop.session,
    before: stopped.stop.operation.expectedSession,
    operation: stopped.stop.operation,
    reservation: stopped.stop.reservation,
  });
  assert.deepEqual(
    value.facade.resolveStoppedWriter(resolverInput(value)),
    resolved,
  );
});

test("stop finalization acknowledgement loss replays without a second physical stop", async () => {
  const value = await fixture();
  await value.facade.runLaunch(runInput(value));
  value.authority.behaviour.stopFinalizeThrowAfterCommit = true;

  const stopped = await value.facade.stopWriterForCapture(
    resolverInput(value),
  );

  assert.equal(stopped.stop.finalized, false);
  assert.equal(value.supervisorStopCalls, 1);
  assert.equal(value.authority.calls.finalizeWriterStopped, 2);
  assert.equal(value.authority.calls.markUncertain, 0);
  assert.equal(value.authority.stopState, "committed");
});

test("prepared capture stop atomically materializes one durable capture handoff", async () => {
  const value = await fixture();
  await value.facade.runLaunch(runInput(value));

  const stopped = await value.facade.stopWriterForPreparedCapture(
    resolverInput(value),
  );

  assert.deepEqual(Reflect.ownKeys(stopped), [
    "capture",
    "evidence",
    "resolution",
    "session",
    "status",
    "stop",
  ]);
  assert.equal(Object.hasOwn(stopped, "capability"), false);
  assert.equal(stopped.status, "prepared");
  assert.equal(stopped.capture.operation.state, "prepared");
  assert.equal(stopped.capture.reservation.state, "prepared");
  assert.equal(stopped.stop.operation.request.contractVersion, 3);
  assert.equal(
    stopped.stop.operation.request.captureIntent.admission.request.operationId,
    CAPTURE_OPERATION_ID,
  );
  assert.equal(
    stopped.capture.operation.expectedSession.document.lastOperation.operationId,
    stopped.stop.operation.operationId,
  );
  assert.equal(
    stopped.session.document.activeOperation.operationId,
    CAPTURE_OPERATION_ID,
  );
  assert.equal(value.supervisorStopCalls, 1);
  assert.equal(value.authority.calls.finalizeWriterStopped, 0);
  assert.equal(value.authority.calls.finalizeWriterStoppedAndCapture, 1);
  assert.equal(value.authority.calls.markUncertain, 0);
  assert.throws(
    () => value.facade.retireStoppedWriter(stopped.resolution),
    assertLauncherError("invalid_logical_writer_launch_request"),
  );
  const replacementLease = lease({
    fencingEpoch: "12",
    holderId: "host-002",
    leaseId: "lease-002",
  });
  const replacementAdmission = {
    attachment: attachment(replacementLease, {
      attachmentId: "attachment-002",
    }),
    canonicalLease: replacementLease,
  };
  assert.throws(
    () =>
      value.stoppedWriterCoordinator.assertWriterLaunchAvailable(
        replacementAdmission,
      ),
    (error) =>
      error instanceof StoppedWriterCapabilityError &&
      error.code === "writer_state_conflict",
  );
  assert.throws(
    () =>
      value.facade.retirePreparedCapture({
        resolution: stopped.resolution,
        result: captureResult(value.image.manifest.runtime.imageDigest),
      }),
    assertLauncherError("invalid_logical_writer_launch_request"),
  );
  const result = value.facade.retirePreparedCapture({
    resolution: stopped.resolution,
    result: stopped.stop.operation.request.captureIntent.predeterminedResult,
  });
  assert.equal(result.mutation.status, "checkpoint-created");
  assert.equal(
    value.stoppedWriterCoordinator.assertWriterLaunchAvailable(
      replacementAdmission,
    ),
    undefined,
  );
});

test("prepared capture stop replays only the exact retained handoff", async () => {
  const value = await fixture();
  await value.facade.runLaunch(runInput(value));
  const capture = resolverInput(value);

  const stopped = await value.facade.stopWriterForPreparedCapture(capture);
  const authorityCallsAfterStop = clone(value.authority.calls);
  const guardCallsAfterStop = value.operationGuard.calls;

  const replayed = await value.facade.stopWriterForPreparedCapture(capture);

  assert.strictEqual(replayed, stopped);
  assert.equal(Object.isFrozen(replayed), true);
  assert.equal(value.operationGuard.calls, guardCallsAfterStop + 1);
  assert.deepEqual(value.authority.calls, authorityCallsAfterStop);
  assert.equal(value.supervisorStopCalls, 1);

  const mismatchedCapture = clone(capture);
  mismatchedCapture.request.operationId =
    "checkpoint-capture-operation-mismatch";
  await assert.rejects(
    value.facade.stopWriterForPreparedCapture(mismatchedCapture),
    assertLauncherError("invalid_logical_writer_launch_request"),
  );
  await assert.rejects(
    value.facade.stopWriterForCapture(capture),
    assertLauncherError("invalid_logical_writer_launch_request"),
  );
  assert.deepEqual(value.authority.calls, authorityCallsAfterStop);
  assert.equal(value.supervisorStopCalls, 1);

  value.facade.retirePreparedCapture({
    resolution: stopped.resolution,
    result: stopped.stop.operation.request.captureIntent.predeterminedResult,
  });
  await assert.rejects(
    value.facade.stopWriterForPreparedCapture(capture),
    assertLauncherError("invalid_logical_writer_launch_request"),
  );
  assert.deepEqual(value.authority.calls, authorityCallsAfterStop);
  assert.equal(value.supervisorStopCalls, 1);
});

test("real launcher retry reconciles a retained prepared capture without republishing", async () => {
  const value = await fixture();
  await value.facade.runLaunch(runInput(value));
  const capture = resolverInput(value);
  let stoppedReceipt = null;
  let resumeInvocations = 0;
  let reconciliationInvocations = 0;
  let physicalPublications = 0;
  let committedResult = null;

  const stopWriterForPreparedCapture = async function (input) {
    stoppedReceipt = await value.facade.stopWriterForPreparedCapture(input);
    return stoppedReceipt;
  };
  Object.freeze(stopWriterForPreparedCapture);
  const launcher = Object.freeze({
    retirePreparedCapture: value.facade.retirePreparedCapture,
    retireStoppedWriter: value.facade.retireStoppedWriter,
    stopWriterForCapture: value.facade.stopWriterForCapture,
    stopWriterForPreparedCapture,
  });

  const operation = async () => undefined;
  const backend = {
    backendId: BACKEND_ID,
    capabilities: backendCapabilities(),
    captureReconciliationContractVersion: 1,
    contractVersion: 1,
    destroySession: operation,
    detachAttachment: operation,
    forceFence: operation,
    prepareWritableAttachment: operation,
    preparedCheckpointCaptureContractVersion: 1,
    provisionSession: operation,
    async reconcileCheckpointCapture() {
      reconciliationInvocations += 1;
      assert.notEqual(committedResult, null);
      if (reconciliationInvocations === 1) {
        throw new Error("committed publication is not visible yet");
      }
      return committedResult;
    },
    async resumePreparedCheckpointCapture() {
      resumeInvocations += 1;
      if (resumeInvocations === 1) {
        physicalPublications += 1;
        committedResult =
          stoppedReceipt.capture.operation.request.predeterminedResult;
        throw new Error("publication acknowledgement lost");
      }
      throw new Error("durable capture is no longer prepared");
    },
    restoreCheckpoint: operation,
    captureCheckpoint: operation,
  };
  const composition = createPostgresDurableStopCaptureComposition({ launcher });
  const options = {
    attachment: capture.attachment,
    backend,
    canonicalLease: lease(),
    checkpointClass: capture.checkpoint.checkpointClass,
    createdAt: capture.checkpoint.createdAt,
    manifest: value.image.manifest,
    now: Date.parse(BASE_TIME),
    request: capture.request,
    storageRef: storageRef(),
  };

  await assert.rejects(
    composition.runPreparedCapture(options),
    (error) => {
      assert.equal(
        error?.code,
        "postgres_durable_stop_capture_composition_outcome_uncertain",
      );
      return true;
    },
  );
  assert.equal(physicalPublications, 1);
  assert.equal(resumeInvocations, 1);
  assert.equal(reconciliationInvocations, 1);
  assert.equal(value.supervisorStopCalls, 1);

  const result = await composition.runPreparedCapture(options);

  assert.deepEqual(result, structuredClone(committedResult));
  assert.equal(physicalPublications, 1);
  assert.equal(resumeInvocations, 2);
  assert.equal(reconciliationInvocations, 2);
  assert.equal(value.supervisorStopCalls, 1);
  assert.throws(
    () => value.facade.resolveStoppedWriter(capture),
    assertLauncherError("invalid_logical_writer_launch_request"),
  );
  assert.equal(value.stoppedWriterCoordinator.dispose(), undefined);
});

test("prepared capture handoff acknowledgement loss uses exact atomic readback", async () => {
  const value = await fixture();
  await value.facade.runLaunch(runInput(value));
  value.authority.behaviour.stopFinalizeThrowAfterCommit = true;

  const stopped = await value.facade.stopWriterForPreparedCapture(
    resolverInput(value),
  );

  assert.equal(stopped.status, "prepared");
  assert.equal(stopped.stop.finalized, false);
  assert.equal(value.supervisorStopCalls, 1);
  assert.equal(value.authority.calls.finalizeWriterStopped, 0);
  assert.equal(value.authority.calls.finalizeWriterStoppedAndCapture, 1);
  assert.equal(value.authority.calls.stopReconcile, 1);
  assert.equal(value.authority.calls.markUncertain, 0);
  value.facade.retirePreparedCapture({
    resolution: stopped.resolution,
    result: stopped.stop.operation.request.captureIntent.predeterminedResult,
  });
});

test("persistent stop finalization acknowledgement loss uses exact terminal readback", async () => {
  const value = await fixture();
  await value.facade.runLaunch(runInput(value));
  value.authority.behaviour.stopFinalizeThrowAfterCommitAlways = true;

  const stopped = await value.facade.stopWriterForCapture(
    resolverInput(value),
  );

  assert.equal(stopped.stop.finalized, false);
  assert.equal(stopped.stop.status, "committed");
  assert.equal(value.supervisorStopCalls, 1);
  assert.equal(value.authority.calls.finalizeWriterStopped, 3);
  assert.equal(value.authority.calls.stopReconcile, 4);
  assert.equal(value.authority.calls.markUncertain, 0);
  assert.equal(value.authority.stopState, "committed");
});

test("an uncertain stop finalizes at revision two without a second physical stop", async () => {
  const value = await fixture();
  await value.facade.runLaunch(runInput(value));
  value.authority.behaviour.stopMarkUncertainBeforeFinalize = true;

  const stopped = await value.facade.stopWriterForCapture(
    resolverInput(value),
  );

  assert.equal(stopped.stop.finalized, true);
  assert.equal(stopped.stop.operation.revision, "3");
  assert.equal(value.supervisorStopCalls, 1);
  assert.equal(value.authority.calls.finalizeWriterStopped, 2);
  assert.equal(value.authority.calls.markUncertain, 0);
  assert.equal(value.authority.stopState, "committed");
});

test("a renewed lease preserves the registered writer identity for stop", async () => {
  const value = await fixture();
  const started = await value.facade.runLaunch(runInput(value));
  const renewedLease = lease({
    expiresAt: "2027-08-04T12:05:00.000Z",
  });
  value.authority.currentLeaseOverride = renewedLease;

  const stopped = await value.facade.stopWriterForCapture(
    resolverInput(value),
  );

  assert.equal(stopped.stop.status, "committed");
  assert.equal(value.supervisorStopCalls, 1);
  assert.deepEqual(
    stopped.stop.operation.expectedSession.document.lease,
    renewedLease,
  );
  assert.deepEqual(
    stopped.resolution.canonicalLeaseAtRegistration,
    started.attempt.request.lease,
  );
  assert.equal(
    stopped.resolution.canonicalLeaseAtRegistration.expiresAt,
    "2027-08-04T12:00:00.000Z",
  );
});

test("stop refreshes a lease renewal that supersedes its first reserve precondition", async () => {
  const value = await fixture();
  const started = await value.facade.runLaunch(runInput(value));
  const reservesBeforeStop = value.authority.calls.reserve;
  const renewedLease = lease({
    expiresAt: "2027-08-04T12:05:00.000Z",
  });
  value.authority.behaviour.stopSessionSupersededBeforeReserve = {
    lease: renewedLease,
    updatedAt: "2026-08-04T12:00:04.500Z",
  };

  const stopped = await value.facade.stopWriterForCapture(
    resolverInput(value),
  );

  assert.equal(stopped.stop.status, "committed");
  assert.equal(
    stopped.stop.operation.expectedSession.revision,
    (BigInt(started.session.revision) + 1n).toString(),
  );
  assert.deepEqual(
    stopped.stop.operation.expectedSession.document.lease,
    renewedLease,
  );
  assert.deepEqual(
    stopped.resolution.canonicalLeaseAtRegistration,
    started.attempt.request.lease,
  );
  assert.equal(value.authority.calls.reserve - reservesBeforeStop, 2);
  assert.equal(value.authority.calls.stopReconcile, 1);
  assert.equal(value.authority.calls.stopClaim, 1);
  assert.equal(value.authority.calls.finalizeWriterStopped, 1);
  assert.equal(value.supervisorStopCalls, 1);
  assert.ok(
    value.events.indexOf("authority.read-session") <
      value.events.indexOf("authority.reconcile-stop"),
  );
  assert.ok(
    value.events.indexOf("authority.reconcile-stop") <
      value.events.indexOf("authority.claim-stop"),
  );
});

test("refreshed stop reserve acknowledgement loss reconciles the renewed identity", async () => {
  const value = await fixture();
  await value.facade.runLaunch(runInput(value));
  const reservesBeforeStop = value.authority.calls.reserve;
  value.authority.behaviour.stopSessionSupersededBeforeReserve = {
    lease: lease({ expiresAt: "2027-08-04T12:05:00.000Z" }),
    updatedAt: "2026-08-04T12:00:04.500Z",
  };
  value.authority.behaviour.stopReserveThrowAfterCommit = true;

  const stopped = await value.facade.stopWriterForCapture(
    resolverInput(value),
  );

  assert.equal(stopped.stop.status, "committed");
  assert.equal(value.authority.calls.reserve - reservesBeforeStop, 2);
  assert.equal(value.authority.calls.stopReconcile, 2);
  assert.equal(value.authority.calls.stopClaim, 1);
  assert.equal(value.authority.calls.finalizeWriterStopped, 1);
  assert.equal(value.supervisorStopCalls, 1);
});

test("stop rejects a superseding revision that rolls back its retained lease expiry", async () => {
  const value = await fixture();
  await value.facade.runLaunch(runInput(value));
  value.authority.currentLeaseOverride = lease({
    expiresAt: "2027-08-04T12:05:00.000Z",
  });
  value.authority.behaviour.stopSessionSupersededBeforeReserve = {
    lease: lease({ expiresAt: "2027-08-04T12:04:00.000Z" }),
    updatedAt: "2026-08-04T12:00:04.500Z",
  };
  const reservesBeforeStop = value.authority.calls.reserve;

  await assert.rejects(
    value.facade.stopWriterForCapture(resolverInput(value)),
    assertLauncherError("logical_writer_launch_outcome_uncertain"),
  );

  assert.equal(value.authority.calls.reserve - reservesBeforeStop, 1);
  assert.equal(value.authority.calls.stopReconcile, 1);
  assert.equal(value.authority.calls.stopClaim, 0);
  assert.equal(value.authority.calls.finalizeWriterStopped, 0);
  assert.equal(value.supervisorStopCalls, 0);
});

for (const [name, currentLease] of [
  [
    "expiration rollback",
    lease({ expiresAt: "2027-08-04T11:59:59.000Z" }),
  ],
  [
    "session change",
    lease({ sessionId: "019f3d80-0000-7000-8000-000000000099" }),
  ],
  ["lease change", lease({ leaseId: "lease-002" })],
  ["holder change", lease({ holderId: "host-002" })],
  ["fencing epoch change", lease({ fencingEpoch: "12" })],
]) {
  test(`stop rejects current lease ${name} before durable reserve`, async () => {
    const value = await fixture();
    await value.facade.runLaunch(runInput(value));
    const reservesBeforeStop = value.authority.calls.reserve;
    value.authority.currentLeaseOverride = currentLease;

    await assert.rejects(
      value.facade.stopWriterForCapture(resolverInput(value)),
      assertLauncherError("logical_writer_launch_outcome_uncertain"),
    );

    assert.equal(value.authority.calls.reserve, reservesBeforeStop);
    assert.equal(value.authority.calls.stopClaim, 0);
    assert.equal(value.authority.calls.finalizeWriterStopped, 0);
    assert.equal(value.supervisorStopCalls, 0);
  });
}

test("stop claim acknowledgement loss recovers the owned physical stop", async () => {
  const value = await fixture();
  await value.facade.runLaunch(runInput(value));
  const readsBeforeStop = value.events.filter(
    (entry) => entry === "authority.read-session",
  ).length;
  value.authority.behaviour.stopClaimThrowAfterCommit = true;

  const stopped = await value.facade.stopWriterForCapture(
    resolverInput(value),
  );

  assert.equal(stopped.stop.status, "committed");
  assert.equal(value.authority.stopState, "committed");
  assert.equal(value.supervisorStopCalls, 1);
  assert.equal(value.authority.calls.stopClaim, 1);
  assert.equal(
    value.events.filter((entry) => entry === "authority.read-session").length -
      readsBeforeStop,
    1,
  );
  assert.equal(value.authority.calls.stopReconcile, 1);
  assert.equal(value.authority.calls.markUncertain, 0);
  assert.equal(value.authority.calls.finalizeWriterStopped, 1);

  await assert.rejects(
    value.facade.stopWriterForCapture(resolverInput(value)),
    assertLauncherError("invalid_logical_writer_launch_request"),
  );
  assert.equal(value.authority.calls.stopClaim, 1);
  assert.equal(value.authority.calls.stopReconcile, 1);
  assert.equal(value.supervisorStopCalls, 1);
});

test("a pre-commit stop claim failure clears its witness before retry", async () => {
  const value = await fixture();
  await value.facade.runLaunch(runInput(value));
  value.authority.behaviour.stopClaimThrowBeforeCommit = true;

  await assert.rejects(
    value.facade.stopWriterForCapture(resolverInput(value)),
    assertLauncherError("logical_writer_launch_outcome_uncertain"),
  );
  assert.equal(value.authority.stopState, "prepared");
  assert.equal(value.authority.calls.stopClaim, 1);
  assert.equal(value.authority.calls.stopReconcile, 1);
  assert.equal(value.authority.calls.markUncertain, 0);
  assert.equal(value.supervisorStopCalls, 0);

  value.authority.behaviour.stopClaimThrowBeforeCommit = false;
  const stopped = await value.facade.stopWriterForCapture(resolverInput(value));
  assert.equal(stopped.stop.status, "committed");
  assert.equal(value.authority.calls.stopClaim, 2);
  assert.equal(value.authority.calls.stopReconcile, 2);
  assert.equal(value.authority.calls.markUncertain, 0);
  assert.equal(value.supervisorStopCalls, 1);
});

test("a rejected stop claim witness cannot authorize a foreign starting state", async () => {
  const value = await fixture();
  await value.facade.runLaunch(runInput(value));
  value.authority.behaviour.stopClaimThrowBeforeCommit = true;

  await assert.rejects(
    value.facade.stopWriterForCapture(resolverInput(value)),
    assertLauncherError("logical_writer_launch_outcome_uncertain"),
  );
  assert.equal(value.authority.stopState, "prepared");
  assert.equal(value.authority.calls.stopClaim, 1);
  assert.equal(value.authority.calls.stopReconcile, 1);
  assert.equal(value.supervisorStopCalls, 0);

  value.authority.stopState = "starting";
  await assert.rejects(
    value.facade.stopWriterForCapture(resolverInput(value)),
    assertLauncherError("logical_writer_launch_outcome_uncertain"),
  );
  assert.equal(value.authority.calls.stopClaim, 1);
  assert.equal(value.authority.calls.stopReconcile, 2);
  assert.equal(value.authority.calls.markUncertain, 0);
  assert.equal(value.supervisorStopCalls, 0);
});

test("a pre-commit stop claim failure cannot adopt foreign starting during readback", async () => {
  const value = await fixture();
  await value.facade.runLaunch(runInput(value));
  value.authority.behaviour.stopClaimThrowBeforeCommit = true;
  value.authority.behaviour.stopForeignClaimBeforeReconcile = true;

  await assert.rejects(
    value.facade.stopWriterForCapture(resolverInput(value)),
    assertLauncherError("logical_writer_launch_outcome_uncertain"),
  );
  assert.equal(value.authority.stopState, "starting");
  assert.equal(value.authority.calls.stopClaim, 1);
  assert.equal(value.authority.calls.stopReconcile, 1);
  assert.equal(value.authority.calls.markUncertain, 0);
  assert.equal(value.authority.calls.finalizeWriterStopped, 0);
  assert.equal(value.supervisorStopCalls, 0);

  await assert.rejects(
    value.facade.stopWriterForCapture(resolverInput(value)),
    assertLauncherError("invalid_logical_writer_launch_request"),
  );
  assert.equal(value.authority.calls.stopClaim, 1);
  assert.equal(value.authority.calls.stopReconcile, 1);
  assert.equal(value.supervisorStopCalls, 0);
});

test("a lost stop claim race never authorizes physical stop", async () => {
  const value = await fixture();
  await value.facade.runLaunch(runInput(value));
  value.authority.behaviour.stopClaimLosesPreparedRace = true;

  await assert.rejects(
    value.facade.stopWriterForCapture(resolverInput(value)),
    assertLauncherError("logical_writer_launch_outcome_uncertain"),
  );
  assert.equal(value.authority.stopState, "starting");
  assert.equal(value.authority.calls.stopClaim, 1);
  assert.equal(value.authority.calls.markUncertain, 0);
  assert.equal(value.supervisorStopCalls, 0);

  await assert.rejects(
    value.facade.stopWriterForCapture(resolverInput(value)),
    assertLauncherError("invalid_logical_writer_launch_request"),
  );
  assert.equal(value.authority.calls.stopClaim, 1);
  assert.equal(value.authority.calls.markUncertain, 0);
  assert.equal(value.supervisorStopCalls, 0);
});

test("exact stop replay resumes a retained prepared operation before physical stop", async () => {
  const value = await fixture();
  await value.facade.runLaunch(runInput(value));
  value.authority.behaviour.stopReserveThrowAfterCommit = true;
  value.authority.behaviour.stopReconcileThrowsOnce = true;

  await assert.rejects(
    value.facade.stopWriterForCapture(resolverInput(value)),
    assertLauncherError("logical_writer_launch_outcome_uncertain"),
  );
  assert.equal(value.authority.stopState, "prepared");
  assert.equal(value.supervisorStopCalls, 0);
  assert.equal(value.authority.calls.stopClaim, 0);

  const stopped = await value.facade.stopWriterForCapture(resolverInput(value));
  assert.equal(stopped.stop.status, "committed");
  assert.equal(value.authority.calls.reserve, 2);
  assert.equal(value.authority.calls.stopReconcile, 2);
  assert.equal(value.authority.calls.stopClaim, 1);
  assert.equal(value.supervisorStopCalls, 1);
});

test("exact stop replay recovers an owned unacknowledged starting claim", async () => {
  const value = await fixture();
  await value.facade.runLaunch(runInput(value));
  value.authority.behaviour.stopClaimThrowAfterCommit = true;
  value.authority.behaviour.stopReconcileThrowsOnce = true;

  await assert.rejects(
    value.facade.stopWriterForCapture(resolverInput(value)),
    assertLauncherError("logical_writer_launch_outcome_uncertain"),
  );
  assert.equal(value.authority.stopState, "starting");
  assert.equal(value.supervisorStopCalls, 0);

  const stopped = await value.facade.stopWriterForCapture(
    resolverInput(value),
  );
  assert.equal(stopped.stop.status, "committed");
  assert.equal(value.authority.calls.stopClaim, 1);
  assert.equal(value.authority.calls.stopReconcile, 2);
  assert.equal(value.authority.calls.finalizeWriterStopped, 1);
  assert.equal(value.supervisorStopCalls, 1);
});

for (const durableState of ["starting", "uncertain"]) {
  test(`a ${durableState} stop without this record's claim grant never dispatches physical stop`, async () => {
    const value = await fixture();
    await value.facade.runLaunch(runInput(value));
    value.authority.behaviour.stopReserveThrowAfterCommit = true;
    value.authority.behaviour.stopReconcileThrowsOnce = true;

    await assert.rejects(
      value.facade.stopWriterForCapture(resolverInput(value)),
      assertLauncherError("logical_writer_launch_outcome_uncertain"),
    );
    assert.equal(value.authority.stopState, "prepared");
    value.authority.stopState = durableState;

    await assert.rejects(
      value.facade.stopWriterForCapture(resolverInput(value)),
      assertLauncherError("logical_writer_launch_outcome_uncertain"),
    );
    assert.equal(value.authority.calls.stopClaim, 0);
    assert.equal(value.supervisorStopCalls, 0);
  });
}

test("persistent malformed stop finalization uses exact terminal readback", async () => {
  const value = await fixture();
  await value.facade.runLaunch(runInput(value));
  value.authority.stopFinalizationMutation = (receipt) => {
    receipt.session.document.lastOperation.resultSha256 = "d".repeat(64);
    return receipt;
  };

  const stopped = await value.facade.stopWriterForCapture(
    resolverInput(value),
  );

  assert.equal(stopped.stop.finalized, false);
  assert.equal(stopped.stop.status, "committed");
  assert.equal(value.supervisorStopCalls, 1);
  assert.equal(value.authority.calls.finalizeWriterStopped, 3);
  assert.equal(value.authority.calls.stopReconcile, 4);
  assert.equal(value.authority.calls.markUncertain, 0);
  assert.deepEqual(
    value.facade.resolveStoppedWriter(resolverInput(value)),
    stopped.resolution,
  );
});

test("fresh non-committed stop readback fails closed after finalization exhaustion", async () => {
  const value = await fixture();
  await value.facade.runLaunch(runInput(value));
  value.authority.behaviour.stopFinalizeThrowBeforeCommit = true;

  await assert.rejects(
    value.facade.stopWriterForCapture(resolverInput(value)),
    assertLauncherError("logical_writer_launch_outcome_uncertain"),
  );

  assert.equal(value.supervisorStopCalls, 1);
  assert.equal(value.authority.calls.finalizeWriterStopped, 3);
  assert.equal(value.authority.calls.stopReconcile, 4);
  assert.equal(value.authority.calls.markUncertain, 1);
  assert.equal(value.authority.stopState, "uncertain");
  assert.throws(
    () => value.facade.resolveStoppedWriter(resolverInput(value)),
    assertLauncherError("invalid_logical_writer_launch_request"),
  );
});

const committedStopReadbackDriftCases = [
  ["request", (receipt) => {
    receipt.operation.request.launch.supervisorId = "foreign-supervisor";
  }],
  ["claim token", (receipt) => {
    receipt.claimTokenMatched = false;
  }],
  ["complete evidence", (receipt) => {
    delete receipt.operation.result.evidence.writerIncarnationId;
  }],
  ["released reservation", (receipt) => {
    receipt.reservation.state = "committed";
  }],
  ["terminal session pointer", (receipt) => {
    receipt.session.document.lastOperation.operationId =
      "foreign-stop-operation";
  }],
  ["terminal session revision", (receipt) => {
    receipt.session.revision = (
      BigInt(receipt.session.revision) + 1n
    ).toString();
  }],
];

for (const [relation, mutate] of committedStopReadbackDriftCases) {
  test(`committed stop readback rejects ${relation} drift`, async () => {
    const value = await fixture();
    await value.facade.runLaunch(runInput(value));
    value.authority.stopFinalizationMutation = (receipt) => {
      receipt.session.document.lastOperation.resultSha256 = "d".repeat(64);
      return receipt;
    };
    value.authority.stopReconcileMutation = (receipt) => {
      mutate(receipt);
      return receipt;
    };

    await assert.rejects(
      value.facade.stopWriterForCapture(resolverInput(value)),
      assertLauncherError("logical_writer_launch_outcome_uncertain"),
    );

    assert.equal(value.supervisorStopCalls, 1);
    assert.equal(value.authority.calls.finalizeWriterStopped, 3);
    assert.equal(value.authority.calls.stopReconcile, 4);
    assert.equal(value.authority.calls.markUncertain, 1);
    assert.equal(value.authority.stopState, "committed");
    assert.throws(
      () => value.facade.resolveStoppedWriter(resolverInput(value)),
      assertLauncherError("invalid_logical_writer_launch_request"),
    );
  });
}

test("concurrent same-tuple stop calls issue one capability and stop once", async () => {
  const value = await fixture();
  await value.facade.runLaunch(runInput(value));
  const capture = resolverInput(value);

  const outcomes = await Promise.allSettled([
    value.facade.stopWriterForCapture(capture),
    value.facade.stopWriterForCapture(capture),
  ]);
  const fulfilled = outcomes.filter((outcome) => outcome.status === "fulfilled");
  const rejected = outcomes.filter((outcome) => outcome.status === "rejected");

  assert.equal(fulfilled.length, 1);
  assert.equal(rejected.length, 1);
  assertLauncherError("invalid_logical_writer_launch_request")(
    rejected[0].reason,
  );
  assert.equal(value.supervisorStopCalls, 1);
  assert.equal(value.authority.calls.stopClaim, 1);
  assert.equal(value.authority.calls.finalizeWriterStopped, 1);
});

test("prepares one exact durable launch seed without consuming or reserving", async () => {
  const value = await fixture();

  const pending = value.facade.prepareLaunchIntent(prepareIntentInput(value));
  assertProtectedPromise(pending);
  const intent = await pending;

  assert.deepEqual(Reflect.ownKeys(intent), [
    "launchAttemptId",
    "measuredImage",
    "supervisor",
  ]);
  assert.equal(Object.getPrototypeOf(intent), null);
  assert.equal(Object.isFrozen(intent), true);
  assert.equal(Object.isFrozen(intent.measuredImage), true);
  assert.deepEqual(JSON.parse(jsonStringify(intent)), {
    launchAttemptId: LAUNCH_ATTEMPT_ID,
    measuredImage: {
      projection: value.reserved.projection,
      runtimeIdentity: value.reserved.runtimeIdentity,
    },
    supervisor: {
      contractVersion: LOGICAL_WRITER_LAUNCH_CONTRACT_VERSION,
      supervisorId: SUPERVISOR_ID,
    },
  });
  assert.equal(value.inspectionCount, 2);
  assert.equal(value.authority.calls.read, 0);
  assert.equal(value.authority.calls.reserve, 0);
  assert.equal(value.authority.calls.claim, 0);
  assert.equal(value.launchCalls, 0);
  assert.deepEqual(value.events, [
    `guard.enter:${LAUNCH_ATTEMPT_ID}`,
    "guard.assert-held",
    "image.inspect:2",
    "guard.assert-held",
    `guard.exit:${LAUNCH_ATTEMPT_ID}`,
  ]);

  const legacy = await value.facade.runLaunch(runInput(value));
  assert.equal(legacy.status, "started");
  assert.equal(value.inspectionCount, 4);
  assert.equal(value.authority.calls.reserve, 1);
  assert.equal(value.authority.calls.claim, 1);
  assert.equal(value.launchCalls, 1);
  const replay = await value.facade.reconcileLaunchAttempt({
    launchAttemptId: LAUNCH_ATTEMPT_ID,
  });
  assert.strictEqual(replay.writer, legacy.writer);
  assert.equal(value.launchCalls, 1);
});

test("prepares a launch intent through a real callback-only PostgreSQL operation guard", async () => {
  const database = callbackOnlyOperationGuardPool();
  const operationGuard = new PostgresOperationGuard({
    dedicatedPool: database.pool,
  });
  const value = await fixture({ operationGuard });

  const intent = await value.facade.prepareLaunchIntent(
    prepareIntentInput(value),
  );

  assert.equal(intent.launchAttemptId, LAUNCH_ATTEMPT_ID);
  assert.equal(intent.supervisor.supervisorId, SUPERVISOR_ID);
  assert.equal(Object.isFrozen(intent), true);
  assert.equal(value.inspectionCount, 2);
  assert.deepEqual(value.events, ["image.inspect:2"]);
  assert.equal(database.state.connectCalls, 1);
  assert.equal(database.state.connectCallbacks, 1);
  assert.equal(database.state.queryCalls > 0, true);
  assert.equal(database.state.queryCallbacks, database.state.queryCalls);
  assert.equal(database.state.releaseCalls, 1);
  assert.equal(database.state.lockHeld, false);
});

for (const terminalKind of [
  WRITER_RELEASE_OPERATION_KIND,
  WRITER_FORCE_FENCE_OPERATION_KIND,
]) {
  test(`prepares from a clean detached ${terminalKind} session without side effects`, async () => {
    const value = await fixture();
    const expectedSession = cleanDetachedPrepareSession(value);
    expectedSession.document.lastOperation.kind = terminalKind;

    const intent = await value.facade.prepareLaunchIntent(
      prepareIntentInput(value, { expectedSession }),
    );

    assert.equal(intent.launchAttemptId, LAUNCH_ATTEMPT_ID);
    assert.equal(value.authority.calls.read, 0);
    assert.equal(value.authority.calls.reserve, 0);
    assert.equal(value.authority.calls.claim, 0);
    assert.equal(value.launchCalls, 0);
    assert.equal(value.inspectionCount, 2);

    const stillIssued = await value.imagePlanBinding.revalidateImageReservation(
      value.imageReservation,
    );
    assert.equal(Object.isFrozen(stillIssued), true);
    assert.equal(value.inspectionCount, 3);
  });
}

for (const [name, mutate] of [
  [
    "document version 2",
    (document) => {
      document.documentVersion = 2;
    },
  ],
  [
    "recovery state",
    (document) => {
      document.recovery = { phase: "restoring" };
    },
  ],
  [
    "active operation",
    (document) => {
      const { resultSha256, ...activeOperation } = document.lastOperation;
      void resultSha256;
      document.activeOperation = {
        ...activeOperation,
        state: "prepared",
      };
    },
  ],
  [
    "launch pointer",
    (document) => {
      document.launch = { launchAttemptId: LAUNCH_ATTEMPT_ID };
    },
  ],
  [
    "attachment",
    (document) => {
      document.attachment = attachment();
    },
  ],
  [
    "lease",
    (document) => {
      document.lease = lease();
    },
  ],
  [
    "missing terminal operation",
    (document) => {
      document.lastOperation = null;
    },
  ],
  [
    "non-committed terminal operation",
    (document) => {
      document.lastOperation.state = "uncertain";
    },
  ],
  [
    "unrelated terminal operation",
    (document) => {
      document.lastOperation.kind =
        RESTORE_DESTINATION_GENERATION_OPERATION_KIND;
    },
  ],
  ...["ATTACHED", "ATTACHING", "BLOCKED", "FENCING", "RELEASING"].map(
    (lifecycle) => [
      `${lifecycle} lifecycle with detached fields`,
      (document) => {
        document.lifecycle = lifecycle;
      },
    ],
  ),
]) {
  test(`rejects dirty or mixed detached prepare input with ${name}`, async () => {
    const value = await fixture();
    const expectedSession = cleanDetachedPrepareSession(value);
    mutate(expectedSession.document);

    await assert.rejects(
      value.facade.prepareLaunchIntent(
        prepareIntentInput(value, { expectedSession }),
      ),
      assertLauncherError("invalid_logical_writer_launch_request"),
    );
    assert.equal(value.authority.calls.read, 0);
    assert.equal(value.authority.calls.reserve, 0);
    assert.equal(value.authority.calls.claim, 0);
    assert.equal(value.launchCalls, 0);
    assert.equal(value.inspectionCount, 1);
    assert.deepEqual(value.events, []);
  });
}

for (const [name, mutate] of [
  [
    "terminal revision mismatch",
    (expectedSession) => {
      expectedSession.revision = "9";
    },
  ],
  [
    "malformed terminal hash",
    (expectedSession) => {
      expectedSession.document.lastOperation.requestSha256 =
        "not-a-sha256";
    },
  ],
  [
    "malformed terminal operation id",
    (expectedSession) => {
      expectedSession.document.lastOperation.operationId =
        "not an opaque id";
    },
  ],
  [
    "authority time rollback",
    (expectedSession) => {
      expectedSession.updatedAt = "2026-08-04T11:59:59.000Z";
    },
  ],
  [
    "storage identity mismatch",
    (expectedSession) => {
      expectedSession.document.storageRef.sessionId =
        "019f3d80-0000-7000-8000-000000000003";
    },
  ],
]) {
  test(`rejects malformed detached authority snapshot with ${name}`, async () => {
    const value = await fixture();
    const expectedSession = cleanDetachedPrepareSession(value);
    mutate(expectedSession);

    await assert.rejects(
      value.facade.prepareLaunchIntent(
        prepareIntentInput(value, { expectedSession }),
      ),
      assertLauncherError("invalid_logical_writer_launch_request"),
    );
    assert.equal(value.authority.calls.read, 0);
    assert.equal(value.authority.calls.reserve, 0);
    assert.equal(value.authority.calls.claim, 0);
    assert.equal(value.launchCalls, 0);
    assert.equal(value.inspectionCount, 1);
    assert.deepEqual(value.events, []);
  });
}

test("runs an activation-materialized prepared launch once without reserving", async () => {
  const value = await fixture();
  const detachedSession = cleanDetachedPrepareSession(value);
  const intent = await value.facade.prepareLaunchIntent(
    prepareIntentInput(value, { expectedSession: detachedSession }),
  );
  const seeded = seedPreparedActivationLaunchHandoff(value, {
    measuredImage: intent.measuredImage,
    supervisor: intent.supervisor,
  });

  const started = await value.facade.runPreparedLaunch(
    preparedRunInput(value),
  );
  const replayed = await value.facade.runPreparedLaunch(
    preparedRunInput(value),
  );

  assert.equal(started.status, "started");
  assert.strictEqual(replayed.writer, started.writer);
  assert.equal(value.authority.calls.reserve, 0);
  assert.equal(value.authority.calls.claim, 1);
  assert.equal(value.authority.calls.finalizeStarted, 1);
  assert.equal(value.launchCalls, 1);
  assert.equal(value.reconcileCalls, 0);
  assert.deepEqual(
    JSON.parse(jsonStringify(value.authority.lastClaimInput)),
    JSON.parse(
      jsonStringify({
        expectedOperationRevision: "0",
        expectedSession: seeded.expectedSession,
        kind: WRITER_LAUNCH_ATTEMPT_OPERATION_KIND,
        operationId: LAUNCH_ATTEMPT_ID,
        request: seeded.request,
        stateOwnerId: STATE_OWNER_ID,
      }),
    ),
  );
});

test("retains the legacy generation-materialized prepared launch handoff", async () => {
  const value = await fixture();
  const intent = await value.facade.prepareLaunchIntent(
    prepareIntentInput(value),
  );
  const seeded = seedPreparedLaunchHandoff(value, {
    measuredImage: intent.measuredImage,
    supervisor: intent.supervisor,
  });
  value.events.length = 0;

  let registeredBeforeFinalize = false;
  value.authority.beforeFinalize = () => {
    assert.throws(
      () =>
        value.stoppedWriterCoordinator.registerWriter({
          attachment: attachment(),
          canonicalLease: lease(),
          processIncarnationId: "replacement-process-prepared-001",
          stopWriter: async () => STOPPED_WRITER_STOP_CONFIRMED,
          writerIncarnationId: "replacement-writer-prepared-001",
        }),
      (error) =>
        error instanceof StoppedWriterCapabilityError &&
        error.code === "writer_state_conflict",
    );
    registeredBeforeFinalize = true;
  };

  const pending = value.facade.runPreparedLaunch(preparedRunInput(value));
  assertProtectedPromise(pending);
  const result = await pending;

  assert.equal(result.status, "started");
  assert.equal(result.writer !== null, true);
  assert.equal(registeredBeforeFinalize, true);
  assert.equal(value.authority.calls.reserve, 0);
  assert.equal(value.authority.calls.claim, 1);
  assert.equal(value.authority.calls.finalizeStarted, 1);
  assert.equal(value.launchCalls, 1);
  assert.equal(value.reconcileCalls, 0);
  assert.equal(value.inspectionCount, 4);
  assert.deepEqual(
    JSON.parse(jsonStringify(value.authority.lastClaimInput)),
    JSON.parse(
      jsonStringify({
        expectedOperationRevision: "0",
        expectedSession: seeded.expectedSession,
        kind: WRITER_LAUNCH_ATTEMPT_OPERATION_KIND,
        operationId: LAUNCH_ATTEMPT_ID,
        request: seeded.request,
        stateOwnerId: STATE_OWNER_ID,
      }),
    ),
  );
  assert.deepEqual(value.events, [
    `guard.enter:${LAUNCH_ATTEMPT_ID}`,
    "authority.read-attempt",
    "guard.assert-held",
    "image.inspect:3",
    "guard.assert-held",
    "authority.claim",
    "guard.assert-held",
    "image.inspect:4",
    "guard.assert-held",
    "supervisor.launch",
    "guard.assert-held",
    "guard.assert-held",
    "authority.finalize-started",
    `guard.exit:${LAUNCH_ATTEMPT_ID}`,
  ]);
});

for (const [name, mutateReceipt] of [
  [
    "hostile producer kind",
    (receipt) => {
      receipt.operation.expectedSession.document.lastOperation.kind =
        WRITER_RELEASE_OPERATION_KIND;
      receipt.session.document.lastOperation.kind =
        WRITER_RELEASE_OPERATION_KIND;
    },
  ],
  [
    "hostile producer operation id",
    (receipt) => {
      receipt.operation.expectedSession.document.lastOperation.operationId =
        "different-activation-operation-001";
      receipt.session.document.lastOperation.operationId =
        "different-activation-operation-001";
    },
  ],
  [
    "hostile atomic creation timestamps",
    (receipt) => {
      receipt.operation.createdAt = BASE_TIME;
      receipt.reservation.createdAt = BASE_TIME;
    },
  ],
  [
    "hostile co-mutated atomic update timestamps",
    (receipt) => {
      receipt.operation.updatedAt = STARTING_TIME;
      receipt.reservation.updatedAt = STARTING_TIME;
      receipt.session.updatedAt = STARTING_TIME;
    },
  ],
  [
    "generation committed after activation",
    (receipt) => {
      receipt.operation.request.generation.committedAt = STARTING_TIME;
      receipt.attempt.request.generation.committedAt = STARTING_TIME;
    },
  ],
]) {
  test(`rejects activation prepared relation with ${name} before image or provider use`, async () => {
    const value = await fixture();
    seedPreparedActivationLaunchHandoff(value);
    value.authority.readReceiptMutation = (receipt) => {
      mutateReceipt(receipt);
      return receipt;
    };

    await assert.rejects(
      value.facade.runPreparedLaunch(preparedRunInput(value)),
      assertLauncherError("logical_writer_launch_outcome_uncertain"),
    );
    assert.equal(value.authority.state, "prepared");
    assert.equal(value.authority.calls.reserve, 0);
    assert.equal(value.authority.calls.claim, 0);
    assert.equal(value.authority.calls.cancel, 0);
    assert.equal(value.launchCalls, 0);
    assert.equal(value.reconcileCalls, 0);
    assert.equal(value.inspectionCount, 1);
  });
}

test("activation claim acknowledgement loss reconciles without image consumption or relaunch", async () => {
  const value = await fixture({ reconcileStatus: "not-started" });
  seedPreparedActivationLaunchHandoff(value);
  value.authority.behaviour.claimThrowAfterCommit = true;

  const result = await value.facade.runPreparedLaunch(
    preparedRunInput(value),
  );
  const replay = await value.facade.runPreparedLaunch(
    preparedRunInput(value),
  );

  assert.equal(result.status, "not-started");
  assert.equal(replay.status, "not-started");
  assert.equal(value.authority.calls.reserve, 0);
  assert.equal(value.authority.calls.claim, 1);
  assert.equal(value.authority.calls.markUncertain, 1);
  assert.equal(value.authority.calls.finalizeStopped, 1);
  assert.equal(value.launchCalls, 0);
  assert.equal(value.reconcileCalls, 1);
  assert.equal(value.inspectionCount, 2);
});

test("cold launcher resumes a prepared attempt with a fresh equivalent image reservation", async () => {
  const value = await fixture();
  const intent = await value.facade.prepareLaunchIntent(
    prepareIntentInput(value),
  );
  seedPreparedLaunchHandoff(value, {
    measuredImage: intent.measuredImage,
    supervisor: intent.supervisor,
  });

  const freshImagePlanBinding = createTestImagePlanBinding(
    value.imagePlanProvider,
  );
  const freshImageReservation =
    await freshImagePlanBinding.prepareImageReservation(
    objectFreeze({
      plan: value.imagePlan,
      sessionManifest: value.image.manifest,
    }),
  );
  const coldFacade = createPostgresLogicalWriterLauncher({
    authority: value.authority,
    imagePlanBinding: freshImagePlanBinding,
    operationGuard: new MemoryOperationGuard(value.events),
    stoppedWriterCoordinator: new StoppedWriterCapabilityCoordinator(),
    supervisor: value.supervisor,
  });
  value.events.length = 0;

  const result = await coldFacade.runPreparedLaunch({
    imageReservation: freshImageReservation,
    launchAttemptId: LAUNCH_ATTEMPT_ID,
  });

  assert.equal(result.status, "started");
  assert.equal(result.writer !== null, true);
  assert.equal(value.authority.calls.reserve, 0);
  assert.equal(value.authority.calls.claim, 1);
  assert.equal(value.authority.calls.finalizeStarted, 1);
  assert.equal(value.launchCalls, 1);
  assert.equal(value.reconcileCalls, 0);
  assert.equal(value.inspectionCount, 5);

  const originalStillIssued =
    await value.imagePlanBinding.revalidateImageReservation(
    value.imageReservation,
  );
  assert.equal(Object.isFrozen(originalStillIssued), true);
  assert.equal(value.inspectionCount, 6);
});

test("prepared image revalidation failure leaves the attempt retryable with a fresh reservation", async () => {
  const value = await fixture();
  seedPreparedLaunchHandoff(value);
  value.failInspectionAt(2);

  await assert.rejects(
    value.facade.runPreparedLaunch(preparedRunInput(value)),
    assertLauncherError("logical_writer_launch_admission_unavailable", true),
  );
  assert.equal(value.authority.state, "prepared");
  assert.equal(value.authority.calls.reserve, 0);
  assert.equal(value.authority.calls.claim, 0);
  assert.equal(value.authority.calls.cancel, 0);
  assert.equal(value.launchCalls, 0);
  assert.equal(value.reconcileCalls, 0);
  assert.equal(value.inspectionCount, 2);

  value.failInspectionAt(null);
  const fresh = await value.imagePlanBinding.prepareImageReservation(
    objectFreeze({
      plan: value.imagePlan,
      sessionManifest: value.image.manifest,
    }),
  );
  const retried = await value.facade.runPreparedLaunch(
    preparedRunInput(value, {
      imageReservation: fresh,
    }),
  );
  assert.equal(retried.status, "started");
  assert.equal(value.authority.calls.reserve, 0);
  assert.equal(value.authority.calls.claim, 1);
  assert.equal(value.launchCalls, 1);
  assert.equal(value.inspectionCount, 5);
});

test("rejects a non-matching opaque image before claim, consumption, or cancellation", async () => {
  const value = await fixture();
  const mismatchedMeasurement = clone({
    projection: value.reserved.projection,
    runtimeIdentity: value.reserved.runtimeIdentity,
  });
  mismatchedMeasurement.runtimeIdentity.codexBinarySha256 = "c".repeat(64);
  seedPreparedLaunchHandoff(value, {
    measuredImage: mismatchedMeasurement,
  });

  await assert.rejects(
    value.facade.runPreparedLaunch(preparedRunInput(value)),
    assertLauncherError("invalid_logical_writer_launch_request"),
  );
  assert.equal(value.authority.state, "prepared");
  assert.equal(value.authority.calls.reserve, 0);
  assert.equal(value.authority.calls.claim, 0);
  assert.equal(value.authority.calls.cancel, 0);
  assert.equal(value.launchCalls, 0);
  assert.equal(value.reconcileCalls, 0);
  assert.equal(value.inspectionCount, 2);

  const stillIssued = await value.imagePlanBinding.revalidateImageReservation(
    value.imageReservation,
  );
  assert.equal(Object.isFrozen(stillIssued), true);
  assert.equal(value.inspectionCount, 3);

  await assert.rejects(
    value.facade.reconcileLaunchAttempt({
      launchAttemptId: LAUNCH_ATTEMPT_ID,
    }),
    assertLauncherError("logical_writer_launch_outcome_uncertain"),
  );
  assert.equal(value.authority.state, "prepared");
  assert.equal(value.authority.calls.cancel, 1);
  assert.equal(value.launchCalls, 0);
});

test("rejects a prepared supervisor mismatch before image use or claim", async () => {
  const value = await fixture();
  seedPreparedLaunchHandoff(value, {
    supervisor: {
      contractVersion: LOGICAL_WRITER_LAUNCH_CONTRACT_VERSION,
      supervisorId: "supervisor-mismatch",
    },
  });

  await assert.rejects(
    value.facade.runPreparedLaunch(preparedRunInput(value)),
    assertLauncherError("invalid_logical_writer_launch_request"),
  );
  assert.equal(value.authority.state, "prepared");
  assert.equal(value.authority.calls.reserve, 0);
  assert.equal(value.authority.calls.claim, 0);
  assert.equal(value.authority.calls.cancel, 0);
  assert.equal(value.launchCalls, 0);
  assert.equal(value.reconcileCalls, 0);
  assert.equal(value.inspectionCount, 1);

  await value.imagePlanBinding.revalidateImageReservation(
    value.imageReservation,
  );
  assert.equal(value.inspectionCount, 2);
});

test("rejects a prepared session mismatch before claim and permits exact retry", async () => {
  const value = await fixture();
  seedPreparedLaunchHandoff(value);
  value.authority.sessionRevisionOverride = "13";

  await assert.rejects(
    value.facade.runPreparedLaunch(preparedRunInput(value)),
    assertLauncherError("logical_writer_launch_outcome_uncertain"),
  );
  assert.equal(value.authority.state, "prepared");
  assert.equal(value.authority.calls.reserve, 0);
  assert.equal(value.authority.calls.claim, 0);
  assert.equal(value.authority.calls.cancel, 0);
  assert.equal(value.launchCalls, 0);
  assert.equal(value.reconcileCalls, 0);
  assert.equal(value.inspectionCount, 1);

  value.authority.sessionRevisionOverride = undefined;
  const retried = await value.facade.runPreparedLaunch(
    preparedRunInput(value),
  );
  assert.equal(retried.status, "started");
  assert.equal(value.authority.calls.reserve, 0);
  assert.equal(value.authority.calls.claim, 1);
  assert.equal(value.launchCalls, 1);
  assert.equal(value.inspectionCount, 3);
});

test("claim acknowledgement loss reads back and reconciles without consuming", async () => {
  const value = await fixture({ reconcileStatus: "not-started" });
  seedPreparedLaunchHandoff(value);
  value.authority.behaviour.claimThrowAfterCommit = true;

  const result = await value.facade.runPreparedLaunch(preparedRunInput(value));
  assert.equal(result.status, "not-started");
  assert.equal(result.writer, null);
  assert.equal(value.authority.calls.reserve, 0);
  assert.equal(value.authority.calls.claim, 1);
  assert.equal(value.authority.calls.cancel, 0);
  assert.equal(value.authority.calls.markUncertain, 1);
  assert.equal(value.authority.calls.finalizeStopped, 1);
  assert.equal(value.launchCalls, 0);
  assert.equal(value.reconcileCalls, 1);
  assert.equal(value.inspectionCount, 2);

  await value.imagePlanBinding.revalidateImageReservation(
    value.imageReservation,
  );
  assert.equal(value.inspectionCount, 3);
});

test("claim failure before commit leaves prepared state and image reservation retryable", async () => {
  const value = await fixture();
  seedPreparedLaunchHandoff(value);
  value.authority.behaviour.claimThrowBeforeCommit = true;
  value.events.length = 0;

  await assert.rejects(
    value.facade.runPreparedLaunch(preparedRunInput(value)),
    assertLauncherError("logical_writer_launch_admission_unavailable", true),
  );
  assert.equal(value.authority.state, "prepared");
  assert.equal(value.authority.calls.reserve, 0);
  assert.equal(value.authority.calls.claim, 1);
  assert.equal(value.authority.calls.cancel, 0);
  assert.equal(value.authority.calls.markUncertain, 0);
  assert.equal(value.launchCalls, 0);
  assert.equal(value.reconcileCalls, 0);
  assert.equal(value.inspectionCount, 2);
  assert.deepEqual(value.events, [
    `guard.enter:${LAUNCH_ATTEMPT_ID}`,
    "authority.read-attempt",
    "guard.assert-held",
    "image.inspect:2",
    "guard.assert-held",
    "authority.claim",
    "authority.read-attempt",
    "guard.assert-held",
    `guard.exit:${LAUNCH_ATTEMPT_ID}`,
  ]);

  value.authority.behaviour.claimThrowBeforeCommit = false;
  const retried = await value.facade.runPreparedLaunch(
    preparedRunInput(value),
  );
  assert.equal(retried.status, "started");
  assert.equal(value.authority.calls.reserve, 0);
  assert.equal(value.authority.calls.claim, 2);
  assert.equal(value.authority.calls.cancel, 0);
  assert.equal(value.launchCalls, 1);
  assert.equal(value.inspectionCount, 4);
});

test("does not consume the image before the durable launch state becomes starting", async () => {
  const value = await fixture();
  seedPreparedLaunchHandoff(value);
  value.authority.behaviour.claimThrowBeforeCommit = true;

  await assert.rejects(
    value.facade.runPreparedLaunch(preparedRunInput(value)),
    assertLauncherError("logical_writer_launch_admission_unavailable", true),
  );
  assert.equal(value.authority.state, "prepared");
  assert.equal(value.inspectionCount, 2);

  const stillIssued =
    await value.imagePlanBinding.revalidateImageReservation(
      value.imageReservation,
    );
  assert.equal(Object.isFrozen(stillIssued), true);
  assert.equal(value.inspectionCount, 3);

  value.authority.behaviour.claimThrowBeforeCommit = false;
  const started = await value.facade.runPreparedLaunch(
    preparedRunInput(value),
  );
  assert.equal(started.status, "started");
  assert.equal(value.authority.state, "committed");
  assert.equal(value.inspectionCount, 5);
  await assert.rejects(
    value.imagePlanBinding.revalidateImageReservation(
      value.imageReservation,
    ),
    (error) =>
      error?.code ===
      "postgres_detached_restore_image_plan_reservation_rejected",
  );
  assert.equal(value.inspectionCount, 5);
});

test("prepared launcher rejects a hostile authority read receipt before image use or claim", async () => {
  const value = await fixture();
  seedPreparedLaunchHandoff(value);
  let traps = 0;
  value.authority.readReceiptMutation = () =>
    new Proxy(
      {},
      {
        ownKeys() {
          traps += 1;
          throw new Error("hostile authority receipt");
        },
      },
    );

  await assert.rejects(
    value.facade.runPreparedLaunch(preparedRunInput(value)),
    assertLauncherError("logical_writer_launch_outcome_uncertain"),
  );
  assert.equal(traps, 0);
  assert.equal(value.authority.state, "prepared");
  assert.equal(value.authority.calls.read, 1);
  assert.equal(value.authority.calls.reserve, 0);
  assert.equal(value.authority.calls.claim, 0);
  assert.equal(value.authority.calls.cancel, 0);
  assert.equal(value.launchCalls, 0);
  assert.equal(value.reconcileCalls, 0);
  assert.equal(value.inspectionCount, 1);
});

test("hostile prepared-claim receipt uses durable readback without consuming or launching", async () => {
  const value = await fixture({ reconcileStatus: "not-started" });
  seedPreparedLaunchHandoff(value);
  let traps = 0;
  value.authority.claimReceiptMutation = () =>
    new Proxy(
      {},
      {
        ownKeys() {
          traps += 1;
          throw new Error("hostile claim receipt");
        },
      },
    );

  const result = await value.facade.runPreparedLaunch(
    preparedRunInput(value),
  );
  assert.equal(result.status, "not-started");
  assert.equal(result.writer, null);
  assert.equal(traps, 0);
  assert.equal(value.authority.calls.reserve, 0);
  assert.equal(value.authority.calls.claim, 1);
  assert.equal(value.authority.calls.cancel, 0);
  assert.equal(value.authority.calls.markUncertain, 1);
  assert.equal(value.authority.calls.finalizeStopped, 1);
  assert.equal(value.launchCalls, 0);
  assert.equal(value.reconcileCalls, 1);
  assert.equal(value.inspectionCount, 2);

  await value.imagePlanBinding.revalidateImageReservation(
    value.imageReservation,
  );
  assert.equal(value.inspectionCount, 3);
});

for (const [name, mutateClaimReceipt] of [
  [
    "changed current lifecycle",
    (receipt) => {
      receipt.session.document.lifecycle = "RELEASING";
    },
  ],
  [
    "co-mutated expected and current session content",
    (receipt) => {
      receipt.operation.expectedSession.document.backendCapabilities.fencing =
        "automatic";
      receipt.session.document.backendCapabilities.fencing = "automatic";
    },
  ],
  [
    "authority clock at lease expiry",
    (receipt) => {
      receipt.authorityNow = receipt.operation.request.lease.expiresAt;
    },
  ],
]) {
  test(`hostile prepared claim with ${name} never consumes or launches`, async () => {
    const value = await fixture({ reconcileStatus: "not-started" });
    seedPreparedLaunchHandoff(value);
    value.authority.claimReceiptMutation = (receipt) => {
      mutateClaimReceipt(receipt);
      return receipt;
    };

    const result = await value.facade.runPreparedLaunch(
      preparedRunInput(value),
    );
    assert.equal(result.status, "not-started");
    assert.equal(result.writer, null);
    assert.equal(value.authority.calls.reserve, 0);
    assert.equal(value.authority.calls.claim, 1);
    assert.equal(value.authority.calls.cancel, 0);
    assert.equal(value.authority.calls.markUncertain, 1);
    assert.equal(value.authority.calls.finalizeStopped, 1);
    assert.equal(value.launchCalls, 0);
    assert.equal(value.reconcileCalls, 1);
    assert.equal(value.inspectionCount, 2);

    await value.imagePlanBinding.revalidateImageReservation(
      value.imageReservation,
    );
    assert.equal(value.inspectionCount, 3);
  });
}

for (const state of ["starting", "uncertain"]) {
  test(`prepared launcher reconciles durable ${state} without relaunch or image use`, async () => {
    const value = await fixture({ reconcileStatus: "complete-stopped" });
    seedPreparedLaunchHandoff(value, { state });

    const result = await value.facade.runPreparedLaunch(
      preparedRunInput(value),
    );
    assert.equal(result.status, "complete-stopped");
    assert.equal(result.writer, null);
    assert.equal(value.authority.calls.reserve, 0);
    assert.equal(value.authority.calls.claim, 0);
    assert.equal(value.authority.calls.cancel, 0);
    assert.equal(
      value.authority.calls.markUncertain,
      state === "starting" ? 1 : 0,
    );
    assert.equal(value.launchCalls, 0);
    assert.equal(value.reconcileCalls, 1);
    assert.equal(value.inspectionCount, 1);
  });
}

for (const [status, launchEvidence] of [
  ["cancelled-before-dispatch", null],
  ["not-started", evidence(LAUNCH_ATTEMPT_ID, "not-started")],
  ["complete-stopped", evidence(LAUNCH_ATTEMPT_ID, "complete-stopped")],
]) {
  test(`prepared launcher returns committed ${status} without image or supervisor use`, async () => {
    const value = await fixture();
    seedPreparedLaunchHandoff(value, {
      launchEvidence,
      state: "committed",
    });

    const result = await value.facade.runPreparedLaunch(
      preparedRunInput(value),
    );
    assert.equal(result.status, status);
    assert.equal(result.writer, null);
    assert.equal(value.authority.calls.reserve, 0);
    assert.equal(value.authority.calls.claim, 0);
    assert.equal(value.authority.calls.cancel, 0);
    assert.equal(value.authority.calls.markUncertain, 0);
    assert.equal(value.launchCalls, 0);
    assert.equal(value.reconcileCalls, 0);
    assert.equal(value.inspectionCount, 1);
  });
}

test("cold committed-started prepared replay never relaunches without its local handle", async () => {
  const value = await fixture();
  seedPreparedLaunchHandoff(value, {
    launchEvidence: evidence(LAUNCH_ATTEMPT_ID, "started"),
    state: "committed",
  });

  await assert.rejects(
    value.facade.runPreparedLaunch(preparedRunInput(value)),
    assertLauncherError("logical_writer_handle_unavailable"),
  );
  assert.equal(value.authority.calls.reserve, 0);
  assert.equal(value.authority.calls.claim, 0);
  assert.equal(value.authority.calls.cancel, 0);
  assert.equal(value.authority.calls.markUncertain, 0);
  assert.equal(value.launchCalls, 0);
  assert.equal(value.reconcileCalls, 0);
  assert.equal(value.inspectionCount, 1);
});

test("committed prepared replay returns the original local writer without relaunch", async () => {
  const value = await fixture();
  seedPreparedLaunchHandoff(value);
  const started = await value.facade.runPreparedLaunch(
    preparedRunInput(value),
  );
  const inspectionsAfterStart = value.inspectionCount;

  const replayed = await value.facade.runPreparedLaunch(
    preparedRunInput(value),
  );
  assert.equal(replayed.status, "started");
  assert.strictEqual(replayed.writer, started.writer);
  assert.equal(value.authority.calls.reserve, 0);
  assert.equal(value.authority.calls.claim, 1);
  assert.equal(value.authority.calls.finalizeStarted, 1);
  assert.equal(value.launchCalls, 1);
  assert.equal(value.reconcileCalls, 0);
  assert.equal(value.inspectionCount, inspectionsAfterStart);
});

test("rejects active prepared-handoff recovery bound to another supervisor", async () => {
  const value = await fixture({ reconcileStatus: "complete-stopped" });
  seedPreparedLaunchHandoff(value, {
    state: "starting",
    supervisor: {
      contractVersion: LOGICAL_WRITER_LAUNCH_CONTRACT_VERSION,
      supervisorId: "different-supervisor-001",
    },
  });

  await assert.rejects(
    value.facade.runPreparedLaunch(preparedRunInput(value)),
    assertLauncherError("invalid_logical_writer_launch_request"),
  );
  assert.equal(value.authority.state, "starting");
  assert.equal(value.authority.calls.reserve, 0);
  assert.equal(value.authority.calls.claim, 0);
  assert.equal(value.authority.calls.cancel, 0);
  assert.equal(value.authority.calls.markUncertain, 0);
  assert.equal(value.launchCalls, 0);
  assert.equal(value.reconcileCalls, 0);
  assert.equal(value.inspectionCount, 1);
});

test("does not infer stopped from historical started state with no current launch", async () => {
  const value = await fixture();
  seedPreparedLaunchHandoff(value, {
    launchEvidence: evidence(LAUNCH_ATTEMPT_ID, "started"),
    state: "committed",
  });
  value.authority.launchPointerMutation = () => null;

  await assert.rejects(
    value.facade.runPreparedLaunch(preparedRunInput(value)),
    assertLauncherError("logical_writer_launch_outcome_uncertain"),
  );
  assert.equal(value.authority.calls.reserve, 0);
  assert.equal(value.authority.calls.claim, 0);
  assert.equal(value.authority.calls.cancel, 0);
  assert.equal(value.launchCalls, 0);
  assert.equal(value.reconcileCalls, 0);
  assert.equal(value.inspectionCount, 1);
});

test("ordinary launch reconciliation remains closed after a joined durable stop", async () => {
  const value = await fixture();
  await value.facade.runLaunch(runInput(value));
  const stopped = await value.facade.stopWriterForCapture(
    resolverInput(value),
  );

  assert.equal(stopped.stop.status, "committed");
  assert.equal(stopped.evidence.status, "complete-stopped");
  assert.equal(value.supervisorStopCalls, 1);

  await assert.rejects(
    value.facade.reconcileLaunchAttempt({
      launchAttemptId: LAUNCH_ATTEMPT_ID,
    }),
    assertLauncherError("logical_writer_launch_outcome_uncertain"),
  );
  assert.equal(value.authority.calls.read, 1);
  assert.equal(value.reconcileCalls, 0);
  assert.equal(value.supervisorStopCalls, 1);
});

test("serializes concurrent prepared claims on the launch-attempt guard key", async () => {
  const value = await fixture();
  seedPreparedLaunchHandoff(value);
  value.events.length = 0;

  const [first, second] = await Promise.all([
    value.facade.runPreparedLaunch(preparedRunInput(value)),
    value.facade.runPreparedLaunch(preparedRunInput(value)),
  ]);

  assert.equal(first.status, "started");
  assert.equal(second.status, "started");
  assert.strictEqual(first.writer, second.writer);
  assert.equal(value.authority.calls.reserve, 0);
  assert.equal(value.authority.calls.claim, 1);
  assert.equal(value.authority.calls.finalizeStarted, 1);
  assert.equal(value.launchCalls, 1);
  assert.equal(value.reconcileCalls, 0);
  assert.equal(value.inspectionCount, 3);
  assert.deepEqual(
    value.events.filter((entry) =>
      entry.startsWith(`guard.enter:${LAUNCH_ATTEMPT_ID}`),
    ),
    [
      `guard.enter:${LAUNCH_ATTEMPT_ID}`,
      `guard.enter:${LAUNCH_ATTEMPT_ID}`,
    ],
  );
});

test("releases stopped launch indexes across repeated writer lifecycles", async () => {
  const value = await fixture();
  const cycleCount = 3;

  for (let index = 0; index < cycleCount; index += 1) {
    const cycle = await prepareLaunchCycle(value, index);

    const result = await value.facade.runLaunch(
      runInput(value, {
        generation: cycle.generation,
        imageReservation: cycle.imageReservation,
        launchAttemptId: cycle.launchAttemptId,
      }),
    );
    assert.equal(result.status, "started");
    assert.equal(
      result.attempt.request.attachment.attachmentId,
      cycle.mounted.attachmentId,
    );
    assert.equal(
      result.attempt.request.lease.fencingEpoch,
      cycle.writerLease.fencingEpoch,
    );

    const stopped = await value.facade.stopWriterForCapture(
      cycleResolverInput(value, cycle),
    );
    const resolved = stopped.resolution;
    await value.stoppedWriterCoordinator.consumeCapability({
      attachment: cycle.mounted,
      canonicalLease: cycle.writerLease,
      capability: stopped.capability,
      processIncarnationId: resolved.processIncarnationId,
      runSnapshot: async () => `captured-${cycle.ordinal}`,
      stopOperationId: resolved.stopOperationId,
      writer: resolved.writer,
      writerIncarnationId: resolved.writerIncarnationId,
    });
    value.facade.retireStoppedWriter(resolved);
  }

  assert.equal(value.launchCalls, cycleCount);
  assert.equal(value.supervisorStopCalls, cycleCount);
});

test("retirement requires the exact stopped resolution and releases both indexes", async () => {
  const value = await fixture();
  await value.facade.runLaunch(runInput(value));
  const stopped = await value.facade.stopWriterForCapture(
    resolverInput(value),
  );
  const resolution = stopped.resolution;

  for (const candidate of [
    {
      ...resolution,
      processIncarnationId: "process-incarnation-mismatch",
    },
    {
      ...resolution,
      stopOperationId: `writer-stop:${"f".repeat(64)}`,
    },
    {
      ...resolution,
      writer: Object.freeze(Object.create(null)),
    },
    {
      ...resolution,
      writerIncarnationId: "writer-incarnation-mismatch",
    },
  ]) {
    assert.throws(
      () => value.facade.retireStoppedWriter(candidate),
      assertLauncherError("invalid_logical_writer_launch_request"),
    );
  }

  await value.stoppedWriterCoordinator.consumeCapability({
    attachment: attachment(),
    canonicalLease: lease(),
    capability: stopped.capability,
    processIncarnationId: resolution.processIncarnationId,
    runSnapshot: async () => "captured",
    stopOperationId: resolution.stopOperationId,
    writer: resolution.writer,
    writerIncarnationId: resolution.writerIncarnationId,
  });
  assert.equal(value.facade.retireStoppedWriter(resolution), undefined);
  assert.throws(
    () => value.facade.resolveStoppedWriter(resolverInput(value)),
    assertLauncherError("invalid_logical_writer_launch_request"),
  );
});

test("real logical writer launcher composes one v2 stop, capture, and retirement", async () => {
  const value = await fixture();
  await value.facade.runLaunch(runInput(value));
  const captureTuple = resolverInput(value);

  let stoppedResult = null;
  let stoppedPromise = null;
  let retireCalls = 0;
  const stopWriterForCapture = function (input) {
    stoppedPromise = value.facade.stopWriterForCapture(input);
    return stoppedPromise;
  };
  const retireStoppedWriter = function (input) {
    retireCalls += 1;
    return value.facade.retireStoppedWriter(input);
  };
  Object.freeze(stopWriterForCapture);
  Object.freeze(retireStoppedWriter);
  const launcher = Object.freeze({
    retireStoppedWriter,
    stopWriterForCapture,
  });

  const captureCalls = [];
  const operation = async () => undefined;
  const backend = {
    contractVersion: 1,
    backendId: BACKEND_ID,
    capabilities: backendCapabilities(),
    async captureCheckpoint(input) {
      captureCalls.push(input);
      const resolution = value.facade.resolveStoppedWriter({
        attachment: input.attachment,
        checkpoint: input.checkpoint,
        request: input.request,
      });
      return value.stoppedWriterCoordinator.consumeCapability({
        attachment: input.attachment,
        canonicalLease: resolution.canonicalLeaseAtRegistration,
        capability: input.stoppedWriterEvidence,
        processIncarnationId: resolution.processIncarnationId,
        runSnapshot: async () => ({
          checkpoint: input.checkpoint,
          mutation: {
            ...input.request,
            proofId: "proof-checkpoint-capture-001",
            status: "checkpoint-created",
          },
        }),
        stopOperationId: resolution.stopOperationId,
        writer: resolution.writer,
        writerIncarnationId: resolution.writerIncarnationId,
      });
    },
    destroySession: operation,
    detachAttachment: operation,
    forceFence: operation,
    prepareWritableAttachment: operation,
    provisionSession: operation,
    restoreCheckpoint: operation,
  };

  const composition = createPostgresDurableStopCaptureComposition({ launcher });
  const result = await composition.runCapture({
    attachment: captureTuple.attachment,
    backend,
    canonicalLease: lease(),
    checkpointClass: captureTuple.checkpoint.checkpointClass,
    createdAt: captureTuple.checkpoint.createdAt,
    manifest: value.image.manifest,
    now: Date.parse(BASE_TIME),
    request: captureTuple.request,
    storageRef: storageRef(),
  });
  stoppedResult = await Reflect.apply(Promise.prototype.then, stoppedPromise, [
    (value) => value,
  ]);

  assert.equal(stoppedResult.stop.stop.contractVersion, 2);
  for (const request of [
    stoppedResult.stop.stop.request,
    stoppedResult.stop.operation.request,
  ]) {
    assert.equal(request.contractVersion, 2);
    assert.match(request.dispatchClaimSha256, /^[0-9a-f]{64}$/u);
  }
  assert.deepEqual(
    stoppedResult.stop.operation.request,
    stoppedResult.stop.stop.request,
  );
  assert.equal(captureCalls.length, 1);
  assert.strictEqual(
    captureCalls[0].stoppedWriterEvidence,
    stoppedResult.capability,
  );
  assert.equal(retireCalls, 1);
  assert.equal(value.supervisorStopCalls, 1);
  assert.equal(value.authority.calls.finalizeWriterStopped, 1);
  assert.deepEqual(result, {
    checkpoint: captureTuple.checkpoint,
    mutation: {
      ...captureTuple.request,
      proofId: "proof-checkpoint-capture-001",
      status: "checkpoint-created",
    },
  });
  assert.throws(
    () => value.facade.resolveStoppedWriter(captureTuple),
    assertLauncherError("invalid_logical_writer_launch_request"),
  );
  assert.equal(value.stoppedWriterCoordinator.dispose(), undefined);
});

test("keeps a failed stop fail-closed for successor launch registration", async () => {
  const value = await fixture({ supervisorStopThrows: true });
  const first = await prepareLaunchCycle(value, 0);
  const started = await value.facade.runLaunch(
    runInput(value, {
      generation: first.generation,
      imageReservation: first.imageReservation,
      launchAttemptId: first.launchAttemptId,
    }),
  );
  assert.equal(started.status, "started");
  await assert.rejects(
    value.facade.stopWriterForCapture(cycleResolverInput(value, first)),
    assertLauncherError("logical_writer_launch_outcome_uncertain"),
  );
  assert.equal(value.supervisorStopCalls, 1);
  assert.equal(value.authority.stopState, "uncertain");
  assert.equal(value.authority.calls.markUncertain, 1);
  assert.throws(
    () => value.facade.resolveStoppedWriter(cycleResolverInput(value, first)),
    assertLauncherError("invalid_logical_writer_launch_request"),
  );

  const successor = await prepareLaunchCycle(value, 1);
  await assert.rejects(
    value.facade.runLaunch(
      runInput(value, {
        generation: successor.generation,
        imageReservation: successor.imageReservation,
        launchAttemptId: successor.launchAttemptId,
      }),
    ),
    assertLauncherError("logical_writer_launch_admission_unavailable", true),
  );
  assert.equal(value.launchCalls, 1);
  assert.equal(value.authority.state, "absent");
  assert.equal(value.authority.calls.finalizeStarted, 1);
  assert.equal(value.authority.calls.markUncertain, 1);
});

test("shared launch exclusion spans concurrent capture through explicit retirement", async () => {
  const stoppedWriterCoordinator = new StoppedWriterCapabilityCoordinator();
  const first = await fixture({ stoppedWriterCoordinator });
  await first.facade.runLaunch(runInput(first));
  const stopped = await first.facade.stopWriterForCapture(resolverInput(first));

  const successor = await fixture({ stoppedWriterCoordinator });
  const next = await prepareLaunchCycle(successor, 1, {
    attachmentOverrides: {
      attachmentId: "attachment-alternate-storage",
      backendId: "alternate-backend",
      operationId: "operation-attach-alternate-storage",
      proofId: "proof-attachment-alternate-storage",
      rootPath: "/var/lib/portable-codex/alternate-storage",
      storageId: "volume-alternate",
    },
  });
  const nextInput = runInput(successor, {
    generation: next.generation,
    imageReservation: next.imageReservation,
    launchAttemptId: next.launchAttemptId,
  });
  await assert.rejects(
    successor.facade.runLaunch(nextInput),
    assertLauncherError("logical_writer_launch_admission_unavailable", true),
  );
  assert.equal(successor.authority.calls.reserve, 0);
  assert.equal(successor.authority.calls.claim, 0);
  assert.equal(successor.launchCalls, 0);

  let captureStarted;
  const captureStartedPromise = new Promise((resolve) => {
    captureStarted = resolve;
  });
  let finishCapture;
  const finishCapturePromise = new Promise((resolve) => {
    finishCapture = resolve;
  });
  const resolution = stopped.resolution;
  const consuming = stoppedWriterCoordinator.consumeCapability({
    attachment: attachment(),
    canonicalLease: lease(),
    capability: stopped.capability,
    processIncarnationId: resolution.processIncarnationId,
    runSnapshot: async () => {
      captureStarted();
      await finishCapturePromise;
      return "captured";
    },
    stopOperationId: resolution.stopOperationId,
    writer: resolution.writer,
    writerIncarnationId: resolution.writerIncarnationId,
  });
  await captureStartedPromise;
  await assert.rejects(
    successor.facade.runLaunch(nextInput),
    assertLauncherError("logical_writer_launch_admission_unavailable", true),
  );
  assert.equal(successor.authority.calls.reserve, 0);
  assert.equal(successor.launchCalls, 0);

  finishCapture();
  assert.equal(await consuming, "captured");
  await assert.rejects(
    successor.facade.runLaunch(nextInput),
    assertLauncherError("logical_writer_launch_admission_unavailable", true),
  );
  assert.equal(successor.authority.calls.reserve, 0);
  assert.equal(successor.launchCalls, 0);

  first.facade.retireStoppedWriter(resolution);
  const launched = await successor.facade.runLaunch(nextInput);
  assert.equal(launched.status, "started");
  assert.equal(successor.authority.calls.reserve, 1);
  assert.equal(successor.authority.calls.claim, 1);
  assert.equal(successor.launchCalls, 1);
});

test("shared session exclusion blocks prepared launch on another storage domain", async () => {
  const stoppedWriterCoordinator = new StoppedWriterCapabilityCoordinator();
  const blockerLease = lease();
  stoppedWriterCoordinator.registerWriter({
    attachment: attachment(blockerLease, {
      attachmentId: "attachment-blocker",
      backendId: "blocker-backend",
      operationId: "operation-attach-blocker",
      proofId: "proof-attachment-blocker",
      rootPath: "/var/lib/portable-codex/blocker",
      storageId: "volume-blocker",
    }),
    canonicalLease: blockerLease,
    processIncarnationId: "process-incarnation-blocker",
    stopWriter: async () => STOPPED_WRITER_STOP_CONFIRMED,
    writerIncarnationId: "writer-incarnation-blocker",
  });
  const value = await fixture({ stoppedWriterCoordinator });
  seedPreparedLaunchHandoff(value);
  const inspectionsBefore = value.inspectionCount;

  await assert.rejects(
    value.facade.runPreparedLaunch(preparedRunInput(value)),
    assertLauncherError("logical_writer_launch_admission_unavailable", true),
  );
  assert.equal(value.authority.calls.claim, 0);
  assert.equal(value.launchCalls, 0);
  assert.equal(value.inspectionCount, inspectionsBefore);
});

test("stop operation identity binds every canonical capture tuple member", async () => {
  const value = await fixture();
  const capture = resolverInput(value);
  const base = derivePostgresLogicalWriterStopOperationId({
    ...capture,
    launchAttemptId: LAUNCH_ATTEMPT_ID,
  });
  const changedCheckpoint = derivePostgresLogicalWriterStopOperationId({
    ...capture,
    checkpoint: {
      ...capture.checkpoint,
      createdAt: "2026-08-04T11:00:01.000Z",
    },
    launchAttemptId: LAUNCH_ATTEMPT_ID,
  });
  const changedRequest = derivePostgresLogicalWriterStopOperationId({
    ...capture,
    launchAttemptId: LAUNCH_ATTEMPT_ID,
    request: {
      ...capture.request,
      operationId: "checkpoint-capture-operation-002",
    },
  });

  assert.match(base, /^writer-stop:[0-9a-f]{64}$/u);
  assert.notEqual(changedCheckpoint, base);
  assert.notEqual(changedRequest, base);
  assert.notEqual(changedCheckpoint, changedRequest);

  const revoked = Proxy.revocable(
    { ...capture, launchAttemptId: LAUNCH_ATTEMPT_ID },
    {},
  );
  revoked.revoke();
  assert.throws(
    () => derivePostgresLogicalWriterStopOperationId(revoked.proxy),
    assertLauncherError("invalid_logical_writer_launch_request"),
  );
});

test("keeps atomic assembly owner-only and exposes only the safe composition", async () => {
  const value = await fixture();
  const request = atomicCrashCaptureRequest(value);
  const collaborators = atomicCompositionCollaborators(request);
  const composition = createPostgresLvmAtomicCrashCaptureComposition({
    atomicCrashCaptureAssembler: value.atomicCrashCaptureAssembler,
    baseBackend: collaborators.baseBackend,
    catalogue: collaborators.catalogue,
    driver: collaborators.driver,
  });

  assert.equal(
    Object.hasOwn(
      postgresLogicalWriterLauncherModule,
      "getPostgresLogicalWriterAtomicCrashCaptureFacet",
    ),
    false,
  );
  assert.equal(
    Object.hasOwn(
      postgresLogicalWriterLauncherModule,
      "createPostgresLvmAtomicCrashCaptureCompositionInternal",
    ),
    false,
  );
  assert.strictEqual(
    postgresLogicalWriterLauncherModule.createPostgresLvmAtomicCrashCaptureComposition,
    createPostgresLvmAtomicCrashCaptureComposition,
  );
  assert.deepEqual(
    Reflect.ownKeys(value.atomicCrashCaptureOwner).sort(),
    ["atomicCrashCaptureAssembler", "launcher"],
  );
  assert.equal(Object.getPrototypeOf(value.atomicCrashCaptureOwner), null);
  assert.equal(Object.isFrozen(value.atomicCrashCaptureOwner), true);
  assert.strictEqual(value.atomicCrashCaptureOwner.launcher, value.facade);
  assert.deepEqual(Reflect.ownKeys(composition).sort(), [
    "reconcileCapture",
    "runCapture",
  ]);
  assert.equal(Object.getPrototypeOf(composition), null);
  assert.equal(Object.isFrozen(composition), true);
  assert.deepEqual(
    Reflect.ownKeys(value.atomicCrashCaptureAssembler),
    [],
  );
  assert.equal(
    Object.getPrototypeOf(value.atomicCrashCaptureAssembler),
    null,
  );
  assert.equal(Object.isFrozen(value.atomicCrashCaptureAssembler), true);
  assert.deepEqual(Reflect.ownKeys(value.facade).sort(), [
    "prepareLaunchIntent",
    "reconcileLaunchAttempt",
    "resolveStoppedWriter",
    "retirePreparedCapture",
    "retireStoppedWriter",
    "runLaunch",
    "runPreparedLaunch",
    "stopWriterForCapture",
    "stopWriterForPreparedCapture",
  ]);
});

test("rejects unbranded atomic assemblers before touching capture collaborators", async () => {
  const value = await fixture();
  const foreign = await fixture();
  const request = atomicCrashCaptureRequest(value);
  const collaborators = atomicCompositionCollaborators(request);
  const clone = structuredClone(value.atomicCrashCaptureAssembler);
  const liveProxy = new Proxy(value.atomicCrashCaptureAssembler, {});
  const revoked = Proxy.revocable(
    value.atomicCrashCaptureAssembler,
    {},
  );
  revoked.revoke();

  for (const atomicCrashCaptureAssembler of [
    value.facade,
    {},
    exactRecord({}),
    clone,
    liveProxy,
    revoked.proxy,
  ]) {
    assert.throws(
      () =>
        createPostgresLvmAtomicCrashCaptureComposition({
          atomicCrashCaptureAssembler,
          baseBackend: collaborators.baseBackend,
          catalogue: collaborators.catalogue,
          driver: collaborators.driver,
        }),
      assertAtomicCompositionError(
        "invalid_postgres_lvm_atomic_crash_capture_composition_options",
      ),
    );
  }

  const foreignComposition =
    createPostgresLvmAtomicCrashCaptureComposition({
      atomicCrashCaptureAssembler:
        foreign.atomicCrashCaptureAssembler,
      baseBackend: collaborators.baseBackend,
      catalogue: collaborators.catalogue,
      driver: collaborators.driver,
    });
  await assert.rejects(
    foreignComposition.runCapture(exactRecord({ request })),
    assertAtomicCompositionError(
      "postgres_lvm_atomic_crash_capture_composition_outcome_uncertain",
    ),
  );
  assert.equal(value.supervisorStopCalls, 0);
  assert.equal(foreign.supervisorStopCalls, 0);
  assert.deepEqual(collaborators.calls, {
    capture: 0,
    claim: 0,
    commit: 0,
    mark: 0,
    read: 0,
    resolve: 0,
    verify: 0,
  });
});

test("atomic complete-stop identity uses its own domain and the full canonical request", async () => {
  const value = await fixture();
  const request = atomicCrashCaptureRequest(value);
  const operationId =
    derivePostgresLogicalWriterAtomicCrashCaptureStopOperationId({
      launchAttemptId: LAUNCH_ATTEMPT_ID,
      request,
    });
  const expectedDigest = createHash("sha256")
    .update(
      "portable-codex-runtime:writer-stop-atomic-crash-capture:v1",
    )
    .update("\0")
    .update(LAUNCH_ATTEMPT_ID)
    .update("\0")
    .update(JSON.stringify(canonicalJsonValue(request)))
    .digest("hex");
  assert.equal(operationId, `writer-stop:${expectedDigest}`);

  const changedAttempt =
    derivePostgresLogicalWriterAtomicCrashCaptureStopOperationId({
      launchAttemptId: LAUNCH_ATTEMPT_ID,
      request: {
        ...request,
        captureAttemptId: "atomic-capture-attempt-002",
      },
    });
  const changedSourcePath =
    derivePostgresLogicalWriterAtomicCrashCaptureStopOperationId({
      launchAttemptId: LAUNCH_ATTEMPT_ID,
      request: {
        ...request,
        sourceAttachment: {
          ...request.sourceAttachment,
          rootPath: "/var/lib/portable-codex/session-001-rebound",
        },
      },
    });
  const changedLaunch =
    derivePostgresLogicalWriterAtomicCrashCaptureStopOperationId({
      launchAttemptId: "writer-launch-attempt-002",
      request,
    });
  assert.notEqual(changedAttempt, operationId);
  assert.notEqual(changedSourcePath, operationId);
  assert.notEqual(changedLaunch, operationId);
  assert.notEqual(
    derivePostgresLogicalWriterStopOperationId({
      ...resolverInput(value),
      launchAttemptId: LAUNCH_ATTEMPT_ID,
    }),
    operationId,
  );
});

test("clean and atomic complete-stop routes reject cross-use in both directions", async () => {
  const clean = await fixture();
  await clean.facade.runLaunch(runInput(clean));
  const cleanCapture = resolverInput(clean);
  const prepared = await clean.facade.stopWriterForPreparedCapture(
    cleanCapture,
  );
  const cleanRequest = atomicCrashCaptureRequest(clean);
  const cleanCollaborators = atomicCompositionCollaborators(cleanRequest);
  const cleanComposition = createPostgresLvmAtomicCrashCaptureComposition({
    atomicCrashCaptureAssembler: clean.atomicCrashCaptureAssembler,
    baseBackend: cleanCollaborators.baseBackend,
    catalogue: cleanCollaborators.catalogue,
    driver: cleanCollaborators.driver,
  });
  await assert.rejects(
    cleanComposition.runCapture(exactRecord({ request: cleanRequest })),
    assertAtomicCompositionError(
      "postgres_lvm_atomic_crash_capture_composition_outcome_uncertain",
    ),
  );
  assert.equal(cleanCollaborators.calls.capture, 0);
  assert.equal(cleanCollaborators.calls.claim, 0);
  clean.facade.retirePreparedCapture({
    resolution: prepared.resolution,
    result: prepared.stop.operation.request.captureIntent.predeterminedResult,
  });

  const atomic = await fixture();
  await atomic.facade.runLaunch(runInput(atomic));
  const request = atomicCrashCaptureRequest(atomic);
  let releaseCapture;
  let signalCaptureStarted;
  const captureStarted = new Promise((resolve) => {
    signalCaptureStarted = resolve;
  });
  const captureGate = new Promise((resolve) => {
    releaseCapture = resolve;
  });
  const atomicCollaborators = atomicCompositionCollaborators(request, {
    captureGate,
    onCaptureStart: signalCaptureStarted,
  });
  const atomicComposition = createPostgresLvmAtomicCrashCaptureComposition({
    atomicCrashCaptureAssembler: atomic.atomicCrashCaptureAssembler,
    baseBackend: atomicCollaborators.baseBackend,
    catalogue: atomicCollaborators.catalogue,
    driver: atomicCollaborators.driver,
  });
  const pendingCapture = atomicComposition.runCapture(
    exactRecord({ request }),
  );
  await captureStarted;
  assert.throws(
    () => atomic.facade.resolveStoppedWriter(resolverInput(atomic)),
    assertLauncherError("invalid_logical_writer_launch_request"),
  );
  await assert.rejects(
    atomic.facade.stopWriterForCapture(resolverInput(atomic)),
    assertLauncherError("invalid_logical_writer_launch_request"),
  );
  releaseCapture();
  assert.deepEqual(
    await pendingCapture,
    atomicCrashCaptureResult(request),
  );
});

test("atomic composition uses captured coordinator intrinsics for fresh and replay retirement", async (t) => {
  for (const entry of [
    { committed: false, name: "consumed authority" },
    { committed: true, name: "issued replay authority" },
  ]) {
    await t.test(entry.name, async () => {
      hostileRegisterWriterCalls = 0;
      hostileConsumeCapabilityCalls = 0;
      hostileLaunchAdmissionCalls = 0;
      hostileRetireWriterCalls = 0;
      hostileRevokeWriterCalls = 0;
      hostileStopWriterCalls = 0;
      const value = await fixture({
        stoppedWriterCoordinator: new HostileStoppedWriterCoordinator(),
      });
      await value.facade.runLaunch(runInput(value));
      const request = atomicCrashCaptureRequest(value);
      const collaborators = atomicCompositionCollaborators(request, entry);
      const composition = createPostgresLvmAtomicCrashCaptureComposition({
        atomicCrashCaptureAssembler: value.atomicCrashCaptureAssembler,
        baseBackend: collaborators.baseBackend,
        catalogue: collaborators.catalogue,
        driver: collaborators.driver,
      });

      assert.deepEqual(
        await composition.runCapture(exactRecord({ request })),
        atomicCrashCaptureResult(request),
      );

      assert.equal(hostileLaunchAdmissionCalls, 0);
      assert.equal(hostileRegisterWriterCalls, 0);
      assert.equal(hostileStopWriterCalls, 0);
      assert.equal(hostileConsumeCapabilityCalls, 0);
      assert.equal(hostileRevokeWriterCalls, 0);
      assert.equal(hostileRetireWriterCalls, 0);
      assert.equal(
        Reflect.apply(
          StoppedWriterCapabilityCoordinator.prototype
            .assertWriterLaunchAvailable,
          value.stoppedWriterCoordinator,
          [higherEpochWriterBinding()],
        ),
        undefined,
      );
    });
  }
});

test("real LVM atomic composition retires fresh and committed-replay complete stops", async (t) => {
  for (const entry of [
    { committed: false, name: "fresh capture" },
    { committed: true, name: "committed replay" },
  ]) {
    await t.test(entry.name, async () => {
      const value = await fixture();
      await value.facade.runLaunch(runInput(value));
      const request = atomicCrashCaptureRequest(value);
      const collaborators = atomicCompositionCollaborators(request, entry);
      const composition = createPostgresLvmAtomicCrashCaptureComposition({
        atomicCrashCaptureAssembler: value.atomicCrashCaptureAssembler,
        baseBackend: collaborators.baseBackend,
        catalogue: collaborators.catalogue,
        driver: collaborators.driver,
      });

      const result = await composition.runCapture(exactRecord({ request }));

      assert.deepEqual(result, atomicCrashCaptureResult(request));
      assert.equal(collaborators.calls.resolve, 1);
      assert.equal(collaborators.calls.claim, 1);
      assert.equal(collaborators.calls.capture, entry.committed ? 0 : 1);
      assert.equal(collaborators.calls.commit, entry.committed ? 0 : 1);
      assert.equal(collaborators.calls.verify, entry.committed ? 1 : 0);
      assert.equal(collaborators.calls.mark, 0);
      assert.equal(value.supervisorStopCalls, 1);
      assert.equal(
        value.stoppedWriterCoordinator.assertWriterLaunchAvailable(
          higherEpochWriterBinding(),
        ),
        undefined,
      );
    });
  }
});

test("atomic composition validates the exact provider request before writer stop", async () => {
  const value = await fixture();
  await value.facade.runLaunch(runInput(value));
  const request = atomicCrashCaptureRequest(value);
  const collaborators = atomicCompositionCollaborators(request);
  const composition = createPostgresLvmAtomicCrashCaptureComposition({
    atomicCrashCaptureAssembler: value.atomicCrashCaptureAssembler,
    baseBackend: collaborators.baseBackend,
    catalogue: collaborators.catalogue,
    driver: collaborators.driver,
  });

  await assert.rejects(
    composition.runCapture(
      exactRecord({
        request: {
          ...request,
          checkpoint: {
            ...request.checkpoint,
            checkpointClass: "clean",
          },
        },
      }),
    ),
    assertAtomicCompositionError(
      "invalid_postgres_lvm_atomic_crash_capture_composition_request",
    ),
  );
  assert.equal(value.supervisorStopCalls, 0);
  assert.deepEqual(collaborators.calls, {
    capture: 0,
    claim: 0,
    commit: 0,
    mark: 0,
    read: 0,
    resolve: 0,
    verify: 0,
  });
});

test("atomic composition retries an exact request after pre-stop uncertainty", async () => {
  const value = await fixture();
  await value.facade.runLaunch(runInput(value));
  const request = atomicCrashCaptureRequest(value);
  const collaborators = atomicCompositionCollaborators(request);
  const composition = createPostgresLvmAtomicCrashCaptureComposition({
    atomicCrashCaptureAssembler: value.atomicCrashCaptureAssembler,
    baseBackend: collaborators.baseBackend,
    catalogue: collaborators.catalogue,
    driver: collaborators.driver,
  });
  const outcomeCode =
    "postgres_lvm_atomic_crash_capture_composition_outcome_uncertain";

  value.authority.behaviour.readSessionThrows = true;
  await assert.rejects(
    composition.runCapture(exactRecord({ request })),
    assertAtomicCompositionError(outcomeCode),
  );
  assert.equal(value.supervisorStopCalls, 0);
  assert.deepEqual(collaborators.calls, {
    capture: 0,
    claim: 0,
    commit: 0,
    mark: 0,
    read: 0,
    resolve: 0,
    verify: 0,
  });

  value.authority.behaviour.readSessionThrows = false;
  assert.deepEqual(
    await composition.runCapture(exactRecord({ request })),
    atomicCrashCaptureResult(request),
  );
  assert.equal(value.supervisorStopCalls, 1);
  assert.equal(collaborators.calls.resolve, 1);
  assert.equal(collaborators.calls.claim, 1);
  assert.equal(collaborators.calls.capture, 1);
  assert.equal(collaborators.calls.commit, 1);
  assert.equal(collaborators.calls.read, 0);
  assert.equal(collaborators.calls.verify, 0);
});

test("atomic composition replays its exact retired result without redispatch", async () => {
  const value = await fixture();
  await value.facade.runLaunch(runInput(value));
  const request = atomicCrashCaptureRequest(value);
  const collaborators = atomicCompositionCollaborators(request);
  const composition = createPostgresLvmAtomicCrashCaptureComposition({
    atomicCrashCaptureAssembler: value.atomicCrashCaptureAssembler,
    baseBackend: collaborators.baseBackend,
    catalogue: collaborators.catalogue,
    driver: collaborators.driver,
  });

  const first = await composition.runCapture(exactRecord({ request }));
  const repeated = await composition.runCapture(exactRecord({ request }));
  const reconciled = await composition.reconcileCapture(
    exactRecord({ request }),
  );

  assert.strictEqual(repeated, first);
  assert.strictEqual(reconciled, first);
  assert.equal(value.supervisorStopCalls, 1);
  assert.deepEqual(collaborators.calls, {
    capture: 1,
    claim: 1,
    commit: 1,
    mark: 0,
    read: 0,
    resolve: 1,
    verify: 0,
  });

  await assert.rejects(
    composition.runCapture(
      exactRecord({
        request: {
          ...request,
          sourceAttachment: {
            ...request.sourceAttachment,
            rootPath: `${request.sourceAttachment.rootPath}-replacement`,
          },
        },
      }),
    ),
    assertAtomicCompositionError(
      "postgres_lvm_atomic_crash_capture_composition_outcome_uncertain",
    ),
  );
});

test("atomic composition reconciles commit acknowledgement loss without redispatch", async (t) => {
  const outcomeCode =
    "postgres_lvm_atomic_crash_capture_composition_outcome_uncertain";

  await t.test("unknown remains blocked until committed-only reconciliation", async () => {
    const value = await fixture();
    await value.facade.runLaunch(runInput(value));
    const request = atomicCrashCaptureRequest(value);
    const collaborators = atomicCompositionCollaborators(request, {
      commitAcknowledgementLoss: true,
      committedReadVisible: false,
    });
    const composition = createPostgresLvmAtomicCrashCaptureComposition({
      atomicCrashCaptureAssembler: value.atomicCrashCaptureAssembler,
      baseBackend: collaborators.baseBackend,
      catalogue: collaborators.catalogue,
      driver: collaborators.driver,
    });
    const siblingComposition =
      createPostgresLvmAtomicCrashCaptureComposition({
        atomicCrashCaptureAssembler:
          value.atomicCrashCaptureAssembler,
        baseBackend: collaborators.baseBackend,
        catalogue: collaborators.catalogue,
        driver: collaborators.driver,
      });

    await assert.rejects(
      composition.runCapture(exactRecord({ request })),
      assertAtomicCompositionError(outcomeCode),
    );
    await assert.rejects(
      siblingComposition.runCapture(exactRecord({ request })),
      assertAtomicCompositionError(outcomeCode),
    );
    assert.equal(value.supervisorStopCalls, 1);
    assert.equal(collaborators.calls.resolve, 1);
    assert.equal(collaborators.calls.claim, 1);
    assert.equal(collaborators.calls.capture, 1);
    assert.equal(collaborators.calls.commit, 1);
    assert.equal(collaborators.calls.read, 1);
    assert.equal(collaborators.calls.verify, 0);
    assert.equal(collaborators.calls.mark, 0);
    assertWriterLaunchBlocked(value.stoppedWriterCoordinator);

    await assert.rejects(
      composition.runCapture(exactRecord({ request })),
      assertAtomicCompositionError(outcomeCode),
    );
    assert.equal(value.supervisorStopCalls, 1);
    assert.equal(collaborators.calls.resolve, 1);
    assert.equal(collaborators.calls.claim, 1);
    assert.equal(collaborators.calls.capture, 1);
    assert.equal(collaborators.calls.commit, 1);
    assert.equal(collaborators.calls.read, 1);

    collaborators.setCommittedReadVisible(true);
    const pendingResult = composition.reconcileCapture(
      exactRecord({ request }),
    );
    await assert.rejects(
      composition.reconcileCapture(exactRecord({ request })),
      assertAtomicCompositionError(outcomeCode),
    );
    const result = await pendingResult;
    assert.deepEqual(result, atomicCrashCaptureResult(request));
    assert.equal(value.supervisorStopCalls, 1);
    assert.equal(collaborators.calls.resolve, 1);
    assert.equal(collaborators.calls.claim, 1);
    assert.equal(collaborators.calls.capture, 1);
    assert.equal(collaborators.calls.commit, 1);
    assert.equal(collaborators.calls.read, 2);
    assert.equal(collaborators.calls.verify, 1);
    assert.equal(collaborators.calls.mark, 0);
    assert.equal(
      value.stoppedWriterCoordinator.assertWriterLaunchAvailable(
        higherEpochWriterBinding(),
      ),
      undefined,
    );
  });

  await t.test("immediate committed read retires after exactly one capture", async () => {
    const value = await fixture();
    await value.facade.runLaunch(runInput(value));
    const request = atomicCrashCaptureRequest(value);
    const collaborators = atomicCompositionCollaborators(request, {
      commitAcknowledgementLoss: true,
      committedReadVisible: true,
    });
    const composition = createPostgresLvmAtomicCrashCaptureComposition({
      atomicCrashCaptureAssembler: value.atomicCrashCaptureAssembler,
      baseBackend: collaborators.baseBackend,
      catalogue: collaborators.catalogue,
      driver: collaborators.driver,
    });

    const result = await composition.runCapture(exactRecord({ request }));
    assert.deepEqual(result, atomicCrashCaptureResult(request));
    assert.equal(value.supervisorStopCalls, 1);
    assert.equal(collaborators.calls.resolve, 1);
    assert.equal(collaborators.calls.claim, 1);
    assert.equal(collaborators.calls.capture, 1);
    assert.equal(collaborators.calls.commit, 1);
    assert.equal(collaborators.calls.read, 1);
    assert.equal(collaborators.calls.verify, 1);
    assert.equal(collaborators.calls.mark, 0);
    assert.equal(
      value.stoppedWriterCoordinator.assertWriterLaunchAvailable(
        higherEpochWriterBinding(),
      ),
      undefined,
    );
  });
});

test("atomic authority failure and concurrent reuse remain permanently closed", async (t) => {
  const outcomeCode =
    "postgres_lvm_atomic_crash_capture_composition_outcome_uncertain";

  await t.test("capture failure becomes uncertain", async () => {
    const value = await fixture();
    await value.facade.runLaunch(runInput(value));
    const request = atomicCrashCaptureRequest(value);
    const collaborators = atomicCompositionCollaborators(request, {
      captureThrows: true,
      committedReadVisible: false,
    });
    const composition = createPostgresLvmAtomicCrashCaptureComposition({
      atomicCrashCaptureAssembler: value.atomicCrashCaptureAssembler,
      baseBackend: collaborators.baseBackend,
      catalogue: collaborators.catalogue,
      driver: collaborators.driver,
    });

    await assert.rejects(
      composition.runCapture(exactRecord({ request })),
      assertAtomicCompositionError(outcomeCode),
    );
    await assert.rejects(
      composition.runCapture(exactRecord({ request })),
      assertAtomicCompositionError(outcomeCode),
    );
    assert.equal(value.supervisorStopCalls, 1);
    assert.equal(collaborators.calls.capture, 1);
    assert.equal(collaborators.calls.commit, 0);
    assertWriterLaunchBlocked(value.stoppedWriterCoordinator);
  });

  await t.test("concurrent reuse cannot overtake the first consume", async () => {
    const value = await fixture();
    await value.facade.runLaunch(runInput(value));
    const request = atomicCrashCaptureRequest(value);
    let releaseCapture;
    let signalCaptureStarted;
    const captureStarted = new Promise((resolve) => {
      signalCaptureStarted = resolve;
    });
    const captureGate = new Promise((resolve) => {
      releaseCapture = resolve;
    });
    const collaborators = atomicCompositionCollaborators(request, {
      captureGate,
      onCaptureStart: signalCaptureStarted,
    });
    const composition = createPostgresLvmAtomicCrashCaptureComposition({
      atomicCrashCaptureAssembler: value.atomicCrashCaptureAssembler,
      baseBackend: collaborators.baseBackend,
      catalogue: collaborators.catalogue,
      driver: collaborators.driver,
    });
    const siblingComposition =
      createPostgresLvmAtomicCrashCaptureComposition({
        atomicCrashCaptureAssembler:
          value.atomicCrashCaptureAssembler,
        baseBackend: collaborators.baseBackend,
        catalogue: collaborators.catalogue,
        driver: collaborators.driver,
      });

    const first = composition.runCapture(
      exactRecord({ request }),
    );
    await captureStarted;
    await assert.rejects(
      composition.runCapture(exactRecord({ request })),
      assertAtomicCompositionError(outcomeCode),
    );
    await assert.rejects(
      siblingComposition.runCapture(exactRecord({ request })),
      assertAtomicCompositionError(outcomeCode),
    );
    releaseCapture();
    assert.deepEqual(
      await first,
      atomicCrashCaptureResult(request),
    );
    assert.equal(value.supervisorStopCalls, 1);
    assert.equal(collaborators.calls.capture, 1);
    assert.equal(
      value.stoppedWriterCoordinator.assertWriterLaunchAvailable(
        higherEpochWriterBinding(),
      ),
      undefined,
    );
  });
});

test("stop request rejects a revoked envelope before physical stop", async () => {
  const value = await fixture();
  await value.facade.runLaunch(runInput(value));
  const revoked = Proxy.revocable(resolverInput(value), {});
  revoked.revoke();

  await assert.rejects(
    value.facade.stopWriterForCapture(revoked.proxy),
    assertLauncherError("invalid_logical_writer_launch_request"),
  );
  assert.equal(value.supervisorStopCalls, 0);
  assert.equal(value.authority.calls.stopClaim, 0);
});

test("resolver binds the complete attachment and one deterministic capture operation", async () => {
  const value = await fixture();
  await value.facade.runLaunch(runInput(value));

  assert.throws(
    () =>
      value.facade.resolveStoppedWriter(
        resolverInput(value, {
          checkpoint: checkpoint(`sha256:${"e".repeat(64)}`),
        }),
      ),
    assertLauncherError("invalid_logical_writer_launch_request"),
  );
  const first = value.facade.resolveStoppedWriter(resolverInput(value));
  assert.match(first.stopOperationId, /^writer-stop:[0-9a-f]{64}$/u);
  assert.throws(
    () =>
      value.facade.resolveStoppedWriter(
        resolverInput(value, {
          attachment: attachment(lease(), {
            rootPath: "/var/lib/portable-codex/replaced-session",
          }),
        }),
      ),
    assertLauncherError("invalid_logical_writer_launch_request"),
  );
  assert.throws(
    () =>
      value.facade.resolveStoppedWriter(
        resolverInput(value, {
          checkpoint: {
            ...checkpoint(value.image.manifest.runtime.imageDigest),
            createdAt: "2026-08-04T11:00:01.000Z",
          },
        }),
      ),
    assertLauncherError("invalid_logical_writer_launch_request"),
  );
  assert.throws(
    () =>
      value.facade.resolveStoppedWriter(
        resolverInput(value, {
          request: {
            ...captureRequest(),
            operationId: "checkpoint-capture-operation-002",
          },
        }),
      ),
    assertLauncherError("invalid_logical_writer_launch_request"),
  );
  const repeated = value.facade.resolveStoppedWriter(resolverInput(value));
  assert.equal(repeated.stopOperationId, first.stopOperationId);
  assert.strictEqual(repeated.writer, first.writer);
});

test("uses the captured base registration intrinsic instead of a subclass override", async () => {
  hostileRegisterWriterCalls = 0;
  hostileLaunchAdmissionCalls = 0;
  const stoppedWriterCoordinator = new HostileStoppedWriterCoordinator();
  const value = await fixture({ stoppedWriterCoordinator });

  const result = await value.facade.runLaunch(runInput(value));
  assert.equal(result.status, "started");
  assert.equal(hostileLaunchAdmissionCalls, 0);
  assert.equal(hostileRegisterWriterCalls, 0);
  const resolved = value.facade.resolveStoppedWriter(resolverInput(value));
  assert.strictEqual(resolved.writer, result.writer);
});

test("keeps launch behavior stable after selected mutable intrinsics are poisoned", async () => {
  const value = await fixture();
  const originals = {
    arrayIsArray: Array.isArray,
    jsonStringify: JSON.stringify,
    objectFreeze: Object.freeze,
    objectGetOwnPropertyDescriptor: Object.getOwnPropertyDescriptor,
    objectGetPrototypeOf: Object.getPrototypeOf,
    objectHasOwn: Object.hasOwn,
    promiseThen: Promise.prototype.then,
    reflectApply: Reflect.apply,
    reflectOwnKeys: Reflect.ownKeys,
  };
  const poisonCalls = [];
  const poison = (name) =>
    function poisonedIntrinsic() {
      poisonCalls.push(name);
      throw new Error(`mutable intrinsic used after capture: ${name}`);
  };
  const launchWriter = async (context) => {
    Array.isArray = poison("Array.isArray");
    JSON.stringify = poison("JSON.stringify");
    Object.freeze = poison("Object.freeze");
    Object.getOwnPropertyDescriptor = poison("Object.getOwnPropertyDescriptor");
    Object.getPrototypeOf = poison("Object.getPrototypeOf");
    Object.hasOwn = poison("Object.hasOwn");
    Reflect.apply = poison("Reflect.apply");
    Reflect.ownKeys = poison("Reflect.ownKeys");
    return {
      receiptVersion: 2,
      evidence: evidence(LAUNCH_ATTEMPT_ID, "started"),
      stopWriter: async (binding) => ({
        confirmation: STOPPED_WRITER_STOP_CONFIRMED,
        contractVersion: LOGICAL_WRITER_SUPERVISOR_CONTRACT_VERSION,
        terminalRecord: supervisorTerminalRecord({
          launchAttemptId: context.attempt.launchAttemptId,
          processIncarnationId: binding.processIncarnationId,
          requestSha256: context.operation.requestSha256,
          stopOperationId: binding.stopOperationId,
          stopProofId: binding.stopOperationId,
          writerIncarnationId: binding.writerIncarnationId,
        }),
      }),
      terminalRecord: null,
    };
  };
  const facade = createPostgresLogicalWriterLauncher({
    authority: value.authority,
    imagePlanBinding: value.imagePlanBinding,
    operationGuard: value.operationGuard,
    stoppedWriterCoordinator: value.stoppedWriterCoordinator,
    supervisor: {
      contractVersion: LOGICAL_WRITER_SUPERVISOR_CONTRACT_VERSION,
      stateOwnerId: STATE_OWNER_ID,
      supervisorId: SUPERVISOR_ID,
      launchWriter,
      reconcileWriterLaunch: async () => ({
        receiptVersion: LOGICAL_WRITER_RECONCILE_RECEIPT_VERSION,
        evidence: evidence(LAUNCH_ATTEMPT_ID, "not-started"),
        terminalRecord: null,
      }),
    },
  });

  let result;
  let failure = null;
  const pending = facade.runLaunch(runInput(value));
  let protectedChain;
  Promise.prototype.then = poison("Promise.prototype.then");
  try {
    protectedChain = pending.then((resolved) => resolved);
  } finally {
    Promise.prototype.then = originals.promiseThen;
  }
  try {
    result = await protectedChain;
  } catch (error) {
    failure = error;
  } finally {
    Array.isArray = originals.arrayIsArray;
    JSON.stringify = originals.jsonStringify;
    Object.freeze = originals.objectFreeze;
    Object.getOwnPropertyDescriptor =
      originals.objectGetOwnPropertyDescriptor;
    Object.getPrototypeOf = originals.objectGetPrototypeOf;
    Object.hasOwn = originals.objectHasOwn;
    Promise.prototype.then = originals.promiseThen;
    Reflect.apply = originals.reflectApply;
    Reflect.ownKeys = originals.reflectOwnKeys;
  }
  assert.deepEqual(poisonCalls, []);
  if (failure !== null) throw failure;
  assert.equal(result.status, "started");
});

test("preexisting writer blocks before durable reservation or physical launch", async () => {
  const value = await fixture({ reconcileStatus: "complete-stopped" });
  value.stoppedWriterCoordinator.registerWriter({
    attachment: attachment(),
    canonicalLease: lease(),
    processIncarnationId: "preexisting-process-001",
    stopWriter: async () => STOPPED_WRITER_STOP_CONFIRMED,
    writerIncarnationId: "preexisting-writer-001",
  });

  await assert.rejects(
    value.facade.runLaunch(runInput(value)),
    assertLauncherError("logical_writer_launch_admission_unavailable", true),
  );
  assert.equal(value.launchCalls, 0);
  assert.equal(value.authority.state, "absent");
  assert.equal(value.authority.calls.reserve, 0);
  assert.equal(value.authority.calls.claim, 0);
  assert.equal(value.authority.calls.markUncertain, 0);
  assert.equal(value.authority.calls.finalizeStarted, 0);
  assert.equal(value.authority.calls.finalizeStopped, 0);
  assert.throws(
    () => value.facade.resolveStoppedWriter(resolverInput(value)),
    assertLauncherError("invalid_logical_writer_launch_request"),
  );
});

test("runLaunch rejects an operation-ID replay with a different durable request", async () => {
  const value = await fixture();
  const first = await value.facade.runLaunch(runInput(value));
  const freshReservation =
    await value.imagePlanBinding.prepareImageReservation(
      objectFreeze({
        plan: value.imagePlan,
        sessionManifest: value.image.manifest,
      }),
    );
  const inspectionsBeforeReplay = value.inspectionCount;

  await assert.rejects(
    value.facade.runLaunch({
      generation: generationSnapshot({
        generationId: "restore-generation-002",
      }),
      imageReservation: freshReservation,
      launchAttemptId: LAUNCH_ATTEMPT_ID,
    }),
    assertLauncherError("invalid_logical_writer_launch_request"),
  );
  assert.equal(value.launchCalls, 1);
  assert.equal(value.authority.calls.claim, 1);
  assert.equal(value.inspectionCount, inspectionsBeforeReplay + 1);
  const replay = await value.facade.reconcileLaunchAttempt({
    launchAttemptId: LAUNCH_ATTEMPT_ID,
  });
  assert.strictEqual(replay.writer, first.writer);
});

for (const status of ["not-started", "complete-stopped"]) {
  test(`finalizes ${status} without registering a writer`, async () => {
    const value = await fixture({ launchStatus: status });
    const result = await value.facade.runLaunch(runInput(value));
    assert.equal(result.status, status);
    assert.equal(result.writer, null);
    assert.equal(result.launch, null);
    assert.equal(value.authority.calls.finalizeStopped, 1);
    assert.equal(value.authority.calls.finalizeStarted, 0);
    assert.throws(
      () => value.facade.resolveStoppedWriter(resolverInput(value)),
      assertLauncherError("invalid_logical_writer_launch_request"),
    );

    const independentWriter = value.stoppedWriterCoordinator.registerWriter({
      attachment: attachment(),
      canonicalLease: lease(),
      processIncarnationId: "independent-process-001",
      stopWriter: async () => STOPPED_WRITER_STOP_CONFIRMED,
      writerIncarnationId: "independent-writer-001",
    });
    assert.equal(Object.isFrozen(independentWriter), true);
  });
}

test("claim acknowledgement loss reconciles without consuming or launching", async () => {
  const value = await fixture({ reconcileStatus: "not-started" });
  value.authority.behaviour.claimThrowAfterCommit = true;

  const result = await value.facade.runLaunch(runInput(value));
  assert.equal(result.status, "not-started");
  assert.equal(result.writer, null);
  assert.equal(value.inspectionCount, 2);
  assert.equal(value.launchCalls, 0);
  assert.equal(value.reconcileCalls, 1);
  assert.equal(value.authority.calls.markUncertain, 1);
  assert.equal(value.authority.calls.finalizeStopped, 1);
  assert.deepEqual(Reflect.ownKeys(value.reconcileContext), [
    "contractVersion",
    "attempt",
    "launch",
    "operation",
    "reservation",
    "session",
  ]);
});

test("session-read admission failure is retryable before durable reservation", async () => {
  const value = await fixture();
  value.authority.behaviour.readSessionThrows = true;

  await assert.rejects(
    value.facade.runLaunch(runInput(value)),
    assertLauncherError("logical_writer_launch_admission_unavailable", true),
  );
  assert.equal(value.inspectionCount, 1);
  assert.equal(value.authority.state, "absent");
  assert.equal(value.authority.calls.reserve, 0);
  assert.equal(value.authority.calls.claim, 0);
  assert.equal(value.launchCalls, 0);
  assert.equal(value.reconcileCalls, 0);
  assert.equal(value.authority.calls.markUncertain, 0);
});

test("guard admission failure permits an exact same-attempt retry", async () => {
  const value = await fixture();
  value.operationGuard.failBeforeCallback = true;

  await assert.rejects(
    value.facade.runLaunch(runInput(value)),
    assertLauncherError("logical_writer_launch_admission_unavailable", true),
  );
  assert.equal(value.inspectionCount, 1);
  assert.equal(value.authority.state, "absent");
  assert.equal(value.authority.calls.reserve, 0);
  assert.equal(value.authority.calls.claim, 0);
  assert.equal(value.launchCalls, 0);
  assert.equal(value.reconcileCalls, 0);
  assert.equal(value.authority.calls.markUncertain, 0);

  value.operationGuard.failBeforeCallback = false;
  const result = await value.facade.runLaunch(runInput(value));
  assert.equal(result.status, "started");
  assert.equal(value.authority.calls.reserve, 1);
  assert.equal(value.launchCalls, 1);
});

test("image revalidation admission failure permits a fresh-reservation retry", async () => {
  const value = await fixture();
  value.failInspectionAt(2);

  await assert.rejects(
    value.facade.runLaunch(runInput(value)),
    assertLauncherError("logical_writer_launch_admission_unavailable", true),
  );
  assert.equal(value.inspectionCount, 2);
  assert.equal(value.authority.state, "absent");
  assert.equal(value.authority.calls.reserve, 0);
  assert.equal(value.authority.calls.claim, 0);
  assert.equal(value.launchCalls, 0);
  assert.equal(value.reconcileCalls, 0);
  assert.equal(value.authority.calls.markUncertain, 0);

  value.failInspectionAt(null);
  const fresh = await value.imagePlanBinding.prepareImageReservation(
    objectFreeze({
      plan: value.imagePlan,
      sessionManifest: value.image.manifest,
    }),
  );
  const result = await value.facade.runLaunch(
    runInput(value, {
      imageReservation: fresh,
    }),
  );
  assert.equal(result.status, "started");
  assert.equal(value.authority.calls.reserve, 1);
  assert.equal(value.launchCalls, 1);
});

test("claim acknowledgement loss plus failed read stays closed", async () => {
  const value = await fixture();
  value.authority.behaviour.claimThrowAfterCommit = true;
  value.authority.behaviour.readThrows = true;

  await assert.rejects(
    value.facade.runLaunch(runInput(value)),
    assertLauncherError("logical_writer_launch_outcome_uncertain"),
  );
  assert.equal(value.inspectionCount, 2);
  assert.equal(value.launchCalls, 0);
  assert.equal(value.reconcileCalls, 0);
  assert.equal(value.authority.calls.markUncertain, 0);
});

test("reserve acknowledgement loss cancels the still-prepared attempt", async () => {
  const value = await fixture();
  value.authority.behaviour.reserveThrowAfterCommit = true;

  const result = await value.facade.runLaunch(runInput(value));
  assert.equal(result.status, "cancelled-before-dispatch");
  assert.equal(result.writer, null);
  assert.equal(value.inspectionCount, 2);
  assert.equal(value.launchCalls, 0);
  assert.equal(value.authority.calls.claim, 0);
  assert.equal(value.authority.calls.cancel, 1);
  assert.equal(value.authority.calls.markUncertain, 0);
});

test("reserve acknowledgement loss plus failed read stays non-retryable", async () => {
  const value = await fixture();
  value.authority.behaviour.reserveThrowAfterCommit = true;
  value.authority.behaviour.readThrows = true;

  await assert.rejects(
    value.facade.runLaunch(runInput(value)),
    assertLauncherError("logical_writer_launch_outcome_uncertain"),
  );
  assert.equal(value.inspectionCount, 2);
  assert.equal(value.authority.state, "prepared");
  assert.equal(value.authority.calls.reserve, 1);
  assert.equal(value.authority.calls.claim, 0);
  assert.equal(value.authority.calls.read, 1);
  assert.equal(value.launchCalls, 0);
  assert.equal(value.reconcileCalls, 0);
  assert.equal(value.authority.calls.markUncertain, 0);
});

test("image consumption ambiguity marks the claimed launch uncertain without dispatch", async () => {
  const value = await fixture();
  value.failInspectionAt(3);

  await assert.rejects(
    value.facade.runLaunch(runInput(value)),
    assertLauncherError("logical_writer_launch_outcome_uncertain"),
  );
  assert.equal(value.inspectionCount, 3);
  assert.equal(value.launchCalls, 0);
  assert.equal(value.authority.state, "uncertain");
  assert.equal(value.authority.calls.markUncertain, 1);
  assert.equal(value.authority.calls.finalizeStarted, 0);
  assert.equal(value.authority.calls.finalizeStopped, 0);
});

test("finalization acknowledgement loss uses exact readback and returns the same handle", async () => {
  const value = await fixture();
  value.authority.behaviour.finalizeThrowAfterCommit = true;

  const result = await value.facade.runLaunch(runInput(value));
  assert.equal(result.status, "started");
  assert.equal(value.launchCalls, 1);
  assert.equal(value.authority.calls.finalizeStarted, 1);
  assert.equal(value.authority.calls.read, 1);
  assert.equal(value.authority.calls.markUncertain, 0);
  const resolved = value.facade.resolveStoppedWriter(resolverInput(value));
  assert.strictEqual(resolved.writer, result.writer);

  const replay = await value.facade.reconcileLaunchAttempt({
    launchAttemptId: LAUNCH_ATTEMPT_ID,
  });
  assert.strictEqual(replay.writer, result.writer);
  assert.equal(value.launchCalls, 1);
});

test("committed receipts reject impossible revision and outcome pairs", async (t) => {
  const cases = [
    ["cancelled-before-dispatch", null, "2"],
    ["started", "started", "1"],
    ["not-started", "not-started", "1"],
    ["complete-stopped", "complete-stopped", "1"],
  ];

  for (const [name, evidenceStatus, revision] of cases) {
    await t.test(name, async () => {
      const value = await fixture();
      const typedRequest = createWriterLaunchAttemptOperationRequest({
        expectedSession: value.expectedSession,
        generation: value.generation,
        measuredImage: {
          projection: value.reserved.projection,
          runtimeIdentity: value.reserved.runtimeIdentity,
        },
        supervisor: { contractVersion: 1, supervisorId: SUPERVISOR_ID },
      });
      value.authority.seed(
        typedRequest,
        "committed",
        evidenceStatus === null
          ? null
          : evidence(LAUNCH_ATTEMPT_ID, evidenceStatus),
      );
      value.authority.terminalRevision = revision;

      await assert.rejects(
        value.facade.reconcileLaunchAttempt({
          launchAttemptId: LAUNCH_ATTEMPT_ID,
        }),
        assertLauncherError("logical_writer_launch_outcome_uncertain"),
      );
      assert.equal(value.launchCalls, 0);
      assert.equal(value.reconcileCalls, 0);
    });
  }
});

test("new finalization receipts require a complete terminal anchor before readback", async (t) => {
  const cases = [
    ["missing", () => null],
    [
      "wrong operation",
      (pointer) => ({
        ...pointer,
        operationId: "writer-launch-attempt-mismatch",
      }),
    ],
    [
      "wrong conflict class",
      (pointer) => ({ ...pointer, conflictClass: "other-conflict" }),
    ],
    [
      "wrong expected session revision",
      (pointer) => ({ ...pointer, expectedSessionRevision: "7" }),
    ],
    [
      "wrong revision",
      (pointer) => ({ ...pointer, operationRevision: "3" }),
    ],
    [
      "wrong request hash",
      (pointer) => ({ ...pointer, requestSha256: "e".repeat(64) }),
    ],
    [
      "wrong result hash",
      (pointer) => ({ ...pointer, resultSha256: "d".repeat(64) }),
    ],
  ];

  for (const [name, mutate] of cases) {
    await t.test(name, async () => {
      const value = await fixture();
      value.authority.finalizationLastOperationMutation = mutate;

      const result = await value.facade.runLaunch(runInput(value));
      assert.equal(result.status, "started");
      assert.equal(value.authority.calls.finalizeStarted, 1);
      assert.equal(value.authority.calls.read, 1);
      assert.strictEqual(
        value.facade.resolveStoppedWriter(resolverInput(value)).writer,
        result.writer,
      );
    });
  }
});

test("new finalization rejects a later active operation before historical readback", async () => {
  const value = await fixture();
  value.authority.beforeFinalize = () => {
    value.authority.activeOperationOverride = {
      conflictClass: SESSION_OPERATION_CONFLICT_CLASS,
      expectedSessionRevision: "11",
      kind: "checkpoint-capture-v1",
      operationId: "checkpoint-capture-operation-after-launch",
      operationRevision: "0",
      requestSha256: "c".repeat(64),
      reservationId: "reservation-checkpoint-after-launch",
      state: "prepared",
    };
    value.authority.sessionRevisionOverride = "12";
    value.authority.sessionUpdatedAtOverride =
      "2026-08-04T12:00:06.000Z";
  };

  const result = await value.facade.runLaunch(runInput(value));
  assert.equal(result.status, "started");
  assert.equal(value.authority.calls.finalizeStarted, 1);
  assert.equal(value.authority.calls.read, 1);
  assert.strictEqual(
    value.facade.resolveStoppedWriter(resolverInput(value)).writer,
    result.writer,
  );
});

test("historical readback accepts a later checkpoint anchor for the current launch", async () => {
  const value = await fixture();
  const started = await value.facade.runLaunch(runInput(value));
  const launchRevision = started.session.revision;
  value.authority.lastOperationOverride = {
    conflictClass: SESSION_OPERATION_CONFLICT_CLASS,
    expectedSessionRevision: launchRevision,
    kind: "checkpoint-capture-v1",
    operationId: "checkpoint-capture-operation-after-launch",
    operationRevision: "2",
    requestSha256: "c".repeat(64),
    reservationId: "reservation-checkpoint-after-launch",
    resultSha256: "b".repeat(64),
    state: "committed",
  };
  value.authority.sessionRevisionOverride = (
    BigInt(launchRevision) + 3n
  ).toString();
  value.authority.sessionUpdatedAtOverride =
    "2026-08-04T12:00:07.000Z";

  const replay = await value.facade.reconcileLaunchAttempt({
    launchAttemptId: LAUNCH_ATTEMPT_ID,
  });
  assert.equal(replay.status, "started");
  assert.strictEqual(replay.writer, started.writer);
  assert.equal(value.authority.calls.read, 1);
  assert.equal(value.launchCalls, 1);
  assert.equal(value.reconcileCalls, 0);
});

test("historical readback binds every current launch digest and start time", async (t) => {
  const cases = [
    [
      "wrong attachment digest",
      (pointer) => ({ ...pointer, attachmentSha256: "e".repeat(64) }),
    ],
    [
      "wrong launch result digest",
      (pointer) => ({ ...pointer, launchResultSha256: "e".repeat(64) }),
    ],
    [
      "wrong lease digest",
      (pointer) => ({ ...pointer, leaseSha256: "e".repeat(64) }),
    ],
    [
      "wrong measured image digest",
      (pointer) => ({ ...pointer, measuredImageSha256: "e".repeat(64) }),
    ],
    [
      "wrong start time",
      (pointer) => ({
        ...pointer,
        startedAt: "2026-08-04T12:00:05.000Z",
      }),
    ],
  ];

  for (const [name, mutate] of cases) {
    await t.test(name, async () => {
      const value = await fixture();
      const started = await value.facade.runLaunch(runInput(value));
      value.authority.lastOperationOverride = {
        conflictClass: SESSION_OPERATION_CONFLICT_CLASS,
        expectedSessionRevision: started.session.revision,
        kind: "checkpoint-capture-v1",
        operationId: "checkpoint-capture-operation-after-launch",
        operationRevision: "2",
        requestSha256: "c".repeat(64),
        reservationId: "reservation-checkpoint-after-launch",
        resultSha256: "b".repeat(64),
        state: "committed",
      };
      value.authority.sessionRevisionOverride = (
        BigInt(started.session.revision) + 3n
      ).toString();
      value.authority.sessionUpdatedAtOverride =
        "2026-08-04T12:00:07.000Z";
      value.authority.launchPointerMutation = mutate;

      await assert.rejects(
        value.facade.reconcileLaunchAttempt({
          launchAttemptId: LAUNCH_ATTEMPT_ID,
        }),
        assertLauncherError("logical_writer_launch_outcome_uncertain"),
      );
      assert.equal(value.authority.calls.read, 1);
      assert.equal(value.launchCalls, 1);
      assert.equal(value.reconcileCalls, 0);
    });
  }
});

test("historical readback accepts the current launch during a later active operation", async () => {
  const value = await fixture();
  const started = await value.facade.runLaunch(runInput(value));
  value.authority.activeOperationOverride = {
    conflictClass: SESSION_OPERATION_CONFLICT_CLASS,
    expectedSessionRevision: started.session.revision,
    kind: "checkpoint-capture-v1",
    operationId: "checkpoint-capture-operation-after-launch",
    operationRevision: "0",
    requestSha256: "c".repeat(64),
    reservationId: "reservation-checkpoint-after-launch",
    state: "prepared",
  };
  value.authority.sessionRevisionOverride = (
    BigInt(started.session.revision) + 1n
  ).toString();
  value.authority.sessionUpdatedAtOverride =
    "2026-08-04T12:00:06.000Z";

  const replay = await value.facade.reconcileLaunchAttempt({
    launchAttemptId: LAUNCH_ATTEMPT_ID,
  });
  assert.equal(replay.status, "started");
  assert.strictEqual(replay.writer, started.writer);
  assert.equal(value.authority.calls.read, 1);
  assert.equal(value.launchCalls, 1);
  assert.equal(value.reconcileCalls, 0);
});

test("active receipts bind conflict class and expected session revision", async (t) => {
  const cases = [
    [
      "wrong conflict class",
      (pointer) => ({ ...pointer, conflictClass: "other-conflict" }),
    ],
    [
      "wrong expected session revision",
      (pointer) => ({ ...pointer, expectedSessionRevision: "7" }),
    ],
  ];

  for (const [name, mutate] of cases) {
    await t.test(name, async () => {
      const value = await fixture();
      const typedRequest = createWriterLaunchAttemptOperationRequest({
        expectedSession: value.expectedSession,
        generation: value.generation,
        measuredImage: {
          projection: value.reserved.projection,
          runtimeIdentity: value.reserved.runtimeIdentity,
        },
        supervisor: { contractVersion: 1, supervisorId: SUPERVISOR_ID },
      });
      value.authority.seed(typedRequest, "starting");
      const operation = value.authority.operation();
      value.authority.activeOperationOverride = mutate(
        activePointer(operation, value.authority.reservation(operation)),
      );

      await assert.rejects(
        value.facade.reconcileLaunchAttempt({
          launchAttemptId: LAUNCH_ATTEMPT_ID,
        }),
        assertLauncherError("logical_writer_launch_outcome_uncertain"),
      );
      assert.equal(value.launchCalls, 0);
      assert.equal(value.reconcileCalls, 0);
    });
  }
});

test("prepared recovery cancels without image consumption or supervisor calls", async () => {
  const value = await fixture();
  const typedRequest = createWriterLaunchAttemptOperationRequest({
    expectedSession: value.expectedSession,
    generation: value.generation,
    measuredImage: {
      projection: value.reserved.projection,
      runtimeIdentity: value.reserved.runtimeIdentity,
    },
    supervisor: { contractVersion: 1, supervisorId: SUPERVISOR_ID },
  });
  value.authority.seed(typedRequest, "prepared");

  const result = await value.facade.reconcileLaunchAttempt({
    launchAttemptId: LAUNCH_ATTEMPT_ID,
  });
  assert.equal(result.status, "cancelled-before-dispatch");
  assert.equal(result.evidence, null);
  assert.equal(result.writer, null);
  assert.equal(value.authority.calls.cancel, 1);
  assert.equal(value.inspectionCount, 1);
  assert.equal(value.launchCalls, 0);
  assert.equal(value.reconcileCalls, 0);
});

test("prepared handoff recovery preserves the attempt for capability-bearing launch", async () => {
  const value = await fixture();
  seedPreparedLaunchHandoff(value);
  const preparedReceipt = clone(value.authority.receipt());

  await assert.rejects(
    value.facade.reconcileLaunchAttempt({
      launchAttemptId: LAUNCH_ATTEMPT_ID,
    }),
    assertLauncherError("logical_writer_launch_outcome_uncertain"),
  );

  assert.equal(value.authority.state, "prepared");
  assert.deepEqual(value.authority.receipt(), preparedReceipt);
  assert.equal(value.authority.calls.cancel, 1);
  assert.equal(value.authority.calls.claim, 0);
  assert.equal(value.authority.calls.reserve, 0);
  assert.equal(value.launchCalls, 0);
  assert.equal(value.reconcileCalls, 0);
  assert.equal(value.inspectionCount, 1);

  const result = await value.facade.runPreparedLaunch(preparedRunInput(value));
  assert.equal(result.status, "started");
  assert.equal(result.writer !== null, true);
  assert.equal(value.authority.calls.cancel, 1);
  assert.equal(value.authority.calls.claim, 1);
  assert.equal(value.authority.calls.reserve, 0);
  assert.equal(value.launchCalls, 1);
});

test("authority-confirmed expired prepared handoff recovery cancels without dispatch", async () => {
  const value = await fixture();
  seedPreparedLaunchHandoff(value);
  value.authority.behaviour.cancelPreparedHandoffConflict = false;

  const result = await value.facade.reconcileLaunchAttempt({
    launchAttemptId: LAUNCH_ATTEMPT_ID,
  });

  assert.equal(result.status, "cancelled-before-dispatch");
  assert.equal(result.writer, null);
  assert.equal(
    value.authority.lastCancelInput.reason,
    WRITER_LAUNCH_PRE_DISPATCH_CANCELLATION_REASON,
  );
  assert.equal(value.authority.calls.cancel, 1);
  assert.equal(value.authority.calls.claim, 0);
  assert.equal(value.launchCalls, 0);
  assert.equal(value.reconcileCalls, 0);
});

for (const state of ["starting", "uncertain"]) {
  test(`${state} recovery never invokes launchWriter`, async () => {
    const value = await fixture({ reconcileStatus: "complete-stopped" });
    const typedRequest = createWriterLaunchAttemptOperationRequest({
      expectedSession: value.expectedSession,
      generation: value.generation,
      measuredImage: {
        projection: value.reserved.projection,
        runtimeIdentity: value.reserved.runtimeIdentity,
      },
      supervisor: { contractVersion: 1, supervisorId: SUPERVISOR_ID },
    });
    value.authority.seed(typedRequest, state);

    const result = await value.facade.reconcileLaunchAttempt({
      launchAttemptId: LAUNCH_ATTEMPT_ID,
    });
    assert.equal(result.status, "complete-stopped");
    assert.equal(result.writer, null);
    assert.equal(value.launchCalls, 0);
    assert.equal(value.reconcileCalls, 1);
    assert.equal(value.inspectionCount, 1);
    assert.equal(value.authority.calls.markUncertain, state === "starting" ? 1 : 0);
    assert.equal(value.authority.supervisorStateGcAuthorizations.size, 0);
  });

  test(`${state} recovery authorizes GC only for a retired rev4 receipt`, async () => {
    const value = await fixture({
      reconcileStatus: "complete-stopped",
      reconcileTerminalRecord: true,
    });
    const typedRequest = createWriterLaunchAttemptOperationRequest({
      expectedSession: value.expectedSession,
      generation: value.generation,
      measuredImage: {
        projection: value.reserved.projection,
        runtimeIdentity: value.reserved.runtimeIdentity,
      },
      supervisor: { contractVersion: 1, supervisorId: SUPERVISOR_ID },
    });
    value.authority.seed(typedRequest, state);

    const result = await value.facade.reconcileLaunchAttempt({
      launchAttemptId: LAUNCH_ATTEMPT_ID,
    });
    assert.equal(result.status, "complete-stopped");
    assert.equal(result.writer, null);
    assert.equal(value.launchCalls, 0);
    assert.equal(value.reconcileCalls, 1);
    assert.equal(value.authority.supervisorStateGcAuthorizations.size, 1);
    const authorization =
      await value.authority.readWriterSupervisorStateGcAuthorization({
        stateOwnerId: STATE_OWNER_ID,
        terminalOperationId: LAUNCH_ATTEMPT_ID,
      });
    assert.equal(authorization.launchAttemptId, LAUNCH_ATTEMPT_ID);
    assert.equal(authorization.terminalKind, WRITER_LAUNCH_ATTEMPT_OPERATION_KIND);
    assert.equal(authorization.terminalRecord.status, "stopped");
    assert.equal(authorization.terminalRecord.revision, 4);
    assert.equal(
      authorization.terminalRecord.requestSha256,
      value.reconcileContext.operation.requestSha256,
    );
  });
}

test("reconciliation rejects terminal records outside the exact retired rev4 relation", async (t) => {
  for (const [name, receipt] of [
    [
      "not-started with a terminal record",
      {
        evidence: evidence(LAUNCH_ATTEMPT_ID, "not-started"),
        receiptVersion: LOGICAL_WRITER_RECONCILE_RECEIPT_VERSION,
        terminalRecord: supervisorTerminalRecord(),
      },
    ],
    [
      "complete-stopped with another attempt record",
      {
        evidence: evidence(LAUNCH_ATTEMPT_ID, "complete-stopped"),
        receiptVersion: LOGICAL_WRITER_RECONCILE_RECEIPT_VERSION,
        terminalRecord: supervisorTerminalRecord({
          launchAttemptId: "writer-launch-attempt-other",
        }),
      },
    ],
    [
      "missing terminal record key",
      {
        evidence: evidence(LAUNCH_ATTEMPT_ID, "complete-stopped"),
        receiptVersion: LOGICAL_WRITER_RECONCILE_RECEIPT_VERSION,
      },
    ],
  ]) {
    await t.test(name, async () => {
      const value = await fixture();
      const typedRequest = createWriterLaunchAttemptOperationRequest({
        expectedSession: value.expectedSession,
        generation: value.generation,
        measuredImage: {
          projection: value.reserved.projection,
          runtimeIdentity: value.reserved.runtimeIdentity,
        },
        supervisor: { contractVersion: 1, supervisorId: SUPERVISOR_ID },
      });
      value.authority.seed(typedRequest, "uncertain");
      const facade = createPostgresLogicalWriterLauncher({
        authority: value.authority,
        imagePlanBinding: value.imagePlanBinding,
        operationGuard: value.operationGuard,
        stoppedWriterCoordinator: value.stoppedWriterCoordinator,
        supervisor: {
          ...value.supervisor,
          async reconcileWriterLaunch() {
            return receipt;
          },
        },
      });

      await assert.rejects(
        facade.reconcileLaunchAttempt({ launchAttemptId: LAUNCH_ATTEMPT_ID }),
        assertLauncherError("logical_writer_launch_outcome_uncertain"),
      );
      assert.equal(value.authority.state, "uncertain");
      assert.equal(value.authority.supervisorStateGcAuthorizations.size, 0);
    });
  }
});

test("foreign configured owner reconciliation fails before physical supervisor I/O", async () => {
  const value = await fixture({ reconcileStatus: "complete-stopped" });
  const typedRequest = createWriterLaunchAttemptOperationRequest({
    expectedSession: value.expectedSession,
    generation: value.generation,
    measuredImage: {
      projection: value.reserved.projection,
      runtimeIdentity: value.reserved.runtimeIdentity,
    },
    supervisor: { contractVersion: 1, supervisorId: SUPERVISOR_ID },
  });
  value.authority.seed(typedRequest, "starting");
  const foreignFacade = createPostgresLogicalWriterLauncher({
    authority: value.authority,
    imagePlanBinding: value.imagePlanBinding,
    operationGuard: value.operationGuard,
    stoppedWriterCoordinator: value.stoppedWriterCoordinator,
    supervisor: {
      ...value.supervisor,
      stateOwnerId: OTHER_STATE_OWNER_ID,
    },
  });

  await assert.rejects(
    foreignFacade.reconcileLaunchAttempt({
      launchAttemptId: LAUNCH_ATTEMPT_ID,
    }),
    assertLauncherError("logical_writer_launch_outcome_uncertain"),
  );
  assert.equal(value.authority.calls.read, 1);
  assert.equal(value.launchCalls, 0);
  assert.equal(value.reconcileCalls, 0);

  await assert.rejects(
    foreignFacade.reconcileLaunchAttempt({
      launchAttemptId: LAUNCH_ATTEMPT_ID,
      stateOwnerId: STATE_OWNER_ID,
    }),
    assertLauncherError("invalid_logical_writer_launch_request"),
  );
  assert.equal(value.authority.calls.read, 1);
  assert.equal(value.reconcileCalls, 0);
});

test("a committed started attempt without the original local handle requires stop or fence", async () => {
  const value = await fixture();
  const typedRequest = createWriterLaunchAttemptOperationRequest({
    expectedSession: value.expectedSession,
    generation: value.generation,
    measuredImage: {
      projection: value.reserved.projection,
      runtimeIdentity: value.reserved.runtimeIdentity,
    },
    supervisor: { contractVersion: 1, supervisorId: SUPERVISOR_ID },
  });
  value.authority.seed(
    typedRequest,
    "committed",
    evidence(LAUNCH_ATTEMPT_ID, "started"),
  );

  await assert.rejects(
    value.facade.reconcileLaunchAttempt({
      launchAttemptId: LAUNCH_ATTEMPT_ID,
    }),
    assertLauncherError("logical_writer_handle_unavailable"),
  );
  assert.equal(value.launchCalls, 0);
  assert.equal(value.reconcileCalls, 0);
});

test("post-launch callback ambiguity marks uncertain once and recovery never relaunches", async () => {
  const value = await fixture({ reconcileStatus: "not-started" });
  const throwingLaunch = async () => {
    value.events.push("supervisor.launch-throws");
    throw new Error("ambiguous launch");
  };
  const facade = createPostgresLogicalWriterLauncher({
    authority: value.authority,
    imagePlanBinding: value.imagePlanBinding,
    operationGuard: value.operationGuard,
    stoppedWriterCoordinator: value.stoppedWriterCoordinator,
    supervisor: {
      contractVersion: LOGICAL_WRITER_SUPERVISOR_CONTRACT_VERSION,
      stateOwnerId: STATE_OWNER_ID,
      supervisorId: SUPERVISOR_ID,
      launchWriter: throwingLaunch,
      reconcileWriterLaunch: async () => ({
        receiptVersion: LOGICAL_WRITER_RECONCILE_RECEIPT_VERSION,
        evidence: evidence(LAUNCH_ATTEMPT_ID, "not-started"),
        terminalRecord: null,
      }),
    },
  });

  await assert.rejects(
    facade.runLaunch(runInput(value)),
    assertLauncherError("logical_writer_launch_outcome_uncertain"),
  );
  assert.equal(value.authority.state, "uncertain");
  assert.equal(value.authority.calls.markUncertain, 1);
  const result = await facade.reconcileLaunchAttempt({
    launchAttemptId: LAUNCH_ATTEMPT_ID,
  });
  assert.equal(result.status, "not-started");
  assert.equal(value.authority.calls.markUncertain, 1);
  assert.equal(
    value.events.filter((entry) => entry === "supervisor.launch-throws").length,
    1,
  );
});

test("recovery retries started finalization for an exact provisional writer before external reconcile", async () => {
  const value = await fixture({ reconcileStatus: "complete-stopped" });
  value.authority.behaviour.finalizeStartedThrowBeforeCommit = true;
  value.authority.behaviour.readThrows = true;

  await assert.rejects(
    value.facade.runLaunch(runInput(value)),
    assertLauncherError("logical_writer_launch_outcome_uncertain"),
  );
  assert.equal(value.authority.state, "uncertain");
  assert.equal(value.launchCalls, 1);
  assert.equal(value.reconcileCalls, 0);
  value.authority.behaviour.readThrows = false;
  value.authority.behaviour.finalizeStartedThrowBeforeCommit = false;
  const result = await value.facade.reconcileLaunchAttempt({
    launchAttemptId: LAUNCH_ATTEMPT_ID,
  });
  assert.equal(result.status, "started");
  assert.equal(value.reconcileCalls, 0);
  const resolved = value.facade.resolveStoppedWriter(resolverInput(value));
  assert.strictEqual(resolved.writer, result.writer);
  assert.throws(
    () =>
      value.stoppedWriterCoordinator.registerWriter({
        attachment: attachment(),
        canonicalLease: lease(),
        processIncarnationId: "replacement-process-001",
        stopWriter: async () => STOPPED_WRITER_STOP_CONFIRMED,
        writerIncarnationId: "replacement-writer-001",
      }),
    (error) =>
      error instanceof StoppedWriterCapabilityError &&
      error.code === "writer_state_conflict",
  );
});

test("durable stopped readback cannot hide an exact local provisional writer", async () => {
  const value = await fixture();
  value.authority.behaviour.finalizeStartedThrowBeforeCommit = true;
  value.authority.behaviour.readThrows = true;

  await assert.rejects(
    value.facade.runLaunch(runInput(value)),
    assertLauncherError("logical_writer_launch_outcome_uncertain"),
  );
  assert.equal(value.authority.state, "uncertain");
  value.authority.behaviour.readThrows = false;
  await value.authority.finalizeWriterLaunchAttemptStopped({
    evidence: evidence(LAUNCH_ATTEMPT_ID, "complete-stopped"),
  });

  await assert.rejects(
    value.facade.reconcileLaunchAttempt({
      launchAttemptId: LAUNCH_ATTEMPT_ID,
    }),
    assertLauncherError("logical_writer_launch_outcome_uncertain"),
  );
  assert.equal(value.reconcileCalls, 0);
  assert.throws(
    () => value.facade.resolveStoppedWriter(resolverInput(value)),
    assertLauncherError("invalid_logical_writer_launch_request"),
  );
  assert.throws(
    () =>
      value.stoppedWriterCoordinator.registerWriter({
        attachment: attachment(),
        canonicalLease: lease(),
        processIncarnationId: "replacement-process-002",
        stopWriter: async () => STOPPED_WRITER_STOP_CONFIRMED,
        writerIncarnationId: "replacement-writer-002",
      }),
    (error) =>
      error instanceof StoppedWriterCapabilityError &&
      error.code === "writer_state_conflict",
  );
});

test("rejects hostile proxy inputs and unsafe callback receipts without dispatch replay", async () => {
  const value = await fixture();
  assert.throws(
    () =>
      createPostgresLogicalWriterLauncher({
        authority: value.authority,
        imagePlanBinding: value.imagePlanBinding,
        operationGuard: value.operationGuard,
        stoppedWriterCoordinator: value.stoppedWriterCoordinator,
        supervisor: {
          ...value.supervisor,
          contractVersion: LOGICAL_WRITER_LAUNCH_CONTRACT_VERSION,
        },
      }),
    assertLauncherError("invalid_logical_writer_launch_request"),
  );
  let traps = 0;
  const hostile = new Proxy(
    {},
    {
      get() {
        traps += 1;
        throw new Error("secret proxy trap");
      },
    },
  );
  await assert.rejects(
    value.facade.runLaunch(hostile),
    assertLauncherError("invalid_logical_writer_launch_request"),
  );
  assert.equal(traps, 0);
  assert.equal(value.authority.calls.reserve, 0);

  const invalidReceiptFacade = createPostgresLogicalWriterLauncher({
    authority: value.authority,
    imagePlanBinding: value.imagePlanBinding,
    operationGuard: value.operationGuard,
    stoppedWriterCoordinator: value.stoppedWriterCoordinator,
    supervisor: {
      contractVersion: LOGICAL_WRITER_SUPERVISOR_CONTRACT_VERSION,
      stateOwnerId: STATE_OWNER_ID,
      supervisorId: SUPERVISOR_ID,
      launchWriter: async () =>
        new Proxy(
          {},
          {
            ownKeys() {
              throw new Error("hostile receipt");
            },
          },
        ),
      reconcileWriterLaunch: async () => ({
        receiptVersion: LOGICAL_WRITER_RECONCILE_RECEIPT_VERSION,
        evidence: evidence(LAUNCH_ATTEMPT_ID, "not-started"),
        terminalRecord: null,
      }),
    },
  });
  await assert.rejects(
    invalidReceiptFacade.runLaunch(runInput(value)),
    assertLauncherError("logical_writer_launch_outcome_uncertain"),
  );
  assert.equal(value.authority.calls.markUncertain, 1);
});
