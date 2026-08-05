import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import test from "node:test";

import {
  PlatformImageReservationCoordinator,
} from "../src/platform-image-reservation.mjs";
import {
  RESTORE_DESTINATION_GENERATION_OPERATION_KIND,
  SESSION_AUTHORITY_DOCUMENT_VERSION,
  SESSION_OPERATION_CONFLICT_CLASS,
  WRITER_LAUNCH_ATTEMPT_OPERATION_KIND,
  WRITER_LAUNCH_PRE_DISPATCH_CANCELLATION_REASON,
  WRITER_LAUNCH_STOP_OPERATION_KIND,
  assertCommittedWriterLaunchStopTransitionProof,
  createWriterLaunchAttemptOperationRequest,
  createWriterLaunchStopOperationRequest,
} from "../src/postgres-session-authority.mjs";
import {
  LOGICAL_WRITER_LAUNCH_CONTRACT_VERSION,
  PostgresLogicalWriterLauncherError,
  createPostgresLogicalWriterLauncher,
  derivePostgresLogicalWriterStopOperationId,
} from "../src/postgres-logical-writer-launcher.mjs";
import {
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
const GENERATION_ID = "restore-generation-001";
const CHECKPOINT_ID = "checkpoint-001";
const ARTIFACT_ID = "artifact-001";
const CAPTURE_OPERATION_ID = "checkpoint-capture-operation-001";
const SUPERVISOR_ID = "supervisor-001";
const PROCESS_INCARNATION_ID = "process-incarnation-001";
const WRITER_INCARNATION_ID = "writer-incarnation-001";
const PROOF_ID = "supervisor-proof-001";
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

class MemoryLaunchAuthority {
  constructor({ events, expectedSession, generation }) {
    this.events = events;
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
    this.behaviour = Object.create(null);
    this.claimReceiptMutation = null;
    this.calls = {
      cancel: 0,
      claim: 0,
      finalizeStarted: 0,
      finalizeStopped: 0,
      finalizeWriterStopped: 0,
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
      contractVersion: 1,
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
        const expectedSession = canonicalAuthoritySession(
          input.expectedSession,
        );
        this.stopBaseInput = {
          expectedSession,
          kind: input.kind,
          operationId: input.operationId,
          request: createWriterLaunchStopOperationRequest({
            expectedSession,
          }),
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

  async reconcileOperation(input) {
    this.calls.stopReconcile += 1;
    this.events.push("authority.reconcile-stop");
    if (this.behaviour.stopReconcileThrowsOnce) {
      this.behaviour.stopReconcileThrowsOnce = false;
      throw new Error("stop reconcile unavailable");
    }
    assert.equal(input.kind, WRITER_LAUNCH_STOP_OPERATION_KIND);
    if (this.stopState === "absent") {
      return {
        operation: null,
        reservation: null,
        session: canonicalAuthoritySession(input.expectedSession),
        status: "absent",
      };
    }
    assert.deepEqual(
      JSON.parse(jsonStringify(input)),
      JSON.parse(jsonStringify(this.stopBaseInput)),
    );
    const operation = this.stopOperation();
    const reservation = this.stopReservation(operation);
    return {
      operation,
      reservation,
      session: this.stopSession(operation, reservation),
      status: operation.state,
    };
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
    assert.deepEqual(
      JSON.parse(jsonStringify(input)),
      JSON.parse(
        jsonStringify({
          ...this.stopBaseInput,
          expectedOperationRevision: "0",
        }),
      ),
    );
    if (this.stopState !== "prepared") {
      const operation = this.stopOperation();
      const reservation = this.stopReservation(operation);
      return {
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
    this.stopState = "starting";
    if (this.behaviour.stopClaimThrowAfterCommit) {
      throw new Error("lost stop claim acknowledgement");
    }
    const operation = this.stopOperation();
    const reservation = this.stopReservation(operation);
    return {
      dispatchGranted: true,
      launch: clone(this.stopBaseInput.request.launch),
      operation,
      reservation,
      session: this.stopSession(operation, reservation),
      status: "starting",
      stop: this.stopRecord(operation),
    };
  }

  async readWriterLaunchAttempt() {
    this.calls.read += 1;
    this.events.push("authority.read-attempt");
    if (this.behaviour.readThrows) throw new Error("read unavailable");
    if (this.state === "absent") throw new Error("attempt absent");
    const receipt = this.receipt();
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

  async finalizeWriterLaunchStopped(input) {
    this.calls.finalizeWriterStopped += 1;
    this.events.push("authority.finalize-writer-stopped");
    assert.deepEqual(
      JSON.parse(jsonStringify(input)),
      JSON.parse(
        jsonStringify({
          ...this.stopBaseInput,
          evidence: input.evidence,
          expectedOperationRevision: "1",
        }),
      ),
    );
    if (this.stopState === "committed") {
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
    if (this.behaviour.stopFinalizeThrowAfterCommit) {
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
      return await callback(
        Object.freeze({
          assertHeld: async () => {
            this.assertions += 1;
            this.events.push("guard.assert-held");
          },
        }),
      );
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
let hostileLaunchAdmissionCalls = 0;
let hostileRetireWriterCalls = 0;
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

  retireWriter() {
    hostileRetireWriterCalls += 1;
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
  reconcileStatus = "not-started",
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
  const operationGuard = new MemoryOperationGuard(events);
  const imageReservations = new PlatformImageReservationCoordinator();
  let inspectionCount = 0;
  let inspectionFailureAt = null;
  const inspectCodex = async () => {
    inspectionCount += 1;
    events.push(`image.inspect:${inspectionCount}`);
    if (inspectionCount === inspectionFailureAt) {
      throw new Error("image inspection unavailable");
    }
    return {
      codexBinaryPath: "/opt/portable-codex/bin/codex",
      codexBinarySha256: "b".repeat(64),
      codexVersion: CODEX_VERSION,
    };
  };
  const reserved = await imageReservations.reservePlatformImage({
    configBytes: image.configBytes,
    descriptor: image.descriptor,
    inspectCodex,
    sessionManifest: image.manifest,
  });
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
    return STOPPED_WRITER_STOP_CONFIRMED;
  };
  const launchWriter = async (context) => {
    launchCalls += 1;
    events.push("supervisor.launch");
    launchContext = context;
    return {
      receiptVersion: 1,
      evidence: evidence(context.attempt.launchAttemptId, launchStatus),
      stopWriter: launchStatus === "started" ? supervisorStopWriter : null,
    };
  };
  const reconcileWriterLaunch = async (context) => {
    reconcileCalls += 1;
    events.push("supervisor.reconcile");
    reconcileContext = context;
    return {
      receiptVersion: 1,
      evidence: evidence(LAUNCH_ATTEMPT_ID, reconcileStatus),
    };
  };
  const supervisor = {
    contractVersion: 1,
    supervisorId: SUPERVISOR_ID,
    launchWriter,
    reconcileWriterLaunch,
  };
  const facade = createPostgresLogicalWriterLauncher({
    authority,
    imageReservations,
    operationGuard,
    stoppedWriterCoordinator,
    supervisor,
  });
  return {
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
    imageReservation: {
      configBytes: image.configBytes,
      descriptor: image.descriptor,
      inspectCodex,
      reservation: reserved.reservation,
    },
    imageReservations,
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
    const reserved = await value.imageReservations.reservePlatformImage({
      configBytes: value.imageReservation.configBytes,
      descriptor: value.imageReservation.descriptor,
      inspectCodex: value.imageReservation.inspectCodex,
      sessionManifest: value.image.manifest,
    });
    imageReservation = {
      configBytes: value.imageReservation.configBytes,
      descriptor: value.imageReservation.descriptor,
      inspectCodex: value.imageReservation.inspectCodex,
      reservation: reserved.reservation,
    };
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
  assert.deepEqual(Reflect.ownKeys(value.facade).sort(), [
    "prepareLaunchIntent",
    "reconcileLaunchAttempt",
    "resolveStoppedWriter",
    "retireStoppedWriter",
    "runLaunch",
    "runPreparedLaunch",
    "stopWriterForCapture",
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

test("same-process stop claim acknowledgement loss proves starting before physical stop", async () => {
  const value = await fixture();
  await value.facade.runLaunch(runInput(value));
  const readsBeforeStop = value.events.filter(
    (entry) => entry === "authority.read-session",
  ).length;
  value.authority.behaviour.stopClaimThrowAfterCommit = true;

  const stopped = await value.facade.stopWriterForCapture(
    resolverInput(value),
  );

  assert.equal(stopped.stop.finalized, true);
  assert.equal(value.supervisorStopCalls, 1);
  assert.equal(value.authority.calls.stopClaim, 1);
  assert.equal(
    value.events.filter((entry) => entry === "authority.read-session").length -
      readsBeforeStop,
    1,
  );
  assert.equal(value.authority.calls.stopReconcile, 1);
  assert.equal(value.authority.calls.markUncertain, 0);
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

test("exact stop replay resumes acknowledged starting only before physical stop", async () => {
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

  const stopped = await value.facade.stopWriterForCapture(resolverInput(value));
  assert.equal(stopped.stop.status, "committed");
  assert.equal(value.authority.calls.stopClaim, 1);
  assert.equal(value.authority.calls.stopReconcile, 2);
  assert.equal(value.supervisorStopCalls, 1);
});

test("a starting stop without this record's claim witness never dispatches physical stop", async () => {
  const value = await fixture();
  await value.facade.runLaunch(runInput(value));
  value.authority.behaviour.stopReserveThrowAfterCommit = true;
  value.authority.behaviour.stopReconcileThrowsOnce = true;

  await assert.rejects(
    value.facade.stopWriterForCapture(resolverInput(value)),
    assertLauncherError("logical_writer_launch_outcome_uncertain"),
  );
  assert.equal(value.authority.stopState, "prepared");
  value.authority.stopState = "starting";

  await assert.rejects(
    value.facade.stopWriterForCapture(resolverInput(value)),
    assertLauncherError("logical_writer_launch_outcome_uncertain"),
  );
  assert.equal(value.authority.calls.stopClaim, 0);
  assert.equal(value.supervisorStopCalls, 0);
});

test("malformed committed stop proof never yields a capability", async () => {
  const value = await fixture();
  await value.facade.runLaunch(runInput(value));
  value.authority.stopFinalizationMutation = (receipt) => {
    receipt.session.document.lastOperation.resultSha256 = "d".repeat(64);
    return receipt;
  };

  await assert.rejects(
    value.facade.stopWriterForCapture(resolverInput(value)),
    assertLauncherError("logical_writer_launch_outcome_uncertain"),
  );
  assert.equal(value.supervisorStopCalls, 1);
  assert.equal(value.authority.calls.finalizeWriterStopped, 2);
  assert.equal(value.authority.calls.markUncertain, 1);
  assert.throws(
    () => value.facade.resolveStoppedWriter(resolverInput(value)),
    assertLauncherError("invalid_logical_writer_launch_request"),
  );
});

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

test("runs an atomically prepared launch without reserving a second attempt", async () => {
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

test("cold launcher resumes a prepared attempt with a fresh equivalent image reservation", async () => {
  const value = await fixture();
  const intent = await value.facade.prepareLaunchIntent(
    prepareIntentInput(value),
  );
  seedPreparedLaunchHandoff(value, {
    measuredImage: intent.measuredImage,
    supervisor: intent.supervisor,
  });

  const freshImageReservations = new PlatformImageReservationCoordinator();
  const fresh = await freshImageReservations.reservePlatformImage({
    configBytes: value.imageReservation.configBytes,
    descriptor: value.imageReservation.descriptor,
    inspectCodex: value.imageReservation.inspectCodex,
    sessionManifest: value.image.manifest,
  });
  const freshImageReservation = {
    configBytes: value.imageReservation.configBytes,
    descriptor: value.imageReservation.descriptor,
    inspectCodex: value.imageReservation.inspectCodex,
    reservation: fresh.reservation,
  };
  const coldFacade = createPostgresLogicalWriterLauncher({
    authority: value.authority,
    imageReservations: freshImageReservations,
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
    await value.imageReservations.revalidateReservation(
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
  const fresh = await value.imageReservations.reservePlatformImage({
    configBytes: value.imageReservation.configBytes,
    descriptor: value.imageReservation.descriptor,
    inspectCodex: value.imageReservation.inspectCodex,
    sessionManifest: value.image.manifest,
  });
  const retried = await value.facade.runPreparedLaunch(
    preparedRunInput(value, {
      imageReservation: {
        ...value.imageReservation,
        reservation: fresh.reservation,
      },
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

  const stillIssued = await value.imageReservations.revalidateReservation(
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

  await value.imageReservations.revalidateReservation(
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

  await value.imageReservations.revalidateReservation(
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

  await value.imageReservations.revalidateReservation(
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

    await value.imageReservations.revalidateReservation(
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
  const launchWriter = async () => {
    Array.isArray = poison("Array.isArray");
    JSON.stringify = poison("JSON.stringify");
    Object.freeze = poison("Object.freeze");
    Object.getOwnPropertyDescriptor = poison("Object.getOwnPropertyDescriptor");
    Object.getPrototypeOf = poison("Object.getPrototypeOf");
    Object.hasOwn = poison("Object.hasOwn");
    Reflect.apply = poison("Reflect.apply");
    Reflect.ownKeys = poison("Reflect.ownKeys");
    return {
      receiptVersion: 1,
      evidence: evidence(LAUNCH_ATTEMPT_ID, "started"),
      stopWriter: async () => STOPPED_WRITER_STOP_CONFIRMED,
    };
  };
  const facade = createPostgresLogicalWriterLauncher({
    authority: value.authority,
    imageReservations: value.imageReservations,
    operationGuard: value.operationGuard,
    stoppedWriterCoordinator: value.stoppedWriterCoordinator,
    supervisor: {
      contractVersion: 1,
      supervisorId: SUPERVISOR_ID,
      launchWriter,
      reconcileWriterLaunch: async () => ({
        receiptVersion: 1,
        evidence: evidence(LAUNCH_ATTEMPT_ID, "not-started"),
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
  const freshReservation = await value.imageReservations.reservePlatformImage({
    configBytes: value.imageReservation.configBytes,
    descriptor: value.imageReservation.descriptor,
    inspectCodex: value.imageReservation.inspectCodex,
    sessionManifest: value.image.manifest,
  });
  const inspectionsBeforeReplay = value.inspectionCount;

  await assert.rejects(
    value.facade.runLaunch({
      generation: generationSnapshot({
        generationId: "restore-generation-002",
      }),
      imageReservation: {
        configBytes: value.imageReservation.configBytes,
        descriptor: value.imageReservation.descriptor,
        inspectCodex: value.imageReservation.inspectCodex,
        reservation: freshReservation.reservation,
      },
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
  const fresh = await value.imageReservations.reservePlatformImage({
    configBytes: value.imageReservation.configBytes,
    descriptor: value.imageReservation.descriptor,
    inspectCodex: value.imageReservation.inspectCodex,
    sessionManifest: value.image.manifest,
  });
  const result = await value.facade.runLaunch(
    runInput(value, {
      imageReservation: {
        ...value.imageReservation,
        reservation: fresh.reservation,
      },
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
  });
}

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
    imageReservations: value.imageReservations,
    operationGuard: value.operationGuard,
    stoppedWriterCoordinator: value.stoppedWriterCoordinator,
    supervisor: {
      contractVersion: 1,
      supervisorId: SUPERVISOR_ID,
      launchWriter: throwingLaunch,
      reconcileWriterLaunch: async () => ({
        receiptVersion: 1,
        evidence: evidence(LAUNCH_ATTEMPT_ID, "not-started"),
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
    imageReservations: value.imageReservations,
    operationGuard: value.operationGuard,
    stoppedWriterCoordinator: value.stoppedWriterCoordinator,
    supervisor: {
      contractVersion: 1,
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
        receiptVersion: 1,
        evidence: evidence(LAUNCH_ATTEMPT_ID, "not-started"),
      }),
    },
  });
  await assert.rejects(
    invalidReceiptFacade.runLaunch(runInput(value)),
    assertLauncherError("logical_writer_launch_outcome_uncertain"),
  );
  assert.equal(value.authority.calls.markUncertain, 1);
});
