import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import test from "node:test";

import {
  PlatformImageReservationCoordinator,
} from "../src/platform-image-reservation.mjs";
import {
  SESSION_AUTHORITY_DOCUMENT_VERSION,
  SESSION_OPERATION_CONFLICT_CLASS,
  WRITER_LAUNCH_ATTEMPT_OPERATION_KIND,
  createWriterLaunchAttemptOperationRequest,
} from "../src/postgres-session-authority.mjs";
import {
  LOGICAL_WRITER_LAUNCH_CONTRACT_VERSION,
  PostgresLogicalWriterLauncherError,
  createPostgresLogicalWriterLauncher,
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

function digest(bytes) {
  return `sha256:${createHash("sha256").update(bytes).digest("hex")}`;
}

function jsonBytes(value) {
  return Buffer.from(JSON.stringify(value), "utf8");
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
    resultSha256: "f".repeat(64),
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
    this.behaviour = Object.create(null);
    this.calls = {
      cancel: 0,
      claim: 0,
      finalizeStarted: 0,
      finalizeStopped: 0,
      markUncertain: 0,
      read: 0,
      reserve: 0,
    };
    this.beforeFinalize = null;
  }

  beginNextAttempt({ expectedSession, generation }) {
    this.expectedSession = clone(expectedSession);
    this.generation = clone(generation);
    this.baseInput = null;
    this.state = "absent";
    this.result = null;
    this.terminalRevision = null;
    this.behaviour = Object.create(null);
    this.beforeFinalize = null;
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
          reason: "launch-dispatch-not-started",
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
    return {
      attachmentId: operation.request.attachment.attachmentId,
      attachmentSha256: "1".repeat(64),
      contractVersion: 1,
      fencingEpoch: operation.request.fencingEpoch,
      generation: clone(operation.request.generation),
      launchAttemptId: operation.operationId,
      launchResultSha256: "2".repeat(64),
      leaseId: operation.request.lease.leaseId,
      leaseSha256: "3".repeat(64),
      measuredImageSha256: "4".repeat(64),
      processIncarnationId: launchEvidence.processIncarnationId,
      startedAt: operation.updatedAt,
      supervisorId: launchEvidence.supervisorId,
      supervisorProofId: launchEvidence.proofId,
      writerIncarnationId: launchEvidence.writerIncarnationId,
    };
  }

  session(operation = this.operation(), reservation = this.reservation(operation)) {
    const document = clone(this.expectedSession.document);
    document.documentVersion = SESSION_AUTHORITY_DOCUMENT_VERSION;
    if (operation.state === "committed") {
      document.activeOperation = null;
      document.lastOperation = terminalPointer(operation, reservation);
      document.launch = this.launchPointer(operation);
    } else {
      document.activeOperation = activePointer(operation, reservation);
      document.launch = null;
    }
    return {
      sessionId: this.expectedSession.sessionId,
      revision: (
        BigInt(this.expectedSession.revision) + BigInt(operation.revision) + 1n
      ).toString(),
      document,
      createdAt: this.expectedSession.createdAt,
      updatedAt: operation.updatedAt,
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

  async readSession() {
    this.events.push("authority.read-session");
    return clone(this.expectedSession);
  }

  async reserveOperation(input) {
    this.calls.reserve += 1;
    this.events.push("authority.reserve");
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

  async claimWriterLaunchAttemptDispatch() {
    this.calls.claim += 1;
    this.events.push("authority.claim");
    assert.equal(this.state, "prepared");
    this.state = "starting";
    if (this.behaviour.claimThrowAfterCommit) throw new Error("lost claim ack");
    const operation = this.operation();
    const reservation = this.reservation(operation);
    return {
      attempt: this.attempt(operation),
      authorityNow: "2026-08-04T12:00:02.500Z",
      dispatchGranted: true,
      generation: clone(this.generation),
      operation,
      reservation,
      session: this.session(operation, reservation),
      status: "starting",
    };
  }

  async readWriterLaunchAttempt() {
    this.calls.read += 1;
    this.events.push("authority.read-attempt");
    if (this.behaviour.readThrows) throw new Error("read unavailable");
    if (this.state === "absent") throw new Error("attempt absent");
    return this.receipt();
  }

  async markOperationUncertain() {
    this.calls.markUncertain += 1;
    this.events.push("authority.mark-uncertain");
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

  async cancelPreparedOperation() {
    this.calls.cancel += 1;
    this.events.push("authority.cancel");
    assert.equal(this.state, "prepared");
    this.state = "committed";
    this.terminalRevision = "1";
    this.result = {
      resultVersion: 1,
      outcome: "cancelled-before-dispatch",
      reason: "launch-dispatch-not-started",
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
}

class MemoryOperationGuard {
  constructor(events) {
    this.events = events;
    this.calls = 0;
    this.assertions = 0;
  }

  async runExclusive(operationId, callback) {
    this.calls += 1;
    this.events.push(`guard.enter:${operationId}`);
    const result = await callback(
      Object.freeze({
        assertHeld: async () => {
          this.assertions += 1;
          this.events.push("guard.assert-held");
        },
      }),
    );
    this.events.push(`guard.exit:${operationId}`);
    return result;
  }
}

let hostileRegisterWriterCalls = 0;

class HostileStoppedWriterCoordinator extends StoppedWriterCapabilityCoordinator {
  registerWriter() {
    hostileRegisterWriterCalls += 1;
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

function assertLauncherError(code) {
  return (error) => {
    assert.ok(error instanceof PostgresLogicalWriterLauncherError);
    assert.equal(error.code, code);
    assert.equal(error.retryable, false);
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
  const facade = createPostgresLogicalWriterLauncher({
    authority,
    imageReservations,
    operationGuard,
    stoppedWriterCoordinator,
    supervisor: {
      contractVersion: 1,
      supervisorId: SUPERVISOR_ID,
      launchWriter,
      reconcileWriterLaunch,
    },
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

function resolverInput(value, overrides = {}) {
  return {
    attachment: attachment(),
    checkpoint: checkpoint(value.image.manifest.runtime.imageDigest),
    request: captureRequest(),
    ...overrides,
  };
}

async function prepareLaunchCycle(value, index) {
  const ordinal = String(index + 1).padStart(3, "0");
  const launchAttemptId = `writer-launch-attempt-${ordinal}`;
  const writerLease = lease({
    fencingEpoch: String(11 + index),
    leaseId: `lease-${ordinal}`,
  });
  const mounted = attachment(writerLease, {
    operationId: `operation-attach-${ordinal}`,
    proofId: `proof-attachment-${ordinal}`,
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
    "reconcileLaunchAttempt",
    "resolveStoppedWriter",
    "runLaunch",
  ]);
  assert.equal(Object.getPrototypeOf(value.facade), null);
  assert.equal(Object.isFrozen(value.facade), true);

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

  const capability = await value.stoppedWriterCoordinator.stopAndIssueCapability({
    processIncarnationId: resolved.processIncarnationId,
    stopOperationId: resolved.stopOperationId,
    writer: resolved.writer,
    writerIncarnationId: resolved.writerIncarnationId,
  });
  assert.equal(value.supervisorStopCalls, 1);
  assert.equal(Object.isFrozen(capability), true);
  assert.throws(
    () => value.facade.resolveStoppedWriter(resolverInput(value)),
    assertLauncherError("invalid_logical_writer_launch_request"),
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

    const resolved = value.facade.resolveStoppedWriter(
      cycleResolverInput(value, cycle),
    );
    const capability =
      await value.stoppedWriterCoordinator.stopAndIssueCapability({
        processIncarnationId: resolved.processIncarnationId,
        stopOperationId: resolved.stopOperationId,
        writer: resolved.writer,
        writerIncarnationId: resolved.writerIncarnationId,
      });

    await assert.rejects(
      value.facade.reconcileLaunchAttempt({
        launchAttemptId: cycle.launchAttemptId,
      }),
      assertLauncherError("logical_writer_handle_unavailable"),
    );
    await value.stoppedWriterCoordinator.consumeCapability({
      attachment: cycle.mounted,
      canonicalLease: cycle.writerLease,
      capability,
      processIncarnationId: resolved.processIncarnationId,
      runSnapshot: async () => `captured-${cycle.ordinal}`,
      stopOperationId: resolved.stopOperationId,
      writer: resolved.writer,
      writerIncarnationId: resolved.writerIncarnationId,
    });
    value.stoppedWriterCoordinator.retireWriter({
      processIncarnationId: resolved.processIncarnationId,
      writer: resolved.writer,
      writerIncarnationId: resolved.writerIncarnationId,
    });
  }

  assert.equal(value.launchCalls, cycleCount);
  assert.equal(value.supervisorStopCalls, cycleCount);
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
  const resolved = value.facade.resolveStoppedWriter(
    cycleResolverInput(value, first),
  );

  await assert.rejects(
    value.stoppedWriterCoordinator.stopAndIssueCapability({
      processIncarnationId: resolved.processIncarnationId,
      stopOperationId: resolved.stopOperationId,
      writer: resolved.writer,
      writerIncarnationId: resolved.writerIncarnationId,
    }),
    (error) =>
      error instanceof StoppedWriterCapabilityError &&
      error.code === "writer_stop_outcome_uncertain",
  );
  assert.equal(value.supervisorStopCalls, 1);
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
    assertLauncherError("logical_writer_launch_outcome_uncertain"),
  );
  assert.equal(value.launchCalls, 2);
  assert.equal(value.authority.state, "uncertain");
  assert.equal(value.authority.calls.finalizeStarted, 1);
  assert.equal(value.authority.calls.markUncertain, 1);
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
  const stoppedWriterCoordinator = new HostileStoppedWriterCoordinator();
  const value = await fixture({ stoppedWriterCoordinator });

  const result = await value.facade.runLaunch(runInput(value));
  assert.equal(result.status, "started");
  assert.equal(hostileRegisterWriterCalls, 0);
  const resolved = value.facade.resolveStoppedWriter(resolverInput(value));
  assert.strictEqual(resolved.writer, result.writer);
});

test("keeps launch behavior stable after selected mutable intrinsics are poisoned", async () => {
  const value = await fixture();
  const originals = {
    arrayIsArray: Array.isArray,
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

test("registration conflict after launch marks uncertainty without finalization", async () => {
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
    assertLauncherError("logical_writer_launch_outcome_uncertain"),
  );
  assert.equal(value.launchCalls, 1);
  assert.equal(value.authority.state, "uncertain");
  assert.equal(value.authority.calls.markUncertain, 1);
  assert.equal(value.authority.calls.finalizeStarted, 0);
  assert.equal(value.authority.calls.finalizeStopped, 0);
  assert.throws(
    () => value.facade.resolveStoppedWriter(resolverInput(value)),
    assertLauncherError("invalid_logical_writer_launch_request"),
  );
  const recovered = await value.facade.reconcileLaunchAttempt({
    launchAttemptId: LAUNCH_ATTEMPT_ID,
  });
  assert.equal(recovered.status, "complete-stopped");
  assert.equal(recovered.writer, null);
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
