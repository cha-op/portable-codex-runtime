import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import test from "node:test";

import {
  createPostgresCheckpointMutationAuthority,
  PostgresCheckpointMutationAuthorityError,
} from "../src/postgres-checkpoint-mutation-authority.mjs";
import {
  PostgresOperationGuard,
} from "../src/postgres-operation-guard.mjs";
import {
  CHECKPOINT_CAPTURE_OPERATION_KIND,
  createCheckpointCaptureOperationRequest,
  SESSION_AUTHORITY_DOCUMENT_VERSION,
  SESSION_OPERATION_CONFLICT_CLASS,
} from "../src/postgres-session-authority.mjs";
import {
  createSessionManifest,
  serializeSessionManifest,
} from "../src/session-storage-contracts.mjs";

const SESSION_ID = "019f2100-0000-7000-8000-000000000001";
const CAPTURE_ATTEMPT_ID = "019f2100-0000-7000-8000-000000000002";
const CROSSED_CAPTURE_ATTEMPT_ID =
  "019f2100-0000-7000-8000-000000000003";
const OPERATION_ID = "checkpoint-capture-operation-001";
const CHECKPOINT_ID = "checkpoint-001";
const ARTIFACT_ID = "checkpoint-artifact-001";
const NOW = "2026-07-31T12:00:00.000Z";
const AUTHORITY_NOW = "2026-07-31T12:00:01.000Z";
const IMAGE_DIGEST = `sha256:${"a".repeat(64)}`;
const SOURCE_OWNED_ROOT = "/srv/portable-codex/sessions";
const SOURCE_DIRECTORY = `${SOURCE_OWNED_ROOT}/session-001`;
const ARTIFACT_OWNED_ROOT = "/srv/portable-codex/checkpoints";
const ARTIFACT_DIRECTORY = `${ARTIFACT_OWNED_ROOT}/${ARTIFACT_ID}`;
const jsonReceiver = JSON;
const jsonStringifyIntrinsic = jsonReceiver.stringify;
const reflectApply = Reflect.apply;
const RESERVATION_ID = `reservation-${createHash("sha256")
  .update(OPERATION_ID, "utf8")
  .digest("hex")}`;

function deepFreeze(value) {
  if (value !== null && typeof value === "object" && !Object.isFrozen(value)) {
    const children = Object.values(value);
    for (let index = 0; index < children.length; index += 1) {
      deepFreeze(children[index]);
    }
    Object.freeze(value);
  }
  return value;
}

function exactKeys(value, keys) {
  assert.deepEqual(Reflect.ownKeys(value).sort(), [...keys].sort());
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
          rows: [{ acquired: true, backend_pid: 9001 }],
        };
      } else if (config.text.includes("FROM pg_catalog.pg_locks")) {
        result = {
          command: "SELECT",
          rows: [{ backend_pid: 9001, lock_held: state.lockHeld }],
        };
      } else if (config.text.includes("pg_advisory_unlock(")) {
        const unlocked = state.lockHeld;
        state.lockHeld = false;
        result = {
          command: "SELECT",
          rows: [{ backend_pid: 9001, unlocked }],
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

function sha256(value) {
  return createHash("sha256")
    .update(
      reflectApply(jsonStringifyIntrinsic, jsonReceiver, [value]),
      "utf8",
    )
    .digest("hex");
}

function operationRequestSha256(state) {
  return sha256({
    requestVersion: 1,
    conflictClass: SESSION_OPERATION_CONFLICT_CLASS,
    expectedSession: state.expectedSession,
    payload: state.typedRequest,
  });
}

function manifest() {
  return JSON.parse(
    serializeSessionManifest(
      createSessionManifest({
        sessionId: SESSION_ID,
        codex: {
          rootThreadId: SESSION_ID,
          sessionId: SESSION_ID,
          ephemeral: false,
          historyMode: "paginated",
        },
        runtime: {
          imageDigest: IMAGE_DIGEST,
          imageMediaType: "application/vnd.oci.image.manifest.v1+json",
          platform: "linux/arm64",
          codexVersion: "codex-cli 0.142.4",
          codexSandbox: "danger-full-access",
        },
      }),
    ),
  );
}

function storageRef() {
  return {
    backendId: "stopped-directory",
    contractVersion: 1,
    sessionId: SESSION_ID,
    storageId: "storage-001",
  };
}

function lease() {
  return {
    contractVersion: 1,
    expiresAt: "2026-07-31T13:00:00.000Z",
    fencingEpoch: "4",
    holderId: "holder-001",
    leaseId: "lease-001",
    sessionId: SESSION_ID,
  };
}

function attachment() {
  return {
    attachmentId: "attachment-001",
    backendId: "stopped-directory",
    contractVersion: 1,
    fencingEpoch: "4",
    holderId: "holder-001",
    kind: "directory",
    leaseId: "lease-001",
    mode: "read-write",
    operationId: "writer-attach-001",
    proofId: "proof-attachment-001",
    rootPath: SOURCE_DIRECTORY,
    sessionId: SESSION_ID,
    storageId: "storage-001",
  };
}

function document(overrides = {}) {
  return {
    activeOperation: null,
    attachment: attachment(),
    backendCapabilities: {
      atomicPointInTimeCheckpoint: false,
      exclusiveWriterAttachment: true,
      fencing: "manual",
      normalDirectoryAttachment: true,
    },
    documentVersion: SESSION_AUTHORITY_DOCUMENT_VERSION,
    lastOperation: {
      conflictClass: SESSION_OPERATION_CONFLICT_CLASS,
      expectedSessionRevision: "4",
      kind: "writer-attachment-acquire-v1",
      operationId: "writer-attach-001",
      operationRevision: "2",
      requestSha256: "b".repeat(64),
      reservationId: "reservation-writer-001",
      resultSha256: "c".repeat(64),
      state: "committed",
    },
    launch: null,
    lease: lease(),
    lifecycle: "ATTACHED",
    manifest: manifest(),
    recovery: null,
    storageRef: storageRef(),
    writerEpoch: "4",
    ...overrides,
  };
}

function session(overrides = {}) {
  return deepFreeze({
    createdAt: NOW,
    document: document(),
    revision: "7",
    sessionId: SESSION_ID,
    updatedAt: NOW,
    ...overrides,
  });
}

function nearRevisionExhaustionSession() {
  const revision = 9_223_372_036_854_775_805n;
  const lastOperation = document().lastOperation;
  return session({
    document: document({
      lastOperation: {
        ...lastOperation,
        expectedSessionRevision: (revision - 3n).toString(),
      },
    }),
    revision: revision.toString(),
  });
}

function checkpoint(overrides = {}) {
  return {
    artifactId: ARTIFACT_ID,
    backendId: "stopped-directory",
    checkpointClass: "clean",
    checkpointId: CHECKPOINT_ID,
    codexSessionId: SESSION_ID,
    codexThreadId: SESSION_ID,
    contractVersion: 1,
    createdAt: NOW,
    imageDigest: IMAGE_DIGEST,
    sessionId: SESSION_ID,
    sourceFencingEpoch: "4",
    storageId: "storage-001",
    ...overrides,
  };
}

function mutationRequest(overrides = {}) {
  return {
    backendId: "stopped-directory",
    contractVersion: 1,
    fencingEpoch: "4",
    holderId: "holder-001",
    leaseId: "lease-001",
    operation: "checkpoint",
    operationId: OPERATION_ID,
    sessionId: SESSION_ID,
    storageId: "storage-001",
    target: {
      artifactId: ARTIFACT_ID,
      checkpointId: CHECKPOINT_ID,
      kind: "checkpoint",
    },
    ...overrides,
  };
}

function captureAdmission() {
  return deepFreeze({
    attachment: attachment(),
    captureAttemptId: CAPTURE_ATTEMPT_ID,
    checkpoint: checkpoint(),
    processIncarnationId: "process-incarnation-001",
    request: mutationRequest(),
    stopOperationId: "stop-operation-001",
    writerIncarnationId: "writer-incarnation-001",
  });
}

function reconciliationAdmission() {
  return deepFreeze({
    checkpoint: checkpoint(),
    request: mutationRequest(),
  });
}

function operationView(state, phase = "starting", revision = "1") {
  const updatedAt = phase === "prepared" ? NOW : AUTHORITY_NOW;
  const result =
    phase === "committed"
      ? {
          captureAttemptId: CAPTURE_ATTEMPT_ID,
          catalogueSha256: sha256(catalogueView(state).document),
          checkpointId: CHECKPOINT_ID,
          outcome: "checkpoint-captured",
          resultVersion: 1,
        }
      : null;
  return deepFreeze({
    conflictClass: SESSION_OPERATION_CONFLICT_CLASS,
    createdAt: NOW,
    expectedSession: state.expectedSession,
    kind: CHECKPOINT_CAPTURE_OPERATION_KIND,
    operationId: OPERATION_ID,
    request: state.typedRequest,
    requestSha256: operationRequestSha256(state),
    result,
    retiredAt: phase === "committed" ? AUTHORITY_NOW : null,
    revision,
    sessionId: SESSION_ID,
    state: phase,
    updatedAt,
  });
}

function reservationView(state, phase = "starting") {
  const updatedAt = phase === "prepared" ? NOW : AUTHORITY_NOW;
  return deepFreeze({
    conflictClass: SESSION_OPERATION_CONFLICT_CLASS,
    createdAt: NOW,
    expectedSessionRevision: state.expectedSession.revision,
    expiresAt: null,
    kind: CHECKPOINT_CAPTURE_OPERATION_KIND,
    operationId: OPERATION_ID,
    releasedAt: phase === "released" ? AUTHORITY_NOW : null,
    requestSha256: operationRequestSha256(state),
    reservationId: RESERVATION_ID,
    sessionId: SESSION_ID,
    state: phase,
    updatedAt,
  });
}

function attemptView(state, phase = "authorized") {
  const admission = state.typedRequest.admission;
  return deepFreeze({
    binding: {
      attachmentId: admission.attachment.attachmentId,
      attachmentOperationId: admission.attachment.operationId,
      attachmentProofId: admission.attachment.proofId,
      captureAttemptId: CAPTURE_ATTEMPT_ID,
      checkpoint: admission.checkpoint,
      contractVersion: 2,
      processIncarnationId: admission.processIncarnationId,
      reservationId: RESERVATION_ID,
      stopOperationId: admission.stopOperationId,
      writerIncarnationId: admission.writerIncarnationId,
    },
    captureAttemptId: CAPTURE_ATTEMPT_ID,
    contractVersion: 1,
    operationId: OPERATION_ID,
    request: admission.request,
    result: state.typedRequest.predeterminedResult,
    state: phase,
  });
}

function completion(state, replayed) {
  return deepFreeze({
    artifactProof: {
      artifactManifestDigest: "e".repeat(64),
      captureOperationId: OPERATION_ID,
      modeledDigest: "f".repeat(64),
    },
    materialization: {
      artifactManifestDigest: "e".repeat(64),
      contractVersion: 2,
      modeledDigest: "f".repeat(64),
      publicationId: "publication-001",
      publicationKind: "checkpoint-artifact",
      stagedRoot: {
        filesystemId: "filesystem-001",
        objectIdentityScheme: "inode-v1",
        objectId: "object-001",
      },
      treeIdentityDigest: "1".repeat(64),
    },
    replayed,
    result: state.typedRequest.predeterminedResult,
  });
}

function catalogueView(state) {
  const value = completion(state, true);
  return deepFreeze({
    captureAttemptId: CAPTURE_ATTEMPT_ID,
    checkpointId: CHECKPOINT_ID,
    committedAt: AUTHORITY_NOW,
    document: {
      artifactProof: value.artifactProof,
      contractVersion: 1,
      materialization: value.materialization,
      result: value.result,
    },
    sessionId: SESSION_ID,
  });
}

function authoritySession(state, phase, revision) {
  const operation = operationView(state, phase, revision);
  const reservation = reservationView(
    state,
    phase === "committed" ? "released" : phase,
  );
  const expectedDocument = state.expectedSession.document;
  const authorityDocument =
    phase === "committed"
      ? {
          ...structuredClone(expectedDocument),
          activeOperation: null,
          lastOperation: {
            conflictClass: operation.conflictClass,
            expectedSessionRevision: state.expectedSession.revision,
            kind: operation.kind,
            operationId: operation.operationId,
            operationRevision: operation.revision,
            requestSha256: operation.requestSha256,
            reservationId: reservation.reservationId,
            resultSha256: sha256(operation.result),
            state: "committed",
          },
        }
      : {
          ...structuredClone(expectedDocument),
          activeOperation: {
            conflictClass: operation.conflictClass,
            expectedSessionRevision: state.expectedSession.revision,
            kind: operation.kind,
            operationId: operation.operationId,
            operationRevision: operation.revision,
            requestSha256: operation.requestSha256,
            reservationId: reservation.reservationId,
            state: phase,
          },
        };
  return deepFreeze({
    createdAt: state.expectedSession.createdAt,
    document: authorityDocument,
    revision: (
      BigInt(state.expectedSession.revision) +
      BigInt(revision) +
      1n
    ).toString(),
    sessionId: SESSION_ID,
    updatedAt: operation.updatedAt,
  });
}

function laterAuthoritySession(state) {
  const laterDocument = structuredClone(state.expectedSession.document);
  laterDocument.attachment = {
    ...laterDocument.attachment,
    fencingEpoch: "5",
    holderId: "holder-later-001",
    leaseId: "lease-later-001",
    operationId: "writer-attach-later-001",
    proofId: "proof-attachment-later-001",
  };
  laterDocument.lease = {
    ...laterDocument.lease,
    expiresAt: "2026-07-31T14:00:00.000Z",
    fencingEpoch: "5",
    holderId: "holder-later-001",
    leaseId: "lease-later-001",
  };
  laterDocument.lastOperation = {
    conflictClass: SESSION_OPERATION_CONFLICT_CLASS,
    expectedSessionRevision: "39",
    kind: "writer-attachment-acquire-v1",
    operationId: "writer-attach-later-001",
    operationRevision: "2",
    requestSha256: "8".repeat(64),
    reservationId: "reservation-writer-later-001",
    resultSha256: "9".repeat(64),
    state: "committed",
  };
  laterDocument.writerEpoch = "5";
  return deepFreeze({
    createdAt: state.expectedSession.createdAt,
    document: laterDocument,
    revision: "42",
    sessionId: SESSION_ID,
    updatedAt: "2026-07-31T12:05:00.000Z",
  });
}

function mismatchedHistoricalAuthoritySession(state, phase, revision) {
  const mismatched = structuredClone(
    authoritySession(state, phase, revision),
  );
  mismatched.document.storageRef.storageId = "storage-crossed-001";
  return deepFreeze(mismatched);
}

function staleHistoricalAuthoritySession(state) {
  const stale = structuredClone(state.expectedSession);
  stale.updatedAt = "2026-07-31T12:00:02.000Z";
  return deepFreeze(stale);
}

function maximumHistoricalAuthoritySession(state) {
  const current = structuredClone(state.expectedSession);
  current.revision = "9223372036854775807";
  current.updatedAt = "2026-07-31T12:00:02.000Z";
  return deepFreeze(current);
}

function forgedAttemptView(state, mode) {
  const attempt = structuredClone(attemptView(state, state.attemptPhase));
  if (mode === "capture-attempt-id") {
    attempt.captureAttemptId = CROSSED_CAPTURE_ATTEMPT_ID;
    attempt.binding.captureAttemptId = CROSSED_CAPTURE_ATTEMPT_ID;
  } else if (mode === "predetermined-result") {
    attempt.result.checkpoint.checkpointId = "checkpoint-crossed-001";
  } else if (mode === "binding-reservation") {
    attempt.binding.reservationId = "reservation-crossed-001";
  } else if (mode === "binding-coordinator") {
    attempt.binding.stopOperationId = "stop-operation-crossed-001";
  }
  return deepFreeze(attempt);
}

class FakeAuthority {
  constructor(state) {
    this.state = state;
  }

  async readSession(options) {
    this.state.trace.push("authority:read-session");
    exactKeys(options, ["sessionId"]);
    assert.equal(options.sessionId, SESSION_ID);
    await this.state.readGate;
    return this.state.expectedSession;
  }

  async reserveOperation(options) {
    this.state.trace.push("authority:reserve");
    this.state.reserveCalls += 1;
    this.state.baseInput = options;
    this.state.expectedSession = options.expectedSession;
    this.state.typedRequest = options.request;
    if (this.state.reserveMode === "throw") {
      throw new Error("/private/database reserve acknowledgement lost");
    }
    if (this.state.reserveMode === "malformed") {
      return deepFreeze({
        acquired: true,
        operation: null,
        reservation: null,
        session: this.state.expectedSession,
        status: "invalid",
      });
    }
    const receipt = {
      acquired: this.state.reserveMode !== "nonfresh",
      operation: operationView(this.state, "prepared", "0"),
      reservation: reservationView(this.state, "prepared"),
      session: authoritySession(this.state, "prepared", "0"),
      status: "prepared",
    };
    if (this.state.reserveMode === "crossed-reservation") {
      receipt.reservation = structuredClone(receipt.reservation);
      receipt.reservation.reservationId = "reservation-crossed-001";
    }
    return deepFreeze(receipt);
  }

  async claimCheckpointCaptureDispatch(options) {
    this.state.trace.push("authority:claim");
    this.state.claimCalls += 1;
    this.state.claimInputs.push(options);
    if (this.state.claimMode.startsWith("throw")) {
      this.state.reconcilePhase =
        this.state.claimMode === "throw-starting" ? "starting" : "prepared";
      throw new Error("/private/database claim acknowledgement lost");
    }
    const receipt = {
      attempt: attemptView(this.state),
      authorityNow: AUTHORITY_NOW,
      dispatchGranted: true,
      operation: operationView(this.state),
      reservation: reservationView(this.state),
      session: authoritySession(this.state, "starting", "1"),
      status: "starting",
    };
    if (this.state.claimMode === "nonfresh") {
      receipt.dispatchGranted = false;
    }
    if (this.state.claimMode === "crossed-reservation") {
      this.state.reconcilePhase = "starting";
      receipt.reservation = structuredClone(receipt.reservation);
      receipt.reservation.reservationId = "reservation-crossed-001";
    }
    return deepFreeze(receipt);
  }

  async reconcileOperation(options) {
    this.state.trace.push("authority:reconcile-operation");
    this.state.reconcileCalls += 1;
    const phase = this.state.reconcilePhase;
    if (phase === "absent") {
      return deepFreeze({
        operation: null,
        reservation: null,
        session: this.state.expectedSession,
        status: "absent",
      });
    }
    return deepFreeze({
      operation: operationView(
        this.state,
        phase,
        phase === "prepared" ? "0" : phase === "starting" ? "1" : "2",
      ),
      reservation: reservationView(this.state, phase),
      session: this.state.expectedSession,
      status: phase,
    });
  }

  async cancelPreparedOperation(options) {
    this.state.trace.push("authority:cancel-prepared");
    this.state.cancelCalls += 1;
    assert.equal(options.reason, "capture-dispatch-not-started");
    return deepFreeze({
      cancelled: true,
      operation: operationView(this.state, "committed", "1"),
      reservation: reservationView(this.state, "released"),
      session: this.state.expectedSession,
      status: "committed",
    });
  }

  async markOperationUncertain() {
    this.state.trace.push("authority:mark-uncertain");
    this.state.uncertainCalls += 1;
    return deepFreeze({
      changed: true,
      operation: operationView(this.state, "uncertain", "2"),
      reservation: reservationView(this.state, "uncertain"),
      session: this.state.expectedSession,
      status: "uncertain",
    });
  }

  async finalizeCheckpointCapture(options) {
    this.state.trace.push("authority:finalize");
    this.state.finalizeInputs.push(options);
    if (this.state.finalizeMode === "throw") {
      throw new Error("/private/database finalize acknowledgement lost");
    }
    if (this.state.finalizeMode === "throw-after-commit") {
      this.state.attemptPhase = "committed";
      this.state.committedRevision =
        options.expectedOperationRevision === "1" ? "2" : "3";
      throw new Error("/private/database finalize acknowledgement lost");
    }
    if (this.state.finalizeMode === "malformed") {
      return deepFreeze({
        attempt: attemptView(this.state),
        catalogue: catalogueView(this.state),
        finalized: true,
        operation: operationView(this.state, "committed", "2"),
        reservation: reservationView(this.state, "released"),
        session: this.state.expectedSession,
        status: "committed",
      });
    }
    const receipt = {
      attempt: attemptView(this.state, "committed"),
      catalogue: catalogueView(this.state),
      finalized: this.state.attemptPhase !== "committed",
      operation: operationView(
        this.state,
        "committed",
        options.expectedOperationRevision === "1" ? "2" : "3",
      ),
      reservation: reservationView(this.state, "released"),
      session:
        this.state.staleHistoricalFinalizeSession &&
        this.state.attemptPhase === "committed"
          ? staleHistoricalAuthoritySession(this.state)
          : this.state.laterSessionOnCommittedReplay &&
        this.state.attemptPhase === "committed"
          ? laterAuthoritySession(this.state)
          : authoritySession(
              this.state,
              "committed",
              options.expectedOperationRevision === "1" ? "2" : "3",
            ),
      status: "committed",
    };
    const forged = structuredClone(receipt);
    if (this.state.finalizeMode === "crossed-catalogue") {
      forged.catalogue.document.materialization.publicationId =
        "publication-crossed-001";
    } else if (this.state.finalizeMode === "crossed-reservation") {
      forged.reservation.reservationId = "reservation-crossed-001";
    } else if (this.state.finalizeMode === "crossed-attempt-result") {
      forged.attempt.result.checkpoint.checkpointId =
        "checkpoint-crossed-001";
    } else if (
      this.state.finalizeMode === "crossed-operation-request"
    ) {
      forged.operation.request.predeterminedResult.checkpoint.checkpointId =
        "checkpoint-crossed-001";
    } else if (this.state.finalizeMode === "materialization-v3") {
      forged.catalogue.document.materialization.contractVersion = 3;
    }
    const result = deepFreeze(forged);
    this.state.finalizeResults.push(result);
    return result;
  }

  async readCheckpointCaptureAttempt() {
    this.state.trace.push("authority:read-attempt");
    await this.state.readAttemptGate;
    const phase = this.state.attemptPhase;
    if (phase === "prepared") {
      return deepFreeze({
        attempt: null,
        catalogue: null,
        operation: operationView(this.state, "prepared", "0"),
        reservation: reservationView(this.state, "prepared"),
        session: authoritySession(this.state, "prepared", "0"),
        status: "prepared",
      });
    }
    const revision =
      phase === "committed"
        ? this.state.committedRevision
        : this.state.authorizedRevision;
    const operationPhase =
      phase === "committed"
        ? "committed"
        : revision === "1"
          ? "starting"
          : "uncertain";
    const attempt =
      this.state.readAttemptForgery === null
        ? attemptView(this.state, phase)
        : forgedAttemptView(this.state, this.state.readAttemptForgery);
    const returnedAttempt = this.state.distinctAttemptData
      ? deepFreeze(structuredClone(attempt))
      : attempt;
    let returnedSession =
      phase === "committed" && this.state.maximumHistoricalReadSession
        ? maximumHistoricalAuthoritySession(this.state)
        : phase === "committed" && this.state.staleHistoricalReadSession
        ? staleHistoricalAuthoritySession(this.state)
        : phase === "committed" &&
      this.state.laterSessionOnCommittedReplay
        ? laterAuthoritySession(this.state)
        : authoritySession(this.state, operationPhase, revision);
    if (
      phase === "committed" &&
      this.state.mismatchedHistoricalSessionIdentity
    ) {
      returnedSession = mismatchedHistoricalAuthoritySession(
        this.state,
        operationPhase,
        revision,
      );
    }
    return deepFreeze({
      attempt: returnedAttempt,
      catalogue:
        phase === "committed" ||
        this.state.readAttemptForgery === "authorized-catalogue"
          ? catalogueView(this.state)
          : null,
      operation: operationView(this.state, operationPhase, revision),
      reservation: reservationView(
        this.state,
        phase === "committed" ? "released" : operationPhase,
      ),
      session: returnedSession,
      status: phase,
    });
  }

}

class FakeGuard {
  constructor(state) {
    this.state = state;
  }

  async runExclusive(operationId, callback) {
    this.state.trace.push("guard:start");
    assert.equal(operationId, OPERATION_ID);
    let probes = 0;
    let completionCalls = 0;
    let completionCarrier;
    let completionValue;
    const complete = Object.freeze((value) => {
      completionCalls += 1;
      assert.equal(completionCalls, 1);
      completionValue = value;
      completionCarrier = Object.freeze(Object.create(null));
      return completionCarrier;
    });
    const carrier = await callback(
      Object.freeze({
        assertHeld: () => {
          probes += 1;
          this.state.trace.push(`guard:probe:${probes}`);
          if (this.state.guardFailureProbe === probes) {
            return Promise.reject(new Error("/private/guard lost"));
          }
          return Promise.resolve();
        },
      }),
      complete,
    );
    assert.equal(completionCalls, 1);
    assert.strictEqual(carrier, completionCarrier);
    this.state.trace.push("guard:end");
    return completionValue;
  }
}

function fixture(overrides = {}) {
  const {
    operationGuard: providedOperationGuard,
    ...stateOverrides
  } = overrides;
  const expectedSession = session();
  const admission = captureAdmission();
  const state = {
    attemptPhase: "authorized",
    authorizedRevision: "1",
    cancelCalls: 0,
    claimCalls: 0,
    claimInputs: [],
    claimMode: "success",
    committedRevision: "2",
    distinctAttemptData: false,
    expectedSession,
    finalizeInputs: [],
    finalizeMode: "success",
    finalizeResults: [],
    guardFailureProbe: 0,
    laterSessionOnCommittedReplay: false,
    maximumHistoricalReadSession: false,
    mismatchedHistoricalSessionIdentity: false,
    readAttemptForgery: null,
    readAttemptGate: Promise.resolve(),
    readGate: Promise.resolve(),
    reconcileCalls: 0,
    reconcilePhase: "prepared",
    reserveCalls: 0,
    reserveMode: "fresh",
    staleHistoricalFinalizeSession: false,
    staleHistoricalReadSession: false,
    trace: [],
    uncertainCalls: 0,
    ...stateOverrides,
  };
  state.typedRequest = createCheckpointCaptureOperationRequest({
    admission,
    expectedSession,
  });
  state.baseInput = deepFreeze({
    expectedSession,
    kind: CHECKPOINT_CAPTURE_OPERATION_KIND,
    operationId: OPERATION_ID,
    request: state.typedRequest,
  });
  const authority = Object.freeze(new FakeAuthority(state));
  const operationGuard =
    providedOperationGuard ?? Object.freeze(new FakeGuard(state));
  let artifactPlannerCalls = 0;
  let sourcePlannerCalls = 0;
  const resolveArtifactPaths = (input) => {
    state.trace.push("planner:artifact");
    artifactPlannerCalls += 1;
    assert.equal(Object.isFrozen(input), true);
    exactKeys(input, ["checkpoint", "request"]);
    return deepFreeze({
      artifactDirectory: ARTIFACT_DIRECTORY,
      artifactOwnedRoot: ARTIFACT_OWNED_ROOT,
    });
  };
  const resolveSourceOwnedRoot = (input) => {
    state.trace.push("planner:source");
    sourcePlannerCalls += 1;
    assert.equal(Object.isFrozen(input), true);
    exactKeys(input, ["canonicalAttachment", "checkpoint", "request"]);
    return deepFreeze({
      sourceDirectory: SOURCE_DIRECTORY,
      sourceOwnedRoot: SOURCE_OWNED_ROOT,
    });
  };
  const adapter = createPostgresCheckpointMutationAuthority({
    authority,
    operationGuard,
    resolveArtifactPaths,
    resolveSourceOwnedRoot,
  });
  return {
    adapter,
    admission,
    authority,
    get artifactPlannerCalls() {
      return artifactPlannerCalls;
    },
    get sourcePlannerCalls() {
      return sourcePlannerCalls;
    },
    state,
  };
}

async function assertAuthorityError(promise, code) {
  await assert.rejects(promise, (error) => {
    assert.ok(error instanceof PostgresCheckpointMutationAuthorityError);
    assert.equal(error.code, code);
    assert.equal(error.retryable, false);
    assert.equal(Object.isFrozen(error), true);
    assert.equal(error.stack.includes("/private/"), false);
    return true;
  });
}

test("factory returns an exact frozen stopped-directory authority facade", () => {
  const { adapter } = fixture();

  exactKeys(adapter, [
    "runCapture",
    "runCaptureReconciliation",
    "runPreparedCapture",
    "runRestore",
  ]);
  assert.equal(Object.isFrozen(adapter), true);
  assert.equal(typeof adapter.runCapture, "function");
  assert.equal(typeof adapter.runCaptureReconciliation, "function");
  assert.equal(typeof adapter.runPreparedCapture, "function");
  assert.equal(typeof adapter.runRestore, "function");
});

test("capture composes with a real callback-only PostgreSQL operation guard", async () => {
  const database = callbackOnlyOperationGuardPool();
  const operationGuard = new PostgresOperationGuard({
    dedicatedPool: database.pool,
  });
  const value = fixture({ operationGuard });
  let callbackCalls = 0;
  let callbackCompletion;

  const result = await value.adapter.runCapture(
    value.admission,
    async () => {
      callbackCalls += 1;
      callbackCompletion = completion(value.state, false);
      return callbackCompletion;
    },
  );

  assert.strictEqual(result, callbackCompletion);
  assert.equal(callbackCalls, 1);
  assert.equal(database.state.connectCalls, 1);
  assert.equal(database.state.connectCallbacks, 1);
  assert.equal(database.state.queryCalls > 0, true);
  assert.equal(database.state.queryCallbacks, database.state.queryCalls);
  assert.equal(database.state.releaseCalls, 1);
  assert.equal(database.state.lockHeld, false);
});

test("error constructor rejects inherited and prototype-polluted codes", () => {
  const inheritedError = (() => {
    try {
      return new PostgresCheckpointMutationAuthorityError("toString");
    } catch (error) {
      return error;
    }
  })();
  assert.ok(inheritedError instanceof TypeError);

  const pollutedCode = "postgres_checkpoint_prototype_polluted";
  const pollutedDescriptor = Object.getOwnPropertyDescriptor(
    Object.prototype,
    pollutedCode,
  );
  let pollutedError;
  try {
    Object.defineProperty(Object.prototype, pollutedCode, {
      configurable: true,
      value: "Prototype pollution must not define an error code",
    });
    try {
      pollutedError = new PostgresCheckpointMutationAuthorityError(
        pollutedCode,
      );
    } catch (error) {
      pollutedError = error;
    }
  } finally {
    if (pollutedDescriptor === undefined) {
      Reflect.deleteProperty(Object.prototype, pollutedCode);
    } else {
      Object.defineProperty(
        Object.prototype,
        pollutedCode,
        pollutedDescriptor,
      );
    }
  }

  assert.ok(pollutedError instanceof TypeError);
});

test("error constructor uses TypeError captured before global replacement", () => {
  const typeErrorDescriptor = Object.getOwnPropertyDescriptor(
    globalThis,
    "TypeError",
  );
  const OriginalTypeError = typeErrorDescriptor.value;
  class ReplacementTypeError extends Error {}
  let observedError;

  try {
    Object.defineProperty(globalThis, "TypeError", {
      ...typeErrorDescriptor,
      value: ReplacementTypeError,
    });
    try {
      observedError = new PostgresCheckpointMutationAuthorityError(
        "unsupported-error-code",
      );
    } catch (error) {
      observedError = error;
    }
  } finally {
    Object.defineProperty(globalThis, "TypeError", typeErrorDescriptor);
  }

  assert.ok(observedError instanceof OriginalTypeError);
  assert.equal(observedError instanceof ReplacementTypeError, false);
});

test("capture plans before reserve, probes around publication, and returns callback completion by identity", async () => {
  const value = fixture();
  let callbackCalls = 0;
  let observedContext;
  let callbackCompletion;

  const result = await value.adapter.runCapture(
    value.admission,
    async (context) => {
      value.state.trace.push("callback:publish");
      callbackCalls += 1;
      observedContext = context;
      callbackCompletion = completion(value.state, false);
      return callbackCompletion;
    },
  );

  assert.strictEqual(result, callbackCompletion);
  assert.equal(callbackCalls, 1);
  assert.equal(Object.isFrozen(observedContext), true);
  exactKeys(observedContext, [
    "artifactDirectory",
    "artifactOwnedRoot",
    "canonicalAttachment",
    "canonicalLease",
    "captureAttemptId",
    "now",
    "reservationId",
    "result",
    "sourceDirectory",
    "sourceOwnedRoot",
    "storageRef",
  ]);
  assert.equal(observedContext.now, Date.parse(AUTHORITY_NOW));
  assert.strictEqual(
    observedContext.result,
    value.state.typedRequest.predeterminedResult,
  );
  assert.deepEqual(value.state.trace, [
    "guard:start",
    "authority:read-session",
    "planner:artifact",
    "planner:source",
    "authority:reserve",
    "authority:claim",
    "guard:probe:1",
    "callback:publish",
    "guard:probe:2",
    "authority:finalize",
    "guard:end",
  ]);
});

test("canonical operation-result hashing ignores inherited numeric array accessors", async () => {
  const value = fixture();
  const callbackCompletion = completion(value.state, false);
  const numericDescriptor = Object.getOwnPropertyDescriptor(
    Array.prototype,
    "4",
  );
  let replacementAttempts = 0;
  let result;

  try {
    Object.defineProperty(Array.prototype, "4", {
      configurable: true,
      get() {
        return undefined;
      },
      set(candidate) {
        const replacement =
          candidate === "resultVersion" ? "outcome" : candidate;
        if (replacement !== candidate) replacementAttempts += 1;
        Object.defineProperty(this, "4", {
          configurable: true,
          enumerable: true,
          value: replacement,
          writable: true,
        });
      },
    });

    result = await value.adapter.runCapture(
      value.admission,
      async () => callbackCompletion,
    );
  } finally {
    if (numericDescriptor === undefined) {
      Reflect.deleteProperty(Array.prototype, "4");
    } else {
      Object.defineProperty(Array.prototype, "4", numericDescriptor);
    }
  }

  assert.strictEqual(result, callbackCompletion);
  assert.equal(replacementAttempts, 0);
  assert.equal(value.state.finalizeInputs.length, 1);
});

test("captured JSON receiver prevents global getter reentrancy", async () => {
  const value = fixture();
  const callbackCompletion = completion(value.state, false);
  const jsonDescriptor = Object.getOwnPropertyDescriptor(globalThis, "JSON");
  let getterCalls = 0;
  let result;

  try {
    Object.defineProperty(globalThis, "JSON", {
      configurable: true,
      enumerable: jsonDescriptor.enumerable,
      get() {
        getterCalls += 1;
        value.state.finalizeMode = "crossed-catalogue";
        return jsonReceiver;
      },
    });

    result = await value.adapter.runCapture(
      value.admission,
      async () => callbackCompletion,
    );
  } finally {
    Object.defineProperty(globalThis, "JSON", jsonDescriptor);
  }

  assert.strictEqual(result, callbackCompletion);
  assert.equal(getterCalls, 0);
  assert.equal(value.state.finalizeMode, "success");
  assert.equal(value.state.finalizeInputs.length, 1);
});

test("captured RegExp exec preserves validation after prototype poisoning", async () => {
  const value = fixture();
  const validAdmission = reconciliationAdmission();
  const invalidAdmission = structuredClone(validAdmission);
  invalidAdmission.checkpoint.sessionId = "invalid-session-id";
  deepFreeze(invalidAdmission);
  const execDescriptor = Object.getOwnPropertyDescriptor(
    RegExp.prototype,
    "exec",
  );
  let validPending;
  let invalidPending;

  try {
    Object.defineProperty(RegExp.prototype, "exec", {
      ...execDescriptor,
      value() {
        throw new Error("poisoned RegExp.prototype.exec");
      },
    });
    validPending = value.adapter.runRestore(validAdmission, async () => {
      assert.fail("restore callback must not run");
    });
    invalidPending = value.adapter.runRestore(
      invalidAdmission,
      async () => {
        assert.fail("restore callback must not run");
      },
    );
  } finally {
    Object.defineProperty(RegExp.prototype, "exec", execDescriptor);
  }

  await assertAuthorityError(
    validPending,
    "postgres_checkpoint_restore_unavailable",
  );
  await assertAuthorityError(
    invalidPending,
    "invalid_postgres_checkpoint_mutation_authority_request",
  );
});

test("planner failure occurs before durable reserve", async () => {
  const value = fixture();
  value.adapter = createPostgresCheckpointMutationAuthority({
    authority: value.authority,
    operationGuard: Object.freeze(new FakeGuard(value.state)),
    resolveArtifactPaths() {
      return deepFreeze({
        artifactDirectory: "/relative/artifact/../artifact",
        artifactOwnedRoot: ARTIFACT_OWNED_ROOT,
      });
    },
    resolveSourceOwnedRoot() {
      throw new Error("must not run");
    },
  });
  let callbackCalls = 0;

  await assertAuthorityError(
    value.adapter.runCapture(value.admission, async () => {
      callbackCalls += 1;
    }),
    "postgres_checkpoint_mutation_authority_outcome_uncertain",
  );
  assert.equal(value.state.reserveCalls, 0);
  assert.equal(callbackCalls, 0);
});

test("non-fresh reserve is never adopted and never invokes publication", async () => {
  const value = fixture({ reserveMode: "nonfresh" });
  let callbackCalls = 0;

  await assertAuthorityError(
    value.adapter.runCapture(value.admission, async () => {
      callbackCalls += 1;
    }),
    "postgres_checkpoint_mutation_authority_outcome_uncertain",
  );

  assert.equal(callbackCalls, 0);
  assert.equal(value.state.reconcileCalls, 0);
  assert.equal(value.state.cancelCalls, 0);
});

test("callback, guard, finalize, and malformed finalize failures mark a definitely-started capture uncertain", async (t) => {
  const cases = [
    {
      name: "callback",
      options: {},
      publish: async () => {
        throw new Error("/private/source/callback failed");
      },
    },
    {
      name: "guard",
      options: { guardFailureProbe: 2 },
      publish: async (context, state) => completion(state, false),
    },
    {
      name: "finalize",
      options: { finalizeMode: "throw" },
      publish: async (context, state) => completion(state, false),
    },
    {
      name: "malformed-finalize",
      options: { finalizeMode: "malformed" },
      publish: async (context, state) => completion(state, false),
    },
    {
      name: "nested-unfrozen-completion",
      options: {},
      publish: async (context, state) => {
        const value = structuredClone(completion(state, false));
        return Object.freeze(value);
      },
    },
  ];

  for (const scenario of cases) {
    await t.test(scenario.name, async () => {
      const value = fixture(scenario.options);
      await assertAuthorityError(
        value.adapter.runCapture(
          value.admission,
          async (context) => scenario.publish(context, value.state),
        ),
        "postgres_checkpoint_mutation_authority_outcome_uncertain",
      );
      assert.equal(value.state.uncertainCalls, 1);
    });
  }
});

test("finalization rejects crossed durable tuples after publication", async (t) => {
  for (const finalizeMode of [
    "crossed-catalogue",
    "crossed-reservation",
    "crossed-attempt-result",
    "crossed-operation-request",
    "materialization-v3",
  ]) {
    await t.test(finalizeMode, async () => {
      const value = fixture({ finalizeMode });
      let callbackCalls = 0;

      await assertAuthorityError(
        value.adapter.runCapture(value.admission, async () => {
          callbackCalls += 1;
          return completion(value.state, false);
        }),
        "postgres_checkpoint_mutation_authority_outcome_uncertain",
      );

      assert.equal(callbackCalls, 1);
      assert.equal(value.state.finalizeInputs.length, 1);
      assert.equal(value.state.uncertainCalls, 1);
    });
  }
});

test("prepared recovery cancels only a definitely prepared pre-dispatch operation", async (t) => {
  await t.test("definite claim rejection", async () => {
    const value = fixture({
      claimMode: "throw-prepared",
      reconcilePhase: "prepared",
    });
    await assertAuthorityError(
      value.adapter.runCapture(value.admission, async () => {
        assert.fail("publication must not run");
      }),
      "postgres_checkpoint_mutation_authority_outcome_uncertain",
    );
    assert.equal(value.state.reconcileCalls, 1);
    assert.equal(value.state.cancelCalls, 1);
    assert.equal(value.state.uncertainCalls, 0);
  });

  await t.test("claim commit uncertainty retains starting", async () => {
    const value = fixture({
      claimMode: "throw-starting",
      reconcilePhase: "starting",
    });
    await assertAuthorityError(
      value.adapter.runCapture(value.admission, async () => {
        assert.fail("publication must not run");
      }),
      "postgres_checkpoint_mutation_authority_outcome_uncertain",
    );
    assert.equal(value.state.reconcileCalls, 1);
    assert.equal(value.state.cancelCalls, 0);
    assert.equal(value.state.uncertainCalls, 0);
  });

  await t.test("malformed acquired reserve receipt is reconciled", async () => {
    const value = fixture({
      reserveMode: "malformed",
      reconcilePhase: "prepared",
    });
    await assertAuthorityError(
      value.adapter.runCapture(value.admission, async () => {
        assert.fail("publication must not run");
      }),
      "postgres_checkpoint_mutation_authority_outcome_uncertain",
    );
    assert.equal(value.state.reconcileCalls, 1);
    assert.equal(value.state.cancelCalls, 1);
  });

  await t.test(
    "crossed prepared tuple is rejected before claim",
    async () => {
      const value = fixture({
        reconcilePhase: "prepared",
        reserveMode: "crossed-reservation",
      });
      let callbackCalls = 0;

      await assertAuthorityError(
        value.adapter.runCapture(value.admission, async () => {
          callbackCalls += 1;
        }),
        "postgres_checkpoint_mutation_authority_outcome_uncertain",
      );

      assert.equal(value.state.claimCalls, 0);
      assert.equal(callbackCalls, 0);
      assert.equal(value.state.reconcileCalls, 1);
      assert.equal(value.state.cancelCalls, 1);
    },
  );
});

test("claim rejects a crossed reservation tuple before publication", async () => {
  const value = fixture({ claimMode: "crossed-reservation" });
  let callbackCalls = 0;

  await assertAuthorityError(
    value.adapter.runCapture(value.admission, async () => {
      callbackCalls += 1;
    }),
    "postgres_checkpoint_mutation_authority_outcome_uncertain",
  );

  assert.equal(value.state.claimCalls, 1);
  assert.equal(callbackCalls, 0);
  assert.equal(value.state.reconcileCalls, 1);
  assert.equal(value.state.cancelCalls, 0);
  assert.equal(value.state.uncertainCalls, 0);
});

test("prepared capture claims under the guard before exposing one fresh publication context", async () => {
  const value = fixture({ attemptPhase: "prepared" });
  const callbackCompletion = completion(value.state, false);
  let observedContext;

  const result = await value.adapter.runPreparedCapture(
    reconciliationAdmission(),
    async (context) => {
      value.state.trace.push("callback:publish-prepared");
      observedContext = context;
      return callbackCompletion;
    },
  );

  assert.strictEqual(result, callbackCompletion);
  exactKeys(observedContext, [
    "artifactDirectory",
    "artifactOwnedRoot",
    "canonicalAttachment",
    "canonicalLease",
    "captureAttempt",
    "contractVersion",
    "now",
    "sourceDirectory",
    "sourceOwnedRoot",
    "storageRef",
  ]);
  assert.equal(Object.isFrozen(observedContext), true);
  assert.equal(observedContext.contractVersion, 1);
  assert.equal(observedContext.captureAttempt.state, "authorized");
  assert.equal(
    observedContext.captureAttempt.operationId,
    OPERATION_ID,
  );
  assert.equal(value.state.claimInputs.length, 1);
  exactKeys(value.state.claimInputs[0], [
    "expectedOperationRevision",
    "expectedSession",
    "kind",
    "operationId",
    "request",
  ]);
  assert.equal(value.state.claimInputs[0].expectedOperationRevision, "0");
  assert.deepEqual(value.state.trace, [
    "guard:start",
    "authority:read-attempt",
    "planner:artifact",
    "planner:source",
    "guard:probe:1",
    "authority:claim",
    "guard:probe:2",
    "callback:publish-prepared",
    "guard:probe:3",
    "authority:finalize",
    "guard:end",
  ]);
});

test("prepared capture never publishes when its claim is ambiguous, non-fresh, or stale", async (t) => {
  for (const scenario of [
    { attemptPhase: "prepared", claimMode: "throw-starting" },
    { attemptPhase: "prepared", claimMode: "nonfresh" },
    { attemptPhase: "authorized", claimMode: "success" },
  ]) {
    await t.test(`${scenario.attemptPhase}-${scenario.claimMode}`, async () => {
      const value = fixture(scenario);
      let callbackCalls = 0;

      await assertAuthorityError(
        value.adapter.runPreparedCapture(
          reconciliationAdmission(),
          async () => {
            callbackCalls += 1;
            return completion(value.state, false);
          },
        ),
        "postgres_checkpoint_mutation_authority_outcome_uncertain",
      );

      assert.equal(callbackCalls, 0);
      assert.equal(value.state.uncertainCalls, 0);
      assert.equal(
        value.state.claimCalls,
        scenario.attemptPhase === "prepared" ? 1 : 0,
      );
    });
  }
});

test("prepared capture marks a definitely dispatched publication failure uncertain", async () => {
  const value = fixture({ attemptPhase: "prepared" });

  await assertAuthorityError(
    value.adapter.runPreparedCapture(
      reconciliationAdmission(),
      async () => {
        throw new Error("private prepared publication failed");
      },
    ),
    "postgres_checkpoint_mutation_authority_outcome_uncertain",
  );

  assert.equal(value.state.claimCalls, 1);
  assert.equal(value.state.uncertainCalls, 1);
});

test("prepared capture finalize acknowledgement loss returns exact committed readback", async () => {
  const value = fixture({
    attemptPhase: "prepared",
    finalizeMode: "throw-after-commit",
  });
  const published = completion(value.state, false);
  let callbackCalls = 0;

  const result = await value.adapter.runPreparedCapture(
    reconciliationAdmission(),
    async () => {
      callbackCalls += 1;
      return published;
    },
  );

  assert.equal(callbackCalls, 1);
  assert.strictEqual(result, published);
  assert.equal(result.replayed, false);
  assert.deepEqual(
    structuredClone(result.artifactProof),
    structuredClone(published.artifactProof),
  );
  assert.deepEqual(
    structuredClone(result.materialization),
    structuredClone(published.materialization),
  );
  assert.deepEqual(
    structuredClone(result.result),
    structuredClone(published.result),
  );
  assert.equal(value.state.uncertainCalls, 0);
  assert.deepEqual(value.state.trace.slice(-4), [
    "authority:finalize",
    "guard:probe:4",
    "authority:read-attempt",
    "guard:end",
  ]);
});

test("authorized and committed reconciliation are source-free and return verifier completion by identity", async (t) => {
  for (const scenario of [
    {
      name: "authorized",
      options: {
        attemptPhase: "authorized",
        authorizedRevision: "1",
        distinctAttemptData: true,
      },
      expectedRevision: "1",
      expectedFinalized: true,
    },
    {
      name: "committed",
      options: { attemptPhase: "committed", committedRevision: "3" },
      expectedRevision: "2",
      expectedFinalized: false,
    },
  ]) {
    await t.test(scenario.name, async () => {
      const value = fixture(scenario.options);
      let verifierCalls = 0;
      const verifierCompletion = completion(value.state, true);
      const result = await value.adapter.runCaptureReconciliation(
        reconciliationAdmission(),
        async (context) => {
          value.state.trace.push("callback:verify");
          verifierCalls += 1;
          exactKeys(context, [
            "artifactDirectory",
            "artifactOwnedRoot",
            "captureAttempt",
          ]);
          return verifierCompletion;
        },
      );

      assert.strictEqual(result, verifierCompletion);
      assert.equal(verifierCalls, 1);
      assert.equal(value.sourcePlannerCalls, 0);
      assert.equal(value.artifactPlannerCalls, 1);
      assert.equal(
        value.state.finalizeInputs[0].expectedOperationRevision,
        scenario.expectedRevision,
      );
      assert.equal(
        value.state.finalizeInputs[0].completion,
        verifierCompletion,
      );
    });
  }
});

test("authorized reconciliation accepts an exact concurrent committed replay", async () => {
  const value = fixture({
    attemptPhase: "authorized",
    authorizedRevision: "1",
    laterSessionOnCommittedReplay: true,
  });
  let verifierCalls = 0;
  const verifierCompletion = completion(value.state, true);

  const result = await value.adapter.runCaptureReconciliation(
    reconciliationAdmission(),
    async (context) => {
      value.state.trace.push("callback:verify");
      verifierCalls += 1;
      assert.equal(context.captureAttempt.state, "authorized");
      value.state.trace.push("callback:concurrent-commit");
      value.state.attemptPhase = "committed";
      return verifierCompletion;
    },
  );

  assert.strictEqual(result, verifierCompletion);
  assert.equal(verifierCalls, 1);
  assert.equal(value.state.finalizeInputs.length, 1);
  assert.equal(
    value.state.finalizeInputs[0].expectedOperationRevision,
    "1",
  );
  assert.strictEqual(
    value.state.finalizeInputs[0].completion,
    verifierCompletion,
  );
  assert.equal(value.state.finalizeResults.length, 1);
  assert.equal(value.state.finalizeResults[0].status, "committed");
  assert.equal(value.state.finalizeResults[0].finalized, false);
  assert.equal(
    value.state.finalizeResults[0].attempt.captureAttemptId,
    CAPTURE_ATTEMPT_ID,
  );
  assert.equal(
    value.state.finalizeResults[0].attempt.state,
    "committed",
  );
  assert.equal(value.state.finalizeResults[0].session.sessionId, SESSION_ID);
  assert.equal(value.state.finalizeResults[0].session.revision, "42");
  assert.equal(value.state.uncertainCalls, 0);
  assert.equal(value.sourcePlannerCalls, 0);
  assert.equal(value.artifactPlannerCalls, 1);
  assert.deepEqual(value.state.trace, [
    "guard:start",
    "authority:read-attempt",
    "planner:artifact",
    "guard:probe:1",
    "callback:verify",
    "callback:concurrent-commit",
    "guard:probe:2",
    "authority:finalize",
    "guard:end",
  ]);
});

test("committed reconciliation tolerates later current session state", async () => {
  const value = fixture({
    attemptPhase: "committed",
    committedRevision: "3",
    laterSessionOnCommittedReplay: true,
  });
  let verifierCalls = 0;
  const verifierCompletion = completion(value.state, true);

  const result = await value.adapter.runCaptureReconciliation(
    reconciliationAdmission(),
    async () => {
      verifierCalls += 1;
      return verifierCompletion;
    },
  );

  assert.strictEqual(result, verifierCompletion);
  assert.equal(verifierCalls, 1);
  assert.equal(value.state.finalizeInputs.length, 1);
  assert.equal(
    value.state.finalizeInputs[0].expectedOperationRevision,
    "2",
  );
});

test("committed reconciliation rejects a session restored before capture commit", async () => {
  const value = fixture({
    attemptPhase: "committed",
    committedRevision: "3",
    staleHistoricalReadSession: true,
  });
  let verifierCalls = 0;

  await assertAuthorityError(
    value.adapter.runCaptureReconciliation(
      reconciliationAdmission(),
      async () => {
        verifierCalls += 1;
        return completion(value.state, true);
      },
    ),
    "postgres_checkpoint_mutation_authority_outcome_uncertain",
  );

  assert.equal(verifierCalls, 0);
  assert.equal(value.artifactPlannerCalls, 0);
  assert.equal(value.state.finalizeInputs.length, 0);
});

test("committed reconciliation rejects a terminal revision beyond PostgreSQL bigint", async () => {
  const value = fixture({
    attemptPhase: "committed",
    committedRevision: "2",
    expectedSession: nearRevisionExhaustionSession(),
    maximumHistoricalReadSession: true,
  });
  let verifierCalls = 0;

  await assertAuthorityError(
    value.adapter.runCaptureReconciliation(
      reconciliationAdmission(),
      async () => {
        verifierCalls += 1;
        return completion(value.state, true);
      },
    ),
    "postgres_checkpoint_mutation_authority_outcome_uncertain",
  );

  assert.equal(verifierCalls, 0);
  assert.equal(value.artifactPlannerCalls, 0);
  assert.equal(value.state.finalizeInputs.length, 0);
});

test("committed replay rejects a stale session finalization receipt", async () => {
  const value = fixture({
    attemptPhase: "committed",
    committedRevision: "3",
    staleHistoricalFinalizeSession: true,
  });
  let verifierCalls = 0;

  await assertAuthorityError(
    value.adapter.runCaptureReconciliation(
      reconciliationAdmission(),
      async () => {
        verifierCalls += 1;
        return completion(value.state, true);
      },
    ),
    "postgres_checkpoint_mutation_authority_outcome_uncertain",
  );

  assert.equal(verifierCalls, 1);
  assert.equal(value.artifactPlannerCalls, 1);
  assert.equal(value.state.finalizeInputs.length, 1);
});

test("committed reconciliation uses the captured BigInt string conversion intrinsic", async () => {
  const value = fixture({
    attemptPhase: "committed",
    committedRevision: "3",
    laterSessionOnCommittedReplay: true,
  });
  const verifierCompletion = completion(value.state, true);
  const toStringDescriptor = Object.getOwnPropertyDescriptor(
    BigInt.prototype,
    "toString",
  );

  try {
    Object.defineProperty(BigInt.prototype, "toString", {
      ...toStringDescriptor,
      value() {
        throw new Error("poisoned BigInt.prototype.toString");
      },
    });

    const result = await value.adapter.runCaptureReconciliation(
      reconciliationAdmission(),
      async () => verifierCompletion,
    );

    assert.strictEqual(result, verifierCompletion);
    assert.equal(value.state.finalizeInputs.length, 1);
    assert.equal(
      value.state.finalizeInputs[0].expectedOperationRevision,
      "2",
    );
  } finally {
    Object.defineProperty(
      BigInt.prototype,
      "toString",
      toStringDescriptor,
    );
  }
});

test("committed reconciliation rejects historical identity mismatch with a poisoned Array iterator", async () => {
  const value = fixture({
    attemptPhase: "committed",
    mismatchedHistoricalSessionIdentity: true,
  });
  value.adapter = createPostgresCheckpointMutationAuthority({
    authority: value.authority,
    operationGuard: Object.freeze(new FakeGuard(value.state)),
    resolveArtifactPaths() {
      return deepFreeze({
        artifactDirectory: ARTIFACT_DIRECTORY,
        artifactOwnedRoot: ARTIFACT_OWNED_ROOT,
      });
    },
    resolveSourceOwnedRoot() {
      return deepFreeze({
        sourceDirectory: SOURCE_DIRECTORY,
        sourceOwnedRoot: SOURCE_OWNED_ROOT,
      });
    },
  });
  const iteratorDescriptor = Object.getOwnPropertyDescriptor(
    Array.prototype,
    Symbol.iterator,
  );
  let verifierCalls = 0;
  let observedError = null;

  try {
    Object.defineProperty(Array.prototype, Symbol.iterator, {
      ...iteratorDescriptor,
      value: function* emptyArrayIterator() {},
    });
    try {
      await value.adapter.runCaptureReconciliation(
        reconciliationAdmission(),
        async () => {
          verifierCalls += 1;
          return completion(value.state, true);
        },
      );
    } catch (error) {
      observedError = error;
    }
  } finally {
    Object.defineProperty(
      Array.prototype,
      Symbol.iterator,
      iteratorDescriptor,
    );
  }

  assert.ok(
    observedError instanceof PostgresCheckpointMutationAuthorityError,
  );
  assert.equal(
    observedError.code,
    "postgres_checkpoint_mutation_authority_outcome_uncertain",
  );
  assert.equal(verifierCalls, 0);
  assert.equal(value.artifactPlannerCalls, 0);
  assert.equal(value.state.finalizeInputs.length, 0);
});

test("reconciliation rejects forged attempt receipts before verification", async (t) => {
  for (const readAttemptForgery of [
    "capture-attempt-id",
    "predetermined-result",
    "binding-reservation",
    "binding-coordinator",
    "authorized-catalogue",
  ]) {
    await t.test(readAttemptForgery, async () => {
      const value = fixture({ readAttemptForgery });
      let verifierCalls = 0;

      await assertAuthorityError(
        value.adapter.runCaptureReconciliation(
          reconciliationAdmission(),
          async () => {
            verifierCalls += 1;
            return completion(value.state, true);
          },
        ),
        "postgres_checkpoint_mutation_authority_outcome_uncertain",
      );

      assert.equal(verifierCalls, 0);
      assert.equal(value.artifactPlannerCalls, 0);
      assert.equal(value.sourcePlannerCalls, 0);
      assert.equal(value.state.finalizeInputs.length, 0);
    });
  }
});

test("admission content is snapshotted before capture and reconciliation awaits", async (t) => {
  await t.test("capture", async () => {
    let releaseRead;
    const readGate = new Promise((resolve) => {
      releaseRead = resolve;
    });
    const value = fixture({ readGate });
    const mutable = structuredClone(value.admission);
    const pending = value.adapter.runCapture(
      mutable,
      async () => completion(value.state, false),
    );
    mutable.request.operationId = "mutated-operation";
    mutable.checkpoint.artifactId = "mutated-artifact";
    releaseRead();
    await pending;
    assert.equal(value.state.baseInput.operationId, OPERATION_ID);
    assert.equal(
      value.state.baseInput.request.admission.checkpoint.artifactId,
      ARTIFACT_ID,
    );
  });

  await t.test("reconciliation", async () => {
    let releaseAttemptRead;
    const readAttemptGate = new Promise((resolve) => {
      releaseAttemptRead = resolve;
    });
    const value = fixture({ readAttemptGate });
    const mutable = structuredClone(reconciliationAdmission());
    const pending = value.adapter.runCaptureReconciliation(
      mutable,
      async () => completion(value.state, true),
    );
    mutable.request.operationId = "mutated-operation";
    mutable.checkpoint.artifactId = "mutated-artifact";
    releaseAttemptRead();
    await pending;
    assert.equal(
      value.state.finalizeInputs[0].operationId,
      OPERATION_ID,
    );
    assert.equal(
      value.state.finalizeInputs[0].request.admission.checkpoint.artifactId,
      ARTIFACT_ID,
    );
  });
});

test("restore fails closed without invoking its callback", async () => {
  const value = fixture();
  let callbackCalls = 0;

  await assertAuthorityError(
    value.adapter.runRestore(reconciliationAdmission(), async () => {
      callbackCalls += 1;
    }),
    "postgres_checkpoint_restore_unavailable",
  );
  assert.equal(callbackCalls, 0);
  assert.deepEqual(value.state.trace, []);
});

test("factory and planners reject proxy, async, generator, and thenable hazards", async (t) => {
  const value = fixture();
  const base = {
    authority: value.authority,
    operationGuard: Object.freeze(new FakeGuard(value.state)),
    resolveArtifactPaths() {
      return deepFreeze({
        artifactDirectory: ARTIFACT_DIRECTORY,
        artifactOwnedRoot: ARTIFACT_OWNED_ROOT,
      });
    },
    resolveSourceOwnedRoot() {
      return deepFreeze({
        sourceDirectory: SOURCE_DIRECTORY,
        sourceOwnedRoot: SOURCE_OWNED_ROOT,
      });
    },
  };

  for (const options of [
    new Proxy(base, {}),
    { ...base, resolveArtifactPaths: async () => ({}) },
    {
      ...base,
      resolveSourceOwnedRoot: function* resolveSourceOwnedRoot() {
        yield SOURCE_OWNED_ROOT;
      },
    },
    { ...base, authority: { ...value.authority, then() {} } },
  ]) {
    assert.throws(
      () => createPostgresCheckpointMutationAuthority(options),
      (error) =>
        error.code ===
        "invalid_postgres_checkpoint_mutation_authority_options",
    );
  }

  await t.test("thenable artifact plan", async () => {
    const adapter = createPostgresCheckpointMutationAuthority({
      ...base,
      resolveArtifactPaths() {
        return {
          artifactDirectory: ARTIFACT_DIRECTORY,
          artifactOwnedRoot: ARTIFACT_OWNED_ROOT,
          then() {},
        };
      },
    });
    await assertAuthorityError(
      adapter.runCapture(value.admission, async () => {
        assert.fail("publication must not run");
      }),
      "postgres_checkpoint_mutation_authority_outcome_uncertain",
    );
    assert.equal(value.state.reserveCalls, 0);
  });
});
