import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import test from "node:test";

import {
  PostgresWriterForceFenceAtomicCaptureCompositionError,
  createPostgresWriterForceFenceAtomicCaptureComposition,
  isPostgresWriterForceFenceAtomicCaptureComposition,
} from "../src/postgres-writer-force-fence-atomic-capture-composition.mjs";
import { PostgresOperationGuard } from "../src/postgres-operation-guard.mjs";
import {
  ATOMIC_CRASH_CAPTURE_OPERATION_KIND,
  SESSION_AUTHORITY_DOCUMENT_VERSION,
  WRITER_ATTACHMENT_ACQUIRE_OPERATION_KIND,
  WRITER_FORCE_FENCE_OPERATION_KIND,
  assertSessionOperationBinding,
  createWriterForceFenceAtomicCaptureOperationRequest,
} from "../src/postgres-session-authority.mjs";
import { createSessionManifest } from "../src/session-storage-contracts.mjs";

const SESSION_ID = "019f2200-0000-7000-8000-000000000001";
const THREAD_ID = "019f2200-0000-7000-8000-000000000002";
const BACKEND_ID = "reconcilable-fence-test";
const STORAGE_ID = "volume-001";
const ATTACHMENT_ID = "attachment-001";
const FENCE_OPERATION_ID = "writer-force-fence-v2-001";
const CAPTURE_OPERATION_ID = "atomic-crash-capture-001";
const CAPTURE_MUTATION_OPERATION_ID = "atomic-crash-provider-001";
const NOW = "2026-08-29T06:00:00.000Z";

function deepFreeze(value, seen = new Set()) {
  if (
    value === null ||
    (typeof value !== "object" && typeof value !== "function") ||
    seen.has(value)
  ) {
    return value;
  }
  seen.add(value);
  for (const key of Reflect.ownKeys(value)) {
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    if (descriptor && Object.hasOwn(descriptor, "value")) {
      deepFreeze(descriptor.value, seen);
    }
  }
  return Object.freeze(value);
}

function nullRecord(entries) {
  const value = Object.create(null);
  for (const [key, entry] of entries) value[key] = entry;
  return Object.freeze(value);
}

function frozenFunction(value) {
  return Object.freeze(value);
}

function sha256Json(value) {
  return createHash("sha256").update(JSON.stringify(value), "utf8").digest("hex");
}

function manifest() {
  return createSessionManifest({
    sessionId: SESSION_ID,
    codex: {
      rootThreadId: THREAD_ID,
      sessionId: THREAD_ID,
      ephemeral: false,
      historyMode: "paginated",
    },
    runtime: {
      imageDigest: `sha256:${"a".repeat(64)}`,
      imageMediaType: "application/vnd.oci.image.manifest.v1+json",
      platform: "linux/arm64",
      codexVersion: "codex-cli 0.144.0",
      codexSandbox: "danger-full-access",
    },
  });
}

function capabilities() {
  return {
    atomicPointInTimeCheckpoint: true,
    exclusiveWriterAttachment: true,
    fencing: "epoch-enforced",
    normalDirectoryAttachment: true,
  };
}

function lease() {
  return {
    contractVersion: 1,
    sessionId: SESSION_ID,
    leaseId: "lease-001",
    holderId: "host-001",
    fencingEpoch: "11",
    expiresAt: "2026-08-29T07:00:00.000Z",
  };
}

function attachment() {
  return {
    contractVersion: 1,
    backendId: BACKEND_ID,
    storageId: STORAGE_ID,
    sessionId: SESSION_ID,
    attachmentId: ATTACHMENT_ID,
    leaseId: "lease-001",
    holderId: "host-001",
    fencingEpoch: "11",
    operationId: "writer-attach-001",
    proofId: "proof-attach-001",
    kind: "directory",
    rootPath: "/var/lib/portable-codex/session-001",
    mode: "read-write",
  };
}

function attachedLastOperation() {
  return {
    conflictClass: "session-mutation",
    expectedSessionRevision: "0",
    kind: WRITER_ATTACHMENT_ACQUIRE_OPERATION_KIND,
    operationId: "writer-attach-001",
    operationRevision: "2",
    requestSha256: "a".repeat(64),
    reservationId: "writer-attach-reservation-001",
    resultSha256: "b".repeat(64),
    state: "committed",
  };
}

function authorityDocument({
  activeOperation = null,
  currentAttachment = attachment(),
  currentLease = lease(),
  lastOperation = attachedLastOperation(),
  lifecycle = "ATTACHED",
  writerEpoch = currentLease?.fencingEpoch ?? "11",
} = {}) {
  return {
    documentVersion: SESSION_AUTHORITY_DOCUMENT_VERSION,
    manifest: manifest(),
    storageRef: {
      contractVersion: 1,
      backendId: BACKEND_ID,
      storageId: STORAGE_ID,
      sessionId: SESSION_ID,
    },
    backendCapabilities: capabilities(),
    lifecycle,
    writerEpoch,
    lease: currentLease,
    attachment: currentAttachment,
    activeOperation,
    lastOperation,
    recovery: null,
    launch: null,
  };
}

function sessionSnapshot(document = authorityDocument(), revision = "3") {
  return deepFreeze({
    createdAt: NOW,
    document,
    revision,
    sessionId: SESSION_ID,
    updatedAt: NOW,
  });
}

function atomicRequest(expectedSession) {
  const sourceAttachment = expectedSession.document.attachment;
  return {
    captureAttemptId: "atomic-crash-attempt-001",
    checkpoint: {
      artifactId: "atomic-crash-artifact-001",
      backendId: BACKEND_ID,
      checkpointClass: "crash-prefix",
      checkpointId: "atomic-crash-checkpoint-001",
      codexSessionId: THREAD_ID,
      codexThreadId: THREAD_ID,
      contractVersion: 1,
      createdAt: NOW,
      imageDigest: expectedSession.document.manifest.runtime.imageDigest,
      sessionId: SESSION_ID,
      sourceFencingEpoch: sourceAttachment.fencingEpoch,
      storageId: STORAGE_ID,
    },
    contractVersion: 1,
    mutationRequest: {
      backendId: BACKEND_ID,
      contractVersion: 1,
      fencingEpoch: sourceAttachment.fencingEpoch,
      holderId: sourceAttachment.holderId,
      leaseId: sourceAttachment.leaseId,
      operation: "checkpoint",
      operationId: CAPTURE_MUTATION_OPERATION_ID,
      sessionId: SESSION_ID,
      storageId: STORAGE_ID,
      target: {
        artifactId: "atomic-crash-artifact-001",
        checkpointId: "atomic-crash-checkpoint-001",
        kind: "checkpoint",
      },
    },
    sourceAttachment: structuredClone(sourceAttachment),
    storageRef: structuredClone(expectedSession.document.storageRef),
  };
}

function request(expectedSession = sessionSnapshot()) {
  return {
    expectedSession,
    operationId: FENCE_OPERATION_ID,
    request: createWriterForceFenceAtomicCaptureOperationRequest({
      atomicCaptureOperationId: CAPTURE_OPERATION_ID,
      atomicRequest: atomicRequest(expectedSession),
      expectedSession,
      fenceOperationId: FENCE_OPERATION_ID,
    }),
  };
}

function fenceRequest() {
  return {
    backendId: BACKEND_ID,
    contractVersion: 1,
    fencingEpoch: "12",
    operationId: FENCE_OPERATION_ID,
    revokedFence: {
      fencingEpoch: "11",
      holderId: "host-001",
      leaseId: "lease-001",
    },
    sessionId: SESSION_ID,
    storageId: STORAGE_ID,
    target: { attachmentId: ATTACHMENT_ID, kind: "attachment" },
  };
}

function fenceResult(requestValue = fenceRequest()) {
  return {
    ...structuredClone(requestValue),
    proofId: "proof-force-fence-001",
    status: "fenced",
  };
}

function operationResult(outcome, proof = null) {
  if (outcome === "writer-fenced") {
    return {
      resultVersion: 1,
      outcome,
      writerEpoch: "12",
      lease: lease(),
      attachment: attachment(),
      fenceTarget: { attachmentId: ATTACHMENT_ID, kind: "attachment" },
      fenceResult: proof,
    };
  }
  return {
    resultVersion: 1,
    outcome: "writer-blocked",
    reason: "provider-outcome-unresolved",
    writerEpoch: "12",
    lease: lease(),
    attachment: attachment(),
    fenceTarget: { attachmentId: ATTACHMENT_ID, kind: "attachment" },
  };
}

function activePointer(binding, state, revision) {
  return {
    conflictClass: "session-mutation",
    expectedSessionRevision: binding.expectedSession.revision,
    kind: binding.kind,
    operationId: binding.operationId,
    operationRevision: revision,
    requestSha256: binding.requestSha256,
    reservationId: binding.reservationId,
    state,
  };
}

function terminalPointer(binding, revision, result) {
  return {
    ...activePointer(binding, "committed", revision),
    resultSha256: sha256Json(result),
  };
}

function operationRecord(binding, state, result = null) {
  const revision = {
    prepared: "0",
    starting: "1",
    uncertain: "2",
    committed: result?.outcome === "writer-blocked" ? "3" : "2",
  }[state];
  return {
    conflictClass: "session-mutation",
    createdAt: NOW,
    expectedSession: binding.expectedSession,
    kind: binding.kind,
    operationId: binding.operationId,
    request: binding.request,
    requestSha256: binding.requestSha256,
    result,
    retiredAt: state === "committed" ? NOW : null,
    revision,
    sessionId: SESSION_ID,
    state,
    updatedAt: NOW,
  };
}

function reservationRecord(binding, state) {
  return {
    conflictClass: "session-mutation",
    createdAt: NOW,
    expectedSessionRevision: binding.expectedSession.revision,
    kind: binding.kind,
    operationId: binding.operationId,
    releasedAt: state === "committed" ? NOW : null,
    requestSha256: binding.requestSha256,
    reservationId: binding.reservationId,
    sessionId: SESSION_ID,
    state: state === "committed" ? "released" : state,
    updatedAt: NOW,
    expiresAt: null,
  };
}

class GuardLockManager {
  constructor() {
    this.holder = null;
  }

  tryAcquire(client) {
    if (this.holder !== null && this.holder !== client) return false;
    this.holder = client;
    return true;
  }

  isHeld(client) {
    return this.holder === client;
  }

  unlock(client) {
    if (!this.isHeld(client)) return false;
    this.holder = null;
    return true;
  }
}

class GuardClient {
  constructor(manager, mode) {
    this.manager = manager;
    this.mode = mode;
    this.probeCount = 0;
  }

  query(query) {
    const text = query.text;
    if (text === "DISCARD ALL") {
      this.manager.unlock(this);
      query.callback(null, { command: "DISCARD", rows: [] });
      return undefined;
    }
    if (text.includes("pg_try_advisory_lock")) {
      query.callback(null, {
        command: "SELECT",
        rows: [
          {
            acquired:
              this.mode !== "busy" && this.manager.tryAcquire(this),
            backend_pid: 8101,
          },
        ],
      });
      return undefined;
    }
    if (text.includes("FROM pg_catalog.pg_locks")) {
      this.probeCount += 1;
      query.callback(null, {
        command: "SELECT",
        rows: [
          {
            backend_pid: 8101,
            lock_held:
              this.manager.isHeld(this) &&
              !(
                this.mode === "lose-after-provider" &&
                this.probeCount >= 4
              ),
          },
        ],
      });
      return undefined;
    }
    if (text.includes("pg_advisory_unlock")) {
      query.callback(null, {
        command: "SELECT",
        rows: [{ backend_pid: 8101, unlocked: this.manager.unlock(this) }],
      });
      return undefined;
    }
    query.callback(new Error(`unexpected guard query: ${text}`));
    return undefined;
  }

  release() {
    this.manager.unlock(this);
  }
}

class GuardPool {
  constructor(mode) {
    this.manager = new GuardLockManager();
    this.mode = mode;
  }

  connect(callback) {
    callback(null, new GuardClient(this.manager, this.mode));
  }
}

function createHarness({
  guardMode = "normal",
  provider = {},
  reconciliation = true,
} = {}) {
  const calls = [];
  const injections = new Map();
  const initialRequest = request();
  const base = deepFreeze({
    expectedSession: initialRequest.expectedSession,
    kind: WRITER_FORCE_FENCE_OPERATION_KIND,
    operationId: initialRequest.operationId,
    request: initialRequest.request,
  });
  const binding = assertSessionOperationBinding(base);
  const state = {
    base,
    binding,
    fenceProof: null,
    phase: "absent",
    session: base.expectedSession,
  };

  function enqueue(method, injection) {
    const queue = injections.get(method) ?? [];
    queue.push(injection);
    injections.set(method, queue);
  }

  async function applyInjection(method, input, mutation) {
    calls.push([method, structuredClone(input)]);
    const queue = injections.get(method);
    const injection = queue?.shift();
    if (injection?.timing === "before") throw injection.error;
    const result = await mutation();
    if (injection?.timing === "after") throw injection.error;
    return injection?.result ?? result;
  }

  function phaseSession(phase) {
    const phaseRevision = {
      prepared: "4",
      starting: "5",
      uncertain: "6",
    }[phase];
    const operationRevision = {
      prepared: "0",
      starting: "1",
      uncertain: "2",
    }[phase];
    return sessionSnapshot(
      authorityDocument({
        activeOperation: activePointer(binding, phase, operationRevision),
        lifecycle: phase === "prepared" ? "ATTACHED" : "FENCING",
        writerEpoch: phase === "prepared" ? "11" : "12",
      }),
      phaseRevision,
    );
  }

  function blockedSession(result) {
    return sessionSnapshot(
      authorityDocument({
        activeOperation: null,
        lastOperation: terminalPointer(binding, "3", result),
        lifecycle: "BLOCKED",
        writerEpoch: "12",
      }),
      "7",
    );
  }

  function captureHandoff(proof = state.fenceProof, finalized = false) {
    const result = operationResult("writer-fenced", proof);
    const fenceOperation = deepFreeze(
      operationRecord(binding, "committed", result),
    );
    const fenceReservation = deepFreeze(
      reservationRecord(binding, "committed"),
    );
    const terminalSession = sessionSnapshot(
      authorityDocument({
        activeOperation: null,
        currentAttachment: null,
        currentLease: null,
        lastOperation: terminalPointer(binding, "2", result),
        lifecycle: "DETACHED",
        writerEpoch: "12",
      }),
      "6",
    );
    const captureBase = deepFreeze({
      expectedSession: terminalSession,
      kind: ATOMIC_CRASH_CAPTURE_OPERATION_KIND,
      operationId: CAPTURE_OPERATION_ID,
      request: base.request.atomicCapture,
    });
    const captureBinding = assertSessionOperationBinding(captureBase);
    const captureOperation = deepFreeze(
      operationRecord(captureBinding, "prepared"),
    );
    const captureReservation = deepFreeze(
      reservationRecord(captureBinding, "prepared"),
    );
    const finalSession = sessionSnapshot(
      authorityDocument({
        activeOperation: activePointer(captureBinding, "prepared", "0"),
        currentAttachment: null,
        currentLease: null,
        lastOperation: terminalPointer(binding, "2", result),
        lifecycle: "DETACHED",
        writerEpoch: "12",
      }),
      "7",
    );
    return deepFreeze({
      capture: {
        operation: captureOperation,
        reservation: captureReservation,
      },
      fence: {
        finalized,
        operation: fenceOperation,
        reservation: fenceReservation,
      },
      session: finalSession,
      status: "prepared",
    });
  }

  function genericReceipt(extra = {}) {
    if (state.phase === "absent") {
      return deepFreeze({
        operation: null,
        reservation: null,
        session: state.session,
        status: "absent",
        ...extra,
      });
    }
    let result = null;
    if (state.phase === "blocked") {
      result = operationResult("writer-blocked");
    } else if (state.phase === "handoff") {
      result = operationResult("writer-fenced", state.fenceProof);
    }
    const operationState =
      state.phase === "blocked" || state.phase === "handoff"
        ? "committed"
        : state.phase;
    return deepFreeze({
      operation: operationRecord(binding, operationState, result),
      reservation: reservationRecord(binding, operationState),
      session: state.session,
      ...extra,
    });
  }

  function setPhase(phase) {
    state.phase = phase;
    if (["prepared", "starting", "uncertain"].includes(phase)) {
      state.session = phaseSession(phase);
    } else if (phase === "blocked") {
      state.session = blockedSession(operationResult("writer-blocked"));
    } else if (phase === "handoff") {
      state.session = captureHandoff().session;
    }
  }

  const reserveOperation = frozenFunction((input) =>
    applyInjection("reserveOperation", input, async () => {
      if (state.phase === "absent") setPhase("prepared");
      return genericReceipt({ acquired: state.phase === "prepared" });
    }),
  );
  const claimWriterForceFenceDispatch = frozenFunction((input) =>
    applyInjection("claimWriterForceFenceDispatch", input, async () => {
      if (state.phase === "prepared") {
        setPhase("starting");
        return genericReceipt({
          dispatchGranted: true,
          fenceRequest: fenceRequest(),
          writerEpoch: "12",
        });
      }
      return genericReceipt({
        dispatchGranted: false,
        ...(["starting", "uncertain"].includes(state.phase)
          ? { fenceRequest: fenceRequest(), writerEpoch: "12" }
          : {}),
      });
    }),
  );
  const reconcileWriterForceFenceAtomicCaptureHandoff = frozenFunction(
    (input) =>
      applyInjection(
        "reconcileWriterForceFenceAtomicCaptureHandoff",
        input,
        async () => {
          if (state.phase === "handoff") return captureHandoff();
          return genericReceipt(
            ["starting", "uncertain"].includes(state.phase)
              ? { fenceRequest: fenceRequest(), writerEpoch: "12" }
              : {},
          );
        },
      ),
  );
  const markOperationUncertain = frozenFunction((input) =>
    applyInjection("markOperationUncertain", input, async () => {
      if (state.phase === "starting") setPhase("uncertain");
      return genericReceipt({ changed: state.phase === "uncertain" });
    }),
  );
  const finalizeWriterOperationBlocked = frozenFunction((input) =>
    applyInjection("finalizeWriterOperationBlocked", input, async () => {
      if (state.phase === "uncertain") setPhase("blocked");
      return genericReceipt({ finalized: state.phase === "blocked" });
    }),
  );
  const finalizeWriterForceFenceAtomicCaptureHandoff = frozenFunction(
    (input) =>
      applyInjection(
        "finalizeWriterForceFenceAtomicCaptureHandoff",
        input,
        async () => {
          state.fenceProof = deepFreeze(structuredClone(input.fenceResult));
          setPhase("handoff");
          return captureHandoff(state.fenceProof, true);
        },
      ),
  );

  const authority = nullRecord([
    ["claimWriterForceFenceDispatch", claimWriterForceFenceDispatch],
    [
      "finalizeWriterForceFenceAtomicCaptureHandoff",
      finalizeWriterForceFenceAtomicCaptureHandoff,
    ],
    ["finalizeWriterOperationBlocked", finalizeWriterOperationBlocked],
    ["markOperationUncertain", markOperationUncertain],
    [
      "reconcileWriterForceFenceAtomicCaptureHandoff",
      reconcileWriterForceFenceAtomicCaptureHandoff,
    ],
    ["reserveOperation", reserveOperation],
  ]);

  const forceFence = frozenFunction((input) => {
    calls.push(["forceFence", structuredClone(input)]);
    if (provider.forceFence) return provider.forceFence(input);
    return Promise.resolve(deepFreeze(fenceResult(input)));
  });
  const reconcileForceFence = frozenFunction((input) => {
    calls.push(["reconcileForceFence", structuredClone(input)]);
    if (provider.reconcileForceFence) {
      return provider.reconcileForceFence(input);
    }
    return Promise.resolve(
      deepFreeze({ contractVersion: 1, outcome: "unknown", result: null }),
    );
  });
  const noop = frozenFunction(async () => undefined);
  const storageBackendEntries = [
    ["contractVersion", 1],
    ["backendId", BACKEND_ID],
    ["capabilities", deepFreeze(capabilities())],
    ["captureCheckpoint", noop],
    ["destroySession", noop],
    ["detachAttachment", noop],
    ["forceFence", forceFence],
    ["prepareWritableAttachment", noop],
    ["provisionSession", noop],
    ["restoreCheckpoint", noop],
  ];
  if (reconciliation) {
    storageBackendEntries.push(
      ["forceFenceReconciliationContractVersion", 1],
      ["reconcileForceFence", reconcileForceFence],
    );
  }
  const storageBackend = nullRecord(storageBackendEntries);
  const operationGuard = new PostgresOperationGuard({
    dedicatedPool: new GuardPool(guardMode),
  });
  const composition = createPostgresWriterForceFenceAtomicCaptureComposition({
    authority,
    operationGuard,
    storageBackend,
  });

  return {
    authority,
    calls,
    composition,
    enqueue,
    request: initialRequest,
    setPhase,
    state,
    storageBackend,
  };
}

function methodCalls(harness) {
  return harness.calls.map(([name]) => name);
}

function assertCompositionError(code) {
  return (error) =>
    error instanceof PostgresWriterForceFenceAtomicCaptureCompositionError &&
    error.code === code &&
    error.retryable === false &&
    Object.isFrozen(error);
}

test("factory returns one frozen branded private V2 facade", () => {
  const fixture = createHarness();

  assert.deepEqual(Reflect.ownKeys(fixture.composition), [
    "forceFenceWriterAtomicCapture",
  ]);
  assert.equal(Object.isFrozen(fixture.composition), true);
  assert.equal(
    Object.isFrozen(fixture.composition.forceFenceWriterAtomicCapture),
    true,
  );
  assert.equal(
    isPostgresWriterForceFenceAtomicCaptureComposition(fixture.composition),
    true,
  );
  assert.equal(
    isPostgresWriterForceFenceAtomicCaptureComposition(
      Object.create(fixture.composition),
    ),
    false,
  );
});

test("fresh V2 force-fence returns one prepared capture proof", async () => {
  const fixture = createHarness();

  const proof = await fixture.composition.forceFenceWriterAtomicCapture(
    fixture.request,
  );

  assert.equal(proof.fence.operation.result.outcome, "writer-fenced");
  assert.equal(proof.capture.operation.state, "prepared");
  assert.equal(proof.capture.reservation.state, "prepared");
  assert.equal(proof.session.document.lifecycle, "DETACHED");
  assert.equal(
    proof.session.document.activeOperation.operationId,
    CAPTURE_OPERATION_ID,
  );
  assert.deepEqual(methodCalls(fixture), [
    "reserveOperation",
    "claimWriterForceFenceDispatch",
    "forceFence",
    "finalizeWriterForceFenceAtomicCaptureHandoff",
  ]);
  assert.equal(Object.isFrozen(proof), true);
});

test("committed handoff replay performs no provider call", async () => {
  const fixture = createHarness();
  const first = await fixture.composition.forceFenceWriterAtomicCapture(
    fixture.request,
  );
  fixture.calls.length = 0;

  const replay = await fixture.composition.forceFenceWriterAtomicCapture(
    fixture.request,
  );

  assert.deepEqual(replay, first);
  assert.deepEqual(methodCalls(fixture), [
    "reserveOperation",
    "reconcileWriterForceFenceAtomicCaptureHandoff",
  ]);
  assert.equal(methodCalls(fixture).includes("forceFence"), false);
  assert.equal(methodCalls(fixture).includes("reconcileForceFence"), false);
});

test("claim acknowledgement loss reconciles without redispatch", async () => {
  const fixture = createHarness({
    provider: {
      reconcileForceFence(input) {
        return Promise.resolve(
          deepFreeze({
            contractVersion: 1,
            outcome: "committed",
            result: fenceResult(input),
          }),
        );
      },
    },
  });
  fixture.enqueue("claimWriterForceFenceDispatch", {
    error: new Error("claim acknowledgement lost"),
    timing: "after",
  });

  const proof = await fixture.composition.forceFenceWriterAtomicCapture(
    fixture.request,
  );

  assert.equal(proof.fence.operation.result.outcome, "writer-fenced");
  assert.deepEqual(methodCalls(fixture), [
    "reserveOperation",
    "claimWriterForceFenceDispatch",
    "reconcileWriterForceFenceAtomicCaptureHandoff",
    "reconcileForceFence",
    "finalizeWriterForceFenceAtomicCaptureHandoff",
  ]);
  assert.equal(methodCalls(fixture).includes("forceFence"), false);
});

test("unknown starting provider outcome blocks without capture", async () => {
  const fixture = createHarness();
  fixture.setPhase("starting");

  const blocked = await fixture.composition.forceFenceWriterAtomicCapture(
    fixture.request,
  );

  assert.equal(blocked.operation.result.outcome, "writer-blocked");
  assert.equal(
    blocked.operation.result.reason,
    "provider-outcome-unresolved",
  );
  assert.equal(blocked.session.document.lifecycle, "BLOCKED");
  assert.equal(blocked.session.document.activeOperation, null);
  assert.equal(Object.hasOwn(blocked, "capture"), false);
  assert.deepEqual(methodCalls(fixture), [
    "reserveOperation",
    "reconcileWriterForceFenceAtomicCaptureHandoff",
    "reconcileForceFence",
    "markOperationUncertain",
    "finalizeWriterOperationBlocked",
  ]);
  assert.equal(methodCalls(fixture).includes("forceFence"), false);
});

test("ambiguous provider failure reconciles then commits BLOCKED", async () => {
  const fixture = createHarness({
    provider: {
      forceFence() {
        return Promise.reject(new Error("force-fence acknowledgement lost"));
      },
    },
  });

  const blocked = await fixture.composition.forceFenceWriterAtomicCapture(
    fixture.request,
  );

  assert.equal(blocked.operation.result.outcome, "writer-blocked");
  assert.deepEqual(methodCalls(fixture), [
    "reserveOperation",
    "claimWriterForceFenceDispatch",
    "forceFence",
    "reconcileForceFence",
    "markOperationUncertain",
    "finalizeWriterOperationBlocked",
  ]);
});

test("base backend without reconciliation fails closed after ambiguity", async () => {
  const fixture = createHarness({
    provider: {
      forceFence() {
        return Promise.reject(new Error("force-fence outcome unknown"));
      },
    },
    reconciliation: false,
  });

  const blocked = await fixture.composition.forceFenceWriterAtomicCapture(
    fixture.request,
  );

  assert.equal(blocked.operation.result.outcome, "writer-blocked");
  assert.deepEqual(methodCalls(fixture), [
    "reserveOperation",
    "claimWriterForceFenceDispatch",
    "forceFence",
    "markOperationUncertain",
    "finalizeWriterOperationBlocked",
  ]);
});

test("handoff acknowledgement loss reads back without refencing", async () => {
  const fixture = createHarness();
  fixture.enqueue("finalizeWriterForceFenceAtomicCaptureHandoff", {
    error: new Error("handoff acknowledgement lost"),
    timing: "after",
  });

  const proof = await fixture.composition.forceFenceWriterAtomicCapture(
    fixture.request,
  );

  assert.equal(proof.capture.operation.state, "prepared");
  assert.deepEqual(methodCalls(fixture), [
    "reserveOperation",
    "claimWriterForceFenceDispatch",
    "forceFence",
    "finalizeWriterForceFenceAtomicCaptureHandoff",
    "reconcileWriterForceFenceAtomicCaptureHandoff",
  ]);
  assert.equal(
    methodCalls(fixture).filter((name) => name === "forceFence").length,
    1,
  );
});

test("handoff finalizer rollback retries only the dedicated finalizer", async () => {
  const fixture = createHarness();
  fixture.enqueue("finalizeWriterForceFenceAtomicCaptureHandoff", {
    error: new Error("handoff transaction rolled back"),
    timing: "before",
  });

  const proof = await fixture.composition.forceFenceWriterAtomicCapture(
    fixture.request,
  );

  assert.equal(proof.capture.operation.state, "prepared");
  assert.deepEqual(methodCalls(fixture), [
    "reserveOperation",
    "claimWriterForceFenceDispatch",
    "forceFence",
    "finalizeWriterForceFenceAtomicCaptureHandoff",
    "reconcileWriterForceFenceAtomicCaptureHandoff",
    "finalizeWriterForceFenceAtomicCaptureHandoff",
  ]);
  assert.equal(
    methodCalls(fixture).filter((name) => name === "forceFence").length,
    1,
  );
  assert.equal(methodCalls(fixture).includes("finalizeWriterOperationBlocked"), false);
});

test("guard contention prevents all authority and provider work", async () => {
  const fixture = createHarness({ guardMode: "busy" });

  await assert.rejects(
    fixture.composition.forceFenceWriterAtomicCapture(fixture.request),
    assertCompositionError(
      "postgres_writer_force_fence_atomic_capture_outcome_uncertain",
    ),
  );

  assert.deepEqual(fixture.calls, []);
});

test("guard loss after provider success prevents authority writes", async () => {
  const fixture = createHarness({ guardMode: "lose-after-provider" });

  await assert.rejects(
    fixture.composition.forceFenceWriterAtomicCapture(fixture.request),
    assertCompositionError(
      "postgres_writer_force_fence_atomic_capture_outcome_uncertain",
    ),
  );

  assert.deepEqual(methodCalls(fixture), [
    "reserveOperation",
    "claimWriterForceFenceDispatch",
    "forceFence",
  ]);
  assert.equal(
    methodCalls(fixture).includes("finalizeWriterOperationBlocked"),
    false,
  );
  assert.equal(
    methodCalls(fixture).includes(
      "finalizeWriterForceFenceAtomicCaptureHandoff",
    ),
    false,
  );
});

test("hostile outer input is rejected before authority dispatch", async () => {
  const fixture = createHarness();

  await assert.rejects(
    fixture.composition.forceFenceWriterAtomicCapture(
      new Proxy(fixture.request, {}),
    ),
    assertCompositionError(
      "invalid_postgres_writer_force_fence_atomic_capture_composition_request",
    ),
  );

  assert.deepEqual(fixture.calls, []);
});
