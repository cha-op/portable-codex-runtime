import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import test from "node:test";

import {
  LVM_ATOMIC_CRASH_CAPTURE_BINDING_KIND,
  LVM_ATOMIC_CRASH_CAPTURE_DRIVER_CONTRACT_VERSION,
  LVM_ATOMIC_CRASH_CAPTURE_PROVIDER_CONTRACT_VERSION,
  assertLvmAtomicCrashCaptureProviderBinding,
} from "../src/lvm-atomic-crash-capture-provider.mjs";
import {
  PostgresWriterForceFenceLvmAtomicCrashCaptureCompositionError,
  createPostgresWriterForceFenceLvmAtomicCrashCaptureComposition,
  isPostgresWriterForceFenceLvmAtomicCrashCaptureComposition,
} from "../src/postgres-writer-force-fence-lvm-atomic-crash-capture-composition.mjs";
import { PostgresOperationGuard } from "../src/postgres-operation-guard.mjs";
import {
  ATOMIC_CRASH_CAPTURE_OPERATION_KIND,
  SESSION_AUTHORITY_DOCUMENT_VERSION,
  WRITER_FORCE_FENCE_OPERATION_KIND,
  assertSessionOperationBinding,
  assertSessionOperationTransitionProof,
} from "../src/postgres-session-authority.mjs";
import {
  ATOMIC_CRASH_CAPTURE_CONTRACT_VERSION,
  createSessionManifest,
} from "../src/session-storage-contracts.mjs";

const BACKEND_ID = "lvm-force-fence-capture-test";
const SESSION_ID = "019f2300-0000-7000-8000-000000000001";
const THREAD_ID = "019f2300-0000-7000-8000-000000000002";
const CAPTURE_OPERATION_ID = "atomic-crash-capture-001";
const PROVIDER_OPERATION_ID = "atomic-crash-provider-001";
const NOW = "2026-08-29T08:00:00.000Z";
const LATER = "2026-08-29T08:00:01.000Z";
const CONTENT_SHA256 = "b".repeat(64);
const ORIGIN_LV_UUID = "ORIGIN-1234567890";
const SNAPSHOT_LV_UUID = "SNAPSHOT-1234567890";

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

function exact(values) {
  return Object.freeze(Object.assign(Object.create(null), values));
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

function fencePointer() {
  return {
    conflictClass: "session-mutation",
    expectedSessionRevision: "3",
    kind: WRITER_FORCE_FENCE_OPERATION_KIND,
    operationId: "writer-force-fence-v2-001",
    operationRevision: "2",
    requestSha256: "a".repeat(64),
    reservationId: "writer-force-fence-reservation-001",
    resultSha256: "b".repeat(64),
    state: "committed",
  };
}

function authorityDocument({
  activeOperation = null,
  lastOperation = fencePointer(),
  lifecycle = "DETACHED",
} = {}) {
  return {
    documentVersion: SESSION_AUTHORITY_DOCUMENT_VERSION,
    manifest: manifest(),
    storageRef: {
      contractVersion: 1,
      backendId: BACKEND_ID,
      storageId: "storage-001",
      sessionId: SESSION_ID,
    },
    backendCapabilities: capabilities(),
    lifecycle,
    writerEpoch: "12",
    lease: null,
    attachment: null,
    activeOperation,
    lastOperation,
    recovery: null,
    launch: null,
  };
}

function sessionSnapshot(document, revision, updatedAt = NOW) {
  return deepFreeze({
    createdAt: NOW,
    document,
    revision,
    sessionId: SESSION_ID,
    updatedAt,
  });
}

function sourceAttachment(overrides = {}) {
  return {
    attachmentId: "attachment-001",
    backendId: BACKEND_ID,
    contractVersion: 1,
    fencingEpoch: "11",
    holderId: "host-001",
    kind: "directory",
    leaseId: "lease-001",
    mode: "read-write",
    operationId: "writer-attach-001",
    proofId: "attach-proof-001",
    rootPath: "/private/source/must-not-be-opened",
    sessionId: SESSION_ID,
    storageId: "storage-001",
    ...overrides,
  };
}

function captureRequest(overrides = {}) {
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
      imageDigest: `sha256:${"a".repeat(64)}`,
      sessionId: SESSION_ID,
      sourceFencingEpoch: "11",
      storageId: "storage-001",
    },
    contractVersion: ATOMIC_CRASH_CAPTURE_CONTRACT_VERSION,
    mutationRequest: {
      backendId: BACKEND_ID,
      contractVersion: 1,
      fencingEpoch: "11",
      holderId: "host-001",
      leaseId: "lease-001",
      operation: "checkpoint",
      operationId: PROVIDER_OPERATION_ID,
      sessionId: SESSION_ID,
      storageId: "storage-001",
      target: {
        artifactId: "atomic-crash-artifact-001",
        checkpointId: "atomic-crash-checkpoint-001",
        kind: "checkpoint",
      },
    },
    sourceAttachment: sourceAttachment(),
    storageRef: {
      backendId: BACKEND_ID,
      contractVersion: 1,
      sessionId: SESSION_ID,
      storageId: "storage-001",
    },
    ...overrides,
  };
}

function committedResult(request, overrides = {}) {
  const { artifact: artifactOverrides = {}, ...outerOverrides } = overrides;
  return {
    artifact: {
      byteLength: "8",
      contentSha256: CONTENT_SHA256,
      objectId: SNAPSHOT_LV_UUID,
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
    proofId: "lvm-proof-001",
    sessionId: request.storageRef.sessionId,
    sourceFencingEpoch: request.checkpoint.sourceFencingEpoch,
    status: "committed",
    storageId: request.storageRef.storageId,
    ...outerOverrides,
  };
}

function providerBinding(overrides = {}) {
  return {
    bindingKind: LVM_ATOMIC_CRASH_CAPTURE_BINDING_KIND,
    contractVersion: LVM_ATOMIC_CRASH_CAPTURE_PROVIDER_CONTRACT_VERSION,
    originLvUuid: ORIGIN_LV_UUID,
    snapshotName: "pcr-snapshot-001",
    snapshotSizeBytes: "4",
    snapshotTag: "pcr.atomic.snapshot-001",
    ...overrides,
  };
}

function activePointer(binding) {
  return {
    conflictClass: "session-mutation",
    expectedSessionRevision: binding.expectedSession.revision,
    kind: binding.kind,
    operationId: binding.operationId,
    operationRevision: "0",
    requestSha256: binding.requestSha256,
    reservationId: binding.reservationId,
    state: "prepared",
  };
}

function terminalPointer(binding, result) {
  return {
    ...activePointer(binding),
    operationRevision: "1",
    resultSha256: sha256Json(result),
    state: "committed",
  };
}

function operationRecord(binding, state, result = null) {
  return deepFreeze({
    conflictClass: "session-mutation",
    createdAt: NOW,
    expectedSession: binding.expectedSession,
    kind: binding.kind,
    operationId: binding.operationId,
    request: binding.request,
    requestSha256: binding.requestSha256,
    result,
    retiredAt: state === "committed" ? LATER : null,
    revision: state === "committed" ? "1" : "0",
    sessionId: SESSION_ID,
    state,
    updatedAt: state === "committed" ? LATER : NOW,
  });
}

function reservationRecord(binding, state) {
  return deepFreeze({
    conflictClass: "session-mutation",
    createdAt: NOW,
    expectedSessionRevision: binding.expectedSession.revision,
    kind: binding.kind,
    operationId: binding.operationId,
    releasedAt: state === "committed" ? LATER : null,
    requestSha256: binding.requestSha256,
    reservationId: binding.reservationId,
    sessionId: SESSION_ID,
    state: state === "committed" ? "released" : "prepared",
    updatedAt: state === "committed" ? LATER : NOW,
    expiresAt: null,
  });
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
  constructor(manager) {
    this.manager = manager;
  }

  query(query) {
    const { text } = query;
    if (text === "DISCARD ALL") {
      this.manager.unlock(this);
      query.callback(null, { command: "DISCARD", rows: [] });
      return undefined;
    }
    if (text.includes("pg_try_advisory_lock")) {
      query.callback(null, {
        command: "SELECT",
        rows: [{ acquired: this.manager.tryAcquire(this), backend_pid: 9101 }],
      });
      return undefined;
    }
    if (text.includes("FROM pg_catalog.pg_locks")) {
      query.callback(null, {
        command: "SELECT",
        rows: [{ backend_pid: 9101, lock_held: this.manager.isHeld(this) }],
      });
      return undefined;
    }
    if (text.includes("pg_advisory_unlock")) {
      query.callback(null, {
        command: "SELECT",
        rows: [{ backend_pid: 9101, unlocked: this.manager.unlock(this) }],
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
  constructor() {
    this.manager = new GuardLockManager();
  }

  connect(callback) {
    callback(null, new GuardClient(this.manager));
  }
}

function createBaseBackend() {
  const noop = frozenFunction(async () => undefined);
  return exact({
    backendId: BACKEND_ID,
    capabilities: deepFreeze(capabilities()),
    captureCheckpoint: noop,
    contractVersion: 1,
    destroySession: noop,
    detachAttachment: noop,
    forceFence: noop,
    prepareWritableAttachment: noop,
    provisionSession: noop,
    restoreCheckpoint: noop,
  });
}

function createCatalogue(options = {}) {
  const calls = { claim: [], commit: [], mark: [], read: [] };
  let state = options.initialState ?? "absent";
  let storedBinding = options.providerBinding ?? null;
  let storedResult = options.result ?? null;
  const dispatchClaim = exact({ claim: "opaque-claim" });
  const catalogue = exact({
    async claimStarting(input) {
      calls.claim.push(input);
      options.events?.push("catalogue-claim");
      if (state === "absent") {
        state = "starting";
        storedBinding = input.providerBinding;
        if (options.claimAckLoss) throw new Error("claim acknowledgement lost");
        return exact({ dispatchClaim, outcome: "dispatch" });
      }
      if (state === "committed") {
        return exact({
          outcome: "committed",
          providerBinding: storedBinding,
          result: storedResult,
        });
      }
      return exact({ outcome: "unknown" });
    },
    async commitResult(input) {
      calls.commit.push(input);
      options.events?.push("catalogue-commit");
      assert.strictEqual(input.dispatchClaim, dispatchClaim);
      state = "committed";
      storedResult = input.result;
      if (options.commitAckLoss) throw new Error("commit acknowledgement lost");
      return exact({
        outcome: "committed",
        providerBinding: storedBinding,
        result: storedResult,
      });
    },
    contractVersion: LVM_ATOMIC_CRASH_CAPTURE_PROVIDER_CONTRACT_VERSION,
    async markUncertain(input) {
      calls.mark.push(input);
      options.events?.push("catalogue-mark-uncertain");
      assert.strictEqual(input.dispatchClaim, dispatchClaim);
      state = "uncertain";
      return exact({ outcome: "uncertain" });
    },
    async readCommitted(input) {
      calls.read.push(input);
      options.events?.push("catalogue-read");
      if (options.readFailure) throw new Error("catalogue read failed");
      if (state !== "committed") return exact({ outcome: "unknown" });
      return exact({
        outcome: "committed",
        providerBinding: storedBinding,
        result: storedResult,
      });
    },
  });
  return {
    calls,
    catalogue,
    get providerState() {
      return state;
    },
    get result() {
      return storedResult;
    },
  };
}

function createDriver(options = {}) {
  const calls = { capture: [], resolve: [], verify: [] };
  const binding = assertLvmAtomicCrashCaptureProviderBinding(
    options.providerBinding ?? providerBinding(),
  );
  const driver = exact({
    async captureSnapshot(input) {
      calls.capture.push(input);
      options.events?.push("driver-capture");
      if (options.capture) return options.capture(input);
      return committedResult(input.request);
    },
    contractVersion: LVM_ATOMIC_CRASH_CAPTURE_DRIVER_CONTRACT_VERSION,
    async resolveProviderBinding(input) {
      calls.resolve.push(input);
      options.events?.push("driver-resolve");
      return binding;
    },
    async verifySnapshot(input) {
      calls.verify.push(input);
      options.events?.push("driver-verify");
      return options.verify ? options.verify(input) : true;
    },
  });
  return { binding, calls, driver };
}

function createHarness(options = {}) {
  const events = [];
  const request = deepFreeze(captureRequest());
  const expectedSession = sessionSnapshot(authorityDocument(), "6");
  const binding = assertSessionOperationBinding({
    expectedSession,
    kind: ATOMIC_CRASH_CAPTURE_OPERATION_KIND,
    operationId: CAPTURE_OPERATION_ID,
    request: { operationId: CAPTURE_OPERATION_ID, request },
  });
  const preparedOperation = operationRecord(binding, "prepared");
  const preparedReservation = reservationRecord(binding, "prepared");
  const preparedSession = sessionSnapshot(
    authorityDocument({ activeOperation: activePointer(binding) }),
    "7",
  );
  assertSessionOperationTransitionProof({
    operation: preparedOperation,
    reservation: preparedReservation,
    session: preparedSession,
  });

  const catalogue =
    options.catalogue ??
    createCatalogue({
      events,
      ...options.catalogueOptions,
      result: options.catalogueOptions?.result ?? committedResult(request),
      providerBinding:
        options.catalogueOptions?.providerBinding ?? providerBinding(),
    });
  const driver =
    options.driver ?? createDriver({ events, ...options.driverOptions });
  const state = { phase: options.authorityPhase ?? "prepared" };
  const calls = { finalize: [], read: [] };

  function committedTransition() {
    const captureResult = catalogue.result;
    assert.notEqual(captureResult, null);
    const result = deepFreeze({
      captureResultSha256: sha256Json(captureResult),
      outcome: "atomic-crash-captured",
      resultVersion: 1,
    });
    const operation = operationRecord(binding, "committed", result);
    const reservation = reservationRecord(binding, "committed");
    const session = sessionSnapshot(
      authorityDocument({
        activeOperation: null,
        lastOperation: terminalPointer(binding, result),
        lifecycle: "RECOVERY_REQUIRED",
      }),
      "8",
      LATER,
    );
    assertSessionOperationTransitionProof({ operation, reservation, session });
    return { operation, reservation, session };
  }

  function receipt(extra = {}) {
    if (state.phase === "committed") {
      const transition = committedTransition();
      return deepFreeze({
        captureResult: catalogue.result,
        operation: transition.operation,
        providerState: catalogue.providerState,
        reservation: transition.reservation,
        session: transition.session,
        status: "committed",
        ...extra,
      });
    }
    return deepFreeze({
      captureResult:
        catalogue.providerState === "committed" ? catalogue.result : null,
      operation: preparedOperation,
      providerState: catalogue.providerState,
      reservation: preparedReservation,
      session: preparedSession,
      status: "prepared",
      ...extra,
    });
  }

  const authority = exact({
    async finalizeAtomicCrashCapture(input) {
      calls.finalize.push(input);
      events.push("authority-finalize");
      assert.equal(input.expectedOperationRevision, "0");
      assert.equal(input.kind, ATOMIC_CRASH_CAPTURE_OPERATION_KIND);
      assert.equal(input.operationId, CAPTURE_OPERATION_ID);
      assert.equal(input.request.operationId, CAPTURE_OPERATION_ID);
      assert.deepEqual(input.request.request, request);
      assert.deepEqual(input.captureResult, catalogue.result);
      assert.deepEqual(input.expectedSession, expectedSession);
      if (options.finalizerBeforeFailure) {
        throw new Error("finalizer failed before commit");
      }
      state.phase = "committed";
      if (options.finalizerAckLoss) {
        throw new Error("finalizer acknowledgement lost");
      }
      return receipt({ finalized: true });
    },
    async readAtomicCrashCapture(input) {
      calls.read.push(input);
      events.push("authority-read");
      assert.deepEqual(Reflect.ownKeys(input), ["operationId", "request"]);
      assert.equal(input.operationId, CAPTURE_OPERATION_ID);
      assert.deepEqual(input.request, request);
      if (options.readReceipt) return options.readReceipt({ receipt, state });
      return receipt();
    },
  });
  const operationGuard =
    options.operationGuard ??
    new PostgresOperationGuard({ dedicatedPool: new GuardPool() });
  const storageBackend = options.storageBackend ?? createBaseBackend();
  const composition =
    options.skipComposition === true
      ? null
      : createPostgresWriterForceFenceLvmAtomicCrashCaptureComposition({
          authority: options.authority ?? authority,
          catalogue: catalogue.catalogue,
          driver: driver.driver,
          operationGuard,
          storageBackend,
        });
  return {
    authority,
    calls,
    catalogue,
    composition,
    driver,
    events,
    input: exact({ operationId: CAPTURE_OPERATION_ID, request }),
    operationGuard,
    receipt,
    request,
    state,
    storageBackend,
  };
}

function isOutcomeUncertain(error) {
  return (
    error instanceof
      PostgresWriterForceFenceLvmAtomicCrashCaptureCompositionError &&
    error.code ===
      "postgres_writer_force_fence_lvm_atomic_crash_capture_outcome_uncertain" &&
    error.retryable === false &&
    Object.isFrozen(error)
  );
}

test("factory returns one exact frozen branded facade", () => {
  const fixture = createHarness();

  assert.deepEqual(Reflect.ownKeys(fixture.composition), [
    "reconcileCapture",
    "runPreparedCapture",
  ]);
  assert.equal(Object.getPrototypeOf(fixture.composition), null);
  assert.equal(Object.isFrozen(fixture.composition), true);
  assert.equal(Object.isFrozen(fixture.composition.runPreparedCapture), true);
  assert.equal(Object.isFrozen(fixture.composition.reconcileCapture), true);
  assert.equal(
    isPostgresWriterForceFenceLvmAtomicCrashCaptureComposition(
      fixture.composition,
    ),
    true,
  );
  assert.equal(
    isPostgresWriterForceFenceLvmAtomicCrashCaptureComposition(
      Object.create(fixture.composition),
    ),
    false,
  );
});

test("fresh prepared capture claims, rechecks authority, dispatches once, verifies, and finalizes", async () => {
  const fixture = createHarness();

  const result = await fixture.composition.runPreparedCapture(fixture.input);

  assert.deepEqual(result, committedResult(fixture.request));
  assert.equal(fixture.driver.calls.resolve.length, 1);
  assert.equal(fixture.catalogue.calls.claim.length, 1);
  assert.equal(fixture.driver.calls.capture.length, 1);
  assert.equal(fixture.catalogue.calls.commit.length, 1);
  assert.equal(fixture.calls.finalize.length, 1);
  assert.equal(fixture.state.phase, "committed");
  assert.deepEqual(fixture.events, [
    "authority-read",
    "driver-resolve",
    "catalogue-claim",
    "authority-read",
    "driver-capture",
    "catalogue-commit",
    "catalogue-read",
    "driver-verify",
    "authority-finalize",
    "catalogue-read",
    "driver-verify",
  ]);
});

for (const providerState of ["starting", "uncertain"]) {
  test(`prepared provider ${providerState} is source-free and never dispatches`, async () => {
    const fixture = createHarness({
      catalogueOptions: { initialState: providerState },
    });

    await assert.rejects(
      fixture.composition.runPreparedCapture(fixture.input),
      isOutcomeUncertain,
    );

    assert.equal(fixture.driver.calls.resolve.length, 0);
    assert.equal(fixture.catalogue.calls.claim.length, 0);
    assert.equal(fixture.driver.calls.capture.length, 0);
    assert.equal(fixture.catalogue.calls.read.length, 1);
    assert.equal(fixture.calls.finalize.length, 0);
    assert.equal(fixture.state.phase, "prepared");
  });
}

test("prepared provider commit is physically verified and finalized without redispatch", async () => {
  const fixture = createHarness({
    catalogueOptions: { initialState: "committed" },
  });

  const result = await fixture.composition.runPreparedCapture(fixture.input);

  assert.deepEqual(result, committedResult(fixture.request));
  assert.equal(fixture.driver.calls.resolve.length, 0);
  assert.equal(fixture.catalogue.calls.claim.length, 0);
  assert.equal(fixture.driver.calls.capture.length, 0);
  assert.equal(fixture.catalogue.calls.read.length, 2);
  assert.equal(fixture.driver.calls.verify.length, 2);
  assert.equal(fixture.calls.finalize.length, 1);
});

test("reconcile is always source-free, including an absent provider record", async () => {
  const fixture = createHarness();

  await assert.rejects(
    fixture.composition.reconcileCapture(fixture.input),
    isOutcomeUncertain,
  );

  assert.equal(fixture.driver.calls.resolve.length, 0);
  assert.equal(fixture.catalogue.calls.claim.length, 0);
  assert.equal(fixture.driver.calls.capture.length, 0);
  assert.equal(fixture.catalogue.calls.read.length, 1);
  assert.equal(fixture.state.phase, "prepared");
});

test("claim acknowledgement loss preserves the blocker and never redispatches", async () => {
  const fixture = createHarness({ catalogueOptions: { claimAckLoss: true } });

  await assert.rejects(
    fixture.composition.runPreparedCapture(fixture.input),
    isOutcomeUncertain,
  );
  await assert.rejects(
    fixture.composition.runPreparedCapture(fixture.input),
    isOutcomeUncertain,
  );

  assert.equal(fixture.catalogue.providerState, "starting");
  assert.equal(fixture.catalogue.calls.claim.length, 1);
  assert.equal(fixture.driver.calls.capture.length, 0);
  assert.equal(fixture.calls.finalize.length, 0);
  assert.equal(fixture.state.phase, "prepared");
});

test("driver failure marks the provider uncertain and leaves authority prepared", async () => {
  const fixture = createHarness({
    driverOptions: {
      capture() {
        throw new Error("driver outcome unknown");
      },
    },
  });

  await assert.rejects(
    fixture.composition.runPreparedCapture(fixture.input),
    isOutcomeUncertain,
  );

  assert.equal(fixture.driver.calls.capture.length, 1);
  assert.equal(fixture.catalogue.calls.mark.length, 1);
  assert.equal(fixture.catalogue.providerState, "uncertain");
  assert.equal(fixture.calls.finalize.length, 0);
  assert.equal(fixture.state.phase, "prepared");
});

test("catalogue commit acknowledgement loss recovers by source-free verification", async () => {
  const fixture = createHarness({ catalogueOptions: { commitAckLoss: true } });

  const result = await fixture.composition.runPreparedCapture(fixture.input);

  assert.deepEqual(result, committedResult(fixture.request));
  assert.equal(fixture.driver.calls.capture.length, 1);
  assert.equal(fixture.catalogue.calls.commit.length, 1);
  assert.equal(fixture.catalogue.calls.mark.length, 0);
  assert.equal(fixture.calls.finalize.length, 1);
  assert.equal(fixture.state.phase, "committed");
});

test("finalizer acknowledgement loss replays only exact committed authority", async () => {
  const fixture = createHarness({
    catalogueOptions: { initialState: "committed" },
    finalizerAckLoss: true,
  });

  const result = await fixture.composition.runPreparedCapture(fixture.input);

  assert.deepEqual(result, committedResult(fixture.request));
  assert.equal(fixture.calls.finalize.length, 1);
  assert.equal(fixture.calls.read.length, 2);
  assert.equal(fixture.driver.calls.capture.length, 0);
  assert.equal(fixture.state.phase, "committed");
});

test("a pre-commit finalizer failure stays blocked and does not call the provider", async () => {
  const fixture = createHarness({
    catalogueOptions: { initialState: "committed" },
    finalizerBeforeFailure: true,
  });

  await assert.rejects(
    fixture.composition.runPreparedCapture(fixture.input),
    isOutcomeUncertain,
  );

  assert.equal(fixture.calls.finalize.length, 1);
  assert.equal(fixture.driver.calls.capture.length, 0);
  assert.equal(fixture.state.phase, "prepared");
});

test("a restarted composition finalizes a durable provider commit without fresh dispatch", async () => {
  const first = createHarness({
    catalogueOptions: { initialState: "committed" },
    skipComposition: true,
  });
  const restarted = createPostgresWriterForceFenceLvmAtomicCrashCaptureComposition({
    authority: first.authority,
    catalogue: first.catalogue.catalogue,
    driver: first.driver.driver,
    operationGuard: first.operationGuard,
    storageBackend: first.storageBackend,
  });

  const result = await restarted.reconcileCapture(first.input);

  assert.deepEqual(result, committedResult(first.request));
  assert.equal(first.catalogue.calls.claim.length, 0);
  assert.equal(first.driver.calls.capture.length, 0);
  assert.equal(first.calls.finalize.length, 1);
});

test("committed authority replay remains source-free and physically verifies", async () => {
  const fixture = createHarness({
    authorityPhase: "committed",
    catalogueOptions: { initialState: "committed" },
  });

  const result = await fixture.composition.runPreparedCapture(fixture.input);

  assert.deepEqual(result, committedResult(fixture.request));
  assert.equal(fixture.catalogue.calls.claim.length, 0);
  assert.equal(fixture.driver.calls.capture.length, 0);
  assert.equal(fixture.calls.finalize.length, 0);
  assert.equal(fixture.catalogue.calls.read.length, 1);
  assert.equal(fixture.driver.calls.verify.length, 1);
});

test("failed physical verification never releases the prepared blocker", async () => {
  const fixture = createHarness({
    catalogueOptions: { initialState: "committed" },
    driverOptions: { verify: () => false },
  });

  await assert.rejects(
    fixture.composition.runPreparedCapture(fixture.input),
    isOutcomeUncertain,
  );

  assert.equal(fixture.driver.calls.verify.length, 1);
  assert.equal(fixture.calls.finalize.length, 0);
  assert.equal(fixture.state.phase, "prepared");
});

test("the operation guard prevents concurrent double dispatch", async () => {
  let releaseCapture;
  let captureStarted;
  const started = new Promise((resolve) => {
    captureStarted = resolve;
  });
  const fixture = createHarness({
    driverOptions: {
      capture(input) {
        captureStarted();
        return new Promise((resolve) => {
          releaseCapture = () => resolve(committedResult(input.request));
        });
      },
    },
  });

  const first = fixture.composition.runPreparedCapture(fixture.input);
  await started;
  await assert.rejects(
    fixture.composition.runPreparedCapture(fixture.input),
    isOutcomeUncertain,
  );
  releaseCapture();
  await first;

  assert.equal(fixture.catalogue.calls.claim.length, 1);
  assert.equal(fixture.driver.calls.capture.length, 1);
});

test("crossed operation identity and provider result are rejected fail-closed", async () => {
  const fixture = createHarness({
    catalogueOptions: { initialState: "committed" },
    readReceipt({ receipt }) {
      const valid = receipt();
      return deepFreeze({
        ...valid,
        operation: {
          ...valid.operation,
          operationId: "crossed-atomic-capture-operation",
        },
      });
    },
  });

  await assert.rejects(
    fixture.composition.runPreparedCapture(fixture.input),
    isOutcomeUncertain,
  );

  assert.equal(fixture.driver.calls.capture.length, 0);
  assert.equal(fixture.calls.finalize.length, 0);
});

test("backend-crossed requests are rejected before authority or provider effects", async () => {
  const fixture = createHarness();
  const input = {
    operationId: CAPTURE_OPERATION_ID,
    request: captureRequest({
      storageRef: {
        backendId: "different-backend",
        contractVersion: 1,
        sessionId: SESSION_ID,
        storageId: "storage-001",
      },
    }),
  };

  await assert.rejects(
    fixture.composition.runPreparedCapture(input),
    (error) =>
      error instanceof
        PostgresWriterForceFenceLvmAtomicCrashCaptureCompositionError &&
      error.code ===
        "invalid_postgres_writer_force_fence_lvm_atomic_crash_capture_composition_request",
  );
  assert.equal(fixture.calls.read.length, 0);
  assert.equal(fixture.catalogue.calls.claim.length, 0);
});

test("fake authorities, fake guards, proxies, and accessor inputs are rejected", async () => {
  const fixture = createHarness({ skipComposition: true });
  const validOptions = {
    authority: fixture.authority,
    catalogue: fixture.catalogue.catalogue,
    driver: fixture.driver.driver,
    operationGuard: fixture.operationGuard,
    storageBackend: fixture.storageBackend,
  };
  const fakeAuthority = Object.freeze({
    finalizeAtomicCrashCapture: async () => undefined,
  });

  assert.throws(
    () =>
      createPostgresWriterForceFenceLvmAtomicCrashCaptureComposition({
        ...validOptions,
        authority: fakeAuthority,
      }),
    /options are invalid/u,
  );
  assert.throws(
    () =>
      createPostgresWriterForceFenceLvmAtomicCrashCaptureComposition({
        ...validOptions,
        operationGuard: Object.freeze({ runExclusive: async () => undefined }),
      }),
    /options are invalid/u,
  );
  assert.throws(
    () =>
      createPostgresWriterForceFenceLvmAtomicCrashCaptureComposition(
        new Proxy(validOptions, {}),
      ),
    /options are invalid/u,
  );
  const accessor = {};
  Object.defineProperty(accessor, "operationId", {
    enumerable: true,
    get() {
      throw new Error("must not run");
    },
  });
  Object.defineProperty(accessor, "request", {
    enumerable: true,
    value: fixture.request,
  });
  await assert.rejects(
    fixture.composition?.runPreparedCapture(accessor) ??
      createPostgresWriterForceFenceLvmAtomicCrashCaptureComposition(
        validOptions,
      ).runPreparedCapture(accessor),
    /request is invalid/u,
  );
});

test("subclass promises and poisoned public promise methods cannot authorize dispatch", { concurrency: false }, async () => {
  const fixture = createHarness({ skipComposition: true });
  class ForeignPromise extends Promise {}
  const authority = exact({
    finalizeAtomicCrashCapture: frozenFunction(async () => undefined),
    readAtomicCrashCapture: frozenFunction(
      () => new ForeignPromise((resolve) => resolve(fixture.receipt())),
    ),
  });
  const composition =
    createPostgresWriterForceFenceLvmAtomicCrashCaptureComposition({
      authority,
      catalogue: fixture.catalogue.catalogue,
      driver: fixture.driver.driver,
      operationGuard: fixture.operationGuard,
      storageBackend: fixture.storageBackend,
    });

  await assert.rejects(
    composition.runPreparedCapture(fixture.input),
    isOutcomeUncertain,
  );
  assert.equal(fixture.catalogue.calls.claim.length, 0);
  assert.equal(fixture.driver.calls.capture.length, 0);

  const normal = createHarness();
  const pending = normal.composition.runPreparedCapture(normal.input);
  const thenDescriptor = Object.getOwnPropertyDescriptor(pending, "then");
  assert.equal(thenDescriptor.configurable, false);
  assert.equal(thenDescriptor.writable, false);
  assert.throws(
    () => Object.defineProperty(pending, "then", { value: () => undefined }),
    TypeError,
  );
  const result = await Promise.prototype.then.call(
    pending,
    (value) => value,
  );
  assert.deepEqual(result, committedResult(normal.request));
});

test("captured WeakMap intrinsics keep dispatch tokens private under prototype poisoning", { concurrency: false }, async () => {
  const descriptor = Object.getOwnPropertyDescriptor(WeakMap.prototype, "get");
  let poisonCalls = 0;
  Object.defineProperty(WeakMap.prototype, "get", {
    ...descriptor,
    value() {
      poisonCalls += 1;
      throw new Error("poisoned WeakMap.get");
    },
  });
  try {
    const fixture = createHarness();
    const result = await fixture.composition.runPreparedCapture(fixture.input);
    assert.deepEqual(result, committedResult(fixture.request));
    assert.equal(poisonCalls, 0);
  } finally {
    Object.defineProperty(WeakMap.prototype, "get", descriptor);
  }
});
