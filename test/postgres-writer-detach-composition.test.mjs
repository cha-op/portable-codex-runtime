import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import test from "node:test";

import {
  PostgresWriterDetachCompositionError,
  createPostgresWriterDetachComposition,
  isPostgresWriterDetachComposition,
} from "../src/postgres-writer-detach-composition.mjs";
import { PostgresOperationGuard } from "../src/postgres-operation-guard.mjs";
import {
  SESSION_AUTHORITY_DOCUMENT_VERSION,
  WRITER_ATTACHMENT_ACQUIRE_OPERATION_KIND,
  WRITER_FORCE_FENCE_OPERATION_KIND,
  WRITER_RELEASE_OPERATION_KIND,
  assertSessionOperationBinding,
} from "../src/postgres-session-authority.mjs";
import { createSessionManifest } from "../src/session-storage-contracts.mjs";

const SESSION_ID = "019f2100-0000-7000-8000-000000000001";
const THREAD_ID = "019f2100-0000-7000-8000-000000000002";
const BACKEND_ID = "single-attach-test";
const STORAGE_ID = "volume-001";
const ATTACHMENT_ID = "attachment-001";
const RELEASE_OPERATION_ID = "writer-release-001";
const FENCE_OPERATION_ID = "writer-force-fence-001";
const NOW = "2026-08-10T10:00:00.000Z";

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
      codexVersion: "codex-cli 0.142.4",
      codexSandbox: "danger-full-access",
    },
  });
}

function capabilities(fencing = "epoch-enforced") {
  return {
    atomicPointInTimeCheckpoint: true,
    exclusiveWriterAttachment: true,
    fencing,
    normalDirectoryAttachment: true,
  };
}

function lease(fencingEpoch = "11") {
  return {
    contractVersion: 1,
    expiresAt: "2026-08-10T11:00:00.000Z",
    fencingEpoch,
    holderId: "host-001",
    leaseId: "lease-001",
    sessionId: SESSION_ID,
  };
}

function attachment(fencingEpoch = "11") {
  const currentLease = lease(fencingEpoch);
  return {
    attachmentId: ATTACHMENT_ID,
    backendId: BACKEND_ID,
    contractVersion: 1,
    fencingEpoch,
    holderId: currentLease.holderId,
    kind: "directory",
    leaseId: currentLease.leaseId,
    mode: "read-write",
    operationId: "writer-attach-001",
    proofId: "proof-attach-001",
    rootPath: "/var/lib/portable-codex/session-001",
    sessionId: SESSION_ID,
    storageId: STORAGE_ID,
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
    resultSha256: "c".repeat(64),
    state: "committed",
  };
}

function authorityDocument({
  activeOperation = null,
  attachment: currentAttachment = attachment(),
  backendCapabilities = capabilities(),
  lastOperation = attachedLastOperation(),
  lease: currentLease = lease(),
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
    backendCapabilities,
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

function target() {
  return { attachmentId: ATTACHMENT_ID, kind: "attachment" };
}

function request(expectedSession, operationId) {
  return { expectedSession, operationId, target: target() };
}

function detachResult(mutationRequest, overrides = {}) {
  return {
    ...structuredClone(mutationRequest),
    proofId: "proof-detach-001",
    status: "detached",
    ...overrides,
  };
}

function fenceResult(fenceRequest, overrides = {}) {
  return {
    ...structuredClone(fenceRequest),
    proofId: "proof-fence-001",
    status: "fenced",
    ...overrides,
  };
}

function exactFrozenFunction(fn) {
  return Object.freeze(fn);
}

function nullRecord(entries) {
  const value = Object.create(null);
  for (const [key, entry] of entries) value[key] = entry;
  return Object.freeze(value);
}

function assertCode(code) {
  return (error) => {
    const messages = {
      invalid_postgres_writer_detach_composition_options:
        "PostgreSQL writer detach composition options are invalid",
      invalid_postgres_writer_detach_composition_request:
        "PostgreSQL writer detach composition request is invalid",
      postgres_writer_detach_composition_outcome_uncertain:
        "PostgreSQL writer detach composition outcome is uncertain",
    };
    return (
      error instanceof PostgresWriterDetachCompositionError &&
      error.name === "PostgresWriterDetachCompositionError" &&
      error.code === code &&
      error.message === messages[code] &&
      error.retryable === false &&
      !Object.hasOwn(error, "cause") &&
      Object.isFrozen(error)
    );
  };
}

function assertDeepFrozen(value, seen = new Set()) {
  if (
    value === null ||
    (typeof value !== "object" && typeof value !== "function") ||
    seen.has(value)
  ) {
    return;
  }
  seen.add(value);
  assert.equal(Object.isFrozen(value), true);
  for (const key of Reflect.ownKeys(value)) {
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    if (descriptor && Object.hasOwn(descriptor, "value")) {
      assertDeepFrozen(descriptor.value, seen);
    }
  }
}

function unsafeThenable(onThen = () => {}) {
  return Object.freeze({
    then: exactFrozenFunction((resolve) => {
      onThen();
      resolve(undefined);
    }),
  });
}

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, reject, resolve };
}

function nextTurn() {
  return new Promise((resolve) => setImmediate(resolve));
}

function sha256Json(value) {
  return createHash("sha256")
    .update(JSON.stringify(value), "utf8")
    .digest("hex");
}

function driftTerminalProof(receipt, proofKey) {
  const drifted = structuredClone(receipt);
  drifted.operation.result[proofKey].proofId = "proof-drifted-readback";
  drifted.session.document.lastOperation.resultSha256 = sha256Json(
    drifted.operation.result,
  );
  return deepFreeze(drifted);
}

class DetachGuardLockManager {
  constructor() {
    this.holders = new Map();
  }

  tryAcquire(key, client) {
    const holder = this.holders.get(key);
    if (holder !== undefined && holder !== client) return false;
    this.holders.set(key, client);
    return true;
  }

  isHeld(key, client) {
    return this.holders.get(key) === client;
  }

  unlock(key, client) {
    if (!this.isHeld(key, client)) return false;
    this.holders.delete(key);
    return true;
  }

  releaseAll(client) {
    for (const [key, holder] of this.holders) {
      if (holder === client) this.holders.delete(key);
    }
  }
}

class DetachGuardClient {
  constructor({ calls, guardState, manager, pid }) {
    this.calls = calls;
    this.guardState = guardState;
    this.heldProbeCount = 0;
    this.manager = manager;
    this.pid = pid;
    this.releaseCalls = [];
    this.resetCount = 0;
  }

  query(...args) {
    const query = args[0];
    const callback = query?.callback;
    const text = query?.text;

    if (text === "DISCARD ALL") {
      this.resetCount += 1;
      this.manager.releaseAll(this);
      callback(null, { command: "DISCARD", rows: [] });
      return undefined;
    }

    const values = query.values;
    const key = values[0];
    if (text.includes("pg_try_advisory_lock")) {
      const acquired =
        this.guardState.mode === "busy"
          ? false
          : this.manager.tryAcquire(key, this);
      callback(null, {
        command: "SELECT",
        rows: [{ acquired, backend_pid: this.pid }],
      });
      return undefined;
    }
    if (text.includes("FROM pg_catalog.pg_locks")) {
      this.heldProbeCount += 1;
      this.calls.push(["assertHeld", this.heldProbeCount]);
      const requestedFailure = `probe-${this.heldProbeCount - 1}`;
      const lockHeld =
        this.guardState.mode === requestedFailure
          ? false
          : this.manager.isHeld(key, this);
      callback(null, {
        command: "SELECT",
        rows: [{ backend_pid: this.pid, lock_held: lockHeld }],
      });
      return undefined;
    }
    if (text.includes("pg_advisory_unlock")) {
      if (this.guardState.mode === "cleanup") {
        callback(new Error("guard cleanup failed"));
        return undefined;
      }
      callback(null, {
        command: "SELECT",
        rows: [
          {
            backend_pid: this.pid,
            unlocked: this.manager.unlock(key, this),
          },
        ],
      });
      return undefined;
    }
    callback(new Error(`unexpected guard query: ${text}`));
    return undefined;
  }

  release(...args) {
    this.releaseCalls.push(args);
    this.manager.releaseAll(this);
    return undefined;
  }
}

class DetachGuardPool {
  constructor({ calls, guardState, manager }) {
    this.calls = calls;
    this.clients = [];
    this.guardState = guardState;
    this.manager = manager;
    this.nextPid = 8_001;
  }

  connect(callback) {
    this.calls.push(["runExclusive"]);
    const client = new DetachGuardClient({
      calls: this.calls,
      guardState: this.guardState,
      manager: this.manager,
      pid: this.nextPid,
    });
    this.nextPid += 1;
    this.clients.push(client);
    callback(null, client);
    return undefined;
  }
}

function createHarness({
  fencing = "epoch-enforced",
  guardMode = "normal",
  provider = {},
  session = sessionSnapshot(
    authorityDocument({ backendCapabilities: capabilities(fencing) }),
  ),
} = {}) {
  const calls = [];
  const injections = new Map();
  const state = {
    binding: null,
    kind: null,
    phase: "absent",
    proof: null,
    request: null,
    session,
  };
  const guardState = { mode: guardMode };
  const guardManager = new DetachGuardLockManager();
  const guardPool = new DetachGuardPool({
    calls,
    guardState,
    manager: guardManager,
  });

  function enqueue(method, injection) {
    const queue = injections.get(method) ?? [];
    queue.push(injection);
    injections.set(method, queue);
  }

  function snapshotInput(value) {
    return structuredClone(value);
  }

  function activePointer(phase, revision) {
    return {
      conflictClass: "session-mutation",
      expectedSessionRevision: state.binding.expectedSession.revision,
      kind: state.kind,
      operationId: state.request.operationId,
      operationRevision: revision,
      requestSha256: state.binding.requestSha256,
      reservationId: state.binding.reservationId,
      state: phase,
    };
  }

  function terminalPointer(revision) {
    return {
      ...activePointer("committed", revision),
      resultSha256: sha256Json(
        operationResult(state.resultOutcome, state.blockedReason),
      ),
    };
  }

  function operationResult(outcome, reason) {
    if (outcome === "writer-released") {
      return {
        resultVersion: 1,
        outcome,
        lease: state.binding.expectedSession.document.lease,
        attachment: state.binding.expectedSession.document.attachment,
        mutationResult: state.proof,
      };
    }
    if (outcome === "writer-fenced") {
      return {
        resultVersion: 1,
        outcome,
        writerEpoch: state.session.document.writerEpoch,
        lease: state.binding.expectedSession.document.lease,
        attachment: state.binding.expectedSession.document.attachment,
        fenceTarget: state.binding.request.target,
        fenceResult: state.proof,
      };
    }
    return {
      resultVersion: 1,
      outcome: "writer-blocked",
      reason,
      writerEpoch: state.session.document.writerEpoch,
      lease: state.binding.expectedSession.document.lease,
      attachment: state.binding.expectedSession.document.attachment,
      fenceTarget: state.binding.request.target,
    };
  }

  function receipt(extra = {}) {
    if (state.phase === "absent") {
      return deepFreeze({
        operation: null,
        reservation: null,
        session: state.session,
        status: "absent",
        ...extra,
      });
    }
    const revision = {
      prepared: "0",
      starting: "1",
      uncertain: "2",
      committed: state.resultOutcome === "writer-blocked" ? "3" : "2",
    }[state.phase];
    const operation = {
      conflictClass: "session-mutation",
      createdAt: NOW,
      expectedSession: state.binding.expectedSession,
      kind: state.kind,
      operationId: state.request.operationId,
      request: state.binding.request,
      requestSha256: state.binding.requestSha256,
      result:
        state.phase === "committed"
          ? operationResult(state.resultOutcome, state.blockedReason)
          : null,
      retiredAt: state.phase === "committed" ? NOW : null,
      revision,
      sessionId: SESSION_ID,
      state: state.phase,
      updatedAt: NOW,
    };
    const reservation = {
      conflictClass: "session-mutation",
      createdAt: NOW,
      expectedSessionRevision: state.request.expectedSession.revision,
      kind: state.kind,
      operationId: state.request.operationId,
      releasedAt: state.phase === "committed" ? NOW : null,
      requestSha256: state.binding.requestSha256,
      reservationId: state.binding.reservationId,
      sessionId: SESSION_ID,
      state: state.phase === "committed" ? "released" : state.phase,
      updatedAt: NOW,
      expiresAt: null,
    };
    return deepFreeze({ operation, reservation, session: state.session, ...extra });
  }

  function setSession(phase, { blocked = false } = {}) {
    let lifecycle;
    if (phase === "committed") {
      lifecycle = blocked ? "BLOCKED" : "DETACHED";
    } else if (phase === "prepared") {
      lifecycle = state.request.expectedSession.document.lifecycle;
    } else if (state.kind === WRITER_RELEASE_OPERATION_KIND) {
      lifecycle = "RELEASING";
    } else {
      lifecycle = "FENCING";
    }
    const writerEpoch =
      state.kind === WRITER_FORCE_FENCE_OPERATION_KIND &&
      (phase === "starting" || phase === "uncertain" || phase === "committed")
        ? String(BigInt(state.request.expectedSession.document.writerEpoch) + 1n)
        : state.request.expectedSession.document.writerEpoch;
    const detached = phase === "committed" && !blocked;
    state.session = sessionSnapshot(
      authorityDocument({
        activeOperation:
          phase === "committed"
            ? null
            : activePointer(
                phase,
                { prepared: "0", starting: "1", uncertain: "2" }[phase],
              ),
        attachment: detached ? null : attachment(),
        backendCapabilities: capabilities(fencing),
        lastOperation:
          phase === "committed"
            ? terminalPointer(blocked ? "3" : "2")
            : state.request.expectedSession.document.lastOperation,
        lease: detached ? null : lease(),
        lifecycle,
        writerEpoch,
      }),
      String(BigInt(state.session.revision) + 1n),
    );
  }

  async function applyInjection(method, value, mutation) {
    calls.push([method, snapshotInput(value)]);
    const queue = injections.get(method);
    const injection = queue?.shift();
    if (injection?.timing === "before") throw injection.error;
    const result = await mutation();
    if (injection?.timing === "after") throw injection.error;
    if (injection?.transform !== undefined) {
      return injection.transform(result);
    }
    if (injection?.result !== undefined) return injection.result;
    return result;
  }

  const reserveOperation = exactFrozenFunction((input) =>
    applyInjection("reserveOperation", input, async () => {
      if (state.phase === "absent") {
        state.kind = input.kind;
        state.request = {
          expectedSession: input.expectedSession,
          operationId: input.operationId,
          target: input.request.target,
        };
        state.binding = assertSessionOperationBinding({
          expectedSession: input.expectedSession,
          kind: input.kind,
          operationId: input.operationId,
          request: input.request,
        });
        state.phase = "prepared";
        setSession("prepared");
        return receipt({ acquired: true });
      }
      return receipt({ acquired: false });
    }),
  );
  const reconcileOperation = exactFrozenFunction((input) =>
    applyInjection("reconcileOperation", input, async () => receipt()),
  );
  const claimWriterReleaseDispatch = exactFrozenFunction((input) =>
    applyInjection("claimWriterReleaseDispatch", input, async () => {
      if (state.phase === "prepared") {
        state.phase = "starting";
        setSession("starting");
        state.mutationRequest = {
          contractVersion: 1,
          backendId: BACKEND_ID,
          storageId: STORAGE_ID,
          sessionId: SESSION_ID,
          leaseId: lease().leaseId,
          holderId: lease().holderId,
          fencingEpoch: lease().fencingEpoch,
          operation: "detach",
          operationId: input.operationId,
          target: target(),
        };
        return receipt({
          dispatchGranted: true,
          lease: lease(),
          mutationRequest: state.mutationRequest,
        });
      }
      return receipt({
        dispatchGranted: false,
        ...(state.mutationRequest
          ? { lease: lease(), mutationRequest: state.mutationRequest }
          : {}),
      });
    }),
  );
  const claimWriterForceFenceDispatch = exactFrozenFunction((input) =>
    applyInjection("claimWriterForceFenceDispatch", input, async () => {
      if (state.phase === "prepared") {
        state.phase = "starting";
        setSession("starting");
        const writerEpoch = state.session.document.writerEpoch;
        state.fenceRequest = {
          backendId: BACKEND_ID,
          contractVersion: 1,
          fencingEpoch: writerEpoch,
          operationId: input.operationId,
          revokedFence: {
            fencingEpoch: lease().fencingEpoch,
            holderId: lease().holderId,
            leaseId: lease().leaseId,
          },
          sessionId: SESSION_ID,
          storageId: STORAGE_ID,
          target: target(),
        };
        return receipt({
          dispatchGranted: true,
          fenceRequest: state.fenceRequest,
          writerEpoch,
        });
      }
      return receipt({
        dispatchGranted: false,
        ...(state.fenceRequest
          ? {
              fenceRequest: state.fenceRequest,
              writerEpoch: state.session.document.writerEpoch,
            }
          : {}),
      });
    }),
  );
  const markOperationUncertain = exactFrozenFunction((input) =>
    applyInjection("markOperationUncertain", input, async () => {
      if (state.phase === "starting") {
        state.phase = "uncertain";
        setSession("uncertain");
        return receipt({ changed: true });
      }
      return receipt({ changed: false });
    }),
  );
  const finalizeWriterOperationBlocked = exactFrozenFunction((input) =>
    applyInjection("finalizeWriterOperationBlocked", input, async () => {
      if (state.phase === "uncertain") {
        state.phase = "committed";
        state.resultOutcome = "writer-blocked";
        state.blockedReason = input.reason;
        setSession("committed", { blocked: true });
        return receipt({ finalized: true });
      }
      return receipt({ finalized: false });
    }),
  );
  const finalizeWriterRelease = exactFrozenFunction((input) =>
    applyInjection("finalizeWriterRelease", input, async () => {
      state.proof = input.mutationResult;
      state.phase = "committed";
      state.resultOutcome = "writer-released";
      setSession("committed");
      return receipt({ finalized: true });
    }),
  );
  const finalizeWriterForceFence = exactFrozenFunction((input) =>
    applyInjection("finalizeWriterForceFence", input, async () => {
      state.proof = input.fenceResult;
      state.phase = "committed";
      state.resultOutcome = "writer-fenced";
      setSession("committed");
      return receipt({ finalized: true });
    }),
  );

  const authority = nullRecord([
    ["claimWriterForceFenceDispatch", claimWriterForceFenceDispatch],
    ["claimWriterReleaseDispatch", claimWriterReleaseDispatch],
    ["finalizeWriterForceFence", finalizeWriterForceFence],
    ["finalizeWriterOperationBlocked", finalizeWriterOperationBlocked],
    ["finalizeWriterRelease", finalizeWriterRelease],
    ["markOperationUncertain", markOperationUncertain],
    ["reconcileOperation", reconcileOperation],
    ["reserveOperation", reserveOperation],
  ]);

  const detachAttachment = exactFrozenFunction((input) => {
    calls.push(["detachAttachment", snapshotInput(input)]);
    if (provider.detachAttachment) return provider.detachAttachment(input);
    return Promise.resolve(deepFreeze(detachResult(input)));
  });
  const forceFence = exactFrozenFunction((input) => {
    calls.push(["forceFence", snapshotInput(input)]);
    if (provider.forceFence) return provider.forceFence(input);
    return Promise.resolve(deepFreeze(fenceResult(input)));
  });
  const noop = exactFrozenFunction(async () => undefined);
  const storageBackend = nullRecord([
    ["contractVersion", 1],
    ["backendId", BACKEND_ID],
    ["capabilities", deepFreeze(capabilities(fencing))],
    ["captureCheckpoint", noop],
    ["destroySession", noop],
    ["detachAttachment", detachAttachment],
    ["forceFence", forceFence],
    ["prepareWritableAttachment", noop],
    ["provisionSession", noop],
    ["restoreCheckpoint", noop],
  ]);

  const operationGuard = new PostgresOperationGuard({
    dedicatedPool: guardPool,
  });

  return {
    authority,
    calls,
    composition: createPostgresWriterDetachComposition({
      authority,
      operationGuard,
      storageBackend,
    }),
    enqueue,
    guardManager,
    guardPool,
    guardState,
    operationGuard,
    state,
    storageBackend,
  };
}

async function seedPrepared(harness, kind, expectedSession, operationId) {
  await harness.authority.reserveOperation({
    expectedSession,
    kind,
    operationId,
    request: { contractVersion: 1, target: target() },
  });
  harness.calls.length = 0;
}

async function seedStarting(harness, kind, expectedSession, operationId) {
  await seedPrepared(harness, kind, expectedSession, operationId);
  const method =
    kind === WRITER_RELEASE_OPERATION_KIND
      ? "claimWriterReleaseDispatch"
      : "claimWriterForceFenceDispatch";
  await harness.authority[method]({
    expectedOperationRevision: "0",
    expectedSession,
    kind,
    operationId,
    request: { contractVersion: 1, target: target() },
  });
  harness.calls.length = 0;
}

async function seedUncertain(harness, kind, expectedSession, operationId) {
  await seedStarting(harness, kind, expectedSession, operationId);
  await harness.authority.markOperationUncertain({
    expectedOperationRevision: "1",
    expectedSession,
    kind,
    operationId,
    request: { contractVersion: 1, target: target() },
  });
  harness.calls.length = 0;
}

async function seedCommitted(
  harness,
  kind,
  expectedSession,
  operationId,
  { blockedReason = null } = {},
) {
  await seedStarting(harness, kind, expectedSession, operationId);
  if (blockedReason !== null) {
    await harness.authority.markOperationUncertain({
      expectedOperationRevision: "1",
      expectedSession,
      kind,
      operationId,
      request: { contractVersion: 1, target: target() },
    });
    await harness.authority.finalizeWriterOperationBlocked({
      expectedOperationRevision: "2",
      expectedSession,
      kind,
      operationId,
      reason: blockedReason,
      request: { contractVersion: 1, target: target() },
    });
  } else if (kind === WRITER_RELEASE_OPERATION_KIND) {
    const proof = deepFreeze(detachResult(harness.state.mutationRequest));
    await harness.authority.finalizeWriterRelease({
      expectedOperationRevision: "1",
      expectedSession,
      kind,
      mutationResult: proof,
      operationId,
      request: { contractVersion: 1, target: target() },
    });
  } else {
    const proof = deepFreeze(fenceResult(harness.state.fenceRequest));
    await harness.authority.finalizeWriterForceFence({
      expectedOperationRevision: "1",
      expectedSession,
      fenceResult: proof,
      kind,
      operationId,
      request: { contractVersion: 1, target: target() },
    });
  }
  harness.calls.length = 0;
}

test("factory returns one frozen branded exact facade", () => {
  const fixture = createHarness();

  assert.equal(isPostgresWriterDetachComposition(fixture.composition), true);
  assert.equal(Object.getPrototypeOf(fixture.composition), null);
  assert.equal(Object.isFrozen(fixture.composition), true);
  assert.deepEqual(Reflect.ownKeys(fixture.composition), [
    "detachWriter",
    "forceFenceWriter",
  ]);
  assert.equal(Object.isFrozen(fixture.composition.detachWriter), true);
  assert.equal(Object.isFrozen(fixture.composition.forceFenceWriter), true);
  assert.equal(isPostgresWriterDetachComposition({}), false);

  let traps = 0;
  const proxy = new Proxy(
    {},
    {
      get() {
        traps += 1;
        throw new Error("must not inspect impostor");
      },
      getOwnPropertyDescriptor() {
        traps += 1;
        throw new Error("must not inspect impostor");
      },
      ownKeys() {
        traps += 1;
        throw new Error("must not inspect impostor");
      },
    },
  );
  assert.equal(isPostgresWriterDetachComposition(proxy), false);
  assert.equal(traps, 0);
});

test("factory rejects non-exact, accessor, Proxy, and thenable options without effects", () => {
  const fixture = createHarness();
  const good = {
    authority: fixture.authority,
    operationGuard: fixture.operationGuard,
    storageBackend: fixture.storageBackend,
  };
  let traps = 0;
  const proxied = new Proxy(good, {
    ownKeys() {
      traps += 1;
      throw new Error("must not run options trap");
    },
  });
  assert.throws(
    () => createPostgresWriterDetachComposition(proxied),
    assertCode("invalid_postgres_writer_detach_composition_options"),
  );
  assert.equal(traps, 0);

  let getterCalls = 0;
  const accessor = { ...good };
  Object.defineProperty(accessor, "authority", {
    enumerable: true,
    get() {
      getterCalls += 1;
      return fixture.authority;
    },
  });
  assert.throws(
    () => createPostgresWriterDetachComposition(accessor),
    assertCode("invalid_postgres_writer_detach_composition_options"),
  );
  assert.equal(getterCalls, 0);

  let thenCalls = 0;
  assert.throws(
    () =>
      createPostgresWriterDetachComposition({
        ...good,
        then: () => {
          thenCalls += 1;
        },
      }),
    assertCode("invalid_postgres_writer_detach_composition_options"),
  );
  assert.equal(thenCalls, 0);

  assert.throws(
    () => createPostgresWriterDetachComposition({ ...good, extra: true }),
    assertCode("invalid_postgres_writer_detach_composition_options"),
  );
  assert.deepEqual(fixture.calls, []);
});

test("revoked options and requests use fixed domain errors without invoking traps", async () => {
  const fixture = createHarness();
  let traps = 0;
  const handler = {
    get() {
      traps += 1;
      throw new Error("revoked proxy trap must not run");
    },
    getOwnPropertyDescriptor() {
      traps += 1;
      throw new Error("revoked proxy trap must not run");
    },
    getPrototypeOf() {
      traps += 1;
      throw new Error("revoked proxy trap must not run");
    },
    ownKeys() {
      traps += 1;
      throw new Error("revoked proxy trap must not run");
    },
  };
  const optionsProxy = Proxy.revocable(
    {
      authority: fixture.authority,
      operationGuard: fixture.operationGuard,
      storageBackend: fixture.storageBackend,
    },
    handler,
  );
  optionsProxy.revoke();
  assert.throws(
    () => createPostgresWriterDetachComposition(optionsProxy.proxy),
    assertCode("invalid_postgres_writer_detach_composition_options"),
  );

  const requestProxy = Proxy.revocable(
    request(fixture.state.session, RELEASE_OPERATION_ID),
    handler,
  );
  requestProxy.revoke();
  await assert.rejects(
    fixture.composition.detachWriter(requestProxy.proxy),
    assertCode("invalid_postgres_writer_detach_composition_request"),
  );
  assert.equal(traps, 0);
  assert.deepEqual(fixture.calls, []);
});

test("factory captures backend data properties without running traps, accessors, or thenables", () => {
  const fixture = createHarness();
  const baseOptions = {
    authority: fixture.authority,
    operationGuard: fixture.operationGuard,
  };
  let traps = 0;
  const proxy = new Proxy(fixture.storageBackend, {
    get() {
      traps += 1;
      throw new Error("must not run backend trap");
    },
    ownKeys() {
      traps += 1;
      throw new Error("must not run backend trap");
    },
  });
  assert.throws(
    () =>
      createPostgresWriterDetachComposition({
        ...baseOptions,
        storageBackend: proxy,
      }),
    assertCode("invalid_postgres_writer_detach_composition_options"),
  );
  assert.equal(traps, 0);

  let getterCalls = 0;
  const accessor = { ...fixture.storageBackend };
  Object.defineProperty(accessor, "backendId", {
    enumerable: true,
    get() {
      getterCalls += 1;
      return BACKEND_ID;
    },
  });
  assert.throws(
    () =>
      createPostgresWriterDetachComposition({
        ...baseOptions,
        storageBackend: Object.freeze(accessor),
      }),
    assertCode("invalid_postgres_writer_detach_composition_options"),
  );
  assert.equal(getterCalls, 0);

  let thenCalls = 0;
  const thenableBackend = Object.freeze({
    ...fixture.storageBackend,
    then() {
      thenCalls += 1;
    },
  });
  assert.throws(
    () =>
      createPostgresWriterDetachComposition({
        ...baseOptions,
        storageBackend: thenableBackend,
      }),
    assertCode("invalid_postgres_writer_detach_composition_options"),
  );
  assert.equal(thenCalls, 0);
  assert.deepEqual(fixture.calls, []);
});

test("factory canonically snapshots mutable backend capabilities", async () => {
  const fixture = createHarness();
  const mutableCapabilities = capabilities("epoch-enforced");
  const storageBackend = Object.freeze({
    ...fixture.storageBackend,
    capabilities: mutableCapabilities,
  });
  const composition = createPostgresWriterDetachComposition({
    authority: fixture.authority,
    operationGuard: fixture.operationGuard,
    storageBackend,
  });
  const expectedSession = fixture.state.session;

  mutableCapabilities.fencing = "manual";
  const result = await composition.forceFenceWriter(
    request(expectedSession, FENCE_OPERATION_ID),
  );

  assert.equal(result.operation.result.outcome, "writer-fenced");
  assert.equal(result.session.document.lifecycle, "DETACHED");
  assert.equal(
    fixture.calls.filter(([name]) => name === "forceFence").length,
    1,
  );
});

test("factory rejects authority and guard proxies without running traps", () => {
  const fixture = createHarness();
  for (const key of ["authority", "operationGuard"]) {
    let traps = 0;
    const collaborator = new Proxy(fixture[key], {
      getOwnPropertyDescriptor() {
        traps += 1;
        throw new Error("must not inspect collaborator proxy");
      },
      isExtensible() {
        traps += 1;
        throw new Error("must not inspect collaborator proxy");
      },
      ownKeys() {
        traps += 1;
        throw new Error("must not inspect collaborator proxy");
      },
      preventExtensions() {
        traps += 1;
        throw new Error("must not inspect collaborator proxy");
      },
    });
    assert.throws(
      () =>
        createPostgresWriterDetachComposition({
          authority:
            key === "authority" ? collaborator : fixture.authority,
          operationGuard:
            key === "operationGuard"
              ? collaborator
              : fixture.operationGuard,
          storageBackend: fixture.storageBackend,
        }),
      assertCode("invalid_postgres_writer_detach_composition_options"),
    );
    assert.equal(traps, 0);
  }
  assert.deepEqual(fixture.calls, []);
});

test("public methods reject non-exact requests before guard, authority, or provider", async () => {
  const fixture = createHarness();
  const expectedSession = fixture.state.session;
  const good = request(expectedSession, RELEASE_OPERATION_ID);
  await assert.rejects(
    fixture.composition.detachWriter(),
    assertCode("invalid_postgres_writer_detach_composition_request"),
  );
  await assert.rejects(
    fixture.composition.forceFenceWriter(good, good),
    assertCode("invalid_postgres_writer_detach_composition_request"),
  );
  let traps = 0;
  const proxied = new Proxy(good, {
    getOwnPropertyDescriptor() {
      traps += 1;
      throw new Error("must not inspect request proxy");
    },
  });
  await assert.rejects(
    fixture.composition.detachWriter(proxied),
    assertCode("invalid_postgres_writer_detach_composition_request"),
  );
  assert.equal(traps, 0);

  let getterCalls = 0;
  const accessor = { ...good };
  Object.defineProperty(accessor, "target", {
    enumerable: true,
    get() {
      getterCalls += 1;
      return target();
    },
  });
  await assert.rejects(
    fixture.composition.detachWriter(accessor),
    assertCode("invalid_postgres_writer_detach_composition_request"),
  );
  assert.equal(getterCalls, 0);

  let thenCalls = 0;
  await assert.rejects(
    fixture.composition.detachWriter({
      ...good,
      then: () => {
        thenCalls += 1;
      },
    }),
    assertCode("invalid_postgres_writer_detach_composition_request"),
  );
  assert.equal(thenCalls, 0);
  await assert.rejects(
    fixture.composition.forceFenceWriter({ ...good, extra: true }),
    assertCode("invalid_postgres_writer_detach_composition_request"),
  );
  await assert.rejects(
    fixture.composition.detachWriter({ ...good, target: { ...target(), extra: true } }),
    assertCode("invalid_postgres_writer_detach_composition_request"),
  );
  assert.deepEqual(fixture.calls, []);
});

test("backend identity and capability mismatch fail before authority and provider", async () => {
  const mismatchedIdentity = sessionSnapshot(
    {
      ...authorityDocument(),
      storageRef: {
        ...authorityDocument().storageRef,
        backendId: "other-backend",
      },
    },
  );
  const identityFixture = createHarness({ session: mismatchedIdentity });
  await assert.rejects(
    identityFixture.composition.detachWriter(
      request(mismatchedIdentity, RELEASE_OPERATION_ID),
    ),
    assertCode("invalid_postgres_writer_detach_composition_request"),
  );
  assert.deepEqual(identityFixture.calls, []);

  const mismatchedCapabilities = sessionSnapshot(
    authorityDocument({ backendCapabilities: capabilities("manual") }),
  );
  const capabilityFixture = createHarness({ session: mismatchedCapabilities });
  await assert.rejects(
    capabilityFixture.composition.forceFenceWriter(
      request(mismatchedCapabilities, FENCE_OPERATION_ID),
    ),
    assertCode("invalid_postgres_writer_detach_composition_request"),
  );
  assert.deepEqual(capabilityFixture.calls, []);
});

for (const scenario of [
  {
    method: "detachWriter",
    operationId: RELEASE_OPERATION_ID,
    providerMethod: "detachAttachment",
    kind: WRITER_RELEASE_OPERATION_KIND,
    outcome: "writer-released",
  },
  {
    method: "forceFenceWriter",
    operationId: FENCE_OPERATION_ID,
    providerMethod: "forceFence",
    kind: WRITER_FORCE_FENCE_OPERATION_KIND,
    outcome: "writer-fenced",
  },
]) {
  test(`${scenario.method} binds exact provider proof and returns one frozen terminal result`, async () => {
    const fixture = createHarness();
    const expectedSession = fixture.state.session;
    const result = await fixture.composition[scenario.method](
      request(expectedSession, scenario.operationId),
    );

    assert.equal(Object.getPrototypeOf(result), null);
    assert.equal(Object.isFrozen(result), true);
    assertDeepFrozen(result);
    assert.deepEqual(Reflect.ownKeys(result), [
      "operation",
      "reservation",
      "session",
    ]);
    assert.equal(result.operation.state, "committed");
    assert.equal(result.operation.result.outcome, scenario.outcome);
    assert.equal(result.session.document.lifecycle, "DETACHED");
    assert.equal(result.session.document.lease, null);
    assert.equal(result.session.document.attachment, null);
    assert.equal(
      fixture.calls.filter(([name]) => name === scenario.providerMethod).length,
      1,
    );
    assert.equal(
      fixture.calls.filter(([name]) =>
        name ===
        (scenario.kind === WRITER_RELEASE_OPERATION_KIND
          ? "claimWriterReleaseDispatch"
          : "claimWriterForceFenceDispatch"),
      ).length,
      1,
    );
    const providerCall = fixture.calls.find(
      ([name]) => name === scenario.providerMethod,
    )[1];
    assert.deepEqual(
      structuredClone(
        scenario.kind === WRITER_RELEASE_OPERATION_KIND
          ? result.operation.result.mutationResult
          : result.operation.result.fenceResult,
      ),
      scenario.kind === WRITER_RELEASE_OPERATION_KIND
        ? detachResult(providerCall)
        : fenceResult(providerCall),
    );
    assert(
      fixture.calls.findIndex(([name]) => name === "assertHeld") <
        fixture.calls.findIndex(([name]) => name === scenario.providerMethod),
    );
    assert(
      fixture.calls.findLastIndex(([name]) => name === "assertHeld") >
        fixture.calls.findIndex(([name]) => name === scenario.providerMethod),
    );
    assert.deepEqual(
      fixture.calls.map(([name]) => name),
      [
        "runExclusive",
        "assertHeld",
        "assertHeld",
        "reserveOperation",
        scenario.kind === WRITER_RELEASE_OPERATION_KIND
          ? "claimWriterReleaseDispatch"
          : "claimWriterForceFenceDispatch",
        "assertHeld",
        scenario.providerMethod,
        "assertHeld",
        "assertHeld",
        scenario.kind === WRITER_RELEASE_OPERATION_KIND
          ? "finalizeWriterRelease"
          : "finalizeWriterForceFence",
        "assertHeld",
      ],
    );
  });
}

test("force fence advances exactly one epoch and proves the revoked fence", async () => {
  const fixture = createHarness();
  const expectedSession = fixture.state.session;
  const result = await fixture.composition.forceFenceWriter(
    request(expectedSession, FENCE_OPERATION_ID),
  );
  const providerRequest = fixture.calls.find(([name]) => name === "forceFence")[1];

  assert.equal(providerRequest.fencingEpoch, "12");
  assert.deepEqual(providerRequest.revokedFence, {
    fencingEpoch: "11",
    holderId: "host-001",
    leaseId: "lease-001",
  });
  assert.equal(result.session.document.writerEpoch, "12");
  assert.equal(result.operation.result.writerEpoch, "12");
});

for (const scenario of [
  {
    method: "detachWriter",
    operationId: RELEASE_OPERATION_ID,
    providerMethod: "detachAttachment",
    kind: WRITER_RELEASE_OPERATION_KIND,
  },
  {
    method: "forceFenceWriter",
    operationId: FENCE_OPERATION_ID,
    providerMethod: "forceFence",
    kind: WRITER_FORCE_FENCE_OPERATION_KIND,
  },
]) {
  test(`${scenario.method} continues an exact prepared operation`, async () => {
    const fixture = createHarness();
    const expectedSession = fixture.state.session;
    await seedPrepared(
      fixture,
      scenario.kind,
      expectedSession,
      scenario.operationId,
    );

    const result = await fixture.composition[scenario.method](
      request(expectedSession, scenario.operationId),
    );

    assert.equal(result.operation.state, "committed");
    assert.equal(result.session.document.lifecycle, "DETACHED");
    assert.equal(
      fixture.calls.filter(([name]) => name === scenario.providerMethod).length,
      1,
    );
  });

  test(`${scenario.method} returns a committed replay without provider dispatch`, async () => {
    const fixture = createHarness();
    const expectedSession = fixture.state.session;
    await seedCommitted(
      fixture,
      scenario.kind,
      expectedSession,
      scenario.operationId,
    );

    const result = await fixture.composition[scenario.method](
      request(expectedSession, scenario.operationId),
    );

    assert.equal(result.operation.state, "committed");
    assert.equal(result.session.document.lifecycle, "DETACHED");
    assert.equal(
      fixture.calls.filter(([name]) => name === scenario.providerMethod).length,
      0,
    );
  });

  for (const initialState of ["starting", "uncertain"]) {
    test(`${scenario.method} converts recovered ${initialState} to durable BLOCKED without provider replay`, async () => {
      const fixture = createHarness();
      const expectedSession = fixture.state.session;
      if (initialState === "starting") {
        await seedStarting(
          fixture,
          scenario.kind,
          expectedSession,
          scenario.operationId,
        );
      } else {
        await seedUncertain(
          fixture,
          scenario.kind,
          expectedSession,
          scenario.operationId,
        );
      }

      const result = await fixture.composition[scenario.method](
        request(expectedSession, scenario.operationId),
      );

      assert.equal(result.operation.state, "committed");
      assert.equal(result.operation.result.outcome, "writer-blocked");
      assert.equal(result.session.document.lifecycle, "BLOCKED");
      assert.equal(
        fixture.calls.filter(([name]) => name === scenario.providerMethod)
          .length,
        0,
      );
      assert.equal(
        result.operation.result.reason,
        scenario.kind === WRITER_FORCE_FENCE_OPERATION_KIND
          ? "provider-outcome-unresolved"
          : "provider-outcome-unresolved",
      );
    });
  }
}

test("manual force fence claims once, preserves the advanced epoch, and blocks without provider", async () => {
  const fixture = createHarness({ fencing: "manual" });
  const expectedSession = fixture.state.session;
  const result = await fixture.composition.forceFenceWriter(
    request(expectedSession, FENCE_OPERATION_ID),
  );

  assert.equal(result.operation.result.outcome, "writer-blocked");
  assert.equal(result.operation.result.reason, "fence-unavailable");
  assert.equal(result.operation.result.writerEpoch, "12");
  assert.equal(result.session.document.lifecycle, "BLOCKED");
  assert.equal(result.session.document.writerEpoch, "12");
  assert.equal(
    fixture.calls.filter(([name]) => name === "claimWriterForceFenceDispatch")
      .length,
    1,
  );
  assert.equal(
    fixture.calls.filter(([name]) => name === "forceFence").length,
    0,
  );
});

const providerFailureScenarios = [
  {
    name: "synchronous throw",
    override(method) {
      return {
        [method]() {
          throw new Error("provider failed synchronously");
        },
      };
    },
  },
  {
    name: "rejection",
    override(method) {
      return {
        [method]() {
          return Promise.reject(new Error("provider rejected"));
        },
      };
    },
  },
  {
    name: "unsafe thenable",
    override(method, observation) {
      return {
        [method]() {
          return unsafeThenable(() => {
            observation.thenCalls += 1;
          });
        },
      };
    },
    verify(observation) {
      assert.equal(observation.thenCalls, 0);
    },
  },
  {
    name: "malformed proof",
    override(method) {
      return {
        [method]() {
          return Promise.resolve(deepFreeze({ malformed: true }));
        },
      };
    },
  },
  {
    name: "crossed proof",
    override(method) {
      return {
        [method](input) {
          const proof =
            method === "detachAttachment"
              ? detachResult(input, { operationId: "other-operation" })
              : fenceResult(input, {
                  revokedFence: {
                    ...input.revokedFence,
                    leaseId: "other-lease",
                  },
                });
          return Promise.resolve(deepFreeze(proof));
        },
      };
    },
  },
];

for (const lane of [
  {
    method: "detachWriter",
    operationId: RELEASE_OPERATION_ID,
    providerMethod: "detachAttachment",
  },
  {
    method: "forceFenceWriter",
    operationId: FENCE_OPERATION_ID,
    providerMethod: "forceFence",
  },
]) {
  for (const scenario of providerFailureScenarios) {
    test(`${lane.method} makes provider ${scenario.name} durably blocked`, async () => {
      const observation = { thenCalls: 0 };
      const fixture = createHarness({
        provider: scenario.override(lane.providerMethod, observation),
      });
      const expectedSession = fixture.state.session;

      const result = await fixture.composition[lane.method](
        request(expectedSession, lane.operationId),
      );

      assert.equal(result.operation.state, "committed");
      assert.equal(result.operation.result.outcome, "writer-blocked");
      assert.equal(
        result.operation.result.reason,
        "provider-outcome-unresolved",
      );
      assert.equal(result.session.document.lifecycle, "BLOCKED");
      assert.equal(
        fixture.calls.filter(([name]) => name === lane.providerMethod).length,
        1,
      );
      assert.equal(
        fixture.calls.filter(([name]) => name === "markOperationUncertain")
          .length,
        1,
      );
      assert.equal(
        fixture.calls.filter(
          ([name]) => name === "finalizeWriterOperationBlocked",
        ).length,
        1,
      );
      scenario.verify?.(observation);
    });
  }
}

test(
  "unsafe rejected provider promises are observed before durable blocking",
  { concurrency: false },
  async () => {
    const unhandledRejections = [];
    const onUnhandledRejection = (reason, promise) => {
      unhandledRejections.push({ promise, reason });
    };
    process.prependListener("unhandledRejection", onUnhandledRejection);
    try {
      const scenarios = [
        {
          name: "Promise subclass",
          create() {
            class RejectedProviderPromise extends Promise {}
            return new RejectedProviderPromise((_resolve, reject) => {
              reject(new Error("provider subclass rejected"));
            });
          },
          verify() {},
        },
        {
          name: "constructor accessor",
          create(observation) {
            const pending = Promise.reject(
              new Error("provider descriptor promise rejected"),
            );
            Object.defineProperty(pending, "constructor", {
              configurable: true,
              enumerable: false,
              get() {
                observation.constructorReads += 1;
                throw new Error("provider constructor getter invoked");
              },
            });
            return pending;
          },
          verify(observation) {
            assert.equal(observation.constructorReads, 0);
          },
        },
      ];
      for (const scenario of scenarios) {
        const observation = { constructorReads: 0 };
        const fixture = createHarness({
          provider: {
            detachAttachment() {
              return scenario.create(observation);
            },
          },
        });
        const expectedSession = fixture.state.session;

        const result = await fixture.composition.detachWriter(
          request(expectedSession, RELEASE_OPERATION_ID),
        );
        await nextTurn();
        await nextTurn();

        assert.equal(
          result.operation.result.outcome,
          "writer-blocked",
          scenario.name,
        );
        assert.equal(
          result.operation.result.reason,
          "provider-outcome-unresolved",
          scenario.name,
        );
        assert.deepEqual(unhandledRejections, [], scenario.name);
        scenario.verify(observation);
      }
    } finally {
      process.removeListener("unhandledRejection", onUnhandledRejection);
    }
  },
);

test(
  "pending provider Promise subclass is drained before durable blocking",
  { concurrency: false },
  async () => {
    const entered = deferred();
    const providerSettlement = deferred();
    const unhandledRejections = [];
    const onUnhandledRejection = (reason, promise) => {
      unhandledRejections.push({ promise, reason });
    };
    let publicSettled = false;
    process.prependListener("unhandledRejection", onUnhandledRejection);
    try {
      class PendingProviderPromise extends Promise {}
      const fixture = createHarness({
        provider: {
          detachAttachment(input) {
            return new PendingProviderPromise((resolve) => {
              providerSettlement.promise.then(() => {
                resolve(deepFreeze(detachResult(input)));
              });
              entered.resolve();
            });
          },
        },
      });
      const expectedSession = fixture.state.session;
      const invocation = fixture.composition.detachWriter(
        request(expectedSession, RELEASE_OPERATION_ID),
      );
      void invocation.then(
        () => {
          publicSettled = true;
        },
        () => {
          publicSettled = true;
        },
      );

      await entered.promise;
      await nextTurn();
      await nextTurn();
      assert.equal(publicSettled, false);
      assert.equal(
        fixture.calls.filter(
          ([name]) => name === "finalizeWriterOperationBlocked",
        ).length,
        0,
      );
      assert.equal(
        fixture.calls.filter(([name]) => name === "finalizeWriterRelease")
          .length,
        0,
      );

      providerSettlement.resolve();
      const result = await invocation;

      assert.equal(result.operation.result.outcome, "writer-blocked");
      assert.equal(
        result.operation.result.reason,
        "provider-outcome-unresolved",
      );
      assert.equal(publicSettled, true);
      assert.equal(
        fixture.calls.filter(
          ([name]) => name === "finalizeWriterOperationBlocked",
        ).length,
        1,
      );
      await nextTurn();
      assert.deepEqual(unhandledRejections, []);
    } finally {
      process.removeListener(
        "unhandledRejection",
        onUnhandledRejection,
      );
    }
  },
);

test("release proof cannot be crossed onto another exact target", async () => {
  const fixture = createHarness({
    provider: {
      detachAttachment(input) {
        return Promise.resolve(
          deepFreeze(
            detachResult(input, {
              target: { attachmentId: "attachment-foreign", kind: "attachment" },
            }),
          ),
        );
      },
    },
  });
  const expectedSession = fixture.state.session;

  const result = await fixture.composition.detachWriter(
    request(expectedSession, RELEASE_OPERATION_ID),
  );

  assert.equal(result.operation.result.outcome, "writer-blocked");
  assert.equal(result.operation.result.reason, "provider-outcome-unresolved");
});

test("release claim storage envelope drift blocks before provider dispatch", async () => {
  const fixture = createHarness();
  const expectedSession = fixture.state.session;
  fixture.enqueue("claimWriterReleaseDispatch", {
    transform(claim) {
      return deepFreeze({
        ...structuredClone(claim),
        mutationRequest: {
          ...structuredClone(claim.mutationRequest),
          storageId: "volume-crossed",
        },
      });
    },
  });

  const result = await fixture.composition.detachWriter(
    request(expectedSession, RELEASE_OPERATION_ID),
  );

  assert.equal(result.operation.result.outcome, "writer-blocked");
  assert.equal(result.operation.result.reason, "provider-outcome-unresolved");
  assert.equal(result.session.document.lifecycle, "BLOCKED");
  assert.equal(
    fixture.calls.filter(([name]) => name === "detachAttachment").length,
    0,
  );
});

for (const scenario of [
  {
    name: "storage ID",
    cross(requestValue) {
      return { ...requestValue, storageId: "volume-crossed" };
    },
  },
  {
    name: "new fencing epoch",
    cross(requestValue) {
      return { ...requestValue, fencingEpoch: "13" };
    },
  },
  {
    name: "revoked fence",
    cross(requestValue) {
      return {
        ...requestValue,
        revokedFence: {
          ...requestValue.revokedFence,
          holderId: "host-crossed",
        },
      };
    },
  },
]) {
  test(`force-fence claim ${scenario.name} drift blocks before provider dispatch`, async () => {
    const fixture = createHarness();
    const expectedSession = fixture.state.session;
    fixture.enqueue("claimWriterForceFenceDispatch", {
      transform(claim) {
        return deepFreeze({
          ...structuredClone(claim),
          fenceRequest: scenario.cross(
            structuredClone(claim.fenceRequest),
          ),
        });
      },
    });

    const result = await fixture.composition.forceFenceWriter(
      request(expectedSession, FENCE_OPERATION_ID),
    );

    assert.equal(result.operation.result.outcome, "writer-blocked");
    assert.equal(
      result.operation.result.reason,
      "provider-outcome-unresolved",
    );
    assert.equal(result.session.document.lifecycle, "BLOCKED");
    assert.equal(
      fixture.calls.filter(([name]) => name === "forceFence").length,
      0,
    );
  });
}

test("non-boolean dispatchGranted never authorizes provider dispatch", async () => {
  const fixture = createHarness();
  const expectedSession = fixture.state.session;
  fixture.enqueue("claimWriterReleaseDispatch", {
    transform(claim) {
      return deepFreeze({
        ...structuredClone(claim),
        dispatchGranted: "true",
      });
    },
  });

  const result = await fixture.composition.detachWriter(
    request(expectedSession, RELEASE_OPERATION_ID),
  );

  assert.equal(result.operation.result.outcome, "writer-blocked");
  assert.equal(result.operation.result.reason, "provider-outcome-unresolved");
  assert.equal(result.session.document.lifecycle, "BLOCKED");
  assert.equal(
    fixture.calls.filter(([name]) => name === "detachAttachment").length,
    0,
  );
});

for (const scenario of [
  {
    name: "mutable request",
    create(observation) {
      const nestedTarget = target();
      const nestedRequest = { contractVersion: 1, target: nestedTarget };
      observation.nestedRequest = nestedRequest;
      observation.nestedTarget = nestedTarget;
      return nestedRequest;
    },
  },
  {
    name: "accessor request",
    create(observation) {
      const nestedTarget = target();
      const nestedRequest = { contractVersion: 1 };
      Object.defineProperty(nestedRequest, "target", {
        enumerable: true,
        get() {
          observation.traps += 1;
          return nestedTarget;
        },
      });
      observation.nestedRequest = nestedRequest;
      observation.nestedTarget = nestedTarget;
      return Object.freeze(nestedRequest);
    },
  },
  {
    name: "Proxy target",
    create(observation) {
      const nestedTarget = new Proxy(target(), {
        get() {
          observation.traps += 1;
          throw new Error("nested target trap must not run");
        },
        getOwnPropertyDescriptor() {
          observation.traps += 1;
          throw new Error("nested target trap must not run");
        },
        getPrototypeOf() {
          observation.traps += 1;
          throw new Error("nested target trap must not run");
        },
        ownKeys() {
          observation.traps += 1;
          throw new Error("nested target trap must not run");
        },
      });
      const nestedRequest = Object.freeze({
        contractVersion: 1,
        target: nestedTarget,
      });
      observation.nestedRequest = nestedRequest;
      observation.nestedTarget = nestedTarget;
      return nestedRequest;
    },
  },
]) {
  test(`shallow-frozen claim with ${scenario.name} reconciles to BLOCKED without leaking nested data`, async () => {
    const fixture = createHarness();
    const expectedSession = fixture.state.session;
    const observation = {
      nestedRequest: null,
      nestedTarget: null,
      traps: 0,
    };
    fixture.enqueue("claimWriterReleaseDispatch", {
      transform(claim) {
        const nestedRequest = scenario.create(observation);
        return Object.freeze({
          ...claim,
          operation: Object.freeze({
            ...claim.operation,
            request: nestedRequest,
          }),
        });
      },
    });

    const result = await fixture.composition.detachWriter(
      request(expectedSession, RELEASE_OPERATION_ID),
    );

    assert.equal(result.operation.result.outcome, "writer-blocked");
    assert.equal(
      result.operation.result.reason,
      "provider-outcome-unresolved",
    );
    assert.equal(result.session.document.lifecycle, "BLOCKED");
    assert.equal(
      fixture.calls.filter(([name]) => name === "detachAttachment").length,
      0,
    );
    assert.equal(observation.traps, 0);
    assert.notStrictEqual(
      result.operation.request,
      observation.nestedRequest,
    );
    assert.notStrictEqual(
      result.operation.request.target,
      observation.nestedTarget,
    );
    assertDeepFrozen(result);
  });
}

for (const scenario of [
  {
    name: "depth greater than 64",
    transform(claim) {
      let nested = Object.freeze({ terminal: true });
      for (let depth = 0; depth < 65; depth += 1) {
        nested = Object.freeze({ next: nested });
      }
      return Object.freeze({
        ...claim,
        operation: Object.freeze({
          ...claim.operation,
          request: Object.freeze({
            contractVersion: 1,
            target: Object.freeze({
              ...claim.operation.request.target,
              nested,
            }),
          }),
        }),
      });
    },
  },
  {
    name: "sparse array length greater than 131072",
    transform(claim) {
      const sparse = new Array(131_073);
      Object.freeze(sparse);
      return Object.freeze({
        ...claim,
        operation: Object.freeze({
          ...claim.operation,
          request: Object.freeze({
            contractVersion: 1,
            target: Object.freeze({
              ...claim.operation.request.target,
              sparse,
            }),
          }),
        }),
      });
    },
  },
]) {
  test(`receipt clone rejects ${scenario.name} without provider dispatch`, async () => {
    const fixture = createHarness();
    const expectedSession = fixture.state.session;
    fixture.enqueue("claimWriterReleaseDispatch", {
      transform: scenario.transform,
    });

    const result = await fixture.composition.detachWriter(
      request(expectedSession, RELEASE_OPERATION_ID),
    );

    assert.equal(result.operation.result.outcome, "writer-blocked");
    assert.equal(
      result.operation.result.reason,
      "provider-outcome-unresolved",
    );
    assert.equal(result.session.document.lifecycle, "BLOCKED");
    assert.equal(
      fixture.calls.filter(([name]) => name === "detachAttachment").length,
      0,
    );
  });
}

test("reserve acknowledgement loss reconciles prepared state without duplicate reserve", async () => {
  const fixture = createHarness();
  const expectedSession = fixture.state.session;
  fixture.enqueue("reserveOperation", {
    error: new Error("reserve acknowledgement lost"),
    timing: "after",
  });

  const result = await fixture.composition.detachWriter(
    request(expectedSession, RELEASE_OPERATION_ID),
  );

  assert.equal(result.operation.state, "committed");
  assert.equal(
    fixture.calls.filter(([name]) => name === "reserveOperation").length,
    1,
  );
  assert.equal(
    fixture.calls.filter(([name]) => name === "reconcileOperation").length,
    1,
  );
  assert.equal(
    fixture.calls.filter(([name]) => name === "detachAttachment").length,
    1,
  );
});

test("reserve failure retries once only after authoritative absence", async () => {
  const fixture = createHarness();
  const expectedSession = fixture.state.session;
  fixture.enqueue("reserveOperation", {
    error: new Error("reserve did not commit"),
    timing: "before",
  });

  const result = await fixture.composition.detachWriter(
    request(expectedSession, RELEASE_OPERATION_ID),
  );

  assert.equal(result.operation.state, "committed");
  assert.equal(
    fixture.calls.filter(([name]) => name === "reserveOperation").length,
    2,
  );
  assert.equal(
    fixture.calls.filter(([name]) => name === "reconcileOperation").length,
    1,
  );
  assert.equal(
    fixture.calls.filter(([name]) => name === "detachAttachment").length,
    1,
  );
});

test("claim acknowledgement loss at starting never dispatches provider", async () => {
  const fixture = createHarness();
  const expectedSession = fixture.state.session;
  fixture.enqueue("claimWriterReleaseDispatch", {
    error: new Error("claim acknowledgement lost"),
    timing: "after",
  });

  const result = await fixture.composition.detachWriter(
    request(expectedSession, RELEASE_OPERATION_ID),
  );

  assert.equal(result.operation.result.outcome, "writer-blocked");
  assert.equal(result.operation.result.reason, "provider-outcome-unresolved");
  assert.equal(
    fixture.calls.filter(([name]) => name === "detachAttachment").length,
    0,
  );
  assert.equal(
    fixture.calls.filter(([name]) => name === "reconcileOperation").length,
    1,
  );
});

test("known valid proof survives finalizer failure and retries only the DB finalizer", async () => {
  const fixture = createHarness();
  const expectedSession = fixture.state.session;
  fixture.enqueue("finalizeWriterRelease", {
    error: new Error("finalizer transaction rolled back"),
    timing: "before",
  });

  const result = await fixture.composition.detachWriter(
    request(expectedSession, RELEASE_OPERATION_ID),
  );

  assert.equal(result.operation.result.outcome, "writer-released");
  assert.equal(
    fixture.calls.filter(([name]) => name === "detachAttachment").length,
    1,
  );
  assert.equal(
    fixture.calls.filter(([name]) => name === "finalizeWriterRelease").length,
    2,
  );
  assert.equal(
    fixture.calls.filter(
      ([name]) => name === "finalizeWriterOperationBlocked",
    ).length,
    0,
  );
});

test("committed finalizer acknowledgement loss returns readback without provider replay", async () => {
  const fixture = createHarness();
  const expectedSession = fixture.state.session;
  fixture.enqueue("finalizeWriterForceFence", {
    error: new Error("finalizer acknowledgement lost"),
    timing: "after",
  });

  const result = await fixture.composition.forceFenceWriter(
    request(expectedSession, FENCE_OPERATION_ID),
  );

  assert.equal(result.operation.result.outcome, "writer-fenced");
  assert.equal(
    fixture.calls.filter(([name]) => name === "forceFence").length,
    1,
  );
  assert.equal(
    fixture.calls.filter(([name]) => name === "finalizeWriterForceFence")
      .length,
    1,
  );
  assert.equal(
    fixture.calls.filter(([name]) => name === "reconcileOperation").length,
    1,
  );
});

for (const lane of [
  {
    finalizer: "finalizeWriterRelease",
    method: "detachWriter",
    operationId: RELEASE_OPERATION_ID,
    proofKey: "mutationResult",
    provider: "detachAttachment",
  },
  {
    finalizer: "finalizeWriterForceFence",
    method: "forceFenceWriter",
    operationId: FENCE_OPERATION_ID,
    proofKey: "fenceResult",
    provider: "forceFence",
  },
]) {
  test(`${lane.method} rejects a committed ACK-loss readback with a different provider proof`, async () => {
    const fixture = createHarness();
    const expectedSession = fixture.state.session;
    fixture.enqueue(lane.finalizer, {
      error: new Error("finalizer acknowledgement lost"),
      timing: "after",
    });
    fixture.enqueue("reconcileOperation", {
      transform: (receipt) => driftTerminalProof(receipt, lane.proofKey),
    });

    await assert.rejects(
      fixture.composition[lane.method](
        request(expectedSession, lane.operationId),
      ),
      assertCode("postgres_writer_detach_composition_outcome_uncertain"),
    );

    assert.equal(
      fixture.calls.filter(([name]) => name === lane.provider).length,
      1,
    );
    assert.equal(
      fixture.calls.filter(([name]) => name === lane.finalizer).length,
      1,
    );
    assert.equal(
      fixture.calls.filter(([name]) => name === "reconcileOperation").length,
      1,
    );
  });
}

test("second finalizer ACK loss rejects a drifted committed readback proof", async () => {
  const fixture = createHarness();
  const expectedSession = fixture.state.session;
  fixture.enqueue("finalizeWriterRelease", {
    error: new Error("finalizer transaction rolled back"),
    timing: "before",
  });
  fixture.enqueue("finalizeWriterRelease", {
    error: new Error("finalizer acknowledgement lost"),
    timing: "after",
  });
  fixture.enqueue("reconcileOperation", {});
  fixture.enqueue("reconcileOperation", {
    transform: (receipt) => driftTerminalProof(receipt, "mutationResult"),
  });

  await assert.rejects(
    fixture.composition.detachWriter(
      request(expectedSession, RELEASE_OPERATION_ID),
    ),
    assertCode("postgres_writer_detach_composition_outcome_uncertain"),
  );

  assert.equal(
    fixture.calls.filter(([name]) => name === "detachAttachment").length,
    1,
  );
  assert.equal(
    fixture.calls.filter(([name]) => name === "finalizeWriterRelease").length,
    2,
  );
  assert.equal(
    fixture.calls.filter(([name]) => name === "reconcileOperation").length,
    2,
  );
});

test("uncertain acknowledgement loss reads uncertain state then blocks", async () => {
  const fixture = createHarness({
    provider: {
      detachAttachment() {
        throw new Error("provider outcome unknown");
      },
    },
  });
  const expectedSession = fixture.state.session;
  fixture.enqueue("markOperationUncertain", {
    error: new Error("uncertain acknowledgement lost"),
    timing: "after",
  });

  const result = await fixture.composition.detachWriter(
    request(expectedSession, RELEASE_OPERATION_ID),
  );

  assert.equal(result.operation.result.outcome, "writer-blocked");
  assert.equal(
    fixture.calls.filter(([name]) => name === "markOperationUncertain").length,
    1,
  );
  assert.equal(
    fixture.calls.filter(([name]) => name === "reconcileOperation").length,
    1,
  );
  assert.equal(
    fixture.calls.filter(
      ([name]) => name === "finalizeWriterOperationBlocked",
    ).length,
    1,
  );
});

test("blocked finalizer acknowledgement loss accepts exact terminal readback", async () => {
  const fixture = createHarness({
    provider: {
      forceFence() {
        throw new Error("fence outcome unknown");
      },
    },
  });
  const expectedSession = fixture.state.session;
  fixture.enqueue("finalizeWriterOperationBlocked", {
    error: new Error("blocked acknowledgement lost"),
    timing: "after",
  });

  const result = await fixture.composition.forceFenceWriter(
    request(expectedSession, FENCE_OPERATION_ID),
  );

  assert.equal(result.operation.result.outcome, "writer-blocked");
  assert.equal(
    fixture.calls.filter(
      ([name]) => name === "finalizeWriterOperationBlocked",
    ).length,
    1,
  );
  assert.equal(
    fixture.calls.filter(([name]) => name === "reconcileOperation").length,
    1,
  );
});

test("guard busy is a fixed composition error with zero callback side effect", async () => {
  const fixture = createHarness({ guardMode: "busy" });
  const expectedSession = fixture.state.session;

  await assert.rejects(
    fixture.composition.detachWriter(
      request(expectedSession, RELEASE_OPERATION_ID),
    ),
    assertCode("postgres_writer_detach_composition_outcome_uncertain"),
  );

  assert.deepEqual(
    fixture.calls.map(([name]) => name),
    ["runExclusive"],
  );
  assert.equal(fixture.state.phase, "absent");
});

test("factory rejects a frozen structural guard without invoking it", () => {
  const fixture = createHarness();
  let fakeGuardCalls = 0;
  const structuralGuard = nullRecord([
    [
      "runExclusive",
      exactFrozenFunction(async () => {
        fakeGuardCalls += 1;
      }),
    ],
  ]);

  assert.throws(
    () =>
      createPostgresWriterDetachComposition({
        authority: fixture.authority,
        operationGuard: structuralGuard,
        storageBackend: fixture.storageBackend,
      }),
    assertCode("invalid_postgres_writer_detach_composition_options"),
  );
  assert.equal(fakeGuardCalls, 0);
  assert.deepEqual(fixture.calls, []);
  assert.equal(fixture.state.phase, "absent");
});

test("factory rejects an unbranded guard prototype impostor without effects", () => {
  const fixture = createHarness();
  const structuralGuard = Object.freeze(
    Object.create(PostgresOperationGuard.prototype),
  );

  assert.throws(
    () =>
      createPostgresWriterDetachComposition({
        authority: fixture.authority,
        operationGuard: structuralGuard,
        storageBackend: fixture.storageBackend,
      }),
    assertCode("invalid_postgres_writer_detach_composition_options"),
  );
  assert.deepEqual(fixture.calls, []);
  assert.equal(fixture.state.phase, "absent");
});

test("invocation uses the captured guard intrinsic instead of an override", async () => {
  const fixture = createHarness();
  let overrideCalls = 0;
  class OverriddenOperationGuard extends PostgresOperationGuard {
    async runExclusive() {
      overrideCalls += 1;
      throw new Error("guard override must not run");
    }
  }
  const operationGuard = new OverriddenOperationGuard({
    dedicatedPool: fixture.guardPool,
  });
  const composition = createPostgresWriterDetachComposition({
    authority: fixture.authority,
    operationGuard,
    storageBackend: fixture.storageBackend,
  });
  const expectedSession = fixture.state.session;

  const result = await composition.detachWriter(
    request(expectedSession, RELEASE_OPERATION_ID),
  );

  assert.equal(result.operation.result.outcome, "writer-released");
  assert.equal(overrideCalls, 0);
  assert.equal(
    fixture.calls.filter(([name]) => name === "detachAttachment").length,
    1,
  );
});

test("pre-reserve guard probe failure leaves no durable or provider side effect", async () => {
  const fixture = createHarness({ guardMode: "probe-1" });
  const expectedSession = fixture.state.session;

  await assert.rejects(
    fixture.composition.detachWriter(
      request(expectedSession, RELEASE_OPERATION_ID),
    ),
    assertCode("postgres_writer_detach_composition_outcome_uncertain"),
  );
  assert.equal(
    fixture.calls.filter(([name]) => name === "detachAttachment").length,
    0,
  );
  assert.equal(fixture.state.phase, "absent");

  fixture.guardState.mode = "normal";
  fixture.calls.length = 0;
  const replay = await fixture.composition.detachWriter(
    request(expectedSession, RELEASE_OPERATION_ID),
  );
  assert.equal(replay.operation.result.outcome, "writer-released");
  assert.equal(
    fixture.calls.filter(([name]) => name === "detachAttachment").length,
    1,
  );
});

for (const probeNumber of [2, 3]) {
  test(`guard probe ${probeNumber} durably blocks an ambiguous claimed operation`, async () => {
    const fixture = createHarness({ guardMode: `probe-${probeNumber}` });
    const expectedSession = fixture.state.session;

    await assert.rejects(
      fixture.composition.detachWriter(
        request(expectedSession, RELEASE_OPERATION_ID),
      ),
      assertCode("postgres_writer_detach_composition_outcome_uncertain"),
    );
    assert.equal(fixture.state.phase, "committed");
    assert.equal(fixture.state.resultOutcome, "writer-blocked");
    assert.equal(
      fixture.calls.filter(([name]) => name === "detachAttachment").length,
      probeNumber === 2 ? 0 : 1,
    );

    fixture.guardState.mode = "normal";
    fixture.calls.length = 0;
    const replay = await fixture.composition.detachWriter(
      request(expectedSession, RELEASE_OPERATION_ID),
    );
    assert.equal(replay.operation.result.outcome, "writer-blocked");
    assert.equal(
      fixture.calls.filter(([name]) => name === "detachAttachment").length,
      0,
    );
  });
}

test("post-callback guard cleanup uncertainty replays committed state without provider", async () => {
  const fixture = createHarness({ guardMode: "cleanup" });
  const expectedSession = fixture.state.session;

  await assert.rejects(
    fixture.composition.forceFenceWriter(
      request(expectedSession, FENCE_OPERATION_ID),
    ),
    assertCode("postgres_writer_detach_composition_outcome_uncertain"),
  );
  assert.equal(fixture.state.resultOutcome, "writer-fenced");
  assert.equal(
    fixture.calls.filter(([name]) => name === "forceFence").length,
    1,
  );

  fixture.guardState.mode = "normal";
  fixture.calls.length = 0;
  const replay = await fixture.composition.forceFenceWriter(
    request(expectedSession, FENCE_OPERATION_ID),
  );
  assert.equal(replay.operation.result.outcome, "writer-fenced");
  assert.equal(
    fixture.calls.filter(([name]) => name === "forceFence").length,
    0,
  );
});

test("same-operation concurrency rejects the busy contender before authority", async () => {
  const entered = deferred();
  const release = deferred();
  let providerCalls = 0;
  const fixture = createHarness({
    provider: {
      async detachAttachment(input) {
        providerCalls += 1;
        entered.resolve();
        await release.promise;
        return deepFreeze(detachResult(input));
      },
    },
  });
  const expectedSession = fixture.state.session;
  const exactRequest = request(expectedSession, RELEASE_OPERATION_ID);

  const first = fixture.composition.detachWriter(exactRequest);
  await entered.promise;
  const second = fixture.composition.detachWriter(exactRequest);
  await assert.rejects(
    second,
    assertCode("postgres_writer_detach_composition_outcome_uncertain"),
  );
  release.resolve();
  const firstResult = await first;

  assert.equal(firstResult.operation.result.outcome, "writer-released");
  assert.equal(providerCalls, 1);
  assert.equal(
    fixture.calls.filter(([name]) => name === "reserveOperation").length,
    1,
  );
});

test(
  "provider callback cannot poison captured array membership validation",
  { concurrency: false },
  async () => {
    const includesDescriptor = Object.getOwnPropertyDescriptor(
      Array.prototype,
      "includes",
    );
    const fixture = createHarness({
      provider: {
        detachAttachment(input) {
          Object.defineProperty(Array.prototype, "includes", {
            ...includesDescriptor,
            value: () => true,
          });
          return Promise.resolve(
            deepFreeze(
              detachResult(input, { operationId: "crossed-operation" }),
            ),
          );
        },
      },
    });
    const expectedSession = fixture.state.session;

    let result;
    let failure = null;
    try {
      result = await fixture.composition.detachWriter(
        request(expectedSession, RELEASE_OPERATION_ID),
      );
    } catch (error) {
      failure = error;
    } finally {
      Object.defineProperty(
        Array.prototype,
        "includes",
        includesDescriptor,
      );
    }

    if (failure !== null) throw failure;
    assert.equal(result.operation.result.outcome, "writer-blocked");
    assert.equal(
      result.operation.result.reason,
      "provider-outcome-unresolved",
    );
  },
);

test(
  "provider callback cannot poison Promise prototype constructor or then",
  { concurrency: false },
  async () => {
    const constructorDescriptor = Object.getOwnPropertyDescriptor(
      Promise.prototype,
      "constructor",
    );
    const thenDescriptor = Object.getOwnPropertyDescriptor(
      Promise.prototype,
      "then",
    );
    const poisonCalls = [];
    let restored = false;
    const restorePromisePrototype = () => {
      if (restored) return;
      restored = true;
      Object.defineProperty(
        Promise.prototype,
        "constructor",
        constructorDescriptor,
      );
      Object.defineProperty(Promise.prototype, "then", thenDescriptor);
    };
    const fixture = createHarness({
      provider: {
        detachAttachment(input) {
          const pending = Promise.resolve(
            deepFreeze(detachResult(input)),
          );
          Object.defineProperty(Promise.prototype, "constructor", {
            configurable: constructorDescriptor.configurable,
            enumerable: constructorDescriptor.enumerable,
            get() {
              poisonCalls.push("Promise.prototype.constructor");
              throw new Error("poisoned Promise constructor invoked");
            },
          });
          Object.defineProperty(Promise.prototype, "then", {
            ...thenDescriptor,
            value() {
              poisonCalls.push("Promise.prototype.then");
              throw new Error("poisoned Promise then invoked");
            },
          });
          queueMicrotask(restorePromisePrototype);
          return pending;
        },
      },
    });
    const expectedSession = fixture.state.session;

    let result;
    let failure = null;
    try {
      result = await fixture.composition.detachWriter(
        request(expectedSession, RELEASE_OPERATION_ID),
      );
    } catch (error) {
      failure = error;
    } finally {
      restorePromisePrototype();
    }

    assert.deepEqual(poisonCalls, []);
    if (failure !== null) throw failure;
    assert.equal(result.operation.result.outcome, "writer-released");
    assert.equal(result.session.document.lifecycle, "DETACHED");
  },
);
