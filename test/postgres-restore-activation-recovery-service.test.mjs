import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { syncBuiltinESMExports } from "node:module";
import path from "node:path";
import test from "node:test";

import { PostgresOperationGuard } from "../src/postgres-operation-guard.mjs";
import {
  PostgresRestoreActivationRecoveryServiceError,
  consumePostgresRestoreActivationRecoveryBatchReceipt,
  createPostgresRestoreActivationRecoveryService,
  isPostgresRestoreActivationRecoveryService,
} from "../src/postgres-restore-activation-recovery-service.mjs";
import {
  PostgresRestoreLifecycleGuardError,
  createPostgresRestoreLifecycleGuard,
} from "../src/postgres-restore-lifecycle-guard.mjs";

const SESSION_ID = "019f2100-0000-7000-8000-000000000001";
const OTHER_SESSION_ID = "019f2100-0000-7000-8000-000000000003";
const THIRD_SESSION_ID = "019f2100-0000-7000-8000-000000000004";
const CODEX_ID = "019f2100-0000-7000-8000-000000000002";
const IMAGE_DIGEST = `sha256:${"a".repeat(64)}`;
const STATE_OWNER_ID = `state-owner:${"a".repeat(64)}`;

function canonicalJson(value) {
  if (Array.isArray(value)) return value.map(canonicalJson);
  if (value !== null && typeof value === "object") {
    const result = Object.create(null);
    for (const key of Object.keys(value).sort()) {
      result[key] = canonicalJson(value[key]);
    }
    return result;
  }
  return value;
}

function podmanWriterRequestSha256(request) {
  return createHash("sha256")
    .update("portable-codex-runtime:podman-writer-request:v1\0", "utf8")
    .update(JSON.stringify(canonicalJson(request)), "utf8")
    .digest("hex");
}

class ServiceRecoveryLockManager {
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

  releaseAll(client) {
    if (this.holder === client) this.holder = null;
  }
}

class ServiceRecoveryGuardClient {
  constructor({ events, loseWhen, manager, pid }) {
    this.events = events;
    this.loseWhen = loseWhen;
    this.manager = manager;
    this.pid = pid;
  }

  query(...args) {
    assert.equal(args.length, 1);
    const query = args[0];
    const text = query.text;
    const callbackDescriptor = Object.getOwnPropertyDescriptor(
      query,
      "callback",
    );
    assert.equal(callbackDescriptor?.enumerable, true);
    assert.equal(Object.hasOwn(callbackDescriptor, "value"), true);
    const callback = callbackDescriptor.value;
    if (text === "DISCARD ALL") {
      this.manager.releaseAll(this);
      callback(null, { command: "DISCARD", rows: [] });
      return undefined;
    }
    if (text.includes("pg_try_advisory_lock")) {
      callback(null, {
        command: "SELECT",
        rows: [
          {
            acquired: this.manager.tryAcquire(this),
            backend_pid: this.pid,
          },
        ],
      });
      return undefined;
    }
    if (text.includes("FROM pg_catalog.pg_locks")) {
      this.events?.push("probe");
      if (this.loseWhen?.()) this.manager.releaseAll(this);
      callback(null, {
        command: "SELECT",
        rows: [
          {
            backend_pid: this.pid,
            lock_held: this.manager.isHeld(this),
          },
        ],
      });
      return undefined;
    }
    if (text.includes("pg_advisory_unlock")) {
      callback(null, {
        command: "SELECT",
        rows: [
          {
            backend_pid: this.pid,
            unlocked: this.manager.unlock(this),
          },
        ],
      });
      return undefined;
    }
    callback(new Error(`unexpected query: ${text}`));
    return undefined;
  }

  release(...args) {
    if (args.length === 1) this.manager.releaseAll(this);
    return undefined;
  }
}

class ServiceRecoveryGuardPool {
  constructor({ events = null, loseWhen = null, manager = null } = {}) {
    this.events = events;
    this.loseWhen = loseWhen;
    this.manager = manager ?? new ServiceRecoveryLockManager();
    this.nextPid = 5_001;
  }

  connect(callback) {
    const client = new ServiceRecoveryGuardClient({
      events: this.events,
      loseWhen: this.loseWhen,
      manager: this.manager,
      pid: this.nextPid,
    });
    this.nextPid += 1;
    const release = (...args) => client.release(...args);
    callback(null, client, release);
    return undefined;
  }
}

function createLifecycleFixture(options = {}) {
  const manager = options.manager ?? new ServiceRecoveryLockManager();
  const foregroundPool = new ServiceRecoveryGuardPool({
    ...options,
    manager,
  });
  const recoveryPool = new ServiceRecoveryGuardPool({
    ...options,
    manager,
  });
  const foregroundOperationGuard = new PostgresOperationGuard({
    dedicatedPool: foregroundPool,
  });
  const recoveryOperationGuard = new PostgresOperationGuard({
    dedicatedPool: recoveryPool,
  });
  return {
    foregroundPool,
    guard: createPostgresRestoreLifecycleGuard({
      foregroundOperationGuard,
      recoveryOperationGuard,
    }),
    manager,
    recoveryPool,
  };
}

function checkpoint(sessionId = SESSION_ID) {
  return {
    contractVersion: 1,
    checkpointId: `checkpoint-${sessionId}`,
    artifactId: `artifact-${sessionId}`,
    backendId: "backend-001",
    storageId: "storage-001",
    sessionId,
    codexThreadId: CODEX_ID,
    codexSessionId: CODEX_ID,
    imageDigest: IMAGE_DIGEST,
    sourceFencingEpoch: "1",
    checkpointClass: "clean",
    createdAt: "2026-08-05T00:00:00.000Z",
  };
}

function restoreRequest(sessionId = SESSION_ID) {
  const value = checkpoint(sessionId);
  return {
    contractVersion: 1,
    backendId: value.backendId,
    storageId: value.storageId,
    sessionId,
    leaseId: `lease-${sessionId}`,
    holderId: "host-001",
    fencingEpoch: "2",
    operation: "restore",
    operationId: `restore-${sessionId}`,
    target: {
      artifactId: value.artifactId,
      checkpointId: value.checkpointId,
      kind: "checkpoint",
    },
  };
}

function generationCandidate(sessionId = SESSION_ID, { v2 = false } = {}) {
  return {
    checkpoint: checkpoint(sessionId),
    generationId: `generation-${sessionId}`,
    ...(v2
      ? {
          launchIntent: launchIntent(sessionId),
        }
      : {}),
    request: restoreRequest(sessionId),
  };
}

function generationReference(sessionId = SESSION_ID) {
  return {
    bindingSha256: "b".repeat(64),
    checkpointId: `checkpoint-${sessionId}`,
    claimedAt: "2026-08-05T00:01:00.000Z",
    committedAt: "2026-08-05T00:02:00.000Z",
    documentSha256: "c".repeat(64),
    generationId: `generation-${sessionId}`,
    operationId: `restore-${sessionId}`,
    sessionId,
    state: "committed",
  };
}

function measuredImage() {
  return {
    projection: {
      codexSandbox: "danger-full-access",
      codexVersion: "codex-cli 0.142.4",
      platformImage: {
        architecture: "arm64",
        config: {
          digest: `sha256:${"d".repeat(64)}`,
          mediaType: "application/vnd.oci.image.config.v1+json",
          size: 1024,
        },
        digest: IMAGE_DIGEST,
        mediaType: "application/vnd.oci.image.manifest.v1+json",
        os: "linux",
        size: 2048,
      },
    },
    runtimeIdentity: {
      codexBinaryPath: "/usr/local/bin/codex",
      codexBinarySha256: "e".repeat(64),
      codexVersion: "codex-cli 0.142.4",
      platformImageDigest: IMAGE_DIGEST,
    },
  };
}

function launchIntent(sessionId = SESSION_ID) {
  return {
    launchAttemptId: `launch-${sessionId}`,
    measuredImage: measuredImage(),
    supervisor: { contractVersion: 1, supervisorId: "supervisor-001" },
  };
}

function lease(sessionId = SESSION_ID) {
  return {
    contractVersion: 1,
    sessionId,
    leaseId: `new-lease-${sessionId}`,
    holderId: "host-002",
    fencingEpoch: "3",
    expiresAt: "2026-08-05T00:10:00.000Z",
  };
}

function attachment(sessionId = SESSION_ID) {
  const writerLease = lease(sessionId);
  return {
    contractVersion: 1,
    backendId: "backend-001",
    storageId: "storage-001",
    sessionId,
    attachmentId: `attachment-${sessionId}`,
    leaseId: writerLease.leaseId,
    holderId: writerLease.holderId,
    fencingEpoch: writerLease.fencingEpoch,
    operationId: `activation-${sessionId}`,
    proofId: `proof-${sessionId}`,
    kind: "directory",
    rootPath: `/var/lib/portable-codex/${sessionId}`,
    mode: "read-write",
  };
}

function launchRequest(sessionId = SESSION_ID) {
  return {
    attachment: attachment(sessionId),
    contractVersion: 1,
    fencingEpoch: lease(sessionId).fencingEpoch,
    generation: generationReference(sessionId),
    lease: lease(sessionId),
    measuredImage: measuredImage(),
    supervisor: { contractVersion: 1, supervisorId: "supervisor-001" },
  };
}

function activationCandidate(
  sessionId = SESSION_ID,
  state = "starting",
  { v2 = false } = {},
) {
  return {
    activationOperationId: `activation-${sessionId}`,
    request: {
      contractVersion: v2 ? 2 : 1,
      destinationRootPath: `/var/lib/portable-codex/${sessionId}`,
      generation: generationReference(sessionId),
      holderId: "host-002",
      launchIntent: launchIntent(sessionId),
      leaseDurationMilliseconds: 600_000,
      predecessor: {
        attachmentId: `old-attachment-${sessionId}`,
        ...(v2
          ? { captureOperationId: `capture-${sessionId}` }
          : {}),
        detachOperationId: `detach-${sessionId}`,
        stopOperationId: `stop-${sessionId}`,
      },
    },
    state,
  };
}

function launchCandidate(sessionId = SESSION_ID, state = "prepared") {
  return {
    launchAttemptId: `launch-${sessionId}`,
    request: launchRequest(sessionId),
    state,
  };
}

function currentLaunchCandidate(sessionId = SESSION_ID) {
  const launchAttemptId = `launch-${sessionId}`;
  return {
    launch: { launchAttemptId },
    launchAttemptId,
    request: launchRequest(sessionId),
  };
}

function supervisorStateGcSession(sessionId = SESSION_ID) {
  return {
    createdAt: "2026-08-05T00:00:00.000Z",
    document: { documentVersion: 3 },
    revision: "4",
    sessionId,
    updatedAt: "2026-08-05T00:05:00.000Z",
  };
}

function supervisorStateGcOperation({
  kind,
  operationId,
  request = { contractVersion: 1 },
  sessionId = SESSION_ID,
}) {
  return {
    conflictClass: "session-mutation",
    createdAt: "2026-08-05T00:00:00.000Z",
    expectedSession: supervisorStateGcSession(sessionId),
    kind,
    operationId,
    request,
    requestSha256: "e".repeat(64),
    result: { resultVersion: 1 },
    retiredAt: "2026-08-05T00:05:00.000Z",
    revision: "2",
    sessionId,
    state: "committed",
    updatedAt: "2026-08-05T00:05:00.000Z",
  };
}

function supervisorStateGcCandidate(
  sessionId = SESSION_ID,
  {
    authorizedAt = "2026-08-05T00:06:00.000Z",
    terminalOperationId = `stop-${sessionId}`,
  } = {},
) {
  const launchAttemptId = `launch-${sessionId}`;
  const request = launchRequest(sessionId);
  const terminalRecord = {
    containerId: "abcdef012345",
    containerName: "portable-codex-writer",
    contractVersion: 1,
    launchAttemptId,
    processIncarnationId: `process-${sessionId}`,
    proofId: `proof-${sessionId}`,
    requestSha256: podmanWriterRequestSha256(request),
    revision: 4,
    status: "stopped",
    stopOperationId: terminalOperationId,
    stopProofId: `stop-proof-${sessionId}`,
    writerIncarnationId: `writer-${sessionId}`,
  };
  return {
    authorization: {
      authorizationSha256: "a".repeat(64),
      authorizedAt,
      contractVersion: 2,
      launchAttemptId,
      sessionId,
      stateOwnerId: STATE_OWNER_ID,
      terminalKind: "writer-launch-stop-v1",
      terminalOperationId,
      terminalRecord,
      terminalRecordSha256: "b".repeat(64),
    },
    launchOperation: supervisorStateGcOperation({
      kind: "writer-launch-attempt-v1",
      operationId: launchAttemptId,
      request,
      sessionId,
    }),
    session: supervisorStateGcSession(sessionId),
    terminalOperation: supervisorStateGcOperation({
      kind: "writer-launch-stop-v1",
      operationId: terminalOperationId,
      sessionId,
    }),
  };
}

function page(candidates, nextAfterSessionId = null) {
  return { candidates, nextAfterSessionId };
}

function gcPage(candidates, nextCandidate = null) {
  return {
    candidates,
    nextAfterAuthorizedAt:
      nextCandidate === null ? null : nextCandidate.authorization.authorizedAt,
    nextAfterSessionId:
      nextCandidate === null ? null : nextCandidate.authorization.sessionId,
    nextAfterTerminalOperationId:
      nextCandidate === null
        ? null
        : nextCandidate.authorization.terminalOperationId,
  };
}

function callbacks(overrides = {}) {
  const calls = [];
  return {
    calls,
    options: {
      async listCurrentWriterLaunchCandidates(request) {
        calls.push(["list-current", request]);
        return page([currentLaunchCandidate()]);
      },
      async listRestoreAttachmentActivationCandidates(request) {
        calls.push(["list-activation", request]);
        return page([activationCandidate()]);
      },
      async listRestoreGenerationCandidates(request) {
        calls.push(["list-generation", request]);
        return page([generationCandidate()]);
      },
      async listWriterLaunchAttemptCandidates(request) {
        calls.push(["list-launch", request]);
        return page([launchCandidate()]);
      },
      async listWriterSupervisorStateGcCandidates(request) {
        calls.push(["list-supervisor-state-gc", request]);
        return gcPage([supervisorStateGcCandidate()]);
      },
      async collectWriterSupervisorStateGc(candidate) {
        calls.push(["collect-supervisor-state-gc", candidate]);
      },
      async reconcileRestoreAttachmentActivation(candidate) {
        calls.push(["reconcile-activation", candidate]);
      },
      async reconcileRestoreGeneration(candidate) {
        calls.push(["reconcile-generation", candidate]);
      },
      async reconcileWriterLaunchAttempt(candidate) {
        calls.push(["reconcile-launch", candidate]);
      },
      ...overrides,
    },
  };
}

function request(overrides = {}) {
  return {
    afterSessionId: null,
    limit: 10,
    signal: null,
    ...overrides,
  };
}

function gcRequest(overrides = {}) {
  return {
    afterAuthorizedAt: null,
    afterSessionId: null,
    afterTerminalOperationId: null,
    limit: 10,
    signal: null,
    ...overrides,
  };
}

function lane(afterSessionId = null, limit = 10) {
  return { afterSessionId, limit };
}

function gcLane(afterSessionId = null, limit = 10) {
  return {
    afterAuthorizedAt:
      afterSessionId === null ? null : "2026-08-05T00:06:00.000Z",
    afterSessionId,
    afterTerminalOperationId:
      afterSessionId === null ? null : `stop-${afterSessionId}`,
    limit,
  };
}

function consumeGcReceipt(
  service,
  afterSessionId,
  limit,
  lifecycleLeaseOrReceipt,
  guardedReceipt,
) {
  const cursor = gcLane(afterSessionId, limit);
  return arguments.length === 5
    ? consumePostgresRestoreActivationRecoveryBatchReceipt(
        service,
        "supervisorStateGc",
        cursor.afterSessionId,
        cursor.afterAuthorizedAt,
        cursor.afterTerminalOperationId,
        cursor.limit,
        lifecycleLeaseOrReceipt,
        guardedReceipt,
      )
    : consumePostgresRestoreActivationRecoveryBatchReceipt(
        service,
        "supervisorStateGc",
        cursor.afterSessionId,
        cursor.afterAuthorizedAt,
        cursor.afterTerminalOperationId,
        cursor.limit,
        lifecycleLeaseOrReceipt,
      );
}

function assertDeepFrozen(value, seen = new Set()) {
  if (value === null || typeof value !== "object" || seen.has(value)) return;
  seen.add(value);
  assert.equal(Object.isFrozen(value), true);
  for (const child of Object.values(value)) assertDeepFrozen(child, seen);
}

function assertCode(code) {
  return (error) =>
    error instanceof PostgresRestoreActivationRecoveryServiceError &&
    error.code === code;
}

function plainBatch(value) {
  return {
    ...value,
    results: value.results.map((entry) => ({ ...entry })),
  };
}

function definePassThroughArrayElement(receiver, key, value) {
  Object.defineProperty(receiver, key, {
    configurable: true,
    enumerable: true,
    value,
    writable: true,
  });
}

function restoreOwnPropertyDescriptor(target, key, descriptor) {
  if (descriptor === undefined) {
    if (!Reflect.deleteProperty(target, key)) {
      throw new Error("Failed to restore polluted prototype property");
    }
    return;
  }
  Object.defineProperty(target, key, descriptor);
}

function createNumericPrototypeTrap(shouldTrap, injectedValues) {
  const entries = [
    {
      descriptor: Object.getOwnPropertyDescriptor(Array.prototype, "0"),
      injected: injectedValues[0],
      key: "0",
      minimumLength: 1,
      prototype: Array.prototype,
      receivers: new WeakSet(),
    },
    {
      descriptor: Object.getOwnPropertyDescriptor(Object.prototype, "1"),
      injected: injectedValues[1],
      key: "1",
      minimumLength: 2,
      prototype: Object.prototype,
      receivers: new WeakSet(),
    },
  ];
  const calls = { get: 0, set: 0 };

  return {
    calls,
    install() {
      for (let index = 0; index < entries.length; index += 1) {
        const entry = entries[index];
        Object.defineProperty(entry.prototype, entry.key, {
          configurable: true,
          get() {
            if (entry.receivers.has(this)) {
              calls.get += 1;
              return entry.injected;
            }
            return undefined;
          },
          set(value) {
            if (Array.isArray(this) && shouldTrap(value)) {
              calls.set += 1;
              entry.receivers.add(this);
              if (this.length < entry.minimumLength) {
                this.length = entry.minimumLength;
              }
              return;
            }
            definePassThroughArrayElement(this, entry.key, value);
          },
        });
      }
    },
    restore() {
      let restorationError;
      for (let index = entries.length - 1; index >= 0; index -= 1) {
        const entry = entries[index];
        try {
          restoreOwnPropertyDescriptor(
            entry.prototype,
            entry.key,
            entry.descriptor,
          );
        } catch (error) {
          restorationError ??= error;
        }
      }
      if (restorationError !== undefined) throw restorationError;
    },
  };
}

test("runs five bounded recovery lanes without treating current launches as adoptable", async () => {
  const fixture = callbacks({
    async reconcileRestoreAttachmentActivation(candidate) {
      fixture.calls.push(["reconcile-activation", candidate]);
      throw new Error("provider remains uncertain");
    },
  });
  const service = createPostgresRestoreActivationRecoveryService(
    fixture.options,
  );

  const generation = await service.runGenerationBatch(request());
  const activation = await service.runActivationBatch(request());
  const launch = await service.runLaunchAttemptBatch(request());
  const current = await service.scanCurrentLaunchBatch(request());
  const supervisorStateGc = await service.runSupervisorStateGcBatch(
    gcRequest(),
  );

  assert.deepEqual(plainBatch(generation), {
    afterSessionId: null,
    nextAfterSessionId: null,
    results: [
      {
        operationId: `restore-${SESSION_ID}`,
        sessionId: SESSION_ID,
        status: "reconciled",
      },
    ],
    status: "sweep-complete",
  });
  assert.deepEqual(plainBatch(supervisorStateGc), {
    afterAuthorizedAt: null,
    afterSessionId: null,
    afterTerminalOperationId: null,
    nextAfterAuthorizedAt: null,
    nextAfterSessionId: null,
    nextAfterTerminalOperationId: null,
    results: [
      {
        authorizedAt: "2026-08-05T00:06:00.000Z",
        operationId: `stop-${SESSION_ID}`,
        sessionId: SESSION_ID,
        status: "reconciled",
      },
    ],
    status: "sweep-complete",
  });
  assert.equal(activation.results[0].status, "pending");
  assert.equal(launch.results[0].status, "reconciled");
  assert.deepEqual(current.results.map((entry) => ({ ...entry })), [
    {
      operationId: `launch-${SESSION_ID}`,
      sessionId: SESSION_ID,
      status: "requires-stop-or-fence",
    },
  ]);
  assert.equal(
    fixture.calls.some(([kind]) => kind === "reconcile-current"),
    false,
  );
  assertDeepFrozen(generation);
  assertDeepFrozen(activation);
  assertDeepFrozen(launch);
  assertDeepFrozen(current);
  assertDeepFrozen(supervisorStateGc);
  assert.deepEqual(Reflect.ownKeys(service), [
    "runActivationBatch",
    "runGenerationBatch",
    "runLaunchAttemptBatch",
    "runSupervisorStateGcBatch",
    "runSweep",
    "scanCurrentLaunchBatch",
  ]);
  assert.equal(
    consumePostgresRestoreActivationRecoveryBatchReceipt.length,
    6,
  );
  assertDeepFrozen(service);
});

test("supervisor-state GC candidates require an exact persistent state owner", async () => {
  for (const mutate of [
    (candidate) => {
      delete candidate.authorization.stateOwnerId;
    },
    (candidate) => {
      candidate.authorization.stateOwnerId = "runtime-recovery-001";
    },
    (candidate) => {
      candidate.authorization.contractVersion = 1;
    },
  ]) {
    const candidate = supervisorStateGcCandidate();
    mutate(candidate);
    let collections = 0;
    const fixture = callbacks({
      async listWriterSupervisorStateGcCandidates() {
        return gcPage([candidate]);
      },
      async collectWriterSupervisorStateGc() {
        collections += 1;
      },
    });
    const service = createPostgresRestoreActivationRecoveryService(
      fixture.options,
    );
    await assert.rejects(
      service.runSupervisorStateGcBatch(gcRequest()),
      assertCode(
        "postgres_restore_activation_recovery_service_outcome_uncertain",
      ),
    );
    assert.equal(collections, 0);
  }
});

test("supervisor-state GC advances past pending and retries after wrap", async () => {
  const first = supervisorStateGcCandidate(SESSION_ID, {
    terminalOperationId: "stop-same-session-001",
  });
  const second = supervisorStateGcCandidate(SESSION_ID, {
    terminalOperationId: "stop-same-session-002",
  });
  const attempts = [];
  const fixture = callbacks({
    async listWriterSupervisorStateGcCandidates(input) {
      if (input.afterSessionId === null) {
        return gcPage([first], first);
      }
      assert.deepEqual({ ...input }, {
        afterAuthorizedAt: first.authorization.authorizedAt,
        afterSessionId: SESSION_ID,
        afterTerminalOperationId:
          first.authorization.terminalOperationId,
        limit: 1,
      });
      return gcPage([second]);
    },
    async collectWriterSupervisorStateGc(candidate) {
      attempts.push(candidate.authorization.terminalOperationId);
      if (
        candidate.authorization.terminalOperationId ===
        first.authorization.terminalOperationId
      ) {
        throw new Error("permanent collection conflict");
      }
    },
  });
  const service = createPostgresRestoreActivationRecoveryService(
    fixture.options,
  );

  const firstBatch = await service.runSupervisorStateGcBatch(
    gcRequest({ limit: 1 }),
  );
  const secondBatch = await service.runSupervisorStateGcBatch(
    gcRequest({
      afterAuthorizedAt: firstBatch.nextAfterAuthorizedAt,
      afterSessionId: firstBatch.nextAfterSessionId,
      afterTerminalOperationId: firstBatch.nextAfterTerminalOperationId,
      limit: 1,
    }),
  );
  const wrappedBatch = await service.runSupervisorStateGcBatch(
    gcRequest({ limit: 1 }),
  );

  assert.deepEqual(
    firstBatch.results.map(({ operationId, status }) => ({
      operationId,
      status,
    })),
    [{ operationId: "stop-same-session-001", status: "pending" }],
  );
  assert.equal(firstBatch.status, "limit-reached");
  assert.equal(firstBatch.nextAfterSessionId, SESSION_ID);
  assert.equal(secondBatch.results[0].operationId, "stop-same-session-002");
  assert.equal(secondBatch.results[0].status, "reconciled");
  assert.equal(secondBatch.status, "sweep-complete");
  assert.equal(secondBatch.nextAfterSessionId, null);
  assert.equal(wrappedBatch.results[0].status, "pending");
  assert.deepEqual(attempts, [
    "stop-same-session-001",
    "stop-same-session-002",
    "stop-same-session-001",
  ]);
});

test(
  "list-time numeric prototype accessors cannot inject or erase candidates",
  { concurrency: false },
  async () => {
    const sourcePage = page([
      generationCandidate(SESSION_ID),
      generationCandidate(OTHER_SESSION_ID),
    ]);
    const injectedFirst = generationCandidate(SESSION_ID);
    const injectedSecond = generationCandidate(OTHER_SESSION_ID);
    injectedFirst.request.operationId = `injected-${SESSION_ID}`;
    injectedSecond.request.operationId = `injected-${OTHER_SESSION_ID}`;
    const prototypeTrap = createNumericPrototypeTrap(
      (value) =>
        value !== null &&
        typeof value === "object" &&
        (Object.hasOwn(value, "checkpoint") ||
          Object.hasOwn(value, "candidate")),
      [injectedFirst, injectedSecond],
    );
    let reconciled = 0;

    const fixture = callbacks({
      listRestoreGenerationCandidates() {
        prototypeTrap.install();
        return sourcePage;
      },
      reconcileRestoreGeneration() {
        reconciled += 1;
      },
    });
    const service = createPostgresRestoreActivationRecoveryService(
      fixture.options,
    );
    let result;
    let runError;

    try {
      result = await service.runGenerationBatch(request());
    } catch (error) {
      runError = error;
    } finally {
      prototypeTrap.restore();
    }

    assert.ifError(runError);
    assert.deepEqual(prototypeTrap.calls, { get: 0, set: 0 });
    assert.equal(reconciled, 2);
    assert.deepEqual(
      result.results.map((entry) => entry.operationId),
      [`restore-${SESSION_ID}`, `restore-${OTHER_SESSION_ID}`],
    );
  },
);

test(
  "reconcile-time numeric accessors and inherited non-writable entries preserve results",
  { concurrency: false },
  async () => {
    const candidates = Array.from({ length: 100 }, (_, index) =>
      generationCandidate(
        `019f2100-0000-7000-8000-${String(index + 1).padStart(12, "0")}`,
      ),
    );
    const sourcePage = page(candidates);
    const injectedFirst = {
      operationId: `injected-${SESSION_ID}`,
      sessionId: SESSION_ID,
      status: "pending",
    };
    const injectedSecond = {
      operationId: `injected-${OTHER_SESSION_ID}`,
      sessionId: OTHER_SESSION_ID,
      status: "pending",
    };
    const prototypeTrap = createNumericPrototypeTrap(
      (value) =>
        value !== null &&
        typeof value === "object" &&
        Object.hasOwn(value, "status") &&
        value !== injectedFirst &&
        value !== injectedSecond,
      [injectedFirst, injectedSecond],
    );
    const nonWritableDescriptor = Object.getOwnPropertyDescriptor(
      Array.prototype,
      "99",
    );
    let installed = false;
    let reconciled = 0;
    let lastReconciledCandidate;

    const fixture = callbacks({
      listRestoreGenerationCandidates() {
        Object.defineProperty(Array.prototype, "99", {
          configurable: true,
          enumerable: false,
          value: "inherited-non-writable",
          writable: false,
        });
        return sourcePage;
      },
      reconcileRestoreGeneration(candidate) {
        reconciled += 1;
        lastReconciledCandidate = candidate;
        if (!installed) {
          installed = true;
          prototypeTrap.install();
        }
      },
    });
    const service = createPostgresRestoreActivationRecoveryService(
      fixture.options,
    );
    let result;
    let runError;

    try {
      result = await service.runGenerationBatch(request({ limit: 100 }));
    } catch (error) {
      runError = error;
    } finally {
      try {
        prototypeTrap.restore();
      } finally {
        restoreOwnPropertyDescriptor(
          Array.prototype,
          "99",
          nonWritableDescriptor,
        );
      }
    }

    assert.ifError(runError);
    assert.deepEqual(prototypeTrap.calls, { get: 0, set: 0 });
    assert.equal(reconciled, 100);
    assert.equal(Object.getPrototypeOf(result.results), Array.prototype);
    assert.equal(result.results.length, 100);
    assert.equal(Reflect.ownKeys(result.results).length, 101);
    assert.deepEqual({ ...result.results[0] }, {
      operationId: `restore-${SESSION_ID}`,
      sessionId: SESSION_ID,
      status: "reconciled",
    });
    assert.deepEqual({ ...result.results[99] }, {
      operationId:
        "restore-019f2100-0000-7000-8000-000000000100",
      sessionId: "019f2100-0000-7000-8000-000000000100",
      status: "reconciled",
    });
    assert.equal(
      lastReconciledCandidate.checkpoint.imageDigest,
      IMAGE_DIGEST,
    );
    assert.equal(result.afterSessionId, null);
    assert.equal(result.nextAfterSessionId, null);
    assert.equal(result.status, "sweep-complete");
  },
);

test("guarded sweep probes around every list and candidate action", async () => {
  const events = [];
  const lifecycleFixture = createLifecycleFixture({ events });
  const fixture = callbacks({
    async listCurrentWriterLaunchCandidates(input) {
      events.push("list:currentLaunch");
      assert.deepEqual(Reflect.ownKeys(input), [
        "afterSessionId",
        "limit",
      ]);
      return page([currentLaunchCandidate()]);
    },
    async listRestoreAttachmentActivationCandidates() {
      events.push("list:activation");
      return page([activationCandidate()]);
    },
    async listRestoreGenerationCandidates() {
      events.push("list:generation");
      return page([generationCandidate()]);
    },
    async listWriterLaunchAttemptCandidates() {
      events.push("list:launchAttempt");
      return page([launchCandidate()]);
    },
    async listWriterSupervisorStateGcCandidates() {
      events.push("list:supervisorStateGc");
      return gcPage([supervisorStateGcCandidate()]);
    },
    async collectWriterSupervisorStateGc() {
      events.push("collect:supervisorStateGc");
    },
    async reconcileRestoreAttachmentActivation() {
      events.push("reconcile:activation");
    },
    async reconcileRestoreGeneration() {
      events.push("reconcile:generation");
    },
    async reconcileWriterLaunchAttempt() {
      events.push("reconcile:launchAttempt");
    },
  });
  const service = createPostgresRestoreActivationRecoveryService(
    fixture.options,
  );

  await lifecycleFixture.guard.runRecovery(async (
    lifecycleLease,
    complete,
  ) => {
    const result = await service.runSweep({
      activation: lane(),
      currentLaunch: lane(),
      generation: lane(),
      launchAttempt: lane(),
      lifecycleLease,
      signal: null,
      supervisorStateGc: gcLane(),
    });
    for (const laneReceipt of [
      ["generation", result.generation],
      ["activation", result.activation],
      ["launchAttempt", result.launchAttempt],
      ["currentLaunch", result.currentLaunch],
      ["supervisorStateGc", result.supervisorStateGc],
    ]) {
      assert.equal(
        laneReceipt[0] === "supervisorStateGc"
          ? consumeGcReceipt(
              service,
              null,
              10,
              lifecycleLease,
              laneReceipt[1],
            )
          : consumePostgresRestoreActivationRecoveryBatchReceipt(
              service,
              laneReceipt[0],
              null,
              10,
              lifecycleLease,
              laneReceipt[1],
            ),
        true,
      );
    }
    return complete(undefined);
  });

  assert.deepEqual(events, [
    "probe",
    "probe",
    "list:generation",
    "probe",
    "probe",
    "reconcile:generation",
    "probe",
    "probe",
    "list:activation",
    "probe",
    "probe",
    "reconcile:activation",
    "probe",
    "probe",
    "list:launchAttempt",
    "probe",
    "probe",
    "reconcile:launchAttempt",
    "probe",
    "probe",
    "list:currentLaunch",
    "probe",
    "probe",
    "probe",
    "probe",
    "list:supervisorStateGc",
    "probe",
    "probe",
    "collect:supervisorStateGc",
    "probe",
    "probe",
  ]);
  assert.equal(lifecycleFixture.manager.holder, null);
});

test(
  "post-import targeted Array iterator pollution cannot rewrite guarded calls",
  { concurrency: false },
  async () => {
    const iteratorDescriptor = Object.getOwnPropertyDescriptor(
      Array.prototype,
      Symbol.iterator,
    );
    assert.equal(typeof iteratorDescriptor?.value, "function");
    const originalIterator = iteratorDescriptor.value;
    const events = [];
    let originalListCalls = 0;
    let originalReconcileCalls = 0;
    let forgedListCalls = 0;
    let forgedReconcileCalls = 0;
    let reconciledSessionId = null;
    let activeLease = null;
    let receipt;
    let serviceError;
    let lifecycleError;
    const targetedIteratorCalls = {
      assertLease: 0,
      callList: 0,
      reconcile: 0,
      runLane: 0,
    };
    const lifecycleFixture = createLifecycleFixture({
      events,
      loseWhen() {
        return originalReconcileCalls === 1 || forgedReconcileCalls === 1;
      },
    });
    const forgedCandidate = generationCandidate(OTHER_SESSION_ID);
    const forgedList = () => {
      forgedListCalls += 1;
      events.push("list:forged");
      return page([forgedCandidate]);
    };
    const forgedReconcile = (candidate) => {
      forgedReconcileCalls += 1;
      events.push(`reconcile:forged:${candidate.request.sessionId}`);
    };
    const fixture = callbacks({
      listRestoreGenerationCandidates(input) {
        originalListCalls += 1;
        events.push("list:original");
        assert.deepEqual(Reflect.ownKeys(input), [
          "afterSessionId",
          "limit",
        ]);
        return page([generationCandidate(SESSION_ID)]);
      },
      reconcileRestoreGeneration(candidate) {
        originalReconcileCalls += 1;
        reconciledSessionId = candidate.request.sessionId;
        events.push(`reconcile:original:${reconciledSessionId}`);
      },
    });
    const originalList = fixture.options.listRestoreGenerationCandidates;
    const originalReconcile = fixture.options.reconcileRestoreGeneration;
    let service;

    function iteratorFor(values) {
      return Reflect.apply(originalIterator, values, []);
    }

    function targetedIterator() {
      const outcomeCode =
        "postgres_restore_activation_recovery_service_outcome_uncertain";
      if (
        this.length === 2 &&
        this[0] === activeLease &&
        this[1] === outcomeCode
      ) {
        targetedIteratorCalls.assertLease += 1;
        return iteratorFor([null, outcomeCode]);
      }
      if (
        this.length === 2 &&
        this[0] === "generation" &&
        this[1]?.lifecycleLease === activeLease
      ) {
        targetedIteratorCalls.runLane += 1;
        return iteratorFor([this[0], this[1]]);
      }
      if (
        this.length === 4 &&
        this[0] === originalList &&
        this[1]?.lifecycleLease === activeLease &&
        this[2] === "generation" &&
        this[3] === outcomeCode
      ) {
        targetedIteratorCalls.callList += 1;
        return iteratorFor([forgedList, this[1], this[2], this[3]]);
      }
      if (
        this.length === 3 &&
        this[0] === originalReconcile &&
        this[2] === outcomeCode
      ) {
        targetedIteratorCalls.reconcile += 1;
        return iteratorFor([forgedReconcile, forgedCandidate, this[2]]);
      }
      return Reflect.apply(originalIterator, this, []);
    }

    try {
      Object.defineProperty(Array.prototype, Symbol.iterator, {
        ...iteratorDescriptor,
        value: targetedIterator,
      });
      service = createPostgresRestoreActivationRecoveryService(
        fixture.options,
      );
      try {
        await lifecycleFixture.guard.runRecovery(async (
          lifecycleLease,
          complete,
        ) => {
          activeLease = lifecycleLease;
          try {
            receipt = await service.runGenerationBatch(
              request({ lifecycleLease }),
            );
          } catch (error) {
            serviceError = error;
            throw error;
          }
          return complete(undefined);
        });
      } catch (error) {
        lifecycleError = error;
      }
    } finally {
      restoreOwnPropertyDescriptor(
        Array.prototype,
        Symbol.iterator,
        iteratorDescriptor,
      );
    }

    assert.deepEqual(
      Object.getOwnPropertyDescriptor(Array.prototype, Symbol.iterator),
      iteratorDescriptor,
    );
    assert.deepEqual(targetedIteratorCalls, {
      assertLease: 0,
      callList: 0,
      reconcile: 0,
      runLane: 0,
    });
    assert.equal(originalListCalls, 1);
    assert.equal(originalReconcileCalls, 1);
    assert.equal(forgedListCalls, 0);
    assert.equal(forgedReconcileCalls, 0);
    assert.equal(reconciledSessionId, SESSION_ID);
    assert.deepEqual(events, [
      "probe",
      "probe",
      "list:original",
      "probe",
      "probe",
      `reconcile:original:${SESSION_ID}`,
      "probe",
      "probe",
    ]);
    assert.equal(
      assertCode(
        "postgres_restore_activation_recovery_service_outcome_uncertain",
      )(serviceError),
      true,
    );
    assert.equal(
      lifecycleError instanceof PostgresRestoreLifecycleGuardError &&
        lifecycleError.code ===
          "postgres_restore_lifecycle_guard_outcome_uncertain",
      true,
    );
    assert.equal(receipt, undefined);
    assert.equal(
      consumePostgresRestoreActivationRecoveryBatchReceipt(
        service,
        "generation",
        null,
        10,
        activeLease,
        receipt,
      ),
      false,
    );
    assert.equal(lifecycleFixture.manager.holder, null);
  },
);

test("guarded receipts reject legacy arity, cross-lease use, and replay", async () => {
  const lifecycleFixture = createLifecycleFixture();
  const otherLifecycleFixture = createLifecycleFixture();
  const service = createPostgresRestoreActivationRecoveryService(
    callbacks().options,
  );
  let firstLease;
  let staleReceipt;

  await lifecycleFixture.guard.runRecovery(async (
    lifecycleLease,
    complete,
  ) => {
    firstLease = lifecycleLease;
    const crossLeaseReceipt = await service.runGenerationBatch(
      request({ lifecycleLease }),
    );
    await otherLifecycleFixture.guard.runRecovery(async (
      otherLease,
      otherComplete,
    ) => {
      assert.notEqual(otherLease, lifecycleLease);
      assert.equal(
        consumePostgresRestoreActivationRecoveryBatchReceipt(
          service,
          "generation",
          null,
          10,
          otherLease,
          crossLeaseReceipt,
        ),
        false,
      );
      return otherComplete(undefined);
    });
    assert.equal(
      consumePostgresRestoreActivationRecoveryBatchReceipt(
        service,
        "generation",
        null,
        10,
        lifecycleLease,
        crossLeaseReceipt,
      ),
      true,
    );
    assert.equal(
      consumePostgresRestoreActivationRecoveryBatchReceipt(
        service,
        "generation",
        null,
        10,
        lifecycleLease,
        crossLeaseReceipt,
      ),
      false,
    );
    staleReceipt = await service.runGenerationBatch(
      request({ lifecycleLease }),
    );
    const arityReceipt = await service.runGenerationBatch(
      request({ lifecycleLease }),
    );
    assert.equal(
      consumePostgresRestoreActivationRecoveryBatchReceipt(
        service,
        "generation",
        null,
        10,
        arityReceipt,
      ),
      false,
    );
    assert.equal(
      consumePostgresRestoreActivationRecoveryBatchReceipt(
        service,
        "generation",
        null,
        10,
        lifecycleLease,
        arityReceipt,
      ),
      true,
    );
    return complete(undefined);
  });

  await lifecycleFixture.guard.runRecovery(async (
    lifecycleLease,
    complete,
  ) => {
    assert.notEqual(lifecycleLease, firstLease);
    assert.equal(
      consumePostgresRestoreActivationRecoveryBatchReceipt(
        service,
        "generation",
        null,
        10,
        lifecycleLease,
        staleReceipt,
      ),
      false,
    );
    const currentReceipt = await service.runGenerationBatch(
      request({ lifecycleLease }),
    );
    assert.equal(
      consumePostgresRestoreActivationRecoveryBatchReceipt(
        service,
        "generation",
        null,
        10,
        firstLease,
        currentReceipt,
      ),
      false,
    );
    assert.equal(
      consumePostgresRestoreActivationRecoveryBatchReceipt(
        service,
        "generation",
        null,
        10,
        lifecycleLease,
        currentReceipt,
      ),
      true,
    );
    assert.equal(
      consumePostgresRestoreActivationRecoveryBatchReceipt(
        service,
        "generation",
        null,
        10,
        lifecycleLease,
        currentReceipt,
      ),
      false,
    );
    return complete(undefined);
  });
});

test("lease loss after a reconcile prevents later candidates and receipts", async () => {
  let reconcileCalls = 0;
  let lost = false;
  const lifecycleFixture = createLifecycleFixture({
    loseWhen() {
      if (!lost && reconcileCalls === 1) {
        lost = true;
        return true;
      }
      return false;
    },
  });
  const fixture = callbacks({
    async listRestoreGenerationCandidates() {
      return page([
        generationCandidate(SESSION_ID),
        generationCandidate(OTHER_SESSION_ID),
      ]);
    },
    async reconcileRestoreGeneration() {
      reconcileCalls += 1;
    },
  });
  const service = createPostgresRestoreActivationRecoveryService(
    fixture.options,
  );
  let receipt;
  let serviceError;

  await assert.rejects(
    lifecycleFixture.guard.runRecovery(async (lifecycleLease) => {
      try {
        receipt = await service.runGenerationBatch(
          request({ lifecycleLease }),
        );
      } catch (error) {
        serviceError = error;
        throw error;
      }
    }),
    (error) =>
      error instanceof PostgresRestoreLifecycleGuardError &&
      error.code ===
        "postgres_restore_lifecycle_guard_outcome_uncertain",
  );
  assert.equal(
    assertCode(
      "postgres_restore_activation_recovery_service_outcome_uncertain",
    )(serviceError),
    true,
  );
  assert.equal(lost, true);
  assert.equal(reconcileCalls, 1);
  assert.equal(receipt, undefined);
  assert.equal(lifecycleFixture.manager.holder, null);
});

test("guarded receipt is minted only after reconciliation drains", async () => {
  const entered = (() => {
    let resolve;
    const promise = new Promise((resolvePromise) => {
      resolve = resolvePromise;
    });
    return { promise, resolve };
  })();
  const finish = (() => {
    let resolve;
    const promise = new Promise((resolvePromise) => {
      resolve = resolvePromise;
    });
    return { promise, resolve };
  })();
  let reconcilerExited = false;
  let receipt;
  const lifecycleFixture = createLifecycleFixture();
  const fixture = callbacks({
    async listRestoreGenerationCandidates() {
      return page([generationCandidate()]);
    },
    async reconcileRestoreGeneration() {
      entered.resolve();
      await finish.promise;
      reconcilerExited = true;
    },
  });
  const service = createPostgresRestoreActivationRecoveryService(
    fixture.options,
  );

  await lifecycleFixture.guard.runRecovery(async (
    lifecycleLease,
    complete,
  ) => {
    const pending = service.runGenerationBatch(
      request({ lifecycleLease }),
    );
    await entered.promise;
    assert.equal(reconcilerExited, false);
    assert.equal(receipt, undefined);

    finish.resolve();
    receipt = await pending;
    assert.equal(reconcilerExited, true);
    assert.equal(
      consumePostgresRestoreActivationRecoveryBatchReceipt(
        service,
        "generation",
        null,
        10,
        lifecycleLease,
        receipt,
      ),
      true,
    );
    return complete(undefined);
  });
});

test(
  "callback-time Promise prototype pollution cannot settle a batch early",
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
    let finishReconciliation;
    const reconciliation = new Promise((resolve) => {
      finishReconciliation = resolve;
    });
    let finishRestoration;
    const restoration = new Promise((resolve) => {
      finishRestoration = resolve;
    });
    Object.defineProperty(restoration, "constructor", {
      configurable: false,
      enumerable: false,
      value: Promise,
      writable: false,
    });
    let poisonedThenCalls = 0;
    let restored = false;
    const restore = () => {
      if (restored) return;
      restored = true;
      Object.defineProperty(
        Promise.prototype,
        "constructor",
        constructorDescriptor,
      );
      Object.defineProperty(Promise.prototype, "then", thenDescriptor);
      finishRestoration();
    };
    const fixture = callbacks({
      listRestoreGenerationCandidates() {
        return page([generationCandidate()]);
      },
      reconcileRestoreGeneration() {
        Object.defineProperty(Promise.prototype, "constructor", {
          ...constructorDescriptor,
          value: speciesHolder,
        });
        Object.defineProperty(Promise.prototype, "then", {
          ...thenDescriptor,
          value(onFulfilled) {
            poisonedThenCalls += 1;
            if (typeof onFulfilled === "function") {
              onFulfilled("reconciled");
            }
          },
        });
        queueMicrotask(restore);
        return reconciliation;
      },
    });
    const service = createPostgresRestoreActivationRecoveryService(
      fixture.options,
    );
    let settled = false;
    let pending;
    try {
      pending = service.runGenerationBatch(request());
      pending.then(
        () => {
          settled = true;
        },
        () => {
          settled = true;
        },
      );
      await restoration;
    } finally {
      restore();
    }
    assert.equal(poisonedThenCalls, 0);
    assert.equal(settled, false);

    finishReconciliation();
    const result = await pending;
    assert.equal(result.status, "sweep-complete");
    assert.equal(result.results[0].status, "reconciled");
  },
);

test("guarded requests reject fake leases without reading their properties", async () => {
  const fixture = callbacks();
  const service = createPostgresRestoreActivationRecoveryService(
    fixture.options,
  );
  let traps = 0;
  const fakeLease = new Proxy(Object.freeze({ mode: "recovery" }), {
    get() {
      traps += 1;
      throw new Error("lease properties must not be read");
    },
    getOwnPropertyDescriptor() {
      traps += 1;
      throw new Error("lease descriptors must not be read");
    },
    getPrototypeOf() {
      traps += 1;
      throw new Error("lease prototype must not be read");
    },
    ownKeys() {
      traps += 1;
      throw new Error("lease keys must not be read");
    },
  });

  await assert.rejects(
    service.runGenerationBatch(request({ lifecycleLease: fakeLease })),
    assertCode(
      "invalid_postgres_restore_activation_recovery_service_request",
    ),
  );
  await assert.rejects(
    service.runSweep({
      activation: lane(),
      currentLaunch: lane(),
      generation: lane(),
      launchAttempt: lane(),
      lifecycleLease: fakeLease,
      signal: null,
      supervisorStateGc: gcLane(),
    }),
    assertCode(
      "invalid_postgres_restore_activation_recovery_service_request",
    ),
  );
  assert.equal(fixture.calls.length, 0);
  assert.equal(traps, 0);
});

test("brands only exact recovery service instances without invoking Proxy traps", () => {
  const service = createPostgresRestoreActivationRecoveryService(
    callbacks().options,
  );
  const clone = Object.freeze(Object.assign(Object.create(null), service));
  let traps = 0;
  const proxy = new Proxy(service, {
    get() {
      traps += 1;
      throw new Error("must not read service properties");
    },
    getOwnPropertyDescriptor() {
      traps += 1;
      throw new Error("must not inspect service descriptors");
    },
    getPrototypeOf() {
      traps += 1;
      throw new Error("must not inspect the service prototype");
    },
    ownKeys() {
      traps += 1;
      throw new Error("must not enumerate service keys");
    },
  });
  const revoked = Proxy.revocable(service, {});
  revoked.revoke();

  assert.equal(isPostgresRestoreActivationRecoveryService(service), true);
  assert.equal(isPostgresRestoreActivationRecoveryService(clone), false);
  assert.equal(isPostgresRestoreActivationRecoveryService(proxy), false);
  assert.equal(
    isPostgresRestoreActivationRecoveryService(revoked.proxy),
    false,
  );
  assert.equal(isPostgresRestoreActivationRecoveryService(), false);
  assert.equal(
    isPostgresRestoreActivationRecoveryService(service, "extra"),
    false,
  );
  assert.equal(traps, 0);
});

test("authentic sweep-complete receipts bind issuer, lane, input, and identity once", async () => {
  const service = createPostgresRestoreActivationRecoveryService(
    callbacks().options,
  );
  const otherService = createPostgresRestoreActivationRecoveryService(
    callbacks().options,
  );
  const receipt = await service.runGenerationBatch(request());
  const clone = Object.freeze(Object.assign(Object.create(null), receipt));

  assert.equal(receipt.status, "sweep-complete");
  assert.equal(
    consumePostgresRestoreActivationRecoveryBatchReceipt(
      service,
      "generation",
      null,
      10,
      clone,
    ),
    false,
  );
  assert.equal(
    consumePostgresRestoreActivationRecoveryBatchReceipt(
      otherService,
      "generation",
      null,
      10,
      receipt,
    ),
    false,
  );
  assert.equal(
    consumePostgresRestoreActivationRecoveryBatchReceipt(
      service,
      "activation",
      null,
      10,
      receipt,
    ),
    false,
  );
  assert.equal(
    consumePostgresRestoreActivationRecoveryBatchReceipt(
      service,
      "generation",
      OTHER_SESSION_ID,
      10,
      receipt,
    ),
    false,
  );
  assert.equal(
    consumePostgresRestoreActivationRecoveryBatchReceipt(
      service,
      "generation",
      null,
      9,
      receipt,
    ),
    false,
  );
  assert.equal(
    consumePostgresRestoreActivationRecoveryBatchReceipt(
      service,
      "generation",
      null,
      10,
      receipt,
    ),
    true,
  );
  assert.equal(
    consumePostgresRestoreActivationRecoveryBatchReceipt(
      service,
      "generation",
      null,
      10,
      receipt,
    ),
    false,
  );
});

test("supervisor-state GC receipts bind the complete input position", async () => {
  const fixture = callbacks({
    async listWriterSupervisorStateGcCandidates() {
      return gcPage([]);
    },
  });
  const service = createPostgresRestoreActivationRecoveryService(
    fixture.options,
  );
  const position = gcLane(SESSION_ID, 10);
  const receipt = await service.runSupervisorStateGcBatch(
    gcRequest(position),
  );

  assert.equal(
    consumePostgresRestoreActivationRecoveryBatchReceipt(
      service,
      "supervisorStateGc",
      position.afterSessionId,
      "2026-08-05T00:06:01.000Z",
      position.afterTerminalOperationId,
      position.limit,
      receipt,
    ),
    false,
  );
  assert.equal(
    consumePostgresRestoreActivationRecoveryBatchReceipt(
      service,
      "supervisorStateGc",
      position.afterSessionId,
      position.afterAuthorizedAt,
      "stop-crossed",
      position.limit,
      receipt,
    ),
    false,
  );
  assert.equal(
    consumeGcReceipt(service, SESSION_ID, 10, receipt),
    true,
  );
  assert.equal(
    consumeGcReceipt(service, SESSION_ID, 10, receipt),
    false,
  );
});

test("receipt verification rejects Proxy wrappers without traps or consumption", async () => {
  const service = createPostgresRestoreActivationRecoveryService(
    callbacks().options,
  );
  const receipt = await service.runGenerationBatch(request());
  let traps = 0;
  const handler = {
    get() {
      traps += 1;
      throw new Error("must not read wrapped properties");
    },
    getOwnPropertyDescriptor() {
      traps += 1;
      throw new Error("must not inspect wrapped descriptors");
    },
    getPrototypeOf() {
      traps += 1;
      throw new Error("must not inspect wrapped prototypes");
    },
    ownKeys() {
      traps += 1;
      throw new Error("must not enumerate wrapped keys");
    },
  };

  assert.equal(
    consumePostgresRestoreActivationRecoveryBatchReceipt(
      new Proxy(service, handler),
      "generation",
      null,
      10,
      receipt,
    ),
    false,
  );
  assert.equal(
    consumePostgresRestoreActivationRecoveryBatchReceipt(
      service,
      "generation",
      null,
      10,
      new Proxy(receipt, handler),
    ),
    false,
  );
  assert.equal(traps, 0);
  assert.equal(
    consumePostgresRestoreActivationRecoveryBatchReceipt(
      service,
      "generation",
      null,
      10,
      receipt,
    ),
    true,
  );
});

test("activation batches admit exact v1 and capture-bound v2 predecessors", async () => {
  const observed = [];
  const fixture = callbacks({
    async listRestoreAttachmentActivationCandidates() {
      return page([
        activationCandidate(SESSION_ID),
        activationCandidate(OTHER_SESSION_ID, "uncertain", { v2: true }),
      ]);
    },
    async reconcileRestoreAttachmentActivation(candidate) {
      observed.push(candidate);
    },
  });
  const service = createPostgresRestoreActivationRecoveryService(
    fixture.options,
  );

  const result = await service.runActivationBatch(request());

  assert.deepEqual(
    result.results.map(({ sessionId, status }) => ({ sessionId, status })),
    [
      { sessionId: SESSION_ID, status: "reconciled" },
      { sessionId: OTHER_SESSION_ID, status: "reconciled" },
    ],
  );
  assert.equal(observed[0].request.contractVersion, 1);
  assert.deepEqual(Reflect.ownKeys(observed[0].request.predecessor), [
    "attachmentId",
    "detachOperationId",
    "stopOperationId",
  ]);
  assert.equal(observed[1].request.contractVersion, 2);
  assert.deepEqual(Reflect.ownKeys(observed[1].request.predecessor), [
    "attachmentId",
    "captureOperationId",
    "detachOperationId",
    "stopOperationId",
  ]);
  assert.equal(
    observed[1].request.predecessor.captureOperationId,
    `capture-${OTHER_SESSION_ID}`,
  );
  assertDeepFrozen(observed[0]);
  assertDeepFrozen(observed[1]);
  assertDeepFrozen(result);
});

test("recovery pathnames accept 4095 UTF-8 bytes and reject 4096", async () => {
  const maximumPath = `/${"é".repeat(2_047)}`;
  const oversizedPath = `${maximumPath}a`;
  assert.equal(Buffer.byteLength(maximumPath, "utf8"), 4_095);
  assert.equal(Buffer.byteLength(oversizedPath, "utf8"), 4_096);

  const acceptedActivation = activationCandidate();
  acceptedActivation.request.destinationRootPath = maximumPath;
  let observedActivation;
  const activationFixture = callbacks({
    async listRestoreAttachmentActivationCandidates() {
      return page([acceptedActivation]);
    },
    async reconcileRestoreAttachmentActivation(candidate) {
      observedActivation = candidate;
    },
  });
  const activationService = createPostgresRestoreActivationRecoveryService(
    activationFixture.options,
  );
  const activationResult = await activationService.runActivationBatch(request());
  assert.equal(activationResult.results[0].status, "reconciled");
  assert.equal(observedActivation.request.destinationRootPath, maximumPath);

  const rejectedActivation = activationCandidate();
  rejectedActivation.request.destinationRootPath = oversizedPath;
  const rejectedActivationFixture = callbacks({
    async listRestoreAttachmentActivationCandidates() {
      return page([rejectedActivation]);
    },
  });
  await assert.rejects(
    createPostgresRestoreActivationRecoveryService(
      rejectedActivationFixture.options,
    ).runActivationBatch(request()),
    assertCode(
      "postgres_restore_activation_recovery_service_outcome_uncertain",
    ),
  );

  const acceptedLaunch = launchCandidate();
  acceptedLaunch.request.measuredImage.runtimeIdentity.codexBinaryPath =
    maximumPath;
  let observedLaunch;
  const launchFixture = callbacks({
    async listWriterLaunchAttemptCandidates() {
      return page([acceptedLaunch]);
    },
    async reconcileWriterLaunchAttempt(candidate) {
      observedLaunch = candidate;
    },
  });
  const launchService = createPostgresRestoreActivationRecoveryService(
    launchFixture.options,
  );
  const launchResult = await launchService.runLaunchAttemptBatch(request());
  assert.equal(launchResult.results[0].status, "reconciled");
  assert.equal(
    observedLaunch.request.measuredImage.runtimeIdentity.codexBinaryPath,
    maximumPath,
  );

  const rejectedLaunch = launchCandidate();
  rejectedLaunch.request.measuredImage.runtimeIdentity.codexBinaryPath =
    oversizedPath;
  const rejectedLaunchFixture = callbacks({
    async listWriterLaunchAttemptCandidates() {
      return page([rejectedLaunch]);
    },
  });
  await assert.rejects(
    createPostgresRestoreActivationRecoveryService(
      rejectedLaunchFixture.options,
    ).runLaunchAttemptBatch(request()),
    assertCode(
      "postgres_restore_activation_recovery_service_outcome_uncertain",
    ),
  );
});

test(
  "recovery validates candidate paths with captured node:path intrinsics",
  { concurrency: false },
  async () => {
    const isAbsoluteDescriptor = Object.getOwnPropertyDescriptor(
      path,
      "isAbsolute",
    );
    const resolveDescriptor = Object.getOwnPropertyDescriptor(path, "resolve");
    const candidate = activationCandidate();
    candidate.request.destinationRootPath = "relative/session";
    let poisonCalls = 0;
    let reconcileCalls = 0;
    const fixture = callbacks({
      async listRestoreAttachmentActivationCandidates() {
        path.isAbsolute = () => {
          poisonCalls += 1;
          return true;
        };
        path.resolve = (value) => {
          poisonCalls += 1;
          return value;
        };
        syncBuiltinESMExports();
        return page([candidate]);
      },
      async reconcileRestoreAttachmentActivation() {
        reconcileCalls += 1;
      },
    });
    try {
      await assert.rejects(
        createPostgresRestoreActivationRecoveryService(
          fixture.options,
        ).runActivationBatch(request()),
        assertCode(
          "postgres_restore_activation_recovery_service_outcome_uncertain",
        ),
      );
      assert.equal(poisonCalls, 0);
      assert.equal(reconcileCalls, 0);
    } finally {
      Object.defineProperty(path, "isAbsolute", isAbsoluteDescriptor);
      Object.defineProperty(path, "resolve", resolveDescriptor);
      syncBuiltinESMExports();
    }
  },
);

test("runSweep preserves independent cursors and fixed lane order", async () => {
  const order = [];
  const fixture = callbacks({
    async listCurrentWriterLaunchCandidates(input) {
      order.push("current");
      assert.equal(input.afterSessionId, THIRD_SESSION_ID);
      return page([]);
    },
    async listRestoreAttachmentActivationCandidates(input) {
      order.push("activation");
      assert.equal(input.afterSessionId, SESSION_ID);
      return page(
        [activationCandidate(OTHER_SESSION_ID, "starting", { v2: true })],
        OTHER_SESSION_ID,
      );
    },
    async listRestoreGenerationCandidates(input) {
      order.push("generation");
      assert.equal(input.afterSessionId, null);
      return page([generationCandidate(SESSION_ID)], SESSION_ID);
    },
    async listWriterLaunchAttemptCandidates(input) {
      order.push("launch");
      assert.equal(input.afterSessionId, OTHER_SESSION_ID);
      return page([launchCandidate(THIRD_SESSION_ID)], THIRD_SESSION_ID);
    },
    async listWriterSupervisorStateGcCandidates(input) {
      order.push("supervisor-state-gc");
      assert.equal(input.afterSessionId, THIRD_SESSION_ID);
      return gcPage([]);
    },
  });
  const service = createPostgresRestoreActivationRecoveryService(
    fixture.options,
  );

  const result = await service.runSweep({
    activation: lane(SESSION_ID, 1),
    currentLaunch: lane(THIRD_SESSION_ID, 4),
    generation: lane(null, 1),
    launchAttempt: lane(OTHER_SESSION_ID, 1),
    signal: null,
    supervisorStateGc: gcLane(THIRD_SESSION_ID, 5),
  });

  assert.deepEqual(order, [
    "generation",
    "activation",
    "launch",
    "current",
    "supervisor-state-gc",
  ]);
  assert.equal(result.generation.nextAfterSessionId, SESSION_ID);
  assert.equal(result.activation.nextAfterSessionId, OTHER_SESSION_ID);
  assert.equal(result.launchAttempt.nextAfterSessionId, THIRD_SESSION_ID);
  assert.equal(result.currentLaunch.nextAfterSessionId, null);
  assert.equal(result.supervisorStateGc.nextAfterSessionId, null);
  assert.equal(result.status, "limit-reached");
  assert.equal(
    consumePostgresRestoreActivationRecoveryBatchReceipt(
      service,
      "generation",
      null,
      1,
      result.generation,
    ),
    true,
  );
  assert.equal(
    consumePostgresRestoreActivationRecoveryBatchReceipt(
      service,
      "activation",
      SESSION_ID,
      1,
      result.activation,
    ),
    true,
  );
  assert.equal(
    consumePostgresRestoreActivationRecoveryBatchReceipt(
      service,
      "launchAttempt",
      OTHER_SESSION_ID,
      1,
      result.launchAttempt,
    ),
    true,
  );
  assert.equal(
    consumePostgresRestoreActivationRecoveryBatchReceipt(
      service,
      "currentLaunch",
      THIRD_SESSION_ID,
      4,
      result.currentLaunch,
    ),
    true,
  );
  assert.equal(
    consumeGcReceipt(
      service,
      THIRD_SESSION_ID,
      5,
      result.supervisorStateGc,
    ),
    true,
  );
  assertDeepFrozen(result);
});

test(
  "runSweep ignores callback-time iterator replacement",
  { concurrency: false },
  async () => {
    const iteratorDescriptor = Object.getOwnPropertyDescriptor(
      Array.prototype,
      Symbol.iterator,
    );
    const originalIterator = iteratorDescriptor.value;
    let iteratorCalls = 0;
    let order = "";

    function recordOrder(value) {
      order = order === "" ? value : `${order},${value}`;
    }

    function pollutedArrayIterator() {
      const field = this[0];
      const isLaneEntry =
        this.length === 2 &&
        field === this[1] &&
        (field === "generation" ||
          field === "activation" ||
          field === "launchAttempt" ||
          field === "currentLaunch" ||
          field === "supervisorStateGc");
      if (!isLaneEntry) {
        return Reflect.apply(originalIterator, this, []);
      }
      iteratorCalls += 1;
      let position = 0;
      return {
        next() {
          position += 1;
          if (position === 1) {
            return { done: false, value: "currentLaunch" };
          }
          if (position === 2) {
            return { done: false, value: "generation" };
          }
          return { done: true, value: undefined };
        },
        return() {
          return { done: true, value: undefined };
        },
      };
    }

    const fixture = callbacks({
      listCurrentWriterLaunchCandidates() {
        recordOrder("current");
        return page([]);
      },
      listRestoreAttachmentActivationCandidates() {
        recordOrder("activation");
        return page([]);
      },
      listRestoreGenerationCandidates() {
        recordOrder("generation");
        Object.defineProperty(Array.prototype, Symbol.iterator, {
          ...iteratorDescriptor,
          value: pollutedArrayIterator,
        });
        return page([]);
      },
      listWriterLaunchAttemptCandidates() {
        recordOrder("launch");
        return page([]);
      },
      listWriterSupervisorStateGcCandidates() {
        recordOrder("supervisor-state-gc");
        return gcPage([]);
      },
    });
    const service = createPostgresRestoreActivationRecoveryService(
      fixture.options,
    );
    const sweepRequest = {
      activation: lane(SESSION_ID, 2),
      currentLaunch: lane(THIRD_SESSION_ID, 4),
      generation: lane(null, 1),
      launchAttempt: lane(OTHER_SESSION_ID, 3),
      signal: null,
      supervisorStateGc: gcLane(THIRD_SESSION_ID, 5),
    };
    let result;
    let runError;

    try {
      result = await service.runSweep(sweepRequest);
    } catch (error) {
      runError = error;
    } finally {
      restoreOwnPropertyDescriptor(
        Array.prototype,
        Symbol.iterator,
        iteratorDescriptor,
      );
    }

    assert.ifError(runError);
    assert.equal(iteratorCalls, 0);
    assert.equal(
      order,
      "generation,activation,launch,current,supervisor-state-gc",
    );
    assert.equal(result.status, "sweep-complete");
    assert.equal(
      consumePostgresRestoreActivationRecoveryBatchReceipt(
        service,
        "generation",
        null,
        1,
        result.generation,
      ),
      true,
    );
    assert.equal(
      consumeGcReceipt(
        service,
        THIRD_SESSION_ID,
        5,
        result.supervisorStateGc,
      ),
      true,
    );
    assert.equal(
      consumePostgresRestoreActivationRecoveryBatchReceipt(
        service,
        "activation",
        SESSION_ID,
        2,
        result.activation,
      ),
      true,
    );
    assert.equal(
      consumePostgresRestoreActivationRecoveryBatchReceipt(
        service,
        "launchAttempt",
        OTHER_SESSION_ID,
        3,
        result.launchAttempt,
      ),
      true,
    );
    assert.equal(
      consumePostgresRestoreActivationRecoveryBatchReceipt(
        service,
        "currentLaunch",
        THIRD_SESSION_ID,
        4,
        result.currentLaunch,
      ),
      true,
    );
  },
);

test("sparse current-launch pages advance the scanned authority cursor", async () => {
  const fixture = callbacks({
    async listCurrentWriterLaunchCandidates() {
      return page([], OTHER_SESSION_ID);
    },
  });
  const service = createPostgresRestoreActivationRecoveryService(
    fixture.options,
  );

  const result = await service.scanCurrentLaunchBatch(request());

  assert.deepEqual(plainBatch(result), {
    afterSessionId: null,
    nextAfterSessionId: OTHER_SESSION_ID,
    results: [],
    status: "limit-reached",
  });
});

test("abort before admission performs no durable read", async () => {
  const controller = new AbortController();
  controller.abort();
  const fixture = callbacks();
  const service = createPostgresRestoreActivationRecoveryService(
    fixture.options,
  );

  const result = await service.runGenerationBatch(
    request({ signal: controller.signal }),
  );

  assert.deepEqual(plainBatch(result), {
    afterSessionId: null,
    nextAfterSessionId: null,
    results: [],
    status: "aborted",
  });
  assert.deepEqual(fixture.calls, []);
});

test("abort drains one in-flight reconciliation and retains its settled cursor", async () => {
  const controller = new AbortController();
  const reconciled = [];
  const fixture = callbacks({
    async listRestoreGenerationCandidates() {
      return page([generationCandidate(SESSION_ID)]);
    },
    async reconcileRestoreGeneration(candidate) {
      reconciled.push(candidate.request.sessionId);
      controller.abort();
    },
  });
  const service = createPostgresRestoreActivationRecoveryService(
    fixture.options,
  );

  const result = await service.runGenerationBatch(
    request({ signal: controller.signal }),
  );

  assert.deepEqual(reconciled, [SESSION_ID]);
  assert.equal(result.status, "aborted");
  assert.equal(result.nextAfterSessionId, SESSION_ID);
  assert.equal(result.results.length, 1);
  assert.equal(result.results[0].status, "reconciled");
  assert.equal(
    consumePostgresRestoreActivationRecoveryBatchReceipt(
      service,
      "generation",
      null,
      10,
      result,
    ),
    true,
  );
});

test("one service admits only one batch at a time", async () => {
  let release;
  const waiting = new Promise((resolve) => {
    release = resolve;
  });
  const fixture = callbacks({
    async listRestoreGenerationCandidates() {
      await waiting;
      return page([]);
    },
  });
  const service = createPostgresRestoreActivationRecoveryService(
    fixture.options,
  );

  const first = service.runGenerationBatch(request());
  await assert.rejects(
    service.runActivationBatch(request()),
    assertCode(
      "postgres_restore_activation_recovery_service_outcome_uncertain",
    ),
  );
  release();
  await first;
});

test("callback failure and generator output stay pending without stopping the page", async () => {
  let calls = 0;
  const fixture = callbacks({
    async listWriterLaunchAttemptCandidates() {
      return page([
        launchCandidate(SESSION_ID),
        launchCandidate(OTHER_SESSION_ID),
      ]);
    },
    reconcileWriterLaunchAttempt() {
      calls += 1;
      if (calls === 1) {
        return (function* rejectedGenerator() {
          yield "must-not-be-consumed";
        })();
      }
      throw new Error("stopped-only evidence unavailable");
    },
  });
  const service = createPostgresRestoreActivationRecoveryService(
    fixture.options,
  );

  const result = await service.runLaunchAttemptBatch(request());

  assert.deepEqual(
    result.results.map(({ status }) => status),
    ["pending", "pending"],
  );
  assert.equal(calls, 2);
});

test("Promise subclasses cannot spoof list or reconciliation results", async () => {
  let thenCalls = 0;
  class SpoofedRecoveryPromise extends Promise {
    then(onFulfilled, onRejected) {
      thenCalls += 1;
      return Promise.resolve("forged").then(onFulfilled, onRejected);
    }
  }
  const spoofedList = new SpoofedRecoveryPromise((resolve) => {
    resolve(page([generationCandidate()]));
  });
  const listFixture = callbacks({
    listRestoreGenerationCandidates() {
      return spoofedList;
    },
  });
  const listService = createPostgresRestoreActivationRecoveryService(
    listFixture.options,
  );

  await assert.rejects(
    listService.runGenerationBatch(request()),
    assertCode(
      "postgres_restore_activation_recovery_service_outcome_uncertain",
    ),
  );

  const spoofedReconciliation = new SpoofedRecoveryPromise((resolve) => {
    resolve({ forged: true });
  });
  const reconcileFixture = callbacks({
    reconcileRestoreGeneration() {
      return spoofedReconciliation;
    },
  });
  const reconcileService = createPostgresRestoreActivationRecoveryService(
    reconcileFixture.options,
  );
  await assert.rejects(
    reconcileService.runGenerationBatch(request()),
    assertCode(
      "postgres_restore_activation_recovery_service_outcome_uncertain",
    ),
  );
  assert.equal(thenCalls, 0);
});

test("ordinary and accessor thenables stay pending without invoking then", async () => {
  let dataThenCalls = 0;
  let accessorThenCalls = 0;
  const values = [
    {
      then() {
        dataThenCalls += 1;
      },
    },
    Object.defineProperty({}, "then", {
      configurable: true,
      enumerable: true,
      get() {
        accessorThenCalls += 1;
        return () => {};
      },
    }),
  ];

  for (const value of values) {
    const fixture = callbacks({
      reconcileRestoreGeneration() {
        return value;
      },
    });
    const service = createPostgresRestoreActivationRecoveryService(
      fixture.options,
    );
    const result = await service.runGenerationBatch(request());
    assert.equal(result.results[0].status, "pending");
  }

  assert.equal(dataThenCalls, 0);
  assert.equal(accessorThenCalls, 0);
});

test("malformed pages and candidates fail closed", async () => {
  const malformed = [
    page([generationCandidate(SESSION_ID)], SESSION_ID),
    page([generationCandidate(SESSION_ID)], OTHER_SESSION_ID),
    page(
      [
        generationCandidate(OTHER_SESSION_ID),
        generationCandidate(SESSION_ID),
      ],
      null,
    ),
    page([generationCandidate(SESSION_ID)], "not-a-session"),
    page([{ ...generationCandidate(), extra: true }]),
    page([
      {
        ...generationCandidate(),
        request: { ...restoreRequest(), operation: "checkpoint" },
      },
    ]),
    page([
      {
        ...generationCandidate(),
        request: { ...restoreRequest(), storageId: "storage-other" },
      },
    ]),
  ];
  for (let index = 0; index < malformed.length; index += 1) {
    const fixture = callbacks({
      async listRestoreGenerationCandidates() {
        return malformed[index];
      },
    });
    const service = createPostgresRestoreActivationRecoveryService(
      fixture.options,
    );
    await assert.rejects(
      service.runGenerationBatch(
        request({ afterSessionId: index === 0 ? SESSION_ID : null }),
      ),
      assertCode(
        "postgres_restore_activation_recovery_service_outcome_uncertain",
      ),
    );
  }

  const invalidActivation = callbacks({
    async listRestoreAttachmentActivationCandidates() {
      return page([activationCandidate(SESSION_ID, "prepared")]);
    },
  });
  const service = createPostgresRestoreActivationRecoveryService(
    invalidActivation.options,
  );
  await assert.rejects(
    service.runActivationBatch(request()),
    assertCode(
      "postgres_restore_activation_recovery_service_outcome_uncertain",
    ),
  );
});

test("activation recovery rejects crossed or incomplete predecessor versions", async () => {
  const v1WithCapture = activationCandidate();
  v1WithCapture.request.predecessor.captureOperationId =
    `capture-${SESSION_ID}`;
  const v2WithoutCapture = activationCandidate(
    SESSION_ID,
    "starting",
    { v2: true },
  );
  delete v2WithoutCapture.request.predecessor.captureOperationId;
  const v2WithInvalidCapture = activationCandidate(
    SESSION_ID,
    "starting",
    { v2: true },
  );
  v2WithInvalidCapture.request.predecessor.captureOperationId = "";

  for (const candidate of [
    v1WithCapture,
    v2WithoutCapture,
    v2WithInvalidCapture,
  ]) {
    const fixture = callbacks({
      async listRestoreAttachmentActivationCandidates() {
        return page([candidate]);
      },
    });
    const service = createPostgresRestoreActivationRecoveryService(
      fixture.options,
    );
    await assert.rejects(
      service.runActivationBatch(request()),
      assertCode(
        "postgres_restore_activation_recovery_service_outcome_uncertain",
      ),
    );
  }
});

test("activation v2 predecessor proxies and accessors fail without traps", async () => {
  let trapCalls = 0;
  const proxied = activationCandidate(SESSION_ID, "starting", { v2: true });
  proxied.request.predecessor = new Proxy(proxied.request.predecessor, {
    get() {
      trapCalls += 1;
      throw new Error("proxy trap must not run");
    },
    getOwnPropertyDescriptor() {
      trapCalls += 1;
      throw new Error("proxy trap must not run");
    },
    getPrototypeOf() {
      trapCalls += 1;
      throw new Error("proxy trap must not run");
    },
    ownKeys() {
      trapCalls += 1;
      throw new Error("proxy trap must not run");
    },
  });
  let getterCalls = 0;
  const accessor = activationCandidate(SESSION_ID, "starting", { v2: true });
  Object.defineProperty(accessor.request.predecessor, "captureOperationId", {
    enumerable: true,
    get() {
      getterCalls += 1;
      throw new Error("accessor must not run");
    },
  });

  for (const candidate of [proxied, accessor]) {
    const fixture = callbacks({
      async listRestoreAttachmentActivationCandidates() {
        return page([candidate]);
      },
    });
    const service = createPostgresRestoreActivationRecoveryService(
      fixture.options,
    );
    await assert.rejects(
      service.runActivationBatch(request()),
      assertCode(
        "postgres_restore_activation_recovery_service_outcome_uncertain",
      ),
    );
  }
  assert.equal(trapCalls, 0);
  assert.equal(getterCalls, 0);
});

test("proxy, accessor, and list exceptions do not cross the recovery boundary", async () => {
  const proxyFixture = callbacks({
    async listRestoreGenerationCandidates() {
      return page([new Proxy(generationCandidate(), {})]);
    },
  });
  const proxyService = createPostgresRestoreActivationRecoveryService(
    proxyFixture.options,
  );
  await assert.rejects(
    proxyService.runGenerationBatch(request()),
    assertCode(
      "postgres_restore_activation_recovery_service_outcome_uncertain",
    ),
  );

  const accessor = generationCandidate();
  Object.defineProperty(accessor, "generationId", {
    enumerable: true,
    get() {
      throw new Error("must not run");
    },
  });
  const accessorFixture = callbacks({
    async listRestoreGenerationCandidates() {
      return page([accessor]);
    },
  });
  const accessorService = createPostgresRestoreActivationRecoveryService(
    accessorFixture.options,
  );
  await assert.rejects(
    accessorService.runGenerationBatch(request()),
    assertCode(
      "postgres_restore_activation_recovery_service_outcome_uncertain",
    ),
  );

  const errorFixture = callbacks({
    async listRestoreGenerationCandidates() {
      throw new Error("database details must not escape");
    },
  });
  const errorService = createPostgresRestoreActivationRecoveryService(
    errorFixture.options,
  );
  let observed;
  await assert.rejects(
    errorService.runGenerationBatch(request()),
    (error) => {
      observed = error;
      return assertCode(
        "postgres_restore_activation_recovery_service_outcome_uncertain",
      )(error);
    },
  );
  assert.equal(observed.message.includes("database details"), false);
});

test("callback-time Symbol.hasInstance pollution cannot leak raw errors", async () => {
  const descriptor = Object.getOwnPropertyDescriptor(
    PostgresRestoreActivationRecoveryServiceError,
    Symbol.hasInstance,
  );
  const fixture = callbacks({
    listRestoreGenerationCandidates() {
      Object.defineProperty(
        PostgresRestoreActivationRecoveryServiceError,
        Symbol.hasInstance,
        {
          configurable: true,
          value() {
            return true;
          },
        },
      );
      throw new Error("raw callback error must not escape");
    },
  });
  const service = createPostgresRestoreActivationRecoveryService(
    fixture.options,
  );
  try {
    await assert.rejects(
      service.runGenerationBatch(request()),
      assertCode(
        "postgres_restore_activation_recovery_service_outcome_uncertain",
      ),
    );
  } finally {
    restoreOwnPropertyDescriptor(
      PostgresRestoreActivationRecoveryServiceError,
      Symbol.hasInstance,
      descriptor,
    );
  }
});

test("callbacks cannot forge or replay module service errors", async () => {
  let replayedError = null;
  const fixture = callbacks({
    listRestoreGenerationCandidates() {
      if (replayedError !== null) throw replayedError;
      throw new PostgresRestoreActivationRecoveryServiceError(
        "invalid_postgres_restore_activation_recovery_service_request",
      );
    },
  });
  const service = createPostgresRestoreActivationRecoveryService(
    fixture.options,
  );
  await assert.rejects(
    service.runGenerationBatch(request()),
    assertCode(
      "postgres_restore_activation_recovery_service_outcome_uncertain",
    ),
  );

  const replayFixture = callbacks({
    listRestoreGenerationCandidates() {
      if (replayedError !== null) throw replayedError;
      return page();
    },
  });
  const replayService = createPostgresRestoreActivationRecoveryService(
    replayFixture.options,
  );
  try {
    await replayService.runGenerationBatch({ ...request(), limit: 0 });
  } catch (error) {
    replayedError = error;
  }
  assert.equal(
    replayedError instanceof PostgresRestoreActivationRecoveryServiceError,
    true,
  );
  await assert.rejects(
    replayService.runGenerationBatch(request()),
    assertCode(
      "postgres_restore_activation_recovery_service_outcome_uncertain",
    ),
  );
});

test("constructor and requests reject non-exact or executable expansion", async () => {
  const fixture = callbacks();
  const invalidOptions = [
    undefined,
    { ...fixture.options, runPreparedLaunch() {} },
    {
      ...fixture.options,
      reconcileWriterLaunchAttempt: function* invalidGenerator() {
        yield "launch";
      },
    },
  ];
  for (const options of invalidOptions) {
    assert.throws(
      () => createPostgresRestoreActivationRecoveryService(options),
      assertCode(
        "invalid_postgres_restore_activation_recovery_service_options",
      ),
    );
  }

  const service = createPostgresRestoreActivationRecoveryService(
    fixture.options,
  );
  for (const input of [
    { afterSessionId: null, limit: 1 },
    request({ extra: true }),
    request({ afterSessionId: "invalid" }),
    request({ limit: 0 }),
    request({ limit: 101 }),
    request({ signal: {} }),
  ]) {
    await assert.rejects(
      service.runGenerationBatch(input),
      assertCode(
        "invalid_postgres_restore_activation_recovery_service_request",
      ),
    );
  }

  let signalPrototypeTrapCalls = 0;
  const hostileSignal = Object.create(
    new Proxy(Object.create(null), {
      getPrototypeOf() {
        signalPrototypeTrapCalls += 1;
        throw new Error("signal prototype trap must not run");
      },
    }),
  );
  await assert.rejects(
    service.runGenerationBatch(request({ signal: hostileSignal })),
    assertCode(
      "invalid_postgres_restore_activation_recovery_service_request",
    ),
  );
  assert.equal(signalPrototypeTrapCalls, 0);
});

test("accepted generation v2 and current launch inputs are defensively frozen", async () => {
  let observedGeneration;
  const generation = generationCandidate(SESSION_ID, { v2: true });
  const fixture = callbacks({
    async listRestoreGenerationCandidates() {
      return page([generation]);
    },
    async reconcileRestoreGeneration(candidate) {
      observedGeneration = candidate;
    },
  });
  const service = createPostgresRestoreActivationRecoveryService(
    fixture.options,
  );

  await service.runGenerationBatch(request());
  generation.launchIntent.launchAttemptId = "tampered-after-list";

  assert.equal(
    observedGeneration.launchIntent.launchAttemptId,
    `launch-${SESSION_ID}`,
  );
  assertDeepFrozen(observedGeneration);
});
