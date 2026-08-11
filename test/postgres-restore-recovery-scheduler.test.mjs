import assert from "node:assert/strict";
import test from "node:test";

import {
  PostgresOperationGuard,
} from "../src/postgres-operation-guard.mjs";
import {
  createPostgresRestoreActivationRecoveryService,
} from "../src/postgres-restore-activation-recovery-service.mjs";
import {
  createPostgresRestoreLifecycleGuard,
} from "../src/postgres-restore-lifecycle-guard.mjs";
import {
  createPostgresRestoreRecoveryRunner,
} from "../src/postgres-restore-recovery-runner.mjs";
import {
  PostgresRestoreRecoverySchedulerError,
  createPostgresRestoreRecoveryScheduler,
  isPostgresRestoreRecoveryScheduler,
} from "../src/postgres-restore-recovery-scheduler.mjs";

const RECOVERY_SCOPE_ID = "restore-scheduler-scope-001";
const SESSION_ID = "019f2700-0000-7000-8000-000000000001";
const CODEX_ID = "019f2700-0000-7000-8000-000000000002";
const IMAGE_DIGEST = `sha256:${"a".repeat(64)}`;
const UPDATED_AT = "2026-08-11T00:00:00.000Z";
const LANE_SPECS = Object.freeze([
  Object.freeze({ field: "generation", lane: "generation" }),
  Object.freeze({ field: "activation", lane: "activation" }),
  Object.freeze({ field: "launchAttempt", lane: "launch-attempt" }),
  Object.freeze({ field: "currentLaunch", lane: "current-launch" }),
]);

function freezeRecord(value) {
  return Object.freeze(Object.assign(Object.create(null), value));
}

function deferred() {
  let reject;
  let resolve;
  const promise = new Promise((resolvePromise, rejectPromise) => {
    reject = rejectPromise;
    resolve = resolvePromise;
  });
  return { promise, reject, resolve };
}

function nextTurn() {
  return new Promise((resolve) => setImmediate(resolve));
}

class AdvisoryLockManager {
  constructor() {
    this.holders = new Map();
  }

  tryAcquire(key, client, mode) {
    const holders = this.holders.get(key) ?? {
      exclusive: new Set(),
      shared: new Set(),
    };
    this.holders.set(key, holders);
    if (mode === "shared") {
      if (
        [...holders.exclusive].some((holder) => holder !== client)
      ) {
        return false;
      }
      holders.shared.add(client);
      return true;
    }
    if (
      [...holders.exclusive, ...holders.shared].some(
        (holder) => holder !== client,
      )
    ) {
      return false;
    }
    holders.exclusive.add(client);
    return true;
  }

  isHeld(key, client, mode) {
    return this.holders.get(key)?.[mode]?.has(client) === true;
  }

  unlock(key, client, mode) {
    const holders = this.holders.get(key);
    if (holders?.[mode]?.delete(client) !== true) return false;
    if (holders.exclusive.size === 0 && holders.shared.size === 0) {
      this.holders.delete(key);
    }
    return true;
  }

  releaseAll(client) {
    for (const [key, holders] of this.holders) {
      holders.exclusive.delete(client);
      holders.shared.delete(client);
      if (holders.exclusive.size === 0 && holders.shared.size === 0) {
        this.holders.delete(key);
      }
    }
  }
}

class GuardClient {
  constructor(manager, pid) {
    this.manager = manager;
    this.pid = pid;
    this.resetCount = 0;
    this.releaseCalls = [];
  }

  async query(...args) {
    const query = args[0];
    const text = typeof query === "string" ? query : query.text;
    if (text === "DISCARD ALL") {
      this.resetCount += 1;
      this.manager.releaseAll(this);
      return { command: "DISCARD", rows: [] };
    }
    const values = typeof query === "string" ? args[1] : query.values;
    const key = values[0];
    if (text.includes("pg_try_advisory_lock")) {
      const mode = text.includes("pg_try_advisory_lock_shared")
        ? "shared"
        : "exclusive";
      return {
        command: "SELECT",
        rows: [
          {
            acquired: this.manager.tryAcquire(key, this, mode),
            backend_pid: this.pid,
          },
        ],
      };
    }
    if (text.includes("FROM pg_catalog.pg_locks")) {
      const mode = text.includes("mode = 'ShareLock'")
        ? "shared"
        : "exclusive";
      return {
        command: "SELECT",
        rows: [
          {
            backend_pid: this.pid,
            lock_held: this.manager.isHeld(key, this, mode),
          },
        ],
      };
    }
    if (text.includes("pg_advisory_unlock")) {
      const mode = text.includes("pg_advisory_unlock_shared")
        ? "shared"
        : "exclusive";
      return {
        command: "SELECT",
        rows: [
          {
            backend_pid: this.pid,
            unlocked: this.manager.unlock(key, this, mode),
          },
        ],
      };
    }
    throw new Error(`unexpected guard query: ${text}`);
  }

  async release(...args) {
    this.releaseCalls.push(args);
    if (args.length === 1) this.manager.releaseAll(this);
  }
}

class GuardPool {
  constructor(manager) {
    this.manager = manager;
    this.connectCalls = 0;
    this.clients = [];
    this.onConnect = null;
  }

  async connect() {
    this.connectCalls += 1;
    this.onConnect?.();
    const client = new GuardClient(
      this.manager,
      20_000 + this.connectCalls,
    );
    this.clients.push(client);
    return client;
  }
}

function createLifecycleFixture() {
  const manager = new AdvisoryLockManager();
  const pool = new GuardPool(manager);
  const operationGuard = new PostgresOperationGuard({ dedicatedPool: pool });
  const lifecycleGuard = createPostgresRestoreLifecycleGuard({
    operationGuard,
  });
  return { lifecycleGuard, manager, operationGuard, pool };
}

function checkpoint() {
  return {
    contractVersion: 1,
    checkpointId: "checkpoint-scheduler-001",
    artifactId: "artifact-scheduler-001",
    backendId: "backend-001",
    storageId: "storage-001",
    sessionId: SESSION_ID,
    codexThreadId: CODEX_ID,
    codexSessionId: CODEX_ID,
    imageDigest: IMAGE_DIGEST,
    sourceFencingEpoch: "1",
    checkpointClass: "clean",
    createdAt: UPDATED_AT,
  };
}

function generationCandidate() {
  const descriptor = checkpoint();
  return {
    checkpoint: descriptor,
    generationId: "generation-scheduler-001",
    request: {
      contractVersion: 1,
      backendId: descriptor.backendId,
      storageId: descriptor.storageId,
      sessionId: SESSION_ID,
      leaseId: "lease-scheduler-001",
      holderId: "host-001",
      fencingEpoch: "2",
      operation: "restore",
      operationId: "restore-scheduler-001",
      target: {
        artifactId: descriptor.artifactId,
        checkpointId: descriptor.checkpointId,
        kind: "checkpoint",
      },
    },
  };
}

function cursor(lane, overrides = {}) {
  return freezeRecord({
    recoveryScopeId: RECOVERY_SCOPE_ID,
    lane,
    afterSessionId: null,
    cycle: "0",
    revision: "0",
    lastTransitionId: null,
    lastRequestSha256: null,
    updatedAt: UPDATED_AT,
    ...overrides,
  });
}

function createCursorStore({ failFirstRead = false } = {}) {
  const calls = [];
  const state = new Map(
    LANE_SPECS.map(({ lane }) => [lane, cursor(lane)]),
  );
  let shouldFailRead = failFirstRead;
  const store = freezeRecord({
    async readLane(input) {
      calls.push(["read", input]);
      if (shouldFailRead) {
        shouldFailRead = false;
        throw new Error("scripted cursor read failure");
      }
      return state.get(input.lane);
    },
    async advanceLane(input) {
      calls.push(["advance", input]);
      const before = state.get(input.lane);
      const next = cursor(input.lane, {
        afterSessionId: input.nextAfterSessionId,
        cycle:
          input.nextAfterSessionId === null
            ? `${BigInt(before.cycle) + 1n}`
            : before.cycle,
        revision: `${BigInt(before.revision) + 1n}`,
        lastTransitionId: input.transitionId,
        lastRequestSha256: input.requestSha256,
      });
      state.set(input.lane, next);
      return freezeRecord({ advanced: true, cursor: next });
    },
  });
  return { calls, state, store };
}

function createService({ generationPage = null, reconcileGeneration } = {}) {
  const calls = [];
  const service = createPostgresRestoreActivationRecoveryService({
    listCurrentWriterLaunchCandidates(input) {
      calls.push(["list:currentLaunch", input]);
      return { candidates: [], nextAfterSessionId: null };
    },
    listRestoreAttachmentActivationCandidates(input) {
      calls.push(["list:activation", input]);
      return { candidates: [], nextAfterSessionId: null };
    },
    listRestoreGenerationCandidates(input) {
      calls.push(["list:generation", input]);
      return generationPage?.(input) ?? {
        candidates: [],
        nextAfterSessionId: null,
      };
    },
    listWriterLaunchAttemptCandidates(input) {
      calls.push(["list:launchAttempt", input]);
      return { candidates: [], nextAfterSessionId: null };
    },
    reconcileRestoreAttachmentActivation(candidate) {
      calls.push(["reconcile:activation", candidate]);
    },
    reconcileRestoreGeneration(candidate) {
      calls.push(["reconcile:generation", candidate]);
      return reconcileGeneration?.(candidate);
    },
    reconcileWriterLaunchAttempt(candidate) {
      calls.push(["reconcile:launchAttempt", candidate]);
    },
  });
  return { calls, service };
}

function createRunnerFixture({
  cursorFixture = createCursorStore(),
  lifecycleFixture = createLifecycleFixture(),
  serviceFixture = createService(),
} = {}) {
  const runner = createPostgresRestoreRecoveryRunner({
    cursorStore: cursorFixture.store,
    lifecycleGuard: lifecycleFixture.lifecycleGuard,
    limits: {
      activation: 1,
      currentLaunch: 1,
      generation: 1,
      launchAttempt: 1,
    },
    recoveryScopeId: RECOVERY_SCOPE_ID,
    recoveryService: serviceFixture.service,
  });
  return { cursorFixture, lifecycleFixture, runner, serviceFixture };
}

function createSchedulerFixture({
  intervalMilliseconds = 60_000,
  onStep,
  runnerFixture = createRunnerFixture(),
} = {}) {
  const steps = [];
  const scheduler = createPostgresRestoreRecoveryScheduler({
    intervalMilliseconds,
    onStep(receipt) {
      steps.push(receipt);
      return onStep?.(receipt);
    },
    runner: runnerFixture.runner,
  });
  return { runnerFixture, scheduler, steps };
}

function schedulerCode(code) {
  return (error) =>
    error instanceof PostgresRestoreRecoverySchedulerError &&
    error.code === code;
}

test("scheduler starts immediately, repeats serially, and stops idempotently", async () => {
  const secondStep = deferred();
  const fixture = createSchedulerFixture({
    intervalMilliseconds: 5,
    onStep() {
      if (fixture.steps.length === 2) secondStep.resolve();
    },
  });

  const completion = fixture.scheduler.start();
  assert.strictEqual(fixture.scheduler.start(), completion);
  await secondStep.promise;
  const stopped = fixture.scheduler.stop();
  assert.strictEqual(stopped, completion);
  assert.strictEqual(fixture.scheduler.stop(), completion);
  assert.deepEqual(await stopped, freezeRecord({ status: "stopped" }));
  assert.equal(fixture.steps.length, 2);
  for (const receipt of fixture.steps) {
    assert.equal(receipt.status, "completed");
    assert.equal(receipt.errorCode, null);
    assert.equal(receipt.recovery.status, "sweep-complete");
    assert.equal(Object.isFrozen(receipt), true);
  }
  await new Promise((resolve) => setTimeout(resolve, 20));
  assert.equal(fixture.steps.length, 2);
  assert.throws(
    () => fixture.scheduler.start(),
    schedulerCode("invalid_postgres_restore_recovery_scheduler_request"),
  );
});

test("concurrent explicit kicks coalesce with the active bounded pass", async () => {
  const listed = deferred();
  const releaseList = deferred();
  const serviceFixture = createService({
    generationPage() {
      listed.resolve();
      return releaseList.promise;
    },
  });
  const fixture = createSchedulerFixture({
    runnerFixture: createRunnerFixture({ serviceFixture }),
  });
  const completion = fixture.scheduler.start();
  await listed.promise;

  const first = fixture.scheduler.runStep({ signal: null });
  const second = fixture.scheduler.runStep({ signal: null });
  assert.strictEqual(first, second);
  releaseList.resolve({ candidates: [], nextAfterSessionId: null });
  assert.equal((await first).status, "completed");
  assert.equal(fixture.steps.length, 1);

  const stopped = fixture.scheduler.stop();
  assert.strictEqual(stopped, completion);
  await stopped;
});

test("a foreground shared lease produces a busy tick without recovery work", async () => {
  const runnerFixture = createRunnerFixture();
  const foregroundEntered = deferred();
  const releaseForeground = deferred();
  const foreground = runnerFixture.lifecycleFixture.lifecycleGuard.runForeground(
    async () => {
      foregroundEntered.resolve();
      await releaseForeground.promise;
    },
  );
  await foregroundEntered.promise;

  const firstStep = deferred();
  const fixture = createSchedulerFixture({
    onStep(receipt) {
      if (fixture.steps.length === 1) firstStep.resolve(receipt);
    },
    runnerFixture,
  });
  const completion = fixture.scheduler.start();
  const busy = await firstStep.promise;
  assert.deepEqual(busy, freezeRecord({
    errorCode: null,
    recovery: null,
    status: "busy",
  }));
  assert.deepEqual(runnerFixture.cursorFixture.calls, []);

  releaseForeground.resolve();
  await foreground;
  const recovered = await fixture.scheduler.runStep({ signal: null });
  assert.equal(recovered.status, "completed");
  assert.equal(recovered.recovery.status, "sweep-complete");
  fixture.scheduler.stop();
  await completion;
});

test("an uncertain pass is reported and a later tick may recover", async () => {
  const cursorFixture = createCursorStore({ failFirstRead: true });
  const firstStep = deferred();
  const fixture = createSchedulerFixture({
    onStep(receipt) {
      if (fixture.steps.length === 1) firstStep.resolve(receipt);
    },
    runnerFixture: createRunnerFixture({ cursorFixture }),
  });
  const completion = fixture.scheduler.start();
  const uncertain = await firstStep.promise;
  assert.deepEqual(uncertain, freezeRecord({
    errorCode:
      "postgres_restore_recovery_scheduler_step_outcome_uncertain",
    recovery: null,
    status: "outcome-uncertain",
  }));
  await nextTurn();

  const recovered = await fixture.scheduler.runStep({ signal: null });
  assert.equal(recovered.status, "completed");
  assert.equal(recovered.recovery.status, "sweep-complete");
  fixture.scheduler.stop();
  await completion;
});

test("stop drains an admitted candidate and its settled cursor before resolving", async () => {
  const reconcileEntered = deferred();
  const releaseReconcile = deferred();
  const serviceFixture = createService({
    generationPage() {
      return {
        candidates: [generationCandidate()],
        nextAfterSessionId: null,
      };
    },
    reconcileGeneration() {
      reconcileEntered.resolve();
      return releaseReconcile.promise;
    },
  });
  const runnerFixture = createRunnerFixture({ serviceFixture });
  const fixture = createSchedulerFixture({ runnerFixture });
  const completion = fixture.scheduler.start();
  await reconcileEntered.promise;

  let stopped = false;
  const stop = fixture.scheduler.stop();
  stop.then(() => {
    stopped = true;
  });
  await nextTurn();
  assert.equal(stopped, false);
  releaseReconcile.resolve();
  assert.deepEqual(await stop, freezeRecord({ status: "stopped" }));
  assert.strictEqual(stop, completion);
  assert.equal(stopped, true);
  assert.equal(fixture.steps.length, 1);
  assert.equal(fixture.steps[0].status, "completed");
  assert.equal(fixture.steps[0].recovery.status, "aborted");
  assert.equal(
    fixture.steps[0].recovery.generation.batch.status,
    "aborted",
  );
  assert.equal(
    runnerFixture.cursorFixture.calls.filter(([kind]) => kind === "advance")
      .length,
    1,
  );
  assert.deepEqual(
    runnerFixture.cursorFixture.calls
      .filter(([kind]) => kind === "read")
      .map(([, input]) => input.lane),
    ["generation"],
  );
});

test("stop joins an explicit step admitted while the loop waits", async () => {
  const firstStep = deferred();
  const reconcileEntered = deferred();
  const releaseReconcile = deferred();
  let generationLists = 0;
  const serviceFixture = createService({
    generationPage() {
      generationLists += 1;
      if (generationLists === 1) {
        return { candidates: [], nextAfterSessionId: null };
      }
      return {
        candidates: [generationCandidate()],
        nextAfterSessionId: null,
      };
    },
    reconcileGeneration() {
      reconcileEntered.resolve();
      return releaseReconcile.promise;
    },
  });
  const runnerFixture = createRunnerFixture({ serviceFixture });
  const fixture = createSchedulerFixture({
    onStep() {
      if (fixture.steps.length === 1) firstStep.resolve();
    },
    runnerFixture,
  });
  const completion = fixture.scheduler.start();
  await firstStep.promise;
  await nextTurn();

  const explicitStep = fixture.scheduler.runStep({ signal: null });
  await reconcileEntered.promise;
  let stopped = false;
  const stop = fixture.scheduler.stop();
  stop.then(() => {
    stopped = true;
  });
  await nextTurn();
  assert.equal(stopped, false);

  releaseReconcile.resolve();
  const explicitReceipt = await explicitStep;
  assert.equal(explicitReceipt.status, "completed");
  assert.equal(explicitReceipt.recovery.status, "aborted");
  assert.deepEqual(await stop, freezeRecord({ status: "stopped" }));
  assert.strictEqual(stop, completion);
  assert.equal(stopped, true);
  assert.equal(fixture.steps.length, 2);
  assert.equal(
    runnerFixture.cursorFixture.calls.filter(([kind]) => kind === "advance")
      .length,
    5,
  );
});

test("explicit observer failure terminates the waiting scheduler", async () => {
  const firstStep = deferred();
  const fixture = createSchedulerFixture({
    onStep() {
      if (fixture.steps.length === 1) {
        firstStep.resolve();
        return;
      }
      throw new Error("explicit observer failed");
    },
  });
  const completion = fixture.scheduler.start();
  await firstStep.promise;
  await nextTurn();

  const explicitStep = fixture.scheduler.runStep({ signal: null });
  await assert.rejects(
    explicitStep,
    schedulerCode("postgres_restore_recovery_scheduler_outcome_uncertain"),
  );
  await assert.rejects(
    completion,
    schedulerCode("postgres_restore_recovery_scheduler_outcome_uncertain"),
  );
  assert.strictEqual(fixture.scheduler.stop(), completion);
});

test("observer failure terminates the scheduler through its completion", async () => {
  const fixture = createSchedulerFixture({
    onStep() {
      throw new Error("observer failed");
    },
  });
  const completion = fixture.scheduler.start();
  await assert.rejects(
    completion,
    schedulerCode("postgres_restore_recovery_scheduler_outcome_uncertain"),
  );
  assert.strictEqual(fixture.scheduler.stop(), completion);
  assert.throws(
    () => fixture.scheduler.runStep({ signal: null }),
    schedulerCode("invalid_postgres_restore_recovery_scheduler_request"),
  );
});

test("observer cannot spoof scheduler error identity", async () => {
  const descriptor = Object.getOwnPropertyDescriptor(
    PostgresRestoreRecoverySchedulerError,
    Symbol.hasInstance,
  );
  const escaped = new Error("poisoned Symbol.hasInstance escaped");
  const fixture = createSchedulerFixture({
    onStep() {
      Object.defineProperty(
        PostgresRestoreRecoverySchedulerError,
        Symbol.hasInstance,
        {
          configurable: true,
          value() {
            throw escaped;
          },
        },
      );
      throw new Error("observer failed");
    },
  });
  let observed;
  try {
    await assert.rejects(
      fixture.scheduler.start(),
      (error) => {
        observed = error;
        return (
          error !== escaped &&
          error?.code ===
            "postgres_restore_recovery_scheduler_outcome_uncertain"
        );
      },
    );
  } finally {
    if (descriptor === undefined) {
      delete PostgresRestoreRecoverySchedulerError[Symbol.hasInstance];
    } else {
      Object.defineProperty(
        PostgresRestoreRecoverySchedulerError,
        Symbol.hasInstance,
        descriptor,
      );
    }
  }
  assert.equal(
    observed instanceof PostgresRestoreRecoverySchedulerError,
    true,
  );
});

test(
  "callback-time Promise pollution cannot admit an async observer",
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
    const observer = deferred();
    const restoration = deferred();
    Object.defineProperty(restoration.promise, "constructor", {
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
      restoration.resolve();
    };
    const fixture = createSchedulerFixture({
      onStep() {
        Object.defineProperty(Promise.prototype, "constructor", {
          ...constructorDescriptor,
          value: speciesHolder,
        });
        Object.defineProperty(Promise.prototype, "then", {
          ...thenDescriptor,
          value(onFulfilled) {
            poisonedThenCalls += 1;
            if (typeof onFulfilled === "function") onFulfilled();
          },
        });
        queueMicrotask(restore);
        return observer.promise;
      },
    });
    const completion = fixture.scheduler.start();
    try {
      await restoration.promise;
    } finally {
      restore();
    }
    assert.equal(poisonedThenCalls, 0);
    assert.equal(fixture.steps.length, 1);
    await assert.rejects(
      completion,
      schedulerCode(
        "postgres_restore_recovery_scheduler_outcome_uncertain",
      ),
    );

    observer.reject(new Error("late observer rejection"));
    await nextTurn();
    await nextTurn();
    const stop = fixture.scheduler.stop();
    assert.strictEqual(stop, completion);
    await assert.rejects(
      stop,
      schedulerCode(
        "postgres_restore_recovery_scheduler_outcome_uncertain",
      ),
    );
  },
);

test(
  "observer cannot wait on a completion-derived Promise",
  { timeout: 1_000 },
  async () => {
    let completion;
    const fixture = createSchedulerFixture({
      onStep() {
        return completion.then(() => undefined);
      },
    });
    completion = fixture.scheduler.start();
    await assert.rejects(
      completion,
      schedulerCode(
        "postgres_restore_recovery_scheduler_outcome_uncertain",
      ),
    );
    assert.strictEqual(fixture.scheduler.stop(), completion);
  },
);

test(
  "observer cannot wait on an active-step-derived Promise",
  { timeout: 1_000 },
  async () => {
    let activeStep;
    const fixture = createSchedulerFixture({
      onStep() {
        return activeStep.then(() => undefined);
      },
    });
    const completion = fixture.scheduler.start();
    activeStep = fixture.scheduler.runStep({ signal: null });
    await assert.rejects(
      activeStep,
      schedulerCode(
        "postgres_restore_recovery_scheduler_outcome_uncertain",
      ),
    );
    await assert.rejects(
      completion,
      schedulerCode(
        "postgres_restore_recovery_scheduler_outcome_uncertain",
      ),
    );
    assert.strictEqual(fixture.scheduler.stop(), completion);
  },
);

test("observer cannot create a completion cycle by returning stop", async () => {
  let scheduler;
  const fixture = createSchedulerFixture({
    onStep() {
      return scheduler.stop();
    },
  });
  scheduler = fixture.scheduler;
  const completion = scheduler.start();
  await assert.rejects(
    completion,
    schedulerCode("postgres_restore_recovery_scheduler_outcome_uncertain"),
  );
  assert.strictEqual(scheduler.stop(), completion);
});

test("async observer descendants cannot reenter the scheduler", async () => {
  let scheduler;
  const fixture = createSchedulerFixture({
    onStep() {
      return Promise.resolve().then(() => scheduler.stop());
    },
  });
  scheduler = fixture.scheduler;
  const completion = scheduler.start();
  await assert.rejects(
    completion,
    schedulerCode("postgres_restore_recovery_scheduler_outcome_uncertain"),
  );
  assert.strictEqual(scheduler.stop(), completion);
});

test("synchronous lower-layer reentry observes stable scheduler promises", async () => {
  const fixture = createSchedulerFixture();
  const pool = fixture.runnerFixture.lifecycleFixture.pool;
  let reenteredStart;
  let reenteredStep;
  let reenteredStop;
  pool.onConnect = () => {
    pool.onConnect = null;
    reenteredStart = fixture.scheduler.start();
    reenteredStep = fixture.scheduler.runStep({ signal: null });
    reenteredStop = fixture.scheduler.stop();
  };

  const completion = fixture.scheduler.start();
  assert.strictEqual(reenteredStart, completion);
  assert.strictEqual(reenteredStop, completion);
  const step = await reenteredStep;
  assert.equal(step.status, "completed");
  assert.equal(step.recovery.status, "aborted");
  assert.deepEqual(await completion, freezeRecord({ status: "stopped" }));
  assert.equal(fixture.steps.length, 1);
});

test("factory and request boundaries require authentic exact collaborators", async () => {
  const runnerFixture = createRunnerFixture();
  const scheduler = createPostgresRestoreRecoveryScheduler({
    intervalMilliseconds: 10,
    onStep() {},
    runner: runnerFixture.runner,
  });
  assert.equal(isPostgresRestoreRecoveryScheduler(scheduler), true);
  assert.deepEqual(Reflect.ownKeys(scheduler), ["runStep", "start", "stop"]);
  assert.equal(Object.isFrozen(scheduler), true);

  let proxyTraps = 0;
  const revoked = Proxy.revocable({}, {
    getPrototypeOf() {
      proxyTraps += 1;
      throw new Error("must not run");
    },
  });
  revoked.revoke();
  assert.equal(isPostgresRestoreRecoveryScheduler(revoked.proxy), false);
  assert.equal(proxyTraps, 0);

  for (const options of [
    {},
    {
      intervalMilliseconds: 0,
      onStep() {},
      runner: runnerFixture.runner,
    },
    {
      intervalMilliseconds: 10,
      onStep() {},
      runner: Object.freeze({ async runOnce() {} }),
    },
    {
      intervalMilliseconds: 10,
      onStep: function* onStep() {},
      runner: runnerFixture.runner,
    },
  ]) {
    assert.throws(
      () => createPostgresRestoreRecoveryScheduler(options),
      schedulerCode("invalid_postgres_restore_recovery_scheduler_options"),
    );
  }

  assert.throws(
    () => scheduler.runStep({ signal: null }),
    schedulerCode("invalid_postgres_restore_recovery_scheduler_request"),
  );
  const stopped = scheduler.stop();
  assert.deepEqual(await stopped, freezeRecord({ status: "stopped" }));
  assert.strictEqual(scheduler.stop(), stopped);
  assert.throws(
    () => scheduler.start("extra"),
    schedulerCode("invalid_postgres_restore_recovery_scheduler_request"),
  );
});
