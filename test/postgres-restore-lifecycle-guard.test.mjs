import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import test from "node:test";

import {
  POSTGRES_OPERATION_GUARD_NAMESPACE,
  POSTGRES_RESTORE_LIFECYCLE_GUARD_NAMESPACE,
  PostgresOperationGuard,
} from "../src/postgres-operation-guard.mjs";
import {
  POSTGRES_RESTORE_LIFECYCLE_LOCK_ID,
  PostgresRestoreLifecycleGuardError,
  assertPostgresRestoreLifecycleLeaseHeld,
  createPostgresRestoreLifecycleGuard,
  isPostgresRestoreLifecycleGuard,
  isPostgresRestoreLifecycleLease,
} from "../src/postgres-restore-lifecycle-guard.mjs";

class AdvisoryLockManager {
  constructor() {
    this.holders = new Map();
  }

  tryAcquire(key, client, mode) {
    let holders = this.holders.get(key);
    if (holders === undefined) {
      holders = { exclusive: new Set(), shared: new Set() };
      this.holders.set(key, holders);
    }
    if (mode === "shared") {
      for (const holder of holders.exclusive) {
        if (holder !== client) return false;
      }
      holders.shared.add(client);
      return true;
    }
    for (const holder of holders.exclusive) {
      if (holder !== client) return false;
    }
    for (const holder of holders.shared) {
      if (holder !== client) return false;
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

class FakeClient {
  constructor({
    manager,
    pid,
    failPreReset = false,
    failPostReset = false,
    failRelease = false,
    failUnlock = false,
    loseTryLockResponse = false,
  }) {
    this.connectionLost = false;
    this.failPostReset = failPostReset;
    this.failPreReset = failPreReset;
    this.failRelease = failRelease;
    this.failUnlock = failUnlock;
    this.heldProbeCount = 0;
    this.loseTryLockResponse = loseTryLockResponse;
    this.manager = manager;
    this.pid = pid;
    this.queries = [];
    this.releaseCalls = [];
    this.resetCount = 0;
  }

  query(...args) {
    this.queries.push(args);
    assert.equal(args.length, 1);
    const query = args[0];
    const text = query?.text;
    const callbackDescriptor = Object.getOwnPropertyDescriptor(
      query,
      "callback",
    );
    assert.equal(callbackDescriptor?.enumerable, true);
    assert.equal(Object.hasOwn(callbackDescriptor, "value"), true);
    const callback = callbackDescriptor.value;

    if (text === "DISCARD ALL") {
      this.resetCount += 1;
      if (
        (this.resetCount === 1 && this.failPreReset) ||
        (this.resetCount === 2 && this.failPostReset) ||
        this.connectionLost
      ) {
        callback(new Error("reset failed"));
        return undefined;
      }
      this.manager.releaseAll(this);
      callback(null, { command: "DISCARD", rows: [] });
      return undefined;
    }

    if (this.connectionLost) {
      callback(new Error("connection lost"));
      return undefined;
    }
    const values = query.values;
    const key = values[0];
    if (text.includes("pg_try_advisory_lock")) {
      const mode = text.includes("pg_try_advisory_lock_shared")
        ? "shared"
        : "exclusive";
      const acquired = this.manager.tryAcquire(key, this, mode);
      if (this.loseTryLockResponse) {
        callback(new Error("try-lock response lost"));
        return undefined;
      }
      callback(null, {
        command: "SELECT",
        rows: [{ acquired, backend_pid: this.pid }],
      });
      return undefined;
    }
    if (text.includes("FROM pg_catalog.pg_locks")) {
      const mode = text.includes("mode = 'ShareLock'")
        ? "shared"
        : "exclusive";
      this.heldProbeCount += 1;
      callback(null, {
        command: "SELECT",
        rows: [
          {
            backend_pid: this.pid,
            lock_held: this.manager.isHeld(key, this, mode),
          },
        ],
      });
      return undefined;
    }
    if (text.includes("pg_advisory_unlock")) {
      const mode = text.includes("pg_advisory_unlock_shared")
        ? "shared"
        : "exclusive";
      if (this.failUnlock) {
        callback(new Error("unlock failed"));
        return undefined;
      }
      callback(null, {
        command: "SELECT",
        rows: [
          {
            backend_pid: this.pid,
            unlocked: this.manager.unlock(key, this, mode),
          },
        ],
      });
      return undefined;
    }
    callback(new Error(`unexpected query: ${text}`));
    return undefined;
  }

  release(...args) {
    this.releaseCalls.push(args);
    if (args.length === 1) this.manager.releaseAll(this);
    if (this.failRelease) throw new Error("release failed");
    return undefined;
  }
}

class FakePool {
  constructor(...clients) {
    this.clients = [...clients];
    this.connectCalls = 0;
  }

  connect(callback) {
    this.connectCalls += 1;
    assert.notEqual(this.clients.length, 0, "unexpected pool.connect()");
    const client = this.clients.shift();
    let released = false;
    const release = (...args) => {
      const result = client.release(...args);
      if (!released && args.length === 0) {
        released = true;
        this.clients.push(client);
      }
      return result;
    };
    callback(null, client, release);
    return undefined;
  }
}

function createFixture(manager, pid, overrides = {}) {
  const foregroundClient = new FakeClient({
    manager,
    pid,
    ...overrides,
  });
  const recoveryClient = new FakeClient({
    manager,
    pid: pid + 10_000,
    ...overrides,
  });
  const foregroundPool = new FakePool(foregroundClient);
  const recoveryPool = new FakePool(recoveryClient);
  const foregroundOperationGuard = new PostgresOperationGuard({
    dedicatedPool: foregroundPool,
  });
  const recoveryOperationGuard = new PostgresOperationGuard({
    dedicatedPool: recoveryPool,
  });
  return {
    foregroundClient,
    foregroundOperationGuard,
    foregroundPool,
    lifecycle: createPostgresRestoreLifecycleGuard({
      foregroundOperationGuard,
      recoveryOperationGuard,
    }),
    recoveryClient,
    recoveryOperationGuard,
    recoveryPool,
  };
}

function clientForMode(fixture, mode) {
  return mode === "foreground"
    ? fixture.foregroundClient
    : fixture.recoveryClient;
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

function lockKey() {
  return createHash("sha256")
    .update(POSTGRES_RESTORE_LIFECYCLE_GUARD_NAMESPACE, "utf8")
    .update("\0", "utf8")
    .update(POSTGRES_RESTORE_LIFECYCLE_LOCK_ID, "utf8")
    .digest()
    .readBigInt64BE(0)
    .toString();
}

function ordinaryCollisionLockKey() {
  return createHash("sha256")
    .update(POSTGRES_OPERATION_GUARD_NAMESPACE, "utf8")
    .update("\0", "utf8")
    .update(POSTGRES_RESTORE_LIFECYCLE_LOCK_ID, "utf8")
    .digest()
    .readBigInt64BE(0)
    .toString();
}

function queryTexts(client) {
  return client.queries.map(([query]) => query.text);
}

async function assertLifecycleError(promise, code) {
  await assert.rejects(promise, (error) => {
    assert.ok(error instanceof PostgresRestoreLifecycleGuardError);
    assert.equal(error.name, "PostgresRestoreLifecycleGuardError");
    assert.equal(error.code, code);
    assert.equal(error.retryable, false);
    assert.equal(Object.isFrozen(error), true);
    assert.equal("operationGuard" in error, false);
    assert.equal("foregroundOperationGuard" in error, false);
    assert.equal("recoveryOperationGuard" in error, false);
    assert.equal("probe" in error, false);
    assert.equal("lease" in error, false);
    return true;
  });
}

test("creates an exact branded facade only from distinct real operation guard pools", () => {
  const manager = new AdvisoryLockManager();
  const fixture = createFixture(manager, 201);
  assert.equal(isPostgresRestoreLifecycleGuard(fixture.lifecycle), true);
  assert.equal(Object.getPrototypeOf(fixture.lifecycle), null);
  assert.equal(Object.isFrozen(fixture.lifecycle), true);
  assert.deepEqual(Reflect.ownKeys(fixture.lifecycle), [
    "runForeground",
    "runRecovery",
  ]);
  assert.equal(Object.isFrozen(fixture.lifecycle.runForeground), true);
  assert.equal(Object.isFrozen(fixture.lifecycle.runRecovery), true);

  const structuralGuard = Object.freeze({
    runRestoreLifecycleExclusive() {},
    runRestoreLifecycleShared() {},
  });
  assert.throws(
    () =>
      createPostgresRestoreLifecycleGuard({
        foregroundOperationGuard: structuralGuard,
        recoveryOperationGuard: structuralGuard,
      }),
    (error) =>
      error instanceof PostgresRestoreLifecycleGuardError &&
      error.code === "invalid_postgres_restore_lifecycle_guard_options",
  );
  assert.throws(
    () =>
      createPostgresRestoreLifecycleGuard({
        foregroundOperationGuard: fixture.foregroundOperationGuard,
        recoveryOperationGuard: fixture.recoveryOperationGuard,
        recoveryScopeId: "must-not-be-accepted",
      }),
    (error) =>
      error instanceof PostgresRestoreLifecycleGuardError &&
      error.code === "invalid_postgres_restore_lifecycle_guard_options",
  );

  assert.throws(
    () =>
      createPostgresRestoreLifecycleGuard({
        foregroundOperationGuard: fixture.foregroundOperationGuard,
        recoveryOperationGuard: fixture.foregroundOperationGuard,
      }),
    (error) =>
      error instanceof PostgresRestoreLifecycleGuardError &&
      error.code === "invalid_postgres_restore_lifecycle_guard_options",
  );
  assert.equal(fixture.foregroundPool.connectCalls, 0);

  const samePoolRecoveryGuard = new PostgresOperationGuard({
    dedicatedPool: fixture.foregroundPool,
  });
  assert.throws(
    () =>
      createPostgresRestoreLifecycleGuard({
        foregroundOperationGuard: fixture.foregroundOperationGuard,
        recoveryOperationGuard: samePoolRecoveryGuard,
      }),
    (error) =>
      error instanceof PostgresRestoreLifecycleGuardError &&
      error.code === "invalid_postgres_restore_lifecycle_guard_options",
  );
  assert.equal(fixture.foregroundPool.connectCalls, 0);
});

test("foreground callbacks overlap under shared locks", async () => {
  const manager = new AdvisoryLockManager();
  const first = createFixture(manager, 202).lifecycle;
  const second = createFixture(manager, 203).lifecycle;
  const bothEntered = deferred();
  const finish = deferred();
  let active = 0;
  let maximumActive = 0;

  const callback = async (lease, complete) => {
    assert.equal(
      isPostgresRestoreLifecycleLease(lease, "foreground"),
      true,
    );
    active += 1;
    maximumActive = Math.max(maximumActive, active);
    if (active === 2) bothEntered.resolve();
    await finish.promise;
    active -= 1;
    return complete(undefined);
  };
  const runs = [first.runForeground(callback), second.runForeground(callback)];
  await bothEntered.promise;
  finish.resolve();
  await Promise.all(runs);

  assert.equal(maximumActive, 2);
  assert.equal(manager.holders.size, 0);
});

for (const scenario of [
  ["foreground", "recovery", "shared/exclusive"],
  ["recovery", "foreground", "exclusive/shared"],
  ["recovery", "recovery", "exclusive/exclusive"],
]) {
  const [firstMode, secondMode, name] = scenario;
  test(`${name} contention reports lifecycle busy`, async () => {
    const manager = new AdvisoryLockManager();
    const first = createFixture(manager, 204).lifecycle;
    const secondFixture = createFixture(manager, 205);
    const entered = deferred();
    const finish = deferred();
    const firstMethod =
      firstMode === "foreground" ? "runForeground" : "runRecovery";
    const secondMethod =
      secondMode === "foreground" ? "runForeground" : "runRecovery";
    let secondCallbackCalls = 0;

    const firstRun = first[firstMethod](async (_lease, complete) => {
      entered.resolve();
      await finish.promise;
      return complete(undefined);
    });
    await entered.promise;
    await assertLifecycleError(
      secondFixture.lifecycle[secondMethod](async (_lease, complete) => {
        secondCallbackCalls += 1;
        return complete(undefined);
      }),
      "postgres_restore_lifecycle_guard_busy",
    );
    assert.equal(secondCallbackCalls, 0);
    assert.deepEqual(
      clientForMode(secondFixture, secondMode).releaseCalls,
      [[]],
    );
    finish.resolve();
    await firstRun;
    assert.equal(manager.holders.size, 0);
  });
}

test(
  "same facade recovery reaches its pool and reports busy while foreground remains held",
  { timeout: 2_000 },
  async () => {
    const manager = new AdvisoryLockManager();
    const fixture = createFixture(manager, 229);
    const foregroundEntered = deferred();
    const finishForeground = deferred();
    let foregroundCompleted = false;
    let recoveryCallbackCalls = 0;
    let timeout;

    const foregroundRun = fixture.lifecycle.runForeground(
      async (_lease, complete) => {
        foregroundEntered.resolve();
        await finishForeground.promise;
        foregroundCompleted = true;
        return complete(undefined);
      },
    );
    await foregroundEntered.promise;
    assert.equal(fixture.foregroundPool.clients.length, 0);
    assert.equal(fixture.recoveryPool.clients.length, 1);

    try {
      const recoveryBusy = assertLifecycleError(
        fixture.lifecycle.runRecovery(async (_lease, complete) => {
          recoveryCallbackCalls += 1;
          return complete(undefined);
        }),
        "postgres_restore_lifecycle_guard_busy",
      );
      const boundedFailure = new Promise((_, reject) => {
        timeout = setTimeout(
          () => reject(new Error("recovery lock attempt did not settle")),
          500,
        );
      });
      await Promise.race([recoveryBusy, boundedFailure]);

      assert.equal(recoveryCallbackCalls, 0);
      assert.equal(foregroundCompleted, false);
      assert.equal(fixture.foregroundPool.connectCalls, 1);
      assert.equal(fixture.recoveryPool.connectCalls, 1);
      assert.equal(fixture.foregroundPool.clients.length, 0);
      assert.equal(fixture.recoveryPool.clients.length, 1);
      assert.equal(
        fixture.recoveryClient.queries.some(([query]) =>
          query?.text?.includes("pg_try_advisory_lock("),
        ),
        true,
      );
      assert.equal(
        manager.isHeld(
          lockKey(),
          fixture.foregroundClient,
          "shared",
        ),
        true,
      );
    } finally {
      clearTimeout(timeout);
      finishForeground.resolve();
      await foregroundRun;
    }

    assert.equal(foregroundCompleted, true);
    assert.equal(fixture.foregroundPool.clients.length, 1);
    assert.equal(manager.holders.size, 0);
  },
);

for (const [lifecycleMode, lifecycleMethod] of [
  ["foreground", "runForeground"],
  ["recovery", "runRecovery"],
]) {
  test(
    `ordinary collision ID remains independent during ${lifecycleMode} lifecycle lock`,
    async () => {
      const manager = new AdvisoryLockManager();
      const lifecycleFixture = createFixture(manager, 230);
      const ordinaryFixture = createFixture(manager, 231);
      let ordinaryCallbackCalls = 0;

      const result = await lifecycleFixture.lifecycle[lifecycleMethod](
        async (lease, completeLifecycle) => {
          await assertPostgresRestoreLifecycleLeaseHeld(
            lease,
            lifecycleMode,
          );
          const ordinaryResult =
            await ordinaryFixture.foregroundOperationGuard.runExclusive(
              POSTGRES_RESTORE_LIFECYCLE_LOCK_ID,
              async (probe, completeOperation) => {
                ordinaryCallbackCalls += 1;
                await probe.assertHeld();
                return completeOperation("ordinary-complete");
              },
            );
          await assertPostgresRestoreLifecycleLeaseHeld(
            lease,
            lifecycleMode,
          );
          return completeLifecycle(ordinaryResult);
        },
      );

      const lifecycleTry = clientForMode(
        lifecycleFixture,
        lifecycleMode,
      ).queries.find(([query]) =>
        query?.text?.includes("pg_try_advisory_lock"),
      )[0];
      const ordinaryTry = ordinaryFixture.foregroundClient.queries.find(
        ([query]) => query?.text?.includes("pg_try_advisory_lock("),
      )[0];
      assert.equal(result, "ordinary-complete");
      assert.equal(ordinaryCallbackCalls, 1);
      assert.deepEqual(lifecycleTry.values, [lockKey()]);
      assert.deepEqual(ordinaryTry.values, [ordinaryCollisionLockKey()]);
      assert.notEqual(lifecycleTry.values[0], ordinaryTry.values[0]);
      assert.equal(manager.holders.size, 0);
    },
  );
}

test("foreground and recovery SQL use one fixed derived key and exact modes", async () => {
  const manager = new AdvisoryLockManager();
  const foreground = createFixture(manager, 206);
  const recovery = createFixture(manager, 207);
  await foreground.lifecycle.runForeground(async (lease, complete) => {
    await assertPostgresRestoreLifecycleLeaseHeld(lease, "foreground");
    return complete(undefined);
  });
  await recovery.lifecycle.runRecovery(async (lease, complete) => {
    await assertPostgresRestoreLifecycleLeaseHeld(lease, "recovery");
    return complete(undefined);
  });

  const foregroundTry = foreground.foregroundClient.queries.find(([query]) =>
    query?.text?.includes("pg_try_advisory_lock_shared"),
  )[0];
  const recoveryTry = recovery.recoveryClient.queries.find(([query]) =>
    query?.text?.includes("pg_try_advisory_lock("),
  )[0];
  assert.deepEqual(foregroundTry.values, [lockKey()]);
  assert.deepEqual(recoveryTry.values, [lockKey()]);
  assert.equal(foregroundTry.queryMode, "extended");
  assert.equal(recoveryTry.queryMode, "extended");
  assert.equal(
    queryTexts(foreground.foregroundClient).some((text) =>
      text.includes("mode = 'ShareLock'"),
    ),
    true,
  );
  assert.equal(
    queryTexts(foreground.foregroundClient).some((text) =>
      text.includes("pg_advisory_unlock_shared"),
    ),
    true,
  );
  assert.equal(
    queryTexts(recovery.recoveryClient).some((text) =>
      text.includes("mode = 'ExclusiveLock'"),
    ),
    true,
  );
  assert.equal(
    queryTexts(recovery.recoveryClient).some((text) =>
      text.includes("pg_advisory_unlock(") &&
      !text.includes("pg_advisory_unlock_shared"),
    ),
    true,
  );
});

test("lease is opaque, callback-bound, probed, and invalid after closure", async () => {
  const manager = new AdvisoryLockManager();
  const fixture = createFixture(manager, 208);
  const expected = Object.freeze({ identity: "preserved" });
  let capturedLease;

  const result = await fixture.lifecycle.runForeground(
    async (lease, complete) => {
      capturedLease = lease;
      assert.equal(Object.getPrototypeOf(lease), null);
      assert.equal(Object.isFrozen(lease), true);
      assert.deepEqual(Reflect.ownKeys(lease), ["mode"]);
      assert.equal(lease.mode, "foreground");
      assert.equal(
        isPostgresRestoreLifecycleLease(lease, "foreground"),
        true,
      );
      assert.equal(
        isPostgresRestoreLifecycleLease(lease, "recovery"),
        false,
      );
      assert.equal(
        await assertPostgresRestoreLifecycleLeaseHeld(
          lease,
          "foreground",
        ),
        undefined,
      );
      return complete(expected);
    },
  );

  assert.strictEqual(result, expected);
  assert.equal(fixture.foregroundClient.heldProbeCount, 3);
  assert.equal(
    isPostgresRestoreLifecycleLease(capturedLease, "foreground"),
    false,
  );
  await assertLifecycleError(
    assertPostgresRestoreLifecycleLeaseHeld(capturedLease, "foreground"),
    "postgres_restore_lifecycle_guard_outcome_uncertain",
  );
});

test("wrong-mode and cross-callback leases fail closed", async () => {
  const manager = new AdvisoryLockManager();
  const wrongMode = createFixture(manager, 209).lifecycle;
  await assertLifecycleError(
    wrongMode.runForeground(async (lease, complete) => {
      await assertLifecycleError(
        assertPostgresRestoreLifecycleLeaseHeld(lease, "recovery"),
        "postgres_restore_lifecycle_guard_outcome_uncertain",
      );
      return complete("must not escape");
    }),
    "postgres_restore_lifecycle_guard_outcome_uncertain",
  );

  const first = createFixture(manager, 210).lifecycle;
  const second = createFixture(manager, 211).lifecycle;
  const firstEntered = deferred();
  const finishFirst = deferred();
  let firstLease;
  const firstRun = first.runForeground(async (lease, complete) => {
    firstLease = lease;
    firstEntered.resolve();
    await finishFirst.promise;
    return complete(undefined);
  });
  await firstEntered.promise;
  await assertLifecycleError(
    second.runForeground(async (ownLease, complete) => {
      assert.equal(
        isPostgresRestoreLifecycleLease(ownLease, "foreground"),
        true,
      );
      assert.equal(
        isPostgresRestoreLifecycleLease(firstLease, "foreground"),
        false,
      );
      await assertLifecycleError(
        assertPostgresRestoreLifecycleLeaseHeld(
          firstLease,
          "foreground",
        ),
        "postgres_restore_lifecycle_guard_outcome_uncertain",
      );
      return complete(undefined);
    }),
    "postgres_restore_lifecycle_guard_outcome_uncertain",
  );
  finishFirst.resolve();
  await firstRun;
});

test("callback rejection identity escapes only after lock cleanup", async () => {
  const manager = new AdvisoryLockManager();
  const fixture = createFixture(manager, 212);
  const callbackError = new Error("callback failed");
  await assert.rejects(
    fixture.lifecycle.runRecovery(async () => {
      throw callbackError;
    }),
    (error) => error === callbackError,
  );
  assert.equal(manager.holders.size, 0);
  assert.equal(fixture.recoveryClient.resetCount, 2);
  assert.deepEqual(fixture.recoveryClient.releaseCalls, [[]]);
});

test("caught lease probe loss still makes the lifecycle outcome uncertain", async () => {
  const manager = new AdvisoryLockManager();
  const fixture = createFixture(manager, 213);
  await assertLifecycleError(
    fixture.lifecycle.runForeground(async (lease, complete) => {
      manager.releaseAll(fixture.foregroundClient);
      await assertLifecycleError(
        assertPostgresRestoreLifecycleLeaseHeld(lease, "foreground"),
        "postgres_restore_lifecycle_guard_outcome_uncertain",
      );
      return complete("must not escape");
    }),
    "postgres_restore_lifecycle_guard_outcome_uncertain",
  );
  assert.equal(fixture.foregroundClient.releaseCalls.length, 1);
  assert.equal(fixture.foregroundClient.releaseCalls[0].length, 1);
});

for (const scenario of [
  {
    name: "connection loss",
    callback(fixture) {
      fixture.recoveryClient.connectionLost = true;
    },
  },
  {
    name: "unlock failure",
    overrides: { failUnlock: true },
  },
  {
    name: "post-lock reset failure",
    overrides: { failPostReset: true },
  },
  {
    name: "pre-lock reset failure",
    overrides: { failPreReset: true },
  },
  {
    name: "lost try-lock response",
    overrides: { loseTryLockResponse: true },
  },
  {
    name: "client release failure",
    overrides: { failRelease: true },
    releaseArgumentCount: 0,
  },
]) {
  test(`${scenario.name} maps to lifecycle uncertainty and fails closed`, async () => {
    const manager = new AdvisoryLockManager();
    const fixture = createFixture(manager, 214, scenario.overrides);
    await assertLifecycleError(
      fixture.lifecycle.runRecovery(async (_lease, complete) => {
        scenario.callback?.(fixture);
        return complete(undefined);
      }),
      "postgres_restore_lifecycle_guard_outcome_uncertain",
    );
    assert.equal(fixture.recoveryClient.releaseCalls.length, 1);
    assert.equal(
      fixture.recoveryClient.releaseCalls[0].length,
      scenario.releaseArgumentCount ?? 1,
    );
    assert.equal(manager.holders.size, 0);
  });
}

test("structural fakes, proxies, accessors, and generators are zero-trap rejected", async () => {
  const manager = new AdvisoryLockManager();
  const fixture = createFixture(manager, 215);
  let trapCalls = 0;
  const proxy = new Proxy(
    {},
    {
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
    },
  );
  assert.throws(
    () => createPostgresRestoreLifecycleGuard(proxy),
    (error) =>
      error instanceof PostgresRestoreLifecycleGuardError &&
      error.code === "invalid_postgres_restore_lifecycle_guard_options",
  );
  assert.equal(isPostgresRestoreLifecycleGuard(proxy), false);
  assert.equal(isPostgresRestoreLifecycleLease(proxy, "foreground"), false);

  const accessorOptions = Object.create(null);
  Object.defineProperties(accessorOptions, {
    foregroundOperationGuard: {
      enumerable: true,
      get() {
        trapCalls += 1;
        throw new Error("accessor must not run");
      },
    },
    recoveryOperationGuard: {
      enumerable: true,
      value: fixture.recoveryOperationGuard,
    },
  });
  assert.throws(
    () => createPostgresRestoreLifecycleGuard(accessorOptions),
    (error) =>
      error instanceof PostgresRestoreLifecycleGuardError &&
      error.code === "invalid_postgres_restore_lifecycle_guard_options",
  );

  const callbackProxy = new Proxy(function callback() {}, {
    apply() {
      trapCalls += 1;
      throw new Error("proxy callback must not run");
    },
  });
  await assertLifecycleError(
    fixture.lifecycle.runForeground(callbackProxy),
    "invalid_postgres_restore_lifecycle_guard_request",
  );
  await assertLifecycleError(
    fixture.lifecycle.runRecovery(function* callback() {}),
    "invalid_postgres_restore_lifecycle_guard_request",
  );
  assert.equal(fixture.foregroundPool.connectCalls, 0);
  assert.equal(fixture.recoveryPool.connectCalls, 0);
  assert.equal(trapCalls, 0);
});

test("unsafe callback values are rejected without thenable or proxy traps", async () => {
  const manager = new AdvisoryLockManager();
  const thenableFixture = createFixture(manager, 216);
  let thenCalls = 0;
  const thenable = Object.create(null, {
    then: {
      enumerable: true,
      get() {
        thenCalls += 1;
        throw new Error("then accessor must not run");
      },
    },
  });
  await assertLifecycleError(
    thenableFixture.lifecycle.runForeground(() => thenable),
    "postgres_restore_lifecycle_guard_outcome_uncertain",
  );
  assert.equal(thenCalls, 0);

  const proxyFixture = createFixture(manager, 217);
  let proxyTrapCalls = 0;
  const proxyResult = new Proxy(
    {},
    {
      get() {
        proxyTrapCalls += 1;
        throw new Error("result proxy trap must not run");
      },
    },
  );
  await assertLifecycleError(
    proxyFixture.lifecycle.runForeground(() => proxyResult),
    "postgres_restore_lifecycle_guard_outcome_uncertain",
  );
  assert.equal(proxyTrapCalls, 0);
  assert.equal(manager.holders.size, 0);
});

test("Promise callbacks must fulfill their authentic completion carrier", async () => {
  const manager = new AdvisoryLockManager();
  const primitiveFixture = createFixture(manager, 218);
  await assertLifecycleError(
    primitiveFixture.lifecycle.runForeground(async () => "raw"),
    "postgres_restore_lifecycle_guard_outcome_uncertain",
  );

  const objectFixture = createFixture(manager, 219);
  await assertLifecycleError(
    objectFixture.lifecycle.runRecovery(async () =>
      Object.freeze({ raw: true }),
    ),
    "postgres_restore_lifecycle_guard_outcome_uncertain",
  );
  assert.equal(manager.holders.size, 0);
});

test("completion carriers are exact, per-run, and exactly once", async () => {
  const manager = new AdvisoryLockManager();
  const fixture = createFixture(manager, 220);
  let firstCarrier;
  let firstComplete;

  assert.equal(
    await fixture.lifecycle.runForeground((_lease, complete) => {
      firstComplete = complete;
      assert.equal(Object.isFrozen(complete), true);
      firstCarrier = complete("first");
      assert.equal(Object.getPrototypeOf(firstCarrier), null);
      assert.equal(Object.isFrozen(firstCarrier), true);
      return firstCarrier;
    }),
    "first",
  );

  await assertLifecycleError(
    fixture.lifecycle.runForeground(async (_lease, complete) => {
      complete("second");
      return firstCarrier;
    }),
    "postgres_restore_lifecycle_guard_outcome_uncertain",
  );
  assert.throws(
    () => firstComplete("late"),
    (error) =>
      error?.code === "postgres_operation_guard_outcome_uncertain",
  );

  await assertLifecycleError(
    fixture.lifecycle.runRecovery((_lease, complete) => {
      complete("first");
      return complete("second");
    }),
    "postgres_restore_lifecycle_guard_outcome_uncertain",
  );
  assert.equal(manager.holders.size, 0);
});

test(
  "inherited Object.prototype.then is never invoked",
  { concurrency: false },
  async () => {
    const manager = new AdvisoryLockManager();
    const fixture = createFixture(manager, 221);
    const thenDescriptor = Object.getOwnPropertyDescriptor(
      Object.prototype,
      "then",
    );
    let thenCalls = 0;
    let rejected = false;
    try {
      Object.defineProperty(Object.prototype, "then", {
        configurable: true,
        get() {
          thenCalls += 1;
          throw new Error("Object.prototype.then must not run");
        },
      });
      await fixture.lifecycle
        .runForeground(async (_lease, complete) =>
          complete(Object.freeze({ result: "unsafe" })),
        )
        .then(
          () => assert.fail("unsafe result must not fulfill"),
          (error) => {
            rejected = true;
            assert.ok(error instanceof PostgresRestoreLifecycleGuardError);
            assert.equal(
              error.code,
              "postgres_restore_lifecycle_guard_outcome_uncertain",
            );
          },
        );
    } finally {
      if (thenDescriptor === undefined) {
        delete Object.prototype.then;
      } else {
        Object.defineProperty(
          Object.prototype,
          "then",
          thenDescriptor,
        );
      }
    }
    assert.equal(rejected, true);
    assert.equal(thenCalls, 0);
    assert.equal(manager.holders.size, 0);
  },
);

test("captured Promise intrinsics survive callback-time poisoning", async () => {
  const manager = new AdvisoryLockManager();
  const fixture = createFixture(manager, 222);
  const allSettledDescriptor = Object.getOwnPropertyDescriptor(
    Promise,
    "allSettled",
  );
  const resolveDescriptor = Object.getOwnPropertyDescriptor(
    Promise,
    "resolve",
  );
  const thenDescriptor = Object.getOwnPropertyDescriptor(
    Promise.prototype,
    "then",
  );
  try {
    const result = await fixture.lifecycle.runForeground(() => {
      Object.defineProperty(Promise, "allSettled", {
        ...allSettledDescriptor,
        value() {
          throw new Error("poisoned Promise.allSettled");
        },
      });
      Object.defineProperty(Promise, "resolve", {
        ...resolveDescriptor,
        value() {
          throw new Error("poisoned Promise.resolve");
        },
      });
      Object.defineProperty(Promise.prototype, "then", {
        ...thenDescriptor,
        value() {
          throw new Error("poisoned Promise.prototype.then");
        },
      });
      return "completed";
    });
    assert.equal(result, "completed");
  } finally {
    Object.defineProperty(Promise, "allSettled", allSettledDescriptor);
    Object.defineProperty(Promise, "resolve", resolveDescriptor);
    Object.defineProperty(Promise.prototype, "then", thenDescriptor);
  }
  assert.equal(manager.holders.size, 0);
});

test("callbacks can return structurally protected foreign promises", async () => {
  const manager = new AdvisoryLockManager();
  const fixture = createFixture(manager, 223);
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
  let foreignThenCalls = 0;
  const expected = Object.freeze({ identity: "completed" });
  assert.strictEqual(
    await fixture.lifecycle.runForeground((_lease, complete) => {
      const carrier = complete(expected);
      const foreign = Promise.resolve(carrier);
      Object.defineProperties(foreign, {
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
            foreignThenCalls += 1;
            throw new Error("foreign own then must not run");
          },
          writable: false,
        },
      });
      return foreign;
    }),
    expected,
  );
  assert.equal(foreignThenCalls, 0);
  assert.equal(manager.holders.size, 0);
});
