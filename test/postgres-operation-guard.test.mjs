import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import test from "node:test";

import {
  POSTGRES_OPERATION_GUARD_NAMESPACE,
  PostgresOperationGuard,
  PostgresOperationGuardError,
} from "../src/postgres-operation-guard.mjs";

class AdvisoryLockManager {
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

class FakeClient {
  constructor({
    manager,
    pid,
    failPreReset = false,
    failPostReset = false,
    failUnlock = false,
    failRelease = false,
    heldProbeGate = undefined,
    heldProbeGateCall = undefined,
    loseTryLockResponse = false,
  }) {
    this.connectionLost = false;
    this.failPreReset = failPreReset;
    this.failPostReset = failPostReset;
    this.failRelease = failRelease;
    this.failUnlock = failUnlock;
    this.heldProbeGate = heldProbeGate;
    this.heldProbeGateCall = heldProbeGateCall;
    this.loseTryLockResponse = loseTryLockResponse;
    this.manager = manager;
    this.pid = pid;
    this.queries = [];
    this.releaseCalls = [];
    this.resetCount = 0;
    this.heldProbeCount = 0;
  }

  async query(...args) {
    this.queries.push(args);
    const query = args[0];
    const text = typeof query === "string" ? query : query?.text;

    if (text === "DISCARD ALL") {
      this.resetCount += 1;
      if (
        (this.resetCount === 1 && this.failPreReset) ||
        (this.resetCount === 2 && this.failPostReset) ||
        this.connectionLost
      ) {
        throw new Error("reset failed");
      }
      this.manager.releaseAll(this);
      return { command: "DISCARD", rows: [] };
    }

    if (this.connectionLost) throw new Error("connection lost");

    const [key] =
      typeof query === "string" ? args[1] : query.values;
    if (text.includes("pg_try_advisory_lock")) {
      const acquired = this.manager.tryAcquire(key, this);
      if (this.loseTryLockResponse) {
        throw new Error("try-lock response lost");
      }
      return {
        command: "SELECT",
        rows: [
          {
            backend_pid: this.pid,
            acquired,
          },
        ],
      };
    }
    if (text.includes("FROM pg_catalog.pg_locks")) {
      this.heldProbeCount += 1;
      if (this.heldProbeCount === this.heldProbeGateCall) {
        await this.heldProbeGate.promise;
      }
      return {
        command: "SELECT",
        rows: [
          {
            backend_pid: this.pid,
            lock_held: this.manager.isHeld(key, this),
          },
        ],
      };
    }
    if (text.includes("pg_advisory_unlock")) {
      if (this.failUnlock) throw new Error("unlock failed");
      return {
        command: "SELECT",
        rows: [
          {
            backend_pid: this.pid,
            unlocked: this.manager.unlock(key, this),
          },
        ],
      };
    }
    throw new Error(`unexpected query: ${text}`);
  }

  async release(...args) {
    this.releaseCalls.push(args);
    if (args.length === 1) this.manager.releaseAll(this);
    if (this.failRelease) throw new Error("release failed");
  }
}

class FakePool {
  constructor(...clients) {
    this.clients = [...clients];
    this.connectCalls = 0;
  }

  async connect() {
    this.connectCalls += 1;
    assert.notEqual(this.clients.length, 0, "unexpected pool.connect()");
    return this.clients.shift();
  }
}

function clientOptions(manager, pid, overrides = {}) {
  return { manager, pid, ...overrides };
}

function makeGuard(client) {
  const pool = new FakePool(client);
  return {
    guard: new PostgresOperationGuard({ dedicatedPool: pool }),
    pool,
  };
}

function queryTexts(client) {
  return client.queries.map(([query]) =>
    typeof query === "string" ? query : query.text,
  );
}

function lockKeyFor(operationId) {
  return createHash("sha256")
    .update(POSTGRES_OPERATION_GUARD_NAMESPACE, "utf8")
    .update("\0", "utf8")
    .update(operationId, "utf8")
    .digest()
    .readBigInt64BE(0)
    .toString();
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

async function assertGuardError(promise, code) {
  await assert.rejects(promise, (error) => {
    assert.ok(error instanceof PostgresOperationGuardError);
    assert.equal(error.name, "PostgresOperationGuardError");
    assert.equal(error.code, code);
    assert.equal(error.retryable, false);
    assert.equal(Object.isFrozen(error), true);
    assert.equal("cause" in error, false);
    assert.equal("client" in error, false);
    assert.equal("query" in error, false);
    assert.equal("key" in error, false);
    return true;
  });
}

test("validates exact options and requests before acquiring a client", async () => {
  const manager = new AdvisoryLockManager();
  const client = new FakeClient(clientOptions(manager, 101));
  const pool = new FakePool(client);

  assert.throws(
    () => new PostgresOperationGuard(),
    (error) =>
      error instanceof PostgresOperationGuardError &&
      error.code === "invalid_postgres_operation_guard_options",
  );
  assert.throws(
    () =>
      new PostgresOperationGuard({
        dedicatedPool: pool,
        extra: true,
      }),
    (error) =>
      error instanceof PostgresOperationGuardError &&
      error.code === "invalid_postgres_operation_guard_options",
  );

  const guard = new PostgresOperationGuard({ dedicatedPool: pool });
  assert.equal(Object.isFrozen(guard), true);
  await assertGuardError(
    guard.runExclusive("../invalid", async () => {}),
    "invalid_postgres_operation_guard_request",
  );
  await assertGuardError(
    guard.runExclusive("valid-operation", function* callback() {}),
    "invalid_postgres_operation_guard_request",
  );
  assert.equal(pool.connectCalls, 0);
});

test("destroys a malformed acquired client and fails closed", async () => {
  const releaseCalls = [];
  const client = {
    query: undefined,
    async release(...args) {
      releaseCalls.push(args);
    },
  };
  const pool = new FakePool(client);
  const guard = new PostgresOperationGuard({ dedicatedPool: pool });
  let callbackCalls = 0;

  await assertGuardError(
    guard.runExclusive("malformed-client", async () => {
      callbackCalls += 1;
    }),
    "postgres_operation_guard_outcome_uncertain",
  );

  assert.equal(callbackCalls, 0);
  assert.equal(pool.connectCalls, 1);
  assert.equal(releaseCalls.length, 1);
  assert.equal(releaseCalls[0].length, 1);
  assert.ok(releaseCalls[0][0] instanceof PostgresOperationGuardError);
});

test("returns callback result identity and exposes only a frozen lock probe", async () => {
  const manager = new AdvisoryLockManager();
  const client = new FakeClient(clientOptions(manager, 102));
  const { guard } = makeGuard(client);
  const expected = Object.freeze({ exact: "identity" });
  let callbackCalls = 0;

  const result = await guard.runExclusive(
    "checkpoint:operation-001",
    async (probe) => {
      callbackCalls += 1;
      assert.equal(Object.isFrozen(probe), true);
      assert.deepEqual(Reflect.ownKeys(probe), ["assertHeld"]);
      assert.equal(typeof probe.assertHeld, "function");
      const health = probe.assertHeld();
      assert.ok(health instanceof Promise);
      assert.equal(await health, undefined);
      await Promise.resolve();
      return expected;
    },
  );

  assert.strictEqual(result, expected);
  assert.equal(callbackCalls, 1);
  assert.equal(client.resetCount, 2);
  assert.deepEqual(client.releaseCalls, [[]]);
  assert.equal(manager.holders.size, 0);
  const tryLock = client.queries.find(([query]) =>
    query?.text?.includes("pg_try_advisory_lock"),
  );
  assert.equal(tryLock.length, 1);
  assert.equal(tryLock[0].queryMode, "extended");
  assert.equal(Object.isFrozen(tryLock[0]), true);
  assert.deepEqual(tryLock[0].values, [
    lockKeyFor("checkpoint:operation-001"),
  ]);
  assert.equal(Object.isFrozen(tryLock[0].values), true);
  assert.equal(
    queryTexts(client).some((text) =>
      /^(?:BEGIN|COMMIT|ROLLBACK)(?:\s|$)/u.test(text),
    ),
    false,
  );
});

test(
  "probe drain survives callback poisoning of mutable Set and Promise surfaces",
  { timeout: 2_000 },
  async () => {
    const manager = new AdvisoryLockManager();
    const heldProbeGate = deferred();
    const client = new FakeClient(
      clientOptions(manager, 103, {
        heldProbeGate,
        heldProbeGateCall: 2,
      }),
    );
    const { guard } = makeGuard(client);
    const allSettledDescriptor = Object.getOwnPropertyDescriptor(
      Promise,
      "allSettled",
    );
    const setSizeDescriptor = Object.getOwnPropertyDescriptor(
      Set.prototype,
      "size",
    );

    try {
      const run = guard.runExclusive(
        "intrinsic-poisoning",
        async ({ assertHeld }) => {
          void assertHeld();
          Object.defineProperty(Promise, "allSettled", {
            ...allSettledDescriptor,
            value() {
              throw new Error("poisoned Promise.allSettled");
            },
          });
          Object.defineProperty(Set.prototype, "size", {
            ...setSizeDescriptor,
            get() {
              throw new Error("poisoned Set.prototype.size");
            },
          });
          setImmediate(() => heldProbeGate.resolve());
          return "completed";
        },
      );
      assert.equal(await run, "completed");
    } finally {
      Object.defineProperty(
        Promise,
        "allSettled",
        allSettledDescriptor,
      );
      Object.defineProperty(Set.prototype, "size", setSizeDescriptor);
    }

    assert.equal(client.heldProbeCount, 3);
    assert.equal(manager.holders.size, 0);
  },
);

test("same operation ID is busy while the first callback holds the lock", async () => {
  const manager = new AdvisoryLockManager();
  const firstClient = new FakeClient(clientOptions(manager, 104));
  const secondClient = new FakeClient(clientOptions(manager, 105));
  const firstGuard = makeGuard(firstClient).guard;
  const secondGuard = makeGuard(secondClient).guard;
  const entered = deferred();
  const finish = deferred();
  let secondCallbackCalls = 0;

  const firstRun = firstGuard.runExclusive("same-operation", async () => {
    entered.resolve();
    await finish.promise;
    return "first";
  });
  await entered.promise;

  await assertGuardError(
    secondGuard.runExclusive("same-operation", async () => {
      secondCallbackCalls += 1;
    }),
    "postgres_operation_guard_busy",
  );
  assert.equal(secondCallbackCalls, 0);
  assert.deepEqual(secondClient.releaseCalls, [[]]);

  finish.resolve();
  assert.equal(await firstRun, "first");
  assert.equal(manager.holders.size, 0);
});

test(
  "different operation IDs can execute concurrently",
  { timeout: 2_000 },
  async () => {
    const manager = new AdvisoryLockManager();
    const firstGuard = makeGuard(
      new FakeClient(clientOptions(manager, 106)),
    ).guard;
    const secondGuard = makeGuard(
      new FakeClient(clientOptions(manager, 107)),
    ).guard;
    const bothEntered = deferred();
    const finish = deferred();
    let active = 0;
    let maximumActive = 0;

    const callback = async () => {
      active += 1;
      maximumActive = Math.max(maximumActive, active);
      if (active === 2) bothEntered.resolve();
      await finish.promise;
      active -= 1;
    };
    const runs = [
      firstGuard.runExclusive("first-operation", callback),
      secondGuard.runExclusive("second-operation", callback),
    ];

    await bothEntered.promise;
    finish.resolve();
    await Promise.all(runs);
    assert.equal(maximumActive, 2);
    assert.equal(manager.holders.size, 0);
  },
);

test("propagates callback rejection only after successful cleanup", async () => {
  const manager = new AdvisoryLockManager();
  const client = new FakeClient(clientOptions(manager, 108));
  const { guard } = makeGuard(client);
  const callbackError = new Error("callback failed");

  await assert.rejects(
    guard.runExclusive("callback-rejection", async () => {
      throw callbackError;
    }),
    (error) => error === callbackError,
  );

  assert.equal(client.resetCount, 2);
  assert.deepEqual(client.releaseCalls, [[]]);
  assert.equal(manager.holders.size, 0);
});

test("fails closed when a caught callback probe observes lock loss", async () => {
  const manager = new AdvisoryLockManager();
  const client = new FakeClient(clientOptions(manager, 109));
  const { guard } = makeGuard(client);

  await assertGuardError(
    guard.runExclusive("lost-lock", async ({ assertHeld }) => {
      manager.releaseAll(client);
      await assertGuardError(
        assertHeld(),
        "postgres_operation_guard_outcome_uncertain",
      );
      return "must not escape";
    }),
    "postgres_operation_guard_outcome_uncertain",
  );

  assert.equal(client.releaseCalls.length, 1);
  assert.equal(client.releaseCalls[0].length, 1);
  assert.ok(
    client.releaseCalls[0][0] instanceof PostgresOperationGuardError,
  );
});

test("fails closed and destroys the client after connection loss", async () => {
  const manager = new AdvisoryLockManager();
  const client = new FakeClient(clientOptions(manager, 110));
  const { guard } = makeGuard(client);

  await assertGuardError(
    guard.runExclusive("lost-connection", async () => {
      client.connectionLost = true;
    }),
    "postgres_operation_guard_outcome_uncertain",
  );

  assert.equal(client.releaseCalls.length, 1);
  assert.equal(client.releaseCalls[0].length, 1);
  assert.ok(
    client.releaseCalls[0][0] instanceof PostgresOperationGuardError,
  );
});

test("unlock failure overrides a successful callback and destroys the client", async () => {
  const manager = new AdvisoryLockManager();
  const client = new FakeClient(
    clientOptions(manager, 111, { failUnlock: true }),
  );
  const { guard } = makeGuard(client);

  await assertGuardError(
    guard.runExclusive("unlock-failure", async () => "success"),
    "postgres_operation_guard_outcome_uncertain",
  );

  assert.equal(client.resetCount, 2);
  assert.equal(client.releaseCalls[0].length, 1);
  assert.equal(manager.holders.size, 0);
});

test("lost try-lock response still unlocks by key and fails closed", async () => {
  const manager = new AdvisoryLockManager();
  const client = new FakeClient(
    clientOptions(manager, 112, { loseTryLockResponse: true }),
  );
  const { guard } = makeGuard(client);

  await assertGuardError(
    guard.runExclusive("lost-lock-response", async () => {
      assert.fail("callback must not run");
    }),
    "postgres_operation_guard_outcome_uncertain",
  );

  assert.equal(
    queryTexts(client).some((text) =>
      text.includes("pg_advisory_unlock"),
    ),
    true,
  );
  assert.equal(client.resetCount, 2);
  assert.equal(client.releaseCalls[0].length, 1);
  assert.equal(manager.holders.size, 0);
});

test("pre-lock reset failure destroys the client before trying the lock", async () => {
  const manager = new AdvisoryLockManager();
  const client = new FakeClient(
    clientOptions(manager, 113, { failPreReset: true }),
  );
  const { guard } = makeGuard(client);

  await assertGuardError(
    guard.runExclusive("pre-reset-failure", async () => {}),
    "postgres_operation_guard_outcome_uncertain",
  );

  assert.deepEqual(queryTexts(client), ["DISCARD ALL"]);
  assert.equal(client.releaseCalls[0].length, 1);
});

test("post-unlock reset failure destroys the client and fails closed", async () => {
  const manager = new AdvisoryLockManager();
  const client = new FakeClient(
    clientOptions(manager, 114, { failPostReset: true }),
  );
  const { guard } = makeGuard(client);

  await assertGuardError(
    guard.runExclusive("post-reset-failure", async () => "success"),
    "postgres_operation_guard_outcome_uncertain",
  );

  assert.equal(client.resetCount, 2);
  assert.equal(client.releaseCalls[0].length, 1);
  assert.equal(manager.holders.size, 0);
});

test("release failure makes the otherwise successful outcome uncertain", async () => {
  const manager = new AdvisoryLockManager();
  const client = new FakeClient(
    clientOptions(manager, 115, { failRelease: true }),
  );
  const { guard } = makeGuard(client);

  await assertGuardError(
    guard.runExclusive("release-failure", async () => "success"),
    "postgres_operation_guard_outcome_uncertain",
  );

  assert.deepEqual(client.releaseCalls, [[]]);
  assert.equal(manager.holders.size, 0);
});
