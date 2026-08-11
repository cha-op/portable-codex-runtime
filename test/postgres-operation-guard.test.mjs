import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import test from "node:test";

import {
  POSTGRES_OPERATION_GUARD_NAMESPACE,
  PostgresOperationGuard,
  PostgresOperationGuardError,
} from "../src/postgres-operation-guard.mjs";

const setSizeGetter = Object.getOwnPropertyDescriptor(
  Set.prototype,
  "size",
).get;
const PromiseConstructor = Promise;
const promiseThenIntrinsic = Promise.prototype.then;
const testPromiseSpeciesHolder = Object.freeze(
  Object.create(null, {
    [Symbol.species]: {
      configurable: false,
      enumerable: false,
      value: PromiseConstructor,
      writable: false,
    },
  }),
);

function setSize(value) {
  return Reflect.apply(setSizeGetter, value, []);
}

class AdvisoryLockManager {
  constructor() {
    this.holders = new Map();
  }

  tryAcquire(key, client, mode = "exclusive") {
    let holders = this.holders.get(key);
    if (holders === undefined) {
      holders = {
        exclusive: new Set(),
        shared: new Set(),
      };
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

  isHeld(key, client, mode = "exclusive") {
    return this.holders.get(key)?.[mode]?.has(client) === true;
  }

  unlock(key, client, mode = "exclusive") {
    const holders = this.holders.get(key);
    if (holders?.[mode]?.delete(client) !== true) return false;
    if (setSize(holders.exclusive) === 0 && setSize(holders.shared) === 0) {
      this.holders.delete(key);
    }
    return true;
  }

  releaseAll(client) {
    for (const [key, holders] of this.holders) {
      holders.exclusive.delete(client);
      holders.shared.delete(client);
      if (
        setSize(holders.exclusive) === 0 &&
        setSize(holders.shared) === 0
      ) {
        this.holders.delete(key);
      }
    }
  }
}

class FakeClient {
  constructor({
    manager,
    pid,
    delayQueries = false,
    failPreReset = false,
    failPostReset = false,
    failUnlock = false,
    failRelease = false,
    heldProbeGate = undefined,
    heldProbeGateCall = undefined,
    heldProbeGates = undefined,
    loseTryLockResponse = false,
    queryReturnValue = undefined,
    releaseReturnValue = undefined,
  }) {
    this.connectionLost = false;
    this.delayQueries = delayQueries;
    this.failPreReset = failPreReset;
    this.failPostReset = failPostReset;
    this.failRelease = failRelease;
    this.failUnlock = failUnlock;
    this.heldProbeGate = heldProbeGate;
    this.heldProbeGateCall = heldProbeGateCall;
    this.heldProbeGates = heldProbeGates;
    this.loseTryLockResponse = loseTryLockResponse;
    this.manager = manager;
    this.pid = pid;
    this.queryReturnValue = queryReturnValue;
    this.queries = [];
    this.releaseCalls = [];
    this.releaseReturnValue = releaseReturnValue;
    this.resetCount = 0;
    this.heldProbeCount = 0;
  }

  query(...args) {
    this.queries.push(args);
    const query = args[0];
    const callback = query?.callback;
    const text = query?.text;
    const respond = (error, result, gate = undefined) => {
      if (gate === undefined) {
        if (this.delayQueries) {
          setImmediate(() => callback(error, result));
        } else {
          callback(error, result);
        }
      } else {
        observePromise(
          gate.promise,
          () => callback(error, result),
          (gateError) => callback(gateError),
        );
      }
      return this.queryReturnValue;
    };

    if (text === "DISCARD ALL") {
      this.resetCount += 1;
      if (
        (this.resetCount === 1 && this.failPreReset) ||
        (this.resetCount === 2 && this.failPostReset) ||
        this.connectionLost
      ) {
        return respond(new Error("reset failed"));
      }
      this.manager.releaseAll(this);
      return respond(null, { command: "DISCARD", rows: [] });
    }

    if (this.connectionLost) {
      return respond(new Error("connection lost"));
    }

    const values = query.values;
    const key = values[0];
    if (text.includes("pg_try_advisory_lock")) {
      const mode = text.includes("pg_try_advisory_lock_shared")
        ? "shared"
        : "exclusive";
      const acquired = this.manager.tryAcquire(key, this, mode);
      if (this.loseTryLockResponse) {
        return respond(new Error("try-lock response lost"));
      }
      return respond(null, {
        command: "SELECT",
        rows: [
          {
            backend_pid: this.pid,
            acquired,
          },
        ],
      });
    }
    if (text.includes("FROM pg_catalog.pg_locks")) {
      const mode = text.includes("mode = 'ShareLock'")
        ? "shared"
        : "exclusive";
      this.heldProbeCount += 1;
      const heldProbeGate =
        this.heldProbeGates?.get(this.heldProbeCount) ??
        (this.heldProbeCount === this.heldProbeGateCall
          ? this.heldProbeGate
          : undefined);
      return respond(null, {
        command: "SELECT",
        rows: [
          {
            backend_pid: this.pid,
            lock_held: this.manager.isHeld(key, this, mode),
          },
        ],
      }, heldProbeGate);
    }
    if (text.includes("pg_advisory_unlock")) {
      const mode = text.includes("pg_advisory_unlock_shared")
        ? "shared"
        : "exclusive";
      if (this.failUnlock) return respond(new Error("unlock failed"));
      return respond(null, {
        command: "SELECT",
        rows: [
          {
            backend_pid: this.pid,
            unlocked: this.manager.unlock(key, this, mode),
          },
        ],
      });
    }
    throw new Error(`unexpected query: ${text}`);
  }

  release(...args) {
    this.releaseCalls.push(args);
    if (args.length === 1) this.manager.releaseAll(this);
    if (this.failRelease) throw new Error("release failed");
    return this.releaseReturnValue;
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
    callback(null, client, client.release);
  }
}

class SynchronousFinalProbeClient {
  constructor({ finalProbe, finalProbeStarted, manager, pid }) {
    this.finalProbe = finalProbe;
    this.finalProbeStarted = finalProbeStarted;
    this.manager = manager;
    this.pid = pid;
    this.queries = [];
    this.releaseCalls = [];
    this.resetCount = 0;
    this.heldProbeCount = 0;
    this.finalProbeResult = undefined;
  }

  query(...args) {
    this.queries.push(args);
    const query = args[0];
    const callback = query.callback;
    const text = query.text;
    if (text === "DISCARD ALL") {
      this.resetCount += 1;
      this.manager.releaseAll(this);
      callback(null, { command: "DISCARD", rows: [] });
      return undefined;
    }

    const values = query.values;
    const key = values[0];
    if (text.includes("pg_try_advisory_lock")) {
      const mode = text.includes("pg_try_advisory_lock_shared")
        ? "shared"
        : "exclusive";
      callback(null, {
        command: "SELECT",
        rows: [
          {
            acquired: this.manager.tryAcquire(key, this, mode),
            backend_pid: this.pid,
          },
        ],
      });
      return undefined;
    }
    if (text.includes("FROM pg_catalog.pg_locks")) {
      const mode = text.includes("mode = 'ShareLock'")
        ? "shared"
        : "exclusive";
      this.heldProbeCount += 1;
      const result = {
        command: "SELECT",
        rows: [
          {
            backend_pid: this.pid,
            lock_held: this.manager.isHeld(key, this, mode),
          },
        ],
      };
      if (this.heldProbeCount === 2) {
        this.finalProbeResult = result;
        this.finalProbeStarted.resolve();
        observePromise(
          this.finalProbe.promise,
          (finalResult) => callback(null, finalResult),
          (error) => callback(error),
        );
        return undefined;
      }
      callback(null, result);
      return undefined;
    }
    if (text.includes("pg_advisory_unlock")) {
      const mode = text.includes("pg_advisory_unlock_shared")
        ? "shared"
        : "exclusive";
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
    throw new Error(`unexpected query: ${text}`);
  }

  release(...args) {
    this.releaseCalls.push(args);
    if (args.length === 1) this.manager.releaseAll(this);
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

function installPromisePrototypePoisoning(targetValues, forgedValue) {
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
        value: PromiseConstructor,
        writable: false,
      },
    }),
  );
  const targets = new WeakSet();
  for (let index = 0; index < targetValues.length; index += 1) {
    targets.add(targetValues[index]);
  }
  const calls = { otherThen: 0, targetThen: 0 };
  Object.defineProperty(Promise.prototype, "constructor", {
    configurable: true,
    enumerable: false,
    value: speciesHolder,
    writable: true,
  });
  Object.defineProperty(Promise.prototype, "then", {
    configurable: true,
    enumerable: false,
    value(onFulfilled, onRejected) {
      if (targets.has(this)) {
        calls.targetThen += 1;
        if (typeof onFulfilled === "function") {
          queueMicrotask(() => onFulfilled(forgedValue()));
        }
        return undefined;
      }
      calls.otherThen += 1;
      return Reflect.apply(promiseThenIntrinsic, this, [
        onFulfilled,
        onRejected,
      ]);
    },
    writable: true,
  });
  let restored = false;
  return {
    calls,
    restore() {
      if (restored) return;
      restored = true;
      Object.defineProperty(
        Promise.prototype,
        "constructor",
        constructorDescriptor,
      );
      Object.defineProperty(Promise.prototype, "then", thenDescriptor);
    },
  };
}

function observePromise(promise, onFulfilled, onRejected) {
  Reflect.apply(promiseThenIntrinsic, promise, [onFulfilled, onRejected]);
}

function protectTestPromise(promise) {
  Object.defineProperties(promise, {
    constructor: {
      configurable: false,
      enumerable: false,
      value: testPromiseSpeciesHolder,
      writable: false,
    },
    then: {
      configurable: false,
      enumerable: false,
      value: promiseThenIntrinsic,
      writable: false,
    },
  });
  return promise;
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
    release(...args) {
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

test("connect errors destroy callback-delivered clients through the third argument", async () => {
  const manager = new AdvisoryLockManager();
  const client = new FakeClient(clientOptions(manager, 151));
  let clientReleaseGets = 0;
  Object.defineProperty(client, "release", {
    configurable: true,
    get() {
      clientReleaseGets += 1;
      throw new Error("callback release must take precedence");
    },
  });
  const releaseCalls = [];
  const callbackRelease = (...args) => {
    releaseCalls.push(args);
  };
  const pool = {
    connect(callback) {
      callback(new Error("connect failed"), client, callbackRelease);
      return undefined;
    },
  };

  await assertGuardError(
    new PostgresOperationGuard({ dedicatedPool: pool }).runExclusive(
      "connect-error-destroy",
      () => "must-not-run",
    ),
    "postgres_operation_guard_outcome_uncertain",
  );
  assert.equal(clientReleaseGets, 0);
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
    async (probe, complete) => {
      callbackCalls += 1;
      assert.equal(Object.isFrozen(probe), true);
      assert.deepEqual(Reflect.ownKeys(probe), ["assertHeld"]);
      assert.equal(typeof probe.assertHeld, "function");
      const health = probe.assertHeld();
      assert.ok(health instanceof Promise);
      assert.equal(await health, undefined);
      await Promise.resolve();
      return complete(expected);
    },
  );

  assert.strictEqual(result, expected);
  assert.equal(callbackCalls, 1);
  assert.equal(client.resetCount, 2);
  assert.deepEqual(client.releaseCalls, [[]]);
  assert.equal(manager.holders.size, 0);
  for (const args of client.queries) {
    assert.equal(args.length, 1);
    const config = args[0];
    assert.equal(Object.getPrototypeOf(config), null);
    assert.equal(Object.isFrozen(config), true);
    assert.deepEqual(Reflect.ownKeys(config), [
      "callback",
      "queryMode",
      "text",
      "values",
    ]);
    assert.equal(typeof config.callback, "function");
    assert.equal(Object.isFrozen(config.callback), true);
    assert.equal(config.queryMode, "extended");
    assert.equal(Object.isFrozen(config.values), true);
  }
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

test("runShared uses the same key derivation with shared lock SQL", async () => {
  const manager = new AdvisoryLockManager();
  const client = new FakeClient(clientOptions(manager, 116));
  const { guard } = makeGuard(client);

  assert.equal(
    await guard.runShared("checkpoint:operation-001", async (probe, complete) => {
      await probe.assertHeld();
      return complete("shared");
    }),
    "shared",
  );

  const tryLock = client.queries.find(([query]) =>
    query?.text?.includes("pg_try_advisory_lock_shared"),
  );
  const probeQueries = client.queries.filter(([query]) =>
    query?.text?.includes("FROM pg_catalog.pg_locks"),
  );
  const unlock = client.queries.find(([query]) =>
    query?.text?.includes("pg_advisory_unlock_shared"),
  );
  assert.deepEqual(tryLock[0].values, [
    lockKeyFor("checkpoint:operation-001"),
  ]);
  assert.equal(
    probeQueries.every(([query]) => query.text.includes("mode = 'ShareLock'")),
    true,
  );
  assert.deepEqual(unlock[0].values, tryLock[0].values);
  assert.equal(manager.holders.size, 0);
});

test("synchronous raw returns and authentic completion carriers preserve identity", async () => {
  const rawManager = new AdvisoryLockManager();
  const rawGuard = makeGuard(
    new FakeClient(clientOptions(rawManager, 130)),
  ).guard;
  const rawExpected = Object.freeze({ mode: "sync-raw" });
  let staleComplete;
  assert.strictEqual(
    await rawGuard.runExclusive("sync-raw-result", (_probe, complete) => {
      assert.equal(Object.isFrozen(complete), true);
      staleComplete = complete;
      return rawExpected;
    }),
    rawExpected,
  );
  assert.throws(
    () => staleComplete("late"),
    (error) =>
      error instanceof PostgresOperationGuardError &&
      error.code === "postgres_operation_guard_outcome_uncertain",
  );

  const undefinedManager = new AdvisoryLockManager();
  const undefinedGuard = makeGuard(
    new FakeClient(clientOptions(undefinedManager, 159)),
  ).guard;
  assert.equal(
    await undefinedGuard.runExclusive("sync-undefined-result", () => undefined),
    undefined,
  );

  const carrierManager = new AdvisoryLockManager();
  const carrierGuard = makeGuard(
    new FakeClient(clientOptions(carrierManager, 131)),
  ).guard;
  const carrierExpected = Object.freeze({ mode: "sync-carrier" });
  let authenticCarrier;
  assert.strictEqual(
    await carrierGuard.runExclusive(
      "sync-carrier-result",
      (_probe, complete) => {
        authenticCarrier = complete(carrierExpected);
        assert.equal(Object.getPrototypeOf(authenticCarrier), null);
        assert.equal(Object.isFrozen(authenticCarrier), true);
        const keys = Reflect.ownKeys(authenticCarrier);
        assert.equal(keys.length, 2);
        assert.equal(keys[0], "value");
        assert.equal(typeof keys[1], "symbol");
        return authenticCarrier;
      },
    ),
    carrierExpected,
  );
});

test("Promise callbacks reject raw fulfillments without a completion carrier", async () => {
  const values = [undefined, "raw-primitive", Object.freeze({ raw: true })];
  for (let index = 0; index < values.length; index += 1) {
    const manager = new AdvisoryLockManager();
    const client = new FakeClient(clientOptions(manager, 132 + index));
    const { guard } = makeGuard(client);
    await assertGuardError(
      guard.runExclusive(`async-raw-${index}`, async () => values[index]),
      "postgres_operation_guard_outcome_uncertain",
    );
    assert.deepEqual(client.releaseCalls, [[]]);
    assert.equal(manager.holders.size, 0);
  }
});

test("rejects double, stale, and structural completion carriers", async () => {
  const sourceManager = new AdvisoryLockManager();
  const sourceGuard = makeGuard(
    new FakeClient(clientOptions(sourceManager, 135)),
  ).guard;
  let authenticCarrier;
  assert.equal(
    await sourceGuard.runExclusive(
      "carrier-source",
      (_probe, complete) => {
        authenticCarrier = complete("source");
        return authenticCarrier;
      },
    ),
    "source",
  );

  const staleManager = new AdvisoryLockManager();
  const staleGuard = makeGuard(
    new FakeClient(clientOptions(staleManager, 136)),
  ).guard;
  await assertGuardError(
    staleGuard.runExclusive("stale-carrier", () => authenticCarrier),
    "postgres_operation_guard_outcome_uncertain",
  );

  const structural = Object.freeze(
    Object.create(
      null,
      Object.getOwnPropertyDescriptors(authenticCarrier),
    ),
  );
  const structuralManager = new AdvisoryLockManager();
  const structuralGuard = makeGuard(
    new FakeClient(clientOptions(structuralManager, 137)),
  ).guard;
  await assertGuardError(
    structuralGuard.runExclusive("structural-carrier", () => structural),
    "postgres_operation_guard_outcome_uncertain",
  );

  const doubleManager = new AdvisoryLockManager();
  const doubleGuard = makeGuard(
    new FakeClient(clientOptions(doubleManager, 138)),
  ).guard;
  await assertGuardError(
    doubleGuard.runExclusive("double-complete", (_probe, complete) => {
      const first = complete("first");
      assert.throws(
        () => complete("second"),
        (error) =>
          error instanceof PostgresOperationGuardError &&
          error.code === "postgres_operation_guard_outcome_uncertain",
      );
      return first;
    }),
    "postgres_operation_guard_outcome_uncertain",
  );
});

test("callback Promise prototype poisoning cannot forge shared cleanup", () =>
  protectTestPromise((async () => {
  const manager = new AdvisoryLockManager();
  const finalProbe = deferred();
  const finalProbeStarted = deferred();
  const client = new SynchronousFinalProbeClient({
    finalProbe,
    finalProbeStarted,
    manager,
    pid: 117,
  });
  const { guard } = makeGuard(client);
  const expected = Object.freeze({ status: "completed" });
  let poisoning;
  let runSettled = false;
  let runError;

  try {
    const run = guard.runShared("prototype-poisoned-shared", () => {
      poisoning = installPromisePrototypePoisoning(
        [finalProbe.promise],
        () => client.finalProbeResult,
      );
      return expected;
    });
    observePromise(
      run,
      () => {
        runSettled = true;
      },
      (error) => {
        runSettled = true;
        runError = error;
      },
    );

    await finalProbeStarted.promise;
    assert.equal(runSettled, false);
    assert.equal(manager.holders.size, 1);
    assert.deepEqual(client.releaseCalls, []);
    assert.equal(poisoning.calls.targetThen, 0);

    finalProbe.resolve(client.finalProbeResult);
    const result = await run;
    const poisonedThenCalls = poisoning.calls.targetThen;
    poisoning.restore();

    assert.ifError(runError);
    assert.strictEqual(result, expected);
    assert.equal(runSettled, true);
    assert.equal(poisonedThenCalls, 0);
    assert.equal(client.resetCount, 2);
    assert.deepEqual(client.releaseCalls, [[]]);
    assert.equal(manager.holders.size, 0);
  } finally {
    poisoning?.restore();
    if (client.finalProbeResult !== undefined) {
      finalProbe.resolve(client.finalProbeResult);
    }
  }
  })()));

test("connect and query callback adapters wait for delayed callbacks", async () => {
  const manager = new AdvisoryLockManager();
  const client = new FakeClient(
    clientOptions(manager, 118, { delayQueries: true }),
  );
  let connectCallbackDelivered = false;
  const pool = {
    connect(callback) {
      setImmediate(() => {
        connectCallbackDelivered = true;
        callback(null, client, client.release);
      });
      return undefined;
    },
  };
  const guard = new PostgresOperationGuard({ dedicatedPool: pool });
  const result = await guard.runExclusive(
    "delayed-callback-adapters",
    () => "connected",
  );

  assert.equal(result, "connected");
  assert.equal(connectCallbackDelivered, true);
  assert.equal(client.resetCount, 2);
  assert.deepEqual(client.releaseCalls, [[]]);
  assert.equal(manager.holders.size, 0);
});

test("callback adapters reject synchronous callbacks with illegal raw returns", async () => {
  let thenGets = 0;
  const illegalReturn = Object.create(null);
  Object.defineProperty(illegalReturn, "then", {
    configurable: false,
    enumerable: true,
    get() {
      thenGets += 1;
      throw new Error("illegal adapter return must not be inspected");
    },
  });

  const connectManager = new AdvisoryLockManager();
  const connectClient = new FakeClient(clientOptions(connectManager, 139));
  const connectPool = {
    connect(callback) {
      callback(null, connectClient, connectClient.release);
      return illegalReturn;
    },
  };
  await assertGuardError(
    new PostgresOperationGuard({ dedicatedPool: connectPool }).runExclusive(
      "illegal-connect-return",
      () => "must-not-run",
    ),
    "postgres_operation_guard_outcome_uncertain",
  );
  assert.equal(connectClient.releaseCalls.length, 1);
  assert.equal(connectClient.releaseCalls[0].length, 1);

  const queryManager = new AdvisoryLockManager();
  const queryClient = new FakeClient(
    clientOptions(queryManager, 140, { queryReturnValue: illegalReturn }),
  );
  await assertGuardError(
    makeGuard(queryClient).guard.runExclusive(
      "illegal-query-return",
      () => "must-not-run",
    ),
    "postgres_operation_guard_outcome_uncertain",
  );
  assert.equal(queryClient.releaseCalls.length, 1);
  assert.equal(queryClient.releaseCalls[0].length, 1);
  assert.equal(thenGets, 0);
});

test("illegal rejected Promise adapter returns are drained without awaiting", async () => {
  const connectManager = new AdvisoryLockManager();
  const connectClient = new FakeClient(clientOptions(connectManager, 149));
  const connectError = new Error("illegal connect Promise");
  const connectPool = {
    connect(callback) {
      callback(null, connectClient, connectClient.release);
      return PromiseConstructor.reject(connectError);
    },
  };
  await assertGuardError(
    new PostgresOperationGuard({ dedicatedPool: connectPool }).runExclusive(
      "rejected-connect-return",
      () => "must-not-run",
    ),
    "postgres_operation_guard_outcome_uncertain",
  );

  const queryManager = new AdvisoryLockManager();
  const queryClient = new FakeClient(
    clientOptions(queryManager, 150, {
      queryReturnValue: PromiseConstructor.reject(
        new Error("illegal query Promise"),
      ),
    }),
  );
  await assertGuardError(
    makeGuard(queryClient).guard.runExclusive(
      "rejected-query-return",
      () => "must-not-run",
    ),
    "postgres_operation_guard_outcome_uncertain",
  );
  await new PromiseConstructor((resolve) => setImmediate(resolve));
  assert.equal(connectClient.releaseCalls.length, 1);
  assert.equal(queryClient.releaseCalls.length, 1);
});

test("connect synchronous throw after delivery destroys the delivered client", async () => {
  const manager = new AdvisoryLockManager();
  const client = new FakeClient(clientOptions(manager, 156));
  const pool = {
    connect(callback) {
      callback(null, client, client.release);
      throw new Error("connect threw after callback");
    },
  };
  await assertGuardError(
    new PostgresOperationGuard({ dedicatedPool: pool }).runExclusive(
      "connect-throw-after-delivery",
      () => "must-not-run",
    ),
    "postgres_operation_guard_outcome_uncertain",
  );
  assert.equal(client.releaseCalls.length, 1);
  assert.equal(client.releaseCalls[0].length, 1);
});

test("duplicate callbacks fail closed and late callbacks are inert", async () => {
  const duplicateManager = new AdvisoryLockManager();
  const duplicateClient = new FakeClient(
    clientOptions(duplicateManager, 141),
  );
  const duplicateExtraClient = new FakeClient(
    clientOptions(duplicateManager, 146),
  );
  const duplicatePool = {
    connect(callback) {
      callback(null, duplicateClient, duplicateClient.release);
      callback(
        null,
        duplicateExtraClient,
        duplicateExtraClient.release,
      );
      return undefined;
    },
  };
  await assertGuardError(
    new PostgresOperationGuard({ dedicatedPool: duplicatePool }).runExclusive(
      "duplicate-connect-callback",
      () => "must-not-run",
    ),
    "postgres_operation_guard_outcome_uncertain",
  );
  assert.equal(duplicateClient.releaseCalls.length, 1);
  assert.equal(duplicateClient.releaseCalls[0].length, 1);
  assert.equal(duplicateExtraClient.releaseCalls.length, 1);
  assert.equal(duplicateExtraClient.releaseCalls[0].length, 1);

  const duplicateQueryManager = new AdvisoryLockManager();
  const duplicateQueryClient = new FakeClient(
    clientOptions(duplicateQueryManager, 152),
  );
  const duplicateQueryMethod = duplicateQueryClient.query;
  duplicateQueryClient.query = function query(config) {
    const returned = Reflect.apply(duplicateQueryMethod, this, [config]);
    config.callback(null, Object.freeze({ duplicate: true }));
    return returned;
  };
  await assertGuardError(
    makeGuard(duplicateQueryClient).guard.runExclusive(
      "duplicate-query-callback",
      () => "must-not-run",
    ),
    "postgres_operation_guard_outcome_uncertain",
  );
  assert.equal(duplicateQueryClient.releaseCalls.length, 1);
  assert.equal(duplicateQueryClient.releaseCalls[0].length, 1);

  const lateConnectManager = new AdvisoryLockManager();
  const lateConnectClient = new FakeClient(
    clientOptions(lateConnectManager, 147),
  );
  const lateConnectExtraClient = new FakeClient(
    clientOptions(lateConnectManager, 148),
  );
  let lateConnectCallback;
  const lateConnectPool = {
    connect(callback) {
      lateConnectCallback = callback;
      callback(null, lateConnectClient, lateConnectClient.release);
      return undefined;
    },
  };
  assert.equal(
    await new PostgresOperationGuard({
      dedicatedPool: lateConnectPool,
    }).runExclusive("late-connect-callback", () => "completed"),
    "completed",
  );
  assert.equal(
    lateConnectCallback(
      null,
      lateConnectClient,
      lateConnectClient.release,
    ),
    undefined,
  );
  assert.deepEqual(lateConnectClient.releaseCalls, [[]]);
  assert.equal(
    lateConnectCallback(
      null,
      lateConnectExtraClient,
      lateConnectExtraClient.release,
    ),
    undefined,
  );
  assert.equal(lateConnectExtraClient.releaseCalls.length, 1);
  assert.equal(lateConnectExtraClient.releaseCalls[0].length, 1);

  const lateManager = new AdvisoryLockManager();
  const lateClient = new FakeClient(clientOptions(lateManager, 142));
  const originalQuery = lateClient.query;
  let lateCallback;
  lateClient.query = function query(config) {
    lateCallback = config.callback;
    return Reflect.apply(originalQuery, this, [config]);
  };
  assert.equal(
    await makeGuard(lateClient).guard.runExclusive(
      "late-query-callback",
      () => "completed",
    ),
    "completed",
  );
  assert.equal(lateCallback(null, Object.freeze({ late: true })), undefined);
});

test("release and destroy require exact synchronous undefined returns", async () => {
  let thenGets = 0;
  const illegalThenable = Object.create(null);
  Object.defineProperty(illegalThenable, "then", {
    configurable: false,
    enumerable: true,
    get() {
      thenGets += 1;
      throw new Error("release return must not be assimilated");
    },
  });
  const nonUndefinedManager = new AdvisoryLockManager();
  const nonUndefinedClient = new FakeClient(
    clientOptions(nonUndefinedManager, 143, {
      releaseReturnValue: illegalThenable,
    }),
  );
  await assertGuardError(
    makeGuard(nonUndefinedClient).guard.runExclusive(
      "non-undefined-release",
      () => "completed",
    ),
    "postgres_operation_guard_outcome_uncertain",
  );
  assert.equal(thenGets, 0);

  const asyncManager = new AdvisoryLockManager();
  const asyncClient = new FakeClient(clientOptions(asyncManager, 144));
  const releaseCalls = asyncClient.releaseCalls;
  asyncClient.release = function release(...args) {
    releaseCalls.push(args);
    if (args.length === 1) asyncManager.releaseAll(this);
    return PromiseConstructor.reject(new Error("async release rejected"));
  };
  await assertGuardError(
    makeGuard(asyncClient).guard.runExclusive(
      "asynchronous-release",
      () => "completed",
    ),
    "postgres_operation_guard_outcome_uncertain",
  );
  await new PromiseConstructor((resolve) => setImmediate(resolve));
  assert.deepEqual(releaseCalls, [[]]);

  const exactManager = new AdvisoryLockManager();
  const exactClient = new FakeClient(clientOptions(exactManager, 158));
  const exactReleaseCalls = exactClient.releaseCalls;
  exactClient.release = function release(...args) {
    exactReleaseCalls.push(args);
    const rejected = PromiseConstructor.reject(
      new Error("exact asynchronous release rejected"),
    );
    Object.defineProperty(rejected, "constructor", {
      configurable: false,
      enumerable: false,
      value: PromiseConstructor,
      writable: false,
    });
    return rejected;
  };
  await assertGuardError(
    makeGuard(exactClient).guard.runExclusive(
      "exact-asynchronous-release",
      () => "completed",
    ),
    "postgres_operation_guard_outcome_uncertain",
  );
  await new PromiseConstructor((resolve) => setImmediate(resolve));
  assert.deepEqual(exactReleaseCalls, [[]]);
});

test("best-effort Promise drain discards fulfilled release payloads", async () => {
  const manager = new AdvisoryLockManager();
  const client = new FakeClient(clientOptions(manager, 157));
  const fulfilledPayload = Object.freeze({ payload: "must-not-propagate" });
  const illegalPromise = PromiseConstructor.resolve(fulfilledPayload);
  const releaseCalls = client.releaseCalls;
  const thenDescriptor = Object.getOwnPropertyDescriptor(
    Object.prototype,
    "then",
  );
  let poisonCalls = 0;
  client.release = function release(...args) {
    releaseCalls.push(args);
    Object.defineProperty(Object.prototype, "then", {
      configurable: true,
      enumerable: false,
      get() {
        poisonCalls += 1;
        throw new Error("fulfilled release payload must be discarded");
      },
    });
    return illegalPromise;
  };

  let observedError;
  try {
    await makeGuard(client).guard.runExclusive(
      "fulfilled-release-payload",
      () => "completed",
    );
    assert.fail("illegal release return must fail closed");
  } catch (error) {
    observedError = error;
  } finally {
    if (thenDescriptor === undefined) {
      delete Object.prototype.then;
    } else {
      Object.defineProperty(Object.prototype, "then", thenDescriptor);
    }
  }
  await new PromiseConstructor((resolve) => setImmediate(resolve));
  assert.ok(observedError instanceof PostgresOperationGuardError);
  assert.equal(
    observedError.code,
    "postgres_operation_guard_outcome_uncertain",
  );
  assert.equal(poisonCalls, 0);
  assert.deepEqual(releaseCalls, [[]]);
});

test("exact captured-constructor Promise uses direct await without species access", () =>
  protectTestPromise((async () => {
  const manager = new AdvisoryLockManager();
  const client = new FakeClient(clientOptions(manager, 128));
  const { guard } = makeGuard(client);
  const completion = deferred();
  const entered = deferred();
  const expected = Object.freeze({ exact: "carrier-value" });
  let carrier;
  let countSpeciesGets = false;
  let speciesGets = 0;
  const speciesDescriptor = Object.getOwnPropertyDescriptor(
    PromiseConstructor,
    Symbol.species,
  );
  Object.defineProperty(completion.promise, "constructor", {
    configurable: false,
    enumerable: false,
    value: PromiseConstructor,
    writable: false,
  });
  Object.defineProperty(PromiseConstructor, Symbol.species, {
    configurable: true,
    get() {
      if (countSpeciesGets) speciesGets += 1;
      return PromiseConstructor;
    },
  });

  try {
    const run = guard.runExclusive(
      "exact-native-direct-await",
      (_probe, complete) => {
        carrier = complete(expected);
        entered.resolve();
        return completion.promise;
      },
    );
    await entered.promise;
    countSpeciesGets = true;
    assert.equal(manager.holders.size, 1);
    assert.deepEqual(client.releaseCalls, []);

    completion.resolve(carrier);
    assert.strictEqual(await run, expected);
    countSpeciesGets = false;
    assert.equal(speciesGets, 0);
    assert.deepEqual(client.releaseCalls, [[]]);
    assert.equal(manager.holders.size, 0);
  } finally {
    countSpeciesGets = false;
    Object.defineProperty(
      PromiseConstructor,
      Symbol.species,
      speciesDescriptor,
    );
    completion.resolve(carrier);
  }
  })()));

test("public reactions reject non-rewritable exact Promise without species access", () =>
  protectTestPromise((async () => {
  const manager = new AdvisoryLockManager();
  const client = new FakeClient(clientOptions(manager, 129));
  const { guard } = makeGuard(client);
  const completion = deferred();
  const entered = deferred();
  let carrier;
  const returned = deferred();
  let countSpeciesGets = false;
  let speciesGets = 0;
  const speciesDescriptor = Object.getOwnPropertyDescriptor(
    PromiseConstructor,
    Symbol.species,
  );
  Object.defineProperty(returned.promise, "constructor", {
    configurable: false,
    enumerable: false,
    value: PromiseConstructor,
    writable: false,
  });
  Object.defineProperty(PromiseConstructor, Symbol.species, {
    configurable: true,
    get() {
      if (countSpeciesGets) speciesGets += 1;
      return PromiseConstructor;
    },
  });

  try {
    const reaction = guard
      .runExclusive(
        "public-reaction-normalization",
        (_probe, complete) => {
          carrier = complete("complete");
          entered.resolve();
          return completion.promise;
        },
      )
      .then(() => returned.promise);
    await entered.promise;
    countSpeciesGets = true;
    completion.resolve(carrier);
    await assertGuardError(
      reaction,
      "postgres_operation_guard_outcome_uncertain",
    );
    countSpeciesGets = false;
    assert.equal(speciesGets, 0);
  } finally {
    countSpeciesGets = false;
    Object.defineProperty(
      PromiseConstructor,
      Symbol.species,
      speciesDescriptor,
    );
    completion.resolve(carrier);
    returned.resolve(undefined);
  }
  })()));

test("public reactions rewrite writable exact Promise constructors in place", async () => {
  const manager = new AdvisoryLockManager();
  const client = new FakeClient(clientOptions(manager, 155));
  const { guard } = makeGuard(client);
  const returned = deferred();
  Object.defineProperty(returned.promise, "constructor", {
    configurable: false,
    enumerable: false,
    value: PromiseConstructor,
    writable: true,
  });

  const reaction = guard
    .runExclusive("public-reaction-rewrite", () => "complete")
    .then(() => returned.promise);
  setImmediate(() => returned.resolve("reaction-complete"));
  assert.equal(await reaction, "reaction-complete");
  const constructorDescriptor = Object.getOwnPropertyDescriptor(
    returned.promise,
    "constructor",
  );
  assert.equal(constructorDescriptor.configurable, false);
  assert.equal(constructorDescriptor.writable, false);
  assert.notStrictEqual(constructorDescriptor.value, PromiseConstructor);
  assert.equal(Object.getPrototypeOf(constructorDescriptor.value), null);
  assert.equal(Object.isFrozen(constructorDescriptor.value), true);
  assert.strictEqual(
    constructorDescriptor.value[Symbol.species],
    PromiseConstructor,
  );
});

test("poisoned callback Promise drains before exclusive unlock and release", () =>
  protectTestPromise((async () => {
  const manager = new AdvisoryLockManager();
  const client = new FakeClient(clientOptions(manager, 119));
  const { guard } = makeGuard(client);
  const callbackEntered = deferred();
  const callbackCompletion = deferred();
  let completionCarrier;
  let poisoning;
  let runSettled = false;

  try {
    const run = guard.runExclusive("prototype-poisoned-callback", (_probe, complete) => {
      callbackEntered.resolve();
      completionCarrier = complete("drained");
      poisoning = installPromisePrototypePoisoning(
        [callbackCompletion.promise],
        () => "forged",
      );
      return callbackCompletion.promise;
    });
    observePromise(
      run,
      () => {
        runSettled = true;
      },
      () => {
        runSettled = true;
      },
    );

    await callbackEntered.promise;
    assert.equal(runSettled, false);
    assert.equal(manager.holders.size, 1);
    assert.deepEqual(client.releaseCalls, []);
    assert.equal(poisoning.calls.targetThen, 0);

    callbackCompletion.resolve(completionCarrier);
    const result = await run;
    const poisonedThenCalls = poisoning.calls.targetThen;
    poisoning.restore();

    assert.equal(result, "drained");
    assert.equal(runSettled, true);
    assert.equal(poisonedThenCalls, 0);
    assert.deepEqual(client.releaseCalls, [[]]);
    assert.equal(manager.holders.size, 0);
  } finally {
    poisoning?.restore();
    callbackCompletion.resolve(completionCarrier);
  }
  })()));

test("exposed run and probe reactions ignore callback-time Promise poison", () =>
  protectTestPromise((async () => {
    const manager = new AdvisoryLockManager();
    const probeGate = deferred();
    const callbackCompletion = deferred();
    const callbackEntered = deferred();
    const client = new FakeClient(
      clientOptions(manager, 120, {
        heldProbeGates: new Map([[2, probeGate]]),
      }),
    );
    const { guard } = makeGuard(client);
    let poisoning;
    let probeReaction;
    let run;
    let runReaction;
    let probeReactionValue;
    let runReactionValue;
    let completionCarrier;

    try {
      run = guard.runExclusive("prototype-poisoned-reactions", (probe, complete) => {
        const probePromise = probe.assertHeld();
        poisoning = installPromisePrototypePoisoning(
          [run, probePromise, callbackCompletion.promise],
          () => "spoofed-before-cleanup",
        );
        probeReaction = probePromise.then((value) => {
          probeReactionValue = value;
          return "probe-complete";
        });
        runReaction = run.then((value) => {
          runReactionValue = value;
          return value;
        });
        completionCarrier = complete("genuine-completion");
        callbackEntered.resolve();
        return callbackCompletion.promise;
      });

      await callbackEntered.promise;
      await PromiseConstructor.resolve();
      assert.equal(probeReactionValue, undefined);
      assert.equal(runReactionValue, undefined);
      assert.equal(manager.holders.size, 1);
      assert.deepEqual(client.releaseCalls, []);
      assert.equal(poisoning.calls.targetThen, 0);

      probeGate.resolve();
      assert.equal(await probeReaction, "probe-complete");
      assert.equal(probeReactionValue, undefined);
      assert.equal(runReactionValue, undefined);
      assert.equal(manager.holders.size, 1);
      assert.deepEqual(client.releaseCalls, []);

      callbackCompletion.resolve(completionCarrier);
      assert.equal(await runReaction, "genuine-completion");
      const poisonedThenCalls = poisoning.calls.targetThen;
      poisoning.restore();

      assert.equal(runReactionValue, "genuine-completion");
      assert.equal(poisonedThenCalls, 0);
      assert.deepEqual(client.releaseCalls, [[]]);
      assert.equal(manager.holders.size, 0);
    } finally {
      poisoning?.restore();
      probeGate.resolve();
      callbackCompletion.resolve(completionCarrier);
    }
  })()));

test("unsafe callback values fail closed without executable protocol access", async () => {
  let proxyTraps = 0;
  let thenGets = 0;
  const callbackProxyResult = new Proxy(Object.create(null), {
    get() {
      proxyTraps += 1;
      throw new Error("must not read proxy result");
    },
    getOwnPropertyDescriptor() {
      proxyTraps += 1;
      throw new Error("must not inspect proxy result");
    },
    getPrototypeOf() {
      proxyTraps += 1;
      throw new Error("must not inspect proxy result");
    },
    ownKeys() {
      proxyTraps += 1;
      throw new Error("must not enumerate proxy result");
    },
  });
  const accessorThenable = Object.create(null);
  Object.defineProperty(accessorThenable, "then", {
    configurable: false,
    enumerable: true,
    get() {
      thenGets += 1;
      throw new Error("must not invoke then accessor");
    },
  });
  class UnsafePromise extends PromiseConstructor {}
  const unsafePromise = new UnsafePromise((resolve) => resolve("unsafe"));
  const cases = [
    callbackProxyResult,
    accessorThenable,
    (function* generatorResult() {})(),
    unsafePromise,
  ];

  for (let index = 0; index < cases.length; index += 1) {
    const manager = new AdvisoryLockManager();
    const client = new FakeClient(clientOptions(manager, 121 + index));
    const { guard } = makeGuard(client);
    await assertGuardError(
      guard.runExclusive(`unsafe-callback-${index}`, () => cases[index]),
      "postgres_operation_guard_outcome_uncertain",
    );
    assert.deepEqual(client.releaseCalls, [[]]);
    assert.equal(manager.holders.size, 0);
  }

  assert.equal(proxyTraps, 0);
  assert.equal(thenGets, 0);
});

test("Object.prototype.then poison cannot forge lock-loss cleanup", async () => {
  const manager = new AdvisoryLockManager();
  const client = new FakeClient(clientOptions(manager, 145));
  const { guard } = makeGuard(client);
  const thenDescriptor = Object.getOwnPropertyDescriptor(
    Object.prototype,
    "then",
  );
  let poisonCalls = 0;

  try {
    const run = guard.runExclusive(
      "object-prototype-then-lock-loss",
      (_probe, complete) => {
        Object.defineProperty(Object.prototype, "then", {
          configurable: true,
          enumerable: false,
          get() {
            poisonCalls += 1;
            throw new Error("Object.prototype.then must not be invoked");
          },
        });
        manager.releaseAll(client);
        return complete("must-not-succeed");
      },
    );
    let observedError;
    try {
      await run;
      assert.fail("lock loss must not succeed");
    } catch (error) {
      observedError = error;
    }
    assert.ok(observedError instanceof PostgresOperationGuardError);
    assert.equal(
      observedError.code,
      "postgres_operation_guard_outcome_uncertain",
    );
  } finally {
    if (thenDescriptor === undefined) {
      delete Object.prototype.then;
    } else {
      Object.defineProperty(Object.prototype, "then", thenDescriptor);
    }
  }

  assert.equal(poisonCalls, 0);
  assert.equal(client.releaseCalls.length, 1);
  assert.equal(client.releaseCalls[0].length, 1);
  assert.equal(manager.holders.size, 0);
});

test("query Result then accessors and proxied rows are never executed", async () => {
  let thenGets = 0;
  const resultManager = new AdvisoryLockManager();
  const resultClient = new FakeClient(clientOptions(resultManager, 153));
  const resultQuery = resultClient.query;
  resultClient.query = function query(config) {
    const callback = config.callback;
    const forwarded = Object.freeze(
      Object.assign(Object.create(null), config, {
        callback(error, result) {
          if (error === null && result?.command === "SELECT") {
            Object.defineProperty(result, "then", {
              configurable: false,
              enumerable: false,
              get() {
                thenGets += 1;
                throw new Error("Result.then must not be read");
              },
            });
          }
          callback(error, result);
        },
      }),
    );
    return Reflect.apply(resultQuery, this, [forwarded]);
  };
  assert.equal(
    await makeGuard(resultClient).guard.runExclusive(
      "result-then-accessor",
      () => "completed",
    ),
    "completed",
  );
  assert.equal(thenGets, 0);

  let rowProxyTraps = 0;
  const rowManager = new AdvisoryLockManager();
  const rowClient = new FakeClient(clientOptions(rowManager, 154));
  const rowQuery = rowClient.query;
  rowClient.query = function query(config) {
    const callback = config.callback;
    const forwarded = Object.freeze(
      Object.assign(Object.create(null), config, {
        callback(error, result) {
          if (
            error === null &&
            config.text.includes("pg_try_advisory_lock")
          ) {
            result.rows[0] = new Proxy(result.rows[0], {
              get() {
                rowProxyTraps += 1;
                throw new Error("proxied row must not be read");
              },
              getOwnPropertyDescriptor() {
                rowProxyTraps += 1;
                throw new Error("proxied row must not be inspected");
              },
              getPrototypeOf() {
                rowProxyTraps += 1;
                throw new Error("proxied row must not be inspected");
              },
              ownKeys() {
                rowProxyTraps += 1;
                throw new Error("proxied row must not be inspected");
              },
            });
          }
          callback(error, result);
        },
      }),
    );
    return Reflect.apply(rowQuery, this, [forwarded]);
  };
  await assertGuardError(
    makeGuard(rowClient).guard.runExclusive(
      "proxied-query-row",
      () => "must-not-run",
    ),
    "postgres_operation_guard_outcome_uncertain",
  );
  assert.equal(rowProxyTraps, 0);
});

test(
  "probe drain avoids mutable Promise and iterator protocols",
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
    const promiseResolveDescriptor = Object.getOwnPropertyDescriptor(
      Promise,
      "resolve",
    );
    const setSizeDescriptor = Object.getOwnPropertyDescriptor(
      Set.prototype,
      "size",
    );
    const arrayIteratorPrototype = Object.getPrototypeOf(
      [][Symbol.iterator](),
    );
    const arrayIteratorNextDescriptor =
      Object.getOwnPropertyDescriptor(arrayIteratorPrototype, "next");
    const setIteratorPrototype = Object.getPrototypeOf(
      new Set()[Symbol.iterator](),
    );
    const setIteratorNextDescriptor =
      Object.getOwnPropertyDescriptor(setIteratorPrototype, "next");

    try {
      const run = guard.runExclusive(
        "intrinsic-poisoning",
        async ({ assertHeld }, complete) => {
          void assertHeld();
          Object.defineProperty(Promise, "allSettled", {
            ...allSettledDescriptor,
            value() {
              throw new Error("poisoned Promise.allSettled");
            },
          });
          Object.defineProperty(Promise, "resolve", {
            ...promiseResolveDescriptor,
            value() {
              throw new Error("poisoned Promise.resolve");
            },
          });
          Object.defineProperty(Set.prototype, "size", {
            ...setSizeDescriptor,
            get() {
              throw new Error("poisoned Set.prototype.size");
            },
          });
          Object.defineProperty(arrayIteratorPrototype, "next", {
            ...arrayIteratorNextDescriptor,
            value() {
              throw new Error("poisoned Array iterator");
            },
          });
          Object.defineProperty(setIteratorPrototype, "next", {
            ...setIteratorNextDescriptor,
            value() {
              throw new Error("poisoned Set iterator");
            },
          });
          setImmediate(() => heldProbeGate.resolve());
          return complete("completed");
        },
      );
      assert.equal(await run, "completed");
    } finally {
      Object.defineProperty(
        Promise,
        "allSettled",
        allSettledDescriptor,
      );
      Object.defineProperty(
        Promise,
        "resolve",
        promiseResolveDescriptor,
      );
      Object.defineProperty(Set.prototype, "size", setSizeDescriptor);
      Object.defineProperty(
        arrayIteratorPrototype,
        "next",
        arrayIteratorNextDescriptor,
      );
      Object.defineProperty(
        setIteratorPrototype,
        "next",
        setIteratorNextDescriptor,
      );
    }

    assert.equal(client.heldProbeCount, 3);
    assert.equal(manager.holders.size, 0);
  },
);

test("probe drain unlinks out-of-order completions", async () => {
  const manager = new AdvisoryLockManager();
  const first = deferred();
  const middle = deferred();
  const last = deferred();
  const client = new FakeClient(
    clientOptions(manager, 104, {
      heldProbeGates: new Map([
        [2, first],
        [3, middle],
        [4, last],
      ]),
    }),
  );
  const { guard } = makeGuard(client);

  const run = guard.runExclusive(
    "out-of-order-probe-drain",
    async ({ assertHeld }, complete) => {
      void assertHeld();
      void assertHeld();
      void assertHeld();
      middle.resolve();
      setImmediate(() => {
        first.resolve();
        setImmediate(() => last.resolve());
      });
      return complete("completed");
    },
  );

  assert.equal(await run, "completed");
  assert.equal(client.heldProbeCount, 5);
  assert.equal(manager.holders.size, 0);
});

test("same operation ID is busy while the first callback holds the lock", async () => {
  const manager = new AdvisoryLockManager();
  const firstClient = new FakeClient(clientOptions(manager, 104));
  const secondClient = new FakeClient(clientOptions(manager, 105));
  const firstGuard = makeGuard(firstClient).guard;
  const secondGuard = makeGuard(secondClient).guard;
  const entered = deferred();
  const finish = deferred();
  let secondCallbackCalls = 0;

  const firstRun = firstGuard.runExclusive("same-operation", async (_probe, complete) => {
    entered.resolve();
    await finish.promise;
    return complete("first");
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

    const callback = async (_probe, complete) => {
      active += 1;
      maximumActive = Math.max(maximumActive, active);
      if (active === 2) bothEntered.resolve();
      await finish.promise;
      active -= 1;
      return complete(undefined);
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
    guard.runExclusive("lost-lock", async ({ assertHeld }, complete) => {
      manager.releaseAll(client);
      await assertGuardError(
        assertHeld(),
        "postgres_operation_guard_outcome_uncertain",
      );
      return complete("must not escape");
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
    guard.runExclusive("lost-connection", () => {
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
    guard.runExclusive("unlock-failure", () => "success"),
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
    guard.runExclusive("post-reset-failure", () => "success"),
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
    guard.runExclusive("release-failure", () => "success"),
    "postgres_operation_guard_outcome_uncertain",
  );

  assert.deepEqual(client.releaseCalls, [[]]);
  assert.equal(manager.holders.size, 0);
});
