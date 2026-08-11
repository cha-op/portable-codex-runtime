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
    failPreReset = false,
    failPostReset = false,
    failUnlock = false,
    failRelease = false,
    heldProbeGate = undefined,
    heldProbeGateCall = undefined,
    heldProbeGates = undefined,
    loseTryLockResponse = false,
  }) {
    this.connectionLost = false;
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

    const values =
      typeof query === "string" ? args[1] : query.values;
    const key = values[0];
    if (text.includes("pg_try_advisory_lock")) {
      const mode = text.includes("pg_try_advisory_lock_shared")
        ? "shared"
        : "exclusive";
      const acquired = this.manager.tryAcquire(key, this, mode);
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
      const mode = text.includes("mode = 'ShareLock'")
        ? "shared"
        : "exclusive";
      this.heldProbeCount += 1;
      const heldProbeGate =
        this.heldProbeGates?.get(this.heldProbeCount) ??
        (this.heldProbeCount === this.heldProbeGateCall
          ? this.heldProbeGate
          : undefined);
      if (heldProbeGate !== undefined) {
        await heldProbeGate.promise;
      }
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
      if (this.failUnlock) throw new Error("unlock failed");
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
        return this.finalProbe.promise;
      }
      return result;
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
      value: PromiseConstructor,
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

test("runShared uses the same key derivation with shared lock SQL", async () => {
  const manager = new AdvisoryLockManager();
  const client = new FakeClient(clientOptions(manager, 116));
  const { guard } = makeGuard(client);

  assert.equal(
    await guard.runShared("checkpoint:operation-001", async (probe) => {
      await probe.assertHeld();
      return "shared";
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

test("connect-time Promise prototype poisoning cannot forge exclusive awaits", () =>
  protectTestPromise((async () => {
  const manager = new AdvisoryLockManager();
  const client = new FakeClient(clientOptions(manager, 118));
  const connection = PromiseConstructor.resolve(client);
  let poisoning;
  const pool = {
    connect() {
      poisoning = installPromisePrototypePoisoning(
        [connection],
        () => client,
      );
      return connection;
    },
  };
  const guard = new PostgresOperationGuard({ dedicatedPool: pool });
  let result;

  try {
    result = await guard.runExclusive(
      "prototype-poisoned-connect",
      () => "connected",
    );
    const poisonedThenCalls = poisoning.calls.targetThen;
    poisoning.restore();

    assert.equal(result, "connected");
    assert.equal(poisonedThenCalls, 0);
    assert.equal(client.resetCount, 2);
    assert.deepEqual(client.releaseCalls, [[]]);
    assert.equal(manager.holders.size, 0);
  } finally {
    poisoning?.restore();
  }
  })()));

test("poisoned callback Promise drains before exclusive unlock and release", () =>
  protectTestPromise((async () => {
  const manager = new AdvisoryLockManager();
  const client = new FakeClient(clientOptions(manager, 119));
  const { guard } = makeGuard(client);
  const callbackEntered = deferred();
  const callbackCompletion = deferred();
  let poisoning;
  let runSettled = false;

  try {
    const run = guard.runExclusive("prototype-poisoned-callback", () => {
      callbackEntered.resolve();
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

    callbackCompletion.resolve("drained");
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
    callbackCompletion.resolve("drained");
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

    try {
      run = guard.runExclusive("prototype-poisoned-reactions", (probe) => {
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

      callbackCompletion.resolve("genuine-completion");
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
      callbackCompletion.resolve("genuine-completion");
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
        async ({ assertHeld }) => {
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
    async ({ assertHeld }) => {
      void assertHeld();
      void assertHeld();
      void assertHeld();
      middle.resolve();
      setImmediate(() => {
        first.resolve();
        setImmediate(() => last.resolve());
      });
      return "completed";
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
