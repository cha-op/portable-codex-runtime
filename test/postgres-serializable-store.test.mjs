import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { EventEmitter } from "node:events";
import { readFile } from "node:fs/promises";
import test from "node:test";

import { DatabaseError } from "pg";

import {
  PostgresSerializableStore,
  PostgresSerializableStoreError,
  SESSION_AUTHORITY_MIGRATION_VERSION,
} from "../src/postgres-serializable-store.mjs";

const COMMIT_RESULT = Object.freeze({ command: "COMMIT" });
const DISCARD_RESULT = Object.freeze({ command: "DISCARD" });
const ROLLBACK_RESULT = Object.freeze({ command: "ROLLBACK" });

class FakeClient {
  constructor(steps, { releaseError, resetSteps = [] } = {}) {
    this.connection = new EventEmitter();
    this.queries = [];
    this.releaseCalls = [];
    this.releaseError = releaseError;
    this.resetSteps = [...resetSteps];
    this.steps = [...steps];
  }

  async query(...args) {
    this.queries.push(args);
    const text = queryText(args);
    if (text === "DISCARD ALL") {
      if (this.resetSteps.length === 0) return DISCARD_RESULT;
      const resetStep = this.resetSteps.shift();
      if (resetStep instanceof Error) throw resetStep;
      return resetStep;
    }
    assert.notEqual(this.steps.length, 0, `unexpected query: ${text}`);
    const step = this.steps.shift();
    if (typeof step === "function") return step(args);
    if (step instanceof Error) {
      if (step instanceof DatabaseError) {
        this.connection.emit("errorMessage", step);
      }
      throw step;
    }
    if (
      text === "ROLLBACK" &&
      step !== null &&
      typeof step === "object" &&
      Reflect.ownKeys(step).length === 0
    ) {
      return ROLLBACK_RESULT;
    }
    return step;
  }

  async release(...args) {
    this.releaseCalls.push(args);
    if (this.releaseError !== undefined) throw this.releaseError;
  }

  assertExhausted() {
    assert.deepEqual(this.steps, []);
    assert.deepEqual(this.resetSteps, []);
    assert.equal(this.connection.listenerCount("errorMessage"), 0);
  }
}

class FakePool {
  constructor(connections) {
    this.connectCalls = 0;
    this.connections = [...connections];
  }

  async connect() {
    this.connectCalls += 1;
    assert.notEqual(this.connections.length, 0, "unexpected pool.connect()");
    const connection = this.connections.shift();
    if (connection instanceof Error) throw connection;
    return connection;
  }
}

function timestampResult(value) {
  return {
    rows: [{ transaction_id: "100", transaction_timestamp: value }],
  };
}

function transactionIdResult(value = "100") {
  return { rows: [{ transaction_id: value }] };
}

function queryText(args) {
  return typeof args[0] === "string" ? args[0] : args[0]?.text;
}

function nonResetQueries(client) {
  return client.queries.filter((args) => queryText(args) !== "DISCARD ALL");
}

function pgError(code, message = code) {
  const error = new DatabaseError(message, 1, "error");
  error.code = code;
  error.severity = "ERROR";
  return error;
}

async function assertStoreError(promise, expected) {
  await assert.rejects(promise, (error) => {
    assert.ok(error instanceof PostgresSerializableStoreError);
    assert.equal(error.name, "PostgresSerializableStoreError");
    assert.equal(error.code, expected.code);
    assert.equal(error.commitState, expected.commitState);
    assert.equal(error.retryable, false);
    assert.equal(Object.hasOwn(error, "cause"), false);
    assert.equal("cause" in error, false);
    if (expected.omittedText !== undefined) {
      assert.equal(error.message.includes(expected.omittedText), false);
      assert.equal(String(error.stack).includes(expected.omittedText), false);
    }
    assert.equal(Object.isFrozen(error), true);
    return true;
  });
}

test("runSerializable binds query and database time to one released client", async () => {
  const client = new FakeClient([
    {},
    timestampResult(new Date("2026-07-23T10:11:12.345Z")),
    { rows: [{ value: 7 }] },
    transactionIdResult(),
    COMMIT_RESULT,
  ]);
  const store = new PostgresSerializableStore({
    maxTransactionAttempts: 2,
    dedicatedPool: new FakePool([client]),
  });
  let retainedTransaction;

  const result = await store.runSerializable(async (transaction) => {
    retainedTransaction = transaction;
    assert.equal(Object.isFrozen(transaction), true);
    assert.deepEqual(Object.keys(transaction), ["now", "query"]);
    assert.equal(transaction.now, "2026-07-23T10:11:12.345Z");
    return transaction.query("SELECT $1::integer AS value", [7]);
  });

  assert.deepEqual(result, { rows: [{ value: 7 }] });
  assert.deepEqual(client.queries, [
    ["DISCARD ALL"],
    ["BEGIN ISOLATION LEVEL SERIALIZABLE READ WRITE"],
    [
      "SELECT transaction_timestamp() AS transaction_timestamp, pg_current_xact_id()::text AS transaction_id",
    ],
    [
      {
        queryMode: "extended",
        text: "SELECT $1::integer AS value",
        values: [7],
      },
    ],
    ["SELECT pg_current_xact_id()::text AS transaction_id"],
    ["COMMIT"],
    ["DISCARD ALL"],
  ]);
  assert.deepEqual(client.releaseCalls, [[]]);
  client.assertExhausted();
  await assertStoreError(
    retainedTransaction.query("SELECT 1"),
    {
      code: "transaction_query_inactive",
      commitState: "not-committed",
    },
  );
});

test("runSerializable retries only callback SQLSTATE failures on new clients", async () => {
  const serializationFailure = pgError("40001");
  const deadlockFailure = pgError("40P01");
  const clients = [
    new FakeClient([
      {},
      timestampResult("2026-07-23T10:11:12.100Z"),
      serializationFailure,
      {},
    ]),
    new FakeClient([
      {},
      timestampResult("2026-07-23T10:11:12.200Z"),
      deadlockFailure,
      {},
    ]),
    new FakeClient([
      {},
      timestampResult("2026-07-23T10:11:12.300Z"),
      { rows: [{ value: "committed" }] },
      transactionIdResult(),
      COMMIT_RESULT,
    ]),
  ];
  const pool = new FakePool(clients);
  const store = new PostgresSerializableStore({
    maxTransactionAttempts: 3,
    dedicatedPool: pool,
  });
  const observedTimes = [];

  const value = await store.runSerializable(async (transaction) => {
    observedTimes.push(transaction.now);
    return transaction.query("SELECT 'committed' AS value");
  });

  assert.deepEqual(value, { rows: [{ value: "committed" }] });
  assert.deepEqual(observedTimes, [
    "2026-07-23T10:11:12.100Z",
    "2026-07-23T10:11:12.200Z",
    "2026-07-23T10:11:12.300Z",
  ]);
  assert.equal(pool.connectCalls, 3);
  for (const [index, client] of clients.entries()) {
    assert.deepEqual(
      nonResetQueries(client).map(queryText),
      index < 2
        ? [
            "BEGIN ISOLATION LEVEL SERIALIZABLE READ WRITE",
            "SELECT transaction_timestamp() AS transaction_timestamp, pg_current_xact_id()::text AS transaction_id",
            "SELECT 'committed' AS value",
            "ROLLBACK",
          ]
        : [
            "BEGIN ISOLATION LEVEL SERIALIZABLE READ WRITE",
            "SELECT transaction_timestamp() AS transaction_timestamp, pg_current_xact_id()::text AS transaction_id",
            "SELECT 'committed' AS value",
            "SELECT pg_current_xact_id()::text AS transaction_id",
            "COMMIT",
          ],
    );
    assert.deepEqual(client.releaseCalls, [[]]);
    client.assertExhausted();
  }
});

test("runSerializable reports bounded retry exhaustion after confirmed rollbacks", async () => {
  const failures = [pgError("40001"), pgError("40P01")];
  const clients = failures.map(
    (failure, index) =>
      new FakeClient([
        {},
        timestampResult(`2026-07-23T10:11:1${index}.000Z`),
        failure,
        {},
      ]),
  );
  const store = new PostgresSerializableStore({
    maxTransactionAttempts: 2,
    dedicatedPool: new FakePool(clients),
  });

  await assertStoreError(
    store.runSerializable((transaction) => transaction.query("SELECT 1")),
    {
      code: "serialization_retry_exhausted",
      commitState: "not-committed",
    },
  );
  for (const client of clients) {
    assert.deepEqual(client.releaseCalls, [[]]);
    client.assertExhausted();
  }
});

test("a callback-spoofed transaction SQLSTATE is never retried", async () => {
  const spoofedFailure = pgError("40001", "application supplied this error");
  const client = new FakeClient([
    {},
    timestampResult("2026-07-23T10:11:12.000Z"),
    {},
  ]);
  const pool = new FakePool([client]);
  const store = new PostgresSerializableStore({
    maxTransactionAttempts: 3,
    dedicatedPool: pool,
  });
  let callbacks = 0;

  await assert.rejects(
    store.runSerializable(() => {
      callbacks += 1;
      throw spoofedFailure;
    }),
    (error) => {
      assert.equal(error, spoofedFailure);
      return true;
    },
  );
  assert.equal(callbacks, 1);
  assert.equal(pool.connectCalls, 1);
  assert.deepEqual(client.releaseCalls, [[]]);
  client.assertExhausted();
});

test("custom parameter conversion cannot impersonate a server retry", async () => {
  const client = new FakeClient([
    {},
    timestampResult("2026-07-23T10:11:12.000Z"),
    {},
  ]);
  const pool = new FakePool([client]);
  const store = new PostgresSerializableStore({
    maxTransactionAttempts: 3,
    dedicatedPool: pool,
  });
  let callbacks = 0;
  let conversions = 0;
  const value = {
    toPostgres() {
      conversions += 1;
      throw pgError("40001", "local converter impersonated a server error");
    },
  };

  await assertStoreError(
    store.runSerializable(async (transaction) => {
      callbacks += 1;
      await transaction.query("SELECT $1::text", [value]);
    }),
    {
      code: "transaction_query_invalid",
      commitState: "not-committed",
    },
  );
  assert.equal(callbacks, 1);
  assert.equal(conversions, 0);
  assert.equal(pool.connectCalls, 1);
  assert.deepEqual(
    nonResetQueries(client).map(queryText),
    [
      "BEGIN ISOLATION LEVEL SERIALIZABLE READ WRITE",
      "SELECT transaction_timestamp() AS transaction_timestamp, pg_current_xact_id()::text AS transaction_id",
      "ROLLBACK",
    ],
  );
  assert.deepEqual(client.releaseCalls, [[]]);
  client.assertExhausted();
});

test("a suppressed revoked query values proxy cannot allow commit", async () => {
  const revoked = Proxy.revocable([], {});
  revoked.revoke();
  const client = new FakeClient([
    {},
    timestampResult("2026-07-23T10:11:12.000Z"),
    {},
  ]);
  const pool = new FakePool([client]);
  const store = new PostgresSerializableStore({
    maxTransactionAttempts: 3,
    dedicatedPool: pool,
  });

  await assertStoreError(
    store.runSerializable(async (transaction) => {
      try {
        await transaction.query("SELECT 1", revoked.proxy);
      } catch {
        // The callback cannot suppress an invalid query and then commit.
      }
    }),
    {
      code: "transaction_query_invalid",
      commitState: "not-committed",
    },
  );
  assert.equal(pool.connectCalls, 1);
  assert.deepEqual(
    nonResetQueries(client).map(queryText),
    [
      "BEGIN ISOLATION LEVEL SERIALIZABLE READ WRITE",
      "SELECT transaction_timestamp() AS transaction_timestamp, pg_current_xact_id()::text AS transaction_id",
      "ROLLBACK",
    ],
  );
  assert.deepEqual(client.releaseCalls, [[]]);
  client.assertExhausted();
});

test("a live query values proxy cannot run descriptor traps", async () => {
  let descriptorReads = 0;
  const values = new Proxy([], {
    getOwnPropertyDescriptor(target, property) {
      descriptorReads += 1;
      return Reflect.getOwnPropertyDescriptor(target, property);
    },
  });
  const client = new FakeClient([
    {},
    timestampResult("2026-07-23T10:11:12.000Z"),
    {},
  ]);
  const pool = new FakePool([client]);
  const store = new PostgresSerializableStore({
    maxTransactionAttempts: 3,
    dedicatedPool: pool,
  });

  await assertStoreError(
    store.runSerializable(async (transaction) => {
      try {
        await transaction.query("SELECT 1", values);
      } catch {
        // The callback cannot suppress an invalid query and then commit.
      }
    }),
    {
      code: "transaction_query_invalid",
      commitState: "not-committed",
    },
  );
  assert.equal(descriptorReads, 0);
  assert.deepEqual(
    nonResetQueries(client).map(queryText),
    [
      "BEGIN ISOLATION LEVEL SERIALIZABLE READ WRITE",
      "SELECT transaction_timestamp() AS transaction_timestamp, pg_current_xact_id()::text AS transaction_id",
      "ROLLBACK",
    ],
  );
  assert.deepEqual(client.releaseCalls, [[]]);
  client.assertExhausted();
});

test("an Array-prototype object cannot masquerade as query values", async () => {
  const values = Object.create(Array.prototype);
  Object.defineProperty(values, "length", {
    configurable: true,
    value: 0,
    writable: true,
  });
  const client = new FakeClient([
    {},
    timestampResult("2026-07-23T10:11:12.000Z"),
    {},
  ]);
  const store = new PostgresSerializableStore({
    dedicatedPool: new FakePool([client]),
  });

  await assertStoreError(
    store.runSerializable((transaction) =>
      transaction.query("SELECT 1", values),
    ),
    {
      code: "transaction_query_invalid",
      commitState: "not-committed",
    },
  );
  assert.deepEqual(
    nonResetQueries(client).map(queryText),
    [
      "BEGIN ISOLATION LEVEL SERIALIZABLE READ WRITE",
      "SELECT transaction_timestamp() AS transaction_timestamp, pg_current_xact_id()::text AS transaction_id",
      "ROLLBACK",
    ],
  );
  assert.deepEqual(client.releaseCalls, [[]]);
  client.assertExhausted();
});

test("query values copy owns every slot despite Array prototype accessors", async () => {
  const lastIndex = 65_534;
  const values = new Array(lastIndex + 1);
  Object.defineProperty(values, String(lastIndex), {
    configurable: true,
    enumerable: true,
    value: "safe",
    writable: true,
  });
  const previousDescriptor = Object.getOwnPropertyDescriptor(
    Array.prototype,
    String(lastIndex),
  );
  let inheritedGets = 0;
  let inheritedSets = 0;
  const client = new FakeClient([
    {},
    timestampResult("2026-07-23T10:11:12.000Z"),
    ([config]) => {
      assert.equal(config.values.length, 65_535);
      assert.equal(Object.hasOwn(config.values, String(lastIndex)), true);
      assert.equal(config.values[lastIndex], "safe");
      return { rows: [{ value: "safe" }] };
    },
    transactionIdResult(),
    COMMIT_RESULT,
  ]);
  const store = new PostgresSerializableStore({
    dedicatedPool: new FakePool([client]),
  });

  Object.defineProperty(Array.prototype, String(lastIndex), {
    configurable: true,
    get() {
      inheritedGets += 1;
      return {
        toPostgres() {
          throw new Error("inherited converter must not run");
        },
      };
    },
    set() {
      inheritedSets += 1;
    },
  });
  try {
    assert.equal(
      await store.runSerializable(async (transaction) => {
        const result = await transaction.query("SELECT $1::text", values);
        return result.rows[0].value;
      }),
      "safe",
    );
  } finally {
    if (previousDescriptor === undefined) {
      delete Array.prototype[lastIndex];
    } else {
      Object.defineProperty(
        Array.prototype,
        String(lastIndex),
        previousDescriptor,
      );
    }
  }
  assert.equal(inheritedGets, 0);
  assert.equal(inheritedSets, 0);
  assert.deepEqual(client.releaseCalls, [[]]);
  client.assertExhausted();
});

test("query values reject more than 65,535 parameters before submission", async () => {
  const client = new FakeClient([
    {},
    timestampResult("2026-07-23T10:11:12.000Z"),
    {},
  ]);
  const store = new PostgresSerializableStore({
    dedicatedPool: new FakePool([client]),
  });

  await assertStoreError(
    store.runSerializable((transaction) =>
      transaction.query("SELECT 1", new Array(65_536)),
    ),
    {
      code: "transaction_query_invalid",
      commitState: "not-committed",
    },
  );
  assert.deepEqual(
    nonResetQueries(client).map(queryText),
    [
      "BEGIN ISOLATION LEVEL SERIALIZABLE READ WRITE",
      "SELECT transaction_timestamp() AS transaction_timestamp, pg_current_xact_id()::text AS transaction_id",
      "ROLLBACK",
    ],
  );
  assert.deepEqual(client.releaseCalls, [[]]);
  client.assertExhausted();
});

test("a client-local SQLSTATE-shaped query error is never retried", async () => {
  const localFailure = pgError("40001", "local result parser failed");
  const client = new FakeClient([
    {},
    timestampResult("2026-07-23T10:11:12.000Z"),
    () => {
      throw localFailure;
    },
    {},
  ]);
  const pool = new FakePool([client]);
  const store = new PostgresSerializableStore({
    maxTransactionAttempts: 3,
    dedicatedPool: pool,
  });
  let callbacks = 0;

  await assertStoreError(
    store.runSerializable(async (transaction) => {
      callbacks += 1;
      await transaction.query("SELECT 1");
    }),
    {
      code: "transaction_boundary_lost",
      commitState: "uncertain",
    },
  );
  assert.equal(callbacks, 1);
  assert.equal(pool.connectCalls, 1);
  assert.equal(client.releaseCalls.length, 1);
  assert.equal(client.releaseCalls[0].length, 1);
  assert.ok(client.releaseCalls[0][0] instanceof Error);
  client.assertExhausted();
});

test("a query error marker cannot be replayed into a later attempt", async () => {
  const firstFailure = pgError("40001");
  const first = new FakeClient([
    {},
    timestampResult("2026-07-23T10:11:12.000Z"),
    firstFailure,
    {},
  ]);
  const second = new FakeClient([
    {},
    timestampResult("2026-07-23T10:11:13.000Z"),
    {},
  ]);
  const pool = new FakePool([first, second]);
  const store = new PostgresSerializableStore({
    maxTransactionAttempts: 3,
    dedicatedPool: pool,
  });
  let callbacks = 0;

  await assert.rejects(
    store.runSerializable(async (transaction) => {
      callbacks += 1;
      if (callbacks === 1) {
        await transaction.query("SELECT 1");
      }
      throw firstFailure;
    }),
    (error) => {
      assert.equal(error, firstFailure);
      return true;
    },
  );
  assert.equal(callbacks, 2);
  assert.equal(pool.connectCalls, 2);
  assert.deepEqual(first.releaseCalls, [[]]);
  assert.deepEqual(second.releaseCalls, [[]]);
  first.assertExhausted();
  second.assertExhausted();
});

test("runSerializable retries a server-proved serialization rollback at COMMIT", async () => {
  const commitFailure = pgError("40001", "serialization failure at commit");
  const first = new FakeClient([
    {},
    timestampResult("2026-07-23T10:11:12.000Z"),
    transactionIdResult(),
    commitFailure,
  ]);
  const second = new FakeClient([
    {},
    timestampResult("2026-07-23T10:11:13.000Z"),
    transactionIdResult(),
    COMMIT_RESULT,
  ]);
  const pool = new FakePool([first, second]);
  const store = new PostgresSerializableStore({
    maxTransactionAttempts: 2,
    dedicatedPool: pool,
  });
  let callbacks = 0;

  assert.equal(
    await store.runSerializable(() => {
      callbacks += 1;
      return callbacks;
    }),
    2,
  );
  assert.equal(callbacks, 2);
  assert.equal(pool.connectCalls, 2);
  assert.deepEqual(
    nonResetQueries(first).map(queryText),
    [
      "BEGIN ISOLATION LEVEL SERIALIZABLE READ WRITE",
      "SELECT transaction_timestamp() AS transaction_timestamp, pg_current_xact_id()::text AS transaction_id",
      "SELECT pg_current_xact_id()::text AS transaction_id",
      "COMMIT",
    ],
  );
  assert.deepEqual(first.releaseCalls, [[commitFailure]]);
  assert.deepEqual(second.releaseCalls, [[]]);
  first.assertExhausted();
  second.assertExhausted();
});

test("a protocol marker cannot be replayed by a later local COMMIT error", async () => {
  const reusedFailure = pgError("40001", "reused serialization failure");
  const beginClient = new FakeClient([reusedFailure]);
  const commitClient = new FakeClient([
    {},
    timestampResult("2026-07-23T10:11:13.000Z"),
    transactionIdResult(),
    () => {
      throw reusedFailure;
    },
    {},
  ]);
  const pool = new FakePool([beginClient, commitClient]);
  const store = new PostgresSerializableStore({
    maxTransactionAttempts: 1,
    dedicatedPool: pool,
  });

  await assertStoreError(
    store.runSerializable(() => assert.fail("callback must not run")),
    {
      code: "transaction_begin_failed",
      commitState: "not-committed",
    },
  );
  await assertStoreError(store.runSerializable(() => "value"), {
    code: "transaction_commit_outcome_uncertain",
    commitState: "uncertain",
  });
  assert.equal(pool.connectCalls, 2);
  assert.deepEqual(beginClient.releaseCalls, [[reusedFailure]]);
  assert.deepEqual(commitClient.releaseCalls, [[reusedFailure]]);
  beginClient.assertExhausted();
  commitClient.assertExhausted();
});

test("runSerializable never retries an uncertain failed COMMIT", async () => {
  const commitFailure = pgError("08006", "commit response was lost");
  const client = new FakeClient([
    {},
    timestampResult("2026-07-23T10:11:12.000Z"),
    transactionIdResult(),
    commitFailure,
    {},
  ]);
  const pool = new FakePool([client]);
  const store = new PostgresSerializableStore({
    maxTransactionAttempts: 3,
    dedicatedPool: pool,
  });
  let callbacks = 0;

  await assertStoreError(
    store.runSerializable(() => {
      callbacks += 1;
      return "value";
    }),
    {
      code: "transaction_commit_outcome_uncertain",
      commitState: "uncertain",
      omittedText: commitFailure.message,
    },
  );
  assert.equal(callbacks, 1);
  assert.equal(pool.connectCalls, 1);
  assert.deepEqual(
    nonResetQueries(client).map(queryText),
    [
      "BEGIN ISOLATION LEVEL SERIALIZABLE READ WRITE",
      "SELECT transaction_timestamp() AS transaction_timestamp, pg_current_xact_id()::text AS transaction_id",
      "SELECT pg_current_xact_id()::text AS transaction_id",
      "COMMIT",
      "ROLLBACK",
    ],
  );
  assert.deepEqual(client.releaseCalls, [[commitFailure]]);
  client.assertExhausted();
});

test("rollback failure supersedes a callback failure and destroys the client", async () => {
  const callbackFailure = new Error("application rejected the mutation");
  const rollbackFailure = new Error("connection dropped during rollback");
  const client = new FakeClient([
    {},
    timestampResult("2026-07-23T10:11:12.000Z"),
    rollbackFailure,
  ]);
  const store = new PostgresSerializableStore({
    dedicatedPool: new FakePool([client]),
  });

  await assertStoreError(
    store.runSerializable(() => {
      throw callbackFailure;
    }),
    {
      code: "transaction_rollback_failed",
      commitState: "uncertain",
      omittedText: rollbackFailure.message,
    },
  );
  assert.deepEqual(client.releaseCalls, [[rollbackFailure]]);
  client.assertExhausted();
});

test("a malformed ROLLBACK acknowledgement is uncertain and destroys the client", async () => {
  const callbackFailure = new Error("application rejected the mutation");
  const client = new FakeClient([
    {},
    timestampResult("2026-07-23T10:11:12.000Z"),
    { command: "UPDATE" },
  ]);
  const store = new PostgresSerializableStore({
    dedicatedPool: new FakePool([client]),
  });

  await assertStoreError(
    store.runSerializable(() => {
      throw callbackFailure;
    }),
    {
      code: "transaction_rollback_failed",
      commitState: "uncertain",
      omittedText: callbackFailure.message,
    },
  );
  assert.equal(client.releaseCalls.length, 1);
  assert.equal(client.releaseCalls[0].length, 1);
  assert.ok(client.releaseCalls[0][0] instanceof Error);
  client.assertExhausted();
});

test("release failure after confirmed COMMIT reports committed state", async () => {
  const releaseFailure = new Error(
    "release leaked postgresql://private-authority.invalid/database",
  );
  const client = new FakeClient(
    [
      {},
      timestampResult("2026-07-23T10:11:12.000Z"),
      transactionIdResult(),
      COMMIT_RESULT,
    ],
    { releaseError: releaseFailure },
  );
  const store = new PostgresSerializableStore({
    dedicatedPool: new FakePool([client]),
  });

  await assertStoreError(store.runSerializable(() => "committed"), {
    code: "client_release_failed",
    commitState: "committed",
    omittedText: "private-authority.invalid",
  });
  assert.deepEqual(client.releaseCalls, [[]]);
  client.assertExhausted();
});

test("invalid transaction timestamps roll back without invoking the callback", async () => {
  const client = new FakeClient([{}, timestampResult("not-a-time"), {}]);
  const store = new PostgresSerializableStore({
    dedicatedPool: new FakePool([client]),
  });
  let callbackInvoked = false;

  await assertStoreError(
    store.runSerializable(() => {
      callbackInvoked = true;
    }),
    {
      code: "transaction_timestamp_failed",
      commitState: "not-committed",
    },
  );
  assert.equal(callbackInvoked, false);
  assert.deepEqual(client.releaseCalls, [[]]);
  client.assertExhausted();
});

test("timestamp query serialization errors are not callback retries", async () => {
  const timestampFailure = pgError("40001");
  const client = new FakeClient([{}, timestampFailure, {}]);
  const pool = new FakePool([client]);
  const store = new PostgresSerializableStore({
    maxTransactionAttempts: 3,
    dedicatedPool: pool,
  });

  await assertStoreError(store.runSerializable(() => assert.fail("callback")), {
    code: "transaction_timestamp_failed",
    commitState: "not-committed",
    omittedText: timestampFailure.message,
  });
  assert.equal(pool.connectCalls, 1);
  assert.deepEqual(client.releaseCalls, [[]]);
  client.assertExhausted();
});

test("a callback cannot suppress a rejected transaction query", async () => {
  const queryFailure = pgError(
    "23505",
    "duplicate key from postgresql://private-authority.invalid/database",
  );
  const client = new FakeClient([
    {},
    timestampResult("2026-07-23T10:11:12.000Z"),
    queryFailure,
    {},
  ]);
  const store = new PostgresSerializableStore({
    dedicatedPool: new FakePool([client]),
  });

  await assertStoreError(
    store.runSerializable(async (transaction) => {
      try {
        await transaction.query("INSERT INTO authority VALUES (1)");
      } catch {
        return "must-not-commit";
      }
      return assert.fail("query must reject");
    }),
    {
      code: "transaction_query_failed",
      commitState: "not-committed",
      omittedText: "private-authority.invalid",
    },
  );
  assert.deepEqual(
    nonResetQueries(client).map(queryText),
    [
      "BEGIN ISOLATION LEVEL SERIALIZABLE READ WRITE",
      "SELECT transaction_timestamp() AS transaction_timestamp, pg_current_xact_id()::text AS transaction_id",
      "INSERT INTO authority VALUES (1)",
      "ROLLBACK",
    ],
  );
  assert.deepEqual(client.releaseCalls, [[]]);
  client.assertExhausted();
});

test("a suppressed server serialization error retries the whole callback", async () => {
  const serializationFailure = pgError("40001");
  const first = new FakeClient([
    {},
    timestampResult("2026-07-23T10:11:12.000Z"),
    serializationFailure,
    {},
  ]);
  const second = new FakeClient([
    {},
    timestampResult("2026-07-23T10:11:13.000Z"),
    { rows: [{ value: 2 }] },
    transactionIdResult(),
    COMMIT_RESULT,
  ]);
  const pool = new FakePool([first, second]);
  const store = new PostgresSerializableStore({
    maxTransactionAttempts: 2,
    dedicatedPool: pool,
  });
  let callbacks = 0;

  const result = await store.runSerializable(async (transaction) => {
    callbacks += 1;
    try {
      return await transaction.query("SELECT $1::integer AS value", [
        callbacks,
      ]);
    } catch {
      return { rows: [{ value: -1 }] };
    }
  });
  assert.deepEqual(result, { rows: [{ value: 2 }] });
  assert.equal(callbacks, 2);
  assert.deepEqual(first.releaseCalls, [[]]);
  assert.deepEqual(second.releaseCalls, [[]]);
  first.assertExhausted();
  second.assertExhausted();
});

test("an unsettled callback query is drained and the transaction is rolled back", async () => {
  let finishQuery;
  const delayedQuery = new Promise((resolve) => {
    finishQuery = resolve;
  });
  const client = new FakeClient([
    {},
    timestampResult("2026-07-23T10:11:12.000Z"),
    () => delayedQuery,
    transactionIdResult(),
    {},
  ]);
  const store = new PostgresSerializableStore({
    dedicatedPool: new FakePool([client]),
  });

  const completion = store.runSerializable((transaction) => {
    void transaction.query("SELECT 1");
  });
  setImmediate(() => finishQuery({ rows: [{ "?column?": 1 }] }));
  await assertStoreError(completion, {
    code: "transaction_query_pending",
    commitState: "not-committed",
  });
  assert.deepEqual(
    nonResetQueries(client).map(queryText),
    [
      "BEGIN ISOLATION LEVEL SERIALIZABLE READ WRITE",
      "SELECT transaction_timestamp() AS transaction_timestamp, pg_current_xact_id()::text AS transaction_id",
      "SELECT 1",
      "SELECT pg_current_xact_id()::text AS transaction_id",
      "ROLLBACK",
    ],
  );
  assert.deepEqual(client.releaseCalls, [[]]);
  client.assertExhausted();
});

test("COMMIT command ROLLBACK is proved not committed", async () => {
  const client = new FakeClient([
    {},
    timestampResult("2026-07-23T10:11:12.000Z"),
    transactionIdResult(),
    { command: "ROLLBACK" },
  ]);
  const store = new PostgresSerializableStore({
    dedicatedPool: new FakePool([client]),
  });

  await assertStoreError(store.runSerializable(() => "must-not-return"), {
    code: "transaction_rolled_back",
    commitState: "not-committed",
  });
  assert.deepEqual(client.releaseCalls, [[]]);
  client.assertExhausted();
});

test("malformed COMMIT acknowledgement is uncertain", async () => {
  const client = new FakeClient([
    {},
    timestampResult("2026-07-23T10:11:12.000Z"),
    transactionIdResult(),
    { command: "UPDATE" },
    {},
  ]);
  const store = new PostgresSerializableStore({
    dedicatedPool: new FakePool([client]),
  });

  await assertStoreError(store.runSerializable(() => "must-not-return"), {
    code: "transaction_commit_outcome_uncertain",
    commitState: "uncertain",
  });
  assert.deepEqual(
    nonResetQueries(client).map(queryText),
    [
      "BEGIN ISOLATION LEVEL SERIALIZABLE READ WRITE",
      "SELECT transaction_timestamp() AS transaction_timestamp, pg_current_xact_id()::text AS transaction_id",
      "SELECT pg_current_xact_id()::text AS transaction_id",
      "COMMIT",
      "ROLLBACK",
    ],
  );
  assert.equal(client.releaseCalls.length, 1);
  assert.equal(client.releaseCalls[0].length, 1);
  assert.ok(client.releaseCalls[0][0] instanceof Error);
  client.assertExhausted();
});

test("callback-controlled COMMIT loses the bound transaction and fails closed", async () => {
  const client = new FakeClient([
    {},
    timestampResult("2026-07-23T10:11:12.000Z"),
    COMMIT_RESULT,
    transactionIdResult("101"),
    {},
  ]);
  const store = new PostgresSerializableStore({
    dedicatedPool: new FakePool([client]),
  });

  await assertStoreError(
    store.runSerializable((transaction) => transaction.query("COMMIT")),
    {
      code: "transaction_boundary_lost",
      commitState: "uncertain",
    },
  );
  assert.deepEqual(
    nonResetQueries(client).map(queryText),
    [
      "BEGIN ISOLATION LEVEL SERIALIZABLE READ WRITE",
      "SELECT transaction_timestamp() AS transaction_timestamp, pg_current_xact_id()::text AS transaction_id",
      "COMMIT",
      "SELECT pg_current_xact_id()::text AS transaction_id",
      "ROLLBACK",
    ],
  );
  assert.equal(client.releaseCalls.length, 1);
  assert.equal(client.releaseCalls[0].length, 1);
  client.assertExhausted();
});

test("callback COMMIT followed by an application failure remains uncertain", async () => {
  const applicationFailure = new Error("callback failed after COMMIT");
  const client = new FakeClient([
    {},
    timestampResult("2026-07-23T10:11:12.000Z"),
    COMMIT_RESULT,
    transactionIdResult("101"),
    {},
  ]);
  const store = new PostgresSerializableStore({
    dedicatedPool: new FakePool([client]),
  });

  await assertStoreError(
    store.runSerializable(async (transaction) => {
      try {
        await transaction.query("COMMIT");
      } catch {
        // The callback must not be able to replace the persistent boundary loss.
      }
      throw applicationFailure;
    }),
    {
      code: "transaction_boundary_lost",
      commitState: "uncertain",
      omittedText: applicationFailure.message,
    },
  );
  assert.deepEqual(nonResetQueries(client).map(queryText), [
    "BEGIN ISOLATION LEVEL SERIALIZABLE READ WRITE",
    "SELECT transaction_timestamp() AS transaction_timestamp, pg_current_xact_id()::text AS transaction_id",
    "COMMIT",
    "SELECT pg_current_xact_id()::text AS transaction_id",
    "ROLLBACK",
  ]);
  assert.equal(client.releaseCalls.length, 1);
  assert.equal(client.releaseCalls[0].length, 1);
  client.assertExhausted();
});

test("a local COMMIT timeout without SQLSTATE is a persistent boundary loss", async () => {
  const localTimeout = new Error("Query read timeout");
  const client = new FakeClient([
    {},
    timestampResult("2026-07-23T10:11:12.000Z"),
    localTimeout,
    {},
  ]);
  const store = new PostgresSerializableStore({
    dedicatedPool: new FakePool([client]),
  });

  await assertStoreError(
    store.runSerializable(async (transaction) => {
      try {
        await transaction.query("COMMIT");
      } catch {
        return "must-not-return";
      }
      return assert.fail("local timeout must reject");
    }),
    {
      code: "transaction_boundary_lost",
      commitState: "uncertain",
      omittedText: localTimeout.message,
    },
  );
  assert.deepEqual(nonResetQueries(client).map(queryText), [
    "BEGIN ISOLATION LEVEL SERIALIZABLE READ WRITE",
    "SELECT transaction_timestamp() AS transaction_timestamp, pg_current_xact_id()::text AS transaction_id",
    "COMMIT",
    "ROLLBACK",
  ]);
  assert.equal(client.releaseCalls.length, 1);
  assert.equal(client.releaseCalls[0].length, 1);
  client.assertExhausted();
});

test("unknown-result SQLSTATEs cannot prove callback COMMIT rejection", async (t) => {
  for (const sqlState of ["08007", "40003", "57P01", "58030", "XX000"]) {
    await t.test(sqlState, async () => {
      const queryFailure = pgError(
        sqlState,
        `COMMIT result is unknown for ${sqlState}`,
      );
      const client = new FakeClient([
        {},
        timestampResult("2026-07-23T10:11:12.000Z"),
        queryFailure,
        {},
      ]);
      const dedicatedPool = new FakePool([client]);
      const store = new PostgresSerializableStore({
        dedicatedPool,
        maxTransactionAttempts: 3,
      });

      await assertStoreError(
        store.runSerializable(async (transaction) => {
          try {
            await transaction.query("COMMIT");
          } catch {
            return "must-not-return";
          }
          return assert.fail("unknown COMMIT result must reject");
        }),
        {
          code: "transaction_boundary_lost",
          commitState: "uncertain",
          omittedText: queryFailure.message,
        },
      );
      assert.equal(dedicatedPool.connectCalls, 1);
      assert.deepEqual(nonResetQueries(client).map(queryText), [
        "BEGIN ISOLATION LEVEL SERIALIZABLE READ WRITE",
        "SELECT transaction_timestamp() AS transaction_timestamp, pg_current_xact_id()::text AS transaction_id",
        "COMMIT",
        "ROLLBACK",
      ]);
      assert.equal(client.releaseCalls.length, 1);
      assert.equal(client.releaseCalls[0].length, 1);
      client.assertExhausted();
    });
  }
});

test("a swallowed callback ROLLBACK remains a persistent boundary loss", async () => {
  const client = new FakeClient([
    {},
    timestampResult("2026-07-23T10:11:12.000Z"),
    ROLLBACK_RESULT,
    transactionIdResult("101"),
    {},
  ]);
  const store = new PostgresSerializableStore({
    dedicatedPool: new FakePool([client]),
  });

  await assertStoreError(
    store.runSerializable(async (transaction) => {
      try {
        await transaction.query("ROLLBACK");
      } catch {
        return "must-not-return";
      }
      return assert.fail("ROLLBACK must lose the transaction boundary");
    }),
    {
      code: "transaction_boundary_lost",
      commitState: "uncertain",
    },
  );
  assert.deepEqual(nonResetQueries(client).map(queryText), [
    "BEGIN ISOLATION LEVEL SERIALIZABLE READ WRITE",
    "SELECT transaction_timestamp() AS transaction_timestamp, pg_current_xact_id()::text AS transaction_id",
    "ROLLBACK",
    "SELECT pg_current_xact_id()::text AS transaction_id",
    "ROLLBACK",
  ]);
  assert.equal(client.releaseCalls.length, 1);
  assert.equal(client.releaseCalls[0].length, 1);
  client.assertExhausted();
});

test("concurrent user queries serialize each boundary proof and cannot hide failure", async () => {
  const secondFailure = pgError("23505", "second query failed");
  const client = new FakeClient([
    {},
    timestampResult("2026-07-23T10:11:12.000Z"),
    { rows: [{ value: 1 }] },
    transactionIdResult(),
    secondFailure,
    {},
  ]);
  const store = new PostgresSerializableStore({
    dedicatedPool: new FakePool([client]),
  });

  await assertStoreError(
    store.runSerializable(async (transaction) => {
      const first = transaction.query("SELECT 1 AS value");
      const second = transaction.query("SELECT 2 AS value");
      await Promise.allSettled([first, second]);
      return "must-not-commit";
    }),
    {
      code: "transaction_query_failed",
      commitState: "not-committed",
      omittedText: secondFailure.message,
    },
  );
  assert.deepEqual(nonResetQueries(client).map(queryText), [
    "BEGIN ISOLATION LEVEL SERIALIZABLE READ WRITE",
    "SELECT transaction_timestamp() AS transaction_timestamp, pg_current_xact_id()::text AS transaction_id",
    "SELECT 1 AS value",
    "SELECT pg_current_xact_id()::text AS transaction_id",
    "SELECT 2 AS value",
    "ROLLBACK",
  ]);
  const userQueries = nonResetQueries(client).filter(
    (args) =>
      args[0] !== "BEGIN ISOLATION LEVEL SERIALIZABLE READ WRITE" &&
      typeof args[0] === "object",
  );
  assert.deepEqual(
    userQueries.map(([config]) => config.queryMode),
    ["extended", "extended"],
  );
  assert.deepEqual(client.releaseCalls, [[]]);
  client.assertExhausted();
});

test("multi-statement text is submitted only through extended protocol", async () => {
  const parseFailure = pgError(
    "42601",
    "cannot insert multiple commands into a prepared statement",
  );
  const client = new FakeClient([
    {},
    timestampResult("2026-07-23T10:11:12.000Z"),
    parseFailure,
    {},
  ]);
  const store = new PostgresSerializableStore({
    dedicatedPool: new FakePool([client]),
  });

  await assertStoreError(
    store.runSerializable((transaction) =>
      transaction.query("SELECT 1; COMMIT"),
    ),
    {
      code: "transaction_query_failed",
      commitState: "not-committed",
      omittedText: parseFailure.message,
    },
  );
  const userQuery = nonResetQueries(client).find(
    ([value]) => typeof value === "object",
  );
  assert.deepEqual(userQuery, [
    {
      queryMode: "extended",
      text: "SELECT 1; COMMIT",
      values: [],
    },
  ]);
  assert.deepEqual(client.releaseCalls, [[]]);
  client.assertExhausted();
});

test("client reset is required before a callback can run", async () => {
  const resetFailure = new Error("stale session could not be reset");
  const client = new FakeClient([], { resetSteps: [resetFailure] });
  const store = new PostgresSerializableStore({
    dedicatedPool: new FakePool([client]),
  });
  let callbackInvoked = false;

  await assertStoreError(
    store.runSerializable(() => {
      callbackInvoked = true;
    }),
    {
      code: "client_reset_failed",
      commitState: "not-committed",
      omittedText: resetFailure.message,
    },
  );
  assert.equal(callbackInvoked, false);
  assert.deepEqual(client.queries, [["DISCARD ALL"]]);
  assert.deepEqual(client.releaseCalls, [[resetFailure]]);
  client.assertExhausted();
});

test("a malformed client reset acknowledgement destroys the client", async () => {
  const client = new FakeClient([], {
    resetSteps: [{ command: "RESET" }],
  });
  const store = new PostgresSerializableStore({
    dedicatedPool: new FakePool([client]),
  });

  await assertStoreError(store.runSerializable(() => assert.fail()), {
    code: "client_reset_failed",
    commitState: "not-committed",
  });
  assert.equal(client.releaseCalls.length, 1);
  assert.equal(client.releaseCalls[0].length, 1);
  assert.ok(client.releaseCalls[0][0] instanceof Error);
  client.assertExhausted();
});

test("reset failure after COMMIT preserves the committed outcome", async () => {
  const resetFailure = new Error("post-commit reset failed");
  const client = new FakeClient(
    [
      {},
      timestampResult("2026-07-23T10:11:12.000Z"),
      transactionIdResult(),
      COMMIT_RESULT,
    ],
    { resetSteps: [DISCARD_RESULT, resetFailure] },
  );
  const store = new PostgresSerializableStore({
    dedicatedPool: new FakePool([client]),
  });

  await assertStoreError(store.runSerializable(() => "committed"), {
    code: "client_reset_failed",
    commitState: "committed",
    omittedText: resetFailure.message,
  });
  assert.deepEqual(client.releaseCalls, [[resetFailure]]);
  client.assertExhausted();
});

test("reset failure after ROLLBACK preserves the not-committed outcome", async () => {
  const applicationFailure = new Error("application rollback");
  const resetFailure = new Error("post-rollback reset failed");
  const client = new FakeClient(
    [
      {},
      timestampResult("2026-07-23T10:11:12.000Z"),
      {},
    ],
    { resetSteps: [DISCARD_RESULT, resetFailure] },
  );
  const store = new PostgresSerializableStore({
    dedicatedPool: new FakePool([client]),
  });

  await assertStoreError(
    store.runSerializable(() => {
      throw applicationFailure;
    }),
    {
      code: "client_reset_failed",
      commitState: "not-committed",
      omittedText: resetFailure.message,
    },
  );
  assert.deepEqual(client.releaseCalls, [[resetFailure]]);
  client.assertExhausted();
});

test("migrate applies the checksum-bound migration in one transaction", async () => {
  const sql = await readFile(
    new URL("../migrations/authority/001-session-authority.sql", import.meta.url),
    "utf8",
  );
  const checksum = createHash("sha256").update(sql, "utf8").digest("hex");
  const client = new FakeClient([
    {},
    {},
    {},
    {},
    { rows: [] },
    {},
    {},
    COMMIT_RESULT,
  ]);
  const store = new PostgresSerializableStore({
    dedicatedPool: new FakePool([client]),
  });

  const result = await store.migrate();

  assert.deepEqual(result, {
    applied: true,
    checksum,
    version: SESSION_AUTHORITY_MIGRATION_VERSION,
  });
  assert.equal(Object.isFrozen(result), true);
  const migrationQueries = nonResetQueries(client);
  assert.deepEqual(migrationQueries[0], ["BEGIN"]);
  assert.deepEqual(migrationQueries[1], [
    "SELECT pg_advisory_xact_lock($1::bigint)",
    ["7275632827684484689"],
  ]);
  assert.deepEqual(migrationQueries[2], [
    "CREATE SCHEMA IF NOT EXISTS session_authority",
  ]);
  assert.match(migrationQueries[3][0], /schema_migrations/u);
  assert.deepEqual(migrationQueries[4], [
    "SELECT version, checksum FROM session_authority.schema_migrations ORDER BY version",
  ]);
  assert.deepEqual(migrationQueries[5], [sql]);
  assert.deepEqual(migrationQueries[6][1], [1, checksum]);
  assert.deepEqual(migrationQueries[7], ["COMMIT"]);
  assert.deepEqual(client.queries.at(0), ["DISCARD ALL"]);
  assert.deepEqual(client.queries.at(-1), ["DISCARD ALL"]);
  assert.deepEqual(client.releaseCalls, [[]]);
  assert.match(sql, /revision bigint NOT NULL DEFAULT 0/u);
  assert.match(sql, /operation_claims_one_active_per_session[\s\S]+WHERE retired_at IS NULL/u);
  assert.match(sql, /reservations_one_active_per_session[\s\S]+WHERE released_at IS NULL/u);
  assert.match(sql, /operation_id character varying\(128\) PRIMARY KEY/u);
  assert.match(sql, /capture_attempt_tombstones/u);
  client.assertExhausted();
});

test("migrate destroys a client when its post-COMMIT reset fails", async () => {
  const resetFailure = new Error("migration reset failed");
  const client = new FakeClient(
    [
      {},
      {},
      {},
      {},
      { rows: [] },
      {},
      {},
      COMMIT_RESULT,
    ],
    { resetSteps: [DISCARD_RESULT, resetFailure] },
  );
  const store = new PostgresSerializableStore({
    dedicatedPool: new FakePool([client]),
  });

  await assertStoreError(store.migrate(), {
    code: "client_reset_failed",
    commitState: "committed",
    omittedText: resetFailure.message,
  });
  assert.equal(queryText(nonResetQueries(client).at(-1)), "COMMIT");
  assert.deepEqual(client.releaseCalls, [[resetFailure]]);
  client.assertExhausted();
});

test("migrate accepts the exact installed checksum without reapplying SQL", async () => {
  const sql = await readFile(
    new URL("../migrations/authority/001-session-authority.sql", import.meta.url),
    "utf8",
  );
  const checksum = createHash("sha256").update(sql, "utf8").digest("hex");
  const client = new FakeClient([
    {},
    {},
    {},
    {},
    { rows: [{ checksum, version: 1 }] },
    COMMIT_RESULT,
  ]);
  const store = new PostgresSerializableStore({
    dedicatedPool: new FakePool([client]),
  });

  assert.deepEqual(await store.migrate(), {
    applied: false,
    checksum,
    version: 1,
  });
  assert.deepEqual(
    nonResetQueries(client).map(queryText),
    [
      "BEGIN",
      "SELECT pg_advisory_xact_lock($1::bigint)",
      "CREATE SCHEMA IF NOT EXISTS session_authority",
      nonResetQueries(client)[3][0],
      "SELECT version, checksum FROM session_authority.schema_migrations ORDER BY version",
      "COMMIT",
    ],
  );
  assert.deepEqual(client.releaseCalls, [[]]);
  client.assertExhausted();
});

test("migrate rolls back an installed checksum mismatch", async () => {
  const client = new FakeClient([
    {},
    {},
    {},
    {},
    { rows: [{ checksum: "0".repeat(64), version: 1 }] },
    {},
  ]);
  const store = new PostgresSerializableStore({
    dedicatedPool: new FakePool([client]),
  });

  await assertStoreError(store.migrate(), {
    code: "migration_checksum_mismatch",
    commitState: "not-committed",
  });
  assert.equal(queryText(nonResetQueries(client).at(-1)), "ROLLBACK");
  assert.deepEqual(client.releaseCalls, [[]]);
  client.assertExhausted();
});

test("migrate rejects a future-only migration ledger", async () => {
  const client = new FakeClient([
    {},
    {},
    {},
    {},
    { rows: [{ checksum: "0".repeat(64), version: 2 }] },
    {},
  ]);
  const store = new PostgresSerializableStore({
    dedicatedPool: new FakePool([client]),
  });

  await assertStoreError(store.migrate(), {
    code: "migration_state_invalid",
    commitState: "not-committed",
  });
  assert.equal(queryText(nonResetQueries(client).at(-1)), "ROLLBACK");
  assert.deepEqual(client.releaseCalls, [[]]);
  client.assertExhausted();
});

test("migrate rejects an exact v1 row accompanied by any extra version", async () => {
  const sql = await readFile(
    new URL("../migrations/authority/001-session-authority.sql", import.meta.url),
    "utf8",
  );
  const checksum = createHash("sha256").update(sql, "utf8").digest("hex");
  const client = new FakeClient([
    {},
    {},
    {},
    {},
    {
      rows: [
        { checksum, version: 1 },
        { checksum: "0".repeat(64), version: 2 },
      ],
    },
    {},
  ]);
  const store = new PostgresSerializableStore({
    dedicatedPool: new FakePool([client]),
  });

  await assertStoreError(store.migrate(), {
    code: "migration_state_invalid",
    commitState: "not-committed",
  });
  assert.equal(queryText(nonResetQueries(client).at(-1)), "ROLLBACK");
  assert.deepEqual(client.releaseCalls, [[]]);
  client.assertExhausted();
});

test("migrate rejects a COMMIT acknowledgement that reports ROLLBACK", async () => {
  const client = new FakeClient([
    {},
    {},
    {},
    {},
    { rows: [] },
    {},
    {},
    { command: "ROLLBACK" },
  ]);
  const store = new PostgresSerializableStore({
    dedicatedPool: new FakePool([client]),
  });

  await assertStoreError(store.migrate(), {
    code: "migration_failed",
    commitState: "not-committed",
  });
  assert.equal(queryText(nonResetQueries(client).at(-1)), "COMMIT");
  assert.deepEqual(client.releaseCalls, [[]]);
  client.assertExhausted();
});

test("migrate treats a failed COMMIT as uncertain and never reapplies", async () => {
  const commitFailure = new Error("migration commit response was lost");
  const client = new FakeClient([
    {},
    {},
    {},
    {},
    { rows: [] },
    {},
    {},
    commitFailure,
    {},
  ]);
  const pool = new FakePool([client]);
  const store = new PostgresSerializableStore({ dedicatedPool: pool });

  await assertStoreError(store.migrate(), {
    code: "transaction_commit_outcome_uncertain",
    commitState: "uncertain",
    omittedText: commitFailure.message,
  });
  assert.equal(pool.connectCalls, 1);
  assert.deepEqual(
    nonResetQueries(client).slice(-2).map(queryText),
    ["COMMIT", "ROLLBACK"],
  );
  assert.deepEqual(client.releaseCalls, [[commitFailure]]);
  client.assertExhausted();
});

test("constructor and query APIs reject shapes that could escape tracking", async () => {
  assert.throws(
    () => new PostgresSerializableStore({ pool: {} }),
    /unexpected or missing fields/u,
  );
  assert.throws(
    () => new PostgresSerializableStore({}),
    /unexpected or missing fields/u,
  );
  assert.throws(
    () => new PostgresSerializableStore({ dedicatedPool: {} }),
    /dedicatedPool must provide connect/u,
  );
  assert.throws(
    () =>
      new PostgresSerializableStore({
        maxTransactionAttempts: 17,
        dedicatedPool: new FakePool([]),
      }),
    /maxTransactionAttempts/u,
  );

  const client = new FakeClient([
    {},
    timestampResult("2026-07-23T10:11:12.000Z"),
    {},
  ]);
  const store = new PostgresSerializableStore({
    dedicatedPool: new FakePool([client]),
  });
  await assertStoreError(
    store.runSerializable((transaction) =>
      transaction.query({ text: "SELECT 1" }),
    ),
    {
      code: "transaction_query_invalid",
      commitState: "not-committed",
    },
  );
  assert.deepEqual(client.releaseCalls, [[]]);
  client.assertExhausted();
});
