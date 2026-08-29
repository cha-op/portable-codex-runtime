import assert from "node:assert/strict";
import { Buffer } from "node:buffer";
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { EventEmitter } from "node:events";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import test from "node:test";

import { Client as PgClient, DatabaseError, Query } from "pg";

import {
  PostgresSerializableStore,
  PostgresSerializableStoreError,
  SESSION_AUTHORITY_MIGRATION_VERSION,
  consumePostgresSerializableTransactionRows,
  isPostgresSerializableStore,
} from "../src/postgres-serializable-store.mjs";

const COMMIT_RESULT = Object.freeze({ command: "COMMIT" });
const DISCARD_RESULT = Object.freeze({ command: "DISCARD" });
const ROLLBACK_RESULT = Object.freeze({ command: "ROLLBACK" });
const SET_RESULT = Object.freeze({ command: "SET" });
const DURABLE_COMMIT_QUERY = "SET LOCAL synchronous_commit = on";
const MIGRATION_SEARCH_PATH_QUERY =
  "SET LOCAL search_path = pg_catalog";
const FIRE_AND_FORGET_FIXTURE = fileURLToPath(
  new URL(
    "./fixtures/postgres-fire-and-forget-rejection.mjs",
    import.meta.url,
  ),
);
const INTRINSIC_POISONING_FIXTURE = fileURLToPath(
  new URL(
    "./fixtures/postgres-intrinsic-poisoning.mjs",
    import.meta.url,
  ),
);
const AUTHORITY_MIGRATION_URLS = Object.freeze([
  new URL(
    "../migrations/authority/001-session-authority.sql",
    import.meta.url,
  ),
  new URL(
    "../migrations/authority/002-restore-destination-generations.sql",
    import.meta.url,
  ),
  new URL(
    "../migrations/authority/003-operation-id-registry.sql",
    import.meta.url,
  ),
  new URL(
    "../migrations/authority/004-restore-attachment-activation.sql",
    import.meta.url,
  ),
  new URL(
    "../migrations/authority/005-restore-recovery-cursors.sql",
    import.meta.url,
  ),
  new URL(
    "../migrations/authority/006-writer-stop-capture-handoff.sql",
    import.meta.url,
  ),
  new URL(
    "../migrations/authority/007-detached-restore-stable-plans.sql",
    import.meta.url,
  ),
  new URL(
    "../migrations/authority/008-filesystem-image-provider-heads.sql",
    import.meta.url,
  ),
  new URL(
    "../migrations/authority/009-writer-supervisor-state-gc.sql",
    import.meta.url,
  ),
  new URL(
    "../migrations/authority/010-filesystem-image-provider-operations.sql",
    import.meta.url,
  ),
  new URL(
    "../migrations/authority/011-filesystem-image-provider-state-v3-adoption.sql",
    import.meta.url,
  ),
  new URL(
    "../migrations/authority/012-atomic-crash-capture-catalogue.sql",
    import.meta.url,
  ),
  new URL(
    "../migrations/authority/013-writer-fence-atomic-capture-handoff.sql",
    import.meta.url,
  ),
]);

class FakeClient {
  constructor(
    steps,
    {
      durabilityBoundarySteps,
      durabilitySteps,
      releaseError,
      resetSteps = [],
    } = {},
  ) {
    this.connection = new EventEmitter();
    this.portalExecutions = [];
    this.portalFlushes = 0;
    this.portalSyncs = 0;
    this.connection.execute = (config) => {
      this.portalExecutions.push(config);
    };
    this.connection.flush = () => {
      this.portalFlushes += 1;
    };
    this.connection.sync = () => {
      this.portalSyncs += 1;
    };
    this.durabilityBoundaryPending = false;
    this.durabilityBoundarySteps =
      durabilityBoundarySteps === undefined
        ? undefined
        : [...durabilityBoundarySteps];
    this.durabilitySteps =
      durabilitySteps === undefined ? undefined : [...durabilitySteps];
    this.queries = [];
    this.releaseCalls = [];
    this.releaseError = releaseError;
    this.resetSteps = [...resetSteps];
    this.steps = [...steps];
  }

  query(...args) {
    this.queries.push(args);
    const text = queryText(args);
    if (text === "DISCARD ALL") {
      if (this.resetSteps.length === 0) return DISCARD_RESULT;
      const resetStep = this.resetSteps.shift();
      if (resetStep instanceof Error) throw resetStep;
      return resetStep;
    }
    if (text === DURABLE_COMMIT_QUERY) {
      assert.deepEqual(args, [
        {
          queryMode: "extended",
          text: DURABLE_COMMIT_QUERY,
          values: [],
        },
      ]);
      if (this.durabilitySteps === undefined) {
        this.durabilityBoundaryPending = true;
        return SET_RESULT;
      }
      assert.notEqual(
        this.durabilitySteps.length,
        0,
        `unexpected durability query: ${text}`,
      );
      const durabilityStep = this.durabilitySteps.shift();
      if (typeof durabilityStep === "function") {
        return durabilityStep(args);
      }
      if (durabilityStep instanceof Error) {
        if (durabilityStep instanceof DatabaseError) {
          this.connection.emit("errorMessage", durabilityStep);
        }
        throw durabilityStep;
      }
      this.durabilityBoundaryPending =
        durabilityStep?.command === "SET";
      return durabilityStep;
    }
    if (text === MIGRATION_SEARCH_PATH_QUERY) {
      return SET_RESULT;
    }
    if (
      text === "SELECT pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id" &&
      this.durabilityBoundaryPending
    ) {
      this.durabilityBoundaryPending = false;
      if (this.durabilityBoundarySteps !== undefined) {
        assert.notEqual(
          this.durabilityBoundarySteps.length,
          0,
          `unexpected durability boundary query: ${text}`,
        );
        const boundaryStep = this.durabilityBoundarySteps.shift();
        if (typeof boundaryStep === "function") {
          return boundaryStep(args);
        }
        if (boundaryStep instanceof Error) {
          if (boundaryStep instanceof DatabaseError) {
            this.connection.emit("errorMessage", boundaryStep);
          }
          throw boundaryStep;
        }
        return boundaryStep;
      }
      const candidate = this.steps[0];
      const candidateRows = candidate?.rows;
      if (
        !Array.isArray(candidateRows) ||
        candidateRows[0]?.transaction_id === undefined
      ) {
        return transactionIdResult();
      }
    }
    assert.notEqual(this.steps.length, 0, `unexpected query: ${text}`);
    const step = this.steps.shift();
    if (typeof step === "function") return step(args, this);
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
    assert.equal(this.durabilityBoundaryPending, false);
    assert.deepEqual(this.durabilityBoundarySteps ?? [], []);
    assert.deepEqual(this.durabilitySteps ?? [], []);
    assert.deepEqual(this.resetSteps, []);
    assert.equal(this.connection.listenerCount("errorMessage"), 0);
  }
}

class PgTimeoutFakeClient extends FakeClient {
  constructor(steps, queryTimeout) {
    super(steps);
    this._Promise = Promise;
    this._ending = false;
    this._queryQueue = [];
    this._queryable = true;
    this._types = undefined;
    this.binary = false;
    this.connectionParameters = { query_timeout: queryTimeout };
    this.pendingStreamSteps = [];
    this.streamPulseCalls = 0;
  }

  query(...args) {
    const query = args[0];
    if (
      query !== null &&
      typeof query === "object" &&
      query.queryMode === "extended" &&
      query.rows === 1024 &&
      typeof query.submit === "function"
    ) {
      this.queries.push(args);
      assert.notEqual(this.steps.length, 0, "missing stream query step");
      this.pendingStreamSteps.push(this.steps.shift());
      return Reflect.apply(PgClient.prototype.query, this, args);
    }
    return super.query(...args);
  }

  _pulseQueryQueue() {
    this.streamPulseCalls += 1;
    const query = this._queryQueue.shift();
    if (query === undefined) return;
    const step = this.pendingStreamSteps.shift();
    assert.equal(typeof step, "function");
    step([query], this);
  }
}

class PgProtocolLifecycleFakeClient extends PgTimeoutFakeClient {
  constructor(steps, queryTimeout, { syncError } = {}) {
    super(steps, queryTimeout);
    this._activeQuery = null;
    this._connecting = false;
    this.readyForQuery = true;
    this.pendingProtocolSteps = [];
    this.protocolReadyMessages = 0;
    this.rollbackSubmissionStates = [];
    this.syncError = syncError;
    this.errorMessageHandler = (message) =>
      Reflect.apply(PgClient.prototype._handleErrorMessage, this, [
        message,
      ]);
    this.readyForQueryHandler = (message) => {
      this.protocolReadyMessages += 1;
      return Reflect.apply(PgClient.prototype._handleReadyForQuery, this, [
        message,
      ]);
    };
    this.connection.on("errorMessage", this.errorMessageHandler);
    this.connection.on("readyForQuery", this.readyForQueryHandler);
    this.connection.sync = () => {
      this.portalSyncs += 1;
      if (this.syncError !== undefined) throw this.syncError;
      queueMicrotask(() => {
        this.connection.emit("readyForQuery", { status: "E" });
      });
    };
  }

  query(...args) {
    if (queryText(args) === "ROLLBACK") {
      this.queries.push(args);
      this.rollbackSubmissionStates.push(this.readyForQuery);
      assert.notEqual(this.steps.length, 0, "missing ROLLBACK step");
      this.pendingProtocolSteps.push(this.steps.shift());
      return Reflect.apply(PgClient.prototype.query, this, args);
    }
    return super.query(...args);
  }

  _getActiveQuery() {
    return this._activeQuery;
  }

  _pulseQueryQueue() {
    this.streamPulseCalls += 1;
    if (this.readyForQuery !== true) return;
    const query = this._queryQueue.shift();
    if (query === undefined) return;
    this._activeQuery = query;
    this.readyForQuery = false;
    if (query.rows === 1024 && query.queryMode === "extended") {
      const step = this.pendingStreamSteps.shift();
      assert.equal(typeof step, "function");
      step([query], this);
      return;
    }
    assert.equal(query.text, "ROLLBACK");
    const step = this.pendingProtocolSteps.shift();
    assert.equal(step?.command, "ROLLBACK");
    queueMicrotask(() => {
      query.handleCommandComplete(
        { text: step.command },
        this.connection,
      );
      this.connection.emit("readyForQuery", { status: "I" });
    });
  }

  assertExhausted() {
    this.connection.removeListener(
      "errorMessage",
      this.errorMessageHandler,
    );
    this.connection.removeListener(
      "readyForQuery",
      this.readyForQueryHandler,
    );
    assert.deepEqual(this.pendingProtocolSteps, []);
    assert.deepEqual(this.pendingStreamSteps, []);
    assert.deepEqual(this._queryQueue, []);
    super.assertExhausted();
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

function streamedRowsStep(
  rows,
  {
    afterRow,
    afterTerminal,
    beforeCommand,
    beforeReady,
    commandMessages,
    error,
    inspectQuery,
    protocolRows = 0,
    rawRows = false,
    returnWrongIdentity = false,
    schedule = queueMicrotask,
    throwAfterTerminal,
  } = {},
) {
  let fieldNames = [];
  if (!rawRows && rows.length !== 0) {
    fieldNames = Object.keys(rows[0]);
    for (let index = 0; index < rows.length; index += 1) {
      assert.deepEqual(Object.keys(rows[index]), fieldNames);
    }
  }
  const fieldDescriptions = fieldNames.map((name) => ({
    dataTypeID:
      typeof rows[0][name] === "number" ? 23 : 25,
    format: "text",
    name,
  }));
  const rowMessages = rawRows
    ? []
    : rows.map((row) => ({
        fields: fieldNames.map((name) =>
          row[name] === null ? null : String(row[name]),
        ),
      }));
  const totalRows = protocolRows + rows.length;
  return (args, client) => {
    assert.equal(args.length, 1);
    const query = args[0];
    assert.equal(query.queryMode, "extended");
    assert.equal(query.rows, 1024);
    assert.equal(typeof query.submit, "function");
    assert.equal(query.listeners("row").length, 1);
    inspectQuery?.(query);
    schedule(() => {
      query.handleRowDescription({ fields: fieldDescriptions });
      assert.equal(query._accumulateRows, false);
      let deliveredRows = 0;
      const recordDelivery = () => {
        deliveredRows += 1;
        if (deliveredRows % 1024 === 0) {
          query.handlePortalSuspended(client.connection);
        }
      };
      for (let index = 0; index < protocolRows; index += 1) {
        query.handleDataRow({ fields: [] });
        recordDelivery();
      }
      for (let index = 0; index < rows.length; index += 1) {
        if (rawRows) {
          query.emit("row", rows[index], query._result);
        } else {
          query.handleDataRow(rowMessages[index]);
        }
        afterRow?.(rows[index], index);
        recordDelivery();
      }
      assert.deepEqual(query._result.rows, []);
      if (error !== undefined) {
        if (error instanceof DatabaseError) {
          client.connection.emit("errorMessage", error);
        }
        query.handleError(error, client.connection);
      } else {
        beforeCommand?.(query, client);
        const messages =
          commandMessages ?? [{ text: `SELECT ${totalRows % 1024}` }];
        for (let index = 0; index < messages.length; index += 1) {
          query.handleCommandComplete(messages[index], client.connection);
        }
        beforeReady?.(query, client);
        query.handleReadyForQuery(client.connection);
      }
      afterTerminal?.(query, client);
    });
    if (throwAfterTerminal !== undefined) throw throwAfterTerminal;
    return returnWrongIdentity ? Object.freeze({}) : query;
  };
}

function protocolErrorResponseStep(error, inspectQuery) {
  return (args, client) => {
    assert.equal(args.length, 1);
    const query = args[0];
    assert.equal(query.queryMode, "extended");
    assert.equal(query.rows, 1024);
    assert.equal(client._activeQuery, query);
    assert.equal(client.readyForQuery, false);
    inspectQuery?.(query);
    client.connection.emit("errorMessage", error);
    return query;
  };
}

function primeStreamParserFailure(query, error) {
  query.handleRowDescription({
    fields: [
      {
        dataTypeID: 23,
        format: "text",
        name: "value",
      },
    ],
  });
  query._result._parsers[0] = () => {
    throw error;
  };
  query.handleDataRow({ fields: ["1"] });
  assert.equal(query._canceledDueToError, error);
  assert.deepEqual(query._result.rows, []);
}

function nonResetQueries(client) {
  return client.queries.filter((args) => queryText(args) !== "DISCARD ALL");
}

async function readAuthorityMigrations() {
  const migrations = [];
  for (let index = 0; index < AUTHORITY_MIGRATION_URLS.length; index += 1) {
    const sql = await readFile(AUTHORITY_MIGRATION_URLS[index], "utf8");
    migrations.push({
      checksum: createHash("sha256").update(sql, "utf8").digest("hex"),
      sql,
      version: index + 1,
    });
  }
  return migrations;
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
      "SELECT pg_catalog.transaction_timestamp() AS transaction_timestamp, pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id",
    ],
    [
      {
        queryMode: "extended",
        text: "SELECT $1::integer AS value",
        values: [7],
      },
    ],
    ["SELECT pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id"],
    [
      {
        queryMode: "extended",
        text: DURABLE_COMMIT_QUERY,
        values: [],
      },
    ],
    ["SELECT pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id"],
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

test("row streaming is exact, branded, native, and absent from the transaction surface", async () => {
  assert.equal(consumePostgresSerializableTransactionRows.length, 4);
  assert.equal(
    Object.isFrozen(consumePostgresSerializableTransactionRows),
    true,
  );
  await assertStoreError(
    consumePostgresSerializableTransactionRows(
      Object.freeze({ now: "2026-07-23T10:11:12.000Z", query() {} }),
      "SELECT 1",
      [],
      () => undefined,
    ),
    {
      code: "transaction_query_invalid",
      commitState: "not-committed",
    },
  );
  await assertStoreError(
    consumePostgresSerializableTransactionRows({}, "SELECT 1", []),
    {
      code: "transaction_query_invalid",
      commitState: "not-committed",
    },
  );

  const client = new FakeClient([
    {},
    timestampResult("2026-07-23T10:11:12.000Z"),
    streamedRowsStep([]),
    transactionIdResult(),
    COMMIT_RESULT,
  ]);
  const store = new PostgresSerializableStore({
    dedicatedPool: new FakePool([client]),
  });
  let retainedTransaction;
  const result = await store.runSerializable(async (transaction) => {
    retainedTransaction = transaction;
    assert.deepEqual(Reflect.ownKeys(transaction), ["now", "query"]);
    const completion = consumePostgresSerializableTransactionRows(
      transaction,
      "SELECT 1 WHERE false",
      [],
      () => undefined,
    );
    assert.equal(Object.getPrototypeOf(completion), Promise.prototype);
    assert.equal(await completion, undefined);
    return "committed";
  });
  assert.equal(result, "committed");
  await assertStoreError(
    consumePostgresSerializableTransactionRows(
      retainedTransaction,
      "SELECT 1",
      [],
      () => undefined,
    ),
    {
      code: "transaction_query_inactive",
      commitState: "not-committed",
    },
  );
  client.assertExhausted();
});

test("row streaming copies 0, 1, and 65535 primitive parameters", async (t) => {
  for (const count of [0, 1, 65_535]) {
    await t.test(String(count), async () => {
      const values = new Array(count).fill(count);
      const client = new FakeClient([
        {},
        timestampResult("2026-07-23T10:11:12.000Z"),
        streamedRowsStep([], {
          inspectQuery(query) {
            assert.equal(query.text, "SELECT $1");
            assert.equal(query.values.length, count);
            assert.equal(Object.isFrozen(query.values), true);
            if (count !== 0) {
              assert.equal(query.values[0], count);
              assert.equal(query.values[count - 1], count);
            }
          },
        }),
        transactionIdResult(),
        COMMIT_RESULT,
      ]);
      const store = new PostgresSerializableStore({
        dedicatedPool: new FakePool([client]),
      });
      await store.runSerializable((transaction) =>
        consumePostgresSerializableTransactionRows(
          transaction,
          "SELECT $1",
          values,
          () => undefined,
        ),
      );
      client.assertExhausted();
    });
  }
});

test(
  "row streaming counts complete portal fetches without accumulating Result rows",
  async (t) => {
    for (const rowCount of [1_024, 1_025, 2_048, 2_049]) {
      await t.test(String(rowCount), async () => {
        const client = new FakeClient([
          {},
          timestampResult("2026-07-23T10:11:12.000Z"),
          streamedRowsStep([], { protocolRows: rowCount }),
          transactionIdResult(),
          COMMIT_RESULT,
        ]);
        const store = new PostgresSerializableStore({
          dedicatedPool: new FakePool([client]),
        });
        let count = 0;
        await store.runSerializable((transaction) =>
          consumePostgresSerializableTransactionRows(
            transaction,
            "SELECT operation_id FROM session_authority.operations",
            [],
            () => {
              count += 1;
            },
          ),
        );
        assert.equal(count, rowCount);
        assert.equal(
          nonResetQueries(client).filter(
            (args) =>
              queryText(args) ===
              "SELECT operation_id FROM session_authority.operations",
          ).length,
          1,
        );
        const portalSuspensionCount = Math.floor(rowCount / 1024);
        assert.deepEqual(
          client.portalExecutions,
          new Array(portalSuspensionCount).fill({ portal: "", rows: 1024 }),
        );
        assert.equal(client.portalFlushes, portalSuspensionCount);
        assert.equal(client.portalSyncs, 1);
        client.assertExhausted();
      });
    }
  },
);

test("pg Client.query keeps streaming callbacks hidden with and without a global timeout", async (t) => {
  for (const queryTimeout of [0, 25]) {
    await t.test(
      queryTimeout === 0 ? "no-timeout-wrapper" : "global-timeout-cleared",
      async () => {
        const rowCount = queryTimeout === 0 ? 1 : 2_049;
        let terminalRows;
        const client = new PgTimeoutFakeClient(
          [
            {},
            timestampResult("2026-07-23T10:11:12.000Z"),
            streamedRowsStep([], {
              afterTerminal(query) {
                terminalRows = query._result.rows;
              },
              inspectQuery(query) {
                assert.equal(query.callback, undefined);
                const descriptor = Object.getOwnPropertyDescriptor(
                  query,
                  "callback",
                );
                assert.equal(typeof descriptor.get, "function");
                assert.equal(typeof descriptor.set, "function");
              },
              protocolRows: rowCount,
              schedule: setImmediate,
            }),
            transactionIdResult(),
            COMMIT_RESULT,
          ],
          queryTimeout,
        );
        const store = new PostgresSerializableStore({
          dedicatedPool: new FakePool([client]),
        });
        let observedRows = 0;
        await store.runSerializable(async (transaction) => {
          await consumePostgresSerializableTransactionRows(
            transaction,
            "SELECT value FROM streamed_rows",
            [],
            () => {
              observedRows += 1;
            },
          );
          await new Promise((resolve) =>
            setTimeout(resolve, queryTimeout === 0 ? 5 : 50),
          );
        });
        assert.equal(observedRows, rowCount);
        assert.deepEqual(terminalRows, []);
        assert.equal(client.streamPulseCalls, 1);
        client.assertExhausted();
      },
    );
  }
});

test("pg ErrorResponse synchronizes once and pulses a real queued ROLLBACK", async () => {
  const serverError = pgError("23505", "streamed duplicate");
  let streamedQuery;
  const client = new PgProtocolLifecycleFakeClient(
    [
      {},
      timestampResult("2026-07-23T10:11:12.000Z"),
      protocolErrorResponseStep(serverError, (query) => {
        streamedQuery = query;
      }),
      ROLLBACK_RESULT,
    ],
    25,
  );
  const store = new PostgresSerializableStore({
    dedicatedPool: new FakePool([client]),
  });
  await assertStoreError(
    store.runSerializable((transaction) =>
      consumePostgresSerializableTransactionRows(
        transaction,
        "SELECT value FROM streamed_rows",
        [],
        () => undefined,
      ),
    ),
    {
      code: "transaction_query_failed",
      commitState: "not-committed",
      omittedText: serverError.message,
    },
  );
  await new Promise((resolve) => setTimeout(resolve, 50));
  assert.deepEqual(streamedQuery._result.rows, []);
  assert.equal(client.portalSyncs, 1);
  assert.equal(client.protocolReadyMessages, 2);
  assert.deepEqual(client.rollbackSubmissionStates, [true]);
  assert.equal(client.readyForQuery, true);
  assert.equal(
    nonResetQueries(client).some((args) => queryText(args) === "COMMIT"),
    false,
  );
  assert.deepEqual(client.releaseCalls, [[]]);
  client.assertExhausted();
});

test("a malformed ErrorResponse SQLSTATE still synchronizes before fail-closed rollback", async () => {
  const serverError = pgError("invalid", "malformed SQLSTATE");
  const client = new PgProtocolLifecycleFakeClient(
    [
      {},
      timestampResult("2026-07-23T10:11:12.000Z"),
      protocolErrorResponseStep(serverError),
      ROLLBACK_RESULT,
    ],
    0,
  );
  const store = new PostgresSerializableStore({
    dedicatedPool: new FakePool([client]),
  });
  await assertStoreError(
    store.runSerializable((transaction) =>
      consumePostgresSerializableTransactionRows(
        transaction,
        "SELECT value FROM streamed_rows",
        [],
        () => undefined,
      ),
    ),
    {
      code: "transaction_boundary_lost",
      commitState: "uncertain",
      omittedText: serverError.message,
    },
  );
  assert.equal(client.portalSyncs, 1);
  assert.equal(client.protocolReadyMessages, 2);
  assert.deepEqual(client.rollbackSubmissionStates, [true]);
  assert.equal(
    nonResetQueries(client).some((args) => queryText(args) === "COMMIT"),
    false,
  );
  assert.equal(client.releaseCalls.length, 1);
  assert.equal(client.releaseCalls[0].length, 1);
  client.assertExhausted();
});

test("a synchronous protocol sync failure preserves the server error and destroys without queuing rollback", async () => {
  const serverError = pgError("23505", "primary server error");
  const syncError = new Error("protocol sync failed");
  let emittedError;
  const client = new PgProtocolLifecycleFakeClient(
    [
      {},
      timestampResult("2026-07-23T10:11:12.000Z"),
      protocolErrorResponseStep(serverError, (query) => {
        Reflect.apply(EventEmitter.prototype.on, query, [
          "error",
          (error) => {
            emittedError ??= error;
          },
        ]);
      }),
    ],
    0,
    { syncError },
  );
  const store = new PostgresSerializableStore({
    dedicatedPool: new FakePool([client]),
  });
  await assertStoreError(
    store.runSerializable((transaction) =>
      consumePostgresSerializableTransactionRows(
        transaction,
        "SELECT value FROM streamed_rows",
        [],
        () => undefined,
      ),
    ),
    {
      code: "transaction_boundary_lost",
      commitState: "uncertain",
      omittedText: syncError.message,
    },
  );
  assert.equal(emittedError, serverError);
  assert.equal(client.portalSyncs, 1);
  assert.equal(client.protocolReadyMessages, 0);
  assert.deepEqual(client.rollbackSubmissionStates, []);
  assert.equal(
    nonResetQueries(client).some((args) => queryText(args) === "ROLLBACK"),
    false,
  );
  assert.equal(client.releaseCalls.length, 1);
  assert.equal(client.releaseCalls[0].length, 1);
  client.assertExhausted();
});

test("a server ErrorResponse remains authoritative after a primitive stream parser failure", async () => {
  const parserError = "stream parser failed";
  const serverError = pgError("23505", "primary server error");
  let emittedError;
  const client = new PgProtocolLifecycleFakeClient(
    [
      {},
      timestampResult("2026-07-23T10:11:12.000Z"),
      protocolErrorResponseStep(serverError, (query) => {
        primeStreamParserFailure(query, parserError);
        Reflect.apply(EventEmitter.prototype.on, query, [
          "error",
          (error) => {
            emittedError ??= error;
          },
        ]);
      }),
      ROLLBACK_RESULT,
    ],
    0,
  );
  const store = new PostgresSerializableStore({
    dedicatedPool: new FakePool([client]),
  });
  await assertStoreError(
    store.runSerializable((transaction) =>
      consumePostgresSerializableTransactionRows(
        transaction,
        "SELECT value FROM streamed_rows",
        [],
        () => undefined,
      ),
    ),
    {
      code: "transaction_query_failed",
      commitState: "not-committed",
      omittedText: parserError,
    },
  );
  assert.equal(emittedError, parserError);
  assert.equal(client.portalSyncs, 1);
  assert.equal(client.protocolReadyMessages, 2);
  assert.deepEqual(client.rollbackSubmissionStates, [true]);
  assert.deepEqual(client.releaseCalls, [[]]);
  client.assertExhausted();
});

test("a sync failure after a primitive stream parser failure destroys without queuing rollback", async () => {
  const parserError = "stream parser failed";
  const serverError = pgError("23505", "primary server error");
  const syncError = new Error("protocol sync failed");
  let emittedError;
  const client = new PgProtocolLifecycleFakeClient(
    [
      {},
      timestampResult("2026-07-23T10:11:12.000Z"),
      protocolErrorResponseStep(serverError, (query) => {
        primeStreamParserFailure(query, parserError);
        Reflect.apply(EventEmitter.prototype.on, query, [
          "error",
          (error) => {
            emittedError ??= error;
          },
        ]);
      }),
    ],
    0,
    { syncError },
  );
  const store = new PostgresSerializableStore({
    dedicatedPool: new FakePool([client]),
  });
  await assertStoreError(
    store.runSerializable((transaction) =>
      consumePostgresSerializableTransactionRows(
        transaction,
        "SELECT value FROM streamed_rows",
        [],
        () => undefined,
      ),
    ),
    {
      code: "transaction_boundary_lost",
      commitState: "uncertain",
      omittedText: syncError.message,
    },
  );
  assert.equal(emittedError, parserError);
  assert.equal(client.portalSyncs, 1);
  assert.equal(client.protocolReadyMessages, 0);
  assert.deepEqual(client.rollbackSubmissionStates, []);
  assert.equal(
    nonResetQueries(client).some((args) => queryText(args) === "ROLLBACK"),
    false,
  );
  assert.equal(client.releaseCalls.length, 1);
  assert.equal(client.releaseCalls[0].length, 1);
  client.assertExhausted();
});

test("pg global query_timeout fails a stalled row stream without accumulating rows", async () => {
  let streamedQuery;
  const client = new PgProtocolLifecycleFakeClient(
    [
      {},
      timestampResult("2026-07-23T10:11:12.000Z"),
      (args) => {
        streamedQuery = args[0];
        assert.equal(streamedQuery.callback, undefined);
        return streamedQuery;
      },
    ],
    5,
  );
  const store = new PostgresSerializableStore({
    dedicatedPool: new FakePool([client]),
  });
  await assertStoreError(
    store.runSerializable((transaction) =>
      consumePostgresSerializableTransactionRows(
        transaction,
        "SELECT value FROM stalled_rows",
        [],
        () => undefined,
      ),
    ),
    {
      code: "transaction_boundary_lost",
      commitState: "uncertain",
      omittedText: "Query read timeout",
    },
  );
  assert.deepEqual(streamedQuery._result.rows, []);
  assert.equal(client.streamPulseCalls, 2);
  assert.equal(client.portalSyncs, 0);
  assert.deepEqual(client.rollbackSubmissionStates, []);
  assert.equal(client.readyForQuery, false);
  assert.equal(
    nonResetQueries(client).some((args) => queryText(args) === "ROLLBACK"),
    false,
  );
  assert.equal(
    nonResetQueries(client).some((args) => queryText(args) === "COMMIT"),
    false,
  );
  assert.equal(client.releaseCalls.length, 1);
  assert.equal(client.releaseCalls[0].length, 1);
  client.assertExhausted();
});

test("row streaming rejects malformed or incomplete terminal results", async (t) => {
  const scenarios = [
    {
      name: "missing-command-complete",
      options: { commandMessages: [] },
    },
    {
      name: "non-select-command",
      options: { commandMessages: [{ text: "UPDATE 1" }] },
    },
    {
      name: "row-count-mismatch",
      options: { commandMessages: [{ text: "SELECT 2" }] },
    },
    {
      name: "terminal-portal-row-count-mismatch",
      options: {
        commandMessages: [{ text: "SELECT 2" }],
        protocolRows: 1_024,
      },
    },
    {
      name: "misaligned-portal-suspension",
      options: {
        beforeCommand(query, client) {
          query.handlePortalSuspended(client.connection);
        },
      },
    },
    {
      name: "multiple-command-results",
      options: {
        commandMessages: [{ text: "SELECT 1" }, { text: "SELECT 0" }],
      },
    },
    {
      name: "duplicate-row-description",
      options: {
        beforeCommand(query) {
          query.handleRowDescription({
            fields: [{ dataTypeID: 23, format: "text", name: "value" }],
          });
        },
      },
    },
    {
      name: "end-result-identity-mismatch",
      options: {
        beforeReady(query) {
          query._results = Object.freeze({ command: "SELECT", rowCount: 1 });
        },
      },
    },
    {
      name: "accumulated-result-row",
      options: {
        beforeReady(query) {
          query._result.rows.push(Object.freeze({ value: 1 }));
        },
      },
    },
  ];
  for (const scenario of scenarios) {
    await t.test(scenario.name, async () => {
      const client = new FakeClient([
        {},
        timestampResult("2026-07-23T10:11:12.000Z"),
        streamedRowsStep([{ value: 1 }], scenario.options),
        transactionIdResult(),
        {},
      ]);
      const store = new PostgresSerializableStore({
        dedicatedPool: new FakePool([client]),
      });
      await assertStoreError(
        store.runSerializable((transaction) =>
          consumePostgresSerializableTransactionRows(
            transaction,
            "SELECT value FROM streamed_rows",
            [],
            () => undefined,
          ),
        ),
        {
          code: "transaction_query_invalid",
          commitState: "not-committed",
        },
      );
      assert.equal(
        nonResetQueries(client).some(
          (args) => queryText(args) === "COMMIT",
        ),
        false,
      );
      client.assertExhausted();
    });
  }
});

test("synchronous terminal events cannot hide submission failures", async (t) => {
  const postEndError = new Error("post-end submission failure");
  const scenarios = [
    {
      name: "wrong-query-identity",
      options: {
        returnWrongIdentity: true,
        schedule(callback) {
          callback();
        },
      },
    },
    {
      name: "post-end-throw",
      options: {
        schedule(callback) {
          callback();
        },
        throwAfterTerminal: postEndError,
      },
    },
  ];
  for (const scenario of scenarios) {
    await t.test(scenario.name, async () => {
      const client = new FakeClient([
        {},
        timestampResult("2026-07-23T10:11:12.000Z"),
        streamedRowsStep([{ value: 1 }], scenario.options),
        {},
      ]);
      const store = new PostgresSerializableStore({
        dedicatedPool: new FakePool([client]),
      });
      await assertStoreError(
        store.runSerializable((transaction) =>
          consumePostgresSerializableTransactionRows(
            transaction,
            "SELECT value FROM streamed_rows",
            [],
            () => undefined,
          ),
        ),
        {
          code: "transaction_boundary_lost",
          commitState: "uncertain",
          omittedText: postEndError.message,
        },
      );
      assert.equal(
        nonResetQueries(client).some(
          (args) => queryText(args) === "COMMIT",
        ),
        false,
      );
      client.assertExhausted();
    });
  }
});

test("a synchronous post-end query error remains primary", async () => {
  const serverError = pgError("23505", "post-end server failure");
  const client = new FakeClient([
    {},
    timestampResult("2026-07-23T10:11:12.000Z"),
    streamedRowsStep([{ value: 1 }], {
      afterTerminal(query, streamClient) {
        streamClient.connection.emit("errorMessage", serverError);
        query.handleError(serverError, streamClient.connection);
      },
      schedule(callback) {
        callback();
      },
    }),
    {},
  ]);
  const store = new PostgresSerializableStore({
    dedicatedPool: new FakePool([client]),
  });
  await assertStoreError(
    store.runSerializable((transaction) =>
      consumePostgresSerializableTransactionRows(
        transaction,
        "SELECT value FROM streamed_rows",
        [],
        () => undefined,
      ),
    ),
    {
      code: "transaction_query_failed",
      commitState: "not-committed",
      omittedText: serverError.message,
    },
  );
  assert.equal(
    nonResetQueries(client).some((args) => queryText(args) === "COMMIT"),
    false,
  );
  client.assertExhausted();
});

test("row streams and ordinary queries share one ordered transaction queue", async () => {
  const order = [];
  const client = new FakeClient([
    {},
    timestampResult("2026-07-23T10:11:12.000Z"),
    () => {
      order.push("query-1");
      return { rows: [{ value: 1 }] };
    },
    transactionIdResult(),
    streamedRowsStep([{ value: 2 }], {
      inspectQuery() {
        order.push("stream-submit");
      },
    }),
    transactionIdResult(),
    () => {
      order.push("query-3");
      return { rows: [{ value: 3 }] };
    },
    transactionIdResult(),
    COMMIT_RESULT,
  ]);
  const store = new PostgresSerializableStore({
    dedicatedPool: new FakePool([client]),
  });
  await store.runSerializable(async (transaction) => {
    const first = transaction.query("SELECT 1");
    const stream = consumePostgresSerializableTransactionRows(
      transaction,
      "SELECT 2",
      [],
      (row) => {
        order.push(`row-${row.value}`);
      },
    );
    const third = transaction.query("SELECT 3");
    await Promise.all([first, stream, third]);
  });
  assert.deepEqual(order, ["query-1", "stream-submit", "row-2", "query-3"]);
  client.assertExhausted();
});

test("a thrown row callback drains the stream and forbids commit when suppressed", async () => {
  const callbackError = new Error("row callback failed");
  let emitted = 0;
  const client = new FakeClient([
    {},
    timestampResult("2026-07-23T10:11:12.000Z"),
    streamedRowsStep([{ value: 1 }, { value: 2 }, { value: 3 }], {
      afterRow() {
        emitted += 1;
      },
    }),
    transactionIdResult(),
    {},
  ]);
  const store = new PostgresSerializableStore({
    dedicatedPool: new FakePool([client]),
  });
  await assert.rejects(
    store.runSerializable(async (transaction) => {
      try {
        await consumePostgresSerializableTransactionRows(
          transaction,
          "SELECT value FROM streamed_rows",
          [],
          () => {
            throw callbackError;
          },
        );
      } catch (error) {
        assert.equal(error, callbackError);
      }
      return "must-not-commit";
    }),
    (error) => error === callbackError,
  );
  assert.equal(emitted, 3);
  assert.equal(client.portalSyncs, 1);
  assert.deepEqual(nonResetQueries(client).map(queryText), [
    "BEGIN ISOLATION LEVEL SERIALIZABLE READ WRITE",
    "SELECT pg_catalog.transaction_timestamp() AS transaction_timestamp, pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id",
    "SELECT value FROM streamed_rows",
    "SELECT pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id",
    "ROLLBACK",
  ]);
  client.assertExhausted();
});

test("a local row parser failure uses the completed portal sync only once", async () => {
  const parseError = new Error("local row parser failed");
  const client = new FakeClient([
    {},
    timestampResult("2026-07-23T10:11:12.000Z"),
    streamedRowsStep([{ value: 1 }], {
      inspectQuery(query) {
        query._result._types = {
          getTypeParser() {
            return () => {
              throw parseError;
            };
          },
        };
      },
    }),
    {},
  ]);
  const store = new PostgresSerializableStore({
    dedicatedPool: new FakePool([client]),
  });
  await assertStoreError(
    store.runSerializable((transaction) =>
      consumePostgresSerializableTransactionRows(
        transaction,
        "SELECT value FROM streamed_rows",
        [],
        () => assert.fail("a parser failure cannot emit a row"),
      ),
    ),
    {
      code: "transaction_boundary_lost",
      commitState: "uncertain",
      omittedText: parseError.message,
    },
  );
  assert.equal(client.portalSyncs, 1);
  assert.equal(
    nonResetQueries(client).some((args) => queryText(args) === "COMMIT"),
    false,
  );
  client.assertExhausted();
});

test("a non-undefined row callback result is observed and cannot commit", async () => {
  const client = new FakeClient([
    {},
    timestampResult("2026-07-23T10:11:12.000Z"),
    streamedRowsStep([{ value: 1 }, { value: 2 }]),
    transactionIdResult(),
    {},
  ]);
  const store = new PostgresSerializableStore({
    dedicatedPool: new FakePool([client]),
  });
  await assertStoreError(
    store.runSerializable(async (transaction) => {
      try {
        await consumePostgresSerializableTransactionRows(
          transaction,
          "SELECT value FROM streamed_rows",
          [],
          () => Promise.reject(new Error("owned callback rejection")),
        );
      } catch {
        return "must-not-commit";
      }
      return assert.fail("stream must reject");
    }),
    {
      code: "transaction_query_invalid",
      commitState: "not-committed",
      omittedText: "owned callback rejection",
    },
  );
  client.assertExhausted();
});

test("a stream server error takes priority over an earlier callback error", async () => {
  const serverError = pgError("23505", "private streamed duplicate");
  const client = new FakeClient([
    {},
    timestampResult("2026-07-23T10:11:12.000Z"),
    streamedRowsStep([{ value: 1 }], { error: serverError }),
    {},
  ]);
  const store = new PostgresSerializableStore({
    dedicatedPool: new FakePool([client]),
  });
  await assertStoreError(
    store.runSerializable(async (transaction) => {
      try {
        await consumePostgresSerializableTransactionRows(
          transaction,
          "SELECT value FROM streamed_rows",
          [],
          () => {
            throw new Error("lower-priority callback failure");
          },
        );
      } catch {
        return "must-not-commit";
      }
    }),
    {
      code: "transaction_query_failed",
      commitState: "not-committed",
      omittedText: "private streamed duplicate",
    },
  );
  assert.equal(client.portalSyncs, 1);
  client.assertExhausted();
});

test("a retryable stream server error takes priority and retries", async () => {
  let attempts = 0;
  const first = new FakeClient([
    {},
    timestampResult("2026-07-23T10:11:12.000Z"),
    streamedRowsStep([{ attempt: 1 }], { error: pgError("40001") }),
    {},
  ]);
  const second = new FakeClient([
    {},
    timestampResult("2026-07-23T10:11:13.000Z"),
    streamedRowsStep([{ attempt: 2 }]),
    transactionIdResult(),
    COMMIT_RESULT,
  ]);
  const store = new PostgresSerializableStore({
    dedicatedPool: new FakePool([first, second]),
    maxTransactionAttempts: 2,
  });
  assert.equal(
    await store.runSerializable(async (transaction) => {
      attempts += 1;
      try {
        await consumePostgresSerializableTransactionRows(
          transaction,
          "SELECT attempt FROM streamed_rows",
          [],
          () => {
            if (attempts === 1) throw new Error("superseded callback");
          },
        );
      } catch {
        // A trusted 40001, not the suppressed callback error, decides retry.
      }
      return attempts;
    }),
    2,
  );
  assert.equal(attempts, 2);
  first.assertExhausted();
  second.assertExhausted();
});

test("a retryable boundary rollback takes priority over a row callback error", async () => {
  const retryableBoundary = pgError("40001");
  let attempts = 0;
  const first = new FakeClient([
    {},
    timestampResult("2026-07-23T10:11:12.000Z"),
    streamedRowsStep([{ attempt: 1 }]),
    retryableBoundary,
  ]);
  const second = new FakeClient([
    {},
    timestampResult("2026-07-23T10:11:13.000Z"),
    streamedRowsStep([{ attempt: 2 }]),
    transactionIdResult(),
    COMMIT_RESULT,
  ]);
  const store = new PostgresSerializableStore({
    dedicatedPool: new FakePool([first, second]),
    maxTransactionAttempts: 2,
  });
  assert.equal(
    await store.runSerializable(async (transaction) => {
      attempts += 1;
      try {
        await consumePostgresSerializableTransactionRows(
          transaction,
          "SELECT attempt FROM streamed_rows",
          [],
          () => {
            if (attempts === 1) throw new Error("superseded callback");
          },
        );
      } catch {
        // The boundary result, not this suppressed local error, decides retry.
      }
      return attempts;
    }),
    2,
  );
  assert.equal(attempts, 2);
  first.assertExhausted();
  second.assertExhausted();
});

test("a lost boundary takes priority over a row callback error", async () => {
  const client = new FakeClient([
    {},
    timestampResult("2026-07-23T10:11:12.000Z"),
    streamedRowsStep([{ value: 1 }]),
    pgError("08006"),
    {},
  ]);
  const store = new PostgresSerializableStore({
    dedicatedPool: new FakePool([client]),
  });
  await assertStoreError(
    store.runSerializable(async (transaction) => {
      try {
        await consumePostgresSerializableTransactionRows(
          transaction,
          "SELECT value FROM streamed_rows",
          [],
          () => {
            throw new Error("superseded callback");
          },
        );
      } catch {
        return "must-not-commit";
      }
    }),
    {
      code: "transaction_boundary_lost",
      commitState: "uncertain",
    },
  );
  client.assertExhausted();
});

test("an unsettled row stream is drained and reported pending", async () => {
  const client = new FakeClient([
    {},
    timestampResult("2026-07-23T10:11:12.000Z"),
    streamedRowsStep([{ value: 1 }], { schedule: setImmediate }),
    transactionIdResult(),
    {},
  ]);
  const store = new PostgresSerializableStore({
    dedicatedPool: new FakePool([client]),
  });
  await assertStoreError(
    store.runSerializable((transaction) => {
      void consumePostgresSerializableTransactionRows(
        transaction,
        "SELECT value FROM streamed_rows",
        [],
        () => undefined,
      );
    }),
    {
      code: "transaction_query_pending",
      commitState: "not-committed",
    },
  );
  client.assertExhausted();
});

test("row streaming rejects PREPARE TRANSACTION before submission", async () => {
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
      consumePostgresSerializableTransactionRows(
        transaction,
        "/* guarded */ PREPARE TRANSACTION 'escape'",
        [],
        () => undefined,
      ),
    ),
    {
      code: "transaction_query_invalid",
      commitState: "not-committed",
    },
  );
  assert.equal(
    nonResetQueries(client).some(
      (args) => queryText(args).includes("PREPARE TRANSACTION"),
    ),
    false,
  );
  client.assertExhausted();
});

test("row streaming rejects an inserted Result prototype parent before construction", async () => {
  const resultPrototype = Object.getPrototypeOf(
    new Query({
      queryMode: "extended",
      rows: 1024,
      text: "SELECT 1",
      values: [],
    })._result,
  );
  const originalParent = Object.getPrototypeOf(resultPrototype);
  let setterCalls = 0;
  const insertedParent = Object.create(originalParent);
  Object.defineProperty(insertedParent, "rows", {
    configurable: true,
    set() {
      setterCalls += 1;
    },
  });
  const client = new FakeClient([
    {},
    timestampResult("2026-07-23T10:11:12.000Z"),
    {},
  ]);
  const store = new PostgresSerializableStore({
    dedicatedPool: new FakePool([client]),
  });
  Object.setPrototypeOf(resultPrototype, insertedParent);
  try {
    await assertStoreError(
      store.runSerializable((transaction) =>
        consumePostgresSerializableTransactionRows(
          transaction,
          "SELECT 1",
          [],
          () => undefined,
        ),
      ),
      {
        code: "transaction_query_invalid",
        commitState: "not-committed",
      },
    );
  } finally {
    Object.setPrototypeOf(resultPrototype, originalParent);
  }
  assert.equal(setterCalls, 0);
  assert.equal(
    nonResetQueries(client).some((args) => queryText(args) === "SELECT 1"),
    false,
  );
  client.assertExhausted();
});

test("row streaming rejects hostile data and pins direct protocol methods", async () => {
  const invalidClient = new FakeClient([
    {},
    timestampResult("2026-07-23T10:11:12.000Z"),
    {},
  ]);
  const invalidStore = new PostgresSerializableStore({
    dedicatedPool: new FakePool([invalidClient]),
  });
  const hostileValues = new Proxy([], {
    get() {
      assert.fail("hostile values must not be inspected through proxy traps");
    },
    getOwnPropertyDescriptor() {
      assert.fail("hostile values must not expose descriptors");
    },
  });
  await assertStoreError(
    invalidStore.runSerializable((transaction) =>
      consumePostgresSerializableTransactionRows(
        transaction,
        "SELECT 1",
        hostileValues,
        () => undefined,
      ),
    ),
    {
      code: "transaction_query_invalid",
      commitState: "not-committed",
    },
  );
  invalidClient.assertExhausted();

  const targetText = "SELECT hostile_row FROM streamed_rows";
  let rowTrapCalls = 0;
  const hostileRow = new Proxy(Object.create(null), {
    get() {
      rowTrapCalls += 1;
      throw new Error("row trap escaped");
    },
  });
  const client = new FakeClient([
    {},
    timestampResult("2026-07-23T10:11:12.000Z"),
    streamedRowsStep([hostileRow], { protocolRows: 1, rawRows: true }),
    transactionIdResult(),
    COMMIT_RESULT,
  ]);
  const store = new PostgresSerializableStore({
    dedicatedPool: new FakePool([client]),
  });
  const queryDescriptor = Object.getOwnPropertyDescriptor(
    Query.prototype,
    "handleReadyForQuery",
  );
  const emitDescriptor = Object.getOwnPropertyDescriptor(
    EventEmitter.prototype,
    "emit",
  );
  const listenersDescriptor = Object.getOwnPropertyDescriptor(
    EventEmitter.prototype,
    "listeners",
  );
  const resultPrototype = Object.getPrototypeOf(
    new Query({
      queryMode: "extended",
      rows: 1024,
      text: "SELECT 1",
      values: [],
    })._result,
  );
  const resultMethodNames = [
    "addFields",
    "parseRow",
    "addCommandComplete",
  ];
  const resultDescriptors = new Map(
    resultMethodNames.map((name) => [
      name,
      Object.getOwnPropertyDescriptor(resultPrototype, name),
    ]),
  );
  let nativeRows = 0;
  try {
    await store.runSerializable((transaction) => {
      Object.defineProperty(Query.prototype, "handleReadyForQuery", {
        ...queryDescriptor,
        value() {
          assert.fail("poisoned Query prototype escaped");
        },
      });
      Object.defineProperty(EventEmitter.prototype, "emit", {
        ...emitDescriptor,
        value(...args) {
          if (this?.text === targetText) {
            assert.fail("poisoned EventEmitter.emit escaped");
          }
          return Reflect.apply(emitDescriptor.value, this, args);
        },
      });
      Object.defineProperty(EventEmitter.prototype, "listeners", {
        ...listenersDescriptor,
        value(...args) {
          if (this?.text === targetText) {
            assert.fail("poisoned EventEmitter.listeners escaped");
          }
          return Reflect.apply(listenersDescriptor.value, this, args);
        },
      });
      for (const name of resultMethodNames) {
        Object.defineProperty(resultPrototype, name, {
          ...resultDescriptors.get(name),
          value() {
            assert.fail(`poisoned Result.${name} escaped`);
          },
        });
      }
      return consumePostgresSerializableTransactionRows(
        transaction,
        targetText,
        [],
        (row) => {
          if (row === hostileRow) return;
          assert.deepEqual(row, {});
          nativeRows += 1;
        },
      );
    });
  } finally {
    Object.defineProperty(
      Query.prototype,
      "handleReadyForQuery",
      queryDescriptor,
    );
    Object.defineProperty(EventEmitter.prototype, "emit", emitDescriptor);
    Object.defineProperty(
      EventEmitter.prototype,
      "listeners",
      listenersDescriptor,
    );
    for (const name of resultMethodNames) {
      Object.defineProperty(
        resultPrototype,
        name,
        resultDescriptors.get(name),
      );
    }
  }
  assert.equal(rowTrapCalls, 0);
  assert.equal(nativeRows, 1);
  client.assertExhausted();
});

test("runSerializable restores durable synchronous commit before COMMIT", async () => {
  const client = new FakeClient([
    {},
    timestampResult("2026-07-23T10:11:12.000Z"),
    SET_RESULT,
    transactionIdResult(),
    COMMIT_RESULT,
  ]);
  const store = new PostgresSerializableStore({
    dedicatedPool: new FakePool([client]),
  });

  assert.equal(
    await store.runSerializable(async (transaction) => {
      const result = await transaction.query(
        "SET LOCAL synchronous_commit = off",
      );
      assert.equal(result.command, "SET");
      return "durably-committed";
    }),
    "durably-committed",
  );

  assert.deepEqual(client.queries, [
    ["DISCARD ALL"],
    ["BEGIN ISOLATION LEVEL SERIALIZABLE READ WRITE"],
    [
      "SELECT pg_catalog.transaction_timestamp() AS transaction_timestamp, pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id",
    ],
    [
      {
        queryMode: "extended",
        text: "SET LOCAL synchronous_commit = off",
        values: [],
      },
    ],
    ["SELECT pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id"],
    [
      {
        queryMode: "extended",
        text: DURABLE_COMMIT_QUERY,
        values: [],
      },
    ],
    ["SELECT pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id"],
    ["COMMIT"],
    ["DISCARD ALL"],
  ]);
  assert.deepEqual(client.releaseCalls, [[]]);
  client.assertExhausted();
});

test("a malformed durable-setting acknowledgement cannot reach COMMIT", async () => {
  const client = new FakeClient(
    [
      {},
      timestampResult("2026-07-23T10:11:12.000Z"),
      {},
    ],
    { durabilitySteps: [{ command: "SELECT" }] },
  );
  const store = new PostgresSerializableStore({
    dedicatedPool: new FakePool([client]),
  });

  await assertStoreError(store.runSerializable(() => "must-not-return"), {
    code: "transaction_boundary_lost",
    commitState: "uncertain",
  });
  assert.deepEqual(nonResetQueries(client).map(queryText), [
    "BEGIN ISOLATION LEVEL SERIALIZABLE READ WRITE",
    "SELECT pg_catalog.transaction_timestamp() AS transaction_timestamp, pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id",
    DURABLE_COMMIT_QUERY,
    "ROLLBACK",
  ]);
  assert.equal(
    client.releaseCalls[0][0]?.message,
    "transaction boundary lost",
  );
  client.assertExhausted();
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
            "SELECT pg_catalog.transaction_timestamp() AS transaction_timestamp, pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id",
            "SELECT 'committed' AS value",
            "ROLLBACK",
          ]
        : [
            "BEGIN ISOLATION LEVEL SERIALIZABLE READ WRITE",
            "SELECT pg_catalog.transaction_timestamp() AS transaction_timestamp, pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id",
            "SELECT 'committed' AS value",
            "SELECT pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id",
            DURABLE_COMMIT_QUERY,
            "SELECT pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id",
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

test("a callback cannot forge a committed store-error outcome", async () => {
  const forgedError = new PostgresSerializableStoreError(
    "client_release_failed",
    "committed",
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
    store.runSerializable(() => {
      throw forgedError;
    }),
    {
      code: "transaction_rolled_back",
      commitState: "not-committed",
      omittedText: forgedError.message,
    },
  );
  assert.deepEqual(
    nonResetQueries(client).map(queryText),
    [
      "BEGIN ISOLATION LEVEL SERIALIZABLE READ WRITE",
      "SELECT pg_catalog.transaction_timestamp() AS transaction_timestamp, pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id",
      "ROLLBACK",
    ],
  );
  assert.deepEqual(client.releaseCalls, [[]]);
  client.assertExhausted();
});

test("alternate newTarget store errors cannot escape a proved rollback", async () => {
  function AlternateError() {}
  const forgedError = Reflect.construct(
    PostgresSerializableStoreError,
    ["client_release_failed", "committed"],
    AlternateError,
  );
  assert.equal(
    forgedError instanceof PostgresSerializableStoreError,
    false,
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
    store.runSerializable(() => {
      throw forgedError;
    }),
    {
      code: "transaction_rolled_back",
      commitState: "not-committed",
      omittedText: forgedError.message,
    },
  );
  assert.deepEqual(client.releaseCalls, [[]]);
  client.assertExhausted();
});

test("prototype-forged store errors cannot escape a proved rollback", async () => {
  const forgedError = Object.create(
    PostgresSerializableStoreError.prototype,
  );
  Object.assign(forgedError, {
    code: "client_release_failed",
    commitState: "committed",
    message: "forged committed store state",
    name: "PostgresSerializableStoreError",
    retryable: false,
  });
  assert.ok(forgedError instanceof PostgresSerializableStoreError);
  const client = new FakeClient([
    {},
    timestampResult("2026-07-23T10:11:12.000Z"),
    {},
  ]);
  const store = new PostgresSerializableStore({
    dedicatedPool: new FakePool([client]),
  });

  await assertStoreError(
    store.runSerializable(() => {
      throw forgedError;
    }),
    {
      code: "transaction_rolled_back",
      commitState: "not-committed",
      omittedText: forgedError.message,
    },
  );
  assert.deepEqual(client.releaseCalls, [[]]);
  client.assertExhausted();
});

test("opaque Proxy errors fail closed after a proved rollback", async () => {
  const target = new PostgresSerializableStoreError(
    "client_release_failed",
    "committed",
  );
  const opaqueError = new Proxy(target, {
    getPrototypeOf() {
      throw new Error("proxy target identity is opaque");
    },
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
    store.runSerializable(() => {
      throw opaqueError;
    }),
    {
      code: "transaction_rolled_back",
      commitState: "not-committed",
    },
  );
  assert.deepEqual(client.releaseCalls, [[]]);
  client.assertExhausted();
});

test("a callback cannot replay store-error state from another operation", async () => {
  const resetFailure = new Error("prior operation reset failed");
  const sourceClient = new FakeClient(
    [
      {},
      timestampResult("2026-07-23T10:11:11.000Z"),
      transactionIdResult(),
      COMMIT_RESULT,
    ],
    { resetSteps: [DISCARD_RESULT, resetFailure] },
  );
  const sourceStore = new PostgresSerializableStore({
    dedicatedPool: new FakePool([sourceClient]),
  });
  let replayedError;
  await assert.rejects(
    sourceStore.runSerializable(() => "committed"),
    (error) => {
      assert.ok(error instanceof PostgresSerializableStoreError);
      assert.equal(error.code, "client_reset_failed");
      assert.equal(error.commitState, "committed");
      replayedError = error;
      return true;
    },
  );
  sourceClient.assertExhausted();

  const client = new FakeClient([
    {},
    timestampResult("2026-07-23T10:11:12.000Z"),
    {},
  ]);
  const store = new PostgresSerializableStore({
    dedicatedPool: new FakePool([client]),
  });
  await assertStoreError(
    store.runSerializable(() => {
      throw replayedError;
    }),
    {
      code: "transaction_rolled_back",
      commitState: "not-committed",
      omittedText: replayedError.message,
    },
  );
  assert.deepEqual(client.releaseCalls, [[]]);
  client.assertExhausted();
});

test("a frozen native callback Promise remains supported", async () => {
  const client = new FakeClient([
    {},
    timestampResult("2026-07-23T10:11:12.000Z"),
    transactionIdResult(),
    COMMIT_RESULT,
  ]);
  const store = new PostgresSerializableStore({
    dedicatedPool: new FakePool([client]),
  });
  const callbackResult = Object.freeze(Promise.resolve("committed"));

  assert.equal(
    await store.runSerializable(() => callbackResult),
    "committed",
  );
  assert.deepEqual(client.releaseCalls, [[]]);
  client.assertExhausted();
});

test("callback Promise subclasses retain ordinary await semantics", async () => {
  class TransformingPromise extends Promise {
    then(onFulfilled, onRejected) {
      return super.then(
        (value) => onFulfilled(`transformed:${value}`),
        onRejected,
      );
    }
  }
  const client = new FakeClient([
    {},
    timestampResult("2026-07-23T10:11:12.000Z"),
    transactionIdResult(),
    COMMIT_RESULT,
  ]);
  const store = new PostgresSerializableStore({
    dedicatedPool: new FakePool([client]),
  });
  const callbackResult = new TransformingPromise((resolve) => {
    resolve("base");
  });

  assert.equal(
    await store.runSerializable(() => callbackResult),
    "transformed:base",
  );
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
      "SELECT pg_catalog.transaction_timestamp() AS transaction_timestamp, pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id",
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
      "SELECT pg_catalog.transaction_timestamp() AS transaction_timestamp, pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id",
      "ROLLBACK",
    ],
  );
  assert.deepEqual(client.releaseCalls, [[]]);
  client.assertExhausted();
});

test("a fire-and-forget invalid query rejection is internally observed", async () => {
  const client = new FakeClient([
    {},
    timestampResult("2026-07-23T10:11:12.000Z"),
    {},
  ]);
  const store = new PostgresSerializableStore({
    dedicatedPool: new FakePool([client]),
  });

  await assertStoreError(
    store.runSerializable((transaction) => {
      void transaction.query({ text: "SELECT 1" });
      return "must-not-commit";
    }),
    {
      code: "transaction_query_invalid",
      commitState: "not-committed",
    },
  );
  assert.deepEqual(
    nonResetQueries(client).map(queryText),
    [
      "BEGIN ISOLATION LEVEL SERIALIZABLE READ WRITE",
      "SELECT pg_catalog.transaction_timestamp() AS transaction_timestamp, pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id",
      "ROLLBACK",
    ],
  );
  assert.deepEqual(client.releaseCalls, [[]]);
  client.assertExhausted();
});

test("all immediate query rejections are safe under strict unhandled mode", async (t) => {
  for (const scenario of [
    "inactive",
    "boundary-escape",
    "invalid-signature",
    "invalid-values",
    "prototype-index-trap",
    "terminal-error",
  ]) {
    await t.test(scenario, () => {
      const result = spawnSync(
        process.execPath,
        [
          "--unhandled-rejections=strict",
          FIRE_AND_FORGET_FIXTURE,
          scenario,
        ],
        {
          encoding: "utf8",
          timeout: 10_000,
        },
      );
      assert.equal(result.error, undefined);
      assert.equal(result.signal, null);
      assert.equal(
        result.status,
        0,
        `strict child failed\nstdout:\n${result.stdout}\nstderr:\n${result.stderr}`,
      );
    });
  }
});

test("an observed local query rejection preserves its error identity", async () => {
  const client = new FakeClient([
    {},
    timestampResult("2026-07-23T10:11:12.000Z"),
    {},
  ]);
  const store = new PostgresSerializableStore({
    dedicatedPool: new FakePool([client]),
  });
  let queryError;
  let transactionError;

  try {
    await store.runSerializable(async (transaction) => {
      await assert.rejects(
        transaction.query({ text: "SELECT 1" }),
        (error) => {
          queryError = error;
          return (
            error?.code === "transaction_query_invalid" &&
            error.commitState === "not-committed"
          );
        },
      );
      return "must-not-commit";
    });
  } catch (error) {
    transactionError = error;
  }

  assert(queryError);
  assert.strictEqual(transactionError, queryError);
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
      "SELECT pg_catalog.transaction_timestamp() AS transaction_timestamp, pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id",
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
      "SELECT pg_catalog.transaction_timestamp() AS transaction_timestamp, pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id",
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
      "SELECT pg_catalog.transaction_timestamp() AS transaction_timestamp, pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id",
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

test("trusted retryable failures during a user-query boundary recheck retry the callback", async (t) => {
  for (const sqlState of ["40001", "40P01"]) {
    await t.test(sqlState, async () => {
      const boundaryFailure = pgError(
        sqlState,
        `boundary recheck failed with ${sqlState}`,
      );
      const first = new FakeClient([
        {},
        timestampResult("2026-07-23T10:11:12.000Z"),
        { rows: [{ value: 1 }] },
        boundaryFailure,
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
        dedicatedPool: pool,
        maxTransactionAttempts: 2,
      });
      let callbacks = 0;

      assert.equal(
        await store.runSerializable(async (transaction) => {
          callbacks += 1;
          const result = await transaction.query(
            "SELECT $1::integer AS value",
            [callbacks],
          );
          return result.rows[0].value;
        }),
        2,
      );
      assert.equal(callbacks, 2);
      assert.equal(pool.connectCalls, 2);
      assert.deepEqual(
        nonResetQueries(first).map(queryText),
        [
          "BEGIN ISOLATION LEVEL SERIALIZABLE READ WRITE",
          "SELECT pg_catalog.transaction_timestamp() AS transaction_timestamp, pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id",
          "SELECT $1::integer AS value",
          "SELECT pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id",
        ],
      );
      assert.deepEqual(first.releaseCalls, [[boundaryFailure]]);
      assert.deepEqual(second.releaseCalls, [[]]);
      first.assertExhausted();
      second.assertExhausted();
    });
  }
});

test("a local SQLSTATE-shaped user-query boundary failure is uncertain and never retried", async () => {
  const localFailure = pgError(
    "40001",
    "local boundary parser impersonated a server rollback",
  );
  const client = new FakeClient([
    {},
    timestampResult("2026-07-23T10:11:12.000Z"),
    { rows: [{ value: 1 }] },
    () => {
      throw localFailure;
    },
    {},
  ]);
  const pool = new FakePool([client]);
  const store = new PostgresSerializableStore({
    dedicatedPool: pool,
    maxTransactionAttempts: 3,
  });
  let callbacks = 0;

  await assertStoreError(
    store.runSerializable(async (transaction) => {
      callbacks += 1;
      await transaction.query("SELECT 1 AS value");
    }),
    {
      code: "transaction_boundary_lost",
      commitState: "uncertain",
      omittedText: localFailure.message,
    },
  );
  assert.equal(callbacks, 1);
  assert.equal(pool.connectCalls, 1);
  assert.deepEqual(nonResetQueries(client).map(queryText), [
    "BEGIN ISOLATION LEVEL SERIALIZABLE READ WRITE",
    "SELECT pg_catalog.transaction_timestamp() AS transaction_timestamp, pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id",
    "SELECT 1 AS value",
    "SELECT pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id",
    "ROLLBACK",
  ]);
  assert.equal(
    client.releaseCalls[0][0]?.message,
    "transaction boundary lost",
  );
  client.assertExhausted();
});

test("a protocol marker cannot be replayed into a later boundary recheck", async () => {
  const reusedFailure = pgError(
    "40001",
    "reused serialization failure",
  );
  const beginClient = new FakeClient([reusedFailure]);
  const boundaryClient = new FakeClient([
    {},
    timestampResult("2026-07-23T10:11:13.000Z"),
    { rows: [{ value: 1 }] },
    () => {
      throw reusedFailure;
    },
    {},
  ]);
  const pool = new FakePool([beginClient, boundaryClient]);
  const store = new PostgresSerializableStore({
    dedicatedPool: pool,
    maxTransactionAttempts: 3,
  });

  await assertStoreError(
    store.runSerializable(() => assert.fail("callback must not run")),
    {
      code: "transaction_begin_failed",
      commitState: "not-committed",
    },
  );
  let callbacks = 0;
  await assertStoreError(
    store.runSerializable(async (transaction) => {
      callbacks += 1;
      await transaction.query("SELECT 1 AS value");
    }),
    {
      code: "transaction_boundary_lost",
      commitState: "uncertain",
      omittedText: reusedFailure.message,
    },
  );
  assert.equal(callbacks, 1);
  assert.equal(pool.connectCalls, 2);
  assert.deepEqual(beginClient.releaseCalls, [[reusedFailure]]);
  assert.equal(
    boundaryClient.releaseCalls[0][0]?.message,
    "transaction boundary lost",
  );
  beginClient.assertExhausted();
  boundaryClient.assertExhausted();
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

test("trusted retryable failures during the final boundary recheck retry the callback", async (t) => {
  for (const sqlState of ["40001", "40P01"]) {
    await t.test(sqlState, async () => {
      const boundaryFailure = pgError(
        sqlState,
        `final boundary recheck failed with ${sqlState}`,
      );
      const first = new FakeClient(
        [
          {},
          timestampResult("2026-07-23T10:11:12.000Z"),
        ],
        { durabilityBoundarySteps: [boundaryFailure] },
      );
      const second = new FakeClient([
        {},
        timestampResult("2026-07-23T10:11:13.000Z"),
        transactionIdResult(),
        COMMIT_RESULT,
      ]);
      const pool = new FakePool([first, second]);
      const store = new PostgresSerializableStore({
        dedicatedPool: pool,
        maxTransactionAttempts: 2,
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
      assert.deepEqual(nonResetQueries(first).map(queryText), [
        "BEGIN ISOLATION LEVEL SERIALIZABLE READ WRITE",
        "SELECT pg_catalog.transaction_timestamp() AS transaction_timestamp, pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id",
        DURABLE_COMMIT_QUERY,
        "SELECT pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id",
      ]);
      assert.deepEqual(first.releaseCalls, [[boundaryFailure]]);
      assert.deepEqual(second.releaseCalls, [[]]);
      first.assertExhausted();
      second.assertExhausted();
    });
  }
});

test("a local SQLSTATE-shaped final boundary failure is uncertain and never retried", async () => {
  const localFailure = pgError(
    "40P01",
    "local final-boundary parser impersonated a deadlock",
  );
  const client = new FakeClient(
    [
      {},
      timestampResult("2026-07-23T10:11:12.000Z"),
      {},
    ],
    {
      durabilityBoundarySteps: [
        () => {
          throw localFailure;
        },
      ],
    },
  );
  const pool = new FakePool([client]);
  const store = new PostgresSerializableStore({
    dedicatedPool: pool,
    maxTransactionAttempts: 3,
  });
  let callbacks = 0;

  await assertStoreError(
    store.runSerializable(() => {
      callbacks += 1;
      return "must-not-return";
    }),
    {
      code: "transaction_boundary_lost",
      commitState: "uncertain",
      omittedText: localFailure.message,
    },
  );
  assert.equal(callbacks, 1);
  assert.equal(pool.connectCalls, 1);
  assert.deepEqual(nonResetQueries(client).map(queryText), [
    "BEGIN ISOLATION LEVEL SERIALIZABLE READ WRITE",
    "SELECT pg_catalog.transaction_timestamp() AS transaction_timestamp, pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id",
    DURABLE_COMMIT_QUERY,
    "SELECT pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id",
    "ROLLBACK",
  ]);
  assert.equal(
    client.releaseCalls[0][0]?.message,
    "transaction boundary lost",
  );
  client.assertExhausted();
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
      "SELECT pg_catalog.transaction_timestamp() AS transaction_timestamp, pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id",
      DURABLE_COMMIT_QUERY,
      "SELECT pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id",
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
      "SELECT pg_catalog.transaction_timestamp() AS transaction_timestamp, pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id",
      DURABLE_COMMIT_QUERY,
      "SELECT pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id",
      "COMMIT",
      "ROLLBACK",
    ],
  );
  assert.deepEqual(client.releaseCalls, [[commitFailure]]);
  client.assertExhausted();
});

test("post-import intrinsic poisoning cannot forge store authority", async (t) => {
  for (const scenario of [
    "weak-map-get",
    "set-has",
    "array-includes",
    "database-error-brand",
    "event-emitter-prototype",
    "hash-prototype",
    "object-command",
    "promise-prototype",
  ]) {
    await t.test(scenario, () => {
      const result = spawnSync(
        process.execPath,
        [
          "--unhandled-rejections=strict",
          INTRINSIC_POISONING_FIXTURE,
          scenario,
        ],
        {
          encoding: "utf8",
          timeout: 10_000,
        },
      );
      assert.equal(result.error, undefined);
      assert.equal(result.signal, null);
      assert.equal(
        result.status,
        0,
        `intrinsic-poisoning child failed\nstdout:\n${result.stdout}\nstderr:\n${result.stderr}`,
      );
    });
  }
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
      "SELECT pg_catalog.transaction_timestamp() AS transaction_timestamp, pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id",
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
      "SELECT pg_catalog.transaction_timestamp() AS transaction_timestamp, pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id",
      "SELECT 1",
      "SELECT pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id",
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
      "SELECT pg_catalog.transaction_timestamp() AS transaction_timestamp, pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id",
      DURABLE_COMMIT_QUERY,
      "SELECT pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id",
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
      "SELECT pg_catalog.transaction_timestamp() AS transaction_timestamp, pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id",
      "COMMIT",
      "SELECT pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id",
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
    "SELECT pg_catalog.transaction_timestamp() AS transaction_timestamp, pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id",
    "COMMIT",
    "SELECT pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id",
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
    "SELECT pg_catalog.transaction_timestamp() AS transaction_timestamp, pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id",
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
        "SELECT pg_catalog.transaction_timestamp() AS transaction_timestamp, pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id",
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
    "SELECT pg_catalog.transaction_timestamp() AS transaction_timestamp, pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id",
    "ROLLBACK",
    "SELECT pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id",
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
    "SELECT pg_catalog.transaction_timestamp() AS transaction_timestamp, pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id",
    "SELECT 1 AS value",
    "SELECT pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id",
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

test("PREPARE TRANSACTION is rejected before PostgreSQL submission", async (t) => {
  const statements = [
    "PREPARE TRANSACTION 'portable-codex-runtime'",
    " \t\r\nprepare transaction 'portable-codex-runtime'",
    "/* leading */ PREPARE/* separator */TRANSACTION 'portable-codex-runtime'",
    [
      ";",
      "/* empty statement */ ; -- another empty statement",
      "PREPARE TRANSACTION 'portable-codex-runtime'",
    ].join("\n"),
    [
      "/* outer /* nested */ comment */",
      "-- line comment",
      "PrEpArE",
      "TrAnSaCtIoN 'portable-codex-runtime'",
    ].join("\n"),
  ];

  for (const [index, statement] of statements.entries()) {
    await t.test(String(index + 1), async () => {
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
          transaction.query(statement),
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
          "SELECT pg_catalog.transaction_timestamp() AS transaction_timestamp, pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id",
          "ROLLBACK",
        ],
      );
      assert.deepEqual(client.releaseCalls, [[]]);
      client.assertExhausted();
    });
  }
});

test("ordinary PREPARE named transaction remains inside the checked transaction", async (t) => {
  for (const statement of [
    "PREPARE transaction AS SELECT 1",
    "PREPARE transaction (integer) AS SELECT $1",
  ]) {
    await t.test(statement, async () => {
      const client = new FakeClient([
        {},
        timestampResult("2026-07-23T10:11:12.000Z"),
        { command: "PREPARE" },
        transactionIdResult(),
        COMMIT_RESULT,
      ]);
      const store = new PostgresSerializableStore({
        dedicatedPool: new FakePool([client]),
      });

      await store.runSerializable((transaction) =>
        transaction.query(statement),
      );
      assert.deepEqual(
        nonResetQueries(client).map(queryText),
        [
          "BEGIN ISOLATION LEVEL SERIALIZABLE READ WRITE",
          "SELECT pg_catalog.transaction_timestamp() AS transaction_timestamp, pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id",
          statement,
          "SELECT pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id",
          DURABLE_COMMIT_QUERY,
          "SELECT pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id",
          "COMMIT",
        ],
      );
      assert.deepEqual(client.releaseCalls, [[]]);
      client.assertExhausted();
    });
  }
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

test("migrate applies the checksum-bound migration chain in one transaction", async () => {
  const migrations = await readAuthorityMigrations();
  const firstMigration = migrations[0];
  const restoreActivationMigration = migrations[3];
  const restoreRecoveryMigration = migrations[4];
  const writerStopCaptureMigration = migrations[5];
  const stablePlanMigration = migrations[6];
  const imageProviderMigration = migrations[7];
  const stateGcMigration = migrations[8];
  const operationIndexMigration = migrations[9];
  const latestMigration = migrations[10];
  const atomicCatalogueMigration = migrations[11];
  const physicalFenceMigration = migrations.at(-1);
  const client = new FakeClient([
    {},
    {},
    {},
    {},
    { rows: [] },
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
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
    checksum: physicalFenceMigration.checksum,
    version: SESSION_AUTHORITY_MIGRATION_VERSION,
  });
  assert.equal(Object.isFrozen(result), true);
  const migrationQueries = nonResetQueries(client);
  assert.deepEqual(migrationQueries[0], ["BEGIN"]);
  assert.deepEqual(migrationQueries[1], [
    MIGRATION_SEARCH_PATH_QUERY,
  ]);
  assert.deepEqual(migrationQueries[2], [
    "SELECT pg_catalog.pg_advisory_xact_lock($1::pg_catalog.int8)",
    ["7275632827684484689"],
  ]);
  assert.deepEqual(migrationQueries[3], [
    "CREATE SCHEMA IF NOT EXISTS session_authority",
  ]);
  assert.match(migrationQueries[4][0], /schema_migrations/u);
  assert.deepEqual(migrationQueries[5], [
    "SELECT version, checksum FROM session_authority.schema_migrations ORDER BY version",
  ]);
  assert.deepEqual(migrationQueries[6], [firstMigration.sql]);
  assert.deepEqual(migrationQueries[7], [
    "INSERT INTO session_authority.schema_migrations (version, checksum, applied_at) VALUES ($1, $2, pg_catalog.transaction_timestamp())",
    [1, firstMigration.checksum],
  ]);
  assert.deepEqual(migrationQueries[8], [migrations[1].sql]);
  assert.deepEqual(migrationQueries[9], [
    "INSERT INTO session_authority.schema_migrations (version, checksum, applied_at) VALUES ($1, $2, pg_catalog.transaction_timestamp())",
    [2, migrations[1].checksum],
  ]);
  assert.deepEqual(migrationQueries[10], [migrations[2].sql]);
  assert.deepEqual(migrationQueries[11], [
    "INSERT INTO session_authority.schema_migrations (version, checksum, applied_at) VALUES ($1, $2, pg_catalog.transaction_timestamp())",
    [3, migrations[2].checksum],
  ]);
  assert.deepEqual(migrationQueries[12], [restoreActivationMigration.sql]);
  assert.deepEqual(migrationQueries[13], [
    "INSERT INTO session_authority.schema_migrations (version, checksum, applied_at) VALUES ($1, $2, pg_catalog.transaction_timestamp())",
    [4, restoreActivationMigration.checksum],
  ]);
  assert.deepEqual(migrationQueries[14], [restoreRecoveryMigration.sql]);
  assert.deepEqual(migrationQueries[15], [
    "INSERT INTO session_authority.schema_migrations (version, checksum, applied_at) VALUES ($1, $2, pg_catalog.transaction_timestamp())",
    [5, restoreRecoveryMigration.checksum],
  ]);
  assert.deepEqual(migrationQueries[16], [writerStopCaptureMigration.sql]);
  assert.deepEqual(migrationQueries[17], [
    "INSERT INTO session_authority.schema_migrations (version, checksum, applied_at) VALUES ($1, $2, pg_catalog.transaction_timestamp())",
    [6, writerStopCaptureMigration.checksum],
  ]);
  assert.deepEqual(migrationQueries[18], [stablePlanMigration.sql]);
  assert.deepEqual(migrationQueries[19], [
    "INSERT INTO session_authority.schema_migrations (version, checksum, applied_at) VALUES ($1, $2, pg_catalog.transaction_timestamp())",
    [7, stablePlanMigration.checksum],
  ]);
  assert.deepEqual(migrationQueries[20], [imageProviderMigration.sql]);
  assert.deepEqual(migrationQueries[21], [
    "INSERT INTO session_authority.schema_migrations (version, checksum, applied_at) VALUES ($1, $2, pg_catalog.transaction_timestamp())",
    [8, imageProviderMigration.checksum],
  ]);
  assert.deepEqual(migrationQueries[22], [stateGcMigration.sql]);
  assert.deepEqual(migrationQueries[23], [
    "INSERT INTO session_authority.schema_migrations (version, checksum, applied_at) VALUES ($1, $2, pg_catalog.transaction_timestamp())",
    [9, stateGcMigration.checksum],
  ]);
  assert.deepEqual(migrationQueries[24], [operationIndexMigration.sql]);
  assert.deepEqual(migrationQueries[25], [
    "INSERT INTO session_authority.schema_migrations (version, checksum, applied_at) VALUES ($1, $2, pg_catalog.transaction_timestamp())",
    [10, operationIndexMigration.checksum],
  ]);
  assert.deepEqual(migrationQueries[26], [latestMigration.sql]);
  assert.deepEqual(migrationQueries[27], [
    "INSERT INTO session_authority.schema_migrations (version, checksum, applied_at) VALUES ($1, $2, pg_catalog.transaction_timestamp())",
    [11, latestMigration.checksum],
  ]);
  assert.deepEqual(migrationQueries[28], [atomicCatalogueMigration.sql]);
  assert.deepEqual(migrationQueries[29], [
    "INSERT INTO session_authority.schema_migrations (version, checksum, applied_at) VALUES ($1, $2, pg_catalog.transaction_timestamp())",
    [12, atomicCatalogueMigration.checksum],
  ]);
  assert.deepEqual(migrationQueries[30], [physicalFenceMigration.sql]);
  assert.deepEqual(migrationQueries[31], [
    "INSERT INTO session_authority.schema_migrations (version, checksum, applied_at) VALUES ($1, $2, pg_catalog.transaction_timestamp())",
    [13, physicalFenceMigration.checksum],
  ]);
  assert.deepEqual(migrationQueries[32], ["COMMIT"]);
  assert.deepEqual(client.queries.at(0), ["DISCARD ALL"]);
  assert.deepEqual(client.queries.at(-1), ["DISCARD ALL"]);
  assert.deepEqual(client.releaseCalls, [[]]);
  assert.match(firstMigration.sql, /revision bigint NOT NULL DEFAULT 0/u);
  assert.match(
    firstMigration.sql,
    /operation_claims_one_active_per_session[\s\S]+WHERE retired_at IS NULL/u,
  );
  assert.match(
    firstMigration.sql,
    /reservations_one_active_per_session[\s\S]+WHERE released_at IS NULL/u,
  );
  assert.match(
    firstMigration.sql,
    /operation_id character varying\(128\) PRIMARY KEY/u,
  );
  assert.match(firstMigration.sql, /capture_attempt_tombstones/u);
  assert.match(migrations[1].sql, /restore_destination_generations/u);
  assert.match(
    migrations[1].sql,
    /UNIQUE \(checkpoint_id, session_id\)/u,
  );
  assert.match(migrations[2].sql, /operation_id_registry/u);
  assert.match(
    migrations[2].sql,
    /request #>> '\{payload,contractVersion\}' = '2'[\s\S]+state <> 'prepared'/u,
  );
  assert.match(
    migrations[2].sql,
    /claim_type IN \('direct-operation', 'restore-launch-intent-v2'\)/u,
  );
  assert.match(
    migrations[2].sql,
    /operation_claims_operation_id_registry_fk/u,
  );
  assert.match(
    atomicCatalogueMigration.sql,
    /CREATE TABLE session_authority\.atomic_crash_captures/u,
  );
  assert.match(
    atomicCatalogueMigration.sql,
    /state IN \('starting', 'uncertain', 'committed'\)/u,
  );
  assert.match(
    atomicCatalogueMigration.sql,
    /NEW\.claimed_at := pg_catalog\.transaction_timestamp\(\)/u,
  );
  assert.match(
    physicalFenceMigration.sql,
    /writer-fence-atomic-capture-intent-v2/u,
  );
  assert.match(
    physicalFenceMigration.sql,
    /DEFERRABLE INITIALLY DEFERRED/u,
  );
  assert.match(
    physicalFenceMigration.sql,
    /operation_claims_writer_fence_atomic_capture_terminal_blocker/u,
  );
  assert.match(
    physicalFenceMigration.sql,
    /operation_claims_writer_fence_v2_immutable/u,
  );
  assert.match(
    physicalFenceMigration.sql,
    /operation_claims_atomic_crash_capture_immutable/u,
  );
  for (const relation of [
    "operation_claims",
    "operation_id_registry",
    "reservations",
    "sessions",
  ]) {
    assert.match(
      physicalFenceMigration.sql,
      new RegExp(
        `AFTER INSERT OR UPDATE OR DELETE ON session_authority\\.${relation}`,
        "u",
      ),
    );
  }
  assert.match(
    migrations[1].sql,
    /state IN \('authorized', 'committed'\)/u,
  );
  assert.match(
    restoreActivationMigration.sql,
    /restore-activation-launch-intent-v1/u,
  );
  assert.match(
    restoreActivationMigration.sql,
    /operation_claims_enforce_restore_activation_launch_id_claim/u,
  );
  assert.match(restoreRecoveryMigration.sql, /restore_recovery_cursors/u);
  assert.match(
    restoreRecoveryMigration.sql,
    /restore_recovery_cursors_lane_allowed/u,
  );
  for (const lane of [
    "generation",
    "activation",
    "launch-attempt",
    "current-launch",
  ]) {
    assert.equal(restoreRecoveryMigration.sql.includes(`'${lane}'`), true);
  }
  assert.match(
    restoreRecoveryMigration.sql,
    /restore_recovery_cursors_transition_digest_pair/u,
  );
  assert.match(writerStopCaptureMigration.sql, /writer-stop-capture-intent-v3/u);
  assert.match(
    writerStopCaptureMigration.sql,
    /operation_claims_enforce_writer_stop_capture_id_claim/u,
  );
  assert.match(
    writerStopCaptureMigration.sql,
    /operation_claims_enforce_writer_stop_capture_materialization/u,
  );
  assert.match(stablePlanMigration.sql, /detached_restore_stable_plans/u);
  assert.match(
    stablePlanMigration.sql,
    /detached-restore-stable-plan-v1/u,
  );
  assert.match(
    stablePlanMigration.sql,
    /operation_claims_enforce_detached_restore_plan_materialization/u,
  );
  assert.doesNotMatch(stablePlanMigration.sql, /jsonb_object_length/u);
  assert.match(
    stablePlanMigration.sql,
    /binding - ARRAY\[[\s\S]+'bindingSha256',[\s\S]+'request'[\s\S]+\]\s*\)\s*= '\{\}'::pg_catalog\.jsonb/u,
  );
  assert.match(
    stablePlanMigration.sql,
    /admission - ARRAY\['checkpoint', 'request'\][\s\S]+\) = '\{\}'::pg_catalog\.jsonb/u,
  );
  assert.match(
    stablePlanMigration.sql,
    /plan_input - ARRAY\[[\s\S]+'captureCreatedAt',[\s\S]+'sourceArtifactOwnedRoot'[\s\S]+\]\s*\)\s*= '\{\}'::pg_catalog\.jsonb/u,
  );
  assert.match(
    stablePlanMigration.sql,
    /ADD CONSTRAINT operation_id_registry_claim_shape[\s\S]+jsonb_typeof\(binding -> 'bindingSha256'\) = 'string'[\s\S]+binding ->> 'bindingSha256' ~ '\^\[0-9a-f\]\{64\}\$'[\s\S]+jsonb_typeof\(binding -> 'planSha256'\) = 'string'[\s\S]+binding ->> 'planSha256' ~ '\^\[0-9a-f\]\{64\}\$'[\s\S]+\) IS TRUE\);/u,
  );
  assert.match(
    stablePlanMigration.sql,
    /CONSTRAINT detached_restore_stable_plans_admission_object[\s\S]+CHECK \(\([\s\S]+jsonb_typeof\(admission -> 'request'\) = 'object'[\s\S]+\) IS TRUE\)/u,
  );
  assert.match(
    stablePlanMigration.sql,
    /CONSTRAINT detached_restore_stable_plans_plan_input_object[\s\S]+jsonb_typeof\([\s\S]+plan_input -> 'captureCreatedAt'[\s\S]+\) = 'string'[\s\S]+jsonb_typeof\([\s\S]+plan_input -> 'leaseDurationMilliseconds'[\s\S]+\) = 'number'[\s\S]+plan_input -> 'sourceArtifactOwnedRoot'[\s\S]+\) = 'string'[\s\S]+\) IS TRUE\)/u,
  );
  assert.match(
    stablePlanMigration.sql,
    /CONSTRAINT detached_restore_stable_plans_request_identity[\s\S]+jsonb_typeof\([\s\S]+admission #> '\{request,operationId\}'[\s\S]+\) = 'string'[\s\S]+admission #> '\{request,backendId\}'[\s\S]+\) = 'string'[\s\S]+\) IS TRUE\)/u,
  );
  assert.match(
    stablePlanMigration.sql,
    /CONSTRAINT detached_restore_stable_plans_request_shape[\s\S]+admission #> '\{request,contractVersion\}'[\s\S]+\) = 'number'[\s\S]+admission #> '\{request,target,kind\}'[\s\S]+\) = 'string'[\s\S]+\) IS TRUE\)/u,
  );
  assert.match(
    stablePlanMigration.sql,
    /operation_id_registry_detached_restore_stable_plan_immutable[\s\S]+CREATE TRIGGER operation_id_registry_stable_plan_update_guard[\s\S]+BEFORE UPDATE ON session_authority\.operation_id_registry/u,
  );
  assert.match(
    stablePlanMigration.sql,
    /NEW\.binding IS NOT DISTINCT FROM OLD\.binding[\s\S]+OLD\.materialized_at IS NULL[\s\S]+NEW\.materialized_at IS NOT NULL/u,
  );
  assert.match(
    stablePlanMigration.sql,
    /CREATE CONSTRAINT TRIGGER operation_id_registry_stable_plan_materialization_guard[\s\S]+AFTER UPDATE ON session_authority\.operation_id_registry[\s\S]+DEFERRABLE INITIALLY DEFERRED/u,
  );
  assert.match(
    stablePlanMigration.sql,
    /operation_claim\.created_at = NEW\.materialized_at[\s\S]+operation_claim\.request #> '\{payload,admission\}' = stable\.admission[\s\S]+detached_restore_stable_plan_claim_materialization/u,
  );
  assert.match(
    stablePlanMigration.sql,
    /CREATE CONSTRAINT TRIGGER detached_restore_stable_plans_enforce_delete_teardown[\s\S]+AFTER DELETE ON session_authority\.detached_restore_stable_plans[\s\S]+DEFERRABLE INITIALLY DEFERRED/u,
  );
  assert.match(
    stablePlanMigration.sql,
    /WHERE registry\.operation_id = OLD\.operation_id[\s\S]+detached_restore_stable_plans_delete_requires_claim_teardown/u,
  );
  assert.match(
    stablePlanMigration.sql,
    /CREATE CONSTRAINT TRIGGER operation_claims_stable_plan_delete_teardown[\s\S]+AFTER DELETE ON session_authority\.operation_claims[\s\S]+DEFERRABLE INITIALLY DEFERRED/u,
  );
  assert.match(
    stablePlanMigration.sql,
    /AS \$enforce_detached_restore_stable_plan_operation_delete\$[\s\S]+WHERE registry\.operation_id = OLD\.operation_id[\s\S]+registry\.session_id = OLD\.session_id[\s\S]+registry\.claim_type = 'detached-restore-stable-plan-v1'[\s\S]+operation_claims_stable_plan_delete_requires_teardown[\s\S]+\$enforce_detached_restore_stable_plan_operation_delete\$;/u,
  );
  assert.doesNotMatch(
    stablePlanMigration.sql,
    /enforce_detached_restore_stable_plan_operation_delete\$[\s\S]+registry\.materialized_at[\s\S]+\$enforce_detached_restore_stable_plan_operation_delete\$/u,
  );
  assert.match(imageProviderMigration.sql, /filesystem_image_provider_heads/u);
  assert.match(
    imageProviderMigration.sql,
    /PRIMARY KEY \(provider_id, anchor_id\)/u,
  );
  assert.match(imageProviderMigration.sql, /contract_version = 2/u);
  for (const column of [
    "anchor_revision",
    "generation",
    "state_revision",
    "checkpoint_state_revision",
  ]) {
    assert.match(
      imageProviderMigration.sql,
      new RegExp(`${column} numeric\\(20, 0\\) NOT NULL`, "u"),
    );
  }
  assert.match(
    imageProviderMigration.sql,
    /anchor_revision BETWEEN 1 AND 18446744073709551615/u,
  );
  assert.match(
    imageProviderMigration.sql,
    /generation BETWEEN 0 AND 18446744073709551615/u,
  );
  assert.match(
    imageProviderMigration.sql,
    /state_revision BETWEEN 0 AND 18446744073709551615/u,
  );
  assert.match(
    imageProviderMigration.sql,
    /checkpoint_state_revision BETWEEN 0 AND 18446744073709551615/u,
  );
  assert.match(
    imageProviderMigration.sql,
    /anchor_revision = state_revision \+ generation/u,
  );
  assert.match(
    imageProviderMigration.sql,
    /state_revision = checkpoint_state_revision \+ frame_count/u,
  );
  assert.match(
    imageProviderMigration.sql,
    /checkpoint_frame_count BETWEEN 0 AND 4294967295/u,
  );
  assert.match(
    imageProviderMigration.sql,
    /checkpoint_frame_count BETWEEN 2 AND 4294967295/u,
  );
  assert.match(
    imageProviderMigration.sql,
    /checkpoint_bytes BETWEEN 0 AND 9007199254740991/u,
  );
  assert.match(imageProviderMigration.sql, /frame_count BETWEEN 0 AND 65535/u);
  assert.match(
    imageProviderMigration.sql,
    /ledger_bytes BETWEEN 0 AND 67108864/u,
  );
  assert.match(
    imageProviderMigration.sql,
    /provider_id character varying\(128\) COLLATE pg_catalog\."C" NOT NULL/u,
  );
  assert.match(
    imageProviderMigration.sql,
    /anchor_id character varying\(128\) COLLATE pg_catalog\."C" NOT NULL/u,
  );
  assert.match(
    imageProviderMigration.sql,
    /last_checksum character\(64\) COLLATE pg_catalog\."C" NOT NULL/u,
  );
  for (const [checksumColumn, constraintName] of [
    [
      "base_head_checksum",
      "filesystem_image_provider_heads_base_checksum_format",
    ],
    [
      "checkpoint_checksum",
      "filesystem_image_provider_heads_checkpoint_checksum_format",
    ],
    [
      "last_checksum",
      "filesystem_image_provider_heads_last_checksum_format",
    ],
  ]) {
    assert.match(
      imageProviderMigration.sql,
      new RegExp(
        `${checksumColumn} character\\(64\\) COLLATE pg_catalog\\.\"C\"`,
        "u",
      ),
    );

    const constraintMarker = `CONSTRAINT ${constraintName}`;
    const constraintStart = imageProviderMigration.sql.indexOf(constraintMarker);
    assert.notEqual(constraintStart, -1);
    const nextConstraintStart = imageProviderMigration.sql.indexOf(
      "\n  CONSTRAINT ",
      constraintStart + constraintMarker.length,
    );
    const constraintSql = imageProviderMigration.sql.slice(
      constraintStart,
      nextConstraintStart === -1 ? undefined : nextConstraintStart,
    );
    assert.match(
      constraintSql,
      new RegExp(`octet_length\\(${checksumColumn}\\) = 64`, "u"),
    );
    assert.match(
      constraintSql,
      new RegExp(`${checksumColumn} !~ '\\[\\^0-9a-f\\]'`, "u"),
    );
  }
  assert.match(
    imageProviderMigration.sql,
    /generation = 0[\s\S]+base_head_checksum IS NULL[\s\S]+checkpoint_checksum IS NULL/u,
  );
  assert.match(
    imageProviderMigration.sql,
    /generation > 0[\s\S]+base_head_checksum IS NOT NULL[\s\S]+checkpoint_checksum IS NOT NULL/u,
  );
  assert.match(
    imageProviderMigration.sql,
    /frame_count = 0[\s\S]+generation > 0[\s\S]+ledger_bytes = 0[\s\S]+last_checksum = checkpoint_checksum/u,
  );
  assert.match(
    imageProviderMigration.sql,
    /provider_id !~ '\[\^A-Za-z0-9\._:-\]'/u,
  );
  assert.match(
    imageProviderMigration.sql,
    /anchor_id !~ '\[\^A-Za-z0-9\._:-\]'/u,
  );
  assert.doesNotMatch(imageProviderMigration.sql, /\bsequence\b/u);
  assert.doesNotMatch(imageProviderMigration.sql, /jsonb/u);
  {
    const latestMigration = operationIndexMigration;
  for (const checksumColumn of [
    "base_head_checksum",
    "checkpoint_checksum",
    "last_checksum",
  ]) {
    assert.match(
      latestMigration.sql,
      new RegExp(
        [
          `ALTER COLUMN ${checksumColumn}`,
          "[\\s\\S]+TYPE character varying\\(64\\) COLLATE ",
          'pg_catalog\\.\"C\"[\\s\\S]+USING ',
          `${checksumColumn}::character varying\\(64\\)`,
        ].join(""),
        "u",
      ),
    );
    assert.doesNotMatch(
      latestMigration.sql,
      new RegExp(
        `ALTER COLUMN ${checksumColumn}[\\s\\S]+TYPE character\\(64\\)`,
        "u",
      ),
    );
  }
  assert.match(
    latestMigration.sql,
    /ALTER COLUMN last_checksum[\s\S]+TYPE character varying\(64\)[\s\S]+USING last_checksum::character varying\(64\);[\s\S]+DROP CONSTRAINT filesystem_image_provider_heads_contract_version_supported[\s\S]+ADD CONSTRAINT filesystem_image_provider_heads_contract_version_supported[\s\S]+contract_version IN \(2, 3\)/u,
  );
  const operationIndexAlterStart = latestMigration.sql.indexOf(
    "ADD COLUMN operation_index_state_revision",
  );
  assert.notEqual(operationIndexAlterStart, -1);
  const operationIndexAlterEnd = latestMigration.sql.indexOf(
    ";",
    operationIndexAlterStart,
  );
  assert.notEqual(operationIndexAlterEnd, -1);
  const operationIndexAlterSql = latestMigration.sql.slice(
    operationIndexAlterStart,
    operationIndexAlterEnd + 1,
  );
  assert.match(
    operationIndexAlterSql,
    /ADD COLUMN operation_index_state_revision numeric\(20, 0\)/u,
  );
  assert.match(
    operationIndexAlterSql,
    /CONSTRAINT filesystem_image_provider_heads_operation_index_revision_match[\s\S]+operation_index_state_revision IS NULL[\s\S]+OR operation_index_state_revision = state_revision/u,
  );
  assert.match(
    operationIndexAlterSql,
    /CONSTRAINT filesystem_image_provider_heads_v3_operation_index_required[\s\S]+contract_version <> 3[\s\S]+OR operation_index_state_revision IS NOT NULL/u,
  );
  const latestMigrationDdlIdentifiers = [
    ...latestMigration.sql.matchAll(
      /\b(?:(?:ADD|ALTER)\s+COLUMN|(?:(?:ADD|DROP)\s+)?CONSTRAINT|CREATE\s+(?:UNIQUE\s+)?INDEX|CREATE\s+(?:CONSTRAINT\s+)?TRIGGER|CREATE\s+(?:TABLE|FUNCTION))\s+(?:session_authority\.)?([A-Za-z_][A-Za-z0-9_]*)/gu,
    ),
  ].map((match) => match[1]);
  assert.ok(
    latestMigrationDdlIdentifiers.includes(
      "filesystem_image_provider_heads_operation_index_revision_match",
    ),
  );
  for (const identifier of latestMigrationDdlIdentifiers) {
    assert.ok(
      Buffer.byteLength(identifier, "utf8") <= 63,
      `PostgreSQL DDL identifier exceeds 63 bytes: ${identifier}`,
    );
  }
  assert.match(
    latestMigration.sql,
    /CREATE TABLE session_authority\.filesystem_image_provider_operations/u,
  );
  assert.match(
    latestMigration.sql,
    /provider_id character varying\(128\) COLLATE pg_catalog\."C" NOT NULL[\s\S]+anchor_id character varying\(128\) COLLATE pg_catalog\."C" NOT NULL[\s\S]+operation_id character varying\(128\) COLLATE pg_catalog\."C" NOT NULL/u,
  );
  assert.match(
    latestMigration.sql,
    /record_contract_version integer NOT NULL[\s\S]+state character varying\(16\) COLLATE pg_catalog\."C" NOT NULL[\s\S]+kind character varying\(32\) COLLATE pg_catalog\."C" NOT NULL[\s\S]+storage_id character varying\(128\) COLLATE pg_catalog\."C" NOT NULL/u,
  );
  assert.match(
    latestMigration.sql,
    /prepared_state_revision numeric\(20, 0\) NOT NULL[\s\S]+prepared_checksum character varying\(64\) COLLATE pg_catalog\."C" NOT NULL[\s\S]+prepared_record_bytes bytea NOT NULL[\s\S]+prepared_record_sha256 character varying\(64\) COLLATE pg_catalog\."C" NOT NULL/u,
  );
  assert.match(
    latestMigration.sql,
    /committed_state_revision numeric\(20, 0\),[\s\S]+committed_checksum_provenance character varying\(32\) COLLATE pg_catalog\."C",[\s\S]+committed_checksum character varying\(64\) COLLATE pg_catalog\."C",[\s\S]+committed_record_bytes bytea,[\s\S]+committed_record_sha256 character varying\(64\) COLLATE pg_catalog\."C"/u,
  );
  for (const digestColumn of [
    "prepared_checksum",
    "prepared_record_sha256",
    "committed_checksum",
    "committed_record_sha256",
  ]) {
    assert.match(
      latestMigration.sql,
      new RegExp(
        `${digestColumn} character varying\\(64\\) COLLATE pg_catalog\\.\"C\"`,
        "u",
      ),
    );
    assert.doesNotMatch(
      latestMigration.sql,
      new RegExp(`${digestColumn} character\\(64\\)`, "u"),
    );
  }
  assert.match(
    latestMigration.sql,
    /PRIMARY KEY \(provider_id, anchor_id, operation_id\)[\s\S]+FOREIGN KEY \(provider_id, anchor_id\)[\s\S]+REFERENCES session_authority\.filesystem_image_provider_heads/u,
  );
  assert.doesNotMatch(latestMigration.sql, /\bON DELETE\b/u);
  assert.match(latestMigration.sql, /record_contract_version = 1/u);
  assert.match(latestMigration.sql, /state IN \('prepared', 'committed'\)/u);
  assert.match(
    latestMigration.sql,
    /kind IN \([\s\S]+'provision',[\s\S]+'attach',[\s\S]+'detach',[\s\S]+'destroy',[\s\S]+'checkpoint',[\s\S]+'restore',[\s\S]+'restore-attach'[\s\S]+\)/u,
  );
  assert.match(
    latestMigration.sql,
    /prepared_state_revision BETWEEN 1 AND 18446744073709551615/u,
  );
  assert.match(
    latestMigration.sql,
    /state = 'prepared'[\s\S]+committed_state_revision IS NULL[\s\S]+committed_checksum_provenance IS NULL[\s\S]+committed_checksum IS NULL[\s\S]+committed_record_bytes IS NULL[\s\S]+committed_record_sha256 IS NULL[\s\S]+state = 'committed'[\s\S]+committed_state_revision IS NOT NULL[\s\S]+committed_checksum_provenance IS NOT NULL[\s\S]+committed_record_bytes IS NOT NULL[\s\S]+committed_record_sha256 IS NOT NULL[\s\S]+committed_state_revision > prepared_state_revision[\s\S]+committed_state_revision <= 18446744073709551615/u,
  );
  assert.match(
    latestMigration.sql,
    /committed_checksum_provenance = 'indexed-frame-v1'[\s\S]+committed_checksum IS NOT NULL[\s\S]+octet_length\(committed_checksum\) = 64[\s\S]+committed_checksum !~ '\[\^0-9a-f\]'/u,
  );
  assert.doesNotMatch(latestMigration.sql, /unavailable-adopted-v2/u);
  for (const recordBytesColumn of [
    "prepared_record_bytes",
    "committed_record_bytes",
  ]) {
    assert.match(
      latestMigration.sql,
      new RegExp(
        `octet_length\\(${recordBytesColumn}\\) BETWEEN 1 AND 4194304`,
        "u",
      ),
    );
  }
  assert.match(
    latestMigration.sql,
    /CONSTRAINT filesystem_image_provider_operations_prepared_bytes_bounded/u,
  );
  assert.match(
    latestMigration.sql,
    /CONSTRAINT filesystem_image_provider_operations_prepared_sha256_format/u,
  );
  assert.doesNotMatch(
    latestMigration.sql,
    /filesystem_image_provider_operations_prepared_record_(?:bytes_bounded|sha256_format)/u,
  );
  for (const digestColumn of [
    "prepared_checksum",
    "prepared_record_sha256",
    "committed_record_sha256",
  ]) {
    assert.match(
      latestMigration.sql,
      new RegExp(
        `octet_length\\(${digestColumn}\\) = 64[\\s\\S]+${digestColumn} !~ '\\[\\^0-9a-f\\]'`,
        "u",
      ),
    );
  }
  assert.match(
    latestMigration.sql,
    /CREATE UNIQUE INDEX filesystem_image_provider_operations_one_prepared_storage[\s\S]+\(\s*provider_id,[\s\S]+anchor_id,[\s\S]+storage_id[\s\S]+WHERE state = 'prepared'/u,
  );
  assert.match(
    latestMigration.sql,
    /CREATE INDEX filesystem_image_provider_operations_state_storage_idx[\s\S]+provider_id,[\s\S]+anchor_id,[\s\S]+state,[\s\S]+storage_id COLLATE pg_catalog\."C"/u,
  );
  assert.match(
    latestMigration.sql,
    /CREATE INDEX filesystem_image_provider_operations_prepared_revision_idx[\s\S]+provider_id,[\s\S]+anchor_id,[\s\S]+prepared_state_revision,[\s\S]+operation_id COLLATE pg_catalog\."C"/u,
  );
  const committedTailIndexStart = latestMigration.sql.indexOf(
    "CREATE INDEX filesystem_image_provider_operations_committed_storage_tail_idx",
  );
  assert.notEqual(committedTailIndexStart, -1);
  const committedTailIndexEnd = latestMigration.sql.indexOf(
    ";",
    committedTailIndexStart,
  );
  assert.notEqual(committedTailIndexEnd, -1);
  const committedTailIndexSql = latestMigration.sql.slice(
    committedTailIndexStart,
    committedTailIndexEnd + 1,
  );
  assert.match(
    committedTailIndexSql,
    /CREATE INDEX filesystem_image_provider_operations_committed_storage_tail_idx[\s\S]+provider_id,[\s\S]+anchor_id,[\s\S]+storage_id COLLATE pg_catalog\."C",[\s\S]+committed_state_revision DESC,[\s\S]+operation_id COLLATE pg_catalog\."C" DESC[\s\S]+WHERE state = 'committed'/u,
  );
  assert.match(
    latestMigration.sql,
    /enforce_filesystem_image_provider_operation_insert[\s\S]+NEW\.state = 'prepared'[\s\S]+NEW\.committed_state_revision IS NULL[\s\S]+NEW\.committed_checksum_provenance IS NULL[\s\S]+NEW\.committed_checksum IS NULL[\s\S]+NEW\.committed_record_bytes IS NULL[\s\S]+NEW\.committed_record_sha256 IS NULL[\s\S]+ERRCODE = '55000'[\s\S]+filesystem_image_provider_operations_insert_prepared_only/u,
  );
  assert.match(
    latestMigration.sql,
    /CREATE TRIGGER filesystem_image_provider_operations_insert_guard[\s\S]+BEFORE INSERT ON session_authority\.filesystem_image_provider_operations/u,
  );
  assert.match(
    latestMigration.sql,
    /enforce_filesystem_image_provider_operation_update[\s\S]+OLD\.state = 'prepared'[\s\S]+NEW\.state = 'committed'[\s\S]+NEW\.provider_id IS NOT DISTINCT FROM OLD\.provider_id[\s\S]+NEW\.anchor_id IS NOT DISTINCT FROM OLD\.anchor_id[\s\S]+NEW\.operation_id IS NOT DISTINCT FROM OLD\.operation_id[\s\S]+NEW\.record_contract_version IS NOT DISTINCT FROM OLD\.record_contract_version[\s\S]+NEW\.kind IS NOT DISTINCT FROM OLD\.kind[\s\S]+NEW\.storage_id IS NOT DISTINCT FROM OLD\.storage_id[\s\S]+NEW\.prepared_state_revision IS NOT DISTINCT FROM OLD\.prepared_state_revision[\s\S]+NEW\.prepared_checksum IS NOT DISTINCT FROM OLD\.prepared_checksum[\s\S]+NEW\.prepared_record_bytes IS NOT DISTINCT FROM OLD\.prepared_record_bytes[\s\S]+NEW\.prepared_record_sha256 IS NOT DISTINCT FROM OLD\.prepared_record_sha256[\s\S]+ERRCODE = '55000'[\s\S]+filesystem_image_provider_operations_prepared_to_committed_only/u,
  );
  assert.match(
    latestMigration.sql,
    /CREATE TRIGGER filesystem_image_provider_operations_update_guard[\s\S]+BEFORE UPDATE ON session_authority\.filesystem_image_provider_operations/u,
  );
  assert.doesNotMatch(
    latestMigration.sql,
    /NEW\.committed_checksum_provenance IS NOT DISTINCT FROM OLD\.committed_checksum_provenance/u,
  );
  assert.doesNotMatch(latestMigration.sql, /\b(?:history_origin|record_origin)\b/u);
  assert.match(
    latestMigration.sql,
    /enforce_filesystem_image_provider_operation_delete[\s\S]+FROM session_authority\.filesystem_image_provider_heads AS head[\s\S]+head\.provider_id = OLD\.provider_id[\s\S]+head\.anchor_id = OLD\.anchor_id[\s\S]+ERRCODE = '23503'[\s\S]+filesystem_image_provider_operations_delete_requires_teardown/u,
  );
  assert.match(
    latestMigration.sql,
    /CREATE CONSTRAINT TRIGGER filesystem_image_provider_operations_delete_guard[\s\S]+AFTER DELETE ON session_authority\.filesystem_image_provider_operations[\s\S]+DEFERRABLE INITIALLY DEFERRED/u,
  );
  assert.match(
    latestMigration.sql,
    /reject_filesystem_image_provider_operation_truncate[\s\S]+RAISE EXCEPTION[\s\S]+ERRCODE = '55000'[\s\S]+filesystem_image_provider_operations_truncate_forbidden/u,
  );
  assert.match(
    latestMigration.sql,
    /CREATE TRIGGER filesystem_image_provider_operations_truncate_guard[\s\S]+BEFORE TRUNCATE ON session_authority\.filesystem_image_provider_operations[\s\S]+FOR EACH STATEMENT[\s\S]+EXECUTE FUNCTION session_authority\.reject_filesystem_image_provider_operation_truncate/u,
  );
  assert.doesNotMatch(latestMigration.sql, /\bjsonb\b|pgcrypto|\bdigest\s*\(/u);
  }
  assert.match(
    latestMigration.sql,
    /ADD COLUMN operation_index_adoption_id[\s\S]+ADD COLUMN operation_index_adoption_xid xid8/u,
  );
  assert.match(
    latestMigration.sql,
    /ADD COLUMN adoption_id character varying\(64\) COLLATE pg_catalog\."C"/u,
  );
  assert.match(
    latestMigration.sql,
    /CREATE TABLE session_authority\.filesystem_image_provider_anchor_lifecycle[\s\S]+retired_xid xid8,[\s\S]+PRIMARY KEY \(provider_id, anchor_id\)/u,
  );
  assert.match(
    latestMigration.sql,
    /INSERT INTO session_authority\.filesystem_image_provider_anchor_lifecycle[\s\S]+SELECT provider_id, anchor_id, NULL[\s\S]+FROM session_authority\.filesystem_image_provider_heads/u,
  );
  assert.match(
    latestMigration.sql,
    /enforce_fs_image_anchor_lifecycle[\s\S]+TG_OP = 'INSERT'[\s\S]+NEW\.retired_xid IS NULL[\s\S]+pg_trigger_depth\(\) = 2[\s\S]+TG_OP = 'UPDATE'[\s\S]+OLD\.retired_xid IS NULL[\s\S]+NEW\.retired_xid = pg_current_xact_id\(\)[\s\S]+filesystem_image_provider_heads[\s\S]+filesystem_image_provider_operations[\s\S]+fs_image_anchor_lifecycle_immutable[\s\S]+BEFORE INSERT OR UPDATE OR DELETE[\s\S]+BEFORE TRUNCATE/u,
  );
  assert.match(
    latestMigration.sql,
    /enforce_filesystem_image_provider_operation_delete[\s\S]+filesystem_image_provider_operations AS operation[\s\S]+UPDATE session_authority\.filesystem_image_provider_anchor_lifecycle[\s\S]+SET retired_xid = pg_current_xact_id\(\)/u,
  );
  assert.match(
    latestMigration.sql,
    /claim_fs_image_anchor_lifecycle[\s\S]+INSERT INTO session_authority\.filesystem_image_provider_anchor_lifecycle[\s\S]+ON CONFLICT \(provider_id, anchor_id\) DO UPDATE[\s\S]+RETURNING retired_xid INTO lifecycle_retired_xid[\s\S]+fs_image_head_retired[\s\S]+CREATE TRIGGER fs_image_heads_lifecycle_claim[\s\S]+AFTER INSERT ON session_authority\.filesystem_image_provider_heads/u,
  );
  assert.match(
    latestMigration.sql,
    /NEW\.provider_id IS DISTINCT FROM OLD\.provider_id[\s\S]+NEW\.anchor_id IS DISTINCT FROM OLD\.anchor_id[\s\S]+fs_image_head_identity_immutable/u,
  );
  assert.match(
    latestMigration.sql,
    /retire_fs_image_anchor[\s\S]+SET retired_xid = pg_current_xact_id\(\)[\s\S]+CREATE TRIGGER fs_image_heads_retirement_guard[\s\S]+AFTER DELETE ON session_authority\.filesystem_image_provider_heads/u,
  );
  assert.match(
    latestMigration.sql,
    /reject_fs_image_head_truncate[\s\S]+fs_image_heads_truncate_forbidden[\s\S]+CREATE TRIGGER fs_image_heads_truncate_guard[\s\S]+BEFORE TRUNCATE ON session_authority\.filesystem_image_provider_heads/u,
  );
  assert.match(
    latestMigration.sql,
    /IF NOT FOUND THEN[\s\S]+filesystem_image_provider_anchor_lifecycle AS lifecycle[\s\S]+lifecycle\.retired_xid = pg_current_xact_id\(\)[\s\S]+fs_image_adoption_teardown/u,
  );
  assert.match(
    latestMigration.sql,
    /committed_checksum_provenance = 'unavailable-adopted-v2'[\s\S]+committed_checksum IS NULL[\s\S]+adoption_id IS NOT NULL/u,
  );
  assert.match(
    latestMigration.sql,
    /head\.operation_index_adoption_xid = pg_current_xact_id\(\)/u,
  );
  assert.match(
    latestMigration.sql,
    /CREATE UNIQUE INDEX fs_image_operations_prepared_revision_uniq[\s\S]+prepared_state_revision/u,
  );
  assert.match(
    latestMigration.sql,
    /CREATE UNIQUE INDEX fs_image_operations_committed_revision_uniq[\s\S]+committed_state_revision[\s\S]+WHERE state = 'committed'/u,
  );
  assert.match(
    latestMigration.sql,
    /fs_image_operations_revision_cross_unique[\s\S]+operation\.committed_state_revision = NEW\.prepared_state_revision[\s\S]+operation\.prepared_state_revision = NEW\.committed_state_revision/u,
  );
  assert.match(
    latestMigration.sql,
    /CREATE TABLE session_authority\.filesystem_image_provider_operation_events[\s\S]+PRIMARY KEY \(provider_id, anchor_id, event_revision\)[\s\S]+UNIQUE \(provider_id, anchor_id, operation_id, phase\)[\s\S]+FOREIGN KEY \(provider_id, anchor_id, operation_id\)[\s\S]+ON DELETE CASCADE/u,
  );
  assert.match(
    latestMigration.sql,
    /INSERT INTO session_authority\.filesystem_image_provider_operation_events[\s\S]+SELECT provider_id, anchor_id, operation_id, 'prepared', prepared_state_revision[\s\S]+UNION ALL[\s\S]+SELECT provider_id, anchor_id, operation_id, 'committed', committed_state_revision/u,
  );
  assert.match(
    latestMigration.sql,
    /validate_fs_image_existing_event_cover[\s\S]+coalesce\(sum\(1::numeric\), 0\)[\s\S]+operation_index_state_revision IS NOT NULL[\s\S]+operation_index_state_revision = 0[\s\S]+first_revision <> 1[\s\S]+last_revision <> head\.operation_index_state_revision[\s\S]+fs_image_operation_events_existing_cover/u,
  );
  assert.match(
    latestMigration.sql,
    /enforce_fs_image_operation_event[\s\S]+TG_OP = 'INSERT'[\s\S]+pg_trigger_depth\(\) = 2[\s\S]+NEW\.phase = 'prepared'[\s\S]+NEW\.phase = 'committed'[\s\S]+TG_OP = 'DELETE'[\s\S]+NOT EXISTS[\s\S]+fs_image_operation_events_immutable[\s\S]+BEFORE INSERT OR UPDATE OR DELETE[\s\S]+BEFORE TRUNCATE/u,
  );
  assert.match(
    latestMigration.sql,
    /claim_fs_image_operation_events[\s\S]+TG_OP = 'INSERT'[\s\S]+NEW\.prepared_state_revision[\s\S]+NEW\.state = 'committed'[\s\S]+NEW\.committed_state_revision[\s\S]+CREATE TRIGGER fs_image_operations_event_claim[\s\S]+AFTER INSERT OR UPDATE/u,
  );
  assert.match(
    latestMigration.sql,
    /NEW\.prepared_state_revision <= head\.state_revision[\s\S]+NEW\.committed_state_revision <= head\.checkpoint_state_revision/u,
  );
  assert.match(
    latestMigration.sql,
    /OLD\.contract_version = 2[\s\S]+NEW\.contract_version = 3[\s\S]+NEW\.checkpoint_state_revision = OLD\.state_revision[\s\S]+NEW\.operation_index_state_revision = OLD\.state_revision/u,
  );
  assert.match(
    latestMigration.sql,
    /CONSTRAINT fs_image_heads_stored_non_genesis[\s\S]+state_revision BETWEEN 1 AND 18446744073709551615/u,
  );
  assert.match(
    latestMigration.sql,
    /NEW\.state_revision = 0[\s\S]+fs_image_head_initial_progress[\s\S]+OLD\.state_revision > 0[\s\S]+NEW\.state_revision > 0/u,
  );
  assert.match(
    latestMigration.sql,
    /OLD\.operation_index_adoption_xid = pg_current_xact_id\(\)[\s\S]+fs_image_head_adoption_same_xact_update/u,
  );
  assert.match(
    latestMigration.sql,
    /ADD COLUMN operation_index_progress_xid xid8[\s\S]+NEW\.operation_index_progress_xid IS NOT NULL[\s\S]+NEW\.operation_index_progress_xid := pg_current_xact_id\(\)[\s\S]+OLD\.operation_index_progress_xid = pg_current_xact_id\(\)[\s\S]+fs_image_head_same_xact_update[\s\S]+NEW\.operation_index_progress_xid IS DISTINCT FROM OLD\.operation_index_progress_xid[\s\S]+fs_image_head_progress_xid_managed/u,
  );
  assert.match(
    latestMigration.sql,
    /NEW\.operation_index_state_revision IS NOT NULL[\s\S]+NEW\.anchor_revision = 1[\s\S]+NEW\.state_revision = 1[\s\S]+fs_image_head_initial_progress/u,
  );
  assert.match(
    latestMigration.sql,
    /OLD\.operation_index_state_revision IS NULL[\s\S]+fs_image_head_index_activation[\s\S]+NEW\.state_revision = OLD\.state_revision \+ 1[\s\S]+NEW\.frame_count = OLD\.frame_count \+ 1[\s\S]+NEW\.ledger_bytes > OLD\.ledger_bytes[\s\S]+NEW\.generation = OLD\.generation \+ 1[\s\S]+fs_image_head_incremental_progress/u,
  );
  assert.match(
    latestMigration.sql,
    /CREATE CONSTRAINT TRIGGER fs_image_heads_adoption_complete[\s\S]+DEFERRABLE INITIALLY DEFERRED/u,
  );
  assert.match(
    latestMigration.sql,
    /validate_fs_image_head_progress[\s\S]+stored_head\.provider_id IS DISTINCT FROM NEW\.provider_id[\s\S]+stored_head\.operation_index_adoption_xid IS DISTINCT FROM NEW\.operation_index_adoption_xid[\s\S]+fs_image_head_progress_final[\s\S]+event\.event_revision = NEW\.state_revision[\s\S]+event\.phase = 'prepared'[\s\S]+operation\.prepared_checksum = NEW\.last_checksum[\s\S]+event\.phase = 'committed'[\s\S]+operation\.committed_checksum = NEW\.last_checksum[\s\S]+fs_image_head_append_event[\s\S]+CREATE CONSTRAINT TRIGGER fs_image_heads_progress_complete[\s\S]+AFTER INSERT OR UPDATE[\s\S]+DEFERRABLE INITIALLY DEFERRED/u,
  );
  assert.match(
    latestMigration.sql,
    /coalesce\(sum\(1::numeric\), 0\)[\s\S]+GROUP BY revision[\s\S]+HAVING sum\(1::numeric\) <> 1/u,
  );
  assert.doesNotMatch(latestMigration.sql, /count\(\*\)/u);
  assert.match(
    latestMigration.sql,
    /selected_mode = 1[\s\S]+operation\.adoption_id IS DISTINCT FROM selected_adoption_id[\s\S]+selected_mode = 0[\s\S]+operation\.adoption_id IS NOT NULL[\s\S]+committed_checksum_provenance[\s\S]+indexed-frame-v1/u,
  );
  for (const column of [
    "provider_id",
    "anchor_id",
    "contract_version",
    "anchor_revision",
    "generation",
    "state_revision",
    "base_head_checksum",
    "checkpoint_state_revision",
    "checkpoint_frame_count",
    "checkpoint_checksum",
    "checkpoint_bytes",
    "frame_count",
    "last_checksum",
    "ledger_bytes",
    "operation_index_state_revision",
    "operation_index_adoption_id",
    "operation_index_adoption_xid",
    "operation_index_progress_xid",
  ]) {
    assert.match(
      latestMigration.sql,
      new RegExp(
        `stored_head\\.${column} IS DISTINCT FROM NEW\\.${column}`,
        "u",
      ),
    );
  }
  const adoptionDdlIdentifiers = [
    ...latestMigration.sql.matchAll(
      /\b(?:(?:ADD|ALTER)\s+COLUMN|(?:(?:ADD|DROP)\s+)?CONSTRAINT|CREATE\s+(?:UNIQUE\s+)?INDEX|CREATE\s+(?:CONSTRAINT\s+)?TRIGGER|CREATE\s+(?:TABLE|FUNCTION))\s+(?:session_authority\.)?([A-Za-z_][A-Za-z0-9_]*)/gu,
    ),
  ].map((match) => match[1]);
  for (const identifier of adoptionDdlIdentifiers) {
    assert.ok(
      Buffer.byteLength(identifier, "utf8") <= 63,
      `PostgreSQL DDL identifier exceeds 63 bytes: ${identifier}`,
    );
  }
  assert.match(
    stateGcMigration.sql,
    /LOCK TABLE session_authority\.sessions IN EXCLUSIVE MODE;[\s\S]+LOCK TABLE session_authority\.operation_claims IN ACCESS EXCLUSIVE MODE;[\s\S]+writer_supervisor_state_owner_migration[\s\S]+launch\.kind = 'writer-launch-attempt-v1'[\s\S]+launch\.state IN \('starting', 'uncertain'\)[\s\S]+FROM session_authority\.sessions AS session[\s\S]+session\.document #> '\{launch\}' IS NOT NULL[\s\S]+session\.document #> '\{launch\}' <> 'null'::jsonb[\s\S]+writer_supervisor_state_owners_require_quiescent_launches[\s\S]+CREATE TABLE session_authority\.writer_supervisor_state_owners/u,
  );
  assert.match(
    stateGcMigration.sql,
    /CREATE TABLE session_authority\.writer_supervisor_state_owners/u,
  );
  assert.match(
    stateGcMigration.sql,
    /launch_attempt_id character varying\(128\) PRIMARY KEY[\s\S]+state_owner_id character varying\(76\) NOT NULL[\s\S]+bound_at timestamp with time zone NOT NULL/u,
  );
  assert.match(
    stateGcMigration.sql,
    /writer_supervisor_state_owners_launch_attempt_fk[\s\S]+FOREIGN KEY \(launch_attempt_id, session_id\)[\s\S]+REFERENCES session_authority\.operation_claims/u,
  );
  assert.match(
    stateGcMigration.sql,
    /writer_supervisor_state_owners_state_owner_id_format[\s\S]+\^state-owner:\[0-9a-f\]\{64\}\$/u,
  );
  assert.match(
    stateGcMigration.sql,
    /writer_supervisor_state_owners_launch_session_owner_unique[\s\S]+UNIQUE \(launch_attempt_id, session_id, state_owner_id\)/u,
  );
  assert.match(
    stateGcMigration.sql,
    /CREATE TRIGGER writer_supervisor_state_owners_reject_update[\s\S]+BEFORE UPDATE ON session_authority\.writer_supervisor_state_owners/u,
  );
  assert.match(
    stateGcMigration.sql,
    /enforce_writer_supervisor_state_owner_delete[\s\S]+FROM session_authority\.operation_id_registry AS registry[\s\S]+registry\.operation_id = OLD\.launch_attempt_id[\s\S]+registry\.session_id = OLD\.session_id[\s\S]+writer_supervisor_state_owners_delete_requires_claim_teardown/u,
  );
  assert.match(
    stateGcMigration.sql,
    /CREATE CONSTRAINT TRIGGER writer_supervisor_state_owners_enforce_delete_teardown[\s\S]+AFTER DELETE ON session_authority\.writer_supervisor_state_owners[\s\S]+DEFERRABLE INITIALLY DEFERRED/u,
  );
  assert.match(
    stateGcMigration.sql,
    /CREATE CONSTRAINT TRIGGER operation_claims_writer_launch_state_owner_guard[\s\S]+AFTER INSERT OR UPDATE ON session_authority\.operation_claims[\s\S]+DEFERRABLE INITIALLY DEFERRED/u,
  );
  assert.match(
    stateGcMigration.sql,
    /enforce_writer_launch_state_owner[\s\S]+OLD\.state = 'prepared'[\s\S]+NEW\.result #>> '\{outcome\}' = 'cancelled-before-dispatch'[\s\S]+FROM session_authority\.writer_supervisor_state_owners AS owner[\s\S]+owner\.launch_attempt_id = NEW\.operation_id[\s\S]+owner\.session_id = NEW\.session_id[\s\S]+owner\.supervisor_id =[\s\S]+NEW\.request #>> '\{payload,supervisor,supervisorId\}'[\s\S]+owner\.bound_at >= NEW\.created_at[\s\S]+operation_claims_writer_launch_state_owner/u,
  );
  assert.match(stateGcMigration.sql, /writer_supervisor_state_gc/u);
  assert.match(
    stateGcMigration.sql,
    /terminal_operation_id character varying\(128\) PRIMARY KEY/u,
  );
  assert.match(
    stateGcMigration.sql,
    /FOREIGN KEY \(terminal_operation_id, session_id\)[\s\S]+REFERENCES session_authority\.operation_claims/u,
  );
  assert.match(
    stateGcMigration.sql,
    /writer_supervisor_state_gc_state_owner_fk[\s\S]+FOREIGN KEY \(launch_attempt_id, session_id, state_owner_id\)[\s\S]+REFERENCES session_authority\.writer_supervisor_state_owners/u,
  );
  assert.match(
    stateGcMigration.sql,
    /writer_supervisor_state_gc_state_owner_id_format[\s\S]+\^state-owner:\[0-9a-f\]\{64\}\$/u,
  );
  for (const terminalKind of [
    "writer-launch-attempt-v1",
    "writer-launch-stop-v1",
  ]) {
    assert.equal(stateGcMigration.sql.includes(`'${terminalKind}'`), true);
  }
  assert.match(
    stateGcMigration.sql,
    /writer_supervisor_state_gc_collection_shape[\s\S]+\(collection_status IS NULL\) =[\s\S]+\(collection_receipt_sha256 IS NULL\)[\s\S]+\(collection_status IS NULL\) = \(collected_at IS NULL\)/u,
  );
  assert.match(
    stateGcMigration.sql,
    /collection_status IN \('collected', 'absent'\)/u,
  );
  assert.match(
    stateGcMigration.sql,
    /jsonb_typeof\(terminal_record\) = 'object'/u,
  );
  for (const digestColumn of [
    "terminal_record_sha256",
    "authorization_sha256",
    "collection_receipt_sha256",
  ]) {
    assert.match(
      stateGcMigration.sql,
      new RegExp(`${digestColumn}[^;]+\\^\\[0-9a-f\\]\\{64\\}\\$`, "u"),
    );
  }
  assert.match(
    stateGcMigration.sql,
    /collected_at IS NULL OR collected_at >= authorized_at/u,
  );
  assert.match(
    stateGcMigration.sql,
    new RegExp(
      [
        "CREATE INDEX writer_supervisor_state_gc_pending_page[\\s\\S]+",
        "\\(\\s*state_owner_id,[\\s\\S]+session_id,[\\s\\S]+",
        "authorized_at,[\\s\\S]+terminal_operation_id COLLATE ",
        'pg_catalog\\."C"[\\s\\S]+WHERE collected_at IS NULL',
      ].join(""),
      "u",
    ),
  );
  assert.match(
    stateGcMigration.sql,
    /DROP CONSTRAINT restore_recovery_cursors_lane_allowed/u,
  );
  assert.match(
    stateGcMigration.sql,
    /ADD CONSTRAINT restore_recovery_cursors_lane_allowed[\s\S]+'supervisor-state-gc'/u,
  );
  assert.match(
    stateGcMigration.sql,
    new RegExp(
      [
        "ADD COLUMN after_authorized_at timestamp with time zone,",
        "[\\s\\S]+ADD COLUMN after_terminal_operation_id[\\s\\S]+",
        'character varying\\(128\\) COLLATE pg_catalog\\."C"',
      ].join(""),
      "u",
    ),
  );
  assert.match(
    stateGcMigration.sql,
    new RegExp(
      [
        "restore_recovery_cursors_gc_position_shape[\\s\\S]+",
        "lane = 'supervisor-state-gc'[\\s\\S]+",
        "after_session_id IS NULL[\\s\\S]+",
        "after_authorized_at IS NULL[\\s\\S]+",
        "after_terminal_operation_id IS NULL[\\s\\S]+",
        "after_session_id IS NOT NULL[\\s\\S]+",
        "after_authorized_at IS NOT NULL[\\s\\S]+",
        "after_terminal_operation_id IS NOT NULL[\\s\\S]+",
        "lane <> 'supervisor-state-gc'[\\s\\S]+",
        "after_authorized_at IS NULL[\\s\\S]+",
        "after_terminal_operation_id IS NULL",
      ].join(""),
      "u",
    ),
  );
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
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
      {},
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
  const migrations = await readAuthorityMigrations();
  const latestMigration = migrations.at(-1);
  const client = new FakeClient([
    {},
    {},
    {},
    {},
    {
      rows: migrations.map(({ checksum, version }) => ({ checksum, version })),
    },
    COMMIT_RESULT,
  ]);
  const store = new PostgresSerializableStore({
    dedicatedPool: new FakePool([client]),
  });

  assert.deepEqual(await store.migrate(), {
    applied: false,
    checksum: latestMigration.checksum,
    version: SESSION_AUTHORITY_MIGRATION_VERSION,
  });
  assert.deepEqual(
    nonResetQueries(client).map(queryText),
    [
      "BEGIN",
      MIGRATION_SEARCH_PATH_QUERY,
      "SELECT pg_catalog.pg_advisory_xact_lock($1::pg_catalog.int8)",
      "CREATE SCHEMA IF NOT EXISTS session_authority",
      nonResetQueries(client)[4][0],
      "SELECT version, checksum FROM session_authority.schema_migrations ORDER BY version",
      "COMMIT",
    ],
  );
  assert.deepEqual(client.releaseCalls, [[]]);
  client.assertExhausted();
});

test("migrate upgrades an exact v1 ledger through v13", async () => {
  const migrations = await readAuthorityMigrations();
  const firstMigration = migrations[0];
  const latestMigration = migrations.at(-1);
  const client = new FakeClient([
    {},
    {},
    {},
    {},
    {
      rows: [
        {
          checksum: firstMigration.checksum,
          version: firstMigration.version,
        },
      ],
    },
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    COMMIT_RESULT,
  ]);
  const store = new PostgresSerializableStore({
    dedicatedPool: new FakePool([client]),
  });

  assert.deepEqual(await store.migrate(), {
    applied: true,
    checksum: latestMigration.checksum,
    version: SESSION_AUTHORITY_MIGRATION_VERSION,
  });
  assert.deepEqual(nonResetQueries(client).slice(6), [
    [migrations[1].sql],
    [
      "INSERT INTO session_authority.schema_migrations (version, checksum, applied_at) VALUES ($1, $2, pg_catalog.transaction_timestamp())",
      [migrations[1].version, migrations[1].checksum],
    ],
    [migrations[2].sql],
    [
      "INSERT INTO session_authority.schema_migrations (version, checksum, applied_at) VALUES ($1, $2, pg_catalog.transaction_timestamp())",
      [migrations[2].version, migrations[2].checksum],
    ],
    [migrations[3].sql],
    [
      "INSERT INTO session_authority.schema_migrations (version, checksum, applied_at) VALUES ($1, $2, pg_catalog.transaction_timestamp())",
      [migrations[3].version, migrations[3].checksum],
    ],
    [migrations[4].sql],
    [
      "INSERT INTO session_authority.schema_migrations (version, checksum, applied_at) VALUES ($1, $2, pg_catalog.transaction_timestamp())",
      [migrations[4].version, migrations[4].checksum],
    ],
    [migrations[5].sql],
    [
      "INSERT INTO session_authority.schema_migrations (version, checksum, applied_at) VALUES ($1, $2, pg_catalog.transaction_timestamp())",
      [migrations[5].version, migrations[5].checksum],
    ],
    [migrations[6].sql],
    [
      "INSERT INTO session_authority.schema_migrations (version, checksum, applied_at) VALUES ($1, $2, pg_catalog.transaction_timestamp())",
      [migrations[6].version, migrations[6].checksum],
    ],
    [migrations[7].sql],
    [
      "INSERT INTO session_authority.schema_migrations (version, checksum, applied_at) VALUES ($1, $2, pg_catalog.transaction_timestamp())",
      [migrations[7].version, migrations[7].checksum],
    ],
    [migrations[8].sql],
    [
      "INSERT INTO session_authority.schema_migrations (version, checksum, applied_at) VALUES ($1, $2, pg_catalog.transaction_timestamp())",
      [migrations[8].version, migrations[8].checksum],
    ],
    [migrations[9].sql],
    [
      "INSERT INTO session_authority.schema_migrations (version, checksum, applied_at) VALUES ($1, $2, pg_catalog.transaction_timestamp())",
      [migrations[9].version, migrations[9].checksum],
    ],
    [migrations[10].sql],
    [
      "INSERT INTO session_authority.schema_migrations (version, checksum, applied_at) VALUES ($1, $2, pg_catalog.transaction_timestamp())",
      [migrations[10].version, migrations[10].checksum],
    ],
    [migrations[11].sql],
    [
      "INSERT INTO session_authority.schema_migrations (version, checksum, applied_at) VALUES ($1, $2, pg_catalog.transaction_timestamp())",
      [migrations[11].version, migrations[11].checksum],
    ],
    [latestMigration.sql],
    [
      "INSERT INTO session_authority.schema_migrations (version, checksum, applied_at) VALUES ($1, $2, pg_catalog.transaction_timestamp())",
      [latestMigration.version, latestMigration.checksum],
    ],
    ["COMMIT"],
  ]);
  assert.deepEqual(client.releaseCalls, [[]]);
  client.assertExhausted();
});

test("migrate upgrades an exact v2 ledger through v13", async () => {
  const migrations = await readAuthorityMigrations();
  const latestMigration = migrations.at(-1);
  const client = new FakeClient([
    {},
    {},
    {},
    {},
    {
      rows: migrations.slice(0, 2).map(({ checksum, version }) => ({
        checksum,
        version,
      })),
    },
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    COMMIT_RESULT,
  ]);
  const store = new PostgresSerializableStore({
    dedicatedPool: new FakePool([client]),
  });

  assert.deepEqual(await store.migrate(), {
    applied: true,
    checksum: latestMigration.checksum,
    version: SESSION_AUTHORITY_MIGRATION_VERSION,
  });
  assert.deepEqual(nonResetQueries(client).slice(6), [
    [migrations[2].sql],
    [
      "INSERT INTO session_authority.schema_migrations (version, checksum, applied_at) VALUES ($1, $2, pg_catalog.transaction_timestamp())",
      [migrations[2].version, migrations[2].checksum],
    ],
    [migrations[3].sql],
    [
      "INSERT INTO session_authority.schema_migrations (version, checksum, applied_at) VALUES ($1, $2, pg_catalog.transaction_timestamp())",
      [migrations[3].version, migrations[3].checksum],
    ],
    [migrations[4].sql],
    [
      "INSERT INTO session_authority.schema_migrations (version, checksum, applied_at) VALUES ($1, $2, pg_catalog.transaction_timestamp())",
      [migrations[4].version, migrations[4].checksum],
    ],
    [migrations[5].sql],
    [
      "INSERT INTO session_authority.schema_migrations (version, checksum, applied_at) VALUES ($1, $2, pg_catalog.transaction_timestamp())",
      [migrations[5].version, migrations[5].checksum],
    ],
    [migrations[6].sql],
    [
      "INSERT INTO session_authority.schema_migrations (version, checksum, applied_at) VALUES ($1, $2, pg_catalog.transaction_timestamp())",
      [migrations[6].version, migrations[6].checksum],
    ],
    [migrations[7].sql],
    [
      "INSERT INTO session_authority.schema_migrations (version, checksum, applied_at) VALUES ($1, $2, pg_catalog.transaction_timestamp())",
      [migrations[7].version, migrations[7].checksum],
    ],
    [migrations[8].sql],
    [
      "INSERT INTO session_authority.schema_migrations (version, checksum, applied_at) VALUES ($1, $2, pg_catalog.transaction_timestamp())",
      [migrations[8].version, migrations[8].checksum],
    ],
    [migrations[9].sql],
    [
      "INSERT INTO session_authority.schema_migrations (version, checksum, applied_at) VALUES ($1, $2, pg_catalog.transaction_timestamp())",
      [migrations[9].version, migrations[9].checksum],
    ],
    [migrations[10].sql],
    [
      "INSERT INTO session_authority.schema_migrations (version, checksum, applied_at) VALUES ($1, $2, pg_catalog.transaction_timestamp())",
      [migrations[10].version, migrations[10].checksum],
    ],
    [migrations[11].sql],
    [
      "INSERT INTO session_authority.schema_migrations (version, checksum, applied_at) VALUES ($1, $2, pg_catalog.transaction_timestamp())",
      [migrations[11].version, migrations[11].checksum],
    ],
    [latestMigration.sql],
    [
      "INSERT INTO session_authority.schema_migrations (version, checksum, applied_at) VALUES ($1, $2, pg_catalog.transaction_timestamp())",
      [latestMigration.version, latestMigration.checksum],
    ],
    ["COMMIT"],
  ]);
  assert.deepEqual(client.releaseCalls, [[]]);
  client.assertExhausted();
});

test("migrate rolls back an installed checksum mismatch", async () => {
  const [firstMigration] = await readAuthorityMigrations();
  const client = new FakeClient([
    {},
    {},
    {},
    {},
    {
      rows: [
        {
          checksum: firstMigration.checksum,
          version: firstMigration.version,
        },
        { checksum: "0".repeat(64), version: 2 },
      ],
    },
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

test("migrate does not trust externally constructed store error state", async () => {
  const forgedError = new PostgresSerializableStoreError(
    "client_release_failed",
    "committed",
  );
  const client = new FakeClient([{}, forgedError, {}]);
  const store = new PostgresSerializableStore({
    dedicatedPool: new FakePool([client]),
  });

  await assertStoreError(store.migrate(), {
    code: "migration_failed",
    commitState: "not-committed",
    omittedText: forgedError.message,
  });
  assert.deepEqual(
    nonResetQueries(client).map(queryText),
    [
      "BEGIN",
      MIGRATION_SEARCH_PATH_QUERY,
      "SELECT pg_catalog.pg_advisory_xact_lock($1::pg_catalog.int8)",
      "ROLLBACK",
    ],
  );
  assert.deepEqual(client.releaseCalls, [[]]);
  client.assertExhausted();
});

test("migrate does not trust store errors replayed from another operation", async () => {
  const resetFailure = new Error("prior operation reset failed");
  const sourceClient = new FakeClient(
    [
      {},
      timestampResult("2026-07-23T10:11:12.000Z"),
      transactionIdResult(),
      COMMIT_RESULT,
    ],
    { resetSteps: [DISCARD_RESULT, resetFailure] },
  );
  const sourceStore = new PostgresSerializableStore({
    dedicatedPool: new FakePool([sourceClient]),
  });
  let replayedError;
  await assert.rejects(
    sourceStore.runSerializable(() => "committed"),
    (error) => {
      assert.ok(error instanceof PostgresSerializableStoreError);
      assert.equal(error.code, "client_reset_failed");
      assert.equal(error.commitState, "committed");
      replayedError = error;
      return true;
    },
  );
  sourceClient.assertExhausted();

  const client = new FakeClient([{}, replayedError, {}]);
  const store = new PostgresSerializableStore({
    dedicatedPool: new FakePool([client]),
  });

  await assertStoreError(store.migrate(), {
    code: "migration_failed",
    commitState: "not-committed",
    omittedText: replayedError.message,
  });
  assert.deepEqual(
    nonResetQueries(client).map(queryText),
    [
      "BEGIN",
      MIGRATION_SEARCH_PATH_QUERY,
      "SELECT pg_catalog.pg_advisory_xact_lock($1::pg_catalog.int8)",
      "ROLLBACK",
    ],
  );
  assert.deepEqual(client.releaseCalls, [[]]);
  client.assertExhausted();
});

test("migration errors own their state despite prototype accessors", async () => {
  const prototype = PostgresSerializableStoreError.prototype;
  const originalDescriptor = Object.getOwnPropertyDescriptor(
    prototype,
    "commitState",
  );
  const client = new FakeClient([
    {},
    {},
    {},
    {},
    () => {
      Object.defineProperty(prototype, "commitState", {
        configurable: true,
        get: () => "committed",
        set: () => undefined,
      });
      return { rows: null };
    },
    {},
  ]);
  const store = new PostgresSerializableStore({
    dedicatedPool: new FakePool([client]),
  });

  try {
    await assertStoreError(store.migrate(), {
      code: "migration_state_invalid",
      commitState: "not-committed",
    });
  } finally {
    if (originalDescriptor === undefined) {
      delete prototype.commitState;
    } else {
      Object.defineProperty(prototype, "commitState", originalDescriptor);
    }
  }
  assert.equal(Object.hasOwn(prototype, "commitState"), false);
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
    { rows: [{ checksum: "0".repeat(64), version: 12 }] },
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

test("migrate rejects an exact latest prefix accompanied by an extra version", async () => {
  const migrations = await readAuthorityMigrations();
  const client = new FakeClient([
    {},
    {},
    {},
    {},
    {
      rows: [
        ...migrations.map(({ checksum, version }) => ({ checksum, version })),
        { checksum: "0".repeat(64), version: 12 },
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

test("migrate rejects a migration ledger with a version gap", async () => {
  const migrations = await readAuthorityMigrations();
  const firstMigration = migrations[0];
  const latestMigration = migrations.at(-1);
  const client = new FakeClient([
    {},
    {},
    {},
    {},
    {
      rows: [
        {
          checksum: firstMigration.checksum,
          version: firstMigration.version,
        },
        { checksum: latestMigration.checksum, version: 3 },
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

test("migrate rejects a migration ledger with a duplicate version", async () => {
  const [firstMigration] = await readAuthorityMigrations();
  const client = new FakeClient([
    {},
    {},
    {},
    {},
    {
      rows: [
        {
          checksum: firstMigration.checksum,
          version: firstMigration.version,
        },
        {
          checksum: firstMigration.checksum,
          version: firstMigration.version,
        },
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

test("migrate rejects a malformed migration ledger row", async () => {
  const [firstMigration] = await readAuthorityMigrations();
  const client = new FakeClient([
    {},
    {},
    {},
    {},
    {
      rows: [
        {
          checksum: firstMigration.checksum,
          unexpected: true,
          version: firstMigration.version,
        },
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
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
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
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
    {},
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

test("store branding accepts only instances constructed by this module", () => {
  const store = new PostgresSerializableStore({
    dedicatedPool: new FakePool([]),
  });
  const forged = Object.freeze(
    Object.create(PostgresSerializableStore.prototype),
  );
  let trapCalls = 0;
  const proxied = new Proxy(store, {
    getPrototypeOf() {
      trapCalls += 1;
      return PostgresSerializableStore.prototype;
    },
  });

  assert.equal(isPostgresSerializableStore(store), true);
  assert.equal(isPostgresSerializableStore(forged), false);
  assert.equal(isPostgresSerializableStore(proxied), false);
  assert.equal(trapCalls, 0);
  assert.equal(Object.isFrozen(isPostgresSerializableStore), true);
});

test("shape-invalid borrowed clients are destroyed exactly once", async (t) => {
  for (const releaseError of [
    undefined,
    new Error("invalid client release failed"),
  ]) {
    await t.test(
      releaseError === undefined ? "release succeeds" : "release rejects",
      async () => {
        const releaseCalls = [];
        const client = {
          connection: null,
          async query() {
            throw new Error("invalid client query must not run");
          },
          async release(...args) {
            releaseCalls.push(args);
            if (releaseError !== undefined) throw releaseError;
          },
        };
        const pool = new FakePool([client]);
        const store = new PostgresSerializableStore({
          dedicatedPool: pool,
        });
        let callbackCalls = 0;

        await assertStoreError(
          store.runSerializable(() => {
            callbackCalls += 1;
          }),
          {
            code: "connection_failed",
            commitState: "not-committed",
            omittedText: releaseError?.message,
          },
        );
        assert.equal(callbackCalls, 0);
        assert.equal(pool.connectCalls, 1);
        assert.equal(releaseCalls.length, 1);
        assert.equal(releaseCalls[0].length, 1);
        assert.ok(releaseCalls[0][0] instanceof Error);
      },
    );
  }
});
