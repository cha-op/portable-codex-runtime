import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import test from "node:test";

import { PostgresSerializableStore } from "../src/postgres-serializable-store.mjs";
import {
  PostgresRestoreRecoveryCursorStoreError,
  createPostgresRestoreRecoveryCursorStore,
} from "../src/postgres-restore-recovery-cursor-store.mjs";

const NOW = "2026-08-10T10:00:00.000Z";
const LATER = "2026-08-10T10:00:01.000Z";
const RECOVERY_SCOPE_ID = "production-eu-west-2";
const SESSION_ID = "019f2100-0000-7000-8000-000000000001";
const OTHER_SESSION_ID = "019f2100-0000-7000-8000-000000000002";
const TRANSITION_ID = "019f2100-0000-7000-8000-000000000003";
const REQUEST_SHA256 = "a".repeat(64);
const TRANSACTION_TIMESTAMP_QUERY =
  "SELECT pg_catalog.transaction_timestamp() AS transaction_timestamp, pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id";
const TRANSACTION_ID_QUERY =
  "SELECT pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id";
const DURABLE_COMMIT_QUERY = "SET LOCAL synchronous_commit = on";
const CURSOR_COLUMNS = [
  "recovery_scope_id",
  "lane",
  "after_session_id::pg_catalog.text AS after_session_id",
  "cycle::pg_catalog.text AS cycle",
  "revision::pg_catalog.text AS revision",
  "last_transition_id::pg_catalog.text AS last_transition_id",
  "last_request_sha256",
  "updated_at",
].join(", ");
const INITIALIZE_QUERY = [
  "INSERT INTO session_authority.restore_recovery_cursors",
  "(recovery_scope_id, lane, after_session_id, cycle, revision,",
  "last_transition_id, last_request_sha256, updated_at)",
  "VALUES ($1, $2, NULL, 0, 0, NULL, NULL, $3)",
  "ON CONFLICT (recovery_scope_id, lane) DO NOTHING",
].join(" ");
const READ_QUERY = [
  `SELECT ${CURSOR_COLUMNS}`,
  "FROM session_authority.restore_recovery_cursors",
  "WHERE recovery_scope_id = $1 AND lane = $2",
  "FOR UPDATE",
].join(" ");
const UPDATE_QUERY = [
  "UPDATE session_authority.restore_recovery_cursors",
  "SET after_session_id = $3, cycle = $4, revision = $5,",
  "last_transition_id = $6, last_request_sha256 = $7, updated_at = $8",
  "WHERE recovery_scope_id = $1 AND lane = $2",
  "AND revision = $9 AND cycle = $10",
  "AND after_session_id IS NOT DISTINCT FROM $11",
  `RETURNING ${CURSOR_COLUMNS}`,
].join(" ");

function queryText(args) {
  return typeof args[0] === "string" ? args[0] : args[0]?.text;
}

function assertExtendedQuery(args, text, values) {
  assert.equal(args.length, 1);
  assert.equal(args[0]?.queryMode, "extended");
  assert.equal(args[0]?.text, text);
  assert.deepEqual(args[0]?.values, values);
}

class ScriptedClient {
  constructor(userSteps, { commitError, now = NOW } = {}) {
    this.commitError = commitError;
    this.connection = new EventEmitter();
    this.now = now;
    this.queries = [];
    this.releaseCalls = [];
    this.userSteps = [...userSteps];
  }

  async query(...args) {
    this.queries.push(args);
    const text = queryText(args);
    if (text === "DISCARD ALL") return { command: "DISCARD" };
    if (text === "BEGIN ISOLATION LEVEL SERIALIZABLE READ WRITE") return {};
    if (text === TRANSACTION_TIMESTAMP_QUERY) {
      return {
        rows: [
          {
            transaction_id: "100",
            transaction_timestamp: new Date(this.now),
          },
        ],
      };
    }
    if (text === TRANSACTION_ID_QUERY) {
      return { rows: [{ transaction_id: "100" }] };
    }
    if (text === DURABLE_COMMIT_QUERY) return { command: "SET" };
    if (text === "COMMIT") {
      if (this.commitError !== undefined) throw this.commitError;
      return { command: "COMMIT" };
    }
    if (text === "ROLLBACK") return { command: "ROLLBACK" };
    assert.notEqual(this.userSteps.length, 0, `unexpected query: ${text}`);
    const step = this.userSteps.shift();
    if (typeof step === "function") return step(args);
    if (step instanceof Error) throw step;
    return step;
  }

  async release(...args) {
    this.releaseCalls.push(args);
  }

  assertExhausted({ destroyed = false } = {}) {
    assert.deepEqual(this.userSteps, []);
    assert.equal(this.releaseCalls.length, 1);
    assert.equal(this.releaseCalls[0].length, destroyed ? 1 : 0);
    assert.equal(this.connection.listenerCount("errorMessage"), 0);
  }
}

class ScriptedPool {
  constructor(clients) {
    this.clients = [...clients];
    this.connectCalls = 0;
  }

  async connect() {
    this.connectCalls += 1;
    assert.notEqual(this.clients.length, 0, "unexpected pool.connect()");
    return this.clients.shift();
  }
}

function cursorRow(overrides = {}) {
  return {
    recovery_scope_id: RECOVERY_SCOPE_ID,
    lane: "generation",
    after_session_id: null,
    cycle: "0",
    revision: "0",
    last_transition_id: null,
    last_request_sha256: null,
    updated_at: new Date(NOW),
    ...overrides,
  };
}

function createHarness(clients) {
  const pool = new ScriptedPool(clients);
  const store = new PostgresSerializableStore({
    dedicatedPool: pool,
    maxTransactionAttempts: 1,
  });
  const cursorStore = createPostgresRestoreRecoveryCursorStore({ store });
  return { cursorStore, pool, store };
}

function readRequest(overrides = {}) {
  return {
    recoveryScopeId: RECOVERY_SCOPE_ID,
    lane: "generation",
    ...overrides,
  };
}

function advanceRequest(overrides = {}) {
  return {
    recoveryScopeId: RECOVERY_SCOPE_ID,
    lane: "generation",
    transitionId: TRANSITION_ID,
    requestSha256: REQUEST_SHA256,
    expectedAfterSessionId: null,
    expectedCycle: "0",
    expectedRevision: "0",
    nextAfterSessionId: SESSION_ID,
    ...overrides,
  };
}

async function assertCursorError(promise, code) {
  await assert.rejects(promise, (error) => {
    assert.ok(error instanceof PostgresRestoreRecoveryCursorStoreError);
    assert.equal(error.name, "PostgresRestoreRecoveryCursorStoreError");
    assert.equal(error.code, code);
    assert.equal(error.retryable, false);
    assert.equal(Object.hasOwn(error, "cause"), false);
    assert.equal(Object.isFrozen(error), true);
    return true;
  });
}

test("readLane lazily initializes and freezes one scoped lane", async () => {
  const client = new ScriptedClient([
    (args) => {
      assertExtendedQuery(args, INITIALIZE_QUERY, [
        RECOVERY_SCOPE_ID,
        "generation",
        NOW,
      ]);
      return { command: "INSERT", rowCount: 1, rows: [] };
    },
    (args) => {
      assertExtendedQuery(args, READ_QUERY, [
        RECOVERY_SCOPE_ID,
        "generation",
      ]);
      return { rows: [cursorRow()] };
    },
  ]);
  const { cursorStore, pool } = createHarness([client]);

  const cursor = await cursorStore.readLane(readRequest());

  assert.deepEqual(cursor, {
    recoveryScopeId: RECOVERY_SCOPE_ID,
    lane: "generation",
    afterSessionId: null,
    cycle: "0",
    revision: "0",
    lastTransitionId: null,
    lastRequestSha256: null,
    updatedAt: NOW,
  });
  assert.equal(Object.isFrozen(cursor), true);
  assert.equal(Object.isFrozen(cursorStore), true);
  assert.deepEqual(Object.keys(cursorStore), ["advanceLane", "readLane"]);
  assert.equal(pool.clients.length, 0);
  client.assertExhausted();
});

test("advanceLane performs an exact revision-cycle-cursor CAS", async () => {
  const target = cursorRow({
    after_session_id: SESSION_ID,
    revision: "1",
    last_transition_id: TRANSITION_ID,
    last_request_sha256: REQUEST_SHA256,
    updated_at: new Date(LATER),
  });
  const client = new ScriptedClient(
    [
      { command: "INSERT", rowCount: 0, rows: [] },
      { rows: [cursorRow()] },
      (args) => {
        assertExtendedQuery(args, UPDATE_QUERY, [
          RECOVERY_SCOPE_ID,
          "generation",
          SESSION_ID,
          "0",
          "1",
          TRANSITION_ID,
          REQUEST_SHA256,
          LATER,
          "0",
          "0",
          null,
        ]);
        return { command: "UPDATE", rowCount: 1, rows: [target] };
      },
    ],
    { now: LATER },
  );
  const { cursorStore } = createHarness([client]);

  const receipt = await cursorStore.advanceLane(advanceRequest());

  assert.equal(receipt.advanced, true);
  assert.equal(receipt.cursor.afterSessionId, SESSION_ID);
  assert.equal(receipt.cursor.revision, "1");
  assert.equal(receipt.cursor.cycle, "0");
  assert.equal(receipt.cursor.updatedAt, LATER);
  assert.equal(Object.isFrozen(receipt), true);
  client.assertExhausted();
});

test("advanceLane recognizes exact durable replay without a second update", async () => {
  const target = cursorRow({
    after_session_id: SESSION_ID,
    revision: "1",
    last_transition_id: TRANSITION_ID,
    last_request_sha256: REQUEST_SHA256,
  });
  const client = new ScriptedClient([
    { command: "INSERT", rowCount: 0, rows: [] },
    { rows: [target] },
  ]);
  const { cursorStore } = createHarness([client]);

  const receipt = await cursorStore.advanceLane(advanceRequest());

  assert.equal(receipt.advanced, false);
  assert.equal(receipt.cursor.revision, "1");
  assert.equal(
    client.queries.some((args) => queryText(args) === UPDATE_QUERY),
    false,
  );
  client.assertExhausted();
});

test("advanceLane wraps a completed sweep to null and increments cycle", async () => {
  const target = cursorRow({
    after_session_id: null,
    cycle: "1",
    revision: "2",
    last_transition_id: TRANSITION_ID,
    last_request_sha256: REQUEST_SHA256,
    updated_at: new Date(LATER),
  });
  const client = new ScriptedClient(
    [
      { command: "INSERT", rowCount: 0, rows: [] },
      {
        rows: [
          cursorRow({
            after_session_id: SESSION_ID,
            revision: "1",
            last_transition_id:
              "019f2100-0000-7000-8000-000000000004",
            last_request_sha256: "b".repeat(64),
          }),
        ],
      },
      { command: "UPDATE", rowCount: 1, rows: [target] },
    ],
    { now: LATER },
  );
  const { cursorStore } = createHarness([client]);

  const receipt = await cursorStore.advanceLane(
    advanceRequest({
      expectedAfterSessionId: SESSION_ID,
      expectedRevision: "1",
      nextAfterSessionId: null,
    }),
  );

  assert.equal(receipt.cursor.afterSessionId, null);
  assert.equal(receipt.cursor.cycle, "1");
  assert.equal(receipt.cursor.revision, "2");
  client.assertExhausted();
});

test("advanceLane rejects stale durable state before update", async () => {
  const client = new ScriptedClient([
    { command: "INSERT", rowCount: 0, rows: [] },
    {
      rows: [
        cursorRow({
          after_session_id: SESSION_ID,
          revision: "1",
          last_transition_id:
            "019f2100-0000-7000-8000-000000000004",
          last_request_sha256: "b".repeat(64),
        }),
      ],
    },
  ]);
  const { cursorStore } = createHarness([client]);

  await assertCursorError(
    cursorStore.advanceLane(advanceRequest()),
    "postgres_restore_recovery_cursor_conflict",
  );

  assert.equal(
    client.queries.some((args) => queryText(args) === UPDATE_QUERY),
    false,
  );
  client.assertExhausted();
});

test("advanceLane readback resolves an acknowledged-loss commit exactly once", async () => {
  const target = cursorRow({
    after_session_id: SESSION_ID,
    revision: "1",
    last_transition_id: TRANSITION_ID,
    last_request_sha256: REQUEST_SHA256,
    updated_at: new Date(LATER),
  });
  const committed = new ScriptedClient(
    [
      { command: "INSERT", rowCount: 0, rows: [] },
      { rows: [cursorRow()] },
      { command: "UPDATE", rowCount: 1, rows: [target] },
    ],
    {
      commitError: new Error("COMMIT acknowledgement lost"),
      now: LATER,
    },
  );
  const readback = new ScriptedClient([{ rows: [target] }], {
    now: LATER,
  });
  const { cursorStore } = createHarness([committed, readback]);

  const receipt = await cursorStore.advanceLane(advanceRequest());

  assert.equal(receipt.advanced, false);
  assert.equal(receipt.cursor.revision, "1");
  assert.equal(
    readback.queries.some((args) => queryText(args) === INITIALIZE_QUERY),
    false,
  );
  assert.equal(
    readback.queries.some((args) => queryText(args) === UPDATE_QUERY),
    false,
  );
  committed.assertExhausted({ destroyed: true });
  readback.assertExhausted();
});

test("advanceLane preserves uncertainty when readback still has expected state", async () => {
  const committed = new ScriptedClient(
    [
      { command: "INSERT", rowCount: 0, rows: [] },
      { rows: [cursorRow()] },
      {
        command: "UPDATE",
        rowCount: 1,
        rows: [
          cursorRow({
            after_session_id: SESSION_ID,
            revision: "1",
            last_transition_id: TRANSITION_ID,
            last_request_sha256: REQUEST_SHA256,
          }),
        ],
      },
    ],
    { commitError: new Error("COMMIT acknowledgement lost") },
  );
  const readback = new ScriptedClient([{ rows: [cursorRow()] }]);
  const { cursorStore } = createHarness([committed, readback]);

  await assertCursorError(
    cursorStore.advanceLane(advanceRequest()),
    "postgres_restore_recovery_cursor_outcome_uncertain",
  );

  committed.assertExhausted({ destroyed: true });
  readback.assertExhausted();
});

test("a proved not-committed missing initial row retries initialize and CAS", async () => {
  const readback = new ScriptedClient([{ rows: [] }]);
  const target = cursorRow({
    after_session_id: SESSION_ID,
    revision: "1",
    last_transition_id: TRANSITION_ID,
    last_request_sha256: REQUEST_SHA256,
    updated_at: new Date(LATER),
  });
  const retry = new ScriptedClient(
    [
      { command: "INSERT", rowCount: 1, rows: [] },
      { rows: [cursorRow({ updated_at: new Date(LATER) })] },
      { command: "UPDATE", rowCount: 1, rows: [target] },
    ],
    { now: LATER },
  );
  const { cursorStore, pool } = createHarness([
    new Error("connection unavailable before BEGIN"),
    readback,
    retry,
  ]);

  const receipt = await cursorStore.advanceLane(advanceRequest());

  assert.equal(receipt.advanced, true);
  assert.equal(receipt.cursor.revision, "1");
  assert.equal(pool.connectCalls, 3);
  readback.assertExhausted();
  retry.assertExhausted();
});

test("malformed durable cursor rows fail closed", async () => {
  const cases = [
    cursorRow({ revision: "-1" }),
    cursorRow({ cycle: "1", revision: "0" }),
    cursorRow({ revision: "1" }),
    cursorRow({ last_request_sha256: "b".repeat(64) }),
    cursorRow({ updated_at: "not-a-timestamp" }),
    { ...cursorRow(), unexpected: true },
  ];

  for (const row of cases) {
    const client = new ScriptedClient([
      { command: "INSERT", rowCount: 0, rows: [] },
      { rows: [row] },
    ]);
    const { cursorStore } = createHarness([client]);
    await assertCursorError(
      cursorStore.readLane(readRequest()),
      "postgres_restore_recovery_cursor_state_invalid",
    );
    client.assertExhausted();
  }
});

test("constructor and request proxy rejection invokes no hostile traps", async () => {
  const { cursorStore, pool, store } = createHarness([]);
  let trapCalls = 0;
  const hostileStore = new Proxy(store, {
    getPrototypeOf() {
      trapCalls += 1;
      throw new Error("must not run getPrototypeOf");
    },
    ownKeys() {
      trapCalls += 1;
      throw new Error("must not run ownKeys");
    },
  });
  assert.throws(
    () => createPostgresRestoreRecoveryCursorStore({ store: hostileStore }),
    (error) =>
      error instanceof PostgresRestoreRecoveryCursorStoreError &&
      error.code ===
        "invalid_postgres_restore_recovery_cursor_store_options",
  );

  const hostileRequest = new Proxy(readRequest(), {
    getPrototypeOf() {
      trapCalls += 1;
      throw new Error("must not run getPrototypeOf");
    },
    ownKeys() {
      trapCalls += 1;
      throw new Error("must not run ownKeys");
    },
  });
  await assertCursorError(
    cursorStore.readLane(hostileRequest),
    "invalid_postgres_restore_recovery_cursor_store_request",
  );
  assert.equal(trapCalls, 0);
  assert.equal(pool.connectCalls, 0);
});

test("revoked row arrays fail closed without native leakage", async () => {
  const { proxy, revoke } = Proxy.revocable([], {});
  revoke();
  const client = new ScriptedClient([
    { command: "INSERT", rowCount: 0, rows: [] },
    { rows: proxy },
  ]);
  const { cursorStore } = createHarness([client]);

  await assertCursorError(
    cursorStore.readLane(readRequest()),
    "postgres_restore_recovery_cursor_state_invalid",
  );

  client.assertExhausted();
});

test("timestamp brand checks do not traverse a hostile prototype", async () => {
  let trapCalls = 0;
  const hostilePrototype = new Proxy(Object.create(null), {
    getPrototypeOf() {
      trapCalls += 1;
      throw new Error("prototype trap must not run");
    },
  });
  const timestamp = Object.create(hostilePrototype);
  const client = new ScriptedClient([
    { command: "INSERT", rowCount: 0, rows: [] },
    { rows: [cursorRow({ updated_at: timestamp })] },
  ]);
  const { cursorStore } = createHarness([client]);

  await assertCursorError(
    cursorStore.readLane(readRequest()),
    "postgres_restore_recovery_cursor_state_invalid",
  );

  assert.equal(trapCalls, 0);
  client.assertExhausted();
});

test("invalid or exhausted transitions perform zero database access", async () => {
  const { cursorStore, pool } = createHarness([]);
  const requests = [
    advanceRequest({ nextAfterSessionId: SESSION_ID.toUpperCase() }),
    advanceRequest({
      expectedAfterSessionId: OTHER_SESSION_ID,
      nextAfterSessionId: SESSION_ID,
    }),
    advanceRequest({
      expectedAfterSessionId: SESSION_ID,
      nextAfterSessionId: OTHER_SESSION_ID,
    }),
    advanceRequest({ expectedRevision: "9223372036854775807" }),
    { ...advanceRequest(), unexpected: true },
  ];

  for (const request of requests) {
    await assertCursorError(
      cursorStore.advanceLane(request),
      "invalid_postgres_restore_recovery_cursor_store_request",
    );
  }
  assert.equal(pool.connectCalls, 0);
});

test("error construction rejects unsupported codes", () => {
  assert.throws(
    () => new PostgresRestoreRecoveryCursorStoreError("unsupported"),
    TypeError,
  );
  assert.equal(Object.isFrozen(PostgresRestoreRecoveryCursorStoreError), true);
  assert.equal(
    Object.isFrozen(PostgresRestoreRecoveryCursorStoreError.prototype),
    true,
  );
});
