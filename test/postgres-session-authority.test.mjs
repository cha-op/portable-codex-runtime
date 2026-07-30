import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import test from "node:test";

import {
  PostgresSerializableStore,
} from "../src/postgres-serializable-store.mjs";
import {
  PostgresSessionAuthority,
  PostgresSessionAuthorityError,
  SESSION_AUTHORITY_DOCUMENT_VERSION,
} from "../src/postgres-session-authority.mjs";
import {
  createSessionManifest,
  serializeSessionManifest,
} from "../src/session-storage-contracts.mjs";

const SESSION_ID = "019f2100-0000-7000-8000-000000000001";
const OTHER_SESSION_ID = "019f2100-0000-7000-8000-000000000002";
const IMAGE_DIGEST = `sha256:${"a".repeat(64)}`;
const NOW = "2026-07-29T12:34:56.789Z";
const TRANSACTION_TIMESTAMP_QUERY =
  "SELECT pg_catalog.transaction_timestamp() AS transaction_timestamp, pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id";
const TRANSACTION_ID_QUERY =
  "SELECT pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id";
const DURABLE_COMMIT_QUERY = "SET LOCAL synchronous_commit = on";
const NO_ACTIVE_ROWS = {
  rows: [{ operation_count: 0, reservation_count: 0 }],
};
const INSERT_QUERY = [
  "INSERT INTO session_authority.sessions",
  "(session_id, revision, document, created_at, updated_at)",
  "VALUES ($1::uuid, 0, $2::jsonb, $3::timestamptz, $3::timestamptz)",
  "ON CONFLICT (session_id) DO NOTHING",
  "RETURNING session_id, revision, document, created_at, updated_at",
].join(" ");
const READ_QUERY = [
  "SELECT session_id, revision, document, created_at, updated_at",
  "FROM session_authority.sessions",
  "WHERE session_id = $1::uuid",
].join(" ");

class ScriptedClient {
  constructor(userResults) {
    this.connection = new EventEmitter();
    this.queries = [];
    this.releaseCalls = [];
    this.userResults = [...userResults];
  }

  async query(...args) {
    this.queries.push(args);
    const text = typeof args[0] === "string" ? args[0] : args[0]?.text;
    if (text === "DISCARD ALL") return { command: "DISCARD" };
    if (text === "BEGIN ISOLATION LEVEL SERIALIZABLE READ WRITE") return {};
    if (text === TRANSACTION_TIMESTAMP_QUERY) {
      return {
        rows: [
          {
            transaction_id: "100",
            transaction_timestamp: new Date(NOW),
          },
        ],
      };
    }
    if (text === TRANSACTION_ID_QUERY) {
      return { rows: [{ transaction_id: "100" }] };
    }
    if (text === DURABLE_COMMIT_QUERY) return { command: "SET" };
    if (text === "COMMIT") return { command: "COMMIT" };
    if (text === "ROLLBACK") return { command: "ROLLBACK" };
    assert.notEqual(
      this.userResults.length,
      0,
      `unexpected authority query: ${text}`,
    );
    const result = this.userResults.shift();
    if (result instanceof Error) throw result;
    return result;
  }

  async release(...args) {
    this.releaseCalls.push(args);
  }

  assertExhausted() {
    assert.deepEqual(this.userResults, []);
    assert.deepEqual(this.releaseCalls, [[]]);
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

function manifest(sessionId = SESSION_ID) {
  return createSessionManifest({
    sessionId,
    codex: {
      rootThreadId: sessionId,
      sessionId,
      ephemeral: false,
      historyMode: "paginated",
    },
    runtime: {
      imageDigest: IMAGE_DIGEST,
      imageMediaType: "application/vnd.oci.image.manifest.v1+json",
      platform: "linux/arm64",
      codexVersion: "codex-cli 0.142.4",
      codexSandbox: "danger-full-access",
    },
  });
}

function storageRef(sessionId = SESSION_ID, storageId = "volume-001") {
  return {
    contractVersion: 1,
    backendId: "single-attach-test",
    storageId,
    sessionId,
  };
}

function backendCapabilities(overrides = {}) {
  return {
    atomicPointInTimeCheckpoint: true,
    exclusiveWriterAttachment: true,
    fencing: "epoch-enforced",
    normalDirectoryAttachment: true,
    ...overrides,
  };
}

function registration(overrides = {}) {
  return {
    manifest: manifest(),
    storageRef: storageRef(),
    backendCapabilities: backendCapabilities(),
    ...overrides,
  };
}

function document(overrides = {}) {
  return {
    documentVersion: SESSION_AUTHORITY_DOCUMENT_VERSION,
    manifest: JSON.parse(serializeSessionManifest(manifest())),
    storageRef: storageRef(),
    backendCapabilities: backendCapabilities(),
    lifecycle: "DETACHED",
    writerEpoch: "0",
    lease: null,
    attachment: null,
    activeOperation: null,
    lastOperation: null,
    recovery: null,
    launch: null,
    ...overrides,
  };
}

function legacyDocument(overrides = {}) {
  const value = document({
    documentVersion: 1,
    ...overrides,
  });
  Reflect.deleteProperty(value, "lastOperation");
  return value;
}

function row(overrides = {}) {
  return {
    session_id: SESSION_ID,
    revision: "0",
    document: structuredClone(document()),
    created_at: new Date(NOW),
    updated_at: new Date(NOW),
    ...overrides,
  };
}

function authorityWithScripts(...scripts) {
  const clients = scripts.map((script) => new ScriptedClient(script));
  const pool = new ScriptedPool(clients);
  const store = new PostgresSerializableStore({
    dedicatedPool: pool,
    maxTransactionAttempts: 1,
  });
  return {
    authority: new PostgresSessionAuthority({ store }),
    clients,
    pool,
    store,
  };
}

function userQueries(client) {
  return client.queries.filter((args) => {
    const text = typeof args[0] === "string" ? args[0] : args[0]?.text;
    return text === INSERT_QUERY || text === READ_QUERY || text === `${READ_QUERY} FOR UPDATE`;
  });
}

function queryTexts(client) {
  return client.queries.map((args) =>
    typeof args[0] === "string" ? args[0] : args[0]?.text,
  );
}

async function assertAuthorityError(promise, code) {
  await assert.rejects(promise, (error) => {
    assert.ok(error instanceof PostgresSessionAuthorityError);
    assert.equal(error.name, "PostgresSessionAuthorityError");
    assert.equal(error.code, code);
    assert.equal(error.retryable, false);
    assert.equal(Object.isFrozen(error), true);
    assert.equal("cause" in error, false);
    return true;
  });
}

test("registerSession inserts one canonical detached document with database time", async () => {
  const insertedRow = row();
  const { authority, clients } = authorityWithScripts([
    { rows: [insertedRow] },
  ]);

  const snapshot = await authority.registerSession(registration());

  assert.deepEqual(snapshot, {
    sessionId: SESSION_ID,
    revision: "0",
    document: document(),
    createdAt: NOW,
    updatedAt: NOW,
  });
  assert.deepEqual(queryTexts(clients[0]), [
    "DISCARD ALL",
    "BEGIN ISOLATION LEVEL SERIALIZABLE READ WRITE",
    TRANSACTION_TIMESTAMP_QUERY,
    INSERT_QUERY,
    TRANSACTION_ID_QUERY,
    DURABLE_COMMIT_QUERY,
    TRANSACTION_ID_QUERY,
    "COMMIT",
    "DISCARD ALL",
  ]);
  const [insert] = userQueries(clients[0]);
  assert.deepEqual(insert, [
    {
      queryMode: "extended",
      text: INSERT_QUERY,
      values: [
        SESSION_ID,
        JSON.stringify(document()),
        NOW,
      ],
    },
  ]);
  clients[0].assertExhausted();
});

test("registerSession rejects non-canonical INSERT RETURNING evidence", async () => {
  const invalidRows = [
    row({ revision: "1" }),
    row({ created_at: new Date("2026-07-29T12:34:55.000Z") }),
    row({
      document: document({
        storageRef: storageRef(SESSION_ID, "unexpected-volume"),
      }),
    }),
  ];
  const { authority, clients } = authorityWithScripts(
    ...invalidRows.map((invalidRow) => [{ rows: [invalidRow] }]),
  );

  for (let index = 0; index < invalidRows.length; index += 1) {
    await assertAuthorityError(
      authority.registerSession(registration()),
      "session_state_invalid",
    );
    assert.deepEqual(queryTexts(clients[index]).slice(-3), [
      TRANSACTION_ID_QUERY,
      "ROLLBACK",
      "DISCARD ALL",
    ]);
    clients[index].assertExhausted();
  }
});

test("registerSession serialization is canonical across reordered identity input", async () => {
  const canonicalManifest = manifest();
  const reorderedManifest = {
    sessionId: canonicalManifest.sessionId,
    schemaVersion: canonicalManifest.schemaVersion,
    runtime: {
      platform: canonicalManifest.runtime.platform,
      imageDigest: canonicalManifest.runtime.imageDigest,
      codexSandbox: canonicalManifest.runtime.codexSandbox,
      imageMediaType: canonicalManifest.runtime.imageMediaType,
      codexVersion: canonicalManifest.runtime.codexVersion,
    },
    layoutVersion: canonicalManifest.layoutVersion,
    codex: {
      sessionId: canonicalManifest.codex.sessionId,
      historyMode: canonicalManifest.codex.historyMode,
      rootThreadId: canonicalManifest.codex.rootThreadId,
      ephemeral: canonicalManifest.codex.ephemeral,
    },
    authMode: canonicalManifest.authMode,
    agents: {
      maxDepth: canonicalManifest.agents.maxDepth,
      defaultMaxSubagents: canonicalManifest.agents.defaultMaxSubagents,
      maxSubagents: canonicalManifest.agents.maxSubagents,
    },
  };
  const { authority, clients } = authorityWithScripts([
    { rows: [row()] },
  ]);

  await authority.registerSession({
    backendCapabilities: {
      normalDirectoryAttachment: true,
      fencing: "epoch-enforced",
      exclusiveWriterAttachment: true,
      atomicPointInTimeCheckpoint: true,
    },
    storageRef: {
      sessionId: SESSION_ID,
      storageId: "volume-001",
      backendId: "single-attach-test",
      contractVersion: 1,
    },
    manifest: reorderedManifest,
  });

  assert.equal(
    userQueries(clients[0])[0][0].values[1],
    JSON.stringify(document()),
  );
  clients[0].assertExhausted();
});

test("registerSession serialization ignores inherited Object.prototype.toJSON", async () => {
  const input = registration();
  const expectedSerialization = JSON.stringify(document());
  const { authority, clients } = authorityWithScripts([
    { rows: [row()] },
  ]);
  const priorToJson = Object.getOwnPropertyDescriptor(
    Object.prototype,
    "toJSON",
  );
  let poisonCalls = 0;
  let snapshot;

  try {
    Object.defineProperty(Object.prototype, "toJSON", {
      configurable: true,
      value() {
        poisonCalls += 1;
        return { poisoned: true };
      },
      writable: true,
    });
    snapshot = await authority.registerSession(input);
  } finally {
    if (priorToJson === undefined) {
      Reflect.deleteProperty(Object.prototype, "toJSON");
    } else {
      Object.defineProperty(Object.prototype, "toJSON", priorToJson);
    }
  }

  assert.equal(poisonCalls, 0);
  assert.equal(
    userQueries(clients[0])[0][0].values[1],
    expectedSerialization,
  );
  assert.deepEqual(snapshot.document, document());
  clients[0].assertExhausted();
});

test("registerSession replays an exact existing document without overwrite", async () => {
  const existing = row();
  const { authority, clients } = authorityWithScripts([
    { rows: [] },
    { rows: [existing] },
    NO_ACTIVE_ROWS,
  ]);

  const snapshot = await authority.registerSession(registration());

  assert.deepEqual(snapshot.document, document());
  assert.deepEqual(userQueries(clients[0]), [
    [
      {
        queryMode: "extended",
        text: INSERT_QUERY,
        values: [SESSION_ID, JSON.stringify(document()), NOW],
      },
    ],
    [
      {
        queryMode: "extended",
        text: `${READ_QUERY} FOR UPDATE`,
        values: [SESSION_ID],
      },
    ],
  ]);
  clients[0].assertExhausted();
});

test("registration replay preserves a legacy revision-zero document", async () => {
  const existing = row({ document: legacyDocument() });
  const { authority, clients } = authorityWithScripts([
    { rows: [] },
    { rows: [existing] },
    NO_ACTIVE_ROWS,
  ]);

  const snapshot = await authority.registerSession(registration());

  assert.deepEqual(snapshot, {
    sessionId: SESSION_ID,
    revision: "0",
    document: legacyDocument(),
    createdAt: NOW,
    updatedAt: NOW,
  });
  assert.equal(
    queryTexts(clients[0]).some((text) => text.startsWith("UPDATE ")),
    false,
  );
  clients[0].assertExhausted();
});

test("registerSession rejects malformed revision and timestamps during replay", async () => {
  const corruptRows = [
    row({ revision: 0 }),
    row({ updated_at: new Date("2026-07-29T12:34:57.000Z") }),
  ];
  const { authority, clients } = authorityWithScripts(
    ...corruptRows.map((corrupt) => [
      { rows: [] },
      { rows: [corrupt] },
    ]),
  );

  for (let index = 0; index < corruptRows.length; index += 1) {
    await assertAuthorityError(
      authority.registerSession(registration()),
      "session_state_invalid",
    );
    assert.equal(userQueries(clients[index]).length, 2);
    clients[index].assertExhausted();
  }
});

test("registerSession rejects a different canonical identity without overwrite", async () => {
  const conflicting = row({
    document: document({
      storageRef: storageRef(SESSION_ID, "volume-conflict"),
    }),
  });
  const { authority, clients } = authorityWithScripts([
    { rows: [] },
    { rows: [conflicting] },
  ]);

  await assertAuthorityError(
    authority.registerSession(registration()),
    "session_identity_conflict",
  );

  assert.deepEqual(queryTexts(clients[0]).slice(-3), [
    TRANSACTION_ID_QUERY,
    "ROLLBACK",
    "DISCARD ALL",
  ]);
  assert.equal(userQueries(clients[0]).length, 2);
  clients[0].assertExhausted();
});

test("readSession returns a canonical snapshot and rejects a missing session", async () => {
  const { authority, clients } = authorityWithScripts(
    [{ rows: [row()] }, NO_ACTIVE_ROWS],
    [{ rows: [] }],
  );

  const snapshot = await authority.readSession({ sessionId: SESSION_ID });
  assert.deepEqual(snapshot, {
    sessionId: SESSION_ID,
    revision: "0",
    document: document(),
    createdAt: NOW,
    updatedAt: NOW,
  });
  await assertAuthorityError(
    authority.readSession({ sessionId: OTHER_SESSION_ID }),
    "session_not_found",
  );
  assert.deepEqual(userQueries(clients[0]), [
    [
      {
        queryMode: "extended",
        text: READ_QUERY,
        values: [SESSION_ID],
      },
    ],
  ]);
  clients[0].assertExhausted();
  clients[1].assertExhausted();
});

test("readSession fails closed on malformed and corrupt rows", async () => {
  const corruptRows = [
    { ...row(), unknown: true },
    row({ revision: 0 }),
    row({ revision: "9223372036854775808" }),
    row({ revision: "2" }),
    row({ document: { ...document(), lifecycle: "ATTACHED" } }),
    row({ document: { ...document(), unknown: true } }),
    row({ created_at: NOW }),
    row({ updated_at: new Date("2026-07-29T12:34:55.000Z") }),
    row({ session_id: OTHER_SESSION_ID }),
  ];
  const { authority, clients } = authorityWithScripts(
    ...corruptRows.map((corrupt) => [{ rows: [corrupt] }]),
  );

  for (let index = 0; index < corruptRows.length; index += 1) {
    await assertAuthorityError(
      authority.readSession({ sessionId: SESSION_ID }),
      "session_state_invalid",
    );
    clients[index].assertExhausted();
  }
});

test("registration and read input validation happen before PostgreSQL access", async () => {
  const { authority, pool, store } = authorityWithScripts();
  assert.throws(
    () => new PostgresSessionAuthority({ store, extra: true }),
    (error) =>
      error instanceof PostgresSessionAuthorityError &&
      error.code === "invalid_authority_options",
  );
  assert.throws(
    () =>
      new PostgresSessionAuthority({
        store: Object.freeze({ runSerializable() {} }),
      }),
    (error) =>
      error instanceof PostgresSessionAuthorityError &&
      error.code === "invalid_authority_options",
  );

  await assertAuthorityError(
    authority.registerSession({
      ...registration(),
      storageRef: storageRef(OTHER_SESSION_ID),
    }),
    "invalid_session_registration",
  );
  await assertAuthorityError(
    authority.registerSession({
      ...registration(),
      backendCapabilities: backendCapabilities({
        normalDirectoryAttachment: false,
      }),
    }),
    "invalid_session_registration",
  );
  await assertAuthorityError(
    authority.registerSession({ ...registration(), extra: true }),
    "invalid_session_registration",
  );
  await assertAuthorityError(
    authority.readSession({ sessionId: "not-a-uuid" }),
    "invalid_session_read",
  );
  await assertAuthorityError(
    authority.readSession({ sessionId: SESSION_ID, extra: true }),
    "invalid_session_read",
  );
  assert.equal(pool.connectCalls, 0);
});

test("registration storage identity ignores RegExp prototype poisoning", async () => {
  const { authority, pool } = authorityWithScripts();
  const originalRegExpExec = RegExp.prototype.exec;
  const originalRegExpTest = RegExp.prototype.test;

  try {
    RegExp.prototype.exec = () => [];
    RegExp.prototype.test = () => true;
    await assertAuthorityError(
      authority.registerSession({
        ...registration(),
        storageRef: storageRef(SESSION_ID, "contains space"),
      }),
      "invalid_session_registration",
    );
  } finally {
    RegExp.prototype.exec = originalRegExpExec;
    RegExp.prototype.test = originalRegExpTest;
  }

  assert.equal(pool.connectCalls, 0);
});

test("public snapshots are deeply frozen defensive copies", async () => {
  const sourceRow = row();
  const { authority, clients } = authorityWithScripts([
    { rows: [sourceRow] },
    NO_ACTIVE_ROWS,
  ]);

  const snapshot = await authority.readSession({ sessionId: SESSION_ID });
  assert.equal(Object.isFrozen(snapshot), true);
  assert.equal(Object.isFrozen(snapshot.document), true);
  assert.equal(Object.isFrozen(snapshot.document.manifest.runtime), true);
  assert.equal(Object.isFrozen(snapshot.document.storageRef), true);
  assert.equal(
    Object.isFrozen(snapshot.document.backendCapabilities),
    true,
  );
  sourceRow.document.manifest.runtime.codexVersion =
    "codex-cli 9.9.9";
  sourceRow.document.storageRef.storageId = "mutated";
  assert.equal(
    snapshot.document.manifest.runtime.codexVersion,
    "codex-cli 0.142.4",
  );
  assert.equal(snapshot.document.storageRef.storageId, "volume-001");
  assert.throws(() => {
    snapshot.document.storageRef.storageId = "mutated";
  }, TypeError);
  clients[0].assertExhausted();
});

test("authority validation uses captured intrinsic decisions", async () => {
  const { authority, clients } = authorityWithScripts([
    { rows: [row()] },
    NO_ACTIVE_ROWS,
  ]);
  const originalEvery = Array.prototype.every;
  const originalDateParse = Date.parse;
  const originalNumberIsFinite = Number.isFinite;
  const originalRegExpExec = RegExp.prototype.exec;
  const originalRegExpTest = RegExp.prototype.test;
  let snapshot;

  try {
    Array.prototype.every = () => false;
    Date.parse = () => Number.NaN;
    Number.isFinite = () => false;
    RegExp.prototype.exec = () => null;
    RegExp.prototype.test = () => false;
    snapshot = await authority.readSession({ sessionId: SESSION_ID });
  } finally {
    Array.prototype.every = originalEvery;
    Date.parse = originalDateParse;
    Number.isFinite = originalNumberIsFinite;
    RegExp.prototype.exec = originalRegExpExec;
    RegExp.prototype.test = originalRegExpTest;
  }

  assert.deepEqual(snapshot, {
    sessionId: SESSION_ID,
    revision: "0",
    document: document(),
    createdAt: NOW,
    updatedAt: NOW,
  });
  clients[0].assertExhausted();
});
