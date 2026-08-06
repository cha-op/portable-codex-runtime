import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import test from "node:test";

import {
  PostgresSerializableStore,
} from "../src/postgres-serializable-store.mjs";
import {
  PostgresSessionAuthority,
  PostgresSessionAuthorityError,
  RESTORE_ATTACHMENT_ACTIVATION_OPERATION_KIND,
  RESTORE_DESTINATION_GENERATION_OPERATION_KIND,
  SESSION_AUTHORITY_DOCUMENT_VERSION,
  WRITER_ATTACHMENT_ACQUIRE_OPERATION_KIND,
  WRITER_RELEASE_OPERATION_KIND,
  createRestoreAttachmentActivationOperationRequest,
  createRestoreAttachmentActivationOperationRequestV2,
  createRestoreDestinationGenerationOperationRequest,
  createRestoreDestinationGenerationOperationRequestV2,
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

function attachedSnapshot() {
  const lease = {
    contractVersion: 1,
    expiresAt: "2026-07-29T13:34:56.789Z",
    fencingEpoch: "2",
    holderId: "host-001",
    leaseId: "lease-001",
    sessionId: SESSION_ID,
  };
  const attachment = {
    attachmentId: "attachment-001",
    backendId: "single-attach-test",
    contractVersion: 1,
    fencingEpoch: lease.fencingEpoch,
    holderId: lease.holderId,
    kind: "directory",
    leaseId: lease.leaseId,
    mode: "read-write",
    operationId: "attachment-operation-001",
    proofId: "attachment-proof-001",
    rootPath: "/var/lib/portable-codex/session-001",
    sessionId: SESSION_ID,
    storageId: "volume-001",
  };
  return {
    createdAt: NOW,
    document: document({
      attachment,
      lastOperation: {
        conflictClass: "session-mutation",
        expectedSessionRevision: "0",
        kind: WRITER_ATTACHMENT_ACQUIRE_OPERATION_KIND,
        operationId: attachment.operationId,
        operationRevision: "2",
        requestSha256: "d".repeat(64),
        reservationId: "attachment-reservation-001",
        resultSha256: "e".repeat(64),
        state: "committed",
      },
      lease,
      lifecycle: "ATTACHED",
      writerEpoch: lease.fencingEpoch,
    }),
    revision: "3",
    sessionId: SESSION_ID,
    updatedAt: NOW,
  };
}

function restoreGenerationAdmissionFixture(
  expectedSession = attachedSnapshot(),
) {
  const { lease, manifest: sessionManifest, storageRef: sessionStorage } =
    expectedSession.document;
  const checkpoint = {
    artifactId: "artifact-001",
    backendId: sessionStorage.backendId,
    checkpointClass: "clean",
    checkpointId: "checkpoint-001",
    codexSessionId: sessionManifest.codex.sessionId,
    codexThreadId: sessionManifest.codex.rootThreadId,
    contractVersion: 1,
    createdAt: "2026-07-29T12:00:00.000Z",
    imageDigest: sessionManifest.runtime.imageDigest,
    sessionId: expectedSession.sessionId,
    sourceFencingEpoch: "1",
    storageId: sessionStorage.storageId,
  };
  return {
    checkpoint,
    request: {
      backendId: sessionStorage.backendId,
      contractVersion: 1,
      fencingEpoch: lease.fencingEpoch,
      holderId: lease.holderId,
      leaseId: lease.leaseId,
      operation: "restore",
      operationId: "restore-operation-001",
      sessionId: expectedSession.sessionId,
      storageId: sessionStorage.storageId,
      target: {
        artifactId: checkpoint.artifactId,
        checkpointId: checkpoint.checkpointId,
        kind: "checkpoint",
      },
    },
  };
}

function writerLaunchIntentFixture(expectedSession = attachedSnapshot()) {
  const runtime = expectedSession.document.manifest.runtime;
  const [os, architecture] = runtime.platform.split("/");
  return {
    launchAttemptId: "writer-launch-001",
    measuredImage: {
      projection: {
        codexSandbox: runtime.codexSandbox,
        codexVersion: runtime.codexVersion,
        platformImage: {
          architecture,
          config: {
            digest: `sha256:${"b".repeat(64)}`,
            mediaType: "application/vnd.oci.image.config.v1+json",
            size: 512,
          },
          digest: runtime.imageDigest,
          mediaType: runtime.imageMediaType,
          os,
          size: 1024,
        },
      },
      runtimeIdentity: {
        codexBinaryPath: "/opt/portable-codex/bin/codex",
        codexBinarySha256: "c".repeat(64),
        codexVersion: runtime.codexVersion,
        platformImageDigest: runtime.imageDigest,
      },
    },
    supervisor: {
      contractVersion: 1,
      supervisorId: "supervisor-001",
    },
  };
}

function detachedRestoreActivationFixture() {
  const expectedSession = {
    createdAt: NOW,
    document: document({
      lastOperation: {
        conflictClass: "session-mutation",
        expectedSessionRevision: "10",
        kind: WRITER_RELEASE_OPERATION_KIND,
        operationId: "writer-release-001",
        operationRevision: "2",
        requestSha256: "f".repeat(64),
        reservationId: "writer-release-reservation-001",
        resultSha256: "1".repeat(64),
        state: "committed",
      },
      writerEpoch: "2",
    }),
    revision: "13",
    sessionId: SESSION_ID,
    updatedAt: "2026-07-29T12:35:00.000Z",
  };
  const generation = {
    binding: {
      attachment: attachedSnapshot().document.attachment,
      marker: "exact-generation-binding",
    },
    checkpointId: "checkpoint-001",
    claimedAt: "2026-07-29T12:34:57.000Z",
    committedAt: "2026-07-29T12:34:58.000Z",
    document: {
      marker: "exact-generation-document",
    },
    generationId: "restore-generation-001",
    operationId: "restore-generation-operation-001",
    sessionId: SESSION_ID,
    state: "committed",
  };
  return {
    destinationRootPath: "/var/lib/portable-codex/restores/session-001",
    expectedSession,
    generation,
    holderId: "host-restore-002",
    launchIntent: writerLaunchIntentFixture(expectedSession),
    leaseDurationMilliseconds: 60_000,
    predecessor: {
      attachmentId: "attachment-001",
      detachOperationId: "writer-release-001",
      stopOperationId: "writer-launch-stop-001",
    },
  };
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
    authority: new PostgresSessionAuthority({
      restoreAttachmentActivationV2FleetCompatible: true,
      restoreGenerationV2FleetCompatible: true,
      store,
    }),
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

function reversePlainData(value) {
  if (Array.isArray(value)) return value.map(reversePlainData);
  if (value === null || typeof value !== "object") return value;
  return Object.fromEntries(
    Object.entries(value)
      .reverse()
      .map(([key, child]) => [key, reversePlainData(child)]),
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
  for (const options of [
    { restoreGenerationV2FleetCompatible: "true", store },
    { restoreAttachmentActivationV2FleetCompatible: 1, store },
    {
      restoreAttachmentActivationV2FleetCompatible: true,
      restoreGenerationV2FleetCompatible: null,
      store,
    },
  ]) {
    assert.throws(
      () => new PostgresSessionAuthority(options),
      (error) =>
        error instanceof PostgresSessionAuthorityError &&
        error.code === "invalid_authority_options",
    );
  }
  for (const options of [
    { store },
    { restoreGenerationV2FleetCompatible: false, store },
    { restoreAttachmentActivationV2FleetCompatible: true, store },
    {
      restoreAttachmentActivationV2FleetCompatible: false,
      restoreGenerationV2FleetCompatible: true,
      store,
    },
  ]) {
    assert.doesNotThrow(() => new PostgresSessionAuthority(options));
  }

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

test("restore generation V2 requests canonically bind one launch intent", () => {
  const expectedSession = attachedSnapshot();
  const admission = restoreGenerationAdmissionFixture(expectedSession);
  const launchIntent = writerLaunchIntentFixture(expectedSession);
  const legacyRequest =
    createRestoreDestinationGenerationOperationRequest({
      admission,
      expectedSession,
    });

  const request = createRestoreDestinationGenerationOperationRequestV2({
    admission,
    expectedSession,
    launchIntent,
  });
  const reordered =
    createRestoreDestinationGenerationOperationRequestV2(
      reversePlainData({ admission, expectedSession, launchIntent }),
    );

  assert.deepEqual(Reflect.ownKeys(request), [
    "admission",
    "contractVersion",
    "launchIntent",
    "predeterminedResult",
  ]);
  assert.equal(request.contractVersion, 2);
  assert.equal(JSON.stringify(request), JSON.stringify(reordered));
  assert.deepEqual(
    JSON.parse(JSON.stringify(request)),
    {
      admission,
      contractVersion: 2,
      launchIntent,
      predeterminedResult: JSON.parse(
        JSON.stringify(legacyRequest.predeterminedResult),
      ),
    },
  );
  assert.equal(Object.isFrozen(request), true);
  assert.equal(Object.isFrozen(request.admission), true);
  assert.equal(Object.isFrozen(request.launchIntent), true);
  assert.equal(Object.isFrozen(request.launchIntent.measuredImage), true);
  assert.equal(
    Object.isFrozen(
      request.launchIntent.measuredImage.projection.platformImage.config,
    ),
    true,
  );

  launchIntent.supervisor.supervisorId = "mutated-supervisor";
  admission.request.target.artifactId = "mutated-artifact";
  assert.equal(
    request.launchIntent.supervisor.supervisorId,
    "supervisor-001",
  );
  assert.equal(
    request.admission.request.target.artifactId,
    "artifact-001",
  );
});

test("restore generation request versions reject crossed launch identities before database access", async () => {
  const expectedSession = attachedSnapshot();
  const admission = restoreGenerationAdmissionFixture(expectedSession);
  const launchIntent = writerLaunchIntentFixture(expectedSession);
  const request = createRestoreDestinationGenerationOperationRequestV2({
    admission,
    expectedSession,
    launchIntent,
  });
  const { authority, pool } = authorityWithScripts();
  const restore = {
    completion: {
      materialization: {},
      replayed: false,
      result: {},
    },
    expectedOperationRevision: "1",
    expectedSession,
    kind: RESTORE_DESTINATION_GENERATION_OPERATION_KIND,
    operationId: admission.request.operationId,
    request,
  };

  assert.throws(
    () =>
      createRestoreDestinationGenerationOperationRequestV2({
        admission,
        expectedSession,
        launchIntent: {
          ...structuredClone(launchIntent),
          launchAttemptId: admission.request.operationId,
        },
      }),
    (error) =>
      error instanceof PostgresSessionAuthorityError &&
      error.code === "invalid_operation_request",
  );
  assert.throws(
    () =>
      createRestoreDestinationGenerationOperationRequestV2({
        admission,
        expectedSession,
        launchIntent: {
          ...structuredClone(launchIntent),
          measuredImage: {
            ...structuredClone(launchIntent.measuredImage),
            runtimeIdentity: {
              ...structuredClone(
                launchIntent.measuredImage.runtimeIdentity,
              ),
              platformImageDigest: `sha256:${"d".repeat(64)}`,
            },
          },
        },
      }),
    (error) =>
      error instanceof PostgresSessionAuthorityError &&
      error.code === "invalid_operation_request",
  );

  await assertAuthorityError(
    authority.finalizeRestoreDestinationGenerationAndReserveWriterLaunchAttempt(
      {
        launch: {
          ...structuredClone(launchIntent),
          launchAttemptId: "writer-launch-mismatch",
        },
        restore,
      },
    ),
    "invalid_operation_request",
  );
  await assertAuthorityError(
    authority.finalizeRestoreDestinationGeneration({
      ...structuredClone(restore),
    }),
    "invalid_operation_request",
  );

  const legacyRequest =
    createRestoreDestinationGenerationOperationRequest({
      admission,
      expectedSession,
    });
  assert.equal(legacyRequest.contractVersion, 1);
  assert.equal(Object.hasOwn(legacyRequest, "launchIntent"), false);
  await assertAuthorityError(
    authority.finalizeRestoreDestinationGenerationAndReserveWriterLaunchAttempt(
      {
        launch: launchIntent,
        restore: {
          ...structuredClone(restore),
          request: legacyRequest,
        },
      },
    ),
    "invalid_operation_request",
  );
  assert.equal(pool.connectCalls, 0);
});

test("restore attachment activation requests bind one detached predecessor, generation, and launch", () => {
  const fixture = detachedRestoreActivationFixture();
  const request = createRestoreAttachmentActivationOperationRequest(fixture);
  const reordered = createRestoreAttachmentActivationOperationRequest(
    reversePlainData(fixture),
  );

  assert.equal(request.contractVersion, 1);
  assert.equal(JSON.stringify(request), JSON.stringify(reordered));
  assert.deepEqual(Reflect.ownKeys(request), [
    "contractVersion",
    "destinationRootPath",
    "generation",
    "holderId",
    "launchIntent",
    "leaseDurationMilliseconds",
    "predecessor",
  ]);
  assert.deepEqual(
    JSON.parse(JSON.stringify(request.predecessor)),
    fixture.predecessor,
  );
  assert.equal(request.generation.generationId, fixture.generation.generationId);
  assert.equal(request.generation.sessionId, fixture.expectedSession.sessionId);
  assert.equal(request.launchIntent.launchAttemptId, "writer-launch-001");
  assert.equal(Object.isFrozen(request), true);
  assert.equal(Object.isFrozen(request.generation), true);
  assert.equal(Object.isFrozen(request.launchIntent.measuredImage), true);
  assert.equal(Object.isFrozen(request.predecessor), true);

  fixture.predecessor.attachmentId = "mutated-attachment";
  fixture.generation.document.marker = "mutated-generation";
  fixture.launchIntent.supervisor.supervisorId = "mutated-supervisor";
  assert.equal(request.predecessor.attachmentId, "attachment-001");
  assert.equal(request.launchIntent.supervisor.supervisorId, "supervisor-001");
});

test("restore attachment activation V2 requests add one exact capture predecessor while preserving V1", () => {
  const fixture = detachedRestoreActivationFixture();
  const v1 = createRestoreAttachmentActivationOperationRequest(fixture);
  const v2Fixture = {
    ...fixture,
    predecessor: {
      ...fixture.predecessor,
      captureOperationId: "checkpoint-capture-operation-001",
    },
  };
  const request = createRestoreAttachmentActivationOperationRequestV2(
    v2Fixture,
  );
  const reordered = createRestoreAttachmentActivationOperationRequestV2(
    reversePlainData(v2Fixture),
  );

  assert.equal(v1.contractVersion, 1);
  assert.deepEqual(Reflect.ownKeys(v1.predecessor), [
    "attachmentId",
    "detachOperationId",
    "stopOperationId",
  ]);
  assert.equal(request.contractVersion, 2);
  assert.equal(JSON.stringify(request), JSON.stringify(reordered));
  assert.deepEqual(Reflect.ownKeys(request.predecessor), [
    "attachmentId",
    "captureOperationId",
    "detachOperationId",
    "stopOperationId",
  ]);
  assert.deepEqual(
    JSON.parse(JSON.stringify(request.predecessor)),
    v2Fixture.predecessor,
  );
  assert.equal(Object.isFrozen(request), true);
  assert.equal(Object.isFrozen(request.predecessor), true);

  for (const predecessor of [
    fixture.predecessor,
    { ...v2Fixture.predecessor, extra: true },
    {
      ...v2Fixture.predecessor,
      captureOperationId: v2Fixture.predecessor.stopOperationId,
    },
    {
      ...v2Fixture.predecessor,
      captureOperationId: v2Fixture.predecessor.detachOperationId,
    },
  ]) {
    assert.throws(
      () =>
        createRestoreAttachmentActivationOperationRequestV2({
          ...v2Fixture,
          predecessor,
        }),
      (error) =>
        error instanceof PostgresSessionAuthorityError &&
        error.code === "invalid_operation_request",
    );
  }
  assert.throws(
    () =>
      createRestoreAttachmentActivationOperationRequest({
        ...fixture,
        predecessor: v2Fixture.predecessor,
      }),
    (error) =>
      error instanceof PostgresSessionAuthorityError &&
      error.code === "invalid_operation_request",
  );
});

test("restore attachment activation rejects non-detached and ambiguous predecessor authority before PostgreSQL", async () => {
  const fixture = detachedRestoreActivationFixture();
  const invalidFixtures = [
    {
      ...fixture,
      expectedSession: attachedSnapshot(),
      launchIntent: writerLaunchIntentFixture(attachedSnapshot()),
    },
    {
      ...fixture,
      destinationRootPath: "/var/lib/portable-codex/restores/../escape",
    },
    {
      ...fixture,
      predecessor: {
        ...fixture.predecessor,
        detachOperationId: "writer-release-other",
      },
    },
    {
      ...fixture,
      expectedSession: {
        ...fixture.expectedSession,
        document: {
          ...fixture.expectedSession.document,
          lastOperation: {
            ...fixture.expectedSession.document.lastOperation,
            kind: RESTORE_DESTINATION_GENERATION_OPERATION_KIND,
          },
        },
      },
    },
  ];
  for (const candidate of invalidFixtures) {
    assert.throws(
      () => createRestoreAttachmentActivationOperationRequest(candidate),
      (error) =>
        error instanceof PostgresSessionAuthorityError &&
        error.code === "invalid_operation_request",
    );
  }

  const request = createRestoreAttachmentActivationOperationRequest(fixture);
  const { authority, pool } = authorityWithScripts();
  await assertAuthorityError(
    authority.reserveOperation({
      expectedSession: fixture.expectedSession,
      kind: RESTORE_ATTACHMENT_ACTIVATION_OPERATION_KIND,
      operationId: fixture.launchIntent.launchAttemptId,
      request,
    }),
    "invalid_operation_request",
  );
  assert.equal(pool.connectCalls, 0);
});

test("restore attachment activation rejects epoch exhaustion explicitly", async () => {
  const fixture = detachedRestoreActivationFixture();
  const request = createRestoreAttachmentActivationOperationRequest(fixture);
  const expectedSession = {
    ...fixture.expectedSession,
    document: {
      ...fixture.expectedSession.document,
      writerEpoch: "18446744073709551615",
    },
  };
  assert.throws(
    () =>
      createRestoreAttachmentActivationOperationRequest({
        ...fixture,
        expectedSession,
      }),
    (error) =>
      error instanceof PostgresSessionAuthorityError &&
      error.code === "invalid_operation_request",
  );

  const { authority, pool } = authorityWithScripts();
  await assertAuthorityError(
    authority.reserveOperation({
      expectedSession,
      kind: RESTORE_ATTACHMENT_ACTIVATION_OPERATION_KIND,
      operationId: "restore-activation-epoch-exhausted",
      request,
    }),
    "writer_epoch_exhausted",
  );
  assert.equal(pool.connectCalls, 0);
});
