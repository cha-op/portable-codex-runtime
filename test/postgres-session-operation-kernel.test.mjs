import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { EventEmitter } from "node:events";
import test from "node:test";

import {
  PostgresSerializableStore,
  PostgresSerializableStoreError,
} from "../src/postgres-serializable-store.mjs";
import {
  PostgresSessionAuthority,
  PostgresSessionAuthorityError,
  SESSION_AUTHORITY_DOCUMENT_VERSION,
  SESSION_OPERATION_CONFLICT_CLASS,
} from "../src/postgres-session-authority.mjs";
import {
  createSessionManifest,
  serializeSessionManifest,
} from "../src/session-storage-contracts.mjs";

const SESSION_ID = "019f2100-0000-7000-8000-000000000001";
const OTHER_SESSION_ID = "019f2100-0000-7000-8000-000000000002";
const OPERATION_ID = "operation-001";
const OTHER_OPERATION_ID = "operation-002";
const IMAGE_DIGEST = `sha256:${"a".repeat(64)}`;
const NOW = "2026-07-29T12:34:56.789Z";
const LATER = "2026-07-29T12:34:57.789Z";
const LATEST = "2026-07-29T12:34:58.789Z";
const FINAL = "2026-07-29T12:34:59.789Z";
const TRANSACTION_TIMESTAMP_QUERY =
  "SELECT pg_catalog.transaction_timestamp() AS transaction_timestamp, pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id";
const TRANSACTION_ID_QUERY =
  "SELECT pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id";
const DURABLE_COMMIT_QUERY = "SET LOCAL synchronous_commit = on";
const SESSION_COLUMNS =
  "session_id, revision, document, created_at, updated_at";
const READ_SESSION_QUERY = [
  `SELECT ${SESSION_COLUMNS}`,
  "FROM session_authority.sessions",
  "WHERE session_id = $1::uuid",
].join(" ");
const OPERATION_COLUMNS = [
  "operation_id",
  "session_id",
  "kind",
  "request",
  "result",
  "state",
  "revision",
  "created_at",
  "updated_at",
  "retired_at",
].join(", ");
const RESERVATION_COLUMNS = [
  "reservation_id",
  "operation_id",
  "session_id",
  "kind",
  "expected_session_revision",
  "state",
  "payload",
  "created_at",
  "updated_at",
  "expires_at",
  "released_at",
].join(", ");
const READ_OPERATION_QUERY = [
  `SELECT ${OPERATION_COLUMNS}`,
  "FROM session_authority.operation_claims",
  "WHERE operation_id = $1",
].join(" ");
const READ_RESERVATION_QUERY = [
  `SELECT ${RESERVATION_COLUMNS}`,
  "FROM session_authority.reservations",
  "WHERE operation_id = $1",
].join(" ");
const READ_ACTIVE_COUNTS_QUERY = [
  "SELECT",
  "(SELECT count(*)::integer",
  "FROM session_authority.operation_claims",
  "WHERE session_id = $1::uuid AND retired_at IS NULL)",
  "AS operation_count,",
  "(SELECT count(*)::integer",
  "FROM session_authority.reservations",
  "WHERE session_id = $1::uuid AND released_at IS NULL)",
  "AS reservation_count",
].join(" ");
const INSERT_OPERATION_QUERY = [
  "INSERT INTO session_authority.operation_claims",
  "(operation_id, session_id, kind, request, result, state, revision,",
  "created_at, updated_at, retired_at)",
  "VALUES ($1, $2::uuid, $3, $4::jsonb, NULL, 'prepared', 0, $5, $5, NULL)",
  "ON CONFLICT (operation_id) DO NOTHING",
  `RETURNING ${OPERATION_COLUMNS}`,
].join(" ");
const INSERT_RESERVATION_QUERY = [
  "INSERT INTO session_authority.reservations",
  "(reservation_id, operation_id, session_id, kind,",
  "expected_session_revision, state, payload, created_at, updated_at,",
  "expires_at, released_at)",
  "VALUES ($1, $2, $3::uuid, $4, $5::bigint, 'prepared',",
  "$6::jsonb, $7, $7, NULL, NULL)",
  "ON CONFLICT (reservation_id) DO NOTHING",
  `RETURNING ${RESERVATION_COLUMNS}`,
].join(" ");
const UPDATE_SESSION_QUERY = [
  "UPDATE session_authority.sessions",
  "SET revision = revision + 1, document = $3::jsonb, updated_at = $4",
  "WHERE session_id = $1::uuid AND revision = $2::bigint",
  `RETURNING ${SESSION_COLUMNS}`,
].join(" ");
const START_OPERATION_QUERY = [
  "UPDATE session_authority.operation_claims",
  "SET state = 'starting', revision = revision + 1, updated_at = $3",
  "WHERE operation_id = $1 AND revision = $2::bigint",
  "AND state = 'prepared' AND retired_at IS NULL",
  `RETURNING ${OPERATION_COLUMNS}`,
].join(" ");
const START_RESERVATION_QUERY = [
  "UPDATE session_authority.reservations",
  "SET state = 'starting', updated_at = $2",
  "WHERE operation_id = $1 AND state = 'prepared' AND released_at IS NULL",
  `RETURNING ${RESERVATION_COLUMNS}`,
].join(" ");
const UNCERTAIN_OPERATION_QUERY = [
  "UPDATE session_authority.operation_claims",
  "SET state = 'uncertain', revision = revision + 1, updated_at = $3",
  "WHERE operation_id = $1 AND revision = $2::bigint",
  "AND state = 'starting' AND retired_at IS NULL",
  `RETURNING ${OPERATION_COLUMNS}`,
].join(" ");
const UNCERTAIN_RESERVATION_QUERY = [
  "UPDATE session_authority.reservations",
  "SET state = 'uncertain', updated_at = $2",
  "WHERE operation_id = $1 AND state = 'starting' AND released_at IS NULL",
  `RETURNING ${RESERVATION_COLUMNS}`,
].join(" ");
const CANCEL_OPERATION_QUERY = [
  "UPDATE session_authority.operation_claims",
  "SET state = 'committed', result = $3::jsonb,",
  "revision = revision + 1, updated_at = $4, retired_at = $4",
  "WHERE operation_id = $1 AND revision = $2::bigint",
  "AND state = 'prepared' AND retired_at IS NULL",
  `RETURNING ${OPERATION_COLUMNS}`,
].join(" ");
const RELEASE_RESERVATION_QUERY = [
  "UPDATE session_authority.reservations",
  "SET state = 'released', updated_at = $2, released_at = $2",
  "WHERE operation_id = $1 AND state = 'prepared' AND released_at IS NULL",
  `RETURNING ${RESERVATION_COLUMNS}`,
].join(" ");
const TRANSACTION_INFRASTRUCTURE_QUERIES = new Set([
  "DISCARD ALL",
  "BEGIN ISOLATION LEVEL SERIALIZABLE READ WRITE",
  TRANSACTION_TIMESTAMP_QUERY,
  TRANSACTION_ID_QUERY,
  DURABLE_COMMIT_QUERY,
  "COMMIT",
  "ROLLBACK",
]);

function queryText(args) {
  return typeof args[0] === "string" ? args[0] : args[0]?.text;
}

class ScriptedClient {
  constructor(
    userSteps,
    {
      commitError,
      commitResult = { command: "COMMIT" },
      now = NOW,
      transactionId = "100",
    } = {},
  ) {
    this.commitError = commitError;
    this.commitResult = commitResult;
    this.connection = new EventEmitter();
    this.now = now;
    this.queries = [];
    this.releaseCalls = [];
    this.transactionId = transactionId;
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
            transaction_id: this.transactionId,
            transaction_timestamp: new Date(this.now),
          },
        ],
      };
    }
    if (text === TRANSACTION_ID_QUERY) {
      return { rows: [{ transaction_id: this.transactionId }] };
    }
    if (text === DURABLE_COMMIT_QUERY) return { command: "SET" };
    if (text === "COMMIT") {
      if (this.commitError !== undefined) throw this.commitError;
      return this.commitResult;
    }
    if (text === "ROLLBACK") return { command: "ROLLBACK" };
    assert.notEqual(
      this.userSteps.length,
      0,
      `unexpected authority query: ${text}`,
    );
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

function registration(sessionId = SESSION_ID) {
  return {
    manifest: manifest(sessionId),
    storageRef: storageRef(sessionId),
    backendCapabilities: backendCapabilities(),
  };
}

function document(sessionId = SESSION_ID, overrides = {}) {
  return {
    documentVersion: SESSION_AUTHORITY_DOCUMENT_VERSION,
    manifest: JSON.parse(serializeSessionManifest(manifest(sessionId))),
    storageRef: storageRef(sessionId),
    backendCapabilities: backendCapabilities(),
    lifecycle: "DETACHED",
    writerEpoch: "0",
    lease: null,
    attachment: null,
    activeOperation: null,
    recovery: null,
    launch: null,
    ...overrides,
  };
}

function sessionRow({
  sessionId = SESSION_ID,
  revision = "0",
  sessionDocument = document(sessionId),
  createdAt = NOW,
  updatedAt = NOW,
} = {}) {
  return {
    session_id: sessionId,
    revision,
    document: structuredClone(sessionDocument),
    created_at: new Date(createdAt),
    updated_at: new Date(updatedAt),
  };
}

function sessionSnapshot({
  sessionId = SESSION_ID,
  revision = "0",
  sessionDocument = document(sessionId),
  createdAt = NOW,
  updatedAt = NOW,
} = {}) {
  return {
    sessionId,
    revision,
    document: structuredClone(sessionDocument),
    createdAt,
    updatedAt,
  };
}

function operationRequest(overrides = {}) {
  return {
    checkpointId: "checkpoint-001",
    labels: ["daily", "manual"],
    metadata: {
      note: "deterministic request",
      sequence: 7,
    },
    ...overrides,
  };
}

function reserveOptions(overrides = {}) {
  return {
    expectedSession: sessionSnapshot(),
    operationId: OPERATION_ID,
    kind: "checkpoint-create",
    request: operationRequest(),
    ...overrides,
  };
}

function sha256(value) {
  return createHash("sha256").update(value, "utf8").digest("hex");
}

function canonicalPayload(value) {
  if (Array.isArray(value)) {
    const result = new Array(value.length);
    for (let index = 0; index < value.length; index += 1) {
      result[index] = canonicalPayload(value[index]);
    }
    Object.setPrototypeOf(result, null);
    return result;
  }
  if (value === null || typeof value !== "object") {
    return Object.is(value, -0) ? 0 : value;
  }
  const result = Object.create(null);
  for (const key of Object.keys(value).sort()) {
    result[key] = canonicalPayload(value[key]);
  }
  return result;
}

function operationEnvelope(options = reserveOptions()) {
  return {
    requestVersion: 1,
    conflictClass: SESSION_OPERATION_CONFLICT_CLASS,
    expectedSession: structuredClone(options.expectedSession),
    payload: canonicalPayload(options.request),
  };
}

function operationBinding(options = reserveOptions()) {
  const envelope = operationEnvelope(options);
  const serializedEnvelope = JSON.stringify(envelope);
  return {
    envelope,
    requestSha256: sha256(serializedEnvelope),
    reservationId: `reservation-${sha256(options.operationId)}`,
    serializedEnvelope,
  };
}

function activeOperation(
  state,
  {
    options = reserveOptions(),
    operationRevision =
      state === "prepared" ? "0" : state === "starting" ? "1" : "2",
  } = {},
) {
  const binding = operationBinding(options);
  return {
    conflictClass: SESSION_OPERATION_CONFLICT_CLASS,
    expectedSessionRevision: options.expectedSession.revision,
    kind: options.kind,
    operationId: options.operationId,
    operationRevision,
    requestSha256: binding.requestSha256,
    reservationId: binding.reservationId,
    state,
  };
}

function operationRow(
  state = "prepared",
  {
    options = reserveOptions(),
    revision =
      state === "prepared"
        ? "0"
        : state === "starting" || state === "committed"
          ? "1"
          : "2",
    createdAt = LATER,
    updatedAt =
      state === "prepared"
        ? LATER
        : state === "starting" || state === "committed"
          ? LATEST
          : FINAL,
    result = null,
    retiredAt = null,
  } = {},
) {
  return {
    operation_id: options.operationId,
    session_id: options.expectedSession.sessionId,
    kind: options.kind,
    request: operationEnvelope(options),
    result: structuredClone(result),
    state,
    revision,
    created_at: new Date(createdAt),
    updated_at: new Date(updatedAt),
    retired_at: retiredAt === null ? null : new Date(retiredAt),
  };
}

function reservationRow(
  state = "prepared",
  {
    options = reserveOptions(),
    createdAt = LATER,
    updatedAt =
      state === "prepared"
        ? LATER
        : state === "starting" || state === "released"
          ? LATEST
          : FINAL,
    releasedAt = state === "released" ? updatedAt : null,
  } = {},
) {
  const binding = operationBinding(options);
  return {
    reservation_id: binding.reservationId,
    operation_id: options.operationId,
    session_id: options.expectedSession.sessionId,
    kind: options.kind,
    expected_session_revision: options.expectedSession.revision,
    state,
    payload: {
      reservationVersion: 1,
      conflictClass: SESSION_OPERATION_CONFLICT_CLASS,
      requestSha256: binding.requestSha256,
    },
    created_at: new Date(createdAt),
    updated_at: new Date(updatedAt),
    expires_at: null,
    released_at: releasedAt === null ? null : new Date(releasedAt),
  };
}

function cancellationResult(reason = "caller-abandoned-before-dispatch") {
  return {
    resultVersion: 1,
    outcome: "cancelled-before-dispatch",
    reason,
  };
}

function phaseSessionRow(
  state,
  {
    options = reserveOptions(),
    revision =
      state === "prepared" ? "1" : state === "starting" ? "2" : "3",
    updatedAt =
      state === "prepared" ? LATER : state === "starting" ? LATEST : FINAL,
    active = activeOperation(state, { options }),
  } = {},
) {
  return sessionRow({
    sessionId: options.expectedSession.sessionId,
    revision,
    sessionDocument: document(options.expectedSession.sessionId, {
      activeOperation: active,
    }),
    createdAt: options.expectedSession.createdAt,
    updatedAt,
  });
}

function snapshotFromSessionRow(value) {
  return {
    sessionId: value.session_id,
    revision: value.revision,
    document: structuredClone(value.document),
    createdAt: value.created_at.toISOString(),
    updatedAt: value.updated_at.toISOString(),
  };
}

function operationView(value) {
  const envelope = value.request;
  return {
    operationId: value.operation_id,
    sessionId: value.session_id,
    kind: value.kind,
    conflictClass: SESSION_OPERATION_CONFLICT_CLASS,
    expectedSession: structuredClone(envelope.expectedSession),
    request: canonicalPayload(envelope.payload),
    requestSha256: sha256(JSON.stringify(envelope)),
    state: value.state,
    revision: value.revision,
    result: structuredClone(value.result),
    createdAt: value.created_at.toISOString(),
    updatedAt: value.updated_at.toISOString(),
    retiredAt:
      value.retired_at === null ? null : value.retired_at.toISOString(),
  };
}

function reservationView(value) {
  return {
    reservationId: value.reservation_id,
    operationId: value.operation_id,
    sessionId: value.session_id,
    kind: value.kind,
    expectedSessionRevision: value.expected_session_revision,
    state: value.state,
    conflictClass: SESSION_OPERATION_CONFLICT_CLASS,
    requestSha256: value.payload.requestSha256,
    createdAt: value.created_at.toISOString(),
    updatedAt: value.updated_at.toISOString(),
    expiresAt:
      value.expires_at === null ? null : value.expires_at.toISOString(),
    releasedAt:
      value.released_at === null ? null : value.released_at.toISOString(),
  };
}

function operationReceipt({
  status,
  session,
  operation = null,
  reservation = null,
  ...flags
}) {
  return {
    status,
    session,
    operation,
    reservation,
    ...flags,
  };
}

function rows(...values) {
  return { rows: values };
}

function extendedQuery(text, values) {
  return [
    {
      queryMode: "extended",
      text,
      values,
    },
  ];
}

function activePhaseSteps(state, options = reserveOptions()) {
  return [
    rows(phaseSessionRow(state, { options })),
    rows(operationRow(state, { options })),
    rows(reservationRow(state, { options })),
  ];
}

function cancelledSessionRow({
  options = reserveOptions(),
  updatedAt = LATEST,
} = {}) {
  return sessionRow({
    sessionId: options.expectedSession.sessionId,
    revision: "2",
    sessionDocument: document(options.expectedSession.sessionId),
    createdAt: options.expectedSession.createdAt,
    updatedAt,
  });
}

function committedOperationRow({
  options = reserveOptions(),
  reason = "caller-abandoned-before-dispatch",
  updatedAt = LATEST,
} = {}) {
  return operationRow("committed", {
    options,
    result: cancellationResult(reason),
    retiredAt: updatedAt,
    updatedAt,
  });
}

function releasedReservationRow({
  options = reserveOptions(),
  updatedAt = LATEST,
} = {}) {
  return reservationRow("released", {
    options,
    releasedAt: updatedAt,
    updatedAt,
  });
}

function authorityWithScripts(...scripts) {
  const clients = scripts.map((script) => {
    if (Array.isArray(script)) return new ScriptedClient(script);
    return new ScriptedClient(script.steps, script.options);
  });
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

function authorityQueries(client) {
  return client.queries.filter(
    (args) => !TRANSACTION_INFRASTRUCTURE_QUERIES.has(queryText(args)),
  );
}

function queryTexts(client) {
  return client.queries.map(queryText);
}

function assertDeepFrozen(value) {
  if (value === null || typeof value !== "object") return;
  assert.equal(Object.isFrozen(value), true);
  for (const key of Reflect.ownKeys(value)) {
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    if (descriptor && Object.hasOwn(descriptor, "value")) {
      assertDeepFrozen(descriptor.value);
    }
  }
}

async function assertAuthorityError(promise, expected) {
  await assert.rejects(promise, (error) => {
    assert.ok(error instanceof PostgresSessionAuthorityError);
    assert.equal(error.name, "PostgresSessionAuthorityError");
    assert.equal(error.code, expected.code);
    assert.equal(error.retryable, false);
    assert.equal(Object.isFrozen(error), true);
    assert.equal(Object.hasOwn(error, "cause"), false);
    assert.equal("cause" in error, false);
    if (expected.conflictClass === undefined) {
      assert.equal(Object.hasOwn(error, "conflictClass"), false);
    } else {
      assert.equal(error.conflictClass, expected.conflictClass);
    }
    if (expected.omittedText !== undefined) {
      assert.equal(error.message.includes(expected.omittedText), false);
      assert.equal(String(error.stack).includes(expected.omittedText), false);
    }
    return true;
  });
}

function assertStoreCommitUncertain(error) {
  assert.ok(error instanceof PostgresSerializableStoreError);
  assert.equal(error.name, "PostgresSerializableStoreError");
  assert.equal(error.code, "transaction_commit_outcome_uncertain");
  assert.equal(error.commitState, "uncertain");
  assert.equal(error.retryable, false);
  assert.equal(Object.isFrozen(error), true);
  assert.equal("cause" in error, false);
  return true;
}

test("operation conflict class is the stable session mutation class", () => {
  assert.equal(SESSION_OPERATION_CONFLICT_CLASS, "session-mutation");
});

test("reserveOperation atomically persists the canonical prepared claim", async () => {
  const options = reserveOptions();
  const preparedOperation = operationRow("prepared", { options });
  const preparedReservation = reservationRow("prepared", { options });
  const preparedSession = phaseSessionRow("prepared", { options });
  const { authority, clients } = authorityWithScripts({
    options: { now: LATER },
    steps: [
      rows(sessionRow()),
      rows({ operation_count: 0, reservation_count: 0 }),
      rows(),
      rows(preparedOperation),
      rows(preparedReservation),
      rows(preparedSession),
    ],
  });

  const receipt = await authority.reserveOperation(options);

  assert.deepEqual(
    receipt,
    operationReceipt({
      status: "prepared",
      session: snapshotFromSessionRow(preparedSession),
      operation: operationView(preparedOperation),
      reservation: reservationView(preparedReservation),
      acquired: true,
    }),
  );
  assertDeepFrozen(receipt);

  const binding = operationBinding(options);
  const nextDocument = document(SESSION_ID, {
    activeOperation: activeOperation("prepared", { options }),
  });
  assert.deepEqual(authorityQueries(clients[0]), [
    extendedQuery(`${READ_SESSION_QUERY} FOR UPDATE`, [SESSION_ID]),
    extendedQuery(READ_ACTIVE_COUNTS_QUERY, [SESSION_ID]),
    extendedQuery(READ_OPERATION_QUERY, [OPERATION_ID]),
    extendedQuery(INSERT_OPERATION_QUERY, [
      OPERATION_ID,
      SESSION_ID,
      options.kind,
      binding.serializedEnvelope,
      LATER,
    ]),
    extendedQuery(INSERT_RESERVATION_QUERY, [
      binding.reservationId,
      OPERATION_ID,
      SESSION_ID,
      options.kind,
      "0",
      JSON.stringify(preparedReservation.payload),
      LATER,
    ]),
    extendedQuery(UPDATE_SESSION_QUERY, [
      SESSION_ID,
      "0",
      JSON.stringify(nextDocument),
      LATER,
    ]),
  ]);
  const authorityTexts = authorityQueries(clients[0]).map(queryText);
  assert.deepEqual(queryTexts(clients[0]), [
    "DISCARD ALL",
    "BEGIN ISOLATION LEVEL SERIALIZABLE READ WRITE",
    TRANSACTION_TIMESTAMP_QUERY,
    ...authorityTexts.flatMap((text) => [text, TRANSACTION_ID_QUERY]),
    DURABLE_COMMIT_QUERY,
    TRANSACTION_ID_QUERY,
    "COMMIT",
    "DISCARD ALL",
  ]);
  clients[0].assertExhausted();
});

test("exact reserve and reconcile replay prepared state without rewriting it", async () => {
  const options = reserveOptions();
  const preparedOperation = operationRow("prepared", { options });
  const preparedReservation = reservationRow("prepared", { options });
  const preparedSession = phaseSessionRow("prepared", { options });
  const { authority, clients } = authorityWithScripts(
    activePhaseSteps("prepared", options),
    activePhaseSteps("prepared", options),
  );

  const reserveReplay = await authority.reserveOperation(options);
  const reconciled = await authority.reconcileOperation(options);

  const stableReceipt = {
    status: "prepared",
    session: snapshotFromSessionRow(preparedSession),
    operation: operationView(preparedOperation),
    reservation: reservationView(preparedReservation),
  };
  assert.deepEqual(reserveReplay, {
    ...stableReceipt,
    acquired: false,
  });
  assert.deepEqual(reconciled, stableReceipt);
  assertDeepFrozen(reserveReplay);
  assertDeepFrozen(reconciled);
  assert.deepEqual(authorityQueries(clients[0]), [
    extendedQuery(`${READ_SESSION_QUERY} FOR UPDATE`, [SESSION_ID]),
    extendedQuery(`${READ_OPERATION_QUERY} FOR UPDATE`, [OPERATION_ID]),
    extendedQuery(`${READ_RESERVATION_QUERY} FOR UPDATE`, [OPERATION_ID]),
  ]);
  assert.deepEqual(authorityQueries(clients[1]), [
    extendedQuery(READ_SESSION_QUERY, [SESSION_ID]),
    extendedQuery(READ_OPERATION_QUERY, [OPERATION_ID]),
    extendedQuery(READ_RESERVATION_QUERY, [OPERATION_ID]),
  ]);
  assert.equal(
    [...authorityQueries(clients[0]), ...authorityQueries(clients[1])].some(
      (args) => queryText(args).startsWith("UPDATE "),
    ),
    false,
  );
  clients[0].assertExhausted();
  clients[1].assertExhausted();
});

test("operation IDs reject reuse across session, kind, or canonical request", async () => {
  const original = reserveOptions();
  const otherSession = reserveOptions({
    expectedSession: sessionSnapshot({
      sessionId: OTHER_SESSION_ID,
      sessionDocument: document(OTHER_SESSION_ID),
    }),
  });
  const otherKind = reserveOptions({ kind: "checkpoint-delete" });
  const otherRequest = reserveOptions({
    request: operationRequest({
      metadata: {
        note: "sensitive-reuse-sentinel",
        sequence: 8,
      },
    }),
  });
  const { authority, clients } = authorityWithScripts(
    [
      rows(
        sessionRow({
          sessionId: OTHER_SESSION_ID,
          sessionDocument: document(OTHER_SESSION_ID),
        }),
      ),
      rows({ operation_count: 0, reservation_count: 0 }),
      rows(operationRow("prepared", { options: original })),
    ],
    activePhaseSteps("prepared", original),
    activePhaseSteps("prepared", original),
  );

  for (const candidate of [otherSession, otherKind, otherRequest]) {
    await assertAuthorityError(authority.reserveOperation(candidate), {
      code: "operation_identity_conflict",
      omittedText: "sensitive-reuse-sentinel",
    });
  }
  for (const client of clients) {
    assert.equal(
      authorityQueries(client).some((args) =>
        /^(?:INSERT|UPDATE) /u.test(queryText(args)),
      ),
      false,
    );
    client.assertExhausted();
  }
});

test("a different operation on the same session is blocked without mutation", async () => {
  const original = reserveOptions();
  const conflicting = reserveOptions({ operationId: OTHER_OPERATION_ID });
  const { authority, clients } = authorityWithScripts([
    ...activePhaseSteps("prepared", original),
    rows(),
  ]);

  await assertAuthorityError(authority.reserveOperation(conflicting), {
    code: "session_operation_conflict",
  });

  assert.deepEqual(authorityQueries(clients[0]), [
    extendedQuery(`${READ_SESSION_QUERY} FOR UPDATE`, [SESSION_ID]),
    extendedQuery(`${READ_OPERATION_QUERY} FOR UPDATE`, [OPERATION_ID]),
    extendedQuery(`${READ_RESERVATION_QUERY} FOR UPDATE`, [OPERATION_ID]),
    extendedQuery(READ_OPERATION_QUERY, [OTHER_OPERATION_ID]),
  ]);
  clients[0].assertExhausted();
});

test("a stale expected session snapshot fails before any claim is inserted", async () => {
  const stale = reserveOptions();
  const current = sessionRow({
    revision: "1",
    updatedAt: LATER,
  });
  const { authority, clients } = authorityWithScripts([
    rows(current),
    rows({ operation_count: 0, reservation_count: 0 }),
    rows(),
  ]);

  await assertAuthorityError(authority.reserveOperation(stale), {
    code: "session_revision_conflict",
  });

  assert.equal(
    authorityQueries(clients[0]).some((args) =>
      /^(?:INSERT|UPDATE) /u.test(queryText(args)),
    ),
    false,
  );
  clients[0].assertExhausted();
});

test("dispatch claim grants exactly once and starting replay grants false", async () => {
  const options = reserveOptions();
  const startingOperation = operationRow("starting", { options });
  const startingReservation = reservationRow("starting", { options });
  const startingSession = phaseSessionRow("starting", { options });
  const input = {
    ...options,
    expectedOperationRevision: "0",
  };
  const { authority, clients } = authorityWithScripts(
    {
      options: { now: LATEST },
      steps: [
        ...activePhaseSteps("prepared", options),
        rows(startingOperation),
        rows(startingReservation),
        rows(startingSession),
      ],
    },
    activePhaseSteps("starting", options),
  );

  const granted = await authority.claimOperationDispatch(input);
  const replay = await authority.claimOperationDispatch(input);

  const stable = {
    status: "starting",
    session: snapshotFromSessionRow(startingSession),
    operation: operationView(startingOperation),
    reservation: reservationView(startingReservation),
  };
  assert.deepEqual(granted, {
    ...stable,
    dispatchGranted: true,
  });
  assert.deepEqual(replay, {
    ...stable,
    dispatchGranted: false,
  });
  assertDeepFrozen(granted);
  assertDeepFrozen(replay);
  assert.deepEqual(authorityQueries(clients[0]), [
    extendedQuery(`${READ_SESSION_QUERY} FOR UPDATE`, [SESSION_ID]),
    extendedQuery(`${READ_OPERATION_QUERY} FOR UPDATE`, [OPERATION_ID]),
    extendedQuery(`${READ_RESERVATION_QUERY} FOR UPDATE`, [OPERATION_ID]),
    extendedQuery(START_OPERATION_QUERY, [OPERATION_ID, "0", LATEST]),
    extendedQuery(START_RESERVATION_QUERY, [OPERATION_ID, LATEST]),
    extendedQuery(UPDATE_SESSION_QUERY, [
      SESSION_ID,
      "1",
      JSON.stringify(
        document(SESSION_ID, {
          activeOperation: activeOperation("starting", { options }),
        }),
      ),
      LATEST,
    ]),
  ]);
  assert.equal(authorityQueries(clients[1]).length, 3);
  assert.equal(
    authorityQueries(clients[1]).some((args) =>
      queryText(args).startsWith("UPDATE "),
    ),
    false,
  );
  clients[0].assertExhausted();
  clients[1].assertExhausted();
});

test("dispatch commit acknowledgement loss never returns or regrants dispatch", async () => {
  const options = reserveOptions();
  const startingOperation = operationRow("starting", { options });
  const startingReservation = reservationRow("starting", { options });
  const startingSession = phaseSessionRow("starting", { options });
  const input = {
    ...options,
    expectedOperationRevision: "0",
  };
  const commitError = new Error("sensitive dispatch commit acknowledgement lost");
  const { authority, clients, pool } = authorityWithScripts(
    {
      options: { commitError, now: LATEST },
      steps: [
        ...activePhaseSteps("prepared", options),
        rows(startingOperation),
        rows(startingReservation),
        rows(startingSession),
      ],
    },
    activePhaseSteps("starting", options),
    activePhaseSteps("starting", options),
  );

  await assert.rejects(
    authority.claimOperationDispatch(input),
    assertStoreCommitUncertain,
  );
  const reconciled = await authority.reconcileOperation(options);
  const replay = await authority.claimOperationDispatch(input);

  const stable = {
    status: "starting",
    session: snapshotFromSessionRow(startingSession),
    operation: operationView(startingOperation),
    reservation: reservationView(startingReservation),
  };
  assert.deepEqual(reconciled, stable);
  assert.deepEqual(replay, {
    ...stable,
    dispatchGranted: false,
  });
  assertDeepFrozen(reconciled);
  assertDeepFrozen(replay);
  assert.equal(pool.connectCalls, 3);
  assert.equal(
    queryTexts(clients[0]).filter((text) => text === "COMMIT").length,
    1,
  );
  assert.equal(queryTexts(clients[0]).at(-1), "ROLLBACK");
  assert.equal(
    authorityQueries(clients[1]).some((args) =>
      /^(?:INSERT|UPDATE) /u.test(queryText(args)),
    ),
    false,
  );
  assert.equal(
    authorityQueries(clients[2]).some((args) =>
      /^(?:INSERT|UPDATE) /u.test(queryText(args)),
    ),
    false,
  );
  clients[0].assertExhausted({ destroyed: true });
  clients[1].assertExhausted();
  clients[2].assertExhausted();
});

test("mark uncertain replays exactly and retains the session blocker", async () => {
  const options = reserveOptions();
  const uncertainOperation = operationRow("uncertain", { options });
  const uncertainReservation = reservationRow("uncertain", { options });
  const uncertainSession = phaseSessionRow("uncertain", { options });
  const input = {
    ...options,
    expectedOperationRevision: "1",
  };
  const { authority, clients } = authorityWithScripts(
    {
      options: { now: FINAL },
      steps: [
        ...activePhaseSteps("starting", options),
        rows(uncertainOperation),
        rows(uncertainReservation),
        rows(uncertainSession),
      ],
    },
    activePhaseSteps("uncertain", options),
    [
      ...activePhaseSteps("uncertain", options),
      rows(),
    ],
  );

  const changed = await authority.markOperationUncertain(input);
  const replay = await authority.markOperationUncertain(input);
  await assertAuthorityError(
    authority.reserveOperation(
      reserveOptions({ operationId: OTHER_OPERATION_ID }),
    ),
    { code: "session_operation_conflict" },
  );

  const stable = {
    status: "uncertain",
    session: snapshotFromSessionRow(uncertainSession),
    operation: operationView(uncertainOperation),
    reservation: reservationView(uncertainReservation),
  };
  assert.deepEqual(changed, { ...stable, changed: true });
  assert.deepEqual(replay, { ...stable, changed: false });
  assert.deepEqual(authorityQueries(clients[0]).slice(3), [
    extendedQuery(UNCERTAIN_OPERATION_QUERY, [
      OPERATION_ID,
      "1",
      FINAL,
    ]),
    extendedQuery(UNCERTAIN_RESERVATION_QUERY, [OPERATION_ID, FINAL]),
    extendedQuery(UPDATE_SESSION_QUERY, [
      SESSION_ID,
      "2",
      JSON.stringify(
        document(SESSION_ID, {
          activeOperation: activeOperation("uncertain", { options }),
        }),
      ),
      FINAL,
    ]),
  ]);
  assert.equal(authorityQueries(clients[1]).length, 3);
  assert.equal(authorityQueries(clients[2]).length, 4);
  for (const client of clients) client.assertExhausted();
});

test("prepared cancellation commits once and exact terminal replay is stable", async () => {
  const options = reserveOptions();
  const reason = "caller-abandoned-before-dispatch";
  const committedOperation = committedOperationRow({ options, reason });
  const releasedReservation = releasedReservationRow({ options });
  const releasedSession = cancelledSessionRow({ options });
  const input = {
    ...options,
    expectedOperationRevision: "0",
    reason,
  };
  const terminalSteps = [
    rows(releasedSession),
    rows({ operation_count: 0, reservation_count: 0 }),
    rows(committedOperation),
    rows(releasedReservation),
  ];
  const { authority, clients } = authorityWithScripts(
    {
      options: { now: LATEST },
      steps: [
        ...activePhaseSteps("prepared", options),
        rows(committedOperation),
        rows(releasedReservation),
        rows(releasedSession),
      ],
    },
    terminalSteps,
    terminalSteps,
  );

  const cancelled = await authority.cancelPreparedOperation(input);
  const replay = await authority.cancelPreparedOperation(input);
  await assertAuthorityError(
    authority.cancelPreparedOperation({
      ...input,
      reason: "different-cancellation-reason",
    }),
    { code: "operation_result_conflict" },
  );

  const stable = {
    status: "committed",
    session: snapshotFromSessionRow(releasedSession),
    operation: operationView(committedOperation),
    reservation: reservationView(releasedReservation),
  };
  assert.deepEqual(cancelled, { ...stable, cancelled: true });
  assert.deepEqual(replay, { ...stable, cancelled: false });
  assertDeepFrozen(cancelled);
  assertDeepFrozen(replay);
  assert.deepEqual(authorityQueries(clients[0]).slice(3), [
    extendedQuery(CANCEL_OPERATION_QUERY, [
      OPERATION_ID,
      "0",
      JSON.stringify(cancellationResult(reason)),
      LATEST,
    ]),
    extendedQuery(RELEASE_RESERVATION_QUERY, [OPERATION_ID, LATEST]),
    extendedQuery(UPDATE_SESSION_QUERY, [
      SESSION_ID,
      "1",
      JSON.stringify(document()),
      LATEST,
    ]),
  ]);
  for (const client of clients.slice(1)) {
    assert.equal(
      authorityQueries(client).some((args) =>
        queryText(args).startsWith("UPDATE "),
      ),
      false,
    );
  }
  for (const client of clients) client.assertExhausted();
});

test("starting and uncertain operations cannot use prepared cancellation", async () => {
  const options = reserveOptions();
  const input = {
    ...options,
    expectedOperationRevision: "0",
    reason: "caller-abandoned-before-dispatch",
  };
  const { authority, clients } = authorityWithScripts(
    activePhaseSteps("starting", options),
    activePhaseSteps("uncertain", options),
  );

  await assertAuthorityError(authority.cancelPreparedOperation(input), {
    code: "operation_transition_conflict",
  });
  await assertAuthorityError(authority.cancelPreparedOperation(input), {
    code: "operation_transition_conflict",
  });

  for (const client of clients) {
    assert.equal(authorityQueries(client).length, 3);
    assert.equal(
      authorityQueries(client).some((args) =>
        queryText(args).startsWith("UPDATE "),
      ),
      false,
    );
    client.assertExhausted();
  }
});

test("reconcile reports an exact absent operation without creating state", async () => {
  const options = reserveOptions();
  const initialSession = sessionRow();
  const { authority, clients } = authorityWithScripts([
    rows(initialSession),
    rows({ operation_count: 0, reservation_count: 0 }),
    rows(),
  ]);

  const receipt = await authority.reconcileOperation(options);

  assert.deepEqual(
    receipt,
    operationReceipt({
      status: "absent",
      session: snapshotFromSessionRow(initialSession),
      operation: null,
      reservation: null,
    }),
  );
  assertDeepFrozen(receipt);
  assert.deepEqual(authorityQueries(clients[0]), [
    extendedQuery(READ_SESSION_QUERY, [SESSION_ID]),
    extendedQuery(READ_ACTIVE_COUNTS_QUERY, [SESSION_ID]),
    extendedQuery(READ_OPERATION_QUERY, [OPERATION_ID]),
  ]);
  clients[0].assertExhausted();
});

test("reserve commit uncertainty propagates and exact reconcile finds prepared", async () => {
  const options = reserveOptions();
  const preparedOperation = operationRow("prepared", { options });
  const preparedReservation = reservationRow("prepared", { options });
  const preparedSession = phaseSessionRow("prepared", { options });
  const commitError = new Error("sensitive commit acknowledgement lost");
  const { authority, clients, pool } = authorityWithScripts(
    {
      options: { commitError, now: LATER },
      steps: [
        rows(sessionRow()),
        rows({ operation_count: 0, reservation_count: 0 }),
        rows(),
        rows(preparedOperation),
        rows(preparedReservation),
        rows(preparedSession),
      ],
    },
    activePhaseSteps("prepared", options),
  );

  await assert.rejects(
    authority.reserveOperation(options),
    assertStoreCommitUncertain,
  );
  const reconciled = await authority.reconcileOperation(options);

  assert.deepEqual(reconciled, {
    status: "prepared",
    session: snapshotFromSessionRow(preparedSession),
    operation: operationView(preparedOperation),
    reservation: reservationView(preparedReservation),
  });
  assert.equal(pool.connectCalls, 2);
  assert.equal(
    queryTexts(clients[0]).filter((text) => text === "COMMIT").length,
    1,
  );
  assert.equal(queryTexts(clients[0]).at(-1), "ROLLBACK");
  assert.equal(
    authorityQueries(clients[1]).some((args) =>
      /^(?:INSERT|UPDATE) /u.test(queryText(args)),
    ),
    false,
  );
  clients[0].assertExhausted({ destroyed: true });
  clients[1].assertExhausted();
});

test("invisible INSERT conflict retries in a fresh transaction to exact replay", async () => {
  const options = reserveOptions();
  const preparedOperation = operationRow("prepared", { options });
  const preparedReservation = reservationRow("prepared", { options });
  const preparedSession = phaseSessionRow("prepared", { options });
  const { authority, clients, pool } = authorityWithScripts(
    [
      rows(sessionRow()),
      rows({ operation_count: 0, reservation_count: 0 }),
      rows(),
      rows(),
      rows(),
    ],
    activePhaseSteps("prepared", options),
  );

  const receipt = await authority.reserveOperation(options);

  assert.deepEqual(receipt, {
    status: "prepared",
    session: snapshotFromSessionRow(preparedSession),
    operation: operationView(preparedOperation),
    reservation: reservationView(preparedReservation),
    acquired: false,
  });
  assert.equal(pool.connectCalls, 2);
  assert.deepEqual(authorityQueries(clients[0]).map(queryText), [
    `${READ_SESSION_QUERY} FOR UPDATE`,
    READ_ACTIVE_COUNTS_QUERY,
    READ_OPERATION_QUERY,
    INSERT_OPERATION_QUERY,
    `${READ_OPERATION_QUERY} FOR UPDATE`,
  ]);
  assert.deepEqual(queryTexts(clients[0]).slice(-3), [
    TRANSACTION_ID_QUERY,
    "ROLLBACK",
    "DISCARD ALL",
  ]);
  assert.equal(
    authorityQueries(clients[1]).some((args) =>
      /^(?:INSERT|UPDATE) /u.test(queryText(args)),
    ),
    false,
  );
  clients[0].assertExhausted();
  clients[1].assertExhausted();
});

test("invisible cross-session operation conflict retries before identity error", async () => {
  const original = reserveOptions();
  const otherSessionSnapshot = sessionSnapshot({
    sessionId: OTHER_SESSION_ID,
    sessionDocument: document(OTHER_SESSION_ID),
  });
  const conflicting = reserveOptions({
    expectedSession: otherSessionSnapshot,
  });
  const otherSessionRow = sessionRow({
    sessionId: OTHER_SESSION_ID,
    sessionDocument: document(OTHER_SESSION_ID),
  });
  const firstAttempt = [
    rows(otherSessionRow),
    rows({ operation_count: 0, reservation_count: 0 }),
    rows(),
    rows(),
    rows(),
  ];
  const secondAttempt = [
    rows(otherSessionRow),
    rows({ operation_count: 0, reservation_count: 0 }),
    rows(operationRow("prepared", { options: original })),
  ];
  const { authority, clients, pool } = authorityWithScripts(
    firstAttempt,
    secondAttempt,
  );

  await assertAuthorityError(authority.reserveOperation(conflicting), {
    code: "operation_identity_conflict",
  });

  assert.equal(pool.connectCalls, 2);
  assert.equal(
    authorityQueries(clients[0]).filter(
      (args) => queryText(args) === INSERT_OPERATION_QUERY,
    ).length,
    1,
  );
  assert.equal(
    authorityQueries(clients[1]).some(
      (args) => queryText(args) === INSERT_OPERATION_QUERY,
    ),
    false,
  );
  clients[0].assertExhausted();
  clients[1].assertExhausted();
});

test("session readback fails closed on pointer, operation, and reservation corruption", async () => {
  const options = reserveOptions();
  const malformedPointer = phaseSessionRow("prepared", {
    options,
    active: activeOperation("prepared", {
      options,
      operationRevision: "1",
    }),
  });
  const danglingPointer = phaseSessionRow("prepared", { options });
  const corruptOperation = operationRow("prepared", {
    options,
    revision: "1",
  });
  const corruptReservation = {
    ...reservationRow("prepared", { options }),
    reservation_id: "reservation-corrupt",
  };
  const { authority, clients } = authorityWithScripts(
    [rows(malformedPointer)],
    [rows(danglingPointer), rows()],
    [rows(danglingPointer), rows(corruptOperation)],
    [
      rows(danglingPointer),
      rows(operationRow("prepared", { options })),
      rows(corruptReservation),
    ],
    [
      rows(sessionRow()),
      rows({ operation_count: 1, reservation_count: 1 }),
    ],
  );
  const expectedCodes = [
    "session_state_invalid",
    "operation_state_invalid",
    "operation_state_invalid",
    "operation_state_invalid",
    "operation_state_invalid",
  ];

  for (const code of expectedCodes) {
    await assertAuthorityError(
      authority.readSession({ sessionId: SESSION_ID }),
      { code },
    );
  }

  for (const client of clients) {
    assert.equal(
      authorityQueries(client).some((args) =>
        queryText(args).startsWith("UPDATE "),
      ),
      false,
    );
    client.assertExhausted();
  }
});

test("registration replay and readSession accept valid progressed phases", async () => {
  const options = reserveOptions();
  const preparedSession = phaseSessionRow("prepared", { options });
  const uncertainSession = phaseSessionRow("uncertain", { options });
  const releasedSession = cancelledSessionRow({ options });
  const { authority, clients } = authorityWithScripts(
    [
      rows(),
      rows(preparedSession),
      rows(operationRow("prepared", { options })),
      rows(reservationRow("prepared", { options })),
    ],
    [
      rows(uncertainSession),
      rows(operationRow("uncertain", { options })),
      rows(reservationRow("uncertain", { options })),
    ],
    [
      rows(),
      rows(releasedSession),
      rows({ operation_count: 0, reservation_count: 0 }),
    ],
    [
      rows(releasedSession),
      rows({ operation_count: 0, reservation_count: 0 }),
    ],
  );

  const registeredPrepared = await authority.registerSession(registration());
  const readUncertain = await authority.readSession({
    sessionId: SESSION_ID,
  });
  const registeredReleased = await authority.registerSession(registration());
  const readReleased = await authority.readSession({
    sessionId: SESSION_ID,
  });

  assert.deepEqual(
    registeredPrepared,
    snapshotFromSessionRow(preparedSession),
  );
  assert.deepEqual(readUncertain, snapshotFromSessionRow(uncertainSession));
  assert.deepEqual(
    registeredReleased,
    snapshotFromSessionRow(releasedSession),
  );
  assert.deepEqual(readReleased, snapshotFromSessionRow(releasedSession));
  for (const snapshot of [
    registeredPrepared,
    readUncertain,
    registeredReleased,
    readReleased,
  ]) {
    assertDeepFrozen(snapshot);
  }
  assert.equal(
    authorityQueries(clients[0]).filter(
      (args) => queryText(args) === `${READ_OPERATION_QUERY} FOR UPDATE`,
    ).length,
    1,
  );
  assert.equal(
    authorityQueries(clients[2]).some(
      (args) => queryText(args) === `${READ_OPERATION_QUERY} FOR UPDATE`,
    ),
    false,
  );
  for (const client of clients) client.assertExhausted();
});

test("operation authority has no provider callback seam", () => {
  const { authority, store } = authorityWithScripts();

  assert.equal(typeof authority.finalizeOperation, "undefined");
  assert.equal(typeof authority.completeOperation, "undefined");

  assert.throws(
    () =>
      new PostgresSessionAuthority({
        store,
        provider: async () => {
          throw new Error("must not run");
        },
      }),
    (error) =>
      error instanceof PostgresSessionAuthorityError &&
      error.code === "invalid_authority_options" &&
      error.retryable === false &&
      Object.isFrozen(error),
  );
});

test("operation requests reject hostile and noncanonical JSON before PostgreSQL access", async () => {
  const { authority, pool } = authorityWithScripts();
  let accessorCalls = 0;
  const accessor = operationRequest();
  Object.defineProperty(accessor, "metadata", {
    enumerable: true,
    get() {
      accessorCalls += 1;
      throw new Error("sensitive accessor sentinel");
    },
  });
  const symbol = operationRequest();
  symbol[Symbol("unexpected")] = true;
  const cyclic = operationRequest();
  cyclic.self = cyclic;
  let deep = "leaf";
  for (let index = 0; index < 64; index += 1) deep = { child: deep };
  const wide = Object.create(null);
  for (let index = 0; index < 5_000; index += 1) {
    wide[`field_${index}`] = null;
  }
  const cases = [
    new Proxy(operationRequest(), {}),
    accessor,
    symbol,
    cyclic,
    operationRequest({ value: Number.NaN }),
    operationRequest({ value: Number.POSITIVE_INFINITY }),
    operationRequest({ value: "\ud800" }),
    operationRequest({ deep }),
    operationRequest({ wide }),
    operationRequest({ values: new Array(10_000).fill(null) }),
    operationRequest({ oversized: "x".repeat(2 * 1024 * 1024) }),
    operationRequest({ accessToken: "forbidden" }),
    operationRequest({ apiKey: "forbidden" }),
  ];

  for (const request of cases) {
    await assertAuthorityError(
      authority.reserveOperation(reserveOptions({ request })),
      {
        code: "invalid_operation_request",
        omittedText: "sensitive accessor sentinel",
      },
    );
  }
  assert.equal(accessorCalls, 0);
  assert.equal(pool.connectCalls, 0);
});

test("all operation APIs require exact own-data option fields", async () => {
  const { authority, pool } = authorityWithScripts();
  const prepared = {
    ...reserveOptions(),
    expectedOperationRevision: "0",
  };
  const starting = {
    ...reserveOptions(),
    expectedOperationRevision: "1",
  };
  const cancelled = {
    ...prepared,
    reason: "caller-abandoned-before-dispatch",
  };
  const cases = [
    () => authority.reserveOperation({ ...reserveOptions(), extra: true }),
    () => authority.reconcileOperation({ ...reserveOptions(), extra: true }),
    () =>
      authority.claimOperationDispatch({
        ...prepared,
        expectedOperationRevision: 0,
      }),
    () =>
      authority.markOperationUncertain({
        ...starting,
        expectedOperationRevision: "0",
      }),
    () =>
      authority.cancelPreparedOperation({
        ...cancelled,
        reason: "",
      }),
    () => authority.reserveOperation(new Proxy(reserveOptions(), {})),
  ];

  for (const invoke of cases) {
    await assertAuthorityError(invoke(), {
      code: "invalid_operation_request",
    });
  }
  assert.equal(pool.connectCalls, 0);
});
