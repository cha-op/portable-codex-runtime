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
  MAX_WRITER_LEASE_DURATION_MILLISECONDS,
  SESSION_AUTHORITY_DOCUMENT_VERSION,
  SESSION_OPERATION_CONFLICT_CLASS,
  WRITER_ATTACHMENT_ACQUIRE_OPERATION_KIND,
  WRITER_LEASE_RENEW_OPERATION_KIND,
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
const AUTHORITY_NOW = "2026-07-29T12:34:58.900Z";
const HIGH_EPOCH_AUTHORITY_NOW = "2026-07-29T12:35:00.000Z";
const RENEW_TRANSACTION_NOW = "2026-07-29T12:35:10.000Z";
const RENEW_AUTHORITY_NOW = "2026-07-29T12:35:20.000Z";
const EXPIRED_FINALIZE_NOW = "2026-07-29T12:40:00.000Z";
const TRANSACTION_TIMESTAMP_QUERY =
  "SELECT pg_catalog.transaction_timestamp() AS transaction_timestamp, pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id";
const TRANSACTION_ID_QUERY =
  "SELECT pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id";
const DURABLE_COMMIT_QUERY = "SET LOCAL synchronous_commit = on";
const READ_AUTHORITY_CLOCK_QUERY =
  "SELECT pg_catalog.clock_timestamp() AS authority_now";
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
const COMMIT_ACTIVE_OPERATION_QUERY = [
  "UPDATE session_authority.operation_claims",
  "SET state = 'committed', result = $3::jsonb,",
  "revision = revision + 1, updated_at = $4, retired_at = $4",
  "WHERE operation_id = $1 AND revision = $2::bigint",
  "AND state = $5 AND retired_at IS NULL",
  `RETURNING ${OPERATION_COLUMNS}`,
].join(" ");
const RELEASE_ACTIVE_RESERVATION_QUERY = [
  "UPDATE session_authority.reservations",
  "SET state = 'released', updated_at = $2, released_at = $2",
  "WHERE operation_id = $1 AND state = $3 AND released_at IS NULL",
  `RETURNING ${RESERVATION_COLUMNS}`,
].join(" ");
const INSERT_COMMITTED_OPERATION_QUERY = [
  "INSERT INTO session_authority.operation_claims",
  "(operation_id, session_id, kind, request, result, state, revision,",
  "created_at, updated_at, retired_at)",
  "VALUES ($1, $2::uuid, $3, $4::jsonb, $5::jsonb, 'committed', 0,",
  "$6, $6, $6)",
  "ON CONFLICT (operation_id) DO NOTHING",
  `RETURNING ${OPERATION_COLUMNS}`,
].join(" ");
const INSERT_RELEASED_RESERVATION_QUERY = [
  "INSERT INTO session_authority.reservations",
  "(reservation_id, operation_id, session_id, kind,",
  "expected_session_revision, state, payload, created_at, updated_at,",
  "expires_at, released_at)",
  "VALUES ($1, $2, $3::uuid, $4, $5::bigint, 'released',",
  "$6::jsonb, $7, $7, NULL, $7)",
  "ON CONFLICT (reservation_id) DO NOTHING",
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
      authorityNow = now,
      transactionId = "100",
    } = {},
  ) {
    this.commitError = commitError;
    this.commitResult = commitResult;
    this.connection = new EventEmitter();
    this.authorityNow = authorityNow;
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
    if (text === READ_AUTHORITY_CLOCK_QUERY) {
      return {
        rows: [{ authority_now: new Date(this.authorityNow) }],
      };
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
    lastOperation: null,
    recovery: null,
    launch: null,
    ...overrides,
  };
}

function legacyDocument(sessionId = SESSION_ID, overrides = {}) {
  const value = document(sessionId, {
    documentVersion: 1,
    ...overrides,
  });
  Reflect.deleteProperty(value, "lastOperation");
  return value;
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

function lastOperation({
  options = reserveOptions(),
  reason = "caller-abandoned-before-dispatch",
} = {}) {
  const binding = operationBinding(options);
  return {
    conflictClass: SESSION_OPERATION_CONFLICT_CLASS,
    expectedSessionRevision: options.expectedSession.revision,
    kind: options.kind,
    operationId: options.operationId,
    operationRevision: "1",
    requestSha256: binding.requestSha256,
    reservationId: binding.reservationId,
    resultSha256: sha256(JSON.stringify(cancellationResult(reason))),
    state: "committed",
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

function writerAcquireOptions(overrides = {}) {
  return {
    expectedSession: sessionSnapshot(),
    operationId: OPERATION_ID,
    kind: WRITER_ATTACHMENT_ACQUIRE_OPERATION_KIND,
    request: {
      contractVersion: 1,
      holderId: "host-001",
      leaseDurationMilliseconds: 60_000,
    },
    ...overrides,
  };
}

function derivedLeaseId(operationId) {
  return `lease-${sha256(`writer-lease:${operationId}`)}`;
}

function derivedAttachmentId(operationId) {
  return `attachment-${sha256(`writer-attachment:${operationId}`)}`;
}

function writerLease(
  options = writerAcquireOptions(),
  authorityNow = AUTHORITY_NOW,
) {
  return {
    contractVersion: 1,
    sessionId: options.expectedSession.sessionId,
    leaseId: derivedLeaseId(options.operationId),
    holderId: options.request.holderId,
    fencingEpoch: (
      BigInt(options.expectedSession.document.writerEpoch) + 1n
    ).toString(),
    expiresAt: new Date(
      Date.parse(authorityNow) +
        options.request.leaseDurationMilliseconds,
    ).toISOString(),
  };
}

function writerMutationRequest(
  options = writerAcquireOptions(),
  lease = writerLease(options),
) {
  return {
    contractVersion: 1,
    backendId: options.expectedSession.document.storageRef.backendId,
    storageId: options.expectedSession.document.storageRef.storageId,
    sessionId: options.expectedSession.sessionId,
    leaseId: lease.leaseId,
    holderId: lease.holderId,
    fencingEpoch: lease.fencingEpoch,
    operation: "attach",
    operationId: options.operationId,
    target: {
      attachmentId: derivedAttachmentId(options.operationId),
      kind: "attachment",
    },
  };
}

function writerMutationResult(
  options = writerAcquireOptions(),
  lease = writerLease(options),
  overrides = {},
) {
  const request = writerMutationRequest(options, lease);
  return {
    ...request,
    status: "attached",
    proofId: "proof-attachment-001",
    rootPath: "/var/lib/portable-codex/session-001",
    ...overrides,
  };
}

function writerAttachment(
  options = writerAcquireOptions(),
  lease = writerLease(options),
  overrides = {},
) {
  return {
    contractVersion: 1,
    backendId: options.expectedSession.document.storageRef.backendId,
    storageId: options.expectedSession.document.storageRef.storageId,
    sessionId: options.expectedSession.sessionId,
    attachmentId: derivedAttachmentId(options.operationId),
    leaseId: lease.leaseId,
    holderId: lease.holderId,
    fencingEpoch: lease.fencingEpoch,
    operationId: options.operationId,
    proofId: "proof-attachment-001",
    kind: "directory",
    rootPath: "/var/lib/portable-codex/session-001",
    mode: "read-write",
    ...overrides,
  };
}

function writerAttachmentResult(
  options = writerAcquireOptions(),
  lease = writerLease(options),
  overrides = {},
) {
  return {
    resultVersion: 1,
    outcome: "writer-attached",
    lease: structuredClone(lease),
    attachment: writerAttachment(options, lease),
    mutationResult: writerMutationResult(options, lease),
    ...overrides,
  };
}

function terminalPointer({ options, operationRevision, result }) {
  const binding = operationBinding(options);
  return {
    conflictClass: SESSION_OPERATION_CONFLICT_CLASS,
    expectedSessionRevision: options.expectedSession.revision,
    kind: options.kind,
    operationId: options.operationId,
    operationRevision,
    requestSha256: binding.requestSha256,
    reservationId: binding.reservationId,
    resultSha256: sha256(JSON.stringify(result)),
    state: "committed",
  };
}

function writerStartingSessionRow({
  options = writerAcquireOptions(),
  lease = writerLease(options),
  updatedAt = LATEST,
} = {}) {
  return sessionRow({
    sessionId: options.expectedSession.sessionId,
    revision: (
      BigInt(options.expectedSession.revision) + 2n
    ).toString(),
    sessionDocument: document(options.expectedSession.sessionId, {
      lifecycle: "ATTACHING",
      writerEpoch: lease.fencingEpoch,
      lease: structuredClone(lease),
      activeOperation: activeOperation("starting", { options }),
      lastOperation:
        options.expectedSession.document.lastOperation === undefined
          ? null
          : structuredClone(options.expectedSession.document.lastOperation),
    }),
    createdAt: options.expectedSession.createdAt,
    updatedAt,
  });
}

function writerUncertainSessionRow({
  options = writerAcquireOptions(),
  lease = writerLease(options),
  updatedAt = FINAL,
} = {}) {
  return sessionRow({
    sessionId: options.expectedSession.sessionId,
    revision: (
      BigInt(options.expectedSession.revision) + 3n
    ).toString(),
    sessionDocument: document(options.expectedSession.sessionId, {
      lifecycle: "ATTACHING",
      writerEpoch: lease.fencingEpoch,
      lease: structuredClone(lease),
      activeOperation: activeOperation("uncertain", { options }),
      lastOperation:
        options.expectedSession.document.lastOperation === undefined
          ? null
          : structuredClone(options.expectedSession.document.lastOperation),
    }),
    createdAt: options.expectedSession.createdAt,
    updatedAt,
  });
}

function writerAttachedSessionRow({
  options = writerAcquireOptions(),
  lease = writerLease(options),
  result = writerAttachmentResult(options, lease),
  operationRevision = "2",
  updatedAt = FINAL,
} = {}) {
  return sessionRow({
    sessionId: options.expectedSession.sessionId,
    revision: (
      BigInt(options.expectedSession.revision) +
      BigInt(operationRevision) +
      1n
    ).toString(),
    sessionDocument: document(options.expectedSession.sessionId, {
      lifecycle: "ATTACHED",
      writerEpoch: lease.fencingEpoch,
      lease: structuredClone(result.lease),
      attachment: structuredClone(result.attachment),
      activeOperation: null,
      lastOperation: terminalPointer({
        options,
        operationRevision,
        result,
      }),
    }),
    createdAt: options.expectedSession.createdAt,
    updatedAt,
  });
}

function writerCommittedOperationRow({
  options = writerAcquireOptions(),
  lease = writerLease(options),
  result = writerAttachmentResult(options, lease),
  revision = "2",
  updatedAt = FINAL,
} = {}) {
  return operationRow("committed", {
    options,
    revision,
    result,
    retiredAt: updatedAt,
    updatedAt,
  });
}

function renewOptions(expectedSession, overrides = {}) {
  return {
    expectedSession,
    operationId: OTHER_OPERATION_ID,
    kind: WRITER_LEASE_RENEW_OPERATION_KIND,
    request: {
      contractVersion: 1,
      leaseDurationMilliseconds: 60_000,
    },
    ...overrides,
  };
}

function renewalResult(
  options,
  authorityNow = RENEW_AUTHORITY_NOW,
) {
  const lease = {
    ...structuredClone(options.expectedSession.document.lease),
    expiresAt: new Date(
      Date.parse(authorityNow) +
        options.request.leaseDurationMilliseconds,
    ).toISOString(),
  };
  return {
    resultVersion: 1,
    outcome: "writer-lease-renewed",
    lease,
    attachment: structuredClone(
      options.expectedSession.document.attachment,
    ),
  };
}

function renewedSessionRow({
  options,
  result = renewalResult(options),
  updatedAt = RENEW_TRANSACTION_NOW,
} = {}) {
  return sessionRow({
    sessionId: options.expectedSession.sessionId,
    revision: (
      BigInt(options.expectedSession.revision) + 1n
    ).toString(),
    sessionDocument: document(options.expectedSession.sessionId, {
      lifecycle: "ATTACHED",
      writerEpoch: result.lease.fencingEpoch,
      lease: structuredClone(result.lease),
      attachment: structuredClone(result.attachment),
      lastOperation: terminalPointer({
        options,
        operationRevision: "0",
        result,
      }),
    }),
    createdAt: options.expectedSession.createdAt,
    updatedAt,
  });
}

function phaseSessionRow(
  state,
  {
    options = reserveOptions(),
    revision = (
      BigInt(options.expectedSession.revision) +
      (state === "prepared" ? 1n : state === "starting" ? 2n : 3n)
    ).toString(),
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
      lastOperation:
        options.expectedSession.document.lastOperation === undefined
          ? null
          : structuredClone(options.expectedSession.document.lastOperation),
    }),
    createdAt: options.expectedSession.createdAt,
    updatedAt,
  });
}

function legacyPhaseSessionRow(state, { options = reserveOptions() } = {}) {
  const value = phaseSessionRow(state, { options });
  value.document = legacyDocument(options.expectedSession.sessionId, {
    activeOperation: activeOperation(state, { options }),
  });
  return value;
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
  reason = "caller-abandoned-before-dispatch",
  updatedAt = LATEST,
} = {}) {
  return sessionRow({
    sessionId: options.expectedSession.sessionId,
    revision: (
      BigInt(options.expectedSession.revision) + 2n
    ).toString(),
    sessionDocument: document(options.expectedSession.sessionId, {
      lastOperation: lastOperation({ options, reason }),
    }),
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

test("a legacy v1 reserve keeps its request identity and upgrades the session to v2", async () => {
  const legacySessionDocument = legacyDocument();
  const options = reserveOptions({
    expectedSession: sessionSnapshot({
      sessionDocument: legacySessionDocument,
    }),
  });
  const initialSession = sessionRow({
    sessionDocument: legacySessionDocument,
  });
  const preparedOperation = operationRow("prepared", { options });
  const preparedReservation = reservationRow("prepared", { options });
  const preparedSession = phaseSessionRow("prepared", { options });
  const { authority, clients } = authorityWithScripts(
    {
      options: { now: LATER },
      steps: [
        rows(initialSession),
        rows({ operation_count: 0, reservation_count: 0 }),
        rows(),
        rows(preparedOperation),
        rows(preparedReservation),
        rows(preparedSession),
      ],
    },
    activePhaseSteps("prepared", options),
  );

  const acquired = await authority.reserveOperation(options);
  const replayed = await authority.reserveOperation(options);

  assert.equal(acquired.acquired, true);
  assert.equal(replayed.acquired, false);
  assert.equal(
    acquired.operation.requestSha256,
    sha256(JSON.stringify(operationEnvelope(options))),
  );
  assert.deepEqual(
    acquired.operation.expectedSession.document,
    legacySessionDocument,
  );
  assert.equal(
    acquired.session.document.documentVersion,
    SESSION_AUTHORITY_DOCUMENT_VERSION,
  );
  assert.equal(acquired.session.document.lastOperation, null);
  assert.deepEqual(replayed.operation, acquired.operation);
  assert.equal(
    authorityQueries(clients[1]).some((args) =>
      queryText(args).startsWith("UPDATE "),
    ),
    false,
  );
  for (const client of clients) client.assertExhausted();
});

test("legacy v1 active documents fail closed before relation reads or phase repair", async () => {
  const legacySessionDocument = legacyDocument();
  const options = reserveOptions({
    expectedSession: sessionSnapshot({
      sessionDocument: legacySessionDocument,
    }),
  });
  const legacyPreparedSession = legacyPhaseSessionRow("prepared", {
    options,
  });
  const legacyStartingSession = legacyPhaseSessionRow("starting", {
    options,
  });
  const legacyUncertainSession = legacyPhaseSessionRow("uncertain", {
    options,
  });
  const { authority, clients } = authorityWithScripts(
    [rows(legacyPreparedSession)],
    [rows(legacyPreparedSession)],
    [rows(legacyStartingSession)],
    [rows(legacyUncertainSession)],
  );

  await assertAuthorityError(
    authority.readSession({ sessionId: SESSION_ID }),
    { code: "session_state_invalid" },
  );
  await assertAuthorityError(
    authority.claimOperationDispatch({
      ...options,
      expectedOperationRevision: "0",
    }),
    { code: "session_state_invalid" },
  );
  await assertAuthorityError(
    authority.markOperationUncertain({
      ...options,
      expectedOperationRevision: "1",
    }),
    { code: "session_state_invalid" },
  );
  await assertAuthorityError(
    authority.readSession({ sessionId: SESSION_ID }),
    { code: "session_state_invalid" },
  );
  for (const client of clients) {
    assert.equal(authorityQueries(client).length, 1);
    assert.equal(
      authorityQueries(client).some((args) =>
        /^(?:INSERT|UPDATE) /u.test(queryText(args)),
      ),
      false,
    );
    client.assertExhausted();
  }
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
  const prior = reserveOptions({
    operationId: OTHER_OPERATION_ID,
  });
  const current = cancelledSessionRow({ options: prior });
  const { authority, clients } = authorityWithScripts([
    rows(current),
    rows({ operation_count: 0, reservation_count: 0 }),
    rows(committedOperationRow({ options: prior })),
    rows(releasedReservationRow({ options: prior })),
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
      JSON.stringify(releasedSession.document),
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

test("cancellation commit acknowledgement loss reconciles the terminal anchor", async () => {
  const options = reserveOptions();
  const reason = "caller-abandoned-before-dispatch";
  const committedOperation = committedOperationRow({ options, reason });
  const releasedReservation = releasedReservationRow({ options });
  const releasedSession = cancelledSessionRow({ options, reason });
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
  const commitError = new Error(
    "sensitive cancellation commit acknowledgement lost",
  );
  const { authority, clients, pool } = authorityWithScripts(
    {
      options: { commitError, now: LATEST },
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

  await assert.rejects(
    authority.cancelPreparedOperation(input),
    assertStoreCommitUncertain,
  );
  const reconciled = await authority.reconcileOperation(options);
  const replayed = await authority.cancelPreparedOperation(input);

  const stable = {
    status: "committed",
    session: snapshotFromSessionRow(releasedSession),
    operation: operationView(committedOperation),
    reservation: reservationView(releasedReservation),
  };
  assert.deepEqual(reconciled, stable);
  assert.deepEqual(replayed, {
    ...stable,
    cancelled: false,
  });
  assertDeepFrozen(reconciled);
  assertDeepFrozen(replayed);
  assert.equal(pool.connectCalls, 3);
  assert.equal(
    queryTexts(clients[0]).filter((text) => text === "COMMIT").length,
    1,
  );
  assert.equal(queryTexts(clients[0]).at(-1), "ROLLBACK");
  for (const client of clients.slice(1)) {
    assert.equal(
      authorityQueries(client).some((args) =>
        /^(?:INSERT|UPDATE) /u.test(queryText(args)),
      ),
      false,
    );
  }
  clients[0].assertExhausted({ destroyed: true });
  clients[1].assertExhausted();
  clients[2].assertExhausted();
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
  const unanchoredTerminalSession = sessionRow({
    revision: "2",
    sessionDocument: document(),
    updatedAt: LATEST,
  });
  const terminalSession = cancelledSessionRow({ options });
  const committedOperation = committedOperationRow({ options });
  const releasedReservation = releasedReservationRow({ options });
  const wrongResultAnchorSession = cancelledSessionRow({ options });
  wrongResultAnchorSession.document.lastOperation.resultSha256 = "0".repeat(64);
  const wrongTimestampOperation = committedOperationRow({
    options,
    updatedAt: FINAL,
  });
  const wrongTimestampReservation = releasedReservationRow({
    options,
    updatedAt: FINAL,
  });
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
    [rows(unanchoredTerminalSession)],
    [
      rows(terminalSession),
      rows({ operation_count: 0, reservation_count: 0 }),
      rows(),
    ],
    [
      rows(terminalSession),
      rows({ operation_count: 0, reservation_count: 0 }),
      rows(committedOperation),
      rows(),
    ],
    [
      rows(wrongResultAnchorSession),
      rows({ operation_count: 0, reservation_count: 0 }),
      rows(committedOperation),
      rows(releasedReservation),
    ],
    [
      rows(terminalSession),
      rows({ operation_count: 0, reservation_count: 0 }),
      rows(wrongTimestampOperation),
      rows(wrongTimestampReservation),
    ],
  );
  const expectedCodes = [
    "session_state_invalid",
    "operation_state_invalid",
    "operation_state_invalid",
    "operation_state_invalid",
    "operation_state_invalid",
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

test("terminal anchor binding mismatches fail after only bounded primary-key reads", async () => {
  const options = reserveOptions();
  const terminalSession = cancelledSessionRow({ options });
  const committedOperation = committedOperationRow({ options });
  const releasedReservation = releasedReservationRow({ options });
  const wrongKindOperation = {
    ...committedOperation,
    kind: "checkpoint-restore",
  };
  const wrongKindReservation = {
    ...releasedReservation,
    kind: "checkpoint-restore",
  };
  const wrongReservationId = {
    ...releasedReservation,
    reservation_id: "reservation-corrupt",
  };
  const wrongRequestOptions = reserveOptions({
    request: operationRequest({
      checkpointId: "checkpoint-corrupt",
    }),
  });
  const { authority, clients } = authorityWithScripts(
    [
      rows(terminalSession),
      rows({ operation_count: 0, reservation_count: 0 }),
      rows(wrongKindOperation),
      rows(wrongKindReservation),
    ],
    [
      rows(terminalSession),
      rows({ operation_count: 0, reservation_count: 0 }),
      rows(committedOperation),
      rows(wrongReservationId),
    ],
    [
      rows(terminalSession),
      rows({ operation_count: 0, reservation_count: 0 }),
      rows(committedOperationRow({ options: wrongRequestOptions })),
      rows(releasedReservationRow({ options: wrongRequestOptions })),
    ],
  );

  for (const client of clients) {
    await assertAuthorityError(
      authority.readSession({ sessionId: SESSION_ID }),
      { code: "operation_state_invalid" },
    );
    assert.deepEqual(authorityQueries(client), [
      extendedQuery(READ_SESSION_QUERY, [SESSION_ID]),
      extendedQuery(READ_ACTIVE_COUNTS_QUERY, [SESSION_ID]),
      extendedQuery(READ_OPERATION_QUERY, [OPERATION_ID]),
      extendedQuery(READ_RESERVATION_QUERY, [OPERATION_ID]),
    ]);
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
      rows(committedOperationRow({ options })),
      rows(releasedReservationRow({ options })),
    ],
    [
      rows(releasedSession),
      rows({ operation_count: 0, reservation_count: 0 }),
      rows(committedOperationRow({ options })),
      rows(releasedReservationRow({ options })),
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
    operationRequest({ value: "\u0000" }),
    operationRequest({ ["key\u0000suffix"]: true }),
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

test("operation key sorting ignores poisoned Array prototype indices", async () => {
  const { authority, pool } = authorityWithScripts();
  const request = operationRequest();
  Object.defineProperty(request, "metadata", {
    enumerable: true,
    get() {
      throw new Error("sensitive accessor sentinel");
    },
  });
  const options = reserveOptions({ request });
  const original = Object.getOwnPropertyDescriptor(Array.prototype, "0");
  let setterCalls = 0;
  let pending;

  try {
    Object.defineProperty(Array.prototype, "0", {
      configurable: true,
      set() {
        setterCalls += 1;
      },
    });
    pending = authority.reserveOperation(options);
  } finally {
    if (original === undefined) {
      Reflect.deleteProperty(Array.prototype, "0");
    } else {
      Object.defineProperty(Array.prototype, "0", original);
    }
  }

  await assertAuthorityError(pending, {
    code: "invalid_operation_request",
    omittedText: "sensitive accessor sentinel",
  });
  assert.equal(setterCalls, 0);
  assert.equal(pool.connectCalls, 0);
});

test("operation canonicalization uses captured post-import global intrinsics", async () => {
  const { authority, pool } = authorityWithScripts();
  const input = {
    ...reserveOptions(),
    expectedOperationRevision: "9",
  };
  const originalString = Object.getOwnPropertyDescriptor(
    globalThis,
    "String",
  );
  const originalJson = Object.getOwnPropertyDescriptor(globalThis, "JSON");
  const originalBuffer = Object.getOwnPropertyDescriptor(
    globalThis,
    "Buffer",
  );
  assert.notEqual(originalString, undefined);
  assert.notEqual(originalJson, undefined);
  assert.notEqual(originalBuffer, undefined);
  const reads = {
    buffer: 0,
    json: 0,
    string: 0,
  };
  let pending;

  try {
    Object.defineProperty(globalThis, "String", {
      configurable: true,
      get() {
        reads.string += 1;
        return originalString.value;
      },
    });
    Object.defineProperty(globalThis, "JSON", {
      configurable: true,
      get() {
        reads.json += 1;
        return originalJson.value;
      },
    });
    Object.defineProperty(globalThis, "Buffer", {
      configurable: true,
      get() {
        reads.buffer += 1;
        return originalBuffer.value;
      },
    });
    pending = authority.claimOperationDispatch(input);
  } finally {
    Object.defineProperty(globalThis, "String", originalString);
    Object.defineProperty(globalThis, "JSON", originalJson);
    Object.defineProperty(globalThis, "Buffer", originalBuffer);
  }

  await assertAuthorityError(pending, {
    code: "invalid_operation_request",
  });
  assert.deepEqual(reads, {
    buffer: 0,
    json: 0,
    string: 0,
  });
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

test("writer attachment dispatch allocates one DB-clock lease and uint64 epoch", async () => {
  const options = writerAcquireOptions();
  const lease = writerLease(options);
  const preparedSession = phaseSessionRow("prepared", { options });
  const startingSession = writerStartingSessionRow({ options, lease });
  const startingOperation = operationRow("starting", { options });
  const startingReservation = reservationRow("starting", { options });
  const { authority, clients } = authorityWithScripts({
    options: {
      authorityNow: AUTHORITY_NOW,
      now: LATEST,
    },
    steps: [
      rows(preparedSession),
      rows(operationRow("prepared", { options })),
      rows(reservationRow("prepared", { options })),
      rows(startingOperation),
      rows(startingReservation),
      rows(startingSession),
    ],
  });

  const receipt = await authority.claimWriterAttachmentDispatch({
    ...options,
    expectedOperationRevision: "0",
  });

  assert.equal(receipt.dispatchGranted, true);
  assert.equal(receipt.authorityNow, AUTHORITY_NOW);
  assert.deepEqual(receipt.lease, lease);
  assert.deepEqual(
    receipt.mutationRequest,
    writerMutationRequest(options, lease),
  );
  assert.equal(receipt.session.document.lifecycle, "ATTACHING");
  assert.equal(receipt.session.document.writerEpoch, "1");
  assert.deepEqual(receipt.session.document.lease, lease);
  assert.equal(receipt.session.document.attachment, null);
  assert.deepEqual(authorityQueries(clients[0]), [
    extendedQuery(`${READ_SESSION_QUERY} FOR UPDATE`, [SESSION_ID]),
    extendedQuery(`${READ_OPERATION_QUERY} FOR UPDATE`, [OPERATION_ID]),
    extendedQuery(`${READ_RESERVATION_QUERY} FOR UPDATE`, [OPERATION_ID]),
    extendedQuery(READ_AUTHORITY_CLOCK_QUERY, []),
    extendedQuery(START_OPERATION_QUERY, [OPERATION_ID, "0", LATEST]),
    extendedQuery(START_RESERVATION_QUERY, [OPERATION_ID, LATEST]),
    extendedQuery(UPDATE_SESSION_QUERY, [
      SESSION_ID,
      "1",
      JSON.stringify(startingSession.document),
      LATEST,
    ]),
  ]);
  clients[0].assertExhausted();
});

test("writer attachment dispatch replay returns evidence without regranting", async () => {
  const options = writerAcquireOptions();
  const lease = writerLease(options);
  const startingSession = writerStartingSessionRow({ options, lease });
  const { authority, clients } = authorityWithScripts({
    steps: [
      rows(startingSession),
      rows(operationRow("starting", { options })),
      rows(reservationRow("starting", { options })),
    ],
  });

  const receipt = await authority.claimWriterAttachmentDispatch({
    ...options,
    expectedOperationRevision: "0",
  });

  assert.equal(receipt.dispatchGranted, false);
  assert.deepEqual(receipt.lease, lease);
  assert.deepEqual(
    receipt.mutationRequest,
    writerMutationRequest(options, lease),
  );
  assert.deepEqual(receipt.session, snapshotFromSessionRow(startingSession));
  assert.deepEqual(
    authorityQueries(clients[0]),
    [
      extendedQuery(`${READ_SESSION_QUERY} FOR UPDATE`, [SESSION_ID]),
      extendedQuery(`${READ_OPERATION_QUERY} FOR UPDATE`, [OPERATION_ID]),
      extendedQuery(`${READ_RESERVATION_QUERY} FOR UPDATE`, [OPERATION_ID]),
    ],
  );
  clients[0].assertExhausted();
});

test("cancelled writer dispatch replay never borrows a later writer lease", async () => {
  const oldOptions = writerAcquireOptions();
  const cancelledRow = cancelledSessionRow({ options: oldOptions });
  const newOptions = writerAcquireOptions({
    expectedSession: snapshotFromSessionRow(cancelledRow),
    operationId: OTHER_OPERATION_ID,
  });
  const newLease = writerLease(newOptions);
  const newResult = writerAttachmentResult(newOptions, newLease);
  const attachedRow = writerAttachedSessionRow({
    options: newOptions,
    lease: newLease,
    result: newResult,
  });
  const oldOperation = committedOperationRow({ options: oldOptions });
  const oldReservation = releasedReservationRow({ options: oldOptions });
  const { authority, clients } = authorityWithScripts([
    rows(attachedRow),
    rows({ operation_count: 0, reservation_count: 0 }),
    rows(
      writerCommittedOperationRow({
        options: newOptions,
        lease: newLease,
        result: newResult,
      }),
    ),
    rows(
      reservationRow("released", {
        options: newOptions,
        updatedAt: FINAL,
        releasedAt: FINAL,
      }),
    ),
    rows(oldOperation),
    rows(oldReservation),
  ]);

  const receipt = await authority.claimWriterAttachmentDispatch({
    ...oldOptions,
    expectedOperationRevision: "0",
  });

  assert.equal(receipt.dispatchGranted, false);
  assert.equal(receipt.operation.result.outcome, "cancelled-before-dispatch");
  assert.equal(Object.hasOwn(receipt, "lease"), false);
  assert.equal(Object.hasOwn(receipt, "mutationRequest"), false);
  assert.deepEqual(receipt.session, snapshotFromSessionRow(attachedRow));
  assert.equal(
    authorityQueries(clients[0]).some((args) =>
      /^(?:INSERT|UPDATE) /u.test(queryText(args)),
    ),
    false,
  );
  clients[0].assertExhausted();
});

test("writer attachment dispatch supports epochs above signed bigint", async () => {
  const writerEpoch = "9223372036854775807";
  const expectedSession = sessionSnapshot({
    sessionDocument: document(SESSION_ID, {
      writerEpoch,
    }),
  });
  const options = writerAcquireOptions({ expectedSession });
  const binding = operationBinding(options);
  const preparedSession = sessionRow({
    revision: "1",
    sessionDocument: document(SESSION_ID, {
      writerEpoch,
      activeOperation: activeOperation("prepared", { options }),
    }),
    createdAt: NOW,
    updatedAt: LATER,
  });
  const lease = writerLease(options, HIGH_EPOCH_AUTHORITY_NOW);
  const startingSession = writerStartingSessionRow({
    options,
    lease,
    updatedAt: FINAL,
  });
  const { authority, clients } = authorityWithScripts({
    options: {
      authorityNow: HIGH_EPOCH_AUTHORITY_NOW,
      now: FINAL,
    },
    steps: [
      rows(preparedSession),
      rows(
        operationRow("prepared", {
          options,
          createdAt: LATER,
          updatedAt: LATER,
        }),
      ),
      rows(
        reservationRow("prepared", {
          options,
          createdAt: LATER,
          updatedAt: LATER,
        }),
      ),
      rows(
        operationRow("starting", {
          options,
          createdAt: LATER,
          updatedAt: FINAL,
        }),
      ),
      rows(
        reservationRow("starting", {
          options,
          createdAt: LATER,
          updatedAt: FINAL,
        }),
      ),
      rows(startingSession),
    ],
  });

  const receipt = await authority.claimWriterAttachmentDispatch({
    ...options,
    expectedOperationRevision: "0",
  });

  assert.equal(
    receipt.lease.fencingEpoch,
    "9223372036854775808",
  );
  assert.equal(
    receipt.session.document.writerEpoch,
    "9223372036854775808",
  );
  assert.equal(
    receipt.operation.requestSha256,
    binding.requestSha256,
  );
  clients[0].assertExhausted();
});

test("writer epoch exhaustion fails before PostgreSQL access", async () => {
  const prior = lastOperation();
  const expectedSession = sessionSnapshot({
    revision: "2",
    sessionDocument: document(SESSION_ID, {
      writerEpoch: "18446744073709551615",
      lastOperation: prior,
    }),
    updatedAt: LATER,
  });
  const { authority, pool } = authorityWithScripts();

  await assertAuthorityError(
    authority.reserveOperation(
      writerAcquireOptions({ expectedSession }),
    ),
    { code: "writer_epoch_exhausted" },
  );
  assert.equal(pool.connectCalls, 0);
});

test("writer dispatch reserves revisions for uncertain recovery and finalize", async () => {
  const ancient = lastOperation();
  ancient.expectedSessionRevision = "9223372036854775800";
  const priorExpectedSession = sessionSnapshot({
    revision: "9223372036854775802",
    sessionDocument: document(SESSION_ID, {
      lastOperation: ancient,
    }),
  });
  const priorOptions = reserveOptions({
    expectedSession: priorExpectedSession,
    operationId: OTHER_OPERATION_ID,
  });
  const prior = lastOperation({ options: priorOptions });
  const expectedSession = sessionSnapshot({
    revision: "9223372036854775804",
    sessionDocument: document(SESSION_ID, {
      lastOperation: prior,
    }),
  });
  const options = writerAcquireOptions({ expectedSession });
  const preparedSession = phaseSessionRow("prepared", { options });
  const priorOperation = operationRow("committed", {
    options: priorOptions,
    createdAt: NOW,
    updatedAt: NOW,
    retiredAt: NOW,
    result: cancellationResult(),
  });
  const priorReservation = reservationRow("released", {
    options: priorOptions,
    createdAt: NOW,
    updatedAt: NOW,
    releasedAt: NOW,
  });
  const { authority, clients } = authorityWithScripts([
    rows(preparedSession),
    rows(operationRow("prepared", { options })),
    rows(reservationRow("prepared", { options })),
    rows(priorOperation),
    rows(priorReservation),
  ]);

  assert.equal(preparedSession.revision, "9223372036854775805");
  await assertAuthorityError(
    authority.claimWriterAttachmentDispatch({
      ...options,
      expectedOperationRevision: "0",
    }),
    { code: "session_revision_exhausted" },
  );
  assert.deepEqual(
    authorityQueries(clients[0]).map(queryText),
    [
      `${READ_SESSION_QUERY} FOR UPDATE`,
      `${READ_OPERATION_QUERY} FOR UPDATE`,
      `${READ_RESERVATION_QUERY} FOR UPDATE`,
      READ_OPERATION_QUERY,
      READ_RESERVATION_QUERY,
    ],
  );
  assert.equal(
    authorityQueries(clients[0]).some((args) =>
      /^(?:INSERT|UPDATE) /u.test(queryText(args)),
    ),
    false,
  );
  clients[0].assertExhausted();
});

test("exact attachment proof finalizes atomically even after lease expiry", async () => {
  const options = writerAcquireOptions();
  const lease = writerLease(options);
  const result = writerAttachmentResult(options, lease);
  const startingSession = writerStartingSessionRow({ options, lease });
  const committedOperation = writerCommittedOperationRow({
    options,
    lease,
    result,
    updatedAt: EXPIRED_FINALIZE_NOW,
  });
  const releasedReservation = reservationRow("released", {
    options,
    updatedAt: EXPIRED_FINALIZE_NOW,
    releasedAt: EXPIRED_FINALIZE_NOW,
  });
  const attachedSession = writerAttachedSessionRow({
    options,
    lease,
    result,
    updatedAt: EXPIRED_FINALIZE_NOW,
  });
  const { authority, clients } = authorityWithScripts({
    options: { now: EXPIRED_FINALIZE_NOW },
    steps: [
      rows(startingSession),
      rows(operationRow("starting", { options })),
      rows(reservationRow("starting", { options })),
      rows(committedOperation),
      rows(releasedReservation),
      rows(attachedSession),
    ],
  });

  const receipt = await authority.finalizeWriterAttachment({
    ...options,
    expectedOperationRevision: "1",
    attachment: result.attachment,
    mutationResult: result.mutationResult,
  });

  assert.equal(receipt.finalized, true);
  assert.equal(receipt.operation.revision, "2");
  assert.equal(receipt.reservation.state, "released");
  assert.equal(receipt.session.document.lifecycle, "ATTACHED");
  assert.deepEqual(receipt.session.document.attachment, result.attachment);
  assert.equal(
    authorityQueries(clients[0]).some(
      (args) => queryText(args) === READ_AUTHORITY_CLOCK_QUERY,
    ),
    false,
  );
  assert.deepEqual(authorityQueries(clients[0]), [
    extendedQuery(`${READ_SESSION_QUERY} FOR UPDATE`, [SESSION_ID]),
    extendedQuery(`${READ_OPERATION_QUERY} FOR UPDATE`, [OPERATION_ID]),
    extendedQuery(`${READ_RESERVATION_QUERY} FOR UPDATE`, [OPERATION_ID]),
    extendedQuery(COMMIT_ACTIVE_OPERATION_QUERY, [
      OPERATION_ID,
      "1",
      JSON.stringify(result),
      EXPIRED_FINALIZE_NOW,
      "starting",
    ]),
    extendedQuery(RELEASE_ACTIVE_RESERVATION_QUERY, [
      OPERATION_ID,
      EXPIRED_FINALIZE_NOW,
      "starting",
    ]),
    extendedQuery(UPDATE_SESSION_QUERY, [
      SESSION_ID,
      "2",
      JSON.stringify(attachedSession.document),
      EXPIRED_FINALIZE_NOW,
    ]),
  ]);
  clients[0].assertExhausted();
});

test("an exact proof can reconcile an uncertain attachment operation", async () => {
  const options = writerAcquireOptions();
  const lease = writerLease(options);
  const result = writerAttachmentResult(options, lease);
  const committedOperation = writerCommittedOperationRow({
    options,
    lease,
    result,
    revision: "3",
    updatedAt: EXPIRED_FINALIZE_NOW,
  });
  const releasedReservation = reservationRow("released", {
    options,
    updatedAt: EXPIRED_FINALIZE_NOW,
    releasedAt: EXPIRED_FINALIZE_NOW,
  });
  const attachedSession = writerAttachedSessionRow({
    options,
    lease,
    result,
    operationRevision: "3",
    updatedAt: EXPIRED_FINALIZE_NOW,
  });
  const { authority, clients } = authorityWithScripts({
    options: { now: EXPIRED_FINALIZE_NOW },
    steps: [
      rows(writerUncertainSessionRow({ options, lease })),
      rows(operationRow("uncertain", { options })),
      rows(reservationRow("uncertain", { options })),
      rows(committedOperation),
      rows(releasedReservation),
      rows(attachedSession),
    ],
  });

  const receipt = await authority.finalizeWriterAttachment({
    ...options,
    expectedOperationRevision: "2",
    attachment: result.attachment,
    mutationResult: result.mutationResult,
  });

  assert.equal(receipt.finalized, true);
  assert.equal(receipt.operation.revision, "3");
  assert.equal(receipt.session.revision, "4");
  assert.equal(
    receipt.session.document.lastOperation.operationRevision,
    "3",
  );
  clients[0].assertExhausted();
});

test("typed writer dispatch can become uncertain and finalize with the exact grant", async () => {
  const options = writerAcquireOptions();
  const lease = writerLease(options);
  const result = writerAttachmentResult(options, lease);
  const preparedSession = phaseSessionRow("prepared", { options });
  const startingOperation = operationRow("starting", { options });
  const startingReservation = reservationRow("starting", { options });
  const startingSession = writerStartingSessionRow({ options, lease });
  const uncertainOperation = operationRow("uncertain", { options });
  const uncertainReservation = reservationRow("uncertain", { options });
  const uncertainSession = writerUncertainSessionRow({ options, lease });
  const committedOperation = writerCommittedOperationRow({
    options,
    lease,
    result,
    revision: "3",
    updatedAt: EXPIRED_FINALIZE_NOW,
  });
  const releasedReservation = reservationRow("released", {
    options,
    updatedAt: EXPIRED_FINALIZE_NOW,
    releasedAt: EXPIRED_FINALIZE_NOW,
  });
  const attachedSession = writerAttachedSessionRow({
    options,
    lease,
    result,
    operationRevision: "3",
    updatedAt: EXPIRED_FINALIZE_NOW,
  });
  const { authority, clients } = authorityWithScripts(
    {
      options: {
        authorityNow: AUTHORITY_NOW,
        now: LATEST,
      },
      steps: [
        rows(preparedSession),
        rows(operationRow("prepared", { options })),
        rows(reservationRow("prepared", { options })),
        rows(startingOperation),
        rows(startingReservation),
        rows(startingSession),
      ],
    },
    {
      options: { now: FINAL },
      steps: [
        rows(startingSession),
        rows(startingOperation),
        rows(startingReservation),
        rows(uncertainOperation),
        rows(uncertainReservation),
        rows(uncertainSession),
      ],
    },
    {
      options: { now: EXPIRED_FINALIZE_NOW },
      steps: [
        rows(uncertainSession),
        rows(uncertainOperation),
        rows(uncertainReservation),
        rows(committedOperation),
        rows(releasedReservation),
        rows(attachedSession),
      ],
    },
  );

  const dispatched = await authority.claimWriterAttachmentDispatch({
    ...options,
    expectedOperationRevision: "0",
  });
  const uncertain = await authority.markOperationUncertain({
    ...options,
    expectedOperationRevision: "1",
  });
  const finalized = await authority.finalizeWriterAttachment({
    ...options,
    expectedOperationRevision: "2",
    attachment: result.attachment,
    mutationResult: result.mutationResult,
  });

  assert.equal(dispatched.dispatchGranted, true);
  assert.deepEqual(dispatched.lease, lease);
  assert.deepEqual(
    dispatched.mutationRequest,
    writerMutationRequest(options, lease),
  );
  assert.equal(dispatched.session.document.lifecycle, "ATTACHING");
  assert.equal(uncertain.changed, true);
  assert.equal(uncertain.session.document.lifecycle, "ATTACHING");
  assert.equal(uncertain.session.document.writerEpoch, lease.fencingEpoch);
  assert.deepEqual(uncertain.session.document.lease, lease);
  assert.equal(uncertain.session.document.attachment, null);
  assert.equal(finalized.finalized, true);
  assert.deepEqual(finalized.operation.result, result);
  assert.equal(finalized.session.document.lifecycle, "ATTACHED");
  assert.equal(finalized.session.document.writerEpoch, lease.fencingEpoch);
  assert.deepEqual(finalized.session.document.lease, lease);
  assert.deepEqual(finalized.session.document.attachment, result.attachment);
  for (const client of clients) client.assertExhausted();
});

test("attachment finalization binds the mutation proof to the exact attachment", async () => {
  const options = writerAcquireOptions();
  const lease = writerLease(options);
  const result = writerAttachmentResult(options, lease);
  const { authority, clients } = authorityWithScripts({
    options: { now: FINAL },
    steps: [
      rows(writerStartingSessionRow({ options, lease })),
      rows(operationRow("starting", { options })),
      rows(reservationRow("starting", { options })),
    ],
  });

  await assertAuthorityError(
    authority.finalizeWriterAttachment({
      ...options,
      expectedOperationRevision: "1",
      attachment: {
        ...result.attachment,
        proofId: "different-proof",
      },
      mutationResult: result.mutationResult,
    }),
    { code: "invalid_operation_request" },
  );
  assert.deepEqual(
    authorityQueries(clients[0]).map(queryText),
    [
      `${READ_SESSION_QUERY} FOR UPDATE`,
      `${READ_OPERATION_QUERY} FOR UPDATE`,
      `${READ_RESERVATION_QUERY} FOR UPDATE`,
    ],
  );
  clients[0].assertExhausted();
});

test("attachment finalization rejects every exact attachment/result binding mismatch", async () => {
  const options = writerAcquireOptions();
  const lease = writerLease(options);
  const result = writerAttachmentResult(options, lease);
  const cases = [
    ["attachment backend", { attachment: { backendId: "other-backend" } }],
    ["attachment storage", { attachment: { storageId: "other-storage" } }],
    ["attachment session", { attachment: { sessionId: OTHER_SESSION_ID } }],
    ["attachment lease", { attachment: { leaseId: "other-lease" } }],
    ["attachment holder", { attachment: { holderId: "other-holder" } }],
    ["attachment epoch", { attachment: { fencingEpoch: "2" } }],
    [
      "attachment operation",
      { attachment: { operationId: OTHER_OPERATION_ID } },
    ],
    [
      "attachment identity",
      { attachment: { attachmentId: "other-attachment" } },
    ],
    ["attachment proof", { attachment: { proofId: "other-proof" } }],
    [
      "attachment root",
      {
        attachment: {
          rootPath: "/var/lib/portable-codex/substituted-session",
        },
      },
    ],
    [
      "result backend",
      { mutationResult: { backendId: "other-backend" } },
    ],
    [
      "result storage",
      { mutationResult: { storageId: "other-storage" } },
    ],
    [
      "result session",
      { mutationResult: { sessionId: OTHER_SESSION_ID } },
    ],
    ["result lease", { mutationResult: { leaseId: "other-lease" } }],
    ["result holder", { mutationResult: { holderId: "other-holder" } }],
    ["result epoch", { mutationResult: { fencingEpoch: "2" } }],
    [
      "result operation",
      { mutationResult: { operationId: OTHER_OPERATION_ID } },
    ],
    [
      "result attachment",
      {
        mutationResult: {
          target: {
            ...result.mutationResult.target,
            attachmentId: "other-attachment",
          },
        },
      },
    ],
    ["result proof", { mutationResult: { proofId: "other-proof" } }],
    [
      "result root",
      {
        mutationResult: {
          rootPath: "/var/lib/portable-codex/substituted-session",
        },
      },
    ],
  ];
  const { authority, clients } = authorityWithScripts(
    ...cases.map(() => [
      rows(writerStartingSessionRow({ options, lease })),
      rows(operationRow("starting", { options })),
      rows(reservationRow("starting", { options })),
    ]),
  );

  for (const [index, [field, mismatch]] of cases.entries()) {
    await assertAuthorityError(
      authority.finalizeWriterAttachment({
        ...options,
        expectedOperationRevision: "1",
        attachment: {
          ...result.attachment,
          ...mismatch.attachment,
        },
        mutationResult: {
          ...result.mutationResult,
          ...mismatch.mutationResult,
        },
      }),
      { code: "invalid_operation_request" },
    );
    assert.deepEqual(
      authorityQueries(clients[index]).map(queryText),
      [
        `${READ_SESSION_QUERY} FOR UPDATE`,
        `${READ_OPERATION_QUERY} FOR UPDATE`,
        `${READ_RESERVATION_QUERY} FOR UPDATE`,
      ],
      `${field} mismatch must fail before any write`,
    );
    clients[index].assertExhausted();
  }
});

test("exact attachment finalization replay returns the original terminal proof", async () => {
  const options = writerAcquireOptions();
  const lease = writerLease(options);
  const result = writerAttachmentResult(options, lease);
  const attachedSession = writerAttachedSessionRow({
    options,
    lease,
    result,
  });
  const committedOperation = writerCommittedOperationRow({
    options,
    lease,
    result,
  });
  const releasedReservation = reservationRow("released", {
    options,
    updatedAt: FINAL,
    releasedAt: FINAL,
  });
  const { authority, clients } = authorityWithScripts([
    rows(attachedSession),
    rows({ operation_count: 0, reservation_count: 0 }),
    rows(committedOperation),
    rows(releasedReservation),
  ]);

  const receipt = await authority.finalizeWriterAttachment({
    ...options,
    expectedOperationRevision: "1",
    attachment: result.attachment,
    mutationResult: result.mutationResult,
  });

  assert.equal(receipt.finalized, false);
  assert.deepEqual(receipt.operation.result, result);
  assert.deepEqual(receipt.session, snapshotFromSessionRow(attachedSession));
  assert.deepEqual(
    authorityQueries(clients[0]).map(queryText),
    [
      `${READ_SESSION_QUERY} FOR UPDATE`,
      READ_ACTIVE_COUNTS_QUERY,
      READ_OPERATION_QUERY,
      READ_RESERVATION_QUERY,
    ],
  );
  clients[0].assertExhausted();
});

test("writer lease renewal is one exact DB-clock terminal transaction", async () => {
  const acquireOptions = writerAcquireOptions();
  const lease = writerLease(acquireOptions);
  const acquireResult = writerAttachmentResult(acquireOptions, lease);
  const attachedRow = writerAttachedSessionRow({
    options: acquireOptions,
    lease,
    result: acquireResult,
  });
  const expectedSession = snapshotFromSessionRow(attachedRow);
  const options = renewOptions(expectedSession);
  const binding = operationBinding(options);
  const result = renewalResult(options);
  const committedOperation = operationRow("committed", {
    options,
    revision: "0",
    createdAt: RENEW_TRANSACTION_NOW,
    updatedAt: RENEW_TRANSACTION_NOW,
    retiredAt: RENEW_TRANSACTION_NOW,
    result,
  });
  const releasedReservation = reservationRow("released", {
    options,
    createdAt: RENEW_TRANSACTION_NOW,
    updatedAt: RENEW_TRANSACTION_NOW,
    releasedAt: RENEW_TRANSACTION_NOW,
  });
  const { authority, clients } = authorityWithScripts({
    options: {
      authorityNow: RENEW_AUTHORITY_NOW,
      now: RENEW_TRANSACTION_NOW,
    },
    steps: [
      rows(attachedRow),
      rows({ operation_count: 0, reservation_count: 0 }),
      rows(
        writerCommittedOperationRow({
          options: acquireOptions,
          lease,
          result: acquireResult,
        }),
      ),
      rows(
        reservationRow("released", {
          options: acquireOptions,
          updatedAt: FINAL,
          releasedAt: FINAL,
        }),
      ),
      rows(),
      rows(committedOperation),
      rows(releasedReservation),
      rows(renewedSessionRow({ options, result })),
    ],
  });

  const receipt = await authority.renewWriterLease(options);

  assert.equal(receipt.renewed, true);
  assert.equal(receipt.authorityNow, RENEW_AUTHORITY_NOW);
  assert.equal(receipt.operation.revision, "0");
  assert.equal(receipt.session.revision, "4");
  assert.equal(
    receipt.session.document.lease.expiresAt,
    result.lease.expiresAt,
  );
  assert.equal(
    receipt.session.document.writerEpoch,
    lease.fencingEpoch,
  );
  assert.deepEqual(
    receipt.session.document.attachment,
    acquireResult.attachment,
  );
  assert.deepEqual(authorityQueries(clients[0]), [
    extendedQuery(`${READ_SESSION_QUERY} FOR UPDATE`, [SESSION_ID]),
    extendedQuery(READ_ACTIVE_COUNTS_QUERY, [SESSION_ID]),
    extendedQuery(READ_OPERATION_QUERY, [OPERATION_ID]),
    extendedQuery(READ_RESERVATION_QUERY, [OPERATION_ID]),
    extendedQuery(READ_OPERATION_QUERY, [OTHER_OPERATION_ID]),
    extendedQuery(READ_AUTHORITY_CLOCK_QUERY, []),
    extendedQuery(INSERT_COMMITTED_OPERATION_QUERY, [
      OTHER_OPERATION_ID,
      SESSION_ID,
      WRITER_LEASE_RENEW_OPERATION_KIND,
      binding.serializedEnvelope,
      JSON.stringify(result),
      RENEW_TRANSACTION_NOW,
    ]),
    extendedQuery(INSERT_RELEASED_RESERVATION_QUERY, [
      binding.reservationId,
      OTHER_OPERATION_ID,
      SESSION_ID,
      WRITER_LEASE_RENEW_OPERATION_KIND,
      expectedSession.revision,
      JSON.stringify(reservationRow("released", {
        options,
        createdAt: RENEW_TRANSACTION_NOW,
        updatedAt: RENEW_TRANSACTION_NOW,
        releasedAt: RENEW_TRANSACTION_NOW,
      }).payload),
      RENEW_TRANSACTION_NOW,
    ]),
    extendedQuery(UPDATE_SESSION_QUERY, [
      SESSION_ID,
      expectedSession.revision,
      JSON.stringify(renewedSessionRow({ options, result }).document),
      RENEW_TRANSACTION_NOW,
    ]),
  ]);
  clients[0].assertExhausted();
});

test("renewal COMMIT acknowledgement loss replays without extending twice", async () => {
  const acquireOptions = writerAcquireOptions();
  const lease = writerLease(acquireOptions);
  const acquireResult = writerAttachmentResult(acquireOptions, lease);
  const attachedRow = writerAttachedSessionRow({
    options: acquireOptions,
    lease,
    result: acquireResult,
  });
  const options = renewOptions(snapshotFromSessionRow(attachedRow));
  const binding = operationBinding(options);
  const result = renewalResult(options);
  const committedOperation = operationRow("committed", {
    options,
    revision: "0",
    createdAt: RENEW_TRANSACTION_NOW,
    updatedAt: RENEW_TRANSACTION_NOW,
    retiredAt: RENEW_TRANSACTION_NOW,
    result,
  });
  const releasedReservation = reservationRow("released", {
    options,
    createdAt: RENEW_TRANSACTION_NOW,
    updatedAt: RENEW_TRANSACTION_NOW,
    releasedAt: RENEW_TRANSACTION_NOW,
  });
  const renewedRow = renewedSessionRow({ options, result });
  const commitError = new Error(
    "sensitive renewal commit acknowledgement lost",
  );
  const { authority, clients, pool } = authorityWithScripts(
    {
      options: {
        authorityNow: RENEW_AUTHORITY_NOW,
        commitError,
        now: RENEW_TRANSACTION_NOW,
      },
      steps: [
        rows(attachedRow),
        rows({ operation_count: 0, reservation_count: 0 }),
        rows(
          writerCommittedOperationRow({
            options: acquireOptions,
            lease,
            result: acquireResult,
          }),
        ),
        rows(
          reservationRow("released", {
            options: acquireOptions,
            updatedAt: FINAL,
            releasedAt: FINAL,
          }),
        ),
        rows(),
        rows(committedOperation),
        rows(releasedReservation),
        rows(renewedRow),
      ],
    },
    [
      rows(renewedRow),
      rows({ operation_count: 0, reservation_count: 0 }),
      rows(committedOperation),
      rows(releasedReservation),
    ],
  );

  await assert.rejects(
    authority.renewWriterLease(options),
    assertStoreCommitUncertain,
  );
  const replay = await authority.renewWriterLease(options);

  assert.equal(replay.renewed, false);
  assert.equal(
    replay.operation.result.lease.expiresAt,
    result.lease.expiresAt,
  );
  assert.deepEqual(replay.session, snapshotFromSessionRow(renewedRow));
  assert.equal(pool.connectCalls, 2);
  assert.equal(
    queryTexts(clients[0]).filter((text) => text === "COMMIT").length,
    1,
  );
  assert.equal(queryTexts(clients[0]).at(-1), "ROLLBACK");
  assert.equal(
    authorityQueries(clients[0]).filter(
      (args) => queryText(args) === INSERT_COMMITTED_OPERATION_QUERY,
    ).length,
    1,
  );
  assert.deepEqual(
    authorityQueries(clients[0]).find(
      (args) => queryText(args) === INSERT_COMMITTED_OPERATION_QUERY,
    ),
    extendedQuery(INSERT_COMMITTED_OPERATION_QUERY, [
      OTHER_OPERATION_ID,
      SESSION_ID,
      WRITER_LEASE_RENEW_OPERATION_KIND,
      binding.serializedEnvelope,
      JSON.stringify(result),
      RENEW_TRANSACTION_NOW,
    ]),
  );
  assert.equal(
    authorityQueries(clients[1]).some(
      (args) =>
        queryText(args) === READ_AUTHORITY_CLOCK_QUERY ||
        /^(?:INSERT|UPDATE) /u.test(queryText(args)),
    ),
    false,
  );
  clients[0].assertExhausted({ destroyed: true });
  clients[1].assertExhausted();
});

test("exact lease renewal replay never extends expiration twice", async () => {
  const acquireOptions = writerAcquireOptions();
  const lease = writerLease(acquireOptions);
  const acquireResult = writerAttachmentResult(acquireOptions, lease);
  const attachedRow = writerAttachedSessionRow({
    options: acquireOptions,
    lease,
    result: acquireResult,
  });
  const options = renewOptions(snapshotFromSessionRow(attachedRow));
  const result = renewalResult(options);
  const renewedRow = renewedSessionRow({ options, result });
  const committedOperation = operationRow("committed", {
    options,
    revision: "0",
    createdAt: RENEW_TRANSACTION_NOW,
    updatedAt: RENEW_TRANSACTION_NOW,
    retiredAt: RENEW_TRANSACTION_NOW,
    result,
  });
  const releasedReservation = reservationRow("released", {
    options,
    createdAt: RENEW_TRANSACTION_NOW,
    updatedAt: RENEW_TRANSACTION_NOW,
    releasedAt: RENEW_TRANSACTION_NOW,
  });
  const { authority, clients } = authorityWithScripts([
    rows(renewedRow),
    rows({ operation_count: 0, reservation_count: 0 }),
    rows(committedOperation),
    rows(releasedReservation),
  ]);

  const receipt = await authority.renewWriterLease(options);

  assert.equal(receipt.renewed, false);
  assert.equal(receipt.operation.revision, "0");
  assert.equal(receipt.operation.result.lease.expiresAt, result.lease.expiresAt);
  assert.deepEqual(receipt.session, snapshotFromSessionRow(renewedRow));
  assert.equal(
    authorityQueries(clients[0]).some(
      (args) => queryText(args) === READ_AUTHORITY_CLOCK_QUERY,
    ),
    false,
  );
  clients[0].assertExhausted();
});

test("invisible renewal INSERT conflict retries to exact replay", async () => {
  const acquireOptions = writerAcquireOptions();
  const lease = writerLease(acquireOptions);
  const acquireResult = writerAttachmentResult(acquireOptions, lease);
  const attachedRow = writerAttachedSessionRow({
    options: acquireOptions,
    lease,
    result: acquireResult,
  });
  const options = renewOptions(snapshotFromSessionRow(attachedRow));
  const result = renewalResult(options);
  const renewedRow = renewedSessionRow({ options, result });
  const committedOperation = operationRow("committed", {
    options,
    revision: "0",
    createdAt: RENEW_TRANSACTION_NOW,
    updatedAt: RENEW_TRANSACTION_NOW,
    retiredAt: RENEW_TRANSACTION_NOW,
    result,
  });
  const releasedReservation = reservationRow("released", {
    options,
    createdAt: RENEW_TRANSACTION_NOW,
    updatedAt: RENEW_TRANSACTION_NOW,
    releasedAt: RENEW_TRANSACTION_NOW,
  });
  const { authority, clients, pool } = authorityWithScripts(
    {
      options: {
        authorityNow: RENEW_AUTHORITY_NOW,
        now: RENEW_TRANSACTION_NOW,
      },
      steps: [
        rows(attachedRow),
        rows({ operation_count: 0, reservation_count: 0 }),
        rows(
          writerCommittedOperationRow({
            options: acquireOptions,
            lease,
            result: acquireResult,
          }),
        ),
        rows(
          reservationRow("released", {
            options: acquireOptions,
            updatedAt: FINAL,
            releasedAt: FINAL,
          }),
        ),
        rows(),
        rows(),
        rows(),
      ],
    },
    [
      rows(renewedRow),
      rows({ operation_count: 0, reservation_count: 0 }),
      rows(committedOperation),
      rows(releasedReservation),
    ],
  );

  const replay = await authority.renewWriterLease(options);

  assert.equal(replay.renewed, false);
  assert.deepEqual(replay.operation.result, result);
  assert.deepEqual(replay.session, snapshotFromSessionRow(renewedRow));
  assert.equal(pool.connectCalls, 2);
  assert.deepEqual(
    authorityQueries(clients[0]).map(queryText),
    [
      `${READ_SESSION_QUERY} FOR UPDATE`,
      READ_ACTIVE_COUNTS_QUERY,
      READ_OPERATION_QUERY,
      READ_RESERVATION_QUERY,
      READ_OPERATION_QUERY,
      READ_AUTHORITY_CLOCK_QUERY,
      INSERT_COMMITTED_OPERATION_QUERY,
      `${READ_OPERATION_QUERY} FOR UPDATE`,
    ],
  );
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

test("unexpired writer lease that would not extend performs zero writes", async () => {
  const acquireOptions = writerAcquireOptions();
  const lease = writerLease(acquireOptions);
  const acquireResult = writerAttachmentResult(acquireOptions, lease);
  const attachedRow = writerAttachedSessionRow({
    options: acquireOptions,
    lease,
    result: acquireResult,
  });
  const options = renewOptions(snapshotFromSessionRow(attachedRow), {
    request: {
      contractVersion: 1,
      leaseDurationMilliseconds: 1,
    },
  });
  const { authority, clients } = authorityWithScripts({
    options: {
      authorityNow: HIGH_EPOCH_AUTHORITY_NOW,
      now: FINAL,
    },
    steps: [
      rows(attachedRow),
      rows({ operation_count: 0, reservation_count: 0 }),
      rows(
        writerCommittedOperationRow({
          options: acquireOptions,
          lease,
          result: acquireResult,
        }),
      ),
      rows(
        reservationRow("released", {
          options: acquireOptions,
          updatedAt: FINAL,
          releasedAt: FINAL,
        }),
      ),
      rows(),
    ],
  });

  assert.ok(Date.parse(lease.expiresAt) > Date.parse(HIGH_EPOCH_AUTHORITY_NOW));
  await assertAuthorityError(authority.renewWriterLease(options), {
    code: "writer_lease_not_extended",
  });
  assert.equal(
    authorityQueries(clients[0]).some((args) =>
      /^(?:INSERT|UPDATE) /u.test(queryText(args)),
    ),
    false,
  );
  assert.equal(
    authorityQueries(clients[0]).at(-1)?.[0]?.text,
    READ_AUTHORITY_CLOCK_QUERY,
  );
  clients[0].assertExhausted();
});

test("writer lease renewal cannot resurrect the lease at expiry equality", async () => {
  const acquireOptions = writerAcquireOptions();
  const lease = writerLease(acquireOptions);
  const acquireResult = writerAttachmentResult(acquireOptions, lease);
  const attachedRow = writerAttachedSessionRow({
    options: acquireOptions,
    lease,
    result: acquireResult,
  });
  const options = renewOptions(snapshotFromSessionRow(attachedRow));
  const { authority, clients } = authorityWithScripts({
    options: {
      authorityNow: lease.expiresAt,
      now: RENEW_TRANSACTION_NOW,
    },
    steps: [
      rows(attachedRow),
      rows({ operation_count: 0, reservation_count: 0 }),
      rows(
        writerCommittedOperationRow({
          options: acquireOptions,
          lease,
          result: acquireResult,
        }),
      ),
      rows(
        reservationRow("released", {
          options: acquireOptions,
          updatedAt: FINAL,
          releasedAt: FINAL,
        }),
      ),
      rows(),
    ],
  });

  await assertAuthorityError(authority.renewWriterLease(options), {
    code: "writer_lease_expired",
  });
  assert.equal(
    authorityQueries(clients[0]).at(-1)?.[0]?.text,
    READ_AUTHORITY_CLOCK_QUERY,
  );
  clients[0].assertExhausted();
});

test("writer lease and attachment APIs reject invalid typed input before PostgreSQL", async () => {
  const { authority, pool } = authorityWithScripts();
  const acquire = writerAcquireOptions();
  const cases = [
    () =>
      authority.reserveOperation({
        ...acquire,
        request: {
          ...acquire.request,
          leaseDurationMilliseconds: 0,
        },
      }),
    () =>
      authority.reserveOperation({
        ...acquire,
        request: {
          ...acquire.request,
          leaseDurationMilliseconds:
            MAX_WRITER_LEASE_DURATION_MILLISECONDS + 1,
        },
      }),
    () =>
      authority.claimOperationDispatch({
        ...acquire,
        expectedOperationRevision: "0",
      }),
    () =>
      authority.renewWriterLease({
        ...renewOptions(sessionSnapshot()),
      }),
  ];

  for (const invoke of cases) {
    await assertAuthorityError(invoke(), {
      code: "invalid_operation_request",
    });
  }
  assert.equal(pool.connectCalls, 0);
});

test("typed writer transitions reject hostile non-exact inputs before PostgreSQL", async () => {
  const { authority, pool } = authorityWithScripts();
  const options = writerAcquireOptions();
  const lease = writerLease(options);
  const result = writerAttachmentResult(options, lease);
  const claim = {
    ...options,
    expectedOperationRevision: "0",
  };
  const finalize = {
    ...options,
    expectedOperationRevision: "1",
    attachment: result.attachment,
    mutationResult: result.mutationResult,
  };
  let accessorCalls = 0;
  const accessorClaim = { ...claim };
  Object.defineProperty(accessorClaim, "operationId", {
    enumerable: true,
    get() {
      accessorCalls += 1;
      throw new Error("sensitive typed writer accessor");
    },
  });
  const symbolFinalize = { ...finalize };
  symbolFinalize[Symbol("unexpected")] = true;
  const cases = [
    () =>
      authority.claimWriterAttachmentDispatch(new Proxy(claim, {})),
    () => authority.claimWriterAttachmentDispatch(accessorClaim),
    () =>
      authority.finalizeWriterAttachment(new Proxy(finalize, {})),
    () => authority.finalizeWriterAttachment(symbolFinalize),
  ];

  for (const invoke of cases) {
    await assertAuthorityError(invoke(), {
      code: "invalid_operation_request",
      omittedText: "sensitive typed writer accessor",
    });
  }
  assert.equal(accessorCalls, 0);
  assert.equal(pool.connectCalls, 0);
});
