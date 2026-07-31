import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { EventEmitter } from "node:events";
import { syncBuiltinESMExports } from "node:module";
import path from "node:path";
import test from "node:test";

import {
  PostgresSerializableStore,
  PostgresSerializableStoreError,
} from "../src/postgres-serializable-store.mjs";
import {
  CHECKPOINT_CAPTURE_OPERATION_KIND,
  createCheckpointCaptureOperationRequest,
  PostgresSessionAuthority,
  PostgresSessionAuthorityError,
  MAX_WRITER_LEASE_DURATION_MILLISECONDS,
  SESSION_AUTHORITY_DOCUMENT_VERSION,
  SESSION_OPERATION_CONFLICT_CLASS,
  WRITER_ATTACHMENT_ACQUIRE_OPERATION_KIND,
  WRITER_FORCE_FENCE_OPERATION_KIND,
  WRITER_LEASE_RENEW_OPERATION_KIND,
  WRITER_RELEASE_OPERATION_KIND,
} from "../src/postgres-session-authority.mjs";
import {
  createSessionManifest,
  serializeSessionManifest,
} from "../src/session-storage-contracts.mjs";

const SESSION_ID = "019f2100-0000-7000-8000-000000000001";
const OTHER_SESSION_ID = "019f2100-0000-7000-8000-000000000002";
const OPERATION_ID = "operation-001";
const OTHER_OPERATION_ID = "operation-002";
const CAPTURE_OPERATION_ID = "checkpoint-capture-operation-001";
const CAPTURE_ATTEMPT_ID = "019f2100-0000-7000-8000-000000000003";
const CHECKPOINT_ID = "checkpoint-001";
const ARTIFACT_ID = "checkpoint-artifact-001";
const PROCESS_INCARNATION_ID = "process-incarnation-001";
const STOP_OPERATION_ID = "stop-operation-001";
const WRITER_INCARNATION_ID = "writer-incarnation-001";
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
const WRITER_PREPARED_NOW = "2026-07-29T12:40:01.000Z";
const WRITER_DISPATCH_NOW = "2026-07-29T12:40:02.000Z";
const WRITER_UNCERTAIN_NOW = "2026-07-29T12:40:03.000Z";
const WRITER_FINALIZE_NOW = "2026-07-29T12:40:04.000Z";
const WRITER_RETRY_PREPARED_NOW = "2026-07-29T12:40:05.000Z";
const WRITER_RETRY_DISPATCH_NOW = "2026-07-29T12:40:06.000Z";
const CAPTURE_PREPARED_NOW = "2026-07-29T12:35:01.000Z";
const CAPTURE_DISPATCH_NOW = "2026-07-29T12:35:02.000Z";
const CAPTURE_AUTHORITY_NOW = "2026-07-29T12:35:02.500Z";
const CAPTURE_UNCERTAIN_NOW = "2026-07-29T12:35:03.000Z";
const CAPTURE_FINALIZE_NOW = "2026-07-29T12:35:04.000Z";
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
const CAPTURE_ATTEMPT_COLUMNS = [
  "capture_attempt_id",
  "operation_id",
  "session_id",
  "binding",
  "claimed_at",
].join(", ");
const CAPTURE_ATTEMPT_TOMBSTONE_COLUMNS = [
  "capture_attempt_id",
  "operation_id",
  "session_id",
  "retired_at",
  "tombstone",
].join(", ");
const CHECKPOINT_CATALOGUE_COLUMNS = [
  "checkpoint_id",
  "session_id",
  "capture_attempt_id",
  "document",
  "committed_at",
].join(", ");
const READ_CAPTURE_ATTEMPT_QUERY = [
  `SELECT ${CAPTURE_ATTEMPT_COLUMNS}`,
  "FROM session_authority.capture_attempt_claims",
  "WHERE operation_id = $1",
].join(" ");
const READ_CAPTURE_ATTEMPT_BY_ID_QUERY = [
  `SELECT ${CAPTURE_ATTEMPT_COLUMNS}`,
  "FROM session_authority.capture_attempt_claims",
  "WHERE capture_attempt_id = $1::uuid",
].join(" ");
const INSERT_CAPTURE_ATTEMPT_QUERY = [
  "INSERT INTO session_authority.capture_attempt_claims",
  "(capture_attempt_id, operation_id, session_id, binding, claimed_at)",
  "VALUES ($1::uuid, $2, $3::uuid, $4::jsonb, $5)",
  "ON CONFLICT DO NOTHING",
  `RETURNING ${CAPTURE_ATTEMPT_COLUMNS}`,
].join(" ");
const READ_CAPTURE_ATTEMPT_TOMBSTONE_QUERY = [
  `SELECT ${CAPTURE_ATTEMPT_TOMBSTONE_COLUMNS}`,
  "FROM session_authority.capture_attempt_tombstones",
  "WHERE operation_id = $1",
].join(" ");
const READ_CHECKPOINT_CATALOGUE_BY_ID_QUERY = [
  `SELECT ${CHECKPOINT_CATALOGUE_COLUMNS}`,
  "FROM session_authority.checkpoint_catalogue",
  "WHERE checkpoint_id = $1",
].join(" ");
const READ_CHECKPOINT_CATALOGUE_BY_ATTEMPT_QUERY = [
  `SELECT ${CHECKPOINT_CATALOGUE_COLUMNS}`,
  "FROM session_authority.checkpoint_catalogue",
  "WHERE capture_attempt_id = $1::uuid",
].join(" ");
const INSERT_CHECKPOINT_CATALOGUE_QUERY = [
  "INSERT INTO session_authority.checkpoint_catalogue",
  "(checkpoint_id, session_id, capture_attempt_id, document, committed_at)",
  "VALUES ($1, $2::uuid, $3::uuid, $4::jsonb, $5)",
  "ON CONFLICT DO NOTHING",
  `RETURNING ${CHECKPOINT_CATALOGUE_COLUMNS}`,
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
      ...structuredClone(options.expectedSession.document),
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
      ...structuredClone(options.expectedSession.document),
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
      ...structuredClone(options.expectedSession.document),
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

function writerAcquiredFixture({
  capabilities = backendCapabilities(),
  operationId = OPERATION_ID,
} = {}) {
  const expectedSession = sessionSnapshot({
    sessionDocument: document(SESSION_ID, {
      backendCapabilities: capabilities,
    }),
  });
  const options = writerAcquireOptions({
    expectedSession,
    operationId,
  });
  const lease = writerLease(options);
  const result = writerAttachmentResult(options, lease);
  const session = writerAttachedSessionRow({
    options,
    lease,
    result,
  });
  return {
    committedOperation: writerCommittedOperationRow({
      options,
      lease,
      result,
    }),
    expectedSession: snapshotFromSessionRow(session),
    lease,
    options,
    releasedReservation: reservationRow("released", {
      options,
      updatedAt: FINAL,
      releasedAt: FINAL,
    }),
    result,
    session,
  };
}

function writerReleaseOptions(fixture, overrides = {}) {
  return {
    expectedSession: fixture.expectedSession,
    operationId: OTHER_OPERATION_ID,
    kind: WRITER_RELEASE_OPERATION_KIND,
    request: {
      contractVersion: 1,
      target: {
        attachmentId: fixture.result.attachment.attachmentId,
        kind: "attachment",
      },
    },
    ...overrides,
  };
}

function writerForceFenceOptions(fixture, overrides = {}) {
  return {
    expectedSession: fixture.expectedSession,
    operationId: OTHER_OPERATION_ID,
    kind: WRITER_FORCE_FENCE_OPERATION_KIND,
    request: {
      contractVersion: 1,
      target: {
        attachmentId:
          fixture.result?.attachment?.attachmentId ??
          fixture.fenceTarget.attachmentId,
        kind: "attachment",
      },
    },
    ...overrides,
  };
}

function writerReleaseMutationRequest(options) {
  const lease = options.expectedSession.document.lease;
  return {
    contractVersion: 1,
    backendId: options.expectedSession.document.storageRef.backendId,
    storageId: options.expectedSession.document.storageRef.storageId,
    sessionId: options.expectedSession.sessionId,
    leaseId: lease.leaseId,
    holderId: lease.holderId,
    fencingEpoch: lease.fencingEpoch,
    operation: "detach",
    operationId: options.operationId,
    target: structuredClone(options.request.target),
  };
}

function writerReleaseMutationResult(options, overrides = {}) {
  return {
    ...writerReleaseMutationRequest(options),
    proofId: "proof-detachment-001",
    status: "detached",
    ...overrides,
  };
}

function writerReleaseResult(options, mutationResult) {
  return {
    resultVersion: 1,
    outcome: "writer-released",
    lease: structuredClone(options.expectedSession.document.lease),
    attachment: structuredClone(
      options.expectedSession.document.attachment,
    ),
    mutationResult: structuredClone(mutationResult),
  };
}

function writerForceFenceRequest(options, writerEpoch) {
  const lease = options.expectedSession.document.lease;
  return {
    backendId: options.expectedSession.document.storageRef.backendId,
    contractVersion: 1,
    fencingEpoch: writerEpoch,
    operationId: options.operationId,
    revokedFence: {
      fencingEpoch: lease.fencingEpoch,
      holderId: lease.holderId,
      leaseId: lease.leaseId,
    },
    sessionId: options.expectedSession.sessionId,
    storageId: options.expectedSession.document.storageRef.storageId,
    target: structuredClone(options.request.target),
  };
}

function writerForceFenceProof(options, writerEpoch, overrides = {}) {
  return {
    ...writerForceFenceRequest(options, writerEpoch),
    proofId: "proof-force-fence-001",
    status: "fenced",
    ...overrides,
  };
}

function writerForceFenceResult(options, writerEpoch, fenceResult) {
  return {
    resultVersion: 1,
    outcome: "writer-fenced",
    writerEpoch,
    lease: structuredClone(options.expectedSession.document.lease),
    attachment: structuredClone(
      options.expectedSession.document.attachment,
    ),
    fenceTarget: structuredClone(options.request.target),
    fenceResult: structuredClone(fenceResult),
  };
}

function writerLifecyclePhaseSessionRow(
  state,
  {
    options,
    lifecycle,
    writerEpoch = options.expectedSession.document.writerEpoch,
    updatedAt =
      state === "prepared" ? LATER : state === "starting" ? LATEST : FINAL,
  },
) {
  return sessionRow({
    sessionId: options.expectedSession.sessionId,
    revision: (
      BigInt(options.expectedSession.revision) +
      (state === "prepared" ? 1n : state === "starting" ? 2n : 3n)
    ).toString(),
    sessionDocument: document(options.expectedSession.sessionId, {
      ...structuredClone(options.expectedSession.document),
      lifecycle:
        state === "prepared"
          ? options.expectedSession.document.lifecycle
          : lifecycle,
      writerEpoch,
      activeOperation: activeOperation(state, { options }),
    }),
    createdAt: options.expectedSession.createdAt,
    updatedAt,
  });
}

function writerReleasePhaseSessionRow(
  state,
  { options, updatedAt } = {},
) {
  return writerLifecyclePhaseSessionRow(state, {
    options,
    lifecycle: "RELEASING",
    updatedAt,
  });
}

function writerForceFencePhaseSessionRow(
  state,
  {
    options,
    writerEpoch = (
      BigInt(options.expectedSession.document.writerEpoch) + 1n
    ).toString(),
    updatedAt,
  } = {},
) {
  return writerLifecyclePhaseSessionRow(state, {
    options,
    lifecycle: "FENCING",
    writerEpoch:
      state === "prepared"
        ? options.expectedSession.document.writerEpoch
        : writerEpoch,
    updatedAt,
  });
}

function writerTerminalOperationRow({
  createdAt,
  options,
  result,
  revision,
  updatedAt = FINAL,
}) {
  return operationRow("committed", {
    createdAt,
    options,
    revision,
    result,
    retiredAt: updatedAt,
    updatedAt,
  });
}

function writerDetachedSessionRow({
  options,
  result,
  operationRevision,
  updatedAt = FINAL,
}) {
  const writerEpoch =
    result.outcome === "writer-fenced"
      ? result.writerEpoch
      : result.lease.fencingEpoch;
  return sessionRow({
    sessionId: options.expectedSession.sessionId,
    revision: (
      BigInt(options.expectedSession.revision) +
      BigInt(operationRevision) +
      1n
    ).toString(),
    sessionDocument: document(options.expectedSession.sessionId, {
      ...structuredClone(options.expectedSession.document),
      lifecycle: "DETACHED",
      writerEpoch,
      lease: null,
      attachment: null,
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

function writerBlockedResult({
  options,
  lease,
  attachment,
  writerEpoch,
  reason = "provider-outcome-unresolved",
}) {
  const fenceTarget =
    options.kind === WRITER_ATTACHMENT_ACQUIRE_OPERATION_KIND
      ? {
          attachmentId: derivedAttachmentId(options.operationId),
          kind: "attachment",
        }
      : structuredClone(options.request.target);
  return {
    resultVersion: 1,
    outcome: "writer-blocked",
    reason,
    writerEpoch,
    lease: structuredClone(lease),
    attachment: structuredClone(attachment),
    fenceTarget,
  };
}

function writerBlockedSessionRow({
  options,
  result,
  updatedAt = EXPIRED_FINALIZE_NOW,
}) {
  return sessionRow({
    sessionId: options.expectedSession.sessionId,
    revision: (
      BigInt(options.expectedSession.revision) + 4n
    ).toString(),
    sessionDocument: document(options.expectedSession.sessionId, {
      ...structuredClone(options.expectedSession.document),
      lifecycle: "BLOCKED",
      writerEpoch: result.writerEpoch,
      lease: structuredClone(result.lease),
      attachment: structuredClone(result.attachment),
      activeOperation: null,
      lastOperation: terminalPointer({
        options,
        operationRevision: "3",
        result,
      }),
    }),
    createdAt: options.expectedSession.createdAt,
    updatedAt,
  });
}

function priorWriterTerminalSteps(fixture) {
  return [
    rows(fixture.committedOperation),
    rows(fixture.releasedReservation),
  ];
}

function activeWriterSteps({
  createdAt,
  fixture,
  options,
  session,
  state,
  updatedAt,
}) {
  return [
    rows(session),
    rows(operationRow(state, { createdAt, options, updatedAt })),
    rows(reservationRow(state, { createdAt, options, updatedAt })),
    ...priorWriterTerminalSteps(fixture),
  ];
}

function anchoredDetachedEpochFixture(writerEpoch) {
  const priorExpectedSession = sessionSnapshot({
    revision: "2",
    sessionDocument: document(SESSION_ID, {
      writerEpoch,
      lastOperation: lastOperation(),
    }),
  });
  const priorOptions = reserveOptions({
    expectedSession: priorExpectedSession,
    operationId: "high-epoch-anchor-operation",
  });
  const result = cancellationResult();
  const committedOperation = writerTerminalOperationRow({
    options: priorOptions,
    result,
    revision: "1",
    updatedAt: LATEST,
  });
  const releasedReservation = reservationRow("released", {
    options: priorOptions,
    updatedAt: LATEST,
    releasedAt: LATEST,
  });
  const session = sessionRow({
    revision: "4",
    sessionDocument: document(SESSION_ID, {
      writerEpoch,
      lastOperation: terminalPointer({
        options: priorOptions,
        operationRevision: "1",
        result,
      }),
    }),
    createdAt: priorExpectedSession.createdAt,
    updatedAt: LATEST,
  });
  return {
    committedOperation,
    expectedSession: snapshotFromSessionRow(session),
    releasedReservation,
    session,
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

function checkpointCaptureFixture() {
  const writer = writerAcquiredFixture();
  const checkpoint = {
    artifactId: ARTIFACT_ID,
    backendId: writer.expectedSession.document.storageRef.backendId,
    checkpointClass: "clean",
    checkpointId: CHECKPOINT_ID,
    codexSessionId:
      writer.expectedSession.document.manifest.codex.sessionId,
    codexThreadId:
      writer.expectedSession.document.manifest.codex.rootThreadId,
    contractVersion: 1,
    createdAt: CAPTURE_PREPARED_NOW,
    imageDigest:
      writer.expectedSession.document.manifest.runtime.imageDigest,
    sessionId: writer.expectedSession.sessionId,
    sourceFencingEpoch: writer.lease.fencingEpoch,
    storageId: writer.expectedSession.document.storageRef.storageId,
  };
  const mutationRequest = {
    backendId: checkpoint.backendId,
    contractVersion: 1,
    fencingEpoch: writer.lease.fencingEpoch,
    holderId: writer.lease.holderId,
    leaseId: writer.lease.leaseId,
    operation: "checkpoint",
    operationId: CAPTURE_OPERATION_ID,
    sessionId: writer.expectedSession.sessionId,
    storageId: checkpoint.storageId,
    target: {
      artifactId: ARTIFACT_ID,
      checkpointId: CHECKPOINT_ID,
      kind: "checkpoint",
    },
  };
  const admission = {
    attachment: structuredClone(
      writer.expectedSession.document.attachment,
    ),
    captureAttemptId: CAPTURE_ATTEMPT_ID,
    checkpoint,
    processIncarnationId: PROCESS_INCARNATION_ID,
    request: mutationRequest,
    stopOperationId: STOP_OPERATION_ID,
    writerIncarnationId: WRITER_INCARNATION_ID,
  };
  const request = createCheckpointCaptureOperationRequest({
    admission,
    expectedSession: writer.expectedSession,
  });
  const options = {
    expectedSession: writer.expectedSession,
    kind: CHECKPOINT_CAPTURE_OPERATION_KIND,
    operationId: CAPTURE_OPERATION_ID,
    request,
  };
  const artifactManifestDigest = "b".repeat(64);
  const modeledDigest = "c".repeat(64);
  const completion = {
    artifactProof: {
      artifactManifestDigest,
      captureOperationId: CAPTURE_OPERATION_ID,
      modeledDigest,
    },
    materialization: {
      artifactManifestDigest,
      contractVersion: 2,
      modeledDigest,
      publicationId: "checkpoint-publication-001",
      publicationKind: "checkpoint-artifact",
      stagedRoot: {
        filesystemId: "filesystem-001",
        objectIdentityScheme: "test-object-id-v1",
        objectId: "test-object-001",
      },
      treeIdentityDigest: "d".repeat(64),
    },
    replayed: false,
    result: request.predeterminedResult,
  };
  return {
    admission,
    checkpoint,
    completion,
    mutationRequest,
    options,
    request,
    writer,
  };
}

function checkpointCatalogueDocument(fixture) {
  return {
    artifactProof: structuredClone(fixture.completion.artifactProof),
    contractVersion: 1,
    materialization: structuredClone(fixture.completion.materialization),
    result: structuredClone(fixture.completion.result),
  };
}

function checkpointCaptureBinding(fixture) {
  const { options, request } = fixture;
  return {
    attachmentId: request.admission.attachment.attachmentId,
    attachmentOperationId: request.admission.attachment.operationId,
    attachmentProofId: request.admission.attachment.proofId,
    captureAttemptId: request.admission.captureAttemptId,
    checkpoint: request.admission.checkpoint,
    contractVersion: 2,
    processIncarnationId: request.admission.processIncarnationId,
    reservationId: operationBinding(options).reservationId,
    stopOperationId: request.admission.stopOperationId,
    writerIncarnationId: request.admission.writerIncarnationId,
  };
}

function checkpointCaptureAttemptRow(
  fixture,
  { binding = checkpointCaptureBinding(fixture) } = {},
) {
  return {
    binding: structuredClone(binding),
    capture_attempt_id: CAPTURE_ATTEMPT_ID,
    claimed_at: new Date(CAPTURE_DISPATCH_NOW),
    operation_id: CAPTURE_OPERATION_ID,
    session_id: SESSION_ID,
  };
}

function checkpointCaptureTombstoneRow(fixture) {
  return {
    capture_attempt_id: CAPTURE_ATTEMPT_ID,
    operation_id: CAPTURE_OPERATION_ID,
    retired_at: new Date(CAPTURE_FINALIZE_NOW),
    session_id: SESSION_ID,
    tombstone: {
      contractVersion: 1,
      reason: "administratively-retired",
    },
  };
}

function checkpointCatalogueRow(
  fixture,
  { document: catalogueDocument = checkpointCatalogueDocument(fixture) } = {},
) {
  return {
    capture_attempt_id: CAPTURE_ATTEMPT_ID,
    checkpoint_id: CHECKPOINT_ID,
    committed_at: new Date(CAPTURE_FINALIZE_NOW),
    document: structuredClone(catalogueDocument),
    session_id: SESSION_ID,
  };
}

function checkpointCaptureTerminalResult(fixture) {
  const catalogueDocument = checkpointCatalogueDocument(fixture);
  return {
    captureAttemptId: CAPTURE_ATTEMPT_ID,
    catalogueSha256: sha256(
      JSON.stringify(catalogueDocument),
    ),
    checkpointId: CHECKPOINT_ID,
    outcome: "checkpoint-captured",
    resultVersion: 1,
  };
}

function checkpointCaptureOperationRow(
  fixture,
  state,
  {
    result =
      state === "committed"
        ? checkpointCaptureTerminalResult(fixture)
        : null,
    revision =
      state === "prepared"
        ? "0"
        : state === "starting"
          ? "1"
          : state === "uncertain"
            ? "2"
            : "3",
    updatedAt =
      state === "prepared"
        ? CAPTURE_PREPARED_NOW
        : state === "starting"
          ? CAPTURE_DISPATCH_NOW
          : state === "uncertain"
            ? CAPTURE_UNCERTAIN_NOW
            : CAPTURE_FINALIZE_NOW,
  } = {},
) {
  return operationRow(state, {
    options: fixture.options,
    revision,
    createdAt: CAPTURE_PREPARED_NOW,
    updatedAt,
    result,
    retiredAt: state === "committed" ? updatedAt : null,
  });
}

function checkpointCaptureReservationRow(
  fixture,
  state,
  {
    updatedAt =
      state === "prepared"
        ? CAPTURE_PREPARED_NOW
        : state === "starting"
          ? CAPTURE_DISPATCH_NOW
          : state === "uncertain"
            ? CAPTURE_UNCERTAIN_NOW
            : CAPTURE_FINALIZE_NOW,
  } = {},
) {
  return reservationRow(state, {
    options: fixture.options,
    createdAt: CAPTURE_PREPARED_NOW,
    updatedAt,
    releasedAt: state === "released" ? updatedAt : null,
  });
}

function checkpointCapturePhaseSessionRow(fixture, state) {
  const operationRevision =
    state === "prepared" ? "0" : state === "starting" ? "1" : "2";
  const updatedAt =
    state === "prepared"
      ? CAPTURE_PREPARED_NOW
      : state === "starting"
        ? CAPTURE_DISPATCH_NOW
        : CAPTURE_UNCERTAIN_NOW;
  return sessionRow({
    sessionId: SESSION_ID,
    revision: (
      BigInt(fixture.options.expectedSession.revision) +
      BigInt(operationRevision) +
      1n
    ).toString(),
    sessionDocument: document(SESSION_ID, {
      ...structuredClone(fixture.options.expectedSession.document),
      activeOperation: activeOperation(state, {
        options: fixture.options,
        operationRevision,
      }),
    }),
    createdAt: fixture.options.expectedSession.createdAt,
    updatedAt,
  });
}

function checkpointCaptureCommittedSessionRow(
  fixture,
  { operationRevision = "3" } = {},
) {
  const result = checkpointCaptureTerminalResult(fixture);
  return sessionRow({
    sessionId: SESSION_ID,
    revision: (
      BigInt(fixture.options.expectedSession.revision) +
      BigInt(operationRevision) +
      1n
    ).toString(),
    sessionDocument: document(SESSION_ID, {
      ...structuredClone(fixture.options.expectedSession.document),
      activeOperation: null,
      lastOperation: terminalPointer({
        options: fixture.options,
        operationRevision,
        result,
      }),
    }),
    createdAt: fixture.options.expectedSession.createdAt,
    updatedAt: CAPTURE_FINALIZE_NOW,
  });
}

function checkpointCaptureAttemptRecord(
  fixture,
  state = "authorized",
) {
  return {
    binding: checkpointCaptureBinding(fixture),
    captureAttemptId: CAPTURE_ATTEMPT_ID,
    contractVersion: 1,
    operationId: CAPTURE_OPERATION_ID,
    request: fixture.request.admission.request,
    result: fixture.request.predeterminedResult,
    state,
  };
}

function checkpointCaptureActiveSteps(fixture, state) {
  const attempt =
    state === "prepared" ? null : checkpointCaptureAttemptRow(fixture);
  const steps = [
    rows(checkpointCapturePhaseSessionRow(fixture, state)),
    rows(checkpointCaptureOperationRow(fixture, state)),
    rows(checkpointCaptureReservationRow(fixture, state)),
    attempt === null ? rows() : rows(attempt),
    rows(),
  ];
  if (attempt !== null) steps.push(rows());
  steps.push(
    rows(fixture.writer.committedOperation),
    rows(fixture.writer.releasedReservation),
  );
  return steps;
}

function checkpointCaptureCommittedSteps(
  fixture,
  {
    catalogue = checkpointCatalogueRow(fixture),
    operationRevision = "3",
    tombstone = null,
  } = {},
) {
  return [
    rows(
      checkpointCaptureCommittedSessionRow(fixture, {
        operationRevision,
      }),
    ),
    rows({ operation_count: 0, reservation_count: 0 }),
    rows(
      checkpointCaptureOperationRow(fixture, "committed", {
        revision: operationRevision,
      }),
    ),
    rows(checkpointCaptureReservationRow(fixture, "released")),
    rows(checkpointCaptureAttemptRow(fixture)),
    tombstone === null ? rows() : rows(tombstone),
    rows(catalogue),
  ];
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
  const anchor = anchoredDetachedEpochFixture(writerEpoch);
  const expectedSession = anchor.expectedSession;
  const options = writerAcquireOptions({ expectedSession });
  const binding = operationBinding(options);
  const preparedSession = writerLifecyclePhaseSessionRow("prepared", {
    options,
    lifecycle: "DETACHED",
    writerEpoch,
    updatedAt: LATEST,
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
          createdAt: LATEST,
          updatedAt: LATEST,
        }),
      ),
      rows(
        reservationRow("prepared", {
          options,
          createdAt: LATEST,
          updatedAt: LATEST,
        }),
      ),
      ...priorWriterTerminalSteps(anchor),
      rows(
        operationRow("starting", {
          options,
          createdAt: LATEST,
          updatedAt: FINAL,
        }),
      ),
      rows(
        reservationRow("starting", {
          options,
          createdAt: LATEST,
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

test("exact-bound attachment proof finalizes atomically after lease expiry", async () => {
  const options = writerAcquireOptions();
  const lease = writerLease(options);
  const rootPath = `/${"a".repeat(4_095)}`;
  const result = writerAttachmentResult(options, lease, {
    attachment: writerAttachment(options, lease, { rootPath }),
    mutationResult: writerMutationResult(options, lease, { rootPath }),
  });
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
  assert.equal(Buffer.byteLength(rootPath, "utf8"), 4_096);
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

test("all typed operation kinds and generic dispatch bypasses reject invalid input before PostgreSQL", async () => {
  const { authority, pool } = authorityWithScripts();
  const acquire = writerAcquireOptions();
  const fixture = writerAcquiredFixture();
  const capture = checkpointCaptureFixture();
  const release = writerReleaseOptions(fixture);
  const fence = writerForceFenceOptions(fixture);
  const fenceEpoch = (
    BigInt(fence.expectedSession.document.writerEpoch) + 1n
  ).toString();
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
      authority.claimOperationDispatch({
        ...release,
        expectedOperationRevision: "0",
      }),
    () =>
      authority.claimOperationDispatch({
        ...fence,
        expectedOperationRevision: "0",
      }),
    () =>
      authority.claimOperationDispatch({
        ...capture.options,
        expectedOperationRevision: "0",
      }),
    () =>
      authority.claimWriterReleaseDispatch({
        ...release,
        expectedOperationRevision: "0",
        extra: true,
      }),
    () =>
      authority.claimWriterForceFenceDispatch({
        ...fence,
        expectedOperationRevision: "0",
        extra: true,
      }),
    () =>
      authority.finalizeWriterRelease({
        ...release,
        expectedOperationRevision: "0",
        mutationResult: writerReleaseMutationResult(release),
      }),
    () =>
      authority.finalizeWriterForceFence({
        ...fence,
        expectedOperationRevision: "0",
        fenceResult: writerForceFenceProof(fence, fenceEpoch),
      }),
    () =>
      authority.finalizeWriterOperationBlocked({
        ...release,
        expectedOperationRevision: "2",
        reason: "fence-unavailable",
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

test("writer attachment finalization bounds provider paths before PostgreSQL", async () => {
  const { authority, pool } = authorityWithScripts();
  const options = writerAcquireOptions();
  const lease = writerLease(options);
  const result = writerAttachmentResult(options, lease);
  const asciiOversizedPath = `/${"a".repeat(4_096)}`;
  const utf8OversizedPath = `/${"\u00e9".repeat(2_048)}`;
  const byteLengthDescriptor = Object.getOwnPropertyDescriptor(
    Buffer,
    "byteLength",
  );
  const charCodeAtDescriptor = Object.getOwnPropertyDescriptor(
    String.prototype,
    "charCodeAt",
  );
  const structuredCloneDescriptor = Object.getOwnPropertyDescriptor(
    globalThis,
    "structuredClone",
  );
  const originalPathResolve = path.resolve;
  let poisonedByteLengthCalls = 0;
  let poisonedCharCodeAtCalls = 0;
  let poisonedCloneCalls = 0;
  let poisonedPathResolveCalls = 0;
  const errors = [];
  const cases = [
    {
      attachment: {
        ...result.attachment,
        rootPath: asciiOversizedPath,
      },
      mutationResult: result.mutationResult,
    },
    {
      attachment: {
        ...result.attachment,
        rootPath: utf8OversizedPath,
      },
      mutationResult: result.mutationResult,
    },
    {
      attachment: result.attachment,
      mutationResult: {
        ...result.mutationResult,
        rootPath: asciiOversizedPath,
      },
    },
    {
      attachment: result.attachment,
      mutationResult: {
        ...result.mutationResult,
        rootPath: utf8OversizedPath,
      },
    },
    {
      attachment: {
        ...result.attachment,
        rootPath: "/var/lib/portable-codex/session-001\0substituted",
      },
      mutationResult: result.mutationResult,
    },
    {
      attachment: result.attachment,
      mutationResult: {
        ...result.mutationResult,
        rootPath: "/var/lib/portable-codex/session-001\0substituted",
      },
    },
    {
      attachment: {
        ...result.attachment,
        rootPath: "/var/lib/portable-codex/../etc",
      },
      mutationResult: result.mutationResult,
    },
    {
      attachment: result.attachment,
      mutationResult: {
        ...result.mutationResult,
        rootPath: "/var/lib/portable-codex/../etc",
      },
    },
  ];

  try {
    Object.defineProperty(Buffer, "byteLength", {
      ...byteLengthDescriptor,
      value() {
        poisonedByteLengthCalls += 1;
        return 0;
      },
    });
    Object.defineProperty(String.prototype, "charCodeAt", {
      ...charCodeAtDescriptor,
      value() {
        poisonedCharCodeAtCalls += 1;
        return 47;
      },
    });
    Object.defineProperty(globalThis, "structuredClone", {
      ...structuredCloneDescriptor,
      value() {
        poisonedCloneCalls += 1;
        return null;
      },
    });
    path.resolve = (value) => {
      poisonedPathResolveCalls += 1;
      return value;
    };
    syncBuiltinESMExports();
    for (const providerEvidence of cases) {
      try {
        await authority.finalizeWriterAttachment({
          ...options,
          expectedOperationRevision: "1",
          ...providerEvidence,
        });
        errors.push(null);
      } catch (error) {
        errors.push(error);
      }
    }
  } finally {
    Object.defineProperty(Buffer, "byteLength", byteLengthDescriptor);
    Object.defineProperty(
      String.prototype,
      "charCodeAt",
      charCodeAtDescriptor,
    );
    Object.defineProperty(
      globalThis,
      "structuredClone",
      structuredCloneDescriptor,
    );
    path.resolve = originalPathResolve;
    syncBuiltinESMExports();
  }

  assert.equal(asciiOversizedPath.length, 4_097);
  assert.equal(Buffer.byteLength(utf8OversizedPath, "utf8"), 4_097);
  assert.equal(poisonedByteLengthCalls, 0);
  assert.equal(poisonedCharCodeAtCalls, 0);
  assert.equal(poisonedCloneCalls, 0);
  assert(poisonedPathResolveCalls > 0);
  assert.equal(errors.length, cases.length);
  for (const error of errors) {
    assert.ok(error instanceof PostgresSessionAuthorityError);
    assert.equal(error.code, "invalid_operation_request");
  }
  assert.equal(pool.connectCalls, 0);
});

test("writer finalization rejects a generic v1 attach result without rootPath", async () => {
  const { authority, pool } = authorityWithScripts();
  const options = writerAcquireOptions();
  const lease = writerLease(options);
  const result = writerAttachmentResult(options, lease);
  const legacyMutationResult = { ...result.mutationResult };
  delete legacyMutationResult.rootPath;

  await assertAuthorityError(
    authority.finalizeWriterAttachment({
      ...options,
      expectedOperationRevision: "1",
      attachment: result.attachment,
      mutationResult: legacyMutationResult,
    }),
    { code: "invalid_operation_request" },
  );
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

test("writer release dispatch grants one expired-lease detach request and replays evidence", async () => {
  const fixture = writerAcquiredFixture();
  const options = writerReleaseOptions(fixture);
  const preparedSession = writerReleasePhaseSessionRow("prepared", {
    options,
    updatedAt: WRITER_PREPARED_NOW,
  });
  const startingSession = writerReleasePhaseSessionRow("starting", {
    options,
    updatedAt: WRITER_DISPATCH_NOW,
  });
  const startingOperation = operationRow("starting", {
    options,
    createdAt: WRITER_PREPARED_NOW,
    updatedAt: WRITER_DISPATCH_NOW,
  });
  const startingReservation = reservationRow("starting", {
    options,
    createdAt: WRITER_PREPARED_NOW,
    updatedAt: WRITER_DISPATCH_NOW,
  });
  const { authority, clients } = authorityWithScripts(
    {
      options: { now: WRITER_DISPATCH_NOW },
      steps: [
        ...activeWriterSteps({
          createdAt: WRITER_PREPARED_NOW,
          fixture,
          options,
          session: preparedSession,
          state: "prepared",
          updatedAt: WRITER_PREPARED_NOW,
        }),
        rows(startingOperation),
        rows(startingReservation),
        rows(startingSession),
      ],
    },
    activeWriterSteps({
      createdAt: WRITER_PREPARED_NOW,
      fixture,
      options,
      session: startingSession,
      state: "starting",
      updatedAt: WRITER_DISPATCH_NOW,
    }),
  );

  const dispatched = await authority.claimWriterReleaseDispatch({
    ...options,
    expectedOperationRevision: "0",
  });
  const replay = await authority.claimWriterReleaseDispatch({
    ...options,
    expectedOperationRevision: "0",
  });

  assert.equal(
    Date.parse(fixture.lease.expiresAt) < Date.parse(WRITER_DISPATCH_NOW),
    true,
  );
  assert.equal(dispatched.dispatchGranted, true);
  assert.equal(replay.dispatchGranted, false);
  assert.equal(dispatched.session.document.lifecycle, "RELEASING");
  assert.equal(
    dispatched.session.document.writerEpoch,
    fixture.lease.fencingEpoch,
  );
  assert.deepEqual(dispatched.lease, fixture.lease);
  assert.deepEqual(
    dispatched.mutationRequest,
    writerReleaseMutationRequest(options),
  );
  assert.deepEqual(replay.mutationRequest, dispatched.mutationRequest);
  assert.equal(Object.hasOwn(dispatched, "fenceRequest"), false);
  assert.deepEqual(authorityQueries(clients[0]), [
    extendedQuery(`${READ_SESSION_QUERY} FOR UPDATE`, [SESSION_ID]),
    extendedQuery(`${READ_OPERATION_QUERY} FOR UPDATE`, [
      OTHER_OPERATION_ID,
    ]),
    extendedQuery(`${READ_RESERVATION_QUERY} FOR UPDATE`, [
      OTHER_OPERATION_ID,
    ]),
    extendedQuery(READ_OPERATION_QUERY, [OPERATION_ID]),
    extendedQuery(READ_RESERVATION_QUERY, [OPERATION_ID]),
    extendedQuery(START_OPERATION_QUERY, [
      OTHER_OPERATION_ID,
      "0",
      WRITER_DISPATCH_NOW,
    ]),
    extendedQuery(START_RESERVATION_QUERY, [
      OTHER_OPERATION_ID,
      WRITER_DISPATCH_NOW,
    ]),
    extendedQuery(UPDATE_SESSION_QUERY, [
      SESSION_ID,
      preparedSession.revision,
      JSON.stringify(startingSession.document),
      WRITER_DISPATCH_NOW,
    ]),
  ]);
  assert.equal(
    clients
      .flatMap((client) => authorityQueries(client))
      .filter((args) => queryText(args) === START_OPERATION_QUERY).length,
    1,
  );
  for (const client of clients) client.assertExhausted();
});

test("starting and uncertain release proofs detach atomically and replay the terminal anchor", async () => {
  const fixture = writerAcquiredFixture();
  const options = writerReleaseOptions(fixture);
  const mutationResult = writerReleaseMutationResult(options);
  const result = writerReleaseResult(options, mutationResult);
  const cases = [
    {
      expectedOperationRevision: "1",
      operationRevision: "2",
      state: "starting",
      stateUpdatedAt: WRITER_DISPATCH_NOW,
    },
    {
      expectedOperationRevision: "2",
      operationRevision: "3",
      state: "uncertain",
      stateUpdatedAt: WRITER_UNCERTAIN_NOW,
    },
  ];
  const scripts = [];
  const expected = [];

  for (const candidate of cases) {
    const activeSession = writerReleasePhaseSessionRow(candidate.state, {
      options,
      updatedAt: candidate.stateUpdatedAt,
    });
    const committedOperation = writerTerminalOperationRow({
      createdAt: WRITER_PREPARED_NOW,
      options,
      result,
      revision: candidate.operationRevision,
      updatedAt: WRITER_FINALIZE_NOW,
    });
    const releasedReservation = reservationRow("released", {
      options,
      createdAt: WRITER_PREPARED_NOW,
      updatedAt: WRITER_FINALIZE_NOW,
      releasedAt: WRITER_FINALIZE_NOW,
    });
    const detachedSession = writerDetachedSessionRow({
      options,
      result,
      operationRevision: candidate.operationRevision,
      updatedAt: WRITER_FINALIZE_NOW,
    });
    scripts.push(
      {
        options: { now: WRITER_FINALIZE_NOW },
        steps: [
          ...activeWriterSteps({
            createdAt: WRITER_PREPARED_NOW,
            fixture,
            options,
            session: activeSession,
            state: candidate.state,
            updatedAt: candidate.stateUpdatedAt,
          }),
          rows(committedOperation),
          rows(releasedReservation),
          rows(detachedSession),
        ],
      },
      [
        rows(detachedSession),
        rows({ operation_count: 0, reservation_count: 0 }),
        rows(committedOperation),
        rows(releasedReservation),
      ],
    );
    expected.push({
      ...candidate,
      activeSessionRevision: activeSession.revision,
      committedOperation,
      detachedSession,
      releasedReservation,
    });
  }
  const { authority, clients } = authorityWithScripts(...scripts);

  for (const [index, candidate] of expected.entries()) {
    const input = {
      ...options,
      expectedOperationRevision:
        candidate.expectedOperationRevision,
      mutationResult,
    };
    const finalized = await authority.finalizeWriterRelease(input);
    const replay = await authority.finalizeWriterRelease(input);

    assert.equal(finalized.finalized, true);
    assert.equal(replay.finalized, false);
    assert.equal(finalized.operation.revision, candidate.operationRevision);
    assert.deepEqual(finalized.operation.result, result);
    assert.deepEqual(replay.operation.result, result);
    assert.equal(finalized.session.document.lifecycle, "DETACHED");
    assert.equal(finalized.session.document.lease, null);
    assert.equal(finalized.session.document.attachment, null);
    assert.equal(
      finalized.session.document.writerEpoch,
      fixture.lease.fencingEpoch,
    );
    assert.deepEqual(
      finalized.session.document.lastOperation,
      terminalPointer({
        options,
        operationRevision: candidate.operationRevision,
        result,
      }),
    );
    assert.deepEqual(
      replay.session,
      snapshotFromSessionRow(candidate.detachedSession),
    );
    assert.equal(
      authorityQueries(clients[index * 2]).some(
        (args) => queryText(args) === READ_AUTHORITY_CLOCK_QUERY,
      ),
      false,
    );
    assert.deepEqual(
      authorityQueries(clients[index * 2]).slice(-3),
      [
        extendedQuery(COMMIT_ACTIVE_OPERATION_QUERY, [
          OTHER_OPERATION_ID,
          candidate.expectedOperationRevision,
          JSON.stringify(result),
          WRITER_FINALIZE_NOW,
          candidate.state,
        ]),
        extendedQuery(RELEASE_ACTIVE_RESERVATION_QUERY, [
          OTHER_OPERATION_ID,
          WRITER_FINALIZE_NOW,
          candidate.state,
        ]),
        extendedQuery(UPDATE_SESSION_QUERY, [
          SESSION_ID,
          candidate.activeSessionRevision,
          JSON.stringify(candidate.detachedSession.document),
          WRITER_FINALIZE_NOW,
        ]),
      ],
    );
  }
  assert.equal(
    Date.parse(fixture.lease.expiresAt) < Date.parse(WRITER_FINALIZE_NOW),
    true,
  );
  for (const client of clients) client.assertExhausted();
});

test("force-fence dispatch advances the epoch once and replays one independent fence request", async () => {
  const fixture = writerAcquiredFixture();
  const options = writerForceFenceOptions(fixture);
  const writerEpoch = (
    BigInt(fixture.expectedSession.document.writerEpoch) + 1n
  ).toString();
  const preparedSession = writerForceFencePhaseSessionRow("prepared", {
    options,
    updatedAt: WRITER_PREPARED_NOW,
  });
  const startingSession = writerForceFencePhaseSessionRow("starting", {
    options,
    writerEpoch,
    updatedAt: WRITER_DISPATCH_NOW,
  });
  const startingOperation = operationRow("starting", {
    options,
    createdAt: WRITER_PREPARED_NOW,
    updatedAt: WRITER_DISPATCH_NOW,
  });
  const startingReservation = reservationRow("starting", {
    options,
    createdAt: WRITER_PREPARED_NOW,
    updatedAt: WRITER_DISPATCH_NOW,
  });
  const expectedFenceRequest = writerForceFenceRequest(
    options,
    writerEpoch,
  );
  const { authority, clients } = authorityWithScripts(
    {
      options: { now: WRITER_DISPATCH_NOW },
      steps: [
        ...activeWriterSteps({
          createdAt: WRITER_PREPARED_NOW,
          fixture,
          options,
          session: preparedSession,
          state: "prepared",
          updatedAt: WRITER_PREPARED_NOW,
        }),
        rows(startingOperation),
        rows(startingReservation),
        rows(startingSession),
      ],
    },
    activeWriterSteps({
      createdAt: WRITER_PREPARED_NOW,
      fixture,
      options,
      session: startingSession,
      state: "starting",
      updatedAt: WRITER_DISPATCH_NOW,
    }),
  );

  const dispatched = await authority.claimWriterForceFenceDispatch({
    ...options,
    expectedOperationRevision: "0",
  });
  const replay = await authority.claimWriterForceFenceDispatch({
    ...options,
    expectedOperationRevision: "0",
  });

  assert.equal(dispatched.dispatchGranted, true);
  assert.equal(replay.dispatchGranted, false);
  assert.equal(dispatched.writerEpoch, writerEpoch);
  assert.equal(replay.writerEpoch, writerEpoch);
  assert.equal(dispatched.session.document.lifecycle, "FENCING");
  assert.equal(dispatched.session.document.writerEpoch, writerEpoch);
  assert.deepEqual(dispatched.fenceRequest, expectedFenceRequest);
  assert.deepEqual(replay.fenceRequest, expectedFenceRequest);
  assert.notStrictEqual(dispatched.fenceRequest, options.request);
  assert.equal(Object.hasOwn(dispatched, "mutationRequest"), false);
  assert.deepEqual(authorityQueries(clients[0]).slice(-3), [
    extendedQuery(START_OPERATION_QUERY, [
      OTHER_OPERATION_ID,
      "0",
      WRITER_DISPATCH_NOW,
    ]),
    extendedQuery(START_RESERVATION_QUERY, [
      OTHER_OPERATION_ID,
      WRITER_DISPATCH_NOW,
    ]),
    extendedQuery(UPDATE_SESSION_QUERY, [
      SESSION_ID,
      preparedSession.revision,
      JSON.stringify(startingSession.document),
      WRITER_DISPATCH_NOW,
    ]),
  ]);
  assert.equal(
    clients
      .flatMap((client) => authorityQueries(client))
      .filter((args) => queryText(args) === START_OPERATION_QUERY).length,
    1,
  );
  for (const client of clients) client.assertExhausted();
});

test("an exact force-fence proof detaches the writer and replays the same epoch anchor", async () => {
  const fixture = writerAcquiredFixture();
  const options = writerForceFenceOptions(fixture);
  const writerEpoch = (
    BigInt(fixture.expectedSession.document.writerEpoch) + 1n
  ).toString();
  const fenceResult = writerForceFenceProof(options, writerEpoch);
  const result = writerForceFenceResult(
    options,
    writerEpoch,
    fenceResult,
  );
  const startingSession = writerForceFencePhaseSessionRow("starting", {
    options,
    writerEpoch,
    updatedAt: WRITER_DISPATCH_NOW,
  });
  const committedOperation = writerTerminalOperationRow({
    createdAt: WRITER_PREPARED_NOW,
    options,
    result,
    revision: "2",
    updatedAt: WRITER_FINALIZE_NOW,
  });
  const releasedReservation = reservationRow("released", {
    options,
    createdAt: WRITER_PREPARED_NOW,
    updatedAt: WRITER_FINALIZE_NOW,
    releasedAt: WRITER_FINALIZE_NOW,
  });
  const detachedSession = writerDetachedSessionRow({
    options,
    result,
    operationRevision: "2",
    updatedAt: WRITER_FINALIZE_NOW,
  });
  const { authority, clients } = authorityWithScripts(
    {
      options: { now: WRITER_FINALIZE_NOW },
      steps: [
        ...activeWriterSteps({
          createdAt: WRITER_PREPARED_NOW,
          fixture,
          options,
          session: startingSession,
          state: "starting",
          updatedAt: WRITER_DISPATCH_NOW,
        }),
        rows(committedOperation),
        rows(releasedReservation),
        rows(detachedSession),
      ],
    },
    [
      rows(detachedSession),
      rows({ operation_count: 0, reservation_count: 0 }),
      rows(committedOperation),
      rows(releasedReservation),
    ],
  );
  const input = {
    ...options,
    expectedOperationRevision: "1",
    fenceResult,
  };

  const finalized = await authority.finalizeWriterForceFence(input);
  const replay = await authority.finalizeWriterForceFence(input);

  assert.equal(finalized.finalized, true);
  assert.equal(replay.finalized, false);
  assert.deepEqual(finalized.operation.result, result);
  assert.deepEqual(replay.operation.result, result);
  assert.equal(finalized.session.document.lifecycle, "DETACHED");
  assert.equal(finalized.session.document.writerEpoch, writerEpoch);
  assert.equal(finalized.session.document.lease, null);
  assert.equal(finalized.session.document.attachment, null);
  assert.deepEqual(
    finalized.session.document.lastOperation,
    terminalPointer({
      options,
      operationRevision: "2",
      result,
    }),
  );
  assert.deepEqual(
    authorityQueries(clients[0]).slice(-3),
    [
      extendedQuery(COMMIT_ACTIVE_OPERATION_QUERY, [
        OTHER_OPERATION_ID,
        "1",
        JSON.stringify(result),
        WRITER_FINALIZE_NOW,
        "starting",
      ]),
      extendedQuery(RELEASE_ACTIVE_RESERVATION_QUERY, [
        OTHER_OPERATION_ID,
        WRITER_FINALIZE_NOW,
        "starting",
      ]),
      extendedQuery(UPDATE_SESSION_QUERY, [
        SESSION_ID,
        startingSession.revision,
        JSON.stringify(detachedSession.document),
        WRITER_FINALIZE_NOW,
      ]),
    ],
  );
  for (const client of clients) client.assertExhausted();
});

test("manual-fencing backends cannot turn a claimed force-fence into success", async () => {
  const fixture = writerAcquiredFixture({
    capabilities: backendCapabilities({ fencing: "manual" }),
  });
  const options = writerForceFenceOptions(fixture);
  const writerEpoch = (
    BigInt(fixture.expectedSession.document.writerEpoch) + 1n
  ).toString();
  const { authority, pool } = authorityWithScripts();

  await assertAuthorityError(
    authority.finalizeWriterForceFence({
      ...options,
      expectedOperationRevision: "1",
      fenceResult: writerForceFenceProof(options, writerEpoch),
    }),
    { code: "writer_fence_unsupported" },
  );
  assert.equal(pool.connectCalls, 0);
});

test("uncertain acquire release and force-fence operations finalize BLOCKED with exact retained evidence", async () => {
  const acquireOptions = writerAcquireOptions();
  const acquireLease = writerLease(acquireOptions);
  const releaseFixture = writerAcquiredFixture();
  const releaseOptions = writerReleaseOptions(releaseFixture);
  const fenceFixture = writerAcquiredFixture();
  const fenceOptions = writerForceFenceOptions(fenceFixture);
  const fenceEpoch = (
    BigInt(fenceOptions.expectedSession.document.writerEpoch) + 1n
  ).toString();
  const scenarios = [
    {
      attachment: null,
      fixture: null,
      lease: acquireLease,
      options: acquireOptions,
      reason: "provider-outcome-unresolved",
      session: writerUncertainSessionRow({
        options: acquireOptions,
        lease: acquireLease,
        updatedAt: WRITER_UNCERTAIN_NOW,
      }),
      writerEpoch: acquireLease.fencingEpoch,
      createdAt: LATER,
    },
    {
      attachment: releaseFixture.result.attachment,
      fixture: releaseFixture,
      lease: releaseFixture.lease,
      options: releaseOptions,
      reason: "provider-outcome-unresolved",
      session: writerReleasePhaseSessionRow("uncertain", {
        options: releaseOptions,
        updatedAt: WRITER_UNCERTAIN_NOW,
      }),
      writerEpoch: releaseFixture.lease.fencingEpoch,
      createdAt: WRITER_PREPARED_NOW,
    },
    {
      attachment: fenceFixture.result.attachment,
      fixture: fenceFixture,
      lease: fenceFixture.lease,
      options: fenceOptions,
      reason: "fence-unavailable",
      session: writerForceFencePhaseSessionRow("uncertain", {
        options: fenceOptions,
        writerEpoch: fenceEpoch,
        updatedAt: WRITER_UNCERTAIN_NOW,
      }),
      writerEpoch: fenceEpoch,
      createdAt: WRITER_PREPARED_NOW,
    },
  ];
  const scripts = [];

  for (const scenario of scenarios) {
    scenario.result = writerBlockedResult({
      options: scenario.options,
      lease: scenario.lease,
      attachment: scenario.attachment,
      writerEpoch: scenario.writerEpoch,
      reason: scenario.reason,
    });
    scenario.committedOperation = writerTerminalOperationRow({
      createdAt: scenario.createdAt,
      options: scenario.options,
      result: scenario.result,
      revision: "3",
      updatedAt: WRITER_FINALIZE_NOW,
    });
    scenario.releasedReservation = reservationRow("released", {
      options: scenario.options,
      createdAt: scenario.createdAt,
      updatedAt: WRITER_FINALIZE_NOW,
      releasedAt: WRITER_FINALIZE_NOW,
    });
    scenario.blockedSession = writerBlockedSessionRow({
      options: scenario.options,
      result: scenario.result,
      updatedAt: WRITER_FINALIZE_NOW,
    });
    const activeSteps =
      scenario.fixture === null
        ? [
            rows(scenario.session),
            rows(
              operationRow("uncertain", {
                options: scenario.options,
                createdAt: scenario.createdAt,
                updatedAt: WRITER_UNCERTAIN_NOW,
              }),
            ),
            rows(
              reservationRow("uncertain", {
                options: scenario.options,
                createdAt: scenario.createdAt,
                updatedAt: WRITER_UNCERTAIN_NOW,
              }),
            ),
          ]
        : activeWriterSteps({
            createdAt: scenario.createdAt,
            fixture: scenario.fixture,
            options: scenario.options,
            session: scenario.session,
            state: "uncertain",
            updatedAt: WRITER_UNCERTAIN_NOW,
          });
    scripts.push(
      {
        options: { now: WRITER_FINALIZE_NOW },
        steps: [
          ...activeSteps,
          rows(scenario.committedOperation),
          rows(scenario.releasedReservation),
          rows(scenario.blockedSession),
        ],
      },
      [
        rows(scenario.blockedSession),
        rows({ operation_count: 0, reservation_count: 0 }),
        rows(scenario.committedOperation),
        rows(scenario.releasedReservation),
      ],
    );
  }
  const { authority, clients } = authorityWithScripts(...scripts);

  for (const [index, scenario] of scenarios.entries()) {
    const input = {
      ...scenario.options,
      expectedOperationRevision: "2",
      reason: scenario.reason,
    };
    const finalized =
      await authority.finalizeWriterOperationBlocked(input);
    const replay =
      await authority.finalizeWriterOperationBlocked(input);

    assert.equal(finalized.finalized, true);
    assert.equal(replay.finalized, false);
    assert.deepEqual(finalized.operation.result, scenario.result);
    assert.deepEqual(replay.operation.result, scenario.result);
    assert.equal(finalized.operation.revision, "3");
    assert.equal(finalized.session.document.lifecycle, "BLOCKED");
    assert.equal(
      finalized.session.document.writerEpoch,
      scenario.writerEpoch,
    );
    assert.deepEqual(finalized.session.document.lease, scenario.lease);
    assert.deepEqual(
      finalized.session.document.attachment,
      scenario.attachment,
    );
    assert.deepEqual(
      finalized.operation.result.fenceTarget,
      scenario.result.fenceTarget,
    );
    assert.deepEqual(
      finalized.session.document.lastOperation,
      terminalPointer({
        options: scenario.options,
        operationRevision: "3",
        result: scenario.result,
      }),
    );
    assert.deepEqual(
      replay.session,
      snapshotFromSessionRow(scenario.blockedSession),
    );
    assert.deepEqual(
      authorityQueries(clients[index * 2]).slice(-3),
      [
        extendedQuery(COMMIT_ACTIVE_OPERATION_QUERY, [
          scenario.options.operationId,
          "2",
          JSON.stringify(scenario.result),
          WRITER_FINALIZE_NOW,
          "uncertain",
        ]),
        extendedQuery(RELEASE_ACTIVE_RESERVATION_QUERY, [
          scenario.options.operationId,
          WRITER_FINALIZE_NOW,
          "uncertain",
        ]),
        extendedQuery(UPDATE_SESSION_QUERY, [
          SESSION_ID,
          scenario.session.revision,
          JSON.stringify(scenario.blockedSession.document),
          WRITER_FINALIZE_NOW,
        ]),
      ],
    );
  }
  for (const client of clients) client.assertExhausted();
});

test("a BLOCKED acquire retries force-fence against its anchored target and advances one epoch", async () => {
  const blockedOptions = writerAcquireOptions();
  const blockedLease = writerLease(blockedOptions);
  const blockedResult = writerBlockedResult({
    options: blockedOptions,
    lease: blockedLease,
    attachment: null,
    writerEpoch: blockedLease.fencingEpoch,
  });
  const blockedOperation = writerTerminalOperationRow({
    options: blockedOptions,
    result: blockedResult,
    revision: "3",
    updatedAt: WRITER_FINALIZE_NOW,
  });
  const blockedReservation = reservationRow("released", {
    options: blockedOptions,
    updatedAt: WRITER_FINALIZE_NOW,
    releasedAt: WRITER_FINALIZE_NOW,
  });
  const blockedSession = writerBlockedSessionRow({
    options: blockedOptions,
    result: blockedResult,
    updatedAt: WRITER_FINALIZE_NOW,
  });
  const blockedFixture = {
    committedOperation: blockedOperation,
    expectedSession: snapshotFromSessionRow(blockedSession),
    fenceTarget: blockedResult.fenceTarget,
    releasedReservation: blockedReservation,
    result: blockedResult,
  };
  const options = writerForceFenceOptions(blockedFixture);
  const writerEpoch = (
    BigInt(blockedResult.writerEpoch) + 1n
  ).toString();
  const preparedSession = writerForceFencePhaseSessionRow("prepared", {
    options,
    updatedAt: WRITER_RETRY_PREPARED_NOW,
  });
  const startingSession = writerForceFencePhaseSessionRow("starting", {
    options,
    writerEpoch,
    updatedAt: WRITER_RETRY_DISPATCH_NOW,
  });
  const startingOperation = operationRow("starting", {
    options,
    createdAt: WRITER_RETRY_PREPARED_NOW,
    updatedAt: WRITER_RETRY_DISPATCH_NOW,
  });
  const startingReservation = reservationRow("starting", {
    options,
    createdAt: WRITER_RETRY_PREPARED_NOW,
    updatedAt: WRITER_RETRY_DISPATCH_NOW,
  });
  const { authority, clients } = authorityWithScripts({
    options: { now: WRITER_RETRY_DISPATCH_NOW },
    steps: [
      ...activeWriterSteps({
        createdAt: WRITER_RETRY_PREPARED_NOW,
        fixture: blockedFixture,
        options,
        session: preparedSession,
        state: "prepared",
        updatedAt: WRITER_RETRY_PREPARED_NOW,
      }),
      rows(startingOperation),
      rows(startingReservation),
      rows(startingSession),
    ],
  });

  const receipt = await authority.claimWriterForceFenceDispatch({
    ...options,
    expectedOperationRevision: "0",
  });

  assert.equal(options.expectedSession.document.lifecycle, "BLOCKED");
  assert.equal(options.expectedSession.document.attachment, null);
  assert.equal(receipt.dispatchGranted, true);
  assert.equal(receipt.writerEpoch, writerEpoch);
  assert.equal(receipt.session.document.lifecycle, "FENCING");
  assert.equal(receipt.session.document.writerEpoch, writerEpoch);
  assert.deepEqual(receipt.fenceRequest.target, blockedResult.fenceTarget);
  assert.deepEqual(
    receipt.fenceRequest.revokedFence,
    {
      fencingEpoch: blockedLease.fencingEpoch,
      holderId: blockedLease.holderId,
      leaseId: blockedLease.leaseId,
    },
  );
  assert.deepEqual(
    authorityQueries(clients[0]).slice(0, 5),
    [
      extendedQuery(`${READ_SESSION_QUERY} FOR UPDATE`, [SESSION_ID]),
      extendedQuery(`${READ_OPERATION_QUERY} FOR UPDATE`, [
        OTHER_OPERATION_ID,
      ]),
      extendedQuery(`${READ_RESERVATION_QUERY} FOR UPDATE`, [
        OTHER_OPERATION_ID,
      ]),
      extendedQuery(READ_OPERATION_QUERY, [OPERATION_ID]),
      extendedQuery(READ_RESERVATION_QUERY, [OPERATION_ID]),
    ],
  );
  clients[0].assertExhausted();
});

test("BLOCKED force-fence target substitution and uint64 exhaustion fail before PostgreSQL", async () => {
  const blockedOptions = writerAcquireOptions();
  const blockedLease = writerLease(blockedOptions);
  const blockedResult = writerBlockedResult({
    options: blockedOptions,
    lease: blockedLease,
    attachment: null,
    writerEpoch: blockedLease.fencingEpoch,
  });
  const blockedSession = writerBlockedSessionRow({
    options: blockedOptions,
    result: blockedResult,
  });
  const fixture = {
    expectedSession: snapshotFromSessionRow(blockedSession),
    fenceTarget: blockedResult.fenceTarget,
    result: blockedResult,
  };
  const options = writerForceFenceOptions(fixture);
  const exhaustedExpectedSession = structuredClone(
    fixture.expectedSession,
  );
  exhaustedExpectedSession.document.writerEpoch =
    "18446744073709551615";
  const { authority, pool } = authorityWithScripts();

  await assertAuthorityError(
    authority.reserveOperation({
      ...options,
      request: {
        ...options.request,
        target: {
          ...options.request.target,
          attachmentId: "substituted-attachment",
        },
      },
    }),
    { code: "invalid_operation_request" },
  );
  await assertAuthorityError(
    authority.reserveOperation({
      ...options,
      expectedSession: exhaustedExpectedSession,
    }),
    { code: "writer_epoch_exhausted" },
  );
  assert.equal(pool.connectCalls, 0);
});

test("release and force-fence proof target tuple and terminal-result mismatches fail closed", async () => {
  const fixture = writerAcquiredFixture();
  const release = writerReleaseOptions(fixture);
  const fence = writerForceFenceOptions(fixture);
  const fenceEpoch = (
    BigInt(fence.expectedSession.document.writerEpoch) + 1n
  ).toString();
  const releaseStarting = writerReleasePhaseSessionRow("starting", {
    options: release,
    updatedAt: WRITER_DISPATCH_NOW,
  });
  const fenceStarting = writerForceFencePhaseSessionRow("starting", {
    options: fence,
    writerEpoch: fenceEpoch,
    updatedAt: WRITER_DISPATCH_NOW,
  });
  const expectedFenceProof = writerForceFenceProof(fence, fenceEpoch);
  const mismatchCases = [
    {
      invoke(authority) {
        return authority.finalizeWriterRelease({
          ...release,
          expectedOperationRevision: "1",
          mutationResult: writerReleaseMutationResult(release, {
            target: {
              ...release.request.target,
              attachmentId: "substituted-attachment",
            },
          }),
        });
      },
      options: release,
      session: releaseStarting,
    },
    {
      invoke(authority) {
        return authority.finalizeWriterRelease({
          ...release,
          expectedOperationRevision: "1",
          mutationResult: writerReleaseMutationResult(release, {
            holderId: "substituted-holder",
          }),
        });
      },
      options: release,
      session: releaseStarting,
    },
    {
      invoke(authority) {
        return authority.finalizeWriterForceFence({
          ...fence,
          expectedOperationRevision: "1",
          fenceResult: writerForceFenceProof(fence, fenceEpoch, {
            target: {
              ...fence.request.target,
              attachmentId: "substituted-attachment",
            },
          }),
        });
      },
      options: fence,
      session: fenceStarting,
    },
    {
      invoke(authority) {
        return authority.finalizeWriterForceFence({
          ...fence,
          expectedOperationRevision: "1",
          fenceResult: writerForceFenceProof(fence, fenceEpoch, {
            revokedFence: {
              ...expectedFenceProof.revokedFence,
              leaseId: "substituted-lease",
            },
          }),
        });
      },
      options: fence,
      session: fenceStarting,
    },
  ];
  const releaseMutationResult = writerReleaseMutationResult(release);
  const releaseResult = writerReleaseResult(
    release,
    releaseMutationResult,
  );
  const releaseCommitted = writerTerminalOperationRow({
    createdAt: WRITER_PREPARED_NOW,
    options: release,
    result: releaseResult,
    revision: "2",
    updatedAt: WRITER_FINALIZE_NOW,
  });
  const releaseReservation = reservationRow("released", {
    options: release,
    createdAt: WRITER_PREPARED_NOW,
    updatedAt: WRITER_FINALIZE_NOW,
    releasedAt: WRITER_FINALIZE_NOW,
  });
  const releaseDetached = writerDetachedSessionRow({
    options: release,
    result: releaseResult,
    operationRevision: "2",
    updatedAt: WRITER_FINALIZE_NOW,
  });
  const { authority, clients } = authorityWithScripts(
    ...mismatchCases.map((candidate) =>
      activeWriterSteps({
        createdAt: WRITER_PREPARED_NOW,
        fixture,
        options: candidate.options,
        session: candidate.session,
        state: "starting",
        updatedAt: WRITER_DISPATCH_NOW,
      }),
    ),
    [
      rows(releaseDetached),
      rows({ operation_count: 0, reservation_count: 0 }),
      rows(releaseCommitted),
      rows(releaseReservation),
    ],
  );

  for (const [index, candidate] of mismatchCases.entries()) {
    await assertAuthorityError(candidate.invoke(authority), {
      code: "invalid_operation_request",
    });
    assert.equal(
      authorityQueries(clients[index]).some((args) =>
        /^(?:INSERT|UPDATE) /u.test(queryText(args)),
      ),
      false,
    );
    clients[index].assertExhausted();
  }
  await assertAuthorityError(
    authority.finalizeWriterRelease({
      ...release,
      expectedOperationRevision: "1",
      mutationResult: {
        ...releaseMutationResult,
        proofId: "different-terminal-proof",
      },
    }),
    { code: "operation_result_conflict" },
  );
  assert.equal(
    authorityQueries(clients.at(-1)).some((args) =>
      /^(?:INSERT|UPDATE) /u.test(queryText(args)),
    ),
    false,
  );
  clients.at(-1).assertExhausted();
});

test("late release and force-fence successes cannot replace writer-blocked terminal anchors", async () => {
  const releaseFixture = writerAcquiredFixture();
  const releaseOptions = writerReleaseOptions(releaseFixture);
  const fenceFixture = writerAcquiredFixture();
  const fenceOptions = writerForceFenceOptions(fenceFixture);
  const fenceEpoch = (
    BigInt(fenceOptions.expectedSession.document.writerEpoch) + 1n
  ).toString();
  const scenarios = [
    {
      attachment: releaseFixture.result.attachment,
      fixture: releaseFixture,
      invoke(authority) {
        return authority.finalizeWriterRelease({
          ...releaseOptions,
          expectedOperationRevision: "2",
          mutationResult: writerReleaseMutationResult(releaseOptions),
        });
      },
      options: releaseOptions,
      reason: "provider-outcome-unresolved",
      writerEpoch: releaseFixture.lease.fencingEpoch,
    },
    {
      attachment: fenceFixture.result.attachment,
      fixture: fenceFixture,
      invoke(authority) {
        return authority.finalizeWriterForceFence({
          ...fenceOptions,
          expectedOperationRevision: "2",
          fenceResult: writerForceFenceProof(
            fenceOptions,
            fenceEpoch,
          ),
        });
      },
      options: fenceOptions,
      reason: "fence-unavailable",
      writerEpoch: fenceEpoch,
    },
  ];
  const scripts = [];

  for (const scenario of scenarios) {
    scenario.result = writerBlockedResult({
      options: scenario.options,
      lease: scenario.fixture.lease,
      attachment: scenario.attachment,
      writerEpoch: scenario.writerEpoch,
      reason: scenario.reason,
    });
    scenario.operation = writerTerminalOperationRow({
      createdAt: WRITER_PREPARED_NOW,
      options: scenario.options,
      result: scenario.result,
      revision: "3",
      updatedAt: WRITER_FINALIZE_NOW,
    });
    scenario.reservation = reservationRow("released", {
      options: scenario.options,
      createdAt: WRITER_PREPARED_NOW,
      updatedAt: WRITER_FINALIZE_NOW,
      releasedAt: WRITER_FINALIZE_NOW,
    });
    scenario.session = writerBlockedSessionRow({
      options: scenario.options,
      result: scenario.result,
      updatedAt: WRITER_FINALIZE_NOW,
    });
    scripts.push([
      rows(scenario.session),
      rows({ operation_count: 0, reservation_count: 0 }),
      rows(scenario.operation),
      rows(scenario.reservation),
    ]);
  }
  const { authority, clients } = authorityWithScripts(...scripts);

  for (const [index, scenario] of scenarios.entries()) {
    await assertAuthorityError(scenario.invoke(authority), {
      code: "operation_transition_conflict",
    });
    assert.equal(
      authorityQueries(clients[index]).some((args) =>
        /^(?:INSERT|UPDATE) /u.test(queryText(args)),
      ),
      false,
    );
    clients[index].assertExhausted();
  }
});

test("manual force-fence uncertainty terminalizes BLOCKED at the advanced epoch", async () => {
  const fixture = writerAcquiredFixture({
    capabilities: backendCapabilities({ fencing: "manual" }),
  });
  const options = writerForceFenceOptions(fixture);
  const writerEpoch = (
    BigInt(options.expectedSession.document.writerEpoch) + 1n
  ).toString();
  const preparedSession = writerForceFencePhaseSessionRow("prepared", {
    options,
    updatedAt: WRITER_PREPARED_NOW,
  });
  const startingOperation = operationRow("starting", {
    options,
    createdAt: WRITER_PREPARED_NOW,
    updatedAt: WRITER_DISPATCH_NOW,
  });
  const startingReservation = reservationRow("starting", {
    options,
    createdAt: WRITER_PREPARED_NOW,
    updatedAt: WRITER_DISPATCH_NOW,
  });
  const startingSession = writerForceFencePhaseSessionRow("starting", {
    options,
    writerEpoch,
    updatedAt: WRITER_DISPATCH_NOW,
  });
  const uncertainOperation = operationRow("uncertain", {
    options,
    createdAt: WRITER_PREPARED_NOW,
    updatedAt: WRITER_UNCERTAIN_NOW,
  });
  const uncertainReservation = reservationRow("uncertain", {
    options,
    createdAt: WRITER_PREPARED_NOW,
    updatedAt: WRITER_UNCERTAIN_NOW,
  });
  const uncertainSession = writerForceFencePhaseSessionRow("uncertain", {
    options,
    writerEpoch,
    updatedAt: WRITER_UNCERTAIN_NOW,
  });
  const result = writerBlockedResult({
    options,
    lease: fixture.lease,
    attachment: fixture.result.attachment,
    writerEpoch,
    reason: "fence-unavailable",
  });
  const committedOperation = writerTerminalOperationRow({
    createdAt: WRITER_PREPARED_NOW,
    options,
    result,
    revision: "3",
    updatedAt: WRITER_FINALIZE_NOW,
  });
  const releasedReservation = reservationRow("released", {
    options,
    createdAt: WRITER_PREPARED_NOW,
    updatedAt: WRITER_FINALIZE_NOW,
    releasedAt: WRITER_FINALIZE_NOW,
  });
  const blockedSession = writerBlockedSessionRow({
    options,
    result,
    updatedAt: WRITER_FINALIZE_NOW,
  });
  const { authority, clients } = authorityWithScripts(
    {
      options: { now: WRITER_DISPATCH_NOW },
      steps: [
        ...activeWriterSteps({
          createdAt: WRITER_PREPARED_NOW,
          fixture,
          options,
          session: preparedSession,
          state: "prepared",
          updatedAt: WRITER_PREPARED_NOW,
        }),
        rows(startingOperation),
        rows(startingReservation),
        rows(startingSession),
      ],
    },
    {
      options: { now: WRITER_UNCERTAIN_NOW },
      steps: [
        ...activeWriterSteps({
          createdAt: WRITER_PREPARED_NOW,
          fixture,
          options,
          session: startingSession,
          state: "starting",
          updatedAt: WRITER_DISPATCH_NOW,
        }),
        rows(uncertainOperation),
        rows(uncertainReservation),
        rows(uncertainSession),
      ],
    },
    {
      options: { now: WRITER_FINALIZE_NOW },
      steps: [
        ...activeWriterSteps({
          createdAt: WRITER_PREPARED_NOW,
          fixture,
          options,
          session: uncertainSession,
          state: "uncertain",
          updatedAt: WRITER_UNCERTAIN_NOW,
        }),
        rows(committedOperation),
        rows(releasedReservation),
        rows(blockedSession),
      ],
    },
  );

  const dispatched = await authority.claimWriterForceFenceDispatch({
    ...options,
    expectedOperationRevision: "0",
  });
  const uncertain = await authority.markOperationUncertain({
    ...options,
    expectedOperationRevision: "1",
  });
  const finalized = await authority.finalizeWriterOperationBlocked({
    ...options,
    expectedOperationRevision: "2",
    reason: "fence-unavailable",
  });

  assert.equal(
    options.expectedSession.document.backendCapabilities.fencing,
    "manual",
  );
  assert.equal(dispatched.dispatchGranted, true);
  assert.equal(dispatched.writerEpoch, writerEpoch);
  assert.equal(uncertain.changed, true);
  assert.equal(uncertain.session.document.lifecycle, "FENCING");
  assert.equal(uncertain.session.document.writerEpoch, writerEpoch);
  assert.equal(finalized.finalized, true);
  assert.equal(finalized.session.document.lifecycle, "BLOCKED");
  assert.equal(finalized.session.document.writerEpoch, writerEpoch);
  assert.deepEqual(finalized.session.document.lease, fixture.lease);
  assert.deepEqual(
    finalized.session.document.attachment,
    fixture.result.attachment,
  );
  assert.deepEqual(finalized.operation.result, result);
  for (const client of clients) client.assertExhausted();
});

test("a failed force-fence can reserve and claim one anchored retry with one more epoch", async () => {
  const fixture = writerAcquiredFixture();
  const failedOptions = writerForceFenceOptions(fixture);
  const failedEpoch = (
    BigInt(failedOptions.expectedSession.document.writerEpoch) + 1n
  ).toString();
  const failedResult = writerBlockedResult({
    options: failedOptions,
    lease: fixture.lease,
    attachment: fixture.result.attachment,
    writerEpoch: failedEpoch,
    reason: "fence-unavailable",
  });
  const failedOperation = writerTerminalOperationRow({
    createdAt: WRITER_PREPARED_NOW,
    options: failedOptions,
    result: failedResult,
    revision: "3",
    updatedAt: WRITER_FINALIZE_NOW,
  });
  const failedReservation = reservationRow("released", {
    options: failedOptions,
    createdAt: WRITER_PREPARED_NOW,
    updatedAt: WRITER_FINALIZE_NOW,
    releasedAt: WRITER_FINALIZE_NOW,
  });
  const blockedSession = writerBlockedSessionRow({
    options: failedOptions,
    result: failedResult,
    updatedAt: WRITER_FINALIZE_NOW,
  });
  const blockedFixture = {
    committedOperation: failedOperation,
    expectedSession: snapshotFromSessionRow(blockedSession),
    fenceTarget: failedResult.fenceTarget,
    releasedReservation: failedReservation,
    result: failedResult,
  };
  const options = writerForceFenceOptions(blockedFixture, {
    operationId: "operation-003",
  });
  const retryEpoch = (BigInt(failedEpoch) + 1n).toString();
  const preparedOperation = operationRow("prepared", {
    options,
    createdAt: WRITER_RETRY_PREPARED_NOW,
    updatedAt: WRITER_RETRY_PREPARED_NOW,
  });
  const preparedReservation = reservationRow("prepared", {
    options,
    createdAt: WRITER_RETRY_PREPARED_NOW,
    updatedAt: WRITER_RETRY_PREPARED_NOW,
  });
  const preparedSession = writerForceFencePhaseSessionRow("prepared", {
    options,
    updatedAt: WRITER_RETRY_PREPARED_NOW,
  });
  const startingOperation = operationRow("starting", {
    options,
    createdAt: WRITER_RETRY_PREPARED_NOW,
    updatedAt: WRITER_RETRY_DISPATCH_NOW,
  });
  const startingReservation = reservationRow("starting", {
    options,
    createdAt: WRITER_RETRY_PREPARED_NOW,
    updatedAt: WRITER_RETRY_DISPATCH_NOW,
  });
  const startingSession = writerForceFencePhaseSessionRow("starting", {
    options,
    writerEpoch: retryEpoch,
    updatedAt: WRITER_RETRY_DISPATCH_NOW,
  });
  const { authority, clients } = authorityWithScripts(
    {
      options: { now: WRITER_RETRY_PREPARED_NOW },
      steps: [
        rows(blockedSession),
        rows({ operation_count: 0, reservation_count: 0 }),
        rows(failedOperation),
        rows(failedReservation),
        rows(),
        rows(preparedOperation),
        rows(preparedReservation),
        rows(preparedSession),
      ],
    },
    {
      options: { now: WRITER_RETRY_DISPATCH_NOW },
      steps: [
        ...activeWriterSteps({
          createdAt: WRITER_RETRY_PREPARED_NOW,
          fixture: blockedFixture,
          options,
          session: preparedSession,
          state: "prepared",
          updatedAt: WRITER_RETRY_PREPARED_NOW,
        }),
        rows(startingOperation),
        rows(startingReservation),
        rows(startingSession),
      ],
    },
    activeWriterSteps({
      createdAt: WRITER_RETRY_PREPARED_NOW,
      fixture: blockedFixture,
      options,
      session: startingSession,
      state: "starting",
      updatedAt: WRITER_RETRY_DISPATCH_NOW,
    }),
  );

  const reserved = await authority.reserveOperation(options);
  const dispatched = await authority.claimWriterForceFenceDispatch({
    ...options,
    expectedOperationRevision: "0",
  });
  const replay = await authority.claimWriterForceFenceDispatch({
    ...options,
    expectedOperationRevision: "0",
  });

  assert.equal(reserved.acquired, true);
  assert.equal(reserved.session.document.lifecycle, "BLOCKED");
  assert.equal(reserved.session.document.writerEpoch, failedEpoch);
  assert.equal(dispatched.dispatchGranted, true);
  assert.equal(replay.dispatchGranted, false);
  assert.equal(dispatched.writerEpoch, retryEpoch);
  assert.equal(replay.writerEpoch, retryEpoch);
  assert.equal(
    BigInt(dispatched.writerEpoch),
    BigInt(failedResult.writerEpoch) + 1n,
  );
  assert.deepEqual(
    dispatched.fenceRequest.target,
    failedResult.fenceTarget,
  );
  assert.deepEqual(replay.fenceRequest, dispatched.fenceRequest);
  assert.deepEqual(dispatched.fenceRequest.revokedFence, {
    fencingEpoch: fixture.lease.fencingEpoch,
    holderId: fixture.lease.holderId,
    leaseId: fixture.lease.leaseId,
  });
  assert.equal(
    clients
      .flatMap((client) => authorityQueries(client))
      .filter((args) => queryText(args) === START_OPERATION_QUERY).length,
    1,
  );
  for (const client of clients) client.assertExhausted();
});

test("BLOCKED force-fence reserve requires capacity for its full terminal path", async () => {
  const fixture = writerAcquiredFixture();
  const highExpectedSession = structuredClone(fixture.expectedSession);
  highExpectedSession.revision = "9223372036854775800";
  highExpectedSession.document.lastOperation.expectedSessionRevision =
    "9223372036854775797";
  const highFixture = {
    ...fixture,
    expectedSession: highExpectedSession,
  };
  const failedOptions = writerForceFenceOptions(highFixture);
  const failedEpoch = (
    BigInt(failedOptions.expectedSession.document.writerEpoch) + 1n
  ).toString();
  const failedResult = writerBlockedResult({
    options: failedOptions,
    lease: fixture.lease,
    attachment: fixture.result.attachment,
    writerEpoch: failedEpoch,
    reason: "fence-unavailable",
  });
  const failedOperation = writerTerminalOperationRow({
    createdAt: WRITER_PREPARED_NOW,
    options: failedOptions,
    result: failedResult,
    revision: "3",
    updatedAt: WRITER_FINALIZE_NOW,
  });
  const failedReservation = reservationRow("released", {
    options: failedOptions,
    createdAt: WRITER_PREPARED_NOW,
    updatedAt: WRITER_FINALIZE_NOW,
    releasedAt: WRITER_FINALIZE_NOW,
  });
  const blockedSession = writerBlockedSessionRow({
    options: failedOptions,
    result: failedResult,
    updatedAt: WRITER_FINALIZE_NOW,
  });
  const blockedFixture = {
    expectedSession: snapshotFromSessionRow(blockedSession),
    fenceTarget: failedResult.fenceTarget,
    result: failedResult,
  };
  const options = writerForceFenceOptions(blockedFixture, {
    operationId: "operation-003",
  });
  const { authority, clients } = authorityWithScripts([
    rows(blockedSession),
    rows({ operation_count: 0, reservation_count: 0 }),
    rows(failedOperation),
    rows(failedReservation),
    rows(),
  ]);

  await assertAuthorityError(authority.reserveOperation(options), {
    code: "session_revision_exhausted",
  });

  assert.deepEqual(
    authorityQueries(clients[0]).map(queryText),
    [
      `${READ_SESSION_QUERY} FOR UPDATE`,
      READ_ACTIVE_COUNTS_QUERY,
      READ_OPERATION_QUERY,
      READ_RESERVATION_QUERY,
      READ_OPERATION_QUERY,
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

test("uncertain force-fence accepts one exact proof and rejects a different replay proof", async () => {
  const fixture = writerAcquiredFixture();
  const options = writerForceFenceOptions(fixture);
  const writerEpoch = (
    BigInt(options.expectedSession.document.writerEpoch) + 1n
  ).toString();
  const fenceResult = writerForceFenceProof(options, writerEpoch);
  const result = writerForceFenceResult(
    options,
    writerEpoch,
    fenceResult,
  );
  const uncertainSession = writerForceFencePhaseSessionRow("uncertain", {
    options,
    writerEpoch,
    updatedAt: WRITER_UNCERTAIN_NOW,
  });
  const committedOperation = writerTerminalOperationRow({
    createdAt: WRITER_PREPARED_NOW,
    options,
    result,
    revision: "3",
    updatedAt: WRITER_FINALIZE_NOW,
  });
  const releasedReservation = reservationRow("released", {
    options,
    createdAt: WRITER_PREPARED_NOW,
    updatedAt: WRITER_FINALIZE_NOW,
    releasedAt: WRITER_FINALIZE_NOW,
  });
  const detachedSession = writerDetachedSessionRow({
    options,
    result,
    operationRevision: "3",
    updatedAt: WRITER_FINALIZE_NOW,
  });
  const { authority, clients } = authorityWithScripts(
    {
      options: { now: WRITER_FINALIZE_NOW },
      steps: [
        ...activeWriterSteps({
          createdAt: WRITER_PREPARED_NOW,
          fixture,
          options,
          session: uncertainSession,
          state: "uncertain",
          updatedAt: WRITER_UNCERTAIN_NOW,
        }),
        rows(committedOperation),
        rows(releasedReservation),
        rows(detachedSession),
      ],
    },
    [
      rows(detachedSession),
      rows({ operation_count: 0, reservation_count: 0 }),
      rows(committedOperation),
      rows(releasedReservation),
    ],
  );

  const finalized = await authority.finalizeWriterForceFence({
    ...options,
    expectedOperationRevision: "2",
    fenceResult,
  });
  await assertAuthorityError(
    authority.finalizeWriterForceFence({
      ...options,
      expectedOperationRevision: "2",
      fenceResult: {
        ...fenceResult,
        proofId: "different-terminal-proof",
      },
    }),
    { code: "operation_result_conflict" },
  );

  assert.equal(finalized.finalized, true);
  assert.equal(finalized.operation.revision, "3");
  assert.equal(finalized.session.document.lifecycle, "DETACHED");
  assert.equal(finalized.session.document.writerEpoch, writerEpoch);
  assert.equal(
    authorityQueries(clients[1]).some((args) =>
      /^(?:INSERT|UPDATE) /u.test(queryText(args)),
    ),
    false,
  );
  for (const client of clients) client.assertExhausted();
});

test("checkpoint capture request builder owns the exact predetermined proof", () => {
  const fixture = checkpointCaptureFixture();
  const replay = createCheckpointCaptureOperationRequest({
    admission: fixture.admission,
    expectedSession: fixture.options.expectedSession,
  });

  assert.equal(
    CHECKPOINT_CAPTURE_OPERATION_KIND,
    "checkpoint-capture-v1",
  );
  assert.deepEqual(replay, fixture.request);
  assert.equal(
    replay.predeterminedResult.mutation.proofId,
    `proof-checkpoint-${sha256(
      `checkpoint-capture-proof:${CAPTURE_OPERATION_ID}`,
    )}`,
  );
  assert.deepEqual(
    replay.predeterminedResult.checkpoint,
    canonicalPayload(fixture.checkpoint),
  );
  assertDeepFrozen(replay);
});

test("checkpoint capture dispatch, source-free reconciliation, uncertain finalize, and exact replay share one SQL authority", async () => {
  const fixture = checkpointCaptureFixture();
  const preparedOperation = checkpointCaptureOperationRow(
    fixture,
    "prepared",
  );
  const preparedReservation = checkpointCaptureReservationRow(
    fixture,
    "prepared",
  );
  const preparedSession = checkpointCapturePhaseSessionRow(
    fixture,
    "prepared",
  );
  const startingOperation = checkpointCaptureOperationRow(
    fixture,
    "starting",
  );
  const startingReservation = checkpointCaptureReservationRow(
    fixture,
    "starting",
  );
  const startingSession = checkpointCapturePhaseSessionRow(
    fixture,
    "starting",
  );
  const uncertainOperation = checkpointCaptureOperationRow(
    fixture,
    "uncertain",
  );
  const uncertainReservation = checkpointCaptureReservationRow(
    fixture,
    "uncertain",
  );
  const uncertainSession = checkpointCapturePhaseSessionRow(
    fixture,
    "uncertain",
  );
  const catalogue = checkpointCatalogueRow(fixture);
  const committedOperation = checkpointCaptureOperationRow(
    fixture,
    "committed",
  );
  const releasedReservation = checkpointCaptureReservationRow(
    fixture,
    "released",
  );
  const committedSession = checkpointCaptureCommittedSessionRow(
    fixture,
  );
  const { authority, clients } = authorityWithScripts(
    {
      options: { now: CAPTURE_PREPARED_NOW },
      steps: [
        rows(fixture.writer.session),
        rows({ operation_count: 0, reservation_count: 0 }),
        rows(fixture.writer.committedOperation),
        rows(fixture.writer.releasedReservation),
        rows(),
        rows(preparedOperation),
        rows(preparedReservation),
        rows(preparedSession),
      ],
    },
    {
      options: {
        authorityNow: CAPTURE_AUTHORITY_NOW,
        now: CAPTURE_DISPATCH_NOW,
      },
      steps: [
        ...checkpointCaptureActiveSteps(fixture, "prepared"),
        rows(checkpointCaptureAttemptRow(fixture)),
        rows(startingOperation),
        rows(startingReservation),
        rows(startingSession),
      ],
    },
    [
      rows(startingOperation),
      ...checkpointCaptureActiveSteps(fixture, "starting"),
    ],
    {
      options: { now: CAPTURE_UNCERTAIN_NOW },
      steps: [
        ...checkpointCaptureActiveSteps(fixture, "starting"),
        rows(uncertainOperation),
        rows(uncertainReservation),
        rows(uncertainSession),
      ],
    },
    {
      options: { now: CAPTURE_FINALIZE_NOW },
      steps: [
        ...checkpointCaptureActiveSteps(fixture, "uncertain"),
        rows(catalogue),
        rows(committedOperation),
        rows(releasedReservation),
        rows(committedSession),
      ],
    },
    checkpointCaptureCommittedSteps(fixture),
    [
      rows(catalogue),
      rows(checkpointCaptureAttemptRow(fixture)),
      rows(committedOperation),
      ...checkpointCaptureCommittedSteps(fixture),
    ],
  );

  const reserved = await authority.reserveOperation(fixture.options);
  const dispatched = await authority.claimCheckpointCaptureDispatch({
    ...fixture.options,
    expectedOperationRevision: "0",
  });
  const preparedReconciliation =
    await authority.readCheckpointCaptureAttempt({
      checkpoint: fixture.checkpoint,
      request: fixture.mutationRequest,
    });
  const uncertain = await authority.markOperationUncertain({
    ...fixture.options,
    expectedOperationRevision: "1",
  });
  const finalized = await authority.finalizeCheckpointCapture({
    ...fixture.options,
    completion: fixture.completion,
    expectedOperationRevision: "2",
  });
  const replayed = await authority.finalizeCheckpointCapture({
    ...fixture.options,
    completion: {
      ...fixture.completion,
      replayed: true,
    },
    expectedOperationRevision: "2",
  });
  const catalogueRead = await authority.readCheckpointCatalogue({
    checkpoint: fixture.checkpoint,
  });

  assert.equal(reserved.acquired, true);
  assert.equal(reserved.operation.state, "prepared");
  assert.equal(dispatched.dispatchGranted, true);
  assert.equal(dispatched.authorityNow, CAPTURE_AUTHORITY_NOW);
  assert.deepEqual(
    dispatched.attempt,
    checkpointCaptureAttemptRecord(fixture),
  );
  assert.equal(preparedReconciliation.status, "authorized");
  assert.equal(preparedReconciliation.catalogue, null);
  assert.deepEqual(
    preparedReconciliation.attempt,
    checkpointCaptureAttemptRecord(fixture),
  );
  assert.equal(uncertain.changed, true);
  assert.equal(uncertain.operation.state, "uncertain");
  assert.equal(finalized.finalized, true);
  assert.equal(finalized.operation.revision, "3");
  assert.deepEqual(
    finalized.attempt,
    checkpointCaptureAttemptRecord(fixture, "committed"),
  );
  assert.equal(
    finalized.operation.result.catalogueSha256,
    checkpointCaptureTerminalResult(fixture).catalogueSha256,
  );
  assert.equal(replayed.finalized, false);
  assert.deepEqual(replayed.catalogue, finalized.catalogue);
  assert.deepEqual(catalogueRead, {
    attempt: checkpointCaptureAttemptRecord(fixture, "committed"),
    catalogue: finalized.catalogue,
    operation: operationView(committedOperation),
  });
  assertDeepFrozen(finalized);

  const dispatchTexts = authorityQueries(clients[1]).map(queryText);
  assert.ok(
    dispatchTexts.indexOf(INSERT_CAPTURE_ATTEMPT_QUERY) <
      dispatchTexts.indexOf(START_OPERATION_QUERY),
  );
  const finalizeTexts = authorityQueries(clients[4]).map(queryText);
  assert.ok(
    finalizeTexts.indexOf(INSERT_CHECKPOINT_CATALOGUE_QUERY) <
      finalizeTexts.indexOf(COMMIT_ACTIVE_OPERATION_QUERY),
  );
  assert.equal(
    authorityQueries(clients[5]).some((args) =>
      /^(?:INSERT|UPDATE) /u.test(queryText(args)),
    ),
    false,
  );
  assert.deepEqual(
    authorityQueries(clients[6]).map(queryText).slice(0, 3),
    [
      READ_CHECKPOINT_CATALOGUE_BY_ID_QUERY,
      READ_CAPTURE_ATTEMPT_BY_ID_QUERY,
      READ_OPERATION_QUERY,
    ],
  );
  for (const client of clients) client.assertExhausted();
});

test("checkpoint finalization rejects a tombstone introduced after the reconciliation read", async () => {
  const fixture = checkpointCaptureFixture();
  const startingOperation = checkpointCaptureOperationRow(
    fixture,
    "starting",
  );
  const startingReservation = checkpointCaptureReservationRow(
    fixture,
    "starting",
  );
  const startingSession = checkpointCapturePhaseSessionRow(
    fixture,
    "starting",
  );
  const tombstone = checkpointCaptureTombstoneRow(fixture);
  const { authority, clients } = authorityWithScripts(
    [
      rows(startingOperation),
      ...checkpointCaptureActiveSteps(fixture, "starting"),
    ],
    {
      options: { now: CAPTURE_FINALIZE_NOW },
      steps: [
        rows(startingSession),
        rows(startingOperation),
        rows(startingReservation),
        rows(checkpointCaptureAttemptRow(fixture)),
        rows(tombstone),
        rows(),
      ],
    },
  );

  const authorized = await authority.readCheckpointCaptureAttempt({
    checkpoint: fixture.checkpoint,
    request: fixture.mutationRequest,
  });

  assert.equal(authorized.status, "authorized");
  await assertAuthorityError(
    authority.finalizeCheckpointCapture({
      ...fixture.options,
      completion: fixture.completion,
      expectedOperationRevision: "1",
    }),
    { code: "checkpoint_capture_not_authorized" },
  );

  const finalizationTexts = authorityQueries(clients[1]).map(queryText);
  for (const forbiddenQuery of [
    INSERT_CHECKPOINT_CATALOGUE_QUERY,
    COMMIT_ACTIVE_OPERATION_QUERY,
    RELEASE_ACTIVE_RESERVATION_QUERY,
    UPDATE_SESSION_QUERY,
  ]) {
    assert.equal(finalizationTexts.includes(forbiddenQuery), false);
  }
  assert.equal(queryTexts(clients[1]).includes("ROLLBACK"), true);
  for (const client of clients) client.assertExhausted();
});

test("checkpoint finalization rejects attempt binding drift after the reconciliation read", async () => {
  const fixture = checkpointCaptureFixture();
  const startingOperation = checkpointCaptureOperationRow(
    fixture,
    "starting",
  );
  const startingReservation = checkpointCaptureReservationRow(
    fixture,
    "starting",
  );
  const startingSession = checkpointCapturePhaseSessionRow(
    fixture,
    "starting",
  );
  const mismatchedAttempt = checkpointCaptureAttemptRow(fixture, {
    binding: {
      ...checkpointCaptureBinding(fixture),
      reservationId: "checkpoint-reservation-mismatched",
    },
  });
  const { authority, clients } = authorityWithScripts(
    [
      rows(startingOperation),
      ...checkpointCaptureActiveSteps(fixture, "starting"),
    ],
    {
      options: { now: CAPTURE_FINALIZE_NOW },
      steps: [
        rows(startingSession),
        rows(startingOperation),
        rows(startingReservation),
        rows(mismatchedAttempt),
      ],
    },
  );

  const authorized = await authority.readCheckpointCaptureAttempt({
    checkpoint: fixture.checkpoint,
    request: fixture.mutationRequest,
  });

  assert.equal(authorized.status, "authorized");
  await assertAuthorityError(
    authority.finalizeCheckpointCapture({
      ...fixture.options,
      completion: fixture.completion,
      expectedOperationRevision: "1",
    }),
    { code: "operation_state_invalid" },
  );

  const finalizationTexts = authorityQueries(clients[1]).map(queryText);
  for (const forbiddenQuery of [
    INSERT_CHECKPOINT_CATALOGUE_QUERY,
    COMMIT_ACTIVE_OPERATION_QUERY,
    RELEASE_ACTIVE_RESERVATION_QUERY,
    UPDATE_SESSION_QUERY,
  ]) {
    assert.equal(finalizationTexts.includes(forbiddenQuery), false);
  }
  assert.equal(queryTexts(clients[1]).includes("ROLLBACK"), true);
  for (const client of clients) client.assertExhausted();
});

test("checkpoint claim and catalogue collisions fail closed before either phase advances", async () => {
  const fixture = checkpointCaptureFixture();
  const differentCompletion = {
    ...fixture.completion,
    materialization: {
      ...fixture.completion.materialization,
      publicationId: "checkpoint-publication-different",
    },
    replayed: true,
  };
  const { authority, clients } = authorityWithScripts(
    {
      options: {
        authorityNow: CAPTURE_AUTHORITY_NOW,
        now: CAPTURE_DISPATCH_NOW,
      },
      steps: [
        ...checkpointCaptureActiveSteps(fixture, "prepared"),
        rows(),
      ],
    },
    {
      options: { now: CAPTURE_FINALIZE_NOW },
      steps: [
        ...checkpointCaptureActiveSteps(fixture, "uncertain"),
        rows(),
      ],
    },
    checkpointCaptureCommittedSteps(fixture),
  );

  await assertAuthorityError(
    authority.claimCheckpointCaptureDispatch({
      ...fixture.options,
      expectedOperationRevision: "0",
    }),
    { code: "checkpoint_identity_conflict" },
  );
  await assertAuthorityError(
    authority.finalizeCheckpointCapture({
      ...fixture.options,
      completion: fixture.completion,
      expectedOperationRevision: "2",
    }),
    { code: "checkpoint_identity_conflict" },
  );
  await assertAuthorityError(
    authority.finalizeCheckpointCapture({
      ...fixture.options,
      completion: differentCompletion,
      expectedOperationRevision: "2",
    }),
    { code: "operation_result_conflict" },
  );

  assert.equal(
    authorityQueries(clients[0]).at(-1)?.[0]?.text,
    INSERT_CAPTURE_ATTEMPT_QUERY,
  );
  assert.equal(
    authorityQueries(clients[1]).at(-1)?.[0]?.text,
    INSERT_CHECKPOINT_CATALOGUE_QUERY,
  );
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

test("checkpoint claim and finalize acknowledgement loss reconcile durable exact state", async () => {
  const fixture = checkpointCaptureFixture();
  const startingOperation = checkpointCaptureOperationRow(
    fixture,
    "starting",
  );
  const startingReservation = checkpointCaptureReservationRow(
    fixture,
    "starting",
  );
  const startingSession = checkpointCapturePhaseSessionRow(
    fixture,
    "starting",
  );
  const catalogue = checkpointCatalogueRow(fixture);
  const committedOperation = checkpointCaptureOperationRow(
    fixture,
    "committed",
    { revision: "2" },
  );
  const releasedReservation = checkpointCaptureReservationRow(
    fixture,
    "released",
  );
  const committedSession = checkpointCaptureCommittedSessionRow(
    fixture,
    { operationRevision: "2" },
  );
  const claimCommitError = new Error(
    "sensitive checkpoint claim acknowledgement lost",
  );
  const finalizeCommitError = new Error(
    "sensitive checkpoint finalize acknowledgement lost",
  );
  const { authority, clients, pool } = authorityWithScripts(
    {
      options: {
        authorityNow: CAPTURE_AUTHORITY_NOW,
        commitError: claimCommitError,
        now: CAPTURE_DISPATCH_NOW,
      },
      steps: [
        ...checkpointCaptureActiveSteps(fixture, "prepared"),
        rows(checkpointCaptureAttemptRow(fixture)),
        rows(startingOperation),
        rows(startingReservation),
        rows(startingSession),
      ],
    },
    [
      rows(startingOperation),
      ...checkpointCaptureActiveSteps(fixture, "starting"),
    ],
    {
      options: {
        commitError: finalizeCommitError,
        now: CAPTURE_FINALIZE_NOW,
      },
      steps: [
        ...checkpointCaptureActiveSteps(fixture, "starting"),
        rows(catalogue),
        rows(committedOperation),
        rows(releasedReservation),
        rows(committedSession),
      ],
    },
    [
      rows(catalogue),
      rows(checkpointCaptureAttemptRow(fixture)),
      rows(committedOperation),
      ...checkpointCaptureCommittedSteps(fixture, {
        operationRevision: "2",
      }),
    ],
  );

  await assert.rejects(
    authority.claimCheckpointCaptureDispatch({
      ...fixture.options,
      expectedOperationRevision: "0",
    }),
    assertStoreCommitUncertain,
  );
  const authorized = await authority.readCheckpointCaptureAttempt({
    checkpoint: fixture.checkpoint,
    request: fixture.mutationRequest,
  });
  await assert.rejects(
    authority.finalizeCheckpointCapture({
      ...fixture.options,
      completion: fixture.completion,
      expectedOperationRevision: "1",
    }),
    assertStoreCommitUncertain,
  );
  const committed = await authority.readCheckpointCatalogue({
    checkpoint: fixture.checkpoint,
  });

  assert.equal(authorized.status, "authorized");
  assert.equal(authorized.operation.state, "starting");
  assert.equal(committed.operation.state, "committed");
  assert.equal(committed.operation.revision, "2");
  assert.equal(pool.connectCalls, 4);
  for (const index of [0, 2]) {
    assert.equal(
      queryTexts(clients[index]).filter((text) => text === "COMMIT")
        .length,
      1,
    );
    assert.equal(queryTexts(clients[index]).at(-1), "ROLLBACK");
    clients[index].assertExhausted({ destroyed: true });
  }
  for (const index of [1, 3]) {
    assert.equal(
      authorityQueries(clients[index]).some((args) =>
        /^(?:INSERT|UPDATE) /u.test(queryText(args)),
      ),
      false,
    );
    clients[index].assertExhausted();
  }
});

test("checkpoint dispatch uses database time and cannot authorize at lease expiry", async () => {
  const fixture = checkpointCaptureFixture();
  const { authority, clients } = authorityWithScripts({
    options: {
      authorityNow: fixture.writer.lease.expiresAt,
      now: CAPTURE_DISPATCH_NOW,
    },
    steps: checkpointCaptureActiveSteps(fixture, "prepared"),
  });

  await assertAuthorityError(
    authority.claimCheckpointCaptureDispatch({
      ...fixture.options,
      expectedOperationRevision: "0",
    }),
    { code: "writer_lease_expired" },
  );

  assert.equal(
    authorityQueries(clients[0]).at(-1)?.[0]?.text,
    READ_AUTHORITY_CLOCK_QUERY,
  );
  assert.equal(
    authorityQueries(clients[0]).some((args) =>
      /^(?:INSERT|UPDATE) /u.test(queryText(args)),
    ),
    false,
  );
  clients[0].assertExhausted();
});

test("checkpoint typed APIs reject non-exact contracts before PostgreSQL", async () => {
  const fixture = checkpointCaptureFixture();
  const { authority, pool } = authorityWithScripts();
  const cases = [
    () =>
      authority.reserveOperation({
        ...fixture.options,
        request: { malformed: true },
      }),
    () =>
      authority.claimCheckpointCaptureDispatch({
        ...fixture.options,
        expectedOperationRevision: "0",
        extra: true,
      }),
    () =>
      authority.finalizeCheckpointCapture({
        ...fixture.options,
        completion: {
          ...fixture.completion,
          materialization: {
            ...fixture.completion.materialization,
            contractVersion: 1,
          },
        },
        expectedOperationRevision: "1",
      }),
    () =>
      authority.readCheckpointCaptureAttempt({
        checkpoint: fixture.checkpoint,
        extra: true,
        request: fixture.mutationRequest,
      }),
    () =>
      authority.readCheckpointCatalogue({
        checkpoint: fixture.checkpoint,
        extra: true,
      }),
  ];

  assert.throws(
    () =>
      createCheckpointCaptureOperationRequest({
        admission: {
          ...fixture.admission,
          extra: true,
        },
        expectedSession: fixture.options.expectedSession,
      }),
    (error) =>
      error instanceof PostgresSessionAuthorityError &&
      error.code === "invalid_operation_request",
  );
  for (const checkpointClass of ["crash-prefix", "graceful-abort"]) {
    assert.throws(
      () =>
        createCheckpointCaptureOperationRequest({
          admission: {
            ...fixture.admission,
            checkpoint: {
              ...fixture.checkpoint,
              checkpointClass,
            },
          },
          expectedSession: fixture.options.expectedSession,
        }),
      (error) =>
        error instanceof PostgresSessionAuthorityError &&
        error.code === "invalid_operation_request",
    );
  }
  for (const invoke of cases) {
    await assertAuthorityError(invoke(), {
      code: "invalid_operation_request",
    });
  }
  assert.equal(pool.connectCalls, 0);
});

test("checkpoint capture tombstones are non-authorizing read evidence", async () => {
  const fixture = checkpointCaptureFixture();
  const committedOperation = checkpointCaptureOperationRow(
    fixture,
    "committed",
  );
  const tombstone = checkpointCaptureTombstoneRow(fixture);
  const { authority, clients } = authorityWithScripts([
    rows(committedOperation),
    ...checkpointCaptureCommittedSteps(fixture, { tombstone }),
  ]);

  await assertAuthorityError(
    authority.readCheckpointCaptureAttempt({
      checkpoint: fixture.checkpoint,
      request: fixture.mutationRequest,
    }),
    { code: "checkpoint_capture_not_authorized" },
  );

  assert.equal(
    authorityQueries(clients[0]).some((args) =>
      /^(?:INSERT|UPDATE) /u.test(queryText(args)),
    ),
    false,
  );
  clients[0].assertExhausted();
});

test("checkpoint catalogue digest tampering fails strict relational readback", async () => {
  const fixture = checkpointCaptureFixture();
  const tamperedDocument = checkpointCatalogueDocument(fixture);
  tamperedDocument.materialization.publicationId =
    "checkpoint-publication-tampered";
  const tamperedCatalogue = checkpointCatalogueRow(fixture, {
    document: tamperedDocument,
  });
  const { authority, clients } = authorityWithScripts(
    checkpointCaptureCommittedSteps(fixture, {
      catalogue: tamperedCatalogue,
    }),
  );

  await assertAuthorityError(
    authority.readSession({ sessionId: SESSION_ID }),
    { code: "operation_state_invalid" },
  );

  assert.equal(
    authorityQueries(clients[0]).some((args) =>
      /^(?:INSERT|UPDATE) /u.test(queryText(args)),
    ),
    false,
  );
  clients[0].assertExhausted();
});
