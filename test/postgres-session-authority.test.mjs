import assert from "node:assert/strict";
import { createHash } from "node:crypto";
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
  WRITER_FORCE_FENCE_OPERATION_KIND,
  WRITER_LAUNCH_ATTEMPT_OPERATION_KIND,
  WRITER_RELEASE_OPERATION_KIND,
  assertSessionOperationBinding,
  assertSessionOperationTransitionProof,
  createRestoreAttachmentActivationOperationRequest,
  createRestoreAttachmentActivationOperationRequestV2,
  createRestoreDestinationGenerationOperationRequest,
  createRestoreDestinationGenerationOperationRequestV2,
  createWriterLaunchAttemptOperationRequest,
} from "../src/postgres-session-authority.mjs";
import {
  createSessionManifest,
  serializeSessionManifest,
} from "../src/session-storage-contracts.mjs";

const SESSION_ID = "019f2100-0000-7000-8000-000000000001";
const OTHER_SESSION_ID = "019f2100-0000-7000-8000-000000000002";
const IMAGE_DIGEST = `sha256:${"a".repeat(64)}`;
const NOW = "2026-07-29T12:34:56.789Z";
const OPERATION_CREATED_AT = "2026-07-29T12:35:01.000Z";
const OPERATION_STARTED_AT = "2026-07-29T12:35:02.000Z";
const OPERATION_RETIRED_AT = "2026-07-29T12:35:03.000Z";
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

function attachedSnapshot(sessionId = SESSION_ID) {
  const lease = {
    contractVersion: 1,
    expiresAt: "2026-07-29T13:34:56.789Z",
    fencingEpoch: "2",
    holderId: "host-001",
    leaseId: "lease-001",
    sessionId,
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
    rootPath: `/var/lib/portable-codex/${sessionId}`,
    sessionId,
    storageId: "volume-001",
  };
  return {
    createdAt: NOW,
    document: document({
      attachment,
      manifest: JSON.parse(serializeSessionManifest(manifest(sessionId))),
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
      storageRef: storageRef(sessionId),
      writerEpoch: lease.fencingEpoch,
    }),
    revision: "3",
    sessionId,
    updatedAt: NOW,
  };
}

function sha256Json(value) {
  return createHash("sha256")
    .update(JSON.stringify(value), "utf8")
    .digest("hex");
}

function sha256Text(value) {
  return createHash("sha256").update(value, "utf8").digest("hex");
}

function writerSupervisorStateGcFixture({
  authorizedAt = OPERATION_RETIRED_AT,
  operationId = "writer-launch-gc-001",
  sessionId = SESSION_ID,
} = {}) {
  const expectedSession = attachedSnapshot(sessionId);
  const intent = writerLaunchIntentFixture(expectedSession);
  const request = createWriterLaunchAttemptOperationRequest({
    expectedSession,
    generation: {
      binding: { marker: `binding-${sessionId}` },
      checkpointId: `checkpoint-${sessionId}`,
      claimedAt: "2026-07-29T12:34:57.000Z",
      committedAt: "2026-07-29T12:34:58.000Z",
      document: { marker: `document-${sessionId}` },
      generationId: `generation-${sessionId}`,
      operationId: `generation-operation-${sessionId}`,
      sessionId,
      state: "committed",
    },
    measuredImage: intent.measuredImage,
    supervisor: intent.supervisor,
  });
  const requestSha256 = sha256Text(
    `portable-codex-runtime:podman-writer-request:v1\0${JSON.stringify(
      request,
    )}`,
  );
  const containerId = "a".repeat(64);
  const containerName = `codex-writer-${sha256Text(
    `portable-codex-runtime:podman-container:v1\0${intent.supervisor.supervisorId}\0${operationId}`,
  ).slice(0, 48)}`;
  const processIncarnationId = `podman-process:${containerId}`;
  const writerIncarnationId = `podman-writer:${sha256Text(
    `portable-codex-runtime:podman-writer:v1\0${intent.supervisor.supervisorId}\0${operationId}\0${requestSha256}\0${containerId}`,
  )}`;
  const proofId = `podman-start:${sha256Text(
    `portable-codex-runtime:podman-start-proof:v1\0${intent.supervisor.supervisorId}\0${operationId}\0${requestSha256}\0${containerId}`,
  )}`;
  const stopProofId = `podman-stopped:${sha256Text(
    `portable-codex-runtime:podman-stopped-proof:v1\0${operationId}\0${requestSha256}\0${containerId}`,
  )}`;
  const terminalRecord = {
    containerId,
    containerName,
    contractVersion: 1,
    launchAttemptId: operationId,
    processIncarnationId,
    proofId,
    requestSha256,
    revision: 4,
    status: "stopped",
    stopOperationId: `local-stop-${operationId}`,
    stopProofId,
    writerIncarnationId,
  };
  const result = {
    evidence: {
      contractVersion: 1,
      launchAttemptId: operationId,
      processIncarnationId,
      proofId: stopProofId,
      status: "complete-stopped",
      supervisorId: intent.supervisor.supervisorId,
      writerIncarnationId,
    },
    outcome: "writer-launch-complete-stopped",
    resultVersion: 1,
  };
  const envelope = {
    conflictClass: "session-mutation",
    expectedSession,
    payload: request,
    requestVersion: 1,
  };
  const operationRequestSha256 = sha256Json(envelope);
  const reservationId = `reservation-${sha256Text(operationId)}`;
  const operationRow = {
    operation_id: operationId,
    session_id: sessionId,
    kind: WRITER_LAUNCH_ATTEMPT_OPERATION_KIND,
    request: envelope,
    result,
    state: "committed",
    revision: "2",
    created_at: new Date(OPERATION_STARTED_AT),
    updated_at: new Date(authorizedAt),
    retired_at: new Date(authorizedAt),
  };
  const terminalDocument = structuredClone(expectedSession.document);
  terminalDocument.activeOperation = null;
  terminalDocument.lastOperation = {
    conflictClass: "session-mutation",
    expectedSessionRevision: expectedSession.revision,
    kind: WRITER_LAUNCH_ATTEMPT_OPERATION_KIND,
    operationId,
    operationRevision: "2",
    requestSha256: operationRequestSha256,
    reservationId,
    resultSha256: sha256Json(result),
    state: "committed",
  };
  terminalDocument.launch = null;
  const sessionRow = {
    session_id: sessionId,
    revision: "6",
    document: terminalDocument,
    created_at: new Date(expectedSession.createdAt),
    updated_at: new Date(authorizedAt),
  };
  const terminalRecordSha256 = sha256Json(terminalRecord);
  const authorizationProjection = {
    authorizedAt,
    contractVersion: 1,
    launchAttemptId: operationId,
    sessionId,
    terminalKind: WRITER_LAUNCH_ATTEMPT_OPERATION_KIND,
    terminalOperationId: operationId,
    terminalRecord,
    terminalRecordSha256,
  };
  const authorization = {
    authorizationSha256: sha256Text(
      `portable-codex-runtime:writer-supervisor-state-gc-authorization:v1\0${JSON.stringify(
        authorizationProjection,
      )}\n`,
    ),
    ...authorizationProjection,
  };
  const gcRow = {
    terminal_operation_id: operationId,
    session_id: sessionId,
    launch_attempt_id: operationId,
    terminal_kind: WRITER_LAUNCH_ATTEMPT_OPERATION_KIND,
    terminal_record: terminalRecord,
    terminal_record_sha256: terminalRecordSha256,
    authorization_sha256: authorization.authorizationSha256,
    authorized_at: new Date(authorizedAt),
    collection_status: null,
    collection_receipt_sha256: null,
    collected_at: null,
  };
  const collectionReceipt = {
    contractVersion: 1,
    launchAttemptId: operationId,
    status: "collected",
    terminalRecordSha256: sha256Text(
      `portable-codex-runtime:podman-writer-state-collection:v1\0${JSON.stringify(
        terminalRecord,
      )}`,
    ),
  };
  return {
    authorization,
    collectionReceipt,
    evidence: result.evidence,
    expectedSession,
    gcRow,
    operationRow,
    request,
    sessionRow,
    terminalRecord,
  };
}

function writerOperationTransitionFixture({ kind, outcome, state }) {
  const operationId =
    kind === WRITER_RELEASE_OPERATION_KIND
      ? "writer-release-transition-001"
      : "writer-force-fence-transition-001";
  const base = attachedSnapshot();
  const binding = assertSessionOperationBinding({
    expectedSession: base,
    kind,
    operationId,
    request: {
      contractVersion: 1,
      target: {
        attachmentId: base.document.attachment.attachmentId,
        kind: "attachment",
      },
    },
  });
  const expectedSession = binding.expectedSession;
  const revision =
    state === "prepared"
      ? "0"
      : state === "starting"
        ? "1"
        : outcome === "writer-blocked"
          ? "3"
          : "2";
  const updatedAt =
    state === "prepared"
      ? OPERATION_CREATED_AT
      : state === "starting"
        ? OPERATION_STARTED_AT
        : OPERATION_RETIRED_AT;
  const nextWriterEpoch = String(
    BigInt(expectedSession.document.writerEpoch) + 1n,
  );
  let result = null;
  if (outcome === "writer-released") {
    const lease = expectedSession.document.lease;
    result = {
      resultVersion: 1,
      outcome,
      lease,
      attachment: expectedSession.document.attachment,
      mutationResult: {
        contractVersion: 1,
        backendId: expectedSession.document.storageRef.backendId,
        storageId: expectedSession.document.storageRef.storageId,
        sessionId: expectedSession.sessionId,
        leaseId: lease.leaseId,
        holderId: lease.holderId,
        fencingEpoch: lease.fencingEpoch,
        operation: "detach",
        operationId,
        target: binding.request.target,
        proofId: "writer-release-proof-001",
        status: "detached",
      },
    };
  } else if (outcome === "writer-blocked") {
    result = {
      resultVersion: 1,
      outcome,
      reason: "provider-outcome-unresolved",
      writerEpoch: nextWriterEpoch,
      lease: expectedSession.document.lease,
      attachment: expectedSession.document.attachment,
      fenceTarget: binding.request.target,
    };
  }
  const operation = {
    operationId,
    sessionId: expectedSession.sessionId,
    kind,
    conflictClass: "session-mutation",
    expectedSession,
    request: binding.request,
    requestSha256: binding.requestSha256,
    state,
    revision,
    result,
    createdAt: OPERATION_CREATED_AT,
    updatedAt,
    retiredAt: state === "committed" ? updatedAt : null,
  };
  const reservation = {
    reservationId: binding.reservationId,
    operationId,
    sessionId: expectedSession.sessionId,
    kind,
    expectedSessionRevision: expectedSession.revision,
    state: state === "committed" ? "released" : state,
    conflictClass: "session-mutation",
    requestSha256: binding.requestSha256,
    createdAt: OPERATION_CREATED_AT,
    updatedAt,
    expiresAt: null,
    releasedAt: state === "committed" ? updatedAt : null,
  };
  const pointer = {
    conflictClass: "session-mutation",
    expectedSessionRevision: expectedSession.revision,
    kind,
    operationId,
    operationRevision: revision,
    requestSha256: binding.requestSha256,
    reservationId: binding.reservationId,
    state,
    ...(state === "committed"
      ? { resultSha256: sha256Json(result) }
      : {}),
  };
  let nextDocument = {
    ...expectedSession.document,
    activeOperation: pointer,
  };
  if (state === "starting") {
    nextDocument = {
      ...nextDocument,
      lifecycle:
        kind === WRITER_RELEASE_OPERATION_KIND
          ? "RELEASING"
          : "FENCING",
      writerEpoch:
        kind === WRITER_FORCE_FENCE_OPERATION_KIND
          ? nextWriterEpoch
          : expectedSession.document.writerEpoch,
    };
  } else if (state === "committed") {
    nextDocument = {
      ...nextDocument,
      activeOperation: null,
      lastOperation: pointer,
      ...(outcome === "writer-released"
        ? {
            attachment: null,
            launch: null,
            lease: null,
            lifecycle: "DETACHED",
          }
        : {
            lifecycle: "BLOCKED",
            writerEpoch: nextWriterEpoch,
          }),
    };
  }
  const session = {
    sessionId: expectedSession.sessionId,
    revision: String(
      BigInt(expectedSession.revision) + BigInt(revision) + 1n,
    ),
    document: nextDocument,
    createdAt: expectedSession.createdAt,
    updatedAt,
  };
  return { operation, reservation, session };
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
      writerLaunchStopV3FleetCompatible: true,
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

function assertOperationTransitionProofInvalid(proof) {
  assert.throws(
    () => assertSessionOperationTransitionProof(proof),
    (error) => {
      assert.ok(error instanceof PostgresSessionAuthorityError);
      assert.equal(error.name, "PostgresSessionAuthorityError");
      assert.equal(error.code, "operation_state_invalid");
      assert.equal(error.retryable, false);
      assert.equal(Object.isFrozen(error), true);
      assert.equal("cause" in error, false);
      return true;
    },
  );
}

test("assertSessionOperationTransitionProof validates active and committed writer detach receipts", () => {
  const fixtures = [
    writerOperationTransitionFixture({
      kind: WRITER_RELEASE_OPERATION_KIND,
      outcome: null,
      state: "prepared",
    }),
    writerOperationTransitionFixture({
      kind: WRITER_FORCE_FENCE_OPERATION_KIND,
      outcome: null,
      state: "starting",
    }),
    writerOperationTransitionFixture({
      kind: WRITER_RELEASE_OPERATION_KIND,
      outcome: "writer-released",
      state: "committed",
    }),
    writerOperationTransitionFixture({
      kind: WRITER_FORCE_FENCE_OPERATION_KIND,
      outcome: "writer-blocked",
      state: "committed",
    }),
  ];

  for (const fixture of fixtures) {
    const proof = assertSessionOperationTransitionProof(fixture);
    assert.equal(proof.operation.operationId, fixture.operation.operationId);
    assert.equal(proof.operation.state, fixture.operation.state);
    assert.equal(
      proof.reservation.reservationId,
      fixture.reservation.reservationId,
    );
    assert.equal(proof.session.revision, fixture.session.revision);
    assert.equal(Object.isFrozen(proof), true);
    assert.equal(Object.isFrozen(proof.operation), true);
    assert.equal(Object.isFrozen(proof.reservation), true);
    assert.equal(Object.isFrozen(proof.session), true);
  }
});

test("assertSessionOperationTransitionProof rejects crossed receipt relations", () => {
  const activeRelease = writerOperationTransitionFixture({
    kind: WRITER_RELEASE_OPERATION_KIND,
    outcome: null,
    state: "prepared",
  });
  const activeForceFence = writerOperationTransitionFixture({
    kind: WRITER_FORCE_FENCE_OPERATION_KIND,
    outcome: null,
    state: "starting",
  });
  const committedRelease = writerOperationTransitionFixture({
    kind: WRITER_RELEASE_OPERATION_KIND,
    outcome: "writer-released",
    state: "committed",
  });

  const wrongPointer = structuredClone(activeRelease);
  wrongPointer.session.document.activeOperation.reservationId =
    "reservation-crossed";
  assertOperationTransitionProofInvalid(wrongPointer);

  const wrongResult = structuredClone(committedRelease);
  wrongResult.operation.result.outcome = "writer-fenced";
  assertOperationTransitionProofInvalid(wrongResult);

  const wrongProviderProof = structuredClone(committedRelease);
  wrongProviderProof.operation.result.mutationResult.operationId =
    "writer-release-transition-crossed";
  assertOperationTransitionProofInvalid(wrongProviderProof);

  const wrongRevision = structuredClone(activeForceFence);
  wrongRevision.operation.revision = "2";
  assertOperationTransitionProofInvalid(wrongRevision);

  const prematurelyReleasedReservation = structuredClone(activeRelease);
  prematurelyReleasedReservation.reservation.state = "released";
  prematurelyReleasedReservation.reservation.releasedAt =
    prematurelyReleasedReservation.reservation.updatedAt;
  assertOperationTransitionProofInvalid(prematurelyReleasedReservation);

  const wrongRequestSha256 = structuredClone(activeRelease);
  wrongRequestSha256.operation.requestSha256 = "0".repeat(64);
  assertOperationTransitionProofInvalid(wrongRequestSha256);
});

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

test("constructor rejects proxy options without invoking hostile traps", () => {
  const { store } = authorityWithScripts();
  let trapCalls = 0;
  const hostileOptions = new Proxy(
    { store },
    {
      get() {
        trapCalls += 1;
        throw new Error("proxy trap must not run");
      },
      getOwnPropertyDescriptor() {
        trapCalls += 1;
        throw new Error("proxy trap must not run");
      },
      getPrototypeOf() {
        trapCalls += 1;
        throw new Error("proxy trap must not run");
      },
      ownKeys() {
        trapCalls += 1;
        throw new Error("proxy trap must not run");
      },
    },
  );

  assert.throws(
    () => new PostgresSessionAuthority(hostileOptions),
    (error) =>
      error instanceof PostgresSessionAuthorityError &&
      error.code === "invalid_authority_options",
  );
  assert.equal(trapCalls, 0);

  const { proxy: revokedOptions, revoke } = Proxy.revocable({ store }, {});
  revoke();
  assert.throws(
    () => new PostgresSessionAuthority(revokedOptions),
    (error) =>
      error instanceof PostgresSessionAuthorityError &&
      error.code === "invalid_authority_options",
  );
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
    { store, writerLaunchStopV3FleetCompatible: "true" },
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
    { store, writerLaunchStopV3FleetCompatible: false },
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

test("writer supervisor state GC lists one validated pending authorization per session", async () => {
  const first = writerSupervisorStateGcFixture();
  const second = writerSupervisorStateGcFixture({
    operationId: "writer-launch-gc-002",
    sessionId: OTHER_SESSION_ID,
  });
  const { authority, clients } = authorityWithScripts([
    { rows: [first.gcRow, second.gcRow] },
    { rows: [first.sessionRow] },
    { rows: [first.operationRow] },
    { rows: [second.sessionRow] },
    { rows: [second.operationRow] },
  ]);

  const page = await authority.listWriterSupervisorStateGcCandidates({
    afterSessionId: null,
    limit: 1,
  });

  assert.equal(Object.getPrototypeOf(page), null);
  assert.equal(page.candidates.length, 1);
  assert.equal(Object.getPrototypeOf(page.candidates[0]), null);
  assert.equal(
    Object.getPrototypeOf(page.candidates[0].authorization),
    null,
  );
  assert.equal(
    page.candidates[0].authorization.authorizationSha256,
    first.authorization.authorizationSha256,
  );
  assert.equal(
    page.candidates[0].launchOperation.operationId,
    first.authorization.launchAttemptId,
  );
  assert.equal(
    page.candidates[0].terminalOperation.operationId,
    first.authorization.terminalOperationId,
  );
  assert.equal(page.candidates[0].session.sessionId, SESSION_ID);
  assert.equal(page.nextAfterSessionId, SESSION_ID);
  assert.ok(Object.isFrozen(page));
  assert.ok(Object.isFrozen(page.candidates[0]));
  assert.ok(
    queryTexts(clients[0]).some((text) =>
      text?.startsWith("SELECT DISTINCT ON (session_id)"),
    ),
  );
  clients[0].assertExhausted();
});

test("writer supervisor state GC finalizer entrypoints preserve the legacy exact input ABI", async () => {
  const fixture = writerSupervisorStateGcFixture();
  const base = {
    evidence: fixture.evidence,
    expectedOperationRevision: "1",
    expectedSession: fixture.expectedSession,
    kind: WRITER_LAUNCH_ATTEMPT_OPERATION_KIND,
    operationId: fixture.authorization.terminalOperationId,
    request: fixture.request,
  };
  const { authority, pool } = authorityWithScripts();
  await assertAuthorityError(
    authority.finalizeWriterLaunchAttemptStopped({
      ...base,
      terminalRecord: fixture.terminalRecord,
    }),
    "invalid_operation_request",
  );
  await assertAuthorityError(
    authority.finalizeWriterLaunchAttemptStoppedAndAuthorizeSupervisorStateGc(
      base,
    ),
    "invalid_operation_request",
  );
  await assertAuthorityError(
    authority.finalizeWriterLaunchAttemptStoppedAndAuthorizeSupervisorStateGc({
      ...base,
      terminalRecord: {
        ...fixture.terminalRecord,
        revision: 3,
        status: "stopping",
        stopProofId: null,
      },
    }),
    "invalid_operation_request",
  );
  assert.equal(pool.connectCalls, 0);
});

test("writer supervisor state GC authorization read validates the exact terminal relation", async () => {
  const fixture = writerSupervisorStateGcFixture();
  const { authority, clients } = authorityWithScripts([
    { rows: [fixture.gcRow] },
    { rows: [fixture.sessionRow] },
    { rows: [fixture.operationRow] },
  ]);
  const authorization =
    await authority.readWriterSupervisorStateGcAuthorization({
      terminalOperationId: fixture.authorization.terminalOperationId,
    });
  assert.equal(Object.getPrototypeOf(authorization), null);
  assert.deepEqual(
    JSON.parse(JSON.stringify(authorization)),
    fixture.authorization,
  );
  assert.ok(Object.isFrozen(authorization));
  assert.ok(Object.isFrozen(authorization.terminalRecord));
  clients[0].assertExhausted();
});

test("writer supervisor state GC does not infer candidates from naked terminal operations", async () => {
  const { authority, clients } = authorityWithScripts([{ rows: [] }]);
  const page = await authority.listWriterSupervisorStateGcCandidates({
    afterSessionId: null,
    limit: 10,
  });
  assert.deepEqual([...page.candidates], []);
  assert.equal(page.nextAfterSessionId, null);
  assert.equal(
    queryTexts(clients[0]).filter((text) =>
      text?.includes("session_authority.operation_claims"),
    ).length,
    0,
  );
  clients[0].assertExhausted();
});

test("writer supervisor state GC completion accepts collected-to-absent acknowledgement-loss replay", async () => {
  const fixture = writerSupervisorStateGcFixture();
  const collectionReceiptSha256 = sha256Text(
    `portable-codex-runtime:writer-supervisor-state-gc-collection-receipt:v1\0${JSON.stringify(
      fixture.collectionReceipt,
    )}\n`,
  );
  const completedRow = {
    ...fixture.gcRow,
    collection_status: "collected",
    collection_receipt_sha256: collectionReceiptSha256,
    collected_at: new Date(OPERATION_RETIRED_AT),
  };
  const { authority, clients } = authorityWithScripts(
    [
      { rows: [fixture.gcRow] },
      { rows: [fixture.sessionRow] },
      { rows: [fixture.operationRow] },
      { rows: [fixture.gcRow] },
      { rows: [completedRow] },
    ],
    [
      { rows: [completedRow] },
      { rows: [fixture.sessionRow] },
      { rows: [fixture.operationRow] },
      { rows: [completedRow] },
    ],
  );

  const completed = await authority.completeWriterSupervisorStateGc({
    authorization: fixture.authorization,
    collectionReceipt: fixture.collectionReceipt,
  });
  assert.equal(Object.getPrototypeOf(completed), null);
  assert.equal(completed.finalized, true);
  assert.equal(completed.collectionStatus, "collected");
  assert.equal(completed.collectionReceiptSha256, collectionReceiptSha256);

  const replay = await authority.completeWriterSupervisorStateGc({
    authorization: fixture.authorization,
    collectionReceipt: {
      ...fixture.collectionReceipt,
      status: "absent",
    },
  });
  assert.equal(replay.finalized, false);
  assert.equal(replay.collectionStatus, "collected");
  assert.equal(replay.collectionReceiptSha256, collectionReceiptSha256);
  clients[0].assertExhausted();
  clients[1].assertExhausted();
});

test("writer supervisor state GC completion rejects a different canonical authorization before update", async () => {
  const stored = writerSupervisorStateGcFixture();
  const foreign = writerSupervisorStateGcFixture({
    operationId: "writer-launch-gc-foreign",
    sessionId: OTHER_SESSION_ID,
  });
  const { authority, clients } = authorityWithScripts([{ rows: [] }]);
  await assertAuthorityError(
    authority.completeWriterSupervisorStateGc({
      authorization: foreign.authorization,
      collectionReceipt: foreign.collectionReceipt,
    }),
    "writer_supervisor_state_gc_collection_conflict",
  );
  assert.equal(
    queryTexts(clients[0]).some((text) =>
      text?.startsWith(
        "UPDATE session_authority.writer_supervisor_state_gc",
      ),
    ),
    false,
  );
  assert.notEqual(
    foreign.authorization.authorizationSha256,
    stored.authorization.authorizationSha256,
  );
  assert.equal(
    queryTexts(clients[0]).some((text) =>
      text?.includes("session_authority.sessions") ||
      text?.includes("session_authority.operation_claims"),
    ),
    false,
  );
  clients[0].assertExhausted();
});
