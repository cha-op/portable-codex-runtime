import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";

import { Pool } from "pg";

import {
  PostgresSessionAuthority,
  PostgresSessionAuthorityError,
  SESSION_AUTHORITY_DOCUMENT_VERSION,
  WRITER_ATTACHMENT_ACQUIRE_OPERATION_KIND,
  WRITER_LEASE_RENEW_OPERATION_KIND,
} from "../src/postgres-session-authority.mjs";
import {
  PostgresSerializableStore,
  PostgresSerializableStoreError,
} from "../src/postgres-serializable-store.mjs";
import { createSessionManifest } from "../src/session-storage-contracts.mjs";

const EMPTY_JSON_OBJECT = "{}";
const IMAGE_DIGEST = `sha256:${"a".repeat(64)}`;
const databaseUrl = process.env.SESSION_AUTHORITY_DATABASE_URL;
const databaseConfigured =
  typeof databaseUrl === "string" && databaseUrl.length > 0;

if (!databaseConfigured) {
  throw new Error(
    "SESSION_AUTHORITY_DATABASE_URL is required for the PostgreSQL integration gate",
  );
}

function registrationInput(
  sessionId,
  {
    storageId = `volume-${randomUUID()}`,
  } = {},
) {
  const codexSessionId = randomUUID();
  return {
    manifest: createSessionManifest({
      sessionId,
      codex: {
        rootThreadId: codexSessionId,
        sessionId: codexSessionId,
        ephemeral: false,
        historyMode: "paginated",
      },
      runtime: {
        imageDigest: IMAGE_DIGEST,
        imageMediaType:
          "application/vnd.oci.image.manifest.v1+json",
        platform: "linux/arm64",
        codexVersion: "codex-cli 0.142.4",
        codexSandbox: "danger-full-access",
      },
    }),
    storageRef: {
      contractVersion: 1,
      backendId: "postgres-authority-integration",
      storageId,
      sessionId,
    },
    backendCapabilities: {
      atomicPointInTimeCheckpoint: true,
      exclusiveWriterAttachment: true,
      fencing: "epoch-enforced",
      normalDirectoryAttachment: true,
    },
  };
}

function firstMatchingQueryBarrierPool(
  pool,
  expectedParticipants,
  label,
  matches,
) {
  let arrivals = 0;
  let release;
  let timer;
  const barrier = new Promise((resolve, reject) => {
    release = () => {
      clearTimeout(timer);
      resolve();
    };
    timer = setTimeout(
      () => reject(new Error(`${label} timed out`)),
      10_000,
    );
    timer.unref();
  });

  return Object.freeze({
    async connect() {
      const client = await pool.connect();
      return {
        connection: client.connection,
        async query(...args) {
          const input = args[0];
          const text =
            typeof input === "string" ? input : input?.text;
          if (
            typeof text === "string" &&
            matches(text) &&
            arrivals < expectedParticipants
          ) {
            arrivals += 1;
            if (arrivals === expectedParticipants) release();
            await barrier;
          }
          return Reflect.apply(client.query, client, args);
        },
        release(...args) {
          return Reflect.apply(client.release, client, args);
        },
      };
    },
    waitForBarrier() {
      return barrier;
    },
  });
}

function firstRegistrationQueryBarrierPool(
  pool,
  expectedParticipants,
  label,
) {
  return firstMatchingQueryBarrierPool(
    pool,
    expectedParticipants,
    label,
    (text) =>
      text.startsWith("INSERT INTO session_authority.sessions"),
  );
}

function firstSessionLockQueryBarrierPool(
  pool,
  expectedParticipants,
  label,
) {
  return firstMatchingQueryBarrierPool(
    pool,
    expectedParticipants,
    label,
    (text) =>
      text.includes("FROM session_authority.sessions") &&
      text.includes("FOR UPDATE"),
  );
}

function firstSessionLockQueryNotificationPool(pool, label) {
  let matched = false;
  let notifyMatch;
  let timer;
  const firstMatch = new Promise((resolve, reject) => {
    notifyMatch = () => {
      clearTimeout(timer);
      resolve();
    };
    timer = setTimeout(
      () => reject(new Error(`${label} timed out`)),
      10_000,
    );
    timer.unref();
  });

  return Object.freeze({
    dedicatedPool: Object.freeze({
      async connect() {
        const client = await pool.connect();
        return {
          connection: client.connection,
          async query(...args) {
            const input = args[0];
            const text =
              typeof input === "string" ? input : input?.text;
            if (
              !matched &&
              typeof text === "string" &&
              text.includes("FROM session_authority.sessions") &&
              text.includes("FOR UPDATE")
            ) {
              matched = true;
              notifyMatch();
            }
            return Reflect.apply(client.query, client, args);
          },
          release(...args) {
            return Reflect.apply(client.release, client, args);
          },
        };
      },
    }),
    waitForFirstMatch() {
      return firstMatch;
    },
  });
}

function firstCommitAcknowledgementLossPool(pool) {
  let acknowledgementLost = false;

  return Object.freeze({
    async connect() {
      const client = await pool.connect();
      return {
        connection: client.connection,
        async query(...args) {
          const input = args[0];
          const text =
            typeof input === "string" ? input : input?.text;
          const result = await Reflect.apply(
            client.query,
            client,
            args,
          );
          if (text === "COMMIT" && !acknowledgementLost) {
            acknowledgementLost = true;
            throw new Error(
              "synthetic dispatch COMMIT acknowledgement loss",
            );
          }
          return result;
        },
        release(...args) {
          return Reflect.apply(client.release, client, args);
        },
      };
    },
  });
}

function assertIdentityConflict(error) {
  assert.ok(error instanceof PostgresSessionAuthorityError);
  assert.equal(error.name, "PostgresSessionAuthorityError");
  assert.equal(error.code, "session_identity_conflict");
  assert.equal(error.retryable, false);
  assert.equal(Object.hasOwn(error, "cause"), false);
  return true;
}

function assertAuthorityCode(code) {
  return (error) => {
    assert.ok(error instanceof PostgresSessionAuthorityError);
    assert.equal(error.name, "PostgresSessionAuthorityError");
    assert.equal(error.code, code);
    assert.equal(error.retryable, false);
    assert.equal(Object.hasOwn(error, "cause"), false);
    return true;
  };
}

function operationInput(
  expectedSession,
  {
    operationId = `operation-${randomUUID()}`,
    kind = "writer-acquire",
    request = {
      action: "reserve-writer",
      nonce: randomUUID(),
    },
  } = {},
) {
  return {
    expectedSession,
    operationId,
    kind,
    request,
  };
}

function writerAttachmentInput(
  expectedSession,
  {
    holderId = `host-${randomUUID()}`,
    leaseDurationMilliseconds = 120_000,
    operationId = `operation-${randomUUID()}`,
  } = {},
) {
  return {
    expectedSession,
    operationId,
    kind: WRITER_ATTACHMENT_ACQUIRE_OPERATION_KIND,
    request: {
      contractVersion: 1,
      holderId,
      leaseDurationMilliseconds,
    },
  };
}

function attachmentEvidence(mutationRequest) {
  const proofId = `proof-${randomUUID()}`;
  const rootPath = `/var/lib/portable-codex/${mutationRequest.sessionId}`;
  return {
    mutationResult: {
      ...structuredClone(mutationRequest),
      status: "attached",
      proofId,
      rootPath,
    },
    attachment: {
      contractVersion: mutationRequest.contractVersion,
      backendId: mutationRequest.backendId,
      storageId: mutationRequest.storageId,
      sessionId: mutationRequest.sessionId,
      attachmentId: mutationRequest.target.attachmentId,
      leaseId: mutationRequest.leaseId,
      holderId: mutationRequest.holderId,
      fencingEpoch: mutationRequest.fencingEpoch,
      operationId: mutationRequest.operationId,
      proofId,
      kind: "directory",
      rootPath,
      mode: "read-write",
    },
  };
}

async function attachWriter(authority, registered, options) {
  const input = writerAttachmentInput(registered, options);
  await authority.reserveOperation(input);
  const starting = await authority.claimWriterAttachmentDispatch({
    ...structuredClone(input),
    expectedOperationRevision: "0",
  });
  return authority.finalizeWriterAttachment({
    ...structuredClone(input),
    expectedOperationRevision: "1",
    ...attachmentEvidence(starting.mutationRequest),
  });
}

function writerLeaseRenewalInput(
  expectedSession,
  {
    leaseDurationMilliseconds = 300_000,
    operationId = `operation-${randomUUID()}`,
  } = {},
) {
  return {
    expectedSession,
    operationId,
    kind: WRITER_LEASE_RENEW_OPERATION_KIND,
    request: {
      contractVersion: 1,
      leaseDurationMilliseconds,
    },
  };
}

function assertOperationReceipt(
  receipt,
  state,
  { currentTerminal = state === "committed" } = {},
) {
  assert.equal(receipt.status, state);
  assert.equal(receipt.operation.state, state);
  assert.equal(
    receipt.reservation.state,
    state === "committed" ? "released" : state,
  );
  assert.equal(
    receipt.session.document.activeOperation?.operationId ??
      null,
    state === "committed" ? null : receipt.operation.operationId,
  );
  if (currentTerminal) {
    assert.equal(state, "committed");
    assert.equal(
      receipt.session.document.lastOperation?.operationId,
      receipt.operation.operationId,
    );
    assert.equal(
      receipt.session.document.lastOperation?.reservationId,
      receipt.reservation.reservationId,
    );
  }
  assert.equal(Object.isFrozen(receipt), true);
  assert.equal(Object.isFrozen(receipt.operation), true);
  assert.equal(Object.isFrozen(receipt.reservation), true);
  assert.equal(Object.isFrozen(receipt.session), true);
}

function assertInitialSession(snapshot, input) {
  assert.equal(snapshot.sessionId, input.manifest.sessionId);
  assert.equal(snapshot.revision, "0");
  assert.deepEqual(snapshot.document, {
    documentVersion: SESSION_AUTHORITY_DOCUMENT_VERSION,
    manifest: input.manifest,
    storageRef: input.storageRef,
    backendCapabilities: input.backendCapabilities,
    lifecycle: "DETACHED",
    writerEpoch: "0",
    lease: null,
    attachment: null,
    activeOperation: null,
    lastOperation: null,
    recovery: null,
    launch: null,
  });
  assert.match(
    snapshot.createdAt,
    /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/u,
  );
  assert.equal(snapshot.updatedAt, snapshot.createdAt);
}

test(
  "PostgreSQL authority migration and serializable executor work end to end",
  { timeout: 30_000 },
  async (t) => {
    const pool = new Pool({
      application_name: "portable-codex-runtime-integration-test",
      connectionString: databaseUrl,
      max: 2,
    });
    let conflictSessionId;
    let preparedTransactionId;
    let shadowSchema;
    t.after(async () => {
      if (preparedTransactionId !== undefined) {
        const prepared = await pool.query(
          [
            "SELECT 1",
            "FROM pg_prepared_xacts",
            "WHERE gid = $1 AND database = current_database()",
          ].join(" "),
          [preparedTransactionId],
        );
        if (prepared.rows.length > 0) {
          await pool.query(
            `ROLLBACK PREPARED '${preparedTransactionId}'`,
          );
        }
      }
      if (shadowSchema !== undefined) {
        await pool.query(
          `DROP SCHEMA IF EXISTS "${shadowSchema}" CASCADE`,
        );
      }
      if (conflictSessionId !== undefined) {
        await pool.query(
          "DELETE FROM session_authority.sessions WHERE session_id = $1",
          [conflictSessionId],
        );
      }
      await pool.end();
    });
    const store = new PostgresSerializableStore({
      dedicatedPool: pool,
      maxTransactionAttempts: 2,
    });
    const resetPool = new Pool({
      application_name:
        "portable-codex-runtime-session-reset-integration-test",
      connectionString: databaseUrl,
      max: 1,
    });
    t.after(() => resetPool.end());
    const resetStore = new PostgresSerializableStore({
      dedicatedPool: resetPool,
    });

    const firstMigration = await store.migrate();
    assert.equal(firstMigration.version, 1);
    assert.equal(firstMigration.checksum.length, 64);
    const secondMigration = await store.migrate();
    assert.equal(secondMigration.applied, false);
    assert.equal(secondMigration.checksum, firstMigration.checksum);

    const baselineWorkMem = await resetStore.runSerializable(
      async (transaction) => {
        const result = await transaction.query("SHOW work_mem");
        return result.rows[0].work_mem;
      },
    );
    await resetStore.runSerializable(async (transaction) => {
      await transaction.query("SET SESSION work_mem = '64MB'");
      await transaction.query(
        "CREATE TEMPORARY TABLE authority_reset_probe (value integer)",
      );
      await transaction.query("LISTEN authority_reset_probe");
      await transaction.query("SELECT pg_advisory_lock(724163882)");
    });
    const resetEvidence = await resetStore.runSerializable(
      async (transaction) => {
        const workMem = await transaction.query("SHOW work_mem");
        const state = await transaction.query(
          [
            "SELECT",
            "to_regclass('pg_temp.authority_reset_probe')::text AS temp_table,",
            "(SELECT count(*)::integer FROM pg_listening_channels()) AS listening_channels,",
            [
              "(SELECT count(*)::integer FROM pg_locks",
              "WHERE locktype = 'advisory' AND pid = pg_backend_pid()) AS advisory_locks",
            ].join(" "),
          ].join(" "),
        );
        return {
          ...state.rows[0],
          workMem: workMem.rows[0].work_mem,
        };
      },
    );
    assert.deepEqual(resetEvidence, {
      advisory_locks: 0,
      listening_channels: 0,
      temp_table: null,
      workMem: baselineWorkMem,
    });

    const durabilityPool = new Pool({
      application_name:
        "portable-codex-runtime-durable-commit-integration-test",
      connectionString: databaseUrl,
      max: 1,
    });
    t.after(() => durabilityPool.end());
    const durabilityQueries = [];
    const durabilityStore = new PostgresSerializableStore({
      dedicatedPool: {
        async connect() {
          const client = await durabilityPool.connect();
          const query = client.query;
          client.query = function (...args) {
            durabilityQueries.push(args);
            return Reflect.apply(query, this, args);
          };
          return client;
        },
      },
    });
    const callbackSynchronousCommit =
      await durabilityStore.runSerializable(async (transaction) => {
        await transaction.query(
          "SET LOCAL synchronous_commit = off",
        );
        const result = await transaction.query(
          "SHOW synchronous_commit",
        );
        return result.rows[0].synchronous_commit;
      });
    assert.equal(callbackSynchronousCommit, "off");
    assert.deepEqual(
      durabilityQueries.map(([input]) =>
        typeof input === "string" ? input : input.text,
      ),
      [
        "DISCARD ALL",
        "BEGIN ISOLATION LEVEL SERIALIZABLE READ WRITE",
        "SELECT pg_catalog.transaction_timestamp() AS transaction_timestamp, pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id",
        "SET LOCAL synchronous_commit = off",
        "SELECT pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id",
        "SHOW synchronous_commit",
        "SELECT pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id",
        "SET LOCAL synchronous_commit = on",
        "SELECT pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id",
        "COMMIT",
        "DISCARD ALL",
      ],
    );
    for (const queryIndex of [3, 5, 7]) {
      assert.equal(
        durabilityQueries[queryIndex][0].queryMode,
        "extended",
      );
    }

    const schema = await store.runSerializable(async (transaction) => {
      const result = await transaction.query(
        [
          "SELECT",
          "to_regclass('session_authority.sessions')::text AS sessions,",
          "to_regclass('session_authority.operation_claims')::text AS operations,",
          "to_regclass('session_authority.capture_attempt_claims')::text AS attempts,",
          "to_regclass('session_authority.capture_attempt_tombstones')::text AS tombstones,",
          "to_regclass('session_authority.checkpoint_catalogue')::text AS catalogue,",
          "to_regclass('session_authority.reservations')::text AS reservations",
        ].join(" "),
      );
      const timestamp = await transaction.query(
        "SELECT transaction_timestamp() AS value",
      );
      assert.equal(timestamp.rows[0].value.toISOString(), transaction.now);
      return result.rows[0];
    });
    assert.deepEqual(schema, {
      attempts: "session_authority.capture_attempt_claims",
      catalogue: "session_authority.checkpoint_catalogue",
      operations: "session_authority.operation_claims",
      reservations: "session_authority.reservations",
      sessions: "session_authority.sessions",
      tombstones: "session_authority.capture_attempt_tombstones",
    });
    const activeIndexes = await store.runSerializable((transaction) =>
      transaction.query(
        [
          "SELECT indexname, indexdef",
          "FROM pg_indexes",
          "WHERE schemaname = 'session_authority'",
          "AND indexname = ANY($1::text[])",
          "ORDER BY indexname",
        ].join(" "),
        [
          "{operation_claims_one_active_per_session,reservations_one_active_per_session}",
        ],
      ),
    );
    assert.deepEqual(
      activeIndexes.rows.map(({ indexname }) => indexname),
      [
        "operation_claims_one_active_per_session",
        "reservations_one_active_per_session",
      ],
    );
    assert.match(
      activeIndexes.rows[0].indexdef,
      /CREATE UNIQUE INDEX[\s\S]+\(session_id\)[\s\S]+retired_at IS NULL/u,
    );
    assert.match(
      activeIndexes.rows[1].indexdef,
      /CREATE UNIQUE INDEX[\s\S]+\(session_id\)[\s\S]+released_at IS NULL/u,
    );
    await assert.rejects(
      store.runSerializable((transaction) => transaction.query("COMMIT")),
      (error) => {
        assert.ok(error instanceof PostgresSerializableStoreError);
        assert.equal(error.code, "transaction_boundary_lost");
        assert.equal(error.commitState, "uncertain");
        assert.equal("cause" in error, false);
        return true;
      },
    );
    shadowSchema =
      `authority_shadow_${randomUUID().replaceAll("-", "_")}`;
    await assert.rejects(
      store.runSerializable(async (transaction) => {
        const transactionIdResult = await transaction.query(
          "SELECT pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id",
        );
        const transactionId =
          transactionIdResult.rows[0].transaction_id;
        assert.match(transactionId, /^[1-9][0-9]*$/u);
        await transaction.query(`CREATE SCHEMA "${shadowSchema}"`);
        await transaction.query(
          [
            `CREATE FUNCTION "${shadowSchema}".pg_current_xact_id()`,
            "RETURNS pg_catalog.xid8",
            "LANGUAGE sql IMMUTABLE",
            `AS $$ SELECT '${transactionId}'::pg_catalog.xid8 $$`,
          ].join(" "),
        );
        await transaction.query(
          `SET search_path = "${shadowSchema}", pg_catalog`,
        );
        await transaction.query("COMMIT");
      }),
      (error) => {
        assert.ok(error instanceof PostgresSerializableStoreError);
        assert.equal(error.code, "transaction_boundary_lost");
        assert.equal(error.commitState, "uncertain");
        assert.equal("cause" in error, false);
        return true;
      },
    );
    await store.runSerializable((transaction) =>
      transaction.query("PREPARE transaction AS SELECT 1"),
    );
    preparedTransactionId =
      `portable-codex-runtime-integration-${randomUUID()}`;
    await assert.rejects(
      store.runSerializable((transaction) =>
        transaction.query(
          [
            "; /* leading empty statement */ PREPARE",
            "/* transaction-boundary */ TRANSACTION",
            `'${preparedTransactionId}'`,
          ].join(" "),
        ),
      ),
      (error) => {
        assert.ok(error instanceof PostgresSerializableStoreError);
        assert.equal(error.code, "transaction_query_invalid");
        assert.equal(error.commitState, "not-committed");
        assert.equal("cause" in error, false);
        return true;
      },
    );
    const preparedTransaction = await pool.query(
      [
        "SELECT 1",
        "FROM pg_prepared_xacts",
        "WHERE gid = $1 AND database = current_database()",
      ].join(" "),
      [preparedTransactionId],
    );
    assert.deepEqual(preparedTransaction.rows, []);

    conflictSessionId = randomUUID();
    await store.runSerializable((transaction) =>
      transaction.query(
        [
          "INSERT INTO session_authority.sessions",
          "(session_id, document, created_at, updated_at)",
          "VALUES ($1, $2::jsonb, $3, $3)",
        ].join(" "),
        [conflictSessionId, EMPTY_JSON_OBJECT, transaction.now],
      ),
    );
    let releaseInitialReaders;
    let barrierTimer;
    const initialReaderBarrier = new Promise((resolve, reject) => {
      releaseInitialReaders = () => {
        clearTimeout(barrierTimer);
        resolve();
      };
      barrierTimer = setTimeout(
        () => reject(new Error("serializable conflict barrier timed out")),
        10_000,
      );
      barrierTimer.unref();
    });
    let initialReaders = 0;
    const callbackAttempts = [0, 0];
    const incrementRevision = (index) =>
      store.runSerializable(async (transaction) => {
        callbackAttempts[index] += 1;
        const before = await transaction.query(
          "SELECT revision FROM session_authority.sessions WHERE session_id = $1",
          [conflictSessionId],
        );
        assert.equal(before.rows.length, 1);
        if (callbackAttempts[index] === 1) {
          initialReaders += 1;
          if (initialReaders === 2) releaseInitialReaders();
          await initialReaderBarrier;
        }
        const updated = await transaction.query(
          [
            "UPDATE session_authority.sessions",
            "SET revision = revision + 1, updated_at = $2",
            "WHERE session_id = $1",
            "RETURNING revision",
          ].join(" "),
          [conflictSessionId, transaction.now],
        );
        return Number(updated.rows[0].revision);
      });
    const revisions = await Promise.all([
      incrementRevision(0),
      incrementRevision(1),
    ]);
    assert.deepEqual(revisions.sort((left, right) => left - right), [1, 2]);
    assert.equal(callbackAttempts[0] + callbackAttempts[1], 3);

    const activeOperationSessionId = randomUUID();
    await assert.rejects(
      store.runSerializable(async (transaction) => {
        await transaction.query(
          [
            "INSERT INTO session_authority.sessions",
            "(session_id, document, created_at, updated_at)",
            "VALUES ($1, $2::jsonb, $3, $3)",
          ].join(" "),
          [activeOperationSessionId, EMPTY_JSON_OBJECT, transaction.now],
        );
        for (const operationId of [
          `integration-operation-${randomUUID()}`,
          `integration-operation-${randomUUID()}`,
        ]) {
          await transaction.query(
            [
              "INSERT INTO session_authority.operation_claims",
              "(operation_id, session_id, kind, request, state, created_at, updated_at)",
              "VALUES ($1, $2, 'integration', $3::jsonb, 'active', $4, $4)",
            ].join(" "),
            [
              operationId,
              activeOperationSessionId,
              EMPTY_JSON_OBJECT,
              transaction.now,
            ],
          );
        }
      }),
      (error) => {
        assert.ok(error instanceof PostgresSerializableStoreError);
        assert.equal(error.code, "transaction_query_failed");
        assert.equal(error.commitState, "not-committed");
        assert.equal("cause" in error, false);
        return true;
      },
    );

    const activeReservationSessionId = randomUUID();
    await assert.rejects(
      store.runSerializable(async (transaction) => {
        await transaction.query(
          [
            "INSERT INTO session_authority.sessions",
            "(session_id, document, created_at, updated_at)",
            "VALUES ($1, $2::jsonb, $3, $3)",
          ].join(" "),
          [activeReservationSessionId, EMPTY_JSON_OBJECT, transaction.now],
        );
        const retiredOperationId = `integration-operation-${randomUUID()}`;
        const activeOperationId = `integration-operation-${randomUUID()}`;
        await transaction.query(
          [
            "INSERT INTO session_authority.operation_claims",
            "(operation_id, session_id, kind, request, state, created_at, updated_at, retired_at)",
            "VALUES ($1, $2, 'integration', $3::jsonb, 'retired', $4, $4, $4)",
          ].join(" "),
          [
            retiredOperationId,
            activeReservationSessionId,
            EMPTY_JSON_OBJECT,
            transaction.now,
          ],
        );
        await transaction.query(
          [
            "INSERT INTO session_authority.operation_claims",
            "(operation_id, session_id, kind, request, state, created_at, updated_at)",
            "VALUES ($1, $2, 'integration', $3::jsonb, 'active', $4, $4)",
          ].join(" "),
          [
            activeOperationId,
            activeReservationSessionId,
            EMPTY_JSON_OBJECT,
            transaction.now,
          ],
        );
        for (const [reservationId, operationId] of [
          [`integration-reservation-${randomUUID()}`, retiredOperationId],
          [`integration-reservation-${randomUUID()}`, activeOperationId],
        ]) {
          await transaction.query(
            [
              "INSERT INTO session_authority.reservations",
              [
                "(reservation_id, operation_id, session_id, kind,",
                "expected_session_revision, state, payload, created_at, updated_at)",
              ].join(" "),
              "VALUES ($1, $2, $3, 'integration', 0, 'active', $4::jsonb, $5, $5)",
            ].join(" "),
            [
              reservationId,
              operationId,
              activeReservationSessionId,
              EMPTY_JSON_OBJECT,
              transaction.now,
            ],
          );
        }
      }),
      (error) => {
        assert.ok(error instanceof PostgresSerializableStoreError);
        assert.equal(error.code, "transaction_query_failed");
        assert.equal(error.commitState, "not-committed");
        assert.equal("cause" in error, false);
        return true;
      },
    );
  },
);

test(
  "PostgresSessionAuthority registration is canonical under replay and concurrency",
  { timeout: 30_000 },
  async (t) => {
    const pool = new Pool({
      application_name:
        "portable-codex-runtime-session-registry-integration-test",
      connectionString: databaseUrl,
      max: 3,
    });
    const sessionIds = [];
    t.after(async () => {
      try {
        if (sessionIds.length > 0) {
          await pool.query(
            [
              "DELETE FROM session_authority.reservations",
              "WHERE session_id = ANY($1::uuid[])",
            ].join(" "),
            [sessionIds],
          );
          await pool.query(
            [
              "DELETE FROM session_authority.operation_claims",
              "WHERE session_id = ANY($1::uuid[])",
            ].join(" "),
            [sessionIds],
          );
          await pool.query(
            [
              "DELETE FROM session_authority.sessions",
              "WHERE session_id = ANY($1::uuid[])",
            ].join(" "),
            [sessionIds],
          );
        }
      } finally {
        await pool.end();
      }
    });
    const store = new PostgresSerializableStore({
      dedicatedPool: pool,
      maxTransactionAttempts: 3,
    });
    await store.migrate();
    const authority = new PostgresSessionAuthority({ store });

    await t.test(
      "registration reads back and exact replay preserves one row",
      async () => {
        const sessionId = randomUUID();
        sessionIds.push(sessionId);
        const input = registrationInput(sessionId);

        const registered = await authority.registerSession(input);
        assertInitialSession(registered, input);
        assert.equal(Object.isFrozen(registered), true);
        assert.equal(Object.isFrozen(registered.document), true);

        const readBack = await authority.readSession({ sessionId });
        assert.deepEqual(readBack, registered);

        const replayed = await authority.registerSession(
          structuredClone(input),
        );
        assert.deepEqual(replayed, registered);

        const stored = await pool.query(
          [
            "SELECT count(*)::integer AS row_count,",
            "min(created_at) AS created_at,",
            "max(updated_at) AS updated_at",
            "FROM session_authority.sessions",
            "WHERE session_id = $1",
          ].join(" "),
          [sessionId],
        );
        assert.equal(stored.rows[0].row_count, 1);
        assert.equal(
          stored.rows[0].created_at.toISOString(),
          registered.createdAt,
        );
        assert.equal(
          stored.rows[0].updated_at.toISOString(),
          registered.updatedAt,
        );
      },
    );

    await t.test(
      "concurrent identical registration converges on one canonical row",
      async () => {
        const sessionId = randomUUID();
        sessionIds.push(sessionId);
        const input = registrationInput(sessionId);
        const concurrentStore = new PostgresSerializableStore({
          dedicatedPool: firstRegistrationQueryBarrierPool(
            pool,
            2,
            "identical registration barrier",
          ),
          maxTransactionAttempts: 3,
        });
        const concurrentAuthority = new PostgresSessionAuthority({
          store: concurrentStore,
        });

        const registrations = await Promise.all([
          concurrentAuthority.registerSession(input),
          concurrentAuthority.registerSession(structuredClone(input)),
        ]);
        assertInitialSession(registrations[0], input);
        assert.deepEqual(registrations[1], registrations[0]);
        assert.deepEqual(
          await authority.readSession({ sessionId }),
          registrations[0],
        );

        const stored = await pool.query(
          [
            "SELECT count(*)::integer AS row_count",
            "FROM session_authority.sessions",
            "WHERE session_id = $1",
          ].join(" "),
          [sessionId],
        );
        assert.equal(stored.rows[0].row_count, 1);
      },
    );

    await t.test(
      "concurrent conflicting registration preserves one identity and rejects the other",
      async () => {
        const sessionId = randomUUID();
        sessionIds.push(sessionId);
        const firstInput = registrationInput(sessionId);
        const secondInput = {
          ...firstInput,
          storageRef: {
            ...firstInput.storageRef,
            storageId: `conflicting-volume-${randomUUID()}`,
          },
        };
        const concurrentStore = new PostgresSerializableStore({
          dedicatedPool: firstRegistrationQueryBarrierPool(
            pool,
            2,
            "conflicting registration barrier",
          ),
          maxTransactionAttempts: 3,
        });
        const concurrentAuthority = new PostgresSessionAuthority({
          store: concurrentStore,
        });

        const outcomes = await Promise.allSettled([
          concurrentAuthority.registerSession(firstInput),
          concurrentAuthority.registerSession(secondInput),
        ]);
        const fulfilled = outcomes.filter(
          ({ status }) => status === "fulfilled",
        );
        const rejected = outcomes.filter(
          ({ status }) => status === "rejected",
        );
        assert.equal(fulfilled.length, 1);
        assert.equal(rejected.length, 1);
        assertIdentityConflict(rejected[0].reason);

        const canonical = await authority.readSession({ sessionId });
        assert.deepEqual(canonical, fulfilled[0].value);
        const winningInput =
          outcomes[0].status === "fulfilled"
            ? firstInput
            : secondInput;
        const losingInput =
          outcomes[0].status === "fulfilled"
            ? secondInput
            : firstInput;
        assertInitialSession(canonical, winningInput);
        await assert.rejects(
          authority.registerSession(losingInput),
          assertIdentityConflict,
        );

        const stored = await pool.query(
          [
            "SELECT document",
            "FROM session_authority.sessions",
            "WHERE session_id = $1",
          ].join(" "),
          [sessionId],
        );
        assert.equal(stored.rows.length, 1);
        assert.deepEqual(stored.rows[0].document, canonical.document);
      },
    );

    await t.test(
      "operation phases replay exactly and uncertain state survives restart",
      async () => {
        const sessionId = randomUUID();
        sessionIds.push(sessionId);
        const registered = await authority.registerSession(
          registrationInput(sessionId),
        );
        const input = operationInput(registered);

        const reserved = await authority.reserveOperation(input);
        assertOperationReceipt(reserved, "prepared");
        assert.equal(reserved.acquired, true);
        assert.equal(reserved.session.revision, "1");
        assert.equal(reserved.operation.revision, "0");
        assert.equal(
          reserved.reservation.expectedSessionRevision,
          "0",
        );

        const replayed = await authority.reserveOperation(
          structuredClone(input),
        );
        assertOperationReceipt(replayed, "prepared");
        assert.equal(replayed.acquired, false);
        assert.deepEqual(replayed.operation, reserved.operation);
        assert.deepEqual(replayed.reservation, reserved.reservation);
        assert.deepEqual(replayed.session, reserved.session);

        const starting = await authority.claimOperationDispatch({
          ...structuredClone(input),
          expectedOperationRevision: "0",
        });
        assertOperationReceipt(starting, "starting");
        assert.equal(starting.dispatchGranted, true);
        assert.equal(starting.session.revision, "2");
        assert.equal(starting.operation.revision, "1");

        const startingReplay =
          await authority.claimOperationDispatch({
            ...structuredClone(input),
            expectedOperationRevision: "0",
          });
        assertOperationReceipt(startingReplay, "starting");
        assert.equal(startingReplay.dispatchGranted, false);
        assert.deepEqual(startingReplay.session, starting.session);

        const uncertain = await authority.markOperationUncertain({
          ...structuredClone(input),
          expectedOperationRevision: "1",
        });
        assertOperationReceipt(uncertain, "uncertain");
        assert.equal(uncertain.changed, true);
        assert.equal(uncertain.session.revision, "3");
        assert.equal(uncertain.operation.revision, "2");

        const restarted = new PostgresSessionAuthority({ store });
        const reconciled = await restarted.reconcileOperation(
          structuredClone(input),
        );
        assertOperationReceipt(reconciled, "uncertain");
        assert.deepEqual(reconciled.session, uncertain.session);
        await assert.rejects(
          restarted.reserveOperation(
            operationInput(registered, {
              operationId: `operation-${randomUUID()}`,
            }),
          ),
          assertAuthorityCode("session_operation_conflict"),
        );

        const stored = await pool.query(
          [
            "SELECT",
            "(SELECT count(*)::integer",
            "FROM session_authority.operation_claims",
            "WHERE session_id = $1 AND retired_at IS NULL)",
            "AS operation_count,",
            "(SELECT count(*)::integer",
            "FROM session_authority.reservations",
            "WHERE session_id = $1 AND released_at IS NULL)",
            "AS reservation_count",
          ].join(" "),
          [sessionId],
        );
        assert.deepEqual(stored.rows[0], {
          operation_count: 1,
          reservation_count: 1,
        });
      },
    );

    await t.test(
      "dispatch COMMIT acknowledgement loss reconciles without regranting",
      async () => {
        const sessionId = randomUUID();
        sessionIds.push(sessionId);
        const registered = await authority.registerSession(
          registrationInput(sessionId),
        );
        const input = operationInput(registered);
        await authority.reserveOperation(input);
        const transition = {
          ...structuredClone(input),
          expectedOperationRevision: "0",
        };
        const acknowledgementLossStore =
          new PostgresSerializableStore({
            dedicatedPool:
              firstCommitAcknowledgementLossPool(pool),
          });
        const acknowledgementLossAuthority =
          new PostgresSessionAuthority({
            store: acknowledgementLossStore,
          });

        await assert.rejects(
          acknowledgementLossAuthority.claimOperationDispatch(
            transition,
          ),
          (error) => {
            assert.ok(
              error instanceof PostgresSerializableStoreError,
            );
            assert.equal(
              error.code,
              "transaction_commit_outcome_uncertain",
            );
            assert.equal(error.commitState, "uncertain");
            assert.equal(error.retryable, false);
            assert.equal("cause" in error, false);
            return true;
          },
        );

        const restarted = new PostgresSessionAuthority({ store });
        const reconciled = await restarted.reconcileOperation(
          structuredClone(input),
        );
        assertOperationReceipt(reconciled, "starting");
        assert.equal(reconciled.session.revision, "2");
        assert.equal(reconciled.operation.revision, "1");

        for (let replayIndex = 0; replayIndex < 2; replayIndex += 1) {
          const replayed =
            await restarted.claimOperationDispatch(
              structuredClone(transition),
            );
          assertOperationReceipt(replayed, "starting");
          assert.equal(replayed.dispatchGranted, false);
          assert.deepEqual(replayed.session, reconciled.session);
          assert.deepEqual(replayed.operation, reconciled.operation);
          assert.deepEqual(
            replayed.reservation,
            reconciled.reservation,
          );
        }
      },
    );

    await t.test(
      "prepared cancellation releases the blocker and replays one terminal result",
      async () => {
        const sessionId = randomUUID();
        sessionIds.push(sessionId);
        const registered = await authority.registerSession(
          registrationInput(sessionId),
        );
        const input = operationInput(registered);
        await authority.reserveOperation(input);

        const cancellation = {
          ...structuredClone(input),
          expectedOperationRevision: "0",
          reason: "caller-abandoned-before-dispatch",
        };
        const cancelled =
          await authority.cancelPreparedOperation(cancellation);
        assertOperationReceipt(cancelled, "committed");
        assert.equal(cancelled.cancelled, true);
        assert.equal(cancelled.session.revision, "2");
        assert.deepEqual(cancelled.operation.result, {
          resultVersion: 1,
          outcome: "cancelled-before-dispatch",
          reason: cancellation.reason,
        });

        const replayed =
          await authority.cancelPreparedOperation(
            structuredClone(cancellation),
          );
        assertOperationReceipt(replayed, "committed");
        assert.equal(replayed.cancelled, false);
        assert.deepEqual(replayed.operation, cancelled.operation);
        assert.deepEqual(replayed.reservation, cancelled.reservation);

        const replacementInput = operationInput(cancelled.session);
        const replacement = await authority.reserveOperation(
          replacementInput,
        );
        assertOperationReceipt(replacement, "prepared");
        assert.equal(replacement.acquired, true);
        assert.equal(replacement.session.revision, "3");
        assert.deepEqual(
          replacement.session.document.lastOperation,
          cancelled.session.document.lastOperation,
        );

        const replacementCancellation = {
          ...structuredClone(replacementInput),
          expectedOperationRevision: "0",
          reason: "replacement-abandoned-before-dispatch",
        };
        const replacementCancelled =
          await authority.cancelPreparedOperation(
            replacementCancellation,
          );
        assertOperationReceipt(replacementCancelled, "committed");
        assert.equal(replacementCancelled.cancelled, true);
        assert.equal(replacementCancelled.session.revision, "4");
        assert.equal(
          replacementCancelled.session.document.lastOperation.operationId,
          replacementInput.operationId,
        );
        assert.notEqual(
          replacementCancelled.session.document.lastOperation.operationId,
          cancelled.session.document.lastOperation.operationId,
        );

        const restarted = new PostgresSessionAuthority({ store });
        const readback = await restarted.readSession({ sessionId });
        assert.deepEqual(readback, replacementCancelled.session);

        const historical = await restarted.reconcileOperation(
          structuredClone(input),
        );
        assertOperationReceipt(historical, "committed", {
          currentTerminal: false,
        });
        assert.deepEqual(historical.session, replacementCancelled.session);
        assert.deepEqual(historical.operation, cancelled.operation);
        assert.deepEqual(historical.reservation, cancelled.reservation);

        const historicalReplay =
          await restarted.cancelPreparedOperation(
            structuredClone(cancellation),
          );
        assertOperationReceipt(historicalReplay, "committed", {
          currentTerminal: false,
        });
        assert.equal(historicalReplay.cancelled, false);
        assert.deepEqual(
          historicalReplay.session,
          replacementCancelled.session,
        );
        assert.deepEqual(historicalReplay.operation, cancelled.operation);
        assert.deepEqual(
          historicalReplay.reservation,
          cancelled.reservation,
        );

        const terminalReplay =
          await restarted.cancelPreparedOperation(
            structuredClone(replacementCancellation),
          );
        assertOperationReceipt(terminalReplay, "committed");
        assert.equal(terminalReplay.cancelled, false);
        assert.deepEqual(
          terminalReplay.session,
          replacementCancelled.session,
        );
        assert.deepEqual(
          terminalReplay.operation,
          replacementCancelled.operation,
        );
        assert.deepEqual(
          terminalReplay.reservation,
          replacementCancelled.reservation,
        );

        const terminalRows = await pool.query(
          [
            "SELECT o.state AS operation_state,",
            "r.state AS reservation_state,",
            "r.expected_session_revision::text",
            "AS expected_session_revision",
            "FROM session_authority.operation_claims o",
            "JOIN session_authority.reservations r",
            "ON r.operation_id = o.operation_id",
            "WHERE o.session_id = $1",
            "ORDER BY r.expected_session_revision",
          ].join(" "),
          [sessionId],
        );
        assert.deepEqual(terminalRows.rows, [
          {
            expected_session_revision: "0",
            operation_state: "committed",
            reservation_state: "released",
          },
          {
            expected_session_revision: "2",
            operation_state: "committed",
            reservation_state: "released",
          },
        ]);
      },
    );

    await t.test(
      "downgraded active v1 documents fail closed without phase repair",
      async () => {
        const sessionId = randomUUID();
        sessionIds.push(sessionId);
        const registered = await authority.registerSession(
          registrationInput(sessionId),
        );
        const input = operationInput(registered);
        const reserved = await authority.reserveOperation(input);
        assertOperationReceipt(reserved, "prepared");

        const downgraded = structuredClone(reserved.session.document);
        downgraded.documentVersion = 1;
        Reflect.deleteProperty(downgraded, "lastOperation");
        await pool.query(
          [
            "UPDATE session_authority.sessions",
            "SET document = $2::jsonb",
            "WHERE session_id = $1",
          ].join(" "),
          [sessionId, JSON.stringify(downgraded)],
        );

        await assert.rejects(
          authority.readSession({ sessionId }),
          assertAuthorityCode("session_state_invalid"),
        );
        await assert.rejects(
          authority.claimOperationDispatch({
            ...structuredClone(input),
            expectedOperationRevision: "0",
          }),
          assertAuthorityCode("session_state_invalid"),
        );

        const stored = await pool.query(
          [
            "SELECT s.revision::text AS session_revision,",
            "s.document ->> 'documentVersion' AS document_version,",
            "o.state AS operation_state,",
            "o.revision::text AS operation_revision,",
            "r.state AS reservation_state",
            "FROM session_authority.sessions s",
            "JOIN session_authority.operation_claims o",
            "ON o.session_id = s.session_id",
            "JOIN session_authority.reservations r",
            "ON r.operation_id = o.operation_id",
            "WHERE s.session_id = $1",
          ].join(" "),
          [sessionId],
        );
        assert.deepEqual(stored.rows, [
          {
            document_version: "1",
            operation_revision: "0",
            operation_state: "prepared",
            reservation_state: "prepared",
            session_revision: "1",
          },
        ]);
      },
    );

    await t.test(
      "missing terminal anchor rows fail closed before a replacement claim",
      async () => {
        const sessionId = randomUUID();
        sessionIds.push(sessionId);
        const registered = await authority.registerSession(
          registrationInput(sessionId),
        );
        const input = operationInput(registered);
        await authority.reserveOperation(input);
        const cancelled =
          await authority.cancelPreparedOperation({
            ...structuredClone(input),
            expectedOperationRevision: "0",
            reason: "terminal-anchor-corruption-probe",
          });

        await pool.query(
          "DELETE FROM session_authority.reservations WHERE operation_id = $1",
          [input.operationId],
        );
        await pool.query(
          "DELETE FROM session_authority.operation_claims WHERE operation_id = $1",
          [input.operationId],
        );

        await assert.rejects(
          authority.readSession({ sessionId }),
          assertAuthorityCode("operation_state_invalid"),
        );
        await assert.rejects(
          authority.reserveOperation(
            operationInput(cancelled.session),
          ),
          assertAuthorityCode("operation_state_invalid"),
        );
        const claims = await pool.query(
          [
            "SELECT",
            "(SELECT count(*)::integer",
            "FROM session_authority.operation_claims",
            "WHERE session_id = $1) AS operation_count,",
            "(SELECT count(*)::integer",
            "FROM session_authority.reservations",
            "WHERE session_id = $1) AS reservation_count",
          ].join(" "),
          [sessionId],
        );
        assert.deepEqual(claims.rows[0], {
          operation_count: 0,
          reservation_count: 0,
        });
      },
    );

    await t.test(
      "concurrent identical reserve converges on one durable claim",
      async () => {
        const sessionId = randomUUID();
        sessionIds.push(sessionId);
        const registered = await authority.registerSession(
          registrationInput(sessionId),
        );
        const input = operationInput(registered);
        const concurrentStore = new PostgresSerializableStore({
          dedicatedPool: firstSessionLockQueryBarrierPool(
            pool,
            2,
            "identical operation reserve barrier",
          ),
          maxTransactionAttempts: 3,
        });
        const concurrentAuthority = new PostgresSessionAuthority({
          store: concurrentStore,
        });

        const receipts = await Promise.all([
          concurrentAuthority.reserveOperation(input),
          concurrentAuthority.reserveOperation(structuredClone(input)),
        ]);
        assert.equal(
          receipts.filter(({ acquired }) => acquired).length,
          1,
        );
        assert.equal(
          receipts.filter(({ acquired }) => !acquired).length,
          1,
        );
        assert.deepEqual(receipts[0].operation, receipts[1].operation);
        assert.deepEqual(
          receipts[0].reservation,
          receipts[1].reservation,
        );
        assert.deepEqual(receipts[0].session, receipts[1].session);

        const stored = await pool.query(
          [
            "SELECT s.revision,",
            "(SELECT count(*)::integer",
            "FROM session_authority.operation_claims o",
            "WHERE o.session_id = s.session_id) AS operation_count,",
            "(SELECT count(*)::integer",
            "FROM session_authority.reservations r",
            "WHERE r.session_id = s.session_id) AS reservation_count",
            "FROM session_authority.sessions s",
            "WHERE s.session_id = $1",
          ].join(" "),
          [sessionId],
        );
        assert.equal(stored.rows[0].revision, "1");
        assert.equal(stored.rows[0].operation_count, 1);
        assert.equal(stored.rows[0].reservation_count, 1);
      },
    );

    await t.test(
      "concurrent different operations admit exactly one per session",
      async () => {
        const sessionId = randomUUID();
        sessionIds.push(sessionId);
        const registered = await authority.registerSession(
          registrationInput(sessionId),
        );
        const first = operationInput(registered);
        const second = operationInput(registered);
        const concurrentStore = new PostgresSerializableStore({
          dedicatedPool: firstSessionLockQueryBarrierPool(
            pool,
            2,
            "conflicting operation reserve barrier",
          ),
          maxTransactionAttempts: 3,
        });
        const concurrentAuthority = new PostgresSessionAuthority({
          store: concurrentStore,
        });

        const outcomes = await Promise.allSettled([
          concurrentAuthority.reserveOperation(first),
          concurrentAuthority.reserveOperation(second),
        ]);
        const fulfilled = outcomes.filter(
          ({ status }) => status === "fulfilled",
        );
        const rejected = outcomes.filter(
          ({ status }) => status === "rejected",
        );
        assert.equal(fulfilled.length, 1);
        assert.equal(rejected.length, 1);
        assertAuthorityCode("session_operation_conflict")(
          rejected[0].reason,
        );
        assertOperationReceipt(fulfilled[0].value, "prepared");
      },
    );

    await t.test(
      "a global operation ID binds exactly one session under concurrency",
      async () => {
        const firstSessionId = randomUUID();
        const secondSessionId = randomUUID();
        sessionIds.push(firstSessionId, secondSessionId);
        const firstSession = await authority.registerSession(
          registrationInput(firstSessionId),
        );
        const secondSession = await authority.registerSession(
          registrationInput(secondSessionId),
        );
        const operationId = `operation-${randomUUID()}`;
        const request = {
          action: "reserve-writer",
          nonce: randomUUID(),
        };
        const first = operationInput(firstSession, {
          operationId,
          request,
        });
        const second = operationInput(secondSession, {
          operationId,
          request: structuredClone(request),
        });
        const concurrentStore = new PostgresSerializableStore({
          dedicatedPool: firstSessionLockQueryBarrierPool(
            pool,
            2,
            "global operation identity barrier",
          ),
          maxTransactionAttempts: 3,
        });
        const concurrentAuthority = new PostgresSessionAuthority({
          store: concurrentStore,
        });

        const outcomes = await Promise.allSettled([
          concurrentAuthority.reserveOperation(first),
          concurrentAuthority.reserveOperation(second),
        ]);
        const fulfilledIndex = outcomes.findIndex(
          ({ status }) => status === "fulfilled",
        );
        const rejectedIndex = outcomes.findIndex(
          ({ status }) => status === "rejected",
        );
        assert.notEqual(fulfilledIndex, -1);
        assert.notEqual(rejectedIndex, -1);
        assertAuthorityCode("operation_identity_conflict")(
          outcomes[rejectedIndex].reason,
        );
        const losingSession =
          rejectedIndex === 0 ? firstSession : secondSession;
        assert.deepEqual(
          await authority.readSession({
            sessionId: losingSession.sessionId,
          }),
          losingSession,
        );
      },
    );

    await t.test(
      "concurrent dispatch claims grant exactly once",
      async () => {
        const sessionId = randomUUID();
        sessionIds.push(sessionId);
        const registered = await authority.registerSession(
          registrationInput(sessionId),
        );
        const input = operationInput(registered);
        await authority.reserveOperation(input);
        const concurrentStore = new PostgresSerializableStore({
          dedicatedPool: firstSessionLockQueryBarrierPool(
            pool,
            2,
            "dispatch claim barrier",
          ),
          maxTransactionAttempts: 3,
        });
        const concurrentAuthority = new PostgresSessionAuthority({
          store: concurrentStore,
        });
        const transition = {
          ...input,
          expectedOperationRevision: "0",
        };

        const receipts = await Promise.all([
          concurrentAuthority.claimOperationDispatch(transition),
          concurrentAuthority.claimOperationDispatch(
            structuredClone(transition),
          ),
        ]);
        assert.equal(
          receipts.filter(({ dispatchGranted }) => dispatchGranted)
            .length,
          1,
        );
        assert.equal(
          receipts.filter(({ dispatchGranted }) => !dispatchGranted)
            .length,
          1,
        );
        assert.deepEqual(receipts[0].operation, receipts[1].operation);
        assert.equal(receipts[0].operation.state, "starting");
        assert.equal(receipts[0].session.revision, "2");
      },
    );

    await t.test(
      "concurrent writer dispatch allocates one post-lock lease and replays exactly",
      async () => {
        const sessionId = randomUUID();
        sessionIds.push(sessionId);
        const registered = await authority.registerSession(
          registrationInput(sessionId),
        );
        const input = writerAttachmentInput(registered, {
          leaseDurationMilliseconds: 500,
        });
        await authority.reserveOperation(input);
        const barrierPool = firstSessionLockQueryBarrierPool(
          pool,
          2,
          "writer dispatch claim barrier",
        );
        const concurrentAuthority = new PostgresSessionAuthority({
          store: new PostgresSerializableStore({
            dedicatedPool: barrierPool,
            maxTransactionAttempts: 3,
          }),
        });
        const transition = {
          ...structuredClone(input),
          expectedOperationRevision: "0",
        };
        const lockClient = await pool.connect();
        let lockHeld = false;
        let lockReleaseAt;
        let receipts;
        try {
          await lockClient.query("BEGIN");
          lockHeld = true;
          await lockClient.query(
            [
              "SELECT session_id",
              "FROM session_authority.sessions",
              "WHERE session_id = $1",
              "FOR UPDATE",
            ].join(" "),
            [sessionId],
          );

          const firstClaim =
            concurrentAuthority.claimWriterAttachmentDispatch(
              transition,
            );
          const secondClaim =
            concurrentAuthority.claimWriterAttachmentDispatch(
              structuredClone(transition),
            );
          const releaseLock = (async () => {
            await barrierPool.waitForBarrier();
            await lockClient.query(
              "SELECT pg_catalog.pg_sleep(0.75)",
            );
            const observed = await lockClient.query(
              [
                "SELECT pg_catalog.clock_timestamp()",
                "AS lock_release_at",
              ].join(" "),
            );
            await lockClient.query("ROLLBACK");
            lockHeld = false;
            return observed.rows[0].lock_release_at;
          })();

          [lockReleaseAt, ...receipts] = await Promise.all([
            releaseLock,
            firstClaim,
            secondClaim,
          ]);
        } finally {
          if (lockHeld) {
            await lockClient.query("ROLLBACK");
          }
          lockClient.release();
        }

        const granted = receipts.find(
          ({ dispatchGranted }) => dispatchGranted,
        );
        const replay = receipts.find(
          ({ dispatchGranted }) => !dispatchGranted,
        );
        assert.notEqual(granted, undefined);
        assert.notEqual(replay, undefined);
        assert.equal(
          receipts.filter(({ dispatchGranted }) => dispatchGranted)
            .length,
          1,
        );
        assert.equal(
          receipts.filter(({ dispatchGranted }) => !dispatchGranted)
            .length,
          1,
        );
        assertOperationReceipt(granted, "starting");
        assertOperationReceipt(replay, "starting");
        assert.deepEqual(replay.operation, granted.operation);
        assert.deepEqual(replay.reservation, granted.reservation);
        assert.deepEqual(replay.session, granted.session);
        assert.deepEqual(replay.lease, granted.lease);
        assert.deepEqual(
          replay.mutationRequest,
          granted.mutationRequest,
        );
        assert.equal(Object.hasOwn(replay, "authorityNow"), false);
        assert.equal(granted.session.document.writerEpoch, "1");
        assert.deepEqual(
          granted.session.document.lease,
          granted.lease,
        );
        assert.ok(
          Date.parse(granted.authorityNow) >=
            lockReleaseAt.getTime(),
        );
        assert.equal(
          Date.parse(granted.lease.expiresAt) -
            Date.parse(granted.authorityNow),
          input.request.leaseDurationMilliseconds,
        );
        assert.ok(
          Date.parse(granted.lease.expiresAt) >
            lockReleaseAt.getTime(),
        );

        const stored = await pool.query(
          [
            "SELECT s.revision::text AS revision,",
            "s.document->>'writerEpoch' AS writer_epoch,",
            "s.document->'lease'->>'leaseId' AS lease_id,",
            "(SELECT count(*)::integer",
            "FROM session_authority.operation_claims o",
            "WHERE o.operation_id = $2) AS operation_count,",
            "(SELECT count(*)::integer",
            "FROM session_authority.reservations r",
            "WHERE r.operation_id = $2) AS reservation_count",
            "FROM session_authority.sessions s",
            "WHERE s.session_id = $1",
          ].join(" "),
          [sessionId, input.operationId],
        );
        assert.deepEqual(stored.rows[0], {
          lease_id: granted.lease.leaseId,
          operation_count: 1,
          reservation_count: 1,
          revision: "2",
          writer_epoch: "1",
        });
      },
    );

    await t.test(
      "writer attachment and lease renewal persist exact typed authority",
      async () => {
        const sessionId = randomUUID();
        sessionIds.push(sessionId);
        const registered = await authority.registerSession(
          registrationInput(sessionId),
        );
        const input = writerAttachmentInput(registered);

        const reserved = await authority.reserveOperation(input);
        assertOperationReceipt(reserved, "prepared");
        assert.equal(reserved.session.document.lifecycle, "DETACHED");
        assert.equal(reserved.session.document.writerEpoch, "0");

        const starting =
          await authority.claimWriterAttachmentDispatch({
            ...structuredClone(input),
            expectedOperationRevision: "0",
          });
        assertOperationReceipt(starting, "starting");
        assert.equal(starting.dispatchGranted, true);
        assert.equal(starting.session.revision, "2");
        assert.equal(starting.session.document.lifecycle, "ATTACHING");
        assert.equal(starting.session.document.writerEpoch, "1");
        assert.deepEqual(starting.session.document.lease, starting.lease);
        assert.equal(starting.session.document.attachment, null);
        assert.equal(
          Date.parse(starting.lease.expiresAt),
          Date.parse(starting.authorityNow) +
            input.request.leaseDurationMilliseconds,
        );

        const replay =
          await authority.claimWriterAttachmentDispatch({
            ...structuredClone(input),
            expectedOperationRevision: "0",
          });
        assertOperationReceipt(replay, "starting");
        assert.equal(replay.dispatchGranted, false);
        assert.deepEqual(replay.session, starting.session);

        const evidence = attachmentEvidence(starting.mutationRequest);
        const attached = await authority.finalizeWriterAttachment({
          ...structuredClone(input),
          expectedOperationRevision: "1",
          ...evidence,
        });
        assertOperationReceipt(attached, "committed");
        assert.equal(attached.finalized, true);
        assert.equal(attached.operation.revision, "2");
        assert.equal(attached.session.revision, "3");
        assert.equal(attached.session.document.lifecycle, "ATTACHED");
        assert.deepEqual(
          attached.session.document.attachment,
          evidence.attachment,
        );
        assert.deepEqual(
          attached.operation.result.mutationResult,
          evidence.mutationResult,
        );

        const renewalInput = writerLeaseRenewalInput(attached.session);
        const renewed = await authority.renewWriterLease(renewalInput);
        assertOperationReceipt(renewed, "committed");
        assert.equal(renewed.renewed, true);
        assert.equal(renewed.operation.revision, "0");
        assert.equal(renewed.session.revision, "4");
        assert.equal(
          renewed.session.document.writerEpoch,
          attached.session.document.writerEpoch,
        );
        assert.equal(
          renewed.session.document.lease.leaseId,
          attached.session.document.lease.leaseId,
        );
        assert.equal(
          renewed.session.document.lease.holderId,
          attached.session.document.lease.holderId,
        );
        assert.ok(
          Date.parse(renewed.session.document.lease.expiresAt) >
            Date.parse(attached.session.document.lease.expiresAt),
        );
        assert.deepEqual(
          renewed.session.document.attachment,
          attached.session.document.attachment,
        );

        const renewedReplay = await authority.renewWriterLease(
          structuredClone(renewalInput),
        );
        assertOperationReceipt(renewedReplay, "committed");
        assert.equal(renewedReplay.renewed, false);
        assert.deepEqual(renewedReplay.operation, renewed.operation);
        assert.deepEqual(renewedReplay.reservation, renewed.reservation);
        assert.deepEqual(renewedReplay.session, renewed.session);

        const stored = await pool.query(
          [
            "SELECT kind, state, revision::text, result->>'outcome' AS outcome",
            "FROM session_authority.operation_claims",
            "WHERE session_id = $1",
          ].join(" "),
          [sessionId],
        );
        assert.deepEqual(
          stored.rows
            .map(({ kind, outcome, revision, state }) => ({
              kind,
              outcome,
              revision,
              state,
            }))
            .sort((left, right) => left.kind.localeCompare(right.kind)),
          [
            {
              kind: WRITER_ATTACHMENT_ACQUIRE_OPERATION_KIND,
              outcome: "writer-attached",
              revision: "2",
              state: "committed",
            },
            {
              kind: WRITER_LEASE_RENEW_OPERATION_KIND,
              outcome: "writer-lease-renewed",
              revision: "0",
              state: "committed",
            },
          ],
        );
      },
    );

    await t.test(
      "writer renewal checks the authority clock after a blocking row lock",
      async () => {
        const sessionId = randomUUID();
        sessionIds.push(sessionId);
        const registered = await authority.registerSession(
          registrationInput(sessionId),
        );
        const attached = await attachWriter(authority, registered, {
          leaseDurationMilliseconds: 3_000,
        });
        const renewalInput = writerLeaseRenewalInput(
          attached.session,
        );
        const storedAuthorityQuery = [
          "SELECT to_jsonb(s) AS session,",
          "(SELECT jsonb_agg(to_jsonb(o) ORDER BY o.operation_id)",
          "FROM session_authority.operation_claims o",
          "WHERE o.session_id = s.session_id) AS operations,",
          "(SELECT jsonb_agg(to_jsonb(r) ORDER BY r.reservation_id)",
          "FROM session_authority.reservations r",
          "WHERE r.session_id = s.session_id) AS reservations",
          "FROM session_authority.sessions s",
          "WHERE s.session_id = $1",
        ].join(" ");
        const storedBefore = await pool.query(
          storedAuthorityQuery,
          [sessionId],
        );
        const lockClient = await pool.connect();
        let lockHeld = false;
        try {
          await lockClient.query("BEGIN");
          lockHeld = true;
          await lockClient.query(
            [
              "SELECT session_id",
              "FROM session_authority.sessions",
              "WHERE session_id = $1",
              "FOR UPDATE",
            ].join(" "),
            [sessionId],
          );

          const notification =
            firstSessionLockQueryNotificationPool(
              pool,
              "blocked writer renewal session lock",
            );
          const blockedAuthority =
            new PostgresSessionAuthority({
              store: new PostgresSerializableStore({
                dedicatedPool: notification.dedicatedPool,
                maxTransactionAttempts: 3,
              }),
            });
          let renewalSettled = false;
          const renewalPromise = blockedAuthority.renewWriterLease(
            renewalInput,
          );
          const expectedRejection = assert.rejects(
            renewalPromise,
            assertAuthorityCode("writer_lease_expired"),
          );
          void renewalPromise.then(
            () => {
              renewalSettled = true;
            },
            () => {
              renewalSettled = true;
            },
          );
          await notification.waitForFirstMatch();
          assert.equal(renewalSettled, false);

          const beforeExpiry = await lockClient.query(
            [
              "SELECT pg_catalog.clock_timestamp() < $1::timestamptz",
              "AS lease_active",
            ].join(" "),
            [attached.session.document.lease.expiresAt],
          );
          assert.equal(beforeExpiry.rows[0].lease_active, true);
          await lockClient.query(
            [
              "SELECT pg_catalog.pg_sleep(",
              "GREATEST(EXTRACT(EPOCH FROM",
              "($1::timestamptz - pg_catalog.clock_timestamp())), 0)",
              "::double precision + 0.2)",
            ].join(" "),
            [attached.session.document.lease.expiresAt],
          );
          const afterExpiry = await lockClient.query(
            [
              "SELECT pg_catalog.clock_timestamp() >= $1::timestamptz",
              "AS lease_expired",
            ].join(" "),
            [attached.session.document.lease.expiresAt],
          );
          assert.equal(afterExpiry.rows[0].lease_expired, true);
          assert.equal(renewalSettled, false);

          await lockClient.query("ROLLBACK");
          lockHeld = false;
          await expectedRejection;
        } finally {
          if (lockHeld) {
            await lockClient.query("ROLLBACK");
          }
          lockClient.release();
        }

        assert.deepEqual(
          await authority.readSession({ sessionId }),
          attached.session,
        );
        const storedAfter = await pool.query(
          storedAuthorityQuery,
          [sessionId],
        );
        assert.deepEqual(storedAfter.rows, storedBefore.rows);
        const stored = await pool.query(
          [
            "SELECT",
            "(SELECT count(*)::integer",
            "FROM session_authority.operation_claims",
            "WHERE operation_id = $1) AS operation_count,",
            "(SELECT count(*)::integer",
            "FROM session_authority.reservations",
            "WHERE operation_id = $1) AS reservation_count",
          ].join(" "),
          [renewalInput.operationId],
        );
        assert.deepEqual(stored.rows[0], {
          operation_count: 0,
          reservation_count: 0,
        });
      },
    );

    await t.test(
      "concurrent identical writer renewal commits once and replays exactly",
      async () => {
        const sessionId = randomUUID();
        sessionIds.push(sessionId);
        const registered = await authority.registerSession(
          registrationInput(sessionId),
        );
        const attached = await attachWriter(authority, registered);
        const input = writerLeaseRenewalInput(attached.session);
        const concurrentAuthority =
          new PostgresSessionAuthority({
            store: new PostgresSerializableStore({
              dedicatedPool: firstSessionLockQueryBarrierPool(
                pool,
                2,
                "identical writer renewal barrier",
              ),
              maxTransactionAttempts: 3,
            }),
          });

        const receipts = await Promise.all([
          concurrentAuthority.renewWriterLease(input),
          concurrentAuthority.renewWriterLease(
            structuredClone(input),
          ),
        ]);
        assert.equal(
          receipts.filter(({ renewed }) => renewed).length,
          1,
        );
        assert.equal(
          receipts.filter(({ renewed }) => !renewed).length,
          1,
        );
        for (const receipt of receipts) {
          assertOperationReceipt(receipt, "committed");
        }
        assert.deepEqual(receipts[0].operation, receipts[1].operation);
        assert.deepEqual(
          receipts[0].reservation,
          receipts[1].reservation,
        );
        assert.deepEqual(receipts[0].session, receipts[1].session);
        assert.equal(receipts[0].session.revision, "4");
        assert.equal(
          receipts[0].session.document.lastOperation.operationId,
          input.operationId,
        );

        const stored = await pool.query(
          [
            "SELECT s.revision::text AS revision,",
            "(SELECT count(*)::integer",
            "FROM session_authority.operation_claims o",
            "WHERE o.operation_id = $2) AS operation_count,",
            "(SELECT count(*)::integer",
            "FROM session_authority.reservations r",
            "WHERE r.operation_id = $2) AS reservation_count",
            "FROM session_authority.sessions s",
            "WHERE s.session_id = $1",
          ].join(" "),
          [sessionId, input.operationId],
        );
        assert.deepEqual(stored.rows[0], {
          operation_count: 1,
          reservation_count: 1,
          revision: "4",
        });
      },
    );

    await t.test(
      "concurrent distinct writer renewals reject the stale snapshot",
      async () => {
        const sessionId = randomUUID();
        sessionIds.push(sessionId);
        const registered = await authority.registerSession(
          registrationInput(sessionId),
        );
        const attached = await attachWriter(authority, registered);
        const first = writerLeaseRenewalInput(attached.session);
        const second = writerLeaseRenewalInput(attached.session);
        const concurrentAuthority =
          new PostgresSessionAuthority({
            store: new PostgresSerializableStore({
              dedicatedPool: firstSessionLockQueryBarrierPool(
                pool,
                2,
                "distinct writer renewal barrier",
              ),
              maxTransactionAttempts: 3,
            }),
          });

        const outcomes = await Promise.allSettled([
          concurrentAuthority.renewWriterLease(first),
          concurrentAuthority.renewWriterLease(second),
        ]);
        const fulfilled = outcomes.filter(
          ({ status }) => status === "fulfilled",
        );
        const rejected = outcomes.filter(
          ({ status }) => status === "rejected",
        );
        assert.equal(fulfilled.length, 1);
        assert.equal(rejected.length, 1);
        assertOperationReceipt(fulfilled[0].value, "committed");
        assert.equal(fulfilled[0].value.renewed, true);
        assertAuthorityCode("session_revision_conflict")(
          rejected[0].reason,
        );
        assert.equal(fulfilled[0].value.session.revision, "4");

        const stored = await pool.query(
          [
            "SELECT s.revision::text AS revision,",
            "(SELECT count(*)::integer",
            "FROM session_authority.operation_claims o",
            "WHERE o.operation_id = ANY($2::text[]))",
            "AS operation_count,",
            "(SELECT count(*)::integer",
            "FROM session_authority.reservations r",
            "WHERE r.operation_id = ANY($2::text[]))",
            "AS reservation_count",
            "FROM session_authority.sessions s",
            "WHERE s.session_id = $1",
          ].join(" "),
          [sessionId, [first.operationId, second.operationId]],
        );
        assert.deepEqual(stored.rows[0], {
          operation_count: 1,
          reservation_count: 1,
          revision: "4",
        });
      },
    );

    await t.test(
      "typed dispatch COMMIT loss preserves one epoch and never regrants",
      async () => {
        const sessionId = randomUUID();
        sessionIds.push(sessionId);
        const registered = await authority.registerSession(
          registrationInput(sessionId),
        );
        const input = writerAttachmentInput(registered);
        await authority.reserveOperation(input);
        const transition = {
          ...structuredClone(input),
          expectedOperationRevision: "0",
        };
        const acknowledgementLossAuthority =
          new PostgresSessionAuthority({
            store: new PostgresSerializableStore({
              dedicatedPool:
                firstCommitAcknowledgementLossPool(pool),
            }),
          });

        await assert.rejects(
          acknowledgementLossAuthority.claimWriterAttachmentDispatch(
            transition,
          ),
          (error) => {
            assert.ok(
              error instanceof PostgresSerializableStoreError,
            );
            assert.equal(
              error.code,
              "transaction_commit_outcome_uncertain",
            );
            assert.equal(error.commitState, "uncertain");
            return true;
          },
        );

        const reconciled = await authority.reconcileOperation(input);
        assertOperationReceipt(reconciled, "starting");
        assert.equal(
          reconciled.session.document.lifecycle,
          "ATTACHING",
        );
        assert.equal(reconciled.session.document.writerEpoch, "1");
        const replay =
          await authority.claimWriterAttachmentDispatch(transition);
        assertOperationReceipt(replay, "starting");
        assert.equal(replay.dispatchGranted, false);
        assert.equal(replay.session.document.writerEpoch, "1");
        assert.deepEqual(replay.session, reconciled.session);
      },
    );

    await t.test(
      "attachment finalize COMMIT loss reconciles the exact proof",
      async () => {
        const sessionId = randomUUID();
        sessionIds.push(sessionId);
        const registered = await authority.registerSession(
          registrationInput(sessionId),
        );
        const input = writerAttachmentInput(registered);
        await authority.reserveOperation(input);
        const starting =
          await authority.claimWriterAttachmentDispatch({
            ...structuredClone(input),
            expectedOperationRevision: "0",
          });
        const finalization = {
          ...structuredClone(input),
          expectedOperationRevision: "1",
          ...attachmentEvidence(starting.mutationRequest),
        };
        const acknowledgementLossAuthority =
          new PostgresSessionAuthority({
            store: new PostgresSerializableStore({
              dedicatedPool:
                firstCommitAcknowledgementLossPool(pool),
            }),
          });

        await assert.rejects(
          acknowledgementLossAuthority.finalizeWriterAttachment(
            finalization,
          ),
          (error) => {
            assert.ok(
              error instanceof PostgresSerializableStoreError,
            );
            assert.equal(
              error.code,
              "transaction_commit_outcome_uncertain",
            );
            assert.equal(error.commitState, "uncertain");
            return true;
          },
        );

        const reconciled = await authority.reconcileOperation(input);
        assertOperationReceipt(reconciled, "committed");
        assert.equal(reconciled.operation.revision, "2");
        assert.equal(
          reconciled.session.document.lifecycle,
          "ATTACHED",
        );
        const replay =
          await authority.finalizeWriterAttachment(finalization);
        assertOperationReceipt(replay, "committed");
        assert.equal(replay.finalized, false);
        assert.deepEqual(replay.operation, reconciled.operation);
        assert.deepEqual(replay.session, reconciled.session);
      },
    );
  },
);
