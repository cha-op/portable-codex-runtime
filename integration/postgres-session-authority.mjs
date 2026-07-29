import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";

import { Pool } from "pg";

import {
  PostgresSessionAuthority,
  PostgresSessionAuthorityError,
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

function assertOperationReceipt(receipt, state) {
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
  assert.equal(Object.isFrozen(receipt), true);
  assert.equal(Object.isFrozen(receipt.operation), true);
  assert.equal(Object.isFrozen(receipt.reservation), true);
  assert.equal(Object.isFrozen(receipt.session), true);
}

function assertInitialSession(snapshot, input) {
  assert.equal(snapshot.sessionId, input.manifest.sessionId);
  assert.equal(snapshot.revision, "0");
  assert.deepEqual(snapshot.document, {
    documentVersion: 1,
    manifest: input.manifest,
    storageRef: input.storageRef,
    backendCapabilities: input.backendCapabilities,
    lifecycle: "DETACHED",
    writerEpoch: "0",
    lease: null,
    attachment: null,
    activeOperation: null,
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
      max: 2,
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

        const replacement = await authority.reserveOperation(
          operationInput(cancelled.session),
        );
        assertOperationReceipt(replacement, "prepared");
        assert.equal(replacement.acquired, true);
        assert.equal(replacement.session.revision, "3");
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
  },
);
