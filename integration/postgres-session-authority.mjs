import assert from "node:assert/strict";
import { randomUUID } from "node:crypto";
import test from "node:test";

import { Pool } from "pg";

import {
  PostgresSerializableStore,
  PostgresSerializableStoreError,
} from "../src/postgres-serializable-store.mjs";

const EMPTY_JSON_OBJECT = "{}";
const databaseUrl = process.env.SESSION_AUTHORITY_DATABASE_URL;
const databaseConfigured =
  typeof databaseUrl === "string" && databaseUrl.length > 0;

if (!databaseConfigured) {
  throw new Error(
    "SESSION_AUTHORITY_DATABASE_URL is required for the PostgreSQL integration gate",
  );
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
