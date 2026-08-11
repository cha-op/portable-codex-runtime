import assert from "node:assert/strict";
import { createHash, randomUUID } from "node:crypto";
import { readFile } from "node:fs/promises";
import test from "node:test";

import { Pool } from "pg";

import {
  operationJournalBindingSha256,
} from "../src/filesystem-operation-journal.mjs";
import {
  PlatformImageReservationCoordinator,
} from "../src/platform-image-reservation.mjs";
import {
  PostgresCheckpointMutationAuthorityError,
  createPostgresCheckpointMutationAuthority,
} from "../src/postgres-checkpoint-mutation-authority.mjs";
import {
  createPostgresCheckpointRecoveryService,
} from "../src/postgres-checkpoint-recovery-service.mjs";
import {
  PostgresOperationGuard,
} from "../src/postgres-operation-guard.mjs";
import {
  createPostgresRestoreActivationRecoveryService,
} from "../src/postgres-restore-activation-recovery-service.mjs";
import {
  createPostgresRestoreLifecycleGuard,
} from "../src/postgres-restore-lifecycle-guard.mjs";
import {
  createPostgresRestoreRecoveryRunner,
} from "../src/postgres-restore-recovery-runner.mjs";
import {
  createPostgresRestoreRecoveryScheduler,
} from "../src/postgres-restore-recovery-scheduler.mjs";
import {
  PostgresWriterDetachCompositionError,
  createPostgresWriterDetachComposition,
} from "../src/postgres-writer-detach-composition.mjs";
import {
  PostgresLogicalWriterLauncherError,
  createPostgresLogicalWriterLauncher,
} from "../src/postgres-logical-writer-launcher.mjs";
import {
  CHECKPOINT_CAPTURE_OPERATION_KIND,
  PostgresSessionAuthority,
  PostgresSessionAuthorityError,
  RESTORE_ATTACHMENT_ACTIVATION_OPERATION_KIND,
  RESTORE_DESTINATION_GENERATION_OPERATION_KIND,
  SESSION_AUTHORITY_DOCUMENT_VERSION,
  WRITER_ATTACHMENT_ACQUIRE_OPERATION_KIND,
  WRITER_FORCE_FENCE_OPERATION_KIND,
  WRITER_LAUNCH_ATTEMPT_OPERATION_KIND,
  WRITER_LAUNCH_STOP_OPERATION_KIND,
  WRITER_LEASE_RENEW_OPERATION_KIND,
  WRITER_RELEASE_OPERATION_KIND,
  createCheckpointCaptureOperationRequest,
  createRestoreAttachmentActivationOperationRequest,
  createRestoreAttachmentActivationOperationRequestV2,
  createRestoreDestinationGenerationOperationRequest,
  createRestoreDestinationGenerationOperationRequestV2,
  createWriterLaunchAttemptOperationRequest,
  createWriterLaunchStopOperationRequest,
  assertWriterLaunchStopCaptureHandoffProof,
} from "../src/postgres-session-authority.mjs";
import {
  PostgresSerializableStore,
  PostgresSerializableStoreError,
  SESSION_AUTHORITY_MIGRATION_VERSION,
} from "../src/postgres-serializable-store.mjs";
import {
  PostgresRestoreRecoveryCursorStoreError,
  createPostgresRestoreRecoveryCursorStore,
} from "../src/postgres-restore-recovery-cursor-store.mjs";
import { createSessionManifest } from "../src/session-storage-contracts.mjs";
import {
  STOPPED_WRITER_STOP_CONFIRMED,
  StoppedWriterCapabilityCoordinator,
} from "../src/stopped-writer-capability.mjs";

const EMPTY_JSON_OBJECT = "{}";
const IMAGE_DIGEST = `sha256:${"a".repeat(64)}`;
const CHECKPOINT_GUARD_APPLICATION_NAME =
  "portable-codex-runtime-checkpoint-guard-integration-test";
const SESSION_AUTHORITY_APPLICATION_NAME =
  "portable-codex-runtime-session-registry-integration-test";
const databaseUrl = process.env.SESSION_AUTHORITY_DATABASE_URL;
const databaseConfigured =
  typeof databaseUrl === "string" && databaseUrl.length > 0;
const AUTHORITY_MIGRATIONS = Object.freeze([
  Object.freeze({
    url: new URL(
      "../migrations/authority/001-session-authority.sql",
      import.meta.url,
    ),
    version: 1,
  }),
  Object.freeze({
    url: new URL(
      "../migrations/authority/002-restore-destination-generations.sql",
      import.meta.url,
    ),
    version: 2,
  }),
  Object.freeze({
    url: new URL(
      "../migrations/authority/003-operation-id-registry.sql",
      import.meta.url,
    ),
    version: 3,
  }),
  Object.freeze({
    url: new URL(
      "../migrations/authority/004-restore-attachment-activation.sql",
      import.meta.url,
    ),
    version: 4,
  }),
  Object.freeze({
    url: new URL(
      "../migrations/authority/005-restore-recovery-cursors.sql",
      import.meta.url,
    ),
    version: 5,
  }),
  Object.freeze({
    url: new URL(
      "../migrations/authority/006-writer-stop-capture-handoff.sql",
      import.meta.url,
    ),
    version: 6,
  }),
]);

if (!databaseConfigured) {
  throw new Error(
    "SESSION_AUTHORITY_DATABASE_URL is required for the PostgreSQL integration gate",
  );
}

async function readTrackedAuthorityMigrations() {
  return Promise.all(
    AUTHORITY_MIGRATIONS.map(async ({ url, version }) => {
      const sql = await readFile(url, "utf8");
      assert.notEqual(sql.length, 0);
      assert.equal(sql.endsWith("\n"), true);
      return {
        checksum: createHash("sha256").update(sql, "utf8").digest("hex"),
        sql,
        version,
      };
    }),
  );
}

async function readMigrationLedger(pool) {
  const result = await pool.query(
    [
      "SELECT version, checksum",
      "FROM session_authority.schema_migrations",
      "ORDER BY version",
    ].join(" "),
  );
  return result.rows;
}

async function installAuthorityMigrations(pool, migrations) {
  assert.notEqual(migrations.length, 0);
  assert.deepEqual(
    migrations.map(({ version }) => version),
    migrations.map((_, index) => index + 1),
  );
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    await client.query("SET LOCAL search_path = pg_catalog");
    await client.query("CREATE SCHEMA session_authority");
    await client.query(
      [
        "CREATE TABLE session_authority.schema_migrations (",
        "version integer PRIMARY KEY CHECK (version > 0),",
        "checksum character(64) NOT NULL CHECK (checksum ~ '^[0-9a-f]{64}$'),",
        "applied_at timestamp with time zone NOT NULL",
        ")",
      ].join(" "),
    );
    for (const migration of migrations) {
      await client.query(migration.sql);
      await client.query(
        [
          "INSERT INTO session_authority.schema_migrations",
          "(version, checksum, applied_at)",
          "VALUES ($1, $2, pg_catalog.transaction_timestamp())",
        ].join(" "),
        [migration.version, migration.checksum],
      );
    }
    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK");
    throw error;
  } finally {
    client.release();
  }
}

async function installVersionOneAuthority(pool, migration) {
  assert.equal(migration.version, 1);
  return installAuthorityMigrations(pool, [migration]);
}

async function insertDirectOperationIdClaim(
  queryable,
  { claimedAt, operationId, sessionId },
) {
  await queryable.query(
    [
      "INSERT INTO session_authority.operation_id_registry",
      "(operation_id, session_id, claim_type, claimant_operation_id,",
      "binding, claimed_at, materialized_at)",
      "VALUES ($1, $2, 'direct-operation', NULL, NULL, $3, $3)",
    ].join(" "),
    [operationId, sessionId, claimedAt],
  );
}

async function waitForMigrationOperationTableLock(queryable) {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    await queryable.query("SELECT pg_catalog.pg_stat_clear_snapshot()");
    const result = await queryable.query(
      [
        "SELECT count(*)::integer AS value",
        "FROM pg_catalog.pg_stat_activity",
        "WHERE datname = pg_catalog.current_database()",
        "AND wait_event_type = 'Lock'",
        "AND position(",
        "'LOCK TABLE session_authority.operation_claims IN ACCESS EXCLUSIVE MODE'",
        "IN query) > 0",
      ].join(" "),
    );
    if (result.rows[0]?.value >= 1) return;
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  assert.fail("operation ID registry migration did not wait for old writers");
}

async function waitForMigrationSessionTableLock(queryable) {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    await queryable.query("SELECT pg_catalog.pg_stat_clear_snapshot()");
    const result = await queryable.query(
      [
        "SELECT count(*)::integer AS value",
        "FROM pg_catalog.pg_locks AS locks",
        "JOIN pg_catalog.pg_class AS relation",
        "ON relation.oid = locks.relation",
        "JOIN pg_catalog.pg_namespace AS namespace",
        "ON namespace.oid = relation.relnamespace",
        "JOIN pg_catalog.pg_stat_activity AS activity",
        "ON activity.pid = locks.pid",
        "WHERE locks.locktype = 'relation'",
        "AND locks.mode = 'ExclusiveLock'",
        "AND locks.granted = false",
        "AND namespace.nspname = 'session_authority'",
        "AND relation.relname = 'sessions'",
        "AND activity.datname = pg_catalog.current_database()",
        "AND position(",
        "'LOCK TABLE session_authority.sessions IN EXCLUSIVE MODE'",
        "IN activity.query) > 0",
      ].join(" "),
    );
    if (result.rows[0]?.value >= 1) return;
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  assert.fail(
    "operation ID registry migration did not wait for session writers",
  );
}

async function assertLegacyRestoreV2MigrationGate(
  pool,
  store,
  trackedMigrations,
) {
  await pool.query("DROP SCHEMA IF EXISTS session_authority CASCADE");
  await installAuthorityMigrations(pool, trackedMigrations.slice(0, 2));

  const blockedOperationId = `legacy-starting-restore-v2-${randomUUID()}`;
  const sessionId = randomUUID();
  const timestamp = await pool.query(
    "SELECT pg_catalog.transaction_timestamp() AS value",
  );
  const now = timestamp.rows[0].value;
  await pool.query(
    [
      "INSERT INTO session_authority.sessions",
      "(session_id, document, created_at, updated_at)",
      "VALUES ($1, $2::jsonb, $3, $3)",
    ].join(" "),
    [sessionId, EMPTY_JSON_OBJECT, now],
  );
  const inserted = await pool.query(
    [
      "INSERT INTO session_authority.operation_claims",
      "(operation_id, session_id, kind, request, state, created_at, updated_at)",
      "VALUES ($1, $2, 'restore-destination-generation-v1',",
      "$3::jsonb, 'prepared', $4, $4)",
      "RETURNING created_at",
    ].join(" "),
    [
      blockedOperationId,
      sessionId,
      JSON.stringify({ payload: { contractVersion: 2 } }),
      now,
    ],
  );
  assert.equal(inserted.rows.length, 1);

  const oldWriter = await pool.connect();
  let oldWriterTransactionOpen = false;
  try {
    await oldWriter.query("BEGIN");
    oldWriterTransactionOpen = true;
    await oldWriter.query(
      [
        "UPDATE session_authority.operation_claims",
        "SET state = 'starting', updated_at = $2",
        "WHERE operation_id = $1",
      ].join(" "),
      [blockedOperationId, now],
    );
    const migrationOutcome = store.migrate().then(
      (value) => ({ error: null, value }),
      (error) => ({ error, value: null }),
    );
    // The integration pool intentionally has only two connections: this old
    // writer and the blocked migrator. Observe the wait through the writer's
    // existing connection so the probe itself cannot deadlock in the pool
    // queue before releasing the table lock.
    await waitForMigrationOperationTableLock(oldWriter);
    await oldWriter.query("COMMIT");
    oldWriterTransactionOpen = false;
    const outcome = await migrationOutcome;
    assert.equal(outcome.value, null);
    assert.ok(outcome.error instanceof PostgresSerializableStoreError);
    assert.equal(outcome.error.code, "migration_failed");
    assert.equal(outcome.error.commitState, "not-committed");
  } finally {
    if (oldWriterTransactionOpen) await oldWriter.query("ROLLBACK");
    oldWriter.release();
  }
  assert.deepEqual(
    await readMigrationLedger(pool),
    trackedMigrations.slice(0, 2).map(({ checksum, version }) => ({
      checksum,
      version,
    })),
  );
  const absentRegistry = await pool.query(
    "SELECT pg_catalog.to_regclass('session_authority.operation_id_registry') AS value",
  );
  assert.equal(absentRegistry.rows[0].value, null);

  await pool.query(
    "DELETE FROM session_authority.operation_claims WHERE operation_id = $1",
    [blockedOperationId],
  );
  const operationId = `legacy-prepared-restore-v2-${randomUUID()}`;
  await pool.query(
    [
      "INSERT INTO session_authority.operation_claims",
      "(operation_id, session_id, kind, request, state, created_at, updated_at)",
      "VALUES ($1, $2, 'restore-destination-generation-v1',",
      "$3::jsonb, 'prepared', $4, $4)",
    ].join(" "),
    [
      operationId,
      sessionId,
      JSON.stringify({ payload: { contractVersion: 2 } }),
      now,
    ],
  );
  const sessionWriter = await pool.connect();
  let sessionWriterTransactionOpen = false;
  let migrationOutcome;
  try {
    // Enter the runtime's session-first lock order before the migration. The
    // writer must still be able to touch its operation before releasing the
    // session row, proving that the migration has not inverted that order.
    await sessionWriter.query("BEGIN");
    sessionWriterTransactionOpen = true;
    const lockedSession = await sessionWriter.query(
      [
        "SELECT session_id",
        "FROM session_authority.sessions",
        "WHERE session_id = $1",
        "FOR UPDATE",
      ].join(" "),
      [sessionId],
    );
    assert.equal(lockedSession.rows.length, 1);
    migrationOutcome = store.migrate().then(
      (value) => ({ error: null, value }),
      (error) => ({ error, value: null }),
    );
    await waitForMigrationSessionTableLock(sessionWriter);
    const touchedOperation = await sessionWriter.query(
      [
        "UPDATE session_authority.operation_claims",
        "SET updated_at = updated_at",
        "WHERE operation_id = $1",
        "RETURNING operation_id, state",
      ].join(" "),
      [operationId],
    );
    assert.deepEqual(touchedOperation.rows, [
      { operation_id: operationId, state: "prepared" },
    ]);
    await sessionWriter.query("COMMIT");
    sessionWriterTransactionOpen = false;
  } finally {
    if (sessionWriterTransactionOpen) {
      await sessionWriter.query("ROLLBACK");
    }
    sessionWriter.release();
  }
  const upgradeOutcome = await migrationOutcome;
  assert.equal(upgradeOutcome.error, null);
  const upgraded = upgradeOutcome.value;
  assert.deepEqual(upgraded, {
    applied: true,
    checksum: trackedMigrations.at(-1).checksum,
    version: 6,
  });
  const registry = await pool.query(
    [
      "SELECT operation_id, session_id, claim_type, claimant_operation_id,",
      "binding, claimed_at, materialized_at",
      "FROM session_authority.operation_id_registry",
      "WHERE operation_id = $1",
    ].join(" "),
    [operationId],
  );
  assert.equal(registry.rows.length, 1);
  assert.equal(registry.rows[0].operation_id, operationId);
  assert.equal(registry.rows[0].session_id, sessionId);
  assert.equal(registry.rows[0].claim_type, "direct-operation");
  assert.equal(registry.rows[0].claimant_operation_id, null);
  assert.equal(registry.rows[0].binding, null);
  assert.equal(
    registry.rows[0].materialized_at.getTime(),
    registry.rows[0].claimed_at.getTime(),
  );
  await assert.rejects(
    pool.query(
      [
        "UPDATE session_authority.operation_claims",
        "SET state = 'starting', updated_at = $2",
        "WHERE operation_id = $1",
      ].join(" "),
      [operationId, now],
    ),
    (error) => {
      assert.equal(error.code, "23514");
      assert.equal(
        error.constraint,
        "operation_claims_restore_v2_launch_id_claim",
      );
      return true;
    },
  );
  const cancelled = await pool.query(
    [
      "UPDATE session_authority.operation_claims",
      "SET state = 'committed',",
      "result = $2::jsonb, revision = revision + 1,",
      "updated_at = $3, retired_at = $3",
      "WHERE operation_id = $1 AND state = 'prepared'",
      "RETURNING state, result, revision, retired_at",
    ].join(" "),
    [
      operationId,
      JSON.stringify({
        outcome: "cancelled-before-dispatch",
        reason: "integration-upgrade-cancellation",
        resultVersion: 1,
      }),
      now,
    ],
  );
  assert.equal(cancelled.rows.length, 1);
  assert.equal(cancelled.rows[0].state, "committed");
  assert.equal(cancelled.rows[0].result.outcome, "cancelled-before-dispatch");
  assert.equal(cancelled.rows[0].revision, "1");
  assert.equal(cancelled.rows[0].retired_at.getTime(), now.getTime());

  await pool.query(
    "DELETE FROM session_authority.operation_claims WHERE operation_id = $1",
    [operationId],
  );
  await pool.query(
    "DELETE FROM session_authority.operation_id_registry WHERE operation_id = $1",
    [operationId],
  );
  await pool.query(
    "DELETE FROM session_authority.sessions WHERE session_id = $1",
    [sessionId],
  );
}

async function waitForBackendLockWait(observer, backendPid) {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    const result = await observer.query(
      [
        "SELECT wait_event_type",
        "FROM pg_catalog.pg_stat_activity",
        "WHERE pid = $1",
      ].join(" "),
      [backendPid],
    );
    if (result.rows[0]?.wait_event_type === "Lock") return;
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  assert.fail("concurrent operation ID claimant did not wait on the registry");
}

async function assertOperationIdRegistryConcurrency(pool) {
  const firstClient = await pool.connect();
  const secondClient = await pool.connect();
  const firstSessionId = randomUUID();
  const secondSessionId = randomUUID();
  const invalidOperationId = `invalid-direct-operation-${randomUUID()}`;
  const operationId = `concurrent-operation-${randomUUID()}`;
  let firstTransactionOpen = false;
  let secondTransactionOpen = false;
  try {
    const now = new Date();
    await firstClient.query(
      [
        "INSERT INTO session_authority.sessions",
        "(session_id, document, created_at, updated_at)",
        "VALUES ($1, $3::jsonb, $4, $4), ($2, $3::jsonb, $4, $4)",
      ].join(" "),
      [firstSessionId, secondSessionId, EMPTY_JSON_OBJECT, now],
    );
    await assert.rejects(
      firstClient.query(
        [
          "INSERT INTO session_authority.operation_id_registry",
          "(operation_id, session_id, claim_type, claimant_operation_id,",
          "binding, claimed_at, materialized_at)",
          "VALUES ($1, $2, 'direct-operation', NULL, NULL, $3, NULL)",
        ].join(" "),
        [invalidOperationId, firstSessionId, now],
      ),
      (error) => {
        assert.equal(error.code, "23514");
        assert.equal(
          error.constraint,
          "operation_id_registry_claim_shape",
        );
        return true;
      },
    );

    await firstClient.query("BEGIN");
    firstTransactionOpen = true;
    await insertDirectOperationIdClaim(firstClient, {
      claimedAt: now,
      operationId,
      sessionId: firstSessionId,
    });
    await firstClient.query(
      [
        "INSERT INTO session_authority.operation_claims",
        "(operation_id, session_id, kind, request, state, created_at, updated_at)",
        "VALUES ($1, $2, 'integration', $3::jsonb, 'active', $4, $4)",
      ].join(" "),
      [operationId, firstSessionId, EMPTY_JSON_OBJECT, now],
    );

    await secondClient.query("BEGIN");
    secondTransactionOpen = true;
    await secondClient.query("SET LOCAL lock_timeout = '5s'");
    const backend = await secondClient.query(
      "SELECT pg_catalog.pg_backend_pid() AS value",
    );
    const competingInsert = secondClient.query(
      [
        "INSERT INTO session_authority.operation_id_registry",
        "(operation_id, session_id, claim_type, claimant_operation_id,",
        "binding, claimed_at, materialized_at)",
        "VALUES ($1, $2, 'direct-operation', NULL, NULL, $3, $3)",
        "ON CONFLICT (operation_id) DO NOTHING",
        "RETURNING operation_id",
      ].join(" "),
      [operationId, secondSessionId, now],
    );
    await waitForBackendLockWait(firstClient, backend.rows[0].value);
    await firstClient.query("COMMIT");
    firstTransactionOpen = false;
    assert.deepEqual((await competingInsert).rows, []);
    await secondClient.query("COMMIT");
    secondTransactionOpen = false;

    const existingConflict = await firstClient.query(
      [
        "INSERT INTO session_authority.operation_id_registry",
        "(operation_id, session_id, claim_type, claimant_operation_id,",
        "binding, claimed_at, materialized_at)",
        "VALUES ($1, $2, 'direct-operation', NULL, NULL, $3, $3)",
        "ON CONFLICT (operation_id) DO NOTHING",
        "RETURNING operation_id",
      ].join(" "),
      [operationId, secondSessionId, now],
    );
    assert.deepEqual(existingConflict.rows, []);
  } finally {
    if (secondTransactionOpen) await secondClient.query("ROLLBACK");
    if (firstTransactionOpen) await firstClient.query("ROLLBACK");
    secondClient.release();
    firstClient.release();
    await pool.query(
      "DELETE FROM session_authority.operation_claims WHERE operation_id = $1",
      [operationId],
    );
    await pool.query(
      [
        "DELETE FROM session_authority.operation_id_registry",
        "WHERE operation_id IN ($1, $2)",
      ].join(" "),
      [operationId, invalidOperationId],
    );
    await pool.query(
      "DELETE FROM session_authority.sessions WHERE session_id IN ($1, $2)",
      [firstSessionId, secondSessionId],
    );
  }
}

async function assertWriterStopCaptureHandoffSchema(pool) {
  const constraints = await pool.query(
    [
      "SELECT conname, pg_catalog.pg_get_constraintdef(oid) AS definition",
      "FROM pg_catalog.pg_constraint",
      "WHERE conrelid =",
      "'session_authority.operation_id_registry'::pg_catalog.regclass",
      "AND conname IN (",
      "'operation_id_registry_claim_type_allowed',",
      "'operation_id_registry_claim_shape')",
      "ORDER BY conname",
    ].join(" "),
  );
  assert.deepEqual(
    constraints.rows.map(({ conname }) => conname),
    [
      "operation_id_registry_claim_shape",
      "operation_id_registry_claim_type_allowed",
    ],
  );
  for (const constraint of constraints.rows) {
    assert.match(
      constraint.definition,
      /writer-stop-capture-intent-v3/u,
    );
  }

  const triggers = await pool.query(
    [
      "SELECT tgname",
      "FROM pg_catalog.pg_trigger",
      "WHERE tgrelid =",
      "'session_authority.operation_claims'::pg_catalog.regclass",
      "AND NOT tgisinternal",
      "AND tgname IN (",
      "'operation_claims_enforce_writer_stop_capture_id_claim',",
      "'operation_claims_enforce_writer_stop_capture_materialization')",
      "ORDER BY tgname",
    ].join(" "),
  );
  assert.deepEqual(triggers.rows, [
    {
      tgname:
        "operation_claims_enforce_writer_stop_capture_id_claim",
    },
    {
      tgname:
        "operation_claims_enforce_writer_stop_capture_materialization",
    },
  ]);
}

async function assertRestoreGenerationConstraints(pool) {
  const client = await pool.connect();
  let transactionOpen = false;
  try {
    const fixtures = [0, 1].map(() => ({
      captureAttemptId: randomUUID(),
      captureOperationId: `integration-capture-operation-${randomUUID()}`,
      checkpointId: `integration-checkpoint-${randomUUID()}`,
      restoreOperationId: `integration-restore-operation-${randomUUID()}`,
      sessionId: randomUUID(),
    }));
    await client.query("BEGIN");
    transactionOpen = true;
    const timestamp = await client.query(
      "SELECT pg_catalog.transaction_timestamp() AS value",
    );
    const now = timestamp.rows[0].value;
    for (const fixture of fixtures) {
      await client.query(
        [
          "INSERT INTO session_authority.sessions",
          "(session_id, document, created_at, updated_at)",
          "VALUES ($1, $2::jsonb, $3, $3)",
        ].join(" "),
        [fixture.sessionId, EMPTY_JSON_OBJECT, now],
      );
      await insertDirectOperationIdClaim(client, {
        claimedAt: now,
        operationId: fixture.captureOperationId,
        sessionId: fixture.sessionId,
      });
      await client.query(
        [
          "INSERT INTO session_authority.operation_claims",
          [
            "(operation_id, session_id, kind, request, result, state,",
            "created_at, updated_at, retired_at)",
          ].join(" "),
          [
            "VALUES ($1, $2, 'integration-capture', $3::jsonb, $3::jsonb,",
            "'committed', $4, $4, $4)",
          ].join(" "),
        ].join(" "),
        [
          fixture.captureOperationId,
          fixture.sessionId,
          EMPTY_JSON_OBJECT,
          now,
        ],
      );
      await client.query(
        [
          "INSERT INTO session_authority.capture_attempt_claims",
          "(capture_attempt_id, operation_id, session_id, binding, claimed_at)",
          "VALUES ($1, $2, $3, $4::jsonb, $5)",
        ].join(" "),
        [
          fixture.captureAttemptId,
          fixture.captureOperationId,
          fixture.sessionId,
          EMPTY_JSON_OBJECT,
          now,
        ],
      );
      await insertDirectOperationIdClaim(client, {
        claimedAt: now,
        operationId: fixture.restoreOperationId,
        sessionId: fixture.sessionId,
      });
      await client.query(
        [
          "INSERT INTO session_authority.checkpoint_catalogue",
          "(checkpoint_id, session_id, capture_attempt_id, document, committed_at)",
          "VALUES ($1, $2, $3, $4::jsonb, $5)",
        ].join(" "),
        [
          fixture.checkpointId,
          fixture.sessionId,
          fixture.captureAttemptId,
          EMPTY_JSON_OBJECT,
          now,
        ],
      );
      await client.query(
        [
          "INSERT INTO session_authority.operation_claims",
          "(operation_id, session_id, kind, request, state, created_at, updated_at)",
          "VALUES ($1, $2, 'integration-restore', $3::jsonb, 'active', $4, $4)",
        ].join(" "),
        [
          fixture.restoreOperationId,
          fixture.sessionId,
          EMPTY_JSON_OBJECT,
          now,
        ],
      );
    }
    const [first, second] = fixtures;
    await client.query("SAVEPOINT state_payload_check");
    await assert.rejects(
      client.query(
        [
          "INSERT INTO session_authority.restore_destination_generations",
          [
            "(generation_id, operation_id, session_id, checkpoint_id, state,",
            "binding, document, claimed_at, committed_at)",
          ].join(" "),
          "VALUES ($1, $2, $3, $4, 'authorized', $5::jsonb, $5::jsonb, $6, NULL)",
        ].join(" "),
        [
          `integration-generation-${randomUUID()}`,
          first.restoreOperationId,
          first.sessionId,
          first.checkpointId,
          EMPTY_JSON_OBJECT,
          now,
        ],
      ),
      (error) => {
        assert.equal(error.code, "23514");
        assert.equal(
          error.constraint,
          "restore_destination_generations_state_payload_pair",
        );
        return true;
      },
    );
    await client.query("ROLLBACK TO SAVEPOINT state_payload_check");
    await client.query("RELEASE SAVEPOINT state_payload_check");

    await client.query("SAVEPOINT operation_session_check");
    await assert.rejects(
      client.query(
        [
          "INSERT INTO session_authority.restore_destination_generations",
          [
            "(generation_id, operation_id, session_id, checkpoint_id, state,",
            "binding, document, claimed_at, committed_at)",
          ].join(" "),
          "VALUES ($1, $2, $3, $4, 'authorized', $5::jsonb, NULL, $6, NULL)",
        ].join(" "),
        [
          `integration-generation-${randomUUID()}`,
          second.restoreOperationId,
          first.sessionId,
          first.checkpointId,
          EMPTY_JSON_OBJECT,
          now,
        ],
      ),
      (error) => {
        assert.equal(error.code, "23503");
        assert.equal(
          error.constraint,
          "restore_destination_generations_operation_session_fk",
        );
        return true;
      },
    );
    await client.query("ROLLBACK TO SAVEPOINT operation_session_check");
    await client.query("RELEASE SAVEPOINT operation_session_check");

    await client.query("SAVEPOINT checkpoint_session_check");
    await assert.rejects(
      client.query(
        [
          "INSERT INTO session_authority.restore_destination_generations",
          [
            "(generation_id, operation_id, session_id, checkpoint_id, state,",
            "binding, document, claimed_at, committed_at)",
          ].join(" "),
          "VALUES ($1, $2, $3, $4, 'authorized', $5::jsonb, NULL, $6, NULL)",
        ].join(" "),
        [
          `integration-generation-${randomUUID()}`,
          first.restoreOperationId,
          first.sessionId,
          second.checkpointId,
          EMPTY_JSON_OBJECT,
          now,
        ],
      ),
      (error) => {
        assert.equal(error.code, "23503");
        assert.equal(
          error.constraint,
          "restore_destination_generations_checkpoint_session_fk",
        );
        return true;
      },
    );
    await client.query("ROLLBACK TO SAVEPOINT checkpoint_session_check");
    await client.query("RELEASE SAVEPOINT checkpoint_session_check");

    const generationId = `integration-generation-${randomUUID()}`;
    await client.query(
      [
        "INSERT INTO session_authority.restore_destination_generations",
        [
          "(generation_id, operation_id, session_id, checkpoint_id, state,",
          "binding, document, claimed_at, committed_at)",
        ].join(" "),
        "VALUES ($1, $2, $3, $4, 'authorized', $5::jsonb, NULL, $6, NULL)",
      ].join(" "),
      [
        generationId,
        first.restoreOperationId,
        first.sessionId,
        first.checkpointId,
        EMPTY_JSON_OBJECT,
        now,
      ],
    );
    const authorized = await client.query(
      [
        "SELECT generation_id, operation_id, session_id, checkpoint_id,",
        "state, binding, document, claimed_at, committed_at",
        "FROM session_authority.restore_destination_generations",
        "WHERE generation_id = $1",
      ].join(" "),
      [generationId],
    );
    assert.equal(authorized.rowCount, 1);
    assert.deepEqual(authorized.rows[0], {
      generation_id: generationId,
      operation_id: first.restoreOperationId,
      session_id: first.sessionId,
      checkpoint_id: first.checkpointId,
      state: "authorized",
      binding: {},
      document: null,
      claimed_at: now,
      committed_at: null,
    });

    await client.query(
      [
        "UPDATE session_authority.restore_destination_generations",
        "SET state = 'committed', document = $2::jsonb, committed_at = $3",
        "WHERE generation_id = $1",
      ].join(" "),
      [generationId, EMPTY_JSON_OBJECT, now],
    );
    const committed = await client.query(
      [
        "SELECT generation_id, operation_id, session_id, checkpoint_id,",
        "state, binding, document, claimed_at, committed_at",
        "FROM session_authority.restore_destination_generations",
        "WHERE generation_id = $1",
      ].join(" "),
      [generationId],
    );
    assert.equal(committed.rowCount, 1);
    assert.deepEqual(committed.rows[0], {
      generation_id: generationId,
      operation_id: first.restoreOperationId,
      session_id: first.sessionId,
      checkpoint_id: first.checkpointId,
      state: "committed",
      binding: {},
      document: {},
      claimed_at: now,
      committed_at: now,
    });

    await client.query("SAVEPOINT committed_at_order_check");
    await assert.rejects(
      client.query(
        [
          "INSERT INTO session_authority.restore_destination_generations",
          [
            "(generation_id, operation_id, session_id, checkpoint_id, state,",
            "binding, document, claimed_at, committed_at)",
          ].join(" "),
          [
            "VALUES ($1, $2, $3, $4, 'committed', $5::jsonb,",
            "$5::jsonb, $6, $7)",
          ].join(" "),
        ].join(" "),
        [
          `integration-generation-${randomUUID()}`,
          second.restoreOperationId,
          second.sessionId,
          second.checkpointId,
          EMPTY_JSON_OBJECT,
          now,
          new Date(now.getTime() - 1),
        ],
      ),
      (error) => {
        assert.equal(error.code, "23514");
        assert.equal(
          error.constraint,
          "restore_destination_generations_committed_at_order",
        );
        return true;
      },
    );
    await client.query("ROLLBACK TO SAVEPOINT committed_at_order_check");
    await client.query("RELEASE SAVEPOINT committed_at_order_check");

    await client.query("SAVEPOINT state_allowed_check");
    await assert.rejects(
      client.query(
        [
          "INSERT INTO session_authority.restore_destination_generations",
          [
            "(generation_id, operation_id, session_id, checkpoint_id, state,",
            "binding, document, claimed_at, committed_at)",
          ].join(" "),
          "VALUES ($1, $2, $3, $4, 'invalid', $5::jsonb, NULL, $6, NULL)",
        ].join(" "),
        [
          `integration-generation-${randomUUID()}`,
          second.restoreOperationId,
          second.sessionId,
          second.checkpointId,
          EMPTY_JSON_OBJECT,
          now,
        ],
      ),
      (error) => {
        assert.equal(error.code, "23514");
        assert.equal(
          error.constraint,
          "restore_destination_generations_state_allowed",
        );
        return true;
      },
    );
    await client.query("ROLLBACK TO SAVEPOINT state_allowed_check");
    await client.query("RELEASE SAVEPOINT state_allowed_check");

    await client.query("SAVEPOINT identity_length_check");
    await assert.rejects(
      client.query(
        [
          "INSERT INTO session_authority.restore_destination_generations",
          [
            "(generation_id, operation_id, session_id, checkpoint_id, state,",
            "binding, document, claimed_at, committed_at)",
          ].join(" "),
          "VALUES ($1, $2, $3, $4, 'authorized', $5::jsonb, NULL, $6, NULL)",
        ].join(" "),
        [
          "g".repeat(129),
          second.restoreOperationId,
          second.sessionId,
          second.checkpointId,
          EMPTY_JSON_OBJECT,
          now,
        ],
      ),
      (error) => {
        assert.equal(error.code, "22001");
        return true;
      },
    );
    await client.query("ROLLBACK TO SAVEPOINT identity_length_check");
    await client.query("RELEASE SAVEPOINT identity_length_check");

    await client.query("SAVEPOINT operation_uniqueness_check");
    await assert.rejects(
      client.query(
        [
          "INSERT INTO session_authority.restore_destination_generations",
          [
            "(generation_id, operation_id, session_id, checkpoint_id, state,",
            "binding, document, claimed_at, committed_at)",
          ].join(" "),
          "VALUES ($1, $2, $3, $4, 'authorized', $5::jsonb, NULL, $6, NULL)",
        ].join(" "),
        [
          `integration-generation-${randomUUID()}`,
          first.restoreOperationId,
          first.sessionId,
          first.checkpointId,
          EMPTY_JSON_OBJECT,
          now,
        ],
      ),
      (error) => {
        assert.equal(error.code, "23505");
        assert.equal(
          error.constraint,
          "restore_destination_generations_operation_id_key",
        );
        return true;
      },
    );
    await client.query("ROLLBACK TO SAVEPOINT operation_uniqueness_check");
    await client.query("RELEASE SAVEPOINT operation_uniqueness_check");
    await client.query("ROLLBACK");
    transactionOpen = false;
  } finally {
    if (transactionOpen) await client.query("ROLLBACK");
    client.release();
  }
}

async function assertRestoreRecoveryCursorSchemaAndStore(pool, store) {
  const columns = await pool.query(
    [
      "SELECT column_name, data_type, is_nullable, character_maximum_length",
      "FROM information_schema.columns",
      "WHERE table_schema = 'session_authority'",
      "AND table_name = 'restore_recovery_cursors'",
      "ORDER BY ordinal_position",
    ].join(" "),
  );
  assert.deepEqual(columns.rows, [
    {
      character_maximum_length: 128,
      column_name: "recovery_scope_id",
      data_type: "character varying",
      is_nullable: "NO",
    },
    {
      character_maximum_length: 32,
      column_name: "lane",
      data_type: "character varying",
      is_nullable: "NO",
    },
    {
      character_maximum_length: null,
      column_name: "after_session_id",
      data_type: "uuid",
      is_nullable: "YES",
    },
    {
      character_maximum_length: null,
      column_name: "cycle",
      data_type: "bigint",
      is_nullable: "NO",
    },
    {
      character_maximum_length: null,
      column_name: "revision",
      data_type: "bigint",
      is_nullable: "NO",
    },
    {
      character_maximum_length: null,
      column_name: "last_transition_id",
      data_type: "uuid",
      is_nullable: "YES",
    },
    {
      character_maximum_length: 64,
      column_name: "last_request_sha256",
      data_type: "character",
      is_nullable: "YES",
    },
    {
      character_maximum_length: null,
      column_name: "updated_at",
      data_type: "timestamp with time zone",
      is_nullable: "NO",
    },
  ]);

  const constraints = await pool.query(
    [
      "SELECT constraint_name",
      "FROM (",
      "SELECT constraint_record.conname AS constraint_name",
      "FROM pg_catalog.pg_constraint AS constraint_record",
      "WHERE constraint_record.conrelid =",
      "'session_authority.restore_recovery_cursors'::pg_catalog.regclass",
      "AND constraint_record.contype IN ('p', 'c')",
      ") AS named_constraints",
      "ORDER BY constraint_name",
    ].join(" "),
  );
  assert.deepEqual(
    constraints.rows.map(({ constraint_name: name }) => name),
    [
      "restore_recovery_cursors_cycle_nonnegative",
      "restore_recovery_cursors_cycle_within_revision",
      "restore_recovery_cursors_initial_shape",
      "restore_recovery_cursors_lane_allowed",
      "restore_recovery_cursors_pkey",
      "restore_recovery_cursors_progressed_shape",
      "restore_recovery_cursors_request_sha256_format",
      "restore_recovery_cursors_revision_nonnegative",
      "restore_recovery_cursors_scope_id_length",
      "restore_recovery_cursors_transition_digest_pair",
    ],
  );

  const constraintClient = await pool.connect();
  let constraintTransactionOpen = false;
  try {
    await constraintClient.query("BEGIN");
    constraintTransactionOpen = true;
    const timestamp = await constraintClient.query(
      "SELECT pg_catalog.transaction_timestamp() AS value",
    );
    const now = timestamp.rows[0].value;
    const transitionId = randomUUID();
    const requestSha256 = "a".repeat(64);
    const scenarios = [
      {
        constraint: "restore_recovery_cursors_scope_id_length",
        values: ["", "generation", null, 0, 0, null, null, now],
      },
      {
        constraint: "restore_recovery_cursors_lane_allowed",
        values: [
          `constraint-${randomUUID()}`,
          "invalid",
          null,
          0,
          0,
          null,
          null,
          now,
        ],
      },
      {
        constraint: "restore_recovery_cursors_cycle_nonnegative",
        values: [
          `constraint-${randomUUID()}`,
          "generation",
          null,
          -1,
          1,
          transitionId,
          requestSha256,
          now,
        ],
      },
      {
        constraint: "restore_recovery_cursors_cycle_within_revision",
        values: [
          `constraint-${randomUUID()}`,
          "generation",
          null,
          2,
          1,
          transitionId,
          requestSha256,
          now,
        ],
      },
      {
        constraint: "restore_recovery_cursors_request_sha256_format",
        values: [
          `constraint-${randomUUID()}`,
          "generation",
          null,
          0,
          1,
          transitionId,
          "invalid",
          now,
        ],
      },
      {
        constraint: "restore_recovery_cursors_initial_shape",
        values: [
          `constraint-${randomUUID()}`,
          "generation",
          randomUUID(),
          0,
          0,
          null,
          null,
          now,
        ],
      },
      {
        constraint: "restore_recovery_cursors_progressed_shape",
        values: [
          `constraint-${randomUUID()}`,
          "generation",
          null,
          0,
          1,
          null,
          null,
          now,
        ],
      },
    ];
    for (const scenario of scenarios) {
      await constraintClient.query("SAVEPOINT cursor_constraint_check");
      await assert.rejects(
        constraintClient.query(
          [
            "INSERT INTO session_authority.restore_recovery_cursors",
            "(recovery_scope_id, lane, after_session_id, cycle, revision,",
            "last_transition_id, last_request_sha256, updated_at)",
            "VALUES ($1, $2, $3, $4, $5, $6, $7, $8)",
          ].join(" "),
          scenario.values,
        ),
        (error) => {
          assert.equal(error.code, "23514");
          assert.equal(error.constraint, scenario.constraint);
          return true;
        },
      );
      await constraintClient.query(
        "ROLLBACK TO SAVEPOINT cursor_constraint_check",
      );
      await constraintClient.query(
        "RELEASE SAVEPOINT cursor_constraint_check",
      );
    }
    await constraintClient.query("ROLLBACK");
    constraintTransactionOpen = false;
  } finally {
    if (constraintTransactionOpen) {
      await constraintClient.query("ROLLBACK");
    }
    constraintClient.release();
  }

  const lanes = [
    "generation",
    "activation",
    "launch-attempt",
    "current-launch",
  ];
  const lazyScopeId = `lazy-${randomUUID()}`;
  const concurrentScopeId = `concurrent-${randomUUID()}`;
  const acknowledgementLossScopeId = `ack-loss-${randomUUID()}`;
  const scopeIds = [
    lazyScopeId,
    concurrentScopeId,
    acknowledgementLossScopeId,
  ];
  const cursorStore = createPostgresRestoreRecoveryCursorStore({ store });
  try {
    const initialized = [];
    for (const lane of lanes) {
      const cursor = await cursorStore.readLane({
        lane,
        recoveryScopeId: lazyScopeId,
      });
      assert.equal(Object.isFrozen(cursor), true);
      assert.deepEqual(
        {
          afterSessionId: cursor.afterSessionId,
          cycle: cursor.cycle,
          lane: cursor.lane,
          lastRequestSha256: cursor.lastRequestSha256,
          lastTransitionId: cursor.lastTransitionId,
          recoveryScopeId: cursor.recoveryScopeId,
          revision: cursor.revision,
        },
        {
          afterSessionId: null,
          cycle: "0",
          lane,
          lastRequestSha256: null,
          lastTransitionId: null,
          recoveryScopeId: lazyScopeId,
          revision: "0",
        },
      );
      assert.match(
        cursor.updatedAt,
        /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$/u,
      );
      initialized.push(cursor);
    }
    assert.deepEqual(
      await cursorStore.readLane({
        lane: "generation",
        recoveryScopeId: lazyScopeId,
      }),
      initialized[0],
    );
    const initializedRows = await pool.query(
      [
        "SELECT lane, revision::text AS revision",
        "FROM session_authority.restore_recovery_cursors",
        "WHERE recovery_scope_id = $1",
        "ORDER BY lane",
      ].join(" "),
      [lazyScopeId],
    );
    assert.deepEqual(initializedRows.rows, [
      { lane: "activation", revision: "0" },
      { lane: "current-launch", revision: "0" },
      { lane: "generation", revision: "0" },
      { lane: "launch-attempt", revision: "0" },
    ]);

    const initial = await cursorStore.readLane({
      lane: "generation",
      recoveryScopeId: concurrentScopeId,
    });
    const attempts = [0, 1].map((index) => ({
      expectedAfterSessionId: initial.afterSessionId,
      expectedCycle: initial.cycle,
      expectedRevision: initial.revision,
      lane: initial.lane,
      nextAfterSessionId: randomUUID(),
      recoveryScopeId: initial.recoveryScopeId,
      requestSha256: String(index + 1).repeat(64),
      transitionId: randomUUID(),
    }));
    const outcomes = await Promise.allSettled(
      attempts.map((input) => cursorStore.advanceLane(input)),
    );
    const successful = outcomes.filter(
      ({ status }) => status === "fulfilled",
    );
    const rejected = outcomes.filter(({ status }) => status === "rejected");
    assert.equal(successful.length, 1);
    assert.equal(rejected.length, 1);
    assert.equal(successful[0].value.advanced, true);
    assert.ok(
      rejected[0].reason instanceof PostgresRestoreRecoveryCursorStoreError,
    );
    assert.equal(
      rejected[0].reason.code,
      "postgres_restore_recovery_cursor_conflict",
    );
    assert.deepEqual(
      await cursorStore.readLane({
        lane: "generation",
        recoveryScopeId: concurrentScopeId,
      }),
      successful[0].value.cursor,
    );

    const acknowledgementLossStore =
      createPostgresRestoreRecoveryCursorStore({
        store: new PostgresSerializableStore({
          dedicatedPool: commitAcknowledgementLossAfterQueryPool(
            pool,
            "restore recovery cursor",
            (text) =>
              text.startsWith(
                "UPDATE session_authority.restore_recovery_cursors",
              ),
          ),
          maxTransactionAttempts: 2,
        }),
      });
    const acknowledgementLossInput = {
      expectedAfterSessionId: null,
      expectedCycle: "0",
      expectedRevision: "0",
      lane: "current-launch",
      nextAfterSessionId: randomUUID(),
      recoveryScopeId: acknowledgementLossScopeId,
      requestSha256: "f".repeat(64),
      transitionId: randomUUID(),
    };
    const recovered = await acknowledgementLossStore.advanceLane(
      acknowledgementLossInput,
    );
    assert.equal(recovered.advanced, false);
    assert.deepEqual(
      {
        afterSessionId: recovered.cursor.afterSessionId,
        cycle: recovered.cursor.cycle,
        lastRequestSha256: recovered.cursor.lastRequestSha256,
        lastTransitionId: recovered.cursor.lastTransitionId,
        revision: recovered.cursor.revision,
      },
      {
        afterSessionId: acknowledgementLossInput.nextAfterSessionId,
        cycle: "0",
        lastRequestSha256: acknowledgementLossInput.requestSha256,
        lastTransitionId: acknowledgementLossInput.transitionId,
        revision: "1",
      },
    );
    assert.deepEqual(
      await acknowledgementLossStore.advanceLane(
        acknowledgementLossInput,
      ),
      recovered,
    );
    const durableReplay = await pool.query(
      [
        "SELECT revision::text AS revision, cycle::text AS cycle,",
        "after_session_id::text AS after_session_id,",
        "last_transition_id::text AS last_transition_id,",
        "last_request_sha256",
        "FROM session_authority.restore_recovery_cursors",
        "WHERE recovery_scope_id = $1 AND lane = $2",
      ].join(" "),
      [acknowledgementLossScopeId, acknowledgementLossInput.lane],
    );
    assert.deepEqual(durableReplay.rows, [
      {
        after_session_id: acknowledgementLossInput.nextAfterSessionId,
        cycle: "0",
        last_request_sha256: acknowledgementLossInput.requestSha256,
        last_transition_id: acknowledgementLossInput.transitionId,
        revision: "1",
      },
    ]);
  } finally {
    await pool.query(
      [
        "DELETE FROM session_authority.restore_recovery_cursors",
        "WHERE recovery_scope_id = ANY($1::text[])",
      ].join(" "),
      [scopeIds],
    );
  }
}

function registrationInput(
  sessionId,
  {
    imageDigest = IMAGE_DIGEST,
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
        imageDigest,
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

function integrationPlatformImageFixture() {
  const configBytes = Buffer.from(
    JSON.stringify({
      architecture: "arm64",
      config: { Env: ["PATH=/usr/local/bin:/usr/bin:/bin"] },
      os: "linux",
      rootfs: {
        diff_ids: [`sha256:${"d".repeat(64)}`],
        type: "layers",
      },
    }),
    "utf8",
  );
  const configDigest = `sha256:${createHash("sha256")
    .update(configBytes)
    .digest("hex")}`;
  const descriptorBytes = Buffer.from(
    JSON.stringify({
      config: {
        digest: configDigest,
        mediaType: "application/vnd.oci.image.config.v1+json",
        size: configBytes.byteLength,
      },
      layers: [
        {
          digest: `sha256:${"c".repeat(64)}`,
          mediaType: "application/vnd.oci.image.layer.v1.tar+gzip",
          size: 1024,
        },
      ],
      mediaType: "application/vnd.oci.image.manifest.v1+json",
      schemaVersion: 2,
    }),
    "utf8",
  );
  return {
    configBytes,
    descriptor: {
      bytes: descriptorBytes,
      digest: `sha256:${createHash("sha256")
        .update(descriptorBytes)
        .digest("hex")}`,
      mediaType: "application/vnd.oci.image.manifest.v1+json",
      size: descriptorBytes.byteLength,
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

function firstMatchingQueryNotificationPool(pool, label, matches) {
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
              matches(text)
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

function firstSessionLockQueryNotificationPool(pool, label) {
  return firstMatchingQueryNotificationPool(
    pool,
    label,
    (text) =>
      text.includes("FROM session_authority.sessions") &&
      text.includes("FOR UPDATE"),
  );
}

function firstRestoreGenerationLockQueryNotificationPool(
  pool,
  label,
) {
  return firstMatchingQueryNotificationPool(
    pool,
    label,
    (text) =>
      text.includes(
        "FROM session_authority.restore_destination_generations",
      ) &&
      text.includes("WHERE generation_id = $1") &&
      text.includes("FOR UPDATE"),
  );
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

function commitAcknowledgementLossAfterQueryPool(
  pool,
  label,
  matches,
) {
  let armed = false;
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
          if (
            typeof text === "string" &&
            matches(text)
          ) {
            armed = true;
          }
          if (
            text === "COMMIT" &&
            armed &&
            !acknowledgementLost
          ) {
            acknowledgementLost = true;
            throw new Error(
              `synthetic ${label} COMMIT acknowledgement loss`,
            );
          }
          return result;
        },
        release(...args) {
          return Reflect.apply(client.release, client, args);
        },
      };
    },
    didLoseAcknowledgement() {
      return acknowledgementLost;
    },
  });
}

function firstMatchingQueryResultFailurePool(pool, label, matches) {
  let failed = false;

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
          if (
            !failed &&
            typeof text === "string" &&
            matches(text)
          ) {
            failed = true;
            throw new Error(
              `synthetic ${label} failure after query result`,
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

function assertCommitOutcomeUncertain(error) {
  assert.ok(error instanceof PostgresSerializableStoreError);
  assert.equal(
    error.code,
    "transaction_commit_outcome_uncertain",
  );
  assert.equal(error.commitState, "uncertain");
  assert.equal(error.retryable, false);
  assert.equal("cause" in error, false);
  return true;
}

function assertTransactionBoundaryLost(error) {
  assert.ok(error instanceof PostgresSerializableStoreError);
  assert.equal(error.code, "transaction_boundary_lost");
  assert.equal(error.commitState, "uncertain");
  assert.equal(error.retryable, false);
  assert.equal("cause" in error, false);
  return true;
}

function assertCheckpointAuthorityCode(code) {
  return (error) => {
    assert.ok(
      error instanceof PostgresCheckpointMutationAuthorityError,
    );
    assert.equal(
      error.name,
      "PostgresCheckpointMutationAuthorityError",
    );
    assert.equal(error.code, code);
    assert.equal(error.retryable, false);
    assert.equal(Object.hasOwn(error, "cause"), false);
    return true;
  };
}

function assertLauncherCode(code) {
  return (error) => {
    assert.ok(error instanceof PostgresLogicalWriterLauncherError);
    assert.equal(error.name, "PostgresLogicalWriterLauncherError");
    assert.equal(error.code, code);
    assert.equal(error.retryable, false);
    assert.equal(Object.hasOwn(error, "cause"), false);
    return true;
  };
}

function assertWriterDetachCompositionCode(code) {
  return (error) => {
    assert.ok(error instanceof PostgresWriterDetachCompositionError);
    assert.equal(error.name, "PostgresWriterDetachCompositionError");
    assert.equal(error.code, code);
    assert.equal(error.retryable, false);
    assert.equal(Object.hasOwn(error, "cause"), false);
    return true;
  };
}

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, reject, resolve };
}

async function settleWithin(promise, label, timeoutMs = 10_000) {
  let timer;
  const timeout = new Promise((resolve, reject) => {
    timer = setTimeout(
      () => reject(new Error(`${label} timed out`)),
      timeoutMs,
    );
    timer.unref();
  });
  try {
    return await Promise.race([promise, timeout]);
  } finally {
    clearTimeout(timer);
  }
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

async function releaseWriter(authority, attached) {
  const input = writerReleaseInput(attached.session);
  await authority.reserveOperation(input);
  const starting = await authority.claimWriterReleaseDispatch({
    ...structuredClone(input),
    expectedOperationRevision: "0",
  });
  return authority.finalizeWriterRelease({
    ...structuredClone(input),
    expectedOperationRevision: "1",
    mutationResult: detachEvidence(starting.mutationRequest),
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

function writerReleaseInput(
  expectedSession,
  {
    operationId = `operation-${randomUUID()}`,
  } = {},
) {
  return {
    expectedSession,
    operationId,
    kind: WRITER_RELEASE_OPERATION_KIND,
    request: {
      contractVersion: 1,
      target: {
        attachmentId:
          expectedSession.document.attachment.attachmentId,
        kind: "attachment",
      },
    },
  };
}

function detachEvidence(mutationRequest) {
  return {
    ...structuredClone(mutationRequest),
    proofId: `proof-${randomUUID()}`,
    status: "detached",
  };
}

function writerForceFenceInput(
  expectedSession,
  {
    operationId = `operation-${randomUUID()}`,
  } = {},
) {
  return {
    expectedSession,
    operationId,
    kind: WRITER_FORCE_FENCE_OPERATION_KIND,
    request: {
      contractVersion: 1,
      target: {
        attachmentId:
          expectedSession.document.attachment.attachmentId,
        kind: "attachment",
      },
    },
  };
}

function forceFenceEvidence(fenceRequest) {
  return {
    ...structuredClone(fenceRequest),
    proofId: `proof-${randomUUID()}`,
    status: "fenced",
  };
}

function writerDetachCompositionRequest(
  expectedSession,
  {
    operationId = `operation-${randomUUID()}`,
    target = {
      attachmentId:
        expectedSession.document.attachment?.attachmentId ??
        expectedSession.document.lastOperation?.result?.fenceTarget
          ?.attachmentId,
      kind: "attachment",
    },
  } = {},
) {
  return { expectedSession, operationId, target };
}

function writerDetachIntegrationBackend({
  backendId = "postgres-authority-integration",
  capabilities = {
    atomicPointInTimeCheckpoint: true,
    exclusiveWriterAttachment: true,
    fencing: "epoch-enforced",
    normalDirectoryAttachment: true,
  },
  detachAttachment,
  forceFence,
}) {
  const unsupported = async () => {
    throw new Error("unexpected storage backend operation");
  };
  return Object.freeze({
    backendId,
    capabilities: Object.freeze({ ...capabilities }),
    captureCheckpoint: unsupported,
    contractVersion: 1,
    destroySession: unsupported,
    detachAttachment,
    forceFence,
    prepareWritableAttachment: unsupported,
    provisionSession: unsupported,
    restoreCheckpoint: unsupported,
  });
}

function checkpointCaptureAdmission(
  attached,
  {
    artifactId = `artifact-${randomUUID()}`,
    captureAttemptId = randomUUID(),
    checkpointId = `checkpoint-${randomUUID()}`,
    operationId = `checkpoint-operation-${randomUUID()}`,
    processIncarnationId = `process-${randomUUID()}`,
    stopOperationId = `stop-${randomUUID()}`,
    writerIncarnationId = `writer-${randomUUID()}`,
  } = {},
) {
  const session = attached.session;
  const { attachment, lease, manifest, storageRef } = session.document;
  const request = {
    backendId: storageRef.backendId,
    contractVersion: 1,
    fencingEpoch: lease.fencingEpoch,
    holderId: lease.holderId,
    leaseId: lease.leaseId,
    operation: "checkpoint",
    operationId,
    sessionId: session.sessionId,
    storageId: storageRef.storageId,
    target: {
      artifactId,
      checkpointId,
      kind: "checkpoint",
    },
  };
  const checkpoint = {
    artifactId,
    backendId: storageRef.backendId,
    checkpointClass: "clean",
    checkpointId,
    codexSessionId: manifest.codex.sessionId,
    codexThreadId: manifest.codex.rootThreadId,
    contractVersion: 1,
    createdAt: new Date().toISOString(),
    imageDigest: manifest.runtime.imageDigest,
    sessionId: session.sessionId,
    sourceFencingEpoch: lease.fencingEpoch,
    storageId: storageRef.storageId,
  };
  return {
    attachment,
    captureAttemptId,
    checkpoint,
    processIncarnationId,
    request,
    stopOperationId,
    writerIncarnationId,
  };
}

function checkpointOperationInput(expectedSession, admission) {
  return {
    expectedSession,
    kind: CHECKPOINT_CAPTURE_OPERATION_KIND,
    operationId: admission.request.operationId,
    request: createCheckpointCaptureOperationRequest({
      admission,
      expectedSession,
    }),
  };
}

function restoreGenerationAdmission(
  attached,
  checkpoint,
  {
    operationId = `restore-operation-${randomUUID()}`,
  } = {},
) {
  const { lease, storageRef } = attached.session.document;
  return {
    checkpoint,
    request: {
      backendId: storageRef.backendId,
      contractVersion: 1,
      fencingEpoch: lease.fencingEpoch,
      holderId: lease.holderId,
      leaseId: lease.leaseId,
      operation: "restore",
      operationId,
      sessionId: attached.session.sessionId,
      storageId: storageRef.storageId,
      target: {
        artifactId: checkpoint.artifactId,
        checkpointId: checkpoint.checkpointId,
        kind: "checkpoint",
      },
    },
  };
}

function restoreGenerationOperationInput(expectedSession, admission) {
  return {
    expectedSession,
    kind: RESTORE_DESTINATION_GENERATION_OPERATION_KIND,
    operationId: admission.request.operationId,
    request: createRestoreDestinationGenerationOperationRequest({
      admission,
      expectedSession,
    }),
  };
}

function restoreGenerationOperationInputV2(
  expectedSession,
  admission,
  launchIntent,
) {
  return {
    expectedSession,
    kind: RESTORE_DESTINATION_GENERATION_OPERATION_KIND,
    operationId: admission.request.operationId,
    request: createRestoreDestinationGenerationOperationRequestV2({
      admission,
      expectedSession,
      launchIntent,
    }),
  };
}

function restoreGenerationLaunchIntent(
  expectedSession,
  {
    launchAttemptId = `writer-launch-${randomUUID()}`,
    supervisorId = `supervisor-${randomUUID()}`,
  } = {},
) {
  return {
    launchAttemptId,
    measuredImage: writerLaunchMeasuredImage(expectedSession),
    supervisor: {
      contractVersion: 1,
      supervisorId,
    },
  };
}

function restoreGenerationCompletion(input, claimed, replayed) {
  const artifactProof = claimed.catalogue.document.artifactProof;
  return {
    materialization: {
      artifactManifestDigest: artifactProof.artifactManifestDigest,
      contractVersion: 3,
      coordinatorBindingSha256: operationJournalBindingSha256(
        claimed.generation.binding,
      ),
      modeledDigest: artifactProof.modeledDigest,
      publicationId: `restore-publication-${input.operationId}`,
      publicationKind: "restore-destination",
      stagedRoot: {
        filesystemId: "integration-restore-filesystem",
        objectIdentityScheme: "integration-restore-object-v1",
        objectId: `restore-object-${input.operationId}`,
      },
      treeIdentityDigest: "e".repeat(64),
    },
    replayed,
    result: input.request.predeterminedResult,
  };
}

function writerLaunchMeasuredImage(expectedSession) {
  const runtime = expectedSession.document.manifest.runtime;
  const [os, architecture] = runtime.platform.split("/");
  return {
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
  };
}

function writerLaunchAttemptInput(
  expectedSession,
  generation,
  {
    operationId = `writer-launch-${randomUUID()}`,
    supervisorId = `supervisor-${randomUUID()}`,
  } = {},
) {
  const measuredImage = writerLaunchMeasuredImage(expectedSession);
  const supervisor = {
    contractVersion: 1,
    supervisorId,
  };
  return {
    expectedSession,
    kind: WRITER_LAUNCH_ATTEMPT_OPERATION_KIND,
    operationId,
    request: createWriterLaunchAttemptOperationRequest({
      expectedSession,
      generation,
      measuredImage,
      supervisor,
    }),
  };
}

function writerLaunchEvidence(input, status) {
  const stoppedBeforeStart = status === "not-started";
  return {
    contractVersion: 1,
    launchAttemptId: input.operationId,
    processIncarnationId: stoppedBeforeStart
      ? null
      : `process-${randomUUID()}`,
    proofId: `supervisor-proof-${randomUUID()}`,
    status,
    supervisorId: input.request.supervisor.supervisorId,
    writerIncarnationId: stoppedBeforeStart
      ? null
      : `writer-${randomUUID()}`,
  };
}

function writerLaunchStopInput(
  expectedSession,
  { operationId = `writer-launch-stop-${randomUUID()}` } = {},
) {
  const claimToken = randomUUID();
  return {
    claimToken,
    input: {
      expectedSession,
      kind: WRITER_LAUNCH_STOP_OPERATION_KIND,
      operationId,
      request: createWriterLaunchStopOperationRequest({
        claimToken,
        expectedSession,
      }),
    },
  };
}

function writerLaunchStopCaptureInput(
  expectedSession,
  {
    captureOperationId = `checkpoint-operation-${randomUUID()}`,
    stopOperationId = `writer-launch-stop-${randomUUID()}`,
  } = {},
) {
  const launch = expectedSession.document.launch;
  assert.notEqual(launch, null);
  const captureAdmission = checkpointCaptureAdmission(
    { session: expectedSession },
    {
      operationId: captureOperationId,
      processIncarnationId: launch.processIncarnationId,
      stopOperationId,
      writerIncarnationId: launch.writerIncarnationId,
    },
  );
  const captureInput = checkpointOperationInput(
    expectedSession,
    captureAdmission,
  );
  const claimToken = randomUUID();
  return {
    captureAdmission,
    captureInput,
    claimToken,
    input: {
      expectedSession,
      kind: WRITER_LAUNCH_STOP_OPERATION_KIND,
      operationId: stopOperationId,
      request: createWriterLaunchStopOperationRequest({
        captureIntent: captureInput.request,
        claimToken,
        expectedSession,
      }),
    },
  };
}

function writerLaunchStopEvidence(input) {
  const launch = input.request.launch;
  return {
    contractVersion: 1,
    launchAttemptId: launch.launchAttemptId,
    processIncarnationId: launch.processIncarnationId,
    proofId: `supervisor-stop-proof-${randomUUID()}`,
    status: "complete-stopped",
    supervisorId: launch.supervisorId,
    writerIncarnationId: launch.writerIncarnationId,
  };
}

function consecutiveFreshSessionIds(existingSessionIds, count) {
  assert.equal(Number.isSafeInteger(count) && count > 0, true);
  const existing = new Set(existingSessionIds);
  for (let attempt = 0; attempt < 32; attempt += 1) {
    const candidates = Array.from(
      { length: Math.max(8, count * 4) },
      () => randomUUID(),
    );
    const candidateSet = new Set(candidates);
    const ordered = [...existingSessionIds, ...candidates].sort();
    for (
      let index = 0;
      index + count <= ordered.length;
      index += 1
    ) {
      const sessionIds = ordered.slice(index, index + count);
      if (
        sessionIds.every(
          (sessionId) =>
            candidateSet.has(sessionId) && !existing.has(sessionId),
        )
      ) {
        const fullSessionOrder = [
          ...existingSessionIds,
          ...sessionIds,
        ].sort();
        const firstSessionIndex = fullSessionOrder.indexOf(sessionIds[0]);
        return {
          afterSessionId:
            firstSessionIndex === 0
              ? null
              : fullSessionOrder[firstSessionIndex - 1],
          sessionIds,
        };
      }
    }
  }
  throw new Error("could not allocate consecutive integration session IDs");
}

async function prepareRestoreGenerationFixture(
  authority,
  checkpointAuthority,
  sessionId,
  {
    finalAttachmentLeaseDurationMilliseconds = 300_000,
    imageDigest = IMAGE_DIGEST,
  } = {},
) {
  const registered = await authority.registerSession(
    registrationInput(sessionId, { imageDigest }),
  );
  const sourceAttachment = await attachWriter(authority, registered, {
    leaseDurationMilliseconds: 300_000,
  });
  const captureAdmission = checkpointCaptureAdmission(sourceAttachment);
  const captureCompletion = await checkpointAuthority.runCapture(
    captureAdmission,
    async (context) => checkpointCompletion(context, false),
  );
  const captureTerminal = await authority.reconcileOperation(
    checkpointOperationInput(
      sourceAttachment.session,
      captureAdmission,
    ),
  );
  assertOperationReceipt(captureTerminal, "committed");
  const released = await releaseWriter(authority, captureTerminal);
  const attached = await attachWriter(authority, released.session, {
    leaseDurationMilliseconds:
      finalAttachmentLeaseDurationMilliseconds,
  });
  assert.equal(
    BigInt(attached.session.document.lease.fencingEpoch) >
      BigInt(captureAdmission.checkpoint.sourceFencingEpoch),
    true,
  );
  return {
    attached,
    captureCompletion,
    checkpoint: captureAdmission.checkpoint,
  };
}

async function prepareCommittedRestoreGenerationFixture(
  authority,
  checkpointAuthority,
  sessionId,
  options,
) {
  const fixture = await prepareRestoreGenerationFixture(
    authority,
    checkpointAuthority,
    sessionId,
    options,
  );
  const admission = restoreGenerationAdmission(
    fixture.attached,
    fixture.checkpoint,
  );
  const input = restoreGenerationOperationInput(
    fixture.attached.session,
    admission,
  );
  await authority.reserveOperation(input);
  const claimed =
    await authority.claimRestoreDestinationGenerationDispatch({
      ...structuredClone(input),
      destinationIsolationProofId:
        `restore-isolation-proof-${randomUUID()}`,
      expectedOperationRevision: "0",
      generationId: `restore-generation-${randomUUID()}`,
    });
  const finalized =
    await authority.finalizeRestoreDestinationGeneration({
      ...structuredClone(input),
      completion: restoreGenerationCompletion(input, claimed, false),
      expectedOperationRevision: "1",
    });
  assertOperationReceipt(finalized, "committed");
  assert.equal(finalized.generation.state, "committed");
  return { ...fixture, admission, finalized, input };
}

async function waitForDatabaseLeaseExpiry(queryable, expiresAt) {
  await queryable.query(
    [
      "SELECT pg_catalog.pg_sleep(",
      "GREATEST(EXTRACT(EPOCH FROM",
      "($1::timestamptz - pg_catalog.clock_timestamp())), 0)",
      "::double precision + 0.2)",
    ].join(" "),
    [expiresAt],
  );
  const expired = await queryable.query(
    [
      "SELECT pg_catalog.clock_timestamp() >= $1::timestamptz",
      "AS lease_expired",
    ].join(" "),
    [expiresAt],
  );
  assert.equal(expired.rows[0].lease_expired, true);
}

async function readRestoreGenerationTransactionState(
  client,
  operationId,
) {
  const result = await client.query(
    [
      "SELECT s.revision::text AS session_revision,",
      "s.document AS session_document,",
      "s.updated_at AS session_updated_at,",
      "o.state AS operation_state,",
      "o.revision::text AS operation_revision,",
      "o.result AS operation_result,",
      "o.updated_at AS operation_updated_at,",
      "o.retired_at AS operation_retired_at,",
      "r.state AS reservation_state,",
      "r.updated_at AS reservation_updated_at,",
      "r.released_at AS reservation_released_at,",
      "g.generation_id, g.state AS generation_state,",
      "g.binding AS generation_binding,",
      "g.document AS generation_document,",
      "g.claimed_at AS generation_claimed_at,",
      "g.committed_at AS generation_committed_at",
      "FROM session_authority.operation_claims o",
      "JOIN session_authority.sessions s",
      "ON s.session_id = o.session_id",
      "JOIN session_authority.reservations r",
      "ON r.operation_id = o.operation_id",
      "LEFT JOIN session_authority.restore_destination_generations g",
      "ON g.operation_id = o.operation_id",
      "WHERE o.operation_id = $1",
    ].join(" "),
    [operationId],
  );
  assert.equal(result.rows.length, 1);
  return result.rows[0];
}

async function readRestoreLaunchHandoffTransactionState(
  client,
  restoreOperationId,
  launchAttemptId,
) {
  const result = await client.query(
    [
      "SELECT s.revision::text AS session_revision,",
      "s.document AS session_document,",
      "s.updated_at AS session_updated_at,",
      "g.state AS generation_state,",
      "g.document AS generation_document,",
      "g.committed_at AS generation_committed_at,",
      "ro.state AS restore_operation_state,",
      "ro.revision::text AS restore_operation_revision,",
      "ro.result AS restore_operation_result,",
      "ro.retired_at AS restore_operation_retired_at,",
      "rr.state AS restore_reservation_state,",
      "rr.released_at AS restore_reservation_released_at,",
      "lo.kind AS launch_operation_kind,",
      "lo.request AS launch_operation_request,",
      "lo.result AS launch_operation_result,",
      "lo.state AS launch_operation_state,",
      "lo.revision::text AS launch_operation_revision,",
      "lo.retired_at AS launch_operation_retired_at,",
      "lr.state AS launch_reservation_state,",
      "lr.released_at AS launch_reservation_released_at,",
      "(g.committed_at = ro.updated_at",
      "AND ro.updated_at = ro.retired_at",
      "AND ro.updated_at = rr.updated_at",
      "AND ro.updated_at = rr.released_at",
      "AND ro.updated_at = lo.created_at",
      "AND ro.updated_at = lo.updated_at",
      "AND ro.updated_at = lr.created_at",
      "AND ro.updated_at = lr.updated_at",
      "AND ro.updated_at = s.updated_at) AS handoff_times_match",
      "FROM session_authority.operation_claims ro",
      "JOIN session_authority.sessions s",
      "ON s.session_id = ro.session_id",
      "JOIN session_authority.reservations rr",
      "ON rr.operation_id = ro.operation_id",
      "JOIN session_authority.restore_destination_generations g",
      "ON g.operation_id = ro.operation_id",
      "LEFT JOIN session_authority.operation_claims lo",
      "ON lo.operation_id = $2 AND lo.session_id = ro.session_id",
      "LEFT JOIN session_authority.reservations lr",
      "ON lr.operation_id = lo.operation_id",
      "WHERE ro.operation_id = $1",
    ].join(" "),
    [restoreOperationId, launchAttemptId],
  );
  assert.equal(result.rows.length, 1);
  return result.rows[0];
}

function integrationArtifactPaths({ checkpoint }) {
  return {
    artifactDirectory:
      `/var/lib/portable-codex-checkpoints/${checkpoint.artifactId}`,
    artifactOwnedRoot: "/var/lib/portable-codex-checkpoints",
  };
}

function integrationSourceOwnedRoot({ canonicalAttachment }) {
  return {
    sourceDirectory: canonicalAttachment.rootPath,
    sourceOwnedRoot: "/var/lib/portable-codex",
  };
}

function checkpointCompletion(context, replayed) {
  const result =
    context.result ?? context.captureAttempt.result;
  const operationId = result.mutation.operationId;
  const artifactId = result.checkpoint.artifactId;
  const artifactManifestDigest = "b".repeat(64);
  const modeledDigest = "c".repeat(64);
  return Object.freeze({
    artifactProof: Object.freeze({
      artifactManifestDigest,
      captureOperationId: operationId,
      modeledDigest,
    }),
    materialization: Object.freeze({
      artifactManifestDigest,
      contractVersion: 2,
      modeledDigest,
      publicationId: `publication-${operationId}`,
      publicationKind: "checkpoint-artifact",
      stagedRoot: Object.freeze({
        filesystemId: "integration-filesystem",
        objectIdentityScheme: "integration-object-v1",
        objectId: `object-${artifactId}`,
      }),
      treeIdentityDigest: "d".repeat(64),
    }),
    replayed,
    result,
  });
}

function assertOperationReceipt(
  receipt,
  state,
  {
    activeOperationId =
      state === "committed" ? null : receipt.operation.operationId,
    currentTerminal = state === "committed",
  } = {},
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
    activeOperationId,
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

    const trackedMigrations = await readTrackedAuthorityMigrations();
    const latestMigration = trackedMigrations.at(-1);
    assert.equal(SESSION_AUTHORITY_MIGRATION_VERSION, 6);
    assert.deepEqual(
      trackedMigrations.map(({ version }) => version),
      [1, 2, 3, 4, 5, 6],
    );

    await pool.query(
      "DROP SCHEMA IF EXISTS session_authority CASCADE",
    );
    const freshMigration = await store.migrate();
    assert.deepEqual(freshMigration, {
      applied: true,
      checksum: latestMigration.checksum,
      version: 6,
    });
    assert.deepEqual(
      await readMigrationLedger(pool),
      trackedMigrations.map(({ checksum, version }) => ({
        checksum,
        version,
      })),
    );
    const freshNoOpMigration = await store.migrate();
    assert.deepEqual(freshNoOpMigration, {
      applied: false,
      checksum: latestMigration.checksum,
      version: 6,
    });

    await pool.query("DROP SCHEMA session_authority CASCADE");
    await installVersionOneAuthority(pool, trackedMigrations[0]);
    assert.deepEqual(await readMigrationLedger(pool), [
      {
        checksum: trackedMigrations[0].checksum,
        version: 1,
      },
    ]);
    const upgradeMigration = await store.migrate();
    assert.deepEqual(upgradeMigration, {
      applied: true,
      checksum: latestMigration.checksum,
      version: 6,
    });
    assert.deepEqual(
      await readMigrationLedger(pool),
      trackedMigrations.map(({ checksum, version }) => ({
        checksum,
        version,
      })),
    );
    assert.deepEqual(await store.migrate(), {
      applied: false,
      checksum: latestMigration.checksum,
      version: 6,
    });
    await assertLegacyRestoreV2MigrationGate(
      pool,
      store,
      trackedMigrations,
    );
    await assertOperationIdRegistryConcurrency(pool);
    await assertWriterStopCaptureHandoffSchema(pool);
    await assertRestoreGenerationConstraints(pool);
    await assertRestoreRecoveryCursorSchemaAndStore(pool, store);

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
          "to_regclass('session_authority.restore_destination_generations')::text AS generations,",
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
      generations:
        "session_authority.restore_destination_generations",
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
          await insertDirectOperationIdClaim(transaction, {
            claimedAt: transaction.now,
            operationId,
            sessionId: activeOperationSessionId,
          });
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
        await insertDirectOperationIdClaim(transaction, {
          claimedAt: transaction.now,
          operationId: retiredOperationId,
          sessionId: activeReservationSessionId,
        });
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
        await insertDirectOperationIdClaim(transaction, {
          claimedAt: transaction.now,
          operationId: activeOperationId,
          sessionId: activeReservationSessionId,
        });
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
  { timeout: 60_000 },
  async (t) => {
    const pool = new Pool({
      application_name:
        SESSION_AUTHORITY_APPLICATION_NAME,
      connectionString: databaseUrl,
      max: 3,
    });
    const guardPool = new Pool({
      application_name: CHECKPOINT_GUARD_APPLICATION_NAME,
      connectionString: databaseUrl,
      max: 2,
    });
    const sessionIds = [];
    t.after(async () => {
      try {
        if (sessionIds.length > 0) {
          await pool.query(
            [
              "DELETE FROM session_authority.restore_destination_generations",
              "WHERE session_id = ANY($1::uuid[])",
            ].join(" "),
            [sessionIds],
          );
          await pool.query(
            [
              "DELETE FROM session_authority.checkpoint_catalogue",
              "WHERE session_id = ANY($1::uuid[])",
            ].join(" "),
            [sessionIds],
          );
          await pool.query(
            [
              "DELETE FROM session_authority.capture_attempt_tombstones",
              "WHERE session_id = ANY($1::uuid[])",
            ].join(" "),
            [sessionIds],
          );
          await pool.query(
            [
              "DELETE FROM session_authority.capture_attempt_claims",
              "WHERE session_id = ANY($1::uuid[])",
            ].join(" "),
            [sessionIds],
          );
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
              "WHERE operation_id IN (",
              "SELECT operation_id",
              "FROM session_authority.operation_id_registry",
              "WHERE session_id = ANY($1::uuid[])",
              "AND claim_type IN (",
              "'restore-launch-intent-v2',",
              "'restore-activation-launch-intent-v1',",
              "'writer-stop-capture-intent-v3'",
              ")",
              ")",
            ].join(" "),
            [sessionIds],
          );
          await pool.query(
            [
              "DELETE FROM session_authority.operation_id_registry",
              "WHERE session_id = ANY($1::uuid[])",
              "AND claim_type IN (",
              "'restore-launch-intent-v2',",
              "'restore-activation-launch-intent-v1',",
              "'writer-stop-capture-intent-v3'",
              ")",
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
              "DELETE FROM session_authority.operation_id_registry",
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
        try {
          await guardPool.end();
        } finally {
          await pool.end();
        }
      }
    });
    const store = new PostgresSerializableStore({
      dedicatedPool: pool,
      maxTransactionAttempts: 3,
    });
    await store.migrate();
    const authority = new PostgresSessionAuthority({
      restoreAttachmentActivationV2FleetCompatible: true,
      restoreAttachmentActivationV2GenerationPredecessorFleetCompatible:
        true,
      restoreGenerationV2FleetCompatible: true,
      store,
      writerLaunchStopV3FleetCompatible: true,
    });
    const operationGuard = new PostgresOperationGuard({
      dedicatedPool: guardPool,
    });
    const checkpointAuthority =
      createPostgresCheckpointMutationAuthority({
        authority,
        operationGuard,
        resolveArtifactPaths: integrationArtifactPaths,
        resolveSourceOwnedRoot: integrationSourceOwnedRoot,
      });

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
      "writer release reaches DETACHED and replays one exact terminal proof",
      async () => {
        const sessionId = randomUUID();
        sessionIds.push(sessionId);
        const registered = await authority.registerSession(
          registrationInput(sessionId),
        );
        const attached = await attachWriter(authority, registered);
        const input = writerReleaseInput(attached.session);

        const reserved = await authority.reserveOperation(input);
        assertOperationReceipt(reserved, "prepared");
        assert.equal(reserved.acquired, true);
        assert.equal(
          reserved.session.document.lifecycle,
          "ATTACHED",
        );

        const transition = {
          ...structuredClone(input),
          expectedOperationRevision: "0",
        };
        const starting =
          await authority.claimWriterReleaseDispatch(transition);
        assertOperationReceipt(starting, "starting");
        assert.equal(starting.dispatchGranted, true);
        assert.equal(
          starting.session.document.lifecycle,
          "RELEASING",
        );
        assert.equal(
          starting.session.document.writerEpoch,
          attached.session.document.writerEpoch,
        );
        assert.deepEqual(
          starting.session.document.lease,
          attached.session.document.lease,
        );
        assert.deepEqual(
          starting.session.document.attachment,
          attached.session.document.attachment,
        );
        assert.equal(starting.mutationRequest.operation, "detach");
        assert.equal(
          starting.mutationRequest.target.attachmentId,
          attached.session.document.attachment.attachmentId,
        );

        const mutationResult = detachEvidence(
          starting.mutationRequest,
        );
        const finalization = {
          ...structuredClone(input),
          expectedOperationRevision: "1",
          mutationResult,
        };
        const released =
          await authority.finalizeWriterRelease(finalization);
        assertOperationReceipt(released, "committed");
        assert.equal(released.finalized, true);
        assert.equal(released.operation.revision, "2");
        assert.equal(
          released.operation.result.outcome,
          "writer-released",
        );
        assert.deepEqual(
          released.operation.result.lease,
          attached.session.document.lease,
        );
        assert.deepEqual(
          released.operation.result.attachment,
          attached.session.document.attachment,
        );
        assert.deepEqual(
          released.operation.result.mutationResult,
          mutationResult,
        );
        assert.equal(
          released.session.document.lifecycle,
          "DETACHED",
        );
        assert.equal(
          released.session.document.writerEpoch,
          attached.session.document.writerEpoch,
        );
        assert.equal(released.session.document.lease, null);
        assert.equal(released.session.document.attachment, null);

        const replayedDispatch =
          await authority.claimWriterReleaseDispatch(
            structuredClone(transition),
          );
        assertOperationReceipt(replayedDispatch, "committed");
        assert.equal(replayedDispatch.dispatchGranted, false);
        assert.deepEqual(
          replayedDispatch.mutationRequest,
          starting.mutationRequest,
        );
        assert.deepEqual(
          replayedDispatch.operation,
          released.operation,
        );
        assert.deepEqual(
          replayedDispatch.reservation,
          released.reservation,
        );
        assert.deepEqual(replayedDispatch.session, released.session);

        const replayedFinalization =
          await authority.finalizeWriterRelease(
            structuredClone(finalization),
          );
        assertOperationReceipt(replayedFinalization, "committed");
        assert.equal(replayedFinalization.finalized, false);
        assert.deepEqual(
          replayedFinalization.operation,
          released.operation,
        );
        assert.deepEqual(
          replayedFinalization.reservation,
          released.reservation,
        );
        assert.deepEqual(
          replayedFinalization.session,
          released.session,
        );

        const restarted = new PostgresSessionAuthority({ store });
        const reconciled = await restarted.reconcileOperation(
          structuredClone(input),
        );
        assertOperationReceipt(reconciled, "committed");
        assert.deepEqual(reconciled.operation, released.operation);
        assert.deepEqual(
          reconciled.reservation,
          released.reservation,
        );
        assert.deepEqual(reconciled.session, released.session);
      },
    );

    await t.test(
      "uncertain release blocks until force fence verifies DETACHED",
      async () => {
        const sessionId = randomUUID();
        sessionIds.push(sessionId);
        const registered = await authority.registerSession(
          registrationInput(sessionId),
        );
        const attached = await attachWriter(authority, registered);
        const releaseInput = writerReleaseInput(attached.session);
        await authority.reserveOperation(releaseInput);
        const releaseStarting =
          await authority.claimWriterReleaseDispatch({
            ...structuredClone(releaseInput),
            expectedOperationRevision: "0",
          });
        assertOperationReceipt(releaseStarting, "starting");

        const releaseUncertain =
          await authority.markOperationUncertain({
            ...structuredClone(releaseInput),
            expectedOperationRevision: "1",
          });
        assertOperationReceipt(releaseUncertain, "uncertain");
        assert.equal(releaseUncertain.changed, true);
        assert.equal(
          releaseUncertain.session.document.lifecycle,
          "RELEASING",
        );
        assert.equal(
          releaseUncertain.session.document.writerEpoch,
          releaseStarting.session.document.writerEpoch,
        );

        const blockedFinalization = {
          ...structuredClone(releaseInput),
          expectedOperationRevision: "2",
          reason: "provider-outcome-unresolved",
        };
        const blocked =
          await authority.finalizeWriterOperationBlocked(
            blockedFinalization,
          );
        assertOperationReceipt(blocked, "committed");
        assert.equal(blocked.finalized, true);
        assert.equal(blocked.operation.revision, "3");
        assert.equal(
          blocked.operation.result.outcome,
          "writer-blocked",
        );
        assert.equal(
          blocked.operation.result.reason,
          "provider-outcome-unresolved",
        );
        assert.equal(blocked.session.document.lifecycle, "BLOCKED");
        assert.equal(
          blocked.session.document.writerEpoch,
          attached.session.document.writerEpoch,
        );
        assert.deepEqual(
          blocked.session.document.lease,
          attached.session.document.lease,
        );
        assert.deepEqual(
          blocked.session.document.attachment,
          attached.session.document.attachment,
        );

        const blockedReplay =
          await authority.finalizeWriterOperationBlocked(
            structuredClone(blockedFinalization),
          );
        assertOperationReceipt(blockedReplay, "committed");
        assert.equal(blockedReplay.finalized, false);
        assert.deepEqual(blockedReplay.operation, blocked.operation);
        assert.deepEqual(blockedReplay.session, blocked.session);

        const fenceInput = writerForceFenceInput(blocked.session);
        const fenceReserved =
          await authority.reserveOperation(fenceInput);
        assertOperationReceipt(fenceReserved, "prepared");
        assert.equal(fenceReserved.acquired, true);
        assert.equal(
          fenceReserved.session.document.lifecycle,
          "BLOCKED",
        );

        const fenceStarting =
          await authority.claimWriterForceFenceDispatch({
            ...structuredClone(fenceInput),
            expectedOperationRevision: "0",
          });
        assertOperationReceipt(fenceStarting, "starting");
        assert.equal(fenceStarting.dispatchGranted, true);
        assert.equal(
          fenceStarting.session.document.lifecycle,
          "FENCING",
        );
        assert.equal(
          BigInt(fenceStarting.writerEpoch),
          BigInt(blocked.session.document.writerEpoch) + 1n,
        );
        assert.equal(
          fenceStarting.fenceRequest.fencingEpoch,
          fenceStarting.writerEpoch,
        );
        assert.deepEqual(
          fenceStarting.fenceRequest.revokedFence,
          {
            fencingEpoch:
              blocked.session.document.lease.fencingEpoch,
            holderId: blocked.session.document.lease.holderId,
            leaseId: blocked.session.document.lease.leaseId,
          },
        );

        const fenceResult = forceFenceEvidence(
          fenceStarting.fenceRequest,
        );
        const fenceFinalization = {
          ...structuredClone(fenceInput),
          expectedOperationRevision: "1",
          fenceResult,
        };
        const fenced =
          await authority.finalizeWriterForceFence(
            fenceFinalization,
          );
        assertOperationReceipt(fenced, "committed");
        assert.equal(fenced.finalized, true);
        assert.equal(
          fenced.operation.result.outcome,
          "writer-fenced",
        );
        assert.equal(
          fenced.operation.result.writerEpoch,
          fenceStarting.writerEpoch,
        );
        assert.deepEqual(
          fenced.operation.result.fenceResult,
          fenceResult,
        );
        assert.equal(
          fenced.session.document.lifecycle,
          "DETACHED",
        );
        assert.equal(
          fenced.session.document.writerEpoch,
          fenceStarting.writerEpoch,
        );
        assert.equal(fenced.session.document.lease, null);
        assert.equal(fenced.session.document.attachment, null);

        const fenceReplay =
          await authority.finalizeWriterForceFence(
            structuredClone(fenceFinalization),
          );
        assertOperationReceipt(fenceReplay, "committed");
        assert.equal(fenceReplay.finalized, false);
        assert.deepEqual(fenceReplay.operation, fenced.operation);
        assert.deepEqual(fenceReplay.session, fenced.session);
      },
    );

    await t.test(
      "uncertain force fence becomes BLOCKED without advancing epoch twice",
      async () => {
        const sessionId = randomUUID();
        sessionIds.push(sessionId);
        const registered = await authority.registerSession(
          registrationInput(sessionId),
        );
        const attached = await attachWriter(authority, registered);
        const input = writerForceFenceInput(attached.session);
        await authority.reserveOperation(input);

        const starting =
          await authority.claimWriterForceFenceDispatch({
            ...structuredClone(input),
            expectedOperationRevision: "0",
          });
        assertOperationReceipt(starting, "starting");
        assert.equal(starting.dispatchGranted, true);
        assert.equal(
          BigInt(starting.writerEpoch),
          BigInt(attached.session.document.writerEpoch) + 1n,
        );

        const uncertain = await authority.markOperationUncertain({
          ...structuredClone(input),
          expectedOperationRevision: "1",
        });
        assertOperationReceipt(uncertain, "uncertain");
        assert.equal(uncertain.changed, true);
        assert.equal(
          uncertain.session.document.lifecycle,
          "FENCING",
        );
        assert.equal(
          uncertain.session.document.writerEpoch,
          starting.writerEpoch,
        );

        const finalization = {
          ...structuredClone(input),
          expectedOperationRevision: "2",
          reason: "fence-unavailable",
        };
        const blocked =
          await authority.finalizeWriterOperationBlocked(
            finalization,
          );
        assertOperationReceipt(blocked, "committed");
        assert.equal(blocked.finalized, true);
        assert.equal(blocked.session.document.lifecycle, "BLOCKED");
        assert.equal(
          blocked.session.document.writerEpoch,
          starting.writerEpoch,
        );
        assert.equal(
          blocked.operation.result.writerEpoch,
          starting.writerEpoch,
        );
        assert.equal(
          blocked.operation.result.reason,
          "fence-unavailable",
        );
        assert.deepEqual(
          blocked.session.document.lease,
          attached.session.document.lease,
        );
        assert.deepEqual(
          blocked.session.document.attachment,
          attached.session.document.attachment,
        );

        const replayed =
          await authority.finalizeWriterOperationBlocked(
            structuredClone(finalization),
          );
        assertOperationReceipt(replayed, "committed");
        assert.equal(replayed.finalized, false);
        assert.deepEqual(replayed.operation, blocked.operation);
        assert.deepEqual(replayed.session, blocked.session);
      },
    );

    await t.test(
      "force-fence dispatch COMMIT loss restarts without regranting or re-advancing epoch",
      async () => {
        const sessionId = randomUUID();
        sessionIds.push(sessionId);
        const registered = await authority.registerSession(
          registrationInput(sessionId),
        );
        const attached = await attachWriter(authority, registered);
        const input = writerForceFenceInput(attached.session);
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
          acknowledgementLossAuthority.claimWriterForceFenceDispatch(
            transition,
          ),
          assertCommitOutcomeUncertain,
        );

        const restarted = new PostgresSessionAuthority({ store });
        const readBack = await restarted.readSession({ sessionId });
        assert.equal(readBack.document.lifecycle, "FENCING");
        assert.equal(
          BigInt(readBack.document.writerEpoch),
          BigInt(attached.session.document.writerEpoch) + 1n,
        );

        const reconciled = await restarted.reconcileOperation(input);
        assertOperationReceipt(reconciled, "starting");
        assert.deepEqual(reconciled.session, readBack);
        assert.equal(
          reconciled.session.document.writerEpoch,
          readBack.document.writerEpoch,
        );

        for (let replayIndex = 0; replayIndex < 2; replayIndex += 1) {
          const replayed =
            await restarted.claimWriterForceFenceDispatch(
              structuredClone(transition),
            );
          assertOperationReceipt(replayed, "starting");
          assert.equal(replayed.dispatchGranted, false);
          assert.equal(
            replayed.writerEpoch,
            readBack.document.writerEpoch,
          );
          assert.deepEqual(replayed.operation, reconciled.operation);
          assert.deepEqual(
            replayed.reservation,
            reconciled.reservation,
          );
          assert.deepEqual(replayed.session, readBack);
          assert.deepEqual(
            replayed.fenceRequest.revokedFence,
            {
              fencingEpoch:
                attached.session.document.lease.fencingEpoch,
              holderId: attached.session.document.lease.holderId,
              leaseId: attached.session.document.lease.leaseId,
            },
          );
        }
      },
    );

    await t.test(
      "force-fence finalize COMMIT loss restarts with one terminal proof",
      async () => {
        const sessionId = randomUUID();
        sessionIds.push(sessionId);
        const registered = await authority.registerSession(
          registrationInput(sessionId),
        );
        const attached = await attachWriter(authority, registered);
        const input = writerForceFenceInput(attached.session);
        await authority.reserveOperation(input);
        const starting =
          await authority.claimWriterForceFenceDispatch({
            ...structuredClone(input),
            expectedOperationRevision: "0",
          });
        const fenceResult = forceFenceEvidence(
          starting.fenceRequest,
        );
        const finalization = {
          ...structuredClone(input),
          expectedOperationRevision: "1",
          fenceResult,
        };
        const acknowledgementLossAuthority =
          new PostgresSessionAuthority({
            store: new PostgresSerializableStore({
              dedicatedPool:
                firstCommitAcknowledgementLossPool(pool),
            }),
          });

        await assert.rejects(
          acknowledgementLossAuthority.finalizeWriterForceFence(
            finalization,
          ),
          assertCommitOutcomeUncertain,
        );

        const restarted = new PostgresSessionAuthority({ store });
        const readBack = await restarted.readSession({ sessionId });
        assert.equal(readBack.document.lifecycle, "DETACHED");
        assert.equal(
          readBack.document.writerEpoch,
          starting.writerEpoch,
        );
        assert.equal(readBack.document.lease, null);
        assert.equal(readBack.document.attachment, null);

        const reconciled = await restarted.reconcileOperation(input);
        assertOperationReceipt(reconciled, "committed");
        assert.deepEqual(reconciled.session, readBack);
        assert.equal(
          reconciled.operation.result.outcome,
          "writer-fenced",
        );
        assert.deepEqual(
          reconciled.operation.result.fenceResult,
          fenceResult,
        );

        const storedBeforeReplay = await pool.query(
          [
            "SELECT s.revision::text AS session_revision,",
            "s.updated_at AS session_updated_at,",
            "o.revision::text AS operation_revision,",
            "o.updated_at AS operation_updated_at,",
            "o.result AS operation_result",
            "FROM session_authority.sessions s",
            "JOIN session_authority.operation_claims o",
            "ON o.session_id = s.session_id",
            "WHERE s.session_id = $1 AND o.operation_id = $2",
          ].join(" "),
          [sessionId, input.operationId],
        );
        assert.equal(storedBeforeReplay.rows.length, 1);

        const replayed =
          await restarted.finalizeWriterForceFence(
            structuredClone(finalization),
          );
        assertOperationReceipt(replayed, "committed");
        assert.equal(replayed.finalized, false);
        assert.deepEqual(replayed.operation, reconciled.operation);
        assert.deepEqual(
          replayed.reservation,
          reconciled.reservation,
        );
        assert.deepEqual(replayed.session, readBack);

        const storedAfterReplay = await pool.query(
          [
            "SELECT s.revision::text AS session_revision,",
            "s.updated_at AS session_updated_at,",
            "o.revision::text AS operation_revision,",
            "o.updated_at AS operation_updated_at,",
            "o.result AS operation_result",
            "FROM session_authority.sessions s",
            "JOIN session_authority.operation_claims o",
            "ON o.session_id = s.session_id",
            "WHERE s.session_id = $1 AND o.operation_id = $2",
          ].join(" "),
          [sessionId, input.operationId],
        );
        assert.deepEqual(
          storedAfterReplay.rows,
          storedBeforeReplay.rows,
        );
      },
    );

    await t.test(
      "a second force fence from BLOCKED advances once and preserves the original fence",
      async () => {
        const sessionId = randomUUID();
        sessionIds.push(sessionId);
        const registered = await authority.registerSession(
          registrationInput(sessionId),
        );
        const attached = await attachWriter(authority, registered);
        const originalLease = structuredClone(
          attached.session.document.lease,
        );
        const originalTarget = {
          attachmentId:
            attached.session.document.attachment.attachmentId,
          kind: "attachment",
        };

        const firstInput = writerForceFenceInput(attached.session);
        await authority.reserveOperation(firstInput);
        const firstStarting =
          await authority.claimWriterForceFenceDispatch({
            ...structuredClone(firstInput),
            expectedOperationRevision: "0",
          });
        const firstUncertain =
          await authority.markOperationUncertain({
            ...structuredClone(firstInput),
            expectedOperationRevision: "1",
          });
        assertOperationReceipt(firstUncertain, "uncertain");
        const firstBlocked =
          await authority.finalizeWriterOperationBlocked({
            ...structuredClone(firstInput),
            expectedOperationRevision: "2",
            reason: "fence-unavailable",
          });
        assertOperationReceipt(firstBlocked, "committed");
        assert.equal(
          firstBlocked.session.document.writerEpoch,
          firstStarting.writerEpoch,
        );
        assert.deepEqual(
          firstBlocked.session.document.lease,
          originalLease,
        );
        assert.deepEqual(
          firstBlocked.operation.result.fenceTarget,
          originalTarget,
        );

        const secondInput = writerForceFenceInput(
          firstBlocked.session,
        );
        const secondReserved =
          await authority.reserveOperation(secondInput);
        assertOperationReceipt(secondReserved, "prepared");
        assert.equal(
          secondReserved.session.document.lifecycle,
          "BLOCKED",
        );
        assert.equal(
          secondReserved.session.document.writerEpoch,
          firstStarting.writerEpoch,
        );
        const secondTransition = {
          ...structuredClone(secondInput),
          expectedOperationRevision: "0",
        };
        const secondStarting =
          await authority.claimWriterForceFenceDispatch(
            secondTransition,
          );
        assertOperationReceipt(secondStarting, "starting");
        assert.equal(secondStarting.dispatchGranted, true);
        assert.equal(
          BigInt(secondStarting.writerEpoch),
          BigInt(firstStarting.writerEpoch) + 1n,
        );
        assert.deepEqual(
          secondStarting.fenceRequest.revokedFence,
          {
            fencingEpoch: originalLease.fencingEpoch,
            holderId: originalLease.holderId,
            leaseId: originalLease.leaseId,
          },
        );
        assert.deepEqual(
          secondStarting.fenceRequest.target,
          originalTarget,
        );

        const secondReplay =
          await authority.claimWriterForceFenceDispatch(
            structuredClone(secondTransition),
          );
        assertOperationReceipt(secondReplay, "starting");
        assert.equal(secondReplay.dispatchGranted, false);
        assert.equal(
          secondReplay.writerEpoch,
          secondStarting.writerEpoch,
        );
        assert.deepEqual(
          secondReplay.fenceRequest,
          secondStarting.fenceRequest,
        );
        assert.deepEqual(
          secondReplay.session,
          secondStarting.session,
        );

        const secondFenceResult = forceFenceEvidence(
          secondStarting.fenceRequest,
        );
        const fenced = await authority.finalizeWriterForceFence({
          ...structuredClone(secondInput),
          expectedOperationRevision: "1",
          fenceResult: secondFenceResult,
        });
        assertOperationReceipt(fenced, "committed");
        assert.equal(fenced.finalized, true);
        assert.equal(fenced.session.document.lifecycle, "DETACHED");
        assert.equal(
          fenced.session.document.writerEpoch,
          secondStarting.writerEpoch,
        );
        assert.equal(fenced.session.document.lease, null);
        assert.equal(fenced.session.document.attachment, null);
        assert.deepEqual(
          fenced.operation.result.fenceResult,
          secondFenceResult,
        );
        assert.deepEqual(
          fenced.operation.result.lease,
          originalLease,
        );
        assert.deepEqual(
          fenced.operation.result.fenceResult.target,
          originalTarget,
        );
      },
    );

    await t.test(
      "concurrent identical force-fence dispatch grants once at one epoch",
      async () => {
        const sessionId = randomUUID();
        sessionIds.push(sessionId);
        const registered = await authority.registerSession(
          registrationInput(sessionId),
        );
        const attached = await attachWriter(authority, registered);
        const input = writerForceFenceInput(attached.session);
        await authority.reserveOperation(input);
        const transition = {
          ...structuredClone(input),
          expectedOperationRevision: "0",
        };
        const concurrentAuthority =
          new PostgresSessionAuthority({
            store: new PostgresSerializableStore({
              dedicatedPool: firstSessionLockQueryBarrierPool(
                pool,
                2,
                "identical force-fence dispatch barrier",
              ),
              maxTransactionAttempts: 3,
            }),
          });

        const receipts = await Promise.all([
          concurrentAuthority.claimWriterForceFenceDispatch(
            transition,
          ),
          concurrentAuthority.claimWriterForceFenceDispatch(
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
        for (const receipt of receipts) {
          assertOperationReceipt(receipt, "starting");
          assert.equal(
            BigInt(receipt.writerEpoch),
            BigInt(attached.session.document.writerEpoch) + 1n,
          );
        }
        assert.deepEqual(receipts[0].operation, receipts[1].operation);
        assert.deepEqual(
          receipts[0].reservation,
          receipts[1].reservation,
        );
        assert.deepEqual(receipts[0].session, receipts[1].session);
        assert.deepEqual(
          receipts[0].fenceRequest,
          receipts[1].fenceRequest,
        );

        const fenced = await authority.finalizeWriterForceFence({
          ...structuredClone(input),
          expectedOperationRevision: "1",
          fenceResult: forceFenceEvidence(
            receipts[0].fenceRequest,
          ),
        });
        assertOperationReceipt(fenced, "committed");
        assert.equal(fenced.finalized, true);
        assert.equal(
          fenced.session.document.writerEpoch,
          receipts[0].writerEpoch,
        );

        const stored = await pool.query(
          [
            "SELECT s.document->>'writerEpoch' AS writer_epoch,",
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
          writer_epoch: receipts[0].writerEpoch,
        });
      },
    );

    await t.test(
      "release dispatch and finalize COMMIT loss recover without duplicate effects",
      async () => {
        const sessionId = randomUUID();
        sessionIds.push(sessionId);
        const registered = await authority.registerSession(
          registrationInput(sessionId),
        );
        const attached = await attachWriter(authority, registered);
        const input = writerReleaseInput(attached.session);
        await authority.reserveOperation(input);
        const transition = {
          ...structuredClone(input),
          expectedOperationRevision: "0",
        };

        const dispatchLossAuthority =
          new PostgresSessionAuthority({
            store: new PostgresSerializableStore({
              dedicatedPool:
                firstCommitAcknowledgementLossPool(pool),
            }),
          });
        await assert.rejects(
          dispatchLossAuthority.claimWriterReleaseDispatch(
            transition,
          ),
          assertCommitOutcomeUncertain,
        );

        const dispatchReconciled =
          await authority.reconcileOperation(input);
        assertOperationReceipt(dispatchReconciled, "starting");
        assert.equal(
          dispatchReconciled.session.document.lifecycle,
          "RELEASING",
        );
        assert.equal(
          dispatchReconciled.session.document.writerEpoch,
          attached.session.document.writerEpoch,
        );
        const dispatchReplay =
          await authority.claimWriterReleaseDispatch(
            structuredClone(transition),
          );
        assertOperationReceipt(dispatchReplay, "starting");
        assert.equal(dispatchReplay.dispatchGranted, false);
        assert.deepEqual(
          dispatchReplay.operation,
          dispatchReconciled.operation,
        );
        assert.deepEqual(
          dispatchReplay.session,
          dispatchReconciled.session,
        );

        const finalization = {
          ...structuredClone(input),
          expectedOperationRevision: "1",
          mutationResult: detachEvidence(
            dispatchReplay.mutationRequest,
          ),
        };
        const finalizeLossAuthority =
          new PostgresSessionAuthority({
            store: new PostgresSerializableStore({
              dedicatedPool:
                firstCommitAcknowledgementLossPool(pool),
            }),
          });
        await assert.rejects(
          finalizeLossAuthority.finalizeWriterRelease(
            finalization,
          ),
          assertCommitOutcomeUncertain,
        );

        const finalizeReconciled =
          await authority.reconcileOperation(input);
        assertOperationReceipt(finalizeReconciled, "committed");
        assert.equal(
          finalizeReconciled.session.document.lifecycle,
          "DETACHED",
        );
        assert.equal(
          finalizeReconciled.session.document.writerEpoch,
          attached.session.document.writerEpoch,
        );
        const finalizeReplay =
          await authority.finalizeWriterRelease(
            structuredClone(finalization),
          );
        assertOperationReceipt(finalizeReplay, "committed");
        assert.equal(finalizeReplay.finalized, false);
        assert.deepEqual(
          finalizeReplay.operation,
          finalizeReconciled.operation,
        );
        assert.deepEqual(
          finalizeReplay.reservation,
          finalizeReconciled.reservation,
        );
        assert.deepEqual(
          finalizeReplay.session,
          finalizeReconciled.session,
        );
      },
    );

    await t.test(
      "writer detach composition holds the real guard and replays release without provider work",
      async () => {
        const sessionId = randomUUID();
        sessionIds.push(sessionId);
        const registered = await authority.registerSession(
          registrationInput(sessionId),
        );
        const attached = await attachWriter(authority, registered);
        let detachCalls = 0;
        const backend = writerDetachIntegrationBackend({
          async detachAttachment(request) {
            detachCalls += 1;
            const held = await pool.query(
              [
                "SELECT count(*)::integer AS held",
                "FROM pg_catalog.pg_locks l",
                "JOIN pg_catalog.pg_stat_activity a ON a.pid = l.pid",
                "WHERE l.locktype = 'advisory' AND l.granted",
                "AND a.application_name = $1",
              ].join(" "),
              [CHECKPOINT_GUARD_APPLICATION_NAME],
            );
            assert.equal(held.rows[0].held, 1);
            return detachEvidence(request);
          },
          async forceFence() {
            throw new Error("unexpected force fence");
          },
        });
        const composition = createPostgresWriterDetachComposition({
          authority,
          operationGuard,
          storageBackend: backend,
        });
        const request = writerDetachCompositionRequest(
          attached.session,
        );

        const terminal = await composition.detachWriter(request);
        assert.equal(detachCalls, 1);
        assert.equal(
          terminal.operation.result.outcome,
          "writer-released",
        );
        assert.equal(terminal.session.document.lifecycle, "DETACHED");
        assert.equal(terminal.session.document.lease, null);
        assert.equal(terminal.session.document.attachment, null);
        assert.equal(
          terminal.session.document.writerEpoch,
          attached.session.document.writerEpoch,
        );

        const replayed = await composition.detachWriter(
          structuredClone(request),
        );
        assert.equal(detachCalls, 1);
        assert.deepEqual(replayed, terminal);

        const heldAfter = await pool.query(
          [
            "SELECT count(*)::integer AS held",
            "FROM pg_catalog.pg_locks l",
            "JOIN pg_catalog.pg_stat_activity a ON a.pid = l.pid",
            "WHERE l.locktype = 'advisory' AND l.granted",
            "AND a.application_name = $1",
          ].join(" "),
          [CHECKPOINT_GUARD_APPLICATION_NAME],
        );
        assert.equal(heldAfter.rows[0].held, 0);
      },
    );

    await t.test(
      "writer detach composition blocks ambiguous release then force-fences explicitly",
      async () => {
        const sessionId = randomUUID();
        sessionIds.push(sessionId);
        const registered = await authority.registerSession(
          registrationInput(sessionId),
        );
        const attached = await attachWriter(authority, registered);
        let detachCalls = 0;
        let fenceCalls = 0;
        const backend = writerDetachIntegrationBackend({
          async detachAttachment() {
            detachCalls += 1;
            return Object.freeze({ malformed: true });
          },
          async forceFence(request) {
            fenceCalls += 1;
            return forceFenceEvidence(request);
          },
        });
        const composition = createPostgresWriterDetachComposition({
          authority,
          operationGuard,
          storageBackend: backend,
        });
        const releaseRequest = writerDetachCompositionRequest(
          attached.session,
        );

        const blocked = await composition.detachWriter(releaseRequest);
        assert.equal(detachCalls, 1);
        assert.equal(fenceCalls, 0);
        assert.equal(
          blocked.operation.result.outcome,
          "writer-blocked",
        );
        assert.equal(
          blocked.operation.result.reason,
          "provider-outcome-unresolved",
        );
        assert.equal(blocked.session.document.lifecycle, "BLOCKED");
        assert.deepEqual(
          blocked.session.document.lease,
          attached.session.document.lease,
        );
        assert.deepEqual(
          blocked.session.document.attachment,
          attached.session.document.attachment,
        );

        const fenceRequest = writerDetachCompositionRequest(
          blocked.session,
          { target: releaseRequest.target },
        );
        const fenced = await composition.forceFenceWriter(
          fenceRequest,
        );
        assert.equal(detachCalls, 1);
        assert.equal(fenceCalls, 1);
        assert.equal(
          fenced.operation.result.outcome,
          "writer-fenced",
        );
        assert.equal(fenced.session.document.lifecycle, "DETACHED");
        assert.equal(
          BigInt(fenced.session.document.writerEpoch),
          BigInt(attached.session.document.writerEpoch) + 1n,
        );

        const manualSessionId = randomUUID();
        sessionIds.push(manualSessionId);
        const manualRegistration = registrationInput(manualSessionId);
        manualRegistration.backendCapabilities = {
          ...manualRegistration.backendCapabilities,
          fencing: "manual",
        };
        const manualRegistered = await authority.registerSession(
          manualRegistration,
        );
        const manualAttached = await attachWriter(
          authority,
          manualRegistered,
        );
        let manualFenceCalls = 0;
        const manualBackend = writerDetachIntegrationBackend({
          capabilities: manualRegistration.backendCapabilities,
          async detachAttachment(request) {
            return detachEvidence(request);
          },
          async forceFence(request) {
            manualFenceCalls += 1;
            return forceFenceEvidence(request);
          },
        });
        const manualComposition =
          createPostgresWriterDetachComposition({
            authority,
            operationGuard,
            storageBackend: manualBackend,
          });
        const manualBlocked =
          await manualComposition.forceFenceWriter(
            writerDetachCompositionRequest(manualAttached.session),
          );
        assert.equal(manualFenceCalls, 0);
        assert.equal(
          manualBlocked.operation.result.outcome,
          "writer-blocked",
        );
        assert.equal(
          manualBlocked.operation.result.reason,
          "fence-unavailable",
        );
        assert.equal(
          BigInt(manualBlocked.session.document.writerEpoch),
          BigInt(manualAttached.session.document.writerEpoch) + 1n,
        );
      },
    );

    await t.test(
      "writer detach composition resolves claim and finalize acknowledgement loss without duplicate provider work",
      async () => {
        const claimLossSessionId = randomUUID();
        sessionIds.push(claimLossSessionId);
        const claimLossRegistered = await authority.registerSession(
          registrationInput(claimLossSessionId),
        );
        const claimLossAttached = await attachWriter(
          authority,
          claimLossRegistered,
        );
        let claimLossProviderCalls = 0;
        const claimLossBackend = writerDetachIntegrationBackend({
          async detachAttachment(request) {
            claimLossProviderCalls += 1;
            return detachEvidence(request);
          },
          async forceFence(request) {
            return forceFenceEvidence(request);
          },
        });
        const claimLossAuthority = new PostgresSessionAuthority({
          store: new PostgresSerializableStore({
            dedicatedPool: commitAcknowledgementLossAfterQueryPool(
              pool,
              "writer detach claim",
              (text) =>
                text.includes(
                  "SET state = 'starting', revision = revision + 1",
                ),
            ),
          }),
        });
        const claimLossComposition =
          createPostgresWriterDetachComposition({
            authority: claimLossAuthority,
            operationGuard,
            storageBackend: claimLossBackend,
          });

        const claimLossTerminal =
          await claimLossComposition.detachWriter(
            writerDetachCompositionRequest(claimLossAttached.session),
          );
        assert.equal(claimLossProviderCalls, 0);
        assert.equal(
          claimLossTerminal.operation.result.outcome,
          "writer-blocked",
        );
        assert.equal(
          claimLossTerminal.operation.result.reason,
          "provider-outcome-unresolved",
        );

        const finalizeLossSessionId = randomUUID();
        sessionIds.push(finalizeLossSessionId);
        const finalizeLossRegistered = await authority.registerSession(
          registrationInput(finalizeLossSessionId),
        );
        const finalizeLossAttached = await attachWriter(
          authority,
          finalizeLossRegistered,
        );
        let finalizeLossProviderCalls = 0;
        const finalizeLossBackend = writerDetachIntegrationBackend({
          async detachAttachment(request) {
            finalizeLossProviderCalls += 1;
            return detachEvidence(request);
          },
          async forceFence(request) {
            return forceFenceEvidence(request);
          },
        });
        const finalizeLossPool = commitAcknowledgementLossAfterQueryPool(
          pool,
          "writer detach finalize",
          (text) =>
            text.includes(
              "SET state = 'committed', result = $3::jsonb",
            ),
        );
        const finalizeLossAuthority = new PostgresSessionAuthority({
          store: new PostgresSerializableStore({
            dedicatedPool: finalizeLossPool,
          }),
        });
        const finalizeLossComposition =
          createPostgresWriterDetachComposition({
            authority: finalizeLossAuthority,
            operationGuard,
            storageBackend: finalizeLossBackend,
          });

        const finalizeLossTerminal =
          await finalizeLossComposition.detachWriter(
            writerDetachCompositionRequest(
              finalizeLossAttached.session,
            ),
        );
        assert.equal(finalizeLossProviderCalls, 1);
        assert.equal(finalizeLossPool.didLoseAcknowledgement(), true);
        assert.equal(
          finalizeLossTerminal.operation.result.outcome,
          "writer-released",
        );
        assert.equal(
          finalizeLossTerminal.session.document.lifecycle,
          "DETACHED",
        );
      },
    );

    await t.test(
      "writer detach composition serializes one real-provider invocation per operation",
      async () => {
        const sessionId = randomUUID();
        sessionIds.push(sessionId);
        const registered = await authority.registerSession(
          registrationInput(sessionId),
        );
        const attached = await attachWriter(authority, registered);
        const entered = deferred();
        const releaseProvider = deferred();
        let providerCalls = 0;
        const backend = writerDetachIntegrationBackend({
          async detachAttachment(request) {
            providerCalls += 1;
            entered.resolve();
            await releaseProvider.promise;
            return detachEvidence(request);
          },
          async forceFence(request) {
            return forceFenceEvidence(request);
          },
        });
        const composition = createPostgresWriterDetachComposition({
          authority,
          operationGuard,
          storageBackend: backend,
        });
        const request = writerDetachCompositionRequest(
          attached.session,
        );
        const first = composition.detachWriter(request);
        let terminal;
        let firstFailure;
        try {
          await settleWithin(
            entered.promise,
            "writer detach provider entry",
          );
          await assert.rejects(
            settleWithin(
              composition.detachWriter(structuredClone(request)),
              "writer detach competing invocation",
            ),
            assertWriterDetachCompositionCode(
              "postgres_writer_detach_composition_outcome_uncertain",
            ),
          );
          assert.equal(providerCalls, 1);
        } finally {
          releaseProvider.resolve();
          try {
            terminal = await settleWithin(
              first,
              "writer detach primary invocation",
            );
          } catch (error) {
            firstFailure = error;
          }
        }
        if (firstFailure !== undefined) throw firstFailure;
        assert.equal(
          terminal.operation.result.outcome,
          "writer-released",
        );

        const replayed = await composition.detachWriter(
          structuredClone(request),
        );
        assert.deepEqual(replayed, terminal);
        assert.equal(providerCalls, 1);
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

    await t.test(
      "bounded checkpoint recovery paginates durable candidates and retries a busy guard",
      async () => {
        const orderedSessionIds = Array.from(
          { length: 4 },
          () => randomUUID(),
        ).sort();
        sessionIds.push(...orderedSessionIds);
        const attached = [];
        for (const sessionId of orderedSessionIds) {
          const registered = await authority.registerSession(
            registrationInput(sessionId),
          );
          attached.push(
            await attachWriter(authority, registered, {
              leaseDurationMilliseconds: 300_000,
            }),
          );
        }

        const startingAdmission = checkpointCaptureAdmission(
          attached[0],
        );
        const preparedAdmission = checkpointCaptureAdmission(
          attached[1],
        );
        const committedAdmission = checkpointCaptureAdmission(
          attached[2],
        );
        const uncertainAdmission = checkpointCaptureAdmission(
          attached[3],
        );
        const startingInput = checkpointOperationInput(
          attached[0].session,
          startingAdmission,
        );
        const preparedInput = checkpointOperationInput(
          attached[1].session,
          preparedAdmission,
        );
        const uncertainInput = checkpointOperationInput(
          attached[3].session,
          uncertainAdmission,
        );

        await authority.reserveOperation(startingInput);
        const starting =
          await authority.claimCheckpointCaptureDispatch({
            ...structuredClone(startingInput),
            expectedOperationRevision: "0",
          });
        assertOperationReceipt(starting, "starting");

        const prepared =
          await authority.reserveOperation(preparedInput);
        assertOperationReceipt(prepared, "prepared");

        const committedCompletion =
          await checkpointAuthority.runCapture(
            committedAdmission,
            async (context) => checkpointCompletion(context, false),
          );
        assert.equal(committedCompletion.replayed, false);

        await authority.reserveOperation(uncertainInput);
        const uncertainStarting =
          await authority.claimCheckpointCaptureDispatch({
            ...structuredClone(uncertainInput),
            expectedOperationRevision: "0",
          });
        assertOperationReceipt(uncertainStarting, "starting");
        const uncertain = await authority.markOperationUncertain({
          ...structuredClone(uncertainInput),
          expectedOperationRevision: "1",
        });
        assertOperationReceipt(uncertain, "uncertain");

        const durableStates = await pool.query(
          [
            "SELECT operation_id, state",
            "FROM session_authority.operation_claims",
            "WHERE operation_id = ANY($1::varchar[])",
          ].join(" "),
          [
            [
              startingAdmission.request.operationId,
              preparedAdmission.request.operationId,
              committedAdmission.request.operationId,
              uncertainAdmission.request.operationId,
            ],
          ],
        );
        assert.deepEqual(
          Object.fromEntries(
            durableStates.rows.map((row) => [
              row.operation_id,
              row.state,
            ]),
          ),
          {
            [startingAdmission.request.operationId]: "starting",
            [preparedAdmission.request.operationId]: "prepared",
            [committedAdmission.request.operationId]: "committed",
            [uncertainAdmission.request.operationId]: "uncertain",
          },
        );

        const firstPage =
          await authority.listCheckpointCaptureRecoveryCandidates({
            afterSessionId: null,
            limit: 1,
          });
        assert.deepEqual(firstPage, {
          candidates: [
            {
              checkpoint:
                startingInput.request.admission.checkpoint,
              request: startingInput.request.admission.request,
              state: "starting",
            },
          ],
          nextAfterSessionId: orderedSessionIds[0],
        });
        assert.equal(Object.isFrozen(firstPage), true);
        assert.equal(Object.isFrozen(firstPage.candidates), true);
        assert.equal(Object.isFrozen(firstPage.candidates[0]), true);
        for (const canonicalValue of [
          firstPage.candidates[0].checkpoint,
          firstPage.candidates[0].request,
          firstPage.candidates[0].request.target,
        ]) {
          assert.equal(Object.getPrototypeOf(canonicalValue), null);
        }

        const secondPage =
          await authority.listCheckpointCaptureRecoveryCandidates({
            afterSessionId: firstPage.nextAfterSessionId,
            limit: 1,
          });
        assert.deepEqual(secondPage, {
          candidates: [
            {
              checkpoint:
                uncertainInput.request.admission.checkpoint,
              request: uncertainInput.request.admission.request,
              state: "uncertain",
            },
          ],
          nextAfterSessionId: null,
        });

        const attemptedOperationIds = [];
        const verifiedOperationIds = [];
        let activeReconciliations = 0;
        let maximumActiveReconciliations = 0;
        const recoveryService =
          createPostgresCheckpointRecoveryService({
            async listCandidates(input) {
              return authority.listCheckpointCaptureRecoveryCandidates(
                input,
              );
            },
            async reconcileCheckpointCapture(candidate) {
              attemptedOperationIds.push(
                candidate.request.operationId,
              );
              activeReconciliations += 1;
              maximumActiveReconciliations = Math.max(
                maximumActiveReconciliations,
                activeReconciliations,
              );
              try {
                return await checkpointAuthority.runCaptureReconciliation(
                  candidate,
                  async (context) => {
                    verifiedOperationIds.push(
                      candidate.request.operationId,
                    );
                    assert.deepEqual(Reflect.ownKeys(context), [
                      "artifactDirectory",
                      "artifactOwnedRoot",
                      "captureAttempt",
                    ]);
                    assert.equal(
                      Object.hasOwn(context, "sourceDirectory"),
                      false,
                    );
                    assert.equal(
                      Object.hasOwn(context, "canonicalAttachment"),
                      false,
                    );
                    return checkpointCompletion(context, true);
                  },
                );
              } finally {
                activeReconciliations -= 1;
              }
            },
          });

        const guardEntered = deferred();
        const releaseGuard = deferred();
        const heldGuard = operationGuard.runExclusive(
          startingAdmission.request.operationId,
          async (probe, complete) => {
            await probe.assertHeld();
            guardEntered.resolve();
            await releaseGuard.promise;
            return complete(undefined);
          },
        );
        await guardEntered.promise;

        let firstBatch;
        try {
          firstBatch = await recoveryService.runBatch({
            afterSessionId: null,
            limit: 2,
            signal: null,
          });
        } finally {
          releaseGuard.resolve();
          await heldGuard;
        }
        assert.deepEqual(structuredClone(firstBatch), {
          nextAfterSessionId: null,
          results: [
            {
              operationId: startingAdmission.request.operationId,
              sessionId: orderedSessionIds[0],
              status: "pending",
            },
            {
              operationId: uncertainAdmission.request.operationId,
              sessionId: orderedSessionIds[3],
              status: "reconciled",
            },
          ],
          status: "sweep-complete",
        });
        assert.equal(Object.getPrototypeOf(firstBatch), null);
        assert.equal(Object.isFrozen(firstBatch), true);
        assert.equal(Object.isFrozen(firstBatch.results), true);
        for (const result of firstBatch.results) {
          assert.equal(Object.getPrototypeOf(result), null);
          assert.equal(Object.isFrozen(result), true);
        }
        assert.deepEqual(attemptedOperationIds, [
          startingAdmission.request.operationId,
          uncertainAdmission.request.operationId,
        ]);
        assert.deepEqual(verifiedOperationIds, [
          uncertainAdmission.request.operationId,
        ]);
        assert.equal(maximumActiveReconciliations, 1);

        const stillStarting = await authority.reconcileOperation(
          startingInput,
        );
        assertOperationReceipt(stillStarting, "starting");
        const uncertainTerminal = await authority.reconcileOperation(
          uncertainInput,
        );
        assertOperationReceipt(uncertainTerminal, "committed");
        assert.equal(
          uncertainTerminal.operation.result.outcome,
          "checkpoint-captured",
        );

        attemptedOperationIds.length = 0;
        verifiedOperationIds.length = 0;
        const retryBatch = await recoveryService.runBatch({
          afterSessionId: null,
          limit: 2,
          signal: null,
        });
        assert.deepEqual(structuredClone(retryBatch), {
          nextAfterSessionId: null,
          results: [
            {
              operationId: startingAdmission.request.operationId,
              sessionId: orderedSessionIds[0],
              status: "reconciled",
            },
          ],
          status: "sweep-complete",
        });
        assert.equal(Object.getPrototypeOf(retryBatch), null);
        assert.equal(Object.isFrozen(retryBatch), true);
        assert.equal(Object.isFrozen(retryBatch.results), true);
        for (const result of retryBatch.results) {
          assert.equal(Object.getPrototypeOf(result), null);
          assert.equal(Object.isFrozen(result), true);
        }
        assert.deepEqual(attemptedOperationIds, [
          startingAdmission.request.operationId,
        ]);
        assert.deepEqual(verifiedOperationIds, [
          startingAdmission.request.operationId,
        ]);
        assert.equal(maximumActiveReconciliations, 1);

        const startingTerminal = await authority.reconcileOperation(
          startingInput,
        );
        assertOperationReceipt(startingTerminal, "committed");
        assert.equal(
          startingTerminal.operation.result.outcome,
          "checkpoint-captured",
        );
        const exhausted =
          await authority.listCheckpointCaptureRecoveryCandidates({
            afterSessionId: null,
            limit: 2,
          });
        assert.deepEqual(exhausted, {
          candidates: [],
          nextAfterSessionId: null,
        });
      },
    );

    await t.test(
      "checkpoint capture holds an independent guard and commits one atomic terminal result",
      async () => {
        const sessionId = randomUUID();
        sessionIds.push(sessionId);
        const registered = await authority.registerSession(
          registrationInput(sessionId),
        );
        const attached = await attachWriter(authority, registered, {
          leaseDurationMilliseconds: 300_000,
        });
        const admission = checkpointCaptureAdmission(attached);
        const publicationEntered = deferred();
        const releasePublication = deferred();
        let publicationCount = 0;
        let competingPublicationCount = 0;
        let publishedCompletion;

        const capture = checkpointAuthority.runCapture(
          admission,
          async (context) => {
            publicationCount += 1;
            publicationEntered.resolve();
            assert.deepEqual(Reflect.ownKeys(context), [
              "artifactDirectory",
              "artifactOwnedRoot",
              "canonicalAttachment",
              "canonicalLease",
              "captureAttemptId",
              "now",
              "reservationId",
              "result",
              "sourceDirectory",
              "sourceOwnedRoot",
              "storageRef",
            ]);
            assert.equal(
              context.sourceDirectory,
              attached.session.document.attachment.rootPath,
            );
            assert.equal(
              context.sourceOwnedRoot,
              "/var/lib/portable-codex",
            );
            assert.equal(
              context.artifactDirectory,
              `/var/lib/portable-codex-checkpoints/${admission.checkpoint.artifactId}`,
            );
            assert.equal(
              context.captureAttemptId,
              admission.captureAttemptId,
            );
            assert.equal(Number.isFinite(context.now), true);

            const guardState = await pool.query(
              [
                "SELECT count(*)::integer AS lock_count,",
                "count(*) FILTER (WHERE a.xact_start IS NOT NULL)::integer",
                "AS transaction_lock_count",
                "FROM pg_catalog.pg_locks l",
                "JOIN pg_catalog.pg_stat_activity a ON a.pid = l.pid",
                "WHERE l.locktype = 'advisory' AND l.granted",
                "AND a.application_name = $1",
              ].join(" "),
              [CHECKPOINT_GUARD_APPLICATION_NAME],
            );
            assert.deepEqual(guardState.rows[0], {
              lock_count: 1,
              transaction_lock_count: 0,
            });
            const transactionState = await pool.query(
              [
                "SELECT count(*)::integer AS idle_transaction_count",
                "FROM pg_catalog.pg_stat_activity",
                "WHERE application_name = $1",
                "AND state LIKE 'idle in transaction%'",
              ].join(" "),
              [SESSION_AUTHORITY_APPLICATION_NAME],
            );
            assert.equal(
              transactionState.rows[0].idle_transaction_count,
              0,
            );

            await releasePublication.promise;
            publishedCompletion = checkpointCompletion(context, false);
            return publishedCompletion;
          },
        );

        await publicationEntered.promise;
        try {
          await assert.rejects(
            checkpointAuthority.runCapture(
              structuredClone(admission),
              async () => {
                competingPublicationCount += 1;
              },
            ),
            assertCheckpointAuthorityCode(
              "postgres_checkpoint_mutation_authority_outcome_uncertain",
            ),
          );
        } finally {
          releasePublication.resolve();
        }

        const captured = await capture;
        assert.strictEqual(captured, publishedCompletion);
        assert.equal(publicationCount, 1);
        assert.equal(competingPublicationCount, 0);

        const terminal = await authority.reconcileOperation(
          checkpointOperationInput(attached.session, admission),
        );
        assertOperationReceipt(terminal, "committed");
        assert.equal(
          terminal.operation.result.outcome,
          "checkpoint-captured",
        );
        assert.equal(
          terminal.session.document.lifecycle,
          "ATTACHED",
        );
        assert.equal(terminal.session.document.activeOperation, null);

        const catalogue = await authority.readCheckpointCatalogue({
          checkpoint: admission.checkpoint,
        });
        assert.equal(catalogue.attempt.state, "committed");
        assert.equal(
          catalogue.catalogue.captureAttemptId,
          admission.captureAttemptId,
        );
        assert.deepEqual(catalogue.catalogue.document, {
          artifactProof: publishedCompletion.artifactProof,
          contractVersion: 1,
          materialization: publishedCompletion.materialization,
          result: publishedCompletion.result,
        });
        assert.deepEqual(catalogue.operation, terminal.operation);

        const atomicState = await pool.query(
          [
            "SELECT o.state AS operation_state,",
            "o.revision::text AS operation_revision,",
            "o.result->>'outcome' AS operation_outcome,",
            "r.state AS reservation_state,",
            "(o.retired_at = r.released_at",
            "AND o.retired_at = c.committed_at",
            "AND o.retired_at = s.updated_at) AS terminal_times_match,",
            "(s.document->'activeOperation' = 'null'::jsonb)",
            "AS active_operation_cleared,",
            "s.document->'lastOperation'->>'operationId'",
            "AS terminal_operation_id,",
            "a.capture_attempt_id::text AS capture_attempt_id,",
            "c.checkpoint_id AS checkpoint_id",
            "FROM session_authority.operation_claims o",
            "JOIN session_authority.reservations r",
            "ON r.operation_id = o.operation_id",
            "JOIN session_authority.capture_attempt_claims a",
            "ON a.operation_id = o.operation_id",
            "JOIN session_authority.checkpoint_catalogue c",
            "ON c.capture_attempt_id = a.capture_attempt_id",
            "JOIN session_authority.sessions s",
            "ON s.session_id = o.session_id",
            "WHERE o.operation_id = $1",
          ].join(" "),
          [admission.request.operationId],
        );
        assert.deepEqual(atomicState.rows, [
          {
            active_operation_cleared: true,
            capture_attempt_id: admission.captureAttemptId,
            checkpoint_id: admission.checkpoint.checkpointId,
            operation_outcome: "checkpoint-captured",
            operation_revision: "2",
            operation_state: "committed",
            reservation_state: "released",
            terminal_operation_id: admission.request.operationId,
            terminal_times_match: true,
          },
        ]);

        let verificationCount = 0;
        let verifiedCompletion;
        const reconciled =
          await checkpointAuthority.runCaptureReconciliation(
            {
              checkpoint: admission.checkpoint,
              request: admission.request,
            },
            async (context) => {
              verificationCount += 1;
              assert.deepEqual(Reflect.ownKeys(context), [
                "artifactDirectory",
                "artifactOwnedRoot",
                "captureAttempt",
              ]);
              assert.equal(
                Object.hasOwn(context, "sourceDirectory"),
                false,
              );
              assert.equal(context.captureAttempt.state, "committed");
              verifiedCompletion = checkpointCompletion(context, true);
              return verifiedCompletion;
            },
          );
        assert.strictEqual(reconciled, verifiedCompletion);
        assert.equal(verificationCount, 1);
        assert.equal(publicationCount, 1);

        const releasedGuard = await pool.query(
          [
            "SELECT count(*)::integer AS lock_count",
            "FROM pg_catalog.pg_locks l",
            "JOIN pg_catalog.pg_stat_activity a ON a.pid = l.pid",
            "WHERE l.locktype = 'advisory' AND l.granted",
            "AND a.application_name = $1",
          ].join(" "),
          [CHECKPOINT_GUARD_APPLICATION_NAME],
        );
        assert.equal(releasedGuard.rows[0].lock_count, 0);
      },
    );

    await t.test(
      "checkpoint finalize acknowledgement loss recovers by source-free verification",
      async () => {
        const sessionId = randomUUID();
        sessionIds.push(sessionId);
        const registered = await authority.registerSession(
          registrationInput(sessionId),
        );
        const attached = await attachWriter(authority, registered, {
          leaseDurationMilliseconds: 300_000,
        });
        const admission = checkpointCaptureAdmission(attached);
        const acknowledgementLossStore =
          new PostgresSerializableStore({
            dedicatedPool: commitAcknowledgementLossAfterQueryPool(
              pool,
              "checkpoint finalize",
              (text) =>
                text.startsWith(
                  "INSERT INTO session_authority.checkpoint_catalogue",
                ),
            ),
            maxTransactionAttempts: 2,
          });
        const acknowledgementLossAuthority =
          new PostgresSessionAuthority({
            store: acknowledgementLossStore,
          });
        const acknowledgementLossCheckpointAuthority =
          createPostgresCheckpointMutationAuthority({
            authority: acknowledgementLossAuthority,
            operationGuard,
            resolveArtifactPaths: integrationArtifactPaths,
            resolveSourceOwnedRoot: integrationSourceOwnedRoot,
          });
        let publicationCount = 0;
        let publishedCompletion;

        await assert.rejects(
          acknowledgementLossCheckpointAuthority.runCapture(
            admission,
            async (context) => {
              publicationCount += 1;
              publishedCompletion = checkpointCompletion(
                context,
                false,
              );
              return publishedCompletion;
            },
          ),
          assertCheckpointAuthorityCode(
            "postgres_checkpoint_mutation_authority_outcome_uncertain",
          ),
        );
        assert.equal(publicationCount, 1);

        const committed = await authority.readCheckpointCatalogue({
          checkpoint: admission.checkpoint,
        });
        assert.equal(committed.attempt.state, "committed");
        assert.equal(committed.operation.state, "committed");
        assert.deepEqual(committed.catalogue.document, {
          artifactProof: publishedCompletion.artifactProof,
          contractVersion: 1,
          materialization: publishedCompletion.materialization,
          result: publishedCompletion.result,
        });

        let verificationCount = 0;
        let verifiedCompletion;
        const recovered =
          await checkpointAuthority.runCaptureReconciliation(
            {
              checkpoint: admission.checkpoint,
              request: admission.request,
            },
            async (context) => {
              verificationCount += 1;
              assert.equal(
                Object.hasOwn(context, "canonicalAttachment"),
                false,
              );
              assert.equal(
                Object.hasOwn(context, "sourceDirectory"),
                false,
              );
              assert.equal(context.captureAttempt.state, "committed");
              verifiedCompletion = checkpointCompletion(context, true);
              return verifiedCompletion;
            },
          );
        assert.strictEqual(recovered, verifiedCompletion);
        assert.equal(verificationCount, 1);
        assert.equal(publicationCount, 1);

        const terminal = await authority.reconcileOperation(
          checkpointOperationInput(attached.session, admission),
        );
        assertOperationReceipt(terminal, "committed");
        assert.equal(terminal.operation.revision, "2");
        assert.deepEqual(terminal.operation, committed.operation);
      },
    );

    await t.test(
      "restore destination generation mutations roll back with their operation state",
      async () => {
        const sessionId = randomUUID();
        sessionIds.push(sessionId);
        const fixture = await prepareRestoreGenerationFixture(
          authority,
          checkpointAuthority,
          sessionId,
        );
        const admission = restoreGenerationAdmission(
          fixture.attached,
          fixture.checkpoint,
        );
        const input = restoreGenerationOperationInput(
          fixture.attached.session,
          admission,
        );
        const reserved = await authority.reserveOperation(input);
        assertOperationReceipt(reserved, "prepared");
        const generationId = `restore-generation-${randomUUID()}`;
        const claimInput = {
          ...structuredClone(input),
          destinationIsolationProofId:
            `restore-isolation-proof-${randomUUID()}`,
          expectedOperationRevision: "0",
          generationId,
        };

        const observer = await pool.connect();
        try {
          const preparedState =
            await readRestoreGenerationTransactionState(
              observer,
              input.operationId,
            );
          assert.equal(preparedState.generation_id, null);
          assert.equal(preparedState.operation_state, "prepared");
          assert.equal(preparedState.operation_revision, "0");
          assert.equal(preparedState.operation_result, null);
          assert.equal(preparedState.operation_retired_at, null);
          assert.equal(preparedState.reservation_state, "prepared");
          assert.equal(preparedState.reservation_released_at, null);
          assert.equal(
            preparedState.session_revision,
            reserved.session.revision,
          );
          assert.deepEqual(
            preparedState.session_document,
            reserved.session.document,
          );

          const claimRollbackAuthority =
            new PostgresSessionAuthority({
              store: new PostgresSerializableStore({
                dedicatedPool: firstMatchingQueryResultFailurePool(
                  pool,
                  "restore generation claim rollback",
                  (text) =>
                    text.startsWith(
                      "UPDATE session_authority.sessions",
                    ),
                ),
                maxTransactionAttempts: 1,
              }),
            });
          await assert.rejects(
            claimRollbackAuthority
              .claimRestoreDestinationGenerationDispatch(claimInput),
            assertTransactionBoundaryLost,
          );
          assert.deepEqual(
            await readRestoreGenerationTransactionState(
              observer,
              input.operationId,
            ),
            preparedState,
          );

          const claimed =
            await authority.claimRestoreDestinationGenerationDispatch(
              structuredClone(claimInput),
            );
          assertOperationReceipt(claimed, "starting");
          assert.equal(claimed.dispatchGranted, true);
          const authorizedState =
            await readRestoreGenerationTransactionState(
              observer,
              input.operationId,
            );
          assert.equal(authorizedState.generation_id, generationId);
          assert.equal(
            authorizedState.generation_state,
            "authorized",
          );
          assert.deepEqual(
            authorizedState.generation_binding,
            structuredClone(claimed.generation.binding),
          );
          assert.equal(authorizedState.generation_document, null);
          assert.equal(authorizedState.generation_committed_at, null);
          assert.equal(authorizedState.operation_state, "starting");
          assert.equal(authorizedState.operation_revision, "1");
          assert.equal(authorizedState.operation_result, null);
          assert.equal(authorizedState.operation_retired_at, null);
          assert.equal(authorizedState.reservation_state, "starting");
          assert.equal(authorizedState.reservation_released_at, null);
          assert.equal(
            authorizedState.session_revision,
            claimed.session.revision,
          );
          assert.deepEqual(
            authorizedState.session_document,
            claimed.session.document,
          );

          const completion = restoreGenerationCompletion(
            input,
            claimed,
            false,
          );
          const finalization = {
            ...structuredClone(input),
            completion,
            expectedOperationRevision: "1",
          };
          const finalizeRollbackAuthority =
            new PostgresSessionAuthority({
              store: new PostgresSerializableStore({
                dedicatedPool: firstMatchingQueryResultFailurePool(
                  pool,
                  "restore generation finalize rollback",
                  (text) =>
                    text.startsWith(
                      "UPDATE session_authority.sessions",
                    ),
                ),
                maxTransactionAttempts: 1,
              }),
            });
          await assert.rejects(
            finalizeRollbackAuthority
              .finalizeRestoreDestinationGeneration(finalization),
            assertTransactionBoundaryLost,
          );
          assert.deepEqual(
            await readRestoreGenerationTransactionState(
              observer,
              input.operationId,
            ),
            authorizedState,
          );
        } finally {
          observer.release();
        }
      },
    );

    await t.test(
      "restore destination generations preserve typed authority across replay and acknowledgement loss",
      async () => {
        const startingSessionId = randomUUID();
        const uncertainSessionId = randomUUID();
        sessionIds.push(startingSessionId, uncertainSessionId);
        const startingFixture =
          await prepareRestoreGenerationFixture(
            authority,
            checkpointAuthority,
            startingSessionId,
          );
        const startingAdmission = restoreGenerationAdmission(
          startingFixture.attached,
          startingFixture.checkpoint,
        );
        const startingInput = restoreGenerationOperationInput(
          startingFixture.attached.session,
          startingAdmission,
        );
        assert.equal(startingInput.request.contractVersion, 1);
        assert.equal(
          Object.hasOwn(startingInput.request, "launchIntent"),
          false,
        );
        assert.deepEqual(
          Reflect.ownKeys(startingInput.request.admission),
          ["checkpoint", "request"],
        );
        assert.deepEqual(
          structuredClone(startingInput.request.admission),
          startingAdmission,
        );

        const startingReserved =
          await authority.reserveOperation(startingInput);
        assertOperationReceipt(startingReserved, "prepared");
        const startingGenerationId =
          `restore-generation-${randomUUID()}`;
        const startingIsolationProofId =
          `restore-isolation-proof-${randomUUID()}`;
        const startingClaimInput = {
          ...structuredClone(startingInput),
          destinationIsolationProofId: startingIsolationProofId,
          expectedOperationRevision: "0",
          generationId: startingGenerationId,
        };
        const startingClaim =
          await authority.claimRestoreDestinationGenerationDispatch(
            startingClaimInput,
          );
        assertOperationReceipt(startingClaim, "starting");
        assert.equal(startingClaim.dispatchGranted, true);
        assert.equal(startingClaim.generation.state, "authorized");
        assert.equal(
          startingClaim.generation.generationId,
          startingGenerationId,
        );
        assert.equal(
          startingClaim.generation.binding.destinationIsolationProofId,
          startingIsolationProofId,
        );
        assert.equal(startingClaim.generation.document, null);

        const authorizedRow = await pool.query(
          [
            "SELECT g.generation_id, g.operation_id,",
            "g.session_id::text AS session_id, g.checkpoint_id,",
            "g.state, g.binding->>'destinationIsolationProofId'",
            "AS destination_isolation_proof_id,",
            "g.document, g.committed_at,",
            "o.state AS operation_state,",
            "o.revision::text AS operation_revision,",
            "r.state AS reservation_state",
            "FROM session_authority.restore_destination_generations g",
            "JOIN session_authority.operation_claims o",
            "ON o.operation_id = g.operation_id",
            "JOIN session_authority.reservations r",
            "ON r.operation_id = g.operation_id",
            "WHERE g.generation_id = $1",
          ].join(" "),
          [startingGenerationId],
        );
        assert.deepEqual(authorizedRow.rows, [
          {
            checkpoint_id: startingFixture.checkpoint.checkpointId,
            committed_at: null,
            destination_isolation_proof_id:
              startingIsolationProofId,
            document: null,
            generation_id: startingGenerationId,
            operation_id: startingInput.operationId,
            operation_revision: "1",
            operation_state: "starting",
            reservation_state: "starting",
            session_id: startingSessionId,
            state: "authorized",
          },
        ]);

        const startingCompletion = restoreGenerationCompletion(
          startingInput,
          startingClaim,
          false,
        );
        const startingFinalization = {
          ...structuredClone(startingInput),
          completion: startingCompletion,
          expectedOperationRevision: "1",
        };
        const startingFinalized =
          await authority.finalizeRestoreDestinationGeneration(
            startingFinalization,
          );
        assertOperationReceipt(startingFinalized, "committed");
        assert.equal(startingFinalized.finalized, true);
        assert.equal(startingFinalized.operation.revision, "2");
        assert.equal(
          startingFinalized.operation.result.outcome,
          "restore-generation-committed",
        );
        assert.equal(startingFinalized.generation.state, "committed");
        assert.deepEqual(
          startingFinalized.generation.document.result,
          startingInput.request.predeterminedResult,
        );

        const startingReadInput = {
          checkpoint: structuredClone(startingAdmission.checkpoint),
          generationId: startingGenerationId,
          request: structuredClone(startingAdmission.request),
        };
        const startingRead =
          await authority.readRestoreDestinationGeneration(
            startingReadInput,
          );
        assertOperationReceipt(startingRead, "committed");
        assert.equal(startingRead.status, "committed");
        assert.deepEqual(
          startingRead.generation,
          startingFinalized.generation,
        );

        const startingClaimReplay =
          await authority.claimRestoreDestinationGenerationDispatch(
            structuredClone(startingClaimInput),
          );
        assertOperationReceipt(startingClaimReplay, "committed");
        assert.equal(startingClaimReplay.dispatchGranted, false);
        assert.deepEqual(
          startingClaimReplay.generation,
          startingFinalized.generation,
        );
        const startingFinalizeReplay =
          await authority.finalizeRestoreDestinationGeneration(
            structuredClone(startingFinalization),
          );
        assertOperationReceipt(startingFinalizeReplay, "committed");
        assert.equal(startingFinalizeReplay.finalized, false);
        assert.deepEqual(
          startingFinalizeReplay.generation,
          startingFinalized.generation,
        );
        assert.deepEqual(
          await authority.readRestoreDestinationGeneration(
            structuredClone(startingReadInput),
          ),
          startingRead,
        );

        const wrongCheckpoint = {
          ...structuredClone(startingAdmission.checkpoint),
          artifactId: `wrong-artifact-${randomUUID()}`,
          checkpointId: `wrong-checkpoint-${randomUUID()}`,
        };
        const wrongRequest = {
          ...structuredClone(startingAdmission.request),
          target: {
            artifactId: wrongCheckpoint.artifactId,
            checkpointId: wrongCheckpoint.checkpointId,
            kind: "checkpoint",
          },
        };
        await assert.rejects(
          authority.readRestoreDestinationGeneration({
            checkpoint: wrongCheckpoint,
            generationId: startingGenerationId,
            request: wrongRequest,
          }),
          assertAuthorityCode("restore_generation_not_authorized"),
        );

        const collisionAdmission = restoreGenerationAdmission(
          { session: startingFinalized.session },
          startingFixture.checkpoint,
        );
        const collisionInput = restoreGenerationOperationInput(
          startingFinalized.session,
          collisionAdmission,
        );
        await authority.reserveOperation(collisionInput);
        await assert.rejects(
          authority.claimRestoreDestinationGenerationDispatch({
            ...structuredClone(collisionInput),
            destinationIsolationProofId:
              `restore-isolation-proof-${randomUUID()}`,
            expectedOperationRevision: "0",
            generationId: startingGenerationId,
          }),
          assertAuthorityCode("restore_generation_identity_conflict"),
        );
        const collisionPrepared =
          await authority.reconcileOperation(collisionInput);
        assertOperationReceipt(collisionPrepared, "prepared");

        const uncertainFixture =
          await prepareRestoreGenerationFixture(
            authority,
            checkpointAuthority,
            uncertainSessionId,
          );
        const uncertainAdmission = restoreGenerationAdmission(
          uncertainFixture.attached,
          uncertainFixture.checkpoint,
        );
        const uncertainInput = restoreGenerationOperationInput(
          uncertainFixture.attached.session,
          uncertainAdmission,
        );
        await authority.reserveOperation(uncertainInput);
        const uncertainGenerationId =
          `restore-generation-${randomUUID()}`;
        const uncertainClaimInput = {
          ...structuredClone(uncertainInput),
          destinationIsolationProofId:
            `restore-isolation-proof-${randomUUID()}`,
          expectedOperationRevision: "0",
          generationId: uncertainGenerationId,
        };
        const claimLossAuthority = new PostgresSessionAuthority({
          store: new PostgresSerializableStore({
            dedicatedPool: commitAcknowledgementLossAfterQueryPool(
              pool,
              "restore generation claim",
              (text) =>
                text.startsWith(
                  "INSERT INTO session_authority.restore_destination_generations",
                ),
            ),
            maxTransactionAttempts: 2,
          }),
        });
        await assert.rejects(
          claimLossAuthority.claimRestoreDestinationGenerationDispatch(
            uncertainClaimInput,
          ),
          assertCommitOutcomeUncertain,
        );
        const claimLossReconciled =
          await authority.reconcileOperation(uncertainInput);
        assertOperationReceipt(claimLossReconciled, "starting");
        const claimReplay =
          await authority.claimRestoreDestinationGenerationDispatch(
            structuredClone(uncertainClaimInput),
          );
        assertOperationReceipt(claimReplay, "starting");
        assert.equal(claimReplay.dispatchGranted, false);
        assert.equal(claimReplay.generation.state, "authorized");
        assert.equal(
          claimReplay.generation.generationId,
          uncertainGenerationId,
        );
        const authorizedRead =
          await authority.readRestoreDestinationGeneration({
            checkpoint: uncertainAdmission.checkpoint,
            generationId: uncertainGenerationId,
            request: uncertainAdmission.request,
          });
        assert.equal(authorizedRead.status, "authorized");
        assert.equal(authorizedRead.operation.state, "starting");
        assert.equal(authorizedRead.reservation.state, "starting");
        assert.deepEqual(
          authorizedRead.generation,
          claimReplay.generation,
        );
        assert.equal(Object.isFrozen(authorizedRead), true);

        const markedUncertain = await authority.markOperationUncertain({
          ...structuredClone(uncertainInput),
          expectedOperationRevision: "1",
        });
        assertOperationReceipt(markedUncertain, "uncertain");
        const uncertainCompletion = restoreGenerationCompletion(
          uncertainInput,
          claimReplay,
          false,
        );
        const uncertainFinalization = {
          ...structuredClone(uncertainInput),
          completion: uncertainCompletion,
          expectedOperationRevision: "2",
        };
        const finalizeLossAuthority = new PostgresSessionAuthority({
          store: new PostgresSerializableStore({
            dedicatedPool: commitAcknowledgementLossAfterQueryPool(
              pool,
              "restore generation finalize",
              (text) =>
                text.startsWith(
                  "UPDATE session_authority.restore_destination_generations",
                ),
            ),
            maxTransactionAttempts: 2,
          }),
        });
        await assert.rejects(
          finalizeLossAuthority.finalizeRestoreDestinationGeneration(
            uncertainFinalization,
          ),
          assertCommitOutcomeUncertain,
        );

        const uncertainReadInput = {
          checkpoint: uncertainAdmission.checkpoint,
          generationId: uncertainGenerationId,
          request: uncertainAdmission.request,
        };
        const finalizeLossRead =
          await authority.readRestoreDestinationGeneration(
            uncertainReadInput,
          );
        assertOperationReceipt(finalizeLossRead, "committed");
        assert.equal(finalizeLossRead.status, "committed");
        assert.equal(
          finalizeLossRead.operation.result.outcome,
          "restore-generation-committed",
        );
        const finalizeLossReplay =
          await authority.finalizeRestoreDestinationGeneration(
            structuredClone(uncertainFinalization),
          );
        assertOperationReceipt(finalizeLossReplay, "committed");
        assert.equal(finalizeLossReplay.finalized, false);
        assert.deepEqual(
          finalizeLossReplay.generation,
          finalizeLossRead.generation,
        );
        assert.deepEqual(
          await authority.readRestoreDestinationGeneration(
            structuredClone(uncertainReadInput),
          ),
          finalizeLossRead,
        );
      },
    );

    await t.test(
      "detached restore activation atomically binds the published object and one prepared launch",
      async () => {
        const sessionId = randomUUID();
        sessionIds.push(sessionId);
        const fixture = await prepareCommittedRestoreGenerationFixture(
          authority,
          checkpointAuthority,
          sessionId,
        );

        const firstLaunchInput = writerLaunchAttemptInput(
          fixture.finalized.session,
          fixture.finalized.generation,
        );
        await authority.reserveOperation(firstLaunchInput);
        await authority.claimWriterLaunchAttemptDispatch({
          ...structuredClone(firstLaunchInput),
          expectedOperationRevision: "0",
        });
        const launched = await authority.finalizeWriterLaunchAttemptStarted({
          ...structuredClone(firstLaunchInput),
          evidence: writerLaunchEvidence(firstLaunchInput, "started"),
          expectedOperationRevision: "1",
        });
        assertOperationReceipt(launched, "committed");

        const stop = writerLaunchStopInput(launched.session);
        await authority.reserveOperation(stop.input);
        await authority.claimWriterLaunchStopDispatch({
          ...structuredClone(stop.input),
          claimToken: stop.claimToken,
          expectedOperationRevision: "0",
        });
        const stopped = await authority.finalizeWriterLaunchStopped({
          ...structuredClone(stop.input),
          evidence: writerLaunchStopEvidence(stop.input),
          expectedOperationRevision: "1",
        });
        assertOperationReceipt(stopped, "committed");
        const released = await releaseWriter(authority, stopped);
        assertOperationReceipt(released, "committed");
        assert.equal(released.session.document.lifecycle, "DETACHED");

        const launchAttemptId = `writer-launch-${randomUUID()}`;
        const launchIntent = {
          launchAttemptId,
          measuredImage: writerLaunchMeasuredImage(released.session),
          supervisor: {
            contractVersion: 1,
            supervisorId: `supervisor-${randomUUID()}`,
          },
        };
        const activationInput = {
          expectedSession: released.session,
          kind: RESTORE_ATTACHMENT_ACTIVATION_OPERATION_KIND,
          operationId: `restore-activation-${randomUUID()}`,
          request: createRestoreAttachmentActivationOperationRequest({
            destinationRootPath:
              `/var/lib/portable-codex/restores/${sessionId}`,
            expectedSession: released.session,
            generation: fixture.finalized.generation,
            holderId: `restore-host-${randomUUID()}`,
            launchIntent,
            leaseDurationMilliseconds: 300_000,
            predecessor: {
              attachmentId:
                stop.input.expectedSession.document.attachment.attachmentId,
              detachOperationId: released.operation.operationId,
              stopOperationId: stopped.operation.operationId,
            },
          }),
        };
        const reserved = await authority.reserveOperation(activationInput);
        assertOperationReceipt(reserved, "prepared");
        const claimed =
          await authority.claimRestoreAttachmentActivationDispatch({
            ...structuredClone(activationInput),
            expectedOperationRevision: "0",
          });
        assertOperationReceipt(claimed, "starting");
        assert.equal(claimed.dispatchGranted, true);
        assert.equal(
          claimed.activationRequest.publication.root.rootPath,
          activationInput.request.destinationRootPath,
        );
        assert.equal(
          claimed.activationRequest.publication.root.objectId,
          fixture.finalized.generation.document.materialization.stagedRoot
            .objectId,
        );

        const candidates =
          await authority.listRestoreAttachmentActivationRecoveryCandidates({
            afterSessionId: null,
            limit: 100,
          });
        const candidate = candidates.candidates.find(
          (value) =>
            value.activationOperationId === activationInput.operationId,
        );
        assert.notEqual(candidate, undefined);
        assert.equal(candidate.state, "starting");
        assert.deepEqual(
          structuredClone(candidate.request),
          structuredClone(activationInput.request),
        );

        const proofId = `restore-attachment-proof-${randomUUID()}`;
        const mutationRequest = claimed.activationRequest.mutationRequest;
        const activationResult = {
          attachment: {
            backendId: mutationRequest.backendId,
            contractVersion: mutationRequest.contractVersion,
            storageId: mutationRequest.storageId,
            sessionId: mutationRequest.sessionId,
            attachmentId: mutationRequest.target.attachmentId,
            leaseId: mutationRequest.leaseId,
            holderId: mutationRequest.holderId,
            fencingEpoch: mutationRequest.fencingEpoch,
            operationId: mutationRequest.operationId,
            proofId,
            kind: "directory",
            rootPath: activationInput.request.destinationRootPath,
            mode: "read-write",
          },
          contractVersion: 1,
          mutationResult: {
            ...structuredClone(mutationRequest),
            proofId,
            status: "attached",
          },
          publication: structuredClone(
            claimed.activationRequest.publication,
          ),
        };
        await assert.rejects(
          authority.finalizeRestoreAttachmentActivationAndReserveWriterLaunchAttempt(
            {
              ...structuredClone(activationInput),
              activationResult: {
                ...structuredClone(activationResult),
                publication: {
                  ...structuredClone(activationResult.publication),
                  root: {
                    ...structuredClone(activationResult.publication.root),
                    objectId: `wrong-object-${randomUUID()}`,
                  },
                },
              },
              expectedOperationRevision: "1",
            },
          ),
          assertAuthorityCode("invalid_operation_request"),
        );
        const uncertain = await authority.markOperationUncertain({
          ...structuredClone(activationInput),
          expectedOperationRevision: "1",
        });
        assertOperationReceipt(uncertain, "uncertain");
        const finalized =
          await authority.finalizeRestoreAttachmentActivationAndReserveWriterLaunchAttempt(
            {
              ...structuredClone(activationInput),
              activationResult,
              expectedOperationRevision: "2",
            },
          );
        assert.equal(finalized.status, "prepared");
        assert.equal(finalized.activation.finalized, true);
        assert.equal(
          finalized.activation.operation.result.outcome,
          "restore-attachment-activated",
        );
        assert.equal(
          finalized.session.document.activeOperation.operationId,
          launchAttemptId,
        );
        assert.equal(
          finalized.session.document.lastOperation.operationId,
          activationInput.operationId,
        );
        assert.deepEqual(
          structuredClone(finalized.session.document.attachment),
          activationResult.attachment,
        );
        assert.equal(
          finalized.launch.attempt.request.generation.generationId,
          fixture.finalized.generation.generationId,
        );

        const replay =
          await authority.finalizeRestoreAttachmentActivationAndReserveWriterLaunchAttempt(
            {
              ...structuredClone(activationInput),
              activationResult,
              expectedOperationRevision: "2",
            },
          );
        assert.equal(replay.activation.finalized, false);
        assert.deepEqual(replay.launch, finalized.launch);
        assert.deepEqual(replay.session, finalized.session);
        const read = await authority.readRestoreAttachmentActivation({
          operationId: activationInput.operationId,
        });
        assertOperationReceipt(read, "committed", {
          activeOperationId: launchAttemptId,
          currentTerminal: false,
        });
        assert.equal(
          read.session.document.lastOperation.operationId,
          activationInput.operationId,
        );
        assert.deepEqual(
          structuredClone(read.activationRequest),
          structuredClone(claimed.activationRequest),
        );

        const activationLaunchInput = {
          expectedSession: finalized.launch.operation.expectedSession,
          kind: WRITER_LAUNCH_ATTEMPT_OPERATION_KIND,
          operationId: launchAttemptId,
          request: finalized.launch.attempt.request,
        };
        const activationLaunchClaimed =
          await authority.claimWriterLaunchAttemptDispatch({
            ...structuredClone(activationLaunchInput),
            expectedOperationRevision: "0",
          });
        assertOperationReceipt(activationLaunchClaimed, "starting");
        const activationLaunched =
          await authority.finalizeWriterLaunchAttemptStarted({
            ...structuredClone(activationLaunchInput),
            evidence: writerLaunchEvidence(
              activationLaunchInput,
              "started",
            ),
            expectedOperationRevision: "1",
          });
        assertOperationReceipt(activationLaunched, "committed");

        const activationStop = writerLaunchStopInput(
          activationLaunched.session,
        );
        await authority.reserveOperation(activationStop.input);
        await authority.claimWriterLaunchStopDispatch({
          ...structuredClone(activationStop.input),
          claimToken: activationStop.claimToken,
          expectedOperationRevision: "0",
        });
        const activationStopped =
          await authority.finalizeWriterLaunchStopped({
            ...structuredClone(activationStop.input),
            evidence: writerLaunchStopEvidence(activationStop.input),
            expectedOperationRevision: "1",
          });
        assertOperationReceipt(activationStopped, "committed");

        // History has advanced beyond the activation operation. The
        // attachment's durable creator operation must still prove that a
        // successor launch uses the exact committed generation publication.
        const successor = writerLaunchAttemptInput(
          activationStopped.session,
          fixture.finalized.generation,
        );
        await authority.reserveOperation(successor);
        const successorClaimed =
          await authority.claimWriterLaunchAttemptDispatch({
            ...structuredClone(successor),
            expectedOperationRevision: "0",
          });
        assertOperationReceipt(successorClaimed, "starting");
      },
    );

    await t.test(
      "capture-bound restore activation accepts a different target generation and atomically prepares its launch",
      async () => {
        const sessionId = randomUUID();
        sessionIds.push(sessionId);
        const targetFixture =
          await prepareCommittedRestoreGenerationFixture(
            authority,
            checkpointAuthority,
            sessionId,
          );
        const targetGeneration = targetFixture.finalized.generation;
        const oldAttachment =
          targetFixture.finalized.session.document.attachment;
        assert.notEqual(oldAttachment, null);
        assert.equal(
          targetGeneration.binding.attachment.attachmentId,
          oldAttachment.attachmentId,
        );

        const currentGenerationAdmission = restoreGenerationAdmission(
          { session: targetFixture.finalized.session },
          targetFixture.checkpoint,
        );
        const currentGenerationInput = restoreGenerationOperationInput(
          targetFixture.finalized.session,
          currentGenerationAdmission,
        );
        await authority.reserveOperation(currentGenerationInput);
        const currentGenerationClaimed =
          await authority.claimRestoreDestinationGenerationDispatch({
            ...structuredClone(currentGenerationInput),
            destinationIsolationProofId:
              `restore-isolation-proof-${randomUUID()}`,
            expectedOperationRevision: "0",
            generationId: `restore-generation-${randomUUID()}`,
          });
        const currentGenerationFinalized =
          await authority.finalizeRestoreDestinationGeneration({
            ...structuredClone(currentGenerationInput),
            completion: restoreGenerationCompletion(
              currentGenerationInput,
              currentGenerationClaimed,
              false,
            ),
            expectedOperationRevision: "1",
          });
        assertOperationReceipt(currentGenerationFinalized, "committed");
        const currentWriterGeneration =
          currentGenerationFinalized.generation;
        assert.notEqual(
          currentWriterGeneration.generationId,
          targetGeneration.generationId,
        );
        assert.equal(
          currentWriterGeneration.binding.attachment.attachmentId,
          oldAttachment.attachmentId,
        );

        const currentLaunchInput = writerLaunchAttemptInput(
          currentGenerationFinalized.session,
          currentWriterGeneration,
        );
        await authority.reserveOperation(currentLaunchInput);
        await authority.claimWriterLaunchAttemptDispatch({
          ...structuredClone(currentLaunchInput),
          expectedOperationRevision: "0",
        });
        const currentLaunchEvidence = writerLaunchEvidence(
          currentLaunchInput,
          "started",
        );
        const currentWriterStarted =
          await authority.finalizeWriterLaunchAttemptStarted({
            ...structuredClone(currentLaunchInput),
            evidence: currentLaunchEvidence,
            expectedOperationRevision: "1",
          });
        assertOperationReceipt(currentWriterStarted, "committed");
        assert.equal(
          currentWriterStarted.session.document.launch.generation
            .generationId,
          currentWriterGeneration.generationId,
        );

        const stop = writerLaunchStopInput(currentWriterStarted.session);
        await authority.reserveOperation(stop.input);
        await authority.claimWriterLaunchStopDispatch({
          ...structuredClone(stop.input),
          claimToken: stop.claimToken,
          expectedOperationRevision: "0",
        });
        const stopped = await authority.finalizeWriterLaunchStopped({
          ...structuredClone(stop.input),
          evidence: writerLaunchStopEvidence(stop.input),
          expectedOperationRevision: "1",
        });
        assertOperationReceipt(stopped, "committed");
        assert.equal(
          stop.input.request.launch.generation.generationId,
          currentWriterGeneration.generationId,
        );

        const captureAdmission = checkpointCaptureAdmission(
          { session: stopped.session },
          {
            processIncarnationId:
              currentLaunchEvidence.processIncarnationId,
            stopOperationId: stopped.operation.operationId,
            writerIncarnationId:
              currentLaunchEvidence.writerIncarnationId,
          },
        );
        await checkpointAuthority.runCapture(
          captureAdmission,
          async (context) => checkpointCompletion(context, false),
        );
        const captured = await authority.reconcileOperation(
          checkpointOperationInput(stopped.session, captureAdmission),
        );
        assertOperationReceipt(captured, "committed");
        assert.equal(
          captured.operation.result.checkpointId,
          captureAdmission.checkpoint.checkpointId,
        );
        const catalogue = await authority.readCheckpointCatalogue({
          checkpoint: captureAdmission.checkpoint,
        });
        assert.equal(catalogue.attempt.state, "committed");
        assert.equal(
          catalogue.attempt.binding.attachmentId,
          oldAttachment.attachmentId,
        );
        assert.equal(
          catalogue.attempt.binding.stopOperationId,
          stopped.operation.operationId,
        );
        assert.equal(
          catalogue.attempt.binding.processIncarnationId,
          currentLaunchEvidence.processIncarnationId,
        );
        assert.equal(
          catalogue.attempt.binding.writerIncarnationId,
          currentLaunchEvidence.writerIncarnationId,
        );

        const released = await releaseWriter(authority, captured);
        assertOperationReceipt(released, "committed");
        assert.equal(released.session.document.lifecycle, "DETACHED");
        assert.equal(
          released.operation.expectedSession.document.lastOperation
            .operationId,
          captured.operation.operationId,
        );

        const launchIntent = restoreGenerationLaunchIntent(
          released.session,
        );
        const activationInput = {
          expectedSession: released.session,
          kind: RESTORE_ATTACHMENT_ACTIVATION_OPERATION_KIND,
          operationId: `restore-activation-${randomUUID()}`,
          request: createRestoreAttachmentActivationOperationRequestV2({
            destinationRootPath:
              `/var/lib/portable-codex/restores/${sessionId}`,
            expectedSession: released.session,
            generation: targetGeneration,
            holderId: `restore-host-${randomUUID()}`,
            launchIntent,
            leaseDurationMilliseconds: 300_000,
            predecessor: {
              attachmentId: oldAttachment.attachmentId,
              captureOperationId: captured.operation.operationId,
              detachOperationId: released.operation.operationId,
              stopOperationId: stopped.operation.operationId,
            },
          }),
        };
        assert.equal(activationInput.request.contractVersion, 2);
        const fleetIncompatibleAuthority =
          new PostgresSessionAuthority({
            restoreAttachmentActivationV2FleetCompatible: false,
            restoreAttachmentActivationV2GenerationPredecessorFleetCompatible:
              false,
            restoreGenerationV2FleetCompatible: true,
            store,
          });
        await assert.rejects(
          fleetIncompatibleAuthority.reserveOperation(
            structuredClone(activationInput),
          ),
          assertAuthorityCode(
            "restore_attachment_activation_v2_fleet_capability_required",
          ),
        );
        const deniedState = await pool.query(
          [
            "SELECT",
            "(SELECT count(*)::integer",
            "FROM session_authority.operation_claims",
            "WHERE operation_id = $1) AS operation_count,",
            "(SELECT count(*)::integer",
            "FROM session_authority.reservations",
            "WHERE operation_id = $1) AS reservation_count,",
            "(SELECT count(*)::integer",
            "FROM session_authority.operation_id_registry",
            "WHERE operation_id IN ($1, $2)) AS registry_count",
          ].join(" "),
          [
            activationInput.operationId,
            activationInput.request.launchIntent.launchAttemptId,
          ],
        );
        assert.deepEqual(deniedState.rows, [
          {
            operation_count: 0,
            registry_count: 0,
            reservation_count: 0,
          },
        ]);

        const topologyIncompatibleAuthority =
          new PostgresSessionAuthority({
            restoreAttachmentActivationV2FleetCompatible: true,
            restoreAttachmentActivationV2GenerationPredecessorFleetCompatible:
              false,
            restoreGenerationV2FleetCompatible: true,
            store,
          });
        const reserved =
          await topologyIncompatibleAuthority.reserveOperation(
            activationInput,
          );
        assertOperationReceipt(reserved, "prepared");
        assert.equal(reserved.acquired, true);
        const gateClosedReplay =
          await fleetIncompatibleAuthority.reserveOperation(
            structuredClone(activationInput),
          );
        assertOperationReceipt(gateClosedReplay, "prepared");
        assert.equal(gateClosedReplay.acquired, false);
        assert.deepEqual(gateClosedReplay.operation, reserved.operation);
        assert.deepEqual(
          gateClosedReplay.reservation,
          reserved.reservation,
        );
        assert.deepEqual(gateClosedReplay.session, reserved.session);
        const claimed =
          await authority.claimRestoreAttachmentActivationDispatch({
            ...structuredClone(activationInput),
            expectedOperationRevision: "0",
          });
        assertOperationReceipt(claimed, "starting");
        assert.equal(claimed.dispatchGranted, true);
        assert.deepEqual(
          structuredClone(
            claimed.operation.request.predecessor,
          ),
          structuredClone(activationInput.request.predecessor),
        );

        const proofId = `restore-attachment-proof-${randomUUID()}`;
        const mutationRequest = claimed.activationRequest.mutationRequest;
        const activationResult = {
          attachment: {
            backendId: mutationRequest.backendId,
            contractVersion: mutationRequest.contractVersion,
            storageId: mutationRequest.storageId,
            sessionId: mutationRequest.sessionId,
            attachmentId: mutationRequest.target.attachmentId,
            leaseId: mutationRequest.leaseId,
            holderId: mutationRequest.holderId,
            fencingEpoch: mutationRequest.fencingEpoch,
            operationId: mutationRequest.operationId,
            proofId,
            kind: "directory",
            rootPath: activationInput.request.destinationRootPath,
            mode: "read-write",
          },
          contractVersion: 1,
          mutationResult: {
            ...structuredClone(mutationRequest),
            proofId,
            status: "attached",
          },
          publication: structuredClone(
            claimed.activationRequest.publication,
          ),
        };
        const finalization = {
          ...structuredClone(activationInput),
          activationResult,
          expectedOperationRevision: "1",
        };
        const finalized =
          await authority.finalizeRestoreAttachmentActivationAndReserveWriterLaunchAttempt(
            structuredClone(finalization),
          );
        assert.equal(finalized.status, "prepared");
        assert.equal(finalized.activation.finalized, true);
        assert.equal(
          finalized.activation.operation.result.outcome,
          "restore-attachment-activated",
        );
        assert.equal(
          finalized.session.document.activeOperation.operationId,
          launchIntent.launchAttemptId,
        );
        assert.equal(
          finalized.session.document.lastOperation.operationId,
          activationInput.operationId,
        );
        assert.equal(
          finalized.launch.attempt.request.generation.generationId,
          targetGeneration.generationId,
        );
        assert.equal(
          finalized.launch.attempt.request.attachment.attachmentId,
          activationResult.attachment.attachmentId,
        );
        assert.notEqual(
          finalized.launch.attempt.request.attachment.attachmentId,
          oldAttachment.attachmentId,
        );

        const oldAttachmentLaunches = await pool.query(
          [
            "SELECT",
            "count(*) FILTER (WHERE",
            "request #>> '{payload,generation,generationId}' = $3",
            "AND request #>> '{payload,attachment,attachmentId}' = $4)",
            "::integer AS current_generation_launch_count,",
            "count(*) FILTER (WHERE",
            "request #>> '{payload,generation,generationId}' = $5",
            "AND request #>> '{payload,attachment,attachmentId}' = $4)",
            "::integer AS target_generation_launch_count",
            "FROM session_authority.operation_claims",
            "WHERE session_id = $1 AND kind = $2",
          ].join(" "),
          [
            sessionId,
            WRITER_LAUNCH_ATTEMPT_OPERATION_KIND,
            currentWriterGeneration.generationId,
            oldAttachment.attachmentId,
            targetGeneration.generationId,
          ],
        );
        assert.deepEqual(oldAttachmentLaunches.rows, [
          {
            current_generation_launch_count: 1,
            target_generation_launch_count: 0,
          },
        ]);

        const read = await authority.readRestoreAttachmentActivation({
          operationId: activationInput.operationId,
        });
        assertOperationReceipt(read, "committed", {
          activeOperationId: launchIntent.launchAttemptId,
          currentTerminal: false,
        });
        assert.deepEqual(
          structuredClone(read.activationRequest),
          structuredClone(claimed.activationRequest),
        );
        const replay =
          await authority.finalizeRestoreAttachmentActivationAndReserveWriterLaunchAttempt(
            structuredClone(finalization),
          );
        assert.equal(replay.activation.finalized, false);
        assert.deepEqual(replay.launch, finalized.launch);
        assert.deepEqual(replay.session, finalized.session);
      },
    );

    await t.test(
      "generation-predecessor activation atomically hands one clean-detached intent to the launcher",
      async () => {
        const sessionId = randomUUID();
        sessionIds.push(sessionId);
        const image = integrationPlatformImageFixture();
        const currentFixture =
          await prepareCommittedRestoreGenerationFixture(
            authority,
            checkpointAuthority,
            sessionId,
            { imageDigest: image.descriptor.digest },
          );
        const currentGeneration = currentFixture.finalized.generation;
        const oldAttachment =
          currentFixture.finalized.session.document.attachment;
        assert.notEqual(oldAttachment, null);

        const currentLaunchInput = writerLaunchAttemptInput(
          currentFixture.finalized.session,
          currentGeneration,
        );
        await authority.reserveOperation(currentLaunchInput);
        await authority.claimWriterLaunchAttemptDispatch({
          ...structuredClone(currentLaunchInput),
          expectedOperationRevision: "0",
        });
        const currentLaunchEvidence = writerLaunchEvidence(
          currentLaunchInput,
          "started",
        );
        const currentWriterStarted =
          await authority.finalizeWriterLaunchAttemptStarted({
            ...structuredClone(currentLaunchInput),
            evidence: currentLaunchEvidence,
            expectedOperationRevision: "1",
          });
        assertOperationReceipt(currentWriterStarted, "committed");
        assert.equal(
          currentWriterStarted.session.document.launch.generation
            .generationId,
          currentGeneration.generationId,
        );

        const stop = writerLaunchStopInput(currentWriterStarted.session);
        await authority.reserveOperation(stop.input);
        await authority.claimWriterLaunchStopDispatch({
          ...structuredClone(stop.input),
          claimToken: stop.claimToken,
          expectedOperationRevision: "0",
        });
        const stopped = await authority.finalizeWriterLaunchStopped({
          ...structuredClone(stop.input),
          evidence: writerLaunchStopEvidence(stop.input),
          expectedOperationRevision: "1",
        });
        assertOperationReceipt(stopped, "committed");

        const captureAdmission = checkpointCaptureAdmission(
          { session: stopped.session },
          {
            processIncarnationId:
              currentLaunchEvidence.processIncarnationId,
            stopOperationId: stopped.operation.operationId,
            writerIncarnationId:
              currentLaunchEvidence.writerIncarnationId,
          },
        );
        await checkpointAuthority.runCapture(
          captureAdmission,
          async (context) => checkpointCompletion(context, false),
        );
        const captured = await authority.reconcileOperation(
          checkpointOperationInput(stopped.session, captureAdmission),
        );
        assertOperationReceipt(captured, "committed");
        assert.equal(
          captured.operation.expectedSession.document.lastOperation
            .operationId,
          stopped.operation.operationId,
        );

        const targetAdmission = restoreGenerationAdmission(
          { session: captured.session },
          currentFixture.checkpoint,
        );
        const targetInput = restoreGenerationOperationInput(
          captured.session,
          targetAdmission,
        );
        assert.equal(targetInput.request.contractVersion, 1);
        await authority.reserveOperation(targetInput);
        const targetClaimed =
          await authority.claimRestoreDestinationGenerationDispatch({
            ...structuredClone(targetInput),
            destinationIsolationProofId:
              `restore-isolation-proof-${randomUUID()}`,
            expectedOperationRevision: "0",
            generationId: `restore-generation-${randomUUID()}`,
          });
        const targetFinalized =
          await authority.finalizeRestoreDestinationGeneration({
            ...structuredClone(targetInput),
            completion: restoreGenerationCompletion(
              targetInput,
              targetClaimed,
              false,
            ),
            expectedOperationRevision: "1",
          });
        assertOperationReceipt(targetFinalized, "committed");
        const targetGeneration = targetFinalized.generation;
        assert.notEqual(
          targetGeneration.generationId,
          currentGeneration.generationId,
        );
        assert.equal(
          targetFinalized.operation.expectedSession.document.lastOperation
            .operationId,
          captured.operation.operationId,
        );
        assert.deepEqual(
          targetFinalized.operation.expectedSession.document.lastOperation,
          captured.session.document.lastOperation,
        );
        assert.equal(
          targetGeneration.binding.attachment.attachmentId,
          oldAttachment.attachmentId,
        );

        const released = await releaseWriter(authority, targetFinalized);
        assertOperationReceipt(released, "committed");
        assert.equal(released.session.document.lifecycle, "DETACHED");
        assert.equal(released.session.document.attachment, null);
        assert.equal(released.session.document.lease, null);
        assert.equal(released.session.document.launch, null);
        assert.equal(released.session.document.activeOperation, null);
        assert.equal(
          released.operation.expectedSession.document.lastOperation
            .operationId,
          targetInput.operationId,
        );
        assert.deepEqual(
          released.operation.expectedSession.document.lastOperation,
          targetFinalized.session.document.lastOperation,
        );

        const imageReservations =
          new PlatformImageReservationCoordinator();
        const inspectCodex = async () => ({
          codexBinaryPath: "/opt/portable-codex/bin/codex",
          codexBinarySha256: "c".repeat(64),
          codexVersion:
            released.session.document.manifest.runtime.codexVersion,
        });
        const reservedImage =
          await imageReservations.reservePlatformImage({
            configBytes: image.configBytes,
            descriptor: image.descriptor,
            inspectCodex,
            sessionManifest: released.session.document.manifest,
          });
        const imageReservation = {
          configBytes: image.configBytes,
          descriptor: image.descriptor,
          inspectCodex,
          reservation: reservedImage.reservation,
        };
        const launchAttemptId = `writer-launch-${randomUUID()}`;
        const supervisorId = `supervisor-${randomUUID()}`;
        let launchCalls = 0;
        let launchReserveCalls = 0;
        let launchedRequest = null;
        const launcherAuthority = {
          async cancelPreparedOperation(options) {
            return authority.cancelPreparedOperation(options);
          },
          async claimWriterLaunchAttemptDispatch(options) {
            return authority.claimWriterLaunchAttemptDispatch(options);
          },
          async claimWriterLaunchStopDispatch(options) {
            return authority.claimWriterLaunchStopDispatch(options);
          },
          async finalizeWriterLaunchAttemptStarted(options) {
            return authority.finalizeWriterLaunchAttemptStarted(options);
          },
          async finalizeWriterLaunchAttemptStopped(options) {
            return authority.finalizeWriterLaunchAttemptStopped(options);
          },
          async finalizeWriterLaunchStopped(options) {
            return authority.finalizeWriterLaunchStopped(options);
          },
          async finalizeWriterLaunchStoppedAndReserveCheckpointCapture(
            options,
          ) {
            return authority.finalizeWriterLaunchStoppedAndReserveCheckpointCapture(
              options,
            );
          },
          async markOperationUncertain(options) {
            return authority.markOperationUncertain(options);
          },
          async readSession(options) {
            return authority.readSession(options);
          },
          async readWriterLaunchAttempt(options) {
            return authority.readWriterLaunchAttempt(options);
          },
          async reconcileWriterLaunchStopOperation(options) {
            return authority.reconcileWriterLaunchStopOperation(options);
          },
          async reserveOperation(options) {
            launchReserveCalls += 1;
            return authority.reserveOperation(options);
          },
        };
        const facade = createPostgresLogicalWriterLauncher({
          authority: launcherAuthority,
          imageReservations,
          operationGuard,
          stoppedWriterCoordinator:
            new StoppedWriterCapabilityCoordinator(),
          supervisor: {
            contractVersion: 1,
            launchWriter: async (context) => {
              launchCalls += 1;
              launchedRequest = context.attempt.request;
              return {
                receiptVersion: 1,
                evidence: writerLaunchEvidence(
                  {
                    operationId: launchAttemptId,
                    request: context.attempt.request,
                  },
                  "started",
                ),
                stopWriter: async function stopWriter() {
                  return STOPPED_WRITER_STOP_CONFIRMED;
                },
              };
            },
            reconcileWriterLaunch: async () => {
              throw new Error(
                "an activation-prepared launch must not reconcile before dispatch",
              );
            },
            supervisorId,
          },
        });
        const launchIntent = await facade.prepareLaunchIntent({
          expectedSession: released.session,
          imageReservation,
          launchAttemptId,
        });
        assert.equal(launchIntent.launchAttemptId, launchAttemptId);
        assert.equal(launchReserveCalls, 0);
        assert.equal(launchCalls, 0);

        const activationInput = {
          expectedSession: released.session,
          kind: RESTORE_ATTACHMENT_ACTIVATION_OPERATION_KIND,
          operationId: `restore-activation-${randomUUID()}`,
          request: createRestoreAttachmentActivationOperationRequestV2({
            destinationRootPath:
              `/var/lib/portable-codex/restores/${sessionId}`,
            expectedSession: released.session,
            generation: targetGeneration,
            holderId: `restore-host-${randomUUID()}`,
            launchIntent,
            leaseDurationMilliseconds: 300_000,
            predecessor: {
              attachmentId: oldAttachment.attachmentId,
              captureOperationId: captured.operation.operationId,
              detachOperationId: released.operation.operationId,
              stopOperationId: stopped.operation.operationId,
            },
          }),
        };
        const topologyIncompatibleAuthority =
          new PostgresSessionAuthority({
            restoreAttachmentActivationV2FleetCompatible: true,
            restoreAttachmentActivationV2GenerationPredecessorFleetCompatible:
              false,
            restoreGenerationV2FleetCompatible: true,
            store,
          });
        await assert.rejects(
          topologyIncompatibleAuthority.reserveOperation(
            structuredClone(activationInput),
          ),
          assertAuthorityCode(
            "restore_attachment_activation_v2_generation_predecessor_fleet_capability_required",
          ),
        );
        const gateClosedState = await pool.query(
          [
            "SELECT",
            "(SELECT count(*)::integer",
            "FROM session_authority.operation_claims",
            "WHERE operation_id = $1) AS operation_count,",
            "(SELECT count(*)::integer",
            "FROM session_authority.reservations",
            "WHERE operation_id = $1) AS reservation_count,",
            "(SELECT count(*)::integer",
            "FROM session_authority.operation_id_registry",
            "WHERE operation_id IN ($1, $2)) AS registry_count",
          ].join(" "),
          [activationInput.operationId, launchAttemptId],
        );
        assert.deepEqual(gateClosedState.rows, [
          {
            operation_count: 0,
            registry_count: 0,
            reservation_count: 0,
          },
        ]);
        const activationReserved =
          await authority.reserveOperation(activationInput);
        assertOperationReceipt(activationReserved, "prepared");
        const activationClaimed =
          await authority.claimRestoreAttachmentActivationDispatch({
            ...structuredClone(activationInput),
            expectedOperationRevision: "0",
          });
        assertOperationReceipt(activationClaimed, "starting");
        assert.deepEqual(
          structuredClone(
            activationClaimed.operation.request.predecessor,
          ),
          structuredClone(activationInput.request.predecessor),
        );

        const proofId = `restore-attachment-proof-${randomUUID()}`;
        const mutationRequest =
          activationClaimed.activationRequest.mutationRequest;
        const activationResult = {
          attachment: {
            backendId: mutationRequest.backendId,
            contractVersion: mutationRequest.contractVersion,
            storageId: mutationRequest.storageId,
            sessionId: mutationRequest.sessionId,
            attachmentId: mutationRequest.target.attachmentId,
            leaseId: mutationRequest.leaseId,
            holderId: mutationRequest.holderId,
            fencingEpoch: mutationRequest.fencingEpoch,
            operationId: mutationRequest.operationId,
            proofId,
            kind: "directory",
            rootPath: activationInput.request.destinationRootPath,
            mode: "read-write",
          },
          contractVersion: 1,
          mutationResult: {
            ...structuredClone(mutationRequest),
            proofId,
            status: "attached",
          },
          publication: structuredClone(
            activationClaimed.activationRequest.publication,
          ),
        };
        const finalization = {
          ...structuredClone(activationInput),
          activationResult,
          expectedOperationRevision: "1",
        };
        const acknowledgementLossAuthority =
          new PostgresSessionAuthority({
            restoreAttachmentActivationV2FleetCompatible: true,
            restoreAttachmentActivationV2GenerationPredecessorFleetCompatible:
              true,
            restoreGenerationV2FleetCompatible: true,
            store: new PostgresSerializableStore({
              dedicatedPool: firstCommitAcknowledgementLossPool(pool),
            }),
          });
        await assert.rejects(
          acknowledgementLossAuthority.finalizeRestoreAttachmentActivationAndReserveWriterLaunchAttempt(
            structuredClone(finalization),
          ),
          assertCommitOutcomeUncertain,
        );
        const finalized =
          await authority.finalizeRestoreAttachmentActivationAndReserveWriterLaunchAttempt(
            structuredClone(finalization),
          );
        assert.equal(finalized.status, "prepared");
        assert.equal(finalized.activation.finalized, false);
        assert.equal(
          finalized.activation.operation.expectedSession.document
            .lastOperation.operationId,
          released.operation.operationId,
        );
        assert.deepEqual(
          finalized.activation.operation.expectedSession.document
            .lastOperation,
          released.session.document.lastOperation,
        );
        assert.equal(
          finalized.launch.operation.expectedSession.document.lastOperation
            .operationId,
          activationInput.operationId,
        );
        assert.deepEqual(
          finalized.launch.operation.expectedSession.document.lastOperation,
          finalized.session.document.lastOperation,
        );
        assert.equal(
          finalized.session.document.activeOperation.operationId,
          launchAttemptId,
        );
        assert.equal(
          finalized.session.document.lastOperation.operationId,
          activationInput.operationId,
        );
        assert.equal(
          finalized.launch.attempt.request.generation.generationId,
          targetGeneration.generationId,
        );
        const finalizationReplay =
          await authority.finalizeRestoreAttachmentActivationAndReserveWriterLaunchAttempt(
            structuredClone(finalization),
          );
        assert.equal(finalizationReplay.activation.finalized, false);
        assert.deepEqual(finalizationReplay.launch, finalized.launch);
        assert.deepEqual(finalizationReplay.session, finalized.session);

        const durablePreparedLaunch = await pool.query(
          [
            "SELECT",
            "count(DISTINCT operations.operation_id)::integer AS operation_count,",
            "count(DISTINCT reservations.reservation_id)::integer AS reservation_count",
            "FROM session_authority.operation_claims AS operations",
            "JOIN session_authority.reservations AS reservations",
            "ON reservations.operation_id = operations.operation_id",
            "WHERE operations.operation_id = $1",
            "AND operations.kind = $2",
          ].join(" "),
          [launchAttemptId, WRITER_LAUNCH_ATTEMPT_OPERATION_KIND],
        );
        assert.deepEqual(durablePreparedLaunch.rows, [
          { operation_count: 1, reservation_count: 1 },
        ]);

        const started = await facade.runPreparedLaunch({
          imageReservation,
          launchAttemptId,
        });
        assert.equal(started.status, "started");
        assert.notEqual(started.writer, null);
        assert.equal(launchReserveCalls, 0);
        assert.equal(launchCalls, 1);
        assert.deepEqual(
          JSON.parse(JSON.stringify(launchedRequest)),
          JSON.parse(
            JSON.stringify(finalized.launch.attempt.request),
          ),
        );

        const replayed = await facade.runPreparedLaunch({
          imageReservation,
          launchAttemptId,
        });
        assert.equal(replayed.status, "started");
        assert.strictEqual(replayed.writer, started.writer);
        assert.equal(launchReserveCalls, 0);
        assert.equal(launchCalls, 1);
        const active = await authority.readWriterLaunchAttempt({
          operationId: launchAttemptId,
        });
        assertOperationReceipt(active, "committed");
        assert.equal(
          active.session.document.launch.launchAttemptId,
          launchAttemptId,
        );
        assert.equal(
          active.session.document.launch.generation.generationId,
          targetGeneration.generationId,
        );
      },
    );

    await t.test(
      "writer launch claim rechecks the authority clock after a blocking generation lock",
      async () => {
        const sessionId = randomUUID();
        sessionIds.push(sessionId);
        const fixture = await prepareCommittedRestoreGenerationFixture(
          authority,
          checkpointAuthority,
          sessionId,
          { finalAttachmentLeaseDurationMilliseconds: 3_000 },
        );
        const input = writerLaunchAttemptInput(
          fixture.finalized.session,
          fixture.finalized.generation,
        );
        const prepared = await authority.reserveOperation(input);
        assertOperationReceipt(prepared, "prepared");

        const lockClient = await pool.connect();
        let lockHeld = false;
        try {
          await lockClient.query("BEGIN");
          lockHeld = true;
          await lockClient.query(
            [
              "SELECT generation_id",
              "FROM session_authority.restore_destination_generations",
              "WHERE generation_id = $1",
              "FOR UPDATE",
            ].join(" "),
            [fixture.finalized.generation.generationId],
          );

          const notification =
            firstRestoreGenerationLockQueryNotificationPool(
              pool,
              "blocked writer launch generation lock",
            );
          const blockedAuthority = new PostgresSessionAuthority({
            store: new PostgresSerializableStore({
              dedicatedPool: notification.dedicatedPool,
              maxTransactionAttempts: 3,
            }),
          });
          let claimSettled = false;
          const claimPromise =
            blockedAuthority.claimWriterLaunchAttemptDispatch({
              ...structuredClone(input),
              expectedOperationRevision: "0",
            });
          const expectedRejection = assert.rejects(
            claimPromise,
            assertAuthorityCode("writer_lease_expired"),
          );
          void claimPromise.then(
            () => {
              claimSettled = true;
            },
            () => {
              claimSettled = true;
            },
          );

          await notification.waitForFirstMatch();
          assert.equal(claimSettled, false);
          await waitForDatabaseLeaseExpiry(
            lockClient,
            fixture.finalized.session.document.lease.expiresAt,
          );
          assert.equal(claimSettled, false);

          await lockClient.query("ROLLBACK");
          lockHeld = false;
          await expectedRejection;
        } finally {
          if (lockHeld) {
            await lockClient.query("ROLLBACK");
          }
          lockClient.release();
        }

        const read = await authority.readWriterLaunchAttempt({
          operationId: input.operationId,
        });
        assertOperationReceipt(read, "prepared");
        assert.equal(read.attempt.state, "prepared");
        assert.equal(read.operation.state, "prepared");
        assert.equal(read.operation.revision, "0");
        assert.equal(read.reservation.state, "prepared");
        assert.equal(read.session.document.launch, null);
        assert.equal(
          read.session.document.activeOperation.operationId,
          input.operationId,
        );
        assert.equal(
          read.session.document.activeOperation.state,
          "prepared",
        );
        assert.equal(
          read.session.document.activeOperation.operationRevision,
          "0",
        );
        assert.deepEqual(read.operation, prepared.operation);
        assert.deepEqual(read.reservation, prepared.reservation);
        assert.deepEqual(read.session, prepared.session);
        const cancelled = await authority.cancelPreparedOperation({
          ...structuredClone(input),
          expectedOperationRevision: "0",
          reason: "lease-expired-before-launch-dispatch",
        });
        assertOperationReceipt(cancelled, "committed");
      },
    );

    await t.test(
      "logical writer launcher recovers renewal and stop-claim acknowledgement loss before finalization",
      async () => {
        const sessionId = randomUUID();
        sessionIds.push(sessionId);
        const image = integrationPlatformImageFixture();
        const fixture = await prepareCommittedRestoreGenerationFixture(
          authority,
          checkpointAuthority,
          sessionId,
          { imageDigest: image.descriptor.digest },
        );
        const imageReservations =
          new PlatformImageReservationCoordinator();
        const inspectCodex = async () => {
          return {
            codexBinaryPath: "/opt/portable-codex/bin/codex",
            codexBinarySha256: "c".repeat(64),
            codexVersion:
              fixture.finalized.session.document.manifest.runtime
                .codexVersion,
          };
        };
        const reserved = await imageReservations.reservePlatformImage({
          configBytes: image.configBytes,
          descriptor: image.descriptor,
          inspectCodex,
          sessionManifest:
            fixture.finalized.session.document.manifest,
        });
        const launchAttemptId = `writer-launch-${randomUUID()}`;
        const supervisorId = `supervisor-${randomUUID()}`;
        let launchCalls = 0;
        let stopCalls = 0;
        let renewedDuringStopReserve = null;
        let stopReadSession = null;
        let stopRenewalArmed = false;
        let stopReserveAttempts = 0;
        let stopClaimAcknowledgementLosses = 0;
        let stopUncertaintyInput = null;
        const stopClaimLossAuthority = new PostgresSessionAuthority({
          store: new PostgresSerializableStore({
            dedicatedPool: firstCommitAcknowledgementLossPool(pool),
          }),
        });
        const launcherAuthority = {
          async cancelPreparedOperation(options) {
            return authority.cancelPreparedOperation(options);
          },
          async claimWriterLaunchAttemptDispatch(options) {
            return authority.claimWriterLaunchAttemptDispatch(options);
          },
          async claimWriterLaunchStopDispatch(options) {
            if (stopClaimAcknowledgementLosses === 0) {
              stopClaimAcknowledgementLosses += 1;
              return stopClaimLossAuthority.claimWriterLaunchStopDispatch(
                options,
              );
            }
            return authority.claimWriterLaunchStopDispatch(options);
          },
          async finalizeWriterLaunchAttemptStarted(options) {
            return authority.finalizeWriterLaunchAttemptStarted(options);
          },
          async finalizeWriterLaunchAttemptStopped(options) {
            return authority.finalizeWriterLaunchAttemptStopped(options);
          },
          async finalizeWriterLaunchStopped(options) {
            return authority.finalizeWriterLaunchStopped(options);
          },
          async finalizeWriterLaunchStoppedAndReserveCheckpointCapture(
            options,
          ) {
            return authority.finalizeWriterLaunchStoppedAndReserveCheckpointCapture(
              options,
            );
          },
          async markOperationUncertain(options) {
            return authority.markOperationUncertain(options);
          },
          async readSession(options) {
            const session = await authority.readSession(options);
            if (stopRenewalArmed) {
              stopReadSession = structuredClone(session);
            }
            return session;
          },
          async readWriterLaunchAttempt(options) {
            return authority.readWriterLaunchAttempt(options);
          },
          async reconcileWriterLaunchStopOperation(options) {
            return authority.reconcileWriterLaunchStopOperation(options);
          },
          async reserveOperation(options) {
            if (options.kind === WRITER_LAUNCH_STOP_OPERATION_KIND) {
              stopReserveAttempts += 1;
              if (stopRenewalArmed) {
                stopRenewalArmed = false;
                assert.notEqual(stopReadSession, null);
                renewedDuringStopReserve = await authority.renewWriterLease(
                  writerLeaseRenewalInput(stopReadSession),
                );
                assertOperationReceipt(
                  renewedDuringStopReserve,
                  "committed",
                );
              }
              stopUncertaintyInput = structuredClone(options);
            }
            return authority.reserveOperation(options);
          },
        };
        const launchWriter = async (context) => {
          launchCalls += 1;
          assert.equal(
            context.attempt.launchAttemptId,
            launchAttemptId,
          );
          return {
            receiptVersion: 1,
            evidence: writerLaunchEvidence(
              {
                operationId: launchAttemptId,
                request: context.attempt.request,
              },
              "started",
            ),
            stopWriter: async function stopWriter() {
              stopCalls += 1;
              assert.notEqual(stopUncertaintyInput, null);
              const uncertain = await authority.markOperationUncertain({
                ...stopUncertaintyInput,
                expectedOperationRevision: "1",
              });
              assertOperationReceipt(uncertain, "uncertain");
              return STOPPED_WRITER_STOP_CONFIRMED;
            },
          };
        };
        const reconcileWriterLaunch = async () => {
          throw new Error("committed launches must not reach the supervisor");
        };
        const facade = createPostgresLogicalWriterLauncher({
          authority: launcherAuthority,
          imageReservations,
          operationGuard,
          stoppedWriterCoordinator:
            new StoppedWriterCapabilityCoordinator(),
          supervisor: {
            contractVersion: 1,
            launchWriter,
            reconcileWriterLaunch,
            supervisorId,
          },
        });

        const started = await facade.runLaunch({
          generation: fixture.finalized.generation,
          imageReservation: {
            configBytes: image.configBytes,
            descriptor: image.descriptor,
            inspectCodex,
            reservation: reserved.reservation,
          },
          launchAttemptId,
        });
        assert.equal(started.status, "started");
        assert.notEqual(started.writer, null);
        assert.equal(launchCalls, 1);

        const read = await authority.readWriterLaunchAttempt({
          operationId: launchAttemptId,
        });
        assertOperationReceipt(read, "committed");
        assert.equal(
          read.operation.result.outcome,
          "writer-launch-started",
        );
        assert.deepEqual(
          JSON.parse(JSON.stringify(read.operation.result.evidence)),
          JSON.parse(JSON.stringify(started.evidence)),
        );
        assert.deepEqual(
          JSON.parse(JSON.stringify(read.launch)),
          JSON.parse(JSON.stringify(started.launch)),
        );
        assert.deepEqual(
          JSON.parse(JSON.stringify(read.session.document.launch)),
          JSON.parse(JSON.stringify(started.launch)),
        );
        const current = await authority.readSession({ sessionId });
        assert.deepEqual(
          JSON.parse(JSON.stringify(current.document.launch)),
          JSON.parse(JSON.stringify(started.launch)),
        );

        const reconciled = await facade.reconcileLaunchAttempt({
          launchAttemptId,
        });
        assert.equal(reconciled.status, "started");
        assert.strictEqual(reconciled.writer, started.writer);
        assert.equal(launchCalls, 1);

        const guardResult = await operationGuard.runExclusive(
          launchAttemptId,
          async (probe, complete) => {
            await probe.assertHeld();
            return complete("guard-reacquired");
          },
        );
        assert.equal(guardResult, "guard-reacquired");

        const capture = checkpointCaptureAdmission(
          { session: current },
          {
            processIncarnationId: started.evidence.processIncarnationId,
            writerIncarnationId: started.evidence.writerIncarnationId,
          },
        );
        const captureInput = {
          attachment: capture.attachment,
          checkpoint: capture.checkpoint,
          request: capture.request,
        };
        stopRenewalArmed = true;
        const stopped = await facade.stopWriterForCapture(captureInput);
        assert.notEqual(renewedDuringStopReserve, null);
        assert.equal(renewedDuringStopReserve.renewed, true);
        assert.equal(stopReserveAttempts, 2);
        assert.equal(stopClaimAcknowledgementLosses, 1);
        assert.equal(stopped.stop.status, "committed");
        assert.equal(stopped.stop.operation.revision, "3");
        assert.equal(stopped.evidence.status, "complete-stopped");
        assert.equal(stopCalls, 1);
        assert.deepEqual(
          stopped.stop.operation.expectedSession.document.lease,
          renewedDuringStopReserve.session.document.lease,
        );
        assert.deepEqual(
          stopped.resolution.canonicalLeaseAtRegistration,
          started.attempt.request.lease,
        );

        const historical = await authority.readWriterLaunchAttempt({
          operationId: launchAttemptId,
        });
        assertOperationReceipt(historical, "committed", {
          currentTerminal: false,
        });
        assert.equal(
          historical.operation.result.outcome,
          "writer-launch-started",
        );
        assert.equal(historical.launch, null);
        assert.equal(historical.session.document.launch, null);

        await assert.rejects(
          facade.reconcileLaunchAttempt({ launchAttemptId }),
          assertLauncherCode("logical_writer_launch_outcome_uncertain"),
        );
        assert.equal(launchCalls, 1);
        assert.equal(stopCalls, 1);
      },
    );

    await t.test(
      "restore launch handoff rolls back every authority write and rejects crossed intent",
      async () => {
        const sessionId = randomUUID();
        sessionIds.push(sessionId);
        const fixture = await prepareRestoreGenerationFixture(
          authority,
          checkpointAuthority,
          sessionId,
        );
        const admission = restoreGenerationAdmission(
          fixture.attached,
          fixture.checkpoint,
        );
        const launchIntent = restoreGenerationLaunchIntent(
          fixture.attached.session,
        );
        const input = restoreGenerationOperationInputV2(
          fixture.attached.session,
          admission,
          launchIntent,
        );
        const fleetIncompatibleAuthority =
          new PostgresSessionAuthority({
            restoreAttachmentActivationV2FleetCompatible: true,
            restoreGenerationV2FleetCompatible: false,
            store,
          });
        await assert.rejects(
          fleetIncompatibleAuthority.reserveOperation(
            structuredClone(input),
          ),
          assertAuthorityCode(
            "restore_generation_v2_fleet_capability_required",
          ),
        );
        const deniedState = await pool.query(
          [
            "SELECT",
            "(SELECT count(*)::integer",
            "FROM session_authority.operation_claims",
            "WHERE operation_id = $1) AS operation_count,",
            "(SELECT count(*)::integer",
            "FROM session_authority.reservations",
            "WHERE operation_id = $1) AS reservation_count,",
            "(SELECT count(*)::integer",
            "FROM session_authority.operation_id_registry",
            "WHERE operation_id IN ($1, $2)) AS registry_count",
          ].join(" "),
          [input.operationId, input.request.launchIntent.launchAttemptId],
        );
        assert.deepEqual(deniedState.rows, [
          {
            operation_count: 0,
            registry_count: 0,
            reservation_count: 0,
          },
        ]);
        const reserved = await authority.reserveOperation(input);
        assertOperationReceipt(reserved, "prepared");
        assert.equal(reserved.acquired, true);
        const gateClosedReplay =
          await fleetIncompatibleAuthority.reserveOperation(
            structuredClone(input),
          );
        assertOperationReceipt(gateClosedReplay, "prepared");
        assert.equal(gateClosedReplay.acquired, false);
        assert.deepEqual(gateClosedReplay.operation, reserved.operation);
        assert.deepEqual(
          gateClosedReplay.reservation,
          reserved.reservation,
        );
        assert.deepEqual(gateClosedReplay.session, reserved.session);
        const claimed =
          await authority.claimRestoreDestinationGenerationDispatch({
            ...structuredClone(input),
            destinationIsolationProofId:
              `restore-isolation-proof-${randomUUID()}`,
            expectedOperationRevision: "0",
            generationId: `restore-generation-${randomUUID()}`,
          });
        const handoffInput = {
          launch: launchIntent,
          restore: {
            ...structuredClone(input),
            completion: restoreGenerationCompletion(
              input,
              claimed,
              false,
            ),
            expectedOperationRevision: "1",
          },
        };
        const observer = await pool.connect();
        try {
          const authorizedState =
            await readRestoreGenerationTransactionState(
              observer,
              input.operationId,
            );

          await assert.rejects(
            authority.finalizeRestoreDestinationGenerationAndReserveWriterLaunchAttempt(
              {
                ...structuredClone(handoffInput),
                launch: {
                  ...structuredClone(launchIntent),
                  launchAttemptId: `writer-launch-${randomUUID()}`,
                },
              },
            ),
            assertAuthorityCode("invalid_operation_request"),
          );
          assert.deepEqual(
            await readRestoreGenerationTransactionState(
              observer,
              input.operationId,
            ),
            authorizedState,
          );

          const rollbackAuthority = new PostgresSessionAuthority({
            restoreGenerationV2FleetCompatible: true,
            store: new PostgresSerializableStore({
              dedicatedPool: firstMatchingQueryResultFailurePool(
                pool,
                "restore launch handoff rollback",
                (text) =>
                  text.startsWith(
                    "INSERT INTO session_authority.reservations",
                  ),
              ),
              maxTransactionAttempts: 1,
            }),
          });
          await assert.rejects(
            rollbackAuthority.finalizeRestoreDestinationGenerationAndReserveWriterLaunchAttempt(
              structuredClone(handoffInput),
            ),
            assertTransactionBoundaryLost,
          );
          assert.deepEqual(
            await readRestoreGenerationTransactionState(
              observer,
              input.operationId,
            ),
            authorizedState,
          );
          const rolledBack =
            await readRestoreLaunchHandoffTransactionState(
              observer,
              input.operationId,
              launchIntent.launchAttemptId,
            );
          assert.equal(rolledBack.generation_state, "authorized");
          assert.equal(rolledBack.generation_document, null);
          assert.equal(rolledBack.restore_operation_state, "starting");
          assert.equal(rolledBack.restore_reservation_state, "starting");
          assert.equal(rolledBack.launch_operation_kind, null);
          assert.equal(rolledBack.launch_operation_request, null);
          assert.equal(rolledBack.launch_reservation_state, null);

          const handedOff =
            await authority.finalizeRestoreDestinationGenerationAndReserveWriterLaunchAttempt(
              structuredClone(handoffInput),
            );
          assert.equal(handedOff.status, "prepared");
          assert.equal(handedOff.restore.finalized, true);
          assert.equal(handedOff.generation.state, "committed");
          assert.equal(handedOff.launch.operation.state, "prepared");
          assert.equal(
            handedOff.launch.attempt.launchAttemptId,
            launchIntent.launchAttemptId,
          );
          const committed =
            await readRestoreLaunchHandoffTransactionState(
              observer,
              input.operationId,
              launchIntent.launchAttemptId,
            );
          assert.equal(committed.handoff_times_match, true);
          assert.equal(committed.generation_state, "committed");
          assert.equal(committed.restore_operation_state, "committed");
          assert.equal(committed.restore_reservation_state, "released");
          assert.equal(committed.launch_operation_state, "prepared");
          assert.equal(committed.launch_reservation_state, "prepared");
          assert.deepEqual(
            committed.session_document,
            structuredClone(handedOff.session.document),
          );
        } finally {
          observer.release();
        }
      },
    );

    await t.test(
      "restore finalization atomically hands one prepared attempt to the launcher",
      async () => {
        const sessionId = randomUUID();
        sessionIds.push(sessionId);
        const image = integrationPlatformImageFixture();
        const fixture = await prepareRestoreGenerationFixture(
          authority,
          checkpointAuthority,
          sessionId,
          { imageDigest: image.descriptor.digest },
        );
        const imageReservations =
          new PlatformImageReservationCoordinator();
        const inspectCodex = async () => ({
          codexBinaryPath: "/opt/portable-codex/bin/codex",
          codexBinarySha256: "c".repeat(64),
          codexVersion:
            fixture.attached.session.document.manifest.runtime
              .codexVersion,
        });
        const reserved = await imageReservations.reservePlatformImage({
          configBytes: image.configBytes,
          descriptor: image.descriptor,
          inspectCodex,
          sessionManifest: fixture.attached.session.document.manifest,
        });
        const launchAttemptId = `writer-launch-${randomUUID()}`;
        const supervisorId = `supervisor-${randomUUID()}`;
        let launchCalls = 0;
        let launchedRequest = null;
        const facade = createPostgresLogicalWriterLauncher({
          authority,
          imageReservations,
          operationGuard,
          stoppedWriterCoordinator:
            new StoppedWriterCapabilityCoordinator(),
          supervisor: {
            contractVersion: 1,
            launchWriter: async (context) => {
              launchCalls += 1;
              launchedRequest = context.attempt.request;
              return {
                receiptVersion: 1,
                evidence: writerLaunchEvidence(
                  {
                    operationId: launchAttemptId,
                    request: context.attempt.request,
                  },
                  "started",
                ),
                stopWriter: async function stopWriter() {
                  return STOPPED_WRITER_STOP_CONFIRMED;
                },
              };
            },
            reconcileWriterLaunch: async () => {
              throw new Error(
                "a prepared handoff must not reconcile before launch",
              );
            },
            supervisorId,
          },
        });
        const imageReservation = {
          configBytes: image.configBytes,
          descriptor: image.descriptor,
          inspectCodex,
          reservation: reserved.reservation,
        };
        const launchIntent = await facade.prepareLaunchIntent({
          expectedSession: fixture.attached.session,
          imageReservation,
          launchAttemptId,
        });
        assert.equal(launchIntent.launchAttemptId, launchAttemptId);
        assert.equal(
          launchIntent.supervisor.supervisorId,
          supervisorId,
        );

        const admission = restoreGenerationAdmission(
          fixture.attached,
          fixture.checkpoint,
        );
        const input = restoreGenerationOperationInputV2(
          fixture.attached.session,
          admission,
          launchIntent,
        );
        await authority.reserveOperation(input);
        const claimed =
          await authority.claimRestoreDestinationGenerationDispatch({
            ...structuredClone(input),
            destinationIsolationProofId:
              `restore-isolation-proof-${randomUUID()}`,
            expectedOperationRevision: "0",
            generationId: `restore-generation-${randomUUID()}`,
          });
        const handoffInput = {
          launch: launchIntent,
          restore: {
            ...structuredClone(input),
            completion: restoreGenerationCompletion(
              input,
              claimed,
              false,
            ),
            expectedOperationRevision: "1",
          },
        };
        const acknowledgementLossAuthority =
          new PostgresSessionAuthority({
            restoreGenerationV2FleetCompatible: true,
            store: new PostgresSerializableStore({
              dedicatedPool: firstCommitAcknowledgementLossPool(pool),
            }),
          });
        await assert.rejects(
          acknowledgementLossAuthority.finalizeRestoreDestinationGenerationAndReserveWriterLaunchAttempt(
            handoffInput,
          ),
          assertCommitOutcomeUncertain,
        );
        const handedOff =
          await authority.finalizeRestoreDestinationGenerationAndReserveWriterLaunchAttempt(
            handoffInput,
          );

        assert.equal(handedOff.status, "prepared");
        assert.equal(handedOff.restore.finalized, false);
        assert.equal(handedOff.restore.operation.state, "committed");
        assert.equal(handedOff.generation.state, "committed");
        assert.equal(handedOff.launch.operation.state, "prepared");
        assert.equal(
          handedOff.launch.attempt.launchAttemptId,
          launchAttemptId,
        );
        assert.equal(
          BigInt(handedOff.launch.operation.expectedSession.revision),
          BigInt(claimed.session.revision) + 1n,
        );
        assert.equal(
          BigInt(handedOff.session.revision),
          BigInt(claimed.session.revision) + 2n,
        );
        assert.equal(
          handedOff.session.document.activeOperation.operationId,
          launchAttemptId,
        );
        assert.equal(
          handedOff.session.document.lastOperation.operationId,
          input.operationId,
        );
        assert.deepEqual(
          handedOff.launch.attempt.request.measuredImage,
          launchIntent.measuredImage,
        );
        assert.deepEqual(
          handedOff.launch.attempt.request.supervisor,
          launchIntent.supervisor,
        );
        assert.equal(
          handedOff.launch.attempt.request.generation.generationId,
          handedOff.generation.generationId,
        );
        assert.equal(
          handedOff.launch.attempt.request.generation.operationId,
          handedOff.generation.operationId,
        );

        const atomicState =
          await readRestoreLaunchHandoffTransactionState(
            pool,
            input.operationId,
            launchAttemptId,
          );
        assert.equal(atomicState.generation_state, "committed");
        assert.deepEqual(
          atomicState.generation_document,
          structuredClone(handedOff.generation.document),
        );
        assert.equal(atomicState.restore_operation_state, "committed");
        assert.equal(atomicState.restore_operation_revision, "2");
        assert.equal(
          atomicState.restore_operation_result.outcome,
          "restore-generation-committed",
        );
        assert.equal(atomicState.restore_reservation_state, "released");
        assert.equal(
          atomicState.launch_operation_kind,
          WRITER_LAUNCH_ATTEMPT_OPERATION_KIND,
        );
        assert.equal(atomicState.launch_operation_state, "prepared");
        assert.equal(atomicState.launch_operation_revision, "0");
        assert.equal(atomicState.launch_operation_result, null);
        assert.equal(atomicState.launch_operation_retired_at, null);
        assert.equal(atomicState.launch_reservation_state, "prepared");
        assert.equal(atomicState.launch_reservation_released_at, null);
        assert.equal(atomicState.handoff_times_match, true);
        assert.equal(
          atomicState.session_revision,
          handedOff.session.revision,
        );
        assert.deepEqual(
          atomicState.session_document,
          structuredClone(handedOff.session.document),
        );
        assert.deepEqual(
          atomicState.launch_operation_request.payload,
          structuredClone(handedOff.launch.attempt.request),
        );

        const handoffReplay =
          await authority.finalizeRestoreDestinationGenerationAndReserveWriterLaunchAttempt(
            structuredClone(handoffInput),
          );
        assert.equal(handoffReplay.status, "prepared");
        assert.equal(handoffReplay.restore.finalized, false);
        assert.deepEqual(handoffReplay.generation, handedOff.generation);
        assert.deepEqual(handoffReplay.launch, handedOff.launch);
        assert.deepEqual(handoffReplay.session, handedOff.session);

        const started = await facade.runPreparedLaunch({
          imageReservation,
          launchAttemptId,
        });
        assert.equal(started.status, "started");
        assert.notEqual(started.writer, null);
        assert.equal(launchCalls, 1);
        assert.deepEqual(
          JSON.parse(JSON.stringify(launchedRequest)),
          JSON.parse(JSON.stringify(handedOff.launch.attempt.request)),
        );

        const replayed = await facade.runPreparedLaunch({
          imageReservation,
          launchAttemptId,
        });
        assert.equal(replayed.status, "started");
        assert.strictEqual(replayed.writer, started.writer);
        assert.equal(launchCalls, 1);

        const read = await authority.readWriterLaunchAttempt({
          operationId: launchAttemptId,
        });
        assertOperationReceipt(read, "committed");
        assert.equal(
          read.operation.result.outcome,
          "writer-launch-started",
        );
        assert.deepEqual(
          JSON.parse(JSON.stringify(read.session.document.launch)),
          JSON.parse(JSON.stringify(started.launch)),
        );
      },
    );

    await t.test(
      "writer launch binds a committed restore, upgrades v2 history, and replays one started attempt",
      async () => {
        const sessionId = randomUUID();
        sessionIds.push(sessionId);
        const fixture = await prepareCommittedRestoreGenerationFixture(
          authority,
          checkpointAuthority,
          sessionId,
        );
        const versionTwoDocument = structuredClone(
          fixture.finalized.session.document,
        );
        versionTwoDocument.documentVersion = 2;
        await pool.query(
          [
            "UPDATE session_authority.sessions",
            "SET document = $2::jsonb",
            "WHERE session_id = $1",
          ].join(" "),
          [sessionId, JSON.stringify(versionTwoDocument)],
        );

        const historical = await authority.readSession({ sessionId });
        assert.equal(historical.document.documentVersion, 2);
        assert.equal(
          historical.document.lastOperation.operationId,
          fixture.input.operationId,
        );
        const historicalGeneration =
          await authority.readRestoreDestinationGeneration({
            checkpoint: fixture.admission.checkpoint,
            generationId: fixture.finalized.generation.generationId,
            request: fixture.admission.request,
          });
        assertOperationReceipt(historicalGeneration, "committed");
        assert.deepEqual(
          historicalGeneration.generation,
          fixture.finalized.generation,
        );
        assert.deepEqual(
          (await readMigrationLedger(pool)).map(({ version }) => version),
          [1, 2, 3, 4, 5, 6],
        );

        const input = writerLaunchAttemptInput(
          historical,
          fixture.finalized.generation,
        );
        const reserved = await authority.reserveOperation(input);
        assertOperationReceipt(reserved, "prepared");
        assert.equal(historical.document.documentVersion, 2);
        assert.equal(
          reserved.session.document.documentVersion,
          SESSION_AUTHORITY_DOCUMENT_VERSION,
        );
        assert.equal(SESSION_AUTHORITY_DOCUMENT_VERSION, 3);

        const claimInput = {
          ...structuredClone(input),
          expectedOperationRevision: "0",
        };
        const claimed =
          await authority.claimWriterLaunchAttemptDispatch(claimInput);
        assertOperationReceipt(claimed, "starting");
        assert.equal(claimed.dispatchGranted, true);
        assert.deepEqual(
          claimed.generation,
          fixture.finalized.generation,
        );

        const evidence = writerLaunchEvidence(input, "started");
        const finalization = {
          ...structuredClone(input),
          evidence,
          expectedOperationRevision: "1",
        };
        const finalized =
          await authority.finalizeWriterLaunchAttemptStarted(
            finalization,
          );
        assertOperationReceipt(finalized, "committed");
        assert.equal(finalized.finalized, true);
        assert.equal(
          finalized.operation.result.outcome,
          "writer-launch-started",
        );
        assert.equal(
          finalized.session.document.launch.launchAttemptId,
          input.operationId,
        );
        assert.equal(
          finalized.session.document.launch.processIncarnationId,
          evidence.processIncarnationId,
        );
        assert.equal(
          finalized.session.document.launch.writerIncarnationId,
          evidence.writerIncarnationId,
        );
        assert.deepEqual(finalized.launch, finalized.session.document.launch);

        const read = await authority.readWriterLaunchAttempt({
          operationId: input.operationId,
        });
        assertOperationReceipt(read, "committed");
        assert.equal(read.attempt.launchAttemptId, input.operationId);
        assert.equal(read.attempt.state, "committed");
        assert.deepEqual(read.attempt.request, input.request);
        assert.deepEqual(read.launch, finalized.launch);

        const claimReplay =
          await authority.claimWriterLaunchAttemptDispatch(
            structuredClone(claimInput),
          );
        assertOperationReceipt(claimReplay, "committed");
        assert.equal(claimReplay.dispatchGranted, false);
        const finalizeReplay =
          await authority.finalizeWriterLaunchAttemptStarted(
            structuredClone(finalization),
          );
        assertOperationReceipt(finalizeReplay, "committed");
        assert.equal(finalizeReplay.finalized, false);
        assert.deepEqual(finalizeReplay.launch, finalized.launch);

        await assert.rejects(
          authority.reserveOperation({
            ...structuredClone(input),
            expectedSession: finalized.session,
            operationId: `writer-launch-${randomUUID()}`,
          }),
          assertAuthorityCode("invalid_operation_request"),
        );
      },
    );

    await t.test(
      "writer launch readback survives renewal, checkpoint, and blocked fencing until exact fence success",
      async () => {
        const sessionId = randomUUID();
        sessionIds.push(sessionId);
        const fixture = await prepareCommittedRestoreGenerationFixture(
          authority,
          checkpointAuthority,
          sessionId,
        );
        const input = writerLaunchAttemptInput(
          fixture.finalized.session,
          fixture.finalized.generation,
        );
        await authority.reserveOperation(input);
        const claimed =
          await authority.claimWriterLaunchAttemptDispatch({
            ...structuredClone(input),
            expectedOperationRevision: "0",
          });
        assertOperationReceipt(claimed, "starting");
        const evidence = writerLaunchEvidence(input, "started");
        const started =
          await authority.finalizeWriterLaunchAttemptStarted({
            ...structuredClone(input),
            evidence,
            expectedOperationRevision: "1",
          });
        assertOperationReceipt(started, "committed");
        const launch = structuredClone(started.launch);

        async function assertCurrentLaunch(lastOperationId) {
          const currentSession = await authority.readSession({
            sessionId,
          });
          const currentAttempt =
            await authority.readWriterLaunchAttempt({
              operationId: input.operationId,
            });
          assert.deepEqual(currentSession.document.launch, launch);
          assert.deepEqual(currentAttempt.launch, launch);
          assert.equal(
            currentSession.document.lastOperation.operationId,
            lastOperationId,
          );
          assert.equal(
            currentAttempt.session.document.lastOperation.operationId,
            lastOperationId,
          );
          assert.equal(
            currentAttempt.operation.operationId,
            input.operationId,
          );
          assert.equal(
            currentAttempt.operation.result.outcome,
            "writer-launch-started",
          );
          return currentSession;
        }

        await assertCurrentLaunch(input.operationId);

        const renewalInput = writerLeaseRenewalInput(started.session);
        const renewed = await authority.renewWriterLease(renewalInput);
        assertOperationReceipt(renewed, "committed");
        assert.deepEqual(renewed.session.document.launch, launch);
        const renewedSession = await assertCurrentLaunch(
          renewalInput.operationId,
        );

        const checkpointAdmission = checkpointCaptureAdmission(
          { session: renewedSession },
          {
            processIncarnationId: evidence.processIncarnationId,
            writerIncarnationId: evidence.writerIncarnationId,
          },
        );
        await checkpointAuthority.runCapture(
          checkpointAdmission,
          async (context) => checkpointCompletion(context, false),
        );
        const checkpointTerminal = await authority.reconcileOperation(
          checkpointOperationInput(
            renewedSession,
            checkpointAdmission,
          ),
        );
        assertOperationReceipt(checkpointTerminal, "committed");
        assert.deepEqual(
          checkpointTerminal.session.document.launch,
          launch,
        );
        const checkpointSession = await assertCurrentLaunch(
          checkpointAdmission.request.operationId,
        );

        const firstFenceInput = writerForceFenceInput(
          checkpointSession,
        );
        await authority.reserveOperation(firstFenceInput);
        const firstFenceStarting =
          await authority.claimWriterForceFenceDispatch({
            ...structuredClone(firstFenceInput),
            expectedOperationRevision: "0",
          });
        assertOperationReceipt(firstFenceStarting, "starting");
        const firstFenceUncertain =
          await authority.markOperationUncertain({
            ...structuredClone(firstFenceInput),
            expectedOperationRevision: "1",
          });
        assertOperationReceipt(firstFenceUncertain, "uncertain");
        const blocked =
          await authority.finalizeWriterOperationBlocked({
            ...structuredClone(firstFenceInput),
            expectedOperationRevision: "2",
            reason: "provider-outcome-unresolved",
          });
        assertOperationReceipt(blocked, "committed");
        assert.equal(blocked.session.document.lifecycle, "BLOCKED");
        assert.deepEqual(blocked.session.document.launch, launch);
        const blockedSession = await assertCurrentLaunch(
          firstFenceInput.operationId,
        );

        const exactFenceInput = writerForceFenceInput(blockedSession);
        await authority.reserveOperation(exactFenceInput);
        const exactFenceStarting =
          await authority.claimWriterForceFenceDispatch({
            ...structuredClone(exactFenceInput),
            expectedOperationRevision: "0",
          });
        assertOperationReceipt(exactFenceStarting, "starting");
        const fenced = await authority.finalizeWriterForceFence({
          ...structuredClone(exactFenceInput),
          expectedOperationRevision: "1",
          fenceResult: forceFenceEvidence(
            exactFenceStarting.fenceRequest,
          ),
        });
        assertOperationReceipt(fenced, "committed");
        assert.equal(fenced.session.document.lifecycle, "DETACHED");
        assert.equal(fenced.session.document.launch, null);

        const detachedSession = await authority.readSession({
          sessionId,
        });
        const historicalAttempt =
          await authority.readWriterLaunchAttempt({
            operationId: input.operationId,
          });
        assert.equal(detachedSession.document.launch, null);
        assert.equal(historicalAttempt.launch, null);
        assert.equal(
          detachedSession.document.lastOperation.operationId,
          exactFenceInput.operationId,
        );
        assert.equal(
          historicalAttempt.session.document.lastOperation.operationId,
          exactFenceInput.operationId,
        );
        assert.equal(
          historicalAttempt.operation.result.outcome,
          "writer-launch-started",
        );
      },
    );

    await t.test(
      "writer launch exact finalization remains available after lease expiry",
      async (t) => {
        for (const scenario of [
          {
            finalizer: "started",
            markUncertain: false,
            operationRevision: "1",
            status: "started",
          },
          {
            finalizer: "stopped",
            markUncertain: true,
            operationRevision: "2",
            status: "complete-stopped",
          },
        ]) {
          await t.test(scenario.status, async () => {
            const sessionId = randomUUID();
            sessionIds.push(sessionId);
            const fixture =
              await prepareCommittedRestoreGenerationFixture(
                authority,
                checkpointAuthority,
                sessionId,
                {
                  finalAttachmentLeaseDurationMilliseconds: 3_000,
                },
              );
            const input = writerLaunchAttemptInput(
              fixture.finalized.session,
              fixture.finalized.generation,
            );
            await authority.reserveOperation(input);
            const starting =
              await authority.claimWriterLaunchAttemptDispatch({
                ...structuredClone(input),
                expectedOperationRevision: "0",
              });
            assertOperationReceipt(starting, "starting");

            if (scenario.markUncertain) {
              const uncertain = await authority.markOperationUncertain({
                ...structuredClone(input),
                expectedOperationRevision: "1",
              });
              assertOperationReceipt(uncertain, "uncertain");
            }

            await waitForDatabaseLeaseExpiry(
              pool,
              fixture.finalized.session.document.lease.expiresAt,
            );
            const finalization = {
              ...structuredClone(input),
              evidence: writerLaunchEvidence(input, scenario.status),
              expectedOperationRevision: scenario.operationRevision,
            };
            const finalized =
              scenario.finalizer === "started"
                ? await authority.finalizeWriterLaunchAttemptStarted(
                    finalization,
                  )
                : await authority.finalizeWriterLaunchAttemptStopped(
                    finalization,
                  );
            assertOperationReceipt(finalized, "committed");
            assert.equal(finalized.finalized, true);
            assert.equal(
              finalized.operation.result.evidence.status,
              scenario.status,
            );
            assert.equal(
              finalized.launch === null,
              scenario.status === "complete-stopped",
            );
          });
        }
      },
    );

    await t.test(
      "writer launch stop proofs replay exactly and release authority for a replacement attempt",
      async (t) => {
        for (const scenario of [
          {
            expectedOperationRevision: "1",
            markUncertain: false,
            outcome: "writer-launch-not-started",
            status: "not-started",
          },
          {
            expectedOperationRevision: "2",
            markUncertain: true,
            outcome: "writer-launch-complete-stopped",
            status: "complete-stopped",
          },
        ]) {
          await t.test(scenario.status, async () => {
            const sessionId = randomUUID();
            sessionIds.push(sessionId);
            const fixture =
              await prepareCommittedRestoreGenerationFixture(
                authority,
                checkpointAuthority,
                sessionId,
              );
            const input = writerLaunchAttemptInput(
              fixture.finalized.session,
              fixture.finalized.generation,
            );
            await authority.reserveOperation(input);
            const claimed =
              await authority.claimWriterLaunchAttemptDispatch({
                ...structuredClone(input),
                expectedOperationRevision: "0",
              });
            assertOperationReceipt(claimed, "starting");

            if (scenario.markUncertain) {
              const uncertain = await authority.markOperationUncertain({
                ...structuredClone(input),
                expectedOperationRevision: "1",
              });
              assertOperationReceipt(uncertain, "uncertain");
            }

            const finalization = {
              ...structuredClone(input),
              evidence: writerLaunchEvidence(input, scenario.status),
              expectedOperationRevision:
                scenario.expectedOperationRevision,
            };
            const stopped =
              await authority.finalizeWriterLaunchAttemptStopped(
                finalization,
              );
            assertOperationReceipt(stopped, "committed");
            assert.equal(stopped.finalized, true);
            assert.equal(stopped.launch, null);
            assert.equal(stopped.session.document.launch, null);
            assert.equal(
              stopped.operation.result.outcome,
              scenario.outcome,
            );

            const replay =
              await authority.finalizeWriterLaunchAttemptStopped(
                structuredClone(finalization),
              );
            assertOperationReceipt(replay, "committed");
            assert.equal(replay.finalized, false);
            assert.deepEqual(replay.operation, stopped.operation);

            const replacementInput = writerLaunchAttemptInput(
              stopped.session,
              fixture.finalized.generation,
            );
            const replacement =
              await authority.reserveOperation(replacementInput);
            assertOperationReceipt(replacement, "prepared");
            assert.equal(replacement.acquired, true);
          });
        }
      },
    );

    await t.test(
      "writer launch claim and finalize acknowledgement loss recover without regranting",
      async () => {
        const sessionId = randomUUID();
        sessionIds.push(sessionId);
        const fixture = await prepareCommittedRestoreGenerationFixture(
          authority,
          checkpointAuthority,
          sessionId,
        );
        const input = writerLaunchAttemptInput(
          fixture.finalized.session,
          fixture.finalized.generation,
        );
        await authority.reserveOperation(input);
        const claimInput = {
          ...structuredClone(input),
          expectedOperationRevision: "0",
        };
        const claimLossAuthority = new PostgresSessionAuthority({
          store: new PostgresSerializableStore({
            dedicatedPool: firstCommitAcknowledgementLossPool(pool),
          }),
        });

        await assert.rejects(
          claimLossAuthority.claimWriterLaunchAttemptDispatch(
            claimInput,
          ),
          assertCommitOutcomeUncertain,
        );
        const restarted = new PostgresSessionAuthority({ store });
        const starting = await restarted.readWriterLaunchAttempt({
          operationId: input.operationId,
        });
        assertOperationReceipt(starting, "starting");
        assert.equal(starting.attempt.state, "starting");
        const claimReplay =
          await restarted.claimWriterLaunchAttemptDispatch(
            structuredClone(claimInput),
          );
        assertOperationReceipt(claimReplay, "starting");
        assert.equal(claimReplay.dispatchGranted, false);

        const finalization = {
          ...structuredClone(input),
          evidence: writerLaunchEvidence(input, "started"),
          expectedOperationRevision: "1",
        };
        const finalizeLossAuthority = new PostgresSessionAuthority({
          store: new PostgresSerializableStore({
            dedicatedPool: firstCommitAcknowledgementLossPool(pool),
          }),
        });
        await assert.rejects(
          finalizeLossAuthority.finalizeWriterLaunchAttemptStarted(
            finalization,
          ),
          assertCommitOutcomeUncertain,
        );

        const committed = await restarted.readWriterLaunchAttempt({
          operationId: input.operationId,
        });
        assertOperationReceipt(committed, "committed");
        assert.equal(
          committed.operation.result.outcome,
          "writer-launch-started",
        );
        assert.equal(
          committed.launch.launchAttemptId,
          input.operationId,
        );
        const finalizeReplay =
          await restarted.finalizeWriterLaunchAttemptStarted(
            structuredClone(finalization),
          );
        assertOperationReceipt(finalizeReplay, "committed");
        assert.equal(finalizeReplay.finalized, false);
        assert.deepEqual(finalizeReplay.launch, committed.launch);
      },
    );

    await t.test(
      "writer launch stop survives acknowledgement loss and clears one exact current launch",
      async () => {
        const existingRows = await pool.query(
          [
            "SELECT session_id::text AS session_id",
            "FROM session_authority.sessions",
            "ORDER BY session_id",
          ].join(" "),
        );
        const consecutive = consecutiveFreshSessionIds(
          existingRows.rows.map(({ session_id: sessionId }) => sessionId),
          3,
        );
        const [
          detachedSessionId,
          launchedSessionId,
          lookaheadSessionId,
        ] = consecutive.sessionIds;
        sessionIds.push(
          detachedSessionId,
          launchedSessionId,
          lookaheadSessionId,
        );
        await authority.registerSession(
          registrationInput(detachedSessionId),
        );
        // Keep the limit+1 lookahead independent from intentionally corrupt
        // rows retained by other fail-closed integration cases.
        await authority.registerSession(
          registrationInput(lookaheadSessionId),
        );
        const fixture = await prepareCommittedRestoreGenerationFixture(
          authority,
          checkpointAuthority,
          launchedSessionId,
          { finalAttachmentLeaseDurationMilliseconds: 3_000 },
        );
        const launchInput = writerLaunchAttemptInput(
          fixture.finalized.session,
          fixture.finalized.generation,
        );
        await authority.reserveOperation(launchInput);
        const launchClaim =
          await authority.claimWriterLaunchAttemptDispatch({
            ...structuredClone(launchInput),
            expectedOperationRevision: "0",
          });
        assertOperationReceipt(launchClaim, "starting");
        const launchEvidence = writerLaunchEvidence(
          launchInput,
          "started",
        );
        const launched =
          await authority.finalizeWriterLaunchAttemptStarted({
            ...structuredClone(launchInput),
            evidence: launchEvidence,
            expectedOperationRevision: "1",
          });
        assertOperationReceipt(launched, "committed");

        const sparsePage =
          await authority.listCurrentWriterLaunchRecoveryCandidates({
            afterSessionId: consecutive.afterSessionId,
            limit: 1,
          });
        assert.deepEqual(sparsePage, {
          candidates: [],
          nextAfterSessionId: detachedSessionId,
        });
        const currentPage =
          await authority.listCurrentWriterLaunchRecoveryCandidates({
            afterSessionId: sparsePage.nextAfterSessionId,
            limit: 1,
          });
        assert.equal(currentPage.candidates.length, 1);
        assert.equal(
          currentPage.nextAfterSessionId,
          launchedSessionId,
        );
        assert.deepEqual(currentPage.candidates[0], {
          launch: launched.launch,
          launchAttemptId: launchInput.operationId,
          request: launchInput.request,
        });

        const originalBefore = await pool.query(
          [
            "SELECT request, result, state, revision, created_at, updated_at, retired_at",
            "FROM session_authority.operation_claims",
            "WHERE operation_id = $1",
          ].join(" "),
          [launchInput.operationId],
        );
        assert.equal(originalBefore.rows.length, 1);
        const {
          claimToken: stopClaimToken,
          input: stopInput,
        } = writerLaunchStopInput(launched.session);
        const prepared = await authority.reserveOperation(stopInput);
        assertOperationReceipt(prepared, "prepared");

        await waitForDatabaseLeaseExpiry(
          pool,
          launched.session.document.lease.expiresAt,
        );
        const claimInput = {
          ...structuredClone(stopInput),
          claimToken: stopClaimToken,
          expectedOperationRevision: "0",
        };
        const claimLossAuthority = new PostgresSessionAuthority({
          store: new PostgresSerializableStore({
            dedicatedPool: firstCommitAcknowledgementLossPool(pool),
          }),
        });
        await assert.rejects(
          claimLossAuthority.claimWriterLaunchStopDispatch(claimInput),
          assertCommitOutcomeUncertain,
        );
        const claimReplay =
          await authority.claimWriterLaunchStopDispatch(
            structuredClone(claimInput),
          );
        assertOperationReceipt(claimReplay, "starting");
        assert.equal(claimReplay.dispatchGranted, false);
        assert.equal(claimReplay.claimTokenMatched, true);
        assert.deepEqual(claimReplay.session.document.launch, launched.launch);
        const wrongTokenReplay =
          await authority.claimWriterLaunchStopDispatch({
            ...structuredClone(stopInput),
            claimToken: randomUUID(),
            expectedOperationRevision: "0",
          });
        assertOperationReceipt(wrongTokenReplay, "starting");
        assert.equal(wrongTokenReplay.dispatchGranted, false);
        assert.equal(wrongTokenReplay.claimTokenMatched, false);
        const reconciledClaim =
          await authority.reconcileWriterLaunchStopOperation({
            ...structuredClone(stopInput),
            claimToken: stopClaimToken,
          });
        assertOperationReceipt(reconciledClaim, "starting");
        assert.equal(reconciledClaim.claimTokenMatched, true);

        const uncertain = await authority.markOperationUncertain({
          ...structuredClone(stopInput),
          expectedOperationRevision: "1",
        });
        assertOperationReceipt(uncertain, "uncertain");
        assert.deepEqual(uncertain.session.document.launch, launched.launch);
        const stopEvidence = writerLaunchStopEvidence(stopInput);
        await assert.rejects(
          authority.finalizeWriterLaunchStopped({
            ...structuredClone(stopInput),
            evidence: {
              ...stopEvidence,
              processIncarnationId: `process-mismatch-${randomUUID()}`,
            },
            expectedOperationRevision: "2",
          }),
          assertAuthorityCode("invalid_operation_request"),
        );

        const finalization = {
          ...structuredClone(stopInput),
          evidence: stopEvidence,
          expectedOperationRevision: "2",
        };
        const finalizeLossAuthority = new PostgresSessionAuthority({
          store: new PostgresSerializableStore({
            dedicatedPool: firstCommitAcknowledgementLossPool(pool),
          }),
        });
        await assert.rejects(
          finalizeLossAuthority.finalizeWriterLaunchStopped(finalization),
          assertCommitOutcomeUncertain,
        );
        const committed = await authority.reconcileOperation(stopInput);
        assertOperationReceipt(committed, "committed");
        assert.equal(
          committed.operation.result.outcome,
          "writer-launch-stopped",
        );
        assert.equal(committed.session.document.launch, null);
        assert.equal(
          committed.session.document.lastOperation.operationId,
          stopInput.operationId,
        );
        const replay = await authority.finalizeWriterLaunchStopped(
          structuredClone(finalization),
        );
        assertOperationReceipt(replay, "committed");
        assert.equal(replay.finalized, false);
        assert.equal(replay.launch, null);
        assert.deepEqual(replay.operation.result, committed.operation.result);

        await assert.rejects(
          authority.finalizeWriterLaunchStopped({
            ...structuredClone(stopInput),
            evidence: {
              ...stopEvidence,
              proofId: `stop-proof-mismatch-${randomUUID()}`,
            },
            expectedOperationRevision: "2",
          }),
          assertAuthorityCode("operation_result_conflict"),
        );
        await assert.rejects(
          authority.finalizeWriterLaunchStopped({
            ...structuredClone(stopInput),
            evidence: stopEvidence,
            expectedOperationRevision: "1",
          }),
          assertAuthorityCode("operation_transition_conflict"),
        );

        const originalAfter = await pool.query(
          [
            "SELECT request, result, state, revision, created_at, updated_at, retired_at",
            "FROM session_authority.operation_claims",
            "WHERE operation_id = $1",
          ].join(" "),
          [launchInput.operationId],
        );
        assert.deepEqual(originalAfter.rows, originalBefore.rows);
        const historical = await authority.readWriterLaunchAttempt({
          operationId: launchInput.operationId,
        });
        assertOperationReceipt(historical, "committed", {
          currentTerminal: false,
        });
        assert.equal(
          historical.operation.result.outcome,
          "writer-launch-started",
        );
        assert.equal(historical.launch, null);

        const corruptedResult = structuredClone(
          originalBefore.rows[0].result,
        );
        corruptedResult.evidence.proofId =
          `corrupt-start-proof-${randomUUID()}`;
        await pool.query(
          [
            "UPDATE session_authority.operation_claims",
            "SET result = $2::jsonb",
            "WHERE operation_id = $1",
          ].join(" "),
          [launchInput.operationId, JSON.stringify(corruptedResult)],
        );
        try {
          await assert.rejects(
            authority.readSession({
              sessionId: launchedSessionId,
            }),
            assertAuthorityCode("operation_state_invalid"),
          );
        } finally {
          await pool.query(
            [
              "UPDATE session_authority.operation_claims",
              "SET result = $2::jsonb",
              "WHERE operation_id = $1",
            ].join(" "),
            [
              launchInput.operationId,
              JSON.stringify(originalBefore.rows[0].result),
            ],
          );
        }
        const restored = await authority.readSession({
          sessionId: launchedSessionId,
        });
        assert.equal(restored.document.launch, null);
      },
    );

    await t.test(
      "writer stop V3 atomically hands off one prepared checkpoint capture",
      async () => {
        const sessionId = randomUUID();
        sessionIds.push(sessionId);
        const fixture = await prepareCommittedRestoreGenerationFixture(
          authority,
          checkpointAuthority,
          sessionId,
        );
        const launchInput = writerLaunchAttemptInput(
          fixture.finalized.session,
          fixture.finalized.generation,
        );
        await authority.reserveOperation(launchInput);
        await authority.claimWriterLaunchAttemptDispatch({
          ...structuredClone(launchInput),
          expectedOperationRevision: "0",
        });
        const launched =
          await authority.finalizeWriterLaunchAttemptStarted({
            ...structuredClone(launchInput),
            evidence: writerLaunchEvidence(launchInput, "started"),
            expectedOperationRevision: "1",
          });

        const handoff = writerLaunchStopCaptureInput(launched.session);
        const preparedStop = await authority.reserveOperation(
          handoff.input,
        );
        assertOperationReceipt(preparedStop, "prepared");
        const claimedStop =
          await authority.claimWriterLaunchStopDispatch({
            ...structuredClone(handoff.input),
            claimToken: handoff.claimToken,
            expectedOperationRevision: "0",
          });
        assertOperationReceipt(claimedStop, "starting");
        assert.equal(claimedStop.dispatchGranted, true);

        const finalization = {
          ...structuredClone(handoff.input),
          evidence: writerLaunchStopEvidence(handoff.input),
          expectedOperationRevision: "1",
        };
        const acknowledgementLossAuthority =
          new PostgresSessionAuthority({
            store: new PostgresSerializableStore({
              dedicatedPool: firstCommitAcknowledgementLossPool(pool),
            }),
          });
        await assert.rejects(
          acknowledgementLossAuthority
            .finalizeWriterLaunchStoppedAndReserveCheckpointCapture(
              finalization,
            ),
          assertCommitOutcomeUncertain,
        );

        const reconciled =
          await authority.reconcileWriterLaunchStopOperation({
            ...structuredClone(handoff.input),
            claimToken: handoff.claimToken,
          });
        assert.equal(reconciled.status, "prepared");
        assert.equal(reconciled.claimTokenMatched, true);
        assert.equal(reconciled.stop.finalized, false);
        assert.equal(reconciled.stop.operation.state, "committed");
        assert.equal(reconciled.capture.operation.state, "prepared");
        assert.equal(
          reconciled.session.document.activeOperation.operationId,
          handoff.captureInput.operationId,
        );
        assert.equal(reconciled.session.document.launch, null);
        assertWriterLaunchStopCaptureHandoffProof({
          before: handoff.input.expectedSession,
          capture: reconciled.capture,
          session: reconciled.session,
          stop: {
            operation: reconciled.stop.operation,
            reservation: reconciled.stop.reservation,
          },
        });

        const replay =
          await authority
            .finalizeWriterLaunchStoppedAndReserveCheckpointCapture(
              structuredClone(finalization),
            );
        assert.equal(replay.status, "prepared");
        assert.equal(replay.stop.finalized, false);
        assert.deepEqual(replay.capture, reconciled.capture);

        const transactionEvidence = await pool.query(
          [
            "SELECT transaction_id FROM (",
            "SELECT xmin::text AS transaction_id",
            "FROM session_authority.sessions WHERE session_id = $1",
            "UNION ALL SELECT xmin::text",
            "FROM session_authority.operation_claims",
            "WHERE operation_id = ANY($2::character varying[])",
            "UNION ALL SELECT xmin::text",
            "FROM session_authority.reservations",
            "WHERE operation_id = ANY($2::character varying[])",
            "UNION ALL SELECT xmin::text",
            "FROM session_authority.operation_id_registry",
            "WHERE operation_id = $3",
            ") AS evidence",
          ].join(" "),
          [
            sessionId,
            [handoff.input.operationId, handoff.captureInput.operationId],
            handoff.captureInput.operationId,
          ],
        );
        assert.equal(transactionEvidence.rows.length, 6);
        assert.equal(
          new Set(
            transactionEvidence.rows.map(
              ({ transaction_id: transactionId }) => transactionId,
            ),
          ).size,
          1,
        );

        const predecessor = await pool.query(
          [
            "SELECT session_id::text AS session_id",
            "FROM session_authority.sessions",
            "WHERE session_id < $1",
            "ORDER BY session_id DESC LIMIT 1",
          ].join(" "),
          [sessionId],
        );
        const recoveryCursor =
          predecessor.rows[0]?.session_id ?? null;
        const preparedPage =
          await authority.listCheckpointCaptureRecoveryCandidates({
            afterSessionId: recoveryCursor,
            limit: 1,
          });
        assert.deepEqual(
          structuredClone(preparedPage.candidates),
          structuredClone([
            {
              checkpoint: handoff.captureAdmission.checkpoint,
              request: handoff.captureAdmission.request,
              state: "prepared",
            },
          ]),
        );
        const preparedRead =
          await authority.readCheckpointCaptureAttempt({
            checkpoint: handoff.captureAdmission.checkpoint,
            request: handoff.captureAdmission.request,
          });
        assert.equal(preparedRead.status, "prepared");
        assert.equal(preparedRead.attempt, null);
        assert.equal(preparedRead.catalogue, null);

        const captureDispatchInput = {
          expectedSession:
            reconciled.capture.operation.expectedSession,
          expectedOperationRevision: "0",
          kind: CHECKPOINT_CAPTURE_OPERATION_KIND,
          operationId: handoff.captureInput.operationId,
          request: handoff.captureInput.request,
        };
        const claimedCapture =
          await authority.claimCheckpointCaptureDispatch(
            captureDispatchInput,
          );
        assert.equal(claimedCapture.dispatchGranted, true);
        assert.equal(claimedCapture.operation.state, "starting");
        const claimReplay =
          await authority.claimCheckpointCaptureDispatch(
            structuredClone(captureDispatchInput),
          );
        assert.equal(claimReplay.dispatchGranted, false);
        assert.equal(claimReplay.operation.state, "starting");

        const startingPage =
          await authority.listCheckpointCaptureRecoveryCandidates({
            afterSessionId: recoveryCursor,
            limit: 1,
          });
        assert.equal(startingPage.candidates.length, 1);
        assert.equal(startingPage.candidates[0].state, "starting");
        assert.equal(
          startingPage.candidates.some(({ state }) => state === "prepared"),
          false,
        );
      },
    );

    await t.test(
      "writer launch recovery is bounded and concurrent claims grant once",
      async () => {
        const existingRows = await pool.query(
          [
            "SELECT session_id::text AS session_id",
            "FROM session_authority.sessions",
            "ORDER BY session_id",
          ].join(" "),
        );
        const consecutive = consecutiveFreshSessionIds(
          existingRows.rows.map(({ session_id: sessionId }) => sessionId),
          3,
        );
        const orderedSessionIds = consecutive.sessionIds;
        sessionIds.push(...orderedSessionIds);
        const inputs = [];
        for (const sessionId of orderedSessionIds) {
          const fixture =
            await prepareCommittedRestoreGenerationFixture(
              authority,
              checkpointAuthority,
              sessionId,
            );
          const input = writerLaunchAttemptInput(
            fixture.finalized.session,
            fixture.finalized.generation,
          );
          inputs.push(input);
          await authority.reserveOperation(input);
        }

        const concurrentAuthority = new PostgresSessionAuthority({
          store: new PostgresSerializableStore({
            dedicatedPool: firstSessionLockQueryBarrierPool(
              pool,
              2,
              "writer launch dispatch claim barrier",
            ),
            maxTransactionAttempts: 3,
          }),
        });
        const concurrentClaimInput = {
          ...structuredClone(inputs[0]),
          expectedOperationRevision: "0",
        };
        const concurrentReceipts = await Promise.all([
          concurrentAuthority.claimWriterLaunchAttemptDispatch(
            concurrentClaimInput,
          ),
          concurrentAuthority.claimWriterLaunchAttemptDispatch(
            structuredClone(concurrentClaimInput),
          ),
        ]);
        assert.equal(
          concurrentReceipts.filter(
            ({ dispatchGranted }) => dispatchGranted,
          ).length,
          1,
        );
        assert.equal(
          concurrentReceipts.filter(
            ({ dispatchGranted }) => !dispatchGranted,
          ).length,
          1,
        );
        assert.deepEqual(
          concurrentReceipts[0].operation,
          concurrentReceipts[1].operation,
        );

        const uncertainStarting =
          await authority.claimWriterLaunchAttemptDispatch({
            ...structuredClone(inputs[1]),
            expectedOperationRevision: "0",
          });
        assertOperationReceipt(uncertainStarting, "starting");
        const uncertain = await authority.markOperationUncertain({
          ...structuredClone(inputs[1]),
          expectedOperationRevision: "1",
        });
        assertOperationReceipt(uncertain, "uncertain");

        const firstPage =
          await authority.listWriterLaunchAttemptRecoveryCandidates({
            afterSessionId: consecutive.afterSessionId,
            limit: 1,
          });
        assert.deepEqual(firstPage.candidates, [
          {
            launchAttemptId: inputs[0].operationId,
            request: inputs[0].request,
            state: "starting",
          },
        ]);
        assert.equal(
          firstPage.nextAfterSessionId,
          orderedSessionIds[0],
        );

        const secondPage =
          await authority.listWriterLaunchAttemptRecoveryCandidates({
            afterSessionId: firstPage.nextAfterSessionId,
            limit: 1,
          });
        assert.deepEqual(secondPage.candidates, [
          {
            launchAttemptId: inputs[1].operationId,
            request: inputs[1].request,
            state: "uncertain",
          },
        ]);
        assert.equal(
          secondPage.nextAfterSessionId,
          orderedSessionIds[1],
        );

        const terminalPage =
          await authority.listWriterLaunchAttemptRecoveryCandidates({
            afterSessionId: orderedSessionIds[1],
            limit: 1,
          });
        assert.deepEqual(terminalPage.candidates, [
          {
            launchAttemptId: inputs[2].operationId,
            request: inputs[2].request,
            state: "prepared",
          },
        ]);
        assert.equal(
          terminalPage.nextAfterSessionId === null ||
            terminalPage.nextAfterSessionId === orderedSessionIds[2],
          true,
        );
        assert.equal(
          inputs[2].operationId ===
            firstPage.candidates[0].launchAttemptId ||
            inputs[2].operationId ===
              secondPage.candidates[0].launchAttemptId,
          false,
        );
      },
    );
  },
);

test(
  "restore lifecycle guard schedules one exclusive database-global recovery pass",
  { timeout: 30_000 },
  async (t) => {
    const pool = new Pool({
      application_name:
        "portable-codex-runtime-restore-scheduler-integration-test",
      connectionString: databaseUrl,
      max: 2,
    });
    const lifecyclePool = new Pool({
      application_name:
        "portable-codex-runtime-restore-lifecycle-guard-integration-test",
      connectionString: databaseUrl,
      max: 4,
    });
    const recoveryScopeId = `integration-restore-${randomUUID()}`;
    t.after(async () => {
      try {
        await pool.query(
          [
            "DELETE FROM session_authority.restore_recovery_cursors",
            "WHERE recovery_scope_id = $1",
          ].join(" "),
          [recoveryScopeId],
        );
      } finally {
        try {
          await lifecyclePool.end();
        } finally {
          await pool.end();
        }
      }
    });

    const store = new PostgresSerializableStore({ dedicatedPool: pool });
    await store.migrate();
    const authority = new PostgresSessionAuthority({
      restoreAttachmentActivationV2FleetCompatible: true,
      restoreAttachmentActivationV2GenerationPredecessorFleetCompatible:
        true,
      restoreGenerationV2FleetCompatible: true,
      store,
      writerLaunchStopV3FleetCompatible: true,
    });
    const lifecycleGuard = createPostgresRestoreLifecycleGuard({
      operationGuard: new PostgresOperationGuard({
        dedicatedPool: lifecyclePool,
      }),
    });
    let unexpectedReconciliations = 0;
    const recoveryService =
      createPostgresRestoreActivationRecoveryService({
        listCurrentWriterLaunchCandidates: (input) =>
          authority.listCurrentWriterLaunchRecoveryCandidates(input),
        listRestoreAttachmentActivationCandidates: (input) =>
          authority.listRestoreAttachmentActivationRecoveryCandidates(input),
        listRestoreGenerationCandidates: (input) =>
          authority.listRestoreDestinationGenerationRecoveryCandidates(input),
        listWriterLaunchAttemptCandidates: (input) =>
          authority.listWriterLaunchAttemptRecoveryCandidates(input),
        reconcileRestoreAttachmentActivation() {
          unexpectedReconciliations += 1;
        },
        reconcileRestoreGeneration() {
          unexpectedReconciliations += 1;
        },
        reconcileWriterLaunchAttempt() {
          unexpectedReconciliations += 1;
        },
      });
    const runner = createPostgresRestoreRecoveryRunner({
      cursorStore: createPostgresRestoreRecoveryCursorStore({ store }),
      lifecycleGuard,
      limits: {
        activation: 10,
        currentLaunch: 10,
        generation: 10,
        launchAttempt: 10,
      },
      recoveryScopeId,
      recoveryService,
    });

    const firstStep = deferred();
    const steps = [];
    const scheduler = createPostgresRestoreRecoveryScheduler({
      intervalMilliseconds: 60_000,
      onStep(receipt) {
        steps.push(receipt);
        if (steps.length === 1) firstStep.resolve(receipt);
      },
      runner,
    });
    const completion = scheduler.start();
    const first = await firstStep.promise;
    assert.equal(first.status, "completed");
    assert.equal(first.recovery.status, "sweep-complete");
    assert.equal(unexpectedReconciliations, 0);
    await new Promise((resolve) => setImmediate(resolve));

    const releaseForeground = deferred();
    const firstForegroundEntered = deferred();
    const secondForegroundEntered = deferred();
    const firstForeground = lifecycleGuard.runForeground(
      async (_lease, complete) => {
        firstForegroundEntered.resolve();
        await releaseForeground.promise;
        return complete(undefined);
      },
    );
    await firstForegroundEntered.promise;
    const secondForeground = lifecycleGuard.runForeground(
      async (_lease, complete) => {
        secondForegroundEntered.resolve();
        await releaseForeground.promise;
        return complete(undefined);
      },
    );
    await secondForegroundEntered.promise;

    const busy = await scheduler.runStep({ signal: null });
    assert.deepEqual(structuredClone(busy), {
      errorCode: null,
      recovery: null,
      status: "busy",
    });
    releaseForeground.resolve();
    await Promise.all([firstForeground, secondForeground]);

    assert.strictEqual(scheduler.stop(), completion);
    assert.deepEqual(structuredClone(await completion), { status: "stopped" });
    assert.equal(steps.length, 2);
    assert.equal(steps[1].status, "busy");

    const stored = await pool.query(
      [
        "SELECT lane, cycle::text, revision::text, after_session_id",
        "FROM session_authority.restore_recovery_cursors",
        "WHERE recovery_scope_id = $1",
        "ORDER BY lane",
      ].join(" "),
      [recoveryScopeId],
    );
    assert.deepEqual(stored.rows, [
      {
        after_session_id: null,
        cycle: "1",
        lane: "activation",
        revision: "1",
      },
      {
        after_session_id: null,
        cycle: "1",
        lane: "current-launch",
        revision: "1",
      },
      {
        after_session_id: null,
        cycle: "1",
        lane: "generation",
        revision: "1",
      },
      {
        after_session_id: null,
        cycle: "1",
        lane: "launch-attempt",
        revision: "1",
      },
    ]);
  },
);
