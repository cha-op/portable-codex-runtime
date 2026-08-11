import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { EventEmitter } from "node:events";
import { syncBuiltinESMExports } from "node:module";
import path from "node:path";
import test from "node:test";

import {
  operationJournalBindingSha256,
} from "../src/filesystem-operation-journal.mjs";
import {
  createPostgresDetachedRestorePlan,
  rehydratePostgresDetachedRestorePlan,
} from "../src/postgres-detached-restore-plan.mjs";
import {
  PostgresSerializableStore,
  PostgresSerializableStoreError,
} from "../src/postgres-serializable-store.mjs";
import {
  CHECKPOINT_CAPTURE_OPERATION_KIND,
  createCheckpointCaptureOperationRequest,
  createRestoreAttachmentActivationOperationRequest,
  createRestoreAttachmentActivationOperationRequestV2,
  createRestoreDestinationGenerationOperationRequest,
  createRestoreDestinationGenerationOperationRequestV2,
  createWriterLaunchAttemptOperationRequest,
  createWriterLaunchStopOperationRequest,
  assertCommittedWriterLaunchStopTransitionProof,
  assertWriterLaunchStopCaptureHandoffProof,
  PostgresSessionAuthority,
  PostgresSessionAuthorityError,
  MAX_WRITER_LEASE_DURATION_MILLISECONDS,
  RESTORE_ATTACHMENT_ACTIVATION_OPERATION_KIND,
  RESTORE_DESTINATION_GENERATION_OPERATION_KIND,
  SESSION_AUTHORITY_DOCUMENT_VERSION,
  SESSION_OPERATION_CONFLICT_CLASS,
  WRITER_ATTACHMENT_ACQUIRE_OPERATION_KIND,
  WRITER_FORCE_FENCE_OPERATION_KIND,
  WRITER_LEASE_RENEW_OPERATION_KIND,
  WRITER_LAUNCH_ATTEMPT_OPERATION_KIND,
  WRITER_LAUNCH_PRE_DISPATCH_CANCELLATION_REASON,
  WRITER_LAUNCH_STOP_OPERATION_KIND,
  WRITER_RELEASE_OPERATION_KIND,
} from "../src/postgres-session-authority.mjs";
import {
  createSessionManifest,
  serializeSessionManifest,
} from "../src/session-storage-contracts.mjs";

const SESSION_ID = "019f2100-0000-7000-8000-000000000001";
const OTHER_SESSION_ID = "019f2100-0000-7000-8000-000000000002";
const THIRD_SESSION_ID = "019f2100-0000-7000-8000-000000000004";
const OPERATION_ID = "operation-001";
const OTHER_OPERATION_ID = "operation-002";
const CAPTURE_OPERATION_ID = "checkpoint-capture-operation-001";
const CAPTURE_ATTEMPT_ID = "019f2100-0000-7000-8000-000000000003";
const OTHER_CAPTURE_ATTEMPT_ID =
  "019f2100-0000-7000-8000-000000000005";
const THIRD_CAPTURE_ATTEMPT_ID =
  "019f2100-0000-7000-8000-000000000006";
const CHECKPOINT_ID = "checkpoint-001";
const ARTIFACT_ID = "checkpoint-artifact-001";
const RESTORE_OPERATION_ID = "restore-generation-operation-001";
const RESTORE_GENERATION_ID = "restore-generation-001";
const DESTINATION_ISOLATION_PROOF_ID = "destination-isolation-proof-001";
const LAUNCH_ATTEMPT_OPERATION_ID = "writer-launch-attempt-operation-001";
const PROCESS_INCARNATION_ID = "process-incarnation-001";
const SUPERVISOR_ID = "supervisor-001";
const SUPERVISOR_PROOF_ID = "supervisor-proof-001";
const STOP_OPERATION_ID = "stop-operation-001";
const RESTORE_ACTIVATION_OPERATION_ID =
  "restore-attachment-activation-operation-001";
const RESTORE_ACTIVATION_DETACH_OPERATION_ID =
  "restore-attachment-activation-detach-001";
const RESTORE_ACTIVATION_LAUNCH_OPERATION_ID =
  "restore-attachment-activation-launch-001";
const STOP_CLAIM_TOKEN = "019f2100-0000-7000-8000-000000000007";
const OTHER_STOP_CLAIM_TOKEN = "019f2100-0000-7000-8000-000000000008";
const WRITER_INCARNATION_ID = "writer-incarnation-001";
const MAX_POSTGRES_BIGINT = 9_223_372_036_854_775_807n;
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
const RESTORE_PREPARED_NOW = "2026-07-29T12:35:10.000Z";
const RESTORE_DISPATCH_NOW = "2026-07-29T12:35:11.000Z";
const RESTORE_AUTHORITY_NOW = "2026-07-29T12:35:11.500Z";
const RESTORE_UNCERTAIN_NOW = "2026-07-29T12:35:12.000Z";
const RESTORE_FINALIZE_NOW = "2026-07-29T12:35:13.000Z";
const RESTORE_CANCEL_NOW = "2026-07-29T12:35:14.000Z";
const LAUNCH_PREPARED_NOW = "2026-07-29T12:35:20.000Z";
const LAUNCH_DISPATCH_NOW = "2026-07-29T12:35:21.000Z";
const LAUNCH_UNCERTAIN_NOW = "2026-07-29T12:35:22.000Z";
const LAUNCH_FINALIZE_NOW = "2026-07-29T12:35:23.000Z";
const LAUNCH_RENEW_TRANSACTION_NOW = "2026-07-29T12:35:29.000Z";
const LAUNCH_RENEW_AUTHORITY_NOW = "2026-07-29T12:35:30.000Z";
const LAUNCH_FENCE_PREPARED_NOW = "2026-07-29T12:35:31.000Z";
const LAUNCH_FENCE_DISPATCH_NOW = "2026-07-29T12:35:32.000Z";
const LAUNCH_FENCE_UNCERTAIN_NOW = "2026-07-29T12:35:33.000Z";
const LAUNCH_FENCE_FINALIZE_NOW = "2026-07-29T12:35:34.000Z";
const LAUNCH_CHECKPOINT_PREPARED_NOW = "2026-07-29T12:35:40.000Z";
const LAUNCH_CHECKPOINT_DISPATCH_NOW = "2026-07-29T12:35:41.000Z";
const LAUNCH_CHECKPOINT_FINALIZE_NOW = "2026-07-29T12:35:42.000Z";
const LAUNCH_STOP_PREPARED_NOW = "2026-07-29T12:36:00.000Z";
const LAUNCH_STOP_DISPATCH_NOW = "2026-07-29T12:36:01.000Z";
const LAUNCH_STOP_UNCERTAIN_NOW = "2026-07-29T12:36:02.000Z";
const LAUNCH_STOP_FINALIZE_NOW = "2026-07-29T12:36:03.000Z";
const LAUNCH_STOP_CAPTURE_DISPATCH_NOW = "2026-07-29T12:36:04.000Z";
const LAUNCH_STOP_CAPTURE_UNCERTAIN_NOW = "2026-07-29T12:36:05.000Z";
const LAUNCH_STOP_CAPTURE_FINALIZE_NOW = "2026-07-29T12:36:06.000Z";
const RESTORE_ACTIVATION_CAPTURE_PREPARED_NOW =
  "2026-07-29T12:36:04.000Z";
const RESTORE_ACTIVATION_CAPTURE_DISPATCH_NOW =
  "2026-07-29T12:36:05.000Z";
const RESTORE_ACTIVATION_CAPTURE_FINALIZE_NOW =
  "2026-07-29T12:36:06.000Z";
const RESTORE_ACTIVATION_GENERATION_PREPARED_NOW =
  "2026-07-29T12:36:07.000Z";
const RESTORE_ACTIVATION_GENERATION_DISPATCH_NOW =
  "2026-07-29T12:36:08.000Z";
const RESTORE_ACTIVATION_GENERATION_FINALIZE_NOW =
  "2026-07-29T12:36:09.000Z";
const RESTORE_ACTIVATION_DETACH_PREPARED_NOW =
  "2026-07-29T12:36:10.000Z";
const RESTORE_ACTIVATION_DETACH_NOW = "2026-07-29T12:36:12.000Z";
const RESTORE_ACTIVATION_PREPARED_NOW = "2026-07-29T12:36:20.000Z";
const RESTORE_ACTIVATION_DISPATCH_NOW = "2026-07-29T12:36:21.000Z";
const RESTORE_ACTIVATION_AUTHORITY_NOW = "2026-07-29T12:36:21.500Z";
const RESTORE_ACTIVATION_FINALIZE_NOW = "2026-07-29T12:36:22.000Z";
const RESTORE_ACTIVATION_LAUNCH_DISPATCH_NOW =
  "2026-07-29T12:36:23.000Z";
const RESTORE_ACTIVATION_LAUNCH_UNCERTAIN_NOW =
  "2026-07-29T12:36:24.000Z";
const REPLAY_STOP_PREPARED_NOW = "2026-07-29T12:35:24.000Z";
const REPLAY_STOP_DISPATCH_NOW = "2026-07-29T12:35:25.000Z";
const REPLAY_STOP_FINALIZE_NOW = "2026-07-29T12:35:26.000Z";
const SUCCESSOR_LAUNCH_PREPARED_NOW = "2026-07-29T12:35:27.000Z";
const SUCCESSOR_LAUNCH_FINALIZE_NOW = "2026-07-29T12:35:29.000Z";
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
const LIST_CURRENT_WRITER_LAUNCH_FIRST_PAGE_QUERY = [
  `SELECT ${SESSION_COLUMNS}`,
  "FROM session_authority.sessions",
  "ORDER BY session_id ASC",
  "LIMIT $1::integer",
].join(" ");
const LIST_CURRENT_WRITER_LAUNCH_AFTER_QUERY = [
  `SELECT ${SESSION_COLUMNS}`,
  "FROM session_authority.sessions",
  "WHERE session_id > $1::uuid",
  "ORDER BY session_id ASC",
  "LIMIT $2::integer",
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
const OPERATION_ID_CLAIM_COLUMNS = [
  "operation_id",
  "session_id",
  "claim_type",
  "claimant_operation_id",
  "binding",
  "claimed_at",
  "materialized_at",
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
const READ_OPERATION_ID_CLAIM_QUERY = [
  `SELECT ${OPERATION_ID_CLAIM_COLUMNS}`,
  "FROM session_authority.operation_id_registry",
  "WHERE operation_id = $1",
].join(" ");
const READ_OPERATION_ID_CLAIM_FOR_UPDATE_QUERY =
  `${READ_OPERATION_ID_CLAIM_QUERY} FOR UPDATE`;
const LIST_CHECKPOINT_CAPTURE_RECOVERY_FIRST_PAGE_QUERY = [
  `SELECT ${OPERATION_COLUMNS}`,
  "FROM session_authority.operation_claims AS capture",
  "WHERE capture.kind = 'checkpoint-capture-v1'",
  "AND (capture.state IN ('starting', 'uncertain') OR (",
  "capture.state = 'prepared' AND EXISTS (",
  "SELECT 1 FROM session_authority.operation_id_registry AS registry",
  "WHERE registry.operation_id = capture.operation_id",
  "AND registry.session_id = capture.session_id",
  "AND registry.claim_type = 'writer-stop-capture-intent-v3'",
  "AND registry.materialized_at IS NOT NULL)))",
  "AND capture.retired_at IS NULL",
  "ORDER BY capture.session_id ASC",
  "LIMIT $1::integer",
].join(" ");
const LIST_CHECKPOINT_CAPTURE_RECOVERY_AFTER_QUERY = [
  `SELECT ${OPERATION_COLUMNS}`,
  "FROM session_authority.operation_claims AS capture",
  "WHERE capture.kind = 'checkpoint-capture-v1'",
  "AND (capture.state IN ('starting', 'uncertain') OR (",
  "capture.state = 'prepared' AND EXISTS (",
  "SELECT 1 FROM session_authority.operation_id_registry AS registry",
  "WHERE registry.operation_id = capture.operation_id",
  "AND registry.session_id = capture.session_id",
  "AND registry.claim_type = 'writer-stop-capture-intent-v3'",
  "AND registry.materialized_at IS NOT NULL)))",
  "AND capture.retired_at IS NULL",
  "AND capture.session_id > $1::uuid",
  "ORDER BY capture.session_id ASC",
  "LIMIT $2::integer",
].join(" ");
const LIST_RESTORE_GENERATION_RECOVERY_FIRST_PAGE_QUERY = [
  `SELECT ${OPERATION_COLUMNS}`,
  "FROM session_authority.operation_claims",
  "WHERE kind = 'restore-destination-generation-v1'",
  "AND state IN ('starting', 'uncertain')",
  "AND retired_at IS NULL",
  "ORDER BY session_id ASC",
  "LIMIT $1::integer",
].join(" ");
const LIST_RESTORE_GENERATION_RECOVERY_AFTER_QUERY = [
  `SELECT ${OPERATION_COLUMNS}`,
  "FROM session_authority.operation_claims",
  "WHERE kind = 'restore-destination-generation-v1'",
  "AND state IN ('starting', 'uncertain')",
  "AND retired_at IS NULL",
  "AND session_id > $1::uuid",
  "ORDER BY session_id ASC",
  "LIMIT $2::integer",
].join(" ");
const LIST_RESTORE_ATTACHMENT_ACTIVATION_RECOVERY_FIRST_PAGE_QUERY = [
  `SELECT ${OPERATION_COLUMNS}`,
  "FROM session_authority.operation_claims",
  "WHERE kind = 'restore-attachment-activation-v1'",
  "AND state IN ('starting', 'uncertain')",
  "AND retired_at IS NULL",
  "ORDER BY session_id ASC",
  "LIMIT $1::integer",
].join(" ");
const LIST_WRITER_LAUNCH_RECOVERY_FIRST_PAGE_QUERY = [
  `SELECT ${OPERATION_COLUMNS}`,
  "FROM session_authority.operation_claims",
  "WHERE kind = 'writer-launch-attempt-v1'",
  "AND state IN ('prepared', 'starting', 'uncertain')",
  "AND retired_at IS NULL",
  "ORDER BY session_id ASC",
  "LIMIT $1::integer",
].join(" ");
const LIST_WRITER_LAUNCH_RECOVERY_AFTER_QUERY = [
  `SELECT ${OPERATION_COLUMNS}`,
  "FROM session_authority.operation_claims",
  "WHERE kind = 'writer-launch-attempt-v1'",
  "AND state IN ('prepared', 'starting', 'uncertain')",
  "AND retired_at IS NULL",
  "AND session_id > $1::uuid",
  "ORDER BY session_id ASC",
  "LIMIT $2::integer",
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
  "WITH claimed_id AS (",
  "INSERT INTO session_authority.operation_id_registry",
  "(operation_id, session_id, claim_type, claimant_operation_id, binding,",
  "claimed_at, materialized_at)",
  "VALUES ($1, $2::uuid, 'direct-operation', NULL, NULL,",
  "$5, $5)",
  "ON CONFLICT (operation_id) DO NOTHING",
  "RETURNING operation_id)",
  "INSERT INTO session_authority.operation_claims",
  "(operation_id, session_id, kind, request, result, state, revision,",
  "created_at, updated_at, retired_at)",
  "SELECT $1, $2::uuid, $3, $4::jsonb, NULL, 'prepared', 0, $5, $5, NULL",
  "FROM claimed_id",
  "ON CONFLICT (operation_id) DO NOTHING",
  `RETURNING ${OPERATION_COLUMNS}`,
].join(" ");
const MATERIALIZE_DETACHED_RESTORE_STABLE_PLAN_ID_CLAIM_QUERY = [
  "UPDATE session_authority.operation_id_registry AS registry",
  "SET materialized_at = $3",
  "FROM session_authority.detached_restore_stable_plans AS stable",
  "WHERE registry.operation_id = $1",
  "AND registry.session_id = $2::uuid",
  "AND registry.claim_type = 'detached-restore-stable-plan-v1'",
  "AND registry.claimant_operation_id IS NULL",
  "AND registry.materialized_at IS NULL",
  "AND stable.operation_id = registry.operation_id",
  "AND stable.session_id = registry.session_id",
  "AND stable.admission = $4::jsonb",
  "AND stable.provisioned_at = registry.claimed_at",
  "AND registry.binding = pg_catalog.jsonb_build_object(",
  "'bindingSha256', stable.binding_sha256,",
  "'contractVersion', stable.plan_contract_version,",
  "'planSha256', stable.plan_sha256,",
  "'request', stable.admission #> '{request}')",
  "RETURNING registry.operation_id AS operation_id,",
  "registry.session_id AS session_id,",
  "registry.claim_type AS claim_type,",
  "registry.claimant_operation_id AS claimant_operation_id,",
  "registry.binding AS binding,",
  "registry.claimed_at AS claimed_at,",
  "registry.materialized_at AS materialized_at",
].join(" ");
const INSERT_MATERIALIZED_DETACHED_RESTORE_STABLE_PLAN_OPERATION_QUERY = [
  "INSERT INTO session_authority.operation_claims",
  "(operation_id, session_id, kind, request, result, state, revision,",
  "created_at, updated_at, retired_at)",
  "VALUES ($1, $2::uuid, $3, $4::jsonb, NULL, 'prepared', 0, $5, $5, NULL)",
  "ON CONFLICT (operation_id) DO NOTHING",
  `RETURNING ${OPERATION_COLUMNS}`,
].join(" ");
const INSERT_RESTORE_LAUNCH_ID_CLAIM_QUERY = [
  "INSERT INTO session_authority.operation_id_registry",
  "(operation_id, session_id, claim_type, claimant_operation_id, binding,",
  "claimed_at, materialized_at)",
  "VALUES ($1, $2::uuid, 'restore-launch-intent-v2',",
  "$3, $4::jsonb, $5, NULL)",
  "ON CONFLICT DO NOTHING",
  `RETURNING ${OPERATION_ID_CLAIM_COLUMNS}`,
].join(" ");
const INSERT_RESTORE_ACTIVATION_LAUNCH_ID_CLAIM_QUERY = [
  "INSERT INTO session_authority.operation_id_registry",
  "(operation_id, session_id, claim_type, claimant_operation_id, binding,",
  "claimed_at, materialized_at)",
  "VALUES ($1, $2::uuid, 'restore-activation-launch-intent-v1',",
  "$3, $4::jsonb, $5, NULL)",
  "ON CONFLICT DO NOTHING",
  `RETURNING ${OPERATION_ID_CLAIM_COLUMNS}`,
].join(" ");
const INSERT_PRECLAIMED_OPERATION_QUERY = [
  "INSERT INTO session_authority.operation_claims",
  "(operation_id, session_id, kind, request, result, state, revision,",
  "created_at, updated_at, retired_at)",
  "SELECT $1::character varying(128), $2::uuid, $3, $4::jsonb,",
  "NULL, 'prepared', 0, $5, $5, NULL",
  "FROM session_authority.operation_id_registry",
  "WHERE operation_id = $1::character varying(128)",
  "AND session_id = $2::uuid",
  "AND claim_type = 'restore-launch-intent-v2'",
  "AND claimant_operation_id = $6 AND binding = $7::jsonb",
  "AND materialized_at IS NULL",
  "ON CONFLICT (operation_id) DO NOTHING",
  `RETURNING ${OPERATION_COLUMNS}`,
].join(" ");
const MATERIALIZE_RESTORE_LAUNCH_ID_CLAIM_QUERY = [
  "UPDATE session_authority.operation_id_registry",
  "SET materialized_at = $3",
  "WHERE operation_id = $1 AND session_id = $2::uuid",
  "AND claim_type = 'restore-launch-intent-v2'",
  "AND claimant_operation_id = $4 AND binding = $5::jsonb",
  "AND materialized_at IS NULL",
  `RETURNING ${OPERATION_ID_CLAIM_COLUMNS}`,
].join(" ");
const INSERT_PRECLAIMED_RESTORE_ACTIVATION_LAUNCH_QUERY = [
  "INSERT INTO session_authority.operation_claims",
  "(operation_id, session_id, kind, request, result, state, revision,",
  "created_at, updated_at, retired_at)",
  "SELECT $1::character varying(128), $2::uuid, $3, $4::jsonb,",
  "NULL, 'prepared', 0, $5, $5, NULL",
  "FROM session_authority.operation_id_registry",
  "WHERE operation_id = $1::character varying(128)",
  "AND session_id = $2::uuid",
  "AND claim_type = 'restore-activation-launch-intent-v1'",
  "AND claimant_operation_id = $6 AND binding = $7::jsonb",
  "AND materialized_at IS NULL",
  "ON CONFLICT (operation_id) DO NOTHING",
  `RETURNING ${OPERATION_COLUMNS}`,
].join(" ");
const MATERIALIZE_RESTORE_ACTIVATION_LAUNCH_ID_CLAIM_QUERY = [
  "UPDATE session_authority.operation_id_registry",
  "SET materialized_at = $3",
  "WHERE operation_id = $1 AND session_id = $2::uuid",
  "AND claim_type = 'restore-activation-launch-intent-v1'",
  "AND claimant_operation_id = $4 AND binding = $5::jsonb",
  "AND materialized_at IS NULL",
  `RETURNING ${OPERATION_ID_CLAIM_COLUMNS}`,
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
  "WITH claimed_id AS (",
  "INSERT INTO session_authority.operation_id_registry",
  "(operation_id, session_id, claim_type, claimant_operation_id, binding,",
  "claimed_at, materialized_at)",
  "VALUES ($1, $2::uuid, 'direct-operation', NULL, NULL,",
  "$6, $6)",
  "ON CONFLICT (operation_id) DO NOTHING",
  "RETURNING operation_id)",
  "INSERT INTO session_authority.operation_claims",
  "(operation_id, session_id, kind, request, result, state, revision,",
  "created_at, updated_at, retired_at)",
  "SELECT $1, $2::uuid, $3, $4::jsonb, $5::jsonb, 'committed', 0,",
  "$6, $6, $6",
  "FROM claimed_id",
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
const RESTORE_GENERATION_COLUMNS = [
  "generation_id",
  "operation_id",
  "session_id",
  "checkpoint_id",
  "state",
  "binding",
  "document",
  "claimed_at",
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
const READ_RESTORE_GENERATION_BY_OPERATION_QUERY = [
  `SELECT ${RESTORE_GENERATION_COLUMNS}`,
  "FROM session_authority.restore_destination_generations",
  "WHERE operation_id = $1",
].join(" ");
const READ_RESTORE_GENERATION_BY_ID_QUERY = [
  `SELECT ${RESTORE_GENERATION_COLUMNS}`,
  "FROM session_authority.restore_destination_generations",
  "WHERE generation_id = $1",
].join(" ");
const INSERT_RESTORE_GENERATION_QUERY = [
  "INSERT INTO session_authority.restore_destination_generations",
  "(generation_id, operation_id, session_id, checkpoint_id, state,",
  "binding, document, claimed_at, committed_at)",
  "VALUES ($1, $2, $3::uuid, $4, 'authorized', $5::jsonb, NULL, $6, NULL)",
  "ON CONFLICT DO NOTHING",
  `RETURNING ${RESTORE_GENERATION_COLUMNS}`,
].join(" ");
const COMMIT_RESTORE_GENERATION_QUERY = [
  "UPDATE session_authority.restore_destination_generations",
  "SET state = 'committed', document = $2::jsonb, committed_at = $3",
  "WHERE operation_id = $1 AND state = 'authorized'",
  "AND document IS NULL AND committed_at IS NULL",
  `RETURNING ${RESTORE_GENERATION_COLUMNS}`,
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

function versionTwoDocument(sessionId = SESSION_ID, overrides = {}) {
  return document(sessionId, {
    ...overrides,
    documentVersion: 2,
    launch: null,
  });
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

function writerLaunchStopClaimSha256(claimToken) {
  return sha256(
    `portable-codex-runtime:writer-launch-stop-claim:v1\0${claimToken}`,
  );
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

function canonicalSha256(value) {
  return sha256(JSON.stringify(canonicalPayload(value)));
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
  sessionId = SESSION_ID,
} = {}) {
  const expectedSession = sessionSnapshot({
    sessionId,
    sessionDocument: document(sessionId, {
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
      launch: null,
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
      launch: structuredClone(options.expectedSession.document.launch),
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

function checkpointCaptureFixture({
  artifactId = ARTIFACT_ID,
  captureAttemptId = CAPTURE_ATTEMPT_ID,
  checkpointId = CHECKPOINT_ID,
  operationId = CAPTURE_OPERATION_ID,
  processIncarnationId = PROCESS_INCARNATION_ID,
  publicationId = "checkpoint-publication-001",
  sessionId = SESSION_ID,
  stopOperationId = STOP_OPERATION_ID,
  writerIncarnationId = WRITER_INCARNATION_ID,
  writerOperationId = OPERATION_ID,
  writer: suppliedWriter = null,
} = {}) {
  const writer =
    suppliedWriter ??
    writerAcquiredFixture({
      operationId: writerOperationId,
      sessionId,
    });
  const checkpoint = {
    artifactId,
    backendId: writer.expectedSession.document.storageRef.backendId,
    checkpointClass: "clean",
    checkpointId,
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
    operationId,
    sessionId: writer.expectedSession.sessionId,
    storageId: checkpoint.storageId,
    target: {
      artifactId,
      checkpointId,
      kind: "checkpoint",
    },
  };
  const admission = {
    attachment: structuredClone(
      writer.expectedSession.document.attachment,
    ),
    captureAttemptId,
    checkpoint,
    processIncarnationId,
    request: mutationRequest,
    stopOperationId,
    writerIncarnationId,
  };
  const request = createCheckpointCaptureOperationRequest({
    admission,
    expectedSession: writer.expectedSession,
  });
  const options = {
    expectedSession: writer.expectedSession,
    kind: CHECKPOINT_CAPTURE_OPERATION_KIND,
    operationId,
    request,
  };
  const artifactManifestDigest = "b".repeat(64);
  const modeledDigest = "c".repeat(64);
  const completion = {
    artifactProof: {
      artifactManifestDigest,
      captureOperationId: operationId,
      modeledDigest,
    },
    materialization: {
      artifactManifestDigest,
      contractVersion: 2,
      modeledDigest,
      publicationId,
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

function checkpointRecoveryFixture(sessionId) {
  if (sessionId === OTHER_SESSION_ID) {
    return checkpointCaptureFixture({
      artifactId: "checkpoint-artifact-002",
      captureAttemptId: OTHER_CAPTURE_ATTEMPT_ID,
      checkpointId: "checkpoint-002",
      operationId: "checkpoint-capture-operation-002",
      processIncarnationId: "process-incarnation-002",
      publicationId: "checkpoint-publication-002",
      sessionId,
      stopOperationId: "stop-operation-002",
      writerIncarnationId: "writer-incarnation-002",
      writerOperationId: "writer-operation-002",
    });
  }
  assert.equal(sessionId, THIRD_SESSION_ID);
  return checkpointCaptureFixture({
    artifactId: "checkpoint-artifact-003",
    captureAttemptId: THIRD_CAPTURE_ATTEMPT_ID,
    checkpointId: "checkpoint-003",
    operationId: "checkpoint-capture-operation-003",
    processIncarnationId: "process-incarnation-003",
    publicationId: "checkpoint-publication-003",
    sessionId,
    stopOperationId: "stop-operation-003",
    writerIncarnationId: "writer-incarnation-003",
    writerOperationId: "writer-operation-003",
  });
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
  {
    binding = checkpointCaptureBinding(fixture),
    claimedAt = CAPTURE_DISPATCH_NOW,
  } = {},
) {
  return {
    binding: structuredClone(binding),
    capture_attempt_id: fixture.request.admission.captureAttemptId,
    claimed_at: new Date(claimedAt),
    operation_id: fixture.options.operationId,
    session_id: fixture.options.expectedSession.sessionId,
  };
}

function checkpointCaptureTombstoneRow(fixture) {
  return {
    capture_attempt_id: fixture.request.admission.captureAttemptId,
    operation_id: fixture.options.operationId,
    retired_at: new Date(CAPTURE_FINALIZE_NOW),
    session_id: fixture.options.expectedSession.sessionId,
    tombstone: {
      contractVersion: 1,
      reason: "administratively-retired",
    },
  };
}

function checkpointCatalogueRow(
  fixture,
  {
    committedAt = CAPTURE_FINALIZE_NOW,
    document: catalogueDocument = checkpointCatalogueDocument(fixture),
  } = {},
) {
  return {
    capture_attempt_id: fixture.request.admission.captureAttemptId,
    checkpoint_id: fixture.checkpoint.checkpointId,
    committed_at: new Date(committedAt),
    document: structuredClone(catalogueDocument),
    session_id: fixture.options.expectedSession.sessionId,
  };
}

function checkpointCaptureTerminalResult(fixture) {
  const catalogueDocument = checkpointCatalogueDocument(fixture);
  return {
    captureAttemptId: fixture.request.admission.captureAttemptId,
    catalogueSha256: sha256(
      JSON.stringify(catalogueDocument),
    ),
    checkpointId: fixture.checkpoint.checkpointId,
    outcome: "checkpoint-captured",
    resultVersion: 1,
  };
}

function checkpointCaptureOperationRow(
  fixture,
  state,
  {
    createdAt = CAPTURE_PREPARED_NOW,
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
    createdAt,
    updatedAt,
    result,
    retiredAt: state === "committed" ? updatedAt : null,
  });
}

function checkpointCaptureReservationRow(
  fixture,
  state,
  {
    createdAt = CAPTURE_PREPARED_NOW,
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
    createdAt,
    updatedAt,
    releasedAt: state === "released" ? updatedAt : null,
  });
}

function checkpointCapturePhaseSessionRow(
  fixture,
  state,
  { updatedAt: suppliedUpdatedAt } = {},
) {
  const operationRevision =
    state === "prepared" ? "0" : state === "starting" ? "1" : "2";
  const updatedAt =
    suppliedUpdatedAt ??
    (state === "prepared"
      ? CAPTURE_PREPARED_NOW
      : state === "starting"
        ? CAPTURE_DISPATCH_NOW
        : CAPTURE_UNCERTAIN_NOW);
  return sessionRow({
    sessionId: fixture.options.expectedSession.sessionId,
    revision: (
      BigInt(fixture.options.expectedSession.revision) +
      BigInt(operationRevision) +
      1n
    ).toString(),
    sessionDocument: document(fixture.options.expectedSession.sessionId, {
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
  {
    operationRevision = "3",
    updatedAt = CAPTURE_FINALIZE_NOW,
  } = {},
) {
  const result = checkpointCaptureTerminalResult(fixture);
  return sessionRow({
    sessionId: fixture.options.expectedSession.sessionId,
    revision: (
      BigInt(fixture.options.expectedSession.revision) +
      BigInt(operationRevision) +
      1n
    ).toString(),
    sessionDocument: document(fixture.options.expectedSession.sessionId, {
      ...structuredClone(fixture.options.expectedSession.document),
      activeOperation: null,
      lastOperation: terminalPointer({
        options: fixture.options,
        operationRevision,
        result,
      }),
    }),
    createdAt: fixture.options.expectedSession.createdAt,
    updatedAt,
  });
}

function checkpointCaptureAttemptRecord(
  fixture,
  state = "authorized",
) {
  return {
    binding: checkpointCaptureBinding(fixture),
    captureAttemptId: fixture.request.admission.captureAttemptId,
    contractVersion: 1,
    operationId: fixture.options.operationId,
    request: fixture.request.admission.request,
    result: fixture.request.predeterminedResult,
    state,
  };
}

function restoreCurrentWriterFixture({
  checkpoint,
  sessionId,
  storageId = "volume-001",
  suffix,
}) {
  const previousOptions = reserveOptions({
    expectedSession: sessionSnapshot({
      sessionId,
      sessionDocument: document(sessionId, {
        storageRef: storageRef(sessionId, storageId),
      }),
    }),
    operationId: `restore-anchor-previous-${suffix}`,
    request: operationRequest({ checkpointId: `restore-anchor-${suffix}` }),
  });
  const previousResult = cancellationResult(`restore-anchor-${suffix}`);
  const anchorExpectedSession = sessionSnapshot({
    sessionId,
    revision: "2",
    sessionDocument: document(sessionId, {
      storageRef: storageRef(sessionId, storageId),
      writerEpoch: checkpoint.sourceFencingEpoch,
      lastOperation: terminalPointer({
        options: previousOptions,
        operationRevision: "1",
        result: previousResult,
      }),
    }),
  });
  const anchorOptions = reserveOptions({
    expectedSession: anchorExpectedSession,
    operationId: `restore-anchor-current-${suffix}`,
    request: operationRequest({
      checkpointId: `restore-anchor-current-${suffix}`,
    }),
  });
  const anchorResult = cancellationResult(
    `restore-anchor-current-${suffix}`,
  );
  const expectedSession = sessionSnapshot({
    sessionId,
    revision: "4",
    sessionDocument: document(sessionId, {
      storageRef: storageRef(sessionId, storageId),
      writerEpoch: checkpoint.sourceFencingEpoch,
      lastOperation: terminalPointer({
        options: anchorOptions,
        operationRevision: "1",
        result: anchorResult,
      }),
    }),
  });
  const options = writerAcquireOptions({
    expectedSession,
    operationId: `restore-writer-${suffix}`,
  });
  const lease = writerLease(options);
  const result = writerAttachmentResult(options, lease, {
    mutationResult: writerMutationResult(options, lease, {
      proofId: `restore-writer-proof-${suffix}`,
      rootPath: `/var/lib/portable-codex/restore-${suffix}`,
    }),
    attachment: writerAttachment(options, lease, {
      proofId: `restore-writer-proof-${suffix}`,
      rootPath: `/var/lib/portable-codex/restore-${suffix}`,
    }),
  });
  const session = writerAttachedSessionRow({ options, lease, result });
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

function restoreCurrentWriterAtRevision(writer, revision) {
  const targetRevision = BigInt(revision);
  const writerExpectedRevision = targetRevision - 3n;
  const anchorExpectedRevision = writerExpectedRevision - 2n;
  assert.ok(anchorExpectedRevision >= 0n);
  const expectedSession = structuredClone(writer.options.expectedSession);
  expectedSession.revision = writerExpectedRevision.toString();
  expectedSession.document.lastOperation.expectedSessionRevision =
    anchorExpectedRevision.toString();
  const options = {
    ...writer.options,
    expectedSession,
  };
  const session = writerAttachedSessionRow({
    options,
    lease: writer.lease,
    result: writer.result,
  });
  return {
    ...writer,
    committedOperation: writerCommittedOperationRow({
      options,
      lease: writer.lease,
      result: writer.result,
    }),
    expectedSession: snapshotFromSessionRow(session),
    options,
    releasedReservation: reservationRow("released", {
      options,
      updatedAt: FINAL,
      releasedAt: FINAL,
    }),
    session,
  };
}

function restoreGenerationFixture({
  committedAt = RESTORE_FINALIZE_NOW,
  dispatchAt = RESTORE_DISPATCH_NOW,
  destinationStorageId = "volume-001",
  destinationIsolationProofId = DESTINATION_ISOLATION_PROOF_ID,
  expectedSession = null,
  expectedSessionRevision = null,
  generationId = RESTORE_GENERATION_ID,
  launchAttemptId = null,
  operationId = RESTORE_OPERATION_ID,
  preparedAt = RESTORE_PREPARED_NOW,
  sessionId = SESSION_ID,
  suffix = sessionId.slice(-3),
} = {}) {
  const captureAttemptId =
    sessionId === SESSION_ID
      ? CAPTURE_ATTEMPT_ID
      : sessionId === OTHER_SESSION_ID
        ? OTHER_CAPTURE_ATTEMPT_ID
        : THIRD_CAPTURE_ATTEMPT_ID;
  const source = checkpointCaptureFixture({
    artifactId:
      sessionId === SESSION_ID ? ARTIFACT_ID : `checkpoint-artifact-${suffix}`,
    captureAttemptId,
    checkpointId:
      sessionId === SESSION_ID ? CHECKPOINT_ID : `checkpoint-${suffix}`,
    operationId:
      sessionId === SESSION_ID
        ? CAPTURE_OPERATION_ID
        : `checkpoint-source-capture-${suffix}`,
    processIncarnationId: `restore-source-process-${suffix}`,
    publicationId: `restore-source-publication-${suffix}`,
    sessionId,
    stopOperationId: `restore-source-stop-${suffix}`,
    writerIncarnationId: `restore-source-writer-incarnation-${suffix}`,
    writerOperationId:
      sessionId === SESSION_ID
        ? OPERATION_ID
        : `checkpoint-source-writer-${suffix}`,
  });
  let writer = restoreCurrentWriterFixture({
    checkpoint: source.checkpoint,
    sessionId,
    storageId: destinationStorageId,
    suffix,
  });
  if (expectedSession !== null) {
    assert.equal(expectedSessionRevision, null);
    assert.notEqual(expectedSession.document.lease, null);
    writer = {
      ...writer,
      expectedSession: structuredClone(expectedSession),
      lease: structuredClone(expectedSession.document.lease),
    };
  } else if (expectedSessionRevision !== null) {
    writer = restoreCurrentWriterAtRevision(
      writer,
      expectedSessionRevision,
    );
  }
  const mutationRequest = {
    backendId: writer.expectedSession.document.storageRef.backendId,
    contractVersion: 1,
    fencingEpoch: writer.lease.fencingEpoch,
    holderId: writer.lease.holderId,
    leaseId: writer.lease.leaseId,
    operation: "restore",
    operationId,
    sessionId,
    storageId: writer.expectedSession.document.storageRef.storageId,
    target: {
      artifactId: source.checkpoint.artifactId,
      checkpointId: source.checkpoint.checkpointId,
      kind: "checkpoint",
    },
  };
  const admission = {
    checkpoint: structuredClone(source.checkpoint),
    request: mutationRequest,
  };
  const launchIntent =
    launchAttemptId === null
      ? null
      : {
          launchAttemptId,
          measuredImage: writerLaunchMeasuredImage(writer.expectedSession),
          supervisor: {
            contractVersion: 1,
            supervisorId: SUPERVISOR_ID,
          },
        };
  const request =
    launchIntent === null
      ? createRestoreDestinationGenerationOperationRequest({
          admission,
          expectedSession: writer.expectedSession,
        })
      : createRestoreDestinationGenerationOperationRequestV2({
          admission,
          expectedSession: writer.expectedSession,
          launchIntent,
        });
  const options = {
    expectedSession: writer.expectedSession,
    kind: RESTORE_DESTINATION_GENERATION_OPERATION_KIND,
    operationId,
    request,
  };
  const fixture = {
    admission,
    committedAt,
    destinationIsolationProofId,
    dispatchAt,
    generationId,
    launchIntent,
    mutationRequest,
    options,
    preparedAt,
    request,
    source,
    writer,
  };
  const completion = {
    materialization: {
      artifactManifestDigest:
        source.completion.artifactProof.artifactManifestDigest,
      coordinatorBindingSha256:
        operationJournalBindingSha256(restoreGenerationBinding(fixture)),
      contractVersion: 3,
      modeledDigest: source.completion.artifactProof.modeledDigest,
      publicationId: `restore-destination-publication-${suffix}`,
      publicationKind: "restore-destination",
      stagedRoot: {
        filesystemId: `restore-filesystem-${suffix}`,
        objectIdentityScheme: "test-object-id-v1",
        objectId: `restore-object-${suffix}`,
      },
      treeIdentityDigest: "e".repeat(64),
    },
    replayed: false,
    result: request.predeterminedResult,
  };
  return { ...fixture, completion };
}

function detachedRestoreStablePlanForFixture(
  fixture,
  { imagePlanId = "restore-image-plan-001" } = {},
) {
  return createPostgresDetachedRestorePlan({
    plan: {
      captureCreatedAt: fixture.source.checkpoint.createdAt,
      destinationDirectory: "/var/lib/portable-codex-restores/session-001",
      destinationOwnedRoot: "/var/lib/portable-codex-restores",
      detachMode: "release",
      holderId: fixture.mutationRequest.holderId,
      imagePlanId,
      leaseDurationMilliseconds: 300_000,
      sourceArtifactDirectory: "/var/lib/portable-codex-artifacts/checkpoint-001",
      sourceArtifactOwnedRoot: "/var/lib/portable-codex-artifacts",
    },
    request: fixture.mutationRequest,
  });
}

function stableRestoreGenerationFixture() {
  const seed = restoreGenerationFixture();
  const stablePlan = detachedRestoreStablePlanForFixture(seed);
  const fixture = restoreGenerationFixture({
    destinationIsolationProofId: stablePlan.destinationIsolationProofId,
    generationId: stablePlan.generationId,
  });
  assert.deepEqual(
    canonicalPayload(stablePlan.request),
    canonicalPayload(fixture.mutationRequest),
  );
  return { ...fixture, stablePlan };
}

function restoreGenerationBinding(fixture) {
  return {
    attachment: structuredClone(
      fixture.options.expectedSession.document.attachment,
    ),
    captureAttemptId: fixture.source.request.admission.captureAttemptId,
    captureOperationId: fixture.source.options.operationId,
    catalogueSha256: sha256(
      JSON.stringify(checkpointCatalogueDocument(fixture.source)),
    ),
    checkpoint: structuredClone(fixture.source.checkpoint),
    contractVersion: 1,
    destinationIsolationProofId: fixture.destinationIsolationProofId,
    destinationState: "detached",
    generationId: fixture.generationId,
    request: structuredClone(fixture.mutationRequest),
    reservationId: operationBinding(fixture.options).reservationId,
  };
}

function restoreGenerationDocument(fixture, completion = fixture.completion) {
  return {
    artifactProof: structuredClone(fixture.source.completion.artifactProof),
    contractVersion: 2,
    materialization: structuredClone(completion.materialization),
    result: structuredClone(completion.result),
  };
}

function restoreGenerationRow(
  fixture,
  state = "authorized",
  overrides = {},
) {
  return {
    generation_id: fixture.generationId,
    operation_id: fixture.options.operationId,
    session_id: fixture.options.expectedSession.sessionId,
    checkpoint_id: fixture.source.checkpoint.checkpointId,
    state,
    binding: restoreGenerationBinding(fixture),
    document:
      state === "committed" ? restoreGenerationDocument(fixture) : null,
    claimed_at: new Date(fixture.dispatchAt),
    committed_at:
      state === "committed" ? new Date(fixture.committedAt) : null,
    ...overrides,
  };
}

function operationIdRegistryRow({
  binding = null,
  claimType = "direct-operation",
  claimedAt = LATER,
  claimantOperationId = OPERATION_ID,
  materializedAt = claimedAt,
  operationId = OPERATION_ID,
  sessionId = SESSION_ID,
} = {}) {
  return {
    operation_id: operationId,
    session_id: sessionId,
    claim_type: claimType,
    claimant_operation_id: claimantOperationId,
    binding: structuredClone(binding),
    claimed_at: new Date(claimedAt),
    materialized_at:
      materializedAt === null ? null : new Date(materializedAt),
  };
}

function restoreLaunchIdClaimRow(
  fixture,
  {
    binding = undefined,
    claimantOperationId = undefined,
    materializedAt = null,
    operationId = undefined,
    sessionId = undefined,
  } = {},
) {
  const restore = fixture.restore ?? fixture;
  return operationIdRegistryRow({
    binding:
      binding === undefined
        ? canonicalPayload(restore.launchIntent)
        : binding,
    claimType: "restore-launch-intent-v2",
    claimedAt: RESTORE_DISPATCH_NOW,
    claimantOperationId:
      claimantOperationId ?? restore.options.operationId,
    materializedAt,
    operationId: operationId ?? restore.launchIntent.launchAttemptId,
    sessionId: sessionId ?? restore.options.expectedSession.sessionId,
  });
}

function detachedRestoreStablePlanIdClaimRow(
  fixture,
  {
    binding = undefined,
    claimedAt = undefined,
    materializedAt = null,
  } = {},
) {
  return operationIdRegistryRow({
    binding:
      binding === undefined
        ? {
            bindingSha256: "b".repeat(64),
            contractVersion: 1,
            planSha256: fixture.stablePlan?.planSha256 ?? "a".repeat(64),
            request: fixture.request.admission.request,
          }
        : binding,
    claimType: "detached-restore-stable-plan-v1",
    claimedAt: claimedAt ?? fixture.preparedAt,
    claimantOperationId: null,
    materializedAt,
    operationId: fixture.options.operationId,
    sessionId: fixture.options.expectedSession.sessionId,
  });
}

function restoreGenerationOperationIdClaimRow(fixture) {
  if (fixture.stablePlan !== undefined) {
    return detachedRestoreStablePlanIdClaimRow(fixture, {
      materializedAt: fixture.preparedAt,
    });
  }
  return operationIdRegistryRow({
    claimedAt: fixture.preparedAt,
    claimantOperationId: null,
    materializedAt: fixture.preparedAt,
    operationId: fixture.options.operationId,
    sessionId: fixture.options.expectedSession.sessionId,
  });
}

function restoreGenerationTerminalResult(fixture, completion = fixture.completion) {
  return {
    catalogueSha256: sha256(
      JSON.stringify(checkpointCatalogueDocument(fixture.source)),
    ),
    checkpointId: fixture.source.checkpoint.checkpointId,
    generationDocumentSha256: sha256(
      JSON.stringify(restoreGenerationDocument(fixture, completion)),
    ),
    generationId: fixture.generationId,
    outcome: "restore-generation-committed",
    resultVersion: 1,
  };
}

function restoreGenerationOperationRow(
  fixture,
  state,
  {
    completion = fixture.completion,
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
        ? fixture.preparedAt
        : state === "starting"
          ? fixture.dispatchAt
          : state === "uncertain"
            ? RESTORE_UNCERTAIN_NOW
            : fixture.committedAt,
    result =
      state === "committed"
        ? restoreGenerationTerminalResult(fixture, completion)
        : null,
  } = {},
) {
  return operationRow(state, {
    options: fixture.options,
    revision,
    createdAt: fixture.preparedAt,
    updatedAt,
    result,
    retiredAt: state === "committed" ? updatedAt : null,
  });
}

function restoreGenerationReservationRow(
  fixture,
  state,
  {
    updatedAt =
      state === "prepared"
        ? fixture.preparedAt
        : state === "starting"
          ? fixture.dispatchAt
          : state === "uncertain"
            ? RESTORE_UNCERTAIN_NOW
            : fixture.committedAt,
  } = {},
) {
  return reservationRow(state, {
    options: fixture.options,
    createdAt: fixture.preparedAt,
    updatedAt,
    releasedAt: state === "released" ? updatedAt : null,
  });
}

function restoreGenerationPhaseSessionRow(fixture, state) {
  const operationRevision =
    state === "prepared" ? "0" : state === "starting" ? "1" : "2";
  const updatedAt =
    state === "prepared"
      ? RESTORE_PREPARED_NOW
      : state === "starting"
        ? RESTORE_DISPATCH_NOW
        : RESTORE_UNCERTAIN_NOW;
  return sessionRow({
    sessionId: fixture.options.expectedSession.sessionId,
    revision: (
      BigInt(fixture.options.expectedSession.revision) +
      BigInt(operationRevision) +
      1n
    ).toString(),
    sessionDocument: document(fixture.options.expectedSession.sessionId, {
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

function restoreGenerationCommittedSessionRow(
  fixture,
  {
    completion = fixture.completion,
    operationRevision = "3",
    updatedAt = fixture.committedAt,
  } = {},
) {
  const result = restoreGenerationTerminalResult(fixture, completion);
  return sessionRow({
    sessionId: fixture.options.expectedSession.sessionId,
    revision: (
      BigInt(fixture.options.expectedSession.revision) +
      BigInt(operationRevision) +
      1n
    ).toString(),
    sessionDocument: document(fixture.options.expectedSession.sessionId, {
      ...structuredClone(fixture.options.expectedSession.document),
      activeOperation: null,
      lastOperation: terminalPointer({
        options: fixture.options,
        operationRevision,
        result,
      }),
    }),
    createdAt: fixture.options.expectedSession.createdAt,
    updatedAt,
  });
}

function restoreCheckpointSourceSteps(fixture) {
  return [
    rows(checkpointCatalogueRow(fixture.source)),
    rows(checkpointCaptureAttemptRow(fixture.source)),
    rows(checkpointCaptureOperationRow(fixture.source, "committed")),
    rows(checkpointCaptureReservationRow(fixture.source, "released")),
    rows(checkpointCaptureAttemptRow(fixture.source)),
    rows(),
    rows(checkpointCatalogueRow(fixture.source)),
  ];
}

function restoreGenerationActiveSteps(
  fixture,
  state,
  { generation = undefined, launchIdClaim = undefined } = {},
) {
  const durableGeneration =
    generation === undefined
      ? state === "prepared"
        ? null
        : restoreGenerationRow(fixture)
      : generation;
  const steps = [
    rows(restoreGenerationPhaseSessionRow(fixture, state)),
    rows(restoreGenerationOperationRow(fixture, state)),
    rows(restoreGenerationReservationRow(fixture, state)),
    durableGeneration === null ? rows() : rows(durableGeneration),
  ];
  if (state !== "prepared") {
    steps.push(...restoreCheckpointSourceSteps(fixture));
  }
  const durableLaunchIdClaim =
    launchIdClaim === undefined &&
    fixture.request.contractVersion === 2 &&
    state !== "prepared"
      ? restoreLaunchIdClaimRow(fixture, {
          materializedAt:
            state === "committed" ? RESTORE_FINALIZE_NOW : null,
        })
      : launchIdClaim;
  if (durableLaunchIdClaim !== undefined) {
    steps.push(
      durableLaunchIdClaim === null
        ? rows()
        : rows(durableLaunchIdClaim),
    );
  }
  steps.push(
    rows(fixture.writer.committedOperation),
    rows(fixture.writer.releasedReservation),
  );
  return steps;
}

function restoreGenerationDispatchReadSteps(
  fixture,
  state,
  { operationIdClaim = undefined, ...relationOptions } = {},
) {
  const durableOperationIdClaim =
    operationIdClaim === undefined
      ? restoreGenerationOperationIdClaimRow(fixture)
      : operationIdClaim;
  return [
    ...restoreGenerationActiveSteps(fixture, state, relationOptions),
    durableOperationIdClaim === null
      ? rows()
      : rows(durableOperationIdClaim),
  ];
}

function restoreGenerationCommittedSteps(
  fixture,
  {
    completion = fixture.completion,
    launchIdClaim = undefined,
    operationRevision = "3",
  } = {},
) {
  const steps = [
    rows(
      restoreGenerationCommittedSessionRow(fixture, {
        completion,
        operationRevision,
      }),
    ),
    rows({ operation_count: 0, reservation_count: 0 }),
    rows(
      restoreGenerationOperationRow(fixture, "committed", {
        completion,
        revision: operationRevision,
      }),
    ),
    rows(restoreGenerationReservationRow(fixture, "released")),
    rows(restoreGenerationRow(fixture, "committed", {
      document: restoreGenerationDocument(fixture, completion),
    })),
    ...restoreCheckpointSourceSteps(fixture),
  ];
  const durableLaunchIdClaim =
    launchIdClaim === undefined && fixture.request.contractVersion === 2
      ? restoreLaunchIdClaimRow(fixture, {
          materializedAt: RESTORE_FINALIZE_NOW,
        })
      : launchIdClaim;
  if (durableLaunchIdClaim !== undefined) {
    steps.push(
      durableLaunchIdClaim === null
        ? rows()
        : rows(durableLaunchIdClaim),
    );
  }
  return steps;
}

function restoreGenerationCommittedDispatchReadSteps(
  fixture,
  options = {},
) {
  return [
    ...restoreGenerationCommittedSteps(fixture, options),
    rows(restoreGenerationOperationIdClaimRow(fixture)),
  ];
}

function restoreGenerationCancelledFixture(fixture) {
  const reason = "caller-abandoned-before-restore-dispatch";
  const result = cancellationResult(reason);
  const operation = restoreGenerationOperationRow(fixture, "committed", {
    revision: "1",
    updatedAt: RESTORE_CANCEL_NOW,
    result,
  });
  const reservation = restoreGenerationReservationRow(fixture, "released", {
    updatedAt: RESTORE_CANCEL_NOW,
  });
  const session = sessionRow({
    sessionId: fixture.options.expectedSession.sessionId,
    revision: (BigInt(fixture.options.expectedSession.revision) + 2n).toString(),
    sessionDocument: document(fixture.options.expectedSession.sessionId, {
      ...structuredClone(fixture.options.expectedSession.document),
      activeOperation: null,
      lastOperation: terminalPointer({
        options: fixture.options,
        operationRevision: "1",
        result,
      }),
    }),
    createdAt: fixture.options.expectedSession.createdAt,
    updatedAt: RESTORE_CANCEL_NOW,
  });
  return { operation, reason, reservation, result, session };
}

function restoreGenerationCancelledSteps(fixture, cancelled) {
  return [
    rows(cancelled.session),
    rows({ operation_count: 0, reservation_count: 0 }),
    rows(cancelled.operation),
    rows(cancelled.reservation),
    rows(),
    rows(restoreGenerationOperationIdClaimRow(fixture)),
  ];
}

function writerLaunchGenerationSnapshot(restore) {
  return {
    binding: canonicalPayload(restoreGenerationBinding(restore)),
    checkpointId: restore.source.checkpoint.checkpointId,
    claimedAt: restore.dispatchAt,
    committedAt: restore.committedAt,
    document: canonicalPayload(restoreGenerationDocument(restore)),
    generationId: restore.generationId,
    operationId: restore.options.operationId,
    sessionId: restore.options.expectedSession.sessionId,
    state: "committed",
  };
}

function writerLaunchMeasuredImage(expectedSession) {
  const runtime = expectedSession.document.manifest.runtime;
  const [, architecture] = runtime.platform.split("/");
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
        os: "linux",
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

function writerLaunchFixture({
  destinationIsolationProofId = DESTINATION_ISOLATION_PROOF_ID,
  expectedSession: suppliedExpectedSession,
  generationId = RESTORE_GENERATION_ID,
  launchOperationId = LAUNCH_ATTEMPT_OPERATION_ID,
  restoreCommittedAt = RESTORE_FINALIZE_NOW,
  restoreDispatchAt = RESTORE_DISPATCH_NOW,
  restoreExpectedSession = null,
  restoreLaunchAttemptId = null,
  restoreOperationId = RESTORE_OPERATION_ID,
  restorePreparedAt = RESTORE_PREPARED_NOW,
  sessionId = SESSION_ID,
  sessionDocumentVersion = SESSION_AUTHORITY_DOCUMENT_VERSION,
} = {}) {
  const restore = restoreGenerationFixture({
    committedAt: restoreCommittedAt,
    dispatchAt: restoreDispatchAt,
    destinationIsolationProofId,
    expectedSession: restoreExpectedSession,
    generationId,
    launchAttemptId: restoreLaunchAttemptId,
    operationId: restoreOperationId,
    preparedAt: restorePreparedAt,
    sessionId,
  });
  const committedSession = restoreGenerationCommittedSessionRow(restore, {
    operationRevision: "2",
  });
  const expectedSession =
    suppliedExpectedSession === undefined
      ? snapshotFromSessionRow(committedSession)
      : structuredClone(suppliedExpectedSession);
  if (sessionDocumentVersion === 2) {
    expectedSession.document = versionTwoDocument(sessionId, {
      ...expectedSession.document,
    });
  }
  const generation = writerLaunchGenerationSnapshot(restore);
  const measuredImage = writerLaunchMeasuredImage(expectedSession);
  const supervisor = {
    contractVersion: 1,
    supervisorId: SUPERVISOR_ID,
  };
  const request = createWriterLaunchAttemptOperationRequest({
    expectedSession,
    generation,
    measuredImage,
    supervisor,
  });
  const options = {
    expectedSession,
    kind: WRITER_LAUNCH_ATTEMPT_OPERATION_KIND,
    operationId: launchOperationId,
    request,
  };
  return {
    generation,
    measuredImage,
    options,
    request,
    restore,
    supervisor,
  };
}

function restoreLaunchHandoffFixture({
  expectedSessionRevision = null,
  launchOperationId = LAUNCH_ATTEMPT_OPERATION_ID,
  restoreOperationId = RESTORE_OPERATION_ID,
  restoreOperationRevision = "2",
} = {}) {
  const restore = restoreGenerationFixture({
    expectedSessionRevision,
    launchAttemptId: launchOperationId,
    operationId: restoreOperationId,
  });
  const expectedSession = snapshotFromSessionRow(
    restoreGenerationCommittedSessionRow(restore, {
      operationRevision: restoreOperationRevision,
    }),
  );
  const generation = writerLaunchGenerationSnapshot(restore);
  const request = createWriterLaunchAttemptOperationRequest({
    expectedSession,
    generation,
    measuredImage: restore.launchIntent.measuredImage,
    supervisor: restore.launchIntent.supervisor,
  });
  const options = {
    expectedSession,
    kind: WRITER_LAUNCH_ATTEMPT_OPERATION_KIND,
    operationId: launchOperationId,
    request,
  };
  return {
    generation,
    measuredImage: restore.launchIntent.measuredImage,
    options,
    request,
    restore,
    supervisor: restore.launchIntent.supervisor,
  };
}

function writerLaunchEvidence(fixture, status = "started", overrides = {}) {
  return {
    contractVersion: 1,
    launchAttemptId: fixture.options.operationId,
    processIncarnationId:
      status === "not-started" ? null : PROCESS_INCARNATION_ID,
    proofId: SUPERVISOR_PROOF_ID,
    status,
    supervisorId: fixture.supervisor.supervisorId,
    writerIncarnationId:
      status === "not-started" ? null : WRITER_INCARNATION_ID,
    ...overrides,
  };
}

function writerLaunchResult(fixture, status = "started", overrides = {}) {
  const outcomes = {
    "complete-stopped": "writer-launch-complete-stopped",
    "not-started": "writer-launch-not-started",
    started: "writer-launch-started",
  };
  return {
    evidence: writerLaunchEvidence(fixture, status),
    outcome: outcomes[status],
    resultVersion: 1,
    ...overrides,
  };
}

function writerLaunchPointer(
  fixture,
  result = writerLaunchResult(fixture),
  startedAt = LAUNCH_FINALIZE_NOW,
) {
  const { attachment, lease } = fixture.options.expectedSession.document;
  return {
    attachmentId: attachment.attachmentId,
    attachmentSha256: canonicalSha256(attachment),
    contractVersion: 1,
    fencingEpoch: lease.fencingEpoch,
    generation: structuredClone(fixture.request.generation),
    launchAttemptId: fixture.options.operationId,
    launchResultSha256: canonicalSha256(result),
    leaseId: lease.leaseId,
    leaseSha256: canonicalSha256(lease),
    measuredImageSha256: canonicalSha256(fixture.request.measuredImage),
    processIncarnationId: result.evidence.processIncarnationId,
    startedAt,
    supervisorId: result.evidence.supervisorId,
    supervisorProofId: result.evidence.proofId,
    writerIncarnationId: result.evidence.writerIncarnationId,
  };
}

function writerLaunchOperationRow(
  fixture,
  state,
  {
    createdAt = LAUNCH_PREPARED_NOW,
    result = state === "committed" ? writerLaunchResult(fixture) : null,
    revision =
      state === "prepared"
        ? "0"
        : state === "starting"
          ? "1"
          : state === "uncertain"
            ? "2"
            : "2",
    updatedAt =
      state === "prepared"
        ? LAUNCH_PREPARED_NOW
        : state === "starting"
          ? LAUNCH_DISPATCH_NOW
          : state === "uncertain"
            ? LAUNCH_UNCERTAIN_NOW
            : LAUNCH_FINALIZE_NOW,
  } = {},
) {
  return operationRow(state, {
    options: fixture.options,
    revision,
    createdAt,
    updatedAt,
    result,
    retiredAt: state === "committed" ? updatedAt : null,
  });
}

function writerLaunchReservationRow(
  fixture,
  state,
  {
    createdAt = LAUNCH_PREPARED_NOW,
    updatedAt =
      state === "prepared"
        ? LAUNCH_PREPARED_NOW
        : state === "starting"
          ? LAUNCH_DISPATCH_NOW
          : state === "uncertain"
            ? LAUNCH_UNCERTAIN_NOW
            : LAUNCH_FINALIZE_NOW,
  } = {},
) {
  return reservationRow(state, {
    options: fixture.options,
    createdAt,
    updatedAt,
    releasedAt: state === "released" ? updatedAt : null,
  });
}

function writerLaunchPhaseSessionRow(
  fixture,
  state,
  { updatedAt: suppliedUpdatedAt } = {},
) {
  const operationRevision =
    state === "prepared" ? "0" : state === "starting" ? "1" : "2";
  const updatedAt =
    suppliedUpdatedAt ??
    (state === "prepared"
      ? LAUNCH_PREPARED_NOW
      : state === "starting"
        ? LAUNCH_DISPATCH_NOW
        : LAUNCH_UNCERTAIN_NOW);
  return sessionRow({
    sessionId: fixture.options.expectedSession.sessionId,
    revision: (
      BigInt(fixture.options.expectedSession.revision) +
      BigInt(operationRevision) +
      1n
    ).toString(),
    sessionDocument: document(fixture.options.expectedSession.sessionId, {
      ...structuredClone(fixture.options.expectedSession.document),
      documentVersion: SESSION_AUTHORITY_DOCUMENT_VERSION,
      activeOperation: activeOperation(state, {
        options: fixture.options,
        operationRevision,
      }),
    }),
    createdAt: fixture.options.expectedSession.createdAt,
    updatedAt,
  });
}

function restoreLaunchHandoffActiveSteps(
  fixture,
  { launchIdClaim = undefined } = {},
) {
  const restoreOperationRevision =
    fixture.options.expectedSession.document.lastOperation.operationRevision;
  const steps = [
    rows(
      writerLaunchPhaseSessionRow(fixture, "prepared", {
        updatedAt: RESTORE_FINALIZE_NOW,
      }),
    ),
    rows(
      writerLaunchOperationRow(fixture, "prepared", {
        createdAt: RESTORE_FINALIZE_NOW,
        updatedAt: RESTORE_FINALIZE_NOW,
      }),
    ),
    rows(
      writerLaunchReservationRow(fixture, "prepared", {
        createdAt: RESTORE_FINALIZE_NOW,
        updatedAt: RESTORE_FINALIZE_NOW,
      }),
    ),
    rows(
      restoreGenerationOperationRow(fixture.restore, "committed", {
        revision: restoreOperationRevision,
      }),
    ),
    rows(restoreGenerationReservationRow(fixture.restore, "released")),
    rows(restoreGenerationRow(fixture.restore, "committed")),
    ...restoreCheckpointSourceSteps(fixture.restore),
  ];
  const durableLaunchIdClaim =
    launchIdClaim === undefined
      ? restoreLaunchIdClaimRow(fixture, {
          materializedAt: RESTORE_FINALIZE_NOW,
        })
      : launchIdClaim;
  steps.push(
    durableLaunchIdClaim === null
      ? rows()
      : rows(durableLaunchIdClaim),
  );
  return steps;
}

function restoreLaunchHandoffRestoreSteps(fixture, state) {
  const restore = fixture.restore ?? fixture;
  return restoreGenerationActiveSteps(restore, state, {
    launchIdClaim: restoreLaunchIdClaimRow(restore),
  });
}

function restoreLaunchHandoffWriteSteps(
  fixture,
  { finalSession = undefined } = {},
) {
  const restore = fixture.restore;
  const restoreOperationRevision =
    fixture.options.expectedSession.document.lastOperation.operationRevision;
  return [
    rows(restoreGenerationRow(restore, "committed")),
    rows(
      restoreGenerationOperationRow(restore, "committed", {
        revision: restoreOperationRevision,
      }),
    ),
    rows(restoreGenerationReservationRow(restore, "released")),
    rows(
      restoreGenerationCommittedSessionRow(restore, {
        operationRevision: restoreOperationRevision,
      }),
    ),
    rows(
      writerLaunchOperationRow(fixture, "prepared", {
        createdAt: RESTORE_FINALIZE_NOW,
        updatedAt: RESTORE_FINALIZE_NOW,
      }),
    ),
    rows(
      writerLaunchReservationRow(fixture, "prepared", {
        createdAt: RESTORE_FINALIZE_NOW,
        updatedAt: RESTORE_FINALIZE_NOW,
      }),
    ),
    rows(
      restoreLaunchIdClaimRow(fixture, {
        materializedAt: RESTORE_FINALIZE_NOW,
      }),
    ),
    finalSession === undefined
      ? rows(
          writerLaunchPhaseSessionRow(fixture, "prepared", {
            updatedAt: RESTORE_FINALIZE_NOW,
          }),
        )
      : finalSession,
  ];
}

function writerLaunchCommittedSessionRow(
  fixture,
  {
    operationRevision = "2",
    result = writerLaunchResult(fixture),
    updatedAt = LAUNCH_FINALIZE_NOW,
  } = {},
) {
  return sessionRow({
    sessionId: fixture.options.expectedSession.sessionId,
    revision: (
      BigInt(fixture.options.expectedSession.revision) +
      BigInt(operationRevision) +
      1n
    ).toString(),
    sessionDocument: document(fixture.options.expectedSession.sessionId, {
      ...structuredClone(fixture.options.expectedSession.document),
      documentVersion: SESSION_AUTHORITY_DOCUMENT_VERSION,
      activeOperation: null,
      lastOperation: terminalPointer({
        options: fixture.options,
        operationRevision,
        result,
      }),
      launch:
        result.outcome === "writer-launch-started"
          ? writerLaunchPointer(fixture, result, updatedAt)
          : null,
    }),
    createdAt: fixture.options.expectedSession.createdAt,
    updatedAt,
  });
}

function writerLaunchCancelledFixture(
  fixture,
  {
    createdAt = LAUNCH_PREPARED_NOW,
    reason = "caller-abandoned-before-launch-dispatch",
    updatedAt = LAUNCH_FINALIZE_NOW,
  } = {},
) {
  const result = cancellationResult(reason);
  return {
    operation: writerLaunchOperationRow(fixture, "committed", {
      createdAt,
      result,
      revision: "1",
      updatedAt,
    }),
    reservation: writerLaunchReservationRow(fixture, "released", {
      createdAt,
      updatedAt,
    }),
    result,
    session: writerLaunchCommittedSessionRow(fixture, {
      operationRevision: "1",
      result,
      updatedAt,
    }),
  };
}

function writerLaunchBaseSessionRow(fixture) {
  const expected = fixture.options.expectedSession;
  return sessionRow({
    sessionId: expected.sessionId,
    revision: expected.revision,
    sessionDocument: expected.document,
    createdAt: expected.createdAt,
    updatedAt: expected.updatedAt,
  });
}

function writerLaunchBaseSteps(fixture) {
  const restoreOperationRevision =
    fixture.options.expectedSession.document.lastOperation.operationRevision;
  const steps = [
    rows(writerLaunchBaseSessionRow(fixture)),
    rows({ operation_count: 0, reservation_count: 0 }),
    rows(
      restoreGenerationOperationRow(fixture.restore, "committed", {
        revision: restoreOperationRevision,
      }),
    ),
    rows(restoreGenerationReservationRow(fixture.restore, "released")),
    rows(restoreGenerationRow(fixture.restore, "committed")),
    ...restoreCheckpointSourceSteps(fixture.restore),
  ];
  if (fixture.restore.request.contractVersion === 2) {
    steps.push(
      rows(
        restoreLaunchIdClaimRow(fixture, {
          materializedAt: RESTORE_FINALIZE_NOW,
        }),
      ),
    );
  }
  return steps;
}

function writerLaunchGenerationReferenceSteps(fixture) {
  const restoreOperationRevision =
    fixture.options.expectedSession.document.lastOperation.operationRevision;
  return [
    rows(restoreGenerationRow(fixture.restore, "committed")),
    rows(
      restoreGenerationOperationRow(fixture.restore, "committed", {
        revision: restoreOperationRevision,
      }),
    ),
    ...restoreCheckpointSourceSteps(fixture.restore),
    rows(restoreGenerationReservationRow(fixture.restore, "released")),
  ];
}

function writerLaunchActiveSteps(
  fixture,
  state,
  { createdAt = LAUNCH_PREPARED_NOW } = {},
) {
  const restoreOperationRevision =
    fixture.options.expectedSession.document.lastOperation.operationRevision;
  const steps = [
    rows(writerLaunchPhaseSessionRow(fixture, state)),
    rows(writerLaunchOperationRow(fixture, state, { createdAt })),
    rows(writerLaunchReservationRow(fixture, state, { createdAt })),
  ];
  if (state !== "prepared") {
    steps.push(...writerLaunchGenerationReferenceSteps(fixture));
  }
  steps.push(
    rows(
      restoreGenerationOperationRow(fixture.restore, "committed", {
        revision: restoreOperationRevision,
      }),
    ),
    rows(restoreGenerationReservationRow(fixture.restore, "released")),
    rows(restoreGenerationRow(fixture.restore, "committed")),
    ...restoreCheckpointSourceSteps(fixture.restore),
  );
  if (fixture.restore.request.contractVersion === 2) {
    steps.push(
      rows(
        restoreLaunchIdClaimRow(fixture, {
          materializedAt: RESTORE_FINALIZE_NOW,
        }),
      ),
    );
  }
  return steps;
}

function writerLaunchCommittedSteps(
  fixture,
  {
    createdAt = LAUNCH_PREPARED_NOW,
    operationRevision = "2",
    result = writerLaunchResult(fixture),
    updatedAt = LAUNCH_FINALIZE_NOW,
  } = {},
) {
  const steps = [
    rows(
      writerLaunchCommittedSessionRow(fixture, {
        operationRevision,
        result,
        updatedAt,
      }),
    ),
    rows({ operation_count: 0, reservation_count: 0 }),
    rows(
      writerLaunchOperationRow(fixture, "committed", {
        createdAt,
        result,
        revision: operationRevision,
        updatedAt,
      }),
    ),
    rows(
      writerLaunchReservationRow(fixture, "released", {
        createdAt,
        updatedAt,
      }),
    ),
    ...writerLaunchGenerationReferenceSteps(fixture),
  ];
  if (result.outcome === "writer-launch-started") {
    steps.push(
      rows(
        writerLaunchOperationRow(fixture, "committed", {
          createdAt,
          result,
          revision: operationRevision,
          updatedAt,
        }),
      ),
      rows(
        writerLaunchReservationRow(fixture, "released", {
          createdAt,
          updatedAt,
        }),
      ),
      ...writerLaunchGenerationReferenceSteps(fixture),
    );
  }
  return steps;
}

function writerLaunchCommittedRelationSteps(
  fixture,
  {
    createdAt = LAUNCH_PREPARED_NOW,
    operationRevision = "2",
    result = writerLaunchResult(fixture),
    updatedAt = LAUNCH_FINALIZE_NOW,
  } = {},
) {
  return [
    rows(
      writerLaunchOperationRow(fixture, "committed", {
        createdAt,
        result,
        revision: operationRevision,
        updatedAt,
      }),
    ),
    rows(
      writerLaunchReservationRow(fixture, "released", {
        createdAt,
        updatedAt,
      }),
    ),
    ...writerLaunchGenerationReferenceSteps(fixture),
  ];
}

function writerLaunchStopFixture({
  claimToken = STOP_CLAIM_TOKEN,
  contractVersion = 2,
  launch = writerLaunchFixture(),
  stopOperationId = STOP_OPERATION_ID,
} = {}) {
  const launchResult = writerLaunchResult(launch);
  const expectedSession = snapshotFromSessionRow(
    writerLaunchCommittedSessionRow(launch, { result: launchResult }),
  );
  const captureIntent =
    contractVersion === 3
      ? checkpointCaptureFixture({
          processIncarnationId:
            expectedSession.document.launch.processIncarnationId,
          stopOperationId,
          writer: {
            expectedSession,
            lease: expectedSession.document.lease,
          },
          writerIncarnationId:
            expectedSession.document.launch.writerIncarnationId,
        })
      : null;
  const request = createWriterLaunchStopOperationRequest(
    contractVersion === 1
      ? { expectedSession }
      : contractVersion === 2
        ? { claimToken, expectedSession }
        : {
            captureIntent: captureIntent.request,
            claimToken,
            expectedSession,
          },
  );
  const options = {
    expectedSession,
    kind: WRITER_LAUNCH_STOP_OPERATION_KIND,
    operationId: stopOperationId,
    request,
  };
  const evidence = {
    contractVersion: 1,
    launchAttemptId: request.launch.launchAttemptId,
    processIncarnationId: request.launch.processIncarnationId,
    proofId: "supervisor-stop-proof-001",
    status: "complete-stopped",
    supervisorId: request.launch.supervisorId,
    writerIncarnationId: request.launch.writerIncarnationId,
  };
  const result = {
    evidence,
    outcome: "writer-launch-stopped",
    resultVersion: 1,
  };
  return {
    captureIntent,
    claimToken: contractVersion === 1 ? null : claimToken,
    evidence,
    launch,
    launchResult,
    options,
    request,
    result,
  };
}

function writerLaunchStopCaptureFixture(fixture) {
  assert.equal(fixture.request.contractVersion, 3);
  const expectedSession = snapshotFromSessionRow(
    writerLaunchStopCommittedSessionRow(fixture),
  );
  const request = fixture.request.captureIntent;
  return {
    ...fixture.captureIntent,
    options: {
      expectedSession,
      kind: CHECKPOINT_CAPTURE_OPERATION_KIND,
      operationId: request.admission.request.operationId,
      request,
    },
    request,
  };
}

function writerLaunchStopCaptureIdClaimRow(
  fixture,
  { materializedAt = null } = {},
) {
  return operationIdRegistryRow({
    binding: fixture.request.captureIntent,
    claimType: "writer-stop-capture-intent-v3",
    claimedAt: LAUNCH_STOP_DISPATCH_NOW,
    claimantOperationId: fixture.options.operationId,
    materializedAt,
    operationId: fixture.request.captureIntent.admission.request.operationId,
    sessionId: fixture.options.expectedSession.sessionId,
  });
}

function writerLaunchStopOperationRow(
  fixture,
  state,
  {
    createdAt = LAUNCH_STOP_PREPARED_NOW,
    result = state === "committed" ? fixture.result : null,
    revision =
      state === "prepared"
        ? "0"
        : state === "starting"
          ? "1"
          : state === "uncertain"
            ? "2"
            : "2",
    updatedAt =
      state === "prepared"
        ? LAUNCH_STOP_PREPARED_NOW
        : state === "starting"
          ? LAUNCH_STOP_DISPATCH_NOW
          : state === "uncertain"
            ? LAUNCH_STOP_UNCERTAIN_NOW
            : LAUNCH_STOP_FINALIZE_NOW,
  } = {},
) {
  return operationRow(state, {
    options: fixture.options,
    revision,
    createdAt,
    updatedAt,
    result,
    retiredAt: state === "committed" ? updatedAt : null,
  });
}

function writerLaunchStopReservationRow(
  fixture,
  state,
  {
    createdAt = LAUNCH_STOP_PREPARED_NOW,
    updatedAt =
      state === "prepared"
        ? LAUNCH_STOP_PREPARED_NOW
        : state === "starting"
          ? LAUNCH_STOP_DISPATCH_NOW
          : state === "uncertain"
            ? LAUNCH_STOP_UNCERTAIN_NOW
            : LAUNCH_STOP_FINALIZE_NOW,
  } = {},
) {
  return reservationRow(state, {
    options: fixture.options,
    createdAt,
    updatedAt,
    releasedAt: state === "released" ? updatedAt : null,
  });
}

function writerLaunchStopPhaseSessionRow(
  fixture,
  state,
  {
    updatedAt =
      state === "prepared"
        ? LAUNCH_STOP_PREPARED_NOW
        : state === "starting"
          ? LAUNCH_STOP_DISPATCH_NOW
          : LAUNCH_STOP_UNCERTAIN_NOW,
  } = {},
) {
  const operationRevision =
    state === "prepared" ? "0" : state === "starting" ? "1" : "2";
  return sessionRow({
    sessionId: fixture.options.expectedSession.sessionId,
    revision: (
      BigInt(fixture.options.expectedSession.revision) +
      BigInt(operationRevision) +
      1n
    ).toString(),
    sessionDocument: document(fixture.options.expectedSession.sessionId, {
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

function writerLaunchStopCommittedSessionRow(
  fixture,
  { operationRevision = "2", updatedAt = LAUNCH_STOP_FINALIZE_NOW } = {},
) {
  return sessionRow({
    sessionId: fixture.options.expectedSession.sessionId,
    revision: (
      BigInt(fixture.options.expectedSession.revision) +
      BigInt(operationRevision) +
      1n
    ).toString(),
    sessionDocument: document(fixture.options.expectedSession.sessionId, {
      ...structuredClone(fixture.options.expectedSession.document),
      activeOperation: null,
      lastOperation: terminalPointer({
        options: fixture.options,
        operationRevision,
        result: fixture.result,
      }),
      launch: null,
    }),
    createdAt: fixture.options.expectedSession.createdAt,
    updatedAt,
  });
}

function writerLaunchStopActiveSteps(fixture, state, timing = {}) {
  const firstLaunchRelation = writerLaunchCommittedRelationSteps(
    fixture.launch,
    { result: fixture.launchResult },
  );
  return [
    rows(writerLaunchStopPhaseSessionRow(fixture, state, timing)),
    rows(writerLaunchStopOperationRow(fixture, state, timing)),
    rows(writerLaunchStopReservationRow(fixture, state, timing)),
    ...firstLaunchRelation,
    ...(fixture.request.contractVersion === 3
      ? [
          state === "prepared"
            ? rows()
            : rows(writerLaunchStopCaptureIdClaimRow(fixture)),
        ]
      : []),
    ...writerLaunchCommittedRelationSteps(fixture.launch, {
      result: fixture.launchResult,
    }),
    ...writerLaunchCommittedRelationSteps(fixture.launch, {
      result: fixture.launchResult,
    }),
  ];
}

function writerLaunchStopCaptureActiveSteps(
  fixture,
  capture,
  state = "prepared",
) {
  const timing = {
    createdAt: LAUNCH_STOP_FINALIZE_NOW,
    updatedAt:
      state === "prepared"
        ? LAUNCH_STOP_FINALIZE_NOW
        : state === "starting"
          ? LAUNCH_STOP_CAPTURE_DISPATCH_NOW
          : LAUNCH_STOP_CAPTURE_UNCERTAIN_NOW,
  };
  return [
    rows(checkpointCapturePhaseSessionRow(capture, state, timing)),
    ...writerLaunchStopCaptureRelationSteps(
      fixture,
      capture,
      state,
    ),
  ];
}

function writerLaunchStopCaptureRelationSteps(
  fixture,
  capture,
  state = "prepared",
) {
  const timing = {
    createdAt: LAUNCH_STOP_FINALIZE_NOW,
    updatedAt:
      state === "prepared"
        ? LAUNCH_STOP_FINALIZE_NOW
        : state === "starting"
          ? LAUNCH_STOP_CAPTURE_DISPATCH_NOW
          : LAUNCH_STOP_CAPTURE_UNCERTAIN_NOW,
  };
  return [
    rows(checkpointCaptureOperationRow(capture, state, timing)),
    rows(checkpointCaptureReservationRow(capture, state, timing)),
    state === "prepared"
      ? rows()
      : rows(
          checkpointCaptureAttemptRow(capture, {
            claimedAt: LAUNCH_STOP_CAPTURE_DISPATCH_NOW,
          }),
        ),
    rows(),
    ...(state === "prepared" ? [] : [rows()]),
    rows(writerLaunchStopOperationRow(fixture, "committed")),
    rows(writerLaunchStopReservationRow(fixture, "released")),
    ...writerLaunchCommittedRelationSteps(fixture.launch, {
      result: fixture.launchResult,
    }),
    rows(
      writerLaunchStopCaptureIdClaimRow(fixture, {
        materializedAt: LAUNCH_STOP_FINALIZE_NOW,
      }),
    ),
  ];
}

function writerLaunchStopCommittedSteps(
  fixture,
  { operationRevision = "2" } = {},
) {
  return [
    rows(
      writerLaunchStopCommittedSessionRow(fixture, { operationRevision }),
    ),
    rows({ operation_count: 0, reservation_count: 0 }),
    rows(
      writerLaunchStopOperationRow(fixture, "committed", {
        revision: operationRevision,
      }),
    ),
    rows(writerLaunchStopReservationRow(fixture, "released")),
    ...writerLaunchCommittedRelationSteps(fixture.launch, {
      result: fixture.launchResult,
    }),
  ];
}

function restoreAttachmentActivationFixture() {
  const launch = writerLaunchFixture();
  const stop = writerLaunchStopFixture({ launch });
  const stoppedSession = snapshotFromSessionRow(
    writerLaunchStopCommittedSessionRow(stop),
  );
  const releaseOptions = writerReleaseOptions(
    {
      expectedSession: stoppedSession,
      result: {
        attachment: structuredClone(stoppedSession.document.attachment),
      },
    },
    { operationId: RESTORE_ACTIVATION_DETACH_OPERATION_ID },
  );
  const releaseMutationResult = writerReleaseMutationResult(releaseOptions);
  const releaseResult = writerReleaseResult(
    releaseOptions,
    releaseMutationResult,
  );
  const releaseOperation = writerTerminalOperationRow({
    createdAt: RESTORE_ACTIVATION_DETACH_PREPARED_NOW,
    options: releaseOptions,
    result: releaseResult,
    revision: "2",
    updatedAt: RESTORE_ACTIVATION_DETACH_NOW,
  });
  const releaseReservation = reservationRow("released", {
    options: releaseOptions,
    createdAt: RESTORE_ACTIVATION_DETACH_PREPARED_NOW,
    updatedAt: RESTORE_ACTIVATION_DETACH_NOW,
    releasedAt: RESTORE_ACTIVATION_DETACH_NOW,
  });
  const detachedSessionRow = writerDetachedSessionRow({
    operationRevision: "2",
    options: releaseOptions,
    result: releaseResult,
    updatedAt: RESTORE_ACTIVATION_DETACH_NOW,
  });
  const detachedSession = snapshotFromSessionRow(detachedSessionRow);
  const generation = writerLaunchGenerationSnapshot(launch.restore);
  const launchIntent = {
    launchAttemptId: RESTORE_ACTIVATION_LAUNCH_OPERATION_ID,
    measuredImage: writerLaunchMeasuredImage(detachedSession),
    supervisor: {
      contractVersion: 1,
      supervisorId: "restore-activation-supervisor-001",
    },
  };
  const request = createRestoreAttachmentActivationOperationRequest({
    destinationRootPath:
      "/var/lib/portable-codex/restores/activation-session-001",
    expectedSession: detachedSession,
    generation,
    holderId: "restore-activation-host-001",
    launchIntent,
    leaseDurationMilliseconds: 60_000,
    predecessor: {
      attachmentId: releaseResult.attachment.attachmentId,
      detachOperationId: releaseOptions.operationId,
      stopOperationId: stop.options.operationId,
    },
  });
  const options = {
    expectedSession: detachedSession,
    kind: RESTORE_ATTACHMENT_ACTIVATION_OPERATION_KIND,
    operationId: RESTORE_ACTIVATION_OPERATION_ID,
    request,
  };
  return {
    detachedSessionRow,
    generation,
    launch,
    launchIntent,
    options,
    releaseOperation,
    releaseOptions,
    releaseReservation,
    releaseResult,
    request,
    stop,
  };
}

function restoreAttachmentActivationV2Fixture({
  detachKind = "release",
  generationContractVersion = 1,
  generationPointerOperationId = null,
  generationPredecessor = false,
} = {}) {
  const launch = writerLaunchFixture({
    generationId: "restore-activation-current-generation-001",
    launchOperationId: "restore-activation-current-launch-001",
    restoreOperationId: "restore-activation-current-generation-operation-001",
  });
  const stop = writerLaunchStopFixture({
    launch,
    stopOperationId: "restore-activation-current-stop-001",
  });
  const stoppedSession = snapshotFromSessionRow(
    writerLaunchStopCommittedSessionRow(stop),
  );
  const capture = checkpointCaptureFixture({
    artifactId: "restore-activation-capture-artifact-001",
    captureAttemptId: "019f2100-0000-7000-8000-000000000009",
    checkpointId: "restore-activation-capture-checkpoint-001",
    operationId: "restore-activation-capture-operation-001",
    processIncarnationId: stop.request.launch.processIncarnationId,
    publicationId: "restore-activation-capture-publication-001",
    stopOperationId: stop.options.operationId,
    writer: {
      expectedSession: stoppedSession,
      lease: stoppedSession.document.lease,
    },
    writerIncarnationId: stop.request.launch.writerIncarnationId,
  });
  const captureResult = checkpointCaptureTerminalResult(capture);
  const captureOperation = operationRow("committed", {
    options: capture.options,
    revision: "3",
    createdAt: RESTORE_ACTIVATION_CAPTURE_PREPARED_NOW,
    updatedAt: RESTORE_ACTIVATION_CAPTURE_FINALIZE_NOW,
    result: captureResult,
    retiredAt: RESTORE_ACTIVATION_CAPTURE_FINALIZE_NOW,
  });
  const captureReservation = reservationRow("released", {
    options: capture.options,
    createdAt: RESTORE_ACTIVATION_CAPTURE_PREPARED_NOW,
    updatedAt: RESTORE_ACTIVATION_CAPTURE_FINALIZE_NOW,
    releasedAt: RESTORE_ACTIVATION_CAPTURE_FINALIZE_NOW,
  });
  const captureAttempt = {
    ...checkpointCaptureAttemptRow(capture),
    claimed_at: new Date(RESTORE_ACTIVATION_CAPTURE_DISPATCH_NOW),
  };
  const captureCatalogue = {
    ...checkpointCatalogueRow(capture),
    committed_at: new Date(RESTORE_ACTIVATION_CAPTURE_FINALIZE_NOW),
  };
  const captureSession = sessionRow({
    sessionId: stoppedSession.sessionId,
    revision: (BigInt(stoppedSession.revision) + 4n).toString(),
    sessionDocument: document(stoppedSession.sessionId, {
      ...structuredClone(stoppedSession.document),
      activeOperation: null,
      lastOperation: terminalPointer({
        options: capture.options,
        operationRevision: "3",
        result: captureResult,
      }),
    }),
    createdAt: stoppedSession.createdAt,
    updatedAt: RESTORE_ACTIVATION_CAPTURE_FINALIZE_NOW,
  });
  const capturedSession = snapshotFromSessionRow(captureSession);
  const generationProducer = writerLaunchFixture({
    generationId: "restore-activation-target-generation-001",
    launchOperationId: "restore-activation-target-unused-launch-001",
    restoreCommittedAt: generationPredecessor
      ? RESTORE_ACTIVATION_GENERATION_FINALIZE_NOW
      : RESTORE_FINALIZE_NOW,
    restoreDispatchAt: generationPredecessor
      ? RESTORE_ACTIVATION_GENERATION_DISPATCH_NOW
      : RESTORE_DISPATCH_NOW,
    restoreExpectedSession: generationPredecessor ? capturedSession : null,
    restoreLaunchAttemptId:
      generationContractVersion === 2
        ? "restore-activation-target-generation-launch-intent-001"
        : null,
    restoreOperationId: "restore-activation-target-generation-operation-001",
    restorePreparedAt: generationPredecessor
      ? RESTORE_ACTIVATION_GENERATION_PREPARED_NOW
      : RESTORE_PREPARED_NOW,
  });
  assert.deepEqual(
    generationProducer.generation.binding.attachment,
    launch.generation.binding.attachment,
  );
  assert.notEqual(
    generationProducer.generation.generationId,
    launch.generation.generationId,
  );
  assert.equal(
    generationProducer.restore.request.contractVersion,
    generationContractVersion,
  );
  const detachExpectedSession = generationPredecessor
    ? structuredClone(
        snapshotFromSessionRow(
          restoreGenerationCommittedSessionRow(generationProducer.restore, {
            operationRevision: "2",
          }),
        ),
      )
    : capturedSession;
  if (generationPointerOperationId !== null) {
    assert.equal(generationPredecessor, true);
    detachExpectedSession.document.lastOperation.operationId =
      generationPointerOperationId;
  }
  const detachFixture = {
    expectedSession: detachExpectedSession,
    result: {
      attachment: structuredClone(
        detachExpectedSession.document.attachment,
      ),
    },
  };
  const releaseOptions =
    detachKind === "force-fence"
      ? writerForceFenceOptions(detachFixture, {
          operationId: RESTORE_ACTIVATION_DETACH_OPERATION_ID,
        })
      : writerReleaseOptions(detachFixture, {
          operationId: RESTORE_ACTIVATION_DETACH_OPERATION_ID,
        });
  const releaseResult =
    detachKind === "force-fence"
      ? (() => {
          const writerEpoch = (
            BigInt(capturedSession.document.writerEpoch) + 1n
          ).toString();
          return writerForceFenceResult(
            releaseOptions,
            writerEpoch,
            writerForceFenceProof(releaseOptions, writerEpoch),
          );
        })()
      : writerReleaseResult(
          releaseOptions,
          writerReleaseMutationResult(releaseOptions),
        );
  const releaseOperation = writerTerminalOperationRow({
    createdAt: RESTORE_ACTIVATION_DETACH_PREPARED_NOW,
    options: releaseOptions,
    result: releaseResult,
    revision: "2",
    updatedAt: RESTORE_ACTIVATION_DETACH_NOW,
  });
  const releaseReservation = reservationRow("released", {
    options: releaseOptions,
    createdAt: RESTORE_ACTIVATION_DETACH_PREPARED_NOW,
    updatedAt: RESTORE_ACTIVATION_DETACH_NOW,
    releasedAt: RESTORE_ACTIVATION_DETACH_NOW,
  });
  const detachedSessionRow = writerDetachedSessionRow({
    operationRevision: "2",
    options: releaseOptions,
    result: releaseResult,
    updatedAt: RESTORE_ACTIVATION_DETACH_NOW,
  });
  const detachedSession = snapshotFromSessionRow(detachedSessionRow);
  const generation = generationProducer.generation;
  const launchIntent = {
    launchAttemptId: RESTORE_ACTIVATION_LAUNCH_OPERATION_ID,
    measuredImage: writerLaunchMeasuredImage(detachedSession),
    supervisor: {
      contractVersion: 1,
      supervisorId: "restore-activation-supervisor-001",
    },
  };
  const request = createRestoreAttachmentActivationOperationRequestV2({
    destinationRootPath:
      "/var/lib/portable-codex/restores/activation-session-001",
    expectedSession: detachedSession,
    generation,
    holderId: "restore-activation-host-001",
    launchIntent,
    leaseDurationMilliseconds: 60_000,
    predecessor: {
      attachmentId: releaseResult.attachment.attachmentId,
      captureOperationId: capture.options.operationId,
      detachOperationId: releaseOptions.operationId,
      stopOperationId: stop.options.operationId,
    },
  });
  const options = {
    expectedSession: detachedSession,
    kind: RESTORE_ATTACHMENT_ACTIVATION_OPERATION_KIND,
    operationId: RESTORE_ACTIVATION_OPERATION_ID,
    request,
  };
  return {
    capture,
    captureAttempt,
    captureCatalogue,
    captureOperation,
    captureReservation,
    detachedSessionRow,
    generation,
    generationProducer,
    generationPredecessor,
    launch,
    launchIntent,
    options,
    releaseOperation,
    releaseOptions,
    releaseReservation,
    releaseResult,
    request,
    stop,
  };
}

function restoreAttachmentActivationLease(fixture) {
  return writerLease(
    fixture.options,
    RESTORE_ACTIVATION_AUTHORITY_NOW,
  );
}

function restoreAttachmentActivationOperationRow(fixture, state) {
  return operationRow(state, {
    options: fixture.options,
    revision: state === "prepared" ? "0" : "1",
    createdAt: RESTORE_ACTIVATION_PREPARED_NOW,
    updatedAt:
      state === "prepared"
        ? RESTORE_ACTIVATION_PREPARED_NOW
        : RESTORE_ACTIVATION_DISPATCH_NOW,
  });
}

function restoreAttachmentActivationReservationRow(fixture, state) {
  return reservationRow(state, {
    options: fixture.options,
    createdAt: RESTORE_ACTIVATION_PREPARED_NOW,
    updatedAt:
      state === "prepared"
        ? RESTORE_ACTIVATION_PREPARED_NOW
        : RESTORE_ACTIVATION_DISPATCH_NOW,
  });
}

function restoreAttachmentActivationPhaseSessionRow(fixture, state) {
  const expected = fixture.options.expectedSession;
  const starting = state === "starting";
  const lease = starting
    ? restoreAttachmentActivationLease(fixture)
    : null;
  return sessionRow({
    sessionId: expected.sessionId,
    revision: (BigInt(expected.revision) + (starting ? 2n : 1n)).toString(),
    sessionDocument: document(expected.sessionId, {
      ...structuredClone(expected.document),
      activeOperation: activeOperation(state, {
        operationRevision: starting ? "1" : "0",
        options: fixture.options,
      }),
      attachment: null,
      launch: null,
      lease,
      lifecycle: starting ? "ATTACHING" : "DETACHED",
      writerEpoch: starting
        ? (BigInt(expected.document.writerEpoch) + 1n).toString()
        : expected.document.writerEpoch,
    }),
    createdAt: expected.createdAt,
    updatedAt:
      starting
        ? RESTORE_ACTIVATION_DISPATCH_NOW
        : RESTORE_ACTIVATION_PREPARED_NOW,
  });
}

function restoreAttachmentActivationLaunchIdClaimRow(
  fixture,
  { materializedAt = null } = {},
) {
  return operationIdRegistryRow({
    binding: canonicalPayload(fixture.launchIntent),
    claimType: "restore-activation-launch-intent-v1",
    claimedAt: RESTORE_ACTIVATION_DISPATCH_NOW,
    claimantOperationId: fixture.options.operationId,
    materializedAt,
    operationId: fixture.launchIntent.launchAttemptId,
    sessionId: fixture.options.expectedSession.sessionId,
  });
}

function restoreAttachmentActivationRelationSteps(
  fixture,
  state,
  {
    launchIdClaim = restoreAttachmentActivationLaunchIdClaimRow(fixture),
  } = {},
) {
  const generationProducer = fixture.generationProducer ?? fixture.launch;
  const steps = [
    ...writerLaunchGenerationReferenceSteps(generationProducer),
    rows(fixture.releaseOperation),
    rows(fixture.releaseReservation),
  ];
  if (fixture.request.contractVersion === 2) {
    steps.push(
      rows(fixture.captureOperation),
      rows(fixture.captureReservation),
      rows(fixture.captureAttempt),
      rows(),
      rows(fixture.captureCatalogue),
    );
  }
  steps.push(
    rows(writerLaunchStopOperationRow(fixture.stop, "committed")),
    rows(writerLaunchStopReservationRow(fixture.stop, "released")),
    ...writerLaunchCommittedRelationSteps(fixture.launch),
  );
  if (state !== "prepared") {
    steps.push(rows(launchIdClaim));
  }
  return steps;
}

function restoreAttachmentActivationActiveSteps(fixture, state) {
  return [
    rows(restoreAttachmentActivationPhaseSessionRow(fixture, state)),
    rows(restoreAttachmentActivationOperationRow(fixture, state)),
    rows(restoreAttachmentActivationReservationRow(fixture, state)),
    ...restoreAttachmentActivationRelationSteps(fixture, state),
    rows(fixture.releaseOperation),
    rows(fixture.releaseReservation),
  ];
}

function restoreAttachmentActivationProviderRequest(fixture) {
  const lease = restoreAttachmentActivationLease(fixture);
  const materialization = fixture.generation.document.materialization;
  return {
    contractVersion: 1,
    lease,
    manifest: structuredClone(
      fixture.options.expectedSession.document.manifest,
    ),
    mutationRequest: writerMutationRequest(fixture.options, lease),
    publication: {
      artifactManifestDigest: materialization.artifactManifestDigest,
      coordinatorBindingSha256: materialization.coordinatorBindingSha256,
      modeledDigest: materialization.modeledDigest,
      publicationId: materialization.publicationId,
      publicationKind: materialization.publicationKind,
      root: {
        ...structuredClone(materialization.stagedRoot),
        rootPath: fixture.request.destinationRootPath,
      },
      treeIdentityDigest: materialization.treeIdentityDigest,
    },
    storageRef: structuredClone(
      fixture.options.expectedSession.document.storageRef,
    ),
  };
}

function restoreAttachmentActivationProviderResult(fixture) {
  const request = restoreAttachmentActivationProviderRequest(fixture);
  const proofId = "restore-attachment-activation-proof-001";
  return {
    attachment: writerAttachment(fixture.options, request.lease, {
      proofId,
      rootPath: fixture.request.destinationRootPath,
    }),
    contractVersion: 1,
    mutationResult: {
      ...structuredClone(request.mutationRequest),
      proofId,
      status: "attached",
    },
    publication: structuredClone(request.publication),
  };
}

function restoreAttachmentActivationTerminalResult(fixture) {
  return {
    activationRequest: canonicalPayload(
      restoreAttachmentActivationProviderRequest(fixture),
    ),
    activationResult: canonicalPayload(
      restoreAttachmentActivationProviderResult(fixture),
    ),
    outcome: "restore-attachment-activated",
    resultVersion: 1,
  };
}

function restoreAttachmentActivationCommittedOperationRow(fixture) {
  return operationRow("committed", {
    options: fixture.options,
    revision: "2",
    createdAt: RESTORE_ACTIVATION_PREPARED_NOW,
    updatedAt: RESTORE_ACTIVATION_FINALIZE_NOW,
    result: restoreAttachmentActivationTerminalResult(fixture),
    retiredAt: RESTORE_ACTIVATION_FINALIZE_NOW,
  });
}

function restoreAttachmentActivationReleasedReservationRow(fixture) {
  return reservationRow("released", {
    options: fixture.options,
    createdAt: RESTORE_ACTIVATION_PREPARED_NOW,
    updatedAt: RESTORE_ACTIVATION_FINALIZE_NOW,
    releasedAt: RESTORE_ACTIVATION_FINALIZE_NOW,
  });
}

function restoreAttachmentActivationTerminalSessionRow(fixture) {
  const expected = fixture.options.expectedSession;
  const result = restoreAttachmentActivationTerminalResult(fixture);
  const activationRequest =
    restoreAttachmentActivationProviderRequest(fixture);
  const activationResult = restoreAttachmentActivationProviderResult(fixture);
  return sessionRow({
    sessionId: expected.sessionId,
    revision: (BigInt(expected.revision) + 3n).toString(),
    sessionDocument: document(expected.sessionId, {
      ...structuredClone(expected.document),
      activeOperation: null,
      attachment: structuredClone(activationResult.attachment),
      lastOperation: terminalPointer({
        operationRevision: "2",
        options: fixture.options,
        result,
      }),
      launch: null,
      lease: structuredClone(activationRequest.lease),
      lifecycle: "ATTACHED",
      writerEpoch: activationRequest.lease.fencingEpoch,
    }),
    createdAt: expected.createdAt,
    updatedAt: RESTORE_ACTIVATION_FINALIZE_NOW,
  });
}

function restoreAttachmentActivationLaunchFixture(fixture) {
  const expectedSession = snapshotFromSessionRow(
    restoreAttachmentActivationTerminalSessionRow(fixture),
  );
  const request = createWriterLaunchAttemptOperationRequest({
    expectedSession,
    generation: fixture.generation,
    measuredImage: fixture.launchIntent.measuredImage,
    supervisor: fixture.launchIntent.supervisor,
  });
  return {
    generation: fixture.generation,
    measuredImage: fixture.launchIntent.measuredImage,
    options: {
      expectedSession,
      kind: WRITER_LAUNCH_ATTEMPT_OPERATION_KIND,
      operationId: fixture.launchIntent.launchAttemptId,
      request,
    },
    request,
    supervisor: fixture.launchIntent.supervisor,
  };
}

function restoreAttachmentActivationLaunchActiveSessionRow(
  fixture,
  {
    launch = restoreAttachmentActivationLaunchFixture(fixture),
    state = "prepared",
    updatedAt = RESTORE_ACTIVATION_FINALIZE_NOW,
  } = {},
) {
  const expected = launch.options.expectedSession;
  const operationRevision =
    state === "prepared" ? "0" : state === "starting" ? "1" : "2";
  return sessionRow({
    sessionId: expected.sessionId,
    revision: (
      BigInt(expected.revision) +
      BigInt(operationRevision) +
      1n
    ).toString(),
    sessionDocument: document(expected.sessionId, {
      ...structuredClone(expected.document),
      activeOperation: activeOperation(state, {
        operationRevision,
        options: launch.options,
      }),
    }),
    createdAt: expected.createdAt,
    updatedAt,
  });
}

function restoreAttachmentActivationLaunchProducerRelationSteps(
  fixture,
  launchIdClaim,
) {
  return [
    rows(restoreAttachmentActivationCommittedOperationRow(fixture)),
    rows(restoreAttachmentActivationReleasedReservationRow(fixture)),
    ...restoreAttachmentActivationRelationSteps(fixture, "committed", {
      launchIdClaim,
    }),
  ];
}

function restoreAttachmentActivationLaunchRecoverySteps(
  fixture,
  {
    launch = restoreAttachmentActivationLaunchFixture(fixture),
    state = "prepared",
    launchCreatedAt = RESTORE_ACTIVATION_FINALIZE_NOW,
    launchUpdatedAt =
      state === "prepared"
        ? RESTORE_ACTIVATION_FINALIZE_NOW
        : state === "starting"
          ? RESTORE_ACTIVATION_LAUNCH_DISPATCH_NOW
          : RESTORE_ACTIVATION_LAUNCH_UNCERTAIN_NOW,
    launchIdClaim = restoreAttachmentActivationLaunchIdClaimRow(fixture, {
      materializedAt: RESTORE_ACTIVATION_FINALIZE_NOW,
    }),
  } = {},
) {
  const launchOperation = writerLaunchOperationRow(launch, state, {
    createdAt: launchCreatedAt,
    updatedAt: launchUpdatedAt,
  });
  const launchReservation = writerLaunchReservationRow(
    launch,
    state,
    {
      createdAt: launchCreatedAt,
      updatedAt: launchUpdatedAt,
    },
  );
  const steps = [
    rows(launchOperation),
    rows(
      restoreAttachmentActivationLaunchActiveSessionRow(fixture, {
        launch,
        state,
        updatedAt: launchUpdatedAt,
      }),
    ),
    rows(launchOperation),
    rows(launchReservation),
  ];
  if (state !== "prepared") {
    steps.push(...writerLaunchGenerationReferenceSteps(fixture.launch));
    steps.push(
      ...restoreAttachmentActivationLaunchProducerRelationSteps(
        fixture,
        launchIdClaim,
      ),
    );
  }
  steps.push(
    ...restoreAttachmentActivationLaunchProducerRelationSteps(
      fixture,
      launchIdClaim,
    ),
  );
  return steps;
}

function writerLaunchCheckpointReplacementFixture(launch) {
  const launchResult = writerLaunchResult(launch);
  const expectedSession = snapshotFromSessionRow(
    writerLaunchCommittedSessionRow(launch, { result: launchResult }),
  );
  const attachment = expectedSession.document.attachment;
  const lease = expectedSession.document.lease;
  const checkpoint = {
    artifactId: ARTIFACT_ID,
    backendId: expectedSession.document.storageRef.backendId,
    checkpointClass: "clean",
    checkpointId: CHECKPOINT_ID,
    codexSessionId: expectedSession.document.manifest.codex.sessionId,
    codexThreadId:
      expectedSession.document.manifest.codex.rootThreadId,
    contractVersion: 1,
    createdAt: LAUNCH_CHECKPOINT_PREPARED_NOW,
    imageDigest: expectedSession.document.manifest.runtime.imageDigest,
    sessionId: expectedSession.sessionId,
    sourceFencingEpoch: lease.fencingEpoch,
    storageId: expectedSession.document.storageRef.storageId,
  };
  const mutationRequest = {
    backendId: checkpoint.backendId,
    contractVersion: 1,
    fencingEpoch: lease.fencingEpoch,
    holderId: lease.holderId,
    leaseId: lease.leaseId,
    operation: "checkpoint",
    operationId: CAPTURE_OPERATION_ID,
    sessionId: expectedSession.sessionId,
    storageId: checkpoint.storageId,
    target: {
      artifactId: checkpoint.artifactId,
      checkpointId: checkpoint.checkpointId,
      kind: "checkpoint",
    },
  };
  const admission = {
    attachment: structuredClone(attachment),
    captureAttemptId: CAPTURE_ATTEMPT_ID,
    checkpoint,
    processIncarnationId:
      expectedSession.document.launch.processIncarnationId,
    request: mutationRequest,
    stopOperationId: STOP_OPERATION_ID,
    writerIncarnationId:
      expectedSession.document.launch.writerIncarnationId,
  };
  const request = createCheckpointCaptureOperationRequest({
    admission,
    expectedSession,
  });
  const options = {
    expectedSession,
    kind: CHECKPOINT_CAPTURE_OPERATION_KIND,
    operationId: CAPTURE_OPERATION_ID,
    request,
  };
  const completion = {
    artifactProof: {
      artifactManifestDigest: "b".repeat(64),
      captureOperationId: options.operationId,
      modeledDigest: "c".repeat(64),
    },
    materialization: {
      artifactManifestDigest: "b".repeat(64),
      contractVersion: 2,
      modeledDigest: "c".repeat(64),
      publicationId: "launch-checkpoint-publication-001",
      publicationKind: "checkpoint-artifact",
      stagedRoot: {
        filesystemId: "launch-checkpoint-filesystem-001",
        objectIdentityScheme: "test-object-id-v1",
        objectId: "launch-checkpoint-object-001",
      },
      treeIdentityDigest: "d".repeat(64),
    },
    replayed: false,
    result: request.predeterminedResult,
  };
  const fixture = {
    admission,
    checkpoint,
    completion,
    mutationRequest,
    options,
    request,
  };
  const result = checkpointCaptureTerminalResult(fixture);
  const operation = operationRow("committed", {
    options,
    revision: "3",
    createdAt: LAUNCH_CHECKPOINT_PREPARED_NOW,
    updatedAt: LAUNCH_CHECKPOINT_FINALIZE_NOW,
    result,
    retiredAt: LAUNCH_CHECKPOINT_FINALIZE_NOW,
  });
  const reservation = reservationRow("released", {
    options,
    createdAt: LAUNCH_CHECKPOINT_PREPARED_NOW,
    updatedAt: LAUNCH_CHECKPOINT_FINALIZE_NOW,
    releasedAt: LAUNCH_CHECKPOINT_FINALIZE_NOW,
  });
  const attempt = {
    ...checkpointCaptureAttemptRow(fixture),
    claimed_at: new Date(LAUNCH_CHECKPOINT_DISPATCH_NOW),
  };
  const catalogue = {
    ...checkpointCatalogueRow(fixture),
    committed_at: new Date(LAUNCH_CHECKPOINT_FINALIZE_NOW),
  };
  const session = sessionRow({
    sessionId: expectedSession.sessionId,
    revision: (BigInt(expectedSession.revision) + 4n).toString(),
    sessionDocument: document(expectedSession.sessionId, {
      ...structuredClone(expectedSession.document),
      activeOperation: null,
      lastOperation: terminalPointer({
        options,
        operationRevision: "3",
        result,
      }),
    }),
    createdAt: expectedSession.createdAt,
    updatedAt: LAUNCH_CHECKPOINT_FINALIZE_NOW,
  });
  return {
    ...fixture,
    attempt,
    catalogue,
    launchResult,
    operation,
    reservation,
    result,
    session,
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

function checkpointHistoricalReplacementFixture(
  fixture,
  {
    capabilities = fixture.options.expectedSession.document
      .backendCapabilities,
    createdAt = fixture.options.expectedSession.createdAt,
  } = {},
) {
  const writerExpected = sessionSnapshot({
    sessionId: fixture.options.expectedSession.sessionId,
    sessionDocument: document(
      fixture.options.expectedSession.sessionId,
      {
        backendCapabilities: structuredClone(capabilities),
      },
    ),
    createdAt,
    updatedAt: createdAt,
  });
  const writerOptions = writerAcquireOptions({
    expectedSession: writerExpected,
    operationId: "replacement-session-writer-attachment",
  });
  const lease = writerLease(writerOptions);
  const writerResult = writerAttachmentResult(writerOptions, lease);
  const attachedSession = writerAttachedSessionRow({
    options: writerOptions,
    lease,
    result: writerResult,
  });
  const predecessorOptions = reserveOptions({
    expectedSession: snapshotFromSessionRow(attachedSession),
    operationId: "replacement-session-predecessor",
    request: operationRequest({
      checkpointId: "replacement-session-predecessor-checkpoint",
    }),
  });
  const predecessorResult = cancellationResult(
    "replacement-session-predecessor",
  );
  const replacementExpectedRow = sessionRow({
    sessionId: fixture.options.expectedSession.sessionId,
    revision: (
      BigInt(predecessorOptions.expectedSession.revision) + 2n
    ).toString(),
    sessionDocument: document(
      fixture.options.expectedSession.sessionId,
      {
        ...structuredClone(predecessorOptions.expectedSession.document),
        activeOperation: null,
        lastOperation: terminalPointer({
          options: predecessorOptions,
          operationRevision: "1",
          result: predecessorResult,
        }),
      },
    ),
    createdAt,
    updatedAt: CAPTURE_PREPARED_NOW,
  });
  const replacementExpected = snapshotFromSessionRow(
    replacementExpectedRow,
  );
  const options = reserveOptions({
    expectedSession: replacementExpected,
    operationId: "replacement-session-operation",
    request: operationRequest({
      checkpointId: "replacement-session-checkpoint",
    }),
  });
  const result = cancellationResult("replacement-session-anchor");
  const operation = operationRow("committed", {
    options,
    revision: "1",
    createdAt: CAPTURE_PREPARED_NOW,
    updatedAt: CAPTURE_FINALIZE_NOW,
    result,
    retiredAt: CAPTURE_FINALIZE_NOW,
  });
  const reservation = reservationRow("released", {
    options,
    createdAt: CAPTURE_PREPARED_NOW,
    updatedAt: CAPTURE_FINALIZE_NOW,
    releasedAt: CAPTURE_FINALIZE_NOW,
  });
  const session = sessionRow({
    sessionId: fixture.options.expectedSession.sessionId,
    revision: (BigInt(replacementExpected.revision) + 2n).toString(),
    sessionDocument: document(
      fixture.options.expectedSession.sessionId,
      {
        ...structuredClone(replacementExpected.document),
        activeOperation: null,
        lastOperation: terminalPointer({
          options,
          operationRevision: "1",
          result,
        }),
      },
    ),
    createdAt,
    updatedAt: CAPTURE_FINALIZE_NOW,
  });
  return { operation, reservation, session };
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
    authority: new PostgresSessionAuthority({
      restoreAttachmentActivationV2FleetCompatible: true,
      restoreAttachmentActivationV2GenerationPredecessorFleetCompatible:
        true,
      restoreGenerationV2FleetCompatible: true,
      store,
      writerLaunchStopV3FleetCompatible: true,
    }),
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

function assertAtomicHandoffValidatedBeforeAuthorityClock(client) {
  const texts = queryTexts(client);
  const authorityClockIndex = texts.indexOf(READ_AUTHORITY_CLOCK_QUERY);
  const launchIdClaimIndex = Math.max(
    texts.lastIndexOf(READ_OPERATION_ID_CLAIM_QUERY),
    texts.lastIndexOf(READ_OPERATION_ID_CLAIM_FOR_UPDATE_QUERY),
  );
  const requiredLockIndexes = [
    `${READ_SESSION_QUERY} FOR UPDATE`,
    `${READ_OPERATION_QUERY} FOR UPDATE`,
    `${READ_RESERVATION_QUERY} FOR UPDATE`,
  ].map((text) => texts.indexOf(text));
  const lastForUpdateIndex = texts.reduce(
    (lastIndex, text, index) =>
      text.includes("FOR UPDATE") ? index : lastIndex,
    -1,
  );

  assert.ok(authorityClockIndex >= 0);
  assert.ok(launchIdClaimIndex >= 0);
  assert.equal(requiredLockIndexes.every((index) => index >= 0), true);
  assert.ok(authorityClockIndex > launchIdClaimIndex);
  assert.ok(authorityClockIndex > lastForUpdateIndex);
  return { authorityClockIndex, texts };
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
    READ_OPERATION_ID_CLAIM_FOR_UPDATE_QUERY,
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

test("a durable restore launch preclaim blocks generic reuse of its global operation ID", async () => {
  const restore = restoreGenerationFixture({
    launchAttemptId: LAUNCH_ATTEMPT_OPERATION_ID,
  });
  const options = reserveOptions({
    operationId: LAUNCH_ATTEMPT_OPERATION_ID,
  });
  const { authority, clients } = authorityWithScripts([
    rows(sessionRow()),
    rows({ operation_count: 0, reservation_count: 0 }),
    rows(),
    rows(),
    rows(),
    rows(restoreLaunchIdClaimRow(restore)),
  ]);

  await assertAuthorityError(authority.reserveOperation(options), {
    code: "operation_identity_conflict",
  });

  assert.deepEqual(authorityQueries(clients[0]).map(queryText), [
    `${READ_SESSION_QUERY} FOR UPDATE`,
    READ_ACTIVE_COUNTS_QUERY,
    READ_OPERATION_QUERY,
    INSERT_OPERATION_QUERY,
    `${READ_OPERATION_QUERY} FOR UPDATE`,
    READ_OPERATION_ID_CLAIM_FOR_UPDATE_QUERY,
  ]);
  assert.equal(
    queryTexts(clients[0]).includes(INSERT_RESERVATION_QUERY),
    false,
  );
  assert.equal(queryTexts(clients[0]).includes(UPDATE_SESSION_QUERY), false);
  clients[0].assertExhausted();
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
  let accessorCalls = 0;

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
  assert.throws(
    () =>
      new PostgresSessionAuthority({
        restoreAttachmentActivationV2GenerationPredecessorFleetCompatible:
          "true",
        store,
      }),
    (error) =>
      error instanceof PostgresSessionAuthorityError &&
      error.code === "invalid_authority_options",
  );
  const accessorOptions = { store };
  Object.defineProperty(
    accessorOptions,
    "restoreAttachmentActivationV2GenerationPredecessorFleetCompatible",
    {
      enumerable: true,
      get() {
        accessorCalls += 1;
        throw new Error("authority option accessor must not run");
      },
    },
  );
  assert.throws(
    () => new PostgresSessionAuthority(accessorOptions),
    (error) =>
      error instanceof PostgresSessionAuthorityError &&
      error.code === "invalid_authority_options",
  );
  assert.equal(accessorCalls, 0);
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
      READ_OPERATION_ID_CLAIM_QUERY,
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
  const restore = restoreGenerationFixture();
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
      authority.claimOperationDispatch({
        ...restore.options,
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

test("writer launch request builder freezes the exact generation, image, lease, attachment, and supervisor binding", () => {
  const fixture = writerLaunchFixture();
  const replay = createWriterLaunchAttemptOperationRequest({
    expectedSession: fixture.options.expectedSession,
    generation: fixture.generation,
    measuredImage: fixture.measuredImage,
    supervisor: fixture.supervisor,
  });

  assert.equal(
    WRITER_LAUNCH_ATTEMPT_OPERATION_KIND,
    "writer-launch-attempt-v1",
  );
  assert.deepEqual(replay, fixture.request);
  assert.deepEqual(Reflect.ownKeys(replay), [
    "attachment",
    "contractVersion",
    "fencingEpoch",
    "generation",
    "lease",
    "measuredImage",
    "supervisor",
  ]);
  assert.equal(
    replay.generation.bindingSha256,
    canonicalSha256(restoreGenerationBinding(fixture.restore)),
  );
  assert.equal(
    replay.generation.documentSha256,
    canonicalSha256(restoreGenerationDocument(fixture.restore)),
  );
  assert.deepEqual(
    replay.attachment,
    canonicalPayload(fixture.options.expectedSession.document.attachment),
  );
  assert.deepEqual(
    replay.lease,
    canonicalPayload(fixture.options.expectedSession.document.lease),
  );
  assertDeepFrozen(replay);
});

test("writer launch reservation upgrades an exact v2 expected session to v3 without rewriting the frozen request", async () => {
  const fixture = writerLaunchFixture({ sessionDocumentVersion: 2 });
  const preparedOperation = writerLaunchOperationRow(fixture, "prepared");
  const preparedReservation = writerLaunchReservationRow(
    fixture,
    "prepared",
  );
  const preparedSession = writerLaunchPhaseSessionRow(fixture, "prepared");
  const { authority, clients } = authorityWithScripts({
    options: { now: LAUNCH_PREPARED_NOW },
    steps: [
      ...writerLaunchBaseSteps(fixture),
      rows(),
      rows(preparedOperation),
      rows(preparedReservation),
      rows(preparedSession),
    ],
  });

  const reserved = await authority.reserveOperation(fixture.options);

  assert.equal(fixture.options.expectedSession.document.documentVersion, 2);
  assert.equal(reserved.acquired, true);
  assert.equal(
    reserved.operation.expectedSession.document.documentVersion,
    2,
  );
  assert.equal(
    reserved.session.document.documentVersion,
    SESSION_AUTHORITY_DOCUMENT_VERSION,
  );
  assert.equal(reserved.session.document.activeOperation.state, "prepared");
  clients[0].assertExhausted();
});

test("writer launch request builder rejects hostile or mismatched bindings without PostgreSQL", async () => {
  const fixture = writerLaunchFixture();
  const startedExpectedSession = snapshotFromSessionRow(
    writerLaunchCommittedSessionRow(fixture),
  );
  const { authority, pool } = authorityWithScripts();
  const invalidBuilders = [
    () =>
      createWriterLaunchAttemptOperationRequest({
        expectedSession: fixture.options.expectedSession,
        generation: fixture.generation,
        measuredImage: fixture.measuredImage,
        supervisor: fixture.supervisor,
        extra: true,
      }),
    () =>
      createWriterLaunchAttemptOperationRequest({
        expectedSession: fixture.options.expectedSession,
        generation: { ...fixture.generation, state: "authorized" },
        measuredImage: fixture.measuredImage,
        supervisor: fixture.supervisor,
      }),
    () =>
      createWriterLaunchAttemptOperationRequest({
        expectedSession: fixture.options.expectedSession,
        generation: {
          ...fixture.generation,
          sessionId: OTHER_SESSION_ID,
        },
        measuredImage: fixture.measuredImage,
        supervisor: fixture.supervisor,
      }),
    () =>
      createWriterLaunchAttemptOperationRequest({
        expectedSession: fixture.options.expectedSession,
        generation: fixture.generation,
        measuredImage: {
          ...fixture.measuredImage,
          runtimeIdentity: {
            ...fixture.measuredImage.runtimeIdentity,
            platformImageDigest: `sha256:${"d".repeat(64)}`,
          },
        },
        supervisor: fixture.supervisor,
      }),
    () =>
      createWriterLaunchAttemptOperationRequest({
        expectedSession: fixture.options.expectedSession,
        generation: fixture.generation,
        measuredImage: fixture.measuredImage,
        supervisor: { ...fixture.supervisor, extra: true },
      }),
    () =>
      createWriterLaunchAttemptOperationRequest({
        expectedSession: startedExpectedSession,
        generation: fixture.generation,
        measuredImage: writerLaunchMeasuredImage(startedExpectedSession),
        supervisor: fixture.supervisor,
      }),
  ];

  for (const invoke of invalidBuilders) {
    assert.throws(
      invoke,
      (error) =>
        error instanceof PostgresSessionAuthorityError &&
        error.code === "invalid_operation_request",
    );
  }
  await assertAuthorityError(
    authority.claimWriterLaunchAttemptDispatch({
      ...fixture.options,
      expectedOperationRevision: "0",
      extra: true,
    }),
    { code: "invalid_operation_request" },
  );
  await assertAuthorityError(
    authority.finalizeWriterLaunchAttemptStarted({
      ...fixture.options,
      evidence: {
        ...writerLaunchEvidence(fixture),
        extra: true,
      },
      expectedOperationRevision: "1",
    }),
    { code: "invalid_operation_request" },
  );
  await assertAuthorityError(
    authority.finalizeWriterLaunchAttemptStopped({
      ...fixture.options,
      evidence: writerLaunchEvidence(fixture, "started"),
      expectedOperationRevision: "1",
    }),
    { code: "invalid_operation_request" },
  );
  await assertAuthorityError(
    authority.listWriterLaunchAttemptRecoveryCandidates({
      afterSessionId: null,
      limit: 1,
      extra: true,
    }),
    { code: "invalid_operation_request" },
  );
  await assertAuthorityError(
    authority.reserveOperation({
      expectedSession: startedExpectedSession,
      kind: WRITER_RELEASE_OPERATION_KIND,
      operationId: OTHER_OPERATION_ID,
      request: {
        contractVersion: 1,
        target: {
          attachmentId:
            startedExpectedSession.document.attachment.attachmentId,
          kind: "attachment",
        },
      },
    }),
    { code: "invalid_operation_request" },
  );
  assert.equal(pool.connectCalls, 0);
});

test("checkpoint capture admission requires the current launch process and writer incarnations", () => {
  const launch = writerLaunchFixture();
  const expectedSession = snapshotFromSessionRow(
    writerLaunchCommittedSessionRow(launch),
  );
  const attachment = expectedSession.document.attachment;
  const lease = expectedSession.document.lease;
  const checkpoint = {
    artifactId: ARTIFACT_ID,
    backendId: expectedSession.document.storageRef.backendId,
    checkpointClass: "clean",
    checkpointId: CHECKPOINT_ID,
    codexSessionId: expectedSession.document.manifest.codex.sessionId,
    codexThreadId:
      expectedSession.document.manifest.codex.rootThreadId,
    contractVersion: 1,
    createdAt: CAPTURE_PREPARED_NOW,
    imageDigest: expectedSession.document.manifest.runtime.imageDigest,
    sessionId: expectedSession.sessionId,
    sourceFencingEpoch: lease.fencingEpoch,
    storageId: expectedSession.document.storageRef.storageId,
  };
  const request = {
    backendId: expectedSession.document.storageRef.backendId,
    contractVersion: 1,
    fencingEpoch: lease.fencingEpoch,
    holderId: lease.holderId,
    leaseId: lease.leaseId,
    operation: "checkpoint",
    operationId: CAPTURE_OPERATION_ID,
    sessionId: expectedSession.sessionId,
    storageId: expectedSession.document.storageRef.storageId,
    target: {
      artifactId: ARTIFACT_ID,
      checkpointId: CHECKPOINT_ID,
      kind: "checkpoint",
    },
  };
  const admission = {
    attachment,
    captureAttemptId: CAPTURE_ATTEMPT_ID,
    checkpoint,
    processIncarnationId:
      expectedSession.document.launch.processIncarnationId,
    request,
    stopOperationId: STOP_OPERATION_ID,
    writerIncarnationId:
      expectedSession.document.launch.writerIncarnationId,
  };

  const accepted = createCheckpointCaptureOperationRequest({
    admission,
    expectedSession,
  });
  assert.equal(
    accepted.admission.processIncarnationId,
    PROCESS_INCARNATION_ID,
  );
  assert.equal(
    accepted.admission.writerIncarnationId,
    WRITER_INCARNATION_ID,
  );

  for (const mismatch of [
    { processIncarnationId: "foreign-process-incarnation" },
    { writerIncarnationId: "foreign-writer-incarnation" },
  ]) {
    assert.throws(
      () =>
        createCheckpointCaptureOperationRequest({
          admission: { ...admission, ...mismatch },
          expectedSession,
        }),
      (error) =>
        error instanceof PostgresSessionAuthorityError &&
        error.code === "invalid_operation_request",
    );
  }
});

test("writer launch compact generation reference matches exact committed restore readback", async () => {
  const fixture = writerLaunchFixture();
  const { authority, clients } = authorityWithScripts([
    rows(restoreGenerationRow(fixture.restore, "committed")),
    rows(
      restoreGenerationOperationRow(fixture.restore, "committed", {
        revision: "2",
      }),
    ),
    ...restoreGenerationCommittedSteps(fixture.restore, {
      operationRevision: "2",
    }),
  ]);

  const read = await authority.readRestoreDestinationGeneration({
    checkpoint: fixture.restore.source.checkpoint,
    generationId: fixture.restore.generationId,
    request: fixture.restore.mutationRequest,
  });
  const generation = read.generation;
  const reference = {
    bindingSha256: canonicalSha256(generation.binding),
    checkpointId: generation.checkpointId,
    claimedAt: generation.claimedAt,
    committedAt: generation.committedAt,
    documentSha256: canonicalSha256(generation.document),
    generationId: generation.generationId,
    operationId: generation.operationId,
    sessionId: generation.sessionId,
    state: generation.state,
  };

  assert.deepEqual(
    canonicalPayload(reference),
    fixture.request.generation,
  );
  assert.equal(
    JSON.stringify(reference),
    JSON.stringify(fixture.request.generation),
  );
  clients[0].assertExhausted();
});

test("writer launch dispatch grants once from the exact committed generation and replays without a second start", async () => {
  const fixture = writerLaunchFixture();
  const startingOperation = writerLaunchOperationRow(fixture, "starting");
  const startingReservation = writerLaunchReservationRow(
    fixture,
    "starting",
  );
  const startingSession = writerLaunchPhaseSessionRow(fixture, "starting");
  const { authority, clients } = authorityWithScripts(
    {
      options: {
        authorityNow: LAUNCH_DISPATCH_NOW,
        now: LAUNCH_DISPATCH_NOW,
      },
      steps: [
        ...writerLaunchActiveSteps(fixture, "prepared"),
        ...writerLaunchGenerationReferenceSteps(fixture),
        rows(startingOperation),
        rows(startingReservation),
        rows(startingSession),
      ],
    },
    writerLaunchActiveSteps(fixture, "starting"),
  );

  const claimed = await authority.claimWriterLaunchAttemptDispatch({
    ...fixture.options,
    expectedOperationRevision: "0",
  });
  const replayed = await authority.claimWriterLaunchAttemptDispatch({
    ...fixture.options,
    expectedOperationRevision: "0",
  });

  assert.equal(claimed.dispatchGranted, true);
  assert.equal(claimed.authorityNow, LAUNCH_DISPATCH_NOW);
  assert.equal(claimed.attempt.state, "starting");
  assert.equal(claimed.generation.state, "committed");
  assert.equal(replayed.dispatchGranted, false);
  assert.equal(replayed.attempt.state, "starting");
  assert.equal(
    authorityQueries(clients[1]).some((args) =>
      /^(?:INSERT|UPDATE) /u.test(queryText(args)),
    ),
    false,
  );
  for (const client of clients) client.assertExhausted();
});

test("writer launch dispatch rejects an expired lease and cross-session generation identity before phase mutation", async (t) => {
  await t.test("expired lease", async () => {
    const fixture = writerLaunchFixture();
    const { authority, clients } = authorityWithScripts({
      options: {
        authorityNow: EXPIRED_FINALIZE_NOW,
        now: LAUNCH_DISPATCH_NOW,
      },
      steps: [
        ...writerLaunchActiveSteps(fixture, "prepared"),
        ...writerLaunchGenerationReferenceSteps(fixture),
      ],
    });

    await assertAuthorityError(
      authority.claimWriterLaunchAttemptDispatch({
        ...fixture.options,
        expectedOperationRevision: "0",
      }),
      { code: "writer_lease_expired" },
    );
    assert.equal(
      authorityQueries(clients[0]).some((args) =>
        /^(?:INSERT|UPDATE) /u.test(queryText(args)),
      ),
      false,
    );
    const texts = queryTexts(clients[0]);
    const authorityClockIndex = texts.indexOf(READ_AUTHORITY_CLOCK_QUERY);
    const lastForUpdateIndex = texts.reduce(
      (lastIndex, text, index) =>
        text.includes("FOR UPDATE") ? index : lastIndex,
      -1,
    );
    assert.ok(lastForUpdateIndex >= 0);
    assert.ok(authorityClockIndex > lastForUpdateIndex);
    clients[0].assertExhausted();
  });

  await t.test("cross-session generation", async () => {
    const fixture = writerLaunchFixture();
    const steps = [
      ...writerLaunchActiveSteps(fixture, "prepared"),
      rows(
        restoreGenerationRow(fixture.restore, "committed", {
          session_id: OTHER_SESSION_ID,
        }),
      ),
    ];
    const { authority, clients } = authorityWithScripts(steps);

    await assertAuthorityError(
      authority.claimWriterLaunchAttemptDispatch({
        ...fixture.options,
        expectedOperationRevision: "0",
      }),
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
});

test("writer launch claim rejects forged committed generation history and claimed-at bounds before mutation", async (t) => {
  const operationHistoryCases = [
    {
      name: "session document identity",
      mutate(operation) {
        operation.request.expectedSession.document.backendCapabilities = {
          ...operation.request.expectedSession.document.backendCapabilities,
          atomicPointInTimeCheckpoint: false,
        };
      },
    },
    {
      name: "session incarnation creation time",
      mutate(operation) {
        operation.request.expectedSession.createdAt = LATER;
      },
    },
    {
      name: "session revision floor",
      mutate(operation, fixture) {
        operation.request.expectedSession.revision =
          fixture.options.expectedSession.revision;
      },
    },
  ];

  for (const scenario of operationHistoryCases) {
    await t.test(scenario.name, async () => {
      const fixture = writerLaunchFixture();
      const operation = restoreGenerationOperationRow(
        fixture.restore,
        "committed",
        { revision: "2" },
      );
      scenario.mutate(operation, fixture);
      const { authority, clients } = authorityWithScripts([
        ...writerLaunchActiveSteps(fixture, "prepared"),
        rows(restoreGenerationRow(fixture.restore, "committed")),
        rows(operation),
      ]);

      await assertAuthorityError(
        authority.claimWriterLaunchAttemptDispatch({
          ...fixture.options,
          expectedOperationRevision: "0",
        }),
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
  }

  for (const scenario of [
    {
      claimedAt: "2026-07-29T12:35:09.000Z",
      name: "claimed before operation creation",
      readsReservation: true,
    },
    {
      claimedAt: RESTORE_CANCEL_NOW,
      name: "claimed after operation update",
      readsReservation: false,
    },
  ]) {
    await t.test(scenario.name, async () => {
      const fixture = writerLaunchFixture();
      const generation = restoreGenerationRow(
        fixture.restore,
        "committed",
        { claimed_at: new Date(scenario.claimedAt) },
      );
      const { authority, clients } = authorityWithScripts([
        ...writerLaunchActiveSteps(fixture, "prepared"),
        rows(generation),
        rows(
          restoreGenerationOperationRow(fixture.restore, "committed", {
            revision: "2",
          }),
        ),
        ...restoreCheckpointSourceSteps(fixture.restore),
        ...(scenario.readsReservation
          ? [
              rows(
                restoreGenerationReservationRow(
                  fixture.restore,
                  "released",
                ),
              ),
            ]
          : []),
      ]);

      await assertAuthorityError(
        authority.claimWriterLaunchAttemptDispatch({
          ...fixture.options,
          expectedOperationRevision: "0",
        }),
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
  }
});

test("V1 writer launch prepared intent remains readable and cancellable with a stale generation while claim fails before mutation", async () => {
  const base = writerLaunchFixture();
  const fabricatedGeneration = {
    ...base.generation,
    generationId: "fabricated-restore-generation",
    operationId: "fabricated-restore-generation-operation",
  };
  const request = createWriterLaunchAttemptOperationRequest({
    expectedSession: base.options.expectedSession,
    generation: fabricatedGeneration,
    measuredImage: base.measuredImage,
    supervisor: base.supervisor,
  });
  const fixture = {
    ...base,
    options: {
      ...base.options,
      operationId: "writer-launch-attempt-stale-generation",
      request,
    },
    request,
  };
  const preparedOperation = writerLaunchOperationRow(fixture, "prepared");
  const preparedReservation = writerLaunchReservationRow(
    fixture,
    "prepared",
  );
  const preparedSession = writerLaunchPhaseSessionRow(fixture, "prepared");
  const reason = "caller-abandoned-before-launch-dispatch";
  const cancellation = cancellationResult(reason);
  const cancelledOperation = writerLaunchOperationRow(
    fixture,
    "committed",
    { result: cancellation, revision: "1" },
  );
  const cancelledReservation = writerLaunchReservationRow(
    fixture,
    "released",
  );
  const cancelledSession = sessionRow({
    sessionId: fixture.options.expectedSession.sessionId,
    revision: (
      BigInt(fixture.options.expectedSession.revision) + 2n
    ).toString(),
    sessionDocument: document(fixture.options.expectedSession.sessionId, {
      ...structuredClone(fixture.options.expectedSession.document),
      documentVersion: SESSION_AUTHORITY_DOCUMENT_VERSION,
      activeOperation: null,
      lastOperation: terminalPointer({
        options: fixture.options,
        operationRevision: "1",
        result: cancellation,
      }),
      launch: null,
    }),
    createdAt: fixture.options.expectedSession.createdAt,
    updatedAt: LAUNCH_FINALIZE_NOW,
  });
  const { authority, clients } = authorityWithScripts(
    {
      options: { now: LAUNCH_PREPARED_NOW },
      steps: [
        ...writerLaunchBaseSteps(fixture),
        rows(),
        rows(preparedOperation),
        rows(preparedReservation),
        rows(preparedSession),
      ],
    },
    [rows(preparedOperation), ...writerLaunchActiveSteps(fixture, "prepared")],
    {
      options: {
        authorityNow: LAUNCH_DISPATCH_NOW,
        now: LAUNCH_DISPATCH_NOW,
      },
      steps: [...writerLaunchActiveSteps(fixture, "prepared"), rows()],
    },
    {
      options: { now: LAUNCH_FINALIZE_NOW },
      steps: [
        ...writerLaunchActiveSteps(fixture, "prepared"),
        rows(cancelledOperation),
        rows(cancelledReservation),
        rows(cancelledSession),
      ],
    },
  );

  const reserved = await authority.reserveOperation(fixture.options);
  const read = await authority.readWriterLaunchAttempt({
    operationId: fixture.options.operationId,
  });
  await assertAuthorityError(
    authority.claimWriterLaunchAttemptDispatch({
      ...fixture.options,
      expectedOperationRevision: "0",
    }),
    { code: "operation_state_invalid" },
  );
  const cancelled = await authority.cancelPreparedOperation({
    ...fixture.options,
    expectedOperationRevision: "0",
    reason,
  });

  assert.equal(reserved.acquired, true);
  assert.equal(fixture.restore.request.contractVersion, 1);
  assert.equal(read.attempt.state, "prepared");
  assert.equal(read.launch, null);
  assert.equal(cancelled.cancelled, true);
  assert.equal(cancelled.operation.result.outcome, "cancelled-before-dispatch");
  assert.equal(
    authorityQueries(clients[1]).some((args) =>
      queryText(args).startsWith(READ_RESTORE_GENERATION_BY_ID_QUERY),
    ),
    false,
  );
  assert.equal(
    authorityQueries(clients[2]).at(-1)[0].text,
    `${READ_RESTORE_GENERATION_BY_ID_QUERY} FOR UPDATE`,
  );
  assert.equal(
    authorityQueries(clients[2]).some((args) =>
      /^(?:INSERT|UPDATE) /u.test(queryText(args)),
    ),
    false,
  );
  assert.equal(
    authorityQueries(clients[3]).some((args) =>
      queryText(args).startsWith(READ_RESTORE_GENERATION_BY_ID_QUERY),
    ),
    false,
  );
  for (const client of clients) client.assertExhausted();
});

test("writer launch starting finalization records one exact current pointer and exact replay", async () => {
  const fixture = writerLaunchFixture();
  const evidence = writerLaunchEvidence(fixture);
  const result = writerLaunchResult(fixture);
  const committedOperation = writerLaunchOperationRow(
    fixture,
    "committed",
    { result, revision: "2" },
  );
  const committedReservation = writerLaunchReservationRow(
    fixture,
    "released",
  );
  const committedSession = writerLaunchCommittedSessionRow(fixture, {
    result,
  });
  const { authority, clients } = authorityWithScripts(
    {
      options: {
        authorityNow: LAUNCH_FINALIZE_NOW,
        now: LAUNCH_FINALIZE_NOW,
      },
      steps: [
        ...writerLaunchActiveSteps(fixture, "starting"),
        rows(committedOperation),
        rows(committedReservation),
        rows(committedSession),
      ],
    },
    writerLaunchCommittedSteps(fixture, { result }),
    writerLaunchCommittedSteps(fixture, { result }),
    writerLaunchCommittedSteps(fixture, { result }),
  );

  const finalized = await authority.finalizeWriterLaunchAttemptStarted({
    ...fixture.options,
    evidence,
    expectedOperationRevision: "1",
  });
  const replayed = await authority.finalizeWriterLaunchAttemptStarted({
    ...fixture.options,
    evidence,
    expectedOperationRevision: "1",
  });
  await assertAuthorityError(
    authority.finalizeWriterLaunchAttemptStarted({
      ...fixture.options,
      evidence: { ...evidence, proofId: "different-supervisor-proof" },
      expectedOperationRevision: "1",
    }),
    { code: "operation_result_conflict" },
  );
  await assertAuthorityError(
    authority.finalizeWriterLaunchAttemptStarted({
      ...fixture.options,
      evidence,
      expectedOperationRevision: "2",
    }),
    { code: "operation_transition_conflict" },
  );

  assert.equal(finalized.finalized, true);
  assert.deepEqual(finalized.launch, writerLaunchPointer(fixture, result));
  assert.equal(finalized.attempt.state, "committed");
  assert.equal(finalized.attempt.result.evidence.status, "started");
  assert.equal(
    finalized.session.document.launch.launchAttemptId,
    fixture.options.operationId,
  );
  assert.equal(replayed.finalized, false);
  assert.deepEqual(replayed.launch, finalized.launch);
  assertDeepFrozen(finalized);
  for (const client of clients) client.assertExhausted();
});

test("writer launch exact not-started and complete-stopped evidence terminalizes without a launch pointer", async (t) => {
  for (const scenario of [
    {
      expectedOperationRevision: "1",
      operationRevision: "2",
      phase: "starting",
      status: "not-started",
    },
    {
      expectedOperationRevision: "2",
      operationRevision: "3",
      phase: "uncertain",
      status: "complete-stopped",
    },
  ]) {
    await t.test(scenario.status, async () => {
      const fixture = writerLaunchFixture();
      const evidence = writerLaunchEvidence(fixture, scenario.status);
      const result = writerLaunchResult(fixture, scenario.status);
      const committedOperation = writerLaunchOperationRow(
        fixture,
        "committed",
        { result, revision: scenario.operationRevision },
      );
      const committedReservation = writerLaunchReservationRow(
        fixture,
        "released",
      );
      const committedSession = writerLaunchCommittedSessionRow(fixture, {
        operationRevision: scenario.operationRevision,
        result,
      });
      const { authority, clients } = authorityWithScripts(
        {
          options: { now: LAUNCH_FINALIZE_NOW },
          steps: [
            ...writerLaunchActiveSteps(fixture, scenario.phase),
            rows(committedOperation),
            rows(committedReservation),
            rows(committedSession),
          ],
        },
        writerLaunchCommittedSteps(fixture, {
          operationRevision: scenario.operationRevision,
          result,
        }),
      );

      const finalized =
        await authority.finalizeWriterLaunchAttemptStopped({
          ...fixture.options,
          evidence,
          expectedOperationRevision: scenario.expectedOperationRevision,
        });
      const replayed =
        await authority.finalizeWriterLaunchAttemptStopped({
          ...fixture.options,
          evidence,
          expectedOperationRevision: scenario.expectedOperationRevision,
        });

      assert.equal(finalized.finalized, true);
      assert.equal(finalized.launch, null);
      assert.equal(finalized.session.document.launch, null);
      assert.equal(finalized.attempt.state, "committed");
      assert.equal(finalized.attempt.result.evidence.status, scenario.status);
      assert.equal(replayed.finalized, false);
      assert.equal(replayed.launch, null);
      for (const client of clients) client.assertExhausted();
    });
  }
});

test("writer launch exact started and complete-stopped finalization remains authoritative after lease expiry", async (t) => {
  for (const scenario of [
    {
      expectedOperationRevision: "1",
      operationRevision: "2",
      phase: "starting",
      status: "started",
    },
    {
      expectedOperationRevision: "2",
      operationRevision: "3",
      phase: "uncertain",
      status: "complete-stopped",
    },
  ]) {
    await t.test(scenario.status, async () => {
      const fixture = writerLaunchFixture();
      const evidence = writerLaunchEvidence(fixture, scenario.status);
      const result = writerLaunchResult(fixture, scenario.status);
      const committedOperation = writerLaunchOperationRow(
        fixture,
        "committed",
        {
          result,
          revision: scenario.operationRevision,
          updatedAt: EXPIRED_FINALIZE_NOW,
        },
      );
      const committedReservation = writerLaunchReservationRow(
        fixture,
        "released",
        { updatedAt: EXPIRED_FINALIZE_NOW },
      );
      const committedSession = writerLaunchCommittedSessionRow(fixture, {
        operationRevision: scenario.operationRevision,
        result,
        updatedAt: EXPIRED_FINALIZE_NOW,
      });
      const { authority, clients } = authorityWithScripts(
        {
          options: {
            authorityNow: EXPIRED_FINALIZE_NOW,
            now: EXPIRED_FINALIZE_NOW,
          },
          steps: [
            ...writerLaunchActiveSteps(fixture, scenario.phase),
            rows(committedOperation),
            rows(committedReservation),
            rows(committedSession),
          ],
        },
        writerLaunchCommittedSteps(fixture, {
          operationRevision: scenario.operationRevision,
          result,
          updatedAt: EXPIRED_FINALIZE_NOW,
        }),
      );

      const finalize = () =>
        scenario.status === "started"
          ? authority.finalizeWriterLaunchAttemptStarted({
              ...fixture.options,
              evidence,
              expectedOperationRevision:
                scenario.expectedOperationRevision,
            })
          : authority.finalizeWriterLaunchAttemptStopped({
              ...fixture.options,
              evidence,
              expectedOperationRevision:
                scenario.expectedOperationRevision,
            });
      const finalized = await finalize();
      const replayed = await finalize();

      assert.ok(
        Date.parse(EXPIRED_FINALIZE_NOW) >
          Date.parse(fixture.options.expectedSession.document.lease.expiresAt),
      );
      assert.equal(finalized.finalized, true);
      assert.equal(finalized.operation.updatedAt, EXPIRED_FINALIZE_NOW);
      assert.equal(replayed.finalized, false);
      assert.deepEqual(replayed.operation.result, finalized.operation.result);
      if (scenario.status === "started") {
        assert.equal(finalized.launch.startedAt, EXPIRED_FINALIZE_NOW);
        assert.deepEqual(replayed.launch, finalized.launch);
      } else {
        assert.equal(finalized.launch, null);
        assert.equal(finalized.session.document.launch, null);
        assert.equal(replayed.launch, null);
      }
      for (const client of clients) {
        assert.equal(
          queryTexts(client).includes(READ_AUTHORITY_CLOCK_QUERY),
          false,
        );
        client.assertExhausted();
      }
    });
  }
});

test("writer launch readback validates the durable operation relation and exposes only the current started pointer", async () => {
  const started = writerLaunchFixture();
  const startedResult = writerLaunchResult(started);
  const stopped = writerLaunchFixture({
    destinationIsolationProofId: "destination-isolation-proof-stopped",
    generationId: "restore-generation-stopped",
    launchOperationId: "writer-launch-attempt-operation-stopped",
    restoreOperationId: "restore-generation-operation-stopped",
  });
  const stoppedResult = writerLaunchResult(stopped, "not-started");
  const { authority, clients } = authorityWithScripts(
    [
      rows(
        writerLaunchOperationRow(started, "committed", {
          result: startedResult,
          revision: "2",
        }),
      ),
      ...writerLaunchCommittedSteps(started, { result: startedResult }),
    ],
    [
      rows(
        writerLaunchOperationRow(stopped, "committed", {
          result: stoppedResult,
          revision: "2",
        }),
      ),
      ...writerLaunchCommittedSteps(stopped, { result: stoppedResult }),
    ],
  );

  const startedRead = await authority.readWriterLaunchAttempt({
    operationId: started.options.operationId,
  });
  const stoppedRead = await authority.readWriterLaunchAttempt({
    operationId: stopped.options.operationId,
  });

  assert.equal(startedRead.attempt.state, "committed");
  assert.equal(startedRead.attempt.result.evidence.status, "started");
  assert.deepEqual(
    startedRead.launch,
    writerLaunchPointer(started, startedResult),
  );
  assert.equal(stoppedRead.attempt.state, "committed");
  assert.equal(stoppedRead.attempt.result.evidence.status, "not-started");
  assert.equal(stoppedRead.launch, null);
  assertDeepFrozen(startedRead);
  assertDeepFrozen(stoppedRead);
  for (const client of clients) client.assertExhausted();
});

test("writer lease renewal preserves the current launch identity while extending only lease expiry", async () => {
  const launch = writerLaunchFixture();
  const launchResult = writerLaunchResult(launch);
  const expectedSession = snapshotFromSessionRow(
    writerLaunchCommittedSessionRow(launch, { result: launchResult }),
  );
  const options = renewOptions(expectedSession);
  const result = renewalResult(options, LAUNCH_RENEW_AUTHORITY_NOW);
  const committedOperation = operationRow("committed", {
    options,
    revision: "0",
    createdAt: LAUNCH_RENEW_TRANSACTION_NOW,
    updatedAt: LAUNCH_RENEW_TRANSACTION_NOW,
    retiredAt: LAUNCH_RENEW_TRANSACTION_NOW,
    result,
  });
  const releasedReservation = reservationRow("released", {
    options,
    createdAt: LAUNCH_RENEW_TRANSACTION_NOW,
    updatedAt: LAUNCH_RENEW_TRANSACTION_NOW,
    releasedAt: LAUNCH_RENEW_TRANSACTION_NOW,
  });
  const renewedSession = renewedSessionRow({
    options,
    result,
    updatedAt: LAUNCH_RENEW_TRANSACTION_NOW,
  });
  const renewedSessionRelationSteps = [
    rows(renewedSession),
    rows({ operation_count: 0, reservation_count: 0 }),
    rows(committedOperation),
    rows(releasedReservation),
    ...writerLaunchCommittedRelationSteps(launch, {
      result: launchResult,
    }),
  ];
  const { authority, clients } = authorityWithScripts(
    {
      options: {
        authorityNow: LAUNCH_RENEW_AUTHORITY_NOW,
        now: LAUNCH_RENEW_TRANSACTION_NOW,
      },
      steps: [
        ...writerLaunchCommittedSteps(launch, { result: launchResult }),
        rows(),
        rows(committedOperation),
        rows(releasedReservation),
        rows(renewedSession),
      ],
    },
    renewedSessionRelationSteps,
    [
      rows(
        writerLaunchOperationRow(launch, "committed", {
          result: launchResult,
          revision: "2",
        }),
      ),
      ...renewedSessionRelationSteps,
      ...writerLaunchCommittedRelationSteps(launch, {
        result: launchResult,
      }),
    ],
  );

  const receipt = await authority.renewWriterLease(options);
  const readSession = await authority.readSession({
    sessionId: expectedSession.sessionId,
  });
  const readAttempt = await authority.readWriterLaunchAttempt({
    operationId: launch.options.operationId,
  });

  assert.equal(receipt.renewed, true);
  assert.equal(receipt.authorityNow, LAUNCH_RENEW_AUTHORITY_NOW);
  assert.equal(
    receipt.session.document.lease.expiresAt,
    result.lease.expiresAt,
  );
  assert.deepEqual(
    receipt.session.document.launch,
    expectedSession.document.launch,
  );
  assert.equal(
    receipt.session.document.launch.launchAttemptId,
    launch.options.operationId,
  );
  assert.equal(
    readSession.document.lastOperation.operationId,
    options.operationId,
  );
  assert.deepEqual(readSession.document.launch, expectedSession.document.launch);
  assert.equal(
    readAttempt.session.document.lastOperation.operationId,
    options.operationId,
  );
  assert.deepEqual(readAttempt.launch, expectedSession.document.launch);
  assert.equal(
    authorityQueries(clients[1]).filter(
      (args) =>
        queryText(args) === READ_OPERATION_QUERY &&
        args[0]?.values?.[0] === launch.options.operationId,
    ).length,
    1,
  );
  assert.equal(
    authorityQueries(clients[2]).filter(
      (args) =>
        queryText(args) === READ_OPERATION_QUERY &&
        args[0]?.values?.[0] === launch.options.operationId,
    ).length,
    3,
  );
  for (const client of clients) client.assertExhausted();
});

test("checkpoint last-operation replacement preserves and independently validates the current launch relation", async () => {
  const launch = writerLaunchFixture();
  const checkpoint = writerLaunchCheckpointReplacementFixture(launch);
  const checkpointRelationSteps = [
    rows(checkpoint.operation),
    rows(checkpoint.reservation),
    rows(checkpoint.attempt),
    rows(),
    rows(checkpoint.catalogue),
  ];
  const sessionRelationSteps = [
    rows(checkpoint.session),
    rows({ operation_count: 0, reservation_count: 0 }),
    ...checkpointRelationSteps,
    ...writerLaunchCommittedRelationSteps(launch, {
      result: checkpoint.launchResult,
    }),
  ];
  const { authority, clients } = authorityWithScripts(
    sessionRelationSteps,
    [
      rows(
        writerLaunchOperationRow(launch, "committed", {
          result: checkpoint.launchResult,
          revision: "2",
        }),
      ),
      ...sessionRelationSteps,
      ...writerLaunchCommittedRelationSteps(launch, {
        result: checkpoint.launchResult,
      }),
    ],
  );

  const readSession = await authority.readSession({
    sessionId: checkpoint.options.expectedSession.sessionId,
  });
  const readAttempt = await authority.readWriterLaunchAttempt({
    operationId: launch.options.operationId,
  });

  assert.equal(
    readSession.document.lastOperation.operationId,
    checkpoint.options.operationId,
  );
  assert.equal(
    readSession.document.launch.launchAttemptId,
    launch.options.operationId,
  );
  assert.equal(
    readAttempt.session.document.lastOperation.operationId,
    checkpoint.options.operationId,
  );
  assert.deepEqual(readAttempt.launch, readSession.document.launch);
  assert.equal(
    authorityQueries(clients[0]).filter(
      (args) =>
        queryText(args) === READ_OPERATION_QUERY &&
        args[0]?.values?.[0] === launch.options.operationId,
    ).length,
    1,
  );
  assert.equal(
    authorityQueries(clients[1]).filter(
      (args) =>
        queryText(args) === READ_OPERATION_QUERY &&
        args[0]?.values?.[0] === launch.options.operationId,
    ).length,
    3,
  );
  for (const client of clients) client.assertExhausted();
});

test("current launch relation rejects forged historical operation identity and revision floors after last-operation replacement", async (t) => {
  for (const scenario of [
    {
      name: "session document identity",
      mutate(operation) {
        operation.request.expectedSession.document.backendCapabilities = {
          ...operation.request.expectedSession.document.backendCapabilities,
          atomicPointInTimeCheckpoint: false,
        };
      },
    },
    {
      name: "session revision floor",
      mutate(operation, checkpoint) {
        operation.request.expectedSession.revision =
          checkpoint.session.revision;
      },
    },
  ]) {
    await t.test(scenario.name, async () => {
      const launch = writerLaunchFixture();
      const checkpoint = writerLaunchCheckpointReplacementFixture(launch);
      const launchOperation = writerLaunchOperationRow(
        launch,
        "committed",
        {
          result: checkpoint.launchResult,
          revision: "2",
        },
      );
      scenario.mutate(launchOperation, checkpoint);
      const { authority, clients } = authorityWithScripts([
        rows(checkpoint.session),
        rows({ operation_count: 0, reservation_count: 0 }),
        rows(checkpoint.operation),
        rows(checkpoint.reservation),
        rows(checkpoint.attempt),
        rows(),
        rows(checkpoint.catalogue),
        rows(launchOperation),
      ]);

      await assertAuthorityError(
        authority.readSession({ sessionId: checkpoint.session.session_id }),
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
  }
});

test("active force-fence starting and uncertain states cannot drop the current launch pointer", async (t) => {
  for (const scenario of [
    {
      operationRevision: "1",
      state: "starting",
      updatedAt: LAUNCH_FENCE_DISPATCH_NOW,
    },
    {
      operationRevision: "2",
      state: "uncertain",
      updatedAt: LAUNCH_FENCE_UNCERTAIN_NOW,
    },
  ]) {
    await t.test(scenario.state, async () => {
      const launch = writerLaunchFixture();
      const expectedSession = snapshotFromSessionRow(
        writerLaunchCommittedSessionRow(launch),
      );
      const fixture = {
        expectedSession,
        result: {
          attachment: structuredClone(expectedSession.document.attachment),
        },
      };
      const options = writerForceFenceOptions(fixture);
      const session = writerForceFencePhaseSessionRow(scenario.state, {
        options,
        updatedAt: scenario.updatedAt,
      });
      session.document.launch = null;
      const operation = operationRow(scenario.state, {
        options,
        revision: scenario.operationRevision,
        createdAt: LAUNCH_FENCE_PREPARED_NOW,
        updatedAt: scenario.updatedAt,
      });
      const reservation = reservationRow(scenario.state, {
        options,
        createdAt: LAUNCH_FENCE_PREPARED_NOW,
        updatedAt: scenario.updatedAt,
      });
      const { authority, clients } = authorityWithScripts([
        rows(session),
        rows(operation),
        rows(reservation),
      ]);

      await assertAuthorityError(
        authority.readSession({ sessionId: expectedSession.sessionId }),
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
  }
});

test("force-fence success clears the current launch while uncertain BLOCKED preserves it", async (t) => {
  function scenarioFixture() {
    const launch = writerLaunchFixture();
    const launchResult = writerLaunchResult(launch);
    const expectedSession = snapshotFromSessionRow(
      writerLaunchCommittedSessionRow(launch, { result: launchResult }),
    );
    const fixture = {
      expectedSession,
      result: {
        attachment: structuredClone(expectedSession.document.attachment),
      },
    };
    const options = writerForceFenceOptions(fixture);
    const writerEpoch = (
      BigInt(expectedSession.document.writerEpoch) + 1n
    ).toString();
    return { fixture, launch, launchResult, options, writerEpoch };
  }

  function activeForceFenceSteps(
    scenario,
    state,
    session,
    updatedAt,
  ) {
    return [
      rows(session),
      rows(
        operationRow(state, {
          options: scenario.options,
          createdAt: LAUNCH_FENCE_PREPARED_NOW,
          updatedAt,
        }),
      ),
      rows(
        reservationRow(state, {
          options: scenario.options,
          createdAt: LAUNCH_FENCE_PREPARED_NOW,
          updatedAt,
        }),
      ),
      ...writerLaunchCommittedRelationSteps(scenario.launch, {
        result: scenario.launchResult,
      }),
      ...writerLaunchCommittedRelationSteps(scenario.launch, {
        result: scenario.launchResult,
      }),
    ];
  }

  await t.test("success clears launch", async () => {
    const scenario = scenarioFixture();
    const fenceResult = writerForceFenceProof(
      scenario.options,
      scenario.writerEpoch,
    );
    const result = writerForceFenceResult(
      scenario.options,
      scenario.writerEpoch,
      fenceResult,
    );
    const startingSession = writerForceFencePhaseSessionRow("starting", {
      options: scenario.options,
      writerEpoch: scenario.writerEpoch,
      updatedAt: LAUNCH_FENCE_DISPATCH_NOW,
    });
    const committedOperation = writerTerminalOperationRow({
      createdAt: LAUNCH_FENCE_PREPARED_NOW,
      options: scenario.options,
      result,
      revision: "2",
      updatedAt: LAUNCH_FENCE_FINALIZE_NOW,
    });
    const releasedReservation = reservationRow("released", {
      options: scenario.options,
      createdAt: LAUNCH_FENCE_PREPARED_NOW,
      updatedAt: LAUNCH_FENCE_FINALIZE_NOW,
      releasedAt: LAUNCH_FENCE_FINALIZE_NOW,
    });
    const detachedSession = writerDetachedSessionRow({
      options: scenario.options,
      result,
      operationRevision: "2",
      updatedAt: LAUNCH_FENCE_FINALIZE_NOW,
    });
    const { authority, clients } = authorityWithScripts({
      options: { now: LAUNCH_FENCE_FINALIZE_NOW },
      steps: [
        ...activeForceFenceSteps(
          scenario,
          "starting",
          startingSession,
          LAUNCH_FENCE_DISPATCH_NOW,
        ),
        rows(committedOperation),
        rows(releasedReservation),
        rows(detachedSession),
      ],
    });

    const finalized = await authority.finalizeWriterForceFence({
      ...scenario.options,
      expectedOperationRevision: "1",
      fenceResult,
    });

    assert.equal(finalized.finalized, true);
    assert.equal(finalized.session.document.lifecycle, "DETACHED");
    assert.equal(finalized.session.document.launch, null);
    clients[0].assertExhausted();
  });

  await t.test("uncertain BLOCKED preserves launch", async () => {
    const scenario = scenarioFixture();
    const reason = "fence-unavailable";
    const result = writerBlockedResult({
      options: scenario.options,
      lease: scenario.options.expectedSession.document.lease,
      attachment: scenario.options.expectedSession.document.attachment,
      writerEpoch: scenario.writerEpoch,
      reason,
    });
    const uncertainSession = writerForceFencePhaseSessionRow(
      "uncertain",
      {
        options: scenario.options,
        writerEpoch: scenario.writerEpoch,
        updatedAt: LAUNCH_FENCE_UNCERTAIN_NOW,
      },
    );
    const committedOperation = writerTerminalOperationRow({
      createdAt: LAUNCH_FENCE_PREPARED_NOW,
      options: scenario.options,
      result,
      revision: "3",
      updatedAt: LAUNCH_FENCE_FINALIZE_NOW,
    });
    const releasedReservation = reservationRow("released", {
      options: scenario.options,
      createdAt: LAUNCH_FENCE_PREPARED_NOW,
      updatedAt: LAUNCH_FENCE_FINALIZE_NOW,
      releasedAt: LAUNCH_FENCE_FINALIZE_NOW,
    });
    const blockedSession = writerBlockedSessionRow({
      options: scenario.options,
      result,
      updatedAt: LAUNCH_FENCE_FINALIZE_NOW,
    });
    const { authority, clients } = authorityWithScripts({
      options: { now: LAUNCH_FENCE_FINALIZE_NOW },
      steps: [
        ...activeForceFenceSteps(
          scenario,
          "uncertain",
          uncertainSession,
          LAUNCH_FENCE_UNCERTAIN_NOW,
        ),
        rows(committedOperation),
        rows(releasedReservation),
        rows(blockedSession),
      ],
    });

    const finalized = await authority.finalizeWriterOperationBlocked({
      ...scenario.options,
      expectedOperationRevision: "2",
      reason,
    });

    assert.equal(finalized.finalized, true);
    assert.equal(finalized.session.document.lifecycle, "BLOCKED");
    assert.deepEqual(
      finalized.session.document.launch,
      scenario.options.expectedSession.document.launch,
    );
    clients[0].assertExhausted();
  });
});

test("writer release rejects a current launch before PostgreSQL", async () => {
  const launch = writerLaunchFixture();
  const expectedSession = snapshotFromSessionRow(
    writerLaunchCommittedSessionRow(launch),
  );
  const fixture = {
    expectedSession,
    result: {
      attachment: structuredClone(expectedSession.document.attachment),
    },
  };
  const { authority, pool } = authorityWithScripts();

  await assertAuthorityError(
    authority.reserveOperation(writerReleaseOptions(fixture)),
    { code: "invalid_operation_request" },
  );

  assert.equal(pool.connectCalls, 0);
});

test("writer launch stop request owns the exact current launch pointer", async () => {
  const fixture = writerLaunchStopFixture();
  const legacy = createWriterLaunchStopOperationRequest({
    expectedSession: fixture.options.expectedSession,
  });
  const replay = createWriterLaunchStopOperationRequest({
    claimToken: fixture.claimToken,
    expectedSession: fixture.options.expectedSession,
  });
  assert.deepEqual(replay, canonicalPayload({
    contractVersion: 2,
    dispatchClaimSha256: writerLaunchStopClaimSha256(fixture.claimToken),
    launch: fixture.options.expectedSession.document.launch,
  }));
  assert.deepEqual(legacy, canonicalPayload({
    contractVersion: 1,
    launch: fixture.options.expectedSession.document.launch,
  }));
  assert.equal(JSON.stringify(replay).includes(fixture.claimToken), false);
  assertDeepFrozen(legacy);
  assertDeepFrozen(replay);

  for (const options of [
    {
      claimToken: fixture.claimToken,
      expectedSession: fixture.options.expectedSession,
      extra: true,
    },
    {
      claimToken: "not-a-uuid",
      expectedSession: fixture.options.expectedSession,
    },
  ]) {
    assert.throws(
      () => createWriterLaunchStopOperationRequest(options),
      (error) =>
        error instanceof PostgresSessionAuthorityError &&
        error.code === "invalid_operation_request",
    );
  }

  for (const mutate of [
    (expectedSession) => {
      expectedSession.document.launch = null;
    },
    (expectedSession) => {
      expectedSession.document.lifecycle = "BLOCKED";
    },
    (expectedSession) => {
      expectedSession.document.activeOperation = activeOperation(
        "prepared",
        { options: reserveOptions({ expectedSession }) },
      );
    },
  ]) {
    const expectedSession = structuredClone(fixture.options.expectedSession);
    mutate(expectedSession);
    assert.throws(
      () =>
        createWriterLaunchStopOperationRequest({
          claimToken: fixture.claimToken,
          expectedSession,
        }),
      (error) =>
        error instanceof PostgresSessionAuthorityError &&
        error.code === "invalid_operation_request",
    );
  }
});

test("writer launch stop V3 freezes the exact checkpoint capture intent", () => {
  const fixture = writerLaunchStopFixture({ contractVersion: 3 });
  assert.deepEqual(
    fixture.request,
    canonicalPayload({
      captureIntent: fixture.captureIntent.request,
      contractVersion: 3,
      dispatchClaimSha256: writerLaunchStopClaimSha256(
        fixture.claimToken,
      ),
      launch: fixture.options.expectedSession.document.launch,
    }),
  );
  assert.equal(
    fixture.request.captureIntent.admission.stopOperationId,
    fixture.options.operationId,
  );
  assert.notEqual(
    fixture.request.captureIntent.admission.request.operationId,
    fixture.options.operationId,
  );
  assert.equal(JSON.stringify(fixture.request).includes(fixture.claimToken), false);
  assertDeepFrozen(fixture.request);

  const wrongProcess = structuredClone(fixture.captureIntent.request);
  wrongProcess.admission.processIncarnationId =
    "process-incarnation-mismatch";
  assert.throws(
    () =>
      createWriterLaunchStopOperationRequest({
        captureIntent: wrongProcess,
        claimToken: fixture.claimToken,
        expectedSession: fixture.options.expectedSession,
      }),
    (error) =>
      error instanceof PostgresSessionAuthorityError &&
      error.code === "invalid_operation_request",
  );
});

test("writer launch stop V3 fresh reservation is default-closed while replay remains readable", async () => {
  const fixture = writerLaunchStopFixture({ contractVersion: 3 });
  const freshSteps = [
    ...writerLaunchCommittedSteps(fixture.launch, {
      result: fixture.launchResult,
    }),
    rows(),
  ];
  const deniedClient = new ScriptedClient(freshSteps);
  const replayClient = new ScriptedClient(
    writerLaunchStopActiveSteps(fixture, "prepared"),
  );
  const store = new PostgresSerializableStore({
    dedicatedPool: new ScriptedPool([deniedClient, replayClient]),
    maxTransactionAttempts: 1,
  });
  const authority = new PostgresSessionAuthority({ store });

  await assertAuthorityError(authority.reserveOperation(fixture.options), {
    code: "writer_launch_stop_v3_fleet_capability_required",
  });
  const replay = await authority.reserveOperation(fixture.options);

  assert.equal(replay.acquired, false);
  assert.equal(replay.operation.state, "prepared");
  assert.equal(
    authorityQueries(deniedClient).some((args) =>
      /^(?:INSERT|UPDATE) /u.test(queryText(args)),
    ),
    false,
  );
  deniedClient.assertExhausted();
  replayClient.assertExhausted();
});

test("writer launch stop V3 claim durably preclaims the capture operation ID and conflicts fail closed", async () => {
  const fixture = writerLaunchStopFixture({ contractVersion: 3 });
  const claim = {
    ...fixture.options,
    claimToken: fixture.claimToken,
    expectedOperationRevision: "0",
  };
  const { authority, clients } = authorityWithScripts(
    {
      options: { now: LAUNCH_STOP_DISPATCH_NOW },
      steps: [
        ...writerLaunchStopActiveSteps(fixture, "prepared"),
        rows(writerLaunchStopCaptureIdClaimRow(fixture)),
        rows(writerLaunchStopOperationRow(fixture, "starting")),
        rows(writerLaunchStopReservationRow(fixture, "starting")),
        rows(writerLaunchStopPhaseSessionRow(fixture, "starting")),
      ],
    },
    {
      options: { now: LAUNCH_STOP_DISPATCH_NOW },
      steps: [
        ...writerLaunchStopActiveSteps(fixture, "prepared"),
        rows(),
      ],
    },
  );

  const granted = await authority.claimWriterLaunchStopDispatch(claim);
  await assertAuthorityError(
    authority.claimWriterLaunchStopDispatch(claim),
    { code: "operation_identity_conflict" },
  );

  assert.equal(granted.dispatchGranted, true);
  assert.equal(granted.claimTokenMatched, true);
  assert.equal(granted.operation.state, "starting");
  const firstQueries = authorityQueries(clients[0]);
  const claimIndex = firstQueries.findIndex((args) =>
    queryText(args).includes("'writer-stop-capture-intent-v3'"),
  );
  const startIndex = firstQueries.findIndex(
    (args) => queryText(args) === START_OPERATION_QUERY,
  );
  assert.equal(claimIndex >= 0, true);
  assert.equal(startIndex > claimIndex, true);
  clients[0].assertExhausted();
  clients[1].assertExhausted();
});

test("writer launch stop V3 claim acknowledgement loss reconciles the reserved capture ID", async () => {
  const fixture = writerLaunchStopFixture({ contractVersion: 3 });
  const { authority, clients } = authorityWithScripts(
    {
      options: {
        commitError: new Error("stop V3 claim acknowledgement lost"),
        now: LAUNCH_STOP_DISPATCH_NOW,
      },
      steps: [
        ...writerLaunchStopActiveSteps(fixture, "prepared"),
        rows(writerLaunchStopCaptureIdClaimRow(fixture)),
        rows(writerLaunchStopOperationRow(fixture, "starting")),
        rows(writerLaunchStopReservationRow(fixture, "starting")),
        rows(writerLaunchStopPhaseSessionRow(fixture, "starting")),
      ],
    },
    writerLaunchStopActiveSteps(fixture, "starting"),
  );

  await assert.rejects(
    authority.claimWriterLaunchStopDispatch({
      ...fixture.options,
      claimToken: fixture.claimToken,
      expectedOperationRevision: "0",
    }),
    assertStoreCommitUncertain,
  );
  const reconciled =
    await authority.reconcileWriterLaunchStopOperation({
      ...fixture.options,
      claimToken: fixture.claimToken,
    });

  assert.equal(reconciled.status, "starting");
  assert.equal(reconciled.claimTokenMatched, true);
  assert.equal(
    authorityQueries(clients[1]).some((args) =>
      /^(?:INSERT|UPDATE) /u.test(queryText(args)),
    ),
    false,
  );
  clients[0].assertExhausted({ destroyed: true });
  clients[1].assertExhausted();
});

test("writer launch stop V3 atomically finalizes stop and prepares capture", async () => {
  const fixture = writerLaunchStopFixture({ contractVersion: 3 });
  const capture = writerLaunchStopCaptureFixture(fixture);
  const terminalSession = writerLaunchStopCommittedSessionRow(fixture);
  const captureOperation = checkpointCaptureOperationRow(
    capture,
    "prepared",
    {
      createdAt: LAUNCH_STOP_FINALIZE_NOW,
      updatedAt: LAUNCH_STOP_FINALIZE_NOW,
    },
  );
  const captureReservation = checkpointCaptureReservationRow(
    capture,
    "prepared",
    {
      createdAt: LAUNCH_STOP_FINALIZE_NOW,
      updatedAt: LAUNCH_STOP_FINALIZE_NOW,
    },
  );
  const captureSession = checkpointCapturePhaseSessionRow(
    capture,
    "prepared",
    { updatedAt: LAUNCH_STOP_FINALIZE_NOW },
  );
  const { authority, clients } = authorityWithScripts(
    {
      options: { now: LAUNCH_STOP_FINALIZE_NOW },
      steps: [
        ...writerLaunchStopActiveSteps(fixture, "starting"),
        rows(
          writerLaunchStopCaptureIdClaimRow(fixture, {
            materializedAt: LAUNCH_STOP_FINALIZE_NOW,
          }),
        ),
        rows(writerLaunchStopOperationRow(fixture, "committed")),
        rows(writerLaunchStopReservationRow(fixture, "released")),
        rows(terminalSession),
        rows(captureOperation),
        rows(captureReservation),
        rows(captureSession),
      ],
    },
    [
      ...writerLaunchStopCaptureActiveSteps(fixture, capture),
      ...writerLaunchStopCaptureRelationSteps(fixture, capture),
    ],
    [
      ...writerLaunchStopCaptureActiveSteps(fixture, capture),
      ...writerLaunchStopCaptureRelationSteps(fixture, capture),
    ],
  );
  const finalization = {
    ...fixture.options,
    evidence: fixture.evidence,
    expectedOperationRevision: "1",
  };

  await assertAuthorityError(
    authority.finalizeWriterLaunchStopped(finalization),
    { code: "invalid_operation_request" },
  );
  const receipt =
    await authority.finalizeWriterLaunchStoppedAndReserveCheckpointCapture(
      finalization,
    );
  const replay =
    await authority.finalizeWriterLaunchStoppedAndReserveCheckpointCapture(
      finalization,
    );
  await assertAuthorityError(
    authority.finalizeWriterLaunchStoppedAndReserveCheckpointCapture({
      ...finalization,
      evidence: {
        ...finalization.evidence,
        proofId: "supervisor-stop-proof-mismatch",
      },
    }),
    { code: "operation_result_conflict" },
  );

  assert.equal(receipt.status, "prepared");
  assert.equal(receipt.stop.finalized, true);
  assert.equal(replay.stop.finalized, false);
  assert.deepEqual(replay.capture, receipt.capture);
  assert.equal(receipt.stop.operation.state, "committed");
  assert.equal(receipt.capture.operation.state, "prepared");
  assert.equal(receipt.session.document.launch, null);
  assert.equal(
    receipt.session.document.activeOperation.operationId,
    capture.options.operationId,
  );
  assert.equal(
    receipt.capture.operation.createdAt,
    receipt.stop.operation.updatedAt,
  );
  const proof = assertWriterLaunchStopCaptureHandoffProof({
    before: fixture.options.expectedSession,
    capture: receipt.capture,
    session: receipt.session,
    stop: {
      operation: receipt.stop.operation,
      reservation: receipt.stop.reservation,
    },
  });
  assert.deepEqual(proof.capture, receipt.capture);
  assert.deepEqual(proof.stop, {
    operation: receipt.stop.operation,
    reservation: receipt.stop.reservation,
  });
  const delayedPrepared = structuredClone({
    before: fixture.options.expectedSession,
    capture: receipt.capture,
    session: receipt.session,
    stop: {
      operation: receipt.stop.operation,
      reservation: receipt.stop.reservation,
    },
  });
  delayedPrepared.capture.operation.updatedAt =
    LAUNCH_STOP_CAPTURE_DISPATCH_NOW;
  delayedPrepared.capture.reservation.updatedAt =
    LAUNCH_STOP_CAPTURE_DISPATCH_NOW;
  delayedPrepared.session.updatedAt = LAUNCH_STOP_CAPTURE_DISPATCH_NOW;
  assert.throws(
    () => assertWriterLaunchStopCaptureHandoffProof(delayedPrepared),
    (error) =>
      error instanceof PostgresSessionAuthorityError &&
      error.code === "operation_state_invalid",
  );
  assertDeepFrozen(receipt);
  assertDeepFrozen(proof);
  clients[0].assertExhausted();
});

test("writer launch stop V3 finalization acknowledgement loss reconciles the prepared capture tuple", async () => {
  const fixture = writerLaunchStopFixture({ contractVersion: 3 });
  const capture = writerLaunchStopCaptureFixture(fixture);
  const terminalSession = writerLaunchStopCommittedSessionRow(fixture);
  const captureOperation = checkpointCaptureOperationRow(
    capture,
    "prepared",
    {
      createdAt: LAUNCH_STOP_FINALIZE_NOW,
      updatedAt: LAUNCH_STOP_FINALIZE_NOW,
    },
  );
  const captureReservation = checkpointCaptureReservationRow(
    capture,
    "prepared",
    {
      createdAt: LAUNCH_STOP_FINALIZE_NOW,
      updatedAt: LAUNCH_STOP_FINALIZE_NOW,
    },
  );
  const captureSession = checkpointCapturePhaseSessionRow(
    capture,
    "prepared",
    { updatedAt: LAUNCH_STOP_FINALIZE_NOW },
  );
  const { authority, clients } = authorityWithScripts(
    {
      options: {
        commitError: new Error("stop-capture handoff acknowledgement lost"),
        now: LAUNCH_STOP_FINALIZE_NOW,
      },
      steps: [
        ...writerLaunchStopActiveSteps(fixture, "starting"),
        rows(
          writerLaunchStopCaptureIdClaimRow(fixture, {
            materializedAt: LAUNCH_STOP_FINALIZE_NOW,
          }),
        ),
        rows(writerLaunchStopOperationRow(fixture, "committed")),
        rows(writerLaunchStopReservationRow(fixture, "released")),
        rows(terminalSession),
        rows(captureOperation),
        rows(captureReservation),
        rows(captureSession),
      ],
    },
    [
      ...writerLaunchStopCaptureActiveSteps(fixture, capture),
      ...writerLaunchStopCaptureRelationSteps(fixture, capture),
    ],
  );
  const finalization = {
    ...fixture.options,
    evidence: fixture.evidence,
    expectedOperationRevision: "1",
  };

  await assert.rejects(
    authority.finalizeWriterLaunchStoppedAndReserveCheckpointCapture(
      finalization,
    ),
    assertStoreCommitUncertain,
  );
  const reconciled =
    await authority.reconcileWriterLaunchStopOperation({
      ...fixture.options,
      claimToken: fixture.claimToken,
    });

  assert.equal(reconciled.status, "prepared");
  assert.equal(reconciled.claimTokenMatched, true);
  assert.equal(reconciled.stop.finalized, false);
  assert.equal(
    reconciled.capture.operation.operationId,
    capture.options.operationId,
  );
  assert.equal(reconciled.capture.operation.state, "prepared");
  assert.equal(reconciled.capture.reservation.state, "prepared");
  assert.equal(
    reconciled.capture.operation.createdAt,
    LAUNCH_STOP_FINALIZE_NOW,
  );
  assert.equal(
    reconciled.session.document.activeOperation.operationId,
    capture.options.operationId,
  );
  assertDeepFrozen(reconciled);
  clients[0].assertExhausted({ destroyed: true });
  clients[1].assertExhausted();
});

test("writer launch stop V3 reconcile returns advanced starting and uncertain capture tuples", async () => {
  const fixture = writerLaunchStopFixture({ contractVersion: 3 });
  const capture = writerLaunchStopCaptureFixture(fixture);
  const states = ["starting", "uncertain"];
  const { authority, clients } = authorityWithScripts(
    ...states.map((state) => [
      ...writerLaunchStopCaptureActiveSteps(fixture, capture, state),
      ...writerLaunchStopCaptureRelationSteps(fixture, capture, state),
    ]),
  );

  for (const state of states) {
    const reconciled =
      await authority.reconcileWriterLaunchStopOperation({
        ...fixture.options,
        claimToken: fixture.claimToken,
      });
    assert.equal(reconciled.status, state);
    assert.equal(reconciled.claimTokenMatched, true);
    assert.equal(reconciled.capture.operation.state, state);
    assert.equal(reconciled.stop.finalized, false);
    assertWriterLaunchStopCaptureHandoffProof({
      before: fixture.options.expectedSession,
      capture: reconciled.capture,
      session: reconciled.session,
      stop: {
        operation: reconciled.stop.operation,
        reservation: reconciled.stop.reservation,
      },
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

test("writer launch stop V3 reconcile returns a committed capture tuple", async () => {
  const fixture = writerLaunchStopFixture({ contractVersion: 3 });
  const capture = writerLaunchStopCaptureFixture(fixture);
  const operation = checkpointCaptureOperationRow(capture, "committed", {
    createdAt: LAUNCH_STOP_FINALIZE_NOW,
    updatedAt: LAUNCH_STOP_CAPTURE_FINALIZE_NOW,
  });
  const reservation = checkpointCaptureReservationRow(
    capture,
    "released",
    {
      createdAt: LAUNCH_STOP_FINALIZE_NOW,
      updatedAt: LAUNCH_STOP_CAPTURE_FINALIZE_NOW,
    },
  );
  const catalogue = checkpointCatalogueRow(capture, {
    committedAt: LAUNCH_STOP_CAPTURE_FINALIZE_NOW,
  });
  const committedRelationSteps = () => [
    rows({ operation_count: 0, reservation_count: 0 }),
    rows(operation),
    rows(reservation),
    rows(
      checkpointCaptureAttemptRow(capture, {
        claimedAt: LAUNCH_STOP_CAPTURE_DISPATCH_NOW,
      }),
    ),
    rows(),
    rows(catalogue),
  ];
  const { authority, clients } = authorityWithScripts([
    rows(
      checkpointCaptureCommittedSessionRow(capture, {
        updatedAt: LAUNCH_STOP_CAPTURE_FINALIZE_NOW,
      }),
    ),
    ...committedRelationSteps(),
    rows(writerLaunchStopOperationRow(fixture, "committed")),
    rows(writerLaunchStopReservationRow(fixture, "released")),
    ...writerLaunchCommittedRelationSteps(fixture.launch, {
      result: fixture.launchResult,
    }),
    rows(
      writerLaunchStopCaptureIdClaimRow(fixture, {
        materializedAt: LAUNCH_STOP_FINALIZE_NOW,
      }),
    ),
    ...committedRelationSteps(),
  ]);

  const reconciled =
    await authority.reconcileWriterLaunchStopOperation({
      ...fixture.options,
      claimToken: fixture.claimToken,
    });

  assert.equal(reconciled.status, "committed");
  assert.equal(reconciled.claimTokenMatched, true);
  assert.equal(reconciled.capture.operation.state, "committed");
  assert.equal(
    reconciled.capture.operation.result.outcome,
    "checkpoint-captured",
  );
  assertWriterLaunchStopCaptureHandoffProof({
    before: fixture.options.expectedSession,
    capture: reconciled.capture,
    session: reconciled.session,
    stop: {
      operation: reconciled.stop.operation,
      reservation: reconciled.stop.reservation,
    },
  });
  clients[0].assertExhausted();
});

test("writer launch stop reconcile distinguishes exact and superseded absent preconditions", async () => {
  const fixture = writerLaunchStopFixture();
  const renewalOptions = renewOptions(fixture.options.expectedSession);
  const renewal = renewalResult(
    renewalOptions,
    LAUNCH_RENEW_AUTHORITY_NOW,
  );
  const renewalOperation = operationRow("committed", {
    options: renewalOptions,
    revision: "0",
    createdAt: LAUNCH_RENEW_TRANSACTION_NOW,
    updatedAt: LAUNCH_RENEW_TRANSACTION_NOW,
    retiredAt: LAUNCH_RENEW_TRANSACTION_NOW,
    result: renewal,
  });
  const renewalReservation = reservationRow("released", {
    options: renewalOptions,
    createdAt: LAUNCH_RENEW_TRANSACTION_NOW,
    updatedAt: LAUNCH_RENEW_TRANSACTION_NOW,
    releasedAt: LAUNCH_RENEW_TRANSACTION_NOW,
  });
  const renewedSession = renewedSessionRow({
    options: renewalOptions,
    result: renewal,
    updatedAt: LAUNCH_RENEW_TRANSACTION_NOW,
  });
  const { authority, clients } = authorityWithScripts(
    [
      ...writerLaunchCommittedSteps(fixture.launch, {
        result: fixture.launchResult,
      }),
      rows(),
      rows(),
    ],
    [
      rows(renewedSession),
      rows({ operation_count: 0, reservation_count: 0 }),
      rows(renewalOperation),
      rows(renewalReservation),
      ...writerLaunchCommittedRelationSteps(fixture.launch, {
        result: fixture.launchResult,
      }),
      rows(),
      rows(),
    ],
  );

  const exact = await authority.reconcileWriterLaunchStopOperation(
    { ...fixture.options, claimToken: fixture.claimToken },
  );
  const superseded = await authority.reconcileWriterLaunchStopOperation(
    { ...fixture.options, claimToken: fixture.claimToken },
  );
  await assertAuthorityError(
    authority.reconcileWriterLaunchStopOperation({
      ...fixture.options,
      claimToken: fixture.claimToken,
      extra: true,
    }),
    { code: "invalid_operation_request" },
  );

  assert.equal(exact.status, "absent");
  assert.equal(exact.claimTokenMatched, false);
  assert.equal(exact.expectedSessionMatched, true);
  assert.deepEqual(exact.session, fixture.options.expectedSession);
  assert.equal(superseded.status, "absent");
  assert.equal(superseded.claimTokenMatched, false);
  assert.equal(superseded.expectedSessionMatched, false);
  assert.deepEqual(
    superseded.session,
    snapshotFromSessionRow(renewedSession),
  );
  assertDeepFrozen(exact);
  assertDeepFrozen(superseded);
  for (const client of clients) {
    assert.deepEqual(
      authorityQueries(client)[0],
      extendedQuery(`${READ_SESSION_QUERY} FOR UPDATE`, [
        fixture.options.expectedSession.sessionId,
      ]),
    );
    assert.equal(
      authorityQueries(client).some((args) =>
        /^(?:INSERT|UPDATE) /u.test(queryText(args)),
      ),
      false,
    );
    client.assertExhausted();
  }
});

test("writer launch stop claim-token inputs are exact outer authority inputs", async () => {
  const fixture = writerLaunchStopFixture();
  const legacy = writerLaunchStopFixture({ contractVersion: 1 });
  const { authority, pool } = authorityWithScripts();
  const baseClaim = {
    ...fixture.options,
    expectedOperationRevision: "0",
  };

  for (const reconcile of [
    fixture.options,
    { ...fixture.options, claimToken: fixture.claimToken, extra: true },
    { ...fixture.options, claimToken: "not-a-uuid" },
    { ...legacy.options, claimToken: STOP_CLAIM_TOKEN },
  ]) {
    await assertAuthorityError(
      authority.reconcileWriterLaunchStopOperation(reconcile),
      { code: "invalid_operation_request" },
    );
  }
  for (const claim of [
    baseClaim,
    { ...baseClaim, claimToken: fixture.claimToken, extra: true },
    { ...baseClaim, claimToken: "not-a-uuid" },
    {
      ...legacy.options,
      claimToken: STOP_CLAIM_TOKEN,
      expectedOperationRevision: "0",
    },
  ]) {
    await assertAuthorityError(
      authority.claimWriterLaunchStopDispatch(claim),
      { code: "invalid_operation_request" },
    );
  }
  for (const request of [
    {
      ...legacy.request,
      dispatchClaimSha256: writerLaunchStopClaimSha256(STOP_CLAIM_TOKEN),
    },
    {
      contractVersion: 2,
      launch: fixture.request.launch,
    },
    {
      contractVersion: 2,
      dispatchClaimSha256: "g".repeat(64),
      launch: fixture.request.launch,
    },
  ]) {
    await assertAuthorityError(
      authority.reserveOperation({ ...fixture.options, request }),
      { code: "invalid_operation_request" },
    );
  }

  assert.equal(pool.connectCalls, 0);
});

test("writer launch stop claim grants only the exact persisted claim digest", async () => {
  const fixture = writerLaunchStopFixture();
  const { authority, clients } = authorityWithScripts(
    writerLaunchStopActiveSteps(fixture, "starting"),
    writerLaunchStopActiveSteps(fixture, "prepared"),
    writerLaunchStopActiveSteps(fixture, "starting"),
  );

  const sameTokenReplay = await authority.claimWriterLaunchStopDispatch({
    ...fixture.options,
    claimToken: fixture.claimToken,
    expectedOperationRevision: "0",
  });
  const wrongTokenPrepared = await authority.claimWriterLaunchStopDispatch({
    ...fixture.options,
    claimToken: OTHER_STOP_CLAIM_TOKEN,
    expectedOperationRevision: "0",
  });
  const wrongTokenReconcile =
    await authority.reconcileWriterLaunchStopOperation({
      ...fixture.options,
      claimToken: OTHER_STOP_CLAIM_TOKEN,
    });

  assert.equal(sameTokenReplay.status, "starting");
  assert.equal(sameTokenReplay.claimTokenMatched, true);
  assert.equal(sameTokenReplay.dispatchGranted, false);
  assert.equal(wrongTokenPrepared.status, "prepared");
  assert.equal(wrongTokenPrepared.claimTokenMatched, false);
  assert.equal(wrongTokenPrepared.dispatchGranted, false);
  assert.equal(wrongTokenReconcile.status, "starting");
  assert.equal(wrongTokenReconcile.claimTokenMatched, false);
  assertDeepFrozen(sameTokenReplay);
  assertDeepFrozen(wrongTokenPrepared);
  assertDeepFrozen(wrongTokenReconcile);
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

test("writer launch stop v1 durable lifecycle preserves legacy inputs and receipts", async () => {
  const fixture = writerLaunchStopFixture({ contractVersion: 1 });
  const startingOperation = writerLaunchStopOperationRow(
    fixture,
    "starting",
  );
  const startingReservation = writerLaunchStopReservationRow(
    fixture,
    "starting",
  );
  const startingSession = writerLaunchStopPhaseSessionRow(
    fixture,
    "starting",
  );
  const committedOperation = writerLaunchStopOperationRow(
    fixture,
    "committed",
  );
  const releasedReservation = writerLaunchStopReservationRow(
    fixture,
    "released",
  );
  const committedSession = writerLaunchStopCommittedSessionRow(fixture);
  const { authority, clients } = authorityWithScripts(
    writerLaunchStopActiveSteps(fixture, "prepared"),
    writerLaunchStopActiveSteps(fixture, "prepared"),
    {
      options: { now: LAUNCH_STOP_DISPATCH_NOW },
      steps: [
        ...writerLaunchStopActiveSteps(fixture, "prepared"),
        rows(startingOperation),
        rows(startingReservation),
        rows(startingSession),
      ],
    },
    writerLaunchStopActiveSteps(fixture, "starting"),
    writerLaunchStopActiveSteps(fixture, "starting"),
    writerLaunchStopActiveSteps(fixture, "starting"),
    {
      options: { now: LAUNCH_STOP_FINALIZE_NOW },
      steps: [
        ...writerLaunchStopActiveSteps(fixture, "starting"),
        rows(committedOperation),
        rows(releasedReservation),
        rows(committedSession),
      ],
    },
    writerLaunchStopCommittedSteps(fixture),
    writerLaunchStopCommittedSteps(fixture),
    [
      ...writerLaunchCommittedSteps(fixture.launch, {
        result: fixture.launchResult,
      }),
      rows(),
      rows(),
    ],
  );

  const preparedRead = await authority.readSession({
    sessionId: fixture.options.expectedSession.sessionId,
  });
  const preparedReconcile =
    await authority.reconcileWriterLaunchStopOperation(fixture.options);
  const claimed = await authority.claimWriterLaunchStopDispatch({
    ...fixture.options,
    expectedOperationRevision: "0",
  });
  const startingRead = await authority.readSession({
    sessionId: fixture.options.expectedSession.sessionId,
  });
  const startingReconcile =
    await authority.reconcileWriterLaunchStopOperation(fixture.options);
  const startingClaimReplay =
    await authority.claimWriterLaunchStopDispatch({
      ...fixture.options,
      expectedOperationRevision: "0",
    });
  const finalized = await authority.finalizeWriterLaunchStopped({
    ...fixture.options,
    evidence: fixture.evidence,
    expectedOperationRevision: "1",
  });
  const committedRead = await authority.readSession({
    sessionId: fixture.options.expectedSession.sessionId,
  });
  const committedReconcile =
    await authority.reconcileWriterLaunchStopOperation(fixture.options);
  const absentReconcile =
    await authority.reconcileWriterLaunchStopOperation(fixture.options);

  assert.equal(fixture.request.contractVersion, 1);
  assert.equal(Object.hasOwn(fixture.request, "dispatchClaimSha256"), false);
  assert.equal(preparedRead.document.activeOperation.state, "prepared");
  assert.equal(preparedReconcile.status, "prepared");
  assert.equal(Object.hasOwn(preparedReconcile, "claimTokenMatched"), false);
  assert.equal(claimed.status, "starting");
  assert.equal(claimed.dispatchGranted, true);
  assert.equal(Object.hasOwn(claimed, "claimTokenMatched"), false);
  assert.equal(claimed.stop.contractVersion, 1);
  assert.equal(startingRead.document.activeOperation.state, "starting");
  assert.equal(startingReconcile.status, "starting");
  assert.equal(Object.hasOwn(startingReconcile, "claimTokenMatched"), false);
  assert.equal(startingClaimReplay.dispatchGranted, false);
  assert.equal(
    Object.hasOwn(startingClaimReplay, "claimTokenMatched"),
    false,
  );
  assert.equal(finalized.finalized, true);
  assert.equal(finalized.stop.contractVersion, 1);
  assert.equal(committedRead.document.launch, null);
  assert.equal(committedReconcile.status, "committed");
  assert.equal(Object.hasOwn(committedReconcile, "claimTokenMatched"), false);
  assert.equal(absentReconcile.status, "absent");
  assert.equal(Object.hasOwn(absentReconcile, "claimTokenMatched"), false);
  const proof = assertCommittedWriterLaunchStopTransitionProof({
    after: finalized.session,
    before: fixture.options.expectedSession,
    operation: finalized.operation,
    reservation: finalized.reservation,
  });
  assert.equal(proof.operation.request.contractVersion, 1);
  assertDeepFrozen(proof);
  for (const client of clients) client.assertExhausted();
});

test("writer launch stop claim, finalize, and exact replay clear only the current launch", async () => {
  const fixture = writerLaunchStopFixture();
  const startingOperation = writerLaunchStopOperationRow(
    fixture,
    "starting",
  );
  const startingReservation = writerLaunchStopReservationRow(
    fixture,
    "starting",
  );
  const startingSession = writerLaunchStopPhaseSessionRow(
    fixture,
    "starting",
  );
  const committedOperation = writerLaunchStopOperationRow(
    fixture,
    "committed",
  );
  const releasedReservation = writerLaunchStopReservationRow(
    fixture,
    "released",
  );
  const committedSession = writerLaunchStopCommittedSessionRow(fixture);
  const { authority, clients } = authorityWithScripts(
    {
      options: { now: LAUNCH_STOP_DISPATCH_NOW },
      steps: [
        ...writerLaunchStopActiveSteps(fixture, "prepared"),
        rows(startingOperation),
        rows(startingReservation),
        rows(startingSession),
      ],
    },
    {
      options: { now: LAUNCH_STOP_FINALIZE_NOW },
      steps: [
        ...writerLaunchStopActiveSteps(fixture, "starting"),
        rows(committedOperation),
        rows(releasedReservation),
        rows(committedSession),
      ],
    },
    writerLaunchStopCommittedSteps(fixture),
  );

  const claimed = await authority.claimWriterLaunchStopDispatch({
    ...fixture.options,
    claimToken: fixture.claimToken,
    expectedOperationRevision: "0",
  });
  const finalized = await authority.finalizeWriterLaunchStopped({
    ...fixture.options,
    evidence: fixture.evidence,
    expectedOperationRevision: "1",
  });
  const replayed = await authority.finalizeWriterLaunchStopped({
    ...fixture.options,
    evidence: fixture.evidence,
    expectedOperationRevision: "1",
  });

  assert.equal(claimed.claimTokenMatched, true);
  assert.equal(claimed.dispatchGranted, true);
  assert.equal(claimed.stop.contractVersion, 2);
  assert.deepEqual(canonicalPayload(claimed.launch), fixture.request.launch);
  assert.equal(finalized.finalized, true);
  assert.equal(finalized.stop.contractVersion, 2);
  assert.equal(finalized.launch, null);
  assert.equal(finalized.session.document.launch, null);
  assert.equal(
    finalized.session.document.lastOperation.operationId,
    fixture.options.operationId,
  );
  assert.deepEqual(finalized.operation.result, fixture.result);
  const proof = {
    after: finalized.session,
    before: fixture.options.expectedSession,
    operation: finalized.operation,
    reservation: finalized.reservation,
  };
  const validatedProof = assertCommittedWriterLaunchStopTransitionProof(proof);
  assertDeepFrozen(validatedProof);
  assert.deepEqual(
    JSON.parse(JSON.stringify(validatedProof)),
    JSON.parse(JSON.stringify(proof)),
  );

  for (const mutate of [
    (candidate) => {
      candidate.after.revision = (
        BigInt(candidate.after.revision) + 1n
      ).toString();
    },
    (candidate) => {
      candidate.after.document.documentVersion = 2;
    },
    (candidate) => {
      candidate.after.document.launch = structuredClone(
        candidate.before.document.launch,
      );
    },
    (candidate) => {
      candidate.after.document.lastOperation.resultSha256 = "f".repeat(64);
    },
    (candidate) => {
      candidate.operation.request.launch.writerIncarnationId =
        "writer-incarnation-tampered";
    },
    (candidate) => {
      candidate.reservation.operationId = "writer-stop-operation-tampered";
    },
  ]) {
    const candidate = structuredClone(proof);
    mutate(candidate);
    assert.throws(
      () => assertCommittedWriterLaunchStopTransitionProof(candidate),
      (error) =>
        error instanceof PostgresSessionAuthorityError &&
        error.code === "operation_state_invalid",
    );
  }
  assert.equal(replayed.finalized, false);
  assert.equal(replayed.launch, null);
  assert.deepEqual(replayed.operation.result, fixture.result);
  for (const client of clients) {
    assert.equal(
      authorityQueries(client).some(
        (args) =>
          /^(?:UPDATE) /u.test(queryText(args)) &&
          args[0]?.values?.[0] === fixture.launch.options.operationId,
      ),
      false,
    );
    client.assertExhausted();
  }
});

test("writer launch stop claim acknowledgement loss reconciles the exact claimant", async () => {
  const fixture = writerLaunchStopFixture();
  const { authority, clients } = authorityWithScripts(
    {
      options: {
        commitError: new Error("writer stop claim acknowledgement lost"),
        now: LAUNCH_STOP_DISPATCH_NOW,
      },
      steps: [
        ...writerLaunchStopActiveSteps(fixture, "prepared"),
        rows(writerLaunchStopOperationRow(fixture, "starting")),
        rows(writerLaunchStopReservationRow(fixture, "starting")),
        rows(writerLaunchStopPhaseSessionRow(fixture, "starting")),
      ],
    },
    writerLaunchStopActiveSteps(fixture, "starting"),
  );
  const claim = {
    ...fixture.options,
    claimToken: fixture.claimToken,
    expectedOperationRevision: "0",
  };

  await assert.rejects(
    authority.claimWriterLaunchStopDispatch(claim),
    assertStoreCommitUncertain,
  );
  const reconciled = await authority.reconcileWriterLaunchStopOperation({
    ...fixture.options,
    claimToken: fixture.claimToken,
  });

  assert.equal(reconciled.status, "starting");
  assert.equal(reconciled.claimTokenMatched, true);
  assertDeepFrozen(reconciled);
  assert.equal(
    authorityQueries(clients[1]).some((args) =>
      /^(?:INSERT|UPDATE) /u.test(queryText(args)),
    ),
    false,
  );
  clients[0].assertExhausted({ destroyed: true });
  clients[1].assertExhausted();
});

test("lost-ack writer launch stop replays do not expose a successor launch", async () => {
  const stopped = writerLaunchStopFixture();
  const successor = writerLaunchFixture({
    expectedSession: snapshotFromSessionRow(
      writerLaunchStopCommittedSessionRow(stopped, {
        updatedAt: REPLAY_STOP_FINALIZE_NOW,
      }),
    ),
    launchOperationId: "writer-launch-attempt-operation-successor",
  });
  const successorResult = writerLaunchResult(successor, "started", {
    evidence: writerLaunchEvidence(successor, "started", {
      processIncarnationId: "process-incarnation-successor",
      proofId: "supervisor-proof-successor",
      writerIncarnationId: "writer-incarnation-successor",
    }),
  });
  const successorPointer = writerLaunchPointer(
    successor,
    successorResult,
    SUCCESSOR_LAUNCH_FINALIZE_NOW,
  );
  const replaySteps = () => [
    ...writerLaunchCommittedSteps(successor, {
      createdAt: SUCCESSOR_LAUNCH_PREPARED_NOW,
      result: successorResult,
      updatedAt: SUCCESSOR_LAUNCH_FINALIZE_NOW,
    }),
    rows(
      writerLaunchStopOperationRow(stopped, "committed", {
        createdAt: REPLAY_STOP_PREPARED_NOW,
        updatedAt: REPLAY_STOP_FINALIZE_NOW,
      }),
    ),
    rows(
      writerLaunchStopReservationRow(stopped, "released", {
        createdAt: REPLAY_STOP_PREPARED_NOW,
        updatedAt: REPLAY_STOP_FINALIZE_NOW,
      }),
    ),
    ...writerLaunchCommittedRelationSteps(stopped.launch, {
      result: stopped.launchResult,
    }),
  ];
  const finalization = {
    ...stopped.options,
    evidence: stopped.evidence,
    expectedOperationRevision: "1",
  };
  const claim = {
    ...stopped.options,
    claimToken: stopped.claimToken,
    expectedOperationRevision: "0",
  };
  const finalizeLoss = authorityWithScripts(
    {
      options: {
        commitError: new Error("writer stop finalization acknowledgement lost"),
        now: REPLAY_STOP_FINALIZE_NOW,
      },
      steps: [
        ...writerLaunchStopActiveSteps(stopped, "starting", {
          createdAt: REPLAY_STOP_PREPARED_NOW,
          updatedAt: REPLAY_STOP_DISPATCH_NOW,
        }),
        rows(
          writerLaunchStopOperationRow(stopped, "committed", {
            createdAt: REPLAY_STOP_PREPARED_NOW,
            updatedAt: REPLAY_STOP_FINALIZE_NOW,
          }),
        ),
        rows(
          writerLaunchStopReservationRow(stopped, "released", {
            createdAt: REPLAY_STOP_PREPARED_NOW,
            updatedAt: REPLAY_STOP_FINALIZE_NOW,
          }),
        ),
        rows(
          writerLaunchStopCommittedSessionRow(stopped, {
            updatedAt: REPLAY_STOP_FINALIZE_NOW,
          }),
        ),
      ],
    },
    replaySteps(),
  );
  const claimLoss = authorityWithScripts(
    {
      options: {
        commitError: new Error("writer stop claim acknowledgement lost"),
        now: REPLAY_STOP_DISPATCH_NOW,
      },
      steps: [
        ...writerLaunchStopActiveSteps(stopped, "prepared", {
          createdAt: REPLAY_STOP_PREPARED_NOW,
          updatedAt: REPLAY_STOP_PREPARED_NOW,
        }),
        rows(
          writerLaunchStopOperationRow(stopped, "starting", {
            createdAt: REPLAY_STOP_PREPARED_NOW,
            updatedAt: REPLAY_STOP_DISPATCH_NOW,
          }),
        ),
        rows(
          writerLaunchStopReservationRow(stopped, "starting", {
            createdAt: REPLAY_STOP_PREPARED_NOW,
            updatedAt: REPLAY_STOP_DISPATCH_NOW,
          }),
        ),
        rows(
          writerLaunchStopPhaseSessionRow(stopped, "starting", {
            updatedAt: REPLAY_STOP_DISPATCH_NOW,
          }),
        ),
      ],
    },
    replaySteps(),
  );

  await assert.rejects(
    finalizeLoss.authority.finalizeWriterLaunchStopped(finalization),
    assertStoreCommitUncertain,
  );
  const finalized =
    await finalizeLoss.authority.finalizeWriterLaunchStopped(finalization);
  await assert.rejects(
    claimLoss.authority.claimWriterLaunchStopDispatch(claim),
    assertStoreCommitUncertain,
  );
  const claimed =
    await claimLoss.authority.claimWriterLaunchStopDispatch(claim);

  assert.notEqual(
    successorPointer.launchAttemptId,
    stopped.request.launch.launchAttemptId,
  );
  assert.equal(finalized.finalized, false);
  assert.equal(finalized.launch, null);
  assert.deepEqual(finalized.session.document.launch, successorPointer);
  assert.throws(
    () =>
      assertCommittedWriterLaunchStopTransitionProof({
        after: finalized.session,
        before: stopped.options.expectedSession,
        operation: finalized.operation,
        reservation: finalized.reservation,
      }),
    (error) =>
      error instanceof PostgresSessionAuthorityError &&
      error.code === "operation_state_invalid",
  );
  assert.equal(claimed.dispatchGranted, false);
  assert.equal(claimed.claimTokenMatched, true);
  assert.equal(claimed.launch, null);
  assert.deepEqual(claimed.session.document.launch, successorPointer);
  const clients = [...finalizeLoss.clients, ...claimLoss.clients];
  for (let index = 0; index < clients.length; index += 1) {
    const client = clients[index];
    assert.equal(
      client.userSteps.length,
      0,
      `lost-ack replay client ${index} left scripted queries`,
    );
    client.assertExhausted({ destroyed: index === 0 || index === 2 });
  }
});

test("writer launch stop rejects tuple and replay proof mismatches without clearing launch", async (t) => {
  const fixture = writerLaunchStopFixture();
  for (const [field, value] of [
    ["launchAttemptId", "writer-launch-attempt-mismatch"],
    ["supervisorId", "supervisor-mismatch"],
    ["processIncarnationId", "process-incarnation-mismatch"],
    ["writerIncarnationId", "writer-incarnation-mismatch"],
  ]) {
    await t.test(field, async () => {
      const { authority, pool } = authorityWithScripts();
      await assertAuthorityError(
        authority.finalizeWriterLaunchStopped({
          ...fixture.options,
          evidence: { ...fixture.evidence, [field]: value },
          expectedOperationRevision: "1",
        }),
        { code: "invalid_operation_request" },
      );
      assert.equal(pool.connectCalls, 0);
    });
  }

  await t.test("proof replay", async () => {
    const { authority, clients } = authorityWithScripts(
      writerLaunchStopCommittedSteps(fixture),
    );
    await assertAuthorityError(
      authority.finalizeWriterLaunchStopped({
        ...fixture.options,
        evidence: {
          ...fixture.evidence,
          proofId: "supervisor-stop-proof-mismatch",
        },
        expectedOperationRevision: "1",
      }),
      { code: "operation_result_conflict" },
    );
    assert.equal(
      authorityQueries(clients[0]).some((args) =>
        /^(?:INSERT|UPDATE) /u.test(queryText(args)),
      ),
      false,
    );
    clients[0].assertExhausted();
  });
});

test("writer launch stop fails closed when the original started result drifts", async () => {
  const fixture = writerLaunchStopFixture();
  const corruptedResult = writerLaunchResult(fixture.launch, "started", {
    evidence: writerLaunchEvidence(fixture.launch, "started", {
      proofId: "different-original-start-proof",
    }),
  });
  const { authority, clients } = authorityWithScripts([
    rows(writerLaunchStopPhaseSessionRow(fixture, "starting")),
    rows(writerLaunchStopOperationRow(fixture, "starting")),
    rows(writerLaunchStopReservationRow(fixture, "starting")),
    ...writerLaunchCommittedRelationSteps(fixture.launch, {
      result: corruptedResult,
    }),
  ]);

  await assertAuthorityError(
    authority.finalizeWriterLaunchStopped({
      ...fixture.options,
      evidence: fixture.evidence,
      expectedOperationRevision: "1",
    }),
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

test("writer launch stop readback rejects a forged historical lease relation", async () => {
  const fixture = writerLaunchStopFixture();
  const expectedSession = structuredClone(fixture.options.expectedSession);
  expectedSession.document.lease.expiresAt = "2026-01-01T00:00:00.000Z";
  const request = createWriterLaunchStopOperationRequest({
    claimToken: fixture.claimToken,
    expectedSession,
  });
  const forged = {
    ...fixture,
    options: {
      ...fixture.options,
      expectedSession,
      request,
    },
    request,
  };
  const { authority, clients } = authorityWithScripts(
    writerLaunchStopCommittedSteps(forged),
  );

  await assertAuthorityError(
    authority.readSession({ sessionId: expectedSession.sessionId }),
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

test("writer launch recovery enumeration returns validated active attempts with state and keyset pagination", async () => {
  const second = writerLaunchFixture({
    destinationIsolationProofId: "destination-isolation-proof-002",
    generationId: "restore-generation-002",
    launchOperationId: "writer-launch-attempt-operation-002",
    restoreOperationId: "restore-generation-operation-002",
    sessionId: OTHER_SESSION_ID,
  });
  const third = writerLaunchFixture({
    destinationIsolationProofId: "destination-isolation-proof-003",
    generationId: "restore-generation-003",
    launchOperationId: "writer-launch-attempt-operation-003",
    restoreOperationId: "restore-generation-operation-003",
    sessionId: THIRD_SESSION_ID,
  });
  const { authority, clients } = authorityWithScripts(
    [
      rows(
        writerLaunchOperationRow(second, "starting"),
        writerLaunchOperationRow(third, "uncertain"),
      ),
      ...writerLaunchActiveSteps(second, "starting"),
      ...writerLaunchActiveSteps(third, "uncertain"),
    ],
    [
      rows(writerLaunchOperationRow(third, "uncertain")),
      ...writerLaunchActiveSteps(third, "uncertain"),
    ],
  );

  const firstPage =
    await authority.listWriterLaunchAttemptRecoveryCandidates({
      afterSessionId: SESSION_ID,
      limit: 1,
    });
  const secondPage =
    await authority.listWriterLaunchAttemptRecoveryCandidates({
      afterSessionId: firstPage.nextAfterSessionId,
      limit: 1,
    });

  assert.deepEqual(firstPage, {
    candidates: [
      {
        launchAttemptId: second.options.operationId,
        request: second.request,
        state: "starting",
      },
    ],
    nextAfterSessionId: OTHER_SESSION_ID,
  });
  assert.deepEqual(secondPage, {
    candidates: [
      {
        launchAttemptId: third.options.operationId,
        request: third.request,
        state: "uncertain",
      },
    ],
    nextAfterSessionId: null,
  });
  assertDeepFrozen(firstPage);
  assertDeepFrozen(secondPage);
  assert.deepEqual(
    authorityQueries(clients[0])[0],
    extendedQuery(LIST_WRITER_LAUNCH_RECOVERY_AFTER_QUERY, [
      SESSION_ID,
      2,
    ]),
  );
  assert.deepEqual(
    authorityQueries(clients[1])[0],
    extendedQuery(LIST_WRITER_LAUNCH_RECOVERY_AFTER_QUERY, [
      OTHER_SESSION_ID,
      2,
    ]),
  );
  for (const client of clients) client.assertExhausted();
});

test("writer launch recovery enumeration includes exact prepared intents", async () => {
  const fixture = writerLaunchFixture();
  const { authority, clients } = authorityWithScripts([
    rows(writerLaunchOperationRow(fixture, "prepared")),
    ...writerLaunchActiveSteps(fixture, "prepared"),
  ]);

  const page = await authority.listWriterLaunchAttemptRecoveryCandidates({
    afterSessionId: null,
    limit: 1,
  });

  assert.deepEqual(page, {
    candidates: [
      {
        launchAttemptId: fixture.options.operationId,
        request: fixture.request,
        state: "prepared",
      },
    ],
    nextAfterSessionId: null,
  });
  assert.deepEqual(
    authorityQueries(clients[0])[0],
    extendedQuery(LIST_WRITER_LAUNCH_RECOVERY_FIRST_PAGE_QUERY, [2]),
  );
  assertDeepFrozen(page);
  clients[0].assertExhausted();
});

test("writer launch recovery enumeration validates activation-created handoff provenance", async (t) => {
  const fixture = restoreAttachmentActivationFixture();
  const launch = restoreAttachmentActivationLaunchFixture(fixture);

  for (const state of ["prepared", "starting", "uncertain"]) {
    await t.test(`valid ${state} handoff`, async () => {
      const { authority, clients } = authorityWithScripts(
        restoreAttachmentActivationLaunchRecoverySteps(fixture, { state }),
      );

      const page = await authority.listWriterLaunchAttemptRecoveryCandidates({
        afterSessionId: null,
        limit: 1,
      });

      assert.deepEqual(page, {
        candidates: [
          {
            launchAttemptId: launch.options.operationId,
            request: launch.request,
            state,
          },
        ],
        nextAfterSessionId: null,
      });
      assertDeepFrozen(page);
      clients[0].assertExhausted();
    });
  }

  const corruptedSupervisor = {
    ...structuredClone(launch.supervisor),
    supervisorId: "restore-activation-supervisor-corrupt",
  };
  const corruptedRequest = createWriterLaunchAttemptOperationRequest({
    expectedSession: launch.options.expectedSession,
    generation: launch.generation,
    measuredImage: launch.measuredImage,
    supervisor: corruptedSupervisor,
  });
  const corruptedLaunch = {
    ...launch,
    options: {
      ...launch.options,
      request: corruptedRequest,
    },
    request: corruptedRequest,
    supervisor: corruptedSupervisor,
  };
  const corruptionCases = [
    {
      name: "activation-derived launch input",
      steps: restoreAttachmentActivationLaunchRecoverySteps(fixture, {
        launch: corruptedLaunch,
      }),
    },
    {
      name: "materialized launch claim timestamp",
      steps: restoreAttachmentActivationLaunchRecoverySteps(fixture, {
        launchIdClaim: restoreAttachmentActivationLaunchIdClaimRow(
          fixture,
          { materializedAt: "2026-07-29T12:36:23.000Z" },
        ),
      }),
    },
    {
      name: "launch createdAt provenance",
      steps: restoreAttachmentActivationLaunchRecoverySteps(fixture, {
        state: "starting",
        launchCreatedAt: "2026-07-29T12:36:22.500Z",
      }),
    },
  ];
  for (const corruption of corruptionCases) {
    await t.test(corruption.name, async () => {
      const { authority, clients } = authorityWithScripts(
        corruption.steps,
      );

      await assertAuthorityError(
        authority.listWriterLaunchAttemptRecoveryCandidates({
          afterSessionId: null,
          limit: 1,
        }),
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
  }
});

test("writer launch read and claim validate activation-created handoff provenance before receipts or writes", async () => {
  const fixture = restoreAttachmentActivationFixture();
  const corruptedLaunchIdClaim =
    restoreAttachmentActivationLaunchIdClaimRow(fixture, {
      materializedAt: "2026-07-29T12:36:23.000Z",
    });
  const readSteps = restoreAttachmentActivationLaunchRecoverySteps(
    fixture,
    { launchIdClaim: corruptedLaunchIdClaim },
  );
  const claimSteps = restoreAttachmentActivationLaunchRecoverySteps(
    fixture,
    { launchIdClaim: corruptedLaunchIdClaim },
  ).slice(1);
  const { authority, clients } = authorityWithScripts(
    readSteps,
    claimSteps,
  );

  await assertAuthorityError(
    authority.readWriterLaunchAttempt({
      operationId: fixture.launchIntent.launchAttemptId,
    }),
    { code: "operation_state_invalid" },
  );
  await assertAuthorityError(
    authority.claimWriterLaunchAttemptDispatch({
      ...restoreAttachmentActivationLaunchFixture(fixture).options,
      expectedOperationRevision: "0",
    }),
    { code: "operation_state_invalid" },
  );

  for (const client of clients) {
    assert.equal(
      authorityQueries(client).some((args) =>
        /^(?:INSERT|UPDATE) /u.test(queryText(args)),
      ),
      false,
    );
    client.assertExhausted();
  }
  assert.equal(
    queryTexts(clients[1]).includes(READ_AUTHORITY_CLOCK_QUERY),
    false,
  );
});

test("current writer launch recovery uses sparse bounded session pages", async () => {
  const launch = writerLaunchFixture({
    destinationIsolationProofId: "destination-isolation-proof-current-page",
    generationId: "restore-generation-current-page",
    launchOperationId: "writer-launch-attempt-current-page",
    restoreOperationId: "restore-generation-operation-current-page",
    sessionId: OTHER_SESSION_ID,
  });
  const launchResult = writerLaunchResult(launch);
  const detached = sessionRow({ sessionId: SESSION_ID });
  const launched = writerLaunchCommittedSessionRow(launch, {
    result: launchResult,
  });
  const { authority, clients } = authorityWithScripts(
    [
      rows(detached, launched),
      rows({ operation_count: 0, reservation_count: 0 }),
    ],
    [
      rows(launched),
      rows({ operation_count: 0, reservation_count: 0 }),
      ...writerLaunchCommittedRelationSteps(launch, {
        result: launchResult,
      }),
      ...writerLaunchCommittedRelationSteps(launch, {
        result: launchResult,
      }),
    ],
  );

  const sparse = await authority.listCurrentWriterLaunchRecoveryCandidates({
    afterSessionId: null,
    limit: 1,
  });
  const current = await authority.listCurrentWriterLaunchRecoveryCandidates({
    afterSessionId: sparse.nextAfterSessionId,
    limit: 1,
  });

  assert.deepEqual(sparse, {
    candidates: [],
    nextAfterSessionId: SESSION_ID,
  });
  assert.deepEqual(current, {
    candidates: [
      {
        launch: launched.document.launch,
        launchAttemptId: launch.options.operationId,
        request: launch.request,
      },
    ],
    nextAfterSessionId: null,
  });
  assert.deepEqual(
    authorityQueries(clients[0])[0],
    extendedQuery(LIST_CURRENT_WRITER_LAUNCH_FIRST_PAGE_QUERY, [2]),
  );
  assert.deepEqual(
    authorityQueries(clients[1])[0],
    extendedQuery(LIST_CURRENT_WRITER_LAUNCH_AFTER_QUERY, [SESSION_ID, 2]),
  );
  assertDeepFrozen(sparse);
  assertDeepFrozen(current);
  for (const client of clients) client.assertExhausted();
});

test("current writer launch recovery fails closed on corrupt launch history", async () => {
  const launch = writerLaunchFixture();
  const launchResult = writerLaunchResult(launch);
  const launched = writerLaunchCommittedSessionRow(launch, {
    result: launchResult,
  });
  const corruptedOperation = writerLaunchOperationRow(
    launch,
    "committed",
    { result: launchResult, revision: "2" },
  );
  corruptedOperation.request.expectedSession.revision = launched.revision;
  const { authority, clients } = authorityWithScripts([
    rows(launched),
    rows({ operation_count: 0, reservation_count: 0 }),
    rows(corruptedOperation),
  ]);

  await assertAuthorityError(
    authority.listCurrentWriterLaunchRecoveryCandidates({
      afterSessionId: null,
      limit: 1,
    }),
    { code: "operation_state_invalid" },
  );
  clients[0].assertExhausted();
});

test("restore generation request builder owns the exact canonical admission and predetermined result", () => {
  const fixture = restoreGenerationFixture();
  const replay = createRestoreDestinationGenerationOperationRequest({
    admission: fixture.admission,
    expectedSession: fixture.options.expectedSession,
  });

  assert.equal(
    RESTORE_DESTINATION_GENERATION_OPERATION_KIND,
    "restore-destination-generation-v1",
  );
  assert.deepEqual(replay, fixture.request);
  assert.deepEqual(Reflect.ownKeys(replay.admission), [
    "checkpoint",
    "request",
  ]);
  assert.equal(
    replay.predeterminedResult.mutation.proofId,
    `proof-restore-${sha256(
      `restore-destination-proof:${RESTORE_OPERATION_ID}`,
    )}`,
  );
  assert.deepEqual(
    replay.predeterminedResult.checkpoint,
    canonicalPayload(fixture.source.checkpoint),
  );
  assertDeepFrozen(replay);

  for (const admission of [
    { ...fixture.admission, extra: true },
    {
      ...fixture.admission,
      request: {
        ...fixture.mutationRequest,
        fencingEpoch: fixture.source.checkpoint.sourceFencingEpoch,
      },
    },
    {
      ...fixture.admission,
      checkpoint: {
        ...fixture.source.checkpoint,
        checkpointClass: "crash-prefix",
      },
    },
  ]) {
    assert.throws(
      () =>
        createRestoreDestinationGenerationOperationRequest({
          admission,
          expectedSession: fixture.options.expectedSession,
        }),
      (error) =>
        error instanceof PostgresSessionAuthorityError &&
        error.code === "invalid_operation_request",
    );
  }
});

test("restore generation v2 request durably binds an exact launch intent without an opaque image reservation", () => {
  const fixture = restoreGenerationFixture({
    launchAttemptId: LAUNCH_ATTEMPT_OPERATION_ID,
  });
  const replay = createRestoreDestinationGenerationOperationRequestV2({
    admission: fixture.admission,
    expectedSession: fixture.options.expectedSession,
    launchIntent: fixture.launchIntent,
  });

  assert.equal(replay.contractVersion, 2);
  assert.deepEqual(replay, fixture.request);
  assert.deepEqual(replay.launchIntent, canonicalPayload(fixture.launchIntent));
  assert.deepEqual(Reflect.ownKeys(replay.launchIntent), [
    "launchAttemptId",
    "measuredImage",
    "supervisor",
  ]);
  assert.equal("imageReservation" in replay.launchIntent, false);
  assertDeepFrozen(replay);

  for (const launchIntent of [
    { ...fixture.launchIntent, imageReservation: "opaque-capability" },
    {
      ...fixture.launchIntent,
      launchAttemptId: fixture.options.operationId,
    },
    {
      ...fixture.launchIntent,
      measuredImage: {
        ...fixture.launchIntent.measuredImage,
        runtimeIdentity: {
          ...fixture.launchIntent.measuredImage.runtimeIdentity,
          platformImageDigest: `sha256:${"f".repeat(64)}`,
        },
      },
    },
  ]) {
    assert.throws(
      () =>
        createRestoreDestinationGenerationOperationRequestV2({
          admission: fixture.admission,
          expectedSession: fixture.options.expectedSession,
          launchIntent,
        }),
      (error) =>
        error instanceof PostgresSessionAuthorityError &&
        error.code === "invalid_operation_request",
    );
  }
});

test("an exact detached restore stable-plan preclaim materializes one V1 generation reservation", async () => {
  const fixture = restoreGenerationFixture();
  const preparedOperation = restoreGenerationOperationRow(
    fixture,
    "prepared",
  );
  const preparedReservation = restoreGenerationReservationRow(
    fixture,
    "prepared",
  );
  const preparedSession = restoreGenerationPhaseSessionRow(
    fixture,
    "prepared",
  );
  const stableClaim = detachedRestoreStablePlanIdClaimRow(fixture);
  const materializedClaim = detachedRestoreStablePlanIdClaimRow(fixture, {
    materializedAt: fixture.preparedAt,
  });
  const { authority, clients } = authorityWithScripts({
    options: { now: fixture.preparedAt },
    steps: [
      rows(fixture.writer.session),
      rows({ operation_count: 0, reservation_count: 0 }),
      rows(fixture.writer.committedOperation),
      rows(fixture.writer.releasedReservation),
      rows(),
      rows(),
      rows(),
      rows(stableClaim),
      rows(materializedClaim),
      rows(preparedOperation),
      rows(preparedReservation),
      rows(preparedSession),
    ],
  });

  const receipt = await authority.reserveOperation(fixture.options);

  assert.equal(receipt.acquired, true);
  assert.equal(receipt.operation.state, "prepared");
  assert.deepEqual(
    authorityQueries(clients[0]).map(queryText).slice(4),
    [
      READ_OPERATION_QUERY,
      INSERT_OPERATION_QUERY,
      `${READ_OPERATION_QUERY} FOR UPDATE`,
      READ_OPERATION_ID_CLAIM_FOR_UPDATE_QUERY,
      MATERIALIZE_DETACHED_RESTORE_STABLE_PLAN_ID_CLAIM_QUERY,
      INSERT_MATERIALIZED_DETACHED_RESTORE_STABLE_PLAN_OPERATION_QUERY,
      INSERT_RESERVATION_QUERY,
      UPDATE_SESSION_QUERY,
    ],
  );
  assert.deepEqual(
    authorityQueries(clients[0]).find(
      (args) =>
        queryText(args) ===
        MATERIALIZE_DETACHED_RESTORE_STABLE_PLAN_ID_CLAIM_QUERY,
    ),
    extendedQuery(MATERIALIZE_DETACHED_RESTORE_STABLE_PLAN_ID_CLAIM_QUERY, [
      fixture.options.operationId,
      fixture.options.expectedSession.sessionId,
      fixture.preparedAt,
      JSON.stringify(canonicalPayload(fixture.request.admission)),
    ]),
  );
  assert.deepEqual(
    authorityQueries(clients[0]).find(
      (args) =>
        queryText(args) ===
        INSERT_MATERIALIZED_DETACHED_RESTORE_STABLE_PLAN_OPERATION_QUERY,
    ),
    extendedQuery(
      INSERT_MATERIALIZED_DETACHED_RESTORE_STABLE_PLAN_OPERATION_QUERY,
      [
        fixture.options.operationId,
        fixture.options.expectedSession.sessionId,
        fixture.options.kind,
        operationBinding(fixture.options).serializedEnvelope,
        fixture.preparedAt,
      ],
    ),
  );
  clients[0].assertExhausted();
});

test("a detached restore stable-plan preclaim cannot adopt V2 or an already materialized ID", async (t) => {
  const scenarios = [
    {
      name: "contract v2",
      fixture: restoreGenerationFixture({
        launchAttemptId: LAUNCH_ATTEMPT_OPERATION_ID,
      }),
      materializedAt: null,
    },
    {
      name: "already materialized",
      fixture: restoreGenerationFixture(),
      materializedAt: RESTORE_PREPARED_NOW,
    },
  ];

  for (const scenario of scenarios) {
    await t.test(scenario.name, async () => {
      const { fixture } = scenario;
      const { authority, clients } = authorityWithScripts({
        options: { now: fixture.preparedAt },
        steps: [
          rows(fixture.writer.session),
          rows({ operation_count: 0, reservation_count: 0 }),
          rows(fixture.writer.committedOperation),
          rows(fixture.writer.releasedReservation),
          rows(),
          rows(),
          rows(),
          rows(
            detachedRestoreStablePlanIdClaimRow(fixture, {
              materializedAt: scenario.materializedAt,
            }),
          ),
        ],
      });

      await assertAuthorityError(
        authority.reserveOperation(fixture.options),
        { code: "operation_identity_conflict" },
      );
      assert.equal(
        queryTexts(clients[0]).includes(
          MATERIALIZE_DETACHED_RESTORE_STABLE_PLAN_ID_CLAIM_QUERY,
        ),
        false,
      );
      assert.equal(
        queryTexts(clients[0]).includes(
          INSERT_MATERIALIZED_DETACHED_RESTORE_STABLE_PLAN_OPERATION_QUERY,
        ),
        false,
      );
      clients[0].assertExhausted();
    });
  }
});

test("a detached restore stable-plan preclaim rejects other operation kinds", async () => {
  const options = reserveOptions();
  const stableClaim = operationIdRegistryRow({
    binding: {
      bindingSha256: "b".repeat(64),
      contractVersion: 1,
      planSha256: "a".repeat(64),
      request: options.request,
    },
    claimType: "detached-restore-stable-plan-v1",
    claimedAt: LATER,
    claimantOperationId: null,
    materializedAt: null,
  });
  const { authority, clients } = authorityWithScripts({
    options: { now: LATER },
    steps: [
      rows(sessionRow()),
      rows({ operation_count: 0, reservation_count: 0 }),
      rows(),
      rows(),
      rows(),
      rows(stableClaim),
    ],
  });

  await assertAuthorityError(authority.reserveOperation(options), {
    code: "operation_identity_conflict",
  });
  assert.equal(
    queryTexts(clients[0]).includes(
      MATERIALIZE_DETACHED_RESTORE_STABLE_PLAN_ID_CLAIM_QUERY,
    ),
    false,
  );
  clients[0].assertExhausted();
});

test("crossed stable-plan admission cannot materialize an operation", async () => {
  const fixture = restoreGenerationFixture();
  const { authority, clients } = authorityWithScripts({
    options: { now: fixture.preparedAt },
    steps: [
      rows(fixture.writer.session),
      rows({ operation_count: 0, reservation_count: 0 }),
      rows(fixture.writer.committedOperation),
      rows(fixture.writer.releasedReservation),
      rows(),
      rows(),
      rows(),
      rows(detachedRestoreStablePlanIdClaimRow(fixture)),
      rows(),
    ],
  });

  await assertAuthorityError(authority.reserveOperation(fixture.options), {
    code: "operation_identity_conflict",
  });
  assert.equal(
    queryTexts(clients[0]).includes(
      MATERIALIZE_DETACHED_RESTORE_STABLE_PLAN_ID_CLAIM_QUERY,
    ),
    true,
  );
  assert.equal(
    queryTexts(clients[0]).includes(
      INSERT_MATERIALIZED_DETACHED_RESTORE_STABLE_PLAN_OPERATION_QUERY,
    ),
    false,
  );
  clients[0].assertExhausted();
});

test("restore generation stable preclaim dispatch binds one rehydrated plan and exact replay", async () => {
  const fixture = stableRestoreGenerationFixture();
  const stablePlan = rehydratePostgresDetachedRestorePlan(
    structuredClone(fixture.stablePlan),
  );
  const startingOperation = restoreGenerationOperationRow(
    fixture,
    "starting",
  );
  const startingReservation = restoreGenerationReservationRow(
    fixture,
    "starting",
  );
  const startingSession = restoreGenerationPhaseSessionRow(
    fixture,
    "starting",
  );
  const { authority, clients } = authorityWithScripts(
    {
      options: {
        authorityNow: RESTORE_AUTHORITY_NOW,
        now: RESTORE_DISPATCH_NOW,
      },
      steps: [
        ...restoreGenerationDispatchReadSteps(fixture, "prepared"),
        ...restoreCheckpointSourceSteps(fixture),
        rows(restoreGenerationRow(fixture)),
        rows(startingOperation),
        rows(startingReservation),
        rows(startingSession),
      ],
    },
    restoreGenerationDispatchReadSteps(fixture, "starting"),
  );
  const input = {
    ...fixture.options,
    destinationIsolationProofId: stablePlan.destinationIsolationProofId,
    expectedOperationRevision: "0",
    generationId: stablePlan.generationId,
    stablePlan,
  };

  const claimed =
    await authority.claimRestoreDestinationGenerationDispatch(input);
  const replayed =
    await authority.claimRestoreDestinationGenerationDispatch(input);

  assert.notStrictEqual(stablePlan, fixture.stablePlan);
  assert.equal(claimed.dispatchGranted, true);
  assert.equal(claimed.generation.generationId, stablePlan.generationId);
  assert.equal(
    claimed.generation.binding.destinationIsolationProofId,
    stablePlan.destinationIsolationProofId,
  );
  assert.equal(replayed.dispatchGranted, false);
  assert.deepEqual(replayed.generation, claimed.generation);
  const firstQueries = queryTexts(clients[0]);
  assert.ok(
    firstQueries.indexOf(READ_OPERATION_ID_CLAIM_FOR_UPDATE_QUERY) <
      firstQueries.indexOf(
        `${READ_CHECKPOINT_CATALOGUE_BY_ID_QUERY} FOR UPDATE`,
      ),
  );
  assert.ok(
    firstQueries.indexOf(READ_OPERATION_ID_CLAIM_FOR_UPDATE_QUERY) <
      firstQueries.indexOf(INSERT_RESTORE_GENERATION_QUERY),
  );
  assert.equal(
    authorityQueries(clients[1]).some((args) =>
      /^(?:INSERT|UPDATE) /u.test(queryText(args)),
    ),
    false,
  );
  for (const client of clients) client.assertExhausted();
});

test("restore generation stable preclaim rejects missing or crossed plan bindings before dispatch", async (t) => {
  const fixture = stableRestoreGenerationFixture();
  const crossedPlan = detachedRestoreStablePlanForFixture(fixture, {
    imagePlanId: "restore-image-plan-crossed",
  });
  const crossedAdmissionClaim = detachedRestoreStablePlanIdClaimRow(fixture);
  crossedAdmissionClaim.materialized_at = new Date(fixture.preparedAt);
  crossedAdmissionClaim.binding.request.holderId = "restore-holder-crossed";
  const scenarios = [
    {
      name: "missing plan",
      input: {},
      state: "prepared",
    },
    {
      name: "crossed plan digest",
      input: { stablePlan: crossedPlan },
      state: "prepared",
    },
    {
      name: "crossed admission request",
      input: { stablePlan: fixture.stablePlan },
      operationIdClaim: crossedAdmissionClaim,
      state: "prepared",
    },
    {
      name: "crossed generation id",
      input: {
        generationId: "restore-generation-crossed",
        stablePlan: fixture.stablePlan,
      },
      state: "prepared",
    },
    {
      name: "crossed destination isolation proof",
      input: {
        destinationIsolationProofId: "destination-proof-crossed",
        stablePlan: fixture.stablePlan,
      },
      state: "prepared",
    },
    {
      name: "replay missing plan",
      input: {},
      state: "starting",
    },
  ];

  for (const scenario of scenarios) {
    await t.test(scenario.name, async () => {
      const { authority, clients } = authorityWithScripts(
        restoreGenerationDispatchReadSteps(fixture, scenario.state, {
          operationIdClaim: scenario.operationIdClaim,
        }),
      );
      await assertAuthorityError(
        authority.claimRestoreDestinationGenerationDispatch({
          ...fixture.options,
          destinationIsolationProofId:
            scenario.input.destinationIsolationProofId ??
            fixture.destinationIsolationProofId,
          expectedOperationRevision: "0",
          generationId:
            scenario.input.generationId ?? fixture.generationId,
          ...(scenario.input.stablePlan === undefined
            ? {}
            : { stablePlan: scenario.input.stablePlan }),
        }),
        { code: "operation_identity_conflict" },
      );
      assert.equal(
        authorityQueries(clients[0]).some((args) =>
          /^(?:INSERT|UPDATE) /u.test(queryText(args)),
        ),
        false,
      );
      assert.equal(
        queryTexts(clients[0]).includes(INSERT_RESTORE_GENERATION_QUERY),
        false,
      );
      clients[0].assertExhausted();
    });
  }
});

test("restore generation direct claims reject an unnecessary stable plan", async (t) => {
  await t.test("direct V1", async () => {
    const fixture = restoreGenerationFixture();
    const stablePlan = detachedRestoreStablePlanForFixture(fixture);
    const { authority, clients } = authorityWithScripts(
      restoreGenerationDispatchReadSteps(fixture, "prepared"),
    );
    await assertAuthorityError(
      authority.claimRestoreDestinationGenerationDispatch({
        ...fixture.options,
        destinationIsolationProofId: fixture.destinationIsolationProofId,
        expectedOperationRevision: "0",
        generationId: fixture.generationId,
        stablePlan,
      }),
      { code: "operation_identity_conflict" },
    );
    assert.equal(
      authorityQueries(clients[0]).some((args) =>
        /^(?:INSERT|UPDATE) /u.test(queryText(args)),
      ),
      false,
    );
    clients[0].assertExhausted();
  });

  await t.test("direct V2", async () => {
    const fixture = restoreGenerationFixture({
      launchAttemptId: LAUNCH_ATTEMPT_OPERATION_ID,
    });
    const stablePlan = detachedRestoreStablePlanForFixture(fixture);
    const { authority, pool } = authorityWithScripts();
    await assertAuthorityError(
      authority.claimRestoreDestinationGenerationDispatch({
        ...fixture.options,
        destinationIsolationProofId: fixture.destinationIsolationProofId,
        expectedOperationRevision: "0",
        generationId: fixture.generationId,
        stablePlan,
      }),
      { code: "invalid_operation_request" },
    );
    assert.equal(pool.connectCalls, 0);
  });
});

test("restore generation V2 fleet gate is independent, zero-write for fresh work, and replay-transparent", async () => {
  const fixture = restoreGenerationFixture({
    launchAttemptId: LAUNCH_ATTEMPT_OPERATION_ID,
  });
  const freshLookupSteps = [
    rows(fixture.writer.session),
    rows({ operation_count: 0, reservation_count: 0 }),
    rows(fixture.writer.committedOperation),
    rows(fixture.writer.releasedReservation),
    rows(),
  ];
  const deniedClient = new ScriptedClient(freshLookupSteps);
  const replayClient = new ScriptedClient(
    restoreGenerationActiveSteps(fixture, "prepared"),
  );
  const deniedStore = new PostgresSerializableStore({
    dedicatedPool: new ScriptedPool([deniedClient, replayClient]),
    maxTransactionAttempts: 1,
  });
  const closedAuthority = new PostgresSessionAuthority({
    restoreAttachmentActivationV2FleetCompatible: true,
    store: deniedStore,
  });

  await assertAuthorityError(
    closedAuthority.reserveOperation(fixture.options),
    { code: "restore_generation_v2_fleet_capability_required" },
  );
  const replay = await closedAuthority.reserveOperation(fixture.options);

  assert.equal(replay.acquired, false);
  assert.equal(replay.operation.state, "prepared");
  assert.equal(
    authorityQueries(deniedClient).some((args) =>
      /^(?:INSERT|UPDATE) /u.test(queryText(args)),
    ),
    false,
  );
  deniedClient.assertExhausted();
  replayClient.assertExhausted();

  const allowedClient = new ScriptedClient(
    [
      ...freshLookupSteps,
      rows(restoreGenerationOperationRow(fixture, "prepared")),
      rows(restoreGenerationReservationRow(fixture, "prepared")),
      rows(restoreGenerationPhaseSessionRow(fixture, "prepared")),
    ],
    { now: RESTORE_PREPARED_NOW },
  );
  const allowedStore = new PostgresSerializableStore({
    dedicatedPool: new ScriptedPool([allowedClient]),
    maxTransactionAttempts: 1,
  });
  const allowedAuthority = new PostgresSessionAuthority({
    restoreGenerationV2FleetCompatible: true,
    store: allowedStore,
  });
  const acquired = await allowedAuthority.reserveOperation(fixture.options);

  assert.equal(acquired.acquired, true);
  assert.equal(acquired.operation.state, "prepared");
  allowedClient.assertExhausted();
});

test("restore generation claim accepts an exact checkpoint from replacement destination storage", async () => {
  const fixture = restoreGenerationFixture({
    destinationStorageId: "volume-restore-002",
  });
  const startingOperation = restoreGenerationOperationRow(
    fixture,
    "starting",
  );
  const startingReservation = restoreGenerationReservationRow(
    fixture,
    "starting",
  );
  const startingSession = restoreGenerationPhaseSessionRow(
    fixture,
    "starting",
  );
  const { authority, clients } = authorityWithScripts({
    options: {
      authorityNow: RESTORE_AUTHORITY_NOW,
      now: RESTORE_DISPATCH_NOW,
    },
    steps: [
      ...restoreGenerationDispatchReadSteps(fixture, "prepared"),
      ...restoreCheckpointSourceSteps(fixture),
      rows(restoreGenerationRow(fixture)),
      rows(startingOperation),
      rows(startingReservation),
      rows(startingSession),
    ],
  });

  const claimed =
    await authority.claimRestoreDestinationGenerationDispatch({
      ...fixture.options,
      destinationIsolationProofId: fixture.destinationIsolationProofId,
      expectedOperationRevision: "0",
      generationId: fixture.generationId,
    });

  assert.notEqual(
    fixture.source.checkpoint.storageId,
    fixture.options.expectedSession.document.storageRef.storageId,
  );
  assert.equal(claimed.dispatchGranted, true);
  assert.equal(
    claimed.generation.binding.checkpoint.storageId,
    fixture.source.checkpoint.storageId,
  );
  assert.equal(
    claimed.generation.binding.request.storageId,
    fixture.options.expectedSession.document.storageRef.storageId,
  );
  assert.deepEqual(
    claimed.generation.binding,
    canonicalPayload(restoreGenerationBinding(fixture)),
  );
  clients[0].assertExhausted();
});

test("restore generation claim rejects non-storage source identity drift before mutation", async (t) => {
  const cases = [
    {
      name: "session incarnation",
      mutate(sourceSession) {
        sourceSession.createdAt = "2026-07-29T12:34:56.790Z";
      },
    },
    {
      name: "immutable manifest",
      mutate(sourceSession) {
        sourceSession.document.manifest.codex.historyMode = "legacy";
      },
    },
    {
      name: "backend capabilities",
      mutate(sourceSession) {
        sourceSession.document.backendCapabilities.fencing = "manual";
      },
    },
  ];

  for (const scenario of cases) {
    await t.test(scenario.name, async () => {
      const fixture = restoreGenerationFixture({
        destinationStorageId: "volume-restore-002",
      });
      scenario.mutate(fixture.source.options.expectedSession);
      const sourceSteps = restoreCheckpointSourceSteps(fixture);
      const { authority, clients } = authorityWithScripts({
        options: {
          authorityNow: RESTORE_AUTHORITY_NOW,
          now: RESTORE_DISPATCH_NOW,
        },
        steps: [
          ...restoreGenerationActiveSteps(fixture, "prepared"),
          ...sourceSteps.slice(0, 1),
        ],
      });

      await assertAuthorityError(
        authority.claimRestoreDestinationGenerationDispatch({
          ...fixture.options,
          destinationIsolationProofId:
            fixture.destinationIsolationProofId,
          expectedOperationRevision: "0",
          generationId: fixture.generationId,
        }),
        { code: "operation_state_invalid" },
      );

      assert.equal(
        authorityQueries(clients[0]).some((args) =>
          /^(?:INSERT|UPDATE) /u.test(queryText(args)),
        ),
        false,
      );
      assert.equal(queryTexts(clients[0]).includes("ROLLBACK"), true);
      clients[0].assertExhausted();
    });
  }
});

test("restore generation dispatch grants once, starting finalization commits once, and exact reads replay", async () => {
  const fixture = restoreGenerationFixture();
  const preparedOperation = restoreGenerationOperationRow(
    fixture,
    "prepared",
  );
  const preparedReservation = restoreGenerationReservationRow(
    fixture,
    "prepared",
  );
  const preparedSession = restoreGenerationPhaseSessionRow(
    fixture,
    "prepared",
  );
  const startingOperation = restoreGenerationOperationRow(
    fixture,
    "starting",
  );
  const startingReservation = restoreGenerationReservationRow(
    fixture,
    "starting",
  );
  const startingSession = restoreGenerationPhaseSessionRow(
    fixture,
    "starting",
  );
  const committedOperation = restoreGenerationOperationRow(
    fixture,
    "committed",
    { revision: "2" },
  );
  const committedReservation = restoreGenerationReservationRow(
    fixture,
    "released",
  );
  const committedSession = restoreGenerationCommittedSessionRow(fixture, {
    operationRevision: "2",
  });
  const conflictingCompletion = {
    ...fixture.completion,
    materialization: {
      ...fixture.completion.materialization,
      treeIdentityDigest: "f".repeat(64),
    },
    replayed: true,
  };
  let insertedBinding;
  const { authority, clients } = authorityWithScripts(
    {
      options: { now: RESTORE_PREPARED_NOW },
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
        authorityNow: RESTORE_AUTHORITY_NOW,
        now: RESTORE_DISPATCH_NOW,
      },
      steps: [
        ...restoreGenerationDispatchReadSteps(fixture, "prepared"),
        ...restoreCheckpointSourceSteps(fixture),
        (args) => {
          assert.equal(queryText(args), INSERT_RESTORE_GENERATION_QUERY);
          insertedBinding = JSON.parse(args[0].values[4]);
          return rows(
            restoreGenerationRow(fixture, "authorized", {
              binding: insertedBinding,
            }),
          );
        },
        rows(startingOperation),
        rows(startingReservation),
        rows(startingSession),
      ],
    },
    restoreGenerationDispatchReadSteps(fixture, "starting"),
    {
      options: { now: RESTORE_FINALIZE_NOW },
      steps: [
        ...restoreGenerationActiveSteps(fixture, "starting"),
        rows(restoreGenerationRow(fixture, "committed")),
        rows(committedOperation),
        rows(committedReservation),
        rows(committedSession),
      ],
    },
    restoreGenerationCommittedSteps(fixture, { operationRevision: "2" }),
    restoreGenerationCommittedSteps(fixture, { operationRevision: "2" }),
    [
      rows(restoreGenerationRow(fixture, "committed")),
      rows(committedOperation),
      ...restoreGenerationCommittedSteps(fixture, {
        operationRevision: "2",
      }),
    ],
  );

  const reserved = await authority.reserveOperation(fixture.options);
  const claimed =
    await authority.claimRestoreDestinationGenerationDispatch({
      ...fixture.options,
      destinationIsolationProofId: fixture.destinationIsolationProofId,
      expectedOperationRevision: "0",
      generationId: fixture.generationId,
    });
  const claimReplay =
    await authority.claimRestoreDestinationGenerationDispatch({
      ...fixture.options,
      destinationIsolationProofId: fixture.destinationIsolationProofId,
      expectedOperationRevision: "0",
      generationId: fixture.generationId,
    });
  const finalized = await authority.finalizeRestoreDestinationGeneration({
    ...fixture.options,
    completion: fixture.completion,
    expectedOperationRevision: "1",
  });
  const replayed = await authority.finalizeRestoreDestinationGeneration({
    ...fixture.options,
    completion: { ...fixture.completion, replayed: true },
    expectedOperationRevision: "1",
  });
  await assertAuthorityError(
    authority.finalizeRestoreDestinationGeneration({
      ...fixture.options,
      completion: conflictingCompletion,
      expectedOperationRevision: "1",
    }),
    { code: "operation_result_conflict" },
  );
  const read = await authority.readRestoreDestinationGeneration({
    checkpoint: fixture.source.checkpoint,
    generationId: fixture.generationId,
    request: fixture.mutationRequest,
  });

  assert.equal(reserved.acquired, true);
  assert.equal(reserved.operation.state, "prepared");
  assert.equal(claimed.dispatchGranted, true);
  assert.equal(claimed.authorityNow, RESTORE_AUTHORITY_NOW);
  assert.equal(claimed.generation.state, "authorized");
  assert.deepEqual(insertedBinding, restoreGenerationBinding(fixture));
  assert.equal(
    claimed.generation.binding.destinationIsolationProofId,
    fixture.destinationIsolationProofId,
  );
  assert.equal(claimReplay.dispatchGranted, false);
  assert.deepEqual(claimReplay.generation, claimed.generation);
  assert.equal(finalized.finalized, true);
  assert.equal(finalized.operation.revision, "2");
  assert.equal(finalized.generation.state, "committed");
  assert.equal(replayed.finalized, false);
  assert.deepEqual(replayed.generation, finalized.generation);
  assert.equal(read.status, "committed");
  assert.deepEqual(read.generation, finalized.generation);
  assertDeepFrozen(finalized);
  assertDeepFrozen(read);

  const claimTexts = authorityQueries(clients[1]).map(queryText);
  const readGenerationForUpdateQuery =
    `${READ_RESTORE_GENERATION_BY_OPERATION_QUERY} FOR UPDATE`;
  assert.equal(
    claimTexts.includes(readGenerationForUpdateQuery),
    true,
  );
  assert.ok(
    claimTexts.indexOf(INSERT_RESTORE_GENERATION_QUERY) <
      claimTexts.indexOf(START_OPERATION_QUERY),
  );
  assert.deepEqual(
    authorityQueries(clients[1]).filter((args) =>
      [readGenerationForUpdateQuery, INSERT_RESTORE_GENERATION_QUERY]
        .includes(queryText(args)),
    ),
    [
      extendedQuery(readGenerationForUpdateQuery, [
        fixture.options.operationId,
      ]),
      extendedQuery(INSERT_RESTORE_GENERATION_QUERY, [
        fixture.generationId,
        fixture.options.operationId,
        fixture.options.expectedSession.sessionId,
        fixture.source.checkpoint.checkpointId,
        JSON.stringify(canonicalPayload(restoreGenerationBinding(fixture))),
        RESTORE_DISPATCH_NOW,
      ]),
    ],
  );
  assert.deepEqual(
    authorityQueries(clients[2]).filter(
      (args) => queryText(args) === readGenerationForUpdateQuery,
    ),
    [
      extendedQuery(readGenerationForUpdateQuery, [
        fixture.options.operationId,
      ]),
    ],
  );
  const finalizeTexts = authorityQueries(clients[3]).map(queryText);
  assert.ok(
    finalizeTexts.indexOf(COMMIT_RESTORE_GENERATION_QUERY) <
      finalizeTexts.indexOf(COMMIT_ACTIVE_OPERATION_QUERY),
  );
  assert.deepEqual(
    authorityQueries(clients[3]).filter((args) =>
      [readGenerationForUpdateQuery, COMMIT_RESTORE_GENERATION_QUERY]
        .includes(queryText(args)),
    ),
    [
      extendedQuery(readGenerationForUpdateQuery, [
        fixture.options.operationId,
      ]),
      extendedQuery(COMMIT_RESTORE_GENERATION_QUERY, [
        fixture.options.operationId,
        JSON.stringify(restoreGenerationDocument(fixture)),
        RESTORE_FINALIZE_NOW,
      ]),
    ],
  );
  assert.equal(
    authorityQueries(clients[2]).some((args) =>
      /^(?:INSERT|UPDATE) /u.test(queryText(args)),
    ),
    false,
  );
  assert.deepEqual(
    authorityQueries(clients[6]).map(queryText).slice(0, 2),
    [READ_RESTORE_GENERATION_BY_ID_QUERY, READ_OPERATION_QUERY],
  );
  assert.deepEqual(
    authorityQueries(clients[6]).filter((args) =>
      [
        READ_RESTORE_GENERATION_BY_ID_QUERY,
        READ_RESTORE_GENERATION_BY_OPERATION_QUERY,
      ].includes(queryText(args)),
    ),
    [
      extendedQuery(READ_RESTORE_GENERATION_BY_ID_QUERY, [
        fixture.generationId,
      ]),
      extendedQuery(READ_RESTORE_GENERATION_BY_OPERATION_QUERY, [
        fixture.options.operationId,
      ]),
    ],
  );
  for (const client of clients) client.assertExhausted();
});

test("restore generation finalization atomically reserves one launch with the real terminal expected session", async () => {
  const fixture = restoreLaunchHandoffFixture();
  const restore = fixture.restore;
  const writeSteps = restoreLaunchHandoffWriteSteps(fixture);
  writeSteps[4] = (args) => {
    assert.equal(queryText(args), INSERT_PRECLAIMED_OPERATION_QUERY);
    const envelope = JSON.parse(args[0].values[3]);
    assert.deepEqual(
      envelope.expectedSession,
      snapshotFromSessionRow(
        restoreGenerationCommittedSessionRow(restore, {
          operationRevision: "2",
        }),
      ),
    );
    assert.equal(
      JSON.stringify(envelope.payload),
      JSON.stringify(canonicalPayload(fixture.request)),
    );
    return rows(
      writerLaunchOperationRow(fixture, "prepared", {
        createdAt: RESTORE_FINALIZE_NOW,
        updatedAt: RESTORE_FINALIZE_NOW,
      }),
    );
  };
  const { authority, clients } = authorityWithScripts({
    options: { now: RESTORE_FINALIZE_NOW },
    steps: [
      ...restoreLaunchHandoffRestoreSteps(restore, "starting"),
      ...writeSteps,
    ],
  });

  const receipt =
    await authority.finalizeRestoreDestinationGenerationAndReserveWriterLaunchAttempt(
      {
        launch: restore.launchIntent,
        restore: {
          ...restore.options,
          completion: restore.completion,
          expectedOperationRevision: "1",
        },
      },
    );

  const startingSession = snapshotFromSessionRow(
    restoreGenerationPhaseSessionRow(restore, "starting"),
  );
  const terminalSession = snapshotFromSessionRow(
    restoreGenerationCommittedSessionRow(restore, {
      operationRevision: "2",
    }),
  );
  assert.equal(receipt.status, "prepared");
  assert.equal(receipt.restore.finalized, true);
  assert.equal(receipt.restore.operation.state, "committed");
  assert.equal(receipt.launch.operation.state, "prepared");
  assert.equal(
    receipt.launch.attempt.launchAttemptId,
    restore.launchIntent.launchAttemptId,
  );
  assert.deepEqual(receipt.launch.attempt.request, fixture.request);
  assert.deepEqual(receipt.launch.operation.expectedSession, terminalSession);
  assert.equal(
    terminalSession.revision,
    (BigInt(startingSession.revision) + 1n).toString(),
  );
  assert.equal(
    receipt.session.revision,
    (BigInt(startingSession.revision) + 2n).toString(),
  );
  assert.equal(
    receipt.session.document.activeOperation.operationId,
    restore.launchIntent.launchAttemptId,
  );
  assert.equal(
    receipt.session.document.lastOperation.operationId,
    restore.options.operationId,
  );
  assert.equal(
    receipt.launch.attempt.request.generation.bindingSha256,
    canonicalSha256(receipt.generation.binding),
  );
  assert.equal(
    receipt.launch.attempt.request.generation.documentSha256,
    canonicalSha256(receipt.generation.document),
  );
  assertDeepFrozen(receipt);

  const mutationOrder = authorityQueries(clients[0])
    .map(queryText)
    .filter((text) => /^(?:INSERT|UPDATE) /u.test(text));
  assert.deepEqual(mutationOrder, [
    COMMIT_RESTORE_GENERATION_QUERY,
    COMMIT_ACTIVE_OPERATION_QUERY,
    RELEASE_ACTIVE_RESERVATION_QUERY,
    UPDATE_SESSION_QUERY,
    INSERT_PRECLAIMED_OPERATION_QUERY,
    INSERT_RESERVATION_QUERY,
    MATERIALIZE_RESTORE_LAUNCH_ID_CLAIM_QUERY,
    UPDATE_SESSION_QUERY,
  ]);
  assert.deepEqual(
    authorityQueries(clients[0]).find(
      (args) => queryText(args) === READ_OPERATION_ID_CLAIM_FOR_UPDATE_QUERY,
    ),
    extendedQuery(READ_OPERATION_ID_CLAIM_FOR_UPDATE_QUERY, [
      restore.launchIntent.launchAttemptId,
    ]),
  );
  assert.deepEqual(
    authorityQueries(clients[0]).find(
      (args) => queryText(args) === MATERIALIZE_RESTORE_LAUNCH_ID_CLAIM_QUERY,
    ),
    extendedQuery(MATERIALIZE_RESTORE_LAUNCH_ID_CLAIM_QUERY, [
      restore.launchIntent.launchAttemptId,
      restore.options.expectedSession.sessionId,
      RESTORE_FINALIZE_NOW,
      restore.options.operationId,
      JSON.stringify(canonicalPayload(restore.launchIntent)),
    ]),
  );
  assert.equal(
    queryTexts(clients[0]).filter((text) => text === "COMMIT").length,
    1,
  );
  clients[0].assertExhausted();
});

test("restore-to-launch handoff rejects missing or altered reserved launch claims before writes", async (t) => {
  const scenarios = [
    { claim: null, name: "missing" },
    {
      claim: restoreLaunchIdClaimRow(restoreLaunchHandoffFixture(), {
        claimantOperationId: OTHER_OPERATION_ID,
      }),
      name: "different claimant",
    },
    {
      claim: restoreLaunchIdClaimRow(restoreLaunchHandoffFixture(), {
        binding: {
          ...restoreLaunchHandoffFixture().restore.launchIntent,
          supervisor: {
            contractVersion: 1,
            supervisorId: "supervisor-tampered",
          },
        },
      }),
      name: "altered binding",
    },
    {
      claim: restoreLaunchIdClaimRow(restoreLaunchHandoffFixture(), {
        materializedAt: RESTORE_FINALIZE_NOW,
      }),
      name: "premature materialization",
    },
  ];

  for (const scenario of scenarios) {
    await t.test(scenario.name, async () => {
      const fixture = restoreLaunchHandoffFixture();
      const restore = fixture.restore;
      const validationSteps = restoreGenerationActiveSteps(
        restore,
        "starting",
        { launchIdClaim: scenario.claim },
      ).slice(0, -2);
      const { authority, clients } = authorityWithScripts(validationSteps);

      await assertAuthorityError(
        authority.finalizeRestoreDestinationGenerationAndReserveWriterLaunchAttempt(
          {
            launch: restore.launchIntent,
            restore: {
              ...restore.options,
              completion: restore.completion,
              expectedOperationRevision: "1",
            },
          },
        ),
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
  }
});

test("restore-to-launch replay rejects missing or altered materialized launch claims", async (t) => {
  const fixture = restoreLaunchHandoffFixture();
  const claims = [
    { claim: null, name: "missing" },
    {
      claim: restoreLaunchIdClaimRow(fixture, {
        binding: {
          ...fixture.restore.launchIntent,
          launchAttemptId: "writer-launch-attempt-tampered",
        },
        materializedAt: RESTORE_FINALIZE_NOW,
      }),
      name: "altered binding",
    },
    {
      claim: restoreLaunchIdClaimRow(fixture),
      name: "not materialized",
    },
  ];

  for (const scenario of claims) {
    await t.test(scenario.name, async () => {
      const { authority, clients } = authorityWithScripts(
        restoreLaunchHandoffActiveSteps(fixture, {
          launchIdClaim: scenario.claim,
        }),
      );

      await assertAuthorityError(
        authority.finalizeRestoreDestinationGenerationAndReserveWriterLaunchAttempt(
          {
            launch: fixture.restore.launchIntent,
            restore: {
              ...fixture.restore.options,
              completion: fixture.restore.completion,
              expectedOperationRevision: "1",
            },
          },
        ),
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
  }
});

test("restore-to-launch handoff rolls every generation and launch write back together", async () => {
  const fixture = restoreLaunchHandoffFixture();
  const restore = fixture.restore;
  const { authority, clients } = authorityWithScripts({
    options: { now: RESTORE_FINALIZE_NOW },
    steps: [
      ...restoreLaunchHandoffRestoreSteps(restore, "starting"),
      ...restoreLaunchHandoffWriteSteps(fixture, {
        finalSession: rows(),
      }),
    ],
  });

  await assertAuthorityError(
    authority.finalizeRestoreDestinationGenerationAndReserveWriterLaunchAttempt(
      {
        launch: restore.launchIntent,
        restore: {
          ...restore.options,
          completion: restore.completion,
          expectedOperationRevision: "1",
        },
      },
    ),
    { code: "session_revision_conflict" },
  );

  const texts = queryTexts(clients[0]);
  assert.equal(texts.includes(INSERT_PRECLAIMED_OPERATION_QUERY), true);
  assert.equal(texts.includes(INSERT_RESERVATION_QUERY), true);
  assert.equal(texts.includes("COMMIT"), false);
  assert.equal(texts.includes("ROLLBACK"), true);
  clients[0].assertExhausted();
});

test("restore-to-launch handoff acknowledgement loss replays the same prepared attempt without writes", async () => {
  const fixture = restoreLaunchHandoffFixture();
  const restore = fixture.restore;
  const committedSteps = restoreLaunchHandoffActiveSteps(fixture);
  const { authority, clients } = authorityWithScripts(
    {
      options: {
        commitError: new Error("restore launch handoff acknowledgement lost"),
        now: RESTORE_FINALIZE_NOW,
      },
      steps: [
        ...restoreLaunchHandoffRestoreSteps(restore, "starting"),
        ...restoreLaunchHandoffWriteSteps(fixture),
      ],
    },
    [
      ...committedSteps,
      ...committedSteps.slice(1),
    ],
  );
  const input = {
    launch: restore.launchIntent,
    restore: {
      ...restore.options,
      completion: restore.completion,
      expectedOperationRevision: "1",
    },
  };

  await assert.rejects(
    authority.finalizeRestoreDestinationGenerationAndReserveWriterLaunchAttempt(
      input,
    ),
    assertStoreCommitUncertain,
  );
  const replay =
    await authority.finalizeRestoreDestinationGenerationAndReserveWriterLaunchAttempt(
      input,
    );

  assert.equal(replay.restore.finalized, false);
  assert.equal(replay.status, "prepared");
  assert.equal(
    replay.launch.operation.operationId,
    restore.launchIntent.launchAttemptId,
  );
  assert.deepEqual(replay.launch.operation.request, fixture.request);
  assert.equal(
    authorityQueries(clients[1]).some((args) =>
      /^(?:INSERT|UPDATE) /u.test(queryText(args)),
    ),
    false,
  );
  assert.equal(queryTexts(clients[0]).at(-1), "ROLLBACK");
  clients[0].assertExhausted({ destroyed: true });
  clients[1].assertExhausted();
});

test("restore-to-launch prepared cancellation requires exact launcher non-dispatch evidence after expiry", async (t) => {
  assert.equal(
    WRITER_LAUNCH_PRE_DISPATCH_CANCELLATION_REASON,
    "launch-dispatch-not-started",
  );

  await t.test("wrong reason", async () => {
    const fixture = restoreLaunchHandoffFixture();
    const expiresAt = fixture.request.lease.expiresAt;
    const transactionNow = new Date(
      Date.parse(expiresAt) - 1,
    ).toISOString();
    const { authority, clients } = authorityWithScripts({
      options: { authorityNow: expiresAt, now: transactionNow },
      steps: restoreLaunchHandoffActiveSteps(fixture),
    });

    await assertAuthorityError(
      authority.cancelPreparedOperation({
        ...fixture.options,
        expectedOperationRevision: "0",
        reason: "caller-abandoned-before-launch-dispatch",
      }),
      { code: "operation_transition_conflict" },
    );

    assert.equal(fixture.restore.request.contractVersion, 2);
    assert.equal(
      queryTexts(clients[0]).includes(READ_AUTHORITY_CLOCK_QUERY),
      false,
    );
    assert.equal(
      authorityQueries(clients[0]).some((args) =>
        /^(?:INSERT|UPDATE|DELETE) /u.test(queryText(args)),
      ),
      false,
    );
    assert.equal(queryTexts(clients[0]).includes("ROLLBACK"), true);
    clients[0].assertExhausted();
  });

  await t.test("before lease expiry", async () => {
    const fixture = restoreLaunchHandoffFixture();
    const expiresAt = fixture.request.lease.expiresAt;
    const authorityNow = new Date(
      Date.parse(expiresAt) - 1,
    ).toISOString();
    const { authority, clients } = authorityWithScripts({
      options: { authorityNow, now: authorityNow },
      steps: restoreLaunchHandoffActiveSteps(fixture),
    });

    await assertAuthorityError(
      authority.cancelPreparedOperation({
        ...fixture.options,
        expectedOperationRevision: "0",
        reason: WRITER_LAUNCH_PRE_DISPATCH_CANCELLATION_REASON,
      }),
      { code: "operation_transition_conflict" },
    );

    const { texts } =
      assertAtomicHandoffValidatedBeforeAuthorityClock(clients[0]);
    assert.equal(
      authorityQueries(clients[0]).some((args) =>
        /^(?:INSERT|UPDATE|DELETE) /u.test(queryText(args)),
      ),
      false,
    );
    assert.equal(texts.includes("ROLLBACK"), true);
    clients[0].assertExhausted();
  });
});

test("expired restore-to-launch handoff cancellation commits at and after the lease boundary", async (t) => {
  for (const offsetMilliseconds of [0, 1]) {
    await t.test(
      offsetMilliseconds === 0 ? "at expiry" : "after expiry",
      async () => {
        const fixture = restoreLaunchHandoffFixture();
        const expiresAt = fixture.request.lease.expiresAt;
        const transactionNow = new Date(
          Date.parse(expiresAt) - 1,
        ).toISOString();
        const authorityNow = new Date(
          Date.parse(expiresAt) + offsetMilliseconds,
        ).toISOString();
        assert.ok(Date.parse(transactionNow) < Date.parse(expiresAt));
        assert.ok(Date.parse(expiresAt) <= Date.parse(authorityNow));
        const cancelled = writerLaunchCancelledFixture(fixture, {
          createdAt: RESTORE_FINALIZE_NOW,
          reason: WRITER_LAUNCH_PRE_DISPATCH_CANCELLATION_REASON,
          updatedAt: authorityNow,
        });
        const { authority, clients } = authorityWithScripts({
          options: { authorityNow, now: transactionNow },
          steps: [
            ...restoreLaunchHandoffActiveSteps(fixture),
            rows(cancelled.operation),
            rows(cancelled.reservation),
            rows(cancelled.session),
          ],
        });

        const receipt = await authority.cancelPreparedOperation({
          ...fixture.options,
          expectedOperationRevision: "0",
          reason: WRITER_LAUNCH_PRE_DISPATCH_CANCELLATION_REASON,
        });

        assert.deepEqual(receipt, {
          cancelled: true,
          operation: operationView(cancelled.operation),
          reservation: reservationView(cancelled.reservation),
          session: snapshotFromSessionRow(cancelled.session),
          status: "committed",
        });
        assert.equal(
          receipt.operation.result.outcome,
          "cancelled-before-dispatch",
        );
        assert.equal(
          receipt.operation.result.reason,
          "launch-dispatch-not-started",
        );
        assert.equal(Object.hasOwn(receipt, "authorityNow"), false);
        assert.equal(receipt.reservation.state, "released");
        assert.equal(receipt.session.document.activeOperation, null);
        assert.equal(
          receipt.session.document.lastOperation.operationId,
          fixture.options.operationId,
        );
        assertDeepFrozen(receipt);

        const { authorityClockIndex, texts } =
          assertAtomicHandoffValidatedBeforeAuthorityClock(clients[0]);
        const firstMutationIndex = texts.findIndex((text) =>
          /^(?:INSERT|UPDATE|DELETE) /u.test(text),
        );
        assert.ok(firstMutationIndex > authorityClockIndex);
        assert.equal(
          authorityQueries(clients[0]).some((args) =>
            /^(?:INSERT|UPDATE|DELETE) /u.test(queryText(args)) &&
            queryText(args).includes(
              "session_authority.operation_id_registry",
            ),
          ),
          false,
        );
        assert.deepEqual(
          authorityQueries(clients[0]).filter((args) =>
            /^(?:INSERT|UPDATE|DELETE) /u.test(queryText(args)),
          ),
          [
            extendedQuery(CANCEL_OPERATION_QUERY, [
              fixture.options.operationId,
              "0",
              JSON.stringify(cancelled.result),
              authorityNow,
            ]),
            extendedQuery(RELEASE_RESERVATION_QUERY, [
              fixture.options.operationId,
              authorityNow,
            ]),
            extendedQuery(UPDATE_SESSION_QUERY, [
              fixture.options.expectedSession.sessionId,
              (
                BigInt(fixture.options.expectedSession.revision) + 1n
              ).toString(),
              JSON.stringify(cancelled.session.document),
              authorityNow,
            ]),
          ],
        );
        clients[0].assertExhausted();
      },
    );
  }
});

test("V1 writer launch prepared cancellation keeps the ordinary query shape", async () => {
  const fixture = writerLaunchFixture();
  const reason = "caller-abandoned-before-launch-dispatch";
  const cancelled = writerLaunchCancelledFixture(fixture, { reason });
  const { authority, clients } = authorityWithScripts({
    options: { now: LAUNCH_FINALIZE_NOW },
    steps: [
      ...writerLaunchActiveSteps(fixture, "prepared"),
      rows(cancelled.operation),
      rows(cancelled.reservation),
      rows(cancelled.session),
    ],
  });

  const receipt = await authority.cancelPreparedOperation({
    ...fixture.options,
    expectedOperationRevision: "0",
    reason,
  });

  assert.equal(fixture.restore.request.contractVersion, 1);
  assert.equal(receipt.cancelled, true);
  assert.equal(Object.hasOwn(receipt, "authorityNow"), false);
  assert.equal(
    queryTexts(clients[0]).includes(READ_AUTHORITY_CLOCK_QUERY),
    false,
  );
  assert.deepEqual(
    authorityQueries(clients[0]).filter((args) =>
      /^(?:INSERT|UPDATE|DELETE) /u.test(queryText(args)),
    ),
    [
      extendedQuery(CANCEL_OPERATION_QUERY, [
        fixture.options.operationId,
        "0",
        JSON.stringify(cancelled.result),
        LAUNCH_FINALIZE_NOW,
      ]),
      extendedQuery(RELEASE_RESERVATION_QUERY, [
        fixture.options.operationId,
        LAUNCH_FINALIZE_NOW,
      ]),
      extendedQuery(UPDATE_SESSION_QUERY, [
        fixture.options.expectedSession.sessionId,
        (
          BigInt(fixture.options.expectedSession.revision) + 1n
        ).toString(),
        JSON.stringify(cancelled.session.document),
        LAUNCH_FINALIZE_NOW,
      ]),
    ],
  );
  clients[0].assertExhausted();
});

test("restore-to-launch V2 candidate with non-atomic timestamps fails closed before cancellation", async () => {
  const fixture = restoreLaunchHandoffFixture();
  const committedSteps = restoreLaunchHandoffActiveSteps(fixture, {
    launchIdClaim: restoreLaunchIdClaimRow(fixture, {
      materializedAt: RESTORE_FINALIZE_NOW,
    }),
  });
  const { authority, clients } = authorityWithScripts([
    rows(
      writerLaunchPhaseSessionRow(fixture, "prepared", {
        updatedAt: LAUNCH_PREPARED_NOW,
      }),
    ),
    rows(writerLaunchOperationRow(fixture, "prepared")),
    rows(writerLaunchReservationRow(fixture, "prepared")),
    ...committedSteps.slice(3),
  ]);

  await assertAuthorityError(
    authority.cancelPreparedOperation({
      ...fixture.options,
      expectedOperationRevision: "0",
      reason: "caller-abandoned-before-launch-dispatch",
    }),
    { code: "operation_state_invalid" },
  );

  assert.equal(fixture.restore.request.contractVersion, 2);
  assert.equal(
    authorityQueries(clients[0]).some((args) =>
      /^(?:INSERT|UPDATE) /u.test(queryText(args)),
    ),
    false,
  );
  assert.equal(queryTexts(clients[0]).includes("ROLLBACK"), true);
  clients[0].assertExhausted();
});

test("restore-to-launch committed replay rejects completion drift without touching the prepared launch", async () => {
  const fixture = restoreLaunchHandoffFixture();
  const restore = fixture.restore;
  const conflictingCompletion = {
    ...restore.completion,
    materialization: {
      ...restore.completion.materialization,
      treeIdentityDigest: "f".repeat(64),
    },
    replayed: true,
  };
  const { authority, clients } = authorityWithScripts(
    restoreLaunchHandoffActiveSteps(fixture),
  );

  await assertAuthorityError(
    authority.finalizeRestoreDestinationGenerationAndReserveWriterLaunchAttempt(
      {
        launch: restore.launchIntent,
        restore: {
          ...restore.options,
          completion: conflictingCompletion,
          expectedOperationRevision: "1",
        },
      },
    ),
    { code: "operation_result_conflict" },
  );

  assert.equal(
    authorityQueries(clients[0]).some((args) =>
      /^(?:INSERT|UPDATE) /u.test(queryText(args)),
    ),
    false,
  );
  clients[0].assertExhausted();
});

test("restore-to-launch replay returns its historically cancelled prepared successor", async () => {
  const fixture = restoreLaunchHandoffFixture();
  const restore = fixture.restore;
  const cancelled = writerLaunchCancelledFixture(fixture, {
    createdAt: RESTORE_FINALIZE_NOW,
    reason: WRITER_LAUNCH_PRE_DISPATCH_CANCELLATION_REASON,
    updatedAt: fixture.request.lease.expiresAt,
  });
  const terminalLaunchSteps = [
    rows({ operation_count: 0, reservation_count: 0 }),
    rows(cancelled.operation),
    rows(cancelled.reservation),
  ];
  const { authority, clients } = authorityWithScripts([
    rows(cancelled.session),
    ...terminalLaunchSteps,
    rows(
      restoreGenerationOperationRow(restore, "committed", {
        revision: "2",
      }),
    ),
    rows(restoreGenerationReservationRow(restore, "released")),
    rows(restoreGenerationRow(restore, "committed")),
    ...restoreCheckpointSourceSteps(restore),
    rows(
      restoreLaunchIdClaimRow(fixture, {
        materializedAt: RESTORE_FINALIZE_NOW,
      }),
    ),
    ...terminalLaunchSteps,
  ]);

  const replay =
    await authority.finalizeRestoreDestinationGenerationAndReserveWriterLaunchAttempt(
      {
        launch: restore.launchIntent,
        restore: {
          ...restore.options,
          completion: restore.completion,
          expectedOperationRevision: "1",
        },
      },
    );

  assert.equal(replay.status, "committed");
  assert.equal(replay.restore.finalized, false);
  assert.deepEqual(replay.launch.operation, operationView(cancelled.operation));
  assert.deepEqual(
    replay.launch.reservation,
    reservationView(cancelled.reservation),
  );
  assert.deepEqual(replay.session, snapshotFromSessionRow(cancelled.session));
  assert.equal(
    replay.launch.operation.result.reason,
    "launch-dispatch-not-started",
  );
  assertDeepFrozen(replay);
  assert.equal(
    authorityQueries(clients[0]).some((args) =>
      /^(?:INSERT|UPDATE|DELETE) /u.test(queryText(args)),
    ),
    false,
  );
  assert.equal(
    queryTexts(clients[0]).includes(READ_AUTHORITY_CLOCK_QUERY),
    false,
  );
  clients[0].assertExhausted();
});

test("restore-to-launch handoff never backfills a separately committed generation", async () => {
  const fixture = restoreLaunchHandoffFixture();
  const restore = fixture.restore;
  const committedSteps = restoreGenerationCommittedSteps(restore, {
    operationRevision: "2",
  });
  const { authority, clients } = authorityWithScripts([
    ...committedSteps,
    ...committedSteps.slice(1),
    rows(),
  ]);

  await assertAuthorityError(
    authority.finalizeRestoreDestinationGenerationAndReserveWriterLaunchAttempt(
      {
        launch: restore.launchIntent,
        restore: {
          ...restore.options,
          completion: restore.completion,
          expectedOperationRevision: "1",
        },
      },
    ),
    { code: "operation_transition_conflict" },
  );

  assert.equal(
    authorityQueries(clients[0]).some((args) =>
      /^(?:INSERT|UPDATE) /u.test(queryText(args)),
    ),
    false,
  );
  clients[0].assertExhausted();
});

test("restore generation V1 claim keeps the exact three-revision boundary", async (t) => {
  await t.test("bigint max-3 reaches terminal through uncertainty", async () => {
    const preparedRevision = MAX_POSTGRES_BIGINT - 3n;
    const fixture = restoreGenerationFixture({
      expectedSessionRevision: (preparedRevision - 1n).toString(),
    });
    const startingOperation = restoreGenerationOperationRow(
      fixture,
      "starting",
    );
    const startingReservation = restoreGenerationReservationRow(
      fixture,
      "starting",
    );
    const startingSession = restoreGenerationPhaseSessionRow(
      fixture,
      "starting",
    );
    const uncertainOperation = restoreGenerationOperationRow(
      fixture,
      "uncertain",
    );
    const uncertainReservation = restoreGenerationReservationRow(
      fixture,
      "uncertain",
    );
    const uncertainSession = restoreGenerationPhaseSessionRow(
      fixture,
      "uncertain",
    );
    const committedOperation = restoreGenerationOperationRow(
      fixture,
      "committed",
    );
    const committedReservation = restoreGenerationReservationRow(
      fixture,
      "released",
    );
    const committedSession = restoreGenerationCommittedSessionRow(fixture);
    const { authority, clients } = authorityWithScripts(
      {
        options: {
          authorityNow: RESTORE_AUTHORITY_NOW,
          now: RESTORE_DISPATCH_NOW,
      },
      steps: [
          ...restoreGenerationDispatchReadSteps(fixture, "prepared"),
          ...restoreCheckpointSourceSteps(fixture),
          rows(restoreGenerationRow(fixture)),
          rows(startingOperation),
          rows(startingReservation),
          rows(startingSession),
        ],
      },
      {
        options: { now: RESTORE_UNCERTAIN_NOW },
        steps: [
          ...restoreGenerationActiveSteps(fixture, "starting"),
          rows(uncertainOperation),
          rows(uncertainReservation),
          rows(uncertainSession),
        ],
      },
      {
        options: { now: RESTORE_FINALIZE_NOW },
        steps: [
          ...restoreGenerationActiveSteps(fixture, "uncertain"),
          rows(restoreGenerationRow(fixture, "committed")),
          rows(committedOperation),
          rows(committedReservation),
          rows(committedSession),
        ],
      },
    );

    const claimed =
      await authority.claimRestoreDestinationGenerationDispatch({
        ...fixture.options,
        destinationIsolationProofId: fixture.destinationIsolationProofId,
        expectedOperationRevision: "0",
        generationId: fixture.generationId,
      });
    const uncertain = await authority.markOperationUncertain({
      ...fixture.options,
      expectedOperationRevision: "1",
    });
    const finalized = await authority.finalizeRestoreDestinationGeneration({
      ...fixture.options,
      completion: fixture.completion,
      expectedOperationRevision: "2",
    });

    assert.equal(fixture.request.contractVersion, 1);
    assert.equal(
      restoreGenerationPhaseSessionRow(fixture, "prepared").revision,
      preparedRevision.toString(),
    );
    assert.equal(claimed.dispatchGranted, true);
    assert.equal(
      claimed.session.revision,
      (preparedRevision + 1n).toString(),
    );
    assert.equal(uncertain.changed, true);
    assert.equal(
      uncertain.session.revision,
      (preparedRevision + 2n).toString(),
    );
    assert.equal(finalized.finalized, true);
    assert.equal(
      finalized.session.revision,
      MAX_POSTGRES_BIGINT.toString(),
    );
    for (const client of clients) client.assertExhausted();
  });

  await t.test("bigint max-2 rejects before authority publication", async () => {
    const preparedRevision = MAX_POSTGRES_BIGINT - 2n;
    const fixture = restoreGenerationFixture({
      expectedSessionRevision: (preparedRevision - 1n).toString(),
    });
    const { authority, clients } = authorityWithScripts(
      restoreGenerationDispatchReadSteps(fixture, "prepared"),
    );

    await assertAuthorityError(
      authority.claimRestoreDestinationGenerationDispatch({
        ...fixture.options,
        destinationIsolationProofId: fixture.destinationIsolationProofId,
        expectedOperationRevision: "0",
        generationId: fixture.generationId,
      }),
      { code: "session_revision_exhausted" },
    );

    assert.equal(fixture.request.contractVersion, 1);
    assert.equal(
      restoreGenerationPhaseSessionRow(fixture, "prepared").revision,
      preparedRevision.toString(),
    );
    assert.equal(
      queryTexts(clients[0]).includes(READ_AUTHORITY_CLOCK_QUERY),
      false,
    );
    assert.equal(
      authorityQueries(clients[0]).some((args) =>
        /^(?:INSERT|UPDATE) /u.test(queryText(args)),
      ),
      false,
    );
    clients[0].assertExhausted();
  });
});

test("restore generation V2 claim rejects bigint max-6 before external publication", async () => {
  const preparedRevision = MAX_POSTGRES_BIGINT - 6n;
  const fixture = restoreLaunchHandoffFixture({
    expectedSessionRevision: (preparedRevision - 1n).toString(),
  });
  const restore = fixture.restore;
  const { authority, clients } = authorityWithScripts(
    restoreGenerationDispatchReadSteps(restore, "prepared"),
  );

  await assertAuthorityError(
    authority.claimRestoreDestinationGenerationDispatch({
      ...restore.options,
      destinationIsolationProofId: restore.destinationIsolationProofId,
      expectedOperationRevision: "0",
      generationId: restore.generationId,
    }),
    { code: "session_revision_exhausted" },
  );

  assert.equal(restore.request.contractVersion, 2);
  assert.equal(
    restoreGenerationPhaseSessionRow(restore, "prepared").revision,
    preparedRevision.toString(),
  );
  assert.equal(
    queryTexts(clients[0]).includes(READ_AUTHORITY_CLOCK_QUERY),
    false,
  );
  assert.equal(
    authorityQueries(clients[0]).some((args) =>
      /^(?:INSERT|UPDATE) /u.test(queryText(args)),
    ),
    false,
  );
  clients[0].assertExhausted();
});

test("restore generation V2 rejects a pre-existing global launch ID before publication", async () => {
  const fixture = restoreGenerationFixture({
    launchAttemptId: LAUNCH_ATTEMPT_OPERATION_ID,
  });
  const { authority, clients } = authorityWithScripts({
    options: {
      authorityNow: RESTORE_AUTHORITY_NOW,
      now: RESTORE_DISPATCH_NOW,
    },
    steps: [
      ...restoreGenerationDispatchReadSteps(fixture, "prepared"),
      ...restoreCheckpointSourceSteps(fixture),
      rows(),
    ],
  });

  await assertAuthorityError(
    authority.claimRestoreDestinationGenerationDispatch({
      ...fixture.options,
      destinationIsolationProofId: fixture.destinationIsolationProofId,
      expectedOperationRevision: "0",
      generationId: fixture.generationId,
    }),
    { code: "operation_identity_conflict" },
  );

  const texts = queryTexts(clients[0]);
  assert.equal(texts.includes(INSERT_RESTORE_LAUNCH_ID_CLAIM_QUERY), true);
  assert.equal(texts.includes(READ_AUTHORITY_CLOCK_QUERY), false);
  assert.equal(texts.includes(INSERT_RESTORE_GENERATION_QUERY), false);
  assert.equal(texts.includes(START_OPERATION_QUERY), false);
  assert.equal(texts.includes(UPDATE_SESSION_QUERY), false);
  clients[0].assertExhausted();
});

test("restore generation V2 launch ID preclaim survives acknowledgement loss and replays without writes", async () => {
  const fixture = restoreGenerationFixture({
    launchAttemptId: LAUNCH_ATTEMPT_OPERATION_ID,
  });
  const startingOperation = restoreGenerationOperationRow(
    fixture,
    "starting",
  );
  const startingReservation = restoreGenerationReservationRow(
    fixture,
    "starting",
  );
  const startingSession = restoreGenerationPhaseSessionRow(
    fixture,
    "starting",
  );
  const { authority, clients } = authorityWithScripts(
    {
      options: {
        authorityNow: RESTORE_AUTHORITY_NOW,
        commitError: new Error("restore claim acknowledgement lost"),
        now: RESTORE_DISPATCH_NOW,
      },
      steps: [
        ...restoreGenerationDispatchReadSteps(fixture, "prepared"),
        ...restoreCheckpointSourceSteps(fixture),
        rows(restoreLaunchIdClaimRow(fixture)),
        rows(restoreGenerationRow(fixture)),
        rows(startingOperation),
        rows(startingReservation),
        rows(startingSession),
      ],
    },
    restoreGenerationDispatchReadSteps(fixture, "starting"),
  );
  const input = {
    ...fixture.options,
    destinationIsolationProofId: fixture.destinationIsolationProofId,
    expectedOperationRevision: "0",
    generationId: fixture.generationId,
  };

  await assert.rejects(
    authority.claimRestoreDestinationGenerationDispatch(input),
    assertStoreCommitUncertain,
  );
  const replay =
    await authority.claimRestoreDestinationGenerationDispatch(input);

  assert.equal(replay.dispatchGranted, false);
  assert.equal(replay.generation.state, "authorized");
  assert.equal(
    authorityQueries(clients[1]).some((args) =>
      /^(?:INSERT|UPDATE) /u.test(queryText(args)),
    ),
    false,
  );
  const firstTexts = queryTexts(clients[0]);
  assert.ok(
    firstTexts.indexOf(INSERT_RESTORE_LAUNCH_ID_CLAIM_QUERY) <
      firstTexts.indexOf(READ_AUTHORITY_CLOCK_QUERY),
  );
  assert.ok(
    firstTexts.indexOf(INSERT_RESTORE_LAUNCH_ID_CLAIM_QUERY) <
      firstTexts.indexOf(INSERT_RESTORE_GENERATION_QUERY),
  );
  assert.deepEqual(
    authorityQueries(clients[0]).find(
      (args) => queryText(args) === INSERT_RESTORE_LAUNCH_ID_CLAIM_QUERY,
    ),
    extendedQuery(INSERT_RESTORE_LAUNCH_ID_CLAIM_QUERY, [
      LAUNCH_ATTEMPT_OPERATION_ID,
      SESSION_ID,
      RESTORE_OPERATION_ID,
      JSON.stringify(canonicalPayload(fixture.launchIntent)),
      RESTORE_DISPATCH_NOW,
    ]),
  );
  clients[0].assertExhausted({ destroyed: true });
  clients[1].assertExhausted();
});

test("restore generation V2 claim at bigint max-7 covers both handoff routes", async (t) => {
  const routes = [
    {
      name: "starting handoff",
      restoreOperationRevision: "2",
      restoreUncertain: false,
    },
    {
      name: "restore-uncertain handoff",
      restoreOperationRevision: "3",
      restoreUncertain: true,
    },
  ];

  for (const route of routes) {
    await t.test(route.name, async () => {
      const preparedRevision = MAX_POSTGRES_BIGINT - 7n;
      const fixture = restoreLaunchHandoffFixture({
        expectedSessionRevision: (preparedRevision - 1n).toString(),
        restoreOperationRevision: route.restoreOperationRevision,
      });
      const restore = fixture.restore;
      const restoreStartingOperation = restoreGenerationOperationRow(
        restore,
        "starting",
      );
      const restoreStartingReservation = restoreGenerationReservationRow(
        restore,
        "starting",
      );
      const restoreStartingSession = restoreGenerationPhaseSessionRow(
        restore,
        "starting",
      );
      const restoreUncertainOperation = restoreGenerationOperationRow(
        restore,
        "uncertain",
      );
      const restoreUncertainReservation = restoreGenerationReservationRow(
        restore,
        "uncertain",
      );
      const restoreUncertainSession = restoreGenerationPhaseSessionRow(
        restore,
        "uncertain",
      );
      const launchStartingOperation = writerLaunchOperationRow(
        fixture,
        "starting",
        { createdAt: RESTORE_FINALIZE_NOW },
      );
      const launchStartingReservation = writerLaunchReservationRow(
        fixture,
        "starting",
        { createdAt: RESTORE_FINALIZE_NOW },
      );
      const launchStartingSession = writerLaunchPhaseSessionRow(
        fixture,
        "starting",
      );
      const launchUncertainOperation = writerLaunchOperationRow(
        fixture,
        "uncertain",
        { createdAt: RESTORE_FINALIZE_NOW },
      );
      const launchUncertainReservation = writerLaunchReservationRow(
        fixture,
        "uncertain",
        { createdAt: RESTORE_FINALIZE_NOW },
      );
      const launchUncertainSession = writerLaunchPhaseSessionRow(
        fixture,
        "uncertain",
      );
      const evidence = writerLaunchEvidence(fixture);
      const result = writerLaunchResult(fixture);
      const launchCommittedOperation = writerLaunchOperationRow(
        fixture,
        "committed",
        {
          createdAt: RESTORE_FINALIZE_NOW,
          result,
          revision: "3",
        },
      );
      const launchCommittedReservation = writerLaunchReservationRow(
        fixture,
        "released",
        { createdAt: RESTORE_FINALIZE_NOW },
      );
      const launchCommittedSession = writerLaunchCommittedSessionRow(
        fixture,
        { operationRevision: "3", result },
      );
      const scripts = [
        {
          options: {
            authorityNow: RESTORE_AUTHORITY_NOW,
            now: RESTORE_DISPATCH_NOW,
          },
          steps: [
            ...restoreGenerationDispatchReadSteps(restore, "prepared"),
            ...restoreCheckpointSourceSteps(restore),
            rows(restoreLaunchIdClaimRow(restore)),
            rows(restoreGenerationRow(restore)),
            rows(restoreStartingOperation),
            rows(restoreStartingReservation),
            rows(restoreStartingSession),
          ],
        },
        ...(route.restoreUncertain
          ? [
              {
                options: { now: RESTORE_UNCERTAIN_NOW },
                steps: [
                  ...restoreGenerationActiveSteps(restore, "starting"),
                  rows(restoreUncertainOperation),
                  rows(restoreUncertainReservation),
                  rows(restoreUncertainSession),
                ],
              },
            ]
          : []),
        {
          options: { now: RESTORE_FINALIZE_NOW },
          steps: [
            ...restoreLaunchHandoffRestoreSteps(
              restore,
              route.restoreUncertain ? "uncertain" : "starting",
            ),
            ...restoreLaunchHandoffWriteSteps(fixture),
          ],
        },
        {
          options: {
            authorityNow: LAUNCH_DISPATCH_NOW,
            now: LAUNCH_DISPATCH_NOW,
          },
          steps: [
            ...restoreLaunchHandoffActiveSteps(fixture),
            ...writerLaunchGenerationReferenceSteps(fixture),
            rows(launchStartingOperation),
            rows(launchStartingReservation),
            rows(launchStartingSession),
          ],
        },
        {
          options: { now: LAUNCH_UNCERTAIN_NOW },
          steps: [
            ...writerLaunchActiveSteps(fixture, "starting", {
              createdAt: RESTORE_FINALIZE_NOW,
            }),
            rows(launchUncertainOperation),
            rows(launchUncertainReservation),
            rows(launchUncertainSession),
          ],
        },
        {
          options: { now: LAUNCH_FINALIZE_NOW },
          steps: [
            ...writerLaunchActiveSteps(fixture, "uncertain", {
              createdAt: RESTORE_FINALIZE_NOW,
            }),
            rows(launchCommittedOperation),
            rows(launchCommittedReservation),
            rows(launchCommittedSession),
          ],
        },
      ];
      const { authority, clients } = authorityWithScripts(...scripts);

      const restoreClaimed =
        await authority.claimRestoreDestinationGenerationDispatch({
          ...restore.options,
          destinationIsolationProofId:
            restore.destinationIsolationProofId,
          expectedOperationRevision: "0",
          generationId: restore.generationId,
        });
      const restoreUncertain = route.restoreUncertain
        ? await authority.markOperationUncertain({
            ...restore.options,
            expectedOperationRevision: "1",
          })
        : null;
      const handoff =
        await authority.finalizeRestoreDestinationGenerationAndReserveWriterLaunchAttempt(
          {
            launch: restore.launchIntent,
            restore: {
              ...restore.options,
              completion: restore.completion,
              expectedOperationRevision: route.restoreUncertain ? "2" : "1",
            },
          },
        );
      const launchClaimed = await authority.claimWriterLaunchAttemptDispatch({
        ...fixture.options,
        expectedOperationRevision: "0",
      });
      const launchUncertain = await authority.markOperationUncertain({
        ...fixture.options,
        expectedOperationRevision: "1",
      });
      const launchFinalized =
        await authority.finalizeWriterLaunchAttemptStarted({
          ...fixture.options,
          evidence,
          expectedOperationRevision: "2",
        });

      assert.equal(restore.request.contractVersion, 2);
      assert.equal(
        restoreGenerationPhaseSessionRow(restore, "prepared").revision,
        preparedRevision.toString(),
      );
      assert.equal(restoreClaimed.dispatchGranted, true);
      assert.equal(
        restoreClaimed.session.revision,
        (preparedRevision + 1n).toString(),
      );
      if (route.restoreUncertain) {
        assert.equal(restoreUncertain.changed, true);
        assert.equal(
          restoreUncertain.session.revision,
          (preparedRevision + 2n).toString(),
        );
      }
      assert.equal(handoff.restore.finalized, true);
      assert.equal(launchClaimed.dispatchGranted, true);
      assert.equal(launchUncertain.changed, true);
      assert.equal(launchFinalized.finalized, true);
      assert.equal(launchFinalized.attempt.state, "committed");
      assert.equal(
        launchFinalized.session.revision,
        (
          preparedRevision + (route.restoreUncertain ? 7n : 6n)
        ).toString(),
      );
      for (const client of clients) client.assertExhausted();
    });
  }
});

test("restore generation V2 committed replay remains available at bigint max", async () => {
  const fixture = restoreGenerationFixture({
    expectedSessionRevision: (MAX_POSTGRES_BIGINT - 3n).toString(),
    launchAttemptId: LAUNCH_ATTEMPT_OPERATION_ID,
  });
  const { authority, clients } = authorityWithScripts(
    restoreGenerationCommittedDispatchReadSteps(fixture, {
      launchIdClaim: restoreLaunchIdClaimRow(fixture, {
        materializedAt: RESTORE_FINALIZE_NOW,
      }),
      operationRevision: "2",
    }),
  );

  const replay =
    await authority.claimRestoreDestinationGenerationDispatch({
      ...fixture.options,
      destinationIsolationProofId: fixture.destinationIsolationProofId,
      expectedOperationRevision: "0",
      generationId: fixture.generationId,
    });

  assert.equal(fixture.request.contractVersion, 2);
  assert.equal(replay.dispatchGranted, false);
  assert.equal(replay.operation.state, "committed");
  assert.equal(replay.generation.state, "committed");
  assert.equal(replay.session.revision, MAX_POSTGRES_BIGINT.toString());
  assert.equal(
    queryTexts(clients[0]).includes(READ_AUTHORITY_CLOCK_QUERY),
    false,
  );
  assert.equal(
    authorityQueries(clients[0]).some((args) =>
      /^(?:INSERT|UPDATE) /u.test(queryText(args)),
    ),
    false,
  );
  clients[0].assertExhausted();
});

test("restore-to-launch handoff at bigint max-5 reaches terminal through uncertainty", async () => {
  const restoreStartingRevision = MAX_POSTGRES_BIGINT - 5n;
  const fixture = restoreLaunchHandoffFixture({
    expectedSessionRevision: (restoreStartingRevision - 2n).toString(),
  });
  const restore = fixture.restore;
  const startingOperation = writerLaunchOperationRow(fixture, "starting", {
    createdAt: RESTORE_FINALIZE_NOW,
  });
  const startingReservation = writerLaunchReservationRow(
    fixture,
    "starting",
    { createdAt: RESTORE_FINALIZE_NOW },
  );
  const startingSession = writerLaunchPhaseSessionRow(fixture, "starting");
  const uncertainOperation = writerLaunchOperationRow(
    fixture,
    "uncertain",
    { createdAt: RESTORE_FINALIZE_NOW },
  );
  const uncertainReservation = writerLaunchReservationRow(
    fixture,
    "uncertain",
    { createdAt: RESTORE_FINALIZE_NOW },
  );
  const uncertainSession = writerLaunchPhaseSessionRow(
    fixture,
    "uncertain",
  );
  const evidence = writerLaunchEvidence(fixture);
  const result = writerLaunchResult(fixture);
  const committedOperation = writerLaunchOperationRow(
    fixture,
    "committed",
    {
      createdAt: RESTORE_FINALIZE_NOW,
      result,
      revision: "3",
    },
  );
  const committedReservation = writerLaunchReservationRow(
    fixture,
    "released",
    { createdAt: RESTORE_FINALIZE_NOW },
  );
  const committedSession = writerLaunchCommittedSessionRow(fixture, {
    operationRevision: "3",
    result,
  });
  const { authority, clients } = authorityWithScripts(
    {
      options: { now: RESTORE_FINALIZE_NOW },
      steps: [
        ...restoreLaunchHandoffRestoreSteps(restore, "starting"),
        ...restoreLaunchHandoffWriteSteps(fixture),
      ],
    },
    {
      options: {
        authorityNow: LAUNCH_DISPATCH_NOW,
        now: LAUNCH_DISPATCH_NOW,
      },
      steps: [
        ...restoreLaunchHandoffActiveSteps(fixture),
        ...writerLaunchGenerationReferenceSteps(fixture),
        rows(startingOperation),
        rows(startingReservation),
        rows(startingSession),
      ],
    },
    {
      options: { now: LAUNCH_UNCERTAIN_NOW },
      steps: [
        ...writerLaunchActiveSteps(fixture, "starting", {
          createdAt: RESTORE_FINALIZE_NOW,
        }),
        rows(uncertainOperation),
        rows(uncertainReservation),
        rows(uncertainSession),
      ],
    },
    {
      options: { now: LAUNCH_FINALIZE_NOW },
      steps: [
        ...writerLaunchActiveSteps(fixture, "uncertain", {
          createdAt: RESTORE_FINALIZE_NOW,
        }),
        rows(committedOperation),
        rows(committedReservation),
        rows(committedSession),
      ],
    },
  );

  const handoff =
    await authority.finalizeRestoreDestinationGenerationAndReserveWriterLaunchAttempt(
      {
        launch: restore.launchIntent,
        restore: {
          ...restore.options,
          completion: restore.completion,
          expectedOperationRevision: "1",
        },
      },
    );
  const claimed = await authority.claimWriterLaunchAttemptDispatch({
    ...fixture.options,
    expectedOperationRevision: "0",
  });
  const uncertain = await authority.markOperationUncertain({
    ...fixture.options,
    expectedOperationRevision: "1",
  });
  const finalized = await authority.finalizeWriterLaunchAttemptStarted({
    ...fixture.options,
    evidence,
    expectedOperationRevision: "2",
  });

  assert.equal(
    restoreGenerationPhaseSessionRow(restore, "starting").revision,
    restoreStartingRevision.toString(),
  );
  assert.equal(
    handoff.session.revision,
    (restoreStartingRevision + 2n).toString(),
  );
  assert.equal(claimed.dispatchGranted, true);
  assert.equal(
    claimed.session.revision,
    (restoreStartingRevision + 3n).toString(),
  );
  assert.equal(uncertain.changed, true);
  assert.equal(
    uncertain.session.revision,
    (restoreStartingRevision + 4n).toString(),
  );
  assert.equal(finalized.finalized, true);
  assert.equal(finalized.attempt.state, "committed");
  assert.equal(finalized.session.revision, MAX_POSTGRES_BIGINT.toString());
  for (const client of clients) client.assertExhausted();
});

test("restore-to-launch handoff rejects an insufficient launch claim revision budget before its first write", async (t) => {
  for (const remainingRevisions of [4n, 3n]) {
    await t.test(`starting at bigint max-${remainingRevisions}`, async () => {
      const restoreStartingRevision =
        MAX_POSTGRES_BIGINT - remainingRevisions;
      const fixture = restoreLaunchHandoffFixture({
        expectedSessionRevision: (restoreStartingRevision - 2n).toString(),
      });
      const restore = fixture.restore;
      const { authority, clients } = authorityWithScripts(
        restoreGenerationActiveSteps(restore, "starting"),
      );

      await assertAuthorityError(
        authority.finalizeRestoreDestinationGenerationAndReserveWriterLaunchAttempt(
          {
            launch: restore.launchIntent,
            restore: {
              ...restore.options,
              completion: restore.completion,
              expectedOperationRevision: "1",
            },
          },
        ),
        { code: "session_revision_exhausted" },
      );

      assert.equal(
        restoreGenerationPhaseSessionRow(restore, "starting").revision,
        restoreStartingRevision.toString(),
      );
      assert.equal(
        authorityQueries(clients[0]).some((args) =>
          /^(?:INSERT|UPDATE) /u.test(queryText(args)),
        ),
        false,
      );
      clients[0].assertExhausted();
    });
  }
});

test("restore-to-launch handoff rejects active session revision drift before its first write", async () => {
  const fixture = restoreLaunchHandoffFixture();
  const restore = fixture.restore;
  const driftedSession = restoreGenerationPhaseSessionRow(
    restore,
    "starting",
  );
  driftedSession.revision = (BigInt(driftedSession.revision) + 1n).toString();
  const { authority, clients } = authorityWithScripts([
    rows(driftedSession),
  ]);

  await assertAuthorityError(
    authority.finalizeRestoreDestinationGenerationAndReserveWriterLaunchAttempt(
      {
        launch: restore.launchIntent,
        restore: {
          ...restore.options,
          completion: restore.completion,
          expectedOperationRevision: "1",
        },
      },
    ),
    { code: "session_state_invalid" },
  );

  assert.equal(
    authorityQueries(clients[0]).some((args) =>
      /^(?:INSERT|UPDATE) /u.test(queryText(args)),
    ),
    false,
  );
  assert.equal(queryTexts(clients[0]).includes("ROLLBACK"), true);
  clients[0].assertExhausted();
});

test("restore generation finalization rejects materialization bound to another generation", async () => {
  const fixture = restoreGenerationFixture();
  const foreign = restoreGenerationFixture({
    destinationIsolationProofId: "destination-isolation-proof-foreign",
    generationId: "restore-generation-foreign",
    operationId: "restore-generation-operation-foreign",
  });
  const completion = {
    ...fixture.completion,
    materialization: structuredClone(foreign.completion.materialization),
  };
  const { authority, clients } = authorityWithScripts(
    restoreGenerationActiveSteps(fixture, "starting"),
  );

  await assertAuthorityError(
    authority.finalizeRestoreDestinationGeneration({
      ...fixture.options,
      completion,
      expectedOperationRevision: "1",
    }),
    { code: "invalid_operation_request" },
  );

  assert.equal(
    authorityQueries(clients[0]).some((args) =>
      /^(?:INSERT|UPDATE) /u.test(queryText(args)),
    ),
    false,
  );
  assert.equal(queryTexts(clients[0]).includes("ROLLBACK"), true);
  clients[0].assertExhausted();
});

test("restore generation binding validation ignores poisoned JSON and hash intrinsics", async () => {
  const fixture = restoreGenerationFixture();
  const foreign = restoreGenerationFixture({
    destinationIsolationProofId: "destination-isolation-proof-poisoned",
    generationId: "restore-generation-poisoned",
    operationId: "restore-generation-operation-poisoned",
  });
  const completion = {
    ...fixture.completion,
    materialization: {
      ...foreign.completion.materialization,
      coordinatorBindingSha256: "0".repeat(64),
    },
  };
  const { authority, clients } = authorityWithScripts(
    restoreGenerationActiveSteps(fixture, "starting"),
  );
  const hashPrototype = Object.getPrototypeOf(createHash("sha256"));
  const stringifyDescriptor = Object.getOwnPropertyDescriptor(
    JSON,
    "stringify",
  );
  const updateDescriptor = Object.getOwnPropertyDescriptor(
    hashPrototype,
    "update",
  );
  const digestDescriptor = Object.getOwnPropertyDescriptor(
    hashPrototype,
    "digest",
  );
  let poisonedStringifyCalls = 0;
  let poisonedUpdateCalls = 0;
  let poisonedDigestCalls = 0;

  try {
    Object.defineProperty(JSON, "stringify", {
      ...stringifyDescriptor,
      value() {
        poisonedStringifyCalls += 1;
        return "{}";
      },
    });
    Object.defineProperty(hashPrototype, "update", {
      ...updateDescriptor,
      value() {
        poisonedUpdateCalls += 1;
        return this;
      },
    });
    Object.defineProperty(hashPrototype, "digest", {
      ...digestDescriptor,
      value() {
        poisonedDigestCalls += 1;
        return "0".repeat(64);
      },
    });

    await assertAuthorityError(
      authority.finalizeRestoreDestinationGeneration({
        ...fixture.options,
        completion,
        expectedOperationRevision: "1",
      }),
      { code: "invalid_operation_request" },
    );
  } finally {
    Object.defineProperty(JSON, "stringify", stringifyDescriptor);
    Object.defineProperty(hashPrototype, "update", updateDescriptor);
    Object.defineProperty(hashPrototype, "digest", digestDescriptor);
  }

  assert.equal(poisonedStringifyCalls, 0);
  assert.equal(poisonedUpdateCalls, 0);
  assert.equal(poisonedDigestCalls, 0);
  assert.equal(
    authorityQueries(clients[0]).some((args) =>
      /^(?:INSERT|UPDATE) /u.test(queryText(args)),
    ),
    false,
  );
  assert.equal(queryTexts(clients[0]).includes("ROLLBACK"), true);
  clients[0].assertExhausted();
});

test("restore generation committed read rejects a tampered materialization binding digest", async () => {
  const fixture = restoreGenerationFixture();
  const completion = {
    ...fixture.completion,
    materialization: {
      ...fixture.completion.materialization,
      coordinatorBindingSha256: "f".repeat(64),
    },
  };
  const { authority, clients } = authorityWithScripts([
    rows(
      restoreGenerationRow(fixture, "committed", {
        document: restoreGenerationDocument(fixture, completion),
      }),
    ),
    rows(
      restoreGenerationOperationRow(fixture, "committed", {
        completion,
        revision: "2",
      }),
    ),
    ...restoreGenerationCommittedSteps(fixture, {
      completion,
      operationRevision: "2",
    }),
  ]);

  await assertAuthorityError(
    authority.readRestoreDestinationGeneration({
      checkpoint: fixture.source.checkpoint,
      generationId: fixture.generationId,
      request: fixture.mutationRequest,
    }),
    { code: "operation_state_invalid" },
  );

  assert.equal(
    authorityQueries(clients[0]).some((args) =>
      /^(?:INSERT|UPDATE) /u.test(queryText(args)),
    ),
    false,
  );
  assert.equal(queryTexts(clients[0]).includes("ROLLBACK"), true);
  clients[0].assertExhausted();
});

test("restore generation starting and committed replays reject mismatched generation identity without mutation", async () => {
  const fixture = restoreGenerationFixture();
  const cases = [
    {
      destinationIsolationProofId: fixture.destinationIsolationProofId,
      generationId: "restore-generation-conflict",
      state: "starting",
    },
    {
      destinationIsolationProofId: "destination-isolation-proof-conflict",
      generationId: fixture.generationId,
      state: "starting",
    },
    {
      destinationIsolationProofId: fixture.destinationIsolationProofId,
      generationId: "restore-generation-conflict",
      state: "committed",
    },
    {
      destinationIsolationProofId: "destination-isolation-proof-conflict",
      generationId: fixture.generationId,
      state: "committed",
    },
  ];
  const { authority, clients } = authorityWithScripts(
    ...cases.map(({ state }) =>
      state === "starting"
        ? restoreGenerationDispatchReadSteps(fixture, state)
        : restoreGenerationCommittedDispatchReadSteps(fixture),
    ),
  );

  for (const identity of cases) {
    await assertAuthorityError(
      authority.claimRestoreDestinationGenerationDispatch({
        ...fixture.options,
        destinationIsolationProofId:
          identity.destinationIsolationProofId,
        expectedOperationRevision: "0",
        generationId: identity.generationId,
      }),
      { code: "restore_generation_identity_conflict" },
    );
  }

  for (const client of clients) {
    assert.equal(
      authorityQueries(client).some((args) =>
        /^(?:INSERT|UPDATE) /u.test(queryText(args)),
      ),
      false,
    );
    assert.equal(queryTexts(client).includes("ROLLBACK"), true);
    client.assertExhausted();
  }
});

test("restore generation uncertain finalization accepts the exact authorized generation", async () => {
  const fixture = restoreGenerationFixture();
  const committedOperation = restoreGenerationOperationRow(
    fixture,
    "committed",
  );
  const committedReservation = restoreGenerationReservationRow(
    fixture,
    "released",
  );
  const committedSession = restoreGenerationCommittedSessionRow(fixture);
  const { authority, clients } = authorityWithScripts({
    options: { now: RESTORE_FINALIZE_NOW },
    steps: [
      ...restoreGenerationActiveSteps(fixture, "uncertain"),
      rows(restoreGenerationRow(fixture, "committed")),
      rows(committedOperation),
      rows(committedReservation),
      rows(committedSession),
    ],
  });

  const finalized = await authority.finalizeRestoreDestinationGeneration({
    ...fixture.options,
    completion: fixture.completion,
    expectedOperationRevision: "2",
  });

  assert.equal(finalized.finalized, true);
  assert.equal(finalized.operation.state, "committed");
  assert.equal(finalized.operation.revision, "3");
  assert.equal(finalized.generation.state, "committed");
  clients[0].assertExhausted();
});

test("restore generation prepared cancellation never creates or later authorizes a generation", async () => {
  const fixture = restoreGenerationFixture();
  const cancelled = restoreGenerationCancelledFixture(fixture);
  const { authority, clients } = authorityWithScripts(
    {
      options: { now: RESTORE_CANCEL_NOW },
      steps: [
        ...restoreGenerationActiveSteps(fixture, "prepared"),
        rows(cancelled.operation),
        rows(cancelled.reservation),
        rows(cancelled.session),
      ],
    },
    restoreGenerationCancelledSteps(fixture, cancelled),
  );

  const receipt = await authority.cancelPreparedOperation({
    ...fixture.options,
    expectedOperationRevision: "0",
    reason: cancelled.reason,
  });
  await assertAuthorityError(
    authority.claimRestoreDestinationGenerationDispatch({
      ...fixture.options,
      destinationIsolationProofId: fixture.destinationIsolationProofId,
      expectedOperationRevision: "0",
      generationId: fixture.generationId,
    }),
    { code: "restore_generation_not_authorized" },
  );

  assert.equal(receipt.cancelled, true);
  assert.equal(receipt.operation.result.outcome, "cancelled-before-dispatch");
  for (const client of clients) {
    assert.equal(
      authorityQueries(client).some(
        (args) => queryText(args) === INSERT_RESTORE_GENERATION_QUERY,
      ),
      false,
    );
    client.assertExhausted();
  }
});

test("restore generation with an absent catalogue source remains readable and cancellable while prepared", async () => {
  const fixture = restoreGenerationFixture();
  const preparedOperation = restoreGenerationOperationRow(
    fixture,
    "prepared",
  );
  const preparedReservation = restoreGenerationReservationRow(
    fixture,
    "prepared",
  );
  const preparedSession = restoreGenerationPhaseSessionRow(
    fixture,
    "prepared",
  );
  const cancelled = restoreGenerationCancelledFixture(fixture);
  const { authority, clients } = authorityWithScripts(
    {
      options: { now: RESTORE_PREPARED_NOW },
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
    restoreGenerationActiveSteps(fixture, "prepared"),
    {
      options: { now: RESTORE_CANCEL_NOW },
      steps: [
        ...restoreGenerationActiveSteps(fixture, "prepared"),
        rows(cancelled.operation),
        rows(cancelled.reservation),
        rows(cancelled.session),
      ],
    },
  );

  const reserved = await authority.reserveOperation(fixture.options);
  const reconciled = await authority.reconcileOperation(fixture.options);
  const receipt = await authority.cancelPreparedOperation({
    ...fixture.options,
    expectedOperationRevision: "0",
    reason: cancelled.reason,
  });

  assert.equal(reserved.acquired, true);
  assert.equal(reconciled.status, "prepared");
  assert.equal(reconciled.operation.operationId, fixture.options.operationId);
  assert.equal(receipt.cancelled, true);
  assert.equal(receipt.operation.result.outcome, "cancelled-before-dispatch");
  assert.equal(
    clients.flatMap(authorityQueries).some((args) =>
      queryText(args).startsWith(READ_CHECKPOINT_CATALOGUE_BY_ID_QUERY),
    ),
    false,
  );
  for (const client of clients) client.assertExhausted();
});

test("restore generation relation corruption fails closed before phase mutation", async () => {
  const fixture = restoreGenerationFixture();
  const corruptGeneration = restoreGenerationRow(fixture, "authorized", {
    binding: {
      ...restoreGenerationBinding(fixture),
      captureOperationId: "substituted-capture-operation",
    },
  });
  const steps = restoreGenerationActiveSteps(fixture, "starting", {
    generation: corruptGeneration,
  }).slice(0, -2);
  const { authority, clients } = authorityWithScripts(steps);

  await assertAuthorityError(
    authority.claimRestoreDestinationGenerationDispatch({
      ...fixture.options,
      destinationIsolationProofId: fixture.destinationIsolationProofId,
      expectedOperationRevision: "0",
      generationId: fixture.generationId,
    }),
    { code: "operation_state_invalid" },
  );

  assert.equal(
    authorityQueries(clients[0]).some((args) =>
      /^(?:INSERT|UPDATE) /u.test(queryText(args)),
    ),
    false,
  );
  assert.equal(queryTexts(clients[0]).includes("ROLLBACK"), true);
  clients[0].assertExhausted();
});

test("restore generation recovery pagination returns exact frozen authorized candidates", async () => {
  const second = restoreGenerationFixture({
    destinationIsolationProofId: "destination-isolation-proof-002",
    generationId: "restore-generation-002",
    operationId: "restore-generation-operation-002",
    sessionId: OTHER_SESSION_ID,
  });
  const third = restoreGenerationFixture({
    destinationIsolationProofId: "destination-isolation-proof-003",
    generationId: "restore-generation-003",
    operationId: "restore-generation-operation-003",
    sessionId: THIRD_SESSION_ID,
  });
  const { authority, clients } = authorityWithScripts(
    [
      rows(
        restoreGenerationOperationRow(second, "starting"),
        restoreGenerationOperationRow(third, "uncertain"),
      ),
      ...restoreGenerationActiveSteps(second, "starting"),
      ...restoreGenerationActiveSteps(third, "uncertain"),
    ],
    [
      rows(restoreGenerationOperationRow(third, "uncertain")),
      ...restoreGenerationActiveSteps(third, "uncertain"),
    ],
    [rows()],
  );

  const firstPage =
    await authority.listRestoreDestinationGenerationRecoveryCandidates({
      afterSessionId: SESSION_ID,
      limit: 1,
    });
  const secondPage =
    await authority.listRestoreDestinationGenerationRecoveryCandidates({
      afterSessionId: firstPage.nextAfterSessionId,
      limit: 1,
    });
  const emptyFirstPage =
    await authority.listRestoreDestinationGenerationRecoveryCandidates({
      afterSessionId: null,
      limit: 1,
    });

  assert.deepEqual(firstPage, {
    candidates: [
      {
        checkpoint: second.request.admission.checkpoint,
        generationId: second.generationId,
        request: second.request.admission.request,
      },
    ],
    nextAfterSessionId: OTHER_SESSION_ID,
  });
  assert.deepEqual(secondPage, {
    candidates: [
      {
        checkpoint: third.request.admission.checkpoint,
        generationId: third.generationId,
        request: third.request.admission.request,
      },
    ],
    nextAfterSessionId: null,
  });
  assert.deepEqual(emptyFirstPage, {
    candidates: [],
    nextAfterSessionId: null,
  });
  assertDeepFrozen(firstPage);
  assertDeepFrozen(secondPage);
  assert.deepEqual(
    authorityQueries(clients[0])[0],
    extendedQuery(LIST_RESTORE_GENERATION_RECOVERY_AFTER_QUERY, [
      SESSION_ID,
      2,
    ]),
  );
  assert.deepEqual(
    authorityQueries(clients[1])[0],
    extendedQuery(LIST_RESTORE_GENERATION_RECOVERY_AFTER_QUERY, [
      OTHER_SESSION_ID,
      2,
    ]),
  );
  assert.deepEqual(
    authorityQueries(clients[2])[0],
    extendedQuery(LIST_RESTORE_GENERATION_RECOVERY_FIRST_PAGE_QUERY, [2]),
  );
  for (const client of clients) client.assertExhausted();
});

test("restore generation v2 recovery returns the exact durable launch intent", async () => {
  const fixture = restoreGenerationFixture({
    launchAttemptId: LAUNCH_ATTEMPT_OPERATION_ID,
  });
  const { authority, clients } = authorityWithScripts([
    rows(restoreGenerationOperationRow(fixture, "starting")),
    ...restoreGenerationActiveSteps(fixture, "starting", {
      launchIdClaim: restoreLaunchIdClaimRow(fixture),
    }),
  ]);

  const page =
    await authority.listRestoreDestinationGenerationRecoveryCandidates({
      afterSessionId: null,
      limit: 1,
    });

  assert.deepEqual(page, {
    candidates: [
      {
        checkpoint: fixture.request.admission.checkpoint,
        generationId: fixture.generationId,
        launchIntent: canonicalPayload(fixture.launchIntent),
        request: fixture.request.admission.request,
      },
    ],
    nextAfterSessionId: null,
  });
  assertDeepFrozen(page);
  assert.deepEqual(
    authorityQueries(clients[0]).find(
      (args) => queryText(args) === READ_OPERATION_ID_CLAIM_QUERY,
    ),
    extendedQuery(READ_OPERATION_ID_CLAIM_QUERY, [
      LAUNCH_ATTEMPT_OPERATION_ID,
    ]),
  );
  clients[0].assertExhausted();
});

test("restore launch intent mismatch and durable tampering fail closed", async () => {
  const fixture = restoreGenerationFixture({
    launchAttemptId: LAUNCH_ATTEMPT_OPERATION_ID,
  });
  const mismatched = {
    ...fixture.launchIntent,
    launchAttemptId: "writer-launch-attempt-mismatched",
  };
  const noDatabase = authorityWithScripts();

  await assertAuthorityError(
    noDatabase.authority
      .finalizeRestoreDestinationGenerationAndReserveWriterLaunchAttempt({
        launch: mismatched,
        restore: {
          ...fixture.options,
          completion: fixture.completion,
          expectedOperationRevision: "1",
        },
      }),
    { code: "invalid_operation_request" },
  );
  assert.equal(noDatabase.pool.connectCalls, 0);

  const tamperedOperation = restoreGenerationOperationRow(
    fixture,
    "starting",
  );
  tamperedOperation.request.payload.launchIntent.supervisor.supervisorId =
    "supervisor-tampered";
  const activeSteps = restoreGenerationActiveSteps(fixture, "starting");
  activeSteps[1] = rows(tamperedOperation);
  const { authority, clients } = authorityWithScripts([
    rows(tamperedOperation),
    ...activeSteps.slice(0, 3),
  ]);

  await assertAuthorityError(
    authority.listRestoreDestinationGenerationRecoveryCandidates({
      afterSessionId: null,
      limit: 1,
    }),
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

test("historical restore generation v1 has no launch intent and cannot use atomic handoff", async () => {
  const fixture = restoreGenerationFixture();
  const { authority, pool } = authorityWithScripts();

  assert.equal(fixture.request.contractVersion, 1);
  assert.equal("launchIntent" in fixture.request, false);
  await assertAuthorityError(
    authority.finalizeRestoreDestinationGenerationAndReserveWriterLaunchAttempt(
      {
        launch: {
          launchAttemptId: LAUNCH_ATTEMPT_OPERATION_ID,
          measuredImage: writerLaunchMeasuredImage(
            fixture.options.expectedSession,
          ),
          supervisor: {
            contractVersion: 1,
            supervisorId: SUPERVISOR_ID,
          },
        },
        restore: {
          ...fixture.options,
          completion: fixture.completion,
          expectedOperationRevision: "1",
        },
      },
    ),
    { code: "invalid_operation_request" },
  );
  assert.equal(pool.connectCalls, 0);
});

test("restore generation v2 cannot bypass atomic handoff through standalone finalization", async () => {
  const fixture = restoreGenerationFixture({
    launchAttemptId: LAUNCH_ATTEMPT_OPERATION_ID,
  });
  const { authority, pool } = authorityWithScripts();

  await assertAuthorityError(
    authority.finalizeRestoreDestinationGeneration({
      ...fixture.options,
      completion: fixture.completion,
      expectedOperationRevision: "1",
    }),
    { code: "invalid_operation_request" },
  );
  assert.equal(pool.connectCalls, 0);
});

test("restore generation typed APIs reject non-exact contracts before PostgreSQL", async () => {
  const fixture = restoreGenerationFixture();
  const { authority, pool } = authorityWithScripts();
  const cases = [
    () =>
      authority.listRestoreDestinationGenerationRecoveryCandidates({
        afterSessionId: null,
        limit: 1,
        extra: true,
      }),
    () =>
      authority.claimRestoreDestinationGenerationDispatch({
        ...fixture.options,
        destinationIsolationProofId: fixture.destinationIsolationProofId,
        expectedOperationRevision: "0",
      }),
    () =>
      authority.claimRestoreDestinationGenerationDispatch({
        ...fixture.options,
        destinationIsolationProofId: fixture.destinationIsolationProofId,
        expectedOperationRevision: "0",
        generationId: fixture.generationId,
        extra: true,
      }),
    () =>
      authority.claimRestoreDestinationGenerationDispatch({
        ...fixture.options,
        destinationIsolationProofId: fixture.destinationIsolationProofId,
        expectedOperationRevision: "0",
        generationId: fixture.generationId,
        stablePlan: null,
      }),
    () =>
      authority.finalizeRestoreDestinationGeneration({
        ...fixture.options,
        completion: { ...fixture.completion, extra: true },
        expectedOperationRevision: "1",
      }),
    () =>
      authority.readRestoreDestinationGeneration({
        checkpoint: fixture.source.checkpoint,
        generationId: fixture.generationId,
        request: fixture.mutationRequest,
        extra: true,
      }),
  ];

  for (const invoke of cases) {
    await assertAuthorityError(invoke(), {
      code: "invalid_operation_request",
    });
  }
  assert.equal(pool.connectCalls, 0);
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

test("checkpoint recovery enumeration returns exact frozen starting and uncertain candidates", async () => {
  const starting = checkpointCaptureFixture();
  const uncertain = checkpointRecoveryFixture(OTHER_SESSION_ID);
  const { authority, clients } = authorityWithScripts([
    rows(
      checkpointCaptureOperationRow(starting, "starting"),
      checkpointCaptureOperationRow(uncertain, "uncertain"),
    ),
    ...checkpointCaptureActiveSteps(starting, "starting"),
    ...checkpointCaptureActiveSteps(uncertain, "uncertain"),
  ]);

  const page = await authority.listCheckpointCaptureRecoveryCandidates({
    afterSessionId: null,
    limit: 2,
  });

  assert.deepEqual(page, {
    candidates: [
      {
        checkpoint: starting.request.admission.checkpoint,
        request: starting.request.admission.request,
        state: "starting",
      },
      {
        checkpoint: uncertain.request.admission.checkpoint,
        request: uncertain.request.admission.request,
        state: "uncertain",
      },
    ],
    nextAfterSessionId: null,
  });
  assert.deepEqual(Reflect.ownKeys(page), [
    "candidates",
    "nextAfterSessionId",
  ]);
  for (const candidate of page.candidates) {
    assert.deepEqual(Reflect.ownKeys(candidate), [
      "checkpoint",
      "request",
      "state",
    ]);
  }
  assertDeepFrozen(page);
  assert.deepEqual(
    authorityQueries(clients[0])[0],
    extendedQuery(LIST_CHECKPOINT_CAPTURE_RECOVERY_FIRST_PAGE_QUERY, [3]),
  );
  clients[0].assertExhausted();
});

test("checkpoint recovery enumeration validates the limit plus one row and advances the durable cursor", async () => {
  const second = checkpointRecoveryFixture(OTHER_SESSION_ID);
  const third = checkpointRecoveryFixture(THIRD_SESSION_ID);
  const { authority, clients } = authorityWithScripts(
    [
      rows(
        checkpointCaptureOperationRow(second, "starting"),
        checkpointCaptureOperationRow(third, "uncertain"),
      ),
      ...checkpointCaptureActiveSteps(second, "starting"),
      ...checkpointCaptureActiveSteps(third, "uncertain"),
    ],
    [
      rows(checkpointCaptureOperationRow(third, "uncertain")),
      ...checkpointCaptureActiveSteps(third, "uncertain"),
    ],
  );

  const firstPage =
    await authority.listCheckpointCaptureRecoveryCandidates({
      afterSessionId: SESSION_ID,
      limit: 1,
    });
  const secondPage =
    await authority.listCheckpointCaptureRecoveryCandidates({
      afterSessionId: firstPage.nextAfterSessionId,
      limit: 1,
    });

  assert.deepEqual(firstPage, {
    candidates: [
      {
        checkpoint: second.request.admission.checkpoint,
        request: second.request.admission.request,
        state: "starting",
      },
    ],
    nextAfterSessionId: OTHER_SESSION_ID,
  });
  assert.deepEqual(secondPage, {
    candidates: [
      {
        checkpoint: third.request.admission.checkpoint,
        request: third.request.admission.request,
        state: "uncertain",
      },
    ],
    nextAfterSessionId: null,
  });
  assert.equal(
    firstPage.candidates[0].request.sessionId,
    firstPage.nextAfterSessionId,
  );
  assert.deepEqual(
    authorityQueries(clients[0])[0],
    extendedQuery(LIST_CHECKPOINT_CAPTURE_RECOVERY_AFTER_QUERY, [
      SESSION_ID,
      2,
    ]),
  );
  assert.deepEqual(
    authorityQueries(clients[1])[0],
    extendedQuery(LIST_CHECKPOINT_CAPTURE_RECOVERY_AFTER_QUERY, [
      OTHER_SESSION_ID,
      2,
    ]),
  );
  for (const client of clients) client.assertExhausted();
});

test("checkpoint recovery accepts only handoff prepared rows and rejects committed or foreign claims", async () => {
  const stop = writerLaunchStopFixture({ contractVersion: 3 });
  const fixture = writerLaunchStopCaptureFixture(stop);
  const foreign = operationRow("starting", {
    options: reserveOptions(),
    revision: "1",
    updatedAt: LATEST,
  });
  const { authority, clients } = authorityWithScripts(
    [
      rows(
        checkpointCaptureOperationRow(fixture, "prepared", {
          createdAt: LAUNCH_STOP_FINALIZE_NOW,
          updatedAt: LAUNCH_STOP_FINALIZE_NOW,
        }),
      ),
      ...writerLaunchStopCaptureActiveSteps(stop, fixture),
    ],
    [rows(checkpointCaptureOperationRow(fixture, "committed"))],
    [rows(foreign)],
  );

  const page = await authority.listCheckpointCaptureRecoveryCandidates({
    afterSessionId: null,
    limit: 1,
  });
  assert.deepEqual(page, {
    candidates: [
      {
        checkpoint: fixture.request.admission.checkpoint,
        request: fixture.request.admission.request,
        state: "prepared",
      },
    ],
    nextAfterSessionId: null,
  });
  clients[0].assertExhausted();

  for (let index = 1; index < clients.length; index += 1) {
    await assertAuthorityError(
      authority.listCheckpointCaptureRecoveryCandidates({
        afterSessionId: null,
        limit: 1,
      }),
      { code: "operation_state_invalid" },
    );
    assert.deepEqual(authorityQueries(clients[index]), [
      extendedQuery(LIST_CHECKPOINT_CAPTURE_RECOVERY_FIRST_PAGE_QUERY, [2]),
    ]);
    assert.equal(queryTexts(clients[index]).includes("ROLLBACK"), true);
    clients[index].assertExhausted();
  }
});

test("prepared stop-capture handoff supports cold read and one fresh dispatch after lease expiry", async () => {
  const stop = writerLaunchStopFixture({ contractVersion: 3 });
  const fixture = writerLaunchStopCaptureFixture(stop);
  const preparedOperation = checkpointCaptureOperationRow(
    fixture,
    "prepared",
    {
      createdAt: LAUNCH_STOP_FINALIZE_NOW,
      updatedAt: LAUNCH_STOP_FINALIZE_NOW,
    },
  );
  const startingOperation = checkpointCaptureOperationRow(
    fixture,
    "starting",
    {
      createdAt: LAUNCH_STOP_FINALIZE_NOW,
      updatedAt: EXPIRED_FINALIZE_NOW,
    },
  );
  const startingReservation = checkpointCaptureReservationRow(
    fixture,
    "starting",
    {
      createdAt: LAUNCH_STOP_FINALIZE_NOW,
      updatedAt: EXPIRED_FINALIZE_NOW,
    },
  );
  const startingSession = checkpointCapturePhaseSessionRow(
    fixture,
    "starting",
    { updatedAt: EXPIRED_FINALIZE_NOW },
  );
  const { authority, clients } = authorityWithScripts(
    [
      rows(preparedOperation),
      ...writerLaunchStopCaptureActiveSteps(stop, fixture),
    ],
    {
      options: {
        authorityNow: EXPIRED_FINALIZE_NOW,
        now: EXPIRED_FINALIZE_NOW,
      },
      steps: [
        ...writerLaunchStopCaptureActiveSteps(stop, fixture),
        rows(
          checkpointCaptureAttemptRow(fixture, {
            claimedAt: EXPIRED_FINALIZE_NOW,
          }),
        ),
        rows(startingOperation),
        rows(startingReservation),
        rows(startingSession),
      ],
    },
  );

  const read = await authority.readCheckpointCaptureAttempt({
    checkpoint: fixture.checkpoint,
    request: fixture.mutationRequest,
  });
  const claimed = await authority.claimCheckpointCaptureDispatch({
    ...fixture.options,
    expectedOperationRevision: "0",
  });

  assert.equal(read.status, "prepared");
  assert.equal(read.attempt, null);
  assert.equal(read.catalogue, null);
  assert.equal(read.operation.state, "prepared");
  assert.equal(claimed.dispatchGranted, true);
  assert.equal(claimed.authorityNow, EXPIRED_FINALIZE_NOW);
  assert.equal(claimed.operation.state, "starting");
  assert.equal(
    Date.parse(fixture.options.expectedSession.document.lease.expiresAt) <
      Date.parse(claimed.authorityNow),
    true,
  );
  for (const client of clients) client.assertExhausted();
});

test("checkpoint recovery enumeration rejects active-pointer, tombstone, and catalogue corruption", async () => {
  const fixture = checkpointCaptureFixture();
  const operation = checkpointCaptureOperationRow(fixture, "starting");
  const reservation = checkpointCaptureReservationRow(fixture, "starting");
  const session = checkpointCapturePhaseSessionRow(fixture, "starting");
  session.document.activeOperation.reservationId =
    "checkpoint-reservation-corrupt";
  const { authority, clients } = authorityWithScripts(
    [rows(operation), rows(session), rows(operation), rows(reservation)],
    [
      rows(operation),
      rows(checkpointCapturePhaseSessionRow(fixture, "starting")),
      rows(operation),
      rows(reservation),
      rows(checkpointCaptureAttemptRow(fixture)),
      rows(checkpointCaptureTombstoneRow(fixture)),
      rows(),
    ],
    [
      rows(operation),
      rows(checkpointCapturePhaseSessionRow(fixture, "starting")),
      rows(operation),
      rows(reservation),
      rows(checkpointCaptureAttemptRow(fixture)),
      rows(),
      rows(checkpointCatalogueRow(fixture)),
    ],
  );

  await assertAuthorityError(
    authority.listCheckpointCaptureRecoveryCandidates({
      afterSessionId: null,
      limit: 1,
    }),
    { code: "operation_state_invalid" },
  );
  await assertAuthorityError(
    authority.listCheckpointCaptureRecoveryCandidates({
      afterSessionId: null,
      limit: 1,
    }),
    { code: "operation_state_invalid" },
  );
  await assertAuthorityError(
    authority.listCheckpointCaptureRecoveryCandidates({
      afterSessionId: null,
      limit: 1,
    }),
    { code: "operation_state_invalid" },
  );

  for (const client of clients) {
    assert.equal(
      authorityQueries(client).some((args) =>
        /^(?:INSERT|UPDATE) /u.test(queryText(args)),
      ),
      false,
    );
    assert.equal(queryTexts(client).includes("ROLLBACK"), true);
    client.assertExhausted();
  }
});

test("checkpoint recovery enumeration rejects corrupt page bounds and ordering", async () => {
  const second = checkpointRecoveryFixture(OTHER_SESSION_ID);
  const third = checkpointRecoveryFixture(THIRD_SESSION_ID);
  const { authority, clients } = authorityWithScripts(
    [rows({}, {}, {})],
    [
      rows(
        checkpointCaptureOperationRow(third, "uncertain"),
        checkpointCaptureOperationRow(second, "starting"),
      ),
      ...checkpointCaptureActiveSteps(third, "uncertain"),
    ],
  );

  await assertAuthorityError(
    authority.listCheckpointCaptureRecoveryCandidates({
      afterSessionId: null,
      limit: 1,
    }),
    { code: "operation_state_invalid" },
  );
  await assertAuthorityError(
    authority.listCheckpointCaptureRecoveryCandidates({
      afterSessionId: SESSION_ID,
      limit: 2,
    }),
    { code: "operation_state_invalid" },
  );

  assert.deepEqual(authorityQueries(clients[0]), [
    extendedQuery(LIST_CHECKPOINT_CAPTURE_RECOVERY_FIRST_PAGE_QUERY, [2]),
  ]);
  for (const client of clients) {
    assert.equal(queryTexts(client).includes("ROLLBACK"), true);
    client.assertExhausted();
  }
});

test("checkpoint recovery enumeration rejects invalid page inputs before PostgreSQL", async () => {
  const { authority, pool } = authorityWithScripts();
  let accessorCalls = 0;
  const accessor = { afterSessionId: null, limit: 1 };
  Object.defineProperty(accessor, "limit", {
    enumerable: true,
    get() {
      accessorCalls += 1;
      return 1;
    },
  });
  const cases = [
    null,
    {},
    { afterSessionId: null },
    { afterSessionId: null, limit: 1, extra: true },
    { afterSessionId: undefined, limit: 1 },
    { afterSessionId: "019F2100-0000-7000-8000-000000000001", limit: 1 },
    { afterSessionId: "not-a-session-id", limit: 1 },
    { afterSessionId: null, limit: 0 },
    { afterSessionId: null, limit: 101 },
    { afterSessionId: null, limit: 1.5 },
    { afterSessionId: null, limit: Number.MAX_SAFE_INTEGER + 1 },
    { afterSessionId: null, limit: "1" },
    { afterSessionId: null, limit: 1n },
    accessor,
    new Proxy({ afterSessionId: null, limit: 1 }, {}),
  ];

  for (const value of cases) {
    await assertAuthorityError(
      authority.listCheckpointCaptureRecoveryCandidates(value),
      { code: "invalid_operation_request" },
    );
  }
  assert.equal(accessorCalls, 0);
  assert.equal(pool.connectCalls, 0);
});

test("checkpoint recovery enumeration uses captured validation and freezing intrinsics", async () => {
  const { authority, clients } = authorityWithScripts([rows()]);
  const safeIntegerDescriptor = Object.getOwnPropertyDescriptor(
    Number,
    "isSafeInteger",
  );
  const isArrayDescriptor = Object.getOwnPropertyDescriptor(Array, "isArray");
  const freezeDescriptor = Object.getOwnPropertyDescriptor(Object, "freeze");
  let poisonedCalls = 0;
  let page;

  try {
    Object.defineProperty(Number, "isSafeInteger", {
      ...safeIntegerDescriptor,
      value() {
        poisonedCalls += 1;
        return false;
      },
    });
    Object.defineProperty(Array, "isArray", {
      ...isArrayDescriptor,
      value() {
        poisonedCalls += 1;
        return false;
      },
    });
    Object.defineProperty(Object, "freeze", {
      ...freezeDescriptor,
      value(value) {
        poisonedCalls += 1;
        return value;
      },
    });
    page = await authority.listCheckpointCaptureRecoveryCandidates({
      afterSessionId: null,
      limit: 1,
    });
  } finally {
    Object.defineProperty(Number, "isSafeInteger", safeIntegerDescriptor);
    Object.defineProperty(Array, "isArray", isArrayDescriptor);
    Object.defineProperty(Object, "freeze", freezeDescriptor);
  }

  assert.equal(poisonedCalls, 0);
  assert.deepEqual(page, { candidates: [], nextAfterSessionId: null });
  assertDeepFrozen(page);
  clients[0].assertExhausted();
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

test("historical checkpoint reads reject a session restored before commit", async () => {
  const fixture = checkpointCaptureFixture();
  const committedOperation = checkpointCaptureOperationRow(
    fixture,
    "committed",
  );
  const { authority, clients } = authorityWithScripts([
    rows(committedOperation),
    rows(fixture.writer.session),
    rows({ operation_count: 0, reservation_count: 0 }),
    rows(fixture.writer.committedOperation),
    rows(fixture.writer.releasedReservation),
    rows(committedOperation),
  ]);

  await assertAuthorityError(
    authority.readCheckpointCaptureAttempt({
      checkpoint: fixture.checkpoint,
      request: fixture.mutationRequest,
    }),
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

test("historical checkpoint catalogue rejects a session restored before commit", async () => {
  const fixture = checkpointCaptureFixture();
  const committedOperation = checkpointCaptureOperationRow(
    fixture,
    "committed",
  );
  const { authority, clients } = authorityWithScripts([
    rows(checkpointCatalogueRow(fixture)),
    rows(checkpointCaptureAttemptRow(fixture)),
    rows(committedOperation),
    rows(fixture.writer.session),
    rows({ operation_count: 0, reservation_count: 0 }),
    rows(fixture.writer.committedOperation),
    rows(fixture.writer.releasedReservation),
    rows(committedOperation),
  ]);

  await assertAuthorityError(
    authority.readCheckpointCatalogue({
      checkpoint: fixture.checkpoint,
    }),
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

test("historical checkpoint attempt rejects replacement document identity at the committed revision floor", async () => {
  const fixture = checkpointCaptureFixture();
  const committedOperation = checkpointCaptureOperationRow(
    fixture,
    "committed",
  );
  const replacement = checkpointHistoricalReplacementFixture(fixture, {
    capabilities: backendCapabilities({ fencing: "manual" }),
  });
  assert.equal(
    replacement.session.revision,
    (
      BigInt(fixture.options.expectedSession.revision) +
      BigInt(committedOperation.revision) +
      1n
    ).toString(),
  );
  assert.notDeepEqual(
    replacement.session.document.backendCapabilities,
    fixture.options.expectedSession.document.backendCapabilities,
  );
  assert.equal(
    replacement.session.created_at.toISOString(),
    fixture.options.expectedSession.createdAt,
  );
  const { authority, clients } = authorityWithScripts([
    rows(committedOperation),
    rows(replacement.session),
    rows({ operation_count: 0, reservation_count: 0 }),
    rows(replacement.operation),
    rows(replacement.reservation),
    rows(committedOperation),
  ]);

  await assertAuthorityError(
    authority.readCheckpointCaptureAttempt({
      checkpoint: fixture.checkpoint,
      request: fixture.mutationRequest,
    }),
    { code: "operation_state_invalid" },
  );

  assert.deepEqual(authorityQueries(clients[0]).map(queryText), [
    READ_OPERATION_QUERY,
    READ_SESSION_QUERY,
    READ_ACTIVE_COUNTS_QUERY,
    READ_OPERATION_QUERY,
    READ_RESERVATION_QUERY,
    READ_OPERATION_QUERY,
  ]);
  assert.equal(
    authorityQueries(clients[0]).some((args) =>
      /^(?:INSERT|UPDATE) /u.test(queryText(args)),
    ),
    false,
  );
  clients[0].assertExhausted();
});

test("historical checkpoint catalogue rejects a replacement session incarnation at the committed revision floor", async () => {
  const fixture = checkpointCaptureFixture();
  const committedOperation = checkpointCaptureOperationRow(
    fixture,
    "committed",
  );
  const replacement = checkpointHistoricalReplacementFixture(fixture, {
    createdAt: "2026-07-29T12:34:56.790Z",
  });
  assert.equal(
    replacement.session.revision,
    (
      BigInt(fixture.options.expectedSession.revision) +
      BigInt(committedOperation.revision) +
      1n
    ).toString(),
  );
  assert.deepEqual(
    {
      manifest: replacement.session.document.manifest,
      storageRef: replacement.session.document.storageRef,
      backendCapabilities:
        replacement.session.document.backendCapabilities,
    },
    {
      manifest: fixture.options.expectedSession.document.manifest,
      storageRef: fixture.options.expectedSession.document.storageRef,
      backendCapabilities:
        fixture.options.expectedSession.document.backendCapabilities,
    },
  );
  assert.notEqual(
    replacement.session.created_at.toISOString(),
    fixture.options.expectedSession.createdAt,
  );
  const { authority, clients } = authorityWithScripts([
    rows(checkpointCatalogueRow(fixture)),
    rows(checkpointCaptureAttemptRow(fixture)),
    rows(committedOperation),
    rows(replacement.session),
    rows({ operation_count: 0, reservation_count: 0 }),
    rows(replacement.operation),
    rows(replacement.reservation),
    rows(committedOperation),
  ]);

  await assertAuthorityError(
    authority.readCheckpointCatalogue({
      checkpoint: fixture.checkpoint,
    }),
    { code: "operation_state_invalid" },
  );

  assert.deepEqual(authorityQueries(clients[0]).map(queryText), [
    READ_CHECKPOINT_CATALOGUE_BY_ID_QUERY,
    READ_CAPTURE_ATTEMPT_BY_ID_QUERY,
    READ_OPERATION_QUERY,
    READ_SESSION_QUERY,
    READ_ACTIVE_COUNTS_QUERY,
    READ_OPERATION_QUERY,
    READ_RESERVATION_QUERY,
    READ_OPERATION_QUERY,
  ]);
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

test("restore attachment activation option proxies fail before reflective traps", async () => {
  const fixture = restoreAttachmentActivationV2Fixture();
  let trapCalls = 0;
  const hostileOptions = new Proxy(
    structuredClone(fixture.options),
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
  const { authority, pool } = authorityWithScripts();

  await assertAuthorityError(
    authority.reserveOperation(hostileOptions),
    { code: "invalid_operation_request" },
  );
  assert.equal(trapCalls, 0);

  const { proxy: revokedOptions, revoke } = Proxy.revocable(
    structuredClone(fixture.options),
    {},
  );
  revoke();
  await assertAuthorityError(
    authority.reserveOperation(revokedOptions),
    { code: "invalid_operation_request" },
  );
  assert.equal(pool.connectCalls, 0);
});

test("restore attachment activation V2 gate is fresh-only and accepts capture-bound distinct generations", async () => {
  const fixture = restoreAttachmentActivationV2Fixture({
    generationContractVersion: 2,
  });
  assert.equal(
    fixture.capture.request.admission.stopOperationId,
    fixture.request.predecessor.stopOperationId,
  );
  assert.equal(
    fixture.capture.request.admission.attachment.attachmentId,
    fixture.request.predecessor.attachmentId,
  );
  assert.deepEqual(
    JSON.parse(
      JSON.stringify(fixture.capture.request.admission.attachment),
    ),
    JSON.parse(
      JSON.stringify(
        fixture.releaseOptions.expectedSession.document.attachment,
      ),
    ),
  );
  const freshLookupSteps = [
    rows(fixture.detachedSessionRow),
    rows({ operation_count: 0, reservation_count: 0 }),
    rows(fixture.releaseOperation),
    rows(fixture.releaseReservation),
    rows(),
  ];
  const deniedClient = new ScriptedClient(freshLookupSteps);
  const replayClient = new ScriptedClient(
    restoreAttachmentActivationActiveSteps(fixture, "prepared"),
  );
  const deniedStore = new PostgresSerializableStore({
    dedicatedPool: new ScriptedPool([deniedClient, replayClient]),
    maxTransactionAttempts: 1,
  });
  const closedAuthority = new PostgresSessionAuthority({
    restoreGenerationV2FleetCompatible: true,
    store: deniedStore,
  });

  await assertAuthorityError(
    closedAuthority.reserveOperation(fixture.options),
    {
      code: "restore_attachment_activation_v2_fleet_capability_required",
    },
  );
  const replay = await closedAuthority.reserveOperation(fixture.options);

  assert.equal(replay.acquired, false);
  assert.equal(replay.operation.state, "prepared");
  assert.equal(
    authorityQueries(deniedClient).some((args) =>
      /^(?:INSERT|UPDATE) /u.test(queryText(args)),
    ),
    false,
  );
  deniedClient.assertExhausted();
  replayClient.assertExhausted();

  const allowedClient = new ScriptedClient(
    [
      ...freshLookupSteps,
      ...restoreAttachmentActivationRelationSteps(fixture, "prepared"),
      rows(restoreAttachmentActivationOperationRow(fixture, "prepared")),
      rows(restoreAttachmentActivationReservationRow(fixture, "prepared")),
      rows(restoreAttachmentActivationPhaseSessionRow(fixture, "prepared")),
    ],
    { now: RESTORE_ACTIVATION_PREPARED_NOW },
  );
  const allowedStore = new PostgresSerializableStore({
    dedicatedPool: new ScriptedPool([allowedClient]),
    maxTransactionAttempts: 1,
  });
  const allowedAuthority = new PostgresSessionAuthority({
    restoreAttachmentActivationV2FleetCompatible: true,
    store: allowedStore,
  });
  const acquired = await allowedAuthority.reserveOperation(fixture.options);

  assert.equal(acquired.acquired, true);
  assert.equal(acquired.operation.state, "prepared");
  assert.notEqual(
    fixture.request.generation.generationId,
    fixture.stop.request.launch.generation.generationId,
  );
  assert.equal(
    fixture.generationProducer.restore.request.contractVersion,
    2,
  );
  allowedClient.assertExhausted();
});

test("restore attachment activation V2 reserves after a committed capture-bound force-fence", async () => {
  const fixture = restoreAttachmentActivationV2Fixture({
    detachKind: "force-fence",
  });
  const client = new ScriptedClient(
    [
      rows(fixture.detachedSessionRow),
      rows({ operation_count: 0, reservation_count: 0 }),
      rows(fixture.releaseOperation),
      rows(fixture.releaseReservation),
      rows(),
      ...restoreAttachmentActivationRelationSteps(fixture, "prepared"),
      rows(restoreAttachmentActivationOperationRow(fixture, "prepared")),
      rows(restoreAttachmentActivationReservationRow(fixture, "prepared")),
      rows(restoreAttachmentActivationPhaseSessionRow(fixture, "prepared")),
    ],
    { now: RESTORE_ACTIVATION_PREPARED_NOW },
  );
  const store = new PostgresSerializableStore({
    dedicatedPool: new ScriptedPool([client]),
    maxTransactionAttempts: 1,
  });
  const authority = new PostgresSessionAuthority({
    restoreAttachmentActivationV2FleetCompatible: true,
    store,
  });

  const receipt = await authority.reserveOperation(fixture.options);

  assert.equal(fixture.releaseOperation.kind, WRITER_FORCE_FENCE_OPERATION_KIND);
  assert.equal(fixture.releaseOperation.result.outcome, "writer-fenced");
  assert.equal(
    fixture.releaseOptions.expectedSession.document.lastOperation.kind,
    CHECKPOINT_CAPTURE_OPERATION_KIND,
  );
  assert.equal(
    fixture.releaseOptions.expectedSession.document.lastOperation.operationId,
    fixture.capture.options.operationId,
  );
  assert.equal(receipt.acquired, true);
  assert.equal(receipt.operation.state, "prepared");
  assert.equal(receipt.reservation.state, "prepared");
  assert.equal(
    receipt.session.document.activeOperation.operationId,
    fixture.options.operationId,
  );
  client.assertExhausted();
});

test("restore attachment activation V2 generation-predecessor gate is fresh-only", async () => {
  const fixture = restoreAttachmentActivationV2Fixture({
    generationPredecessor: true,
  });
  const freshLookupSteps = [
    rows(fixture.detachedSessionRow),
    rows({ operation_count: 0, reservation_count: 0 }),
    rows(fixture.releaseOperation),
    rows(fixture.releaseReservation),
    rows(),
  ];
  const deniedClient = new ScriptedClient([
    ...freshLookupSteps,
    ...restoreAttachmentActivationRelationSteps(fixture, "prepared"),
  ]);
  const replayClient = new ScriptedClient(
    restoreAttachmentActivationActiveSteps(fixture, "prepared"),
  );
  const readClient = new ScriptedClient([
    rows(restoreAttachmentActivationOperationRow(fixture, "prepared")),
    ...restoreAttachmentActivationActiveSteps(fixture, "prepared"),
  ]);
  const claimClient = new ScriptedClient(
    [
      ...restoreAttachmentActivationActiveSteps(fixture, "prepared"),
      rows(restoreAttachmentActivationLaunchIdClaimRow(fixture)),
      rows(restoreAttachmentActivationOperationRow(fixture, "starting")),
      rows(
        restoreAttachmentActivationReservationRow(fixture, "starting"),
      ),
      rows(restoreAttachmentActivationPhaseSessionRow(fixture, "starting")),
    ],
    {
      authorityNow: RESTORE_ACTIVATION_AUTHORITY_NOW,
      now: RESTORE_ACTIVATION_DISPATCH_NOW,
    },
  );
  const deniedStore = new PostgresSerializableStore({
    dedicatedPool: new ScriptedPool([deniedClient]),
    maxTransactionAttempts: 1,
  });
  const deniedAuthority = new PostgresSessionAuthority({
    restoreAttachmentActivationV2FleetCompatible: true,
    store: deniedStore,
  });
  const defaultClosedStore = new PostgresSerializableStore({
    dedicatedPool: new ScriptedPool([
      replayClient,
      readClient,
      claimClient,
    ]),
    maxTransactionAttempts: 1,
  });
  const defaultClosedAuthority = new PostgresSessionAuthority({
    store: defaultClosedStore,
  });

  await assertAuthorityError(
    deniedAuthority.reserveOperation(fixture.options),
    {
      code: "restore_attachment_activation_v2_generation_predecessor_fleet_capability_required",
    },
  );
  const replay =
    await defaultClosedAuthority.reserveOperation(fixture.options);
  const read =
    await defaultClosedAuthority.readRestoreAttachmentActivation({
      operationId: fixture.options.operationId,
    });
  const claimed =
    await defaultClosedAuthority.claimRestoreAttachmentActivationDispatch({
      ...structuredClone(fixture.options),
      expectedOperationRevision: "0",
    });

  assert.equal(replay.acquired, false);
  assert.equal(read.operation.state, "prepared");
  assert.equal(claimed.dispatchGranted, true);
  assert.equal(claimed.operation.state, "starting");
  assert.equal(
    authorityQueries(deniedClient).some((args) =>
      /^(?:INSERT|UPDATE) /u.test(queryText(args)),
    ),
    false,
  );
  for (const client of [
    deniedClient,
    replayClient,
    readClient,
    claimClient,
  ]) {
    client.assertExhausted();
  }
});

test("restore attachment activation V2 accepts exact generation predecessors after release and force-fence", async (t) => {
  for (const detachKind of ["release", "force-fence"]) {
    await t.test(detachKind, async () => {
      const fixture = restoreAttachmentActivationV2Fixture({
        detachKind,
        generationPredecessor: true,
      });
      const client = new ScriptedClient(
        [
          rows(fixture.detachedSessionRow),
          rows({ operation_count: 0, reservation_count: 0 }),
          rows(fixture.releaseOperation),
          rows(fixture.releaseReservation),
          rows(),
          ...restoreAttachmentActivationRelationSteps(fixture, "prepared"),
          rows(restoreAttachmentActivationOperationRow(fixture, "prepared")),
          rows(
            restoreAttachmentActivationReservationRow(fixture, "prepared"),
          ),
          rows(
            restoreAttachmentActivationPhaseSessionRow(fixture, "prepared"),
          ),
        ],
        { now: RESTORE_ACTIVATION_PREPARED_NOW },
      );
      const store = new PostgresSerializableStore({
        dedicatedPool: new ScriptedPool([client]),
        maxTransactionAttempts: 1,
      });
      const authority = new PostgresSessionAuthority({
        restoreAttachmentActivationV2FleetCompatible: true,
        restoreAttachmentActivationV2GenerationPredecessorFleetCompatible:
          true,
        store,
      });

      const receipt = await authority.reserveOperation(fixture.options);

      assert.equal(
        fixture.releaseOptions.expectedSession.document.lastOperation.kind,
        RESTORE_DESTINATION_GENERATION_OPERATION_KIND,
      );
      assert.equal(
        fixture.releaseOptions.expectedSession.document.lastOperation
          .operationId,
        fixture.request.generation.operationId,
      );
      assert.equal(
        fixture.generationProducer.restore.options.expectedSession.document
          .lastOperation.kind,
        CHECKPOINT_CAPTURE_OPERATION_KIND,
      );
      assert.equal(
        fixture.generationProducer.restore.options.expectedSession.document
          .lastOperation.operationId,
        fixture.request.predecessor.captureOperationId,
      );
      assert.equal(receipt.acquired, true);
      assert.equal(receipt.operation.state, "prepared");
      assert.equal(
        fixture.generationProducer.restore.request.contractVersion,
        1,
      );
      client.assertExhausted();
    });
  }
});

test("restore attachment activation V2 generation predecessors reject substitution, pointer drift, and non-terminal links before writes", async (t) => {
  const asStarting = (operation) => ({
    ...structuredClone(operation),
    result: null,
    retired_at: null,
    revision: "1",
    state: "starting",
  });
  const relationPrefix = (fixture) => [
    ...writerLaunchGenerationReferenceSteps(fixture.generationProducer),
    rows(fixture.releaseOperation),
    rows(fixture.releaseReservation),
  ];
  const capturePrefix = (fixture) => [
    ...relationPrefix(fixture),
    rows(fixture.captureOperation),
    rows(fixture.captureReservation),
    rows(fixture.captureAttempt),
    rows(),
    rows(fixture.captureCatalogue),
  ];
  const fixture = restoreAttachmentActivationV2Fixture({
    generationPredecessor: true,
  });
  const pointerDriftFixture = restoreAttachmentActivationV2Fixture({
    generationPointerOperationId: "substituted-target-generation-operation",
    generationPredecessor: true,
  });
  const v2GenerationFixture = restoreAttachmentActivationV2Fixture({
    generationContractVersion: 2,
    generationPredecessor: true,
  });
  const generationOperation = restoreGenerationOperationRow(
    fixture.generationProducer.restore,
    "committed",
    { revision: "2" },
  );
  const cases = [
    {
      name: "target generation substitution",
      fixture,
      relationSteps: [
        rows(
          restoreGenerationRow(
            fixture.generationProducer.restore,
            "committed",
            { operation_id: "substituted-target-generation-operation" },
          ),
        ),
      ],
    },
    {
      name: "non-terminal target generation",
      fixture,
      relationSteps: [
        rows(
          restoreGenerationRow(
            fixture.generationProducer.restore,
            "committed",
          ),
        ),
        rows(asStarting(generationOperation)),
      ],
    },
    {
      name: "detach target generation pointer drift",
      fixture: pointerDriftFixture,
      relationSteps: relationPrefix(pointerDriftFixture),
    },
    {
      name: "atomic V2 target generation cannot be the direct predecessor",
      fixture: v2GenerationFixture,
      relationSteps: relationPrefix(v2GenerationFixture),
    },
    {
      name: "non-terminal detach",
      fixture,
      relationSteps: [
        ...writerLaunchGenerationReferenceSteps(
          fixture.generationProducer,
        ),
        rows(asStarting(fixture.releaseOperation)),
      ],
    },
    {
      name: "non-terminal predecessor capture",
      fixture,
      relationSteps: [
        ...relationPrefix(fixture),
        rows(asStarting(fixture.captureOperation)),
      ],
    },
    {
      name: "non-terminal predecessor stop",
      fixture,
      relationSteps: [
        ...capturePrefix(fixture),
        rows(writerLaunchStopOperationRow(fixture.stop, "starting")),
      ],
    },
  ];

  for (const candidate of cases) {
    await t.test(candidate.name, async () => {
      const client = new ScriptedClient([
        rows(candidate.fixture.detachedSessionRow),
        rows({ operation_count: 0, reservation_count: 0 }),
        rows(candidate.fixture.releaseOperation),
        rows(candidate.fixture.releaseReservation),
        rows(),
        ...candidate.relationSteps,
      ]);
      const store = new PostgresSerializableStore({
        dedicatedPool: new ScriptedPool([client]),
        maxTransactionAttempts: 1,
      });
      const authority = new PostgresSessionAuthority({
        restoreAttachmentActivationV2FleetCompatible: true,
        restoreAttachmentActivationV2GenerationPredecessorFleetCompatible:
          true,
        store,
      });

      await assertAuthorityError(
        authority.reserveOperation(candidate.fixture.options),
        { code: "operation_state_invalid" },
      );
      assert.equal(
        authorityQueries(client).some((args) =>
          /^(?:INSERT|UPDATE) /u.test(queryText(args)),
        ),
        false,
      );
      client.assertExhausted();
    });
  }
});

test("restore attachment activation V2 rejects substituted capture authority and reordered predecessors before writes", async (t) => {
  const capturePointerFixture = restoreAttachmentActivationV2Fixture();
  const capturePointerRequest =
    createRestoreAttachmentActivationOperationRequestV2({
      destinationRootPath:
        capturePointerFixture.request.destinationRootPath,
      expectedSession: capturePointerFixture.options.expectedSession,
      generation: capturePointerFixture.generation,
      holderId: capturePointerFixture.request.holderId,
      launchIntent: capturePointerFixture.launchIntent,
      leaseDurationMilliseconds:
        capturePointerFixture.request.leaseDurationMilliseconds,
      predecessor: {
        ...capturePointerFixture.request.predecessor,
        captureOperationId: "substituted-capture-operation-001",
      },
    });
  const bindingFixture = restoreAttachmentActivationV2Fixture();
  const driftedAttempt = structuredClone(bindingFixture.captureAttempt);
  driftedAttempt.binding.attachmentId = "substituted-attachment-001";
  const reorderedFixture = restoreAttachmentActivationFixture();
  const reorderedRequest =
    createRestoreAttachmentActivationOperationRequestV2({
      destinationRootPath: reorderedFixture.request.destinationRootPath,
      expectedSession: reorderedFixture.options.expectedSession,
      generation: reorderedFixture.generation,
      holderId: reorderedFixture.request.holderId,
      launchIntent: reorderedFixture.launchIntent,
      leaseDurationMilliseconds:
        reorderedFixture.request.leaseDurationMilliseconds,
      predecessor: {
        attachmentId: reorderedFixture.request.predecessor.attachmentId,
        captureOperationId: "missing-ordered-capture-operation-001",
        detachOperationId:
          reorderedFixture.request.predecessor.detachOperationId,
        stopOperationId: reorderedFixture.request.predecessor.stopOperationId,
      },
    });
  const cases = [
    {
      fixture: capturePointerFixture,
      name: "capture operation ID substitution",
      options: {
        ...capturePointerFixture.options,
        request: capturePointerRequest,
      },
      relationSteps: [
        ...writerLaunchGenerationReferenceSteps(
          capturePointerFixture.generationProducer,
        ),
        rows(capturePointerFixture.releaseOperation),
        rows(capturePointerFixture.releaseReservation),
      ],
    },
    {
      fixture: bindingFixture,
      name: "capture attempt attachment binding drift",
      options: bindingFixture.options,
      relationSteps: [
        ...writerLaunchGenerationReferenceSteps(
          bindingFixture.generationProducer,
        ),
        rows(bindingFixture.releaseOperation),
        rows(bindingFixture.releaseReservation),
        rows(bindingFixture.captureOperation),
        rows(bindingFixture.captureReservation),
        rows(driftedAttempt),
      ],
    },
    {
      fixture: reorderedFixture,
      name: "detach still directly follows stop",
      options: { ...reorderedFixture.options, request: reorderedRequest },
      relationSteps: [
        ...writerLaunchGenerationReferenceSteps(reorderedFixture.launch),
        rows(reorderedFixture.releaseOperation),
        rows(reorderedFixture.releaseReservation),
      ],
    },
  ];

  for (const candidate of cases) {
    await t.test(candidate.name, async () => {
      const client = new ScriptedClient([
        rows(candidate.fixture.detachedSessionRow),
        rows({ operation_count: 0, reservation_count: 0 }),
        rows(candidate.fixture.releaseOperation),
        rows(candidate.fixture.releaseReservation),
        rows(),
        ...candidate.relationSteps,
      ]);
      const store = new PostgresSerializableStore({
        dedicatedPool: new ScriptedPool([client]),
        maxTransactionAttempts: 1,
      });
      const authority = new PostgresSessionAuthority({
        restoreAttachmentActivationV2FleetCompatible: true,
        store,
      });

      await assertAuthorityError(
        authority.reserveOperation(candidate.options),
        { code: "operation_state_invalid" },
      );
      assert.equal(
        authorityQueries(client).some((args) =>
          /^(?:INSERT|UPDATE) /u.test(queryText(args)),
        ),
        false,
      );
      client.assertExhausted();
    });
  }
});

test("restore attachment activation V2 rejects non-terminal predecessors and corrupt capture bindings before writes", async (t) => {
  const fixture = restoreAttachmentActivationV2Fixture();
  const asStarting = (operation) => ({
    ...structuredClone(operation),
    result: null,
    retired_at: null,
    revision: "1",
    state: "starting",
  });
  const nonCleanCapture = structuredClone(fixture.captureOperation);
  nonCleanCapture.request.payload.admission.checkpoint.checkpointClass =
    "crash-prefix";
  const driftedCatalogue = structuredClone(fixture.captureCatalogue);
  driftedCatalogue.document.materialization.publicationId =
    "restore-activation-capture-publication-substituted";
  assert.notEqual(
    fixture.captureOperation.result.catalogueSha256,
    sha256(JSON.stringify(driftedCatalogue.document)),
  );

  const generationSteps = () =>
    writerLaunchGenerationReferenceSteps(fixture.generationProducer);
  const committedCapturePrefix = () => [
    ...generationSteps(),
    rows(fixture.releaseOperation),
    rows(fixture.releaseReservation),
    rows(fixture.captureOperation),
    rows(fixture.captureReservation),
    rows(fixture.captureAttempt),
    rows(),
  ];
  const cases = [
    {
      name: "non-terminal detach",
      relationSteps: [
        ...generationSteps(),
        rows(asStarting(fixture.releaseOperation)),
      ],
    },
    {
      name: "non-terminal capture",
      relationSteps: [
        ...generationSteps(),
        rows(fixture.releaseOperation),
        rows(fixture.releaseReservation),
        rows(asStarting(fixture.captureOperation)),
      ],
    },
    {
      name: "non-terminal stop",
      relationSteps: [
        ...committedCapturePrefix(),
        rows(fixture.captureCatalogue),
        rows(writerLaunchStopOperationRow(fixture.stop, "starting")),
      ],
    },
    {
      name: "non-clean capture",
      relationSteps: [
        ...generationSteps(),
        rows(fixture.releaseOperation),
        rows(fixture.releaseReservation),
        rows(nonCleanCapture),
      ],
    },
    {
      name: "catalogue/result digest drift",
      relationSteps: [
        ...committedCapturePrefix(),
        rows(driftedCatalogue),
      ],
    },
  ];

  for (const candidate of cases) {
    await t.test(candidate.name, async () => {
      const client = new ScriptedClient([
        rows(fixture.detachedSessionRow),
        rows({ operation_count: 0, reservation_count: 0 }),
        rows(fixture.releaseOperation),
        rows(fixture.releaseReservation),
        rows(),
        ...candidate.relationSteps,
      ]);
      const store = new PostgresSerializableStore({
        dedicatedPool: new ScriptedPool([client]),
        maxTransactionAttempts: 1,
      });
      const authority = new PostgresSessionAuthority({
        restoreAttachmentActivationV2FleetCompatible: true,
        store,
      });

      await assertAuthorityError(
        authority.reserveOperation(fixture.options),
        { code: "operation_state_invalid" },
      );
      assert.equal(
        authorityQueries(client).some((args) =>
          /^(?:INSERT|UPDATE) /u.test(queryText(args)),
        ),
        false,
      );
      client.assertExhausted();
    });
  }
});

test("restore attachment activation claim preclaims one launch and returns the exact provider request", async () => {
  const fixture = restoreAttachmentActivationFixture();
  const claim = restoreAttachmentActivationLaunchIdClaimRow(fixture);
  const { authority, clients } = authorityWithScripts({
    options: {
      authorityNow: RESTORE_ACTIVATION_AUTHORITY_NOW,
      now: RESTORE_ACTIVATION_DISPATCH_NOW,
    },
    steps: [
      ...restoreAttachmentActivationActiveSteps(fixture, "prepared"),
      rows(claim),
      rows(restoreAttachmentActivationOperationRow(fixture, "starting")),
      rows(
        restoreAttachmentActivationReservationRow(fixture, "starting"),
      ),
      rows(restoreAttachmentActivationPhaseSessionRow(fixture, "starting")),
    ],
  });

  const receipt =
    await authority.claimRestoreAttachmentActivationDispatch({
      ...structuredClone(fixture.options),
      expectedOperationRevision: "0",
    });

  assert.equal(receipt.status, "starting");
  assert.equal(receipt.dispatchGranted, true);
  assert.deepEqual(
    JSON.parse(JSON.stringify(receipt.generation)),
    JSON.parse(JSON.stringify(fixture.generation)),
  );
  assert.deepEqual(
    JSON.parse(JSON.stringify(receipt.activationRequest.lease)),
    JSON.parse(
      JSON.stringify(restoreAttachmentActivationLease(fixture)),
    ),
  );
  assert.equal(
    receipt.activationRequest.mutationRequest.operationId,
    fixture.options.operationId,
  );
  assert.equal(
    receipt.activationRequest.mutationRequest.target.attachmentId,
    derivedAttachmentId(fixture.options.operationId),
  );
  assert.equal(
    receipt.activationRequest.publication.root.objectId,
    fixture.generation.document.materialization.stagedRoot.objectId,
  );
  assert.equal(
    receipt.activationRequest.publication.root.rootPath,
    fixture.request.destinationRootPath,
  );
  assert.equal(
    receipt.session.document.activeOperation.operationId,
    fixture.options.operationId,
  );
  assert.equal(receipt.session.document.lifecycle, "ATTACHING");
  assertDeepFrozen(receipt);

  const queryArguments = authorityQueries(clients[0]);
  assert.deepEqual(
    queryArguments.find(
      (args) =>
        queryText(args) ===
        INSERT_RESTORE_ACTIVATION_LAUNCH_ID_CLAIM_QUERY,
    ),
    extendedQuery(INSERT_RESTORE_ACTIVATION_LAUNCH_ID_CLAIM_QUERY, [
      fixture.launchIntent.launchAttemptId,
      fixture.options.expectedSession.sessionId,
      fixture.options.operationId,
      JSON.stringify(canonicalPayload(fixture.launchIntent)),
      RESTORE_ACTIVATION_DISPATCH_NOW,
    ]),
  );
  const mutationOrder = queryArguments
    .map(queryText)
    .filter((text) => /^(?:INSERT|UPDATE) /u.test(text));
  assert.deepEqual(mutationOrder, [
    INSERT_RESTORE_ACTIVATION_LAUNCH_ID_CLAIM_QUERY,
    START_OPERATION_QUERY,
    START_RESERVATION_QUERY,
    UPDATE_SESSION_QUERY,
  ]);
  assert.ok(
    queryTexts(clients[0]).indexOf(
      INSERT_RESTORE_ACTIVATION_LAUNCH_ID_CLAIM_QUERY,
    ) < queryTexts(clients[0]).indexOf(READ_AUTHORITY_CLOCK_QUERY),
  );
  clients[0].assertExhausted();
});

test("restore attachment activation recovery returns the exact frozen durable request", async () => {
  const fixture = restoreAttachmentActivationFixture();
  const { authority, clients } = authorityWithScripts([
    rows(restoreAttachmentActivationOperationRow(fixture, "starting")),
    ...restoreAttachmentActivationActiveSteps(fixture, "starting"),
  ]);

  const page =
    await authority.listRestoreAttachmentActivationRecoveryCandidates({
      afterSessionId: null,
      limit: 10,
    });

  assert.equal(page.nextAfterSessionId, null);
  assert.equal(page.candidates.length, 1);
  assert.deepEqual(Reflect.ownKeys(page.candidates[0]), [
    "activationOperationId",
    "request",
    "state",
  ]);
  assert.equal(
    page.candidates[0].activationOperationId,
    fixture.options.operationId,
  );
  assert.equal(page.candidates[0].state, "starting");
  assert.deepEqual(
    JSON.parse(JSON.stringify(page.candidates[0].request)),
    JSON.parse(JSON.stringify(fixture.request)),
  );
  assertDeepFrozen(page);
  assert.deepEqual(
    authorityQueries(clients[0])[0],
    extendedQuery(
      LIST_RESTORE_ATTACHMENT_ACTIVATION_RECOVERY_FIRST_PAGE_QUERY,
      [11],
    ),
  );
  assert.equal(
    authorityQueries(clients[0]).some((args) =>
      /^(?:INSERT|UPDATE) /u.test(queryText(args)),
    ),
    false,
  );
  clients[0].assertExhausted();
});

test("default-closed activation V2 authority lists and reads an existing recovery candidate", async () => {
  const fixture = restoreAttachmentActivationV2Fixture();
  const recoverySteps = () => [
    rows(restoreAttachmentActivationOperationRow(fixture, "starting")),
    ...restoreAttachmentActivationActiveSteps(fixture, "starting"),
  ];
  const listClient = new ScriptedClient(recoverySteps());
  const readClient = new ScriptedClient(recoverySteps());
  const store = new PostgresSerializableStore({
    dedicatedPool: new ScriptedPool([listClient, readClient]),
    maxTransactionAttempts: 1,
  });
  const authority = new PostgresSessionAuthority({ store });

  const page =
    await authority.listRestoreAttachmentActivationRecoveryCandidates({
      afterSessionId: null,
      limit: 10,
    });
  const receipt = await authority.readRestoreAttachmentActivation({
    operationId: fixture.options.operationId,
  });

  assert.equal(page.nextAfterSessionId, null);
  assert.equal(page.candidates.length, 1);
  assert.equal(page.candidates[0].state, "starting");
  assert.equal(page.candidates[0].request.contractVersion, 2);
  assert.deepEqual(
    JSON.parse(JSON.stringify(page.candidates[0].request)),
    JSON.parse(JSON.stringify(fixture.request)),
  );
  assert.equal(receipt.operation.state, "starting");
  assert.equal(receipt.operation.request.contractVersion, 2);
  assert.deepEqual(
    JSON.parse(JSON.stringify(receipt.generation)),
    JSON.parse(JSON.stringify(fixture.generation)),
  );
  assert.equal(
    receipt.activationRequest.mutationRequest.operationId,
    fixture.options.operationId,
  );
  assertDeepFrozen(page);
  assertDeepFrozen(receipt);
  for (const client of [listClient, readClient]) {
    assert.equal(
      authorityQueries(client).some((args) =>
        /^(?:INSERT|UPDATE) /u.test(queryText(args)),
      ),
      false,
    );
    client.assertExhausted();
  }
});

test("restore attachment activation finalization atomically commits the attachment and prepared launch", async () => {
  const fixture = restoreAttachmentActivationFixture();
  const launch = restoreAttachmentActivationLaunchFixture(fixture);
  const activationResult = restoreAttachmentActivationProviderResult(fixture);
  const { authority, clients } = authorityWithScripts({
    options: { now: RESTORE_ACTIVATION_FINALIZE_NOW },
    steps: [
      ...restoreAttachmentActivationActiveSteps(fixture, "starting"),
      rows(restoreAttachmentActivationCommittedOperationRow(fixture)),
      rows(restoreAttachmentActivationReleasedReservationRow(fixture)),
      rows(restoreAttachmentActivationTerminalSessionRow(fixture)),
      rows(
        writerLaunchOperationRow(launch, "prepared", {
          createdAt: RESTORE_ACTIVATION_FINALIZE_NOW,
          updatedAt: RESTORE_ACTIVATION_FINALIZE_NOW,
        }),
      ),
      rows(
        writerLaunchReservationRow(launch, "prepared", {
          createdAt: RESTORE_ACTIVATION_FINALIZE_NOW,
          updatedAt: RESTORE_ACTIVATION_FINALIZE_NOW,
        }),
      ),
      rows(
        restoreAttachmentActivationLaunchIdClaimRow(fixture, {
          materializedAt: RESTORE_ACTIVATION_FINALIZE_NOW,
        }),
      ),
      rows(restoreAttachmentActivationLaunchActiveSessionRow(fixture)),
    ],
  });

  const receipt =
    await authority.finalizeRestoreAttachmentActivationAndReserveWriterLaunchAttempt(
      {
        ...structuredClone(fixture.options),
        activationResult,
        expectedOperationRevision: "1",
      },
    );

  assert.equal(receipt.status, "prepared");
  assert.equal(receipt.activation.finalized, true);
  assert.equal(
    receipt.activation.operation.result.outcome,
    "restore-attachment-activated",
  );
  assert.equal(
    receipt.launch.attempt.launchAttemptId,
    fixture.launchIntent.launchAttemptId,
  );
  assert.deepEqual(
    JSON.parse(JSON.stringify(receipt.launch.attempt.request)),
    JSON.parse(JSON.stringify(launch.request)),
  );
  assert.equal(
    receipt.session.document.attachment.operationId,
    fixture.options.operationId,
  );
  assert.equal(
    receipt.session.document.activeOperation.operationId,
    fixture.launchIntent.launchAttemptId,
  );
  assert.equal(
    receipt.session.document.lastOperation.operationId,
    fixture.options.operationId,
  );
  assertDeepFrozen(receipt);

  const queryArguments = authorityQueries(clients[0]);
  const mutationOrder = queryArguments
    .map(queryText)
    .filter((text) => /^(?:INSERT|UPDATE) /u.test(text));
  assert.deepEqual(mutationOrder, [
    COMMIT_ACTIVE_OPERATION_QUERY,
    RELEASE_ACTIVE_RESERVATION_QUERY,
    UPDATE_SESSION_QUERY,
    INSERT_PRECLAIMED_RESTORE_ACTIVATION_LAUNCH_QUERY,
    INSERT_RESERVATION_QUERY,
    MATERIALIZE_RESTORE_ACTIVATION_LAUNCH_ID_CLAIM_QUERY,
    UPDATE_SESSION_QUERY,
  ]);
  assert.deepEqual(
    queryArguments.find(
      (args) =>
        queryText(args) ===
        MATERIALIZE_RESTORE_ACTIVATION_LAUNCH_ID_CLAIM_QUERY,
    ),
    extendedQuery(
      MATERIALIZE_RESTORE_ACTIVATION_LAUNCH_ID_CLAIM_QUERY,
      [
        fixture.launchIntent.launchAttemptId,
        fixture.options.expectedSession.sessionId,
        RESTORE_ACTIVATION_FINALIZE_NOW,
        fixture.options.operationId,
        JSON.stringify(canonicalPayload(fixture.launchIntent)),
      ],
    ),
  );
  assert.equal(
    queryTexts(clients[0]).filter((text) => text === "COMMIT").length,
    1,
  );
  clients[0].assertExhausted();
});

test("restore attachment activation V2 generation-predecessor finalization survives acknowledgement loss and replays its atomic prepared launch", async () => {
  const fixture = restoreAttachmentActivationV2Fixture({
    generationPredecessor: true,
  });
  const launch = restoreAttachmentActivationLaunchFixture(fixture);
  const activationResult = restoreAttachmentActivationProviderResult(fixture);
  const committedSteps = restoreAttachmentActivationLaunchRecoverySteps(
    fixture,
    { launch },
  );
  const { authority, clients } = authorityWithScripts(
    {
      options: {
        commitError: new Error(
          "restore activation handoff acknowledgement lost",
        ),
        now: RESTORE_ACTIVATION_FINALIZE_NOW,
      },
      steps: [
        ...restoreAttachmentActivationActiveSteps(fixture, "starting"),
        rows(restoreAttachmentActivationCommittedOperationRow(fixture)),
        rows(restoreAttachmentActivationReleasedReservationRow(fixture)),
        rows(restoreAttachmentActivationTerminalSessionRow(fixture)),
        rows(
          writerLaunchOperationRow(launch, "prepared", {
            createdAt: RESTORE_ACTIVATION_FINALIZE_NOW,
            updatedAt: RESTORE_ACTIVATION_FINALIZE_NOW,
          }),
        ),
        rows(
          writerLaunchReservationRow(launch, "prepared", {
            createdAt: RESTORE_ACTIVATION_FINALIZE_NOW,
            updatedAt: RESTORE_ACTIVATION_FINALIZE_NOW,
          }),
        ),
        rows(
          restoreAttachmentActivationLaunchIdClaimRow(fixture, {
            materializedAt: RESTORE_ACTIVATION_FINALIZE_NOW,
          }),
        ),
        rows(restoreAttachmentActivationLaunchActiveSessionRow(fixture)),
      ],
    },
    [
      ...committedSteps.slice(1),
      ...committedSteps.slice(2),
    ],
  );
  const input = {
    ...structuredClone(fixture.options),
    activationResult,
    expectedOperationRevision: "1",
  };

  await assert.rejects(
    authority.finalizeRestoreAttachmentActivationAndReserveWriterLaunchAttempt(
      input,
    ),
    assertStoreCommitUncertain,
  );
  const replay =
    await authority.finalizeRestoreAttachmentActivationAndReserveWriterLaunchAttempt(
      input,
    );

  assert.equal(replay.activation.finalized, false);
  assert.equal(replay.status, "prepared");
  assert.equal(
    replay.launch.operation.operationId,
    fixture.launchIntent.launchAttemptId,
  );
  assert.deepEqual(replay.launch.operation.request, launch.request);
  assert.equal(
    authorityQueries(clients[1]).some((args) =>
      /^(?:INSERT|UPDATE) /u.test(queryText(args)),
    ),
    false,
  );
  assert.equal(queryTexts(clients[0]).at(-1), "ROLLBACK");
  clients[0].assertExhausted({ destroyed: true });
  clients[1].assertExhausted();
});
