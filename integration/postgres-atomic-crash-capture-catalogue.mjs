import assert from "node:assert/strict";
import { Buffer } from "node:buffer";
import { createHash, randomUUID } from "node:crypto";

import {
  PostgresAtomicCrashCaptureCatalogueError,
  createPostgresAtomicCrashCaptureCatalogue,
} from "../src/postgres-atomic-crash-capture-catalogue.mjs";
import {
  PostgresSerializableStore,
} from "../src/postgres-serializable-store.mjs";

const SESSION_ID = "019f8e00-0000-7000-8000-000000000001";
const CODEX_THREAD_ID = "019f8e00-0000-7000-8000-000000000002";
const IMAGE_DIGEST = `sha256:${"b".repeat(64)}`;

function canonicalJson(value) {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  if (value !== null && typeof value === "object") {
    return `{${Object.keys(value)
      .sort()
      .map((key) => `${JSON.stringify(key)}:${canonicalJson(value[key])}`)
      .join(",")}}`;
  }
  return JSON.stringify(value);
}

function canonicalSha256(value) {
  return createHash("sha256").update(canonicalJson(value), "utf8").digest("hex");
}

function captureRequest(prefix, overrides = {}) {
  const artifactId = overrides.artifactId ?? `artifact-${prefix}`;
  const captureAttemptId =
    overrides.captureAttemptId ?? `capture-${prefix}`;
  const checkpointId = overrides.checkpointId ?? `checkpoint-${prefix}`;
  const operationId = overrides.operationId ?? `operation-${prefix}`;
  return {
    captureAttemptId,
    checkpoint: {
      artifactId,
      backendId: "lvm-integration",
      checkpointClass: "crash-prefix",
      checkpointId,
      codexSessionId: CODEX_THREAD_ID,
      codexThreadId: CODEX_THREAD_ID,
      contractVersion: 1,
      createdAt: "2026-08-29T00:00:00.000Z",
      imageDigest: IMAGE_DIGEST,
      sessionId: SESSION_ID,
      sourceFencingEpoch: "18446744073709551615",
      storageId: "storage-integration-atomic",
    },
    contractVersion: 1,
    mutationRequest: {
      backendId: "lvm-integration",
      contractVersion: 1,
      fencingEpoch: "18446744073709551615",
      holderId: "holder-integration-atomic",
      leaseId: "lease-integration-atomic",
      operation: "checkpoint",
      operationId,
      sessionId: SESSION_ID,
      storageId: "storage-integration-atomic",
      target: { artifactId, checkpointId, kind: "checkpoint" },
    },
    sourceAttachment: {
      attachmentId: "attachment-integration-atomic",
      backendId: "lvm-integration",
      contractVersion: 1,
      fencingEpoch: "18446744073709551615",
      holderId: "holder-integration-atomic",
      kind: "directory",
      leaseId: "lease-integration-atomic",
      mode: "read-write",
      operationId: "operation-attach-integration-atomic",
      proofId: "proof-attach-integration-atomic",
      rootPath: "/var/lib/portable-codex/integration-atomic",
      sessionId: SESSION_ID,
      storageId: "storage-integration-atomic",
    },
    storageRef: {
      backendId: "lvm-integration",
      contractVersion: 1,
      sessionId: SESSION_ID,
      storageId: "storage-integration-atomic",
    },
  };
}

function captureResult(request, proofId = "proof-integration-atomic") {
  return {
    artifact: {
      byteLength: "1073741824",
      contentSha256: "c".repeat(64),
      objectId: `object-${request.captureAttemptId}`,
      objectIdentityScheme: "lvm-lv-uuid-v1",
      readOnly: true,
    },
    artifactId: request.checkpoint.artifactId,
    backendId: request.storageRef.backendId,
    captureAttemptId: request.captureAttemptId,
    checkpointId: request.checkpoint.checkpointId,
    contractVersion: 1,
    operationId: request.mutationRequest.operationId,
    proofId,
    sessionId: request.storageRef.sessionId,
    sourceFencingEpoch: request.checkpoint.sourceFencingEpoch,
    status: "committed",
    storageId: request.storageRef.storageId,
  };
}

function providerBinding(prefix) {
  return {
    bindingKind: "lvm-classic-snapshot-v1",
    contractVersion: 1,
    originLvUuid: `origin-${prefix}`,
    snapshotName: `snapshot-${prefix}`,
    snapshotSizeBytes: "1073741824",
    snapshotTag: `portable-codex.${prefix}`,
  };
}

function jsonbOverheadProviderBinding() {
  const binding = {
    entries: new Array(8_189).fill(0),
    padding: "",
  };
  const remaining =
    65_536 - Buffer.byteLength(canonicalJson(binding), "utf8");
  assert.ok(remaining > 0);
  binding.padding = "x".repeat(remaining);
  assert.equal(Buffer.byteLength(canonicalJson(binding), "utf8"), 65_536);
  return binding;
}

function acknowledgementLossPool(pool, matches) {
  let armed = false;
  let acknowledgementLost = false;
  return Object.freeze({
    dedicatedPool: Object.freeze({
      async connect() {
        const client = await pool.connect();
        return {
          connection: client.connection,
          async query(...args) {
            const input = args[0];
            const text = typeof input === "string" ? input : input?.text;
            const result = await Reflect.apply(client.query, client, args);
            if (typeof text === "string" && matches(text)) armed = true;
            if (text === "COMMIT" && armed && !acknowledgementLost) {
              acknowledgementLost = true;
              throw new Error("synthetic atomic catalogue COMMIT acknowledgement loss");
            }
            return result;
          },
          release(...args) {
            return Reflect.apply(client.release, client, args);
          },
        };
      },
    }),
    didLoseAcknowledgement() {
      return acknowledgementLost;
    },
  });
}

function assertCatalogueConflict(error) {
  assert.ok(error instanceof PostgresAtomicCrashCaptureCatalogueError);
  assert.equal(error.code, "postgres_atomic_crash_capture_catalogue_conflict");
  assert.equal(error.retryable, false);
  return true;
}

function assertGuard(constraint) {
  return (error) => {
    assert.equal(error.code, "55000");
    assert.equal(error.constraint, constraint);
    return true;
  };
}

async function catalogueRow(pool, captureAttemptId) {
  const result = await pool.query(
    [
      "SELECT capture_attempt_id, operation_id, checkpoint_id, artifact_id,",
      "state, request_json, request_sha256, provider_binding,",
      "provider_binding_json, provider_binding_sha256, result_json,",
      "result_sha256,",
      "claimed_at, uncertain_at, committed_at",
      "FROM session_authority.atomic_crash_captures",
      "WHERE capture_attempt_id = $1",
    ].join(" "),
    [captureAttemptId],
  );
  assert.equal(result.rows.length, 1);
  return result.rows[0];
}

async function claim(catalogue, request, binding) {
  return catalogue.claimStarting({ providerBinding: binding, request });
}

export async function assertPostgresAtomicCrashCaptureCatalogueIntegration(
  pool,
  store,
) {
  const catalogue = createPostgresAtomicCrashCaptureCatalogue({ store });
  const overheadPrefix = `jsonb-overhead-${randomUUID()}`;
  const overheadRequest = captureRequest(overheadPrefix);
  const overheadBinding = jsonbOverheadProviderBinding();
  const overheadJson = canonicalJson(overheadBinding);
  const overheadSizes = await pool.query(
    [
      "SELECT pg_catalog.octet_length(pg_catalog.convert_to($1, 'UTF8'))",
      "AS canonical_bytes, pg_catalog.pg_column_size($1::pg_catalog.jsonb)",
      "AS jsonb_bytes",
    ].join(" "),
    [overheadJson],
  );
  assert.equal(overheadSizes.rows.length, 1);
  assert.ok(overheadSizes.rows[0].canonical_bytes <= 65_536);
  assert.ok(overheadSizes.rows[0].jsonb_bytes > 65_536);
  assert.equal(
    (await claim(catalogue, overheadRequest, overheadBinding)).outcome,
    "dispatch",
  );
  const overheadRow = await catalogueRow(
    pool,
    overheadRequest.captureAttemptId,
  );
  assert.deepEqual(overheadRow.provider_binding, overheadBinding);
  assert.equal(overheadRow.provider_binding_json, overheadJson);

  const concurrentPrefix = `concurrent-${randomUUID()}`;
  const concurrentRequest = captureRequest(concurrentPrefix);
  const concurrentBinding = providerBinding(concurrentPrefix);
  const concurrent = await Promise.all([
    claim(catalogue, concurrentRequest, concurrentBinding),
    claim(catalogue, concurrentRequest, concurrentBinding),
  ]);
  assert.deepEqual(
    concurrent.map(({ outcome }) => outcome).sort(),
    ["dispatch", "unknown"],
  );
  const concurrentDispatch = concurrent.find(
    ({ outcome }) => outcome === "dispatch",
  );
  let row = await catalogueRow(pool, concurrentRequest.captureAttemptId);
  assert.equal(row.state, "starting");
  assert.ok(row.claimed_at instanceof Date);
  assert.equal(row.uncertain_at, null);
  assert.equal(row.committed_at, null);
  assert.equal(row.operation_id, concurrentRequest.mutationRequest.operationId);
  assert.equal(row.checkpoint_id, concurrentRequest.checkpoint.checkpointId);
  assert.equal(row.artifact_id, concurrentRequest.checkpoint.artifactId);

  assert.equal(
    (
      await catalogue.markUncertain({
        dispatchClaim: concurrentDispatch.dispatchClaim,
      })
    ).outcome,
    "uncertain",
  );
  row = await catalogueRow(pool, concurrentRequest.captureAttemptId);
  assert.equal(row.state, "uncertain");
  assert.ok(row.uncertain_at instanceof Date);
  assert.equal(row.committed_at, null);
  assert.ok(row.uncertain_at.getTime() >= row.claimed_at.getTime());

  const uncertainResult = captureResult(concurrentRequest);
  await pool.query(
    [
      "UPDATE session_authority.atomic_crash_captures",
      "SET state = 'committed', result_json = $2::jsonb, result_sha256 = $3",
      "WHERE capture_attempt_id = $1",
    ].join(" "),
    [
      concurrentRequest.captureAttemptId,
      canonicalJson(uncertainResult),
      canonicalSha256(uncertainResult),
    ],
  );
  row = await catalogueRow(pool, concurrentRequest.captureAttemptId);
  assert.equal(row.state, "committed");
  assert.ok(row.committed_at instanceof Date);
  assert.ok(row.committed_at.getTime() >= row.uncertain_at.getTime());
  assert.equal(
    (await catalogue.readCommitted({ request: concurrentRequest })).outcome,
    "committed",
  );

  const directPrefix = `direct-${randomUUID()}`;
  const directRequest = captureRequest(directPrefix);
  const directBinding = providerBinding(directPrefix);
  const directClaim = await claim(catalogue, directRequest, directBinding);
  assert.equal(directClaim.outcome, "dispatch");
  const directCommitted = await catalogue.commitResult({
    dispatchClaim: directClaim.dispatchClaim,
    result: captureResult(directRequest),
  });
  assert.equal(directCommitted.outcome, "committed");
  row = await catalogueRow(pool, directRequest.captureAttemptId);
  assert.equal(row.state, "committed");
  assert.equal(row.uncertain_at, null);
  assert.ok(row.committed_at instanceof Date);
  assert.ok(row.committed_at.getTime() >= row.claimed_at.getTime());
  const restarted = createPostgresAtomicCrashCaptureCatalogue({ store });
  assert.equal(
    (await restarted.readCommitted({ request: directRequest })).outcome,
    "committed",
  );

  const insertLossPrefix = `insert-ack-loss-${randomUUID()}`;
  const insertLossRequest = captureRequest(insertLossPrefix);
  const insertLossBinding = providerBinding(insertLossPrefix);
  const insertLossPool = acknowledgementLossPool(
    pool,
    (text) =>
      text.startsWith("INSERT INTO session_authority.atomic_crash_captures"),
  );
  const insertLossCatalogue = createPostgresAtomicCrashCaptureCatalogue({
    store: new PostgresSerializableStore({
      dedicatedPool: insertLossPool.dedicatedPool,
    }),
  });
  const insertLoss = await claim(
    insertLossCatalogue,
    insertLossRequest,
    insertLossBinding,
  );
  assert.equal(insertLossPool.didLoseAcknowledgement(), true);
  assert.equal(insertLoss.outcome, "unknown");
  assert.equal(Object.hasOwn(insertLoss, "dispatchClaim"), false);
  assert.equal(
    (await claim(catalogue, insertLossRequest, insertLossBinding)).outcome,
    "unknown",
  );

  const commitLossPrefix = `commit-ack-loss-${randomUUID()}`;
  const commitLossRequest = captureRequest(commitLossPrefix);
  const commitLossBinding = providerBinding(commitLossPrefix);
  const commitLossPool = acknowledgementLossPool(
    pool,
    (text) =>
      text.startsWith("UPDATE session_authority.atomic_crash_captures") &&
      text.includes("SET state = 'committed'"),
  );
  const commitLossCatalogue = createPostgresAtomicCrashCaptureCatalogue({
    store: new PostgresSerializableStore({
      dedicatedPool: commitLossPool.dedicatedPool,
    }),
  });
  const commitLossClaim = await claim(
    commitLossCatalogue,
    commitLossRequest,
    commitLossBinding,
  );
  assert.equal(commitLossClaim.outcome, "dispatch");
  assert.equal(
    (
      await commitLossCatalogue.commitResult({
        dispatchClaim: commitLossClaim.dispatchClaim,
        result: captureResult(commitLossRequest),
      })
    ).outcome,
    "committed",
  );
  assert.equal(commitLossPool.didLoseAcknowledgement(), true);

  const originalIds = {
    artifactId: directRequest.checkpoint.artifactId,
    captureAttemptId: directRequest.captureAttemptId,
    checkpointId: directRequest.checkpoint.checkpointId,
    operationId: directRequest.mutationRequest.operationId,
  };
  for (const preserved of Object.keys(originalIds)) {
    const collisionPrefix = `collision-${preserved}-${randomUUID()}`;
    const candidate = captureRequest(collisionPrefix, {
      [preserved]: originalIds[preserved],
    });
    await assert.rejects(
      claim(catalogue, candidate, providerBinding(collisionPrefix)),
      assertCatalogueConflict,
    );
  }

  const immutableConstraint = "atomic_crash_captures_immutable_transition";
  for (const statement of [
    [
      "UPDATE session_authority.atomic_crash_captures SET capture_attempt_id = $2 WHERE capture_attempt_id = $1",
      [directRequest.captureAttemptId, `changed-${randomUUID()}`],
    ],
    [
      "UPDATE session_authority.atomic_crash_captures SET request_json = jsonb_set(request_json, '{sourceAttachment,rootPath}', '\"/tampered\"') WHERE capture_attempt_id = $1",
      [directRequest.captureAttemptId],
    ],
    [
      "UPDATE session_authority.atomic_crash_captures SET provider_binding = jsonb_set(provider_binding, '{snapshotName}', '\"tampered\"') WHERE capture_attempt_id = $1",
      [directRequest.captureAttemptId],
    ],
    [
      "UPDATE session_authority.atomic_crash_captures SET result_json = jsonb_set(result_json, '{proofId}', '\"tampered\"') WHERE capture_attempt_id = $1",
      [directRequest.captureAttemptId],
    ],
    [
      "UPDATE session_authority.atomic_crash_captures SET claimed_at = claimed_at + interval '1 second' WHERE capture_attempt_id = $1",
      [directRequest.captureAttemptId],
    ],
    [
      "UPDATE session_authority.atomic_crash_captures SET uncertain_at = claimed_at WHERE capture_attempt_id = $1",
      [directRequest.captureAttemptId],
    ],
    [
      "UPDATE session_authority.atomic_crash_captures SET committed_at = committed_at + interval '1 second' WHERE capture_attempt_id = $1",
      [directRequest.captureAttemptId],
    ],
    [
      "UPDATE session_authority.atomic_crash_captures SET state = 'starting', result_json = NULL, result_sha256 = NULL, committed_at = NULL WHERE capture_attempt_id = $1",
      [directRequest.captureAttemptId],
    ],
  ]) {
    await assert.rejects(
      pool.query(statement[0], statement[1]),
      assertGuard(immutableConstraint),
    );
  }

  const timestampPrefix = `timestamp-owner-${randomUUID()}`;
  const timestampRequest = captureRequest(timestampPrefix);
  const timestampBinding = providerBinding(timestampPrefix);
  const timestampClaim = await claim(
    catalogue,
    timestampRequest,
    timestampBinding,
  );
  assert.equal(timestampClaim.outcome, "dispatch");
  await assert.rejects(
    pool.query(
      [
        "UPDATE session_authority.atomic_crash_captures",
        "SET state = 'uncertain', uncertain_at = pg_catalog.clock_timestamp()",
        "WHERE capture_attempt_id = $1",
      ].join(" "),
      [timestampRequest.captureAttemptId],
    ),
    assertGuard(immutableConstraint),
  );
  await assert.rejects(
    pool.query(
      "DELETE FROM session_authority.atomic_crash_captures WHERE capture_attempt_id = $1",
      [directRequest.captureAttemptId],
    ),
    assertGuard("atomic_crash_captures_permanent"),
  );
  await assert.rejects(
    pool.query("TRUNCATE session_authority.atomic_crash_captures"),
    assertGuard("atomic_crash_captures_permanent"),
  );

  const forgedPrefix = `forged-time-${randomUUID()}`;
  const forgedRequest = captureRequest(forgedPrefix);
  const forgedBinding = providerBinding(forgedPrefix);
  await assert.rejects(
    pool.query(
      [
        "INSERT INTO session_authority.atomic_crash_captures",
        "(capture_attempt_id, operation_id, checkpoint_id, artifact_id,",
        "contract_version, backend_id, session_id, storage_id,",
        "source_fencing_epoch, request_json, request_sha256, provider_binding,",
        "provider_binding_sha256, state, result_json, result_sha256, claimed_at)",
        "VALUES ($1, $2, $3, $4, 1, $5, $6, $7, $8::numeric,",
        "$9::jsonb, $10, $11::jsonb, $12, 'starting', NULL, NULL,",
        "pg_catalog.clock_timestamp())",
      ].join(" "),
      [
        forgedRequest.captureAttemptId,
        forgedRequest.mutationRequest.operationId,
        forgedRequest.checkpoint.checkpointId,
        forgedRequest.checkpoint.artifactId,
        forgedRequest.storageRef.backendId,
        forgedRequest.storageRef.sessionId,
        forgedRequest.storageRef.storageId,
        forgedRequest.checkpoint.sourceFencingEpoch,
        canonicalJson(forgedRequest),
        canonicalSha256(forgedRequest),
        canonicalJson(forgedBinding),
        canonicalSha256(forgedBinding),
      ],
    ),
    assertGuard("atomic_crash_captures_insert_starting_only"),
  );
}

Object.freeze(assertPostgresAtomicCrashCaptureCatalogueIntegration);
