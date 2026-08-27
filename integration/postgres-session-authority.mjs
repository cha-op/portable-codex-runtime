import assert from "node:assert/strict";
import { createHash, randomUUID } from "node:crypto";
import {
  lstat,
  mkdir,
  mkdtemp,
  readFile,
  realpath,
  rename,
  rm,
  symlink,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { Pool } from "pg";

import {
  FilesystemOperationJournal,
  operationJournalBindingSha256,
} from "../src/filesystem-operation-journal.mjs";
import {
  POSTGRES_DETACHED_RESTORE_IMAGE_PLAN_PROVIDER_CONTRACT_VERSION,
  createPostgresDetachedRestoreImagePlanBinding,
  isPostgresDetachedRestoreImagePlanReservation,
} from "../src/postgres-detached-restore-image-plan-binding.mjs";
import {
  createPhysicalCollaboratorSettlement,
} from "../src/physical-collaborator-settlement.mjs";
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
  createPostgresDetachedRestorePlan,
} from "../src/postgres-detached-restore-plan.mjs";
import {
  createPodmanWriterSupervisorBundle,
} from "../src/podman-writer-supervisor.mjs";
import {
  createPodmanWriterSupervisorStateBundle,
  preparePodmanWriterSupervisorStateOwner,
} from "../src/podman-writer-supervisor-state.mjs";
import {
  createPostgresDetachedRestoreDeployment,
} from "../src/postgres-detached-restore-deployment.mjs";
import {
  createPostgresDetachedRestoreOperationalLeaseBudget,
} from "../src/postgres-detached-restore-operational-lease-budget.mjs";
import {
  POSTGRES_LOGICAL_WRITER_LAUNCH_PHYSICAL_RECEIPT_VERSION,
  POSTGRES_LOGICAL_WRITER_SUPERVISOR_PHYSICAL_CONTRACT_VERSION,
  POSTGRES_WRITER_SUPERVISOR_STATE_COLLECTION_PHYSICAL_CONTRACT_VERSION,
  createPostgresDetachedRestorePhysicalBindings,
} from "../src/postgres-detached-restore-physical-bindings.mjs";
import {
  POSTGRES_DETACHED_RESTORE_FLEET_CONFIRMED,
} from "../src/postgres-detached-restore-foreground-composition.mjs";
import {
  createPostgresDetachedRestoreRuntimeComposition,
} from "../src/postgres-detached-restore-runtime-composition.mjs";
import {
  createPostgresDetachedRestoreRuntimeController,
} from "../src/postgres-detached-restore-runtime-controller.mjs";
import {
  POSTGRES_DETACHED_RESTORE_STABLE_PLAN_PROVISIONING_CONFIRMED,
  PostgresDetachedRestoreStablePlanRegistryError,
  createPostgresDetachedRestoreStablePlanRegistry,
} from "../src/postgres-detached-restore-stable-plan-registry.mjs";
import {
  PostgresWriterDetachCompositionError,
  createPostgresWriterDetachComposition,
} from "../src/postgres-writer-detach-composition.mjs";
import {
  LOGICAL_WRITER_LAUNCH_RECEIPT_VERSION,
  LOGICAL_WRITER_RECONCILE_RECEIPT_VERSION,
  LOGICAL_WRITER_SUPERVISOR_CONTRACT_VERSION,
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
  assertSessionOperationTransitionProof,
  assertWriterLaunchStopCaptureHandoffProof,
} from "../src/postgres-session-authority.mjs";
import {
  PostgresSerializableStore,
  PostgresSerializableStoreError,
  SESSION_AUTHORITY_MIGRATION_VERSION,
} from "../src/postgres-serializable-store.mjs";
import {
  createPostgresFilesystemImageProviderHeadAnchor,
} from "../src/postgres-filesystem-image-provider-head-anchor.mjs";
import {
  PostgresFilesystemImageProviderStateAuthorityError,
  createPostgresFilesystemImageProviderStateAdoptionAuthority,
  createPostgresFilesystemImageProviderStatePagedAdoptionAuthority,
  createPostgresFilesystemImageProviderStateAuthority,
  createPostgresFilesystemImageProviderStateRuntimeAuthority,
} from "../src/postgres-filesystem-image-provider-state-authority.mjs";
import {
  FILESYSTEM_IMAGE_PROVIDER_STATE_HEAD_CONTRACT_VERSION,
  FILESYSTEM_IMAGE_PROVIDER_STATE_V2_HEAD_CONTRACT_VERSION,
  filesystemImageProviderStateHeadChecksum,
} from "../src/filesystem-image-provider-state.mjs";
import {
  PostgresRestoreRecoveryCursorStoreError,
  createPostgresRestoreRecoveryCursorStore,
} from "../src/postgres-restore-recovery-cursor-store.mjs";
import {
  RESTORE_ATTACHMENT_ACTIVATION_CONTRACT_VERSION,
  RESTORE_ATTACHMENT_RECONCILIATION_CONTRACT_VERSION,
  createSessionManifest,
} from "../src/session-storage-contracts.mjs";
import {
  StoppedDirectoryPublication,
} from "../src/stopped-directory-publication.mjs";
import {
  STOPPED_WRITER_STOP_CONFIRMED,
  StoppedWriterCapabilityCoordinator,
} from "../src/stopped-writer-capability.mjs";

const EMPTY_JSON_OBJECT = "{}";
const IMAGE_DIGEST = `sha256:${"a".repeat(64)}`;
const INTEGRATION_STATE_OWNER_ID = `state-owner:${"a".repeat(64)}`;
const CHECKPOINT_GUARD_APPLICATION_NAME =
  "portable-codex-runtime-checkpoint-guard-integration-test";
const SESSION_AUTHORITY_APPLICATION_NAME =
  "portable-codex-runtime-session-registry-integration-test";
const OPERATIONAL_LEASE_DATABASE_REQUEST_MILLISECONDS = 30_000;
const OPERATIONAL_LEASE_DURATION_MILLISECONDS = 5_000_000;
const OPERATIONAL_LEASE_SAFETY_MARGIN_MILLISECONDS = 30_000;
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
  Object.freeze({
    url: new URL(
      "../migrations/authority/007-detached-restore-stable-plans.sql",
      import.meta.url,
    ),
    version: 7,
  }),
  Object.freeze({
    url: new URL(
      "../migrations/authority/008-filesystem-image-provider-heads.sql",
      import.meta.url,
    ),
    version: 8,
  }),
  Object.freeze({
    url: new URL(
      "../migrations/authority/009-writer-supervisor-state-gc.sql",
      import.meta.url,
    ),
    version: 9,
  }),
  Object.freeze({
    url: new URL(
      "../migrations/authority/010-filesystem-image-provider-operations.sql",
      import.meta.url,
    ),
    version: 10,
  }),
  Object.freeze({
    url: new URL(
      "../migrations/authority/011-filesystem-image-provider-state-v3-adoption.sql",
      import.meta.url,
    ),
    version: 11,
  }),
]);

if (!databaseConfigured) {
  throw new Error(
    "SESSION_AUTHORITY_DATABASE_URL is required for the PostgreSQL integration gate",
  );
}

function explicitPostgresConnectionFromDatabaseUrl(value) {
  const parsed = new URL(value);
  assert.equal(
    parsed.protocol === "postgres:" || parsed.protocol === "postgresql:",
    true,
  );
  assert.equal(parsed.hostname.length > 0, true);
  assert.equal(parsed.username.length > 0, true);
  assert.equal(parsed.pathname.length > 1, true);
  assert.equal(parsed.search, "");
  assert.equal(parsed.hash, "");
  const port = parsed.port === "" ? 5432 : Number(parsed.port);
  assert.equal(Number.isSafeInteger(port) && port >= 1 && port <= 65_535, true);
  return Object.freeze({
    database: decodeURIComponent(parsed.pathname.slice(1)),
    host: parsed.hostname,
    password: decodeURIComponent(parsed.password),
    port,
    user: decodeURIComponent(parsed.username),
  });
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

async function assertFilesystemImageProviderHeadAnchorSchemaAndStore(
  pool,
  store,
) {
  const providerId = "filesystem-image-ext4";
  const anchorId = `integration-${randomUUID()}`;
  const createAnchor = (
    selectedStore = store,
    selectedAnchorId = anchorId,
  ) =>
    createPostgresFilesystemImageProviderHeadAnchor({
      store: selectedStore,
      providerId,
      anchorId: selectedAnchorId,
    });
  const genesis = {
    contractVersion: FILESYSTEM_IMAGE_PROVIDER_STATE_V2_HEAD_CONTRACT_VERSION,
    anchorRevision: "0",
    generation: "0",
    stateRevision: "0",
    baseHeadChecksum: null,
    checkpointStateRevision: "0",
    checkpointFrameCount: 0,
    checkpointChecksum: null,
    checkpointBytes: 0,
    frameCount: 0,
    lastChecksum: null,
    ledgerBytes: 0,
  };
  const appendHead = (expectedHead, checksumCharacter, ledgerBytes) => ({
    contractVersion: expectedHead.contractVersion,
    anchorRevision: (BigInt(expectedHead.anchorRevision) + 1n).toString(),
    generation: expectedHead.generation,
    stateRevision: (BigInt(expectedHead.stateRevision) + 1n).toString(),
    baseHeadChecksum: expectedHead.baseHeadChecksum,
    checkpointStateRevision: expectedHead.checkpointStateRevision,
    checkpointFrameCount: expectedHead.checkpointFrameCount,
    checkpointChecksum: expectedHead.checkpointChecksum,
    checkpointBytes: expectedHead.checkpointBytes,
    frameCount: expectedHead.frameCount + 1,
    lastChecksum: checksumCharacter.repeat(64),
    ledgerBytes,
  });
  const rotationHead = (
    expectedHead,
    checksumCharacter,
    checkpointFrameCount,
    checkpointBytes,
  ) => {
    const checkpointChecksum = checksumCharacter.repeat(64);
    return {
      contractVersion: expectedHead.contractVersion,
      anchorRevision: (BigInt(expectedHead.anchorRevision) + 1n).toString(),
      generation: (BigInt(expectedHead.generation) + 1n).toString(),
      stateRevision: expectedHead.stateRevision,
      baseHeadChecksum:
        filesystemImageProviderStateHeadChecksum(expectedHead),
      checkpointStateRevision: expectedHead.stateRevision,
      checkpointFrameCount,
      checkpointChecksum,
      checkpointBytes,
      frameCount: 0,
      lastChecksum: checkpointChecksum,
      ledgerBytes: 0,
    };
  };
  const first = appendHead(genesis, "a", 512);
  const second = appendHead(first, "b", 1024);
  const anchor = createAnchor();
  assert.deepEqual(await anchor.readHead(), genesis);
  assert.equal(
    await anchor.compareAndAdvance({
      expectedHead: genesis,
      nextHead: first,
    }),
    true,
  );
  assert.deepEqual(await createAnchor().readHead(), first);
  assert.equal(
    await anchor.compareAndAdvance({
      expectedHead: {
        ...first,
        lastChecksum: "f".repeat(64),
      },
      nextHead: second,
    }),
    false,
  );
  assert.deepEqual(await anchor.readHead(), first);
  assert.equal(
    await anchor.compareAndAdvance({
      expectedHead: first,
      nextHead: second,
    }),
    true,
  );
  assert.deepEqual(await createAnchor().readHead(), second);
  const concurrentHeads = [
    appendHead(second, "c", 1536),
    appendHead(second, "d", 2048),
  ];
  const concurrentResults = await Promise.all(
    concurrentHeads.map((nextHead) =>
      createAnchor().compareAndAdvance({
        expectedHead: second,
        nextHead,
      }),
    ),
  );
  assert.deepEqual([...concurrentResults].sort(), [false, true]);
  const concurrentWinner = concurrentHeads[
    concurrentResults.indexOf(true)
  ];
  assert.deepEqual(await createAnchor().readHead(), concurrentWinner);

  const rotationHeads = [
    rotationHead(concurrentWinner, "e", 2, 1536),
    rotationHead(concurrentWinner, "f", 3, 2048),
  ];
  const rotationResults = await Promise.all(
    rotationHeads.map((nextHead) =>
      createAnchor().compareAndAdvance({
        expectedHead: concurrentWinner,
        nextHead,
      }),
    ),
  );
  assert.deepEqual([...rotationResults].sort(), [false, true]);
  const rotationWinner = rotationHeads[rotationResults.indexOf(true)];
  assert.deepEqual(await createAnchor().readHead(), rotationWinner);
  const afterRotation = appendHead(rotationWinner, "0", 640);
  assert.equal(
    await createAnchor().compareAndAdvance({
      expectedHead: rotationWinner,
      nextHead: afterRotation,
    }),
    true,
  );
  assert.deepEqual(await createAnchor().readHead(), afterRotation);

  const stored = await pool.query(
    [
      "SELECT provider_id, anchor_id, contract_version,",
      "anchor_revision::pg_catalog.text AS anchor_revision,",
      "generation::pg_catalog.text AS generation,",
      "state_revision::pg_catalog.text AS state_revision,",
      "base_head_checksum,",
      "checkpoint_state_revision::pg_catalog.text AS checkpoint_state_revision,",
      "checkpoint_frame_count::pg_catalog.text AS checkpoint_frame_count,",
      "checkpoint_checksum,",
      "checkpoint_bytes::pg_catalog.text AS checkpoint_bytes,",
      "frame_count::pg_catalog.text AS frame_count, last_checksum,",
      "ledger_bytes::pg_catalog.text AS ledger_bytes",
      "FROM session_authority.filesystem_image_provider_heads",
      "WHERE provider_id = $1 AND anchor_id = $2",
    ].join(" "),
    [providerId, anchorId],
  );
  assert.deepEqual(stored.rows, [
    {
      provider_id: providerId,
      anchor_id: anchorId,
      contract_version: afterRotation.contractVersion,
      anchor_revision: afterRotation.anchorRevision,
      generation: afterRotation.generation,
      state_revision: afterRotation.stateRevision,
      base_head_checksum: afterRotation.baseHeadChecksum,
      checkpoint_state_revision: afterRotation.checkpointStateRevision,
      checkpoint_frame_count: String(afterRotation.checkpointFrameCount),
      checkpoint_checksum: afterRotation.checkpointChecksum,
      checkpoint_bytes: String(afterRotation.checkpointBytes),
      frame_count: String(afterRotation.frameCount),
      last_checksum: afterRotation.lastChecksum,
      ledger_bytes: String(afterRotation.ledgerBytes),
    },
  ]);

  await assert.rejects(
    pool.query(
      [
        "INSERT INTO session_authority.filesystem_image_provider_heads",
        "(provider_id, anchor_id, contract_version, anchor_revision, generation,",
        "state_revision, base_head_checksum, checkpoint_state_revision,",
        "checkpoint_frame_count, checkpoint_checksum, checkpoint_bytes,",
        "frame_count, last_checksum, ledger_bytes)",
        "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12, $13, $14)",
      ].join(" "),
      [
        providerId,
        `malformed-${randomUUID()}`,
        2,
        "1",
        "0",
        "1",
        null,
        "0",
        "0",
        null,
        "0",
        1,
        "a".repeat(64),
        "0",
      ],
    ),
    (error) => {
      assert.equal(error.code, "23514");
      return true;
    },
  );

  const acknowledgementLossAnchorId = `ack-loss-${randomUUID()}`;
  const acknowledgementLossPool =
    commitAcknowledgementLossAfterQueryPool(
      pool,
      "filesystem image provider head anchor",
      (text) =>
        text.startsWith(
          "INSERT INTO session_authority.filesystem_image_provider_heads",
        ),
    );
  const acknowledgementLossStore = new PostgresSerializableStore({
    dedicatedPool: acknowledgementLossPool,
    maxTransactionAttempts: 1,
  });
  await assert.rejects(
    createAnchor(
      acknowledgementLossStore,
      acknowledgementLossAnchorId,
    ).compareAndAdvance({
      expectedHead: genesis,
      nextHead: first,
    }),
    assertCommitOutcomeUncertain,
  );
  assert.equal(acknowledgementLossPool.didLoseAcknowledgement(), true);
  assert.deepEqual(
    await createAnchor(store, acknowledgementLossAnchorId).readHead(),
    first,
  );

  const rotationAcknowledgementLossAnchorId =
    `rotation-ack-loss-${randomUUID()}`;
  assert.equal(
    await createAnchor(
      store,
      rotationAcknowledgementLossAnchorId,
    ).compareAndAdvance({
      expectedHead: genesis,
      nextHead: first,
    }),
    true,
  );
  const rotationAcknowledgementLossPool =
    commitAcknowledgementLossAfterQueryPool(
      pool,
      "filesystem image provider head anchor rotation",
      (text) =>
        text.startsWith(
          "UPDATE session_authority.filesystem_image_provider_heads",
        ),
    );
  const rotationAcknowledgementLossStore = new PostgresSerializableStore({
    dedicatedPool: rotationAcknowledgementLossPool,
    maxTransactionAttempts: 1,
  });
  const acknowledgementLossRotation = rotationHead(first, "9", 2, 768);
  await assert.rejects(
    createAnchor(
      rotationAcknowledgementLossStore,
      rotationAcknowledgementLossAnchorId,
    ).compareAndAdvance({
      expectedHead: first,
      nextHead: acknowledgementLossRotation,
    }),
    assertCommitOutcomeUncertain,
  );
  assert.equal(rotationAcknowledgementLossPool.didLoseAcknowledgement(), true);
  assert.deepEqual(
    await createAnchor(
      store,
      rotationAcknowledgementLossAnchorId,
    ).readHead(),
    acknowledgementLossRotation,
  );

  const resultLossAnchorId = `result-loss-${randomUUID()}`;
  const resultLossPool = firstMatchingQueryResultFailurePool(
    pool,
    "filesystem image provider head anchor read",
    (text) =>
      text.startsWith("SELECT ") &&
      text.includes(
        "FROM session_authority.filesystem_image_provider_heads",
      ),
  );
  const resultLossStore = new PostgresSerializableStore({
    dedicatedPool: resultLossPool,
    maxTransactionAttempts: 1,
  });
  await assert.rejects(
    createAnchor(resultLossStore, resultLossAnchorId).readHead(),
    assertTransactionBoundaryLost,
  );
  assert.equal(resultLossPool.didFail(), true);

  await pool.query(
    [
      "DELETE FROM session_authority.filesystem_image_provider_heads",
      "WHERE provider_id = $1 AND anchor_id IN ($2, $3, $4)",
    ].join(" "),
    [
      providerId,
      anchorId,
      acknowledgementLossAnchorId,
      rotationAcknowledgementLossAnchorId,
    ],
  );
}

async function assertFilesystemImageProviderStateAuthoritySchemaAndStore(
  pool,
  store,
  trackedMigrations,
) {
  await pool.query("DROP SCHEMA IF EXISTS session_authority CASCADE");
  await installAuthorityMigrations(pool, trackedMigrations.slice(0, 9));

  const providerId = "filesystem-image-ext4";
  const anchorId = `operation-index-${randomUUID()}`;
  const legacyAnchorId = `operation-index-legacy-v2-${randomUUID()}`;
  const acknowledgementLossAnchorId =
    `operation-index-ack-loss-${randomUUID()}`;
  const adoptedAnchorId = `operation-index-adopted-v2-${randomUUID()}`;
  const adoptionAcknowledgementLossAnchorId =
    `operation-index-adoption-ack-loss-${randomUUID()}`;
  const adoptionValidationAnchorId =
    `operation-index-adoption-validation-${randomUUID()}`;
  const concurrentAdoptionAnchorId =
    `operation-index-concurrent-adoption-${randomUUID()}`;
  const lifecycleRaceAnchorId =
    `operation-index-lifecycle-race-${randomUUID()}`;
  const revisionRaceAnchorId =
    `operation-index-revision-race-${randomUUID()}`;
  const progressValidationAnchorId =
    `operation-index-progress-validation-${randomUUID()}`;
  const streamingAdoptionAnchorId =
    `operation-index-streaming-adoption-${randomUUID()}`;
  const createHeadAnchor = (selectedStore, selectedAnchorId) =>
    createPostgresFilesystemImageProviderHeadAnchor({
      store: selectedStore,
      providerId,
      anchorId: selectedAnchorId,
    });
  const createAuthority = (selectedStore, selectedAnchorId) =>
    createPostgresFilesystemImageProviderStateAuthority({
      store: selectedStore,
      providerId,
      anchorId: selectedAnchorId,
    });
  const createRuntimeAuthority = (selectedStore, selectedAnchorId) =>
    createPostgresFilesystemImageProviderStateRuntimeAuthority({
      store: selectedStore,
      providerId,
      anchorId: selectedAnchorId,
    });
  const createAdoptionAuthority = (selectedStore, selectedAnchorId) =>
    createPostgresFilesystemImageProviderStateAdoptionAuthority({
      store: selectedStore,
      providerId,
      anchorId: selectedAnchorId,
    });
  const createPagedAdoptionAuthority = (selectedStore, selectedAnchorId) =>
    createPostgresFilesystemImageProviderStatePagedAdoptionAuthority({
      store: selectedStore,
      providerId,
      anchorId: selectedAnchorId,
    });
  const genesis = {
    contractVersion: FILESYSTEM_IMAGE_PROVIDER_STATE_V2_HEAD_CONTRACT_VERSION,
    anchorRevision: "0",
    generation: "0",
    stateRevision: "0",
    baseHeadChecksum: null,
    checkpointStateRevision: "0",
    checkpointFrameCount: 0,
    checkpointChecksum: null,
    checkpointBytes: 0,
    frameCount: 0,
    lastChecksum: null,
    ledgerBytes: 0,
  };
  const appendHead = (expectedHead, frameChecksum, ledgerBytes) => ({
    contractVersion: expectedHead.contractVersion,
    anchorRevision: (BigInt(expectedHead.anchorRevision) + 1n).toString(),
    generation: expectedHead.generation,
    stateRevision: (BigInt(expectedHead.stateRevision) + 1n).toString(),
    baseHeadChecksum: expectedHead.baseHeadChecksum,
    checkpointStateRevision: expectedHead.checkpointStateRevision,
    checkpointFrameCount: expectedHead.checkpointFrameCount,
    checkpointChecksum: expectedHead.checkpointChecksum,
    checkpointBytes: expectedHead.checkpointBytes,
    frameCount: expectedHead.frameCount + 1,
    lastChecksum: frameChecksum,
    ledgerBytes,
  });
  const rotationHead = (expectedHead) => {
    const checkpointChecksum = "e".repeat(64);
    return {
      contractVersion: expectedHead.contractVersion,
      anchorRevision: (BigInt(expectedHead.anchorRevision) + 1n).toString(),
      generation: (BigInt(expectedHead.generation) + 1n).toString(),
      stateRevision: expectedHead.stateRevision,
      baseHeadChecksum:
        filesystemImageProviderStateHeadChecksum(expectedHead),
      checkpointStateRevision: expectedHead.stateRevision,
      checkpointFrameCount: 2,
      checkpointChecksum,
      checkpointBytes: 768,
      frameCount: 0,
      lastChecksum: checkpointChecksum,
      ledgerBytes: 0,
    };
  };
  const preparedRecord = ({
    checksum,
    kind = "provision",
    operationId,
    revision,
    request,
    storageId,
    storageStateBefore = null,
  }) => ({
    kind,
    operationId,
    preparedChecksum: checksum,
    preparedStateRevision: revision,
    request: request ?? { storageId },
    state: "prepared",
    storageId,
    storageStateBefore,
  });
  const provisionedStorage = (storageId) => ({
    storageId,
    sessionId: "provider-operation-index-session",
    backendId: "backend-ext4",
    filesystemId: "filesystem-provider-operation-index",
    imagePath: `/var/lib/portable-codex/${storageId}.img`,
    lifecycle: "provisioned",
    revision: "1",
    writerEpoch: "0",
    writerAuthority: null,
    mount: {
      mountPath: `/var/lib/portable-codex/${storageId}`,
      imageIdentity: {
        filesystemId: "filesystem-provider-operation-index",
        objectIdentityScheme: "linux-dev-inode",
        objectId: `${storageId}:image`,
      },
      rootIdentity: {
        filesystemId: "filesystem-provider-operation-index",
        objectIdentityScheme: "linux-dev-inode",
        objectId: `${storageId}:root`,
      },
    },
    publicationControlIdentity: {
      filesystemId: "filesystem-provider-operation-index",
      objectIdentityScheme: "linux-dev-inode",
      objectId: `${storageId}:publication`,
    },
    dataRoot: null,
    attachment: null,
  });
  const committedRecord = (prepared, revision) => ({
    ...prepared,
    state: "committed",
    committedStateRevision: revision,
    expectedStorage: null,
    result: { status: "created" },
    storageState: provisionedStorage(prepared.storageId),
  });
  const canonicalRecordJson = (value) => {
    if (value === null || typeof value !== "object") {
      return JSON.stringify(value);
    }
    if (Array.isArray(value)) {
      return `[${value.map(canonicalRecordJson).join(",")}]`;
    }
    return `{${Object.keys(value)
      .sort()
      .map(
        (key) =>
          `${JSON.stringify(key)}:${canonicalRecordJson(value[key])}`,
      )
      .join(",")}}`;
  };
  const projectionChecksum = (domain, values) => {
    const hash = createHash("sha256")
      .update(domain, "utf8")
      .update(`${values.length}\0`, "utf8");
    for (const value of values) {
      const bytes = Buffer.from(canonicalRecordJson(value), "utf8");
      hash.update(`${bytes.length}\0`, "utf8").update(bytes);
    }
    return hash.digest("hex");
  };
  const createOperationPager = (operations, tracker = undefined) => {
    const readPage = Object.freeze(
      async function readPage({ afterOperationId, limit }) {
        if (tracker !== undefined) {
          tracker.operationRequests.push({ afterOperationId, limit });
        }
        const start =
          afterOperationId === null
            ? 0
            : operations.findIndex(
                ({ operationId }) => operationId > afterOperationId,
              );
        const normalizedStart = start === -1 ? operations.length : start;
        const page = Object.freeze(
          operations.slice(normalizedStart, normalizedStart + limit),
        );
        return Object.freeze({
          operations: page,
          nextAfterOperationId:
            normalizedStart + page.length < operations.length
              ? page[page.length - 1].operationId
              : null,
        });
      },
    );
    return Object.freeze({ contractVersion: 1, readPage });
  };
  const createStoragePager = (storages, tracker = undefined) => {
    const readPage = Object.freeze(
      async function readPage({ afterStorageId, limit }) {
        if (tracker !== undefined) {
          tracker.storageRequests.push({ afterStorageId, limit });
        }
        const start =
          afterStorageId === null
            ? 0
            : storages.findIndex(
                ({ storage }) => storage.storageId > afterStorageId,
              );
        const normalizedStart = start === -1 ? storages.length : start;
        const page = Object.freeze(
          storages.slice(normalizedStart, normalizedStart + limit),
        );
        return Object.freeze({
          storages: page,
          nextAfterStorageId:
            normalizedStart + page.length < storages.length
              ? page[page.length - 1].storage.storageId
              : null,
        });
      },
    );
    return Object.freeze({ contractVersion: 1, readPage });
  };
  const stableStorageProjectionChecksum = (storage) =>
    createHash("sha256")
      .update(
        "portable-codex/filesystem-image-provider-state/stable-storage-projection/v1\0",
        "utf8",
      )
      .update(canonicalRecordJson({ ...storage, revision: "1" }), "utf8")
      .digest("hex");
  const projectionReceipt = (request) => {
    const attachmentOriginsChecksum = projectionChecksum(
      "portable-codex/filesystem-image-provider-state/attachment-origin-projection/v1\0",
      request.attachmentOrigins,
    );
    return {
      contractVersion: 1,
      projectionChecksum: createHash("sha256")
        .update(
          "portable-codex/filesystem-image-provider-state/authority-projection-receipt/v1\0",
          "utf8",
        )
        .update(
          canonicalRecordJson({
            attachmentOriginCount: request.attachmentOrigins.length,
            attachmentOriginsChecksum,
            expectedHeadChecksum: filesystemImageProviderStateHeadChecksum(
              request.expectedHead,
            ),
            preparedOperationCount: request.preparedOperationCount,
            preparedProjectionChecksum: request.preparedProjectionChecksum,
          }),
          "utf8",
        )
        .digest("hex"),
    };
  };
  const operationMaterial = (record) => {
    const bytes = Buffer.from(canonicalRecordJson(record), "utf8");
    const sha256 = createHash("sha256")
      .update(
        "portable-codex/filesystem-image-provider-state/operation-record/v1\0",
        "utf8",
      )
      .update(bytes)
      .digest("hex");
    return { bytes, sha256 };
  };
  const rawHeadInsertQuery = [
    "INSERT INTO session_authority.filesystem_image_provider_heads",
    "(provider_id, anchor_id, contract_version, anchor_revision, generation,",
    "state_revision, base_head_checksum, checkpoint_state_revision,",
    "checkpoint_frame_count, checkpoint_checksum, checkpoint_bytes,",
    "frame_count, last_checksum, ledger_bytes, operation_index_state_revision)",
    "VALUES ($1, $2, $3, $4::pg_catalog.numeric, $5::pg_catalog.numeric,",
    "$6::pg_catalog.numeric, $7, $8::pg_catalog.numeric, $9::pg_catalog.int8,",
    "$10, $11::pg_catalog.int8, $12::pg_catalog.int4, $13,",
    "$14::pg_catalog.int8, $15::pg_catalog.numeric)",
  ].join(" ");
  const rawHeadInsertValues = (
    selectedAnchorId,
    head,
    operationIndexStateRevision,
  ) => [
    providerId,
    selectedAnchorId,
    head.contractVersion,
    head.anchorRevision,
    head.generation,
    head.stateRevision,
    head.baseHeadChecksum,
    head.checkpointStateRevision,
    head.checkpointFrameCount,
    head.checkpointChecksum,
    head.checkpointBytes,
    head.frameCount,
    head.lastChecksum,
    head.ledgerBytes,
    operationIndexStateRevision,
  ];
  const rawHeadUpdateQuery = [
    "UPDATE session_authority.filesystem_image_provider_heads",
    "SET contract_version = $3, anchor_revision = $4::pg_catalog.numeric,",
    "generation = $5::pg_catalog.numeric, state_revision = $6::pg_catalog.numeric,",
    "base_head_checksum = $7, checkpoint_state_revision = $8::pg_catalog.numeric,",
    "checkpoint_frame_count = $9::pg_catalog.int8, checkpoint_checksum = $10,",
    "checkpoint_bytes = $11::pg_catalog.int8, frame_count = $12::pg_catalog.int4,",
    "last_checksum = $13, ledger_bytes = $14::pg_catalog.int8,",
    "operation_index_state_revision = $15::pg_catalog.numeric",
    "WHERE provider_id = $1 AND anchor_id = $2",
  ].join(" ");

  const legacyHead = appendHead(genesis, "1".repeat(64), 512);
  assert.equal(
    await createHeadAnchor(store, legacyAnchorId).compareAndAdvance({
      expectedHead: genesis,
      nextHead: legacyHead,
    }),
    true,
  );
  const adoptedPreparedHead = appendHead(genesis, "4".repeat(64), 512);
  const adoptedCommittedHead = appendHead(
    adoptedPreparedHead,
    "5".repeat(64),
    1024,
  );
  const adoptedOperationId = `provider-operation-adopted-${randomUUID()}`;
  const adoptedPrepared = preparedRecord({
    checksum: adoptedPreparedHead.lastChecksum,
    operationId: adoptedOperationId,
    revision: adoptedPreparedHead.stateRevision,
    storageId: `provider-storage-adopted-${randomUUID()}`,
  });
  const adoptedCommitted = committedRecord(
    adoptedPrepared,
    adoptedCommittedHead.stateRevision,
  );
  const adoptedHeadAnchor = createHeadAnchor(store, adoptedAnchorId);
  assert.equal(
    await adoptedHeadAnchor.compareAndAdvance({
      expectedHead: genesis,
      nextHead: adoptedPreparedHead,
    }),
    true,
  );
  assert.equal(
    await adoptedHeadAnchor.compareAndAdvance({
      expectedHead: adoptedPreparedHead,
      nextHead: adoptedCommittedHead,
    }),
    true,
  );
  const revisionRaceHeadAnchor = createHeadAnchor(store, revisionRaceAnchorId);
  let revisionRaceHead = genesis;
  for (const checksumCharacter of ["6", "7", "8"]) {
    const nextHead = appendHead(
      revisionRaceHead,
      checksumCharacter.repeat(64),
      Number(revisionRaceHead.ledgerBytes) + 512,
    );
    assert.equal(
      await revisionRaceHeadAnchor.compareAndAdvance({
        expectedHead: revisionRaceHead,
        nextHead,
      }),
      true,
    );
    revisionRaceHead = nextHead;
  }
  const legacyBefore = await pool.query(
    [
      "SELECT contract_version, anchor_revision::pg_catalog.text AS anchor_revision,",
      "state_revision::pg_catalog.text AS state_revision, last_checksum",
      "FROM session_authority.filesystem_image_provider_heads",
      "WHERE provider_id = $1 AND anchor_id = $2",
    ].join(" "),
    [providerId, legacyAnchorId],
  );
  const rejectedShortAnchorId =
    `operation-index-bpchar-short-${randomUUID()}`;
  await assert.rejects(
    pool.query(
      [
        "INSERT INTO session_authority.filesystem_image_provider_heads",
        "(provider_id, anchor_id, contract_version, anchor_revision, generation,",
        "state_revision, base_head_checksum, checkpoint_state_revision,",
        "checkpoint_frame_count, checkpoint_checksum, checkpoint_bytes,",
        "frame_count, last_checksum, ledger_bytes)",
        "VALUES ($1, $2, 2, 1, 0, 1, NULL, 0, 0, NULL, 0, 1, $3, 1)",
      ].join(" "),
      [providerId, rejectedShortAnchorId, "a"],
    ),
    (error) => {
      assert.equal(error.code, "23514");
      assert.equal(
        error.constraint,
        "filesystem_image_provider_heads_last_checksum_format",
      );
      return true;
    },
  );
  const rejectedShortHead = await pool.query(
    [
      "SELECT last_checksum",
      "FROM session_authority.filesystem_image_provider_heads",
      "WHERE provider_id = $1 AND anchor_id = $2",
    ].join(" "),
    [providerId, rejectedShortAnchorId],
  );
  assert.deepEqual(rejectedShortHead.rows, []);
  assert.deepEqual(
    await readMigrationLedger(pool),
    trackedMigrations.slice(0, 9).map(({ checksum, version }) => ({
      checksum,
      version,
    })),
  );
  const versionNineHeadChecksumType = await pool.query(
    [
      "SELECT pg_catalog.format_type(attribute.atttypid, attribute.atttypmod)",
      "AS data_type",
      "FROM pg_catalog.pg_attribute AS attribute",
      "JOIN pg_catalog.pg_class AS relation ON relation.oid = attribute.attrelid",
      "JOIN pg_catalog.pg_namespace AS namespace",
      "ON namespace.oid = relation.relnamespace",
      "WHERE namespace.nspname = 'session_authority'",
      "AND relation.relname = 'filesystem_image_provider_heads'",
      "AND attribute.attname = 'last_checksum'",
      "AND attribute.attnum > 0 AND attribute.attisdropped = false",
    ].join(" "),
  );
  assert.deepEqual(versionNineHeadChecksumType.rows, [
    { data_type: "character(64)" },
  ]);
  await installAuthorityMigration(pool, trackedMigrations[9]);
  const migrationCoverageAnchorId =
    `operation-index-migration-cover-${randomUUID()}`;
  const migrationCoverageHead = {
    ...genesis,
    contractVersion: FILESYSTEM_IMAGE_PROVIDER_STATE_HEAD_CONTRACT_VERSION,
    anchorRevision: "2",
    stateRevision: "2",
    frameCount: 2,
    lastChecksum: "c".repeat(64),
    ledgerBytes: 1024,
  };
  assert.equal(
    (
      await pool.query(
        rawHeadInsertQuery,
        rawHeadInsertValues(
          migrationCoverageAnchorId,
          migrationCoverageHead,
          migrationCoverageHead.stateRevision,
        ),
      )
    ).rowCount,
    1,
  );
  const migrationCoveragePrepared = preparedRecord({
    checksum: "b".repeat(64),
    operationId: `provider-operation-migration-cover-${randomUUID()}`,
    revision: "1",
    storageId: `provider-storage-migration-cover-${randomUUID()}`,
  });
  const migrationCoverageMaterial = operationMaterial(
    migrationCoveragePrepared,
  );
  assert.equal(
    (
      await pool.query(
        [
          "INSERT INTO session_authority.filesystem_image_provider_operations",
          "(provider_id, anchor_id, operation_id, record_contract_version, state,",
          "kind, storage_id, prepared_state_revision, prepared_checksum,",
          "prepared_record_bytes, prepared_record_sha256)",
          "VALUES ($1, $2, $3, 1, 'prepared', $4, $5,",
          "$6::pg_catalog.numeric, $7, $8, $9)",
        ].join(" "),
        [
          providerId,
          migrationCoverageAnchorId,
          migrationCoveragePrepared.operationId,
          migrationCoveragePrepared.kind,
          migrationCoveragePrepared.storageId,
          migrationCoveragePrepared.preparedStateRevision,
          migrationCoveragePrepared.preparedChecksum,
          migrationCoverageMaterial.bytes,
          migrationCoverageMaterial.sha256,
        ],
      )
    ).rowCount,
    1,
  );
  await assert.rejects(store.migrate(), (error) => {
    assert.ok(error instanceof PostgresSerializableStoreError);
    assert.equal(error.code, "migration_failed");
    assert.equal(error.commitState, "not-committed");
    return true;
  });
  assert.deepEqual(
    await readMigrationLedger(pool),
    trackedMigrations.slice(0, 10).map(({ checksum, version }) => ({
      checksum,
      version,
    })),
  );
  assert.equal(
    (
      await pool.query(
        "SELECT pg_catalog.to_regclass('session_authority.filesystem_image_provider_operation_events') AS relation",
      )
    ).rows[0].relation,
    null,
  );
  const migrationCoverageCleanup = await pool.connect();
  let migrationCoverageCleanupOpen = false;
  try {
    await migrationCoverageCleanup.query("BEGIN");
    migrationCoverageCleanupOpen = true;
    assert.equal(
      (
        await migrationCoverageCleanup.query(
          [
            "DELETE FROM session_authority.filesystem_image_provider_operations",
            "WHERE provider_id = $1 AND anchor_id = $2",
          ].join(" "),
          [providerId, migrationCoverageAnchorId],
        )
      ).rowCount,
      1,
    );
    assert.equal(
      (
        await migrationCoverageCleanup.query(
          [
            "DELETE FROM session_authority.filesystem_image_provider_heads",
            "WHERE provider_id = $1 AND anchor_id = $2",
          ].join(" "),
          [providerId, migrationCoverageAnchorId],
        )
      ).rowCount,
      1,
    );
    await migrationCoverageCleanup.query("COMMIT");
    migrationCoverageCleanupOpen = false;
  } finally {
    if (migrationCoverageCleanupOpen) {
      await migrationCoverageCleanup.query("ROLLBACK");
    }
    migrationCoverageCleanup.release();
  }
  const migrationZeroMarkerAnchorId =
    `operation-index-migration-zero-marker-${randomUUID()}`;
  const migrationZeroMarkerHead = rotationHead(genesis);
  assert.equal(
    (
      await pool.query(
        rawHeadInsertQuery,
        rawHeadInsertValues(
          migrationZeroMarkerAnchorId,
          migrationZeroMarkerHead,
          null,
        ),
      )
    ).rowCount,
    1,
  );
  await assert.rejects(store.migrate(), (error) => {
    assert.ok(error instanceof PostgresSerializableStoreError);
    assert.equal(error.code, "migration_failed");
    assert.equal(error.commitState, "not-committed");
    return true;
  });
  assert.deepEqual(
    await readMigrationLedger(pool),
    trackedMigrations.slice(0, 10).map(({ checksum, version }) => ({
      checksum,
      version,
    })),
  );
  assert.equal(
    (
      await pool.query(
        "SELECT pg_catalog.to_regclass('session_authority.filesystem_image_provider_operation_events') AS relation",
      )
    ).rows[0].relation,
    null,
  );
  assert.equal(
    (
      await pool.query(
        [
          "DELETE FROM session_authority.filesystem_image_provider_heads",
          "WHERE provider_id = $1 AND anchor_id = $2",
        ].join(" "),
        [providerId, migrationZeroMarkerAnchorId],
      )
    ).rowCount,
    1,
  );
  assert.deepEqual(await store.migrate(), {
    applied: true,
    checksum: trackedMigrations.at(-1).checksum,
    version: 11,
  });
  const forgedLifecycleAnchorIds = [];
  for (const retiredExpression of ["NULL", "pg_current_xact_id()"]) {
    const forgedLifecycleAnchorId =
      `operation-index-forged-lifecycle-${randomUUID()}`;
    forgedLifecycleAnchorIds.push(forgedLifecycleAnchorId);
    await assert.rejects(
      pool.query(
        [
          "INSERT INTO session_authority.filesystem_image_provider_anchor_lifecycle",
          "(provider_id, anchor_id, retired_xid)",
          `VALUES ($1, $2, ${retiredExpression})`,
        ].join(" "),
        [providerId, forgedLifecycleAnchorId],
      ),
      (error) => {
        assert.equal(error.code, "55000");
        assert.equal(
          error.constraint,
          "fs_image_anchor_lifecycle_immutable",
        );
        return true;
      },
    );
  }
  assert.deepEqual(
    (
      await pool.query(
        [
          "SELECT anchor_id",
          "FROM session_authority.filesystem_image_provider_anchor_lifecycle",
          "WHERE provider_id = $1",
          "AND anchor_id::pg_catalog.text = ANY($2::pg_catalog.text[])",
        ].join(" "),
        [providerId, forgedLifecycleAnchorIds],
      )
    ).rows,
    [],
  );
  await assert.rejects(
    pool.query(
      [
        "UPDATE session_authority.filesystem_image_provider_anchor_lifecycle",
        "SET retired_xid = retired_xid",
        "WHERE provider_id = $1 AND anchor_id = $2",
      ].join(" "),
      [providerId, legacyAnchorId],
    ),
    (error) => {
      assert.equal(error.code, "55000");
      assert.equal(error.constraint, "fs_image_anchor_lifecycle_immutable");
      return true;
    },
  );
  const runtimeGenesis = {
    ...genesis,
    contractVersion: FILESYSTEM_IMAGE_PROVIDER_STATE_HEAD_CONTRACT_VERSION,
  };
  const storedV3Genesis = rotationHead(runtimeGenesis);
  await assert.rejects(
    pool.query(
      rawHeadInsertQuery,
      rawHeadInsertValues(
        progressValidationAnchorId,
        storedV3Genesis,
        storedV3Genesis.stateRevision,
      ),
    ),
    (error) => {
      assert.equal(error.code, "55000");
      assert.equal(error.constraint, "fs_image_head_initial_progress");
      return true;
    },
  );
  const storedV2Genesis = rotationHead(genesis);
  await assert.rejects(
    pool.query(
      rawHeadInsertQuery,
      rawHeadInsertValues(
        progressValidationAnchorId,
        storedV2Genesis,
        null,
      ),
    ),
    (error) => {
      assert.equal(error.code, "55000");
      assert.equal(error.constraint, "fs_image_head_initial_progress");
      return true;
    },
  );
  const progressPreparedHead = appendHead(
    runtimeGenesis,
    "9".repeat(64),
    512,
  );
  const missingInitialEventClient = await pool.connect();
  let missingInitialEventOpen = false;
  try {
    await missingInitialEventClient.query("BEGIN");
    missingInitialEventOpen = true;
    assert.equal(
      (
        await missingInitialEventClient.query(
          rawHeadInsertQuery,
          rawHeadInsertValues(
            progressValidationAnchorId,
            progressPreparedHead,
            progressPreparedHead.stateRevision,
          ),
        )
      ).rowCount,
      1,
    );
    await assert.rejects(
      missingInitialEventClient.query(
        "SET CONSTRAINTS session_authority.fs_image_heads_progress_complete IMMEDIATE",
      ),
      (error) => {
        assert.equal(error.code, "23514");
        assert.equal(error.constraint, "fs_image_head_append_event");
        return true;
      },
    );
    await missingInitialEventClient.query("ROLLBACK");
    missingInitialEventOpen = false;
  } finally {
    if (missingInitialEventOpen) {
      await missingInitialEventClient.query("ROLLBACK");
    }
    missingInitialEventClient.release();
  }
  const skippedInitialHead = appendHead(
    progressPreparedHead,
    "a".repeat(64),
    1024,
  );
  await assert.rejects(
    pool.query(
      rawHeadInsertQuery,
      rawHeadInsertValues(
        progressValidationAnchorId,
        skippedInitialHead,
        skippedInitialHead.stateRevision,
      ),
    ),
    (error) => {
      assert.equal(error.code, "55000");
      assert.equal(error.constraint, "fs_image_head_initial_progress");
      return true;
    },
  );
  const progressOperationId =
    `provider-operation-progress-${randomUUID()}`;
  const progressPrepared = preparedRecord({
    checksum: progressPreparedHead.lastChecksum,
    operationId: progressOperationId,
    revision: progressPreparedHead.stateRevision,
    storageId: `provider-storage-progress-${randomUUID()}`,
  });
  const progressRuntimeAuthority = createRuntimeAuthority(
    store,
    progressValidationAnchorId,
  );
  assert.equal(
    await progressRuntimeAuthority.compareAndAdvance({
      expectedHead: runtimeGenesis,
      nextHead: progressPreparedHead,
      transition: {
        contractVersion: 1,
        type: "append-prepared-v1",
        frameChecksum: progressPreparedHead.lastChecksum,
        record: progressPrepared,
      },
    }),
    true,
  );
  const progressCommittedHead = appendHead(
    progressPreparedHead,
    "b".repeat(64),
    1024,
  );
  const progressCommitted = committedRecord(
    progressPrepared,
    progressCommittedHead.stateRevision,
  );
  assert.equal(
    await progressRuntimeAuthority.compareAndAdvance({
      expectedHead: progressPreparedHead,
      nextHead: progressCommittedHead,
      transition: {
        contractVersion: 1,
        type: "append-committed-v1",
        frameChecksum: progressCommittedHead.lastChecksum,
        record: progressCommitted,
      },
    }),
    true,
  );
  const progressRotatedHead = rotationHead(progressCommittedHead);
  assert.equal(
    await progressRuntimeAuthority.compareAndAdvance({
      expectedHead: progressCommittedHead,
      nextHead: progressRotatedHead,
      transition: { contractVersion: 1, type: "rotate-v1" },
    }),
    true,
  );
  const progressMissingEventHead = appendHead(
    progressRotatedHead,
    "c".repeat(64),
    512,
  );
  const missingLaterEventClient = await pool.connect();
  let missingLaterEventOpen = false;
  try {
    await missingLaterEventClient.query("BEGIN");
    missingLaterEventOpen = true;
    assert.equal(
      (
        await missingLaterEventClient.query(
          rawHeadUpdateQuery,
          rawHeadInsertValues(
            progressValidationAnchorId,
            progressMissingEventHead,
            progressMissingEventHead.stateRevision,
          ),
        )
      ).rowCount,
      1,
    );
    await assert.rejects(
      missingLaterEventClient.query(
        "SET CONSTRAINTS session_authority.fs_image_heads_progress_complete IMMEDIATE",
      ),
      (error) => {
        assert.equal(error.code, "23514");
        assert.equal(error.constraint, "fs_image_head_append_event");
        return true;
      },
    );
    await missingLaterEventClient.query("ROLLBACK");
    missingLaterEventOpen = false;
  } finally {
    if (missingLaterEventOpen) {
      await missingLaterEventClient.query("ROLLBACK");
    }
    missingLaterEventClient.release();
  }
  const progressSkippedHead = appendHead(
    progressMissingEventHead,
    "d".repeat(64),
    1024,
  );
  await assert.rejects(
    pool.query(
      rawHeadUpdateQuery,
      rawHeadInsertValues(
        progressValidationAnchorId,
        progressSkippedHead,
        progressSkippedHead.stateRevision,
      ),
    ),
    (error) => {
      assert.equal(error.code, "55000");
      assert.equal(error.constraint, "fs_image_head_incremental_progress");
      return true;
    },
  );
  assert.deepEqual(
    await progressRuntimeAuthority.readHead(),
    progressRotatedHead,
  );
  const progressFencedHead = appendHead(
    progressRotatedHead,
    "f".repeat(64),
    512,
  );
  const progressFencedPrepared = preparedRecord({
    checksum: progressFencedHead.lastChecksum,
    operationId: `provider-operation-progress-fence-${randomUUID()}`,
    revision: progressFencedHead.stateRevision,
    storageId: `provider-storage-progress-fence-${randomUUID()}`,
  });
  const progressFencedMaterial = operationMaterial(progressFencedPrepared);
  const progressFenceClient = await pool.connect();
  let progressFenceOpen = false;
  try {
    await progressFenceClient.query("BEGIN");
    progressFenceOpen = true;
    assert.equal(
      (
        await progressFenceClient.query(
          rawHeadUpdateQuery,
          rawHeadInsertValues(
            progressValidationAnchorId,
            progressFencedHead,
            progressFencedHead.stateRevision,
          ),
        )
      ).rowCount,
      1,
    );
    assert.equal(
      (
        await progressFenceClient.query(
          [
            "INSERT INTO session_authority.filesystem_image_provider_operations",
            "(provider_id, anchor_id, operation_id, record_contract_version, state,",
            "kind, storage_id, prepared_state_revision, prepared_checksum,",
            "prepared_record_bytes, prepared_record_sha256)",
            "VALUES ($1, $2, $3, 1, 'prepared', $4, $5,",
            "$6::pg_catalog.numeric, $7, $8, $9)",
          ].join(" "),
          [
            providerId,
            progressValidationAnchorId,
            progressFencedPrepared.operationId,
            progressFencedPrepared.kind,
            progressFencedPrepared.storageId,
            progressFencedPrepared.preparedStateRevision,
            progressFencedPrepared.preparedChecksum,
            progressFencedMaterial.bytes,
            progressFencedMaterial.sha256,
          ],
        )
      ).rowCount,
      1,
    );
    await progressFenceClient.query(
      "SET CONSTRAINTS session_authority.fs_image_heads_progress_complete IMMEDIATE",
    );
    const progressFencedRotation = rotationHead(progressFencedHead);
    await assert.rejects(
      progressFenceClient.query(
        rawHeadUpdateQuery,
        rawHeadInsertValues(
          progressValidationAnchorId,
          progressFencedRotation,
          progressFencedRotation.stateRevision,
        ),
      ),
      (error) => {
        assert.equal(error.code, "55000");
        assert.equal(error.constraint, "fs_image_head_same_xact_update");
        return true;
      },
    );
    await progressFenceClient.query("ROLLBACK");
    progressFenceOpen = false;
  } finally {
    if (progressFenceOpen) {
      await progressFenceClient.query("ROLLBACK");
    }
    progressFenceClient.release();
  }
  assert.deepEqual(
    await progressRuntimeAuthority.readHead(),
    progressRotatedHead,
  );
  const progressTeardownClient = await pool.connect();
  let progressTeardownOpen = false;
  try {
    await progressTeardownClient.query("BEGIN");
    progressTeardownOpen = true;
    assert.equal(
      (
        await progressTeardownClient.query(
          [
            "DELETE FROM session_authority.filesystem_image_provider_operations",
            "WHERE provider_id = $1 AND anchor_id = $2",
          ].join(" "),
          [providerId, progressValidationAnchorId],
        )
      ).rowCount,
      1,
    );
    assert.equal(
      (
        await progressTeardownClient.query(
          [
            "DELETE FROM session_authority.filesystem_image_provider_heads",
            "WHERE provider_id = $1 AND anchor_id = $2",
          ].join(" "),
          [providerId, progressValidationAnchorId],
        )
      ).rowCount,
      1,
    );
    await progressTeardownClient.query("SET CONSTRAINTS ALL IMMEDIATE");
    await progressTeardownClient.query("COMMIT");
    progressTeardownOpen = false;
  } finally {
    if (progressTeardownOpen) {
      await progressTeardownClient.query("ROLLBACK");
    }
    progressTeardownClient.release();
  }
  assert.deepEqual(
    (
      await pool.query(
        [
          "SELECT retired_xid IS NOT NULL AS retired,",
          "(SELECT pg_catalog.count(*) FROM",
          "session_authority.filesystem_image_provider_heads AS head",
          "WHERE head.provider_id = lifecycle.provider_id",
          "AND head.anchor_id = lifecycle.anchor_id) AS head_count,",
          "(SELECT pg_catalog.count(*) FROM",
          "session_authority.filesystem_image_provider_operations AS operation",
          "WHERE operation.provider_id = lifecycle.provider_id",
          "AND operation.anchor_id = lifecycle.anchor_id) AS operation_count,",
          "(SELECT pg_catalog.count(*) FROM",
          "session_authority.filesystem_image_provider_operation_events AS event",
          "WHERE event.provider_id = lifecycle.provider_id",
          "AND event.anchor_id = lifecycle.anchor_id) AS event_count",
          "FROM session_authority.filesystem_image_provider_anchor_lifecycle AS lifecycle",
          "WHERE lifecycle.provider_id = $1 AND lifecycle.anchor_id = $2",
        ].join(" "),
        [providerId, progressValidationAnchorId],
      )
    ).rows,
    [
      {
        event_count: "0",
        head_count: "0",
        operation_count: "0",
        retired: true,
      },
    ],
  );
  const revisionRacePreparedB = preparedRecord({
    checksum: "6".repeat(64),
    operationId: `provider-operation-revision-race-b-${randomUUID()}`,
    revision: "1",
    storageId: `provider-storage-revision-race-b-${randomUUID()}`,
  });
  const revisionRacePreparedA = preparedRecord({
    checksum: "8".repeat(64),
    operationId: `provider-operation-revision-race-a-${randomUUID()}`,
    revision: "3",
    storageId: `provider-storage-revision-race-a-${randomUUID()}`,
  });
  const revisionRaceCommittedB = committedRecord(
    revisionRacePreparedB,
    "3",
  );
  const revisionRacePreparedBMaterial = operationMaterial(
    revisionRacePreparedB,
  );
  const revisionRacePreparedAMaterial = operationMaterial(
    revisionRacePreparedA,
  );
  const revisionRaceCommittedBMaterial = operationMaterial(
    revisionRaceCommittedB,
  );
  const revisionRacePreparedInsertQuery = [
    "INSERT INTO session_authority.filesystem_image_provider_operations",
    "(provider_id, anchor_id, operation_id, record_contract_version, state,",
    "kind, storage_id, prepared_state_revision, prepared_checksum,",
    "prepared_record_bytes, prepared_record_sha256)",
    "VALUES ($1, $2, $3, 1, 'prepared', $4, $5,",
    "$6::pg_catalog.numeric, $7, $8, $9)",
  ].join(" ");
  const revisionRacePreparedValues = (record, material) => [
    providerId,
    revisionRaceAnchorId,
    record.operationId,
    record.kind,
    record.storageId,
    record.preparedStateRevision,
    record.preparedChecksum,
    material.bytes,
    material.sha256,
  ];
  assert.equal(
    (
      await pool.query(
        revisionRacePreparedInsertQuery,
        revisionRacePreparedValues(
          revisionRacePreparedB,
          revisionRacePreparedBMaterial,
        ),
      )
    ).rowCount,
    1,
  );
  const revisionClaimClient = await pool.connect();
  const revisionCollisionClient = await pool.connect();
  let revisionClaimOpen = false;
  let revisionCollisionOpen = false;
  let revisionCollisionOutcome;
  try {
    await revisionClaimClient.query("BEGIN");
    revisionClaimOpen = true;
    assert.equal(
      (
        await revisionClaimClient.query(
          revisionRacePreparedInsertQuery,
          revisionRacePreparedValues(
            revisionRacePreparedA,
            revisionRacePreparedAMaterial,
          ),
        )
      ).rowCount,
      1,
    );

    await revisionCollisionClient.query("BEGIN");
    revisionCollisionOpen = true;
    await revisionCollisionClient.query("SET LOCAL lock_timeout = '5s'");
    const revisionCollisionBackendPid = (
      await revisionCollisionClient.query(
        "SELECT pg_catalog.pg_backend_pid() AS backend_pid",
      )
    ).rows[0].backend_pid;
    revisionCollisionOutcome = revisionCollisionClient
      .query(
        [
          "UPDATE session_authority.filesystem_image_provider_operations",
          "SET state = 'committed', committed_state_revision = $4::pg_catalog.numeric,",
          "committed_checksum_provenance = 'indexed-frame-v1',",
          "committed_checksum = $5, committed_record_bytes = $6,",
          "committed_record_sha256 = $7",
          "WHERE provider_id = $1 AND anchor_id = $2 AND operation_id = $3",
        ].join(" "),
        [
          providerId,
          revisionRaceAnchorId,
          revisionRacePreparedB.operationId,
          revisionRaceCommittedB.committedStateRevision,
          revisionRaceHead.lastChecksum,
          revisionRaceCommittedBMaterial.bytes,
          revisionRaceCommittedBMaterial.sha256,
        ],
      )
      .then(
        (value) => ({ status: "fulfilled", value }),
        (error) => ({ error, status: "rejected" }),
      );
    await waitForBackendLockWait(
      revisionClaimClient,
      revisionCollisionBackendPid,
    );
    await revisionClaimClient.query("COMMIT");
    revisionClaimOpen = false;
    const outcome = await revisionCollisionOutcome;
    assert.equal(outcome.status, "rejected");
    assert.equal(outcome.error.code, "23505");
    assert.equal(
      outcome.error.constraint,
      "fs_image_operation_events_revision_pkey",
    );
    await revisionCollisionClient.query("ROLLBACK");
    revisionCollisionOpen = false;
  } finally {
    if (revisionClaimOpen) await revisionClaimClient.query("ROLLBACK");
    if (revisionCollisionOutcome !== undefined) {
      await revisionCollisionOutcome;
    }
    if (revisionCollisionOpen) {
      await revisionCollisionClient.query("ROLLBACK");
    }
    revisionClaimClient.release();
    revisionCollisionClient.release();
  }
  assert.deepEqual(
    (
      await pool.query(
        [
          "SELECT operation_id, phase, event_revision::pg_catalog.text",
          "FROM session_authority.filesystem_image_provider_operation_events",
          "WHERE provider_id = $1 AND anchor_id = $2",
          "ORDER BY event_revision",
        ].join(" "),
        [providerId, revisionRaceAnchorId],
      )
    ).rows,
    [
      {
        operation_id: revisionRacePreparedB.operationId,
        phase: "prepared",
        event_revision: "1",
      },
      {
        operation_id: revisionRacePreparedA.operationId,
        phase: "prepared",
        event_revision: "3",
      },
    ],
  );
  await assert.rejects(
    pool.query(
      [
        "INSERT INTO session_authority.filesystem_image_provider_operation_events",
        "(provider_id, anchor_id, operation_id, phase, event_revision)",
        "VALUES ($1, $2, $3, 'committed', 2)",
      ].join(" "),
      [providerId, revisionRaceAnchorId, revisionRacePreparedB.operationId],
    ),
    (error) => {
      assert.equal(error.code, "55000");
      assert.equal(error.constraint, "fs_image_operation_events_immutable");
      return true;
    },
  );
  for (const statement of [
    [
      "UPDATE session_authority.filesystem_image_provider_operation_events",
      "SET event_revision = event_revision",
      "WHERE provider_id = $1 AND anchor_id = $2 AND event_revision = 3",
    ].join(" "),
    [
      "DELETE FROM session_authority.filesystem_image_provider_operation_events",
      "WHERE provider_id = $1 AND anchor_id = $2 AND event_revision = 3",
    ].join(" "),
  ]) {
    await assert.rejects(
      pool.query(statement, [providerId, revisionRaceAnchorId]),
      (error) => {
        assert.equal(error.code, "55000");
        assert.equal(error.constraint, "fs_image_operation_events_immutable");
        return true;
      },
    );
  }
  const operationIndexMarkerColumn = await pool.query(
    [
      "SELECT pg_catalog.format_type(attribute.atttypid, attribute.atttypmod)",
      "AS data_type, attribute.attnotnull",
      "FROM pg_catalog.pg_attribute AS attribute",
      "JOIN pg_catalog.pg_class AS relation ON relation.oid = attribute.attrelid",
      "JOIN pg_catalog.pg_namespace AS namespace",
      "ON namespace.oid = relation.relnamespace",
      "WHERE namespace.nspname = 'session_authority'",
      "AND relation.relname = 'filesystem_image_provider_heads'",
      "AND attribute.attname = 'operation_index_state_revision'",
      "AND attribute.attnum > 0 AND attribute.attisdropped = false",
    ].join(" "),
  );
  assert.deepEqual(operationIndexMarkerColumn.rows, [
    { data_type: "numeric(20,0)", attnotnull: false },
  ]);
  const legacyAfter = await pool.query(
    [
      "SELECT contract_version, anchor_revision::pg_catalog.text AS anchor_revision,",
      "state_revision::pg_catalog.text AS state_revision, last_checksum",
      "FROM session_authority.filesystem_image_provider_heads",
      "WHERE provider_id = $1 AND anchor_id = $2",
    ].join(" "),
    [providerId, legacyAnchorId],
  );
  assert.deepEqual(legacyAfter.rows, legacyBefore.rows);
  assert.deepEqual(legacyAfter.rows, [
    {
      contract_version: 2,
      anchor_revision: "1",
      state_revision: "1",
      last_checksum: "1".repeat(64),
    },
  ]);
  await assert.rejects(
    pool.query(
      [
        "UPDATE session_authority.filesystem_image_provider_heads",
        "SET last_checksum = $3",
        "WHERE provider_id = $1 AND anchor_id = $2",
      ].join(" "),
      [providerId, legacyAnchorId, "a".repeat(63)],
    ),
    (error) => {
      assert.equal(error.code, "23514");
      assert.equal(
        error.constraint,
        "filesystem_image_provider_heads_last_checksum_format",
      );
      return true;
    },
  );
  const legacyAfterRejectedShortChecksum = await pool.query(
    [
      "SELECT contract_version, anchor_revision::pg_catalog.text AS anchor_revision,",
      "state_revision::pg_catalog.text AS state_revision, last_checksum",
      "FROM session_authority.filesystem_image_provider_heads",
      "WHERE provider_id = $1 AND anchor_id = $2",
    ].join(" "),
    [providerId, legacyAnchorId],
  );
  assert.deepEqual(legacyAfterRejectedShortChecksum.rows, legacyAfter.rows);
  const migratedMarkerRows = await pool.query(
    [
      "SELECT anchor_id,",
      "operation_index_state_revision::pg_catalog.text",
      "AS operation_index_state_revision",
      "FROM session_authority.filesystem_image_provider_heads",
      "WHERE provider_id = $1 AND anchor_id IN ($2, $3)",
      'ORDER BY anchor_id COLLATE pg_catalog."C"',
    ].join(" "),
    [providerId, legacyAnchorId, adoptedAnchorId],
  );
  assert.deepEqual(
    migratedMarkerRows.rows,
    [legacyAnchorId, adoptedAnchorId]
      .sort()
      .map((selectedAnchorId) => ({
        anchor_id: selectedAnchorId,
        operation_index_state_revision: null,
      })),
  );
  const checksumColumnTypes = await pool.query(
    [
      "SELECT relation.relname AS relation_name,",
      "attribute.attname AS column_name,",
      "pg_catalog.format_type(attribute.atttypid, attribute.atttypmod)",
      "AS data_type",
      "FROM pg_catalog.pg_attribute AS attribute",
      "JOIN pg_catalog.pg_class AS relation ON relation.oid = attribute.attrelid",
      "JOIN pg_catalog.pg_namespace AS namespace",
      "ON namespace.oid = relation.relnamespace",
      "WHERE namespace.nspname = 'session_authority'",
      "AND (",
      "(relation.relname = 'filesystem_image_provider_heads'",
      "AND attribute.attname IN (",
      "'base_head_checksum', 'checkpoint_checksum', 'last_checksum'",
      ")) OR",
      "(relation.relname = 'filesystem_image_provider_operations'",
      "AND attribute.attname IN (",
      "'prepared_checksum', 'prepared_record_sha256',",
      "'committed_checksum', 'committed_record_sha256'",
      ")))",
      "AND attribute.attnum > 0 AND attribute.attisdropped = false",
      "ORDER BY relation.relname COLLATE pg_catalog.\"C\",",
      "attribute.attname COLLATE pg_catalog.\"C\"",
    ].join(" "),
  );
  assert.deepEqual(checksumColumnTypes.rows, [
    {
      relation_name: "filesystem_image_provider_heads",
      column_name: "base_head_checksum",
      data_type: "character varying(64)",
    },
    {
      relation_name: "filesystem_image_provider_heads",
      column_name: "checkpoint_checksum",
      data_type: "character varying(64)",
    },
    {
      relation_name: "filesystem_image_provider_heads",
      column_name: "last_checksum",
      data_type: "character varying(64)",
    },
    {
      relation_name: "filesystem_image_provider_operations",
      column_name: "committed_checksum",
      data_type: "character varying(64)",
    },
    {
      relation_name: "filesystem_image_provider_operations",
      column_name: "committed_record_sha256",
      data_type: "character varying(64)",
    },
    {
      relation_name: "filesystem_image_provider_operations",
      column_name: "prepared_checksum",
      data_type: "character varying(64)",
    },
    {
      relation_name: "filesystem_image_provider_operations",
      column_name: "prepared_record_sha256",
      data_type: "character varying(64)",
    },
  ]);
  const operationIndexes = await pool.query(
    [
      "SELECT indexname",
      "FROM pg_catalog.pg_indexes",
      "WHERE schemaname = 'session_authority'",
      "AND tablename = 'filesystem_image_provider_operations'",
      "ORDER BY indexname COLLATE pg_catalog.\"C\"",
    ].join(" "),
  );
  assert.deepEqual(operationIndexes.rows, [
    {
      indexname:
        "filesystem_image_provider_operations_committed_storage_tail_idx",
    },
    { indexname: "filesystem_image_provider_operations_one_prepared_storage" },
    { indexname: "filesystem_image_provider_operations_pkey" },
    { indexname: "filesystem_image_provider_operations_prepared_revision_idx" },
    { indexname: "filesystem_image_provider_operations_state_storage_idx" },
    { indexname: "fs_image_operations_committed_revision_uniq" },
    { indexname: "fs_image_operations_prepared_revision_uniq" },
  ]);
  const committedStorageTailIndex = await pool.query(
    [
      "SELECT index_catalog.indisunique, index_catalog.indnkeyatts,",
      "index_catalog.indpred IS NOT NULL AS has_predicate,",
      "pg_catalog.pg_get_expr(",
      "index_catalog.indpred, index_catalog.indrelid, true) AS predicate",
      "FROM pg_catalog.pg_index AS index_catalog",
      "JOIN pg_catalog.pg_class AS index_relation",
      "ON index_relation.oid = index_catalog.indexrelid",
      "JOIN pg_catalog.pg_namespace AS namespace",
      "ON namespace.oid = index_relation.relnamespace",
      "WHERE namespace.nspname = 'session_authority'",
      "AND index_relation.relname =",
      "'filesystem_image_provider_operations_committed_storage_tail_idx'",
    ].join(" "),
  );
  assert.equal(committedStorageTailIndex.rowCount, 1);
  assert.equal(committedStorageTailIndex.rows[0].indisunique, false);
  assert.equal(committedStorageTailIndex.rows[0].indnkeyatts, 5);
  assert.equal(committedStorageTailIndex.rows[0].has_predicate, true);
  assert.equal(
    committedStorageTailIndex.rows[0].predicate.includes("state"),
    true,
  );
  assert.equal(
    committedStorageTailIndex.rows[0].predicate.includes("committed"),
    true,
  );
  const operationTriggers = await pool.query(
    [
      "SELECT trigger.tgname, trigger.tgdeferrable, trigger.tginitdeferred",
      "FROM pg_catalog.pg_trigger AS trigger",
      "JOIN pg_catalog.pg_class AS relation ON relation.oid = trigger.tgrelid",
      "JOIN pg_catalog.pg_namespace AS namespace",
      "ON namespace.oid = relation.relnamespace",
      "WHERE namespace.nspname = 'session_authority'",
      "AND relation.relname = 'filesystem_image_provider_operations'",
      "AND trigger.tgisinternal = false",
      "ORDER BY trigger.tgname COLLATE pg_catalog.\"C\"",
    ].join(" "),
  );
  assert.deepEqual(operationTriggers.rows, [
    {
      tgname: "filesystem_image_provider_operations_delete_guard",
      tgdeferrable: true,
      tginitdeferred: true,
    },
    {
      tgname: "filesystem_image_provider_operations_insert_guard",
      tgdeferrable: false,
      tginitdeferred: false,
    },
    {
      tgname: "filesystem_image_provider_operations_truncate_guard",
      tgdeferrable: false,
      tginitdeferred: false,
    },
    {
      tgname: "filesystem_image_provider_operations_update_guard",
      tgdeferrable: false,
      tginitdeferred: false,
    },
    {
      tgname: "fs_image_operations_event_claim",
      tgdeferrable: false,
      tginitdeferred: false,
    },
  ]);

  const assertStateInvalid = (error) => {
    assert.equal(
      error instanceof PostgresFilesystemImageProviderStateAuthorityError,
      true,
    );
    assert.equal(
      error.code,
      "postgres_filesystem_image_provider_state_authority_state_invalid",
    );
    return true;
  };
  const readProviderAnchorSnapshot = async (selectedAnchorId) => {
    const head = await pool.query(
      [
        "SELECT contract_version, anchor_revision::pg_catalog.text,",
        "generation::pg_catalog.text, state_revision::pg_catalog.text,",
        "base_head_checksum, checkpoint_state_revision::pg_catalog.text,",
        "checkpoint_frame_count::pg_catalog.text, checkpoint_checksum,",
        "checkpoint_bytes::pg_catalog.text, frame_count::pg_catalog.text,",
        "last_checksum, ledger_bytes::pg_catalog.text,",
        "operation_index_state_revision::pg_catalog.text",
        "FROM session_authority.filesystem_image_provider_heads",
        "WHERE provider_id = $1 AND anchor_id = $2",
      ].join(" "),
      [providerId, selectedAnchorId],
    );
    const operations = await pool.query(
      [
        "SELECT operation_id, record_contract_version, state, kind, storage_id,",
        "prepared_state_revision::pg_catalog.text, prepared_checksum,",
        "pg_catalog.encode(prepared_record_bytes, 'hex') AS prepared_record_hex,",
        "prepared_record_sha256, committed_state_revision::pg_catalog.text,",
        "committed_checksum_provenance, committed_checksum,",
        "pg_catalog.encode(committed_record_bytes, 'hex') AS committed_record_hex,",
        "committed_record_sha256",
        "FROM session_authority.filesystem_image_provider_operations",
        "WHERE provider_id = $1 AND anchor_id = $2",
        'ORDER BY operation_id COLLATE pg_catalog."C"',
      ].join(" "),
      [providerId, selectedAnchorId],
    );
    return { head: head.rows, operations: operations.rows };
  };

  const legacyAuthority = createAuthority(store, legacyAnchorId);
  assert.deepEqual(await legacyAuthority.readHead(), legacyHead);
  const legacySnapshot = await readProviderAnchorSnapshot(legacyAnchorId);
  const legacyOperationId = `provider-operation-legacy-${randomUUID()}`;
  const legacyNextHead = appendHead(legacyHead, "a".repeat(64), 1024);
  const legacyPrepared = preparedRecord({
    checksum: legacyNextHead.lastChecksum,
    operationId: legacyOperationId,
    revision: legacyNextHead.stateRevision,
    storageId: `provider-storage-legacy-${randomUUID()}`,
  });
  await assert.rejects(
    legacyAuthority.readOperation({
      expectedHead: legacyHead,
      operationId: legacyOperationId,
    }),
    assertStateInvalid,
  );
  await assert.rejects(
    legacyAuthority.readOperationsPage({
      afterOperationId: null,
      expectedHead: legacyHead,
      limit: 1,
    }),
    assertStateInvalid,
  );
  await assert.rejects(
    legacyAuthority.compareAndAdvance({
      expectedHead: legacyHead,
      nextHead: legacyNextHead,
      transition: {
        contractVersion: 1,
        type: "append-prepared-v1",
        frameChecksum: legacyNextHead.lastChecksum,
        record: legacyPrepared,
      },
    }),
    assertStateInvalid,
  );
  assert.deepEqual(
    await readProviderAnchorSnapshot(legacyAnchorId),
    legacySnapshot,
  );

  const authority = createAuthority(store, anchorId);
  assert.deepEqual(await authority.readHead(), genesis);
  const operationZ = `provider-operation-z-${randomUUID()}`;
  const preparedZHead = appendHead(genesis, "a".repeat(64), 512);
  const preparedZ = preparedRecord({
    checksum: preparedZHead.lastChecksum,
    operationId: operationZ,
    revision: preparedZHead.stateRevision,
    storageId: `provider-storage-z-${randomUUID()}`,
  });
  const committedZHead = appendHead(preparedZHead, "b".repeat(64), 1536);
  const committedZ = committedRecord(
    preparedZ,
    committedZHead.stateRevision,
  );
  const committedZMaterial = operationMaterial(committedZ);
  assert.equal(
    await authority.compareAndAdvance({
      expectedHead: genesis,
      nextHead: preparedZHead,
      transition: {
        contractVersion: 1,
        type: "append-prepared-v1",
        frameChecksum: preparedZHead.lastChecksum,
        record: preparedZ,
      },
    }),
    true,
  );
  const preparedZSnapshot = await readProviderAnchorSnapshot(anchorId);
  assert.equal(
    preparedZSnapshot.head[0].operation_index_state_revision,
    preparedZHead.stateRevision,
  );
  assert.deepEqual(
    preparedZSnapshot.operations.map(({ operation_id, state }) => ({
      operation_id,
      state,
    })),
    [{ operation_id: operationZ, state: "prepared" }],
  );
  assert.deepEqual(
    await authority.readOperation({
      expectedHead: preparedZHead,
      operationId: operationZ,
    }),
    preparedZ,
  );
  const readPreparedZStored = async () =>
    pool.query(
      [
        "SELECT operation_id, record_contract_version, state, kind, storage_id,",
        "prepared_state_revision::pg_catalog.text, prepared_checksum,",
        "prepared_record_bytes, prepared_record_sha256,",
        "committed_state_revision::pg_catalog.text,",
        "committed_checksum_provenance, committed_checksum,",
        "committed_record_bytes, committed_record_sha256",
        "FROM session_authority.filesystem_image_provider_operations",
        "WHERE provider_id = $1 AND anchor_id = $2 AND operation_id = $3",
      ].join(" "),
      [providerId, anchorId, operationZ],
    );
  const preparedZStored = await readPreparedZStored();
  assert.equal(preparedZStored.rowCount, 1);
  const assertPreparedZStoredUnchanged = async () => {
    assert.deepEqual((await readPreparedZStored()).rows, preparedZStored.rows);
  };
  // A native commit advances the head before it appends the operation suffix.
  // Reproduce that ordering so malformed suffixes reach the table CHECK.
  const assertCommittedShapeRejected = async (query, values) => {
    const client = await pool.connect();
    let transactionOpen = false;
    try {
      await client.query("BEGIN");
      transactionOpen = true;
      assert.equal(
        (
          await client.query(
            rawHeadUpdateQuery,
            rawHeadInsertValues(
              anchorId,
              committedZHead,
              committedZHead.stateRevision,
            ),
          )
        ).rowCount,
        1,
      );
      await assert.rejects(client.query(query, values), (error) => {
        assert.equal(error.code, "23514");
        assert.equal(
          error.constraint,
          "fs_image_operations_committed_shape",
        );
        return true;
      });
      await client.query("ROLLBACK");
      transactionOpen = false;
    } finally {
      if (transactionOpen) await client.query("ROLLBACK");
      client.release();
    }
    assert.deepEqual(
      await readProviderAnchorSnapshot(anchorId),
      preparedZSnapshot,
    );
  };
  await assert.rejects(
    pool.query(
      [
        "INSERT INTO session_authority.filesystem_image_provider_operations",
        "(provider_id, anchor_id, operation_id, record_contract_version,",
        "state, kind, storage_id, prepared_state_revision, prepared_checksum,",
        "prepared_record_bytes, prepared_record_sha256)",
        "SELECT provider_id, anchor_id, $4, record_contract_version, state,",
        "kind, $5, prepared_state_revision, $6, prepared_record_bytes,",
        "prepared_record_sha256",
        "FROM session_authority.filesystem_image_provider_operations",
        "WHERE provider_id = $1 AND anchor_id = $2 AND operation_id = $3",
      ].join(" "),
      [
        providerId,
        anchorId,
        operationZ,
        `provider-operation-short-prepared-checksum-${randomUUID()}`,
        `provider-storage-short-prepared-checksum-${randomUUID()}`,
        "a".repeat(63),
      ],
    ),
    (error) => {
      assert.equal(error.code, "23514");
      assert.equal(
        error.constraint,
        "filesystem_image_provider_operations_prepared_checksum_format",
      );
      return true;
    },
  );
  await assertPreparedZStoredUnchanged();
  await assert.rejects(
    pool.query(
      [
        "INSERT INTO session_authority.filesystem_image_provider_operations",
        "(provider_id, anchor_id, operation_id, record_contract_version,",
        "state, kind, storage_id, prepared_state_revision, prepared_checksum,",
        "prepared_record_bytes, prepared_record_sha256)",
        "SELECT provider_id, anchor_id, $4, record_contract_version, state,",
        "kind, $5, prepared_state_revision, prepared_checksum,",
        "prepared_record_bytes, $6",
        "FROM session_authority.filesystem_image_provider_operations",
        "WHERE provider_id = $1 AND anchor_id = $2 AND operation_id = $3",
      ].join(" "),
      [
        providerId,
        anchorId,
        operationZ,
        `provider-operation-short-prepared-sha256-${randomUUID()}`,
        `provider-storage-short-prepared-sha256-${randomUUID()}`,
        "b".repeat(63),
      ],
    ),
    (error) => {
      assert.equal(error.code, "23514");
      assert.equal(
        error.constraint,
        "filesystem_image_provider_operations_prepared_sha256_format",
      );
      return true;
    },
  );
  await assertPreparedZStoredUnchanged();
  await assertCommittedShapeRejected(
    [
      "UPDATE session_authority.filesystem_image_provider_operations",
      "SET state = 'committed',",
      "committed_state_revision = prepared_state_revision + 1,",
      "committed_checksum_provenance = 'indexed-frame-v1',",
      "committed_checksum = $4, committed_record_bytes = $5,",
      "committed_record_sha256 = $6",
      "WHERE provider_id = $1 AND anchor_id = $2 AND operation_id = $3",
    ].join(" "),
    [
      providerId,
      anchorId,
      operationZ,
      "c".repeat(63),
      committedZMaterial.bytes,
      committedZMaterial.sha256,
    ],
  );
  await assertPreparedZStoredUnchanged();
  await assertCommittedShapeRejected(
    [
      "UPDATE session_authority.filesystem_image_provider_operations",
      "SET state = 'committed',",
      "committed_state_revision = prepared_state_revision + 1,",
      "committed_checksum_provenance = 'indexed-frame-v1',",
      "committed_checksum = $4, committed_record_bytes = $5,",
      "committed_record_sha256 = $6",
      "WHERE provider_id = $1 AND anchor_id = $2 AND operation_id = $3",
    ].join(" "),
    [
      providerId,
      anchorId,
      operationZ,
      committedZHead.lastChecksum,
      committedZMaterial.bytes,
      "d".repeat(63),
    ],
  );
  await assertPreparedZStoredUnchanged();
  await assert.rejects(
    pool.query(
      [
        "INSERT INTO session_authority.filesystem_image_provider_operations",
        "(provider_id, anchor_id, operation_id, record_contract_version,",
        "state, kind, storage_id, prepared_state_revision, prepared_checksum,",
        "prepared_record_bytes, prepared_record_sha256,",
        "committed_state_revision, committed_checksum,",
        "committed_record_bytes, committed_record_sha256)",
        "SELECT provider_id, anchor_id, $4, record_contract_version,",
        "'committed', kind, storage_id, prepared_state_revision,",
        "prepared_checksum, prepared_record_bytes, prepared_record_sha256,",
        "prepared_state_revision + 1, prepared_checksum,",
        "prepared_record_bytes, prepared_record_sha256",
        "FROM session_authority.filesystem_image_provider_operations",
        "WHERE provider_id = $1 AND anchor_id = $2 AND operation_id = $3",
      ].join(" "),
      [
        providerId,
        anchorId,
        operationZ,
        `provider-operation-direct-committed-${randomUUID()}`,
      ],
    ),
    (error) => {
      assert.equal(error.code, "55000");
      assert.equal(
        error.constraint,
        "fs_image_operations_insert_path",
      );
      return true;
    },
  );
  await assert.rejects(
    pool.query(
      [
        "INSERT INTO session_authority.filesystem_image_provider_operations",
        "(provider_id, anchor_id, operation_id, record_contract_version,",
        "state, kind, storage_id, prepared_state_revision, prepared_checksum,",
        "prepared_record_bytes, prepared_record_sha256)",
        "SELECT provider_id, anchor_id, $4, record_contract_version, state,",
        "kind, storage_id, prepared_state_revision, prepared_checksum,",
        "prepared_record_bytes, prepared_record_sha256",
        "FROM session_authority.filesystem_image_provider_operations",
        "WHERE provider_id = $1 AND anchor_id = $2 AND operation_id = $3",
      ].join(" "),
      [
        providerId,
        anchorId,
        operationZ,
        `provider-operation-same-storage-${randomUUID()}`,
      ],
    ),
    (error) => {
      assert.equal(error.code, "23505");
      assert.equal(
        error.constraint,
        "filesystem_image_provider_operations_one_prepared_storage",
      );
      return true;
    },
  );
  await assert.rejects(
    pool.query(
      [
        "UPDATE session_authority.filesystem_image_provider_operations",
        "SET state = 'committed', committed_state_revision = $4,",
        "committed_checksum_provenance = 'indexed-frame-v1',",
        "committed_checksum = $5, committed_record_bytes = $6,",
        "committed_record_sha256 = $7",
        "WHERE provider_id = $1 AND anchor_id = $2 AND operation_id = $3",
      ].join(" "),
      [
        providerId,
        anchorId,
        operationZ,
        committedZHead.stateRevision,
        committedZHead.lastChecksum,
        committedZMaterial.bytes,
        committedZMaterial.sha256,
      ],
    ),
    (error) => {
      assert.equal(error.code, "55000");
      assert.equal(
        error.constraint,
        "fs_image_operations_native_commit_only",
      );
      return true;
    },
  );
  await assert.rejects(
    pool.query(
      [
        "UPDATE session_authority.filesystem_image_provider_operations",
        "SET kind = 'attach'",
        "WHERE provider_id = $1 AND anchor_id = $2 AND operation_id = $3",
      ].join(" "),
      [providerId, anchorId, operationZ],
    ),
    (error) => {
      assert.equal(error.code, "55000");
      assert.equal(
        error.constraint,
        "fs_image_operations_native_commit_only",
      );
      return true;
    },
  );
  await assert.rejects(
    pool.query(
      [
        "DELETE FROM session_authority.filesystem_image_provider_heads",
        "WHERE provider_id = $1 AND anchor_id = $2",
      ].join(" "),
      [providerId, anchorId],
    ),
    (error) => {
      assert.equal(error.code, "23503");
      assert.equal(
        error.constraint,
        "filesystem_image_provider_operations_head_fk",
      );
      return true;
    },
  );

  const reusedOperationHead = appendHead(
    preparedZHead,
    "2".repeat(64),
    1536,
  );
  await assert.rejects(
    authority.compareAndAdvance({
      expectedHead: preparedZHead,
      nextHead: reusedOperationHead,
      transition: {
        contractVersion: 1,
        type: "append-prepared-v1",
        frameChecksum: reusedOperationHead.lastChecksum,
        record: preparedRecord({
          checksum: reusedOperationHead.lastChecksum,
          operationId: operationZ,
          revision: reusedOperationHead.stateRevision,
          storageId: preparedZ.storageId,
        }),
      },
    }),
    assertStateInvalid,
  );
  assert.deepEqual(await authority.readHead(), preparedZHead);

  await assert.rejects(
    authority.compareAndAdvance({
      expectedHead: preparedZHead,
      nextHead: committedZHead,
      transition: {
        contractVersion: 1,
        type: "append-committed-v1",
        frameChecksum: committedZHead.lastChecksum,
        record: {
          ...committedZ,
          request: { ...committedZ.request, mismatch: true },
        },
      },
    }),
    assertStateInvalid,
  );
  assert.deepEqual(await authority.readHead(), preparedZHead);

  const committedAcknowledgementLossPool =
    commitAcknowledgementLossAfterQueryPool(
      pool,
      "filesystem image provider committed operation index",
      (text) =>
        text.startsWith(
          "UPDATE session_authority.filesystem_image_provider_operations",
        ),
    );
  const committedAcknowledgementLossStore = new PostgresSerializableStore({
    dedicatedPool: committedAcknowledgementLossPool,
    maxTransactionAttempts: 1,
  });
  await assert.rejects(
    createAuthority(
      committedAcknowledgementLossStore,
      anchorId,
    ).compareAndAdvance({
      expectedHead: preparedZHead,
      nextHead: committedZHead,
      transition: {
        contractVersion: 1,
        type: "append-committed-v1",
        frameChecksum: committedZHead.lastChecksum,
        record: committedZ,
      },
    }),
    assertCommitOutcomeUncertain,
  );
  assert.equal(
    committedAcknowledgementLossPool.didLoseAcknowledgement(),
    true,
  );
  assert.deepEqual(await authority.readHead(), committedZHead);
  assert.deepEqual(
    await authority.readOperation({
      expectedHead: committedZHead,
      operationId: operationZ,
    }),
    committedZ,
  );
  const committedZSnapshot = await readProviderAnchorSnapshot(anchorId);
  assert.equal(
    committedZSnapshot.head[0].operation_index_state_revision,
    committedZHead.stateRevision,
  );
  assert.deepEqual(
    committedZSnapshot.operations.map(({ operation_id, state }) => ({
      operation_id,
      state,
    })),
    [{ operation_id: operationZ, state: "committed" }],
  );

  const duplicateProvisionHead = appendHead(
    committedZHead,
    "2".repeat(64),
    2048,
  );
  const duplicateProvision = preparedRecord({
    checksum: duplicateProvisionHead.lastChecksum,
    operationId: `provider-operation-duplicate-${randomUUID()}`,
    revision: duplicateProvisionHead.stateRevision,
    storageId: preparedZ.storageId,
  });
  await assert.rejects(
    authority.compareAndAdvance({
      expectedHead: committedZHead,
      nextHead: duplicateProvisionHead,
      transition: {
        contractVersion: 1,
        type: "append-prepared-v1",
        frameChecksum: duplicateProvisionHead.lastChecksum,
        record: duplicateProvision,
      },
    }),
    assertStateInvalid,
  );
  assert.deepEqual(
    await readProviderAnchorSnapshot(anchorId),
    committedZSnapshot,
  );

  const mismatchedProjectionHead = appendHead(
    committedZHead,
    "3".repeat(64),
    2048,
  );
  const mismatchedProjection = preparedRecord({
    checksum: mismatchedProjectionHead.lastChecksum,
    kind: "checkpoint",
    operationId: `provider-operation-mismatched-before-${randomUUID()}`,
    revision: mismatchedProjectionHead.stateRevision,
    request: { storageId: preparedZ.storageId },
    storageId: preparedZ.storageId,
    storageStateBefore: {
      ...committedZ.storageState,
      publicationControlIdentity: {
        ...committedZ.storageState.publicationControlIdentity,
        objectId: `${preparedZ.storageId}:different-publication`,
      },
    },
  });
  await assert.rejects(
    authority.compareAndAdvance({
      expectedHead: committedZHead,
      nextHead: mismatchedProjectionHead,
      transition: {
        contractVersion: 1,
        type: "append-prepared-v1",
        frameChecksum: mismatchedProjectionHead.lastChecksum,
        record: mismatchedProjection,
      },
    }),
    assertStateInvalid,
  );
  assert.deepEqual(
    await readProviderAnchorSnapshot(anchorId),
    committedZSnapshot,
  );

  const phantomStorageId = `provider-storage-phantom-${randomUUID()}`;
  const phantomAttachHead = appendHead(
    committedZHead,
    "4".repeat(64),
    2048,
  );
  const phantomAttach = preparedRecord({
    checksum: phantomAttachHead.lastChecksum,
    kind: "attach",
    operationId: `provider-operation-phantom-${randomUUID()}`,
    revision: phantomAttachHead.stateRevision,
    request: { storageId: phantomStorageId },
    storageId: phantomStorageId,
    storageStateBefore: provisionedStorage(phantomStorageId),
  });
  await assert.rejects(
    authority.compareAndAdvance({
      expectedHead: committedZHead,
      nextHead: phantomAttachHead,
      transition: {
        contractVersion: 1,
        type: "append-prepared-v1",
        frameChecksum: phantomAttachHead.lastChecksum,
        record: phantomAttach,
      },
    }),
    assertStateInvalid,
  );
  assert.deepEqual(
    await readProviderAnchorSnapshot(anchorId),
    committedZSnapshot,
  );

  await assert.rejects(
    pool.query(
      [
        "UPDATE session_authority.filesystem_image_provider_heads",
        "SET operation_index_state_revision = NULL",
        "WHERE provider_id = $1 AND anchor_id = $2",
      ].join(" "),
      [providerId, anchorId],
    ),
    (error) => {
      assert.equal(error.code, "55000");
      assert.equal(error.constraint, "fs_image_head_incremental_progress");
      return true;
    },
  );
  assert.deepEqual(
    await readProviderAnchorSnapshot(anchorId),
    committedZSnapshot,
  );

  const operationA = `provider-operation-A-${randomUUID()}`;
  const preparedAHead = appendHead(committedZHead, "c".repeat(64), 2048);
  const preparedA = preparedRecord({
    checksum: preparedAHead.lastChecksum,
    operationId: operationA,
    revision: preparedAHead.stateRevision,
    storageId: `provider-storage-A-${randomUUID()}`,
  });
  assert.equal(
    await authority.compareAndAdvance({
      expectedHead: committedZHead,
      nextHead: preparedAHead,
      transition: {
        contractVersion: 1,
        type: "append-prepared-v1",
        frameChecksum: preparedAHead.lastChecksum,
        record: preparedA,
      },
    }),
    true,
  );
  const committedAHead = appendHead(preparedAHead, "d".repeat(64), 2560);
  const committedA = committedRecord(
    preparedA,
    committedAHead.stateRevision,
  );
  assert.equal(
    await authority.compareAndAdvance({
      expectedHead: preparedAHead,
      nextHead: committedAHead,
      transition: {
        contractVersion: 1,
        type: "append-committed-v1",
        frameChecksum: committedAHead.lastChecksum,
        record: committedA,
      },
    }),
    true,
  );

  const firstPage = await authority.readOperationsPage({
    afterOperationId: null,
    expectedHead: committedAHead,
    limit: 1,
  });
  assert.deepEqual(firstPage, {
    operations: [committedA],
    nextAfterOperationId: operationA,
  });
  assert.deepEqual(
    await authority.readOperationsPage({
      afterOperationId: firstPage.nextAfterOperationId,
      expectedHead: committedAHead,
      limit: 1,
    }),
    {
      operations: [committedZ],
      nextAfterOperationId: null,
    },
  );

  const staleExpectedHead = {
    ...committedAHead,
    lastChecksum: "f".repeat(64),
  };
  const staleNextHead = appendHead(
    staleExpectedHead,
    "6".repeat(64),
    3072,
  );
  const staleOperationId = `provider-operation-stale-${randomUUID()}`;
  const stalePrepared = preparedRecord({
    checksum: staleNextHead.lastChecksum,
    operationId: staleOperationId,
    revision: staleNextHead.stateRevision,
    storageId: `provider-storage-stale-${randomUUID()}`,
  });
  assert.equal(
    await authority.compareAndAdvance({
      expectedHead: staleExpectedHead,
      nextHead: staleNextHead,
      transition: {
        contractVersion: 1,
        type: "append-prepared-v1",
        frameChecksum: staleNextHead.lastChecksum,
        record: stalePrepared,
      },
    }),
    false,
  );
  assert.deepEqual(await authority.readHead(), committedAHead);
  assert.equal(
    await authority.readOperation({
      expectedHead: committedAHead,
      operationId: staleOperationId,
    }),
    null,
  );

  const missingOperationId = `provider-operation-missing-${randomUUID()}`;
  const missingPrepared = preparedRecord({
    checksum: "7".repeat(64),
    operationId: missingOperationId,
    revision: committedAHead.stateRevision,
    storageId: `provider-storage-missing-${randomUUID()}`,
  });
  const missingCommittedHead = appendHead(
    committedAHead,
    "8".repeat(64),
    3072,
  );
  await assert.rejects(
    authority.compareAndAdvance({
      expectedHead: committedAHead,
      nextHead: missingCommittedHead,
      transition: {
        contractVersion: 1,
        type: "append-committed-v1",
        frameChecksum: missingCommittedHead.lastChecksum,
        record: committedRecord(
          missingPrepared,
          missingCommittedHead.stateRevision,
        ),
      },
    }),
    assertStateInvalid,
  );
  assert.deepEqual(await authority.readHead(), committedAHead);

  const beforeRotationSnapshot = await readProviderAnchorSnapshot(anchorId);
  const rotatedHead = rotationHead(committedAHead);
  assert.equal(
    await authority.compareAndAdvance({
      expectedHead: committedAHead,
      nextHead: rotatedHead,
      transition: { contractVersion: 1, type: "rotate-v1" },
    }),
    true,
  );
  const afterRotationSnapshot = await readProviderAnchorSnapshot(anchorId);
  assert.equal(
    afterRotationSnapshot.head[0].operation_index_state_revision,
    beforeRotationSnapshot.head[0].operation_index_state_revision,
  );
  assert.deepEqual(
    afterRotationSnapshot.operations,
    beforeRotationSnapshot.operations,
  );
  assert.deepEqual(
    await authority.readOperationsPage({
      afterOperationId: null,
      expectedHead: rotatedHead,
      limit: 2,
    }),
    {
      operations: [committedA, committedZ],
      nextAfterOperationId: null,
    },
  );
  const indexedCutHead = {
    ...rotationHead(rotatedHead),
    contractVersion: FILESYSTEM_IMAGE_PROVIDER_STATE_HEAD_CONTRACT_VERSION,
  };
  const indexedOperations = [committedA, committedZ].sort((left, right) =>
    left.operationId < right.operationId
      ? -1
      : left.operationId > right.operationId
        ? 1
        : 0,
  );
  const indexedStorages = [committedA.storageState, committedZ.storageState]
    .sort((left, right) =>
      left.storageId < right.storageId
        ? -1
        : left.storageId > right.storageId
          ? 1
          : 0,
    )
    .map((storage) => ({
      currentAttachmentOriginOperationId: null,
      storage,
    }));
  assert.equal(
    await createAdoptionAuthority(store, anchorId).compareAndAdopt({
      expectedHead: rotatedHead,
      nextHead: indexedCutHead,
      operations: indexedOperations,
      storages: indexedStorages,
    }),
    true,
  );
  const indexedRuntimeAuthority = createRuntimeAuthority(store, anchorId);
  assert.deepEqual(await indexedRuntimeAuthority.readHead(), indexedCutHead);
  assert.deepEqual(
    await indexedRuntimeAuthority.readPreparedOperationsPage({
      afterStorageId: null,
      expectedHead: indexedCutHead,
      limit: 4,
    }),
    { operations: [], nextAfterStorageId: null },
  );
  // Exercise the quoted text[] representation; unquoted NULL is an array
  // null sentinel in PostgreSQL rather than this legal opaque operation ID.
  const projectionOriginOperationId = "NULL";
  const projectionOriginPreparedHead = appendHead(
    indexedCutHead,
    "1".repeat(64),
    512,
  );
  const projectionOriginPrepared = preparedRecord({
    checksum: projectionOriginPreparedHead.lastChecksum,
    kind: "attach",
    operationId: projectionOriginOperationId,
    revision: projectionOriginPreparedHead.stateRevision,
    request: { storageId: committedZ.storageId },
    storageId: committedZ.storageId,
    storageStateBefore: committedZ.storageState,
  });
  assert.equal(
    await indexedRuntimeAuthority.compareAndAdvance({
      expectedHead: indexedCutHead,
      nextHead: projectionOriginPreparedHead,
      transition: {
        contractVersion: 1,
        type: "append-prepared-v1",
        frameChecksum: projectionOriginPreparedHead.lastChecksum,
        record: projectionOriginPrepared,
      },
    }),
    true,
  );
  const projectionAttachmentId = `attachment-${randomUUID()}`;
  const projectionAttachment = {
    attachmentId: projectionAttachmentId,
    leaseId: `lease-${randomUUID()}`,
    holderId: `holder-${randomUUID()}`,
    fencingEpoch: "1",
    rootPath: `${committedZ.storageState.mount.mountPath}/${projectionAttachmentId}`,
    proofId: `proof-${randomUUID()}`,
    imageIdentity: committedZ.storageState.mount.imageIdentity,
    rootIdentity: {
      filesystemId: committedZ.storageState.filesystemId,
      objectIdentityScheme: "linux-dev-inode",
      objectId: `${committedZ.storageId}:attachment-root`,
    },
  };
  const projectionAttachedStorage = {
    ...committedZ.storageState,
    lifecycle: "attached",
    revision: "2",
    writerEpoch: "1",
    writerAuthority: {
      fencingEpoch: projectionAttachment.fencingEpoch,
      holderId: projectionAttachment.holderId,
      leaseId: projectionAttachment.leaseId,
    },
    dataRoot: {
      rootPath: projectionAttachment.rootPath,
      imageIdentity: projectionAttachment.imageIdentity,
      rootIdentity: projectionAttachment.rootIdentity,
    },
    attachment: projectionAttachment,
  };
  const projectionOriginCommittedHead = appendHead(
    projectionOriginPreparedHead,
    "2".repeat(64),
    1024,
  );
  const projectionOriginCommitted = {
    ...projectionOriginPrepared,
    state: "committed",
    committedStateRevision: projectionOriginCommittedHead.stateRevision,
    expectedStorage: {
      lifecycle: committedZ.storageState.lifecycle,
      revision: committedZ.storageState.revision,
    },
    result: { status: "attached" },
    storageState: projectionAttachedStorage,
  };
  assert.equal(
    await indexedRuntimeAuthority.compareAndAdvance({
      expectedHead: projectionOriginPreparedHead,
      nextHead: projectionOriginCommittedHead,
      transition: {
        contractVersion: 1,
        type: "append-committed-v1",
        frameChecksum: projectionOriginCommittedHead.lastChecksum,
        record: projectionOriginCommitted,
      },
    }),
    true,
  );
  const projectionPreparedHead = appendHead(
    projectionOriginCommittedHead,
    "3".repeat(64),
    1536,
  );
  const projectionPrepared = preparedRecord({
    checksum: projectionPreparedHead.lastChecksum,
    operationId: `provider-operation-projection-prepared-${randomUUID()}`,
    revision: projectionPreparedHead.stateRevision,
    storageId: `provider-storage-projection-prepared-${randomUUID()}`,
  });
  assert.equal(
    await indexedRuntimeAuthority.compareAndAdvance({
      expectedHead: projectionOriginCommittedHead,
      nextHead: projectionPreparedHead,
      transition: {
        contractVersion: 1,
        type: "append-prepared-v1",
        frameChecksum: projectionPreparedHead.lastChecksum,
        record: projectionPrepared,
      },
    }),
    true,
  );
  assert.deepEqual(
    await indexedRuntimeAuthority.readPreparedOperationsPage({
      afterStorageId: null,
      expectedHead: projectionPreparedHead,
      limit: 4,
    }),
    { operations: [projectionPrepared], nextAfterStorageId: null },
  );
  const projectionRequest = {
    expectedHead: projectionPreparedHead,
    preparedOperationCount: 1,
    preparedProjectionChecksum: projectionChecksum(
      "portable-codex/filesystem-image-provider-state/prepared-projection/v1\0",
      [projectionPrepared],
    ),
    attachmentOrigins: Object.freeze([
      Object.freeze({
        currentStorageRevision: projectionAttachedStorage.revision,
        operationId: projectionOriginOperationId,
        stableStorageChecksum: stableStorageProjectionChecksum(
          projectionAttachedStorage,
        ),
        storageId: projectionAttachedStorage.storageId,
      }),
    ]),
  };
  assert.deepEqual(
    await indexedRuntimeAuthority.compareProjection(projectionRequest),
    projectionReceipt(projectionRequest),
  );
  const projectionSnapshot = await readProviderAnchorSnapshot(anchorId);
  assert.equal(
    await indexedRuntimeAuthority.compareProjection({
      ...projectionRequest,
      preparedProjectionChecksum: "f".repeat(64),
    }),
    null,
  );
  assert.equal(
    await indexedRuntimeAuthority.compareProjection({
      ...projectionRequest,
      expectedHead: projectionOriginCommittedHead,
    }),
    null,
  );
  assert.deepEqual(
    await readProviderAnchorSnapshot(anchorId),
    projectionSnapshot,
  );

  // Cross the store's 1,024-row portal fetch boundary with small records.
  const streamingOperationCount = 1_025;
  const streamingOperations = [];
  for (let index = 0; index < streamingOperationCount; index += 1) {
    const ordinal = String(index + 1).padStart(4, "0");
    streamingOperations.push(
      preparedRecord({
        checksum: "6".repeat(64),
        operationId: `provider-operation-streaming-${ordinal}`,
        revision: String(index + 1),
        storageId: `provider-storage-streaming-${ordinal}`,
      }),
    );
  }
  const streamingExpectedHead = {
    ...genesis,
    anchorRevision: String(streamingOperationCount),
    stateRevision: String(streamingOperationCount),
    frameCount: streamingOperationCount,
    lastChecksum: "6".repeat(64),
    ledgerBytes: streamingOperationCount,
  };
  assert.equal(
    (
      await pool.query(
        rawHeadInsertQuery,
        rawHeadInsertValues(
          streamingAdoptionAnchorId,
          streamingExpectedHead,
          null,
        ),
      )
    ).rowCount,
    1,
  );
  const streamingCutHead = {
    ...rotationHead(streamingExpectedHead),
    contractVersion: FILESYSTEM_IMAGE_PROVIDER_STATE_HEAD_CONTRACT_VERSION,
    checkpointBytes: streamingOperationCount * 256,
    checkpointFrameCount: streamingOperationCount + 2,
  };
  assert.equal(
    await createPagedAdoptionAuthority(
      store,
      streamingAdoptionAnchorId,
    ).compareAndAdopt({
      expectedHead: streamingExpectedHead,
      nextHead: streamingCutHead,
      operationPager: createOperationPager(streamingOperations),
      storagePager: createStoragePager([]),
    }),
    true,
  );
  const streamingProjectionRequest = {
    attachmentOrigins: Object.freeze([]),
    expectedHead: streamingCutHead,
    preparedOperationCount: streamingOperationCount,
    preparedProjectionChecksum: projectionChecksum(
      "portable-codex/filesystem-image-provider-state/prepared-projection/v1\0",
      streamingOperations,
    ),
  };
  assert.deepEqual(
    await createRuntimeAuthority(
      store,
      streamingAdoptionAnchorId,
    ).compareProjection(streamingProjectionRequest),
    projectionReceipt(streamingProjectionRequest),
  );
  assert.equal(
    (
      await pool.query(
        [
          "SELECT pg_catalog.count(*)::pg_catalog.int4 AS operation_count",
          "FROM session_authority.filesystem_image_provider_operations",
          "WHERE provider_id = $1 AND anchor_id = $2",
        ].join(" "),
        [providerId, streamingAdoptionAnchorId],
      )
    ).rows[0].operation_count,
    streamingOperationCount,
  );

  const concurrentAuthority = createAuthority(
    store,
    concurrentAdoptionAnchorId,
  );
  const concurrentPreparedHead = appendHead(
    genesis,
    "2".repeat(64),
    512,
  );
  const concurrentOperationId =
    `provider-operation-concurrent-source-${randomUUID()}`;
  const concurrentPrepared = preparedRecord({
    checksum: concurrentPreparedHead.lastChecksum,
    operationId: concurrentOperationId,
    revision: concurrentPreparedHead.stateRevision,
    storageId: `provider-storage-concurrent-source-${randomUUID()}`,
  });
  assert.equal(
    await concurrentAuthority.compareAndAdvance({
      expectedHead: genesis,
      nextHead: concurrentPreparedHead,
      transition: {
        contractVersion: 1,
        type: "append-prepared-v1",
        frameChecksum: concurrentPreparedHead.lastChecksum,
        record: concurrentPrepared,
      },
    }),
    true,
  );
  const concurrentCommittedHead = appendHead(
    concurrentPreparedHead,
    "3".repeat(64),
    1024,
  );
  const concurrentCommitted = committedRecord(
    concurrentPrepared,
    concurrentCommittedHead.stateRevision,
  );
  assert.equal(
    await concurrentAuthority.compareAndAdvance({
      expectedHead: concurrentPreparedHead,
      nextHead: concurrentCommittedHead,
      transition: {
        contractVersion: 1,
        type: "append-committed-v1",
        frameChecksum: concurrentCommittedHead.lastChecksum,
        record: concurrentCommitted,
      },
    }),
    true,
  );
  const concurrentAdoptionHead = {
    ...rotationHead(concurrentCommittedHead),
    contractVersion: FILESYSTEM_IMAGE_PROVIDER_STATE_HEAD_CONTRACT_VERSION,
  };
  const concurrentAppendHead = appendHead(
    concurrentCommittedHead,
    "4".repeat(64),
    1536,
  );
  const concurrentAppendOperation = preparedRecord({
    checksum: concurrentAppendHead.lastChecksum,
    operationId: `provider-operation-concurrent-append-${randomUUID()}`,
    revision: concurrentAppendHead.stateRevision,
    storageId: `provider-storage-concurrent-append-${randomUUID()}`,
  });
  const [concurrentAdoptionResult, concurrentAppendResult] =
    await Promise.all([
      createAdoptionAuthority(store, concurrentAdoptionAnchorId).compareAndAdopt({
        expectedHead: concurrentCommittedHead,
        nextHead: concurrentAdoptionHead,
        operations: [concurrentCommitted],
        storages: [
          {
            currentAttachmentOriginOperationId: null,
            storage: concurrentCommitted.storageState,
          },
        ],
      }),
      concurrentAuthority.compareAndAdvance({
        expectedHead: concurrentCommittedHead,
        nextHead: concurrentAppendHead,
        transition: {
          contractVersion: 1,
          type: "append-prepared-v1",
          frameChecksum: concurrentAppendHead.lastChecksum,
          record: concurrentAppendOperation,
        },
      }),
    ]);
  assert.equal(
    Number(concurrentAdoptionResult) + Number(concurrentAppendResult),
    1,
  );
  if (concurrentAdoptionResult) {
    assert.deepEqual(
      await createRuntimeAuthority(store, concurrentAdoptionAnchorId).readHead(),
      concurrentAdoptionHead,
    );
  } else {
    assert.deepEqual(await concurrentAuthority.readHead(), concurrentAppendHead);
    assert.deepEqual(
      await concurrentAuthority.readOperation({
        expectedHead: concurrentAppendHead,
        operationId: concurrentAppendOperation.operationId,
      }),
      concurrentAppendOperation,
    );
  }

  const adoptedPreparedBytes = Buffer.from(
    canonicalRecordJson(adoptedPrepared),
    "utf8",
  );
  const adoptedPreparedSha256 = createHash("sha256")
    .update(
      "portable-codex/filesystem-image-provider-state/operation-record/v1\0",
      "utf8",
    )
    .update(adoptedPreparedBytes)
    .digest("hex");
  const adoptedCommittedBytes = Buffer.from(
    canonicalRecordJson(adoptedCommitted),
    "utf8",
  );
  const adoptedCommittedSha256 = createHash("sha256")
    .update(
      "portable-codex/filesystem-image-provider-state/operation-record/v1\0",
      "utf8",
    )
    .update(adoptedCommittedBytes)
    .digest("hex");
  const adoptedInsertQuery = [
    "INSERT INTO session_authority.filesystem_image_provider_operations",
    "(provider_id, anchor_id, operation_id, record_contract_version, state,",
    "kind, storage_id, prepared_state_revision, prepared_checksum,",
    "prepared_record_bytes, prepared_record_sha256)",
    "VALUES ($1, $2, $3, 1, 'prepared', $4, $5,",
    "$6::pg_catalog.numeric, $7, $8, $9)",
  ].join(" ");
  const adoptedInsertValues = [
    providerId,
    adoptedAnchorId,
    adoptedOperationId,
    adoptedPrepared.kind,
    adoptedPrepared.storageId,
    adoptedPrepared.preparedStateRevision,
    adoptedPrepared.preparedChecksum,
    adoptedPreparedBytes,
    adoptedPreparedSha256,
  ];
  const adoptedUpdateQuery = [
    "UPDATE session_authority.filesystem_image_provider_operations",
    "SET state = 'committed', committed_state_revision = $4::pg_catalog.numeric,",
    "committed_checksum_provenance = 'unavailable-adopted-v2',",
    "committed_checksum = NULL, committed_record_bytes = $5,",
    "committed_record_sha256 = $6",
    "WHERE provider_id = $1 AND anchor_id = $2 AND operation_id = $3",
  ].join(" ");
  const adoptedUpdateValues = [
    providerId,
    adoptedAnchorId,
    adoptedOperationId,
    adoptedCommittedHead.stateRevision,
    adoptedCommittedBytes,
    adoptedCommittedSha256,
  ];
  const adoptedLegacySnapshot =
    await readProviderAnchorSnapshot(adoptedAnchorId);
  const rejectedAdoptionClient = await pool.connect();
  let rejectedAdoptionTransactionOpen = false;
  try {
    await rejectedAdoptionClient.query("BEGIN");
    rejectedAdoptionTransactionOpen = true;
    assert.equal(
      (
        await rejectedAdoptionClient.query(
          adoptedInsertQuery,
          adoptedInsertValues,
        )
      ).rowCount,
      1,
    );
    await assert.rejects(
      rejectedAdoptionClient.query(
        adoptedUpdateQuery,
        adoptedUpdateValues,
      ),
      (error) => {
        assert.equal(error.code, "55000");
        assert.equal(
          error.constraint,
          "fs_image_operations_native_commit_only",
        );
        return true;
      },
    );
    await rejectedAdoptionClient.query("ROLLBACK");
    rejectedAdoptionTransactionOpen = false;
  } finally {
    if (rejectedAdoptionTransactionOpen) {
      await rejectedAdoptionClient.query("ROLLBACK");
    }
    rejectedAdoptionClient.release();
  }
  assert.deepEqual(
    await readProviderAnchorSnapshot(adoptedAnchorId),
    adoptedLegacySnapshot,
  );

  await assert.rejects(
    pool.query(
      [
        "UPDATE session_authority.filesystem_image_provider_heads",
        "SET contract_version = 3",
        "WHERE provider_id = $1 AND anchor_id = $2",
      ].join(" "),
      [providerId, adoptedAnchorId],
    ),
    (error) => {
      assert.equal(error.code, "55000");
      assert.equal(
        error.constraint,
        "fs_image_head_contract_transition",
      );
      return true;
    },
  );
  assert.deepEqual(
    await readProviderAnchorSnapshot(adoptedAnchorId),
    adoptedLegacySnapshot,
  );

  const adoptedCutHead = {
    ...rotationHead(adoptedCommittedHead),
    contractVersion: FILESYSTEM_IMAGE_PROVIDER_STATE_HEAD_CONTRACT_VERSION,
  };
  const adoptedHeadUpdateQuery = [
    "UPDATE session_authority.filesystem_image_provider_heads",
    "SET contract_version = $3, anchor_revision = $4::pg_catalog.numeric,",
    "generation = $5::pg_catalog.numeric, state_revision = $6::pg_catalog.numeric,",
    "base_head_checksum = $7,",
    "checkpoint_state_revision = $8::pg_catalog.numeric,",
    "checkpoint_frame_count = $9::pg_catalog.int8, checkpoint_checksum = $10,",
    "checkpoint_bytes = $11::pg_catalog.int8, frame_count = $12::pg_catalog.int4,",
    "last_checksum = $13, ledger_bytes = $14::pg_catalog.int8,",
    "operation_index_state_revision = $15::pg_catalog.numeric",
    "WHERE provider_id = $1 AND anchor_id = $2",
    "AND contract_version = $16",
    "AND anchor_revision = $17::pg_catalog.numeric",
    "AND generation = $18::pg_catalog.numeric",
    "AND state_revision = $19::pg_catalog.numeric",
    "AND base_head_checksum IS NOT DISTINCT FROM $20",
    "AND checkpoint_state_revision = $21::pg_catalog.numeric",
    "AND checkpoint_frame_count = $22::pg_catalog.int8",
    "AND checkpoint_checksum IS NOT DISTINCT FROM $23",
    "AND checkpoint_bytes = $24::pg_catalog.int8",
    "AND frame_count = $25::pg_catalog.int4",
    "AND last_checksum IS NOT DISTINCT FROM $26",
    "AND ledger_bytes = $27::pg_catalog.int8",
    "AND operation_index_state_revision IS NULL",
  ].join(" ");
  const adoptedHeadUpdateValues = [
    providerId,
    adoptedAnchorId,
    adoptedCutHead.contractVersion,
    adoptedCutHead.anchorRevision,
    adoptedCutHead.generation,
    adoptedCutHead.stateRevision,
    adoptedCutHead.baseHeadChecksum,
    adoptedCutHead.checkpointStateRevision,
    adoptedCutHead.checkpointFrameCount,
    adoptedCutHead.checkpointChecksum,
    adoptedCutHead.checkpointBytes,
    adoptedCutHead.frameCount,
    adoptedCutHead.lastChecksum,
    adoptedCutHead.ledgerBytes,
    adoptedCutHead.stateRevision,
    adoptedCommittedHead.contractVersion,
    adoptedCommittedHead.anchorRevision,
    adoptedCommittedHead.generation,
    adoptedCommittedHead.stateRevision,
    adoptedCommittedHead.baseHeadChecksum,
    adoptedCommittedHead.checkpointStateRevision,
    adoptedCommittedHead.checkpointFrameCount,
    adoptedCommittedHead.checkpointChecksum,
    adoptedCommittedHead.checkpointBytes,
    adoptedCommittedHead.frameCount,
    adoptedCommittedHead.lastChecksum,
    adoptedCommittedHead.ledgerBytes,
  ];
  const rawAdoptionManifestId = "a".repeat(64);
  const coveredAdoptionHeadUpdateQuery = [
    "UPDATE session_authority.filesystem_image_provider_heads",
    "SET contract_version = $3, anchor_revision = $4::pg_catalog.numeric,",
    "generation = $5::pg_catalog.numeric, state_revision = $6::pg_catalog.numeric,",
    "base_head_checksum = $7,",
    "checkpoint_state_revision = $8::pg_catalog.numeric,",
    "checkpoint_frame_count = $9::pg_catalog.int8, checkpoint_checksum = $10,",
    "checkpoint_bytes = $11::pg_catalog.int8, frame_count = $12::pg_catalog.int4,",
    "last_checksum = $13, ledger_bytes = $14::pg_catalog.int8,",
    "operation_index_state_revision = $15::pg_catalog.numeric,",
    "operation_index_adoption_id = $16",
    "WHERE provider_id = $1 AND anchor_id = $2",
    "AND contract_version = $17",
    "AND anchor_revision = $18::pg_catalog.numeric",
    "AND generation = $19::pg_catalog.numeric",
    "AND state_revision = $20::pg_catalog.numeric",
    "AND base_head_checksum IS NOT DISTINCT FROM $21",
    "AND checkpoint_state_revision = $22::pg_catalog.numeric",
    "AND checkpoint_frame_count = $23::pg_catalog.int8",
    "AND checkpoint_checksum IS NOT DISTINCT FROM $24",
    "AND checkpoint_bytes = $25::pg_catalog.int8",
    "AND frame_count = $26::pg_catalog.int4",
    "AND last_checksum IS NOT DISTINCT FROM $27",
    "AND ledger_bytes = $28::pg_catalog.int8",
    "AND operation_index_state_revision IS NOT DISTINCT FROM $29::pg_catalog.numeric",
    "AND operation_index_adoption_id IS NULL",
    "AND operation_index_adoption_xid IS NULL",
  ].join(" ");
  const coveredAdoptionHeadValues = (
    selectedAnchorId,
    expectedHead,
    nextHead,
    manifestId,
    sourceMarker = null,
  ) => [
    providerId,
    selectedAnchorId,
    nextHead.contractVersion,
    nextHead.anchorRevision,
    nextHead.generation,
    nextHead.stateRevision,
    nextHead.baseHeadChecksum,
    nextHead.checkpointStateRevision,
    nextHead.checkpointFrameCount,
    nextHead.checkpointChecksum,
    nextHead.checkpointBytes,
    nextHead.frameCount,
    nextHead.lastChecksum,
    nextHead.ledgerBytes,
    nextHead.stateRevision,
    manifestId,
    expectedHead.contractVersion,
    expectedHead.anchorRevision,
    expectedHead.generation,
    expectedHead.stateRevision,
    expectedHead.baseHeadChecksum,
    expectedHead.checkpointStateRevision,
    expectedHead.checkpointFrameCount,
    expectedHead.checkpointChecksum,
    expectedHead.checkpointBytes,
    expectedHead.frameCount,
    expectedHead.lastChecksum,
    expectedHead.ledgerBytes,
    sourceMarker,
  ];
  const coveredAdoptionHeadUpdateValues = coveredAdoptionHeadValues(
    adoptedAnchorId,
    adoptedCommittedHead,
    adoptedCutHead,
    rawAdoptionManifestId,
  );
  const coveredAdoptionOperationInsertQuery = [
    "INSERT INTO session_authority.filesystem_image_provider_operations",
    "(provider_id, anchor_id, operation_id, record_contract_version, state,",
    "kind, storage_id, prepared_state_revision, prepared_checksum,",
    "prepared_record_bytes, prepared_record_sha256, committed_state_revision,",
    "committed_checksum_provenance, committed_checksum, committed_record_bytes,",
    "committed_record_sha256, adoption_id)",
    "VALUES ($1, $2, $3, 1, 'committed', $4, $5,",
    "$6::pg_catalog.numeric, $7, $8, $9, $10::pg_catalog.numeric,",
    "'unavailable-adopted-v2', NULL, $11, $12, $13)",
  ].join(" ");
  const coveredAdoptionOperationInsertValues = [
    providerId,
    adoptedAnchorId,
    adoptedOperationId,
    adoptedCommitted.kind,
    adoptedCommitted.storageId,
    adoptedCommitted.preparedStateRevision,
    adoptedCommitted.preparedChecksum,
    adoptedPreparedBytes,
    adoptedPreparedSha256,
    adoptedCommitted.committedStateRevision,
    adoptedCommittedBytes,
    adoptedCommittedSha256,
    rawAdoptionManifestId,
  ];
  const completeRawAdoption = async (client) => {
    assert.equal(
      (
        await client.query(
          coveredAdoptionHeadUpdateQuery,
          coveredAdoptionHeadUpdateValues,
        )
      ).rowCount,
      1,
    );
    assert.equal(
      (
        await client.query(
          coveredAdoptionOperationInsertQuery,
          coveredAdoptionOperationInsertValues,
        )
      ).rowCount,
      1,
    );
  };
  const rejectedAdoptionTeardownReinsertClient = await pool.connect();
  let rejectedAdoptionTeardownReinsertOpen = false;
  try {
    await rejectedAdoptionTeardownReinsertClient.query("BEGIN");
    rejectedAdoptionTeardownReinsertOpen = true;
    assert.equal(
      (
        await rejectedAdoptionTeardownReinsertClient.query(
          coveredAdoptionHeadUpdateQuery,
          coveredAdoptionHeadUpdateValues,
        )
      ).rowCount,
      1,
    );
    assert.equal(
      (
        await rejectedAdoptionTeardownReinsertClient.query(
          [
            "DELETE FROM session_authority.filesystem_image_provider_heads",
            "WHERE provider_id = $1 AND anchor_id = $2",
          ].join(" "),
          [providerId, adoptedAnchorId],
        )
      ).rowCount,
      1,
    );
    await rejectedAdoptionTeardownReinsertClient.query(
      "SET CONSTRAINTS session_authority.fs_image_heads_adoption_complete IMMEDIATE",
    );
    // Reinsert the valid pre-adoption V2 head so the lifecycle claim, rather
    // than the V3 initial-progress guard, owns this rejection.
    await assert.rejects(
      rejectedAdoptionTeardownReinsertClient.query(
        rawHeadInsertQuery,
        rawHeadInsertValues(
          adoptedAnchorId,
          adoptedCommittedHead,
          null,
        ),
      ),
      (error) => {
        assert.equal(error.code, "55000");
        assert.equal(error.constraint, "fs_image_head_retired");
        return true;
      },
    );
    await rejectedAdoptionTeardownReinsertClient.query("ROLLBACK");
    rejectedAdoptionTeardownReinsertOpen = false;
  } finally {
    if (rejectedAdoptionTeardownReinsertOpen) {
      await rejectedAdoptionTeardownReinsertClient.query("ROLLBACK");
    }
    rejectedAdoptionTeardownReinsertClient.release();
  }
  assert.deepEqual(
    await readProviderAnchorSnapshot(adoptedAnchorId),
    adoptedLegacySnapshot,
  );
  let adoptionValidationHead = genesis;
  const adoptionValidationHeadAnchor = createHeadAnchor(
    store,
    adoptionValidationAnchorId,
  );
  for (const checksumCharacter of ["9", "a", "b"]) {
    const nextHead = appendHead(
      adoptionValidationHead,
      checksumCharacter.repeat(64),
      Number(adoptionValidationHead.ledgerBytes) + 512,
    );
    assert.equal(
      await adoptionValidationHeadAnchor.compareAndAdvance({
        expectedHead: adoptionValidationHead,
        nextHead,
      }),
      true,
    );
    adoptionValidationHead = nextHead;
  }
  const adoptionValidationCutHead = {
    ...rotationHead(adoptionValidationHead),
    contractVersion: FILESYSTEM_IMAGE_PROVIDER_STATE_HEAD_CONTRACT_VERSION,
  };
  const adoptionValidationManifestId = "b".repeat(64);
  const adoptionValidationHeadUpdateValues = coveredAdoptionHeadValues(
    adoptionValidationAnchorId,
    adoptionValidationHead,
    adoptionValidationCutHead,
    adoptionValidationManifestId,
  );
  const coveredAdoptionPreparedInsertQuery = [
    "INSERT INTO session_authority.filesystem_image_provider_operations",
    "(provider_id, anchor_id, operation_id, record_contract_version, state,",
    "kind, storage_id, prepared_state_revision, prepared_checksum,",
    "prepared_record_bytes, prepared_record_sha256, adoption_id)",
    "VALUES ($1, $2, $3, 1, 'prepared', $4, $5,",
    "$6::pg_catalog.numeric, $7, $8, $9, $10)",
  ].join(" ");
  const rawPreparedMaterial = (revision, suffix) => {
    const record = preparedRecord({
      checksum: suffix.repeat(64),
      operationId:
        `provider-operation-adoption-validation-${revision}-${randomUUID()}`,
      revision,
      storageId:
        `provider-storage-adoption-validation-${revision}-${randomUUID()}`,
    });
    const bytes = Buffer.from(canonicalRecordJson(record), "utf8");
    const sha256 = createHash("sha256")
      .update(
        "portable-codex/filesystem-image-provider-state/operation-record/v1\0",
        "utf8",
      )
      .update(bytes)
      .digest("hex");
    return { bytes, record, sha256 };
  };
  const insertRawPrepared = async (
    client,
    material,
    manifestId = adoptionValidationManifestId,
  ) =>
    await client.query(coveredAdoptionPreparedInsertQuery, [
      providerId,
      adoptionValidationAnchorId,
      material.record.operationId,
      material.record.kind,
      material.record.storageId,
      material.record.preparedStateRevision,
      material.record.preparedChecksum,
      material.bytes,
      material.sha256,
      manifestId,
    ]);
  const withRawValidationAdoption = async (action) => {
    const client = await pool.connect();
    let transactionOpen = false;
    try {
      await client.query("BEGIN");
      transactionOpen = true;
      assert.equal(
        (
          await client.query(
            coveredAdoptionHeadUpdateQuery,
            adoptionValidationHeadUpdateValues,
          )
        ).rowCount,
        1,
      );
      await action(client);
      await client.query("ROLLBACK");
      transactionOpen = false;
    } finally {
      if (transactionOpen) await client.query("ROLLBACK");
      client.release();
    }
  };
  const assertDeferredRevisionCoverFailure = async (materials) => {
    await withRawValidationAdoption(async (client) => {
      for (const material of materials) {
        assert.equal((await insertRawPrepared(client, material)).rowCount, 1);
      }
      await assert.rejects(
        client.query(
          "SET CONSTRAINTS session_authority.fs_image_heads_adoption_complete IMMEDIATE",
        ),
        (error) => {
          assert.equal(error.code, "23514");
          assert.equal(error.constraint, "fs_image_adoption_revision_cover");
          return true;
        },
      );
    });
  };
  const revisionOne = rawPreparedMaterial("1", "9");
  const revisionThree = rawPreparedMaterial("3", "b");
  await assertDeferredRevisionCoverFailure([]);
  await assertDeferredRevisionCoverFailure([revisionOne]);
  await assertDeferredRevisionCoverFailure([revisionOne, revisionThree]);
  await withRawValidationAdoption(async (client) => {
    assert.equal((await insertRawPrepared(client, revisionOne)).rowCount, 1);
    await assert.rejects(
      insertRawPrepared(client, rawPreparedMaterial("1", "c")),
      (error) => {
        assert.equal(error.code, "23505");
        assert.equal(
          error.constraint,
          "fs_image_operations_prepared_revision_uniq",
        );
        return true;
      },
    );
  });
  await withRawValidationAdoption(async (client) => {
    await assert.rejects(
      insertRawPrepared(client, rawPreparedMaterial("4", "c")),
      (error) => {
        assert.equal(error.code, "55000");
        assert.equal(error.constraint, "fs_image_operations_insert_path");
        return true;
      },
    );
  });
  await withRawValidationAdoption(async (client) => {
    await assert.rejects(
      insertRawPrepared(client, revisionOne, "c".repeat(64)),
      (error) => {
        assert.equal(error.code, "55000");
        assert.equal(error.constraint, "fs_image_operations_insert_path");
        return true;
      },
    );
  });
  assert.deepEqual(
    await createRuntimeAuthority(store, adoptionValidationAnchorId).readHead(),
    adoptionValidationHead,
  );
  const rejectedCoveredAdoptionClient = await pool.connect();
  let rejectedCoveredAdoptionTransactionOpen = false;
  try {
    await rejectedCoveredAdoptionClient.query("BEGIN");
    rejectedCoveredAdoptionTransactionOpen = true;
    await assert.rejects(
      rejectedCoveredAdoptionClient.query(
        adoptedHeadUpdateQuery,
        adoptedHeadUpdateValues,
      ),
      (error) => {
        assert.equal(error.code, "55000");
        assert.equal(error.constraint, "fs_image_head_contract_transition");
        return true;
      },
    );
    await rejectedCoveredAdoptionClient.query("ROLLBACK");
    rejectedCoveredAdoptionTransactionOpen = false;
  } finally {
    if (rejectedCoveredAdoptionTransactionOpen) {
      await rejectedCoveredAdoptionClient.query("ROLLBACK");
    }
    rejectedCoveredAdoptionClient.release();
  }
  assert.deepEqual(
    await readProviderAnchorSnapshot(adoptedAnchorId),
    adoptedLegacySnapshot,
  );

  const postValidationOperationId =
    `provider-operation-post-validation-${randomUUID()}`;
  const postValidationPrepared = preparedRecord({
    checksum: "8".repeat(64),
    operationId: postValidationOperationId,
    revision: adoptedCommittedHead.stateRevision,
    storageId: `provider-storage-post-validation-${randomUUID()}`,
  });
  const postValidationPreparedBytes = Buffer.from(
    canonicalRecordJson(postValidationPrepared),
    "utf8",
  );
  const postValidationPreparedSha256 = createHash("sha256")
    .update(
      "portable-codex/filesystem-image-provider-state/operation-record/v1\0",
      "utf8",
    )
    .update(postValidationPreparedBytes)
    .digest("hex");
  const rejectedPostValidationInsertClient = await pool.connect();
  let rejectedPostValidationInsertTransactionOpen = false;
  try {
    await rejectedPostValidationInsertClient.query("BEGIN");
    rejectedPostValidationInsertTransactionOpen = true;
    await completeRawAdoption(rejectedPostValidationInsertClient);
    await rejectedPostValidationInsertClient.query(
      "SET CONSTRAINTS session_authority.fs_image_heads_adoption_complete IMMEDIATE",
    );
    await assert.rejects(
      rejectedPostValidationInsertClient.query(adoptedInsertQuery, [
        providerId,
        adoptedAnchorId,
        postValidationOperationId,
        postValidationPrepared.kind,
        postValidationPrepared.storageId,
        postValidationPrepared.preparedStateRevision,
        postValidationPrepared.preparedChecksum,
        postValidationPreparedBytes,
        postValidationPreparedSha256,
      ]),
      (error) => {
        assert.equal(error.code, "23505");
        assert.equal(
          error.constraint,
          "fs_image_operations_revision_cross_unique",
        );
        return true;
      },
    );
    await rejectedPostValidationInsertClient.query("ROLLBACK");
    rejectedPostValidationInsertTransactionOpen = false;
  } finally {
    if (rejectedPostValidationInsertTransactionOpen) {
      await rejectedPostValidationInsertClient.query("ROLLBACK");
    }
    rejectedPostValidationInsertClient.release();
  }
  assert.deepEqual(
    await readProviderAnchorSnapshot(adoptedAnchorId),
    adoptedLegacySnapshot,
  );

  const rejectedSameTransactionHeadClient = await pool.connect();
  let rejectedSameTransactionHeadOpen = false;
  try {
    await rejectedSameTransactionHeadClient.query("BEGIN");
    rejectedSameTransactionHeadOpen = true;
    await completeRawAdoption(rejectedSameTransactionHeadClient);
    await assert.rejects(
      rejectedSameTransactionHeadClient.query(
        [
          "UPDATE session_authority.filesystem_image_provider_heads",
          "SET checkpoint_checksum = $3, last_checksum = $3",
          "WHERE provider_id = $1 AND anchor_id = $2",
        ].join(" "),
        [providerId, adoptedAnchorId, "f".repeat(64)],
      ),
      (error) => {
        assert.equal(error.code, "55000");
        assert.equal(
          error.constraint,
          "fs_image_head_adoption_same_xact_update",
        );
        return true;
      },
    );
    await rejectedSameTransactionHeadClient.query("ROLLBACK");
    rejectedSameTransactionHeadOpen = false;
  } finally {
    if (rejectedSameTransactionHeadOpen) {
      await rejectedSameTransactionHeadClient.query("ROLLBACK");
    }
    rejectedSameTransactionHeadClient.release();
  }
  assert.deepEqual(
    await readProviderAnchorSnapshot(adoptedAnchorId),
    adoptedLegacySnapshot,
  );

  assert.equal(
    await createAdoptionAuthority(store, adoptedAnchorId).compareAndAdopt({
      expectedHead: adoptedCommittedHead,
      nextHead: adoptedCutHead,
      operations: [adoptedCommitted],
      storages: [
        {
          currentAttachmentOriginOperationId: null,
          storage: adoptedCommitted.storageState,
        },
      ],
    }),
    true,
  );
  const adoptedCutSnapshot =
    await readProviderAnchorSnapshot(adoptedAnchorId);
  assert.equal(adoptedCutSnapshot.operations.length, 1);

  const rejectedPostCutAdoptionClient = await pool.connect();
  let rejectedPostCutAdoptionTransactionOpen = false;
  try {
    await rejectedPostCutAdoptionClient.query("BEGIN");
    rejectedPostCutAdoptionTransactionOpen = true;
    // The adoption transaction already installed this committed row. Its
    // legacy suffix cannot be replayed after the cut has committed.
    await assert.rejects(
      rejectedPostCutAdoptionClient.query(
        adoptedUpdateQuery,
        adoptedUpdateValues,
      ),
      (error) => {
        assert.equal(error.code, "55000");
        assert.equal(
          error.constraint,
          "fs_image_operations_native_commit_only",
        );
        return true;
      },
    );
    await rejectedPostCutAdoptionClient.query("ROLLBACK");
    rejectedPostCutAdoptionTransactionOpen = false;
  } finally {
    if (rejectedPostCutAdoptionTransactionOpen) {
      await rejectedPostCutAdoptionClient.query("ROLLBACK");
    }
    rejectedPostCutAdoptionClient.release();
  }
  assert.deepEqual(
    await readProviderAnchorSnapshot(adoptedAnchorId),
    adoptedCutSnapshot,
  );

  const adoptedRows = await pool.query(
    [
      "SELECT contract_version, checkpoint_state_revision::text,",
      "operation_index_state_revision::text",
      "FROM session_authority.filesystem_image_provider_heads",
      "WHERE provider_id = $1 AND anchor_id = $2",
    ].join(" "),
    [providerId, adoptedAnchorId],
  );
  assert.deepEqual(adoptedRows.rows, [
    {
      contract_version: 3,
      checkpoint_state_revision: adoptedCommittedHead.stateRevision,
      operation_index_state_revision: adoptedCommittedHead.stateRevision,
    },
  ]);
  const adoptedRuntimeAuthority = createRuntimeAuthority(store, adoptedAnchorId);
  assert.deepEqual(await adoptedRuntimeAuthority.readHead(), adoptedCutHead);
  assert.deepEqual(
    await adoptedRuntimeAuthority.readOperation({
      expectedHead: adoptedCutHead,
      operationId: adoptedOperationId,
    }),
    adoptedCommitted,
  );
  assert.deepEqual(
    await adoptedRuntimeAuthority.readPreparedOperationsPage({
      afterStorageId: null,
      expectedHead: adoptedCutHead,
      limit: 4,
    }),
    { operations: [], nextAfterStorageId: null },
  );

  const adoptionAcknowledgementLossOperations = [];
  for (let index = 0; index < 5; index += 1) {
    const ordinal = String(index + 1).padStart(4, "0");
    const prepared = preparedRecord({
      checksum: "6".repeat(64),
      operationId: `provider-operation-adoption-ack-loss-${ordinal}`,
      revision: String(index * 2 + 1),
      storageId: `provider-storage-adoption-ack-loss-${ordinal}`,
    });
    adoptionAcknowledgementLossOperations.push(
      committedRecord(prepared, String(index * 2 + 2)),
    );
  }
  const adoptionAcknowledgementLossCommittedHead = {
    ...genesis,
    anchorRevision: "10",
    stateRevision: "10",
    frameCount: 10,
    lastChecksum: "7".repeat(64),
    ledgerBytes: 5_120,
  };
  assert.equal(
    (
      await pool.query(
        rawHeadInsertQuery,
        rawHeadInsertValues(
          adoptionAcknowledgementLossAnchorId,
          adoptionAcknowledgementLossCommittedHead,
          null,
        ),
      )
    ).rowCount,
    1,
  );
  const adoptionAcknowledgementLossCutHead = {
    ...rotationHead(adoptionAcknowledgementLossCommittedHead),
    contractVersion: FILESYSTEM_IMAGE_PROVIDER_STATE_HEAD_CONTRACT_VERSION,
    checkpointBytes: 5_120,
    checkpointFrameCount: 7,
  };
  const adoptionAcknowledgementLossPool =
    commitAcknowledgementLossAfterQueryPool(
      pool,
      "filesystem image provider adoption",
      (text) =>
        text.startsWith(
          "UPDATE session_authority.filesystem_image_provider_heads",
        ) && text.includes("operation_index_adoption_id = $16"),
    );
  const adoptionAcknowledgementLossStore = new PostgresSerializableStore({
    dedicatedPool: adoptionAcknowledgementLossPool,
    maxTransactionAttempts: 1,
  });
  const adoptionAcknowledgementLossPagerTracker = {
    operationRequests: [],
    storageRequests: [],
  };
  const adoptionAcknowledgementLossStorages =
    adoptionAcknowledgementLossOperations.map((operation) =>
      Object.freeze({
        currentAttachmentOriginOperationId: null,
        storage: operation.storageState,
      }));
  assert.equal(
    await createPagedAdoptionAuthority(
      adoptionAcknowledgementLossStore,
      adoptionAcknowledgementLossAnchorId,
    ).compareAndAdopt({
      expectedHead: adoptionAcknowledgementLossCommittedHead,
      nextHead: adoptionAcknowledgementLossCutHead,
      operationPager: createOperationPager(
        adoptionAcknowledgementLossOperations,
        adoptionAcknowledgementLossPagerTracker,
      ),
      storagePager: createStoragePager(
        adoptionAcknowledgementLossStorages,
        adoptionAcknowledgementLossPagerTracker,
      ),
    }),
    true,
  );
  assert.equal(
    adoptionAcknowledgementLossPool.didLoseAcknowledgement(),
    true,
  );
  assert.deepEqual(adoptionAcknowledgementLossPagerTracker.operationRequests, [
    { afterOperationId: null, limit: 4 },
    {
      afterOperationId:
        adoptionAcknowledgementLossOperations[3].operationId,
      limit: 4,
    },
    { afterOperationId: null, limit: 4 },
    {
      afterOperationId:
        adoptionAcknowledgementLossOperations[3].operationId,
      limit: 4,
    },
  ]);
  assert.deepEqual(adoptionAcknowledgementLossPagerTracker.storageRequests, [
    { afterStorageId: null, limit: 4 },
    {
      afterStorageId:
        adoptionAcknowledgementLossStorages[3].storage.storageId,
      limit: 4,
    },
    { afterStorageId: null, limit: 4 },
    {
      afterStorageId:
        adoptionAcknowledgementLossStorages[3].storage.storageId,
      limit: 4,
    },
  ]);
  const durableAdoptionAcknowledgementLossAuthority = createRuntimeAuthority(
    store,
    adoptionAcknowledgementLossAnchorId,
  );
  assert.deepEqual(
    await durableAdoptionAcknowledgementLossAuthority.readHead(),
    adoptionAcknowledgementLossCutHead,
  );
  assert.deepEqual(
    await durableAdoptionAcknowledgementLossAuthority.readOperation({
      expectedHead: adoptionAcknowledgementLossCutHead,
      operationId: adoptionAcknowledgementLossOperations[0].operationId,
    }),
    adoptionAcknowledgementLossOperations[0],
  );

  const acknowledgementLossPool = commitAcknowledgementLossAfterQueryPool(
    pool,
    "filesystem image provider operation index",
    (text) =>
      text.startsWith(
        "INSERT INTO session_authority.filesystem_image_provider_operations",
      ),
  );
  const acknowledgementLossStore = new PostgresSerializableStore({
    dedicatedPool: acknowledgementLossPool,
    maxTransactionAttempts: 1,
  });
  const acknowledgementLossAuthority = createAuthority(
    acknowledgementLossStore,
    acknowledgementLossAnchorId,
  );
  const acknowledgementLossHead = appendHead(
    genesis,
    "9".repeat(64),
    512,
  );
  const acknowledgementLossOperationId =
    `provider-operation-ack-loss-${randomUUID()}`;
  const acknowledgementLossPrepared = preparedRecord({
    checksum: acknowledgementLossHead.lastChecksum,
    operationId: acknowledgementLossOperationId,
    revision: acknowledgementLossHead.stateRevision,
    storageId: `provider-storage-ack-loss-${randomUUID()}`,
  });
  await assert.rejects(
    acknowledgementLossAuthority.compareAndAdvance({
      expectedHead: genesis,
      nextHead: acknowledgementLossHead,
      transition: {
        contractVersion: 1,
        type: "append-prepared-v1",
        frameChecksum: acknowledgementLossHead.lastChecksum,
        record: acknowledgementLossPrepared,
      },
    }),
    assertCommitOutcomeUncertain,
  );
  assert.equal(acknowledgementLossPool.didLoseAcknowledgement(), true);
  const durableAcknowledgementLossAuthority = createAuthority(
    store,
    acknowledgementLossAnchorId,
  );
  assert.deepEqual(
    await durableAcknowledgementLossAuthority.readHead(),
    acknowledgementLossHead,
  );
  assert.deepEqual(
    await durableAcknowledgementLossAuthority.readOperation({
      expectedHead: acknowledgementLossHead,
      operationId: acknowledgementLossOperationId,
    }),
    acknowledgementLossPrepared,
  );
  const acknowledgementLossSnapshot =
    await readProviderAnchorSnapshot(acknowledgementLossAnchorId);
  assert.equal(
    acknowledgementLossSnapshot.head[0].operation_index_state_revision,
    acknowledgementLossHead.stateRevision,
  );
  assert.deepEqual(
    acknowledgementLossSnapshot.operations.map(
      ({ operation_id, state }) => ({ operation_id, state }),
    ),
    [{ operation_id: acknowledgementLossOperationId, state: "prepared" }],
  );

  const rejectedHistoryMoveAnchorId =
    `operation-index-history-move-${randomUUID()}`;
  const rejectedHistoryMoveClient = await pool.connect();
  let rejectedHistoryMoveTransactionOpen = false;
  try {
    await rejectedHistoryMoveClient.query("BEGIN");
    rejectedHistoryMoveTransactionOpen = true;
    assert.equal(
      (
        await rejectedHistoryMoveClient.query(
          [
            "DELETE FROM session_authority.filesystem_image_provider_operations",
            "WHERE provider_id = $1 AND anchor_id = $2 AND operation_id = $3",
          ].join(" "),
          [
            providerId,
            acknowledgementLossAnchorId,
            acknowledgementLossOperationId,
          ],
        )
      ).rowCount,
      1,
    );
    await assert.rejects(
      rejectedHistoryMoveClient.query(
        [
          "UPDATE session_authority.filesystem_image_provider_heads",
          "SET anchor_id = $3",
          "WHERE provider_id = $1 AND anchor_id = $2",
        ].join(" "),
        [
          providerId,
          acknowledgementLossAnchorId,
          rejectedHistoryMoveAnchorId,
        ],
      ),
      (error) => {
        assert.equal(error.code, "55000");
        assert.equal(error.constraint, "fs_image_head_identity_immutable");
        return true;
      },
    );
    await rejectedHistoryMoveClient.query("ROLLBACK");
    rejectedHistoryMoveTransactionOpen = false;
  } finally {
    if (rejectedHistoryMoveTransactionOpen) {
      await rejectedHistoryMoveClient.query("ROLLBACK");
    }
    rejectedHistoryMoveClient.release();
  }
  assert.deepEqual(
    await readProviderAnchorSnapshot(acknowledgementLossAnchorId),
    acknowledgementLossSnapshot,
  );
  assert.deepEqual(
    await readProviderAnchorSnapshot(rejectedHistoryMoveAnchorId),
    { head: [], operations: [] },
  );

  const rejectedZeroHistoryMoveAnchorId =
    `operation-index-zero-history-move-${randomUUID()}`;
  await assert.rejects(
    pool.query(
      [
        "UPDATE session_authority.filesystem_image_provider_heads",
        "SET anchor_id = $3",
        "WHERE provider_id = $1 AND anchor_id = $2",
      ].join(" "),
      [providerId, legacyAnchorId, rejectedZeroHistoryMoveAnchorId],
    ),
    (error) => {
      assert.equal(error.code, "55000");
      assert.equal(error.constraint, "fs_image_head_identity_immutable");
      return true;
    },
  );
  assert.deepEqual(
    await readProviderAnchorSnapshot(rejectedZeroHistoryMoveAnchorId),
    { head: [], operations: [] },
  );

  const zeroHistoryRetirementSnapshot =
    await readProviderAnchorSnapshot(legacyAnchorId);
  const zeroHistoryRetirementClient = await pool.connect();
  let zeroHistoryRetirementTransactionOpen = false;
  try {
    await zeroHistoryRetirementClient.query("BEGIN");
    zeroHistoryRetirementTransactionOpen = true;
    assert.equal(
      (
        await zeroHistoryRetirementClient.query(
          [
            "DELETE FROM session_authority.filesystem_image_provider_heads",
            "WHERE provider_id = $1 AND anchor_id = $2",
          ].join(" "),
          [providerId, legacyAnchorId],
        )
      ).rowCount,
      1,
    );
    await assert.rejects(
      zeroHistoryRetirementClient.query(
        rawHeadInsertQuery,
        rawHeadInsertValues(legacyAnchorId, legacyHead, null),
      ),
      (error) => {
        assert.equal(error.code, "55000");
        assert.equal(error.constraint, "fs_image_head_retired");
        return true;
      },
    );
    await zeroHistoryRetirementClient.query("ROLLBACK");
    zeroHistoryRetirementTransactionOpen = false;
  } finally {
    if (zeroHistoryRetirementTransactionOpen) {
      await zeroHistoryRetirementClient.query("ROLLBACK");
    }
    zeroHistoryRetirementClient.release();
  }
  assert.deepEqual(
    await readProviderAnchorSnapshot(legacyAnchorId),
    zeroHistoryRetirementSnapshot,
  );

  const lifecycleRaceAuthority = createAuthority(store, lifecycleRaceAnchorId);
  const lifecycleRaceHead = appendHead(genesis, "0".repeat(64), 512);
  const lifecycleRaceOperation = preparedRecord({
    checksum: lifecycleRaceHead.lastChecksum,
    operationId: `provider-operation-lifecycle-race-${randomUUID()}`,
    revision: lifecycleRaceHead.stateRevision,
    storageId: `provider-storage-lifecycle-race-${randomUUID()}`,
  });
  assert.equal(
    await lifecycleRaceAuthority.compareAndAdvance({
      expectedHead: genesis,
      nextHead: lifecycleRaceHead,
      transition: {
        contractVersion: 1,
        type: "append-prepared-v1",
        frameChecksum: lifecycleRaceHead.lastChecksum,
        record: lifecycleRaceOperation,
      },
    }),
    true,
  );
  const lifecycleTeardownClient = await pool.connect();
  const lifecycleReinsertClient = await pool.connect();
  let lifecycleTeardownOpen = false;
  let lifecycleReinsertOpen = false;
  let lifecycleReinsertOutcome;
  try {
    await lifecycleTeardownClient.query("BEGIN");
    lifecycleTeardownOpen = true;
    assert.equal(
      (
        await lifecycleTeardownClient.query(
          [
            "DELETE FROM session_authority.filesystem_image_provider_operations",
            "WHERE provider_id = $1 AND anchor_id = $2",
          ].join(" "),
          [providerId, lifecycleRaceAnchorId],
        )
      ).rowCount,
      1,
    );
    assert.equal(
      (
        await lifecycleTeardownClient.query(
          [
            "DELETE FROM session_authority.filesystem_image_provider_heads",
            "WHERE provider_id = $1 AND anchor_id = $2",
          ].join(" "),
          [providerId, lifecycleRaceAnchorId],
        )
      ).rowCount,
      1,
    );
    await lifecycleTeardownClient.query(
      "SET CONSTRAINTS session_authority.filesystem_image_provider_operations_delete_guard IMMEDIATE",
    );

    await lifecycleReinsertClient.query("BEGIN");
    lifecycleReinsertOpen = true;
    await lifecycleReinsertClient.query("SET LOCAL lock_timeout = '5s'");
    const lifecycleReinsertBackendPid = (
      await lifecycleReinsertClient.query(
        "SELECT pg_catalog.pg_backend_pid() AS backend_pid",
      )
    ).rows[0].backend_pid;
    lifecycleReinsertOutcome = lifecycleReinsertClient
      .query(
        rawHeadInsertQuery,
        rawHeadInsertValues(
          lifecycleRaceAnchorId,
          lifecycleRaceHead,
          lifecycleRaceHead.stateRevision,
        ),
      )
      .then(
        (value) => ({ status: "fulfilled", value }),
        (error) => ({ error, status: "rejected" }),
      );
    await waitForBackendLockWait(
      lifecycleTeardownClient,
      lifecycleReinsertBackendPid,
    );
    await lifecycleTeardownClient.query("COMMIT");
    lifecycleTeardownOpen = false;
    const outcome = await lifecycleReinsertOutcome;
    assert.equal(outcome.status, "rejected");
    assert.equal(outcome.error.code, "55000");
    assert.equal(outcome.error.constraint, "fs_image_head_retired");
    await lifecycleReinsertClient.query("ROLLBACK");
    lifecycleReinsertOpen = false;
  } finally {
    if (lifecycleTeardownOpen) {
      await lifecycleTeardownClient.query("ROLLBACK");
    }
    if (lifecycleReinsertOutcome !== undefined) {
      await lifecycleReinsertOutcome;
    }
    if (lifecycleReinsertOpen) {
      await lifecycleReinsertClient.query("ROLLBACK");
    }
    lifecycleTeardownClient.release();
    lifecycleReinsertClient.release();
  }
  assert.deepEqual(
    (
      await pool.query(
        [
          "SELECT retired_xid IS NOT NULL AS retired",
          "FROM session_authority.filesystem_image_provider_anchor_lifecycle",
          "WHERE provider_id = $1 AND anchor_id = $2",
        ].join(" "),
        [providerId, lifecycleRaceAnchorId],
      )
    ).rows,
    [{ retired: true }],
  );
  assert.deepEqual(
    await readProviderAnchorSnapshot(lifecycleRaceAnchorId),
    { head: [], operations: [] },
  );
  for (const statement of [
    [
      "UPDATE session_authority.filesystem_image_provider_anchor_lifecycle",
      "SET retired_xid = NULL",
      "WHERE provider_id = $1 AND anchor_id = $2",
    ].join(" "),
    [
      "DELETE FROM session_authority.filesystem_image_provider_anchor_lifecycle",
      "WHERE provider_id = $1 AND anchor_id = $2",
    ].join(" "),
  ]) {
    await assert.rejects(
      pool.query(statement, [providerId, lifecycleRaceAnchorId]),
      (error) => {
        assert.equal(error.code, "55000");
        assert.equal(
          error.constraint,
          "fs_image_anchor_lifecycle_immutable",
        );
        return true;
      },
    );
  }

  const rejectedRetiredAnchorReuseClient = await pool.connect();
  let rejectedRetiredAnchorReuseTransactionOpen = false;
  try {
    await rejectedRetiredAnchorReuseClient.query("BEGIN");
    rejectedRetiredAnchorReuseTransactionOpen = true;
    assert.equal(
      (
        await rejectedRetiredAnchorReuseClient.query(
          [
            "DELETE FROM session_authority.filesystem_image_provider_operations",
            "WHERE provider_id = $1 AND anchor_id = $2 AND operation_id = $3",
          ].join(" "),
          [
            providerId,
            acknowledgementLossAnchorId,
            acknowledgementLossOperationId,
          ],
        )
      ).rowCount,
      1,
    );
    assert.equal(
      (
        await rejectedRetiredAnchorReuseClient.query(
          [
            "DELETE FROM session_authority.filesystem_image_provider_heads",
            "WHERE provider_id = $1 AND anchor_id = $2",
          ].join(" "),
          [providerId, acknowledgementLossAnchorId],
        )
      ).rowCount,
      1,
    );
    await rejectedRetiredAnchorReuseClient.query(
      "SET CONSTRAINTS session_authority.filesystem_image_provider_operations_delete_guard IMMEDIATE",
    );
    await assert.rejects(
      rejectedRetiredAnchorReuseClient.query(
        [
          "INSERT INTO session_authority.filesystem_image_provider_heads",
          "(provider_id, anchor_id, contract_version, anchor_revision, generation,",
          "state_revision, base_head_checksum, checkpoint_state_revision,",
          "checkpoint_frame_count, checkpoint_checksum, checkpoint_bytes,",
          "frame_count, last_checksum, ledger_bytes, operation_index_state_revision)",
          "VALUES ($1, $2, $3, $4::pg_catalog.numeric, $5::pg_catalog.numeric,",
          "$6::pg_catalog.numeric, $7, $8::pg_catalog.numeric, $9::pg_catalog.int8,",
          "$10, $11::pg_catalog.int8, $12::pg_catalog.int4, $13,",
          "$14::pg_catalog.int8, $15::pg_catalog.numeric)",
        ].join(" "),
        [
          providerId,
          acknowledgementLossAnchorId,
          acknowledgementLossHead.contractVersion,
          acknowledgementLossHead.anchorRevision,
          acknowledgementLossHead.generation,
          acknowledgementLossHead.stateRevision,
          acknowledgementLossHead.baseHeadChecksum,
          acknowledgementLossHead.checkpointStateRevision,
          acknowledgementLossHead.checkpointFrameCount,
          acknowledgementLossHead.checkpointChecksum,
          acknowledgementLossHead.checkpointBytes,
          acknowledgementLossHead.frameCount,
          acknowledgementLossHead.lastChecksum,
          acknowledgementLossHead.ledgerBytes,
          acknowledgementLossHead.stateRevision,
        ],
      ),
      (error) => {
        assert.equal(error.code, "55000");
        assert.equal(error.constraint, "fs_image_head_retired");
        return true;
      },
    );
    await rejectedRetiredAnchorReuseClient.query("ROLLBACK");
    rejectedRetiredAnchorReuseTransactionOpen = false;
  } finally {
    if (rejectedRetiredAnchorReuseTransactionOpen) {
      await rejectedRetiredAnchorReuseClient.query("ROLLBACK");
    }
    rejectedRetiredAnchorReuseClient.release();
  }
  assert.deepEqual(
    await readProviderAnchorSnapshot(acknowledgementLossAnchorId),
    acknowledgementLossSnapshot,
  );

  const stored = await pool.query(
    [
      "SELECT operation_id, state, prepared_checksum,",
      "committed_checksum_provenance, committed_checksum,",
      "prepared_record_bytes, committed_record_bytes,",
      "octet_length(prepared_record_bytes) AS prepared_bytes,",
      "octet_length(committed_record_bytes) AS committed_bytes,",
      "prepared_record_sha256, committed_record_sha256",
      "FROM session_authority.filesystem_image_provider_operations",
      "WHERE provider_id = $1 AND anchor_id = $2",
      'ORDER BY operation_id COLLATE pg_catalog."C"',
    ].join(" "),
    [providerId, anchorId],
  );
  const expectedStored = new Map([
    [
      operationA,
      {
        committed: committedA,
        committedChecksum: committedAHead.lastChecksum,
        prepared: preparedA,
      },
    ],
    [
      operationZ,
      {
        committed: committedZ,
        committedChecksum: committedZHead.lastChecksum,
        prepared: preparedZ,
      },
    ],
    [
      projectionOriginOperationId,
      {
        committed: projectionOriginCommitted,
        committedChecksum: projectionOriginCommittedHead.lastChecksum,
        prepared: projectionOriginPrepared,
      },
    ],
    [
      projectionPrepared.operationId,
      {
        committed: null,
        committedChecksum: null,
        prepared: projectionPrepared,
      },
    ],
  ]);
  assert.equal(stored.rows.length, expectedStored.size);
  for (const row of stored.rows) {
    const expected = expectedStored.get(row.operation_id);
    assert.notEqual(expected, undefined);
    const committed = expected.committed !== null;
    assert.equal(row.state, committed ? "committed" : "prepared");
    assert.equal(row.prepared_checksum, expected.prepared.preparedChecksum);
    assert.equal(row.prepared_bytes > 0, true);
    assert.deepEqual(
      JSON.parse(row.prepared_record_bytes.toString("utf8")),
      expected.prepared,
    );
    if (committed) {
      assert.equal(row.committed_checksum_provenance, "indexed-frame-v1");
      assert.equal(row.committed_checksum, expected.committedChecksum);
      assert.equal(row.committed_bytes > row.prepared_bytes, true);
      assert.deepEqual(
        JSON.parse(row.committed_record_bytes.toString("utf8")),
        expected.committed,
      );
    } else {
      assert.equal(row.committed_checksum_provenance, null);
      assert.equal(row.committed_checksum, null);
      assert.equal(row.committed_record_bytes, null);
      assert.equal(row.committed_bytes, null);
      assert.equal(row.committed_record_sha256, null);
    }
    const materials = [
      [row.prepared_record_bytes, row.prepared_record_sha256],
    ];
    if (committed) {
      materials.push([
        row.committed_record_bytes,
        row.committed_record_sha256,
      ]);
    }
    for (const [bytes, digest] of materials) {
      assert.equal(
        createHash("sha256")
          .update(
            "portable-codex/filesystem-image-provider-state/operation-record/v1\0",
            "utf8",
          )
          .update(bytes)
          .digest("hex"),
        digest,
      );
    }
  }

  const permanentHistoryBeforeTruncate = await pool.query(
    [
      "SELECT",
      "(SELECT pg_catalog.count(*)::pg_catalog.text",
      "FROM session_authority.filesystem_image_provider_heads",
      "WHERE provider_id = $1) AS head_count,",
      "(SELECT pg_catalog.count(*)::pg_catalog.text",
      "FROM session_authority.filesystem_image_provider_operations",
      "WHERE provider_id = $1) AS operation_count",
    ].join(" "),
    [providerId],
  );
  assert.notEqual(permanentHistoryBeforeTruncate.rows[0].head_count, "0");
  assert.notEqual(permanentHistoryBeforeTruncate.rows[0].operation_count, "0");
  // Include each target's explicit FK closure so PostgreSQL reaches its first
  // listed BEFORE TRUNCATE guard instead of rejecting the statement early.
  await assert.rejects(
    pool.query(
      [
        "TRUNCATE TABLE session_authority.filesystem_image_provider_operations,",
        "session_authority.filesystem_image_provider_operation_events",
      ].join(" "),
    ),
    (error) => {
      assert.equal(error.code, "55000");
      assert.equal(
        error.constraint,
        "filesystem_image_provider_operations_truncate_forbidden",
      );
      return true;
    },
  );
  await assert.rejects(
    pool.query(
      [
        "TRUNCATE TABLE session_authority.filesystem_image_provider_heads,",
        "session_authority.filesystem_image_provider_operations,",
        "session_authority.filesystem_image_provider_operation_events",
      ].join(" "),
    ),
    (error) => {
      assert.equal(error.code, "55000");
      assert.equal(error.constraint, "fs_image_heads_truncate_forbidden");
      return true;
    },
  );
  await assert.rejects(
    pool.query(
      "TRUNCATE TABLE session_authority.filesystem_image_provider_operation_events",
    ),
    (error) => {
      assert.equal(error.code, "55000");
      assert.equal(error.constraint, "fs_image_operation_events_immutable");
      return true;
    },
  );
  await assert.rejects(
    pool.query(
      "TRUNCATE TABLE session_authority.filesystem_image_provider_anchor_lifecycle",
    ),
    (error) => {
      assert.equal(error.code, "55000");
      assert.equal(error.constraint, "fs_image_anchor_lifecycle_immutable");
      return true;
    },
  );
  assert.deepEqual(
    (await pool.query(
      [
        "SELECT",
        "(SELECT pg_catalog.count(*)::pg_catalog.text",
        "FROM session_authority.filesystem_image_provider_heads",
        "WHERE provider_id = $1) AS head_count,",
        "(SELECT pg_catalog.count(*)::pg_catalog.text",
        "FROM session_authority.filesystem_image_provider_operations",
        "WHERE provider_id = $1) AS operation_count",
      ].join(" "),
      [providerId],
    )).rows,
    permanentHistoryBeforeTruncate.rows,
  );

  await assert.rejects(
    pool.query(
      [
        "UPDATE session_authority.filesystem_image_provider_operations",
        "SET kind = 'attach'",
        "WHERE provider_id = $1 AND anchor_id = $2 AND operation_id = $3",
      ].join(" "),
      [providerId, acknowledgementLossAnchorId, acknowledgementLossOperationId],
    ),
    (error) => {
      assert.equal(error.code, "55000");
      assert.equal(
        error.constraint,
        "fs_image_operations_native_commit_only",
      );
      return true;
    },
  );
  await assert.rejects(
    pool.query(
      [
        "DELETE FROM session_authority.filesystem_image_provider_operations",
        "WHERE provider_id = $1 AND anchor_id = $2 AND operation_id = $3",
      ].join(" "),
      [providerId, acknowledgementLossAnchorId, acknowledgementLossOperationId],
    ),
    (error) => {
      assert.equal(error.code, "23503");
      assert.equal(
        error.constraint,
        "filesystem_image_provider_operations_delete_requires_teardown",
      );
      return true;
    },
  );

  const cleanup = await pool.connect();
  let cleanupTransactionOpen = false;
  try {
    await cleanup.query("BEGIN");
    cleanupTransactionOpen = true;
    await cleanup.query(
      [
        "DELETE FROM session_authority.filesystem_image_provider_operations",
        "WHERE provider_id = $1 AND anchor_id IN ($2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
      ].join(" "),
      [
        providerId,
        anchorId,
        legacyAnchorId,
        acknowledgementLossAnchorId,
        adoptedAnchorId,
        adoptionAcknowledgementLossAnchorId,
        adoptionValidationAnchorId,
        concurrentAdoptionAnchorId,
        lifecycleRaceAnchorId,
        revisionRaceAnchorId,
        streamingAdoptionAnchorId,
      ],
    );
    await cleanup.query(
      [
        "DELETE FROM session_authority.filesystem_image_provider_heads",
        "WHERE provider_id = $1 AND anchor_id IN ($2, $3, $4, $5, $6, $7, $8, $9, $10, $11)",
      ].join(" "),
      [
        providerId,
        anchorId,
        legacyAnchorId,
        acknowledgementLossAnchorId,
        adoptedAnchorId,
        adoptionAcknowledgementLossAnchorId,
        adoptionValidationAnchorId,
        concurrentAdoptionAnchorId,
        lifecycleRaceAnchorId,
        revisionRaceAnchorId,
        streamingAdoptionAnchorId,
      ],
    );
    await cleanup.query("COMMIT");
    cleanupTransactionOpen = false;
  } finally {
    if (cleanupTransactionOpen) await cleanup.query("ROLLBACK");
    cleanup.release();
  }
}

function frozenNullPrototypeRecord(entries) {
  return Object.freeze(Object.assign(Object.create(null), entries));
}

async function readSessionAuthorityMutationSnapshot(pool, sessionId) {
  const relations = [
    "sessions",
    "operation_claims",
    "operation_id_registry",
    "reservations",
    "capture_attempt_claims",
    "capture_attempt_tombstones",
    "checkpoint_catalogue",
    "restore_destination_generations",
    "detached_restore_stable_plans",
  ];
  const snapshot = Object.create(null);
  for (const relation of relations) {
    const result = await pool.query(
      [
        "SELECT COALESCE(",
        "jsonb_agg(to_jsonb(authority_row)",
        "ORDER BY to_jsonb(authority_row)::text),",
        "'[]'::jsonb) AS rows",
        `FROM session_authority.${relation} AS authority_row`,
        "WHERE session_id = $1::uuid",
      ].join(" "),
      [sessionId],
    );
    snapshot[relation] = result.rows[0].rows;
  }
  return snapshot;
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

async function installAuthorityMigration(pool, migration) {
  const client = await pool.connect();
  try {
    await client.query("BEGIN");
    await client.query("SET LOCAL search_path = pg_catalog");
    await client.query(migration.sql);
    await client.query(
      [
        "INSERT INTO session_authority.schema_migrations",
        "(version, checksum, applied_at)",
        "VALUES ($1, $2, pg_catalog.transaction_timestamp())",
      ].join(" "),
      [migration.version, migration.checksum],
    );
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

async function insertRawDetachedRestoreStablePlanClaim(
  queryable,
  {
    admission,
    bindingSha256,
    operationId,
    planSha256,
    sessionId,
  },
) {
  await queryable.query(
    [
      "INSERT INTO session_authority.operation_id_registry",
      "(operation_id, session_id, claim_type, claimant_operation_id,",
      "binding, claimed_at, materialized_at)",
      "VALUES ($1, $2::uuid, 'detached-restore-stable-plan-v1', NULL,",
      "pg_catalog.jsonb_build_object(",
      "'bindingSha256', $3::text,",
      "'contractVersion', 1,",
      "'planSha256', $4::text,",
      "'request', $5::jsonb),",
      "pg_catalog.transaction_timestamp(), NULL)",
    ].join(" "),
    [
      operationId,
      sessionId,
      bindingSha256,
      planSha256,
      JSON.stringify(admission.request),
    ],
  );
}

async function insertRawDetachedRestoreStablePlan(
  queryable,
  {
    admission,
    backendId,
    bindingSha256,
    operationId,
    planInput,
    planSha256,
    sessionId,
    storageId,
  },
) {
  return queryable.query(
    [
      "INSERT INTO session_authority.detached_restore_stable_plans",
      "(operation_id, session_id, backend_id, storage_id,",
      "plan_contract_version, admission, plan_input, plan_sha256,",
      "binding_sha256, provisioned_at)",
      "VALUES ($1::character varying(128), $2::uuid, $3, $4, 1,",
      "$5::jsonb, $6::jsonb,",
      "$7, $8, (",
      "SELECT claimed_at",
      "FROM session_authority.operation_id_registry",
      "WHERE operation_id = $1::character varying(128)",
      "AND session_id = $2::uuid",
      "))",
    ].join(" "),
    [
      operationId,
      sessionId,
      backendId,
      storageId,
      JSON.stringify(admission),
      JSON.stringify(planInput),
      planSha256,
      bindingSha256,
    ],
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
    version: 11,
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

async function assertWriterSupervisorStateOwnerMigrationGate(
  pool,
  store,
  trackedMigrations,
) {
  await pool.query("DROP SCHEMA IF EXISTS session_authority CASCADE");
  await installAuthorityMigrations(pool, trackedMigrations.slice(0, 8));

  const firstOperationId = `legacy-writer-launch-${randomUUID()}`;
  const firstSessionId = randomUUID();
  const secondOperationId = `legacy-writer-launch-${randomUUID()}`;
  const secondSessionId = randomUUID();
  const currentOperationId = `legacy-current-writer-launch-${randomUUID()}`;
  const currentSessionId = randomUUID();
  const staleOperationId = `post-migration-writer-launch-${randomUUID()}`;
  const boundOperationId = `post-migration-writer-launch-${randomUUID()}`;
  const supervisorId = `migration-supervisor-${randomUUID()}`;
  const stateOwnerId = `state-owner:${"a".repeat(64)}`;
  const replacementStateOwnerId = `state-owner:${"b".repeat(64)}`;
  const timestamp = await pool.query(
    "SELECT pg_catalog.transaction_timestamp() AS value",
  );
  const now = timestamp.rows[0].value;
  const request = JSON.stringify({
    payload: {
      contractVersion: 1,
      supervisor: { contractVersion: 1, supervisorId },
    },
  });

  for (const sessionId of [
    firstSessionId,
    secondSessionId,
    currentSessionId,
  ]) {
    await pool.query(
      [
        "INSERT INTO session_authority.sessions",
        "(session_id, document, created_at, updated_at)",
        "VALUES ($1, $2::jsonb, $3, $3)",
      ].join(" "),
      [sessionId, EMPTY_JSON_OBJECT, now],
    );
  }
  for (const [operationId, sessionId] of [
    [firstOperationId, firstSessionId],
    [secondOperationId, secondSessionId],
  ]) {
    await insertDirectOperationIdClaim(pool, {
      claimedAt: now,
      operationId,
      sessionId,
    });
    await pool.query(
      [
        "INSERT INTO session_authority.operation_claims",
        "(operation_id, session_id, kind, request, state, created_at, updated_at)",
        "VALUES ($1, $2, 'writer-launch-attempt-v1',",
        "$3::jsonb, 'prepared', $4, $4)",
      ].join(" "),
      [operationId, sessionId, request, now],
    );
  }

  const oldWriter = await pool.connect();
  let oldWriterTransactionOpen = false;
  try {
    await oldWriter.query("BEGIN");
    oldWriterTransactionOpen = true;
    const lockedSession = await oldWriter.query(
      [
        "SELECT session_id",
        "FROM session_authority.sessions",
        "WHERE session_id = $1",
        "FOR UPDATE",
      ].join(" "),
      [firstSessionId],
    );
    assert.equal(lockedSession.rows.length, 1);
    const migrationOutcome = store.migrate().then(
      (value) => ({ error: null, value }),
      (error) => ({ error, value: null }),
    );
    await waitForMigrationSessionTableLock(oldWriter);
    const starting = await oldWriter.query(
      [
        "UPDATE session_authority.operation_claims",
        "SET state = 'starting', revision = revision + 1, updated_at = $2",
        "WHERE operation_id = $1 AND state = 'prepared'",
        "RETURNING state, revision",
      ].join(" "),
      [firstOperationId, now],
    );
    assert.deepEqual(starting.rows, [{ revision: "1", state: "starting" }]);
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
    trackedMigrations.slice(0, 8).map(({ checksum, version }) => ({
      checksum,
      version,
    })),
  );
  const absentOwnerTable = await pool.query(
    "SELECT pg_catalog.to_regclass('session_authority.writer_supervisor_state_owners') AS value",
  );
  assert.equal(absentOwnerTable.rows[0].value, null);

  await pool.query(
    [
      "UPDATE session_authority.operation_claims",
      "SET state = 'prepared', revision = 0, updated_at = $2",
      "WHERE operation_id = $1",
    ].join(" "),
    [firstOperationId, now],
  );
  await pool.query(
    [
      "UPDATE session_authority.operation_claims",
      "SET state = 'uncertain', revision = 2, updated_at = $2",
      "WHERE operation_id = $1",
    ].join(" "),
    [secondOperationId, now],
  );
  await assert.rejects(store.migrate(), (error) => {
    assert.ok(error instanceof PostgresSerializableStoreError);
    assert.equal(error.code, "migration_failed");
    assert.equal(error.commitState, "not-committed");
    return true;
  });
  assert.deepEqual(
    await readMigrationLedger(pool),
    trackedMigrations.slice(0, 8).map(({ checksum, version }) => ({
      checksum,
      version,
    })),
  );
  await pool.query(
    [
      "UPDATE session_authority.operation_claims",
      "SET state = 'prepared', revision = 0, updated_at = $2",
      "WHERE operation_id = $1",
    ].join(" "),
    [secondOperationId, now],
  );

  await insertDirectOperationIdClaim(pool, {
    claimedAt: now,
    operationId: currentOperationId,
    sessionId: currentSessionId,
  });
  await pool.query(
    [
      "INSERT INTO session_authority.operation_claims",
      "(operation_id, session_id, kind, request, result, state, revision,",
      "created_at, updated_at, retired_at)",
      "VALUES ($1, $2, 'writer-launch-attempt-v1', $3::jsonb,",
      "$4::jsonb, 'committed', 2, $5, $5, $5)",
    ].join(" "),
    [
      currentOperationId,
      currentSessionId,
      request,
      JSON.stringify({
        evidence: { status: "started" },
        outcome: "writer-launch-started",
        resultVersion: 1,
      }),
      now,
    ],
  );
  await pool.query(
    [
      "UPDATE session_authority.sessions",
      "SET document = $2::jsonb, revision = revision + 1, updated_at = $3",
      "WHERE session_id = $1",
    ].join(" "),
    [
      currentSessionId,
      // The migration gate is deliberately shape-agnostic: any non-null
      // current-launch pointer blocks until the session is drained or fenced.
      JSON.stringify({ launch: { launchAttemptId: currentOperationId } }),
      now,
    ],
  );
  await assert.rejects(store.migrate(), (error) => {
    assert.ok(error instanceof PostgresSerializableStoreError);
    assert.equal(error.code, "migration_failed");
    assert.equal(error.commitState, "not-committed");
    return true;
  });
  assert.deepEqual(
    await readMigrationLedger(pool),
    trackedMigrations.slice(0, 8).map(({ checksum, version }) => ({
      checksum,
      version,
    })),
  );
  const absentOwnerTableAfterCurrentLaunch = await pool.query(
    "SELECT pg_catalog.to_regclass('session_authority.writer_supervisor_state_owners') AS value",
  );
  assert.equal(absentOwnerTableAfterCurrentLaunch.rows[0].value, null);
  await pool.query(
    [
      "UPDATE session_authority.sessions",
      "SET document = $2::jsonb, revision = revision + 1, updated_at = $3",
      "WHERE session_id = $1",
    ].join(" "),
    [currentSessionId, JSON.stringify({ launch: null }), now],
  );

  await pool.query(
    [
      "UPDATE session_authority.operation_claims",
      "SET state = 'committed', result = $2::jsonb, revision = 1,",
      "updated_at = $3, retired_at = $3",
      "WHERE operation_id = $1 AND state = 'prepared'",
    ].join(" "),
    [
      firstOperationId,
      JSON.stringify({
        outcome: "cancelled-before-dispatch",
        reason: "legacy-migration-cancellation",
        resultVersion: 1,
      }),
      now,
    ],
  );
  await pool.query(
    [
      "UPDATE session_authority.operation_claims",
      "SET state = 'committed', result = $2::jsonb, revision = 1,",
      "updated_at = $3, retired_at = $3",
      "WHERE operation_id = $1 AND state = 'prepared'",
    ].join(" "),
    [
      secondOperationId,
      JSON.stringify({
        outcome: "writer-launch-complete-stopped",
        resultVersion: 1,
      }),
      now,
    ],
  );
  for (const [operationId, sessionId] of [
    [staleOperationId, firstSessionId],
    [boundOperationId, secondSessionId],
  ]) {
    await insertDirectOperationIdClaim(pool, {
      claimedAt: now,
      operationId,
      sessionId,
    });
    await pool.query(
      [
        "INSERT INTO session_authority.operation_claims",
        "(operation_id, session_id, kind, request, state, created_at, updated_at)",
        "VALUES ($1, $2, 'writer-launch-attempt-v1',",
        "$3::jsonb, 'prepared', $4, $4)",
      ].join(" "),
      [operationId, sessionId, request, now],
    );
  }

  assert.deepEqual(await store.migrate(), {
    applied: true,
    checksum: trackedMigrations.at(-1).checksum,
    version: 11,
  });
  const legacyTerminal = await pool.query(
    [
      "SELECT operation_id, result #>> '{outcome}' AS outcome",
      "FROM session_authority.operation_claims",
      "WHERE operation_id IN ($1, $2, $3)",
      "ORDER BY operation_id",
    ].join(" "),
    [firstOperationId, secondOperationId, currentOperationId],
  );
  assert.deepEqual(
    legacyTerminal.rows,
    [
      {
        operation_id: firstOperationId,
        outcome: "cancelled-before-dispatch",
      },
      {
        operation_id: secondOperationId,
        outcome: "writer-launch-complete-stopped",
      },
      {
        operation_id: currentOperationId,
        outcome: "writer-launch-started",
      },
    ].sort((left, right) =>
      left.operation_id.localeCompare(right.operation_id),
    ),
  );
  const unboundLegacyCurrent = await pool.query(
    [
      "SELECT launch_attempt_id",
      "FROM session_authority.writer_supervisor_state_owners",
      "WHERE launch_attempt_id = $1",
    ].join(" "),
    [currentOperationId],
  );
  assert.deepEqual(unboundLegacyCurrent.rows, []);

  const staleWriter = await pool.connect();
  let staleWriterTransactionOpen = false;
  try {
    await staleWriter.query("BEGIN");
    staleWriterTransactionOpen = true;
    const starting = await staleWriter.query(
      [
        "UPDATE session_authority.operation_claims",
        "SET state = 'starting', revision = revision + 1, updated_at = $2",
        "WHERE operation_id = $1 AND state = 'prepared'",
        "RETURNING state, revision",
      ].join(" "),
      [staleOperationId, now],
    );
    assert.deepEqual(starting.rows, [{ revision: "1", state: "starting" }]);
    await assert.rejects(staleWriter.query("COMMIT"), (error) => {
      assert.equal(error.code, "23514");
      assert.equal(
        error.constraint,
        "operation_claims_writer_launch_state_owner",
      );
      return true;
    });
  } finally {
    if (staleWriterTransactionOpen) await staleWriter.query("ROLLBACK");
    staleWriter.release();
  }
  const ownerless = await pool.query(
    [
      "SELECT state, revision::text AS revision",
      "FROM session_authority.operation_claims",
      "WHERE operation_id = $1",
    ].join(" "),
    [staleOperationId],
  );
  assert.deepEqual(ownerless.rows, [{ revision: "0", state: "prepared" }]);

  const cancelled = await pool.query(
    [
      "UPDATE session_authority.operation_claims",
      "SET state = 'committed', result = $2::jsonb,",
      "revision = revision + 1, updated_at = $3, retired_at = $3",
      "WHERE operation_id = $1 AND state = 'prepared'",
      "RETURNING state, result, revision::text AS revision",
    ].join(" "),
    [
      staleOperationId,
      JSON.stringify({
        outcome: "cancelled-before-dispatch",
        reason: "migration-ownerless-cancellation",
        resultVersion: 1,
      }),
      now,
    ],
  );
  assert.deepEqual(cancelled.rows, [
    {
      result: {
        outcome: "cancelled-before-dispatch",
        reason: "migration-ownerless-cancellation",
        resultVersion: 1,
      },
      revision: "1",
      state: "committed",
    },
  ]);

  const earlyBindingWriter = await pool.connect();
  let earlyBindingTransactionOpen = false;
  try {
    await earlyBindingWriter.query("BEGIN");
    earlyBindingTransactionOpen = true;
    const committed = await earlyBindingWriter.query(
      [
        "UPDATE session_authority.operation_claims",
        "SET state = 'committed', result = $2::jsonb, revision = 1,",
        "updated_at = $3, retired_at = $3",
        "WHERE operation_id = $1 AND state = 'prepared'",
        "RETURNING session_id",
      ].join(" "),
      [
        boundOperationId,
        JSON.stringify({
          outcome: "writer-launch-complete-stopped",
          resultVersion: 1,
        }),
        now,
      ],
    );
    assert.equal(committed.rows.length, 1);
    await earlyBindingWriter.query(
      [
        "INSERT INTO session_authority.writer_supervisor_state_owners",
        "(launch_attempt_id, session_id, supervisor_id, state_owner_id, bound_at)",
        "VALUES ($1, $2, $3, $4, $5)",
      ].join(" "),
      [
        boundOperationId,
        committed.rows[0].session_id,
        supervisorId,
        stateOwnerId,
        new Date(now.getTime() - 1_000),
      ],
    );
    await assert.rejects(earlyBindingWriter.query("COMMIT"), (error) => {
      assert.equal(error.code, "23514");
      assert.equal(
        error.constraint,
        "operation_claims_writer_launch_state_owner",
      );
      return true;
    });
  } finally {
    if (earlyBindingTransactionOpen) {
      await earlyBindingWriter.query("ROLLBACK");
    }
    earlyBindingWriter.release();
  }

  const boundWriter = await pool.connect();
  let boundWriterTransactionOpen = false;
  try {
    await boundWriter.query("BEGIN");
    boundWriterTransactionOpen = true;
    const starting = await boundWriter.query(
      [
        "UPDATE session_authority.operation_claims",
        "SET state = 'starting', revision = revision + 1,",
        "updated_at = $2",
        "WHERE operation_id = $1 AND state = 'prepared'",
        "RETURNING session_id, updated_at",
      ].join(" "),
      [boundOperationId, now],
    );
    assert.equal(starting.rows.length, 1);
    await boundWriter.query(
      [
        "INSERT INTO session_authority.writer_supervisor_state_owners",
        "(launch_attempt_id, session_id, supervisor_id, state_owner_id, bound_at)",
        "VALUES ($1, $2, $3, $4, $5)",
      ].join(" "),
      [
        boundOperationId,
        starting.rows[0].session_id,
        supervisorId,
        stateOwnerId,
        starting.rows[0].updated_at,
      ],
    );
    await boundWriter.query("COMMIT");
    boundWriterTransactionOpen = false;
  } finally {
    if (boundWriterTransactionOpen) await boundWriter.query("ROLLBACK");
    boundWriter.release();
  }
  const bound = await pool.query(
    [
      "SELECT launch.state, owner.state_owner_id",
      "FROM session_authority.operation_claims AS launch",
      "JOIN session_authority.writer_supervisor_state_owners AS owner",
      "ON owner.launch_attempt_id = launch.operation_id",
      "AND owner.session_id = launch.session_id",
      "WHERE launch.operation_id = $1",
    ].join(" "),
    [boundOperationId],
  );
  assert.deepEqual(bound.rows, [
    { state: "starting", state_owner_id: stateOwnerId },
  ]);

  await assert.rejects(
    pool.query(
      "DELETE FROM session_authority.writer_supervisor_state_owners WHERE launch_attempt_id = $1",
      [boundOperationId],
    ),
    (error) => {
      assert.equal(error.code, "23503");
      assert.equal(
        error.constraint,
        "writer_supervisor_state_owners_delete_requires_claim_teardown",
      );
      return true;
    },
  );

  const rebindWriter = await pool.connect();
  let rebindWriterTransactionOpen = false;
  try {
    await rebindWriter.query("BEGIN");
    rebindWriterTransactionOpen = true;
    await rebindWriter.query(
      "DELETE FROM session_authority.writer_supervisor_state_owners WHERE launch_attempt_id = $1",
      [boundOperationId],
    );
    await rebindWriter.query(
      [
        "INSERT INTO session_authority.writer_supervisor_state_owners",
        "(launch_attempt_id, session_id, supervisor_id, state_owner_id, bound_at)",
        "VALUES ($1, $2, $3, $4, $5)",
      ].join(" "),
      [
        boundOperationId,
        secondSessionId,
        supervisorId,
        replacementStateOwnerId,
        now,
      ],
    );
    await assert.rejects(rebindWriter.query("COMMIT"), (error) => {
      assert.equal(error.code, "23503");
      assert.equal(
        error.constraint,
        "writer_supervisor_state_owners_delete_requires_claim_teardown",
      );
      return true;
    });
  } finally {
    if (rebindWriterTransactionOpen) await rebindWriter.query("ROLLBACK");
    rebindWriter.release();
  }
  const stillBound = await pool.query(
    [
      "SELECT state_owner_id",
      "FROM session_authority.writer_supervisor_state_owners",
      "WHERE launch_attempt_id = $1",
    ].join(" "),
    [boundOperationId],
  );
  assert.deepEqual(stillBound.rows, [{ state_owner_id: stateOwnerId }]);

  const cleanup = await pool.connect();
  let cleanupTransactionOpen = false;
  try {
    await cleanup.query("BEGIN");
    cleanupTransactionOpen = true;
    await cleanup.query(
      "DELETE FROM session_authority.writer_supervisor_state_owners WHERE launch_attempt_id = $1",
      [boundOperationId],
    );
    await cleanup.query(
      [
        "DELETE FROM session_authority.operation_claims",
        "WHERE operation_id IN ($1, $2, $3, $4, $5)",
      ].join(" "),
      [
        firstOperationId,
        secondOperationId,
        currentOperationId,
        staleOperationId,
        boundOperationId,
      ],
    );
    await cleanup.query(
      [
        "DELETE FROM session_authority.operation_id_registry",
        "WHERE operation_id IN ($1, $2, $3, $4, $5)",
      ].join(" "),
      [
        firstOperationId,
        secondOperationId,
        currentOperationId,
        staleOperationId,
        boundOperationId,
      ],
    );
    await cleanup.query(
      "DELETE FROM session_authority.sessions WHERE session_id IN ($1, $2, $3)",
      [firstSessionId, secondSessionId, currentSessionId],
    );
    await cleanup.query("COMMIT");
    cleanupTransactionOpen = false;
  } finally {
    if (cleanupTransactionOpen) await cleanup.query("ROLLBACK");
    cleanup.release();
  }
}

async function waitForBackendLockWait(observer, backendPid) {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    await observer.query("SELECT pg_catalog.pg_stat_clear_snapshot()");
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
    {
      character_maximum_length: null,
      column_name: "after_authorized_at",
      data_type: "timestamp with time zone",
      is_nullable: "YES",
    },
    {
      character_maximum_length: 128,
      column_name: "after_terminal_operation_id",
      data_type: "character varying",
      is_nullable: "YES",
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
      "restore_recovery_cursors_gc_position_shape",
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
        constraint: "restore_recovery_cursors_gc_position_shape",
        values: [
          `constraint-${randomUUID()}`,
          "supervisor-state-gc",
          randomUUID(),
          0,
          1,
          transitionId,
          requestSha256,
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
    "supervisor-state-gc",
  ];
  const lazyScopeId = `lazy-${randomUUID()}`;
  const concurrentScopeId = `concurrent-${randomUUID()}`;
  const acknowledgementLossScopeId = `ack-loss-${randomUUID()}`;
  const gcScopeId = `gc-${randomUUID()}`;
  const scopeIds = [
    lazyScopeId,
    concurrentScopeId,
    acknowledgementLossScopeId,
    gcScopeId,
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
      { lane: "supervisor-state-gc", revision: "0" },
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

    const gcInitial = await cursorStore.readLane({
      lane: "supervisor-state-gc",
      recoveryScopeId: gcScopeId,
    });
    assert.equal(gcInitial.afterAuthorizedAt, null);
    assert.equal(gcInitial.afterSessionId, null);
    assert.equal(gcInitial.afterTerminalOperationId, null);
    const gcAuthorizedAt = new Date().toISOString();
    const gcSessionId = randomUUID();
    const gcTerminalOperationId = `gc-terminal-${randomUUID()}`;
    const gcAdvanceInput = {
      expectedAfterAuthorizedAt: null,
      expectedAfterSessionId: null,
      expectedAfterTerminalOperationId: null,
      expectedCycle: gcInitial.cycle,
      expectedRevision: gcInitial.revision,
      lane: gcInitial.lane,
      nextAfterAuthorizedAt: gcAuthorizedAt,
      nextAfterSessionId: gcSessionId,
      nextAfterTerminalOperationId: gcTerminalOperationId,
      recoveryScopeId: gcInitial.recoveryScopeId,
      requestSha256: "e".repeat(64),
      transitionId: randomUUID(),
    };
    const gcAdvanced = await cursorStore.advanceLane(gcAdvanceInput);
    assert.equal(gcAdvanced.cursor.afterAuthorizedAt, gcAuthorizedAt);
    assert.equal(gcAdvanced.cursor.afterSessionId, gcSessionId);
    assert.equal(
      gcAdvanced.cursor.afterTerminalOperationId,
      gcTerminalOperationId,
    );
    assert.deepEqual(
      await cursorStore.readLane({
        lane: "supervisor-state-gc",
        recoveryScopeId: gcScopeId,
      }),
      gcAdvanced.cursor,
    );
    const gcWrapped = await cursorStore.advanceLane({
      expectedAfterAuthorizedAt: gcAuthorizedAt,
      expectedAfterSessionId: gcSessionId,
      expectedAfterTerminalOperationId: gcTerminalOperationId,
      expectedCycle: gcAdvanced.cursor.cycle,
      expectedRevision: gcAdvanced.cursor.revision,
      lane: gcAdvanced.cursor.lane,
      nextAfterAuthorizedAt: null,
      nextAfterSessionId: null,
      nextAfterTerminalOperationId: null,
      recoveryScopeId: gcAdvanced.cursor.recoveryScopeId,
      requestSha256: "d".repeat(64),
      transitionId: randomUUID(),
    });
    assert.equal(gcWrapped.cursor.afterAuthorizedAt, null);
    assert.equal(gcWrapped.cursor.afterSessionId, null);
    assert.equal(gcWrapped.cursor.afterTerminalOperationId, null);
    assert.equal(gcWrapped.cursor.cycle, "1");

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

function integrationDetachedRestoreImagePlan(session) {
  return createPostgresDetachedRestorePlan({
    request: {
      backendId: session.document.storageRef.backendId,
      contractVersion: 1,
      fencingEpoch: "1",
      holderId: `integration-image-holder-${randomUUID()}`,
      leaseId: `integration-image-lease-${randomUUID()}`,
      operation: "restore",
      operationId: `integration-image-restore-${randomUUID()}`,
      sessionId: session.sessionId,
      storageId: session.document.storageRef.storageId,
      target: {
        artifactId: `integration-image-artifact-${randomUUID()}`,
        checkpointId: `integration-image-checkpoint-${randomUUID()}`,
        kind: "checkpoint",
      },
    },
    plan: {
      captureCreatedAt: new Date().toISOString(),
      destinationDirectory:
        `/var/lib/portable-codex-restores/${session.sessionId}`,
      destinationOwnedRoot: "/var/lib/portable-codex-restores",
      detachMode: "release",
      holderId: `integration-image-launch-holder-${randomUUID()}`,
      imagePlanId: `integration-image-plan-${randomUUID()}`,
      leaseDurationMilliseconds: 300_000,
      sourceArtifactDirectory:
        `/var/lib/portable-codex-checkpoints/${session.sessionId}`,
      sourceArtifactOwnedRoot: "/var/lib/portable-codex-checkpoints",
    },
  });
}

function integrationImagePlanSettlementPolicies() {
  return {
    inspectCodex: {
      deadlineMilliseconds: 30_000,
      settlementGraceMilliseconds: 5_000,
    },
    resolveImagePlan: {
      deadlineMilliseconds: 45_000,
      settlementGraceMilliseconds: 10_000,
    },
  };
}

function integrationImagePlanSettlements(
  policies = integrationImagePlanSettlementPolicies(),
) {
  const onFatal = Object.freeze(function onFatal() {
    assert.fail("integration image-plan provider must settle");
  });
  return Object.freeze({
    inspectCodex: createPhysicalCollaboratorSettlement({
      deadlineMilliseconds: policies.inspectCodex.deadlineMilliseconds,
      onFatal,
      settlementGraceMilliseconds:
        policies.inspectCodex.settlementGraceMilliseconds,
    }),
    resolveImagePlan: createPhysicalCollaboratorSettlement({
      deadlineMilliseconds: policies.resolveImagePlan.deadlineMilliseconds,
      onFatal,
      settlementGraceMilliseconds:
        policies.resolveImagePlan.settlementGraceMilliseconds,
    }),
  });
}

function integrationDeploymentPhysicalSettlementPolicies() {
  const policy = () => ({
    deadlineMilliseconds: 120_000,
    settlementGraceMilliseconds: 30_000,
  });
  const group = (methods) =>
    Object.fromEntries(methods.map((method) => [method, policy()]));
  return {
    lifecycleBackendSettlement: group([
      "captureCheckpoint",
      "destroySession",
      "detachAttachment",
      "forceFence",
      "prepareRestoreAttachment",
      "prepareWritableAttachment",
      "provisionSession",
      "reconcileRestoreAttachment",
      "restoreCheckpoint",
    ]),
    publicationSettlement: group([
      "publishFreshCheckpointArtifact",
      "publishRestoreDestination",
      "verifyCommittedCheckpointArtifact",
      "verifyCommittedRestoreDestination",
    ]),
    resolveRestoreDestinationSettlement: policy(),
    supervisorSettlement: group([
      "launchWriter",
      "reconcileWriterLaunch",
      "stopWriter",
    ]),
    supervisorStateCollectionSettlement: policy(),
  };
}

function assertFreshOpaqueInvocation(invocation, seen) {
  assert.equal(Object.getPrototypeOf(invocation), null);
  assert.equal(Object.isFrozen(invocation), true);
  assert.deepEqual(Reflect.ownKeys(invocation), []);
  assert.equal(seen.has(invocation), false);
  seen.add(invocation);
}

function assertPublicCheckpointBackendSurface(backend) {
  assert.equal(Object.getPrototypeOf(backend), null);
  assert.equal(Object.isFrozen(backend), true);
  assert.deepEqual(Reflect.ownKeys(backend).sort(), [
    "backendId",
    "capabilities",
    "captureCheckpoint",
    "contractVersion",
    "restoreCheckpoint",
  ]);
  assert.equal(backend.backendId, "postgres-authority-integration");
  assert.equal(backend.contractVersion, 1);
  assert.equal(Object.isFrozen(backend.capabilities), true);
  assert.deepEqual(Reflect.ownKeys(backend.capabilities).sort(), [
    "atomicPointInTimeCheckpoint",
    "exclusiveWriterAttachment",
    "fencing",
    "normalDirectoryAttachment",
  ]);
  assert.deepEqual({ ...backend.capabilities }, {
    atomicPointInTimeCheckpoint: true,
    exclusiveWriterAttachment: true,
    fencing: "epoch-enforced",
    normalDirectoryAttachment: true,
  });
  assert.equal(typeof backend.captureCheckpoint, "function");
  assert.equal(typeof backend.restoreCheckpoint, "function");
  assert.equal(Object.isFrozen(backend.captureCheckpoint), true);
  assert.equal(Object.isFrozen(backend.restoreCheckpoint), true);
  for (const hidden of [
    "destroySession",
    "detachAttachment",
    "forceFence",
    "prepareWritableAttachment",
    "provisionSession",
  ]) {
    assert.equal(hidden in backend, false);
  }
}

function integrationImagePlanBindingFixture({ image, plan, session }) {
  const providerCalls = { inspect: 0, resolve: 0 };
  const providerInvocations = new Set();
  const providerSignals = new Set();
  const imagePlanProviderId = `integration-image-provider-${randomUUID()}`;
  const boundPlan = plan ?? integrationDetachedRestoreImagePlan(session);
  const provider = Object.freeze({
    contractVersion:
      POSTGRES_DETACHED_RESTORE_IMAGE_PLAN_PROVIDER_CONTRACT_VERSION,
    imagePlanProviderId,
    async inspectCodex(input) {
      providerCalls.inspect += 1;
      assert.equal(Object.getPrototypeOf(input), null);
      assert.equal(Object.isFrozen(input), true);
      assert.deepEqual(Reflect.ownKeys(input).sort(), [
        "imagePlanId",
        "imagePlanProviderId",
        "inspection",
        "invocation",
        "signal",
      ]);
      assertFreshOpaqueInvocation(input.invocation, providerInvocations);
      assert.equal(input.signal instanceof AbortSignal, true);
      assert.equal(input.signal.aborted, false);
      assert.equal(providerSignals.has(input.signal), false);
      providerSignals.add(input.signal);
      assert.equal(input.imagePlanId, boundPlan.imagePlanId);
      assert.equal(input.imagePlanProviderId, imagePlanProviderId);
      assert.equal(
        Object.getPrototypeOf(input.inspection),
        Object.prototype,
      );
      assert.equal(Object.isFrozen(input.inspection), true);
      assert.deepEqual(Reflect.ownKeys(input.inspection).sort(), [
        "codexSandbox",
        "codexVersion",
        "platformImage",
      ]);
      assert.equal(
        input.inspection.codexVersion,
        session.document.manifest.runtime.codexVersion,
      );
      return frozenNullPrototypeRecord({
        codexBinaryPath: "/opt/portable-codex/bin/codex",
        codexBinarySha256: "c".repeat(64),
        codexVersion: session.document.manifest.runtime.codexVersion,
      });
    },
    async resolveImagePlan(input) {
      providerCalls.resolve += 1;
      assert.equal(Object.getPrototypeOf(input), null);
      assert.equal(Object.isFrozen(input), true);
      assert.deepEqual(Reflect.ownKeys(input).sort(), [
        "imagePlanId",
        "imagePlanProviderId",
        "invocation",
        "sessionManifest",
        "signal",
      ]);
      assertFreshOpaqueInvocation(input.invocation, providerInvocations);
      assert.equal(input.signal instanceof AbortSignal, true);
      assert.equal(input.signal.aborted, false);
      assert.equal(providerSignals.has(input.signal), false);
      providerSignals.add(input.signal);
      assert.equal(input.imagePlanId, boundPlan.imagePlanId);
      assert.equal(input.imagePlanProviderId, imagePlanProviderId);
      assert.deepEqual(input.sessionManifest, session.document.manifest);
      return frozenNullPrototypeRecord({
        configBytes: image.configBytes,
        descriptor: Object.freeze({ ...image.descriptor }),
      });
    },
  });
  return Object.freeze({
    binding: createPostgresDetachedRestoreImagePlanBinding({
      provider,
      settlement: integrationImagePlanSettlements(),
    }),
    plan: boundPlan,
    provider,
    providerCalls,
    providerInvocations,
    providerSignals,
  });
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
        query(...args) {
          const input = args[0];
          const text =
            typeof input === "string" ? input : input?.text;
          const result = Reflect.apply(
            client.query,
            client,
            args,
          );
          // A custom pg.Query must be returned synchronously with its identity
          // intact so the bounded row-stream path can observe its protocol.
          if (result === input) return result;
          return (async () => {
            const value = await result;
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
            return value;
          })();
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
    didFail() {
      return failed;
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

async function waitForApplicationAdvisoryLock(
  queryable,
  { applicationName, mode },
) {
  const deadline = Date.now() + 10_000;
  while (Date.now() < deadline) {
    const result = await queryable.query(
      [
        "SELECT pg_catalog.count(*)::integer AS lock_count",
        "FROM pg_catalog.pg_locks AS locks",
        "JOIN pg_catalog.pg_stat_activity AS activity",
        "ON activity.pid = locks.pid",
        "WHERE locks.locktype = 'advisory'",
        "AND locks.granted",
        "AND locks.mode = $2",
        "AND activity.application_name = $1",
      ].join(" "),
      [applicationName, mode],
    );
    if (result.rows[0].lock_count > 0) return;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  throw new Error(
    `timed out waiting for ${mode} advisory lock from ${applicationName}`,
  );
}

async function waitForDeploymentApplicationSessions(
  queryable,
  applicationNames,
  expectedCount,
) {
  const deadline = Date.now() + 10_000;
  while (Date.now() < deadline) {
    await queryable.query("SELECT pg_catalog.pg_stat_clear_snapshot()");
    const result = await queryable.query(
      [
        "SELECT application_name, pg_catalog.count(*)::integer AS session_count",
        "FROM pg_catalog.pg_stat_activity",
        "WHERE application_name = ANY($1::text[])",
        "GROUP BY application_name",
        "ORDER BY application_name",
      ].join(" "),
      [applicationNames],
    );
    const count = result.rows.reduce(
      (total, row) => total + row.session_count,
      0,
    );
    if (count === expectedCount) return result.rows;
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
  assert.fail(
    `deployment application session count did not reach ${expectedCount}`,
  );
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

function attachmentEvidence(
  mutationRequest,
  {
    rootPath =
      `/var/lib/portable-codex/${mutationRequest.sessionId}`,
  } = {},
) {
  const proofId = `proof-${randomUUID()}`;
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
    ...attachmentEvidence(starting.mutationRequest, options),
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

function restoreRuntimeIntegrationLifecycleBackend(
  calls,
  physicalInvocations = null,
  physicalSignals = null,
) {
  const unexpectedProviderCall = async function unexpectedProviderCall(
    input,
    physicalContext,
  ) {
    calls.provider += 1;
    void input;
    if (physicalInvocations !== null) {
      assert.equal(arguments.length, 2);
      assert.deepEqual(Reflect.ownKeys(physicalContext).sort(), [
        "contractVersion",
        "invocation",
        "signal",
      ]);
      assert.equal(physicalContext.contractVersion, 1);
      assertFreshOpaqueInvocation(
        physicalContext.invocation,
        physicalInvocations,
      );
      assert.equal(physicalContext.signal instanceof AbortSignal, true);
      assert.equal(physicalContext.signal.aborted, false);
      assert.equal(physicalSignals.has(physicalContext.signal), false);
      physicalSignals.add(physicalContext.signal);
    }
    throw new Error("restore runtime lifecycle provider must not run");
  };
  return Object.freeze({
    backendId: "postgres-authority-integration",
    capabilities: Object.freeze({
      atomicPointInTimeCheckpoint: true,
      exclusiveWriterAttachment: true,
      fencing: "epoch-enforced",
      normalDirectoryAttachment: true,
    }),
    captureCheckpoint: unexpectedProviderCall,
    contractVersion: 1,
    destroySession: unexpectedProviderCall,
    detachAttachment: unexpectedProviderCall,
    forceFence: unexpectedProviderCall,
    physicalInvocationContractVersion: 1,
    prepareRestoreAttachment: unexpectedProviderCall,
    prepareWritableAttachment: unexpectedProviderCall,
    provisionSession: unexpectedProviderCall,
    reconcileRestoreAttachment: unexpectedProviderCall,
    restoreAttachmentActivationContractVersion:
      RESTORE_ATTACHMENT_ACTIVATION_CONTRACT_VERSION,
    restoreAttachmentReconciliationContractVersion:
      RESTORE_ATTACHMENT_RECONCILIATION_CONTRACT_VERSION,
    restoreCheckpoint: unexpectedProviderCall,
  });
}

function restoreRuntimeIntegrationPublication(calls, recoveryScopeId) {
  const controlledPublicationFailure =
    async function controlledPublicationFailure() {
      calls.publication += 1;
      throw new Error("controlled restore runtime publication failure");
    };
  const journal = new FilesystemOperationJournal({
    acquireLock: controlledPublicationFailure,
    directory:
      `/var/lib/portable-codex/runtime-${recoveryScopeId}-journal`,
    inspectAncestorAcl: controlledPublicationFailure,
    inspectDirectoryAcl: controlledPublicationFailure,
    inspectTemporaryRecord: controlledPublicationFailure,
    syncDirectory: controlledPublicationFailure,
  });
  return new StoppedDirectoryPublication({
    acquireLock: controlledPublicationFailure,
    inspectFilesystem: controlledPublicationFailure,
    inspectOwnedRootAcl: controlledPublicationFailure,
    inspectOwnedRootAncestorAcl: controlledPublicationFailure,
    inspectPersistentObjectIdentity: controlledPublicationFailure,
    journal,
    listMountPoints: controlledPublicationFailure,
  });
}

function integrationPublicationDestinationChangedError() {
  const error = new Error("integration publication destination changed");
  error.code = "destination_changed";
  Object.defineProperty(error, "renameOutcome", {
    value: "not-committed",
  });
  return error;
}

function integrationPublicationLockProvider() {
  return async () => ({
    async assertHeld() {},
    async release() {},
    async renameWhileHeld(source, destination, expectedDestination) {
      if (expectedDestination?.kind === "absent") {
        try {
          await lstat(destination);
        } catch (error) {
          if (error?.code !== "ENOENT") throw error;
          await rename(source, destination);
          return;
        }
        throw integrationPublicationDestinationChangedError();
      }
      await rename(source, destination);
    },
  });
}

async function inspectIntegrationPersistentObjectIdentity(path) {
  const metadata = await lstat(path, { bigint: true });
  return {
    device: metadata.dev.toString(),
    inode: metadata.ino.toString(),
    objectId:
      `integration-object-${metadata.dev}-${metadata.ino}-${metadata.birthtimeNs}`,
  };
}

async function createRestoreRuntimePublicationTree() {
  const root = await realpath(
    await mkdtemp(
      join(tmpdir(), "portable-codex-runtime-publication-"),
    ),
  );
  try {
    const sourceOwnedRoot = join(root, "source-root");
    const artifactOwnedRoot = join(root, "artifact-root");
    const destinationOwnedRoot = join(root, "destination-root");
    const journalDirectory = join(root, "journal");
    for (const directory of [
      sourceOwnedRoot,
      artifactOwnedRoot,
      destinationOwnedRoot,
      journalDirectory,
    ]) {
      await mkdir(directory, { mode: 0o700 });
    }
    const sourceDirectory = join(sourceOwnedRoot, "session");
    const recoverySourceDirectory = join(
      sourceOwnedRoot,
      "recovery-session",
    );
    for (const directory of [sourceDirectory, recoverySourceDirectory]) {
      await mkdir(join(directory, "workspace", "nested"), {
        mode: 0o700,
        recursive: true,
      });
      await writeFile(
        join(directory, "workspace", "README.md"),
        "portable runtime integration\n",
        { mode: 0o640 },
      );
      await writeFile(
        join(directory, "workspace", "nested", "state.jsonl"),
        '{"type":"turn","state":"completed"}\n',
        { mode: 0o600 },
      );
      await symlink(
        "README.md",
        join(directory, "workspace", "current"),
      );
    }
    return Object.freeze({
      artifactOwnedRoot,
      destinationDirectory: join(
        destinationOwnedRoot,
        "restored-session",
      ),
      destinationOwnedRoot,
      journalDirectory,
      recoveryDestinationDirectory: join(
        destinationOwnedRoot,
        "recovery-session",
      ),
      recoverySourceDirectory,
      root,
      sourceDirectory,
      sourceOwnedRoot,
    });
  } catch (error) {
    await rm(root, { force: true, recursive: true });
    throw error;
  }
}

function restoreRuntimePublicationEvidence(
  calls,
  observation,
  journalDirectory,
) {
  class RestoreRuntimeOperationJournal extends FilesystemOperationJournal {
    async prepare(options) {
      if (options.operationId === observation.restoreOperationId) {
        calls.publishRestoreDestination += 1;
        assert.deepEqual(Reflect.ownKeys(options).sort(), [
          "binding",
          "operationId",
          "request",
          "result",
        ]);
        assert.equal(Object.isFrozen(options), true);
        assert.equal(
          options.binding.publication.publicationKind,
          "restore-destination",
        );
        assert.equal(options.request.operation, "restore");
        assert.equal(options.result.mutation.operation, "restore");
        observation.restoreVerificationInput = Object.freeze({
          artifactProof:
            options.binding.publication.source.artifactProof,
          binding: options.binding.coordinator,
          operationId: options.operationId,
          request: options.request,
          result: options.result,
        });
        observation.events.push("publishRestoreDestination");
      }
      return super.prepare(options);
    }

    async read(options) {
      const result = await super.read(options);
      if (options.operationId === observation.captureFailureOperationId) {
        calls.verifyCommittedCheckpointArtifact += 1;
      }
      if (
        options.operationId === observation.restoreOperationId &&
        result.record?.state === "committed"
      ) {
        calls.verifyCommittedRestoreDestination += 1;
        assert.deepEqual(Reflect.ownKeys(options), ["operationId"]);
        observation.events.push("verifyCommittedRestoreDestination");
      }
      return result;
    }
  }

  const journal = new RestoreRuntimeOperationJournal({
    acquireLock: integrationPublicationLockProvider(),
    directory: journalDirectory,
    inspectAncestorAcl: async () => false,
    inspectDirectoryAcl: async () => false,
  });
  return new StoppedDirectoryPublication({
    acquireLock: integrationPublicationLockProvider(),
    faults: {
      async afterJournalPrepared() {
        if (!observation.captureFailureArmed) return;
        calls.publishFreshCheckpointArtifact += 1;
        if (observation.captureFailureCount === 0) {
          observation.captureFailureCount += 1;
          throw new Error(
            "synthetic uncertain checkpoint publication",
          );
        }
      },
    },
    inspectFilesystem: async () => ({
      durability: "local-fsync-rename",
      filesystemId: "integration-test-filesystem",
      objectIdentityScheme: "integration-test-object-generation-v1",
      type: "integration-test-local",
    }),
    inspectOwnedRootAcl: async (path) => {
      calls.publication += 1;
      observation.ownedRootInspections.push(path);
      return false;
    },
    inspectOwnedRootAncestorAcl: async () => false,
    inspectPersistentObjectIdentity:
      inspectIntegrationPersistentObjectIdentity,
    journal,
    listMountPoints: async () => ["/"],
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

function canonicalJsonForPodmanFixture(value) {
  if (value === null || typeof value !== "object") {
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    return `[${value.map(canonicalJsonForPodmanFixture).join(",")}]`;
  }
  return `{${Object.keys(value)
    .sort()
    .map(
      (key) =>
        `${JSON.stringify(key)}:${canonicalJsonForPodmanFixture(value[key])}`,
    )
    .join(",")}}`;
}

function sha256Text(value) {
  return createHash("sha256").update(value, "utf8").digest("hex");
}

function recoveryOwnerScopeId(recoveryScopeId, stateOwnerId) {
  return `recovery-owner:${sha256Text(
    [
      "portable-codex-runtime:postgres-restore-recovery-owner-scope:v1",
      recoveryScopeId,
      stateOwnerId,
    ].join("\0"),
  )}`;
}

function podmanWriterTerminalFixture({
  evidenceContractVersion =
    POSTGRES_LOGICAL_WRITER_SUPERVISOR_PHYSICAL_CONTRACT_VERSION,
  launchAttemptId,
  request,
  stopOperationId = null,
  supervisorId,
}) {
  const containerId = sha256Text(
    `portable-codex-runtime:integration-podman-container:v1\0${launchAttemptId}`,
  );
  const requestSha256 = sha256Text(
    `portable-codex-runtime:podman-writer-request:v1\0${canonicalJsonForPodmanFixture(
      request,
    )}`,
  );
  const processIncarnationId = `podman-process:${containerId}`;
  const writerIncarnationId = `podman-writer:${sha256Text(
    `portable-codex-runtime:podman-writer:v1\0${supervisorId}\0${launchAttemptId}\0${requestSha256}\0${containerId}`,
  )}`;
  const proofId = `podman-start:${sha256Text(
    `portable-codex-runtime:podman-start-proof:v1\0${supervisorId}\0${launchAttemptId}\0${requestSha256}\0${containerId}`,
  )}`;
  const stopProofId = `podman-stopped:${sha256Text(
    `portable-codex-runtime:podman-stopped-proof:v1\0${launchAttemptId}\0${requestSha256}\0${containerId}`,
  )}`;
  return {
    evidence: frozenNullPrototypeRecord({
      contractVersion: evidenceContractVersion,
      launchAttemptId,
      processIncarnationId,
      proofId,
      status: "started",
      supervisorId,
      writerIncarnationId,
    }),
    terminalRecord:
      stopOperationId === null
        ? null
        : frozenNullPrototypeRecord({
            containerId,
            containerName: `codex-writer-${sha256Text(
              `portable-codex-runtime:podman-container:v1\0${supervisorId}\0${launchAttemptId}`,
            ).slice(0, 48)}`,
            contractVersion: 1,
            launchAttemptId,
            processIncarnationId,
            proofId,
            requestSha256,
            revision: 4,
            status: "stopped",
            stopOperationId,
            stopProofId,
            writerIncarnationId,
          }),
  };
}

function podmanWriterStateCollectionSha256(terminalRecord, stateOwnerId) {
  return sha256Text(
    `portable-codex-runtime:podman-writer-state-collection:v2\0${stateOwnerId}\0${JSON.stringify(
      terminalRecord,
    )}`,
  );
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
    capturePublication,
    finalAttachmentLeaseDurationMilliseconds = 300_000,
    finalAttachmentRootPath,
    imageDigest = IMAGE_DIGEST,
    sourceAttachmentRootPath,
  } = {},
) {
  const registered = await authority.registerSession(
    registrationInput(sessionId, { imageDigest }),
  );
  const sourceAttachment = await attachWriter(authority, registered, {
    leaseDurationMilliseconds: 300_000,
    ...(sourceAttachmentRootPath === undefined
      ? {}
      : { rootPath: sourceAttachmentRootPath }),
  });
  const captureAdmission = checkpointCaptureAdmission(sourceAttachment);
  const captureCompletion = await checkpointAuthority.runCapture(
    captureAdmission,
    capturePublication === undefined
      ? async (context) => checkpointCompletion(context, false)
      : async (context) =>
          capturePublication(context, captureAdmission),
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
    ...(finalAttachmentRootPath === undefined
      ? {}
      : { rootPath: finalAttachmentRootPath }),
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

async function publishIntegrationCheckpointArtifact(
  publication,
  context,
  admission,
) {
  const binding = Object.freeze({
    attachmentId: context.canonicalAttachment.attachmentId,
    attachmentOperationId: context.canonicalAttachment.operationId,
    attachmentProofId: context.canonicalAttachment.proofId,
    captureAttemptId: context.captureAttemptId,
    checkpoint: admission.checkpoint,
    contractVersion: 2,
    processIncarnationId: admission.processIncarnationId,
    reservationId: context.reservationId,
    stopOperationId: admission.stopOperationId,
    writerIncarnationId: admission.writerIncarnationId,
  });
  const outcome = await publication.publishFreshCheckpointArtifact({
    artifactDirectory: context.artifactDirectory,
    artifactOwnedRoot: context.artifactOwnedRoot,
    binding,
    operationId: admission.request.operationId,
    request: admission.request,
    result: context.result,
    sourceDirectory: context.sourceDirectory,
    sourceOwnedRoot: context.sourceOwnedRoot,
  });
  assert.equal(outcome.replayed, false);
  return Object.freeze({
    artifactProof: Object.freeze({
      artifactManifestDigest:
        outcome.materialization.artifactManifestDigest,
      captureOperationId: admission.request.operationId,
      modeledDigest: outcome.materialization.modeledDigest,
    }),
    materialization: outcome.materialization,
    replayed: outcome.replayed,
    result: context.result,
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
    assert.equal(SESSION_AUTHORITY_MIGRATION_VERSION, 11);
    assert.deepEqual(
      trackedMigrations.map(({ version }) => version),
      [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
    );

    await pool.query(
      "DROP SCHEMA IF EXISTS session_authority CASCADE",
    );
    const freshMigration = await store.migrate();
    assert.deepEqual(freshMigration, {
      applied: true,
      checksum: latestMigration.checksum,
      version: 11,
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
      version: 11,
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
      version: 11,
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
      version: 11,
    });
    await assertFilesystemImageProviderHeadAnchorSchemaAndStore(pool, store);
    await assertFilesystemImageProviderStateAuthoritySchemaAndStore(
      pool,
      store,
      trackedMigrations,
    );
    await assertLegacyRestoreV2MigrationGate(
      pool,
      store,
      trackedMigrations,
    );
    await assertWriterSupervisorStateOwnerMigrationGate(
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
    const assembledDurableCutEvidence = new Map();
    t.after(async () => {
      try {
        if (sessionIds.length > 0) {
          const cleanupClient = await pool.connect();
          let cleanupTransactionOpen = false;
          try {
            await cleanupClient.query("BEGIN");
            cleanupTransactionOpen = true;
            await cleanupClient.query(
              [
                "DELETE FROM session_authority.restore_destination_generations",
                "WHERE session_id = ANY($1::uuid[])",
              ].join(" "),
              [sessionIds],
            );
            await cleanupClient.query(
              [
                "DELETE FROM session_authority.checkpoint_catalogue",
                "WHERE session_id = ANY($1::uuid[])",
              ].join(" "),
              [sessionIds],
            );
            await cleanupClient.query(
              [
                "DELETE FROM session_authority.capture_attempt_tombstones",
                "WHERE session_id = ANY($1::uuid[])",
              ].join(" "),
              [sessionIds],
            );
            await cleanupClient.query(
              [
                "DELETE FROM session_authority.capture_attempt_claims",
                "WHERE session_id = ANY($1::uuid[])",
              ].join(" "),
              [sessionIds],
            );
            await cleanupClient.query(
              [
                "DELETE FROM session_authority.writer_supervisor_state_gc",
                "WHERE session_id = ANY($1::uuid[])",
              ].join(" "),
              [sessionIds],
            );
            await cleanupClient.query(
              [
                "DELETE FROM session_authority.writer_supervisor_state_owners",
                "WHERE session_id = ANY($1::uuid[])",
              ].join(" "),
              [sessionIds],
            );
            await cleanupClient.query(
              [
                "DELETE FROM session_authority.reservations",
                "WHERE session_id = ANY($1::uuid[])",
              ].join(" "),
              [sessionIds],
            );
            await cleanupClient.query(
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
            await cleanupClient.query(
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
            await cleanupClient.query(
              [
                "DELETE FROM session_authority.operation_claims",
                "WHERE session_id = ANY($1::uuid[])",
              ].join(" "),
              [sessionIds],
            );
            await cleanupClient.query(
              [
                "DELETE FROM session_authority.operation_id_registry",
                "WHERE session_id = ANY($1::uuid[])",
              ].join(" "),
              [sessionIds],
            );
            await cleanupClient.query(
              [
                "DELETE FROM session_authority.sessions",
                "WHERE session_id = ANY($1::uuid[])",
              ].join(" "),
              [sessionIds],
            );
            await cleanupClient.query("COMMIT");
            cleanupTransactionOpen = false;
          } finally {
            if (cleanupTransactionOpen) {
              await cleanupClient.query("ROLLBACK");
            }
            cleanupClient.release();
          }
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

        let finalReplay = null;
        for (let replayIndex = 0; replayIndex < 2; replayIndex += 1) {
          const replayed =
            await restarted.claimWriterForceFenceDispatch(
              structuredClone(transition),
            );
          finalReplay = replayed;
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
        assembledDurableCutEvidence.set("writer-force-fence", {
          acknowledgementBoundary: "dispatch",
          kind: input.kind,
          operationId: input.operationId,
          replayGranted: finalReplay.dispatchGranted,
          sessionId,
          state: reconciled.operation.state,
        });
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
        assembledDurableCutEvidence.set("writer-release", {
          acknowledgementBoundary: "dispatch-and-finalize",
          kind: input.kind,
          operationId: input.operationId,
          replayGranted: dispatchReplay.dispatchGranted,
          sessionId,
          state: finalizeReconciled.operation.state,
        });
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
        assembledDurableCutEvidence.set("checkpoint-capture", {
          acknowledgementBoundary: "finalize",
          kind: terminal.operation.kind,
          operationId: admission.request.operationId,
          publicationCount,
          sessionId,
          state: terminal.operation.state,
          verificationCount,
        });
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
        assembledDurableCutEvidence.set("restore-generation", {
          acknowledgementBoundary: "dispatch-and-finalize",
          kind: uncertainInput.kind,
          operationId: uncertainInput.operationId,
          replayGranted: claimReplay.dispatchGranted,
          sessionId: uncertainSessionId,
          state: finalizeLossRead.operation.state,
        });
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
          stateOwnerId: INTEGRATION_STATE_OWNER_ID,
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
        const claimReplay =
          await authority.claimRestoreAttachmentActivationDispatch({
            ...structuredClone(activationInput),
            expectedOperationRevision: "0",
          });
        assertOperationReceipt(claimReplay, "starting");
        assert.equal(claimReplay.dispatchGranted, false);
        assert.deepEqual(claimReplay.operation, claimed.operation);
        assert.deepEqual(
          claimReplay.activationRequest,
          claimed.activationRequest,
        );
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
            stateOwnerId: INTEGRATION_STATE_OWNER_ID,
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
            stateOwnerId: INTEGRATION_STATE_OWNER_ID,
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
          stateOwnerId: INTEGRATION_STATE_OWNER_ID,
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
          stateOwnerId: INTEGRATION_STATE_OWNER_ID,
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

        const imagePlanFixture = integrationImagePlanBindingFixture({
          image,
          session: released.session,
        });
        const imageReservation =
          await imagePlanFixture.binding.prepareImageReservation({
            plan: imagePlanFixture.plan,
            sessionManifest: released.session.document.manifest,
          });
        assert.equal(
          isPostgresDetachedRestoreImagePlanReservation(imageReservation),
          true,
        );
        assert.deepEqual(Reflect.ownKeys(imageReservation), []);
        assert.deepEqual(imagePlanFixture.providerCalls, {
          inspect: 1,
          resolve: 1,
        });
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
          async finalizeWriterLaunchAttemptStoppedAndAuthorizeSupervisorStateGc(
            options,
          ) {
            return authority.finalizeWriterLaunchAttemptStoppedAndAuthorizeSupervisorStateGc(
              options,
            );
          },
          async finalizeWriterLaunchStopped(options) {
            return authority.finalizeWriterLaunchStopped(options);
          },
          async finalizeWriterLaunchStoppedAndAuthorizeSupervisorStateGc(
            options,
          ) {
            return authority.finalizeWriterLaunchStoppedAndAuthorizeSupervisorStateGc(
              options,
            );
          },
          async finalizeWriterLaunchStoppedAndReserveCheckpointCapture(
            options,
          ) {
            return authority.finalizeWriterLaunchStoppedAndReserveCheckpointCapture(
              options,
            );
          },
          async finalizeWriterLaunchStoppedAndReserveCheckpointCaptureAndAuthorizeSupervisorStateGc(
            options,
          ) {
            return authority.finalizeWriterLaunchStoppedAndReserveCheckpointCaptureAndAuthorizeSupervisorStateGc(
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
          async readWriterSupervisorStateGcAuthorization(options) {
            return authority.readWriterSupervisorStateGcAuthorization(options);
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
          imagePlanBinding: imagePlanFixture.binding,
          operationGuard,
          stoppedWriterCoordinator:
            new StoppedWriterCapabilityCoordinator(),
          supervisor: {
            contractVersion: LOGICAL_WRITER_SUPERVISOR_CONTRACT_VERSION,
            launchWriter: async (context) => {
              launchCalls += 1;
              launchedRequest = context.attempt.request;
              const launchFixture = podmanWriterTerminalFixture({
                evidenceContractVersion: 1,
                launchAttemptId,
                request: context.attempt.request,
                supervisorId,
              });
              return {
                receiptVersion: LOGICAL_WRITER_LAUNCH_RECEIPT_VERSION,
                evidence: launchFixture.evidence,
                stopWriter: async function stopWriter(stopInput) {
                  const stoppedFixture = podmanWriterTerminalFixture({
                    evidenceContractVersion: 1,
                    launchAttemptId,
                    request: context.attempt.request,
                    stopOperationId: stopInput.stopOperationId,
                    supervisorId,
                  });
                  return frozenNullPrototypeRecord({
                    confirmation: STOPPED_WRITER_STOP_CONFIRMED,
                    contractVersion:
                      LOGICAL_WRITER_SUPERVISOR_CONTRACT_VERSION,
                    terminalRecord: stoppedFixture.terminalRecord,
                  });
                },
                terminalRecord: null,
              };
            },
            reconcileWriterLaunch: async () => {
              throw new Error(
                "an activation-prepared launch must not reconcile before dispatch",
              );
            },
            stateOwnerId: INTEGRATION_STATE_OWNER_ID,
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
        assert.deepEqual(imagePlanFixture.providerCalls, {
          inspect: 2,
          resolve: 1,
        });

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
          stateOwnerId: INTEGRATION_STATE_OWNER_ID,
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
        assembledDurableCutEvidence.set("restore-activation", {
          acknowledgementBoundary: "finalize",
          finalizedAgain: finalizationReplay.activation.finalized,
          kind: activationInput.kind,
          operationId: activationInput.operationId,
          sessionId,
          state: finalized.activation.operation.state,
        });
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
              stateOwnerId: INTEGRATION_STATE_OWNER_ID,
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
          stateOwnerId: INTEGRATION_STATE_OWNER_ID,
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
      "cold rev4 launch reconciliation commits owner-bound GC authorization",
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
        const imagePlanFixture = integrationImagePlanBindingFixture({
          image,
          session: fixture.finalized.session,
        });
        const launchAttemptId = `writer-launch-${randomUUID()}`;
        const supervisorId = `supervisor-${randomUUID()}`;
        const input = writerLaunchAttemptInput(
          fixture.finalized.session,
          fixture.finalized.generation,
          { operationId: launchAttemptId, supervisorId },
        );
        await authority.reserveOperation(input);
        await authority.claimWriterLaunchAttemptDispatch({
          ...structuredClone(input),
          expectedOperationRevision: "0",
          stateOwnerId: INTEGRATION_STATE_OWNER_ID,
        });
        await authority.markOperationUncertain({
          ...structuredClone(input),
          expectedOperationRevision: "1",
        });
        const stoppedFixture = podmanWriterTerminalFixture({
          evidenceContractVersion: 1,
          launchAttemptId,
          request: input.request,
          stopOperationId: `cold-retirement-${randomUUID()}`,
          supervisorId,
        });
        const stoppedEvidence = frozenNullPrototypeRecord({
          ...stoppedFixture.evidence,
          proofId: stoppedFixture.terminalRecord.stopProofId,
          status: "complete-stopped",
        });
        let launchCalls = 0;
        let reconcileCalls = 0;
        const facade = createPostgresLogicalWriterLauncher({
          authority,
          imagePlanBinding: imagePlanFixture.binding,
          operationGuard,
          stoppedWriterCoordinator:
            new StoppedWriterCapabilityCoordinator(),
          supervisor: {
            contractVersion: LOGICAL_WRITER_SUPERVISOR_CONTRACT_VERSION,
            async launchWriter() {
              launchCalls += 1;
              throw new Error("cold recovery must not launch");
            },
            async reconcileWriterLaunch(context) {
              reconcileCalls += 1;
              assert.equal(
                context.attempt.launchAttemptId,
                launchAttemptId,
              );
              return frozenNullPrototypeRecord({
                evidence: stoppedEvidence,
                receiptVersion: LOGICAL_WRITER_RECONCILE_RECEIPT_VERSION,
                terminalRecord: stoppedFixture.terminalRecord,
              });
            },
            stateOwnerId: INTEGRATION_STATE_OWNER_ID,
            supervisorId,
          },
        });

        const result = await facade.reconcileLaunchAttempt({
          launchAttemptId,
        });
        assert.equal(result.status, "complete-stopped");
        assert.equal(result.writer, null);
        assert.equal(launchCalls, 0);
        assert.equal(reconcileCalls, 1);
        const read = await authority.readWriterLaunchAttempt({
          operationId: launchAttemptId,
          stateOwnerId: INTEGRATION_STATE_OWNER_ID,
        });
        assertOperationReceipt(read, "committed");
        assert.equal(
          read.operation.result.outcome,
          "writer-launch-complete-stopped",
        );
        assert.equal(
          read.operation.result.evidence.status,
          "complete-stopped",
        );
        const authorization =
          await authority.readWriterSupervisorStateGcAuthorization({
            stateOwnerId: INTEGRATION_STATE_OWNER_ID,
            terminalOperationId: launchAttemptId,
          });
        assert.equal(authorization.launchAttemptId, launchAttemptId);
        assert.equal(
          authorization.stateOwnerId,
          INTEGRATION_STATE_OWNER_ID,
        );
        assert.deepEqual(
          structuredClone(authorization.terminalRecord),
          structuredClone(stoppedFixture.terminalRecord),
        );
        const pendingGc = await pool.query(
          [
            "SELECT state_owner_id, collected_at",
            "FROM session_authority.writer_supervisor_state_gc",
            "WHERE launch_attempt_id = $1",
          ].join(" "),
          [launchAttemptId],
        );
        assert.deepEqual(pendingGc.rows, [
          {
            collected_at: null,
            state_owner_id: INTEGRATION_STATE_OWNER_ID,
          },
        ]);
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
        const imagePlanFixture = integrationImagePlanBindingFixture({
          image,
          session: fixture.finalized.session,
        });
        const imageReservation =
          await imagePlanFixture.binding.prepareImageReservation({
            plan: imagePlanFixture.plan,
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
          async finalizeWriterLaunchAttemptStoppedAndAuthorizeSupervisorStateGc(
            options,
          ) {
            return authority.finalizeWriterLaunchAttemptStoppedAndAuthorizeSupervisorStateGc(
              options,
            );
          },
          async finalizeWriterLaunchStopped(options) {
            return authority.finalizeWriterLaunchStopped(options);
          },
          async finalizeWriterLaunchStoppedAndAuthorizeSupervisorStateGc(
            options,
          ) {
            return authority.finalizeWriterLaunchStoppedAndAuthorizeSupervisorStateGc(
              options,
            );
          },
          async finalizeWriterLaunchStoppedAndReserveCheckpointCapture(
            options,
          ) {
            return authority.finalizeWriterLaunchStoppedAndReserveCheckpointCapture(
              options,
            );
          },
          async finalizeWriterLaunchStoppedAndReserveCheckpointCaptureAndAuthorizeSupervisorStateGc(
            options,
          ) {
            return authority.finalizeWriterLaunchStoppedAndReserveCheckpointCaptureAndAuthorizeSupervisorStateGc(
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
          async readWriterSupervisorStateGcAuthorization(options) {
            return authority.readWriterSupervisorStateGcAuthorization(options);
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
          const launchFixture = podmanWriterTerminalFixture({
            evidenceContractVersion: 1,
            launchAttemptId,
            request: context.attempt.request,
            supervisorId,
          });
          return {
            receiptVersion: LOGICAL_WRITER_LAUNCH_RECEIPT_VERSION,
            evidence: launchFixture.evidence,
            stopWriter: async function stopWriter(stopInput) {
              stopCalls += 1;
              assert.notEqual(stopUncertaintyInput, null);
              const uncertain = await authority.markOperationUncertain({
                ...stopUncertaintyInput,
                expectedOperationRevision: "1",
              });
              assertOperationReceipt(uncertain, "uncertain");
              const stoppedFixture = podmanWriterTerminalFixture({
                evidenceContractVersion: 1,
                launchAttemptId,
                request: context.attempt.request,
                stopOperationId: stopInput.stopOperationId,
                supervisorId,
              });
              return frozenNullPrototypeRecord({
                confirmation: STOPPED_WRITER_STOP_CONFIRMED,
                contractVersion:
                  LOGICAL_WRITER_SUPERVISOR_CONTRACT_VERSION,
                terminalRecord: stoppedFixture.terminalRecord,
              });
            },
            terminalRecord: null,
          };
        };
        const reconcileWriterLaunch = async () => {
          throw new Error("committed launches must not reach the supervisor");
        };
        const facade = createPostgresLogicalWriterLauncher({
          authority: launcherAuthority,
          imagePlanBinding: imagePlanFixture.binding,
          operationGuard,
          stoppedWriterCoordinator:
            new StoppedWriterCapabilityCoordinator(),
          supervisor: {
            contractVersion: LOGICAL_WRITER_SUPERVISOR_CONTRACT_VERSION,
            launchWriter,
            reconcileWriterLaunch,
            stateOwnerId: INTEGRATION_STATE_OWNER_ID,
            supervisorId,
          },
        });

        const started = await facade.runLaunch({
          generation: fixture.finalized.generation,
          imageReservation,
          launchAttemptId,
        });
        assert.equal(started.status, "started");
        assert.notEqual(started.writer, null);
        assert.equal(launchCalls, 1);
        assert.deepEqual(imagePlanFixture.providerCalls, {
          inspect: 3,
          resolve: 1,
        });

        const read = await authority.readWriterLaunchAttempt({
          operationId: launchAttemptId,
          stateOwnerId: INTEGRATION_STATE_OWNER_ID,
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
          stateOwnerId: INTEGRATION_STATE_OWNER_ID,
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
        const imagePlanFixture = integrationImagePlanBindingFixture({
          image,
          session: fixture.attached.session,
        });
        const imageReservation =
          await imagePlanFixture.binding.prepareImageReservation({
            plan: imagePlanFixture.plan,
            sessionManifest: fixture.attached.session.document.manifest,
          });
        const launchAttemptId = `writer-launch-${randomUUID()}`;
        const supervisorId = `supervisor-${randomUUID()}`;
        let launchCalls = 0;
        let launchedRequest = null;
        const facade = createPostgresLogicalWriterLauncher({
          authority,
          imagePlanBinding: imagePlanFixture.binding,
          operationGuard,
          stoppedWriterCoordinator:
            new StoppedWriterCapabilityCoordinator(),
          supervisor: {
            contractVersion: LOGICAL_WRITER_SUPERVISOR_CONTRACT_VERSION,
            launchWriter: async (context) => {
              launchCalls += 1;
              launchedRequest = context.attempt.request;
              const launchFixture = podmanWriterTerminalFixture({
                evidenceContractVersion: 1,
                launchAttemptId,
                request: context.attempt.request,
                supervisorId,
              });
              return {
                receiptVersion: LOGICAL_WRITER_LAUNCH_RECEIPT_VERSION,
                evidence: launchFixture.evidence,
                stopWriter: async function stopWriter(stopInput) {
                  const stoppedFixture = podmanWriterTerminalFixture({
                    evidenceContractVersion: 1,
                    launchAttemptId,
                    request: context.attempt.request,
                    stopOperationId: stopInput.stopOperationId,
                    supervisorId,
                  });
                  return frozenNullPrototypeRecord({
                    confirmation: STOPPED_WRITER_STOP_CONFIRMED,
                    contractVersion:
                      LOGICAL_WRITER_SUPERVISOR_CONTRACT_VERSION,
                    terminalRecord: stoppedFixture.terminalRecord,
                  });
                },
                terminalRecord: null,
              };
            },
            reconcileWriterLaunch: async () => {
              throw new Error(
                "a prepared handoff must not reconcile before launch",
              );
            },
            stateOwnerId: INTEGRATION_STATE_OWNER_ID,
            supervisorId,
          },
        });
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
        assert.deepEqual(imagePlanFixture.providerCalls, {
          inspect: 2,
          resolve: 1,
        });

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
          stateOwnerId: INTEGRATION_STATE_OWNER_ID,
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
          [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11],
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
          stateOwnerId: INTEGRATION_STATE_OWNER_ID,
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
          stateOwnerId: INTEGRATION_STATE_OWNER_ID,
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
            stateOwnerId: INTEGRATION_STATE_OWNER_ID,
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
              stateOwnerId: INTEGRATION_STATE_OWNER_ID,
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
            stateOwnerId: INTEGRATION_STATE_OWNER_ID,
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
                stateOwnerId: INTEGRATION_STATE_OWNER_ID,
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
                stateOwnerId: INTEGRATION_STATE_OWNER_ID,
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
          stateOwnerId: INTEGRATION_STATE_OWNER_ID,
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
          stateOwnerId: INTEGRATION_STATE_OWNER_ID,
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
          stateOwnerId: INTEGRATION_STATE_OWNER_ID,
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
        assembledDurableCutEvidence.set("writer-launch", {
          acknowledgementBoundary: "dispatch-and-finalize",
          kind: input.kind,
          operationId: input.operationId,
          replayGranted: claimReplay.dispatchGranted,
          sessionId,
          state: committed.operation.state,
        });
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
            stateOwnerId: INTEGRATION_STATE_OWNER_ID,
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
        assembledDurableCutEvidence.set("writer-stop", {
          acknowledgementBoundary: "dispatch-and-finalize",
          kind: stopInput.kind,
          operationId: stopInput.operationId,
          replayGranted: claimReplay.dispatchGranted,
          sessionId: launchedSessionId,
          state: committed.operation.state,
        });

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
          stateOwnerId: INTEGRATION_STATE_OWNER_ID,
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
          stateOwnerId: INTEGRATION_STATE_OWNER_ID,
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
          stateOwnerId: INTEGRATION_STATE_OWNER_ID,
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
            stateOwnerId: INTEGRATION_STATE_OWNER_ID,
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
            stateOwnerId: INTEGRATION_STATE_OWNER_ID,
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
            stateOwnerId: INTEGRATION_STATE_OWNER_ID,
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
            stateOwnerId: INTEGRATION_STATE_OWNER_ID,
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

    await t.test(
      "assembled durable-cut matrix binds seven acknowledgement-loss paths to persisted authority rows",
      async () => {
        const cutMatrix = [
          {
            acknowledgementBoundary: "dispatch-and-finalize",
            cutName: "writer-stop",
            kind: WRITER_LAUNCH_STOP_OPERATION_KIND,
            state: "committed",
          },
          {
            acknowledgementBoundary: "finalize",
            cutName: "checkpoint-capture",
            kind: CHECKPOINT_CAPTURE_OPERATION_KIND,
            state: "committed",
          },
          {
            acknowledgementBoundary: "dispatch-and-finalize",
            cutName: "restore-generation",
            kind: RESTORE_DESTINATION_GENERATION_OPERATION_KIND,
            state: "committed",
          },
          {
            acknowledgementBoundary: "dispatch-and-finalize",
            cutName: "writer-release",
            kind: WRITER_RELEASE_OPERATION_KIND,
            state: "committed",
          },
          {
            acknowledgementBoundary: "dispatch",
            cutName: "writer-force-fence",
            kind: WRITER_FORCE_FENCE_OPERATION_KIND,
            state: "starting",
          },
          {
            acknowledgementBoundary: "finalize",
            cutName: "restore-activation",
            kind: RESTORE_ATTACHMENT_ACTIVATION_OPERATION_KIND,
            state: "committed",
          },
          {
            acknowledgementBoundary: "dispatch-and-finalize",
            cutName: "writer-launch",
            kind: WRITER_LAUNCH_ATTEMPT_OPERATION_KIND,
            state: "committed",
          },
        ];
        const cutNames = cutMatrix.map(({ cutName }) => cutName);
        assert.deepEqual(
          [...assembledDurableCutEvidence.keys()].sort(),
          [...cutNames].sort(),
        );

        const evidenceByOperationId = new Map(
          cutMatrix.map((specification) => {
            const { cutName } = specification;
            const evidence = assembledDurableCutEvidence.get(cutName);
            assert.notEqual(evidence, undefined);
            assert.equal(
              evidence.acknowledgementBoundary,
              specification.acknowledgementBoundary,
            );
            assert.equal(evidence.kind, specification.kind);
            assert.equal(evidence.state, specification.state);
            return [
              evidence.operationId,
              { cutName, evidence, specification },
            ];
          }),
        );
        assert.equal(evidenceByOperationId.size, cutNames.length);
        const persisted = await pool.query(
          [
            "SELECT o.operation_id, o.session_id::text AS session_id,",
            "o.kind, o.state,",
            "count(r.reservation_id)::integer AS reservation_count",
            "FROM session_authority.operation_claims AS o",
            "LEFT JOIN session_authority.reservations AS r",
            "ON r.operation_id = o.operation_id",
            "WHERE o.operation_id = ANY($1::character varying[])",
            "GROUP BY o.operation_id, o.session_id, o.kind, o.state",
            "ORDER BY o.operation_id",
          ].join(" "),
          [[...evidenceByOperationId.keys()]],
        );
        assert.equal(persisted.rows.length, cutNames.length);
        for (const row of persisted.rows) {
          const bound = evidenceByOperationId.get(row.operation_id);
          assert.notEqual(bound, undefined);
          assert.equal(row.session_id, bound.evidence.sessionId);
          assert.equal(row.kind, bound.specification.kind);
          assert.equal(row.state, bound.specification.state);
          assert.equal(row.reservation_count, 1);
          evidenceByOperationId.delete(row.operation_id);
        }
        assert.equal(evidenceByOperationId.size, 0);

        for (const cutName of [
          "writer-stop",
          "restore-generation",
          "writer-release",
          "writer-force-fence",
          "writer-launch",
        ]) {
          assert.equal(
            assembledDurableCutEvidence.get(cutName).replayGranted,
            false,
          );
        }
        assert.deepEqual(
          {
            publicationCount:
              assembledDurableCutEvidence.get("checkpoint-capture")
                .publicationCount,
            verificationCount:
              assembledDurableCutEvidence.get("checkpoint-capture")
                .verificationCount,
          },
          { publicationCount: 1, verificationCount: 1 },
        );
        assert.equal(
          assembledDurableCutEvidence.get("restore-activation")
            .finalizedAgain,
          false,
        );
      },
    );
  },
);

test(
  "restore runtime controller drains and verifies a committed restore after acknowledgement-loss readback interruption with fresh assembled objects",
  { timeout: 60_000 },
  async (t) => {
    const foregroundLifecycleApplicationName =
      `pcr-restore-foreground-${randomUUID()}`;
    const authorityPool = new Pool({
      application_name:
        "portable-codex-runtime-restore-authority-integration-test",
      connectionString: databaseUrl,
      max: 2,
    });
    const operationPool = new Pool({
      application_name:
        "portable-codex-runtime-restore-operation-integration-test",
      connectionString: databaseUrl,
      max: 1,
    });
    const foregroundLifecyclePool = new Pool({
      application_name: foregroundLifecycleApplicationName,
      connectionString: databaseUrl,
      max: 1,
    });
    const recoveryLifecyclePool = new Pool({
      application_name:
        "portable-codex-runtime-restore-lifecycle-recovery-integration-test",
      connectionString: databaseUrl,
      max: 1,
    });
    const recoveryScopeId = `integration-restore-${randomUUID()}`;
    const controllerRecoveryScopeId =
      `integration-restore-controller-${randomUUID()}`;
    const restartedRecoveryScopeId = recoveryScopeId;
    const sessionId = randomUUID();
    const blockedSessionId = randomUUID();
    const sessionIds = [sessionId, blockedSessionId];
    const image = integrationPlatformImageFixture();
    const imagePlanProviderId =
      `runtime-image-provider-${randomUUID()}`;
    const supervisorId = `runtime-supervisor-${randomUUID()}`;
    const stateOwnerId = `state-owner:${sha256Text(randomUUID())}`;
    const effectiveRecoveryScopeId = recoveryOwnerScopeId(
      recoveryScopeId,
      stateOwnerId,
    );
    const effectiveControllerRecoveryScopeId = recoveryOwnerScopeId(
      controllerRecoveryScopeId,
      stateOwnerId,
    );
    const effectiveRestartedRecoveryScopeId = recoveryOwnerScopeId(
      restartedRecoveryScopeId,
      stateOwnerId,
    );
    assert.equal(
      effectiveRestartedRecoveryScopeId,
      effectiveRecoveryScopeId,
    );
    assert.notEqual(
      recoveryOwnerScopeId(
        recoveryScopeId,
        `state-owner:${sha256Text(`foreign-${randomUUID()}`)}`,
      ),
      effectiveRecoveryScopeId,
    );
    const collectedSupervisorState = new Set();
    const supervisorStateCollectionInvocations = new Set();
    const supervisorStateCollectionSignals = new Set();
    const supervisorStateCollections = [];
    let controller = null;
    let physicalBindings = null;
    let restartedController = null;
    let restartedPhysicalBindings = null;
    let runtime = null;
    let foregroundTeardown = null;
    const publicationTree =
      await createRestoreRuntimePublicationTree();
    const publicationObservation = {
      captureFailureArmed: false,
      captureFailureCount: 0,
      captureFailureOperationId: null,
      events: [],
      ownedRootInspections: [],
      restoreOperationId: null,
      restoreVerificationInput: null,
    };
    t.after(async () => {
      if (controller !== null) {
        await Promise.allSettled([controller.stop()]);
      }
      if (restartedController !== null) {
        await Promise.allSettled([restartedController.stop()]);
      }
      let teardownToAwait = foregroundTeardown;
      if (teardownToAwait === null && runtime !== null) {
        try {
          teardownToAwait = runtime.scheduler.stop();
        } catch (error) {
          teardownToAwait = Promise.reject(error);
        }
      }
      if (teardownToAwait !== null) {
        await Promise.allSettled([teardownToAwait]);
      }
      if (physicalBindings !== null) {
        await Promise.allSettled([physicalBindings.stop()]);
      }
      if (restartedPhysicalBindings !== null) {
        await Promise.allSettled([restartedPhysicalBindings.stop()]);
      }
      try {
        const cleanupClient = await authorityPool.connect();
        try {
          await cleanupClient.query("BEGIN");
          await cleanupClient.query(
            [
              "DELETE FROM session_authority.restore_recovery_cursors",
              "WHERE recovery_scope_id = ANY($1::text[])",
            ].join(" "),
            [[
              effectiveRecoveryScopeId,
              effectiveControllerRecoveryScopeId,
              effectiveRestartedRecoveryScopeId,
            ]],
          );
          await cleanupClient.query(
            [
              "DELETE FROM session_authority.detached_restore_stable_plans",
              "WHERE session_id = ANY($1::uuid[])",
            ].join(" "),
            [sessionIds],
          );
          await cleanupClient.query(
            [
              "DELETE FROM session_authority.restore_destination_generations",
              "WHERE session_id = ANY($1::uuid[])",
            ].join(" "),
            [sessionIds],
          );
          await cleanupClient.query(
            [
              "DELETE FROM session_authority.writer_supervisor_state_gc",
              "WHERE session_id = ANY($1::uuid[])",
            ].join(" "),
            [sessionIds],
          );
          await cleanupClient.query(
            [
              "DELETE FROM session_authority.writer_supervisor_state_owners",
              "WHERE session_id = ANY($1::uuid[])",
            ].join(" "),
            [sessionIds],
          );
          await cleanupClient.query(
            [
              "DELETE FROM session_authority.checkpoint_catalogue",
              "WHERE session_id = ANY($1::uuid[])",
            ].join(" "),
            [sessionIds],
          );
          await cleanupClient.query(
            [
              "DELETE FROM session_authority.capture_attempt_tombstones",
              "WHERE session_id = ANY($1::uuid[])",
            ].join(" "),
            [sessionIds],
          );
          await cleanupClient.query(
            [
              "DELETE FROM session_authority.capture_attempt_claims",
              "WHERE session_id = ANY($1::uuid[])",
            ].join(" "),
            [sessionIds],
          );
          await cleanupClient.query(
            [
              "DELETE FROM session_authority.reservations",
              "WHERE session_id = ANY($1::uuid[])",
            ].join(" "),
            [sessionIds],
          );
          await cleanupClient.query(
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
          await cleanupClient.query(
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
          await cleanupClient.query(
            [
              "DELETE FROM session_authority.operation_claims",
              "WHERE session_id = ANY($1::uuid[])",
            ].join(" "),
            [sessionIds],
          );
          await cleanupClient.query(
            [
              "DELETE FROM session_authority.operation_id_registry",
              "WHERE session_id = ANY($1::uuid[])",
            ].join(" "),
            [sessionIds],
          );
          await cleanupClient.query(
            [
              "DELETE FROM session_authority.sessions",
              "WHERE session_id = ANY($1::uuid[])",
            ].join(" "),
            [sessionIds],
          );
          await cleanupClient.query("COMMIT");
        } catch (error) {
          try {
            await cleanupClient.query("ROLLBACK");
          } catch {
            // Pool shutdown below destroys any connection that cannot reset.
          }
          throw error;
        } finally {
          cleanupClient.release();
        }
      } finally {
        try {
          try {
            await operationPool.end();
          } finally {
            try {
              await recoveryLifecyclePool.end();
            } finally {
              try {
                await foregroundLifecyclePool.end();
              } finally {
                await authorityPool.end();
              }
            }
          }
        } finally {
          await rm(publicationTree.root, {
            force: true,
            recursive: true,
          });
        }
      }
    });

    const store = new PostgresSerializableStore({
      dedicatedPool: authorityPool,
      maxTransactionAttempts: 3,
    });
    const authority = new PostgresSessionAuthority({
      restoreAttachmentActivationV2FleetCompatible: true,
      restoreAttachmentActivationV2GenerationPredecessorFleetCompatible:
        true,
      restoreGenerationV2FleetCompatible: true,
      store,
      writerLaunchStopV3FleetCompatible: true,
    });
    const checkpointAuthority = createPostgresCheckpointMutationAuthority({
      authority,
      operationGuard: new PostgresOperationGuard({
        dedicatedPool: operationPool,
      }),
      resolveArtifactPaths({ checkpoint }) {
        return {
          artifactDirectory: join(
            publicationTree.artifactOwnedRoot,
            checkpoint.artifactId,
          ),
          artifactOwnedRoot: publicationTree.artifactOwnedRoot,
        };
      },
      resolveSourceOwnedRoot({ canonicalAttachment }) {
        assert.equal(
          [
            publicationTree.sourceDirectory,
            publicationTree.recoverySourceDirectory,
          ].includes(canonicalAttachment.rootPath),
          true,
        );
        return {
          sourceDirectory: canonicalAttachment.rootPath,
          sourceOwnedRoot: publicationTree.sourceOwnedRoot,
        };
      },
    });

    let firstStep = deferred();
    const foregroundGateEntered = deferred();
    const releaseForegroundGate = deferred();
    const imageProviderInvocations = new Set();
    const imageProviderSignals = new Set();
    const steps = [];
    const calls = {
      artifactResolver: 0,
      destinationResolver: 0,
      fleetGate: 0,
      image: 0,
      planProvisioningGate: 0,
      provider: 0,
      publication: 0,
      publishFreshCheckpointArtifact: 0,
      publishRestoreDestination: 0,
      sourceResolver: 0,
      supervisorLaunch: 0,
      supervisorReconcile: 0,
      supervisorStop: 0,
      verifyCommittedCheckpointArtifact: 0,
      verifyCommittedRestoreDestination: 0,
    };
    let foregroundAllowed = false;
    let holdForegroundGate = false;
    let recoveryStablePlan = null;
    let stablePlan = null;
    const physicalInvocations = new Set();
    const physicalSignals = new Set();
    const physicalPolicies =
      integrationDeploymentPhysicalSettlementPolicies();
    const rawSupervisorStateCollector = Object.freeze({
      async collectTerminalState(input) {
        assert.equal(arguments.length, 1);
        assert.deepEqual(Reflect.ownKeys(input).sort(), [
          "contractVersion",
          "invocation",
          "signal",
          "stateOwnerId",
          "terminalRecord",
        ]);
        assert.equal(
          input.contractVersion,
          POSTGRES_WRITER_SUPERVISOR_STATE_COLLECTION_PHYSICAL_CONTRACT_VERSION,
        );
        assert.equal(input.stateOwnerId, stateOwnerId);
        assertFreshOpaqueInvocation(
          input.invocation,
          supervisorStateCollectionInvocations,
        );
        assert.equal(input.signal instanceof AbortSignal, true);
        assert.equal(input.signal.aborted, false);
        assert.equal(supervisorStateCollectionSignals.has(input.signal), false);
        supervisorStateCollectionSignals.add(input.signal);
        const launchAttemptId = input.terminalRecord.launchAttemptId;
        const status = collectedSupervisorState.has(launchAttemptId)
          ? "absent"
          : "collected";
        collectedSupervisorState.add(launchAttemptId);
        supervisorStateCollections.push({ launchAttemptId, status });
        return frozenNullPrototypeRecord({
          contractVersion:
            POSTGRES_WRITER_SUPERVISOR_STATE_COLLECTION_PHYSICAL_CONTRACT_VERSION,
          launchAttemptId,
          stateOwnerId,
          status,
          terminalRecordSha256:
            podmanWriterStateCollectionSha256(
              input.terminalRecord,
              stateOwnerId,
            ),
        });
      },
      contractVersion:
        POSTGRES_WRITER_SUPERVISOR_STATE_COLLECTION_PHYSICAL_CONTRACT_VERSION,
      stateOwnerId,
      supervisorId,
    });
    const imagePlanProviderSettlement =
      integrationImagePlanSettlementPolicies();
    const operationalLeaseBudget =
      createPostgresDetachedRestoreOperationalLeaseBudget({
        databaseRequestMilliseconds:
          OPERATIONAL_LEASE_DATABASE_REQUEST_MILLISECONDS,
        imagePlanProviderSettlement,
        leaseDurationMilliseconds:
          OPERATIONAL_LEASE_DURATION_MILLISECONDS,
        lifecycleBackendSettlement:
          physicalPolicies.lifecycleBackendSettlement,
        publicationSettlement: physicalPolicies.publicationSettlement,
        resolveRestoreDestinationSettlement:
          physicalPolicies.resolveRestoreDestinationSettlement,
        safetyMarginMilliseconds:
          OPERATIONAL_LEASE_SAFETY_MARGIN_MILLISECONDS,
        supervisorSettlement: physicalPolicies.supervisorSettlement,
      });
    const rawLifecycleBackend =
      restoreRuntimeIntegrationLifecycleBackend(
        calls,
        physicalInvocations,
        physicalSignals,
      );
    const rawPublication = restoreRuntimePublicationEvidence(
      calls,
      publicationObservation,
      publicationTree.journalDirectory,
    );
    const generationCommitAcknowledgementLossPool =
      commitAcknowledgementLossAfterQueryPool(
        authorityPool,
        "runtime restore generation finalize",
        (text) =>
          text.startsWith(
            "UPDATE session_authority.restore_destination_generations",
          ),
      );
    // Lose the finalize acknowledgement, then interrupt the same runtime's
    // committed readback so only a fresh assembly can adopt the durable row.
    const interruptedAuthorityPool =
      firstMatchingQueryResultFailurePool(
        generationCommitAcknowledgementLossPool,
        "runtime restore generation committed readback",
        (text) =>
          generationCommitAcknowledgementLossPool.didLoseAcknowledgement() &&
          text.startsWith("SELECT") &&
          text.includes(
            "FROM session_authority.restore_destination_generations",
          ),
      );
    physicalBindings = createPostgresDetachedRestorePhysicalBindings({
      lifecycleBackend: rawLifecycleBackend,
      lifecycleSettlement: physicalPolicies.lifecycleBackendSettlement,
      onFatal() {
        assert.fail("integration physical collaborator must settle");
      },
      publication: rawPublication,
      publicationSettlement: physicalPolicies.publicationSettlement,
      async resolveRestoreDestination(input) {
        calls.destinationResolver += 1;
        assert.equal(arguments.length, 1);
        assert.equal(input.contractVersion, 1);
        assertFreshOpaqueInvocation(input.invocation, physicalInvocations);
        assert.equal(input.signal instanceof AbortSignal, true);
        assert.equal(input.signal.aborted, false);
        assert.equal(physicalSignals.has(input.signal), false);
        physicalSignals.add(input.signal);
        throw new Error("restore runtime destination must not resolve");
      },
      resolveRestoreDestinationContractVersion: 1,
      resolveRestoreDestinationSettlement:
        physicalPolicies.resolveRestoreDestinationSettlement,
      supervisor: Object.freeze({
        contractVersion:
          POSTGRES_LOGICAL_WRITER_SUPERVISOR_PHYSICAL_CONTRACT_VERSION,
        async launchWriter(context) {
          calls.supervisorLaunch += 1;
          assert.equal(arguments.length, 1);
          assert.equal(
            context.contractVersion,
            POSTGRES_LOGICAL_WRITER_SUPERVISOR_PHYSICAL_CONTRACT_VERSION,
          );
          assertFreshOpaqueInvocation(
            context.invocation,
            physicalInvocations,
          );
          assert.equal(context.signal instanceof AbortSignal, true);
          assert.equal(context.signal.aborted, false);
          assert.equal(physicalSignals.has(context.signal), false);
          physicalSignals.add(context.signal);
          assert.equal(
            context.attempt.request.supervisor.supervisorId,
            supervisorId,
          );
          const launchFixture = podmanWriterTerminalFixture({
            launchAttemptId: context.attempt.launchAttemptId,
            request: context.attempt.request,
            supervisorId,
          });
          return frozenNullPrototypeRecord({
            receiptVersion:
              POSTGRES_LOGICAL_WRITER_LAUNCH_PHYSICAL_RECEIPT_VERSION,
            evidence: launchFixture.evidence,
            stopWriter: async function stopWriter(stopInput) {
              calls.supervisorStop += 1;
              assert.equal(arguments.length, 1);
              assert.deepEqual(Reflect.ownKeys(stopInput).sort(), [
                "attachment",
                "contractVersion",
                "invocation",
                "processIncarnationId",
                "signal",
                "stopOperationId",
                "writerFence",
                "writerIncarnationId",
              ]);
              assert.equal(
                stopInput.contractVersion,
                POSTGRES_LOGICAL_WRITER_SUPERVISOR_PHYSICAL_CONTRACT_VERSION,
              );
              assertFreshOpaqueInvocation(
                stopInput.invocation,
                physicalInvocations,
              );
              assert.equal(stopInput.signal instanceof AbortSignal, true);
              assert.equal(stopInput.signal.aborted, false);
              assert.equal(physicalSignals.has(stopInput.signal), false);
              physicalSignals.add(stopInput.signal);
              const stoppedFixture = podmanWriterTerminalFixture({
                launchAttemptId: context.attempt.launchAttemptId,
                request: context.attempt.request,
                stopOperationId: stopInput.stopOperationId,
                supervisorId,
              });
              return frozenNullPrototypeRecord({
                contractVersion:
                  POSTGRES_LOGICAL_WRITER_SUPERVISOR_PHYSICAL_CONTRACT_VERSION,
                status: "stopped",
                terminalRecord: stoppedFixture.terminalRecord,
              });
            },
            terminalRecord: null,
          });
        },
        async reconcileWriterLaunch(context) {
          calls.supervisorReconcile += 1;
          assert.equal(arguments.length, 1);
          assert.equal(
            context.contractVersion,
            POSTGRES_LOGICAL_WRITER_SUPERVISOR_PHYSICAL_CONTRACT_VERSION,
          );
          assertFreshOpaqueInvocation(
            context.invocation,
            physicalInvocations,
          );
          assert.equal(context.signal instanceof AbortSignal, true);
          assert.equal(context.signal.aborted, false);
          assert.equal(physicalSignals.has(context.signal), false);
          physicalSignals.add(context.signal);
          throw new Error(
            "same-process runtime launch must not reconcile",
          );
        },
        stateOwnerId,
        supervisorId,
      }),
      supervisorSettlement: physicalPolicies.supervisorSettlement,
      supervisorStateCollectionSettlement:
        physicalPolicies.supervisorStateCollectionSettlement,
      supervisorStateCollector: rawSupervisorStateCollector,
    });
    const runtimeOptions = {
      authority: {
        maxTransactionAttempts: 3,
        restoreAttachmentActivationV2FleetCompatible: true,
        restoreAttachmentActivationV2GenerationPredecessorFleetCompatible:
          true,
        restoreGenerationV2FleetCompatible: true,
        writerLaunchStopV3FleetCompatible: true,
      },
      foreground: {
        async fleetCapabilityGate({ admission, plan }) {
          calls.fleetGate += 1;
          const expectedPlan =
            admission.request.sessionId === sessionId
              ? stablePlan
              : recoveryStablePlan;
          assert.equal(
            sessionIds.includes(admission.request.sessionId),
            true,
          );
          assert.equal(plan.planSha256, expectedPlan?.planSha256);
          if (!foregroundAllowed) return null;
          if (holdForegroundGate) {
            foregroundGateEntered.resolve();
            await releaseForegroundGate.promise;
          }
          return POSTGRES_DETACHED_RESTORE_FLEET_CONFIRMED;
        },
      },
      launch: {
        imagePlanBinding: createPostgresDetachedRestoreImagePlanBinding({
          provider: Object.freeze({
            contractVersion:
              POSTGRES_DETACHED_RESTORE_IMAGE_PLAN_PROVIDER_CONTRACT_VERSION,
            imagePlanProviderId,
            async inspectCodex(input) {
              calls.image += 1;
              assert.equal(input.imagePlanProviderId, imagePlanProviderId);
              assertFreshOpaqueInvocation(
                input.invocation,
                imageProviderInvocations,
              );
              assert.equal(input.signal instanceof AbortSignal, true);
              assert.equal(input.signal.aborted, false);
              assert.equal(imageProviderSignals.has(input.signal), false);
              imageProviderSignals.add(input.signal);
              return frozenNullPrototypeRecord({
                codexBinaryPath: "/opt/portable-codex/bin/codex",
                codexBinarySha256: "c".repeat(64),
                codexVersion: input.inspection.codexVersion,
              });
            },
            async resolveImagePlan(input) {
              calls.image += 1;
              assert.equal(input.imagePlanProviderId, imagePlanProviderId);
              assertFreshOpaqueInvocation(
                input.invocation,
                imageProviderInvocations,
              );
              assert.equal(input.signal instanceof AbortSignal, true);
              assert.equal(input.signal.aborted, false);
              assert.equal(imageProviderSignals.has(input.signal), false);
              imageProviderSignals.add(input.signal);
              return frozenNullPrototypeRecord({
                configBytes: image.configBytes,
                descriptor: Object.freeze({ ...image.descriptor }),
              });
            },
          }),
          settlement: integrationImagePlanSettlements(
            imagePlanProviderSettlement,
          ),
        }),
        stoppedWriterCoordinator:
          new StoppedWriterCapabilityCoordinator(),
        supervisor: physicalBindings.supervisor,
        supervisorStateCollector:
          physicalBindings.supervisorStateCollector,
      },
      pools: {
        authority: interruptedAuthorityPool,
        foregroundLifecycle: foregroundLifecyclePool,
        operation: operationPool,
        recoveryLifecycle: recoveryLifecyclePool,
      },
      planRegistry: {
        operationalLeaseBudget,
        provisioningFleetCapabilityGate({ admission, plan }) {
          calls.planProvisioningGate += 1;
          const expectedPlan =
            admission.request.sessionId === sessionId
              ? stablePlan
              : recoveryStablePlan;
          assert.equal(
            sessionIds.includes(admission.request.sessionId),
            true,
          );
          assert.equal(plan.planSha256, expectedPlan?.planSha256);
          return POSTGRES_DETACHED_RESTORE_STABLE_PLAN_PROVISIONING_CONFIRMED;
        },
      },
      recovery: {
        intervalMilliseconds: 60_000,
        limits: {
          activation: 10,
          currentLaunch: 10,
          generation: 10,
          launchAttempt: 10,
          supervisorStateGc: 10,
        },
        onStep(receipt) {
          steps.push(receipt);
          if (steps.length === 1) firstStep.resolve(receipt);
        },
        recoveryScopeId: controllerRecoveryScopeId,
      },
      storage: {
        backendId: "postgres-authority-integration",
        lifecycleBackend: physicalBindings.lifecycleBackend,
        publication: physicalBindings.publication,
        resolveArtifactPaths({ checkpoint }) {
          calls.artifactResolver += 1;
          return {
            artifactDirectory: join(
              publicationTree.artifactOwnedRoot,
              checkpoint.artifactId,
            ),
            artifactOwnedRoot: publicationTree.artifactOwnedRoot,
          };
        },
        resolveRestoreDestination:
          physicalBindings.resolveRestoreDestination,
        resolveSourceOwnedRoot({ canonicalAttachment }) {
          calls.sourceResolver += 1;
          assert.equal(
            [
              publicationTree.sourceDirectory,
              publicationTree.recoverySourceDirectory,
            ].includes(canonicalAttachment.rootPath),
            true,
          );
          return {
            sourceDirectory: canonicalAttachment.rootPath,
            sourceOwnedRoot: publicationTree.sourceOwnedRoot,
          };
        },
      },
    };
    const controlledRuntime =
      createPostgresDetachedRestoreRuntimeComposition(runtimeOptions);
    assertPublicCheckpointBackendSurface(controlledRuntime.backend);
    assert.equal("foreground" in controlledRuntime, false);
    assert.equal("runRestore" in controlledRuntime, false);
    controller = createPostgresDetachedRestoreRuntimeController({
      runtime: controlledRuntime,
    });
    assertPublicCheckpointBackendSurface(controller.backend);
    assert.equal("foreground" in controller, false);
    assert.equal("runRestore" in controller, false);

    const blockedRegistration = registrationInput(blockedSessionId);
    const blockedArtifactId = `controller-artifact-${randomUUID()}`;
    const blockedCheckpointId = `controller-checkpoint-${randomUUID()}`;
    const blockedAdmission = {
      checkpoint: {
        artifactId: blockedArtifactId,
        backendId: blockedRegistration.storageRef.backendId,
        checkpointClass: "clean",
        checkpointId: blockedCheckpointId,
        codexSessionId: blockedRegistration.manifest.codex.sessionId,
        codexThreadId: blockedRegistration.manifest.codex.rootThreadId,
        contractVersion: 1,
        createdAt: new Date().toISOString(),
        imageDigest: blockedRegistration.manifest.runtime.imageDigest,
        sessionId: blockedSessionId,
        sourceFencingEpoch: "1",
        storageId: blockedRegistration.storageRef.storageId,
      },
      request: {
        backendId: blockedRegistration.storageRef.backendId,
        contractVersion: 1,
        fencingEpoch: "2",
        holderId: `controller-holder-${randomUUID()}`,
        leaseId: `controller-lease-${randomUUID()}`,
        operation: "restore",
        operationId: `controller-restore-${randomUUID()}`,
        sessionId: blockedSessionId,
        storageId: blockedRegistration.storageRef.storageId,
        target: {
          artifactId: blockedArtifactId,
          checkpointId: blockedCheckpointId,
          kind: "checkpoint",
        },
      },
    };
    const controllerRequestError = (error) => {
      assert.equal(
        error.code,
        "invalid_postgres_detached_restore_runtime_controller_request",
      );
      return true;
    };
    const assertControllerIngressClosed = async () => {
      await Promise.all([
        assert.rejects(
          controller.backend.restoreCheckpoint(blockedAdmission),
          controllerRequestError,
        ),
        assert.rejects(
          controller.stablePlanProvisioning.provisionStablePlan({}),
          controllerRequestError,
        ),
        assert.rejects(
          controller.imagePlanReservations.prepareImageReservation({}),
          controllerRequestError,
        ),
        assert.rejects(
          controller.writerLaunch.reconcileLaunchAttempt({}),
          controllerRequestError,
        ),
        assert.rejects(
          controller.writerLaunch.runLaunch({}),
          controllerRequestError,
        ),
      ]);
    };

    await assertControllerIngressClosed();
    const controllerStarting = controller.start();
    await assertControllerIngressClosed();
    const controllerReady = await settleWithin(
      controllerStarting,
      "restore runtime controller startup",
    );
    assert.deepEqual(structuredClone(controllerReady), { status: "ready" });
    const controllerFirst = await settleWithin(
      firstStep.promise,
      "restore runtime controller initial recovery step",
    );
    assert.equal(controllerFirst.status, "completed");
    assert.equal(controllerFirst.errorCode, null);
    assert.equal(controllerFirst.recovery.status, "sweep-complete");
    for (const field of [
      "generation",
      "activation",
      "launchAttempt",
      "currentLaunch",
      "supervisorStateGc",
    ]) {
      assert.equal(
        controllerFirst.recovery[field].batch.status,
        "sweep-complete",
      );
      assert.equal(controllerFirst.recovery[field].batch.results.length, 0);
    }
    await assert.rejects(
      controller.backend.restoreCheckpoint(blockedAdmission),
      (error) => {
        assert.equal(error.code, "stopped_directory_backend_outcome_uncertain");
        return true;
      },
    );
    for (const field of [
      "image",
      "provider",
      "publication",
      "publishRestoreDestination",
      "supervisorLaunch",
      "supervisorReconcile",
      "supervisorStop",
      "verifyCommittedRestoreDestination",
    ]) {
      assert.equal(calls[field], 0);
    }

    const blockingClient = await authorityPool.connect();
    let blockingTransactionOpen = false;
    try {
      await blockingClient.query("BEGIN");
      blockingTransactionOpen = true;
      await blockingClient.query(
        "LOCK TABLE session_authority.sessions IN ACCESS EXCLUSIVE MODE",
      );
      const admittedRestore =
        controller.backend.restoreCheckpoint(blockedAdmission);
      const admittedRestoreSettlement = assert.rejects(
        admittedRestore,
        (error) => {
          assert.equal(
            error.code,
            "stopped_directory_backend_outcome_uncertain",
          );
          return true;
        },
      );
      await waitForApplicationAdvisoryLock(operationPool, {
        applicationName: foregroundLifecycleApplicationName,
        mode: "ShareLock",
      });

      const controllerStopping = controller.stop();
      let controllerStopSettled = false;
      controllerStopping.then(
        () => {
          controllerStopSettled = true;
        },
        () => {
          controllerStopSettled = true;
        },
      );
      await assertControllerIngressClosed();
      const schedulerStopped = await settleWithin(
        controlledRuntime.scheduler.stop(),
        "restore runtime controller immediate scheduler stop",
      );
      assert.deepEqual(structuredClone(schedulerStopped), {
        status: "stopped",
      });
      assert.equal(controllerStopSettled, false);

      await blockingClient.query("ROLLBACK");
      blockingTransactionOpen = false;
      await admittedRestoreSettlement;
      const controllerStopped = await settleWithin(
        controllerStopping,
        "restore runtime controller shutdown drain",
      );
      assert.deepEqual(structuredClone(controllerStopped), {
        status: "stopped",
      });
      assert.equal(controllerStopSettled, true);
    } finally {
      if (blockingTransactionOpen) {
        await blockingClient.query("ROLLBACK");
      }
      blockingClient.release();
    }

    for (const pool of [
      authorityPool,
      operationPool,
      foregroundLifecyclePool,
      recoveryLifecyclePool,
    ]) {
      const alive = await pool.query("SELECT 1::integer AS alive");
      assert.deepEqual(alive.rows, [{ alive: 1 }]);
    }
    for (const field of [
      "image",
      "provider",
      "publication",
      "publishRestoreDestination",
      "supervisorLaunch",
      "supervisorReconcile",
      "supervisorStop",
      "verifyCommittedRestoreDestination",
    ]) {
      assert.equal(calls[field], 0);
    }

    steps.length = 0;
    firstStep = deferred();
    runtime = createPostgresDetachedRestoreRuntimeComposition({
      ...runtimeOptions,
      recovery: {
        ...runtimeOptions.recovery,
        recoveryScopeId,
      },
    });
    const completion = runtime.scheduler.start();
    const first = await settleWithin(
      firstStep.promise,
      "restore runtime initial recovery step",
    );
    assert.equal(first.status, "completed");
    assert.equal(first.recovery.status, "sweep-complete");
    assert.equal(first.recovery.recoveryScopeId, effectiveRecoveryScopeId);
    for (const field of [
      "generation",
      "activation",
      "launchAttempt",
      "currentLaunch",
      "supervisorStateGc",
    ]) {
      assert.equal(first.recovery[field].batch.status, "sweep-complete");
      assert.equal(first.recovery[field].batch.results.length, 0);
    }
    await new Promise((resolve) => setImmediate(resolve));

    const fixture = await prepareCommittedRestoreGenerationFixture(
      authority,
      checkpointAuthority,
      sessionId,
      {
        capturePublication: (context, captureAdmission) =>
          publishIntegrationCheckpointArtifact(
            physicalBindings.publication,
            context,
            captureAdmission,
          ),
        finalAttachmentRootPath: publicationTree.sourceDirectory,
        imageDigest: image.descriptor.digest,
        sourceAttachmentRootPath: publicationTree.sourceDirectory,
      },
    );
    const launchImagePlan = integrationDetachedRestoreImagePlan(
      fixture.finalized.session,
    );
    const imageReservation =
      await runtime.imagePlanReservations.prepareImageReservation({
        plan: launchImagePlan,
        sessionManifest: fixture.finalized.session.document.manifest,
      });
    assert.equal(
      isPostgresDetachedRestoreImagePlanReservation(imageReservation),
      true,
    );
    assert.equal(calls.image, 2);
    const launchAttemptId = `writer-launch-${randomUUID()}`;
    const launched = await runtime.writerLaunch.runLaunch({
      generation: fixture.finalized.generation,
      imageReservation,
      launchAttemptId,
    });
    assert.equal(launched.status, "started");
    assert.notEqual(launched.writer, null);
    assert.equal(launched.session.document.lifecycle, "ATTACHED");
    assert.notEqual(launched.session.document.launch, null);
    assert.equal(calls.supervisorLaunch, 1);
    assert.equal(calls.supervisorReconcile, 0);
    assert.equal(calls.supervisorStop, 0);
    assert.equal(calls.image, 4);

    const admission = restoreGenerationAdmission(
      launched,
      fixture.checkpoint,
    );
    const sourceArtifact = {
      artifactDirectory: join(
        publicationTree.artifactOwnedRoot,
        fixture.checkpoint.artifactId,
      ),
      artifactOwnedRoot: publicationTree.artifactOwnedRoot,
    };
    stablePlan = createPostgresDetachedRestorePlan({
      request: admission.request,
      plan: {
        captureCreatedAt: new Date().toISOString(),
        destinationDirectory: publicationTree.destinationDirectory,
        destinationOwnedRoot: publicationTree.destinationOwnedRoot,
        detachMode: "release",
        holderId: `restore-holder-${randomUUID()}`,
        imagePlanId: `image-plan-${randomUUID()}`,
        leaseDurationMilliseconds:
          operationalLeaseBudget.leaseDurationMilliseconds,
        sourceArtifactDirectory: sourceArtifact.artifactDirectory,
        sourceArtifactOwnedRoot: sourceArtifact.artifactOwnedRoot,
      },
    });

    const provisionedStablePlan =
      await runtime.stablePlanProvisioning.provisionStablePlan({
        admission,
        plan: stablePlan,
      });
    assert.notStrictEqual(provisionedStablePlan, stablePlan);
    assert.equal(provisionedStablePlan.planSha256, stablePlan.planSha256);
    assert.equal(calls.planProvisioningGate, 1);

    const rawStablePlanInput = {
      captureCreatedAt: stablePlan.captureCreatedAt,
      destinationDirectory: stablePlan.destinationDirectory,
      destinationOwnedRoot: stablePlan.destinationOwnedRoot,
      detachMode: stablePlan.detachMode,
      holderId: stablePlan.holderId,
      imagePlanId: stablePlan.imagePlanId,
      leaseDurationMilliseconds:
        stablePlan.leaseDurationMilliseconds,
      sourceArtifactDirectory: stablePlan.sourceArtifactDirectory,
      sourceArtifactOwnedRoot: stablePlan.sourceArtifactOwnedRoot,
    };
    const rawBindingSha256 = "b".repeat(64);
    const rawPlanSha256 = "a".repeat(64);
    const hostileDigestClaims = [
      {
        bindingSha256Json: "null",
        label: "null-binding-sha",
        planSha256Json: JSON.stringify(rawPlanSha256),
      },
      {
        bindingSha256Json: "1".repeat(64),
        label: "numeric-binding-sha",
        planSha256Json: JSON.stringify(rawPlanSha256),
      },
      {
        bindingSha256Json: JSON.stringify(rawBindingSha256),
        label: "null-plan-sha",
        planSha256Json: "null",
      },
      {
        bindingSha256Json: JSON.stringify(rawBindingSha256),
        label: "numeric-plan-sha",
        planSha256Json: "1".repeat(64),
      },
    ];
    for (const hostile of hostileDigestClaims) {
      const operationId =
        `hostile-${hostile.label}-${randomUUID()}`;
      const request = structuredClone(admission.request);
      request.operationId = operationId;
      await assert.rejects(
        authorityPool.query(
          [
            "INSERT INTO session_authority.operation_id_registry",
            "(operation_id, session_id, claim_type, claimant_operation_id,",
            "binding, claimed_at, materialized_at)",
            "VALUES ($1, $2::uuid,",
            "'detached-restore-stable-plan-v1', NULL,",
            "pg_catalog.jsonb_build_object(",
            "'bindingSha256', $3::jsonb,",
            "'contractVersion', 1,",
            "'planSha256', $4::jsonb,",
            "'request', $5::jsonb),",
            "pg_catalog.transaction_timestamp(), NULL)",
          ].join(" "),
          [
            operationId,
            sessionId,
            hostile.bindingSha256Json,
            hostile.planSha256Json,
            JSON.stringify(request),
          ],
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
    }

    const hostileStablePlans = [
      {
        constraint:
          "detached_restore_stable_plans_request_identity",
        label: "missing-operation-id",
        mutateAdmission(candidate) {
          delete candidate.request.operationId;
        },
      },
      {
        backendId: "1",
        constraint:
          "detached_restore_stable_plans_request_identity",
        label: "numeric-backend-id",
        mutateAdmission(candidate) {
          candidate.request.backendId = 1;
        },
      },
      {
        constraint: "detached_restore_stable_plans_request_shape",
        label: "missing-operation",
        mutateAdmission(candidate) {
          delete candidate.request.operation;
        },
      },
      {
        constraint: "detached_restore_stable_plans_request_shape",
        label: "numeric-target-kind",
        mutateAdmission(candidate) {
          candidate.request.target.kind = 1;
        },
      },
      {
        constraint:
          "detached_restore_stable_plans_plan_input_object",
        label: "null-plan-input",
        planInput: Object.fromEntries(
          Object.keys(rawStablePlanInput).map((key) => [key, null]),
        ),
      },
      {
        constraint:
          "detached_restore_stable_plans_plan_input_object",
        label: "string-lease-duration",
        planInput: {
          ...rawStablePlanInput,
          leaseDurationMilliseconds: "300000",
        },
      },
    ];
    const hostilePlanClient = await authorityPool.connect();
    try {
      for (const hostile of hostileStablePlans) {
        const operationId =
          `hostile-plan-${hostile.label}-${randomUUID()}`;
        const hostileAdmission = structuredClone(admission);
        hostileAdmission.request.operationId = operationId;
        hostile.mutateAdmission?.(hostileAdmission);
        await hostilePlanClient.query("BEGIN");
        try {
          await insertRawDetachedRestoreStablePlanClaim(
            hostilePlanClient,
            {
              admission: hostileAdmission,
              bindingSha256: rawBindingSha256,
              operationId,
              planSha256: rawPlanSha256,
              sessionId,
            },
          );
          await assert.rejects(
            insertRawDetachedRestoreStablePlan(hostilePlanClient, {
              admission: hostileAdmission,
              backendId:
                hostile.backendId ??
                hostileAdmission.request.backendId,
              bindingSha256: rawBindingSha256,
              operationId,
              planInput:
                hostile.planInput ?? rawStablePlanInput,
              planSha256: rawPlanSha256,
              sessionId,
              storageId: hostileAdmission.request.storageId,
            }),
            (error) => {
              assert.equal(error.code, "23514");
              assert.equal(error.constraint, hostile.constraint);
              return true;
            },
          );
        } finally {
          await hostilePlanClient.query("ROLLBACK");
        }
      }
    } finally {
      hostilePlanClient.release();
    }

    const deleteFixtureOperationId =
      `stable-operation-delete-${randomUUID()}`;
    const deleteFixtureAdmission = structuredClone(admission);
    deleteFixtureAdmission.request.operationId =
      deleteFixtureOperationId;
    const deleteFixtureClient = await authorityPool.connect();
    let deleteFixtureTransactionOpen = false;
    try {
      await deleteFixtureClient.query("BEGIN");
      deleteFixtureTransactionOpen = true;
      await insertRawDetachedRestoreStablePlanClaim(
        deleteFixtureClient,
        {
          admission: deleteFixtureAdmission,
          bindingSha256: rawBindingSha256,
          operationId: deleteFixtureOperationId,
          planSha256: rawPlanSha256,
          sessionId,
        },
      );
      await insertRawDetachedRestoreStablePlan(deleteFixtureClient, {
        admission: deleteFixtureAdmission,
        backendId: deleteFixtureAdmission.request.backendId,
        bindingSha256: rawBindingSha256,
        operationId: deleteFixtureOperationId,
        planInput: rawStablePlanInput,
        planSha256: rawPlanSha256,
        sessionId,
        storageId: deleteFixtureAdmission.request.storageId,
      });
      await deleteFixtureClient.query("COMMIT");
      deleteFixtureTransactionOpen = false;

      await deleteFixtureClient.query("BEGIN");
      deleteFixtureTransactionOpen = true;
      const materializedAt = await deleteFixtureClient.query(
        "SELECT pg_catalog.transaction_timestamp() AS value",
      );
      const operationCreatedAt = materializedAt.rows[0].value;
      await deleteFixtureClient.query(
        [
          "UPDATE session_authority.operation_id_registry",
          "SET materialized_at = $2",
          "WHERE operation_id = $1",
        ].join(" "),
        [deleteFixtureOperationId, operationCreatedAt],
      );
      await deleteFixtureClient.query(
        [
          "INSERT INTO session_authority.operation_claims",
          "(operation_id, session_id, kind, request, result, state,",
          "revision, created_at, updated_at, retired_at)",
          "VALUES ($1, $2::uuid, 'restore-destination-generation-v1',",
          "$3::jsonb, NULL, 'committed', 0, $4, $4, $4)",
        ].join(" "),
        [
          deleteFixtureOperationId,
          sessionId,
          JSON.stringify({
            payload: {
              admission: deleteFixtureAdmission,
              contractVersion: 1,
            },
          }),
          operationCreatedAt,
        ],
      );
      await deleteFixtureClient.query("COMMIT");
      deleteFixtureTransactionOpen = false;

      await assert.rejects(
        deleteFixtureClient.query(
          [
            "DELETE FROM session_authority.operation_claims",
            "WHERE operation_id = $1",
          ].join(" "),
          [deleteFixtureOperationId],
        ),
        (error) => {
          assert.equal(error.code, "23503");
          assert.equal(
            error.constraint,
            "operation_claims_stable_plan_delete_requires_teardown",
          );
          return true;
        },
      );
      const operationAfterRejectedDelete =
        await deleteFixtureClient.query(
          [
            "SELECT operation_id",
            "FROM session_authority.operation_claims",
            "WHERE operation_id = $1",
          ].join(" "),
          [deleteFixtureOperationId],
        );
      assert.deepEqual(operationAfterRejectedDelete.rows, [
        { operation_id: deleteFixtureOperationId },
      ]);

      await deleteFixtureClient.query("BEGIN");
      deleteFixtureTransactionOpen = true;
      await deleteFixtureClient.query(
        [
          "DELETE FROM session_authority.detached_restore_stable_plans",
          "WHERE operation_id = $1",
        ].join(" "),
        [deleteFixtureOperationId],
      );
      await deleteFixtureClient.query(
        [
          "DELETE FROM session_authority.operation_claims",
          "WHERE operation_id = $1",
        ].join(" "),
        [deleteFixtureOperationId],
      );
      await deleteFixtureClient.query(
        [
          "DELETE FROM session_authority.operation_id_registry",
          "WHERE operation_id = $1",
        ].join(" "),
        [deleteFixtureOperationId],
      );
      await deleteFixtureClient.query("COMMIT");
      deleteFixtureTransactionOpen = false;
      const teardownState = await deleteFixtureClient.query(
        [
          "SELECT",
          "EXISTS (SELECT 1",
          "FROM session_authority.detached_restore_stable_plans",
          "WHERE operation_id = $1) AS stable_plan_exists,",
          "EXISTS (SELECT 1",
          "FROM session_authority.operation_claims",
          "WHERE operation_id = $1) AS operation_exists,",
          "EXISTS (SELECT 1",
          "FROM session_authority.operation_id_registry",
          "WHERE operation_id = $1) AS registry_exists",
        ].join(" "),
        [deleteFixtureOperationId],
      );
      assert.deepEqual(teardownState.rows, [
        {
          operation_exists: false,
          registry_exists: false,
          stable_plan_exists: false,
        },
      ]);
    } finally {
      if (deleteFixtureTransactionOpen) {
        await deleteFixtureClient.query("ROLLBACK");
      }
      deleteFixtureClient.release();
    }

    const crossedStableClaimPlanSha256 =
      stablePlan.planSha256 === "0".repeat(64)
        ? "1".repeat(64)
        : "0".repeat(64);
    const stableClaimUpdateAttempts = [
      {
        code: "55000",
        constraint:
          "operation_id_registry_detached_restore_stable_plan_immutable",
        text: [
          "UPDATE session_authority.operation_id_registry",
          "SET binding = pg_catalog.jsonb_set(",
          "binding, '{planSha256}', pg_catalog.to_jsonb($2::text), false)",
          "WHERE operation_id = $1",
        ].join(" "),
        values: [
          admission.request.operationId,
          crossedStableClaimPlanSha256,
        ],
      },
      {
        code: "55000",
        constraint:
          "operation_id_registry_detached_restore_stable_plan_immutable",
        text: [
          "UPDATE session_authority.operation_id_registry",
          "SET claim_type = 'direct-operation', binding = NULL,",
          "materialized_at = claimed_at",
          "WHERE operation_id = $1",
        ].join(" "),
        values: [admission.request.operationId],
      },
      {
        code: "23514",
        constraint:
          "detached_restore_stable_plan_claim_materialization",
        text: [
          "UPDATE session_authority.operation_id_registry",
          "SET materialized_at = pg_catalog.transaction_timestamp()",
          "WHERE operation_id = $1",
        ].join(" "),
        values: [admission.request.operationId],
      },
    ];
    for (const attempt of stableClaimUpdateAttempts) {
      await assert.rejects(
        authorityPool.query(attempt.text, attempt.values),
        (error) => {
          assert.equal(error.code, attempt.code);
          assert.equal(error.constraint, attempt.constraint);
          return true;
        },
      );
    }
    const stableClaimAfterRejectedUpdates = await authorityPool.query(
      [
        "SELECT claim_type, binding ->> 'planSha256' AS plan_sha256,",
        "materialized_at",
        "FROM session_authority.operation_id_registry",
        "WHERE operation_id = $1",
      ].join(" "),
      [admission.request.operationId],
    );
    assert.deepEqual(stableClaimAfterRejectedUpdates.rows, [
      {
        claim_type: "detached-restore-stable-plan-v1",
        materialized_at: null,
        plan_sha256: stablePlan.planSha256,
      },
    ]);

    let restartedRegistryGateCalls = 0;
    const restartedRegistry =
      createPostgresDetachedRestoreStablePlanRegistry({
        operationalLeaseBudget,
        provisioningFleetCapabilityGate() {
          restartedRegistryGateCalls += 1;
          return POSTGRES_DETACHED_RESTORE_STABLE_PLAN_PROVISIONING_CONFIRMED;
        },
        store: new PostgresSerializableStore({
          dedicatedPool: authorityPool,
          maxTransactionAttempts: 3,
        }),
      });
    const restartedPlan = await restartedRegistry.resolveStablePlan({
      admission,
      expectedSession: launched.session,
    });
    assert.notStrictEqual(restartedPlan, stablePlan);
    assert.equal(restartedPlan.planSha256, stablePlan.planSha256);
    assert.equal(restartedRegistryGateCalls, 0);

    await restartedRegistry.provisionStablePlan({
      admission,
      plan: stablePlan,
    });
    assert.equal(restartedRegistryGateCalls, 1);

    const crossedPlan = createPostgresDetachedRestorePlan({
      request: admission.request,
      plan: {
        captureCreatedAt: stablePlan.captureCreatedAt,
        destinationDirectory: stablePlan.destinationDirectory,
        destinationOwnedRoot: stablePlan.destinationOwnedRoot,
        detachMode: stablePlan.detachMode,
        holderId: stablePlan.holderId,
        imagePlanId: `${stablePlan.imagePlanId}-crossed`,
        leaseDurationMilliseconds:
          stablePlan.leaseDurationMilliseconds,
        sourceArtifactDirectory: stablePlan.sourceArtifactDirectory,
        sourceArtifactOwnedRoot: stablePlan.sourceArtifactOwnedRoot,
      },
    });
    await assert.rejects(
      restartedRegistry.provisionStablePlan({
        admission,
        plan: crossedPlan,
      }),
      (error) => {
        assert(
          error instanceof PostgresDetachedRestoreStablePlanRegistryError,
        );
        assert.equal(
          error.code,
          "postgres_detached_restore_stable_plan_registry_identity_conflict",
        );
        return true;
      },
    );
    assert.equal(restartedRegistryGateCalls, 2);

    const acknowledgementLossAdmission = structuredClone(admission);
    acknowledgementLossAdmission.request.operationId =
      `restore-plan-ack-loss-${randomUUID()}`;
    const acknowledgementLossPlan = createPostgresDetachedRestorePlan({
      request: acknowledgementLossAdmission.request,
      plan: {
        captureCreatedAt: stablePlan.captureCreatedAt,
        destinationDirectory: `${stablePlan.destinationDirectory}-ack-loss`,
        destinationOwnedRoot: stablePlan.destinationOwnedRoot,
        detachMode: stablePlan.detachMode,
        holderId: `${stablePlan.holderId}-ack-loss`,
        imagePlanId: `${stablePlan.imagePlanId}-ack-loss`,
        leaseDurationMilliseconds:
          stablePlan.leaseDurationMilliseconds,
        sourceArtifactDirectory: stablePlan.sourceArtifactDirectory,
        sourceArtifactOwnedRoot: stablePlan.sourceArtifactOwnedRoot,
      },
    });
    let acknowledgementLossGateCalls = 0;
    const acknowledgementLossRegistry =
      createPostgresDetachedRestoreStablePlanRegistry({
        operationalLeaseBudget,
        provisioningFleetCapabilityGate() {
          acknowledgementLossGateCalls += 1;
          return POSTGRES_DETACHED_RESTORE_STABLE_PLAN_PROVISIONING_CONFIRMED;
        },
        store: new PostgresSerializableStore({
          dedicatedPool: firstCommitAcknowledgementLossPool(authorityPool),
          maxTransactionAttempts: 3,
        }),
      });
    const acknowledgedByReadback =
      await acknowledgementLossRegistry.provisionStablePlan({
        admission: acknowledgementLossAdmission,
        plan: acknowledgementLossPlan,
      });
    assert.equal(
      acknowledgedByReadback.planSha256,
      acknowledgementLossPlan.planSha256,
    );
    assert.equal(acknowledgementLossGateCalls, 1);
    const recoveredAcknowledgementLossPlan =
      await restartedRegistry.resolveStablePlan({
        admission: acknowledgementLossAdmission,
        expectedSession: launched.session,
      });
    assert.equal(
      recoveredAcknowledgementLossPlan.planSha256,
      acknowledgementLossPlan.planSha256,
    );
    const stablePlanRows = await authorityPool.query(
      [
        "SELECT operation_id",
        "FROM session_authority.detached_restore_stable_plans",
        "WHERE session_id = $1::uuid",
        "ORDER BY operation_id",
      ].join(" "),
      [sessionId],
    );
    assert.deepEqual(
      stablePlanRows.rows.map(({ operation_id: operationId }) => operationId),
      [
        acknowledgementLossAdmission.request.operationId,
        admission.request.operationId,
      ].sort(),
    );
    await assert.rejects(
      authorityPool.query(
        [
          "DELETE FROM session_authority.detached_restore_stable_plans",
          "WHERE operation_id = $1",
        ].join(" "),
        [admission.request.operationId],
      ),
      (error) => {
        assert.equal(error.code, "23503");
        assert.equal(
          error.constraint,
          "detached_restore_stable_plans_delete_requires_claim_teardown",
        );
        return true;
      },
    );
    const planAfterRejectedDelete =
      await restartedRegistry.resolveStablePlan({
        admission,
        expectedSession: launched.session,
      });
    assert.equal(planAfterRejectedDelete.planSha256, stablePlan.planSha256);

    const beforeGateReject = await readSessionAuthorityMutationSnapshot(
      authorityPool,
      sessionId,
    );
    await assert.rejects(
      runtime.backend.restoreCheckpoint(admission, async () => null),
      (error) => {
        assert.equal(
          error.code,
          "invalid_stopped_directory_backend_request",
        );
        return true;
      },
    );
    await assert.rejects(
      runtime.backend.restoreCheckpoint(admission),
      (error) => {
        assert.equal(
          error.code,
          "stopped_directory_backend_outcome_uncertain",
        );
        return true;
      },
    );
    assert.deepEqual(
      await readSessionAuthorityMutationSnapshot(authorityPool, sessionId),
      beforeGateReject,
    );
    assert.equal(calls.fleetGate, 1);
    assert.equal(calls.provider, 0);
    assert.equal(calls.publishRestoreDestination, 0);
    assert.equal(calls.verifyCommittedRestoreDestination, 0);

    foregroundAllowed = true;
    holdForegroundGate = true;

    const beforeSession = await authority.readSession({ sessionId });
    publicationObservation.restoreOperationId =
      admission.request.operationId;
    const restore = runtime.backend.restoreCheckpoint(admission);
    let busyStep = null;
    let primaryFailure = null;
    try {
      await settleWithin(
        foregroundGateEntered.promise,
        "restore runtime foreground lifecycle lease",
      );
      busyStep = runtime.scheduler.runStep({ signal: null });
      const busy = await settleWithin(
        busyStep,
        "restore runtime recovery collision",
      );
      assert.deepEqual(structuredClone(busy), {
        errorCode: null,
        recovery: null,
        status: "busy",
      });
    } catch (error) {
      primaryFailure = error;
    } finally {
      releaseForegroundGate.resolve();
      foregroundTeardown = (async () => {
        const stoppedCompletion = runtime.scheduler.stop();
        const effects =
          busyStep === null
            ? [restore, completion]
            : [restore, busyStep, completion];
        const settlements = await Promise.allSettled(effects);
        return {
          backendSettlement: settlements[0],
          busySettlement:
            busyStep === null ? null : settlements[1],
          schedulerSettlement:
            settlements[busyStep === null ? 1 : 2],
          stoppedCompletion,
        };
      })();
    }

    let teardown = null;
    try {
      teardown = await settleWithin(
        foregroundTeardown,
        "restore runtime backend and scheduler shutdown",
      );
    } catch (error) {
      if (primaryFailure === null) primaryFailure = error;
    }
    if (primaryFailure !== null) throw primaryFailure;
    if (teardown.busySettlement?.status === "rejected") {
      throw teardown.busySettlement.reason;
    }
    assert.equal(teardown.busySettlement?.status, "fulfilled");
    if (teardown.schedulerSettlement.status === "rejected") {
      throw teardown.schedulerSettlement.reason;
    }
    assert.equal(teardown.backendSettlement.status, "rejected");
    assert.equal(
      teardown.backendSettlement.reason.code,
      "stopped_directory_backend_outcome_uncertain",
    );
    const afterSession = await authority.readSession({ sessionId });
    assert.notEqual(beforeSession.document.launch, null);
    assert.equal(afterSession.document.launch, null);
    assert.equal(
      afterSession.document.lastOperation?.kind,
      RESTORE_DESTINATION_GENERATION_OPERATION_KIND,
    );
    assert.equal(
      afterSession.document.lastOperation?.operationId,
      admission.request.operationId,
    );
    assert.equal(afterSession.document.lastOperation?.state, "committed");
    assert.equal(afterSession.document.activeOperation, null);
    const captureOperation = await authorityPool.query(
      [
        "SELECT kind, state",
        "FROM session_authority.operation_claims",
        "WHERE operation_id = $1",
      ].join(" "),
      [stablePlan.captureOperationId],
    );
    assert.deepEqual(captureOperation.rows, [
      {
        kind: CHECKPOINT_CAPTURE_OPERATION_KIND,
        state: "committed",
      },
    ]);
    assert.equal(
      generationCommitAcknowledgementLossPool.didLoseAcknowledgement(),
      true,
    );
    assert.equal(interruptedAuthorityPool.didFail(), true);
    const committedGenerationBeforeRestart = await authorityPool.query(
      [
        "SELECT g.state, g.document, g.committed_at,",
        "o.state AS operation_state,",
        "o.revision::text AS operation_revision, o.result",
        "FROM session_authority.restore_destination_generations AS g",
        "JOIN session_authority.operation_claims AS o",
        "ON o.operation_id = g.operation_id",
        "WHERE g.operation_id = $1",
      ].join(" "),
      [admission.request.operationId],
    );
    assert.equal(committedGenerationBeforeRestart.rows.length, 1);
    assert.equal(committedGenerationBeforeRestart.rows[0].state, "committed");
    assert.equal(
      committedGenerationBeforeRestart.rows[0].operation_state,
      "committed",
    );
    const destinationBeforeRestart = await lstat(
      publicationTree.destinationDirectory,
      { bigint: true },
    );
    assert.equal(calls.artifactResolver > 0, true);
    assert.equal(calls.destinationResolver, 0);
    assert.equal(calls.fleetGate, 2);
    assert.equal(calls.image, 4);
    assert.equal(calls.planProvisioningGate, 1);
    assert.equal(calls.provider, 0);
    assert.equal(calls.publishRestoreDestination, 1);
    assert.equal(calls.verifyCommittedRestoreDestination, 0);
    assert.equal(calls.sourceResolver > 0, true);
    assert.equal(calls.supervisorLaunch, 1);
    assert.equal(calls.supervisorReconcile, 0);
    assert.equal(calls.supervisorStop, 1);
    assert.equal(physicalInvocations.size, 2);
    assert.equal(physicalSignals.size, 2);
    assert.deepEqual(publicationObservation.events, [
      "publishRestoreDestination",
    ]);
    assert.equal(
      publicationObservation.ownedRootInspections.includes(
        publicationTree.artifactOwnedRoot,
      ),
      true,
    );
    assert.equal(
      publicationObservation.ownedRootInspections.includes(
        publicationTree.destinationOwnedRoot,
      ),
      true,
    );
    for (const signal of physicalSignals) {
      assert.equal(signal.aborted, false);
    }

    const recoveryFixture = await prepareCommittedRestoreGenerationFixture(
      authority,
      checkpointAuthority,
      blockedSessionId,
      {
        capturePublication: (context, captureAdmission) =>
          publishIntegrationCheckpointArtifact(
            physicalBindings.publication,
            context,
            captureAdmission,
          ),
        finalAttachmentRootPath:
          publicationTree.recoverySourceDirectory,
        imageDigest: image.descriptor.digest,
        sourceAttachmentRootPath:
          publicationTree.recoverySourceDirectory,
      },
    );
    const recoveryImageReservation =
      await runtime.imagePlanReservations.prepareImageReservation({
        plan: integrationDetachedRestoreImagePlan(
          recoveryFixture.finalized.session,
        ),
        sessionManifest:
          recoveryFixture.finalized.session.document.manifest,
      });
    const recoveryLaunchAttemptId = `writer-launch-${randomUUID()}`;
    assert.notEqual(recoveryLaunchAttemptId, launchAttemptId);
    const recoveryLaunched = await runtime.writerLaunch.runLaunch({
      generation: recoveryFixture.finalized.generation,
      imageReservation: recoveryImageReservation,
      launchAttemptId: recoveryLaunchAttemptId,
    });
    assert.equal(recoveryLaunched.status, "started");
    const recoveryAdmission = restoreGenerationAdmission(
      recoveryLaunched,
      recoveryFixture.checkpoint,
    );
    recoveryStablePlan = createPostgresDetachedRestorePlan({
      request: recoveryAdmission.request,
      plan: {
        captureCreatedAt: new Date().toISOString(),
        destinationDirectory:
          publicationTree.recoveryDestinationDirectory,
        destinationOwnedRoot: publicationTree.destinationOwnedRoot,
        detachMode: "release",
        holderId: `restore-holder-${randomUUID()}`,
        imagePlanId: `image-plan-${randomUUID()}`,
        leaseDurationMilliseconds:
          operationalLeaseBudget.leaseDurationMilliseconds,
        sourceArtifactDirectory: join(
          publicationTree.artifactOwnedRoot,
          recoveryFixture.checkpoint.artifactId,
        ),
        sourceArtifactOwnedRoot: publicationTree.artifactOwnedRoot,
      },
    });
    await runtime.stablePlanProvisioning.provisionStablePlan({
      admission: recoveryAdmission,
      plan: recoveryStablePlan,
    });
    publicationObservation.captureFailureOperationId =
      recoveryStablePlan.captureOperationId;
    publicationObservation.captureFailureArmed = true;
    await assert.rejects(
      runtime.backend.restoreCheckpoint(recoveryAdmission),
      (error) => {
        assert.equal(
          error.code,
          "stopped_directory_backend_outcome_uncertain",
        );
        return true;
      },
    );
    assert.equal(publicationObservation.captureFailureCount, 1);
    assert.equal(calls.publishFreshCheckpointArtifact, 1);
    // The failed fresh invocation reads the absent journal before preparing,
    // then its same-call committed-only fallback reads the prepared record.
    assert.equal(calls.verifyCommittedCheckpointArtifact, 2);
    const recoveryCaptureBeforeRestart = await authorityPool.query(
      [
        "SELECT kind, state, revision::text AS revision",
        "FROM session_authority.operation_claims",
        "WHERE operation_id = $1",
      ].join(" "),
      [recoveryStablePlan.captureOperationId],
    );
    assert.deepEqual(recoveryCaptureBeforeRestart.rows, [
      {
        kind: CHECKPOINT_CAPTURE_OPERATION_KIND,
        revision: "2",
        state: "uncertain",
      },
    ]);

    assert.strictEqual(teardown.stoppedCompletion, completion);
    assert.deepEqual(
      structuredClone(teardown.schedulerSettlement.value),
      {
        status: "stopped",
      },
    );
    assert.equal(steps.length, 2);
    assert.equal(steps[1].status, "busy");

    const stored = await authorityPool.query(
      [
        "SELECT lane, cycle::text, revision::text, after_session_id",
        "FROM session_authority.restore_recovery_cursors",
        "WHERE recovery_scope_id = $1",
        "ORDER BY lane",
      ].join(" "),
      [effectiveRecoveryScopeId],
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
      {
        after_session_id: null,
        cycle: "1",
        lane: "supervisor-state-gc",
        revision: "1",
      },
    ]);

    assert.deepEqual(
      structuredClone(await physicalBindings.stop()),
      { status: "stopped" },
    );

    const firstSupervisorStateGcPage =
      await authority.listWriterSupervisorStateGcCandidates({
        afterAuthorizedAt: null,
        afterSessionId: null,
        afterTerminalOperationId: null,
        limit: 1,
        stateOwnerId,
      });
    assert.equal(firstSupervisorStateGcPage.candidates.length, 1);
    assert.notEqual(firstSupervisorStateGcPage.nextAfterAuthorizedAt, null);
    assert.notEqual(firstSupervisorStateGcPage.nextAfterSessionId, null);
    assert.notEqual(
      firstSupervisorStateGcPage.nextAfterTerminalOperationId,
      null,
    );
    const secondSupervisorStateGcPage =
      await authority.listWriterSupervisorStateGcCandidates({
        afterAuthorizedAt:
          firstSupervisorStateGcPage.nextAfterAuthorizedAt,
        afterSessionId: firstSupervisorStateGcPage.nextAfterSessionId,
        afterTerminalOperationId:
          firstSupervisorStateGcPage.nextAfterTerminalOperationId,
        limit: 1,
        stateOwnerId,
      });
    assert.equal(secondSupervisorStateGcPage.candidates.length, 1);
    assert.equal(secondSupervisorStateGcPage.nextAfterAuthorizedAt, null);
    assert.equal(secondSupervisorStateGcPage.nextAfterSessionId, null);
    assert.equal(
      secondSupervisorStateGcPage.nextAfterTerminalOperationId,
      null,
    );
    assert.deepEqual(
      [
        firstSupervisorStateGcPage.candidates[0].authorization
          .launchAttemptId,
        secondSupervisorStateGcPage.candidates[0].authorization
          .launchAttemptId,
      ].sort(),
      [launchAttemptId, recoveryLaunchAttemptId].sort(),
    );

    const restartedCalls = {
      artifactResolver: 0,
      destinationResolver: 0,
      fleetGate: 0,
      image: 0,
      planProvisioningGate: 0,
      provider: 0,
      publication: 0,
      publishFreshCheckpointArtifact: 0,
      publishRestoreDestination: 0,
      sourceResolver: 0,
      supervisorLaunch: 0,
      supervisorReconcile: 0,
      verifyCommittedCheckpointArtifact: 0,
      verifyCommittedRestoreDestination: 0,
    };
    const restartedPhysicalInvocations = new Set();
    const restartedPhysicalSignals = new Set();
    const restartedLifecycleBackend =
      restoreRuntimeIntegrationLifecycleBackend(
        restartedCalls,
        restartedPhysicalInvocations,
        restartedPhysicalSignals,
      );
    const restartedPublication = restoreRuntimePublicationEvidence(
      restartedCalls,
      publicationObservation,
      publicationTree.journalDirectory,
    );
    restartedPhysicalBindings =
      createPostgresDetachedRestorePhysicalBindings({
        lifecycleBackend: restartedLifecycleBackend,
        lifecycleSettlement:
          physicalPolicies.lifecycleBackendSettlement,
        onFatal() {
          assert.fail(
            "restarted integration physical collaborator must settle",
          );
        },
        publication: restartedPublication,
        publicationSettlement: physicalPolicies.publicationSettlement,
        async resolveRestoreDestination(input) {
          restartedCalls.destinationResolver += 1;
          void input;
          throw new Error(
            "committed restore retry must not resolve a destination",
          );
        },
        resolveRestoreDestinationContractVersion: 1,
        resolveRestoreDestinationSettlement:
          physicalPolicies.resolveRestoreDestinationSettlement,
        supervisor: Object.freeze({
          contractVersion:
            POSTGRES_LOGICAL_WRITER_SUPERVISOR_PHYSICAL_CONTRACT_VERSION,
          async launchWriter(input) {
            restartedCalls.supervisorLaunch += 1;
            void input;
            throw new Error(
              "committed restore retry must not launch a writer",
            );
          },
          async reconcileWriterLaunch(input) {
            restartedCalls.supervisorReconcile += 1;
            void input;
            throw new Error(
              "committed restore retry must not reconcile a writer",
            );
          },
          stateOwnerId,
          supervisorId,
        }),
        supervisorSettlement: physicalPolicies.supervisorSettlement,
        supervisorStateCollectionSettlement:
          physicalPolicies.supervisorStateCollectionSettlement,
        supervisorStateCollector: rawSupervisorStateCollector,
      });
    const restartedImagePlanProviderId =
      `restarted-image-provider-${randomUUID()}`;
    const restartedAuthorityReads = {
      activation: 0,
      generation: 0,
      launch: 0,
    };
    const restartedAuthorityPool = Object.freeze({
      async connect() {
        const client = await authorityPool.connect();
        return {
          connection: client.connection,
          query(...args) {
            const input = args[0];
            const text =
              typeof input === "string" ? input : input?.text;
            const values =
              typeof input === "string" ? args[1] : input?.values;
            if (
              typeof text === "string" &&
              text.startsWith("SELECT") &&
              text.includes(
                "FROM session_authority.restore_destination_generations",
              ) &&
              text.includes("WHERE generation_id = $1") &&
              Array.isArray(values) &&
              values[0] === stablePlan.generationId
            ) {
              restartedAuthorityReads.generation += 1;
            }
            if (
              typeof text === "string" &&
              text.startsWith("SELECT") &&
              text.includes(
                "FROM session_authority.operation_claims",
              ) &&
              text.includes("WHERE operation_id = $1") &&
              Array.isArray(values)
            ) {
              if (values[0] === stablePlan.activationOperationId) {
                restartedAuthorityReads.activation += 1;
              }
              if (values[0] === stablePlan.launchAttemptId) {
                restartedAuthorityReads.launch += 1;
              }
            }
            return Reflect.apply(client.query, client, args);
          },
          release(...args) {
            return Reflect.apply(client.release, client, args);
          },
        };
      },
    });
    const restartedForegroundLifecycleTraces = [];
    const restartedForegroundLifecyclePool = Object.freeze({
      connect(callback) {
        const trace = [];
        restartedForegroundLifecycleTraces.push(trace);
        return foregroundLifecyclePool.connect(
          (error, client, release) => {
            if (error !== null && error !== undefined) {
              return callback(error, client, release);
            }
            const query = client.query;
            const tracedClient = Object.freeze({
              connection: client.connection,
              query(...args) {
                const input = args[0];
                const text =
                  typeof input === "string" ? input : input?.text;
                if (text === "DISCARD ALL") {
                  trace.push("discard");
                } else if (text?.includes("pg_try_advisory_lock")) {
                  trace.push("acquire");
                } else if (text?.includes("FROM pg_catalog.pg_locks")) {
                  trace.push("assert-held");
                } else if (text?.includes("pg_advisory_unlock")) {
                  trace.push("unlock");
                } else {
                  trace.push("unexpected-query");
                }
                return Reflect.apply(query, client, args);
              },
              release(...args) {
                return Reflect.apply(release, client, args);
              },
            });
            return callback(null, tracedClient, tracedClient.release);
          },
        );
      },
    });
    let restartedOperationGuardConnects = 0;
    const restartedOperationGuardTraces = [];
    const restartedOperationPool = Object.freeze({
      connect(callback) {
        restartedOperationGuardConnects += 1;
        const trace = [];
        restartedOperationGuardTraces.push(trace);
        return operationPool.connect((error, client, release) => {
          if (error !== null && error !== undefined) {
            return callback(error, client, release);
          }
          const query = client.query;
          const tracedClient = Object.freeze({
            connection: client.connection,
            query(...args) {
              const input = args[0];
              const text =
                typeof input === "string" ? input : input?.text;
              if (text === "DISCARD ALL") {
                trace.push("discard");
              } else if (text?.includes("pg_try_advisory_lock")) {
                trace.push("acquire");
              } else if (text?.includes("FROM pg_catalog.pg_locks")) {
                trace.push("assert-held");
              } else if (text?.includes("pg_advisory_unlock")) {
                trace.push("unlock");
              } else {
                trace.push("unexpected-query");
              }
              return Reflect.apply(query, client, args);
            },
            release(...args) {
              return Reflect.apply(release, client, args);
            },
          });
          return callback(null, tracedClient, tracedClient.release);
        });
      },
    });
    const restartedSteps = [];
    const restartedRuntime =
      createPostgresDetachedRestoreRuntimeComposition({
        ...runtimeOptions,
        foreground: {
          fleetCapabilityGate() {
            restartedCalls.fleetGate += 1;
            return POSTGRES_DETACHED_RESTORE_FLEET_CONFIRMED;
          },
        },
        launch: {
          imagePlanBinding:
            createPostgresDetachedRestoreImagePlanBinding({
              provider: Object.freeze({
                contractVersion:
                  POSTGRES_DETACHED_RESTORE_IMAGE_PLAN_PROVIDER_CONTRACT_VERSION,
                imagePlanProviderId: restartedImagePlanProviderId,
                async inspectCodex(input) {
                  restartedCalls.image += 1;
                  void input;
                  throw new Error(
                    "committed restore retry must not inspect Codex",
                  );
                },
                async resolveImagePlan(input) {
                  restartedCalls.image += 1;
                  void input;
                  throw new Error(
                    "committed restore retry must not resolve an image",
                  );
                },
              }),
              settlement: integrationImagePlanSettlements(
                imagePlanProviderSettlement,
              ),
            }),
          stoppedWriterCoordinator:
            new StoppedWriterCapabilityCoordinator(),
          supervisor: restartedPhysicalBindings.supervisor,
          supervisorStateCollector:
            restartedPhysicalBindings.supervisorStateCollector,
        },
        planRegistry: {
          operationalLeaseBudget,
          provisioningFleetCapabilityGate() {
            restartedCalls.planProvisioningGate += 1;
            return POSTGRES_DETACHED_RESTORE_STABLE_PLAN_PROVISIONING_CONFIRMED;
          },
        },
        pools: {
          ...runtimeOptions.pools,
          authority: restartedAuthorityPool,
          foregroundLifecycle: restartedForegroundLifecyclePool,
          operation: restartedOperationPool,
        },
        recovery: {
          ...runtimeOptions.recovery,
          onStep(receipt) {
            restartedSteps.push(receipt);
          },
          recoveryScopeId: restartedRecoveryScopeId,
        },
        storage: {
          backendId: "postgres-authority-integration",
          lifecycleBackend: restartedPhysicalBindings.lifecycleBackend,
          publication: restartedPhysicalBindings.publication,
          resolveArtifactPaths({ checkpoint }) {
            restartedCalls.artifactResolver += 1;
            return {
              artifactDirectory: join(
                publicationTree.artifactOwnedRoot,
                checkpoint.artifactId,
              ),
              artifactOwnedRoot: publicationTree.artifactOwnedRoot,
            };
          },
          resolveRestoreDestination:
            restartedPhysicalBindings.resolveRestoreDestination,
          resolveSourceOwnedRoot({ canonicalAttachment }) {
            restartedCalls.sourceResolver += 1;
            return {
              sourceDirectory: canonicalAttachment.rootPath,
              sourceOwnedRoot: publicationTree.sourceOwnedRoot,
            };
          },
        },
      });
    assertPublicCheckpointBackendSurface(restartedRuntime.backend);
    assert.equal("foreground" in restartedRuntime, false);
    assert.equal("runRestore" in restartedRuntime, false);
    assert.notStrictEqual(restartedPhysicalBindings, physicalBindings);
    assert.notStrictEqual(restartedRuntime, runtime);
    restartedController = createPostgresDetachedRestoreRuntimeController({
      runtime: restartedRuntime,
    });
    assertPublicCheckpointBackendSurface(restartedController.backend);
    assert.equal("foreground" in restartedController, false);
    assert.equal("runRestore" in restartedController, false);
    assert.deepEqual(
      structuredClone(
        await settleWithin(
          restartedController.start(),
          "restarted restore runtime controller startup",
        ),
      ),
      { status: "ready" },
    );
    assert.equal(restartedSteps.length >= 1, true);
    assert.equal(
      restartedSteps[0].recovery.recoveryScopeId,
      effectiveRestartedRecoveryScopeId,
    );
    const expectedCollectedLaunchAttemptIds = [
      launchAttemptId,
      recoveryLaunchAttemptId,
    ].sort();
    assert.deepEqual(
      [...supervisorStateCollections].sort((left, right) =>
        left.launchAttemptId.localeCompare(right.launchAttemptId),
      ),
      expectedCollectedLaunchAttemptIds.map((collectedLaunchAttemptId) => ({
        launchAttemptId: collectedLaunchAttemptId,
        status: "collected",
      })),
    );
    assert.equal(supervisorStateCollectionInvocations.size, 2);
    assert.equal(supervisorStateCollectionSignals.size, 2);
    const completedSupervisorStateGc = await authorityPool.query(
      [
        "SELECT launch_attempt_id, terminal_kind, collection_status,",
        "collection_receipt_sha256, collected_at IS NOT NULL AS collected",
        "FROM session_authority.writer_supervisor_state_gc",
        "WHERE launch_attempt_id = ANY($1::text[])",
        "ORDER BY launch_attempt_id",
      ].join(" "),
      [[launchAttemptId, recoveryLaunchAttemptId]],
    );
    assert.deepEqual(
      completedSupervisorStateGc.rows.map((row) => ({
        collected: row.collected,
        collection_status: row.collection_status,
        launch_attempt_id: row.launch_attempt_id,
        terminal_kind: row.terminal_kind,
      })),
      expectedCollectedLaunchAttemptIds.map((collectedLaunchAttemptId) => ({
        collected: true,
        collection_status: "collected",
        launch_attempt_id: collectedLaunchAttemptId,
        terminal_kind: WRITER_LAUNCH_STOP_OPERATION_KIND,
      })),
    );
    for (const row of completedSupervisorStateGc.rows) {
      assert.match(row.collection_receipt_sha256, /^[0-9a-f]{64}$/u);
    }

    const captureRecoveryCountsBefore = {
      artifactResolver: restartedCalls.artifactResolver,
      publication: restartedCalls.publication,
      verifyCommittedCheckpointArtifact:
        restartedCalls.verifyCommittedCheckpointArtifact,
    };
    const captureRecoveryPhysicalInvocationsBefore =
      restartedPhysicalInvocations.size;
    const captureRecoveryPhysicalSignalsBefore =
      restartedPhysicalSignals.size;
    const captureRecoverySourceResolverBefore =
      restartedCalls.sourceResolver;
    const captureRecoveryPublicationBefore =
      restartedCalls.publishFreshCheckpointArtifact;
    await assert.rejects(
      restartedController.backend.restoreCheckpoint(recoveryAdmission),
      (error) => {
        assert.equal(
          error.code,
          "stopped_directory_backend_outcome_uncertain",
        );
        return true;
      },
    );
    assert.equal(
      restartedCalls.artifactResolver,
      captureRecoveryCountsBefore.artifactResolver + 1,
    );
    assert.equal(
      restartedCalls.publication > captureRecoveryCountsBefore.publication,
      true,
    );
    assert.equal(
      restartedCalls.verifyCommittedCheckpointArtifact,
      captureRecoveryCountsBefore.verifyCommittedCheckpointArtifact + 1,
    );
    assert.equal(
      restartedCalls.publishFreshCheckpointArtifact,
      captureRecoveryPublicationBefore,
    );
    assert.equal(
      restartedCalls.sourceResolver,
      captureRecoverySourceResolverBefore,
    );
    assert.equal(
      restartedPhysicalInvocations.size,
      captureRecoveryPhysicalInvocationsBefore,
    );
    assert.equal(
      restartedPhysicalSignals.size,
      captureRecoveryPhysicalSignalsBefore,
    );
    for (const field of [
      "destinationResolver",
      "fleetGate",
      "image",
      "planProvisioningGate",
      "provider",
      "publishRestoreDestination",
      "supervisorLaunch",
      "supervisorReconcile",
      "verifyCommittedRestoreDestination",
    ]) {
      assert.equal(restartedCalls[field], 0);
    }
    const recoveryCaptureAfterRestart = await authorityPool.query(
      [
        "SELECT kind, state, revision::text AS revision",
        "FROM session_authority.operation_claims",
        "WHERE operation_id = $1",
      ].join(" "),
      [recoveryStablePlan.captureOperationId],
    );
    assert.deepEqual(recoveryCaptureAfterRestart.rows, [
      {
        kind: CHECKPOINT_CAPTURE_OPERATION_KIND,
        revision: "2",
        state: "uncertain",
      },
    ]);
    publicationObservation.captureFailureArmed = false;

    await rm(publicationTree.artifactOwnedRoot, {
      force: true,
      recursive: true,
    });
    await assert.rejects(
      lstat(publicationTree.artifactOwnedRoot),
      (error) => error?.code === "ENOENT",
    );

    const detachBeforeRetry = await authorityPool.query(
      [
        "SELECT operation_id",
        "FROM session_authority.operation_claims",
        "WHERE operation_id = $1",
      ].join(" "),
      [stablePlan.detachOperationId],
    );
    assert.deepEqual(detachBeforeRetry.rows, []);
    const activationAndLaunchBeforeRetry = await authorityPool.query(
      [
        "SELECT operation_id, kind",
        "FROM session_authority.operation_claims",
        "WHERE operation_id = ANY($1::text[])",
        "ORDER BY operation_id",
      ].join(" "),
      [[stablePlan.activationOperationId, stablePlan.launchAttemptId]],
    );
    assert.deepEqual(activationAndLaunchBeforeRetry.rows, []);
    const sessionBeforeRestoreRetry = await authorityPool.query(
      [
        "SELECT document->>'lifecycle' AS lifecycle,",
        "document->'activeOperation' AS active_operation,",
        "document->'lastOperation'->>'kind' AS last_kind,",
        "document->'lastOperation'->>'operationId' AS last_operation_id,",
        "document->'lastOperation'->>'state' AS last_state,",
        "document->'lease' <> 'null'::jsonb AS has_lease,",
        "document->'attachment' <> 'null'::jsonb AS has_attachment",
        "FROM session_authority.sessions",
        "WHERE session_id = $1",
      ].join(" "),
      [sessionId],
    );
    assert.deepEqual(sessionBeforeRestoreRetry.rows, [
      {
        active_operation: null,
        has_attachment: true,
        has_lease: true,
        last_kind: RESTORE_DESTINATION_GENERATION_OPERATION_KIND,
        last_operation_id: admission.request.operationId,
        last_state: "committed",
        lifecycle: "ATTACHED",
      },
    ]);
    const directGenerationRead =
      await authority.readRestoreDestinationGeneration({
        checkpoint: admission.checkpoint,
        generationId: stablePlan.generationId,
        request: admission.request,
      });
    assert.equal(directGenerationRead.status, "committed");
    assert.equal(directGenerationRead.operation.state, "committed");
    assert.equal(directGenerationRead.reservation.state, "released");
    assertSessionOperationTransitionProof({
      operation: directGenerationRead.operation,
      reservation: directGenerationRead.reservation,
      session: directGenerationRead.session,
    });
    const directGenerationLast =
      directGenerationRead.session.document.lastOperation;
    assert.deepEqual(
      {
        expectedSessionRevision:
          directGenerationLast.expectedSessionRevision,
        operationId: directGenerationLast.operationId,
        operationRevision: directGenerationLast.operationRevision,
        requestSha256: directGenerationLast.requestSha256,
        reservationId: directGenerationLast.reservationId,
        state: directGenerationLast.state,
      },
      {
        expectedSessionRevision:
          directGenerationRead.reservation.expectedSessionRevision,
        operationId: admission.request.operationId,
        operationRevision: directGenerationRead.operation.revision,
        requestSha256: directGenerationRead.operation.requestSha256,
        reservationId: directGenerationRead.reservation.reservationId,
        state: "committed",
      },
    );
    assert.equal(directGenerationRead.session.document.launch, null);
    assert.deepEqual(
      structuredClone(
        directGenerationRead.generation.binding.attachment,
      ),
      structuredClone(
        directGenerationRead.session.document.attachment,
      ),
    );
    assert.notEqual(publicationObservation.restoreVerificationInput, null);
    // Prove the complete raw verifier succeeds after the artifact source is
    // gone before attributing any later failure to the assembled facade.
    const directVerificationRequest = frozenNullPrototypeRecord({
        ...publicationObservation.restoreVerificationInput,
        destinationDirectory: publicationTree.destinationDirectory,
        destinationOwnedRoot: publicationTree.destinationOwnedRoot,
      });
    const directVerification =
      await restartedPublication.verifyCommittedRestoreDestination(
        directVerificationRequest,
      );
    assert.equal(directVerification.replayed, true);
    // Prove the deployment-owned settlement wrapper accepts and returns the
    // same committed verification before exercising the private router.
    const directBoundVerification =
      await restartedPhysicalBindings.publication
        .verifyCommittedRestoreDestination(directVerificationRequest);
    assert.deepEqual(
      structuredClone(directBoundVerification),
      structuredClone(directVerification),
    );
    assert.deepEqual(
      structuredClone(directBoundVerification.materialization),
      committedGenerationBeforeRestart.rows[0].document.materialization,
    );
    assert.deepEqual(
      structuredClone(directBoundVerification.result),
      committedGenerationBeforeRestart.rows[0].document.result,
    );
    assert.deepEqual(
      structuredClone(directBoundVerification.result),
      structuredClone(
        directGenerationRead.operation.request.predeterminedResult,
      ),
    );
    let directDetachGuardConnects = 0;
    const directDetachProbe = createPostgresWriterDetachComposition({
      authority,
      operationGuard: new PostgresOperationGuard({
        dedicatedPool: Object.freeze({
          connect(callback) {
            directDetachGuardConnects += 1;
            callback(
              new Error("synthetic direct detach guard failure"),
              null,
              undefined,
            );
            return undefined;
          },
        }),
      }),
      storageBackend: restartedPhysicalBindings.lifecycleBackend,
    });
    await assert.rejects(
      directDetachProbe.detachWriter(
        frozenNullPrototypeRecord({
          expectedSession: directGenerationRead.session,
          operationId: stablePlan.detachOperationId,
          target: frozenNullPrototypeRecord({
            attachmentId:
              directGenerationRead.generation.binding.attachment
                .attachmentId,
            kind: "attachment",
          }),
        }),
      ),
      (error) => {
        assert.equal(
          error.code,
          "postgres_writer_detach_composition_outcome_uncertain",
        );
        return true;
      },
    );
    assert.equal(directDetachGuardConnects, 1);

    // This boundary proves complete in-process object replacement over the
    // same PostgreSQL and stopped-directory journal state.
    publicationObservation.events.length = 0;
    publicationObservation.ownedRootInspections.length = 0;
    const firstRuntimeCountsBeforeRestart = {
      publication: calls.publication,
      publishRestoreDestination: calls.publishRestoreDestination,
      sourceResolver: calls.sourceResolver,
      supervisorStop: calls.supervisorStop,
      verifyCommittedRestoreDestination:
        calls.verifyCommittedRestoreDestination,
    };
    const restoreRetryArtifactResolverBefore =
      restartedCalls.artifactResolver;
    const restoreRetryPublicationBefore = restartedCalls.publication;
    const restoreRetryVerificationBefore =
      restartedCalls.verifyCommittedRestoreDestination;
    const restoreRetryPhysicalInvocationsBefore =
      restartedPhysicalInvocations.size;
    const restoreRetryPhysicalSignalsBefore =
      restartedPhysicalSignals.size;
    const restoreRetryOperationGuardConnectsBefore =
      restartedOperationGuardConnects;
    const restoreRetryOperationGuardTracesBefore =
      restartedOperationGuardTraces.length;
    const restoreRetryForegroundLifecycleTracesBefore =
      restartedForegroundLifecycleTraces.length;
    const restoreRetryAuthorityReadsBefore = {
      ...restartedAuthorityReads,
    };
    await assert.rejects(
      restartedController.backend.restoreCheckpoint(admission),
      (error) => {
        assert.equal(
          error.code,
          "stopped_directory_backend_outcome_uncertain",
        );
        return true;
      },
    );
    const detachAfterRetry = await authorityPool.query(
      [
        "SELECT o.operation_id, o.kind, o.state,",
        "o.revision::text AS revision,",
        "o.result->>'outcome' AS outcome,",
        "o.result->>'reason' AS reason,",
        "r.state AS reservation_state,",
        "r.released_at IS NOT NULL AS reservation_released",
        "FROM session_authority.operation_claims AS o",
        "LEFT JOIN session_authority.reservations AS r",
        "ON r.operation_id = o.operation_id",
        "AND r.session_id = o.session_id",
        "WHERE o.operation_id = $1",
      ].join(" "),
      [stablePlan.detachOperationId],
    );
    assert.deepEqual(
      {
        detach: detachAfterRetry.rows,
        events: publicationObservation.events,
        authorityReads: {
          activation:
            restartedAuthorityReads.activation -
            restoreRetryAuthorityReadsBefore.activation,
          generation:
            restartedAuthorityReads.generation -
            restoreRetryAuthorityReadsBefore.generation,
          launch:
            restartedAuthorityReads.launch -
            restoreRetryAuthorityReadsBefore.launch,
        },
        generationGuardTrace:
          restartedOperationGuardTraces[
            restoreRetryOperationGuardTracesBefore
          ],
        foregroundLifecycleTrace:
          restartedForegroundLifecycleTraces[
            restoreRetryForegroundLifecycleTracesBefore
          ],
        physicalInvocations:
          restartedPhysicalInvocations.size -
          restoreRetryPhysicalInvocationsBefore,
        physicalSignals:
          restartedPhysicalSignals.size - restoreRetryPhysicalSignalsBefore,
        operationGuardConnects:
          restartedOperationGuardConnects -
          restoreRetryOperationGuardConnectsBefore,
        provider: restartedCalls.provider,
        publicationAdvanced:
          restartedCalls.publication > restoreRetryPublicationBefore,
        verification:
          restartedCalls.verifyCommittedRestoreDestination -
          restoreRetryVerificationBefore,
      },
      {
        authorityReads: {
          activation: 1,
          generation: 2,
          launch: 1,
        },
        detach: [
          {
            kind: WRITER_RELEASE_OPERATION_KIND,
            operation_id: stablePlan.detachOperationId,
            outcome: "writer-blocked",
            reason: "provider-outcome-unresolved",
            reservation_released: true,
            reservation_state: "released",
            revision: "3",
            state: "committed",
          },
        ],
        events: ["verifyCommittedRestoreDestination"],
        foregroundLifecycleTrace: [
          "discard",
          "acquire",
          "assert-held",
          "assert-held",
          "assert-held",
          "assert-held",
          "assert-held",
          "assert-held",
          "assert-held",
          "unlock",
          "discard",
        ],
        generationGuardTrace: [
          "discard",
          "acquire",
          "assert-held",
          "assert-held",
          "assert-held",
          "assert-held",
          "unlock",
          "discard",
        ],
        operationGuardConnects: 2,
        physicalInvocations: 1,
        physicalSignals: 1,
        provider: 1,
        publicationAdvanced: true,
        verification: 1,
      },
    );
    assert.deepEqual(
      {
        publication: calls.publication,
        publishRestoreDestination: calls.publishRestoreDestination,
        sourceResolver: calls.sourceResolver,
        supervisorStop: calls.supervisorStop,
        verifyCommittedRestoreDestination:
          calls.verifyCommittedRestoreDestination,
      },
      firstRuntimeCountsBeforeRestart,
    );
    assert.equal(
      restartedCalls.artifactResolver,
      restoreRetryArtifactResolverBefore,
    );
    assert.equal(
      restartedCalls.publication > restoreRetryPublicationBefore,
      true,
    );
    for (const field of [
      "destinationResolver",
      "fleetGate",
      "image",
      "planProvisioningGate",
      "publishRestoreDestination",
      "sourceResolver",
      "supervisorLaunch",
      "supervisorReconcile",
    ]) {
      assert.equal(restartedCalls[field], 0);
    }
    // Verification authorizes progress to the distinct detach operation; it
    // never authorizes a second destination-publication mutation.
    assert.equal(restartedCalls.provider, 1);
    assert.equal(
      restartedCalls.verifyCommittedRestoreDestination,
      restoreRetryVerificationBefore + 1,
    );
    assert.deepEqual(publicationObservation.events, [
      "verifyCommittedRestoreDestination",
    ]);
    assert.equal(
      publicationObservation.ownedRootInspections.includes(
        publicationTree.destinationOwnedRoot,
      ),
      true,
    );
    assert.equal(
      publicationObservation.ownedRootInspections.includes(
        publicationTree.artifactOwnedRoot,
      ),
      false,
    );
    assert.equal(
      publicationObservation.ownedRootInspections.includes(
        publicationTree.sourceOwnedRoot,
      ),
      false,
    );
    assert.equal(
      restartedPhysicalInvocations.size,
      restoreRetryPhysicalInvocationsBefore + 1,
    );
    assert.equal(
      restartedPhysicalSignals.size,
      restoreRetryPhysicalSignalsBefore + 1,
    );
    const committedGenerationAfterRestart = await authorityPool.query(
      [
        "SELECT g.state, g.document, g.committed_at,",
        "o.state AS operation_state,",
        "o.revision::text AS operation_revision, o.result",
        "FROM session_authority.restore_destination_generations AS g",
        "JOIN session_authority.operation_claims AS o",
        "ON o.operation_id = g.operation_id",
        "WHERE g.operation_id = $1",
      ].join(" "),
      [admission.request.operationId],
    );
    assert.deepEqual(
      committedGenerationAfterRestart.rows,
      committedGenerationBeforeRestart.rows,
    );
    const destinationAfterRestart = await lstat(
      publicationTree.destinationDirectory,
      { bigint: true },
    );
    assert.equal(destinationAfterRestart.dev, destinationBeforeRestart.dev);
    assert.equal(destinationAfterRestart.ino, destinationBeforeRestart.ino);
    assert.equal(
      destinationAfterRestart.birthtimeNs,
      destinationBeforeRestart.birthtimeNs,
    );
    assert.deepEqual(
      structuredClone(
        await settleWithin(
          restartedController.stop(),
          "restarted restore runtime controller shutdown",
        ),
      ),
      { status: "stopped" },
    );
  },
);

test(
  "PostgreSQL detached restore deployment owns topology startup, image-plan preparation, drain, and pool shutdown",
  { timeout: 60_000 },
  async (t) => {
    const connection = explicitPostgresConnectionFromDatabaseUrl(databaseUrl);
    const applicationNamePrefix = `pcrd-${randomUUID().slice(0, 8)}`;
    const applicationNames = [
      `${applicationNamePrefix}:authority`,
      `${applicationNamePrefix}:foreground-lifecycle`,
      `${applicationNamePrefix}:operation`,
      `${applicationNamePrefix}:recovery-lifecycle`,
    ].sort();
    const inspectionPool = new Pool({
      application_name: `${applicationNamePrefix}:inspection`,
      connectionString: databaseUrl,
      max: 1,
    });
    const recoveryScopeId = `integration-deployment-${randomUUID()}`;
    const sessionId = randomUUID();
    const provisioningEntered = deferred();
    const releaseProvisioning = deferred();
    const deploymentImage = integrationPlatformImageFixture();
    const imagePlanProviderId =
      `deployment-image-provider-${randomUUID()}`;
    const imageProviderInvocations = new Set();
    const imageProviderSignals = new Set();
    let deploymentPlan = null;
    let deploymentSession = null;
    let holdProvisioning = false;
    const steps = [];
    const calls = {
      fleetGate: 0,
      image: 0,
      imageInspect: 0,
      imageResolve: 0,
      planGate: 0,
      provider: 0,
      publication: 0,
      supervisor: 0,
    };
    const physicalPolicies =
      integrationDeploymentPhysicalSettlementPolicies();
    const deploymentSupervisorId =
      `deployment-supervisor-${randomUUID()}`;
    const deploymentSupervisorStateParent = await mkdtemp(
      join(
        await realpath(tmpdir()),
        "portable-codex-runtime-deployment-supervisor-",
      ),
    );
    const deploymentSupervisorStateParentIdentity = await lstat(
      deploymentSupervisorStateParent,
      { bigint: true },
    );
    t.after(async () => {
      // Cleanup protects the private parent's object identity. Child-entry
      // churn is expected state content, so ctime/size are deliberately not
      // treated as replacement evidence.
      const currentIdentity = await lstat(
        deploymentSupervisorStateParent,
        { bigint: true },
      );
      assert.equal(
        currentIdentity.dev,
        deploymentSupervisorStateParentIdentity.dev,
      );
      assert.equal(
        currentIdentity.ino,
        deploymentSupervisorStateParentIdentity.ino,
      );
      assert.equal(
        currentIdentity.birthtimeNs,
        deploymentSupervisorStateParentIdentity.birthtimeNs,
      );
      await rm(deploymentSupervisorStateParent, {
        force: true,
        recursive: true,
      });
    });
    const deploymentSupervisorStateRoot = join(
      deploymentSupervisorStateParent,
      "state",
    );
    const initialDeploymentStateOwner =
      await preparePodmanWriterSupervisorStateOwner({
        expectedStateOwnerId: null,
        root: deploymentSupervisorStateRoot,
      });
    const initialDeploymentStateBundle =
      createPodmanWriterSupervisorStateBundle({
        owner: initialDeploymentStateOwner,
      });
    const createDeploymentSupervisorBundle = (stateBundle) =>
      createPodmanWriterSupervisorBundle({
        configuredAttachmentRoot: deploymentSupervisorStateParent,
        images: Object.freeze({
          [deploymentImage.descriptor.digest]: Object.freeze({
            architecture: "arm64",
            codexVersion: "0.142.4",
            imageReference:
              `localhost/portable-codex@${deploymentImage.descriptor.digest}`,
            os: "linux",
          }),
        }),
        podmanEnvironment: Object.freeze({
          HOME: "/var/empty/podman",
          XDG_RUNTIME_DIR: "/run/user/1000",
        }),
        podmanExecutable: "/usr/bin/podman",
        stateBundle,
        supervisorId: deploymentSupervisorId,
        writerCommand: Object.freeze([
          "/usr/local/bin/codex",
          "app-server",
        ]),
        writerEnvironment: Object.freeze({
          CODEX_HOME: "/session/.codex",
          LANG: "C.UTF-8",
        }),
      });
    const initialDeploymentSupervisorBundle =
      createDeploymentSupervisorBundle(initialDeploymentStateBundle);
    const restartedDeploymentStateOwner =
      await preparePodmanWriterSupervisorStateOwner({
        expectedStateOwnerId: initialDeploymentStateBundle.stateOwnerId,
        root: deploymentSupervisorStateRoot,
      });
    const restartedDeploymentStateBundle =
      createPodmanWriterSupervisorStateBundle({
        owner: restartedDeploymentStateOwner,
      });
    const deploymentSupervisorBundle =
      createDeploymentSupervisorBundle(restartedDeploymentStateBundle);
    const deploymentStateOwnerId =
      deploymentSupervisorBundle.supervisor.stateOwnerId;
    assert.equal(
      deploymentStateOwnerId,
      initialDeploymentSupervisorBundle.supervisor.stateOwnerId,
    );
    assert.notStrictEqual(
      deploymentSupervisorBundle.supervisor,
      initialDeploymentSupervisorBundle.supervisor,
    );
    const deploymentEffectiveRecoveryScopeId = recoveryOwnerScopeId(
      recoveryScopeId,
      deploymentStateOwnerId,
    );
    const deployment = createPostgresDetachedRestoreDeployment({
      postgres: {
        applicationNamePrefix,
        database: connection.database,
        host: connection.host,
        password: connection.password,
        poolMaximums: {
          authority: 2,
          foregroundLifecycle: 1,
          operation: 1,
          recoveryLifecycle: 1,
        },
        port: connection.port,
        timeouts: {
          connectionMilliseconds: 10_000,
          idleClientMilliseconds: 120_000,
          idleTransactionMilliseconds: 10_000,
          lockMilliseconds: 10_000,
          queryMilliseconds: 10_000,
          statementMilliseconds: 10_000,
        },
        tls: {
          ca: null,
          cert: null,
          key: null,
          mode: "disable",
          serverName: null,
        },
        user: connection.user,
      },
      runtime: {
        authority: {
          maxTransactionAttempts: 3,
          restoreAttachmentActivationV2FleetCompatible: true,
          restoreAttachmentActivationV2GenerationPredecessorFleetCompatible:
            true,
          restoreGenerationV2FleetCompatible: true,
          writerLaunchStopV3FleetCompatible: true,
        },
        foreground: {
          fleetCapabilityGate() {
            calls.fleetGate += 1;
            return null;
          },
        },
        launch: {
          imagePlanProvider: Object.freeze({
            contractVersion:
              POSTGRES_DETACHED_RESTORE_IMAGE_PLAN_PROVIDER_CONTRACT_VERSION,
            imagePlanProviderId,
            async inspectCodex(input) {
              calls.image += 1;
              calls.imageInspect += 1;
              assert.notEqual(deploymentPlan, null);
              assert.notEqual(deploymentSession, null);
              assert.equal(Object.getPrototypeOf(input), null);
              assert.equal(Object.isFrozen(input), true);
              assert.deepEqual(Reflect.ownKeys(input).sort(), [
                "imagePlanId",
                "imagePlanProviderId",
                "inspection",
                "invocation",
                "signal",
              ]);
              assertFreshOpaqueInvocation(
                input.invocation,
                imageProviderInvocations,
              );
              assert.equal(input.signal instanceof AbortSignal, true);
              assert.equal(input.signal.aborted, false);
              assert.equal(imageProviderSignals.has(input.signal), false);
              imageProviderSignals.add(input.signal);
              assert.equal(input.imagePlanId, deploymentPlan.imagePlanId);
              assert.equal(input.imagePlanProviderId, imagePlanProviderId);
              assert.equal(
                Object.getPrototypeOf(input.inspection),
                Object.prototype,
              );
              assert.equal(Object.isFrozen(input.inspection), true);
              assert.deepEqual(Reflect.ownKeys(input.inspection).sort(), [
                "codexSandbox",
                "codexVersion",
                "platformImage",
              ]);
              assert.equal(
                input.inspection.codexVersion,
                deploymentSession.document.manifest.runtime.codexVersion,
              );
              assert.equal(
                input.inspection.platformImage.digest,
                deploymentImage.descriptor.digest,
              );
              return frozenNullPrototypeRecord({
                codexBinaryPath: "/opt/portable-codex/bin/codex",
                codexBinarySha256: "c".repeat(64),
                codexVersion:
                  deploymentSession.document.manifest.runtime.codexVersion,
              });
            },
            async resolveImagePlan(input) {
              calls.image += 1;
              calls.imageResolve += 1;
              assert.notEqual(deploymentPlan, null);
              assert.notEqual(deploymentSession, null);
              assert.equal(Object.getPrototypeOf(input), null);
              assert.equal(Object.isFrozen(input), true);
              assert.deepEqual(Reflect.ownKeys(input).sort(), [
                "imagePlanId",
                "imagePlanProviderId",
                "invocation",
                "sessionManifest",
                "signal",
              ]);
              assertFreshOpaqueInvocation(
                input.invocation,
                imageProviderInvocations,
              );
              assert.equal(input.signal instanceof AbortSignal, true);
              assert.equal(input.signal.aborted, false);
              assert.equal(imageProviderSignals.has(input.signal), false);
              imageProviderSignals.add(input.signal);
              assert.equal(input.imagePlanId, deploymentPlan.imagePlanId);
              assert.equal(input.imagePlanProviderId, imagePlanProviderId);
              assert.deepEqual(
                input.sessionManifest,
                deploymentSession.document.manifest,
              );
              return frozenNullPrototypeRecord({
                configBytes: deploymentImage.configBytes,
                descriptor: Object.freeze({
                  ...deploymentImage.descriptor,
                }),
              });
            },
          }),
          imagePlanProviderSettlement: {
            inspectCodex: {
              deadlineMilliseconds: 30_000,
              settlementGraceMilliseconds: 5_000,
            },
            resolveImagePlan: {
              deadlineMilliseconds: 45_000,
              settlementGraceMilliseconds: 10_000,
            },
          },
          stoppedWriterCoordinator:
            new StoppedWriterCapabilityCoordinator(),
          supervisor: deploymentSupervisorBundle.supervisor,
          supervisorSettlement: physicalPolicies.supervisorSettlement,
          supervisorStateCollectionSettlement:
            physicalPolicies.supervisorStateCollectionSettlement,
          supervisorStateCollector:
            deploymentSupervisorBundle.supervisorStateCollector,
        },
        operationalLease: {
          databaseRequestMilliseconds:
            OPERATIONAL_LEASE_DATABASE_REQUEST_MILLISECONDS,
          leaseDurationMilliseconds:
            OPERATIONAL_LEASE_DURATION_MILLISECONDS,
          safetyMarginMilliseconds:
            OPERATIONAL_LEASE_SAFETY_MARGIN_MILLISECONDS,
        },
        planRegistry: {
          async provisioningFleetCapabilityGate() {
            calls.planGate += 1;
            if (holdProvisioning) {
              provisioningEntered.resolve();
              await releaseProvisioning.promise;
            }
            return POSTGRES_DETACHED_RESTORE_STABLE_PLAN_PROVISIONING_CONFIRMED;
          },
        },
        recovery: {
          intervalMilliseconds: 60_000,
          limits: {
            activation: 10,
            currentLaunch: 10,
            generation: 10,
            launchAttempt: 10,
            supervisorStateGc: 10,
          },
          onStep(receipt) {
            steps.push(receipt);
          },
          recoveryScopeId,
        },
        storage: {
          backendId: "postgres-authority-integration",
          lifecycleBackend:
            restoreRuntimeIntegrationLifecycleBackend(calls),
          lifecycleBackendSettlement:
            physicalPolicies.lifecycleBackendSettlement,
          publication: restoreRuntimeIntegrationPublication(
            calls,
            recoveryScopeId,
          ),
          publicationSettlement: physicalPolicies.publicationSettlement,
          resolveArtifactPaths: integrationArtifactPaths,
          async resolveRestoreDestination(input) {
            void input;
            throw new Error("deployment destination must not resolve");
          },
          resolveRestoreDestinationContractVersion: 1,
          resolveRestoreDestinationSettlement:
            physicalPolicies.resolveRestoreDestinationSettlement,
          resolveSourceOwnedRoot: integrationSourceOwnedRoot,
        },
      },
    });

    t.after(async () => {
      await Promise.allSettled([deployment.stop()]);
      try {
        const cleanupClient = await inspectionPool.connect();
        try {
          await cleanupClient.query("BEGIN");
          await cleanupClient.query(
            [
              "DELETE FROM session_authority.restore_recovery_cursors",
              "WHERE recovery_scope_id = $1",
            ].join(" "),
            [deploymentEffectiveRecoveryScopeId],
          );
          await cleanupClient.query(
            [
              "DELETE FROM session_authority.detached_restore_stable_plans",
              "WHERE session_id = $1::uuid",
            ].join(" "),
            [sessionId],
          );
          await cleanupClient.query(
            [
              "DELETE FROM session_authority.writer_supervisor_state_gc",
              "WHERE session_id = $1::uuid",
            ].join(" "),
            [sessionId],
          );
          await cleanupClient.query(
            [
              "DELETE FROM session_authority.writer_supervisor_state_owners",
              "WHERE session_id = $1::uuid",
            ].join(" "),
            [sessionId],
          );
          await cleanupClient.query(
            [
              "DELETE FROM session_authority.reservations",
              "WHERE session_id = $1::uuid",
            ].join(" "),
            [sessionId],
          );
          await cleanupClient.query(
            [
              "DELETE FROM session_authority.operation_claims",
              "WHERE session_id = $1::uuid",
            ].join(" "),
            [sessionId],
          );
          await cleanupClient.query(
            [
              "DELETE FROM session_authority.operation_id_registry",
              "WHERE session_id = $1::uuid",
            ].join(" "),
            [sessionId],
          );
          await cleanupClient.query(
            [
              "DELETE FROM session_authority.sessions",
              "WHERE session_id = $1::uuid",
            ].join(" "),
            [sessionId],
          );
          await cleanupClient.query("COMMIT");
        } catch (error) {
          try {
            await cleanupClient.query("ROLLBACK");
          } catch {
            // Pool shutdown below destroys any connection that cannot reset.
          }
          throw error;
        } finally {
          cleanupClient.release();
        }
      } finally {
        await inspectionPool.end();
      }
    });

    assert.throws(
      () => deployment.imagePlanReservations.prepareImageReservation({}),
      (error) => {
        assert.equal(
          error.code,
          "invalid_postgres_detached_restore_deployment_request",
        );
        return true;
      },
    );
    assert.deepEqual(
      structuredClone(
        await settleWithin(deployment.start(), "deployment startup"),
      ),
      { status: "ready" },
    );
    assert.equal(steps.length >= 1, true);
    assert.equal(steps[0].status, "completed");
    assert.equal(steps[0].recovery.status, "sweep-complete");
    assert.equal(
      steps[0].recovery.recoveryScopeId,
      deploymentEffectiveRecoveryScopeId,
    );

    const ledger = await readMigrationLedger(inspectionPool);
    assert.deepEqual(
      ledger.map(({ version }) => version),
      AUTHORITY_MIGRATIONS.map(({ version }) => version),
    );
    const cursors = await inspectionPool.query(
      [
        "SELECT lane, cycle::text AS cycle, revision::text AS revision",
        "FROM session_authority.restore_recovery_cursors",
        "WHERE recovery_scope_id = $1",
        "ORDER BY lane",
      ].join(" "),
      [deploymentEffectiveRecoveryScopeId],
    );
    assert.deepEqual(cursors.rows, [
      { cycle: "1", lane: "activation", revision: "1" },
      { cycle: "1", lane: "current-launch", revision: "1" },
      { cycle: "1", lane: "generation", revision: "1" },
      { cycle: "1", lane: "launch-attempt", revision: "1" },
      { cycle: "1", lane: "supervisor-state-gc", revision: "1" },
    ]);
    const activeSessions = await waitForDeploymentApplicationSessions(
      inspectionPool,
      applicationNames,
      4,
    );
    assert.deepEqual(
      activeSessions.map(({ application_name: name }) => name),
      applicationNames,
    );

    const advertisedBackendCapabilities = {
      ...deployment.backend.capabilities,
    };
    assert.deepEqual(advertisedBackendCapabilities, {
      atomicPointInTimeCheckpoint: true,
      exclusiveWriterAttachment: true,
      fencing: "epoch-enforced",
      normalDirectoryAttachment: true,
    });
    const registration = {
      ...registrationInput(sessionId, {
        imageDigest: deploymentImage.descriptor.digest,
      }),
      backendCapabilities: advertisedBackendCapabilities,
    };
    const inspectionAuthority = new PostgresSessionAuthority({
      restoreAttachmentActivationV2FleetCompatible: true,
      restoreAttachmentActivationV2GenerationPredecessorFleetCompatible:
        true,
      restoreGenerationV2FleetCompatible: true,
      store: new PostgresSerializableStore({
        dedicatedPool: inspectionPool,
        maxTransactionAttempts: 3,
      }),
      writerLaunchStopV3FleetCompatible: true,
    });
    const registered = await inspectionAuthority.registerSession(registration);
    assert.equal(registered.sessionId, sessionId);
    const attached = await attachWriter(inspectionAuthority, registered);
    assert.equal(attached.session.document.lifecycle, "ATTACHED");
    deploymentSession = attached.session;
    const artifactId = `deployment-artifact-${randomUUID()}`;
    const checkpointId = `deployment-checkpoint-${randomUUID()}`;
    const admission = {
      checkpoint: {
        artifactId,
        backendId: registration.storageRef.backendId,
        checkpointClass: "clean",
        checkpointId,
        codexSessionId: registration.manifest.codex.sessionId,
        codexThreadId: registration.manifest.codex.rootThreadId,
        contractVersion: 1,
        createdAt: new Date().toISOString(),
        imageDigest: registration.manifest.runtime.imageDigest,
        sessionId,
        sourceFencingEpoch: "1",
        storageId: registration.storageRef.storageId,
      },
      request: {
        backendId: registration.storageRef.backendId,
        contractVersion: 1,
        fencingEpoch: "2",
        holderId: `deployment-holder-${randomUUID()}`,
        leaseId: `deployment-lease-${randomUUID()}`,
        operation: "restore",
        operationId: `deployment-restore-${randomUUID()}`,
        sessionId,
        storageId: registration.storageRef.storageId,
        target: {
          artifactId,
          checkpointId,
          kind: "checkpoint",
        },
      },
    };
    const createDeploymentPlan = (leaseDurationMilliseconds) =>
      createPostgresDetachedRestorePlan({
        request: admission.request,
        plan: {
          captureCreatedAt: new Date().toISOString(),
          destinationDirectory:
            `/var/lib/portable-codex-restores/${sessionId}`,
          destinationOwnedRoot: "/var/lib/portable-codex-restores",
          detachMode: "release",
          holderId: `deployment-restore-holder-${randomUUID()}`,
          imagePlanId: `deployment-image-plan-${randomUUID()}`,
          leaseDurationMilliseconds,
          sourceArtifactDirectory:
            `/var/lib/portable-codex-checkpoints/${artifactId}`,
          sourceArtifactOwnedRoot: "/var/lib/portable-codex-checkpoints",
        },
      });
    const beforeTooShortPlan =
      await readSessionAuthorityMutationSnapshot(inspectionPool, sessionId);
    await assert.rejects(
      deployment.stablePlanProvisioning.provisionStablePlan({
        admission,
        plan: createDeploymentPlan(
          OPERATIONAL_LEASE_DURATION_MILLISECONDS - 1,
        ),
      }),
      (error) => {
        assert(
          error instanceof PostgresDetachedRestoreStablePlanRegistryError,
        );
        assert.equal(
          error.code,
          "postgres_detached_restore_stable_plan_registry_operational_lease_required",
        );
        return true;
      },
    );
    assert.deepEqual(
      await readSessionAuthorityMutationSnapshot(inspectionPool, sessionId),
      beforeTooShortPlan,
    );
    assert.deepEqual(calls, {
      fleetGate: 0,
      image: 0,
      imageInspect: 0,
      imageResolve: 0,
      planGate: 0,
      provider: 0,
      publication: 0,
      supervisor: 0,
    });

    const plan = createDeploymentPlan(
      OPERATIONAL_LEASE_DURATION_MILLISECONDS,
    );
    deploymentPlan = plan;
    const provisioned =
      await deployment.stablePlanProvisioning.provisionStablePlan({
        admission,
        plan,
      });
    assert.notStrictEqual(provisioned, plan);
    assert.equal(provisioned.planSha256, plan.planSha256);
    assert.equal(provisioned.imagePlanId, plan.imagePlanId);
    const imageReservation =
      await deployment.imagePlanReservations.prepareImageReservation({
        plan: provisioned,
        sessionManifest: attached.session.document.manifest,
      });
    assert.equal(
      isPostgresDetachedRestoreImagePlanReservation(imageReservation),
      true,
    );
    assert.equal(Object.getPrototypeOf(imageReservation), null);
    assert.equal(Object.isFrozen(imageReservation), true);
    assert.deepEqual(Reflect.ownKeys(imageReservation), []);
    assert.equal(calls.image, 2);
    assert.equal(calls.imageInspect, 1);
    assert.equal(calls.imageResolve, 1);
    assert.equal(imageProviderInvocations.size, 2);
    assert.equal(imageProviderSignals.size, 2);
    assert.equal(calls.provider, 0);
    assert.equal(calls.publication, 0);
    assert.equal(calls.supervisor, 0);
    assertPublicCheckpointBackendSurface(deployment.backend);
    assert.equal("foreground" in deployment, false);
    assert.equal("runRestore" in deployment, false);
    await assert.rejects(
      deployment.writerLaunch.runLaunch({
        generation: {
          binding: null,
          checkpointId,
          claimedAt: new Date().toISOString(),
          committedAt: new Date().toISOString(),
          document: {},
          generationId: plan.generationId,
          operationId: plan.request.operationId,
          sessionId,
          state: "committed",
        },
        imageReservation,
        launchAttemptId: plan.launchAttemptId,
      }),
      (error) => {
        assert.equal(
          error.code,
          "invalid_logical_writer_launch_request",
        );
        return true;
      },
    );
    assert.equal(calls.image, 3);
    assert.equal(calls.imageInspect, 2);
    assert.equal(calls.imageResolve, 1);
    assert.equal(imageProviderInvocations.size, 3);
    assert.equal(imageProviderSignals.size, 3);
    assert.equal(calls.provider, 0);
    assert.equal(calls.publication, 0);
    assert.equal(calls.supervisor, 0);

    holdProvisioning = true;
    const admitted =
      deployment.stablePlanProvisioning.provisionStablePlan({
        admission,
        plan,
      });
    await settleWithin(
      provisioningEntered.promise,
      "deployment admitted provisioning",
    );
    const stopping = deployment.stop();
    assert.strictEqual(deployment.stop(), stopping);
    let stopSettled = false;
    void stopping.then(
      () => {
        stopSettled = true;
      },
      () => {
        stopSettled = true;
      },
    );
    await new Promise((resolve) => setImmediate(resolve));
    assert.equal(stopSettled, false);
    assert.equal(
      (
        await waitForDeploymentApplicationSessions(
          inspectionPool,
          applicationNames,
          4,
        )
      ).length,
      4,
    );

    releaseProvisioning.resolve();
    const replayedProvisioning = await admitted;
    assert.equal(replayedProvisioning.planSha256, plan.planSha256);
    assert.deepEqual(
      structuredClone(
        await settleWithin(stopping, "deployment shutdown drain"),
      ),
      { status: "stopped" },
    );
    assert.equal(stopSettled, true);
    assert.deepEqual(
      await waitForDeploymentApplicationSessions(
        inspectionPool,
        applicationNames,
        0,
      ),
      [],
    );
    assert.equal(calls.planGate, 2);
    assert.equal(calls.fleetGate, 0);
    assert.equal(calls.image, 3);
    assert.equal(calls.imageInspect, 2);
    assert.equal(calls.imageResolve, 1);
    assert.equal(imageProviderInvocations.size, 3);
    assert.equal(imageProviderSignals.size, 3);
    for (const signal of imageProviderSignals) {
      assert.equal(signal.aborted, false);
    }
    assert.equal(calls.provider, 0);
    assert.equal(calls.publication, 0);
    assert.equal(calls.supervisor, 0);
    assert.throws(
      () =>
        deployment.stablePlanProvisioning.provisionStablePlan({
          admission,
          plan,
        }),
      (error) => {
        assert.equal(
          error.code,
          "invalid_postgres_detached_restore_deployment_request",
        );
        return true;
      },
    );
    assert.throws(
      () => deployment.imagePlanReservations.prepareImageReservation({}),
      (error) => {
        assert.equal(
          error.code,
          "invalid_postgres_detached_restore_deployment_request",
        );
        return true;
      },
    );
  },
);
