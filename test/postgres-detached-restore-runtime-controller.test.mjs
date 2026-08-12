import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import { readFile } from "node:fs/promises";
import test from "node:test";

import { FilesystemOperationJournal } from "../src/filesystem-operation-journal.mjs";
import {
  POSTGRES_DETACHED_RESTORE_IMAGE_PLAN_PROVIDER_CONTRACT_VERSION,
  createPostgresDetachedRestoreImagePlanBinding,
} from "../src/postgres-detached-restore-image-plan-binding.mjs";
import {
  createPhysicalCollaboratorSettlement,
} from "../src/physical-collaborator-settlement.mjs";
import {
  createPostgresDetachedRestorePhysicalBindings,
  isPostgresDetachedRestorePublicationBinding,
} from "../src/postgres-detached-restore-physical-bindings.mjs";
import {
  PostgresDetachedRestoreRuntimeControllerError,
  createPostgresDetachedRestoreRuntimeController,
  isPostgresDetachedRestoreRuntimeController,
} from "../src/postgres-detached-restore-runtime-controller.mjs";
import {
  createPostgresDetachedRestoreRuntimeComposition,
} from "../src/postgres-detached-restore-runtime-composition.mjs";
import {
  createPostgresDetachedRestorePlan,
} from "../src/postgres-detached-restore-plan.mjs";
import {
  POSTGRES_DETACHED_RESTORE_STABLE_PLAN_PROVISIONING_CONFIRMED,
  PostgresDetachedRestoreStablePlanRegistryError,
} from "../src/postgres-detached-restore-stable-plan-registry.mjs";
import {
  LOGICAL_WRITER_LAUNCH_CONTRACT_VERSION,
} from "../src/postgres-logical-writer-launcher.mjs";
import {
  RESTORE_ATTACHMENT_ACTIVATION_CONTRACT_VERSION,
  RESTORE_ATTACHMENT_RECONCILIATION_CONTRACT_VERSION,
  createSessionManifest,
} from "../src/session-storage-contracts.mjs";
import { StoppedDirectoryPublication } from "../src/stopped-directory-publication.mjs";
import { StoppedWriterCapabilityCoordinator } from "../src/stopped-writer-capability.mjs";

const SESSION_ID = "019f8700-0000-7000-8000-000000000001";
const THREAD_ID = "019f8700-0000-7000-8000-000000000002";
const IMAGE_DIGEST = `sha256:${"a".repeat(64)}`;
const BACKEND_ID = "controller-capture-only-backend";
const SOURCE_STORAGE_ID = "controller-source-storage-001";
const DESTINATION_STORAGE_ID = "controller-destination-storage-001";
const NOW = "2026-08-12T00:00:00.000Z";
const RECOVERY_SCOPE_ID = "controller-recovery-001";
const MIGRATION_URLS = Object.freeze([
  new URL("../migrations/authority/001-session-authority.sql", import.meta.url),
  new URL("../migrations/authority/002-restore-destination-generations.sql", import.meta.url),
  new URL("../migrations/authority/003-operation-id-registry.sql", import.meta.url),
  new URL("../migrations/authority/004-restore-attachment-activation.sql", import.meta.url),
  new URL("../migrations/authority/005-restore-recovery-cursors.sql", import.meta.url),
  new URL("../migrations/authority/006-writer-stop-capture-handoff.sql", import.meta.url),
  new URL("../migrations/authority/007-detached-restore-stable-plans.sql", import.meta.url),
]);
const LANES = Object.freeze([
  "generation",
  "activation",
  "launch-attempt",
  "current-launch",
]);
const PHYSICAL_PUBLICATION_METHODS = Object.freeze([
  "publishFreshCheckpointArtifact",
  "publishRestoreDestination",
  "verifyCommittedCheckpointArtifact",
  "verifyCommittedRestoreDestination",
]);
const PHYSICAL_LIFECYCLE_METHODS = Object.freeze([
  "captureCheckpoint",
  "destroySession",
  "detachAttachment",
  "forceFence",
  "prepareRestoreAttachment",
  "prepareWritableAttachment",
  "provisionSession",
  "reconcileRestoreAttachment",
  "restoreCheckpoint",
]);
const PHYSICAL_SUPERVISOR_METHODS = Object.freeze([
  "launchWriter",
  "reconcileWriterLaunch",
  "stopWriter",
]);
const OPTION_CODE =
  "invalid_postgres_detached_restore_runtime_controller_options";
const REQUEST_CODE =
  "invalid_postgres_detached_restore_runtime_controller_request";
const OUTCOME_CODE =
  "postgres_detached_restore_runtime_controller_outcome_uncertain";

function freezeRecord(value) {
  return Object.freeze(Object.assign(Object.create(null), value));
}

const ignoreSettlementFatal = Object.freeze(() => undefined);

function createImagePlanProviderSettlement() {
  const options = Object.freeze({
    deadlineMilliseconds: 30_000,
    onFatal: ignoreSettlementFatal,
    settlementGraceMilliseconds: 1_000,
  });
  return Object.freeze({
    inspectCodex: createPhysicalCollaboratorSettlement(options),
    resolveImagePlan: createPhysicalCollaboratorSettlement(options),
  });
}

function createTestImagePlanBinding(provider) {
  return createPostgresDetachedRestoreImagePlanBinding(
    Object.freeze({
      provider,
      settlement: createImagePlanProviderSettlement(),
    }),
  );
}

function physicalPolicies(methods) {
  return Object.fromEntries(
    methods.map((method) => [
      method,
      Object.freeze({
        deadlineMilliseconds: 30_000,
        settlementGraceMilliseconds: 1_000,
      }),
    ]),
  );
}

function createTestPhysicalBindings(fixture, rawPublication, rawLifecycle) {
  const unexpected = async function unexpectedPhysicalProvider() {
    fixture.calls.provider += 1;
    throw new Error("physical provider must not run in controller tests");
  };
  return createPostgresDetachedRestorePhysicalBindings({
    lifecycleBackend: Object.freeze({
      ...rawLifecycle,
      physicalInvocationContractVersion: 1,
    }),
    lifecycleSettlement: physicalPolicies(PHYSICAL_LIFECYCLE_METHODS),
    onFatal: ignoreSettlementFatal,
    publication: rawPublication,
    publicationSettlement: physicalPolicies(PHYSICAL_PUBLICATION_METHODS),
    resolveRestoreDestination: unexpected,
    resolveRestoreDestinationContractVersion: 1,
    resolveRestoreDestinationSettlement: Object.freeze({
      deadlineMilliseconds: 30_000,
      settlementGraceMilliseconds: 1_000,
    }),
    supervisor: Object.freeze({
      contractVersion: 2,
      launchWriter: unexpected,
      reconcileWriterLaunch: unexpected,
      supervisorId: "controller-physical-supervisor-001",
    }),
    supervisorSettlement: physicalPolicies(PHYSICAL_SUPERVISOR_METHODS),
  });
}

function deferred() {
  let reject;
  let resolve;
  const promise = new Promise((resolvePromise, rejectPromise) => {
    reject = rejectPromise;
    resolve = resolvePromise;
  });
  return { promise, reject, resolve };
}

function nextTurn() {
  return new Promise((resolve) => setImmediate(resolve));
}

function queryText(args) {
  return typeof args[0] === "string" ? args[0] : args[0]?.text;
}

function queryValues(args) {
  return typeof args[0] === "string" ? args[1] : args[0]?.values;
}

function controllerError(code) {
  return (error) => {
    assert(error instanceof PostgresDetachedRestoreRuntimeControllerError);
    assert.equal(error.code, code);
    assert.equal(error.retryable, false);
    assert.equal(Object.isFrozen(error), true);
    assert.equal("cause" in error, false);
    return true;
  };
}

function provisioningCapabilityError(error) {
  assert(error instanceof PostgresDetachedRestoreStablePlanRegistryError);
  assert.equal(
    error.code,
    "postgres_detached_restore_stable_plan_provisioning_capability_required",
  );
  assert.equal(error.retryable, false);
  assert.equal(Object.isFrozen(error), true);
  return true;
}

function restoreAdmission(operationId = "controller-restore-operation-001") {
  return {
    checkpoint: {
      artifactId: "controller-source-artifact-001",
      backendId: BACKEND_ID,
      checkpointClass: "clean",
      checkpointId: "controller-source-checkpoint-001",
      codexSessionId: THREAD_ID,
      codexThreadId: THREAD_ID,
      contractVersion: 1,
      createdAt: "2026-08-11T22:00:00.000Z",
      imageDigest: IMAGE_DIGEST,
      sessionId: SESSION_ID,
      sourceFencingEpoch: "41",
      storageId: SOURCE_STORAGE_ID,
    },
    request: {
      backendId: BACKEND_ID,
      contractVersion: 1,
      fencingEpoch: "42",
      holderId: "controller-restore-holder-001",
      leaseId: "controller-restore-lease-001",
      operation: "restore",
      operationId,
      sessionId: SESSION_ID,
      storageId: DESTINATION_STORAGE_ID,
      target: {
        artifactId: "controller-source-artifact-001",
        checkpointId: "controller-source-checkpoint-001",
        kind: "checkpoint",
      },
    },
  };
}

function stablePlan(admission = restoreAdmission()) {
  return createPostgresDetachedRestorePlan({
    request: admission.request,
    plan: {
      captureCreatedAt: "2026-08-11T23:00:00.000Z",
      destinationDirectory: "/var/lib/portable-codex/restores/controller-001",
      destinationOwnedRoot: "/var/lib/portable-codex/restores",
      detachMode: "release",
      holderId: "controller-restored-writer-001",
      imagePlanId: "controller-image-plan-001",
      leaseDurationMilliseconds: 600_000,
      sourceArtifactDirectory:
        "/var/lib/portable-codex/artifacts/controller-source-001",
      sourceArtifactOwnedRoot: "/var/lib/portable-codex/artifacts",
    },
  });
}

function runtimeManifest() {
  return createSessionManifest({
    sessionId: SESSION_ID,
    codex: {
      ephemeral: false,
      historyMode: "paginated",
      rootThreadId: THREAD_ID,
      sessionId: THREAD_ID,
    },
    runtime: {
      codexSandbox: "danger-full-access",
      codexVersion: "codex-cli 0.144.1",
      imageDigest: IMAGE_DIGEST,
      imageMediaType: "application/vnd.oci.image.manifest.v1+json",
      platform: "linux/arm64",
    },
  });
}

class ControllerAuthorityClient {
  constructor(fixture) {
    this.connection = new EventEmitter();
    this.fixture = fixture;
    this.inTransaction = false;
    this.releaseCalls = [];
    this.transactionLane = null;
  }

  async query(...args) {
    const text = queryText(args);
    const values = queryValues(args) ?? [];
    this.fixture.events.push(["authority", text]);
    if (text === "DISCARD ALL") return { command: "DISCARD" };
    if (text === "BEGIN" || text === "BEGIN ISOLATION LEVEL SERIALIZABLE READ WRITE") {
      this.inTransaction = true;
      return { command: "BEGIN" };
    }
    if (text === "ROLLBACK") {
      this.inTransaction = false;
      this.transactionLane = null;
      return { command: "ROLLBACK" };
    }
    if (text === "COMMIT") {
      this.inTransaction = false;
      this.transactionLane = null;
      return { command: "COMMIT" };
    }
    if (text === "SET LOCAL search_path = pg_catalog") {
      return { command: "SET" };
    }
    if (text === "SET LOCAL synchronous_commit = on") {
      return { command: "SET" };
    }
    if (text.includes("pg_advisory_xact_lock")) {
      return { command: "SELECT", rows: [{}] };
    }
    if (text === "CREATE SCHEMA IF NOT EXISTS session_authority") {
      return { command: "CREATE" };
    }
    if (text.includes("CREATE TABLE IF NOT EXISTS session_authority.schema_migrations")) {
      return { command: "CREATE" };
    }
    if (text === "SELECT version, checksum FROM session_authority.schema_migrations ORDER BY version") {
      if (this.fixture.holdMigration !== null) {
        this.fixture.migrationEntered.resolve();
        await this.fixture.holdMigration.promise;
      }
      return { command: "SELECT", rows: [] };
    }
    if (this.fixture.migrationSql.has(text)) return { command: "CREATE" };
    if (text.startsWith("INSERT INTO session_authority.schema_migrations")) {
      return { command: "INSERT", rowCount: 1, rows: [] };
    }
    if (text.includes("transaction_timestamp() AS transaction_timestamp")) {
      return {
        command: "SELECT",
        rows: [{ transaction_id: "100", transaction_timestamp: new Date(NOW) }],
      };
    }
    if (text.includes("pg_current_xact_id()::pg_catalog.text AS transaction_id")) {
      return { command: "SELECT", rows: [{ transaction_id: "100" }] };
    }
    if (text.startsWith("INSERT INTO session_authority.restore_recovery_cursors")) {
      const lane = values[1];
      this.transactionLane = lane;
      if (!this.fixture.cursors.has(lane)) {
        this.fixture.cursors.set(lane, {
          after_session_id: null,
          cycle: "0",
          lane,
          last_request_sha256: null,
          last_transition_id: null,
          recovery_scope_id: RECOVERY_SCOPE_ID,
          revision: "0",
          updated_at: new Date(NOW),
        });
      }
      return { command: "INSERT", rowCount: 1, rows: [] };
    }
    if (text.includes("FROM session_authority.restore_recovery_cursors")) {
      const lane = values[1] ?? this.transactionLane;
      const row = this.fixture.cursors.get(lane);
      return { command: "SELECT", rows: row === undefined ? [] : [{ ...row }] };
    }
    if (text.startsWith("UPDATE session_authority.restore_recovery_cursors")) {
      const lane = values[1];
      const row = {
        after_session_id: values[2],
        cycle: values[3],
        lane,
        last_request_sha256: values[6],
        last_transition_id: values[5],
        recovery_scope_id: values[0],
        revision: values[4],
        updated_at: new Date(values[7]),
      };
      this.fixture.cursors.set(lane, row);
      return { command: "UPDATE", rowCount: 1, rows: [{ ...row }] };
    }
    for (const lane of LANES) {
      if (
        text.includes("session_authority") &&
        text.includes("LIMIT") &&
        (text.includes(lane.replace("launch-attempt", "writer-launch")) ||
          lane === "current-launch")
      ) {
        return { command: "SELECT", rows: [] };
      }
    }
    // Every recovery candidate query is read-only and an empty authority is a
    // valid first sweep. Keep this fallback narrow to SELECTs under the schema.
    if (text.startsWith("SELECT") && text.includes("session_authority.")) {
      return { command: "SELECT", rows: [] };
    }
    this.fixture.lastUnexpectedQuery = text;
    throw new Error(`unexpected authority query: ${text}`);
  }

  async release(...args) {
    this.releaseCalls.push(args);
  }
}

class ControllerAuthorityPool {
  constructor(fixture) {
    this.fixture = fixture;
    this.calls = { connect: 0, end: 0, query: 0 };
    this.clients = [];
  }

  async connect() {
    this.calls.connect += 1;
    const client = new ControllerAuthorityClient(this.fixture);
    this.clients.push(client);
    return client;
  }

  end() {
    this.calls.end += 1;
    throw new Error("authority pool remains caller-owned");
  }
}

class ControllerGuardClient {
  constructor(pool, pid) {
    this.pool = pool;
    this.pid = pid;
    this.releaseCalls = [];
    this.shared = false;
    this.exclusive = false;
  }

  query(query) {
    const text = query.text;
    const callback = query.callback;
    this.pool.fixture.events.push([this.pool.role, text]);
    if (text === "DISCARD ALL") {
      this.shared = false;
      this.exclusive = false;
      callback(null, { command: "DISCARD", rows: [] });
      return undefined;
    }
    if (text.includes("pg_try_advisory_lock_shared")) {
      const acquired =
        this.pool.role !== "recovery-lifecycle" ||
        this.pool.fixture.recoveryLockAcquired;
      this.shared = acquired;
      callback(null, {
        command: "SELECT",
        rows: [{ acquired, backend_pid: this.pid }],
      });
      return undefined;
    }
    if (text.includes("pg_try_advisory_lock")) {
      const acquired =
        this.pool.role !== "recovery-lifecycle" ||
        this.pool.fixture.recoveryLockAcquired;
      this.exclusive = acquired === true;
      callback(null, {
        command: "SELECT",
        rows: [{ acquired, backend_pid: this.pid }],
      });
      return undefined;
    }
    if (text.includes("FROM pg_catalog.pg_locks")) {
      callback(null, {
        command: "SELECT",
        rows: [
          {
            backend_pid: this.pid,
            lock_held: text.includes("ShareLock") ? this.shared : this.exclusive,
          },
        ],
      });
      return undefined;
    }
    if (text.includes("pg_advisory_unlock_shared")) {
      const unlocked = this.shared;
      this.shared = false;
      callback(null, {
        command: "SELECT",
        rows: [{ backend_pid: this.pid, unlocked }],
      });
      return undefined;
    }
    if (text.includes("pg_advisory_unlock")) {
      const unlocked = this.exclusive;
      this.exclusive = false;
      callback(null, {
        command: "SELECT",
        rows: [{ backend_pid: this.pid, unlocked }],
      });
      return undefined;
    }
    this.pool.fixture.lastGuardError = text;
    callback(new Error(`unexpected guard query: ${text}`));
    return undefined;
  }

  release(...args) {
    this.releaseCalls.push(args);
    this.shared = false;
    this.exclusive = false;
    return undefined;
  }
}

class ControllerGuardPool {
  constructor(fixture, role) {
    this.fixture = fixture;
    this.role = role;
    this.calls = { connect: 0, end: 0, query: 0 };
    this.clients = [];
  }

  connect(callback) {
    this.calls.connect += 1;
    const client = new ControllerGuardClient(
      this,
      30_000 + this.calls.connect,
    );
    this.clients.push(client);
    const release = (...args) => client.release(...args);
    const deliver = () => callback(null, client, release);
    if (this.fixture.blockedGuardRoles.has(this.role)) {
      this.fixture.guardConnectReleases.set(this.role, deliver);
      this.fixture.guardConnectEntered.get(this.role).resolve();
      return undefined;
    }
    deliver();
    return undefined;
  }

  end() {
    this.calls.end += 1;
    throw new Error(`${this.role} pool remains caller-owned`);
  }
}

function createLifecycleBackend(fixture) {
  const unexpected = async function unexpectedProvider() {
    fixture.calls.provider += 1;
    throw new Error("physical provider must not run in controller tests");
  };
  return Object.freeze({
    backendId: BACKEND_ID,
    capabilities: Object.freeze({
      atomicPointInTimeCheckpoint: true,
      exclusiveWriterAttachment: true,
      fencing: "epoch-enforced",
      normalDirectoryAttachment: true,
    }),
    captureCheckpoint: unexpected,
    contractVersion: 1,
    destroySession: unexpected,
    detachAttachment: unexpected,
    forceFence: unexpected,
    prepareRestoreAttachment: unexpected,
    prepareWritableAttachment: unexpected,
    provisionSession: unexpected,
    reconcileRestoreAttachment: unexpected,
    restoreAttachmentActivationContractVersion:
      RESTORE_ATTACHMENT_ACTIVATION_CONTRACT_VERSION,
    restoreAttachmentReconciliationContractVersion:
      RESTORE_ATTACHMENT_RECONCILIATION_CONTRACT_VERSION,
    restoreCheckpoint: unexpected,
  });
}

function createPublication(fixture) {
  const unexpected = async function unexpectedPublication() {
    fixture.calls.publication += 1;
    throw new Error("physical publication must not run in controller tests");
  };
  const journal = new FilesystemOperationJournal({
    acquireLock: unexpected,
    directory: "/var/lib/portable-codex/controller-test-journal",
    inspectAncestorAcl: unexpected,
    inspectDirectoryAcl: unexpected,
    inspectTemporaryRecord: unexpected,
    syncDirectory: unexpected,
  });
  return new StoppedDirectoryPublication({
    acquireLock: unexpected,
    inspectFilesystem: unexpected,
    inspectOwnedRootAcl: unexpected,
    inspectOwnedRootAncestorAcl: unexpected,
    inspectPersistentObjectIdentity: unexpected,
    journal,
    listMountPoints: unexpected,
  });
}

async function createFixture({
  holdMigration = false,
  onStep = () => undefined,
  recoveryLockAcquired = true,
} = {}) {
  const migrationSql = new Set(
    await Promise.all(MIGRATION_URLS.map((url) => readFile(url, "utf8"))),
  );
  const fixture = {
    calls: {
      fleetGate: 0,
      image: 0,
      planGate: 0,
      provider: 0,
      publication: 0,
      resolver: 0,
      supervisor: 0,
    },
    blockedGuardRoles: new Set(),
    cursors: new Map(),
    events: [],
    guardConnectEntered: new Map([
      ["foreground-lifecycle", deferred()],
      ["operation", deferred()],
    ]),
    guardConnectReleases: new Map(),
    holdMigration: holdMigration ? deferred() : null,
    imagePlanBlock: null,
    imagePlanEntered: deferred(),
    lastGuardError: null,
    lastUnexpectedQuery: null,
    migrationEntered: deferred(),
    migrationSql,
    recoveryLockAcquired,
  };
  const pools = {
    authority: new ControllerAuthorityPool(fixture),
    foregroundLifecycle: new ControllerGuardPool(
      fixture,
      "foreground-lifecycle",
    ),
    operation: new ControllerGuardPool(fixture, "operation"),
    recoveryLifecycle: new ControllerGuardPool(
      fixture,
      "recovery-lifecycle",
    ),
  };
  const imagePlanBinding = createTestImagePlanBinding(
    Object.freeze({
      contractVersion:
        POSTGRES_DETACHED_RESTORE_IMAGE_PLAN_PROVIDER_CONTRACT_VERSION,
      imagePlanProviderId: "controller-image-provider-001",
      async inspectCodex() {
        fixture.calls.image += 1;
        throw new Error("image inspection must not run");
      },
      async resolveImagePlan() {
        fixture.calls.image += 1;
        fixture.imagePlanEntered.resolve();
        await fixture.imagePlanBlock?.promise;
        throw new Error("controlled image plan resolution failure");
      },
    }),
  );
  const lifecycleBackend = createLifecycleBackend(fixture);
  const rawPublication = createPublication(fixture);
  const physicalBindings = createTestPhysicalBindings(
    fixture,
    rawPublication,
    lifecycleBackend,
  );
  assert.equal(
    isPostgresDetachedRestorePublicationBinding(
      physicalBindings.publication,
    ),
    true,
  );
  const runtime = createPostgresDetachedRestoreRuntimeComposition({
    authority: {
      maxTransactionAttempts: 1,
      restoreAttachmentActivationV2FleetCompatible: true,
      restoreAttachmentActivationV2GenerationPredecessorFleetCompatible: true,
      restoreGenerationV2FleetCompatible: true,
      writerLaunchStopV3FleetCompatible: true,
    },
    foreground: {
      fleetCapabilityGate() {
        fixture.calls.fleetGate += 1;
        throw new Error("foreground must not reach physical work");
      },
    },
    launch: {
      imagePlanBinding,
      stoppedWriterCoordinator: new StoppedWriterCapabilityCoordinator(),
      supervisor: Object.freeze({
        contractVersion: LOGICAL_WRITER_LAUNCH_CONTRACT_VERSION,
        async launchWriter() {
          fixture.calls.supervisor += 1;
          throw new Error("supervisor must not launch");
        },
        async reconcileWriterLaunch() {
          fixture.calls.supervisor += 1;
          throw new Error("supervisor must not reconcile");
        },
        supervisorId: "controller-supervisor-001",
      }),
    },
    planRegistry: {
      async provisioningFleetCapabilityGate(input) {
        fixture.calls.planGate += 1;
        if (fixture.planGateOverride !== undefined) {
          return fixture.planGateOverride(input);
        }
        await fixture.planGateBlock?.promise;
        if (fixture.planGateReject !== undefined) {
          throw fixture.planGateReject;
        }
        return POSTGRES_DETACHED_RESTORE_STABLE_PLAN_PROVISIONING_CONFIRMED;
      },
    },
    pools,
    recovery: {
      intervalMilliseconds: 60_000,
      limits: {
        activation: 1,
        currentLaunch: 1,
        generation: 1,
        launchAttempt: 1,
      },
      onStep,
      recoveryScopeId: RECOVERY_SCOPE_ID,
    },
    storage: {
      backendId: BACKEND_ID,
      lifecycleBackend,
      publication: physicalBindings.publication,
      resolveArtifactPaths() {
        fixture.calls.resolver += 1;
        throw new Error("artifact resolver must not run");
      },
      resolveRestoreDestination() {
        fixture.calls.resolver += 1;
        throw new Error("destination resolver must not run");
      },
      resolveSourceOwnedRoot() {
        fixture.calls.resolver += 1;
        throw new Error("source resolver must not run");
      },
    },
  });
  const controller = createPostgresDetachedRestoreRuntimeController({ runtime });
  return Object.assign(fixture, { controller, pools, runtime });
}

function assertNoPhysicalEffects(fixture) {
  assert.equal(fixture.calls.fleetGate, 0);
  assert.equal(fixture.calls.image, 0);
  assert.equal(fixture.calls.provider, 0);
  assert.equal(fixture.calls.publication, 0);
  assert.equal(fixture.calls.resolver, 0);
  assert.equal(fixture.calls.supervisor, 0);
  for (const pool of Object.values(fixture.pools)) {
    assert.equal(pool.calls.end, 0);
  }
}

test("controller exposes only exact frozen admission and lifecycle facets", async () => {
  const fixture = await createFixture();
  const { controller } = fixture;
  assert.deepEqual(Reflect.ownKeys(controller), [
    "foreground",
    "imagePlanReservations",
    "stablePlanProvisioning",
    "start",
    "stop",
    "writerLaunch",
  ]);
  assert.equal(Object.getPrototypeOf(controller), null);
  assert.equal(Object.isFrozen(controller), true);
  assert.equal(isPostgresDetachedRestoreRuntimeController(controller), true);
  assert.equal(isPostgresDetachedRestoreRuntimeController({ ...controller }), false);
  assert.deepEqual(Reflect.ownKeys(controller.foreground), [
    "restoreContextContractVersion",
    "runRestore",
  ]);
  assert.deepEqual(Reflect.ownKeys(controller.imagePlanReservations), [
    "prepareImageReservation",
  ]);
  assert.deepEqual(Reflect.ownKeys(controller.stablePlanProvisioning), [
    "provisionStablePlan",
  ]);
  assert.deepEqual(Reflect.ownKeys(controller.writerLaunch), [
    "reconcileLaunchAttempt",
    "runLaunch",
  ]);
  for (const hidden of ["backend", "bootstrap", "runtime", "scheduler"]) {
    assert.equal(hidden in controller, false);
  }
  for (const value of [
    controller,
    controller.foreground,
    controller.imagePlanReservations,
    controller.stablePlanProvisioning,
    controller.writerLaunch,
    controller.start,
    controller.stop,
    controller.foreground.runRestore,
    controller.imagePlanReservations.prepareImageReservation,
    controller.stablePlanProvisioning.provisionStablePlan,
    controller.writerLaunch.reconcileLaunchAttempt,
    controller.writerLaunch.runLaunch,
  ]) {
    assert.equal(Object.isFrozen(value), true);
  }
  await assert.rejects(
    controller.imagePlanReservations.prepareImageReservation({}),
    controllerError(REQUEST_CODE),
  );
  await assert.rejects(
    controller.foreground.runRestore(restoreAdmission(), async () => null),
    controllerError(REQUEST_CODE),
  );
  await assert.rejects(
    controller.stablePlanProvisioning.provisionStablePlan({
      admission: restoreAdmission(),
      plan: stablePlan(),
    }),
    controllerError(REQUEST_CODE),
  );
  await assert.rejects(
    controller.writerLaunch.runLaunch({}),
    controllerError(REQUEST_CODE),
  );
  assertNoPhysicalEffects(fixture);
  await controller.stop();
});

test("one authentic runtime has exactly one controller owner without construction I/O", async () => {
  const fixture = await createFixture();
  const ioSnapshot = () => ({
    calls: { ...fixture.calls },
    cursorCount: fixture.cursors.size,
    eventCount: fixture.events.length,
    pools: Object.fromEntries(
      Object.entries(fixture.pools).map(([name, pool]) => [
        name,
        { ...pool.calls, clientCount: pool.clients.length },
      ]),
    ),
  });
  const assertDuplicateRejectedWithoutIo = () => {
    const before = ioSnapshot();
    assert.throws(
      () =>
        createPostgresDetachedRestoreRuntimeController({
          runtime: fixture.runtime,
        }),
      controllerError(OPTION_CODE),
    );
    assert.deepEqual(ioSnapshot(), before);
  };
  assertDuplicateRejectedWithoutIo();
  await fixture.controller.stop();
  assertDuplicateRejectedWithoutIo();
  assertNoPhysicalEffects(fixture);
});

test("distinct authentic runtimes have independent controller lifecycles", async () => {
  const [first, second] = await Promise.all([createFixture(), createFixture()]);
  assert.notStrictEqual(first.runtime, second.runtime);
  assert.notStrictEqual(first.controller, second.controller);
  assert.deepEqual(await Promise.all([
    first.controller.start(),
    second.controller.start(),
  ]), [
    freezeRecord({ status: "ready" }),
    freezeRecord({ status: "ready" }),
  ]);
  assert.deepEqual(await Promise.all([
    first.controller.stop(),
    second.controller.stop(),
  ]), [
    freezeRecord({ status: "stopped" }),
    freezeRecord({ status: "stopped" }),
  ]);
  assertNoPhysicalEffects(first);
  assertNoPhysicalEffects(second);
});

test("start migrates and completes one coalesced initial sweep before opening ingress", async () => {
  const fixture = await createFixture({ holdMigration: true });
  const first = fixture.controller.start();
  assert.strictEqual(fixture.controller.start(), first);
  await fixture.migrationEntered.promise;
  await assert.rejects(
    fixture.controller.imagePlanReservations.prepareImageReservation({}),
    controllerError(REQUEST_CODE),
  );
  await assert.rejects(
    fixture.controller.stablePlanProvisioning.provisionStablePlan({
      admission: restoreAdmission(),
      plan: stablePlan(),
    }),
    controllerError(REQUEST_CODE),
  );
  fixture.holdMigration.resolve();
  await assert.doesNotReject(first, () => {
    assert.fail(
      `unexpected authority query: ${fixture.lastUnexpectedQuery}; guard: ${fixture.lastGuardError}; events: ${fixture.events
        .map(([role, text]) => `${role}:${text.slice(0, 80)}`)
        .join(" | ")}`,
    );
  });
  assert.deepEqual(await first, freezeRecord({ status: "ready" }));
  assert.equal(fixture.cursors.size, 4);
  for (const lane of LANES) {
    assert.equal(fixture.cursors.get(lane).cycle, "1");
    assert.equal(fixture.cursors.get(lane).revision, "1");
  }
  fixture.planGateBlock = deferred();
  const admission = restoreAdmission();
  const provision = fixture.controller.stablePlanProvisioning.provisionStablePlan({
    admission,
    plan: stablePlan(admission),
  });
  await nextTurn();
  assert.equal(fixture.calls.planGate, 1);
  fixture.planGateReject = new Error("controlled provision failure");
  fixture.planGateBlock.resolve();
  await assert.rejects(provision);
  await fixture.controller.stop();
  assert.throws(
    () => fixture.controller.start(),
    controllerError(REQUEST_CODE),
  );
  assertNoPhysicalEffects(fixture);
});

test("stop during migration is single-flight and startup can never reopen ingress", async () => {
  const fixture = await createFixture({ holdMigration: true });
  const start = fixture.controller.start();
  await fixture.migrationEntered.promise;
  const stop = fixture.controller.stop();
  assert.strictEqual(fixture.controller.stop(), stop);
  fixture.holdMigration.resolve();
  await assert.rejects(start, controllerError(OUTCOME_CODE));
  assert.deepEqual(await stop, freezeRecord({ status: "stopped" }));
  assert.throws(() => fixture.controller.start(), controllerError(REQUEST_CODE));
  await assert.rejects(
    fixture.controller.stablePlanProvisioning.provisionStablePlan({
      admission: restoreAdmission(),
      plan: stablePlan(),
    }),
    controllerError(REQUEST_CODE),
  );
  assertNoPhysicalEffects(fixture);
});

test("stop closes ingress immediately and drains every admitted ingress facet", async () => {
  const fixture = await createFixture();
  await fixture.controller.start();
  fixture.planGateBlock = deferred();
  fixture.imagePlanBlock = deferred();
  fixture.blockedGuardRoles.add("foreground-lifecycle");
  fixture.blockedGuardRoles.add("operation");
  const foreground = fixture.controller.foreground.runRestore(
    restoreAdmission("controller-foreground-drain-001"),
    async () => null,
  );
  const launch = fixture.controller.writerLaunch.reconcileLaunchAttempt({
    launchAttemptId: "controller-launch-drain-001",
  });
  const imagePlanReservation =
    fixture.controller.imagePlanReservations.prepareImageReservation({
      plan: stablePlan(),
      sessionManifest: runtimeManifest(),
    });
  const provisionAdmission = restoreAdmission("controller-provision-drain-001");
  const provision = fixture.controller.stablePlanProvisioning.provisionStablePlan({
    admission: provisionAdmission,
    plan: stablePlan(provisionAdmission),
  });
  await Promise.all([
    fixture.guardConnectEntered.get("foreground-lifecycle").promise,
    fixture.guardConnectEntered.get("operation").promise,
    fixture.imagePlanEntered.promise,
  ]);
  assert.equal(fixture.calls.planGate, 1);
  const stop = fixture.controller.stop();
  let stopSettled = false;
  void stop.then(() => {
    stopSettled = true;
  });
  await assert.rejects(
    fixture.controller.imagePlanReservations.prepareImageReservation({}),
    controllerError(REQUEST_CODE),
  );
  await assert.rejects(
    fixture.controller.stablePlanProvisioning.provisionStablePlan({
      admission: restoreAdmission("controller-provision-after-stop-001"),
      plan: stablePlan(
        restoreAdmission("controller-provision-after-stop-001"),
      ),
    }),
    controllerError(REQUEST_CODE),
  );
  await assert.rejects(
    fixture.controller.foreground.runRestore(
      restoreAdmission("controller-foreground-after-stop-001"),
      async () => null,
    ),
    controllerError(REQUEST_CODE),
  );
  await assert.rejects(
    fixture.controller.writerLaunch.reconcileLaunchAttempt({
      launchAttemptId: "controller-launch-after-stop-001",
    }),
    controllerError(REQUEST_CODE),
  );
  await nextTurn();
  assert.equal(stopSettled, false);
  fixture.planGateReject = new Error("controlled admitted rejection");
  fixture.planGateBlock.resolve();
  fixture.imagePlanBlock.resolve();
  fixture.guardConnectReleases.get("foreground-lifecycle")();
  fixture.guardConnectReleases.get("operation")();
  const settlements = await Promise.allSettled([
    foreground,
    imagePlanReservation,
    provision,
    launch,
  ]);
  assert.deepEqual(
    settlements.map(({ status }) => status),
    ["rejected", "rejected", "rejected", "rejected"],
  );
  assert.deepEqual(await stop, freezeRecord({ status: "stopped" }));
  assert.equal(fixture.calls.image, 1);
  fixture.calls.image = 0;
  await assert.rejects(
    fixture.controller.imagePlanReservations.prepareImageReservation({}),
    controllerError(REQUEST_CODE),
  );
  assertNoPhysicalEffects(fixture);
});

test(
  "provisioning gates cannot make their own controller wait on admitted ingress",
  { timeout: 1_000 },
  async () => {
    const fixture = await createFixture();
    const start = fixture.controller.start();
    await start;
    const stopErrors = [];
    let stopReturned = 0;
    const stopFromGate = () => {
      try {
        const pending = fixture.controller.stop();
        stopReturned += 1;
        return pending;
      } catch (error) {
        stopErrors.push(error);
        throw error;
      }
    };
    const gateBehaviors = [
      () => stopFromGate(),
      () => Promise.resolve().then(() => stopFromGate()),
    ];
    let gateIndex = 0;
    fixture.planGateOverride = () => gateBehaviors[gateIndex++]();
    const directAdmission = restoreAdmission("controller-self-stop-direct-001");
    const chainedAdmission = restoreAdmission(
      "controller-self-stop-chained-001",
    );
    const direct = fixture.controller.stablePlanProvisioning.provisionStablePlan({
      admission: directAdmission,
      plan: stablePlan(directAdmission),
    });
    const chained =
      fixture.controller.stablePlanProvisioning.provisionStablePlan({
        admission: chainedAdmission,
        plan: stablePlan(chainedAdmission),
      });
    const settlements = await Promise.allSettled([direct, chained]);
    assert.deepEqual(
      settlements.map(({ status }) => status),
      ["rejected", "rejected"],
    );
    for (const { reason } of settlements) provisioningCapabilityError(reason);
    assert.equal(gateIndex, 2);
    assert.equal(stopReturned, 0);
    assert.equal(stopErrors.length, 2);
    for (const error of stopErrors) controllerError(REQUEST_CODE)(error);
    assert.strictEqual(fixture.controller.start(), start);
    assert.deepEqual(
      await fixture.controller.stop(),
      freezeRecord({ status: "stopped" }),
    );
    assertNoPhysicalEffects(fixture);
  },
);

test(
  "one controller ingress context may stop a distinct controller",
  { timeout: 1_000 },
  async () => {
    const [first, second] = await Promise.all([createFixture(), createFixture()]);
    const [firstStart, secondStart] = [
      first.controller.start(),
      second.controller.start(),
    ];
    await Promise.all([firstStart, secondStart]);
    let firstStop = null;
    second.planGateOverride = () => {
      firstStop = first.controller.stop();
      return firstStop;
    };
    const admission = restoreAdmission("controller-cross-stop-001");
    await assert.rejects(
      second.controller.stablePlanProvisioning.provisionStablePlan({
        admission,
        plan: stablePlan(admission),
      }),
      provisioningCapabilityError,
    );
    assert.notEqual(firstStop, null);
    assert.deepEqual(
      await firstStop,
      freezeRecord({ status: "stopped" }),
    );
    assert.throws(
      () => first.controller.start(),
      controllerError(REQUEST_CODE),
    );
    assert.strictEqual(second.controller.start(), secondStart);
    assert.deepEqual(
      await second.controller.stop(),
      freezeRecord({ status: "stopped" }),
    );
    assertNoPhysicalEffects(first);
    assertNoPhysicalEffects(second);
  },
);

test("controller preserves the capture-only backend and caller-owned pools", async () => {
  const fixture = await createFixture();
  await assert.rejects(
    fixture.runtime.backend.restoreCheckpoint(restoreAdmission()),
    (error) => error?.code === "stopped_directory_backend_outcome_uncertain",
  );
  assertNoPhysicalEffects(fixture);
  await fixture.controller.start();
  await fixture.controller.stop();
  assertNoPhysicalEffects(fixture);
});

test("stop ignores after-import Array iterator poison and stays single-flight", async () => {
  const fixture = await createFixture();
  const iteratorDescriptor = Object.getOwnPropertyDescriptor(
    Array.prototype,
    Symbol.iterator,
  );
  let iteratorCalls = 0;
  let reentryCalls = 0;
  let reentryOpen = true;
  let stop;
  Object.defineProperty(Array.prototype, Symbol.iterator, {
    ...iteratorDescriptor,
    value: function poisonedArrayIterator() {
      iteratorCalls += 1;
      if (reentryOpen) {
        reentryOpen = false;
        reentryCalls += 1;
        fixture.controller.stop();
      }
      throw new Error("Array iterator poison must not run");
    },
  });
  try {
    stop = fixture.controller.stop();
  } finally {
    Object.defineProperty(
      Array.prototype,
      Symbol.iterator,
      iteratorDescriptor,
    );
  }
  assert.equal(iteratorCalls, 0);
  assert.equal(reentryCalls, 0);
  assert.strictEqual(fixture.controller.stop(), stop);
  assert.deepEqual(await stop, freezeRecord({ status: "stopped" }));
  assertNoPhysicalEffects(fixture);
});

test("startup keeps ingress closed for incomplete and hostile initial sweeps", async (t) => {
  let unsafePromiseThenCalls = 0;
  let thenableCalls = 0;
  class UnsafeObserverPromise extends Promise {
    then(...args) {
      unsafePromiseThenCalls += 1;
      return super.then(...args);
    }
  }
  const cases = [
    ["busy receipt", { recoveryLockAcquired: false }],
    ["outcome-uncertain receipt", { recoveryLockAcquired: "malformed" }],
    [
      "rejected observer Promise",
      { onStep: () => Promise.reject(new Error("controlled observer rejection")) },
    ],
    [
      "unsafe Promise subclass",
      {
        onStep: () =>
          new UnsafeObserverPromise((resolve) => resolve(undefined)),
      },
    ],
    [
      "malformed thenable",
      {
        onStep: () =>
          Object.freeze({
            then() {
              thenableCalls += 1;
            },
          }),
      },
    ],
  ];
  for (let index = 0; index < cases.length; index += 1) {
    const [name, options] = cases[index];
    await t.test(name, async () => {
      const fixture = await createFixture(options);
      await assert.rejects(
        fixture.controller.start(),
        controllerError(OUTCOME_CODE),
      );
      const admission = restoreAdmission(`controller-closed-${index + 1}`);
      await assert.rejects(
        fixture.controller.stablePlanProvisioning.provisionStablePlan({
          admission,
          plan: stablePlan(admission),
        }),
        controllerError(REQUEST_CODE),
      );
      assertNoPhysicalEffects(fixture);
    });
  }
  assert.equal(unsafePromiseThenCalls, 0);
  assert.equal(thenableCalls, 0);
});

test("options and lifecycle requests reject proxy, accessor, and malformed calls", async (t) => {
  const fixture = await createFixture();
  await t.test("extra option", () => {
    assert.throws(
      () =>
        createPostgresDetachedRestoreRuntimeController({
          extra: true,
          runtime: fixture.runtime,
        }),
      controllerError(OPTION_CODE),
    );
  });
  await t.test("runtime accessor", () => {
    let getterCalls = 0;
    const options = {};
    Object.defineProperty(options, "runtime", {
      enumerable: true,
      get() {
        getterCalls += 1;
        return fixture.runtime;
      },
    });
    assert.throws(
      () => createPostgresDetachedRestoreRuntimeController(options),
      controllerError(OPTION_CODE),
    );
    assert.equal(getterCalls, 0);
  });
  await t.test("proxied options", () => {
    let traps = 0;
    const options = new Proxy(
      { runtime: fixture.runtime },
      {
        ownKeys() {
          traps += 1;
          throw new Error("options trap must not run");
        },
      },
    );
    assert.throws(
      () => createPostgresDetachedRestoreRuntimeController(options),
      controllerError(OPTION_CODE),
    );
    assert.equal(traps, 0);
  });
  assert.throws(() => fixture.controller.start(null), controllerError(REQUEST_CODE));
  assert.throws(() => fixture.controller.stop(null), controllerError(REQUEST_CODE));
  await fixture.controller.stop();
});
