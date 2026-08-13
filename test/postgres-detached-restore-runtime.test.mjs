import assert from "node:assert/strict";
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
  createPostgresDetachedRestoreOperationalLeaseBudget,
  isPostgresDetachedRestoreOperationalLeaseBudget,
} from "../src/postgres-detached-restore-operational-lease-budget.mjs";
import {
  createPostgresDetachedRestorePhysicalBindings,
  isPostgresDetachedRestorePublicationBinding,
} from "../src/postgres-detached-restore-physical-bindings.mjs";
import {
  LOGICAL_WRITER_LAUNCH_CONTRACT_VERSION,
  PostgresLogicalWriterLauncherError,
} from "../src/postgres-logical-writer-launcher.mjs";
import {
  PostgresDetachedRestoreRuntimeCompositionError,
  createPostgresDetachedRestoreRuntimeComposition,
  isPostgresDetachedRestoreRuntimeComposition,
} from "../src/postgres-detached-restore-runtime-composition.mjs";
import {
  PostgresDetachedRestoreStablePlanRegistryError,
} from "../src/postgres-detached-restore-stable-plan-registry.mjs";
import {
  PostgresRestoreRecoverySchedulerError,
  isPostgresRestoreRecoveryScheduler,
} from "../src/postgres-restore-recovery-scheduler.mjs";
import {
  RESTORE_ATTACHMENT_ACTIVATION_CONTRACT_VERSION,
  RESTORE_ATTACHMENT_RECONCILIATION_CONTRACT_VERSION,
  SessionStorageContractError,
  assertCheckpointBackend,
  assertCheckpointCaptureReconciliationBackend,
  assertPreparedCheckpointCaptureBackend,
  assertRestoreAttachmentActivationBackend,
  assertRestoreAttachmentReconciliationBackend,
  assertStorageBackend,
} from "../src/session-storage-contracts.mjs";
import {
  StoppedDirectoryBackendError,
} from "../src/stopped-directory-backend.mjs";
import { StoppedDirectoryPublication } from "../src/stopped-directory-publication.mjs";
import { StoppedWriterCapabilityCoordinator } from "../src/stopped-writer-capability.mjs";

const SESSION_ID = "019f8500-0000-7000-8000-000000000001";
const THREAD_ID = "019f8500-0000-7000-8000-000000000002";
const IMAGE_DIGEST = `sha256:${"a".repeat(64)}`;
const BACKEND_ID = "runtime-stopped-directory-backend";
const SOURCE_STORAGE_ID = "source-storage-001";
const DESTINATION_STORAGE_ID = "destination-storage-001";
const CHECKPOINT_ID = "checkpoint-001";
const ARTIFACT_ID = "artifact-001";
const RUNTIME_OPTION_ERROR =
  "invalid_postgres_detached_restore_runtime_composition_options";
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

class NoIoPool {
  constructor(role) {
    this.role = role;
    this.calls = { connect: 0, end: 0, query: 0 };
  }

  connect() {
    this.calls.connect += 1;
    throw new Error(`${this.role} pool connect must not run`);
  }

  query() {
    this.calls.query += 1;
    throw new Error(`${this.role} pool query must not run`);
  }

  end() {
    this.calls.end += 1;
    throw new Error(`${this.role} pool end must remain caller-owned`);
  }
}

function cloneWithDataProperty(value, key, replacement) {
  const descriptors = Object.getOwnPropertyDescriptors(value);
  descriptors[key] = {
    configurable: false,
    enumerable: true,
    value: replacement,
    writable: false,
  };
  return Object.create(Object.getPrototypeOf(value), descriptors);
}

function cloneWithAccessor(value, key, getter) {
  const descriptors = Object.getOwnPropertyDescriptors(value);
  descriptors[key] = {
    configurable: false,
    enumerable: true,
    get: getter,
  };
  return Object.create(Object.getPrototypeOf(value), descriptors);
}

function noIoRawPool() {
  return {
    calls: { connect: 0, end: 0, query: 0 },
    end() {
      this.calls.end += 1;
      throw new Error("raw pool end must remain caller-owned");
    },
    query() {
      this.calls.query += 1;
      throw new Error("raw pool query must not run");
    },
  };
}

function ordinaryReceiverWithProxyPrototype(target, traps) {
  const prototype = new Proxy(target, {
    get(...args) {
      traps.get += 1;
      return Reflect.get(...args);
    },
    getOwnPropertyDescriptor(...args) {
      traps.getOwnPropertyDescriptor += 1;
      return Reflect.getOwnPropertyDescriptor(...args);
    },
    getPrototypeOf(...args) {
      traps.getPrototypeOf += 1;
      return Reflect.getPrototypeOf(...args);
    },
  });
  return Object.create(prototype);
}

function exactKeys(value, expected) {
  assert.deepEqual(Reflect.ownKeys(value).sort(), [...expected].sort());
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

function createTestOperationalLeaseBudget() {
  return createPostgresDetachedRestoreOperationalLeaseBudget({
    databaseRequestMilliseconds: 30_000,
    imagePlanProviderSettlement: physicalPolicies([
      "inspectCodex",
      "resolveImagePlan",
    ]),
    leaseDurationMilliseconds: 600_000,
    lifecycleBackendSettlement: physicalPolicies(
      PHYSICAL_LIFECYCLE_METHODS,
    ),
    publicationSettlement: physicalPolicies(PHYSICAL_PUBLICATION_METHODS),
    resolveRestoreDestinationSettlement: Object.freeze({
      deadlineMilliseconds: 30_000,
      settlementGraceMilliseconds: 1_000,
    }),
    safetyMarginMilliseconds: 1_000,
    supervisorSettlement: physicalPolicies(PHYSICAL_SUPERVISOR_METHODS),
  });
}

function createTestPhysicalBindings(calls, rawPublication, rawLifecycle) {
  const lifecycleBackend = Object.freeze({
    ...rawLifecycle,
    physicalInvocationContractVersion: 1,
  });
  const unexpected = async function unexpectedPhysicalProvider() {
    calls.provider += 1;
    throw new Error("physical provider must not run");
  };
  return createPostgresDetachedRestorePhysicalBindings({
    lifecycleBackend,
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
      supervisorId: "runtime-physical-supervisor-001",
    }),
    supervisorSettlement: physicalPolicies(PHYSICAL_SUPERVISOR_METHODS),
  });
}

function runtimeOptionError(error) {
  assert(error instanceof PostgresDetachedRestoreRuntimeCompositionError);
  assert.equal(error.code, RUNTIME_OPTION_ERROR);
  assert.equal(error.retryable, false);
  assert.equal(Object.isFrozen(error), true);
  return true;
}

function schedulerRequestError(error) {
  assert(error instanceof PostgresRestoreRecoverySchedulerError);
  assert.equal(
    error.code,
    "invalid_postgres_restore_recovery_scheduler_request",
  );
  return true;
}

function launcherRequestError(error) {
  assert(error instanceof PostgresLogicalWriterLauncherError);
  assert.equal(error.code, "invalid_logical_writer_launch_request");
  assert.equal(error.retryable, false);
  assert.equal(Object.isFrozen(error), true);
  return true;
}

function stablePlanRegistryRequestError(error) {
  assert(error instanceof PostgresDetachedRestoreStablePlanRegistryError);
  assert.equal(
    error.code,
    "invalid_postgres_detached_restore_stable_plan_registry_request",
  );
  assert.equal(error.retryable, false);
  assert.equal(Object.isFrozen(error), true);
  return true;
}

function storageBackendError(error) {
  assert(error instanceof SessionStorageContractError);
  assert.equal(error.code, "invalid_storage_backend");
  return true;
}

function createLifecycleBackend(calls) {
  const invoke = async function invokeProvider() {
    calls.provider += 1;
    throw new Error("capture lifecycle provider must not run");
  };
  return Object.freeze({
    backendId: BACKEND_ID,
    capabilities: Object.freeze({
      atomicPointInTimeCheckpoint: true,
      exclusiveWriterAttachment: true,
      fencing: "epoch-enforced",
      normalDirectoryAttachment: true,
    }),
    captureCheckpoint: invoke,
    contractVersion: 1,
    destroySession: invoke,
    detachAttachment: invoke,
    forceFence: invoke,
    prepareRestoreAttachment: invoke,
    prepareWritableAttachment: invoke,
    provisionSession: invoke,
    reconcileRestoreAttachment: invoke,
    restoreAttachmentActivationContractVersion:
      RESTORE_ATTACHMENT_ACTIVATION_CONTRACT_VERSION,
    restoreAttachmentReconciliationContractVersion:
      RESTORE_ATTACHMENT_RECONCILIATION_CONTRACT_VERSION,
    restoreCheckpoint: invoke,
  });
}

function createPublication(calls) {
  const journal = new FilesystemOperationJournal({
    acquireLock: async function acquireJournalLock() {
      calls.publication += 1;
      throw new Error("journal lock must not be acquired");
    },
    directory: "/var/lib/portable-codex/runtime-composition-test-journal",
    inspectAncestorAcl: async function inspectJournalAncestorAcl() {
      calls.publication += 1;
      throw new Error("journal ancestor ACL must not be inspected");
    },
    inspectDirectoryAcl: async function inspectJournalDirectoryAcl() {
      calls.publication += 1;
      throw new Error("journal directory ACL must not be inspected");
    },
    inspectTemporaryRecord: async function inspectJournalTemporaryRecord() {
      calls.publication += 1;
      throw new Error("journal temporary record must not be inspected");
    },
    syncDirectory: async function syncJournalDirectory() {
      calls.publication += 1;
      throw new Error("journal directory must not be synced");
    },
  });
  return new StoppedDirectoryPublication({
    acquireLock: async function acquirePublicationLock() {
      calls.publication += 1;
      throw new Error("publication lock must not be acquired");
    },
    inspectFilesystem: async function inspectPublicationFilesystem() {
      calls.publication += 1;
      throw new Error("publication filesystem must not be inspected");
    },
    inspectOwnedRootAcl: async function inspectPublicationOwnedRootAcl() {
      calls.publication += 1;
      throw new Error("publication root ACL must not be inspected");
    },
    inspectOwnedRootAncestorAcl:
      async function inspectPublicationOwnedRootAncestorAcl() {
        calls.publication += 1;
        throw new Error("publication root ancestor ACL must not be inspected");
      },
    inspectPersistentObjectIdentity:
      async function inspectPublicationPersistentObjectIdentity() {
        calls.publication += 1;
        throw new Error("publication object identity must not be inspected");
      },
    journal,
    listMountPoints: async function listPublicationMountPoints() {
      calls.publication += 1;
      throw new Error("publication mount points must not be listed");
    },
  });
}

function createRuntimeFixture() {
  const calls = {
    fleetGate: 0,
    image: 0,
    onStep: 0,
    planProvisioningGate: 0,
    provider: 0,
    publication: 0,
    storageResolver: 0,
    supervisor: 0,
  };
  const pools = {
    authority: new NoIoPool("authority"),
    foregroundLifecycle: new NoIoPool("foreground-lifecycle"),
    operation: new NoIoPool("operation"),
    recoveryLifecycle: new NoIoPool("recovery-lifecycle"),
  };
  const lifecycleBackend = createLifecycleBackend(calls);
  const rawPublication = createPublication(calls);
  const physicalBindings = createTestPhysicalBindings(
    calls,
    rawPublication,
    lifecycleBackend,
  );
  const publication = physicalBindings.publication;
  const imagePlanBinding = createTestImagePlanBinding(
    Object.freeze({
      contractVersion:
        POSTGRES_DETACHED_RESTORE_IMAGE_PLAN_PROVIDER_CONTRACT_VERSION,
      imagePlanProviderId: "runtime-image-provider-001",
      async inspectCodex() {
        calls.image += 1;
        throw new Error("image inspection must not run");
      },
      async resolveImagePlan() {
        calls.image += 1;
        throw new Error("image plan resolution must not run");
      },
    }),
  );
  const stoppedWriterCoordinator = new StoppedWriterCapabilityCoordinator();
  const supervisor = Object.freeze({
    contractVersion: LOGICAL_WRITER_LAUNCH_CONTRACT_VERSION,
    async launchWriter() {
      calls.supervisor += 1;
      throw new Error("writer supervisor must not launch");
    },
    async reconcileWriterLaunch() {
      calls.supervisor += 1;
      throw new Error("writer supervisor must not reconcile");
    },
    supervisorId: "runtime-supervisor-001",
  });
  const options = {
    authority: {
      maxTransactionAttempts: 3,
      restoreAttachmentActivationV2FleetCompatible: true,
      restoreAttachmentActivationV2GenerationPredecessorFleetCompatible: true,
      restoreGenerationV2FleetCompatible: true,
      writerLaunchStopV3FleetCompatible: true,
    },
    foreground: {
      fleetCapabilityGate() {
        calls.fleetGate += 1;
        throw new Error("foreground fleet gate must not run");
      },
    },
    launch: {
      imagePlanBinding,
      stoppedWriterCoordinator,
      supervisor,
    },
    pools,
    planRegistry: {
      operationalLeaseBudget: createTestOperationalLeaseBudget(),
      provisioningFleetCapabilityGate() {
        calls.planProvisioningGate += 1;
        throw new Error("stable-plan provisioning gate must not run");
      },
    },
    recovery: {
      intervalMilliseconds: 60_000,
      limits: {
        activation: 1,
        currentLaunch: 1,
        generation: 1,
        launchAttempt: 1,
      },
      onStep() {
        calls.onStep += 1;
      },
      recoveryScopeId: "runtime-recovery-001",
    },
    storage: {
      backendId: BACKEND_ID,
      lifecycleBackend,
      publication,
      resolveArtifactPaths() {
        calls.storageResolver += 1;
        throw new Error("artifact paths must not be resolved");
      },
      resolveRestoreDestination() {
        calls.storageResolver += 1;
        throw new Error("restore destination must not be resolved");
      },
      resolveSourceOwnedRoot() {
        calls.storageResolver += 1;
        throw new Error("source root must not be resolved");
      },
    },
  };
  return {
    calls,
    collaborators: {
      imagePlanBinding,
      lifecycleBackend,
      publication,
      stoppedWriterCoordinator,
      supervisor,
    },
    options,
    pools,
  };
}

function assertNoActivity(fixture) {
  for (const pool of new Set(Object.values(fixture.pools))) {
    assert.deepEqual(pool.calls, { connect: 0, end: 0, query: 0 });
  }
  assertNoCollaboratorActivity(fixture);
}

function assertNoCollaboratorActivity(fixture) {
  assert.deepEqual(fixture.calls, {
    fleetGate: 0,
    image: 0,
    onStep: 0,
    planProvisioningGate: 0,
    provider: 0,
    publication: 0,
    storageResolver: 0,
    supervisor: 0,
  });
}

function restoreAdmission() {
  return {
    checkpoint: {
      artifactId: ARTIFACT_ID,
      backendId: BACKEND_ID,
      checkpointClass: "clean",
      checkpointId: CHECKPOINT_ID,
      codexSessionId: THREAD_ID,
      codexThreadId: THREAD_ID,
      contractVersion: 1,
      createdAt: "2026-08-11T10:00:00.000Z",
      imageDigest: IMAGE_DIGEST,
      sessionId: SESSION_ID,
      sourceFencingEpoch: "11",
      storageId: SOURCE_STORAGE_ID,
    },
    request: {
      backendId: BACKEND_ID,
      contractVersion: 1,
      fencingEpoch: "12",
      holderId: "restore-holder-001",
      leaseId: "restore-lease-001",
      operation: "restore",
      operationId: "restore-operation-001",
      sessionId: SESSION_ID,
      storageId: DESTINATION_STORAGE_ID,
      target: {
        artifactId: ARTIFACT_ID,
        checkpointId: CHECKPOINT_ID,
        kind: "checkpoint",
      },
    },
  };
}

test("runtime composition constructs a frozen branded restore-capable surface without I/O", () => {
  const fixture = createRuntimeFixture();
  assert.equal(
    isPostgresDetachedRestoreOperationalLeaseBudget(
      fixture.options.planRegistry.operationalLeaseBudget,
    ),
    true,
  );
  assert.equal(
    isPostgresDetachedRestorePublicationBinding(
      fixture.collaborators.publication,
    ),
    true,
  );
  const runtime = createPostgresDetachedRestoreRuntimeComposition(
    fixture.options,
  );

  assert.deepEqual(Reflect.ownKeys(runtime), [
    "backend",
    "bootstrap",
    "imagePlanReservations",
    "scheduler",
    "stablePlanProvisioning",
    "writerLaunch",
  ]);
  assert.equal(Object.isFrozen(runtime), true);
  assert.equal(isPostgresDetachedRestoreRuntimeComposition(runtime), true);
  assert.equal(
    isPostgresDetachedRestoreRuntimeComposition(
      Object.freeze({
        backend: runtime.backend,
        bootstrap: runtime.bootstrap,
        imagePlanReservations: runtime.imagePlanReservations,
        scheduler: runtime.scheduler,
        stablePlanProvisioning: runtime.stablePlanProvisioning,
        writerLaunch: runtime.writerLaunch,
      }),
    ),
    false,
  );

  exactKeys(runtime.backend, [
    "backendId",
    "capabilities",
    "captureCheckpoint",
    "contractVersion",
    "restoreCheckpoint",
  ]);
  assert.equal(Object.getPrototypeOf(runtime.backend), null);
  assert.equal(Object.isFrozen(runtime.backend), true);
  const checkpointProjection = assertCheckpointBackend(runtime.backend);
  exactKeys(checkpointProjection, [
    "backendId",
    "capabilities",
    "captureCheckpoint",
    "contractVersion",
    "restoreCheckpoint",
  ]);
  assert.equal(Object.getPrototypeOf(checkpointProjection), null);
  assert.equal(Object.isFrozen(checkpointProjection), true);
  assert.equal(checkpointProjection.backendId, runtime.backend.backendId);
  assert.deepEqual(
    { ...checkpointProjection.capabilities },
    { ...runtime.backend.capabilities },
  );
  assert.equal(
    checkpointProjection.contractVersion,
    runtime.backend.contractVersion,
  );
  for (const name of ["captureCheckpoint", "restoreCheckpoint"]) {
    assert.equal(typeof checkpointProjection[name], "function");
    assert.equal(Object.isFrozen(checkpointProjection[name]), true);
  }
  for (const validator of [
    assertStorageBackend,
    assertCheckpointCaptureReconciliationBackend,
    assertPreparedCheckpointCaptureBackend,
    assertRestoreAttachmentActivationBackend,
    assertRestoreAttachmentReconciliationBackend,
  ]) {
    assert.throws(() => validator(runtime.backend), storageBackendError);
  }
  assert.equal(runtime.backend.backendId, BACKEND_ID);
  assert.equal(runtime.backend.contractVersion, 1);
  exactKeys(runtime.backend.capabilities, [
    "atomicPointInTimeCheckpoint",
    "exclusiveWriterAttachment",
    "fencing",
    "normalDirectoryAttachment",
  ]);
  assert.equal(Object.getPrototypeOf(runtime.backend.capabilities), null);
  assert.equal(Object.isFrozen(runtime.backend.capabilities), true);
  assert.deepEqual(
    { ...runtime.backend.capabilities },
    { ...fixture.options.storage.lifecycleBackend.capabilities },
  );
  assert.deepEqual(
    { ...runtime.backend.capabilities },
    {
      atomicPointInTimeCheckpoint: true,
      exclusiveWriterAttachment: true,
      fencing: "epoch-enforced",
      normalDirectoryAttachment: true,
    },
  );
  for (const name of Reflect.ownKeys(runtime.backend)) {
    assert.deepEqual(Object.getOwnPropertyDescriptor(runtime.backend, name), {
      configurable: false,
      enumerable: true,
      value: runtime.backend[name],
      writable: false,
    });
  }
  for (const name of ["captureCheckpoint", "restoreCheckpoint"]) {
    assert.equal(typeof runtime.backend[name], "function");
    assert.equal(Object.isFrozen(runtime.backend[name]), true);
  }
  for (const name of [
    "captureReconciliationContractVersion",
    "destroySession",
    "detachAttachment",
    "forceFence",
    "physicalInvocationContractVersion",
    "prepareRestoreAttachment",
    "prepareWritableAttachment",
    "preparedCheckpointCaptureContractVersion",
    "provisionSession",
    "reconcileCheckpointCapture",
    "reconcileRestoreAttachment",
    "restoreAttachmentActivationContractVersion",
    "restoreAttachmentReconciliationContractVersion",
    "resumePreparedCheckpointCapture",
  ]) {
    assert.equal(name in runtime.backend, false);
  }
  assert.equal("foreground" in runtime, false);

  exactKeys(runtime.bootstrap, ["migrate"]);
  assert.equal(Object.getPrototypeOf(runtime.bootstrap), null);
  assert.equal(Object.isFrozen(runtime.bootstrap), true);
  assert.deepEqual(
    Object.getOwnPropertyDescriptor(runtime.bootstrap, "migrate"),
    {
      configurable: false,
      enumerable: true,
      value: runtime.bootstrap.migrate,
      writable: false,
    },
  );
  assert.equal(typeof runtime.bootstrap.migrate, "function");
  assert.equal(Object.isFrozen(runtime.bootstrap.migrate), true);
  for (const name of ["close", "pool", "runSerializable", "store"]) {
    assert.equal(name in runtime.bootstrap, false);
  }

  exactKeys(runtime.imagePlanReservations, ["prepareImageReservation"]);
  assert.equal(Object.getPrototypeOf(runtime.imagePlanReservations), null);
  assert.equal(Object.isFrozen(runtime.imagePlanReservations), true);
  assert.equal(
    Object.isFrozen(runtime.imagePlanReservations.prepareImageReservation),
    true,
  );

  assert.equal(isPostgresRestoreRecoveryScheduler(runtime.scheduler), true);
  exactKeys(runtime.scheduler, ["runStep", "start", "stop"]);
  assert.equal(Object.isFrozen(runtime.scheduler), true);

  exactKeys(runtime.stablePlanProvisioning, ["provisionStablePlan"]);
  assert.equal(Object.getPrototypeOf(runtime.stablePlanProvisioning), null);
  assert.equal(Object.isFrozen(runtime.stablePlanProvisioning), true);
  assert.deepEqual(
    Object.getOwnPropertyDescriptor(
      runtime.stablePlanProvisioning,
      "provisionStablePlan",
    ),
    {
      configurable: false,
      enumerable: true,
      value: runtime.stablePlanProvisioning.provisionStablePlan,
      writable: false,
    },
  );
  assert.equal(
    typeof runtime.stablePlanProvisioning.provisionStablePlan,
    "function",
  );
  assert.equal(
    Object.isFrozen(runtime.stablePlanProvisioning.provisionStablePlan),
    true,
  );
  for (const name of ["resolveStablePlan", "store"]) {
    assert.equal(name in runtime.stablePlanProvisioning, false);
  }

  exactKeys(runtime.writerLaunch, ["reconcileLaunchAttempt", "runLaunch"]);
  assert.equal(Object.getPrototypeOf(runtime.writerLaunch), null);
  assert.equal(Object.isFrozen(runtime.writerLaunch), true);
  for (const name of ["reconcileLaunchAttempt", "runLaunch"]) {
    const descriptor = Object.getOwnPropertyDescriptor(
      runtime.writerLaunch,
      name,
    );
    assert.deepEqual(descriptor, {
      configurable: false,
      enumerable: true,
      value: runtime.writerLaunch[name],
      writable: false,
    });
    assert.equal(typeof runtime.writerLaunch[name], "function");
    assert.equal(Object.isFrozen(runtime.writerLaunch[name]), true);
  }
  for (const name of [
    "launcher",
    "prepareLaunchIntent",
    "resolveStoppedWriter",
    "retirePreparedCapture",
    "retireStoppedWriter",
    "runPreparedLaunch",
    "stopWriterForCapture",
    "stopWriterForPreparedCapture",
  ]) {
    assert.equal(name in runtime.writerLaunch, false);
  }

  assert.throws(
    () =>
      runtime.scheduler.runStep({
        signal: new AbortController().signal,
      }),
    schedulerRequestError,
  );
  assertNoActivity(fixture);
});

test("runtime facets keep per-runtime identity and captured receivers", async () => {
  const firstFixture = createRuntimeFixture();
  const secondFixture = createRuntimeFixture();
  const first = createPostgresDetachedRestoreRuntimeComposition(
    firstFixture.options,
  );
  const second = createPostgresDetachedRestoreRuntimeComposition(
    secondFixture.options,
  );

  assert.notStrictEqual(first.writerLaunch, second.writerLaunch);
  assert.notStrictEqual(first.bootstrap, second.bootstrap);
  assert.notStrictEqual(first.bootstrap.migrate, second.bootstrap.migrate);
  assert.notStrictEqual(
    first.writerLaunch.reconcileLaunchAttempt,
    second.writerLaunch.reconcileLaunchAttempt,
  );
  assert.notStrictEqual(
    first.writerLaunch.runLaunch,
    second.writerLaunch.runLaunch,
  );
  assert.notStrictEqual(
    first.stablePlanProvisioning,
    second.stablePlanProvisioning,
  );
  assert.notStrictEqual(
    first.stablePlanProvisioning.provisionStablePlan,
    second.stablePlanProvisioning.provisionStablePlan,
  );
  assertNoActivity(firstFixture);
  assertNoActivity(secondFixture);

  const traps = {
    get: 0,
    getOwnPropertyDescriptor: 0,
    getPrototypeOf: 0,
  };
  const hostileReceiver = new Proxy(Object.create(null), {
    get() {
      traps.get += 1;
      throw new Error("caller receiver must not be read");
    },
    getOwnPropertyDescriptor() {
      traps.getOwnPropertyDescriptor += 1;
      throw new Error("caller receiver descriptor must not be read");
    },
    getPrototypeOf() {
      traps.getPrototypeOf += 1;
      throw new Error("caller receiver prototype must not be read");
    },
  });

  for (const name of ["reconcileLaunchAttempt", "runLaunch"]) {
    const pending = Reflect.apply(
      first.writerLaunch[name],
      hostileReceiver,
      [],
    );
    assert(pending instanceof Promise);
    await assert.rejects(pending, launcherRequestError);
  }
  const invalidProvision = Reflect.apply(
    first.stablePlanProvisioning.provisionStablePlan,
    hostileReceiver,
    [],
  );
  assert(invalidProvision instanceof Promise);
  await assert.rejects(invalidProvision, stablePlanRegistryRequestError);
  assert.deepEqual(traps, {
    get: 0,
    getOwnPropertyDescriptor: 0,
    getPrototypeOf: 0,
  });
  assertNoActivity(firstFixture);
  assertNoActivity(secondFixture);
});

test("public backend methods reject wrong, detached, and cloned receivers without I/O", () => {
  const firstFixture = createRuntimeFixture();
  const secondFixture = createRuntimeFixture();
  const first = createPostgresDetachedRestoreRuntimeComposition(
    firstFixture.options,
  );
  const second = createPostgresDetachedRestoreRuntimeComposition(
    secondFixture.options,
  );
  const clone = Object.freeze(
    Object.create(
      null,
      Object.getOwnPropertyDescriptors(first.backend),
    ),
  );
  const proxy = new Proxy(first.backend, {
    get() {
      assert.fail("wrong receiver Proxy must not be read");
    },
    getOwnPropertyDescriptor() {
      assert.fail("wrong receiver Proxy descriptor must not be read");
    },
    getPrototypeOf() {
      assert.fail("wrong receiver Proxy prototype must not be read");
    },
  });

  assert.notStrictEqual(first.backend, second.backend);
  assert.notStrictEqual(first.backend, clone);
  assert.deepEqual(Reflect.ownKeys(clone), Reflect.ownKeys(first.backend));
  for (const name of ["captureCheckpoint", "restoreCheckpoint"]) {
    const detached = first.backend[name];
    assert.throws(() => detached({}), TypeError);
    assert.throws(
      () => Reflect.apply(first.backend[name], second.backend, [{}]),
      TypeError,
    );
    assert.throws(
      () => Reflect.apply(first.backend[name], clone, [{}]),
      TypeError,
    );
    assert.throws(
      () => Reflect.apply(first.backend[name], proxy, [{}]),
      TypeError,
    );
  }
  assertNoActivity(firstFixture);
  assertNoActivity(secondFixture);
});

test("runtime composition rejects every pairwise pool alias before I/O", async (t) => {
  const roles = [
    "authority",
    "operation",
    "foregroundLifecycle",
    "recoveryLifecycle",
  ];
  const aliases = [];
  for (let left = 0; left < roles.length; left += 1) {
    for (let right = left + 1; right < roles.length; right += 1) {
      aliases.push([roles[left], roles[right]]);
    }
  }
  assert.equal(aliases.length, 6);

  for (const [left, right] of aliases) {
    await t.test(`${left} and ${right}`, () => {
      const fixture = createRuntimeFixture();
      fixture.pools[right] = fixture.pools[left];
      assert.throws(
        () => createPostgresDetachedRestoreRuntimeComposition(fixture.options),
        runtimeOptionError,
      );
      assertNoActivity(fixture);
    });
  }
});

test("public backend delegates restore through the private foreground composition", async () => {
  const fixture = createRuntimeFixture();
  const runtime = createPostgresDetachedRestoreRuntimeComposition(
    fixture.options,
  );

  const restore = runtime.backend.restoreCheckpoint(restoreAdmission());
  assert(restore instanceof Promise);
  await assert.rejects(
    restore,
    (error) => {
      assert(error instanceof StoppedDirectoryBackendError);
      assert.equal(error.code, "stopped_directory_backend_outcome_uncertain");
      assert.equal(error.retryable, false);
      assert.equal(Object.isFrozen(error), true);
      return true;
    },
  );

  assert.deepEqual(fixture.pools.foregroundLifecycle.calls, {
    connect: 1,
    end: 0,
    query: 0,
  });
  for (const name of ["authority", "operation", "recoveryLifecycle"]) {
    assert.deepEqual(fixture.pools[name].calls, {
      connect: 0,
      end: 0,
      query: 0,
    });
  }
  assertNoCollaboratorActivity(fixture);
});

test("public backend retains the checkpoint capture authority without exposing reconciliation", async () => {
  const fixture = createRuntimeFixture();
  const runtime = createPostgresDetachedRestoreRuntimeComposition(
    fixture.options,
  );

  const capture = runtime.backend.captureCheckpoint({});
  assert(capture instanceof Promise);
  await assert.rejects(capture, (error) => {
    assert(error instanceof StoppedDirectoryBackendError);
    assert.equal(error.code, "invalid_stopped_directory_backend_request");
    assert.equal(error.retryable, false);
    assert.equal(Object.isFrozen(error), true);
    return true;
  });
  assert.equal("reconcileCheckpointCapture" in runtime.backend, false);
  assertNoActivity(fixture);
});

test("runtime leaves lifecycle and pool shutdown ownership with the caller", async () => {
  const fixture = createRuntimeFixture();
  const runtime = createPostgresDetachedRestoreRuntimeComposition(
    fixture.options,
  );

  for (const name of [
    "close",
    "migrate",
    "planRegistry",
    "resolveStablePlan",
    "runner",
    "shutdown",
    "start",
    "store",
  ]) {
    assert.equal(name in runtime, false);
  }
  assertNoActivity(fixture);

  const firstStop = runtime.scheduler.stop();
  assert.strictEqual(runtime.scheduler.stop(), firstStop);
  const stopped = await firstStop;
  exactKeys(stopped, ["status"]);
  assert.equal(stopped.status, "stopped");
  assert.equal(Object.isFrozen(stopped), true);
  assertNoActivity(fixture);
});

test("runtime composition rejects hostile options without leaking hostile behavior", async (t) => {
  await t.test("forged operational lease budget", () => {
    const fixture = createRuntimeFixture();
    assert.throws(
      () =>
        createPostgresDetachedRestoreRuntimeComposition({
          ...fixture.options,
          planRegistry: {
            ...fixture.options.planRegistry,
            operationalLeaseBudget: Object.freeze({}),
          },
        }),
      runtimeOptionError,
    );
    assertNoActivity(fixture);
  });

  await t.test("forged publication duck", () => {
    const fixture = createRuntimeFixture();
    const publication = Object.freeze(
      Object.fromEntries(
        PHYSICAL_PUBLICATION_METHODS.map((method) => [
          method,
          Object.freeze(async function forgedPublicationMethod() {
            throw new Error("forged publication must not run");
          }),
        ]),
      ),
    );
    assert.equal(isPostgresDetachedRestorePublicationBinding(publication), false);
    assert.throws(
      () =>
        createPostgresDetachedRestoreRuntimeComposition({
          ...fixture.options,
          storage: { ...fixture.options.storage, publication },
        }),
      runtimeOptionError,
    );
    assertNoActivity(fixture);
  });

  await t.test("extra top-level field", () => {
    const fixture = createRuntimeFixture();
    assert.throws(
      () =>
        createPostgresDetachedRestoreRuntimeComposition({
          ...fixture.options,
          extra: true,
        }),
      runtimeOptionError,
    );
    assertNoActivity(fixture);
  });

  await t.test("legacy external stable-plan resolver", () => {
    const fixture = createRuntimeFixture();
    assert.throws(
      () =>
        createPostgresDetachedRestoreRuntimeComposition({
          ...fixture.options,
          foreground: {
            ...fixture.options.foreground,
            resolveStablePlan() {
              throw new Error("legacy stable-plan resolver must not run");
            },
          },
        }),
      runtimeOptionError,
    );
    assertNoActivity(fixture);
  });

  await t.test("missing stable-plan registry options", () => {
    const fixture = createRuntimeFixture();
    const { planRegistry: ignored, ...options } = fixture.options;
    void ignored;
    assert.throws(
      () => createPostgresDetachedRestoreRuntimeComposition(options),
      runtimeOptionError,
    );
    assertNoActivity(fixture);
  });

  await t.test("nested accessor", () => {
    const fixture = createRuntimeFixture();
    let getterCalls = 0;
    const { backendId: ignored, ...storage } = fixture.options.storage;
    void ignored;
    Object.defineProperty(storage, "backendId", {
      enumerable: true,
      get() {
        getterCalls += 1;
        throw new Error("backendId getter must not run");
      },
    });
    assert.throws(
      () =>
        createPostgresDetachedRestoreRuntimeComposition({
          ...fixture.options,
          storage,
        }),
      runtimeOptionError,
    );
    assert.equal(getterCalls, 0);
    assertNoActivity(fixture);
  });

  await t.test("proxied nested record", () => {
    const fixture = createRuntimeFixture();
    let prototypeTrapCalls = 0;
    const storage = new Proxy(fixture.options.storage, {
      getPrototypeOf() {
        prototypeTrapCalls += 1;
        throw new Error("proxy prototype trap must not run");
      },
    });
    assert.throws(
      () =>
        createPostgresDetachedRestoreRuntimeComposition({
          ...fixture.options,
          storage,
        }),
      runtimeOptionError,
    );
    assert.equal(prototypeTrapCalls, 0);
    assertNoActivity(fixture);
  });

  const brandedPrototypeCases = [
    {
      apply(options, value) {
        return {
          ...options,
          launch: { ...options.launch, imagePlanBinding: value },
        };
      },
      name: "image-plan binding receiver with a Proxy prototype",
      target: createRuntimeFixture().collaborators.imagePlanBinding,
    },
    {
      apply(options, value) {
        return {
          ...options,
          launch: { ...options.launch, stoppedWriterCoordinator: value },
        };
      },
      name: "stopped-writer coordinator receiver with a Proxy prototype",
      target: StoppedWriterCapabilityCoordinator.prototype,
    },
    {
      apply(options, value) {
        return {
          ...options,
          storage: { ...options.storage, publication: value },
        };
      },
      name: "publication receiver with a Proxy prototype",
      target: StoppedDirectoryPublication.prototype,
    },
  ];
  for (const hostile of brandedPrototypeCases) {
    await t.test(hostile.name, () => {
      const fixture = createRuntimeFixture();
      const traps = {
        get: 0,
        getOwnPropertyDescriptor: 0,
        getPrototypeOf: 0,
      };
      const value = ordinaryReceiverWithProxyPrototype(hostile.target, traps);
      assert.throws(
        () =>
          createPostgresDetachedRestoreRuntimeComposition(
            hostile.apply(fixture.options, value),
          ),
        runtimeOptionError,
      );
      assert.deepEqual(traps, {
        get: 0,
        getOwnPropertyDescriptor: 0,
        getPrototypeOf: 0,
      });
      assertNoActivity(fixture);
    });
  }

  await t.test("raw pool with a Proxy prototype", () => {
    const fixture = createRuntimeFixture();
    let prototypeTrapCalls = 0;
    const prototype = new Proxy(
      {
        connect() {
          throw new Error("prototype pool connect must not run");
        },
      },
      {
        get() {
          prototypeTrapCalls += 1;
          throw new Error("pool prototype get trap must not run");
        },
        getOwnPropertyDescriptor() {
          prototypeTrapCalls += 1;
          throw new Error(
            "pool prototype descriptor trap must not run",
          );
        },
        getPrototypeOf() {
          prototypeTrapCalls += 1;
          throw new Error("pool prototype traversal trap must not run");
        },
        ownKeys() {
          prototypeTrapCalls += 1;
          throw new Error("pool prototype ownKeys trap must not run");
        },
      },
    );
    const pool = Object.create(
      prototype,
      Object.getOwnPropertyDescriptors(noIoRawPool()),
    );
    fixture.pools.authority = pool;
    assert.throws(
      () => createPostgresDetachedRestoreRuntimeComposition(fixture.options),
      runtimeOptionError,
    );
    assert.equal(prototypeTrapCalls, 0);
    assertNoActivity(fixture);
  });

  await t.test("raw pool connect accessor", () => {
    const fixture = createRuntimeFixture();
    let getterCalls = 0;
    const pool = noIoRawPool();
    Object.defineProperty(pool, "connect", {
      enumerable: true,
      get() {
        getterCalls += 1;
        throw new Error("pool connect getter must not run");
      },
    });
    fixture.pools.operation = pool;
    assert.throws(
      () => createPostgresDetachedRestoreRuntimeComposition(fixture.options),
      runtimeOptionError,
    );
    assert.equal(getterCalls, 0);
    assertNoActivity(fixture);
  });

  await t.test("lifecycle backend consumed-field accessor", () => {
    const fixture = createRuntimeFixture();
    let getterCalls = 0;
    const lifecycleBackend = cloneWithAccessor(
      fixture.options.storage.lifecycleBackend,
      "contractVersion",
      () => {
        getterCalls += 1;
        throw new Error("lifecycle contractVersion getter must not run");
      },
    );
    assert.throws(
      () =>
        createPostgresDetachedRestoreRuntimeComposition({
          ...fixture.options,
          storage: {
            ...fixture.options.storage,
            lifecycleBackend,
          },
        }),
      runtimeOptionError,
    );
    assert.equal(getterCalls, 0);
    assertNoActivity(fixture);
  });

  await t.test("lifecycle backend restore reconciliation method", () => {
    const fixture = createRuntimeFixture();
    const lifecycleBackend = cloneWithDataProperty(
      fixture.options.storage.lifecycleBackend,
      "reconcileRestoreAttachment",
      undefined,
    );
    assert.throws(
      () =>
        createPostgresDetachedRestoreRuntimeComposition({
          ...fixture.options,
          storage: {
            ...fixture.options.storage,
            lifecycleBackend,
          },
        }),
      runtimeOptionError,
    );
    assertNoActivity(fixture);
  });

  await t.test("lifecycle backend capabilities Proxy", () => {
    const fixture = createRuntimeFixture();
    let capabilityTrapCalls = 0;
    const capabilities = new Proxy(
      fixture.options.storage.lifecycleBackend.capabilities,
      {
        get() {
          capabilityTrapCalls += 1;
          throw new Error("capabilities get trap must not run");
        },
        getOwnPropertyDescriptor() {
          capabilityTrapCalls += 1;
          throw new Error("capabilities descriptor trap must not run");
        },
        getPrototypeOf() {
          capabilityTrapCalls += 1;
          throw new Error("capabilities prototype trap must not run");
        },
        ownKeys() {
          capabilityTrapCalls += 1;
          throw new Error("capabilities ownKeys trap must not run");
        },
      },
    );
    const lifecycleBackend = cloneWithDataProperty(
      fixture.options.storage.lifecycleBackend,
      "capabilities",
      capabilities,
    );
    assert.throws(
      () =>
        createPostgresDetachedRestoreRuntimeComposition({
          ...fixture.options,
          storage: {
            ...fixture.options.storage,
            lifecycleBackend,
          },
        }),
      runtimeOptionError,
    );
    assert.equal(capabilityTrapCalls, 0);
    assertNoActivity(fixture);
  });

  await t.test("lifecycle backend capabilities accessor", () => {
    const fixture = createRuntimeFixture();
    let getterCalls = 0;
    const capabilities = cloneWithAccessor(
      fixture.options.storage.lifecycleBackend.capabilities,
      "fencing",
      () => {
        getterCalls += 1;
        throw new Error("capabilities fencing getter must not run");
      },
    );
    const lifecycleBackend = cloneWithDataProperty(
      fixture.options.storage.lifecycleBackend,
      "capabilities",
      capabilities,
    );
    assert.throws(
      () =>
        createPostgresDetachedRestoreRuntimeComposition({
          ...fixture.options,
          storage: {
            ...fixture.options.storage,
            lifecycleBackend,
          },
        }),
      runtimeOptionError,
    );
    assert.equal(getterCalls, 0);
    assertNoActivity(fixture);
  });

  await t.test("downstream constructor error", () => {
    const fixture = createRuntimeFixture();
    assert.throws(
      () =>
        createPostgresDetachedRestoreRuntimeComposition({
          ...fixture.options,
          authority: {
            ...fixture.options.authority,
            maxTransactionAttempts: 0,
          },
        }),
      runtimeOptionError,
    );
    assertNoActivity(fixture);
  });

  await t.test("externally constructed lookalike error", () => {
    const fixture = createRuntimeFixture();
    const external = new PostgresDetachedRestoreRuntimeCompositionError(
      RUNTIME_OPTION_ERROR,
    );
    let actual;
    assert.throws(
      () => createPostgresDetachedRestoreRuntimeComposition(external),
      (error) => {
        actual = error;
        return runtimeOptionError(error);
      },
    );
    assert.notStrictEqual(actual, external);
    assertNoActivity(fixture);
  });
});
