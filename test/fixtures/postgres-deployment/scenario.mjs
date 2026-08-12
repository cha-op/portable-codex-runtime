import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";

import { FilesystemOperationJournal } from "../../../src/filesystem-operation-journal.mjs";
import {
  PostgresDetachedRestoreImagePlanBindingError,
  POSTGRES_DETACHED_RESTORE_IMAGE_PLAN_PROVIDER_CONTRACT_VERSION,
} from "../../../src/postgres-detached-restore-image-plan-binding.mjs";
import {
  createPostgresDetachedRestorePlan,
} from "../../../src/postgres-detached-restore-plan.mjs";
import {
  POSTGRES_DETACHED_RESTORE_STABLE_PLAN_PROVISIONING_CONFIRMED,
} from "../../../src/postgres-detached-restore-stable-plan-registry.mjs";
import {
  RESTORE_ATTACHMENT_ACTIVATION_CONTRACT_VERSION,
  RESTORE_ATTACHMENT_RECONCILIATION_CONTRACT_VERSION,
  createSessionManifest,
} from "../../../src/session-storage-contracts.mjs";
import { StoppedDirectoryPublication } from "../../../src/stopped-directory-publication.mjs";
import { StoppedWriterCapabilityCoordinator } from "../../../src/stopped-writer-capability.mjs";
import {
  PostgresDetachedRestoreDeploymentError,
  createPostgresDetachedRestoreDeployment,
  isPostgresDetachedRestoreDeployment,
} from "../../../src/postgres-detached-restore-deployment.mjs";
import {
  configureFakePg,
  fakePgState,
  holdFakePoolEnd,
  holdFakeTopology,
  isFakePgQueryResult,
} from "./fake-pg.mjs";

const SYNTHETIC_PASSWORD_ID = "access-a";
const SYNTHETIC_PASSWORD = "codex_synth_v1_access_a";
const BACKEND_ID = "deployment-capture-only-backend";
const SESSION_ID = "019f8800-0000-7000-8000-000000000001";
const THREAD_ID = "019f8800-0000-7000-8000-000000000002";
const IMAGE_DIGEST = `sha256:${"a".repeat(64)}`;
const OCI_MANIFEST_MEDIA_TYPE =
  "application/vnd.oci.image.manifest.v1+json";
const OCI_CONFIG_MEDIA_TYPE =
  "application/vnd.oci.image.config.v1+json";
const MIGRATION_URLS = Object.freeze([
  new URL("../../../migrations/authority/001-session-authority.sql", import.meta.url),
  new URL("../../../migrations/authority/002-restore-destination-generations.sql", import.meta.url),
  new URL("../../../migrations/authority/003-operation-id-registry.sql", import.meta.url),
  new URL("../../../migrations/authority/004-restore-attachment-activation.sql", import.meta.url),
  new URL("../../../migrations/authority/005-restore-recovery-cursors.sql", import.meta.url),
  new URL("../../../migrations/authority/006-writer-stop-capture-handoff.sql", import.meta.url),
  new URL("../../../migrations/authority/007-detached-restore-stable-plans.sql", import.meta.url),
]);
const LIFECYCLE_PHYSICAL_METHODS = Object.freeze([
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
const PUBLICATION_PHYSICAL_METHODS = Object.freeze([
  "publishFreshCheckpointArtifact",
  "publishRestoreDestination",
  "verifyCommittedCheckpointArtifact",
  "verifyCommittedRestoreDestination",
]);
const SUPERVISOR_PHYSICAL_METHODS = Object.freeze([
  "launchWriter",
  "reconcileWriterLaunch",
  "stopWriter",
]);

function exactKeys(value, expected) {
  assert.deepEqual(Reflect.ownKeys(value).sort(), [...expected].sort());
}

function safeProviderCarrier(value) {
  return Object.freeze(Object.assign(Object.create(null), value));
}

function assertStatusReceipt(value, expected) {
  exactKeys(value, ["status"]);
  assert.equal(Object.getPrototypeOf(value), null);
  assert.equal(Object.isFrozen(value), true);
  assert.equal(value.status, expected);
}

function restoreAdmission(operationId = "deployment-restore-operation-001") {
  return {
    checkpoint: {
      artifactId: "deployment-source-artifact-001",
      backendId: BACKEND_ID,
      checkpointClass: "clean",
      checkpointId: "deployment-source-checkpoint-001",
      codexSessionId: THREAD_ID,
      codexThreadId: THREAD_ID,
      contractVersion: 1,
      createdAt: "2026-08-11T22:00:00.000Z",
      imageDigest: IMAGE_DIGEST,
      sessionId: SESSION_ID,
      sourceFencingEpoch: "41",
      storageId: "deployment-source-storage-001",
    },
    request: {
      backendId: BACKEND_ID,
      contractVersion: 1,
      fencingEpoch: "42",
      holderId: "deployment-restore-holder-001",
      leaseId: "deployment-restore-lease-001",
      operation: "restore",
      operationId,
      sessionId: SESSION_ID,
      storageId: "deployment-destination-storage-001",
      target: {
        artifactId: "deployment-source-artifact-001",
        checkpointId: "deployment-source-checkpoint-001",
        kind: "checkpoint",
      },
    },
  };
}

function stablePlan(admission, leaseDurationMilliseconds = 600_000) {
  return createPostgresDetachedRestorePlan({
    request: admission.request,
    plan: {
      captureCreatedAt: "2026-08-11T23:00:00.000Z",
      destinationDirectory: "/var/lib/portable-codex/restores/deployment-001",
      destinationOwnedRoot: "/var/lib/portable-codex/restores",
      detachMode: "release",
      holderId: "deployment-restored-writer-001",
      imagePlanId: "deployment-image-plan-001",
      leaseDurationMilliseconds,
      sourceArtifactDirectory:
        "/var/lib/portable-codex/artifacts/deployment-source-001",
      sourceArtifactOwnedRoot: "/var/lib/portable-codex/artifacts",
    },
  });
}

function digest(bytes) {
  return `sha256:${createHash("sha256").update(bytes).digest("hex")}`;
}

function imagePlanFixture() {
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
  const descriptorBytes = Buffer.from(
    JSON.stringify({
      config: {
        digest: digest(configBytes),
        mediaType: OCI_CONFIG_MEDIA_TYPE,
        size: configBytes.byteLength,
      },
      layers: [
        {
          digest: `sha256:${"c".repeat(64)}`,
          mediaType: "application/vnd.oci.image.layer.v1.tar+gzip",
          size: 1024,
        },
      ],
      mediaType: OCI_MANIFEST_MEDIA_TYPE,
      schemaVersion: 2,
    }),
    "utf8",
  );
  const descriptor = Object.freeze({
    bytes: descriptorBytes,
    digest: digest(descriptorBytes),
    mediaType: OCI_MANIFEST_MEDIA_TYPE,
    size: descriptorBytes.byteLength,
  });
  return Object.freeze({
    configBytes,
    descriptor,
    sessionManifest: createSessionManifest({
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
        imageDigest: descriptor.digest,
        imageMediaType: OCI_MANIFEST_MEDIA_TYPE,
        platform: "linux/arm64",
      },
    }),
  });
}

function deferred() {
  let resolve;
  const promise = new Promise((resolvePromise) => {
    resolve = resolvePromise;
  });
  return { promise, resolve };
}

function physicalPolicy(
  deadlineMilliseconds = 30_000,
  settlementGraceMilliseconds = 1_000,
) {
  return {
    deadlineMilliseconds,
    settlementGraceMilliseconds,
  };
}

function physicalPolicyGroup(methods) {
  return Object.fromEntries(methods.map((method) => [method, physicalPolicy()]));
}

function physicalPolicyFixture() {
  const fixture = {
    imagePlanProviderSettlement: physicalPolicyGroup([
      "inspectCodex",
      "resolveImagePlan",
    ]),
    lifecycleBackendSettlement: physicalPolicyGroup(
      LIFECYCLE_PHYSICAL_METHODS,
    ),
    publicationSettlement: physicalPolicyGroup(PUBLICATION_PHYSICAL_METHODS),
    resolveRestoreDestinationSettlement: physicalPolicy(),
    supervisorSettlement: physicalPolicyGroup(SUPERVISOR_PHYSICAL_METHODS),
  };
  assert.equal(
    Object.values(fixture)
      .flatMap((group) =>
        "deadlineMilliseconds" in group ? [group] : Object.values(group),
      ).length,
    19,
  );
  return fixture;
}

function deploymentPhysicalPolicyLeaves(options) {
  return [
    ...Object.values(options.runtime.launch.imagePlanProviderSettlement),
    ...Object.values(options.runtime.launch.supervisorSettlement),
    ...Object.values(options.runtime.storage.lifecycleBackendSettlement),
    ...Object.values(options.runtime.storage.publicationSettlement),
    options.runtime.storage.resolveRestoreDestinationSettlement,
  ];
}

function assertPhysicalInvocationContext(context, seen) {
  exactKeys(context, ["contractVersion", "invocation", "signal"]);
  assert.equal(Object.getPrototypeOf(context), null);
  assert.equal(Object.isFrozen(context), true);
  assert.equal(context.contractVersion, 1);
  assert.equal(Object.getPrototypeOf(context.invocation), null);
  assert.equal(Object.isFrozen(context.invocation), true);
  assert.deepEqual(Reflect.ownKeys(context.invocation), []);
  assert(context.signal instanceof AbortSignal);
  assert.equal(context.signal.aborted, false);
  assert.equal(seen.has(context), false);
  assert.equal(seen.has(context.invocation), false);
  seen.add(context);
  seen.add(context.invocation);
}

function assertSupervisorPhysicalRequest(input, seen) {
  assert.equal(Object.getPrototypeOf(input), null);
  assert.equal(Object.isFrozen(input), true);
  assert.equal(input.contractVersion, 2);
  assert.equal(Object.getPrototypeOf(input.invocation), null);
  assert.equal(Object.isFrozen(input.invocation), true);
  assert.deepEqual(Reflect.ownKeys(input.invocation), []);
  assert(input.signal instanceof AbortSignal);
  assert.equal(input.signal.aborted, false);
  assert.equal(seen.has(input.invocation), false);
  assert.equal(seen.has(input.signal), false);
  seen.add(input.invocation);
  seen.add(input.signal);
}

function unexpected(calls, key) {
  return async function unexpectedEffect() {
    calls[key] += 1;
    throw new Error(`${key} must not run`);
  };
}

function lifecycleBackend(calls, controls) {
  const methods = {};
  for (const method of LIFECYCLE_PHYSICAL_METHODS) {
    methods[method] = async function lifecyclePhysicalMethod(
      input,
      physicalContext,
    ) {
      calls.provider += 1;
      assertPhysicalInvocationContext(
        physicalContext,
        controls.physicalInvocationContexts,
      );
      controls.lifecycleContexts.push({ input, method, physicalContext });
      throw new Error(`${method} must not run`);
    };
  }
  return Object.freeze({
    backendId: BACKEND_ID,
    capabilities: Object.freeze({
      atomicPointInTimeCheckpoint: true,
      exclusiveWriterAttachment: true,
      fencing: "epoch-enforced",
      normalDirectoryAttachment: true,
    }),
    contractVersion: 1,
    physicalInvocationContractVersion: 1,
    restoreAttachmentActivationContractVersion:
      RESTORE_ATTACHMENT_ACTIVATION_CONTRACT_VERSION,
    restoreAttachmentReconciliationContractVersion:
      RESTORE_ATTACHMENT_RECONCILIATION_CONTRACT_VERSION,
    ...methods,
  });
}

function publication(calls) {
  const effect = unexpected(calls, "publication");
  return new StoppedDirectoryPublication({
    acquireLock: effect,
    inspectFilesystem: effect,
    inspectOwnedRootAcl: effect,
    inspectOwnedRootAncestorAcl: effect,
    inspectPersistentObjectIdentity: effect,
    journal: new FilesystemOperationJournal({
      acquireLock: effect,
      directory: "/var/lib/portable-codex/deployment-test-journal",
      inspectAncestorAcl: effect,
      inspectDirectoryAcl: effect,
      inspectTemporaryRecord: effect,
      syncDirectory: effect,
    }),
    listMountPoints: effect,
  });
}

function validOptions() {
  const calls = {
    fleetGate: 0,
    image: 0,
    onStep: 0,
    planGate: 0,
    provider: 0,
    publication: 0,
    resolver: 0,
    supervisor: 0,
  };
  const controls = {
    imagePlanBlock: null,
    imagePlanEntered: null,
    imagePlanFixture: imagePlanFixture(),
    imagePlanInspectorOverride: null,
    imagePlanResolverOverride: null,
    lifecycleContexts: [],
    physicalInvocationContexts: new Set(),
    planGateOverride: null,
    resolverInputs: [],
    resolveRestoreDestinationOverride: null,
    supervisorContexts: [],
    supervisorLaunchOverride: null,
    supervisorReconcileOverride: null,
  };
  const policies = physicalPolicyFixture();
  const options = {
    postgres: {
      applicationNamePrefix: "pcr-deployment-test",
      database: "portable_codex_runtime",
      host: "db.example.test",
      password: SYNTHETIC_PASSWORD,
      poolMaximums: {
        authority: 2,
        foregroundLifecycle: 1,
        operation: 1,
        recoveryLifecycle: 1,
      },
      port: 5432,
      timeouts: {
        connectionMilliseconds: 2_000,
        idleClientMilliseconds: 30_000,
        idleTransactionMilliseconds: 10_000,
        lockMilliseconds: 5_000,
        queryMilliseconds: 20_000,
        statementMilliseconds: 15_000,
      },
      tls: {
        ca: null,
        cert: null,
        key: null,
        mode: "disable",
        serverName: null,
      },
      user: "portable_codex_runtime",
    },
    runtime: {
      authority: {
        maxTransactionAttempts: 1,
        restoreAttachmentActivationV2FleetCompatible: true,
        restoreAttachmentActivationV2GenerationPredecessorFleetCompatible:
          true,
        restoreGenerationV2FleetCompatible: true,
        writerLaunchStopV3FleetCompatible: true,
      },
      foreground: {
        fleetCapabilityGate() {
          calls.fleetGate += 1;
          throw new Error("foreground must remain closed");
        },
      },
      launch: {
        imagePlanProvider: Object.freeze({
          contractVersion:
            POSTGRES_DETACHED_RESTORE_IMAGE_PLAN_PROVIDER_CONTRACT_VERSION,
          imagePlanProviderId: "deployment-image-provider-001",
          async inspectCodex(input) {
            calls.image += 1;
            assertImageProviderRequest(input, "inspect");
            if (controls.imagePlanInspectorOverride !== null) {
              return controls.imagePlanInspectorOverride(input);
            }
            return safeProviderCarrier({
              codexBinaryPath: "/opt/portable-codex/bin/codex",
              codexBinarySha256: "b".repeat(64),
              codexVersion: "codex-cli 0.144.1",
            });
          },
          async resolveImagePlan(input) {
            calls.image += 1;
            assertImageProviderRequest(input, "resolve");
            if (controls.imagePlanResolverOverride !== null) {
              return controls.imagePlanResolverOverride(input);
            }
            controls.imagePlanEntered?.resolve();
            await controls.imagePlanBlock?.promise;
            return safeProviderCarrier({
              configBytes: controls.imagePlanFixture.configBytes,
              descriptor: controls.imagePlanFixture.descriptor,
            });
          },
        }),
        imagePlanProviderSettlement: policies.imagePlanProviderSettlement,
        stoppedWriterCoordinator: new StoppedWriterCapabilityCoordinator(),
        supervisor: Object.freeze({
          contractVersion: 2,
          async launchWriter(input) {
            calls.supervisor += 1;
            assert.equal(arguments.length, 1);
            assertSupervisorPhysicalRequest(
              input,
              controls.physicalInvocationContexts,
            );
            controls.supervisorContexts.push({
              input,
              method: "launchWriter",
            });
            if (controls.supervisorLaunchOverride !== null) {
              return controls.supervisorLaunchOverride(input);
            }
            throw new Error("supervisor launch must not run");
          },
          async reconcileWriterLaunch(input) {
            calls.supervisor += 1;
            assert.equal(arguments.length, 1);
            assertSupervisorPhysicalRequest(
              input,
              controls.physicalInvocationContexts,
            );
            controls.supervisorContexts.push({
              input,
              method: "reconcileWriterLaunch",
            });
            if (controls.supervisorReconcileOverride !== null) {
              return controls.supervisorReconcileOverride(input);
            }
            throw new Error("supervisor reconcile must not run");
          },
          supervisorId: "deployment-supervisor-001",
        }),
        supervisorSettlement: policies.supervisorSettlement,
      },
      operationalLease: {
        databaseRequestMilliseconds: 30_000,
        leaseDurationMilliseconds: 600_000,
        safetyMarginMilliseconds: 30_000,
      },
      planRegistry: {
        provisioningFleetCapabilityGate(input) {
          calls.planGate += 1;
          if (controls.planGateOverride !== null) {
            return controls.planGateOverride(input);
          }
          throw new Error("plan gate must not run");
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
        recoveryScopeId: "deployment-recovery-001",
      },
      storage: {
        backendId: BACKEND_ID,
        lifecycleBackend: lifecycleBackend(calls, controls),
        lifecycleBackendSettlement: policies.lifecycleBackendSettlement,
        publication: publication(calls),
        publicationSettlement: policies.publicationSettlement,
        resolveArtifactPaths() {
          calls.resolver += 1;
          throw new Error("artifact resolver must not run");
        },
        async resolveRestoreDestination(input) {
          calls.resolver += 1;
          exactKeys(input, [
            "attachment",
            "checkpoint",
            "contractVersion",
            "invocation",
            "request",
            "signal",
          ]);
          assert.equal(Object.getPrototypeOf(input), null);
          assert.equal(Object.isFrozen(input), true);
          assert.equal(input.contractVersion, 1);
          assert.equal(Object.getPrototypeOf(input.invocation), null);
          assert.equal(Object.isFrozen(input.invocation), true);
          assert.deepEqual(Reflect.ownKeys(input.invocation), []);
          assert(input.signal instanceof AbortSignal);
          assert.equal(input.signal.aborted, false);
          assert.equal(controls.resolverInputs.includes(input), false);
          controls.resolverInputs.push(input);
          if (controls.resolveRestoreDestinationOverride !== null) {
            return controls.resolveRestoreDestinationOverride(input);
          }
          throw new Error("destination resolver must not run");
        },
        resolveRestoreDestinationContractVersion: 1,
        resolveRestoreDestinationSettlement:
          policies.resolveRestoreDestinationSettlement,
        resolveSourceOwnedRoot() {
          calls.resolver += 1;
          throw new Error("source resolver must not run");
        },
      },
    },
  };
  return { calls, controls, options };
}

function assertNoConstructionEffects(calls, poolCount) {
  assert.equal(fakePgState().pools.length, poolCount);
  assert.deepEqual(calls, {
    fleetGate: 0,
    image: 0,
    onStep: 0,
    planGate: 0,
    provider: 0,
    publication: 0,
    resolver: 0,
    supervisor: 0,
  });
}

function assertOperationalLeaseOptionRejection(options, calls) {
  const poolCount = fakePgState().pools.length;
  assert.equal(poolCount, 0);
  assert.throws(
    () => createPostgresDetachedRestoreDeployment(options),
    (error) =>
      assertDeploymentError(
        error,
        "invalid_postgres_detached_restore_deployment_options",
      ),
  );
  assertNoConstructionEffects(calls, poolCount);
}

function assertDeploymentError(error, code) {
  assert(error instanceof PostgresDetachedRestoreDeploymentError);
  assert.equal(error.code, code);
  assert.equal(error.retryable, false);
  assert.equal(Object.isFrozen(error), true);
  assert.equal("cause" in error, false);
  return true;
}

function requestError(error) {
  return assertDeploymentError(
    error,
    "invalid_postgres_detached_restore_deployment_request",
  );
}

function outcomeError(error) {
  return assertDeploymentError(
    error,
    "postgres_detached_restore_deployment_outcome_uncertain",
  );
}

function imageResolutionError(error) {
  assert(error instanceof PostgresDetachedRestoreImagePlanBindingError);
  assert.equal(
    error.code,
    "postgres_detached_restore_image_plan_resolution_uncertain",
  );
  assert.equal(error.retryable, false);
  assert.equal(Object.isFrozen(error), true);
  assert.equal("cause" in error, false);
  return true;
}

function assertImageProviderRequest(input, kind) {
  exactKeys(
    input,
    kind === "resolve"
      ? [
          "imagePlanId",
          "imagePlanProviderId",
          "invocation",
          "sessionManifest",
          "signal",
        ]
      : [
          "imagePlanId",
          "imagePlanProviderId",
          "inspection",
          "invocation",
          "signal",
        ],
  );
  assert.equal(Object.getPrototypeOf(input), null);
  assert.equal(Object.isFrozen(input), true);
  assert.equal(Object.getPrototypeOf(input.invocation), null);
  assert.equal(Object.isFrozen(input.invocation), true);
  assert.deepEqual(Reflect.ownKeys(input.invocation), []);
  assert(input.signal instanceof AbortSignal);
  assert.equal(input.signal.aborted, false);
}

function nextTurn() {
  return new Promise((resolve) => setImmediate(resolve));
}

async function waitForEvent(predicate) {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    if (fakePgState().events.some(predicate)) return;
    await nextTurn();
  }
  assert.fail("expected fake PostgreSQL event was not observed");
}

function poolSummary() {
  return fakePgState().pools.map((pool) => ({
    calls: { ...pool.calls },
    options: pool.options,
    role: pool.role,
  }));
}

async function zeroIoAndLifecycle() {
  configureFakePg();
  const { calls, options } = validOptions();
  const deployment = createPostgresDetachedRestoreDeployment(options);
  exactKeys(deployment, [
    "foreground",
    "imagePlanReservations",
    "stablePlanProvisioning",
    "start",
    "stop",
    "writerLaunch",
  ]);
  assert.equal(Object.getPrototypeOf(deployment), null);
  assert.equal(Object.isFrozen(deployment), true);
  assert.equal(isPostgresDetachedRestoreDeployment(deployment), true);
  exactKeys(deployment.imagePlanReservations, ["prepareImageReservation"]);
  assert.equal(Object.getPrototypeOf(deployment.imagePlanReservations), null);
  assert.equal(Object.isFrozen(deployment.imagePlanReservations), true);
  assert.equal(
    Object.isFrozen(
      deployment.imagePlanReservations.prepareImageReservation,
    ),
    true,
  );
  for (const hidden of [
    "backend",
    "bootstrap",
    "controller",
    "pools",
    "postgres",
    "runtime",
    "scheduler",
  ]) {
    assert.equal(hidden in deployment, false);
  }
  const before = poolSummary();
  assert.equal(before.length, 4);
  assert.deepEqual(
    before.map(({ calls: poolCalls, role }) => [role, poolCalls]),
    [
      ["authority", { connect: 0, end: 0 }],
      ["operation", { connect: 0, end: 0 }],
      ["foregroundLifecycle", { connect: 0, end: 0 }],
      ["recoveryLifecycle", { connect: 0, end: 0 }],
    ],
  );
  const applications = new Set();
  const passwordProviders = new Set();
  for (let index = 0; index < before.length; index += 1) {
    const { options: poolOptions, role } = before[index];
    exactKeys(poolOptions, [
      "allowExitOnIdle",
      "application_name",
      "binary",
      "client_encoding",
      "connectionTimeoutMillis",
      "database",
      "enableChannelBinding",
      "host",
      "idleTimeoutMillis",
      "idle_in_transaction_session_timeout",
      "keepAlive",
      "keepAliveInitialDelayMillis",
      "lock_timeout",
      "max",
      "maxLifetimeSeconds",
      "min",
      "options",
      "password",
      "port",
      "Promise",
      "query_timeout",
      "replication",
      "ssl",
      "sslnegotiation",
      "statement_timeout",
      "user",
    ]);
    applications.add(poolOptions.application_name);
    passwordProviders.add(poolOptions.password);
    assert.equal(poolOptions.database, options.postgres.database);
    assert.equal(poolOptions.binary, false);
    assert.equal(poolOptions.host, options.postgres.host);
    assert.equal(typeof poolOptions.password, "function");
    assert.equal(Object.isFrozen(poolOptions.password), true);
    assert.equal(
      await Reflect.apply(poolOptions.password, { hostile: true }, []),
      SYNTHETIC_PASSWORD,
    );
    assert.equal(poolOptions.port, options.postgres.port);
    assert.equal(poolOptions.user, options.postgres.user);
    assert.equal(typeof poolOptions.Promise, "function");
    assert.equal(Object.isFrozen(poolOptions.Promise), true);
    assert.equal(Object.hasOwn(poolOptions.Promise, "resolve"), true);
    assert.equal(Object.hasOwn(poolOptions.Promise, "reject"), true);
    assert.equal(Object.hasOwn(poolOptions.Promise, "try"), true);
    assert.equal(poolOptions.Promise.try, undefined);
    assert.equal(
      poolOptions.max,
      options.postgres.poolMaximums[role],
    );
    assert.equal(
      poolOptions.connectionTimeoutMillis,
      options.postgres.timeouts.connectionMilliseconds,
    );
    assert.equal(
      poolOptions.idleTimeoutMillis,
      options.postgres.timeouts.idleClientMilliseconds,
    );
    assert.equal(
      poolOptions.idle_in_transaction_session_timeout,
      options.postgres.timeouts.idleTransactionMilliseconds,
    );
    assert.equal(
      poolOptions.lock_timeout,
      options.postgres.timeouts.lockMilliseconds,
    );
    assert.equal(
      poolOptions.query_timeout,
      options.postgres.timeouts.queryMilliseconds,
    );
    assert.equal(
      poolOptions.statement_timeout,
      options.postgres.timeouts.statementMilliseconds,
    );
    assert.equal(poolOptions.ssl, false);
    assert.equal(poolOptions.sslnegotiation, "postgres");
    assert.equal(poolOptions.client_encoding, "UTF8");
    assert.equal(poolOptions.options, "-c search_path=pg_catalog");
    assert.equal(poolOptions.replication, "false");
    assert.equal(poolOptions.enableChannelBinding, true);
    assert.equal(poolOptions.allowExitOnIdle, false);
    assert.equal(poolOptions.keepAlive, true);
    assert.equal(poolOptions.keepAliveInitialDelayMillis, 0);
    assert.equal(poolOptions.maxLifetimeSeconds, 0);
    assert.equal(poolOptions.min, 0);
  }
  assert.equal(applications.size, 4);
  assert.equal(passwordProviders.size, 1);
  assert.equal(SYNTHETIC_PASSWORD_ID, "access-a");
  assert.deepEqual(calls, {
    fleetGate: 0,
    image: 0,
    onStep: 0,
    planGate: 0,
    provider: 0,
    publication: 0,
    resolver: 0,
    supervisor: 0,
  });

  const started = deployment.start();
  assert.strictEqual(deployment.start(), started);
  assertStatusReceipt(await started, "ready");
  for (const pool of fakePgState().pools) {
    for (const client of pool.clients) {
      assert.equal(client.listenerCount("error"), 1);
    }
  }
  assert.equal(fakePgState().cursors.size, 4);
  assert.equal(calls.onStep >= 1, true);
  for (const key of ["image", "provider", "publication", "supervisor"]) {
    assert.equal(calls[key], 0);
  }
  const stopped = deployment.stop();
  assert.strictEqual(deployment.stop(), stopped);
  assertStatusReceipt(await stopped, "stopped");
  assert.deepEqual(fakePgState().endOrder, [
    "recoveryLifecycle",
    "foregroundLifecycle",
    "operation",
    "authority",
  ]);
  assert.deepEqual(
    fakePgState().pools.map((pool) => pool.calls.end),
    [1, 1, 1, 1],
  );
}

async function partialConstructionFailure() {
  for (let failureAt = 1; failureAt <= 4; failureAt += 1) {
    configureFakePg({ constructorFailureAt: failureAt });
    const { options } = validOptions();
    assert.throws(
      () => createPostgresDetachedRestoreDeployment(options),
      (error) =>
        assertDeploymentError(
          error,
          "postgres_detached_restore_deployment_outcome_uncertain",
        ),
    );
    await new Promise((resolve) => setImmediate(resolve));
    assert.deepEqual(
      fakePgState().pools.map((pool) => pool.calls.end),
      new Array(failureAt - 1).fill(1),
    );
    assert.deepEqual(
      fakePgState().endOrder,
      ["authority", "operation", "foregroundLifecycle"].slice(
        0,
        failureAt - 1,
      ).reverse(),
    );
  }
}

async function topologyFailure() {
  configureFakePg({ inRecovery: true });
  const { calls, options } = validOptions();
  const deployment = createPostgresDetachedRestoreDeployment(options);
  await assert.rejects(
    deployment.start(),
    (error) =>
      assertDeploymentError(
        error,
        "postgres_detached_restore_deployment_outcome_uncertain",
      ),
  );
  assert.equal(fakePgState().cursors.size, 0);
  assert.deepEqual(fakePgState().endOrder, [
    "recoveryLifecycle",
    "foregroundLifecycle",
    "operation",
    "authority",
  ]);
  for (const pool of fakePgState().pools) {
    assert.equal(pool.clients.length, 1);
    assert.equal(pool.clients[0].doneCalls.length, 1);
    assert.equal(pool.clients[0].releaseCalls.length, 0);
    assert.equal(pool.calls.end, 1);
  }
  for (const key of ["image", "provider", "publication", "supervisor"]) {
    assert.equal(calls[key], 0);
  }
  const stopped = deployment.stop();
  assert.strictEqual(deployment.stop(), stopped);
  assertStatusReceipt(await stopped, "stopped");
  assert.throws(
    () => deployment.start(),
    (error) =>
      assertDeploymentError(
        error,
        "invalid_postgres_detached_restore_deployment_request",
      ),
  );
}

async function topologyFailureWithPoolCloseFailure() {
  configureFakePg({ endFailures: ["operation"], inRecovery: true });
  const deployment = createPostgresDetachedRestoreDeployment(
    validOptions().options,
  );
  await assert.rejects(deployment.start(), outcomeError);
  const stopped = deployment.stop();
  assert.strictEqual(deployment.stop(), stopped);
  await assert.rejects(stopped, outcomeError);
  assert.deepEqual(fakePgState().endOrder, [
    "recoveryLifecycle",
    "foregroundLifecycle",
    "operation",
    "authority",
  ]);
  assert.deepEqual(
    fakePgState().pools.map((pool) => pool.calls.end),
    [1, 1, 1, 1],
  );
  assert.throws(() => deployment.start(), requestError);
}

async function invalidClientQueryStillReleasesAndCleansUp() {
  for (const invalidQueryKind of ["missing", "accessor"]) {
    configureFakePg({
      invalidClientQueryKinds: { authority: invalidQueryKind },
    });
    const deployment = createPostgresDetachedRestoreDeployment(
      validOptions().options,
    );
    await assert.rejects(deployment.start(), outcomeError);
    const stopped = deployment.stop();
    assert.strictEqual(deployment.stop(), stopped);
    assertStatusReceipt(await stopped, "stopped");
    const authorityClient = fakePgState().pools[0].clients[0];
    assert.equal(authorityClient.doneCalls.length, 1);
    assert.equal(authorityClient.releaseCalls.length, 0);
    assert.equal(fakePgState().invalidClientQueryAccessorReads, 0);
    assert.deepEqual(fakePgState().endOrder, [
      "recoveryLifecycle",
      "foregroundLifecycle",
      "operation",
      "authority",
    ]);
    assert.deepEqual(
      fakePgState().pools.map((pool) => pool.calls.end),
      [1, 1, 1, 1],
    );
    assert.throws(() => deployment.start(), requestError);
  }
}

function topologyResult(overrides = {}, extra = undefined) {
  const row = {
    backend_pid: 40_001,
    database_name: "portable_codex_runtime",
    database_user: "portable_codex_runtime",
    in_recovery: false,
    server_version_num: "130000",
    transaction_read_only: "off",
    ...overrides,
  };
  if (extra !== undefined) row.extra = extra;
  return { command: "SELECT", rows: [row] };
}

async function hostileTopologyEvidenceFailsClosed() {
  const cases = [
    { inRecovery: true },
    { databaseName: "crossed_database" },
    { databaseUser: "crossed_user" },
    { serverVersionNum: "120000" },
    { serverVersionNum: "13.0" },
    { transactionReadOnly: "on" },
    { backendPids: { authority: 0 } },
    { backendPids: { operation: 40_001 } },
    { topologyResults: { authority: { command: "SELECT", rows: [] } } },
    {
      topologyResults: {
        authority: {
          command: "SELECT",
          rows: [
            topologyResult().rows[0],
            { ...topologyResult().rows[0], backend_pid: 49_999 },
          ],
        },
      },
    },
    { topologyResults: { authority: topologyResult({}, true) } },
    { topologyResults: { authority: topologyResult({ backend_pid: null }) } },
    { tryLockOverrides: { authority: false } },
    { tryLockOverrides: { operation: true } },
    { unlockOverrides: { authority: false } },
  ];
  for (let index = 0; index < cases.length; index += 1) {
    configureFakePg(cases[index]);
    const deployment = createPostgresDetachedRestoreDeployment(
      validOptions().options,
    );
    await assert.rejects(deployment.start(), outcomeError);
    assert.equal(fakePgState().cursors.size, 0, `case ${index}`);
    assert.deepEqual(
      fakePgState().endOrder,
      [
        "recoveryLifecycle",
        "foregroundLifecycle",
        "operation",
        "authority",
      ],
      `case ${index}`,
    );
    for (const pool of fakePgState().pools) {
      assert.equal(pool.calls.end, 1, `case ${index} ${pool.role}`);
      assert.equal(pool.clients.length, 1, `case ${index} ${pool.role}`);
      assert.equal(
        pool.clients[0].doneCalls.length,
        1,
        `case ${index} ${pool.role}`,
      );
    }
    assert.throws(() => deployment.start(), requestError);
  }
}

async function allPoolEndsAttempted() {
  configureFakePg({
    endFailures: ["recoveryLifecycle", "operation"],
  });
  const { options } = validOptions();
  const deployment = createPostgresDetachedRestoreDeployment(options);
  const stopped = deployment.stop();
  await assert.rejects(
    stopped,
    (error) =>
      assertDeploymentError(
        error,
        "postgres_detached_restore_deployment_outcome_uncertain",
      ),
  );
  assert.deepEqual(fakePgState().endOrder, [
    "recoveryLifecycle",
    "foregroundLifecycle",
    "operation",
    "authority",
  ]);
  assert.deepEqual(
    fakePgState().pools.map((pool) => pool.calls.end),
    [1, 1, 1, 1],
  );
  assert.strictEqual(deployment.stop(), stopped);
  await assert.rejects(stopped);
  assert.deepEqual(
    fakePgState().pools.map((pool) => pool.calls.end),
    [1, 1, 1, 1],
  );
}

async function abnormalPoolEndResultsStillAttemptEveryPool() {
  const cases = [
    { endSyncThrows: ["foregroundLifecycle"] },
    { endNonPromiseResults: ["operation"] },
  ];
  for (const fakeOptions of cases) {
    configureFakePg(fakeOptions);
    const deployment = createPostgresDetachedRestoreDeployment(
      validOptions().options,
    );
    const stopped = deployment.stop();
    assert.strictEqual(deployment.stop(), stopped);
    await assert.rejects(stopped, outcomeError);
    assert.deepEqual(fakePgState().endOrder, [
      "recoveryLifecycle",
      "foregroundLifecycle",
      "operation",
      "authority",
    ]);
    assert.deepEqual(
      fakePgState().pools.map((pool) => pool.calls.end),
      [1, 1, 1, 1],
    );
  }
}

async function stopWaitsForPoolAcknowledgements() {
  configureFakePg();
  const authorityEnd = holdFakePoolEnd("authority");
  const { options } = validOptions();
  const deployment = createPostgresDetachedRestoreDeployment(options);
  const stopped = deployment.stop();
  let settled = false;
  void stopped.then(
    () => {
      settled = true;
    },
    () => {
      settled = true;
    },
  );
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(settled, false);
  assert.deepEqual(fakePgState().endOrder, [
    "recoveryLifecycle",
    "foregroundLifecycle",
    "operation",
    "authority",
  ]);
  authorityEnd.resolve();
  assert.deepEqual(await stopped, Object.assign(Object.create(null), {
    status: "stopped",
  }));
}

async function idlePoolErrorForcesTerminalShutdown() {
  configureFakePg();
  const { options } = validOptions();
  const deployment = createPostgresDetachedRestoreDeployment(options);
  fakePgState().pools[1].emitIdleError();
  await new Promise((resolve) => setImmediate(resolve));
  assert.deepEqual(fakePgState().endOrder, [
    "recoveryLifecycle",
    "foregroundLifecycle",
    "operation",
    "authority",
  ]);
  assert.throws(
    () => deployment.start(),
    (error) =>
      assertDeploymentError(
        error,
        "invalid_postgres_detached_restore_deployment_request",
      ),
  );
  await assert.rejects(
    deployment.stop(),
    (error) =>
      assertDeploymentError(
        error,
        "postgres_detached_restore_deployment_outcome_uncertain",
      ),
  );
}

async function hostileOptions() {
  configureFakePg();
  const cases = [];
  {
    const { options } = validOptions();
    cases.push({ ...options, extra: true });
  }
  for (const value of [0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY]) {
    const { options } = validOptions();
    options.postgres.poolMaximums.authority = value;
    cases.push(options);
  }
  {
    const { options } = validOptions();
    options.postgres.tls = {
      ca: "-----BEGIN CERTIFICATE-----\nfixture\n-----END CERTIFICATE-----",
      cert: null,
      key: null,
      mode: "verify-full",
      serverName: "crossed.example.test",
    };
    cases.push(options);
  }
  {
    const { options } = validOptions();
    options.postgres.applicationNamePrefix = "x".repeat(64);
    cases.push(options);
  }
  for (const value of [0, -1, 1.5, Number.NaN, Number.POSITIVE_INFINITY]) {
    const { options } = validOptions();
    options.postgres.timeouts.queryMilliseconds = value;
    cases.push(options);
  }
  for (let leafIndex = 0; leafIndex < 19; leafIndex += 1) {
    for (const field of [
      "deadlineMilliseconds",
      "settlementGraceMilliseconds",
    ]) {
      for (const value of [0, 86_400_001]) {
        const { options } = validOptions();
        deploymentPhysicalPolicyLeaves(options)[leafIndex][field] = value;
        cases.push(options);
      }
    }
  }
  {
    const { options } = validOptions();
    options.runtime.launch.imagePlanProviderSettlement.inspectCodex.extra =
      true;
    cases.push(options);
  }
  for (const options of cases) {
    const before = fakePgState().pools.length;
    assert.throws(
      () => createPostgresDetachedRestoreDeployment(options),
      (error) =>
        assertDeploymentError(
          error,
          "invalid_postgres_detached_restore_deployment_options",
        ),
    );
    assert.equal(fakePgState().pools.length, before);
  }
  const { options } = validOptions();
  let getterCalls = 0;
  Object.defineProperty(options.postgres, "host", {
    enumerable: true,
    get() {
      getterCalls += 1;
      return "db.example.test";
    },
  });
  assert.throws(
    () => createPostgresDetachedRestoreDeployment(options),
    (error) =>
      assertDeploymentError(
        error,
        "invalid_postgres_detached_restore_deployment_options",
      ),
  );
  assert.equal(getterCalls, 0);
}

async function exactPhysicalConfigRejection() {
  configureFakePg();
  const cases = [];
  {
    const { options } = validOptions();
    options.runtime.launch.extra = true;
    cases.push(options);
  }
  {
    const { options } = validOptions();
    delete options.runtime.launch.supervisorSettlement.stopWriter;
    cases.push(options);
  }
  {
    const { options } = validOptions();
    options.runtime.launch.supervisorSettlement.stopWriter.extra = true;
    cases.push(options);
  }
  {
    const { options } = validOptions();
    options.runtime.launch.supervisor = Object.freeze({
      ...options.runtime.launch.supervisor,
      contractVersion: 1,
    });
    cases.push(options);
  }
  {
    const { options } = validOptions();
    options.runtime.launch.supervisor = Object.freeze({
      ...options.runtime.launch.supervisor,
      launchWriter: null,
    });
    cases.push(options);
  }
  {
    const { options } = validOptions();
    options.runtime.storage.extra = true;
    cases.push(options);
  }
  {
    const { options } = validOptions();
    delete options.runtime.storage.lifecycleBackendSettlement.restoreCheckpoint;
    cases.push(options);
  }
  {
    const { options } = validOptions();
    options.runtime.storage.publicationSettlement.extra = physicalPolicy();
    cases.push(options);
  }
  {
    const { options } = validOptions();
    options.runtime.storage.resolveRestoreDestinationContractVersion = 2;
    cases.push(options);
  }
  {
    const { options } = validOptions();
    options.runtime.storage.resolveRestoreDestinationSettlement.extra = true;
    cases.push(options);
  }
  {
    const { options } = validOptions();
    const inherited = Object.create(
      options.runtime.storage.lifecycleBackend,
    );
    delete inherited.physicalInvocationContractVersion;
    options.runtime.storage.lifecycleBackend = inherited;
    cases.push(options);
  }

  for (let index = 0; index < cases.length; index += 1) {
    const before = fakePgState().pools.length;
    assert.throws(
      () => createPostgresDetachedRestoreDeployment(cases[index]),
      (error) =>
        assertDeploymentError(
          error,
          "invalid_postgres_detached_restore_deployment_options",
        ),
      `case ${index}`,
    );
    assert.equal(fakePgState().pools.length, before, `case ${index}`);
  }
}

async function exactOperationalLeaseConfigRejection() {
  configureFakePg();
  const cases = [];
  {
    const fixture = validOptions();
    delete fixture.options.runtime.operationalLease;
    cases.push(fixture);
  }
  {
    const fixture = validOptions();
    fixture.options.runtime.operationalLease.extra = true;
    cases.push(fixture);
  }
  for (const field of [
    "databaseRequestMilliseconds",
    "leaseDurationMilliseconds",
    "safetyMarginMilliseconds",
  ]) {
    {
      const fixture = validOptions();
      delete fixture.options.runtime.operationalLease[field];
      cases.push(fixture);
    }
    for (const value of [
      0,
      -1,
      1.5,
      Number.NaN,
      Number.POSITIVE_INFINITY,
      86_400_001,
    ]) {
      const fixture = validOptions();
      fixture.options.runtime.operationalLease[field] = value;
      cases.push(fixture);
    }
  }
  for (const { calls, options } of cases) {
    assertOperationalLeaseOptionRejection(options, calls);
  }

  const { calls, options } = validOptions();
  let getterCalls = 0;
  Object.defineProperty(
    options.runtime.operationalLease,
    "databaseRequestMilliseconds",
    {
      enumerable: true,
      get() {
        getterCalls += 1;
        return 30_000;
      },
    },
  );
  assertOperationalLeaseOptionRejection(options, calls);
  assert.equal(getterCalls, 0);
}

async function tooShortOperationalLeaseRejection() {
  configureFakePg();
  const { calls, options } = validOptions();
  options.runtime.operationalLease.leaseDurationMilliseconds = 1;
  assertOperationalLeaseOptionRejection(options, calls);
}

async function operationalLeaseAggregateOverflowRejection() {
  configureFakePg();
  const { calls, options } = validOptions();
  for (const policy of deploymentPhysicalPolicyLeaves(options)) {
    policy.deadlineMilliseconds = 86_400_000;
    policy.settlementGraceMilliseconds = 86_400_000;
  }
  options.runtime.operationalLease.databaseRequestMilliseconds = 86_400_000;
  options.runtime.operationalLease.leaseDurationMilliseconds = 86_400_000;
  options.runtime.operationalLease.safetyMarginMilliseconds = 86_400_000;
  assertOperationalLeaseOptionRejection(options, calls);
}

async function applicationNameBudget() {
  configureFakePg();
  const { options } = validOptions();
  options.postgres.applicationNamePrefix = "p".repeat(32);
  const deployment = createPostgresDetachedRestoreDeployment(options);
  for (const pool of fakePgState().pools) {
    assert.equal(
      Buffer.byteLength(pool.options.application_name, "utf8") <= 63,
      true,
    );
  }
  await deployment.stop();
}

async function verifyFullTlsConfiguration() {
  configureFakePg();
  const { options } = validOptions();
  options.postgres.tls = {
    ca: "deployment-test-ca",
    cert: "deployment-test-client-certificate",
    key: SYNTHETIC_PASSWORD,
    mode: "verify-full",
    serverName: options.postgres.host,
  };
  const deployment = createPostgresDetachedRestoreDeployment(options);
  for (const pool of fakePgState().pools) {
    exactKeys(pool.options.ssl, [
      "ca",
      "cert",
      "key",
      "rejectUnauthorized",
      "servername",
    ]);
    assert.equal(pool.options.ssl.ca, "deployment-test-ca");
    assert.equal(
      pool.options.ssl.cert,
      "deployment-test-client-certificate",
    );
    assert.equal(pool.options.ssl.key, SYNTHETIC_PASSWORD);
    assert.equal(pool.options.ssl.rejectUnauthorized, true);
    assert.equal(pool.options.ssl.servername, options.postgres.host);
    assert.equal(pool.options.sslnegotiation, "postgres");
  }
  await deployment.stop();

  configureFakePg();
  const crossed = validOptions().options;
  crossed.postgres.tls = {
    ca: "deployment-test-ca",
    cert: null,
    key: null,
    mode: "verify-full",
    serverName: "crossed.example.test",
  };
  assert.throws(
    () => createPostgresDetachedRestoreDeployment(crossed),
    (error) =>
      assertDeploymentError(
        error,
        "invalid_postgres_detached_restore_deployment_options",
      ),
  );
  assert.equal(fakePgState().pools.length, 0);
}

async function admittedIngressCannotStopItsDeployment() {
  configureFakePg();
  const { calls, controls, options } = validOptions();
  const deployment = createPostgresDetachedRestoreDeployment(options);
  await deployment.start();
  const stopErrors = [];
  let stopReturned = 0;
  controls.planGateOverride = () => {
    try {
      deployment.stop();
      stopReturned += 1;
    } catch (error) {
      stopErrors.push(error);
    }
    return POSTGRES_DETACHED_RESTORE_STABLE_PLAN_PROVISIONING_CONFIRMED;
  };
  const directAdmission = restoreAdmission("deployment-self-stop-direct-001");
  await Promise.allSettled([
    deployment.stablePlanProvisioning.provisionStablePlan({
      admission: directAdmission,
      plan: stablePlan(directAdmission),
    }),
  ]);
  controls.planGateOverride = () =>
    Promise.resolve().then(() => {
      try {
        deployment.stop();
        stopReturned += 1;
      } catch (error) {
        stopErrors.push(error);
      }
      return POSTGRES_DETACHED_RESTORE_STABLE_PLAN_PROVISIONING_CONFIRMED;
    });
  const chainedAdmission = restoreAdmission(
    "deployment-self-stop-chained-001",
  );
  await Promise.allSettled([
    deployment.stablePlanProvisioning.provisionStablePlan({
      admission: chainedAdmission,
      plan: stablePlan(chainedAdmission),
    }),
  ]);
  assert.equal(stopReturned, 0);
  assert.equal(stopErrors.length, 2);
  assert.equal(calls.planGate, 2);
  for (const error of stopErrors) requestError(error);
  assert.deepEqual(fakePgState().endOrder, []);
  assertStatusReceipt(await deployment.stop(), "stopped");
}

async function stopDuringTopologyNeverReopensIngress() {
  configureFakePg();
  const topology = holdFakeTopology("authority");
  const { options } = validOptions();
  const deployment = createPostgresDetachedRestoreDeployment(options);
  const started = deployment.start();
  await waitForEvent(
    ([kind, role, text]) =>
      kind === "query" &&
      role === "authority" &&
      text.includes("current_database()"),
  );
  const stopped = deployment.stop();
  assert.strictEqual(deployment.stop(), stopped);
  assert.throws(
    () => deployment.stablePlanProvisioning.provisionStablePlan({}),
    requestError,
  );
  await nextTurn();
  assert.deepEqual(fakePgState().endOrder, []);
  topology.resolve();
  await assert.rejects(started, outcomeError);
  assertStatusReceipt(await stopped, "stopped");
  assert.deepEqual(fakePgState().endOrder, [
    "recoveryLifecycle",
    "foregroundLifecycle",
    "operation",
    "authority",
  ]);
  for (const pool of fakePgState().pools) {
    assert.equal(pool.clients.length, 1);
    assert.equal(pool.clients[0].doneCalls.length, 1);
    assert.equal(pool.clients[0].releaseCalls.length, 0);
  }
  assert.throws(() => deployment.start(), requestError);
}

async function checkedOutClientErrorForcesFatalShutdown() {
  configureFakePg();
  const topology = holdFakeTopology("authority");
  const deployment = createPostgresDetachedRestoreDeployment(
    validOptions().options,
  );
  const started = deployment.start();
  await waitForEvent(
    ([kind, role, text]) =>
      kind === "query" &&
      role === "authority" &&
      text.includes("current_database()"),
  );
  const authorityClient = fakePgState().pools[0].clients[0];
  assert.equal(authorityClient.listenerCount("error"), 1);
  assert.doesNotThrow(() => {
    assert.equal(
      authorityClient.emit(
        "error",
        new Error("controlled checked-out client failure"),
      ),
      true,
    );
  });
  const stopped = deployment.stop();
  assert.strictEqual(deployment.stop(), stopped);
  assert.throws(
    () => deployment.stablePlanProvisioning.provisionStablePlan({}),
    requestError,
  );
  await nextTurn();
  assert.deepEqual(fakePgState().endOrder, []);
  topology.resolve();
  await assert.rejects(started, outcomeError);
  await assert.rejects(stopped, outcomeError);
  assert.equal(
    fakePgState().events.filter(
      ([kind, , text]) =>
        kind === "query" && text.includes("current_database()"),
    ).length,
    1,
  );
  assert.deepEqual(fakePgState().endOrder, [
    "recoveryLifecycle",
    "foregroundLifecycle",
    "operation",
    "authority",
  ]);
  for (const pool of fakePgState().pools) {
    assert.equal(pool.calls.end, 1);
    assert.equal(pool.clients[0].doneCalls.length, 1);
    assert.equal(pool.clients[0].releaseCalls.length, 0);
  }
  assert.throws(() => deployment.start(), requestError);
}

async function synchronousConnectPoolErrorUsesAssignedStartPromise() {
  configureFakePg({ synchronousConnectErrors: ["authority"] });
  const { options } = validOptions();
  const deployment = createPostgresDetachedRestoreDeployment(options);
  const started = deployment.start();
  assert(started instanceof Promise);
  await assert.rejects(started, outcomeError);
  const stopped = deployment.stop();
  assert.strictEqual(deployment.stop(), stopped);
  await assert.rejects(stopped, outcomeError);
  assert.deepEqual(fakePgState().endOrder, [
    "recoveryLifecycle",
    "foregroundLifecycle",
    "operation",
    "authority",
  ]);
  assert.deepEqual(
    fakePgState().pools.map((pool) => pool.calls.end),
    [1, 1, 1, 1],
  );
  assert.equal(fakePgState().cursors.size, 0);
  assert.equal(
    fakePgState().events.some(
      ([kind, , text]) => kind === "query" && text.includes("current_database()"),
    ),
    false,
  );
  assert.throws(
    () => deployment.stablePlanProvisioning.provisionStablePlan({}),
    requestError,
  );
  assert.throws(() => deployment.start(), requestError);
}

async function independentDeploymentsUseDistinctProbeKeys() {
  configureFakePg();
  const first = createPostgresDetachedRestoreDeployment(validOptions().options);
  const second = createPostgresDetachedRestoreDeployment(validOptions().options);
  await Promise.all([first.start(), second.start()]);
  const lockQueries = fakePgState().events.filter(
    ([kind, , text]) =>
      kind === "query" && text.includes("pg_try_advisory_lock"),
  );
  const lockKeySets = fakePgState().pools
    .filter((pool) => pool.role === "authority")
    .map((pool) =>
      pool.clients
        .flatMap((client) => client.probeLockKeys ?? [])
        .map((values) => values.join(":")),
    );
  assert.equal(lockQueries.length >= 8, true);
  assert.equal(fakePgState().lockHolders.size, 0);
  // Distinct deployments must not share one process-global fixed probe key.
  assert.notDeepEqual(lockKeySets[0], lockKeySets[1]);
  await Promise.all([first.stop(), second.stop()]);
}

async function emptyPasswordBlocksAmbientFallback() {
  configureFakePg();
  const { options } = validOptions();
  options.postgres.password = "";
  const deployment = createPostgresDetachedRestoreDeployment(options);
  assert.equal(fakePgState().pools.length, 4);
  for (const pool of fakePgState().pools) {
    assert.equal(typeof pool.options.password, "function");
    assert.equal(await pool.options.password(), "");
    assert.notEqual(pool.options.password, process.env.PGPASSWORD);
    assert.equal("connectionString" in pool.options, false);
  }
  await deployment.stop();
}

async function afterImportPromiseTryPoisonIsIgnored() {
  configureFakePg();
  let poisonCalls = 0;
  Object.defineProperty(Promise, "try", {
    configurable: true,
    enumerable: false,
    value: function poisonedPromiseTry() {
      poisonCalls += 1;
      throw new Error("after-import Promise.try poison must not run");
    },
    writable: true,
  });
  const deployment = createPostgresDetachedRestoreDeployment(
    validOptions().options,
  );
  assertStatusReceipt(await deployment.start(), "ready");
  assertStatusReceipt(await deployment.stop(), "stopped");
  assert.equal(poisonCalls, 0);
}

async function afterImportPromiseConstructorGetterIsIgnored() {
  configureFakePg({ inRecovery: true });
  // Let the top-level harness attach to this scenario Promise before poisoning
  // the inherited constructor getter that it would otherwise consult itself.
  await nextTurn();
  const constructorDescriptor = Object.getOwnPropertyDescriptor(
    Promise.prototype,
    "constructor",
  );
  const tryDescriptor = Object.getOwnPropertyDescriptor(
    Function.prototype,
    "try",
  );
  let poisonReads = 0;
  let poisonTryReads = 0;
  try {
    Object.defineProperty(Promise.prototype, "constructor", {
      configurable: constructorDescriptor.configurable,
      enumerable: constructorDescriptor.enumerable,
      get() {
        poisonReads += 1;
        throw new Error(
          "after-import Promise.prototype.constructor poison must not run",
        );
      },
    });
    Object.defineProperty(Function.prototype, "try", {
      configurable: true,
      enumerable: false,
      get() {
        poisonTryReads += 1;
        throw new Error("after-import Function.prototype.try poison must not run");
      },
    });
    const deployment = createPostgresDetachedRestoreDeployment(
      validOptions().options,
    );
    const driverPromise = fakePgState().pools[0].options.Promise;
    assert.equal(Object.hasOwn(driverPromise, "try"), true);
    assert.equal(driverPromise.try, undefined);
    assert.equal(
      await driverPromise
        .resolve()
        .then(() => fakePgState().pools[0].options.password())
        .then((password) => password),
      SYNTHETIC_PASSWORD,
    );
    const started = deployment.start();
    assert.strictEqual(deployment.start(), started);
    let startError;
    try {
      await started;
    } catch (error) {
      startError = error;
    }
    outcomeError(startError);
    const stopped = deployment.stop();
    assert.strictEqual(deployment.stop(), stopped);
    assertStatusReceipt(await stopped, "stopped");
    assert.deepEqual(fakePgState().endOrder, [
      "recoveryLifecycle",
      "foregroundLifecycle",
      "operation",
      "authority",
    ]);
    assert.deepEqual(
      fakePgState().pools.map((pool) => pool.calls.end),
      [1, 1, 1, 1],
    );
    assert.equal(poisonReads, 0);
    assert.equal(poisonTryReads, 0);
  } finally {
    Object.defineProperty(
      Promise.prototype,
      "constructor",
      constructorDescriptor,
    );
    if (tryDescriptor === undefined) {
      delete Function.prototype.try;
    } else {
      Object.defineProperty(Function.prototype, "try", tryDescriptor);
    }
  }
}

async function afterImportPromiseSpeciesGetterFailsClosed() {
  configureFakePg();
  const deployment = createPostgresDetachedRestoreDeployment(
    validOptions().options,
  );
  assertStatusReceipt(await deployment.start(), "ready");
  const stopped = deployment.stop();
  assertStatusReceipt(await stopped, "stopped");

  const candidate = Promise.resolve("candidate-value");
  let reactionCalls = 0;
  const untrustedReaction = Object.freeze(function untrustedReaction() {
    reactionCalls += 1;
    throw new Error("untrusted Promise reaction must not run");
  });
  Object.defineProperties(candidate, {
    catch: {
      configurable: false,
      enumerable: false,
      value: untrustedReaction,
      writable: false,
    },
    constructor: {
      configurable: false,
      enumerable: false,
      value: Promise,
      writable: false,
    },
    finally: {
      configurable: false,
      enumerable: false,
      value: untrustedReaction,
      writable: false,
    },
    then: {
      configurable: false,
      enumerable: false,
      value: untrustedReaction,
      writable: false,
    },
  });
  const speciesDescriptor = Object.getOwnPropertyDescriptor(
    Promise,
    Symbol.species,
  );
  let speciesReads = 0;
  let failure;
  Object.defineProperty(Promise, Symbol.species, {
    configurable: speciesDescriptor.configurable,
    enumerable: speciesDescriptor.enumerable,
    get() {
      speciesReads += 1;
      throw new Error("Promise species must not be read");
    },
  });
  try {
    try {
      await stopped.then(() => candidate);
    } catch (error) {
      failure = error;
    }
  } finally {
    Object.defineProperty(Promise, Symbol.species, speciesDescriptor);
  }

  outcomeError(failure);
  assert.equal(speciesReads, 0);
  assert.equal(reactionCalls, 0);
}

async function objectPrototypeThenCannotForgeDriverEvidence() {
  configureFakePg({ inRecovery: true });
  await nextTurn();
  const deployment = createPostgresDetachedRestoreDeployment(
    validOptions().options,
  );
  const descriptor = Object.getOwnPropertyDescriptor(Object.prototype, "then");
  let poisonCalls = 0;
  let rawResultReads = 0;
  const forgedThen = Object.freeze(function forgedThen(resolve) {
    poisonCalls += 1;
    resolve({
      command: "SELECT",
      rows: [
        {
          backend_pid: 47_000,
          database_name: "portable_codex_runtime",
          database_user: "portable_codex_runtime",
          in_recovery: false,
          server_version_num: "130000",
          transaction_read_only: "off",
        },
      ],
    });
  });
  try {
    Object.defineProperty(Object.prototype, "then", {
      configurable: true,
      enumerable: false,
      get() {
        if (!isFakePgQueryResult(this)) return undefined;
        rawResultReads += 1;
        return forgedThen;
      },
    });
    let startError;
    try {
      await deployment.start();
    } catch (error) {
      startError = error;
    }
    outcomeError(startError);
    assert.equal(rawResultReads, 0);
    assert.equal(poisonCalls, 0);
    assertStatusReceipt(await deployment.stop(), "stopped");
  } finally {
    if (descriptor === undefined) {
      delete Object.prototype.then;
    } else {
      Object.defineProperty(Object.prototype, "then", descriptor);
    }
  }
}

async function imagePlanReservationIngressIsGatedAndDrained() {
  configureFakePg();
  const { calls, controls, options } = validOptions();
  const deployment = createPostgresDetachedRestoreDeployment(options);
  const admission = restoreAdmission("deployment-image-plan-ingress-001");
  const input = {
    plan: stablePlan(admission),
    sessionManifest: controls.imagePlanFixture.sessionManifest,
  };

  assert.throws(
    () => deployment.imagePlanReservations.prepareImageReservation(input),
    requestError,
  );
  const starting = deployment.start();
  assert.throws(
    () => deployment.imagePlanReservations.prepareImageReservation(input),
    requestError,
  );
  assertStatusReceipt(await starting, "ready");

  controls.imagePlanEntered = deferred();
  controls.imagePlanBlock = deferred();
  const admitted =
    deployment.imagePlanReservations.prepareImageReservation(input);
  await controls.imagePlanEntered.promise;
  const stopped = deployment.stop();
  let stopSettled = false;
  void stopped.then(() => {
    stopSettled = true;
  });
  assert.throws(
    () => deployment.imagePlanReservations.prepareImageReservation(input),
    requestError,
  );
  await nextTurn();
  assert.equal(stopSettled, false);
  controls.imagePlanBlock.resolve();
  await assert.rejects(admitted, imageResolutionError);
  assertStatusReceipt(await stopped, "stopped");
  assert.throws(
    () => deployment.imagePlanReservations.prepareImageReservation(input),
    requestError,
  );
  assert.equal(calls.image, 1);
}

async function imagePlanStopAbortsAndDrainsActiveProvider() {
  configureFakePg();
  const { calls, controls, options } = validOptions();
  const resolverEntered = deferred();
  const resolverSettled = deferred();
  let resolverRequest;
  controls.imagePlanResolverOverride = function resolveAfterAbort(input) {
    resolverRequest = input;
    resolverEntered.resolve();
    input.signal.addEventListener(
      "abort",
      () => resolverSettled.resolve(safeProviderCarrier({
        configBytes: controls.imagePlanFixture.configBytes,
        descriptor: controls.imagePlanFixture.descriptor,
      })),
      { once: true },
    );
    return resolverSettled.promise;
  };
  options.runtime.launch.imagePlanProviderSettlement.resolveImagePlan = {
    deadlineMilliseconds: 30_000,
    settlementGraceMilliseconds: 100,
  };
  const deployment = createPostgresDetachedRestoreDeployment(options);
  assertStatusReceipt(await deployment.start(), "ready");
  const input = {
    plan: stablePlan(restoreAdmission("deployment-image-plan-stop-drain-001")),
    sessionManifest: controls.imagePlanFixture.sessionManifest,
  };
  const admitted = deployment.imagePlanReservations.prepareImageReservation(input);
  await resolverEntered.promise;
  assert.equal(resolverRequest.signal.aborted, false);

  const stopped = deployment.stop();
  assert.strictEqual(deployment.stop(), stopped);
  assert.equal(resolverRequest.signal.aborted, true);
  await assert.rejects(admitted, imageResolutionError);
  assertStatusReceipt(await stopped, "stopped");
  assert.equal(calls.image, 1);
  assert.deepEqual(fakePgState().endOrder, [
    "recoveryLifecycle",
    "foregroundLifecycle",
    "operation",
    "authority",
  ]);
}

async function allSettlementStopsStartBeforeAwait() {
  configureFakePg();
  const { controls, options } = validOptions();
  const resolverEntered = deferred();
  const releaseResolver = deferred();
  let resolverRequest;
  controls.imagePlanResolverOverride = function heldResolver(input) {
    resolverRequest = input;
    resolverEntered.resolve();
    return releaseResolver.promise.then(() =>
      safeProviderCarrier({
        configBytes: controls.imagePlanFixture.configBytes,
        descriptor: controls.imagePlanFixture.descriptor,
      }),
    );
  };
  const deployment = createPostgresDetachedRestoreDeployment(options);
  assertStatusReceipt(await deployment.start(), "ready");

  const firstAdmission = restoreAdmission(
    "deployment-stop-order-resolver-001",
  );
  const first = deployment.imagePlanReservations.prepareImageReservation({
    plan: stablePlan(firstAdmission),
    sessionManifest: controls.imagePlanFixture.sessionManifest,
  });
  await resolverEntered.promise;
  assert.equal(resolverRequest.signal.aborted, false);

  const stopped = deployment.stop();
  // stop() synchronously starts every fixed registry stop before it awaits
  // any one result. The active provider is therefore aborted immediately.
  assert.equal(resolverRequest.signal.aborted, true);
  await nextTurn();
  assert.deepEqual(fakePgState().endOrder, []);

  releaseResolver.resolve();
  await assert.rejects(first, imageResolutionError);
  assertStatusReceipt(await stopped, "stopped");
  assert.deepEqual(fakePgState().endOrder, [
    "recoveryLifecycle",
    "foregroundLifecycle",
    "operation",
    "authority",
  ]);
}

async function imagePlanGraceBreachForcesFatalShutdown() {
  configureFakePg();
  const { calls, controls, options } = validOptions();
  const resolverEntered = deferred();
  const neverSettles = new Promise(() => {});
  let resolverRequest;
  controls.imagePlanResolverOverride = function unresolvedProvider(input) {
    resolverRequest = input;
    resolverEntered.resolve();
    return neverSettles;
  };
  options.runtime.launch.imagePlanProviderSettlement.resolveImagePlan = {
    deadlineMilliseconds: 15,
    settlementGraceMilliseconds: 15,
  };
  const deployment = createPostgresDetachedRestoreDeployment(options);
  assertStatusReceipt(await deployment.start(), "ready");
  const input = {
    plan: stablePlan(restoreAdmission("deployment-image-plan-grace-breach-001")),
    sessionManifest: controls.imagePlanFixture.sessionManifest,
  };
  const admitted = deployment.imagePlanReservations.prepareImageReservation(input);
  await resolverEntered.promise;
  assert.equal(resolverRequest.signal.aborted, false);

  await assert.rejects(admitted, imageResolutionError);
  assert.equal(resolverRequest.signal.aborted, true);
  assert.throws(
    () => deployment.imagePlanReservations.prepareImageReservation(input),
    requestError,
  );
  assert.throws(
    () => deployment.stablePlanProvisioning.provisionStablePlan({}),
    requestError,
  );
  const stopped = deployment.stop();
  assert.strictEqual(deployment.stop(), stopped);
  await assert.rejects(stopped, outcomeError);
  assert.equal(calls.image, 1);
  assert.deepEqual(fakePgState().endOrder, [
    "recoveryLifecycle",
    "foregroundLifecycle",
    "operation",
    "authority",
  ]);
  assert.deepEqual(
    fakePgState().pools.map((pool) => pool.calls.end),
    [1, 1, 1, 1],
  );
}

const scenarios = Object.freeze({
  "abnormal-pool-end-results-still-attempt-every-pool":
    abnormalPoolEndResultsStillAttemptEveryPool,
  "admitted-ingress-cannot-stop-its-deployment":
    admittedIngressCannotStopItsDeployment,
  "after-import-promise-constructor-getter-is-ignored":
    afterImportPromiseConstructorGetterIsIgnored,
  "after-import-promise-species-getter-fails-closed":
    afterImportPromiseSpeciesGetterFailsClosed,
  "after-import-promise-try-poison-is-ignored":
    afterImportPromiseTryPoisonIsIgnored,
  "all-pool-ends-attempted": allPoolEndsAttempted,
  "all-settlement-stops-start-before-await":
    allSettlementStopsStartBeforeAwait,
  "application-name-budget": applicationNameBudget,
  "checked-out-client-error-forces-fatal-shutdown":
    checkedOutClientErrorForcesFatalShutdown,
  "empty-password-blocks-ambient-fallback":
    emptyPasswordBlocksAmbientFallback,
  "exact-operational-lease-config-rejection":
    exactOperationalLeaseConfigRejection,
  "exact-physical-config-rejection": exactPhysicalConfigRejection,
  "hostile-options": hostileOptions,
  "hostile-topology-evidence-fails-closed":
    hostileTopologyEvidenceFailsClosed,
  "idle-pool-error-forces-terminal-shutdown":
    idlePoolErrorForcesTerminalShutdown,
  "image-plan-grace-breach-forces-fatal-shutdown":
    imagePlanGraceBreachForcesFatalShutdown,
  "image-plan-reservation-ingress-is-gated-and-drained":
    imagePlanReservationIngressIsGatedAndDrained,
  "image-plan-stop-aborts-and-drains-active-provider":
    imagePlanStopAbortsAndDrainsActiveProvider,
  "independent-deployments-use-distinct-probe-keys":
    independentDeploymentsUseDistinctProbeKeys,
  "invalid-client-query-still-releases-and-cleans-up":
    invalidClientQueryStillReleasesAndCleansUp,
  "object-prototype-then-cannot-forge-driver-evidence":
    objectPrototypeThenCannotForgeDriverEvidence,
  "operational-lease-aggregate-overflow-rejection":
    operationalLeaseAggregateOverflowRejection,
  "partial-construction-failure": partialConstructionFailure,
  "stop-waits-for-pool-acknowledgements":
    stopWaitsForPoolAcknowledgements,
  "stop-during-topology-never-reopens-ingress":
    stopDuringTopologyNeverReopensIngress,
  "synchronous-connect-pool-error-uses-assigned-start-promise":
    synchronousConnectPoolErrorUsesAssignedStartPromise,
  "topology-failure": topologyFailure,
  "topology-failure-with-pool-close-failure":
    topologyFailureWithPoolCloseFailure,
  "too-short-operational-lease-rejection":
    tooShortOperationalLeaseRejection,
  "verify-full-tls-configuration": verifyFullTlsConfiguration,
  "zero-io-and-lifecycle": zeroIoAndLifecycle,
});

const name = process.argv[2];
if (name !== undefined) {
  assert.equal(typeof scenarios[name], "function", `unknown scenario ${name}`);
  await Promise.all(MIGRATION_URLS.map((url) => readFile(url, "utf8")));
  await scenarios[name]();
  process.stdout.write(
    `${JSON.stringify({ scenario: name, status: "passed" })}\n`,
  );
}
