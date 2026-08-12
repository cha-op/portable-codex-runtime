import assert from "node:assert/strict";
import test from "node:test";

import {
  PostgresDetachedRestoreOperationalLeaseBudgetError,
  assertPostgresDetachedRestoreOperationalLeaseDuration,
  assertPostgresDetachedRestoreOperationalLeasePlan,
  createPostgresDetachedRestoreOperationalLeaseBudget,
  isPostgresDetachedRestoreOperationalLeaseBudget,
} from "../src/postgres-detached-restore-operational-lease-budget.mjs";
import { createPostgresDetachedRestorePlan } from "../src/postgres-detached-restore-plan.mjs";

const IMAGE_METHODS = Object.freeze(["inspectCodex", "resolveImagePlan"]);
const SUPERVISOR_METHODS = Object.freeze([
  "launchWriter",
  "reconcileWriterLaunch",
  "stopWriter",
]);
const LIFECYCLE_METHODS = Object.freeze([
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
const PUBLICATION_METHODS = Object.freeze([
  "publishFreshCheckpointArtifact",
  "publishRestoreDestination",
  "verifyCommittedCheckpointArtifact",
  "verifyCommittedRestoreDestination",
]);

function policy(deadlineMilliseconds = 1, settlementGraceMilliseconds = 1) {
  return { deadlineMilliseconds, settlementGraceMilliseconds };
}

function policies(methods) {
  return Object.fromEntries(methods.map((method) => [method, policy()]));
}

function options(leaseDurationMilliseconds = 29) {
  return {
    databaseRequestMilliseconds: 10,
    imagePlanProviderSettlement: policies(IMAGE_METHODS),
    leaseDurationMilliseconds,
    lifecycleBackendSettlement: policies(LIFECYCLE_METHODS),
    publicationSettlement: policies(PUBLICATION_METHODS),
    resolveRestoreDestinationSettlement: policy(),
    safetyMarginMilliseconds: 5,
    supervisorSettlement: policies(SUPERVISOR_METHODS),
  };
}

function createPlan(leaseDurationMilliseconds = 29) {
  return createPostgresDetachedRestorePlan({
    request: {
      backendId: "storage-backend-001",
      contractVersion: 1,
      fencingEpoch: "42",
      holderId: "restore-holder-001",
      leaseId: "restore-lease-001",
      operation: "restore",
      operationId: "restore-root-001",
      sessionId: "018f8475-7c55-7a11-8a88-001122334455",
      storageId: "storage-001",
      target: {
        artifactId: "artifact-source-001",
        checkpointId: "checkpoint-source-001",
        kind: "checkpoint",
      },
    },
    plan: {
      captureCreatedAt: "2026-08-12T09:00:00.000Z",
      destinationDirectory: "/var/lib/portable-codex/restores/destination-001",
      destinationOwnedRoot: "/var/lib/portable-codex/restores",
      detachMode: "release",
      holderId: "writer-holder-002",
      imagePlanId: "image-plan-001",
      leaseDurationMilliseconds,
      sourceArtifactDirectory: "/var/lib/portable-codex/artifacts/source-001",
      sourceArtifactOwnedRoot: "/var/lib/portable-codex/artifacts",
    },
  });
}

function budgetError(code) {
  return (error) => {
    assert(error instanceof PostgresDetachedRestoreOperationalLeaseBudgetError);
    assert.equal(error.code, code);
    assert.equal(error.retryable, false);
    assert.equal(Object.isFrozen(error), true);
    return true;
  };
}

test("operational lease budget derives two exact database-clock windows", () => {
  const budget = createPostgresDetachedRestoreOperationalLeaseBudget(options());
  assert.equal(isPostgresDetachedRestoreOperationalLeaseBudget(budget), true);
  assert.deepEqual(Reflect.ownKeys(budget), [
    "contractVersion",
    "databaseRequestMilliseconds",
    "leaseDurationMilliseconds",
    "minimumLeaseDurationMilliseconds",
    "safetyMarginMilliseconds",
    "windows",
  ]);
  assert.equal(Object.getPrototypeOf(budget), null);
  assert.equal(Object.isFrozen(budget), true);
  assert.equal(Object.getPrototypeOf(budget.windows), null);
  assert.equal(Object.isFrozen(budget.windows), true);
  assert.deepEqual({ ...budget.windows }, {
    activationToLaunchClaimMilliseconds: 29,
    renewalToGenerationClaimMilliseconds: 21,
  });
  assert.equal(budget.minimumLeaseDurationMilliseconds, 29);
  assert.equal(budget.leaseDurationMilliseconds, 29);
});

test("renewal window can independently determine the minimum lease", () => {
  const exact = options(49);
  exact.supervisorSettlement.stopWriter = policy(20, 10);
  const budget = createPostgresDetachedRestoreOperationalLeaseBudget(exact);
  assert.deepEqual({ ...budget.windows }, {
    activationToLaunchClaimMilliseconds: 29,
    renewalToGenerationClaimMilliseconds: 49,
  });
  assert.equal(budget.minimumLeaseDurationMilliseconds, 49);
  assert.equal(budget.leaseDurationMilliseconds, 49);

  const short = options(48);
  short.supervisorSettlement.stopWriter = policy(20, 10);
  assert.throws(
    () => createPostgresDetachedRestoreOperationalLeaseBudget(short),
    budgetError(
      "invalid_postgres_detached_restore_operational_lease_budget_options",
    ),
  );
});

test("critical paths count every distinguishable sequential boundary exactly once", () => {
  const value = options(183);
  value.databaseRequestMilliseconds = 25;
  value.safetyMarginMilliseconds = 27;
  value.supervisorSettlement.stopWriter = policy(2, 3);
  value.publicationSettlement.publishFreshCheckpointArtifact = policy(4, 5);
  value.publicationSettlement.verifyCommittedCheckpointArtifact = policy(6, 5);
  value.resolveRestoreDestinationSettlement = policy(7, 6);
  value.publicationSettlement.verifyCommittedRestoreDestination = policy(8, 7);
  value.lifecycleBackendSettlement.reconcileRestoreAttachment = policy(9, 8);
  value.lifecycleBackendSettlement.prepareRestoreAttachment = policy(10, 9);
  value.imagePlanProviderSettlement.resolveImagePlan = policy(11, 10);
  value.imagePlanProviderSettlement.inspectCodex = policy(12, 11);
  const budget = createPostgresDetachedRestoreOperationalLeaseBudget(value);
  assert.equal(
    budget.windows.renewalToGenerationClaimMilliseconds,
    5 + 9 + 11 + 25 + 27,
  );
  // The retained-prepared continuation is the strict positive-term upper
  // bound: common 13 + 15 + 17, prepare 19, image resolve 21, two distinct
  // uses of the 23-unit inspection policy, then database 25 + margin 27.
  assert.equal(
    budget.windows.activationToLaunchClaimMilliseconds,
    13 + 15 + 17 + 19 + 21 + 23 + 23 + 25 + 27,
  );
  assert.equal(budget.minimumLeaseDurationMilliseconds, 183);
});

test("all nineteen policy leaves are validated but non-window methods do not inflate a lease", () => {
  const baseline = createPostgresDetachedRestoreOperationalLeaseBudget(options());
  const groups = [
    ["imagePlanProviderSettlement", IMAGE_METHODS],
    ["supervisorSettlement", SUPERVISOR_METHODS],
    ["lifecycleBackendSettlement", LIFECYCLE_METHODS],
    ["publicationSettlement", PUBLICATION_METHODS],
  ];
  for (const [groupName, methods] of groups) {
    for (const method of methods) {
      const value = options();
      value[groupName][method] = policy(0, 1);
      assert.throws(
        () => createPostgresDetachedRestoreOperationalLeaseBudget(value),
        budgetError(
          "invalid_postgres_detached_restore_operational_lease_budget_options",
        ),
      );
    }
  }
  const invalidResolver = options();
  invalidResolver.resolveRestoreDestinationSettlement = policy(1, 0);
  assert.throws(
    () => createPostgresDetachedRestoreOperationalLeaseBudget(invalidResolver),
    budgetError(
      "invalid_postgres_detached_restore_operational_lease_budget_options",
    ),
  );

  const nonWindow = options();
  for (const method of [
    "captureCheckpoint",
    "destroySession",
    "detachAttachment",
    "forceFence",
    "prepareWritableAttachment",
    "provisionSession",
    "restoreCheckpoint",
  ]) {
    nonWindow.lifecycleBackendSettlement[method] = policy(86_400_000, 86_400_000);
  }
  nonWindow.publicationSettlement.publishRestoreDestination = policy(
    86_400_000,
    86_400_000,
  );
  nonWindow.supervisorSettlement.launchWriter = policy(86_400_000, 86_400_000);
  nonWindow.supervisorSettlement.reconcileWriterLaunch = policy(
    86_400_000,
    86_400_000,
  );
  const unchanged = createPostgresDetachedRestoreOperationalLeaseBudget(nonWindow);
  assert.deepEqual({ ...unchanged.windows }, { ...baseline.windows });
});

test("short, overflowing, accessor, proxy, and extra-key options fail closed", () => {
  assert.throws(
    () => createPostgresDetachedRestoreOperationalLeaseBudget(options(28)),
    budgetError(
      "invalid_postgres_detached_restore_operational_lease_budget_options",
    ),
  );
  const overflowing = options(86_400_000);
  overflowing.imagePlanProviderSettlement.inspectCodex = policy(
    86_400_000,
    86_400_000,
  );
  assert.throws(
    () => createPostgresDetachedRestoreOperationalLeaseBudget(overflowing),
    budgetError(
      "invalid_postgres_detached_restore_operational_lease_budget_options",
    ),
  );
  const accessor = options();
  let getterCalls = 0;
  Object.defineProperty(accessor, "databaseRequestMilliseconds", {
    enumerable: true,
    get() {
      getterCalls += 1;
      return 10;
    },
  });
  assert.throws(
    () => createPostgresDetachedRestoreOperationalLeaseBudget(accessor),
    budgetError(
      "invalid_postgres_detached_restore_operational_lease_budget_options",
    ),
  );
  assert.equal(getterCalls, 0);
  assert.throws(
    () => createPostgresDetachedRestoreOperationalLeaseBudget(new Proxy(options(), {})),
    budgetError(
      "invalid_postgres_detached_restore_operational_lease_budget_options",
    ),
  );
  assert.throws(
    () =>
      createPostgresDetachedRestoreOperationalLeaseBudget({
        ...options(),
        extra: true,
      }),
    budgetError(
      "invalid_postgres_detached_restore_operational_lease_budget_options",
    ),
  );
});

test("only a branded budget admits an authentic plan with the exact configured lease", () => {
  const budget = createPostgresDetachedRestoreOperationalLeaseBudget(options());
  const plan = createPlan();
  assert.equal(assertPostgresDetachedRestoreOperationalLeasePlan(budget, plan), plan);
  assert.equal(assertPostgresDetachedRestoreOperationalLeaseDuration(budget, 29), 29);
  assert.throws(
    () => assertPostgresDetachedRestoreOperationalLeasePlan(budget, createPlan(30)),
    budgetError("postgres_detached_restore_operational_lease_required"),
  );
  assert.throws(
    () => assertPostgresDetachedRestoreOperationalLeasePlan(Object.freeze({}), plan),
    budgetError("postgres_detached_restore_operational_lease_required"),
  );
  assert.throws(
    () => assertPostgresDetachedRestoreOperationalLeaseDuration(budget, 30),
    budgetError("postgres_detached_restore_operational_lease_required"),
  );
  assert.equal(isPostgresDetachedRestoreOperationalLeaseBudget(Object.freeze({})), false);
});
