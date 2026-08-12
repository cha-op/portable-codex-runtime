import { types as utilTypes } from "node:util";

import { isPostgresDetachedRestorePlan } from "./postgres-detached-restore-plan.mjs";

const ErrorConstructor = Error;
const isProxyValue = utilTypes.isProxy;
const numberIsSafeInteger = Number.isSafeInteger;
const objectCreate = Object.create;
const objectDefineProperty = Object.defineProperty;
const objectFreeze = Object.freeze;
const objectGetOwnPropertyDescriptor = Object.getOwnPropertyDescriptor;
const objectGetPrototypeOf = Object.getPrototypeOf;
const objectHasOwn = Object.hasOwn;
const objectPrototype = Object.prototype;
const reflectApply = Reflect.apply;
const reflectOwnKeys = Reflect.ownKeys;
const mathMax = Math.max;
const TypeErrorConstructor = TypeError;
const weakSetAdd = WeakSet.prototype.add;
const weakSetHas = WeakSet.prototype.has;
const WeakSetConstructor = WeakSet;

export const POSTGRES_DETACHED_RESTORE_OPERATIONAL_LEASE_BUDGET_CONTRACT_VERSION =
  1;

const MAX_MILLISECONDS = 86_400_000;
const OPTION_CODE =
  "invalid_postgres_detached_restore_operational_lease_budget_options";
const PLAN_CODE = "postgres_detached_restore_operational_lease_required";
const ERROR_MESSAGES = objectFreeze({
  [OPTION_CODE]: "Invalid PostgreSQL detached restore operational lease budget options",
  [PLAN_CODE]: "PostgreSQL detached restore operational lease is required",
});

const FACTORY_OPTION_KEYS = objectFreeze([
  "databaseRequestMilliseconds",
  "imagePlanProviderSettlement",
  "leaseDurationMilliseconds",
  "lifecycleBackendSettlement",
  "publicationSettlement",
  "resolveRestoreDestinationSettlement",
  "safetyMarginMilliseconds",
  "supervisorSettlement",
]);
const POLICY_KEYS = objectFreeze([
  "deadlineMilliseconds",
  "settlementGraceMilliseconds",
]);
const IMAGE_KEYS = objectFreeze(["inspectCodex", "resolveImagePlan"]);
const SUPERVISOR_KEYS = objectFreeze([
  "launchWriter",
  "reconcileWriterLaunch",
  "stopWriter",
]);
const LIFECYCLE_KEYS = objectFreeze([
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
const PUBLICATION_KEYS = objectFreeze([
  "publishFreshCheckpointArtifact",
  "publishRestoreDestination",
  "verifyCommittedCheckpointArtifact",
  "verifyCommittedRestoreDestination",
]);

const budgetBrands = new WeakSetConstructor();

function fail(code) {
  throw new PostgresDetachedRestoreOperationalLeaseBudgetError(code);
}

function ensure(condition, code) {
  if (!condition) fail(code);
}

function exactFrozenRecord(values) {
  const result = objectCreate(null);
  const keys = reflectOwnKeys(values);
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
    objectDefineProperty(result, key, {
      enumerable: true,
      value: values[key],
    });
  }
  return objectFreeze(result);
}

function exactDataObject(value, expectedKeys, code) {
  ensure(value !== null && typeof value === "object" && !isProxyValue(value), code);
  let prototype;
  let keys;
  try {
    prototype = objectGetPrototypeOf(value);
    keys = reflectOwnKeys(value);
  } catch {
    fail(code);
  }
  ensure(prototype === null || prototype === objectPrototype, code);
  ensure(keys.length === expectedKeys.length, code);
  const result = objectCreate(null);
  for (let index = 0; index < expectedKeys.length; index += 1) {
    const key = expectedKeys[index];
    let descriptor;
    try {
      descriptor = objectGetOwnPropertyDescriptor(value, key);
    } catch {
      fail(code);
    }
    ensure(
      descriptor?.enumerable === true && objectHasOwn(descriptor, "value"),
      code,
    );
    objectDefineProperty(result, key, {
      enumerable: true,
      value: descriptor.value,
    });
  }
  return objectFreeze(result);
}

function milliseconds(value, code) {
  ensure(
    numberIsSafeInteger(value) && value >= 1 && value <= MAX_MILLISECONDS,
    code,
  );
  return value;
}

function checkedAdd(left, right, code) {
  ensure(
    numberIsSafeInteger(left) &&
      numberIsSafeInteger(right) &&
      left >= 0 &&
      right >= 0 &&
      left <= MAX_MILLISECONDS - right,
    code,
  );
  return left + right;
}

function policyDuration(value, code) {
  const policy = exactDataObject(value, POLICY_KEYS, code);
  const deadline = milliseconds(policy.deadlineMilliseconds, code);
  const grace = milliseconds(policy.settlementGraceMilliseconds, code);
  const duration = deadline + grace;
  ensure(
    numberIsSafeInteger(duration) && duration > 0,
    code,
  );
  return duration;
}

function policyDurations(value, keys, code) {
  const policies = exactDataObject(value, keys, code);
  const result = objectCreate(null);
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
    objectDefineProperty(result, key, {
      enumerable: true,
      value: policyDuration(policies[key], code),
    });
  }
  return objectFreeze(result);
}

function addTerms(terms, code) {
  let total = 0;
  for (let index = 0; index < terms.length; index += 1) {
    total = checkedAdd(total, terms[index], code);
  }
  return total;
}

export class PostgresDetachedRestoreOperationalLeaseBudgetError extends ErrorConstructor {
  constructor(code) {
    if (typeof code !== "string" || !objectHasOwn(ERROR_MESSAGES, code)) {
      throw new TypeErrorConstructor(
        "Unsupported PostgreSQL detached restore operational lease budget error",
      );
    }
    const message = ERROR_MESSAGES[code];
    super(message);
    objectDefineProperty(this, "name", {
      value: "PostgresDetachedRestoreOperationalLeaseBudgetError",
    });
    objectDefineProperty(this, "code", { enumerable: true, value: code });
    objectDefineProperty(this, "retryable", {
      enumerable: true,
      value: false,
    });
    objectDefineProperty(this, "stack", {
      value: `PostgresDetachedRestoreOperationalLeaseBudgetError: ${message}`,
    });
    objectFreeze(this);
  }
}

export function createPostgresDetachedRestoreOperationalLeaseBudget(...args) {
  ensure(args.length === 1, OPTION_CODE);
  const options = exactDataObject(args[0], FACTORY_OPTION_KEYS, OPTION_CODE);
  const databaseRequestMilliseconds = milliseconds(
    options.databaseRequestMilliseconds,
    OPTION_CODE,
  );
  const leaseDurationMilliseconds = milliseconds(
    options.leaseDurationMilliseconds,
    OPTION_CODE,
  );
  const safetyMarginMilliseconds = milliseconds(
    options.safetyMarginMilliseconds,
    OPTION_CODE,
  );
  const image = policyDurations(
    options.imagePlanProviderSettlement,
    IMAGE_KEYS,
    OPTION_CODE,
  );
  const supervisor = policyDurations(
    options.supervisorSettlement,
    SUPERVISOR_KEYS,
    OPTION_CODE,
  );
  const lifecycle = policyDurations(
    options.lifecycleBackendSettlement,
    LIFECYCLE_KEYS,
    OPTION_CODE,
  );
  const publication = policyDurations(
    options.publicationSettlement,
    PUBLICATION_KEYS,
    OPTION_CODE,
  );
  const resolver = policyDuration(
    options.resolveRestoreDestinationSettlement,
    OPTION_CODE,
  );

  const databaseAndMargin = addTerms(
    [databaseRequestMilliseconds, safetyMarginMilliseconds],
    OPTION_CODE,
  );
  const renewalToGenerationClaimMilliseconds = addTerms(
    [
      supervisor.stopWriter,
      publication.publishFreshCheckpointArtifact,
      publication.verifyCommittedCheckpointArtifact,
      databaseAndMargin,
    ],
    OPTION_CODE,
  );
  const activationCommon = addTerms(
    [
      resolver,
      publication.verifyCommittedRestoreDestination,
      lifecycle.reconcileRestoreAttachment,
    ],
    OPTION_CODE,
  );
  const activationToLaunchClaimMilliseconds = addTerms(
    [
      activationCommon,
      lifecycle.prepareRestoreAttachment,
      image.resolveImagePlan,
      image.inspectCodex,
      image.inspectCodex,
      databaseAndMargin,
    ],
    OPTION_CODE,
  );
  const minimumLeaseDurationMilliseconds = reflectApply(mathMax, undefined, [
    renewalToGenerationClaimMilliseconds,
    activationToLaunchClaimMilliseconds,
  ]);
  ensure(
    leaseDurationMilliseconds >= minimumLeaseDurationMilliseconds,
    OPTION_CODE,
  );

  const budget = exactFrozenRecord({
    contractVersion:
      POSTGRES_DETACHED_RESTORE_OPERATIONAL_LEASE_BUDGET_CONTRACT_VERSION,
    databaseRequestMilliseconds,
    leaseDurationMilliseconds,
    minimumLeaseDurationMilliseconds,
    safetyMarginMilliseconds,
    windows: exactFrozenRecord({
      activationToLaunchClaimMilliseconds,
      renewalToGenerationClaimMilliseconds,
    }),
  });
  reflectApply(weakSetAdd, budgetBrands, [budget]);
  return budget;
}

export function isPostgresDetachedRestoreOperationalLeaseBudget(value) {
  try {
    return (
      arguments.length === 1 &&
      value !== null &&
      typeof value === "object" &&
      !isProxyValue(value) &&
      reflectApply(weakSetHas, budgetBrands, [value])
    );
  } catch {
    return false;
  }
}

export function assertPostgresDetachedRestoreOperationalLeasePlan(...args) {
  ensure(args.length === 2, PLAN_CODE);
  const [budget, plan] = args;
  ensure(
    isPostgresDetachedRestoreOperationalLeaseBudget(budget) &&
      isPostgresDetachedRestorePlan(plan) &&
      plan.leaseDurationMilliseconds === budget.leaseDurationMilliseconds &&
      plan.leaseDurationMilliseconds >= budget.minimumLeaseDurationMilliseconds,
    PLAN_CODE,
  );
  return plan;
}

export function assertPostgresDetachedRestoreOperationalLeaseDuration(...args) {
  ensure(args.length === 2, PLAN_CODE);
  const [budget, leaseDurationMilliseconds] = args;
  ensure(
    isPostgresDetachedRestoreOperationalLeaseBudget(budget) &&
      numberIsSafeInteger(leaseDurationMilliseconds) &&
      leaseDurationMilliseconds === budget.leaseDurationMilliseconds &&
      leaseDurationMilliseconds >= budget.minimumLeaseDurationMilliseconds,
    PLAN_CODE,
  );
  return leaseDurationMilliseconds;
}

objectFreeze(PostgresDetachedRestoreOperationalLeaseBudgetError.prototype);
objectFreeze(PostgresDetachedRestoreOperationalLeaseBudgetError);
objectFreeze(createPostgresDetachedRestoreOperationalLeaseBudget);
objectFreeze(isPostgresDetachedRestoreOperationalLeaseBudget);
objectFreeze(assertPostgresDetachedRestoreOperationalLeasePlan);
objectFreeze(assertPostgresDetachedRestoreOperationalLeaseDuration);
