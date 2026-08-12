import { Hash, createHash as createHashExport } from "node:crypto";
import { types as utilTypes } from "node:util";

import {
  POSTGRES_DETACHED_RESTORE_PLAN_CONTRACT_VERSION,
  createPostgresDetachedRestorePlan,
  isPostgresDetachedRestorePlan,
} from "./postgres-detached-restore-plan.mjs";
import {
  PostgresSerializableStore,
  PostgresSerializableStoreError,
} from "./postgres-serializable-store.mjs";
import { assertSessionAuthoritySnapshot } from "./postgres-session-authority.mjs";
import {
  assertCheckpointDescriptor,
  assertRestoreCheckpointAdmission,
} from "./session-storage-contracts.mjs";

const arrayIncludesIntrinsic = Array.prototype.includes;
const arrayIsArray = Array.isArray;
const arrayPrototype = Array.prototype;
const createHashIntrinsic = createHashExport;
const DateConstructor = Date;
const dateGetTimeIntrinsic = Date.prototype.getTime;
const dateParseIntrinsic = Date.parse;
const dateToISOStringIntrinsic = Date.prototype.toISOString;
const ErrorConstructor = Error;
const hashDigestIntrinsic = Hash.prototype.digest;
const hashUpdateIntrinsic = Hash.prototype.update;
const isGeneratorFunctionValue = utilTypes.isGeneratorFunction;
const isGeneratorObjectValue = utilTypes.isGeneratorObject;
const isPromiseValue = utilTypes.isPromise;
const isProxyValue = utilTypes.isProxy;
const JSONValue = JSON;
const jsonStringifyIntrinsic = JSON.stringify;
const numberIsFinite = Number.isFinite;
const numberIsSafeInteger = Number.isSafeInteger;
const objectCreate = Object.create;
const objectDefineProperty = Object.defineProperty;
const objectDefineProperties = Object.defineProperties;
const objectFreeze = Object.freeze;
const objectGetOwnPropertyDescriptor = Object.getOwnPropertyDescriptor;
const objectGetPrototypeOf = Object.getPrototypeOf;
const objectHasOwn = Object.hasOwn;
const objectIsFrozen = Object.isFrozen;
const objectPrototype = Object.prototype;
const PromiseConstructor = Promise;
const promisePrototype = Promise.prototype;
const promiseResolveIntrinsic = Promise.resolve;
const promiseSpeciesSymbol = Symbol.species;
const promiseThenIntrinsic = Promise.prototype.then;
const promiseSpeciesHolder = objectFreeze(
  objectCreate(null, {
    [promiseSpeciesSymbol]: {
      configurable: false,
      enumerable: false,
      value: PromiseConstructor,
      writable: false,
    },
  }),
);
const reflectApply = Reflect.apply;
const reflectOwnKeys = Reflect.ownKeys;
const regexpExecIntrinsic = RegExp.prototype.exec;
const runSerializableIntrinsic =
  PostgresSerializableStore.prototype.runSerializable;
const TypeErrorConstructor = TypeError;
const weakSetAddIntrinsic = WeakSet.prototype.add;
const weakSetHasIntrinsic = WeakSet.prototype.has;
const WeakSetConstructor = WeakSet;

export const POSTGRES_DETACHED_RESTORE_STABLE_PLAN_REGISTRY_CONTRACT_VERSION =
  1;
export const POSTGRES_DETACHED_RESTORE_STABLE_PLAN_PROVISIONING_CONFIRMED =
  objectFreeze(objectCreate(null));

const CLAIM_TYPE = "detached-restore-stable-plan-v1";
const BINDING_DOMAIN =
  "portable-codex-runtime:postgres-detached-restore-plan-registry-binding:v1";
const MAX_DATA_DEPTH = 24;
const MAX_DATA_NODES = 8_192;
// The largest V1 canonical object is the 21-field persisted plan.
const MAX_DATA_OBJECT_KEYS = 24;
const OPAQUE_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u;
const SHA256_PATTERN = /^[0-9a-f]{64}$/u;
const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u;

const FACTORY_OPTION_KEYS = objectFreeze([
  "provisioningFleetCapabilityGate",
  "store",
]);
const PROVISION_KEYS = objectFreeze(["admission", "plan"]);
const RESOLVE_KEYS = objectFreeze(["admission", "expectedSession"]);
const PLAN_INPUT_KEYS = objectFreeze([
  "captureCreatedAt",
  "destinationDirectory",
  "destinationOwnedRoot",
  "detachMode",
  "holderId",
  "imagePlanId",
  "leaseDurationMilliseconds",
  "sourceArtifactDirectory",
  "sourceArtifactOwnedRoot",
]);
const CLAIM_BINDING_KEYS = objectFreeze([
  "bindingSha256",
  "contractVersion",
  "planSha256",
  "request",
]);
const SESSION_ROW_KEYS = objectFreeze(["session_id"]);
const REGISTRY_ROW_KEYS = objectFreeze([
  "admission",
  "backend_id",
  "binding_sha256",
  "claim_binding",
  "claim_type",
  "claimed_at",
  "claimant_operation_id",
  "materialized_at",
  "operation_id",
  "plan_contract_version",
  "plan_input",
  "plan_sha256",
  "provisioned_at",
  "session_id",
  "stable_operation_id",
  "stable_session_id",
  "storage_id",
]);

const CLAIM_COLUMNS = [
  "operation_id",
  "session_id",
  "claim_type",
  "claimant_operation_id",
  "binding",
  "claimed_at",
  "materialized_at",
].join(", ");
const SESSION_LOCK_QUERY = [
  "SELECT session_id::pg_catalog.text AS session_id",
  "FROM session_authority.sessions",
  "WHERE session_id = $1::uuid",
  "FOR UPDATE",
].join(" ");
const INSERT_CLAIM_QUERY = [
  "INSERT INTO session_authority.operation_id_registry",
  "(operation_id, session_id, claim_type, claimant_operation_id, binding,",
  "claimed_at, materialized_at)",
  `VALUES ($1, $2::uuid, '${CLAIM_TYPE}', NULL, $3::jsonb, $4, NULL)`,
  "ON CONFLICT (operation_id) DO NOTHING",
  `RETURNING ${CLAIM_COLUMNS}`,
].join(" ");
const INSERT_PLAN_QUERY = [
  "INSERT INTO session_authority.detached_restore_stable_plans",
  "(operation_id, session_id, backend_id, storage_id,",
  "plan_contract_version, admission, plan_input, plan_sha256,",
  "binding_sha256, provisioned_at)",
  "VALUES ($1, $2::uuid, $3, $4, $5, $6::jsonb, $7::jsonb, $8, $9, $10)",
  "ON CONFLICT (operation_id) DO NOTHING",
  "RETURNING operation_id",
].join(" ");
const REGISTRY_COLUMNS = [
  "registry.operation_id AS operation_id",
  "registry.session_id::pg_catalog.text AS session_id",
  "registry.claim_type AS claim_type",
  "registry.claimant_operation_id AS claimant_operation_id",
  "registry.binding AS claim_binding",
  "registry.claimed_at AS claimed_at",
  "registry.materialized_at AS materialized_at",
  "stable.operation_id AS stable_operation_id",
  "stable.session_id::pg_catalog.text AS stable_session_id",
  "stable.backend_id AS backend_id",
  "stable.storage_id AS storage_id",
  "stable.plan_contract_version AS plan_contract_version",
  "stable.admission AS admission",
  "stable.plan_input AS plan_input",
  "stable.plan_sha256 AS plan_sha256",
  "stable.binding_sha256 AS binding_sha256",
  "stable.provisioned_at AS provisioned_at",
].join(", ");
const READ_PLAN_QUERY = [
  `SELECT ${REGISTRY_COLUMNS}`,
  "FROM session_authority.operation_id_registry AS registry",
  "LEFT JOIN session_authority.detached_restore_stable_plans AS stable",
  "ON stable.operation_id = registry.operation_id",
  "AND stable.session_id = registry.session_id",
  "WHERE registry.operation_id = $1",
].join(" ");
const READ_PLAN_FOR_UPDATE_QUERY = `${READ_PLAN_QUERY} FOR UPDATE OF registry`;

const ERROR_MESSAGES = objectFreeze({
  invalid_postgres_detached_restore_stable_plan_registry_options:
    "PostgreSQL detached restore stable-plan registry options are invalid",
  invalid_postgres_detached_restore_stable_plan_registry_request:
    "PostgreSQL detached restore stable-plan registry request is invalid",
  postgres_detached_restore_stable_plan_registry_identity_conflict:
    "PostgreSQL detached restore stable-plan identity conflicts with durable state",
  postgres_detached_restore_stable_plan_registry_not_found:
    "PostgreSQL detached restore stable plan was not found",
  postgres_detached_restore_stable_plan_registry_outcome_uncertain:
    "PostgreSQL detached restore stable-plan outcome is uncertain",
  postgres_detached_restore_stable_plan_provisioning_capability_required:
    "PostgreSQL detached restore stable-plan provisioning fleet capability is required",
  postgres_detached_restore_stable_plan_registry_state_invalid:
    "PostgreSQL detached restore stable-plan durable state is invalid",
});
const internalErrors = new WeakSetConstructor();
const registryBrands = new WeakSetConstructor();
const VISIBILITY_RETRY = objectFreeze(objectCreate(null));

function callIntrinsic(intrinsic, receiver, args) {
  return reflectApply(intrinsic, receiver, args);
}

function arrayIncludes(value, candidate) {
  return callIntrinsic(arrayIncludesIntrinsic, value, [candidate]);
}

function regexpTest(pattern, value) {
  return callIntrinsic(regexpExecIntrinsic, pattern, [value]) !== null;
}

function isInternalError(error) {
  return (
    error !== null &&
    !isProxyValue(error) &&
    callIntrinsic(weakSetHasIntrinsic, internalErrors, [error])
  );
}

function fail(code) {
  const error = new PostgresDetachedRestoreStablePlanRegistryError(code);
  callIntrinsic(weakSetAddIntrinsic, internalErrors, [error]);
  throw error;
}

function ensure(condition, code) {
  if (!condition) fail(code);
}

function ownDataValue(value, key, code) {
  ensure(
    value !== null &&
      (typeof value === "object" || typeof value === "function") &&
      !isProxyValue(value),
    code,
  );
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
  return descriptor.value;
}

function exactDataObject(value, expectedKeys, code) {
  ensure(
    value !== null &&
      typeof value === "object" &&
      !isProxyValue(value) &&
      !arrayIsArray(value),
    code,
  );
  let prototype;
  let keys;
  try {
    prototype = objectGetPrototypeOf(value);
    keys = reflectOwnKeys(value);
  } catch {
    fail(code);
  }
  ensure(
    (prototype === objectPrototype || prototype === null) &&
      keys.length === expectedKeys.length,
    code,
  );
  const normalized = objectCreate(null);
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
    ensure(typeof key === "string" && arrayIncludes(expectedKeys, key), code);
    normalized[key] = ownDataValue(value, key, code);
  }
  return normalized;
}

function exactFrozenRecord(value) {
  const result = objectCreate(null);
  const keys = reflectOwnKeys(value);
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
    objectDefineProperty(result, key, {
      enumerable: true,
      value: value[key],
    });
  }
  return objectFreeze(result);
}

function sortedStringKeys(value, code) {
  let keys;
  try {
    keys = reflectOwnKeys(value);
  } catch {
    fail(code);
  }
  ensure(keys.length <= MAX_DATA_OBJECT_KEYS, code);
  for (let index = 0; index < keys.length; index += 1) {
    ensure(typeof keys[index] === "string", code);
  }
  for (let index = 1; index < keys.length; index += 1) {
    const selected = keys[index];
    let position = index;
    while (position > 0 && selected < keys[position - 1]) {
      keys[position] = keys[position - 1];
      position -= 1;
    }
    keys[position] = selected;
  }
  return keys;
}

function canonicalDataTree(value, code, state = { nodes: 0 }, depth = 0) {
  ensure(depth <= MAX_DATA_DEPTH && state.nodes < MAX_DATA_NODES, code);
  state.nodes += 1;
  if (value === null || typeof value !== "object") {
    ensure(
      value === null ||
        typeof value === "string" ||
        typeof value === "number" ||
        typeof value === "boolean",
      code,
    );
    ensure(typeof value !== "number" || numberIsFinite(value), code);
    return value;
  }
  ensure(
    !isProxyValue(value) && !arrayIsArray(value),
    code,
  );
  let prototype;
  try {
    prototype = objectGetPrototypeOf(value);
  } catch {
    fail(code);
  }
  ensure(prototype === objectPrototype || prototype === null, code);
  const result = objectCreate(null);
  const keys = sortedStringKeys(value, code);
  ensure(keys.length <= MAX_DATA_NODES - state.nodes, code);
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
    const child = ownDataValue(value, key, code);
    objectDefineProperty(result, key, {
      enumerable: true,
      value: canonicalDataTree(child, code, state, depth + 1),
    });
  }
  return objectFreeze(result);
}

function canonicalSerialize(value, code) {
  let serialized;
  try {
    serialized = callIntrinsic(jsonStringifyIntrinsic, JSONValue, [
      canonicalDataTree(value, code),
    ]);
  } catch (error) {
    if (isInternalError(error)) throw error;
    fail(code);
  }
  ensure(typeof serialized === "string", code);
  return serialized;
}

function sameData(left, right, code) {
  return canonicalSerialize(left, code) === canonicalSerialize(right, code);
}

function opaqueId(value, code) {
  ensure(typeof value === "string" && regexpTest(OPAQUE_ID_PATTERN, value), code);
  return value;
}

function canonicalUuid(value, code) {
  ensure(typeof value === "string" && regexpTest(UUID_PATTERN, value), code);
  return value;
}

function sha256Value(value, code) {
  ensure(typeof value === "string" && regexpTest(SHA256_PATTERN, value), code);
  return value;
}

function canonicalTimestamp(value, code) {
  let milliseconds;
  let canonical;
  try {
    milliseconds =
      typeof value === "string"
        ? callIntrinsic(dateParseIntrinsic, DateConstructor, [value])
        : callIntrinsic(dateGetTimeIntrinsic, value, []);
    canonical = callIntrinsic(
      dateToISOStringIntrinsic,
      new DateConstructor(milliseconds),
      [],
    );
  } catch {
    fail(code);
  }
  ensure(
    numberIsFinite(milliseconds) &&
      (typeof value !== "string" || value === canonical),
    code,
  );
  return canonical;
}

function bindingSha256(admission, plan, code) {
  let hash;
  let digest;
  try {
    hash = callIntrinsic(createHashIntrinsic, undefined, ["sha256"]);
    callIntrinsic(hashUpdateIntrinsic, hash, [BINDING_DOMAIN, "utf8"]);
    callIntrinsic(hashUpdateIntrinsic, hash, ["\0", "utf8"]);
    callIntrinsic(hashUpdateIntrinsic, hash, [
      canonicalSerialize(
        exactFrozenRecord({
          admission,
          contractVersion:
            POSTGRES_DETACHED_RESTORE_STABLE_PLAN_REGISTRY_CONTRACT_VERSION,
          plan,
        }),
        code,
      ),
      "utf8",
    ]);
    callIntrinsic(hashUpdateIntrinsic, hash, ["\n", "utf8"]);
    digest = callIntrinsic(hashDigestIntrinsic, hash, ["hex"]);
  } catch (error) {
    if (isInternalError(error)) throw error;
    fail(code);
  }
  return sha256Value(digest, code);
}

function planInputFromPlan(plan) {
  return exactFrozenRecord({
    captureCreatedAt: plan.captureCreatedAt,
    destinationDirectory: plan.destinationDirectory,
    destinationOwnedRoot: plan.destinationOwnedRoot,
    detachMode: plan.detachMode,
    holderId: plan.holderId,
    imagePlanId: plan.imagePlanId,
    leaseDurationMilliseconds: plan.leaseDurationMilliseconds,
    sourceArtifactDirectory: plan.sourceArtifactDirectory,
    sourceArtifactOwnedRoot: plan.sourceArtifactOwnedRoot,
  });
}

function claimBinding(input) {
  return exactFrozenRecord({
    bindingSha256: input.bindingSha256,
    contractVersion:
      POSTGRES_DETACHED_RESTORE_STABLE_PLAN_REGISTRY_CONTRACT_VERSION,
    planSha256: input.plan.planSha256,
    request: input.admission.request,
  });
}

function normalizeAdmission(value, code) {
  try {
    return assertRestoreCheckpointAdmission(value);
  } catch {
    fail(code);
  }
}

function normalizeProvisionRequest(value, code) {
  const input = exactDataObject(value, PROVISION_KEYS, code);
  const admission = normalizeAdmission(input.admission, code);
  ensure(isPostgresDetachedRestorePlan(input.plan), code);
  ensure(sameData(input.plan.request, admission.request, code), code);
  const planInput = planInputFromPlan(input.plan);
  const binding = bindingSha256(admission, input.plan, code);
  return exactFrozenRecord({
    admission,
    bindingSha256: binding,
    operationId: admission.request.operationId,
    plan: input.plan,
    planInput,
    sessionId: admission.request.sessionId,
  });
}

function normalizeResolveRequest(value, code) {
  const input = exactDataObject(value, RESOLVE_KEYS, code);
  const admission = normalizeAdmission(input.admission, code);
  let expectedSession;
  try {
    expectedSession = assertSessionAuthoritySnapshot(input.expectedSession);
    assertCheckpointDescriptor(admission.checkpoint, {
      manifest: expectedSession.document.manifest,
    });
  } catch {
    fail(code);
  }
  const storageRef = expectedSession.document.storageRef;
  ensure(
    expectedSession.sessionId === admission.request.sessionId &&
      storageRef.sessionId === admission.request.sessionId &&
      storageRef.backendId === admission.request.backendId &&
      storageRef.storageId === admission.request.storageId,
    code,
  );
  return exactFrozenRecord({
    admission,
    operationId: admission.request.operationId,
  });
}

function trustedFunction(value, code) {
  ensure(
    typeof value === "function" &&
      !isProxyValue(value) &&
      !isGeneratorFunctionValue(value),
    code,
  );
  return value;
}

function protectPromiseReaction(callback) {
  if (typeof callback !== "function") return callback;
  return (value) =>
    protectPromise(callIntrinsic(callback, undefined, [value]));
}

function protectedPromiseThen(onFulfilled, onRejected) {
  return protectPromise(
    callIntrinsic(promiseThenIntrinsic, this, [
      protectPromiseReaction(onFulfilled),
      protectPromiseReaction(onRejected),
    ]),
  );
}

function protectedPromiseCatch(onRejected) {
  return callIntrinsic(protectedPromiseThen, this, [undefined, onRejected]);
}

function resolveProtectedPromise(value) {
  return protectPromise(
    callIntrinsic(promiseResolveIntrinsic, PromiseConstructor, [
      protectPromise(value),
    ]),
  );
}

function protectedPromiseFinally(onFinally) {
  if (typeof onFinally !== "function") {
    return callIntrinsic(protectedPromiseThen, this, [onFinally, onFinally]);
  }
  const runFinally = () =>
    resolveProtectedPromise(callIntrinsic(onFinally, undefined, []));
  return callIntrinsic(protectedPromiseThen, this, [
    (value) =>
      callIntrinsic(protectedPromiseThen, runFinally(), [() => value]),
    (reason) =>
      callIntrinsic(protectedPromiseThen, runFinally(), [
        () => {
          throw reason;
        },
      ]),
  ]);
}

function frozenDataDescriptor(descriptor, value) {
  return (
    descriptor?.configurable === false &&
    descriptor.enumerable === false &&
    descriptor.writable === false &&
    objectHasOwn(descriptor, "value") &&
    descriptor.value === value
  );
}

function safePromiseSpeciesHolder(value) {
  if (
    value === null ||
    typeof value !== "object" ||
    isProxyValue(value) ||
    !objectIsFrozen(value)
  ) {
    return false;
  }
  let prototype;
  let keys;
  let descriptor;
  try {
    prototype = objectGetPrototypeOf(value);
    keys = reflectOwnKeys(value);
    descriptor = objectGetOwnPropertyDescriptor(value, promiseSpeciesSymbol);
  } catch {
    return false;
  }
  return (
    prototype === null &&
    keys.length === 1 &&
    keys[0] === promiseSpeciesSymbol &&
    frozenDataDescriptor(descriptor, PromiseConstructor)
  );
}

function protectPromise(value) {
  if (!isPromiseValue(value)) return value;
  ensure(
    !isProxyValue(value) &&
      !isGeneratorObjectValue(value) &&
      objectGetPrototypeOf(value) === promisePrototype,
    "postgres_detached_restore_stable_plan_registry_outcome_uncertain",
  );
  let constructorDescriptor;
  let thenDescriptor;
  let catchDescriptor;
  let finallyDescriptor;
  try {
    constructorDescriptor = objectGetOwnPropertyDescriptor(
      value,
      "constructor",
    );
    thenDescriptor = objectGetOwnPropertyDescriptor(value, "then");
    catchDescriptor = objectGetOwnPropertyDescriptor(value, "catch");
    finallyDescriptor = objectGetOwnPropertyDescriptor(value, "finally");
  } catch {
    fail("postgres_detached_restore_stable_plan_registry_outcome_uncertain");
  }
  const reactionsAreOurs =
    frozenDataDescriptor(thenDescriptor, protectedPromiseThen) &&
    frozenDataDescriptor(catchDescriptor, protectedPromiseCatch) &&
    frozenDataDescriptor(finallyDescriptor, protectedPromiseFinally);
  if (
    frozenDataDescriptor(constructorDescriptor, promiseSpeciesHolder) &&
    reactionsAreOurs
  ) {
    return value;
  }
  if (
    constructorDescriptor !== undefined &&
    safePromiseSpeciesHolder(constructorDescriptor.value) &&
    !frozenDataDescriptor(constructorDescriptor, promiseSpeciesHolder)
  ) {
    let child;
    try {
      child = callIntrinsic(promiseThenIntrinsic, value, [undefined, undefined]);
    } catch {
      fail("postgres_detached_restore_stable_plan_registry_outcome_uncertain");
    }
    return protectPromise(child);
  }
  const storePinned = frozenDataDescriptor(
    constructorDescriptor,
    PromiseConstructor,
  );
  ensure(
    (constructorDescriptor === undefined &&
      thenDescriptor === undefined &&
      catchDescriptor === undefined &&
      finallyDescriptor === undefined) ||
      (storePinned &&
        ((thenDescriptor === undefined &&
          catchDescriptor === undefined &&
          finallyDescriptor === undefined) ||
          reactionsAreOurs)),
    "postgres_detached_restore_stable_plan_registry_outcome_uncertain",
  );
  if (storePinned && reactionsAreOurs) return value;
  const descriptors = {
      catch: {
        configurable: false,
        enumerable: false,
        value: protectedPromiseCatch,
        writable: false,
      },
      finally: {
        configurable: false,
        enumerable: false,
        value: protectedPromiseFinally,
        writable: false,
      },
      then: {
        configurable: false,
        enumerable: false,
        value: protectedPromiseThen,
        writable: false,
      },
  };
  if (!storePinned) {
    descriptors.constructor = {
      configurable: false,
      enumerable: false,
      value: promiseSpeciesHolder,
      writable: false,
    };
  }
  try {
    objectDefineProperties(value, descriptors);
  } catch {
    fail("postgres_detached_restore_stable_plan_registry_outcome_uncertain");
  }
  return value;
}

function storeFromValue(value, code) {
  ensure(
    value !== null && typeof value === "object" && !isProxyValue(value),
    code,
  );
  let prototype;
  let keys;
  try {
    prototype = objectGetPrototypeOf(value);
    keys = reflectOwnKeys(value);
  } catch {
    fail(code);
  }
  ensure(
    prototype === PostgresSerializableStore.prototype &&
      keys.length === 0 &&
      objectIsFrozen(value),
    code,
  );
  return value;
}

function rowsFromResult(value, code) {
  const rows = ownDataValue(value, "rows", code);
  let prototype;
  let lengthDescriptor;
  let keys;
  try {
    prototype = objectGetPrototypeOf(rows);
    lengthDescriptor = objectGetOwnPropertyDescriptor(rows, "length");
    keys = reflectOwnKeys(rows);
  } catch {
    fail(code);
  }
  ensure(
    arrayIsArray(rows) &&
      !isProxyValue(rows) &&
      prototype === arrayPrototype &&
      lengthDescriptor?.enumerable === false &&
      objectHasOwn(lengthDescriptor, "value"),
    code,
  );
  const length = lengthDescriptor.value;
  ensure(
    numberIsSafeInteger(length) &&
      length >= 0 &&
      length <= 2 &&
      keys.length === length + 1,
    code,
  );
  const result = [];
  for (let index = 0; index < length; index += 1) {
    result[index] = ownDataValue(rows, String(index), code);
  }
  return result;
}

function runSerializable(store, callback) {
  let promise;
  try {
    promise = callIntrinsic(runSerializableIntrinsic, store, [callback]);
  } catch {
    fail("postgres_detached_restore_stable_plan_registry_outcome_uncertain");
  }
  return protectPromise(promise);
}

function isStoreError(error) {
  if (
    error === null ||
    typeof error !== "object" ||
    isProxyValue(error)
  ) {
    return false;
  }
  try {
    return objectGetPrototypeOf(error) === PostgresSerializableStoreError.prototype;
  } catch {
    return false;
  }
}

function storeCommitState(error) {
  if (!isStoreError(error)) return null;
  try {
    const value = ownDataValue(
      error,
      "commitState",
      "postgres_detached_restore_stable_plan_registry_outcome_uncertain",
    );
    return value === "not-committed" || value === "uncertain" ? value : null;
  } catch {
    return null;
  }
}

function exactNativePromise(value, code) {
  ensure(
    isPromiseValue(value) &&
      !isProxyValue(value) &&
      !isGeneratorObjectValue(value),
    code,
  );
  let prototype;
  let ownConstructor;
  let ownThen;
  let inheritedConstructor;
  try {
    prototype = objectGetPrototypeOf(value);
    ownConstructor = objectGetOwnPropertyDescriptor(value, "constructor");
    ownThen = objectGetOwnPropertyDescriptor(value, "then");
    inheritedConstructor = objectGetOwnPropertyDescriptor(
      promisePrototype,
      "constructor",
    );
  } catch {
    fail(code);
  }
  ensure(
    prototype === promisePrototype &&
      ownConstructor === undefined &&
      ownThen === undefined &&
      inheritedConstructor?.enumerable === false &&
      objectHasOwn(inheritedConstructor, "value") &&
      inheritedConstructor.value === PromiseConstructor,
    code,
  );
  return value;
}

async function invokeProvisioningGate(gate, input) {
  const code =
    "postgres_detached_restore_stable_plan_provisioning_capability_required";
  let result;
  try {
    result = callIntrinsic(gate, undefined, [
      exactFrozenRecord({ admission: input.admission, plan: input.plan }),
    ]);
  } catch {
    fail(code);
  }
  if (result !== POSTGRES_DETACHED_RESTORE_STABLE_PLAN_PROVISIONING_CONFIRMED) {
    let settled;
    try {
      const promise = protectPromise(exactNativePromise(result, code));
      settled = await protectPromise(
        callIntrinsic(promiseThenIntrinsic, promise, [
          (value) => exactFrozenRecord({ ok: true, value }),
          () => exactFrozenRecord({ ok: false, value: null }),
        ]),
      );
    } catch (error) {
      if (isInternalError(error)) throw error;
      fail(code);
    }
    ensure(
      settled.ok === true &&
        settled.value ===
          POSTGRES_DETACHED_RESTORE_STABLE_PLAN_PROVISIONING_CONFIRMED,
      code,
    );
  }
}

function normalizeClaimBinding(value, admission, code) {
  const input = exactDataObject(value, CLAIM_BINDING_KEYS, code);
  ensure(
    input.contractVersion ===
      POSTGRES_DETACHED_RESTORE_STABLE_PLAN_REGISTRY_CONTRACT_VERSION,
    code,
  );
  let normalizedRequest;
  try {
    normalizedRequest = assertRestoreCheckpointAdmission(
      exactFrozenRecord({
        checkpoint: admission.checkpoint,
        request: input.request,
      }),
    ).request;
  } catch {
    fail(code);
  }
  const normalized = exactFrozenRecord({
    bindingSha256: sha256Value(input.bindingSha256, code),
    contractVersion:
      POSTGRES_DETACHED_RESTORE_STABLE_PLAN_REGISTRY_CONTRACT_VERSION,
    planSha256: sha256Value(input.planSha256, code),
    request: normalizedRequest,
  });
  ensure(sameData(normalized.request, admission.request, code), code);
  return normalized;
}

function normalizeRegistryRow(row, code) {
  const input = exactDataObject(row, REGISTRY_ROW_KEYS, code);
  const operationId = opaqueId(input.operation_id, code);
  const sessionId = canonicalUuid(input.session_id, code);
  if (input.claim_type !== CLAIM_TYPE) {
    return exactFrozenRecord({ operationId, status: "identity-conflict" });
  }
  ensure(
    input.claimant_operation_id === null &&
      input.stable_operation_id !== null &&
      input.stable_session_id !== null,
    code,
  );
  const stableOperationId = opaqueId(input.stable_operation_id, code);
  const stableSessionId = canonicalUuid(input.stable_session_id, code);
  const claimedAt = canonicalTimestamp(input.claimed_at, code);
  const materializedAt =
    input.materialized_at === null
      ? null
      : canonicalTimestamp(input.materialized_at, code);
  const provisionedAt = canonicalTimestamp(input.provisioned_at, code);
  ensure(
    operationId === stableOperationId &&
      sessionId === stableSessionId &&
      claimedAt === provisionedAt &&
      (materializedAt === null ||
        callIntrinsic(dateParseIntrinsic, DateConstructor, [materializedAt]) >=
          callIntrinsic(dateParseIntrinsic, DateConstructor, [claimedAt])) &&
      input.plan_contract_version ===
        POSTGRES_DETACHED_RESTORE_PLAN_CONTRACT_VERSION,
    code,
  );
  const admission = normalizeAdmission(input.admission, code);
  const planInput = exactDataObject(input.plan_input, PLAN_INPUT_KEYS, code);
  let plan;
  try {
    plan = createPostgresDetachedRestorePlan({
      plan: planInput,
      request: admission.request,
    });
  } catch {
    fail(code);
  }
  const planSha256 = sha256Value(input.plan_sha256, code);
  const durableBindingSha256 = sha256Value(input.binding_sha256, code);
  const expectedBindingSha256 = bindingSha256(admission, plan, code);
  const durableClaimBinding = normalizeClaimBinding(
    input.claim_binding,
    admission,
    code,
  );
  const expectedClaimBinding = exactFrozenRecord({
    bindingSha256: expectedBindingSha256,
    contractVersion:
      POSTGRES_DETACHED_RESTORE_STABLE_PLAN_REGISTRY_CONTRACT_VERSION,
    planSha256: plan.planSha256,
    request: admission.request,
  });
  ensure(
    operationId === admission.request.operationId &&
      sessionId === admission.request.sessionId &&
      opaqueId(input.backend_id, code) === admission.request.backendId &&
      opaqueId(input.storage_id, code) === admission.request.storageId &&
      planSha256 === plan.planSha256 &&
      durableBindingSha256 === expectedBindingSha256 &&
      sameData(durableClaimBinding, expectedClaimBinding, code),
    code,
  );
  return exactFrozenRecord({
    admission,
    bindingSha256: expectedBindingSha256,
    operationId,
    plan,
    provisionedAt,
    sessionId,
    status: "stable-plan",
  });
}

async function queryRows(transaction, text, values, code) {
  return exactFrozenRecord({
    rows: rowsFromResult(
      await protectPromise(transaction.query(text, values)),
      code,
    ),
  });
}

function readRegistryTransaction(store, operationId, forUpdate) {
  return runSerializable(store, (transaction) =>
    protectPromise(
      (async () => {
        const code =
          "postgres_detached_restore_stable_plan_registry_state_invalid";
        const rowsResult = await protectPromise(
          queryRows(
            transaction,
            forUpdate ? READ_PLAN_FOR_UPDATE_QUERY : READ_PLAN_QUERY,
            [operationId],
            code,
          ),
        );
        const rows = rowsResult.rows;
        ensure(rows.length <= 1, code);
        return rows.length === 0 ? null : normalizeRegistryRow(rows[0], code);
      })(),
    ),
  );
}

function compareObserved(observed, input) {
  if (observed === null) return null;
  if (observed.status !== "stable-plan") {
    fail("postgres_detached_restore_stable_plan_registry_identity_conflict");
  }
  ensure(
    sameData(
      observed.admission,
      input.admission,
      "postgres_detached_restore_stable_plan_registry_identity_conflict",
    ),
    "postgres_detached_restore_stable_plan_registry_identity_conflict",
  );
  if (input.plan !== undefined) {
    ensure(
      sameData(
        observed.plan,
        input.plan,
        "postgres_detached_restore_stable_plan_registry_identity_conflict",
      ),
      "postgres_detached_restore_stable_plan_registry_identity_conflict",
    );
  }
  return observed.plan;
}

function provisionTransaction(store, input) {
  return runSerializable(store, (transaction) =>
    protectPromise(
      (async () => {
        const code =
          "postgres_detached_restore_stable_plan_registry_state_invalid";
        const sessionRowsResult = await protectPromise(
          queryRows(
            transaction,
            SESSION_LOCK_QUERY,
            [input.sessionId],
            code,
          ),
        );
        const sessionRows = sessionRowsResult.rows;
        ensure(sessionRows.length <= 1, code);
        if (sessionRows.length === 0) {
          fail(
            "postgres_detached_restore_stable_plan_registry_identity_conflict",
          );
        }
        const sessionRow = exactDataObject(
          sessionRows[0],
          SESSION_ROW_KEYS,
          code,
        );
        ensure(sessionRow.session_id === input.sessionId, code);
        const binding = claimBinding(input);
        const claimRowsResult = await protectPromise(
          queryRows(
            transaction,
            INSERT_CLAIM_QUERY,
            [
              input.operationId,
              input.sessionId,
              canonicalSerialize(binding, code),
              transaction.now,
            ],
            code,
          ),
        );
        const claimRows = claimRowsResult.rows;
        ensure(claimRows.length <= 1, code);
        if (claimRows.length === 1) {
          const planRowsResult = await protectPromise(
            queryRows(
              transaction,
              INSERT_PLAN_QUERY,
              [
                input.operationId,
                input.sessionId,
                input.admission.request.backendId,
                input.admission.request.storageId,
                POSTGRES_DETACHED_RESTORE_PLAN_CONTRACT_VERSION,
                canonicalSerialize(input.admission, code),
                canonicalSerialize(input.planInput, code),
                input.plan.planSha256,
                input.bindingSha256,
                transaction.now,
              ],
              code,
            ),
          );
          const planRows = planRowsResult.rows;
          ensure(planRows.length === 1, code);
        }
        const observedRowsResult = await protectPromise(
          queryRows(
            transaction,
            READ_PLAN_FOR_UPDATE_QUERY,
            [input.operationId],
            code,
          ),
        );
        const observedRows = observedRowsResult.rows;
        ensure(observedRows.length <= 1, code);
        if (observedRows.length === 0) throw VISIBILITY_RETRY;
        return compareObserved(
          normalizeRegistryRow(observedRows[0], code),
          input,
        );
      })(),
    ),
  );
}

async function readback(store, input) {
  const observed = await protectPromise(
    readRegistryTransaction(store, input.operationId, false),
  );
  return compareObserved(observed, input);
}

async function provisionWithRecovery(store, input) {
  try {
    return await protectPromise(provisionTransaction(store, input));
  } catch (error) {
    if (isInternalError(error)) throw error;
    if (error === VISIBILITY_RETRY) {
      try {
        return await protectPromise(provisionTransaction(store, input));
      } catch (retryError) {
        if (isInternalError(retryError)) throw retryError;
        if (retryError !== VISIBILITY_RETRY) error = retryError;
      }
    }
    const commitState = storeCommitState(error);
    let observed;
    try {
      observed = await protectPromise(readback(store, input));
    } catch (readError) {
      if (isInternalError(readError)) throw readError;
      fail("postgres_detached_restore_stable_plan_registry_outcome_uncertain");
    }
    if (observed !== null) return observed;
    if (commitState === "not-committed") {
      try {
        return await protectPromise(provisionTransaction(store, input));
      } catch (retryError) {
        if (isInternalError(retryError)) throw retryError;
      }
      try {
        observed = await protectPromise(readback(store, input));
      } catch (readError) {
        if (isInternalError(readError)) throw readError;
        fail("postgres_detached_restore_stable_plan_registry_outcome_uncertain");
      }
      if (observed !== null) return observed;
    }
    fail("postgres_detached_restore_stable_plan_registry_outcome_uncertain");
  }
}

export class PostgresDetachedRestoreStablePlanRegistryError extends ErrorConstructor {
  constructor(code) {
    if (typeof code !== "string" || !objectHasOwn(ERROR_MESSAGES, code)) {
      throw new TypeErrorConstructor(
        "Unsupported PostgreSQL detached restore stable-plan registry error",
      );
    }
    const message = ERROR_MESSAGES[code];
    super(message);
    objectDefineProperty(this, "name", {
      enumerable: false,
      value: "PostgresDetachedRestoreStablePlanRegistryError",
    });
    objectDefineProperty(this, "code", { enumerable: true, value: code });
    objectDefineProperty(this, "retryable", {
      enumerable: true,
      value: false,
    });
    objectDefineProperty(this, "stack", {
      enumerable: false,
      value: `PostgresDetachedRestoreStablePlanRegistryError: ${message}`,
    });
    objectFreeze(this);
  }
}

export function createPostgresDetachedRestoreStablePlanRegistry(...args) {
  const optionCode =
    "invalid_postgres_detached_restore_stable_plan_registry_options";
  ensure(args.length === 1, optionCode);
  const options = exactDataObject(args[0], FACTORY_OPTION_KEYS, optionCode);
  const store = storeFromValue(options.store, optionCode);
  const provisioningFleetCapabilityGate = trustedFunction(
    options.provisioningFleetCapabilityGate,
    optionCode,
  );
  const requestCode =
    "invalid_postgres_detached_restore_stable_plan_registry_request";

  const provisionStablePlanInternal = async (...methodArgs) => {
    ensure(methodArgs.length === 1, requestCode);
    const input = normalizeProvisionRequest(methodArgs[0], requestCode);
    await protectPromise(
      invokeProvisioningGate(provisioningFleetCapabilityGate, input),
    );
    return await protectPromise(provisionWithRecovery(store, input));
  };
  const provisionStablePlan = function provisionStablePlan(...methodArgs) {
    return protectPromise(provisionStablePlanInternal(...methodArgs));
  };

  const resolveStablePlanInternal = async (...methodArgs) => {
    ensure(methodArgs.length === 1, requestCode);
    const input = normalizeResolveRequest(methodArgs[0], requestCode);
    let observed;
    try {
      observed = await protectPromise(
        readRegistryTransaction(store, input.operationId, false),
      );
    } catch (error) {
      if (isInternalError(error)) throw error;
      fail("postgres_detached_restore_stable_plan_registry_outcome_uncertain");
    }
    if (observed === null) {
      fail("postgres_detached_restore_stable_plan_registry_not_found");
    }
    return compareObserved(observed, input);
  };
  const resolveStablePlan = function resolveStablePlan(...methodArgs) {
    return protectPromise(resolveStablePlanInternal(...methodArgs));
  };

  objectFreeze(provisionStablePlan);
  objectFreeze(resolveStablePlan);
  const registry = exactFrozenRecord({ provisionStablePlan, resolveStablePlan });
  callIntrinsic(weakSetAddIntrinsic, registryBrands, [registry]);
  return registry;
}

export function isPostgresDetachedRestoreStablePlanRegistry(value) {
  if (arguments.length !== 1) return false;
  try {
    return (
      value !== null &&
      typeof value === "object" &&
      !isProxyValue(value) &&
      callIntrinsic(weakSetHasIntrinsic, registryBrands, [value])
    );
  } catch {
    return false;
  }
}

objectFreeze(PostgresDetachedRestoreStablePlanRegistryError.prototype);
objectFreeze(PostgresDetachedRestoreStablePlanRegistryError);
objectFreeze(createPostgresDetachedRestoreStablePlanRegistry);
objectFreeze(isPostgresDetachedRestoreStablePlanRegistry);
