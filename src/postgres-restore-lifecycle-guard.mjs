import { AsyncLocalStorage } from "node:async_hooks";
import { types as utilTypes } from "node:util";

import {
  POSTGRES_RESTORE_LIFECYCLE_LOCK_ID,
  PostgresOperationGuard,
  PostgresOperationGuardError,
  haveDistinctPostgresOperationGuardPools,
  isPostgresOperationGuard,
} from "./postgres-operation-guard.mjs";

export { POSTGRES_RESTORE_LIFECYCLE_LOCK_ID };

const FOREGROUND_MODE = "foreground";
const RECOVERY_MODE = "recovery";
const MAX_PROTOTYPE_DEPTH = 64;

const ERROR_MESSAGES = Object.freeze({
  invalid_postgres_restore_lifecycle_guard_options:
    "PostgreSQL restore lifecycle guard options are invalid",
  invalid_postgres_restore_lifecycle_guard_request:
    "PostgreSQL restore lifecycle guard request is invalid",
  postgres_restore_lifecycle_guard_busy:
    "PostgreSQL restore lifecycle guard is already held",
  postgres_restore_lifecycle_guard_outcome_uncertain:
    "PostgreSQL restore lifecycle guard outcome is uncertain",
});

const arrayIncludesIntrinsic = Array.prototype.includes;
const arrayIsArray = Array.isArray;
const asyncLocalStorageGetStoreIntrinsic =
  AsyncLocalStorage.prototype.getStore;
const asyncLocalStorageRunIntrinsic = AsyncLocalStorage.prototype.run;
const ErrorConstructor = Error;
const functionApplyIntrinsic = Reflect.apply;
const functionHasInstanceIntrinsic = Function.prototype[Symbol.hasInstance];
const isGeneratorFunctionValue = utilTypes.isGeneratorFunction;
const isGeneratorObjectValue = utilTypes.isGeneratorObject;
const isPromiseValue = utilTypes.isPromise;
const isProxyValue = utilTypes.isProxy;
const objectCreate = Object.create;
const objectDefineProperties = Object.defineProperties;
const objectDefineProperty = Object.defineProperty;
const objectFreeze = Object.freeze;
const objectGetOwnPropertyDescriptor = Object.getOwnPropertyDescriptor;
const objectGetPrototypeOf = Object.getPrototypeOf;
const objectHasOwn = Object.hasOwn;
const objectIsExtensible = Object.isExtensible;
const objectIsFrozen = Object.isFrozen;
const objectPrototype = Object.prototype;
const PromiseConstructor = Promise;
const promisePrototype = Promise.prototype;
const promiseResolveIntrinsic = Promise.resolve;
const promiseSpeciesSymbol = Symbol.species;
const promiseThenIntrinsic = Promise.prototype.then;
const reflectOwnKeys = Reflect.ownKeys;
const runRestoreLifecycleExclusiveIntrinsic =
  PostgresOperationGuard.prototype.runRestoreLifecycleExclusive;
const runRestoreLifecycleSharedIntrinsic =
  PostgresOperationGuard.prototype.runRestoreLifecycleShared;
const haveDistinctOperationGuardPoolsIntrinsic =
  haveDistinctPostgresOperationGuardPools;
const TypeErrorConstructor = TypeError;
const WeakMapConstructor = WeakMap;
const weakMapDeleteIntrinsic = WeakMap.prototype.delete;
const weakMapGetIntrinsic = WeakMap.prototype.get;
const weakMapHasIntrinsic = WeakMap.prototype.has;
const weakMapSetIntrinsic = WeakMap.prototype.set;
const WeakSetConstructor = WeakSet;
const weakSetAddIntrinsic = WeakSet.prototype.add;
const weakSetDeleteIntrinsic = WeakSet.prototype.delete;
const weakSetHasIntrinsic = WeakSet.prototype.has;

const OPTION_KEYS = objectFreeze([
  "foregroundOperationGuard",
  "recoveryOperationGuard",
]);
const PROBE_KEYS = objectFreeze(["assertHeld"]);
const lifecycleContext = new AsyncLocalStorage();
const failedLifecycleContexts = new WeakSetConstructor();
const activeLifecycleContexts = new WeakSetConstructor();
const lifecycleErrors = new WeakSetConstructor();
const lifecycleFacades = new WeakSetConstructor();
const lifecycleFacadeOperationGuardReceivers = new WeakMapConstructor();
const lifecycleLeases = new WeakMapConstructor();
const callbackFailures = new WeakMapConstructor();
const promiseSettlementBrand = objectFreeze(objectCreate(null));
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

function callIntrinsic(intrinsic, receiver, args) {
  return functionApplyIntrinsic(intrinsic, receiver, args);
}

function arrayIncludes(value, candidate) {
  return callIntrinsic(arrayIncludesIntrinsic, value, [candidate]);
}

function weakMapDelete(map, key) {
  return callIntrinsic(weakMapDeleteIntrinsic, map, [key]);
}

function weakMapGet(map, key) {
  return callIntrinsic(weakMapGetIntrinsic, map, [key]);
}

function weakMapHas(map, key) {
  return callIntrinsic(weakMapHasIntrinsic, map, [key]);
}

function weakMapSet(map, key, value) {
  callIntrinsic(weakMapSetIntrinsic, map, [key, value]);
}

function weakSetAdd(set, value) {
  callIntrinsic(weakSetAddIntrinsic, set, [value]);
}

function weakSetDelete(set, value) {
  return callIntrinsic(weakSetDeleteIntrinsic, set, [value]);
}

function weakSetHas(set, value) {
  return callIntrinsic(weakSetHasIntrinsic, set, [value]);
}

function makeError(code) {
  return new PostgresRestoreLifecycleGuardError(code);
}

function fail(code) {
  throw makeError(code);
}

function ensure(condition, code) {
  if (!condition) fail(code);
}

function exactFrozenRecord(value) {
  const result = objectCreate(null);
  const keys = reflectOwnKeys(value);
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
    const descriptor = objectGetOwnPropertyDescriptor(value, key);
    objectDefineProperty(result, key, {
      configurable: false,
      enumerable: true,
      value: descriptor.value,
      writable: false,
    });
  }
  return objectFreeze(result);
}

function exactDataObject(value, expectedKeys, code) {
  if (
    value === null ||
    typeof value !== "object" ||
    arrayIsArray(value) ||
    isProxyValue(value)
  ) {
    fail(code);
  }
  let prototype;
  let keys;
  try {
    prototype = objectGetPrototypeOf(value);
    keys = reflectOwnKeys(value);
  } catch {
    fail(code);
  }
  ensure(
    arrayIncludes([objectPrototype, null], prototype) &&
      keys.length === expectedKeys.length,
    code,
  );
  const normalized = objectCreate(null);
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
    ensure(
      typeof key === "string" && arrayIncludes(expectedKeys, key),
      code,
    );
    const descriptor = objectGetOwnPropertyDescriptor(value, key);
    ensure(
      descriptor?.enumerable === true &&
        objectHasOwn(descriptor, "value"),
      code,
    );
    objectDefineProperty(normalized, key, {
      enumerable: true,
      value: descriptor.value,
    });
  }
  for (let index = 0; index < expectedKeys.length; index += 1) {
    ensure(objectHasOwn(normalized, expectedKeys[index]), code);
  }
  return normalized;
}

function trustedCallback(value, code) {
  ensure(
    typeof value === "function" &&
      !isProxyValue(value) &&
      !isGeneratorFunctionValue(value),
    code,
  );
  return value;
}

function foregroundOperationGuardBinding(value, code) {
  ensure(
    isPostgresOperationGuard(value) &&
      objectIsFrozen(value) &&
      objectIsFrozen(runRestoreLifecycleSharedIntrinsic),
    code,
  );
  return exactFrozenRecord({
    method: runRestoreLifecycleSharedIntrinsic,
    receiver: value,
  });
}

function recoveryOperationGuardBinding(value, code) {
  ensure(
    isPostgresOperationGuard(value) &&
      objectIsFrozen(value) &&
      objectIsFrozen(runRestoreLifecycleExclusiveIntrinsic),
    code,
  );
  return exactFrozenRecord({
    method: runRestoreLifecycleExclusiveIntrinsic,
    receiver: value,
  });
}

function operationProbeBinding(value, code) {
  const probe = exactDataObject(value, PROBE_KEYS, code);
  ensure(objectIsFrozen(value), code);
  return trustedCallback(probe.assertHeld, code);
}

function hasThenProperty(value, code) {
  if (
    value === null ||
    (typeof value !== "object" && typeof value !== "function")
  ) {
    return false;
  }
  let current = value;
  let depth = 0;
  while (current !== null) {
    ensure(depth <= MAX_PROTOTYPE_DEPTH && !isProxyValue(current), code);
    let descriptor;
    try {
      descriptor = objectGetOwnPropertyDescriptor(current, "then");
      current = objectGetPrototypeOf(current);
    } catch {
      fail(code);
    }
    if (descriptor !== undefined) return true;
    depth += 1;
  }
  return false;
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
  try {
    prototype = objectGetPrototypeOf(value);
    keys = reflectOwnKeys(value);
  } catch {
    return false;
  }
  if (
    prototype !== null ||
    keys.length !== 1 ||
    keys[0] !== promiseSpeciesSymbol
  ) {
    return false;
  }
  const descriptor = objectGetOwnPropertyDescriptor(
    value,
    promiseSpeciesSymbol,
  );
  return (
    descriptor?.configurable === false &&
    descriptor.enumerable === false &&
    objectHasOwn(descriptor, "value") &&
    descriptor.value === PromiseConstructor &&
    descriptor.writable === false
  );
}

function promiseSettlementCarrier(status, value) {
  return exactFrozenRecord({
    brand: promiseSettlementBrand,
    status,
    value,
  });
}

function promiseFulfillmentCarrier(value) {
  return promiseSettlementCarrier("fulfilled", value);
}

function promiseRejectionCarrier(value) {
  return promiseSettlementCarrier("rejected", value);
}

objectFreeze(promiseFulfillmentCarrier);
objectFreeze(promiseRejectionCarrier);

function unwrapPromiseSettlementCarrier(value, code) {
  ensure(
    value !== null &&
      typeof value === "object" &&
      !isProxyValue(value) &&
      objectGetPrototypeOf(value) === null &&
      objectIsFrozen(value),
    code,
  );
  const keys = reflectOwnKeys(value);
  ensure(
    keys.length === 3 &&
      keys[0] === "brand" &&
      keys[1] === "status" &&
      keys[2] === "value",
    code,
  );
  const brand = objectGetOwnPropertyDescriptor(value, "brand");
  const status = objectGetOwnPropertyDescriptor(value, "status");
  const payload = objectGetOwnPropertyDescriptor(value, "value");
  ensure(
    brand?.value === promiseSettlementBrand &&
      status?.enumerable === true &&
      objectHasOwn(status, "value") &&
      (status.value === "fulfilled" || status.value === "rejected") &&
      payload?.enumerable === true &&
      objectHasOwn(payload, "value"),
    code,
  );
  return exactFrozenRecord({
    status: status.value,
    value: payload.value,
  });
}

function prepareNativePromise(value, code) {
  ensure(
    isPromiseValue(value) &&
      !isProxyValue(value) &&
      !isGeneratorObjectValue(value),
    code,
  );
  let prototype;
  let ownConstructor;
  let ownThen;
  try {
    prototype = objectGetPrototypeOf(value);
    ownConstructor = objectGetOwnPropertyDescriptor(value, "constructor");
    ownThen = objectGetOwnPropertyDescriptor(value, "then");
  } catch {
    fail(code);
  }
  ensure(prototype === promisePrototype, code);
  const hasSafeOwnConstructor =
    ownConstructor !== undefined &&
    objectHasOwn(ownConstructor, "value") &&
    safePromiseSpeciesHolder(ownConstructor.value);
  if (!hasSafeOwnConstructor) {
    ensure(
      ownThen === undefined ||
        (objectHasOwn(ownThen, "value") &&
          ownThen.value === protectedPromiseThen),
      code,
    );
  }

  if (
    ownConstructor === undefined ||
    ownConstructor.configurable === true
  ) {
    try {
      ensure(objectIsExtensible(value), code);
      objectDefineProperty(value, "constructor", {
        configurable: false,
        enumerable: false,
        value: promiseSpeciesHolder,
        writable: false,
      });
      ownConstructor = objectGetOwnPropertyDescriptor(
        value,
        "constructor",
      );
    } catch {
      fail(code);
    }
  }

  if (
    ownConstructor !== undefined &&
    objectHasOwn(ownConstructor, "value") &&
    safePromiseSpeciesHolder(ownConstructor.value)
  ) {
    let normalized;
    try {
      normalized = callIntrinsic(promiseThenIntrinsic, value, [
        promiseFulfillmentCarrier,
        promiseRejectionCarrier,
      ]);
      objectDefineProperty(normalized, "constructor", {
        configurable: false,
        enumerable: false,
        value: PromiseConstructor,
        writable: false,
      });
    } catch {
      fail(code);
    }
    return exactFrozenRecord({ promise: normalized, wrapped: true });
  }

  ensure(
    ownConstructor !== undefined &&
      objectHasOwn(ownConstructor, "value") &&
      ownConstructor.value === PromiseConstructor,
    code,
  );
  return exactFrozenRecord({ promise: value, wrapped: false });
}

async function settleNativePromiseInternal(value, code) {
  const normalized = prepareNativePromise(value, code);
  let settled;
  try {
    settled = await normalized.promise;
  } catch (error) {
    return exactFrozenRecord({ status: "rejected", value: error });
  }
  if (!normalized.wrapped) {
    return exactFrozenRecord({ status: "fulfilled", value: settled });
  }
  return unwrapPromiseSettlementCarrier(settled, code);
}

function settleNativePromise(value, code) {
  return protectPromise(settleNativePromiseInternal(value, code));
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
  return callIntrinsic(protectedPromiseThen, this, [
    undefined,
    onRejected,
  ]);
}

function resolveProtectedPromise(value) {
  return protectPromise(
    callIntrinsic(promiseResolveIntrinsic, PromiseConstructor, [value]),
  );
}

function protectedPromiseFinally(onFinally) {
  if (typeof onFinally !== "function") {
    return callIntrinsic(protectedPromiseThen, this, [
      onFinally,
      onFinally,
    ]);
  }
  const runFinally = () =>
    resolveProtectedPromise(callIntrinsic(onFinally, undefined, []));
  return callIntrinsic(protectedPromiseThen, this, [
    (value) =>
      callIntrinsic(protectedPromiseThen, runFinally(), [
        () => value,
        undefined,
      ]),
    (reason) =>
      callIntrinsic(protectedPromiseThen, runFinally(), [
        () => {
          throw reason;
        },
        undefined,
      ]),
  ]);
}

function protectPromise(value) {
  if (!isPromiseValue(value)) return value;
  const constructorDescriptor = objectGetOwnPropertyDescriptor(
    value,
    "constructor",
  );
  if (
    constructorDescriptor !== undefined &&
    objectHasOwn(constructorDescriptor, "value") &&
    constructorDescriptor.value !== promiseSpeciesHolder &&
    safePromiseSpeciesHolder(constructorDescriptor.value)
  ) {
    return protectPromise(
      callIntrinsic(promiseThenIntrinsic, value, [undefined, undefined]),
    );
  }
  objectDefineProperties(value, {
    catch: {
      configurable: false,
      enumerable: false,
      value: protectedPromiseCatch,
      writable: false,
    },
    constructor: {
      configurable: false,
      enumerable: false,
      value: promiseSpeciesHolder,
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
  });
  return value;
}

function callbackFailure(error) {
  const carrier = objectFreeze(objectCreate(null));
  weakMapSet(callbackFailures, carrier, error);
  return carrier;
}

function currentLifecycleContext() {
  return callIntrinsic(
    asyncLocalStorageGetStoreIntrinsic,
    lifecycleContext,
    [],
  );
}

function markCurrentLifecycleContextFailed() {
  const context = currentLifecycleContext();
  if (
    context !== null &&
    (typeof context === "object" || typeof context === "function") &&
    !isProxyValue(context) &&
    weakSetHas(activeLifecycleContexts, context)
  ) {
    weakSetAdd(failedLifecycleContexts, context);
  }
}

function leaseRecord(value, expectedMode) {
  if (
    value === null ||
    (typeof value !== "object" && typeof value !== "function") ||
    isProxyValue(value) ||
    (expectedMode !== FOREGROUND_MODE && expectedMode !== RECOVERY_MODE) ||
    !weakMapHas(lifecycleLeases, value)
  ) {
    return null;
  }
  const record = weakMapGet(lifecycleLeases, value);
  if (
    record.mode !== expectedMode ||
    currentLifecycleContext() !== record.context
  ) {
    return null;
  }
  return record;
}

function operationErrorCode(value) {
  if (
    value === null ||
    (typeof value !== "object" && typeof value !== "function") ||
    isProxyValue(value)
  ) {
    return null;
  }
  let authentic = false;
  try {
    authentic = callIntrinsic(
      functionHasInstanceIntrinsic,
      PostgresOperationGuardError,
      [value],
    );
  } catch {
    return null;
  }
  if (!authentic) return null;
  const descriptor = objectGetOwnPropertyDescriptor(value, "code");
  if (descriptor === undefined || !objectHasOwn(descriptor, "value")) {
    return null;
  }
  return descriptor.value;
}

function throwMappedOperationError(error) {
  if (operationErrorCode(error) === "postgres_operation_guard_busy") {
    fail("postgres_restore_lifecycle_guard_busy");
  }
  fail("postgres_restore_lifecycle_guard_outcome_uncertain");
}

function createLease(mode, assertHeld, context) {
  const lease = exactFrozenRecord({ mode });
  weakMapSet(
    lifecycleLeases,
    lease,
    exactFrozenRecord({ assertHeld, context, mode }),
  );
  return lease;
}

function closeCallbackContext(lease, context) {
  const failed = weakSetHas(failedLifecycleContexts, context);
  weakMapDelete(lifecycleLeases, lease);
  weakSetDelete(failedLifecycleContexts, context);
  weakSetDelete(activeLifecycleContexts, context);
  if (failed) {
    fail("postgres_restore_lifecycle_guard_outcome_uncertain");
  }
}

async function settleCallbackResultInternal(result, lease, context) {
  try {
    const settlement = await settleNativePromise(
      result,
      "postgres_restore_lifecycle_guard_outcome_uncertain",
    );
    if (settlement.status === "rejected") {
      throw callbackFailure(settlement.value);
    }
    ensure(
      !isProxyValue(settlement.value) &&
        !isGeneratorFunctionValue(settlement.value) &&
        !isGeneratorObjectValue(settlement.value) &&
        !hasThenProperty(
          settlement.value,
          "postgres_restore_lifecycle_guard_outcome_uncertain",
        ),
      "postgres_restore_lifecycle_guard_outcome_uncertain",
    );
    return settlement.value;
  } finally {
    closeCallbackContext(lease, context);
  }
}

function settleCallbackResult(result, lease, context) {
  return protectPromise(
    settleCallbackResultInternal(result, lease, context),
  );
}

function invokeCallbackInternal(
  mode,
  callback,
  assertHeld,
  complete,
  context,
) {
  const lease = createLease(mode, assertHeld, context);
  let result;
  try {
    result = callIntrinsic(callback, undefined, [lease, complete]);
  } catch (error) {
    closeCallbackContext(lease, context);
    throw callbackFailure(error);
  }
  try {
    ensure(
      !isProxyValue(result) &&
        !isGeneratorFunctionValue(result) &&
        !isGeneratorObjectValue(result),
      "postgres_restore_lifecycle_guard_outcome_uncertain",
    );
  } catch (error) {
    closeCallbackContext(lease, context);
    throw error;
  }
  if (isPromiseValue(result)) {
    return settleCallbackResult(result, lease, context);
  }
  try {
    ensure(
      !hasThenProperty(
        result,
        "postgres_restore_lifecycle_guard_outcome_uncertain",
      ),
      "postgres_restore_lifecycle_guard_outcome_uncertain",
    );
    return result;
  } finally {
    closeCallbackContext(lease, context);
  }
}

function invokeCallback(mode, callback, assertHeld, complete) {
  const context = objectFreeze(objectCreate(null));
  weakSetAdd(activeLifecycleContexts, context);
  const runInContext = () =>
    protectPromise(
      invokeCallbackInternal(
        mode,
        callback,
        assertHeld,
        complete,
        context,
      ),
    );
  objectFreeze(runInContext);
  return callIntrinsic(asyncLocalStorageRunIntrinsic, lifecycleContext, [
    context,
    runInContext,
  ]);
}

function guardCallback(mode, callback, probeValue, completeValue) {
  const code = "postgres_restore_lifecycle_guard_outcome_uncertain";
  let assertHeld;
  let complete;
  try {
    assertHeld = operationProbeBinding(probeValue, code);
    complete = trustedCallback(completeValue, code);
    ensure(objectIsFrozen(completeValue), code);
  } catch {
    fail(code);
  }
  return protectPromise(
    invokeCallback(mode, callback, assertHeld, complete),
  );
}

async function invokeLifecycleInternal(binding, mode, callback) {
  const callbackAdapter = (probe, complete) =>
    guardCallback(mode, callback, probe, complete);
  objectFreeze(callbackAdapter);
  let pending;
  try {
    pending = callIntrinsic(binding.method, binding.receiver, [
      callbackAdapter,
    ]);
  } catch {
    fail("postgres_restore_lifecycle_guard_outcome_uncertain");
  }
  let settlement;
  try {
    settlement = await settleNativePromise(
      pending,
      "postgres_restore_lifecycle_guard_outcome_uncertain",
    );
  } catch (error) {
    if (weakSetHas(lifecycleErrors, error)) throw error;
    fail("postgres_restore_lifecycle_guard_outcome_uncertain");
  }
  if (settlement.status === "fulfilled") return settlement.value;
  const error = settlement.value;
  if (
    error !== null &&
    (typeof error === "object" || typeof error === "function") &&
    !isProxyValue(error) &&
    weakMapHas(callbackFailures, error)
  ) {
    const callbackError = weakMapGet(callbackFailures, error);
    weakMapDelete(callbackFailures, error);
    if (
      operationErrorCode(callbackError) ===
      "postgres_operation_guard_outcome_uncertain"
    ) {
      fail("postgres_restore_lifecycle_guard_outcome_uncertain");
    }
    throw callbackError;
  }
  if (
    error !== null &&
    (typeof error === "object" || typeof error === "function") &&
    !isProxyValue(error) &&
    weakSetHas(lifecycleErrors, error)
  ) {
    throw error;
  }
  throwMappedOperationError(error);
}

async function invokeLifecycleRequest(binding, mode, args) {
  const code = "invalid_postgres_restore_lifecycle_guard_request";
  ensure(args.length === 1, code);
  const callback = trustedCallback(args[0], code);
  return await protectPromise(
    invokeLifecycleInternal(binding, mode, callback),
  );
}

function invokeLifecycle(binding, mode, args) {
  return protectPromise(invokeLifecycleRequest(binding, mode, args));
}

async function assertLeaseHeldInternal(args) {
  const code = "postgres_restore_lifecycle_guard_outcome_uncertain";
  if (args.length !== 2) {
    markCurrentLifecycleContextFailed();
    fail(code);
  }
  const record = leaseRecord(args[0], args[1]);
  if (record === null) {
    markCurrentLifecycleContextFailed();
    fail(code);
  }
  let pending;
  try {
    pending = callIntrinsic(record.assertHeld, undefined, []);
  } catch (error) {
    markCurrentLifecycleContextFailed();
    throwMappedOperationError(error);
  }
  let settlement;
  try {
    settlement = await settleNativePromise(pending, code);
  } catch {
    markCurrentLifecycleContextFailed();
    fail(code);
  }
  if (settlement.status === "rejected") {
    markCurrentLifecycleContextFailed();
    throwMappedOperationError(settlement.value);
  }
  if (settlement.value !== undefined) {
    markCurrentLifecycleContextFailed();
    fail(code);
  }
}

export class PostgresRestoreLifecycleGuardError extends ErrorConstructor {
  constructor(code) {
    if (typeof code !== "string" || !objectHasOwn(ERROR_MESSAGES, code)) {
      throw new TypeErrorConstructor(
        "Unsupported PostgreSQL restore lifecycle guard error",
      );
    }
    const message = ERROR_MESSAGES[code];
    super(message);
    objectDefineProperty(this, "name", {
      configurable: false,
      enumerable: false,
      value: "PostgresRestoreLifecycleGuardError",
      writable: false,
    });
    objectDefineProperty(this, "code", {
      configurable: false,
      enumerable: true,
      value: code,
      writable: false,
    });
    objectDefineProperty(this, "retryable", {
      configurable: false,
      enumerable: true,
      value: false,
      writable: false,
    });
    objectDefineProperty(this, "stack", {
      configurable: false,
      enumerable: false,
      value: `PostgresRestoreLifecycleGuardError: ${message}`,
      writable: false,
    });
    weakSetAdd(lifecycleErrors, this);
    objectFreeze(this);
  }
}

export function createPostgresRestoreLifecycleGuard(...args) {
  const code = "invalid_postgres_restore_lifecycle_guard_options";
  ensure(args.length === 1, code);
  const options = exactDataObject(args[0], OPTION_KEYS, code);
  const foregroundBinding = foregroundOperationGuardBinding(
    options.foregroundOperationGuard,
    code,
  );
  const recoveryBinding = recoveryOperationGuardBinding(
    options.recoveryOperationGuard,
    code,
  );
  ensure(
    objectIsFrozen(haveDistinctOperationGuardPoolsIntrinsic) &&
      callIntrinsic(haveDistinctOperationGuardPoolsIntrinsic, undefined, [
        foregroundBinding.receiver,
        recoveryBinding.receiver,
      ]),
    code,
  );

  const runForeground = function runForeground(...methodArgs) {
    return protectPromise(
      invokeLifecycle(foregroundBinding, FOREGROUND_MODE, methodArgs),
    );
  };
  const runRecovery = function runRecovery(...methodArgs) {
    return protectPromise(
      invokeLifecycle(recoveryBinding, RECOVERY_MODE, methodArgs),
    );
  };
  objectFreeze(runForeground);
  objectFreeze(runRecovery);
  const facade = exactFrozenRecord({ runForeground, runRecovery });
  weakMapSet(
    lifecycleFacadeOperationGuardReceivers,
    facade,
    exactFrozenRecord({
      foreground: foregroundBinding.receiver,
      recovery: recoveryBinding.receiver,
    }),
  );
  weakSetAdd(lifecycleFacades, facade);
  return facade;
}

export function isPostgresRestoreLifecycleGuard(value) {
  return (
    value !== null &&
    (typeof value === "object" || typeof value === "function") &&
    !isProxyValue(value) &&
    weakSetHas(lifecycleFacades, value)
  );
}

export function haveDistinctPostgresRestoreLifecycleOperationGuardPools(
  lifecycleGuard,
  operationGuard,
) {
  if (
    arguments.length !== 2 ||
    !isPostgresRestoreLifecycleGuard(lifecycleGuard)
  ) {
    return false;
  }
  const receivers = weakMapGet(
    lifecycleFacadeOperationGuardReceivers,
    lifecycleGuard,
  );
  return (
    receivers !== undefined &&
    objectIsFrozen(haveDistinctOperationGuardPoolsIntrinsic) &&
    callIntrinsic(haveDistinctOperationGuardPoolsIntrinsic, undefined, [
      receivers.foreground,
      operationGuard,
    ]) &&
    callIntrinsic(haveDistinctOperationGuardPoolsIntrinsic, undefined, [
      receivers.recovery,
      operationGuard,
    ])
  );
}

export function isPostgresRestoreLifecycleLease(value, expectedMode) {
  return leaseRecord(value, expectedMode) !== null;
}

export function assertPostgresRestoreLifecycleLeaseHeld(...args) {
  return protectPromise(assertLeaseHeldInternal(args));
}

objectFreeze(PostgresRestoreLifecycleGuardError.prototype);
objectFreeze(PostgresRestoreLifecycleGuardError);
objectFreeze(
  haveDistinctPostgresRestoreLifecycleOperationGuardPools,
);
