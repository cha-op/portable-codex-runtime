import { types as utilTypes } from "node:util";

import {
  WRITER_FORCE_FENCE_OPERATION_KIND,
  assertSessionAuthoritySnapshot,
  assertSessionOperationBinding,
  assertSessionOperationTransitionProof,
  assertWriterForceFenceAtomicCaptureHandoffProof,
} from "./postgres-session-authority.mjs";
import {
  PostgresOperationGuard,
  isPostgresOperationGuard,
} from "./postgres-operation-guard.mjs";
import {
  STORAGE_FORCE_FENCE_RECONCILIATION_CONTRACT_VERSION,
  assertStorageBackend,
  assertStorageBackendCapabilities,
  assertStorageForceFenceReconciliationBackend,
  assertStorageForceFenceReconciliationResult,
  assertStorageForceFenceRequest,
  assertStorageForceFenceResult,
} from "./session-storage-contracts.mjs";

const arrayIncludesIntrinsic = Array.prototype.includes;
const arrayIsArray = Array.isArray;
const arrayPrototype = Array.prototype;
const ArrayConstructor = Array;
const BigIntConstructor = BigInt;
const bigIntToStringIntrinsic = BigInt.prototype.toString;
const isGeneratorFunctionValue = utilTypes.isGeneratorFunction;
const isGeneratorObjectValue = utilTypes.isGeneratorObject;
const isPromiseValue = utilTypes.isPromise;
const isProxyValue = utilTypes.isProxy;
const numberIsSafeInteger = Number.isSafeInteger;
const objectCreate = Object.create;
const objectDefineProperties = Object.defineProperties;
const objectDefineProperty = Object.defineProperty;
const objectFreeze = Object.freeze;
const objectGetOwnPropertyDescriptor = Object.getOwnPropertyDescriptor;
const objectGetPrototypeOf = Object.getPrototypeOf;
const objectHasOwn = Object.hasOwn;
const objectIs = Object.is;
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
const runExclusiveIntrinsic = PostgresOperationGuard.prototype.runExclusive;
const TypeErrorConstructor = TypeError;
const WeakMapConstructor = WeakMap;
const weakMapGetIntrinsic = WeakMap.prototype.get;
const weakMapHasIntrinsic = WeakMap.prototype.has;
const weakMapSetIntrinsic = WeakMap.prototype.set;
const WeakSetConstructor = WeakSet;
const weakSetAddIntrinsic = WeakSet.prototype.add;
const weakSetDeleteIntrinsic = WeakSet.prototype.delete;
const weakSetHasIntrinsic = WeakSet.prototype.has;

const OPAQUE_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u;
const MAX_CLONE_DEPTH = 64;
const MAX_CLONE_ENTRIES = 131_072;
const MAX_CLONE_NODES = 65_536;
const WRITER_FORCE_FENCE_CONTRACT_VERSION_V2 = 2;

const OPTION_KEYS = objectFreeze([
  "authority",
  "operationGuard",
  "storageBackend",
]);
const REQUEST_KEYS = objectFreeze([
  "expectedSession",
  "operationId",
  "request",
]);
const AUTHORITY_METHODS = objectFreeze([
  "claimWriterForceFenceDispatch",
  "finalizeWriterForceFenceAtomicCaptureHandoff",
  "finalizeWriterOperationBlocked",
  "markOperationUncertain",
  "reconcileWriterForceFenceAtomicCaptureHandoff",
  "reserveOperation",
]);
const STORAGE_BACKEND_METHODS = objectFreeze([
  "captureCheckpoint",
  "destroySession",
  "detachAttachment",
  "forceFence",
  "prepareWritableAttachment",
  "provisionSession",
  "restoreCheckpoint",
]);
const STORAGE_BACKEND_METADATA_KEYS = objectFreeze([
  "backendId",
  "capabilities",
  "contractVersion",
]);
const CAPABILITY_KEYS = objectFreeze([
  "atomicPointInTimeCheckpoint",
  "exclusiveWriterAttachment",
  "fencing",
  "normalDirectoryAttachment",
]);
const PROBE_KEYS = objectFreeze(["assertHeld"]);

const ERROR_MESSAGES = objectFreeze({
  invalid_postgres_writer_force_fence_atomic_capture_composition_options:
    "PostgreSQL writer force-fence atomic-capture composition options are invalid",
  invalid_postgres_writer_force_fence_atomic_capture_composition_request:
    "PostgreSQL writer force-fence atomic-capture composition request is invalid",
  postgres_writer_force_fence_atomic_capture_outcome_uncertain:
    "PostgreSQL writer force-fence atomic-capture outcome is uncertain",
});

const INVOCATION_UNCERTAIN = objectFreeze(objectCreate(null));
const facades = new WeakSetConstructor();

function callIntrinsic(intrinsic, receiver, args) {
  return reflectApply(intrinsic, receiver, args);
}

function arrayIncludes(value, candidate) {
  return callIntrinsic(arrayIncludesIntrinsic, value, [candidate]);
}

function regexpTest(pattern, value) {
  return callIntrinsic(regexpExecIntrinsic, pattern, [value]) !== null;
}

function weakSetAdd(set, value) {
  callIntrinsic(weakSetAddIntrinsic, set, [value]);
}

function weakSetDelete(set, value) {
  callIntrinsic(weakSetDeleteIntrinsic, set, [value]);
}

function weakSetHas(set, value) {
  return callIntrinsic(weakSetHasIntrinsic, set, [value]);
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

function protectPromiseReaction(callback) {
  if (typeof callback !== "function") return callback;
  return (value) => protectPromise(callIntrinsic(callback, undefined, [value]));
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
      callIntrinsic(protectedPromiseThen, runFinally(), [() => value, undefined]),
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

export class PostgresWriterForceFenceAtomicCaptureCompositionError extends Error {
  constructor(code) {
    if (typeof code !== "string" || !objectHasOwn(ERROR_MESSAGES, code)) {
      throw new TypeErrorConstructor(
        "Unsupported PostgreSQL writer force-fence atomic-capture composition error",
      );
    }
    const message = ERROR_MESSAGES[code];
    super(message);
    objectDefineProperty(this, "name", {
      enumerable: false,
      value: "PostgresWriterForceFenceAtomicCaptureCompositionError",
    });
    objectDefineProperty(this, "code", { enumerable: true, value: code });
    objectDefineProperty(this, "retryable", {
      enumerable: true,
      value: false,
    });
    objectDefineProperty(this, "stack", {
      enumerable: false,
      value: `PostgresWriterForceFenceAtomicCaptureCompositionError: ${message}`,
    });
    objectFreeze(this);
  }
}

function fail(code) {
  throw new PostgresWriterForceFenceAtomicCaptureCompositionError(code);
}

function ensure(condition, code) {
  if (!condition) fail(code);
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
    arrayIncludes([objectPrototype, null], prototype) &&
      keys.length === expectedKeys.length,
    code,
  );
  const normalized = objectCreate(null);
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
    ensure(typeof key === "string" && arrayIncludes(expectedKeys, key), code);
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

function exactFrozenRecord(value) {
  const record = objectCreate(null);
  const keys = reflectOwnKeys(value);
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
    objectDefineProperty(record, key, {
      enumerable: true,
      value: objectGetOwnPropertyDescriptor(value, key).value,
    });
  }
  return objectFreeze(record);
}

function frozenDataCloneInternal(value, code, active, copies, budget, depth) {
  ensure(depth <= MAX_CLONE_DEPTH, code);
  budget.nodes += 1;
  ensure(budget.nodes <= MAX_CLONE_NODES, code);
  if (value === null || typeof value !== "object") {
    ensure(typeof value !== "function" && typeof value !== "symbol", code);
    return value;
  }
  ensure(!isProxyValue(value), code);
  let frozen;
  try {
    frozen = objectIsFrozen(value);
  } catch {
    fail(code);
  }
  ensure(frozen && !weakSetHas(active, value), code);
  if (weakMapHas(copies, value)) return weakMapGet(copies, value);

  let prototype;
  let keys;
  try {
    prototype = objectGetPrototypeOf(value);
    keys = reflectOwnKeys(value);
  } catch {
    fail(code);
  }
  budget.entries += keys.length;
  ensure(
    keys.length <= MAX_CLONE_ENTRIES && budget.entries <= MAX_CLONE_ENTRIES,
    code,
  );
  const array = arrayIsArray(value);
  ensure(
    array
      ? prototype === arrayPrototype
      : arrayIncludes([objectPrototype, null], prototype),
    code,
  );
  weakSetAdd(active, value);
  let clone;
  try {
    if (array) {
      const lengthDescriptor = objectGetOwnPropertyDescriptor(value, "length");
      ensure(
        lengthDescriptor !== undefined &&
          objectHasOwn(lengthDescriptor, "value") &&
          numberIsSafeInteger(lengthDescriptor.value) &&
          lengthDescriptor.value >= 0 &&
          lengthDescriptor.value <= MAX_CLONE_ENTRIES,
        code,
      );
      clone = new ArrayConstructor(lengthDescriptor.value);
    } else {
      clone = objectCreate(prototype);
    }
    for (let index = 0; index < keys.length; index += 1) {
      const key = keys[index];
      ensure(typeof key === "string", code);
      const descriptor = objectGetOwnPropertyDescriptor(value, key);
      ensure(descriptor !== undefined && objectHasOwn(descriptor, "value"), code);
      if (array && key === "length") {
        ensure(descriptor.enumerable === false, code);
        continue;
      }
      ensure(descriptor.enumerable === true, code);
      objectDefineProperty(clone, key, {
        enumerable: true,
        value: frozenDataCloneInternal(
          descriptor.value,
          code,
          active,
          copies,
          budget,
          depth + 1,
        ),
      });
    }
    objectFreeze(clone);
    weakMapSet(copies, value, clone);
    return clone;
  } finally {
    weakSetDelete(active, value);
  }
}

function frozenDataClone(value, code, budget = { entries: 0, nodes: 0 }) {
  return frozenDataCloneInternal(
    value,
    code,
    new WeakSetConstructor(),
    new WeakMapConstructor(),
    budget,
    0,
  );
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

function optionalOwnDataValue(value, key, code) {
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
  if (descriptor === undefined) {
    return exactFrozenRecord({ found: false, value: undefined });
  }
  ensure(descriptor.enumerable === true && objectHasOwn(descriptor, "value"), code);
  return exactFrozenRecord({ found: true, value: descriptor.value });
}

function findDataValue(receiver, name, code) {
  ensure(
    receiver !== null &&
      (typeof receiver === "object" || typeof receiver === "function") &&
      !isProxyValue(receiver),
    code,
  );
  let current = receiver;
  while (current !== null) {
    ensure(!isProxyValue(current), code);
    let descriptor;
    try {
      descriptor = objectGetOwnPropertyDescriptor(current, name);
    } catch {
      fail(code);
    }
    if (descriptor !== undefined) {
      ensure(objectHasOwn(descriptor, "value"), code);
      return exactFrozenRecord({ found: true, value: descriptor.value });
    }
    try {
      current = objectGetPrototypeOf(current);
    } catch {
      fail(code);
    }
  }
  return exactFrozenRecord({ found: false, value: undefined });
}

function rejectThenableObject(value, code) {
  ensure(!findDataValue(value, "then", code).found, code);
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

function lookupMethod(receiver, name, code) {
  const resolved = findDataValue(receiver, name, code);
  ensure(resolved.found, code);
  return trustedFunction(resolved.value, code);
}

function collaboratorBinding(value, methodNames, code) {
  ensure(
    value !== null &&
      (typeof value === "object" || typeof value === "function") &&
      !isProxyValue(value) &&
      objectIsFrozen(value),
    code,
  );
  rejectThenableObject(value, code);
  const methods = objectCreate(null);
  for (let index = 0; index < methodNames.length; index += 1) {
    const name = methodNames[index];
    objectDefineProperty(methods, name, {
      enumerable: true,
      value: lookupMethod(value, name, code),
    });
  }
  return exactFrozenRecord({
    methods: exactFrozenRecord(methods),
    receiver: value,
  });
}

function operationGuardBinding(value, code) {
  ensure(isPostgresOperationGuard(value) && objectIsFrozen(value), code);
  return exactFrozenRecord({
    methods: exactFrozenRecord({ runExclusive: runExclusiveIntrinsic }),
    receiver: value,
  });
}

function storageBackendBinding(value, code) {
  ensure(
    value !== null &&
      typeof value === "object" &&
      !isProxyValue(value) &&
      !arrayIsArray(value) &&
      objectIsFrozen(value),
    code,
  );
  rejectThenableObject(value, code);
  const projection = objectCreate(null);
  let capabilities;
  for (let index = 0; index < STORAGE_BACKEND_METADATA_KEYS.length; index += 1) {
    const name = STORAGE_BACKEND_METADATA_KEYS[index];
    const resolved = findDataValue(value, name, code);
    ensure(resolved.found, code);
    let projected = resolved.value;
    if (name === "capabilities") {
      try {
        capabilities = assertStorageBackendCapabilities(projected);
      } catch {
        fail(code);
      }
      projected = capabilities;
    }
    objectDefineProperty(projection, name, { enumerable: true, value: projected });
  }
  const methods = objectCreate(null);
  for (let index = 0; index < STORAGE_BACKEND_METHODS.length; index += 1) {
    const name = STORAGE_BACKEND_METHODS[index];
    const method = lookupMethod(value, name, code);
    objectDefineProperty(methods, name, { enumerable: true, value: method });
    objectDefineProperty(projection, name, { enumerable: true, value: method });
  }
  let backend;
  try {
    backend = assertStorageBackend(objectFreeze(projection));
  } catch {
    fail(code);
  }
  const reconciliationVersion = findDataValue(
    value,
    "forceFenceReconciliationContractVersion",
    code,
  );
  const reconciliationMethod = findDataValue(value, "reconcileForceFence", code);
  ensure(reconciliationVersion.found === reconciliationMethod.found, code);
  let reconcileForceFence = null;
  if (reconciliationVersion.found) {
    ensure(
      reconciliationVersion.value ===
        STORAGE_FORCE_FENCE_RECONCILIATION_CONTRACT_VERSION,
      code,
    );
    reconcileForceFence = trustedFunction(reconciliationMethod.value, code);
    try {
      assertStorageForceFenceReconciliationBackend(value);
    } catch {
      fail(code);
    }
  }
  return exactFrozenRecord({
    backendId: backend.backendId,
    capabilities,
    methods: exactFrozenRecord({
      ...methods,
      ...(reconcileForceFence === null ? {} : { reconcileForceFence }),
    }),
    receiver: value,
    supportsReconciliation: reconcileForceFence !== null,
  });
}

function sameCapabilities(left, right, code) {
  const expected = exactDataObject(left, CAPABILITY_KEYS, code);
  const actual = exactDataObject(right, CAPABILITY_KEYS, code);
  for (let index = 0; index < CAPABILITY_KEYS.length; index += 1) {
    const key = CAPABILITY_KEYS[index];
    if (!objectIs(expected[key], actual[key])) return false;
  }
  return true;
}

function normalizeRequest(value, backend) {
  const code =
    "invalid_postgres_writer_force_fence_atomic_capture_composition_request";
  const outer = exactDataObject(value, REQUEST_KEYS, code);
  ensure(
    typeof outer.operationId === "string" &&
      regexpTest(OPAQUE_ID_PATTERN, outer.operationId),
    code,
  );
  let expectedSession;
  let binding;
  try {
    expectedSession = assertSessionAuthoritySnapshot(outer.expectedSession);
    binding = assertSessionOperationBinding({
      expectedSession,
      kind: WRITER_FORCE_FENCE_OPERATION_KIND,
      operationId: outer.operationId,
      request: outer.request,
    });
  } catch {
    fail(code);
  }
  const document = binding.expectedSession.document;
  ensure(
    binding.request.contractVersion === WRITER_FORCE_FENCE_CONTRACT_VERSION_V2 &&
      document.activeOperation === null &&
      document.lifecycle === "ATTACHED" &&
      document.lease !== null &&
      document.attachment !== null &&
      document.storageRef.backendId === backend.backendId &&
      document.backendCapabilities.fencing !== "manual" &&
      sameCapabilities(document.backendCapabilities, backend.capabilities, code),
    code,
  );
  return exactFrozenRecord({
    expectedSession: binding.expectedSession,
    kind: WRITER_FORCE_FENCE_OPERATION_KIND,
    operationId: binding.operationId,
    request: binding.request,
  });
}

function nativePromisePrototypeIsSafe(value) {
  if (
    !isPromiseValue(value) ||
    isProxyValue(value) ||
    isGeneratorObjectValue(value)
  ) {
    return false;
  }
  let prototype;
  try {
    prototype = objectGetPrototypeOf(value);
  } catch {
    return false;
  }
  return prototype === promisePrototype;
}

function nativePromiseConstructorIsSafe(value) {
  let descriptor;
  try {
    descriptor = objectGetOwnPropertyDescriptor(value, "constructor");
    if (descriptor === undefined) {
      descriptor = objectGetOwnPropertyDescriptor(promisePrototype, "constructor");
    }
  } catch {
    return false;
  }
  return (
    descriptor !== undefined &&
    objectHasOwn(descriptor, "value") &&
    descriptor.value === PromiseConstructor
  );
}

function isSafePromiseSpeciesHolder(value) {
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
  if (prototype !== null || keys.length !== 1 || keys[0] !== promiseSpeciesSymbol) {
    return false;
  }
  const descriptor = objectGetOwnPropertyDescriptor(value, promiseSpeciesSymbol);
  return (
    descriptor?.configurable === false &&
    descriptor.enumerable === false &&
    objectHasOwn(descriptor, "value") &&
    descriptor.value === PromiseConstructor &&
    descriptor.writable === false
  );
}

function nativePromiseConstructorIsProtected(value) {
  let descriptor;
  try {
    descriptor = objectGetOwnPropertyDescriptor(value, "constructor");
  } catch {
    return false;
  }
  return (
    descriptor !== undefined &&
    objectHasOwn(descriptor, "value") &&
    isSafePromiseSpeciesHolder(descriptor.value)
  );
}

function normalizeNativePromise(value) {
  if (
    !isPromiseValue(value) ||
    isProxyValue(value) ||
    isGeneratorObjectValue(value)
  ) {
    return null;
  }
  const accepted = nativePromisePrototypeIsSafe(value);
  try {
    return exactFrozenRecord({ accepted, promise: protectPromise(value) });
  } catch {
    if (accepted && nativePromiseConstructorIsSafe(value)) {
      return exactFrozenRecord({ accepted, promise: value });
    }
    if (accepted && nativePromiseConstructorIsProtected(value)) {
      try {
        return exactFrozenRecord({
          accepted,
          promise: protectPromise(
            callIntrinsic(promiseThenIntrinsic, value, [undefined, undefined]),
          ),
        });
      } catch {
        return null;
      }
    }
    return null;
  }
}

async function invokeAsync(binding, name, args) {
  let pending;
  try {
    pending = callIntrinsic(binding.methods[name], binding.receiver, args);
  } catch {
    throw INVOCATION_UNCERTAIN;
  }
  const normalized = normalizeNativePromise(pending);
  if (normalized === null) throw INVOCATION_UNCERTAIN;
  let result;
  try {
    result = await normalized.promise;
  } catch {
    throw INVOCATION_UNCERTAIN;
  }
  if (!normalized.accepted) throw INVOCATION_UNCERTAIN;
  return result;
}

async function invokeProvider(backend, name, input) {
  let value;
  try {
    value = callIntrinsic(backend.methods[name], backend.receiver, [input]);
  } catch {
    throw INVOCATION_UNCERTAIN;
  }
  if (isPromiseValue(value)) {
    const normalized = normalizeNativePromise(value);
    if (normalized === null) throw INVOCATION_UNCERTAIN;
    try {
      value = await normalized.promise;
    } catch {
      throw INVOCATION_UNCERTAIN;
    }
    if (!normalized.accepted) throw INVOCATION_UNCERTAIN;
  } else if (value !== null && typeof value === "object") {
    if (isProxyValue(value) || isGeneratorObjectValue(value)) {
      throw INVOCATION_UNCERTAIN;
    }
    try {
      rejectThenableObject(
        value,
        "postgres_writer_force_fence_atomic_capture_outcome_uncertain",
      );
    } catch {
      throw INVOCATION_UNCERTAIN;
    }
  }
  return value;
}

function normalizeProbe(value, code) {
  const probe = exactDataObject(value, PROBE_KEYS, code);
  ensure(objectIsFrozen(value), code);
  return trustedFunction(probe.assertHeld, code);
}

async function assertGuardHeld(assertHeld) {
  let pending;
  try {
    pending = callIntrinsic(assertHeld, undefined, []);
  } catch {
    throw INVOCATION_UNCERTAIN;
  }
  const normalized = normalizeNativePromise(pending);
  if (normalized === null) throw INVOCATION_UNCERTAIN;
  try {
    await normalized.promise;
  } catch {
    throw INVOCATION_UNCERTAIN;
  }
  if (!normalized.accepted) throw INVOCATION_UNCERTAIN;
}

function settleProtectedPromise(value) {
  return protectPromise(
    callIntrinsic(promiseThenIntrinsic, value, [
      (result) => exactFrozenRecord({ fulfilled: true, value: result }),
      () => exactFrozenRecord({ fulfilled: false, value: undefined }),
    ]),
  );
}

function settleGuardPromise(value, markSettled) {
  return protectPromise(
    callIntrinsic(promiseThenIntrinsic, value, [
      (result) => {
        markSettled();
        return exactFrozenRecord({ fulfilled: true, value: result });
      },
      () => {
        markSettled();
        return exactFrozenRecord({ fulfilled: false, value: undefined });
      },
    ]),
  );
}

async function runGuarded(operationGuard, operationId, callback) {
  const code = "postgres_writer_force_fence_atomic_capture_outcome_uncertain";
  let callbackCalls = 0;
  let callbackCarrier = null;
  let callbackCompletion = null;
  let callbackResult;
  let callbackResultCaptured = false;
  let callbackSettlement = null;
  let callbackViolation = false;
  let guardSettled = false;
  let open = true;
  const runCallback = async (probeValue, completeValue) => {
    await resolveProtectedPromise(undefined);
    await resolveProtectedPromise(undefined);
    ensure(open && !guardSettled && !callbackViolation, code);
    const rawAssertHeld = normalizeProbe(probeValue, code);
    const complete = trustedFunction(completeValue, code);
    ensure(objectIsFrozen(completeValue), code);
    const guardedAssertHeld = objectFreeze(async () => {
      ensure(open && !guardSettled && !callbackViolation, code);
      await protectPromise(assertGuardHeld(rawAssertHeld));
      await resolveProtectedPromise(undefined);
      ensure(open && !guardSettled && !callbackViolation, code);
    });
    callbackResult = await protectPromise(callback(guardedAssertHeld));
    callbackResultCaptured = true;
    ensure(open && !guardSettled && !callbackViolation, code);
    callbackCarrier = callIntrinsic(complete, undefined, [callbackResult]);
    return callbackCarrier;
  };
  const guardedCallback = (probeValue, completeValue) => {
    callbackCalls += 1;
    if (!open || callbackCalls !== 1) {
      callbackViolation = true;
      return callbackCompletion ?? resolveProtectedPromise(undefined);
    }
    callbackCompletion = protectPromise(runCallback(probeValue, completeValue));
    callbackSettlement = settleProtectedPromise(callbackCompletion);
    return callbackCompletion;
  };
  let guardCompletion = null;
  let guardPromiseAccepted = false;
  try {
    const pending = callIntrinsic(
      operationGuard.methods.runExclusive,
      operationGuard.receiver,
      [operationId, guardedCallback],
    );
    const normalized = normalizeNativePromise(pending);
    if (normalized !== null) {
      guardCompletion = normalized.promise;
      guardPromiseAccepted = normalized.accepted;
    }
  } catch {
    guardCompletion = null;
  }
  let guardOutcome = null;
  if (guardCompletion !== null) {
    guardOutcome = await protectPromise(
      settleGuardPromise(guardCompletion, () => {
        guardSettled = true;
      }),
    );
  } else {
    guardSettled = true;
  }
  open = false;
  let callbackOutcome = null;
  if (callbackSettlement !== null) {
    try {
      callbackOutcome = await protectPromise(callbackSettlement);
    } catch {
      callbackOutcome = null;
    }
  }
  ensure(
    guardPromiseAccepted &&
      guardOutcome?.fulfilled === true &&
      callbackCalls === 1 &&
      !callbackViolation &&
      callbackOutcome?.fulfilled === true &&
      callbackResultCaptured &&
      objectIs(callbackOutcome.value, callbackCarrier) &&
      objectIs(guardOutcome.value, callbackResult),
    code,
  );
  return guardOutcome.value;
}

function frozenReceipt(value, code) {
  ensure(
    value !== null &&
      typeof value === "object" &&
      !isProxyValue(value) &&
      objectIsFrozen(value),
    code,
  );
  return value;
}

function receiptCore(value, base, code) {
  const receipt = frozenReceipt(value, code);
  const rawOperation = ownDataValue(receipt, "operation", code);
  const rawReservation = ownDataValue(receipt, "reservation", code);
  const rawSession = ownDataValue(receipt, "session", code);
  const cloneBudget = { entries: 0, nodes: 0 };
  let operation;
  let reservation;
  let session;
  try {
    operation =
      rawOperation === null
        ? null
        : frozenDataClone(rawOperation, code, cloneBudget);
    reservation =
      rawReservation === null
        ? null
        : frozenDataClone(rawReservation, code, cloneBudget);
    const sessionCandidate = frozenDataClone(rawSession, code, cloneBudget);
    if (operation === null || reservation === null) {
      ensure(operation === null && reservation === null, code);
      session = assertSessionAuthoritySnapshot(sessionCandidate);
    } else {
      const transition = assertSessionOperationTransitionProof({
        operation,
        reservation,
        session: sessionCandidate,
      });
      operation = transition.operation;
      reservation = transition.reservation;
      session = transition.session;
    }
  } catch {
    fail(code);
  }
  let expectedBinding;
  try {
    expectedBinding = assertSessionOperationBinding(base);
  } catch {
    fail(code);
  }
  if (operation === null || reservation === null) {
    let observedBinding;
    try {
      observedBinding = assertSessionOperationBinding({
        ...base,
        expectedSession: session,
      });
    } catch {
      fail(code);
    }
    ensure(observedBinding.requestSha256 === expectedBinding.requestSha256, code);
  } else {
    ensure(
      operation.operationId === expectedBinding.operationId &&
        operation.kind === expectedBinding.kind &&
        operation.sessionId === expectedBinding.expectedSession.sessionId &&
        operation.requestSha256 === expectedBinding.requestSha256 &&
        reservation.reservationId === expectedBinding.reservationId,
      code,
    );
  }
  return exactFrozenRecord({ operation, reservation, session });
}

function receiptObservation(value, base, code) {
  const core = receiptCore(value, base, code);
  const state =
    core.operation === null
      ? "absent"
      : ownDataValue(core.operation, "state", code);
  ensure(
    arrayIncludes(["absent", "prepared", "starting", "uncertain", "committed"], state),
    code,
  );
  const rawFenceRequest = optionalOwnDataValue(value, "fenceRequest", code);
  const rawWriterEpoch = optionalOwnDataValue(value, "writerEpoch", code);
  const rawDispatchGranted = optionalOwnDataValue(value, "dispatchGranted", code);
  ensure(rawFenceRequest.found === rawWriterEpoch.found, code);
  if (rawDispatchGranted.found) {
    ensure(typeof rawDispatchGranted.value === "boolean", code);
  }
  let fenceRequest = null;
  let writerEpoch = null;
  if (rawFenceRequest.found) {
    try {
      fenceRequest = assertStorageForceFenceRequest(
        frozenDataClone(rawFenceRequest.value, code),
      );
    } catch {
      fail(code);
    }
    writerEpoch = rawWriterEpoch.value;
    ensure(typeof writerEpoch === "string", code);
  }
  return exactFrozenRecord({
    ...core,
    dispatchGranted: rawDispatchGranted.found
      ? rawDispatchGranted.value
      : null,
    fenceRequest,
    state,
    writerEpoch,
  });
}

function handoffProof(value, base, code) {
  const receipt = frozenReceipt(value, code);
  const cloneBudget = { entries: 0, nodes: 0 };
  let capture;
  let fence;
  let session;
  try {
    capture = frozenDataClone(
      ownDataValue(receipt, "capture", code),
      code,
      cloneBudget,
    );
    const rawFence = frozenDataClone(
      ownDataValue(receipt, "fence", code),
      code,
      cloneBudget,
    );
    fence = exactFrozenRecord({
      operation: ownDataValue(rawFence, "operation", code),
      reservation: ownDataValue(rawFence, "reservation", code),
    });
    session = frozenDataClone(
      ownDataValue(receipt, "session", code),
      code,
      cloneBudget,
    );
    const proof = assertWriterForceFenceAtomicCaptureHandoffProof({
      before: base.expectedSession,
      capture,
      fence,
      session,
    });
    ensure(proof.fence.operation.operationId === base.operationId, code);
    return proof;
  } catch {
    fail(code);
  }
}

function parseAuthorityReceipt(value, base) {
  const code = "postgres_writer_force_fence_atomic_capture_outcome_uncertain";
  const capture = optionalOwnDataValue(value, "capture", code);
  const fence = optionalOwnDataValue(value, "fence", code);
  if (capture.found || fence.found) {
    ensure(capture.found && fence.found, code);
    return exactFrozenRecord({
      kind: "handoff",
      proof: handoffProof(value, base, code),
    });
  }
  return exactFrozenRecord({
    kind: "operation",
    observation: receiptObservation(value, base, code),
  });
}

function blockedTransition(observation, base) {
  const code = "postgres_writer_force_fence_atomic_capture_outcome_uncertain";
  ensure(
    observation.state === "committed" &&
      observation.operation !== null &&
      observation.reservation !== null,
    code,
  );
  const result = ownDataValue(observation.operation, "result", code);
  ensure(
    result !== null &&
      typeof result === "object" &&
      ownDataValue(result, "outcome", code) === "writer-blocked" &&
      ownDataValue(result, "reason", code) === "provider-outcome-unresolved" &&
      observation.session.document.lifecycle === "BLOCKED" &&
      observation.session.document.activeOperation === null,
    code,
  );
  let binding;
  try {
    binding = assertSessionOperationBinding(base);
  } catch {
    fail(code);
  }
  ensure(observation.operation.requestSha256 === binding.requestSha256, code);
  return exactFrozenRecord({
    operation: observation.operation,
    reservation: observation.reservation,
    session: observation.session,
  });
}

function validateFenceRequest(value, writerEpoch, base, backend) {
  const code = "postgres_writer_force_fence_atomic_capture_outcome_uncertain";
  let request;
  try {
    request = assertStorageForceFenceRequest(value);
  } catch {
    fail(code);
  }
  const expectedEpoch = callIntrinsic(
    bigIntToStringIntrinsic,
    BigIntConstructor(base.expectedSession.document.writerEpoch) + 1n,
    [],
  );
  const lease = base.expectedSession.document.lease;
  ensure(
    lease !== null &&
      request.backendId === backend.backendId &&
      request.contractVersion === 1 &&
      request.operationId === base.operationId &&
      request.sessionId === base.expectedSession.sessionId &&
      request.storageId === base.expectedSession.document.storageRef.storageId &&
      request.target.attachmentId === base.request.target.attachmentId &&
      request.target.kind === "attachment" &&
      request.revokedFence.leaseId === lease.leaseId &&
      request.revokedFence.holderId === lease.holderId &&
      request.revokedFence.fencingEpoch === lease.fencingEpoch &&
      request.fencingEpoch === expectedEpoch &&
      writerEpoch === expectedEpoch,
    code,
  );
  return request;
}

function validateFenceResult(value, request) {
  try {
    return assertStorageForceFenceResult(value, { request });
  } catch {
    fail("postgres_writer_force_fence_atomic_capture_outcome_uncertain");
  }
}

function validateHandoffResult(proof, request, expectedResult) {
  const code = "postgres_writer_force_fence_atomic_capture_outcome_uncertain";
  let stored;
  try {
    stored = validateFenceResult(
      proof.fence.operation.result.fenceResult,
      request,
    );
  } catch {
    fail(code);
  }
  const expected = validateFenceResult(expectedResult, request);
  ensure(stored.proofId === expected.proofId, code);
  return proof;
}

async function reconcileAuthority(authority, base) {
  return parseAuthorityReceipt(
    await protectPromise(
      invokeAsync(authority, "reconcileWriterForceFenceAtomicCaptureHandoff", [
        base,
      ]),
    ),
    base,
  );
}

async function reserveOrReconcile(authority, base) {
  const code = "postgres_writer_force_fence_atomic_capture_outcome_uncertain";
  for (let attempt = 0; attempt < 2; attempt += 1) {
    try {
      return parseAuthorityReceipt(
        await protectPromise(invokeAsync(authority, "reserveOperation", [base])),
        base,
      );
    } catch {
      const observed = await protectPromise(reconcileAuthority(authority, base));
      if (
        observed.kind === "handoff" ||
        observed.observation.state !== "absent"
      ) {
        return observed;
      }
    }
  }
  fail(code);
}

async function claimOrReconcile(authority, base) {
  const code = "postgres_writer_force_fence_atomic_capture_outcome_uncertain";
  const transition = exactFrozenRecord({
    ...base,
    expectedOperationRevision: "0",
  });
  for (let attempt = 0; attempt < 2; attempt += 1) {
    try {
      const claimed = parseAuthorityReceipt(
        await protectPromise(
          invokeAsync(authority, "claimWriterForceFenceDispatch", [transition]),
        ),
        base,
      );
      if (claimed.kind === "handoff") return claimed;
      ensure(claimed.observation.state !== "absent", code);
      return claimed;
    } catch {
      const observed = await protectPromise(reconcileAuthority(authority, base));
      if (
        observed.kind === "handoff" ||
        observed.observation.state !== "prepared"
      ) {
        return observed;
      }
    }
  }
  fail(code);
}

async function markUncertain(authority, base, observation, assertHeld) {
  const code = "postgres_writer_force_fence_atomic_capture_outcome_uncertain";
  let observed = exactFrozenRecord({
    kind: "operation",
    observation,
  });
  if (observation.state !== "starting") return observed;
  const transition = exactFrozenRecord({
    ...base,
    expectedOperationRevision: "1",
  });
  for (let attempt = 0; attempt < 2; attempt += 1) {
    try {
      await protectPromise(assertGuardHeld(assertHeld));
      observed = parseAuthorityReceipt(
        await protectPromise(
          invokeAsync(authority, "markOperationUncertain", [transition]),
        ),
        base,
      );
    } catch {
      observed = await protectPromise(reconcileAuthority(authority, base));
    }
    if (observed.kind === "handoff") return observed;
    if (observed.observation.state !== "starting") return observed;
  }
  fail(code);
}

async function finalizeBlocked(authority, base, observation, assertHeld) {
  const code = "postgres_writer_force_fence_atomic_capture_outcome_uncertain";
  let observed = await protectPromise(
    markUncertain(authority, base, observation, assertHeld),
  );
  if (observed.kind === "handoff") return observed.proof;
  if (observed.observation.state === "committed") {
    return blockedTransition(observed.observation, base);
  }
  ensure(observed.observation.state === "uncertain", code);
  const transition = exactFrozenRecord({
    ...base,
    expectedOperationRevision: "2",
    reason: "provider-outcome-unresolved",
  });
  for (let attempt = 0; attempt < 2; attempt += 1) {
    try {
      await protectPromise(assertGuardHeld(assertHeld));
      observed = parseAuthorityReceipt(
        await protectPromise(
          invokeAsync(authority, "finalizeWriterOperationBlocked", [transition]),
        ),
        base,
      );
    } catch {
      observed = await protectPromise(reconcileAuthority(authority, base));
    }
    if (observed.kind === "handoff") return observed.proof;
    if (observed.observation.state === "committed") {
      return blockedTransition(observed.observation, base);
    }
    ensure(observed.observation.state === "uncertain", code);
  }
  fail(code);
}

async function reconcileProvider(backend, request, assertHeld) {
  if (!backend.supportsReconciliation) return null;
  try {
    await protectPromise(assertGuardHeld(assertHeld));
    const raw = await protectPromise(
      invokeProvider(backend, "reconcileForceFence", request),
    );
    const reconciled = assertStorageForceFenceReconciliationResult(raw, {
      request,
    });
    await protectPromise(assertGuardHeld(assertHeld));
    return reconciled.outcome === "committed" ? reconciled.result : null;
  } catch {
    return null;
  }
}

async function finalizeHandoff(
  authority,
  base,
  observation,
  request,
  result,
  assertHeld,
) {
  const code = "postgres_writer_force_fence_atomic_capture_outcome_uncertain";
  let observed = exactFrozenRecord({
    kind: "operation",
    observation,
  });
  for (let attempt = 0; attempt < 2; attempt += 1) {
    if (observed.kind === "handoff") {
      return validateHandoffResult(observed.proof, request, result);
    }
    if (observed.observation.state === "committed") {
      return blockedTransition(observed.observation, base);
    }
    ensure(
      observed.observation.state === "starting" ||
        observed.observation.state === "uncertain",
      code,
    );
    const revision = observed.observation.operation.revision;
    ensure(revision === "1" || revision === "2", code);
    const transition = exactFrozenRecord({
      ...base,
      expectedOperationRevision: revision,
      fenceResult: result,
    });
    try {
      await protectPromise(assertGuardHeld(assertHeld));
      observed = parseAuthorityReceipt(
        await protectPromise(
          invokeAsync(
            authority,
            "finalizeWriterForceFenceAtomicCaptureHandoff",
            [transition],
          ),
        ),
        base,
      );
    } catch {
      observed = await protectPromise(reconcileAuthority(authority, base));
    }
  }
  if (observed.kind === "handoff") {
    return validateHandoffResult(observed.proof, request, result);
  }
  if (observed.observation.state === "committed") {
    return blockedTransition(observed.observation, base);
  }
  fail(code);
}

async function recoverOrBlock(
  authority,
  backend,
  base,
  observation,
  assertHeld,
) {
  const code = "postgres_writer_force_fence_atomic_capture_outcome_uncertain";
  if (observation.fenceRequest === null) {
    const readback = await protectPromise(reconcileAuthority(authority, base));
    if (readback.kind === "handoff") return readback.proof;
    if (readback.observation.state === "committed") {
      return blockedTransition(readback.observation, base);
    }
    observation = readback.observation;
  }
  ensure(
    observation.state === "starting" || observation.state === "uncertain",
    code,
  );
  ensure(observation.fenceRequest !== null, code);
  const request = validateFenceRequest(
    observation.fenceRequest,
    observation.writerEpoch,
    base,
    backend,
  );
  const result = await protectPromise(
    reconcileProvider(backend, request, assertHeld),
  );
  if (result !== null) {
    return await protectPromise(
      finalizeHandoff(
        authority,
        base,
        observation,
        request,
        result,
        assertHeld,
      ),
    );
  }
  await protectPromise(assertGuardHeld(assertHeld));
  return await protectPromise(
    finalizeBlocked(authority, base, observation, assertHeld),
  );
}

async function execute(
  authority,
  operationGuard,
  backend,
  request,
) {
  const base = normalizeRequest(request, backend);
  return await protectPromise(
    runGuarded(operationGuard, base.operationId, async (assertHeld) => {
      const code =
        "postgres_writer_force_fence_atomic_capture_outcome_uncertain";
      try {
        await protectPromise(assertGuardHeld(assertHeld));
      } catch {
        fail(code);
      }
      let observed = await protectPromise(reserveOrReconcile(authority, base));
      if (observed.kind === "handoff") return observed.proof;
      if (observed.observation.state === "committed") {
        return blockedTransition(observed.observation, base);
      }
      if (
        observed.observation.state === "starting" ||
        observed.observation.state === "uncertain"
      ) {
        return await protectPromise(
          recoverOrBlock(
            authority,
            backend,
            base,
            observed.observation,
            assertHeld,
          ),
        );
      }
      ensure(observed.observation.state === "prepared", code);

      observed = await protectPromise(claimOrReconcile(authority, base));
      if (observed.kind === "handoff") return observed.proof;
      if (observed.observation.state === "committed") {
        return blockedTransition(observed.observation, base);
      }
      const claimed = observed.observation;
      if (claimed.dispatchGranted !== true) {
        ensure(
          claimed.state === "starting" || claimed.state === "uncertain",
          code,
        );
        return await protectPromise(
          recoverOrBlock(authority, backend, base, claimed, assertHeld),
        );
      }
      ensure(claimed.state === "starting" && claimed.fenceRequest !== null, code);
      const fenceRequest = validateFenceRequest(
        claimed.fenceRequest,
        claimed.writerEpoch,
        base,
        backend,
      );

      let fenceResult = null;
      try {
        await protectPromise(assertGuardHeld(assertHeld));
        const raw = await protectPromise(
          invokeProvider(backend, "forceFence", fenceRequest),
        );
        fenceResult = validateFenceResult(raw, fenceRequest);
        await protectPromise(assertGuardHeld(assertHeld));
      } catch {
        fenceResult = await protectPromise(
          reconcileProvider(backend, fenceRequest, assertHeld),
        );
      }
      if (fenceResult === null) {
        await protectPromise(assertGuardHeld(assertHeld));
        return await protectPromise(
          finalizeBlocked(authority, base, claimed, assertHeld),
        );
      }
      return await protectPromise(
        finalizeHandoff(
          authority,
          base,
          claimed,
          fenceRequest,
          fenceResult,
          assertHeld,
        ),
      );
    }),
  );
}

async function invokeFacadeMethod(
  authority,
  operationGuard,
  backend,
  methodArgs,
) {
  ensure(
    methodArgs.length === 1,
    "invalid_postgres_writer_force_fence_atomic_capture_composition_request",
  );
  return await protectPromise(
    execute(authority, operationGuard, backend, methodArgs[0]),
  );
}

export function createPostgresWriterForceFenceAtomicCaptureComposition(
  ...args
) {
  const code =
    "invalid_postgres_writer_force_fence_atomic_capture_composition_options";
  ensure(args.length === 1, code);
  const options = exactDataObject(args[0], OPTION_KEYS, code);
  const authority = collaboratorBinding(options.authority, AUTHORITY_METHODS, code);
  const operationGuard = operationGuardBinding(options.operationGuard, code);
  const backend = storageBackendBinding(options.storageBackend, code);
  const forceFenceWriterAtomicCapture = function forceFenceWriterAtomicCapture(
    ...methodArgs
  ) {
    return protectPromise(
      invokeFacadeMethod(authority, operationGuard, backend, methodArgs),
    );
  };
  objectFreeze(forceFenceWriterAtomicCapture);
  const facade = exactFrozenRecord({ forceFenceWriterAtomicCapture });
  weakSetAdd(facades, facade);
  return facade;
}

export function isPostgresWriterForceFenceAtomicCaptureComposition(value) {
  return (
    value !== null &&
    (typeof value === "object" || typeof value === "function") &&
    !isProxyValue(value) &&
    weakSetHas(facades, value)
  );
}

objectFreeze(PostgresWriterForceFenceAtomicCaptureCompositionError.prototype);
objectFreeze(PostgresWriterForceFenceAtomicCaptureCompositionError);
objectFreeze(createPostgresWriterForceFenceAtomicCaptureComposition);
objectFreeze(isPostgresWriterForceFenceAtomicCaptureComposition);
