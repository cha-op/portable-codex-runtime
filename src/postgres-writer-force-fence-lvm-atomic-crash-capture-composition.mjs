import { types as utilTypes } from "node:util";

import { createLvmAtomicCrashCaptureProvider } from "./lvm-atomic-crash-capture-provider.mjs";
import {
  ATOMIC_CRASH_CAPTURE_OPERATION_KIND,
  assertSessionOperationBinding,
  assertSessionOperationTransitionProof,
} from "./postgres-session-authority.mjs";
import {
  PostgresOperationGuard,
  isPostgresOperationGuard,
} from "./postgres-operation-guard.mjs";
import {
  capturePreparedAtomicCrashCheckpoint,
  prepareAtomicCrashCapture,
  verifyCommittedAtomicCrashCapture,
} from "./session-crash-capture-core.mjs";
import {
  assertAtomicCrashCaptureRequest,
  assertAtomicCrashCaptureResult,
} from "./session-storage-contracts.mjs";

// Capture intrinsics before any authority, catalogue, driver, or storage
// collaborator can run. The protected property is one exact durable capture:
// only the authority-bound request may consume the provider dispatch token,
// and only physically verified committed evidence may release its blocker.
const arrayIncludesIntrinsic = Array.prototype.includes;
const arrayIsArray = Array.isArray;
const arrayPrototype = Array.prototype;
const ArrayConstructor = Array;
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
const reflectApply = Reflect.apply;
const reflectOwnKeys = Reflect.ownKeys;
const regexpExecIntrinsic = RegExp.prototype.exec;
const runExclusiveIntrinsic = PostgresOperationGuard.prototype.runExclusive;
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

const OPAQUE_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u;
const MAX_CLONE_DEPTH = 64;
const MAX_CLONE_ENTRIES = 131_072;
const MAX_CLONE_NODES = 65_536;
const OPTION_KEYS = objectFreeze([
  "authority",
  "catalogue",
  "driver",
  "operationGuard",
  "storageBackend",
]);
const INVOCATION_KEYS = objectFreeze(["operationId", "request"]);
const AUTHORITY_METHODS = objectFreeze([
  "finalizeAtomicCrashCapture",
  "readAtomicCrashCapture",
]);
const AUTHORITY_ADMISSION_KEYS = objectFreeze([
  "captureAuthority",
  "request",
]);
const PROVIDER_STATES = objectFreeze([
  "absent",
  "starting",
  "uncertain",
  "committed",
]);
const ERROR_MESSAGES = objectFreeze({
  invalid_postgres_writer_force_fence_lvm_atomic_crash_capture_composition_options:
    "PostgreSQL writer force-fence LVM atomic crash-capture composition options are invalid",
  invalid_postgres_writer_force_fence_lvm_atomic_crash_capture_composition_request:
    "PostgreSQL writer force-fence LVM atomic crash-capture composition request is invalid",
  postgres_writer_force_fence_lvm_atomic_crash_capture_outcome_uncertain:
    "PostgreSQL writer force-fence LVM atomic crash-capture outcome is uncertain",
});
const INVOCATION_UNCERTAIN = objectFreeze(objectCreate(null));
const facades = new WeakSetConstructor();
const dispatchAttempts = new WeakMapConstructor();
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
  return reflectApply(intrinsic, receiver, args);
}

function arrayIncludes(value, candidate) {
  return callIntrinsic(arrayIncludesIntrinsic, value, [candidate]);
}

function regexpTest(pattern, value) {
  return callIntrinsic(regexpExecIntrinsic, pattern, [value]) !== null;
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
  callIntrinsic(weakSetDeleteIntrinsic, set, [value]);
}

function weakSetHas(set, value) {
  return callIntrinsic(weakSetHasIntrinsic, set, [value]);
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

export class PostgresWriterForceFenceLvmAtomicCrashCaptureCompositionError extends Error {
  constructor(code) {
    if (typeof code !== "string" || !objectHasOwn(ERROR_MESSAGES, code)) {
      throw new TypeErrorConstructor(
        "Unsupported PostgreSQL writer force-fence LVM atomic crash-capture composition error",
      );
    }
    super(ERROR_MESSAGES[code]);
    objectDefineProperty(this, "name", {
      enumerable: false,
      value:
        "PostgresWriterForceFenceLvmAtomicCrashCaptureCompositionError",
    });
    objectDefineProperty(this, "code", { enumerable: true, value: code });
    objectDefineProperty(this, "retryable", {
      enumerable: true,
      value: false,
    });
    objectDefineProperty(this, "stack", {
      enumerable: false,
      value:
        "PostgresWriterForceFenceLvmAtomicCrashCaptureCompositionError: " +
        ERROR_MESSAGES[code],
    });
    objectFreeze(this);
  }
}

function fail(code) {
  throw new PostgresWriterForceFenceLvmAtomicCrashCaptureCompositionError(code);
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

function sameFrozenData(left, right, state = { nodes: 0 }, depth = 0) {
  if (objectIs(left, right)) return true;
  if (
    left === null ||
    right === null ||
    typeof left !== "object" ||
    typeof right !== "object" ||
    isProxyValue(left) ||
    isProxyValue(right) ||
    arrayIsArray(left) ||
    arrayIsArray(right) ||
    !objectIsFrozen(left) ||
    !objectIsFrozen(right) ||
    depth >= 24 ||
    state.nodes >= 1_024
  ) {
    return false;
  }
  let leftPrototype;
  let rightPrototype;
  let leftKeys;
  let rightKeys;
  try {
    leftPrototype = objectGetPrototypeOf(left);
    rightPrototype = objectGetPrototypeOf(right);
    leftKeys = reflectOwnKeys(left);
    rightKeys = reflectOwnKeys(right);
  } catch {
    return false;
  }
  if (
    leftPrototype !== rightPrototype ||
    (leftPrototype !== objectPrototype && leftPrototype !== null) ||
    leftKeys.length !== rightKeys.length
  ) {
    return false;
  }
  state.nodes += 1;
  for (let index = 0; index < leftKeys.length; index += 1) {
    const key = leftKeys[index];
    if (typeof key !== "string" || !arrayIncludes(rightKeys, key)) return false;
    let leftDescriptor;
    let rightDescriptor;
    try {
      leftDescriptor = objectGetOwnPropertyDescriptor(left, key);
      rightDescriptor = objectGetOwnPropertyDescriptor(right, key);
    } catch {
      return false;
    }
    if (
      leftDescriptor?.enumerable !== true ||
      rightDescriptor?.enumerable !== true ||
      !objectHasOwn(leftDescriptor, "value") ||
      !objectHasOwn(rightDescriptor, "value") ||
      !sameFrozenData(
        leftDescriptor.value,
        rightDescriptor.value,
        state,
        depth + 1,
      )
    ) {
      return false;
    }
  }
  return true;
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

function normalizeProbe(value, code) {
  const probe = exactDataObject(value, ["assertHeld"], code);
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
  const code =
    "postgres_writer_force_fence_lvm_atomic_crash_capture_outcome_uncertain";
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

function normalizeInvocation(value, backendId) {
  const code =
    "invalid_postgres_writer_force_fence_lvm_atomic_crash_capture_composition_request";
  const input = exactDataObject(value, INVOCATION_KEYS, code);
  ensure(
    typeof input.operationId === "string" &&
      regexpTest(OPAQUE_ID_PATTERN, input.operationId),
    code,
  );
  let request;
  try {
    request = assertAtomicCrashCaptureRequest(input.request);
  } catch {
    fail(code);
  }
  ensure(
    request.storageRef.backendId === backendId &&
      input.operationId !== request.mutationRequest.operationId,
    code,
  );
  return exactFrozenRecord({ operationId: input.operationId, request });
}

function normalizeCaptureResult(value, request, code, previousResult = null) {
  try {
    return previousResult === null
      ? assertAtomicCrashCaptureResult(value, { request })
      : assertAtomicCrashCaptureResult(value, { previousResult, request });
  } catch {
    fail(code);
  }
}

function normalizeAuthorityReceipt(value, input) {
  const code =
    "postgres_writer_force_fence_lvm_atomic_crash_capture_outcome_uncertain";
  ensure(
    value !== null &&
      typeof value === "object" &&
      !isProxyValue(value) &&
      objectIsFrozen(value),
    code,
  );
  const cloneBudget = { entries: 0, nodes: 0 };
  let transition;
  try {
    transition = assertSessionOperationTransitionProof({
      operation: frozenDataClone(
        ownDataValue(value, "operation", code),
        code,
        cloneBudget,
      ),
      reservation: frozenDataClone(
        ownDataValue(value, "reservation", code),
        code,
        cloneBudget,
      ),
      session: frozenDataClone(
        ownDataValue(value, "session", code),
        code,
        cloneBudget,
      ),
    });
  } catch {
    fail(code);
  }
  const { operation, reservation, session } = transition;
  ensure(
    operation.state === "prepared" || operation.state === "committed",
    code,
  );
  const providerState = ownDataValue(value, "providerState", code);
  const status = ownDataValue(value, "status", code);
  ensure(
    arrayIncludes(PROVIDER_STATES, providerState) && status === operation.state,
    code,
  );
  let binding;
  try {
    binding = assertSessionOperationBinding({
      expectedSession: operation.expectedSession,
      kind: ATOMIC_CRASH_CAPTURE_OPERATION_KIND,
      operationId: input.operationId,
      request: exactFrozenRecord({
        operationId: input.operationId,
        request: input.request,
      }),
    });
  } catch {
    fail(code);
  }
  ensure(
    operation.operationId === binding.operationId &&
      operation.kind === binding.kind &&
      operation.sessionId === binding.expectedSession.sessionId &&
      operation.requestSha256 === binding.requestSha256 &&
      reservation.reservationId === binding.reservationId,
    code,
  );

  const rawCaptureResult = ownDataValue(value, "captureResult", code);
  let captureResult = null;
  if (rawCaptureResult !== null) {
    captureResult = normalizeCaptureResult(
      frozenDataClone(rawCaptureResult, code, cloneBudget),
      input.request,
      code,
    );
    ensure(providerState === "committed", code);
  }

  if (operation.state === "prepared") {
    const active = session.document.activeOperation;
    ensure(
      operation.revision === "0" &&
        operation.result === null &&
        reservation.state === "prepared" &&
        session.document.lifecycle === "DETACHED" &&
        active !== null &&
        active.operationId === operation.operationId &&
        active.kind === operation.kind &&
        active.requestSha256 === operation.requestSha256 &&
        active.reservationId === reservation.reservationId &&
        active.state === "prepared",
      code,
    );
  } else {
    const last = session.document.lastOperation;
    ensure(
      operation.revision === "1" &&
        operation.result?.outcome === "atomic-crash-captured" &&
        reservation.state === "released" &&
        providerState === "committed" &&
        captureResult !== null &&
        session.document.lifecycle === "RECOVERY_REQUIRED" &&
        session.document.activeOperation === null &&
        last !== null &&
        last.operationId === operation.operationId &&
        last.kind === operation.kind &&
        last.requestSha256 === operation.requestSha256 &&
        last.reservationId === reservation.reservationId &&
        last.state === "committed",
      code,
    );
  }
  return exactFrozenRecord({
    captureResult,
    operation,
    providerState,
    reservation,
    session,
    status,
  });
}

async function readAuthority(authority, input, assertHeld) {
  const code =
    "postgres_writer_force_fence_lvm_atomic_crash_capture_outcome_uncertain";
  try {
    await protectPromise(assertGuardHeld(assertHeld));
    const raw = await protectPromise(
      invokeAsync(authority, "readAtomicCrashCapture", [
        exactFrozenRecord({
          operationId: input.operationId,
          request: input.request,
        }),
      ]),
    );
    await protectPromise(assertGuardHeld(assertHeld));
    return normalizeAuthorityReceipt(raw, input);
  } catch {
    fail(code);
  }
}

async function verifyCommitted(backend, input, expectedResult, assertHeld) {
  const code =
    "postgres_writer_force_fence_lvm_atomic_crash_capture_outcome_uncertain";
  let verification;
  try {
    await protectPromise(assertGuardHeld(assertHeld));
    verification = await protectPromise(
      verifyCommittedAtomicCrashCapture({
        backend,
        request: input.request,
      }),
    );
    await protectPromise(assertGuardHeld(assertHeld));
  } catch {
    fail(code);
  }
  if (verification.outcome !== "committed") return null;
  return normalizeCaptureResult(
    verification.result,
    input.request,
    code,
    expectedResult,
  );
}

async function returnCommitted(backend, input, observation, assertHeld) {
  const code =
    "postgres_writer_force_fence_lvm_atomic_crash_capture_outcome_uncertain";
  ensure(
    observation.operation.state === "committed" &&
      observation.captureResult !== null,
    code,
  );
  const verified = await protectPromise(
    verifyCommitted(
      backend,
      input,
      observation.captureResult,
      assertHeld,
    ),
  );
  ensure(verified !== null, code);
  return verified;
}

async function finalizeCommitted(
  authority,
  backend,
  input,
  observation,
  captureResult,
  assertHeld,
) {
  const code =
    "postgres_writer_force_fence_lvm_atomic_crash_capture_outcome_uncertain";
  ensure(observation.operation.state === "prepared", code);
  const transition = exactFrozenRecord({
    captureResult,
    expectedOperationRevision: "0",
    expectedSession: observation.operation.expectedSession,
    kind: ATOMIC_CRASH_CAPTURE_OPERATION_KIND,
    operationId: input.operationId,
    request: exactFrozenRecord({
      operationId: input.operationId,
      request: input.request,
    }),
  });
  let finalized;
  try {
    await protectPromise(assertGuardHeld(assertHeld));
    finalized = normalizeAuthorityReceipt(
      await protectPromise(
        invokeAsync(authority, "finalizeAtomicCrashCapture", [transition]),
      ),
      input,
    );
    await protectPromise(assertGuardHeld(assertHeld));
  } catch {
    finalized = await protectPromise(
      readAuthority(authority, input, assertHeld),
    );
  }
  ensure(finalized.operation.state === "committed", code);
  normalizeCaptureResult(
    finalized.captureResult,
    input.request,
    code,
    captureResult,
  );
  return await protectPromise(
    returnCommitted(backend, input, finalized, assertHeld),
  );
}

async function recoverPrepared(
  authority,
  backend,
  input,
  observation,
  expectedResult,
  assertHeld,
) {
  const code =
    "postgres_writer_force_fence_lvm_atomic_crash_capture_outcome_uncertain";
  ensure(observation.operation.state === "prepared", code);
  const durableExpected =
    expectedResult ?? observation.captureResult;
  const verified = await protectPromise(
    verifyCommitted(backend, input, durableExpected, assertHeld),
  );
  ensure(verified !== null, code);
  return await protectPromise(
    finalizeCommitted(
      authority,
      backend,
      input,
      observation,
      verified,
      assertHeld,
    ),
  );
}

async function consumeProviderAuthority(admissionValue, runCaptureValue) {
  const code =
    "postgres_writer_force_fence_lvm_atomic_crash_capture_outcome_uncertain";
  const admission = exactDataObject(
    admissionValue,
    AUTHORITY_ADMISSION_KEYS,
    code,
  );
  ensure(objectIsFrozen(admissionValue), code);
  const token = admission.captureAuthority;
  ensure(
    token !== null &&
      typeof token === "object" &&
      !isProxyValue(token) &&
      objectIsFrozen(token),
    code,
  );
  const attempt = weakMapGet(dispatchAttempts, token);
  ensure(
    attempt !== undefined &&
      attempt.state === "armed" &&
      sameFrozenData(
        assertAtomicCrashCaptureRequest(admission.request),
        attempt.input.request,
      ),
    code,
  );
  const runCapture = trustedFunction(runCaptureValue, code);
  ensure(objectIsFrozen(runCaptureValue), code);
  attempt.state = "checking-authority";
  const observation = await protectPromise(
    readAuthority(attempt.authority, attempt.input, attempt.assertHeld),
  );
  ensure(
    observation.operation.state === "prepared" &&
      observation.providerState === "starting" &&
      observation.captureResult === null,
    code,
  );
  await protectPromise(assertGuardHeld(attempt.assertHeld));
  attempt.state = "dispatching";
  let pending;
  try {
    pending = callIntrinsic(runCapture, undefined, []);
  } catch {
    attempt.state = "uncertain";
    fail(code);
  }
  const normalized = normalizeNativePromise(pending);
  if (normalized === null) {
    attempt.state = "uncertain";
    fail(code);
  }
  try {
    const result = await normalized.promise;
    normalizeCaptureResult(result, attempt.input.request, code);
    ensure(normalized.accepted && attempt.state === "dispatching", code);
    await protectPromise(assertGuardHeld(attempt.assertHeld));
    attempt.state = "captured";
    return result;
  } catch {
    attempt.state = "uncertain";
    fail(code);
  }
}

objectFreeze(consumeProviderAuthority);

async function freshDispatch(
  authority,
  backend,
  input,
  observation,
  assertHeld,
) {
  const code =
    "postgres_writer_force_fence_lvm_atomic_crash_capture_outcome_uncertain";
  ensure(
    observation.operation.state === "prepared" &&
      observation.providerState === "absent" &&
      observation.captureResult === null,
    code,
  );
  let preparedCapture;
  try {
    preparedCapture = prepareAtomicCrashCapture({
      backend,
      request: input.request,
    });
  } catch {
    fail(code);
  }
  const captureAuthority = objectFreeze(objectCreate(null));
  const attempt = {
    assertHeld,
    authority,
    input,
    state: "armed",
  };
  weakMapSet(dispatchAttempts, captureAuthority, attempt);
  let result = null;
  try {
    await protectPromise(assertGuardHeld(assertHeld));
    result = await protectPromise(
      capturePreparedAtomicCrashCheckpoint({
        captureAuthority,
        preparedCapture,
      }),
    );
    await protectPromise(assertGuardHeld(assertHeld));
    ensure(attempt.state === "armed" || attempt.state === "captured", code);
    result = normalizeCaptureResult(result, input.request, code);
  } catch {
    attempt.state = "uncertain";
    result = null;
  } finally {
    weakMapDelete(dispatchAttempts, captureAuthority);
  }
  return await protectPromise(
    recoverPrepared(
      authority,
      backend,
      input,
      observation,
      result,
      assertHeld,
    ),
  );
}

async function execute(
  authority,
  operationGuard,
  backend,
  backendId,
  value,
  allowFresh,
) {
  const input = normalizeInvocation(value, backendId);
  return await protectPromise(
    runGuarded(operationGuard, input.operationId, async (assertHeld) => {
      const observation = await protectPromise(
        readAuthority(authority, input, assertHeld),
      );
      if (observation.operation.state === "committed") {
        return await protectPromise(
          returnCommitted(backend, input, observation, assertHeld),
        );
      }
      if (allowFresh && observation.providerState === "absent") {
        return await protectPromise(
          freshDispatch(authority, backend, input, observation, assertHeld),
        );
      }
      return await protectPromise(
        recoverPrepared(
          authority,
          backend,
          input,
          observation,
          null,
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
  backendId,
  allowFresh,
  args,
) {
  ensure(
    args.length === 1,
    "invalid_postgres_writer_force_fence_lvm_atomic_crash_capture_composition_request",
  );
  return await protectPromise(
    execute(
      authority,
      operationGuard,
      backend,
      backendId,
      args[0],
      allowFresh,
    ),
  );
}

export function createPostgresWriterForceFenceLvmAtomicCrashCaptureComposition(
  ...args
) {
  const code =
    "invalid_postgres_writer_force_fence_lvm_atomic_crash_capture_composition_options";
  ensure(args.length === 1, code);
  const options = exactDataObject(args[0], OPTION_KEYS, code);
  const authority = collaboratorBinding(options.authority, AUTHORITY_METHODS, code);
  const operationGuard = operationGuardBinding(options.operationGuard, code);
  let backend;
  try {
    backend = createLvmAtomicCrashCaptureProvider({
      authorityConsumer: consumeProviderAuthority,
      baseBackend: options.storageBackend,
      catalogue: options.catalogue,
      driver: options.driver,
    });
  } catch {
    fail(code);
  }
  const backendId = backend.backendId;
  const runPreparedCapture = function runPreparedCapture(...methodArgs) {
    return protectPromise(
      invokeFacadeMethod(
        authority,
        operationGuard,
        backend,
        backendId,
        true,
        methodArgs,
      ),
    );
  };
  const reconcileCapture = function reconcileCapture(...methodArgs) {
    return protectPromise(
      invokeFacadeMethod(
        authority,
        operationGuard,
        backend,
        backendId,
        false,
        methodArgs,
      ),
    );
  };
  objectFreeze(runPreparedCapture);
  objectFreeze(reconcileCapture);
  const facade = exactFrozenRecord({ reconcileCapture, runPreparedCapture });
  weakSetAdd(facades, facade);
  return facade;
}

export function isPostgresWriterForceFenceLvmAtomicCrashCaptureComposition(
  value,
) {
  return (
    value !== null &&
    (typeof value === "object" || typeof value === "function") &&
    !isProxyValue(value) &&
    weakSetHas(facades, value)
  );
}

objectFreeze(
  PostgresWriterForceFenceLvmAtomicCrashCaptureCompositionError.prototype,
);
objectFreeze(PostgresWriterForceFenceLvmAtomicCrashCaptureCompositionError);
objectFreeze(
  createPostgresWriterForceFenceLvmAtomicCrashCaptureComposition,
);
objectFreeze(
  isPostgresWriterForceFenceLvmAtomicCrashCaptureComposition,
);
