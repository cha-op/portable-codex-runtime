import { types as utilTypes } from "node:util";

import {
  WRITER_FORCE_FENCE_OPERATION_KIND,
  WRITER_RELEASE_OPERATION_KIND,
  assertSessionAuthoritySnapshot,
  assertSessionOperationBinding,
  assertSessionOperationTransitionProof,
} from "./postgres-session-authority.mjs";
import {
  PostgresOperationGuard,
  isPostgresOperationGuard,
} from "./postgres-operation-guard.mjs";
import {
  assertStorageBackend,
  assertStorageBackendCapabilities,
  assertStorageForceFenceRequest,
  assertStorageForceFenceResult,
  assertStorageMutationRequest,
  assertStorageMutationResult,
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
const WeakSetConstructor = WeakSet;
const weakSetDeleteIntrinsic = WeakSet.prototype.delete;
const weakSetAddIntrinsic = WeakSet.prototype.add;
const weakSetHasIntrinsic = WeakSet.prototype.has;
const WeakMapConstructor = WeakMap;
const weakMapGetIntrinsic = WeakMap.prototype.get;
const weakMapHasIntrinsic = WeakMap.prototype.has;
const weakMapSetIntrinsic = WeakMap.prototype.set;

const OPAQUE_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u;
const MAX_RECEIPT_CLONE_DEPTH = 64;
const MAX_RECEIPT_CLONE_ENTRIES = 131_072;
const MAX_RECEIPT_CLONE_NODES = 65_536;

const OPTION_KEYS = objectFreeze([
  "authority",
  "operationGuard",
  "storageBackend",
]);
const REQUEST_KEYS = objectFreeze([
  "expectedSession",
  "operationId",
  "target",
]);
const TARGET_KEYS = objectFreeze(["attachmentId", "kind"]);
const PROBE_KEYS = objectFreeze(["assertHeld"]);
const AUTHORITY_METHODS = objectFreeze([
  "claimWriterForceFenceDispatch",
  "claimWriterReleaseDispatch",
  "finalizeWriterForceFence",
  "finalizeWriterOperationBlocked",
  "finalizeWriterRelease",
  "markOperationUncertain",
  "reconcileOperation",
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
const CAPABILITY_KEYS = objectFreeze([
  "atomicPointInTimeCheckpoint",
  "exclusiveWriterAttachment",
  "fencing",
  "normalDirectoryAttachment",
]);
const TERMINAL_OUTCOMES = objectFreeze([
  "writer-blocked",
  "writer-fenced",
  "writer-released",
]);

const ERROR_MESSAGES = objectFreeze({
  invalid_postgres_writer_detach_composition_options:
    "PostgreSQL writer detach composition options are invalid",
  invalid_postgres_writer_detach_composition_request:
    "PostgreSQL writer detach composition request is invalid",
  postgres_writer_detach_composition_outcome_uncertain:
    "PostgreSQL writer detach composition outcome is uncertain",
});

const facades = new WeakSetConstructor();
const INVOCATION_UNCERTAIN = objectFreeze(objectCreate(null));

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
    callIntrinsic(promiseResolveIntrinsic, PromiseConstructor, [
      protectPromise(value),
    ]),
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

export class PostgresWriterDetachCompositionError extends Error {
  constructor(code) {
    if (typeof code !== "string" || !objectHasOwn(ERROR_MESSAGES, code)) {
      throw new TypeErrorConstructor(
        "Unsupported PostgreSQL writer detach composition error",
      );
    }
    const message = ERROR_MESSAGES[code];
    super(message);
    objectDefineProperty(this, "name", {
      enumerable: false,
      value: "PostgresWriterDetachCompositionError",
    });
    objectDefineProperty(this, "code", {
      enumerable: true,
      value: code,
    });
    objectDefineProperty(this, "retryable", {
      enumerable: true,
      value: false,
    });
    objectDefineProperty(this, "stack", {
      enumerable: false,
      value: `PostgresWriterDetachCompositionError: ${message}`,
    });
    objectFreeze(this);
  }
}

function fail(code) {
  throw new PostgresWriterDetachCompositionError(code);
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
    ensure(
      typeof key === "string" && arrayIncludes(expectedKeys, key),
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
    const descriptor = objectGetOwnPropertyDescriptor(value, key);
    objectDefineProperty(record, key, {
      enumerable: true,
      value: descriptor.value,
    });
  }
  return objectFreeze(record);
}

function frozenDataCloneInternal(
  value,
  code,
  active,
  copies,
  budget,
  depth,
) {
  ensure(depth <= MAX_RECEIPT_CLONE_DEPTH, code);
  budget.nodes += 1;
  ensure(budget.nodes <= MAX_RECEIPT_CLONE_NODES, code);
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
  ensure(frozen, code);
  ensure(!weakSetHas(active, value), code);
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
    keys.length <= MAX_RECEIPT_CLONE_ENTRIES &&
      budget.entries <= MAX_RECEIPT_CLONE_ENTRIES,
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
      let lengthDescriptor;
      try {
        lengthDescriptor = objectGetOwnPropertyDescriptor(value, "length");
      } catch {
        fail(code);
      }
      ensure(
        lengthDescriptor !== undefined &&
          objectHasOwn(lengthDescriptor, "value") &&
          numberIsSafeInteger(lengthDescriptor.value) &&
          lengthDescriptor.value >= 0 &&
          lengthDescriptor.value <= MAX_RECEIPT_CLONE_ENTRIES,
        code,
      );
      clone = new ArrayConstructor(lengthDescriptor.value);
    } else {
      clone = objectCreate(prototype);
    }
    for (let index = 0; index < keys.length; index += 1) {
      const key = keys[index];
      ensure(typeof key === "string", code);
      let descriptor;
      try {
        descriptor = objectGetOwnPropertyDescriptor(value, key);
      } catch {
        fail(code);
      }
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

function frozenDataClone(
  value,
  code,
  budget = { entries: 0, nodes: 0 },
) {
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
  const then = findDataValue(value, "then", code);
  ensure(!then.found, code);
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
    methods: exactFrozenRecord({
      runExclusive: runExclusiveIntrinsic,
    }),
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
  const identityFields = ["backendId", "capabilities", "contractVersion"];
  let capabilities;
  for (let index = 0; index < identityFields.length; index += 1) {
    const name = identityFields[index];
    const resolved = findDataValue(value, name, code);
    ensure(resolved.found, code);
    let projectedValue = resolved.value;
    if (name === "capabilities") {
      try {
        capabilities = assertStorageBackendCapabilities(projectedValue);
      } catch {
        fail(code);
      }
      projectedValue = capabilities;
    }
    objectDefineProperty(projection, name, {
      enumerable: true,
      value: projectedValue,
    });
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
  return exactFrozenRecord({
    backendId: backend.backendId,
    capabilities,
    methods: exactFrozenRecord(methods),
    receiver: value,
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

function canonicalTarget(value, code) {
  const target = exactDataObject(value, TARGET_KEYS, code);
  ensure(
    target.kind === "attachment" &&
      typeof target.attachmentId === "string" &&
      regexpTest(OPAQUE_ID_PATTERN, target.attachmentId),
    code,
  );
  return exactFrozenRecord({
    attachmentId: target.attachmentId,
    kind: "attachment",
  });
}

function normalizeRequest(value, kind, backend) {
  const code = "invalid_postgres_writer_detach_composition_request";
  const request = exactDataObject(value, REQUEST_KEYS, code);
  ensure(
    typeof request.operationId === "string" &&
      regexpTest(OPAQUE_ID_PATTERN, request.operationId),
    code,
  );
  const target = canonicalTarget(request.target, code);
  let expectedSession;
  try {
    expectedSession = assertSessionAuthoritySnapshot(request.expectedSession);
  } catch {
    fail(code);
  }
  const document = expectedSession.document;
  ensure(
    document.activeOperation === null &&
      document.storageRef.backendId === backend.backendId &&
      sameCapabilities(
        document.backendCapabilities,
        backend.capabilities,
        code,
      ),
    code,
  );
  if (kind === WRITER_RELEASE_OPERATION_KIND) {
    ensure(
      document.lifecycle === "ATTACHED" &&
        document.launch === null &&
        document.lease !== null &&
        document.attachment !== null &&
        document.attachment.attachmentId === target.attachmentId,
      code,
    );
  } else {
    ensure(
      (document.lifecycle === "ATTACHED" ||
        document.lifecycle === "BLOCKED") &&
        document.lease !== null &&
        (document.attachment === null ||
          document.attachment.attachmentId === target.attachmentId),
      code,
    );
  }
  let binding;
  try {
    binding = assertSessionOperationBinding({
      expectedSession,
      kind,
      operationId: request.operationId,
      request: {
        contractVersion: 1,
        target,
      },
    });
  } catch {
    fail(code);
  }
  return exactFrozenRecord({
    expectedSession: binding.expectedSession,
    kind,
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
      descriptor = objectGetOwnPropertyDescriptor(
        promisePrototype,
        "constructor",
      );
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
    return exactFrozenRecord({
      accepted,
      promise: protectPromise(value),
    });
  } catch {
    if (accepted && nativePromiseConstructorIsSafe(value)) {
      return exactFrozenRecord({ accepted, promise: value });
    }
    if (accepted && nativePromiseConstructorIsProtected(value)) {
      try {
        const normalized = callIntrinsic(promiseThenIntrinsic, value, [
          undefined,
          undefined,
        ]);
        return exactFrozenRecord({
          accepted,
          promise: protectPromise(normalized),
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
        "postgres_writer_detach_composition_outcome_uncertain",
      );
    } catch {
      throw INVOCATION_UNCERTAIN;
    }
  }
  return value;
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
  let operation;
  let reservation;
  let session;
  const cloneBudget = { entries: 0, nodes: 0 };
  try {
    operation =
      rawOperation === null
        ? null
        : frozenDataClone(rawOperation, code, cloneBudget);
    reservation =
      rawReservation === null
        ? null
        : frozenDataClone(rawReservation, code, cloneBudget);
    const sessionCandidate = frozenDataClone(
      rawSession,
      code,
      cloneBudget,
    );
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
  if (operation === null || reservation === null) {
    let expectedBinding;
    let observedBinding;
    try {
      expectedBinding = assertSessionOperationBinding(base);
      observedBinding = assertSessionOperationBinding({
        ...base,
        expectedSession: session,
      });
    } catch {
      fail(code);
    }
    ensure(
      observedBinding.requestSha256 === expectedBinding.requestSha256,
      code,
    );
    return exactFrozenRecord({ operation, reservation, session });
  }
  let expectedBinding;
  try {
    expectedBinding = assertSessionOperationBinding(base);
  } catch {
    fail(code);
  }
  ensure(
    operation.operationId === expectedBinding.operationId &&
      operation.kind === expectedBinding.kind &&
      operation.sessionId === expectedBinding.expectedSession.sessionId &&
      operation.requestSha256 === expectedBinding.requestSha256 &&
      reservation.reservationId === expectedBinding.reservationId,
    code,
  );
  return exactFrozenRecord({ operation, reservation, session });
}

function receiptState(receipt, base, code) {
  const core = receiptCore(receipt, base, code);
  if (core.operation === null) return exactFrozenRecord({ ...core, state: "absent" });
  const state = ownDataValue(core.operation, "state", code);
  ensure(
    arrayIncludes(["prepared", "starting", "uncertain", "committed"], state),
    code,
  );
  return exactFrozenRecord({ ...core, state });
}

function terminalResult(receipt, base, expectedOutcome = undefined) {
  const code = "postgres_writer_detach_composition_outcome_uncertain";
  const core = receiptCore(receipt, base, code);
  ensure(core.operation !== null && core.reservation !== null, code);
  ensure(
    ownDataValue(core.operation, "state", code) === "committed" &&
      ownDataValue(core.reservation, "state", code) === "released",
    code,
  );
  const result = ownDataValue(core.operation, "result", code);
  ensure(
    result !== null &&
      typeof result === "object" &&
      objectIsFrozen(result),
    code,
  );
  const outcome = ownDataValue(result, "outcome", code);
  const revision = ownDataValue(core.operation, "revision", code);
  ensure(
    arrayIncludes(TERMINAL_OUTCOMES, outcome) &&
      ownDataValue(result, "resultVersion", code) === 1 &&
      (base.kind === WRITER_RELEASE_OPERATION_KIND
        ? outcome === "writer-released" || outcome === "writer-blocked"
        : outcome === "writer-fenced" || outcome === "writer-blocked") &&
      (outcome === "writer-blocked"
        ? revision === "3"
        : revision === "2" || revision === "3"),
    code,
  );
  if (outcome === "writer-blocked") {
    const reason = ownDataValue(result, "reason", code);
    ensure(
      reason === "provider-outcome-unresolved" ||
        (base.kind === WRITER_FORCE_FENCE_OPERATION_KIND &&
          reason === "fence-unavailable"),
      code,
    );
  }
  if (expectedOutcome !== undefined) ensure(outcome === expectedOutcome, code);
  const lifecycle = ownDataValue(
    ownDataValue(core.session, "document", code),
    "lifecycle",
    code,
  );
  ensure(
    outcome === "writer-blocked"
      ? lifecycle === "BLOCKED"
      : lifecycle === "DETACHED",
    code,
  );
  return exactFrozenRecord({
    operation: core.operation,
    reservation: core.reservation,
    session: core.session,
  });
}

function operationRevision(receipt, base, code) {
  const core = receiptCore(receipt, base, code);
  ensure(core.operation !== null, code);
  const revision = ownDataValue(core.operation, "revision", code);
  ensure(revision === "1" || revision === "2", code);
  return revision;
}

async function reconcile(authority, base) {
  return receiptState(
    await protectPromise(
      invokeAsync(authority, "reconcileOperation", [base]),
    ),
    base,
    "postgres_writer_detach_composition_outcome_uncertain",
  );
}

async function reserveOrReconcile(authority, base) {
  try {
    return receiptState(
      await protectPromise(
        invokeAsync(authority, "reserveOperation", [base]),
      ),
      base,
      "postgres_writer_detach_composition_outcome_uncertain",
    );
  } catch {
    let read = await protectPromise(reconcile(authority, base));
    if (read.state !== "absent") return read;
    try {
      return receiptState(
        await protectPromise(
          invokeAsync(authority, "reserveOperation", [base]),
        ),
        base,
        "postgres_writer_detach_composition_outcome_uncertain",
      );
    } catch {
      read = await protectPromise(reconcile(authority, base));
      ensure(
        read.state !== "absent",
        "postgres_writer_detach_composition_outcome_uncertain",
      );
      return read;
    }
  }
}

async function claimOrReconcile(authority, base, method) {
  const code = "postgres_writer_detach_composition_outcome_uncertain";
  const transition = exactFrozenRecord({
    ...base,
    expectedOperationRevision: "0",
  });
  const invokeClaim = async () => {
    const claim = await protectPromise(
      invokeAsync(authority, method, [transition]),
    );
    receiptState(claim, base, code);
    return claim;
  };
  try {
    return await protectPromise(invokeClaim());
  } catch {
    let read = await protectPromise(reconcile(authority, base));
    if (read.state !== "prepared") return read;
    try {
      return await protectPromise(invokeClaim());
    } catch {
      read = await protectPromise(reconcile(authority, base));
      ensure(
        read.state !== "prepared" && read.state !== "absent",
        code,
      );
      return read;
    }
  }
}

async function markUncertain(authority, base, receipt) {
  const code = "postgres_writer_detach_composition_outcome_uncertain";
  let observed = receiptState(receipt, base, code);
  if (observed.state !== "starting") return observed;
  const transition = exactFrozenRecord({
    ...base,
    expectedOperationRevision: "1",
  });
  for (let attempt = 0; attempt < 2; attempt += 1) {
    try {
      return receiptState(
        await protectPromise(
          invokeAsync(authority, "markOperationUncertain", [transition]),
        ),
        base,
        code,
      );
    } catch {
      observed = await protectPromise(reconcile(authority, base));
      if (observed.state !== "starting") return observed;
    }
  }
  fail(code);
}

async function finalizeBlocked(authority, base, receipt, reason) {
  const code = "postgres_writer_detach_composition_outcome_uncertain";
  let observed = await protectPromise(
    markUncertain(authority, base, receipt),
  );
  if (observed.state === "committed") return terminalResult(observed, base);
  ensure(observed.state === "uncertain", code);
  const transition = exactFrozenRecord({
    ...base,
    expectedOperationRevision: "2",
    reason,
  });
  for (let attempt = 0; attempt < 2; attempt += 1) {
    try {
      const finalized = await protectPromise(
        invokeAsync(
          authority,
          "finalizeWriterOperationBlocked",
          [transition],
        ),
      );
      return terminalResult(finalized, base);
    } catch {
      observed = await protectPromise(reconcile(authority, base));
      if (observed.state === "committed") {
        return terminalResult(observed, base);
      }
      ensure(observed.state === "uncertain", code);
    }
  }
  fail(code);
}

async function finalizeSuccess(
  authority,
  base,
  receipt,
  mode,
  providerRequest,
  proof,
) {
  const code = "postgres_writer_detach_composition_outcome_uncertain";
  const method =
    mode === "release"
      ? "finalizeWriterRelease"
      : "finalizeWriterForceFence";
  const proofKey = mode === "release" ? "mutationResult" : "fenceResult";
  let observed = receiptState(receipt, base, code);
  for (let attempt = 0; attempt < 2; attempt += 1) {
    if (observed.state === "committed") {
      return terminalSuccessResult(
        observed,
        base,
        mode,
        providerRequest,
        proof,
      );
    }
    ensure(
      observed.state === "starting" || observed.state === "uncertain",
      code,
    );
    const transition = exactFrozenRecord({
      ...base,
      expectedOperationRevision: operationRevision(observed, base, code),
      [proofKey]: proof,
    });
    try {
      return terminalSuccessResult(
        await protectPromise(invokeAsync(authority, method, [transition])),
        base,
        mode,
        providerRequest,
        proof,
      );
    } catch {
      observed = await protectPromise(reconcile(authority, base));
    }
  }
  if (observed.state === "committed") {
    return terminalSuccessResult(
      observed,
      base,
      mode,
      providerRequest,
      proof,
    );
  }
  fail(code);
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
      (result) =>
        exactFrozenRecord({
          fulfilled: true,
          value: result,
        }),
      () =>
        exactFrozenRecord({
          fulfilled: false,
          value: undefined,
        }),
    ]),
  );
}

function settleGuardPromise(value, markSettled) {
  return protectPromise(
    callIntrinsic(promiseThenIntrinsic, value, [
      (result) => {
        markSettled();
        return exactFrozenRecord({
          fulfilled: true,
          value: result,
        });
      },
      () => {
        markSettled();
        return exactFrozenRecord({
          fulfilled: false,
          value: undefined,
        });
      },
    ]),
  );
}

async function runGuarded(operationGuard, operationId, callback) {
  const code = "postgres_writer_detach_composition_outcome_uncertain";
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
    callbackCompletion = protectPromise(
      runCallback(probeValue, completeValue),
    );
    callbackSettlement = settleProtectedPromise(callbackCompletion);
    return callbackCompletion;
  };
  let guardCompletion = null;
  let guardPromiseAccepted = false;
  try {
    const pending = callIntrinsic(
      operationGuard.methods.runExclusive,
      operationGuard.receiver,
      [
        operationId,
        guardedCallback,
      ],
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

function validateProviderRequest(value, base, backend, mode, claim) {
  const code = "postgres_writer_detach_composition_outcome_uncertain";
  let request;
  try {
    request =
      mode === "release"
        ? assertStorageMutationRequest(value)
        : assertStorageForceFenceRequest(value);
  } catch {
    fail(code);
  }
  ensure(
    request.backendId === backend.backendId &&
      request.contractVersion === 1 &&
      request.operationId === base.operationId &&
      request.sessionId === base.expectedSession.sessionId &&
      request.storageId === base.expectedSession.document.storageRef.storageId &&
      request.target.attachmentId === base.request.target.attachmentId &&
      request.target.kind === "attachment" &&
      (mode === "release"
        ? request.operation === "detach" &&
          request.leaseId === base.expectedSession.document.lease.leaseId &&
          request.holderId === base.expectedSession.document.lease.holderId &&
          request.fencingEpoch ===
            base.expectedSession.document.lease.fencingEpoch
        : request.revokedFence.leaseId ===
            base.expectedSession.document.lease.leaseId &&
          request.revokedFence.holderId ===
            base.expectedSession.document.lease.holderId &&
          request.revokedFence.fencingEpoch ===
            base.expectedSession.document.lease.fencingEpoch &&
          request.fencingEpoch ===
            callIntrinsic(
              bigIntToStringIntrinsic,
              BigIntConstructor(
                base.expectedSession.document.writerEpoch,
              ) + 1n,
              [],
            ) &&
          ownDataValue(claim, "writerEpoch", code) ===
            request.fencingEpoch),
    code,
  );
  return request;
}

function validateProviderResult(value, request, mode) {
  const code = "postgres_writer_detach_composition_outcome_uncertain";
  try {
    return mode === "release"
      ? assertStorageMutationResult(value, { request })
      : assertStorageForceFenceResult(value, { request });
  } catch {
    fail(code);
  }
}

function terminalSuccessResult(
  receipt,
  base,
  mode,
  providerRequest,
  proof,
) {
  const code = "postgres_writer_detach_composition_outcome_uncertain";
  const expectedOutcome =
    mode === "release" ? "writer-released" : "writer-fenced";
  const proofKey = mode === "release" ? "mutationResult" : "fenceResult";
  const terminal = terminalResult(receipt, base, expectedOutcome);
  const result = ownDataValue(terminal.operation, "result", code);
  const storedProof = ownDataValue(result, proofKey, code);
  const expectedProof = validateProviderResult(
    proof,
    providerRequest,
    mode,
  );
  const actualProof = validateProviderResult(
    storedProof,
    providerRequest,
    mode,
  );
  ensure(
    ownDataValue(actualProof, "proofId", code) ===
      ownDataValue(expectedProof, "proofId", code),
    code,
  );
  return terminal;
}

async function executeDetach(
  authority,
  operationGuard,
  backend,
  mode,
  request,
) {
  const kind =
    mode === "release"
      ? WRITER_RELEASE_OPERATION_KIND
      : WRITER_FORCE_FENCE_OPERATION_KIND;
  const base = normalizeRequest(request, kind, backend);
  const blockedReason =
    mode === "force" && backend.capabilities.fencing === "manual"
      ? "fence-unavailable"
      : "provider-outcome-unresolved";
  return await protectPromise(
    runGuarded(operationGuard, base.operationId, async (assertHeld) => {
    try {
      await protectPromise(assertGuardHeld(assertHeld));
    } catch {
      fail("postgres_writer_detach_composition_outcome_uncertain");
    }
    let observed = await protectPromise(reserveOrReconcile(authority, base));
    if (observed.state === "committed") return terminalResult(observed, base);
    if (observed.state === "starting" || observed.state === "uncertain") {
      return await protectPromise(
        finalizeBlocked(authority, base, observed, blockedReason),
      );
    }
    ensure(
      observed.state === "prepared",
      "postgres_writer_detach_composition_outcome_uncertain",
    );
    const claimMethod =
      mode === "release"
        ? "claimWriterReleaseDispatch"
        : "claimWriterForceFenceDispatch";
    const claim = await protectPromise(
      claimOrReconcile(authority, base, claimMethod),
    );
    observed = receiptState(
      claim,
      base,
      "postgres_writer_detach_composition_outcome_uncertain",
    );
    if (observed.state === "committed") return terminalResult(observed, base);
    let dispatchGranted = false;
    try {
      dispatchGranted =
        ownDataValue(
          claim,
          "dispatchGranted",
          "postgres_writer_detach_composition_outcome_uncertain",
        ) === true;
    } catch {
      dispatchGranted = false;
    }
    if (!dispatchGranted) {
      ensure(
        observed.state === "starting" || observed.state === "uncertain",
        "postgres_writer_detach_composition_outcome_uncertain",
      );
      return await protectPromise(
        finalizeBlocked(authority, base, observed, blockedReason),
      );
    }
    ensure(
      observed.state === "starting",
      "postgres_writer_detach_composition_outcome_uncertain",
    );
    if (blockedReason === "fence-unavailable") {
      return await protectPromise(
        finalizeBlocked(authority, base, observed, blockedReason),
      );
    }

    const requestKey = mode === "release" ? "mutationRequest" : "fenceRequest";
    let providerRequest;
    try {
      providerRequest = validateProviderRequest(
        ownDataValue(
          claim,
          requestKey,
          "postgres_writer_detach_composition_outcome_uncertain",
        ),
        base,
        backend,
        mode,
        claim,
      );
      await protectPromise(assertGuardHeld(assertHeld));
    } catch {
      return await protectPromise(
        finalizeBlocked(
          authority,
          base,
          observed,
          "provider-outcome-unresolved",
        ),
      );
    }

    let proof;
    try {
      const raw = await protectPromise(
        invokeProvider(
          backend,
          mode === "release" ? "detachAttachment" : "forceFence",
          providerRequest,
        ),
      );
      proof = validateProviderResult(raw, providerRequest, mode);
      await protectPromise(assertGuardHeld(assertHeld));
      await protectPromise(assertGuardHeld(assertHeld));
    } catch {
      return await protectPromise(
        finalizeBlocked(
          authority,
          base,
          observed,
          "provider-outcome-unresolved",
        ),
      );
    }
    return await protectPromise(
      finalizeSuccess(
        authority,
        base,
        observed,
        mode,
        providerRequest,
        proof,
      ),
    );
    }),
  );
}

async function invokeDetachMethod(
  authority,
  operationGuard,
  backend,
  mode,
  methodArgs,
) {
  ensure(
    methodArgs.length === 1,
    "invalid_postgres_writer_detach_composition_request",
  );
  return await protectPromise(
    executeDetach(
      authority,
      operationGuard,
      backend,
      mode,
      methodArgs[0],
    ),
  );
}

export function createPostgresWriterDetachComposition(...args) {
  const optionCode = "invalid_postgres_writer_detach_composition_options";
  ensure(args.length === 1, optionCode);
  const options = exactDataObject(args[0], OPTION_KEYS, optionCode);
  const authority = collaboratorBinding(
    options.authority,
    AUTHORITY_METHODS,
    optionCode,
  );
  const operationGuard = operationGuardBinding(
    options.operationGuard,
    optionCode,
  );
  const backend = storageBackendBinding(options.storageBackend, optionCode);

  const detachWriter = function detachWriter(...methodArgs) {
    return protectPromise(
      invokeDetachMethod(
        authority,
        operationGuard,
        backend,
        "release",
        methodArgs,
      ),
    );
  };
  const forceFenceWriter = function forceFenceWriter(...methodArgs) {
    return protectPromise(
      invokeDetachMethod(
        authority,
        operationGuard,
        backend,
        "force",
        methodArgs,
      ),
    );
  };
  objectFreeze(detachWriter);
  objectFreeze(forceFenceWriter);
  const facade = exactFrozenRecord({ detachWriter, forceFenceWriter });
  weakSetAdd(facades, facade);
  return facade;
}

export function isPostgresWriterDetachComposition(value) {
  return (
    value !== null &&
    (typeof value === "object" || typeof value === "function") &&
    !isProxyValue(value) &&
    weakSetHas(facades, value)
  );
}

objectFreeze(PostgresWriterDetachCompositionError.prototype);
objectFreeze(PostgresWriterDetachCompositionError);
