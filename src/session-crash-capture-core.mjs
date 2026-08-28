import { types as utilTypes } from "node:util";

import {
  SessionStorageContractError,
  assertAtomicCrashCaptureRequest,
  assertAtomicCrashCaptureResult,
  assertAtomicCrashCaptureVerificationResult,
  createAtomicCrashCaptureBackendFacade,
} from "./session-storage-contracts.mjs";

const arrayEveryIntrinsic = Array.prototype.every;
const arrayIncludesIntrinsic = Array.prototype.includes;
const arrayIsArray = Array.isArray;
const objectCreate = Object.create;
const objectFreeze = Object.freeze;
const objectGetOwnPropertyDescriptor = Object.getOwnPropertyDescriptor;
const objectGetPrototypeOf = Object.getPrototypeOf;
const objectHasOwn = Object.hasOwn;
const objectIsFrozen = Object.isFrozen;
const objectPrototype = Object.prototype;
const reflectApply = Reflect.apply;
const reflectOwnKeys = Reflect.ownKeys;
const { isProxy: isProxyValue } = utilTypes;
const weakMapGetIntrinsic = WeakMap.prototype.get;
const weakMapHasIntrinsic = WeakMap.prototype.has;
const weakMapSetIntrinsic = WeakMap.prototype.set;

const CORE_ERROR_MESSAGES = objectFreeze({
  atomic_crash_capture_outcome_uncertain:
    "Atomic crash-capture outcome is uncertain",
  atomic_crash_capture_verification_outcome_uncertain:
    "Atomic crash-capture verification outcome is uncertain",
});

function weakMapGet(value, key) {
  return reflectApply(weakMapGetIntrinsic, value, [key]);
}

function weakMapHas(value, key) {
  return reflectApply(weakMapHasIntrinsic, value, [key]);
}

function weakMapSet(value, key, entry) {
  reflectApply(weakMapSetIntrinsic, value, [key, entry]);
}

function arrayEvery(value, callback) {
  return reflectApply(arrayEveryIntrinsic, value, [callback]);
}

function arrayIncludes(value, candidate) {
  return reflectApply(arrayIncludesIntrinsic, value, [candidate]);
}

export class SessionCrashCaptureCoreError extends Error {
  constructor(code) {
    if (!objectHasOwn(CORE_ERROR_MESSAGES, code)) {
      throw new TypeError("unsupported session crash-capture core error code");
    }
    super(CORE_ERROR_MESSAGES[code]);
    this.name = "SessionCrashCaptureCoreError";
    this.code = code;
    this.retryable = false;
    objectFreeze(this);
  }
}

function failContract(code, message) {
  throw new SessionStorageContractError(code, message);
}

function ensureContract(condition, code, message) {
  if (!condition) failContract(code, message);
}

function assertExactOptions(value, keys, label) {
  if (
    isProxyValue(value) ||
    value === null ||
    typeof value !== "object" ||
    arrayIsArray(value)
  ) {
    failContract(
      "invalid_atomic_crash_capture",
      `${label} must be a plain object`,
    );
  }
  let prototype;
  let actual;
  try {
    prototype = objectGetPrototypeOf(value);
    actual = reflectOwnKeys(value);
  } catch {
    failContract(
      "invalid_atomic_crash_capture",
      `${label} must be a plain object`,
    );
  }
  ensureContract(
    arrayIncludes([objectPrototype, null], prototype),
    "invalid_atomic_crash_capture",
    `${label} must be a plain object`,
  );
  ensureContract(
    actual.length === keys.length &&
      arrayEvery(
        actual,
        (key) => typeof key === "string" && arrayIncludes(keys, key),
      ),
    "invalid_atomic_crash_capture",
    `${label} contains unexpected or missing fields`,
  );
  const normalized = objectCreate(null);
  for (let index = 0; index < actual.length; index += 1) {
    const key = actual[index];
    let descriptor;
    try {
      descriptor = objectGetOwnPropertyDescriptor(value, key);
    } catch {
      failContract(
        "invalid_atomic_crash_capture",
        `${label} fields must be enumerable plain data properties`,
      );
    }
    ensureContract(
      descriptor?.enumerable === true && objectHasOwn(descriptor, "value"),
      "invalid_atomic_crash_capture",
      `${label} fields must be enumerable plain data properties`,
    );
    normalized[key] = descriptor.value;
  }
  return normalized;
}

function validateContract(operation, code, message) {
  try {
    return operation();
  } catch (error) {
    let isContractError = false;
    try {
      isContractError = error instanceof SessionStorageContractError;
    } catch {
      // Hostile thrown values are normalized below.
    }
    if (isContractError) throw error;
    failContract(code, message);
  }
}

function validateExternalOperation(operation, code, message) {
  try {
    return operation();
  } catch {
    failContract(code, message);
  }
}

function checkedBackend(value) {
  return validateExternalOperation(
    () => createAtomicCrashCaptureBackendFacade(value),
    "invalid_storage_backend",
    "atomic crash-capture backend is invalid",
  );
}

function checkedBackendMethod(backend, method) {
  return validateExternalOperation(
    () => {
      const operation = backend[method];
      ensureContract(
        typeof operation === "function",
        "invalid_storage_backend",
        "atomic crash-capture backend operation is invalid",
      );
      return operation;
    },
    "invalid_storage_backend",
    "atomic crash-capture backend operation is invalid",
  );
}

function assertCaptureAuthority(value) {
  ensureContract(
    value !== null &&
      typeof value === "object" &&
      !isProxyValue(value) &&
      !arrayIsArray(value),
    "invalid_atomic_crash_capture",
    "capture authority must be an opaque non-proxy object handle",
  );
  return value;
}

function assertBackendMatchesRequest(backend, request) {
  ensureContract(
    backend.backendId === request.storageRef.backendId,
    "invalid_storage_backend",
    "atomic crash-capture backend does not match canonical storage",
  );
}

const preparedAtomicCrashCaptures = new WeakMap();

/**
 * Freezes one exact provider request and captures the provider methods before
 * any stopped-or-fenced authority is presented. The returned object is a
 * same-process one-use token, not durable dispatch authority.
 */
export function prepareAtomicCrashCapture(options) {
  const { backend, request } = assertExactOptions(
    options,
    ["backend", "request"],
    "atomic crash-capture preparation options",
  );
  const normalizedRequest = validateContract(
    () => assertAtomicCrashCaptureRequest(request),
    "invalid_atomic_crash_capture",
    "atomic crash-capture request is invalid",
  );
  const physicalBackend = checkedBackend(backend);
  assertBackendMatchesRequest(physicalBackend, normalizedRequest);
  const capture = checkedBackendMethod(
    physicalBackend,
    "captureAtomicCrashCheckpoint",
  );
  const preparedCapture = objectFreeze({
    backendId: physicalBackend.backendId,
    request: normalizedRequest,
  });
  weakMapSet(preparedAtomicCrashCaptures, preparedCapture, {
    backend: physicalBackend,
    capture,
    state: "prepared",
  });
  return preparedCapture;
}

/**
 * Consumes one same-process token before invoking the provider. A rejection,
 * malformed result, or acknowledgement loss cannot reopen that token.
 */
export async function capturePreparedAtomicCrashCheckpoint(options) {
  const { captureAuthority, preparedCapture } = assertExactOptions(
    options,
    ["captureAuthority", "preparedCapture"],
    "prepared atomic crash-capture options",
  );
  const authority = assertCaptureAuthority(captureAuthority);
  ensureContract(
    preparedCapture !== null &&
      typeof preparedCapture === "object" &&
      !isProxyValue(preparedCapture) &&
      !arrayIsArray(preparedCapture) &&
      objectIsFrozen(preparedCapture) &&
      weakMapHas(preparedAtomicCrashCaptures, preparedCapture),
    "invalid_atomic_crash_capture",
    "prepared atomic crash-capture token is invalid",
  );
  const preparedState = weakMapGet(
    preparedAtomicCrashCaptures,
    preparedCapture,
  );
  ensureContract(
    preparedState.state === "prepared",
    "invalid_atomic_crash_capture",
    "prepared atomic crash-capture token was already dispatched",
  );
  preparedState.state = "dispatched";
  const { backend, capture } = preparedState;
  const { request } = preparedCapture;
  try {
    const result = await reflectApply(capture, backend, [
      objectFreeze({ captureAuthority: authority, request }),
    ]);
    return assertAtomicCrashCaptureResult(result, { request });
  } catch {
    throw new SessionCrashCaptureCoreError(
      "atomic_crash_capture_outcome_uncertain",
    );
  }
}

/**
 * Performs only source-free committed-result verification. `unknown` remains
 * a durable blocker and this path never consumes or reconstructs capture
 * authority and never authorizes another provider dispatch.
 */
export async function verifyCommittedAtomicCrashCapture(options) {
  const { backend, request } = assertExactOptions(
    options,
    ["backend", "request"],
    "atomic crash-capture verification options",
  );
  const normalizedRequest = validateContract(
    () => assertAtomicCrashCaptureRequest(request),
    "invalid_atomic_crash_capture",
    "atomic crash-capture request is invalid",
  );
  const physicalBackend = checkedBackend(backend);
  assertBackendMatchesRequest(physicalBackend, normalizedRequest);
  const verify = checkedBackendMethod(
    physicalBackend,
    "verifyCommittedAtomicCrashCheckpoint",
  );
  try {
    const result = await reflectApply(verify, physicalBackend, [
      normalizedRequest,
    ]);
    return assertAtomicCrashCaptureVerificationResult(result, {
      request: normalizedRequest,
    });
  } catch {
    throw new SessionCrashCaptureCoreError(
      "atomic_crash_capture_verification_outcome_uncertain",
    );
  }
}
