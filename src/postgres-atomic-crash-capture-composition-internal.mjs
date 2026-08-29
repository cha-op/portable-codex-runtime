import { types as utilTypes } from "node:util";

import {
  createLvmAtomicCrashCaptureProvider,
} from "./lvm-atomic-crash-capture-provider.mjs";
import {
  capturePreparedAtomicCrashCheckpoint,
  prepareAtomicCrashCapture,
  verifyCommittedAtomicCrashCapture,
} from "./session-crash-capture-core.mjs";
import {
  assertAtomicCrashCaptureRequest,
  assertAtomicCrashCaptureResult,
} from "./session-storage-contracts.mjs";

// Capture intrinsics before any collaborator receives a stopped-writer
// authority. The composition protects authority identity and exact request
// content; the launcher facet owns the writer/coordinator binding itself.
const arrayEveryIntrinsic = Array.prototype.every;
const arrayIncludesIntrinsic = Array.prototype.includes;
const arrayIsArray = Array.isArray;
const arraySomeIntrinsic = Array.prototype.some;
const MapConstructor = Map;
const mapDeleteIntrinsic = Map.prototype.delete;
const mapGetIntrinsic = Map.prototype.get;
const mapHasIntrinsic = Map.prototype.has;
const mapSetIntrinsic = Map.prototype.set;
const objectAssign = Object.assign;
const objectCreate = Object.create;
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
const promiseSpeciesSymbol = Symbol.species;
const promiseThenIntrinsic = promisePrototype.then;
const reflectApply = Reflect.apply;
const reflectOwnKeys = Reflect.ownKeys;
const weakMapGetIntrinsic = WeakMap.prototype.get;
const weakMapDeleteIntrinsic = WeakMap.prototype.delete;
const weakMapSetIntrinsic = WeakMap.prototype.set;
const WeakMapConstructor = WeakMap;
const {
  isGeneratorObject: isGeneratorObjectValue,
  isPromise: isPromiseValue,
  isProxy: isProxyValue,
} = utilTypes;

const ERROR_MESSAGES = objectFreeze({
  invalid_postgres_lvm_atomic_crash_capture_composition_options:
    "PostgreSQL LVM atomic crash-capture composition options are invalid",
  invalid_postgres_lvm_atomic_crash_capture_composition_request:
    "PostgreSQL LVM atomic crash-capture composition request is invalid",
  postgres_lvm_atomic_crash_capture_composition_outcome_uncertain:
    "PostgreSQL LVM atomic crash-capture composition outcome is uncertain",
  postgres_lvm_atomic_crash_capture_retirement_outcome_uncertain:
    "PostgreSQL LVM atomic crash-capture retirement outcome is uncertain",
});

const COMPOSITION_OPTION_KEYS = objectFreeze([
  "baseBackend",
  "catalogue",
  "driver",
  "launcher",
]);
const RUN_KEYS = objectFreeze(["request"]);
const AUTHORITY_ADMISSION_KEYS = objectFreeze([
  "captureAuthority",
  "request",
]);
const RETIREMENT_KEYS = objectFreeze([
  "captureAuthority",
  "request",
  "result",
]);

export class PostgresLvmAtomicCrashCaptureCompositionError extends Error {
  constructor(code) {
    if (!objectHasOwn(ERROR_MESSAGES, code)) {
      throw new TypeError(
        "Unsupported PostgreSQL LVM atomic crash-capture composition error",
      );
    }
    super(ERROR_MESSAGES[code]);
    this.name = "PostgresLvmAtomicCrashCaptureCompositionError";
    this.code = code;
    this.retryable = false;
    objectFreeze(this);
  }
}

function fail(code) {
  throw new PostgresLvmAtomicCrashCaptureCompositionError(code);
}

function ensure(condition, code) {
  if (!condition) fail(code);
}

function arrayEvery(value, callback) {
  return reflectApply(arrayEveryIntrinsic, value, [callback]);
}

function arrayIncludes(value, candidate) {
  return reflectApply(arrayIncludesIntrinsic, value, [candidate]);
}

function arraySome(value, callback) {
  return reflectApply(arraySomeIntrinsic, value, [callback]);
}

function mapGet(value, key) {
  return reflectApply(mapGetIntrinsic, value, [key]);
}

function mapDelete(value, key) {
  return reflectApply(mapDeleteIntrinsic, value, [key]);
}

function mapHas(value, key) {
  return reflectApply(mapHasIntrinsic, value, [key]);
}

function mapSet(value, key, entry) {
  reflectApply(mapSetIntrinsic, value, [key, entry]);
}

function weakMapGet(value, key) {
  return reflectApply(weakMapGetIntrinsic, value, [key]);
}

function weakMapDelete(value, key) {
  return reflectApply(weakMapDeleteIntrinsic, value, [key]);
}

function weakMapSet(value, key, entry) {
  reflectApply(weakMapSetIntrinsic, value, [key, entry]);
}

function inspectExactDataObject(value, keys, code) {
  ensure(
    value !== null &&
      typeof value === "object" &&
      !isProxyValue(value) &&
      !arrayIsArray(value),
    code,
  );
  let prototype;
  let actualKeys;
  try {
    prototype = objectGetPrototypeOf(value);
    actualKeys = reflectOwnKeys(value);
  } catch {
    fail(code);
  }
  ensure(
    (prototype === objectPrototype || prototype === null) &&
      actualKeys.length === keys.length &&
      arrayEvery(
        actualKeys,
        (key) => typeof key === "string" && arrayIncludes(keys, key),
      ),
    code,
  );
  const normalized = objectCreate(null);
  for (let index = 0; index < actualKeys.length; index += 1) {
    const key = actualKeys[index];
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
    normalized[key] = descriptor.value;
  }
  return normalized;
}

function exactFrozenRecord(values) {
  return objectFreeze(reflectApply(objectAssign, undefined, [
    objectCreate(null),
    values,
  ]));
}

function collaboratorMethod(value, name, code) {
  ensure(
    value !== null &&
      typeof value === "object" &&
      !isProxyValue(value) &&
      objectIsFrozen(value),
    code,
  );
  let descriptor;
  try {
    descriptor = objectGetOwnPropertyDescriptor(value, name);
  } catch {
    fail(code);
  }
  ensure(
    descriptor?.enumerable === true &&
      objectHasOwn(descriptor, "value") &&
      typeof descriptor.value === "function" &&
      !isProxyValue(descriptor.value),
    code,
  );
  return descriptor.value;
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

function normalizeSafeNativePromise(value) {
  if (
    !isPromiseValue(value) ||
    isProxyValue(value) ||
    isGeneratorObjectValue(value)
  ) {
    return null;
  }
  let prototype;
  let descriptor;
  try {
    prototype = objectGetPrototypeOf(value);
    descriptor = objectGetOwnPropertyDescriptor(value, "constructor");
    if (descriptor === undefined) {
      descriptor = objectGetOwnPropertyDescriptor(
        promisePrototype,
        "constructor",
      );
    }
  } catch {
    return null;
  }
  if (
    prototype !== promisePrototype ||
    descriptor === undefined ||
    !objectHasOwn(descriptor, "value")
  ) {
    return null;
  }
  if (descriptor.value === PromiseConstructor) return value;
  if (!isSafePromiseSpeciesHolder(descriptor.value)) return null;

  try {
    const normalized = reflectApply(promiseThenIntrinsic, value, [
      undefined,
      undefined,
    ]);
    objectDefineProperty(normalized, "constructor", {
      configurable: false,
      enumerable: false,
      value: PromiseConstructor,
      writable: false,
    });
    return normalized;
  } catch {
    return null;
  }
}

const absorbRetirementRejection = objectFreeze(
  function absorbRetirementRejection() {},
);

function drainRetirementPromise(value) {
  const pending = normalizeSafeNativePromise(value);
  if (pending === null) return;
  try {
    reflectApply(promiseThenIntrinsic, pending, [
      undefined,
      absorbRetirementRejection,
    ]);
  } catch {
    // The caller still receives the fixed retirement uncertainty outcome.
  }
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
    leftKeys.length !== rightKeys.length ||
    arraySome(
      leftKeys,
      (key) => typeof key !== "string" || !arrayIncludes(rightKeys, key),
    )
  ) {
    return false;
  }
  state.nodes += 1;
  for (let index = 0; index < leftKeys.length; index += 1) {
    const key = leftKeys[index];
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

function normalizeRequest(value, code) {
  try {
    return assertAtomicCrashCaptureRequest(value);
  } catch {
    fail(code);
  }
}

function normalizeResult(value, request, code) {
  try {
    return assertAtomicCrashCaptureResult(value, { request });
  } catch {
    fail(code);
  }
}

function opaqueAuthority(value, code) {
  ensure(
    value !== null &&
      typeof value === "object" &&
      !arrayIsArray(value) &&
      !isProxyValue(value) &&
      objectIsFrozen(value),
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
  ensure(prototype === null && keys.length === 0, code);
  return value;
}

/**
 * Privately assembles one PostgreSQL complete-stop authority with one classic
 * LVM atomic provider. Public lifecycle capability discovery remains outside
 * this module and continues to report atomic capture as unavailable.
 */
export function createPostgresLvmAtomicCrashCaptureCompositionInternal(
  options,
  resolveFacet,
) {
  const optionCode =
    "invalid_postgres_lvm_atomic_crash_capture_composition_options";
  const requestCode =
    "invalid_postgres_lvm_atomic_crash_capture_composition_request";
  const outcomeCode =
    "postgres_lvm_atomic_crash_capture_composition_outcome_uncertain";
  const retirementCode =
    "postgres_lvm_atomic_crash_capture_retirement_outcome_uncertain";
  ensure(
    arguments.length === 2 &&
      typeof resolveFacet === "function" &&
      !isProxyValue(resolveFacet) &&
      objectIsFrozen(resolveFacet),
    optionCode,
  );
  const normalized = inspectExactDataObject(
    options,
    COMPOSITION_OPTION_KEYS,
    optionCode,
  );

  let facet;
  try {
    facet = reflectApply(resolveFacet, undefined, [normalized.launcher]);
  } catch {
    fail(optionCode);
  }
  const completeStop = collaboratorMethod(
    facet,
    "completeStop",
    optionCode,
  );
  const resolveCaptureAuthority = collaboratorMethod(
    facet,
    "resolveCaptureAuthority",
    optionCode,
  );
  const retireCompleteStop = collaboratorMethod(
    facet,
    "retireCompleteStop",
    optionCode,
  );

  const attemptsByCaptureAttemptId = new MapConstructor();
  const attemptsByCaptureAuthority = new WeakMapConstructor();

  const authorityConsumer = async function authorityConsumer(
    admissionValue,
    runCaptureValue,
  ) {
    const admission = inspectExactDataObject(
      admissionValue,
      AUTHORITY_ADMISSION_KEYS,
      outcomeCode,
    );
    const captureAuthority = opaqueAuthority(
      admission.captureAuthority,
      outcomeCode,
    );
    const request = normalizeRequest(admission.request, outcomeCode);
    const attempt = weakMapGet(
      attemptsByCaptureAuthority,
      captureAuthority,
    );
    ensure(
      attempt !== undefined &&
        attempt.state === "dispatching" &&
        sameFrozenData(request, attempt.request),
      outcomeCode,
    );
    attempt.state = "resolving-authority";

    let pending;
    try {
      pending = reflectApply(resolveCaptureAuthority, facet, [
        exactFrozenRecord({ captureAuthority, request }),
        runCaptureValue,
      ]);
    } catch {
      attempt.state = "capture-uncertain";
      fail(outcomeCode);
    }
    pending = normalizeSafeNativePromise(pending);
    if (pending === null) {
      attempt.state = "capture-uncertain";
      fail(outcomeCode);
    }

    try {
      const result = await pending;
      ensure(attempt.state === "resolving-authority", outcomeCode);
      attempt.state = "authority-consumed";
      return result;
    } catch {
      attempt.state = "capture-uncertain";
      fail(outcomeCode);
    }
  };
  objectFreeze(authorityConsumer);

  let backend;
  try {
    backend = createLvmAtomicCrashCaptureProvider({
      authorityConsumer,
      baseBackend: normalized.baseBackend,
      catalogue: normalized.catalogue,
      driver: normalized.driver,
    });
  } catch {
    fail(optionCode);
  }

  function retireAttempt(attempt, value) {
    const result = normalizeResult(value, attempt.request, retirementCode);
    ensure(attempt.state === "committed", retirementCode);
    let retirement;
    try {
      retirement = reflectApply(retireCompleteStop, facet, [
        exactFrozenRecord({
          captureAuthority: attempt.captureAuthority,
          request: attempt.request,
          result,
        }),
      ]);
    } catch {
      attempt.state = "retirement-uncertain";
      fail(retirementCode);
    }
    if (retirement !== undefined) {
      drainRetirementPromise(retirement);
      attempt.state = "retirement-uncertain";
      fail(retirementCode);
    }
    ensure(
      mapGet(
        attemptsByCaptureAttemptId,
        attempt.request.captureAttemptId,
      ) === attempt &&
        weakMapGet(
          attemptsByCaptureAuthority,
          attempt.captureAuthority,
        ) === attempt,
      retirementCode,
    );
    mapDelete(
      attemptsByCaptureAttemptId,
      attempt.request.captureAttemptId,
    );
    weakMapDelete(
      attemptsByCaptureAuthority,
      attempt.captureAuthority,
    );
    attempt.state = "retired";
    return result;
  }

  async function verifyAndRetire(attempt) {
    ensure(attempt.state === "capture-uncertain", outcomeCode);
    attempt.state = "verifying-committed";
    let verification;
    try {
      verification = await verifyCommittedAtomicCrashCapture({
        backend,
        request: attempt.request,
      });
    } catch {
      attempt.state = "capture-uncertain";
      fail(outcomeCode);
    }
    if (verification.outcome !== "committed") {
      attempt.state = "capture-uncertain";
      fail(outcomeCode);
    }
    attempt.state = "committed";
    return retireAttempt(attempt, verification.result);
  }

  const runCapture = async function runCapture(optionsValue) {
    const input = inspectExactDataObject(
      optionsValue,
      RUN_KEYS,
      requestCode,
    );
    let preparedCapture;
    try {
      preparedCapture = prepareAtomicCrashCapture({
        backend,
        request: input.request,
      });
    } catch {
      fail(requestCode);
    }
    const request = preparedCapture.request;
    ensure(
      !mapHas(attemptsByCaptureAttemptId, request.captureAttemptId),
      outcomeCode,
    );
    const attempt = {
      captureAuthority: null,
      request,
      state: "prepared",
    };
    mapSet(
      attemptsByCaptureAttemptId,
      request.captureAttemptId,
      attempt,
    );

    attempt.state = "stopping";
    let pendingStop;
    try {
      pendingStop = reflectApply(completeStop, facet, [request]);
    } catch {
      attempt.state = "stop-uncertain";
      fail(outcomeCode);
    }
    pendingStop = normalizeSafeNativePromise(pendingStop);
    if (pendingStop === null) {
      attempt.state = "stop-uncertain";
      fail(outcomeCode);
    }
    try {
      attempt.captureAuthority = opaqueAuthority(
        await pendingStop,
        outcomeCode,
      );
    } catch {
      attempt.state = "stop-uncertain";
      fail(outcomeCode);
    }
    ensure(
      weakMapGet(
        attemptsByCaptureAuthority,
        attempt.captureAuthority,
      ) === undefined,
      outcomeCode,
    );
    weakMapSet(
      attemptsByCaptureAuthority,
      attempt.captureAuthority,
      attempt,
    );
    attempt.state = "dispatching";

    let result;
    try {
      result = await capturePreparedAtomicCrashCheckpoint({
        captureAuthority: attempt.captureAuthority,
        preparedCapture,
      });
    } catch {
      attempt.state = "capture-uncertain";
      return verifyAndRetire(attempt);
    }
    ensure(
      attempt.state === "dispatching" ||
        attempt.state === "authority-consumed",
      outcomeCode,
    );
    attempt.state = "committed";
    return retireAttempt(attempt, result);
  };

  const reconcileCapture = async function reconcileCapture(optionsValue) {
    const input = inspectExactDataObject(
      optionsValue,
      RUN_KEYS,
      requestCode,
    );
    const request = normalizeRequest(input.request, requestCode);
    const attempt = mapGet(
      attemptsByCaptureAttemptId,
      request.captureAttemptId,
    );
    ensure(
      attempt !== undefined &&
        attempt.state === "capture-uncertain" &&
        sameFrozenData(request, attempt.request),
      outcomeCode,
    );
    return verifyAndRetire(attempt);
  };

  objectFreeze(runCapture);
  objectFreeze(reconcileCapture);
  return exactFrozenRecord({ reconcileCapture, runCapture });
}

objectFreeze(PostgresLvmAtomicCrashCaptureCompositionError.prototype);
objectFreeze(PostgresLvmAtomicCrashCaptureCompositionError);
