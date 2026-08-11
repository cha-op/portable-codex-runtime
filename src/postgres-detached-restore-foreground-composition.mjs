import { types as utilTypes } from "node:util";

import {
  isPostgresDetachedRestorePlan,
} from "./postgres-detached-restore-plan.mjs";
import {
  derivePostgresLogicalWriterStopOperationId,
} from "./postgres-logical-writer-launcher.mjs";
import {
  PostgresOperationGuard,
  isPostgresOperationGuard,
} from "./postgres-operation-guard.mjs";
import {
  assertPostgresRestoreLifecycleLeaseHeld,
  haveDistinctPostgresRestoreLifecycleOperationGuardPools,
  isPostgresRestoreLifecycleGuard,
} from "./postgres-restore-lifecycle-guard.mjs";
import {
  CHECKPOINT_CAPTURE_OPERATION_KIND,
  RESTORE_ATTACHMENT_ACTIVATION_OPERATION_KIND,
  RESTORE_DESTINATION_GENERATION_OPERATION_KIND,
  WRITER_FORCE_FENCE_OPERATION_KIND,
  WRITER_LAUNCH_ATTEMPT_OPERATION_KIND,
  WRITER_LAUNCH_STOP_OPERATION_KIND,
  WRITER_LEASE_RENEW_OPERATION_KIND,
  WRITER_RELEASE_OPERATION_KIND,
  PostgresSessionAuthorityError,
  assertSessionAuthoritySnapshot,
  assertSessionOperationTransitionProof,
  createRestoreAttachmentActivationOperationRequestV2,
  createRestoreDestinationGenerationOperationRequest,
  createWriterLaunchAttemptOperationRequest,
} from "./postgres-session-authority.mjs";
import {
  assertCheckpointCaptureReconciliationBackend,
  assertCheckpointDescriptor,
  assertPreparedCheckpointCaptureBackend,
  assertStorageMutationRequest,
  assertStorageMutationResult,
} from "./session-storage-contracts.mjs";

const arrayIncludesIntrinsic = Array.prototype.includes;
const arrayIsArray = Array.isArray;
const arraySortIntrinsic = Array.prototype.sort;
const DateConstructor = Date;
const dateNowIntrinsic = Date.now;
const dateParseIntrinsic = Date.parse;
const functionApplyIntrinsic = Reflect.apply;
const haveDistinctLifecycleOperationGuardPoolsIntrinsic =
  haveDistinctPostgresRestoreLifecycleOperationGuardPools;
const isGeneratorFunctionValue = utilTypes.isGeneratorFunction;
const isGeneratorObjectValue = utilTypes.isGeneratorObject;
const isPromiseValue = utilTypes.isPromise;
const isProxyValue = utilTypes.isProxy;
const numberIsFinite = Number.isFinite;
const jsonStringifyIntrinsic = JSON.stringify;
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
const objectSetPrototypeOf = Object.setPrototypeOf;
const PromiseConstructor = Promise;
const promisePrototype = Promise.prototype;
const promiseSpeciesSymbol = Symbol.species;
const promiseThenIntrinsic = Promise.prototype.then;
const sessionAuthorityErrorPrototype = PostgresSessionAuthorityError.prototype;
const reflectOwnKeys = Reflect.ownKeys;
const runExclusiveIntrinsic = PostgresOperationGuard.prototype.runExclusive;
const TypeErrorConstructor = TypeError;
const WeakMapConstructor = WeakMap;
const weakMapGetIntrinsic = WeakMap.prototype.get;
const weakMapHasIntrinsic = WeakMap.prototype.has;
const weakMapSetIntrinsic = WeakMap.prototype.set;
const WeakSetConstructor = WeakSet;
const weakSetAddIntrinsic = WeakSet.prototype.add;
const weakSetHasIntrinsic = WeakSet.prototype.has;

const OPTION_KEYS = objectFreeze([
  "authority",
  "captureBackend",
  "durableStopCapture",
  "fleetCapabilityGate",
  "lifecycleGuard",
  "launcher",
  "operationGuard",
  "prepareImageReservation",
  "resolveStablePlan",
  "restoreActivationCoordinator",
  "writerDetach",
]);
const ADMISSION_KEYS = objectFreeze(["checkpoint", "request"]);
const PROBE_KEYS = objectFreeze(["assertHeld"]);
const AUTHORITY_METHODS = objectFreeze([
  "claimRestoreAttachmentActivationDispatch",
  "claimRestoreDestinationGenerationDispatch",
  "finalizeRestoreDestinationGeneration",
  "readCheckpointCaptureAttempt",
  "readRestoreAttachmentActivation",
  "readRestoreDestinationGeneration",
  "readSession",
  "readWriterLaunchAttempt",
  "renewWriterLease",
  "reserveOperation",
]);
const CAPTURE_BACKEND_METHODS = objectFreeze([
  "captureCheckpoint",
  "destroySession",
  "detachAttachment",
  "forceFence",
  "prepareWritableAttachment",
  "provisionSession",
  "reconcileCheckpointCapture",
  "restoreCheckpoint",
  "resumePreparedCheckpointCapture",
]);
const CAPTURE_BACKEND_DATA_KEYS = objectFreeze([
  "backendId",
  "capabilities",
  "captureReconciliationContractVersion",
  "contractVersion",
  "preparedCheckpointCaptureContractVersion",
]);
const DURABLE_CAPTURE_METHODS = objectFreeze(["runPreparedCapture"]);
const LAUNCHER_METHODS = objectFreeze([
  "prepareLaunchIntent",
  "runPreparedLaunch",
]);
const ACTIVATION_COORDINATOR_METHODS = objectFreeze([
  "reconcileRestoreAttachmentActivation",
]);
const WRITER_DETACH_METHODS = objectFreeze([
  "detachWriter",
  "forceFenceWriter",
]);
const LIFECYCLE_METHODS = objectFreeze(["runForeground"]);
const IMAGE_RESERVATION_KEYS = objectFreeze([
  "configBytes",
  "descriptor",
  "inspectCodex",
  "reservation",
]);
const RESERVATION_IDENTITY_KEYS = objectFreeze([
  "conflictClass",
  "createdAt",
  "expectedSessionRevision",
  "expiresAt",
  "kind",
  "operationId",
  "requestSha256",
  "reservationId",
  "sessionId",
]);
const MAX_PROTOTYPE_DEPTH = 12;
const OPAQUE_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u;
const regexpExecIntrinsic = RegExp.prototype.exec;
const StringConstructor = String;
const MAX_DATA_DEPTH = 32;
const MAX_DATA_NODES = 65_536;

const ERROR_MESSAGES = objectFreeze({
  invalid_postgres_detached_restore_foreground_composition_options:
    "PostgreSQL detached restore foreground composition options are invalid",
  invalid_postgres_detached_restore_foreground_composition_request:
    "PostgreSQL detached restore foreground composition request is invalid",
  postgres_detached_restore_fleet_capability_required:
    "PostgreSQL detached restore fleet capability is required",
  postgres_detached_restore_foreground_composition_outcome_uncertain:
    "PostgreSQL detached restore foreground composition outcome is uncertain",
});

const facades = new WeakSetConstructor();
const internalErrors = new WeakSetConstructor();
const publicPromiseMirrors = new WeakMapConstructor();
const promiseSettlementBrand = objectFreeze(objectCreate(null));
const fleetFailureCarrier = objectFreeze(objectCreate(null));
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

export const POSTGRES_DETACHED_RESTORE_FLEET_CONFIRMED = objectFreeze(
  objectCreate(null),
);

function callIntrinsic(intrinsic, receiver, args) {
  return functionApplyIntrinsic(intrinsic, receiver, args);
}

function arrayIncludes(value, candidate) {
  return callIntrinsic(arrayIncludesIntrinsic, value, [candidate]);
}

function weakSetAdd(value, entry) {
  callIntrinsic(weakSetAddIntrinsic, value, [entry]);
}

function weakSetHas(value, entry) {
  return callIntrinsic(weakSetHasIntrinsic, value, [entry]);
}

function weakMapGet(value, key) {
  return callIntrinsic(weakMapGetIntrinsic, value, [key]);
}

function weakMapHas(value, key) {
  return callIntrinsic(weakMapHasIntrinsic, value, [key]);
}

function weakMapSet(value, key, entry) {
  callIntrinsic(weakMapSetIntrinsic, value, [key, entry]);
}

export class PostgresDetachedRestoreForegroundCompositionError extends Error {
  constructor(code) {
    if (typeof code !== "string" || !objectHasOwn(ERROR_MESSAGES, code)) {
      throw new TypeErrorConstructor(
        "Unsupported PostgreSQL detached restore foreground composition error",
      );
    }
    super(ERROR_MESSAGES[code]);
    objectDefineProperty(this, "name", {
      enumerable: false,
      value: "PostgresDetachedRestoreForegroundCompositionError",
    });
    objectDefineProperty(this, "code", { enumerable: true, value: code });
    objectDefineProperty(this, "retryable", {
      enumerable: true,
      value: false,
    });
    objectFreeze(this);
  }
}

function fail(code) {
  const error = new PostgresDetachedRestoreForegroundCompositionError(code);
  weakSetAdd(internalErrors, error);
  throw error;
}

function ensure(condition, code) {
  if (!condition) fail(code);
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

function exactDataObject(value, expectedKeys, code, frozen = false) {
  ensure(
    value !== null &&
      typeof value === "object" &&
      !arrayIsArray(value) &&
      !isProxyValue(value),
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
      (!frozen || objectIsFrozen(value)) &&
      keys.length === expectedKeys.length,
    code,
  );
  const normalized = objectCreate(null);
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
    ensure(typeof key === "string" && arrayIncludes(expectedKeys, key), code);
    const descriptor = objectGetOwnPropertyDescriptor(value, key);
    ensure(
      descriptor?.enumerable === true && objectHasOwn(descriptor, "value"),
      code,
    );
    normalized[key] = descriptor.value;
  }
  for (let index = 0; index < expectedKeys.length; index += 1) {
    ensure(objectHasOwn(normalized, expectedKeys[index]), code);
  }
  return normalized;
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

function assertThenFreeValue(value, code) {
  if (
    value === null ||
    (typeof value !== "object" && typeof value !== "function")
  ) {
    return value;
  }
  ensure(!isProxyValue(value), code);
  let current = value;
  try {
    for (let depth = 0; depth <= MAX_PROTOTYPE_DEPTH; depth += 1) {
      if (current === null) return value;
      ensure(!isProxyValue(current), code);
      const descriptor = objectGetOwnPropertyDescriptor(current, "then");
      if (descriptor !== undefined) {
        ensure(
          objectHasOwn(descriptor, "value") &&
            descriptor.value === undefined,
          code,
        );
        return value;
      }
      current = objectGetPrototypeOf(current);
    }
  } catch (error) {
    if (weakSetHas(internalErrors, error)) throw error;
    fail(code);
  }
  fail(code);
}

function lookupDataValue(receiver, name, code) {
  ensure(
    receiver !== null &&
      (typeof receiver === "object" || typeof receiver === "function") &&
      !isProxyValue(receiver),
    code,
  );
  try {
    let current = receiver;
    for (
      let depth = 0;
      current !== null && depth <= MAX_PROTOTYPE_DEPTH;
      depth += 1
    ) {
      ensure(!isProxyValue(current), code);
      const descriptor = objectGetOwnPropertyDescriptor(current, name);
      if (descriptor !== undefined) {
        ensure(objectHasOwn(descriptor, "value"), code);
        return descriptor.value;
      }
      current = objectGetPrototypeOf(current);
    }
  } catch (error) {
    if (weakSetHas(internalErrors, error)) throw error;
    fail(code);
  }
  fail(code);
}

function lookupMethod(receiver, name, code) {
  return trustedFunction(lookupDataValue(receiver, name, code), code);
}

function preflightCollaboratorPrototypeChain(receiver, code) {
  ensure(
    receiver !== null &&
      (typeof receiver === "object" || typeof receiver === "function") &&
      !isProxyValue(receiver),
    code,
  );
  try {
    let current = receiver;
    for (let depth = 0; depth <= MAX_PROTOTYPE_DEPTH; depth += 1) {
      if (current === null) return;
      ensure(!isProxyValue(current), code);
      current = objectGetPrototypeOf(current);
    }
  } catch (error) {
    if (weakSetHas(internalErrors, error)) throw error;
    fail(code);
  }
  fail(code);
}

function collaborator(value, methods, code) {
  preflightCollaboratorPrototypeChain(value, code);
  const bindings = objectCreate(null);
  for (let index = 0; index < methods.length; index += 1) {
    const name = methods[index];
    bindings[name] = lookupMethod(value, name, code);
  }
  return exactFrozenRecord({ methods: objectFreeze(bindings), receiver: value });
}

function preflightCollaboratorData(receiver, keys, code) {
  for (let index = 0; index < keys.length; index += 1) {
    lookupDataValue(receiver, keys[index], code);
  }
}

function regexpTest(pattern, value) {
  return callIntrinsic(regexpExecIntrinsic, pattern, [value]) !== null;
}

function opaqueId(value, code) {
  ensure(typeof value === "string" && regexpTest(OPAQUE_ID_PATTERN, value), code);
  return value;
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
  let species;
  try {
    prototype = objectGetPrototypeOf(value);
    keys = reflectOwnKeys(value);
    species = objectGetOwnPropertyDescriptor(value, promiseSpeciesSymbol);
  } catch {
    return false;
  }
  return (
    prototype === null &&
    keys.length === 1 &&
    keys[0] === promiseSpeciesSymbol &&
    species?.configurable === false &&
    species.enumerable === false &&
    objectHasOwn(species, "value") &&
    species.value === PromiseConstructor &&
    species.writable === false
  );
}

function pinPromiseConstructor(value, code) {
  let descriptor;
  try {
    descriptor = objectGetOwnPropertyDescriptor(value, "constructor");
    if (descriptor === undefined) {
      ensure(objectIsExtensible(value), code);
      objectDefineProperty(value, "constructor", {
        configurable: false,
        enumerable: false,
        value: PromiseConstructor,
        writable: false,
      });
      descriptor = objectGetOwnPropertyDescriptor(value, "constructor");
    }
  } catch {
    fail(code);
  }
  ensure(
    descriptor?.configurable === false &&
      descriptor.enumerable === false &&
      objectHasOwn(descriptor, "value") &&
      descriptor.value === PromiseConstructor &&
      descriptor.writable === false,
    code,
  );
  return value;
}

function adoptNativePromise(value, code) {
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
  ensure(prototype === promisePrototype, code);
  if (ownConstructor === undefined) {
    ensure(
      ownThen === undefined &&
      inheritedConstructor?.configurable === true &&
        inheritedConstructor.enumerable === false &&
        objectHasOwn(inheritedConstructor, "value") &&
        inheritedConstructor.value === PromiseConstructor &&
        inheritedConstructor.writable === true,
      code,
    );
    return value;
  }
  ensure(objectHasOwn(ownConstructor, "value"), code);
  if (ownConstructor.value === PromiseConstructor) {
    ensure(ownThen === undefined, code);
    return value;
  }
  ensure(safePromiseSpeciesHolder(ownConstructor.value), code);
  if (ownThen !== undefined) {
    ensure(
      objectHasOwn(ownThen, "value") &&
        typeof ownThen.value === "function" &&
        !isProxyValue(ownThen.value) &&
        !isGeneratorFunctionValue(ownThen.value),
      code,
    );
  }
  let adopted;
  try {
    adopted = callIntrinsic(promiseThenIntrinsic, value, [undefined, undefined]);
  } catch {
    fail(code);
  }
  ensure(
    isPromiseValue(adopted) &&
      !isProxyValue(adopted) &&
      !isGeneratorObjectValue(adopted) &&
      objectGetPrototypeOf(adopted) === promisePrototype &&
      objectGetOwnPropertyDescriptor(adopted, "then") === undefined,
    code,
  );
  return pinPromiseConstructor(adopted, code);
}

async function settleNativePromiseInternal(value, code) {
  const adopted = adoptNativePromise(value, code);
  try {
    return exactFrozenRecord({
      error: null,
      ok: true,
      value: await adopted,
    });
  } catch (error) {
    return exactFrozenRecord({ error, ok: false, value: null });
  }
}

function settleNativePromise(value, code) {
  return pinPromiseConstructor(settleNativePromiseInternal(value, code), code);
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

function unwrapPromiseSettlementCarrier(value, code) {
  const carrier = exactDataObject(
    value,
    ["brand", "status", "value"],
    code,
    true,
  );
  ensure(
    carrier.brand === promiseSettlementBrand &&
      (carrier.status === "fulfilled" || carrier.status === "rejected"),
    code,
  );
  return carrier;
}

function protectPublicPromise(value, code) {
  ensure(
    isPromiseValue(value) &&
      !isProxyValue(value) &&
      !isGeneratorObjectValue(value) &&
      objectGetPrototypeOf(value) === promisePrototype &&
      !weakMapHas(publicPromiseMirrors, value),
    code,
  );
  let constructorDescriptor;
  let thenDescriptor;
  let catchDescriptor;
  let finallyDescriptor;
  try {
    constructorDescriptor = objectGetOwnPropertyDescriptor(value, "constructor");
    thenDescriptor = objectGetOwnPropertyDescriptor(value, "then");
    catchDescriptor = objectGetOwnPropertyDescriptor(value, "catch");
    finallyDescriptor = objectGetOwnPropertyDescriptor(value, "finally");
  } catch {
    fail(code);
  }
  ensure(
    constructorDescriptor === undefined &&
      thenDescriptor === undefined &&
      catchDescriptor === undefined &&
      finallyDescriptor === undefined &&
      objectIsExtensible(value),
    code,
  );
  let mirror;
  try {
    objectDefineProperty(value, "constructor", {
      configurable: true,
      enumerable: false,
      value: promiseSpeciesHolder,
      writable: false,
    });
    mirror = callIntrinsic(promiseThenIntrinsic, value, [
      promiseFulfillmentCarrier,
      promiseRejectionCarrier,
    ]);
    objectDefineProperty(mirror, "constructor", {
      configurable: false,
      enumerable: false,
      value: promiseSpeciesHolder,
      writable: false,
    });
    objectDefineProperties(value, {
      catch: {
        configurable: false,
        enumerable: false,
        value: publicPromiseCatch,
        writable: false,
      },
      constructor: {
        configurable: false,
        enumerable: false,
        value: PromiseConstructor,
        writable: false,
      },
      finally: {
        configurable: false,
        enumerable: false,
        value: publicPromiseFinally,
        writable: false,
      },
      then: {
        configurable: false,
        enumerable: false,
        value: publicPromiseThen,
        writable: false,
      },
    });
  } catch {
    fail(code);
  }
  ensure(
    isPromiseValue(mirror) &&
      !isProxyValue(mirror) &&
      objectGetPrototypeOf(mirror) === promisePrototype,
    code,
  );
  weakMapSet(publicPromiseMirrors, value, mirror);
  return value;
}

function publicPromiseThen(onFulfilled, onRejected) {
  const code =
    "postgres_detached_restore_foreground_composition_outcome_uncertain";
  ensure(weakMapHas(publicPromiseMirrors, this), code);
  const mirror = weakMapGet(publicPromiseMirrors, this);
  const dispatch = (rawCarrier) => {
    const carrier = unwrapPromiseSettlementCarrier(rawCarrier, code);
    if (carrier.status === "fulfilled") {
      return protectPublicReactionResult(
        typeof onFulfilled === "function"
          ? callIntrinsic(onFulfilled, undefined, [carrier.value])
          : carrier.value,
        code,
      );
    }
    if (typeof onRejected === "function") {
      return protectPublicReactionResult(
        callIntrinsic(onRejected, undefined, [carrier.value]),
        code,
      );
    }
    throw carrier.value;
  };
  let child;
  try {
    child = callIntrinsic(promiseThenIntrinsic, mirror, [dispatch, undefined]);
  } catch {
    fail(code);
  }
  return protectPublicPromise(child, code);
}

function publicPromiseCatch(onRejected) {
  return callIntrinsic(publicPromiseThen, this, [undefined, onRejected]);
}

function protectPublicReactionResult(value, code) {
  if (!isPromiseValue(value)) return value;
  if (weakMapHas(publicPromiseMirrors, value)) return value;
  let prototype;
  let ownConstructor;
  try {
    prototype = objectGetPrototypeOf(value);
    ownConstructor = objectGetOwnPropertyDescriptor(value, "constructor");
  } catch {
    fail(code);
  }
  ensure(prototype === promisePrototype, code);
  if (
    ownConstructor !== undefined &&
    objectHasOwn(ownConstructor, "value") &&
    safePromiseSpeciesHolder(ownConstructor.value)
  ) {
    let bridged;
    try {
      bridged = callIntrinsic(promiseThenIntrinsic, value, [
        undefined,
        undefined,
      ]);
    } catch {
      fail(code);
    }
    return protectPublicPromise(bridged, code);
  }
  return protectPublicPromise(value, code);
}

function resolvePublicPromise(value) {
  const code =
    "postgres_detached_restore_foreground_composition_outcome_uncertain";
  let pending;
  let resolvePending;
  try {
    pending = new PromiseConstructor((resolve) => {
      resolvePending = resolve;
    });
  } catch {
    fail(code);
  }
  const protectedPending = protectPublicPromise(pending, code);
  const protectedValue = protectPublicReactionResult(value, code);
  try {
    callIntrinsic(resolvePending, undefined, [protectedValue]);
  } catch {
    fail(code);
  }
  return protectedPending;
}

function publicPromiseFinally(onFinally) {
  if (typeof onFinally !== "function") {
    return callIntrinsic(publicPromiseThen, this, [onFinally, onFinally]);
  }
  const runFinally = () =>
    resolvePublicPromise(callIntrinsic(onFinally, undefined, []));
  return callIntrinsic(publicPromiseThen, this, [
    (value) =>
      callIntrinsic(publicPromiseThen, runFinally(), [() => value, undefined]),
    (reason) =>
      callIntrinsic(publicPromiseThen, runFinally(), [
        () => {
          throw reason;
        },
        undefined,
      ]),
  ]);
}

objectFreeze(promiseFulfillmentCarrier);
objectFreeze(promiseRejectionCarrier);
objectFreeze(publicPromiseThen);
objectFreeze(publicPromiseCatch);
objectFreeze(publicPromiseFinally);

async function invoke(binding, name, args, code) {
  let pending;
  try {
    pending = callIntrinsic(binding.methods[name], binding.receiver, args);
  } catch {
    fail(code);
  }
  const settled = await settleNativePromise(pending, code);
  if (settled.ok) return assertThenFreeValue(settled.value, code);
  if (weakSetHas(internalErrors, settled.error)) throw settled.error;
  fail(code);
}

async function invokeForRead(binding, name, args, code) {
  let pending;
  try {
    pending = callIntrinsic(binding.methods[name], binding.receiver, args);
  } catch (error) {
    if (weakSetHas(internalErrors, error)) throw error;
    return exactFrozenRecord({ error, ok: false, value: null });
  }
  return assertThenFreeValue(await settleNativePromise(pending, code), code);
}

function authorityAbsence(error, code) {
  try {
    if (
      error === null ||
      typeof error !== "object" ||
      isProxyValue(error) ||
      !objectIsFrozen(error)
    ) {
      return false;
    }
    let current = objectGetPrototypeOf(error);
    let authentic = false;
    for (let depth = 0; current !== null && depth <= 4; depth += 1) {
      if (current === sessionAuthorityErrorPrototype) {
        authentic = true;
        break;
      }
      current = objectGetPrototypeOf(current);
    }
    const descriptor = objectGetOwnPropertyDescriptor(error, "code");
    return (
      authentic &&
      descriptor?.enumerable === true &&
      objectHasOwn(descriptor, "value") &&
      descriptor.value === code
    );
  } catch {
    return false;
  }
}

function sessionSnapshot(value, code) {
  try {
    return assertSessionAuthoritySnapshot(value);
  } catch {
    fail(code);
  }
}

function ownDataValue(value, key, code) {
  ensure(
    value !== null && typeof value === "object" && !isProxyValue(value),
    code,
  );
  const descriptor = objectGetOwnPropertyDescriptor(value, key);
  ensure(descriptor !== undefined && objectHasOwn(descriptor, "value"), code);
  return descriptor.value;
}

function pointerMatches(pointer, operationId, kind) {
  if (pointer === null || typeof pointer !== "object" || isProxyValue(pointer)) {
    return false;
  }
  try {
    return (
      ownDataValue(pointer, "operationId", "postgres_detached_restore_foreground_composition_outcome_uncertain") === operationId &&
      ownDataValue(pointer, "kind", "postgres_detached_restore_foreground_composition_outcome_uncertain") === kind
    );
  } catch {
    return false;
  }
}

function operationMatches(operation, operationId, kind, code) {
  ensure(
    ownDataValue(operation, "operationId", code) === operationId &&
      ownDataValue(operation, "kind", code) === kind,
    code,
  );
  return operation;
}

function parseTimestamp(value, code) {
  const milliseconds = callIntrinsic(dateParseIntrinsic, DateConstructor, [value]);
  ensure(numberIsFinite(milliseconds), code);
  return milliseconds;
}

function dataTree(value, code, state, depth = 0) {
  ensure(depth <= MAX_DATA_DEPTH, code);
  state.nodes += 1;
  ensure(state.nodes <= MAX_DATA_NODES, code);
  if (
    value === null ||
    typeof value === "string" ||
    typeof value === "boolean"
  ) {
    return value;
  }
  if (typeof value === "number") {
    ensure(numberIsFinite(value), code);
    return value;
  }
  ensure(
    typeof value === "object" &&
      !isProxyValue(value) &&
      (arrayIsArray(value) ||
        objectGetPrototypeOf(value) === objectPrototype ||
        objectGetPrototypeOf(value) === null),
    code,
  );
  if (arrayIsArray(value)) {
    const keys = reflectOwnKeys(value);
    ensure(keys.length === value.length + 1 && keys[keys.length - 1] === "length", code);
    const result = [];
    try {
      callIntrinsic(objectSetPrototypeOf, Object, [result, null]);
    } catch {
      fail(code);
    }
    for (let index = 0; index < value.length; index += 1) {
      const key = StringConstructor(index);
      const descriptor = objectGetOwnPropertyDescriptor(value, key);
      ensure(
        descriptor?.enumerable === true && objectHasOwn(descriptor, "value"),
        code,
      );
      result[index] = dataTree(descriptor.value, code, state, depth + 1);
    }
    return objectFreeze(result);
  }
  const keys = reflectOwnKeys(value);
  for (let index = 0; index < keys.length; index += 1) {
    ensure(typeof keys[index] === "string", code);
  }
  callIntrinsic(arraySortIntrinsic, keys, []);
  const result = objectCreate(null);
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
    const descriptor = objectGetOwnPropertyDescriptor(value, key);
    ensure(
      descriptor?.enumerable === true && objectHasOwn(descriptor, "value"),
      code,
    );
    objectDefineProperty(result, key, {
      enumerable: true,
      value: dataTree(descriptor.value, code, state, depth + 1),
    });
  }
  return objectFreeze(result);
}

function sameData(left, right, code) {
  let leftSerialized;
  let rightSerialized;
  try {
    leftSerialized = callIntrinsic(jsonStringifyIntrinsic, JSON, [
      dataTree(left, code, { nodes: 0 }),
    ]);
    rightSerialized = callIntrinsic(jsonStringifyIntrinsic, JSON, [
      dataTree(right, code, { nodes: 0 }),
    ]);
  } catch (error) {
    if (weakSetHas(internalErrors, error)) throw error;
    fail(code);
  }
  ensure(
    typeof leftSerialized === "string" &&
      leftSerialized === rightSerialized,
    code,
  );
  return true;
}

function normalizeAdmission(value, code) {
  const input = exactDataObject(value, ADMISSION_KEYS, code);
  let checkpoint;
  let request;
  try {
    checkpoint = assertCheckpointDescriptor(input.checkpoint);
    request = assertStorageMutationRequest(input.request);
  } catch {
    fail(code);
  }
  ensure(
    checkpoint.checkpointClass === "clean" &&
      request.operation === "restore" &&
      request.sessionId === checkpoint.sessionId &&
      request.backendId === checkpoint.backendId &&
      request.target.kind === "checkpoint" &&
      request.target.checkpointId === checkpoint.checkpointId &&
      request.target.artifactId === checkpoint.artifactId,
    code,
  );
  return exactFrozenRecord({ checkpoint, request });
}

function captureSafetyTuple(plan, session, admission, code) {
  const document = session.document;
  const manifest = document.manifest;
  const storageRef = document.storageRef;
  const request = exactFrozenRecord({
    backendId: admission.request.backendId,
    contractVersion: 1,
    fencingEpoch: admission.request.fencingEpoch,
    holderId: admission.request.holderId,
    leaseId: admission.request.leaseId,
    operation: "checkpoint",
    operationId: plan.captureOperationId,
    sessionId: admission.request.sessionId,
    storageId: admission.request.storageId,
    target: exactFrozenRecord({
      artifactId: plan.captureArtifactId,
      checkpointId: plan.captureCheckpointId,
      kind: "checkpoint",
    }),
  });
  let canonicalRequest;
  let checkpoint;
  try {
    canonicalRequest = assertStorageMutationRequest(request);
    checkpoint = assertCheckpointDescriptor(
      exactFrozenRecord({
        artifactId: plan.captureArtifactId,
        backendId: admission.request.backendId,
        checkpointClass: "clean",
        checkpointId: plan.captureCheckpointId,
        codexSessionId: manifest.codex.sessionId,
        codexThreadId: manifest.codex.rootThreadId,
        contractVersion: storageRef.contractVersion,
        createdAt: plan.captureCreatedAt,
        imageDigest: manifest.runtime.imageDigest,
        sessionId: admission.request.sessionId,
        sourceFencingEpoch: admission.request.fencingEpoch,
        storageId: admission.request.storageId,
      }),
      { manifest, storageRef },
    );
  } catch {
    fail(code);
  }
  return exactFrozenRecord({ checkpoint, request: canonicalRequest });
}

async function invokeCallback(callback, input, code) {
  let value;
  try {
    value = callIntrinsic(callback, undefined, [input]);
  } catch {
    fail(code);
  }
  if (!isPromiseValue(value)) return assertThenFreeValue(value, code);
  const settled = await settleNativePromise(value, code);
  if (settled.ok) return assertThenFreeValue(settled.value, code);
  if (weakSetHas(internalErrors, settled.error)) throw settled.error;
  fail(code);
}

async function assertLifecycleHeld(lease, code) {
  let pending;
  try {
    pending = assertPostgresRestoreLifecycleLeaseHeld(lease, "foreground");
  } catch {
    fail(code);
  }
  const settled = await settleNativePromise(pending, code);
  if (settled.ok) return;
  if (weakSetHas(internalErrors, settled.error)) throw settled.error;
  fail(code);
}

function frozenReceipt(value, code) {
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
  ensure(prototype === objectPrototype || prototype === null, code);
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
    let descriptor;
    try {
      descriptor = objectGetOwnPropertyDescriptor(value, key);
    } catch {
      fail(code);
    }
    ensure(
      typeof key === "string" &&
        descriptor?.enumerable === true &&
        objectHasOwn(descriptor, "value"),
      code,
    );
  }
  return value;
}

function normalizeCaptureReceipt(value, tuple, code) {
  const receipt = frozenReceipt(value, code);
  const operation = operationMatches(
    ownDataValue(receipt, "operation", code),
    tuple.request.operationId,
    CHECKPOINT_CAPTURE_OPERATION_KIND,
    code,
  );
  const request = ownDataValue(operation, "request", code);
  const admission = ownDataValue(request, "admission", code);
  ensure(
    ownDataValue(request, "contractVersion", code) === 1 &&
      sameData(ownDataValue(admission, "checkpoint", code), tuple.checkpoint, code) &&
      sameData(ownDataValue(admission, "request", code), tuple.request, code),
    code,
  );
  const state = ownDataValue(operation, "state", code);
  ensure(
    arrayIncludes(["prepared", "starting", "uncertain", "committed"], state),
    code,
  );
  const stopOperationId = opaqueId(
    ownDataValue(admission, "stopOperationId", code),
    code,
  );
  return exactFrozenRecord({
    attempt: ownDataValue(receipt, "attempt", code),
    catalogue: ownDataValue(receipt, "catalogue", code),
    operation,
    reservation: ownDataValue(receipt, "reservation", code),
    session: sessionSnapshot(ownDataValue(receipt, "session", code), code),
    state,
    stopOperationId,
  });
}

async function readCaptureOptional(authority, tuple, code) {
  const settled = await invokeForRead(
    authority,
    "readCheckpointCaptureAttempt",
    [exactFrozenRecord({ checkpoint: tuple.checkpoint, request: tuple.request })],
    code,
  );
  if (settled.ok) return normalizeCaptureReceipt(settled.value, tuple, code);
  if (authorityAbsence(settled.error, "checkpoint_capture_not_authorized")) {
    return null;
  }
  fail(code);
}

function normalizeGenerationReceipt(value, plan, admission, code) {
  const receipt = frozenReceipt(value, code);
  const operation = operationMatches(
    ownDataValue(receipt, "operation", code),
    admission.request.operationId,
    RESTORE_DESTINATION_GENERATION_OPERATION_KIND,
    code,
  );
  const operationRequest = ownDataValue(operation, "request", code);
  ensure(
    ownDataValue(operationRequest, "contractVersion", code) === 1 &&
      sameData(ownDataValue(operationRequest, "admission", code), admission, code),
    code,
  );
  const state = ownDataValue(operation, "state", code);
  ensure(arrayIncludes(["starting", "uncertain", "committed"], state), code);
  const generation = ownDataValue(receipt, "generation", code);
  ensure(
    generation !== null &&
      opaqueId(ownDataValue(generation, "generationId", code), code) ===
        plan.generationId &&
      ownDataValue(generation, "operationId", code) ===
        admission.request.operationId &&
      arrayIncludes(["authorized", "committed"], ownDataValue(generation, "state", code)),
    code,
  );
  return exactFrozenRecord({
    catalogue: ownDataValue(receipt, "catalogue", code),
    generation,
    operation,
    reservation: ownDataValue(receipt, "reservation", code),
    session: sessionSnapshot(ownDataValue(receipt, "session", code), code),
    state,
  });
}

async function readGenerationOptional(authority, plan, admission, code) {
  const settled = await invokeForRead(
    authority,
    "readRestoreDestinationGeneration",
    [
      exactFrozenRecord({
        checkpoint: admission.checkpoint,
        generationId: plan.generationId,
        request: admission.request,
      }),
    ],
    code,
  );
  if (settled.ok) {
    return normalizeGenerationReceipt(settled.value, plan, admission, code);
  }
  if (authorityAbsence(settled.error, "restore_generation_not_authorized")) {
    return null;
  }
  fail(code);
}

function normalizeActivationReceipt(value, plan, code) {
  const receipt = frozenReceipt(value, code);
  const operation = operationMatches(
    ownDataValue(receipt, "operation", code),
    plan.activationOperationId,
    RESTORE_ATTACHMENT_ACTIVATION_OPERATION_KIND,
    code,
  );
  const state = ownDataValue(operation, "state", code);
  ensure(
    arrayIncludes(["prepared", "starting", "uncertain", "committed"], state),
    code,
  );
  return exactFrozenRecord({
    activationRequest: ownDataValue(receipt, "activationRequest", code),
    generation: ownDataValue(receipt, "generation", code),
    operation,
    request: ownDataValue(operation, "request", code),
    reservation: ownDataValue(receipt, "reservation", code),
    session: sessionSnapshot(ownDataValue(receipt, "session", code), code),
    state,
  });
}

async function readActivationOptional(authority, plan, code) {
  const settled = await invokeForRead(
    authority,
    "readRestoreAttachmentActivation",
    [exactFrozenRecord({ operationId: plan.activationOperationId })],
    code,
  );
  if (settled.ok) return normalizeActivationReceipt(settled.value, plan, code);
  if (
    authorityAbsence(
      settled.error,
      "restore_attachment_activation_not_authorized",
    )
  ) {
    return null;
  }
  fail(code);
}

function normalizeLaunchReceipt(value, plan, code) {
  const receipt = frozenReceipt(value, code);
  const operation = operationMatches(
    ownDataValue(receipt, "operation", code),
    plan.launchAttemptId,
    WRITER_LAUNCH_ATTEMPT_OPERATION_KIND,
    code,
  );
  const state = ownDataValue(operation, "state", code);
  ensure(
    arrayIncludes(["prepared", "starting", "uncertain", "committed"], state),
    code,
  );
  const attempt = ownDataValue(receipt, "attempt", code);
  ensure(
    ownDataValue(attempt, "launchAttemptId", code) === plan.launchAttemptId &&
      ownDataValue(attempt, "state", code) === state &&
      sameData(
        ownDataValue(attempt, "request", code),
        ownDataValue(operation, "request", code),
        code,
      ),
    code,
  );
  const session = sessionSnapshot(ownDataValue(receipt, "session", code), code);
  if (state === "committed") {
    ensure(
      session.document.activeOperation === null &&
        pointerMatches(
          session.document.lastOperation,
          plan.launchAttemptId,
          WRITER_LAUNCH_ATTEMPT_OPERATION_KIND,
        ) &&
        session.document.launch !== null &&
        ownDataValue(session.document.launch, "launchAttemptId", code) ===
          plan.launchAttemptId,
      code,
    );
  } else {
    ensure(
      pointerMatches(
        session.document.activeOperation,
        plan.launchAttemptId,
        WRITER_LAUNCH_ATTEMPT_OPERATION_KIND,
      ),
      code,
    );
  }
  return exactFrozenRecord({
    attempt,
    launch: ownDataValue(receipt, "launch", code),
    operation,
    reservation: ownDataValue(receipt, "reservation", code),
    session,
    state,
  });
}

async function readLaunchOptional(authority, plan, code) {
  const settled = await invokeForRead(
    authority,
    "readWriterLaunchAttempt",
    [exactFrozenRecord({ operationId: plan.launchAttemptId })],
    code,
  );
  if (settled.ok) return normalizeLaunchReceipt(settled.value, plan, code);
  if (authorityAbsence(settled.error, "writer_launch_attempt_not_authorized")) {
    return null;
  }
  fail(code);
}

async function runOperationGuard(operationGuard, operationId, callback, code) {
  let pending;
  try {
    pending = callIntrinsic(runExclusiveIntrinsic, operationGuard, [
      operationId,
      async (probeValue, completeValue) => {
        const probe = exactDataObject(probeValue, PROBE_KEYS, code, true);
        const assertHeld = trustedFunction(probe.assertHeld, code);
        const complete = trustedFunction(completeValue, code);
        ensure(objectIsFrozen(completeValue), code);
        const assertOperationHeld = async () => {
          let probePending;
          try {
            probePending = callIntrinsic(assertHeld, undefined, []);
          } catch {
            fail(code);
          }
          const settled = await settleNativePromise(probePending, code);
          if (!settled.ok) {
            if (weakSetHas(internalErrors, settled.error)) throw settled.error;
            fail(code);
          }
        };
        objectFreeze(assertOperationHeld);
        let callbackPending;
        try {
          callbackPending = callIntrinsic(callback, undefined, [
            assertOperationHeld,
          ]);
        } catch (error) {
          if (weakSetHas(internalErrors, error)) throw error;
          fail(code);
        }
        const callbackSettlement = await settleNativePromise(
          callbackPending,
          code,
        );
        if (!callbackSettlement.ok) {
          if (weakSetHas(internalErrors, callbackSettlement.error)) {
            throw callbackSettlement.error;
          }
          fail(code);
        }
        return assertThenFreeValue(
          callIntrinsic(complete, undefined, [callbackSettlement.value]),
          code,
        );
      },
    ]);
  } catch (error) {
    if (weakSetHas(internalErrors, error)) throw error;
    fail(code);
  }
  const settled = await settleNativePromise(pending, code);
  if (settled.ok) return assertThenFreeValue(settled.value, code);
  if (weakSetHas(internalErrors, settled.error)) throw settled.error;
  fail(code);
}

function pointerKindForPlan(plan) {
  return plan.detachMode === "release"
    ? WRITER_RELEASE_OPERATION_KIND
    : WRITER_FORCE_FENCE_OPERATION_KIND;
}

function pointerIsWorkflow(pointer, plan, stopOperationId) {
  return (
    pointerMatches(pointer, plan.renewalOperationId, WRITER_LEASE_RENEW_OPERATION_KIND) ||
    (stopOperationId !== null &&
      pointerMatches(pointer, stopOperationId, WRITER_LAUNCH_STOP_OPERATION_KIND)) ||
    pointerMatches(pointer, plan.captureOperationId, CHECKPOINT_CAPTURE_OPERATION_KIND) ||
    pointerMatches(
      pointer,
      plan.request.operationId,
      RESTORE_DESTINATION_GENERATION_OPERATION_KIND,
    ) ||
    pointerMatches(pointer, plan.detachOperationId, pointerKindForPlan(plan)) ||
    pointerMatches(
      pointer,
      plan.activationOperationId,
      RESTORE_ATTACHMENT_ACTIVATION_OPERATION_KIND,
    ) ||
    pointerMatches(pointer, plan.launchAttemptId, WRITER_LAUNCH_ATTEMPT_OPERATION_KIND)
  );
}

async function readSession(authority, sessionId, code) {
  return assertThenFreeValue(sessionSnapshot(
    await invoke(
      authority,
      "readSession",
      [exactFrozenRecord({ sessionId })],
      code,
    ),
    code,
  ), code);
}

function deriveCurrentStopOperationId(session, tuple, code) {
  const document = session.document;
  ensure(
    document.lifecycle === "ATTACHED" &&
      document.attachment !== null &&
      document.lease !== null &&
      document.launch !== null,
    code,
  );
  let derived;
  try {
    derived = derivePostgresLogicalWriterStopOperationId({
      attachment: document.attachment,
      checkpoint: tuple.checkpoint,
      launchAttemptId: ownDataValue(document.launch, "launchAttemptId", code),
      request: tuple.request,
    });
  } catch {
    fail(code);
  }
  return opaqueId(derived, code);
}

function normalizeRenewalReceipt(value, base, code) {
  const receipt = frozenReceipt(value, code);
  const operation = operationMatches(
    ownDataValue(receipt, "operation", code),
    base.operationId,
    WRITER_LEASE_RENEW_OPERATION_KIND,
    code,
  );
  ensure(
    ownDataValue(operation, "state", code) === "committed" &&
      sameData(ownDataValue(operation, "expectedSession", code), base.expectedSession, code) &&
      sameData(ownDataValue(operation, "request", code), base.request, code),
    code,
  );
  const session = sessionSnapshot(ownDataValue(receipt, "session", code), code);
  const reservation = ownDataValue(receipt, "reservation", code);
  ensure(
    session.document.lifecycle === "ATTACHED" &&
      session.document.lease !== null &&
      session.document.attachment !== null &&
      pointerMatches(
        session.document.lastOperation,
        base.operationId,
        WRITER_LEASE_RENEW_OPERATION_KIND,
      ),
    code,
  );
  try {
    assertSessionOperationTransitionProof({
      operation,
      reservation,
      session,
    });
  } catch {
    fail(code);
  }
  return session;
}

async function renewFreshSession(authority, plan, session, lease, code) {
  const base = exactFrozenRecord({
    expectedSession: session,
    kind: WRITER_LEASE_RENEW_OPERATION_KIND,
    operationId: plan.renewalOperationId,
    request: exactFrozenRecord({
      contractVersion: 1,
      leaseDurationMilliseconds: plan.leaseDurationMilliseconds,
    }),
  });
  await assertLifecycleHeld(lease, code);
  const settled = await invokeForRead(
    authority,
    "renewWriterLease",
    [base],
    code,
  );
  if (settled.ok) {
    const renewed = normalizeRenewalReceipt(settled.value, base, code);
    await assertLifecycleHeld(lease, code);
    return assertThenFreeValue(renewed, code);
  }
  // There is no typed read-by-operation-id seam for a renewal. A last-pointer
  // match cannot prove the original expected snapshot or request digest after
  // an acknowledgement loss, so a failed fresh call is never reclassified as
  // success here.
  fail(code);
}

function normalizeCaptureResult(value, tuple, code) {
  const result = exactDataObject(value, ["checkpoint", "mutation"], code, true);
  let checkpoint;
  let mutation;
  try {
    checkpoint = assertCheckpointDescriptor(result.checkpoint);
    mutation = assertStorageMutationResult(result.mutation, {
      request: tuple.request,
    });
  } catch {
    fail(code);
  }
  ensure(
    sameData(checkpoint, tuple.checkpoint, code) &&
      mutation.operation === "checkpoint" &&
      mutation.status === "checkpoint-created",
    code,
  );
  return value;
}

async function continueCapture(
  bindings,
  tuple,
  plan,
  session,
  captureRead,
  stopOperationId,
  lease,
  code,
) {
  let result;
  if (captureRead === null) {
    ensure(
      session.document.lifecycle === "ATTACHED" &&
        session.document.activeOperation === null &&
        session.document.attachment !== null &&
        session.document.lease !== null &&
        session.document.launch !== null,
      code,
    );
    const actualStopOperationId = deriveCurrentStopOperationId(
      session,
      tuple,
      code,
    );
    ensure(
      stopOperationId === null || stopOperationId === actualStopOperationId,
      code,
    );
    const now = callIntrinsic(dateNowIntrinsic, DateConstructor, []);
    ensure(
      numberIsFinite(now) &&
        now < parseTimestamp(session.document.lease.expiresAt, code),
      code,
    );
    await assertLifecycleHeld(lease, code);
    result = await invoke(
      bindings.durableStopCapture,
      "runPreparedCapture",
      [
        exactFrozenRecord({
          attachment: session.document.attachment,
          backend: bindings.captureBackend.receiver,
          canonicalLease: session.document.lease,
          checkpointClass: "clean",
          createdAt: plan.captureCreatedAt,
          manifest: session.document.manifest,
          now,
          request: tuple.request,
          storageRef: session.document.storageRef,
        }),
      ],
      code,
    );
    stopOperationId = actualStopOperationId;
  } else {
    stopOperationId = captureRead.stopOperationId;
    await assertLifecycleHeld(lease, code);
    if (captureRead.state === "prepared") {
      try {
        assertSessionOperationTransitionProof({
          operation: captureRead.operation,
          reservation: captureRead.reservation,
          session: captureRead.session,
        });
      } catch {
        fail(code);
      }
      result = await invoke(
        bindings.captureBackend,
        "resumePreparedCheckpointCapture",
        [exactFrozenRecord({ checkpoint: tuple.checkpoint, request: tuple.request })],
        code,
      );
    } else {
      result = await invoke(
        bindings.captureBackend,
        "reconcileCheckpointCapture",
        [exactFrozenRecord({ checkpoint: tuple.checkpoint, request: tuple.request })],
        code,
      );
    }
  }
  normalizeCaptureResult(result, tuple, code);
  await assertLifecycleHeld(lease, code);
  const committed = await readCaptureOptional(bindings.authority, tuple, code);
  ensure(
    committed !== null &&
      committed.state === "committed" &&
      committed.stopOperationId === stopOperationId,
    code,
  );
  return exactFrozenRecord({ capture: committed, stopOperationId });
}

async function invokeAsyncCallback(callback, input, code) {
  let pending;
  try {
    pending = callIntrinsic(callback, undefined, [input]);
  } catch {
    fail(code);
  }
  const settled = await settleNativePromise(pending, code);
  if (settled.ok) return assertThenFreeValue(settled.value, code);
  if (weakSetHas(internalErrors, settled.error)) throw settled.error;
  fail(code);
}

function normalizeReservedOperation(value, base, code) {
  const receipt = frozenReceipt(value, code);
  const operation = operationMatches(
    ownDataValue(receipt, "operation", code),
    base.operationId,
    base.kind,
    code,
  );
  ensure(
    ownDataValue(operation, "state", code) === "prepared" &&
      sameData(ownDataValue(operation, "expectedSession", code), base.expectedSession, code) &&
      sameData(ownDataValue(operation, "request", code), base.request, code),
    code,
  );
  return exactFrozenRecord({
    operation,
    reservation: ownDataValue(receipt, "reservation", code),
    session: sessionSnapshot(ownDataValue(receipt, "session", code), code),
  });
}

function generationPublicationContext(read, plan, publicationMode, code) {
  const expectedSession = ownDataValue(read.operation, "expectedSession", code);
  const document = ownDataValue(expectedSession, "document", code);
  const binding = ownDataValue(read.generation, "binding", code);
  const catalogueDocument = ownDataValue(read.catalogue, "document", code);
  const artifactProof = ownDataValue(catalogueDocument, "artifactProof", code);
  const predeterminedResult = ownDataValue(
    ownDataValue(read.operation, "request", code),
    "predeterminedResult",
    code,
  );
  return exactFrozenRecord({
    artifactDirectory: plan.sourceArtifactDirectory,
    artifactOwnedRoot: plan.sourceArtifactOwnedRoot,
    artifactProof,
    canonicalLease: ownDataValue(document, "lease", code),
    destinationDirectory: plan.destinationDirectory,
    destinationIsolationProofId: plan.destinationIsolationProofId,
    destinationOwnedRoot: plan.destinationOwnedRoot,
    destinationState: "detached",
    generationBinding: binding,
    now: parseTimestamp(ownDataValue(read.generation, "claimedAt", code), code),
    publicationMode,
    reservationId: ownDataValue(read.reservation, "reservationId", code),
    result: predeterminedResult,
    storageRef: ownDataValue(document, "storageRef", code),
  });
}

function validateGenerationCompletion(completion, read, mode, code) {
  const normalized = exactDataObject(
    completion,
    ["materialization", "replayed", "result"],
    code,
    true,
  );
  const expectedResult = ownDataValue(
    ownDataValue(read.operation, "request", code),
    "predeterminedResult",
    code,
  );
  ensure(
    typeof normalized.replayed === "boolean" &&
      (mode !== "committed-only" || normalized.replayed === true) &&
      sameData(normalized.result, expectedResult, code),
    code,
  );
  return completion;
}

function validateCommittedGeneration(read, completion, code) {
  ensure(
    read.state === "committed" &&
      ownDataValue(read.generation, "state", code) === "committed",
    code,
  );
  const document = ownDataValue(read.generation, "document", code);
  ensure(
    document !== null &&
      sameData(
        ownDataValue(document, "materialization", code),
        ownDataValue(completion, "materialization", code),
        code,
      ) &&
      sameData(
        ownDataValue(document, "result", code),
        ownDataValue(completion, "result", code),
        code,
      ),
    code,
  );
  return read;
}

async function runGeneration(
  bindings,
  plan,
  admission,
  capture,
  existing,
  publish,
  lease,
  code,
) {
  const expectedSession =
    existing === null
      ? capture.session
      : ownDataValue(existing.operation, "expectedSession", code);
  let typedRequest;
  try {
    typedRequest = createRestoreDestinationGenerationOperationRequest({
      admission,
      expectedSession,
    });
  } catch {
    fail(code);
  }
  const base = exactFrozenRecord({
    expectedSession,
    kind: RESTORE_DESTINATION_GENERATION_OPERATION_KIND,
    operationId: admission.request.operationId,
    request: typedRequest,
  });
  return runOperationGuard(
    bindings.operationGuard,
    base.operationId,
    async (assertOperationHeld) => {
      let read = existing;
      let dispatchGranted = false;
      if (read === null) {
        await assertOperationHeld();
        await assertLifecycleHeld(lease, code);
        const reserved = normalizeReservedOperation(
          await invoke(bindings.authority, "reserveOperation", [base], code),
          base,
          code,
        );
        const claimInput = exactFrozenRecord({
          ...base,
          destinationIsolationProofId: plan.destinationIsolationProofId,
          expectedOperationRevision: "0",
          generationId: plan.generationId,
        });
        const claimSettlement = await invokeForRead(
          bindings.authority,
          "claimRestoreDestinationGenerationDispatch",
          [claimInput],
          code,
        );
        if (claimSettlement.ok) {
          read = normalizeGenerationReceipt(
            claimSettlement.value,
            plan,
            admission,
            code,
          );
          dispatchGranted =
            ownDataValue(claimSettlement.value, "dispatchGranted", code) === true;
        } else {
          read = await readGenerationOptional(
            bindings.authority,
            plan,
            admission,
            code,
          );
          ensure(read !== null, code);
        }
        ensure(reserved.operation.operationId === read.operation.operationId, code);
      }
      ensure(
        sameData(ownDataValue(read.operation, "expectedSession", code), base.expectedSession, code) &&
          sameData(ownDataValue(read.operation, "request", code), base.request, code),
        code,
      );
      if (dispatchGranted) {
        ensure(
          read.state === "starting" &&
            ownDataValue(read.generation, "state", code) === "authorized",
          code,
        );
        try {
          assertSessionOperationTransitionProof({
            operation: read.operation,
            reservation: read.reservation,
            session: read.session,
          });
        } catch {
          fail(code);
        }
      }
      try {
        assertSessionOperationTransitionProof({
          operation: capture.operation,
          reservation: capture.reservation,
          session: base.expectedSession,
        });
      } catch {
        fail(code);
      }
      const publicationMode = dispatchGranted
        ? "fresh-or-exact-replay"
        : "committed-only";
      await assertOperationHeld();
      await assertLifecycleHeld(lease, code);
      const completion = validateGenerationCompletion(
        await invokeAsyncCallback(
          publish,
          generationPublicationContext(read, plan, publicationMode, code),
          code,
        ),
        read,
        publicationMode,
        code,
      );
      await assertOperationHeld();
      await assertLifecycleHeld(lease, code);
      if (read.state !== "committed") {
        const expectedOperationRevision = ownDataValue(
          read.operation,
          "revision",
          code,
        );
        ensure(
          expectedOperationRevision === "1" || expectedOperationRevision === "2",
          code,
        );
        await invokeForRead(
          bindings.authority,
          "finalizeRestoreDestinationGeneration",
          [
            exactFrozenRecord({
              ...base,
              completion,
              expectedOperationRevision,
            }),
          ],
          code,
        );
      }
      const committed = await readGenerationOptional(
        bindings.authority,
        plan,
        admission,
        code,
      );
      ensure(committed !== null, code);
      validateCommittedGeneration(committed, completion, code);
      return exactFrozenRecord({ completion, generation: committed });
    },
    code,
  );
}

function validateGenerationLastPointer(session, generation, admission, code) {
  const last = session.document.lastOperation;
  const operation = generation.operation;
  const reservation = generation.reservation;
  ensure(
    pointerMatches(
      last,
      admission.request.operationId,
      RESTORE_DESTINATION_GENERATION_OPERATION_KIND,
    ) &&
      ownDataValue(last, "state", code) === "committed" &&
      ownDataValue(last, "operationRevision", code) ===
        ownDataValue(operation, "revision", code) &&
      ownDataValue(last, "requestSha256", code) ===
        ownDataValue(operation, "requestSha256", code) &&
      ownDataValue(last, "reservationId", code) ===
        ownDataValue(reservation, "reservationId", code) &&
      ownDataValue(last, "expectedSessionRevision", code) ===
        ownDataValue(reservation, "expectedSessionRevision", code) &&
      ownDataValue(reservation, "state", code) === "released",
    code,
  );
  return last;
}

function reconstructDetachExpectedSession(current, generation, plan, admission, code) {
  const active = current.document.activeOperation;
  ensure(
    active !== null &&
      pointerMatches(active, plan.detachOperationId, pointerKindForPlan(plan)) &&
      arrayIncludes(
        ["prepared", "starting", "uncertain"],
        ownDataValue(active, "state", code),
      ) &&
      current.document.launch === null,
    code,
  );
  const state = ownDataValue(active, "state", code);
  ensure(
    (state === "prepared" && current.document.lifecycle === "ATTACHED") ||
      (state !== "prepared" &&
        current.document.lifecycle ===
          (plan.detachMode === "release" ? "RELEASING" : "FENCING")),
    code,
  );
  const last = validateGenerationLastPointer(current, generation, admission, code);
  const preGeneration = ownDataValue(generation.operation, "expectedSession", code);
  const preDocument = ownDataValue(preGeneration, "document", code);
  const generationValue = generation.generation;
  ensure(
    ownDataValue(generationValue, "committedAt", code) ===
      ownDataValue(generation.operation, "updatedAt", code) &&
      sameData(current.document.manifest, preDocument.manifest, code) &&
      sameData(current.document.storageRef, preDocument.storageRef, code) &&
      sameData(
        ownDataValue(ownDataValue(generationValue, "binding", code), "attachment", code),
        preDocument.attachment,
        code,
      ) &&
      current.sessionId === preGeneration.sessionId &&
      current.createdAt === preGeneration.createdAt,
    code,
  );
  if (current.document.attachment !== null) {
    sameData(current.document.attachment, preDocument.attachment, code);
  }
  if (current.document.lease !== null) {
    sameData(current.document.lease, preDocument.lease, code);
  }
  const document = exactFrozenRecord({
    activeOperation: null,
    attachment: preDocument.attachment,
    backendCapabilities: preDocument.backendCapabilities,
    documentVersion: preDocument.documentVersion,
    lastOperation: last,
    launch: null,
    lease: preDocument.lease,
    lifecycle: "ATTACHED",
    manifest: preDocument.manifest,
    recovery: preDocument.recovery,
    storageRef: preDocument.storageRef,
    writerEpoch: preDocument.writerEpoch,
  });
  let reconstructed;
  try {
    reconstructed = assertSessionAuthoritySnapshot(
      exactFrozenRecord({
        createdAt: preGeneration.createdAt,
        document,
        revision: ownDataValue(active, "expectedSessionRevision", code),
        sessionId: preGeneration.sessionId,
        updatedAt: ownDataValue(generation.operation, "updatedAt", code),
      }),
    );
  } catch {
    fail(code);
  }
  return reconstructed;
}

function normalizeDetachTerminal(value, base, plan, code) {
  const receipt = frozenReceipt(value, code);
  const operation = operationMatches(
    ownDataValue(receipt, "operation", code),
    plan.detachOperationId,
    pointerKindForPlan(plan),
    code,
  );
  ensure(
    ownDataValue(operation, "state", code) === "committed" &&
      sameData(ownDataValue(operation, "expectedSession", code), base.expectedSession, code) &&
      sameData(
        ownDataValue(operation, "request", code),
        exactFrozenRecord({ contractVersion: 1, target: base.target }),
        code,
      ),
    code,
  );
  const result = ownDataValue(operation, "result", code);
  ensure(
    ownDataValue(result, "outcome", code) ===
      (plan.detachMode === "release" ? "writer-released" : "writer-fenced"),
    code,
  );
  const session = sessionSnapshot(ownDataValue(receipt, "session", code), code);
  ensure(
    session.document.lifecycle === "DETACHED" &&
      session.document.activeOperation === null &&
      pointerMatches(
        session.document.lastOperation,
        plan.detachOperationId,
        pointerKindForPlan(plan),
      ),
    code,
  );
  try {
    assertSessionOperationTransitionProof({
      operation,
      reservation: ownDataValue(receipt, "reservation", code),
      session,
    });
  } catch {
    fail(code);
  }
  return session;
}

async function detachWriter(bindings, generation, plan, admission, lease, code) {
  let current = generation.session;
  if (current.document.lifecycle === "DETACHED") {
    ensure(
      current.document.activeOperation === null &&
        pointerMatches(
          current.document.lastOperation,
          plan.detachOperationId,
          pointerKindForPlan(plan),
        ),
      code,
    );
    return assertThenFreeValue(current, code);
  }
  let expectedSession;
  if (current.document.activeOperation === null) {
    ensure(
      current.document.lifecycle === "ATTACHED" &&
        validateGenerationLastPointer(current, generation, admission, code),
      code,
    );
    expectedSession = current;
  } else {
    expectedSession = reconstructDetachExpectedSession(
      current,
      generation,
      plan,
      admission,
      code,
    );
  }
  const attachment = ownDataValue(
    ownDataValue(generation.generation, "binding", code),
    "attachment",
    code,
  );
  const base = exactFrozenRecord({
    expectedSession,
    operationId: plan.detachOperationId,
    target: exactFrozenRecord({
      attachmentId: ownDataValue(attachment, "attachmentId", code),
      kind: "attachment",
    }),
  });
  await assertLifecycleHeld(lease, code);
  const receipt = await invoke(
    bindings.writerDetach,
    plan.detachMode === "release" ? "detachWriter" : "forceFenceWriter",
    [base],
    code,
  );
  await assertLifecycleHeld(lease, code);
  return assertThenFreeValue(
    normalizeDetachTerminal(receipt, base, plan, code),
    code,
  );
}

function normalizeImageReservation(value, code) {
  const normalized = exactDataObject(value, IMAGE_RESERVATION_KEYS, code);
  return exactFrozenRecord({
    configBytes: normalized.configBytes,
    descriptor: normalized.descriptor,
    inspectCodex: trustedFunction(normalized.inspectCodex, code),
    reservation: normalized.reservation,
  });
}

async function prepareImage(bindings, plan, launchIntent, lease, code) {
  await assertLifecycleHeld(lease, code);
  const prepared = await invokeCallback(
    bindings.prepareImageReservation,
    exactFrozenRecord({
      imagePlanId: plan.imagePlanId,
      launchIntent,
      plan,
    }),
    code,
  );
  await assertLifecycleHeld(lease, code);
  return normalizeImageReservation(prepared, code);
}

function normalizeLaunchIntent(value, plan, code) {
  const launchIntent = frozenReceipt(value, code);
  ensure(
    ownDataValue(launchIntent, "launchAttemptId", code) ===
      plan.launchAttemptId,
    code,
  );
  ownDataValue(launchIntent, "measuredImage", code);
  ownDataValue(launchIntent, "supervisor", code);
  return launchIntent;
}

function activationOperationRequest(
  expectedSession,
  generation,
  plan,
  launchIntent,
  stopOperationId,
  code,
) {
  const attachment = ownDataValue(
    ownDataValue(generation.generation, "binding", code),
    "attachment",
    code,
  );
  let request;
  try {
    request = createRestoreAttachmentActivationOperationRequestV2({
      destinationRootPath: plan.destinationDirectory,
      expectedSession,
      generation: generation.generation,
      holderId: plan.holderId,
      launchIntent,
      leaseDurationMilliseconds: plan.leaseDurationMilliseconds,
      predecessor: exactFrozenRecord({
        attachmentId: ownDataValue(attachment, "attachmentId", code),
        captureOperationId: plan.captureOperationId,
        detachOperationId: plan.detachOperationId,
        stopOperationId,
      }),
    });
  } catch {
    fail(code);
  }
  return request;
}

function normalizeActivationClaim(value, plan, code) {
  const read = normalizeActivationReceipt(value, plan, code);
  const dispatchGranted = ownDataValue(value, "dispatchGranted", code);
  ensure(
    typeof dispatchGranted === "boolean" && read.state !== "prepared",
    code,
  );
  return exactFrozenRecord({ dispatchGranted, read });
}

function validateActivationLaunchTransitionChain(
  activationOperation,
  activationReservation,
  activationRequest,
  generation,
  launchOperation,
  launchAttempt,
  launchReservation,
  launchSession,
  code,
) {
  let expectedLaunchRequest;
  try {
    const launchExpectedSession = ownDataValue(
      launchOperation,
      "expectedSession",
      code,
    );
    const launchIntent = ownDataValue(
      activationRequest,
      "launchIntent",
      code,
    );
    expectedLaunchRequest = createWriterLaunchAttemptOperationRequest({
      expectedSession: launchExpectedSession,
      generation: generation.generation,
      measuredImage: ownDataValue(launchIntent, "measuredImage", code),
      supervisor: ownDataValue(launchIntent, "supervisor", code),
    });
    assertSessionOperationTransitionProof({
      operation: activationOperation,
      reservation: activationReservation,
      session: launchExpectedSession,
    });
    assertSessionOperationTransitionProof({
      operation: launchOperation,
      reservation: launchReservation,
      session: launchSession,
    });
  } catch {
    fail(code);
  }
  ensure(
    sameData(
      ownDataValue(launchOperation, "request", code),
      expectedLaunchRequest,
      code,
    ) &&
      sameData(
        ownDataValue(launchAttempt, "request", code),
        expectedLaunchRequest,
        code,
      ),
    code,
  );
  return expectedLaunchRequest;
}

function normalizeActivationHandoff(
  value,
  activation,
  generation,
  plan,
  code,
) {
  const receipt = frozenReceipt(value, code);
  const activationPart = ownDataValue(receipt, "activation", code);
  const activationOperation = operationMatches(
    ownDataValue(activationPart, "operation", code),
    plan.activationOperationId,
    RESTORE_ATTACHMENT_ACTIVATION_OPERATION_KIND,
    code,
  );
  ensure(
    ownDataValue(activationOperation, "state", code) === "committed" &&
      sameData(
        ownDataValue(activationOperation, "request", code),
        activation.request,
        code,
      ) &&
      sameData(ownDataValue(receipt, "generation", code), generation.generation, code),
    code,
  );
  const launchPart = ownDataValue(receipt, "launch", code);
  const launchOperation = operationMatches(
    ownDataValue(launchPart, "operation", code),
    plan.launchAttemptId,
    WRITER_LAUNCH_ATTEMPT_OPERATION_KIND,
    code,
  );
  const launchState = ownDataValue(launchOperation, "state", code);
  const launchAttempt = ownDataValue(launchPart, "attempt", code);
  ensure(
    arrayIncludes(["prepared", "starting", "uncertain", "committed"], launchState) &&
      ownDataValue(launchAttempt, "launchAttemptId", code) ===
        plan.launchAttemptId &&
      ownDataValue(receipt, "status", code) === launchState,
    code,
  );
  const launchSession = sessionSnapshot(
    ownDataValue(receipt, "session", code),
    code,
  );
  validateActivationLaunchTransitionChain(
    activationOperation,
    ownDataValue(activationPart, "reservation", code),
    activation.request,
    generation,
    launchOperation,
    launchAttempt,
    ownDataValue(launchPart, "reservation", code),
    launchSession,
    code,
  );
  return exactFrozenRecord({
    attempt: launchAttempt,
    operation: launchOperation,
    reservation: ownDataValue(launchPart, "reservation", code),
    session: launchSession,
    state: launchState,
  });
}

function reservationIdentityMatches(actual, expected, code) {
  for (
    let index = 0;
    index < RESERVATION_IDENTITY_KEYS.length;
    index += 1
  ) {
    const key = RESERVATION_IDENTITY_KEYS[index];
    ensure(
      sameData(
        ownDataValue(actual, key, code),
        ownDataValue(expected, key, code),
        code,
      ),
      code,
    );
  }
}

function normalizeLaunchRunResult(value, expected, plan, code) {
  const result = frozenReceipt(value, code);
  const operation = operationMatches(
    ownDataValue(result, "operation", code),
    plan.launchAttemptId,
    WRITER_LAUNCH_ATTEMPT_OPERATION_KIND,
    code,
  );
  const expectedOperation = expected.operation;
  const attempt = ownDataValue(result, "attempt", code);
  const reservation = ownDataValue(result, "reservation", code);
  const session = sessionSnapshot(ownDataValue(result, "session", code), code);
  ensure(
    ownDataValue(result, "contractVersion", code) === 1 &&
      ownDataValue(operation, "state", code) === "committed" &&
      ownDataValue(result, "status", code) === "started" &&
      sameData(
        ownDataValue(operation, "expectedSession", code),
        ownDataValue(expectedOperation, "expectedSession", code),
        code,
      ) &&
      sameData(
        ownDataValue(operation, "request", code),
        ownDataValue(expectedOperation, "request", code),
        code,
      ) &&
      ownDataValue(operation, "requestSha256", code) ===
        ownDataValue(expectedOperation, "requestSha256", code) &&
      ownDataValue(operation, "createdAt", code) ===
        ownDataValue(expectedOperation, "createdAt", code) &&
      ownDataValue(attempt, "launchAttemptId", code) === plan.launchAttemptId &&
      ownDataValue(attempt, "state", code) === "committed" &&
      sameData(
        ownDataValue(attempt, "request", code),
        ownDataValue(operation, "request", code),
        code,
      ) &&
      sameData(
        ownDataValue(attempt, "result", code),
        ownDataValue(operation, "result", code),
        code,
      ) &&
      sameData(
        ownDataValue(result, "evidence", code),
        ownDataValue(ownDataValue(operation, "result", code), "evidence", code),
        code,
      ) &&
      session.document.activeOperation === null &&
      pointerMatches(
        session.document.lastOperation,
        plan.launchAttemptId,
        WRITER_LAUNCH_ATTEMPT_OPERATION_KIND,
      ) &&
      session.document.launch !== null &&
      ownDataValue(session.document.launch, "launchAttemptId", code) ===
        plan.launchAttemptId &&
      sameData(
        ownDataValue(result, "launch", code),
        session.document.launch,
        code,
      ),
    code,
  );
  reservationIdentityMatches(reservation, expected.reservation, code);
  try {
    assertSessionOperationTransitionProof({
      operation,
      reservation,
      session,
    });
  } catch {
    fail(code);
  }
  return result;
}

async function claimActivation(
  bindings,
  base,
  plan,
  lease,
  existing,
  code,
) {
  const claimed = await runOperationGuard(
    bindings.operationGuard,
    base.operationId,
    async (assertOperationHeld) => {
      let read = existing;
      if (read === null) {
        await assertOperationHeld();
        await assertLifecycleHeld(lease, code);
        normalizeReservedOperation(
          await invoke(bindings.authority, "reserveOperation", [base], code),
          base,
          code,
        );
        read = null;
      }
      if (read === null || read.state === "prepared") {
        await assertOperationHeld();
        await assertLifecycleHeld(lease, code);
        const settled = await invokeForRead(
          bindings.authority,
          "claimRestoreAttachmentActivationDispatch",
          [
            exactFrozenRecord({
              ...base,
              expectedOperationRevision: "0",
            }),
          ],
          code,
        );
        if (settled.ok) {
          const claim = normalizeActivationClaim(settled.value, plan, code);
          read = claim.read;
        } else {
          read = await readActivationOptional(bindings.authority, plan, code);
          ensure(read !== null && read.state !== "prepared", code);
        }
      }
      ensure(
        read !== null &&
          sameData(read.operation.expectedSession, base.expectedSession, code) &&
          sameData(read.request, base.request, code),
        code,
      );
      return read;
    },
    code,
  );
  return assertThenFreeValue(claimed, code);
}

async function runActivationAndLaunch(
  bindings,
  generation,
  detachedSession,
  plan,
  stopOperationId,
  activationRead,
  launchRead,
  lease,
  code,
) {
  ensure(launchRead === null || activationRead !== null, code);

  let activation = activationRead;
  let launchIntent;
  let imageReservation = null;
  if (activation === null) {
    ensure(
      detachedSession.document.lifecycle === "DETACHED" &&
        detachedSession.document.activeOperation === null &&
        pointerMatches(
          detachedSession.document.lastOperation,
          plan.detachOperationId,
          pointerKindForPlan(plan),
        ),
      code,
    );
    imageReservation = await prepareImage(bindings, plan, null, lease, code);
    launchIntent = normalizeLaunchIntent(
      await invoke(
        bindings.launcher,
        "prepareLaunchIntent",
        [
          exactFrozenRecord({
            expectedSession: detachedSession,
            imageReservation,
            launchAttemptId: plan.launchAttemptId,
          }),
        ],
        code,
      ),
      plan,
      code,
    );
  } else {
    launchIntent = normalizeLaunchIntent(
      ownDataValue(activation.request, "launchIntent", code),
      plan,
      code,
    );
  }

  let persistedExpectedSession = null;
  let persistedRequest = null;
  if (activation !== null) {
    persistedExpectedSession = sessionSnapshot(
      ownDataValue(activation.operation, "expectedSession", code),
      code,
    );
    persistedRequest = activationOperationRequest(
      persistedExpectedSession,
      generation,
      plan,
      launchIntent,
      stopOperationId,
      code,
    );
    ensure(
      sameData(activation.request, persistedRequest, code) &&
        sameData(activation.generation, generation.generation, code),
      code,
    );
  }

  if (launchRead !== null) {
    ensure(activation.state === "committed", code);
    validateActivationLaunchTransitionChain(
      activation.operation,
      activation.reservation,
      activation.request,
      generation,
      launchRead.operation,
      launchRead.attempt,
      launchRead.reservation,
      launchRead.session,
      code,
    );
    imageReservation = await prepareImage(
      bindings,
      plan,
      launchIntent,
      lease,
      code,
    );
    normalizeLaunchRunResult(
      await invoke(
        bindings.launcher,
        "runPreparedLaunch",
        [exactFrozenRecord({ imageReservation, launchAttemptId: plan.launchAttemptId })],
        code,
      ),
      launchRead,
      plan,
      code,
    );
    return;
  }

  const expectedSession =
    activation === null
      ? detachedSession
      : persistedExpectedSession;
  const request =
    activation === null
      ? activationOperationRequest(
          expectedSession,
          generation,
          plan,
          launchIntent,
          stopOperationId,
          code,
        )
      : persistedRequest;
  const base = exactFrozenRecord({
    expectedSession,
    kind: RESTORE_ATTACHMENT_ACTIVATION_OPERATION_KIND,
    operationId: plan.activationOperationId,
    request,
  });
  activation = await claimActivation(
    bindings,
    base,
    plan,
    lease,
    activation,
    code,
  );
  await assertLifecycleHeld(lease, code);
  const candidateState =
    activation.state === "starting" ? "starting" : "uncertain";
  const handoff = normalizeActivationHandoff(
    await invoke(
      bindings.restoreActivationCoordinator,
      "reconcileRestoreAttachmentActivation",
      [
        exactFrozenRecord({
          activationOperationId: plan.activationOperationId,
          request: activation.request,
          state: candidateState,
        }),
      ],
      code,
    ),
    activation,
    generation,
    plan,
    code,
  );
  await assertLifecycleHeld(lease, code);
  if (imageReservation === null) {
    imageReservation = await prepareImage(
      bindings,
      plan,
      launchIntent,
      lease,
      code,
    );
  }
  normalizeLaunchRunResult(
    await invoke(
      bindings.launcher,
      "runPreparedLaunch",
      [exactFrozenRecord({ imageReservation, launchAttemptId: plan.launchAttemptId })],
      code,
    ),
    handoff,
    plan,
    code,
  );
}

async function executeForegroundRestore(
  bindings,
  admission,
  publish,
  lease,
  code,
) {
  await assertLifecycleHeld(lease, code);
  let current = await readSession(
    bindings.authority,
    admission.request.sessionId,
    code,
  );
  const plan = await invokeCallback(
    bindings.resolveStablePlan,
    exactFrozenRecord({ admission, expectedSession: current }),
    code,
  );
  ensure(
    isPostgresDetachedRestorePlan(plan) &&
      sameData(plan.request, admission.request, code),
    code,
  );
  const tuple = captureSafetyTuple(plan, current, admission, code);
  let capture = await readCaptureOptional(bindings.authority, tuple, code);
  let generation = await readGenerationOptional(
    bindings.authority,
    plan,
    admission,
    code,
  );

  let stopOperationId = capture?.stopOperationId ?? null;
  const active = current.document.activeOperation;
  const last = current.document.lastOperation;
  if (
    capture === null &&
    current.document.lifecycle === "ATTACHED" &&
    current.document.attachment !== null &&
    current.document.lease !== null &&
    current.document.launch !== null
  ) {
    stopOperationId = deriveCurrentStopOperationId(current, tuple, code);
  }
  if (
    active !== null &&
    ownDataValue(active, "kind", code) === WRITER_LAUNCH_STOP_OPERATION_KIND
  ) {
    // No public stop-token read can prove or resume this dispatch boundary.
    fail(code);
  }
  if (
    generation === null &&
    pointerMatches(
      active,
      plan.request.operationId,
      RESTORE_DESTINATION_GENERATION_OPERATION_KIND,
    )
  ) {
    // V1 typed generation reads deliberately reject prepared operations.
    fail(code);
  }

  const existingWorkflow =
    capture !== null ||
    generation !== null ||
    pointerIsWorkflow(active, plan, stopOperationId) ||
    pointerIsWorkflow(last, plan, stopOperationId);
  if (!existingWorkflow) {
    ensure(active === null && stopOperationId !== null, code);
    const capability = await invokeCallback(
      bindings.fleetCapabilityGate,
      exactFrozenRecord({ admission, plan }),
      "postgres_detached_restore_fleet_capability_required",
    );
    ensure(
      capability === POSTGRES_DETACHED_RESTORE_FLEET_CONFIRMED,
      "postgres_detached_restore_fleet_capability_required",
    );
    current = await renewFreshSession(
      bindings.authority,
      plan,
      current,
      lease,
      code,
    );
  } else if (capture === null && generation === null) {
    // A renewal pointer does not expose the original expected-session or
    // request digest. Without a typed renewal read, this cold cut cannot be
    // bound back to the plan and must not authorize a stop.
    fail(code);
  }

  let captureCompletion;
  if (generation === null) {
    captureCompletion = await continueCapture(
      bindings,
      tuple,
      plan,
      current,
      capture,
      stopOperationId,
      lease,
      code,
    );
    capture = captureCompletion.capture;
    stopOperationId = captureCompletion.stopOperationId;
  } else {
    ensure(
      capture !== null &&
        capture.state === "committed" &&
        capture.stopOperationId === stopOperationId,
      code,
    );
  }

  const generationResult = await runGeneration(
    bindings,
    plan,
    admission,
    capture,
    generation,
    publish,
    lease,
    code,
  );
  generation = generationResult.generation;
  const activation = await readActivationOptional(
    bindings.authority,
    plan,
    code,
  );
  const launch = await readLaunchOptional(bindings.authority, plan, code);
  let detachedSession;
  if (activation !== null || launch !== null) {
    ensure(activation !== null, code);
    detachedSession = sessionSnapshot(
      ownDataValue(activation.operation, "expectedSession", code),
      code,
    );
  } else {
    detachedSession = await detachWriter(
      bindings,
      generation,
      plan,
      admission,
      lease,
      code,
    );
  }
  await runActivationAndLaunch(
    bindings,
    generation,
    detachedSession,
    plan,
    stopOperationId,
    activation,
    launch,
    lease,
    code,
  );
  await assertLifecycleHeld(lease, code);
  return assertThenFreeValue(generationResult.completion, code);
}

function createBindings(options, code) {
  ensure(isPostgresRestoreLifecycleGuard(options.lifecycleGuard), code);
  ensure(isPostgresOperationGuard(options.operationGuard), code);
  ensure(
    objectIsFrozen(haveDistinctLifecycleOperationGuardPoolsIntrinsic) &&
      callIntrinsic(
        haveDistinctLifecycleOperationGuardPoolsIntrinsic,
        undefined,
        [options.lifecycleGuard, options.operationGuard],
      ),
    code,
  );
  const captureBackend = collaborator(
    options.captureBackend,
    CAPTURE_BACKEND_METHODS,
    code,
  );
  preflightCollaboratorData(
    captureBackend.receiver,
    CAPTURE_BACKEND_DATA_KEYS,
    code,
  );
  try {
    assertPreparedCheckpointCaptureBackend(captureBackend.receiver);
    assertCheckpointCaptureReconciliationBackend(captureBackend.receiver);
  } catch {
    fail(code);
  }
  return exactFrozenRecord({
    authority: collaborator(options.authority, AUTHORITY_METHODS, code),
    captureBackend,
    durableStopCapture: collaborator(
      options.durableStopCapture,
      DURABLE_CAPTURE_METHODS,
      code,
    ),
    fleetCapabilityGate: trustedFunction(options.fleetCapabilityGate, code),
    launcher: collaborator(options.launcher, LAUNCHER_METHODS, code),
    lifecycle: collaborator(options.lifecycleGuard, LIFECYCLE_METHODS, code),
    operationGuard: options.operationGuard,
    prepareImageReservation: trustedFunction(
      options.prepareImageReservation,
      code,
    ),
    resolveStablePlan: trustedFunction(options.resolveStablePlan, code),
    restoreActivationCoordinator: collaborator(
      options.restoreActivationCoordinator,
      ACTIVATION_COORDINATOR_METHODS,
      code,
    ),
    writerDetach: collaborator(options.writerDetach, WRITER_DETACH_METHODS, code),
  });
}

export function isPostgresDetachedRestoreForegroundComposition(value) {
  try {
    return (
      arguments.length === 1 &&
      value !== null &&
      typeof value === "object" &&
      !isProxyValue(value) &&
      weakSetHas(facades, value)
    );
  } catch {
    return false;
  }
}

export function createPostgresDetachedRestoreForegroundComposition(...args) {
  const optionCode =
    "invalid_postgres_detached_restore_foreground_composition_options";
  ensure(args.length === 1, optionCode);
  const options = exactDataObject(args[0], OPTION_KEYS, optionCode);
  const bindings = createBindings(options, optionCode);
  const requestCode =
    "invalid_postgres_detached_restore_foreground_composition_request";
  const outcomeCode =
    "postgres_detached_restore_foreground_composition_outcome_uncertain";

  async function runRestoreInternal(runArgs) {
    ensure(runArgs.length === 2, requestCode);
    const admission = normalizeAdmission(runArgs[0], requestCode);
    const publish = trustedFunction(runArgs[1], requestCode);
    let callbackInternalError = null;
    try {
      const lifecycleResult = await invoke(
        bindings.lifecycle,
        "runForeground",
        [
          async (lease, completeValue) => {
            let complete;
            try {
              complete = trustedFunction(completeValue, outcomeCode);
              ensure(objectIsFrozen(completeValue), outcomeCode);
              const completion = await executeForegroundRestore(
                bindings,
                admission,
                publish,
                lease,
                outcomeCode,
              );
              return assertThenFreeValue(
                callIntrinsic(complete, undefined, [completion]),
                outcomeCode,
              );
            } catch (error) {
              if (weakSetHas(internalErrors, error)) {
                callbackInternalError = error;
                if (
                  error.code ===
                  "postgres_detached_restore_fleet_capability_required"
                ) {
                  return assertThenFreeValue(
                    callIntrinsic(complete, undefined, [
                      fleetFailureCarrier,
                    ]),
                    outcomeCode,
                  );
                }
              }
              throw error;
            }
          },
        ],
        outcomeCode,
      );
      if (lifecycleResult === fleetFailureCarrier) {
        throw callbackInternalError;
      }
      return assertThenFreeValue(lifecycleResult, outcomeCode);
    } catch (error) {
      if (callbackInternalError !== null) throw callbackInternalError;
      throw error;
    }
  }

  const runRestore = function runRestore(...runArgs) {
    return protectPublicPromise(
      runRestoreInternal(runArgs),
      outcomeCode,
    );
  };
  objectFreeze(runRestore);
  const facade = exactFrozenRecord({
    restoreContextContractVersion: 3,
    runRestore,
  });
  weakSetAdd(facades, facade);
  return facade;
}

objectFreeze(PostgresDetachedRestoreForegroundCompositionError.prototype);
objectFreeze(PostgresDetachedRestoreForegroundCompositionError);
