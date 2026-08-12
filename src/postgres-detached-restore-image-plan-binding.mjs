import { Buffer } from "node:buffer";
import { types as utilTypes } from "node:util";

import {
  MAX_IMAGE_CONFIG_BYTES,
  MAX_PLATFORM_MANIFEST_BYTES,
  PlatformImageReservationCoordinator,
  PlatformImageReservationError,
} from "./platform-image-reservation.mjs";
import {
  isPostgresDetachedRestorePlan,
} from "./postgres-detached-restore-plan.mjs";
import {
  assertSessionManifest,
} from "./session-storage-contracts.mjs";
import {
  isPhysicalCollaboratorSettlement,
} from "./physical-collaborator-settlement.mjs";

const arrayIncludesIntrinsic = Array.prototype.includes;
const arrayIsArray = Array.isArray;
const bufferAllocUnsafe = Buffer.allocUnsafe;
const bufferIsBuffer = Buffer.isBuffer;
const objectCreate = Object.create;
const objectDefineProperties = Object.defineProperties;
const objectDefineProperty = Object.defineProperty;
const objectFreeze = Object.freeze;
const objectGetOwnPropertyDescriptor = Object.getOwnPropertyDescriptor;
const objectGetPrototypeOf = Object.getPrototypeOf;
const objectHasOwn = Object.hasOwn;
const objectIsFrozen = Object.isFrozen;
const objectPrototype = Object.prototype;
const PlatformImageReservationCoordinatorConstructor =
  PlatformImageReservationCoordinator;
const platformConsumeReservationIntrinsic =
  PlatformImageReservationCoordinator.prototype.consumeReservation;
const platformReserveImageIntrinsic =
  PlatformImageReservationCoordinator.prototype.reservePlatformImage;
const platformRevalidateReservationIntrinsic =
  PlatformImageReservationCoordinator.prototype.revalidateReservation;
const PromiseConstructor = Promise;
const promisePrototype = Promise.prototype;
const promiseResolveIntrinsic = Promise.resolve;
const promiseSpeciesSymbol = Symbol.species;
const promiseThenIntrinsic = Promise.prototype.then;
const reflectApply = Reflect.apply;
const reflectConstruct = Reflect.construct;
const reflectOwnKeys = Reflect.ownKeys;
const regexpExecIntrinsic = RegExp.prototype.exec;
const typedArrayPrototype = objectGetPrototypeOf(Uint8Array.prototype);
const typedArrayByteLengthGetter = objectGetOwnPropertyDescriptor(
  typedArrayPrototype,
  "byteLength",
).get;
const typedArraySetIntrinsic = objectGetOwnPropertyDescriptor(
  typedArrayPrototype,
  "set",
).value;
const TypeErrorConstructor = TypeError;
const weakMapGetIntrinsic = WeakMap.prototype.get;
const weakMapSetIntrinsic = WeakMap.prototype.set;
const WeakMapConstructor = WeakMap;
const weakSetAddIntrinsic = WeakSet.prototype.add;
const weakSetHasIntrinsic = WeakSet.prototype.has;
const WeakSetConstructor = WeakSet;
const {
  isGeneratorFunction: isGeneratorFunctionValue,
  isGeneratorObject: isGeneratorObjectValue,
  isPromise: isPromiseValue,
  isProxy: isProxyValue,
  isUint8Array: isUint8ArrayValue,
} = utilTypes;

export const POSTGRES_DETACHED_RESTORE_IMAGE_PLAN_PROVIDER_CONTRACT_VERSION =
  2;
export const POSTGRES_DETACHED_RESTORE_IMAGE_PLAN_BINDING_CONTRACT_VERSION = 1;

const FACTORY_OPTION_KEYS = objectFreeze([
  "provider",
  "settlement",
]);
const PROVIDER_KEYS = objectFreeze([
  "contractVersion",
  "imagePlanProviderId",
  "inspectCodex",
  "resolveImagePlan",
]);
const SETTLEMENT_KEYS = objectFreeze(["inspectCodex", "resolveImagePlan"]);
const SETTLEMENT_CONTEXT_KEYS = objectFreeze(["invocation", "signal"]);
const SETTLEMENT_RESULT_KEYS = objectFreeze([
  "contractVersion",
  "invocation",
  "outcome",
  "value",
]);
const PREPARE_INPUT_KEYS = objectFreeze(["plan", "sessionManifest"]);
const RESOLVER_RESULT_KEYS = objectFreeze(["configBytes", "descriptor"]);
const DESCRIPTOR_KEYS = objectFreeze([
  "bytes",
  "digest",
  "mediaType",
  "size",
]);
const INSPECTION_MEASUREMENT_KEYS = objectFreeze([
  "codexBinaryPath",
  "codexBinarySha256",
  "codexVersion",
]);
const OPAQUE_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u;

const ERROR_MESSAGES = objectFreeze({
  invalid_postgres_detached_restore_image_plan_binding_options:
    "PostgreSQL detached restore image-plan binding options are invalid",
  invalid_postgres_detached_restore_image_plan_binding_request:
    "PostgreSQL detached restore image-plan binding request is invalid",
  postgres_detached_restore_image_plan_inspection_uncertain:
    "PostgreSQL detached restore image-plan inspection is uncertain",
  postgres_detached_restore_image_plan_reservation_rejected:
    "PostgreSQL detached restore image-plan reservation was rejected",
  postgres_detached_restore_image_plan_resolution_uncertain:
    "PostgreSQL detached restore image-plan resolution is uncertain",
});

const bindingBrands = new WeakSetConstructor();
const errorBrands = new WeakSetConstructor();
const protectedPromiseBrands = new WeakSetConstructor();
const reservationBrands = new WeakSetConstructor();
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

function weakMapGet(value, key) {
  return callIntrinsic(weakMapGetIntrinsic, value, [key]);
}

function weakMapSet(value, key, entry) {
  callIntrinsic(weakMapSetIntrinsic, value, [key, entry]);
}

function weakSetAdd(value, entry) {
  callIntrinsic(weakSetAddIntrinsic, value, [entry]);
}

function weakSetHas(value, entry) {
  return callIntrinsic(weakSetHasIntrinsic, value, [entry]);
}

function regexpTest(pattern, value) {
  return callIntrinsic(regexpExecIntrinsic, pattern, [value]) !== null;
}

function typedArrayByteLength(value) {
  return callIntrinsic(typedArrayByteLengthGetter, value, []);
}

function typedArraySet(value, source) {
  callIntrinsic(typedArraySetIntrinsic, value, [source]);
}

export class PostgresDetachedRestoreImagePlanBindingError extends TypeErrorConstructor {
  constructor(code) {
    if (typeof code !== "string" || !objectHasOwn(ERROR_MESSAGES, code)) {
      throw new TypeErrorConstructor(
        "Unsupported PostgreSQL detached restore image-plan binding error",
      );
    }
    const message = ERROR_MESSAGES[code];
    super(message);
    objectDefineProperties(this, {
      code: {
        configurable: false,
        enumerable: true,
        value: code,
        writable: false,
      },
      name: {
        configurable: false,
        enumerable: false,
        value: "PostgresDetachedRestoreImagePlanBindingError",
        writable: false,
      },
      retryable: {
        configurable: false,
        enumerable: true,
        value: false,
        writable: false,
      },
      stack: {
        configurable: false,
        enumerable: false,
        value: `PostgresDetachedRestoreImagePlanBindingError: ${message}`,
        writable: false,
      },
    });
    objectFreeze(this);
  }
}

function makeError(code) {
  const error = new PostgresDetachedRestoreImagePlanBindingError(code);
  weakSetAdd(errorBrands, error);
  return error;
}

function fail(code) {
  throw makeError(code);
}

function ensure(condition, code) {
  if (!condition) fail(code);
}

function isInternalError(value) {
  return (
    value !== null &&
    typeof value === "object" &&
    !isProxyValue(value) &&
    weakSetHas(errorBrands, value)
  );
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

function exactDataObject(
  value,
  expectedKeys,
  code,
  frozen = false,
  nullPrototype = false,
) {
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
    (nullPrototype
      ? prototype === null
      : prototype === objectPrototype || prototype === null) &&
      (!frozen || objectIsFrozen(value)) &&
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
      configurable: false,
      enumerable: true,
      value: descriptor.value,
      writable: false,
    });
  }
  for (let index = 0; index < expectedKeys.length; index += 1) {
    ensure(objectHasOwn(normalized, expectedKeys[index]), code);
  }
  return objectFreeze(normalized);
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

function exactNativePromise(value, code) {
  ensure(
    isPromiseValue(value) &&
      !isProxyValue(value) &&
      !isGeneratorObjectValue(value),
    code,
  );
  let prototype;
  let ownCatch;
  let ownConstructor;
  let ownFinally;
  let ownThen;
  try {
    prototype = objectGetPrototypeOf(value);
    ownCatch = objectGetOwnPropertyDescriptor(value, "catch");
    ownConstructor = objectGetOwnPropertyDescriptor(value, "constructor");
    ownFinally = objectGetOwnPropertyDescriptor(value, "finally");
    ownThen = objectGetOwnPropertyDescriptor(value, "then");
  } catch {
    fail(code);
  }
  if (weakSetHas(protectedPromiseBrands, value)) {
    ensure(
      prototype === promisePrototype &&
        exactProtectedPromiseDescriptor(ownCatch, protectedPromiseCatch) &&
        exactProtectedPromiseDescriptor(
          ownConstructor,
          promiseSpeciesHolder,
        ) &&
        exactProtectedPromiseDescriptor(
          ownFinally,
          protectedPromiseFinally,
        ) &&
        exactProtectedPromiseDescriptor(ownThen, protectedPromiseThen),
      code,
    );
    return value;
  }
  ensure(
    prototype === promisePrototype &&
      ownConstructor === undefined &&
      ownThen === undefined,
    code,
  );
  return value;
}

function exactProtectedPromiseDescriptor(descriptor, expectedValue) {
  return (
    descriptor !== undefined &&
    descriptor.configurable === false &&
    descriptor.enumerable === false &&
    descriptor.value === expectedValue &&
    descriptor.writable === false
  );
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
      callIntrinsic(protectedPromiseThen, runFinally(), [() => value]),
    (reason) =>
      callIntrinsic(protectedPromiseThen, runFinally(), [
        () => {
          throw reason;
        },
      ]),
  ]);
}

function protectPromise(value) {
  if (!isPromiseValue(value)) return value;
  if (weakSetHas(protectedPromiseBrands, value)) {
    return exactNativePromise(
      value,
      "postgres_detached_restore_image_plan_resolution_uncertain",
    );
  }
  try {
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
    weakSetAdd(protectedPromiseBrands, value);
  } catch {
    fail("postgres_detached_restore_image_plan_resolution_uncertain");
  }
  return value;
}

function copyBoundedBytes(value, maximum, code) {
  ensure(
    !isProxyValue(value) &&
      (bufferIsBuffer(value) || isUint8ArrayValue(value)),
    code,
  );
  let sourceLength;
  try {
    sourceLength = typedArrayByteLength(value);
  } catch {
    fail(code);
  }
  ensure(sourceLength > 0 && sourceLength <= maximum, code);
  let copy;
  try {
    copy = bufferAllocUnsafe(sourceLength);
    typedArraySet(copy, value);
    ensure(
      typedArrayByteLength(copy) === sourceLength &&
        typedArrayByteLength(value) === sourceLength,
      code,
    );
  } catch (error) {
    if (isInternalError(error)) throw error;
    fail(code);
  }
  return copy;
}

function normalizeResolvedImage(value, code) {
  const result = exactDataObject(
    value,
    RESOLVER_RESULT_KEYS,
    code,
    true,
    true,
  );
  const descriptor = exactDataObject(
    result.descriptor,
    DESCRIPTOR_KEYS,
    code,
    true,
  );
  return exactFrozenRecord({
    configBytes: copyBoundedBytes(
      result.configBytes,
      MAX_IMAGE_CONFIG_BYTES,
      code,
    ),
    descriptor: exactFrozenRecord({
      bytes: copyBoundedBytes(
        descriptor.bytes,
        MAX_PLATFORM_MANIFEST_BYTES,
        code,
      ),
      digest: descriptor.digest,
      mediaType: descriptor.mediaType,
      size: descriptor.size,
    }),
  });
}

function normalizePrepareInput(value, code) {
  const input = exactDataObject(value, PREPARE_INPUT_KEYS, code);
  ensure(isPostgresDetachedRestorePlan(input.plan), code);
  let sessionManifest;
  try {
    sessionManifest = assertSessionManifest(input.sessionManifest);
  } catch {
    fail(code);
  }
  ensure(sessionManifest.sessionId === input.plan.request.sessionId, code);
  return exactFrozenRecord({ plan: input.plan, sessionManifest });
}

function normalizeProvider(value, code) {
  const provider = exactDataObject(value, PROVIDER_KEYS, code, true);
  ensure(
    provider.contractVersion ===
      POSTGRES_DETACHED_RESTORE_IMAGE_PLAN_PROVIDER_CONTRACT_VERSION &&
      typeof provider.imagePlanProviderId === "string" &&
      regexpTest(OPAQUE_ID_PATTERN, provider.imagePlanProviderId),
    code,
  );
  return exactFrozenRecord({
    contractVersion: provider.contractVersion,
    imagePlanProviderId: provider.imagePlanProviderId,
    inspectCodex: trustedFunction(provider.inspectCodex, code),
    resolveImagePlan: trustedFunction(provider.resolveImagePlan, code),
  });
}

function normalizeSettlement(value, code) {
  const settlement = exactDataObject(value, SETTLEMENT_KEYS, code, true);
  ensure(
    isPhysicalCollaboratorSettlement(settlement.inspectCodex) &&
      isPhysicalCollaboratorSettlement(settlement.resolveImagePlan) &&
      settlement.inspectCodex !== settlement.resolveImagePlan,
    code,
  );
  const inspectCodexInvoke = objectGetOwnPropertyDescriptor(
    settlement.inspectCodex,
    "invoke",
  );
  const resolveImagePlanInvoke = objectGetOwnPropertyDescriptor(
    settlement.resolveImagePlan,
    "invoke",
  );
  ensure(
    inspectCodexInvoke !== undefined &&
      objectHasOwn(inspectCodexInvoke, "value") &&
      resolveImagePlanInvoke !== undefined &&
      objectHasOwn(resolveImagePlanInvoke, "value"),
    code,
  );
  return exactFrozenRecord({
    inspectCodex: trustedFunction(inspectCodexInvoke.value, code),
    resolveImagePlan: trustedFunction(resolveImagePlanInvoke.value, code),
  });
}

function plainNativePromiseBridge(protectedPending, code) {
  let bridge;
  try {
    bridge = new PromiseConstructor((resolve, reject) => {
      callIntrinsic(promiseThenIntrinsic, protectedPending, [
        (value) => {
          callIntrinsic(resolve, undefined, [value]);
        },
        (error) => {
          callIntrinsic(reject, undefined, [error]);
        },
      ]);
    });
  } catch {
    fail(code);
  }
  return exactNativePromise(bridge, code);
}

function invokeSettledProvider(
  invokeSettlement,
  callback,
  request,
  normalize,
  code,
) {
  const start = objectFreeze(function start(contextValue) {
    const context = exactDataObject(
      contextValue,
      SETTLEMENT_CONTEXT_KEYS,
      code,
      true,
      true,
    );
    const providerRequest = exactFrozenRecord({
      ...request,
      invocation: context.invocation,
      signal: context.signal,
    });
    return callIntrinsic(callback, undefined, [providerRequest]);
  });
  let pending;
  try {
    pending = callIntrinsic(invokeSettlement, undefined, [
      exactFrozenRecord({ start }),
    ]);
  } catch {
    fail(code);
  }
  try {
    return protectPromise(
      callIntrinsic(protectedPromiseThen, pending, [
        (value) => {
          const result = exactDataObject(
            value,
            SETTLEMENT_RESULT_KEYS,
            code,
            true,
            true,
          );
          ensure(
            result.contractVersion === 1 &&
              result.invocation !== null &&
              typeof result.invocation === "object" &&
              !isProxyValue(result.invocation) &&
              result.outcome === "success",
            code,
          );
          return normalize(result.value, code);
        },
        () => fail(code),
      ]),
    );
  } catch {
    fail(code);
  }
}

function normalizeInspectionMeasurement(value, code) {
  const measurement = exactDataObject(
    value,
    INSPECTION_MEASUREMENT_KEYS,
    code,
    true,
    true,
  );
  return exactFrozenRecord({
    codexBinaryPath: measurement.codexBinaryPath,
    codexBinarySha256: measurement.codexBinarySha256,
    codexVersion: measurement.codexVersion,
  });
}

function isPlatformError(value) {
  if (
    value === null ||
    typeof value !== "object" ||
    isProxyValue(value)
  ) {
    return false;
  }
  try {
    return objectGetPrototypeOf(value) === PlatformImageReservationError.prototype;
  } catch {
    return false;
  }
}

function translatePlatformError(error, fallbackCode) {
  if (!isPlatformError(error)) {
    if (isInternalError(error)) throw error;
    fail(fallbackCode);
  }
  if (error.code === "platform_image_inspection_uncertain") {
    fail("postgres_detached_restore_image_plan_inspection_uncertain");
  }
  if (error.code === "platform_image_reservation_rejected") {
    fail("postgres_detached_restore_image_plan_reservation_rejected");
  }
  fail(fallbackCode);
}

function makeOpaqueReservation() {
  const reservation = objectFreeze(objectCreate(null));
  weakSetAdd(reservationBrands, reservation);
  return reservation;
}

function invokeCoordinator(coordinator, method, input) {
  try {
    return callIntrinsic(method, coordinator, [input]);
  } catch (error) {
    translatePlatformError(
      error,
      "postgres_detached_restore_image_plan_reservation_rejected",
    );
  }
}

/**
 * Creates one branded image-plan authority around one private reservation
 * coordinator. Construction validates only local data and performs no I/O.
 */
export function createPostgresDetachedRestoreImagePlanBinding(...args) {
  const optionCode =
    "invalid_postgres_detached_restore_image_plan_binding_options";
  ensure(args.length === 1, optionCode);
  const options = exactDataObject(args[0], FACTORY_OPTION_KEYS, optionCode);
  const provider = normalizeProvider(options.provider, optionCode);
  const settlement = normalizeSettlement(options.settlement, optionCode);
  const coordinator = reflectConstruct(
    PlatformImageReservationCoordinatorConstructor,
    [],
  );
  const records = new WeakMapConstructor();

  async function prepareInternal(methodArgs) {
    const requestCode =
      "invalid_postgres_detached_restore_image_plan_binding_request";
    ensure(methodArgs.length === 1, requestCode);
    const input = normalizePrepareInput(methodArgs[0], requestCode);
    const imagePlanId = input.plan.imagePlanId;
    const resolverRequest = exactFrozenRecord({
      imagePlanId,
      imagePlanProviderId: provider.imagePlanProviderId,
      sessionManifest: input.sessionManifest,
    });
    let resolvedValue;
    try {
      resolvedValue = await invokeSettledProvider(
        settlement.resolveImagePlan,
        provider.resolveImagePlan,
        resolverRequest,
        normalizeResolvedImage,
        "postgres_detached_restore_image_plan_resolution_uncertain",
      );
    } catch (error) {
      if (isInternalError(error)) throw error;
      fail("postgres_detached_restore_image_plan_resolution_uncertain");
    }
    const resolved = resolvedValue;
    const inspectCodex = objectFreeze(
      function inspectCodex(inspection) {
        const code =
          "postgres_detached_restore_image_plan_inspection_uncertain";
        return plainNativePromiseBridge(
          invokeSettledProvider(
            settlement.inspectCodex,
            provider.inspectCodex,
            exactFrozenRecord({
              imagePlanId,
              imagePlanProviderId: provider.imagePlanProviderId,
              inspection,
            }),
            normalizeInspectionMeasurement,
            code,
          ),
          code,
        );
      },
    );
    let internalReservation;
    try {
      internalReservation = await invokeCoordinator(
        coordinator,
        platformReserveImageIntrinsic,
        exactFrozenRecord({
          configBytes: resolved.configBytes,
          descriptor: resolved.descriptor,
          inspectCodex,
          sessionManifest: input.sessionManifest,
        }),
      );
    } catch (error) {
      translatePlatformError(
        error,
        "postgres_detached_restore_image_plan_resolution_uncertain",
      );
    }
    const reservation = makeOpaqueReservation();
    weakMapSet(records, reservation, {
      configBytes: resolved.configBytes,
      descriptor: resolved.descriptor,
      inspectCodex,
      reservation: internalReservation.reservation,
      state: "issued",
    });
    return reservation;
  }

  async function useReservation(methodArgs, consume) {
    const requestCode =
      "invalid_postgres_detached_restore_image_plan_binding_request";
    ensure(methodArgs.length === 1, requestCode);
    const reservation = methodArgs[0];
    ensure(
      reservation !== null &&
        typeof reservation === "object" &&
        !isProxyValue(reservation) &&
        weakSetHas(reservationBrands, reservation),
      requestCode,
    );
    const record = weakMapGet(records, reservation);
    if (record?.state === "using") record.state = "revoked";
    ensure(
      record !== undefined && record.state === "issued",
      "postgres_detached_restore_image_plan_reservation_rejected",
    );
    record.state = "using";
    try {
      const measured = await invokeCoordinator(
        coordinator,
        consume
          ? platformConsumeReservationIntrinsic
          : platformRevalidateReservationIntrinsic,
        exactFrozenRecord({
          configBytes: record.configBytes,
          descriptor: record.descriptor,
          inspectCodex: record.inspectCodex,
          reservation: record.reservation,
        }),
      );
      ensure(
        record.state === "using",
        "postgres_detached_restore_image_plan_reservation_rejected",
      );
      record.state = consume ? "consumed" : "issued";
      return measured;
    } catch (error) {
      record.state = "revoked";
      translatePlatformError(
        error,
        "postgres_detached_restore_image_plan_reservation_rejected",
      );
    }
  }

  const prepareImageReservation = objectFreeze(
    function prepareImageReservation(...methodArgs) {
      return protectPromise(prepareInternal(methodArgs));
    },
  );
  const revalidateImageReservation = objectFreeze(
    function revalidateImageReservation(...methodArgs) {
      return protectPromise(useReservation(methodArgs, false));
    },
  );
  const consumeImageReservation = objectFreeze(
    function consumeImageReservation(...methodArgs) {
      return protectPromise(useReservation(methodArgs, true));
    },
  );
  const binding = exactFrozenRecord({
    consumeImageReservation,
    contractVersion:
      POSTGRES_DETACHED_RESTORE_IMAGE_PLAN_BINDING_CONTRACT_VERSION,
    imagePlanProviderId: provider.imagePlanProviderId,
    prepareImageReservation,
    revalidateImageReservation,
  });
  weakSetAdd(bindingBrands, binding);
  return binding;
}

export function isPostgresDetachedRestoreImagePlanBinding(value) {
  try {
    return (
      arguments.length === 1 &&
      value !== null &&
      typeof value === "object" &&
      !isProxyValue(value) &&
      weakSetHas(bindingBrands, value)
    );
  } catch {
    return false;
  }
}

export function isPostgresDetachedRestoreImagePlanReservation(value) {
  try {
    return (
      arguments.length === 1 &&
      value !== null &&
      typeof value === "object" &&
      !isProxyValue(value) &&
      weakSetHas(reservationBrands, value)
    );
  } catch {
    return false;
  }
}

objectFreeze(PostgresDetachedRestoreImagePlanBindingError.prototype);
objectFreeze(PostgresDetachedRestoreImagePlanBindingError);
objectFreeze(createPostgresDetachedRestoreImagePlanBinding);
objectFreeze(isPostgresDetachedRestoreImagePlanBinding);
objectFreeze(isPostgresDetachedRestoreImagePlanReservation);
