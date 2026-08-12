import { AsyncLocalStorage, AsyncResource } from "node:async_hooks";
import { Buffer } from "node:buffer";
import { randomBytes } from "node:crypto";
import { EventEmitter } from "node:events";
import { isIP } from "node:net";
import { types as utilTypes } from "node:util";

import { Client, Pool } from "pg";

import {
  createPostgresDetachedRestoreRuntimeComposition,
} from "./postgres-detached-restore-runtime-composition.mjs";
import {
  createPostgresDetachedRestoreRuntimeController,
} from "./postgres-detached-restore-runtime-controller.mjs";

const ArrayConstructor = Array;
const arrayIncludesIntrinsic = Array.prototype.includes;
const arrayIsArray = Array.isArray;
const AsyncLocalStorageConstructor = AsyncLocalStorage;
const asyncLocalStorageGetStoreIntrinsic =
  AsyncLocalStorage.prototype.getStore;
const asyncLocalStorageRunIntrinsic = AsyncLocalStorage.prototype.run;
const AsyncResourceConstructor = AsyncResource;
const asyncResourceEmitDestroyIntrinsic = AsyncResource.prototype.emitDestroy;
const asyncResourceRunInAsyncScopeIntrinsic =
  AsyncResource.prototype.runInAsyncScope;
const bufferByteLengthIntrinsic = Buffer.byteLength;
const bufferReadInt32BEIntrinsic = Buffer.prototype.readInt32BE;
const ErrorConstructor = Error;
const eventEmitterOnIntrinsic = EventEmitter.prototype.on;
const isGeneratorFunctionValue = utilTypes.isGeneratorFunction;
const isGeneratorObjectValue = utilTypes.isGeneratorObject;
const isPromiseValue = utilTypes.isPromise;
const isProxyValue = utilTypes.isProxy;
const netIsIPIntrinsic = isIP;
const NumberConstructor = Number;
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
const objectSetPrototypeOf = Object.setPrototypeOf;
const PoolConstructor = Pool;
const clientQueryIntrinsic = Client.prototype.query;
const poolConnectIntrinsic = Pool.prototype.connect;
const poolEndIntrinsic = Pool.prototype.end;
const PromiseConstructor = Promise;
const promisePrototype = Promise.prototype;
const promiseResolveIntrinsic = Promise.resolve;
const promiseSpeciesSymbol = Symbol.species;
const promiseThenIntrinsic = Promise.prototype.then;
const randomBytesIntrinsic = randomBytes;
const reflectApply = Reflect.apply;
const reflectConstruct = Reflect.construct;
const reflectOwnKeys = Reflect.ownKeys;
const regexpExecIntrinsic = RegExp.prototype.exec;
const stringIncludesIntrinsic = String.prototype.includes;
const numberIsSafeIntegerIntrinsic = Number.isSafeInteger;
const bufferIsBufferIntrinsic = Buffer.isBuffer;
const TypeErrorConstructor = TypeError;
const WeakSetConstructor = WeakSet;
const weakSetAddIntrinsic = WeakSet.prototype.add;
const weakSetHasIntrinsic = WeakSet.prototype.has;

const createRuntimeCompositionIntrinsic =
  createPostgresDetachedRestoreRuntimeComposition;
const createRuntimeControllerIntrinsic =
  createPostgresDetachedRestoreRuntimeController;

const OPTION_ERROR_CODE =
  "invalid_postgres_detached_restore_deployment_options";
const REQUEST_ERROR_CODE =
  "invalid_postgres_detached_restore_deployment_request";
const OUTCOME_ERROR_CODE =
  "postgres_detached_restore_deployment_outcome_uncertain";
const ERROR_MESSAGES = objectFreeze({
  [OPTION_ERROR_CODE]:
    "PostgreSQL detached restore deployment options are invalid",
  [REQUEST_ERROR_CODE]:
    "PostgreSQL detached restore deployment request is invalid",
  [OUTCOME_ERROR_CODE]:
    "PostgreSQL detached restore deployment outcome is uncertain",
});
const MAX_DRIVER_RESULT_KEYS = 16;
const MAX_DRIVER_RESULT_SET_LENGTH = 256;
const MAX_DRIVER_ROW_KEYS = 256;

const TOP_LEVEL_OPTION_KEYS = objectFreeze(["postgres", "runtime"]);
const POSTGRES_OPTION_KEYS = objectFreeze([
  "applicationNamePrefix",
  "database",
  "host",
  "password",
  "poolMaximums",
  "port",
  "timeouts",
  "tls",
  "user",
]);
const POOL_MAXIMUM_KEYS = objectFreeze([
  "authority",
  "foregroundLifecycle",
  "operation",
  "recoveryLifecycle",
]);
const TIMEOUT_KEYS = objectFreeze([
  "connectionMilliseconds",
  "idleClientMilliseconds",
  "idleTransactionMilliseconds",
  "lockMilliseconds",
  "queryMilliseconds",
  "statementMilliseconds",
]);
const TLS_KEYS = objectFreeze([
  "ca",
  "cert",
  "key",
  "mode",
  "serverName",
]);
const RUNTIME_OPTION_KEYS = objectFreeze([
  "authority",
  "foreground",
  "launch",
  "planRegistry",
  "recovery",
  "storage",
]);
const AUTHORITY_OPTION_KEYS = objectFreeze([
  "maxTransactionAttempts",
  "restoreAttachmentActivationV2FleetCompatible",
  "restoreAttachmentActivationV2GenerationPredecessorFleetCompatible",
  "restoreGenerationV2FleetCompatible",
  "writerLaunchStopV3FleetCompatible",
]);
const FOREGROUND_OPTION_KEYS = objectFreeze(["fleetCapabilityGate"]);
const LAUNCH_OPTION_KEYS = objectFreeze([
  "imageReservations",
  "prepareImageReservation",
  "stoppedWriterCoordinator",
  "supervisor",
]);
const PLAN_REGISTRY_OPTION_KEYS = objectFreeze([
  "provisioningFleetCapabilityGate",
]);
const RECOVERY_OPTION_KEYS = objectFreeze([
  "intervalMilliseconds",
  "limits",
  "onStep",
  "recoveryScopeId",
]);
const STORAGE_OPTION_KEYS = objectFreeze([
  "backendId",
  "lifecycleBackend",
  "publication",
  "resolveArtifactPaths",
  "resolveRestoreDestination",
  "resolveSourceOwnedRoot",
]);
const CONTROLLER_KEYS = objectFreeze([
  "foreground",
  "stablePlanProvisioning",
  "start",
  "stop",
  "writerLaunch",
]);
const FOREGROUND_KEYS = objectFreeze([
  "restoreContextContractVersion",
  "runRestore",
]);
const STABLE_PLAN_PROVISIONING_KEYS = objectFreeze(["provisionStablePlan"]);
const WRITER_LAUNCH_KEYS = objectFreeze([
  "reconcileLaunchAttempt",
  "runLaunch",
]);
const STATUS_KEYS = objectFreeze(["status"]);
const TOPOLOGY_ROW_KEYS = objectFreeze([
  "backend_pid",
  "database_name",
  "database_user",
  "in_recovery",
  "server_version_num",
  "transaction_read_only",
]);
const BOOLEAN_ROW_KEYS = objectFreeze(["acquired"]);
const UNLOCK_ROW_KEYS = objectFreeze(["unlocked"]);
const ROLE_NAMES = objectFreeze([
  "authority",
  "operation",
  "foregroundLifecycle",
  "recoveryLifecycle",
]);
const ROLE_APPLICATION_SUFFIXES = objectFreeze([
  ":authority",
  ":operation",
  ":foreground-lifecycle",
  ":recovery-lifecycle",
]);
const MAX_PROTOTYPE_DEPTH = 64;
const MAX_POSTGRES_INTEGER = 2_147_483_647;
const MAX_POOL_CONNECTIONS = 64;
const MIN_POSTGRES_SERVER_VERSION = 130_000;
const MAX_APPLICATION_PREFIX_BYTES = 32;
const MAX_APPLICATION_NAME_BYTES = 63;
const MAX_CREDENTIAL_BYTES = 16 * 1024;
const MAX_TLS_MATERIAL_BYTES = 4 * 1024 * 1024;
const APPLICATION_PREFIX_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._-]{0,31}$/u;
const HOST_PATTERN =
  /^(?=.{1,253}$)(?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?)(?:\.(?:[A-Za-z0-9](?:[A-Za-z0-9-]{0,61}[A-Za-z0-9])?))*$/u;
const SERVER_VERSION_PATTERN = /^[0-9]{1,10}$/u;

const TOPOLOGY_SQL = [
  "SELECT",
  "pg_catalog.pg_backend_pid() AS backend_pid,",
  "pg_catalog.current_database() AS database_name,",
  "CURRENT_USER AS database_user,",
  "pg_catalog.pg_is_in_recovery() AS in_recovery,",
  "pg_catalog.current_setting('server_version_num') AS server_version_num,",
  "pg_catalog.current_setting('transaction_read_only') AS transaction_read_only",
].join(" ");
const TRY_LOCK_SQL =
  "SELECT pg_catalog.pg_try_advisory_lock($1::integer, $2::integer) AS acquired";
const UNLOCK_SQL =
  "SELECT pg_catalog.pg_advisory_unlock($1::integer, $2::integer) AS unlocked";

const deploymentBrands = new WeakSetConstructor();
const deploymentErrorBrands = new WeakSetConstructor();
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
function DriverPromise(executor) {
  return protectPromise(reflectConstruct(PromiseConstructor, [executor]));
}

const driverPromiseResolve = function resolve(value) {
  return protectPromise(
    callIntrinsic(promiseResolveIntrinsic, PromiseConstructor, [
      protectPromise(value),
    ]),
  );
};
const driverPromiseReject = function reject(reason) {
  return protectPromise(
    new PromiseConstructor((resolve, reject) => {
      void resolve;
      callIntrinsic(reject, undefined, [reason]);
    }),
  );
};
objectDefineProperties(DriverPromise, {
  reject: {
    configurable: false,
    enumerable: false,
    value: objectFreeze(driverPromiseReject),
    writable: false,
  },
  resolve: {
    configurable: false,
    enumerable: false,
    value: objectFreeze(driverPromiseResolve),
    writable: false,
  },
  try: {
    configurable: false,
    enumerable: false,
    value: undefined,
    writable: false,
  },
});
objectFreeze(DriverPromise);

function protectStoreCompatiblePromise(value) {
  ensure(
    isPromiseValue(value) &&
      !isProxyValue(value) &&
      objectGetPrototypeOf(value) === promisePrototype,
    OUTCOME_ERROR_CODE,
  );
  const constructorDescriptor = objectGetOwnPropertyDescriptor(
    value,
    "constructor",
  );
  if (constructorDescriptor === undefined) {
    objectDefineProperty(value, "constructor", {
      configurable: false,
      enumerable: false,
      value: PromiseConstructor,
      writable: false,
    });
  } else {
    ensure(
      constructorDescriptor.configurable === false &&
        constructorDescriptor.enumerable === false &&
        constructorDescriptor.writable === false &&
        constructorDescriptor.value === PromiseConstructor,
      OUTCOME_ERROR_CODE,
    );
  }
  return value;
}
const READY_RESULT = exactFrozenRecord({ status: "ready" });
const STOPPED_RESULT = exactFrozenRecord({ status: "stopped" });

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

function weakSetHas(set, value) {
  return callIntrinsic(weakSetHasIntrinsic, set, [value]);
}

function byteLength(value) {
  return callIntrinsic(bufferByteLengthIntrinsic, Buffer, [value, "utf8"]);
}

function stringIncludes(value, candidate) {
  return callIntrinsic(stringIncludesIntrinsic, value, [candidate]);
}

export class PostgresDetachedRestoreDeploymentError extends ErrorConstructor {
  constructor(code) {
    if (typeof code !== "string" || !objectHasOwn(ERROR_MESSAGES, code)) {
      throw new TypeErrorConstructor(
        "Unsupported PostgreSQL detached restore deployment error",
      );
    }
    const message = ERROR_MESSAGES[code];
    super(message);
    objectDefineProperty(this, "name", {
      configurable: false,
      enumerable: false,
      value: "PostgresDetachedRestoreDeploymentError",
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
      value: `PostgresDetachedRestoreDeploymentError: ${message}`,
      writable: false,
    });
    objectFreeze(this);
  }
}

function makeError(code) {
  const error = new PostgresDetachedRestoreDeploymentError(code);
  weakSetAdd(deploymentErrorBrands, error);
  return error;
}

function fail(code) {
  throw makeError(code);
}

function ensure(condition, code) {
  if (!condition) fail(code);
}

function isDeploymentError(value) {
  return (
    value !== null &&
    (typeof value === "object" || typeof value === "function") &&
    !isProxyValue(value) &&
    weakSetHas(deploymentErrorBrands, value)
  );
}

function exactFrozenRecord(value) {
  const result = objectCreate(null);
  const keys = reflectOwnKeys(value);
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
    const descriptor = objectGetOwnPropertyDescriptor(value, key);
    ensure(
      descriptor !== undefined && objectHasOwn(descriptor, "value"),
      OPTION_ERROR_CODE,
    );
    objectDefineProperty(result, key, {
      configurable: false,
      enumerable: true,
      value: descriptor.value,
      writable: false,
    });
  }
  return objectFreeze(result);
}

function exactDataObject(value, expectedKeys, code = OPTION_ERROR_CODE) {
  ensure(
    value !== null &&
      typeof value === "object" &&
      !arrayIsArray(value) &&
      !isProxyValue(value),
    code,
  );
  let keys;
  let prototype;
  try {
    keys = reflectOwnKeys(value);
    prototype = objectGetPrototypeOf(value);
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

function trustedFunction(value, code = OPTION_ERROR_CODE) {
  ensure(
    typeof value === "function" &&
      !isProxyValue(value) &&
      !isGeneratorFunctionValue(value),
    code,
  );
  return value;
}

function prototypeDataValue(receiver, name, code = OUTCOME_ERROR_CODE) {
  ensure(
    receiver !== null &&
      (typeof receiver === "object" || typeof receiver === "function") &&
      !isProxyValue(receiver),
    code,
  );
  let current = receiver;
  for (let depth = 0; depth <= MAX_PROTOTYPE_DEPTH; depth += 1) {
    ensure(current !== null && !isProxyValue(current), code);
    let descriptor;
    try {
      descriptor = objectGetOwnPropertyDescriptor(current, name);
    } catch {
      fail(code);
    }
    if (descriptor !== undefined) {
      ensure(objectHasOwn(descriptor, "value"), code);
      return trustedFunction(descriptor.value, code);
    }
    try {
      current = objectGetPrototypeOf(current);
    } catch {
      fail(code);
    }
  }
  fail(code);
}

function ownDataValue(receiver, name, code = OUTCOME_ERROR_CODE) {
  ensure(
    receiver !== null && typeof receiver === "object" && !isProxyValue(receiver),
    code,
  );
  let descriptor;
  try {
    descriptor = objectGetOwnPropertyDescriptor(receiver, name);
  } catch {
    fail(code);
  }
  ensure(descriptor !== undefined && objectHasOwn(descriptor, "value"), code);
  return descriptor.value;
}

function safeDriverQueryResult(value) {
  // Driver values never cross a Promise fulfillment boundary directly. A
  // frozen null-prototype copy prevents inherited `then` from assimilating a
  // Result, a result set, or a row before the authority layer validates it.
  ensure(
    value !== null && typeof value === "object" && !isProxyValue(value),
    OUTCOME_ERROR_CODE,
  );
  if (arrayIsArray(value)) {
    ensure(
      callIntrinsic(numberIsSafeIntegerIntrinsic, NumberConstructor, [
        value.length,
      ]) &&
        value.length >= 0 &&
        value.length <= MAX_DRIVER_RESULT_SET_LENGTH,
      OUTCOME_ERROR_CODE,
    );
    const resultSet = new ArrayConstructor();
    objectSetPrototypeOf(resultSet, null);
    for (let index = 0; index < value.length; index += 1) {
      let descriptor;
      try {
        descriptor = objectGetOwnPropertyDescriptor(value, `${index}`);
      } catch {
        fail(OUTCOME_ERROR_CODE);
      }
      ensure(
        descriptor?.enumerable === true &&
          objectHasOwn(descriptor, "value"),
        OUTCOME_ERROR_CODE,
      );
      objectDefineProperty(resultSet, `${index}`, {
        configurable: false,
        enumerable: true,
        value: safeDriverQueryResult(descriptor.value),
        writable: false,
      });
    }
    return objectFreeze(resultSet);
  }

  let keys;
  try {
    keys = reflectOwnKeys(value);
  } catch {
    fail(OUTCOME_ERROR_CODE);
  }
  ensure(
    keys.length > 0 && keys.length <= MAX_DRIVER_RESULT_KEYS,
    OUTCOME_ERROR_CODE,
  );
  const carrier = objectCreate(null);
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
    ensure(typeof key === "string" && key !== "then", OUTCOME_ERROR_CODE);
    let descriptor;
    try {
      descriptor = objectGetOwnPropertyDescriptor(value, key);
    } catch {
      fail(OUTCOME_ERROR_CODE);
    }
    ensure(
      descriptor?.enumerable === true && objectHasOwn(descriptor, "value"),
      OUTCOME_ERROR_CODE,
    );
    objectDefineProperty(carrier, key, {
      configurable: false,
      enumerable: true,
      value:
        key === "rows"
          ? safeDriverRows(descriptor.value)
          : descriptor.value,
      writable: false,
    });
  }
  return objectFreeze(carrier);
}

function safeDriverRows(value) {
  ensure(
    arrayIsArray(value) &&
      !isProxyValue(value) &&
      callIntrinsic(numberIsSafeIntegerIntrinsic, NumberConstructor, [
        value.length,
      ]) &&
      value.length >= 0 &&
      value.length <= MAX_DRIVER_RESULT_SET_LENGTH,
    OUTCOME_ERROR_CODE,
  );
  const rows = new ArrayConstructor();
  objectSetPrototypeOf(rows, null);
  for (let index = 0; index < value.length; index += 1) {
    let descriptor;
    try {
      descriptor = objectGetOwnPropertyDescriptor(value, `${index}`);
    } catch {
      fail(OUTCOME_ERROR_CODE);
    }
    ensure(
      descriptor?.enumerable === true && objectHasOwn(descriptor, "value"),
      OUTCOME_ERROR_CODE,
    );
    objectDefineProperty(rows, `${index}`, {
      configurable: false,
      enumerable: true,
      value: safeDriverRow(descriptor.value),
      writable: false,
    });
  }
  return objectFreeze(rows);
}

function safeDriverRow(value) {
  ensure(
    value !== null && typeof value === "object" && !isProxyValue(value),
    OUTCOME_ERROR_CODE,
  );
  let keys;
  try {
    keys = reflectOwnKeys(value);
  } catch {
    fail(OUTCOME_ERROR_CODE);
  }
  ensure(keys.length <= MAX_DRIVER_ROW_KEYS, OUTCOME_ERROR_CODE);
  if (arrayIsArray(value)) {
    ensure(keys.length === value.length + 1, OUTCOME_ERROR_CODE);
    const row = new ArrayConstructor();
    objectSetPrototypeOf(row, null);
    for (let index = 0; index < value.length; index += 1) {
      const descriptor = objectGetOwnPropertyDescriptor(value, `${index}`);
      ensure(
        descriptor?.enumerable === true && objectHasOwn(descriptor, "value"),
        OUTCOME_ERROR_CODE,
      );
      objectDefineProperty(row, `${index}`, {
        configurable: false,
        enumerable: true,
        value: descriptor.value,
        writable: false,
      });
    }
    return objectFreeze(row);
  }
  const row = objectCreate(null);
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
    ensure(typeof key === "string" && key !== "then", OUTCOME_ERROR_CODE);
    const descriptor = objectGetOwnPropertyDescriptor(value, key);
    ensure(
      descriptor?.enumerable === true && objectHasOwn(descriptor, "value"),
      OUTCOME_ERROR_CODE,
    );
    objectDefineProperty(row, key, {
      configurable: false,
      enumerable: true,
      value: descriptor.value,
      writable: false,
    });
  }
  return objectFreeze(row);
}

function binding(receiver, name, code = OUTCOME_ERROR_CODE) {
  return objectFreeze({
    method: prototypeDataValue(receiver, name, code),
    receiver,
  });
}

function invoke(bindingValue, args, code = OUTCOME_ERROR_CODE) {
  try {
    return callIntrinsic(bindingValue.method, bindingValue.receiver, args);
  } catch {
    fail(code);
  }
}

function safePromiseReactionDescriptor(descriptor) {
  return (
    descriptor !== undefined &&
    descriptor.configurable === false &&
    descriptor.enumerable === false &&
    descriptor.writable === false &&
    typeof descriptor.value === "function" &&
    !isProxyValue(descriptor.value) &&
    !isGeneratorFunctionValue(descriptor.value)
  );
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

objectFreeze(protectedPromiseThen);
objectFreeze(protectedPromiseCatch);
objectFreeze(protectedPromiseFinally);

function frozenDataDescriptor(descriptor, value) {
  return (
    descriptor !== undefined &&
    descriptor.configurable === false &&
    descriptor.enumerable === false &&
    descriptor.writable === false &&
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
  let keys;
  let prototype;
  let descriptor;
  try {
    keys = reflectOwnKeys(value);
    prototype = objectGetPrototypeOf(value);
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
    OUTCOME_ERROR_CODE,
  );
  let thenDescriptor;
  let catchDescriptor;
  let finallyDescriptor;
  let constructorDescriptor;
  try {
    thenDescriptor = objectGetOwnPropertyDescriptor(value, "then");
    catchDescriptor = objectGetOwnPropertyDescriptor(value, "catch");
    finallyDescriptor = objectGetOwnPropertyDescriptor(value, "finally");
    constructorDescriptor = objectGetOwnPropertyDescriptor(value, "constructor");
  } catch {
    fail(OUTCOME_ERROR_CODE);
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
  const constructorIsPromise =
    constructorDescriptor === undefined ||
    frozenDataDescriptor(constructorDescriptor, PromiseConstructor);
  const constructorIsSafeSpecies =
    constructorDescriptor !== undefined &&
    safePromiseSpeciesHolder(constructorDescriptor.value);
  const hasNoOwnReactions =
    thenDescriptor === undefined &&
    catchDescriptor === undefined &&
    finallyDescriptor === undefined;
  if (!hasNoOwnReactions && !reactionsAreOurs) {
    ensure(constructorIsPromise || constructorIsSafeSpecies, OUTCOME_ERROR_CODE);
    ensure(safePromiseReactionDescriptor(thenDescriptor), OUTCOME_ERROR_CODE);
    ensure(safePromiseReactionDescriptor(catchDescriptor), OUTCOME_ERROR_CODE);
    ensure(safePromiseReactionDescriptor(finallyDescriptor), OUTCOME_ERROR_CODE);
    let child;
    try {
      child = callIntrinsic(promiseThenIntrinsic, value, [undefined, undefined]);
    } catch {
      fail(OUTCOME_ERROR_CODE);
    }
    return protectPromise(child);
  }
  ensure(
    (constructorIsPromise || constructorIsSafeSpecies) &&
      (hasNoOwnReactions || reactionsAreOurs),
    OUTCOME_ERROR_CODE,
  );
  if (reactionsAreOurs) return value;
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
  if (!frozenDataDescriptor(constructorDescriptor, PromiseConstructor)) {
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
    fail(OUTCOME_ERROR_CODE);
  }
  return value;
}

function invokePromise(bindingValue, args, code = OUTCOME_ERROR_CODE) {
  const pending = invoke(bindingValue, args, code);
  ensure(isPromiseValue(pending), code);
  return protectPromise(pending);
}

function settlement(value) {
  return exactFrozenRecord(value);
}

async function settlePromiseInternal(pending) {
  try {
    return settlement({ error: null, ok: true, value: await pending });
  } catch (error) {
    return settlement({ error, ok: false, value: null });
  }
}

function settlePromise(pending) {
  return protectPromise(settlePromiseInternal(protectPromise(pending)));
}

async function settleInvocationInternal(bindingValue, args) {
  try {
    return await settlePromise(invokePromise(bindingValue, args));
  } catch (error) {
    return settlement({ error, ok: false, value: null });
  }
}

function settleInvocation(bindingValue, args) {
  return protectPromise(settleInvocationInternal(bindingValue, args));
}

function normalizeString(value, minimumBytes, maximumBytes) {
  ensure(typeof value === "string", OPTION_ERROR_CODE);
  const length = byteLength(value);
  ensure(length >= minimumBytes && length <= maximumBytes, OPTION_ERROR_CODE);
  return value;
}

function normalizeInteger(value, minimum, maximum) {
  ensure(
    callIntrinsic(numberIsSafeIntegerIntrinsic, NumberConstructor, [value]) &&
      value >= minimum &&
      value <= maximum,
    OPTION_ERROR_CODE,
  );
  return value;
}

function normalizeRuntimeOptions(value) {
  const runtime = exactDataObject(value, RUNTIME_OPTION_KEYS);
  const authority = exactDataObject(runtime.authority, AUTHORITY_OPTION_KEYS);
  const foreground = exactDataObject(runtime.foreground, FOREGROUND_OPTION_KEYS);
  const launch = exactDataObject(runtime.launch, LAUNCH_OPTION_KEYS);
  const planRegistry = exactDataObject(
    runtime.planRegistry,
    PLAN_REGISTRY_OPTION_KEYS,
  );
  const recovery = exactDataObject(runtime.recovery, RECOVERY_OPTION_KEYS);
  const storage = exactDataObject(runtime.storage, STORAGE_OPTION_KEYS);
  trustedFunction(foreground.fleetCapabilityGate);
  trustedFunction(launch.prepareImageReservation);
  trustedFunction(planRegistry.provisioningFleetCapabilityGate);
  trustedFunction(recovery.onStep);
  trustedFunction(storage.resolveArtifactPaths);
  trustedFunction(storage.resolveRestoreDestination);
  trustedFunction(storage.resolveSourceOwnedRoot);
  return exactFrozenRecord({
    authority,
    foreground,
    launch,
    planRegistry,
    recovery,
    storage,
  });
}

function normalizePostgresOptions(value) {
  const postgres = exactDataObject(value, POSTGRES_OPTION_KEYS);
  const poolMaximums = exactDataObject(
    postgres.poolMaximums,
    POOL_MAXIMUM_KEYS,
  );
  const timeouts = exactDataObject(postgres.timeouts, TIMEOUT_KEYS);
  const tls = exactDataObject(postgres.tls, TLS_KEYS);
  const applicationNamePrefix = normalizeString(
    postgres.applicationNamePrefix,
    1,
    MAX_APPLICATION_PREFIX_BYTES,
  );
  ensure(
    regexpTest(APPLICATION_PREFIX_PATTERN, applicationNamePrefix),
    OPTION_ERROR_CODE,
  );
  const database = normalizeString(postgres.database, 1, 128);
  const user = normalizeString(postgres.user, 1, 128);
  const password = normalizeString(postgres.password, 0, MAX_CREDENTIAL_BYTES);
  ensure(
    !stringIncludes(database, "\u0000") &&
      !stringIncludes(user, "\u0000"),
    OPTION_ERROR_CODE,
  );
  ensure(!stringIncludes(password, "\u0000"), OPTION_ERROR_CODE);
  const host = normalizeString(postgres.host, 1, 253);
  ensure(
    callIntrinsic(netIsIPIntrinsic, undefined, [host]) !== 0 ||
      regexpTest(HOST_PATTERN, host),
    OPTION_ERROR_CODE,
  );
  const port = normalizeInteger(postgres.port, 1, 65_535);
  const normalizedPoolMaximums = exactFrozenRecord({
    authority: normalizeInteger(
      poolMaximums.authority,
      1,
      MAX_POOL_CONNECTIONS,
    ),
    foregroundLifecycle: normalizeInteger(
      poolMaximums.foregroundLifecycle,
      1,
      MAX_POOL_CONNECTIONS,
    ),
    operation: normalizeInteger(
      poolMaximums.operation,
      1,
      MAX_POOL_CONNECTIONS,
    ),
    recoveryLifecycle: normalizeInteger(
      poolMaximums.recoveryLifecycle,
      1,
      MAX_POOL_CONNECTIONS,
    ),
  });
  const normalizedTimeouts = exactFrozenRecord({
    connectionMilliseconds: normalizeInteger(
      timeouts.connectionMilliseconds,
      1,
      MAX_POSTGRES_INTEGER,
    ),
    idleClientMilliseconds: normalizeInteger(
      timeouts.idleClientMilliseconds,
      1,
      MAX_POSTGRES_INTEGER,
    ),
    idleTransactionMilliseconds: normalizeInteger(
      timeouts.idleTransactionMilliseconds,
      1,
      MAX_POSTGRES_INTEGER,
    ),
    lockMilliseconds: normalizeInteger(
      timeouts.lockMilliseconds,
      1,
      MAX_POSTGRES_INTEGER,
    ),
    queryMilliseconds: normalizeInteger(
      timeouts.queryMilliseconds,
      1,
      MAX_POSTGRES_INTEGER,
    ),
    statementMilliseconds: normalizeInteger(
      timeouts.statementMilliseconds,
      1,
      MAX_POSTGRES_INTEGER,
    ),
  });
  ensure(tls.mode === "disable" || tls.mode === "verify-full", OPTION_ERROR_CODE);
  let normalizedTls;
  if (tls.mode === "disable") {
    ensure(
      tls.ca === null &&
        tls.cert === null &&
        tls.key === null &&
        tls.serverName === null,
      OPTION_ERROR_CODE,
    );
    normalizedTls = exactFrozenRecord({
      ca: null,
      cert: null,
      key: null,
      mode: "disable",
      serverName: null,
    });
  } else {
    const ca = normalizeString(tls.ca, 1, MAX_TLS_MATERIAL_BYTES);
    const serverName = normalizeString(tls.serverName, 1, 253);
    ensure(serverName === host, OPTION_ERROR_CODE);
    const certIsNull = tls.cert === null;
    const keyIsNull = tls.key === null;
    ensure(certIsNull === keyIsNull, OPTION_ERROR_CODE);
    const cert = certIsNull
      ? null
      : normalizeString(tls.cert, 1, MAX_TLS_MATERIAL_BYTES);
    const key = keyIsNull
      ? null
      : normalizeString(tls.key, 1, MAX_TLS_MATERIAL_BYTES);
    normalizedTls = exactFrozenRecord({
      ca,
      cert,
      key,
      mode: "verify-full",
      serverName,
    });
  }
  for (let index = 0; index < ROLE_APPLICATION_SUFFIXES.length; index += 1) {
    ensure(
      byteLength(applicationNamePrefix + ROLE_APPLICATION_SUFFIXES[index]) <=
        MAX_APPLICATION_NAME_BYTES,
      OPTION_ERROR_CODE,
    );
  }
  return exactFrozenRecord({
    applicationNamePrefix,
    database,
    host,
    password,
    poolMaximums: normalizedPoolMaximums,
    port,
    timeouts: normalizedTimeouts,
    tls: normalizedTls,
    user,
  });
}

function createPasswordProvider(password) {
  const provider = function deploymentPassword() {
    return password;
  };
  return objectFreeze(provider);
}

function createSslOptions(tls) {
  if (tls.mode === "disable") return false;
  const ssl = {
    ca: tls.ca,
    rejectUnauthorized: true,
    servername: tls.serverName,
  };
  if (tls.cert !== null) {
    ssl.cert = tls.cert;
    ssl.key = tls.key;
  }
  return ssl;
}

function poolConfig(
  postgres,
  roleIndex,
  passwordProvider,
) {
  const role = ROLE_NAMES[roleIndex];
  return {
    allowExitOnIdle: false,
    application_name:
      postgres.applicationNamePrefix + ROLE_APPLICATION_SUFFIXES[roleIndex],
    binary: false,
    client_encoding: "UTF8",
    connectionTimeoutMillis: postgres.timeouts.connectionMilliseconds,
    database: postgres.database,
    enableChannelBinding: true,
    host: postgres.host,
    idle_in_transaction_session_timeout:
      postgres.timeouts.idleTransactionMilliseconds,
    idleTimeoutMillis: postgres.timeouts.idleClientMilliseconds,
    keepAlive: true,
    keepAliveInitialDelayMillis: 0,
    lock_timeout: postgres.timeouts.lockMilliseconds,
    max: postgres.poolMaximums[role],
    maxLifetimeSeconds: 0,
    min: 0,
    options: "-c search_path=pg_catalog",
    password: passwordProvider,
    port: postgres.port,
    Promise: DriverPromise,
    query_timeout: postgres.timeouts.queryMilliseconds,
    replication: "false",
    ssl: createSslOptions(postgres.tls),
    sslnegotiation: "postgres",
    statement_timeout: postgres.timeouts.statementMilliseconds,
    user: postgres.user,
  };
}

function poolRecord(pool, role) {
  return objectFreeze({
    pool,
    role,
  });
}

function bestEffortDestroyCheckout(release) {
  if (typeof release !== "function" || isProxyValue(release)) return;
  try {
    const returned = callIntrinsic(release, undefined, [
      makeError(OUTCOME_ERROR_CODE),
    ]);
    if (isPromiseValue(returned)) void settlePromise(returned);
  } catch {
    // The malformed checkout remains primary while the owned slot is
    // destroyed on a best-effort basis.
  }
}

function createRuntimeClientFacade(client, release) {
  const releaseMethod = trustedFunction(release, OUTCOME_ERROR_CODE);
  let connection;
  try {
    connection = ownDataValue(client, "connection");
  } catch (error) {
    bestEffortDestroyCheckout(releaseMethod);
    throw error;
  }

  const releaseClient = function releaseClient(...releaseArgs) {
    ensure(releaseArgs.length <= 1, OUTCOME_ERROR_CODE);
    let returned;
    try {
      returned = callIntrinsic(releaseMethod, undefined, releaseArgs);
    } catch {
      fail(OUTCOME_ERROR_CODE);
    }
    ensure(returned === undefined, OUTCOME_ERROR_CODE);
  };

  const queryClient = function queryClient(...queryArgs) {
    ensure(queryArgs.length === 1 || queryArgs.length === 2, OUTCOME_ERROR_CODE);
    const config = queryArgs.length === 1 ? queryArgs[0] : null;
    let callbackDescriptor;
    if (
      config !== null &&
      typeof config === "object" &&
      !isProxyValue(config)
    ) {
      try {
        callbackDescriptor = objectGetOwnPropertyDescriptor(config, "callback");
      } catch {
        fail(OUTCOME_ERROR_CODE);
      }
    }
    if (callbackDescriptor !== undefined) {
      ensure(
        objectHasOwn(callbackDescriptor, "value") &&
          typeof callbackDescriptor.value === "function" &&
          !isProxyValue(callbackDescriptor.value) &&
          !isGeneratorFunctionValue(callbackDescriptor.value),
        OUTCOME_ERROR_CODE,
      );
      let returned;
      try {
        returned = callIntrinsic(
          clientQueryIntrinsic,
          client,
          queryArgs,
        );
      } catch {
        fail(OUTCOME_ERROR_CODE);
      }
      ensure(returned === undefined, OUTCOME_ERROR_CODE);
      return undefined;
    }

    let completed = false;
    const pending = new PromiseConstructor((resolve, reject) => {
      const onQuery = function onQuery(error, result) {
        if (completed) return;
        completed = true;
        if (error !== null && error !== undefined) {
          callIntrinsic(reject, undefined, [error]);
          return;
        }
        let safeResult;
        try {
          safeResult = safeDriverQueryResult(result);
        } catch (resultError) {
          callIntrinsic(reject, undefined, [resultError]);
          return;
        }
        callIntrinsic(resolve, undefined, [safeResult]);
      };
      objectFreeze(onQuery);
      const forwarded = [];
      for (let index = 0; index < queryArgs.length; index += 1) {
        forwarded[index] = queryArgs[index];
      }
      forwarded[queryArgs.length] = onQuery;
      try {
        const returned = callIntrinsic(
          clientQueryIntrinsic,
          client,
          forwarded,
        );
        if (returned !== undefined) {
          callIntrinsic(reject, undefined, [makeError(OUTCOME_ERROR_CODE)]);
        }
      } catch (error) {
        callIntrinsic(reject, undefined, [error]);
      }
    });
    return protectStoreCompatiblePromise(pending);
  };

  objectFreeze(queryClient);
  objectFreeze(releaseClient);
  const facade = exactFrozenRecord({
    connection,
    query: queryClient,
    release: releaseClient,
  });
  return facade;
}

function createRuntimePoolFacade(record) {
  const connect = function connect(...connectArgs) {
    ensure(connectArgs.length <= 1, OUTCOME_ERROR_CODE);
    const callback =
      connectArgs.length === 0
        ? null
        : trustedFunction(connectArgs[0], OUTCOME_ERROR_CODE);
    let completed = false;
    if (callback !== null) {
      const onCheckout = function onCheckout(error, client, release) {
        if (completed) {
          bestEffortDestroyCheckout(release);
          return;
        }
        completed = true;
        if (error !== null && error !== undefined) {
          callIntrinsic(callback, undefined, [error, undefined, undefined]);
          return;
        }
        let facade;
        try {
          facade = createRuntimeClientFacade(client, release);
        } catch (facadeError) {
          callIntrinsic(callback, undefined, [facadeError, undefined, undefined]);
          return;
        }
        callIntrinsic(callback, undefined, [
          undefined,
          facade,
          facade.release,
        ]);
      };
      objectFreeze(onCheckout);
      let returned;
      try {
        returned = callIntrinsic(poolConnectIntrinsic, record.pool, [
          onCheckout,
        ]);
      } catch {
        fail(OUTCOME_ERROR_CODE);
      }
      ensure(returned === undefined, OUTCOME_ERROR_CODE);
      return undefined;
    }

    const pending = new PromiseConstructor((resolve, reject) => {
      const onCheckout = function onCheckout(error, client, release) {
        if (completed) {
          bestEffortDestroyCheckout(release);
          return;
        }
        completed = true;
        if (error !== null && error !== undefined) {
          callIntrinsic(reject, undefined, [error]);
          return;
        }
        try {
          callIntrinsic(resolve, undefined, [
            createRuntimeClientFacade(client, release),
          ]);
        } catch (facadeError) {
          callIntrinsic(reject, undefined, [facadeError]);
        }
      };
      objectFreeze(onCheckout);
      try {
        const returned = callIntrinsic(poolConnectIntrinsic, record.pool, [
          onCheckout,
        ]);
        if (returned !== undefined) {
          callIntrinsic(reject, undefined, [makeError(OUTCOME_ERROR_CODE)]);
        }
      } catch (error) {
        callIntrinsic(reject, undefined, [error]);
      }
    });
    return protectStoreCompatiblePromise(pending);
  };
  objectFreeze(connect);
  return exactFrozenRecord({ connect });
}

function validateOwnedPool(records, index) {
  const pool = records[index].pool;
  ensure(
    pool !== null && typeof pool === "object" && !isProxyValue(pool),
    OUTCOME_ERROR_CODE,
  );
  for (let prior = 0; prior < index; prior += 1) {
    ensure(!objectIs(records[prior].pool, pool), OUTCOME_ERROR_CODE);
  }
}

function startConstructionCleanup(records) {
  for (let index = records.length - 1; index >= 0; index -= 1) {
    try {
      void settlePromise(endPool(records[index]));
    } catch {
      // Construction already failed. Every already-owned pool is still
      // attempted, and all observable asynchronous failures are absorbed.
    }
  }
}

function createProbeKey() {
  let bytes;
  try {
    bytes = callIntrinsic(randomBytesIntrinsic, undefined, [8]);
    ensure(
      callIntrinsic(bufferIsBufferIntrinsic, Buffer, [bytes]) &&
        bytes.length === 8,
      OUTCOME_ERROR_CODE,
    );
    return objectFreeze([
      callIntrinsic(bufferReadInt32BEIntrinsic, bytes, [0]),
      callIntrinsic(bufferReadInt32BEIntrinsic, bytes, [4]),
    ]);
  } catch (error) {
    if (isDeploymentError(error)) throw error;
    fail(OUTCOME_ERROR_CODE);
  }
}

function resultRows(result) {
  const rows = ownDataValue(result, "rows");
  ensure(arrayIsArray(rows) && !isProxyValue(rows), OUTCOME_ERROR_CODE);
  let keys;
  try {
    keys = reflectOwnKeys(rows);
  } catch {
    fail(OUTCOME_ERROR_CODE);
  }
  ensure(
    rows.length === 1 &&
      keys.length === 2 &&
      arrayIncludes(keys, "0") &&
      arrayIncludes(keys, "length"),
    OUTCOME_ERROR_CODE,
  );
  return rows;
}

function oneExactRow(result, keys) {
  const rows = resultRows(result);
  ensure(rows.length === 1, OUTCOME_ERROR_CODE);
  return exactDataObject(rows[0], keys, OUTCOME_ERROR_CODE);
}

function topologyRow(result, expectedDatabase, expectedUser) {
  const row = oneExactRow(result, TOPOLOGY_ROW_KEYS);
  ensure(
    callIntrinsic(numberIsSafeIntegerIntrinsic, NumberConstructor, [
      row.backend_pid,
    ]) &&
      row.backend_pid > 0,
    OUTCOME_ERROR_CODE,
  );
  ensure(row.database_name === expectedDatabase, OUTCOME_ERROR_CODE);
  ensure(row.database_user === expectedUser, OUTCOME_ERROR_CODE);
  ensure(row.in_recovery === false, OUTCOME_ERROR_CODE);
  ensure(
    typeof row.server_version_num === "string" &&
      regexpTest(SERVER_VERSION_PATTERN, row.server_version_num) &&
      callIntrinsic(NumberConstructor, undefined, [row.server_version_num]) >=
        MIN_POSTGRES_SERVER_VERSION,
    OUTCOME_ERROR_CODE,
  );
  ensure(row.transaction_read_only === "off", OUTCOME_ERROR_CODE);
  return row;
}

function completeClientRecord(ownedClient) {
  ensure(
    ownedClient.client !== null &&
      typeof ownedClient.client === "object" &&
      !isProxyValue(ownedClient.client),
    OUTCOME_ERROR_CODE,
  );
  return objectFreeze({
    client: ownedClient.client,
    query: objectFreeze({
      method: clientQueryIntrinsic,
      receiver: ownedClient.client,
    }),
    release: ownedClient.release,
  });
}

function checkoutPool(record) {
  let completed = false;
  const pending = new PromiseConstructor((resolve, reject) => {
    const onCheckout = function onCheckout(error, client, release) {
      if (completed) return;
      completed = true;
      if (error !== null && error !== undefined) {
        callIntrinsic(reject, undefined, [makeError(OUTCOME_ERROR_CODE)]);
        return;
      }
      try {
        const releaseMethod = trustedFunction(release, OUTCOME_ERROR_CODE);
        callIntrinsic(resolve, undefined, [
          objectFreeze({
            client,
            release: objectFreeze({
              method: releaseMethod,
              receiver: undefined,
            }),
          }),
        ]);
      } catch (checkoutError) {
        callIntrinsic(reject, undefined, [checkoutError]);
      }
    };
    objectFreeze(onCheckout);
    try {
      callIntrinsic(poolConnectIntrinsic, record.pool, [onCheckout]);
    } catch {
      callIntrinsic(reject, undefined, [makeError(OUTCOME_ERROR_CODE)]);
    }
  });
  return protectPromise(pending);
}

function endPool(record) {
  let completed = false;
  const pending = new PromiseConstructor((resolve, reject) => {
    const onEnded = function onEnded(error) {
      if (completed) return;
      completed = true;
      if (error !== null && error !== undefined) {
        callIntrinsic(reject, undefined, [makeError(OUTCOME_ERROR_CODE)]);
        return;
      }
      callIntrinsic(resolve, undefined, [undefined]);
    };
    objectFreeze(onEnded);
    try {
      callIntrinsic(poolEndIntrinsic, record.pool, [onEnded]);
    } catch {
      callIntrinsic(reject, undefined, [makeError(OUTCOME_ERROR_CODE)]);
    }
  });
  return protectPromise(pending);
}

function invokeClientQuery(record, text, values = undefined) {
  let completed = false;
  const pending = new PromiseConstructor((resolve, reject) => {
    const callback = function callback(error, result) {
      if (completed) return;
      completed = true;
      if (error !== null && error !== undefined) {
        callIntrinsic(reject, undefined, [error]);
        return;
      }
      let safeResult;
      try {
        safeResult = safeDriverQueryResult(result);
      } catch (resultError) {
        callIntrinsic(reject, undefined, [resultError]);
        return;
      }
      callIntrinsic(resolve, undefined, [safeResult]);
    };
    objectFreeze(callback);
    const args =
      values === undefined ? [text, callback] : [text, values, callback];
    try {
      const returned = invoke(record.query, args);
      if (returned !== undefined) {
        callIntrinsic(reject, undefined, [makeError(OUTCOME_ERROR_CODE)]);
      }
    } catch (error) {
      callIntrinsic(reject, undefined, [error]);
    }
  });
  return protectPromise(pending);
}

function releaseClient(record) {
  if (record === null) return false;
  try {
    const returned = callIntrinsic(
      record.release.method,
      record.release.receiver,
      [],
    );
    if (returned === undefined) return true;
    if (
      isPromiseValue(returned) &&
      !isProxyValue(returned) &&
      objectGetPrototypeOf(returned) === promisePrototype
    ) {
      void settlePromise(returned);
    }
    return false;
  } catch {
    return false;
  }
}

function validateStatus(value, expected) {
  const result = exactDataObject(value, STATUS_KEYS, OUTCOME_ERROR_CODE);
  ensure(result.status === expected, OUTCOME_ERROR_CODE);
}

export function createPostgresDetachedRestoreDeployment(...args) {
  ensure(args.length === 1, OPTION_ERROR_CODE);
  const options = exactDataObject(args[0], TOP_LEVEL_OPTION_KEYS);
  const postgres = normalizePostgresOptions(options.postgres);
  const runtimeOptions = normalizeRuntimeOptions(options.runtime);
  const probeKey = createProbeKey();
  const passwordProvider = createPasswordProvider(postgres.password);
  const poolRecords = [];
  let requestFatalShutdown = null;
  const handleConnectedClientError = function handleConnectedClientError() {
    if (requestFatalShutdown !== null) requestFatalShutdown();
  };
  const onConnectedClient = function onConnectedClient(client) {
    callIntrinsic(eventEmitterOnIntrinsic, client, [
      "error",
      handleConnectedClientError,
    ]);
  };
  objectFreeze(handleConnectedClientError);
  objectFreeze(onConnectedClient);
  let runtime;
  let controller;
  let controllerBindings;
  try {
    for (let index = 0; index < ROLE_NAMES.length; index += 1) {
      const pool = reflectConstruct(PoolConstructor, [
        poolConfig(postgres, index, passwordProvider),
      ]);
      poolRecords[poolRecords.length] = poolRecord(
        pool,
        ROLE_NAMES[index],
      );
      validateOwnedPool(poolRecords, poolRecords.length - 1);
      callIntrinsic(eventEmitterOnIntrinsic, pool, [
        "connect",
        onConnectedClient,
      ]);
    }
    const runtimePools = [
      createRuntimePoolFacade(poolRecords[0]),
      createRuntimePoolFacade(poolRecords[1]),
      createRuntimePoolFacade(poolRecords[2]),
      createRuntimePoolFacade(poolRecords[3]),
    ];
    runtime = callIntrinsic(createRuntimeCompositionIntrinsic, undefined, [
      exactFrozenRecord({
        authority: runtimeOptions.authority,
        foreground: runtimeOptions.foreground,
        launch: runtimeOptions.launch,
        planRegistry: runtimeOptions.planRegistry,
        pools: exactFrozenRecord({
          authority: runtimePools[0],
          foregroundLifecycle: runtimePools[2],
          operation: runtimePools[1],
          recoveryLifecycle: runtimePools[3],
        }),
        recovery: runtimeOptions.recovery,
        storage: runtimeOptions.storage,
      }),
    ]);
    controller = callIntrinsic(createRuntimeControllerIntrinsic, undefined, [
      exactFrozenRecord({ runtime }),
    ]);
    const normalizedController = exactDataObject(
      controller,
      CONTROLLER_KEYS,
      OUTCOME_ERROR_CODE,
    );
    const controllerForeground = exactDataObject(
      normalizedController.foreground,
      FOREGROUND_KEYS,
      OUTCOME_ERROR_CODE,
    );
    const controllerProvisioning = exactDataObject(
      normalizedController.stablePlanProvisioning,
      STABLE_PLAN_PROVISIONING_KEYS,
      OUTCOME_ERROR_CODE,
    );
    const controllerWriterLaunch = exactDataObject(
      normalizedController.writerLaunch,
      WRITER_LAUNCH_KEYS,
      OUTCOME_ERROR_CODE,
    );
    ensure(
      controllerForeground.restoreContextContractVersion === 3,
      OUTCOME_ERROR_CODE,
    );
    controllerBindings = exactFrozenRecord({
      controllerStart: binding(normalizedController, "start"),
      controllerStop: binding(normalizedController, "stop"),
      provisionStablePlan: binding(
        controllerProvisioning,
        "provisionStablePlan",
      ),
      reconcileLaunchAttempt: binding(
        controllerWriterLaunch,
        "reconcileLaunchAttempt",
      ),
      runLaunch: binding(controllerWriterLaunch, "runLaunch"),
      runRestore: binding(controllerForeground, "runRestore"),
    });
  } catch {
    startConstructionCleanup(poolRecords);
    fail(OUTCOME_ERROR_CODE);
  }

  const ingressContexts = new AsyncLocalStorageConstructor();
  const ingressContext = objectFreeze(objectCreate(null));
  const fatalScope = new AsyncResourceConstructor(
    "PostgresDetachedRestoreDeploymentFatalShutdown",
  );
  let state = "idle";
  let startPromise = null;
  let stopPromise = null;
  let poolClosePromise = null;
  let fatalShutdown = false;

  async function topologyPreflight() {
    const clients = [null, null, null, null];
    const acquiredLocks = [false, false, false, false];
    let failure = false;
    try {
      for (let index = 0; index < poolRecords.length; index += 1) {
        ensure(
          state === "starting" && !fatalShutdown,
          OUTCOME_ERROR_CODE,
        );
        const ownedClient = await checkoutPool(poolRecords[index]);
        clients[index] = ownedClient;
        clients[index] = completeClientRecord(ownedClient);
        ensure(
          state === "starting" && !fatalShutdown,
          OUTCOME_ERROR_CODE,
        );
      }
      const topologyRows = [null, null, null, null];
      for (let index = 0; index < clients.length; index += 1) {
        ensure(
          state === "starting" && !fatalShutdown,
          OUTCOME_ERROR_CODE,
        );
        const topologyResult = await invokeClientQuery(
          clients[index],
          TOPOLOGY_SQL,
        );
        ensure(
          state === "starting" && !fatalShutdown,
          OUTCOME_ERROR_CODE,
        );
        topologyRows[index] = topologyRow(
          topologyResult,
          postgres.database,
          postgres.user,
        );
      }
      for (let left = 0; left < topologyRows.length; left += 1) {
        for (let right = left + 1; right < topologyRows.length; right += 1) {
          ensure(
            topologyRows[left].backend_pid !== topologyRows[right].backend_pid,
            OUTCOME_ERROR_CODE,
          );
        }
      }
      for (let index = 0; index < clients.length; index += 1) {
        ensure(
          state === "starting" && !fatalShutdown,
          OUTCOME_ERROR_CODE,
        );
        const row = oneExactRow(
          await invokeClientQuery(clients[index], TRY_LOCK_SQL, probeKey),
          BOOLEAN_ROW_KEYS,
        );
        ensure(typeof row.acquired === "boolean", OUTCOME_ERROR_CODE);
        acquiredLocks[index] = row.acquired;
        ensure(row.acquired === (index === 0), OUTCOME_ERROR_CODE);
        ensure(
          state === "starting" && !fatalShutdown,
          OUTCOME_ERROR_CODE,
        );
      }
    } catch {
      failure = true;
    }
    for (let index = acquiredLocks.length - 1; index >= 0; index -= 1) {
      if (!acquiredLocks[index] || clients[index] === null) continue;
      try {
        const row = oneExactRow(
          await invokeClientQuery(clients[index], UNLOCK_SQL, probeKey),
          UNLOCK_ROW_KEYS,
        );
        if (row.unlocked !== true) failure = true;
      } catch {
        failure = true;
      }
    }
    for (let index = clients.length - 1; index >= 0; index -= 1) {
      if (!releaseClient(clients[index])) failure = true;
    }
    ensure(!failure, OUTCOME_ERROR_CODE);
  }

  function closePools() {
    if (poolClosePromise !== null) return poolClosePromise;
    const closeInternal = async () => {
      const recovery = settlePromise(endPool(poolRecords[3]));
      const foreground = settlePromise(endPool(poolRecords[2]));
      const operation = settlePromise(endPool(poolRecords[1]));
      const authority = settlePromise(endPool(poolRecords[0]));
      const recoveryResult = await recovery;
      const foregroundResult = await foreground;
      const operationResult = await operation;
      const authorityResult = await authority;
      ensure(
        recoveryResult.ok &&
          foregroundResult.ok &&
          operationResult.ok &&
          authorityResult.ok,
        OUTCOME_ERROR_CODE,
      );
    };
    poolClosePromise = protectPromise(closeInternal());
    return poolClosePromise;
  }

  async function stopInternal(activeStart) {
    const controllerStop = settleInvocation(
      controllerBindings.controllerStop,
      [],
    );
    const startSettlement =
      activeStart === null
        ? settlement({ error: null, ok: true, value: null })
        : await settlePromise(activeStart);
    void startSettlement;
    const controllerSettlement = await controllerStop;
    const poolSettlement = await settlePromise(closePools());
    try {
      callIntrinsic(asyncResourceEmitDestroyIntrinsic, fatalScope, []);
    } catch {
      fatalShutdown = true;
    }
    if (!controllerSettlement.ok || !poolSettlement.ok || fatalShutdown) {
      state = "failed";
      fail(OUTCOME_ERROR_CODE);
    }
    state = "stopped";
    return STOPPED_RESULT;
  }

  function beginStop(fatal) {
    if (state !== "stopped") fatalShutdown = fatalShutdown || fatal;
    if (stopPromise !== null) return stopPromise;
    const activeStart = startPromise;
    state = "stopping";
    stopPromise = protectPromise(stopInternal(activeStart));
    return stopPromise;
  }

  async function cleanupAfterStartFailure() {
    const controllerSettlement = await settleInvocation(
      controllerBindings.controllerStop,
      [],
    );
    const poolSettlement = await settlePromise(closePools());
    let cleanupFailed = !controllerSettlement.ok || !poolSettlement.ok;
    try {
      callIntrinsic(asyncResourceEmitDestroyIntrinsic, fatalScope, []);
    } catch {
      cleanupFailed = true;
    }
    if (cleanupFailed || fatalShutdown) {
      state = "failed";
      fail(OUTCOME_ERROR_CODE);
    }
    state = "stopped";
    return STOPPED_RESULT;
  }

  async function startInternal() {
    try {
      await protectPromise(topologyPreflight());
      ensure(state === "starting", OUTCOME_ERROR_CODE);
      const receipt = await invokePromise(controllerBindings.controllerStart, []);
      validateStatus(receipt, "ready");
      ensure(state === "starting", OUTCOME_ERROR_CODE);
      state = "ready";
      return READY_RESULT;
    } catch {
      if (state === "starting") {
        state = "stopping";
        stopPromise = protectPromise(cleanupAfterStartFailure());
        await settlePromise(stopPromise);
      }
      fail(OUTCOME_ERROR_CODE);
    }
  }

  function runIngress(bindingValue, invocationArgs) {
    ensure(state === "ready", REQUEST_ERROR_CODE);
    const invokeAdmitted = () => invokePromise(bindingValue, invocationArgs);
    objectFreeze(invokeAdmitted);
    return callIntrinsic(asyncLocalStorageRunIntrinsic, ingressContexts, [
      ingressContext,
      invokeAdmitted,
    ]);
  }

  function ingress(bindingValue) {
    const method = function deploymentIngress(...invocationArgs) {
      return runIngress(bindingValue, invocationArgs);
    };
    return objectFreeze(method);
  }

  function stopFromIngressContext() {
    return (
      callIntrinsic(asyncLocalStorageGetStoreIntrinsic, ingressContexts, []) ===
      ingressContext
    );
  }

  function observeFatalStop() {
    const stopped = beginStop(true);
    void settlePromise(stopped);
  }
  objectFreeze(observeFatalStop);

  function handlePoolError() {
    try {
      callIntrinsic(asyncResourceRunInAsyncScopeIntrinsic, fatalScope, [
        observeFatalStop,
        undefined,
      ]);
    } catch {
      try {
        void settlePromise(beginStop(true));
      } catch {
        // An error event must never escape its EventEmitter boundary. A
        // synchronous shutdown construction failure is already terminal.
      }
    }
  }
  objectFreeze(handlePoolError);
  requestFatalShutdown = handlePoolError;

  try {
    for (let index = 0; index < poolRecords.length; index += 1) {
      callIntrinsic(eventEmitterOnIntrinsic, poolRecords[index].pool, [
        "error",
        handlePoolError,
      ]);
    }
  } catch {
    void settlePromise(beginStop(true));
    fail(OUTCOME_ERROR_CODE);
  }

  const start = function start(...startArgs) {
    ensure(startArgs.length === 0, REQUEST_ERROR_CODE);
    ensure(
      state === "idle" || state === "starting" || state === "ready",
      REQUEST_ERROR_CODE,
    );
    if (state !== "idle") return startPromise;
    state = "starting";
    const startSeed = protectPromise(
      callIntrinsic(promiseResolveIntrinsic, PromiseConstructor, [undefined]),
    );
    startPromise = callIntrinsic(protectedPromiseThen, startSeed, [
      startInternal,
    ]);
    return startPromise;
  };

  const stop = function stop(...stopArgs) {
    ensure(stopArgs.length === 0, REQUEST_ERROR_CODE);
    ensure(!stopFromIngressContext(), REQUEST_ERROR_CODE);
    return beginStop(false);
  };

  objectFreeze(start);
  objectFreeze(stop);
  const deployment = exactFrozenRecord({
    foreground: exactFrozenRecord({
      restoreContextContractVersion: 3,
      runRestore: ingress(controllerBindings.runRestore),
    }),
    stablePlanProvisioning: exactFrozenRecord({
      provisionStablePlan: ingress(controllerBindings.provisionStablePlan),
    }),
    start,
    stop,
    writerLaunch: exactFrozenRecord({
      reconcileLaunchAttempt: ingress(
        controllerBindings.reconcileLaunchAttempt,
      ),
      runLaunch: ingress(controllerBindings.runLaunch),
    }),
  });
  weakSetAdd(deploymentBrands, deployment);
  return deployment;
}

export function isPostgresDetachedRestoreDeployment(value) {
  try {
    return (
      arguments.length === 1 &&
      value !== null &&
      typeof value === "object" &&
      !isProxyValue(value) &&
      weakSetHas(deploymentBrands, value)
    );
  } catch {
    return false;
  }
}

objectFreeze(PostgresDetachedRestoreDeploymentError.prototype);
objectFreeze(PostgresDetachedRestoreDeploymentError);
objectFreeze(createPostgresDetachedRestoreDeployment);
objectFreeze(isPostgresDetachedRestoreDeployment);
