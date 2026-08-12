import { AsyncLocalStorage } from "node:async_hooks";
import { types as utilTypes } from "node:util";

import {
  isPostgresDetachedRestoreRuntimeComposition,
} from "./postgres-detached-restore-runtime-composition.mjs";
import {
  SESSION_AUTHORITY_MIGRATION_VERSION,
} from "./postgres-serializable-store.mjs";

const arrayIncludesIntrinsic = Array.prototype.includes;
const arrayIsArray = Array.isArray;
const AsyncLocalStorageConstructor = AsyncLocalStorage;
const asyncLocalStorageGetStoreIntrinsic =
  AsyncLocalStorage.prototype.getStore;
const asyncLocalStorageRunIntrinsic = AsyncLocalStorage.prototype.run;
const ErrorConstructor = Error;
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
const TypeErrorConstructor = TypeError;
const WeakSetConstructor = WeakSet;
const weakSetAddIntrinsic = WeakSet.prototype.add;
const weakSetHasIntrinsic = WeakSet.prototype.has;

const isRuntimeCompositionIntrinsic =
  isPostgresDetachedRestoreRuntimeComposition;

const OPTION_KEYS = objectFreeze(["runtime"]);
const RUNTIME_KEYS = objectFreeze([
  "backend",
  "bootstrap",
  "foreground",
  "scheduler",
  "stablePlanProvisioning",
  "writerLaunch",
]);
const BOOTSTRAP_KEYS = objectFreeze(["migrate"]);
const FOREGROUND_KEYS = objectFreeze([
  "restoreContextContractVersion",
  "runRestore",
]);
const SCHEDULER_KEYS = objectFreeze(["runStep", "start", "stop"]);
const STABLE_PLAN_PROVISIONING_KEYS = objectFreeze(["provisionStablePlan"]);
const WRITER_LAUNCH_KEYS = objectFreeze([
  "reconcileLaunchAttempt",
  "runLaunch",
]);
const MIGRATION_RESULT_KEYS = objectFreeze([
  "applied",
  "checksum",
  "version",
]);
const SCHEDULER_STEP_KEYS = objectFreeze([
  "errorCode",
  "recovery",
  "status",
]);
const RECOVERY_RESULT_KEYS = objectFreeze([
  "activation",
  "currentLaunch",
  "generation",
  "launchAttempt",
  "recoveryScopeId",
  "status",
]);
const SHA256_PATTERN = /^[0-9a-f]{64}$/u;

const OPTION_ERROR_CODE =
  "invalid_postgres_detached_restore_runtime_controller_options";
const REQUEST_ERROR_CODE =
  "invalid_postgres_detached_restore_runtime_controller_request";
const OUTCOME_ERROR_CODE =
  "postgres_detached_restore_runtime_controller_outcome_uncertain";
const ERROR_MESSAGES = objectFreeze({
  [OPTION_ERROR_CODE]:
    "PostgreSQL detached restore runtime controller options are invalid",
  [REQUEST_ERROR_CODE]:
    "PostgreSQL detached restore runtime controller request is invalid",
  [OUTCOME_ERROR_CODE]:
    "PostgreSQL detached restore runtime controller outcome is uncertain",
});

const controllerBrands = new WeakSetConstructor();
const controllerErrorBrands = new WeakSetConstructor();
const claimedRuntimeCompositions = new WeakSetConstructor();
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

export class PostgresDetachedRestoreRuntimeControllerError extends ErrorConstructor {
  constructor(code) {
    if (typeof code !== "string" || !objectHasOwn(ERROR_MESSAGES, code)) {
      throw new TypeErrorConstructor(
        "Unsupported PostgreSQL detached restore runtime controller error",
      );
    }
    const message = ERROR_MESSAGES[code];
    super(message);
    objectDefineProperty(this, "name", {
      configurable: false,
      enumerable: false,
      value: "PostgresDetachedRestoreRuntimeControllerError",
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
      value: `PostgresDetachedRestoreRuntimeControllerError: ${message}`,
      writable: false,
    });
    objectFreeze(this);
  }
}

function makeError(code) {
  const error = new PostgresDetachedRestoreRuntimeControllerError(code);
  weakSetAdd(controllerErrorBrands, error);
  return error;
}

function fail(code) {
  throw makeError(code);
}

function ensure(condition, code) {
  if (!condition) fail(code);
}

function isControllerError(value) {
  return (
    value !== null &&
    (typeof value === "object" || typeof value === "function") &&
    !isProxyValue(value) &&
    weakSetHas(controllerErrorBrands, value)
  );
}

function exactFrozenRecord(value) {
  const result = objectCreate(null);
  const keys = reflectOwnKeys(value);
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
    objectDefineProperty(result, key, {
      configurable: false,
      enumerable: true,
      value: value[key],
      writable: false,
    });
  }
  return objectFreeze(result);
}

function exactDataObject(value, expectedKeys, code, options = undefined) {
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
  const requireFrozen = options?.frozen === true;
  const requireNullPrototype = options?.nullPrototype === true;
  ensure(
    (requireNullPrototype
      ? prototype === null
      : prototype === objectPrototype || prototype === null) &&
      (!requireFrozen || objectIsFrozen(value)) &&
      keys.length === expectedKeys.length,
    code,
  );
  const result = objectCreate(null);
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
    result[key] = descriptor.value;
  }
  for (let index = 0; index < expectedKeys.length; index += 1) {
    ensure(objectHasOwn(result, expectedKeys[index]), code);
  }
  return result;
}

function exactFrozenSurface(value, expectedKeys, code) {
  return exactDataObject(value, expectedKeys, code, {
    frozen: true,
    nullPrototype: true,
  });
}

function trustedFunction(value, code) {
  ensure(
    typeof value === "function" &&
      !isProxyValue(value) &&
      !isGeneratorFunctionValue(value) &&
      objectIsFrozen(value),
    code,
  );
  return value;
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

function safePromiseReactionDescriptor(descriptor) {
  return (
    descriptor?.configurable === false &&
    descriptor.enumerable === false &&
    descriptor.writable === false &&
    objectHasOwn(descriptor, "value") &&
    typeof descriptor.value === "function" &&
    !isProxyValue(descriptor.value) &&
    !isGeneratorFunctionValue(descriptor.value)
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
  let descriptor;
  let keys;
  let prototype;
  try {
    descriptor = objectGetOwnPropertyDescriptor(value, promiseSpeciesSymbol);
    keys = reflectOwnKeys(value);
    prototype = objectGetPrototypeOf(value);
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

function protectPromise(value) {
  if (!isPromiseValue(value)) return value;
  ensure(
    !isProxyValue(value) &&
      !isGeneratorObjectValue(value) &&
      objectGetPrototypeOf(value) === promisePrototype,
    OUTCOME_ERROR_CODE,
  );
  let catchDescriptor;
  let constructorDescriptor;
  let finallyDescriptor;
  let thenDescriptor;
  try {
    catchDescriptor = objectGetOwnPropertyDescriptor(value, "catch");
    constructorDescriptor = objectGetOwnPropertyDescriptor(value, "constructor");
    finallyDescriptor = objectGetOwnPropertyDescriptor(value, "finally");
    thenDescriptor = objectGetOwnPropertyDescriptor(value, "then");
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
    ensure(
      safePromiseReactionDescriptor(finallyDescriptor),
      OUTCOME_ERROR_CODE,
    );
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

function invokePromise(binding, args) {
  let pending;
  try {
    pending = callIntrinsic(binding.method, binding.receiver, args);
  } catch {
    fail(OUTCOME_ERROR_CODE);
  }
  ensure(isPromiseValue(pending), OUTCOME_ERROR_CODE);
  return protectPromise(pending);
}

function binding(surface, name, code) {
  return objectFreeze({
    method: trustedFunction(surface[name], code),
    receiver: surface,
  });
}

function runtimeBindings(runtime) {
  ensure(
    objectIsFrozen(isRuntimeCompositionIntrinsic) &&
      callIntrinsic(isRuntimeCompositionIntrinsic, undefined, [runtime]),
    OPTION_ERROR_CODE,
  );
  const normalizedRuntime = exactFrozenSurface(
    runtime,
    RUNTIME_KEYS,
    OPTION_ERROR_CODE,
  );
  const bootstrap = exactFrozenSurface(
    normalizedRuntime.bootstrap,
    BOOTSTRAP_KEYS,
    OPTION_ERROR_CODE,
  );
  const foreground = exactFrozenSurface(
    normalizedRuntime.foreground,
    FOREGROUND_KEYS,
    OPTION_ERROR_CODE,
  );
  const scheduler = exactFrozenSurface(
    normalizedRuntime.scheduler,
    SCHEDULER_KEYS,
    OPTION_ERROR_CODE,
  );
  const stablePlanProvisioning = exactFrozenSurface(
    normalizedRuntime.stablePlanProvisioning,
    STABLE_PLAN_PROVISIONING_KEYS,
    OPTION_ERROR_CODE,
  );
  const writerLaunch = exactFrozenSurface(
    normalizedRuntime.writerLaunch,
    WRITER_LAUNCH_KEYS,
    OPTION_ERROR_CODE,
  );
  ensure(foreground.restoreContextContractVersion === 3, OPTION_ERROR_CODE);
  return exactFrozenRecord({
    migrate: binding(bootstrap, "migrate", OPTION_ERROR_CODE),
    reconcileLaunchAttempt: binding(
      writerLaunch,
      "reconcileLaunchAttempt",
      OPTION_ERROR_CODE,
    ),
    runLaunch: binding(writerLaunch, "runLaunch", OPTION_ERROR_CODE),
    runRestore: binding(foreground, "runRestore", OPTION_ERROR_CODE),
    runStep: binding(scheduler, "runStep", OPTION_ERROR_CODE),
    schedulerStart: binding(scheduler, "start", OPTION_ERROR_CODE),
    schedulerStop: binding(scheduler, "stop", OPTION_ERROR_CODE),
    provisionStablePlan: binding(
      stablePlanProvisioning,
      "provisionStablePlan",
      OPTION_ERROR_CODE,
    ),
  });
}

function validateMigrationResult(value) {
  const result = exactDataObject(
    value,
    MIGRATION_RESULT_KEYS,
    OUTCOME_ERROR_CODE,
    { frozen: true },
  );
  ensure(
    typeof result.applied === "boolean" &&
      typeof result.checksum === "string" &&
      regexpTest(SHA256_PATTERN, result.checksum) &&
      result.version === SESSION_AUTHORITY_MIGRATION_VERSION,
    OUTCOME_ERROR_CODE,
  );
}

function validateInitialRecoveryReceipt(value) {
  const receipt = exactDataObject(
    value,
    SCHEDULER_STEP_KEYS,
    OUTCOME_ERROR_CODE,
    { frozen: true, nullPrototype: true },
  );
  ensure(
    receipt.status === "completed" && receipt.errorCode === null,
    OUTCOME_ERROR_CODE,
  );
  const recovery = exactDataObject(
    receipt.recovery,
    RECOVERY_RESULT_KEYS,
    OUTCOME_ERROR_CODE,
    { frozen: true, nullPrototype: true },
  );
  ensure(recovery.status === "sweep-complete", OUTCOME_ERROR_CODE);
}

function settlement(value) {
  return exactFrozenRecord(value);
}

async function settlePromise(pending) {
  try {
    return settlement({ error: null, ok: true, value: await pending });
  } catch (error) {
    return settlement({ error, ok: false, value: null });
  }
}

export function createPostgresDetachedRestoreRuntimeController(...args) {
  ensure(args.length === 1, OPTION_ERROR_CODE);
  const options = exactDataObject(args[0], OPTION_KEYS, OPTION_ERROR_CODE);
  const runtime = options.runtime;
  const bindings = runtimeBindings(runtime);
  const admittedIngressContexts = new AsyncLocalStorageConstructor();
  const admittedIngressContext = objectFreeze(objectCreate(null));
  ensure(!weakSetHas(claimedRuntimeCompositions, runtime), OPTION_ERROR_CODE);
  weakSetAdd(claimedRuntimeCompositions, runtime);

  let state = "idle";
  let startPromise = null;
  let stopPromise = null;
  let schedulerCompletion = null;
  let inFlightIngress = 0;
  let ingressDrainPromise = null;
  let resolveIngressDrain = null;

  function closeIngressRecord() {
    if (inFlightIngress !== 0 || resolveIngressDrain === null) return;
    const resolve = resolveIngressDrain;
    resolveIngressDrain = null;
    callIntrinsic(resolve, undefined, [undefined]);
  }

  function waitForIngressDrain() {
    if (inFlightIngress === 0) return resolveProtectedPromise(undefined);
    if (ingressDrainPromise !== null) return ingressDrainPromise;
    ingressDrainPromise = protectPromise(
      new PromiseConstructor((resolve) => {
        resolveIngressDrain = resolve;
      }),
    );
    return ingressDrainPromise;
  }

  async function runIngress(bindingValue, invocationArgs) {
    ensure(state === "ready", REQUEST_ERROR_CODE);
    inFlightIngress += 1;
    try {
      return await invokePromise(bindingValue, invocationArgs);
    } finally {
      inFlightIngress -= 1;
      closeIngressRecord();
    }
  }

  function ingress(bindingValue) {
    const method = function controlledIngress(...invocationArgs) {
      const invokeAdmittedIngress = () =>
        protectPromise(runIngress(bindingValue, invocationArgs));
      objectFreeze(invokeAdmittedIngress);
      return callIntrinsic(
        asyncLocalStorageRunIntrinsic,
        admittedIngressContexts,
        [admittedIngressContext, invokeAdmittedIngress],
      );
    };
    return objectFreeze(method);
  }

  function stopFromAdmittedIngressContext() {
    return (
      callIntrinsic(
        asyncLocalStorageGetStoreIntrinsic,
        admittedIngressContexts,
        [],
      ) === admittedIngressContext
    );
  }

  function observeSchedulerCompletion(pending) {
    const observed = protectPromise(pending);
    let child;
    try {
      child = callIntrinsic(promiseThenIntrinsic, observed, [
        () => {
          if (state === "starting" || state === "ready") state = "failed";
        },
        () => {
          if (state === "starting" || state === "ready") state = "failed";
        },
      ]);
    } catch {
      fail(OUTCOME_ERROR_CODE);
    }
    void protectPromise(child);
  }

  async function stopSchedulerAfterStartFailure() {
    let stopped;
    try {
      stopped = invokePromise(bindings.schedulerStop, []);
    } catch {
      return;
    }
    await settlePromise(stopped);
  }

  async function startInternal() {
    try {
      const migration = await invokePromise(bindings.migrate, []);
      validateMigrationResult(migration);
      ensure(state === "starting", OUTCOME_ERROR_CODE);

      schedulerCompletion = invokePromise(bindings.schedulerStart, []);
      observeSchedulerCompletion(schedulerCompletion);
      const initialStep = invokePromise(bindings.runStep, [
        exactFrozenRecord({ signal: null }),
      ]);
      const receipt = await initialStep;
      validateInitialRecoveryReceipt(receipt);
      ensure(state === "starting", OUTCOME_ERROR_CODE);
      state = "ready";
      return READY_RESULT;
    } catch (error) {
      if (state === "starting") state = "failed";
      await stopSchedulerAfterStartFailure();
      if (isControllerError(error) && error.code === OUTCOME_ERROR_CODE) {
        throw error;
      }
      throw makeError(OUTCOME_ERROR_CODE);
    }
  }

  async function stopInternal(schedulerStopCompletion, activeStart) {
    const ingressDrain = waitForIngressDrain();
    const schedulerSettlement = await settlePromise(schedulerStopCompletion);
    const startSettlement =
      activeStart === null
        ? settlement({ error: null, ok: true, value: null })
        : await settlePromise(activeStart);
    const ingressSettlement = await settlePromise(ingressDrain);
    if (!schedulerSettlement.ok || !ingressSettlement.ok) {
      state = "failed";
      fail(OUTCOME_ERROR_CODE);
    }
    // A stop racing startup intentionally makes that start reject. Its
    // settlement is drained here but does not weaken the completed stop
    // barrier once the scheduler and every admitted ingress have drained.
    void startSettlement;
    state = "stopped";
    return STOPPED_RESULT;
  }

  const start = function start(...startArgs) {
    ensure(startArgs.length === 0, REQUEST_ERROR_CODE);
    ensure(
      state === "idle" || state === "starting" || state === "ready",
      REQUEST_ERROR_CODE,
    );
    if (state !== "idle") return startPromise;
    state = "starting";
    startPromise = protectPromise(startInternal());
    return startPromise;
  };

  const stop = function stop(...stopArgs) {
    ensure(stopArgs.length === 0, REQUEST_ERROR_CODE);
    ensure(!stopFromAdmittedIngressContext(), REQUEST_ERROR_CODE);
    if (stopPromise !== null) return stopPromise;
    state = "stopping";
    let schedulerStopCompletion;
    try {
      schedulerStopCompletion = invokePromise(bindings.schedulerStop, []);
    } catch (error) {
      schedulerStopCompletion = protectPromise(
        new PromiseConstructor((resolve, reject) => {
          void resolve;
          callIntrinsic(reject, undefined, [error]);
        }),
      );
    }
    stopPromise = protectPromise(
      stopInternal(schedulerStopCompletion, startPromise),
    );
    return stopPromise;
  };

  objectFreeze(start);
  objectFreeze(stop);
  const controller = exactFrozenRecord({
    foreground: exactFrozenRecord({
      restoreContextContractVersion: 3,
      runRestore: ingress(bindings.runRestore),
    }),
    stablePlanProvisioning: exactFrozenRecord({
      provisionStablePlan: ingress(bindings.provisionStablePlan),
    }),
    start,
    stop,
    writerLaunch: exactFrozenRecord({
      reconcileLaunchAttempt: ingress(bindings.reconcileLaunchAttempt),
      runLaunch: ingress(bindings.runLaunch),
    }),
  });
  weakSetAdd(controllerBrands, controller);
  return controller;
}

export function isPostgresDetachedRestoreRuntimeController(value) {
  try {
    return (
      arguments.length === 1 &&
      value !== null &&
      typeof value === "object" &&
      !isProxyValue(value) &&
      weakSetHas(controllerBrands, value)
    );
  } catch {
    return false;
  }
}

objectFreeze(PostgresDetachedRestoreRuntimeControllerError.prototype);
objectFreeze(PostgresDetachedRestoreRuntimeControllerError);
objectFreeze(createPostgresDetachedRestoreRuntimeController);
objectFreeze(isPostgresDetachedRestoreRuntimeController);
