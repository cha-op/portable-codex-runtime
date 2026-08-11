import { AsyncLocalStorage } from "node:async_hooks";
import { types as utilTypes } from "node:util";

import {
  isPostgresRestoreRecoveryRunner,
  PostgresRestoreRecoveryRunnerError,
} from "./postgres-restore-recovery-runner.mjs";

const arrayIncludesIntrinsic = Array.prototype.includes;
const arrayIsArray = Array.isArray;
const asyncLocalStorageGetStoreIntrinsic =
  AsyncLocalStorage.prototype.getStore;
const asyncLocalStorageRunIntrinsic = AsyncLocalStorage.prototype.run;
const clearTimeoutIntrinsic = globalThis.clearTimeout;
const ErrorConstructor = Error;
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
const objectIsFrozen = Object.isFrozen;
const objectPrototype = Object.prototype;
const PromiseConstructor = Promise;
const promisePrototype = Promise.prototype;
const promiseResolveIntrinsic = Promise.resolve;
const promiseSpeciesSymbol = Symbol.species;
const promiseThenIntrinsic = Promise.prototype.then;
const reflectApply = Reflect.apply;
const reflectOwnKeys = Reflect.ownKeys;
const setTimeoutIntrinsic = globalThis.setTimeout;
const TypeErrorConstructor = TypeError;
const WeakSetConstructor = WeakSet;
const weakSetAddIntrinsic = WeakSet.prototype.add;
const weakSetHasIntrinsic = WeakSet.prototype.has;

const AbortControllerConstructor = globalThis.AbortController;
const abortControllerAbortIntrinsic = AbortControllerConstructor.prototype.abort;
const abortControllerSignalGetter = objectGetOwnPropertyDescriptor(
  AbortControllerConstructor.prototype,
  "signal",
).get;
const AbortSignalConstructor = globalThis.AbortSignal;
const abortSignalAbortedGetter = objectGetOwnPropertyDescriptor(
  AbortSignalConstructor.prototype,
  "aborted",
).get;
const eventTargetAddEventListenerIntrinsic =
  globalThis.EventTarget.prototype.addEventListener;
const eventTargetRemoveEventListenerIntrinsic =
  globalThis.EventTarget.prototype.removeEventListener;

const MAX_INTERVAL_MILLISECONDS = 86_400_000;
const OPTION_KEYS = objectFreeze([
  "intervalMilliseconds",
  "onStep",
  "runner",
]);
const RUN_STEP_KEYS = objectFreeze(["signal"]);
const ERROR_MESSAGES = objectFreeze({
  invalid_postgres_restore_recovery_scheduler_options:
    "PostgreSQL restore recovery scheduler options are invalid",
  invalid_postgres_restore_recovery_scheduler_request:
    "PostgreSQL restore recovery scheduler request is invalid",
  postgres_restore_recovery_scheduler_outcome_uncertain:
    "PostgreSQL restore recovery scheduler outcome is uncertain",
});
const STEP_OUTCOME_UNCERTAIN =
  "postgres_restore_recovery_scheduler_step_outcome_uncertain";

const schedulerBrands = new WeakSetConstructor();
const schedulerErrorBrands = new WeakSetConstructor();
const observerContexts = new AsyncLocalStorage();
const discardObserverSettlement = objectFreeze(
  function discardObserverSettlement() {},
);
const promiseSpeciesHolder = objectCreate(null);
objectDefineProperty(promiseSpeciesHolder, promiseSpeciesSymbol, {
  configurable: false,
  enumerable: false,
  value: PromiseConstructor,
  writable: false,
});
objectFreeze(promiseSpeciesHolder);

function callIntrinsic(intrinsic, receiver, args) {
  return reflectApply(intrinsic, receiver, args);
}

function weakSetAdd(set, value) {
  callIntrinsic(weakSetAddIntrinsic, set, [value]);
}

function weakSetHas(set, value) {
  return callIntrinsic(weakSetHasIntrinsic, set, [value]);
}

function arrayIncludes(value, candidate) {
  return callIntrinsic(arrayIncludesIntrinsic, value, [candidate]);
}

function fail(code) {
  const error = new PostgresRestoreRecoverySchedulerError(code);
  weakSetAdd(schedulerErrorBrands, error);
  throw error;
}

function ensure(condition, code) {
  if (!condition) fail(code);
}

function exactDataObject(value, expectedKeys, code) {
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
  return result;
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

function signalIsAborted(signal, code) {
  if (signal === null) return false;
  ensure(
    signal !== null &&
      typeof signal === "object" &&
      !isProxyValue(signal),
    code,
  );
  try {
    return callIntrinsic(abortSignalAbortedGetter, signal, []);
  } catch {
    fail(code);
  }
}

function normalizeCallback(value, code) {
  ensure(
    typeof value === "function" &&
      !isProxyValue(value) &&
      !isGeneratorFunctionValue(value),
    code,
  );
  return value;
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
      !isProxyValue(descriptor.value) &&
      !isGeneratorFunctionValue(descriptor.value),
    code,
  );
  return descriptor.value;
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
  try {
    const constructorDescriptor = objectGetOwnPropertyDescriptor(
      value,
      "constructor",
    );
    if (
      constructorDescriptor !== undefined &&
      objectHasOwn(constructorDescriptor, "value") &&
      constructorDescriptor.value !== promiseSpeciesHolder &&
      isSafePromiseSpeciesHolder(constructorDescriptor.value)
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
  } catch {
    fail("postgres_restore_recovery_scheduler_outcome_uncertain");
  }
  return value;
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
  if (descriptor.value === PromiseConstructor) {
    try {
      return protectPromise(value);
    } catch {
      return null;
    }
  }
  if (!isSafePromiseSpeciesHolder(descriptor.value)) return null;
  try {
    const normalized = callIntrinsic(promiseThenIntrinsic, value, [
      undefined,
      undefined,
    ]);
    return protectPromise(normalized);
  } catch {
    return null;
  }
}

function drainObserverPromise(value) {
  const normalized = normalizeSafeNativePromise(value);
  if (normalized === null) return false;
  try {
    callIntrinsic(promiseThenIntrinsic, normalized, [
      discardObserverSettlement,
      discardObserverSettlement,
    ]);
  } catch {
    return false;
  }
  return true;
}

function hasUntrustedThenableShape(value) {
  if (
    value === null ||
    (typeof value !== "object" && typeof value !== "function")
  ) {
    return false;
  }
  let current = value;
  for (let depth = 0; current !== null && depth < 64; depth += 1) {
    if (isProxyValue(current)) return true;
    let descriptor;
    try {
      descriptor = objectGetOwnPropertyDescriptor(current, "then");
      current = objectGetPrototypeOf(current);
    } catch {
      return true;
    }
    if (descriptor === undefined) continue;
    if (!objectHasOwn(descriptor, "value")) return true;
    return typeof descriptor.value === "function";
  }
  return current !== null;
}

function authenticRunnerError(error, code) {
  if (
    error === null ||
    typeof error !== "object" ||
    isProxyValue(error) ||
    objectGetPrototypeOf(error) !== PostgresRestoreRecoveryRunnerError.prototype
  ) {
    return false;
  }
  const descriptor = objectGetOwnPropertyDescriptor(error, "code");
  return descriptor !== undefined &&
    objectHasOwn(descriptor, "value") &&
    descriptor.value === code;
}

function abortControllerSignal(controller) {
  return callIntrinsic(abortControllerSignalGetter, controller, []);
}

function abortControllerAbort(controller) {
  callIntrinsic(abortControllerAbortIntrinsic, controller, []);
}

export class PostgresRestoreRecoverySchedulerError extends ErrorConstructor {
  constructor(code) {
    if (typeof code !== "string" || !objectHasOwn(ERROR_MESSAGES, code)) {
      throw new TypeErrorConstructor(
        "Unsupported PostgreSQL restore recovery scheduler error",
      );
    }
    const message = ERROR_MESSAGES[code];
    super(message);
    objectDefineProperty(this, "name", {
      enumerable: false,
      value: "PostgresRestoreRecoverySchedulerError",
    });
    objectDefineProperty(this, "code", { enumerable: true, value: code });
    objectDefineProperty(this, "retryable", {
      enumerable: true,
      value: false,
    });
    objectDefineProperty(this, "stack", {
      enumerable: false,
      value: `PostgresRestoreRecoverySchedulerError: ${message}`,
    });
    objectFreeze(this);
  }
}

export function createPostgresRestoreRecoveryScheduler(...args) {
  const optionCode = "invalid_postgres_restore_recovery_scheduler_options";
  ensure(args.length === 1, optionCode);
  const options = exactDataObject(args[0], OPTION_KEYS, optionCode);
  ensure(
    numberIsSafeInteger(options.intervalMilliseconds) &&
      options.intervalMilliseconds >= 1 &&
      options.intervalMilliseconds <= MAX_INTERVAL_MILLISECONDS,
    optionCode,
  );
  ensure(isPostgresRestoreRecoveryRunner(options.runner), optionCode);
  const runner = options.runner;
  const runOnce = collaboratorMethod(runner, "runOnce", optionCode);
  const onStep = normalizeCallback(options.onStep, optionCode);
  const intervalMilliseconds = options.intervalMilliseconds;
  const requestCode = "invalid_postgres_restore_recovery_scheduler_request";
  const outcomeCode = "postgres_restore_recovery_scheduler_outcome_uncertain";

  let state = "idle";
  let completion = null;
  let activeStep = null;
  let activeController = null;
  let activeDelayCancel = null;
  let terminalStepError = null;
  let observerOpen = false;
  const observerContext = objectFreeze(objectCreate(null));
  const stoppedResult = exactFrozenRecord({ status: "stopped" });

  function observerReentryIsActive() {
    return callIntrinsic(
      asyncLocalStorageGetStoreIntrinsic,
      observerContexts,
      [],
    ) === observerContext;
  }

  async function callObserverInternal(receipt) {
    ensure(!observerOpen, outcomeCode);
    observerOpen = true;
    let value;
    try {
      const invokeObserver = () =>
        callIntrinsic(onStep, undefined, [receipt]);
      value = callIntrinsic(
        asyncLocalStorageRunIntrinsic,
        observerContexts,
        [observerContext, invokeObserver],
      );
      if (isGeneratorObjectValue(value)) fail(outcomeCode);
      if (isPromiseValue(value)) {
        ensure(drainObserverPromise(value), outcomeCode);
        fail(outcomeCode);
      }
      ensure(
        value === undefined &&
          !isGeneratorObjectValue(value) &&
          !hasUntrustedThenableShape(value),
        outcomeCode,
      );
    } catch {
      fail(outcomeCode);
    } finally {
      observerOpen = false;
    }
  }

  function callObserver(receipt) {
    return protectPromise(callObserverInternal(receipt));
  }

  async function callRunnerInternal(signal) {
    let value;
    try {
      value = callIntrinsic(runOnce, runner, [
        exactFrozenRecord({ signal }),
      ]);
      value = normalizeSafeNativePromise(value);
      ensure(value !== null, outcomeCode);
      return await value;
    } catch (error) {
      if (
        authenticRunnerError(
          error,
          "postgres_restore_recovery_runner_busy",
        )
      ) {
        return exactFrozenRecord({
          errorCode: null,
          recovery: null,
          status: "busy",
        });
      }
      return exactFrozenRecord({
        errorCode: STEP_OUTCOME_UNCERTAIN,
        recovery: null,
        status: "outcome-uncertain",
      });
    }
  }

  function callRunner(signal) {
    return protectPromise(callRunnerInternal(signal));
  }

  function beginStep(externalSignal) {
    if (activeStep !== null) return activeStep;
    const controller = new AbortControllerConstructor();
    const signal = abortControllerSignal(controller);
    activeController = controller;
    let externalAbortListener = null;
    if (externalSignal !== null) {
      externalAbortListener = () => abortControllerAbort(controller);
      try {
        callIntrinsic(eventTargetAddEventListenerIntrinsic, externalSignal, [
          "abort",
          externalAbortListener,
          objectFreeze({ once: true }),
        ]);
        if (signalIsAborted(externalSignal, requestCode)) {
          abortControllerAbort(controller);
        }
      } catch {
        activeController = null;
        fail(requestCode);
      }
    }

    const cleanup = () => {
      let cleanupFailed = false;
      if (externalAbortListener !== null) {
        try {
          callIntrinsic(
            eventTargetRemoveEventListenerIntrinsic,
            externalSignal,
            ["abort", externalAbortListener],
          );
        } catch {
          cleanupFailed = true;
        }
      }
      activeController = null;
      activeStep = null;
      if (cleanupFailed) fail(outcomeCode);
    };

    const recordStepFailure = (error) => {
      if (state !== "running") return;
      state = "failed";
      terminalStepError = error;
      if (activeDelayCancel !== null) activeDelayCancel();
    };

    let rejectStep;
    let resolveStep;
    const step = protectPromise(
      new PromiseConstructor((resolve, reject) => {
        rejectStep = reject;
        resolveStep = resolve;
      }),
    );
    activeStep = step;

    const runStepInternal = async () => {
      const raw = await callRunner(signal);
      const receipt =
        raw?.status === "busy" || raw?.status === "outcome-uncertain"
          ? raw
          : exactFrozenRecord({
              errorCode: null,
              recovery: raw,
              status: "completed",
            });
      await callObserver(receipt);
      return receipt;
    };
    const settleStep = (status, value) => {
      if (status === "rejected") {
        try {
          cleanup();
        } catch (cleanupError) {
          value = cleanupError;
        }
        recordStepFailure(value);
        rejectStep(value);
        return;
      }
      try {
        cleanup();
      } catch (error) {
        recordStepFailure(error);
        rejectStep(error);
        return;
      }
      resolveStep(value);
    };
    let pending;
    try {
      pending = protectPromise(runStepInternal());
    } catch (error) {
      settleStep("rejected", error);
      return step;
    }
    callIntrinsic(promiseThenIntrinsic, pending, [
      (value) => settleStep("fulfilled", value),
      (error) => settleStep("rejected", error),
    ]);
    return step;
  }

  function waitForInterval() {
    return protectPromise(
      new PromiseConstructor((resolve) => {
        let settled = false;
        let timer = null;
        const finish = () => {
          if (settled) return;
          settled = true;
          if (timer !== null) {
            callIntrinsic(clearTimeoutIntrinsic, undefined, [timer]);
            timer = null;
          }
          activeDelayCancel = null;
          resolve();
        };
        activeDelayCancel = finish;
        timer = callIntrinsic(setTimeoutIntrinsic, undefined, [
          finish,
          intervalMilliseconds,
        ]);
      }),
    );
  }

  async function runLoop() {
    try {
      while (state === "running") {
        await beginStep(null);
        if (state !== "running") break;
        await waitForInterval();
      }
      if (activeStep !== null) await activeStep;
      if (terminalStepError !== null) throw terminalStepError;
      if (state === "stopping") state = "stopped";
      return stoppedResult;
    } catch (error) {
      state = "failed";
      if (weakSetHas(schedulerErrorBrands, error)) throw error;
      fail(outcomeCode);
    } finally {
      if (activeDelayCancel !== null) activeDelayCancel();
    }
  }

  const runStep = function runStep(...runArgs) {
    ensure(!observerReentryIsActive(), requestCode);
    ensure(runArgs.length === 1, requestCode);
    const request = exactDataObject(runArgs[0], RUN_STEP_KEYS, requestCode);
    signalIsAborted(request.signal, requestCode);
    ensure(state === "running", requestCode);
    return beginStep(request.signal);
  };

  const start = function start(...startArgs) {
    ensure(!observerReentryIsActive(), requestCode);
    ensure(startArgs.length === 0, requestCode);
    if (state === "running") return completion;
    ensure(state === "idle", requestCode);
    state = "running";
    let rejectCompletion;
    let resolveCompletion;
    completion = protectPromise(
      new PromiseConstructor((resolve, reject) => {
        rejectCompletion = reject;
        resolveCompletion = resolve;
      }),
    );
    let loop;
    try {
      loop = protectPromise(runLoop());
    } catch (error) {
      state = "failed";
      rejectCompletion(error);
      return completion;
    }
    callIntrinsic(promiseThenIntrinsic, loop, [
      resolveCompletion,
      rejectCompletion,
    ]);
    return completion;
  };

  const stop = function stop(...stopArgs) {
    ensure(!observerReentryIsActive(), requestCode);
    ensure(stopArgs.length === 0, requestCode);
    if (state === "idle") {
      state = "stopped";
      completion = resolveProtectedPromise(stoppedResult);
      return completion;
    }
    if (state === "running") {
      state = "stopping";
      if (activeController !== null) abortControllerAbort(activeController);
      if (activeDelayCancel !== null) activeDelayCancel();
    }
    ensure(
      state === "stopping" || state === "stopped" || state === "failed",
      requestCode,
    );
    return completion;
  };

  objectFreeze(runStep);
  objectFreeze(start);
  objectFreeze(stop);
  const scheduler = exactFrozenRecord({ runStep, start, stop });
  weakSetAdd(schedulerBrands, scheduler);
  return scheduler;
}

export function isPostgresRestoreRecoveryScheduler(value) {
  return (
    value !== null &&
    (typeof value === "object" || typeof value === "function") &&
    !isProxyValue(value) &&
    weakSetHas(schedulerBrands, value)
  );
}

objectFreeze(PostgresRestoreRecoverySchedulerError.prototype);
