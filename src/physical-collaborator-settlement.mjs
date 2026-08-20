import { performance as nodePerformance } from "node:perf_hooks";
import { types as utilTypes } from "node:util";

const AbortControllerConstructor = globalThis.AbortController;
const abortControllerAbortIntrinsic = AbortControllerConstructor.prototype.abort;
const abortControllerSignalGetter = Object.getOwnPropertyDescriptor(
  AbortControllerConstructor.prototype,
  "signal",
).get;
const arrayIncludesIntrinsic = Array.prototype.includes;
const arrayIsArray = Array.isArray;
const clearTimeoutIntrinsic = globalThis.clearTimeout;
const numberIsSafeIntegerIntrinsic = Number.isSafeInteger;
const numberIsFiniteIntrinsic = Number.isFinite;
const objectCreate = Object.create;
const objectDefineProperties = Object.defineProperties;
const objectDefineProperty = Object.defineProperty;
const objectFreeze = Object.freeze;
const objectGetOwnPropertyDescriptor = Object.getOwnPropertyDescriptor;
const objectGetPrototypeOf = Object.getPrototypeOf;
const objectHasOwn = Object.hasOwn;
const objectIsFrozen = Object.isFrozen;
const objectPrototype = Object.prototype;
const NumberConstructor = Number;
const PromiseConstructor = Promise;
const promisePrototype = Promise.prototype;
const promiseSpeciesSymbol = Symbol.species;
const promisePrototypeConstructorDescriptor = objectGetOwnPropertyDescriptor(
  promisePrototype,
  "constructor",
);
const promiseResolveIntrinsic = Promise.resolve;
const promiseSpeciesDescriptor = objectGetOwnPropertyDescriptor(
  PromiseConstructor,
  promiseSpeciesSymbol,
);
const promiseThenIntrinsic = Promise.prototype.then;
const performanceNowIntrinsic = objectGetOwnPropertyDescriptor(
  objectGetPrototypeOf(nodePerformance),
  "now",
).value;
const reflectApply = Reflect.apply;
const reflectOwnKeys = Reflect.ownKeys;
const setAddIntrinsic = Set.prototype.add;
const setDeleteIntrinsic = Set.prototype.delete;
const setForEachIntrinsic = Set.prototype.forEach;
const setSizeGetter = objectGetOwnPropertyDescriptor(Set.prototype, "size").get;
const SetConstructor = Set;
const setTimeoutIntrinsic = globalThis.setTimeout;
const TypeErrorConstructor = TypeError;
const weakSetAddIntrinsic = WeakSet.prototype.add;
const weakSetHasIntrinsic = WeakSet.prototype.has;
const WeakSetConstructor = WeakSet;
const {
  isGeneratorObject: isGeneratorObjectValue,
  isPromise: isPromiseValue,
  isProxy: isProxyValue,
} = utilTypes;

export const PHYSICAL_COLLABORATOR_SETTLEMENT_CONTRACT_VERSION = 1;

const OPTION_KEYS = objectFreeze([
  "deadlineMilliseconds",
  "onFatal",
  "settlementGraceMilliseconds",
]);
const INVOKE_KEYS = objectFreeze(["start"]);
const SETTLEMENT_KEYS = objectFreeze([
  "contractVersion",
  "invoke",
  "stop",
]);
const STOPPED_RECEIPT = exactFrozenRecord({ status: "stopped" });
const TIMER_SCHEDULING = objectFreeze(objectCreate(null));
const MAX_TIMER_MILLISECONDS = 86_400_000;

const ERROR_MESSAGES = objectFreeze({
  invalid_physical_collaborator_settlement_options:
    "Physical collaborator settlement options are invalid",
  invalid_physical_collaborator_settlement_request:
    "Physical collaborator settlement request is invalid",
  physical_collaborator_late_settlement:
    "Physical collaborator settled after its result deadline",
  physical_collaborator_no_settlement:
    "Physical collaborator did not settle within its grace period",
  physical_collaborator_rejected:
    "Physical collaborator rejected before its result deadline",
  physical_collaborator_settlement_outcome_uncertain:
    "Physical collaborator settlement outcome is uncertain",
});

const protectedPromiseBrands = new WeakSetConstructor();
const settlementBrands = new WeakSetConstructor();
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

function setAdd(value, entry) {
  callIntrinsic(setAddIntrinsic, value, [entry]);
}

function setDelete(value, entry) {
  return callIntrinsic(setDeleteIntrinsic, value, [entry]);
}

function setForEach(value, callback) {
  callIntrinsic(setForEachIntrinsic, value, [callback]);
}

function setSize(value) {
  return callIntrinsic(setSizeGetter, value, []);
}

function weakSetAdd(value, entry) {
  callIntrinsic(weakSetAddIntrinsic, value, [entry]);
}

function weakSetHas(value, entry) {
  return callIntrinsic(weakSetHasIntrinsic, value, [entry]);
}

export class PhysicalCollaboratorSettlementError extends TypeErrorConstructor {
  constructor(code) {
    if (typeof code !== "string" || !objectHasOwn(ERROR_MESSAGES, code)) {
      throw new TypeErrorConstructor(
        "Unsupported physical collaborator settlement error",
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
        value: "PhysicalCollaboratorSettlementError",
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
        value: `PhysicalCollaboratorSettlementError: ${message}`,
        writable: false,
      },
    });
    objectFreeze(this);
  }
}

function makeError(code) {
  return new PhysicalCollaboratorSettlementError(code);
}

function fail(code) {
  throw makeError(code);
}

function ensure(condition, code) {
  if (!condition) fail(code);
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
      !isProxyValue(value),
    code,
  );
  return value;
}

function normalizeMilliseconds(value, code) {
  ensure(
    callIntrinsic(numberIsSafeIntegerIntrinsic, NumberConstructor, [value]) &&
      value > 0 &&
      value <= MAX_TIMER_MILLISECONDS,
    code,
  );
  return value;
}

function monotonicNow() {
  let value;
  try {
    value = callIntrinsic(performanceNowIntrinsic, nodePerformance, []);
  } catch {
    fail("physical_collaborator_settlement_outcome_uncertain");
  }
  ensure(
    callIntrinsic(numberIsFiniteIntrinsic, NumberConstructor, [value]) &&
      value >= 0,
    "physical_collaborator_settlement_outcome_uncertain",
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

function exactDescriptor(left, right) {
  return (
    left !== undefined &&
    right !== undefined &&
    left.configurable === right.configurable &&
    left.enumerable === right.enumerable &&
    left.get === right.get &&
    left.set === right.set &&
    left.value === right.value &&
    left.writable === right.writable
  );
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
      ownCatch === undefined &&
      ownConstructor === undefined &&
      ownFinally === undefined &&
      ownThen === undefined,
    code,
  );
  return value;
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
      "physical_collaborator_settlement_outcome_uncertain",
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
    fail("physical_collaborator_settlement_outcome_uncertain");
  }
  return value;
}

function deferredPromise() {
  let rejectPromise;
  let resolvePromise;
  const promise = protectPromise(
    new PromiseConstructor((resolve, reject) => {
      rejectPromise = reject;
      resolvePromise = resolve;
    }),
  );
  return objectFreeze({ promise, reject: rejectPromise, resolve: resolvePromise });
}

function makeInvocation() {
  return objectFreeze(objectCreate(null));
}

function createPhysicalCollaboratorSettlementInternal(
  args,
  providerSettlementRequired,
) {
  const optionCode = "invalid_physical_collaborator_settlement_options";
  const requestCode = "invalid_physical_collaborator_settlement_request";
  ensure(args.length === 1, optionCode);
  const options = exactDataObject(args[0], OPTION_KEYS, optionCode);
  const deadlineMilliseconds = normalizeMilliseconds(
    options.deadlineMilliseconds,
    optionCode,
  );
  const settlementGraceMilliseconds = normalizeMilliseconds(
    options.settlementGraceMilliseconds,
    optionCode,
  );
  const onFatal = trustedFunction(options.onFatal, optionCode);
  const active = new SetConstructor();
  let lifecycle = "running";
  let fatalCallbacksInFlight = 0;
  let terminalFailure = false;
  let terminalFailureCode = null;
  let stopPromise = null;
  let stopReject = null;
  let stopResolve = null;

  function clearRecordTimer(record, property) {
    const timer = record[property];
    if (timer === null) return true;
    try {
      callIntrinsic(clearTimeoutIntrinsic, undefined, [timer]);
      record[property] = null;
      return true;
    } catch {
      return false;
    }
  }

  function clearRecordTimers(record) {
    const deadlineCleared = clearRecordTimer(record, "deadlineTimer");
    const graceCleared = clearRecordTimer(record, "graceTimer");
    return deadlineCleared && graceCleared;
  }

  function scheduleRecordTimer(record, property, delay, callback) {
    let handle = null;
    let synchronousCallback = false;
    const guardedCallback = objectFreeze(function guardedCallback() {
      if (record[property] === TIMER_SCHEDULING) {
        synchronousCallback = true;
        return;
      }
      if (record[property] !== handle) return;
      record[property] = null;
      callIntrinsic(callback, undefined, []);
    });
    record[property] = TIMER_SCHEDULING;
    try {
      handle = callIntrinsic(setTimeoutIntrinsic, undefined, [
        guardedCallback,
        delay,
      ]);
    } catch {
      record[property] = null;
      return false;
    }
    record[property] = handle;
    return !synchronousCallback;
  }

  function maybeCompleteStop() {
    if (
      stopPromise === null ||
      setSize(active) !== 0 ||
      fatalCallbacksInFlight !== 0
    ) {
      return;
    }
    if (terminalFailure) {
      lifecycle = "failed";
      callIntrinsic(stopReject, undefined, [
        makeError(
          terminalFailureCode ??
            "physical_collaborator_settlement_outcome_uncertain",
        ),
      ]);
      return;
    }
    lifecycle = "stopped";
    callIntrinsic(stopResolve, undefined, [STOPPED_RECEIPT]);
  }

  function finishRecord(record, code, value) {
    if (record.phase === "done" || record.phase === "breached") return;
    const timersCleared = clearRecordTimers(record);
    if (!timersCleared) {
      record.trigger = "contract";
      breachRecord(
        record,
        "physical_collaborator_settlement_outcome_uncertain",
      );
      return;
    }
    record.phase = "done";
    setDelete(active, record);
    if (code === null) {
      callIntrinsic(record.resolve, undefined, [value]);
    } else {
      callIntrinsic(record.reject, undefined, [makeError(code)]);
    }
    maybeCompleteStop();
  }

  function rejectContractViolation(record) {
    record.trigger = "contract";
    beginGrace(record, "contract");
    if (record.phase === "draining") {
      breachRecord(record, "physical_collaborator_no_settlement");
    }
  }

  function invokeFatal(record) {
    let succeeded = false;
    try {
      const result = callIntrinsic(onFatal, undefined, [
        exactFrozenRecord({
          contractVersion: PHYSICAL_COLLABORATOR_SETTLEMENT_CONTRACT_VERSION,
          invocation: record.invocation,
          outcome: "no-settlement",
          trigger:
            record.trigger === "contract"
              ? "contract-violation"
              : record.trigger,
        }),
      ]);
      succeeded = result === undefined;
      if (!succeeded) observeInvalidNativePromise(result);
    } catch {
      succeeded = false;
    }
    return succeeded;
  }

  function observeInvalidNativePromise(value) {
    try {
      if (!isPromiseValue(value) || isProxyValue(value)) return;
      const prototype = objectGetPrototypeOf(value);
      const ownConstructor = objectGetOwnPropertyDescriptor(
        value,
        "constructor",
      );
      let safeConstructor = false;
      if (ownConstructor === undefined) {
        try {
          objectDefineProperty(value, "constructor", {
            configurable: false,
            enumerable: false,
            value: promiseSpeciesHolder,
            writable: false,
          });
          safeConstructor = true;
        } catch {
          safeConstructor =
            prototype === promisePrototype &&
            exactDescriptor(
              objectGetOwnPropertyDescriptor(
                promisePrototype,
                "constructor",
              ),
              promisePrototypeConstructorDescriptor,
            ) &&
            exactDescriptor(
              objectGetOwnPropertyDescriptor(
                PromiseConstructor,
                promiseSpeciesSymbol,
              ),
              promiseSpeciesDescriptor,
            );
        }
      } else {
        if (ownConstructor.configurable === true) {
          try {
            objectDefineProperty(value, "constructor", {
              configurable: false,
              enumerable: false,
              value: promiseSpeciesHolder,
              writable: false,
            });
            safeConstructor = true;
          } catch {
            safeConstructor = false;
          }
        } else {
          safeConstructor =
            exactProtectedPromiseDescriptor(
              ownConstructor,
              promiseSpeciesHolder,
            ) ||
            (objectHasOwn(ownConstructor, "value") &&
              ownConstructor.value === PromiseConstructor &&
              exactDescriptor(
                objectGetOwnPropertyDescriptor(
                  PromiseConstructor,
                  promiseSpeciesSymbol,
                ),
                promiseSpeciesDescriptor,
              ));
        }
      }
      if (!safeConstructor) return;
      const ignoreInvalidPromise = objectFreeze(
        function ignoreInvalidPromise() {},
      );
      void callIntrinsic(promiseThenIntrinsic, value, [
        ignoreInvalidPromise,
        ignoreInvalidPromise,
      ]);
    } catch {
      // Best effort only: do not invoke a thenable, proxy, accessor, or an
      // untrusted species constructor merely to observe a contract violation.
    }
  }

  function breachRecord(record, code) {
    if (providerSettlementRequired && record.providerPending) {
      quiesceRecord(record, code);
      return;
    }
    if (record.phase === "done" || record.phase === "breached") return;
    const timersCleared = clearRecordTimers(record);
    if (!timersCleared) void clearRecordTimers(record);
    record.phase = "breached";
    terminalFailure = true;
    if (lifecycle === "running") lifecycle = "failed";
    setDelete(active, record);
    let abortSucceeded = true;
    if (!record.aborted) {
      try {
        callIntrinsic(abortControllerAbortIntrinsic, record.controller, []);
        record.aborted = true;
      } catch {
        abortSucceeded = false;
      }
    }
    const preFatalStopFailureCode =
      timersCleared && abortSucceeded
        ? code
        : "physical_collaborator_settlement_outcome_uncertain";
    if (
      terminalFailureCode === null ||
      preFatalStopFailureCode ===
        "physical_collaborator_settlement_outcome_uncertain"
    ) {
      terminalFailureCode = preFatalStopFailureCode;
    }
    fatalCallbacksInFlight += 1;
    const fatalSucceeded = invokeFatal(record);
    fatalCallbacksInFlight -= 1;
    if (!fatalSucceeded) {
      terminalFailureCode =
        "physical_collaborator_settlement_outcome_uncertain";
    }
    callIntrinsic(record.reject, undefined, [
      makeError(code),
    ]);
    maybeCompleteStop();
  }

  function quiesceRecord(record, code) {
    if (
      record.phase === "done" ||
      record.phase === "breached" ||
      record.phase === "quiescing"
    ) {
      return;
    }
    const timersCleared = clearRecordTimers(record);
    if (!timersCleared) void clearRecordTimers(record);
    record.phase = "quiescing";
    terminalFailure = true;
    if (lifecycle === "running") lifecycle = "failed";
    let abortSucceeded = true;
    if (!record.aborted) {
      try {
        callIntrinsic(abortControllerAbortIntrinsic, record.controller, []);
        record.aborted = true;
      } catch {
        abortSucceeded = false;
      }
    }
    const failureCode =
      timersCleared && abortSucceeded
        ? code
        : "physical_collaborator_settlement_outcome_uncertain";
    record.quiescenceCode = failureCode;
    if (
      terminalFailureCode === null ||
      failureCode === "physical_collaborator_settlement_outcome_uncertain"
    ) {
      terminalFailureCode = failureCode;
    }
    fatalCallbacksInFlight += 1;
    const fatalSucceeded = invokeFatal(record);
    fatalCallbacksInFlight -= 1;
    if (!fatalSucceeded) {
      record.quiescenceCode =
        "physical_collaborator_settlement_outcome_uncertain";
      terminalFailureCode =
        "physical_collaborator_settlement_outcome_uncertain";
    }
  }

  function recordNow(record) {
    try {
      return monotonicNow();
    } catch {
      record.trigger = "contract";
      breachRecord(
        record,
        "physical_collaborator_settlement_outcome_uncertain",
      );
      return null;
    }
  }

  function armGraceTimer(record) {
    if (record.phase !== "draining") return;
    const now = recordNow(record);
    if (now === null) return;
    const remaining = record.graceExpiresAt - now;
    if (remaining <= 0) {
      breachRecord(record, "physical_collaborator_no_settlement");
      return;
    }
    const graceExpired = objectFreeze(function graceExpired() {
      armGraceTimer(record);
    });
    if (
      !scheduleRecordTimer(
        record,
        "graceTimer",
        remaining,
        graceExpired,
      )
    ) {
      breachRecord(
        record,
        "physical_collaborator_settlement_outcome_uncertain",
      );
    }
  }

  function enforceGraceExpiry(record) {
    if (record.phase !== "draining") return;
    const now = recordNow(record);
    if (now !== null && now >= record.graceExpiresAt) {
      breachRecord(record, "physical_collaborator_no_settlement");
    }
  }

  function beginGrace(record, trigger, explicitGraceExpiresAt = null) {
    if (record.phase !== "accepting") return;
    record.phase = "draining";
    record.trigger = trigger;
    if (!clearRecordTimer(record, "deadlineTimer")) {
      breachRecord(
        record,
        "physical_collaborator_settlement_outcome_uncertain",
      );
      return;
    }
    if (explicitGraceExpiresAt === null) {
      if (trigger === "deadline") {
        record.graceExpiresAt =
          record.deadlineAt + settlementGraceMilliseconds;
      } else {
        const now = recordNow(record);
        if (now === null) return;
        record.graceExpiresAt = now + settlementGraceMilliseconds;
      }
    } else {
      record.graceExpiresAt = explicitGraceExpiresAt;
    }
    armGraceTimer(record);
    if (record.phase !== "draining") {
      return;
    }
    try {
      callIntrinsic(abortControllerAbortIntrinsic, record.controller, []);
      record.aborted = true;
    } catch {
      breachRecord(
        record,
        "physical_collaborator_settlement_outcome_uncertain",
      );
    }
    enforceGraceExpiry(record);
  }

  function armDeadlineTimer(record) {
    if (record.phase !== "accepting") return;
    const now = recordNow(record);
    if (now === null) return;
    const remaining = record.deadlineAt - now;
    if (remaining <= 0) {
      beginGrace(record, "deadline");
      return;
    }
    const deadlineExpired = objectFreeze(function deadlineExpired() {
      armDeadlineTimer(record);
    });
    if (
      !scheduleRecordTimer(
        record,
        "deadlineTimer",
        remaining,
        deadlineExpired,
      )
    ) {
      record.trigger = "contract";
      breachRecord(
        record,
        "physical_collaborator_settlement_outcome_uncertain",
      );
    }
  }

  function enforceElapsedBoundaries(record) {
    if (record.phase === "accepting") {
      const now = recordNow(record);
      if (now !== null && now >= record.deadlineAt) {
        beginGrace(record, "deadline");
      }
    }
    enforceGraceExpiry(record);
  }

  function observeProvider(record, pending) {
    const onFulfilled = objectFreeze(function onFulfilled(value) {
      if (record.phase === "done" || record.phase === "breached") return;
      enforceElapsedBoundaries(record);
      if (record.phase === "done" || record.phase === "breached") return;
      record.providerPending = false;
      if (record.phase === "quiescing") {
        finishRecord(
          record,
          record.quiescenceCode ?? "physical_collaborator_no_settlement",
          null,
        );
        return;
      }
      if (record.phase === "draining") {
        finishRecord(record, "physical_collaborator_late_settlement", null);
        return;
      }
      const result = exactFrozenRecord({
        contractVersion: PHYSICAL_COLLABORATOR_SETTLEMENT_CONTRACT_VERSION,
        invocation: record.invocation,
        outcome: "success",
        value,
      });
      finishRecord(record, null, result);
    });
    const onRejected = objectFreeze(function onRejected() {
      if (record.phase === "done" || record.phase === "breached") return;
      enforceElapsedBoundaries(record);
      if (record.phase === "done" || record.phase === "breached") return;
      record.providerPending = false;
      if (record.phase === "quiescing") {
        finishRecord(
          record,
          record.quiescenceCode ?? "physical_collaborator_no_settlement",
          null,
        );
        return;
      }
      finishRecord(
        record,
        record.phase === "draining"
          ? "physical_collaborator_late_settlement"
          : "physical_collaborator_rejected",
        null,
      );
    });
    try {
      void callIntrinsic(protectedPromiseThen, pending, [
        onFulfilled,
        onRejected,
      ]);
    } catch {
      record.trigger = "contract";
      breachRecord(
        record,
        "physical_collaborator_settlement_outcome_uncertain",
      );
    }
  }

  const invoke = objectFreeze(function invoke(...methodArgs) {
    ensure(methodArgs.length === 1, requestCode);
    ensure(lifecycle === "running", requestCode);
    const input = exactDataObject(methodArgs[0], INVOKE_KEYS, requestCode);
    const start = trustedFunction(input.start, requestCode);
    const invocation = makeInvocation();
    const controller = new AbortControllerConstructor();
    let signal;
    try {
      signal = callIntrinsic(abortControllerSignalGetter, controller, []);
    } catch {
      fail("physical_collaborator_settlement_outcome_uncertain");
    }
    const deferred = deferredPromise();
    const record = objectCreate(null);
    record.controller = controller;
    record.aborted = false;
    record.deadlineAt = null;
    record.deadlineTimer = null;
    record.graceExpiresAt = null;
    record.graceTimer = null;
    record.invocation = invocation;
    record.phase = "accepting";
    record.promise = deferred.promise;
    record.providerPending = false;
    record.quiescenceCode = null;
    record.reject = deferred.reject;
    record.resolve = deferred.resolve;
    record.trigger = "deadline";
    setAdd(active, record);

    try {
      record.deadlineAt = monotonicNow() + deadlineMilliseconds;
    } catch {
      record.trigger = "contract";
      breachRecord(
        record,
        "physical_collaborator_settlement_outcome_uncertain",
      );
      return record.promise;
    }
    armDeadlineTimer(record);
    if (record.phase !== "accepting") return record.promise;

    let pending;
    try {
      pending = callIntrinsic(start, undefined, [
        exactFrozenRecord({ invocation, signal }),
      ]);
    } catch {
      rejectContractViolation(record);
      return record.promise;
    }
    try {
      pending = protectPromise(
        exactNativePromise(
          pending,
          "physical_collaborator_settlement_outcome_uncertain",
        ),
      );
    } catch {
      observeInvalidNativePromise(pending);
      rejectContractViolation(record);
      return record.promise;
    }
    record.providerPending = true;
    observeProvider(record, pending);
    enforceElapsedBoundaries(record);
    return record.promise;
  });

  const stop = objectFreeze(function stop(...methodArgs) {
    ensure(methodArgs.length === 0, requestCode);
    if (stopPromise !== null) return stopPromise;
    const deferred = deferredPromise();
    stopPromise = deferred.promise;
    stopReject = deferred.reject;
    stopResolve = deferred.resolve;
    if (lifecycle !== "failed") lifecycle = "stopping";
    let stopGraceExpiresAt = null;
    try {
      stopGraceExpiresAt = monotonicNow() + settlementGraceMilliseconds;
    } catch {
      terminalFailure = true;
      terminalFailureCode =
        "physical_collaborator_settlement_outcome_uncertain";
    }
    const abortActive = objectFreeze(function abortActive(record) {
      enforceElapsedBoundaries(record);
      if (record.phase !== "accepting") return;
      if (stopGraceExpiresAt === null) {
        record.trigger = "contract";
        breachRecord(
          record,
          "physical_collaborator_settlement_outcome_uncertain",
        );
        return;
      }
      beginGrace(record, "stop", stopGraceExpiresAt);
    });
    setForEach(active, abortActive);
    maybeCompleteStop();
    return stopPromise;
  });

  const settlement = exactFrozenRecord({
    contractVersion: PHYSICAL_COLLABORATOR_SETTLEMENT_CONTRACT_VERSION,
    invoke,
    stop,
  });
  ensure(reflectOwnKeys(settlement).length === SETTLEMENT_KEYS.length, optionCode);
  weakSetAdd(settlementBrands, settlement);
  return settlement;
}

export function createPhysicalCollaboratorSettlement(...args) {
  return createPhysicalCollaboratorSettlementInternal(args, false);
}

export function createQuiescentPhysicalCollaboratorSettlement(...args) {
  return createPhysicalCollaboratorSettlementInternal(args, true);
}

export function isPhysicalCollaboratorSettlement(value) {
  try {
    return (
      arguments.length === 1 &&
      value !== null &&
      typeof value === "object" &&
      !isProxyValue(value) &&
      weakSetHas(settlementBrands, value)
    );
  } catch {
    return false;
  }
}

objectFreeze(PhysicalCollaboratorSettlementError.prototype);
objectFreeze(PhysicalCollaboratorSettlementError);
objectFreeze(createPhysicalCollaboratorSettlement);
objectFreeze(createQuiescentPhysicalCollaboratorSettlement);
objectFreeze(isPhysicalCollaboratorSettlement);
