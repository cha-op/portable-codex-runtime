import { types as utilTypes } from "node:util";

import {
  PHYSICAL_COLLABORATOR_SETTLEMENT_CONTRACT_VERSION,
  createPhysicalCollaboratorSettlement,
} from "./physical-collaborator-settlement.mjs";
import {
  assertRestoreAttachmentReconciliationBackend,
} from "./session-storage-contracts.mjs";
import {
  StoppedDirectoryPublication,
} from "./stopped-directory-publication.mjs";
import {
  STOPPED_WRITER_STOP_CONFIRMED,
} from "./stopped-writer-capability.mjs";

const arrayIncludesIntrinsic = Array.prototype.includes;
const arrayIsArray = Array.isArray;
const objectCreate = Object.create;
const objectDefineProperties = Object.defineProperties;
const objectDefineProperty = Object.defineProperty;
const objectFreeze = Object.freeze;
const objectGetOwnPropertyDescriptor = Object.getOwnPropertyDescriptor;
const objectGetPrototypeOf = Object.getPrototypeOf;
const objectHasOwn = Object.hasOwn;
const objectIsFrozen = Object.isFrozen;
const objectPrototype = Object.prototype;
const numberIsSafeIntegerIntrinsic = Number.isSafeInteger;
const NumberConstructor = Number;
const PromiseConstructor = Promise;
const promisePrototype = Promise.prototype;
const promiseResolveIntrinsic = Promise.resolve;
const promiseThenIntrinsic = Promise.prototype.then;
const promiseSpeciesSymbol = Symbol.species;
const reflectApply = Reflect.apply;
const reflectOwnKeys = Reflect.ownKeys;
const regexpExecIntrinsic = RegExp.prototype.exec;
const TypeErrorConstructor = TypeError;
const weakSetAddIntrinsic = WeakSet.prototype.add;
const weakSetHasIntrinsic = WeakSet.prototype.has;
const WeakSetConstructor = WeakSet;
const { isGeneratorFunction, isPromise, isProxy } = utilTypes;

export const POSTGRES_DETACHED_RESTORE_PHYSICAL_BINDINGS_CONTRACT_VERSION = 1;
export const POSTGRES_LOGICAL_WRITER_SUPERVISOR_PHYSICAL_CONTRACT_VERSION = 2;
export const POSTGRES_SESSION_STORAGE_PHYSICAL_INVOCATION_CONTRACT_VERSION = 1;
export const POSTGRES_RESTORE_DESTINATION_RESOLVER_PHYSICAL_CONTRACT_VERSION = 1;

const OPTION_KEYS = objectFreeze([
  "lifecycleBackend",
  "lifecycleSettlement",
  "onFatal",
  "publication",
  "publicationSettlement",
  "resolveRestoreDestination",
  "resolveRestoreDestinationContractVersion",
  "resolveRestoreDestinationSettlement",
  "supervisor",
  "supervisorSettlement",
]);
const POLICY_KEYS = objectFreeze([
  "deadlineMilliseconds",
  "settlementGraceMilliseconds",
]);
const CAPABILITY_KEYS = objectFreeze([
  "atomicPointInTimeCheckpoint",
  "exclusiveWriterAttachment",
  "fencing",
  "normalDirectoryAttachment",
]);
const SUPERVISOR_KEYS = objectFreeze([
  "contractVersion",
  "launchWriter",
  "reconcileWriterLaunch",
  "supervisorId",
]);
const SUPERVISOR_METHODS = objectFreeze([
  "launchWriter",
  "reconcileWriterLaunch",
  "stopWriter",
]);
const LIFECYCLE_METHODS = objectFreeze([
  "captureCheckpoint",
  "destroySession",
  "detachAttachment",
  "forceFence",
  "prepareRestoreAttachment",
  "prepareWritableAttachment",
  "provisionSession",
  "reconcileRestoreAttachment",
  "restoreCheckpoint",
]);
const LIFECYCLE_PREFLIGHT_KEYS = objectFreeze([
  "backendId",
  "capabilities",
  "contractVersion",
  "physicalInvocationContractVersion",
  "restoreAttachmentActivationContractVersion",
  "restoreAttachmentReconciliationContractVersion",
  "captureCheckpoint",
  "destroySession",
  "detachAttachment",
  "forceFence",
  "prepareRestoreAttachment",
  "prepareWritableAttachment",
  "provisionSession",
  "reconcileRestoreAttachment",
  "restoreCheckpoint",
]);
const PUBLICATION_METHODS = objectFreeze([
  "publishFreshCheckpointArtifact",
  "publishRestoreDestination",
  "verifyCommittedCheckpointArtifact",
  "verifyCommittedRestoreDestination",
]);
const SETTLEMENT_RESULT_KEYS = objectFreeze([
  "contractVersion",
  "invocation",
  "outcome",
  "value",
]);
const LAUNCH_RECEIPT_KEYS = objectFreeze([
  "evidence",
  "receiptVersion",
  "stopWriter",
]);
const RECONCILE_RECEIPT_KEYS = objectFreeze([
  "evidence",
  "receiptVersion",
]);
const STOP_RECEIPT_KEYS = objectFreeze(["contractVersion", "status"]);
const STOP_FACADE_REQUEST_KEYS = objectFreeze([
  "attachment",
  "processIncarnationId",
  "stopOperationId",
  "writerFence",
  "writerIncarnationId",
]);
const EVIDENCE_KEYS = objectFreeze([
  "contractVersion",
  "launchAttemptId",
  "processIncarnationId",
  "proofId",
  "status",
  "supervisorId",
  "writerIncarnationId",
]);
const MAX_MILLISECONDS = 86_400_000;
const MAX_SHALLOW_KEYS = 256;
const OPAQUE_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u;

const ERROR_MESSAGES = objectFreeze({
  invalid_postgres_detached_restore_physical_bindings_options:
    "PostgreSQL detached restore physical bindings options are invalid",
  invalid_postgres_detached_restore_physical_bindings_request:
    "PostgreSQL detached restore physical bindings request is invalid",
  postgres_detached_restore_physical_bindings_outcome_uncertain:
    "PostgreSQL detached restore physical bindings outcome is uncertain",
});

const bindingBrands = new WeakSetConstructor();
const errorBrands = new WeakSetConstructor();
const publicationBrands = new WeakSetConstructor();
const returnedPromiseBrands = new WeakSetConstructor();
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
const publishFreshCheckpointArtifactIntrinsic =
  StoppedDirectoryPublication.prototype.publishFreshCheckpointArtifact;
const publishRestoreDestinationIntrinsic =
  StoppedDirectoryPublication.prototype.publishRestoreDestination;
const verifyCommittedCheckpointArtifactIntrinsic =
  StoppedDirectoryPublication.prototype.verifyCommittedCheckpointArtifact;
const verifyCommittedRestoreDestinationIntrinsic =
  StoppedDirectoryPublication.prototype.verifyCommittedRestoreDestination;
const publicationIntrinsics = objectFreeze({
  publishFreshCheckpointArtifact: publishFreshCheckpointArtifactIntrinsic,
  publishRestoreDestination: publishRestoreDestinationIntrinsic,
  verifyCommittedCheckpointArtifact: verifyCommittedCheckpointArtifactIntrinsic,
  verifyCommittedRestoreDestination: verifyCommittedRestoreDestinationIntrinsic,
});

function callIntrinsic(intrinsic, receiver, args) {
  return reflectApply(intrinsic, receiver, args);
}

function weakSetAdd(set, value) {
  callIntrinsic(weakSetAddIntrinsic, set, [value]);
}

function weakSetHas(set, value) {
  return callIntrinsic(weakSetHasIntrinsic, set, [value]);
}

function includes(values, value) {
  return callIntrinsic(arrayIncludesIntrinsic, values, [value]);
}

function regexpTest(pattern, value) {
  return callIntrinsic(regexpExecIntrinsic, pattern, [value]) !== null;
}

export class PostgresDetachedRestorePhysicalBindingsError extends TypeErrorConstructor {
  constructor(code) {
    if (typeof code !== "string" || !objectHasOwn(ERROR_MESSAGES, code)) {
      throw new TypeErrorConstructor(
        "Unsupported PostgreSQL detached restore physical bindings error",
      );
    }
    const message = ERROR_MESSAGES[code];
    super(message);
    objectDefineProperties(this, {
      code: { enumerable: true, value: code },
      name: { value: "PostgresDetachedRestorePhysicalBindingsError" },
      retryable: { enumerable: true, value: false },
      stack: {
        value: `PostgresDetachedRestorePhysicalBindingsError: ${message}`,
      },
    });
    objectFreeze(this);
  }
}

function makeError(code) {
  const error = new PostgresDetachedRestorePhysicalBindingsError(code);
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
    !isProxy(value) &&
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

function dataObject(value, expectedKeys, code, exact = true) {
  ensure(
    value !== null &&
      typeof value === "object" &&
      !isProxy(value) &&
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
    (prototype === objectPrototype || prototype === null) &&
      keys.length <= MAX_SHALLOW_KEYS &&
      (!exact || keys.length === expectedKeys.length),
    code,
  );
  const normalized = objectCreate(null);
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
    ensure(
      typeof key === "string" && (!exact || includes(expectedKeys, key)),
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
  return objectFreeze(normalized);
}

function shallowSnapshot(value, code) {
  return dataObject(value, objectFreeze([]), code, false);
}

function resultSnapshot(value, code) {
  const snapshot = shallowSnapshot(value, code);
  ensure(!objectHasOwn(snapshot, "then"), code);
  return snapshot;
}

function protectReturnedPromiseReaction(callback) {
  if (typeof callback !== "function") return callback;
  return (value) =>
    protectReturnedPromise(
      callIntrinsic(callback, undefined, [value]),
      "postgres_detached_restore_physical_bindings_outcome_uncertain",
    );
}

function protectedReturnedPromiseThen(onFulfilled, onRejected) {
  return protectReturnedPromise(
    callIntrinsic(promiseThenIntrinsic, this, [
      protectReturnedPromiseReaction(onFulfilled),
      protectReturnedPromiseReaction(onRejected),
    ]),
    "postgres_detached_restore_physical_bindings_outcome_uncertain",
  );
}

function protectedReturnedPromiseCatch(onRejected) {
  return callIntrinsic(protectedReturnedPromiseThen, this, [
    undefined,
    onRejected,
  ]);
}

function resolveProtectedReturnedPromise(value, code) {
  return protectReturnedPromise(
    callIntrinsic(promiseResolveIntrinsic, PromiseConstructor, [
      protectReturnedPromise(value, code),
    ]),
    code,
  );
}

function protectedReturnedPromiseFinally(onFinally) {
  if (typeof onFinally !== "function") {
    return callIntrinsic(protectedReturnedPromiseThen, this, [
      onFinally,
      onFinally,
    ]);
  }
  const runFinally = () =>
    resolveProtectedReturnedPromise(
      callIntrinsic(onFinally, undefined, []),
      "postgres_detached_restore_physical_bindings_outcome_uncertain",
    );
  return callIntrinsic(protectedReturnedPromiseThen, this, [
    (value) =>
      callIntrinsic(protectedReturnedPromiseThen, runFinally(), [() => value]),
    (reason) =>
      callIntrinsic(protectedReturnedPromiseThen, runFinally(), [
        () => {
          throw reason;
        },
      ]),
  ]);
}

objectFreeze(protectedReturnedPromiseThen);
objectFreeze(protectedReturnedPromiseCatch);
objectFreeze(protectedReturnedPromiseFinally);

function frozenReturnedPromiseDataDescriptor(descriptor, value) {
  return (
    descriptor !== undefined &&
    descriptor.configurable === false &&
    descriptor.enumerable === false &&
    descriptor.writable === false &&
    descriptor.value === value
  );
}

function safeReturnedPromiseReactionDescriptor(descriptor) {
  return (
    descriptor !== undefined &&
    descriptor.configurable === false &&
    descriptor.enumerable === false &&
    descriptor.writable === false &&
    typeof descriptor.value === "function" &&
    !isProxy(descriptor.value) &&
    !isGeneratorFunction(descriptor.value)
  );
}

function safeReturnedPromiseSpeciesHolder(value) {
  if (
    value === null ||
    typeof value !== "object" ||
    isProxy(value) ||
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
    frozenReturnedPromiseDataDescriptor(descriptor, PromiseConstructor)
  );
}

function protectReturnedPromise(value, code) {
  if (!isPromise(value)) return value;
  ensure(value !== null && typeof value === "object" && !isProxy(value), code);
  let prototype;
  let catchDescriptor;
  let constructorDescriptor;
  let finallyDescriptor;
  let thenDescriptor;
  try {
    prototype = objectGetPrototypeOf(value);
    catchDescriptor = objectGetOwnPropertyDescriptor(value, "catch");
    constructorDescriptor = objectGetOwnPropertyDescriptor(value, "constructor");
    finallyDescriptor = objectGetOwnPropertyDescriptor(value, "finally");
    thenDescriptor = objectGetOwnPropertyDescriptor(value, "then");
  } catch {
    fail(code);
  }
  ensure(prototype === promisePrototype, code);
  if (weakSetHas(returnedPromiseBrands, value)) return value;

  const reactionsAreOurs =
    frozenReturnedPromiseDataDescriptor(
      catchDescriptor,
      protectedReturnedPromiseCatch,
    ) &&
    frozenReturnedPromiseDataDescriptor(
      finallyDescriptor,
      protectedReturnedPromiseFinally,
    ) &&
    frozenReturnedPromiseDataDescriptor(
      thenDescriptor,
      protectedReturnedPromiseThen,
    );
  const hasNoOwnReactions =
    catchDescriptor === undefined &&
    finallyDescriptor === undefined &&
    thenDescriptor === undefined;
  if (!hasNoOwnReactions) {
    ensure(
      reactionsAreOurs ||
        (safeReturnedPromiseReactionDescriptor(catchDescriptor) &&
          safeReturnedPromiseReactionDescriptor(finallyDescriptor) &&
          safeReturnedPromiseReactionDescriptor(thenDescriptor)),
      code,
    );
    if (constructorDescriptor === undefined) {
      try {
        objectDefineProperty(value, "constructor", {
          configurable: false,
          enumerable: false,
          value: promiseSpeciesHolder,
          writable: false,
        });
        constructorDescriptor = objectGetOwnPropertyDescriptor(
          value,
          "constructor",
        );
      } catch {
        fail(code);
      }
    }
    ensure(
      frozenReturnedPromiseDataDescriptor(
        constructorDescriptor,
        constructorDescriptor.value,
      ) && safeReturnedPromiseSpeciesHolder(constructorDescriptor.value),
      code,
    );
    let child;
    try {
      child = callIntrinsic(promiseThenIntrinsic, value, [undefined, undefined]);
    } catch {
      fail(code);
    }
    return protectReturnedPromise(child, code);
  }

  ensure(
    constructorDescriptor === undefined ||
      (frozenReturnedPromiseDataDescriptor(
        constructorDescriptor,
        constructorDescriptor.value,
      ) && safeReturnedPromiseSpeciesHolder(constructorDescriptor.value)),
    code,
  );
  try {
    const descriptors = {
      catch: {
        configurable: false,
        enumerable: false,
        value: protectedReturnedPromiseCatch,
        writable: false,
      },
      finally: {
        configurable: false,
        enumerable: false,
        value: protectedReturnedPromiseFinally,
        writable: false,
      },
      then: {
        configurable: false,
        enumerable: false,
        value: protectedReturnedPromiseThen,
        writable: false,
      },
    };
    if (constructorDescriptor === undefined) {
      descriptors.constructor = {
        configurable: false,
        enumerable: false,
        value: promiseSpeciesHolder,
        writable: false,
      };
    }
    objectDefineProperties(value, descriptors);
    weakSetAdd(returnedPromiseBrands, value);
  } catch {
    fail(code);
  }
  return value;
}

function observePromise(value, code) {
  try {
    return protectReturnedPromise(
      callIntrinsic(promiseThenIntrinsic, value, [
        (settledValue) => exactFrozenRecord({ failed: false, value: settledValue }),
        () => exactFrozenRecord({ failed: true, value: null }),
      ]),
      code,
    );
  } catch {
    fail(code);
  }
}

function trustedFunction(value, code) {
  ensure(
    typeof value === "function" && !isProxy(value) && !isGeneratorFunction(value),
    code,
  );
  return value;
}

function ownDataValue(value, key, code) {
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

function dataValue(value, key, code) {
  let current = value;
  for (let depth = 0; depth < 64 && current !== null; depth += 1) {
    ensure(!isProxy(current), code);
    let descriptor;
    try {
      descriptor = objectGetOwnPropertyDescriptor(current, key);
    } catch {
      fail(code);
    }
    if (descriptor !== undefined) {
      ensure(objectHasOwn(descriptor, "value"), code);
      return descriptor.value;
    }
    try {
      current = objectGetPrototypeOf(current);
    } catch {
      fail(code);
    }
  }
  fail(code);
}

function ownExactFunction(value, key, code) {
  return trustedFunction(ownDataValue(value, key, code), code);
}

function preflightLifecycleBackend(value, code) {
  ensure(value !== null && typeof value === "object" && !isProxy(value), code);
  for (let index = 0; index < LIFECYCLE_PREFLIGHT_KEYS.length; index += 1) {
    dataValue(value, LIFECYCLE_PREFLIGHT_KEYS[index], code);
  }
  const capabilities = dataValue(value, "capabilities", code);
  dataObject(capabilities, CAPABILITY_KEYS, code);
}

function normalizePolicy(value, code) {
  const policy = dataObject(value, POLICY_KEYS, code);
  ensure(
    callIntrinsic(numberIsSafeIntegerIntrinsic, NumberConstructor, [
      policy.deadlineMilliseconds,
    ]) &&
      policy.deadlineMilliseconds >= 1 &&
      policy.deadlineMilliseconds <= MAX_MILLISECONDS &&
      callIntrinsic(numberIsSafeIntegerIntrinsic, NumberConstructor, [
        policy.settlementGraceMilliseconds,
      ]) &&
      policy.settlementGraceMilliseconds >= 1 &&
      policy.settlementGraceMilliseconds <= MAX_MILLISECONDS,
    code,
  );
  return exactFrozenRecord({
    deadlineMilliseconds: policy.deadlineMilliseconds,
    settlementGraceMilliseconds: policy.settlementGraceMilliseconds,
  });
}

function normalizePolicies(value, methods, code) {
  const policies = dataObject(value, methods, code);
  const result = objectCreate(null);
  for (let index = 0; index < methods.length; index += 1) {
    const method = methods[index];
    objectDefineProperty(result, method, {
      enumerable: true,
      value: normalizePolicy(policies[method], code),
    });
  }
  return objectFreeze(result);
}

function createSettlement(policy, onFatal, code) {
  try {
    return createPhysicalCollaboratorSettlement(
      exactFrozenRecord({ ...policy, onFatal }),
    );
  } catch {
    fail(code);
  }
}

function invokeSettlement(settlement, start, code) {
  let pending;
  try {
    pending = callIntrinsic(
      ownExactFunction(settlement, "invoke", code),
      undefined,
      [exactFrozenRecord({ start })],
    );
  } catch {
    fail(code);
  }
  return pending;
}

async function settledValue(
  settlement,
  callback,
  receiver,
  request,
  enrich,
  includeContext,
  code,
) {
  let expectedInvocation = null;
  const start = objectFreeze(function start(contextValue) {
    const context = dataObject(
      contextValue,
      objectFreeze(["invocation", "signal"]),
      code,
    );
    expectedInvocation = context.invocation;
    const sidecar = exactFrozenRecord({
        contractVersion:
          POSTGRES_SESSION_STORAGE_PHYSICAL_INVOCATION_CONTRACT_VERSION,
        invocation: context.invocation,
        signal: context.signal,
      });
    return callIntrinsic(
      callback,
      receiver,
      includeContext
        ? [enrich(request, context), sidecar]
        : [enrich(request, context)],
    );
  });
  try {
    const raw = await invokeSettlement(settlement, start, code);
    const receipt = dataObject(raw, SETTLEMENT_RESULT_KEYS, code);
    ensure(
      receipt.contractVersion ===
        PHYSICAL_COLLABORATOR_SETTLEMENT_CONTRACT_VERSION &&
        receipt.invocation === expectedInvocation &&
        receipt.outcome === "success",
      code,
    );
    return exactFrozenRecord({ value: receipt.value });
  } catch (error) {
    if (isInternalError(error)) throw error;
    fail(code);
  }
}

function identityRequest(request) {
  return request;
}

function physicalRequest(request, context, code) {
  const input = shallowSnapshot(request, code);
  return exactFrozenRecord({
    ...input,
    contractVersion: POSTGRES_LOGICAL_WRITER_SUPERVISOR_PHYSICAL_CONTRACT_VERSION,
    invocation: context.invocation,
    signal: context.signal,
  });
}

function physicalStopRequest(request, context, code) {
  const input = shallowSnapshot(request, code);
  return exactFrozenRecord({
    ...input,
    contractVersion: POSTGRES_LOGICAL_WRITER_SUPERVISOR_PHYSICAL_CONTRACT_VERSION,
    invocation: context.invocation,
    signal: context.signal,
  });
}

function facadeEvidence(value, statuses, code) {
  const evidence = dataObject(value, EVIDENCE_KEYS, code);
  ensure(
    evidence.contractVersion ===
      POSTGRES_LOGICAL_WRITER_SUPERVISOR_PHYSICAL_CONTRACT_VERSION &&
      typeof evidence.launchAttemptId === "string" &&
      typeof evidence.proofId === "string" &&
      typeof evidence.supervisorId === "string" &&
      regexpTest(OPAQUE_ID_PATTERN, evidence.launchAttemptId) &&
      regexpTest(OPAQUE_ID_PATTERN, evidence.proofId) &&
      regexpTest(OPAQUE_ID_PATTERN, evidence.supervisorId) &&
      includes(statuses, evidence.status) &&
      (evidence.status === "not-started"
        ? evidence.processIncarnationId === null &&
          evidence.writerIncarnationId === null
        : typeof evidence.processIncarnationId === "string" &&
          typeof evidence.writerIncarnationId === "string" &&
          regexpTest(OPAQUE_ID_PATTERN, evidence.processIncarnationId) &&
          regexpTest(OPAQUE_ID_PATTERN, evidence.writerIncarnationId)),
    code,
  );
  return exactFrozenRecord({ ...evidence, contractVersion: 1 });
}

function constructionCleanup(settlements) {
  for (let index = 0; index < settlements.length; index += 1) {
    try {
      const pending = callIntrinsic(
        ownExactFunction(
          settlements[index],
          "stop",
          "invalid_postgres_detached_restore_physical_bindings_options",
        ),
        undefined,
        [],
      );
      void callIntrinsic(promiseThenIntrinsic, pending, [
        () => {},
        () => {},
      ]);
    } catch {
      // Construction already failed; cleanup remains best effort.
    }
  }
}

export function createPostgresDetachedRestorePhysicalBindings(...args) {
  const optionCode =
    "invalid_postgres_detached_restore_physical_bindings_options";
  const requestCode =
    "invalid_postgres_detached_restore_physical_bindings_request";
  const outcomeCode =
    "postgres_detached_restore_physical_bindings_outcome_uncertain";
  ensure(args.length === 1, optionCode);
  const options = dataObject(args[0], OPTION_KEYS, optionCode);
  const onFatal = trustedFunction(options.onFatal, optionCode);
  const supervisorValue = dataObject(options.supervisor, SUPERVISOR_KEYS, optionCode);
  ensure(
    supervisorValue.contractVersion ===
      POSTGRES_LOGICAL_WRITER_SUPERVISOR_PHYSICAL_CONTRACT_VERSION &&
      typeof supervisorValue.supervisorId === "string" &&
      regexpTest(OPAQUE_ID_PATTERN, supervisorValue.supervisorId),
    optionCode,
  );
  const rawSupervisor = exactFrozenRecord({
    contractVersion: supervisorValue.contractVersion,
    launchWriter: trustedFunction(supervisorValue.launchWriter, optionCode),
    reconcileWriterLaunch: trustedFunction(
      supervisorValue.reconcileWriterLaunch,
      optionCode,
    ),
    supervisorId: supervisorValue.supervisorId,
  });
  let rawLifecycle;
  try {
    preflightLifecycleBackend(options.lifecycleBackend, optionCode);
    rawLifecycle = assertRestoreAttachmentReconciliationBackend(
      options.lifecycleBackend,
    );
  } catch {
    fail(optionCode);
  }
  ensure(
    ownDataValue(rawLifecycle, "physicalInvocationContractVersion", optionCode) ===
      POSTGRES_SESSION_STORAGE_PHYSICAL_INVOCATION_CONTRACT_VERSION,
    optionCode,
  );
  const lifecycleCapabilities = dataObject(
    dataValue(rawLifecycle, "capabilities", optionCode),
    CAPABILITY_KEYS,
    optionCode,
  );
  const rawPublication = options.publication;
  ensure(
    !isProxy(rawPublication) &&
      rawPublication instanceof StoppedDirectoryPublication,
    optionCode,
  );
  ensure(
    options.resolveRestoreDestinationContractVersion ===
      POSTGRES_RESTORE_DESTINATION_RESOLVER_PHYSICAL_CONTRACT_VERSION,
    optionCode,
  );
  const rawResolver = trustedFunction(
    options.resolveRestoreDestination,
    optionCode,
  );
  const supervisorPolicies = normalizePolicies(
    options.supervisorSettlement,
    SUPERVISOR_METHODS,
    optionCode,
  );
  const lifecyclePolicies = normalizePolicies(
    options.lifecycleSettlement,
    LIFECYCLE_METHODS,
    optionCode,
  );
  const publicationPolicies = normalizePolicies(
    options.publicationSettlement,
    PUBLICATION_METHODS,
    optionCode,
  );
  const resolverPolicy = normalizePolicy(
    options.resolveRestoreDestinationSettlement,
    optionCode,
  );
  const settlements = [];
  const settlementByName = objectCreate(null);
  try {
    for (let index = 0; index < SUPERVISOR_METHODS.length; index += 1) {
      const method = SUPERVISOR_METHODS[index];
      const settlement = createSettlement(
        supervisorPolicies[method],
        onFatal,
        optionCode,
      );
      settlements[settlements.length] = settlement;
      settlementByName[`supervisor:${method}`] = settlement;
    }
    for (let index = 0; index < LIFECYCLE_METHODS.length; index += 1) {
      const method = LIFECYCLE_METHODS[index];
      const settlement = createSettlement(
        lifecyclePolicies[method],
        onFatal,
        optionCode,
      );
      settlements[settlements.length] = settlement;
      settlementByName[`lifecycle:${method}`] = settlement;
    }
    for (let index = 0; index < PUBLICATION_METHODS.length; index += 1) {
      const method = PUBLICATION_METHODS[index];
      const settlement = createSettlement(
        publicationPolicies[method],
        onFatal,
        optionCode,
      );
      settlements[settlements.length] = settlement;
      settlementByName[`publication:${method}`] = settlement;
    }
    const settlement = createSettlement(resolverPolicy, onFatal, optionCode);
    settlements[settlements.length] = settlement;
    settlementByName.resolver = settlement;
  } catch (error) {
    constructionCleanup(settlements);
    if (isInternalError(error)) throw error;
    fail(optionCode);
  }
  ensure(settlements.length === 17, optionCode);

  function assembleBindings() {
    const stopWriter = objectFreeze(async function stopWriter(...methodArgs) {
      ensure(methodArgs.length === 1, requestCode);
      const stop = dataObject(
        methodArgs[0],
        objectFreeze(["callback", "request"]),
        requestCode,
      );
      const carrier = await settledValue(
        settlementByName["supervisor:stopWriter"],
        trustedFunction(stop.callback, requestCode),
        undefined,
        stop.request,
        (request, context) => physicalStopRequest(request, context, requestCode),
        false,
        outcomeCode,
      );
      const value = carrier.value;
      const receipt = dataObject(value, STOP_RECEIPT_KEYS, outcomeCode);
      ensure(
        receipt.contractVersion ===
          POSTGRES_LOGICAL_WRITER_SUPERVISOR_PHYSICAL_CONTRACT_VERSION &&
          receipt.status === "stopped",
        outcomeCode,
      );
      return STOPPED_WRITER_STOP_CONFIRMED;
    });

    const launchWriter = objectFreeze(async function launchWriter(...methodArgs) {
      ensure(methodArgs.length === 1, requestCode);
      const request = shallowSnapshot(methodArgs[0], requestCode);
      ensure(request.contractVersion === 1, requestCode);
      const carrier = await settledValue(
        settlementByName["supervisor:launchWriter"],
        rawSupervisor.launchWriter,
        undefined,
        request,
        (input, context) => physicalRequest(input, context, requestCode),
        false,
        outcomeCode,
      );
      const value = carrier.value;
      const receipt = dataObject(value, LAUNCH_RECEIPT_KEYS, outcomeCode);
      ensure(receipt.receiptVersion === 1, outcomeCode);
      const evidence = facadeEvidence(
        receipt.evidence,
        objectFreeze(["complete-stopped", "not-started", "started"]),
        outcomeCode,
      );
      let facadeStopWriter = null;
      if (evidence.status === "started") {
        const rawStop = trustedFunction(receipt.stopWriter, outcomeCode);
        facadeStopWriter = objectFreeze(
          async function facadeStopWriter(...stopArgs) {
            ensure(stopArgs.length === 1, requestCode);
            const request = dataObject(
              stopArgs[0],
              STOP_FACADE_REQUEST_KEYS,
              requestCode,
            );
            return stopWriter(exactFrozenRecord({ callback: rawStop, request }));
          },
        );
      } else {
        ensure(receipt.stopWriter === null, outcomeCode);
      }
      return exactFrozenRecord({
        evidence,
        receiptVersion: receipt.receiptVersion,
        stopWriter: facadeStopWriter,
      });
    });

    const reconcileWriterLaunch = objectFreeze(
      async function reconcileWriterLaunch(...methodArgs) {
        ensure(methodArgs.length === 1, requestCode);
        const request = shallowSnapshot(methodArgs[0], requestCode);
        ensure(request.contractVersion === 1, requestCode);
        const carrier = await settledValue(
          settlementByName["supervisor:reconcileWriterLaunch"],
          rawSupervisor.reconcileWriterLaunch,
          undefined,
          request,
          (input, context) => physicalRequest(input, context, requestCode),
          false,
          outcomeCode,
        );
        const value = carrier.value;
        const receipt = dataObject(value, RECONCILE_RECEIPT_KEYS, outcomeCode);
        ensure(receipt.receiptVersion === 1, outcomeCode);
        return exactFrozenRecord({
          evidence: facadeEvidence(
            receipt.evidence,
            objectFreeze(["complete-stopped", "not-started"]),
            outcomeCode,
          ),
          receiptVersion: receipt.receiptVersion,
        });
      },
    );
    const supervisor = exactFrozenRecord({
      contractVersion: 1,
      launchWriter,
      reconcileWriterLaunch,
      supervisorId: rawSupervisor.supervisorId,
    });

    const lifecycleRecord = {
      backendId: dataValue(rawLifecycle, "backendId", optionCode),
      capabilities: lifecycleCapabilities,
      contractVersion: dataValue(rawLifecycle, "contractVersion", optionCode),
      restoreAttachmentActivationContractVersion: dataValue(
        rawLifecycle,
        "restoreAttachmentActivationContractVersion",
        optionCode,
      ),
      restoreAttachmentReconciliationContractVersion: dataValue(
        rawLifecycle,
        "restoreAttachmentReconciliationContractVersion",
        optionCode,
      ),
    };
    for (let index = 0; index < LIFECYCLE_METHODS.length; index += 1) {
      const method = LIFECYCLE_METHODS[index];
      const callback = trustedFunction(
        dataValue(rawLifecycle, method, optionCode),
        optionCode,
      );
      lifecycleRecord[method] = objectFreeze(
        async function lifecycleMethod(...methodArgs) {
          ensure(methodArgs.length === 1, requestCode);
          const carrier = await settledValue(
            settlementByName[`lifecycle:${method}`],
            callback,
            rawLifecycle,
            methodArgs[0],
            identityRequest,
            true,
            outcomeCode,
          );
          return resultSnapshot(carrier.value, outcomeCode);
        },
      );
    }
    const lifecycleBackend = exactFrozenRecord(lifecycleRecord);

    const publicationRecord = objectCreate(null);
    for (let index = 0; index < PUBLICATION_METHODS.length; index += 1) {
      const method = PUBLICATION_METHODS[index];
      const callback = trustedFunction(
        publicationIntrinsics[method],
        optionCode,
      );
      publicationRecord[method] = objectFreeze(
        async function publicationMethod(...methodArgs) {
          ensure(methodArgs.length === 1, requestCode);
          const carrier = await settledValue(
            settlementByName[`publication:${method}`],
            callback,
            rawPublication,
            methodArgs[0],
            identityRequest,
            true,
            outcomeCode,
          );
          return resultSnapshot(carrier.value, outcomeCode);
        },
      );
    }
    const publication = exactFrozenRecord(publicationRecord);
    weakSetAdd(publicationBrands, publication);

    const resolveRestoreDestination = objectFreeze(
      async function resolveRestoreDestination(...methodArgs) {
        ensure(methodArgs.length === 1, requestCode);
        const request = shallowSnapshot(methodArgs[0], requestCode);
        const carrier = await settledValue(
          settlementByName.resolver,
          rawResolver,
          undefined,
          request,
          (input, context) =>
            exactFrozenRecord({
              ...input,
              contractVersion:
                POSTGRES_RESTORE_DESTINATION_RESOLVER_PHYSICAL_CONTRACT_VERSION,
              invocation: context.invocation,
              signal: context.signal,
            }),
          false,
          outcomeCode,
        );
        return resultSnapshot(carrier.value, outcomeCode);
      },
    );

    let stopPromise = null;
    const stop = objectFreeze(function stop(...methodArgs) {
      ensure(methodArgs.length === 0, requestCode);
      if (stopPromise !== null) return stopPromise;
      let rejectStop;
      let resolveStop;
      stopPromise = protectReturnedPromise(
        new PromiseConstructor((resolve, reject) => {
          rejectStop = reject;
          resolveStop = resolve;
        }),
        outcomeCode,
      );
      void callIntrinsic(promiseThenIntrinsic, stopPromise, [
        () => {},
        () => {},
      ]);

      const pending = [];
      let invocationFailed = false;
      for (let index = 0; index < settlements.length; index += 1) {
        try {
          const stopPending = callIntrinsic(
            ownExactFunction(settlements[index], "stop", outcomeCode),
            undefined,
            [],
          );
          pending[pending.length] = observePromise(stopPending, outcomeCode);
        } catch {
          invocationFailed = true;
        }
      }
      let remaining = pending.length;
      const settled = objectFreeze(function settled(failed) {
        invocationFailed = invocationFailed || failed;
        remaining -= 1;
        if (remaining !== 0) return;
        if (invocationFailed) {
          rejectStop(makeError(outcomeCode));
          return;
        }
        resolveStop(exactFrozenRecord({ status: "stopped" }));
      });
      if (remaining === 0) {
        if (invocationFailed) rejectStop(makeError(outcomeCode));
        else resolveStop(exactFrozenRecord({ status: "stopped" }));
        return stopPromise;
      }
      for (let index = 0; index < pending.length; index += 1) {
        try {
          void callIntrinsic(promiseThenIntrinsic, pending[index], [
            (result) => settled(result.failed),
            () => settled(true),
          ]);
        } catch {
          settled(true);
        }
      }
      return stopPromise;
    });

    const binding = exactFrozenRecord({
      contractVersion: POSTGRES_DETACHED_RESTORE_PHYSICAL_BINDINGS_CONTRACT_VERSION,
      lifecycleBackend,
      publication,
      resolveRestoreDestination,
      stop,
      supervisor,
    });
    weakSetAdd(bindingBrands, binding);
    return binding;
  }

  try {
    return assembleBindings();
  } catch (error) {
    constructionCleanup(settlements);
    if (isInternalError(error)) throw error;
    fail(optionCode);
  }
}

export function isPostgresDetachedRestorePhysicalBindings(value) {
  try {
    return (
      arguments.length === 1 &&
      value !== null &&
      typeof value === "object" &&
      !isProxy(value) &&
      weakSetHas(bindingBrands, value)
    );
  } catch {
    return false;
  }
}

export function isPostgresDetachedRestorePublicationBinding(value) {
  try {
    return (
      arguments.length === 1 &&
      value !== null &&
      typeof value === "object" &&
      !isProxy(value) &&
      weakSetHas(publicationBrands, value)
    );
  } catch {
    return false;
  }
}

objectFreeze(PostgresDetachedRestorePhysicalBindingsError.prototype);
objectFreeze(PostgresDetachedRestorePhysicalBindingsError);
objectFreeze(createPostgresDetachedRestorePhysicalBindings);
objectFreeze(isPostgresDetachedRestorePhysicalBindings);
objectFreeze(isPostgresDetachedRestorePublicationBinding);
