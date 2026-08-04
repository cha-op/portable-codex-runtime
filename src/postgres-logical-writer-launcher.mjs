import { Hash, createHash } from "node:crypto";
import { types as utilTypes } from "node:util";

import {
  PlatformImageReservationCoordinator,
} from "./platform-image-reservation.mjs";
import {
  SESSION_OPERATION_CONFLICT_CLASS,
  WRITER_LAUNCH_ATTEMPT_OPERATION_KIND,
  createWriterLaunchAttemptOperationRequest,
} from "./postgres-session-authority.mjs";
import {
  assertCheckpointDescriptor,
  assertLeaseGrant,
  assertSessionAttachment,
  assertStorageMutationRequest,
} from "./session-storage-contracts.mjs";
import {
  STOPPED_WRITER_STOP_CONFIRMED,
  StoppedWriterCapabilityCoordinator,
} from "./stopped-writer-capability.mjs";

const arrayIncludesIntrinsic = Array.prototype.includes;
const arrayIsArray = Array.isArray;
const ArrayConstructor = Array;
const arrayPrototype = Array.prototype;
const createHashIntrinsic = createHash;
const functionToStringIntrinsic = Function.prototype.toString;
const hashDigestIntrinsic = Hash.prototype.digest;
const hashUpdateIntrinsic = Hash.prototype.update;
const imageConsumeReservationIntrinsic =
  PlatformImageReservationCoordinator.prototype.consumeReservation;
const imageRevalidateReservationIntrinsic =
  PlatformImageReservationCoordinator.prototype.revalidateReservation;
const mapGetIntrinsic = Map.prototype.get;
const mapHasIntrinsic = Map.prototype.has;
const mapSetIntrinsic = Map.prototype.set;
const MapConstructor = Map;
const numberIsFinite = Number.isFinite;
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
const promiseResolveIntrinsic = Promise.resolve;
const promiseSpeciesSymbol = Symbol.species;
const promiseThenIntrinsic = Promise.prototype.then;
const reflectApply = Reflect.apply;
const reflectOwnKeys = Reflect.ownKeys;
const regexpExecIntrinsic = RegExp.prototype.exec;
const stoppedRegisterWriterIntrinsic =
  StoppedWriterCapabilityCoordinator.prototype.registerWriter;
const TypeErrorConstructor = TypeError;
const WeakSetConstructor = WeakSet;
const weakSetAddIntrinsic = WeakSet.prototype.add;
const weakSetDeleteIntrinsic = WeakSet.prototype.delete;
const weakSetHasIntrinsic = WeakSet.prototype.has;
const {
  isAsyncFunction: isAsyncFunctionValue,
  isGeneratorFunction: isGeneratorFunctionValue,
  isGeneratorObject: isGeneratorObjectValue,
  isPromise: isPromiseValue,
  isProxy: isProxyValue,
} = utilTypes;

export const LOGICAL_WRITER_LAUNCH_CONTRACT_VERSION = 1;

const CALLBACK_RECEIPT_VERSION = 1;
const MAX_DATA_TREE_DEPTH = 24;
const MAX_DATA_TREE_NODES = 16_384;
const NATIVE_FUNCTION_SOURCE_PATTERN =
  /\{\s*\[\s*native\s+code\s*\]\s*\}$/u;
const OPAQUE_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u;
const REVISION_PATTERN = /^(?:0|[1-9][0-9]{0,18})$/u;
const SHA256_PATTERN = /^[0-9a-f]{64}$/u;
const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u;

const OPTION_KEYS = objectFreeze([
  "authority",
  "imageReservations",
  "operationGuard",
  "stoppedWriterCoordinator",
  "supervisor",
]);
const SUPERVISOR_KEYS = objectFreeze([
  "contractVersion",
  "launchWriter",
  "reconcileWriterLaunch",
  "supervisorId",
]);
const AUTHORITY_METHODS = objectFreeze([
  "cancelPreparedOperation",
  "claimWriterLaunchAttemptDispatch",
  "finalizeWriterLaunchAttemptStarted",
  "finalizeWriterLaunchAttemptStopped",
  "markOperationUncertain",
  "readSession",
  "readWriterLaunchAttempt",
  "reserveOperation",
]);
const RUN_INPUT_KEYS = objectFreeze([
  "generation",
  "imageReservation",
  "launchAttemptId",
]);
const IMAGE_RESERVATION_KEYS = objectFreeze([
  "configBytes",
  "descriptor",
  "inspectCodex",
  "reservation",
]);
const RECONCILE_INPUT_KEYS = objectFreeze(["launchAttemptId"]);
const RESOLVER_INPUT_KEYS = objectFreeze([
  "attachment",
  "checkpoint",
  "request",
]);
const PROBE_KEYS = objectFreeze(["assertHeld"]);
const SESSION_KEYS = objectFreeze([
  "createdAt",
  "document",
  "revision",
  "sessionId",
  "updatedAt",
]);
const SESSION_DOCUMENT_KEYS = objectFreeze([
  "activeOperation",
  "attachment",
  "backendCapabilities",
  "documentVersion",
  "lastOperation",
  "launch",
  "lease",
  "lifecycle",
  "manifest",
  "recovery",
  "storageRef",
  "writerEpoch",
]);
const ACTIVE_OPERATION_KEYS = objectFreeze([
  "conflictClass",
  "expectedSessionRevision",
  "kind",
  "operationId",
  "operationRevision",
  "requestSha256",
  "reservationId",
  "state",
]);
const LAST_OPERATION_KEYS = objectFreeze([
  ...ACTIVE_OPERATION_KEYS,
  "resultSha256",
]);
const OPERATION_KEYS = objectFreeze([
  "conflictClass",
  "createdAt",
  "expectedSession",
  "kind",
  "operationId",
  "request",
  "requestSha256",
  "result",
  "retiredAt",
  "revision",
  "sessionId",
  "state",
  "updatedAt",
]);
const RESERVATION_KEYS = objectFreeze([
  "conflictClass",
  "createdAt",
  "expectedSessionRevision",
  "expiresAt",
  "kind",
  "operationId",
  "releasedAt",
  "requestSha256",
  "reservationId",
  "sessionId",
  "state",
  "updatedAt",
]);
const ATTEMPT_KEYS = objectFreeze([
  "contractVersion",
  "launchAttemptId",
  "request",
  "result",
  "state",
]);
const TYPED_REQUEST_KEYS = objectFreeze([
  "attachment",
  "contractVersion",
  "fencingEpoch",
  "generation",
  "lease",
  "measuredImage",
  "supervisor",
]);
const GENERATION_REFERENCE_KEYS = objectFreeze([
  "bindingSha256",
  "checkpointId",
  "claimedAt",
  "committedAt",
  "documentSha256",
  "generationId",
  "operationId",
  "sessionId",
  "state",
]);
const GENERATION_SNAPSHOT_KEYS = objectFreeze([
  "binding",
  "checkpointId",
  "claimedAt",
  "committedAt",
  "document",
  "generationId",
  "operationId",
  "sessionId",
  "state",
]);
const MEASURED_IMAGE_KEYS = objectFreeze(["projection", "runtimeIdentity"]);
const IMAGE_PROJECTION_KEYS = objectFreeze([
  "codexSandbox",
  "codexVersion",
  "platformImage",
]);
const PLATFORM_IMAGE_KEYS = objectFreeze([
  "architecture",
  "config",
  "digest",
  "mediaType",
  "os",
  "size",
]);
const IMAGE_CONFIG_KEYS = objectFreeze(["digest", "mediaType", "size"]);
const RUNTIME_IDENTITY_KEYS = objectFreeze([
  "codexBinaryPath",
  "codexBinarySha256",
  "codexVersion",
  "platformImageDigest",
]);
const SUPERVISOR_IDENTITY_KEYS = objectFreeze([
  "contractVersion",
  "supervisorId",
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
const TERMINAL_RESULT_KEYS = objectFreeze([
  "evidence",
  "outcome",
  "resultVersion",
]);
const CANCELLATION_RESULT_KEYS = objectFreeze([
  "outcome",
  "reason",
  "resultVersion",
]);
const LAUNCH_POINTER_KEYS = objectFreeze([
  "attachmentId",
  "attachmentSha256",
  "contractVersion",
  "fencingEpoch",
  "generation",
  "launchAttemptId",
  "launchResultSha256",
  "leaseId",
  "leaseSha256",
  "measuredImageSha256",
  "processIncarnationId",
  "startedAt",
  "supervisorId",
  "supervisorProofId",
  "writerIncarnationId",
]);
const RESERVE_RECEIPT_KEYS = objectFreeze([
  "acquired",
  "operation",
  "reservation",
  "session",
  "status",
]);
const CLAIM_GRANTED_RECEIPT_KEYS = objectFreeze([
  "attempt",
  "authorityNow",
  "dispatchGranted",
  "generation",
  "operation",
  "reservation",
  "session",
  "status",
]);
const CLAIM_NOT_GRANTED_RECEIPT_KEYS = objectFreeze([
  "attempt",
  "dispatchGranted",
  "generation",
  "operation",
  "reservation",
  "session",
  "status",
]);
const READ_RECEIPT_KEYS = objectFreeze([
  "attempt",
  "launch",
  "operation",
  "reservation",
  "session",
  "status",
]);
const FINALIZE_RECEIPT_KEYS = objectFreeze([
  "attempt",
  "finalized",
  "launch",
  "operation",
  "reservation",
  "session",
  "status",
]);
const CANCEL_RECEIPT_KEYS = objectFreeze([
  "cancelled",
  "operation",
  "reservation",
  "session",
  "status",
]);
const LAUNCH_CALLBACK_RECEIPT_KEYS = objectFreeze([
  "evidence",
  "receiptVersion",
  "stopWriter",
]);
const RECONCILE_CALLBACK_RECEIPT_KEYS = objectFreeze([
  "evidence",
  "receiptVersion",
]);
const ATTACHMENT_KEYS = objectFreeze([
  "attachmentId",
  "backendId",
  "contractVersion",
  "fencingEpoch",
  "holderId",
  "kind",
  "leaseId",
  "mode",
  "operationId",
  "proofId",
  "rootPath",
  "sessionId",
  "storageId",
]);
const LEASE_KEYS = objectFreeze([
  "contractVersion",
  "expiresAt",
  "fencingEpoch",
  "holderId",
  "leaseId",
  "sessionId",
]);
const STOP_BINDING_KEYS = objectFreeze([
  "attachment",
  "processIncarnationId",
  "stopOperationId",
  "writerFence",
  "writerIncarnationId",
]);
const WRITER_FENCE_KEYS = objectFreeze([
  "contractVersion",
  "fencingEpoch",
  "holderId",
  "leaseId",
  "sessionId",
]);

const ERROR_MESSAGES = objectFreeze({
  invalid_logical_writer_launch_request:
    "Logical writer launch request is invalid",
  logical_writer_handle_unavailable:
    "The original writer handle is unavailable; stop or fence is required",
  logical_writer_launch_outcome_uncertain:
    "Logical writer launch outcome is uncertain",
});
const INTERNAL_ERRORS = new WeakSetConstructor();
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

function mapGet(map, key) {
  return callIntrinsic(mapGetIntrinsic, map, [key]);
}

function mapHas(map, key) {
  return callIntrinsic(mapHasIntrinsic, map, [key]);
}

function mapSet(map, key, value) {
  callIntrinsic(mapSetIntrinsic, map, [key, value]);
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

function regexpTest(pattern, value) {
  return callIntrinsic(regexpExecIntrinsic, pattern, [value]) !== null;
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

export class PostgresLogicalWriterLauncherError extends Error {
  constructor(code) {
    if (typeof code !== "string" || !objectHasOwn(ERROR_MESSAGES, code)) {
      throw new TypeErrorConstructor(
        "Unsupported PostgreSQL logical writer launcher error",
      );
    }
    const message = ERROR_MESSAGES[code];
    super(message);
    objectDefineProperty(this, "name", {
      enumerable: false,
      value: "PostgresLogicalWriterLauncherError",
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
      value: `PostgresLogicalWriterLauncherError: ${message}`,
    });
    objectFreeze(this);
  }
}

function makeError(code) {
  const error = new PostgresLogicalWriterLauncherError(code);
  weakSetAdd(INTERNAL_ERRORS, error);
  return error;
}

function fail(code) {
  throw makeError(code);
}

function ensure(condition, code) {
  if (!condition) fail(code);
}

function isInternalError(error, code = undefined) {
  return (
    error !== null &&
    typeof error === "object" &&
    weakSetHas(INTERNAL_ERRORS, error) &&
    (code === undefined || error.code === code)
  );
}

function exactDataObjectVariant(value, keySets, code) {
  ensure(
    value !== null &&
      typeof value === "object" &&
      !arrayIsArray(value) &&
      !isProxyValue(value),
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
  ensure(prototype === objectPrototype || prototype === null, code);
  let expectedKeys = null;
  for (let index = 0; index < keySets.length; index += 1) {
    const candidate = keySets[index];
    if (actualKeys.length === candidate.length) {
      let match = true;
      for (let keyIndex = 0; keyIndex < actualKeys.length; keyIndex += 1) {
        const key = actualKeys[keyIndex];
        if (typeof key !== "string" || !arrayIncludes(candidate, key)) {
          match = false;
          break;
        }
      }
      if (match) {
        expectedKeys = candidate;
        break;
      }
    }
  }
  ensure(expectedKeys !== null, code);
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
    objectDefineProperty(normalized, key, {
      enumerable: true,
      value: descriptor.value,
    });
  }
  return normalized;
}

function exactDataObject(value, keys, code) {
  return exactDataObjectVariant(value, [keys], code);
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

function consumeTreeNode(state, code) {
  state.nodes += 1;
  ensure(
    state.depth <= MAX_DATA_TREE_DEPTH &&
      state.nodes <= MAX_DATA_TREE_NODES,
    code,
  );
}

function deepFrozenDataSnapshot(value, state, code) {
  consumeTreeNode(state, code);
  if (
    value === null ||
    typeof value === "string" ||
    typeof value === "boolean"
  ) {
    return value;
  }
  if (typeof value === "number") {
    ensure(numberIsFinite(value), code);
    return objectIs(value, -0) ? 0 : value;
  }
  ensure(typeof value === "object" && !isProxyValue(value), code);
  const array = arrayIsArray(value);
  ensure(!weakSetHas(state.seen, value), code);
  let prototype;
  let keys;
  try {
    prototype = objectGetPrototypeOf(value);
    keys = reflectOwnKeys(value);
  } catch {
    fail(code);
  }
  ensure(
    array
      ? prototype === arrayPrototype
      : prototype === objectPrototype || prototype === null,
    code,
  );
  weakSetAdd(state.seen, value);
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
        keys.length === lengthDescriptor.value + 1,
      code,
    );
    const result = new ArrayConstructor(lengthDescriptor.value);
    for (let index = 0; index < lengthDescriptor.value; index += 1) {
      const key = String(index);
      const descriptor = objectGetOwnPropertyDescriptor(value, key);
      ensure(
        descriptor?.enumerable === true && objectHasOwn(descriptor, "value"),
        code,
      );
      const childState = {
        depth: state.depth + 1,
        nodes: state.nodes,
        seen: state.seen,
      };
      objectDefineProperty(result, key, {
        enumerable: true,
        value: deepFrozenDataSnapshot(descriptor.value, childState, code),
      });
      state.nodes = childState.nodes;
    }
    weakSetDelete(state.seen, value);
    return objectFreeze(result);
  }
  const result = objectCreate(null);
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
    ensure(typeof key === "string", code);
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
    const childState = {
      depth: state.depth + 1,
      nodes: state.nodes,
      seen: state.seen,
    };
    objectDefineProperty(result, key, {
      enumerable: true,
      value: deepFrozenDataSnapshot(descriptor.value, childState, code),
    });
    state.nodes = childState.nodes;
  }
  weakSetDelete(state.seen, value);
  return objectFreeze(result);
}

function snapshotData(value, code) {
  return deepFrozenDataSnapshot(
    value,
    { depth: 0, nodes: 0, seen: new WeakSetConstructor() },
    code,
  );
}

function sameDataTree(left, right, state, code) {
  consumeTreeNode(state, code);
  if (objectIs(left, right)) {
    ensure(
      left === null || typeof left !== "object" || !isProxyValue(left),
      code,
    );
    return true;
  }
  if (
    left === null ||
    right === null ||
    typeof left !== "object" ||
    typeof right !== "object"
  ) {
    return false;
  }
  ensure(
    !isProxyValue(left) &&
      !isProxyValue(right) &&
      !weakSetHas(state.leftSeen, left) &&
      !weakSetHas(state.rightSeen, right),
    code,
  );
  const leftArray = arrayIsArray(left);
  const rightArray = arrayIsArray(right);
  if (leftArray !== rightArray) return false;
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
    fail(code);
  }
  ensure(
    leftArray
      ? leftPrototype === arrayPrototype && rightPrototype === arrayPrototype
      : (leftPrototype === objectPrototype || leftPrototype === null) &&
          (rightPrototype === objectPrototype || rightPrototype === null),
    code,
  );
  if (leftKeys.length !== rightKeys.length) return false;
  weakSetAdd(state.leftSeen, left);
  weakSetAdd(state.rightSeen, right);
  for (let index = 0; index < leftKeys.length; index += 1) {
    const key = leftKeys[index];
    if (!objectHasOwn(right, key)) return false;
    let leftDescriptor;
    let rightDescriptor;
    try {
      leftDescriptor = objectGetOwnPropertyDescriptor(left, key);
      rightDescriptor = objectGetOwnPropertyDescriptor(right, key);
    } catch {
      fail(code);
    }
    ensure(
      objectHasOwn(leftDescriptor, "value") &&
        objectHasOwn(rightDescriptor, "value") &&
        (key === "length" ||
          (leftDescriptor.enumerable === true &&
            rightDescriptor.enumerable === true)),
      code,
    );
    const childState = {
      depth: state.depth + 1,
      leftSeen: state.leftSeen,
      nodes: state.nodes,
      rightSeen: state.rightSeen,
    };
    if (
      !sameDataTree(
        leftDescriptor.value,
        rightDescriptor.value,
        childState,
        code,
      )
    ) {
      return false;
    }
    state.nodes = childState.nodes;
  }
  weakSetDelete(state.leftSeen, left);
  weakSetDelete(state.rightSeen, right);
  return true;
}

function sameContent(left, right, code) {
  return sameDataTree(
    left,
    right,
    {
      depth: 0,
      leftSeen: new WeakSetConstructor(),
      nodes: 0,
      rightSeen: new WeakSetConstructor(),
    },
    code,
  );
}

function assertOpaqueId(value, code) {
  ensure(typeof value === "string" && regexpTest(OPAQUE_ID_PATTERN, value), code);
  return value;
}

function assertSessionId(value, code) {
  ensure(typeof value === "string" && regexpTest(UUID_PATTERN, value), code);
  return value;
}

function assertRevision(value, code) {
  ensure(typeof value === "string" && regexpTest(REVISION_PATTERN, value), code);
  return value;
}

function assertSha256(value, code) {
  ensure(typeof value === "string" && regexpTest(SHA256_PATTERN, value), code);
  return value;
}

function assertSourceBackedFunction(value, { asynchronous, code }) {
  ensure(
    typeof value === "function" &&
      !isProxyValue(value) &&
      !isGeneratorFunctionValue(value) &&
      isAsyncFunctionValue(value) === asynchronous,
    code,
  );
  let source;
  try {
    source = callIntrinsic(functionToStringIntrinsic, value, []);
  } catch {
    fail(code);
  }
  ensure(!regexpTest(NATIVE_FUNCTION_SOURCE_PATTERN, source), code);
  return value;
}

function assertCallback(value, code) {
  ensure(
    typeof value === "function" &&
      !isProxyValue(value) &&
      !isGeneratorFunctionValue(value),
    code,
  );
  return value;
}

function lookupAsyncMethod(receiver, name, code) {
  ensure(
    receiver !== null &&
      arrayIncludes(["object", "function"], typeof receiver) &&
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
      return assertSourceBackedFunction(descriptor.value, {
        asynchronous: true,
        code,
      });
    }
    try {
      current = objectGetPrototypeOf(current);
    } catch {
      fail(code);
    }
  }
  fail(code);
}

function rejectThenableObject(value, code) {
  ensure(
    value !== null &&
      arrayIncludes(["object", "function"], typeof value) &&
      !isProxyValue(value),
    code,
  );
  let current = value;
  while (current !== null) {
    ensure(!isProxyValue(current), code);
    let descriptor;
    try {
      descriptor = objectGetOwnPropertyDescriptor(current, "then");
    } catch {
      fail(code);
    }
    ensure(descriptor === undefined, code);
    try {
      current = objectGetPrototypeOf(current);
    } catch {
      fail(code);
    }
  }
}

function collaboratorBinding(value, methodNames, code) {
  rejectThenableObject(value, code);
  const methods = objectCreate(null);
  for (let index = 0; index < methodNames.length; index += 1) {
    const name = methodNames[index];
    objectDefineProperty(methods, name, {
      enumerable: true,
      value: lookupAsyncMethod(value, name, code),
    });
  }
  return exactFrozenRecord({
    methods: exactFrozenRecord(methods),
    receiver: value,
  });
}

function isSafeNativePromise(value) {
  if (
    !isPromiseValue(value) ||
    isProxyValue(value) ||
    isGeneratorObjectValue(value)
  ) {
    return false;
  }
  let current = value;
  while (current !== null) {
    if (isProxyValue(current)) return false;
    let descriptor;
    try {
      descriptor = objectGetOwnPropertyDescriptor(current, "constructor");
    } catch {
      return false;
    }
    if (descriptor !== undefined) {
      return (
        objectHasOwn(descriptor, "value") &&
        descriptor.value === PromiseConstructor
      );
    }
    try {
      current = objectGetPrototypeOf(current);
    } catch {
      return false;
    }
  }
  return false;
}

async function invokeAsync(binding, name, args, code) {
  let pending;
  try {
    pending = callIntrinsic(binding.methods[name], binding.receiver, args);
  } catch {
    fail(code);
  }
  ensure(isSafeNativePromise(pending), code);
  try {
    return await pending;
  } catch {
    fail(code);
  }
}

async function invokeGuard(binding, args, code) {
  let pending;
  try {
    pending = callIntrinsic(
      binding.methods.runExclusive,
      binding.receiver,
      args,
    );
  } catch (error) {
    if (isInternalError(error)) throw error;
    fail(code);
  }
  ensure(isSafeNativePromise(pending), code);
  try {
    return await pending;
  } catch (error) {
    if (isInternalError(error)) throw error;
    fail(code);
  }
}

async function invokeSupervisor(callback, context, code) {
  let pending;
  try {
    pending = callIntrinsic(callback, undefined, [context]);
  } catch {
    fail(code);
  }
  ensure(isSafeNativePromise(pending), code);
  try {
    return await pending;
  } catch {
    fail(code);
  }
}

async function invokeImageCoordinator(coordinator, method, options, code) {
  let pending;
  try {
    pending = callIntrinsic(method, coordinator, [options]);
  } catch {
    fail(code);
  }
  ensure(
    isPromiseValue(pending) &&
      !isProxyValue(pending) &&
      !isGeneratorObjectValue(pending),
    code,
  );
  try {
    return await pending;
  } catch {
    fail(code);
  }
}

function normalizeProbe(value, code) {
  const normalized = exactDataObject(value, PROBE_KEYS, code);
  return exactFrozenRecord({
    assertHeld: assertCallback(normalized.assertHeld, code),
  });
}

async function assertGuardHeld(probe, code) {
  let pending;
  try {
    pending = callIntrinsic(probe.assertHeld, undefined, []);
  } catch {
    fail(code);
  }
  ensure(isSafeNativePromise(pending), code);
  try {
    await pending;
  } catch {
    fail(code);
  }
}

function normalizeSession(value, code) {
  const session = exactDataObject(value, SESSION_KEYS, code);
  const document = exactDataObject(
    session.document,
    SESSION_DOCUMENT_KEYS,
    code,
  );
  ensure(
    (document.documentVersion === 2 || document.documentVersion === 3) &&
      document.recovery === null &&
      arrayIncludes(
        ["ATTACHED", "ATTACHING", "BLOCKED", "FENCING", "RELEASING"],
        document.lifecycle,
      ),
    code,
  );
  return snapshotData(
    {
      createdAt: session.createdAt,
      document: session.document,
      revision: assertRevision(session.revision, code),
      sessionId: assertSessionId(session.sessionId, code),
      updatedAt: session.updatedAt,
    },
    code,
  );
}

function validateSessionPointer(session, operation, reservation, code) {
  const document = exactDataObject(
    session.document,
    SESSION_DOCUMENT_KEYS,
    code,
  );
  if (operation.state === "committed") {
    if (document.lastOperation?.operationId === operation.operationId) {
      const last = exactDataObject(
        document.lastOperation,
        LAST_OPERATION_KEYS,
        code,
      );
      ensure(
        last.kind === operation.kind &&
          last.state === "committed" &&
          last.operationRevision === operation.revision &&
          last.reservationId === reservation.reservationId &&
          last.requestSha256 === operation.requestSha256,
        code,
      );
    }
    return;
  }
  const active = exactDataObject(
    document.activeOperation,
    ACTIVE_OPERATION_KEYS,
    code,
  );
  ensure(
    active.operationId === operation.operationId &&
      active.kind === operation.kind &&
      active.state === operation.state &&
      active.operationRevision === operation.revision &&
      active.reservationId === reservation.reservationId &&
      active.requestSha256 === operation.requestSha256,
    code,
  );
}

function normalizeMeasuredImage(value, code) {
  const measured = exactDataObject(value, MEASURED_IMAGE_KEYS, code);
  const projection = exactDataObject(
    measured.projection,
    IMAGE_PROJECTION_KEYS,
    code,
  );
  const platformImage = exactDataObject(
    projection.platformImage,
    PLATFORM_IMAGE_KEYS,
    code,
  );
  const config = exactDataObject(platformImage.config, IMAGE_CONFIG_KEYS, code);
  const runtimeIdentity = exactDataObject(
    measured.runtimeIdentity,
    RUNTIME_IDENTITY_KEYS,
    code,
  );
  ensure(
    typeof projection.codexSandbox === "string" &&
      typeof projection.codexVersion === "string" &&
      typeof platformImage.architecture === "string" &&
      typeof platformImage.digest === "string" &&
      typeof platformImage.mediaType === "string" &&
      typeof platformImage.os === "string" &&
      numberIsSafeInteger(platformImage.size) &&
      platformImage.size >= 0 &&
      typeof config.digest === "string" &&
      typeof config.mediaType === "string" &&
      numberIsSafeInteger(config.size) &&
      config.size >= 0 &&
      typeof runtimeIdentity.codexBinaryPath === "string" &&
      assertSha256(runtimeIdentity.codexBinarySha256, code) ===
        runtimeIdentity.codexBinarySha256 &&
      runtimeIdentity.codexVersion === projection.codexVersion &&
      runtimeIdentity.platformImageDigest === platformImage.digest,
    code,
  );
  return snapshotData(value, code);
}

function normalizeTypedRequest(value, expectedSession, code) {
  const request = exactDataObject(value, TYPED_REQUEST_KEYS, code);
  const supervisor = exactDataObject(
    request.supervisor,
    SUPERVISOR_IDENTITY_KEYS,
    code,
  );
  const generation = exactDataObject(
    request.generation,
    GENERATION_REFERENCE_KEYS,
    code,
  );
  let attachment;
  let lease;
  try {
    attachment = assertSessionAttachment(request.attachment);
    lease = assertLeaseGrant(request.lease);
  } catch {
    fail(code);
  }
  ensure(
    request.contractVersion === LOGICAL_WRITER_LAUNCH_CONTRACT_VERSION &&
      request.fencingEpoch === lease.fencingEpoch &&
      attachment.sessionId === lease.sessionId &&
      attachment.leaseId === lease.leaseId &&
      attachment.holderId === lease.holderId &&
      attachment.fencingEpoch === lease.fencingEpoch &&
      generation.state === "committed" &&
      generation.sessionId === lease.sessionId &&
      assertSha256(generation.bindingSha256, code) ===
        generation.bindingSha256 &&
      assertSha256(generation.documentSha256, code) ===
        generation.documentSha256 &&
      supervisor.contractVersion === LOGICAL_WRITER_LAUNCH_CONTRACT_VERSION &&
      assertOpaqueId(supervisor.supervisorId, code) === supervisor.supervisorId,
    code,
  );
  normalizeMeasuredImage(request.measuredImage, code);
  if (expectedSession !== null) {
    ensure(
      expectedSession.sessionId === lease.sessionId &&
        expectedSession.document.lifecycle === "ATTACHED" &&
        sameContent(
          expectedSession.document.attachment,
          request.attachment,
          code,
        ) &&
        sameContent(expectedSession.document.lease, request.lease, code),
      code,
    );
  }
  return snapshotData(value, code);
}

function normalizeTerminalEvidence(value, attempt, statuses, code) {
  const evidence = exactDataObject(value, EVIDENCE_KEYS, code);
  ensure(
    evidence.contractVersion === LOGICAL_WRITER_LAUNCH_CONTRACT_VERSION &&
      evidence.launchAttemptId === attempt.launchAttemptId &&
      evidence.supervisorId === attempt.request.supervisor.supervisorId &&
      arrayIncludes(statuses, evidence.status) &&
      assertOpaqueId(evidence.proofId, code) === evidence.proofId,
    code,
  );
  if (evidence.status === "not-started") {
    ensure(
      evidence.processIncarnationId === null &&
        evidence.writerIncarnationId === null,
      code,
    );
  } else {
    assertOpaqueId(evidence.processIncarnationId, code);
    assertOpaqueId(evidence.writerIncarnationId, code);
  }
  return snapshotData(value, code);
}

function normalizeOperationResult(value, attempt, code) {
  if (value === null) return exactFrozenRecord({ evidence: null, status: null });
  const outcomeDescriptor = objectGetOwnPropertyDescriptor(value, "outcome");
  ensure(outcomeDescriptor !== undefined && objectHasOwn(outcomeDescriptor, "value"), code);
  if (outcomeDescriptor.value === "cancelled-before-dispatch") {
    const result = exactDataObject(value, CANCELLATION_RESULT_KEYS, code);
    ensure(
      result.resultVersion === 1 &&
        typeof result.reason === "string" &&
        regexpTest(OPAQUE_ID_PATTERN, result.reason),
      code,
    );
    return exactFrozenRecord({
      evidence: null,
      status: "cancelled-before-dispatch",
    });
  }
  const result = exactDataObject(value, TERMINAL_RESULT_KEYS, code);
  ensure(result.resultVersion === 1, code);
  const evidence = normalizeTerminalEvidence(
    result.evidence,
    attempt,
    ["complete-stopped", "not-started", "started"],
    code,
  );
  const outcomes = {
    "complete-stopped": "writer-launch-complete-stopped",
    "not-started": "writer-launch-not-started",
    started: "writer-launch-started",
  };
  ensure(result.outcome === outcomes[evidence.status], code);
  return exactFrozenRecord({ evidence, status: evidence.status });
}

function normalizeOperation(value, launchAttemptId, expectedRequest, code) {
  const normalized = exactDataObject(value, OPERATION_KEYS, code);
  const expectedSession = normalizeSession(normalized.expectedSession, code);
  const request = normalizeTypedRequest(
    normalized.request,
    expectedSession,
    code,
  );
  ensure(
    normalized.conflictClass === SESSION_OPERATION_CONFLICT_CLASS &&
      normalized.kind === WRITER_LAUNCH_ATTEMPT_OPERATION_KIND &&
      normalized.operationId === launchAttemptId &&
      normalized.sessionId === expectedSession.sessionId &&
      assertSha256(normalized.requestSha256, code) ===
        normalized.requestSha256 &&
      arrayIncludes(["prepared", "starting", "uncertain", "committed"], normalized.state),
    code,
  );
  const revision = assertRevision(normalized.revision, code);
  ensure(
    (normalized.state === "prepared" && revision === "0") ||
      (normalized.state === "starting" && revision === "1") ||
      (normalized.state === "uncertain" && revision === "2") ||
      (normalized.state === "committed" &&
        (revision === "1" || revision === "2" || revision === "3")),
    code,
  );
  if (expectedRequest !== null) {
    ensure(sameContent(request, expectedRequest, code), code);
  }
  const provisionalAttempt = exactFrozenRecord({
    contractVersion: LOGICAL_WRITER_LAUNCH_CONTRACT_VERSION,
    launchAttemptId,
    request,
    result: normalized.result,
    state: normalized.state,
  });
  const terminal = normalizeOperationResult(
    normalized.result,
    provisionalAttempt,
    code,
  );
  ensure(
    normalized.state === "committed"
      ? normalized.retiredAt === normalized.updatedAt && terminal.status !== null
      : normalized.retiredAt === null &&
          normalized.result === null &&
          terminal.status === null,
    code,
  );
  const operation = snapshotData(value, code);
  return exactFrozenRecord({ operation, request, terminal });
}

function normalizeReservation(value, operation, code) {
  const reservation = exactDataObject(value, RESERVATION_KEYS, code);
  const expectedState =
    operation.state === "committed" ? "released" : operation.state;
  ensure(
    reservation.conflictClass === SESSION_OPERATION_CONFLICT_CLASS &&
      reservation.operationId === operation.operationId &&
      reservation.sessionId === operation.sessionId &&
      reservation.kind === operation.kind &&
      reservation.expectedSessionRevision === operation.expectedSession.revision &&
      reservation.requestSha256 === operation.requestSha256 &&
      reservation.state === expectedState &&
      assertOpaqueId(reservation.reservationId, code) === reservation.reservationId &&
      reservation.expiresAt === null &&
      (expectedState === "released"
        ? reservation.releasedAt === operation.retiredAt
        : reservation.releasedAt === null),
    code,
  );
  return snapshotData(value, code);
}

function normalizeAttempt(value, operation, request, code) {
  const attempt = exactDataObject(value, ATTEMPT_KEYS, code);
  ensure(
    attempt.contractVersion === LOGICAL_WRITER_LAUNCH_CONTRACT_VERSION &&
      attempt.launchAttemptId === operation.operationId &&
      attempt.state === operation.state &&
      sameContent(attempt.request, request, code) &&
      sameContent(attempt.result, operation.result, code),
    code,
  );
  return snapshotData(value, code);
}

function normalizeLaunchPointer(value, operation, attempt, terminal, code) {
  if (value === null) {
    ensure(terminal.status !== "started", code);
    return null;
  }
  const launch = exactDataObject(value, LAUNCH_POINTER_KEYS, code);
  ensure(
    terminal.status === "started" &&
      launch.contractVersion === LOGICAL_WRITER_LAUNCH_CONTRACT_VERSION &&
      launch.launchAttemptId === operation.operationId &&
      launch.attachmentId === attempt.request.attachment.attachmentId &&
      launch.fencingEpoch === attempt.request.fencingEpoch &&
      launch.leaseId === attempt.request.lease.leaseId &&
      launch.processIncarnationId === terminal.evidence.processIncarnationId &&
      launch.supervisorId === terminal.evidence.supervisorId &&
      launch.supervisorProofId === terminal.evidence.proofId &&
      launch.writerIncarnationId === terminal.evidence.writerIncarnationId &&
      sameContent(launch.generation, attempt.request.generation, code),
    code,
  );
  assertSha256(launch.attachmentSha256, code);
  assertSha256(launch.launchResultSha256, code);
  assertSha256(launch.leaseSha256, code);
  assertSha256(launch.measuredImageSha256, code);
  return snapshotData(value, code);
}

function normalizeReceiptCommon(
  value,
  keys,
  launchAttemptId,
  expectedRequest,
  code,
) {
  const receipt = exactDataObject(value, keys, code);
  const normalizedOperation = normalizeOperation(
    receipt.operation,
    launchAttemptId,
    expectedRequest,
    code,
  );
  const operation = normalizedOperation.operation;
  ensure(receipt.status === operation.state, code);
  const reservation = normalizeReservation(receipt.reservation, operation, code);
  const session = normalizeSession(receipt.session, code);
  ensure(session.sessionId === operation.sessionId, code);
  validateSessionPointer(session, operation, reservation, code);
  return {
    normalizedOperation,
    operation,
    receipt,
    reservation,
    session,
  };
}

function normalizeReserveReceipt(value, launchAttemptId, typedRequest, code) {
  const common = normalizeReceiptCommon(
    value,
    RESERVE_RECEIPT_KEYS,
    launchAttemptId,
    typedRequest,
    code,
  );
  ensure(typeof common.receipt.acquired === "boolean", code);
  if (common.receipt.acquired) {
    ensure(common.operation.state === "prepared", code);
  }
  return exactFrozenRecord({
    acquired: common.receipt.acquired,
    operation: common.operation,
    reservation: common.reservation,
    session: common.session,
    status: common.operation.state,
  });
}

function normalizeClaimReceipt(
  value,
  launchAttemptId,
  typedRequest,
  inputGeneration,
  code,
) {
  const shape = exactDataObjectVariant(
    value,
    [CLAIM_GRANTED_RECEIPT_KEYS, CLAIM_NOT_GRANTED_RECEIPT_KEYS],
    code,
  );
  const keys = objectHasOwn(shape, "authorityNow")
    ? CLAIM_GRANTED_RECEIPT_KEYS
    : CLAIM_NOT_GRANTED_RECEIPT_KEYS;
  const common = normalizeReceiptCommon(
    value,
    keys,
    launchAttemptId,
    typedRequest,
    code,
  );
  ensure(typeof common.receipt.dispatchGranted === "boolean", code);
  const attempt = normalizeAttempt(
    common.receipt.attempt,
    common.operation,
    common.normalizedOperation.request,
    code,
  );
  let generation = null;
  if (common.receipt.generation !== null) {
    exactDataObject(
      common.receipt.generation,
      GENERATION_SNAPSHOT_KEYS,
      code,
    );
    generation = snapshotData(common.receipt.generation, code);
    ensure(sameContent(generation, inputGeneration, code), code);
  }
  if (common.receipt.dispatchGranted) {
    ensure(
      objectHasOwn(common.receipt, "authorityNow") &&
        common.operation.state === "starting" &&
        generation !== null &&
        typeof common.receipt.authorityNow === "string",
      code,
    );
  } else {
    ensure(!objectHasOwn(common.receipt, "authorityNow"), code);
  }
  return exactFrozenRecord({
    attempt,
    authorityNow: common.receipt.authorityNow,
    dispatchGranted: common.receipt.dispatchGranted,
    generation,
    operation: common.operation,
    reservation: common.reservation,
    session: common.session,
    status: common.operation.state,
  });
}

function normalizeReadReceipt(value, launchAttemptId, code) {
  const common = normalizeReceiptCommon(
    value,
    READ_RECEIPT_KEYS,
    launchAttemptId,
    null,
    code,
  );
  const attempt = normalizeAttempt(
    common.receipt.attempt,
    common.operation,
    common.normalizedOperation.request,
    code,
  );
  const launch = normalizeLaunchPointer(
    common.receipt.launch,
    common.operation,
    attempt,
    common.normalizedOperation.terminal,
    code,
  );
  if (launch !== null) {
    ensure(sameContent(common.session.document.launch, launch, code), code);
  }
  return exactFrozenRecord({
    attempt,
    evidence: common.normalizedOperation.terminal.evidence,
    launch,
    operation: common.operation,
    reservation: common.reservation,
    session: common.session,
    status:
      common.operation.state === "committed"
        ? common.normalizedOperation.terminal.status
        : common.operation.state,
  });
}

function normalizeFinalizeReceipt(
  value,
  launchAttemptId,
  expectedRequest,
  expectedEvidence,
  code,
) {
  const common = normalizeReceiptCommon(
    value,
    FINALIZE_RECEIPT_KEYS,
    launchAttemptId,
    expectedRequest,
    code,
  );
  ensure(
    typeof common.receipt.finalized === "boolean" &&
      common.operation.state === "committed",
    code,
  );
  const attempt = normalizeAttempt(
    common.receipt.attempt,
    common.operation,
    common.normalizedOperation.request,
    code,
  );
  ensure(
    sameContent(
      common.normalizedOperation.terminal.evidence,
      expectedEvidence,
      code,
    ),
    code,
  );
  const launch = normalizeLaunchPointer(
    common.receipt.launch,
    common.operation,
    attempt,
    common.normalizedOperation.terminal,
    code,
  );
  ensure(sameContent(common.session.document.launch, launch, code), code);
  return exactFrozenRecord({
    attempt,
    evidence: common.normalizedOperation.terminal.evidence,
    launch,
    operation: common.operation,
    reservation: common.reservation,
    session: common.session,
    status: common.normalizedOperation.terminal.status,
  });
}

function normalizeCancelReceipt(value, launchAttemptId, expectedRequest, code) {
  const common = normalizeReceiptCommon(
    value,
    CANCEL_RECEIPT_KEYS,
    launchAttemptId,
    expectedRequest,
    code,
  );
  ensure(
    typeof common.receipt.cancelled === "boolean" &&
      common.operation.state === "committed" &&
      common.normalizedOperation.terminal.status ===
        "cancelled-before-dispatch",
    code,
  );
}

function operationInput(operation) {
  return exactFrozenRecord({
    expectedSession: operation.expectedSession,
    kind: WRITER_LAUNCH_ATTEMPT_OPERATION_KIND,
    operationId: operation.operationId,
    request: operation.request,
  });
}

function transitionInput(operation) {
  return exactFrozenRecord({
    ...operationInput(operation),
    expectedOperationRevision: operation.revision,
  });
}

function successResult(receipt, writer) {
  return exactFrozenRecord({
    contractVersion: LOGICAL_WRITER_LAUNCH_CONTRACT_VERSION,
    attempt: receipt.attempt,
    evidence: receipt.evidence,
    launch: receipt.launch,
    operation: receipt.operation,
    reservation: receipt.reservation,
    session: receipt.session,
    status: receipt.status,
    writer,
  });
}

function sha256Parts(parts, code) {
  let hash;
  try {
    hash = callIntrinsic(createHashIntrinsic, undefined, ["sha256"]);
    for (let index = 0; index < parts.length; index += 1) {
      callIntrinsic(hashUpdateIntrinsic, hash, [parts[index], "utf8"]);
    }
    const digest = callIntrinsic(hashDigestIntrinsic, hash, ["hex"]);
    ensure(typeof digest === "string" && regexpTest(SHA256_PATTERN, digest), code);
    return digest;
  } catch (error) {
    if (isInternalError(error)) throw error;
    fail(code);
  }
}

function stopOperationId(captureOperationId, launchAttemptId, code) {
  const digest = sha256Parts(
    [
      "portable-codex-runtime:writer-stop:v1",
      "\0",
      captureOperationId,
      "\0",
      launchAttemptId,
    ],
    code,
  );
  return `writer-stop:${digest}`;
}

function normalizeRunInput(value, code) {
  const input = exactDataObject(value, RUN_INPUT_KEYS, code);
  const image = exactDataObject(
    input.imageReservation,
    IMAGE_RESERVATION_KEYS,
    code,
  );
  ensure(
    typeof image.inspectCodex === "function" &&
      !isProxyValue(image.inspectCodex) &&
      image.reservation !== null &&
      typeof image.reservation === "object" &&
      !isProxyValue(image.reservation),
    code,
  );
  const generation = snapshotData(input.generation, code);
  exactDataObject(generation, GENERATION_SNAPSHOT_KEYS, code);
  ensure(
    generation.state === "committed" &&
      generation.document !== null &&
      generation.committedAt !== null,
    code,
  );
  return exactFrozenRecord({
    generation,
    imageReservation: exactFrozenRecord({
      configBytes: image.configBytes,
      descriptor: image.descriptor,
      inspectCodex: image.inspectCodex,
      reservation: image.reservation,
    }),
    launchAttemptId: assertOpaqueId(input.launchAttemptId, code),
  });
}

function normalizeReconcileInput(value, code) {
  const input = exactDataObject(value, RECONCILE_INPUT_KEYS, code);
  return exactFrozenRecord({
    launchAttemptId: assertOpaqueId(input.launchAttemptId, code),
  });
}

function normalizeLaunchCallbackReceipt(value, attempt, code) {
  const receipt = exactDataObject(value, LAUNCH_CALLBACK_RECEIPT_KEYS, code);
  ensure(receipt.receiptVersion === CALLBACK_RECEIPT_VERSION, code);
  const evidence = normalizeTerminalEvidence(
    receipt.evidence,
    attempt,
    ["complete-stopped", "not-started", "started"],
    code,
  );
  let stopWriter = null;
  if (evidence.status === "started") {
    stopWriter = assertSourceBackedFunction(receipt.stopWriter, {
      asynchronous: true,
      code,
    });
  } else {
    ensure(receipt.stopWriter === null, code);
  }
  return exactFrozenRecord({ evidence, stopWriter });
}

function normalizeReconcileCallbackReceipt(value, attempt, code) {
  const receipt = exactDataObject(
    value,
    RECONCILE_CALLBACK_RECEIPT_KEYS,
    code,
  );
  ensure(receipt.receiptVersion === CALLBACK_RECEIPT_VERSION, code);
  return exactFrozenRecord({
    evidence: normalizeTerminalEvidence(
      receipt.evidence,
      attempt,
      ["complete-stopped", "not-started"],
      code,
    ),
  });
}

function assertOpaqueWriterHandle(value, code) {
  ensure(
    value !== null &&
      typeof value === "object" &&
      !arrayIsArray(value) &&
      !isProxyValue(value) &&
      objectGetPrototypeOf(value) === null &&
      reflectOwnKeys(value).length === 0 &&
      objectIsFrozen(value),
    code,
  );
  return value;
}

function evidenceMatchesRecord(evidence, record, code) {
  return (
    evidence.status === "started" &&
    evidence.launchAttemptId === record.launchAttemptId &&
    evidence.processIncarnationId === record.processIncarnationId &&
    evidence.supervisorId === record.supervisorId &&
    evidence.writerIncarnationId === record.writerIncarnationId &&
    sameContent(evidence, record.evidence, code)
  );
}

/**
 * Composes durable launch admission with one-use image and in-process writer
 * capabilities. The registered stop callback deliberately remains a local
 * supervisor seam: production restore must stay disabled until a durable,
 * typed stop transition owns this boundary.
 */
export function createPostgresLogicalWriterLauncher(...args) {
  const optionCode = "invalid_logical_writer_launch_request";
  const outcomeCode = "logical_writer_launch_outcome_uncertain";
  ensure(args.length === 1, optionCode);
  const options = exactDataObject(args[0], OPTION_KEYS, optionCode);
  const authority = collaboratorBinding(
    options.authority,
    AUTHORITY_METHODS,
    optionCode,
  );
  const operationGuard = collaboratorBinding(
    options.operationGuard,
    ["runExclusive"],
    optionCode,
  );
  ensure(
    !isProxyValue(options.imageReservations) &&
      options.imageReservations instanceof PlatformImageReservationCoordinator,
    optionCode,
  );
  ensure(
    !isProxyValue(options.stoppedWriterCoordinator) &&
      options.stoppedWriterCoordinator instanceof
        StoppedWriterCapabilityCoordinator,
    optionCode,
  );
  const supervisorOptions = exactDataObject(
    options.supervisor,
    SUPERVISOR_KEYS,
    optionCode,
  );
  ensure(
    supervisorOptions.contractVersion ===
      LOGICAL_WRITER_LAUNCH_CONTRACT_VERSION,
    optionCode,
  );
  const supervisor = exactFrozenRecord({
    contractVersion: LOGICAL_WRITER_LAUNCH_CONTRACT_VERSION,
    launchWriter: assertSourceBackedFunction(supervisorOptions.launchWriter, {
      asynchronous: true,
      code: optionCode,
    }),
    reconcileWriterLaunch: assertSourceBackedFunction(
      supervisorOptions.reconcileWriterLaunch,
      { asynchronous: true, code: optionCode },
    ),
    supervisorId: assertOpaqueId(supervisorOptions.supervisorId, optionCode),
  });
  const imageReservations = options.imageReservations;
  const stoppedWriterCoordinator = options.stoppedWriterCoordinator;
  const recordsByAttempt = new MapConstructor();
  const recordsByAttachmentId = new MapConstructor();

  async function readAttempt(launchAttemptId) {
    return normalizeReadReceipt(
      await invokeAsync(
        authority,
        "readWriterLaunchAttempt",
        [exactFrozenRecord({ operationId: launchAttemptId })],
        outcomeCode,
      ),
      launchAttemptId,
      outcomeCode,
    );
  }

  async function bestEffortMarkUncertain(operation, state) {
    if (state.attempted || operation.state !== "starting") return;
    state.attempted = true;
    try {
      await invokeAsync(
        authority,
        "markOperationUncertain",
        [
          exactFrozenRecord({
            ...operationInput(operation),
            expectedOperationRevision: "1",
          }),
        ],
        outcomeCode,
      );
    } catch {
      // Durable readback remains authoritative after this best-effort write.
    }
  }

  function adoptCommittedStarted(read) {
    const record = mapGet(recordsByAttempt, read.attempt.launchAttemptId);
    if (
      record === undefined ||
      (record.state !== "provisional" && record.state !== "ready") ||
      !evidenceMatchesRecord(read.evidence, record, outcomeCode) ||
      !sameContent(read.attempt.request, record.request, outcomeCode)
    ) {
      fail("logical_writer_handle_unavailable");
    }
    assertOpaqueWriterHandle(record.writer, outcomeCode);
    if (record.state === "provisional") record.state = "ready";
    return record.writer;
  }

  function terminalResultFromRead(read) {
    ensure(read.operation.state === "committed", outcomeCode);
    if (read.status === "started") {
      return successResult(read, adoptCommittedStarted(read));
    }
    ensure(
      read.status === "not-started" ||
        read.status === "complete-stopped" ||
        read.status === "cancelled-before-dispatch",
      outcomeCode,
    );
    const local = mapGet(recordsByAttempt, read.attempt.launchAttemptId);
    ensure(local === undefined || local.writer === null, outcomeCode);
    return successResult(read, null);
  }

  function exactProvisionalRecord(read) {
    const record = mapGet(recordsByAttempt, read.attempt.launchAttemptId);
    if (
      record === undefined ||
      record.state !== "provisional" ||
      !evidenceMatchesRecord(record.evidence, record, outcomeCode) ||
      !sameContent(read.attempt.request, record.request, outcomeCode)
    ) {
      return null;
    }
    return record;
  }

  async function finalizeWithReadback(
    readLike,
    evidence,
    methodName,
    uncertaintyState,
  ) {
    let finalReceipt;
    try {
      finalReceipt = normalizeFinalizeReceipt(
        await invokeAsync(
          authority,
          methodName,
          [
            exactFrozenRecord({
              evidence,
              expectedOperationRevision: readLike.operation.revision,
              expectedSession: readLike.operation.expectedSession,
              kind: WRITER_LAUNCH_ATTEMPT_OPERATION_KIND,
              operationId: readLike.operation.operationId,
              request: readLike.operation.request,
            }),
          ],
          outcomeCode,
        ),
        readLike.operation.operationId,
        readLike.operation.request,
        evidence,
        outcomeCode,
      );
      const writer =
        evidence.status === "started"
          ? adoptCommittedStarted(finalReceipt)
          : null;
      return successResult(finalReceipt, writer);
    } catch (error) {
      if (
        isInternalError(error, "logical_writer_handle_unavailable")
      ) {
        throw error;
      }
    }

    try {
      const readback = await readAttempt(readLike.operation.operationId);
      if (
        readback.operation.state === "committed" &&
        sameContent(readback.evidence, evidence, outcomeCode)
      ) {
        return terminalResultFromRead(readback);
      }
    } catch (error) {
      if (isInternalError(error, "logical_writer_handle_unavailable")) {
        throw error;
      }
    }
    await bestEffortMarkUncertain(readLike.operation, uncertaintyState);
    fail(outcomeCode);
  }

  function registerStartedWriter(claim, callbackReceipt) {
    const evidence = callbackReceipt.evidence;
    ensure(!mapHas(recordsByAttempt, claim.attempt.launchAttemptId), outcomeCode);
    ensure(
      !mapHas(
        recordsByAttachmentId,
        claim.attempt.request.attachment.attachmentId,
      ),
      outcomeCode,
    );
    const record = {
      attachment: claim.attempt.request.attachment,
      authorizedCapture: null,
      authorizedStopOperationId: null,
      canonicalLease: claim.attempt.request.lease,
      codexSessionId:
        claim.operation.expectedSession.document.manifest.codex.sessionId,
      codexThreadId:
        claim.operation.expectedSession.document.manifest.codex.rootThreadId,
      evidence,
      imageDigest:
        claim.attempt.request.measuredImage.projection.platformImage.digest,
      launchAttemptId: claim.attempt.launchAttemptId,
      processIncarnationId: evidence.processIncarnationId,
      request: claim.attempt.request,
      state: "registering",
      stopWriter: callbackReceipt.stopWriter,
      supervisorId: evidence.supervisorId,
      writer: null,
      writerIncarnationId: evidence.writerIncarnationId,
    };
    const registeredStopWriter = async function registeredStopWriter(
      bindingValue,
    ) {
      const binding = exactDataObject(
        bindingValue,
        STOP_BINDING_KEYS,
        outcomeCode,
      );
      const writerFence = exactDataObject(
        binding.writerFence,
        WRITER_FENCE_KEYS,
        outcomeCode,
      );
      ensure(
        record.state === "ready" &&
          record.authorizedStopOperationId !== null &&
          binding.stopOperationId === record.authorizedStopOperationId &&
          binding.processIncarnationId === record.processIncarnationId &&
          binding.writerIncarnationId === record.writerIncarnationId &&
          sameContent(binding.attachment, record.attachment, outcomeCode) &&
          writerFence.contractVersion === record.canonicalLease.contractVersion &&
          writerFence.sessionId === record.canonicalLease.sessionId &&
          writerFence.leaseId === record.canonicalLease.leaseId &&
          writerFence.holderId === record.canonicalLease.holderId &&
          writerFence.fencingEpoch === record.canonicalLease.fencingEpoch,
        outcomeCode,
      );
      record.state = "stopping";
      try {
        const stopped = await invokeSupervisor(
          record.stopWriter,
          snapshotData(bindingValue, outcomeCode),
          outcomeCode,
        );
        ensure(stopped === STOPPED_WRITER_STOP_CONFIRMED, outcomeCode);
        record.state = "stopped";
        return STOPPED_WRITER_STOP_CONFIRMED;
      } catch {
        record.state = "lost";
        fail(outcomeCode);
      }
    };
    objectFreeze(registeredStopWriter);
    try {
      record.writer = callIntrinsic(
        stoppedRegisterWriterIntrinsic,
        stoppedWriterCoordinator,
        [
          exactFrozenRecord({
            attachment: record.attachment,
            canonicalLease: record.canonicalLease,
            processIncarnationId: record.processIncarnationId,
            stopWriter: registeredStopWriter,
            writerIncarnationId: record.writerIncarnationId,
          }),
        ],
      );
      assertOpaqueWriterHandle(record.writer, outcomeCode);
      record.state = "provisional";
      mapSet(recordsByAttempt, record.launchAttemptId, record);
      mapSet(
        recordsByAttachmentId,
        record.attachment.attachmentId,
        record,
      );
      return record;
    } catch (error) {
      record.state = "lost";
      if (isInternalError(error)) throw error;
      fail(outcomeCode);
    }
  }

  async function reconcileWithinGuard(
    launchAttemptId,
    probe,
    uncertaintyState,
    expectedRequest = null,
  ) {
    let read = await readAttempt(launchAttemptId);
    if (expectedRequest !== null) {
      ensure(
        sameContent(read.attempt.request, expectedRequest, optionCode),
        optionCode,
      );
    }
    if (read.operation.state === "committed") {
      return terminalResultFromRead(read);
    }
    if (read.operation.state === "prepared") {
      normalizeCancelReceipt(
        await invokeAsync(
          authority,
          "cancelPreparedOperation",
          [
            exactFrozenRecord({
              ...operationInput(read.operation),
              expectedOperationRevision: "0",
              reason: "launch-dispatch-not-started",
            }),
          ],
          outcomeCode,
        ),
        launchAttemptId,
        read.operation.request,
        outcomeCode,
      );
      read = await readAttempt(launchAttemptId);
      ensure(
        read.operation.state === "committed" &&
          read.status === "cancelled-before-dispatch",
        outcomeCode,
      );
      return terminalResultFromRead(read);
    }
    const provisional = exactProvisionalRecord(read);
    if (provisional !== null) {
      await assertGuardHeld(probe, outcomeCode);
      return finalizeWithReadback(
        read,
        provisional.evidence,
        "finalizeWriterLaunchAttemptStarted",
        uncertaintyState,
      );
    }
    if (read.operation.state === "starting") {
      await bestEffortMarkUncertain(read.operation, uncertaintyState);
      read = await readAttempt(launchAttemptId);
      if (read.operation.state === "committed") {
        return terminalResultFromRead(read);
      }
    }
    ensure(
      read.operation.state === "starting" ||
        read.operation.state === "uncertain",
      outcomeCode,
    );
    await assertGuardHeld(probe, outcomeCode);
    const callbackReceipt = normalizeReconcileCallbackReceipt(
      await invokeSupervisor(
        supervisor.reconcileWriterLaunch,
        exactFrozenRecord({
          contractVersion: LOGICAL_WRITER_LAUNCH_CONTRACT_VERSION,
          attempt: read.attempt,
          launch: read.launch,
          operation: read.operation,
          reservation: read.reservation,
          session: read.session,
        }),
        outcomeCode,
      ),
      read.attempt,
      outcomeCode,
    );
    await assertGuardHeld(probe, outcomeCode);
    const local = mapGet(recordsByAttempt, launchAttemptId);
    if (local !== undefined && local.state === "provisional") {
      local.state = "lost";
    }
    return finalizeWithReadback(
      read,
      callbackReceipt.evidence,
      "finalizeWriterLaunchAttemptStopped",
      uncertaintyState,
    );
  }

  async function runLaunchInternal(...runArgs) {
    ensure(runArgs.length === 1, optionCode);
    const input = normalizeRunInput(runArgs[0], optionCode);
    const uncertaintyState = { attempted: false };
    let dispatchDefinitelyBegan = false;
    let dispatchOperation = null;

    try {
      return await invokeGuard(
        operationGuard,
        [
          input.launchAttemptId,
          async (probeValue) => {
            const probe = normalizeProbe(probeValue, outcomeCode);
            const expectedSession = normalizeSession(
              await invokeAsync(
                authority,
                "readSession",
                [exactFrozenRecord({ sessionId: input.generation.sessionId })],
                outcomeCode,
              ),
              outcomeCode,
            );
            let measuredImage;
            try {
              measuredImage = normalizeMeasuredImage(
                await invokeImageCoordinator(
                  imageReservations,
                  imageRevalidateReservationIntrinsic,
                  input.imageReservation,
                  outcomeCode,
                ),
                outcomeCode,
              );
            } catch {
              fail(outcomeCode);
            }
            let typedRequest;
            try {
              typedRequest = createWriterLaunchAttemptOperationRequest({
                expectedSession,
                generation: input.generation,
                measuredImage,
                supervisor: exactFrozenRecord({
                  contractVersion: supervisor.contractVersion,
                  supervisorId: supervisor.supervisorId,
                }),
              });
            } catch {
              fail(optionCode);
            }
            const baseInput = exactFrozenRecord({
              expectedSession,
              kind: WRITER_LAUNCH_ATTEMPT_OPERATION_KIND,
              operationId: input.launchAttemptId,
              request: typedRequest,
            });

            let reserve;
            try {
              reserve = normalizeReserveReceipt(
                await invokeAsync(
                  authority,
                  "reserveOperation",
                  [baseInput],
                  outcomeCode,
                ),
                input.launchAttemptId,
                typedRequest,
                outcomeCode,
              );
            } catch {
              return reconcileWithinGuard(
                input.launchAttemptId,
                probe,
                uncertaintyState,
                typedRequest,
              );
            }
            if (!reserve.acquired) {
              return reconcileWithinGuard(
                input.launchAttemptId,
                probe,
                uncertaintyState,
                typedRequest,
              );
            }

            let claim;
            try {
              claim = normalizeClaimReceipt(
                await invokeAsync(
                  authority,
                  "claimWriterLaunchAttemptDispatch",
                  [
                    exactFrozenRecord({
                      ...baseInput,
                      expectedOperationRevision: "0",
                    }),
                  ],
                  outcomeCode,
                ),
                input.launchAttemptId,
                typedRequest,
                input.generation,
                outcomeCode,
              );
            } catch {
              return reconcileWithinGuard(
                input.launchAttemptId,
                probe,
                uncertaintyState,
                typedRequest,
              );
            }
            if (!claim.dispatchGranted) {
              return reconcileWithinGuard(
                input.launchAttemptId,
                probe,
                uncertaintyState,
                typedRequest,
              );
            }
            dispatchDefinitelyBegan = true;
            dispatchOperation = claim.operation;

            await assertGuardHeld(probe, outcomeCode);
            const consumedImage = normalizeMeasuredImage(
              await invokeImageCoordinator(
                imageReservations,
                imageConsumeReservationIntrinsic,
                input.imageReservation,
                outcomeCode,
              ),
              outcomeCode,
            );
            ensure(
              sameContent(
                consumedImage,
                claim.attempt.request.measuredImage,
                outcomeCode,
              ),
              outcomeCode,
            );
            await assertGuardHeld(probe, outcomeCode);
            const callbackReceipt = normalizeLaunchCallbackReceipt(
              await invokeSupervisor(
                supervisor.launchWriter,
                exactFrozenRecord({
                  contractVersion: LOGICAL_WRITER_LAUNCH_CONTRACT_VERSION,
                  attempt: claim.attempt,
                  authorityNow: claim.authorityNow,
                  consumedImage,
                  generation: claim.generation,
                  operation: claim.operation,
                  reservation: claim.reservation,
                  session: claim.session,
                }),
                outcomeCode,
              ),
              claim.attempt,
              outcomeCode,
            );
            await assertGuardHeld(probe, outcomeCode);

            if (callbackReceipt.evidence.status !== "started") {
              return finalizeWithReadback(
                claim,
                callbackReceipt.evidence,
                "finalizeWriterLaunchAttemptStopped",
                uncertaintyState,
              );
            }
            registerStartedWriter(claim, callbackReceipt);
            await assertGuardHeld(probe, outcomeCode);
            return finalizeWithReadback(
              claim,
              callbackReceipt.evidence,
              "finalizeWriterLaunchAttemptStarted",
              uncertaintyState,
            );
          },
        ],
        outcomeCode,
      );
    } catch (error) {
      if (
        isInternalError(error, optionCode) ||
        isInternalError(error, "logical_writer_handle_unavailable")
      ) {
        throw error;
      }
      if (dispatchDefinitelyBegan && dispatchOperation !== null) {
        await bestEffortMarkUncertain(dispatchOperation, uncertaintyState);
      }
      fail(outcomeCode);
    }
  }

  async function reconcileLaunchAttemptInternal(...reconcileArgs) {
    ensure(reconcileArgs.length === 1, optionCode);
    const input = normalizeReconcileInput(reconcileArgs[0], optionCode);
    const uncertaintyState = { attempted: false };
    try {
      return await invokeGuard(
        operationGuard,
        [
          input.launchAttemptId,
          async (probeValue) =>
            reconcileWithinGuard(
              input.launchAttemptId,
              normalizeProbe(probeValue, outcomeCode),
              uncertaintyState,
            ),
        ],
        outcomeCode,
      );
    } catch (error) {
      if (isInternalError(error, "logical_writer_handle_unavailable")) {
        throw error;
      }
      fail(outcomeCode);
    }
  }

  function resolveStoppedWriter(...resolverArgs) {
    ensure(resolverArgs.length === 1, optionCode);
    try {
      const input = exactDataObject(
        resolverArgs[0],
        RESOLVER_INPUT_KEYS,
        optionCode,
      );
      let attachment;
      let checkpoint;
      let request;
      try {
        attachment = assertSessionAttachment(input.attachment);
        checkpoint = assertCheckpointDescriptor(input.checkpoint);
        request = assertStorageMutationRequest(input.request);
      } catch {
        fail(optionCode);
      }
      ensure(
        request.operation === "checkpoint" &&
          attachment.sessionId === checkpoint.sessionId &&
          attachment.sessionId === request.sessionId &&
          attachment.backendId === checkpoint.backendId &&
          attachment.backendId === request.backendId &&
          attachment.storageId === checkpoint.storageId &&
          attachment.storageId === request.storageId &&
          attachment.leaseId === request.leaseId &&
          attachment.holderId === request.holderId &&
          attachment.fencingEpoch === request.fencingEpoch &&
          attachment.fencingEpoch === checkpoint.sourceFencingEpoch &&
          checkpoint.checkpointClass === "clean" &&
          request.target.kind === "checkpoint" &&
          request.target.checkpointId === checkpoint.checkpointId &&
          request.target.artifactId === checkpoint.artifactId,
        optionCode,
      );
      const record = mapGet(
        recordsByAttachmentId,
        attachment.attachmentId,
      );
      ensure(
        record !== undefined &&
          record.state === "ready" &&
          sameContent(attachment, record.attachment, optionCode) &&
          checkpoint.codexSessionId === record.codexSessionId &&
          checkpoint.codexThreadId === record.codexThreadId &&
          checkpoint.imageDigest === record.imageDigest,
        optionCode,
      );
      const capture = exactFrozenRecord({ attachment, checkpoint, request });
      ensure(
        record.authorizedCapture === null ||
          sameContent(record.authorizedCapture, capture, optionCode),
        optionCode,
      );
      const derivedStopOperationId = stopOperationId(
        assertOpaqueId(request.operationId, optionCode),
        record.launchAttemptId,
        optionCode,
      );
      ensure(
        record.authorizedStopOperationId === null ||
          record.authorizedStopOperationId === derivedStopOperationId,
        optionCode,
      );
      record.authorizedCapture = capture;
      record.authorizedStopOperationId = derivedStopOperationId;
      return exactFrozenRecord({
        canonicalLeaseAtRegistration: record.canonicalLease,
        processIncarnationId: record.processIncarnationId,
        stopOperationId: derivedStopOperationId,
        writer: record.writer,
        writerIncarnationId: record.writerIncarnationId,
      });
    } catch (error) {
      if (isInternalError(error, optionCode)) throw error;
      fail(optionCode);
    }
  }

  const runLaunch = function runLaunch(...runArgs) {
    return protectPromise(runLaunchInternal(...runArgs));
  };
  const reconcileLaunchAttempt = function reconcileLaunchAttempt(
    ...reconcileArgs
  ) {
    return protectPromise(reconcileLaunchAttemptInternal(...reconcileArgs));
  };
  objectFreeze(runLaunch);
  objectFreeze(reconcileLaunchAttempt);
  objectFreeze(resolveStoppedWriter);
  return exactFrozenRecord({
    reconcileLaunchAttempt,
    resolveStoppedWriter,
    runLaunch,
  });
}

objectFreeze(PostgresLogicalWriterLauncherError.prototype);
objectFreeze(PostgresLogicalWriterLauncherError);
