import { Hash, createHash, randomUUID } from "node:crypto";
import { types as utilTypes } from "node:util";

import {
  isPostgresDetachedRestoreImagePlanBinding,
  isPostgresDetachedRestoreImagePlanReservation,
} from "./postgres-detached-restore-image-plan-binding.mjs";
import {
  PostgresOperationGuard,
  isPostgresOperationGuard,
} from "./postgres-operation-guard.mjs";
import {
  RESTORE_ATTACHMENT_ACTIVATION_OPERATION_KIND,
  RESTORE_DESTINATION_GENERATION_OPERATION_KIND,
  SESSION_OPERATION_CONFLICT_CLASS,
  WRITER_FORCE_FENCE_OPERATION_KIND,
  WRITER_LAUNCH_ATTEMPT_OPERATION_KIND,
  WRITER_LAUNCH_PRE_DISPATCH_CANCELLATION_REASON,
  WRITER_LAUNCH_STOP_OPERATION_KIND,
  WRITER_RELEASE_OPERATION_KIND,
  assertCommittedWriterLaunchStopTransitionProof,
  assertSessionAuthoritySnapshot,
  assertWriterLaunchStopCaptureHandoffProof,
  createCheckpointCaptureOperationRequest,
  createWriterLaunchAttemptOperationRequest,
  createWriterLaunchStopOperationRequest,
} from "./postgres-session-authority.mjs";
import {
  assertPodmanWriterSupervisorStateRecord,
} from "./podman-writer-supervisor-state.mjs";
import {
  assertAtomicCrashCaptureRequest,
  assertAtomicCrashCaptureResult,
  assertCheckpointDescriptor,
  assertLeaseGrant,
  assertSessionAttachment,
  assertSessionManifest,
  assertStorageMutationRequest,
  assertStorageMutationResult,
} from "./session-storage-contracts.mjs";
import {
  STOPPED_WRITER_STOP_CONFIRMED,
  StoppedWriterCapabilityCoordinator,
} from "./stopped-writer-capability.mjs";

const arrayIncludesIntrinsic = Array.prototype.includes;
const arrayIsArray = Array.isArray;
const ArrayConstructor = Array;
const arrayPrototype = Array.prototype;
const BigIntConstructor = BigInt;
const createHashIntrinsic = createHash;
const randomUUIDIntrinsic = randomUUID;
const dateParseIntrinsic = Date.parse;
const dateToISOStringIntrinsic = Date.prototype.toISOString;
const DateConstructor = Date;
const functionToStringIntrinsic = Function.prototype.toString;
const hashDigestIntrinsic = Hash.prototype.digest;
const hashUpdateIntrinsic = Hash.prototype.update;
const JsonObject = JSON;
const jsonStringifyIntrinsic = JSON.stringify;
const mapDeleteIntrinsic = Map.prototype.delete;
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
const promisePrototype = Promise.prototype;
const promiseResolveIntrinsic = Promise.resolve;
const promiseSpeciesSymbol = Symbol.species;
const promiseThenIntrinsic = Promise.prototype.then;
const postgresOperationGuardRunExclusiveIntrinsic =
  PostgresOperationGuard.prototype.runExclusive;
const reflectApply = Reflect.apply;
const reflectOwnKeys = Reflect.ownKeys;
const regexpExecIntrinsic = RegExp.prototype.exec;
const stoppedAssertWriterLaunchAvailableIntrinsic =
  StoppedWriterCapabilityCoordinator.prototype.assertWriterLaunchAvailable;
const stoppedRegisterWriterIntrinsic =
  StoppedWriterCapabilityCoordinator.prototype.registerWriter;
const stoppedConsumeCapabilityIntrinsic =
  StoppedWriterCapabilityCoordinator.prototype.consumeCapability;
const stoppedRevokeWriterIntrinsic =
  StoppedWriterCapabilityCoordinator.prototype.revokeWriter;
const stoppedRetireWriterIntrinsic =
  StoppedWriterCapabilityCoordinator.prototype.retireWriter;
const stoppedStopAndIssueCapabilityIntrinsic =
  StoppedWriterCapabilityCoordinator.prototype.stopAndIssueCapability;
const TypeErrorConstructor = TypeError;
const weakMapDeleteIntrinsic = WeakMap.prototype.delete;
const weakMapGetIntrinsic = WeakMap.prototype.get;
const weakMapSetIntrinsic = WeakMap.prototype.set;
const WeakMapConstructor = WeakMap;
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
export const LOGICAL_WRITER_SUPERVISOR_CONTRACT_VERSION = 4;
export const LOGICAL_WRITER_LAUNCH_RECEIPT_VERSION = 2;
export const LOGICAL_WRITER_RECONCILE_RECEIPT_VERSION = 2;
const MAX_DATA_TREE_DEPTH = 24;
const MAX_DATA_TREE_NODES = 16_384;
const NATIVE_FUNCTION_SOURCE_PATTERN =
  /\{\s*\[\s*native\s+code\s*\]\s*\}$/u;
const OPAQUE_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u;
const REVISION_PATTERN = /^(?:0|[1-9][0-9]{0,18})$/u;
const SHA256_PATTERN = /^[0-9a-f]{64}$/u;
const STATE_OWNER_ID_PATTERN = /^state-owner:[0-9a-f]{64}$/u;
const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u;

const OPTION_KEYS = objectFreeze([
  "authority",
  "imagePlanBinding",
  "operationGuard",
  "stoppedWriterCoordinator",
  "supervisor",
]);
const PREPARE_INPUT_KEYS = objectFreeze([
  "expectedSession",
  "imageReservation",
  "launchAttemptId",
]);
const SUPERVISOR_KEYS = objectFreeze([
  "contractVersion",
  "launchWriter",
  "reconcileWriterLaunch",
  "stateOwnerId",
  "supervisorId",
]);
const AUTHORITY_METHODS = objectFreeze([
  "cancelPreparedOperation",
  "claimWriterLaunchAttemptDispatch",
  "claimWriterLaunchStopDispatch",
  "finalizeWriterLaunchAttemptStarted",
  "finalizeWriterLaunchAttemptStopped",
  "finalizeWriterLaunchAttemptStoppedAndAuthorizeSupervisorStateGc",
  "finalizeWriterLaunchStopped",
  "finalizeWriterLaunchStoppedAndAuthorizeSupervisorStateGc",
  "finalizeWriterLaunchStoppedAndReserveCheckpointCapture",
  "finalizeWriterLaunchStoppedAndReserveCheckpointCaptureAndAuthorizeSupervisorStateGc",
  "markOperationUncertain",
  "readSession",
  "readWriterSupervisorStateGcAuthorization",
  "readWriterLaunchAttempt",
  "reconcileWriterLaunchStopOperation",
  "reserveOperation",
]);
const RUN_INPUT_KEYS = objectFreeze([
  "generation",
  "imageReservation",
  "launchAttemptId",
]);
const PREPARED_RUN_INPUT_KEYS = objectFreeze([
  "imageReservation",
  "launchAttemptId",
]);
const RECONCILE_INPUT_KEYS = objectFreeze(["launchAttemptId"]);
const RESOLVER_INPUT_KEYS = objectFreeze([
  "attachment",
  "checkpoint",
  "request",
]);
const STOP_OPERATION_ID_INPUT_KEYS = objectFreeze([
  "attachment",
  "checkpoint",
  "launchAttemptId",
  "request",
]);
const ATOMIC_CRASH_STOP_OPERATION_ID_INPUT_KEYS = objectFreeze([
  "launchAttemptId",
  "request",
]);
const ATOMIC_CRASH_CAPTURE_ADMISSION_KEYS = objectFreeze([
  "captureAuthority",
  "request",
]);
const ATOMIC_CRASH_CAPTURE_RETIREMENT_KEYS = objectFreeze([
  "captureAuthority",
  "request",
  "result",
]);
const STOP_FINALIZATION_MAX_ATTEMPTS = 3;
const STOP_RESERVATION_MAX_ATTEMPTS = 3;
const WRITER_LAUNCH_STOP_CLAIM_CONTRACT_VERSION = 2;
const WRITER_LAUNCH_STOP_CAPTURE_HANDOFF_CONTRACT_VERSION = 3;
const CLEAN_CAPTURE_ROUTE = "clean-capture-v1";
const ATOMIC_CRASH_CAPTURE_ROUTE = "atomic-crash-capture-v1";
const STOP_RESOLUTION_KEYS = objectFreeze([
  "canonicalLeaseAtRegistration",
  "processIncarnationId",
  "stopOperationId",
  "writer",
  "writerIncarnationId",
]);
const PREPARED_CAPTURE_RETIREMENT_KEYS = objectFreeze([
  "resolution",
  "result",
]);
const PREPARED_CAPTURE_RESULT_KEYS = objectFreeze([
  "checkpoint",
  "mutation",
]);
const PREPARED_CAPTURE_HANDOFF_RECEIPT_KEYS = objectFreeze([
  "capture",
  "evidence",
  "resolution",
  "session",
  "status",
  "stop",
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
const STOP_RESERVE_RECEIPT_KEYS = objectFreeze([
  "acquired",
  "operation",
  "reservation",
  "session",
  "status",
]);
const STOP_RECONCILE_RECEIPT_KEYS = objectFreeze([
  "claimTokenMatched",
  "operation",
  "reservation",
  "session",
  "status",
]);
const STOP_RECONCILE_ABSENT_RECEIPT_KEYS = objectFreeze([
  "claimTokenMatched",
  "expectedSessionMatched",
  "operation",
  "reservation",
  "session",
  "status",
]);
const STOP_CLAIM_RECEIPT_KEYS = objectFreeze([
  "claimTokenMatched",
  "dispatchGranted",
  "launch",
  "operation",
  "reservation",
  "session",
  "status",
  "stop",
]);
const STOP_FINALIZE_RECEIPT_KEYS = objectFreeze([
  "finalized",
  "launch",
  "operation",
  "reservation",
  "session",
  "status",
  "stop",
]);
const STOP_FINALIZE_GC_RECEIPT_KEYS = objectFreeze([
  ...STOP_FINALIZE_RECEIPT_KEYS,
  "supervisorStateGcAuthorization",
]);
const STOP_CAPTURE_HANDOFF_RECEIPT_KEYS = objectFreeze([
  "capture",
  "session",
  "status",
  "stop",
]);
const STOP_CAPTURE_HANDOFF_GC_RECEIPT_KEYS = objectFreeze([
  ...STOP_CAPTURE_HANDOFF_RECEIPT_KEYS,
  "supervisorStateGcAuthorization",
]);
const STOP_CAPTURE_HANDOFF_RECONCILE_RECEIPT_KEYS = objectFreeze([
  "capture",
  "claimTokenMatched",
  "session",
  "status",
  "stop",
]);
const STOP_CAPTURE_HANDOFF_RELATION_KEYS = objectFreeze([
  "operation",
  "reservation",
]);
const STOP_CAPTURE_HANDOFF_STOP_KEYS = objectFreeze([
  "finalized",
  "operation",
  "record",
  "reservation",
]);
const STOP_RECORD_KEYS = objectFreeze([
  "contractVersion",
  "launchAttemptId",
  "request",
  "result",
  "state",
  "stopOperationId",
]);
const STOP_REQUEST_V2_KEYS = objectFreeze([
  "contractVersion",
  "dispatchClaimSha256",
  "launch",
]);
const STOP_REQUEST_V3_KEYS = objectFreeze([
  "captureIntent",
  "contractVersion",
  "dispatchClaimSha256",
  "launch",
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
const FINALIZE_GC_RECEIPT_KEYS = objectFreeze([
  ...FINALIZE_RECEIPT_KEYS,
  "supervisorStateGcAuthorization",
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
  "terminalRecord",
]);
const RECONCILE_CALLBACK_RECEIPT_KEYS = objectFreeze([
  "evidence",
  "receiptVersion",
  "terminalRecord",
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
const PHYSICAL_STOP_RESULT_KEYS = objectFreeze([
  "confirmation",
  "contractVersion",
  "terminalRecord",
]);
const SUPERVISOR_STATE_GC_AUTHORIZATION_KEYS = objectFreeze([
  "authorizationSha256",
  "authorizedAt",
  "contractVersion",
  "launchAttemptId",
  "sessionId",
  "stateOwnerId",
  "terminalKind",
  "terminalOperationId",
  "terminalRecord",
  "terminalRecordSha256",
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
  logical_writer_launch_admission_unavailable:
    "Logical writer launch admission is temporarily unavailable",
  logical_writer_launch_outcome_uncertain:
    "Logical writer launch outcome is uncertain",
});
const INTERNAL_ERRORS = new WeakSetConstructor();
const ATOMIC_CRASH_CAPTURE_FACETS = new WeakMapConstructor();
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

function mapDelete(map, key) {
  return callIntrinsic(mapDeleteIntrinsic, map, [key]);
}

function mapHas(map, key) {
  return callIntrinsic(mapHasIntrinsic, map, [key]);
}

function mapSet(map, key, value) {
  callIntrinsic(mapSetIntrinsic, map, [key, value]);
}

function weakMapGet(map, key) {
  return callIntrinsic(weakMapGetIntrinsic, map, [key]);
}

function weakMapSet(map, key, value) {
  callIntrinsic(weakMapSetIntrinsic, map, [key, value]);
}

function weakMapDelete(map, key) {
  return callIntrinsic(weakMapDeleteIntrinsic, map, [key]);
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
      value: code === "logical_writer_launch_admission_unavailable",
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

function canonicalJsonDataTree(value) {
  if (value === null || typeof value !== "object") return value;
  if (arrayIsArray(value)) {
    const result = new ArrayConstructor(value.length);
    for (let index = 0; index < value.length; index += 1) {
      objectDefineProperty(result, String(index), {
        enumerable: true,
        value: canonicalJsonDataTree(value[index]),
      });
    }
    return objectFreeze(result);
  }
  const keys = reflectOwnKeys(value);
  const sorted = new ArrayConstructor(keys.length);
  for (let index = 0; index < keys.length; index += 1) {
    sorted[index] = keys[index];
  }
  for (let outer = 1; outer < sorted.length; outer += 1) {
    const key = sorted[outer];
    let inner = outer - 1;
    while (inner >= 0 && sorted[inner] > key) {
      sorted[inner + 1] = sorted[inner];
      inner -= 1;
    }
    sorted[inner + 1] = key;
  }
  const result = objectCreate(null);
  for (let index = 0; index < sorted.length; index += 1) {
    const key = sorted[index];
    objectDefineProperty(result, key, {
      enumerable: true,
      value: canonicalJsonDataTree(value[key]),
    });
  }
  return objectFreeze(result);
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

function assertStateOwnerId(value, code) {
  ensure(
    typeof value === "string" && regexpTest(STATE_OWNER_ID_PATTERN, value),
    code,
  );
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

function operationGuardBinding(value, code) {
  if (isPostgresOperationGuard(value)) {
    ensure(objectIsFrozen(value), code);
    return exactFrozenRecord({
      kind: "postgres-operation-guard",
      methods: exactFrozenRecord({
        runExclusive: postgresOperationGuardRunExclusiveIntrinsic,
      }),
      receiver: value,
    });
  }
  const legacy = collaboratorBinding(value, ["runExclusive"], code);
  return exactFrozenRecord({
    kind: "legacy-async",
    methods: legacy.methods,
    receiver: legacy.receiver,
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

function adoptPostgresOperationGuardPromise(value, code) {
  ensure(
    isPromiseValue(value) &&
      !isProxyValue(value) &&
      !isGeneratorObjectValue(value),
    code,
  );
  let sourceConstructorDescriptor;
  let sourceConstructorKeys;
  let sourceConstructorPrototype;
  let sourceSpeciesDescriptor;
  let sourcePrototype;
  try {
    sourcePrototype = objectGetPrototypeOf(value);
    sourceConstructorDescriptor = objectGetOwnPropertyDescriptor(
      value,
      "constructor",
    );
  } catch {
    fail(code);
  }
  const sourceConstructor = sourceConstructorDescriptor?.value;
  ensure(
    sourcePrototype === promisePrototype &&
      sourceConstructorDescriptor?.configurable === false &&
      sourceConstructorDescriptor.enumerable === false &&
      objectHasOwn(sourceConstructorDescriptor, "value") &&
      sourceConstructorDescriptor.writable === false &&
      sourceConstructor !== null &&
      typeof sourceConstructor === "object" &&
      !isProxyValue(sourceConstructor) &&
      objectIsFrozen(sourceConstructor),
    code,
  );
  try {
    sourceConstructorPrototype = objectGetPrototypeOf(sourceConstructor);
    sourceConstructorKeys = reflectOwnKeys(sourceConstructor);
    sourceSpeciesDescriptor = objectGetOwnPropertyDescriptor(
      sourceConstructor,
      promiseSpeciesSymbol,
    );
  } catch {
    fail(code);
  }
  ensure(
    sourceConstructorPrototype === null &&
      sourceConstructorKeys.length === 1 &&
      sourceConstructorKeys[0] === promiseSpeciesSymbol &&
      sourceSpeciesDescriptor?.configurable === false &&
      sourceSpeciesDescriptor.enumerable === false &&
      objectHasOwn(sourceSpeciesDescriptor, "value") &&
      sourceSpeciesDescriptor.value === PromiseConstructor &&
      sourceSpeciesDescriptor.writable === false,
    code,
  );

  let adopted;
  let adoptedConstructorDescriptor;
  let adoptedPrototype;
  try {
    adopted = callIntrinsic(promiseThenIntrinsic, value, [
      undefined,
      undefined,
    ]);
    adoptedPrototype = objectGetPrototypeOf(adopted);
    adoptedConstructorDescriptor = objectGetOwnPropertyDescriptor(
      adopted,
      "constructor",
    );
  } catch {
    fail(code);
  }
  ensure(
    isPromiseValue(adopted) &&
      !isProxyValue(adopted) &&
      !isGeneratorObjectValue(adopted) &&
      adoptedPrototype === promisePrototype &&
      adoptedConstructorDescriptor === undefined,
    code,
  );
  try {
    objectDefineProperty(adopted, "constructor", {
      configurable: false,
      enumerable: false,
      value: PromiseConstructor,
      writable: false,
    });
    adoptedConstructorDescriptor = objectGetOwnPropertyDescriptor(
      adopted,
      "constructor",
    );
  } catch {
    fail(code);
  }
  ensure(
    adoptedConstructorDescriptor?.configurable === false &&
      adoptedConstructorDescriptor.enumerable === false &&
      objectHasOwn(adoptedConstructorDescriptor, "value") &&
      adoptedConstructorDescriptor.value === PromiseConstructor &&
      adoptedConstructorDescriptor.writable === false,
    code,
  );
  return adopted;
}

function bridgePostgresOperationGuardProbe(value, code) {
  const probe = exactDataObject(value, PROBE_KEYS, code);
  ensure(objectIsFrozen(value), code);
  const assertHeldIntrinsic = assertCallback(probe.assertHeld, code);
  const assertHeld = objectFreeze((...args) => {
    let pending;
    try {
      pending = callIntrinsic(assertHeldIntrinsic, undefined, args);
    } catch {
      fail(code);
    }
    return adoptPostgresOperationGuardPromise(pending, code);
  });
  return exactFrozenRecord({ assertHeld });
}

function bridgePostgresOperationGuardCallback(callback, code) {
  const guardedCallback = assertCallback(callback, code);
  return objectFreeze((probe, complete) =>
    callIntrinsic(guardedCallback, undefined, [
      bridgePostgresOperationGuardProbe(probe, code),
      complete,
    ]));
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
  const callArgs =
    binding.kind === "postgres-operation-guard"
      ? [
          args[0],
          bridgePostgresOperationGuardCallback(args[1], code),
        ]
      : args;
  try {
    pending = callIntrinsic(
      binding.methods.runExclusive,
      binding.receiver,
      callArgs,
    );
  } catch (error) {
    if (isInternalError(error)) throw error;
    fail(code);
  }
  if (binding.kind === "postgres-operation-guard") {
    pending = adoptPostgresOperationGuardPromise(pending, code);
  } else {
    ensure(binding.kind === "legacy-async" && isSafeNativePromise(pending), code);
  }
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

async function invokeStoppedCoordinator(
  coordinator,
  method,
  options,
  code,
) {
  let pending;
  try {
    pending = callIntrinsic(method, coordinator, [options]);
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

function invokeStoppedCoordinatorSync(coordinator, method, options, code) {
  try {
    return callIntrinsic(method, coordinator, [options]);
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

function normalizeSession(value, code, allowCleanDetached = false) {
  const session = exactDataObject(value, SESSION_KEYS, code);
  const document = exactDataObject(
    session.document,
    SESSION_DOCUMENT_KEYS,
    code,
  );
  if (allowCleanDetached && document.lifecycle === "DETACHED") {
    let canonicalSession;
    try {
      canonicalSession = assertSessionAuthoritySnapshot(value);
    } catch {
      fail(code);
    }
    const canonicalDocument = canonicalSession.document;
    const lastOperation = exactDataObject(
      canonicalDocument.lastOperation,
      LAST_OPERATION_KEYS,
      code,
    );
    ensure(
      canonicalDocument.documentVersion === 3 &&
        canonicalDocument.recovery === null &&
        canonicalDocument.activeOperation === null &&
        canonicalDocument.launch === null &&
        canonicalDocument.attachment === null &&
        canonicalDocument.lease === null &&
        lastOperation.state === "committed" &&
        arrayIncludes(
          [
            WRITER_RELEASE_OPERATION_KIND,
            WRITER_FORCE_FENCE_OPERATION_KIND,
          ],
          lastOperation.kind,
        ),
      code,
    );
    return snapshotData(canonicalSession, code);
  }
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

function validateSessionPointer(
  session,
  operation,
  reservation,
  resultSha256,
  code,
) {
  const document = exactDataObject(
    session.document,
    SESSION_DOCUMENT_KEYS,
    code,
  );
  if (operation.state === "committed") {
    if (document.lastOperation?.operationId !== operation.operationId) {
      return false;
    }
    const last = exactDataObject(
      document.lastOperation,
      LAST_OPERATION_KEYS,
      code,
    );
    ensure(
      last.conflictClass === operation.conflictClass &&
        last.expectedSessionRevision === operation.expectedSession.revision &&
        last.operationId === operation.operationId &&
        last.kind === operation.kind &&
        last.state === operation.state &&
        last.operationRevision === operation.revision &&
        last.reservationId === reservation.reservationId &&
        last.requestSha256 === operation.requestSha256 &&
        last.resultSha256 === resultSha256,
      code,
    );
    return document.activeOperation === null;
  }
  const active = exactDataObject(
    document.activeOperation,
    ACTIVE_OPERATION_KEYS,
    code,
  );
  ensure(
    active.conflictClass === operation.conflictClass &&
      active.expectedSessionRevision === operation.expectedSession.revision &&
      active.operationId === operation.operationId &&
      active.kind === operation.kind &&
      active.state === operation.state &&
      active.operationRevision === operation.revision &&
      active.reservationId === reservation.reservationId &&
      active.requestSha256 === operation.requestSha256,
    code,
  );
  return false;
}

function canonicalTimestampMilliseconds(value, code) {
  ensure(typeof value === "string", code);
  const milliseconds = reflectApply(dateParseIntrinsic, DateConstructor, [
    value,
  ]);
  ensure(numberIsFinite(milliseconds), code);
  const normalized = new DateConstructor(milliseconds);
  ensure(
    reflectApply(dateToISOStringIntrinsic, normalized, []) === value,
    code,
  );
  return milliseconds;
}

function validateActiveSessionRelation(
  session,
  operation,
  reservation,
  code,
) {
  ensure(operation.state !== "committed", code);
  const expectedSession = operation.expectedSession;
  const expectedDocument = exactDataObject(
    expectedSession.document,
    SESSION_DOCUMENT_KEYS,
    code,
  );
  const currentDocument = exactDataObject(
    session.document,
    SESSION_DOCUMENT_KEYS,
    code,
  );
  // Protect session content stability rather than object identity. An active
  // transition may only upgrade the document version, replace its active
  // pointer, advance the revision, and move the operation-owned timestamp.
  const stableDocument = exactFrozenRecord({
    activeOperation: currentDocument.activeOperation,
    attachment: expectedDocument.attachment,
    backendCapabilities: expectedDocument.backendCapabilities,
    documentVersion: 3,
    lastOperation: expectedDocument.lastOperation,
    launch: expectedDocument.launch,
    lease: expectedDocument.lease,
    lifecycle: expectedDocument.lifecycle,
    manifest: expectedDocument.manifest,
    recovery: expectedDocument.recovery,
    storageRef: expectedDocument.storageRef,
    writerEpoch: expectedDocument.writerEpoch,
  });
  ensure(
    session.createdAt === expectedSession.createdAt &&
      session.updatedAt === operation.updatedAt &&
      BigIntConstructor(session.revision) ===
        BigIntConstructor(expectedSession.revision) +
          BigIntConstructor(operation.revision) +
          1n &&
      reservation.createdAt === operation.createdAt &&
      reservation.updatedAt === operation.updatedAt &&
      sameContent(currentDocument, stableDocument, code),
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
      assertOpaqueId(generation.checkpointId, code) ===
        generation.checkpointId &&
      assertOpaqueId(generation.generationId, code) ===
        generation.generationId &&
      assertOpaqueId(generation.operationId, code) ===
        generation.operationId &&
      assertSessionId(generation.sessionId, code) === generation.sessionId &&
      typeof generation.claimedAt === "string" &&
      typeof generation.committedAt === "string" &&
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

function normalizeGenerationSnapshotForReference(value, reference, code) {
  const generation = snapshotData(value, code);
  exactDataObject(generation, GENERATION_SNAPSHOT_KEYS, code);
  ensure(
    generation.binding !== null &&
      typeof generation.binding === "object" &&
      generation.document !== null &&
      typeof generation.document === "object" &&
      generation.checkpointId === reference.checkpointId &&
      generation.claimedAt === reference.claimedAt &&
      generation.committedAt === reference.committedAt &&
      generation.generationId === reference.generationId &&
      generation.operationId === reference.operationId &&
      generation.sessionId === reference.sessionId &&
      generation.state === "committed" &&
      canonicalJsonProjectionSha256(generation.binding, code) ===
        reference.bindingSha256 &&
      canonicalJsonProjectionSha256(generation.document, code) ===
        reference.documentSha256,
    code,
  );
  return generation;
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
  return exactFrozenRecord({
    contractVersion: LOGICAL_WRITER_LAUNCH_CONTRACT_VERSION,
    launchAttemptId: attempt.launchAttemptId,
    processIncarnationId: evidence.processIncarnationId,
    proofId: evidence.proofId,
    status: evidence.status,
    supervisorId: attempt.request.supervisor.supervisorId,
    writerIncarnationId: evidence.writerIncarnationId,
  });
}

function normalizeOperationResult(value, attempt, code) {
  if (value === null) {
    return exactFrozenRecord({
      evidence: null,
      result: null,
      resultSha256: null,
      status: null,
    });
  }
  const outcomeDescriptor = objectGetOwnPropertyDescriptor(value, "outcome");
  ensure(outcomeDescriptor !== undefined && objectHasOwn(outcomeDescriptor, "value"), code);
  if (outcomeDescriptor.value === "cancelled-before-dispatch") {
    const result = exactDataObject(value, CANCELLATION_RESULT_KEYS, code);
    ensure(
      result.resultVersion === 1 &&
        typeof result.reason === "string" &&
        result.reason.length <= 64 &&
        regexpTest(OPAQUE_ID_PATTERN, result.reason),
      code,
    );
    const canonicalResult = exactFrozenRecord({
      resultVersion: 1,
      outcome: "cancelled-before-dispatch",
      reason: result.reason,
    });
    return exactFrozenRecord({
      evidence: null,
      result: canonicalResult,
      resultSha256: canonicalJsonSha256(canonicalResult, code),
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
  const canonicalResult = exactFrozenRecord({
    evidence,
    outcome: outcomes[evidence.status],
    resultVersion: 1,
  });
  return exactFrozenRecord({
    evidence,
    result: canonicalResult,
    resultSha256: canonicalJsonSha256(canonicalResult, code),
    status: evidence.status,
  });
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
    normalized.state !== "committed" ||
      (terminal.status === "cancelled-before-dispatch"
        ? revision === "1"
        : terminal.status !== null &&
          (revision === "2" || revision === "3")),
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
      launch.startedAt === operation.updatedAt &&
      launch.supervisorId === terminal.evidence.supervisorId &&
      launch.supervisorProofId === terminal.evidence.proofId &&
      launch.writerIncarnationId === terminal.evidence.writerIncarnationId &&
      launch.attachmentSha256 ===
        canonicalJsonProjectionSha256(attempt.request.attachment, code) &&
      launch.launchResultSha256 === terminal.resultSha256 &&
      launch.leaseSha256 ===
        canonicalJsonProjectionSha256(attempt.request.lease, code) &&
      launch.measuredImageSha256 ===
        canonicalJsonProjectionSha256(attempt.request.measuredImage, code) &&
      sameContent(launch.generation, attempt.request.generation, code),
    code,
  );
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
  const terminalAnchorMatches = validateSessionPointer(
    session,
    operation,
    reservation,
    normalizedOperation.terminal.resultSha256,
    code,
  );
  if (operation.state !== "committed") {
    validateActiveSessionRelation(session, operation, reservation, code);
  }
  return {
    normalizedOperation,
    operation,
    receipt,
    reservation,
    session,
    terminalAnchorMatches,
  };
}

function normalizeReserveReceipt(
  value,
  launchAttemptId,
  typedRequest,
  expectedSession,
  code,
) {
  const common = normalizeReceiptCommon(
    value,
    RESERVE_RECEIPT_KEYS,
    launchAttemptId,
    typedRequest,
    code,
  );
  ensure(
    typeof common.receipt.acquired === "boolean" &&
      sameContent(
        common.operation.expectedSession,
        expectedSession,
        code,
      ),
    code,
  );
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
  expectedSession,
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
  ensure(
    typeof common.receipt.dispatchGranted === "boolean" &&
      sameContent(
        common.operation.expectedSession,
        expectedSession,
        code,
      ),
    code,
  );
  const attempt = normalizeAttempt(
    common.receipt.attempt,
    common.operation,
    common.normalizedOperation.request,
    code,
  );
  let generation = null;
  if (common.receipt.generation !== null) {
    generation = normalizeGenerationSnapshotForReference(
      common.receipt.generation,
      common.normalizedOperation.request.generation,
      code,
    );
    if (inputGeneration !== null) {
      ensure(sameContent(generation, inputGeneration, code), code);
    }
  }
  if (common.receipt.dispatchGranted) {
    const authorityNow = canonicalTimestampMilliseconds(
      common.receipt.authorityNow,
      code,
    );
    const operationUpdatedAt = canonicalTimestampMilliseconds(
      common.operation.updatedAt,
      code,
    );
    const leaseExpiresAt = canonicalTimestampMilliseconds(
      common.normalizedOperation.request.lease.expiresAt,
      code,
    );
    ensure(
      objectHasOwn(common.receipt, "authorityNow") &&
        common.operation.state === "starting" &&
        generation !== null &&
        authorityNow >= operationUpdatedAt &&
        authorityNow < leaseExpiresAt,
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
      common.operation.state === "committed" &&
      (!common.receipt.finalized || common.terminalAnchorMatches),
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

function normalizeFinalizeGcReceipt(
  value,
  launchAttemptId,
  expectedRequest,
  expectedEvidence,
  terminalRecord,
  stateOwnerId,
  code,
) {
  const receipt = exactDataObject(value, FINALIZE_GC_RECEIPT_KEYS, code);
  const normalized = normalizeFinalizeReceipt(
    exactFrozenRecord({
      attempt: receipt.attempt,
      finalized: receipt.finalized,
      launch: receipt.launch,
      operation: receipt.operation,
      reservation: receipt.reservation,
      session: receipt.session,
      status: receipt.status,
    }),
    launchAttemptId,
    expectedRequest,
    expectedEvidence,
    code,
  );
  normalizeSupervisorStateGcAuthorization(
    receipt.supervisorStateGcAuthorization,
    {
      launchAttemptId,
      sessionId: normalized.operation.sessionId,
      stateOwnerId,
      terminalKind: WRITER_LAUNCH_ATTEMPT_OPERATION_KIND,
      terminalOperationId: normalized.operation.operationId,
      terminalRecord,
    },
    code,
  );
  return normalized;
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
      (!common.receipt.cancelled || common.terminalAnchorMatches) &&
      common.normalizedOperation.terminal.status ===
        "cancelled-before-dispatch",
    code,
  );
}

function normalizeStopPhaseOperation(value, baseInput, states, code) {
  const normalized = exactDataObject(value, OPERATION_KEYS, code);
  const expectedSession = normalizeSession(normalized.expectedSession, code);
  const operation = snapshotData(value, code);
  ensure(
    operation.conflictClass === SESSION_OPERATION_CONFLICT_CLASS &&
      operation.kind === WRITER_LAUNCH_STOP_OPERATION_KIND &&
      operation.operationId === baseInput.operationId &&
      operation.sessionId === baseInput.expectedSession.sessionId &&
      sameContent(expectedSession, baseInput.expectedSession, code) &&
      sameContent(operation.request, baseInput.request, code) &&
      assertSha256(operation.requestSha256, code) ===
        operation.requestSha256 &&
      arrayIncludes(states, operation.state) &&
      operation.result === null &&
      operation.retiredAt === null,
    code,
  );
  const expectedRevision = {
    prepared: "0",
    starting: "1",
    uncertain: "2",
  }[operation.state];
  ensure(operation.revision === expectedRevision, code);
  return operation;
}

function normalizeStopRecord(value, operation, baseInput, code) {
  const stop = exactDataObject(value, STOP_RECORD_KEYS, code);
  ensure(
    stop.contractVersion === baseInput.request.contractVersion &&
      stop.launchAttemptId === baseInput.request.launch.launchAttemptId &&
      stop.stopOperationId === baseInput.operationId &&
      stop.state === operation.state &&
      sameContent(stop.request, baseInput.request, code) &&
      sameContent(stop.result, operation.result, code),
    code,
  );
  return snapshotData(value, code);
}

function normalizeStopReserveReceipt(value, baseInput, code) {
  const receipt = exactDataObject(value, STOP_RESERVE_RECEIPT_KEYS, code);
  ensure(typeof receipt.acquired === "boolean", code);
  const operation = normalizeStopPhaseOperation(
    receipt.operation,
    baseInput,
    ["prepared", "starting"],
    code,
  );
  const reservation = normalizeReservation(
    receipt.reservation,
    operation,
    code,
  );
  const session = normalizeSession(receipt.session, code);
  ensure(
    receipt.status === operation.state &&
      session.sessionId === operation.sessionId &&
      (!receipt.acquired || operation.state === "prepared") &&
      sameContent(session.document.launch, baseInput.request.launch, code),
    code,
  );
  validateSessionPointer(session, operation, reservation, null, code);
  validateActiveSessionRelation(session, operation, reservation, code);
  return exactFrozenRecord({
    acquired: receipt.acquired,
    operation,
    reservation,
    session,
    status: operation.state,
  });
}

function normalizeStopReconcileReceipt(value, baseInput, code) {
  const receipt = exactDataObjectVariant(
    value,
    [STOP_RECONCILE_RECEIPT_KEYS, STOP_RECONCILE_ABSENT_RECEIPT_KEYS],
    code,
  );
  const session = normalizeSession(receipt.session, code);
  if (receipt.operation === null || receipt.reservation === null) {
    const expectedSessionMatched = receipt.expectedSessionMatched;
    const exactExpectedSession = sameContent(
      session,
      baseInput.expectedSession,
      code,
    );
    ensure(
      receipt.operation === null &&
        receipt.reservation === null &&
        receipt.status === "absent" &&
        receipt.claimTokenMatched === false &&
        typeof expectedSessionMatched === "boolean" &&
        expectedSessionMatched === exactExpectedSession &&
        (expectedSessionMatched ||
          (session.sessionId === baseInput.expectedSession.sessionId &&
            session.createdAt === baseInput.expectedSession.createdAt &&
            sameContent(
              session.document.manifest,
              baseInput.expectedSession.document.manifest,
              code,
            ) &&
            sameContent(
              session.document.storageRef,
              baseInput.expectedSession.document.storageRef,
              code,
            ) &&
            sameContent(
              session.document.backendCapabilities,
              baseInput.expectedSession.document.backendCapabilities,
              code,
            ) &&
            BigIntConstructor(session.revision) >
              BigIntConstructor(baseInput.expectedSession.revision))),
      code,
    );
    return exactFrozenRecord({
      claimTokenMatched: false,
      expectedSessionMatched,
      operation: null,
      reservation: null,
      session,
      status: "absent",
    });
  }
  ensure(!objectHasOwn(receipt, "expectedSessionMatched"), code);
  ensure(typeof receipt.claimTokenMatched === "boolean", code);
  const operation = normalizeStopPhaseOperation(
    receipt.operation,
    baseInput,
    ["prepared", "starting", "uncertain"],
    code,
  );
  const reservation = normalizeReservation(
    receipt.reservation,
    operation,
    code,
  );
  ensure(
    receipt.status === operation.state &&
      session.sessionId === operation.sessionId &&
      sameContent(session.document.launch, baseInput.request.launch, code),
    code,
  );
  validateSessionPointer(session, operation, reservation, null, code);
  validateActiveSessionRelation(session, operation, reservation, code);
  return exactFrozenRecord({
    claimTokenMatched: receipt.claimTokenMatched,
    operation,
    reservation,
    session,
    status: operation.state,
  });
}

function normalizeStopClaimReceipt(value, baseInput, code) {
  const receipt = exactDataObject(value, STOP_CLAIM_RECEIPT_KEYS, code);
  ensure(
    typeof receipt.claimTokenMatched === "boolean" &&
      typeof receipt.dispatchGranted === "boolean",
    code,
  );
  const operation = normalizeStopPhaseOperation(
    receipt.operation,
    baseInput,
    ["prepared", "starting", "uncertain"],
    code,
  );
  const reservation = normalizeReservation(
    receipt.reservation,
    operation,
    code,
  );
  const session = normalizeSession(receipt.session, code);
  const launch = snapshotData(receipt.launch, code);
  ensure(
    receipt.status === operation.state &&
      session.sessionId === operation.sessionId &&
      sameContent(launch, baseInput.request.launch, code) &&
      sameContent(session.document.launch, launch, code) &&
      (!receipt.dispatchGranted || receipt.claimTokenMatched) &&
      (receipt.dispatchGranted
        ? operation.state === "starting"
        : operation.state === "prepared" ||
          operation.state === "starting" ||
          operation.state === "uncertain"),
    code,
  );
  validateSessionPointer(session, operation, reservation, null, code);
  validateActiveSessionRelation(session, operation, reservation, code);
  const stop = normalizeStopRecord(
    receipt.stop,
    operation,
    baseInput,
    code,
  );
  return exactFrozenRecord({
    claimTokenMatched: receipt.claimTokenMatched,
    dispatchGranted: receipt.dispatchGranted,
    launch,
    operation,
    reservation,
    session,
    status: operation.state,
    stop,
  });
}

function normalizeStopFinalizationReceipt(
  value,
  baseInput,
  expectedEvidence,
  code,
) {
  const receipt = exactDataObject(
    value,
    STOP_FINALIZE_RECEIPT_KEYS,
    code,
  );
  ensure(
    typeof receipt.finalized === "boolean" &&
      receipt.launch === null &&
      receipt.status === "committed",
    code,
  );
  let transition;
  try {
    transition = assertCommittedWriterLaunchStopTransitionProof({
      after: receipt.session,
      before: baseInput.expectedSession,
      operation: receipt.operation,
      reservation: receipt.reservation,
    });
  } catch {
    fail(code);
  }
  const operation = transition.operation;
  ensure(
    operation.operationId === baseInput.operationId &&
      sameContent(operation.expectedSession, baseInput.expectedSession, code) &&
      sameContent(operation.request, baseInput.request, code),
    code,
  );
  const result = exactDataObject(
    operation.result,
    TERMINAL_RESULT_KEYS,
    code,
  );
  ensure(
    result.resultVersion === 1 &&
      result.outcome === "writer-launch-stopped" &&
      sameContent(result.evidence, expectedEvidence, code),
    code,
  );
  const stop = normalizeStopRecord(
    receipt.stop,
    operation,
    baseInput,
    code,
  );
  return exactFrozenRecord({
    finalized: receipt.finalized,
    launch: null,
    operation,
    reservation: transition.reservation,
    session: transition.after,
    status: "committed",
    stop,
  });
}

function normalizeStopFinalizationGcReceipt(
  value,
  baseInput,
  expectedEvidence,
  terminalRecord,
  stateOwnerId,
  code,
) {
  const receipt = exactDataObject(
    value,
    STOP_FINALIZE_GC_RECEIPT_KEYS,
    code,
  );
  const normalized = normalizeStopFinalizationReceipt(
    exactFrozenRecord({
      finalized: receipt.finalized,
      launch: receipt.launch,
      operation: receipt.operation,
      reservation: receipt.reservation,
      session: receipt.session,
      status: receipt.status,
      stop: receipt.stop,
    }),
    baseInput,
    expectedEvidence,
    code,
  );
  normalizeSupervisorStateGcAuthorization(
    receipt.supervisorStateGcAuthorization,
    {
      launchAttemptId: baseInput.request.launch.launchAttemptId,
      sessionId: normalized.operation.sessionId,
      stateOwnerId,
      terminalKind: WRITER_LAUNCH_STOP_OPERATION_KIND,
      terminalOperationId: normalized.operation.operationId,
      terminalRecord,
    },
    code,
  );
  return normalized;
}

function normalizeCommittedStopReadbackReceipt(
  value,
  baseInput,
  claimToken,
  expectedEvidence,
  code,
) {
  const requestContractVersion = baseInput.request.contractVersion;
  ensure(
    requestContractVersion === WRITER_LAUNCH_STOP_CLAIM_CONTRACT_VERSION,
    code,
  );
  const receipt = exactDataObject(
    value,
    STOP_RECONCILE_RECEIPT_KEYS,
    code,
  );
  ensure(
    receipt.status === "committed" &&
      typeof claimToken === "string" &&
      regexpTest(UUID_PATTERN, claimToken) &&
      receipt.claimTokenMatched === true &&
      stopClaimTokenMatchesRequest(claimToken, baseInput.request, code),
    code,
  );

  let transition;
  try {
    transition = assertCommittedWriterLaunchStopTransitionProof({
      after: receipt.session,
      before: baseInput.expectedSession,
      operation: receipt.operation,
      reservation: receipt.reservation,
    });
  } catch {
    fail(code);
  }
  const operation = transition.operation;
  ensure(
    operation.operationId === baseInput.operationId &&
      sameContent(operation.expectedSession, baseInput.expectedSession, code) &&
      sameContent(operation.request, baseInput.request, code),
    code,
  );
  const result = exactDataObject(
    operation.result,
    TERMINAL_RESULT_KEYS,
    code,
  );
  const evidence = exactDataObject(result.evidence, EVIDENCE_KEYS, code);
  ensure(
    result.resultVersion === 1 &&
      result.outcome === "writer-launch-stopped" &&
      evidence.contractVersion === LOGICAL_WRITER_LAUNCH_CONTRACT_VERSION &&
      evidence.status === "complete-stopped" &&
      evidence.launchAttemptId === baseInput.request.launch.launchAttemptId &&
      evidence.processIncarnationId ===
        baseInput.request.launch.processIncarnationId &&
      evidence.supervisorId === baseInput.request.launch.supervisorId &&
      evidence.writerIncarnationId ===
        baseInput.request.launch.writerIncarnationId &&
      evidence.proofId === baseInput.operationId &&
      sameContent(evidence, expectedEvidence, code),
    code,
  );
  const stop = exactFrozenRecord({
    contractVersion: requestContractVersion,
    launchAttemptId: baseInput.request.launch.launchAttemptId,
    request: operation.request,
    result: operation.result,
    state: "committed",
    stopOperationId: operation.operationId,
  });
  return exactFrozenRecord({
    finalized: false,
    launch: null,
    operation,
    reservation: transition.reservation,
    session: transition.after,
    status: "committed",
    stop,
  });
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

function canonicalJsonSha256(value, code) {
  let serialized;
  try {
    serialized = reflectApply(jsonStringifyIntrinsic, JsonObject, [value]);
  } catch {
    fail(code);
  }
  ensure(typeof serialized === "string", code);
  return sha256Parts([serialized], code);
}

function canonicalJsonProjectionSha256(value, code) {
  return canonicalJsonSha256(
    canonicalJsonDataTree(snapshotData(value, code)),
    code,
  );
}

function createStopClaimToken(code) {
  let token;
  try {
    token = callIntrinsic(randomUUIDIntrinsic, undefined, []);
  } catch {
    fail(code);
  }
  ensure(typeof token === "string" && regexpTest(UUID_PATTERN, token), code);
  return token;
}

function stopClaimTokenMatchesRequest(claimToken, requestValue, code) {
  const request = exactDataObjectVariant(
    requestValue,
    [STOP_REQUEST_V2_KEYS, STOP_REQUEST_V3_KEYS],
    code,
  );
  ensure(
    request.contractVersion === WRITER_LAUNCH_STOP_CLAIM_CONTRACT_VERSION ||
      request.contractVersion ===
        WRITER_LAUNCH_STOP_CAPTURE_HANDOFF_CONTRACT_VERSION,
    code,
  );
  return (
    sha256Parts(
      [
        "portable-codex-runtime:writer-launch-stop-claim:v1",
        "\0",
        claimToken,
      ],
      code,
    ) === assertSha256(request.dispatchClaimSha256, code)
  );
}

function stopOperationId(capture, launchAttemptId, code) {
  let serializedCapture;
  try {
    serializedCapture = reflectApply(jsonStringifyIntrinsic, JsonObject, [
      canonicalJsonDataTree(snapshotData(capture, code)),
    ]);
  } catch (error) {
    if (isInternalError(error)) throw error;
    fail(code);
  }
  ensure(typeof serializedCapture === "string", code);
  const digest = sha256Parts(
    [
      "portable-codex-runtime:writer-stop-capture:v2",
      "\0",
      launchAttemptId,
      "\0",
      serializedCapture,
    ],
    code,
  );
  return `writer-stop:${digest}`;
}

function atomicCrashCaptureStopOperationId(request, launchAttemptId, code) {
  let serializedRequest;
  try {
    serializedRequest = reflectApply(jsonStringifyIntrinsic, JsonObject, [
      canonicalJsonDataTree(snapshotData(request, code)),
    ]);
  } catch (error) {
    if (isInternalError(error)) throw error;
    fail(code);
  }
  ensure(typeof serializedRequest === "string", code);
  const digest = sha256Parts(
    [
      "portable-codex-runtime:writer-stop-atomic-crash-capture:v1",
      "\0",
      launchAttemptId,
      "\0",
      serializedRequest,
    ],
    code,
  );
  return `writer-stop:${digest}`;
}

/**
 * Derives the durable writer-stop operation identity from the complete
 * canonical capture tuple and the launch attempt it will retire.
 */
export function derivePostgresLogicalWriterStopOperationId(...args) {
  const code = "invalid_logical_writer_launch_request";
  ensure(args.length === 1, code);
  try {
    const input = exactDataObject(
      args[0],
      STOP_OPERATION_ID_INPUT_KEYS,
      code,
    );
    const capture = normalizeCaptureTuple(
      exactFrozenRecord({
        attachment: input.attachment,
        checkpoint: input.checkpoint,
        request: input.request,
      }),
      code,
    );
    return stopOperationId(
      capture,
      assertOpaqueId(input.launchAttemptId, code),
      code,
    );
  } catch (error) {
    if (isInternalError(error, code)) throw error;
    fail(code);
  }
}

/**
 * Derives the durable complete-stop identity from the full canonical atomic
 * crash-capture request and the launch attempt it will retire.
 */
export function derivePostgresLogicalWriterAtomicCrashCaptureStopOperationId(
  ...args
) {
  const code = "invalid_logical_writer_launch_request";
  ensure(args.length === 1, code);
  try {
    const input = exactDataObject(
      args[0],
      ATOMIC_CRASH_STOP_OPERATION_ID_INPUT_KEYS,
      code,
    );
    const request = normalizeAtomicCrashCaptureRequest(input.request, code);
    return atomicCrashCaptureStopOperationId(
      request,
      assertOpaqueId(input.launchAttemptId, code),
      code,
    );
  } catch (error) {
    if (isInternalError(error, code)) throw error;
    fail(code);
  }
}

function normalizeImageReservation(value, code) {
  ensure(isPostgresDetachedRestoreImagePlanReservation(value), code);
  return value;
}

function ensureMeasuredImageMatchesSession(
  measuredImage,
  expectedSession,
  code,
) {
  let manifest;
  try {
    manifest = assertSessionManifest(expectedSession.document.manifest);
  } catch {
    fail(code);
  }
  const projection = measuredImage.projection;
  const platformImage = projection.platformImage;
  ensure(
    manifest.sessionId === expectedSession.sessionId &&
      projection.codexSandbox === manifest.runtime.codexSandbox &&
      projection.codexVersion === manifest.runtime.codexVersion &&
      platformImage.digest === manifest.runtime.imageDigest &&
      platformImage.mediaType === manifest.runtime.imageMediaType &&
      `${platformImage.os}/${platformImage.architecture}` ===
        manifest.runtime.platform,
    code,
  );
}

function normalizePrepareInput(value, code) {
  const input = exactDataObject(value, PREPARE_INPUT_KEYS, code);
  const expectedSession = normalizeSession(
    input.expectedSession,
    code,
    true,
  );
  const document = expectedSession.document;
  ensure(
    (document.lifecycle === "ATTACHED" &&
      document.activeOperation === null &&
      document.launch === null &&
      document.attachment !== null &&
      document.lease !== null) ||
      document.lifecycle === "DETACHED",
    code,
  );
  return exactFrozenRecord({
    expectedSession,
    imageReservation: normalizeImageReservation(
      input.imageReservation,
      code,
    ),
    launchAttemptId: assertOpaqueId(input.launchAttemptId, code),
  });
}

function normalizeRunInput(value, code) {
  const input = exactDataObject(value, RUN_INPUT_KEYS, code);
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
    imageReservation: normalizeImageReservation(
      input.imageReservation,
      code,
    ),
    launchAttemptId: assertOpaqueId(input.launchAttemptId, code),
  });
}

function normalizePreparedRunInput(value, code) {
  const input = exactDataObject(value, PREPARED_RUN_INPUT_KEYS, code);
  return exactFrozenRecord({
    imageReservation: normalizeImageReservation(
      input.imageReservation,
      code,
    ),
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
  ensure(
    receipt.receiptVersion === LOGICAL_WRITER_LAUNCH_RECEIPT_VERSION,
    code,
  );
  const evidence = normalizeTerminalEvidence(
    receipt.evidence,
    attempt,
    ["complete-stopped", "not-started", "started"],
    code,
  );
  let stopWriter = null;
  let terminalRecord = null;
  if (evidence.status === "started") {
    stopWriter = assertSourceBackedFunction(receipt.stopWriter, {
      asynchronous: true,
      code,
    });
    ensure(receipt.terminalRecord === null, code);
  } else if (evidence.status === "complete-stopped") {
    try {
      terminalRecord = assertPodmanWriterSupervisorStateRecord(
        receipt.terminalRecord,
      );
    } catch {
      fail(code);
    }
    ensure(
      terminalRecord.status === "stopped" &&
        terminalRecord.revision === 4 &&
        terminalRecord.launchAttemptId === attempt.launchAttemptId &&
        terminalRecord.processIncarnationId ===
          evidence.processIncarnationId &&
        terminalRecord.stopProofId === evidence.proofId &&
        terminalRecord.writerIncarnationId ===
          evidence.writerIncarnationId,
      code,
    );
    ensure(receipt.stopWriter === null, code);
  } else {
    ensure(receipt.stopWriter === null && receipt.terminalRecord === null, code);
  }
  return exactFrozenRecord({ evidence, stopWriter, terminalRecord });
}

function normalizeSupervisorStateGcAuthorization(
  value,
  {
    launchAttemptId,
    sessionId,
    stateOwnerId,
    terminalKind,
    terminalOperationId,
    terminalRecord,
  },
  code,
) {
  const authorization = exactDataObject(
    value,
    SUPERVISOR_STATE_GC_AUTHORIZATION_KEYS,
    code,
  );
  ensure(
    authorization.contractVersion === 2 &&
      authorization.launchAttemptId === launchAttemptId &&
      authorization.sessionId === sessionId &&
      authorization.stateOwnerId === stateOwnerId &&
      authorization.terminalKind === terminalKind &&
      authorization.terminalOperationId === terminalOperationId &&
      assertSha256(authorization.authorizationSha256, code) ===
        authorization.authorizationSha256 &&
      assertSha256(authorization.terminalRecordSha256, code) ===
        authorization.terminalRecordSha256 &&
      sameContent(authorization.terminalRecord, terminalRecord, code),
    code,
  );
  canonicalTimestampMilliseconds(authorization.authorizedAt, code);
  return snapshotData(value, code);
}

async function readSupervisorStateGcAuthorization(
  authority,
  expected,
  code,
) {
  const value = await invokeAsync(
    authority,
    "readWriterSupervisorStateGcAuthorization",
    [
      exactFrozenRecord({
        stateOwnerId: expected.stateOwnerId,
        terminalOperationId: expected.terminalOperationId,
      }),
    ],
    code,
  );
  return normalizeSupervisorStateGcAuthorization(value, expected, code);
}

function normalizeReconcileCallbackReceipt(value, attempt, code) {
  const receipt = exactDataObject(
    value,
    RECONCILE_CALLBACK_RECEIPT_KEYS,
    code,
  );
  ensure(
    receipt.receiptVersion === LOGICAL_WRITER_RECONCILE_RECEIPT_VERSION,
    code,
  );
  const evidence = normalizeTerminalEvidence(
    receipt.evidence,
    attempt,
    ["complete-stopped", "not-started"],
    code,
  );
  let terminalRecord = null;
  if (receipt.terminalRecord !== null) {
    ensure(evidence.status === "complete-stopped", code);
    try {
      terminalRecord = assertPodmanWriterSupervisorStateRecord(
        receipt.terminalRecord,
      );
    } catch {
      fail(code);
    }
    ensure(
      terminalRecord.status === "stopped" &&
        terminalRecord.revision === 4 &&
        terminalRecord.launchAttemptId === attempt.launchAttemptId &&
        terminalRecord.processIncarnationId ===
          evidence.processIncarnationId &&
        terminalRecord.stopProofId === evidence.proofId &&
        terminalRecord.writerIncarnationId ===
          evidence.writerIncarnationId,
      code,
    );
  }
  return exactFrozenRecord({ evidence, terminalRecord });
}

function assertOpaqueWriterHandle(value, code) {
  ensure(
    value !== null &&
      typeof value === "object" &&
      !isProxyValue(value) &&
      !arrayIsArray(value) &&
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

function normalizeCaptureTuple(value, code) {
  const input = exactDataObject(value, RESOLVER_INPUT_KEYS, code);
  let attachment;
  let checkpoint;
  let request;
  try {
    attachment = assertSessionAttachment(input.attachment);
    checkpoint = assertCheckpointDescriptor(input.checkpoint);
    request = assertStorageMutationRequest(input.request);
  } catch {
    fail(code);
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
    code,
  );
  return exactFrozenRecord({ attachment, checkpoint, request });
}

function normalizeAtomicCrashCaptureRequest(value, code) {
  try {
    return assertAtomicCrashCaptureRequest(value);
  } catch {
    fail(code);
  }
}

function captureTupleForAtomicCrashRequest(request) {
  return exactFrozenRecord({
    attachment: request.sourceAttachment,
    checkpoint: request.checkpoint,
    request: request.mutationRequest,
  });
}

/**
 * Composes durable launch/stop admission with one-use image and in-process
 * writer capabilities. Production restore remains disabled until the wider
 * bounded-recovery protocol composes this primitive with capture publication.
 */
export function createPostgresLogicalWriterLauncher(...args) {
  const optionCode = "invalid_logical_writer_launch_request";
  const admissionCode = "logical_writer_launch_admission_unavailable";
  const outcomeCode = "logical_writer_launch_outcome_uncertain";
  ensure(args.length === 1, optionCode);
  const options = exactDataObject(args[0], OPTION_KEYS, optionCode);
  const authority = collaboratorBinding(
    options.authority,
    AUTHORITY_METHODS,
    optionCode,
  );
  const operationGuard = operationGuardBinding(
    options.operationGuard,
    optionCode,
  );
  ensure(
    isPostgresDetachedRestoreImagePlanBinding(options.imagePlanBinding),
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
      LOGICAL_WRITER_SUPERVISOR_CONTRACT_VERSION,
    optionCode,
  );
  const supervisor = exactFrozenRecord({
    contractVersion: LOGICAL_WRITER_SUPERVISOR_CONTRACT_VERSION,
    launchWriter: assertSourceBackedFunction(supervisorOptions.launchWriter, {
      asynchronous: true,
      code: optionCode,
    }),
    reconcileWriterLaunch: assertSourceBackedFunction(
      supervisorOptions.reconcileWriterLaunch,
      { asynchronous: true, code: optionCode },
    ),
    stateOwnerId: assertStateOwnerId(
      supervisorOptions.stateOwnerId,
      optionCode,
    ),
    supervisorId: assertOpaqueId(supervisorOptions.supervisorId, optionCode),
  });
  const imagePlanBinding = options.imagePlanBinding;
  const imageConsumeReservation = objectGetOwnPropertyDescriptor(
    imagePlanBinding,
    "consumeImageReservation",
  )?.value;
  const imageRevalidateReservation = objectGetOwnPropertyDescriptor(
    imagePlanBinding,
    "revalidateImageReservation",
  )?.value;
  ensure(
    typeof imageConsumeReservation === "function" &&
      typeof imageRevalidateReservation === "function",
    optionCode,
  );
  const stoppedWriterCoordinator = options.stoppedWriterCoordinator;
  const atomicCrashCaptureAuthorities = new WeakMapConstructor();
  const recordsByAttempt = new MapConstructor();
  const recordsByAttachmentId = new MapConstructor();
  const recordsByWriter = new WeakMapConstructor();

  function assertWriterLaunchAvailable(attachment, canonicalLease, code) {
    invokeStoppedCoordinatorSync(
      stoppedWriterCoordinator,
      stoppedAssertWriterLaunchAvailableIntrinsic,
      exactFrozenRecord({ attachment, canonicalLease }),
      code,
    );
  }

  function releaseStoppedRecord(record) {
    ensure(
      record.state === "stopped" &&
        mapGet(recordsByAttempt, record.launchAttemptId) === record &&
        mapGet(
          recordsByAttachmentId,
          record.attachment.attachmentId,
        ) === record,
      outcomeCode,
    );
    const attemptDeleted = mapDelete(
      recordsByAttempt,
      record.launchAttemptId,
    );
    const attachmentDeleted = mapDelete(
      recordsByAttachmentId,
      record.attachment.attachmentId,
    );
    const writerDeleted = weakMapDelete(recordsByWriter, record.writer);
    ensure(attemptDeleted && attachmentDeleted && writerDeleted, outcomeCode);
  }

  async function readAttempt(launchAttemptId) {
    return normalizeReadReceipt(
      await invokeAsync(
        authority,
        "readWriterLaunchAttempt",
        [
          exactFrozenRecord({
            operationId: launchAttemptId,
            stateOwnerId: supervisor.stateOwnerId,
          }),
        ],
        outcomeCode,
      ),
      launchAttemptId,
      outcomeCode,
    );
  }

  async function revalidateImageReservation(imageReservation, code) {
    return normalizeMeasuredImage(
      await invokeImageCoordinator(
        imagePlanBinding,
        imageRevalidateReservation,
        imageReservation,
        code,
      ),
      code,
    );
  }

  function validateFacadeSupervisor(request, code) {
    ensure(
      sameContent(
        request.supervisor,
        exactFrozenRecord({
          contractVersion: LOGICAL_WRITER_LAUNCH_CONTRACT_VERSION,
          supervisorId: supervisor.supervisorId,
        }),
        code,
      ),
      code,
    );
  }

  function validatePreparedAuthorityRelation(read) {
    ensure(
      read.operation.state === "prepared" &&
        read.operation.revision === "0" &&
        read.attempt.state === "prepared" &&
        read.attempt.result === null &&
        read.launch === null,
      outcomeCode,
    );
    const request = read.attempt.request;
    const expectedSession = read.operation.expectedSession;
    const expectedDocument = expectedSession.document;
    const currentDocument = read.session.document;
    const expectedLastOperation = exactDataObject(
      expectedDocument.lastOperation,
      LAST_OPERATION_KEYS,
      outcomeCode,
    );
    const generationProducer =
      expectedLastOperation.kind ===
        RESTORE_DESTINATION_GENERATION_OPERATION_KIND &&
      expectedLastOperation.operationId === request.generation.operationId &&
      request.generation.committedAt === expectedSession.updatedAt;
    const activationProducer =
      expectedLastOperation.kind ===
        RESTORE_ATTACHMENT_ACTIVATION_OPERATION_KIND &&
      expectedLastOperation.operationId === request.attachment.operationId &&
      canonicalTimestampMilliseconds(
        request.generation.committedAt,
        outcomeCode,
      ) <=
        canonicalTimestampMilliseconds(
          expectedSession.updatedAt,
          outcomeCode,
        ) &&
      read.operation.createdAt === expectedSession.updatedAt &&
      read.operation.updatedAt === expectedSession.updatedAt &&
      read.reservation.createdAt === expectedSession.updatedAt &&
      read.reservation.updatedAt === expectedSession.updatedAt;
    ensure(
      expectedDocument.lifecycle === "ATTACHED" &&
        expectedDocument.activeOperation === null &&
        expectedDocument.launch === null &&
        expectedDocument.attachment !== null &&
        expectedDocument.lease !== null &&
        expectedDocument.writerEpoch === request.fencingEpoch &&
        expectedLastOperation.state === "committed" &&
        (generationProducer || activationProducer) &&
        currentDocument.lifecycle === "ATTACHED" &&
        currentDocument.launch === null &&
        currentDocument.writerEpoch === request.fencingEpoch &&
        sameContent(
          expectedDocument.attachment,
          request.attachment,
          outcomeCode,
        ) &&
        sameContent(expectedDocument.lease, request.lease, outcomeCode) &&
        sameContent(
          currentDocument.attachment,
          request.attachment,
          outcomeCode,
        ) &&
        sameContent(currentDocument.lease, request.lease, outcomeCode) &&
        sameContent(
          currentDocument.lastOperation,
          expectedDocument.lastOperation,
          outcomeCode,
        ) &&
        sameContent(
          currentDocument.manifest,
          expectedDocument.manifest,
          outcomeCode,
        ) &&
        BigIntConstructor(read.session.revision) ===
          BigIntConstructor(expectedSession.revision) + 1n,
      outcomeCode,
    );
    ensureMeasuredImageMatchesSession(
      request.measuredImage,
      expectedSession,
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
    ensure(read.launch !== null, outcomeCode);
    if (record.launch === null) {
      record.launch = read.launch;
    } else {
      ensure(sameContent(record.launch, read.launch, outcomeCode), outcomeCode);
    }
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
    terminalRecord = null,
  ) {
    let finalReceipt;
    try {
      const rawReceipt = await invokeAsync(
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
              ...(terminalRecord === null ? {} : { terminalRecord }),
            }),
          ],
          outcomeCode,
        );
      finalReceipt =
        terminalRecord === null
          ? normalizeFinalizeReceipt(
              rawReceipt,
              readLike.operation.operationId,
              readLike.operation.request,
              evidence,
              outcomeCode,
            )
          : normalizeFinalizeGcReceipt(
              rawReceipt,
              readLike.operation.operationId,
              readLike.operation.request,
              evidence,
              terminalRecord,
              supervisor.stateOwnerId,
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
        if (terminalRecord !== null) {
          await readSupervisorStateGcAuthorization(
            authority,
            {
              launchAttemptId: readback.attempt.launchAttemptId,
              sessionId: readback.operation.sessionId,
              stateOwnerId: supervisor.stateOwnerId,
              terminalKind: WRITER_LAUNCH_ATTEMPT_OPERATION_KIND,
              terminalOperationId: readback.operation.operationId,
              terminalRecord,
            },
            outcomeCode,
          );
        }
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
      authorizedAtomicCrashRequest: null,
      authorizedCapture: null,
      authorizedCaptureAttemptId: null,
      authorizedStopOperationId: null,
      canonicalLease: claim.attempt.request.lease,
      codexSessionId:
        claim.operation.expectedSession.document.manifest.codex.sessionId,
      codexThreadId:
        claim.operation.expectedSession.document.manifest.codex.rootThreadId,
      evidence,
      imageDigest:
        claim.attempt.request.measuredImage.projection.platformImage.digest,
      launch: null,
      launchAttemptId: claim.attempt.launchAttemptId,
      pendingStop: null,
      preparedCaptureHandoffReceipt: null,
      processIncarnationId: evidence.processIncarnationId,
      request: claim.attempt.request,
      state: "registering",
      stopBaseInput: null,
      stopClaimAttemptedFor: null,
      stopClaimToken: null,
      stopContractVersion: null,
      stopRoute: null,
      stopEvidence: null,
      stopReceipt: null,
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
        record.state === "stop-dispatch" &&
          record.pendingStop !== null &&
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
        await assertGuardHeld(record.pendingStop.probe, outcomeCode);
        const stopped = exactDataObject(
          await invokeSupervisor(
            record.stopWriter,
            snapshotData(bindingValue, outcomeCode),
            outcomeCode,
          ),
          PHYSICAL_STOP_RESULT_KEYS,
          outcomeCode,
        );
        ensure(
          stopped.confirmation === STOPPED_WRITER_STOP_CONFIRMED &&
            stopped.contractVersion ===
              LOGICAL_WRITER_SUPERVISOR_CONTRACT_VERSION,
          outcomeCode,
        );
        let terminalRecord;
        try {
          terminalRecord = assertPodmanWriterSupervisorStateRecord(
            stopped.terminalRecord,
          );
        } catch {
          fail(outcomeCode);
        }
        ensure(
          terminalRecord.status === "stopped" &&
            terminalRecord.revision === 4 &&
            terminalRecord.launchAttemptId === record.launchAttemptId &&
            terminalRecord.processIncarnationId ===
              record.processIncarnationId &&
            terminalRecord.stopOperationId === binding.stopOperationId &&
            terminalRecord.writerIncarnationId ===
              record.writerIncarnationId,
          outcomeCode,
        );
        await assertGuardHeld(record.pendingStop.probe, outcomeCode);
        const stopEvidence = exactFrozenRecord({
          contractVersion: LOGICAL_WRITER_LAUNCH_CONTRACT_VERSION,
          launchAttemptId: record.launchAttemptId,
          processIncarnationId: record.processIncarnationId,
          proofId: binding.stopOperationId,
          status: "complete-stopped",
          supervisorId: record.supervisorId,
          writerIncarnationId: record.writerIncarnationId,
        });
        const stopReceipt =
          record.pendingStop.baseInput.request.contractVersion ===
          WRITER_LAUNCH_STOP_CAPTURE_HANDOFF_CONTRACT_VERSION
            ? await finalizeStopCaptureWithReadback(
                record.pendingStop.baseInput,
                record.stopClaimToken,
                stopEvidence,
                terminalRecord,
              )
            : await finalizeStopWithReadback(
                record.pendingStop.baseInput,
                record.stopClaimToken,
                stopEvidence,
                terminalRecord,
              );
        await assertGuardHeld(record.pendingStop.probe, outcomeCode);
        record.stopEvidence = stopEvidence;
        record.stopReceipt = stopReceipt;
        record.pendingStop = null;
        record.state = "stopped";
        return STOPPED_WRITER_STOP_CONFIRMED;
      } catch {
        record.pendingStop = null;
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
      weakMapSet(recordsByWriter, record.writer, record);
      return record;
    } catch (error) {
      record.state = "lost";
      if (isInternalError(error)) throw error;
      fail(outcomeCode);
    }
  }

  async function dispatchGrantedLaunch(
    claim,
    imageReservation,
    probe,
    uncertaintyState,
  ) {
    ensure(claim.dispatchGranted === true, outcomeCode);
    await assertGuardHeld(probe, outcomeCode);
    assertWriterLaunchAvailable(
      claim.attempt.request.attachment,
      claim.attempt.request.lease,
      outcomeCode,
    );
    const consumedImage = normalizeMeasuredImage(
      await invokeImageCoordinator(
        imagePlanBinding,
        imageConsumeReservation,
        imageReservation,
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
    assertWriterLaunchAvailable(
      claim.attempt.request.attachment,
      claim.attempt.request.lease,
      outcomeCode,
    );
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
      const authorizeSupervisorStateGc =
        callbackReceipt.evidence.status === "complete-stopped";
      return finalizeWithReadback(
        claim,
        callbackReceipt.evidence,
        authorizeSupervisorStateGc
          ? "finalizeWriterLaunchAttemptStoppedAndAuthorizeSupervisorStateGc"
          : "finalizeWriterLaunchAttemptStopped",
        uncertaintyState,
        authorizeSupervisorStateGc ? callbackReceipt.terminalRecord : null,
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
              reason: WRITER_LAUNCH_PRE_DISPATCH_CANCELLATION_REASON,
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
    const authorizeSupervisorStateGc =
      callbackReceipt.terminalRecord !== null;
    return finalizeWithReadback(
      read,
      callbackReceipt.evidence,
      authorizeSupervisorStateGc
        ? "finalizeWriterLaunchAttemptStoppedAndAuthorizeSupervisorStateGc"
        : "finalizeWriterLaunchAttemptStopped",
      uncertaintyState,
      callbackReceipt.terminalRecord,
    );
  }

  async function reconcilePreparedClaimAmbiguity(
    launchAttemptId,
    probe,
    uncertaintyState,
    expectedRequest,
  ) {
    const read = await readAttempt(launchAttemptId);
    ensure(
      sameContent(read.attempt.request, expectedRequest, optionCode),
      optionCode,
    );
    if (read.operation.state === "prepared") {
      validatePreparedAuthorityRelation(read);
      await assertGuardHeld(probe, admissionCode);
      fail(admissionCode);
    }
    return reconcileWithinGuard(
      launchAttemptId,
      probe,
      uncertaintyState,
      expectedRequest,
    );
  }

  async function prepareLaunchIntentInternal(...prepareArgs) {
    ensure(prepareArgs.length === 1, optionCode);
    const input = normalizePrepareInput(prepareArgs[0], optionCode);
    try {
      return await invokeGuard(
        operationGuard,
        [
          input.launchAttemptId,
          async (probeValue, completeValue) => {
            const probe = normalizeProbe(probeValue, admissionCode);
            const complete = assertCallback(completeValue, admissionCode);
            ensure(objectIsFrozen(completeValue), admissionCode);
            await assertGuardHeld(probe, admissionCode);
            const measuredImage = await revalidateImageReservation(
              input.imageReservation,
              admissionCode,
            );
            await assertGuardHeld(probe, admissionCode);
            ensureMeasuredImageMatchesSession(
              measuredImage,
              input.expectedSession,
              optionCode,
            );
            const result = exactFrozenRecord({
              launchAttemptId: input.launchAttemptId,
              measuredImage,
              supervisor: exactFrozenRecord({
                contractVersion: LOGICAL_WRITER_LAUNCH_CONTRACT_VERSION,
                supervisorId: supervisor.supervisorId,
              }),
            });
            return callIntrinsic(complete, undefined, [result]);
          },
        ],
        admissionCode,
      );
    } catch (error) {
      if (
        isInternalError(error, optionCode) ||
        isInternalError(error, admissionCode)
      ) {
        throw error;
      }
      fail(admissionCode);
    }
  }

  async function runLaunchInternal(...runArgs) {
    ensure(runArgs.length === 1, optionCode);
    const input = normalizeRunInput(runArgs[0], optionCode);
    const uncertaintyState = { attempted: false };
    let dispatchDefinitelyBegan = false;
    let dispatchOperation = null;
    let durableReservationMayExist = false;

    try {
      return await invokeGuard(
        operationGuard,
        [
          input.launchAttemptId,
          async (probeValue, completeValue) => {
            const probe = normalizeProbe(probeValue, outcomeCode);
            const complete = assertCallback(completeValue, outcomeCode);
            ensure(objectIsFrozen(completeValue), outcomeCode);
            const expectedSession = normalizeSession(
              await invokeAsync(
                authority,
                "readSession",
                [exactFrozenRecord({ sessionId: input.generation.sessionId })],
                admissionCode,
              ),
              admissionCode,
            );
            const measuredImage = await revalidateImageReservation(
              input.imageReservation,
              admissionCode,
            );
            let typedRequest;
            try {
              typedRequest = createWriterLaunchAttemptOperationRequest({
                expectedSession,
                generation: input.generation,
                measuredImage,
                supervisor: exactFrozenRecord({
                  contractVersion: LOGICAL_WRITER_LAUNCH_CONTRACT_VERSION,
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

            assertWriterLaunchAvailable(
              typedRequest.attachment,
              typedRequest.lease,
              admissionCode,
            );

            let reserve;
            durableReservationMayExist = true;
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
                expectedSession,
                outcomeCode,
              );
            } catch {
              return callIntrinsic(complete, undefined, [
                await reconcileWithinGuard(
                  input.launchAttemptId,
                  probe,
                  uncertaintyState,
                  typedRequest,
                ),
              ]);
            }
            if (!reserve.acquired) {
              return callIntrinsic(complete, undefined, [
                await reconcileWithinGuard(
                  input.launchAttemptId,
                  probe,
                  uncertaintyState,
                  typedRequest,
                ),
              ]);
            }

            assertWriterLaunchAvailable(
              typedRequest.attachment,
              typedRequest.lease,
              outcomeCode,
            );

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
                      stateOwnerId: supervisor.stateOwnerId,
                    }),
                  ],
                  outcomeCode,
                ),
                input.launchAttemptId,
                typedRequest,
                input.generation,
                expectedSession,
                outcomeCode,
              );
            } catch {
              return callIntrinsic(complete, undefined, [
                await reconcileWithinGuard(
                  input.launchAttemptId,
                  probe,
                  uncertaintyState,
                  typedRequest,
                ),
              ]);
            }
            if (!claim.dispatchGranted) {
              return callIntrinsic(complete, undefined, [
                await reconcileWithinGuard(
                  input.launchAttemptId,
                  probe,
                  uncertaintyState,
                  typedRequest,
                ),
              ]);
            }
            dispatchDefinitelyBegan = true;
            dispatchOperation = claim.operation;
            return callIntrinsic(complete, undefined, [
              await dispatchGrantedLaunch(
                claim,
                input.imageReservation,
                probe,
                uncertaintyState,
              ),
            ]);
          },
        ],
        outcomeCode,
      );
    } catch (error) {
      if (
        isInternalError(error, optionCode) ||
        isInternalError(error, "logical_writer_handle_unavailable") ||
        isInternalError(error, admissionCode)
      ) {
        throw error;
      }
      if (!durableReservationMayExist) fail(admissionCode);
      if (dispatchDefinitelyBegan && dispatchOperation !== null) {
        await bestEffortMarkUncertain(dispatchOperation, uncertaintyState);
      }
      fail(outcomeCode);
    }
  }

  async function runPreparedLaunchInternal(...runArgs) {
    ensure(runArgs.length === 1, optionCode);
    const input = normalizePreparedRunInput(runArgs[0], optionCode);
    const uncertaintyState = { attempted: false };
    let dispatchDefinitelyBegan = false;
    let dispatchOperation = null;

    try {
      return await invokeGuard(
        operationGuard,
        [
          input.launchAttemptId,
          async (probeValue, completeValue) => {
            const probe = normalizeProbe(probeValue, outcomeCode);
            const complete = assertCallback(completeValue, outcomeCode);
            ensure(objectIsFrozen(completeValue), outcomeCode);
            const read = await readAttempt(input.launchAttemptId);
            if (read.operation.state === "committed") {
              return callIntrinsic(complete, undefined, [
                terminalResultFromRead(read),
              ]);
            }
            validateFacadeSupervisor(read.attempt.request, optionCode);
            if (
              read.operation.state === "starting" ||
              read.operation.state === "uncertain"
            ) {
              return callIntrinsic(complete, undefined, [
                await reconcileWithinGuard(
                  input.launchAttemptId,
                  probe,
                  uncertaintyState,
                  read.attempt.request,
                ),
              ]);
            }

            validatePreparedAuthorityRelation(read);
            assertWriterLaunchAvailable(
              read.attempt.request.attachment,
              read.attempt.request.lease,
              admissionCode,
            );
            await assertGuardHeld(probe, outcomeCode);
            const measuredImage = await revalidateImageReservation(
              input.imageReservation,
              admissionCode,
            );
            await assertGuardHeld(probe, outcomeCode);
            ensure(
              sameContent(
                measuredImage,
                read.attempt.request.measuredImage,
                optionCode,
              ),
              optionCode,
            );

            assertWriterLaunchAvailable(
              read.attempt.request.attachment,
              read.attempt.request.lease,
              outcomeCode,
            );

            let claim;
            try {
              claim = normalizeClaimReceipt(
                await invokeAsync(
                  authority,
                  "claimWriterLaunchAttemptDispatch",
                  [
                    exactFrozenRecord({
                      ...transitionInput(read.operation),
                      stateOwnerId: supervisor.stateOwnerId,
                    }),
                  ],
                  outcomeCode,
                ),
                input.launchAttemptId,
                read.attempt.request,
                null,
                read.operation.expectedSession,
                outcomeCode,
              );
            } catch {
              return callIntrinsic(complete, undefined, [
                await reconcilePreparedClaimAmbiguity(
                  input.launchAttemptId,
                  probe,
                  uncertaintyState,
                  read.attempt.request,
                ),
              ]);
            }
            if (!claim.dispatchGranted) {
              return callIntrinsic(complete, undefined, [
                await reconcilePreparedClaimAmbiguity(
                  input.launchAttemptId,
                  probe,
                  uncertaintyState,
                  read.attempt.request,
                ),
              ]);
            }
            dispatchDefinitelyBegan = true;
            dispatchOperation = claim.operation;
            return callIntrinsic(complete, undefined, [
              await dispatchGrantedLaunch(
                claim,
                input.imageReservation,
                probe,
                uncertaintyState,
              ),
            ]);
          },
        ],
        outcomeCode,
      );
    } catch (error) {
      if (
        isInternalError(error, optionCode) ||
        isInternalError(error, "logical_writer_handle_unavailable") ||
        isInternalError(error, admissionCode)
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
          async (probeValue, completeValue) => {
            const complete = assertCallback(completeValue, outcomeCode);
            ensure(objectIsFrozen(completeValue), outcomeCode);
            return callIntrinsic(complete, undefined, [
              await reconcileWithinGuard(
                input.launchAttemptId,
                normalizeProbe(probeValue, outcomeCode),
                uncertaintyState,
              ),
            ]);
          },
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

  function captureRecord(
    capture,
    states,
    code,
    stopContractVersion = WRITER_LAUNCH_STOP_CLAIM_CONTRACT_VERSION,
    stopRoute = CLEAN_CAPTURE_ROUTE,
    atomicCrashRequest = null,
  ) {
    const record = mapGet(
      recordsByAttachmentId,
      capture.attachment.attachmentId,
    );
    ensure(
      record !== undefined &&
        arrayIncludes(states, record.state) &&
        sameContent(capture.attachment, record.attachment, code) &&
        capture.checkpoint.codexSessionId === record.codexSessionId &&
        capture.checkpoint.codexThreadId === record.codexThreadId &&
        capture.checkpoint.imageDigest === record.imageDigest,
      code,
    );
    ensure(
      stopContractVersion === WRITER_LAUNCH_STOP_CLAIM_CONTRACT_VERSION ||
        stopContractVersion ===
          WRITER_LAUNCH_STOP_CAPTURE_HANDOFF_CONTRACT_VERSION,
      code,
    );
    ensure(
      stopRoute === CLEAN_CAPTURE_ROUTE ||
        (stopRoute === ATOMIC_CRASH_CAPTURE_ROUTE &&
          stopContractVersion ===
            WRITER_LAUNCH_STOP_CLAIM_CONTRACT_VERSION),
      code,
    );
    ensure(
      record.stopContractVersion === null ||
        record.stopContractVersion === stopContractVersion,
      code,
    );
    ensure(record.stopRoute === null || record.stopRoute === stopRoute, code);
    let derivedStopOperationId;
    if (stopRoute === CLEAN_CAPTURE_ROUTE) {
      ensure(
        atomicCrashRequest === null &&
          record.authorizedAtomicCrashRequest === null &&
          (record.authorizedCapture === null ||
            sameContent(record.authorizedCapture, capture, code)),
        code,
      );
      derivedStopOperationId = stopOperationId(
        capture,
        record.launchAttemptId,
        code,
      );
    } else {
      ensure(
        record.authorizedCapture === null &&
          atomicCrashRequest !== null &&
          sameContent(
            capture,
            captureTupleForAtomicCrashRequest(atomicCrashRequest),
            code,
          ) &&
          (record.authorizedAtomicCrashRequest === null ||
            sameContent(
              record.authorizedAtomicCrashRequest,
              atomicCrashRequest,
              code,
            )),
        code,
      );
      derivedStopOperationId = atomicCrashCaptureStopOperationId(
        atomicCrashRequest,
        record.launchAttemptId,
        code,
      );
    }
    ensure(
      record.authorizedStopOperationId === null ||
        record.authorizedStopOperationId === derivedStopOperationId,
      code,
    );
    if (record.stopContractVersion === null) {
      record.stopContractVersion = stopContractVersion;
    }
    if (record.stopRoute === null) record.stopRoute = stopRoute;
    if (
      stopContractVersion ===
        WRITER_LAUNCH_STOP_CAPTURE_HANDOFF_CONTRACT_VERSION &&
      record.authorizedCaptureAttemptId === null
    ) {
      record.authorizedCaptureAttemptId = createStopClaimToken(code);
    }
    if (record.authorizedStopOperationId === null) {
      // The raw bearer never enters the durable operation or public
      // resolution; only its domain-separated digest is persisted.
      record.stopClaimToken = createStopClaimToken(code);
    }
    ensure(
      typeof record.stopClaimToken === "string" &&
        regexpTest(UUID_PATTERN, record.stopClaimToken),
      code,
    );
    if (stopRoute === CLEAN_CAPTURE_ROUTE) {
      record.authorizedCapture = capture;
    } else {
      record.authorizedAtomicCrashRequest = atomicCrashRequest;
    }
    record.authorizedStopOperationId = derivedStopOperationId;
    return record;
  }

  function resolutionForRecord(record) {
    return exactFrozenRecord({
      canonicalLeaseAtRegistration: record.canonicalLease,
      processIncarnationId: record.processIncarnationId,
      stopOperationId: record.authorizedStopOperationId,
      writer: record.writer,
      writerIncarnationId: record.writerIncarnationId,
    });
  }

  function validatePreparedCaptureHandoffState(record, capture) {
    ensure(
      record.state === "stopped" &&
        record.stopRoute === CLEAN_CAPTURE_ROUTE &&
        record.stopContractVersion ===
          WRITER_LAUNCH_STOP_CAPTURE_HANDOFF_CONTRACT_VERSION &&
        record.authorizedCapture !== null &&
        sameContent(record.authorizedCapture, capture, outcomeCode) &&
        record.authorizedStopOperationId ===
          stopOperationId(capture, record.launchAttemptId, outcomeCode) &&
        record.stopBaseInput !== null &&
        record.stopBaseInput.operationId ===
          record.authorizedStopOperationId &&
        record.stopBaseInput.request.contractVersion ===
          WRITER_LAUNCH_STOP_CAPTURE_HANDOFF_CONTRACT_VERSION &&
        typeof record.stopClaimToken === "string" &&
        regexpTest(UUID_PATTERN, record.stopClaimToken) &&
        stopClaimTokenMatchesRequest(
          record.stopClaimToken,
          record.stopBaseInput.request,
          outcomeCode,
        ) &&
        record.stopEvidence !== null &&
        record.stopReceipt !== null,
      outcomeCode,
    );
    const normalizedStopReceipt = normalizeStopCaptureHandoffReceipt(
      record.stopReceipt,
      record.stopBaseInput,
      record.stopClaimToken,
      record.stopEvidence,
      false,
    );
    ensure(
      sameContent(normalizedStopReceipt, record.stopReceipt, outcomeCode),
      outcomeCode,
    );
  }

  function replayPreparedCaptureHandoff(record, capture) {
    validatePreparedCaptureHandoffState(record, capture);
    const receipt = record.preparedCaptureHandoffReceipt;
    ensure(receipt !== null && objectIsFrozen(receipt), outcomeCode);
    const stored = exactDataObject(
      receipt,
      PREPARED_CAPTURE_HANDOFF_RECEIPT_KEYS,
      outcomeCode,
    );
    const resolution = normalizeStopResolution(
      stored.resolution,
      outcomeCode,
    );
    ensure(
      stored.capture === record.stopReceipt.capture &&
        stored.evidence === record.stopEvidence &&
        stored.session === record.stopReceipt.session &&
        stored.status === record.stopReceipt.status &&
        stored.stop === record.stopReceipt.stop &&
        resolution.writer === record.writer &&
        sameContent(
          resolution,
          resolutionForRecord(record),
          outcomeCode,
        ),
      outcomeCode,
    );
    return receipt;
  }

  function storePreparedCaptureHandoff(record, capture) {
    ensure(record.preparedCaptureHandoffReceipt === null, outcomeCode);
    validatePreparedCaptureHandoffState(record, capture);
    record.preparedCaptureHandoffReceipt = exactFrozenRecord({
      capture: record.stopReceipt.capture,
      evidence: record.stopEvidence,
      resolution: resolutionForRecord(record),
      session: record.stopReceipt.session,
      status: record.stopReceipt.status,
      stop: record.stopReceipt.stop,
    });
    return replayPreparedCaptureHandoff(record, capture);
  }

  function validateCurrentStopSession(session, record, code) {
    let manifest;
    let currentLease;
    let registeredLease;
    try {
      manifest = assertSessionManifest(session.document.manifest);
      currentLease = assertLeaseGrant(session.document.lease);
      registeredLease = assertLeaseGrant(record.canonicalLease);
    } catch {
      fail(code);
    }
    ensure(
      currentLease.contractVersion === registeredLease.contractVersion &&
        currentLease.sessionId === registeredLease.sessionId &&
        currentLease.leaseId === registeredLease.leaseId &&
        currentLease.holderId === registeredLease.holderId &&
        currentLease.fencingEpoch === registeredLease.fencingEpoch &&
        canonicalTimestampMilliseconds(currentLease.expiresAt, code) >=
          canonicalTimestampMilliseconds(registeredLease.expiresAt, code),
      code,
    );
    ensure(
      record.launch !== null &&
        session.sessionId === record.attachment.sessionId &&
        session.document.lifecycle === "ATTACHED" &&
        session.document.activeOperation === null &&
        session.document.writerEpoch === record.canonicalLease.fencingEpoch &&
        sameContent(session.document.attachment, record.attachment, code) &&
        sameContent(session.document.launch, record.launch, code) &&
        manifest.codex.sessionId === record.codexSessionId &&
        manifest.codex.rootThreadId === record.codexThreadId &&
        manifest.runtime.imageDigest === record.imageDigest,
      code,
    );
  }

  async function reconcileStopOperation(baseInput, claimToken) {
    return normalizeStopReconcileReceipt(
      await readStopOperationReceipt(baseInput, claimToken),
      baseInput,
      outcomeCode,
    );
  }

  async function readStopOperationReceipt(baseInput, claimToken) {
    return invokeAsync(
      authority,
      "reconcileWriterLaunchStopOperation",
      [exactFrozenRecord({ ...baseInput, claimToken })],
      outcomeCode,
    );
  }

  function normalizeStopCaptureHandoffReceipt(
    value,
    baseInput,
    claimToken,
    expectedEvidence,
    reconcile,
    terminalRecord = null,
  ) {
    const receipt = exactDataObject(
      value,
      reconcile
        ? STOP_CAPTURE_HANDOFF_RECONCILE_RECEIPT_KEYS
        : terminalRecord === null
          ? STOP_CAPTURE_HANDOFF_RECEIPT_KEYS
          : STOP_CAPTURE_HANDOFF_GC_RECEIPT_KEYS,
      outcomeCode,
    );
    if (reconcile) {
      ensure(
        receipt.claimTokenMatched === true &&
          stopClaimTokenMatchesRequest(
            claimToken,
            baseInput.request,
            outcomeCode,
          ),
        outcomeCode,
      );
    }
    const capture = exactDataObject(
      receipt.capture,
      STOP_CAPTURE_HANDOFF_RELATION_KEYS,
      outcomeCode,
    );
    const stop = exactDataObject(
      receipt.stop,
      STOP_CAPTURE_HANDOFF_STOP_KEYS,
      outcomeCode,
    );
    ensure(typeof stop.finalized === "boolean", outcomeCode);
    let proof;
    try {
      proof = assertWriterLaunchStopCaptureHandoffProof({
        before: baseInput.expectedSession,
        capture,
        session: receipt.session,
        stop: exactFrozenRecord({
          operation: stop.operation,
          reservation: stop.reservation,
        }),
      });
    } catch {
      fail(outcomeCode);
    }
    ensure(
      receipt.status === proof.capture.operation.state &&
        proof.stop.operation.operationId === baseInput.operationId &&
        sameContent(
          proof.stop.operation.request,
          baseInput.request,
          outcomeCode,
        ) &&
        proof.capture.operation.operationId ===
          baseInput.request.captureIntent.admission.request.operationId &&
        sameContent(
          proof.capture.operation.request,
          baseInput.request.captureIntent,
          outcomeCode,
        ),
      outcomeCode,
    );
    const result = exactDataObject(
      proof.stop.operation.result,
      TERMINAL_RESULT_KEYS,
      outcomeCode,
    );
    ensure(
      result.resultVersion === 1 &&
        result.outcome === "writer-launch-stopped" &&
        sameContent(result.evidence, expectedEvidence, outcomeCode),
      outcomeCode,
    );
    const record = normalizeStopRecord(
      stop.record,
      proof.stop.operation,
      baseInput,
      outcomeCode,
    );
    const normalized = exactFrozenRecord({
      capture: proof.capture,
      session: proof.session,
      status: proof.capture.operation.state,
      stop: exactFrozenRecord({
        finalized: stop.finalized,
        operation: proof.stop.operation,
        record,
        reservation: proof.stop.reservation,
      }),
    });
    if (terminalRecord !== null) {
      normalizeSupervisorStateGcAuthorization(
        receipt.supervisorStateGcAuthorization,
        {
          launchAttemptId: baseInput.request.launch.launchAttemptId,
          sessionId: proof.stop.operation.sessionId,
          stateOwnerId: supervisor.stateOwnerId,
          terminalKind: WRITER_LAUNCH_STOP_OPERATION_KIND,
          terminalOperationId: proof.stop.operation.operationId,
          terminalRecord,
        },
        outcomeCode,
      );
    }
    return normalized;
  }

  async function finalizeStopCaptureWithReadback(
    baseInput,
    claimToken,
    evidence,
    terminalRecord,
  ) {
    let expectedOperationRevision = "1";
    for (
      let attempt = 0;
      attempt < STOP_FINALIZATION_MAX_ATTEMPTS;
      attempt += 1
    ) {
      const finalizationInput = exactFrozenRecord({
        ...baseInput,
        evidence,
        expectedOperationRevision,
        terminalRecord,
      });
      try {
        return normalizeStopCaptureHandoffReceipt(
          await invokeAsync(
            authority,
            "finalizeWriterLaunchStoppedAndReserveCheckpointCaptureAndAuthorizeSupervisorStateGc",
            [finalizationInput],
            outcomeCode,
          ),
          baseInput,
          claimToken,
          evidence,
          false,
          terminalRecord,
        );
      } catch {
        let readback;
        try {
          readback = snapshotData(
            await readStopOperationReceipt(baseInput, claimToken),
            outcomeCode,
          );
          try {
            const committed = normalizeStopCaptureHandoffReceipt(
              readback,
              baseInput,
              claimToken,
              evidence,
              true,
            );
            await readSupervisorStateGcAuthorization(
              authority,
              {
                launchAttemptId:
                  baseInput.request.launch.launchAttemptId,
                sessionId: committed.stop.operation.sessionId,
                stateOwnerId: supervisor.stateOwnerId,
                terminalKind: WRITER_LAUNCH_STOP_OPERATION_KIND,
                terminalOperationId: committed.stop.operation.operationId,
                terminalRecord,
              },
              outcomeCode,
            );
            return committed;
          } catch {
            const phase = normalizeStopReconcileReceipt(
              readback,
              baseInput,
              outcomeCode,
            );
            ensure(
              phase.claimTokenMatched === true &&
                (phase.status === "starting" ||
                  phase.status === "uncertain"),
              outcomeCode,
            );
            expectedOperationRevision =
              phase.status === "uncertain" ? "2" : "1";
          }
        } catch {
          // A later retry may still observe the exact committed handoff.
        }
      }
    }
    let terminalReadback;
    try {
      terminalReadback = snapshotData(
        await readStopOperationReceipt(baseInput, claimToken),
        outcomeCode,
      );
    } catch {
      fail(outcomeCode);
    }
    const committed = normalizeStopCaptureHandoffReceipt(
      terminalReadback,
      baseInput,
      claimToken,
      evidence,
      true,
    );
    await readSupervisorStateGcAuthorization(
      authority,
      {
        launchAttemptId: baseInput.request.launch.launchAttemptId,
        sessionId: committed.stop.operation.sessionId,
        stateOwnerId: supervisor.stateOwnerId,
        terminalKind: WRITER_LAUNCH_STOP_OPERATION_KIND,
        terminalOperationId: committed.stop.operation.operationId,
        terminalRecord,
      },
      outcomeCode,
    );
    return committed;
  }

  async function finalizeStopWithReadback(
    baseInput,
    claimToken,
    evidence,
    terminalRecord,
  ) {
    let expectedOperationRevision = "1";
    for (
      let attempt = 0;
      attempt < STOP_FINALIZATION_MAX_ATTEMPTS;
      attempt += 1
    ) {
      const finalizationInput = exactFrozenRecord({
        ...baseInput,
        evidence,
        expectedOperationRevision,
        terminalRecord,
      });
      try {
        return normalizeStopFinalizationGcReceipt(
          await invokeAsync(
            authority,
            "finalizeWriterLaunchStoppedAndAuthorizeSupervisorStateGc",
            [finalizationInput],
            outcomeCode,
          ),
          baseInput,
          evidence,
          terminalRecord,
          supervisor.stateOwnerId,
          outcomeCode,
        );
      } catch {
        try {
          const phaseReadback = snapshotData(
            await readStopOperationReceipt(baseInput, claimToken),
            outcomeCode,
          );
          const phase = normalizeStopReconcileReceipt(
            phaseReadback,
            baseInput,
            outcomeCode,
          );
          ensure(
            phase.claimTokenMatched === true &&
              (phase.status === "starting" ||
                phase.status === "uncertain"),
            outcomeCode,
          );
          expectedOperationRevision =
            phase.status === "uncertain" ? "2" : "1";
        } catch {
          // A committed receipt fails the active-phase normalizer and is
          // accepted only by the exact terminal relation below.
        }
      }
    }
    let terminalReadback;
    try {
      terminalReadback = snapshotData(
        await readStopOperationReceipt(baseInput, claimToken),
        outcomeCode,
      );
    } catch {
      fail(outcomeCode);
    }
    const committed = normalizeCommittedStopReadbackReceipt(
      terminalReadback,
      baseInput,
      claimToken,
      evidence,
      outcomeCode,
    );
    await readSupervisorStateGcAuthorization(
      authority,
      {
        launchAttemptId: baseInput.request.launch.launchAttemptId,
        sessionId: committed.operation.sessionId,
        stateOwnerId: supervisor.stateOwnerId,
        terminalKind: WRITER_LAUNCH_STOP_OPERATION_KIND,
        terminalOperationId: committed.operation.operationId,
        terminalRecord,
      },
      outcomeCode,
    );
    return committed;
  }

  function stopBaseInputForSession(
    expectedSession,
    record,
    stopOperation,
    claimToken,
    code,
  ) {
    let typedRequest;
    try {
      const captureIntent =
        record.stopContractVersion ===
        WRITER_LAUNCH_STOP_CAPTURE_HANDOFF_CONTRACT_VERSION
          ? createCheckpointCaptureOperationRequest({
              admission: exactFrozenRecord({
                attachment: record.authorizedCapture.attachment,
                captureAttemptId: record.authorizedCaptureAttemptId,
                checkpoint: record.authorizedCapture.checkpoint,
                processIncarnationId: record.processIncarnationId,
                request: record.authorizedCapture.request,
                stopOperationId: stopOperation,
                writerIncarnationId: record.writerIncarnationId,
              }),
              expectedSession,
            })
          : null;
      typedRequest = createWriterLaunchStopOperationRequest(
        captureIntent === null
          ? { claimToken, expectedSession }
          : { captureIntent, claimToken, expectedSession },
      );
    } catch {
      fail(code);
    }
    return exactFrozenRecord({
      expectedSession,
      kind: WRITER_LAUNCH_STOP_OPERATION_KIND,
      operationId: stopOperation,
      request: typedRequest,
    });
  }

  async function reserveStopOperation(baseInput) {
    return normalizeStopReserveReceipt(
      await invokeAsync(
        authority,
        "reserveOperation",
        [baseInput],
        outcomeCode,
      ),
      baseInput,
      outcomeCode,
    );
  }

  async function bestEffortMarkStopUncertain(baseInput, state) {
    if (state.attempted) return;
    state.attempted = true;
    try {
      await invokeAsync(
        authority,
        "markOperationUncertain",
        [
          exactFrozenRecord({
            ...baseInput,
            expectedOperationRevision: "1",
          }),
        ],
        outcomeCode,
      );
    } catch {
      // The unresolved durable stop operation remains the recovery authority.
    }
  }

  async function stopWriterForCaptureInternal(
    stopContractVersion,
    stopRoute,
    ...stopArgs
  ) {
    ensure(stopArgs.length === 1, optionCode);
    let atomicCrashRequest = null;
    let capture;
    if (stopRoute === CLEAN_CAPTURE_ROUTE) {
      capture = normalizeCaptureTuple(stopArgs[0], optionCode);
    } else {
      ensure(stopRoute === ATOMIC_CRASH_CAPTURE_ROUTE, optionCode);
      atomicCrashRequest = normalizeAtomicCrashCaptureRequest(
        stopArgs[0],
        optionCode,
      );
      capture = captureTupleForAtomicCrashRequest(atomicCrashRequest);
    }
    const acceptedRecordStates =
      stopContractVersion ===
      WRITER_LAUNCH_STOP_CAPTURE_HANDOFF_CONTRACT_VERSION
        ? ["ready", "stopped"]
        : ["ready"];
    const initialRecord = captureRecord(
      capture,
      acceptedRecordStates,
      optionCode,
      stopContractVersion,
      stopRoute,
      atomicCrashRequest,
    );
    const stopOperation = initialRecord.authorizedStopOperationId;
    const claimToken = initialRecord.stopClaimToken;
    ensure(
      typeof claimToken === "string" && regexpTest(UUID_PATTERN, claimToken),
      optionCode,
    );
    const uncertaintyState = { attempted: false };
    try {
      return await invokeGuard(
        operationGuard,
        [
          stopOperation,
          async (probeValue, completeValue) => {
            const probe = normalizeProbe(probeValue, outcomeCode);
            const complete = assertCallback(completeValue, outcomeCode);
            ensure(objectIsFrozen(completeValue), outcomeCode);
            const record = captureRecord(
              capture,
              acceptedRecordStates,
              optionCode,
              stopContractVersion,
              stopRoute,
              atomicCrashRequest,
            );
            ensure(record === initialRecord, optionCode);
            await assertGuardHeld(probe, outcomeCode);
            if (record.state === "stopped") {
              ensure(
                stopContractVersion ===
                  WRITER_LAUNCH_STOP_CAPTURE_HANDOFF_CONTRACT_VERSION,
                outcomeCode,
              );
              return callIntrinsic(complete, undefined, [
                replayPreparedCaptureHandoff(record, capture),
              ]);
            }
            let baseInput = record.stopBaseInput;
            const retainedBaseInput = baseInput !== null;
            if (baseInput === null) {
              const expectedSession = normalizeSession(
                await invokeAsync(
                  authority,
                  "readSession",
                  [
                    exactFrozenRecord({
                      sessionId: capture.attachment.sessionId,
                    }),
                  ],
                  outcomeCode,
                ),
                outcomeCode,
              );
              validateCurrentStopSession(expectedSession, record, outcomeCode);
              baseInput = stopBaseInputForSession(
                expectedSession,
                record,
                stopOperation,
                claimToken,
                optionCode,
              );
              record.stopBaseInput = baseInput;
            }
            ensure(
              baseInput.operationId === stopOperation &&
                sameContent(
                  baseInput.request.launch,
                  record.launch,
                  outcomeCode,
                ) &&
                stopClaimTokenMatchesRequest(
                  claimToken,
                  baseInput.request,
                  outcomeCode,
                ),
              outcomeCode,
            );

            let phase = null;
            if (retainedBaseInput) {
              phase = await reconcileStopOperation(baseInput, claimToken);
            }
            for (
              let attempt = 0;
              attempt < STOP_RESERVATION_MAX_ATTEMPTS;
              attempt += 1
            ) {
              if (
                phase?.status === "absent" &&
                !phase.expectedSessionMatched
              ) {
                validateCurrentStopSession(
                  phase.session,
                  record,
                  outcomeCode,
                );
                ensure(
                  canonicalTimestampMilliseconds(
                    phase.session.document.lease.expiresAt,
                    outcomeCode,
                  ) >=
                    canonicalTimestampMilliseconds(
                      baseInput.expectedSession.document.lease.expiresAt,
                      outcomeCode,
                    ),
                  outcomeCode,
                );
                baseInput = stopBaseInputForSession(
                  phase.session,
                  record,
                  stopOperation,
                  claimToken,
                  outcomeCode,
                );
                record.stopBaseInput = baseInput;
                phase = null;
              }
              if (phase !== null && phase.status !== "absent") break;
              try {
                phase = await reserveStopOperation(baseInput);
              } catch {
                phase = await reconcileStopOperation(baseInput, claimToken);
              }
            }
            ensure(phase !== null && phase.status !== "absent", outcomeCode);

            if (
              phase.status !== "prepared" &&
              phase.claimTokenMatched !== true
            ) {
              phase = await reconcileStopOperation(baseInput, claimToken);
            }

            if (phase.status === "prepared") {
              record.stopClaimAttemptedFor = stopOperation;
              let claimValue;
              let claimInvocationFailed = false;
              try {
                claimValue = await invokeAsync(
                  authority,
                  "claimWriterLaunchStopDispatch",
                  [
                    exactFrozenRecord({
                      ...baseInput,
                      claimToken,
                      expectedOperationRevision: "0",
                    }),
                  ],
                  outcomeCode,
                );
              } catch {
                claimInvocationFailed = true;
                phase = await reconcileStopOperation(baseInput, claimToken);
              }
              if (!claimInvocationFailed) {
                phase = normalizeStopClaimReceipt(
                  claimValue,
                  baseInput,
                  outcomeCode,
                );
              }
              if (
                phase.status === "prepared" ||
                phase.claimTokenMatched !== true
              ) {
                record.stopClaimAttemptedFor = null;
              }
            }
            const physicalStopAuthorized =
              // Protect claimant identity, not mere starting-state content:
              // the durable digest, authority match, and local attempted edge
              // must all identify this record's claim.
              phase.status === "starting" &&
              phase.claimTokenMatched === true &&
              record.stopClaimAttemptedFor === stopOperation &&
              stopClaimTokenMatchesRequest(
                claimToken,
                baseInput.request,
                outcomeCode,
              );
            if (!physicalStopAuthorized && phase.status !== "prepared") {
              record.state = "lost";
            }
            ensure(physicalStopAuthorized, outcomeCode);

            await assertGuardHeld(probe, outcomeCode);
            ensure(record.state === "ready", outcomeCode);
            record.pendingStop = exactFrozenRecord({ baseInput, probe });
            record.state = "stop-dispatch";
            let capability;
            try {
              capability = await invokeStoppedCoordinator(
                stoppedWriterCoordinator,
                stoppedStopAndIssueCapabilityIntrinsic,
                exactFrozenRecord({
                  processIncarnationId: record.processIncarnationId,
                  stopOperationId: stopOperation,
                  writer: record.writer,
                  writerIncarnationId: record.writerIncarnationId,
                }),
                outcomeCode,
              );
            } catch {
              if (record.state === "stop-dispatch") {
                record.pendingStop = null;
                record.state = "lost";
              }
              if (physicalStopAuthorized) {
                await bestEffortMarkStopUncertain(
                  baseInput,
                  uncertaintyState,
                );
              }
              fail(outcomeCode);
            }
            assertOpaqueWriterHandle(capability, outcomeCode);
            ensure(
              record.state === "stopped" &&
                record.stopEvidence !== null &&
                record.stopReceipt !== null,
              outcomeCode,
            );
            if (
              stopContractVersion ===
              WRITER_LAUNCH_STOP_CAPTURE_HANDOFF_CONTRACT_VERSION
            ) {
              invokeStoppedCoordinatorSync(
                stoppedWriterCoordinator,
                stoppedRevokeWriterIntrinsic,
                exactFrozenRecord({
                  processIncarnationId: record.processIncarnationId,
                  writer: record.writer,
                  writerIncarnationId: record.writerIncarnationId,
                }),
                outcomeCode,
              );
              return callIntrinsic(complete, undefined, [
                storePreparedCaptureHandoff(record, capture),
              ]);
            }
            const result = exactFrozenRecord({
              capability,
              evidence: record.stopEvidence,
              resolution: resolutionForRecord(record),
              stop: record.stopReceipt,
            });
            return callIntrinsic(complete, undefined, [result]);
          },
        ],
        outcomeCode,
      );
    } catch (error) {
      if (isInternalError(error, optionCode)) throw error;
      fail(outcomeCode);
    }
  }

  function resolveStoppedWriter(...resolverArgs) {
    ensure(resolverArgs.length === 1, optionCode);
    try {
      const capture = normalizeCaptureTuple(resolverArgs[0], optionCode);
      return resolutionForRecord(
        captureRecord(capture, ["ready", "stopped"], optionCode),
      );
    } catch (error) {
      if (isInternalError(error, optionCode)) throw error;
      fail(optionCode);
    }
  }

  function normalizeStopResolution(value, code) {
    let resolution;
    try {
      const input = exactDataObject(
        value,
        STOP_RESOLUTION_KEYS,
        code,
      );
      resolution = exactFrozenRecord({
        canonicalLeaseAtRegistration: assertLeaseGrant(
          input.canonicalLeaseAtRegistration,
        ),
        processIncarnationId: assertOpaqueId(
          input.processIncarnationId,
          code,
        ),
        stopOperationId: assertOpaqueId(input.stopOperationId, code),
        writer: assertOpaqueWriterHandle(input.writer, code),
        writerIncarnationId: assertOpaqueId(
          input.writerIncarnationId,
          code,
        ),
      });
    } catch (error) {
      if (isInternalError(error, code)) throw error;
      fail(code);
    }
    return resolution;
  }

  function retireRecord(record) {
    invokeStoppedCoordinatorSync(
      stoppedWriterCoordinator,
      stoppedRetireWriterIntrinsic,
      exactFrozenRecord({
        processIncarnationId: record.processIncarnationId,
        writer: record.writer,
        writerIncarnationId: record.writerIncarnationId,
      }),
      outcomeCode,
    );
    releaseStoppedRecord(record);
  }

  function stoppedRecordForResolution(
    resolution,
    expectedContractVersion,
    code,
  ) {
    const record = weakMapGet(recordsByWriter, resolution.writer);
    ensure(
      record !== undefined &&
        record.state === "stopped" &&
        record.stopReceipt !== null &&
        record.stopRoute === CLEAN_CAPTURE_ROUTE &&
        record.stopContractVersion === expectedContractVersion &&
        resolution.processIncarnationId === record.processIncarnationId &&
        resolution.writerIncarnationId === record.writerIncarnationId &&
        resolution.stopOperationId === record.authorizedStopOperationId &&
        sameContent(
          resolution.canonicalLeaseAtRegistration,
          record.canonicalLease,
          code,
        ),
      code,
    );
    return record;
  }

  function retireStoppedWriter(...retireArgs) {
    ensure(retireArgs.length === 1, optionCode);
    const resolution = normalizeStopResolution(retireArgs[0], optionCode);
    const record = stoppedRecordForResolution(
      resolution,
      WRITER_LAUNCH_STOP_CLAIM_CONTRACT_VERSION,
      optionCode,
    );
    retireRecord(record);
  }

  function retirePreparedCapture(...retireArgs) {
    ensure(retireArgs.length === 1, optionCode);
    let input;
    let resolution;
    let result;
    try {
      input = exactDataObject(
        retireArgs[0],
        PREPARED_CAPTURE_RETIREMENT_KEYS,
        optionCode,
      );
      resolution = normalizeStopResolution(input.resolution, optionCode);
      const record = stoppedRecordForResolution(
        resolution,
        WRITER_LAUNCH_STOP_CAPTURE_HANDOFF_CONTRACT_VERSION,
        optionCode,
      );
      ensure(record.authorizedCapture !== null, optionCode);
      const rawResult = exactDataObject(
        input.result,
        PREPARED_CAPTURE_RESULT_KEYS,
        optionCode,
      );
      const checkpoint = assertCheckpointDescriptor(rawResult.checkpoint);
      const mutation = assertStorageMutationResult(rawResult.mutation, {
        request: record.authorizedCapture.request,
      });
      ensure(
        sameContent(
          checkpoint,
          record.authorizedCapture.checkpoint,
          optionCode,
        ) &&
          sameContent(
            exactFrozenRecord({ checkpoint, mutation }),
            record.stopBaseInput.request.captureIntent.predeterminedResult,
            optionCode,
          ),
        optionCode,
      );
      result = exactFrozenRecord({ checkpoint, mutation });
      retireRecord(record);
    } catch (error) {
      if (isInternalError(error, optionCode)) throw error;
      fail(optionCode);
    }
    return result;
  }

  function atomicAuthorityRecordFor(
    captureAuthority,
    request,
    states,
    code,
  ) {
    const authority = assertOpaqueWriterHandle(captureAuthority, code);
    const authorityRecord = weakMapGet(
      atomicCrashCaptureAuthorities,
      authority,
    );
    ensure(
      authorityRecord !== undefined &&
        authorityRecord.captureAuthority === authority &&
        arrayIncludes(states, authorityRecord.state) &&
        sameContent(authorityRecord.request, request, code),
      code,
    );
    const record = authorityRecord.record;
    ensure(
      record.state === "stopped" &&
        record.stopReceipt !== null &&
        record.stopRoute === ATOMIC_CRASH_CAPTURE_ROUTE &&
        record.stopContractVersion ===
          WRITER_LAUNCH_STOP_CLAIM_CONTRACT_VERSION &&
        record.authorizedCapture === null &&
        record.authorizedAtomicCrashRequest !== null &&
        sameContent(record.authorizedAtomicCrashRequest, request, code) &&
        record.authorizedStopOperationId ===
          atomicCrashCaptureStopOperationId(
            request,
            record.launchAttemptId,
            code,
          ) &&
        mapGet(recordsByAttempt, record.launchAttemptId) === record &&
        mapGet(
          recordsByAttachmentId,
          record.attachment.attachmentId,
        ) === record &&
        weakMapGet(recordsByWriter, record.writer) === record,
      code,
    );
    return authorityRecord;
  }

  async function completeAtomicCrashStopInternal(...completeStopArgs) {
    ensure(completeStopArgs.length === 1, optionCode);
    const request = normalizeAtomicCrashCaptureRequest(
      completeStopArgs[0],
      optionCode,
    );
    const stopped = await stopWriterForCaptureInternal(
      WRITER_LAUNCH_STOP_CLAIM_CONTRACT_VERSION,
      ATOMIC_CRASH_CAPTURE_ROUTE,
      request,
    );
    const captureAuthority = assertOpaqueWriterHandle(
      stopped.capability,
      outcomeCode,
    );
    const record = weakMapGet(recordsByWriter, stopped.resolution.writer);
    ensure(
      record !== undefined &&
        record.state === "stopped" &&
        record.stopRoute === ATOMIC_CRASH_CAPTURE_ROUTE &&
        record.authorizedAtomicCrashRequest !== null &&
        sameContent(
          record.authorizedAtomicCrashRequest,
          request,
          outcomeCode,
        ) &&
        weakMapGet(
          atomicCrashCaptureAuthorities,
          captureAuthority,
        ) === undefined,
      outcomeCode,
    );
    weakMapSet(atomicCrashCaptureAuthorities, captureAuthority, {
      captureAuthority,
      consumedResult: null,
      record,
      request: record.authorizedAtomicCrashRequest,
      state: "issued",
    });
    return captureAuthority;
  }

  async function resolveAtomicCrashCaptureAuthorityInternal(
    ...resolveArgs
  ) {
    ensure(resolveArgs.length === 2, optionCode);
    const admission = exactDataObject(
      resolveArgs[0],
      ATOMIC_CRASH_CAPTURE_ADMISSION_KEYS,
      optionCode,
    );
    const request = normalizeAtomicCrashCaptureRequest(
      admission.request,
      optionCode,
    );
    const runCapture = assertSourceBackedFunction(resolveArgs[1], {
      asynchronous: true,
      code: optionCode,
    });
    ensure(objectIsFrozen(runCapture), optionCode);
    const authorityRecord = atomicAuthorityRecordFor(
      admission.captureAuthority,
      request,
      ["issued"],
      optionCode,
    );
    const record = authorityRecord.record;
    authorityRecord.state = "resolving";
    let callbackCalls = 0;
    const runSnapshot = async function runAtomicCrashCaptureSnapshot(
      ...callbackArgs
    ) {
      callbackCalls += 1;
      ensure(callbackArgs.length === 1 && callbackCalls === 1, outcomeCode);
      const binding = exactDataObject(
        callbackArgs[0],
        STOP_BINDING_KEYS,
        outcomeCode,
      );
      const writerFence = exactDataObject(
        binding.writerFence,
        WRITER_FENCE_KEYS,
        outcomeCode,
      );
      ensure(
        sameContent(
          binding.attachment,
          request.sourceAttachment,
          outcomeCode,
        ) &&
          binding.processIncarnationId === record.processIncarnationId &&
          binding.stopOperationId === record.authorizedStopOperationId &&
          binding.writerIncarnationId === record.writerIncarnationId &&
          writerFence.contractVersion ===
            record.canonicalLease.contractVersion &&
          writerFence.fencingEpoch === record.canonicalLease.fencingEpoch &&
          writerFence.holderId === record.canonicalLease.holderId &&
          writerFence.leaseId === record.canonicalLease.leaseId &&
          writerFence.sessionId === record.canonicalLease.sessionId,
        outcomeCode,
      );
      return await callIntrinsic(runCapture, undefined, []);
    };
    objectFreeze(runSnapshot);
    try {
      const result = await invokeStoppedCoordinator(
        stoppedWriterCoordinator,
        stoppedConsumeCapabilityIntrinsic,
        exactFrozenRecord({
          attachment: request.sourceAttachment,
          canonicalLease: record.canonicalLease,
          capability: authorityRecord.captureAuthority,
          processIncarnationId: record.processIncarnationId,
          runSnapshot,
          stopOperationId: record.authorizedStopOperationId,
          writer: record.writer,
          writerIncarnationId: record.writerIncarnationId,
        }),
        outcomeCode,
      );
      ensure(callbackCalls === 1, outcomeCode);
      try {
        authorityRecord.consumedResult = assertAtomicCrashCaptureResult(
          result,
          { request },
        );
      } catch {
        fail(outcomeCode);
      }
      authorityRecord.state = "consumed";
      return result;
    } catch {
      authorityRecord.state = "uncertain";
      fail(outcomeCode);
    }
  }

  function retireCompleteAtomicCrashStop(...retireArgs) {
    ensure(retireArgs.length === 1, optionCode);
    let input;
    let request;
    let result;
    try {
      input = exactDataObject(
        retireArgs[0],
        ATOMIC_CRASH_CAPTURE_RETIREMENT_KEYS,
        optionCode,
      );
      request = normalizeAtomicCrashCaptureRequest(input.request, optionCode);
      result = assertAtomicCrashCaptureResult(input.result, { request });
    } catch (error) {
      if (isInternalError(error, optionCode)) throw error;
      fail(optionCode);
    }
    const authorityRecord = atomicAuthorityRecordFor(
      input.captureAuthority,
      request,
      ["issued", "consumed"],
      optionCode,
    );
    const record = authorityRecord.record;
    ensure(
      authorityRecord.state === "issued"
        ? authorityRecord.consumedResult === null
        : authorityRecord.consumedResult !== null &&
            sameContent(
              authorityRecord.consumedResult,
              result,
              optionCode,
            ),
      optionCode,
    );
    const needsRevocation = authorityRecord.state === "issued";
    authorityRecord.state = "retiring";
    try {
      if (needsRevocation) {
        invokeStoppedCoordinatorSync(
          stoppedWriterCoordinator,
          stoppedRevokeWriterIntrinsic,
          exactFrozenRecord({
            processIncarnationId: record.processIncarnationId,
            writer: record.writer,
            writerIncarnationId: record.writerIncarnationId,
          }),
          outcomeCode,
        );
      }
      retireRecord(record);
      authorityRecord.state = "retired";
    } catch {
      authorityRecord.state = "uncertain";
      fail(outcomeCode);
    }
  }

  const prepareLaunchIntent = function prepareLaunchIntent(...prepareArgs) {
    return protectPromise(prepareLaunchIntentInternal(...prepareArgs));
  };
  const reconcileLaunchAttempt = function reconcileLaunchAttempt(
    ...reconcileArgs
  ) {
    return protectPromise(reconcileLaunchAttemptInternal(...reconcileArgs));
  };
  const runLaunch = function runLaunch(...runArgs) {
    return protectPromise(runLaunchInternal(...runArgs));
  };
  const runPreparedLaunch = function runPreparedLaunch(...runArgs) {
    return protectPromise(runPreparedLaunchInternal(...runArgs));
  };
  const stopWriterForCapture = function stopWriterForCapture(...stopArgs) {
    return protectPromise(
      stopWriterForCaptureInternal(
        WRITER_LAUNCH_STOP_CLAIM_CONTRACT_VERSION,
        CLEAN_CAPTURE_ROUTE,
        ...stopArgs,
      ),
    );
  };
  const stopWriterForPreparedCapture =
    function stopWriterForPreparedCapture(...stopArgs) {
      return protectPromise(
        stopWriterForCaptureInternal(
          WRITER_LAUNCH_STOP_CAPTURE_HANDOFF_CONTRACT_VERSION,
          CLEAN_CAPTURE_ROUTE,
          ...stopArgs,
        ),
      );
    };
  const completeStop = function completeStop(...completeStopArgs) {
    return protectPromise(
      completeAtomicCrashStopInternal(...completeStopArgs),
    );
  };
  const resolveCaptureAuthority = function resolveCaptureAuthority(
    ...resolveArgs
  ) {
    return protectPromise(
      resolveAtomicCrashCaptureAuthorityInternal(...resolveArgs),
    );
  };
  const retireCompleteStop = function retireCompleteStop(...retireArgs) {
    return retireCompleteAtomicCrashStop(...retireArgs);
  };
  objectFreeze(completeStop);
  objectFreeze(prepareLaunchIntent);
  objectFreeze(reconcileLaunchAttempt);
  objectFreeze(resolveCaptureAuthority);
  objectFreeze(retireCompleteStop);
  objectFreeze(retirePreparedCapture);
  objectFreeze(retireStoppedWriter);
  objectFreeze(resolveStoppedWriter);
  objectFreeze(runLaunch);
  objectFreeze(runPreparedLaunch);
  objectFreeze(stopWriterForCapture);
  objectFreeze(stopWriterForPreparedCapture);
  const atomicCrashCaptureFacet = exactFrozenRecord({
    completeStop,
    resolveCaptureAuthority,
    retireCompleteStop,
  });
  const facade = exactFrozenRecord({
    prepareLaunchIntent,
    reconcileLaunchAttempt,
    retirePreparedCapture,
    retireStoppedWriter,
    resolveStoppedWriter,
    runLaunch,
    runPreparedLaunch,
    stopWriterForCapture,
    stopWriterForPreparedCapture,
  });
  weakMapSet(ATOMIC_CRASH_CAPTURE_FACETS, facade, atomicCrashCaptureFacet);
  return facade;
}

/**
 * Returns the launcher-owned private complete-stop facet. Only a facade
 * created by this module is accepted; structural substitutes are rejected.
 */
export function getPostgresLogicalWriterAtomicCrashCaptureFacet(...args) {
  const code = "invalid_logical_writer_launch_request";
  ensure(args.length === 1, code);
  const launcher = args[0];
  ensure(
    launcher !== null &&
      typeof launcher === "object" &&
      !isProxyValue(launcher),
    code,
  );
  const facet = weakMapGet(ATOMIC_CRASH_CAPTURE_FACETS, launcher);
  ensure(facet !== undefined, code);
  return facet;
}

objectFreeze(PostgresLogicalWriterLauncherError.prototype);
objectFreeze(PostgresLogicalWriterLauncherError);
