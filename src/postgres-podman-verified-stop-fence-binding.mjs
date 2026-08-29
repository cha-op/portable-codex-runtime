import { Buffer } from "node:buffer";
import { types as utilTypes } from "node:util";

import {
  SESSION_OPERATION_CONFLICT_CLASS,
  WRITER_FORCE_FENCE_OPERATION_KIND,
  WRITER_LAUNCH_ATTEMPT_OPERATION_KIND,
  assertSessionAuthoritySnapshot,
  assertSessionOperationTransitionProof,
} from "./postgres-session-authority.mjs";
import {
  PODMAN_EXT4_VERIFIED_STOP_FENCE_PROVIDER_CONTRACT_VERSION,
  assertPodmanExt4VerifiedStopFenceBinding,
} from "./podman-ext4-verified-stop-fence-provider.mjs";
import {
  assertStorageForceFenceRequest,
} from "./session-storage-contracts.mjs";

const {
  isAsyncFunction,
  isGeneratorFunction,
  isGeneratorObject,
  isPromise,
  isProxy,
} = utilTypes;
const abortControllerSignalGetter = Object.getOwnPropertyDescriptor(
  AbortController.prototype,
  "signal",
).get;
const abortSignalAbortedGetter = Object.getOwnPropertyDescriptor(
  AbortSignal.prototype,
  "aborted",
).get;
const AbortControllerConstructor = AbortController;
const arrayIncludesIntrinsic = Array.prototype.includes;
const arrayIsArrayIntrinsic = Array.isArray;
const arrayJoinIntrinsic = Array.prototype.join;
const arrayPrototype = Array.prototype;
const arrayPushIntrinsic = Array.prototype.push;
const arraySortIntrinsic = Array.prototype.sort;
const BigIntConstructor = BigInt;
const bufferByteLengthIntrinsic = Buffer.byteLength;
const jsonStringifyIntrinsic = JSON.stringify;
const jsonObject = JSON;
const numberIsFiniteIntrinsic = Number.isFinite;
const objectCreateIntrinsic = Object.create;
const objectDefinePropertyIntrinsic = Object.defineProperty;
const objectFreezeIntrinsic = Object.freeze;
const objectGetOwnPropertyDescriptorIntrinsic = Object.getOwnPropertyDescriptor;
const objectGetPrototypeOfIntrinsic = Object.getPrototypeOf;
const objectHasOwnIntrinsic = Object.hasOwn;
const objectIsFrozenIntrinsic = Object.isFrozen;
const objectIsIntrinsic = Object.is;
const objectPrototype = Object.prototype;
const promisePrototype = Promise.prototype;
const reflectApplyIntrinsic = Reflect.apply;
const reflectOwnKeysIntrinsic = Reflect.ownKeys;
const regexpExecIntrinsic = RegExp.prototype.exec;
const StringConstructor = String;
const TypeErrorConstructor = TypeError;
const weakSetAddIntrinsic = WeakSet.prototype.add;
const weakSetHasIntrinsic = WeakSet.prototype.has;
const WeakSetConstructor = WeakSet;

export const POSTGRES_PODMAN_VERIFIED_STOP_FENCE_BINDING_CONTRACT_VERSION = 1;

const OPTION_KEYS = objectFreezeIntrinsic([
  "authority",
  "signalFactory",
  "stateOwnerId",
]);
const REQUIRED_OPTION_KEYS = objectFreezeIntrinsic([
  "authority",
  "stateOwnerId",
]);
const AUTHORITY_METHOD_KEYS = objectFreezeIntrinsic([
  "readWriterForceFenceProviderBinding",
  "readWriterLaunchAttempt",
]);
const FORCE_FENCE_BINDING_KEYS = objectFreezeIntrinsic([
  "expectedSession",
  "fenceRequest",
  "operationId",
  "writerEpoch",
]);
const LAUNCH_RECEIPT_KEYS = objectFreezeIntrinsic([
  "attempt",
  "launch",
  "operation",
  "reservation",
  "session",
  "status",
]);
const LAUNCH_ATTEMPT_KEYS = objectFreezeIntrinsic([
  "contractVersion",
  "launchAttemptId",
  "request",
  "result",
  "state",
]);
const MAX_PROTOTYPE_DEPTH = 64;
const MAX_CANONICAL_DEPTH = 64;
const MAX_CANONICAL_NODES = 32_768;
const MAX_CANONICAL_BYTES = 4 * 1024 * 1024;
const STATE_OWNER_ID_PATTERN = /^state-owner:[0-9a-f]{64}$/u;

const ERROR_MESSAGES = objectFreezeIntrinsic({
  invalid_postgres_podman_verified_stop_fence_binding_options:
    "PostgreSQL Podman verified-stop fence binding options are invalid",
  invalid_postgres_podman_verified_stop_fence_binding_request:
    "PostgreSQL Podman verified-stop fence binding request is invalid",
  postgres_podman_verified_stop_fence_binding_outcome_uncertain:
    "PostgreSQL Podman verified-stop fence binding outcome is uncertain",
});

const errorBrands = new WeakSetConstructor();

function callIntrinsic(intrinsic, receiver, args) {
  return reflectApplyIntrinsic(intrinsic, receiver, args);
}

function arrayIncludes(values, value) {
  return callIntrinsic(arrayIncludesIntrinsic, values, [value]);
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

export class PostgresPodmanVerifiedStopFenceBindingError extends TypeErrorConstructor {
  constructor(code) {
    if (typeof code !== "string" || !objectHasOwnIntrinsic(ERROR_MESSAGES, code)) {
      throw new TypeErrorConstructor(
        "Unsupported PostgreSQL Podman verified-stop fence binding error",
      );
    }
    const message = ERROR_MESSAGES[code];
    super(message);
    objectDefinePropertyIntrinsic(this, "code", {
      configurable: false,
      enumerable: true,
      value: code,
      writable: false,
    });
    objectDefinePropertyIntrinsic(this, "name", {
      configurable: false,
      enumerable: false,
      value: "PostgresPodmanVerifiedStopFenceBindingError",
      writable: false,
    });
    objectDefinePropertyIntrinsic(this, "retryable", {
      configurable: false,
      enumerable: true,
      value: false,
      writable: false,
    });
    objectDefinePropertyIntrinsic(this, "stack", {
      configurable: false,
      enumerable: false,
      value: `PostgresPodmanVerifiedStopFenceBindingError: ${message}`,
      writable: false,
    });
    objectFreezeIntrinsic(this);
    weakSetAdd(errorBrands, this);
  }
}

function fail(code) {
  throw new PostgresPodmanVerifiedStopFenceBindingError(code);
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

function exactFrozenRecord(values) {
  const result = objectCreateIntrinsic(null);
  const keys = reflectOwnKeysIntrinsic(values);
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
    const descriptor = objectGetOwnPropertyDescriptorIntrinsic(values, key);
    objectDefinePropertyIntrinsic(result, key, {
      configurable: false,
      enumerable: true,
      value: descriptor.value,
      writable: false,
    });
  }
  return objectFreezeIntrinsic(result);
}

function dataObject(
  value,
  allowedKeys,
  requiredKeys,
  code,
  { exact = false, frozen = false, nullPrototype = false } = {},
) {
  ensure(
    value !== null &&
      typeof value === "object" &&
      !arrayIsArrayIntrinsic(value) &&
      !isProxy(value),
    code,
  );
  let keys;
  let prototype;
  try {
    keys = reflectOwnKeysIntrinsic(value);
    prototype = objectGetPrototypeOfIntrinsic(value);
  } catch {
    fail(code);
  }
  ensure(
    (nullPrototype
      ? prototype === null
      : prototype === objectPrototype || prototype === null) &&
      (!frozen || objectIsFrozenIntrinsic(value)) &&
      (!exact || keys.length === allowedKeys.length),
    code,
  );
  const result = objectCreateIntrinsic(null);
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
    ensure(
      typeof key === "string" && arrayIncludes(allowedKeys, key),
      code,
    );
    let descriptor;
    try {
      descriptor = objectGetOwnPropertyDescriptorIntrinsic(value, key);
    } catch {
      fail(code);
    }
    ensure(
      descriptor?.enumerable === true &&
        objectHasOwnIntrinsic(descriptor, "value"),
      code,
    );
    objectDefinePropertyIntrinsic(result, key, {
      configurable: false,
      enumerable: true,
      value: descriptor.value,
      writable: false,
    });
  }
  for (let index = 0; index < requiredKeys.length; index += 1) {
    ensure(objectHasOwnIntrinsic(result, requiredKeys[index]), code);
  }
  return objectFreezeIntrinsic(result);
}

function exactDataObject(value, keys, code, options = {}) {
  return dataObject(value, keys, keys, code, { ...options, exact: true });
}

function validateAuthority(value, code) {
  ensure(
    value !== null &&
      typeof value === "object" &&
      !arrayIsArrayIntrinsic(value) &&
      !isProxy(value) &&
      !isPromise(value),
    code,
  );
  let cursor = value;
  const methods = objectCreateIntrinsic(null);
  for (let depth = 0; depth < MAX_PROTOTYPE_DEPTH; depth += 1) {
    ensure(cursor !== null && !isProxy(cursor), code);
    let next;
    let thenDescriptor;
    try {
      next = objectGetPrototypeOfIntrinsic(cursor);
      thenDescriptor = objectGetOwnPropertyDescriptorIntrinsic(cursor, "then");
    } catch {
      fail(code);
    }
    ensure(thenDescriptor === undefined, code);
    for (let index = 0; index < AUTHORITY_METHOD_KEYS.length; index += 1) {
      const name = AUTHORITY_METHOD_KEYS[index];
      if (objectHasOwnIntrinsic(methods, name)) continue;
      let descriptor;
      try {
        descriptor = objectGetOwnPropertyDescriptorIntrinsic(cursor, name);
      } catch {
        fail(code);
      }
      if (descriptor === undefined) continue;
      ensure(
        objectHasOwnIntrinsic(descriptor, "value") &&
          typeof descriptor.value === "function" &&
          !isProxy(descriptor.value) &&
          !isGeneratorFunction(descriptor.value),
        code,
      );
      methods[name] = descriptor.value;
    }
    if (cursor === objectPrototype) {
      ensure(next === null, code);
      break;
    }
    if (next === null) break;
    cursor = next;
  }
  for (let index = 0; index < AUTHORITY_METHOD_KEYS.length; index += 1) {
    ensure(objectHasOwnIntrinsic(methods, AUTHORITY_METHOD_KEYS[index]), code);
  }
  return objectFreezeIntrinsic(methods);
}

function isSafeNativePromise(value) {
  if (!isPromise(value) || isProxy(value) || isGeneratorObject(value)) {
    return false;
  }
  try {
    return (
      objectGetPrototypeOfIntrinsic(value) === promisePrototype &&
      objectGetOwnPropertyDescriptorIntrinsic(value, "catch") === undefined &&
      objectGetOwnPropertyDescriptorIntrinsic(value, "constructor") ===
        undefined &&
      objectGetOwnPropertyDescriptorIntrinsic(value, "finally") === undefined &&
      objectGetOwnPropertyDescriptorIntrinsic(value, "then") === undefined
    );
  } catch {
    return false;
  }
}

async function invokeAuthority(method, authority, input, code) {
  let pending;
  try {
    pending = callIntrinsic(method, authority, [input]);
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

function canonicalValue(value, parts, state, depth = 0) {
  ensure(
    depth <= MAX_CANONICAL_DEPTH && state.nodes < MAX_CANONICAL_NODES,
    state.code,
  );
  state.nodes += 1;
  if (value === null) {
    callIntrinsic(arrayPushIntrinsic, parts, ["null"]);
    return;
  }
  if (typeof value === "boolean") {
    callIntrinsic(arrayPushIntrinsic, parts, [value ? "true" : "false"]);
    return;
  }
  if (typeof value === "string") {
    callIntrinsic(arrayPushIntrinsic, parts, [
      callIntrinsic(jsonStringifyIntrinsic, jsonObject, [value]),
    ]);
    return;
  }
  if (typeof value === "number") {
    ensure(
      numberIsFiniteIntrinsic(value) && !objectIsIntrinsic(value, -0),
      state.code,
    );
    callIntrinsic(arrayPushIntrinsic, parts, [StringConstructor(value)]);
    return;
  }
  ensure(
    typeof value === "object" && !isProxy(value) && !isPromise(value),
    state.code,
  );
  ensure(!weakSetHas(state.seen, value), state.code);
  weakSetAdd(state.seen, value);
  let keys;
  let prototype;
  try {
    keys = reflectOwnKeysIntrinsic(value);
    prototype = objectGetPrototypeOfIntrinsic(value);
  } catch {
    fail(state.code);
  }
  if (arrayIsArrayIntrinsic(value)) {
    ensure(
      (prototype === arrayPrototype || prototype === null) &&
        keys.length === value.length + 1 &&
        arrayIncludes(keys, "length"),
      state.code,
    );
    callIntrinsic(arrayPushIntrinsic, parts, ["["]);
    for (let index = 0; index < value.length; index += 1) {
      const key = StringConstructor(index);
      let descriptor;
      try {
        descriptor = objectGetOwnPropertyDescriptorIntrinsic(value, key);
      } catch {
        fail(state.code);
      }
      ensure(
        descriptor?.enumerable === true &&
          objectHasOwnIntrinsic(descriptor, "value"),
        state.code,
      );
      if (index !== 0) callIntrinsic(arrayPushIntrinsic, parts, [","]);
      canonicalValue(descriptor.value, parts, state, depth + 1);
    }
    callIntrinsic(arrayPushIntrinsic, parts, ["]"]);
  } else {
    ensure(prototype === objectPrototype || prototype === null, state.code);
    for (let index = 0; index < keys.length; index += 1) {
      ensure(typeof keys[index] === "string" && keys[index] !== "then", state.code);
    }
    callIntrinsic(arraySortIntrinsic, keys, []);
    callIntrinsic(arrayPushIntrinsic, parts, ["{"]);
    for (let index = 0; index < keys.length; index += 1) {
      const key = keys[index];
      let descriptor;
      try {
        descriptor = objectGetOwnPropertyDescriptorIntrinsic(value, key);
      } catch {
        fail(state.code);
      }
      ensure(
        descriptor?.enumerable === true &&
          objectHasOwnIntrinsic(descriptor, "value"),
        state.code,
      );
      if (index !== 0) callIntrinsic(arrayPushIntrinsic, parts, [","]);
      callIntrinsic(arrayPushIntrinsic, parts, [
        callIntrinsic(jsonStringifyIntrinsic, jsonObject, [key]),
        ":",
      ]);
      canonicalValue(descriptor.value, parts, state, depth + 1);
    }
    callIntrinsic(arrayPushIntrinsic, parts, ["}"]);
  }
  state.seen = new WeakSetConstructor();
}

function canonicalSerialize(value, code) {
  const parts = [];
  canonicalValue(
    value,
    parts,
    { code, nodes: 0, seen: new WeakSetConstructor() },
  );
  const serialized = callIntrinsic(arrayJoinIntrinsic, parts, [""]);
  ensure(
    callIntrinsic(bufferByteLengthIntrinsic, Buffer, [serialized, "utf8"]) <=
      MAX_CANONICAL_BYTES,
    code,
  );
  return serialized;
}

function sameCanonical(left, right, code) {
  return canonicalSerialize(left, code) === canonicalSerialize(right, code);
}

function normalizedForceFenceBinding(value, request, code) {
  const receipt = exactDataObject(
    value,
    FORCE_FENCE_BINDING_KEYS,
    code,
    { frozen: true, nullPrototype: true },
  );
  let expectedSession;
  let fenceRequest;
  try {
    expectedSession = assertSessionAuthoritySnapshot(receipt.expectedSession);
    fenceRequest = assertStorageForceFenceRequest(receipt.fenceRequest);
  } catch {
    fail(code);
  }
  const document = expectedSession.document;
  const attachment = document.attachment;
  const lease = document.lease;
  const launch = document.launch;
  ensure(
    receipt.operationId === request.operationId &&
      receipt.writerEpoch === request.fencingEpoch &&
      sameCanonical(fenceRequest, request, code) &&
      document.lifecycle === "ATTACHED" &&
      document.activeOperation === null &&
      document.backendCapabilities.fencing !== "manual" &&
      attachment !== null &&
      lease !== null &&
      launch !== null &&
      expectedSession.sessionId === request.sessionId &&
      document.storageRef.backendId === request.backendId &&
      document.storageRef.storageId === request.storageId &&
      document.storageRef.sessionId === request.sessionId &&
      document.writerEpoch === request.revokedFence.fencingEpoch &&
      BigIntConstructor(request.fencingEpoch) ===
        BigIntConstructor(document.writerEpoch) + 1n &&
      attachment.backendId === request.backendId &&
      attachment.storageId === request.storageId &&
      attachment.sessionId === request.sessionId &&
      attachment.attachmentId === request.target.attachmentId &&
      attachment.fencingEpoch === request.revokedFence.fencingEpoch &&
      attachment.holderId === request.revokedFence.holderId &&
      attachment.leaseId === request.revokedFence.leaseId &&
      lease.sessionId === request.sessionId &&
      lease.fencingEpoch === request.revokedFence.fencingEpoch &&
      lease.holderId === request.revokedFence.holderId &&
      lease.leaseId === request.revokedFence.leaseId &&
      launch.attachmentId === request.target.attachmentId &&
      launch.fencingEpoch === request.revokedFence.fencingEpoch &&
      launch.leaseId === request.revokedFence.leaseId,
    code,
  );
  return exactFrozenRecord({ expectedSession, fenceRequest, launch });
}

function validateCurrentFenceSession(session, expectedSession, request, code) {
  const document = session.document;
  const expectedDocument = expectedSession.document;
  const active = document.activeOperation;
  ensure(
    session.sessionId === expectedSession.sessionId &&
      session.createdAt === expectedSession.createdAt &&
      document.lifecycle === "FENCING" &&
      document.writerEpoch === request.fencingEpoch &&
      active !== null &&
      active.conflictClass === SESSION_OPERATION_CONFLICT_CLASS &&
      active.kind === WRITER_FORCE_FENCE_OPERATION_KIND &&
      active.operationId === request.operationId &&
      active.expectedSessionRevision === expectedSession.revision &&
      ((active.state === "starting" && active.operationRevision === "1") ||
        (active.state === "uncertain" && active.operationRevision === "2")) &&
      BigIntConstructor(session.revision) ===
        BigIntConstructor(expectedSession.revision) +
          (active.state === "starting" ? 2n : 3n) &&
      sameCanonical(document.attachment, expectedDocument.attachment, code) &&
      sameCanonical(
        document.backendCapabilities,
        expectedDocument.backendCapabilities,
        code,
      ) &&
      sameCanonical(document.lastOperation, expectedDocument.lastOperation, code) &&
      sameCanonical(document.launch, expectedDocument.launch, code) &&
      sameCanonical(document.lease, expectedDocument.lease, code) &&
      sameCanonical(document.manifest, expectedDocument.manifest, code) &&
      sameCanonical(document.recovery, expectedDocument.recovery, code) &&
      sameCanonical(document.storageRef, expectedDocument.storageRef, code),
    code,
  );
}

function normalizedLaunchBinding(value, fence, request, stateOwnerId, code) {
  const receipt = exactDataObject(value, LAUNCH_RECEIPT_KEYS, code, {
    frozen: true,
  });
  const attempt = exactDataObject(receipt.attempt, LAUNCH_ATTEMPT_KEYS, code, {
    frozen: true,
  });
  let currentSession;
  let transition;
  try {
    currentSession = assertSessionAuthoritySnapshot(receipt.session);
    transition = assertSessionOperationTransitionProof({
      operation: receipt.operation,
      reservation: receipt.reservation,
      session: fence.expectedSession,
    });
  } catch {
    fail(code);
  }
  validateCurrentFenceSession(
    currentSession,
    fence.expectedSession,
    request,
    code,
  );
  const operation = transition.operation;
  const reservation = transition.reservation;
  ensure(
    receipt.status === "committed" &&
      attempt.contractVersion === 1 &&
      attempt.launchAttemptId === fence.launch.launchAttemptId &&
      attempt.state === "committed" &&
      attempt.result !== null &&
      attempt.result.outcome === "writer-launch-started" &&
      operation.kind === WRITER_LAUNCH_ATTEMPT_OPERATION_KIND &&
      operation.operationId === fence.launch.launchAttemptId &&
      operation.sessionId === request.sessionId &&
      operation.state === "committed" &&
      operation.result !== null &&
      operation.result.outcome === "writer-launch-started" &&
      reservation.operationId === operation.operationId &&
      reservation.sessionId === operation.sessionId &&
      reservation.state === "released" &&
      reservation.releasedAt !== null &&
      sameCanonical(attempt.request, operation.request, code) &&
      sameCanonical(attempt.result, operation.result, code) &&
      sameCanonical(receipt.launch, fence.launch, code) &&
      sameCanonical(currentSession.document.launch, fence.launch, code),
    code,
  );
  try {
    return assertPodmanExt4VerifiedStopFenceBinding(
      exactFrozenRecord({
        contractVersion:
          PODMAN_EXT4_VERIFIED_STOP_FENCE_PROVIDER_CONTRACT_VERSION,
        launch: fence.launch,
        request: operation.request,
        result: operation.result,
        stateOwnerId,
      }),
    );
  } catch {
    fail(code);
  }
}

function defaultSignalFactory() {
  const controller = new AbortControllerConstructor();
  return callIntrinsic(abortControllerSignalGetter, controller, []);
}

objectFreezeIntrinsic(defaultSignalFactory);

function validateSignal(value, issuedSignals, code) {
  ensure(
    value !== null &&
      typeof value === "object" &&
      !isProxy(value) &&
      !weakSetHas(issuedSignals, value),
    code,
  );
  try {
    callIntrinsic(abortSignalAbortedGetter, value, []);
  } catch {
    fail(code);
  }
  weakSetAdd(issuedSignals, value);
  return value;
}

/**
 * Resolves one force-fence request to the exact committed Podman writer
 * incarnation retained by PostgreSQL. The resolver neither dispatches the
 * stop nor keeps a process-local copy of durable launch evidence.
 */
export function createPostgresPodmanVerifiedStopFenceBindingResolver(...args) {
  const optionCode =
    "invalid_postgres_podman_verified_stop_fence_binding_options";
  ensure(args.length === 1, optionCode);
  const options = dataObject(
    args[0],
    OPTION_KEYS,
    REQUIRED_OPTION_KEYS,
    optionCode,
  );
  const authority = options.authority;
  const authorityMethods = validateAuthority(authority, optionCode);
  ensure(
    typeof options.stateOwnerId === "string" &&
      regexpTest(STATE_OWNER_ID_PATTERN, options.stateOwnerId),
    optionCode,
  );
  const stateOwnerId = options.stateOwnerId;
  const signalFactory = objectHasOwnIntrinsic(options, "signalFactory")
    ? options.signalFactory
    : defaultSignalFactory;
  ensure(
    typeof signalFactory === "function" &&
      !isProxy(signalFactory) &&
      !isGeneratorFunction(signalFactory) &&
      !isAsyncFunction(signalFactory),
    optionCode,
  );
  const issuedSignals = new WeakSetConstructor();
  const outcomeCode =
    "postgres_podman_verified_stop_fence_binding_outcome_uncertain";

  const resolveFenceBinding = async function resolveFenceBinding(value) {
    ensure(
      arguments.length === 1,
      "invalid_postgres_podman_verified_stop_fence_binding_request",
    );
    let request;
    try {
      request = assertStorageForceFenceRequest(value);
    } catch {
      fail("invalid_postgres_podman_verified_stop_fence_binding_request");
    }
    try {
      const forceReceipt = await invokeAuthority(
        authorityMethods.readWriterForceFenceProviderBinding,
        authority,
        exactFrozenRecord({ operationId: request.operationId }),
        outcomeCode,
      );
      const fence = normalizedForceFenceBinding(
        forceReceipt,
        request,
        outcomeCode,
      );
      const launchReceipt = await invokeAuthority(
        authorityMethods.readWriterLaunchAttempt,
        authority,
        exactFrozenRecord({
          operationId: fence.launch.launchAttemptId,
          stateOwnerId,
        }),
        outcomeCode,
      );
      const binding = normalizedLaunchBinding(
        launchReceipt,
        fence,
        request,
        stateOwnerId,
        outcomeCode,
      );
      let signal;
      try {
        signal = callIntrinsic(signalFactory, undefined, []);
      } catch {
        fail(outcomeCode);
      }
      signal = validateSignal(signal, issuedSignals, outcomeCode);
      return exactFrozenRecord({ binding, signal });
    } catch (error) {
      if (isInternalError(error)) throw error;
      fail(outcomeCode);
    }
  };
  objectFreezeIntrinsic(resolveFenceBinding);
  return resolveFenceBinding;
}

objectFreezeIntrinsic(PostgresPodmanVerifiedStopFenceBindingError.prototype);
objectFreezeIntrinsic(PostgresPodmanVerifiedStopFenceBindingError);
objectFreezeIntrinsic(createPostgresPodmanVerifiedStopFenceBindingResolver);
