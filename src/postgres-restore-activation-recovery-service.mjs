import { types as utilTypes } from "node:util";
import { isAbsolute, resolve } from "node:path";

import {
  assertCheckpointDescriptor,
  assertLeaseGrant,
  assertSessionAttachment,
  assertStorageMutationRequest,
} from "./session-storage-contracts.mjs";

const arrayIsArray = Array.isArray;
const arrayEveryIntrinsic = Array.prototype.every;
const arrayIncludesIntrinsic = Array.prototype.includes;
const arrayPushIntrinsic = Array.prototype.push;
const arrayPrototype = Array.prototype;
const bufferByteLength = Buffer.byteLength;
const DateConstructor = Date;
const dateParse = Date.parse;
const dateToISOStringIntrinsic = Date.prototype.toISOString;
const ErrorConstructor = Error;
const isGeneratorFunctionValue = utilTypes.isGeneratorFunction;
const isGeneratorObjectValue = utilTypes.isGeneratorObject;
const isPromiseValue = utilTypes.isPromise;
const isProxyValue = utilTypes.isProxy;
const numberIsFinite = Number.isFinite;
const numberIsSafeInteger = Number.isSafeInteger;
const objectCreate = Object.create;
const objectDefineProperty = Object.defineProperty;
const objectFreeze = Object.freeze;
const objectGetOwnPropertyDescriptor = Object.getOwnPropertyDescriptor;
const objectGetPrototypeOf = Object.getPrototypeOf;
const objectHasOwn = Object.hasOwn;
const objectIsFrozen = Object.isFrozen;
const objectIsPrototypeOfIntrinsic = Object.prototype.isPrototypeOf;
const objectPrototype = Object.prototype;
const PromiseConstructor = Promise;
const promisePrototype = Promise.prototype;
const promiseSpeciesSymbol = Symbol.species;
const promiseThenIntrinsic = Promise.prototype.then;
const reflectApply = Reflect.apply;
const reflectOwnKeys = Reflect.ownKeys;
const regexpExecIntrinsic = RegExp.prototype.exec;
const TypeErrorConstructor = TypeError;

const AbortSignalConstructor = globalThis.AbortSignal;
const abortSignalPrototype = AbortSignalConstructor.prototype;
const abortSignalAbortedGetter = objectGetOwnPropertyDescriptor(
  abortSignalPrototype,
  "aborted",
).get;

const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u;
const OPAQUE_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u;
const SHA256_PATTERN = /^[0-9a-f]{64}$/u;
const OCI_SHA256_PATTERN = /^sha256:[0-9a-f]{64}$/u;
const MAX_BATCH_SIZE = 100;
const MAX_JSON_DEPTH = 32;
const MAX_JSON_NODES = 8_192;
const MAX_JSON_BYTES = 262_144;
const MAX_ARRAY_ENTRIES = 1_024;
const MAX_OBJECT_KEYS = 256;

const OPTION_KEYS = objectFreeze([
  "listCurrentWriterLaunchCandidates",
  "listRestoreAttachmentActivationCandidates",
  "listRestoreGenerationCandidates",
  "listWriterLaunchAttemptCandidates",
  "reconcileRestoreAttachmentActivation",
  "reconcileRestoreGeneration",
  "reconcileWriterLaunchAttempt",
]);
const BATCH_REQUEST_KEYS = objectFreeze([
  "afterSessionId",
  "limit",
  "signal",
]);
const SWEEP_REQUEST_KEYS = objectFreeze([
  "activation",
  "currentLaunch",
  "generation",
  "launchAttempt",
  "signal",
]);
const SWEEP_LANE_KEYS = objectFreeze(["afterSessionId", "limit"]);
const PAGE_KEYS = objectFreeze(["candidates", "nextAfterSessionId"]);
const GENERATION_CANDIDATE_KEYS = objectFreeze([
  "checkpoint",
  "generationId",
  "request",
]);
const GENERATION_CANDIDATE_V2_KEYS = objectFreeze([
  "checkpoint",
  "generationId",
  "launchIntent",
  "request",
]);
const ACTIVATION_CANDIDATE_KEYS = objectFreeze([
  "activationOperationId",
  "request",
  "state",
]);
const ACTIVATION_REQUEST_KEYS = objectFreeze([
  "contractVersion",
  "destinationRootPath",
  "generation",
  "holderId",
  "launchIntent",
  "leaseDurationMilliseconds",
  "predecessor",
]);
const ACTIVATION_PREDECESSOR_KEYS = objectFreeze([
  "attachmentId",
  "detachOperationId",
  "stopOperationId",
]);
const LAUNCH_CANDIDATE_KEYS = objectFreeze([
  "launchAttemptId",
  "request",
  "state",
]);
const LAUNCH_REQUEST_KEYS = objectFreeze([
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
const LAUNCH_INTENT_KEYS = objectFreeze([
  "launchAttemptId",
  "measuredImage",
  "supervisor",
]);
const SUPERVISOR_KEYS = objectFreeze(["contractVersion", "supervisorId"]);
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
const CURRENT_LAUNCH_CANDIDATE_KEYS = objectFreeze([
  "launch",
  "launchAttemptId",
  "request",
]);

const ERROR_MESSAGES = objectFreeze({
  invalid_postgres_restore_activation_recovery_service_options:
    "PostgreSQL restore activation recovery service options are invalid",
  invalid_postgres_restore_activation_recovery_service_request:
    "PostgreSQL restore activation recovery service request is invalid",
  postgres_restore_activation_recovery_service_outcome_uncertain:
    "PostgreSQL restore activation recovery service outcome is uncertain",
});

function callIntrinsic(intrinsic, receiver, args) {
  return reflectApply(intrinsic, receiver, args);
}

function regexpTest(pattern, value) {
  return callIntrinsic(regexpExecIntrinsic, pattern, [value]) !== null;
}

function arrayIncludes(value, candidate) {
  return callIntrinsic(arrayIncludesIntrinsic, value, [candidate]);
}

function arrayPush(value, candidate) {
  return callIntrinsic(arrayPushIntrinsic, value, [candidate]);
}

function arrayEvery(value, callback) {
  return callIntrinsic(arrayEveryIntrinsic, value, [callback]);
}

function fail(code) {
  throw new PostgresRestoreActivationRecoveryServiceError(code);
}

function ensure(condition, code) {
  if (!condition) fail(code);
}

function exactDataObject(value, expectedKeys, code) {
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
    (prototype === objectPrototype || prototype === null) &&
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
    normalized[key] = descriptor.value;
  }
  return normalized;
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

function frozenArray(values) {
  const result = [];
  for (let index = 0; index < values.length; index += 1) {
    result[index] = values[index];
  }
  return objectFreeze(result);
}

function canonicalSessionId(value, code) {
  ensure(typeof value === "string" && regexpTest(UUID_PATTERN, value), code);
  return value;
}

function canonicalOpaqueId(value, code) {
  ensure(
    typeof value === "string" && regexpTest(OPAQUE_ID_PATTERN, value),
    code,
  );
  return value;
}

function canonicalSha256(value, code) {
  ensure(typeof value === "string" && regexpTest(SHA256_PATTERN, value), code);
  return value;
}

function canonicalTimestamp(value, code) {
  ensure(typeof value === "string", code);
  let milliseconds;
  let canonical;
  try {
    milliseconds = callIntrinsic(dateParse, DateConstructor, [value]);
    canonical = callIntrinsic(
      dateToISOStringIntrinsic,
      new DateConstructor(value),
      [],
    );
  } catch {
    fail(code);
  }
  ensure(
    numberIsFinite(milliseconds) && canonical === value,
    code,
  );
  return value;
}

function validateMeasuredImage(value, code) {
  const measuredImage = exactDataObject(value, MEASURED_IMAGE_KEYS, code);
  const projection = exactDataObject(
    measuredImage.projection,
    IMAGE_PROJECTION_KEYS,
    code,
  );
  const platformImage = exactDataObject(
    projection.platformImage,
    PLATFORM_IMAGE_KEYS,
    code,
  );
  const config = exactDataObject(
    platformImage.config,
    IMAGE_CONFIG_KEYS,
    code,
  );
  const runtimeIdentity = exactDataObject(
    measuredImage.runtimeIdentity,
    RUNTIME_IDENTITY_KEYS,
    code,
  );
  ensure(
    typeof projection.codexSandbox === "string" &&
      projection.codexSandbox.length >= 1 &&
      projection.codexSandbox.length <= 128 &&
      typeof projection.codexVersion === "string" &&
      projection.codexVersion.length >= 1 &&
      projection.codexVersion.length <= 128 &&
      typeof platformImage.architecture === "string" &&
      platformImage.architecture.length >= 1 &&
      platformImage.architecture.length <= 64 &&
      typeof platformImage.os === "string" &&
      platformImage.os.length >= 1 &&
      platformImage.os.length <= 64 &&
      typeof platformImage.mediaType === "string" &&
      platformImage.mediaType.length >= 1 &&
      platformImage.mediaType.length <= 256 &&
      typeof config.mediaType === "string" &&
      config.mediaType.length >= 1 &&
      config.mediaType.length <= 256 &&
      numberIsSafeInteger(platformImage.size) &&
      platformImage.size > 0 &&
      numberIsSafeInteger(config.size) &&
      config.size > 0 &&
      typeof platformImage.digest === "string" &&
      regexpTest(OCI_SHA256_PATTERN, platformImage.digest) &&
      typeof config.digest === "string" &&
      regexpTest(OCI_SHA256_PATTERN, config.digest) &&
      typeof runtimeIdentity.codexBinaryPath === "string" &&
      runtimeIdentity.codexBinaryPath.length > 1 &&
      runtimeIdentity.codexBinaryPath.length <= 4_096 &&
      isAbsolute(runtimeIdentity.codexBinaryPath) &&
      resolve(runtimeIdentity.codexBinaryPath) ===
        runtimeIdentity.codexBinaryPath &&
      canonicalSha256(runtimeIdentity.codexBinarySha256, code) ===
        runtimeIdentity.codexBinarySha256 &&
      runtimeIdentity.codexVersion === projection.codexVersion &&
      runtimeIdentity.platformImageDigest === platformImage.digest,
    code,
  );
}

function validateSupervisor(value, code) {
  const supervisor = exactDataObject(value, SUPERVISOR_KEYS, code);
  ensure(supervisor.contractVersion === 1, code);
  canonicalOpaqueId(supervisor.supervisorId, code);
}

function validateLaunchIntent(value, code) {
  const intent = exactDataObject(value, LAUNCH_INTENT_KEYS, code);
  canonicalOpaqueId(intent.launchAttemptId, code);
  validateMeasuredImage(intent.measuredImage, code);
  validateSupervisor(intent.supervisor, code);
}

function validateGenerationReference(value, code) {
  const generation = exactDataObject(value, GENERATION_REFERENCE_KEYS, code);
  const claimedAt = canonicalTimestamp(generation.claimedAt, code);
  const committedAt = canonicalTimestamp(generation.committedAt, code);
  const claimedAtMilliseconds = callIntrinsic(
    dateParse,
    DateConstructor,
    [claimedAt],
  );
  const committedAtMilliseconds = callIntrinsic(
    dateParse,
    DateConstructor,
    [committedAt],
  );
  ensure(
    generation.state === "committed" &&
      committedAtMilliseconds >= claimedAtMilliseconds,
    code,
  );
  canonicalSha256(generation.bindingSha256, code);
  canonicalSha256(generation.documentSha256, code);
  canonicalOpaqueId(generation.checkpointId, code);
  canonicalOpaqueId(generation.generationId, code);
  canonicalOpaqueId(generation.operationId, code);
  canonicalSessionId(generation.sessionId, code);
  return generation.sessionId;
}

function clonePlainJson(value, code) {
  const state = { bytes: 0, nodes: 0 };

  function accountString(candidate) {
    state.bytes += callIntrinsic(bufferByteLength, Buffer, [
      candidate,
      "utf8",
    ]);
    ensure(state.bytes <= MAX_JSON_BYTES, code);
    return candidate;
  }

  function visit(candidate, depth) {
    state.nodes += 1;
    ensure(
      depth <= MAX_JSON_DEPTH && state.nodes <= MAX_JSON_NODES,
      code,
    );
    if (candidate === null || typeof candidate === "boolean") {
      return candidate;
    }
    if (typeof candidate === "string") return accountString(candidate);
    if (typeof candidate === "number") {
      ensure(numberIsFinite(candidate), code);
      return candidate;
    }
    ensure(
      typeof candidate === "object" && !isProxyValue(candidate),
      code,
    );
    let prototype;
    let keys;
    try {
      prototype = objectGetPrototypeOf(candidate);
      keys = reflectOwnKeys(candidate);
    } catch {
      fail(code);
    }
    if (arrayIsArray(candidate)) {
      ensure(
        (prototype === arrayPrototype || prototype === null) &&
          candidate.length <= MAX_ARRAY_ENTRIES,
        code,
      );
      const expectedKeyCount = candidate.length + 1;
      ensure(
        keys.length === expectedKeyCount &&
          keys[keys.length - 1] === "length",
        code,
      );
      const result = [];
      for (let index = 0; index < candidate.length; index += 1) {
        const key = `${index}`;
        ensure(keys[index] === key, code);
        const descriptor = objectGetOwnPropertyDescriptor(candidate, key);
        ensure(
          descriptor?.enumerable === true &&
            objectHasOwn(descriptor, "value"),
          code,
        );
        result[index] = visit(descriptor.value, depth + 1);
      }
      return objectFreeze(result);
    }
    ensure(
      (prototype === objectPrototype || prototype === null) &&
        keys.length <= MAX_OBJECT_KEYS,
      code,
    );
    const result = objectCreate(null);
    for (let index = 0; index < keys.length; index += 1) {
      const key = keys[index];
      ensure(typeof key === "string", code);
      accountString(key);
      const descriptor = objectGetOwnPropertyDescriptor(candidate, key);
      ensure(
        descriptor?.enumerable === true && objectHasOwn(descriptor, "value"),
        code,
      );
      objectDefineProperty(result, key, {
        enumerable: true,
        value: visit(descriptor.value, depth + 1),
      });
    }
    return objectFreeze(result);
  }

  return visit(value, 0);
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
  if (descriptor.value === PromiseConstructor) return value;
  if (!isSafePromiseSpeciesHolder(descriptor.value)) return null;
  try {
    const normalized = callIntrinsic(promiseThenIntrinsic, value, [
      undefined,
      undefined,
    ]);
    objectDefineProperty(normalized, "constructor", {
      configurable: false,
      enumerable: false,
      value: PromiseConstructor,
      writable: false,
    });
    return normalized;
  } catch {
    return null;
  }
}

function signalIsAborted(signal, code) {
  if (signal === null) return false;
  ensure(
    signal !== null &&
      typeof signal === "object" &&
      !isProxyValue(signal) &&
      callIntrinsic(objectIsPrototypeOfIntrinsic, abortSignalPrototype, [
        signal,
      ]),
    code,
  );
  try {
    return callIntrinsic(abortSignalAbortedGetter, signal, []);
  } catch {
    fail(code);
  }
}

function normalizeLaneRequest(value, code) {
  const normalized = exactDataObject(value, SWEEP_LANE_KEYS, code);
  const afterSessionId =
    normalized.afterSessionId === null
      ? null
      : canonicalSessionId(normalized.afterSessionId, code);
  ensure(
    numberIsSafeInteger(normalized.limit) &&
      normalized.limit >= 1 &&
      normalized.limit <= MAX_BATCH_SIZE,
    code,
  );
  return exactFrozenRecord({ afterSessionId, limit: normalized.limit });
}

function normalizeBatchRequest(value, code) {
  const normalized = exactDataObject(value, BATCH_REQUEST_KEYS, code);
  const lane = normalizeLaneRequest(
    {
      afterSessionId: normalized.afterSessionId,
      limit: normalized.limit,
    },
    code,
  );
  signalIsAborted(normalized.signal, code);
  return exactFrozenRecord({
    afterSessionId: lane.afterSessionId,
    limit: lane.limit,
    signal: normalized.signal,
  });
}

function normalizeSweepRequest(value, code) {
  const normalized = exactDataObject(value, SWEEP_REQUEST_KEYS, code);
  signalIsAborted(normalized.signal, code);
  return exactFrozenRecord({
    activation: normalizeLaneRequest(normalized.activation, code),
    currentLaunch: normalizeLaneRequest(normalized.currentLaunch, code),
    generation: normalizeLaneRequest(normalized.generation, code),
    launchAttempt: normalizeLaneRequest(normalized.launchAttempt, code),
    signal: normalized.signal,
  });
}

function generationCandidate(value, code) {
  const raw = clonePlainJson(value, code);
  const hasLaunchIntent = objectHasOwn(raw, "launchIntent");
  const normalized = exactDataObject(
    raw,
    hasLaunchIntent
      ? GENERATION_CANDIDATE_V2_KEYS
      : GENERATION_CANDIDATE_KEYS,
    code,
  );
  let checkpoint;
  let request;
  try {
    checkpoint = assertCheckpointDescriptor(normalized.checkpoint);
    request = assertStorageMutationRequest(normalized.request);
  } catch {
    fail(code);
  }
  ensure(
    request.operation === "restore" &&
      request.sessionId === checkpoint.sessionId &&
      request.backendId === checkpoint.backendId &&
      request.storageId === checkpoint.storageId &&
      request.target.checkpointId === checkpoint.checkpointId &&
      request.target.artifactId === checkpoint.artifactId,
    code,
  );
  const candidate = exactFrozenRecord({
    checkpoint,
    generationId: canonicalOpaqueId(normalized.generationId, code),
    ...(hasLaunchIntent ? { launchIntent: normalized.launchIntent } : {}),
    request,
  });
  if (hasLaunchIntent) validateLaunchIntent(normalized.launchIntent, code);
  return exactFrozenRecord({
    candidate,
    operationId: request.operationId,
    sessionId: request.sessionId,
  });
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

function activationCandidate(value, code) {
  const raw = clonePlainJson(value, code);
  const normalized = exactDataObject(raw, ACTIVATION_CANDIDATE_KEYS, code);
  const request = exactDataObject(normalized.request, ACTIVATION_REQUEST_KEYS, code);
  ensure(
    normalized.state === "starting" || normalized.state === "uncertain",
    code,
  );
  ensure(
    request.contractVersion === 1 &&
      typeof request.destinationRootPath === "string" &&
      request.destinationRootPath.length > 1 &&
      request.destinationRootPath.length <= 4_096 &&
      isAbsolute(request.destinationRootPath) &&
      resolve(request.destinationRootPath) === request.destinationRootPath &&
      numberIsSafeInteger(request.leaseDurationMilliseconds) &&
      request.leaseDurationMilliseconds > 0 &&
      request.leaseDurationMilliseconds <= 86_400_000,
    code,
  );
  canonicalOpaqueId(request.holderId, code);
  validateLaunchIntent(request.launchIntent, code);
  const predecessor = exactDataObject(
    request.predecessor,
    ACTIVATION_PREDECESSOR_KEYS,
    code,
  );
  canonicalOpaqueId(predecessor.attachmentId, code);
  canonicalOpaqueId(predecessor.detachOperationId, code);
  canonicalOpaqueId(predecessor.stopOperationId, code);
  const sessionId = validateGenerationReference(request.generation, code);
  return exactFrozenRecord({
    candidate: exactFrozenRecord({
      activationOperationId: canonicalOpaqueId(
        normalized.activationOperationId,
        code,
      ),
      request: normalized.request,
      state: normalized.state,
    }),
    operationId: normalized.activationOperationId,
    sessionId,
  });
}

function launchCandidate(value, code, current) {
  const raw = clonePlainJson(value, code);
  const normalized = exactDataObject(
    raw,
    current ? CURRENT_LAUNCH_CANDIDATE_KEYS : LAUNCH_CANDIDATE_KEYS,
    code,
  );
  const request = exactDataObject(normalized.request, LAUNCH_REQUEST_KEYS, code);
  let lease;
  let attachment;
  try {
    lease = assertLeaseGrant(request.lease);
    attachment = assertSessionAttachment(request.attachment);
  } catch {
    fail(code);
  }
  const launchAttemptId = canonicalOpaqueId(
    normalized.launchAttemptId,
    code,
  );
  ensure(
    request.contractVersion === 1 &&
      request.fencingEpoch === lease.fencingEpoch &&
      lease.sessionId === attachment.sessionId &&
      lease.leaseId === attachment.leaseId &&
      lease.holderId === attachment.holderId &&
      lease.fencingEpoch === attachment.fencingEpoch,
    code,
  );
  ensure(validateGenerationReference(request.generation, code) === lease.sessionId, code);
  validateMeasuredImage(request.measuredImage, code);
  validateSupervisor(request.supervisor, code);
  if (!current) {
    ensure(
      normalized.state === "prepared" ||
        normalized.state === "starting" ||
        normalized.state === "uncertain",
      code,
    );
  } else {
    ensure(
      normalized.launch !== null &&
        typeof normalized.launch === "object" &&
        !arrayIsArray(normalized.launch),
      code,
    );
    const launchAttemptDescriptor = objectGetOwnPropertyDescriptor(
      normalized.launch,
      "launchAttemptId",
    );
    ensure(
      launchAttemptDescriptor?.enumerable === true &&
        objectHasOwn(launchAttemptDescriptor, "value") &&
        canonicalOpaqueId(launchAttemptDescriptor.value, code) ===
          launchAttemptId,
      code,
    );
  }
  return exactFrozenRecord({
    candidate: exactFrozenRecord({
      ...(current ? { launch: normalized.launch } : {}),
      launchAttemptId,
      request: normalized.request,
      ...(!current ? { state: normalized.state } : {}),
    }),
    operationId: launchAttemptId,
    sessionId: lease.sessionId,
  });
}

function normalizePage(value, request, kind, code) {
  const raw = exactDataObject(value, PAGE_KEYS, code);
  const rawCandidates = clonePlainJson(raw.candidates, code);
  ensure(
    arrayIsArray(rawCandidates) &&
      rawCandidates.length <= request.limit,
    code,
  );
  const normalizeCandidate = {
    activation: activationCandidate,
    currentLaunch: (candidate, candidateCode) =>
      launchCandidate(candidate, candidateCode, true),
    generation: generationCandidate,
    launchAttempt: (candidate, candidateCode) =>
      launchCandidate(candidate, candidateCode, false),
  }[kind];
  const candidates = [];
  let previousSessionId = request.afterSessionId;
  for (let index = 0; index < rawCandidates.length; index += 1) {
    const normalized = normalizeCandidate(rawCandidates[index], code);
    ensure(
      previousSessionId === null || normalized.sessionId > previousSessionId,
      code,
    );
    arrayPush(candidates, normalized);
    previousSessionId = normalized.sessionId;
  }
  const nextAfterSessionId =
    raw.nextAfterSessionId === null
      ? null
      : canonicalSessionId(raw.nextAfterSessionId, code);
  ensure(
    nextAfterSessionId === null ||
      (request.afterSessionId === null ||
        nextAfterSessionId > request.afterSessionId),
    code,
  );
  if (nextAfterSessionId !== null) {
    if (kind === "currentLaunch") {
      ensure(
        previousSessionId === null ||
          nextAfterSessionId >= previousSessionId,
        code,
      );
    } else {
      ensure(
        candidates.length === request.limit &&
          previousSessionId === nextAfterSessionId,
        code,
      );
    }
  }
  return exactFrozenRecord({
    candidates: frozenArray(candidates),
    nextAfterSessionId,
  });
}

async function callList(callback, request, kind, code) {
  let value;
  try {
    value = callIntrinsic(callback, undefined, [
      exactFrozenRecord({
        afterSessionId: request.afterSessionId,
        limit: request.limit,
      }),
    ]);
    if (isGeneratorObjectValue(value)) fail(code);
    if (isPromiseValue(value)) {
      value = normalizeSafeNativePromise(value);
      ensure(value !== null, code);
      value = await value;
    }
    if (isGeneratorObjectValue(value)) fail(code);
  } catch (error) {
    if (error instanceof PostgresRestoreActivationRecoveryServiceError) {
      throw error;
    }
    fail(code);
  }
  return normalizePage(value, request, kind, code);
}

async function reconcileCandidate(callback, candidate) {
  try {
    let pending = callIntrinsic(callback, undefined, [candidate]);
    if (isGeneratorObjectValue(pending)) return "pending";
    if (isPromiseValue(pending)) {
      pending = normalizeSafeNativePromise(pending);
      if (pending === null) return "pending";
    } else if (hasUntrustedThenableShape(pending)) {
      return "pending";
    }
    const value = isPromiseValue(pending) ? await pending : pending;
    if (isGeneratorObjectValue(value)) return "pending";
    return "reconciled";
  } catch {
    return "pending";
  }
}

function batchResult(afterSessionId, nextAfterSessionId, results, status) {
  return exactFrozenRecord({
    afterSessionId,
    nextAfterSessionId,
    results: frozenArray(results),
    status,
  });
}

function emptyAbortedBatch(request) {
  return batchResult(
    request.afterSessionId,
    request.afterSessionId,
    [],
    "aborted",
  );
}

export class PostgresRestoreActivationRecoveryServiceError extends ErrorConstructor {
  constructor(code) {
    if (typeof code !== "string" || !objectHasOwn(ERROR_MESSAGES, code)) {
      throw new TypeErrorConstructor(
        "Unsupported PostgreSQL restore activation recovery service error",
      );
    }
    const message = ERROR_MESSAGES[code];
    super(message);
    objectDefineProperty(this, "name", {
      enumerable: false,
      value: "PostgresRestoreActivationRecoveryServiceError",
    });
    objectDefineProperty(this, "code", { enumerable: true, value: code });
    objectDefineProperty(this, "retryable", {
      enumerable: true,
      value: false,
    });
    objectDefineProperty(this, "stack", {
      enumerable: false,
      value: `PostgresRestoreActivationRecoveryServiceError: ${message}`,
    });
    objectFreeze(this);
  }
}

export function createPostgresRestoreActivationRecoveryService(...args) {
  const optionCode =
    "invalid_postgres_restore_activation_recovery_service_options";
  ensure(args.length === 1, optionCode);
  const options = exactDataObject(args[0], OPTION_KEYS, optionCode);
  const callbacks = exactFrozenRecord({
    activation: exactFrozenRecord({
      list: normalizeCallback(
        options.listRestoreAttachmentActivationCandidates,
        optionCode,
      ),
      reconcile: normalizeCallback(
        options.reconcileRestoreAttachmentActivation,
        optionCode,
      ),
    }),
    currentLaunch: exactFrozenRecord({
      list: normalizeCallback(
        options.listCurrentWriterLaunchCandidates,
        optionCode,
      ),
      reconcile: null,
    }),
    generation: exactFrozenRecord({
      list: normalizeCallback(
        options.listRestoreGenerationCandidates,
        optionCode,
      ),
      reconcile: normalizeCallback(
        options.reconcileRestoreGeneration,
        optionCode,
      ),
    }),
    launchAttempt: exactFrozenRecord({
      list: normalizeCallback(
        options.listWriterLaunchAttemptCandidates,
        optionCode,
      ),
      reconcile: normalizeCallback(
        options.reconcileWriterLaunchAttempt,
        optionCode,
      ),
    }),
  });

  const requestCode =
    "invalid_postgres_restore_activation_recovery_service_request";
  const outcomeCode =
    "postgres_restore_activation_recovery_service_outcome_uncertain";
  let inFlight = false;

  async function runLane(kind, request) {
    if (signalIsAborted(request.signal, requestCode)) {
      return emptyAbortedBatch(request);
    }
    const lane = callbacks[kind];
    const page = await callList(
      lane.list,
      request,
      kind,
      outcomeCode,
    );
    if (signalIsAborted(request.signal, requestCode)) {
      return emptyAbortedBatch(request);
    }
    const results = [];
    let settledCursor = request.afterSessionId;
    for (let index = 0; index < page.candidates.length; index += 1) {
      if (signalIsAborted(request.signal, requestCode)) {
        return batchResult(
          request.afterSessionId,
          settledCursor,
          results,
          "aborted",
        );
      }
      const normalized = page.candidates[index];
      const status =
        kind === "currentLaunch"
          ? "requires-stop-or-fence"
          : await reconcileCandidate(lane.reconcile, normalized.candidate);
      arrayPush(
        results,
        exactFrozenRecord({
          operationId: normalized.operationId,
          sessionId: normalized.sessionId,
          status,
        }),
      );
      settledCursor = normalized.sessionId;
      if (signalIsAborted(request.signal, requestCode)) {
        return batchResult(
          request.afterSessionId,
          settledCursor,
          results,
          "aborted",
        );
      }
    }
    return batchResult(
      request.afterSessionId,
      page.nextAfterSessionId,
      results,
      page.nextAfterSessionId === null
        ? "sweep-complete"
        : "limit-reached",
    );
  }

  async function withFlight(callback) {
    ensure(!inFlight, outcomeCode);
    inFlight = true;
    try {
      return await callback();
    } finally {
      inFlight = false;
    }
  }

  function publicBatch(kind, argsValue) {
    ensure(argsValue.length === 1, requestCode);
    const request = normalizeBatchRequest(argsValue[0], requestCode);
    return withFlight(() => runLane(kind, request));
  }

  const runGenerationBatch = async function runGenerationBatch(...runArgs) {
    return publicBatch("generation", runArgs);
  };
  const runActivationBatch = async function runActivationBatch(...runArgs) {
    return publicBatch("activation", runArgs);
  };
  const runLaunchAttemptBatch = async function runLaunchAttemptBatch(
    ...runArgs
  ) {
    return publicBatch("launchAttempt", runArgs);
  };
  const scanCurrentLaunchBatch = async function scanCurrentLaunchBatch(
    ...runArgs
  ) {
    return publicBatch("currentLaunch", runArgs);
  };
  const runSweep = async function runSweep(...runArgs) {
    ensure(runArgs.length === 1, requestCode);
    const request = normalizeSweepRequest(runArgs[0], requestCode);
    return withFlight(async () => {
      const results = objectCreate(null);
      const laneOrder = [
        ["generation", "generation"],
        ["activation", "activation"],
        ["launchAttempt", "launchAttempt"],
        ["currentLaunch", "currentLaunch"],
      ];
      for (let index = 0; index < laneOrder.length; index += 1) {
        const [field, kind] = laneOrder[index];
        const lane = request[field];
        results[field] = await runLane(
          kind,
          exactFrozenRecord({
            afterSessionId: lane.afterSessionId,
            limit: lane.limit,
            signal: request.signal,
          }),
        );
      }
      const statuses = [
        results.generation.status,
        results.activation.status,
        results.launchAttempt.status,
        results.currentLaunch.status,
      ];
      const status = arrayIncludes(statuses, "aborted")
        ? "aborted"
        : arrayEvery(statuses, (value) => value === "sweep-complete")
          ? "sweep-complete"
          : "limit-reached";
      return exactFrozenRecord({
        activation: results.activation,
        currentLaunch: results.currentLaunch,
        generation: results.generation,
        launchAttempt: results.launchAttempt,
        status,
      });
    });
  };

  objectFreeze(runGenerationBatch);
  objectFreeze(runActivationBatch);
  objectFreeze(runLaunchAttemptBatch);
  objectFreeze(scanCurrentLaunchBatch);
  objectFreeze(runSweep);
  return exactFrozenRecord({
    runActivationBatch,
    runGenerationBatch,
    runLaunchAttemptBatch,
    runSweep,
    scanCurrentLaunchBatch,
  });
}
