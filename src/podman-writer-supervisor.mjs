import { Buffer } from "node:buffer";
import { spawn, spawnSync } from "node:child_process";
import { Hash, createHash } from "node:crypto";
import {
  closeSync,
  constants as fsConstants,
  fstatSync,
  lstatSync,
  openSync,
  realpathSync,
  statSync,
} from "node:fs";
import { isAbsolute, resolve } from "node:path";
import { types as utilTypes } from "node:util";

import {
  assertLeaseGrant,
  assertSessionAttachment,
} from "./session-storage-contracts.mjs";
import {
  SESSION_OPERATION_CONFLICT_CLASS,
  WRITER_LAUNCH_ATTEMPT_OPERATION_KIND,
} from "./postgres-session-authority.mjs";
import {
  PODMAN_WRITER_SUPERVISOR_STATE_CONTRACT_VERSION,
  assertPodmanWriterSupervisorStateRecord,
} from "./podman-writer-supervisor-state.mjs";

const { isGeneratorFunction, isPromise, isProxy } = utilTypes;
const arrayEveryIntrinsic = Array.prototype.every;
const arrayIncludesIntrinsic = Array.prototype.includes;
const arrayIsArrayIntrinsic = Array.isArray;
const arrayPushIntrinsic = Array.prototype.push;
const arraySliceIntrinsic = Array.prototype.slice;
const arraySortIntrinsic = Array.prototype.sort;
const bufferAllocIntrinsic = Buffer.alloc;
const bufferByteLengthIntrinsic = Buffer.byteLength;
const bufferConcatIntrinsic = Buffer.concat;
const bufferFromIntrinsic = Buffer.from;
const bufferIsBufferIntrinsic = Buffer.isBuffer;
const bufferToStringIntrinsic = Buffer.prototype.toString;
const dateParseIntrinsic = Date.parse;
const dateToISOStringIntrinsic = Date.prototype.toISOString;
const DateConstructor = Date;
const jsonParseIntrinsic = JSON.parse;
const jsonStringifyIntrinsic = JSON.stringify;
const JsonObject = JSON;
const hashDigestIntrinsic = Hash.prototype.digest;
const hashUpdateIntrinsic = Hash.prototype.update;
const numberIsFiniteIntrinsic = Number.isFinite;
const numberIsSafeIntegerIntrinsic = Number.isSafeInteger;
const objectAssignIntrinsic = Object.assign;
const objectCreateIntrinsic = Object.create;
const objectDefinePropertiesIntrinsic = Object.defineProperties;
const objectFreezeIntrinsic = Object.freeze;
const objectGetOwnPropertyDescriptorIntrinsic = Object.getOwnPropertyDescriptor;
const objectGetPrototypeOfIntrinsic = Object.getPrototypeOf;
const objectHasOwnIntrinsic = Object.hasOwn;
const objectIsFrozenIntrinsic = Object.isFrozen;
const objectIsIntrinsic = Object.is;
const objectPrototype = Object.prototype;
const PromiseConstructor = Promise;
const promisePrototype = Promise.prototype;
const reflectApplyIntrinsic = Reflect.apply;
const reflectOwnKeysIntrinsic = Reflect.ownKeys;
const regexpExecIntrinsic = RegExp.prototype.exec;
const stringEndsWithIntrinsic = String.prototype.endsWith;
const stringIncludesIntrinsic = String.prototype.includes;
const stringSliceIntrinsic = String.prototype.slice;
const stringStartsWithIntrinsic = String.prototype.startsWith;
const stringTrimIntrinsic = String.prototype.trim;
const StringConstructor = String;
const weakSetAddIntrinsic = WeakSet.prototype.add;
const weakSetHasIntrinsic = WeakSet.prototype.has;
const currentPlatform = process.platform;
const currentProcessId = process.pid;
const currentUserId = typeof process.getuid === "function" ? process.getuid() : null;
const currentUserIdBigInt = currentUserId === null ? null : BigInt(currentUserId);
const realpathNativeIntrinsic = realpathSync.native;
const openDirectoryFlag = fsConstants.O_DIRECTORY;
const readOnlyFlag = fsConstants.O_RDONLY;
const noFollowFlag = fsConstants.O_NOFOLLOW;
const fileTypeMask = BigInt(fsConstants.S_IFMT);
const directoryFileType = BigInt(fsConstants.S_IFDIR);

function callIntrinsic(intrinsic, receiver, arguments_) {
  return reflectApplyIntrinsic(intrinsic, receiver, arguments_);
}

function arrayEvery(value, callback) {
  return callIntrinsic(arrayEveryIntrinsic, value, [callback]);
}

function arrayIncludes(value, candidate) {
  return callIntrinsic(arrayIncludesIntrinsic, value, [candidate]);
}

function arrayPush(value, item) {
  return callIntrinsic(arrayPushIntrinsic, value, [item]);
}

function arrayPushTwo(value, first, second) {
  return callIntrinsic(arrayPushIntrinsic, value, [first, second]);
}

function regexpTest(pattern, value) {
  return callIntrinsic(regexpExecIntrinsic, pattern, [value]) !== null;
}

function stringIncludes(value, candidate) {
  return callIntrinsic(stringIncludesIntrinsic, value, [candidate]);
}

export const PODMAN_WRITER_SUPERVISOR_CONTRACT_VERSION = 2;

const OPAQUE_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u;
const OCI_DIGEST_PATTERN = /^sha256:[0-9a-f]{64}$/u;
const IMAGE_REFERENCE_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._/:@-]{0,511}$/u;
const SHA256_PATTERN = /^[0-9a-f]{64}$/u;
const CONTAINER_ID_PATTERN = /^[0-9a-f]{12,64}$/u;
const FULL_CONTAINER_ID_PATTERN = /^[0-9a-f]{64}$/u;
const PROC_FD_SOURCE_PATTERN = /^\/proc\/[1-9][0-9]*\/fd\/[0-9]+$/u;
const ENVIRONMENT_NAME_PATTERN = /^[A-Za-z_][A-Za-z0-9_]{0,63}$/u;
const CODEX_VERSION_PATTERN = /^[A-Za-z0-9][A-Za-z0-9.+_-]{0,127}$/u;
const MAX_DATA_DEPTH = 32;
const MAX_DATA_NODES = 32_768;
const MAX_CANONICAL_BYTES = 4 * 1024 * 1024;

const EMPTY_KEYS = Object.freeze([]);
const OPTION_KEYS = Object.freeze([
  "commandRunner",
  "commandTimeoutMilliseconds",
  "configuredAttachmentRoot",
  "filesystemAuthority",
  "images",
  "maxOutputBytes",
  "podmanEnvironment",
  "podmanExecutable",
  "state",
  "stopTimeoutSeconds",
  "supervisorId",
  "writerCommand",
  "writerEnvironment",
]);
const REQUIRED_OPTION_KEYS = Object.freeze([
  "configuredAttachmentRoot",
  "images",
  "podmanEnvironment",
  "podmanExecutable",
  "state",
  "supervisorId",
  "writerCommand",
  "writerEnvironment",
]);
const IMAGE_POLICY_KEYS = Object.freeze([
  "architecture",
  "codexVersion",
  "imageReference",
  "os",
]);
const STATE_KEYS = Object.freeze(["claim", "contractVersion", "read", "transition"]);
const FILESYSTEM_AUTHORITY_KEYS = Object.freeze([
  "acquire",
  "close",
  "contractVersion",
  "verifyCurrent",
  "verifyRunningMount",
]);
const FILESYSTEM_ACQUISITION_KEYS = Object.freeze(["handle", "mountSource"]);
const RUNNER_RESULT_KEYS = Object.freeze(["stderr", "stdout"]);
const LAUNCH_KEYS = Object.freeze([
  "attempt",
  "authorityNow",
  "consumedImage",
  "contractVersion",
  "generation",
  "invocation",
  "operation",
  "reservation",
  "session",
  "signal",
]);
const RECONCILE_KEYS = Object.freeze([
  "attempt",
  "contractVersion",
  "invocation",
  "launch",
  "operation",
  "reservation",
  "session",
  "signal",
]);
const STOP_KEYS = Object.freeze([
  "attachment",
  "contractVersion",
  "invocation",
  "processIncarnationId",
  "signal",
  "stopOperationId",
  "writerFence",
  "writerIncarnationId",
]);
const ATTEMPT_KEYS = Object.freeze([
  "contractVersion",
  "launchAttemptId",
  "request",
  "result",
  "state",
]);
const TYPED_REQUEST_KEYS = Object.freeze([
  "attachment",
  "contractVersion",
  "fencingEpoch",
  "generation",
  "lease",
  "measuredImage",
  "supervisor",
]);
const GENERATION_REFERENCE_KEYS = Object.freeze([
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
const GENERATION_SNAPSHOT_KEYS = Object.freeze([
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
const MEASURED_IMAGE_KEYS = Object.freeze(["projection", "runtimeIdentity"]);
const IMAGE_PROJECTION_KEYS = Object.freeze([
  "codexSandbox",
  "codexVersion",
  "platformImage",
]);
const PLATFORM_IMAGE_KEYS = Object.freeze([
  "architecture",
  "config",
  "digest",
  "mediaType",
  "os",
  "size",
]);
const IMAGE_CONFIG_KEYS = Object.freeze(["digest", "mediaType", "size"]);
const RUNTIME_IDENTITY_KEYS = Object.freeze([
  "codexBinaryPath",
  "codexBinarySha256",
  "codexVersion",
  "platformImageDigest",
]);
const SUPERVISOR_IDENTITY_KEYS = Object.freeze([
  "contractVersion",
  "supervisorId",
]);
const OPERATION_KEYS = Object.freeze([
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
const RESERVATION_KEYS = Object.freeze([
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
const SESSION_KEYS = Object.freeze([
  "createdAt",
  "document",
  "revision",
  "sessionId",
  "updatedAt",
]);
const SESSION_DOCUMENT_KEYS = Object.freeze([
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
const WRITER_FENCE_KEYS = Object.freeze([
  "contractVersion",
  "fencingEpoch",
  "holderId",
  "leaseId",
  "sessionId",
]);
const STATE_CLAIM_RECEIPT_KEYS = Object.freeze(["created", "record"]);
const ALLOWED_PODMAN_ENVIRONMENT = Object.freeze([
  "HOME",
  "LANG",
  "LC_ALL",
  "TMPDIR",
  "XDG_CONFIG_HOME",
  "XDG_DATA_HOME",
  "XDG_RUNTIME_DIR",
]);
const ERROR_MESSAGES = Object.freeze({
  invalid_podman_writer_supervisor_options:
    "Podman writer supervisor options are invalid",
  invalid_podman_writer_supervisor_request:
    "Podman writer supervisor request is invalid",
  podman_writer_attachment_mismatch:
    "The attachment root object identity or POSIX access policy changed",
  podman_writer_attachment_missing:
    "The attachment root is missing",
  podman_writer_attachment_revalidation_failed:
    "The attachment root or live bind could not be read and revalidated",
  podman_writer_image_mismatch:
    "The configured local Podman image does not match the requested digest and runtime",
  podman_writer_output_invalid:
    "Podman returned malformed or contradictory bounded output",
  podman_writer_rootless_required:
    "Podman rootless execution could not be proved",
  podman_writer_state_conflict:
    "Podman writer supervisor durable state conflicts with the request",
  podman_writer_supervisor_aborted:
    "Podman writer supervisor invocation was aborted",
  podman_writer_supervisor_outcome_uncertain:
    "Podman writer supervisor outcome is uncertain",
});

const errorBrands = new WeakSet();
const abortSignalAbortedGetter = Object.getOwnPropertyDescriptor(
  AbortSignal.prototype,
  "aborted",
)?.get;

export class PodmanWriterSupervisorError extends Error {
  constructor(code) {
    if (typeof code !== "string" || !objectHasOwnIntrinsic(ERROR_MESSAGES, code)) {
      throw new TypeError("unsupported Podman writer supervisor error code");
    }
    super(ERROR_MESSAGES[code]);
    callIntrinsic(objectDefinePropertiesIntrinsic, Object, [this, {
      code: { enumerable: true, value: code },
      name: { enumerable: true, value: "PodmanWriterSupervisorError" },
      retryable: { enumerable: true, value: false },
      stack: {
        configurable: false,
        enumerable: false,
        value: `PodmanWriterSupervisorError: ${ERROR_MESSAGES[code]}`,
        writable: false,
      },
    }]);
    callIntrinsic(weakSetAddIntrinsic, errorBrands, [this]);
    callIntrinsic(objectFreezeIntrinsic, Object, [this]);
  }
}

function fail(code) {
  throw new PodmanWriterSupervisorError(code);
}

function ensure(condition, code = "invalid_podman_writer_supervisor_request") {
  if (!condition) fail(code);
}

function frozenRecord(value) {
  return callIntrinsic(objectFreezeIntrinsic, Object, [
    callIntrinsic(objectAssignIntrinsic, Object, [
      callIntrinsic(objectCreateIntrinsic, Object, [null]),
      value,
    ]),
  ]);
}

function frozenFunction(value) {
  return callIntrinsic(objectFreezeIntrinsic, Object, [value]);
}

function dataObject(value, allowedKeys, requiredKeys, code) {
  ensure(
    value !== null &&
      typeof value === "object" &&
      !arrayIsArrayIntrinsic(value) &&
      !isProxy(value),
    code,
  );
  let prototype;
  let keys;
  try {
    prototype = callIntrinsic(objectGetPrototypeOfIntrinsic, Object, [value]);
    keys = reflectOwnKeysIntrinsic(value);
  } catch {
    fail(code);
  }
  ensure(
    (prototype === objectPrototype || prototype === null) &&
      arrayEvery(
        keys,
        (key) => typeof key === "string" && arrayIncludes(allowedKeys, key),
      ) &&
      arrayEvery(requiredKeys, (key) => arrayIncludes(keys, key)),
    code,
  );
  const result = callIntrinsic(objectCreateIntrinsic, Object, [null]);
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
    let descriptor;
    try {
      descriptor = callIntrinsic(objectGetOwnPropertyDescriptorIntrinsic, Object, [value, key]);
    } catch {
      fail(code);
    }
    ensure(
      descriptor?.enumerable === true && objectHasOwnIntrinsic(descriptor, "value"),
      code,
    );
    result[key] = descriptor.value;
  }
  return result;
}

function exactDataObject(value, keys, code) {
  const result = dataObject(value, keys, keys, code);
  ensure(reflectOwnKeysIntrinsic(result).length === keys.length, code);
  return result;
}

function assertOpaqueId(value, code) {
  ensure(typeof value === "string" && regexpTest(OPAQUE_ID_PATTERN, value), code);
  return value;
}

function assertSha256(value, code) {
  ensure(typeof value === "string" && regexpTest(SHA256_PATTERN, value), code);
  return value;
}

function assertIsoInstant(value, code) {
  ensure(
    typeof value === "string" &&
      numberIsFiniteIntrinsic(callIntrinsic(dateParseIntrinsic, DateConstructor, [value])) &&
      callIntrinsic(dateToISOStringIntrinsic, new DateConstructor(value), []) === value,
    code,
  );
  return value;
}

function assertInvocation(value, code) {
  const invocation = exactDataObject(value, EMPTY_KEYS, code);
  ensure(
    callIntrinsic(objectGetPrototypeOfIntrinsic, Object, [value]) === null &&
      objectIsFrozenIntrinsic(value),
    code,
  );
  return invocation;
}

function assertAbortSignal(value, code) {
  ensure(typeof abortSignalAbortedGetter === "function", code);
  try {
    callIntrinsic(abortSignalAbortedGetter, value, []);
  } catch {
    fail(code);
  }
  return value;
}

function signalAborted(signal) {
  return callIntrinsic(abortSignalAbortedGetter, signal, []);
}

function ensureNotAborted(signal) {
  if (signalAborted(signal)) fail("podman_writer_supervisor_aborted");
}

function canonicalData(value, state, depth = 0) {
  ensure(depth <= MAX_DATA_DEPTH, "invalid_podman_writer_supervisor_request");
  state.nodes += 1;
  ensure(
    state.nodes <= MAX_DATA_NODES,
    "invalid_podman_writer_supervisor_request",
  );
  if (value === null || typeof value === "boolean" || typeof value === "string") {
    return value;
  }
  if (typeof value === "number") {
    ensure(
      numberIsFiniteIntrinsic(value) && !objectIsIntrinsic(value, -0),
      "invalid_podman_writer_supervisor_request",
    );
    return value;
  }
  ensure(
    typeof value === "object" && !isProxy(value),
    "invalid_podman_writer_supervisor_request",
  );
  if (arrayIsArrayIntrinsic(value)) {
    ensure(value.length <= 4096, "invalid_podman_writer_supervisor_request");
    const keys = reflectOwnKeysIntrinsic(value);
    ensure(
      keys.length === value.length + 1 && arrayIncludes(keys, "length"),
      "invalid_podman_writer_supervisor_request",
    );
    const result = [];
    for (let index = 0; index < value.length; index += 1) {
      const descriptor = callIntrinsic(objectGetOwnPropertyDescriptorIntrinsic, Object, [
        value,
        StringConstructor(index),
      ]);
      ensure(
        descriptor?.enumerable === true && objectHasOwnIntrinsic(descriptor, "value"),
        "invalid_podman_writer_supervisor_request",
      );
      arrayPush(result, canonicalData(descriptor.value, state, depth + 1));
    }
    return result;
  }
  const prototype = callIntrinsic(objectGetPrototypeOfIntrinsic, Object, [value]);
  ensure(
    prototype === objectPrototype || prototype === null,
    "invalid_podman_writer_supervisor_request",
  );
  const keys = reflectOwnKeysIntrinsic(value);
  ensure(
    keys.length <= 256 && arrayEvery(keys, (key) => typeof key === "string"),
    "invalid_podman_writer_supervisor_request",
  );
  callIntrinsic(arraySortIntrinsic, keys, []);
  const result = callIntrinsic(objectCreateIntrinsic, Object, [null]);
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
    const descriptor = callIntrinsic(objectGetOwnPropertyDescriptorIntrinsic, Object, [value, key]);
    ensure(
      descriptor?.enumerable === true && objectHasOwnIntrinsic(descriptor, "value"),
      "invalid_podman_writer_supervisor_request",
    );
    result[key] = canonicalData(descriptor.value, state, depth + 1);
  }
  return result;
}

function canonicalJson(value) {
  let serialized;
  try {
    serialized = callIntrinsic(jsonStringifyIntrinsic, JsonObject, [
      canonicalData(value, { nodes: 0 }),
    ]);
  } catch (error) {
    if (callIntrinsic(weakSetHasIntrinsic, errorBrands, [error])) throw error;
    fail("invalid_podman_writer_supervisor_request");
  }
  ensure(
    typeof serialized === "string" &&
      callIntrinsic(bufferByteLengthIntrinsic, Buffer, [serialized, "utf8"]) <=
        MAX_CANONICAL_BYTES,
    "invalid_podman_writer_supervisor_request",
  );
  return serialized;
}

function sha256Parts(...parts) {
  const hash = createHash("sha256");
  for (let index = 0; index < parts.length; index += 1) {
    callIntrinsic(hashUpdateIntrinsic, hash, [parts[index], "utf8"]);
  }
  return callIntrinsic(hashDigestIntrinsic, hash, ["hex"]);
}

function requestDigest(request) {
  return sha256Parts(
    "portable-codex-runtime:podman-writer-request:v1\0",
    canonicalJson(request),
  );
}

function sameData(left, right) {
  return canonicalJson(left) === canonicalJson(right);
}

function normalizeMeasuredImage(value, code) {
  const measured = exactDataObject(value, MEASURED_IMAGE_KEYS, code);
  const projection = exactDataObject(measured.projection, IMAGE_PROJECTION_KEYS, code);
  const platformImage = exactDataObject(
    projection.platformImage,
    PLATFORM_IMAGE_KEYS,
    code,
  );
  const config = exactDataObject(platformImage.config, IMAGE_CONFIG_KEYS, code);
  const runtime = exactDataObject(
    measured.runtimeIdentity,
    RUNTIME_IDENTITY_KEYS,
    code,
  );
  ensure(
    typeof projection.codexSandbox === "string" &&
      typeof projection.codexVersion === "string" &&
      typeof platformImage.architecture === "string" &&
      typeof platformImage.os === "string" &&
      typeof platformImage.mediaType === "string" &&
      numberIsSafeIntegerIntrinsic(platformImage.size) &&
      platformImage.size >= 0 &&
      typeof config.mediaType === "string" &&
      numberIsSafeIntegerIntrinsic(config.size) &&
      config.size >= 0 &&
      regexpTest(OCI_DIGEST_PATTERN, platformImage.digest) &&
      regexpTest(OCI_DIGEST_PATTERN, config.digest) &&
      typeof runtime.codexBinaryPath === "string" &&
      assertSha256(runtime.codexBinarySha256, code) === runtime.codexBinarySha256 &&
      runtime.codexVersion === projection.codexVersion &&
      runtime.platformImageDigest === platformImage.digest,
    code,
  );
  return frozenRecord({ measured, platformImage, projection, runtime });
}

function normalizeTypedRequest(value, supervisorId, code) {
  const request = exactDataObject(value, TYPED_REQUEST_KEYS, code);
  let attachment;
  let lease;
  try {
    attachment = assertSessionAttachment(request.attachment);
    lease = assertLeaseGrant(request.lease);
  } catch {
    fail(code);
  }
  const generation = exactDataObject(
    request.generation,
    GENERATION_REFERENCE_KEYS,
    code,
  );
  const supervisor = exactDataObject(
    request.supervisor,
    SUPERVISOR_IDENTITY_KEYS,
    code,
  );
  const image = normalizeMeasuredImage(request.measuredImage, code);
  ensure(
    request.contractVersion === 1 &&
      request.fencingEpoch === lease.fencingEpoch &&
      attachment.sessionId === lease.sessionId &&
      attachment.leaseId === lease.leaseId &&
      attachment.holderId === lease.holderId &&
      attachment.fencingEpoch === lease.fencingEpoch &&
      generation.state === "committed" &&
      generation.sessionId === lease.sessionId &&
      assertOpaqueId(generation.checkpointId, code) === generation.checkpointId &&
      assertOpaqueId(generation.generationId, code) === generation.generationId &&
      assertOpaqueId(generation.operationId, code) === generation.operationId &&
      assertSha256(generation.bindingSha256, code) === generation.bindingSha256 &&
      assertSha256(generation.documentSha256, code) === generation.documentSha256 &&
      supervisor.contractVersion === 1 &&
      supervisor.supervisorId === supervisorId,
    code,
  );
  return frozenRecord({ attachment, generation, image, lease, request });
}

function normalizeCommon(input, supervisorId, states, code) {
  const attempt = exactDataObject(input.attempt, ATTEMPT_KEYS, code);
  const operation = exactDataObject(input.operation, OPERATION_KEYS, code);
  const reservation = exactDataObject(input.reservation, RESERVATION_KEYS, code);
  const session = exactDataObject(input.session, SESSION_KEYS, code);
  const sessionDocument = exactDataObject(
    session.document,
    SESSION_DOCUMENT_KEYS,
    code,
  );
  const typed = normalizeTypedRequest(attempt.request, supervisorId, code);
  ensure(
    attempt.contractVersion === 1 &&
      assertOpaqueId(attempt.launchAttemptId, code) === attempt.launchAttemptId &&
      arrayIncludes(states, attempt.state) &&
      attempt.result === null &&
      operation.conflictClass === SESSION_OPERATION_CONFLICT_CLASS &&
      operation.kind === WRITER_LAUNCH_ATTEMPT_OPERATION_KIND &&
      operation.operationId === attempt.launchAttemptId &&
      operation.state === attempt.state &&
      operation.result === null &&
      sameData(operation.request, attempt.request) &&
      assertSha256(operation.requestSha256, code) === operation.requestSha256 &&
      reservation.operationId === attempt.launchAttemptId &&
      reservation.conflictClass === SESSION_OPERATION_CONFLICT_CLASS &&
      reservation.kind === WRITER_LAUNCH_ATTEMPT_OPERATION_KIND &&
      reservation.state === attempt.state &&
      reservation.requestSha256 === operation.requestSha256 &&
      reservation.sessionId === operation.sessionId &&
      session.sessionId === typed.lease.sessionId &&
      sessionDocument.lifecycle === "ATTACHED" &&
      sameData(sessionDocument.attachment, typed.attachment) &&
      sameData(sessionDocument.lease, typed.lease),
    code,
  );
  assertInvocation(input.invocation, code);
  const signal = assertAbortSignal(input.signal, code);
  canonicalJson(attempt);
  canonicalJson(operation);
  canonicalJson(reservation);
  canonicalJson(session);
  return frozenRecord({ attempt, operation, reservation, session, signal, typed });
}

function normalizeLaunchInput(value, supervisorId) {
  const code = "invalid_podman_writer_supervisor_request";
  const input = exactDataObject(value, LAUNCH_KEYS, code);
  ensure(input.contractVersion === PODMAN_WRITER_SUPERVISOR_CONTRACT_VERSION, code);
  const common = normalizeCommon(input, supervisorId, ["starting"], code);
  assertIsoInstant(input.authorityNow, code);
  const consumed = normalizeMeasuredImage(input.consumedImage, code);
  ensure(sameData(consumed.measured, common.typed.image.measured), code);
  const generation = exactDataObject(
    input.generation,
    GENERATION_SNAPSHOT_KEYS,
    code,
  );
  ensure(
    generation.state === "committed" &&
      generation.binding !== null &&
      generation.document !== null &&
      generation.checkpointId === common.typed.generation.checkpointId &&
      generation.claimedAt === common.typed.generation.claimedAt &&
      generation.committedAt === common.typed.generation.committedAt &&
      generation.generationId === common.typed.generation.generationId &&
      generation.operationId === common.typed.generation.operationId &&
      generation.sessionId === common.typed.generation.sessionId,
    code,
  );
  canonicalJson(generation.binding);
  canonicalJson(generation.document);
  return frozenRecord({ ...common, consumed, generation });
}

function normalizeReconcileInput(value, supervisorId) {
  const code = "invalid_podman_writer_supervisor_request";
  const input = exactDataObject(value, RECONCILE_KEYS, code);
  ensure(
    input.contractVersion === PODMAN_WRITER_SUPERVISOR_CONTRACT_VERSION &&
      input.launch === null,
    code,
  );
  return normalizeCommon(input, supervisorId, ["starting", "uncertain"], code);
}

function normalizeStopInput(value, expected) {
  const code = "invalid_podman_writer_supervisor_request";
  const input = exactDataObject(value, STOP_KEYS, code);
  ensure(input.contractVersion === PODMAN_WRITER_SUPERVISOR_CONTRACT_VERSION, code);
  assertInvocation(input.invocation, code);
  const signal = assertAbortSignal(input.signal, code);
  let attachment;
  try {
    attachment = assertSessionAttachment(input.attachment);
  } catch {
    fail(code);
  }
  const fence = exactDataObject(input.writerFence, WRITER_FENCE_KEYS, code);
  ensure(
    sameData(attachment, expected.attachment) &&
      input.processIncarnationId === expected.processIncarnationId &&
      input.writerIncarnationId === expected.writerIncarnationId &&
      assertOpaqueId(input.stopOperationId, code) === input.stopOperationId &&
      fence.contractVersion === expected.lease.contractVersion &&
      fence.sessionId === expected.lease.sessionId &&
      fence.leaseId === expected.lease.leaseId &&
      fence.holderId === expected.lease.holderId &&
      fence.fencingEpoch === expected.lease.fencingEpoch,
    code,
  );
  return frozenRecord({ signal, stopOperationId: input.stopOperationId });
}

function normalizeStringRecord(value, allowedKeys, code) {
  const record = dataObject(value, allowedKeys, EMPTY_KEYS, code);
  const keys = reflectOwnKeysIntrinsic(record);
  callIntrinsic(arraySortIntrinsic, keys, []);
  const normalized = callIntrinsic(objectCreateIntrinsic, Object, [null]);
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
    ensure(
      regexpTest(ENVIRONMENT_NAME_PATTERN, key) &&
        typeof record[key] === "string" &&
        record[key].length <= 4096 &&
        !stringIncludes(record[key], "\0"),
      code,
    );
    normalized[key] = record[key];
  }
  return callIntrinsic(objectFreezeIntrinsic, Object, [normalized]);
}

function normalizeEnvironment(value, podman, code) {
  if (podman) {
    const record = dataObject(value, ALLOWED_PODMAN_ENVIRONMENT, EMPTY_KEYS, code);
    return normalizeStringRecord(record, ALLOWED_PODMAN_ENVIRONMENT, code);
  }
  ensure(
    value !== null &&
      typeof value === "object" &&
      !arrayIsArrayIntrinsic(value) &&
      !isProxy(value),
    code,
  );
  const keys = reflectOwnKeysIntrinsic(value);
  ensure(
    arrayEvery(keys, (key) => typeof key === "string") && keys.length <= 64,
    code,
  );
  return normalizeStringRecord(value, keys, code);
}

function normalizeImages(value, code) {
  ensure(
    value !== null &&
      typeof value === "object" &&
      !arrayIsArrayIntrinsic(value) &&
      !isProxy(value) &&
      (callIntrinsic(objectGetPrototypeOfIntrinsic, Object, [value]) === objectPrototype ||
        callIntrinsic(objectGetPrototypeOfIntrinsic, Object, [value]) === null),
    code,
  );
  const digests = reflectOwnKeysIntrinsic(value);
  ensure(
    digests.length >= 1 &&
      digests.length <= 32 &&
      arrayEvery(digests, (digest) => typeof digest === "string"),
    code,
  );
  const result = callIntrinsic(objectCreateIntrinsic, Object, [null]);
  callIntrinsic(arraySortIntrinsic, digests, []);
  for (let index = 0; index < digests.length; index += 1) {
    const digest = digests[index];
    ensure(regexpTest(OCI_DIGEST_PATTERN, digest), code);
    const descriptor = callIntrinsic(objectGetOwnPropertyDescriptorIntrinsic, Object, [
      value,
      digest,
    ]);
    ensure(
      descriptor?.enumerable === true && objectHasOwnIntrinsic(descriptor, "value"),
      code,
    );
    const policy = exactDataObject(descriptor.value, IMAGE_POLICY_KEYS, code);
    ensure(
      arrayIncludes(["amd64", "arm64"], policy.architecture) &&
        policy.os === "linux" &&
        typeof policy.codexVersion === "string" &&
        regexpTest(CODEX_VERSION_PATTERN, policy.codexVersion) &&
        typeof policy.imageReference === "string" &&
        policy.imageReference.length <= 512 &&
        regexpTest(IMAGE_REFERENCE_PATTERN, policy.imageReference) &&
        !regexpTest(/[\s\0]/u, policy.imageReference) &&
        (policy.imageReference === digest ||
          callIntrinsic(stringEndsWithIntrinsic, policy.imageReference, [`@${digest}`])),
      code,
    );
    result[digest] = frozenRecord(policy);
  }
  return callIntrinsic(objectFreezeIntrinsic, Object, [result]);
}

function normalizeCommand(value, code) {
  ensure(
    arrayIsArrayIntrinsic(value) &&
      !isProxy(value) &&
      value.length >= 1 &&
      value.length <= 64,
    code,
  );
  const keys = reflectOwnKeysIntrinsic(value);
  ensure(
    keys.length === value.length + 1 && arrayIncludes(keys, "length"),
    code,
  );
  const command = [];
  for (let index = 0; index < value.length; index += 1) {
    const descriptor = callIntrinsic(objectGetOwnPropertyDescriptorIntrinsic, Object, [
      value,
      StringConstructor(index),
    ]);
    ensure(
      descriptor?.enumerable === true && objectHasOwnIntrinsic(descriptor, "value"),
      code,
    );
    const argument = descriptor.value;
    ensure(
      typeof argument === "string" &&
        argument.length >= 1 &&
        argument.length <= 4096 &&
        !stringIncludes(argument, "\0"),
      code,
    );
    arrayPush(command, argument);
  }
  ensure(callIntrinsic(stringStartsWithIntrinsic, command[0], ["/"]), code);
  return callIntrinsic(objectFreezeIntrinsic, Object, [command]);
}

function assertFunction(value, code) {
  ensure(
    typeof value === "function" && !isProxy(value) && !isGeneratorFunction(value),
    code,
  );
  return value;
}

function normalizeState(value, code) {
  const state = exactDataObject(value, STATE_KEYS, code);
  ensure(
    state.contractVersion === PODMAN_WRITER_SUPERVISOR_STATE_CONTRACT_VERSION,
    code,
  );
  return frozenRecord({
    claim: assertFunction(state.claim, code),
    contractVersion: state.contractVersion,
    read: assertFunction(state.read, code),
    transition: assertFunction(state.transition, code),
  });
}

function normalizeFilesystemAuthority(value, code) {
  const authority = exactDataObject(value, FILESYSTEM_AUTHORITY_KEYS, code);
  ensure(authority.contractVersion === 1, code);
  return frozenRecord({
    acquire: assertFunction(authority.acquire, code),
    close: assertFunction(authority.close, code),
    contractVersion: authority.contractVersion,
    verifyCurrent: assertFunction(authority.verifyCurrent, code),
    verifyRunningMount: assertFunction(authority.verifyRunningMount, code),
  });
}

function filesystemErrorCode(error) {
  if (error === null || (typeof error !== "object" && typeof error !== "function")) {
    return null;
  }
  try {
    const descriptor = callIntrinsic(objectGetOwnPropertyDescriptorIntrinsic, Object, [
      error,
      "code",
    ]);
    return descriptor !== undefined && objectHasOwnIntrinsic(descriptor, "value") &&
        typeof descriptor.value === "string"
      ? descriptor.value
      : null;
  } catch {
    return null;
  }
}

function failFilesystem(error, missingCode = "podman_writer_attachment_missing") {
  if (callIntrinsic(weakSetHasIntrinsic, errorBrands, [error])) throw error;
  const code = filesystemErrorCode(error);
  if (code === "ENOENT") fail(missingCode);
  if (code === "ENOTDIR" || code === "ELOOP") {
    fail("podman_writer_attachment_mismatch");
  }
  fail("podman_writer_attachment_revalidation_failed");
}

function filesystemOperation(operation, missingCode) {
  try {
    return operation();
  } catch (error) {
    failFilesystem(error, missingCode);
  }
}

const ACCESS_POLICY_MASK = 0o7777n;
const PRIVATE_DIRECTORY_MODE = 0o700n;
const GETFACL_EXECUTABLE = "/usr/bin/getfacl";
const GETFACL_ENVIRONMENT = Object.freeze({
  LANG: "C",
  LC_ALL: "C",
  PATH: "/usr/bin:/bin",
});
const BASE_PRIVATE_DIRECTORY_ACL = "user::rwx\ngroup::---\nother::---\n\n";

function directorySnapshot(stat, code) {
  ensure(
    currentUserIdBigInt !== null &&
    typeof stat.mode === "bigint" &&
      (stat.mode & fileTypeMask) === directoryFileType &&
      typeof stat.dev === "bigint" &&
      typeof stat.ino === "bigint" &&
      typeof stat.uid === "bigint" &&
      typeof stat.nlink === "bigint" &&
      stat.uid === currentUserIdBigInt &&
      stat.nlink >= 1n &&
      (stat.mode & ACCESS_POLICY_MASK) === PRIVATE_DIRECTORY_MODE,
    code,
  );
  return frozenRecord({
    dev: stat.dev,
    ino: stat.ino,
    mode: stat.mode & ACCESS_POLICY_MASK,
    uid: stat.uid,
  });
}

function sameDirectoryAuthority(left, right) {
  // dev+ino identify the held filesystem object while its descriptor remains
  // open. The current service uid and exact 0700 mode are the stat-visible
  // policy; a separate getfacl proof excludes access/default ACL entries.
  // Directory size, exact nlink, and all timestamps are intentionally excluded
  // because child-entry churn changes them without replacing the object or
  // access policy. nlink is still required to remain a positive directory
  // link count at every observation.
  return left.dev === right.dev && left.ino === right.ino &&
    left.uid === right.uid && left.mode === right.mode;
}

function assertCanonicalAttachmentRoot(
  rootPath,
  code = "invalid_podman_writer_supervisor_request",
) {
  ensure(
    typeof rootPath === "string" &&
      rootPath.length > 1 &&
      rootPath.length <= 4096,
    code,
  );
  // Bound UTF-16 code units before either UTF-8 sizing or encoding so an
  // untrusted request cannot force an attacker-sized temporary allocation.
  const encoded = callIntrinsic(bufferFromIntrinsic, Buffer, [rootPath, "utf8"]);
  ensure(
    callIntrinsic(bufferByteLengthIntrinsic, Buffer, [rootPath, "utf8"]) <=
      4096 &&
      callIntrinsic(bufferToStringIntrinsic, encoded, ["utf8"]) === rootPath &&
      isAbsolute(rootPath) &&
      resolve(rootPath) === rootPath &&
      !regexpTest(/[\0,\r\n]/u, rootPath),
    code,
  );
  return rootPath;
}

function isStrictlyWithin(configuredRoot, rootPath) {
  return rootPath.length > configuredRoot.length + 1 &&
    callIntrinsic(stringStartsWithIntrinsic, rootPath, [`${configuredRoot}/`]);
}

function validatePrivateDirectoryAcl(path) {
  let result;
  try {
    result = spawnSync(
      GETFACL_EXECUTABLE,
      ["--absolute-names", "--omit-header", "--numeric", "--", path],
      {
        cwd: "/",
        encoding: "buffer",
        env: GETFACL_ENVIRONMENT,
        killSignal: "SIGKILL",
        maxBuffer: 64 * 1024,
        shell: false,
        stdio: ["ignore", "pipe", "pipe"],
        timeout: 30_000,
      },
    );
  } catch {
    fail("podman_writer_attachment_revalidation_failed");
  }
  ensure(
    result !== null && typeof result === "object" && !isProxy(result),
    "podman_writer_attachment_revalidation_failed",
  );
  const resultValue = (key, required = true) => {
    let descriptor;
    try {
      descriptor = callIntrinsic(
        objectGetOwnPropertyDescriptorIntrinsic,
        Object,
        [result, key],
      );
    } catch {
      fail("podman_writer_attachment_revalidation_failed");
    }
    if (descriptor === undefined) {
      ensure(!required, "podman_writer_attachment_revalidation_failed");
      return undefined;
    }
    ensure(
      objectHasOwnIntrinsic(descriptor, "value"),
      "podman_writer_attachment_revalidation_failed",
    );
    return descriptor.value;
  };
  const error = resultValue("error", false);
  const status = resultValue("status");
  const signal = resultValue("signal");
  const stdout = resultValue("stdout");
  const stderr = resultValue("stderr");
  ensure(
    error === undefined &&
      status === 0 &&
      signal === null &&
      bufferIsBufferIntrinsic(stdout) &&
      bufferIsBufferIntrinsic(stderr) &&
      stdout.length <= 64 * 1024 &&
      stderr.length === 0,
    "podman_writer_attachment_revalidation_failed",
  );
  const output = callIntrinsic(bufferToStringIntrinsic, stdout, ["utf8"]);
  ensure(output === BASE_PRIVATE_DIRECTORY_ACL, "podman_writer_attachment_mismatch");
}

const defaultAuthorityHandles = new WeakSet();
const closedDefaultAuthorityHandles = new WeakSet();

function currentDirectorySnapshot(rootPath, heldSnapshot) {
  const canonical = filesystemOperation(() => realpathNativeIntrinsic(rootPath));
  ensure(canonical === rootPath, "podman_writer_attachment_mismatch");
  const linkStat = filesystemOperation(() => lstatSync(rootPath, { bigint: true }));
  const linkSnapshot = directorySnapshot(
    linkStat,
    "podman_writer_attachment_mismatch",
  );
  const pathSnapshot = directorySnapshot(
    filesystemOperation(() => statSync(rootPath, { bigint: true })),
    "podman_writer_attachment_mismatch",
  );
  ensure(
    sameDirectoryAuthority(linkSnapshot, pathSnapshot) &&
      sameDirectoryAuthority(pathSnapshot, heldSnapshot),
    "podman_writer_attachment_mismatch",
  );
  return pathSnapshot;
}

function openPinnedDirectory(path) {
  const canonical = filesystemOperation(() => realpathNativeIntrinsic(path));
  ensure(canonical === path, "podman_writer_attachment_mismatch");
  let fileDescriptor = null;
  try {
    fileDescriptor = filesystemOperation(() =>
      openSync(path, readOnlyFlag | openDirectoryFlag | noFollowFlag),
    );
    const snapshot = directorySnapshot(
      filesystemOperation(() => fstatSync(fileDescriptor, { bigint: true })),
      "podman_writer_attachment_mismatch",
    );
    const mountSource = `/proc/${currentProcessId}/fd/${fileDescriptor}`;
    const heldCanonical = filesystemOperation(
      () => realpathNativeIntrinsic(mountSource),
      "podman_writer_attachment_revalidation_failed",
    );
    ensure(heldCanonical === path, "podman_writer_attachment_mismatch");
    currentDirectorySnapshot(path, snapshot);
    validatePrivateDirectoryAcl(mountSource);
    currentDirectorySnapshot(path, snapshot);
    return frozenRecord({ fileDescriptor, mountSource, path, snapshot });
  } catch (error) {
    if (fileDescriptor !== null) {
      try {
        closeSync(fileDescriptor);
      } catch {
        // Preserve the authoritative acquisition failure. No Podman command
        // has consumed this descriptor yet, so a close failure can only leak
        // authority and cannot weaken the rejection.
      }
    }
    throw error;
  }
}

function validatePinnedDirectory(pin) {
  const before = directorySnapshot(
    filesystemOperation(() => fstatSync(pin.fileDescriptor, { bigint: true })),
    "podman_writer_attachment_mismatch",
  );
  ensure(
    sameDirectoryAuthority(before, pin.snapshot),
    "podman_writer_attachment_mismatch",
  );
  currentDirectorySnapshot(pin.path, pin.snapshot);
  validatePrivateDirectoryAcl(pin.mountSource);
  const after = directorySnapshot(
    filesystemOperation(() => fstatSync(pin.fileDescriptor, { bigint: true })),
    "podman_writer_attachment_mismatch",
  );
  ensure(
    sameDirectoryAuthority(after, pin.snapshot),
    "podman_writer_attachment_mismatch",
  );
  currentDirectorySnapshot(pin.path, pin.snapshot);
}

const defaultFilesystemAuthority = frozenRecord({
  contractVersion: 1,
  acquire: frozenFunction(async function acquire(input) {
    // This built-in authority protects the object selected from the canonical
    // path at acquisition time. The complete normalized attachment is bound
    // to the held handle so callers cannot substitute another authorization,
    // but opaque proofId semantics require a deployment-owned authority with
    // a persistent proof-to-object mapping; they cannot be inferred from a
    // pathname or from stat metadata alone.
    assertCanonicalAttachmentRoot(input.configuredAttachmentRoot);
    assertCanonicalAttachmentRoot(input.attachment.rootPath);
    ensure(
      currentPlatform === "linux" &&
        currentUserIdBigInt !== null &&
        numberIsSafeIntegerIntrinsic(openDirectoryFlag) &&
        numberIsSafeIntegerIntrinsic(noFollowFlag),
      "podman_writer_attachment_revalidation_failed",
    );
    ensure(
      isStrictlyWithin(
        input.configuredAttachmentRoot,
        input.attachment.rootPath,
      ),
      "podman_writer_attachment_mismatch",
    );
    let configured = null;
    let attachment = null;
    try {
      configured = openPinnedDirectory(input.configuredAttachmentRoot);
      attachment = openPinnedDirectory(input.attachment.rootPath);
      validatePinnedDirectory(configured);
      const handle = frozenRecord({
        attachment,
        attachmentAuthorization: input.attachment,
        configured,
        configuredAttachmentRoot: input.configuredAttachmentRoot,
        rootPath: input.attachment.rootPath,
      });
      callIntrinsic(weakSetAddIntrinsic, defaultAuthorityHandles, [handle]);
      return frozenRecord({ handle, mountSource: attachment.mountSource });
    } catch (error) {
      if (attachment !== null) {
        try {
          closeSync(attachment.fileDescriptor);
        } catch {
          // Preserve the authoritative acquisition failure.
        }
      }
      if (configured !== null) {
        try {
          closeSync(configured.fileDescriptor);
        } catch {
          // Preserve the authoritative acquisition failure.
        }
      }
      throw error;
    }
  }),
  verifyCurrent: frozenFunction(async function verifyCurrent(input) {
    ensure(
      callIntrinsic(weakSetHasIntrinsic, defaultAuthorityHandles, [input.handle]) &&
        !callIntrinsic(weakSetHasIntrinsic, closedDefaultAuthorityHandles, [
          input.handle,
        ]) &&
        input.configuredAttachmentRoot ===
          input.handle.configuredAttachmentRoot &&
        input.attachment === input.handle.attachmentAuthorization &&
        input.attachment.rootPath === input.handle.rootPath,
      "podman_writer_attachment_revalidation_failed",
    );
    validatePinnedDirectory(input.handle.configured);
    validatePinnedDirectory(input.handle.attachment);
    return true;
  }),
  verifyRunningMount: frozenFunction(async function verifyRunningMount(input) {
    await callIntrinsic(defaultFilesystemAuthority.verifyCurrent, undefined, [
      frozenRecord({
        configuredAttachmentRoot: input.configuredAttachmentRoot,
        handle: input.handle,
        attachment: input.attachment,
      }),
    ]);
    ensure(
      numberIsSafeIntegerIntrinsic(input.containerPid) && input.containerPid > 0,
      "podman_writer_attachment_revalidation_failed",
    );
    const livePath = `/proc/${input.containerPid}/root/session`;
    const liveBefore = directorySnapshot(
      filesystemOperation(
        () => statSync(livePath, { bigint: true }),
        "podman_writer_attachment_revalidation_failed",
      ),
      "podman_writer_attachment_mismatch",
    );
    ensure(
      sameDirectoryAuthority(liveBefore, input.handle.attachment.snapshot),
      "podman_writer_attachment_mismatch",
    );
    validatePrivateDirectoryAcl(livePath);
    const liveAfter = directorySnapshot(
      filesystemOperation(
        () => statSync(livePath, { bigint: true }),
        "podman_writer_attachment_revalidation_failed",
      ),
      "podman_writer_attachment_mismatch",
    );
    ensure(
      sameDirectoryAuthority(liveAfter, input.handle.attachment.snapshot),
      "podman_writer_attachment_mismatch",
    );
    await callIntrinsic(defaultFilesystemAuthority.verifyCurrent, undefined, [
      frozenRecord({
        configuredAttachmentRoot: input.configuredAttachmentRoot,
        handle: input.handle,
        attachment: input.attachment,
      }),
    ]);
    return true;
  }),
  close: frozenFunction(async function close(input) {
    ensure(
      callIntrinsic(weakSetHasIntrinsic, defaultAuthorityHandles, [input.handle]) &&
        !callIntrinsic(weakSetHasIntrinsic, closedDefaultAuthorityHandles, [
          input.handle,
        ]),
      "podman_writer_attachment_revalidation_failed",
    );
    callIntrinsic(weakSetAddIntrinsic, closedDefaultAuthorityHandles, [input.handle]);
    let closeError = null;
    try {
      closeSync(input.handle.attachment.fileDescriptor);
    } catch (error) {
      closeError = error;
    }
    try {
      closeSync(input.handle.configured.fileDescriptor);
    } catch (error) {
      if (closeError === null) closeError = error;
    }
    if (closeError !== null) {
      failFilesystem(closeError, "podman_writer_attachment_revalidation_failed");
    }
    return true;
  }),
});

function isNativePromise(value) {
  if (!isPromise(value) || isProxy(value)) return false;
  try {
    return (
      callIntrinsic(objectGetPrototypeOfIntrinsic, Object, [value]) === promisePrototype &&
      callIntrinsic(objectGetOwnPropertyDescriptorIntrinsic, Object, [
        value,
        "catch",
      ]) === undefined &&
      callIntrinsic(objectGetOwnPropertyDescriptorIntrinsic, Object, [
        value,
        "constructor",
      ]) === undefined &&
      callIntrinsic(objectGetOwnPropertyDescriptorIntrinsic, Object, [
        value,
        "finally",
      ]) === undefined &&
      callIntrinsic(objectGetOwnPropertyDescriptorIntrinsic, Object, [
        value,
        "then",
      ]) === undefined
    );
  } catch {
    return false;
  }
}

async function invokeState(method, input) {
  let pending;
  try {
    pending = callIntrinsic(method, undefined, [input]);
  } catch {
    fail("podman_writer_supervisor_outcome_uncertain");
  }
  ensure(isNativePromise(pending), "podman_writer_supervisor_outcome_uncertain");
  try {
    return await pending;
  } catch (error) {
    if (callIntrinsic(weakSetHasIntrinsic, errorBrands, [error])) throw error;
    fail("podman_writer_supervisor_outcome_uncertain");
  }
}

async function invokeFilesystemAuthority(method, input) {
  let pending;
  try {
    pending = callIntrinsic(method, undefined, [input]);
  } catch (error) {
    failFilesystem(error, "podman_writer_attachment_revalidation_failed");
  }
  ensure(
    isNativePromise(pending),
    "podman_writer_attachment_revalidation_failed",
  );
  try {
    return await pending;
  } catch (error) {
    failFilesystem(error, "podman_writer_attachment_revalidation_failed");
  }
}

async function acquireFilesystemAuthority(
  authority,
  configuredAttachmentRoot,
  attachment,
  signal,
) {
  const rootPath = attachment.rootPath;
  assertCanonicalAttachmentRoot(configuredAttachmentRoot);
  assertCanonicalAttachmentRoot(rootPath);
  ensure(
    isStrictlyWithin(configuredAttachmentRoot, rootPath),
    "podman_writer_attachment_mismatch",
  );
  ensureNotAborted(signal);
  const raw = await invokeFilesystemAuthority(
    authority.acquire,
    frozenRecord({ attachment, configuredAttachmentRoot }),
  );
  const acquired = exactDataObject(
    raw,
    FILESYSTEM_ACQUISITION_KEYS,
    "podman_writer_attachment_revalidation_failed",
  );
  ensure(
    acquired.handle !== null &&
      (typeof acquired.handle === "object" ||
        typeof acquired.handle === "function") &&
      !isProxy(acquired.handle),
    "podman_writer_attachment_revalidation_failed",
  );
  const normalized = frozenRecord(acquired);
  const validMountSource = typeof acquired.mountSource === "string" &&
    acquired.mountSource.length > 1 &&
    acquired.mountSource.length <= 4096 &&
    isAbsolute(acquired.mountSource) &&
    resolve(acquired.mountSource) === acquired.mountSource &&
    acquired.mountSource !== rootPath &&
    !regexpTest(/[\0,\r\n]/u, acquired.mountSource) &&
    regexpTest(PROC_FD_SOURCE_PATTERN, acquired.mountSource);
  if (!validMountSource) {
    await closeFilesystemAuthority(authority, normalized);
    fail("podman_writer_attachment_revalidation_failed");
  }
  if (signalAborted(signal)) {
    await closeFilesystemAuthority(authority, normalized);
    fail("podman_writer_supervisor_aborted");
  }
  return normalized;
}

async function verifyCurrentFilesystemAuthority(
  authority,
  acquired,
  configuredAttachmentRoot,
  attachment,
  signal,
) {
  ensureNotAborted(signal);
  const verified = await invokeFilesystemAuthority(
    authority.verifyCurrent,
    frozenRecord({ attachment, configuredAttachmentRoot, handle: acquired.handle }),
  );
  ensure(verified === true, "podman_writer_attachment_mismatch");
  ensureNotAborted(signal);
}

async function verifyRunningFilesystemAuthority(
  authority,
  acquired,
  configuredAttachmentRoot,
  attachment,
  containerPid,
  signal,
) {
  ensureNotAborted(signal);
  const verified = await invokeFilesystemAuthority(
    authority.verifyRunningMount,
    frozenRecord({
      attachment,
      configuredAttachmentRoot,
      containerPid,
      handle: acquired.handle,
    }),
  );
  ensure(verified === true, "podman_writer_attachment_mismatch");
  ensureNotAborted(signal);
}

async function closeFilesystemAuthority(authority, acquired) {
  const closed = await invokeFilesystemAuthority(
    authority.close,
    frozenRecord({ handle: acquired.handle }),
  );
  ensure(closed === true, "podman_writer_attachment_revalidation_failed");
}

function defaultCommandRunner(executable, arguments_, options) {
  return new PromiseConstructor((resolvePromise, rejectPromise) => {
    const discardCommandOutput =
      arguments_.length === 2 &&
      arguments_[0] === "start" &&
      regexpTest(FULL_CONTAINER_ID_PATTERN, arguments_[1]);
    let child;
    try {
      child = spawn(executable, arguments_, {
        cwd: "/",
        env: options.environment,
        killSignal: "SIGKILL",
        shell: false,
        signal: options.signal,
        // A detached Podman container may keep the CLI's stdout/stderr file
        // descriptions open after the direct CLI has exited. `start` has no
        // authoritative output: its exit status is followed by an exact
        // inspect plus live attachment proof. Avoid pipes only for that closed
        // command shape so `close` still proves the direct CLI was reaped.
        stdio: discardCommandOutput
          ? ["ignore", "ignore", "ignore"]
          : ["ignore", "pipe", "pipe"],
      });
    } catch (error) {
      rejectPromise(error);
      return;
    }
    let settled = false;
    let primaryError = null;
    let terminationRequested = false;
    let discardOutput = false;
    let stdout = callIntrinsic(bufferAllocIntrinsic, Buffer, [0]);
    let stderr = callIntrinsic(bufferAllocIntrinsic, Buffer, [0]);
    let timer;
    const rememberFailure = (error, terminate) => {
      if (primaryError === null) primaryError = error;
      if (!terminate || terminationRequested || settled) return;
      terminationRequested = true;
      try {
        child.kill("SIGKILL");
      } catch {
        // The authority cannot be released safely until `close` proves reap.
      }
    };
    const append = (current, chunk) => {
      if (discardOutput) return current;
      if (current.length + chunk.length > options.maxOutputBytes) {
        discardOutput = true;
        rememberFailure(
          new Error("Podman output exceeded the configured bound"),
          true,
        );
        return current;
      }
      return callIntrinsic(bufferConcatIntrinsic, Buffer, [[current, chunk]]);
    };
    if (!discardCommandOutput) {
      child.stdout.on("data", (chunk) => {
        stdout = append(stdout, chunk);
      });
      child.stderr.on("data", (chunk) => {
        stderr = append(stderr, chunk);
      });
      child.stdout.on("error", (error) => rememberFailure(error, true));
      child.stderr.on("error", (error) => rememberFailure(error, true));
    }
    child.on("error", (error) => rememberFailure(error, false));
    child.once("close", (code, signal) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      if (primaryError !== null) {
        rejectPromise(primaryError);
        return;
      }
      if (code !== 0 || signal !== null) {
        rejectPromise(new Error("Podman command failed"));
        return;
      }
      resolvePromise(frozenRecord({
        stderr: callIntrinsic(bufferToStringIntrinsic, stderr, ["utf8"]),
        stdout: callIntrinsic(bufferToStringIntrinsic, stdout, ["utf8"]),
      }));
    });
    timer = setTimeout(() => {
      rememberFailure(new Error("Podman command timed out"), true);
    }, options.timeoutMilliseconds);
    timer.unref?.();
  });
}

function containerName(supervisorId, launchAttemptId) {
  return `codex-writer-${sha256Parts(
    "portable-codex-runtime:podman-container:v1\0",
    supervisorId,
    "\0",
    launchAttemptId,
  ).slice(0, 48)}`;
}

function processIncarnationId(containerId) {
  return `podman-process:${containerId}`;
}

function writerIncarnationId(supervisorId, launchAttemptId, requestSha256, containerId) {
  return `podman-writer:${sha256Parts(
    "portable-codex-runtime:podman-writer:v1\0",
    supervisorId,
    "\0",
    launchAttemptId,
    "\0",
    requestSha256,
    "\0",
    containerId,
  )}`;
}

function startProofId(supervisorId, launchAttemptId, requestSha256, containerId) {
  return `podman-start:${sha256Parts(
    "portable-codex-runtime:podman-start-proof:v1\0",
    supervisorId,
    "\0",
    launchAttemptId,
    "\0",
    requestSha256,
    "\0",
    containerId,
  )}`;
}

function stoppedProofId(launchAttemptId, requestSha256, containerId) {
  return `podman-stopped:${sha256Parts(
    "portable-codex-runtime:podman-stopped-proof:v1\0",
    launchAttemptId,
    "\0",
    requestSha256,
    "\0",
    containerId,
  )}`;
}

function evidence(supervisorId, record, status, proofId) {
  return frozenRecord({
    contractVersion: PODMAN_WRITER_SUPERVISOR_CONTRACT_VERSION,
    launchAttemptId: record.launchAttemptId,
    processIncarnationId:
      status === "not-started" ? null : record.processIncarnationId,
    proofId,
    status,
    supervisorId,
    writerIncarnationId:
      status === "not-started" ? null : record.writerIncarnationId,
  });
}

function notStartedEvidence(supervisorId, launchAttemptId, requestSha256) {
  return evidence(
    supervisorId,
    frozenRecord({
      launchAttemptId,
      processIncarnationId: null,
      writerIncarnationId: null,
    }),
    "not-started",
    `podman-not-started:${sha256Parts(
      "portable-codex-runtime:podman-not-started:v1\0",
      supervisorId,
      "\0",
      launchAttemptId,
      "\0",
      requestSha256,
    )}`,
  );
}

function validateRecord(recordValue, expected) {
  let record;
  try {
    record = assertPodmanWriterSupervisorStateRecord(recordValue);
  } catch {
    fail("podman_writer_supervisor_outcome_uncertain");
  }
  ensure(
    record.launchAttemptId === expected.launchAttemptId &&
      record.requestSha256 === expected.requestSha256 &&
      record.containerName === expected.containerName,
    "podman_writer_state_conflict",
  );
  if (record.containerId !== null) {
    ensure(
      record.processIncarnationId === processIncarnationId(record.containerId) &&
        record.writerIncarnationId ===
          writerIncarnationId(
            expected.supervisorId,
            expected.launchAttemptId,
            expected.requestSha256,
            record.containerId,
          ),
      "podman_writer_supervisor_outcome_uncertain",
    );
  }
  if (arrayIncludes(["started", "stopping", "stopped"], record.status)) {
    ensure(
      record.proofId ===
        startProofId(
          expected.supervisorId,
          expected.launchAttemptId,
          expected.requestSha256,
          record.containerId,
        ),
      "podman_writer_supervisor_outcome_uncertain",
    );
  }
  if (record.status === "stopped") {
    ensure(
      record.stopProofId ===
        stoppedProofId(
          expected.launchAttemptId,
          expected.requestSha256,
          record.containerId,
        ),
      "podman_writer_supervisor_outcome_uncertain",
    );
  }
  return record;
}

function parsedJson(stdout, code) {
  ensure(typeof stdout === "string" && stdout.length > 0, code);
  try {
    return callIntrinsic(jsonParseIntrinsic, JsonObject, [stdout]);
  } catch {
    fail(code);
  }
}

function jsonObject(value, code) {
  ensure(
    value !== null &&
      typeof value === "object" &&
      !arrayIsArrayIntrinsic(value) &&
      (callIntrinsic(objectGetPrototypeOfIntrinsic, Object, [value]) ===
          objectPrototype ||
        callIntrinsic(objectGetPrototypeOfIntrinsic, Object, [value]) === null),
    code,
  );
  return value;
}

function ownJsonValue(value, key, code) {
  jsonObject(value, code);
  const descriptor = callIntrinsic(objectGetOwnPropertyDescriptorIntrinsic, Object, [
    value,
    key,
  ]);
  ensure(
    descriptor?.enumerable === true && objectHasOwnIntrinsic(descriptor, "value"),
    code,
  );
  return descriptor.value;
}

function ownJsonAlias(value, keys, code) {
  jsonObject(value, code);
  let found = false;
  let result;
  for (let index = 0; index < keys.length; index += 1) {
    const descriptor = callIntrinsic(objectGetOwnPropertyDescriptorIntrinsic, Object, [
      value,
      keys[index],
    ]);
    if (descriptor === undefined) continue;
    ensure(
      !found &&
        descriptor.enumerable === true &&
        objectHasOwnIntrinsic(descriptor, "value"),
      code,
    );
    found = true;
    result = descriptor.value;
  }
  ensure(found, code);
  return result;
}

function ownJsonArrayElement(value, index, code) {
  ensure(arrayIsArrayIntrinsic(value), code);
  const descriptor = callIntrinsic(objectGetOwnPropertyDescriptorIntrinsic, Object, [
    value,
    StringConstructor(index),
  ]);
  ensure(
    descriptor?.enumerable === true && objectHasOwnIntrinsic(descriptor, "value"),
    code,
  );
  return descriptor.value;
}

function inspectObject(stdout, code) {
  const parsed = parsedJson(stdout, code);
  if (arrayIsArrayIntrinsic(parsed)) {
    ensure(parsed.length === 1, code);
    return ownJsonArrayElement(parsed, 0, code);
  }
  return jsonObject(parsed, code);
}

function validateImageInspection(value, policy, digest) {
  const imageDigest = ownJsonAlias(
    value,
    ["Digest", "digest"],
    "podman_writer_image_mismatch",
  );
  const architecture = ownJsonAlias(
    value,
    ["Architecture", "architecture"],
    "podman_writer_image_mismatch",
  );
  const os = ownJsonAlias(
    value,
    ["Os", "OS", "os"],
    "podman_writer_image_mismatch",
  );
  ensure(
    imageDigest === digest &&
      architecture === policy.architecture &&
      os === policy.os,
    "podman_writer_image_mismatch",
  );
}

function normalizedPodmanName(value) {
  if (typeof value !== "string") return null;
  return callIntrinsic(stringStartsWithIntrinsic, value, ["/"])
    ? callIntrinsic(stringSliceIntrinsic, value, [1])
    : value;
}

function validatePsContainer(value, expectedName, code) {
  const id = ownJsonAlias(value, ["Id", "ID"], code);
  const names = ownJsonAlias(value, ["Names", "Name"], code);
  const state = ownJsonValue(value, "State", code);
  ensure(
    typeof id === "string" &&
      regexpTest(CONTAINER_ID_PATTERN, id) &&
      typeof state === "string" &&
      arrayIncludes(
        ["configured", "created", "exited", "running", "stopped"],
        state,
      ),
    code,
  );
  if (arrayIsArrayIntrinsic(names)) {
    ensure(
      names.length === 1 &&
        normalizedPodmanName(ownJsonArrayElement(names, 0, code)) === expectedName,
      code,
    );
    return frozenRecord({ id, state });
  }
  ensure(normalizedPodmanName(names) === expectedName, code);
  return frozenRecord({ id, state });
}

function validatePsInspectionState(candidate, inspection, code) {
  const state = ownJsonValue(inspection, "State", code);
  const status = ownJsonValue(state, "Status", code);
  const running = ownJsonValue(state, "Running", code);
  ensure(
    (candidate.state === "running" && running === true && status === "running") ||
      (arrayIncludes(["configured", "created"], candidate.state) &&
        running === false && status === "configured") ||
      (arrayIncludes(["exited", "stopped"], candidate.state) &&
        running === false && arrayIncludes(["exited", "stopped"], status)),
    code,
  );
}

function validAttachmentMountSource(source, expected) {
  if (
    typeof source !== "string" ||
    source.length <= 1 ||
    source.length > 4096 ||
    !isAbsolute(source) ||
    resolve(source) !== source ||
    regexpTest(/[\0,\r\n]/u, source)
  ) {
    return false;
  }
  if (expected.liveObjectProof === true) return true;
  return source === expected.attachmentRoot ||
    source === expected.attachmentSource ||
    regexpTest(PROC_FD_SOURCE_PATTERN, source);
}

function validateContainerInspection(
  value,
  expected,
  running,
  code = "podman_writer_output_invalid",
) {
  const id = ownJsonAlias(value, ["Id", "ID"], code);
  const name = normalizedPodmanName(ownJsonValue(value, "Name", code));
  const imageDigest = ownJsonValue(value, "ImageDigest", code);
  const state = ownJsonValue(value, "State", code);
  const mounts = ownJsonValue(value, "Mounts", code);
  jsonObject(state, code);
  const stateRunning = ownJsonValue(state, "Running", code);
  const statePid = ownJsonValue(state, "Pid", code);
  const stateStatus = ownJsonValue(state, "Status", code);
  ensure(
    typeof id === "string" &&
      regexpTest(CONTAINER_ID_PATTERN, id) &&
      (expected.containerId === null || id === expected.containerId) &&
      name === expected.containerName &&
      imageDigest === expected.imageDigest &&
      stateRunning === running &&
      arrayIsArrayIntrinsic(mounts),
    code,
  );
  const bindMounts = [];
  for (let index = 0; index < mounts.length; index += 1) {
    const mount = ownJsonArrayElement(mounts, index, code);
    jsonObject(mount, code);
    if (ownJsonValue(mount, "Type", code) === "bind") {
      arrayPush(bindMounts, mount);
    }
  }
  const bindMount = bindMounts.length === 1 ? bindMounts[0] : null;
  ensure(
    bindMounts.length === 1 &&
      validAttachmentMountSource(ownJsonValue(bindMount, "Source", code), expected) &&
      ownJsonValue(bindMount, "Destination", code) === "/session" &&
      ownJsonValue(bindMount, "RW", code) === true &&
      ownJsonValue(bindMount, "Propagation", code) === "rprivate",
    code,
  );
  if (running) {
    ensure(
      numberIsSafeIntegerIntrinsic(statePid) &&
        statePid > 0 &&
        stateStatus === "running",
      code,
    );
  } else {
    ensure(
      statePid === 0 &&
        arrayIncludes(["configured", "exited", "stopped"], stateStatus),
      code,
    );
  }
  return id;
}

function runningContainerPid(value, code) {
  const state = ownJsonValue(value, "State", code);
  const pid = ownJsonValue(state, "Pid", code);
  ensure(numberIsSafeIntegerIntrinsic(pid) && pid > 0, code);
  return pid;
}

function newStateRecord(values) {
  return frozenRecord({
    containerId: null,
    containerName: values.containerName,
    contractVersion: PODMAN_WRITER_SUPERVISOR_STATE_CONTRACT_VERSION,
    launchAttemptId: values.launchAttemptId,
    processIncarnationId: null,
    proofId: null,
    requestSha256: values.requestSha256,
    revision: 0,
    status: "preparing",
    stopOperationId: null,
    stopProofId: null,
    writerIncarnationId: null,
    ...values.override,
  });
}

export function createPodmanWriterSupervisor(...args) {
  const optionCode = "invalid_podman_writer_supervisor_options";
  ensure(args.length === 1, optionCode);
  const options = dataObject(args[0], OPTION_KEYS, REQUIRED_OPTION_KEYS, optionCode);
  const supervisorId = assertOpaqueId(options.supervisorId, optionCode);
  ensure(
    typeof options.podmanExecutable === "string" &&
      options.podmanExecutable.length > 1 &&
      !stringIncludes(options.podmanExecutable, "\0") &&
      isAbsolute(options.podmanExecutable) &&
      resolve(options.podmanExecutable) === options.podmanExecutable,
    optionCode,
  );
  const podmanExecutable = options.podmanExecutable;
  const configuredAttachmentRoot = assertCanonicalAttachmentRoot(
    options.configuredAttachmentRoot,
    optionCode,
  );
  const images = normalizeImages(options.images, optionCode);
  const writerCommand = normalizeCommand(options.writerCommand, optionCode);
  const writerEnvironment = normalizeEnvironment(
    options.writerEnvironment,
    false,
    optionCode,
  );
  const podmanEnvironment = normalizeEnvironment(
    options.podmanEnvironment,
    true,
    optionCode,
  );
  const state = normalizeState(options.state, optionCode);
  const filesystemAuthority = options.filesystemAuthority === undefined
    ? defaultFilesystemAuthority
    : normalizeFilesystemAuthority(options.filesystemAuthority, optionCode);
  const runner = options.commandRunner === undefined
    ? defaultCommandRunner
    : assertFunction(options.commandRunner, optionCode);
  const timeout = options.commandTimeoutMilliseconds ?? 30_000;
  const maxOutputBytes = options.maxOutputBytes ?? 1024 * 1024;
  const stopTimeoutSeconds = options.stopTimeoutSeconds ?? 10;
  ensure(
    numberIsSafeIntegerIntrinsic(timeout) && timeout >= 1 && timeout <= 86_400_000 &&
      numberIsSafeIntegerIntrinsic(maxOutputBytes) &&
      maxOutputBytes >= 1024 &&
      maxOutputBytes <= 16 * 1024 * 1024 &&
      numberIsSafeIntegerIntrinsic(stopTimeoutSeconds) &&
      stopTimeoutSeconds >= 1 &&
      stopTimeoutSeconds <= 600 &&
      timeout >= stopTimeoutSeconds * 1000 + 1000,
    optionCode,
  );

  async function runPodman(arguments_, signal) {
    ensureNotAborted(signal);
    const commandArguments = callIntrinsic(objectFreezeIntrinsic, Object, [
      callIntrinsic(arraySliceIntrinsic, arguments_, []),
    ]);
    const runnerOptions = frozenRecord({
      environment: podmanEnvironment,
      maxOutputBytes,
      signal,
      timeoutMilliseconds: timeout,
    });
    let pending;
    try {
      pending = callIntrinsic(runner, undefined, [
        podmanExecutable,
        commandArguments,
        runnerOptions,
      ]);
    } catch {
      if (signalAborted(signal)) fail("podman_writer_supervisor_aborted");
      fail("podman_writer_supervisor_outcome_uncertain");
    }
    ensure(isNativePromise(pending), "podman_writer_supervisor_outcome_uncertain");
    let raw;
    try {
      raw = await pending;
    } catch {
      if (signalAborted(signal)) fail("podman_writer_supervisor_aborted");
      fail("podman_writer_supervisor_outcome_uncertain");
    }
    ensureNotAborted(signal);
    const result = exactDataObject(
      raw,
      RUNNER_RESULT_KEYS,
      "podman_writer_output_invalid",
    );
    ensure(
      typeof result.stdout === "string" &&
        typeof result.stderr === "string" &&
        callIntrinsic(bufferByteLengthIntrinsic, Buffer, [result.stdout, "utf8"]) <=
          maxOutputBytes &&
        callIntrinsic(bufferByteLengthIntrinsic, Buffer, [result.stderr, "utf8"]) <=
          maxOutputBytes,
      "podman_writer_output_invalid",
    );
    return frozenRecord(result);
  }

  async function readState(expected) {
    const raw = await invokeState(
      state.read,
      frozenRecord({ launchAttemptId: expected.launchAttemptId }),
    );
    return raw === null ? null : validateRecord(raw, expected);
  }

  async function transition(record, expectedStatus, next) {
    const raw = await invokeState(
      state.transition,
      frozenRecord({
        expectedRevision: record.revision,
        expectedStatus,
        record: next,
      }),
    );
    return raw;
  }

  async function proveRunningAttachment(attachment, inspection, signal, code) {
    const acquired = await acquireFilesystemAuthority(
      filesystemAuthority,
      configuredAttachmentRoot,
      attachment,
      signal,
    );
    try {
      await verifyRunningFilesystemAuthority(
        filesystemAuthority,
        acquired,
        configuredAttachmentRoot,
        attachment,
        runningContainerPid(inspection, code),
        signal,
      );
    } finally {
      await closeFilesystemAuthority(filesystemAuthority, acquired);
    }
  }

  async function proveNoStoppedContainer(record, expected, signal) {
    // A durable stopped tombstone is the authorization boundary for
    // retirement. Removal and both absence queries are replayable, so an
    // rm/response crash leaves the same stopped record available for a cold
    // retry; no earlier state is allowed to remove a container.
    ensure(
      record.status === "stopped" && record.containerId !== null,
      "podman_writer_supervisor_outcome_uncertain",
    );
    const removed = await runPodman(
      ["rm", "--ignore", record.containerId],
      signal,
    );
    ensure(
      removed.stderr === "" &&
        (removed.stdout === "" || removed.stdout === `${record.containerId}\n`),
      "podman_writer_output_invalid",
    );
    const filters = [
      `name=^${expected.containerName}$`,
      `id=${record.containerId}`,
    ];
    for (let index = 0; index < filters.length; index += 1) {
      const listed = await runPodman(
        [
          "ps",
          "-a",
          "--no-trunc",
          "--filter",
          filters[index],
          "--format=json",
        ],
        signal,
      );
      ensure(listed.stderr === "", "podman_writer_output_invalid");
      const candidates = parsedJson(
        listed.stdout,
        "podman_writer_supervisor_outcome_uncertain",
      );
      ensure(
        arrayIsArrayIntrinsic(candidates) && candidates.length === 0,
        "podman_writer_supervisor_outcome_uncertain",
      );
    }
  }

  function makeStopWriter(expected) {
    const stopWriter = frozenFunction(async function stopWriter(inputValue) {
      ensure(arguments.length === 1, "invalid_podman_writer_supervisor_request");
      const input = normalizeStopInput(inputValue, expected);
      ensureNotAborted(input.signal);
      let record = await readState(expected);
      ensure(record !== null, "podman_writer_supervisor_outcome_uncertain");
      if (record.status === "stopped") {
        ensure(
          record.stopOperationId === input.stopOperationId,
          "podman_writer_state_conflict",
        );
        await proveNoStoppedContainer(record, expected, input.signal);
        return frozenRecord({
          contractVersion: PODMAN_WRITER_SUPERVISOR_CONTRACT_VERSION,
          status: "stopped",
        });
      }
      if (record.status === "started") {
        const stopping = newStateRecord({
          containerName: expected.containerName,
          launchAttemptId: expected.launchAttemptId,
          requestSha256: expected.requestSha256,
          override: {
            containerId: record.containerId,
            processIncarnationId: record.processIncarnationId,
            proofId: record.proofId,
            revision: 3,
            status: "stopping",
            stopOperationId: input.stopOperationId,
            writerIncarnationId: record.writerIncarnationId,
          },
        });
        record = validateRecord(
          await transition(record, "started", stopping),
          expected,
        );
      } else {
        ensure(
          record.status === "stopping" &&
            record.stopOperationId === input.stopOperationId,
          "podman_writer_state_conflict",
        );
      }
      await runPodman(
        [
          "stop",
          "--ignore",
          "--time",
          StringConstructor(stopTimeoutSeconds),
          record.containerId,
        ],
        input.signal,
      );
      // Podman wait joins the container lifecycle, not merely its initial PID;
      // the following inspect then requires the cgroup-visible container state
      // to be non-running with no remaining container PID before persistence.
      const waited = await runPodman(
        ["wait", "--condition=stopped", record.containerId],
        input.signal,
      );
      ensure(
        regexpTest(/^[-]?[0-9]+\n?$/u, waited.stdout),
        "podman_writer_output_invalid",
      );
      const inspected = await runPodman(
        ["container", "inspect", "--format=json", record.containerId],
        input.signal,
      );
      validateContainerInspection(
        inspectObject(inspected.stdout, "podman_writer_output_invalid"),
        {
          attachmentRoot: expected.attachment.rootPath,
          containerId: record.containerId,
          containerName: expected.containerName,
          imageDigest: expected.imageDigest,
        },
        false,
      );
      const stopped = newStateRecord({
        containerName: expected.containerName,
        launchAttemptId: expected.launchAttemptId,
        requestSha256: expected.requestSha256,
        override: {
          containerId: record.containerId,
          processIncarnationId: record.processIncarnationId,
          proofId: record.proofId,
          revision: 4,
          status: "stopped",
          stopOperationId: input.stopOperationId,
          stopProofId: stoppedProofId(
            expected.launchAttemptId,
            expected.requestSha256,
            record.containerId,
          ),
          writerIncarnationId: record.writerIncarnationId,
        },
      });
      record = validateRecord(
        await transition(record, "stopping", stopped),
        expected,
      );
      await proveNoStoppedContainer(record, expected, input.signal);
      return frozenRecord({
        contractVersion: PODMAN_WRITER_SUPERVISOR_CONTRACT_VERSION,
        status: "stopped",
      });
    });
    return stopWriter;
  }

  const launchWriter = frozenFunction(async function launchWriter(inputValue) {
    ensure(arguments.length === 1, "invalid_podman_writer_supervisor_request");
    const input = normalizeLaunchInput(inputValue, supervisorId);
    const attachment = input.typed.attachment;
    const attachmentRoot = assertCanonicalAttachmentRoot(attachment.rootPath);
    ensure(
      isStrictlyWithin(configuredAttachmentRoot, attachmentRoot),
      "podman_writer_attachment_mismatch",
    );
    const launchAttemptId = input.attempt.launchAttemptId;
    const digest = input.typed.image.platformImage.digest;
    const policy = images[digest];
    ensure(policy !== undefined, "podman_writer_image_mismatch");
    ensure(
      input.typed.image.platformImage.architecture === policy.architecture &&
        input.typed.image.platformImage.os === policy.os &&
        input.typed.image.projection.codexVersion === policy.codexVersion &&
        input.typed.image.runtime.codexVersion === policy.codexVersion,
      "podman_writer_image_mismatch",
    );
    const digestOfRequest = requestDigest(input.typed.request);
    const name = containerName(supervisorId, launchAttemptId);
    const expected = frozenRecord({
      attachment: input.typed.attachment,
      containerName: name,
      imageDigest: digest,
      launchAttemptId,
      lease: input.typed.lease,
      requestSha256: digestOfRequest,
      supervisorId,
    });
    let record = await readState(expected);
    if (record !== null) {
      if (record.status === "started") {
        return frozenRecord({
          evidence: evidence(supervisorId, record, "started", record.proofId),
          receiptVersion: 1,
          stopWriter: makeStopWriter({
            ...expected,
            processIncarnationId: record.processIncarnationId,
            writerIncarnationId: record.writerIncarnationId,
          }),
        });
      }
      if (record.status === "stopped") {
        await proveNoStoppedContainer(record, expected, input.signal);
        return frozenRecord({
          evidence: evidence(
            supervisorId,
            record,
            "complete-stopped",
            record.stopProofId,
          ),
          receiptVersion: 1,
          stopWriter: null,
        });
      }
      fail("podman_writer_supervisor_outcome_uncertain");
    }
    ensureNotAborted(input.signal);
    const info = await runPodman(["info", "--format=json"], input.signal);
    const infoValue = jsonObject(
      parsedJson(info.stdout, "podman_writer_output_invalid"),
      "podman_writer_output_invalid",
    );
    const host = ownJsonAlias(
      infoValue,
      ["host", "Host"],
      "podman_writer_output_invalid",
    );
    const security = ownJsonAlias(
      host,
      ["security", "Security"],
      "podman_writer_output_invalid",
    );
    const rootless = ownJsonAlias(
      security,
      ["rootless", "Rootless"],
      "podman_writer_output_invalid",
    );
    ensure(rootless === true, "podman_writer_rootless_required");
    const imageInspect = await runPodman(
      ["image", "inspect", "--format=json", policy.imageReference],
      input.signal,
    );
    validateImageInspection(
      inspectObject(imageInspect.stdout, "podman_writer_output_invalid"),
      policy,
      digest,
    );
    const acquired = await acquireFilesystemAuthority(
      filesystemAuthority,
      configuredAttachmentRoot,
      attachment,
      input.signal,
    );
    try {
      const preparing = newStateRecord({
        containerName: name,
        launchAttemptId,
        requestSha256: digestOfRequest,
      });
      const claimRaw = await invokeState(
        state.claim,
        frozenRecord({ record: preparing }),
      );
      const claim = exactDataObject(
        claimRaw,
        STATE_CLAIM_RECEIPT_KEYS,
        "podman_writer_supervisor_outcome_uncertain",
      );
      ensure(
        typeof claim.created === "boolean",
        "podman_writer_supervisor_outcome_uncertain",
      );
      record = validateRecord(claim.record, expected);
      if (!claim.created) {
        if (record.status === "started") {
          return frozenRecord({
            evidence: evidence(supervisorId, record, "started", record.proofId),
            receiptVersion: 1,
            stopWriter: makeStopWriter({
              ...expected,
              processIncarnationId: record.processIncarnationId,
              writerIncarnationId: record.writerIncarnationId,
            }),
          });
        }
        fail("podman_writer_supervisor_outcome_uncertain");
      }
      await verifyCurrentFilesystemAuthority(
        filesystemAuthority,
        acquired,
        configuredAttachmentRoot,
        attachment,
        input.signal,
      );
      const createArguments = [
        "create",
        "--name",
        name,
        "--pull=never",
        "--read-only",
        "--security-opt=no-new-privileges",
        "--cap-drop=all",
        "--userns=keep-id",
        "--restart=no",
        "--mount",
        `type=bind,source=${acquired.mountSource}` +
          ",target=/session,rw,bind-propagation=rprivate",
        "--workdir",
        "/session",
      ];
      const environmentKeys = reflectOwnKeysIntrinsic(writerEnvironment);
      for (let index = 0; index < environmentKeys.length; index += 1) {
        const key = environmentKeys[index];
        arrayPushTwo(createArguments, "--env", `${key}=${writerEnvironment[key]}`);
      }
      arrayPush(createArguments, policy.imageReference);
      for (let index = 0; index < writerCommand.length; index += 1) {
        arrayPush(createArguments, writerCommand[index]);
      }
      const createdOutput = await runPodman(createArguments, input.signal);
      const containerId = callIntrinsic(
        stringTrimIntrinsic,
        createdOutput.stdout,
        [],
      );
      ensure(
        regexpTest(CONTAINER_ID_PATTERN, containerId) &&
          createdOutput.stdout === `${containerId}\n`,
        "podman_writer_output_invalid",
      );
      const created = newStateRecord({
        containerName: name,
        launchAttemptId,
        requestSha256: digestOfRequest,
        override: {
          containerId,
          processIncarnationId: processIncarnationId(containerId),
          revision: 1,
          status: "created",
          writerIncarnationId: writerIncarnationId(
            supervisorId,
            launchAttemptId,
            digestOfRequest,
            containerId,
          ),
        },
      });
      record = validateRecord(
        await transition(record, "preparing", created),
        expected,
      );
      await runPodman(["start", containerId], input.signal);
      const inspected = await runPodman(
        ["container", "inspect", "--format=json", containerId],
        input.signal,
      );
      const inspection = inspectObject(
        inspected.stdout,
        "podman_writer_output_invalid",
      );
      validateContainerInspection(
        inspection,
        {
          attachmentRoot,
          attachmentSource: acquired.mountSource,
          containerId,
          containerName: name,
          imageDigest: digest,
          liveObjectProof: true,
        },
        true,
      );
      await verifyRunningFilesystemAuthority(
        filesystemAuthority,
        acquired,
        configuredAttachmentRoot,
        attachment,
        runningContainerPid(inspection, "podman_writer_output_invalid"),
        input.signal,
      );
      const started = newStateRecord({
        containerName: name,
        launchAttemptId,
        requestSha256: digestOfRequest,
        override: {
          containerId,
          processIncarnationId: record.processIncarnationId,
          proofId: startProofId(
            supervisorId,
            launchAttemptId,
            digestOfRequest,
            containerId,
          ),
          revision: 2,
          status: "started",
          writerIncarnationId: record.writerIncarnationId,
        },
      });
      record = validateRecord(
        await transition(record, "created", started),
        expected,
      );
      return frozenRecord({
        evidence: evidence(supervisorId, record, "started", record.proofId),
        receiptVersion: 1,
        stopWriter: makeStopWriter({
          ...expected,
          processIncarnationId: record.processIncarnationId,
          writerIncarnationId: record.writerIncarnationId,
        }),
      });
    } finally {
      await closeFilesystemAuthority(filesystemAuthority, acquired);
    }
  });

  const reconcileWriterLaunch = frozenFunction(
    async function reconcileWriterLaunch(inputValue) {
      ensure(arguments.length === 1, "invalid_podman_writer_supervisor_request");
      const input = normalizeReconcileInput(inputValue, supervisorId);
      const attachment = input.typed.attachment;
      const attachmentRoot = assertCanonicalAttachmentRoot(attachment.rootPath);
      ensure(
        isStrictlyWithin(configuredAttachmentRoot, attachmentRoot),
        "podman_writer_attachment_mismatch",
      );
      const launchAttemptId = input.attempt.launchAttemptId;
      const digestOfRequest = requestDigest(input.typed.request);
      const name = containerName(supervisorId, launchAttemptId);
      const expected = frozenRecord({
        containerName: name,
        launchAttemptId,
        requestSha256: digestOfRequest,
        supervisorId,
      });
      const record = await readState(expected);
      if (record?.status === "stopped") {
        return frozenRecord({
          evidence: evidence(
            supervisorId,
            record,
            "complete-stopped",
            record.stopProofId,
          ),
          receiptVersion: 1,
        });
      }
      if (record === null || record.status === "preparing") {
        const listed = await runPodman(
          [
            "ps",
            "-a",
            "--no-trunc",
            "--filter",
            `name=^${name}$`,
            "--format=json",
          ],
          input.signal,
        );
        ensure(
          listed.stderr === "",
          "podman_writer_supervisor_outcome_uncertain",
        );
        const candidates = parsedJson(
          listed.stdout,
          "podman_writer_supervisor_outcome_uncertain",
        );
        ensure(
          arrayIsArrayIntrinsic(candidates),
          "podman_writer_supervisor_outcome_uncertain",
        );
        if (candidates.length === 0) {
          return frozenRecord({
            evidence: notStartedEvidence(
              supervisorId,
              launchAttemptId,
              digestOfRequest,
            ),
            receiptVersion: 1,
          });
        }
        ensure(
          candidates.length === 1,
          "podman_writer_supervisor_outcome_uncertain",
        );
        const candidate = validatePsContainer(
          ownJsonArrayElement(
            candidates,
            0,
            "podman_writer_supervisor_outcome_uncertain",
          ),
          name,
          "podman_writer_supervisor_outcome_uncertain",
        );
        const inspected = await runPodman(
          ["container", "inspect", "--format=json", name],
          input.signal,
        );
        const inspection = inspectObject(
          inspected.stdout,
          "podman_writer_supervisor_outcome_uncertain",
        );
        const inspectionState = ownJsonValue(
          inspection,
          "State",
          "podman_writer_supervisor_outcome_uncertain",
        );
        const isRunning = ownJsonValue(
          inspectionState,
          "Running",
          "podman_writer_supervisor_outcome_uncertain",
        );
        ensure(
          typeof isRunning === "boolean",
          "podman_writer_supervisor_outcome_uncertain",
        );
        const observedContainerId = validateContainerInspection(
          inspection,
          {
            attachmentRoot,
            containerId: candidate.id,
            containerName: name,
            imageDigest: input.typed.image.platformImage.digest,
            liveObjectProof: isRunning,
          },
          isRunning,
          "podman_writer_supervisor_outcome_uncertain",
        );
        if (isRunning) {
          await proveRunningAttachment(
            attachment,
            inspection,
            input.signal,
            "podman_writer_supervisor_outcome_uncertain",
          );
        }
        validatePsInspectionState(
          candidate,
          inspection,
          "podman_writer_supervisor_outcome_uncertain",
        );
        if (isRunning) {
          fail("podman_writer_supervisor_outcome_uncertain");
        }
        const observedStatus = ownJsonValue(
          inspectionState,
          "Status",
          "podman_writer_supervisor_outcome_uncertain",
        );
        const observed = frozenRecord({
          launchAttemptId,
          processIncarnationId: processIncarnationId(observedContainerId),
          writerIncarnationId: writerIncarnationId(
            supervisorId,
            launchAttemptId,
            digestOfRequest,
            observedContainerId,
          ),
        });
        ensure(
          observedStatus !== "configured",
          "podman_writer_supervisor_outcome_uncertain",
        );
        // This is terminal observation, not a durable supervisor tombstone.
        // Retaining the exact stopped container makes an acknowledgement-loss
        // retry reproduce complete-stopped rather than not-started.
        return frozenRecord({
          evidence: evidence(
            supervisorId,
            observed,
            "complete-stopped",
            stoppedProofId(
              launchAttemptId,
              digestOfRequest,
              observedContainerId,
            ),
          ),
          receiptVersion: 1,
        });
      }
      ensure(
        record.containerId !== null,
        "podman_writer_supervisor_outcome_uncertain",
      );
      const inspected = await runPodman(
        ["container", "inspect", "--format=json", record.containerId],
        input.signal,
      );
      const inspection = inspectObject(
        inspected.stdout,
        "podman_writer_supervisor_outcome_uncertain",
      );
      const inspectionState = ownJsonValue(
        inspection,
        "State",
        "podman_writer_supervisor_outcome_uncertain",
      );
      const isRunning = ownJsonValue(
        inspectionState,
        "Running",
        "podman_writer_supervisor_outcome_uncertain",
      );
      ensure(
        typeof isRunning === "boolean",
        "podman_writer_supervisor_outcome_uncertain",
      );
      validateContainerInspection(
        inspection,
        {
          attachmentRoot,
          containerId: record.containerId,
          containerName: name,
          imageDigest: input.typed.image.platformImage.digest,
          liveObjectProof: isRunning,
        },
        isRunning,
        "podman_writer_supervisor_outcome_uncertain",
      );
      if (isRunning) {
        await proveRunningAttachment(
          attachment,
          inspection,
          input.signal,
          "podman_writer_supervisor_outcome_uncertain",
        );
        fail("podman_writer_supervisor_outcome_uncertain");
      }
      const observedStatus = ownJsonValue(
        inspectionState,
        "Status",
        "podman_writer_supervisor_outcome_uncertain",
      );
      ensure(
        observedStatus !== "configured",
        "podman_writer_supervisor_outcome_uncertain",
      );
      return frozenRecord({
        evidence: evidence(
          supervisorId,
          record,
          "complete-stopped",
          stoppedProofId(launchAttemptId, digestOfRequest, record.containerId),
        ),
        receiptVersion: 1,
      });
    },
  );

  return frozenRecord({
    contractVersion: PODMAN_WRITER_SUPERVISOR_CONTRACT_VERSION,
    supervisorId,
    launchWriter,
    reconcileWriterLaunch,
  });
}

callIntrinsic(objectFreezeIntrinsic, Object, [PodmanWriterSupervisorError.prototype]);
callIntrinsic(objectFreezeIntrinsic, Object, [PodmanWriterSupervisorError]);
callIntrinsic(objectFreezeIntrinsic, Object, [createPodmanWriterSupervisor]);
