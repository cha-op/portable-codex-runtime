import { Buffer } from "node:buffer";
import { createHash, timingSafeEqual } from "node:crypto";
import { constants as fsConstants } from "node:fs";
import { lstat, open, realpath } from "node:fs/promises";
import { basename, dirname, isAbsolute, join, parse, resolve } from "node:path";
import { types as utilTypes } from "node:util";

import {
  acquireAdvisoryLock,
  sameFileIdentity,
} from "./advisory-lock.mjs";
import {
  recoveryPathHasExtendedAcl,
  recoveryPathHasUnsafeAncestorAcl,
} from "./stopped-tree.mjs";

const reflectApplyIntrinsic = Reflect.apply;
const reflectOwnKeysIntrinsic = Reflect.ownKeys;
const arrayEveryIntrinsic = Array.prototype.every;
const arrayIncludesIntrinsic = Array.prototype.includes;
const arrayIsArrayIntrinsic = Array.isArray;
const arrayJoinIntrinsic = Array.prototype.join;
const arrayPushIntrinsic = Array.prototype.push;
const arraySliceIntrinsic = Array.prototype.slice;
const arraySortIntrinsic = Array.prototype.sort;
const ArrayConstructor = Array;
const BigIntConstructor = BigInt;
const bufferAllocIntrinsic = Buffer.alloc;
const bufferAllocUnsafeIntrinsic = Buffer.allocUnsafe;
const bufferByteLengthIntrinsic = Buffer.byteLength;
const bufferConcatIntrinsic = Buffer.concat;
const bufferCopyIntrinsic = Buffer.prototype.copy;
const bufferEqualsIntrinsic = Buffer.prototype.equals;
const bufferFromIntrinsic = Buffer.from;
const bufferIndexOfIntrinsic = Buffer.prototype.indexOf;
const bufferIsBufferIntrinsic = Buffer.isBuffer;
const bufferReadUInt32BEIntrinsic = Buffer.prototype.readUInt32BE;
const bufferSubarrayIntrinsic = Buffer.prototype.subarray;
const bufferToStringIntrinsic = Buffer.prototype.toString;
const bufferWriteUInt32BEIntrinsic = Buffer.prototype.writeUInt32BE;
const BufferConstructor = Buffer;
const dateNowIntrinsic = Date.now;
const ErrorConstructor = Error;
const hashPrototype = Object.getPrototypeOf(createHash("sha256"));
const hashDigestIntrinsic = hashPrototype.digest;
const hashUpdateIntrinsic = hashPrototype.update;
const isPromiseValue = utilTypes.isPromise;
const isProxyValue = utilTypes.isProxy;
const mapDeleteIntrinsic = Map.prototype.delete;
const mapForEachIntrinsic = Map.prototype.forEach;
const mapGetIntrinsic = Map.prototype.get;
const mapHasIntrinsic = Map.prototype.has;
const mapSetIntrinsic = Map.prototype.set;
const MapConstructor = Map;
const mathMaxIntrinsic = Math.max;
const mathMinIntrinsic = Math.min;
const numberIsFiniteIntrinsic = Number.isFinite;
const numberIsSafeIntegerIntrinsic = Number.isSafeInteger;
const NumberConstructor = Number;
const objectCreateIntrinsic = Object.create;
const objectDefinePropertyIntrinsic = Object.defineProperty;
const objectFreezeIntrinsic = Object.freeze;
const objectGetOwnPropertyDescriptorIntrinsic = Object.getOwnPropertyDescriptor;
const objectGetPrototypeOfIntrinsic = Object.getPrototypeOf;
const objectHasOwnIntrinsic = Object.hasOwn;
const objectIsIntrinsic = Object.is;
const objectPrototype = Object.prototype;
const promiseResolveIntrinsic = Promise.resolve;
const promiseThenIntrinsic = Promise.prototype.then;
const PromiseConstructor = Promise;
const promisePrototype = Promise.prototype;
const regexpTestIntrinsic = RegExp.prototype.test;
const setTimeoutIntrinsic = setTimeout;
const setAddIntrinsic = Set.prototype.add;
const setHasIntrinsic = Set.prototype.has;
const SetConstructor = Set;
const StringConstructor = String;
const stringIncludesIntrinsic = String.prototype.includes;
const TypeErrorConstructor = TypeError;
const weakSetAddIntrinsic = WeakSet.prototype.add;
const weakSetHasIntrinsic = WeakSet.prototype.has;
const WeakSetConstructor = WeakSet;

function callIntrinsic(intrinsic, receiver, args) {
  return reflectApplyIntrinsic(intrinsic, receiver, args);
}

function arrayEvery(value, predicate) {
  return callIntrinsic(arrayEveryIntrinsic, value, [predicate]);
}

function arrayIncludes(value, candidate) {
  return callIntrinsic(arrayIncludesIntrinsic, value, [candidate]);
}

function arrayPush(value, candidate) {
  return callIntrinsic(arrayPushIntrinsic, value, [candidate]);
}

function arraySort(value, comparator) {
  return callIntrinsic(arraySortIntrinsic, value, [comparator]);
}

function bufferAlloc(size) {
  return callIntrinsic(bufferAllocIntrinsic, BufferConstructor, [size]);
}

function bufferAllocUnsafe(size) {
  return callIntrinsic(bufferAllocUnsafeIntrinsic, BufferConstructor, [size]);
}

function bufferByteLength(value, encoding) {
  return callIntrinsic(bufferByteLengthIntrinsic, BufferConstructor, [value, encoding]);
}

function bufferConcat(values) {
  return callIntrinsic(bufferConcatIntrinsic, BufferConstructor, [values]);
}

function bufferCopy(source, target, offset) {
  return callIntrinsic(bufferCopyIntrinsic, source, [target, offset]);
}

function bufferEquals(left, right) {
  return callIntrinsic(bufferEqualsIntrinsic, left, [right]);
}

function bufferFrom(value, encoding) {
  return callIntrinsic(
    bufferFromIntrinsic,
    BufferConstructor,
    encoding === undefined ? [value] : [value, encoding],
  );
}

function bufferIndexOf(buffer, value, offset) {
  return callIntrinsic(bufferIndexOfIntrinsic, buffer, [value, offset]);
}

function bufferIsBuffer(value) {
  return callIntrinsic(bufferIsBufferIntrinsic, BufferConstructor, [value]);
}

function bufferReadUInt32BE(buffer, offset) {
  return callIntrinsic(bufferReadUInt32BEIntrinsic, buffer, [offset]);
}

function bufferSubarray(buffer, start, end) {
  return callIntrinsic(bufferSubarrayIntrinsic, buffer, [start, end]);
}

function bufferToString(buffer, encoding) {
  return callIntrinsic(bufferToStringIntrinsic, buffer, [encoding]);
}

function bufferWriteUInt32BE(buffer, value, offset) {
  return callIntrinsic(bufferWriteUInt32BEIntrinsic, buffer, [value, offset]);
}

function mapDelete(map, key) {
  return callIntrinsic(mapDeleteIntrinsic, map, [key]);
}

function mapForEach(map, callback) {
  return callIntrinsic(mapForEachIntrinsic, map, [callback]);
}

function mapGet(map, key) {
  return callIntrinsic(mapGetIntrinsic, map, [key]);
}

function mapHas(map, key) {
  return callIntrinsic(mapHasIntrinsic, map, [key]);
}

function mapSet(map, key, value) {
  return callIntrinsic(mapSetIntrinsic, map, [key, value]);
}

function objectCreate(prototype) {
  return callIntrinsic(objectCreateIntrinsic, Object, [prototype]);
}

function objectDefineProperty(value, key, descriptor) {
  return callIntrinsic(objectDefinePropertyIntrinsic, Object, [value, key, descriptor]);
}

function objectFreeze(value) {
  return callIntrinsic(objectFreezeIntrinsic, Object, [value]);
}

function objectGetOwnPropertyDescriptor(value, key) {
  return callIntrinsic(objectGetOwnPropertyDescriptorIntrinsic, Object, [value, key]);
}

function objectGetPrototypeOf(value) {
  return callIntrinsic(objectGetPrototypeOfIntrinsic, Object, [value]);
}

function objectHasOwn(value, key) {
  return callIntrinsic(objectHasOwnIntrinsic, Object, [value, key]);
}

function reflectOwnKeys(value) {
  return callIntrinsic(reflectOwnKeysIntrinsic, undefined, [value]);
}

function regexpTest(pattern, value) {
  return callIntrinsic(regexpTestIntrinsic, pattern, [value]);
}

function stringIncludes(value, candidate) {
  return callIntrinsic(stringIncludesIntrinsic, value, [candidate]);
}

function ignoreRejection(promise) {
  return callIntrinsic(promiseThenIntrinsic, promise, [undefined, () => {}]);
}

function promiseResolve(value) {
  return callIntrinsic(promiseResolveIntrinsic, PromiseConstructor, [value]);
}

export const FILESYSTEM_IMAGE_PROVIDER_STATE_CONTRACT_VERSION = 1;
export const FILESYSTEM_IMAGE_PROVIDER_STATE_HEAD_CONTRACT_VERSION = 1;
export const FILESYSTEM_IMAGE_PROVIDER_STATE_LOCK_NAME =
  ".filesystem-image-provider-state.lock";
export const FILESYSTEM_IMAGE_PROVIDER_STATE_LEDGER_NAME = "state.log";

const MAX_CANONICAL_BYTES = 768 * 1024;
const MAX_CANONICAL_DEPTH = 32;
const MAX_CANONICAL_NODES = 16_384;
const MAX_FRAME_PAYLOAD_BYTES = 1024 * 1024;
const MAX_LEDGER_BYTES = 64 * 1024 * 1024;
const MAX_FRAME_COUNT = 65_535;
const MAX_PATH_BYTES = 4_096;
const MAX_PHYSICAL_OBJECT_ID_BYTES = 512;
const MAX_UINT64 = 18_446_744_073_709_551_615n;
const OPAQUE_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u;
const DECIMAL_PATTERN = /^(?:0|[1-9][0-9]*)$/u;
const SHA256_PATTERN = /^[0-9a-f]{64}$/u;
const OPERATION_KINDS = objectFreeze([
  "provision",
  "attach",
  "detach",
  "destroy",
  "checkpoint",
  "restore",
  "restore-attach",
]);
const LIFECYCLES = objectFreeze([
  "provisioned",
  "attached",
  "detached",
  "destroyed",
]);

// These sentinels contain invalid leading UTF-8 bytes, so neither can occur in
// a canonical JSON payload. That makes a later sentinel decisive evidence that
// an apparent short frame is committed-middle corruption rather than a torn
// final append.
const FRAME_MAGIC = bufferFrom([0x89, 0x46, 0x49, 0x50, 0x0d, 0x0a, 0x1a, 0x0a]);
const FRAME_END_MAGIC = bufferFrom([
  0x8a, 0x45, 0x4e, 0x44, 0x0d, 0x0a, 0x1a, 0x0a,
]);
const FRAME_METADATA_BYTES = 4 + 4 + 32;
const FRAME_HEADER_BYTES = FRAME_MAGIC.length + FRAME_METADATA_BYTES;
const FRAME_FOOTER_BYTES = FRAME_END_MAGIC.length + FRAME_METADATA_BYTES;
const FRAME_DOMAIN = bufferFrom(
  "portable-codex/filesystem-image-provider-state/frame/v1\0",
  "utf8",
);

const ERROR_MESSAGES = objectFreeze({
  invalid_request: "Filesystem image provider state request is invalid",
  unsafe_directory: "Filesystem image provider state directory is unsafe",
  io_failed: "Filesystem image provider state I/O failed",
  commit_outcome_uncertain:
    "Filesystem image provider state commit outcome is uncertain",
  corrupt_ledger: "Filesystem image provider state ledger is corrupt",
  operation_conflict: "Filesystem image provider operation conflicts with durable state",
  operation_already_prepared:
    "Filesystem image provider operation is already durably prepared",
});
const INTERNAL_ERRORS = new WeakSetConstructor();
const operationQueues = new MapConstructor();
const jsonParseIntrinsic = JSON.parse;
const jsonStringifyIntrinsic = JSON.stringify;

export class FilesystemImageProviderStateError extends ErrorConstructor {
  constructor(code) {
    if (!objectHasOwn(ERROR_MESSAGES, code)) {
      throw new TypeErrorConstructor("unsupported filesystem image provider state error");
    }
    super(ERROR_MESSAGES[code]);
    this.name = "FilesystemImageProviderStateError";
    this.code = code;
    this.commitState =
      code === "commit_outcome_uncertain" ? "uncertain" : "not-committed";
    this.retryable = false;
    objectFreeze(this);
  }
}

function stateError(code) {
  const error = new FilesystemImageProviderStateError(code);
  callIntrinsic(weakSetAddIntrinsic, INTERNAL_ERRORS, [error]);
  return error;
}

function fail(code) {
  throw stateError(code);
}

function ensure(condition, code) {
  if (!condition) fail(code);
}

function isInternalError(error) {
  return (
    error !== null &&
    arrayIncludes(["object", "function"], typeof error) &&
    !isProxyValue(error) &&
    callIntrinsic(weakSetHasIntrinsic, INTERNAL_ERRORS, [error])
  );
}

function safeErrorCode(error) {
  try {
    const descriptor = objectGetOwnPropertyDescriptor(error, "code");
    return descriptor && objectHasOwn(descriptor, "value")
      ? descriptor.value
      : undefined;
  } catch {
    return undefined;
  }
}

function inspectPlainObject(value, code) {
  ensure(
    value !== null &&
      typeof value === "object" &&
      !callIntrinsic(arrayIsArrayIntrinsic, ArrayConstructor, [value]) &&
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
      arrayEvery(keys, (key) => typeof key === "string"),
    code,
  );
  return keys;
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

function exactDataObject(value, allowedKeys, requiredKeys, code) {
  const keys = inspectPlainObject(value, code);
  ensure(
    keys.length <= allowedKeys.length &&
      arrayEvery(keys, (key) => arrayIncludes(allowedKeys, key)) &&
      arrayEvery(requiredKeys, (key) => arrayIncludes(keys, key)),
    code,
  );
  const normalized = objectCreate(null);
  for (const key of keys) normalized[key] = ownDataValue(value, key, code);
  return normalized;
}

function consumeBudget(state, bytes, code) {
  ensure(
    numberIsSafeIntegerIntrinsic(bytes) &&
      bytes >= 0 &&
      state.budget.bytes <= MAX_CANONICAL_BYTES - bytes,
    code,
  );
  state.budget.bytes += bytes;
}

function assertLosslessString(value, code, maxBytes = MAX_CANONICAL_BYTES) {
  ensure(typeof value === "string", code);
  const encoded = bufferFrom(value, "utf8");
  ensure(
    encoded.length <= maxBytes && bufferToString(encoded, "utf8") === value,
    code,
  );
  return value;
}

function canonicalize(
  value,
  code,
  state = {
    budget: { bytes: 0, nodes: 0 },
    depth: 0,
    seen: new SetConstructor(),
  },
) {
  state.budget.nodes += 1;
  ensure(
    state.budget.nodes <= MAX_CANONICAL_NODES &&
      state.depth <= MAX_CANONICAL_DEPTH,
    code,
  );
  if (value === null) {
    consumeBudget(state, 4, code);
    return null;
  }
  if (typeof value === "boolean") {
    consumeBudget(state, value ? 4 : 5, code);
    return value;
  }
  if (typeof value === "number") {
    ensure(numberIsFiniteIntrinsic(value), code);
    const normalized = callIntrinsic(objectIsIntrinsic, Object, [value, -0]) ? 0 : value;
    consumeBudget(
      state,
      bufferByteLength(jsonStringifyIntrinsic(normalized), "utf8"),
      code,
    );
    return normalized;
  }
  if (typeof value === "string") {
    assertLosslessString(value, code);
    consumeBudget(
      state,
      bufferByteLength(jsonStringifyIntrinsic(value), "utf8"),
      code,
    );
    return value;
  }
  ensure(
    typeof value === "object" &&
      !isProxyValue(value) &&
      !callIntrinsic(setHasIntrinsic, state.seen, [value]),
    code,
  );
  callIntrinsic(setAddIntrinsic, state.seen, [value]);
  const nestedState = () => ({ ...state, depth: state.depth + 1 });
  if (callIntrinsic(arrayIsArrayIntrinsic, ArrayConstructor, [value])) {
    let keys;
    try {
      keys = reflectOwnKeys(value);
    } catch {
      fail(code);
    }
    const keySet = new SetConstructor();
    for (const key of keys) callIntrinsic(setAddIntrinsic, keySet, [key]);
    ensure(
      numberIsSafeIntegerIntrinsic(value.length) &&
        keys.length === value.length + 1 &&
        callIntrinsic(setHasIntrinsic, keySet, ["length"]),
      code,
    );
    for (let index = 0; index < value.length; index += 1) {
      ensure(
        callIntrinsic(setHasIntrinsic, keySet, [StringConstructor(index)]),
        code,
      );
    }
    consumeBudget(state, 2 + mathMaxIntrinsic(0, value.length - 1), code);
    const result = [];
    for (let index = 0; index < value.length; index += 1) {
      const descriptor = objectGetOwnPropertyDescriptor(
        value,
        StringConstructor(index),
      );
      ensure(
        descriptor?.enumerable === true && objectHasOwn(descriptor, "value"),
        code,
      );
      arrayPush(result, canonicalize(descriptor.value, code, nestedState()));
    }
    return objectFreeze(result);
  }

  const keys = inspectPlainObject(value, code);
  consumeBudget(state, 2 + mathMaxIntrinsic(0, keys.length - 1), code);
  const result = {};
  const sortedKeys = callIntrinsic(arraySliceIntrinsic, keys, []);
  arraySort(sortedKeys);
  for (const key of sortedKeys) {
    assertLosslessString(key, code);
    consumeBudget(
      state,
      bufferByteLength(jsonStringifyIntrinsic(key), "utf8") + 1,
      code,
    );
    objectDefineProperty(result, key, {
      enumerable: true,
      value: canonicalize(ownDataValue(value, key, code), code, nestedState()),
    });
  }
  return objectFreeze(result);
}

function canonicalObject(value, code) {
  inspectPlainObject(value, code);
  return canonicalize(value, code);
}

function canonicalString(value) {
  if (value === null) return "null";
  if (typeof value === "boolean") return value ? "true" : "false";
  if (typeof value === "number" || typeof value === "string") {
    return jsonStringifyIntrinsic(value);
  }
  if (callIntrinsic(arrayIsArrayIntrinsic, ArrayConstructor, [value])) {
    const items = [];
    for (let index = 0; index < value.length; index += 1) {
      arrayPush(items, canonicalString(value[index]));
    }
    return `[${callIntrinsic(arrayJoinIntrinsic, items, [","])}]`;
  }
  const fields = [];
  const sortedKeys = reflectOwnKeys(value);
  arraySort(sortedKeys);
  for (const key of sortedKeys) {
    const descriptor = objectGetOwnPropertyDescriptor(value, key);
    arrayPush(
      fields,
      `${jsonStringifyIntrinsic(key)}:${canonicalString(descriptor.value)}`,
    );
  }
  return `{${callIntrinsic(arrayJoinIntrinsic, fields, [","])}}`;
}

function canonicalEqual(left, right) {
  return canonicalString(left) === canonicalString(right);
}

function canonicalOpaqueId(value, code) {
  ensure(typeof value === "string" && regexpTest(OPAQUE_ID_PATTERN, value), code);
  return value;
}

function canonicalOperationKind(value, code) {
  ensure(typeof value === "string" && arrayIncludes(OPERATION_KINDS, value), code);
  return value;
}

function canonicalUint64(value, code, { positive = false } = {}) {
  ensure(typeof value === "string" && regexpTest(DECIMAL_PATTERN, value), code);
  let parsed;
  try {
    parsed = BigIntConstructor(value);
  } catch {
    fail(code);
  }
  ensure(parsed <= MAX_UINT64 && (!positive || parsed > 0n), code);
  return objectFreeze({ parsed, value });
}

function incrementUint64(value, code) {
  const parsed = canonicalUint64(value, code, { positive: true }).parsed;
  ensure(parsed < MAX_UINT64, code);
  return StringConstructor(parsed + 1n);
}

function canonicalAbsolutePath(value, code) {
  assertLosslessString(value, code, MAX_PATH_BYTES);
  ensure(
    !stringIncludes(value, "\0") &&
      isAbsolute(value) &&
      resolve(value) === value &&
      value !== parse(value).root,
    code,
  );
  return value;
}

function canonicalPhysicalIdentity(value, code) {
  const identity = exactDataObject(
    value,
    ["filesystemId", "objectIdentityScheme", "objectId"],
    ["filesystemId", "objectIdentityScheme", "objectId"],
    code,
  );
  const objectId = assertLosslessString(
    identity.objectId,
    code,
    MAX_PHYSICAL_OBJECT_ID_BYTES,
  );
  ensure(objectId.length > 0 && !stringIncludes(objectId, "\0"), code);
  return objectFreeze({
    filesystemId: canonicalOpaqueId(identity.filesystemId, code),
    objectIdentityScheme: canonicalOpaqueId(identity.objectIdentityScheme, code),
    objectId,
  });
}

function canonicalMount(value, code) {
  const mount = exactDataObject(
    value,
    ["mountPath", "imageIdentity", "rootIdentity"],
    ["mountPath", "imageIdentity", "rootIdentity"],
    code,
  );
  return objectFreeze({
    mountPath: canonicalAbsolutePath(mount.mountPath, code),
    imageIdentity: canonicalPhysicalIdentity(mount.imageIdentity, code),
    rootIdentity: canonicalPhysicalIdentity(mount.rootIdentity, code),
  });
}

function canonicalDataRoot(value, code) {
  const dataRoot = exactDataObject(
    value,
    ["rootPath", "imageIdentity", "rootIdentity"],
    ["rootPath", "imageIdentity", "rootIdentity"],
    code,
  );
  return objectFreeze({
    rootPath: canonicalAbsolutePath(dataRoot.rootPath, code),
    imageIdentity: canonicalPhysicalIdentity(dataRoot.imageIdentity, code),
    rootIdentity: canonicalPhysicalIdentity(dataRoot.rootIdentity, code),
  });
}

function canonicalWriterAuthority(value, code) {
  const authority = exactDataObject(
    value,
    ["fencingEpoch", "holderId", "leaseId"],
    ["fencingEpoch", "holderId", "leaseId"],
    code,
  );
  return objectFreeze({
    fencingEpoch: canonicalUint64(authority.fencingEpoch, code, {
      positive: true,
    }).value,
    holderId: canonicalOpaqueId(authority.holderId, code),
    leaseId: canonicalOpaqueId(authority.leaseId, code),
  });
}

function canonicalAttachment(value, code) {
  const attachment = exactDataObject(
    value,
    [
      "attachmentId",
      "leaseId",
      "holderId",
      "fencingEpoch",
      "rootPath",
      "proofId",
      "imageIdentity",
      "rootIdentity",
    ],
    [
      "attachmentId",
      "leaseId",
      "holderId",
      "fencingEpoch",
      "rootPath",
      "proofId",
      "imageIdentity",
      "rootIdentity",
    ],
    code,
  );
  return objectFreeze({
    attachmentId: canonicalOpaqueId(attachment.attachmentId, code),
    leaseId: canonicalOpaqueId(attachment.leaseId, code),
    holderId: canonicalOpaqueId(attachment.holderId, code),
    fencingEpoch: canonicalUint64(attachment.fencingEpoch, code).value,
    rootPath: canonicalAbsolutePath(attachment.rootPath, code),
    proofId: canonicalOpaqueId(attachment.proofId, code),
    imageIdentity: canonicalPhysicalIdentity(attachment.imageIdentity, code),
    rootIdentity: canonicalPhysicalIdentity(attachment.rootIdentity, code),
  });
}

const STORAGE_STATE_KEYS = objectFreeze([
  "storageId",
  "sessionId",
  "backendId",
  "filesystemId",
  "imagePath",
  "lifecycle",
  "revision",
  "writerEpoch",
  "writerAuthority",
  "mount",
  "publicationControlIdentity",
  "dataRoot",
  "attachment",
]);

function canonicalStorageState(value, code) {
  const state = exactDataObject(
    value,
    STORAGE_STATE_KEYS,
    STORAGE_STATE_KEYS,
    code,
  );
  ensure(
    typeof state.lifecycle === "string" && arrayIncludes(LIFECYCLES, state.lifecycle),
    code,
  );
  const revision = canonicalUint64(state.revision, code, { positive: true }).value;
  const writerEpoch = canonicalUint64(state.writerEpoch, code).value;
  const writerAuthority =
    state.writerAuthority === null
      ? null
      : canonicalWriterAuthority(state.writerAuthority, code);
  const mount = state.mount === null ? null : canonicalMount(state.mount, code);
  const publicationControlIdentity =
    state.publicationControlIdentity === null
      ? null
      : canonicalPhysicalIdentity(state.publicationControlIdentity, code);
  const dataRoot =
    state.dataRoot === null ? null : canonicalDataRoot(state.dataRoot, code);
  const attachment =
    state.attachment === null ? null : canonicalAttachment(state.attachment, code);
  ensure(
    state.lifecycle === "destroyed" ? mount === null : mount !== null,
    code,
  );
  ensure(
    state.lifecycle === "destroyed"
      ? publicationControlIdentity === null
      : publicationControlIdentity !== null,
    code,
  );
  ensure(
    state.lifecycle === "attached" ? attachment !== null : attachment === null,
    code,
  );
  ensure(state.lifecycle === "destroyed" ? dataRoot === null : true, code);
  if (writerAuthority !== null) {
    ensure(writerAuthority.fencingEpoch === writerEpoch, code);
  }
  if (mount !== null) {
    ensure(
      mount.rootIdentity.filesystemId === state.filesystemId,
      code,
    );
  }
  if (publicationControlIdentity !== null) {
    ensure(
      publicationControlIdentity.filesystemId === state.filesystemId &&
        mount !== null &&
        publicationControlIdentity.objectIdentityScheme ===
          mount.rootIdentity.objectIdentityScheme &&
        publicationControlIdentity.objectId !== mount.rootIdentity.objectId,
      code,
    );
  }
  if (dataRoot !== null) {
    ensure(
      mount !== null &&
        dirname(dataRoot.rootPath) === mount.mountPath &&
        !arrayIncludes(["", ".", ".."], basename(dataRoot.rootPath)) &&
        canonicalEqual(dataRoot.imageIdentity, mount.imageIdentity) &&
        dataRoot.rootIdentity.filesystemId === state.filesystemId &&
        !canonicalEqual(dataRoot.rootIdentity, mount.rootIdentity),
      code,
    );
  }
  if (attachment !== null) {
    ensure(
      attachment.fencingEpoch === writerEpoch &&
        writerAuthority !== null &&
        attachment.fencingEpoch === writerAuthority.fencingEpoch &&
        attachment.holderId === writerAuthority.holderId &&
        attachment.leaseId === writerAuthority.leaseId &&
        dataRoot !== null &&
        attachment.rootPath === dataRoot.rootPath &&
        canonicalEqual(attachment.imageIdentity, dataRoot.imageIdentity) &&
        canonicalEqual(attachment.rootIdentity, dataRoot.rootIdentity),
      code,
    );
  }
  return objectFreeze({
    storageId: canonicalOpaqueId(state.storageId, code),
    sessionId: canonicalOpaqueId(state.sessionId, code),
    backendId: canonicalOpaqueId(state.backendId, code),
    filesystemId: canonicalOpaqueId(state.filesystemId, code),
    imagePath: canonicalAbsolutePath(state.imagePath, code),
    lifecycle: state.lifecycle,
    revision,
    writerEpoch,
    writerAuthority,
    mount,
    publicationControlIdentity,
    dataRoot,
    attachment,
  });
}

function expectedStorageState(value) {
  return value === null
    ? null
    : objectFreeze({ lifecycle: value.lifecycle, revision: value.revision });
}

function canonicalExpectedStorage(value, code) {
  if (value === null) return null;
  const expected = exactDataObject(
    value,
    ["lifecycle", "revision"],
    ["lifecycle", "revision"],
    code,
  );
  ensure(
    typeof expected.lifecycle === "string" &&
      arrayIncludes(LIFECYCLES, expected.lifecycle),
    code,
  );
  return objectFreeze({
    lifecycle: expected.lifecycle,
    revision: canonicalUint64(expected.revision, code, { positive: true }).value,
  });
}

function assertMountStable(previous, next, code) {
  ensure(
    previous.mount !== null &&
      next.mount !== null &&
      canonicalEqual(previous.mount, next.mount) &&
      previous.publicationControlIdentity !== null &&
      next.publicationControlIdentity !== null &&
      canonicalEqual(
        previous.publicationControlIdentity,
        next.publicationControlIdentity,
      ),
    code,
  );
}

function assertPreparePrecondition(current, kind, code) {
  if (kind === "provision") {
    ensure(current === null, code);
    return;
  }
  ensure(current !== null && current.lifecycle !== "destroyed", code);
  if (arrayIncludes(["attach", "restore-attach"], kind)) {
    ensure(arrayIncludes(["provisioned", "detached"], current.lifecycle), code);
    return;
  }
  if (kind === "detach") {
    ensure(current.lifecycle === "attached", code);
    return;
  }
  if (kind === "destroy") {
    ensure(arrayIncludes(["provisioned", "detached"], current.lifecycle), code);
    return;
  }
  if (kind === "checkpoint") return;
  if (kind === "restore") {
    ensure(arrayIncludes(["provisioned", "detached"], current.lifecycle), code);
    return;
  }
  fail(code);
}

function pendingOperationForStorage(state, storageId) {
  let pending = null;
  mapForEach(state.operations, (operation) => {
    if (
      pending === null &&
      operation.state === "prepared" &&
      operation.storageId === storageId
    ) {
      pending = operation;
    }
  });
  return pending;
}

function assertStorageTransition(previous, next, kind, code) {
  ensure(next.storageId !== undefined, code);
  if (previous === null) {
    ensure(
      kind === "provision" &&
        next.lifecycle === "provisioned" &&
        next.revision === "1" &&
        next.writerEpoch === "0" &&
        next.writerAuthority === null &&
        next.mount !== null &&
        next.publicationControlIdentity !== null &&
        next.dataRoot === null &&
        next.attachment === null,
      code,
    );
    return;
  }
  ensure(
    previous.lifecycle !== "destroyed" &&
      next.storageId === previous.storageId &&
      next.sessionId === previous.sessionId &&
      next.backendId === previous.backendId &&
      next.filesystemId === previous.filesystemId &&
      next.imagePath === previous.imagePath &&
      next.revision === incrementUint64(previous.revision, code),
    code,
  );

  if (kind === "attach") {
    ensure(
      arrayIncludes(["provisioned", "detached"], previous.lifecycle) &&
        next.lifecycle === "attached" &&
        next.dataRoot !== null &&
        (previous.dataRoot === null ||
          canonicalEqual(previous.dataRoot, next.dataRoot)) &&
        canonicalUint64(next.writerEpoch, code).parsed >
          canonicalUint64(previous.writerEpoch, code).parsed,
      code,
    );
    assertMountStable(previous, next, code);
    return;
  }
  if (kind === "restore-attach") {
    ensure(
      arrayIncludes(["provisioned", "detached"], previous.lifecycle) &&
        next.lifecycle === "attached" &&
        next.dataRoot !== null &&
        (previous.dataRoot === null ||
          !canonicalEqual(previous.dataRoot, next.dataRoot)) &&
        canonicalUint64(next.writerEpoch, code).parsed >
          canonicalUint64(previous.writerEpoch, code).parsed,
      code,
    );
    assertMountStable(previous, next, code);
    return;
  }
  if (kind === "detach") {
    ensure(
      previous.lifecycle === "attached" &&
        next.lifecycle === "detached" &&
        next.writerEpoch === previous.writerEpoch &&
        canonicalEqual(previous.writerAuthority, next.writerAuthority) &&
        canonicalEqual(previous.dataRoot, next.dataRoot),
      code,
    );
    assertMountStable(previous, next, code);
    return;
  }
  if (kind === "destroy") {
    ensure(
      arrayIncludes(["provisioned", "detached"], previous.lifecycle) &&
        next.lifecycle === "destroyed" &&
        next.writerEpoch === previous.writerEpoch &&
        canonicalEqual(previous.writerAuthority, next.writerAuthority) &&
        next.mount === null &&
        next.publicationControlIdentity === null &&
        next.dataRoot === null &&
        next.attachment === null,
      code,
    );
    return;
  }
  if (arrayIncludes(["checkpoint", "restore"], kind)) {
    ensure(
      next.lifecycle === previous.lifecycle &&
        next.writerEpoch === previous.writerEpoch &&
        canonicalEqual(previous.writerAuthority, next.writerAuthority),
      code,
    );
    assertMountStable(previous, next, code);
    ensure(
      canonicalEqual(previous.attachment, next.attachment),
      code,
    );
    ensure(canonicalEqual(previous.dataRoot, next.dataRoot), code);
    return;
  }
  fail(code);
}

function canonicalPreviousChecksum(value, code) {
  ensure(
    value === null || (typeof value === "string" && regexpTest(SHA256_PATTERN, value)),
    code,
  );
  return value;
}

function canonicalSequence(value, code) {
  ensure(
    numberIsSafeIntegerIntrinsic(value) && value >= 1 && value <= MAX_FRAME_COUNT,
    code,
  );
  return value;
}

function canonicalLedgerHead(value, code) {
  const head = exactDataObject(
    value,
    ["contractVersion", "sequence", "lastChecksum", "ledgerBytes"],
    ["contractVersion", "sequence", "lastChecksum", "ledgerBytes"],
    code,
  );
  ensure(
    head.contractVersion ===
      FILESYSTEM_IMAGE_PROVIDER_STATE_HEAD_CONTRACT_VERSION &&
      numberIsSafeIntegerIntrinsic(head.sequence) &&
      head.sequence >= 0 &&
      head.sequence <= MAX_FRAME_COUNT &&
      numberIsSafeIntegerIntrinsic(head.ledgerBytes) &&
      head.ledgerBytes >= 0 &&
      head.ledgerBytes <= MAX_LEDGER_BYTES,
    code,
  );
  const lastChecksum = canonicalPreviousChecksum(head.lastChecksum, code);
  ensure(
    head.sequence === 0
      ? lastChecksum === null && head.ledgerBytes === 0
      : lastChecksum !== null && head.ledgerBytes > 0,
    code,
  );
  return objectFreeze({
    contractVersion: FILESYSTEM_IMAGE_PROVIDER_STATE_HEAD_CONTRACT_VERSION,
    sequence: head.sequence,
    lastChecksum,
    ledgerBytes: head.ledgerBytes,
  });
}

export function normalizeFilesystemImageProviderStateHead(value) {
  return canonicalLedgerHead(value, "invalid_request");
}

function canonicalHeadAnchor(value, code) {
  // The caller must scope this collaborator to durable trusted storage outside
  // the replaceable ledger directory. Captured callbacks are always invoked
  // receiver-less, so neither callback receives this state object implicitly.
  const anchor = exactDataObject(
    value,
    ["readHead", "compareAndAdvance"],
    ["readHead", "compareAndAdvance"],
    code,
  );
  ensure(
    typeof anchor.readHead === "function" &&
      !isProxyValue(anchor.readHead) &&
      typeof anchor.compareAndAdvance === "function" &&
      !isProxyValue(anchor.compareAndAdvance),
    code,
  );
  return objectFreeze({
    readHead: anchor.readHead,
    compareAndAdvance: anchor.compareAndAdvance,
  });
}

function normalizePreparedFrame(value, code) {
  const frame = exactDataObject(
    value,
    [
      "contractVersion",
      "kind",
      "operationId",
      "previousChecksum",
      "request",
      "sequence",
      "storageId",
      "storageStateBefore",
      "type",
    ],
    [
      "contractVersion",
      "kind",
      "operationId",
      "previousChecksum",
      "request",
      "sequence",
      "storageId",
      "storageStateBefore",
      "type",
    ],
    code,
  );
  ensure(
    frame.contractVersion === FILESYSTEM_IMAGE_PROVIDER_STATE_CONTRACT_VERSION &&
      frame.type === "prepared",
    code,
  );
  return objectFreeze({
    contractVersion: FILESYSTEM_IMAGE_PROVIDER_STATE_CONTRACT_VERSION,
    kind: canonicalOperationKind(frame.kind, code),
    operationId: canonicalOpaqueId(frame.operationId, code),
    previousChecksum: canonicalPreviousChecksum(frame.previousChecksum, code),
    request: canonicalObject(frame.request, code),
    sequence: canonicalSequence(frame.sequence, code),
    storageId: canonicalOpaqueId(frame.storageId, code),
    storageStateBefore:
      frame.storageStateBefore === null
        ? null
        : canonicalStorageState(frame.storageStateBefore, code),
    type: "prepared",
  });
}

function normalizeCommittedFrame(value, code) {
  const frame = exactDataObject(
    value,
    [
      "contractVersion",
      "expectedStorage",
      "operationId",
      "previousChecksum",
      "request",
      "result",
      "sequence",
      "storageState",
      "type",
    ],
    [
      "contractVersion",
      "expectedStorage",
      "operationId",
      "previousChecksum",
      "request",
      "result",
      "sequence",
      "storageState",
      "type",
    ],
    code,
  );
  ensure(
    frame.contractVersion === FILESYSTEM_IMAGE_PROVIDER_STATE_CONTRACT_VERSION &&
      frame.type === "committed",
    code,
  );
  return objectFreeze({
    contractVersion: FILESYSTEM_IMAGE_PROVIDER_STATE_CONTRACT_VERSION,
    expectedStorage: canonicalExpectedStorage(frame.expectedStorage, code),
    operationId: canonicalOpaqueId(frame.operationId, code),
    previousChecksum: canonicalPreviousChecksum(frame.previousChecksum, code),
    request: canonicalObject(frame.request, code),
    result: canonicalObject(frame.result, code),
    sequence: canonicalSequence(frame.sequence, code),
    storageState: canonicalStorageState(frame.storageState, code),
    type: "committed",
  });
}

function normalizeFrame(value, code) {
  const keys = inspectPlainObject(value, code);
  ensure(arrayIncludes(keys, "type"), code);
  const type = ownDataValue(value, "type", code);
  if (type === "prepared") return normalizePreparedFrame(value, code);
  if (type === "committed") return normalizeCommittedFrame(value, code);
  fail(code);
}

function frameChecksum(payload, payloadLength, sequence) {
  const metadata = bufferAllocUnsafe(8);
  bufferWriteUInt32BE(metadata, payloadLength, 0);
  bufferWriteUInt32BE(metadata, sequence, 4);
  const hash = createHash("sha256");
  callIntrinsic(hashUpdateIntrinsic, hash, [FRAME_DOMAIN]);
  callIntrinsic(hashUpdateIntrinsic, hash, [metadata]);
  callIntrinsic(hashUpdateIntrinsic, hash, [payload]);
  return callIntrinsic(hashDigestIntrinsic, hash, []);
}

function encodeFrame(frame) {
  const payload = bufferFrom(canonicalString(frame), "utf8");
  ensure(
    payload.length > 0 && payload.length <= MAX_FRAME_PAYLOAD_BYTES,
    "invalid_request",
  );
  const checksum = frameChecksum(payload, payload.length, frame.sequence);
  const header = bufferAllocUnsafe(FRAME_HEADER_BYTES);
  bufferCopy(FRAME_MAGIC, header, 0);
  bufferWriteUInt32BE(header, payload.length, FRAME_MAGIC.length);
  bufferWriteUInt32BE(header, frame.sequence, FRAME_MAGIC.length + 4);
  bufferCopy(checksum, header, FRAME_MAGIC.length + 8);
  const footer = bufferAllocUnsafe(FRAME_FOOTER_BYTES);
  bufferCopy(FRAME_END_MAGIC, footer, 0);
  bufferWriteUInt32BE(footer, payload.length, FRAME_END_MAGIC.length);
  bufferWriteUInt32BE(footer, frame.sequence, FRAME_END_MAGIC.length + 4);
  bufferCopy(checksum, footer, FRAME_END_MAGIC.length + 8);
  return objectFreeze({
    bytes: bufferConcat([header, payload, footer]),
    checksum: bufferToString(checksum, "hex"),
    frame,
  });
}

function parseCanonicalFrame(payload) {
  const text = bufferToString(payload, "utf8");
  ensure(bufferEquals(bufferFrom(text, "utf8"), payload), "corrupt_ledger");
  let parsed;
  try {
    parsed = jsonParseIntrinsic(text);
  } catch {
    fail("corrupt_ledger");
  }
  const frame = normalizeFrame(parsed, "corrupt_ledger");
  ensure(canonicalString(frame) === text, "corrupt_ledger");
  return frame;
}

function emptyReplayState() {
  return {
    lastChecksum: null,
    operations: new MapConstructor(),
    sequence: 0,
    storages: new MapConstructor(),
  };
}

function cloneReplayState(state) {
  const copy = {
    lastChecksum: state.lastChecksum,
    operations: new MapConstructor(),
    sequence: state.sequence,
    storages: new MapConstructor(),
  };
  mapForEach(state.operations, (value, key) => mapSet(copy.operations, key, value));
  mapForEach(state.storages, (value, key) => mapSet(copy.storages, key, value));
  return copy;
}

function preparedRecord(frame) {
  return objectFreeze({
    kind: frame.kind,
    operationId: frame.operationId,
    request: frame.request,
    state: "prepared",
    storageId: frame.storageId,
    storageStateBefore: frame.storageStateBefore,
  });
}

function committedRecord(prepared, frame) {
  return objectFreeze({
    kind: prepared.kind,
    operationId: prepared.operationId,
    request: prepared.request,
    state: "committed",
    storageId: prepared.storageId,
    storageStateBefore: prepared.storageStateBefore,
    expectedStorage: frame.expectedStorage,
    result: frame.result,
    storageState: frame.storageState,
  });
}

function applyFrame(state, frame, checksum, code) {
  ensure(
    frame.sequence === state.sequence + 1 &&
      frame.previousChecksum === state.lastChecksum,
    code,
  );
  if (frame.type === "prepared") {
    ensure(!mapHas(state.operations, frame.operationId), code);
    const currentStorage = mapGet(state.storages, frame.storageId) ?? null;
    ensure(
      pendingOperationForStorage(state, frame.storageId) === null &&
        canonicalEqual(currentStorage, frame.storageStateBefore),
      code,
    );
    assertPreparePrecondition(currentStorage, frame.kind, code);
    mapSet(state.operations, frame.operationId, preparedRecord(frame));
  } else {
    const operation = mapGet(state.operations, frame.operationId);
    ensure(
      operation?.state === "prepared" &&
        canonicalEqual(operation.request, frame.request),
      code,
    );
    const currentStorage = mapGet(state.storages, operation.storageId) ?? null;
    ensure(
      canonicalEqual(currentStorage, operation.storageStateBefore) &&
        canonicalEqual(frame.expectedStorage, expectedStorageState(currentStorage)) &&
        frame.storageState.storageId === operation.storageId,
      code,
    );
    assertStorageTransition(currentStorage, frame.storageState, operation.kind, code);
    const committed = committedRecord(operation, frame);
    mapSet(state.operations, frame.operationId, committed);
    mapSet(state.storages, operation.storageId, frame.storageState);
  }
  state.sequence = frame.sequence;
  state.lastChecksum = checksum;
}

function hasLaterSentinel(bytes, start, expectedFooterStart) {
  const laterFrame = bufferIndexOf(bytes, FRAME_MAGIC, start);
  if (laterFrame !== -1) return true;
  const laterFooter = bufferIndexOf(bytes, FRAME_END_MAGIC, start);
  return laterFooter !== -1 && laterFooter !== expectedFooterStart;
}

function parseLedger(bytes) {
  ensure(bufferIsBuffer(bytes) && bytes.length <= MAX_LEDGER_BYTES, "corrupt_ledger");
  const state = emptyReplayState();
  let offset = 0;
  while (offset < bytes.length) {
    const remaining = bytes.length - offset;
    if (remaining < FRAME_HEADER_BYTES) {
      const prefixLength = mathMinIntrinsic(remaining, FRAME_MAGIC.length);
      ensure(
        bufferEquals(
          bufferSubarray(bytes, offset, offset + prefixLength),
          bufferSubarray(FRAME_MAGIC, 0, prefixLength),
        ),
        "corrupt_ledger",
      );
      return { state, tailOffset: offset };
    }
    ensure(
      bufferEquals(
        bufferSubarray(bytes, offset, offset + FRAME_MAGIC.length),
        FRAME_MAGIC,
      ),
      "corrupt_ledger",
    );
    const payloadLength = bufferReadUInt32BE(bytes, offset + FRAME_MAGIC.length);
    const sequence = bufferReadUInt32BE(
      bytes,
      offset + FRAME_MAGIC.length + 4,
    );
    ensure(
      payloadLength > 0 &&
        payloadLength <= MAX_FRAME_PAYLOAD_BYTES &&
        sequence === state.sequence + 1 &&
        sequence <= MAX_FRAME_COUNT,
      "corrupt_ledger",
    );
    const payloadStart = offset + FRAME_HEADER_BYTES;
    const footerStart = payloadStart + payloadLength;
    const frameEnd = footerStart + FRAME_FOOTER_BYTES;
    if (frameEnd > bytes.length) {
      ensure(
        !hasLaterSentinel(bytes, payloadStart, footerStart),
        "corrupt_ledger",
      );
      if (footerStart <= bytes.length) {
        const completePayload = bufferSubarray(bytes, payloadStart, footerStart);
        const headerChecksum = bufferSubarray(
          bytes,
          offset + FRAME_MAGIC.length + 8,
          offset + FRAME_HEADER_BYTES,
        );
        const actualChecksum = frameChecksum(
          completePayload,
          payloadLength,
          sequence,
        );
        ensure(
          timingSafeEqual(headerChecksum, actualChecksum),
          "corrupt_ledger",
        );
        const completeFrame = parseCanonicalFrame(completePayload);
        ensure(completeFrame.sequence === sequence, "corrupt_ledger");
        applyFrame(
          cloneReplayState(state),
          completeFrame,
          bufferToString(actualChecksum, "hex"),
          "corrupt_ledger",
        );
      }
      return { state, tailOffset: offset };
    }

    const headerChecksum = bufferSubarray(
      bytes,
      offset + FRAME_MAGIC.length + 8,
      offset + FRAME_HEADER_BYTES,
    );
    ensure(
      bufferEquals(
        bufferSubarray(
          bytes,
          footerStart,
          footerStart + FRAME_END_MAGIC.length,
        ),
        FRAME_END_MAGIC,
      ) &&
        bufferReadUInt32BE(bytes, footerStart + FRAME_END_MAGIC.length) ===
          payloadLength &&
        bufferReadUInt32BE(bytes, footerStart + FRAME_END_MAGIC.length + 4) ===
          sequence,
      "corrupt_ledger",
    );
    const footerChecksum = bufferSubarray(
      bytes,
      footerStart + FRAME_END_MAGIC.length + 8,
      frameEnd,
    );
    const payload = bufferSubarray(bytes, payloadStart, footerStart);
    const actualChecksum = frameChecksum(payload, payloadLength, sequence);
    ensure(
      timingSafeEqual(headerChecksum, footerChecksum) &&
        timingSafeEqual(headerChecksum, actualChecksum),
      "corrupt_ledger",
    );
    const frame = parseCanonicalFrame(payload);
    ensure(frame.sequence === sequence, "corrupt_ledger");
    applyFrame(
      state,
      frame,
      bufferToString(actualChecksum, "hex"),
      "corrupt_ledger",
    );
    offset = frameEnd;
  }
  return { state, tailOffset: null };
}

function replayHead(bytes, parsed) {
  const ledgerBytes = parsed.tailOffset ?? bytes.length;
  return canonicalLedgerHead(
    {
      contractVersion: FILESYSTEM_IMAGE_PROVIDER_STATE_HEAD_CONTRACT_VERSION,
      sequence: parsed.state.sequence,
      lastChecksum: parsed.state.lastChecksum,
      ledgerBytes,
    },
    "corrupt_ledger",
  );
}

function reconcileLedgerWithTrustedHead(bytes, trustedHead) {
  // The external head is authoritative. First prove that its exact byte range
  // is a complete canonical ledger prefix; only then may bytes beyond that
  // boundary be considered an unanchored append rather than committed data.
  ensure(trustedHead.ledgerBytes <= bytes.length, "corrupt_ledger");
  const prefixBytes = bufferSubarray(bytes, 0, trustedHead.ledgerBytes);
  const prefix = parseLedger(prefixBytes);
  ensure(
    prefix.tailOffset === null &&
      canonicalEqual(replayHead(prefixBytes, prefix), trustedHead),
    "corrupt_ledger",
  );

  if (trustedHead.ledgerBytes === bytes.length) {
    return objectFreeze({ parsed: prefix, truncateOffset: null });
  }

  const whole = parseLedger(bytes);
  const oneCompleteUnanchoredFrame =
    whole.tailOffset === null &&
    whole.state.sequence === trustedHead.sequence + 1;
  const oneTornUnanchoredFrame =
    whole.tailOffset === trustedHead.ledgerBytes &&
    canonicalEqual(replayHead(bytes, whole), trustedHead);
  ensure(
    oneCompleteUnanchoredFrame || oneTornUnanchoredFrame,
    "corrupt_ledger",
  );
  return objectFreeze({
    parsed: prefix,
    truncateOffset: trustedHead.ledgerBytes,
  });
}

function isExactNativePromise(value) {
  if (
    value === null ||
    typeof value !== "object" ||
    isProxyValue(value) ||
    !isPromiseValue(value)
  ) {
    return false;
  }
  try {
    return (
      objectGetPrototypeOf(value) === promisePrototype &&
      objectGetOwnPropertyDescriptor(value, "catch") === undefined &&
      objectGetOwnPropertyDescriptor(value, "constructor") === undefined &&
      objectGetOwnPropertyDescriptor(value, "finally") === undefined &&
      objectGetOwnPropertyDescriptor(value, "then") === undefined
    );
  } catch {
    return false;
  }
}

function invokeNativePromise(operation, args, code) {
  let result;
  try {
    result = callIntrinsic(operation, undefined, args);
  } catch {
    fail(code);
  }
  ensure(isExactNativePromise(result), code);
  return result;
}

async function readTrustedLedgerHead(anchor) {
  let value;
  try {
    value = await invokeNativePromise(anchor.readHead, [], "io_failed");
  } catch (error) {
    if (isInternalError(error)) throw error;
    fail("io_failed");
  }
  return canonicalLedgerHead(value, "io_failed");
}

async function advanceTrustedLedgerHead(anchor, expectedHead, nextHead) {
  // A true acknowledgement is the collaborator's assertion that it durably
  // and atomically replaced exactly expectedHead with nextHead. Any other
  // outcome is unusable after ledger write-start and therefore uncertain.
  const request = objectFreeze({ expectedHead, nextHead });
  let acknowledged;
  try {
    acknowledged = await invokeNativePromise(
      anchor.compareAndAdvance,
      [request],
      "io_failed",
    );
  } catch (error) {
    if (isInternalError(error)) throw error;
    fail("io_failed");
  }
  ensure(acknowledged === true, "io_failed");
}

function integerAsBigInt(value) {
  if (typeof value === "bigint") return value;
  return numberIsSafeIntegerIntrinsic(value) ? BigIntConstructor(value) : null;
}

function ancestorPermissionsAreSafe(metadata, childUid, currentUid) {
  const mode = integerAsBigInt(metadata.mode);
  const uid = integerAsBigInt(metadata.uid);
  const child = integerAsBigInt(childUid);
  const owner = integerAsBigInt(currentUid);
  if (arrayIncludes([mode, uid, child, owner], null) || !metadata.isDirectory()) {
    return false;
  }
  const trustedOwner = uid === owner || uid === 0n;
  const trustedChild = child === owner || child === 0n;
  const writable = (mode & 0o022n) !== 0n;
  const stickyProtectsChild = (mode & 0o1000n) !== 0n && trustedChild;
  return trustedOwner && (!writable || stickyProtectsChild);
}

async function inspectAcl(inspector, path, code) {
  let unsafe;
  try {
    unsafe = await inspector(path);
  } catch {
    fail(code);
  }
  ensure(unsafe === false, code);
}

async function openDirectoryAuthority(
  directory,
  { expectedPin, inspectAncestorAcl, inspectDirectoryAcl },
) {
  ensure(
    typeof directory === "string" &&
      isAbsolute(directory) &&
      resolve(directory) === directory &&
      directory !== parse(directory).root,
    "unsafe_directory",
  );
  const currentUid = process.geteuid?.() ?? process.getuid?.();
  ensure(currentUid !== undefined, "unsafe_directory");
  let metadata;
  let canonical;
  try {
    metadata = await lstat(directory, { bigint: true });
    canonical = await realpath(directory);
  } catch {
    fail("unsafe_directory");
  }
  ensure(
    metadata.isDirectory() &&
      !metadata.isSymbolicLink() &&
      metadata.uid === BigIntConstructor(currentUid) &&
      NumberConstructor(metadata.mode & 0o7777n) === 0o700,
    "unsafe_directory",
  );
  let handle;
  try {
    handle = await open(
      canonical,
      fsConstants.O_RDONLY | fsConstants.O_DIRECTORY | fsConstants.O_NOFOLLOW,
    );
    const held = await handle.stat({ bigint: true });
    ensure(
      held.isDirectory() && sameFileIdentity(held, metadata),
      "unsafe_directory",
    );
    if (expectedPin !== undefined) {
      ensure(
        canonical === expectedPin.path &&
          sameFileIdentity(metadata, expectedPin.identity) &&
          sameFileIdentity(held, expectedPin.identity),
        "unsafe_directory",
      );
    }
    await inspectAcl(inspectDirectoryAcl, canonical, "unsafe_directory");
    const ancestors = [];
    let childUid = metadata.uid;
    let current = dirname(canonical);
    while (true) {
      const ancestor = await lstat(current, { bigint: true });
      ensure(
        ancestorPermissionsAreSafe(ancestor, childUid, currentUid),
        "unsafe_directory",
      );
      await inspectAcl(inspectAncestorAcl, current, "unsafe_directory");
      arrayPush(ancestors, objectFreeze({ identity: ancestor, path: current }));
      const parent = dirname(current);
      if (parent === current) break;
      childUid = ancestor.uid;
      current = parent;
    }
    const authority = {
      ancestors,
      currentUid,
      handle,
      identity: metadata,
      inspectAncestorAcl,
      inspectDirectoryAcl,
      path: canonical,
    };
    authority.assertCurrent = async () => {
      try {
        const pathMetadata = await lstat(authority.path, { bigint: true });
        const heldMetadata = await authority.handle.stat({ bigint: true });
        ensure(
          pathMetadata.isDirectory() &&
            sameFileIdentity(pathMetadata, authority.identity) &&
            sameFileIdentity(heldMetadata, authority.identity) &&
            pathMetadata.uid === BigIntConstructor(authority.currentUid) &&
            NumberConstructor(pathMetadata.mode & 0o7777n) === 0o700,
          "unsafe_directory",
        );
        await inspectAcl(
          authority.inspectDirectoryAcl,
          authority.path,
          "unsafe_directory",
        );
        let currentChildUid = pathMetadata.uid;
        for (const ancestor of authority.ancestors) {
          const currentAncestor = await lstat(ancestor.path, { bigint: true });
          ensure(
            sameFileIdentity(currentAncestor, ancestor.identity) &&
              ancestorPermissionsAreSafe(
                currentAncestor,
                currentChildUid,
                authority.currentUid,
              ),
            "unsafe_directory",
          );
          await inspectAcl(
            authority.inspectAncestorAcl,
            ancestor.path,
            "unsafe_directory",
          );
          currentChildUid = currentAncestor.uid;
        }
      } catch (error) {
        if (isInternalError(error)) throw error;
        fail("unsafe_directory");
      }
    };
    await authority.assertCurrent();
    return authority;
  } catch (error) {
    if (handle !== undefined) await ignoreRejection(handle.close());
    if (isInternalError(error)) throw error;
    fail("unsafe_directory");
  }
}

function safeFileMetadata(metadata, currentUid, { empty = false } = {}) {
  return (
    metadata.isFile() &&
    metadata.nlink === 1n &&
    metadata.uid === BigIntConstructor(currentUid) &&
    NumberConstructor(metadata.mode & 0o7777n) === 0o600 &&
    (!empty || metadata.size === 0n)
  );
}

async function assertPathFileCurrent(path, handle, identity, currentUid, options = {}) {
  let held;
  let pathMetadata;
  try {
    held = await handle.stat({ bigint: true });
    pathMetadata = await lstat(path, { bigint: true });
  } catch {
    fail("corrupt_ledger");
  }
  ensure(
    safeFileMetadata(held, currentUid, options) &&
      safeFileMetadata(pathMetadata, currentUid, options) &&
      sameFileIdentity(held, identity) &&
      sameFileIdentity(pathMetadata, identity),
    "corrupt_ledger",
  );
  return held;
}

async function provisionLockFile(authority, syncDirectory) {
  const path = join(authority.path, FILESYSTEM_IMAGE_PROVIDER_STATE_LOCK_NAME);
  let handle;
  let created = false;
  try {
    try {
      handle = await open(
        path,
        fsConstants.O_RDWR |
          fsConstants.O_CREAT |
          fsConstants.O_EXCL |
          fsConstants.O_NOFOLLOW |
          fsConstants.O_NONBLOCK,
        0o600,
      );
      created = true;
      await handle.chmod(0o600);
      await handle.sync();
    } catch (error) {
      if (safeErrorCode(error) !== "EEXIST") throw error;
      handle = await open(
        path,
        fsConstants.O_RDWR | fsConstants.O_NOFOLLOW | fsConstants.O_NONBLOCK,
      );
    }
    const metadata = await handle.stat({ bigint: true });
    ensure(
      safeFileMetadata(metadata, authority.currentUid, { empty: true }),
      "corrupt_ledger",
    );
    await assertPathFileCurrent(
      path,
      handle,
      metadata,
      authority.currentUid,
      { empty: true },
    );
    if (created) await syncDirectory(authority.handle, authority.path);
    await authority.assertCurrent();
    await handle.close();
    handle = undefined;
    return objectFreeze({
      identity: objectFreeze({ dev: metadata.dev, ino: metadata.ino }),
      path,
    });
  } catch (error) {
    if (handle !== undefined) await ignoreRejection(handle.close());
    if (isInternalError(error)) throw error;
    fail("io_failed");
  }
}

async function assertLockPinCurrent(pin, currentUid) {
  let metadata;
  try {
    metadata = await lstat(pin.path, { bigint: true });
  } catch {
    fail("corrupt_ledger");
  }
  ensure(
    safeFileMetadata(metadata, currentUid, { empty: true }) &&
      sameFileIdentity(metadata, pin.identity),
    "corrupt_ledger",
  );
}

async function openLedgerFile(authority, expectedPin, syncDirectory) {
  const path = join(authority.path, FILESYSTEM_IMAGE_PROVIDER_STATE_LEDGER_NAME);
  let handle;
  let created = false;
  let existingObserved = expectedPin !== undefined;
  try {
    if (expectedPin === undefined) {
      try {
        handle = await open(
          path,
          fsConstants.O_RDWR |
            fsConstants.O_CREAT |
            fsConstants.O_EXCL |
            fsConstants.O_NOFOLLOW |
            fsConstants.O_NONBLOCK,
          0o600,
        );
        created = true;
        await handle.chmod(0o600);
        await handle.sync();
      } catch (error) {
        if (safeErrorCode(error) !== "EEXIST") throw error;
        existingObserved = true;
      }
    }
    if (handle === undefined) {
      handle = await open(
        path,
        fsConstants.O_RDWR | fsConstants.O_NOFOLLOW | fsConstants.O_NONBLOCK,
      );
    }
    const metadata = await handle.stat({ bigint: true });
    ensure(
      safeFileMetadata(metadata, authority.currentUid) &&
        metadata.size <= BigIntConstructor(MAX_LEDGER_BYTES) &&
        (expectedPin === undefined || sameFileIdentity(metadata, expectedPin.identity)),
      "corrupt_ledger",
    );
    await assertPathFileCurrent(path, handle, metadata, authority.currentUid);
    if (created) await syncDirectory(authority.handle, authority.path);
    await authority.assertCurrent();
    return {
      created,
      handle,
      pin: objectFreeze({
        identity: objectFreeze({ dev: metadata.dev, ino: metadata.ino }),
        path,
      }),
    };
  } catch (error) {
    if (handle !== undefined) await ignoreRejection(handle.close());
    if (isInternalError(error)) throw error;
    fail(existingObserved ? "corrupt_ledger" : "io_failed");
  }
}

async function readExact(handle, size, position) {
  const bytes = bufferAlloc(size);
  let offset = 0;
  while (offset < size) {
    const { bytesRead } = await handle.read(
      bytes,
      offset,
      size - offset,
      position + offset,
    );
    if (bytesRead === 0) fail("corrupt_ledger");
    offset += bytesRead;
  }
  return bytes;
}

async function readStableLedger(authority, ledger) {
  try {
    await authority.assertCurrent();
    const before = await assertPathFileCurrent(
      ledger.pin.path,
      ledger.handle,
      ledger.pin.identity,
      authority.currentUid,
    );
    ensure(before.size <= BigIntConstructor(MAX_LEDGER_BYTES), "corrupt_ledger");
    const first = await readExact(ledger.handle, NumberConstructor(before.size), 0);
    const after = await assertPathFileCurrent(
      ledger.pin.path,
      ledger.handle,
      ledger.pin.identity,
      authority.currentUid,
    );
    ensure(after.size === before.size, "corrupt_ledger");
    if (after.mtimeNs !== before.mtimeNs || after.ctimeNs !== before.ctimeNs) {
      // Stat deltas are only a revalidation trigger. The protected content
      // property changes only when a second stable byte read differs.
      const second = await readExact(
        ledger.handle,
        NumberConstructor(after.size),
        0,
      );
      const finalMetadata = await assertPathFileCurrent(
        ledger.pin.path,
        ledger.handle,
        ledger.pin.identity,
        authority.currentUid,
      );
      ensure(
        finalMetadata.size === after.size && bufferEquals(first, second),
        "corrupt_ledger",
      );
      return second;
    }
    return first;
  } catch (error) {
    if (isInternalError(error)) throw error;
    fail("io_failed");
  }
}

async function truncateUnanchoredSuffix(
  authority,
  headAnchor,
  ledger,
  bytes,
  tailOffset,
  lock,
  syncDirectory,
  trustedHead,
) {
  try {
    await lock.assertHeld();
    await authority.assertCurrent();
    const current = await readStableLedger(authority, ledger);
    ensure(bufferEquals(current, bytes), "corrupt_ledger");
    const currentHead = await readTrustedLedgerHead(headAnchor);
    ensure(canonicalEqual(currentHead, trustedHead), "corrupt_ledger");
    await ledger.handle.truncate(tailOffset);
    await ledger.handle.sync();
    await syncDirectory(authority.handle, authority.path);
    await lock.assertHeld();
    const recovered = await readStableLedger(authority, ledger);
    ensure(
      bufferEquals(recovered, bufferSubarray(bytes, 0, tailOffset)),
      "corrupt_ledger",
    );
    return recovered;
  } catch (error) {
    if (isInternalError(error)) throw error;
    fail("io_failed");
  }
}

async function writeAll(handle, bytes, position) {
  let offset = 0;
  while (offset < bytes.length) {
    const { bytesWritten } = await handle.write(
      bytes,
      offset,
      bytes.length - offset,
      position + offset,
    );
    if (bytesWritten <= 0) throw new Error("short ledger write");
    offset += bytesWritten;
  }
}

async function appendFrame({
  authority,
  headAnchor,
  ledger,
  lock,
  replay,
  event,
  mutation,
  syncDirectory,
}) {
  const sequence = replay.state.sequence + 1;
  ensure(sequence <= MAX_FRAME_COUNT, "io_failed");
  const frame = normalizeFrame(
    {
      ...event,
      contractVersion: FILESYSTEM_IMAGE_PROVIDER_STATE_CONTRACT_VERSION,
      previousChecksum: replay.state.lastChecksum,
      sequence,
    },
    "invalid_request",
  );
  const encoded = encodeFrame(frame);
  ensure(
    replay.bytes.length <= MAX_LEDGER_BYTES - encoded.bytes.length,
    "io_failed",
  );
  const expectedHead = canonicalLedgerHead(
    {
      contractVersion: FILESYSTEM_IMAGE_PROVIDER_STATE_HEAD_CONTRACT_VERSION,
      sequence: replay.state.sequence,
      lastChecksum: replay.state.lastChecksum,
      ledgerBytes: replay.bytes.length,
    },
    "corrupt_ledger",
  );
  const nextHead = canonicalLedgerHead(
    {
      contractVersion: FILESYSTEM_IMAGE_PROVIDER_STATE_HEAD_CONTRACT_VERSION,
      sequence,
      lastChecksum: encoded.checksum,
      ledgerBytes: replay.bytes.length + encoded.bytes.length,
    },
    "corrupt_ledger",
  );
  try {
    await lock.assertHeld();
    await authority.assertCurrent();
    const current = await readStableLedger(authority, ledger);
    ensure(bufferEquals(current, replay.bytes), "corrupt_ledger");
    mutation.attempted = true;
    await writeAll(ledger.handle, encoded.bytes, current.length);
    await ledger.handle.sync();
    await lock.assertHeld();
    await authority.assertCurrent();
    const expected = bufferConcat([current, encoded.bytes]);
    const readback = await readStableLedger(authority, ledger);
    ensure(bufferEquals(readback, expected), "corrupt_ledger");
    const verified = parseLedger(readback);
    ensure(
      verified.tailOffset === null &&
        verified.state.sequence === sequence &&
        verified.state.lastChecksum === encoded.checksum,
      "corrupt_ledger",
    );
    await lock.assertHeld();
    await authority.assertCurrent();
    await advanceTrustedLedgerHead(headAnchor, expectedHead, nextHead);
    await syncDirectory(authority.handle, authority.path);
    return { bytes: readback, state: verified.state };
  } catch (error) {
    if (mutation.attempted) fail("commit_outcome_uncertain");
    if (isInternalError(error)) throw error;
    fail("io_failed");
  }
}

function runQueued(key, operation) {
  const previous = mapGet(operationQueues, key) ?? promiseResolve();
  const current = callIntrinsic(promiseThenIntrinsic, previous, [operation, operation]);
  mapSet(operationQueues, key, current);
  const cleanup = () => {
    if (mapGet(operationQueues, key) === current) mapDelete(operationQueues, key);
  };
  return callIntrinsic(promiseThenIntrinsic, current, [
    (value) => {
      cleanup();
      return value;
    },
    (error) => {
      cleanup();
      throw error;
    },
  ]);
}

function delay(milliseconds) {
  return new PromiseConstructor((resolveDelay) =>
    setTimeoutIntrinsic(resolveDelay, milliseconds),
  );
}

async function acquireSerializedLock(provider, path, timeoutMs, retryMs) {
  const deadline = dateNowIntrinsic() + timeoutMs;
  while (true) {
    try {
      const lock = await provider(path, { requireExisting: true });
      ensure(
        lock !== null &&
          typeof lock === "object" &&
          typeof lock.assertHeld === "function" &&
          typeof lock.release === "function",
        "io_failed",
      );
      return lock;
    } catch (error) {
      if (
        safeErrorCode(error) !== "lock_unavailable" ||
        dateNowIntrinsic() >= deadline
      ) {
        fail("io_failed");
      }
      await delay(retryMs);
    }
  }
}

function snapshotFromState(state) {
  const compareIds = (left, right) => (left < right ? -1 : left > right ? 1 : 0);
  const operations = [];
  mapForEach(state.operations, (operation) => arrayPush(operations, operation));
  arraySort(operations, (left, right) =>
    compareIds(left.operationId, right.operationId));
  const storages = [];
  mapForEach(state.storages, (storage) => arrayPush(storages, storage));
  arraySort(storages, (left, right) =>
    compareIds(left.storageId, right.storageId));
  return objectFreeze({
    contractVersion: FILESYSTEM_IMAGE_PROVIDER_STATE_CONTRACT_VERSION,
    sequence: state.sequence,
    operations: objectFreeze(operations),
    storages: objectFreeze(storages),
  });
}

function operationView(record, currentStorageState, transient) {
  return objectFreeze({
    ...record,
    currentStorageState,
    ...transient,
  });
}

function normalizeRuntimeError(error, mutationAttempted) {
  if (mutationAttempted) return stateError("commit_outcome_uncertain");
  if (isInternalError(error)) return error;
  return stateError("io_failed");
}

export class FilesystemImageProviderState {
  #acquireLock;
  #directory;
  #directoryPinPromise;
  #headAnchor;
  #inspectAncestorAcl;
  #inspectDirectoryAcl;
  #ledgerPin;
  #lockRetryMs;
  #lockTimeoutMs;
  #syncDirectory;

  constructor(options) {
    const normalized = exactDataObject(
      options,
      [
        "directory",
        "acquireLock",
        "headAnchor",
        "inspectAncestorAcl",
        "inspectDirectoryAcl",
        "lockRetryMs",
        "lockTimeoutMs",
        "syncDirectory",
      ],
      ["directory", "headAnchor"],
      "invalid_request",
    );
    ensure(
      typeof normalized.directory === "string" &&
        isAbsolute(normalized.directory) &&
        resolve(normalized.directory) === normalized.directory,
      "invalid_request",
    );
    this.#directory = normalized.directory;
    this.#headAnchor = canonicalHeadAnchor(
      normalized.headAnchor,
      "invalid_request",
    );
    this.#acquireLock = objectHasOwn(normalized, "acquireLock")
      ? normalized.acquireLock
      : acquireAdvisoryLock;
    this.#inspectAncestorAcl =
      objectHasOwn(normalized, "inspectAncestorAcl")
        ? normalized.inspectAncestorAcl
        : recoveryPathHasUnsafeAncestorAcl;
    this.#inspectDirectoryAcl =
      objectHasOwn(normalized, "inspectDirectoryAcl")
        ? normalized.inspectDirectoryAcl
        : recoveryPathHasExtendedAcl;
    this.#syncDirectory =
      objectHasOwn(normalized, "syncDirectory")
        ? normalized.syncDirectory
        : async (handle) => handle.sync();
    this.#lockRetryMs = objectHasOwn(normalized, "lockRetryMs")
      ? normalized.lockRetryMs
      : 10;
    this.#lockTimeoutMs = objectHasOwn(normalized, "lockTimeoutMs")
      ? normalized.lockTimeoutMs
      : 5_000;
    ensure(
      arrayEvery(
        [
          this.#acquireLock,
          this.#inspectAncestorAcl,
          this.#inspectDirectoryAcl,
          this.#syncDirectory,
          this.#headAnchor.readHead,
          this.#headAnchor.compareAndAdvance,
        ],
        (operation) => typeof operation === "function",
      ) &&
        numberIsSafeIntegerIntrinsic(this.#lockRetryMs) &&
        this.#lockRetryMs >= 1 &&
        this.#lockRetryMs <= 1_000 &&
        numberIsSafeIntegerIntrinsic(this.#lockTimeoutMs) &&
        this.#lockTimeoutMs >= this.#lockRetryMs &&
        this.#lockTimeoutMs <= 60_000,
      "invalid_request",
    );
    objectFreeze(this);
  }

  #getDirectoryPin() {
    if (this.#directoryPinPromise !== undefined) return this.#directoryPinPromise;
    const attempt = (async () => {
      const authority = await openDirectoryAuthority(this.#directory, {
        inspectAncestorAcl: this.#inspectAncestorAcl,
        inspectDirectoryAcl: this.#inspectDirectoryAcl,
      });
      let lockPin;
      try {
        lockPin = await provisionLockFile(authority, this.#syncDirectory);
        return objectFreeze({
          directory: objectFreeze({
            identity: objectFreeze({
              dev: authority.identity.dev,
              ino: authority.identity.ino,
            }),
            path: authority.path,
          }),
          lock: lockPin,
        });
      } finally {
        try {
          await authority.handle.close();
        } catch {
          fail("io_failed");
        }
      }
    })();
    this.#directoryPinPromise = attempt;
    void callIntrinsic(promiseThenIntrinsic, attempt, [undefined, () => {
      if (this.#directoryPinPromise === attempt) {
        this.#directoryPinPromise = undefined;
      }
    }]);
    return attempt;
  }

  async #run(operation) {
    const pin = await this.#getDirectoryPin();
    const queueKey = `${StringConstructor(pin.directory.identity.dev)}\0${StringConstructor(pin.directory.identity.ino)}`;
    return await runQueued(queueKey, async () => {
      const mutation = { attempted: false };
      let authority;
      let ledger;
      let lock;
      let primaryError;
      let completed;
      try {
        authority = await openDirectoryAuthority(pin.directory.path, {
          expectedPin: pin.directory,
          inspectAncestorAcl: this.#inspectAncestorAcl,
          inspectDirectoryAcl: this.#inspectDirectoryAcl,
        });
        await assertLockPinCurrent(pin.lock, authority.currentUid);
        lock = await acquireSerializedLock(
          this.#acquireLock,
          pin.lock.path,
          this.#lockTimeoutMs,
          this.#lockRetryMs,
        );
        await lock.assertHeld();
        await assertLockPinCurrent(pin.lock, authority.currentUid);
        await authority.assertCurrent();
        ledger = await openLedgerFile(
          authority,
          this.#ledgerPin,
          this.#syncDirectory,
        );
        this.#ledgerPin ??= ledger.pin;
        let bytes = await readStableLedger(authority, ledger);
        const trustedHead = await readTrustedLedgerHead(this.#headAnchor);
        const reconciled = reconcileLedgerWithTrustedHead(bytes, trustedHead);
        let parsed = reconciled.parsed;
        if (reconciled.truncateOffset !== null) {
          bytes = await truncateUnanchoredSuffix(
            authority,
            this.#headAnchor,
            ledger,
            bytes,
            reconciled.truncateOffset,
            lock,
            this.#syncDirectory,
            trustedHead,
          );
          parsed = parseLedger(bytes);
          ensure(
            parsed.tailOffset === null &&
              canonicalEqual(replayHead(bytes, parsed), trustedHead),
            "corrupt_ledger",
          );
        }
        completed = await operation({
          append: (event) =>
            appendFrame({
              authority,
              event,
              headAnchor: this.#headAnchor,
              ledger,
              lock,
              mutation,
              replay: { bytes, state: parsed.state },
              syncDirectory: this.#syncDirectory,
            }),
          bytes,
          state: parsed.state,
        });
        await lock.assertHeld();
        await authority.assertCurrent();
      } catch (error) {
        primaryError = normalizeRuntimeError(error, mutation.attempted);
      }

      let cleanupFailed = false;
      try {
        await ledger?.handle.close();
      } catch {
        cleanupFailed = true;
      }
      try {
        await lock?.release();
      } catch {
        cleanupFailed = true;
      }
      try {
        await authority?.handle.close();
      } catch {
        cleanupFailed = true;
      }
      if (cleanupFailed) {
        primaryError = stateError(
          mutation.attempted ? "commit_outcome_uncertain" : "io_failed",
        );
      }
      if (primaryError !== undefined) throw primaryError;
      return completed;
    });
  }

  async prepareOperation(options) {
    const input = exactDataObject(
      options,
      [
        "kind",
        "operationId",
        "request",
        "storageId",
        "expectedStorageState",
      ],
      ["kind", "operationId", "request", "storageId"],
      "invalid_request",
    );
    const hasExpectedStorageState = objectHasOwn(
      input,
      "expectedStorageState",
    );
    const normalized = objectFreeze({
      kind: canonicalOperationKind(input.kind, "invalid_request"),
      operationId: canonicalOpaqueId(input.operationId, "invalid_request"),
      request: canonicalObject(input.request, "invalid_request"),
      storageId: canonicalOpaqueId(input.storageId, "invalid_request"),
      expectedStorageState: hasExpectedStorageState
        ? input.expectedStorageState === null
          ? null
          : canonicalStorageState(
              input.expectedStorageState,
              "invalid_request",
            )
        : undefined,
    });
    return await this.#run(async ({ append, state }) => {
      const existing = mapGet(state.operations, normalized.operationId);
      if (existing !== undefined) {
        ensure(
          existing.kind === normalized.kind &&
            existing.storageId === normalized.storageId &&
            canonicalEqual(existing.request, normalized.request),
          "operation_conflict",
        );
        const currentStorageState =
          mapGet(state.storages, existing.storageId) ?? null;
        return operationView(existing, currentStorageState, {
          replayed: true,
          shouldDispatch: false,
        });
      }
      const storageStateBefore =
        mapGet(state.storages, normalized.storageId) ?? null;
      if (hasExpectedStorageState) {
        ensure(
          canonicalEqual(storageStateBefore, normalized.expectedStorageState),
          "operation_conflict",
        );
      }
      ensure(
        pendingOperationForStorage(state, normalized.storageId) === null,
        "operation_already_prepared",
      );
      assertPreparePrecondition(
        storageStateBefore,
        normalized.kind,
        "operation_conflict",
      );
      const next = await append({
        kind: normalized.kind,
        operationId: normalized.operationId,
        request: normalized.request,
        storageId: normalized.storageId,
        storageStateBefore,
        type: "prepared",
      });
      const record = mapGet(next.state.operations, normalized.operationId);
      return operationView(record, storageStateBefore, {
        replayed: false,
        shouldDispatch: true,
      });
    });
  }

  async commitOperation(options) {
    const input = exactDataObject(
      options,
      ["operationId", "request", "result", "storageState"],
      ["operationId", "request", "result", "storageState"],
      "invalid_request",
    );
    const normalized = objectFreeze({
      operationId: canonicalOpaqueId(input.operationId, "invalid_request"),
      request: canonicalObject(input.request, "invalid_request"),
      result: canonicalObject(input.result, "invalid_request"),
      storageState: canonicalStorageState(input.storageState, "invalid_request"),
    });
    return await this.#run(async ({ append, state }) => {
      const existing = mapGet(state.operations, normalized.operationId);
      ensure(existing !== undefined, "operation_conflict");
      ensure(
        canonicalEqual(existing.request, normalized.request),
        "operation_conflict",
      );
      if (existing.state === "committed") {
        const currentStorageState = mapGet(state.storages, existing.storageId) ?? null;
        return operationView(existing, currentStorageState, {
          replayed: true,
          shouldDispatch: false,
        });
      }
      const currentStorageState = mapGet(state.storages, existing.storageId) ?? null;
      ensure(
        canonicalEqual(currentStorageState, existing.storageStateBefore) &&
          normalized.storageState.storageId === existing.storageId,
        "operation_conflict",
      );
      try {
        assertStorageTransition(
          currentStorageState,
          normalized.storageState,
          existing.kind,
          "operation_conflict",
        );
      } catch (error) {
        if (isInternalError(error)) throw error;
        fail("operation_conflict");
      }
      const next = await append({
        expectedStorage: expectedStorageState(currentStorageState),
        operationId: normalized.operationId,
        request: normalized.request,
        result: normalized.result,
        storageState: normalized.storageState,
        type: "committed",
      });
      const record = mapGet(next.state.operations, normalized.operationId);
      return operationView(record, normalized.storageState, {
        replayed: false,
        shouldDispatch: false,
      });
    });
  }

  async readOperation(options) {
    const input = exactDataObject(
      options,
      ["operationId", "request"],
      ["operationId"],
      "invalid_request",
    );
    const operationId = canonicalOpaqueId(input.operationId, "invalid_request");
    const request = objectHasOwn(input, "request")
      ? canonicalObject(input.request, "invalid_request")
      : undefined;
    return await this.#run(async ({ state }) => {
      const record = mapGet(state.operations, operationId);
      if (record === undefined) return null;
      if (request !== undefined) {
        ensure(canonicalEqual(record.request, request), "operation_conflict");
      }
      return operationView(record, mapGet(state.storages, record.storageId) ?? null, {});
    });
  }

  async readStorage(storageId) {
    const normalizedId = canonicalOpaqueId(storageId, "invalid_request");
    return await this.#run(
      async ({ state }) => mapGet(state.storages, normalizedId) ?? null,
    );
  }

  // snapshot() deliberately performs an authoritative locked replay. Cold-open
  // callers can await it before serving and enumerate every non-destroyed mount
  // that must be revalidated or remounted. It never exposes a transient empty
  // cache and returns a native Promise because this method is async.
  async snapshot() {
    return await this.#run(async ({ state }) => snapshotFromState(state));
  }
}

objectFreeze(FilesystemImageProviderStateError.prototype);
objectFreeze(FilesystemImageProviderStateError);
objectFreeze(normalizeFilesystemImageProviderStateHead);
objectFreeze(FilesystemImageProviderState.prototype);
objectFreeze(FilesystemImageProviderState);
