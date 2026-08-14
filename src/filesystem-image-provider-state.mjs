import { Buffer } from "node:buffer";
import { createHash, timingSafeEqual } from "node:crypto";
import { constants as fsConstants } from "node:fs";
import { lstat, open, realpath, unlink } from "node:fs/promises";
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
const mapSizeGetterIntrinsic = Object.getOwnPropertyDescriptor(
  Map.prototype,
  "size",
).get;
const MapConstructor = Map;
const mathMaxIntrinsic = Math.max;
const mathMinIntrinsic = Math.min;
const numberIsFiniteIntrinsic = Number.isFinite;
const numberIsSafeIntegerIntrinsic = Number.isSafeInteger;
const NUMBER_MAX_SAFE_INTEGER = Number.MAX_SAFE_INTEGER;
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

function mapSize(map) {
  return callIntrinsic(mapSizeGetterIntrinsic, map, []);
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

export const FILESYSTEM_IMAGE_PROVIDER_STATE_CONTRACT_VERSION = 2;
export const FILESYSTEM_IMAGE_PROVIDER_STATE_HEAD_CONTRACT_VERSION = 2;
export const FILESYSTEM_IMAGE_PROVIDER_STATE_LOCK_NAME =
  ".filesystem-image-provider-state.lock";
export const FILESYSTEM_IMAGE_PROVIDER_STATE_LEDGER_NAME = "state.g0.log";
export const FILESYSTEM_IMAGE_PROVIDER_STATE_DEFAULT_ACTIVE_LEDGER_BYTES_WATERMARK =
  8 * 1024 * 1024;
export const FILESYSTEM_IMAGE_PROVIDER_STATE_DEFAULT_ACTIVE_FRAME_COUNT_WATERMARK =
  8_192;

const MAX_CANONICAL_BYTES = 768 * 1024;
const MAX_CANONICAL_DEPTH = 32;
const MAX_CANONICAL_NODES = 16_384;
const MAX_FRAME_PAYLOAD_BYTES = 4 * 1024 * 1024;
const MAX_LEDGER_BYTES = 64 * 1024 * 1024;
const MAX_FRAME_COUNT = 65_535;
const MAX_UINT32 = 4_294_967_295;
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
  "portable-codex/filesystem-image-provider-state/frame/v2\0",
  "utf8",
);
const HEAD_DOMAIN = bufferFrom(
  "portable-codex/filesystem-image-provider-state/head/v2\0",
  "utf8",
);
const CHECKPOINT_STATE_DOMAIN = bufferFrom(
  "portable-codex/filesystem-image-provider-state/checkpoint-state/v2\0",
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
  state_capacity_exhausted:
    "Filesystem image provider state active ledger capacity is exhausted",
  maintenance_failed: "Filesystem image provider state maintenance failed",
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

function incrementNonnegativeUint64(value, code) {
  const parsed = canonicalUint64(value, code).parsed;
  ensure(parsed < MAX_UINT64, code);
  return StringConstructor(parsed + 1n);
}

function decrementPositiveUint64(value, code) {
  const parsed = canonicalUint64(value, code, { positive: true }).parsed;
  return StringConstructor(parsed - 1n);
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

function canonicalCheckpointSequence(value, code) {
  ensure(
    numberIsSafeIntegerIntrinsic(value) && value >= 1 && value <= MAX_UINT32,
    code,
  );
  return value;
}

function canonicalNonnegativeUint64(value, code) {
  return canonicalUint64(value, code).value;
}

function canonicalPositiveUint64(value, code) {
  return canonicalUint64(value, code, { positive: true }).value;
}

function uint64Difference(left, right, code) {
  const leftValue = canonicalUint64(left, code).parsed;
  const rightValue = canonicalUint64(right, code).parsed;
  ensure(leftValue >= rightValue, code);
  const difference = leftValue - rightValue;
  ensure(difference <= BigIntConstructor(NUMBER_MAX_SAFE_INTEGER), code);
  return NumberConstructor(difference);
}

function headChecksum(head, code) {
  const normalized = canonicalLedgerHead(head, code);
  const hash = createHash("sha256");
  callIntrinsic(hashUpdateIntrinsic, hash, [HEAD_DOMAIN]);
  callIntrinsic(hashUpdateIntrinsic, hash, [bufferFrom(canonicalString(normalized), "utf8")]);
  return bufferToString(callIntrinsic(hashDigestIntrinsic, hash, []), "hex");
}

function canonicalLedgerHead(value, code) {
  const head = exactDataObject(
    value,
    [
      "contractVersion",
      "anchorRevision",
      "generation",
      "stateRevision",
      "baseHeadChecksum",
      "checkpointStateRevision",
      "checkpointFrameCount",
      "checkpointChecksum",
      "checkpointBytes",
      "frameCount",
      "lastChecksum",
      "ledgerBytes",
    ],
    [
      "contractVersion",
      "anchorRevision",
      "generation",
      "stateRevision",
      "baseHeadChecksum",
      "checkpointStateRevision",
      "checkpointFrameCount",
      "checkpointChecksum",
      "checkpointBytes",
      "frameCount",
      "lastChecksum",
      "ledgerBytes",
    ],
    code,
  );
  ensure(
    head.contractVersion ===
      FILESYSTEM_IMAGE_PROVIDER_STATE_HEAD_CONTRACT_VERSION &&
      numberIsSafeIntegerIntrinsic(head.checkpointFrameCount) &&
      head.checkpointFrameCount >= 0 &&
      head.checkpointFrameCount <= MAX_UINT32 &&
      numberIsSafeIntegerIntrinsic(head.checkpointBytes) &&
      head.checkpointBytes >= 0 &&
      numberIsSafeIntegerIntrinsic(head.frameCount) &&
      head.frameCount >= 0 &&
      head.frameCount <= MAX_FRAME_COUNT &&
      numberIsSafeIntegerIntrinsic(head.ledgerBytes) &&
      head.ledgerBytes >= 0 &&
      head.ledgerBytes <= MAX_LEDGER_BYTES,
    code,
  );
  const anchorRevision = canonicalNonnegativeUint64(head.anchorRevision, code);
  const generation = canonicalNonnegativeUint64(head.generation, code);
  const stateRevision = canonicalNonnegativeUint64(head.stateRevision, code);
  const checkpointStateRevision = canonicalNonnegativeUint64(
    head.checkpointStateRevision,
    code,
  );
  const generationValue = canonicalUint64(generation, code).parsed;
  const stateRevisionValue = canonicalUint64(stateRevision, code).parsed;
  const anchorRevisionValue = canonicalUint64(anchorRevision, code).parsed;
  const checkpointStateRevisionValue = canonicalUint64(
    checkpointStateRevision,
    code,
  ).parsed;
  ensure(
    checkpointStateRevisionValue <= stateRevisionValue &&
      anchorRevisionValue === generationValue + stateRevisionValue &&
      uint64Difference(stateRevision, checkpointStateRevision, code) ===
        head.frameCount,
    code,
  );
  const baseHeadChecksum = canonicalPreviousChecksum(head.baseHeadChecksum, code);
  const checkpointChecksum = canonicalPreviousChecksum(
    head.checkpointChecksum,
    code,
  );
  const lastChecksum = canonicalPreviousChecksum(head.lastChecksum, code);
  const generationZero = generation === "0";
  const genesis = generationZero && stateRevision === "0";
  if (genesis) {
    ensure(
      anchorRevision === "0" &&
        stateRevision === "0" &&
        baseHeadChecksum === null &&
        checkpointStateRevision === "0" &&
        head.checkpointFrameCount === 0 &&
        checkpointChecksum === null &&
        head.checkpointBytes === 0 &&
        head.frameCount === 0 &&
        lastChecksum === null &&
        head.ledgerBytes === 0,
      code,
    );
  } else if (generationZero) {
    ensure(
      anchorRevision === stateRevision &&
        baseHeadChecksum === null &&
        checkpointStateRevision === "0" &&
        head.checkpointFrameCount === 0 &&
        checkpointChecksum === null &&
        head.checkpointBytes === 0 &&
        head.frameCount > 0 &&
        lastChecksum !== null &&
        head.ledgerBytes > 0,
      code,
    );
  } else {
    ensure(
      baseHeadChecksum !== null &&
        head.checkpointFrameCount >= 2 &&
        checkpointChecksum !== null &&
        head.checkpointBytes > 0 &&
        lastChecksum !== null &&
        (head.frameCount === 0
          ? head.ledgerBytes === 0 && lastChecksum === checkpointChecksum
          : head.ledgerBytes > 0),
      code,
    );
  }
  return objectFreeze({
    contractVersion: FILESYSTEM_IMAGE_PROVIDER_STATE_HEAD_CONTRACT_VERSION,
    anchorRevision,
    generation,
    stateRevision,
    baseHeadChecksum,
    checkpointStateRevision,
    checkpointFrameCount: head.checkpointFrameCount,
    checkpointChecksum,
    checkpointBytes: head.checkpointBytes,
    frameCount: head.frameCount,
    lastChecksum,
    ledgerBytes: head.ledgerBytes,
  });
}

export function normalizeFilesystemImageProviderStateHead(value) {
  return canonicalLedgerHead(value, "invalid_request");
}

export function filesystemImageProviderStateHeadChecksum(value) {
  return headChecksum(value, "invalid_request");
}

export function filesystemImageProviderStateCheckpointName(generation) {
  return stateCheckpointName(generation, "invalid_request");
}

export function filesystemImageProviderStateLedgerName(generation) {
  return stateLedgerName(generation, "invalid_request");
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

function hasLaterSentinel(bytes, start, expectedFooterStart) {
  const laterFrame = bufferIndexOf(bytes, FRAME_MAGIC, start);
  if (laterFrame !== -1) return true;
  const laterFooter = bufferIndexOf(bytes, FRAME_END_MAGIC, start);
  return laterFooter !== -1 && laterFooter !== expectedFooterStart;
}

function canonicalStateRevision(value, code, { positive = false } = {}) {
  return canonicalUint64(value, code, { positive }).value;
}

function normalizePreparedDeltaFrame(value, code) {
  const frame = exactDataObject(
    value,
    [
      "contractVersion",
      "kind",
      "operationId",
      "previousChecksum",
      "request",
      "sequence",
      "stateRevision",
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
      "stateRevision",
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
    stateRevision: canonicalStateRevision(frame.stateRevision, code, {
      positive: true,
    }),
    storageId: canonicalOpaqueId(frame.storageId, code),
    storageStateBefore:
      frame.storageStateBefore === null
        ? null
        : canonicalStorageState(frame.storageStateBefore, code),
    type: "prepared",
  });
}

function normalizeCommittedDeltaFrame(value, code) {
  const frame = exactDataObject(
    value,
    [
      "contractVersion",
      "expectedStorage",
      "operationId",
      "preparedChecksum",
      "previousChecksum",
      "result",
      "sequence",
      "stateRevision",
      "storageState",
      "type",
    ],
    [
      "contractVersion",
      "expectedStorage",
      "operationId",
      "preparedChecksum",
      "previousChecksum",
      "result",
      "sequence",
      "stateRevision",
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
    preparedChecksum: canonicalPreviousChecksum(frame.preparedChecksum, code),
    previousChecksum: canonicalPreviousChecksum(frame.previousChecksum, code),
    result: canonicalObject(frame.result, code),
    sequence: canonicalSequence(frame.sequence, code),
    stateRevision: canonicalStateRevision(frame.stateRevision, code, {
      positive: true,
    }),
    storageState: canonicalStorageState(frame.storageState, code),
    type: "committed",
  });
}

function normalizeDeltaFrame(value, code) {
  const keys = inspectPlainObject(value, code);
  ensure(arrayIncludes(keys, "type"), code);
  const type = ownDataValue(value, "type", code);
  if (type === "prepared") return normalizePreparedDeltaFrame(value, code);
  if (type === "committed") return normalizeCommittedDeltaFrame(value, code);
  fail(code);
}

function makePreparedRecord(frame, checksum) {
  return objectFreeze({
    kind: frame.kind,
    operationId: frame.operationId,
    request: frame.request,
    state: "prepared",
    storageId: frame.storageId,
    storageStateBefore: frame.storageStateBefore,
    _preparedChecksum: checksum,
    _preparedStateRevision: frame.stateRevision,
  });
}

function makeCommittedRecord(prepared, frame) {
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
    _preparedChecksum: prepared._preparedChecksum,
    _preparedStateRevision: prepared._preparedStateRevision,
    _committedStateRevision: frame.stateRevision,
  });
}

function publicOperationRecord(record) {
  const common = {
    kind: record.kind,
    operationId: record.operationId,
    request: record.request,
    state: record.state,
    storageId: record.storageId,
    storageStateBefore: record.storageStateBefore,
  };
  return record.state === "prepared"
    ? objectFreeze(common)
    : objectFreeze({
        ...common,
        expectedStorage: record.expectedStorage,
        result: record.result,
        storageState: record.storageState,
      });
}

function checkpointOperationRecord(record) {
  const common = {
    kind: record.kind,
    operationId: record.operationId,
    preparedChecksum: record._preparedChecksum,
    preparedStateRevision: record._preparedStateRevision,
    request: record.request,
    state: record.state,
    storageId: record.storageId,
    storageStateBefore: record.storageStateBefore,
  };
  return record.state === "prepared"
    ? objectFreeze(common)
    : objectFreeze({
        ...common,
        committedStateRevision: record._committedStateRevision,
        expectedStorage: record.expectedStorage,
        result: record.result,
        storageState: record.storageState,
      });
}

function normalizeCheckpointOperationRecord(value, code) {
  const keys = inspectPlainObject(value, code);
  ensure(arrayIncludes(keys, "state"), code);
  const state = ownDataValue(value, "state", code);
  const commonKeys = [
    "kind",
    "operationId",
    "preparedChecksum",
    "preparedStateRevision",
    "request",
    "state",
    "storageId",
    "storageStateBefore",
  ];
  const committedKeys = [
    ...commonKeys,
    "committedStateRevision",
    "expectedStorage",
    "result",
    "storageState",
  ];
  const record = exactDataObject(
    value,
    state === "prepared" ? commonKeys : committedKeys,
    state === "prepared" ? commonKeys : committedKeys,
    code,
  );
  ensure(state === "prepared" || state === "committed", code);
  const prepared = {
    kind: canonicalOperationKind(record.kind, code),
    operationId: canonicalOpaqueId(record.operationId, code),
    preparedChecksum: canonicalPreviousChecksum(record.preparedChecksum, code),
    preparedStateRevision: canonicalStateRevision(
      record.preparedStateRevision,
      code,
      { positive: true },
    ),
    request: canonicalObject(record.request, code),
    state: "prepared",
    storageId: canonicalOpaqueId(record.storageId, code),
    storageStateBefore:
      record.storageStateBefore === null
        ? null
        : canonicalStorageState(record.storageStateBefore, code),
  };
  ensure(prepared.preparedChecksum !== null, code);
  if (state === "prepared") return objectFreeze(prepared);
  return objectFreeze({
    ...prepared,
    state: "committed",
    committedStateRevision: canonicalStateRevision(
      record.committedStateRevision,
      code,
      { positive: true },
    ),
    expectedStorage: canonicalExpectedStorage(record.expectedStorage, code),
    result: canonicalObject(record.result, code),
    storageState: canonicalStorageState(record.storageState, code),
  });
}

function checkpointOperationStateRecord(record) {
  const prepared = {
    kind: record.kind,
    operationId: record.operationId,
    request: record.request,
    state: "prepared",
    storageId: record.storageId,
    storageStateBefore: record.storageStateBefore,
    _preparedChecksum: record.preparedChecksum,
    _preparedStateRevision: record.preparedStateRevision,
  };
  return record.state === "prepared"
    ? objectFreeze(prepared)
    : objectFreeze({
        ...prepared,
        state: "committed",
        expectedStorage: record.expectedStorage,
        result: record.result,
        storageState: record.storageState,
        _committedStateRevision: record.committedStateRevision,
      });
}

function emptyGenerationState(stateRevision = "0") {
  return {
    operations: new MapConstructor(),
    stateRevision,
    storages: new MapConstructor(),
  };
}

function cloneGenerationState(state) {
  const copy = emptyGenerationState(state.stateRevision);
  mapForEach(state.operations, (value, key) => mapSet(copy.operations, key, value));
  mapForEach(state.storages, (value, key) => mapSet(copy.storages, key, value));
  return copy;
}

function applyDeltaFrame(state, frame, checksum, expectedSequence, code) {
  ensure(
    frame.sequence === expectedSequence &&
      frame.stateRevision === incrementNonnegativeUint64(state.stateRevision, code),
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
    mapSet(state.operations, frame.operationId, makePreparedRecord(frame, checksum));
  } else {
    const operation = mapGet(state.operations, frame.operationId);
    ensure(
      operation?.state === "prepared" &&
        operation._preparedChecksum === frame.preparedChecksum,
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
    const committed = makeCommittedRecord(operation, frame);
    mapSet(state.operations, frame.operationId, committed);
    mapSet(state.storages, operation.storageId, frame.storageState);
  }
  state.stateRevision = frame.stateRevision;
}

function normalizeCheckpointStartFrame(value, code) {
  const frame = exactDataObject(
    value,
    [
      "baseHeadChecksum",
      "contractVersion",
      "generation",
      "operationCount",
      "previousChecksum",
      "sequence",
      "stateRevision",
      "storageCount",
      "type",
    ],
    [
      "baseHeadChecksum",
      "contractVersion",
      "generation",
      "operationCount",
      "previousChecksum",
      "sequence",
      "stateRevision",
      "storageCount",
      "type",
    ],
    code,
  );
  ensure(
    frame.contractVersion === FILESYSTEM_IMAGE_PROVIDER_STATE_CONTRACT_VERSION &&
      frame.type === "checkpoint-start" &&
      numberIsSafeIntegerIntrinsic(frame.operationCount) &&
      frame.operationCount >= 0 &&
      frame.operationCount <= MAX_UINT32 &&
      numberIsSafeIntegerIntrinsic(frame.storageCount) &&
      frame.storageCount >= 0 &&
      frame.storageCount <= MAX_UINT32,
    code,
  );
  const baseHeadChecksum = canonicalPreviousChecksum(frame.baseHeadChecksum, code);
  ensure(baseHeadChecksum !== null, code);
  return objectFreeze({
    baseHeadChecksum,
    contractVersion: FILESYSTEM_IMAGE_PROVIDER_STATE_CONTRACT_VERSION,
    generation: canonicalPositiveUint64(frame.generation, code),
    operationCount: frame.operationCount,
    previousChecksum: canonicalPreviousChecksum(frame.previousChecksum, code),
    sequence: canonicalCheckpointSequence(frame.sequence, code),
    stateRevision: canonicalStateRevision(frame.stateRevision, code),
    storageCount: frame.storageCount,
    type: "checkpoint-start",
  });
}

function normalizeCheckpointOperationFrame(value, code) {
  const frame = exactDataObject(
    value,
    [
      "contractVersion",
      "generation",
      "operation",
      "previousChecksum",
      "sequence",
      "type",
    ],
    [
      "contractVersion",
      "generation",
      "operation",
      "previousChecksum",
      "sequence",
      "type",
    ],
    code,
  );
  ensure(
    frame.contractVersion === FILESYSTEM_IMAGE_PROVIDER_STATE_CONTRACT_VERSION &&
      frame.type === "checkpoint-operation",
    code,
  );
  return objectFreeze({
    contractVersion: FILESYSTEM_IMAGE_PROVIDER_STATE_CONTRACT_VERSION,
    generation: canonicalPositiveUint64(frame.generation, code),
    operation: normalizeCheckpointOperationRecord(frame.operation, code),
    previousChecksum: canonicalPreviousChecksum(frame.previousChecksum, code),
    sequence: canonicalCheckpointSequence(frame.sequence, code),
    type: "checkpoint-operation",
  });
}

function normalizeCheckpointStorageFrame(value, code) {
  const frame = exactDataObject(
    value,
    [
      "contractVersion",
      "generation",
      "previousChecksum",
      "sequence",
      "storage",
      "type",
    ],
    [
      "contractVersion",
      "generation",
      "previousChecksum",
      "sequence",
      "storage",
      "type",
    ],
    code,
  );
  ensure(
    frame.contractVersion === FILESYSTEM_IMAGE_PROVIDER_STATE_CONTRACT_VERSION &&
      frame.type === "checkpoint-storage",
    code,
  );
  return objectFreeze({
    contractVersion: FILESYSTEM_IMAGE_PROVIDER_STATE_CONTRACT_VERSION,
    generation: canonicalPositiveUint64(frame.generation, code),
    previousChecksum: canonicalPreviousChecksum(frame.previousChecksum, code),
    sequence: canonicalCheckpointSequence(frame.sequence, code),
    storage: canonicalStorageState(frame.storage, code),
    type: "checkpoint-storage",
  });
}

function normalizeCheckpointEndFrame(value, code) {
  const frame = exactDataObject(
    value,
    [
      "contractVersion",
      "generation",
      "operationCount",
      "previousChecksum",
      "sequence",
      "stateChecksum",
      "stateRevision",
      "storageCount",
      "type",
    ],
    [
      "contractVersion",
      "generation",
      "operationCount",
      "previousChecksum",
      "sequence",
      "stateChecksum",
      "stateRevision",
      "storageCount",
      "type",
    ],
    code,
  );
  ensure(
    frame.contractVersion === FILESYSTEM_IMAGE_PROVIDER_STATE_CONTRACT_VERSION &&
      frame.type === "checkpoint-end" &&
      numberIsSafeIntegerIntrinsic(frame.operationCount) &&
      frame.operationCount >= 0 &&
      frame.operationCount <= MAX_UINT32 &&
      numberIsSafeIntegerIntrinsic(frame.storageCount) &&
      frame.storageCount >= 0 &&
      frame.storageCount <= MAX_UINT32 &&
      typeof frame.stateChecksum === "string" &&
      regexpTest(SHA256_PATTERN, frame.stateChecksum),
    code,
  );
  return objectFreeze({
    contractVersion: FILESYSTEM_IMAGE_PROVIDER_STATE_CONTRACT_VERSION,
    generation: canonicalPositiveUint64(frame.generation, code),
    operationCount: frame.operationCount,
    previousChecksum: canonicalPreviousChecksum(frame.previousChecksum, code),
    sequence: canonicalCheckpointSequence(frame.sequence, code),
    stateChecksum: frame.stateChecksum,
    stateRevision: canonicalStateRevision(frame.stateRevision, code),
    storageCount: frame.storageCount,
    type: "checkpoint-end",
  });
}

function normalizeCheckpointFrame(value, code) {
  const keys = inspectPlainObject(value, code);
  ensure(arrayIncludes(keys, "type"), code);
  const type = ownDataValue(value, "type", code);
  if (type === "checkpoint-start") return normalizeCheckpointStartFrame(value, code);
  if (type === "checkpoint-operation") {
    return normalizeCheckpointOperationFrame(value, code);
  }
  if (type === "checkpoint-storage") {
    return normalizeCheckpointStorageFrame(value, code);
  }
  if (type === "checkpoint-end") return normalizeCheckpointEndFrame(value, code);
  fail(code);
}

function encodeCanonicalFrame(frame, code) {
  const payload = bufferFrom(canonicalString(frame), "utf8");
  ensure(payload.length > 0 && payload.length <= MAX_FRAME_PAYLOAD_BYTES, code);
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

function parseCanonicalPayload(payload, normalizer) {
  const text = bufferToString(payload, "utf8");
  ensure(bufferEquals(bufferFrom(text, "utf8"), payload), "corrupt_ledger");
  let parsed;
  try {
    parsed = jsonParseIntrinsic(text);
  } catch {
    fail("corrupt_ledger");
  }
  const frame = normalizer(parsed, "corrupt_ledger");
  ensure(canonicalString(frame) === text, "corrupt_ledger");
  return frame;
}

function checkpointArrays(state) {
  const operations = [];
  mapForEach(state.operations, (operation) => {
    arrayPush(operations, checkpointOperationRecord(operation));
  });
  arraySort(operations, (left, right) =>
    left.operationId < right.operationId
      ? -1
      : left.operationId > right.operationId
        ? 1
        : 0);
  const storages = [];
  mapForEach(state.storages, (storage) => arrayPush(storages, storage));
  arraySort(storages, (left, right) =>
    left.storageId < right.storageId ? -1 : left.storageId > right.storageId ? 1 : 0);
  return objectFreeze({
    operations: objectFreeze(operations),
    storages: objectFreeze(storages),
  });
}

function createCheckpointStateHash(stateRevision) {
  const hash = createHash("sha256");
  callIntrinsic(hashUpdateIntrinsic, hash, [CHECKPOINT_STATE_DOMAIN]);
  callIntrinsic(hashUpdateIntrinsic, hash, [
    bufferFrom(canonicalString({ stateRevision }), "utf8"),
  ]);
  return hash;
}

function updateCheckpointStateHash(hash, type, record) {
  callIntrinsic(hashUpdateIntrinsic, hash, [bufferFrom(`${type}\0`, "utf8")]);
  callIntrinsic(hashUpdateIntrinsic, hash, [
    bufferFrom(canonicalString(record), "utf8"),
  ]);
}

function finishCheckpointStateHash(hash) {
  return bufferToString(callIntrinsic(hashDigestIntrinsic, hash, []), "hex");
}

function checkpointStateChecksum(stateRevision, operations, storages) {
  const hash = createCheckpointStateHash(stateRevision);
  for (const operation of operations) {
    updateCheckpointStateHash(hash, "operation", operation);
  }
  for (const storage of storages) {
    updateCheckpointStateHash(hash, "storage", storage);
  }
  return finishCheckpointStateHash(hash);
}

function validateCheckpointState(state, code) {
  const seenRevisions = new SetConstructor();
  const latestStorage = new MapConstructor();
  const pendingStorage = new SetConstructor();
  let eventCount = 0n;
  mapForEach(state.operations, (operation) => {
    const preparedRevision = canonicalUint64(
      operation._preparedStateRevision,
      code,
      { positive: true },
    ).parsed;
    ensure(
      preparedRevision <= canonicalUint64(state.stateRevision, code).parsed &&
        !callIntrinsic(setHasIntrinsic, seenRevisions, [operation._preparedStateRevision]),
      code,
    );
    callIntrinsic(setAddIntrinsic, seenRevisions, [operation._preparedStateRevision]);
    eventCount += 1n;
    if (operation.state === "prepared") {
      ensure(
        !callIntrinsic(setHasIntrinsic, pendingStorage, [operation.storageId]),
        code,
      );
      callIntrinsic(setAddIntrinsic, pendingStorage, [operation.storageId]);
      return;
    }
    const committedRevision = canonicalUint64(
      operation._committedStateRevision,
      code,
      { positive: true },
    ).parsed;
    ensure(
      committedRevision > preparedRevision &&
        committedRevision <= canonicalUint64(state.stateRevision, code).parsed &&
        !callIntrinsic(setHasIntrinsic, seenRevisions, [operation._committedStateRevision]),
      code,
    );
    callIntrinsic(setAddIntrinsic, seenRevisions, [operation._committedStateRevision]);
    eventCount += 1n;
    ensure(
      canonicalEqual(
        operation.expectedStorage,
        expectedStorageState(operation.storageStateBefore),
      ) && operation.storageState.storageId === operation.storageId,
      code,
    );
    assertStorageTransition(
      operation.storageStateBefore,
      operation.storageState,
      operation.kind,
      code,
    );
    const latest = mapGet(latestStorage, operation.storageId);
    if (
      latest === undefined ||
      canonicalUint64(latest.revision, code).parsed < committedRevision
    ) {
      mapSet(latestStorage, operation.storageId, {
        revision: operation._committedStateRevision,
        storage: operation.storageState,
      });
    }
  });
  ensure(eventCount === canonicalUint64(state.stateRevision, code).parsed, code);
  mapForEach(state.storages, (storage, storageId) => {
    const latest = mapGet(latestStorage, storageId);
    ensure(latest !== undefined && canonicalEqual(latest.storage, storage), code);
  });
  mapForEach(latestStorage, (_value, storageId) => {
    ensure(mapHas(state.storages, storageId), code);
  });
  mapForEach(state.operations, (operation) => {
    if (operation.state !== "prepared") return;
    ensure(
      canonicalEqual(
        mapGet(state.storages, operation.storageId) ?? null,
        operation.storageStateBefore,
      ),
      code,
    );
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
  // The two identity/policy checks bind the held file and pathname. Return the
  // later pathname metadata only as a content-revalidation trigger for cache
  // eligibility; a timestamp delta is not itself evidence of content change.
  return pathMetadata;
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
        finalMetadata.size === after.size &&
          finalMetadata.mtimeNs === after.mtimeNs &&
          finalMetadata.ctimeNs === after.ctimeNs &&
          bufferEquals(first, second),
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

function stateCheckpointName(generation, code) {
  return `state.g${canonicalNonnegativeUint64(generation, code)}.checkpoint`;
}

function stateLedgerName(generation, code) {
  return `state.g${canonicalNonnegativeUint64(generation, code)}.log`;
}

function metadataSnapshot(metadata) {
  return objectFreeze({
    ctimeNs: metadata.ctimeNs,
    mtimeNs: metadata.mtimeNs,
    size: metadata.size,
  });
}

function sameMetadataSnapshot(left, right) {
  return (
    left.size === right.size &&
    left.mtimeNs === right.mtimeNs &&
    left.ctimeNs === right.ctimeNs
  );
}

async function openNamedStateFile(
  authority,
  name,
  { expectedPin, writable = false } = {},
) {
  const path = join(authority.path, name);
  let handle;
  try {
    handle = await open(
      path,
      (writable ? fsConstants.O_RDWR : fsConstants.O_RDONLY) |
        fsConstants.O_NOFOLLOW |
        fsConstants.O_NONBLOCK,
    );
    const metadata = await handle.stat({ bigint: true });
    ensure(
      safeFileMetadata(metadata, authority.currentUid) &&
        metadata.size <= BigIntConstructor(NUMBER_MAX_SAFE_INTEGER) &&
        (expectedPin === undefined || sameFileIdentity(metadata, expectedPin.identity)),
      "corrupt_ledger",
    );
    const currentMetadata = await assertPathFileCurrent(
      path,
      handle,
      metadata,
      authority.currentUid,
    );
    await authority.assertCurrent();
    return {
      handle,
      metadata: currentMetadata,
      pin: objectFreeze({
        identity: objectFreeze({ dev: metadata.dev, ino: metadata.ino }),
        path,
      }),
    };
  } catch (error) {
    if (handle !== undefined) await ignoreRejection(handle.close());
    if (isInternalError(error)) throw error;
    fail("corrupt_ledger");
  }
}

async function createNamedStateFile(authority, name) {
  const path = join(authority.path, name);
  let handle;
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
    await handle.chmod(0o600);
    const metadata = await handle.stat({ bigint: true });
    ensure(
      safeFileMetadata(metadata, authority.currentUid, { empty: true }),
      "corrupt_ledger",
    );
    await assertPathFileCurrent(path, handle, metadata, authority.currentUid, {
      empty: true,
    });
    return {
      handle,
      metadata,
      pin: objectFreeze({
        identity: objectFreeze({ dev: metadata.dev, ino: metadata.ino }),
        path,
      }),
    };
  } catch (error) {
    if (handle !== undefined) await ignoreRejection(handle.close());
    if (isInternalError(error)) throw error;
    fail("maintenance_failed");
  }
}

async function unlinkSafeStateFile(authority, name) {
  const path = join(authority.path, name);
  let metadata;
  try {
    metadata = await lstat(path, { bigint: true });
  } catch (error) {
    if (safeErrorCode(error) === "ENOENT") return false;
    fail("maintenance_failed");
  }
  ensure(safeFileMetadata(metadata, authority.currentUid), "corrupt_ledger");
  try {
    await unlink(path);
  } catch {
    fail("maintenance_failed");
  }
  return true;
}

async function cleanupGeneration(authority, generation, syncDirectory) {
  canonicalNonnegativeUint64(generation, "corrupt_ledger");
  const checkpointRemoved = await unlinkSafeStateFile(
    authority,
    stateCheckpointName(generation, "corrupt_ledger"),
  );
  const ledgerRemoved = await unlinkSafeStateFile(
    authority,
    stateLedgerName(generation, "corrupt_ledger"),
  );
  if (checkpointRemoved || ledgerRemoved) {
    try {
      await syncDirectory(authority.handle, authority.path);
    } catch {
      fail("maintenance_failed");
    }
  }
}

async function cleanupCheckpointFile(authority, generation, syncDirectory) {
  const removed = await unlinkSafeStateFile(
    authority,
    stateCheckpointName(generation, "corrupt_ledger"),
  );
  if (removed) {
    try {
      await syncDirectory(authority.handle, authority.path);
    } catch {
      fail("maintenance_failed");
    }
  }
}

function parseEnvelopeFromBuffer(bytes, offset, expectedSequence, normalizer) {
  ensure(bytes.length - offset >= FRAME_HEADER_BYTES, "corrupt_ledger");
  ensure(
    bufferEquals(
      bufferSubarray(bytes, offset, offset + FRAME_MAGIC.length),
      FRAME_MAGIC,
    ),
    "corrupt_ledger",
  );
  const payloadLength = bufferReadUInt32BE(bytes, offset + FRAME_MAGIC.length);
  const sequence = bufferReadUInt32BE(bytes, offset + FRAME_MAGIC.length + 4);
  ensure(
    payloadLength > 0 &&
      payloadLength <= MAX_FRAME_PAYLOAD_BYTES &&
      sequence === expectedSequence,
    "corrupt_ledger",
  );
  const payloadStart = offset + FRAME_HEADER_BYTES;
  const footerStart = payloadStart + payloadLength;
  const frameEnd = footerStart + FRAME_FOOTER_BYTES;
  ensure(frameEnd <= bytes.length, "corrupt_ledger");
  ensure(
    bufferEquals(
      bufferSubarray(bytes, footerStart, footerStart + FRAME_END_MAGIC.length),
      FRAME_END_MAGIC,
    ) &&
      bufferReadUInt32BE(bytes, footerStart + FRAME_END_MAGIC.length) ===
        payloadLength &&
      bufferReadUInt32BE(bytes, footerStart + FRAME_END_MAGIC.length + 4) ===
        sequence,
    "corrupt_ledger",
  );
  const headerChecksum = bufferSubarray(
    bytes,
    offset + FRAME_MAGIC.length + 8,
    offset + FRAME_HEADER_BYTES,
  );
  const footerChecksum = bufferSubarray(
    bytes,
    footerStart + FRAME_END_MAGIC.length + 8,
    frameEnd,
  );
  const payload = bufferSubarray(bytes, payloadStart, footerStart);
  const checksum = frameChecksum(payload, payloadLength, sequence);
  ensure(
    timingSafeEqual(headerChecksum, footerChecksum) &&
      timingSafeEqual(headerChecksum, checksum),
    "corrupt_ledger",
  );
  return objectFreeze({
    checksum: bufferToString(checksum, "hex"),
    frame: parseCanonicalPayload(payload, normalizer),
    frameEnd,
  });
}

function validateUnanchoredDeltaTail(bytes, offset, head, state) {
  const remaining = bytes.length - offset;
  ensure(remaining > 0, "corrupt_ledger");
  if (remaining < FRAME_HEADER_BYTES) {
    const prefixLength = mathMinIntrinsic(remaining, FRAME_MAGIC.length);
    ensure(
      bufferEquals(
        bufferSubarray(bytes, offset, offset + prefixLength),
        bufferSubarray(FRAME_MAGIC, 0, prefixLength),
      ),
      "corrupt_ledger",
    );
    return;
  }
  ensure(
    bufferEquals(
      bufferSubarray(bytes, offset, offset + FRAME_MAGIC.length),
      FRAME_MAGIC,
    ),
    "corrupt_ledger",
  );
  const payloadLength = bufferReadUInt32BE(bytes, offset + FRAME_MAGIC.length);
  const sequence = bufferReadUInt32BE(bytes, offset + FRAME_MAGIC.length + 4);
  ensure(
    payloadLength > 0 &&
      payloadLength <= MAX_FRAME_PAYLOAD_BYTES &&
      sequence === head.frameCount + 1 &&
      sequence <= MAX_FRAME_COUNT,
    "corrupt_ledger",
  );
  const payloadStart = offset + FRAME_HEADER_BYTES;
  const footerStart = payloadStart + payloadLength;
  const frameEnd = footerStart + FRAME_FOOTER_BYTES;
  if (frameEnd <= bytes.length) {
    ensure(frameEnd === bytes.length, "corrupt_ledger");
    const parsed = parseEnvelopeFromBuffer(
      bytes,
      offset,
      sequence,
      normalizeDeltaFrame,
    );
    ensure(parsed.frame.previousChecksum === head.lastChecksum, "corrupt_ledger");
    applyDeltaFrame(
      cloneGenerationState(state),
      parsed.frame,
      parsed.checksum,
      sequence,
      "corrupt_ledger",
    );
    return;
  }
  ensure(!hasLaterSentinel(bytes, payloadStart, footerStart), "corrupt_ledger");
  if (footerStart <= bytes.length) {
    const payload = bufferSubarray(bytes, payloadStart, footerStart);
    const expectedChecksum = bufferSubarray(
      bytes,
      offset + FRAME_MAGIC.length + 8,
      offset + FRAME_HEADER_BYTES,
    );
    const checksum = frameChecksum(payload, payloadLength, sequence);
    ensure(timingSafeEqual(expectedChecksum, checksum), "corrupt_ledger");
    const frame = parseCanonicalPayload(payload, normalizeDeltaFrame);
    ensure(frame.previousChecksum === head.lastChecksum, "corrupt_ledger");
    applyDeltaFrame(
      cloneGenerationState(state),
      frame,
      bufferToString(checksum, "hex"),
      sequence,
      "corrupt_ledger",
    );
  }
}

function parseActiveLedger(bytes, head, checkpointState) {
  ensure(bufferIsBuffer(bytes) && bytes.length <= MAX_LEDGER_BYTES, "corrupt_ledger");
  ensure(head.ledgerBytes <= bytes.length, "corrupt_ledger");
  const state = cloneGenerationState(checkpointState);
  let checksum = head.checkpointChecksum;
  let offset = 0;
  for (let sequence = 1; sequence <= head.frameCount; sequence += 1) {
    const parsed = parseEnvelopeFromBuffer(
      bufferSubarray(bytes, 0, head.ledgerBytes),
      offset,
      sequence,
      normalizeDeltaFrame,
    );
    ensure(parsed.frame.previousChecksum === checksum, "corrupt_ledger");
    applyDeltaFrame(state, parsed.frame, parsed.checksum, sequence, "corrupt_ledger");
    checksum = parsed.checksum;
    offset = parsed.frameEnd;
  }
  ensure(
    offset === head.ledgerBytes &&
      checksum === head.lastChecksum &&
      state.stateRevision === head.stateRevision,
    "corrupt_ledger",
  );
  const truncateOffset = bytes.length === head.ledgerBytes ? null : head.ledgerBytes;
  if (truncateOffset !== null) {
    validateUnanchoredDeltaTail(bytes, head.ledgerBytes, head, state);
  }
  return objectFreeze({ state, truncateOffset });
}

async function readEnvelopeAt(
  handle,
  offset,
  totalBytes,
  expectedSequence,
  normalizer,
) {
  ensure(
    offset <= totalBytes - FRAME_HEADER_BYTES,
    "corrupt_ledger",
  );
  const header = await readExact(handle, FRAME_HEADER_BYTES, offset);
  ensure(
    bufferEquals(bufferSubarray(header, 0, FRAME_MAGIC.length), FRAME_MAGIC),
    "corrupt_ledger",
  );
  const payloadLength = bufferReadUInt32BE(header, FRAME_MAGIC.length);
  const sequence = bufferReadUInt32BE(header, FRAME_MAGIC.length + 4);
  ensure(
    payloadLength > 0 &&
      payloadLength <= MAX_FRAME_PAYLOAD_BYTES &&
      sequence === expectedSequence,
    "corrupt_ledger",
  );
  const payloadStart = offset + FRAME_HEADER_BYTES;
  const footerStart = payloadStart + payloadLength;
  const frameEnd = footerStart + FRAME_FOOTER_BYTES;
  ensure(frameEnd <= totalBytes, "corrupt_ledger");
  const payload = await readExact(handle, payloadLength, payloadStart);
  const footer = await readExact(handle, FRAME_FOOTER_BYTES, footerStart);
  ensure(
    bufferEquals(
      bufferSubarray(footer, 0, FRAME_END_MAGIC.length),
      FRAME_END_MAGIC,
    ) &&
      bufferReadUInt32BE(footer, FRAME_END_MAGIC.length) === payloadLength &&
      bufferReadUInt32BE(footer, FRAME_END_MAGIC.length + 4) === sequence,
    "corrupt_ledger",
  );
  const headerChecksum = bufferSubarray(
    header,
    FRAME_MAGIC.length + 8,
    FRAME_HEADER_BYTES,
  );
  const footerChecksum = bufferSubarray(
    footer,
    FRAME_END_MAGIC.length + 8,
    FRAME_FOOTER_BYTES,
  );
  const checksum = frameChecksum(payload, payloadLength, sequence);
  ensure(
    timingSafeEqual(headerChecksum, footerChecksum) &&
      timingSafeEqual(headerChecksum, checksum),
    "corrupt_ledger",
  );
  return objectFreeze({
    checksum: bufferToString(checksum, "hex"),
    frame: parseCanonicalPayload(payload, normalizer),
    frameEnd,
  });
}

async function parseCheckpointStream(handle, head) {
  let sequence = 1;
  let offset = 0;
  let checksum = head.baseHeadChecksum;
  const startEnvelope = await readEnvelopeAt(
    handle,
    offset,
    head.checkpointBytes,
    sequence,
    normalizeCheckpointFrame,
  );
  const start = startEnvelope.frame;
  ensure(
    start.type === "checkpoint-start" &&
      start.generation === head.generation &&
      start.stateRevision === head.checkpointStateRevision &&
      start.baseHeadChecksum === head.baseHeadChecksum &&
      start.previousChecksum === checksum &&
      start.operationCount + start.storageCount + 2 === head.checkpointFrameCount,
    "corrupt_ledger",
  );
  checksum = startEnvelope.checksum;
  offset = startEnvelope.frameEnd;
  sequence += 1;
  const state = emptyGenerationState(head.checkpointStateRevision);
  const stateHash = createCheckpointStateHash(head.checkpointStateRevision);
  let previousOperationId = null;
  for (let index = 0; index < start.operationCount; index += 1) {
    const envelope = await readEnvelopeAt(
      handle,
      offset,
      head.checkpointBytes,
      sequence,
      normalizeCheckpointFrame,
    );
    const frame = envelope.frame;
    ensure(
      frame.type === "checkpoint-operation" &&
        frame.generation === head.generation &&
        frame.previousChecksum === checksum &&
        (previousOperationId === null ||
          previousOperationId < frame.operation.operationId) &&
        !mapHas(state.operations, frame.operation.operationId),
      "corrupt_ledger",
    );
    previousOperationId = frame.operation.operationId;
    mapSet(
      state.operations,
      frame.operation.operationId,
      checkpointOperationStateRecord(frame.operation),
    );
    updateCheckpointStateHash(stateHash, "operation", frame.operation);
    checksum = envelope.checksum;
    offset = envelope.frameEnd;
    sequence += 1;
  }
  let previousStorageId = null;
  for (let index = 0; index < start.storageCount; index += 1) {
    const envelope = await readEnvelopeAt(
      handle,
      offset,
      head.checkpointBytes,
      sequence,
      normalizeCheckpointFrame,
    );
    const frame = envelope.frame;
    ensure(
      frame.type === "checkpoint-storage" &&
        frame.generation === head.generation &&
        frame.previousChecksum === checksum &&
        (previousStorageId === null || previousStorageId < frame.storage.storageId) &&
        !mapHas(state.storages, frame.storage.storageId),
      "corrupt_ledger",
    );
    previousStorageId = frame.storage.storageId;
    mapSet(state.storages, frame.storage.storageId, frame.storage);
    updateCheckpointStateHash(stateHash, "storage", frame.storage);
    checksum = envelope.checksum;
    offset = envelope.frameEnd;
    sequence += 1;
  }
  const endEnvelope = await readEnvelopeAt(
    handle,
    offset,
    head.checkpointBytes,
    sequence,
    normalizeCheckpointFrame,
  );
  const end = endEnvelope.frame;
  ensure(
    end.type === "checkpoint-end" &&
      end.generation === head.generation &&
      end.stateRevision === head.checkpointStateRevision &&
      end.operationCount === start.operationCount &&
      end.storageCount === start.storageCount &&
      end.previousChecksum === checksum &&
      end.stateChecksum === finishCheckpointStateHash(stateHash) &&
      endEnvelope.frameEnd === head.checkpointBytes &&
      endEnvelope.checksum === head.checkpointChecksum &&
      sequence === head.checkpointFrameCount,
    "corrupt_ledger",
  );
  validateCheckpointState(state, "corrupt_ledger");
  return state;
}

async function loadCheckpointState(authority, checkpoint, head) {
  ensure(
    checkpoint.metadata.size === BigIntConstructor(head.checkpointBytes),
    "corrupt_ledger",
  );
  const first = await parseCheckpointStream(checkpoint.handle, head);
  const after = await assertPathFileCurrent(
    checkpoint.pin.path,
    checkpoint.handle,
    checkpoint.pin.identity,
    authority.currentUid,
  );
  ensure(after.size === checkpoint.metadata.size, "corrupt_ledger");
  if (
    after.mtimeNs === checkpoint.metadata.mtimeNs &&
    after.ctimeNs === checkpoint.metadata.ctimeNs
  ) {
    return objectFreeze({ metadata: after, state: first });
  }
  const second = await parseCheckpointStream(checkpoint.handle, head);
  const finalMetadata = await assertPathFileCurrent(
    checkpoint.pin.path,
    checkpoint.handle,
    checkpoint.pin.identity,
    authority.currentUid,
  );
  ensure(
    finalMetadata.size === after.size &&
      finalMetadata.mtimeNs === after.mtimeNs &&
      finalMetadata.ctimeNs === after.ctimeNs,
    "corrupt_ledger",
  );
  return objectFreeze({ metadata: finalMetadata, state: second });
}

function checkpointIdentityEqual(left, right) {
  return (
    left.generation === right.generation &&
    left.baseHeadChecksum === right.baseHeadChecksum &&
    left.checkpointStateRevision === right.checkpointStateRevision &&
    left.checkpointFrameCount === right.checkpointFrameCount &&
    left.checkpointChecksum === right.checkpointChecksum &&
    left.checkpointBytes === right.checkpointBytes
  );
}

async function truncateActiveTail(
  authority,
  headAnchor,
  ledger,
  bytes,
  offset,
  lock,
  trustedHead,
) {
  try {
    await lock.assertHeld();
    await authority.assertCurrent();
    const observed = await readTrustedLedgerHead(headAnchor);
    ensure(canonicalEqual(observed, trustedHead), "corrupt_ledger");
    const current = await readStableLedger(authority, ledger);
    ensure(bufferEquals(current, bytes), "corrupt_ledger");
    await ledger.handle.truncate(offset);
    await ledger.handle.sync();
    await lock.assertHeld();
    const recovered = await readStableLedger(authority, ledger);
    ensure(
      bufferEquals(recovered, bufferSubarray(bytes, 0, offset)),
      "corrupt_ledger",
    );
    return recovered;
  } catch (error) {
    if (isInternalError(error)) throw error;
    fail("maintenance_failed");
  }
}

async function closeStateFile(file) {
  if (file === undefined) return;
  try {
    await file.handle.close();
  } catch {
    fail("maintenance_failed");
  }
}

async function loadGenerationState({
  authority,
  cache,
  head,
  headAnchor,
  lock,
}) {
  if (head.generation === "0" && head.stateRevision === "0") {
    return objectFreeze({
      cache: objectFreeze({
        checkpointMetadata: null,
        checkpointPin: null,
        checkpointState: emptyGenerationState("0"),
        head,
        ledgerMetadata: null,
        ledgerPin: null,
        state: emptyGenerationState("0"),
      }),
      state: emptyGenerationState("0"),
    });
  }

  let checkpoint;
  let ledger;
  try {
    const cacheSameGeneration = cache?.head.generation === head.generation;
    if (head.generation !== "0") {
      checkpoint = await openNamedStateFile(
        authority,
        stateCheckpointName(head.generation, "corrupt_ledger"),
        {
          expectedPin: cacheSameGeneration ? cache.checkpointPin : undefined,
        },
      );
    }
    ledger = await openNamedStateFile(
      authority,
      stateLedgerName(head.generation, "corrupt_ledger"),
      {
        expectedPin:
          cacheSameGeneration && cache.ledgerPin !== null
            ? cache.ledgerPin
            : undefined,
        writable: true,
      },
    );
    ensure(
      ledger.metadata.size >= BigIntConstructor(head.ledgerBytes) &&
        ledger.metadata.size <= BigIntConstructor(MAX_LEDGER_BYTES),
      "corrupt_ledger",
    );

    const checkpointMetadata =
      checkpoint === undefined ? null : metadataSnapshot(checkpoint.metadata);
    const ledgerMetadata = metadataSnapshot(ledger.metadata);
    if (
      cache !== undefined &&
      canonicalEqual(cache.head, head) &&
      (checkpointMetadata === null
        ? cache.checkpointMetadata === null
        : cache.checkpointMetadata !== null &&
          sameMetadataSnapshot(checkpointMetadata, cache.checkpointMetadata)) &&
      cache.ledgerMetadata !== null &&
      sameMetadataSnapshot(ledgerMetadata, cache.ledgerMetadata)
    ) {
      return objectFreeze({ cache, state: cache.state });
    }

    let checkpointState;
    let finalCheckpointMetadata = checkpointMetadata;
    if (head.generation === "0") {
      checkpointState = emptyGenerationState("0");
    } else if (
      cacheSameGeneration &&
      checkpointIdentityEqual(cache.head, head) &&
      cache.checkpointMetadata !== null &&
      checkpointMetadata !== null &&
      sameMetadataSnapshot(cache.checkpointMetadata, checkpointMetadata)
    ) {
      checkpointState = cache.checkpointState;
    } else {
      const loaded = await loadCheckpointState(authority, checkpoint, head);
      checkpointState = loaded.state;
      finalCheckpointMetadata = metadataSnapshot(loaded.metadata);
    }

    let bytes = await readStableLedger(authority, ledger);
    const parsed = parseActiveLedger(bytes, head, checkpointState);
    if (parsed.truncateOffset !== null) {
      bytes = await truncateActiveTail(
        authority,
        headAnchor,
        ledger,
        bytes,
        parsed.truncateOffset,
        lock,
        head,
      );
      const reparsed = parseActiveLedger(bytes, head, checkpointState);
      ensure(reparsed.truncateOffset === null, "corrupt_ledger");
    }
    const finalLedgerMetadata = await assertPathFileCurrent(
      ledger.pin.path,
      ledger.handle,
      ledger.pin.identity,
      authority.currentUid,
    );
    const nextCache = objectFreeze({
      checkpointMetadata: finalCheckpointMetadata,
      checkpointPin: checkpoint?.pin ?? null,
      checkpointState,
      head,
      ledgerMetadata: metadataSnapshot(finalLedgerMetadata),
      ledgerPin: ledger.pin,
      state: parsed.state,
    });
    return objectFreeze({ cache: nextCache, state: parsed.state });
  } finally {
    await closeStateFile(ledger);
    await closeStateFile(checkpoint);
  }
}

async function compareAndResolveTrustedHead(anchor, expectedHead, nextHead) {
  let acknowledged = false;
  try {
    acknowledged = await invokeNativePromise(
      anchor.compareAndAdvance,
      [objectFreeze({ expectedHead, nextHead })],
      "io_failed",
    );
    if (acknowledged === true) return "advanced";
  } catch {
    // A read-back below is the only authority after an acknowledgement loss.
  }
  try {
    const observed = await readTrustedLedgerHead(anchor);
    if (canonicalEqual(observed, nextHead)) return "advanced";
    if (canonicalEqual(observed, expectedHead)) return "unchanged";
  } catch {
    // The caller must treat the transition as unresolved.
  }
  return "unknown";
}

async function writeCheckpointFrames(handle, state, generation, baseHeadChecksum) {
  const snapshot = checkpointArrays(state);
  const checkpointFrameCount =
    snapshot.operations.length + snapshot.storages.length + 2;
  ensure(
    numberIsSafeIntegerIntrinsic(checkpointFrameCount) &&
      checkpointFrameCount <= MAX_UINT32,
    "state_capacity_exhausted",
  );
  const stateChecksum = checkpointStateChecksum(
    state.stateRevision,
    snapshot.operations,
    snapshot.storages,
  );
  let sequence = 1;
  let previousChecksum = baseHeadChecksum;
  let checkpointBytes = 0;
  const appendCheckpointFrame = async (value) => {
    const frame = normalizeCheckpointFrame(
      {
        ...value,
        contractVersion: FILESYSTEM_IMAGE_PROVIDER_STATE_CONTRACT_VERSION,
        generation,
        previousChecksum,
        sequence,
      },
      "corrupt_ledger",
    );
    const encoded = encodeCanonicalFrame(frame, "state_capacity_exhausted");
    ensure(
      checkpointBytes <= NUMBER_MAX_SAFE_INTEGER - encoded.bytes.length,
      "state_capacity_exhausted",
    );
    await writeAll(handle, encoded.bytes, checkpointBytes);
    checkpointBytes += encoded.bytes.length;
    previousChecksum = encoded.checksum;
    sequence += 1;
  };
  await appendCheckpointFrame({
    baseHeadChecksum,
    operationCount: snapshot.operations.length,
    stateRevision: state.stateRevision,
    storageCount: snapshot.storages.length,
    type: "checkpoint-start",
  });
  for (const operation of snapshot.operations) {
    await appendCheckpointFrame({
      operation,
      type: "checkpoint-operation",
    });
  }
  for (const storage of snapshot.storages) {
    await appendCheckpointFrame({
      storage,
      type: "checkpoint-storage",
    });
  }
  await appendCheckpointFrame({
    operationCount: snapshot.operations.length,
    stateChecksum,
    stateRevision: state.stateRevision,
    storageCount: snapshot.storages.length,
    type: "checkpoint-end",
  });
  ensure(sequence - 1 === checkpointFrameCount, "corrupt_ledger");
  return objectFreeze({
    checkpointBytes,
    checkpointChecksum: previousChecksum,
    checkpointFrameCount,
  });
}

async function rotateGeneration({
  authority,
  cache,
  headAnchor,
  lock,
  syncDirectory,
}) {
  const expectedHead = cache.head;
  const nextGeneration = incrementNonnegativeUint64(
    expectedHead.generation,
    "state_capacity_exhausted",
  );
  const baseHeadChecksum = headChecksum(expectedHead, "corrupt_ledger");
  let checkpoint;
  let ledger;
  let committed = false;
  try {
    checkpoint = await createNamedStateFile(
      authority,
      stateCheckpointName(nextGeneration, "corrupt_ledger"),
    );
    ledger = await createNamedStateFile(
      authority,
      stateLedgerName(nextGeneration, "corrupt_ledger"),
    );
    const checkpointResult = await writeCheckpointFrames(
      checkpoint.handle,
      cache.state,
      nextGeneration,
      baseHeadChecksum,
    );
    await checkpoint.handle.sync();
    await ledger.handle.sync();
    const checkpointMetadata = await assertPathFileCurrent(
      checkpoint.pin.path,
      checkpoint.handle,
      checkpoint.pin.identity,
      authority.currentUid,
    );
    const ledgerMetadata = await assertPathFileCurrent(
      ledger.pin.path,
      ledger.handle,
      ledger.pin.identity,
      authority.currentUid,
      { empty: true },
    );
    ensure(
      checkpointMetadata.size ===
        BigIntConstructor(checkpointResult.checkpointBytes),
      "corrupt_ledger",
    );
    await syncDirectory(authority.handle, authority.path);
    await lock.assertHeld();
    await authority.assertCurrent();
    const observedBeforeCas = await readTrustedLedgerHead(headAnchor);
    ensure(canonicalEqual(observedBeforeCas, expectedHead), "corrupt_ledger");
    const nextHead = canonicalLedgerHead(
      {
        contractVersion: FILESYSTEM_IMAGE_PROVIDER_STATE_HEAD_CONTRACT_VERSION,
        anchorRevision: incrementNonnegativeUint64(
          expectedHead.anchorRevision,
          "state_capacity_exhausted",
        ),
        generation: nextGeneration,
        stateRevision: expectedHead.stateRevision,
        baseHeadChecksum,
        checkpointStateRevision: expectedHead.stateRevision,
        checkpointFrameCount: checkpointResult.checkpointFrameCount,
        checkpointChecksum: checkpointResult.checkpointChecksum,
        checkpointBytes: checkpointResult.checkpointBytes,
        frameCount: 0,
        lastChecksum: checkpointResult.checkpointChecksum,
        ledgerBytes: 0,
      },
      "corrupt_ledger",
    );
    const candidateCheckpoint = {
      ...checkpoint,
      metadata: checkpointMetadata,
    };
    const loaded = await loadCheckpointState(authority, candidateCheckpoint, nextHead);
    const loadedSnapshot = checkpointArrays(loaded.state);
    const expectedSnapshot = checkpointArrays(cache.state);
    ensure(
      checkpointStateChecksum(
        loaded.state.stateRevision,
        loadedSnapshot.operations,
        loadedSnapshot.storages,
      ) ===
        checkpointStateChecksum(
          cache.state.stateRevision,
          expectedSnapshot.operations,
          expectedSnapshot.storages,
        ),
      "corrupt_ledger",
    );
    const outcome = await compareAndResolveTrustedHead(
      headAnchor,
      expectedHead,
      nextHead,
    );
    if (outcome === "unchanged") {
      await ignoreRejection(checkpoint.handle.close());
      checkpoint = undefined;
      await ignoreRejection(ledger.handle.close());
      ledger = undefined;
      await cleanupGeneration(authority, nextGeneration, syncDirectory);
      fail("maintenance_failed");
    }
    if (outcome !== "advanced") fail("maintenance_failed");
    committed = true;
    const nextCache = objectFreeze({
      checkpointMetadata: metadataSnapshot(loaded.metadata),
      checkpointPin: checkpoint.pin,
      checkpointState: loaded.state,
      head: nextHead,
      ledgerMetadata: metadataSnapshot(ledgerMetadata),
      ledgerPin: ledger.pin,
      state: loaded.state,
    });
    await ignoreRejection(checkpoint.handle.close());
    checkpoint = undefined;
    await ignoreRejection(ledger.handle.close());
    ledger = undefined;
    await cleanupGeneration(authority, expectedHead.generation, syncDirectory);
    return nextCache;
  } catch (error) {
    if (checkpoint !== undefined) await ignoreRejection(checkpoint.handle.close());
    if (ledger !== undefined) await ignoreRejection(ledger.handle.close());
    if (committed) throw error;
    if (isInternalError(error)) throw error;
    fail("maintenance_failed");
  }
}

function encodeDeltaEvent(head, event) {
  const sequence = head.frameCount + 1;
  ensure(sequence <= MAX_FRAME_COUNT, "state_capacity_exhausted");
  const frame = normalizeDeltaFrame(
    {
      ...event,
      contractVersion: FILESYSTEM_IMAGE_PROVIDER_STATE_CONTRACT_VERSION,
      previousChecksum: head.lastChecksum,
      sequence,
      stateRevision: incrementNonnegativeUint64(
        head.stateRevision,
        "state_capacity_exhausted",
      ),
    },
    "invalid_request",
  );
  return encodeCanonicalFrame(frame, "invalid_request");
}

function eventRequiresRotation(head, encodedBytes, rotationPolicy) {
  if (
    head.frameCount >= MAX_FRAME_COUNT ||
    head.ledgerBytes > MAX_LEDGER_BYTES - encodedBytes
  ) {
    return true;
  }
  if (head.frameCount === 0) return false;
  return (
    head.frameCount + 1 > rotationPolicy.activeFrameCountWatermark ||
    head.ledgerBytes + encodedBytes >
      rotationPolicy.activeLedgerBytesWatermark
  );
}

async function appendDeltaEvent({
  authority,
  cache,
  event,
  headAnchor,
  lock,
  syncDirectory,
}) {
  const expectedHead = cache.head;
  const encoded = encodeDeltaEvent(expectedHead, event);
  ensure(
    expectedHead.ledgerBytes <= MAX_LEDGER_BYTES - encoded.bytes.length,
    "state_capacity_exhausted",
  );
  const trueGenesis =
    expectedHead.generation === "0" && expectedHead.stateRevision === "0";
  let ledger;
  let casStarted = false;
  let writeStarted = false;
  let outcome = "unknown";
  try {
    ledger = trueGenesis
      ? await createNamedStateFile(
          authority,
          stateLedgerName(expectedHead.generation, "corrupt_ledger"),
        )
      : await openNamedStateFile(
          authority,
          stateLedgerName(expectedHead.generation, "corrupt_ledger"),
          { expectedPin: cache.ledgerPin, writable: true },
        );
    ensure(
      ledger.metadata.size === BigIntConstructor(expectedHead.ledgerBytes),
      "corrupt_ledger",
    );
    await lock.assertHeld();
    await authority.assertCurrent();
    const observedBeforeWrite = await readTrustedLedgerHead(headAnchor);
    ensure(canonicalEqual(observedBeforeWrite, expectedHead), "corrupt_ledger");
    writeStarted = true;
    await writeAll(ledger.handle, encoded.bytes, expectedHead.ledgerBytes);
    await ledger.handle.sync();
    if (trueGenesis) await syncDirectory(authority.handle, authority.path);
    await lock.assertHeld();
    const readback = await readStableLedger(authority, ledger);
    const nextHead = canonicalLedgerHead(
      {
        contractVersion: FILESYSTEM_IMAGE_PROVIDER_STATE_HEAD_CONTRACT_VERSION,
        anchorRevision: incrementNonnegativeUint64(
          expectedHead.anchorRevision,
          "state_capacity_exhausted",
        ),
        generation: expectedHead.generation,
        stateRevision: encoded.frame.stateRevision,
        baseHeadChecksum: expectedHead.baseHeadChecksum,
        checkpointStateRevision: expectedHead.checkpointStateRevision,
        checkpointFrameCount: expectedHead.checkpointFrameCount,
        checkpointChecksum: expectedHead.checkpointChecksum,
        checkpointBytes: expectedHead.checkpointBytes,
        frameCount: encoded.frame.sequence,
        lastChecksum: encoded.checksum,
        ledgerBytes: expectedHead.ledgerBytes + encoded.bytes.length,
      },
      "corrupt_ledger",
    );
    const parsed = parseActiveLedger(readback, nextHead, cache.checkpointState);
    ensure(parsed.truncateOffset === null, "corrupt_ledger");
    const finalMetadata = await assertPathFileCurrent(
      ledger.pin.path,
      ledger.handle,
      ledger.pin.identity,
      authority.currentUid,
    );
    casStarted = true;
    outcome = await compareAndResolveTrustedHead(
      headAnchor,
      expectedHead,
      nextHead,
    );
    if (outcome === "unchanged") {
      if (trueGenesis) {
        await ignoreRejection(ledger.handle.close());
        ledger = undefined;
        await cleanupGeneration(
          authority,
          expectedHead.generation,
          syncDirectory,
        );
      } else {
        await ledger.handle.truncate(expectedHead.ledgerBytes);
        await ledger.handle.sync();
      }
      fail("io_failed");
    }
    if (outcome !== "advanced") fail("commit_outcome_uncertain");
    const nextCache = objectFreeze({
      checkpointMetadata: cache.checkpointMetadata,
      checkpointPin: cache.checkpointPin,
      checkpointState: cache.checkpointState,
      head: nextHead,
      ledgerMetadata: metadataSnapshot(finalMetadata),
      ledgerPin: ledger.pin,
      state: parsed.state,
    });
    await ignoreRejection(ledger.handle.close());
    ledger = undefined;
    return nextCache;
  } catch (error) {
    if (!casStarted && writeStarted && ledger !== undefined) {
      try {
        if (trueGenesis) {
          await ignoreRejection(ledger.handle.close());
          ledger = undefined;
          await cleanupGeneration(
            authority,
            expectedHead.generation,
            syncDirectory,
          );
        } else {
          await ledger.handle.truncate(expectedHead.ledgerBytes);
          await ledger.handle.sync();
        }
      } catch {
        if (ledger !== undefined) await ignoreRejection(ledger.handle.close());
        fail("maintenance_failed");
      }
    }
    if (ledger !== undefined) await ignoreRejection(ledger.handle.close());
    if (isInternalError(error)) throw error;
    if (casStarted && outcome === "unknown") fail("commit_outcome_uncertain");
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
  mapForEach(state.operations, (operation) =>
    arrayPush(operations, publicOperationRecord(operation)));
  arraySort(operations, (left, right) =>
    compareIds(left.operationId, right.operationId));
  const storages = [];
  mapForEach(state.storages, (storage) => arrayPush(storages, storage));
  arraySort(storages, (left, right) =>
    compareIds(left.storageId, right.storageId));
  return objectFreeze({
    contractVersion: FILESYSTEM_IMAGE_PROVIDER_STATE_CONTRACT_VERSION,
    sequence: NumberConstructor(
      canonicalUint64(state.stateRevision, "corrupt_ledger").parsed,
    ),
    operations: objectFreeze(operations),
    storages: objectFreeze(storages),
  });
}

function operationView(record, currentStorageState, transient) {
  return objectFreeze({
    ...publicOperationRecord(record),
    currentStorageState,
    ...transient,
  });
}

function normalizeRuntimeError(error) {
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
  #cache;
  #lockRetryMs;
  #lockTimeoutMs;
  #rotationPolicy;
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
        "rotationPolicy",
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
    const rotationPolicy = objectHasOwn(normalized, "rotationPolicy")
      ? exactDataObject(
          normalized.rotationPolicy,
          ["activeLedgerBytesWatermark", "activeFrameCountWatermark"],
          ["activeLedgerBytesWatermark", "activeFrameCountWatermark"],
          "invalid_request",
        )
      : {
          activeLedgerBytesWatermark:
            FILESYSTEM_IMAGE_PROVIDER_STATE_DEFAULT_ACTIVE_LEDGER_BYTES_WATERMARK,
          activeFrameCountWatermark:
            FILESYSTEM_IMAGE_PROVIDER_STATE_DEFAULT_ACTIVE_FRAME_COUNT_WATERMARK,
        };
    this.#rotationPolicy = objectFreeze({
      activeLedgerBytesWatermark: rotationPolicy.activeLedgerBytesWatermark,
      activeFrameCountWatermark: rotationPolicy.activeFrameCountWatermark,
    });
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
        this.#lockTimeoutMs <= 60_000 &&
        numberIsSafeIntegerIntrinsic(
          this.#rotationPolicy.activeLedgerBytesWatermark,
        ) &&
        this.#rotationPolicy.activeLedgerBytesWatermark >= 1 &&
        this.#rotationPolicy.activeLedgerBytesWatermark <= MAX_LEDGER_BYTES &&
        numberIsSafeIntegerIntrinsic(
          this.#rotationPolicy.activeFrameCountWatermark,
        ) &&
        this.#rotationPolicy.activeFrameCountWatermark >= 1 &&
        this.#rotationPolicy.activeFrameCountWatermark <= MAX_FRAME_COUNT,
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
      let authority;
      let lock;
      let primaryError;
      let completed;
      let userCommitted = false;
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
        const trustedHead = await readTrustedLedgerHead(this.#headAnchor);
        const trueGenesis =
          trustedHead.generation === "0" && trustedHead.stateRevision === "0";
        if (trueGenesis) {
          await cleanupGeneration(authority, "0", this.#syncDirectory);
        } else if (trustedHead.generation === "0") {
          await cleanupCheckpointFile(authority, "0", this.#syncDirectory);
        } else if (trustedHead.generation !== "0") {
          await cleanupGeneration(
            authority,
            decrementPositiveUint64(trustedHead.generation, "corrupt_ledger"),
            this.#syncDirectory,
          );
        }
        if (canonicalUint64(trustedHead.generation, "corrupt_ledger").parsed < MAX_UINT64) {
          await cleanupGeneration(
            authority,
            incrementNonnegativeUint64(
              trustedHead.generation,
              "corrupt_ledger",
            ),
            this.#syncDirectory,
          );
        }
        let loaded = await loadGenerationState({
          authority,
          cache: this.#cache,
          head: trustedHead,
          headAnchor: this.#headAnchor,
          lock,
        });
        let currentCache = loaded.cache;
        this.#cache = currentCache;
        const append = async (event) => {
          if (currentCache.head.frameCount >= MAX_FRAME_COUNT) {
            currentCache = await rotateGeneration({
              authority,
              cache: currentCache,
              headAnchor: this.#headAnchor,
              lock,
              syncDirectory: this.#syncDirectory,
            });
            this.#cache = currentCache;
          } else {
            const preview = encodeDeltaEvent(currentCache.head, event);
            if (
              eventRequiresRotation(
                currentCache.head,
                preview.bytes.length,
                this.#rotationPolicy,
              )
            ) {
              currentCache = await rotateGeneration({
                authority,
                cache: currentCache,
                headAnchor: this.#headAnchor,
                lock,
                syncDirectory: this.#syncDirectory,
              });
              this.#cache = currentCache;
            }
          }
          currentCache = await appendDeltaEvent({
            authority,
            cache: currentCache,
            event,
            headAnchor: this.#headAnchor,
            lock,
            syncDirectory: this.#syncDirectory,
          });
          userCommitted = true;
          this.#cache = currentCache;
          return objectFreeze({ state: currentCache.state });
        };
        completed = await operation({
          append,
          state: currentCache.state,
        });
        if (!userCommitted) {
          await lock.assertHeld();
          await authority.assertCurrent();
        }
      } catch (error) {
        primaryError = normalizeRuntimeError(error);
        if (
          primaryError.code === "commit_outcome_uncertain" ||
          primaryError.code === "maintenance_failed" ||
          primaryError.code === "corrupt_ledger"
        ) {
          this.#cache = undefined;
        }
      }

      let cleanupFailed = false;
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
      if (cleanupFailed && !userCommitted) {
        primaryError ??= stateError("maintenance_failed");
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
        preparedChecksum: existing._preparedChecksum,
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

  async inspectCapacity() {
    return await this.#run(async ({ state }) => {
      const head = this.#cache.head;
      let preparedOperationCount = 0;
      mapForEach(state.operations, (operation) => {
        if (operation.state === "prepared") preparedOperationCount += 1;
      });
      return objectFreeze({
        contractVersion: FILESYSTEM_IMAGE_PROVIDER_STATE_CONTRACT_VERSION,
        anchorRevision: head.anchorRevision,
        generation: head.generation,
        stateRevision: head.stateRevision,
        checkpointStateRevision: head.checkpointStateRevision,
        checkpointBytes: head.checkpointBytes,
        checkpointFrameCount: head.checkpointFrameCount,
        activeLedgerBytes: head.ledgerBytes,
        activeFrameCount: head.frameCount,
        remainingLedgerBytes: MAX_LEDGER_BYTES - head.ledgerBytes,
        remainingFrameCount: MAX_FRAME_COUNT - head.frameCount,
        activeLedgerBytesWatermark:
          this.#rotationPolicy.activeLedgerBytesWatermark,
        activeFrameCountWatermark:
          this.#rotationPolicy.activeFrameCountWatermark,
        rotationRequired:
          head.frameCount >= this.#rotationPolicy.activeFrameCountWatermark ||
          head.ledgerBytes >= this.#rotationPolicy.activeLedgerBytesWatermark,
        retainedOperationCount: mapSize(state.operations),
        preparedOperationCount,
        storageCount: mapSize(state.storages),
      });
    });
  }

  // snapshot() always validates the authoritative head and selected file
  // identities under the lock. An exact head-and-metadata cache hit avoids
  // replay; cold opens and changed content still rebuild the complete state.
  async snapshot() {
    return await this.#run(async ({ state }) => snapshotFromState(state));
  }
}

objectFreeze(FilesystemImageProviderStateError.prototype);
objectFreeze(FilesystemImageProviderStateError);
objectFreeze(normalizeFilesystemImageProviderStateHead);
objectFreeze(filesystemImageProviderStateHeadChecksum);
objectFreeze(filesystemImageProviderStateCheckpointName);
objectFreeze(filesystemImageProviderStateLedgerName);
objectFreeze(FilesystemImageProviderState.prototype);
objectFreeze(FilesystemImageProviderState);
