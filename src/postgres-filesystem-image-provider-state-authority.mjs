import { Buffer } from "node:buffer";
import { Hash, createHash } from "node:crypto";
import { basename, dirname, isAbsolute, parse, resolve } from "node:path";
import { types as utilTypes } from "node:util";

import {
  FILESYSTEM_IMAGE_PROVIDER_STATE_HEAD_CONTRACT_VERSION,
  filesystemImageProviderStateHeadChecksum,
  normalizeFilesystemImageProviderStateHead,
} from "./filesystem-image-provider-state.mjs";
import {
  PostgresSerializableStore,
  isPostgresSerializableStore,
} from "./postgres-serializable-store.mjs";

export const POSTGRES_FILESYSTEM_IMAGE_PROVIDER_STATE_AUTHORITY_CONTRACT_VERSION =
  1;

const MAX_CANONICAL_BYTES = 768 * 1024;
const MAX_OPERATION_RECORD_BYTES = 4 * 1024 * 1024;
const MAX_CANONICAL_DEPTH = 32;
const MAX_CANONICAL_NODES = 16_384;
const MAX_CHECKPOINT_FRAME_COUNT = 4_294_967_295;
const MAX_CHECKPOINT_BYTES = 9_007_199_254_740_991;
const MAX_FRAME_COUNT = 65_535;
const MAX_LEDGER_BYTES = 64 * 1024 * 1024;
const MAX_PATH_BYTES = 4_095;
const MAX_PHYSICAL_OBJECT_ID_BYTES = 512;
const MAX_UINT64 = 18_446_744_073_709_551_615n;
// A row may carry a 4 MiB prepared record and a 4 MiB committed record. Keep
// limit-plus-one materialization bounded to at most five such rows.
const MAX_PAGE_SIZE = 4;
const OPAQUE_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u;
const DECIMAL_PATTERN = /^(?:0|[1-9][0-9]*)$/u;
const SHA256_PATTERN = /^[0-9a-f]{64}$/u;
const OPERATION_KINDS = Object.freeze([
  "provision",
  "attach",
  "detach",
  "destroy",
  "checkpoint",
  "restore",
  "restore-attach",
]);
const LIFECYCLES = Object.freeze([
  "provisioned",
  "attached",
  "detached",
  "destroyed",
]);
const OPERATION_RECORD_DOMAIN = Buffer.from(
  "portable-codex/filesystem-image-provider-state/operation-record/v1\0",
  "utf8",
);

const OPTION_KEYS = Object.freeze(["store", "providerId", "anchorId"]);
const READ_OPERATION_KEYS = Object.freeze(["expectedHead", "operationId"]);
const READ_PAGE_KEYS = Object.freeze([
  "afterOperationId",
  "expectedHead",
  "limit",
]);
const ADVANCE_KEYS = Object.freeze([
  "expectedHead",
  "nextHead",
  "transition",
]);
const PREPARED_RECORD_KEYS = Object.freeze([
  "kind",
  "operationId",
  "preparedChecksum",
  "preparedStateRevision",
  "request",
  "state",
  "storageId",
  "storageStateBefore",
]);
const COMMITTED_RECORD_KEYS = Object.freeze([
  ...PREPARED_RECORD_KEYS,
  "committedStateRevision",
  "expectedStorage",
  "result",
  "storageState",
]);
const HEAD_ROW_KEYS = Object.freeze([
  "provider_id",
  "anchor_id",
  "contract_version",
  "anchor_revision",
  "generation",
  "state_revision",
  "base_head_checksum",
  "checkpoint_state_revision",
  "checkpoint_frame_count",
  "checkpoint_checksum",
  "checkpoint_bytes",
  "frame_count",
  "last_checksum",
  "ledger_bytes",
]);
const OPERATION_ROW_KEYS = Object.freeze([
  "provider_id",
  "anchor_id",
  "operation_id",
  "record_contract_version",
  "state",
  "kind",
  "storage_id",
  "prepared_state_revision",
  "prepared_checksum",
  "prepared_record_bytes",
  "prepared_record_sha256",
  "committed_state_revision",
  "committed_checksum_provenance",
  "committed_checksum",
  "committed_record_bytes",
  "committed_record_sha256",
]);
const ERROR_MESSAGES = Object.freeze({
  invalid_postgres_filesystem_image_provider_state_authority_options:
    "PostgreSQL filesystem image provider state authority options are invalid",
  invalid_postgres_filesystem_image_provider_state_authority_request:
    "PostgreSQL filesystem image provider state authority request is invalid",
  postgres_filesystem_image_provider_state_authority_state_invalid:
    "PostgreSQL filesystem image provider state authority state is invalid",
});

const HEAD_COLUMNS = [
  "provider_id",
  "anchor_id",
  "contract_version",
  "anchor_revision::pg_catalog.text AS anchor_revision",
  "generation::pg_catalog.text AS generation",
  "state_revision::pg_catalog.text AS state_revision",
  "base_head_checksum",
  "checkpoint_state_revision::pg_catalog.text AS checkpoint_state_revision",
  "checkpoint_frame_count::pg_catalog.text AS checkpoint_frame_count",
  "checkpoint_checksum",
  "checkpoint_bytes::pg_catalog.text AS checkpoint_bytes",
  "frame_count::pg_catalog.text AS frame_count",
  "last_checksum",
  "ledger_bytes::pg_catalog.text AS ledger_bytes",
].join(", ");
const OPERATION_COLUMNS = [
  "provider_id",
  "anchor_id",
  "operation_id",
  "record_contract_version",
  "state",
  "kind",
  "storage_id",
  "prepared_state_revision::pg_catalog.text AS prepared_state_revision",
  "prepared_checksum",
  "prepared_record_bytes",
  "prepared_record_sha256",
  "committed_state_revision::pg_catalog.text AS committed_state_revision",
  "committed_checksum_provenance",
  "committed_checksum",
  "committed_record_bytes",
  "committed_record_sha256",
].join(", ");
const READ_HEAD_QUERY = [
  `SELECT ${HEAD_COLUMNS}`,
  "FROM session_authority.filesystem_image_provider_heads",
  "WHERE provider_id = $1 AND anchor_id = $2",
].join(" ");
const INSERT_HEAD_QUERY = [
  "INSERT INTO session_authority.filesystem_image_provider_heads",
  "(provider_id, anchor_id, contract_version, anchor_revision, generation,",
  "state_revision, base_head_checksum, checkpoint_state_revision,",
  "checkpoint_frame_count, checkpoint_checksum, checkpoint_bytes,",
  "frame_count, last_checksum, ledger_bytes)",
  "VALUES ($1, $2, $3, $4::pg_catalog.numeric, $5::pg_catalog.numeric,",
  "$6::pg_catalog.numeric, $7, $8::pg_catalog.numeric, $9::pg_catalog.int8,",
  "$10, $11::pg_catalog.int8, $12::pg_catalog.int4, $13, $14::pg_catalog.int8)",
  "ON CONFLICT (provider_id, anchor_id) DO NOTHING",
  `RETURNING ${HEAD_COLUMNS}`,
].join(" ");
const UPDATE_HEAD_QUERY = [
  "UPDATE session_authority.filesystem_image_provider_heads",
  "SET contract_version = $3, anchor_revision = $4::pg_catalog.numeric,",
  "generation = $5::pg_catalog.numeric, state_revision = $6::pg_catalog.numeric,",
  "base_head_checksum = $7, checkpoint_state_revision = $8::pg_catalog.numeric,",
  "checkpoint_frame_count = $9::pg_catalog.int8, checkpoint_checksum = $10,",
  "checkpoint_bytes = $11::pg_catalog.int8, frame_count = $12::pg_catalog.int4,",
  "last_checksum = $13, ledger_bytes = $14::pg_catalog.int8",
  "WHERE provider_id = $1 AND anchor_id = $2",
  "AND contract_version = $15",
  "AND anchor_revision = $16::pg_catalog.numeric",
  "AND generation = $17::pg_catalog.numeric",
  "AND state_revision = $18::pg_catalog.numeric",
  "AND base_head_checksum IS NOT DISTINCT FROM $19",
  "AND checkpoint_state_revision = $20::pg_catalog.numeric",
  "AND checkpoint_frame_count = $21::pg_catalog.int8",
  "AND checkpoint_checksum IS NOT DISTINCT FROM $22",
  "AND checkpoint_bytes = $23::pg_catalog.int8",
  "AND frame_count = $24::pg_catalog.int4",
  "AND last_checksum IS NOT DISTINCT FROM $25",
  "AND ledger_bytes = $26::pg_catalog.int8",
  `RETURNING ${HEAD_COLUMNS}`,
].join(" ");
const READ_OPERATION_QUERY = [
  `SELECT ${OPERATION_COLUMNS}`,
  "FROM session_authority.filesystem_image_provider_operations",
  "WHERE provider_id = $1 AND anchor_id = $2 AND operation_id = $3",
].join(" ");
const READ_OPERATIONS_PAGE_FIRST_QUERY = [
  `SELECT ${OPERATION_COLUMNS}`,
  "FROM session_authority.filesystem_image_provider_operations",
  "WHERE provider_id = $1 AND anchor_id = $2",
  'ORDER BY operation_id COLLATE pg_catalog."C"',
  "LIMIT $3::pg_catalog.int4",
].join(" ");
const READ_OPERATIONS_PAGE_AFTER_QUERY = [
  `SELECT ${OPERATION_COLUMNS}`,
  "FROM session_authority.filesystem_image_provider_operations",
  "WHERE provider_id = $1 AND anchor_id = $2",
  'AND operation_id COLLATE pg_catalog."C" > $3 COLLATE pg_catalog."C"',
  'ORDER BY operation_id COLLATE pg_catalog."C"',
  "LIMIT $4::pg_catalog.int4",
].join(" ");
const INSERT_PREPARED_QUERY = [
  "INSERT INTO session_authority.filesystem_image_provider_operations",
  "(provider_id, anchor_id, operation_id, record_contract_version, state, kind,",
  "storage_id, prepared_state_revision, prepared_checksum,",
  "prepared_record_bytes, prepared_record_sha256)",
  "VALUES ($1, $2, $3, $4, $5, $6, $7, $8::pg_catalog.numeric, $9,",
  "pg_catalog.decode($10, 'hex'), $11)",
  "ON CONFLICT DO NOTHING",
  `RETURNING ${OPERATION_COLUMNS}`,
].join(" ");
const UPDATE_COMMITTED_QUERY = [
  "UPDATE session_authority.filesystem_image_provider_operations",
  "SET state = $12, committed_state_revision = $13::pg_catalog.numeric,",
  "committed_checksum_provenance = $14, committed_checksum = $15,",
  "committed_record_bytes = pg_catalog.decode($16, 'hex'),",
  "committed_record_sha256 = $17",
  "WHERE provider_id = $1 AND anchor_id = $2 AND operation_id = $3",
  "AND record_contract_version = $4 AND state = $5 AND kind = $6",
  "AND storage_id = $7 AND prepared_state_revision = $8::pg_catalog.numeric",
  "AND prepared_checksum = $9",
  "AND prepared_record_bytes = pg_catalog.decode($10, 'hex')",
  "AND prepared_record_sha256 = $11",
  "AND committed_state_revision IS NULL",
  "AND committed_checksum_provenance IS NULL AND committed_checksum IS NULL",
  "AND committed_record_bytes IS NULL AND committed_record_sha256 IS NULL",
  `RETURNING ${OPERATION_COLUMNS}`,
].join(" ");

const reflectApplyIntrinsic = Reflect.apply;
const reflectOwnKeysIntrinsic = Reflect.ownKeys;
const arrayEveryIntrinsic = Array.prototype.every;
const arrayIncludesIntrinsic = Array.prototype.includes;
const arrayIsArrayIntrinsic = Array.isArray;
const arrayJoinIntrinsic = Array.prototype.join;
const arrayPushIntrinsic = Array.prototype.push;
const arraySliceIntrinsic = Array.prototype.slice;
const arraySortIntrinsic = Array.prototype.sort;
const arrayPrototype = Array.prototype;
const ArrayConstructor = Array;
const BigIntConstructor = BigInt;
const bufferByteLengthIntrinsic = Buffer.byteLength;
const bufferEqualsIntrinsic = Buffer.prototype.equals;
const bufferFromIntrinsic = Buffer.from;
const bufferIsBufferIntrinsic = Buffer.isBuffer;
const bufferToStringIntrinsic = Buffer.prototype.toString;
const BufferConstructor = Buffer;
const createHashIntrinsic = createHash;
const hashDigestIntrinsic = Hash.prototype.digest;
const hashUpdateIntrinsic = Hash.prototype.update;
const isProxyValue = utilTypes.isProxy;
const jsonParseIntrinsic = JSON.parse;
const jsonStringifyIntrinsic = JSON.stringify;
const mathMaxIntrinsic = Math.max;
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
const ObjectConstructor = Object;
const pathBasenameIntrinsic = basename;
const pathDirnameIntrinsic = dirname;
const pathIsAbsoluteIntrinsic = isAbsolute;
const pathParseIntrinsic = parse;
const pathResolveIntrinsic = resolve;
const regexpTestIntrinsic = RegExp.prototype.test;
const runSerializableIntrinsic =
  PostgresSerializableStore.prototype.runSerializable;
const setAddIntrinsic = Set.prototype.add;
const setHasIntrinsic = Set.prototype.has;
const SetConstructor = Set;
const StringConstructor = String;
const stringIncludesIntrinsic = String.prototype.includes;
const TypeErrorConstructor = TypeError;

function callIntrinsic(intrinsic, receiver, args) {
  return reflectApplyIntrinsic(intrinsic, receiver, args);
}

function arrayEvery(value, predicate) {
  return callIntrinsic(arrayEveryIntrinsic, value, [predicate]);
}

function arrayIncludes(value, candidate) {
  return callIntrinsic(arrayIncludesIntrinsic, value, [candidate]);
}

function arrayIsArray(value) {
  return callIntrinsic(arrayIsArrayIntrinsic, ArrayConstructor, [value]);
}

function arrayPush(value, candidate) {
  return callIntrinsic(arrayPushIntrinsic, value, [candidate]);
}

function bufferByteLength(value, encoding) {
  return callIntrinsic(bufferByteLengthIntrinsic, BufferConstructor, [
    value,
    encoding,
  ]);
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

function bufferIsBuffer(value) {
  return callIntrinsic(bufferIsBufferIntrinsic, BufferConstructor, [value]);
}

function bufferToString(value, encoding) {
  return callIntrinsic(bufferToStringIntrinsic, value, [encoding]);
}

function objectCreate(prototype) {
  return callIntrinsic(objectCreateIntrinsic, ObjectConstructor, [prototype]);
}

function objectDefineProperty(value, key, descriptor) {
  return callIntrinsic(objectDefinePropertyIntrinsic, ObjectConstructor, [
    value,
    key,
    descriptor,
  ]);
}

function objectFreeze(value) {
  return callIntrinsic(objectFreezeIntrinsic, ObjectConstructor, [value]);
}

function objectGetOwnPropertyDescriptor(value, key) {
  return callIntrinsic(
    objectGetOwnPropertyDescriptorIntrinsic,
    ObjectConstructor,
    [value, key],
  );
}

function objectGetPrototypeOf(value) {
  return callIntrinsic(objectGetPrototypeOfIntrinsic, ObjectConstructor, [
    value,
  ]);
}

function objectHasOwn(value, key) {
  return callIntrinsic(objectHasOwnIntrinsic, ObjectConstructor, [value, key]);
}

function reflectOwnKeys(value) {
  return callIntrinsic(reflectOwnKeysIntrinsic, undefined, [value]);
}

function regexpTest(pattern, value) {
  return callIntrinsic(regexpTestIntrinsic, pattern, [value]);
}

function fail(code) {
  throw new PostgresFilesystemImageProviderStateAuthorityError(code);
}

function ensure(condition, code) {
  if (!condition) fail(code);
}

function inspectPlainObject(value, code) {
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

function exactDataObject(value, expectedKeys, code) {
  const keys = inspectPlainObject(value, code);
  ensure(
    keys.length === expectedKeys.length &&
      arrayEvery(keys, (key) => arrayIncludes(expectedKeys, key)),
    code,
  );
  const normalized = objectCreate(null);
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
    normalized[key] = ownDataValue(value, key, code);
  }
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

function assertLosslessString(value, code, maximum = MAX_CANONICAL_BYTES) {
  ensure(
    typeof value === "string" &&
      numberIsSafeIntegerIntrinsic(maximum) &&
      maximum >= 0 &&
      value.length <= maximum,
    code,
  );
  const encoded = bufferFrom(value, "utf8");
  ensure(
    encoded.length <= maximum && bufferToString(encoded, "utf8") === value,
    code,
  );
  return value;
}

function assertCanonicalArrayPrecursorCapacity(length, state, code) {
  ensure(numberIsSafeIntegerIntrinsic(length) && length >= 0, code);
  const minimumBytes = length === 0 ? 2 : 2 * length + 1;
  ensure(
    length <= MAX_CANONICAL_NODES - state.budget.nodes &&
      state.budget.bytes <= MAX_CANONICAL_BYTES - minimumBytes,
    code,
  );
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
    const normalized = callIntrinsic(objectIsIntrinsic, ObjectConstructor, [
      value,
      -0,
    ])
      ? 0
      : value;
    consumeBudget(
      state,
      bufferByteLength(jsonStringifyIntrinsic(normalized), "utf8"),
      code,
    );
    return normalized;
  }
  if (typeof value === "string") {
    assertLosslessString(value, code, MAX_CANONICAL_BYTES - state.budget.bytes);
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
  if (arrayIsArray(value)) {
    const length = value.length;
    // Bound recursive work and the own-key/Set precursor before enumerating an
    // untrusted dense array. Every canonical element consumes at least one
    // node and one JSON byte in addition to the array punctuation.
    assertCanonicalArrayPrecursorCapacity(length, state, code);
    let keys;
    try {
      keys = reflectOwnKeys(value);
    } catch {
      fail(code);
    }
    ensure(
      keys.length === length + 1,
      code,
    );
    const keySet = new SetConstructor();
    for (let index = 0; index < keys.length; index += 1) {
      callIntrinsic(setAddIntrinsic, keySet, [keys[index]]);
    }
    ensure(callIntrinsic(setHasIntrinsic, keySet, ["length"]), code);
    consumeBudget(state, 2 + mathMaxIntrinsic(0, length - 1), code);
    const result = [];
    for (let index = 0; index < length; index += 1) {
      const key = StringConstructor(index);
      ensure(callIntrinsic(setHasIntrinsic, keySet, [key]), code);
      const descriptor = objectGetOwnPropertyDescriptor(value, key);
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
  callIntrinsic(arraySortIntrinsic, sortedKeys, []);
  for (let index = 0; index < sortedKeys.length; index += 1) {
    const key = sortedKeys[index];
    assertLosslessString(key, code, MAX_CANONICAL_BYTES - state.budget.bytes);
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
  if (arrayIsArray(value)) {
    const items = [];
    for (let index = 0; index < value.length; index += 1) {
      arrayPush(items, canonicalString(value[index]));
    }
    return `[${callIntrinsic(arrayJoinIntrinsic, items, [","])}]`;
  }
  const fields = [];
  const keys = reflectOwnKeys(value);
  callIntrinsic(arraySortIntrinsic, keys, []);
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
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
  ensure(
    typeof value === "string" && regexpTest(OPAQUE_ID_PATTERN, value),
    code,
  );
  return value;
}

function canonicalChecksum(value, code) {
  ensure(
    typeof value === "string" && regexpTest(SHA256_PATTERN, value),
    code,
  );
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

function canonicalAbsolutePath(value, code) {
  assertLosslessString(value, code, MAX_PATH_BYTES);
  ensure(
    !callIntrinsic(stringIncludesIntrinsic, value, ["\0"]) &&
      callIntrinsic(pathIsAbsoluteIntrinsic, undefined, [value]) &&
      callIntrinsic(pathResolveIntrinsic, undefined, [value]) === value &&
      value !== callIntrinsic(pathParseIntrinsic, undefined, [value]).root,
    code,
  );
  return value;
}

function canonicalPhysicalIdentity(value, code) {
  const identity = exactDataObject(
    value,
    ["filesystemId", "objectIdentityScheme", "objectId"],
    code,
  );
  const objectId = assertLosslessString(
    identity.objectId,
    code,
    MAX_PHYSICAL_OBJECT_ID_BYTES,
  );
  ensure(
    objectId.length > 0 &&
      !callIntrinsic(stringIncludesIntrinsic, objectId, ["\0"]),
    code,
  );
  return objectFreeze({
    filesystemId: canonicalOpaqueId(identity.filesystemId, code),
    objectIdentityScheme: canonicalOpaqueId(
      identity.objectIdentityScheme,
      code,
    ),
    objectId,
  });
}

function canonicalMount(value, code) {
  const mount = exactDataObject(
    value,
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

const STORAGE_STATE_KEYS = Object.freeze([
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
  const state = exactDataObject(value, STORAGE_STATE_KEYS, code);
  ensure(
    typeof state.lifecycle === "string" &&
      arrayIncludes(LIFECYCLES, state.lifecycle),
    code,
  );
  const revision = canonicalUint64(state.revision, code, {
    positive: true,
  }).value;
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
    state.attachment === null
      ? null
      : canonicalAttachment(state.attachment, code);
  ensure(state.lifecycle === "destroyed" ? mount === null : mount !== null, code);
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
    ensure(mount.rootIdentity.filesystemId === state.filesystemId, code);
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
        callIntrinsic(pathDirnameIntrinsic, undefined, [dataRoot.rootPath]) ===
          mount.mountPath &&
        !arrayIncludes(
          ["", ".", ".."],
          callIntrinsic(pathBasenameIntrinsic, undefined, [dataRoot.rootPath]),
        ) &&
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

function canonicalExpectedStorage(value, code) {
  if (value === null) return null;
  const expected = exactDataObject(value, ["lifecycle", "revision"], code);
  ensure(
    typeof expected.lifecycle === "string" &&
      arrayIncludes(LIFECYCLES, expected.lifecycle),
    code,
  );
  return objectFreeze({
    lifecycle: expected.lifecycle,
    revision: canonicalUint64(expected.revision, code, { positive: true })
      .value,
  });
}

function expectedStorageState(value) {
  return value === null
    ? null
    : objectFreeze({ lifecycle: value.lifecycle, revision: value.revision });
}

function incrementUint64(value, code) {
  const parsed = canonicalUint64(value, code, { positive: true }).parsed;
  ensure(parsed < MAX_UINT64, code);
  return StringConstructor(parsed + 1n);
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

function assertStorageTransition(previous, next, kind, code) {
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
    ensure(canonicalEqual(previous.attachment, next.attachment), code);
    ensure(canonicalEqual(previous.dataRoot, next.dataRoot), code);
    return;
  }
  fail(code);
}

function normalizeOperationRecord(value, code) {
  const keys = inspectPlainObject(value, code);
  ensure(arrayIncludes(keys, "state"), code);
  const state = ownDataValue(value, "state", code);
  ensure(state === "prepared" || state === "committed", code);
  const record = exactDataObject(
    value,
    state === "prepared" ? PREPARED_RECORD_KEYS : COMMITTED_RECORD_KEYS,
    code,
  );
  ensure(
    typeof record.kind === "string" &&
      arrayIncludes(OPERATION_KINDS, record.kind),
    code,
  );
  const prepared = {
    kind: record.kind,
    operationId: canonicalOpaqueId(record.operationId, code),
    preparedChecksum: canonicalChecksum(record.preparedChecksum, code),
    preparedStateRevision: canonicalUint64(
      record.preparedStateRevision,
      code,
      { positive: true },
    ).value,
    request: canonicalObject(record.request, code),
    state: "prepared",
    storageId: canonicalOpaqueId(record.storageId, code),
    storageStateBefore:
      record.storageStateBefore === null
        ? null
        : canonicalStorageState(record.storageStateBefore, code),
  };
  ensure(
    prepared.storageStateBefore === null ||
      prepared.storageStateBefore.storageId === prepared.storageId,
    code,
  );
  assertPreparePrecondition(prepared.storageStateBefore, prepared.kind, code);
  if (state === "prepared") return objectFreeze(prepared);
  const committedStateRevision = canonicalUint64(
    record.committedStateRevision,
    code,
    { positive: true },
  ).value;
  ensure(
    BigIntConstructor(committedStateRevision) >
      BigIntConstructor(prepared.preparedStateRevision),
    code,
  );
  const expectedStorage = canonicalExpectedStorage(record.expectedStorage, code);
  const storageState = canonicalStorageState(record.storageState, code);
  ensure(
    canonicalEqual(
      expectedStorage,
      expectedStorageState(prepared.storageStateBefore),
    ) && storageState.storageId === prepared.storageId,
    code,
  );
  assertStorageTransition(
    prepared.storageStateBefore,
    storageState,
    prepared.kind,
    code,
  );
  return objectFreeze({
    ...prepared,
    state: "committed",
    committedStateRevision,
    expectedStorage,
    result: canonicalObject(record.result, code),
    storageState,
  });
}

function recordMaterial(record, code) {
  const normalized = normalizeOperationRecord(record, code);
  const bytes = bufferFrom(canonicalString(normalized), "utf8");
  ensure(bytes.length >= 1 && bytes.length <= MAX_OPERATION_RECORD_BYTES, code);
  const hash = createHashIntrinsic("sha256");
  callIntrinsic(hashUpdateIntrinsic, hash, [OPERATION_RECORD_DOMAIN]);
  callIntrinsic(hashUpdateIntrinsic, hash, [bytes]);
  const sha256 = callIntrinsic(hashDigestIntrinsic, hash, ["hex"]);
  return objectFreeze({ bytes, record: normalized, sha256 });
}

function parseRecordBytes(bytesValue, expectedState, expectedSha256, code) {
  ensure(
    bufferIsBuffer(bytesValue) &&
      bytesValue.length >= 1 &&
      bytesValue.length <= MAX_OPERATION_RECORD_BYTES,
    code,
  );
  const bytes = bufferFrom(bytesValue);
  const text = bufferToString(bytes, "utf8");
  ensure(bufferEquals(bufferFrom(text, "utf8"), bytes), code);
  let parsed;
  try {
    parsed = jsonParseIntrinsic(text);
  } catch {
    fail(code);
  }
  const material = recordMaterial(parsed, code);
  ensure(
    material.record.state === expectedState &&
      bufferEquals(material.bytes, bytes) &&
      material.sha256 === expectedSha256,
    code,
  );
  return material;
}

function canonicalHead(value, code) {
  try {
    return callIntrinsic(normalizeFilesystemImageProviderStateHead, undefined, [
      value,
    ]);
  } catch {
    fail(code);
  }
}

function canonicalHeadChecksum(value, code) {
  try {
    return callIntrinsic(filesystemImageProviderStateHeadChecksum, undefined, [
      value,
    ]);
  } catch {
    fail(code);
  }
}

function genesisHead() {
  return canonicalHead(
    {
      contractVersion: FILESYSTEM_IMAGE_PROVIDER_STATE_HEAD_CONTRACT_VERSION,
      anchorRevision: "0",
      generation: "0",
      stateRevision: "0",
      baseHeadChecksum: null,
      checkpointStateRevision: "0",
      checkpointFrameCount: 0,
      checkpointChecksum: null,
      checkpointBytes: 0,
      frameCount: 0,
      lastChecksum: null,
      ledgerBytes: 0,
    },
    "postgres_filesystem_image_provider_state_authority_state_invalid",
  );
}

function headEqual(left, right) {
  return (
    left.contractVersion === right.contractVersion &&
    left.anchorRevision === right.anchorRevision &&
    left.generation === right.generation &&
    left.stateRevision === right.stateRevision &&
    left.baseHeadChecksum === right.baseHeadChecksum &&
    left.checkpointStateRevision === right.checkpointStateRevision &&
    left.checkpointFrameCount === right.checkpointFrameCount &&
    left.checkpointChecksum === right.checkpointChecksum &&
    left.checkpointBytes === right.checkpointBytes &&
    left.frameCount === right.frameCount &&
    left.lastChecksum === right.lastChecksum &&
    left.ledgerBytes === right.ledgerBytes
  );
}

function canonicalStoredNumber(value, maximum, code) {
  ensure(typeof value === "string" && regexpTest(DECIMAL_PATTERN, value), code);
  let parsed;
  try {
    parsed = BigIntConstructor(value);
  } catch {
    fail(code);
  }
  ensure(parsed <= BigIntConstructor(maximum), code);
  const number = NumberConstructor(parsed);
  ensure(numberIsSafeIntegerIntrinsic(number), code);
  return number;
}

function decimalSuccessor(previous, next) {
  return BigIntConstructor(next) === BigIntConstructor(previous) + 1n;
}

function isNormalAppend(expectedHead, nextHead) {
  return (
    nextHead.contractVersion === expectedHead.contractVersion &&
    decimalSuccessor(expectedHead.anchorRevision, nextHead.anchorRevision) &&
    nextHead.generation === expectedHead.generation &&
    decimalSuccessor(expectedHead.stateRevision, nextHead.stateRevision) &&
    nextHead.baseHeadChecksum === expectedHead.baseHeadChecksum &&
    nextHead.checkpointStateRevision === expectedHead.checkpointStateRevision &&
    nextHead.checkpointFrameCount === expectedHead.checkpointFrameCount &&
    nextHead.checkpointChecksum === expectedHead.checkpointChecksum &&
    nextHead.checkpointBytes === expectedHead.checkpointBytes &&
    nextHead.frameCount === expectedHead.frameCount + 1 &&
    nextHead.frameCount <= MAX_FRAME_COUNT &&
    nextHead.lastChecksum !== null &&
    nextHead.ledgerBytes > expectedHead.ledgerBytes
  );
}

function isPureRotation(expectedHead, nextHead, code) {
  if (
    nextHead.contractVersion !== expectedHead.contractVersion ||
    !decimalSuccessor(expectedHead.anchorRevision, nextHead.anchorRevision) ||
    !decimalSuccessor(expectedHead.generation, nextHead.generation) ||
    nextHead.stateRevision !== expectedHead.stateRevision ||
    expectedHead.frameCount === 0 ||
    expectedHead.ledgerBytes === 0 ||
    nextHead.checkpointStateRevision !== expectedHead.stateRevision ||
    nextHead.checkpointFrameCount < 2 ||
    nextHead.checkpointFrameCount > MAX_CHECKPOINT_FRAME_COUNT ||
    nextHead.checkpointChecksum === null ||
    nextHead.checkpointBytes <= 0 ||
    nextHead.checkpointBytes > MAX_CHECKPOINT_BYTES ||
    nextHead.frameCount !== 0 ||
    nextHead.lastChecksum !== nextHead.checkpointChecksum ||
    nextHead.ledgerBytes !== 0
  ) {
    return false;
  }
  return nextHead.baseHeadChecksum === canonicalHeadChecksum(expectedHead, code);
}

function rowsFromResult(result, command, maximumRows, code) {
  const observedCommand = ownDataValue(result, "command", code);
  const rowCount = ownDataValue(result, "rowCount", code);
  const rows = ownDataValue(result, "rows", code);
  ensure(
    observedCommand === command &&
      numberIsSafeIntegerIntrinsic(rowCount) &&
      rowCount >= 0 &&
      rowCount <= maximumRows &&
      !isProxyValue(rows) &&
      arrayIsArray(rows) &&
      objectGetPrototypeOf(rows) === arrayPrototype &&
      rows.length === rowCount,
    code,
  );
  for (let index = 0; index < rows.length; index += 1) {
    ownDataValue(rows, StringConstructor(index), code);
  }
  return rows;
}

function normalizeHeadRow(value, identity, code) {
  const row = exactDataObject(value, HEAD_ROW_KEYS, code);
  ensure(
    canonicalOpaqueId(row.provider_id, code) === identity.providerId &&
      canonicalOpaqueId(row.anchor_id, code) === identity.anchorId,
    code,
  );
  const head = canonicalHead(
    {
      contractVersion: row.contract_version,
      anchorRevision: row.anchor_revision,
      generation: row.generation,
      stateRevision: row.state_revision,
      baseHeadChecksum: row.base_head_checksum,
      checkpointStateRevision: row.checkpoint_state_revision,
      checkpointFrameCount: canonicalStoredNumber(
        row.checkpoint_frame_count,
        MAX_CHECKPOINT_FRAME_COUNT,
        code,
      ),
      checkpointChecksum: row.checkpoint_checksum,
      checkpointBytes: canonicalStoredNumber(
        row.checkpoint_bytes,
        MAX_CHECKPOINT_BYTES,
        code,
      ),
      frameCount: canonicalStoredNumber(
        row.frame_count,
        MAX_FRAME_COUNT,
        code,
      ),
      lastChecksum: row.last_checksum,
      ledgerBytes: canonicalStoredNumber(
        row.ledger_bytes,
        MAX_LEDGER_BYTES,
        code,
      ),
    },
    code,
  );
  ensure(!headEqual(head, genesisHead()), code);
  return head;
}

function normalizeOperationRow(value, identity, code) {
  const row = exactDataObject(value, OPERATION_ROW_KEYS, code);
  ensure(
    canonicalOpaqueId(row.provider_id, code) === identity.providerId &&
      canonicalOpaqueId(row.anchor_id, code) === identity.anchorId &&
      row.record_contract_version ===
        POSTGRES_FILESYSTEM_IMAGE_PROVIDER_STATE_AUTHORITY_CONTRACT_VERSION &&
      (row.state === "prepared" || row.state === "committed"),
    code,
  );
  const operationId = canonicalOpaqueId(row.operation_id, code);
  const kind = canonicalOpaqueId(row.kind, code);
  const storageId = canonicalOpaqueId(row.storage_id, code);
  const preparedStateRevision = canonicalUint64(
    row.prepared_state_revision,
    code,
    { positive: true },
  ).value;
  const preparedChecksum = canonicalChecksum(row.prepared_checksum, code);
  const preparedSha256 = canonicalChecksum(row.prepared_record_sha256, code);
  const preparedMaterial = parseRecordBytes(
    row.prepared_record_bytes,
    "prepared",
    preparedSha256,
    code,
  );
  ensure(
    preparedMaterial.record.operationId === operationId &&
      preparedMaterial.record.kind === kind &&
      preparedMaterial.record.storageId === storageId &&
      preparedMaterial.record.preparedStateRevision === preparedStateRevision &&
      preparedMaterial.record.preparedChecksum === preparedChecksum,
    code,
  );
  if (row.state === "prepared") {
    ensure(
      row.committed_state_revision === null &&
        row.committed_checksum_provenance === null &&
        row.committed_checksum === null &&
        row.committed_record_bytes === null &&
        row.committed_record_sha256 === null,
      code,
    );
    return preparedMaterial;
  }
  const committedStateRevision = canonicalUint64(
    row.committed_state_revision,
    code,
    { positive: true },
  ).value;
  ensure(
    row.committed_checksum_provenance === "indexed-frame-v1" ||
      row.committed_checksum_provenance === "unavailable-adopted-v2",
    code,
  );
  const committedChecksumProvenance = row.committed_checksum_provenance;
  let committedChecksum = null;
  if (committedChecksumProvenance === "indexed-frame-v1") {
    committedChecksum = canonicalChecksum(row.committed_checksum, code);
  } else {
    ensure(row.committed_checksum === null, code);
  }
  const committedSha256 = canonicalChecksum(row.committed_record_sha256, code);
  const committedMaterial = parseRecordBytes(
    row.committed_record_bytes,
    "committed",
    committedSha256,
    code,
  );
  const committed = committedMaterial.record;
  ensure(
    committed.operationId === operationId &&
      committed.kind === kind &&
      committed.storageId === storageId &&
      committed.preparedStateRevision === preparedStateRevision &&
      committed.preparedChecksum === preparedChecksum &&
      committed.committedStateRevision === committedStateRevision &&
      canonicalEqual(committed.request, preparedMaterial.record.request) &&
      canonicalEqual(
        committed.storageStateBefore,
        preparedMaterial.record.storageStateBefore,
      ),
    code,
  );
  return objectFreeze({
    ...committedMaterial,
    committedChecksum,
    committedChecksumProvenance,
    preparedMaterial,
  });
}

function assertOperationVisibleAtHead(material, head, code) {
  const record = material.record;
  const headRevision = BigIntConstructor(head.stateRevision);
  const preparedRevision = BigIntConstructor(record.preparedStateRevision);
  const headEndsInOperationFrame =
    head.generation === "0" || head.frameCount > 0;
  ensure(preparedRevision <= headRevision, code);
  if (preparedRevision === headRevision && headEndsInOperationFrame) {
    ensure(record.preparedChecksum === head.lastChecksum, code);
  }
  if (record.state === "committed") {
    const committedRevision = BigIntConstructor(record.committedStateRevision);
    ensure(committedRevision <= headRevision, code);
    if (
      material.committedChecksumProvenance === "unavailable-adopted-v2"
    ) {
      ensure(
        head.contractVersion === 3 &&
          committedRevision <=
            BigIntConstructor(head.checkpointStateRevision),
        code,
      );
    }
    if (committedRevision === headRevision && headEndsInOperationFrame) {
      ensure(
        material.committedChecksumProvenance === "indexed-frame-v1" &&
          material.committedChecksum === head.lastChecksum,
        code,
      );
    }
  }
}

function queryTransaction(transaction, text, values, code) {
  const query = ownDataValue(transaction, "query", code);
  ensure(typeof query === "function" && !isProxyValue(query), code);
  return callIntrinsic(query, undefined, [text, values]);
}

function runSerializable(store, callback) {
  return callIntrinsic(runSerializableIntrinsic, store, [callback]);
}

function headValues(identity, head) {
  return [
    identity.providerId,
    identity.anchorId,
    head.contractVersion,
    head.anchorRevision,
    head.generation,
    head.stateRevision,
    head.baseHeadChecksum,
    head.checkpointStateRevision,
    StringConstructor(head.checkpointFrameCount),
    head.checkpointChecksum,
    StringConstructor(head.checkpointBytes),
    StringConstructor(head.frameCount),
    head.lastChecksum,
    StringConstructor(head.ledgerBytes),
  ];
}

function expectedHeadValues(identity, expectedHead, nextHead) {
  const nextValues = headValues(identity, nextHead);
  if (headEqual(expectedHead, genesisHead())) return nextValues;
  return [
    ...nextValues,
    expectedHead.contractVersion,
    expectedHead.anchorRevision,
    expectedHead.generation,
    expectedHead.stateRevision,
    expectedHead.baseHeadChecksum,
    expectedHead.checkpointStateRevision,
    StringConstructor(expectedHead.checkpointFrameCount),
    expectedHead.checkpointChecksum,
    StringConstructor(expectedHead.checkpointBytes),
    StringConstructor(expectedHead.frameCount),
    expectedHead.lastChecksum,
    StringConstructor(expectedHead.ledgerBytes),
  ];
}

async function readHeadInTransaction(transaction, identity, code) {
  const result = await queryTransaction(
    transaction,
    READ_HEAD_QUERY,
    [identity.providerId, identity.anchorId],
    code,
  );
  const rows = rowsFromResult(result, "SELECT", 1, code);
  return rows.length === 0 ? genesisHead() : normalizeHeadRow(rows[0], identity, code);
}

async function requireExpectedHead(transaction, identity, expectedHead, code) {
  const observed = await readHeadInTransaction(transaction, identity, code);
  ensure(headEqual(observed, expectedHead), code);
}

async function compareHeadInTransaction(
  transaction,
  identity,
  expectedHead,
  nextHead,
  code,
) {
  const genesis = headEqual(expectedHead, genesisHead());
  const result = await queryTransaction(
    transaction,
    genesis ? INSERT_HEAD_QUERY : UPDATE_HEAD_QUERY,
    expectedHeadValues(identity, expectedHead, nextHead),
    code,
  );
  const rows = rowsFromResult(result, genesis ? "INSERT" : "UPDATE", 1, code);
  if (rows.length === 0) return false;
  const stored = normalizeHeadRow(rows[0], identity, code);
  ensure(headEqual(stored, nextHead), code);
  return true;
}

async function readOperationInTransaction(
  transaction,
  identity,
  operationId,
  expectedHead,
  code,
) {
  const result = await queryTransaction(
    transaction,
    READ_OPERATION_QUERY,
    [identity.providerId, identity.anchorId, operationId],
    code,
  );
  const rows = rowsFromResult(result, "SELECT", 1, code);
  if (rows.length === 0) return null;
  const material = normalizeOperationRow(rows[0], identity, code);
  assertOperationVisibleAtHead(material, expectedHead, code);
  return material.record;
}

async function readOperationsPageInTransaction(
  transaction,
  identity,
  input,
  code,
) {
  const maximumRows = input.limit + 1;
  const first = input.afterOperationId === null;
  const result = await queryTransaction(
    transaction,
    first ? READ_OPERATIONS_PAGE_FIRST_QUERY : READ_OPERATIONS_PAGE_AFTER_QUERY,
    first
      ? [identity.providerId, identity.anchorId, StringConstructor(maximumRows)]
      : [
          identity.providerId,
          identity.anchorId,
          input.afterOperationId,
          StringConstructor(maximumRows),
        ],
    code,
  );
  const rows = rowsFromResult(result, "SELECT", maximumRows, code);
  const normalized = [];
  let previousOperationId = input.afterOperationId;
  for (let index = 0; index < rows.length; index += 1) {
    const material = normalizeOperationRow(rows[index], identity, code);
    assertOperationVisibleAtHead(material, input.expectedHead, code);
    const record = material.record;
    ensure(
      previousOperationId === null || record.operationId > previousOperationId,
      code,
    );
    previousOperationId = record.operationId;
    arrayPush(normalized, record);
  }
  const hasMore = normalized.length > input.limit;
  const operations = callIntrinsic(arraySliceIntrinsic, normalized, [
    0,
    input.limit,
  ]);
  objectFreeze(operations);
  return objectFreeze({
    operations,
    nextAfterOperationId:
      hasMore && operations.length > 0
        ? operations[operations.length - 1].operationId
        : null,
  });
}

function preparedInsertValues(identity, material) {
  const record = material.record;
  return [
    identity.providerId,
    identity.anchorId,
    record.operationId,
    POSTGRES_FILESYSTEM_IMAGE_PROVIDER_STATE_AUTHORITY_CONTRACT_VERSION,
    "prepared",
    record.kind,
    record.storageId,
    record.preparedStateRevision,
    record.preparedChecksum,
    bufferToString(material.bytes, "hex"),
    material.sha256,
  ];
}

async function insertPreparedInTransaction(
  transaction,
  identity,
  material,
  code,
) {
  const result = await queryTransaction(
    transaction,
    INSERT_PREPARED_QUERY,
    preparedInsertValues(identity, material),
    code,
  );
  const rows = rowsFromResult(result, "INSERT", 1, code);
  ensure(rows.length === 1, code);
  const stored = normalizeOperationRow(rows[0], identity, code);
  ensure(
    stored.record.state === "prepared" &&
      bufferEquals(stored.bytes, material.bytes) &&
      stored.sha256 === material.sha256,
    code,
  );
}

async function updateCommittedInTransaction(
  transaction,
  identity,
  material,
  frameChecksum,
  code,
) {
  const record = material.record;
  const preparedRecord = objectFreeze({
    kind: record.kind,
    operationId: record.operationId,
    preparedChecksum: record.preparedChecksum,
    preparedStateRevision: record.preparedStateRevision,
    request: record.request,
    state: "prepared",
    storageId: record.storageId,
    storageStateBefore: record.storageStateBefore,
  });
  const preparedMaterial = recordMaterial(preparedRecord, code);
  const values = [
    identity.providerId,
    identity.anchorId,
    record.operationId,
    POSTGRES_FILESYSTEM_IMAGE_PROVIDER_STATE_AUTHORITY_CONTRACT_VERSION,
    "prepared",
    record.kind,
    record.storageId,
    record.preparedStateRevision,
    record.preparedChecksum,
    bufferToString(preparedMaterial.bytes, "hex"),
    preparedMaterial.sha256,
    "committed",
    record.committedStateRevision,
    "indexed-frame-v1",
    frameChecksum,
    bufferToString(material.bytes, "hex"),
    material.sha256,
  ];
  const result = await queryTransaction(
    transaction,
    UPDATE_COMMITTED_QUERY,
    values,
    code,
  );
  const rows = rowsFromResult(result, "UPDATE", 1, code);
  ensure(rows.length === 1, code);
  const stored = normalizeOperationRow(rows[0], identity, code);
  ensure(
    stored.record.state === "committed" &&
      stored.committedChecksumProvenance === "indexed-frame-v1" &&
      stored.committedChecksum === frameChecksum &&
      bufferEquals(stored.bytes, material.bytes) &&
      stored.sha256 === material.sha256,
    code,
  );
}

function normalizeTransition(value, expectedHead, nextHead, code) {
  const keys = inspectPlainObject(value, code);
  ensure(arrayIncludes(keys, "type"), code);
  const type = ownDataValue(value, "type", code);
  if (type === "rotate-v1") {
    const transition = exactDataObject(
      value,
      ["contractVersion", "type"],
      code,
    );
    ensure(
      transition.contractVersion ===
        POSTGRES_FILESYSTEM_IMAGE_PROVIDER_STATE_AUTHORITY_CONTRACT_VERSION &&
        isPureRotation(expectedHead, nextHead, code),
      code,
    );
    return objectFreeze({
      contractVersion:
        POSTGRES_FILESYSTEM_IMAGE_PROVIDER_STATE_AUTHORITY_CONTRACT_VERSION,
      type,
    });
  }
  ensure(type === "append-prepared-v1" || type === "append-committed-v1", code);
  const transition = exactDataObject(
    value,
    ["contractVersion", "type", "frameChecksum", "record"],
    code,
  );
  const frameChecksum = canonicalChecksum(transition.frameChecksum, code);
  const material = recordMaterial(transition.record, code);
  const expectedState =
    type === "append-prepared-v1" ? "prepared" : "committed";
  ensure(
    transition.contractVersion ===
      POSTGRES_FILESYSTEM_IMAGE_PROVIDER_STATE_AUTHORITY_CONTRACT_VERSION &&
      material.record.state === expectedState &&
      isNormalAppend(expectedHead, nextHead) &&
      frameChecksum === nextHead.lastChecksum &&
      (expectedState === "prepared"
        ? material.record.preparedStateRevision === nextHead.stateRevision &&
          material.record.preparedChecksum === frameChecksum
        : material.record.committedStateRevision === nextHead.stateRevision),
    code,
  );
  return objectFreeze({
    contractVersion:
      POSTGRES_FILESYSTEM_IMAGE_PROVIDER_STATE_AUTHORITY_CONTRACT_VERSION,
    type,
    frameChecksum,
    material,
  });
}

export class PostgresFilesystemImageProviderStateAuthorityError extends Error {
  constructor(code) {
    if (typeof code !== "string" || !objectHasOwn(ERROR_MESSAGES, code)) {
      throw new TypeErrorConstructor(
        "Unsupported PostgreSQL filesystem image provider state authority error",
      );
    }
    super(ERROR_MESSAGES[code]);
    objectDefineProperty(this, "name", {
      enumerable: false,
      value: "PostgresFilesystemImageProviderStateAuthorityError",
    });
    objectDefineProperty(this, "code", { enumerable: true, value: code });
    objectDefineProperty(this, "retryable", {
      enumerable: true,
      value: false,
    });
    objectFreeze(this);
  }
}

export function createPostgresFilesystemImageProviderStateAuthority(...args) {
  const optionCode =
    "invalid_postgres_filesystem_image_provider_state_authority_options";
  ensure(args.length === 1, optionCode);
  const options = exactDataObject(args[0], OPTION_KEYS, optionCode);
  ensure(
    callIntrinsic(isPostgresSerializableStore, undefined, [options.store]) ===
      true,
    optionCode,
  );
  const store = options.store;
  const identity = objectFreeze({
    providerId: canonicalOpaqueId(options.providerId, optionCode),
    anchorId: canonicalOpaqueId(options.anchorId, optionCode),
  });
  const requestCode =
    "invalid_postgres_filesystem_image_provider_state_authority_request";
  const stateCode =
    "postgres_filesystem_image_provider_state_authority_state_invalid";

  const readHead = async function readHead(...readArgs) {
    ensure(readArgs.length === 0, requestCode);
    return await runSerializable(store, async (transaction) =>
      await readHeadInTransaction(transaction, identity, stateCode),
    );
  };

  const readOperation = async function readOperation(...readArgs) {
    ensure(readArgs.length === 1, requestCode);
    const input = exactDataObject(readArgs[0], READ_OPERATION_KEYS, requestCode);
    const expectedHead = canonicalHead(input.expectedHead, requestCode);
    const operationId = canonicalOpaqueId(input.operationId, requestCode);
    return await runSerializable(store, async (transaction) => {
      await requireExpectedHead(transaction, identity, expectedHead, stateCode);
      return await readOperationInTransaction(
        transaction,
        identity,
        operationId,
        expectedHead,
        stateCode,
      );
    });
  };

  const readOperationsPage = async function readOperationsPage(...pageArgs) {
    ensure(pageArgs.length === 1, requestCode);
    const input = exactDataObject(pageArgs[0], READ_PAGE_KEYS, requestCode);
    const expectedHead = canonicalHead(input.expectedHead, requestCode);
    const afterOperationId =
      input.afterOperationId === null
        ? null
        : canonicalOpaqueId(input.afterOperationId, requestCode);
    ensure(
      numberIsSafeIntegerIntrinsic(input.limit) &&
        input.limit >= 1 &&
        input.limit <= MAX_PAGE_SIZE,
      requestCode,
    );
    const normalizedInput = objectFreeze({
      afterOperationId,
      expectedHead,
      limit: input.limit,
    });
    return await runSerializable(store, async (transaction) => {
      await requireExpectedHead(transaction, identity, expectedHead, stateCode);
      return await readOperationsPageInTransaction(
        transaction,
        identity,
        normalizedInput,
        stateCode,
      );
    });
  };

  const compareAndAdvance = async function compareAndAdvance(...advanceArgs) {
    ensure(advanceArgs.length === 1, requestCode);
    const input = exactDataObject(advanceArgs[0], ADVANCE_KEYS, requestCode);
    const expectedHead = canonicalHead(input.expectedHead, requestCode);
    const nextHead = canonicalHead(input.nextHead, requestCode);
    const transition = normalizeTransition(
      input.transition,
      expectedHead,
      nextHead,
      requestCode,
    );
    return await runSerializable(store, async (transaction) => {
      const advanced = await compareHeadInTransaction(
        transaction,
        identity,
        expectedHead,
        nextHead,
        stateCode,
      );
      if (!advanced) return false;
      if (transition.type === "append-prepared-v1") {
        await insertPreparedInTransaction(
          transaction,
          identity,
          transition.material,
          stateCode,
        );
      } else if (transition.type === "append-committed-v1") {
        await updateCommittedInTransaction(
          transaction,
          identity,
          transition.material,
          transition.frameChecksum,
          stateCode,
        );
      }
      return true;
    });
  };

  objectFreeze(readHead);
  objectFreeze(readOperation);
  objectFreeze(readOperationsPage);
  objectFreeze(compareAndAdvance);
  return objectFreeze({
    contractVersion:
      POSTGRES_FILESYSTEM_IMAGE_PROVIDER_STATE_AUTHORITY_CONTRACT_VERSION,
    readHead,
    readOperation,
    readOperationsPage,
    compareAndAdvance,
  });
}

objectFreeze(PostgresFilesystemImageProviderStateAuthorityError.prototype);
objectFreeze(PostgresFilesystemImageProviderStateAuthorityError);
objectFreeze(createPostgresFilesystemImageProviderStateAuthority);
