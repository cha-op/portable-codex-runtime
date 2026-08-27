import { Buffer } from "node:buffer";
import { Hash, createHash } from "node:crypto";
import { basename, dirname, isAbsolute, parse, resolve } from "node:path";
import { types as utilTypes } from "node:util";

import {
  FILESYSTEM_IMAGE_PROVIDER_STATE_HEAD_CONTRACT_VERSION,
  FILESYSTEM_IMAGE_PROVIDER_STATE_V2_HEAD_CONTRACT_VERSION,
  filesystemImageProviderStateHeadChecksum,
  normalizeFilesystemImageProviderStateHead,
} from "./filesystem-image-provider-state.mjs";
import {
  PostgresSerializableStoreError,
  PostgresSerializableStore,
  consumePostgresSerializableTransactionRows,
  isPostgresSerializableStore,
} from "./postgres-serializable-store.mjs";

export const POSTGRES_FILESYSTEM_IMAGE_PROVIDER_STATE_AUTHORITY_CONTRACT_VERSION =
  1;
export const POSTGRES_FILESYSTEM_IMAGE_PROVIDER_STATE_RUNTIME_AUTHORITY_CONTRACT_VERSION =
  3;
export const POSTGRES_FILESYSTEM_IMAGE_PROVIDER_STATE_ADOPTION_AUTHORITY_CONTRACT_VERSION =
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
// The frozen adoption ABI supplies complete arrays, so this is an explicit
// operational capacity rather than the uint32 checkpoint format limit. States
// beyond it require a future versioned streaming adoption capability.
const MAX_ADOPTION_OPERATIONS = 65_535;
const MAX_ADOPTION_STORAGES = 65_535;
const MAX_ADOPTION_INSERT_BATCH_SIZE = 64;
// Runtime projection batching is independent from the frozen adoption ABI.
const MAX_RUNTIME_ATTACHMENT_ORIGIN_BATCH_SIZE = 65_535;
// The complete-array ABI also has one shared canonical-material budget. The
// budget counts every stored operation material (prepared and, when present,
// committed) plus every projected storage wrapper. Larger states need a
// future versioned streaming adoption capability.
const MAX_ADOPTION_CANONICAL_BYTES = MAX_LEDGER_BYTES;
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
const ADOPTION_MANIFEST_DOMAIN = Buffer.from(
  "portable-codex/filesystem-image-provider-state/adoption-manifest/v1\0",
  "utf8",
);
const PREPARED_PROJECTION_DOMAIN = Buffer.from(
  "portable-codex/filesystem-image-provider-state/prepared-projection/v1\0",
  "utf8",
);
const STABLE_STORAGE_PROJECTION_DOMAIN = Buffer.from(
  "portable-codex/filesystem-image-provider-state/stable-storage-projection/v1\0",
  "utf8",
);
const ATTACHMENT_ORIGIN_PROJECTION_DOMAIN = Buffer.from(
  "portable-codex/filesystem-image-provider-state/attachment-origin-projection/v1\0",
  "utf8",
);
const AUTHORITY_PROJECTION_RECEIPT_DOMAIN = Buffer.from(
  "portable-codex/filesystem-image-provider-state/authority-projection-receipt/v1\0",
  "utf8",
);

const OPTION_KEYS = Object.freeze(["store", "providerId", "anchorId"]);
const READ_OPERATION_KEYS = Object.freeze(["expectedHead", "operationId"]);
const READ_PAGE_KEYS = Object.freeze([
  "afterOperationId",
  "expectedHead",
  "limit",
]);
const READ_PREPARED_PAGE_KEYS = Object.freeze([
  "afterStorageId",
  "expectedHead",
  "limit",
]);
const COMPARE_PROJECTION_KEYS = Object.freeze([
  "expectedHead",
  "preparedOperationCount",
  "preparedProjectionChecksum",
  "attachmentOrigins",
]);
const ATTACHMENT_ORIGIN_KEYS = Object.freeze([
  "currentStorageRevision",
  "operationId",
  "stableStorageChecksum",
  "storageId",
]);
const ADOPTION_KEYS = Object.freeze([
  "expectedHead",
  "nextHead",
  "operations",
  "storages",
]);
const ADOPTION_STORAGE_KEYS = Object.freeze([
  "currentAttachmentOriginOperationId",
  "storage",
]);
const ADVANCE_KEYS = Object.freeze([
  "expectedHead",
  "nextHead",
  "transition",
]);
const ROTATION_TRANSITION_KEYS = Object.freeze(["contractVersion", "type"]);
const APPEND_TRANSITION_KEYS = Object.freeze([
  "contractVersion",
  "type",
  "frameChecksum",
  "record",
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
  "operation_index_state_revision",
  "operation_index_adoption_id",
  "operation_index_adoption_xid",
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
  "adoption_id",
]);
const ERROR_MESSAGES = Object.freeze({
  invalid_postgres_filesystem_image_provider_state_authority_options:
    "PostgreSQL filesystem image provider state authority options are invalid",
  invalid_postgres_filesystem_image_provider_state_authority_request:
    "PostgreSQL filesystem image provider state authority request is invalid",
  postgres_filesystem_image_provider_state_authority_state_invalid:
    "PostgreSQL filesystem image provider state authority state is invalid",
  postgres_filesystem_image_provider_state_adoption_commit_outcome_uncertain:
    "PostgreSQL filesystem image provider state adoption commit outcome is uncertain",
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
  "operation_index_state_revision::pg_catalog.text AS operation_index_state_revision",
  "operation_index_adoption_id",
  "operation_index_adoption_xid::pg_catalog.text AS operation_index_adoption_xid",
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
  "adoption_id",
].join(", ");
const READ_HEAD_QUERY = [
  `SELECT ${HEAD_COLUMNS}`,
  "FROM session_authority.filesystem_image_provider_heads",
  "WHERE provider_id = $1 AND anchor_id = $2",
].join(" ");
const READ_HEAD_FOR_UPDATE_QUERY = `${READ_HEAD_QUERY} FOR UPDATE`;
const INSERT_HEAD_QUERY = [
  "INSERT INTO session_authority.filesystem_image_provider_heads",
  "(provider_id, anchor_id, contract_version, anchor_revision, generation,",
  "state_revision, base_head_checksum, checkpoint_state_revision,",
  "checkpoint_frame_count, checkpoint_checksum, checkpoint_bytes,",
  "frame_count, last_checksum, ledger_bytes, operation_index_state_revision)",
  "VALUES ($1, $2, $3, $4::pg_catalog.numeric, $5::pg_catalog.numeric,",
  "$6::pg_catalog.numeric, $7, $8::pg_catalog.numeric, $9::pg_catalog.int8,",
  "$10, $11::pg_catalog.int8, $12::pg_catalog.int4, $13, $14::pg_catalog.int8,",
  "$15::pg_catalog.numeric)",
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
  "last_checksum = $13, ledger_bytes = $14::pg_catalog.int8,",
  "operation_index_state_revision = $15::pg_catalog.numeric",
  "WHERE provider_id = $1 AND anchor_id = $2",
  "AND contract_version = $16",
  "AND anchor_revision = $17::pg_catalog.numeric",
  "AND generation = $18::pg_catalog.numeric",
  "AND state_revision = $19::pg_catalog.numeric",
  "AND base_head_checksum IS NOT DISTINCT FROM $20",
  "AND checkpoint_state_revision = $21::pg_catalog.numeric",
  "AND checkpoint_frame_count = $22::pg_catalog.int8",
  "AND checkpoint_checksum IS NOT DISTINCT FROM $23",
  "AND checkpoint_bytes = $24::pg_catalog.int8",
  "AND frame_count = $25::pg_catalog.int4",
  "AND last_checksum IS NOT DISTINCT FROM $26",
  "AND ledger_bytes = $27::pg_catalog.int8",
  "AND operation_index_state_revision = $19::pg_catalog.numeric",
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
const READ_PREPARED_PAGE_FIRST_QUERY = [
  `SELECT ${OPERATION_COLUMNS}`,
  "FROM session_authority.filesystem_image_provider_operations",
  "WHERE provider_id = $1 AND anchor_id = $2 AND state = 'prepared'",
  'ORDER BY storage_id COLLATE pg_catalog."C"',
  "LIMIT $3::pg_catalog.int4",
].join(" ");
const READ_PREPARED_PAGE_AFTER_QUERY = [
  `SELECT ${OPERATION_COLUMNS}`,
  "FROM session_authority.filesystem_image_provider_operations",
  "WHERE provider_id = $1 AND anchor_id = $2 AND state = 'prepared'",
  'AND storage_id COLLATE pg_catalog."C" > $3 COLLATE pg_catalog."C"',
  'ORDER BY storage_id COLLATE pg_catalog."C"',
  "LIMIT $4::pg_catalog.int4",
].join(" ");
const READ_ALL_PREPARED_OPERATIONS_QUERY = [
  `SELECT ${OPERATION_COLUMNS}`,
  "FROM session_authority.filesystem_image_provider_operations",
  "WHERE provider_id = $1 AND anchor_id = $2 AND state = 'prepared'",
  'ORDER BY storage_id COLLATE pg_catalog."C"',
  "LIMIT $3::pg_catalog.int8",
].join(" ");
const READ_ATTACHMENT_ORIGINS_QUERY = [
  `SELECT ${OPERATION_COLUMNS}`,
  "FROM session_authority.filesystem_image_provider_operations",
  "WHERE provider_id = $1 AND anchor_id = $2",
  "AND operation_id = ANY($3::text[])",
  'ORDER BY operation_id COLLATE pg_catalog."C"',
  "LIMIT $4::pg_catalog.int4",
].join(" ");
const READ_ALL_OPERATIONS_QUERY = [
  `SELECT ${OPERATION_COLUMNS}`,
  "FROM session_authority.filesystem_image_provider_operations",
  "WHERE provider_id = $1 AND anchor_id = $2",
  'ORDER BY operation_id COLLATE pg_catalog."C"',
  "LIMIT $3::pg_catalog.int4",
].join(" ");
const READ_LATEST_COMMITTED_STORAGE_QUERY = [
  `SELECT ${OPERATION_COLUMNS}`,
  "FROM session_authority.filesystem_image_provider_operations",
  "WHERE provider_id = $1 AND anchor_id = $2 AND storage_id = $3",
  "AND state = 'committed'",
  "ORDER BY committed_state_revision DESC,",
  'operation_id COLLATE pg_catalog."C" DESC',
  "LIMIT 1",
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
const ADOPT_HEAD_QUERY = [
  "UPDATE session_authority.filesystem_image_provider_heads",
  "SET contract_version = $3, anchor_revision = $4::pg_catalog.numeric,",
  "generation = $5::pg_catalog.numeric, state_revision = $6::pg_catalog.numeric,",
  "base_head_checksum = $7, checkpoint_state_revision = $8::pg_catalog.numeric,",
  "checkpoint_frame_count = $9::pg_catalog.int8, checkpoint_checksum = $10,",
  "checkpoint_bytes = $11::pg_catalog.int8, frame_count = $12::pg_catalog.int4,",
  "last_checksum = $13, ledger_bytes = $14::pg_catalog.int8,",
  "operation_index_state_revision = $15::pg_catalog.numeric,",
  "operation_index_adoption_id = $16",
  "WHERE provider_id = $1 AND anchor_id = $2",
  "AND contract_version = $17 AND anchor_revision = $18::pg_catalog.numeric",
  "AND generation = $19::pg_catalog.numeric",
  "AND state_revision = $20::pg_catalog.numeric",
  "AND base_head_checksum IS NOT DISTINCT FROM $21",
  "AND checkpoint_state_revision = $22::pg_catalog.numeric",
  "AND checkpoint_frame_count = $23::pg_catalog.int8",
  "AND checkpoint_checksum IS NOT DISTINCT FROM $24",
  "AND checkpoint_bytes = $25::pg_catalog.int8",
  "AND frame_count = $26::pg_catalog.int4",
  "AND last_checksum IS NOT DISTINCT FROM $27",
  "AND ledger_bytes = $28::pg_catalog.int8",
  "AND operation_index_state_revision IS NOT DISTINCT FROM $29::pg_catalog.numeric",
  "AND operation_index_adoption_id IS NULL",
  "AND operation_index_adoption_xid IS NULL",
  `RETURNING ${HEAD_COLUMNS}`,
].join(" ");
const INSERT_ADOPTED_OPERATION_PREFIX = [
  "INSERT INTO session_authority.filesystem_image_provider_operations",
  "(provider_id, anchor_id, operation_id, record_contract_version, state, kind,",
  "storage_id, prepared_state_revision, prepared_checksum,",
  "prepared_record_bytes, prepared_record_sha256, committed_state_revision,",
  "committed_checksum_provenance, committed_checksum, committed_record_bytes,",
  "committed_record_sha256, adoption_id)",
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
const mathMinIntrinsic = Math.min;
const mapGetIntrinsic = Map.prototype.get;
const mapHasIntrinsic = Map.prototype.has;
const mapSetIntrinsic = Map.prototype.set;
const mapDeleteIntrinsic = Map.prototype.delete;
const MapConstructor = Map;
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
const objectIsExtensibleIntrinsic = Object.isExtensible;
const objectIsFrozenIntrinsic = Object.isFrozen;
const mapSizeGetterIntrinsic = Object.getOwnPropertyDescriptor(
  Map.prototype,
  "size",
).get;
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

function mapGet(value, key) {
  return callIntrinsic(mapGetIntrinsic, value, [key]);
}

function mapHas(value, key) {
  return callIntrinsic(mapHasIntrinsic, value, [key]);
}

function mapSet(value, key, entry) {
  return callIntrinsic(mapSetIntrinsic, value, [key, entry]);
}

function mapDelete(value, key) {
  return callIntrinsic(mapDeleteIntrinsic, value, [key]);
}

function mapSize(value) {
  return callIntrinsic(mapSizeGetterIntrinsic, value, []);
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

function objectIsExtensible(value) {
  return callIntrinsic(objectIsExtensibleIntrinsic, ObjectConstructor, [
    value,
  ]);
}

function objectIsFrozen(value) {
  return callIntrinsic(objectIsFrozenIntrinsic, ObjectConstructor, [value]);
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

function assertPlainObjectShape(value, code) {
  ensure(
    value !== null &&
      typeof value === "object" &&
      !isProxyValue(value) &&
      !arrayIsArray(value),
    code,
  );
  let prototype;
  try {
    prototype = objectGetPrototypeOf(value);
  } catch {
    fail(code);
  }
  ensure(prototype === objectPrototype || prototype === null, code);
}

function inspectPlainObject(value, maximumKeys, code) {
  ensure(
    numberIsSafeIntegerIntrinsic(maximumKeys) && maximumKeys >= 0,
    code,
  );
  assertPlainObjectShape(value, code);
  let keys;
  try {
    keys = reflectOwnKeys(value);
  } catch {
    fail(code);
  }
  // The JavaScript object ABI has no bounded own-key iterator, so the first
  // Reflect.ownKeys array for a plain object is an unavoidable boundary.
  // Reject it immediately when oversized so no second huge array, O(n log n)
  // sort, descriptor walk, or recursive traversal follows. Bounding that first
  // enumeration requires a future serialized token/byte ABI instead of an
  // object-valued one.
  ensure(keys.length <= maximumKeys, code);
  ensure(arrayEvery(keys, (key) => typeof key === "string"), code);
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

function ownFrozenDataValue(value, key, code) {
  let descriptor;
  try {
    descriptor = objectGetOwnPropertyDescriptor(value, key);
  } catch {
    fail(code);
  }
  ensure(
    descriptor?.enumerable === true &&
      descriptor.configurable === false &&
      descriptor.writable === false &&
      objectHasOwn(descriptor, "value"),
    code,
  );
  return descriptor.value;
}

function exactDataObjectFromKeys(value, keys, expectedKeys, code) {
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

function exactDataObject(value, expectedKeys, code) {
  const keys = inspectPlainObject(value, expectedKeys.length, code);
  return exactDataObjectFromKeys(value, keys, expectedKeys, code);
}

function denseDataArray(value, maximumLength, code) {
  ensure(
    !isProxyValue(value) &&
      arrayIsArray(value) &&
      objectGetPrototypeOf(value) === arrayPrototype &&
      numberIsSafeIntegerIntrinsic(value.length) &&
      value.length <= maximumLength,
    code,
  );
  let keys;
  try {
    keys = reflectOwnKeys(value);
  } catch {
    fail(code);
  }
  ensure(keys.length === value.length + 1, code);
  const normalized = [];
  for (let index = 0; index < value.length; index += 1) {
    const descriptor = objectGetOwnPropertyDescriptor(
      value,
      StringConstructor(index),
    );
    ensure(
      descriptor?.enumerable === true && objectHasOwn(descriptor, "value"),
      code,
    );
    arrayPush(normalized, descriptor.value);
  }
  return normalized;
}

function frozenDenseDataArrayLength(value, maximumLength, code) {
  ensure(
    !isProxyValue(value) &&
      arrayIsArray(value) &&
      objectGetPrototypeOf(value) === arrayPrototype &&
      numberIsSafeIntegerIntrinsic(value.length) &&
      value.length <= maximumLength &&
      objectIsExtensible(value) === false,
    code,
  );
  const lengthDescriptor = objectGetOwnPropertyDescriptor(value, "length");
  ensure(
    lengthDescriptor?.enumerable === false &&
      lengthDescriptor.configurable === false &&
      lengthDescriptor.writable === false &&
      objectHasOwn(lengthDescriptor, "value") &&
      lengthDescriptor.value === value.length,
    code,
  );
  // Enumerating every own key would allocate an authority-owned O(length)
  // keys array before batching begins. The projection ABI consumes only the
  // frozen dense indexed payload; each index is validated by descriptor as it
  // is hashed below, and inert extra own properties are not projection input.
  return value.length;
}

function consumeAdoptionCanonicalBytes(budget, bytes, code) {
  ensure(
    numberIsSafeIntegerIntrinsic(bytes) &&
      bytes >= 0 &&
      budget.bytes <= MAX_ADOPTION_CANONICAL_BYTES - bytes,
    code,
  );
  budget.bytes += bytes;
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

function canonicalObjectPrecursorBytes(keys, state, code) {
  ensure(
    numberIsSafeIntegerIntrinsic(keys.length) &&
      keys.length >= 0 &&
      keys.length <= MAX_CANONICAL_NODES - state.budget.nodes,
    code,
  );
  let keyAndStructureBytes =
    2 + mathMaxIntrinsic(0, keys.length - 1);
  for (let index = 0; index < keys.length; index += 1) {
    const key = assertLosslessString(keys[index], code);
    const encodedKeyBytes = bufferByteLength(
      jsonStringifyIntrinsic(key),
      "utf8",
    );
    ensure(
      numberIsSafeIntegerIntrinsic(encodedKeyBytes) &&
        encodedKeyBytes >= 0 &&
        encodedKeyBytes < MAX_CANONICAL_BYTES &&
        keyAndStructureBytes <=
          MAX_CANONICAL_BYTES - encodedKeyBytes - 1,
      code,
    );
    keyAndStructureBytes += encodedKeyBytes + 1;
  }
  // Reserve at least one JSON byte and one node for every member value before
  // any key-array copy or sort. The exact values consume their budget during
  // the later recursive traversal.
  ensure(
    keys.length <= MAX_CANONICAL_BYTES - keyAndStructureBytes &&
      state.budget.bytes <=
        MAX_CANONICAL_BYTES - keyAndStructureBytes - keys.length,
    code,
  );
  return keyAndStructureBytes;
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
  const keys = inspectPlainObject(
    value,
    MAX_CANONICAL_NODES - state.budget.nodes,
    code,
  );
  const precursorBytes = canonicalObjectPrecursorBytes(keys, state, code);
  consumeBudget(state, precursorBytes, code);
  const result = {};
  const sortedKeys = callIntrinsic(arraySliceIntrinsic, keys, []);
  callIntrinsic(arraySortIntrinsic, sortedKeys, []);
  for (let index = 0; index < sortedKeys.length; index += 1) {
    const key = sortedKeys[index];
    objectDefineProperty(result, key, {
      enumerable: true,
      value: canonicalize(ownDataValue(value, key, code), code, nestedState()),
    });
  }
  return objectFreeze(result);
}

function canonicalObject(value, code) {
  // The provider v2 ABI requires an object root. This constant-work shape
  // check intentionally does not enumerate keys; canonicalize performs the
  // sole bounded Reflect.ownKeys pass in its ordinary-object branch.
  assertPlainObjectShape(value, code);
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

function beginCountedProjectionHash(domain, count) {
  const hash = createHashIntrinsic("sha256");
  callIntrinsic(hashUpdateIntrinsic, hash, [domain]);
  callIntrinsic(hashUpdateIntrinsic, hash, [
    bufferFrom(`${StringConstructor(count)}\0`, "ascii"),
  ]);
  return hash;
}

function updateLengthPrefixedBytes(hash, bytes) {
  callIntrinsic(hashUpdateIntrinsic, hash, [
    bufferFrom(`${StringConstructor(bytes.length)}\0`, "ascii"),
  ]);
  callIntrinsic(hashUpdateIntrinsic, hash, [bytes]);
}

function postgresTextArrayLiteral(values) {
  // canonicalOpaqueId excludes every PostgreSQL array delimiter and escape
  // character. Quote each value so the otherwise-valid ID "NULL" remains
  // text instead of PostgreSQL array syntax's unquoted SQL NULL sentinel.
  if (values.length === 0) return "{}";
  return `{"${callIntrinsic(arrayJoinIntrinsic, values, ['","'])}"}`;
}

function digestCanonicalProjection(domain, value) {
  const bytes = bufferFrom(canonicalString(value), "utf8");
  const hash = createHashIntrinsic("sha256");
  callIntrinsic(hashUpdateIntrinsic, hash, [domain]);
  callIntrinsic(hashUpdateIntrinsic, hash, [bytes]);
  return callIntrinsic(hashDigestIntrinsic, hash, ["hex"]);
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
  ensure(
    typeof value === "string" &&
      value.length <= 20 &&
      regexpTest(DECIMAL_PATTERN, value),
    code,
  );
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

function stableStorageProjectionChecksum(storage, code) {
  const normalized = canonicalStorageState(
    {
      storageId: storage.storageId,
      sessionId: storage.sessionId,
      backendId: storage.backendId,
      filesystemId: storage.filesystemId,
      imagePath: storage.imagePath,
      lifecycle: storage.lifecycle,
      revision: "1",
      writerEpoch: storage.writerEpoch,
      writerAuthority: storage.writerAuthority,
      mount: storage.mount,
      publicationControlIdentity: storage.publicationControlIdentity,
      dataRoot: storage.dataRoot,
      attachment: storage.attachment,
    },
    code,
  );
  return digestCanonicalProjection(
    STABLE_STORAGE_PROJECTION_DOMAIN,
    normalized,
  );
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
  const keys = inspectPlainObject(value, COMMITTED_RECORD_KEYS.length, code);
  ensure(arrayIncludes(keys, "state"), code);
  const state = ownDataValue(value, "state", code);
  ensure(state === "prepared" || state === "committed", code);
  const record = exactDataObjectFromKeys(
    value,
    keys,
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

function genesisHead(
  contractVersion = FILESYSTEM_IMAGE_PROVIDER_STATE_HEAD_CONTRACT_VERSION,
) {
  return canonicalHead(
    {
      contractVersion,
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
  ensure(
    typeof value === "string" &&
      value.length <= 20 &&
      regexpTest(DECIMAL_PATTERN, value),
    code,
  );
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
  ensure(!headEqual(head, genesisHead(head.contractVersion)), code);
  const operationIndexStateRevision =
    row.operation_index_state_revision === null
      ? null
      : canonicalUint64(row.operation_index_state_revision, code).value;
  const operationIndexAdoptionId =
    row.operation_index_adoption_id === null
      ? null
      : canonicalChecksum(row.operation_index_adoption_id, code);
  const operationIndexAdoptionXid =
    row.operation_index_adoption_xid === null
      ? null
      : canonicalUint64(row.operation_index_adoption_xid, code).value;
  ensure(
    (operationIndexAdoptionId === null) ===
      (operationIndexAdoptionXid === null) &&
      (operationIndexAdoptionId === null || head.contractVersion === 3),
    code,
  );
  return objectFreeze({
    exists: true,
    head,
    operationIndexAdoptionId,
    operationIndexAdoptionXid,
    operationIndexStateRevision,
  });
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
  const adoptionId =
    row.adoption_id === null
      ? null
      : canonicalChecksum(row.adoption_id, code);
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
    return objectFreeze({ ...preparedMaterial, adoptionId });
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
    adoptionId,
    committedChecksum,
    committedChecksumProvenance,
    preparedMaterial,
  });
}

function assertOperationVisibleAtHead(material, snapshot, code) {
  const head = snapshot.head;
  const record = material.record;
  const headRevision = BigIntConstructor(head.stateRevision);
  const preparedRevision = BigIntConstructor(record.preparedStateRevision);
  const headEndsInOperationFrame =
    head.generation === "0" || head.frameCount > 0;
  ensure(preparedRevision <= headRevision, code);
  ensure(
    material.adoptionId === null ||
      material.adoptionId === snapshot.operationIndexAdoptionId,
    code,
  );
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
          material.adoptionId !== null &&
          material.adoptionId === snapshot.operationIndexAdoptionId &&
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
    head.stateRevision,
  ];
}

function expectedHeadValues(identity, expectedHead, nextHead) {
  const nextValues = headValues(identity, nextHead);
  if (headEqual(expectedHead, genesisHead(expectedHead.contractVersion))) {
    return nextValues;
  }
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

async function readHeadInTransaction(
  transaction,
  identity,
  code,
  genesisContractVersion,
) {
  const snapshot = await readHeadSnapshotInTransaction(
    transaction,
    identity,
    code,
    false,
    genesisContractVersion,
  );
  return snapshot.head;
}

async function readHeadSnapshotInTransaction(
  transaction,
  identity,
  code,
  forUpdate = false,
  genesisContractVersion = FILESYSTEM_IMAGE_PROVIDER_STATE_HEAD_CONTRACT_VERSION,
) {
  const result = await queryTransaction(
    transaction,
    forUpdate ? READ_HEAD_FOR_UPDATE_QUERY : READ_HEAD_QUERY,
    [identity.providerId, identity.anchorId],
    code,
  );
  const rows = rowsFromResult(result, "SELECT", 1, code);
  if (rows.length !== 0) return normalizeHeadRow(rows[0], identity, code);
  const head = genesisHead(genesisContractVersion);
  return objectFreeze({
    exists: false,
    head,
    operationIndexAdoptionId: null,
    operationIndexAdoptionXid: null,
    operationIndexStateRevision: head.stateRevision,
  });
}

async function requireExpectedHead(transaction, identity, expectedHead, code) {
  const observed = await readHeadSnapshotInTransaction(
    transaction,
    identity,
    code,
    false,
    expectedHead.contractVersion,
  );
  ensure(headEqual(observed.head, expectedHead), code);
  return observed;
}

function requireExpectedOperationIndex(snapshot, expectedHead, code) {
  ensure(
    snapshot.operationIndexStateRevision === expectedHead.stateRevision,
    code,
  );
}

async function compareHeadInTransaction(
  transaction,
  identity,
  expectedHead,
  nextHead,
  observedHead,
  code,
) {
  const genesis = !observedHead.exists;
  const result = await queryTransaction(
    transaction,
    genesis ? INSERT_HEAD_QUERY : UPDATE_HEAD_QUERY,
    expectedHeadValues(identity, expectedHead, nextHead),
    code,
  );
  const rows = rowsFromResult(result, genesis ? "INSERT" : "UPDATE", 1, code);
  if (rows.length === 0) return false;
  const stored = normalizeHeadRow(rows[0], identity, code);
  ensure(
    headEqual(stored.head, nextHead) &&
      stored.operationIndexStateRevision === nextHead.stateRevision,
    code,
  );
  return true;
}

async function readOperationInTransaction(
  transaction,
  identity,
  operationId,
  expectedSnapshot,
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
  assertOperationVisibleAtHead(material, expectedSnapshot, code);
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
    assertOperationVisibleAtHead(material, input.expectedSnapshot, code);
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

async function readPreparedOperationsPageInTransaction(
  transaction,
  identity,
  input,
  code,
) {
  const maximumRows = input.limit + 1;
  const first = input.afterStorageId === null;
  const result = await queryTransaction(
    transaction,
    first ? READ_PREPARED_PAGE_FIRST_QUERY : READ_PREPARED_PAGE_AFTER_QUERY,
    first
      ? [identity.providerId, identity.anchorId, StringConstructor(maximumRows)]
      : [
          identity.providerId,
          identity.anchorId,
          input.afterStorageId,
          StringConstructor(maximumRows),
        ],
    code,
  );
  const rows = rowsFromResult(result, "SELECT", maximumRows, code);
  const normalized = [];
  let previousStorageId = input.afterStorageId;
  for (let index = 0; index < rows.length; index += 1) {
    const material = normalizeOperationRow(rows[index], identity, code);
    assertOperationVisibleAtHead(material, input.expectedSnapshot, code);
    const record = material.record;
    ensure(
      record.state === "prepared" &&
        (previousStorageId === null || record.storageId > previousStorageId),
      code,
    );
    previousStorageId = record.storageId;
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
    nextAfterStorageId:
      hasMore && operations.length > 0
        ? operations[operations.length - 1].storageId
        : null,
  });
}

async function comparePreparedProjectionInTransaction(
  transaction,
  identity,
  expectedSnapshot,
  expectedCount,
  expectedChecksum,
  structuralBound,
  code,
) {
  const hash = beginCountedProjectionHash(
    PREPARED_PROJECTION_DOMAIN,
    expectedCount,
  );
  let observedCount = 0;
  let previousStorageId = null;
  await consumePostgresSerializableTransactionRows(
    transaction,
    READ_ALL_PREPARED_OPERATIONS_QUERY,
    [
      identity.providerId,
      identity.anchorId,
      StringConstructor(structuralBound + 1),
    ],
    (row) => {
      const material = normalizeOperationRow(row, identity, code);
      assertOperationVisibleAtHead(material, expectedSnapshot, code);
      const record = material.record;
      observedCount += 1;
      ensure(observedCount <= structuralBound, code);
      ensure(
        record.state === "prepared" &&
          (previousStorageId === null ||
            record.storageId > previousStorageId),
        code,
      );
      previousStorageId = record.storageId;
      const bytes = bufferFrom(canonicalString(record), "utf8");
      updateLengthPrefixedBytes(hash, bytes);
    },
  );
  const checksum = callIntrinsic(hashDigestIntrinsic, hash, ["hex"]);
  if (observedCount !== expectedCount || checksum !== expectedChecksum) {
    return null;
  }
  return objectFreeze({ checksum, count: observedCount });
}

function normalizeAttachmentOrigin(value, code) {
  const origin = exactDataObject(value, ATTACHMENT_ORIGIN_KEYS, code);
  ensure(objectIsFrozen(value), code);
  return objectFreeze({
    currentStorageRevision: canonicalUint64(
      origin.currentStorageRevision,
      code,
      { positive: true },
    ).value,
    operationId: canonicalOpaqueId(origin.operationId, code),
    stableStorageChecksum: canonicalChecksum(
      origin.stableStorageChecksum,
      code,
    ),
    storageId: canonicalOpaqueId(origin.storageId, code),
  });
}

function normalizeAttachmentOrigins(value, maximumCount, code) {
  const count = frozenDenseDataArrayLength(value, maximumCount, code);
  const hash = beginCountedProjectionHash(
    ATTACHMENT_ORIGIN_PROJECTION_DOMAIN,
    count,
  );
  let previousStorageId = null;
  for (let index = 0; index < count; index += 1) {
    const origin = normalizeAttachmentOrigin(
      ownFrozenDataValue(value, StringConstructor(index), code),
      code,
    );
    ensure(
      previousStorageId === null || origin.storageId > previousStorageId,
      code,
    );
    previousStorageId = origin.storageId;
    updateLengthPrefixedBytes(
      hash,
      bufferFrom(canonicalString(origin), "utf8"),
    );
  }
  return objectFreeze({
    checksum: callIntrinsic(hashDigestIntrinsic, hash, ["hex"]),
    count,
    source: value,
  });
}

async function compareAttachmentOriginsInTransaction(
  transaction,
  identity,
  expectedSnapshot,
  origins,
  stateCode,
) {
  let totalObservedCount = 0;
  let projectionMismatch = false;
  let batchStart = 0;
  do {
    const batchEnd = callIntrinsic(mathMinIntrinsic, undefined, [
      origins.count,
      batchStart + MAX_RUNTIME_ATTACHMENT_ORIGIN_BATCH_SIZE,
    ]);
    const batchByOperationId = new MapConstructor();
    const batchOperationIds = [];
    const batchOperationIdSet = new SetConstructor();
    for (let index = batchStart; index < batchEnd; index += 1) {
      const origin = normalizeAttachmentOrigin(
        ownFrozenDataValue(
          origins.source,
          StringConstructor(index),
          stateCode,
        ),
        stateCode,
      );
      if (mapHas(batchByOperationId, origin.operationId)) {
        projectionMismatch = true;
      }
      mapSet(batchByOperationId, origin.operationId, origin);
      arrayPush(batchOperationIds, origin.operationId);
      callIntrinsic(setAddIntrinsic, batchOperationIdSet, [
        origin.operationId,
      ]);
    }
    const batchExpectedCount = batchOperationIds.length;
    let batchObservedCount = 0;
    let previousOperationId = null;
    await consumePostgresSerializableTransactionRows(
      transaction,
      READ_ATTACHMENT_ORIGINS_QUERY,
      [
        identity.providerId,
        identity.anchorId,
        postgresTextArrayLiteral(batchOperationIds),
        StringConstructor(batchExpectedCount + 1),
      ],
      (row) => {
        const material = normalizeOperationRow(row, identity, stateCode);
        assertOperationVisibleAtHead(material, expectedSnapshot, stateCode);
        const operationId = material.record.operationId;
        batchObservedCount += 1;
        totalObservedCount += 1;
        ensure(batchObservedCount <= batchExpectedCount + 1, stateCode);
        if (
          (previousOperationId !== null &&
            operationId <= previousOperationId) ||
          !callIntrinsic(setHasIntrinsic, batchOperationIdSet, [operationId])
        ) {
          projectionMismatch = true;
        }
        previousOperationId = operationId;
        const origin = mapGet(batchByOperationId, operationId);
        if (origin === undefined) {
          projectionMismatch = true;
          return;
        }
        const record = material.record;
        if (
          record.state !== "committed" ||
          (record.kind !== "attach" && record.kind !== "restore-attach") ||
          record.operationId !== origin.operationId ||
          record.storageId !== origin.storageId ||
          BigIntConstructor(record.storageState.revision) >
            BigIntConstructor(origin.currentStorageRevision) ||
          stableStorageProjectionChecksum(record.storageState, stateCode) !==
            origin.stableStorageChecksum
        ) {
          projectionMismatch = true;
        }
      },
    );
    if (batchObservedCount !== batchExpectedCount) {
      projectionMismatch = true;
    }
    batchStart = batchEnd;
  } while (batchStart < origins.count);
  if (totalObservedCount !== origins.count) projectionMismatch = true;
  if (projectionMismatch) return null;
  return origins.checksum;
}

async function readLatestCommittedStorageStateInTransaction(
  transaction,
  identity,
  storageId,
  expectedSnapshot,
  code,
) {
  const result = await queryTransaction(
    transaction,
    READ_LATEST_COMMITTED_STORAGE_QUERY,
    [identity.providerId, identity.anchorId, storageId],
    code,
  );
  const rows = rowsFromResult(result, "SELECT", 1, code);
  if (rows.length === 0) return null;
  const material = normalizeOperationRow(rows[0], identity, code);
  ensure(
    material.record.state === "committed" &&
      material.record.storageId === storageId,
    code,
  );
  assertOperationVisibleAtHead(material, expectedSnapshot, code);
  return material.record.storageState;
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
  const keys = inspectPlainObject(value, APPEND_TRANSITION_KEYS.length, code);
  ensure(arrayIncludes(keys, "type"), code);
  const type = ownDataValue(value, "type", code);
  if (type === "rotate-v1") {
    const transition = exactDataObjectFromKeys(
      value,
      keys,
      ROTATION_TRANSITION_KEYS,
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
  const transition = exactDataObjectFromKeys(
    value,
    keys,
    APPEND_TRANSITION_KEYS,
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

function isAdoptionRotation(expectedHead, nextHead, code) {
  return (
    expectedHead.contractVersion ===
      FILESYSTEM_IMAGE_PROVIDER_STATE_V2_HEAD_CONTRACT_VERSION &&
    expectedHead.stateRevision !== "0" &&
    nextHead.contractVersion ===
      FILESYSTEM_IMAGE_PROVIDER_STATE_HEAD_CONTRACT_VERSION &&
    decimalSuccessor(expectedHead.anchorRevision, nextHead.anchorRevision) &&
    decimalSuccessor(expectedHead.generation, nextHead.generation) &&
    nextHead.stateRevision === expectedHead.stateRevision &&
    nextHead.checkpointStateRevision === expectedHead.stateRevision &&
    nextHead.checkpointFrameCount >= 2 &&
    nextHead.checkpointFrameCount <= MAX_CHECKPOINT_FRAME_COUNT &&
    nextHead.checkpointChecksum !== null &&
    nextHead.checkpointBytes >= 1 &&
    nextHead.checkpointBytes <= MAX_CHECKPOINT_BYTES &&
    nextHead.frameCount === 0 &&
    nextHead.lastChecksum === nextHead.checkpointChecksum &&
    nextHead.ledgerBytes === 0 &&
    nextHead.baseHeadChecksum === canonicalHeadChecksum(expectedHead, code)
  );
}

function canonicalAdoptionStorages(value, budget, code) {
  const source = denseDataArray(value, MAX_ADOPTION_STORAGES, code);
  const storages = [];
  let previousStorageId = null;
  for (let index = 0; index < source.length; index += 1) {
    const wrapper = exactDataObject(
      source[index],
      ADOPTION_STORAGE_KEYS,
      code,
    );
    const storage = canonicalStorageState(wrapper.storage, code);
    const currentAttachmentOriginOperationId =
      wrapper.currentAttachmentOriginOperationId === null
        ? null
        : canonicalOpaqueId(
            wrapper.currentAttachmentOriginOperationId,
            code,
          );
    ensure(
      (storage.lifecycle === "attached") ===
        (currentAttachmentOriginOperationId !== null) &&
        (previousStorageId === null || storage.storageId > previousStorageId),
      code,
    );
    previousStorageId = storage.storageId;
    const normalized = objectFreeze({
      currentAttachmentOriginOperationId,
      storage,
    });
    consumeAdoptionCanonicalBytes(
      budget,
      bufferByteLength(canonicalString(normalized), "utf8"),
      code,
    );
    arrayPush(storages, normalized);
  }
  return objectFreeze(storages);
}

function canonicalAdoptionOperations(value, budget, code) {
  const source = denseDataArray(value, MAX_ADOPTION_OPERATIONS, code);
  const materials = [];
  let previousOperationId = null;
  for (let index = 0; index < source.length; index += 1) {
    const material = recordMaterial(source[index], code);
    consumeAdoptionCanonicalBytes(budget, material.bytes.length, code);
    if (material.record.state === "committed") {
      consumeAdoptionCanonicalBytes(
        budget,
        preparedMaterialFromCommitted(material, code).bytes.length,
        code,
      );
    }
    ensure(
      previousOperationId === null ||
        material.record.operationId > previousOperationId,
      code,
    );
    previousOperationId = material.record.operationId;
    arrayPush(materials, material);
  }
  return objectFreeze(materials);
}

function validateAdoptionReplay(materials, storages, expectedHead, code) {
  const maximumEvents = BigIntConstructor(MAX_ADOPTION_OPERATIONS * 2);
  const stateRevision = BigIntConstructor(expectedHead.stateRevision);
  ensure(stateRevision <= maximumEvents, code);
  const events = new MapConstructor();
  for (let index = 0; index < materials.length; index += 1) {
    const record = materials[index].record;
    ensure(!mapHas(events, record.preparedStateRevision), code);
    mapSet(events, record.preparedStateRevision, {
      material: materials[index],
      type: "prepared",
    });
    if (record.state === "committed") {
      ensure(!mapHas(events, record.committedStateRevision), code);
      mapSet(events, record.committedStateRevision, {
        material: materials[index],
        type: "committed",
      });
    }
  }
  ensure(BigIntConstructor(mapSize(events)) === stateRevision, code);

  const currentStorages = new MapConstructor();
  const origins = new MapConstructor();
  const pendingStorages = new MapConstructor();
  for (let revision = 1n; revision <= stateRevision; revision += 1n) {
    const event = mapGet(events, StringConstructor(revision));
    ensure(event !== undefined, code);
    const record = event.material.record;
    if (event.type === "prepared") {
      const current = mapHas(currentStorages, record.storageId)
        ? mapGet(currentStorages, record.storageId)
        : null;
      ensure(
        !mapHas(pendingStorages, record.storageId) &&
          canonicalEqual(current, record.storageStateBefore),
        code,
      );
      assertPreparePrecondition(current, record.kind, code);
      mapSet(pendingStorages, record.storageId, record.operationId);
      continue;
    }
    ensure(
      mapGet(pendingStorages, record.storageId) === record.operationId,
      code,
    );
    mapDelete(pendingStorages, record.storageId);
    const previousStorage = mapHas(currentStorages, record.storageId)
      ? mapGet(currentStorages, record.storageId)
      : null;
    assertStorageTransition(
      previousStorage,
      record.storageState,
      record.kind,
      code,
    );
    const previousOrigin = mapHas(origins, record.storageId)
      ? mapGet(origins, record.storageId)
      : null;
    let nextOrigin = previousOrigin;
    if (record.storageState.lifecycle !== "attached") {
      nextOrigin = null;
    } else if (
      record.kind === "attach" ||
      record.kind === "restore-attach"
    ) {
      nextOrigin = record.operationId;
    }
    ensure(nextOrigin !== null || record.storageState.lifecycle !== "attached", code);
    mapSet(currentStorages, record.storageId, record.storageState);
    mapSet(origins, record.storageId, nextOrigin);
  }

  ensure(mapSize(currentStorages) === storages.length, code);
  for (let index = 0; index < storages.length; index += 1) {
    const wrapper = storages[index];
    ensure(
      mapHas(currentStorages, wrapper.storage.storageId) &&
        canonicalEqual(
          mapGet(currentStorages, wrapper.storage.storageId),
          wrapper.storage,
        ) &&
        mapGet(origins, wrapper.storage.storageId) ===
          wrapper.currentAttachmentOriginOperationId,
      code,
    );
  }
}

function adoptionManifest(
  identity,
  provenance,
  expectedHead,
  nextHead,
  materials,
  storages,
) {
  const hash = createHashIntrinsic("sha256");
  callIntrinsic(hashUpdateIntrinsic, hash, [ADOPTION_MANIFEST_DOMAIN]);
  callIntrinsic(hashUpdateIntrinsic, hash, [
    `header\0${canonicalString({
      anchorId: identity.anchorId,
      expectedHead,
      nextHead,
      provenance,
      providerId: identity.providerId,
    })}\0`,
    "utf8",
  ]);
  for (let index = 0; index < materials.length; index += 1) {
    callIntrinsic(hashUpdateIntrinsic, hash, ["operation\0", "utf8"]);
    callIntrinsic(hashUpdateIntrinsic, hash, [
      `${materials[index].sha256}\0`,
      "utf8",
    ]);
    callIntrinsic(hashUpdateIntrinsic, hash, [materials[index].bytes]);
    callIntrinsic(hashUpdateIntrinsic, hash, ["\0", "utf8"]);
  }
  for (let index = 0; index < storages.length; index += 1) {
    callIntrinsic(hashUpdateIntrinsic, hash, [
      `storage\0${canonicalString(storages[index])}\0`,
      "utf8",
    ]);
  }
  return callIntrinsic(hashDigestIntrinsic, hash, ["hex"]);
}

function normalizeAdoptionRequest(value, identity, code) {
  const input = exactDataObject(value, ADOPTION_KEYS, code);
  const expectedHead = canonicalHead(input.expectedHead, code);
  const nextHead = canonicalHead(input.nextHead, code);
  ensure(isAdoptionRotation(expectedHead, nextHead, code), code);
  const budget = { bytes: 0 };
  const materials = canonicalAdoptionOperations(input.operations, budget, code);
  const storages = canonicalAdoptionStorages(input.storages, budget, code);
  validateAdoptionReplay(materials, storages, expectedHead, code);
  return objectFreeze({
    expectedHead,
    manifestIds: objectFreeze({
      indexed: adoptionManifest(
        identity,
        "indexed-frame-v1-retained",
        expectedHead,
        nextHead,
        materials,
        storages,
      ),
      legacy: adoptionManifest(
        identity,
        "unavailable-adopted-v2",
        expectedHead,
        nextHead,
        materials,
        storages,
      ),
    }),
    materials,
    nextHead,
    storages,
  });
}

function selectAdoptionManifest(input, mode) {
  return objectFreeze({
    ...input,
    manifestId: input.manifestIds[mode],
    sourceMode: mode,
  });
}

function adoptionHeadValues(identity, input, sourceMarker) {
  return [
    ...headValues(identity, input.nextHead),
    input.manifestId,
    input.expectedHead.contractVersion,
    input.expectedHead.anchorRevision,
    input.expectedHead.generation,
    input.expectedHead.stateRevision,
    input.expectedHead.baseHeadChecksum,
    input.expectedHead.checkpointStateRevision,
    StringConstructor(input.expectedHead.checkpointFrameCount),
    input.expectedHead.checkpointChecksum,
    StringConstructor(input.expectedHead.checkpointBytes),
    StringConstructor(input.expectedHead.frameCount),
    input.expectedHead.lastChecksum,
    StringConstructor(input.expectedHead.ledgerBytes),
    sourceMarker,
  ];
}

function operationMaterialsEqual(left, right) {
  return (
    left.record.operationId === right.record.operationId &&
    left.sha256 === right.sha256 &&
    bufferEquals(left.bytes, right.bytes)
  );
}

async function verifyAdoptionRowsInTransaction(
  transaction,
  identity,
  snapshot,
  input,
  compareInput,
  code,
) {
  const budget = { bytes: 0 };
  const expectedLength = compareInput ? input.materials.length : 0;
  let observedCount = 0;
  let observedMode = null;
  let previousOperationId = null;
  let semanticMismatch = false;
  await consumePostgresSerializableTransactionRows(
    transaction,
    READ_ALL_OPERATIONS_QUERY,
    [
      identity.providerId,
      identity.anchorId,
      StringConstructor(MAX_ADOPTION_OPERATIONS + 1),
    ],
    (row) => {
      const material = normalizeOperationRow(row, identity, code);
      consumeAdoptionCanonicalBytes(budget, material.bytes.length, code);
      if (material.record.state === "committed") {
        consumeAdoptionCanonicalBytes(
          budget,
          material.preparedMaterial.bytes.length,
          code,
        );
      }
      assertOperationVisibleAtHead(material, snapshot, code);
      observedCount += 1;
      ensure(
        observedCount <= MAX_ADOPTION_OPERATIONS &&
          (previousOperationId === null ||
            material.record.operationId > previousOperationId),
        code,
      );
      previousOperationId = material.record.operationId;
      const expected = compareInput
        ? input.materials[observedCount - 1]
        : undefined;
      if (
        expected === undefined ||
        !operationMaterialsEqual(material, expected)
      ) {
        semanticMismatch = true;
      }
      const rowMode =
        material.adoptionId === null
          ? "indexed"
          : input.manifestId !== undefined &&
              material.adoptionId === input.manifestId
            ? "legacy"
            : null;
      ensure(rowMode !== null, code);
      if (material.record.state === "committed") {
        ensure(
          rowMode === "legacy"
            ? material.committedChecksumProvenance ===
                "unavailable-adopted-v2" && material.committedChecksum === null
            : material.committedChecksumProvenance === "indexed-frame-v1" &&
                material.committedChecksum !== null,
          code,
        );
      }
      observedMode ??= rowMode;
      ensure(observedMode === rowMode, code);
    },
  );
  ensure(observedCount === expectedLength && !semanticMismatch, code);
  return observedMode ?? "empty";
}

function preparedMaterialFromCommitted(material, code) {
  const record = material.record;
  return recordMaterial(
    objectFreeze({
      kind: record.kind,
      operationId: record.operationId,
      preparedChecksum: record.preparedChecksum,
      preparedStateRevision: record.preparedStateRevision,
      request: record.request,
      state: "prepared",
      storageId: record.storageId,
      storageStateBefore: record.storageStateBefore,
    }),
    code,
  );
}

function adoptedOperationValues(identity, material, manifestId, code) {
  const record = material.record;
  const preparedMaterial =
    record.state === "prepared"
      ? material
      : preparedMaterialFromCommitted(material, code);
  return [
    identity.providerId,
    identity.anchorId,
    record.operationId,
    POSTGRES_FILESYSTEM_IMAGE_PROVIDER_STATE_AUTHORITY_CONTRACT_VERSION,
    record.state,
    record.kind,
    record.storageId,
    record.preparedStateRevision,
    record.preparedChecksum,
    bufferToString(preparedMaterial.bytes, "hex"),
    preparedMaterial.sha256,
    record.state === "committed" ? record.committedStateRevision : null,
    record.state === "committed" ? "unavailable-adopted-v2" : null,
    null,
    record.state === "committed" ? bufferToString(material.bytes, "hex") : null,
    record.state === "committed" ? material.sha256 : null,
    manifestId,
  ];
}

function adoptedOperationBatchQuery(batchSize) {
  let tuples = "";
  for (let index = 0; index < batchSize; index += 1) {
    const offset = index * 17;
    if (index !== 0) tuples += ", ";
    tuples += callIntrinsic(
      arrayJoinIntrinsic,
      [
        `($${offset + 1}, $${offset + 2}, $${offset + 3},`,
        `$${offset + 4}, $${offset + 5}, $${offset + 6}, $${offset + 7},`,
        `$${offset + 8}::pg_catalog.numeric, $${offset + 9},`,
        `pg_catalog.decode($${offset + 10}, 'hex'), $${offset + 11},`,
        `$${offset + 12}::pg_catalog.numeric, $${offset + 13},`,
        `$${offset + 14}, CASE WHEN $${offset + 15}::pg_catalog.text IS NULL`,
        `THEN NULL ELSE pg_catalog.decode($${offset + 15}, 'hex') END,`,
        `$${offset + 16}, $${offset + 17})`,
      ],
      [" "],
    );
  }
  return callIntrinsic(
    arrayJoinIntrinsic,
    [
      INSERT_ADOPTED_OPERATION_PREFIX,
      `VALUES ${tuples}`,
      "ON CONFLICT DO NOTHING",
      `RETURNING ${OPERATION_COLUMNS}`,
    ],
    [" "],
  );
}

async function insertAdoptedOperationsInTransaction(
  transaction,
  identity,
  input,
  code,
) {
  for (
    let batchStart = 0;
    batchStart < input.materials.length;
    batchStart += MAX_ADOPTION_INSERT_BATCH_SIZE
  ) {
    const batchSize = mathMinIntrinsic(
      MAX_ADOPTION_INSERT_BATCH_SIZE,
      input.materials.length - batchStart,
    );
    const values = [];
    for (let index = 0; index < batchSize; index += 1) {
      const material = input.materials[batchStart + index];
      const rowValues = adoptedOperationValues(
        identity,
        material,
        input.manifestId,
        code,
      );
      for (let valueIndex = 0; valueIndex < rowValues.length; valueIndex += 1) {
        arrayPush(values, rowValues[valueIndex]);
      }
    }
    const result = await queryTransaction(
      transaction,
      adoptedOperationBatchQuery(batchSize),
      values,
      code,
    );
    const rows = rowsFromResult(result, "INSERT", batchSize, code);
    ensure(rows.length === batchSize, code);
    const returned = new MapConstructor();
    for (let index = 0; index < rows.length; index += 1) {
      const stored = normalizeOperationRow(rows[index], identity, code);
      ensure(!mapHas(returned, stored.record.operationId), code);
      mapSet(returned, stored.record.operationId, stored);
    }
    for (let index = 0; index < batchSize; index += 1) {
      const material = input.materials[batchStart + index];
      const stored = mapGet(returned, material.record.operationId);
      ensure(
        stored !== undefined &&
          operationMaterialsEqual(stored, material) &&
          stored.adoptionId === input.manifestId &&
          (stored.record.state !== "committed" ||
            (stored.committedChecksumProvenance ===
              "unavailable-adopted-v2" &&
              stored.committedChecksum === null)),
        code,
      );
    }
  }
}

function targetAdoptionSnapshotMatches(snapshot, input) {
  return (
    snapshot.exists &&
    headEqual(snapshot.head, input.nextHead) &&
    snapshot.operationIndexStateRevision === input.nextHead.stateRevision &&
    snapshot.operationIndexAdoptionId === input.manifestId &&
    snapshot.operationIndexAdoptionXid !== null
  );
}

function selectedAdoptionForTarget(snapshot, input) {
  if (!snapshot.exists || !headEqual(snapshot.head, input.nextHead)) return null;
  const legacy = selectAdoptionManifest(input, "legacy");
  if (targetAdoptionSnapshotMatches(snapshot, legacy)) return legacy;
  const indexed = selectAdoptionManifest(input, "indexed");
  if (targetAdoptionSnapshotMatches(snapshot, indexed)) return indexed;
  return null;
}

async function verifyTargetAdoptionInTransaction(
  transaction,
  identity,
  snapshot,
  input,
  code,
) {
  ensure(targetAdoptionSnapshotMatches(snapshot, input), code);
  const observedMode = await verifyAdoptionRowsInTransaction(
    transaction,
    identity,
    snapshot,
    input,
    true,
    code,
  );
  ensure(
    observedMode === "empty" || observedMode === input.sourceMode,
    code,
  );
}

async function sourceAdoptionModeInTransaction(
  transaction,
  identity,
  snapshot,
  input,
  code,
) {
  ensure(
    snapshot.exists &&
      headEqual(snapshot.head, input.expectedHead) &&
      snapshot.operationIndexAdoptionId === null &&
      snapshot.operationIndexAdoptionXid === null,
    code,
  );
  if (snapshot.operationIndexStateRevision === null) {
    const observedMode = await verifyAdoptionRowsInTransaction(
      transaction,
      identity,
      snapshot,
      input,
      false,
      code,
    );
    ensure(observedMode === "empty", code);
    return "legacy";
  }
  const observedMode = await verifyAdoptionRowsInTransaction(
    transaction,
    identity,
    snapshot,
    input,
    true,
    code,
  );
  ensure(
    snapshot.operationIndexStateRevision === input.expectedHead.stateRevision &&
      observedMode !== "legacy",
    code,
  );
  return "indexed";
}

async function compareAndAdoptInTransaction(
  transaction,
  identity,
  input,
  code,
) {
  const observed = await readHeadSnapshotInTransaction(
    transaction,
    identity,
    code,
    true,
    FILESYSTEM_IMAGE_PROVIDER_STATE_V2_HEAD_CONTRACT_VERSION,
  );
  const idempotentInput = selectedAdoptionForTarget(observed, input);
  if (idempotentInput !== null) {
    await verifyTargetAdoptionInTransaction(
      transaction,
      identity,
      observed,
      idempotentInput,
      code,
    );
    return true;
  }
  if (!observed.exists || !headEqual(observed.head, input.expectedHead)) {
    return false;
  }
  const sourceMode = await sourceAdoptionModeInTransaction(
    transaction,
    identity,
    observed,
    input,
    code,
  );
  const selectedInput = selectAdoptionManifest(input, sourceMode);
  const update = await queryTransaction(
    transaction,
    ADOPT_HEAD_QUERY,
    adoptionHeadValues(
      identity,
      selectedInput,
      observed.operationIndexStateRevision,
    ),
    code,
  );
  const rows = rowsFromResult(update, "UPDATE", 1, code);
  if (rows.length === 0) return false;
  const adopted = normalizeHeadRow(rows[0], identity, code);
  ensure(targetAdoptionSnapshotMatches(adopted, selectedInput), code);
  if (sourceMode === "legacy") {
    await insertAdoptedOperationsInTransaction(
      transaction,
      identity,
      selectedInput,
      code,
    );
  }
  await verifyTargetAdoptionInTransaction(
    transaction,
    identity,
    adopted,
    selectedInput,
    code,
  );
  return true;
}

async function resolveAdoptionCommitOutcome(store, identity, input, code) {
  try {
    return await runSerializable(store, async (transaction) => {
      // Order readback after any still-resolving writer; a serialization
      // failure remains uncertain instead of misreporting an unchanged head.
      const observed = await readHeadSnapshotInTransaction(
        transaction,
        identity,
        code,
        true,
        FILESYSTEM_IMAGE_PROVIDER_STATE_V2_HEAD_CONTRACT_VERSION,
      );
      const selectedInput = selectedAdoptionForTarget(observed, input);
      if (selectedInput !== null) {
        await verifyTargetAdoptionInTransaction(
          transaction,
          identity,
          observed,
          selectedInput,
          code,
        );
        return true;
      }
      if (
        observed.exists &&
        headEqual(observed.head, input.expectedHead) &&
        observed.operationIndexAdoptionId === null &&
        observed.operationIndexAdoptionXid === null
      ) {
        await sourceAdoptionModeInTransaction(
          transaction,
          identity,
          observed,
          input,
          code,
        );
        return false;
      }
      fail(
        "postgres_filesystem_image_provider_state_adoption_commit_outcome_uncertain",
      );
    });
  } catch {
    fail(
      "postgres_filesystem_image_provider_state_adoption_commit_outcome_uncertain",
    );
  }
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

function normalizeProjectionRequestOuter(value, code) {
  const input = exactDataObject(value, COMPARE_PROJECTION_KEYS, code);
  ensure(
    numberIsSafeIntegerIntrinsic(input.preparedOperationCount) &&
      input.preparedOperationCount >= 0,
    code,
  );
  const expectedHead = canonicalHead(input.expectedHead, code);
  const structuralBound =
    expectedHead.checkpointFrameCount + expectedHead.frameCount;
  ensure(numberIsSafeIntegerIntrinsic(structuralBound), code);
  const preparedProjectionChecksum = canonicalChecksum(
    input.preparedProjectionChecksum,
    code,
  );
  return objectFreeze({
    attachmentOrigins: input.attachmentOrigins,
    expectedHead,
    preparedOperationCount: input.preparedOperationCount,
    preparedProjectionChecksum,
  });
}

function createAuthoritySurface(args, runtimeOnly) {
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
    return await runSerializable(store, async (transaction) => {
      const head = await readHeadInTransaction(
        transaction,
        identity,
        stateCode,
        runtimeOnly
          ? FILESYSTEM_IMAGE_PROVIDER_STATE_HEAD_CONTRACT_VERSION
          : FILESYSTEM_IMAGE_PROVIDER_STATE_V2_HEAD_CONTRACT_VERSION,
      );
      ensure(
        runtimeOnly ||
          head.contractVersion ===
            FILESYSTEM_IMAGE_PROVIDER_STATE_V2_HEAD_CONTRACT_VERSION,
        stateCode,
      );
      return head;
    });
  };

  const readOperation = async function readOperation(...readArgs) {
    ensure(readArgs.length === 1, requestCode);
    const input = exactDataObject(readArgs[0], READ_OPERATION_KEYS, requestCode);
    const expectedHead = canonicalHead(input.expectedHead, requestCode);
    const operationId = canonicalOpaqueId(input.operationId, requestCode);
    return await runSerializable(store, async (transaction) => {
      const observedHead = await requireExpectedHead(
        transaction,
        identity,
        expectedHead,
        stateCode,
      );
      ensure(
        expectedHead.contractVersion ===
          (runtimeOnly
            ? FILESYSTEM_IMAGE_PROVIDER_STATE_HEAD_CONTRACT_VERSION
            : FILESYSTEM_IMAGE_PROVIDER_STATE_V2_HEAD_CONTRACT_VERSION),
        stateCode,
      );
      requireExpectedOperationIndex(observedHead, expectedHead, stateCode);
      if (!observedHead.exists) return null;
      return await readOperationInTransaction(
        transaction,
        identity,
        operationId,
        observedHead,
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
      const observedHead = await requireExpectedHead(
        transaction,
        identity,
        expectedHead,
        stateCode,
      );
      ensure(
        expectedHead.contractVersion ===
          (runtimeOnly
            ? FILESYSTEM_IMAGE_PROVIDER_STATE_HEAD_CONTRACT_VERSION
            : FILESYSTEM_IMAGE_PROVIDER_STATE_V2_HEAD_CONTRACT_VERSION),
        stateCode,
      );
      requireExpectedOperationIndex(observedHead, expectedHead, stateCode);
      if (!observedHead.exists) {
        const operations = [];
        objectFreeze(operations);
        return objectFreeze({ operations, nextAfterOperationId: null });
      }
      return await readOperationsPageInTransaction(
        transaction,
        identity,
        objectFreeze({
          ...normalizedInput,
          expectedSnapshot: observedHead,
        }),
        stateCode,
      );
    });
  };

  const readPreparedOperationsPage = async function readPreparedOperationsPage(
    ...pageArgs
  ) {
    ensure(pageArgs.length === 1, requestCode);
    const input = exactDataObject(
      pageArgs[0],
      READ_PREPARED_PAGE_KEYS,
      requestCode,
    );
    const expectedHead = canonicalHead(input.expectedHead, requestCode);
    const afterStorageId =
      input.afterStorageId === null
        ? null
        : canonicalOpaqueId(input.afterStorageId, requestCode);
    ensure(
      numberIsSafeIntegerIntrinsic(input.limit) &&
        input.limit >= 1 &&
        input.limit <= MAX_PAGE_SIZE,
      requestCode,
    );
    return await runSerializable(store, async (transaction) => {
      const observedHead = await requireExpectedHead(
        transaction,
        identity,
        expectedHead,
        stateCode,
      );
      ensure(expectedHead.contractVersion === 3, stateCode);
      requireExpectedOperationIndex(observedHead, expectedHead, stateCode);
      if (!observedHead.exists) {
        const operations = [];
        objectFreeze(operations);
        return objectFreeze({ operations, nextAfterStorageId: null });
      }
      return await readPreparedOperationsPageInTransaction(
        transaction,
        identity,
        objectFreeze({
          afterStorageId,
          expectedSnapshot: observedHead,
          limit: input.limit,
        }),
        stateCode,
      );
    });
  };

  const compareProjection = async function compareProjection(
    ...projectionArgs
  ) {
    ensure(runtimeOnly && projectionArgs.length === 1, requestCode);
    const input = normalizeProjectionRequestOuter(
      projectionArgs[0],
      requestCode,
    );
    return await runSerializable(store, async (transaction) => {
      const observedHead = await readHeadSnapshotInTransaction(
        transaction,
        identity,
        stateCode,
        false,
        input.expectedHead.contractVersion,
      );
      if (!headEqual(observedHead.head, input.expectedHead)) return null;
      const structuralBound =
        observedHead.head.checkpointFrameCount + observedHead.head.frameCount;
      const attachmentOrigins = normalizeAttachmentOrigins(
        input.attachmentOrigins,
        structuralBound,
        requestCode,
      );
      ensure(attachmentOrigins.count <= structuralBound, requestCode);
      ensure(
        input.expectedHead.contractVersion ===
          FILESYSTEM_IMAGE_PROVIDER_STATE_HEAD_CONTRACT_VERSION,
        stateCode,
      );
      requireExpectedOperationIndex(observedHead, input.expectedHead, stateCode);
      ensure(
        input.preparedOperationCount <= structuralBound,
        requestCode,
      );
      const prepared = await comparePreparedProjectionInTransaction(
        transaction,
        identity,
        observedHead,
        input.preparedOperationCount,
        input.preparedProjectionChecksum,
        structuralBound,
        stateCode,
      );
      const attachmentOriginsChecksum =
        await compareAttachmentOriginsInTransaction(
          transaction,
          identity,
          observedHead,
          attachmentOrigins,
          stateCode,
        );
      if (prepared === null || attachmentOriginsChecksum === null) return null;
      const receipt = objectFreeze({
        attachmentOriginCount: attachmentOrigins.count,
        attachmentOriginsChecksum,
        expectedHeadChecksum: canonicalHeadChecksum(
          observedHead.head,
          stateCode,
        ),
        preparedOperationCount: prepared.count,
        preparedProjectionChecksum: prepared.checksum,
      });
      return objectFreeze({
        contractVersion: 1,
        projectionChecksum: digestCanonicalProjection(
          AUTHORITY_PROJECTION_RECEIPT_DOMAIN,
          receipt,
        ),
      });
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
      const observedHead = await readHeadSnapshotInTransaction(
        transaction,
        identity,
        stateCode,
        false,
        expectedHead.contractVersion,
      );
      if (!headEqual(observedHead.head, expectedHead)) return false;
      const operationalContractVersion = runtimeOnly
        ? FILESYSTEM_IMAGE_PROVIDER_STATE_HEAD_CONTRACT_VERSION
        : FILESYSTEM_IMAGE_PROVIDER_STATE_V2_HEAD_CONTRACT_VERSION;
      ensure(
        expectedHead.contractVersion === operationalContractVersion &&
          nextHead.contractVersion === operationalContractVersion,
        stateCode,
      );
      requireExpectedOperationIndex(observedHead, expectedHead, stateCode);
      const advanced = await compareHeadInTransaction(
        transaction,
        identity,
        expectedHead,
        nextHead,
        observedHead,
        stateCode,
      );
      if (!advanced) return false;
      if (transition.type !== "rotate-v1") {
        const currentStorageState =
          await readLatestCommittedStorageStateInTransaction(
            transaction,
            identity,
            transition.material.record.storageId,
            observedHead,
            stateCode,
          );
        ensure(
          canonicalEqual(
            currentStorageState,
            transition.material.record.storageStateBefore,
          ),
          stateCode,
        );
      }
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
  objectFreeze(readPreparedOperationsPage);
  objectFreeze(compareProjection);
  objectFreeze(compareAndAdvance);
  return runtimeOnly
    ? objectFreeze({
        contractVersion:
          POSTGRES_FILESYSTEM_IMAGE_PROVIDER_STATE_RUNTIME_AUTHORITY_CONTRACT_VERSION,
        readHead,
        readOperation,
        readOperationsPage,
        readPreparedOperationsPage,
        compareProjection,
        compareAndAdvance,
      })
    : objectFreeze({
        contractVersion:
          POSTGRES_FILESYSTEM_IMAGE_PROVIDER_STATE_AUTHORITY_CONTRACT_VERSION,
        readHead,
        readOperation,
        readOperationsPage,
        compareAndAdvance,
      });
}

export function createPostgresFilesystemImageProviderStateAuthority(...args) {
  return createAuthoritySurface(args, false);
}

export function createPostgresFilesystemImageProviderStateRuntimeAuthority(
  ...args
) {
  return createAuthoritySurface(args, true);
}

export function createPostgresFilesystemImageProviderStateAdoptionAuthority(
  ...args
) {
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

  const compareAndAdopt = async function compareAndAdopt(...adoptionArgs) {
    ensure(adoptionArgs.length === 1, requestCode);
    const input = normalizeAdoptionRequest(
      adoptionArgs[0],
      identity,
      requestCode,
    );
    try {
      return await runSerializable(store, async (transaction) =>
        await compareAndAdoptInTransaction(
          transaction,
          identity,
          input,
          stateCode,
        ),
      );
    } catch (error) {
      if (
        error instanceof PostgresSerializableStoreError &&
        error.code === "transaction_commit_outcome_uncertain" &&
        error.commitState === "uncertain"
      ) {
        return await resolveAdoptionCommitOutcome(
          store,
          identity,
          input,
          stateCode,
        );
      }
      throw error;
    }
  };

  objectFreeze(compareAndAdopt);
  return objectFreeze({
    contractVersion:
      POSTGRES_FILESYSTEM_IMAGE_PROVIDER_STATE_ADOPTION_AUTHORITY_CONTRACT_VERSION,
    compareAndAdopt,
  });
}

objectFreeze(PostgresFilesystemImageProviderStateAuthorityError.prototype);
objectFreeze(PostgresFilesystemImageProviderStateAuthorityError);
objectFreeze(createPostgresFilesystemImageProviderStateAuthority);
objectFreeze(createPostgresFilesystemImageProviderStateRuntimeAuthority);
objectFreeze(createPostgresFilesystemImageProviderStateAdoptionAuthority);
