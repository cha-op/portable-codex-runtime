import { types as utilTypes } from "node:util";

import {
  FILESYSTEM_IMAGE_PROVIDER_STATE_V2_HEAD_CONTRACT_VERSION,
  filesystemImageProviderStateHeadChecksum,
  normalizeFilesystemImageProviderStateHead,
} from "./filesystem-image-provider-state.mjs";
import {
  PostgresSerializableStore,
  isPostgresSerializableStore,
} from "./postgres-serializable-store.mjs";

const arrayIncludesIntrinsic = Array.prototype.includes;
const arrayIsArrayIntrinsic = Array.isArray;
const arrayPrototype = Array.prototype;
const ArrayConstructor = Array;
const BigIntConstructor = BigInt;
const ErrorConstructor = Error;
const isProxyValue = utilTypes.isProxy;
const numberIsSafeInteger = Number.isSafeInteger;
const NumberConstructor = Number;
const objectCreateIntrinsic = Object.create;
const objectDefinePropertyIntrinsic = Object.defineProperty;
const objectFreezeIntrinsic = Object.freeze;
const objectGetOwnPropertyDescriptorIntrinsic =
  Object.getOwnPropertyDescriptor;
const objectGetPrototypeOfIntrinsic = Object.getPrototypeOf;
const objectHasOwnIntrinsic = Object.hasOwn;
const objectPrototype = Object.prototype;
const ObjectConstructor = Object;
const reflectApplyIntrinsic = Reflect.apply;
const reflectOwnKeysIntrinsic = Reflect.ownKeys;
const regexpTestIntrinsic = RegExp.prototype.test;
const runSerializableIntrinsic =
  PostgresSerializableStore.prototype.runSerializable;
const StringConstructor = String;
const TypeErrorConstructor = TypeError;

export const POSTGRES_FILESYSTEM_IMAGE_PROVIDER_HEAD_ANCHOR_CONTRACT_VERSION =
  2;

const MAX_CHECKPOINT_FRAME_COUNT = 4_294_967_295;
const MAX_CHECKPOINT_BYTES = 9_007_199_254_740_991;
const MAX_FRAME_COUNT = 65_535;
const MAX_LEDGER_BYTES = 64 * 1024 * 1024;
const OPAQUE_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u;
const DECIMAL_PATTERN = /^(?:0|[1-9][0-9]*)$/u;
const OPTION_KEYS = Object.freeze(["store", "providerId", "anchorId"]);
const ADVANCE_KEYS = Object.freeze(["expectedHead", "nextHead"]);
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
const ERROR_MESSAGES = Object.freeze({
  invalid_postgres_filesystem_image_provider_head_anchor_options:
    "PostgreSQL filesystem image provider head anchor options are invalid",
  invalid_postgres_filesystem_image_provider_head_anchor_request:
    "PostgreSQL filesystem image provider head anchor request is invalid",
  postgres_filesystem_image_provider_head_anchor_state_invalid:
    "PostgreSQL filesystem image provider head anchor state is invalid",
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
const READ_QUERY = [
  `SELECT ${HEAD_COLUMNS}`,
  "FROM session_authority.filesystem_image_provider_heads",
  "WHERE provider_id = $1 AND anchor_id = $2",
].join(" ");
const INSERT_QUERY = [
  "INSERT INTO session_authority.filesystem_image_provider_heads",
  "(provider_id, anchor_id, contract_version, anchor_revision, generation,",
  "state_revision, base_head_checksum, checkpoint_state_revision,",
  "checkpoint_frame_count, checkpoint_checksum, checkpoint_bytes,",
  "frame_count, last_checksum, ledger_bytes)",
  "VALUES ($1, $2, $3, $4::pg_catalog.numeric, $5::pg_catalog.numeric,",
  "$6::pg_catalog.numeric, $7, $8::pg_catalog.numeric, $9::pg_catalog.int8,",
  "$10, $11::pg_catalog.int8, $12::pg_catalog.int4, $13,",
  "$14::pg_catalog.int8)",
  "ON CONFLICT (provider_id, anchor_id) DO NOTHING",
  `RETURNING ${HEAD_COLUMNS}`,
].join(" ");
const UPDATE_QUERY = [
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

function callIntrinsic(intrinsic, receiver, args) {
  return reflectApplyIntrinsic(intrinsic, receiver, args);
}

function arrayIncludes(value, candidate) {
  return callIntrinsic(arrayIncludesIntrinsic, value, [candidate]);
}

function arrayIsArray(value) {
  return callIntrinsic(arrayIsArrayIntrinsic, ArrayConstructor, [value]);
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
  return callIntrinsic(
    objectGetPrototypeOfIntrinsic,
    ObjectConstructor,
    [value],
  );
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
  const error = new PostgresFilesystemImageProviderHeadAnchorError(code);
  throw error;
}

function ensure(condition, code) {
  if (!condition) fail(code);
}

function ownDataValue(value, key, code) {
  ensure(
    value !== null &&
      (typeof value === "object" || typeof value === "function") &&
      !isProxyValue(value),
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
  return descriptor.value;
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
    ensure(typeof key === "string" && arrayIncludes(expectedKeys, key), code);
    normalized[key] = ownDataValue(value, key, code);
  }
  return normalized;
}

function canonicalOpaqueId(value, code) {
  ensure(
    typeof value === "string" && regexpTest(OPAQUE_ID_PATTERN, value),
    code,
  );
  return value;
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
    return callIntrinsic(
      filesystemImageProviderStateHeadChecksum,
      undefined,
      [value],
    );
  } catch {
    fail(code);
  }
}

function genesisHead() {
  return canonicalHead(
    {
      contractVersion: FILESYSTEM_IMAGE_PROVIDER_STATE_V2_HEAD_CONTRACT_VERSION,
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
    "postgres_filesystem_image_provider_head_anchor_state_invalid",
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
  ensure(numberIsSafeInteger(number), code);
  return number;
}

function decimalSuccessor(previous, next) {
  return (
    BigIntConstructor(next) === BigIntConstructor(previous) + 1n
  );
}

function isNormalAppend(expectedHead, nextHead) {
  return (
    nextHead.contractVersion === expectedHead.contractVersion &&
    decimalSuccessor(expectedHead.anchorRevision, nextHead.anchorRevision) &&
    nextHead.generation === expectedHead.generation &&
    decimalSuccessor(expectedHead.stateRevision, nextHead.stateRevision) &&
    nextHead.baseHeadChecksum === expectedHead.baseHeadChecksum &&
    nextHead.checkpointStateRevision ===
      expectedHead.checkpointStateRevision &&
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
  return (
    nextHead.baseHeadChecksum === canonicalHeadChecksum(expectedHead, code)
  );
}

function rowsFromResult(result, command, code) {
  const observedCommand = ownDataValue(result, "command", code);
  const rowCount = ownDataValue(result, "rowCount", code);
  const rows = ownDataValue(result, "rows", code);
  ensure(
    observedCommand === command &&
      numberIsSafeInteger(rowCount) &&
      rowCount >= 0 &&
      rowCount <= 1 &&
      !isProxyValue(rows) &&
      arrayIsArray(rows) &&
      objectGetPrototypeOf(rows) === arrayPrototype &&
      rows.length === rowCount,
    code,
  );
  for (let index = 0; index < rows.length; index += 1) {
    ownDataValue(
      rows,
      callIntrinsic(StringConstructor, undefined, [index]),
      code,
    );
  }
  return rows;
}

function normalizeHeadRow(value, expectedIdentity, code) {
  const row = exactDataObject(value, HEAD_ROW_KEYS, code);
  ensure(
    canonicalOpaqueId(row.provider_id, code) === expectedIdentity.providerId &&
      canonicalOpaqueId(row.anchor_id, code) === expectedIdentity.anchorId,
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
  ensure(
    head.contractVersion ===
      FILESYSTEM_IMAGE_PROVIDER_STATE_V2_HEAD_CONTRACT_VERSION &&
      !headEqual(head, genesisHead()),
    code,
  );
  return head;
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
    callIntrinsic(StringConstructor, undefined, [head.checkpointFrameCount]),
    head.checkpointChecksum,
    callIntrinsic(StringConstructor, undefined, [head.checkpointBytes]),
    callIntrinsic(StringConstructor, undefined, [head.frameCount]),
    head.lastChecksum,
    callIntrinsic(StringConstructor, undefined, [head.ledgerBytes]),
  ];
}

async function readDurableHead(store, identity) {
  return await runSerializable(store, async (transaction) => {
    const code =
      "postgres_filesystem_image_provider_head_anchor_state_invalid";
    const result = await queryTransaction(
      transaction,
      READ_QUERY,
      [identity.providerId, identity.anchorId],
      code,
    );
    const rows = rowsFromResult(result, "SELECT", code);
    return rows.length === 0
      ? genesisHead()
      : normalizeHeadRow(rows[0], identity, code);
  });
}

async function compareAndAdvanceDurableHead(store, identity, input) {
  return await runSerializable(store, async (transaction) => {
    const code =
      "postgres_filesystem_image_provider_head_anchor_state_invalid";
    const genesis = headEqual(input.expectedHead, genesisHead());
    const text = genesis ? INSERT_QUERY : UPDATE_QUERY;
    const nextValues = headValues(identity, input.nextHead);
    const values = genesis
      ? nextValues
      : [
          nextValues[0],
          nextValues[1],
          nextValues[2],
          nextValues[3],
          nextValues[4],
          nextValues[5],
          nextValues[6],
          nextValues[7],
          nextValues[8],
          nextValues[9],
          nextValues[10],
          nextValues[11],
          nextValues[12],
          nextValues[13],
          input.expectedHead.contractVersion,
          input.expectedHead.anchorRevision,
          input.expectedHead.generation,
          input.expectedHead.stateRevision,
          input.expectedHead.baseHeadChecksum,
          input.expectedHead.checkpointStateRevision,
          callIntrinsic(StringConstructor, undefined, [
            input.expectedHead.checkpointFrameCount,
          ]),
          input.expectedHead.checkpointChecksum,
          callIntrinsic(StringConstructor, undefined, [
            input.expectedHead.checkpointBytes,
          ]),
          callIntrinsic(StringConstructor, undefined, [
            input.expectedHead.frameCount,
          ]),
          input.expectedHead.lastChecksum,
          callIntrinsic(StringConstructor, undefined, [
            input.expectedHead.ledgerBytes,
          ]),
        ];
    const result = await queryTransaction(
      transaction,
      text,
      values,
      code,
    );
    const rows = rowsFromResult(result, genesis ? "INSERT" : "UPDATE", code);
    if (rows.length === 0) return false;
    const stored = normalizeHeadRow(rows[0], identity, code);
    ensure(headEqual(stored, input.nextHead), code);
    return true;
  });
}

export class PostgresFilesystemImageProviderHeadAnchorError extends ErrorConstructor {
  constructor(code) {
    if (typeof code !== "string" || !objectHasOwn(ERROR_MESSAGES, code)) {
      throw new TypeErrorConstructor(
        "Unsupported PostgreSQL filesystem image provider head anchor error",
      );
    }
    const message = ERROR_MESSAGES[code];
    super(message);
    objectDefineProperty(this, "name", {
      enumerable: false,
      value: "PostgresFilesystemImageProviderHeadAnchorError",
    });
    objectDefineProperty(this, "code", { enumerable: true, value: code });
    objectDefineProperty(this, "retryable", {
      enumerable: true,
      value: false,
    });
    objectFreeze(this);
  }
}

/**
 * Creates a durable CAS anchor scoped only by provider and anchor IDs.
 * The PostgreSQL store must be independent of the replaceable ledger path.
 */
export function createPostgresFilesystemImageProviderHeadAnchor(...args) {
  const optionCode =
    "invalid_postgres_filesystem_image_provider_head_anchor_options";
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
    "invalid_postgres_filesystem_image_provider_head_anchor_request";

  const readHead = async function readHead(...readArgs) {
    ensure(readArgs.length === 0, requestCode);
    return await readDurableHead(store, identity);
  };

  const compareAndAdvance = async function compareAndAdvance(...advanceArgs) {
    ensure(advanceArgs.length === 1, requestCode);
    const request = exactDataObject(advanceArgs[0], ADVANCE_KEYS, requestCode);
    const expectedHead = canonicalHead(request.expectedHead, requestCode);
    const nextHead = canonicalHead(request.nextHead, requestCode);
    ensure(
      expectedHead.contractVersion ===
        FILESYSTEM_IMAGE_PROVIDER_STATE_V2_HEAD_CONTRACT_VERSION &&
        nextHead.contractVersion ===
          FILESYSTEM_IMAGE_PROVIDER_STATE_V2_HEAD_CONTRACT_VERSION &&
        (isNormalAppend(expectedHead, nextHead) ||
          isPureRotation(expectedHead, nextHead, requestCode)),
      requestCode,
    );
    return await compareAndAdvanceDurableHead(
      store,
      identity,
      objectFreeze({ expectedHead, nextHead }),
    );
  };

  objectFreeze(readHead);
  objectFreeze(compareAndAdvance);
  return objectFreeze({ readHead, compareAndAdvance });
}

objectFreeze(PostgresFilesystemImageProviderHeadAnchorError.prototype);
objectFreeze(PostgresFilesystemImageProviderHeadAnchorError);
objectFreeze(createPostgresFilesystemImageProviderHeadAnchor);
