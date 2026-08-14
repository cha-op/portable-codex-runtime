import { types as utilTypes } from "node:util";

import {
  FILESYSTEM_IMAGE_PROVIDER_STATE_HEAD_CONTRACT_VERSION,
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
  1;

const MAX_LEDGER_BYTES = 64 * 1024 * 1024;
const OPAQUE_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u;
const DECIMAL_PATTERN = /^(?:0|[1-9][0-9]*)$/u;
const OPTION_KEYS = Object.freeze(["store", "providerId", "anchorId"]);
const ADVANCE_KEYS = Object.freeze(["expectedHead", "nextHead"]);
const HEAD_ROW_KEYS = Object.freeze([
  "provider_id",
  "anchor_id",
  "contract_version",
  "sequence",
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
  "sequence",
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
  "(provider_id, anchor_id, contract_version, sequence,",
  "last_checksum, ledger_bytes)",
  "VALUES ($1, $2, $3, $4, $5, $6::pg_catalog.int8)",
  "ON CONFLICT (provider_id, anchor_id) DO NOTHING",
  `RETURNING ${HEAD_COLUMNS}`,
].join(" ");
const UPDATE_QUERY = [
  "UPDATE session_authority.filesystem_image_provider_heads",
  "SET contract_version = $3, sequence = $4,",
  "last_checksum = $5, ledger_bytes = $6::pg_catalog.int8",
  "WHERE provider_id = $1 AND anchor_id = $2",
  "AND contract_version = $7 AND sequence = $8",
  "AND last_checksum IS NOT DISTINCT FROM $9",
  "AND ledger_bytes = $10::pg_catalog.int8",
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

function genesisHead() {
  return canonicalHead(
    {
      contractVersion: FILESYSTEM_IMAGE_PROVIDER_STATE_HEAD_CONTRACT_VERSION,
      sequence: 0,
      lastChecksum: null,
      ledgerBytes: 0,
    },
    "postgres_filesystem_image_provider_head_anchor_state_invalid",
  );
}

function headEqual(left, right) {
  return (
    left.contractVersion === right.contractVersion &&
    left.sequence === right.sequence &&
    left.lastChecksum === right.lastChecksum &&
    left.ledgerBytes === right.ledgerBytes
  );
}

function canonicalLedgerBytes(value, code) {
  ensure(typeof value === "string" && regexpTest(DECIMAL_PATTERN, value), code);
  let parsed;
  try {
    parsed = BigIntConstructor(value);
  } catch {
    fail(code);
  }
  ensure(parsed <= BigIntConstructor(MAX_LEDGER_BYTES), code);
  const number = NumberConstructor(parsed);
  ensure(numberIsSafeInteger(number), code);
  return number;
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
      sequence: row.sequence,
      lastChecksum: row.last_checksum,
      ledgerBytes: canonicalLedgerBytes(row.ledger_bytes, code),
    },
    code,
  );
  ensure(head.sequence > 0, code);
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
    head.sequence,
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
    const genesis = input.expectedHead.sequence === 0;
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
          input.expectedHead.contractVersion,
          input.expectedHead.sequence,
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
      nextHead.sequence === expectedHead.sequence + 1 &&
        nextHead.ledgerBytes > expectedHead.ledgerBytes &&
        nextHead.lastChecksum !== null,
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
