import { Buffer } from "node:buffer";
import { Hash, createHash as createHashExport } from "node:crypto";
import { isProxy as isProxyValue } from "node:util/types";

import {
  PostgresSerializableStore,
  PostgresSerializableStoreError,
  isPostgresSerializableStore,
} from "./postgres-serializable-store.mjs";
import {
  assertAtomicCrashCaptureRequest,
  assertAtomicCrashCaptureResult,
} from "./session-storage-contracts.mjs";

export const POSTGRES_ATOMIC_CRASH_CAPTURE_CATALOGUE_CONTRACT_VERSION = 1;

const MAX_REQUEST_JSON_BYTES = 262_144;
const MAX_PROVIDER_BINDING_JSON_BYTES = 65_536;
const MAX_RESULT_JSON_BYTES = 131_072;
const MAX_JSON_DEPTH = 24;
const MAX_JSON_NODES = 8_192;
const MAX_JSON_OBJECT_KEYS = 1_024;
const JSONB_INCOMPATIBLE_STRING_PATTERN = /\u0000|[\uD800-\uDFFF]/u;
const SHA256_PATTERN = /^[0-9a-f]{64}$/u;
const UINT64_PATTERN = /^(?:[1-9][0-9]{0,19})$/u;
const UINT64_MAX = 18_446_744_073_709_551_615n;

const FACTORY_KEYS = Object.freeze(["store"]);
const CLAIM_KEYS = Object.freeze(["providerBinding", "request"]);
const MARK_KEYS = Object.freeze(["dispatchClaim"]);
const COMMIT_KEYS = Object.freeze(["dispatchClaim", "result"]);
const READ_KEYS = Object.freeze(["request"]);
const ROW_KEYS = Object.freeze([
  "artifact_id",
  "backend_id",
  "capture_attempt_id",
  "checkpoint_id",
  "claimed_at",
  "committed_at",
  "contract_version",
  "operation_id",
  "provider_binding",
  "provider_binding_json",
  "provider_binding_sha256",
  "request_json",
  "request_sha256",
  "result_json",
  "result_sha256",
  "session_id",
  "source_fencing_epoch",
  "state",
  "storage_id",
  "uncertain_at",
]);

const COLUMNS = [
  "capture_attempt_id",
  "operation_id",
  "checkpoint_id",
  "artifact_id",
  "contract_version",
  "backend_id",
  "session_id",
  "storage_id",
  "source_fencing_epoch::pg_catalog.text AS source_fencing_epoch",
  "request_json",
  "request_sha256",
  "provider_binding",
  "provider_binding_json",
  "provider_binding_sha256",
  "state",
  "result_json",
  "result_sha256",
  "claimed_at",
  "uncertain_at",
  "committed_at",
].join(", ");

const INSERT_QUERY = [
  "INSERT INTO session_authority.atomic_crash_captures",
  "(capture_attempt_id, operation_id, checkpoint_id, artifact_id,",
  "contract_version, backend_id, session_id, storage_id,",
  "source_fencing_epoch, request_json, request_sha256, provider_binding,",
  "provider_binding_json, provider_binding_sha256, state, result_json,",
  "result_sha256)",
  "VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::pg_catalog.numeric,",
  "$10::pg_catalog.jsonb, $11, $12::pg_catalog.jsonb, $12, $13,",
  "'starting', NULL, NULL)",
  "ON CONFLICT DO NOTHING",
  "RETURNING capture_attempt_id",
].join(" ");

const SELECT_BY_IDENTITIES_QUERY = [
  `SELECT ${COLUMNS}`,
  "FROM session_authority.atomic_crash_captures",
  "WHERE capture_attempt_id = $1 OR operation_id = $2",
  "OR checkpoint_id = $3 OR artifact_id = $4",
].join(" ");

const SELECT_BY_IDENTITIES_FOR_UPDATE_QUERY =
  `${SELECT_BY_IDENTITIES_QUERY} FOR UPDATE`;

const MARK_UNCERTAIN_QUERY = [
  "UPDATE session_authority.atomic_crash_captures",
  "SET state = 'uncertain'",
  "WHERE capture_attempt_id = $1 AND operation_id = $2",
  "AND checkpoint_id = $3 AND artifact_id = $4",
  "AND request_sha256 = $5 AND provider_binding_sha256 = $6",
  "AND state = 'starting'",
  `RETURNING ${COLUMNS}`,
].join(" ");

const COMMIT_RESULT_QUERY = [
  "UPDATE session_authority.atomic_crash_captures",
  "SET state = 'committed', result_json = $7::pg_catalog.jsonb,",
  "result_sha256 = $8",
  "WHERE capture_attempt_id = $1 AND operation_id = $2",
  "AND checkpoint_id = $3 AND artifact_id = $4",
  "AND request_sha256 = $5 AND provider_binding_sha256 = $6",
  "AND state IN ('starting', 'uncertain')",
  `RETURNING ${COLUMNS}`,
].join(" ");

const ERROR_MESSAGES = Object.freeze({
  invalid_postgres_atomic_crash_capture_catalogue_options:
    "PostgreSQL atomic crash-capture catalogue options are invalid",
  invalid_postgres_atomic_crash_capture_catalogue_request:
    "PostgreSQL atomic crash-capture catalogue request is invalid",
  invalid_postgres_atomic_crash_capture_dispatch_claim:
    "PostgreSQL atomic crash-capture dispatch claim is invalid or consumed",
  postgres_atomic_crash_capture_catalogue_conflict:
    "PostgreSQL atomic crash-capture catalogue identity conflicts with durable state",
  postgres_atomic_crash_capture_catalogue_outcome_uncertain:
    "PostgreSQL atomic crash-capture catalogue outcome is uncertain",
  postgres_atomic_crash_capture_catalogue_state_invalid:
    "PostgreSQL atomic crash-capture catalogue state is invalid",
});

const ArrayConstructor = Array;
const arrayIncludesIntrinsic = Array.prototype.includes;
const arrayIsArray = Array.isArray;
const arrayPrototype = Array.prototype;
const BigIntConstructor = BigInt;
const bufferByteLengthIntrinsic = Buffer.byteLength;
const createHashIntrinsic = createHashExport;
const DateConstructor = Date;
const dateGetTimeIntrinsic = Date.prototype.getTime;
const dateParseIntrinsic = Date.parse;
const dateToISOStringIntrinsic = Date.prototype.toISOString;
const hashDigestIntrinsic = Hash.prototype.digest;
const hashUpdateIntrinsic = Hash.prototype.update;
const JSONValue = JSON;
const jsonStringifyIntrinsic = JSON.stringify;
const numberIsFinite = Number.isFinite;
const numberIsSafeInteger = Number.isSafeInteger;
const objectCreate = Object.create;
const objectDefineProperty = Object.defineProperty;
const objectFreeze = Object.freeze;
const objectGetOwnPropertyDescriptor = Object.getOwnPropertyDescriptor;
const objectGetPrototypeOf = Object.getPrototypeOf;
const objectHasOwn = Object.hasOwn;
const objectIs = Object.is;
const objectPrototype = Object.prototype;
const reflectApply = Reflect.apply;
const reflectOwnKeys = Reflect.ownKeys;
const regexpExecIntrinsic = RegExp.prototype.exec;
const runSerializableIntrinsic =
  PostgresSerializableStore.prototype.runSerializable;
const StringConstructor = String;
const TypeErrorConstructor = TypeError;
const weakMapDeleteIntrinsic = WeakMap.prototype.delete;
const weakMapGetIntrinsic = WeakMap.prototype.get;
const weakMapHasIntrinsic = WeakMap.prototype.has;
const weakMapSetIntrinsic = WeakMap.prototype.set;
const WeakMapConstructor = WeakMap;
const weakSetAddIntrinsic = WeakSet.prototype.add;
const weakSetDeleteIntrinsic = WeakSet.prototype.delete;
const weakSetHasIntrinsic = WeakSet.prototype.has;
const WeakSetConstructor = WeakSet;

const catalogueBrands = new WeakSetConstructor();
const catalogueErrors = new WeakSetConstructor();
const dispatchClaims = new WeakMapConstructor();
const VISIBILITY_RETRY = objectFreeze(objectCreate(null));

function callIntrinsic(intrinsic, receiver, args) {
  return reflectApply(intrinsic, receiver, args);
}

function arrayIncludes(value, candidate) {
  return callIntrinsic(arrayIncludesIntrinsic, value, [candidate]);
}

function regexpTest(pattern, value) {
  return callIntrinsic(regexpExecIntrinsic, pattern, [value]) !== null;
}

function fail(code) {
  throw new PostgresAtomicCrashCaptureCatalogueError(code);
}

function ensure(condition, code) {
  if (!condition) fail(code);
}

function isCatalogueError(error) {
  try {
    return (
      error !== null &&
      !isProxyValue(error) &&
      callIntrinsic(weakSetHasIntrinsic, catalogueErrors, [error])
    );
  } catch {
    return false;
  }
}

function exactFrozenRecord(value) {
  const result = objectCreate(null);
  const keys = reflectOwnKeys(value);
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
    objectDefineProperty(result, key, {
      configurable: false,
      enumerable: true,
      value: value[key],
      writable: false,
    });
  }
  return objectFreeze(result);
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
  const result = objectCreate(null);
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
    ensure(
      typeof key === "string" && arrayIncludes(expectedKeys, key),
      code,
    );
    result[key] = ownDataValue(value, key, code);
  }
  return result;
}

function sortedStringKeys(value, code) {
  let keys;
  try {
    keys = reflectOwnKeys(value);
  } catch {
    fail(code);
  }
  ensure(keys.length <= MAX_JSON_OBJECT_KEYS, code);
  for (let index = 0; index < keys.length; index += 1) {
    ensure(typeof keys[index] === "string", code);
  }
  for (let index = 1; index < keys.length; index += 1) {
    const selected = keys[index];
    let position = index;
    while (position > 0 && selected < keys[position - 1]) {
      keys[position] = keys[position - 1];
      position -= 1;
    }
    keys[position] = selected;
  }
  return keys;
}

function canonicalJsonValue(value, code, state, depth) {
  ensure(depth <= MAX_JSON_DEPTH && state.nodes < MAX_JSON_NODES, code);
  state.nodes += 1;
  if (value === null || typeof value !== "object") {
    ensure(
      value === null ||
        typeof value === "string" ||
        typeof value === "boolean" ||
        (typeof value === "number" && numberIsFinite(value)),
      code,
    );
    if (typeof value === "string") {
      ensure(
        !regexpTest(JSONB_INCOMPATIBLE_STRING_PATTERN, value) &&
        callIntrinsic(bufferByteLengthIntrinsic, Buffer, [value, "utf8"]) <=
          state.maxBytes,
        code,
      );
    }
    return typeof value === "number" && objectIs(value, -0) ? 0 : value;
  }
  ensure(
    !isProxyValue(value) &&
      !callIntrinsic(weakSetHasIntrinsic, state.seen, [value]),
    code,
  );
  callIntrinsic(weakSetAddIntrinsic, state.seen, [value]);
  let result;
  if (arrayIsArray(value)) {
    let prototype;
    let keys;
    let lengthDescriptor;
    try {
      prototype = objectGetPrototypeOf(value);
      keys = reflectOwnKeys(value);
      lengthDescriptor = objectGetOwnPropertyDescriptor(value, "length");
    } catch {
      fail(code);
    }
    ensure(
      prototype === arrayPrototype &&
        numberIsSafeInteger(value.length) &&
        value.length >= 0 &&
        keys.length === value.length + 1 &&
        lengthDescriptor !== undefined &&
        objectHasOwn(lengthDescriptor, "value") &&
        lengthDescriptor.value === value.length &&
        value.length <= MAX_JSON_NODES - state.nodes,
      code,
    );
    result = new ArrayConstructor(value.length);
    for (let index = 0; index < value.length; index += 1) {
      const key = callIntrinsic(StringConstructor, undefined, [index]);
      const descriptor = objectGetOwnPropertyDescriptor(value, key);
      ensure(
        descriptor?.enumerable === true && objectHasOwn(descriptor, "value"),
        code,
      );
      result[index] = canonicalJsonValue(
        descriptor.value,
        code,
        state,
        depth + 1,
      );
    }
  } else {
    let prototype;
    try {
      prototype = objectGetPrototypeOf(value);
    } catch {
      fail(code);
    }
    ensure(prototype === objectPrototype || prototype === null, code);
    const keys = sortedStringKeys(value, code);
    ensure(keys.length <= MAX_JSON_NODES - state.nodes, code);
    result = objectCreate(null);
    for (let index = 0; index < keys.length; index += 1) {
      const key = keys[index];
      ensure(
        !regexpTest(JSONB_INCOMPATIBLE_STRING_PATTERN, key) &&
        callIntrinsic(bufferByteLengthIntrinsic, Buffer, [key, "utf8"]) <=
          state.maxBytes,
        code,
      );
      const child = ownDataValue(value, key, code);
      objectDefineProperty(result, key, {
        configurable: false,
        enumerable: true,
        value: canonicalJsonValue(child, code, state, depth + 1),
        writable: false,
      });
    }
  }
  callIntrinsic(weakSetDeleteIntrinsic, state.seen, [value]);
  return objectFreeze(result);
}

function canonicalJsonObject(value, code, maxBytes) {
  const canonical = canonicalJsonValue(
    value,
    code,
    {
      maxBytes,
      nodes: 0,
      seen: new WeakSetConstructor(),
    },
    0,
  );
  ensure(
    canonical !== null &&
      typeof canonical === "object" &&
      !arrayIsArray(canonical),
    code,
  );
  let serialized;
  try {
    serialized = callIntrinsic(jsonStringifyIntrinsic, JSONValue, [canonical]);
  } catch {
    fail(code);
  }
  ensure(
    typeof serialized === "string" &&
      callIntrinsic(bufferByteLengthIntrinsic, Buffer, [serialized, "utf8"]) <=
        maxBytes,
    code,
  );
  return exactFrozenRecord({ canonical, serialized });
}

function sha256(value, code) {
  let hash;
  let digest;
  try {
    hash = callIntrinsic(createHashIntrinsic, undefined, ["sha256"]);
    callIntrinsic(hashUpdateIntrinsic, hash, [value, "utf8"]);
    digest = callIntrinsic(hashDigestIntrinsic, hash, ["hex"]);
  } catch {
    fail(code);
  }
  ensure(typeof digest === "string" && regexpTest(SHA256_PATTERN, digest), code);
  return digest;
}

function canonicalUint64(value, code) {
  ensure(typeof value === "string" && regexpTest(UINT64_PATTERN, value), code);
  let parsed;
  try {
    parsed = callIntrinsic(BigIntConstructor, undefined, [value]);
  } catch {
    fail(code);
  }
  ensure(parsed <= UINT64_MAX, code);
  return value;
}

function canonicalTimestamp(value, code) {
  let milliseconds;
  try {
    milliseconds =
      typeof value === "string"
        ? callIntrinsic(dateParseIntrinsic, DateConstructor, [value])
        : value !== null &&
            typeof value === "object" &&
            !isProxyValue(value)
          ? callIntrinsic(dateGetTimeIntrinsic, value, [])
          : Number.NaN;
  } catch {
    fail(code);
  }
  ensure(numberIsFinite(milliseconds), code);
  return exactFrozenRecord({
    milliseconds,
    value: callIntrinsic(
      dateToISOStringIntrinsic,
      new DateConstructor(milliseconds),
      [],
    ),
  });
}

function normalizedRequest(value, code) {
  let request;
  try {
    request = assertAtomicCrashCaptureRequest(value);
  } catch {
    fail(code);
  }
  const json = canonicalJsonObject(request, code, MAX_REQUEST_JSON_BYTES);
  return exactFrozenRecord({
    artifactId: request.checkpoint.artifactId,
    backendId: request.storageRef.backendId,
    captureAttemptId: request.captureAttemptId,
    checkpointId: request.checkpoint.checkpointId,
    contractVersion: request.contractVersion,
    operationId: request.mutationRequest.operationId,
    request: json.canonical,
    requestJson: json.serialized,
    requestSha256: sha256(json.serialized, code),
    sessionId: request.storageRef.sessionId,
    sourceFencingEpoch: request.checkpoint.sourceFencingEpoch,
    storageId: request.storageRef.storageId,
  });
}

function normalizedProviderBinding(value, code) {
  const json = canonicalJsonObject(
    value,
    code,
    MAX_PROVIDER_BINDING_JSON_BYTES,
  );
  return exactFrozenRecord({
    providerBinding: json.canonical,
    providerBindingJson: json.serialized,
    providerBindingSha256: sha256(json.serialized, code),
  });
}

function normalizedClaimInput(value, code) {
  const input = exactDataObject(value, CLAIM_KEYS, code);
  return exactFrozenRecord({
    ...normalizedRequest(input.request, code),
    ...normalizedProviderBinding(input.providerBinding, code),
  });
}

function normalizedReadInput(value, code) {
  const input = exactDataObject(value, READ_KEYS, code);
  return normalizedRequest(input.request, code);
}

function rowsFromResult(result, maximum, code) {
  ensure(
    result !== null && typeof result === "object" && !isProxyValue(result),
    code,
  );
  const rows = ownDataValue(result, "rows", code);
  let prototype;
  try {
    prototype = objectGetPrototypeOf(rows);
  } catch {
    fail(code);
  }
  ensure(
    !isProxyValue(rows) &&
      arrayIsArray(rows) &&
      prototype === arrayPrototype &&
      numberIsSafeInteger(rows.length) &&
      rows.length <= maximum,
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

function normalizedStoredResult(value, request, code) {
  let result;
  try {
    result = assertAtomicCrashCaptureResult(value, { request });
  } catch {
    fail(code);
  }
  const json = canonicalJsonObject(result, code, MAX_RESULT_JSON_BYTES);
  return exactFrozenRecord({
    result: json.canonical,
    resultJson: json.serialized,
    resultSha256: sha256(json.serialized, code),
  });
}

function normalizedRow(value, code) {
  const row = exactDataObject(value, ROW_KEYS, code);
  ensure(
    row.contract_version ===
      POSTGRES_ATOMIC_CRASH_CAPTURE_CATALOGUE_CONTRACT_VERSION &&
      typeof row.capture_attempt_id === "string" &&
      typeof row.operation_id === "string" &&
      typeof row.checkpoint_id === "string" &&
      typeof row.artifact_id === "string" &&
      typeof row.backend_id === "string" &&
      typeof row.session_id === "string" &&
      typeof row.storage_id === "string" &&
      typeof row.request_sha256 === "string" &&
      regexpTest(SHA256_PATTERN, row.request_sha256) &&
      typeof row.provider_binding_json === "string" &&
      typeof row.provider_binding_sha256 === "string" &&
      regexpTest(SHA256_PATTERN, row.provider_binding_sha256),
    code,
  );
  const sourceFencingEpoch = canonicalUint64(row.source_fencing_epoch, code);
  const claimedAt = canonicalTimestamp(row.claimed_at, code);
  const uncertainAt =
    row.uncertain_at === null ? null : canonicalTimestamp(row.uncertain_at, code);
  const committedAt =
    row.committed_at === null ? null : canonicalTimestamp(row.committed_at, code);
  const request = normalizedRequest(row.request_json, code);
  const binding = normalizedProviderBinding(row.provider_binding, code);
  ensure(
    request.captureAttemptId === row.capture_attempt_id &&
      request.operationId === row.operation_id &&
      request.checkpointId === row.checkpoint_id &&
      request.artifactId === row.artifact_id &&
      request.backendId === row.backend_id &&
      request.sessionId === row.session_id &&
      request.storageId === row.storage_id &&
      request.sourceFencingEpoch === sourceFencingEpoch &&
      request.requestSha256 === row.request_sha256 &&
      binding.providerBindingJson === row.provider_binding_json &&
      binding.providerBindingSha256 === row.provider_binding_sha256,
    code,
  );
  let result = null;
  let resultJson = null;
  let resultSha256 = null;
  if (row.state === "committed") {
    ensure(
      row.result_json !== null &&
        typeof row.result_sha256 === "string" &&
        regexpTest(SHA256_PATTERN, row.result_sha256) &&
        committedAt !== null &&
        committedAt.milliseconds >= claimedAt.milliseconds &&
        (uncertainAt === null ||
          (uncertainAt.milliseconds >= claimedAt.milliseconds &&
            committedAt.milliseconds >= uncertainAt.milliseconds)),
      code,
    );
    const normalized = normalizedStoredResult(
      row.result_json,
      request.request,
      code,
    );
    ensure(normalized.resultSha256 === row.result_sha256, code);
    result = normalized.result;
    resultJson = normalized.resultJson;
    resultSha256 = normalized.resultSha256;
  } else {
    ensure(
      (row.state === "starting" || row.state === "uncertain") &&
        row.result_json === null &&
        row.result_sha256 === null &&
        committedAt === null &&
        (row.state === "starting"
          ? uncertainAt === null
          : uncertainAt !== null &&
            uncertainAt.milliseconds >= claimedAt.milliseconds),
      code,
    );
  }
  return exactFrozenRecord({
    ...request,
    ...binding,
    result,
    resultJson,
    resultSha256,
    state: row.state,
    claimedAt: claimedAt.value,
    committedAt: committedAt?.value ?? null,
    uncertainAt: uncertainAt?.value ?? null,
  });
}

function rowMatchesRequest(row, input) {
  return (
    row.captureAttemptId === input.captureAttemptId &&
    row.operationId === input.operationId &&
    row.checkpointId === input.checkpointId &&
    row.artifactId === input.artifactId &&
    row.backendId === input.backendId &&
    row.sessionId === input.sessionId &&
    row.storageId === input.storageId &&
    row.sourceFencingEpoch === input.sourceFencingEpoch &&
    row.requestSha256 === input.requestSha256 &&
    row.requestJson === input.requestJson
  );
}

function rowMatchesBinding(row, input) {
  return (
    row.providerBindingSha256 === input.providerBindingSha256 &&
    row.providerBindingJson === input.providerBindingJson
  );
}

function rowMatchesResult(row, result) {
  return (
    row.resultSha256 === result.resultSha256 &&
    row.resultJson === result.resultJson
  );
}

function committedProjection(row) {
  ensure(
    row.state === "committed" &&
      row.result !== null &&
      row.providerBinding !== null,
    "postgres_atomic_crash_capture_catalogue_state_invalid",
  );
  return exactFrozenRecord({
    outcome: "committed",
    providerBinding: row.providerBinding,
    result: row.result,
  });
}

function unknownProjection() {
  return exactFrozenRecord({ outcome: "unknown" });
}

function uncertainProjection() {
  return exactFrozenRecord({ outcome: "uncertain" });
}

function identityValues(input) {
  return objectFreeze([
    input.captureAttemptId,
    input.operationId,
    input.checkpointId,
    input.artifactId,
  ]);
}

function insertValues(input) {
  return objectFreeze([
    input.captureAttemptId,
    input.operationId,
    input.checkpointId,
    input.artifactId,
    input.contractVersion,
    input.backendId,
    input.sessionId,
    input.storageId,
    input.sourceFencingEpoch,
    input.requestJson,
    input.requestSha256,
    input.providerBindingJson,
    input.providerBindingSha256,
  ]);
}

function transitionValues(input) {
  return objectFreeze([
    input.captureAttemptId,
    input.operationId,
    input.checkpointId,
    input.artifactId,
    input.requestSha256,
    input.providerBindingSha256,
  ]);
}

function runSerializable(store, callback) {
  return callIntrinsic(runSerializableIntrinsic, store, [callback]);
}

function queryTransaction(transaction, text, values, code) {
  const query = ownDataValue(transaction, "query", code);
  ensure(typeof query === "function" && !isProxyValue(query), code);
  return callIntrinsic(query, transaction, [text, values]);
}

async function selectRows(transaction, input, forUpdate) {
  const code = "postgres_atomic_crash_capture_catalogue_state_invalid";
  return rowsFromResult(
    await queryTransaction(
      transaction,
      forUpdate
        ? SELECT_BY_IDENTITIES_FOR_UPDATE_QUERY
        : SELECT_BY_IDENTITIES_QUERY,
      identityValues(input),
      code,
    ),
    4,
    code,
  );
}

function exactObservedRow(rows, input, requireBinding) {
  if (rows.length === 0) return null;
  ensure(
    rows.length === 1,
    "postgres_atomic_crash_capture_catalogue_conflict",
  );
  const row = normalizedRow(
    rows[0],
    "postgres_atomic_crash_capture_catalogue_state_invalid",
  );
  ensure(
    rowMatchesRequest(row, input) &&
      (!requireBinding || rowMatchesBinding(row, input)),
    "postgres_atomic_crash_capture_catalogue_conflict",
  );
  return row;
}

function observedProjection(row) {
  if (row === null || row.state === "starting" || row.state === "uncertain") {
    return unknownProjection();
  }
  return committedProjection(row);
}

async function claimTransaction(store, input) {
  return runSerializable(store, async (transaction) => {
    const code = "postgres_atomic_crash_capture_catalogue_state_invalid";
    const insertRows = rowsFromResult(
      await queryTransaction(transaction, INSERT_QUERY, insertValues(input), code),
      1,
      code,
    );
    if (insertRows.length === 1) {
      const inserted = exactDataObject(
        insertRows[0],
        ["capture_attempt_id"],
        code,
      );
      ensure(inserted.capture_attempt_id === input.captureAttemptId, code);
      return exactFrozenRecord({ inserted: true, projection: null });
    }
    const row = exactObservedRow(await selectRows(transaction, input, true), input, true);
    if (row === null) throw VISIBILITY_RETRY;
    return exactFrozenRecord({
      inserted: false,
      projection: observedProjection(row),
    });
  });
}

async function readRowTransaction(store, input, requireBinding) {
  return runSerializable(store, async (transaction) =>
    exactObservedRow(
      await selectRows(transaction, input, false),
      input,
      requireBinding,
    ),
  );
}

async function markUncertainTransaction(store, input) {
  return runSerializable(store, async (transaction) => {
    const code = "postgres_atomic_crash_capture_catalogue_state_invalid";
    const updated = rowsFromResult(
      await queryTransaction(
        transaction,
        MARK_UNCERTAIN_QUERY,
        transitionValues(input),
        code,
      ),
      1,
      code,
    );
    if (updated.length === 1) {
      const row = normalizedRow(updated[0], code);
      ensure(
        rowMatchesRequest(row, input) &&
          rowMatchesBinding(row, input) &&
          row.state === "uncertain",
        code,
      );
      return uncertainProjection();
    }
    const row = exactObservedRow(await selectRows(transaction, input, true), input, true);
    ensure(row !== null, code);
    if (row.state === "uncertain") return uncertainProjection();
    fail(code);
  });
}

async function commitResultTransaction(store, input, result) {
  return runSerializable(store, async (transaction) => {
    const code = "postgres_atomic_crash_capture_catalogue_state_invalid";
    const values = objectFreeze([
      input.captureAttemptId,
      input.operationId,
      input.checkpointId,
      input.artifactId,
      input.requestSha256,
      input.providerBindingSha256,
      result.resultJson,
      result.resultSha256,
    ]);
    const updated = rowsFromResult(
      await queryTransaction(
        transaction,
        COMMIT_RESULT_QUERY,
        values,
        code,
      ),
      1,
      code,
    );
    if (updated.length === 1) {
      const row = normalizedRow(updated[0], code);
      ensure(
        rowMatchesRequest(row, input) &&
          rowMatchesBinding(row, input) &&
          rowMatchesResult(row, result),
        code,
      );
      return committedProjection(row);
    }
    const row = exactObservedRow(await selectRows(transaction, input, true), input, true);
    ensure(row !== null, code);
    if (row.state === "committed") {
      ensure(
        rowMatchesResult(row, result),
        "postgres_atomic_crash_capture_catalogue_conflict",
      );
      return committedProjection(row);
    }
    fail(code);
  });
}

function storeCommitState(error) {
  if (
    error === null ||
    typeof error !== "object" ||
    isProxyValue(error)
  ) {
    return null;
  }
  let prototype;
  try {
    prototype = objectGetPrototypeOf(error);
  } catch {
    return null;
  }
  if (prototype !== PostgresSerializableStoreError.prototype) return null;
  let commitState;
  try {
    commitState = ownDataValue(
      error,
      "commitState",
      "postgres_atomic_crash_capture_catalogue_outcome_uncertain",
    );
  } catch {
    return null;
  }
  return commitState === "not-committed" || commitState === "uncertain"
    ? commitState
    : null;
}

function newDispatchProjection(owner, input) {
  const dispatchClaim = objectFreeze(objectCreate(null));
  callIntrinsic(weakMapSetIntrinsic, dispatchClaims, [
    dispatchClaim,
    exactFrozenRecord({ input, owner }),
  ]);
  return exactFrozenRecord({ dispatchClaim, outcome: "dispatch" });
}

function consumeDispatchClaim(value, owner) {
  ensure(
    value !== null &&
      typeof value === "object" &&
      !isProxyValue(value) &&
      callIntrinsic(weakMapHasIntrinsic, dispatchClaims, [value]),
    "invalid_postgres_atomic_crash_capture_dispatch_claim",
  );
  const state = callIntrinsic(weakMapGetIntrinsic, dispatchClaims, [value]);
  ensure(
    state !== undefined && state.owner === owner,
    "invalid_postgres_atomic_crash_capture_dispatch_claim",
  );
  callIntrinsic(weakMapDeleteIntrinsic, dispatchClaims, [value]);
  return state.input;
}

async function claimWithRecovery(store, owner, input) {
  let attempt;
  try {
    attempt = await claimTransaction(store, input);
  } catch (error) {
    if (isCatalogueError(error)) throw error;
    if (error === VISIBILITY_RETRY) {
      try {
        attempt = await claimTransaction(store, input);
      } catch (retryError) {
        if (isCatalogueError(retryError)) throw retryError;
        error = retryError;
      }
      if (attempt !== undefined) {
        return attempt.inserted
          ? newDispatchProjection(owner, input)
          : attempt.projection;
      }
    }
    const commitState = storeCommitState(error);
    let observed;
    try {
      observed = await readRowTransaction(store, input, true);
    } catch (readError) {
      if (isCatalogueError(readError)) throw readError;
      fail("postgres_atomic_crash_capture_catalogue_outcome_uncertain");
    }
    if (observed !== null) return observedProjection(observed);
    if (commitState === "not-committed") {
      try {
        attempt = await claimTransaction(store, input);
      } catch (retryError) {
        if (isCatalogueError(retryError)) throw retryError;
      }
      if (attempt !== undefined) {
        return attempt.inserted
          ? newDispatchProjection(owner, input)
          : attempt.projection;
      }
    }
    fail("postgres_atomic_crash_capture_catalogue_outcome_uncertain");
  }
  return attempt.inserted
    ? newDispatchProjection(owner, input)
    : attempt.projection;
}

async function transitionWithRecovery(store, input, transition, result) {
  const invoke = () =>
    transition === "mark"
      ? markUncertainTransaction(store, input)
      : commitResultTransaction(store, input, result);
  try {
    return await invoke();
  } catch (error) {
    if (isCatalogueError(error)) throw error;
    const commitState = storeCommitState(error);
    let observed;
    try {
      observed = await readRowTransaction(store, input, true);
    } catch (readError) {
      if (isCatalogueError(readError)) throw readError;
      fail("postgres_atomic_crash_capture_catalogue_outcome_uncertain");
    }
    if (transition === "mark" && observed?.state === "uncertain") {
      return uncertainProjection();
    }
    if (transition === "commit" && observed?.state === "committed") {
      ensure(
        rowMatchesResult(observed, result),
        "postgres_atomic_crash_capture_catalogue_conflict",
      );
      return committedProjection(observed);
    }
    if (
      commitState === "not-committed" &&
      observed !== null &&
      (observed.state === "starting" ||
        (transition === "commit" && observed.state === "uncertain"))
    ) {
      try {
        return await invoke();
      } catch (retryError) {
        if (isCatalogueError(retryError)) throw retryError;
      }
      try {
        observed = await readRowTransaction(store, input, true);
      } catch (readError) {
        if (isCatalogueError(readError)) throw readError;
        fail("postgres_atomic_crash_capture_catalogue_outcome_uncertain");
      }
      if (transition === "mark" && observed?.state === "uncertain") {
        return uncertainProjection();
      }
      if (transition === "commit" && observed?.state === "committed") {
        ensure(
          rowMatchesResult(observed, result),
          "postgres_atomic_crash_capture_catalogue_conflict",
        );
        return committedProjection(observed);
      }
    }
    fail("postgres_atomic_crash_capture_catalogue_outcome_uncertain");
  }
}

export class PostgresAtomicCrashCaptureCatalogueError extends Error {
  constructor(code) {
    if (typeof code !== "string" || !objectHasOwn(ERROR_MESSAGES, code)) {
      throw new TypeErrorConstructor(
        "Unsupported PostgreSQL atomic crash-capture catalogue error",
      );
    }
    const message = ERROR_MESSAGES[code];
    super(message);
    objectDefineProperty(this, "name", {
      enumerable: false,
      value: "PostgresAtomicCrashCaptureCatalogueError",
    });
    objectDefineProperty(this, "code", { enumerable: true, value: code });
    objectDefineProperty(this, "retryable", {
      enumerable: true,
      value: false,
    });
    objectDefineProperty(this, "stack", {
      enumerable: false,
      value: `PostgresAtomicCrashCaptureCatalogueError: ${message}`,
    });
    callIntrinsic(weakSetAddIntrinsic, catalogueErrors, [this]);
    objectFreeze(this);
  }
}

export function createPostgresAtomicCrashCaptureCatalogue(...args) {
  const optionCode =
    "invalid_postgres_atomic_crash_capture_catalogue_options";
  ensure(args.length === 1, optionCode);
  const options = exactDataObject(args[0], FACTORY_KEYS, optionCode);
  ensure(isPostgresSerializableStore(options.store), optionCode);
  const store = options.store;
  const owner = objectFreeze(objectCreate(null));
  const requestCode =
    "invalid_postgres_atomic_crash_capture_catalogue_request";

  const claimStarting = async function claimStarting(...methodArgs) {
    ensure(methodArgs.length === 1, requestCode);
    const input = normalizedClaimInput(methodArgs[0], requestCode);
    return claimWithRecovery(store, owner, input);
  };

  const markUncertain = async function markUncertain(...methodArgs) {
    ensure(methodArgs.length === 1, requestCode);
    const method = exactDataObject(methodArgs[0], MARK_KEYS, requestCode);
    const input = consumeDispatchClaim(method.dispatchClaim, owner);
    return transitionWithRecovery(store, input, "mark", null);
  };

  const commitResult = async function commitResult(...methodArgs) {
    ensure(methodArgs.length === 1, requestCode);
    const method = exactDataObject(methodArgs[0], COMMIT_KEYS, requestCode);
    const input = consumeDispatchClaim(method.dispatchClaim, owner);
    const result = normalizedStoredResult(
      method.result,
      input.request,
      requestCode,
    );
    return transitionWithRecovery(store, input, "commit", result);
  };

  const readCommitted = async function readCommitted(...methodArgs) {
    ensure(methodArgs.length === 1, requestCode);
    const input = normalizedReadInput(methodArgs[0], requestCode);
    for (let attempt = 0; attempt < 2; attempt += 1) {
      try {
        const row = await readRowTransaction(store, input, false);
        return observedProjection(row);
      } catch (error) {
        if (isCatalogueError(error)) throw error;
      }
    }
    fail("postgres_atomic_crash_capture_catalogue_outcome_uncertain");
  };

  objectFreeze(claimStarting);
  objectFreeze(markUncertain);
  objectFreeze(commitResult);
  objectFreeze(readCommitted);
  const catalogue = exactFrozenRecord({
    claimStarting,
    commitResult,
    contractVersion:
      POSTGRES_ATOMIC_CRASH_CAPTURE_CATALOGUE_CONTRACT_VERSION,
    markUncertain,
    readCommitted,
  });
  callIntrinsic(weakSetAddIntrinsic, catalogueBrands, [catalogue]);
  return catalogue;
}

export function isPostgresAtomicCrashCaptureCatalogue(value) {
  if (arguments.length !== 1) return false;
  try {
    return (
      value !== null &&
      typeof value === "object" &&
      !isProxyValue(value) &&
      callIntrinsic(weakSetHasIntrinsic, catalogueBrands, [value])
    );
  } catch {
    return false;
  }
}

objectFreeze(PostgresAtomicCrashCaptureCatalogueError.prototype);
objectFreeze(PostgresAtomicCrashCaptureCatalogueError);
objectFreeze(createPostgresAtomicCrashCaptureCatalogue);
objectFreeze(isPostgresAtomicCrashCaptureCatalogue);
