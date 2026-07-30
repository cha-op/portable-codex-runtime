import { createHash } from "node:crypto";
import { types as utilTypes } from "node:util";

import {
  PostgresSerializableStore,
} from "./postgres-serializable-store.mjs";
import {
  assertSessionManifest,
  assertSessionStorageRef,
  assertStorageBackendCapabilities,
} from "./session-storage-contracts.mjs";

export const SESSION_AUTHORITY_DOCUMENT_VERSION = 1;
export const SESSION_OPERATION_CONFLICT_CLASS = "session-mutation";

const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u;
const REVISION_PATTERN = /^(?:0|[1-9][0-9]{0,18})$/u;
const SHA256_PATTERN = /^[0-9a-f]{64}$/u;
const SENSITIVE_OPERATION_KEY_PATTERN =
  /(?:api[_-]?key|auth(?:json|orization)?|cookie|credential|password|private.?key|secret|token)/iu;
const MAX_POSTGRES_BIGINT = 9_223_372_036_854_775_807n;
const MAX_OPERATION_JSON_BYTES = 65_536;
const MAX_OPERATION_JSON_DEPTH = 32;
const MAX_OPERATION_JSON_NODES = 4_096;
const OPERATION_REQUEST_VERSION = 1;
const RESERVATION_PAYLOAD_VERSION = 1;
const OPERATION_RESULT_VERSION = 1;
const DOCUMENT_KEYS = Object.freeze([
  "activeOperation",
  "attachment",
  "backendCapabilities",
  "documentVersion",
  "launch",
  "lease",
  "lifecycle",
  "manifest",
  "recovery",
  "storageRef",
  "writerEpoch",
]);
const ACTIVE_OPERATION_KEYS = Object.freeze([
  "conflictClass",
  "expectedSessionRevision",
  "kind",
  "operationId",
  "operationRevision",
  "requestSha256",
  "reservationId",
  "state",
]);
const ROW_KEYS = Object.freeze([
  "created_at",
  "document",
  "revision",
  "session_id",
  "updated_at",
]);
const SNAPSHOT_KEYS = Object.freeze([
  "createdAt",
  "document",
  "revision",
  "sessionId",
  "updatedAt",
]);
const OPERATION_INPUT_KEYS = Object.freeze([
  "expectedSession",
  "kind",
  "operationId",
  "request",
]);
const OPERATION_TRANSITION_INPUT_KEYS = Object.freeze([
  "expectedOperationRevision",
  "expectedSession",
  "kind",
  "operationId",
  "request",
]);
const OPERATION_CANCELLATION_INPUT_KEYS = Object.freeze([
  "expectedOperationRevision",
  "expectedSession",
  "kind",
  "operationId",
  "reason",
  "request",
]);
const OPERATION_REQUEST_KEYS = Object.freeze([
  "conflictClass",
  "expectedSession",
  "payload",
  "requestVersion",
]);
const OPERATION_ROW_KEYS = Object.freeze([
  "created_at",
  "kind",
  "operation_id",
  "request",
  "result",
  "retired_at",
  "revision",
  "session_id",
  "state",
  "updated_at",
]);
const RESERVATION_PAYLOAD_KEYS = Object.freeze([
  "conflictClass",
  "requestSha256",
  "reservationVersion",
]);
const RESERVATION_ROW_KEYS = Object.freeze([
  "created_at",
  "expected_session_revision",
  "expires_at",
  "kind",
  "operation_id",
  "payload",
  "released_at",
  "reservation_id",
  "session_id",
  "state",
  "updated_at",
]);
const CANCELLATION_RESULT_KEYS = Object.freeze([
  "outcome",
  "reason",
  "resultVersion",
]);
const ACTIVE_OPERATION_STATES = Object.freeze([
  "prepared",
  "starting",
  "uncertain",
]);
const ERROR_MESSAGES = Object.freeze({
  invalid_authority_options: "PostgreSQL session authority options are invalid",
  invalid_operation_request: "Session operation request is invalid",
  invalid_session_read: "Session read request is invalid",
  invalid_session_registration: "Session registration request is invalid",
  operation_identity_conflict:
    "Operation ID is already bound to a different canonical request",
  operation_result_conflict:
    "Operation ID is already bound to a different terminal result",
  operation_state_invalid: "Stored operation authority state is invalid",
  operation_transition_conflict:
    "Operation cannot perform the requested phase transition",
  session_identity_conflict:
    "Session ID is already bound to a different canonical document",
  session_not_found: "Session is not registered",
  session_operation_conflict:
    "Session already has an active conflicting operation",
  session_revision_conflict:
    "Session revision does not match the expected canonical snapshot",
  session_revision_exhausted: "Session revision is exhausted",
  session_state_invalid: "Stored session authority state is invalid",
});

const BigIntConstructor = BigInt;
const bigIntToStringIntrinsic = BigInt.prototype.toString;
const ArrayConstructor = Array;
const arrayEveryIntrinsic = Array.prototype.every;
const arrayIncludesIntrinsic = Array.prototype.includes;
const arrayIsArray = Array.isArray;
const bufferByteLengthIntrinsic = Buffer.byteLength;
const createHashIntrinsic = createHash;
const DateConstructor = Date;
const dateGetTimeIntrinsic = Date.prototype.getTime;
const dateParseIntrinsic = Date.parse;
const datePrototype = Date.prototype;
const dateToISOStringIntrinsic = Date.prototype.toISOString;
const hashPrototype = Object.getPrototypeOf(createHash("sha256"));
const hashDigestIntrinsic = hashPrototype.digest;
const hashUpdateIntrinsic = hashPrototype.update;
const jsonStringifyIntrinsic = JSON.stringify;
const isProxyValue = utilTypes.isProxy;
const numberIsFinite = Number.isFinite;
const numberIsSafeInteger = Number.isSafeInteger;
const objectCreate = Object.create;
const objectFreeze = Object.freeze;
const objectGetOwnPropertyDescriptor = Object.getOwnPropertyDescriptor;
const objectGetPrototypeOf = Object.getPrototypeOf;
const objectHasOwn = Object.hasOwn;
const objectIs = Object.is;
const objectIsFrozen = Object.isFrozen;
const objectPrototype = Object.prototype;
const objectSetPrototypeOf = Object.setPrototypeOf;
const reflectApply = Reflect.apply;
const reflectOwnKeys = Reflect.ownKeys;
const regexpExecIntrinsic = RegExp.prototype.exec;
const runSerializableIntrinsic =
  PostgresSerializableStore.prototype.runSerializable;
const stringCharCodeAtIntrinsic = String.prototype.charCodeAt;
const weakSetAddIntrinsic = WeakSet.prototype.add;
const weakSetDeleteIntrinsic = WeakSet.prototype.delete;
const weakSetHasIntrinsic = WeakSet.prototype.has;
const WeakSetConstructor = WeakSet;
const OPERATION_VISIBILITY_RETRY = objectFreeze(
  new Error("operation identity visibility retry"),
);

const INSERT_SESSION_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    "INSERT INTO session_authority.sessions",
    "(session_id, revision, document, created_at, updated_at)",
    "VALUES ($1::uuid, 0, $2::jsonb, $3::timestamptz, $3::timestamptz)",
    "ON CONFLICT (session_id) DO NOTHING",
    "RETURNING session_id, revision, document, created_at, updated_at",
  ].join(" "),
});
const READ_SESSION_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    "SELECT session_id, revision, document, created_at, updated_at",
    "FROM session_authority.sessions",
    "WHERE session_id = $1::uuid",
  ].join(" "),
});
const READ_SESSION_FOR_UPDATE_QUERY = Object.freeze({
  queryMode: "extended",
  text: `${READ_SESSION_QUERY.text} FOR UPDATE`,
});
const OPERATION_RETURNING_COLUMNS = [
  "operation_id",
  "session_id",
  "kind",
  "request",
  "result",
  "state",
  "revision",
  "created_at",
  "updated_at",
  "retired_at",
].join(", ");
const RESERVATION_RETURNING_COLUMNS = [
  "reservation_id",
  "operation_id",
  "session_id",
  "kind",
  "expected_session_revision",
  "state",
  "payload",
  "created_at",
  "updated_at",
  "expires_at",
  "released_at",
].join(", ");
const READ_OPERATION_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    `SELECT ${OPERATION_RETURNING_COLUMNS}`,
    "FROM session_authority.operation_claims",
    "WHERE operation_id = $1",
  ].join(" "),
});
const READ_OPERATION_FOR_UPDATE_QUERY = Object.freeze({
  queryMode: "extended",
  text: `${READ_OPERATION_QUERY.text} FOR UPDATE`,
});
const READ_RESERVATION_BY_OPERATION_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    `SELECT ${RESERVATION_RETURNING_COLUMNS}`,
    "FROM session_authority.reservations",
    "WHERE operation_id = $1",
  ].join(" "),
});
const READ_RESERVATION_BY_OPERATION_FOR_UPDATE_QUERY = Object.freeze({
  queryMode: "extended",
  text: `${READ_RESERVATION_BY_OPERATION_QUERY.text} FOR UPDATE`,
});
const READ_ACTIVE_COUNTS_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    "SELECT",
    "(SELECT count(*)::integer",
    "FROM session_authority.operation_claims",
    "WHERE session_id = $1::uuid AND retired_at IS NULL)",
    "AS operation_count,",
    "(SELECT count(*)::integer",
    "FROM session_authority.reservations",
    "WHERE session_id = $1::uuid AND released_at IS NULL)",
    "AS reservation_count",
  ].join(" "),
});
const INSERT_OPERATION_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    "INSERT INTO session_authority.operation_claims",
    "(operation_id, session_id, kind, request, result, state, revision,",
    "created_at, updated_at, retired_at)",
    "VALUES ($1, $2::uuid, $3, $4::jsonb, NULL, 'prepared', 0, $5, $5, NULL)",
    "ON CONFLICT (operation_id) DO NOTHING",
    `RETURNING ${OPERATION_RETURNING_COLUMNS}`,
  ].join(" "),
});
const INSERT_RESERVATION_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    "INSERT INTO session_authority.reservations",
    "(reservation_id, operation_id, session_id, kind,",
    "expected_session_revision, state, payload, created_at, updated_at,",
    "expires_at, released_at)",
    "VALUES ($1, $2, $3::uuid, $4, $5::bigint, 'prepared',",
    "$6::jsonb, $7, $7, NULL, NULL)",
    "ON CONFLICT (reservation_id) DO NOTHING",
    `RETURNING ${RESERVATION_RETURNING_COLUMNS}`,
  ].join(" "),
});
const UPDATE_SESSION_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    "UPDATE session_authority.sessions",
    "SET revision = revision + 1, document = $3::jsonb, updated_at = $4",
    "WHERE session_id = $1::uuid AND revision = $2::bigint",
    "RETURNING session_id, revision, document, created_at, updated_at",
  ].join(" "),
});
const START_OPERATION_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    "UPDATE session_authority.operation_claims",
    "SET state = 'starting', revision = revision + 1, updated_at = $3",
    "WHERE operation_id = $1 AND revision = $2::bigint",
    "AND state = 'prepared' AND retired_at IS NULL",
    `RETURNING ${OPERATION_RETURNING_COLUMNS}`,
  ].join(" "),
});
const START_RESERVATION_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    "UPDATE session_authority.reservations",
    "SET state = 'starting', updated_at = $2",
    "WHERE operation_id = $1 AND state = 'prepared' AND released_at IS NULL",
    `RETURNING ${RESERVATION_RETURNING_COLUMNS}`,
  ].join(" "),
});
const UNCERTAIN_OPERATION_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    "UPDATE session_authority.operation_claims",
    "SET state = 'uncertain', revision = revision + 1, updated_at = $3",
    "WHERE operation_id = $1 AND revision = $2::bigint",
    "AND state = 'starting' AND retired_at IS NULL",
    `RETURNING ${OPERATION_RETURNING_COLUMNS}`,
  ].join(" "),
});
const UNCERTAIN_RESERVATION_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    "UPDATE session_authority.reservations",
    "SET state = 'uncertain', updated_at = $2",
    "WHERE operation_id = $1 AND state = 'starting' AND released_at IS NULL",
    `RETURNING ${RESERVATION_RETURNING_COLUMNS}`,
  ].join(" "),
});
const CANCEL_OPERATION_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    "UPDATE session_authority.operation_claims",
    "SET state = 'committed', result = $3::jsonb,",
    "revision = revision + 1, updated_at = $4, retired_at = $4",
    "WHERE operation_id = $1 AND revision = $2::bigint",
    "AND state = 'prepared' AND retired_at IS NULL",
    `RETURNING ${OPERATION_RETURNING_COLUMNS}`,
  ].join(" "),
});
const RELEASE_RESERVATION_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    "UPDATE session_authority.reservations",
    "SET state = 'released', updated_at = $2, released_at = $2",
    "WHERE operation_id = $1 AND state = 'prepared' AND released_at IS NULL",
    `RETURNING ${RESERVATION_RETURNING_COLUMNS}`,
  ].join(" "),
});

export class PostgresSessionAuthorityError extends Error {
  constructor(code) {
    if (!objectHasOwn(ERROR_MESSAGES, code)) {
      throw new TypeError("unsupported PostgreSQL session authority error");
    }
    super(ERROR_MESSAGES[code]);
    this.name = "PostgresSessionAuthorityError";
    this.code = code;
    this.retryable = false;
    objectFreeze(this);
  }
}

function fail(code) {
  throw new PostgresSessionAuthorityError(code);
}

function ensure(condition, code) {
  if (!condition) fail(code);
}

function regexpTest(pattern, value) {
  return reflectApply(regexpExecIntrinsic, pattern, [value]) !== null;
}

function sha256(value) {
  const hash = createHashIntrinsic("sha256");
  reflectApply(hashUpdateIntrinsic, hash, [value, "utf8"]);
  return reflectApply(hashDigestIntrinsic, hash, ["hex"]);
}

function assertLosslessString(value, code) {
  ensure(typeof value === "string", code);
  for (let index = 0; index < value.length; index += 1) {
    const unit = reflectApply(stringCharCodeAtIntrinsic, value, [index]);
    if (unit >= 0xd800 && unit <= 0xdbff) {
      ensure(index + 1 < value.length, code);
      const next = reflectApply(stringCharCodeAtIntrinsic, value, [index + 1]);
      ensure(next >= 0xdc00 && next <= 0xdfff, code);
      index += 1;
    } else {
      ensure(unit < 0xdc00 || unit > 0xdfff, code);
    }
  }
  return value;
}

function consumeOperationJsonBytes(state, additionalBytes, code) {
  ensure(
    numberIsSafeInteger(additionalBytes) &&
      additionalBytes >= 0 &&
      state.budget.bytes <= MAX_OPERATION_JSON_BYTES - additionalBytes,
    code,
  );
  state.budget.bytes += additionalBytes;
}

function consumeOperationJsonString(state, value, code) {
  ensure(typeof value === "string", code);
  consumeOperationJsonBytes(state, 2, code);
  for (let index = 0; index < value.length; index += 1) {
    const unit = reflectApply(stringCharCodeAtIntrinsic, value, [index]);
    if (unit === 0x22 || unit === 0x5c) {
      consumeOperationJsonBytes(state, 2, code);
    } else if (unit <= 0x1f) {
      const shortEscape =
        unit === 0x08 ||
        unit === 0x09 ||
        unit === 0x0a ||
        unit === 0x0c ||
        unit === 0x0d;
      consumeOperationJsonBytes(state, shortEscape ? 2 : 6, code);
    } else if (unit <= 0x7f) {
      consumeOperationJsonBytes(state, 1, code);
    } else if (unit <= 0x7ff) {
      consumeOperationJsonBytes(state, 2, code);
    } else if (unit >= 0xd800 && unit <= 0xdbff) {
      ensure(index + 1 < value.length, code);
      const next = reflectApply(stringCharCodeAtIntrinsic, value, [index + 1]);
      ensure(next >= 0xdc00 && next <= 0xdfff, code);
      consumeOperationJsonBytes(state, 4, code);
      index += 1;
    } else {
      ensure(unit < 0xdc00 || unit > 0xdfff, code);
      consumeOperationJsonBytes(state, 3, code);
    }
  }
  return value;
}

function canonicalOpaqueId(value, maxLength, code) {
  assertLosslessString(value, code);
  ensure(value.length >= 1 && value.length <= maxLength, code);
  for (let index = 0; index < value.length; index += 1) {
    const unit = reflectApply(stringCharCodeAtIntrinsic, value, [index]);
    const alphaNumeric =
      (unit >= 0x30 && unit <= 0x39) ||
      (unit >= 0x41 && unit <= 0x5a) ||
      (unit >= 0x61 && unit <= 0x7a);
    const punctuation =
      unit === 0x2d || unit === 0x2e || unit === 0x3a || unit === 0x5f;
    ensure(alphaNumeric || punctuation, code);
  }
  return value;
}

function sortedStringKeys(keys, code) {
  const copy = new ArrayConstructor(keys.length);
  for (let index = 0; index < keys.length; index += 1) {
    ensure(typeof keys[index] === "string", code);
    copy[index] = keys[index];
  }
  for (let outer = 1; outer < copy.length; outer += 1) {
    const value = copy[outer];
    let inner = outer - 1;
    while (inner >= 0 && copy[inner] > value) {
      copy[inner + 1] = copy[inner];
      inner -= 1;
    }
    copy[inner + 1] = value;
  }
  return copy;
}

function canonicalJsonValue(value, state, code) {
  state.budget.nodes += 1;
  ensure(
    state.budget.nodes <= MAX_OPERATION_JSON_NODES &&
      state.depth <= MAX_OPERATION_JSON_DEPTH,
    code,
  );
  if (value === null) {
    consumeOperationJsonBytes(state, 4, code);
    return value;
  }
  if (typeof value === "boolean") {
    consumeOperationJsonBytes(state, value ? 4 : 5, code);
    return value;
  }
  if (typeof value === "string") {
    return consumeOperationJsonString(state, value, code);
  }
  if (typeof value === "number") {
    ensure(numberIsFinite(value), code);
    const normalized = objectIs(value, -0) ? 0 : value;
    const serialized = reflectApply(jsonStringifyIntrinsic, JSON, [
      normalized,
    ]);
    consumeOperationJsonBytes(
      state,
      reflectApply(bufferByteLengthIntrinsic, Buffer, [
        serialized,
        "utf8",
      ]),
      code,
    );
    return normalized;
  }
  ensure(
    typeof value === "object" &&
      !isProxyValue(value) &&
      !reflectApply(weakSetHasIntrinsic, state.seen, [value]),
    code,
  );
  reflectApply(weakSetAddIntrinsic, state.seen, [value]);
  let result;
  if (arrayIsArray(value)) {
    ensure(
      numberIsSafeInteger(value.length) &&
        value.length <=
          MAX_OPERATION_JSON_NODES - state.budget.nodes,
      code,
    );
    consumeOperationJsonBytes(
      state,
      2 + (value.length === 0 ? 0 : value.length - 1),
      code,
    );
    const ownKeys = reflectOwnKeys(value);
    ensure(ownKeys.length === value.length + 1, code);
    result = new ArrayConstructor(value.length);
    objectSetPrototypeOf(result, null);
    for (let index = 0; index < value.length; index += 1) {
      const descriptor = objectGetOwnPropertyDescriptor(value, String(index));
      ensure(
        descriptor?.enumerable === true &&
          objectHasOwn(descriptor, "value"),
        code,
      );
      const childState = {
        budget: state.budget,
        depth: state.depth + 1,
        seen: state.seen,
      };
      result[index] = canonicalJsonValue(
        descriptor.value,
        childState,
        code,
      );
    }
    const lengthDescriptor = objectGetOwnPropertyDescriptor(value, "length");
    ensure(
      lengthDescriptor !== undefined &&
        objectHasOwn(lengthDescriptor, "value") &&
        lengthDescriptor.value === value.length,
      code,
    );
  } else {
    let prototype;
    let ownKeys;
    try {
      prototype = objectGetPrototypeOf(value);
      ownKeys = reflectOwnKeys(value);
    } catch {
      fail(code);
    }
    ensure(prototype === objectPrototype || prototype === null, code);
    ensure(
      ownKeys.length <=
        MAX_OPERATION_JSON_NODES - state.budget.nodes,
      code,
    );
    consumeOperationJsonBytes(
      state,
      2 + (ownKeys.length === 0 ? 0 : ownKeys.length - 1),
      code,
    );
    for (let index = 0; index < ownKeys.length; index += 1) {
      const key = consumeOperationJsonString(
        state,
        ownKeys[index],
        code,
      );
      ensure(!regexpTest(SENSITIVE_OPERATION_KEY_PATTERN, key), code);
      consumeOperationJsonBytes(state, 1, code);
    }
    const keys = sortedStringKeys(ownKeys, code);
    result = objectCreate(null);
    for (let index = 0; index < keys.length; index += 1) {
      const key = keys[index];
      const descriptor = objectGetOwnPropertyDescriptor(value, key);
      ensure(
        descriptor?.enumerable === true &&
          objectHasOwn(descriptor, "value"),
        code,
      );
      const childState = {
        budget: state.budget,
        depth: state.depth + 1,
        seen: state.seen,
      };
      result[key] = canonicalJsonValue(descriptor.value, childState, code);
    }
  }
  reflectApply(weakSetDeleteIntrinsic, state.seen, [value]);
  return result;
}

function canonicalJsonObject(value, code = "invalid_operation_request") {
  const state = {
    budget: {
      bytes: 0,
      nodes: 0,
    },
    depth: 0,
    seen: new WeakSetConstructor(),
  };
  const canonical = canonicalJsonValue(value, state, code);
  ensure(
    canonical !== null &&
      typeof canonical === "object" &&
      !arrayIsArray(canonical),
    code,
  );
  const serialized = reflectApply(jsonStringifyIntrinsic, JSON, [canonical]);
  const serializedBytes = reflectApply(
    bufferByteLengthIntrinsic,
    Buffer,
    [serialized, "utf8"],
  );
  ensure(
    typeof serialized === "string" &&
      serializedBytes === state.budget.bytes &&
      serializedBytes <= MAX_OPERATION_JSON_BYTES,
    code,
  );
  return deepFreeze(canonical);
}

function arrayEvery(value, callback) {
  return reflectApply(arrayEveryIntrinsic, value, [callback]);
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

function exactPlainObject(value, keys, code) {
  if (
    value === null ||
    typeof value !== "object" ||
    arrayIsArray(value) ||
    isProxyValue(value)
  ) {
    fail(code);
  }
  let actual;
  let prototype;
  try {
    prototype = objectGetPrototypeOf(value);
    actual = reflectOwnKeys(value);
  } catch {
    fail(code);
  }
  ensure(prototype === objectPrototype || prototype === null, code);
  ensure(
    actual.length === keys.length &&
      arrayEvery(
        actual,
        (key) =>
          typeof key === "string" &&
          reflectApply(arrayIncludesIntrinsic, keys, [key]),
    ),
    code,
  );
  const normalized = objectCreate(null);
  for (let index = 0; index < actual.length; index += 1) {
    const key = actual[index];
    normalized[key] = ownDataValue(value, key, code);
  }
  return normalized;
}

function canonicalSessionId(value, code) {
  ensure(typeof value === "string" && regexpTest(UUID_PATTERN, value), code);
  return value;
}

function canonicalActiveOperation(value, code) {
  const active = exactPlainObject(value, ACTIVE_OPERATION_KEYS, code);
  const state = canonicalOpaqueId(active.state, 32, code);
  ensure(
    reflectApply(arrayIncludesIntrinsic, ACTIVE_OPERATION_STATES, [state]),
    code,
  );
  const expectedSessionRevision = canonicalRevisionForCode(
    active.expectedSessionRevision,
    code,
  );
  const operationRevision = canonicalRevisionForCode(
    active.operationRevision,
    code,
  );
  const expectedOperationRevision =
    state === "prepared" ? "0" : state === "starting" ? "1" : "2";
  ensure(operationRevision === expectedOperationRevision, code);
  ensure(
    active.conflictClass === SESSION_OPERATION_CONFLICT_CLASS &&
      typeof active.requestSha256 === "string" &&
      regexpTest(SHA256_PATTERN, active.requestSha256),
    code,
  );
  return deepFreeze({
    conflictClass: SESSION_OPERATION_CONFLICT_CLASS,
    expectedSessionRevision,
    kind: canonicalOpaqueId(active.kind, 64, code),
    operationId: canonicalOpaqueId(active.operationId, 128, code),
    operationRevision,
    requestSha256: active.requestSha256,
    reservationId: canonicalOpaqueId(active.reservationId, 128, code),
    state,
  });
}

function canonicalDocument(value, code) {
  const document = exactPlainObject(value, DOCUMENT_KEYS, code);
  ensure(
    document.documentVersion === SESSION_AUTHORITY_DOCUMENT_VERSION &&
      document.lifecycle === "DETACHED" &&
      document.writerEpoch === "0" &&
      document.lease === null &&
      document.attachment === null &&
      document.recovery === null &&
      document.launch === null,
    code,
  );
  let manifest;
  let storageRef;
  let backendCapabilities;
  try {
    manifest = assertSessionManifest(document.manifest);
    storageRef = assertSessionStorageRef(document.storageRef);
    backendCapabilities = assertStorageBackendCapabilities(
      document.backendCapabilities,
    );
  } catch {
    fail(code);
  }
  ensure(manifest.sessionId === storageRef.sessionId, code);
  const activeOperation =
    document.activeOperation === null
      ? null
      : canonicalActiveOperation(document.activeOperation, code);
  return assembleCanonicalDocument({
    activeOperation,
    backendCapabilities,
    manifest,
    storageRef,
  });
}

function registrationDocument(options) {
  const normalized = exactPlainObject(
    options,
    ["backendCapabilities", "manifest", "storageRef"],
    "invalid_session_registration",
  );
  let manifest;
  let storageRef;
  let backendCapabilities;
  try {
    manifest = assertSessionManifest(normalized.manifest);
    storageRef = assertSessionStorageRef(normalized.storageRef);
    backendCapabilities = assertStorageBackendCapabilities(
      normalized.backendCapabilities,
    );
  } catch {
    fail("invalid_session_registration");
  }
  ensure(
    manifest.sessionId === storageRef.sessionId,
    "invalid_session_registration",
  );
  return assembleCanonicalDocument({
    backendCapabilities,
    manifest,
    storageRef,
  });
}

function assembleCanonicalDocument({
  activeOperation = null,
  backendCapabilities,
  manifest,
  storageRef,
}) {
  return deepFreeze({
    documentVersion: SESSION_AUTHORITY_DOCUMENT_VERSION,
    manifest: {
      schemaVersion: manifest.schemaVersion,
      sessionId: manifest.sessionId,
      codex: {
        rootThreadId: manifest.codex.rootThreadId,
        sessionId: manifest.codex.sessionId,
        ephemeral: manifest.codex.ephemeral,
        historyMode: manifest.codex.historyMode,
      },
      runtime: {
        imageDigest: manifest.runtime.imageDigest,
        imageMediaType: manifest.runtime.imageMediaType,
        platform: manifest.runtime.platform,
        codexVersion: manifest.runtime.codexVersion,
        codexSandbox: manifest.runtime.codexSandbox,
      },
      layoutVersion: manifest.layoutVersion,
      authMode: manifest.authMode,
      agents: {
        defaultMaxSubagents: manifest.agents.defaultMaxSubagents,
        maxSubagents: manifest.agents.maxSubagents,
        maxDepth: manifest.agents.maxDepth,
      },
    },
    storageRef: {
      contractVersion: storageRef.contractVersion,
      backendId: storageRef.backendId,
      storageId: storageRef.storageId,
      sessionId: storageRef.sessionId,
    },
    backendCapabilities: {
      atomicPointInTimeCheckpoint:
        backendCapabilities.atomicPointInTimeCheckpoint,
      exclusiveWriterAttachment:
        backendCapabilities.exclusiveWriterAttachment,
      fencing: backendCapabilities.fencing,
      normalDirectoryAttachment:
        backendCapabilities.normalDirectoryAttachment,
    },
    lifecycle: "DETACHED",
    writerEpoch: "0",
    lease: null,
    attachment: null,
    activeOperation,
    recovery: null,
    launch: null,
  });
}

function deepFreeze(value) {
  if (
    value !== null &&
    typeof value === "object" &&
    !objectIsFrozen(value)
  ) {
    const keys = reflectOwnKeys(value);
    for (let index = 0; index < keys.length; index += 1) {
      const descriptor = objectGetOwnPropertyDescriptor(value, keys[index]);
      if (descriptor && objectHasOwn(descriptor, "value")) {
        deepFreeze(descriptor.value);
      }
    }
    objectFreeze(value);
  }
  return value;
}

function nullPrototypeJsonDataTree(value) {
  if (value === null || typeof value !== "object") return value;
  const keys = reflectOwnKeys(value);
  let copy;
  if (arrayIsArray(value)) {
    copy = new ArrayConstructor(value.length);
    objectSetPrototypeOf(copy, null);
  } else {
    copy = objectCreate(null);
  }
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
    if (key === "length") continue;
    const descriptor = objectGetOwnPropertyDescriptor(value, key);
    if (descriptor && objectHasOwn(descriptor, "value")) {
      copy[key] = nullPrototypeJsonDataTree(descriptor.value);
    }
  }
  return copy;
}

function canonicalSerialize(document) {
  return reflectApply(jsonStringifyIntrinsic, JSON, [
    nullPrototypeJsonDataTree(document),
  ]);
}

function canonicalRevisionForCode(value, code) {
  ensure(
    typeof value === "string" && regexpTest(REVISION_PATTERN, value),
    code,
  );
  let revision;
  try {
    revision = BigIntConstructor(value);
  } catch {
    fail(code);
  }
  ensure(revision <= MAX_POSTGRES_BIGINT, code);
  return value;
}

function canonicalRevision(value) {
  return canonicalRevisionForCode(value, "session_state_invalid");
}

function canonicalTimestamp(value) {
  ensure(
    value !== null &&
      typeof value === "object" &&
      !isProxyValue(value) &&
      objectGetPrototypeOf(value) === datePrototype,
    "session_state_invalid",
  );
  const milliseconds = reflectApply(dateGetTimeIntrinsic, value, []);
  ensure(numberIsFinite(milliseconds), "session_state_invalid");
  return reflectApply(dateToISOStringIntrinsic, value, []);
}

function canonicalTimestampString(value, code) {
  assertLosslessString(value, code);
  const milliseconds = reflectApply(
    dateParseIntrinsic,
    DateConstructor,
    [value],
  );
  ensure(numberIsFinite(milliseconds), code);
  const normalized = new DateConstructor(milliseconds);
  ensure(
    reflectApply(dateToISOStringIntrinsic, normalized, []) === value,
    code,
  );
  return value;
}

function timestampMilliseconds(value) {
  return reflectApply(dateParseIntrinsic, DateConstructor, [value]);
}

function validateSessionRevisionState(snapshot, code) {
  const revision = BigIntConstructor(snapshot.revision);
  ensure(
    timestampMilliseconds(snapshot.updatedAt) >=
      timestampMilliseconds(snapshot.createdAt),
    code,
  );
  const active = snapshot.document.activeOperation;
  if (active === null) {
    if (revision === 0n) {
      ensure(snapshot.createdAt === snapshot.updatedAt, code);
    }
    return;
  }
  const expected = BigIntConstructor(active.expectedSessionRevision);
  const operationRevision = BigIntConstructor(active.operationRevision);
  ensure(
    expected + operationRevision + 1n === revision &&
      revision <= MAX_POSTGRES_BIGINT,
    code,
  );
}

function rowsFromResult(result, code = "session_state_invalid") {
  ensure(
    result !== null &&
      typeof result === "object" &&
      !isProxyValue(result),
    code,
  );
  const rows = ownDataValue(result, "rows", code);
  ensure(
    arrayIsArray(rows) &&
      !isProxyValue(rows) &&
      (rows.length === 0 || rows.length === 1),
    code,
  );
  for (let index = 0; index < rows.length; index += 1) {
    ownDataValue(rows, String(index), code);
  }
  return rows;
}

function snapshotFromRow(row, expectedSessionId) {
  const normalized = exactPlainObject(
    row,
    ROW_KEYS,
    "session_state_invalid",
  );
  const sessionId = canonicalSessionId(
    normalized.session_id,
    "session_state_invalid",
  );
  ensure(sessionId === expectedSessionId, "session_state_invalid");
  const revision = canonicalRevision(normalized.revision);
  const document = canonicalDocument(
    normalized.document,
    "session_state_invalid",
  );
  ensure(document.manifest.sessionId === sessionId, "session_state_invalid");
  const createdAt = canonicalTimestamp(normalized.created_at);
  const updatedAt = canonicalTimestamp(normalized.updated_at);
  const snapshot = deepFreeze({
    sessionId,
    revision,
    document,
    createdAt,
    updatedAt,
  });
  validateSessionRevisionState(snapshot, "session_state_invalid");
  return snapshot;
}

function expectedSnapshotFromValue(value, code = "invalid_operation_request") {
  const normalized = exactPlainObject(value, SNAPSHOT_KEYS, code);
  const sessionId = canonicalSessionId(normalized.sessionId, code);
  const revision = canonicalRevisionForCode(normalized.revision, code);
  const document = canonicalDocument(normalized.document, code);
  ensure(document.manifest.sessionId === sessionId, code);
  const createdAt = canonicalTimestampString(normalized.createdAt, code);
  const updatedAt = canonicalTimestampString(normalized.updatedAt, code);
  const snapshot = deepFreeze({
    sessionId,
    revision,
    document,
    createdAt,
    updatedAt,
  });
  validateSessionRevisionState(snapshot, code);
  return snapshot;
}

function canonicalSnapshotBytes(snapshot) {
  return canonicalSerialize({
    sessionId: snapshot.sessionId,
    revision: snapshot.revision,
    document: snapshot.document,
    createdAt: snapshot.createdAt,
    updatedAt: snapshot.updatedAt,
  });
}

function canonicalIdentityBytes(document) {
  return canonicalSerialize({
    manifest: document.manifest,
    storageRef: document.storageRef,
    backendCapabilities: document.backendCapabilities,
  });
}

function canonicalOperationEnvelope(value, code) {
  const normalized = exactPlainObject(value, OPERATION_REQUEST_KEYS, code);
  ensure(
    normalized.requestVersion === OPERATION_REQUEST_VERSION &&
      normalized.conflictClass === SESSION_OPERATION_CONFLICT_CLASS,
    code,
  );
  return deepFreeze({
    requestVersion: OPERATION_REQUEST_VERSION,
    conflictClass: SESSION_OPERATION_CONFLICT_CLASS,
    expectedSession: expectedSnapshotFromValue(
      normalized.expectedSession,
      code,
    ),
    payload: canonicalJsonObject(normalized.payload, code),
  });
}

function canonicalOperationInput(options, keys = OPERATION_INPUT_KEYS) {
  const normalized = exactPlainObject(
    options,
    keys,
    "invalid_operation_request",
  );
  const expectedSession = expectedSnapshotFromValue(
    normalized.expectedSession,
  );
  ensure(
    expectedSession.document.activeOperation === null,
    "invalid_operation_request",
  );
  const operationId = canonicalOpaqueId(
    normalized.operationId,
    128,
    "invalid_operation_request",
  );
  const kind = canonicalOpaqueId(
    normalized.kind,
    64,
    "invalid_operation_request",
  );
  const request = canonicalJsonObject(normalized.request);
  const envelope = deepFreeze({
    requestVersion: OPERATION_REQUEST_VERSION,
    conflictClass: SESSION_OPERATION_CONFLICT_CLASS,
    expectedSession,
    payload: request,
  });
  const serializedEnvelope = canonicalSerialize(envelope);
  const requestSha256 = sha256(serializedEnvelope);
  const reservationId = `reservation-${sha256(operationId)}`;
  return deepFreeze({
    envelope,
    expectedSession,
    kind,
    operationId,
    request,
    requestSha256,
    reservationId,
    serializedEnvelope,
  });
}

function operationInputWithExpectedRevision(options, expectedRevision) {
  const input = canonicalOperationInput(
    options,
    OPERATION_TRANSITION_INPUT_KEYS,
  );
  const normalized = exactPlainObject(
    options,
    OPERATION_TRANSITION_INPUT_KEYS,
    "invalid_operation_request",
  );
  const expectedOperationRevision = canonicalRevisionForCode(
    normalized.expectedOperationRevision,
    "invalid_operation_request",
  );
  ensure(
    expectedOperationRevision === expectedRevision,
    "invalid_operation_request",
  );
  return deepFreeze({
    ...input,
    expectedOperationRevision,
  });
}

function cancellationInput(options) {
  const input = canonicalOperationInput(
    options,
    OPERATION_CANCELLATION_INPUT_KEYS,
  );
  const normalized = exactPlainObject(
    options,
    OPERATION_CANCELLATION_INPUT_KEYS,
    "invalid_operation_request",
  );
  const expectedOperationRevision = canonicalRevisionForCode(
    normalized.expectedOperationRevision,
    "invalid_operation_request",
  );
  ensure(expectedOperationRevision === "0", "invalid_operation_request");
  const reason = canonicalOpaqueId(
    normalized.reason,
    64,
    "invalid_operation_request",
  );
  const result = deepFreeze({
    resultVersion: OPERATION_RESULT_VERSION,
    outcome: "cancelled-before-dispatch",
    reason,
  });
  return deepFreeze({
    ...input,
    expectedOperationRevision,
    reason,
    result,
    serializedResult: canonicalSerialize(result),
  });
}

function canonicalNullableRowTimestamp(value, code) {
  return value === null ? null : canonicalTimestampForCode(value, code);
}

function canonicalTimestampForCode(value, code) {
  ensure(
    value !== null &&
      typeof value === "object" &&
      !isProxyValue(value) &&
      objectGetPrototypeOf(value) === datePrototype,
    code,
  );
  const milliseconds = reflectApply(dateGetTimeIntrinsic, value, []);
  ensure(numberIsFinite(milliseconds), code);
  return reflectApply(dateToISOStringIntrinsic, value, []);
}

function canonicalCancellationResult(value, code) {
  const result = exactPlainObject(value, CANCELLATION_RESULT_KEYS, code);
  ensure(
    result.resultVersion === OPERATION_RESULT_VERSION &&
      result.outcome === "cancelled-before-dispatch",
    code,
  );
  return deepFreeze({
    resultVersion: OPERATION_RESULT_VERSION,
    outcome: "cancelled-before-dispatch",
    reason: canonicalOpaqueId(result.reason, 64, code),
  });
}

function operationSnapshotFromRow(row) {
  const code = "operation_state_invalid";
  const normalized = exactPlainObject(row, OPERATION_ROW_KEYS, code);
  const operationId = canonicalOpaqueId(
    normalized.operation_id,
    128,
    code,
  );
  const sessionId = canonicalSessionId(normalized.session_id, code);
  const kind = canonicalOpaqueId(normalized.kind, 64, code);
  const envelope = canonicalOperationEnvelope(normalized.request, code);
  ensure(
    envelope.expectedSession.sessionId === sessionId &&
      envelope.expectedSession.document.activeOperation === null,
    code,
  );
  const state = canonicalOpaqueId(normalized.state, 32, code);
  ensure(
    reflectApply(arrayIncludesIntrinsic, ACTIVE_OPERATION_STATES, [state]) ||
      state === "committed",
    code,
  );
  const revision = canonicalRevisionForCode(normalized.revision, code);
  const createdAt = canonicalTimestampForCode(normalized.created_at, code);
  const updatedAt = canonicalTimestampForCode(normalized.updated_at, code);
  const retiredAt = canonicalNullableRowTimestamp(
    normalized.retired_at,
    code,
  );
  ensure(
    timestampMilliseconds(updatedAt) >= timestampMilliseconds(createdAt),
    code,
  );
  let result = null;
  if (state === "prepared") {
    ensure(
      revision === "0" &&
        normalized.result === null &&
        retiredAt === null &&
        createdAt === updatedAt,
      code,
    );
  } else if (state === "starting") {
    ensure(
      revision === "1" &&
        normalized.result === null &&
        retiredAt === null,
      code,
    );
  } else if (state === "uncertain") {
    ensure(
      revision === "2" &&
        normalized.result === null &&
        retiredAt === null,
      code,
    );
  } else {
    ensure(revision === "1" && retiredAt === updatedAt, code);
    result = canonicalCancellationResult(normalized.result, code);
  }
  return deepFreeze({
    operationId,
    sessionId,
    kind,
    conflictClass: SESSION_OPERATION_CONFLICT_CLASS,
    expectedSession: envelope.expectedSession,
    request: envelope.payload,
    requestSha256: sha256(canonicalSerialize(envelope)),
    state,
    revision,
    result,
    createdAt,
    updatedAt,
    retiredAt,
  });
}

function canonicalReservationPayload(value, code) {
  const payload = exactPlainObject(value, RESERVATION_PAYLOAD_KEYS, code);
  ensure(
    payload.reservationVersion === RESERVATION_PAYLOAD_VERSION &&
      payload.conflictClass === SESSION_OPERATION_CONFLICT_CLASS &&
      typeof payload.requestSha256 === "string" &&
      regexpTest(SHA256_PATTERN, payload.requestSha256),
    code,
  );
  return deepFreeze({
    reservationVersion: RESERVATION_PAYLOAD_VERSION,
    conflictClass: SESSION_OPERATION_CONFLICT_CLASS,
    requestSha256: payload.requestSha256,
  });
}

function reservationSnapshotFromRow(row) {
  const code = "operation_state_invalid";
  const normalized = exactPlainObject(row, RESERVATION_ROW_KEYS, code);
  const reservationId = canonicalOpaqueId(
    normalized.reservation_id,
    128,
    code,
  );
  const operationId = canonicalOpaqueId(
    normalized.operation_id,
    128,
    code,
  );
  const sessionId = canonicalSessionId(normalized.session_id, code);
  const kind = canonicalOpaqueId(normalized.kind, 64, code);
  const expectedSessionRevision = canonicalRevisionForCode(
    normalized.expected_session_revision,
    code,
  );
  const state = canonicalOpaqueId(normalized.state, 32, code);
  ensure(
    reflectApply(arrayIncludesIntrinsic, ACTIVE_OPERATION_STATES, [state]) ||
      state === "released",
    code,
  );
  const payload = canonicalReservationPayload(normalized.payload, code);
  const createdAt = canonicalTimestampForCode(normalized.created_at, code);
  const updatedAt = canonicalTimestampForCode(normalized.updated_at, code);
  const expiresAt = canonicalNullableRowTimestamp(
    normalized.expires_at,
    code,
  );
  const releasedAt = canonicalNullableRowTimestamp(
    normalized.released_at,
    code,
  );
  ensure(
    expiresAt === null &&
      timestampMilliseconds(updatedAt) >= timestampMilliseconds(createdAt),
    code,
  );
  if (state === "prepared") {
    ensure(createdAt === updatedAt && releasedAt === null, code);
  } else if (state === "released") {
    ensure(releasedAt === updatedAt, code);
  } else {
    ensure(releasedAt === null, code);
  }
  return deepFreeze({
    reservationId,
    operationId,
    sessionId,
    kind,
    expectedSessionRevision,
    state,
    conflictClass: payload.conflictClass,
    requestSha256: payload.requestSha256,
    createdAt,
    updatedAt,
    expiresAt,
    releasedAt,
  });
}

function validateOperationIdentity(operation, input) {
  ensure(
    operation.operationId === input.operationId &&
      operation.sessionId === input.expectedSession.sessionId &&
      operation.kind === input.kind &&
      operation.requestSha256 === input.requestSha256 &&
      canonicalSnapshotBytes(operation.expectedSession) ===
        canonicalSnapshotBytes(input.expectedSession) &&
      canonicalSerialize(operation.request) === canonicalSerialize(input.request),
    "operation_identity_conflict",
  );
}

function validateOperationReservation(operation, reservation, input) {
  const expectedReservationState =
    operation.state === "committed" ? "released" : operation.state;
  ensure(
    reservation.reservationId === input.reservationId &&
      reservation.operationId === operation.operationId &&
      reservation.sessionId === operation.sessionId &&
      reservation.kind === operation.kind &&
      reservation.expectedSessionRevision ===
        operation.expectedSession.revision &&
      reservation.state === expectedReservationState &&
      reservation.requestSha256 === operation.requestSha256 &&
      reservation.createdAt === operation.createdAt &&
      reservation.updatedAt === operation.updatedAt &&
      (operation.state !== "committed" ||
        reservation.releasedAt === operation.retiredAt),
    "operation_state_invalid",
  );
}

function validateActivePointer(session, operation, reservation) {
  const active = session.document.activeOperation;
  ensure(
    active !== null &&
      operation.state !== "committed" &&
      active.operationId === operation.operationId &&
      active.reservationId === reservation.reservationId &&
      active.kind === operation.kind &&
      active.state === operation.state &&
      active.expectedSessionRevision ===
        operation.expectedSession.revision &&
      active.operationRevision === operation.revision &&
      active.requestSha256 === operation.requestSha256 &&
      active.conflictClass === SESSION_OPERATION_CONFLICT_CLASS &&
      canonicalIdentityBytes(session.document) ===
        canonicalIdentityBytes(operation.expectedSession.document) &&
      session.createdAt === operation.expectedSession.createdAt &&
      session.updatedAt === operation.updatedAt,
    "operation_state_invalid",
  );
}

function activePointerFor(input, state, operationRevision) {
  return deepFreeze({
    conflictClass: SESSION_OPERATION_CONFLICT_CLASS,
    expectedSessionRevision: input.expectedSession.revision,
    kind: input.kind,
    operationId: input.operationId,
    operationRevision,
    requestSha256: input.requestSha256,
    reservationId: input.reservationId,
    state,
  });
}

function documentWithActiveOperation(document, activeOperation) {
  return deepFreeze({
    documentVersion: document.documentVersion,
    manifest: document.manifest,
    storageRef: document.storageRef,
    backendCapabilities: document.backendCapabilities,
    lifecycle: document.lifecycle,
    writerEpoch: document.writerEpoch,
    lease: document.lease,
    attachment: document.attachment,
    activeOperation,
    recovery: document.recovery,
    launch: document.launch,
  });
}

function operationReceipt({
  operation,
  reservation,
  session,
  status = operation?.state ?? "absent",
  ...flags
}) {
  return deepFreeze({
    status,
    session,
    operation,
    reservation,
    ...flags,
  });
}

async function readSessionSnapshot(transaction, sessionId, forUpdate) {
  const rows = rowsFromResult(
    await transaction.query(
      forUpdate
        ? READ_SESSION_FOR_UPDATE_QUERY.text
        : READ_SESSION_QUERY.text,
      [sessionId],
    ),
  );
  ensure(rows.length === 1, "session_not_found");
  return snapshotFromRow(rows[0], sessionId);
}

async function readOperationSnapshot(transaction, operationId, forUpdate) {
  const rows = rowsFromResult(
    await transaction.query(
      forUpdate
        ? READ_OPERATION_FOR_UPDATE_QUERY.text
        : READ_OPERATION_QUERY.text,
      [operationId],
    ),
    "operation_state_invalid",
  );
  return rows.length === 0 ? null : operationSnapshotFromRow(rows[0]);
}

async function readReservationSnapshot(
  transaction,
  operationId,
  forUpdate,
) {
  const rows = rowsFromResult(
    await transaction.query(
      forUpdate
        ? READ_RESERVATION_BY_OPERATION_FOR_UPDATE_QUERY.text
        : READ_RESERVATION_BY_OPERATION_QUERY.text,
      [operationId],
    ),
    "operation_state_invalid",
  );
  return rows.length === 0 ? null : reservationSnapshotFromRow(rows[0]);
}

async function ensureNoActiveRows(transaction, sessionId) {
  const rows = rowsFromResult(
    await transaction.query(READ_ACTIVE_COUNTS_QUERY.text, [sessionId]),
    "operation_state_invalid",
  );
  ensure(rows.length === 1, "operation_state_invalid");
  const counts = exactPlainObject(
    rows[0],
    ["operation_count", "reservation_count"],
    "operation_state_invalid",
  );
  ensure(
    counts.operation_count === 0 && counts.reservation_count === 0,
    "operation_state_invalid",
  );
}

async function validateSessionRelations(transaction, session, forUpdate) {
  const active = session.document.activeOperation;
  if (active === null) {
    await ensureNoActiveRows(transaction, session.sessionId);
    return null;
  }
  const operation = await readOperationSnapshot(
    transaction,
    active.operationId,
    forUpdate,
  );
  ensure(operation !== null, "operation_state_invalid");
  const reservation = await readReservationSnapshot(
    transaction,
    active.operationId,
    forUpdate,
  );
  ensure(reservation !== null, "operation_state_invalid");
  validateOperationReservation(operation, reservation, {
    reservationId: active.reservationId,
  });
  validateActivePointer(session, operation, reservation);
  return deepFreeze({ operation, reservation });
}

async function readRequestedOperation(
  transaction,
  session,
  input,
  forUpdate,
) {
  const active = await validateSessionRelations(
    transaction,
    session,
    forUpdate,
  );
  const requestedIsActive =
    active?.operation.operationId === input.operationId;
  // Mutations already hold this session row and its active relation locks.
  // A foreign or retired operation is identity evidence only; locking it
  // would allow crossed foreign IDs to create an avoidable lock cycle.
  let operation = requestedIsActive
    ? active.operation
    : await readOperationSnapshot(
        transaction,
        input.operationId,
        false,
      );
  if (operation === null) {
    return deepFreeze({ active, operation: null, reservation: null });
  }
  validateOperationIdentity(operation, input);
  const reservation = requestedIsActive
    ? active.reservation
    : await readReservationSnapshot(
        transaction,
        input.operationId,
        false,
      );
  ensure(reservation !== null, "operation_state_invalid");
  validateOperationReservation(operation, reservation, input);
  if (operation.state !== "committed") {
    validateActivePointer(session, operation, reservation);
  }
  return deepFreeze({ active, operation, reservation });
}

function ensureExactExpectedSession(session, expected) {
  if (
    canonicalIdentityBytes(session.document) !==
    canonicalIdentityBytes(expected.document)
  ) {
    fail("session_identity_conflict");
  }
  ensure(
    canonicalSnapshotBytes(session) === canonicalSnapshotBytes(expected),
    "session_revision_conflict",
  );
}

function nextRevision(value, code = "session_revision_exhausted") {
  const revision = BigIntConstructor(value);
  ensure(revision < MAX_POSTGRES_BIGINT, code);
  return reflectApply(bigIntToStringIntrinsic, revision + 1n, []);
}

function reservationPayload(input) {
  return deepFreeze({
    reservationVersion: RESERVATION_PAYLOAD_VERSION,
    conflictClass: SESSION_OPERATION_CONFLICT_CLASS,
    requestSha256: input.requestSha256,
  });
}

async function updateSessionPhase(
  transaction,
  session,
  input,
  activeOperation,
) {
  const nextDocument = documentWithActiveOperation(
    session.document,
    activeOperation,
  );
  const rows = rowsFromResult(
    await transaction.query(UPDATE_SESSION_QUERY.text, [
      session.sessionId,
      session.revision,
      canonicalSerialize(nextDocument),
      transaction.now,
    ]),
  );
  ensure(rows.length === 1, "session_revision_conflict");
  const updated = snapshotFromRow(rows[0], session.sessionId);
  ensure(
    updated.revision === nextRevision(session.revision) &&
      updated.updatedAt === transaction.now &&
      canonicalSerialize(updated.document) ===
        canonicalSerialize(nextDocument) &&
      canonicalIdentityBytes(updated.document) ===
        canonicalIdentityBytes(input.expectedSession.document),
    "session_state_invalid",
  );
  return updated;
}

function runSerializable(store, callback) {
  return reflectApply(runSerializableIntrinsic, store, [callback]);
}

export class PostgresSessionAuthority {
  #store;

  constructor(options) {
    const normalized = exactPlainObject(
      options,
      ["store"],
      "invalid_authority_options",
    );
    let prototype;
    let ownKeys;
    try {
      prototype = objectGetPrototypeOf(normalized.store);
      ownKeys = reflectOwnKeys(normalized.store);
    } catch {
      fail("invalid_authority_options");
    }
    ensure(
      normalized.store !== null &&
        typeof normalized.store === "object" &&
        !isProxyValue(normalized.store) &&
        prototype === PostgresSerializableStore.prototype &&
        ownKeys.length === 0 &&
        objectIsFrozen(normalized.store),
      "invalid_authority_options",
    );
    this.#store = normalized.store;
    objectFreeze(this);
  }

  async registerSession(options) {
    const document = registrationDocument(options);
    const sessionId = document.manifest.sessionId;
    const serializedDocument = canonicalSerialize(document);
    return runSerializable(this.#store, async (transaction) => {
      const inserted = rowsFromResult(
        await transaction.query(INSERT_SESSION_QUERY.text, [
          sessionId,
          serializedDocument,
          transaction.now,
        ]),
      );
      if (inserted.length === 1) {
        const snapshot = snapshotFromRow(inserted[0], sessionId);
        ensure(
          snapshot.revision === "0" &&
            snapshot.createdAt === transaction.now &&
            snapshot.updatedAt === transaction.now &&
            canonicalSerialize(snapshot.document) === serializedDocument,
          "session_state_invalid",
        );
        return snapshot;
      }
      const existing = rowsFromResult(
        await transaction.query(READ_SESSION_FOR_UPDATE_QUERY.text, [
          sessionId,
        ]),
      );
      ensure(existing.length === 1, "session_state_invalid");
      const snapshot = snapshotFromRow(existing[0], sessionId);
      ensure(
        canonicalIdentityBytes(snapshot.document) ===
          canonicalIdentityBytes(document),
        "session_identity_conflict",
      );
      await validateSessionRelations(transaction, snapshot, true);
      return snapshot;
    });
  }

  async readSession(options) {
    const normalized = exactPlainObject(
      options,
      ["sessionId"],
      "invalid_session_read",
    );
    const sessionId = canonicalSessionId(
      normalized.sessionId,
      "invalid_session_read",
    );
    return runSerializable(this.#store, async (transaction) => {
      const rows = rowsFromResult(
        await transaction.query(READ_SESSION_QUERY.text, [sessionId]),
      );
      ensure(rows.length === 1, "session_not_found");
      const snapshot = snapshotFromRow(rows[0], sessionId);
      await validateSessionRelations(transaction, snapshot, false);
      return snapshot;
    });
  }

  async reserveOperation(options) {
    const input = canonicalOperationInput(options);
    const reserve = () =>
      runSerializable(this.#store, async (transaction) => {
        const session = await readSessionSnapshot(
          transaction,
          input.expectedSession.sessionId,
          true,
        );
        const observed = await readRequestedOperation(
          transaction,
          session,
          input,
          true,
        );
        if (observed.operation !== null) {
          return operationReceipt({
            acquired: false,
            operation: observed.operation,
            reservation: observed.reservation,
            session,
          });
        }
        ensure(observed.active === null, "session_operation_conflict");
        ensureExactExpectedSession(session, input.expectedSession);
        nextRevision(session.revision);

        const operationRows = rowsFromResult(
          await transaction.query(INSERT_OPERATION_QUERY.text, [
            input.operationId,
            session.sessionId,
            input.kind,
            input.serializedEnvelope,
            transaction.now,
          ]),
          "operation_state_invalid",
        );
        if (operationRows.length === 0) {
          const existing = await readOperationSnapshot(
            transaction,
            input.operationId,
            true,
          );
          if (existing === null) {
            throw OPERATION_VISIBILITY_RETRY;
          }
          validateOperationIdentity(existing, input);
          fail("operation_state_invalid");
        }
        const operation = operationSnapshotFromRow(operationRows[0]);
        validateOperationIdentity(operation, input);

        const payload = reservationPayload(input);
        const reservationRows = rowsFromResult(
          await transaction.query(INSERT_RESERVATION_QUERY.text, [
            input.reservationId,
            input.operationId,
            session.sessionId,
            input.kind,
            session.revision,
            canonicalSerialize(payload),
            transaction.now,
          ]),
          "operation_state_invalid",
        );
        ensure(reservationRows.length === 1, "operation_state_invalid");
        const reservation = reservationSnapshotFromRow(reservationRows[0]);
        validateOperationReservation(operation, reservation, input);

        const updatedSession = await updateSessionPhase(
          transaction,
          session,
          input,
          activePointerFor(input, "prepared", "0"),
        );
        validateActivePointer(updatedSession, operation, reservation);
        return operationReceipt({
          acquired: true,
          operation,
          reservation,
          session: updatedSession,
        });
      });
    try {
      return await reserve();
    } catch (error) {
      if (error !== OPERATION_VISIBILITY_RETRY) {
        throw error;
      }
    }
    try {
      return await reserve();
    } catch (error) {
      if (error === OPERATION_VISIBILITY_RETRY) {
        fail("operation_state_invalid");
      }
      throw error;
    }
  }

  async reconcileOperation(options) {
    const input = canonicalOperationInput(options);
    return runSerializable(this.#store, async (transaction) => {
      const session = await readSessionSnapshot(
        transaction,
        input.expectedSession.sessionId,
        false,
      );
      const observed = await readRequestedOperation(
        transaction,
        session,
        input,
        false,
      );
      if (observed.operation !== null) {
        return operationReceipt({
          operation: observed.operation,
          reservation: observed.reservation,
          session,
        });
      }
      ensure(observed.active === null, "session_operation_conflict");
      ensureExactExpectedSession(session, input.expectedSession);
      return operationReceipt({
        operation: null,
        reservation: null,
        session,
        status: "absent",
      });
    });
  }

  async claimOperationDispatch(options) {
    const input = operationInputWithExpectedRevision(options, "0");
    return runSerializable(this.#store, async (transaction) => {
      const session = await readSessionSnapshot(
        transaction,
        input.expectedSession.sessionId,
        true,
      );
      const observed = await readRequestedOperation(
        transaction,
        session,
        input,
        true,
      );
      ensure(
        observed.operation !== null && observed.reservation !== null,
        "operation_transition_conflict",
      );
      if (observed.operation.state !== "prepared") {
        return operationReceipt({
          dispatchGranted: false,
          operation: observed.operation,
          reservation: observed.reservation,
          session,
        });
      }
      ensure(
        observed.operation.revision === input.expectedOperationRevision,
        "operation_transition_conflict",
      );
      nextRevision(session.revision);
      const operationRows = rowsFromResult(
        await transaction.query(START_OPERATION_QUERY.text, [
          input.operationId,
          input.expectedOperationRevision,
          transaction.now,
        ]),
        "operation_state_invalid",
      );
      ensure(operationRows.length === 1, "operation_transition_conflict");
      const reservationRows = rowsFromResult(
        await transaction.query(START_RESERVATION_QUERY.text, [
          input.operationId,
          transaction.now,
        ]),
        "operation_state_invalid",
      );
      ensure(reservationRows.length === 1, "operation_transition_conflict");
      const operation = operationSnapshotFromRow(operationRows[0]);
      const reservation = reservationSnapshotFromRow(reservationRows[0]);
      validateOperationIdentity(operation, input);
      validateOperationReservation(operation, reservation, input);
      const updatedSession = await updateSessionPhase(
        transaction,
        session,
        input,
        activePointerFor(input, "starting", "1"),
      );
      validateActivePointer(updatedSession, operation, reservation);
      return operationReceipt({
        dispatchGranted: true,
        operation,
        reservation,
        session: updatedSession,
      });
    });
  }

  async markOperationUncertain(options) {
    const input = operationInputWithExpectedRevision(options, "1");
    return runSerializable(this.#store, async (transaction) => {
      const session = await readSessionSnapshot(
        transaction,
        input.expectedSession.sessionId,
        true,
      );
      const observed = await readRequestedOperation(
        transaction,
        session,
        input,
        true,
      );
      ensure(
        observed.operation !== null && observed.reservation !== null,
        "operation_transition_conflict",
      );
      if (observed.operation.state === "uncertain") {
        return operationReceipt({
          changed: false,
          operation: observed.operation,
          reservation: observed.reservation,
          session,
        });
      }
      ensure(
        observed.operation.state === "starting" &&
          observed.operation.revision === input.expectedOperationRevision,
        "operation_transition_conflict",
      );
      nextRevision(session.revision);
      const operationRows = rowsFromResult(
        await transaction.query(UNCERTAIN_OPERATION_QUERY.text, [
          input.operationId,
          input.expectedOperationRevision,
          transaction.now,
        ]),
        "operation_state_invalid",
      );
      ensure(operationRows.length === 1, "operation_transition_conflict");
      const reservationRows = rowsFromResult(
        await transaction.query(UNCERTAIN_RESERVATION_QUERY.text, [
          input.operationId,
          transaction.now,
        ]),
        "operation_state_invalid",
      );
      ensure(reservationRows.length === 1, "operation_transition_conflict");
      const operation = operationSnapshotFromRow(operationRows[0]);
      const reservation = reservationSnapshotFromRow(reservationRows[0]);
      validateOperationIdentity(operation, input);
      validateOperationReservation(operation, reservation, input);
      const updatedSession = await updateSessionPhase(
        transaction,
        session,
        input,
        activePointerFor(input, "uncertain", "2"),
      );
      validateActivePointer(updatedSession, operation, reservation);
      return operationReceipt({
        changed: true,
        operation,
        reservation,
        session: updatedSession,
      });
    });
  }

  async cancelPreparedOperation(options) {
    const input = cancellationInput(options);
    return runSerializable(this.#store, async (transaction) => {
      const session = await readSessionSnapshot(
        transaction,
        input.expectedSession.sessionId,
        true,
      );
      const observed = await readRequestedOperation(
        transaction,
        session,
        input,
        true,
      );
      ensure(
        observed.operation !== null && observed.reservation !== null,
        "operation_transition_conflict",
      );
      if (observed.operation.state === "committed") {
        ensure(
          canonicalSerialize(observed.operation.result) ===
            canonicalSerialize(input.result),
          "operation_result_conflict",
        );
        return operationReceipt({
          cancelled: false,
          operation: observed.operation,
          reservation: observed.reservation,
          session,
        });
      }
      ensure(
        observed.operation.state === "prepared" &&
          observed.operation.revision === input.expectedOperationRevision,
        "operation_transition_conflict",
      );
      nextRevision(session.revision);
      const operationRows = rowsFromResult(
        await transaction.query(CANCEL_OPERATION_QUERY.text, [
          input.operationId,
          input.expectedOperationRevision,
          input.serializedResult,
          transaction.now,
        ]),
        "operation_state_invalid",
      );
      ensure(operationRows.length === 1, "operation_transition_conflict");
      const reservationRows = rowsFromResult(
        await transaction.query(RELEASE_RESERVATION_QUERY.text, [
          input.operationId,
          transaction.now,
        ]),
        "operation_state_invalid",
      );
      ensure(reservationRows.length === 1, "operation_transition_conflict");
      const operation = operationSnapshotFromRow(operationRows[0]);
      const reservation = reservationSnapshotFromRow(reservationRows[0]);
      validateOperationIdentity(operation, input);
      validateOperationReservation(operation, reservation, input);
      ensure(
        canonicalSerialize(operation.result) ===
          canonicalSerialize(input.result),
        "operation_result_conflict",
      );
      const updatedSession = await updateSessionPhase(
        transaction,
        session,
        input,
        null,
      );
      return operationReceipt({
        cancelled: true,
        operation,
        reservation,
        session: updatedSession,
      });
    });
  }
}
