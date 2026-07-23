import { Hash, createHash } from "node:crypto";
import { EventEmitter } from "node:events";
import { readFile } from "node:fs/promises";
import { isPromise, isProxy } from "node:util/types";

import { DatabaseError } from "pg";

export const SESSION_AUTHORITY_MIGRATION_VERSION = 1;
export const DEFAULT_TRANSACTION_ATTEMPTS = 3;
export const MAX_TRANSACTION_ATTEMPTS = 16;

const MIGRATION_URL = new URL(
  "../migrations/authority/001-session-authority.sql",
  import.meta.url,
);
const MIGRATION_LOCK_KEY = "7275632827684484689";
const ArrayConstructor = Array;
const arrayEveryIntrinsic = Array.prototype.every;
const arrayIncludesIntrinsic = Array.prototype.includes;
const arrayIsArray = Array.isArray;
const arrayJoinIntrinsic = Array.prototype.join;
const arrayPushIntrinsic = Array.prototype.push;
const DateConstructor = Date;
const dateGetTimeIntrinsic = Date.prototype.getTime;
const dateParse = Date.parse;
const dateToISOStringIntrinsic = Date.prototype.toISOString;
const databaseErrorPrototype = DatabaseError.prototype;
const ErrorConstructor = Error;
const eventEmitterEmitIntrinsic = EventEmitter.prototype.emit;
const eventEmitterPrependListenerIntrinsic =
  EventEmitter.prototype.prependListener;
const eventEmitterPrototype = EventEmitter.prototype;
const eventEmitterRemoveListenerIntrinsic =
  EventEmitter.prototype.removeListener;
const createHashIntrinsic = createHash;
const hashDigestIntrinsic = Hash.prototype.digest;
const hashUpdateIntrinsic = Hash.prototype.update;
const numberIsFinite = Number.isFinite;
const numberIsSafeInteger = Number.isSafeInteger;
const numberNaN = Number.NaN;
const objectCreate = Object.create;
const objectDefineProperties = Object.defineProperties;
const objectDefineProperty = Object.defineProperty;
const objectFreeze = Object.freeze;
const objectGetOwnPropertyDescriptor = Object.getOwnPropertyDescriptor;
const objectGetPrototypeOf = Object.getPrototypeOf;
const objectHasOwn = Object.hasOwn;
const objectIs = Object.is;
const objectIsPrototypeOfIntrinsic = Object.prototype.isPrototypeOf;
const objectPrototype = Object.prototype;
const PromiseConstructor = Promise;
const reflectApply = Reflect.apply;
const reflectDeleteProperty = Reflect.deleteProperty;
const reflectOwnKeys = Reflect.ownKeys;
const regexpExecIntrinsic = RegExp.prototype.exec;
const setAddIntrinsic = Set.prototype.add;
const setDeleteIntrinsic = Set.prototype.delete;
const setForEachIntrinsic = Set.prototype.forEach;
const setHasIntrinsic = Set.prototype.has;
const setSizeGetter = objectGetOwnPropertyDescriptor(
  Set.prototype,
  "size",
).get;
const SetConstructor = Set;
const StringConstructor = String;
const stringEndsWithIntrinsic = String.prototype.endsWith;
const stringSliceIntrinsic = String.prototype.slice;
const TypeErrorConstructor = TypeError;
const weakMapDeleteIntrinsic = WeakMap.prototype.delete;
const weakMapGetIntrinsic = WeakMap.prototype.get;
const weakMapHasIntrinsic = WeakMap.prototype.has;
const weakMapSetIntrinsic = WeakMap.prototype.set;
const WeakMapConstructor = WeakMap;
const weakSetAddIntrinsic = WeakSet.prototype.add;
const weakSetHasIntrinsic = WeakSet.prototype.has;
const WeakSetConstructor = WeakSet;
const protocolEmitDescriptor = objectFreeze({
  configurable: true,
  enumerable: false,
  value: eventEmitterEmitIntrinsic,
  writable: false,
});

function callIntrinsic(intrinsic, receiver, args) {
  return reflectApply(intrinsic, receiver, args);
}

function sha256Hex(value, encoding) {
  const hash = createHashIntrinsic("sha256");
  callIntrinsic(hashUpdateIntrinsic, hash, [value, encoding]);
  return callIntrinsic(hashDigestIntrinsic, hash, ["hex"]);
}

function arrayEvery(value, callback) {
  return callIntrinsic(arrayEveryIntrinsic, value, [callback]);
}

function arrayIncludes(value, candidate) {
  return callIntrinsic(arrayIncludesIntrinsic, value, [candidate]);
}

function arrayJoin(value, separator) {
  return callIntrinsic(arrayJoinIntrinsic, value, [separator]);
}

function arrayPush(value, entry) {
  return callIntrinsic(arrayPushIntrinsic, value, [entry]);
}

function dateGetTime(value) {
  return callIntrinsic(dateGetTimeIntrinsic, value, []);
}

function dateToISOString(value) {
  return callIntrinsic(dateToISOStringIntrinsic, value, []);
}

function regexpTest(pattern, value) {
  return callIntrinsic(regexpExecIntrinsic, pattern, [value]) !== null;
}

function objectIsPrototypeOf(prototype, value) {
  return callIntrinsic(objectIsPrototypeOfIntrinsic, prototype, [value]);
}

function ownDataValue(value, key) {
  try {
    const valueType = typeof value;
    if (
      value === null ||
      (valueType !== "object" && valueType !== "function") ||
      isProxy(value)
    ) {
      return undefined;
    }
    const descriptor = objectGetOwnPropertyDescriptor(value, key);
    return descriptor !== undefined && objectHasOwn(descriptor, "value")
      ? descriptor.value
      : undefined;
  } catch {
    return undefined;
  }
}

function setAdd(value, entry) {
  return callIntrinsic(setAddIntrinsic, value, [entry]);
}

function setDelete(value, entry) {
  return callIntrinsic(setDeleteIntrinsic, value, [entry]);
}

function setForEach(value, callback) {
  return callIntrinsic(setForEachIntrinsic, value, [callback]);
}

function setHas(value, entry) {
  return callIntrinsic(setHasIntrinsic, value, [entry]);
}

function setSize(value) {
  return callIntrinsic(setSizeGetter, value, []);
}

function stringEndsWith(value, suffix) {
  return callIntrinsic(stringEndsWithIntrinsic, value, [suffix]);
}

function stringSlice(value, start, end) {
  return callIntrinsic(stringSliceIntrinsic, value, [start, end]);
}

function weakMapDelete(value, key) {
  return callIntrinsic(weakMapDeleteIntrinsic, value, [key]);
}

function weakMapGet(value, key) {
  return callIntrinsic(weakMapGetIntrinsic, value, [key]);
}

function weakMapHas(value, key) {
  return callIntrinsic(weakMapHasIntrinsic, value, [key]);
}

function weakMapSet(value, key, entry) {
  return callIntrinsic(weakMapSetIntrinsic, value, [key, entry]);
}

function weakSetAdd(value, entry) {
  return callIntrinsic(weakSetAddIntrinsic, value, [entry]);
}

function weakSetHas(value, entry) {
  return callIntrinsic(weakSetHasIntrinsic, value, [entry]);
}

function protectPromise(value) {
  if (!isPromise(value)) return value;
  objectDefineProperty(value, "constructor", {
    configurable: false,
    enumerable: false,
    value: PromiseConstructor,
    writable: false,
  });
  return value;
}

const RETRYABLE_TRANSACTION_CODES = new SetConstructor(["40001", "40P01"]);
const QUERY_PARAMETER_TYPES = new SetConstructor([
  "bigint",
  "boolean",
  "number",
  "string",
  "undefined",
]);
const MAX_QUERY_PARAMETERS = 65_535;
const COMMIT_STATES = new SetConstructor([
  "committed",
  "not-committed",
  "uncertain",
]);
const PROTOCOL_ERROR_SQLSTATES = new WeakMapConstructor();
const STORE_ERRORS = new WeakSetConstructor();
const ERROR_MESSAGES = objectFreeze({
  client_reset_failed: "PostgreSQL client reset failed",
  client_release_failed: "PostgreSQL client release failed",
  connection_failed: "PostgreSQL connection acquisition failed",
  migration_checksum_mismatch: "Authority schema migration checksum mismatch",
  migration_failed: "Authority schema migration failed",
  migration_source_failed: "Authority schema migration source could not be read",
  migration_state_invalid: "Authority schema migration state is invalid",
  serialization_retry_exhausted: "Serializable transaction retry limit was exhausted",
  transaction_begin_failed: "Serializable transaction could not begin",
  transaction_boundary_lost: "Serializable transaction boundary was lost",
  transaction_commit_outcome_uncertain:
    "Serializable transaction commit outcome is uncertain",
  transaction_query_inactive: "Transaction query capability is no longer active",
  transaction_query_failed: "Transaction query failed",
  transaction_query_invalid: "Transaction query arguments are invalid",
  transaction_query_pending:
    "Transaction callback returned with an unsettled query",
  transaction_rolled_back: "Serializable transaction was rolled back",
  transaction_rollback_failed: "Serializable transaction rollback failed",
  transaction_timestamp_failed:
    "Serializable transaction timestamp could not be established",
});

export class PostgresSerializableStoreError extends Error {
  constructor(code, commitState = "not-committed") {
    if (
      !objectHasOwn(ERROR_MESSAGES, code) ||
      !setHas(COMMIT_STATES, commitState)
    ) {
      throw new TypeErrorConstructor(
        "unsupported PostgreSQL serializable store error",
      );
    }
    super(ERROR_MESSAGES[code]);
    objectDefineProperties(this, {
      name: {
        configurable: true,
        enumerable: true,
        value: "PostgresSerializableStoreError",
        writable: true,
      },
      code: {
        configurable: true,
        enumerable: true,
        value: code,
        writable: true,
      },
      commitState: {
        configurable: true,
        enumerable: true,
        value: commitState,
        writable: true,
      },
      retryable: {
        configurable: true,
        enumerable: true,
        value: false,
        writable: true,
      },
    });
    weakSetAdd(STORE_ERRORS, this);
    objectFreeze(this);
  }
}

const postgresSerializableStoreErrorPrototype =
  PostgresSerializableStoreError.prototype;

function isPostgresSerializableStoreError(error) {
  try {
    const errorType = typeof error;
    return (
      error !== null &&
      (errorType === "object" || errorType === "function") &&
      (isProxy(error) ||
        weakSetHas(STORE_ERRORS, error) ||
        objectIsPrototypeOf(
          postgresSerializableStoreErrorPrototype,
          error,
        ))
    );
  } catch {
    return true;
  }
}

function storeError(code, commitState = "not-committed") {
  return new PostgresSerializableStoreError(code, commitState);
}

function observedRejectedPromise(error) {
  const rejection = protectPromise(
    (async () => {
      throw error;
    })(),
  );
  void (async () => {
    try {
      await rejection;
    } catch {
      // The returned rejection remains observable without becoming unhandled.
    }
  })();
  return rejection;
}

function inspectOptions(options) {
  if (
    options === null ||
    typeof options !== "object" ||
    arrayIsArray(options) ||
    !arrayIncludes([objectPrototype, null], objectGetPrototypeOf(options))
  ) {
    throw new TypeErrorConstructor(
      "PostgreSQL serializable store options must be a plain object",
    );
  }
  const keys = reflectOwnKeys(options);
  if (
    !arrayEvery(
      keys,
      (key) =>
        typeof key === "string" &&
        arrayIncludes(["dedicatedPool", "maxTransactionAttempts"], key),
    ) ||
    !arrayIncludes(keys, "dedicatedPool")
  ) {
    throw new TypeErrorConstructor(
      "PostgreSQL serializable store options contain unexpected or missing fields",
    );
  }
  const normalized = objectCreate(null);
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
    const descriptor = objectGetOwnPropertyDescriptor(options, key);
    if (!descriptor?.enumerable || !objectHasOwn(descriptor, "value")) {
      throw new TypeErrorConstructor(
        "PostgreSQL serializable store options must use plain data fields",
      );
    }
    normalized[key] = descriptor.value;
  }
  return normalized;
}

function validateDedicatedPool(pool) {
  if (
    pool === null ||
    !arrayIncludes(["object", "function"], typeof pool) ||
    typeof pool.connect !== "function"
  ) {
    throw new TypeErrorConstructor("dedicatedPool must provide connect()");
  }
  return pool;
}

function validateAttemptLimit(value) {
  if (
    !numberIsSafeInteger(value) ||
    value < 1 ||
    value > MAX_TRANSACTION_ATTEMPTS
  ) {
    throw new TypeErrorConstructor(
      `maxTransactionAttempts must be an integer from 1 through ${MAX_TRANSACTION_ATTEMPTS}`,
    );
  }
  return value;
}

function observedProtocolSqlState(error) {
  try {
    const sqlState = weakMapGet(PROTOCOL_ERROR_SQLSTATES, error);
    weakMapDelete(PROTOCOL_ERROR_SQLSTATES, error);
    return sqlState;
  } catch {
    return undefined;
  }
}

function hasRetryableTransactionSqlState(error) {
  return setHas(
    RETRYABLE_TRANSACTION_CODES,
    observedProtocolSqlState(error),
  );
}

function isTrustedUserQueryRejectionSqlState(sqlState) {
  return (
    sqlState !== undefined &&
    sqlState !== "40003" &&
    !arrayIncludes(["08", "57", "58", "XX"], stringSlice(sqlState, 0, 2))
  );
}

function copyQueryValues(values) {
  try {
    if (
      isProxy(values) ||
      !arrayIsArray(values) ||
      objectGetPrototypeOf(values) !== ArrayConstructor.prototype
    ) {
      throw new TypeErrorConstructor(
        "query values must use the built-in Array prototype",
      );
    }
    const lengthDescriptor = objectGetOwnPropertyDescriptor(values, "length");
    const length = lengthDescriptor?.value;
    if (
      !objectHasOwn(lengthDescriptor ?? {}, "value") ||
      !numberIsSafeInteger(length) ||
      length < 0 ||
      length > MAX_QUERY_PARAMETERS
    ) {
      throw new TypeErrorConstructor("query values length is invalid");
    }

    const copied = new ArrayConstructor(length);
    for (let index = 0; index < length; index += 1) {
      const descriptor = objectGetOwnPropertyDescriptor(
        values,
        StringConstructor(index),
      );
      if (descriptor !== undefined && !objectHasOwn(descriptor, "value")) {
        throw new TypeErrorConstructor(
          "query values must use plain data fields",
        );
      }
      const value = descriptor?.value;
      if (
        value !== null &&
        !setHas(QUERY_PARAMETER_TYPES, typeof value)
      ) {
        throw new TypeErrorConstructor("query values must be primitive data");
      }
      objectDefineProperty(copied, StringConstructor(index), {
        configurable: true,
        enumerable: true,
        value,
        writable: true,
      });
    }
    return objectFreeze(copied);
  } catch {
    throw storeError("transaction_query_invalid");
  }
}

function isPostgresSqlWhitespace(character) {
  return (
    character === " " ||
    character === "\t" ||
    character === "\n" ||
    character === "\v" ||
    character === "\f" ||
    character === "\r"
  );
}

function skipPostgresSqlTrivia(text, start) {
  let cursor = start;
  while (cursor < text.length) {
    if (isPostgresSqlWhitespace(text[cursor])) {
      cursor += 1;
      continue;
    }
    if (
      text[cursor] === "-" &&
      cursor + 1 < text.length &&
      text[cursor + 1] === "-"
    ) {
      cursor += 2;
      while (
        cursor < text.length &&
        text[cursor] !== "\n" &&
        text[cursor] !== "\r"
      ) {
        cursor += 1;
      }
      continue;
    }
    if (
      text[cursor] === "/" &&
      cursor + 1 < text.length &&
      text[cursor + 1] === "*"
    ) {
      let depth = 1;
      cursor += 2;
      while (cursor < text.length && depth > 0) {
        if (
          text[cursor] === "/" &&
          cursor + 1 < text.length &&
          text[cursor + 1] === "*"
        ) {
          depth += 1;
          cursor += 2;
        } else if (
          text[cursor] === "*" &&
          cursor + 1 < text.length &&
          text[cursor + 1] === "/"
        ) {
          depth -= 1;
          cursor += 2;
        } else {
          cursor += 1;
        }
      }
      if (depth > 0) return text.length;
      continue;
    }
    break;
  }
  return cursor;
}

function isPostgresIdentifierContinuation(character) {
  return (
    character === "_" ||
    character === "$" ||
    (character >= "0" && character <= "9") ||
    (character >= "A" && character <= "Z") ||
    (character >= "a" && character <= "z") ||
    character >= "\u0080"
  );
}

function hasPostgresKeyword(text, start, uppercase, lowercase) {
  const end = start + uppercase.length;
  if (start < 0 || end > text.length) return false;
  for (let index = 0; index < uppercase.length; index += 1) {
    const character = text[start + index];
    if (character !== uppercase[index] && character !== lowercase[index]) {
      return false;
    }
  }
  return (
    end === text.length ||
    !isPostgresIdentifierContinuation(text[end])
  );
}

function isPrepareTransactionStatement(text) {
  let prepareStart = skipPostgresSqlTrivia(text, 0);
  while (prepareStart < text.length && text[prepareStart] === ";") {
    prepareStart = skipPostgresSqlTrivia(text, prepareStart + 1);
  }
  if (!hasPostgresKeyword(text, prepareStart, "PREPARE", "prepare")) {
    return false;
  }
  const afterPrepare = prepareStart + "PREPARE".length;
  const transactionStart = skipPostgresSqlTrivia(text, afterPrepare);
  if (
    transactionStart === afterPrepare ||
    !hasPostgresKeyword(
      text,
      transactionStart,
      "TRANSACTION",
      "transaction",
    )
  ) {
    return false;
  }
  const afterTransaction = transactionStart + "TRANSACTION".length;
  const argumentStart = skipPostgresSqlTrivia(text, afterTransaction);
  return !(
    hasPostgresKeyword(text, argumentStart, "AS", "as") ||
    (argumentStart < text.length && text[argumentStart] === "(")
  );
}

function validateClient(client) {
  try {
    const connection = client.connection;
    return (
      client !== null &&
      arrayIncludes(["object", "function"], typeof client) &&
      typeof client.query === "function" &&
      typeof client.release === "function" &&
      connection !== null &&
      typeof connection === "object" &&
      !isProxy(connection) &&
      objectIsPrototypeOf(eventEmitterPrototype, connection)
    );
  } catch {
    return false;
  }
}

async function clientQuery(client, ...args) {
  const connection = client.connection;
  const priorEmitDescriptor = objectGetOwnPropertyDescriptor(
    connection,
    "emit",
  );
  const observed = new WeakMapConstructor();
  const observeProtocolError = (error) => {
    try {
      const name = ownDataValue(error, "name");
      const code = ownDataValue(error, "code");
      if (
        objectIsPrototypeOf(databaseErrorPrototype, error) &&
        name === "error" &&
        typeof code === "string" &&
        regexpTest(/^[0-9A-Z]{5}$/u, code)
      ) {
        weakMapSet(observed, error, code);
      }
    } catch {
      // An invalid event payload cannot become trusted protocol evidence.
    }
  };

  // Keep the driver-originated protocol event channel independent from
  // callback-controlled EventEmitter prototype mutations for this query.
  objectDefineProperty(connection, "emit", protocolEmitDescriptor);
  let listenerAttached = false;
  try {
    callIntrinsic(
      eventEmitterPrependListenerIntrinsic,
      connection,
      ["errorMessage", observeProtocolError],
    );
    listenerAttached = true;
    try {
      return await protectPromise(
        reflectApply(client.query, client, args),
      );
    } catch (error) {
      try {
        weakMapDelete(PROTOCOL_ERROR_SQLSTATES, error);
        const sqlState = weakMapGet(observed, error);
        if (sqlState !== undefined) {
          weakMapSet(PROTOCOL_ERROR_SQLSTATES, error, sqlState);
        }
      } catch {
        // Preserve the query failure without adding protocol provenance.
      }
      throw error;
    }
  } finally {
    try {
      if (listenerAttached) {
        callIntrinsic(
          eventEmitterRemoveListenerIntrinsic,
          connection,
          ["errorMessage", observeProtocolError],
        );
      }
    } finally {
      if (priorEmitDescriptor === undefined) {
        if (!reflectDeleteProperty(connection, "emit")) {
          throw new TypeErrorConstructor(
            "PostgreSQL protocol emitter could not be restored",
          );
        }
      } else {
        objectDefineProperty(
          connection,
          "emit",
          priorEmitDescriptor,
        );
      }
    }
  }
}

async function acquireClient(pool) {
  let client;
  try {
    client = await protectPromise(
      reflectApply(pool.connect, pool, []),
    );
  } catch {
    throw storeError("connection_failed");
  }
  if (!validateClient(client)) {
    const connectionError = storeError("connection_failed");
    try {
      const release = client?.release;
      if (typeof release === "function") {
        await protectPromise(
          reflectApply(release, client, [connectionError]),
        );
      }
    } catch {
      // The shape failure remains primary after best-effort slot destruction.
    }
    throw connectionError;
  }
  return client;
}

function releaseOnce(client) {
  let released = false;
  return async (destroyCause, commitState) => {
    if (released) {
      throw storeError("client_release_failed", commitState);
    }
    released = true;
    try {
      if (destroyCause === undefined) {
        await protectPromise(
          reflectApply(client.release, client, []),
        );
      } else {
        await protectPromise(
          reflectApply(client.release, client, [destroyCause]),
        );
      }
    } catch {
      throw storeError("client_release_failed", commitState);
    }
  };
}

function resultCommand(result) {
  const command = ownDataValue(result, "command");
  return typeof command === "string" ? command : undefined;
}

async function resetAndRelease(client, release, commitState) {
  let resetFailure;
  try {
    const result = await protectPromise(
      clientQuery(client, "DISCARD ALL"),
    );
    if (resultCommand(result) !== "DISCARD") {
      resetFailure = new ErrorConstructor(
        "invalid DISCARD ALL acknowledgement",
      );
    }
  } catch (error) {
    resetFailure = error;
  }
  if (resetFailure !== undefined) {
    try {
      await protectPromise(release(resetFailure, commitState));
    } catch {
      // Preserve the reset classification while the client is being destroyed.
    }
    throw storeError("client_reset_failed", commitState);
  }
  await protectPromise(release(undefined, commitState));
}

async function acquireCleanClient(pool) {
  const client = await protectPromise(acquireClient(pool));
  const release = releaseOnce(client);
  try {
    const result = await protectPromise(
      clientQuery(client, "DISCARD ALL"),
    );
    if (resultCommand(result) !== "DISCARD") {
      throw new ErrorConstructor("invalid DISCARD ALL acknowledgement");
    }
  } catch (error) {
    try {
      await protectPromise(release(error, "not-committed"));
    } catch {
      // Preserve the reset classification while the client is being destroyed.
    }
    throw storeError("client_reset_failed");
  }
  return objectFreeze({ client, release });
}

async function rollbackAndRelease(client, release, originalError) {
  let rollbackResult;
  try {
    rollbackResult = await protectPromise(
      clientQuery(client, "ROLLBACK"),
    );
  } catch (rollbackError) {
    try {
      await protectPromise(release(rollbackError, "uncertain"));
    } catch {
      // The rollback failure already makes the transaction outcome uncertain.
    }
    throw storeError("transaction_rollback_failed", "uncertain");
  }
  if (resultCommand(rollbackResult) !== "ROLLBACK") {
    const rollbackError = new ErrorConstructor(
      "invalid ROLLBACK acknowledgement",
    );
    try {
      await protectPromise(release(rollbackError, "uncertain"));
    } catch {
      // The malformed acknowledgement already makes the outcome uncertain.
    }
    throw storeError("transaction_rollback_failed", "uncertain");
  }
  await protectPromise(
    resetAndRelease(client, release, "not-committed"),
  );
  return originalError;
}

async function failCommitUncertain(client, release, commitError) {
  try {
    await protectPromise(clientQuery(client, "ROLLBACK"));
  } catch {
    // A failed COMMIT is already uncertain; rollback is only best-effort cleanup.
  }
  try {
    await protectPromise(release(commitError, "uncertain"));
  } catch {
    // Preserve the primary commit uncertainty classification.
  }
  throw storeError("transaction_commit_outcome_uncertain", "uncertain");
}

async function releaseAfterServerRollback(client, release, transactionError) {
  // The SQLSTATE proves rollback, but a possibly aborted session is destroyed
  // instead of being reset and returned to the dedicated pool.
  await protectPromise(
    release(transactionError, "not-committed"),
  );
}

async function failBoundaryUncertain(client, release) {
  try {
    await protectPromise(clientQuery(client, "ROLLBACK"));
  } catch {
    // The original transaction boundary is already unproven.
  }
  try {
    await protectPromise(
      release(
        new ErrorConstructor("transaction boundary lost"),
        "uncertain",
      ),
    );
  } catch {
    // Preserve the primary boundary-loss classification.
  }
  throw storeError("transaction_boundary_lost", "uncertain");
}

function canonicalTransactionTimestamp(result) {
  const rows = ownDataValue(result, "rows");
  const row =
    arrayIsArray(rows) &&
    !isProxy(rows) &&
    ownDataValue(rows, "length") === 1
      ? ownDataValue(rows, "0")
      : undefined;
  const value = ownDataValue(row, "transaction_timestamp");
  let timestamp;
  try {
    timestamp =
      typeof value === "string" ? dateParse(value) : dateGetTime(value);
  } catch {
    timestamp = numberNaN;
  }
  if (!numberIsFinite(timestamp)) {
    throw storeError("transaction_timestamp_failed");
  }
  return dateToISOString(new DateConstructor(timestamp));
}

function canonicalTransactionId(result) {
  const rows = ownDataValue(result, "rows");
  const row =
    arrayIsArray(rows) &&
    !isProxy(rows) &&
    ownDataValue(rows, "length") === 1
      ? ownDataValue(rows, "0")
      : undefined;
  const value = ownDataValue(row, "transaction_id");
  if (
    typeof value !== "string" ||
    !regexpTest(/^[1-9][0-9]*$/u, value)
  ) {
    throw storeError("transaction_boundary_lost");
  }
  return value;
}

function createTransactionCapability(
  client,
  now,
  transactionId,
  transactionError,
) {
  let active = true;
  let boundaryLost = false;
  let firstQueryFailure;
  let queryCount = 0;
  let queryQueue = protectPromise((async () => undefined)());
  let terminalQueryError;
  const pending = new SetConstructor();
  const queryErrorSqlStates = new WeakMapConstructor();
  const markLocalQueryError = () => {
    const error = transactionError("transaction_query_invalid");
    firstQueryFailure ??= objectFreeze({ error, source: "local" });
    terminalQueryError ??= error;
    return error;
  };
  const markQueryError = (error) => {
    const sqlState = observedProtocolSqlState(error);
    if (!isTrustedUserQueryRejectionSqlState(sqlState)) {
      boundaryLost = true;
      return transactionError(
        "transaction_boundary_lost",
        "uncertain",
      );
    }
    firstQueryFailure ??= objectFreeze({ error, source: "server" });
    if (
      error !== null &&
      arrayIncludes(["object", "function"], typeof error)
    ) {
      weakMapSet(queryErrorSqlStates, error, sqlState);
    }
    return error;
  };

  const query = (...args) => {
    if (!active) {
      return observedRejectedPromise(
        transactionError("transaction_query_inactive"),
      );
    }
    if (
      (args.length !== 1 && args.length !== 2) ||
      typeof args[0] !== "string" ||
      args[0].length === 0 ||
      isPrepareTransactionStatement(args[0])
    ) {
      queryCount += 1;
      return observedRejectedPromise(markLocalQueryError());
    }
    if (terminalQueryError !== undefined) {
      return observedRejectedPromise(terminalQueryError);
    }

    let values;
    try {
      values =
        args.length === 2 ? copyQueryValues(args[1]) : objectFreeze([]);
    } catch {
      queryCount += 1;
      return observedRejectedPromise(markLocalQueryError());
    }

    queryCount += 1;
    const queryConfig = objectFreeze({
      queryMode: "extended",
      text: args[0],
      values,
    });
    const previousQuery = queryQueue;
    const execution = protectPromise(
      (async () => {
        await protectPromise(previousQuery);
        if (terminalQueryError !== undefined) throw terminalQueryError;
        if (boundaryLost) {
          throw transactionError(
            "transaction_boundary_lost",
            "uncertain",
          );
        }

        let result;
        try {
          result = await protectPromise(
            clientQuery(client, queryConfig),
          );
        } catch (error) {
          terminalQueryError = markQueryError(error);
          throw terminalQueryError;
        }

        let boundaryResult;
        try {
          boundaryResult = await protectPromise(
            clientQuery(
              client,
              "SELECT pg_current_xact_id()::text AS transaction_id",
            ),
          );
          if (canonicalTransactionId(boundaryResult) !== transactionId) {
            throw new ErrorConstructor("transaction identifier changed");
          }
        } catch {
          boundaryLost = true;
          throw transactionError(
            "transaction_boundary_lost",
            "uncertain",
          );
        }
        return result;
      })(),
    );
    let operation;
    operation = protectPromise(
      (async () => {
        try {
          return await protectPromise(execution);
        } finally {
          setDelete(pending, operation);
        }
      })(),
    );
    queryQueue = protectPromise(
      (async () => {
        try {
          await protectPromise(operation);
        } catch {
          // A rejected query does not prevent the queue from draining.
        }
      })(),
    );
    setAdd(pending, operation);
    void (async () => {
      try {
        await protectPromise(operation);
      } catch {
        // The caller observes the original operation rejection.
      }
    })();
    return operation;
  };

  const transaction = objectFreeze({ now, query });
  return objectFreeze({
    close: async () => {
      active = false;
      const queryPending = setSize(pending) !== 0;
      if (queryPending) {
        const unsettled = [];
        setForEach(pending, (operation) => {
          arrayPush(unsettled, operation);
        });
        for (let index = 0; index < unsettled.length; index += 1) {
          try {
            await protectPromise(unsettled[index]);
          } catch {
            // Closing observes every pending query without changing its result.
          }
        }
      }
      return objectFreeze({
        boundaryLost,
        firstQueryFailure,
        queryCount,
        queryPending,
      });
    },
    isQueryError: (error) =>
      firstQueryFailure !== undefined &&
      firstQueryFailure.source === "server" &&
      (objectIs(firstQueryFailure.error, error) ||
        (error !== null &&
          arrayIncludes(["object", "function"], typeof error) &&
          weakMapHas(queryErrorSqlStates, error))),
    isRetryableQueryError: (error) =>
      error !== null &&
      arrayIncludes(["object", "function"], typeof error) &&
      setHas(
        RETRYABLE_TRANSACTION_CODES,
        weakMapGet(queryErrorSqlStates, error),
      ),
    transaction,
  });
}

async function readMigration() {
  let sql;
  try {
    sql = await protectPromise(readFile(MIGRATION_URL, "utf8"));
  } catch {
    throw storeError("migration_source_failed");
  }
  if (sql.length === 0 || !stringEndsWith(sql, "\n")) {
    throw storeError("migration_source_failed");
  }
  const checksum = sha256Hex(sql, "utf8");
  return objectFreeze({ checksum, sql });
}

/**
 * Executes authority work on an otherwise unused, dedicated node-postgres
 * Pool. The store resets every reusable client with DISCARD ALL, which would
 * invalidate session state owned by any other pool consumer.
 */
export class PostgresSerializableStore {
  #dedicatedPool;
  #maxTransactionAttempts;

  constructor(options) {
    const normalized = inspectOptions(options);
    this.#dedicatedPool = validateDedicatedPool(normalized.dedicatedPool);
    this.#maxTransactionAttempts = validateAttemptLimit(
      normalized.maxTransactionAttempts ?? DEFAULT_TRANSACTION_ATTEMPTS,
    );
    objectFreeze(this);
  }

  async migrate() {
    const migration = await protectPromise(readMigration());
    const { client, release } = await protectPromise(
      acquireCleanClient(this.#dedicatedPool),
    );

    try {
      await protectPromise(clientQuery(client, "BEGIN"));
    } catch (error) {
      await protectPromise(release(error, "not-committed"));
      throw storeError("transaction_begin_failed");
    }

    const migrationErrors = new WeakSetConstructor();
    const migrationError = (code) => {
      const error = storeError(code);
      reflectApply(weakSetAddIntrinsic, migrationErrors, [error]);
      return error;
    };
    let applied = false;
    try {
      await protectPromise(
        clientQuery(
          client,
          "SELECT pg_advisory_xact_lock($1::bigint)",
          [MIGRATION_LOCK_KEY],
        ),
      );
      await protectPromise(
        clientQuery(
          client,
          "CREATE SCHEMA IF NOT EXISTS session_authority",
        ),
      );
      await protectPromise(
        clientQuery(
          client,
          arrayJoin(
            [
              "CREATE TABLE IF NOT EXISTS session_authority.schema_migrations (",
              "version integer PRIMARY KEY CHECK (version > 0),",
              "checksum character(64) NOT NULL CHECK (checksum ~ '^[0-9a-f]{64}$'),",
              "applied_at timestamp with time zone NOT NULL",
              ")",
            ],
            " ",
          ),
        ),
      );
      const current = await protectPromise(
        clientQuery(
          client,
          arrayJoin(
            [
              "SELECT version, checksum",
              "FROM session_authority.schema_migrations",
              "ORDER BY version",
            ],
            " ",
          ),
        ),
      );
      const currentRows = ownDataValue(current, "rows");
      const currentLength =
        arrayIsArray(currentRows) && !isProxy(currentRows)
          ? ownDataValue(currentRows, "length")
          : undefined;
      if (
        !numberIsSafeInteger(currentLength) ||
        currentLength < 0
      ) {
        throw migrationError("migration_state_invalid");
      }
      if (currentLength !== 0) {
        const currentRow = ownDataValue(currentRows, "0");
        const currentVersion = ownDataValue(currentRow, "version");
        const currentChecksum = ownDataValue(currentRow, "checksum");
        if (
          currentLength !== 1 ||
          currentVersion !== SESSION_AUTHORITY_MIGRATION_VERSION ||
          typeof currentChecksum !== "string"
        ) {
          throw migrationError("migration_state_invalid");
        }
        if (currentChecksum !== migration.checksum) {
          throw migrationError("migration_checksum_mismatch");
        }
      } else {
        await protectPromise(clientQuery(client, migration.sql));
        await protectPromise(
          clientQuery(
            client,
            arrayJoin(
              [
                "INSERT INTO session_authority.schema_migrations",
                "(version, checksum, applied_at)",
                "VALUES ($1, $2, transaction_timestamp())",
              ],
              " ",
            ),
            [SESSION_AUTHORITY_MIGRATION_VERSION, migration.checksum],
          ),
        );
        applied = true;
      }
    } catch (error) {
      await protectPromise(
        rollbackAndRelease(client, release, error),
      );
      if (reflectApply(weakSetHasIntrinsic, migrationErrors, [error])) {
        throw error;
      }
      throw storeError("migration_failed");
    }

    let commitResult;
    try {
      commitResult = await protectPromise(
        clientQuery(client, "COMMIT"),
      );
    } catch (error) {
      if (hasRetryableTransactionSqlState(error)) {
        await protectPromise(
          releaseAfterServerRollback(client, release, error),
        );
        throw storeError("migration_failed");
      }
      await protectPromise(
        failCommitUncertain(client, release, error),
      );
    }
    const command = resultCommand(commitResult);
    if (command === "ROLLBACK") {
      await protectPromise(
        resetAndRelease(client, release, "not-committed"),
      );
      throw storeError("migration_failed");
    }
    if (command !== "COMMIT") {
      await protectPromise(
        failCommitUncertain(
          client,
          release,
          new ErrorConstructor("invalid COMMIT acknowledgement"),
        ),
      );
    }
    await protectPromise(
      resetAndRelease(client, release, "committed"),
    );
    return objectFreeze({
      applied,
      checksum: migration.checksum,
      version: SESSION_AUTHORITY_MIGRATION_VERSION,
    });
  }

  async runSerializable(callback) {
    if (typeof callback !== "function") {
      throw new TypeErrorConstructor(
        "transaction callback must be a function",
      );
    }

    for (
      let attempt = 1;
      attempt <= this.#maxTransactionAttempts;
      attempt += 1
    ) {
      const transactionErrors = new WeakSetConstructor();
      const transactionError = (
        code,
        commitState = "not-committed",
      ) => {
        const error = storeError(code, commitState);
        reflectApply(weakSetAddIntrinsic, transactionErrors, [error]);
        return error;
      };
      const { client, release } = await protectPromise(
        acquireCleanClient(this.#dedicatedPool),
      );

      try {
        await protectPromise(
          clientQuery(
            client,
            "BEGIN ISOLATION LEVEL SERIALIZABLE READ WRITE",
          ),
        );
      } catch (error) {
        await protectPromise(release(error, "not-committed"));
        throw storeError("transaction_begin_failed");
      }

      let timestampResult;
      try {
        timestampResult = await protectPromise(
          clientQuery(
            client,
            arrayJoin(
              [
                "SELECT transaction_timestamp() AS transaction_timestamp,",
                "pg_current_xact_id()::text AS transaction_id",
              ],
              " ",
            ),
          ),
        );
      } catch (error) {
        await protectPromise(
          rollbackAndRelease(client, release, error),
        );
        throw storeError(
          "transaction_timestamp_failed",
          "not-committed",
        );
      }

      let now;
      let transactionId;
      try {
        now = canonicalTransactionTimestamp(timestampResult);
        transactionId = canonicalTransactionId(timestampResult);
      } catch (error) {
        await protectPromise(
          rollbackAndRelease(client, release, error),
        );
        throw error;
      }

      const capability = createTransactionCapability(
        client,
        now,
        transactionId,
        transactionError,
      );
      let callbackError;
      let callbackFailed = false;
      let value;
      try {
        // Preserve ordinary await semantics for the callback's own result,
        // including Promise subclasses and cross-realm promises. The callback
        // controls only its value/error here; every authority-owned await that
        // follows is protected independently.
        value = await reflectApply(callback, undefined, [
          capability.transaction,
        ]);
      } catch (error) {
        callbackFailed = true;
        callbackError = error;
      }
      const closedCapability = await protectPromise(
        capability.close(),
      );
      if (closedCapability.boundaryLost) {
        await protectPromise(
          failBoundaryUncertain(client, release),
        );
      }
      if (!callbackFailed && closedCapability.queryPending) {
        callbackFailed = true;
        callbackError = transactionError(
          "transaction_query_pending",
        );
      }
      if (
        !callbackFailed &&
        closedCapability.firstQueryFailure !== undefined
      ) {
        callbackFailed = true;
        callbackError = closedCapability.firstQueryFailure.error;
      }

      if (callbackFailed) {
        await protectPromise(
          rollbackAndRelease(client, release, callbackError),
        );
        if (capability.isQueryError(callbackError)) {
          if (capability.isRetryableQueryError(callbackError)) {
            if (attempt < this.#maxTransactionAttempts) continue;
            throw storeError(
              "serialization_retry_exhausted",
              "not-committed",
            );
          }
          throw storeError("transaction_query_failed");
        }
        if (
          isPostgresSerializableStoreError(callbackError) &&
          !reflectApply(weakSetHasIntrinsic, transactionErrors, [
            callbackError,
          ])
        ) {
          throw transactionError("transaction_rolled_back");
        }
        throw callbackError;
      }

      if (closedCapability.queryCount === 0) {
        let boundaryResult;
        try {
          boundaryResult = await protectPromise(
            clientQuery(
              client,
              "SELECT pg_current_xact_id()::text AS transaction_id",
            ),
          );
        } catch {
          await protectPromise(
            failBoundaryUncertain(client, release),
          );
        }
        let currentTransactionId;
        try {
          currentTransactionId = canonicalTransactionId(boundaryResult);
        } catch {
          await protectPromise(
            failBoundaryUncertain(client, release),
          );
        }
        if (currentTransactionId !== transactionId) {
          await protectPromise(
            failBoundaryUncertain(client, release),
          );
        }
      }

      let commitResult;
      try {
        commitResult = await protectPromise(
          clientQuery(client, "COMMIT"),
        );
      } catch (error) {
        if (hasRetryableTransactionSqlState(error)) {
          await protectPromise(
            releaseAfterServerRollback(client, release, error),
          );
          if (attempt < this.#maxTransactionAttempts) continue;
          throw storeError(
            "serialization_retry_exhausted",
            "not-committed",
          );
        }
        await protectPromise(
          failCommitUncertain(client, release, error),
        );
      }
      const command = resultCommand(commitResult);
      if (command === "ROLLBACK") {
        await protectPromise(
          resetAndRelease(client, release, "not-committed"),
        );
        throw storeError("transaction_rolled_back");
      }
      if (command !== "COMMIT") {
        await protectPromise(
          failCommitUncertain(
            client,
            release,
            new ErrorConstructor("invalid COMMIT acknowledgement"),
          ),
        );
      }
      await protectPromise(
        resetAndRelease(client, release, "committed"),
      );
      return value;
    }

    throw new ErrorConstructor("unreachable transaction attempt state");
  }
}
