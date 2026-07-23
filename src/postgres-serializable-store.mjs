import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { isProxy } from "node:util/types";

import { DatabaseError } from "pg";

export const SESSION_AUTHORITY_MIGRATION_VERSION = 1;
export const DEFAULT_TRANSACTION_ATTEMPTS = 3;
export const MAX_TRANSACTION_ATTEMPTS = 16;

const MIGRATION_URL = new URL(
  "../migrations/authority/001-session-authority.sql",
  import.meta.url,
);
const MIGRATION_LOCK_KEY = "7275632827684484689";
const RETRYABLE_TRANSACTION_CODES = new Set(["40001", "40P01"]);
const QUERY_PARAMETER_TYPES = new Set([
  "bigint",
  "boolean",
  "number",
  "string",
  "undefined",
]);
const MAX_QUERY_PARAMETERS = 65_535;
const COMMIT_STATES = new Set(["committed", "not-committed", "uncertain"]);
const PROTOCOL_ERROR_SQLSTATES = new WeakMap();
const objectDefineProperties = Object.defineProperties;
const objectFreeze = Object.freeze;
const objectHasOwn = Object.hasOwn;
const reflectApply = Reflect.apply;
const setHasIntrinsic = Set.prototype.has;
const weakSetAddIntrinsic = WeakSet.prototype.add;
const weakSetHasIntrinsic = WeakSet.prototype.has;
const WeakSetConstructor = WeakSet;
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
      !reflectApply(setHasIntrinsic, COMMIT_STATES, [commitState])
    ) {
      throw new TypeError("unsupported PostgreSQL serializable store error");
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
    objectFreeze(this);
  }
}

function storeError(code, commitState = "not-committed") {
  return new PostgresSerializableStoreError(code, commitState);
}

function observedRejectedPromise(error) {
  const rejection = Promise.reject(error);
  void rejection.then(undefined, () => undefined);
  return rejection;
}

function inspectOptions(options) {
  if (
    options === null ||
    typeof options !== "object" ||
    Array.isArray(options) ||
    ![Object.prototype, null].includes(Object.getPrototypeOf(options))
  ) {
    throw new TypeError("PostgreSQL serializable store options must be a plain object");
  }
  const keys = Reflect.ownKeys(options);
  if (
    !keys.every(
      (key) =>
        typeof key === "string" &&
        ["dedicatedPool", "maxTransactionAttempts"].includes(key),
    ) ||
    !keys.includes("dedicatedPool")
  ) {
    throw new TypeError(
      "PostgreSQL serializable store options contain unexpected or missing fields",
    );
  }
  const normalized = Object.create(null);
  for (const key of keys) {
    const descriptor = Object.getOwnPropertyDescriptor(options, key);
    if (!descriptor?.enumerable || !Object.hasOwn(descriptor, "value")) {
      throw new TypeError(
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
    !["object", "function"].includes(typeof pool) ||
    typeof pool.connect !== "function"
  ) {
    throw new TypeError("dedicatedPool must provide connect()");
  }
  return pool;
}

function validateAttemptLimit(value) {
  if (
    !Number.isSafeInteger(value) ||
    value < 1 ||
    value > MAX_TRANSACTION_ATTEMPTS
  ) {
    throw new TypeError(
      `maxTransactionAttempts must be an integer from 1 through ${MAX_TRANSACTION_ATTEMPTS}`,
    );
  }
  return value;
}

function observedProtocolSqlState(error) {
  try {
    const sqlState = PROTOCOL_ERROR_SQLSTATES.get(error);
    PROTOCOL_ERROR_SQLSTATES.delete(error);
    return sqlState;
  } catch {
    return undefined;
  }
}

function hasRetryableTransactionSqlState(error) {
  return RETRYABLE_TRANSACTION_CODES.has(observedProtocolSqlState(error));
}

function isTrustedUserQueryRejectionSqlState(sqlState) {
  return (
    sqlState !== undefined &&
    sqlState !== "40003" &&
    !["08", "57", "58", "XX"].includes(sqlState.slice(0, 2))
  );
}

function copyQueryValues(values) {
  try {
    if (
      isProxy(values) ||
      !Array.isArray(values) ||
      Object.getPrototypeOf(values) !== Array.prototype
    ) {
      throw new TypeError("query values must use the built-in Array prototype");
    }
    const lengthDescriptor = Object.getOwnPropertyDescriptor(values, "length");
    const length = lengthDescriptor?.value;
    if (
      !Object.hasOwn(lengthDescriptor ?? {}, "value") ||
      !Number.isSafeInteger(length) ||
      length < 0 ||
      length > MAX_QUERY_PARAMETERS
    ) {
      throw new TypeError("query values length is invalid");
    }

    const copied = new Array(length);
    for (let index = 0; index < length; index += 1) {
      const descriptor = Object.getOwnPropertyDescriptor(values, String(index));
      if (descriptor !== undefined && !Object.hasOwn(descriptor, "value")) {
        throw new TypeError("query values must use plain data fields");
      }
      const value = descriptor?.value;
      if (
        value !== null &&
        !QUERY_PARAMETER_TYPES.has(typeof value)
      ) {
        throw new TypeError("query values must be primitive data");
      }
      Object.defineProperty(copied, String(index), {
        configurable: true,
        enumerable: true,
        value,
        writable: true,
      });
    }
    return Object.freeze(copied);
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
    return (
      client !== null &&
      ["object", "function"].includes(typeof client) &&
      typeof client.query === "function" &&
      typeof client.release === "function" &&
      client.connection !== null &&
      typeof client.connection === "object" &&
      typeof client.connection.prependListener === "function" &&
      typeof client.connection.removeListener === "function"
    );
  } catch {
    return false;
  }
}

async function clientQuery(client, ...args) {
  const observed = new WeakMap();
  const observeProtocolError = (error) => {
    try {
      if (
        error instanceof DatabaseError &&
        error.name === "error" &&
        typeof error.code === "string" &&
        /^[0-9A-Z]{5}$/u.test(error.code)
      ) {
        observed.set(error, error.code);
      }
    } catch {
      // An invalid event payload cannot become trusted protocol evidence.
    }
  };

  Reflect.apply(
    client.connection.prependListener,
    client.connection,
    ["errorMessage", observeProtocolError],
  );
  try {
    return await Reflect.apply(client.query, client, args);
  } catch (error) {
    try {
      PROTOCOL_ERROR_SQLSTATES.delete(error);
      const sqlState = observed.get(error);
      if (sqlState !== undefined) {
        PROTOCOL_ERROR_SQLSTATES.set(error, sqlState);
      }
    } catch {
      // Preserve the query failure without adding protocol provenance.
    }
    throw error;
  } finally {
    Reflect.apply(
      client.connection.removeListener,
      client.connection,
      ["errorMessage", observeProtocolError],
    );
  }
}

async function acquireClient(pool) {
  let client;
  try {
    client = await Reflect.apply(pool.connect, pool, []);
  } catch {
    throw storeError("connection_failed");
  }
  if (!validateClient(client)) {
    throw storeError("connection_failed");
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
        await Reflect.apply(client.release, client, []);
      } else {
        await Reflect.apply(client.release, client, [destroyCause]);
      }
    } catch {
      throw storeError("client_release_failed", commitState);
    }
  };
}

function resultCommand(result) {
  try {
    return result !== null &&
      typeof result === "object" &&
      typeof result.command === "string"
      ? result.command
      : undefined;
  } catch {
    return undefined;
  }
}

async function resetAndRelease(client, release, commitState) {
  let resetFailure;
  try {
    const result = await clientQuery(client, "DISCARD ALL");
    if (resultCommand(result) !== "DISCARD") {
      resetFailure = new Error("invalid DISCARD ALL acknowledgement");
    }
  } catch (error) {
    resetFailure = error;
  }
  if (resetFailure !== undefined) {
    try {
      await release(resetFailure, commitState);
    } catch {
      // Preserve the reset classification while the client is being destroyed.
    }
    throw storeError("client_reset_failed", commitState);
  }
  await release(undefined, commitState);
}

async function acquireCleanClient(pool) {
  const client = await acquireClient(pool);
  const release = releaseOnce(client);
  try {
    const result = await clientQuery(client, "DISCARD ALL");
    if (resultCommand(result) !== "DISCARD") {
      throw new Error("invalid DISCARD ALL acknowledgement");
    }
  } catch (error) {
    try {
      await release(error, "not-committed");
    } catch {
      // Preserve the reset classification while the client is being destroyed.
    }
    throw storeError("client_reset_failed");
  }
  return Object.freeze({ client, release });
}

async function rollbackAndRelease(client, release, originalError) {
  let rollbackResult;
  try {
    rollbackResult = await clientQuery(client, "ROLLBACK");
  } catch (rollbackError) {
    try {
      await release(rollbackError, "uncertain");
    } catch {
      // The rollback failure already makes the transaction outcome uncertain.
    }
    throw storeError("transaction_rollback_failed", "uncertain");
  }
  if (resultCommand(rollbackResult) !== "ROLLBACK") {
    const rollbackError = new Error("invalid ROLLBACK acknowledgement");
    try {
      await release(rollbackError, "uncertain");
    } catch {
      // The malformed acknowledgement already makes the outcome uncertain.
    }
    throw storeError("transaction_rollback_failed", "uncertain");
  }
  await resetAndRelease(client, release, "not-committed");
  return originalError;
}

async function failCommitUncertain(client, release, commitError) {
  try {
    await clientQuery(client, "ROLLBACK");
  } catch {
    // A failed COMMIT is already uncertain; rollback is only best-effort cleanup.
  }
  try {
    await release(commitError, "uncertain");
  } catch {
    // Preserve the primary commit uncertainty classification.
  }
  throw storeError("transaction_commit_outcome_uncertain", "uncertain");
}

async function releaseAfterServerRollback(client, release, transactionError) {
  // The SQLSTATE proves rollback, but a possibly aborted session is destroyed
  // instead of being reset and returned to the dedicated pool.
  await release(transactionError, "not-committed");
}

async function failBoundaryUncertain(client, release) {
  try {
    await clientQuery(client, "ROLLBACK");
  } catch {
    // The original transaction boundary is already unproven.
  }
  try {
    await release(new Error("transaction boundary lost"), "uncertain");
  } catch {
    // Preserve the primary boundary-loss classification.
  }
  throw storeError("transaction_boundary_lost", "uncertain");
}

function canonicalTransactionTimestamp(result) {
  const value =
    result !== null &&
    typeof result === "object" &&
    Array.isArray(result.rows) &&
    result.rows.length === 1 &&
    result.rows[0] !== null &&
    typeof result.rows[0] === "object"
      ? result.rows[0].transaction_timestamp
      : undefined;
  let timestamp;
  try {
    timestamp =
      value instanceof Date
        ? value.getTime()
        : typeof value === "string"
          ? Date.parse(value)
          : Number.NaN;
  } catch {
    timestamp = Number.NaN;
  }
  if (!Number.isFinite(timestamp)) {
    throw storeError("transaction_timestamp_failed");
  }
  return new Date(timestamp).toISOString();
}

function canonicalTransactionId(result) {
  const value =
    result !== null &&
    typeof result === "object" &&
    Array.isArray(result.rows) &&
    result.rows.length === 1 &&
    result.rows[0] !== null &&
    typeof result.rows[0] === "object"
      ? result.rows[0].transaction_id
      : undefined;
  if (typeof value !== "string" || !/^[1-9][0-9]*$/u.test(value)) {
    throw storeError("transaction_boundary_lost");
  }
  return value;
}

function createTransactionCapability(client, now, transactionId) {
  let active = true;
  let boundaryLost = false;
  let firstQueryFailure;
  let queryCount = 0;
  let queryQueue = Promise.resolve();
  let terminalQueryError;
  const pending = new Set();
  const queryErrorSqlStates = new WeakMap();
  const markLocalQueryError = () => {
    const error = storeError("transaction_query_invalid");
    firstQueryFailure ??= Object.freeze({ error, source: "local" });
    terminalQueryError ??= error;
    return error;
  };
  const markQueryError = (error) => {
    const sqlState = observedProtocolSqlState(error);
    if (!isTrustedUserQueryRejectionSqlState(sqlState)) {
      boundaryLost = true;
      return storeError("transaction_boundary_lost", "uncertain");
    }
    firstQueryFailure ??= Object.freeze({ error, source: "server" });
    if (
      error !== null &&
      ["object", "function"].includes(typeof error)
    ) {
      queryErrorSqlStates.set(error, sqlState);
    }
    return error;
  };

  const query = (...args) => {
    if (!active) {
      return observedRejectedPromise(
        storeError("transaction_query_inactive"),
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
      values = args.length === 2 ? copyQueryValues(args[1]) : Object.freeze([]);
    } catch {
      queryCount += 1;
      return observedRejectedPromise(markLocalQueryError());
    }

    queryCount += 1;
    const queryConfig = Object.freeze({
      queryMode: "extended",
      text: args[0],
      values,
    });
    const operation = queryQueue.then(async () => {
      if (terminalQueryError !== undefined) throw terminalQueryError;
      if (boundaryLost) {
        throw storeError("transaction_boundary_lost", "uncertain");
      }

      let result;
      try {
        result = await clientQuery(client, queryConfig);
      } catch (error) {
        terminalQueryError = markQueryError(error);
        throw terminalQueryError;
      }

      let boundaryResult;
      try {
        boundaryResult = await clientQuery(
          client,
          "SELECT pg_current_xact_id()::text AS transaction_id",
        );
        if (canonicalTransactionId(boundaryResult) !== transactionId) {
          throw new Error("transaction identifier changed");
        }
      } catch {
        boundaryLost = true;
        throw storeError("transaction_boundary_lost", "uncertain");
      }
      return result;
    });
    queryQueue = operation.catch(() => undefined);
    pending.add(operation);
    void operation.then(
      () => pending.delete(operation),
      () => pending.delete(operation),
    );
    return operation;
  };

  const transaction = Object.freeze({ now, query });
  return Object.freeze({
    close: async () => {
      active = false;
      const queryPending = pending.size !== 0;
      if (queryPending) {
        const unsettled = [...pending];
        await Promise.allSettled(unsettled);
      }
      return Object.freeze({
        boundaryLost,
        firstQueryFailure,
        queryCount,
        queryPending,
      });
    },
    isQueryError: (error) =>
      firstQueryFailure !== undefined &&
      firstQueryFailure.source === "server" &&
      (Object.is(firstQueryFailure.error, error) ||
        (error !== null &&
          ["object", "function"].includes(typeof error) &&
          queryErrorSqlStates.has(error))),
    isRetryableQueryError: (error) =>
      error !== null &&
      ["object", "function"].includes(typeof error) &&
      RETRYABLE_TRANSACTION_CODES.has(queryErrorSqlStates.get(error)),
    transaction,
  });
}

async function readMigration() {
  let sql;
  try {
    sql = await readFile(MIGRATION_URL, "utf8");
  } catch {
    throw storeError("migration_source_failed");
  }
  if (sql.length === 0 || !sql.endsWith("\n")) {
    throw storeError("migration_source_failed");
  }
  const checksum = createHash("sha256").update(sql, "utf8").digest("hex");
  return Object.freeze({ checksum, sql });
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
    Object.freeze(this);
  }

  async migrate() {
    const migration = await readMigration();
    const { client, release } = await acquireCleanClient(this.#dedicatedPool);

    try {
      await clientQuery(client, "BEGIN");
    } catch (error) {
      await release(error, "not-committed");
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
      await clientQuery(
        client,
        "SELECT pg_advisory_xact_lock($1::bigint)",
        [MIGRATION_LOCK_KEY],
      );
      await clientQuery(client, "CREATE SCHEMA IF NOT EXISTS session_authority");
      await clientQuery(
        client,
        [
          "CREATE TABLE IF NOT EXISTS session_authority.schema_migrations (",
          "version integer PRIMARY KEY CHECK (version > 0),",
          "checksum character(64) NOT NULL CHECK (checksum ~ '^[0-9a-f]{64}$'),",
          "applied_at timestamp with time zone NOT NULL",
          ")",
        ].join(" "),
      );
      const current = await clientQuery(
        client,
        [
          "SELECT version, checksum",
          "FROM session_authority.schema_migrations",
          "ORDER BY version",
        ].join(" "),
      );
      if (
        current === null ||
        typeof current !== "object" ||
        !Array.isArray(current.rows)
      ) {
        throw migrationError("migration_state_invalid");
      }
      if (current.rows.length !== 0) {
        if (
          current.rows.length !== 1 ||
          current.rows[0] === null ||
          typeof current.rows[0] !== "object" ||
          current.rows[0].version !== SESSION_AUTHORITY_MIGRATION_VERSION ||
          typeof current.rows[0].checksum !== "string"
        ) {
          throw migrationError("migration_state_invalid");
        }
        if (current.rows[0].checksum !== migration.checksum) {
          throw migrationError("migration_checksum_mismatch");
        }
      } else {
        await clientQuery(client, migration.sql);
        await clientQuery(
          client,
          [
            "INSERT INTO session_authority.schema_migrations",
            "(version, checksum, applied_at)",
            "VALUES ($1, $2, transaction_timestamp())",
          ].join(" "),
          [SESSION_AUTHORITY_MIGRATION_VERSION, migration.checksum],
        );
        applied = true;
      }
    } catch (error) {
      await rollbackAndRelease(client, release, error);
      if (reflectApply(weakSetHasIntrinsic, migrationErrors, [error])) {
        throw error;
      }
      throw storeError("migration_failed");
    }

    let commitResult;
    try {
      commitResult = await clientQuery(client, "COMMIT");
    } catch (error) {
      if (hasRetryableTransactionSqlState(error)) {
        await releaseAfterServerRollback(client, release, error);
        throw storeError("migration_failed");
      }
      await failCommitUncertain(client, release, error);
    }
    const command = resultCommand(commitResult);
    if (command === "ROLLBACK") {
      await resetAndRelease(client, release, "not-committed");
      throw storeError("migration_failed");
    }
    if (command !== "COMMIT") {
      await failCommitUncertain(
        client,
        release,
        new Error("invalid COMMIT acknowledgement"),
      );
    }
    await resetAndRelease(client, release, "committed");
    return Object.freeze({
      applied,
      checksum: migration.checksum,
      version: SESSION_AUTHORITY_MIGRATION_VERSION,
    });
  }

  async runSerializable(callback) {
    if (typeof callback !== "function") {
      throw new TypeError("transaction callback must be a function");
    }

    for (
      let attempt = 1;
      attempt <= this.#maxTransactionAttempts;
      attempt += 1
    ) {
      const { client, release } = await acquireCleanClient(
        this.#dedicatedPool,
      );

      try {
        await clientQuery(
          client,
          "BEGIN ISOLATION LEVEL SERIALIZABLE READ WRITE",
        );
      } catch (error) {
        await release(error, "not-committed");
        throw storeError("transaction_begin_failed");
      }

      let timestampResult;
      try {
        timestampResult = await clientQuery(
          client,
          [
            "SELECT transaction_timestamp() AS transaction_timestamp,",
            "pg_current_xact_id()::text AS transaction_id",
          ].join(" "),
        );
      } catch (error) {
        await rollbackAndRelease(client, release, error);
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
        await rollbackAndRelease(client, release, error);
        throw error;
      }

      const capability = createTransactionCapability(
        client,
        now,
        transactionId,
      );
      let callbackError;
      let callbackFailed = false;
      let value;
      try {
        value = await Reflect.apply(callback, undefined, [
          capability.transaction,
        ]);
      } catch (error) {
        callbackFailed = true;
        callbackError = error;
      }
      const closedCapability = await capability.close();
      if (closedCapability.boundaryLost) {
        await failBoundaryUncertain(client, release);
      }
      if (!callbackFailed && closedCapability.queryPending) {
        callbackFailed = true;
        callbackError = storeError("transaction_query_pending");
      }
      if (
        !callbackFailed &&
        closedCapability.firstQueryFailure !== undefined
      ) {
        callbackFailed = true;
        callbackError = closedCapability.firstQueryFailure.error;
      }

      if (callbackFailed) {
        await rollbackAndRelease(client, release, callbackError);
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
        throw callbackError;
      }

      if (closedCapability.queryCount === 0) {
        let boundaryResult;
        try {
          boundaryResult = await clientQuery(
            client,
            "SELECT pg_current_xact_id()::text AS transaction_id",
          );
        } catch {
          await failBoundaryUncertain(client, release);
        }
        let currentTransactionId;
        try {
          currentTransactionId = canonicalTransactionId(boundaryResult);
        } catch {
          await failBoundaryUncertain(client, release);
        }
        if (currentTransactionId !== transactionId) {
          await failBoundaryUncertain(client, release);
        }
      }

      let commitResult;
      try {
        commitResult = await clientQuery(client, "COMMIT");
      } catch (error) {
        if (hasRetryableTransactionSqlState(error)) {
          await releaseAfterServerRollback(client, release, error);
          if (attempt < this.#maxTransactionAttempts) continue;
          throw storeError(
            "serialization_retry_exhausted",
            "not-committed",
          );
        }
        await failCommitUncertain(client, release, error);
      }
      const command = resultCommand(commitResult);
      if (command === "ROLLBACK") {
        await resetAndRelease(client, release, "not-committed");
        throw storeError("transaction_rolled_back");
      }
      if (command !== "COMMIT") {
        await failCommitUncertain(
          client,
          release,
          new Error("invalid COMMIT acknowledgement"),
        );
      }
      await resetAndRelease(client, release, "committed");
      return value;
    }

    throw new Error("unreachable transaction attempt state");
  }
}
