import { isProxy as isProxyValue } from "node:util/types";

import {
  PostgresSerializableStore,
  PostgresSerializableStoreError,
} from "./postgres-serializable-store.mjs";

const arrayIncludesIntrinsic = Array.prototype.includes;
const arrayIsArray = Array.isArray;
const arrayPrototype = Array.prototype;
const BigIntConstructor = BigInt;
const DateConstructor = Date;
const dateGetTimeIntrinsic = Date.prototype.getTime;
const dateParse = Date.parse;
const dateToISOStringIntrinsic = Date.prototype.toISOString;
const ErrorConstructor = Error;
const numberIsFinite = Number.isFinite;
const numberNaN = Number.NaN;
const objectCreate = Object.create;
const objectDefineProperty = Object.defineProperty;
const objectFreeze = Object.freeze;
const objectGetOwnPropertyDescriptor = Object.getOwnPropertyDescriptor;
const objectGetPrototypeOf = Object.getPrototypeOf;
const objectHasOwn = Object.hasOwn;
const objectIsFrozen = Object.isFrozen;
const objectPrototype = Object.prototype;
const reflectApply = Reflect.apply;
const reflectOwnKeys = Reflect.ownKeys;
const regexpExecIntrinsic = RegExp.prototype.exec;
const runSerializableIntrinsic =
  PostgresSerializableStore.prototype.runSerializable;
const StringConstructor = String;
const TypeErrorConstructor = TypeError;
const weakSetAddIntrinsic = WeakSet.prototype.add;
const weakSetHasIntrinsic = WeakSet.prototype.has;
const WeakSetConstructor = WeakSet;

const LANES = objectFreeze([
  "generation",
  "activation",
  "launch-attempt",
  "current-launch",
  "supervisor-state-gc",
]);
const READ_KEYS = objectFreeze(["lane", "recoveryScopeId"]);
const ADVANCE_KEYS = objectFreeze([
  "expectedAfterSessionId",
  "expectedCycle",
  "expectedRevision",
  "lane",
  "nextAfterSessionId",
  "recoveryScopeId",
  "requestSha256",
  "transitionId",
]);
const CURSOR_ROW_KEYS = objectFreeze([
  "after_session_id",
  "cycle",
  "lane",
  "last_request_sha256",
  "last_transition_id",
  "recovery_scope_id",
  "revision",
  "updated_at",
]);
const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u;
const OPAQUE_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u;
const SHA256_PATTERN = /^[0-9a-f]{64}$/u;
const DECIMAL_PATTERN = /^(?:0|[1-9][0-9]*)$/u;
const MAX_BIGINT = 9_223_372_036_854_775_807n;

const CURSOR_COLUMNS = [
  "recovery_scope_id",
  "lane",
  "after_session_id::pg_catalog.text AS after_session_id",
  "cycle::pg_catalog.text AS cycle",
  "revision::pg_catalog.text AS revision",
  "last_transition_id::pg_catalog.text AS last_transition_id",
  "last_request_sha256",
  "updated_at",
].join(", ");
const INITIALIZE_QUERY = [
  "INSERT INTO session_authority.restore_recovery_cursors",
  "(recovery_scope_id, lane, after_session_id, cycle, revision,",
  "last_transition_id, last_request_sha256, updated_at)",
  "VALUES ($1, $2, NULL, 0, 0, NULL, NULL, $3)",
  "ON CONFLICT (recovery_scope_id, lane) DO NOTHING",
].join(" ");
const READ_QUERY = [
  `SELECT ${CURSOR_COLUMNS}`,
  "FROM session_authority.restore_recovery_cursors",
  "WHERE recovery_scope_id = $1 AND lane = $2",
  "FOR UPDATE",
].join(" ");
const UPDATE_QUERY = [
  "UPDATE session_authority.restore_recovery_cursors",
  "SET after_session_id = $3, cycle = $4, revision = $5,",
  "last_transition_id = $6, last_request_sha256 = $7, updated_at = $8",
  "WHERE recovery_scope_id = $1 AND lane = $2",
  "AND revision = $9 AND cycle = $10",
  "AND after_session_id IS NOT DISTINCT FROM $11",
  `RETURNING ${CURSOR_COLUMNS}`,
].join(" ");

const ERROR_MESSAGES = objectFreeze({
  invalid_postgres_restore_recovery_cursor_store_options:
    "PostgreSQL restore recovery cursor store options are invalid",
  invalid_postgres_restore_recovery_cursor_store_request:
    "PostgreSQL restore recovery cursor store request is invalid",
  postgres_restore_recovery_cursor_conflict:
    "PostgreSQL restore recovery cursor transition conflicts with durable state",
  postgres_restore_recovery_cursor_outcome_uncertain:
    "PostgreSQL restore recovery cursor outcome is uncertain",
  postgres_restore_recovery_cursor_state_invalid:
    "PostgreSQL restore recovery cursor state is invalid",
});
const CURSOR_ERRORS = new WeakSetConstructor();

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
  throw new PostgresRestoreRecoveryCursorStoreError(code);
}

function ensure(condition, code) {
  if (!condition) fail(code);
}

function isCursorError(error) {
  return (
    error !== null &&
    !isProxyValue(error) &&
    callIntrinsic(weakSetHasIntrinsic, CURSOR_ERRORS, [error])
  );
}

function isStoreError(error) {
  if (
    error === null ||
    typeof error !== "object" ||
    isProxyValue(error)
  ) {
    return false;
  }
  try {
    return (
      objectGetPrototypeOf(error) === PostgresSerializableStoreError.prototype
    );
  } catch {
    return false;
  }
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
    ensure(
      typeof key === "string" && arrayIncludes(expectedKeys, key),
      code,
    );
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

function canonicalLane(value, code) {
  ensure(typeof value === "string" && arrayIncludes(LANES, value), code);
  return value;
}

function canonicalUuid(value, code) {
  ensure(typeof value === "string" && regexpTest(UUID_PATTERN, value), code);
  return value;
}

function canonicalNullableSessionId(value, code) {
  return value === null ? null : canonicalUuid(value, code);
}

function canonicalSha256(value, code) {
  ensure(
    typeof value === "string" && regexpTest(SHA256_PATTERN, value),
    code,
  );
  return value;
}

function canonicalBigint(value, code) {
  ensure(typeof value === "string" && regexpTest(DECIMAL_PATTERN, value), code);
  let parsed;
  try {
    parsed = BigIntConstructor(value);
  } catch {
    fail(code);
  }
  ensure(parsed <= MAX_BIGINT, code);
  return objectFreeze({ parsed, value });
}

function incrementBigint(value, code) {
  ensure(value.parsed < MAX_BIGINT, code);
  return callIntrinsic(StringConstructor, undefined, [value.parsed + 1n]);
}

function canonicalTimestamp(value, code) {
  let milliseconds;
  try {
    milliseconds =
      typeof value === "string"
        ? dateParse(value)
        : value !== null &&
            typeof value === "object" &&
            !isProxyValue(value)
          ? callIntrinsic(dateGetTimeIntrinsic, value, [])
          : numberNaN;
  } catch {
    milliseconds = numberNaN;
  }
  ensure(numberIsFinite(milliseconds), code);
  return callIntrinsic(dateToISOStringIntrinsic, new DateConstructor(milliseconds), []);
}

function rowsFromResult(result, code) {
  ensure(
    result !== null && typeof result === "object" && !isProxyValue(result),
    code,
  );
  const rows = ownDataValue(result, "rows", code);
  ensure(
    !isProxyValue(rows) &&
      arrayIsArray(rows) &&
      (objectGetPrototypeOf(rows) === arrayPrototype ||
        (objectGetPrototypeOf(rows) === null && objectIsFrozen(rows))) &&
      rows.length <= 1,
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

function normalizeCursorRow(value, expected, code) {
  const row = exactDataObject(value, CURSOR_ROW_KEYS, code);
  const recoveryScopeId = canonicalOpaqueId(row.recovery_scope_id, code);
  const lane = canonicalLane(row.lane, code);
  ensure(
    recoveryScopeId === expected.recoveryScopeId && lane === expected.lane,
    code,
  );
  const afterSessionId = canonicalNullableSessionId(
    row.after_session_id,
    code,
  );
  const cycle = canonicalBigint(row.cycle, code);
  const revision = canonicalBigint(row.revision, code);
  ensure(cycle.parsed <= revision.parsed, code);
  const lastTransitionId =
    row.last_transition_id === null
      ? null
      : canonicalUuid(row.last_transition_id, code);
  const lastRequestSha256 =
    row.last_request_sha256 === null
      ? null
      : canonicalSha256(row.last_request_sha256, code);
  ensure(
    (lastTransitionId === null) === (lastRequestSha256 === null),
    code,
  );
  if (revision.parsed === 0n) {
    ensure(
      cycle.parsed === 0n &&
        afterSessionId === null &&
        lastTransitionId === null,
      code,
    );
  } else {
    ensure(lastTransitionId !== null, code);
  }
  return objectFreeze({
    recoveryScopeId,
    lane,
    afterSessionId,
    cycle: cycle.value,
    revision: revision.value,
    lastTransitionId,
    lastRequestSha256,
    updatedAt: canonicalTimestamp(row.updated_at, code),
  });
}

function normalizeReadRequest(value, code) {
  const normalized = exactDataObject(value, READ_KEYS, code);
  return objectFreeze({
    recoveryScopeId: canonicalOpaqueId(normalized.recoveryScopeId, code),
    lane: canonicalLane(normalized.lane, code),
  });
}

function normalizeAdvanceRequest(value, code) {
  const normalized = exactDataObject(value, ADVANCE_KEYS, code);
  const expectedAfterSessionId = canonicalNullableSessionId(
    normalized.expectedAfterSessionId,
    code,
  );
  const nextAfterSessionId = canonicalNullableSessionId(
    normalized.nextAfterSessionId,
    code,
  );
  if (nextAfterSessionId !== null) {
    ensure(
      expectedAfterSessionId === null ||
        nextAfterSessionId > expectedAfterSessionId,
      code,
    );
  }
  const expectedCycle = canonicalBigint(normalized.expectedCycle, code);
  const expectedRevision = canonicalBigint(normalized.expectedRevision, code);
  ensure(expectedCycle.parsed <= expectedRevision.parsed, code);
  if (expectedRevision.parsed === 0n) {
    ensure(
      expectedCycle.parsed === 0n && expectedAfterSessionId === null,
      code,
    );
  }
  const nextCycle =
    nextAfterSessionId === null
      ? incrementBigint(expectedCycle, code)
      : expectedCycle.value;
  const nextRevision = incrementBigint(expectedRevision, code);
  return objectFreeze({
    recoveryScopeId: canonicalOpaqueId(normalized.recoveryScopeId, code),
    lane: canonicalLane(normalized.lane, code),
    transitionId: canonicalUuid(normalized.transitionId, code),
    requestSha256: canonicalSha256(normalized.requestSha256, code),
    expectedAfterSessionId,
    expectedCycle: expectedCycle.value,
    expectedRevision: expectedRevision.value,
    nextAfterSessionId,
    nextCycle,
    nextRevision,
  });
}

function runSerializable(store, callback) {
  return callIntrinsic(runSerializableIntrinsic, store, [callback]);
}

function queryTransaction(transaction, text, values, code) {
  const query = ownDataValue(transaction, "query", code);
  ensure(typeof query === "function" && !isProxyValue(query), code);
  return callIntrinsic(query, transaction, [text, values]);
}

async function initializeAndRead(transaction, input, code) {
  const now = ownDataValue(transaction, "now", code);
  canonicalTimestamp(now, code);
  await queryTransaction(
    transaction,
    INITIALIZE_QUERY,
    [input.recoveryScopeId, input.lane, now],
    code,
  );
  const rows = rowsFromResult(
    await queryTransaction(
      transaction,
      READ_QUERY,
      [input.recoveryScopeId, input.lane],
      code,
    ),
    code,
  );
  ensure(rows.length === 1, code);
  return normalizeCursorRow(rows[0], input, code);
}

async function readExisting(store, input) {
  return runSerializable(store, async (transaction) => {
    const rows = rowsFromResult(
      await queryTransaction(
        transaction,
        READ_QUERY,
        [input.recoveryScopeId, input.lane],
        "postgres_restore_recovery_cursor_state_invalid",
      ),
      "postgres_restore_recovery_cursor_state_invalid",
    );
    if (rows.length === 0) return null;
    return normalizeCursorRow(
      rows[0],
      input,
      "postgres_restore_recovery_cursor_state_invalid",
    );
  });
}

function cursorMatchesExpected(cursor, input) {
  return (
    cursor.revision === input.expectedRevision &&
    cursor.cycle === input.expectedCycle &&
    cursor.afterSessionId === input.expectedAfterSessionId
  );
}

function inputExpectsInitialCursor(input) {
  return (
    input.expectedRevision === "0" &&
    input.expectedCycle === "0" &&
    input.expectedAfterSessionId === null
  );
}

function cursorMatchesTarget(cursor, input) {
  return (
    cursor.revision === input.nextRevision &&
    cursor.cycle === input.nextCycle &&
    cursor.afterSessionId === input.nextAfterSessionId &&
    cursor.lastTransitionId === input.transitionId &&
    cursor.lastRequestSha256 === input.requestSha256
  );
}

async function advanceTransaction(store, input) {
  return runSerializable(store, async (transaction) => {
    const code = "postgres_restore_recovery_cursor_state_invalid";
    const cursor = await initializeAndRead(transaction, input, code);
    if (cursor.lastTransitionId === input.transitionId) {
      ensure(
        cursorMatchesTarget(cursor, input),
        "postgres_restore_recovery_cursor_conflict",
      );
      return objectFreeze({ advanced: false, cursor });
    }
    ensure(
      cursorMatchesExpected(cursor, input),
      "postgres_restore_recovery_cursor_conflict",
    );
    const now = canonicalTimestamp(
      ownDataValue(transaction, "now", code),
      code,
    );
    const rows = rowsFromResult(
      await queryTransaction(
        transaction,
        UPDATE_QUERY,
        [
          input.recoveryScopeId,
          input.lane,
          input.nextAfterSessionId,
          input.nextCycle,
          input.nextRevision,
          input.transitionId,
          input.requestSha256,
          now,
          input.expectedRevision,
          input.expectedCycle,
          input.expectedAfterSessionId,
        ],
        code,
      ),
      code,
    );
    ensure(
      rows.length === 1,
      "postgres_restore_recovery_cursor_conflict",
    );
    const updated = normalizeCursorRow(rows[0], input, code);
    ensure(
      cursorMatchesTarget(updated, input) && updated.updatedAt === now,
      "postgres_restore_recovery_cursor_state_invalid",
    );
    return objectFreeze({ advanced: true, cursor: updated });
  });
}

function storeErrorCommitState(error) {
  if (!isStoreError(error)) return null;
  try {
    const value = ownDataValue(
      error,
      "commitState",
      "postgres_restore_recovery_cursor_outcome_uncertain",
    );
    return value === "not-committed" || value === "uncertain"
      ? value
      : null;
  } catch {
    return null;
  }
}

export class PostgresRestoreRecoveryCursorStoreError extends ErrorConstructor {
  constructor(code) {
    if (typeof code !== "string" || !objectHasOwn(ERROR_MESSAGES, code)) {
      throw new TypeErrorConstructor(
        "Unsupported PostgreSQL restore recovery cursor store error",
      );
    }
    const message = ERROR_MESSAGES[code];
    super(message);
    objectDefineProperty(this, "name", {
      enumerable: false,
      value: "PostgresRestoreRecoveryCursorStoreError",
    });
    objectDefineProperty(this, "code", { enumerable: true, value: code });
    objectDefineProperty(this, "retryable", {
      enumerable: true,
      value: false,
    });
    objectDefineProperty(this, "stack", {
      enumerable: false,
      value: `PostgresRestoreRecoveryCursorStoreError: ${message}`,
    });
    callIntrinsic(weakSetAddIntrinsic, CURSOR_ERRORS, [this]);
    objectFreeze(this);
  }
}

/**
 * Low-level durable cursor CAS for the composed recovery runner. This store
 * proves only scoped cursor concurrency, replay, and commit outcome; it does
 * not inspect lane candidates or attest that a recovery batch has settled.
 * Production composition must keep the instance capability-private and call
 * advanceLane only after consuming an authentic recovery-service batch
 * receipt.
 */
export function createPostgresRestoreRecoveryCursorStore(...args) {
  const optionCode =
    "invalid_postgres_restore_recovery_cursor_store_options";
  ensure(args.length === 1, optionCode);
  const options = exactDataObject(args[0], ["store"], optionCode);
  const store = options.store;
  ensure(
    store !== null && typeof store === "object" && !isProxyValue(store),
    optionCode,
  );
  let storePrototype;
  let storeKeys;
  try {
    storePrototype = objectGetPrototypeOf(store);
    storeKeys = reflectOwnKeys(store);
  } catch {
    fail(optionCode);
  }
  ensure(
    storePrototype === PostgresSerializableStore.prototype &&
      storeKeys.length === 0 &&
      objectIsFrozen(store),
    optionCode,
  );
  const requestCode =
    "invalid_postgres_restore_recovery_cursor_store_request";

  const readLane = async function readLane(...readArgs) {
    ensure(readArgs.length === 1, requestCode);
    const input = normalizeReadRequest(readArgs[0], requestCode);
    const attempt = () =>
      runSerializable(store, (transaction) =>
        initializeAndRead(
          transaction,
          input,
          "postgres_restore_recovery_cursor_state_invalid",
        ),
      );
    try {
      return await attempt();
    } catch (error) {
      if (isCursorError(error)) throw error;
    }
    try {
      return await attempt();
    } catch (error) {
      if (isCursorError(error)) throw error;
      fail("postgres_restore_recovery_cursor_outcome_uncertain");
    }
  };

  const advanceLane = async function advanceLane(...advanceArgs) {
    ensure(advanceArgs.length === 1, requestCode);
    const input = normalizeAdvanceRequest(advanceArgs[0], requestCode);
    try {
      return await advanceTransaction(store, input);
    } catch (error) {
      if (isCursorError(error)) throw error;
      const commitState = storeErrorCommitState(error);
      let observed;
      try {
        observed = await readExisting(store, input);
      } catch (readError) {
        if (isCursorError(readError)) throw readError;
        fail("postgres_restore_recovery_cursor_outcome_uncertain");
      }
      if (observed !== null && cursorMatchesTarget(observed, input)) {
        return objectFreeze({ advanced: false, cursor: observed });
      }
      if (
        commitState === "not-committed" &&
        ((observed === null && inputExpectsInitialCursor(input)) ||
          (observed !== null && cursorMatchesExpected(observed, input)))
      ) {
        try {
          return await advanceTransaction(store, input);
        } catch (retryError) {
          if (isCursorError(retryError)) throw retryError;
        }
        try {
          observed = await readExisting(store, input);
        } catch (readError) {
          if (isCursorError(readError)) throw readError;
          fail("postgres_restore_recovery_cursor_outcome_uncertain");
        }
        if (observed !== null && cursorMatchesTarget(observed, input)) {
          return objectFreeze({ advanced: false, cursor: observed });
        }
      }
      fail("postgres_restore_recovery_cursor_outcome_uncertain");
    }
  };

  objectFreeze(readLane);
  objectFreeze(advanceLane);
  return objectFreeze({ advanceLane, readLane });
}

objectFreeze(PostgresRestoreRecoveryCursorStoreError.prototype);
objectFreeze(PostgresRestoreRecoveryCursorStoreError);
