import { createHash } from "node:crypto";
import { types as utilTypes } from "node:util";

export const POSTGRES_OPERATION_GUARD_NAMESPACE =
  "portable-codex-runtime:postgres-operation-guard:v1";

const OPERATION_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u;
const SIGNED_INT64_MIN = -(1n << 63n);
const SIGNED_INT64_MAX = (1n << 63n) - 1n;

const ERROR_MESSAGES = Object.freeze({
  invalid_postgres_operation_guard_options:
    "PostgreSQL operation guard options are invalid",
  invalid_postgres_operation_guard_request:
    "PostgreSQL operation guard request is invalid",
  postgres_operation_guard_busy:
    "PostgreSQL operation guard is already held",
  postgres_operation_guard_outcome_uncertain:
    "PostgreSQL operation guard outcome is uncertain",
});

const TRY_EXCLUSIVE_LOCK_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    "SELECT",
    "pg_catalog.pg_backend_pid() AS backend_pid,",
    "pg_catalog.pg_try_advisory_lock($1::pg_catalog.int8) AS acquired",
  ].join(" "),
});

const TRY_SHARED_LOCK_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    "SELECT",
    "pg_catalog.pg_backend_pid() AS backend_pid,",
    "pg_catalog.pg_try_advisory_lock_shared($1::pg_catalog.int8) AS acquired",
  ].join(" "),
});

const ASSERT_EXCLUSIVE_LOCK_HELD_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    "SELECT",
    "pg_catalog.pg_backend_pid() AS backend_pid,",
    "EXISTS (",
    "SELECT 1",
    "FROM pg_catalog.pg_locks",
    "WHERE locktype = 'advisory'",
    "AND database = (",
    "SELECT oid FROM pg_catalog.pg_database",
    "WHERE datname = pg_catalog.current_database()",
    ")",
    "AND pid = pg_catalog.pg_backend_pid()",
    "AND classid = (",
    "(($1::pg_catalog.int8 >> 32) & 4294967295::pg_catalog.int8)",
    ")::pg_catalog.oid",
    "AND objid = (",
    "($1::pg_catalog.int8 & 4294967295::pg_catalog.int8)",
    ")::pg_catalog.oid",
    "AND objsubid = 1",
    "AND mode = 'ExclusiveLock'",
    "AND granted",
    ") AS lock_held",
  ].join(" "),
});

const ASSERT_SHARED_LOCK_HELD_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    "SELECT",
    "pg_catalog.pg_backend_pid() AS backend_pid,",
    "EXISTS (",
    "SELECT 1",
    "FROM pg_catalog.pg_locks",
    "WHERE locktype = 'advisory'",
    "AND database = (",
    "SELECT oid FROM pg_catalog.pg_database",
    "WHERE datname = pg_catalog.current_database()",
    ")",
    "AND pid = pg_catalog.pg_backend_pid()",
    "AND classid = (",
    "(($1::pg_catalog.int8 >> 32) & 4294967295::pg_catalog.int8)",
    ")::pg_catalog.oid",
    "AND objid = (",
    "($1::pg_catalog.int8 & 4294967295::pg_catalog.int8)",
    ")::pg_catalog.oid",
    "AND objsubid = 1",
    "AND mode = 'ShareLock'",
    "AND granted",
    ") AS lock_held",
  ].join(" "),
});

const UNLOCK_EXCLUSIVE_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    "SELECT",
    "pg_catalog.pg_backend_pid() AS backend_pid,",
    "pg_catalog.pg_advisory_unlock($1::pg_catalog.int8) AS unlocked",
  ].join(" "),
});

const UNLOCK_SHARED_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    "SELECT",
    "pg_catalog.pg_backend_pid() AS backend_pid,",
    "pg_catalog.pg_advisory_unlock_shared($1::pg_catalog.int8) AS unlocked",
  ].join(" "),
});

const arrayEveryIntrinsic = Array.prototype.every;
const arrayIncludesIntrinsic = Array.prototype.includes;
const arrayIsArray = Array.isArray;
const bufferReadBigInt64BEIntrinsic = Buffer.prototype.readBigInt64BE;
const bigIntToStringIntrinsic = BigInt.prototype.toString;
const createHashIntrinsic = createHash;
const ErrorConstructor = Error;
const functionApplyIntrinsic = Reflect.apply;
const hashPrototype = Object.getPrototypeOf(createHash("sha256"));
const hashDigestIntrinsic = hashPrototype.digest;
const hashUpdateIntrinsic = hashPrototype.update;
const isProxyValue = utilTypes.isProxy;
const isGeneratorFunctionValue = utilTypes.isGeneratorFunction;
const isGeneratorObjectValue = utilTypes.isGeneratorObject;
const isPromiseValue = utilTypes.isPromise;
const numberIsSafeInteger = Number.isSafeInteger;
const objectCreate = Object.create;
const objectDefineProperty = Object.defineProperty;
const objectDefineProperties = Object.defineProperties;
const objectFreeze = Object.freeze;
const objectGetOwnPropertyDescriptor = Object.getOwnPropertyDescriptor;
const objectGetPrototypeOf = Object.getPrototypeOf;
const objectHasOwn = Object.hasOwn;
const objectIsFrozen = Object.isFrozen;
const objectPrototype = Object.prototype;
const PromiseConstructor = Promise;
const promisePrototype = Promise.prototype;
const promiseResolveIntrinsic = Promise.resolve;
const promiseSpeciesSymbol = Symbol.species;
const promiseThenIntrinsic = Promise.prototype.then;
const reflectOwnKeys = Reflect.ownKeys;
const regexpTestIntrinsic = RegExp.prototype.test;
const TypeErrorConstructor = TypeError;
const WeakSetConstructor = WeakSet;
const weakSetAddIntrinsic = WeakSet.prototype.add;
const weakSetHasIntrinsic = WeakSet.prototype.has;

const EXCLUSIVE_LOCK_MODE = objectFreeze({
  assertHeldQuery: ASSERT_EXCLUSIVE_LOCK_HELD_QUERY,
  tryLockQuery: TRY_EXCLUSIVE_LOCK_QUERY,
  unlockQuery: UNLOCK_EXCLUSIVE_QUERY,
});
const SHARED_LOCK_MODE = objectFreeze({
  assertHeldQuery: ASSERT_SHARED_LOCK_HELD_QUERY,
  tryLockQuery: TRY_SHARED_LOCK_QUERY,
  unlockQuery: UNLOCK_SHARED_QUERY,
});

const operationGuards = new WeakSetConstructor();
const promiseSpeciesHolder = objectFreeze(
  objectCreate(null, {
    [promiseSpeciesSymbol]: {
      configurable: false,
      enumerable: false,
      value: PromiseConstructor,
      writable: false,
    },
  }),
);

function callIntrinsic(intrinsic, receiver, args) {
  return functionApplyIntrinsic(intrinsic, receiver, args);
}

function weakSetAdd(set, value) {
  callIntrinsic(weakSetAddIntrinsic, set, [value]);
}

function weakSetHas(set, value) {
  return callIntrinsic(weakSetHasIntrinsic, set, [value]);
}

function arrayEvery(value, callback) {
  return callIntrinsic(arrayEveryIntrinsic, value, [callback]);
}

function arrayIncludes(value, candidate) {
  return callIntrinsic(arrayIncludesIntrinsic, value, [candidate]);
}

function regexpTest(pattern, value) {
  return callIntrinsic(regexpTestIntrinsic, pattern, [value]);
}

function makeError(code) {
  return new PostgresOperationGuardError(code);
}

function fail(code) {
  throw makeError(code);
}

function ensure(condition, code) {
  if (!condition) fail(code);
}

function exactFrozenRecord(value) {
  const result = objectCreate(null);
  const keys = reflectOwnKeys(value);
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
    const descriptor = objectGetOwnPropertyDescriptor(value, key);
    objectDefineProperty(result, key, {
      configurable: false,
      enumerable: true,
      value: descriptor.value,
      writable: false,
    });
  }
  return objectFreeze(result);
}

function safePromiseSpeciesHolder(value) {
  if (
    value === null ||
    typeof value !== "object" ||
    isProxyValue(value) ||
    !objectIsFrozen(value)
  ) {
    return false;
  }
  let prototype;
  let keys;
  try {
    prototype = objectGetPrototypeOf(value);
    keys = reflectOwnKeys(value);
  } catch {
    return false;
  }
  if (
    prototype !== null ||
    keys.length !== 1 ||
    keys[0] !== promiseSpeciesSymbol
  ) {
    return false;
  }
  const descriptor = objectGetOwnPropertyDescriptor(
    value,
    promiseSpeciesSymbol,
  );
  return (
    descriptor?.configurable === false &&
    descriptor.enumerable === false &&
    objectHasOwn(descriptor, "value") &&
    descriptor.value === PromiseConstructor &&
    descriptor.writable === false
  );
}

function protectPromiseReaction(callback) {
  if (typeof callback !== "function") return callback;
  return (value) => {
    const result = callIntrinsic(callback, undefined, [value]);
    if (isPromiseValue(result)) {
      const normalized = normalizeSafeNativePromise(
        result,
        "postgres_operation_guard_outcome_uncertain",
      );
      ensure(normalized !== null, "postgres_operation_guard_outcome_uncertain");
      return normalized;
    }
    assertSafeFulfilledValue(
      result,
      "postgres_operation_guard_outcome_uncertain",
    );
    return result;
  };
}

function protectedPromiseThen(onFulfilled, onRejected) {
  return protectPromise(
    callIntrinsic(promiseThenIntrinsic, this, [
      protectPromiseReaction(onFulfilled),
      protectPromiseReaction(onRejected),
    ]),
  );
}

function protectedPromiseCatch(onRejected) {
  return callIntrinsic(protectedPromiseThen, this, [
    undefined,
    onRejected,
  ]);
}

function resolveProtectedPromise(value) {
  if (!isPromiseValue(value)) {
    assertSafeFulfilledValue(
      value,
      "postgres_operation_guard_outcome_uncertain",
    );
  } else {
    value = normalizeSafeNativePromise(
      value,
      "postgres_operation_guard_outcome_uncertain",
    );
    ensure(value !== null, "postgres_operation_guard_outcome_uncertain");
  }
  return protectPromise(
    callIntrinsic(promiseResolveIntrinsic, PromiseConstructor, [value]),
  );
}

function protectedPromiseFinally(onFinally) {
  if (typeof onFinally !== "function") {
    return callIntrinsic(protectedPromiseThen, this, [
      onFinally,
      onFinally,
    ]);
  }
  const runFinally = () =>
    resolveProtectedPromise(callIntrinsic(onFinally, undefined, []));
  return callIntrinsic(protectedPromiseThen, this, [
    (value) =>
      callIntrinsic(protectedPromiseThen, runFinally(), [
        () => value,
        undefined,
      ]),
    (reason) =>
      callIntrinsic(protectedPromiseThen, runFinally(), [
        () => {
          throw reason;
        },
        undefined,
      ]),
  ]);
}

// Every Promise that crosses an await or callback boundary gets immutable own
// reaction methods plus a frozen species holder. This protects both language
// await and direct then/catch/finally calls from callback-time prototype poison.
function protectPromise(value) {
  const code = "postgres_operation_guard_outcome_uncertain";
  ensure(
    isPromiseValue(value) &&
      !isProxyValue(value) &&
      objectGetPrototypeOf(value) === promisePrototype,
    code,
  );
  const constructorDescriptor = objectGetOwnPropertyDescriptor(
    value,
    "constructor",
  );
  if (
    constructorDescriptor !== undefined &&
    objectHasOwn(constructorDescriptor, "value") &&
    constructorDescriptor.value !== promiseSpeciesHolder &&
    safePromiseSpeciesHolder(constructorDescriptor.value)
  ) {
    return protectPromise(
      callIntrinsic(promiseThenIntrinsic, value, [undefined, undefined]),
    );
  }
  try {
    objectDefineProperties(value, {
      catch: {
        configurable: false,
        enumerable: false,
        value: protectedPromiseCatch,
        writable: false,
      },
      constructor: {
        configurable: false,
        enumerable: false,
        value: promiseSpeciesHolder,
        writable: false,
      },
      finally: {
        configurable: false,
        enumerable: false,
        value: protectedPromiseFinally,
        writable: false,
      },
      then: {
        configurable: false,
        enumerable: false,
        value: protectedPromiseThen,
        writable: false,
      },
    });
  } catch {
    fail(code);
  }
  return value;
}

function normalizeSafeNativePromise(value, code) {
  if (
    !isPromiseValue(value) ||
    isProxyValue(value) ||
    isGeneratorObjectValue(value)
  ) {
    return null;
  }
  let prototype;
  let constructorDescriptor;
  try {
    prototype = objectGetPrototypeOf(value);
    constructorDescriptor = objectGetOwnPropertyDescriptor(
      value,
      "constructor",
    );
  } catch {
    return null;
  }
  if (prototype !== promisePrototype) return null;

  if (
    constructorDescriptor === undefined ||
    constructorDescriptor.configurable === true
  ) {
    try {
      return protectPromise(value);
    } catch {
      return null;
    }
  }

  if (
    objectHasOwn(constructorDescriptor, "value") &&
    constructorDescriptor.value === PromiseConstructor
  ) {
    try {
      objectDefineProperty(value, "constructor", {
        configurable: false,
        enumerable: false,
        value: PromiseConstructor,
        writable: false,
      });
      return protectPromise(
        callIntrinsic(promiseThenIntrinsic, value, [undefined, undefined]),
      );
    } catch {
      return null;
    }
  }

  if (
    !objectHasOwn(constructorDescriptor, "value") ||
    !safePromiseSpeciesHolder(constructorDescriptor.value)
  ) {
    return null;
  }
  try {
    if (constructorDescriptor.value === promiseSpeciesHolder) {
      return protectPromise(value);
    }
    return protectPromise(callIntrinsic(promiseThenIntrinsic, value, [
      undefined,
      undefined,
    ]));
  } catch {
    fail(code);
  }
}

function hasUntrustedThenableShape(value) {
  if (
    value === null ||
    (typeof value !== "object" && typeof value !== "function")
  ) {
    return false;
  }
  let current = value;
  for (let depth = 0; current !== null && depth < 64; depth += 1) {
    if (isProxyValue(current)) return true;
    let descriptor;
    try {
      descriptor = objectGetOwnPropertyDescriptor(current, "then");
      current = objectGetPrototypeOf(current);
    } catch {
      return true;
    }
    if (descriptor !== undefined) return true;
  }
  return current !== null;
}

function assertSafeFulfilledValue(value, code) {
  ensure(
    !isProxyValue(value) &&
      !isGeneratorFunctionValue(value) &&
      !isGeneratorObjectValue(value) &&
      !hasUntrustedThenableShape(value),
    code,
  );
}

async function settleValueInternal(value, code) {
  if (!isPromiseValue(value)) {
    assertSafeFulfilledValue(value, code);
    return exactFrozenRecord({ status: "fulfilled", value });
  }
  const normalized = normalizeSafeNativePromise(value, code);
  ensure(normalized !== null, code);
  let settled;
  try {
    settled = await normalized;
  } catch (error) {
    return exactFrozenRecord({ status: "rejected", value: error });
  }
  assertSafeFulfilledValue(settled, code);
  return exactFrozenRecord({ status: "fulfilled", value: settled });
}

function settleValue(value, code) {
  return protectPromise(settleValueInternal(value, code));
}

async function callAsyncValueInternal(method, receiver, args, code) {
  let value;
  try {
    value = callIntrinsic(method, receiver, args);
  } catch {
    fail(code);
  }
  const settlement = await settleValue(value, code);
  ensure(settlement.status === "fulfilled", code);
  return settlement.value;
}

function callAsyncValue(method, receiver, args, code) {
  return protectPromise(callAsyncValueInternal(method, receiver, args, code));
}

function inspectExactOptions(value) {
  const code = "invalid_postgres_operation_guard_options";
  if (
    value === null ||
    typeof value !== "object" ||
    arrayIsArray(value) ||
    isProxyValue(value)
  ) {
    fail(code);
  }
  let prototype;
  let keys;
  try {
    prototype = objectGetPrototypeOf(value);
    keys = reflectOwnKeys(value);
  } catch {
    fail(code);
  }
  ensure(
    arrayIncludes([objectPrototype, null], prototype) &&
      keys.length === 1 &&
      keys[0] === "dedicatedPool",
    code,
  );
  const descriptor = objectGetOwnPropertyDescriptor(value, "dedicatedPool");
  ensure(
    descriptor?.enumerable === true &&
      objectHasOwn(descriptor, "value"),
    code,
  );
  return descriptor.value;
}

function trustedPool(pool) {
  const code = "invalid_postgres_operation_guard_options";
  ensure(
    pool !== null &&
      arrayIncludes(["object", "function"], typeof pool) &&
      !isProxyValue(pool),
    code,
  );
  let connect;
  try {
    connect = pool.connect;
  } catch {
    fail(code);
  }
  ensure(typeof connect === "function" && !isProxyValue(connect), code);
  return objectFreeze({ connect, pool });
}

function normalizeOperationId(value) {
  ensure(
    typeof value === "string" &&
      regexpTest(OPERATION_ID_PATTERN, value),
    "invalid_postgres_operation_guard_request",
  );
  return value;
}

function normalizeCallback(value) {
  ensure(
    typeof value === "function" &&
      !isProxyValue(value) &&
      !isGeneratorFunctionValue(value),
    "invalid_postgres_operation_guard_request",
  );
  return value;
}

function operationLockKey(operationId) {
  const hash = callIntrinsic(createHashIntrinsic, undefined, ["sha256"]);
  callIntrinsic(hashUpdateIntrinsic, hash, [
    POSTGRES_OPERATION_GUARD_NAMESPACE,
    "utf8",
  ]);
  callIntrinsic(hashUpdateIntrinsic, hash, ["\0", "utf8"]);
  callIntrinsic(hashUpdateIntrinsic, hash, [operationId, "utf8"]);
  const digest = callIntrinsic(hashDigestIntrinsic, hash, []);
  const signed = callIntrinsic(bufferReadBigInt64BEIntrinsic, digest, [0]);
  ensure(
    signed >= SIGNED_INT64_MIN && signed <= SIGNED_INT64_MAX,
    "invalid_postgres_operation_guard_request",
  );
  return callIntrinsic(bigIntToStringIntrinsic, signed, []);
}

function ownDataValue(value, key, code) {
  if (
    value === null ||
    !arrayIncludes(["object", "function"], typeof value) ||
    isProxyValue(value)
  ) {
    fail(code);
  }
  let descriptor;
  try {
    descriptor = objectGetOwnPropertyDescriptor(value, key);
  } catch {
    fail(code);
  }
  ensure(descriptor !== undefined && objectHasOwn(descriptor, "value"), code);
  return descriptor.value;
}

function exactRow(result, keys) {
  const code = "postgres_operation_guard_outcome_uncertain";
  ensure(ownDataValue(result, "command", code) === "SELECT", code);
  const rows = ownDataValue(result, "rows", code);
  ensure(
    arrayIsArray(rows) && !isProxyValue(rows) && rows.length === 1,
    code,
  );
  const row = rows[0];
  ensure(
    row !== null &&
      typeof row === "object" &&
      !arrayIsArray(row) &&
      !isProxyValue(row),
    code,
  );
  let actual;
  let prototype;
  try {
    actual = reflectOwnKeys(row);
    prototype = objectGetPrototypeOf(row);
  } catch {
    fail(code);
  }
  ensure(
    arrayIncludes([objectPrototype, null], prototype) &&
      actual.length === keys.length &&
      arrayEvery(
        actual,
        (key) => typeof key === "string" && arrayIncludes(keys, key),
      ),
    code,
  );
  const normalized = objectCreate(null);
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
    const descriptor = objectGetOwnPropertyDescriptor(row, key);
    ensure(
      descriptor?.enumerable === true &&
        objectHasOwn(descriptor, "value"),
      code,
    );
    normalized[key] = descriptor.value;
  }
  return normalized;
}

function validatedBackendPid(value) {
  ensure(
    numberIsSafeInteger(value) && value > 0 && value <= 2_147_483_647,
    "postgres_operation_guard_outcome_uncertain",
  );
  return value;
}

function discardAcknowledged(result) {
  return ownDataValue(
    result,
    "command",
    "postgres_operation_guard_outcome_uncertain",
  ) === "DISCARD";
}

async function acquireClientInternal(poolBinding) {
  let client;
  try {
    client = await callAsyncValue(
      poolBinding.connect,
      poolBinding.pool,
      [],
      "postgres_operation_guard_outcome_uncertain",
    );
  } catch {
    fail("postgres_operation_guard_outcome_uncertain");
  }
  if (
    client === null ||
    !arrayIncludes(["object", "function"], typeof client) ||
    isProxyValue(client)
  ) {
    fail("postgres_operation_guard_outcome_uncertain");
  }
  let query;
  let release;
  try {
    release = client.release;
    query = client.query;
  } catch {
    if (typeof release === "function" && !isProxyValue(release)) {
      try {
        await callAsyncValue(
          release,
          client,
          [makeError("postgres_operation_guard_outcome_uncertain")],
          "postgres_operation_guard_outcome_uncertain",
        );
      } catch {
        // Shape uncertainty remains primary after best-effort destruction.
      }
    }
    fail("postgres_operation_guard_outcome_uncertain");
  }
  if (
    typeof query !== "function" ||
    typeof release !== "function" ||
    isProxyValue(query) ||
    isProxyValue(release)
  ) {
    if (typeof release === "function" && !isProxyValue(release)) {
      try {
        await callAsyncValue(
          release,
          client,
          [makeError("postgres_operation_guard_outcome_uncertain")],
          "postgres_operation_guard_outcome_uncertain",
        );
      } catch {
        // Shape uncertainty remains primary after best-effort destruction.
      }
    }
    fail("postgres_operation_guard_outcome_uncertain");
  }
  return exactFrozenRecord({ client, query, release });
}

function acquireClient(poolBinding) {
  return protectPromise(acquireClientInternal(poolBinding));
}

async function queryClientInternal(binding, query, values = undefined) {
  try {
    const args =
      values === undefined
        ? [query]
        : [
            objectFreeze({
              queryMode: query.queryMode,
              text: query.text,
              values: objectFreeze(values),
            }),
          ];
    return await callAsyncValue(
      binding.query,
      binding.client,
      args,
      "postgres_operation_guard_outcome_uncertain",
    );
  } catch {
    fail("postgres_operation_guard_outcome_uncertain");
  }
}

function queryClient(binding, query, values = undefined) {
  return protectPromise(queryClientInternal(binding, query, values));
}

async function resetClientInternal(binding) {
  const result = await queryClient(binding, "DISCARD ALL");
  ensure(
    discardAcknowledged(result),
    "postgres_operation_guard_outcome_uncertain",
  );
}

function resetClient(binding) {
  return protectPromise(resetClientInternal(binding));
}

async function destroyClientInternal(binding, cause) {
  try {
    await callAsyncValue(
      binding.release,
      binding.client,
      [cause],
      "postgres_operation_guard_outcome_uncertain",
    );
  } catch {
    fail("postgres_operation_guard_outcome_uncertain");
  }
}

function destroyClient(binding, cause) {
  return protectPromise(destroyClientInternal(binding, cause));
}

async function releaseClientInternal(binding) {
  try {
    await callAsyncValue(
      binding.release,
      binding.client,
      [],
      "postgres_operation_guard_outcome_uncertain",
    );
  } catch {
    fail("postgres_operation_guard_outcome_uncertain");
  }
}

function releaseClient(binding) {
  return protectPromise(releaseClientInternal(binding));
}

async function acquireAdvisoryLockInternal(binding, key, lockMode) {
  const result = await queryClient(binding, lockMode.tryLockQuery, [key]);
  const row = exactRow(result, ["acquired", "backend_pid"]);
  const backendPid = validatedBackendPid(row.backend_pid);
  ensure(
    typeof row.acquired === "boolean",
    "postgres_operation_guard_outcome_uncertain",
  );
  return exactFrozenRecord({ acquired: row.acquired, backendPid });
}

function acquireAdvisoryLock(binding, key, lockMode) {
  return protectPromise(
    acquireAdvisoryLockInternal(binding, key, lockMode),
  );
}

async function assertAdvisoryLockHeldInternal(
  binding,
  key,
  expectedBackendPid,
  lockMode,
) {
  const result = await queryClient(binding, lockMode.assertHeldQuery, [key]);
  const row = exactRow(result, ["backend_pid", "lock_held"]);
  ensure(
    validatedBackendPid(row.backend_pid) === expectedBackendPid &&
      row.lock_held === true,
    "postgres_operation_guard_outcome_uncertain",
  );
}

function assertAdvisoryLockHeld(binding, key, expectedBackendPid, lockMode) {
  return protectPromise(
    assertAdvisoryLockHeldInternal(
      binding,
      key,
      expectedBackendPid,
      lockMode,
    ),
  );
}

async function unlockAdvisoryLockInternal(
  binding,
  key,
  expectedBackendPid,
  lockMode,
) {
  const result = await queryClient(binding, lockMode.unlockQuery, [key]);
  const row = exactRow(result, ["backend_pid", "unlocked"]);
  ensure(
    validatedBackendPid(row.backend_pid) === expectedBackendPid &&
      row.unlocked === true,
    "postgres_operation_guard_outcome_uncertain",
  );
}

function unlockAdvisoryLock(binding, key, expectedBackendPid, lockMode) {
  return protectPromise(
    unlockAdvisoryLockInternal(
      binding,
      key,
      expectedBackendPid,
      lockMode,
    ),
  );
}

async function cleanAndReleaseInternal(
  binding,
  {
    backendPid,
    destroy,
    key,
    lockMode,
    shouldUnlock,
  },
) {
  let cleanupFailed = false;
  if (shouldUnlock) {
    try {
      await unlockAdvisoryLock(binding, key, backendPid, lockMode);
    } catch {
      cleanupFailed = true;
    }
  }
  try {
    await resetClient(binding);
  } catch {
    cleanupFailed = true;
  }

  const destroyCause =
    destroy || cleanupFailed
      ? makeError("postgres_operation_guard_outcome_uncertain")
      : undefined;
  try {
    if (destroyCause === undefined) {
      await releaseClient(binding);
    } else {
      await destroyClient(binding, destroyCause);
    }
  } catch {
    cleanupFailed = true;
  }
  if (cleanupFailed) {
    fail("postgres_operation_guard_outcome_uncertain");
  }
}

function cleanAndRelease(binding, options) {
  return protectPromise(cleanAndReleaseInternal(binding, options));
}

async function runWithLockModeInternal(poolBinding, args, lockMode) {
  ensure(
    args.length === 2,
    "invalid_postgres_operation_guard_request",
  );
  const operationId = normalizeOperationId(args[0]);
  const callback = normalizeCallback(args[1]);
  const key = operationLockKey(operationId);
  const binding = await acquireClient(poolBinding);

  try {
    await resetClient(binding);
  } catch {
    try {
      await destroyClient(
        binding,
        makeError("postgres_operation_guard_outcome_uncertain"),
      );
    } catch {
      // The pre-lock reset failure already requires a failed closed result.
    }
    fail("postgres_operation_guard_outcome_uncertain");
  }

  let lockAttempted = false;
  let lockKnownBusy = false;
  let lockHeld = false;
  let backendPid;
  let healthFailed = false;
  let busy = false;
  let callbackFailed = false;
  let callbackError;
  let callbackResult;

  try {
    lockAttempted = true;
    const acquired = await acquireAdvisoryLock(binding, key, lockMode);
    backendPid = acquired.backendPid;
    if (!acquired.acquired) {
      lockKnownBusy = true;
      busy = true;
    } else {
      lockHeld = true;
      await assertAdvisoryLockHeld(binding, key, backendPid, lockMode);

      let callbackOpen = true;
      let activeProbeHead = null;
      let activeProbeTail = null;
      let probeFailed = false;
      const assertHeld = (...probeArgs) => {
        const pending = protectPromise(
          (async () => {
            ensure(
              callbackOpen && probeArgs.length === 0,
              "postgres_operation_guard_outcome_uncertain",
            );
            await assertAdvisoryLockHeld(
              binding,
              key,
              backendPid,
              lockMode,
            );
          })(),
        );
        const node = objectCreate(null);
        node.drain = undefined;
        node.next = null;
        node.previous = activeProbeTail;
        if (activeProbeTail === null) {
          activeProbeHead = node;
        } else {
          activeProbeTail.next = node;
        }
        activeProbeTail = node;
        node.drain = protectPromise(
          (async () => {
            try {
              await pending;
            } catch {
              probeFailed = true;
            } finally {
              if (node.previous === null) {
                activeProbeHead = node.next;
              } else {
                node.previous.next = node.next;
              }
              if (node.next === null) {
                activeProbeTail = node.previous;
              } else {
                node.next.previous = node.previous;
              }
              node.drain = undefined;
              node.next = null;
              node.previous = null;
            }
          })(),
        );
        return pending;
      };
      const probe = objectFreeze({ assertHeld });

      let rawCallbackResult;
      try {
        rawCallbackResult = callIntrinsic(callback, undefined, [probe]);
      } catch (error) {
        callbackFailed = true;
        callbackError = error;
      }
      if (!callbackFailed) {
        try {
          const callbackSettlement = await settleValue(
            rawCallbackResult,
            "postgres_operation_guard_outcome_uncertain",
          );
          if (callbackSettlement.status === "rejected") {
            callbackFailed = true;
            callbackError = callbackSettlement.value;
          } else {
            callbackResult = callbackSettlement.value;
          }
        } catch (error) {
          callbackFailed = true;
          callbackError = error;
        }
      }
      // The callback is closed only after a native Promise has genuinely
      // settled; a poisoned prototype cannot advance this boundary.
      callbackOpen = false;

      while (activeProbeHead !== null) {
        await activeProbeHead.drain;
      }
      if (probeFailed) healthFailed = true;
      try {
        await assertAdvisoryLockHeld(binding, key, backendPid, lockMode);
      } catch {
        healthFailed = true;
      }
    }
  } catch {
    healthFailed = true;
  }

  try {
    await cleanAndRelease(binding, {
      backendPid,
      destroy: healthFailed,
      key,
      lockMode,
      shouldUnlock: lockAttempted && !lockKnownBusy,
    });
    lockHeld = false;
  } catch {
    fail("postgres_operation_guard_outcome_uncertain");
  }

  if (healthFailed || lockHeld) {
    fail("postgres_operation_guard_outcome_uncertain");
  }
  if (busy) fail("postgres_operation_guard_busy");
  if (callbackFailed) throw callbackError;
  assertSafeFulfilledValue(
    callbackResult,
    "postgres_operation_guard_outcome_uncertain",
  );
  return callbackResult;
}

function runWithLockMode(poolBinding, args, lockMode) {
  return protectPromise(runWithLockModeInternal(poolBinding, args, lockMode));
}

export class PostgresOperationGuardError extends ErrorConstructor {
  constructor(code) {
    const message = ERROR_MESSAGES[code];
    if (message === undefined) {
      throw new TypeErrorConstructor(
        "Unsupported PostgreSQL operation guard error",
      );
    }
    super(message);
    objectDefineProperty(this, "name", {
      configurable: false,
      enumerable: false,
      value: "PostgresOperationGuardError",
      writable: false,
    });
    objectDefineProperty(this, "code", {
      configurable: false,
      enumerable: true,
      value: code,
      writable: false,
    });
    objectDefineProperty(this, "retryable", {
      configurable: false,
      enumerable: true,
      value: false,
      writable: false,
    });
    objectDefineProperty(this, "stack", {
      configurable: false,
      enumerable: false,
      value: `PostgresOperationGuardError: ${message}`,
      writable: false,
    });
    objectFreeze(this);
  }
}

export class PostgresOperationGuard {
  #poolBinding;

  constructor(...args) {
    ensure(
      args.length === 1,
      "invalid_postgres_operation_guard_options",
    );
    this.#poolBinding = trustedPool(inspectExactOptions(args[0]));
    weakSetAdd(operationGuards, this);
    objectFreeze(this);
  }

  runExclusive(...args) {
    return runWithLockMode(this.#poolBinding, args, EXCLUSIVE_LOCK_MODE);
  }

  runShared(...args) {
    return runWithLockMode(this.#poolBinding, args, SHARED_LOCK_MODE);
  }
}

export function isPostgresOperationGuard(value) {
  return (
    value !== null &&
    (typeof value === "object" || typeof value === "function") &&
    !isProxyValue(value) &&
    weakSetHas(operationGuards, value)
  );
}

objectFreeze(PostgresOperationGuard.prototype);
objectFreeze(PostgresOperationGuardError.prototype);
