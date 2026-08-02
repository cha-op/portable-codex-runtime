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

const TRY_LOCK_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    "SELECT",
    "pg_catalog.pg_backend_pid() AS backend_pid,",
    "pg_catalog.pg_try_advisory_lock($1::pg_catalog.int8) AS acquired",
  ].join(" "),
});

const ASSERT_LOCK_HELD_QUERY = Object.freeze({
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

const UNLOCK_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    "SELECT",
    "pg_catalog.pg_backend_pid() AS backend_pid,",
    "pg_catalog.pg_advisory_unlock($1::pg_catalog.int8) AS unlocked",
  ].join(" "),
});

const ArrayConstructor = Array;
const arrayFromIntrinsic = Array.from;
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
const numberIsSafeInteger = Number.isSafeInteger;
const objectCreate = Object.create;
const objectDefineProperty = Object.defineProperty;
const objectFreeze = Object.freeze;
const objectGetOwnPropertyDescriptor = Object.getOwnPropertyDescriptor;
const objectGetPrototypeOf = Object.getPrototypeOf;
const objectHasOwn = Object.hasOwn;
const objectPrototype = Object.prototype;
const PromiseConstructor = Promise;
const promiseAllSettledIntrinsic = Promise.allSettled;
const promiseThenIntrinsic = Promise.prototype.then;
const reflectOwnKeys = Reflect.ownKeys;
const regexpTestIntrinsic = RegExp.prototype.test;
const SetConstructor = Set;
const setAddIntrinsic = Set.prototype.add;
const setDeleteIntrinsic = Set.prototype.delete;
const setSizeGetter = objectGetOwnPropertyDescriptor(
  Set.prototype,
  "size",
).get;
const setValuesIntrinsic = Set.prototype.values;
const TypeErrorConstructor = TypeError;

function callIntrinsic(intrinsic, receiver, args) {
  return functionApplyIntrinsic(intrinsic, receiver, args);
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

function setSize(value) {
  return callIntrinsic(setSizeGetter, value, []);
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

async function acquireClient(poolBinding) {
  let client;
  try {
    client = await callIntrinsic(
      poolBinding.connect,
      poolBinding.pool,
      [],
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
        await callIntrinsic(release, client, [
          makeError("postgres_operation_guard_outcome_uncertain"),
        ]);
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
        await callIntrinsic(release, client, [
          makeError("postgres_operation_guard_outcome_uncertain"),
        ]);
      } catch {
        // Shape uncertainty remains primary after best-effort destruction.
      }
    }
    fail("postgres_operation_guard_outcome_uncertain");
  }
  return objectFreeze({ client, query, release });
}

async function queryClient(binding, query, values = undefined) {
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
    return await callIntrinsic(
      binding.query,
      binding.client,
      args,
    );
  } catch {
    fail("postgres_operation_guard_outcome_uncertain");
  }
}

async function resetClient(binding) {
  const result = await queryClient(binding, "DISCARD ALL");
  ensure(
    discardAcknowledged(result),
    "postgres_operation_guard_outcome_uncertain",
  );
}

async function destroyClient(binding, cause) {
  try {
    await callIntrinsic(binding.release, binding.client, [cause]);
  } catch {
    fail("postgres_operation_guard_outcome_uncertain");
  }
}

async function releaseClient(binding) {
  try {
    await callIntrinsic(binding.release, binding.client, []);
  } catch {
    fail("postgres_operation_guard_outcome_uncertain");
  }
}

async function acquireAdvisoryLock(binding, key) {
  const result = await queryClient(binding, TRY_LOCK_QUERY, [key]);
  const row = exactRow(result, ["acquired", "backend_pid"]);
  const backendPid = validatedBackendPid(row.backend_pid);
  ensure(
    typeof row.acquired === "boolean",
    "postgres_operation_guard_outcome_uncertain",
  );
  return objectFreeze({ acquired: row.acquired, backendPid });
}

async function assertAdvisoryLockHeld(binding, key, expectedBackendPid) {
  const result = await queryClient(binding, ASSERT_LOCK_HELD_QUERY, [key]);
  const row = exactRow(result, ["backend_pid", "lock_held"]);
  ensure(
    validatedBackendPid(row.backend_pid) === expectedBackendPid &&
      row.lock_held === true,
    "postgres_operation_guard_outcome_uncertain",
  );
}

async function unlockAdvisoryLock(binding, key, expectedBackendPid) {
  const result = await queryClient(binding, UNLOCK_QUERY, [key]);
  const row = exactRow(result, ["backend_pid", "unlocked"]);
  ensure(
    validatedBackendPid(row.backend_pid) === expectedBackendPid &&
      row.unlocked === true,
    "postgres_operation_guard_outcome_uncertain",
  );
}

async function cleanAndRelease(
  binding,
  {
    backendPid,
    destroy,
    key,
    shouldUnlock,
  },
) {
  let cleanupFailed = false;
  if (shouldUnlock) {
    try {
      await unlockAdvisoryLock(binding, key, backendPid);
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
    objectFreeze(this);
  }

  async runExclusive(...args) {
    ensure(
      args.length === 2,
      "invalid_postgres_operation_guard_request",
    );
    const operationId = normalizeOperationId(args[0]);
    const callback = normalizeCallback(args[1]);
    const key = operationLockKey(operationId);
    const binding = await acquireClient(this.#poolBinding);

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
      const acquired = await acquireAdvisoryLock(binding, key);
      backendPid = acquired.backendPid;
      if (!acquired.acquired) {
        lockKnownBusy = true;
        busy = true;
      } else {
        lockHeld = true;
        await assertAdvisoryLockHeld(binding, key, backendPid);

        let callbackOpen = true;
        let probeFailed = false;
        const activeProbes = new SetConstructor();
        const assertHeld = (...probeArgs) => {
          const pending = (async () => {
            ensure(
              callbackOpen && probeArgs.length === 0,
              "postgres_operation_guard_outcome_uncertain",
            );
            await assertAdvisoryLockHeld(binding, key, backendPid);
          })();
          callIntrinsic(setAddIntrinsic, activeProbes, [pending]);
          void callIntrinsic(promiseThenIntrinsic, pending, [
            () => {
              callIntrinsic(setDeleteIntrinsic, activeProbes, [pending]);
            },
            () => {
              probeFailed = true;
              callIntrinsic(setDeleteIntrinsic, activeProbes, [pending]);
            },
          ]);
          return pending;
        };
        const probe = objectFreeze({ assertHeld });

        try {
          callbackResult = await callIntrinsic(callback, undefined, [probe]);
        } catch (error) {
          callbackFailed = true;
          callbackError = error;
        } finally {
          callbackOpen = false;
        }

        while (setSize(activeProbes) > 0) {
          const pending = callIntrinsic(
            arrayFromIntrinsic,
            ArrayConstructor,
            [
              callIntrinsic(setValuesIntrinsic, activeProbes, []),
            ],
          );
          await callIntrinsic(
            promiseAllSettledIntrinsic,
            PromiseConstructor,
            [pending],
          );
        }
        if (probeFailed) healthFailed = true;
        try {
          await assertAdvisoryLockHeld(binding, key, backendPid);
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
    return callbackResult;
  }
}

objectFreeze(PostgresOperationGuard.prototype);
objectFreeze(PostgresOperationGuardError.prototype);
