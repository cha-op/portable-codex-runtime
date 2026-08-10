import { createHash, randomUUID } from "node:crypto";
import { types as utilTypes } from "node:util";

import {
  consumePostgresRestoreActivationRecoveryBatchReceipt,
  isPostgresRestoreActivationRecoveryService,
} from "./postgres-restore-activation-recovery-service.mjs";

const arrayIncludesIntrinsic = Array.prototype.includes;
const arrayIsArray = Array.isArray;
const arrayPrototype = Array.prototype;
const BigIntConstructor = BigInt;
const DateConstructor = Date;
const dateParseIntrinsic = Date.parse;
const dateToISOStringIntrinsic = Date.prototype.toISOString;
const ErrorConstructor = Error;
const isGeneratorFunctionValue = utilTypes.isGeneratorFunction;
const isGeneratorObjectValue = utilTypes.isGeneratorObject;
const isPromiseValue = utilTypes.isPromise;
const isProxyValue = utilTypes.isProxy;
const jsonStringifyIntrinsic = JSON.stringify;
const numberIsFinite = Number.isFinite;
const numberIsSafeInteger = Number.isSafeInteger;
const objectCreate = Object.create;
const objectDefineProperty = Object.defineProperty;
const objectFreeze = Object.freeze;
const objectGetOwnPropertyDescriptor = Object.getOwnPropertyDescriptor;
const objectGetPrototypeOf = Object.getPrototypeOf;
const objectHasOwn = Object.hasOwn;
const objectIsFrozen = Object.isFrozen;
const objectPrototype = Object.prototype;
const objectSetPrototypeOfIntrinsic = Object.setPrototypeOf;
const PromiseConstructor = Promise;
const promisePrototype = Promise.prototype;
const promiseSpeciesSymbol = Symbol.species;
const promiseThenIntrinsic = Promise.prototype.then;
const randomUUIDIntrinsic = randomUUID;
const reflectApply = Reflect.apply;
const reflectOwnKeys = Reflect.ownKeys;
const regexpExecIntrinsic = RegExp.prototype.exec;
const TypeErrorConstructor = TypeError;

const hashProbe = createHash("sha256");
const hashPrototype = objectGetPrototypeOf(hashProbe);
const hashUpdateIntrinsic = hashPrototype.update;
const hashDigestIntrinsic = hashPrototype.digest;

const AbortSignalConstructor = globalThis.AbortSignal;
const abortSignalAbortedGetter = objectGetOwnPropertyDescriptor(
  AbortSignalConstructor.prototype,
  "aborted",
).get;

const MAX_BATCH_SIZE = 100;
const MAX_BIGINT = 9_223_372_036_854_775_807n;
const OPAQUE_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u;
const SESSION_ID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u;
const SHA256_PATTERN = /^[0-9a-f]{64}$/u;
const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u;
const UNSIGNED_DECIMAL_PATTERN = /^(?:0|[1-9][0-9]*)$/u;

const OPTION_KEYS = objectFreeze([
  "cursorStore",
  "limits",
  "recoveryScopeId",
  "recoveryService",
]);
const LIMIT_KEYS = objectFreeze([
  "activation",
  "currentLaunch",
  "generation",
  "launchAttempt",
]);
const RUN_REQUEST_KEYS = objectFreeze(["signal"]);
const CURSOR_KEYS = objectFreeze([
  "afterSessionId",
  "cycle",
  "lane",
  "lastRequestSha256",
  "lastTransitionId",
  "recoveryScopeId",
  "revision",
  "updatedAt",
]);
const BATCH_KEYS = objectFreeze([
  "afterSessionId",
  "nextAfterSessionId",
  "results",
  "status",
]);
const BATCH_RESULT_KEYS = objectFreeze([
  "operationId",
  "sessionId",
  "status",
]);
const ADVANCE_RESULT_KEYS = objectFreeze(["advanced", "cursor"]);
const LANE_ORDER = objectFreeze([
  objectFreeze(["generation", "generation", "runGenerationBatch"]),
  objectFreeze(["activation", "activation", "runActivationBatch"]),
  objectFreeze([
    "launchAttempt",
    "launch-attempt",
    "runLaunchAttemptBatch",
  ]),
  objectFreeze([
    "currentLaunch",
    "current-launch",
    "scanCurrentLaunchBatch",
  ]),
]);
const RESULT_LANE_KEYS = objectFreeze([
  "generation",
  "activation",
  "launchAttempt",
  "currentLaunch",
]);

const ERROR_MESSAGES = objectFreeze({
  invalid_postgres_restore_recovery_runner_options:
    "PostgreSQL restore recovery runner options are invalid",
  invalid_postgres_restore_recovery_runner_request:
    "PostgreSQL restore recovery runner request is invalid",
  postgres_restore_recovery_runner_outcome_uncertain:
    "PostgreSQL restore recovery runner outcome is uncertain",
});

function callIntrinsic(intrinsic, receiver, args) {
  return reflectApply(intrinsic, receiver, args);
}

function regexpTest(pattern, value) {
  return callIntrinsic(regexpExecIntrinsic, pattern, [value]) !== null;
}

function arrayIncludes(value, candidate) {
  return callIntrinsic(arrayIncludesIntrinsic, value, [candidate]);
}

function fail(code) {
  throw new PostgresRestoreRecoveryRunnerError(code);
}

function ensure(condition, code) {
  if (!condition) fail(code);
}

function exactDataObject(
  value,
  expectedKeys,
  code,
  { frozen = false } = {},
) {
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
      (!frozen || objectIsFrozen(value)) &&
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
    normalized[key] = descriptor.value;
  }
  return normalized;
}

function exactFrozenRecord(value) {
  const result = objectCreate(null);
  const keys = reflectOwnKeys(value);
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
    objectDefineProperty(result, key, {
      enumerable: true,
      value: value[key],
    });
  }
  return objectFreeze(result);
}

function frozenArray(values) {
  const result = [];
  for (let index = 0; index < values.length; index += 1) {
    result[index] = values[index];
  }
  return objectFreeze(result);
}

function frozenNullPrototypeArray(values) {
  const result = [];
  for (let index = 0; index < values.length; index += 1) {
    result[index] = values[index];
  }
  callIntrinsic(objectSetPrototypeOfIntrinsic, Object, [result, null]);
  return objectFreeze(result);
}

function canonicalOpaqueId(value, code) {
  ensure(
    typeof value === "string" && regexpTest(OPAQUE_ID_PATTERN, value),
    code,
  );
  return value;
}

function canonicalSessionId(value, code) {
  ensure(
    typeof value === "string" && regexpTest(SESSION_ID_PATTERN, value),
    code,
  );
  return value;
}

function canonicalUuid(value, code) {
  ensure(typeof value === "string" && regexpTest(UUID_PATTERN, value), code);
  return value;
}

function canonicalSha256(value, code) {
  ensure(
    typeof value === "string" && regexpTest(SHA256_PATTERN, value),
    code,
  );
  return value;
}

function canonicalUnsignedDecimal(value, code) {
  ensure(
    typeof value === "string" &&
      value.length >= 1 &&
      regexpTest(UNSIGNED_DECIMAL_PATTERN, value),
    code,
  );
  let parsed;
  try {
    parsed = callIntrinsic(BigIntConstructor, undefined, [value]);
  } catch {
    fail(code);
  }
  ensure(parsed <= MAX_BIGINT, code);
  return exactFrozenRecord({ parsed, value });
}

function canonicalTimestamp(value, code) {
  ensure(typeof value === "string", code);
  let milliseconds;
  let canonical;
  try {
    milliseconds = callIntrinsic(dateParseIntrinsic, DateConstructor, [value]);
    canonical = callIntrinsic(
      dateToISOStringIntrinsic,
      new DateConstructor(value),
      [],
    );
  } catch {
    fail(code);
  }
  ensure(numberIsFinite(milliseconds) && canonical === value, code);
  return value;
}

function signalIsAborted(signal, code) {
  if (signal === null) return false;
  ensure(
    signal !== null &&
      typeof signal === "object" &&
      !isProxyValue(signal),
    code,
  );
  try {
    return callIntrinsic(abortSignalAbortedGetter, signal, []);
  } catch {
    fail(code);
  }
}

function normalizeLimits(value, code) {
  const normalized = exactDataObject(value, LIMIT_KEYS, code);
  const result = objectCreate(null);
  for (let index = 0; index < RESULT_LANE_KEYS.length; index += 1) {
    const lane = RESULT_LANE_KEYS[index];
    const limit = normalized[lane];
    ensure(
      numberIsSafeInteger(limit) && limit >= 1 && limit <= MAX_BATCH_SIZE,
      code,
    );
    result[lane] = limit;
  }
  return exactFrozenRecord(result);
}

function collaboratorMethod(value, name, code) {
  ensure(
    value !== null &&
      typeof value === "object" &&
      !isProxyValue(value) &&
      !arrayIsArray(value) &&
      objectIsFrozen(value),
    code,
  );
  let descriptor;
  try {
    descriptor = objectGetOwnPropertyDescriptor(value, name);
  } catch {
    fail(code);
  }
  ensure(
    descriptor?.enumerable === true &&
      objectHasOwn(descriptor, "value") &&
      typeof descriptor.value === "function" &&
      !isProxyValue(descriptor.value) &&
      !isGeneratorFunctionValue(descriptor.value),
    code,
  );
  return descriptor.value;
}

function isSafePromiseSpeciesHolder(value) {
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

function normalizeSafeNativePromise(value) {
  if (
    !isPromiseValue(value) ||
    isProxyValue(value) ||
    isGeneratorObjectValue(value)
  ) {
    return null;
  }
  let prototype;
  let descriptor;
  try {
    prototype = objectGetPrototypeOf(value);
    descriptor = objectGetOwnPropertyDescriptor(value, "constructor");
    if (descriptor === undefined) {
      descriptor = objectGetOwnPropertyDescriptor(
        promisePrototype,
        "constructor",
      );
    }
  } catch {
    return null;
  }
  if (
    prototype !== promisePrototype ||
    descriptor === undefined ||
    !objectHasOwn(descriptor, "value")
  ) {
    return null;
  }
  if (descriptor.value === PromiseConstructor) return value;
  if (!isSafePromiseSpeciesHolder(descriptor.value)) return null;
  try {
    const normalized = callIntrinsic(promiseThenIntrinsic, value, [
      undefined,
      undefined,
    ]);
    objectDefineProperty(normalized, "constructor", {
      configurable: false,
      enumerable: false,
      value: PromiseConstructor,
      writable: false,
    });
    return normalized;
  } catch {
    return null;
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
    if (descriptor === undefined) continue;
    if (!objectHasOwn(descriptor, "value")) return true;
    return typeof descriptor.value === "function";
  }
  return current !== null;
}

async function callCollaborator(method, receiver, input, code) {
  let value;
  try {
    value = callIntrinsic(method, receiver, [input]);
    if (isGeneratorObjectValue(value)) fail(code);
    if (isPromiseValue(value)) {
      value = normalizeSafeNativePromise(value);
      ensure(value !== null, code);
      value = await value;
    } else {
      ensure(!hasUntrustedThenableShape(value), code);
    }
    ensure(
      !isGeneratorObjectValue(value) && !hasUntrustedThenableShape(value),
      code,
    );
    return value;
  } catch {
    // Collaborators are outside the runner's error-brand boundary. Even an
    // exported runner-shaped error from one of them is untrusted and must not
    // escape as a caller/request classification.
    fail(code);
  }
}

function normalizeCursor(value, recoveryScopeId, lane, code) {
  const cursor = exactDataObject(value, CURSOR_KEYS, code, { frozen: true });
  const afterSessionId =
    cursor.afterSessionId === null
      ? null
      : canonicalSessionId(cursor.afterSessionId, code);
  const lastTransitionId =
    cursor.lastTransitionId === null
      ? null
      : canonicalUuid(cursor.lastTransitionId, code);
  const lastRequestSha256 =
    cursor.lastRequestSha256 === null
      ? null
      : canonicalSha256(cursor.lastRequestSha256, code);
  ensure(
    cursor.recoveryScopeId === recoveryScopeId &&
      cursor.lane === lane &&
      (lastTransitionId === null) === (lastRequestSha256 === null),
    code,
  );
  const cycle = canonicalUnsignedDecimal(cursor.cycle, code);
  const revision = canonicalUnsignedDecimal(cursor.revision, code);
  ensure(cycle.parsed <= revision.parsed, code);
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
  return exactFrozenRecord({
    recoveryScopeId,
    lane,
    afterSessionId,
    cycle: cycle.value,
    revision: revision.value,
    lastTransitionId,
    lastRequestSha256,
    updatedAt: canonicalTimestamp(cursor.updatedAt, code),
  });
}

function normalizeFrozenArray(value, code) {
  ensure(
    value !== null &&
      typeof value === "object" &&
      !isProxyValue(value) &&
      arrayIsArray(value) &&
      objectIsFrozen(value),
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
    (prototype === arrayPrototype || prototype === null) &&
      keys.length === value.length + 1 &&
      keys[keys.length - 1] === "length",
    code,
  );
  const result = [];
  for (let index = 0; index < value.length; index += 1) {
    const key = `${index}`;
    ensure(keys[index] === key, code);
    const descriptor = objectGetOwnPropertyDescriptor(value, key);
    ensure(
      descriptor?.enumerable === true && objectHasOwn(descriptor, "value"),
      code,
    );
    result[index] = descriptor.value;
  }
  return result;
}

function normalizeBatch(value, cursor, field, limit, code) {
  const batch = exactDataObject(value, BATCH_KEYS, code, { frozen: true });
  const afterSessionId =
    batch.afterSessionId === null
      ? null
      : canonicalSessionId(batch.afterSessionId, code);
  const nextAfterSessionId =
    batch.nextAfterSessionId === null
      ? null
      : canonicalSessionId(batch.nextAfterSessionId, code);
  ensure(afterSessionId === cursor.afterSessionId, code);
  ensure(
    batch.status === "aborted" ||
      batch.status === "limit-reached" ||
      batch.status === "sweep-complete",
    code,
  );
  const values = normalizeFrozenArray(batch.results, code);
  ensure(values.length <= limit, code);
  const results = [];
  let previousSessionId = afterSessionId;
  for (let index = 0; index < values.length; index += 1) {
    const normalized = exactDataObject(values[index], BATCH_RESULT_KEYS, code, {
      frozen: true,
    });
    const operationId = canonicalOpaqueId(normalized.operationId, code);
    const sessionId = canonicalSessionId(normalized.sessionId, code);
    ensure(previousSessionId === null || sessionId > previousSessionId, code);
    if (field === "currentLaunch") {
      ensure(normalized.status === "requires-stop-or-fence", code);
    } else {
      ensure(
        normalized.status === "pending" || normalized.status === "reconciled",
        code,
      );
    }
    results[index] = exactFrozenRecord({
      operationId,
      sessionId,
      status: normalized.status,
    });
    previousSessionId = sessionId;
  }

  if (batch.status === "sweep-complete") {
    ensure(nextAfterSessionId === null, code);
  } else if (batch.status === "limit-reached") {
    ensure(
      nextAfterSessionId !== null &&
        (afterSessionId === null || nextAfterSessionId > afterSessionId),
      code,
    );
    if (field === "currentLaunch") {
      ensure(
        previousSessionId === null || nextAfterSessionId >= previousSessionId,
        code,
      );
    } else {
      ensure(
        results.length === limit && nextAfterSessionId === previousSessionId,
        code,
      );
    }
  } else {
    ensure(
      nextAfterSessionId ===
        (results.length === 0 ? afterSessionId : previousSessionId),
      code,
    );
  }

  return exactFrozenRecord({
    afterSessionId,
    nextAfterSessionId,
    results: frozenArray(results),
    status: batch.status,
  });
}

function canonicalRequestSha256({
  batch,
  cursor,
  lane,
  limit,
  recoveryScopeId,
}) {
  // This payload is the stable identity of one settled lane result. The UUID
  // distinguishes attempts, while the cursor CAS decides durability: after an
  // ambiguous advance, a restarted runner reads the row instead of trusting
  // process-local state or replaying from an in-memory transition identifier.
  let serialized;
  let hash;
  try {
    // JSON.stringify performs inherited toJSON lookups for object values.
    // Keep the public batch array conventional, but hash a private Array whose
    // prototype is null. The payload, batch, and result records already have
    // null prototypes, so no inherited hook can rewrite or collapse the
    // durable request identity. Array classification and key order remain
    // unchanged, preserving the unpolluted JSON bytes.
    const digestBatch = exactFrozenRecord({
      afterSessionId: batch.afterSessionId,
      nextAfterSessionId: batch.nextAfterSessionId,
      results: frozenNullPrototypeArray(batch.results),
      status: batch.status,
    });
    const payload = exactFrozenRecord({
      contractVersion: 1,
      recoveryScopeId,
      lane,
      expectedRevision: cursor.revision,
      expectedCycle: cursor.cycle,
      expectedAfterSessionId: cursor.afterSessionId,
      nextAfterSessionId: batch.nextAfterSessionId,
      limit,
      batch: digestBatch,
    });
    serialized = callIntrinsic(jsonStringifyIntrinsic, JSON, [payload]);
    hash = callIntrinsic(createHash, undefined, ["sha256"]);
    callIntrinsic(hashUpdateIntrinsic, hash, [serialized, "utf8"]);
    return callIntrinsic(hashDigestIntrinsic, hash, ["hex"]);
  } catch {
    fail("postgres_restore_recovery_runner_outcome_uncertain");
  }
}

function incrementCanonicalDecimal(value, code) {
  const normalized = canonicalUnsignedDecimal(value, code);
  ensure(normalized.parsed < MAX_BIGINT, code);
  return canonicalUnsignedDecimal(
    `${normalized.parsed + 1n}`,
    code,
  ).value;
}

function normalizeAdvance(
  value,
  {
    batch,
    cursorBefore,
    lane,
    recoveryScopeId,
    requestSha256,
    transitionId,
  },
  code,
) {
  const advance = exactDataObject(value, ADVANCE_RESULT_KEYS, code, {
    frozen: true,
  });
  ensure(typeof advance.advanced === "boolean", code);
  const cursor = normalizeCursor(
    advance.cursor,
    recoveryScopeId,
    lane,
    code,
  );
  ensure(
    cursor.afterSessionId === batch.nextAfterSessionId &&
      cursor.revision === incrementCanonicalDecimal(cursorBefore.revision, code) &&
      cursor.cycle ===
        (batch.nextAfterSessionId === null
          ? incrementCanonicalDecimal(cursorBefore.cycle, code)
          : cursorBefore.cycle) &&
      cursor.lastTransitionId === transitionId &&
      cursor.lastRequestSha256 === requestSha256,
    code,
  );
  return exactFrozenRecord({ advanced: advance.advanced, cursor });
}

function emptyResult(recoveryScopeId) {
  return exactFrozenRecord({
    recoveryScopeId,
    generation: null,
    activation: null,
    launchAttempt: null,
    currentLaunch: null,
    status: "aborted",
  });
}

export class PostgresRestoreRecoveryRunnerError extends ErrorConstructor {
  constructor(code) {
    if (typeof code !== "string" || !objectHasOwn(ERROR_MESSAGES, code)) {
      throw new TypeErrorConstructor(
        "Unsupported PostgreSQL restore recovery runner error",
      );
    }
    const message = ERROR_MESSAGES[code];
    super(message);
    objectDefineProperty(this, "name", {
      enumerable: false,
      value: "PostgresRestoreRecoveryRunnerError",
    });
    objectDefineProperty(this, "code", { enumerable: true, value: code });
    objectDefineProperty(this, "retryable", {
      enumerable: true,
      value: false,
    });
    objectDefineProperty(this, "stack", {
      enumerable: false,
      value: `PostgresRestoreRecoveryRunnerError: ${message}`,
    });
    objectFreeze(this);
  }
}

export function createPostgresRestoreRecoveryRunner(...args) {
  const optionCode = "invalid_postgres_restore_recovery_runner_options";
  ensure(args.length === 1, optionCode);
  const options = exactDataObject(args[0], OPTION_KEYS, optionCode);
  const recoveryScopeId = canonicalOpaqueId(
    options.recoveryScopeId,
    optionCode,
  );
  const limits = normalizeLimits(options.limits, optionCode);
  const cursorStore = options.cursorStore;
  const recoveryService = options.recoveryService;
  ensure(
    isPostgresRestoreActivationRecoveryService(recoveryService),
    optionCode,
  );
  const readLane = collaboratorMethod(cursorStore, "readLane", optionCode);
  const advanceLane = collaboratorMethod(
    cursorStore,
    "advanceLane",
    optionCode,
  );
  const recoveryMethods = objectCreate(null);
  for (let index = 0; index < LANE_ORDER.length; index += 1) {
    const [field, , methodName] = LANE_ORDER[index];
    recoveryMethods[field] = collaboratorMethod(
      recoveryService,
      methodName,
      optionCode,
    );
  }
  const frozenRecoveryMethods = exactFrozenRecord(recoveryMethods);

  const requestCode = "invalid_postgres_restore_recovery_runner_request";
  const outcomeCode = "postgres_restore_recovery_runner_outcome_uncertain";
  let inFlight = false;

  const runOnce = async function runOnce(...runArgs) {
    ensure(runArgs.length === 1, requestCode);
    const request = exactDataObject(
      runArgs[0],
      RUN_REQUEST_KEYS,
      requestCode,
    );
    signalIsAborted(request.signal, requestCode);
    ensure(!inFlight, outcomeCode);
    inFlight = true;
    try {
      if (signalIsAborted(request.signal, requestCode)) {
        return emptyResult(recoveryScopeId);
      }
      const laneReceipts = objectCreate(null);
      for (let index = 0; index < RESULT_LANE_KEYS.length; index += 1) {
        laneReceipts[RESULT_LANE_KEYS[index]] = null;
      }
      let status = "limit-reached";
      for (let index = 0; index < LANE_ORDER.length; index += 1) {
        const [field, lane] = LANE_ORDER[index];
        if (signalIsAborted(request.signal, requestCode)) {
          status = "aborted";
          break;
        }

        const rawCursor = await callCollaborator(
          readLane,
          cursorStore,
          exactFrozenRecord({ recoveryScopeId, lane }),
          outcomeCode,
        );
        const cursorBefore = normalizeCursor(
          rawCursor,
          recoveryScopeId,
          lane,
          outcomeCode,
        );
        const rawBatch = await callCollaborator(
          frozenRecoveryMethods[field],
          recoveryService,
          exactFrozenRecord({
            afterSessionId: cursorBefore.afterSessionId,
            limit: limits[field],
            signal: request.signal,
          }),
          outcomeCode,
        );
        ensure(
          consumePostgresRestoreActivationRecoveryBatchReceipt(
            recoveryService,
            field,
            cursorBefore.afterSessionId,
            limits[field],
            rawBatch,
          ),
          outcomeCode,
        );
        const batch = normalizeBatch(
          rawBatch,
          cursorBefore,
          field,
          limits[field],
          outcomeCode,
        );
        if (
          batch.status === "aborted" &&
          batch.nextAfterSessionId === cursorBefore.afterSessionId
        ) {
          laneReceipts[field] = exactFrozenRecord({
            cursorBefore,
            batch,
            transitionId: null,
            requestSha256: null,
            advance: null,
          });
          status = "aborted";
          break;
        }

        const requestSha256 = canonicalRequestSha256({
          batch,
          cursor: cursorBefore,
          lane,
          limit: limits[field],
          recoveryScopeId,
        });
        let transitionId;
        try {
          transitionId = canonicalOpaqueId(
            callIntrinsic(randomUUIDIntrinsic, undefined, []),
            outcomeCode,
          );
          ensure(regexpTest(UUID_PATTERN, transitionId), outcomeCode);
        } catch (error) {
          if (error instanceof PostgresRestoreRecoveryRunnerError) throw error;
          fail(outcomeCode);
        }
        const rawAdvance = await callCollaborator(
          advanceLane,
          cursorStore,
          exactFrozenRecord({
            recoveryScopeId,
            lane,
            transitionId,
            expectedRevision: cursorBefore.revision,
            expectedCycle: cursorBefore.cycle,
            expectedAfterSessionId: cursorBefore.afterSessionId,
            nextAfterSessionId: batch.nextAfterSessionId,
            requestSha256,
          }),
          outcomeCode,
        );
        const advance = normalizeAdvance(
          rawAdvance,
          {
            batch,
            cursorBefore,
            lane,
            recoveryScopeId,
            requestSha256,
            transitionId,
          },
          outcomeCode,
        );
        laneReceipts[field] = exactFrozenRecord({
          cursorBefore,
          batch,
          transitionId,
          requestSha256,
          advance,
        });
        if (batch.status === "aborted") {
          status = "aborted";
          break;
        }
      }

      if (status !== "aborted") {
        let allComplete = true;
        for (let index = 0; index < RESULT_LANE_KEYS.length; index += 1) {
          const receipt = laneReceipts[RESULT_LANE_KEYS[index]];
          if (receipt === null || receipt.batch.status !== "sweep-complete") {
            allComplete = false;
            break;
          }
        }
        status = allComplete ? "sweep-complete" : "limit-reached";
      }

      return exactFrozenRecord({
        recoveryScopeId,
        generation: laneReceipts.generation,
        activation: laneReceipts.activation,
        launchAttempt: laneReceipts.launchAttempt,
        currentLaunch: laneReceipts.currentLaunch,
        status,
      });
    } finally {
      inFlight = false;
    }
  };

  objectFreeze(runOnce);
  return exactFrozenRecord({ runOnce });
}
