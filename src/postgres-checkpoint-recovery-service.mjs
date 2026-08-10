import { types as utilTypes } from "node:util";

const arrayIsArray = Array.isArray;
const arrayPrototype = Array.prototype;
const BigIntConstructor = BigInt;
const DateConstructor = Date;
const dateParseIntrinsic = Date.parse;
const dateToISOStringIntrinsic = Date.prototype.toISOString;
const ErrorConstructor = Error;
const isGeneratorFunctionValue = utilTypes.isGeneratorFunction;
const isGeneratorObjectValue = utilTypes.isGeneratorObject;
const isProxyValue = utilTypes.isProxy;
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
const reflectApply = Reflect.apply;
const reflectOwnKeys = Reflect.ownKeys;
const regexpExecIntrinsic = RegExp.prototype.exec;
const TypeErrorConstructor = TypeError;

const AbortSignalConstructor = globalThis.AbortSignal;
const abortSignalPrototype = AbortSignalConstructor.prototype;
const abortSignalAbortedGetter = objectGetOwnPropertyDescriptor(
  abortSignalPrototype,
  "aborted",
).get;

const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u;
const OPAQUE_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u;
const OCI_DIGEST_PATTERN = /^sha256:[0-9a-f]{64}$/u;
const POSITIVE_UINT64_PATTERN = /^[1-9][0-9]{0,19}$/u;
const UINT64_MAX = 18_446_744_073_709_551_615n;
const MAX_BATCH_SIZE = 100;

const LEGACY_OPTION_KEYS = objectFreeze([
  "listCandidates",
  "reconcileCheckpointCapture",
]);
const OPTION_KEYS = objectFreeze([
  ...LEGACY_OPTION_KEYS,
  "resumePreparedCheckpointCapture",
]);
const RUN_BATCH_KEYS = objectFreeze([
  "afterSessionId",
  "limit",
  "signal",
]);
const PAGE_KEYS = objectFreeze([
  "candidates",
  "nextAfterSessionId",
]);
const CANDIDATE_KEYS = objectFreeze([
  "checkpoint",
  "request",
  "state",
]);
const CHECKPOINT_KEYS = objectFreeze([
  "artifactId",
  "backendId",
  "checkpointClass",
  "checkpointId",
  "codexSessionId",
  "codexThreadId",
  "contractVersion",
  "createdAt",
  "imageDigest",
  "sessionId",
  "sourceFencingEpoch",
  "storageId",
]);
const MUTATION_REQUEST_KEYS = objectFreeze([
  "backendId",
  "contractVersion",
  "fencingEpoch",
  "holderId",
  "leaseId",
  "operation",
  "operationId",
  "sessionId",
  "storageId",
  "target",
]);
const MUTATION_TARGET_KEYS = objectFreeze([
  "artifactId",
  "checkpointId",
  "kind",
]);

const ERROR_MESSAGES = objectCreate(null);
objectDefineProperty(
  ERROR_MESSAGES,
  "invalid_postgres_checkpoint_recovery_service_options",
  {
    enumerable: true,
    value: "PostgreSQL checkpoint recovery service options are invalid",
  },
);
objectDefineProperty(
  ERROR_MESSAGES,
  "invalid_postgres_checkpoint_recovery_service_request",
  {
    enumerable: true,
    value: "PostgreSQL checkpoint recovery service request is invalid",
  },
);
objectDefineProperty(
  ERROR_MESSAGES,
  "postgres_checkpoint_recovery_service_outcome_uncertain",
  {
    enumerable: true,
    value: "PostgreSQL checkpoint recovery service outcome is uncertain",
  },
);
objectFreeze(ERROR_MESSAGES);

function callIntrinsic(intrinsic, receiver, args) {
  return reflectApply(intrinsic, receiver, args);
}

function regexpTest(pattern, value) {
  return callIntrinsic(regexpExecIntrinsic, pattern, [value]) !== null;
}

function makeError(code) {
  return new PostgresCheckpointRecoveryServiceError(code);
}

function fail(code) {
  throw makeError(code);
}

function ensure(condition, code) {
  if (!condition) fail(code);
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
    let descriptor;
    try {
      descriptor = objectGetOwnPropertyDescriptor(value, key);
    } catch {
      fail(code);
    }
    let expected = false;
    if (typeof key === "string") {
      for (
        let expectedIndex = 0;
        expectedIndex < expectedKeys.length;
        expectedIndex += 1
      ) {
        if (key === expectedKeys[expectedIndex]) {
          expected = true;
          break;
        }
      }
    }
    ensure(
      expected &&
        descriptor?.enumerable === true &&
        objectHasOwn(descriptor, "value"),
      code,
    );
    objectDefineProperty(normalized, key, {
      enumerable: true,
      value: descriptor.value,
    });
  }
  for (let index = 0; index < expectedKeys.length; index += 1) {
    ensure(objectHasOwn(normalized, expectedKeys[index]), code);
  }
  return normalized;
}

function exactDataObjectVariant(value, expectedKeySets, code) {
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
  ensure(prototype === objectPrototype || prototype === null, code);
  let expectedKeys = null;
  for (let variant = 0; variant < expectedKeySets.length; variant += 1) {
    const candidate = expectedKeySets[variant];
    if (keys.length !== candidate.length) continue;
    let matches = true;
    for (let index = 0; matches && index < keys.length; index += 1) {
      const key = keys[index];
      let found = false;
      if (typeof key === "string") {
        for (
          let expectedIndex = 0;
          expectedIndex < candidate.length;
          expectedIndex += 1
        ) {
          if (key === candidate[expectedIndex]) {
            found = true;
            break;
          }
        }
      }
      matches = found;
    }
    if (matches) {
      expectedKeys = candidate;
      break;
    }
  }
  ensure(expectedKeys !== null, code);
  return exactDataObject(value, expectedKeys, code);
}

function exactFrozenRecord(value) {
  const record = objectCreate(null);
  const keys = reflectOwnKeys(value);
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
    objectDefineProperty(record, key, {
      enumerable: true,
      value: value[key],
    });
  }
  return objectFreeze(record);
}

function normalizeCallback(value, code) {
  ensure(
    typeof value === "function" &&
      !isProxyValue(value) &&
      !isGeneratorFunctionValue(value),
    code,
  );
  return value;
}

function canonicalSessionId(value, code) {
  ensure(
    typeof value === "string" && regexpTest(UUID_PATTERN, value),
    code,
  );
  return value;
}

function opaqueId(value, code) {
  ensure(
    typeof value === "string" && regexpTest(OPAQUE_ID_PATTERN, value),
    code,
  );
  return value;
}

function positiveUint64(value, code) {
  ensure(
    typeof value === "string" &&
      regexpTest(POSITIVE_UINT64_PATTERN, value),
    code,
  );
  let parsed;
  try {
    parsed = BigIntConstructor(value);
  } catch {
    fail(code);
  }
  ensure(parsed <= UINT64_MAX, code);
  return value;
}

function canonicalTimestamp(value, code) {
  ensure(typeof value === "string", code);
  let milliseconds;
  let canonical;
  try {
    milliseconds = callIntrinsic(dateParseIntrinsic, DateConstructor, [
      value,
    ]);
    canonical = callIntrinsic(
      dateToISOStringIntrinsic,
      new DateConstructor(milliseconds),
      [],
    );
  } catch {
    fail(code);
  }
  ensure(numberIsFinite(milliseconds) && canonical === value, code);
  return value;
}

function normalizeSignal(value, code) {
  if (value === null) return null;
  ensure(
    typeof value === "object" &&
      !isProxyValue(value) &&
      objectGetPrototypeOf(value) === abortSignalPrototype,
    code,
  );
  try {
    ensure(
      typeof callIntrinsic(abortSignalAbortedGetter, value, []) ===
        "boolean",
      code,
    );
  } catch {
    fail(code);
  }
  return value;
}

function signalIsAborted(signal, code) {
  if (signal === null) return false;
  let aborted;
  try {
    aborted = callIntrinsic(abortSignalAbortedGetter, signal, []);
  } catch {
    fail(code);
  }
  ensure(typeof aborted === "boolean", code);
  return aborted;
}

function normalizeRequest(value, code) {
  const request = exactDataObject(value, RUN_BATCH_KEYS, code);
  const afterSessionId =
    request.afterSessionId === null
      ? null
      : canonicalSessionId(request.afterSessionId, code);
  ensure(
    numberIsSafeInteger(request.limit) &&
      request.limit >= 1 &&
      request.limit <= MAX_BATCH_SIZE,
    code,
  );
  return exactFrozenRecord({
    afterSessionId,
    limit: request.limit,
    signal: normalizeSignal(request.signal, code),
  });
}

function frozenDenseCandidateArray(value, limit, code) {
  ensure(
    value !== null &&
      typeof value === "object" &&
      !isProxyValue(value) &&
      arrayIsArray(value) &&
      objectGetPrototypeOf(value) === arrayPrototype &&
      objectIsFrozen(value) &&
      value.length <= limit,
    code,
  );
  let keys;
  try {
    keys = reflectOwnKeys(value);
  } catch {
    fail(code);
  }
  ensure(keys.length === value.length + 1, code);
  const lengthDescriptor = objectGetOwnPropertyDescriptor(value, "length");
  ensure(
    lengthDescriptor !== undefined &&
      objectHasOwn(lengthDescriptor, "value") &&
      lengthDescriptor.value === value.length &&
      lengthDescriptor.enumerable === false,
    code,
  );
  for (let index = 0; index < value.length; index += 1) {
    const key = `${index}`;
    const descriptor = objectGetOwnPropertyDescriptor(value, key);
    ensure(
      descriptor !== undefined &&
        descriptor.enumerable === true &&
        objectHasOwn(descriptor, "value"),
      code,
    );
  }
  return value;
}

function normalizeCandidate(value, code) {
  const candidate = exactDataObject(value, CANDIDATE_KEYS, code);
  ensure(objectIsFrozen(value), code);
  const checkpoint = exactDataObject(
    candidate.checkpoint,
    CHECKPOINT_KEYS,
    code,
  );
  const request = exactDataObject(
    candidate.request,
    MUTATION_REQUEST_KEYS,
    code,
  );
  const target = exactDataObject(
    request.target,
    MUTATION_TARGET_KEYS,
    code,
  );
  ensure(
    objectIsFrozen(candidate.checkpoint) &&
      objectIsFrozen(candidate.request) &&
      objectIsFrozen(request.target) &&
      checkpoint.contractVersion === 1 &&
      request.contractVersion === 1 &&
      checkpoint.checkpointClass === "clean" &&
      request.operation === "checkpoint" &&
      target.kind === "checkpoint" &&
      checkpoint.backendId === request.backendId &&
      checkpoint.storageId === request.storageId &&
      checkpoint.sessionId === request.sessionId &&
      checkpoint.sourceFencingEpoch === request.fencingEpoch &&
      checkpoint.checkpointId === target.checkpointId &&
      checkpoint.artifactId === target.artifactId &&
      checkpoint.codexSessionId === checkpoint.codexThreadId,
    code,
  );
  ensure(
    candidate.state === "prepared" ||
      candidate.state === "starting" ||
      candidate.state === "uncertain",
    code,
  );
  opaqueId(checkpoint.artifactId, code);
  opaqueId(checkpoint.backendId, code);
  opaqueId(checkpoint.checkpointId, code);
  opaqueId(checkpoint.storageId, code);
  canonicalSessionId(checkpoint.codexSessionId, code);
  canonicalSessionId(checkpoint.codexThreadId, code);
  canonicalSessionId(checkpoint.sessionId, code);
  canonicalTimestamp(checkpoint.createdAt, code);
  ensure(
    typeof checkpoint.imageDigest === "string" &&
      regexpTest(OCI_DIGEST_PATTERN, checkpoint.imageDigest),
    code,
  );
  positiveUint64(checkpoint.sourceFencingEpoch, code);
  opaqueId(request.backendId, code);
  opaqueId(request.holderId, code);
  opaqueId(request.leaseId, code);
  opaqueId(request.operationId, code);
  opaqueId(request.storageId, code);
  canonicalSessionId(request.sessionId, code);
  positiveUint64(request.fencingEpoch, code);
  opaqueId(target.artifactId, code);
  opaqueId(target.checkpointId, code);
  return exactFrozenRecord({
    callbackInput: exactFrozenRecord({
      checkpoint: candidate.checkpoint,
      request: candidate.request,
    }),
    operationId: request.operationId,
    route:
      candidate.state === "prepared"
        ? "prepared-publish"
        : "committed-verify",
    sessionId: request.sessionId,
  });
}

function normalizePage(value, { afterSessionId, limit }, code) {
  const page = exactDataObject(value, PAGE_KEYS, code);
  ensure(objectIsFrozen(value), code);
  const candidates = frozenDenseCandidateArray(
    page.candidates,
    limit,
    code,
  );
  const normalized = [];
  let previousSessionId = afterSessionId;
  for (let index = 0; index < candidates.length; index += 1) {
    const descriptor = objectGetOwnPropertyDescriptor(
      candidates,
      `${index}`,
    );
    const candidate = normalizeCandidate(descriptor.value, code);
    ensure(
      previousSessionId === null ||
        candidate.sessionId > previousSessionId,
      code,
    );
    objectDefineProperty(normalized, `${index}`, {
      configurable: true,
      enumerable: true,
      value: candidate,
      writable: true,
    });
    previousSessionId = candidate.sessionId;
  }
  objectFreeze(normalized);

  const nextAfterSessionId =
    page.nextAfterSessionId === null
      ? null
      : canonicalSessionId(page.nextAfterSessionId, code);
  if (nextAfterSessionId !== null) {
    ensure(
      candidates.length === limit &&
        previousSessionId === nextAfterSessionId,
      code,
    );
  }
  return exactFrozenRecord({
    candidates: normalized,
    nextAfterSessionId,
  });
}

function appendFrozenResult(results, result) {
  objectDefineProperty(results, `${results.length}`, {
    configurable: true,
    enumerable: true,
    value: result,
    writable: true,
  });
}

function batchResult(nextAfterSessionId, results, status) {
  return exactFrozenRecord({
    nextAfterSessionId,
    results: objectFreeze(results),
    status,
  });
}

async function callListCandidates(callback, input, code) {
  try {
    return await callIntrinsic(callback, undefined, [input]);
  } catch {
    fail(code);
  }
}

async function reconcileCandidate(callback, candidate) {
  try {
    const pending = callIntrinsic(callback, undefined, [candidate]);
    if (isGeneratorObjectValue(pending)) return "pending";
    const result = await pending;
    if (isGeneratorObjectValue(result)) return "pending";
    return "reconciled";
  } catch {
    return "pending";
  }
}

export class PostgresCheckpointRecoveryServiceError extends ErrorConstructor {
  constructor(code) {
    if (typeof code !== "string" || !objectHasOwn(ERROR_MESSAGES, code)) {
      throw new TypeErrorConstructor(
        "Unsupported PostgreSQL checkpoint recovery service error",
      );
    }
    const message = ERROR_MESSAGES[code];
    super(message);
    objectDefineProperty(this, "name", {
      enumerable: false,
      value: "PostgresCheckpointRecoveryServiceError",
    });
    objectDefineProperty(this, "code", {
      enumerable: true,
      value: code,
    });
    objectDefineProperty(this, "retryable", {
      enumerable: true,
      value: false,
    });
    objectDefineProperty(this, "stack", {
      enumerable: false,
      value: `PostgresCheckpointRecoveryServiceError: ${message}`,
    });
    objectFreeze(this);
  }
}

export function createPostgresCheckpointRecoveryService(...args) {
  const optionCode =
    "invalid_postgres_checkpoint_recovery_service_options";
  ensure(args.length === 1, optionCode);
  const options = exactDataObjectVariant(
    args[0],
    [LEGACY_OPTION_KEYS, OPTION_KEYS],
    optionCode,
  );
  const listCandidates = normalizeCallback(
    options.listCandidates,
    optionCode,
  );
  const reconcileCheckpointCapture = normalizeCallback(
    options.reconcileCheckpointCapture,
    optionCode,
  );
  const resumePreparedCheckpointCapture = objectHasOwn(
    options,
    "resumePreparedCheckpointCapture",
  )
    ? normalizeCallback(
        options.resumePreparedCheckpointCapture,
        optionCode,
      )
    : null;

  const requestCode =
    "invalid_postgres_checkpoint_recovery_service_request";
  const outcomeCode =
    "postgres_checkpoint_recovery_service_outcome_uncertain";
  let inFlight = false;

  const runBatch = async function runBatch(...runArgs) {
    ensure(runArgs.length === 1, requestCode);
    const request = normalizeRequest(runArgs[0], requestCode);
    ensure(!inFlight, outcomeCode);
    inFlight = true;
    try {
      const results = [];
      let settledCursor = request.afterSessionId;
      if (signalIsAborted(request.signal, requestCode)) {
        return batchResult(settledCursor, results, "aborted");
      }

      const rawPage = await callListCandidates(
        listCandidates,
        exactFrozenRecord({
          afterSessionId: request.afterSessionId,
          limit: request.limit,
        }),
        outcomeCode,
      );
      const page = normalizePage(rawPage, request, outcomeCode);
      if (signalIsAborted(request.signal, requestCode)) {
        return batchResult(settledCursor, results, "aborted");
      }

      for (let index = 0; index < page.candidates.length; index += 1) {
        if (signalIsAborted(request.signal, requestCode)) {
          return batchResult(settledCursor, results, "aborted");
        }
        const candidate = page.candidates[index];
        const callback =
          candidate.route === "prepared-publish"
            ? resumePreparedCheckpointCapture
            : reconcileCheckpointCapture;
        const status =
          callback === null
            ? "pending"
            : await reconcileCandidate(
                callback,
                candidate.callbackInput,
              );
        appendFrozenResult(
          results,
          exactFrozenRecord({
            operationId: candidate.operationId,
            sessionId: candidate.sessionId,
            status,
          }),
        );
        settledCursor = candidate.sessionId;
        if (signalIsAborted(request.signal, requestCode)) {
          return batchResult(settledCursor, results, "aborted");
        }
      }

      return batchResult(
        page.nextAfterSessionId,
        results,
        page.nextAfterSessionId === null
          ? "sweep-complete"
          : "limit-reached",
      );
    } finally {
      inFlight = false;
    }
  };

  return exactFrozenRecord({ runBatch });
}
