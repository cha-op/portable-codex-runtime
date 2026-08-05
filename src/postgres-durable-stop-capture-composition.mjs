import { types as utilTypes } from "node:util";

import {
  capturePreparedCleanCheckpoint,
  prepareCleanCheckpointCapture,
} from "./session-snapshot-core.mjs";
import {
  derivePostgresLogicalWriterStopOperationId,
} from "./postgres-logical-writer-launcher.mjs";

// The validated receipt and retirement resolution must remain bound to the
// stopped writer even when the stop collaborator mutates shared intrinsics.
const arrayEveryIntrinsic = Array.prototype.every;
const arrayIncludesIntrinsic = Array.prototype.includes;
const arrayIsArray = Array.isArray;
const arraySomeIntrinsic = Array.prototype.some;
const objectCreate = Object.create;
const objectDefineProperty = Object.defineProperty;
const objectFreeze = Object.freeze;
const objectGetOwnPropertyDescriptor = Object.getOwnPropertyDescriptor;
const objectGetPrototypeOf = Object.getPrototypeOf;
const objectHasOwn = Object.hasOwn;
const objectIs = Object.is;
const objectIsFrozen = Object.isFrozen;
const objectPrototype = Object.prototype;
const PromiseConstructor = Promise;
const promisePrototype = Promise.prototype;
const promiseSpeciesSymbol = Symbol.species;
const promiseThenIntrinsic = promisePrototype.then;
const reflectApply = Reflect.apply;
const reflectOwnKeys = Reflect.ownKeys;
const regexpExecIntrinsic = RegExp.prototype.exec;
const {
  isGeneratorObject: isGeneratorObjectValue,
  isPromise: isPromiseValue,
  isProxy: isProxyValue,
} = utilTypes;

const ERROR_MESSAGES = objectFreeze({
  invalid_postgres_durable_stop_capture_composition_options:
    "PostgreSQL durable stop and capture composition options are invalid",
  invalid_postgres_durable_stop_capture_composition_request:
    "PostgreSQL durable stop and capture composition request is invalid",
  postgres_durable_stop_capture_composition_outcome_uncertain:
    "PostgreSQL durable stop and capture composition outcome is uncertain",
  postgres_durable_stop_capture_retirement_outcome_uncertain:
    "PostgreSQL durable stop and capture retirement outcome is uncertain",
});

const EVIDENCE_KEYS = objectFreeze([
  "contractVersion",
  "launchAttemptId",
  "processIncarnationId",
  "proofId",
  "status",
  "supervisorId",
  "writerIncarnationId",
]);
const RESOLUTION_KEYS = objectFreeze([
  "canonicalLeaseAtRegistration",
  "processIncarnationId",
  "stopOperationId",
  "writer",
  "writerIncarnationId",
]);
const STOP_RESULT_KEYS = objectFreeze([
  "capability",
  "evidence",
  "resolution",
  "stop",
]);
const STOP_RECEIPT_KEYS = objectFreeze([
  "status",
  "session",
  "operation",
  "reservation",
  "finalized",
  "launch",
  "stop",
]);
const STOP_RECORD_KEYS = objectFreeze([
  "contractVersion",
  "launchAttemptId",
  "request",
  "result",
  "state",
  "stopOperationId",
]);
const STOP_TERMINAL_RESULT_KEYS = objectFreeze([
  "evidence",
  "outcome",
  "resultVersion",
]);
const STOP_REQUEST_V1_KEYS = objectFreeze(["contractVersion", "launch"]);
const STOP_REQUEST_V2_KEYS = objectFreeze([
  "contractVersion",
  "dispatchClaimSha256",
  "launch",
]);
const SHA256_PATTERN = /^[0-9a-f]{64}$/u;
const CAPTURE_OPTION_KEYS = objectFreeze([
  "attachment",
  "backend",
  "canonicalLease",
  "checkpointClass",
  "createdAt",
  "manifest",
  "now",
  "request",
  "storageRef",
]);

export class PostgresDurableStopCaptureCompositionError extends Error {
  constructor(code) {
    if (!objectHasOwn(ERROR_MESSAGES, code)) {
      throw new TypeError(
        "Unsupported PostgreSQL durable stop and capture composition error",
      );
    }
    super(ERROR_MESSAGES[code]);
    this.name = "PostgresDurableStopCaptureCompositionError";
    this.code = code;
    this.retryable = false;
    objectFreeze(this);
  }
}

function fail(code) {
  throw new PostgresDurableStopCaptureCompositionError(code);
}

function ensure(condition, code) {
  if (!condition) fail(code);
}

function regexpTest(pattern, value) {
  return reflectApply(regexpExecIntrinsic, pattern, [value]) !== null;
}

function arrayEvery(value, callback) {
  return reflectApply(arrayEveryIntrinsic, value, [callback]);
}

function arrayIncludes(value, candidate) {
  return reflectApply(arrayIncludesIntrinsic, value, [candidate]);
}

function arraySome(value, callback) {
  return reflectApply(arraySomeIntrinsic, value, [callback]);
}

function exactDataObject(value, keys, code, { frozen = false } = {}) {
  ensure(
    value !== null &&
      typeof value === "object" &&
      !isProxyValue(value) &&
      !arrayIsArray(value),
    code,
  );
  let prototype;
  let actualKeys;
  try {
    prototype = objectGetPrototypeOf(value);
    actualKeys = reflectOwnKeys(value);
  } catch {
    fail(code);
  }
  ensure(
    (prototype === objectPrototype || prototype === null) &&
      (!frozen || objectIsFrozen(value)) &&
      actualKeys.length === keys.length &&
      arrayEvery(
        actualKeys,
        (key) => typeof key === "string" && arrayIncludes(keys, key),
      ),
    code,
  );
  const normalized = objectCreate(null);
  for (let index = 0; index < actualKeys.length; index += 1) {
    const key = actualKeys[index];
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

function frozenRecord(value, code) {
  ensure(
    value !== null &&
      typeof value === "object" &&
      !isProxyValue(value) &&
      !arrayIsArray(value) &&
      objectIsFrozen(value),
    code,
  );
  return value;
}

function frozenDataProjection(value, keys, code) {
  const record = frozenRecord(value, code);
  const normalized = objectCreate(null);
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
    let descriptor;
    try {
      descriptor = objectGetOwnPropertyDescriptor(record, key);
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

function opaqueHandle(value, code) {
  const handle = frozenRecord(value, code);
  let prototype;
  let keys;
  try {
    prototype = objectGetPrototypeOf(handle);
    keys = reflectOwnKeys(handle);
  } catch {
    fail(code);
  }
  ensure(prototype === null && keys.length === 0, code);
  return handle;
}

function opaqueId(value, code) {
  ensure(
    typeof value === "string" &&
      regexpTest(/^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u, value),
    code,
  );
  return value;
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
    const normalized = reflectApply(promiseThenIntrinsic, value, [
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

function sameEvidence(left, right) {
  return arrayEvery(EVIDENCE_KEYS, (key) => left[key] === right[key]);
}

function sameLaunchIdentity(left, right) {
  return arrayEvery(
    [
      "launchAttemptId",
      "processIncarnationId",
      "supervisorId",
      "writerIncarnationId",
    ],
    (key) => left[key] === right[key],
  );
}

function sameFrozenData(left, right, state = { nodes: 0 }, depth = 0) {
  if (objectIs(left, right)) return true;
  if (
    left === null ||
    right === null ||
    typeof left !== "object" ||
    typeof right !== "object" ||
    isProxyValue(left) ||
    isProxyValue(right) ||
    arrayIsArray(left) ||
    arrayIsArray(right) ||
    !objectIsFrozen(left) ||
    !objectIsFrozen(right) ||
    depth >= 24 ||
    state.nodes >= 1_024
  ) {
    return false;
  }
  let leftPrototype;
  let rightPrototype;
  let leftKeys;
  let rightKeys;
  try {
    leftPrototype = objectGetPrototypeOf(left);
    rightPrototype = objectGetPrototypeOf(right);
    leftKeys = reflectOwnKeys(left);
    rightKeys = reflectOwnKeys(right);
  } catch {
    return false;
  }
  if (
    leftPrototype !== rightPrototype ||
    (leftPrototype !== objectPrototype && leftPrototype !== null) ||
    leftKeys.length !== rightKeys.length ||
    arraySome(
      leftKeys,
      (key) => typeof key !== "string" || !arrayIncludes(rightKeys, key),
    )
  ) {
    return false;
  }
  state.nodes += 1;
  for (let index = 0; index < leftKeys.length; index += 1) {
    const key = leftKeys[index];
    let leftDescriptor;
    let rightDescriptor;
    try {
      leftDescriptor = objectGetOwnPropertyDescriptor(left, key);
      rightDescriptor = objectGetOwnPropertyDescriptor(right, key);
    } catch {
      return false;
    }
    if (
      leftDescriptor?.enumerable !== true ||
      rightDescriptor?.enumerable !== true ||
      !objectHasOwn(leftDescriptor, "value") ||
      !objectHasOwn(rightDescriptor, "value") ||
      !sameFrozenData(
        leftDescriptor.value,
        rightDescriptor.value,
        state,
        depth + 1,
      )
    ) {
      return false;
    }
  }
  return true;
}

function normalizeEvidence(value, code) {
  const evidence = exactDataObject(value, EVIDENCE_KEYS, code, {
    frozen: true,
  });
  ensure(
    evidence.contractVersion === 1 &&
      evidence.status === "complete-stopped",
    code,
  );
  const idKeys = [
    "launchAttemptId",
    "processIncarnationId",
    "proofId",
    "supervisorId",
    "writerIncarnationId",
  ];
  for (let index = 0; index < idKeys.length; index += 1) {
    const key = idKeys[index];
    opaqueId(evidence[key], code);
  }
  return evidence;
}

function normalizeResolution(value, preparedCapture, code) {
  const resolution = exactDataObject(value, RESOLUTION_KEYS, code, {
    frozen: true,
  });
  const lease = frozenDataProjection(
    resolution.canonicalLeaseAtRegistration,
    ["fencingEpoch", "holderId", "leaseId", "sessionId"],
    code,
  );
  const attachment = preparedCapture.attachment;
  ensure(
    lease.sessionId === attachment.sessionId &&
      lease.leaseId === attachment.leaseId &&
      lease.holderId === attachment.holderId &&
      lease.fencingEpoch === attachment.fencingEpoch,
    code,
  );
  opaqueId(resolution.processIncarnationId, code);
  opaqueId(resolution.stopOperationId, code);
  opaqueId(resolution.writerIncarnationId, code);
  opaqueHandle(resolution.writer, code);
  return resolution;
}

function normalizeStopRequest(value, code) {
  const record = frozenRecord(value, code);
  let contractVersionDescriptor;
  try {
    contractVersionDescriptor = objectGetOwnPropertyDescriptor(
      record,
      "contractVersion",
    );
  } catch {
    fail(code);
  }
  ensure(
    contractVersionDescriptor?.enumerable === true &&
      objectHasOwn(contractVersionDescriptor, "value"),
    code,
  );
  const contractVersion = contractVersionDescriptor.value;
  ensure(contractVersion === 1 || contractVersion === 2, code);
  const request = exactDataObject(
    record,
    contractVersion === 1 ? STOP_REQUEST_V1_KEYS : STOP_REQUEST_V2_KEYS,
    code,
    { frozen: true },
  );
  if (contractVersion === 2) {
    ensure(
      typeof request.dispatchClaimSha256 === "string" &&
        regexpTest(SHA256_PATTERN, request.dispatchClaimSha256),
      code,
    );
  }
  return request;
}

function normalizeStopReceipt(value, evidence, resolution, preparedCapture, code) {
  const receipt = exactDataObject(value, STOP_RECEIPT_KEYS, code, {
    frozen: true,
  });
  ensure(
    receipt.status === "committed" &&
      typeof receipt.finalized === "boolean" &&
      receipt.launch === null,
    code,
  );
  const operation = frozenDataProjection(
    receipt.operation,
    ["operationId", "request", "result", "sessionId", "state"],
    code,
  );
  const reservation = frozenDataProjection(
    receipt.reservation,
    ["operationId", "sessionId", "state"],
    code,
  );
  const session = frozenDataProjection(
    receipt.session,
    ["document", "sessionId"],
    code,
  );
  const sessionDocument = frozenDataProjection(
    session.document,
    ["launch"],
    code,
  );
  ensure(
    operation.operationId === resolution.stopOperationId &&
      operation.sessionId === preparedCapture.attachment.sessionId &&
      operation.state === "committed" &&
      reservation.operationId === resolution.stopOperationId &&
      reservation.sessionId === preparedCapture.attachment.sessionId &&
      reservation.state === "released" &&
      session.sessionId === preparedCapture.attachment.sessionId &&
      sessionDocument.launch === null,
    code,
  );

  const stop = exactDataObject(receipt.stop, STOP_RECORD_KEYS, code, {
    frozen: true,
  });
  ensure(
    stop.state === "committed" &&
      stop.stopOperationId === resolution.stopOperationId &&
      stop.launchAttemptId === evidence.launchAttemptId,
    code,
  );
  const terminal = exactDataObject(
    stop.result,
    STOP_TERMINAL_RESULT_KEYS,
    code,
    { frozen: true },
  );
  const terminalEvidence = normalizeEvidence(terminal.evidence, code);
  ensure(
    terminal.outcome === "writer-launch-stopped" &&
      terminal.resultVersion === 1 &&
      sameEvidence(terminalEvidence, evidence),
    code,
  );
  const operationResult = exactDataObject(
    operation.result,
    STOP_TERMINAL_RESULT_KEYS,
    code,
    { frozen: true },
  );
  const operationEvidence = normalizeEvidence(operationResult.evidence, code);
  ensure(
    operationResult.outcome === terminal.outcome &&
      operationResult.resultVersion === terminal.resultVersion &&
      sameEvidence(operationEvidence, evidence),
    code,
  );
  const request = normalizeStopRequest(stop.request, code);
  const operationRequest = normalizeStopRequest(operation.request, code);
  const launch = frozenDataProjection(
    request.launch,
    [
      "attachmentId",
      "contractVersion",
      "fencingEpoch",
      "launchAttemptId",
      "leaseId",
      "processIncarnationId",
      "supervisorId",
      "writerIncarnationId",
    ],
    code,
  );
  const operationLaunch = frozenDataProjection(
    operationRequest.launch,
    [
      "attachmentId",
      "contractVersion",
      "fencingEpoch",
      "launchAttemptId",
      "leaseId",
      "processIncarnationId",
      "supervisorId",
      "writerIncarnationId",
    ],
    code,
  );
  ensure(
    stop.contractVersion === request.contractVersion &&
      operationRequest.contractVersion === request.contractVersion &&
      sameFrozenData(stop.request, operation.request) &&
      sameLaunchIdentity(operationLaunch, launch) &&
      launch.contractVersion === 1 &&
      launch.attachmentId === preparedCapture.attachment.attachmentId &&
      launch.leaseId === preparedCapture.attachment.leaseId &&
      launch.fencingEpoch === preparedCapture.attachment.fencingEpoch &&
      launch.launchAttemptId === evidence.launchAttemptId &&
      launch.processIncarnationId === evidence.processIncarnationId &&
      launch.writerIncarnationId === evidence.writerIncarnationId &&
      launch.supervisorId === evidence.supervisorId,
    code,
  );
  return receipt;
}

function normalizeStoppedCapture(value, preparedCapture, code) {
  const stopped = exactDataObject(value, STOP_RESULT_KEYS, code, {
    frozen: true,
  });
  const capability = opaqueHandle(stopped.capability, code);
  const evidence = normalizeEvidence(stopped.evidence, code);
  const resolution = normalizeResolution(stopped.resolution, preparedCapture, code);
  let expectedStopOperationId;
  try {
    expectedStopOperationId = derivePostgresLogicalWriterStopOperationId({
      attachment: preparedCapture.attachment,
      checkpoint: preparedCapture.checkpoint,
      launchAttemptId: evidence.launchAttemptId,
      request: preparedCapture.request,
    });
  } catch {
    fail(code);
  }
  ensure(
    evidence.processIncarnationId === resolution.processIncarnationId &&
      evidence.writerIncarnationId === resolution.writerIncarnationId &&
      evidence.proofId === expectedStopOperationId &&
      resolution.stopOperationId === expectedStopOperationId,
    code,
  );
  normalizeStopReceipt(
    stopped.stop,
    evidence,
    resolution,
    preparedCapture,
    code,
  );
  return objectFreeze({
    capability,
    evidence: stopped.evidence,
    resolution: stopped.resolution,
    stop: stopped.stop,
  });
}

const absorbRetirementRejection = objectFreeze(
  function absorbRetirementRejection() {},
);

function drainRetirementPromise(value) {
  const normalized = normalizeSafeNativePromise(value);
  if (normalized === null) return;
  try {
    reflectApply(promiseThenIntrinsic, normalized, [
      undefined,
      absorbRetirementRejection,
    ]);
  } catch {
    // The caller still receives the fail-closed retirement classification.
  }
}

function collaboratorMethod(value, name, code) {
  ensure(
    value !== null &&
      typeof value === "object" &&
      !isProxyValue(value) &&
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
      !isProxyValue(descriptor.value),
    code,
  );
  return descriptor.value;
}

export function createPostgresDurableStopCaptureComposition(options) {
  const optionCode =
    "invalid_postgres_durable_stop_capture_composition_options";
  const outcomeCode =
    "postgres_durable_stop_capture_composition_outcome_uncertain";
  const requestCode =
    "invalid_postgres_durable_stop_capture_composition_request";
  const retirementCode =
    "postgres_durable_stop_capture_retirement_outcome_uncertain";
  const normalized = exactDataObject(options, ["launcher"], optionCode);
  const launcher = normalized.launcher;
  const stopWriterForCapture = collaboratorMethod(
    launcher,
    "stopWriterForCapture",
    optionCode,
  );
  const retireStoppedWriter = collaboratorMethod(
    launcher,
    "retireStoppedWriter",
    optionCode,
  );

  const runCapture = async function runCapture(optionsValue) {
    const captureOptions = exactDataObject(
      optionsValue,
      CAPTURE_OPTION_KEYS,
      requestCode,
    );
    let preparedCapture;
    try {
      preparedCapture = prepareCleanCheckpointCapture(captureOptions);
    } catch {
      fail(requestCode);
    }
    const stopInput = objectFreeze({
      attachment: preparedCapture.attachment,
      checkpoint: preparedCapture.checkpoint,
      request: preparedCapture.request,
    });

    let pendingStop;
    try {
      pendingStop = reflectApply(stopWriterForCapture, launcher, [stopInput]);
    } catch {
      fail(outcomeCode);
    }
    pendingStop = normalizeSafeNativePromise(pendingStop);
    ensure(pendingStop !== null, outcomeCode);

    let stoppedValue;
    try {
      stoppedValue = await pendingStop;
    } catch {
      fail(outcomeCode);
    }

    let stopped;
    try {
      stopped = normalizeStoppedCapture(
        stoppedValue,
        preparedCapture,
        outcomeCode,
      );
    } catch {
      fail(outcomeCode);
    }

    let result;
    try {
      result = await capturePreparedCleanCheckpoint({
        preparedCapture,
        stoppedWriterEvidence: stopped.capability,
      });
    } catch {
      fail(outcomeCode);
    }

    let retirement;
    try {
      retirement = reflectApply(retireStoppedWriter, launcher, [
        stopped.resolution,
      ]);
    } catch {
      fail(retirementCode);
    }
    if (retirement !== undefined) {
      drainRetirementPromise(retirement);
      fail(retirementCode);
    }
    return result;
  };

  objectFreeze(runCapture);
  return objectFreeze({ runCapture });
}

objectFreeze(PostgresDurableStopCaptureCompositionError.prototype);
objectFreeze(PostgresDurableStopCaptureCompositionError);
