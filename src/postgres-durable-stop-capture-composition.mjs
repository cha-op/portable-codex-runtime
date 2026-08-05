import { types as utilTypes } from "node:util";

import {
  capturePreparedCleanCheckpoint,
  prepareCleanCheckpointCapture,
} from "./session-snapshot-core.mjs";
import {
  derivePostgresLogicalWriterStopOperationId,
} from "./postgres-logical-writer-launcher.mjs";

const PromiseConstructor = Promise;
const promiseSpeciesSymbol = Symbol.species;
const promiseThenIntrinsic = Promise.prototype.then;
const reflectApply = Reflect.apply;
const regexpExecIntrinsic = RegExp.prototype.exec;
const {
  isGeneratorObject: isGeneratorObjectValue,
  isPromise: isPromiseValue,
} = utilTypes;

const ERROR_MESSAGES = Object.freeze({
  invalid_postgres_durable_stop_capture_composition_options:
    "PostgreSQL durable stop and capture composition options are invalid",
  invalid_postgres_durable_stop_capture_composition_request:
    "PostgreSQL durable stop and capture composition request is invalid",
  postgres_durable_stop_capture_composition_outcome_uncertain:
    "PostgreSQL durable stop and capture composition outcome is uncertain",
  postgres_durable_stop_capture_retirement_outcome_uncertain:
    "PostgreSQL durable stop and capture retirement outcome is uncertain",
});

const EVIDENCE_KEYS = Object.freeze([
  "contractVersion",
  "launchAttemptId",
  "processIncarnationId",
  "proofId",
  "status",
  "supervisorId",
  "writerIncarnationId",
]);
const RESOLUTION_KEYS = Object.freeze([
  "canonicalLeaseAtRegistration",
  "processIncarnationId",
  "stopOperationId",
  "writer",
  "writerIncarnationId",
]);
const STOP_RESULT_KEYS = Object.freeze([
  "capability",
  "evidence",
  "resolution",
  "stop",
]);
const STOP_RECEIPT_KEYS = Object.freeze([
  "status",
  "session",
  "operation",
  "reservation",
  "finalized",
  "launch",
  "stop",
]);
const STOP_RECORD_KEYS = Object.freeze([
  "contractVersion",
  "launchAttemptId",
  "request",
  "result",
  "state",
  "stopOperationId",
]);
const STOP_TERMINAL_RESULT_KEYS = Object.freeze([
  "evidence",
  "outcome",
  "resultVersion",
]);
const STOP_REQUEST_V1_KEYS = Object.freeze(["contractVersion", "launch"]);
const STOP_REQUEST_V2_KEYS = Object.freeze([
  "contractVersion",
  "dispatchClaimSha256",
  "launch",
]);
const SHA256_PATTERN = /^[0-9a-f]{64}$/u;
const CAPTURE_OPTION_KEYS = Object.freeze([
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
    if (!Object.hasOwn(ERROR_MESSAGES, code)) {
      throw new TypeError(
        "Unsupported PostgreSQL durable stop and capture composition error",
      );
    }
    super(ERROR_MESSAGES[code]);
    this.name = "PostgresDurableStopCaptureCompositionError";
    this.code = code;
    this.retryable = false;
    Object.freeze(this);
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

function exactDataObject(value, keys, code, { frozen = false } = {}) {
  ensure(
    value !== null &&
      typeof value === "object" &&
      !utilTypes.isProxy(value) &&
      !Array.isArray(value),
    code,
  );
  let prototype;
  let actualKeys;
  try {
    prototype = Object.getPrototypeOf(value);
    actualKeys = Reflect.ownKeys(value);
  } catch {
    fail(code);
  }
  ensure(
    (prototype === Object.prototype || prototype === null) &&
      (!frozen || Object.isFrozen(value)) &&
      actualKeys.length === keys.length &&
      actualKeys.every(
        (key) => typeof key === "string" && keys.includes(key),
      ),
    code,
  );
  const normalized = Object.create(null);
  for (const key of actualKeys) {
    let descriptor;
    try {
      descriptor = Object.getOwnPropertyDescriptor(value, key);
    } catch {
      fail(code);
    }
    ensure(
      descriptor?.enumerable === true && Object.hasOwn(descriptor, "value"),
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
      !utilTypes.isProxy(value) &&
      !Array.isArray(value) &&
      Object.isFrozen(value),
    code,
  );
  return value;
}

function frozenDataProjection(value, keys, code) {
  const record = frozenRecord(value, code);
  const normalized = Object.create(null);
  for (const key of keys) {
    let descriptor;
    try {
      descriptor = Object.getOwnPropertyDescriptor(record, key);
    } catch {
      fail(code);
    }
    ensure(
      descriptor?.enumerable === true && Object.hasOwn(descriptor, "value"),
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
    prototype = Object.getPrototypeOf(handle);
    keys = Reflect.ownKeys(handle);
  } catch {
    fail(code);
  }
  ensure(prototype === null && keys.length === 0, code);
  return handle;
}

function opaqueId(value, code) {
  ensure(
    typeof value === "string" &&
      /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u.test(value),
    code,
  );
  return value;
}

function isSafePromiseConstructor(value) {
  if (value === PromiseConstructor) return true;
  if (
    value === null ||
    typeof value !== "object" ||
    utilTypes.isProxy(value) ||
    !Object.isFrozen(value)
  ) {
    return false;
  }
  let prototype;
  let keys;
  try {
    prototype = Object.getPrototypeOf(value);
    keys = Reflect.ownKeys(value);
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
  const descriptor = Object.getOwnPropertyDescriptor(
    value,
    promiseSpeciesSymbol,
  );
  return (
    descriptor?.configurable === false &&
    descriptor.enumerable === false &&
    Object.hasOwn(descriptor, "value") &&
    descriptor.value === PromiseConstructor &&
    descriptor.writable === false
  );
}

function isSafeNativePromise(value) {
  if (
    !isPromiseValue(value) ||
    utilTypes.isProxy(value) ||
    isGeneratorObjectValue(value)
  ) {
    return false;
  }
  let current = value;
  while (current !== null) {
    if (utilTypes.isProxy(current)) return false;
    let descriptor;
    try {
      descriptor = Object.getOwnPropertyDescriptor(current, "constructor");
    } catch {
      return false;
    }
    if (descriptor !== undefined) {
      return (
        Object.hasOwn(descriptor, "value") &&
        isSafePromiseConstructor(descriptor.value)
      );
    }
    try {
      current = Object.getPrototypeOf(current);
    } catch {
      return false;
    }
  }
  return false;
}

function sameEvidence(left, right) {
  return EVIDENCE_KEYS.every((key) => left[key] === right[key]);
}

function sameLaunchIdentity(left, right) {
  return [
    "launchAttemptId",
    "processIncarnationId",
    "supervisorId",
    "writerIncarnationId",
  ].every((key) => left[key] === right[key]);
}

function sameFrozenData(left, right, state = { nodes: 0 }, depth = 0) {
  if (Object.is(left, right)) return true;
  if (
    left === null ||
    right === null ||
    typeof left !== "object" ||
    typeof right !== "object" ||
    utilTypes.isProxy(left) ||
    utilTypes.isProxy(right) ||
    Array.isArray(left) ||
    Array.isArray(right) ||
    !Object.isFrozen(left) ||
    !Object.isFrozen(right) ||
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
    leftPrototype = Object.getPrototypeOf(left);
    rightPrototype = Object.getPrototypeOf(right);
    leftKeys = Reflect.ownKeys(left);
    rightKeys = Reflect.ownKeys(right);
  } catch {
    return false;
  }
  if (
    leftPrototype !== rightPrototype ||
    (leftPrototype !== Object.prototype && leftPrototype !== null) ||
    leftKeys.length !== rightKeys.length ||
    leftKeys.some((key) => typeof key !== "string" || !rightKeys.includes(key))
  ) {
    return false;
  }
  state.nodes += 1;
  for (const key of leftKeys) {
    let leftDescriptor;
    let rightDescriptor;
    try {
      leftDescriptor = Object.getOwnPropertyDescriptor(left, key);
      rightDescriptor = Object.getOwnPropertyDescriptor(right, key);
    } catch {
      return false;
    }
    if (
      leftDescriptor?.enumerable !== true ||
      rightDescriptor?.enumerable !== true ||
      !Object.hasOwn(leftDescriptor, "value") ||
      !Object.hasOwn(rightDescriptor, "value") ||
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
  for (const key of [
    "launchAttemptId",
    "processIncarnationId",
    "proofId",
    "supervisorId",
    "writerIncarnationId",
  ]) {
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
    contractVersionDescriptor = Object.getOwnPropertyDescriptor(
      record,
      "contractVersion",
    );
  } catch {
    fail(code);
  }
  ensure(
    contractVersionDescriptor?.enumerable === true &&
      Object.hasOwn(contractVersionDescriptor, "value"),
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
  return Object.freeze({
    capability,
    evidence: stopped.evidence,
    resolution: stopped.resolution,
    stop: stopped.stop,
  });
}

const absorbRetirementRejection = Object.freeze(
  function absorbRetirementRejection() {},
);

function drainRetirementPromise(value) {
  if (!isSafeNativePromise(value)) return;
  try {
    reflectApply(promiseThenIntrinsic, value, [
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
      !utilTypes.isProxy(value) &&
      Object.isFrozen(value),
    code,
  );
  let descriptor;
  try {
    descriptor = Object.getOwnPropertyDescriptor(value, name);
  } catch {
    fail(code);
  }
  ensure(
    descriptor?.enumerable === true &&
      Object.hasOwn(descriptor, "value") &&
      typeof descriptor.value === "function" &&
      !utilTypes.isProxy(descriptor.value),
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
    const stopInput = Object.freeze({
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
    ensure(isSafeNativePromise(pendingStop), outcomeCode);

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

  Object.freeze(runCapture);
  return Object.freeze({ runCapture });
}

Object.freeze(PostgresDurableStopCaptureCompositionError.prototype);
Object.freeze(PostgresDurableStopCaptureCompositionError);
