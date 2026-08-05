import { createHash } from "node:crypto";
import { types as utilTypes } from "node:util";

import { PostgresOperationGuard } from "./postgres-operation-guard.mjs";
import {
  RESTORE_ATTACHMENT_ACTIVATION_OPERATION_KIND,
  RESTORE_DESTINATION_GENERATION_DOCUMENT_CONTRACT_VERSION,
  RESTORE_DESTINATION_GENERATION_OPERATION_KIND,
  SESSION_OPERATION_CONFLICT_CLASS,
  WRITER_LAUNCH_ATTEMPT_OPERATION_KIND,
  createRestoreAttachmentActivationOperationRequest,
} from "./postgres-session-authority.mjs";
import {
  assertCheckpointDescriptor,
  assertRestoreAttachmentActivationBackend,
  assertRestoreAttachmentActivationRequest,
  assertRestoreAttachmentActivationResult,
  assertStorageMutationRequest,
} from "./session-storage-contracts.mjs";
import { StoppedDirectoryPublication } from "./stopped-directory-publication.mjs";

const arrayIncludesIntrinsic = Array.prototype.includes;
const arrayIsArray = Array.isArray;
const arrayPrototype = Array.prototype;
const BigIntConstructor = BigInt;
const bigIntToStringIntrinsic = BigInt.prototype.toString;
const createHashIntrinsic = createHash;
const DateConstructor = Date;
const dateParseIntrinsic = Date.parse;
const dateToISOStringIntrinsic = Date.prototype.toISOString;
const ErrorConstructor = Error;
const isGeneratorFunctionValue = utilTypes.isGeneratorFunction;
const isGeneratorObjectValue = utilTypes.isGeneratorObject;
const isPromiseValue = utilTypes.isPromise;
const isProxyValue = utilTypes.isProxy;
const objectCreate = Object.create;
const objectDefineProperty = Object.defineProperty;
const objectFreeze = Object.freeze;
const objectGetOwnPropertyDescriptor = Object.getOwnPropertyDescriptor;
const objectGetPrototypeOf = Object.getPrototypeOf;
const objectHasOwn = Object.hasOwn;
const objectIs = Object.is;
const objectIsFrozen = Object.isFrozen;
const objectPrototype = Object.prototype;
const numberIsFinite = Number.isFinite;
const hashPrototype = objectGetPrototypeOf(createHash("sha256"));
const hashDigestIntrinsic = hashPrototype.digest;
const hashUpdateIntrinsic = hashPrototype.update;
const PromiseConstructor = Promise;
const promisePrototype = Promise.prototype;
const promiseSpeciesSymbol = Symbol.species;
const promiseThenIntrinsic = Promise.prototype.then;
const reflectApply = Reflect.apply;
const reflectOwnKeys = Reflect.ownKeys;
const regexpExecIntrinsic = RegExp.prototype.exec;
const TypeErrorConstructor = TypeError;

const runExclusiveIntrinsic = PostgresOperationGuard.prototype.runExclusive;
const verifyCommittedRestoreDestinationIntrinsic =
  StoppedDirectoryPublication.prototype.verifyCommittedRestoreDestination;

const OPAQUE_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u;
const SHA256_PATTERN = /^[0-9a-f]{64}$/u;
const UINT64_PATTERN = /^(0|[1-9][0-9]{0,19})$/u;
const MAX_DATA_DEPTH = 32;
const MAX_DATA_NODES = 16_384;
const MAX_DATA_ARRAY_ENTRIES = 2_048;
const MAX_DATA_OBJECT_KEYS = 256;
const MAX_DATA_STRING_LENGTH = 1_048_576;
const OPTION_KEYS = objectFreeze([
  "authority",
  "operationGuard",
  "publication",
  "resolveRestoreDestination",
  "storageBackend",
]);
const AUTHORITY_KEYS = objectFreeze([
  "finalizeRestoreAttachmentActivationAndReserveWriterLaunchAttempt",
  "finalizeRestoreDestinationGeneration",
  "markOperationUncertain",
  "readRestoreAttachmentActivation",
  "readRestoreDestinationGeneration",
]);
const DESTINATION_KEYS = objectFreeze([
  "destinationDirectory",
  "destinationOwnedRoot",
]);
const GENERATION_CANDIDATE_KEYS = objectFreeze([
  "checkpoint",
  "generationId",
  "request",
]);
const GENERATION_CANDIDATE_V2_KEYS = objectFreeze([
  "checkpoint",
  "generationId",
  "launchIntent",
  "request",
]);
const ACTIVATION_CANDIDATE_KEYS = objectFreeze([
  "activationOperationId",
  "request",
  "state",
]);
const OPERATION_GUARD_PROBE_KEYS = objectFreeze(["assertHeld"]);
const OPERATION_KEYS = objectFreeze([
  "conflictClass",
  "createdAt",
  "expectedSession",
  "kind",
  "operationId",
  "request",
  "requestSha256",
  "result",
  "retiredAt",
  "revision",
  "sessionId",
  "state",
  "updatedAt",
]);
const SNAPSHOT_KEYS = objectFreeze([
  "createdAt",
  "document",
  "revision",
  "sessionId",
  "updatedAt",
]);
const DOCUMENT_KEYS = objectFreeze([
  "activeOperation",
  "attachment",
  "backendCapabilities",
  "documentVersion",
  "lastOperation",
  "launch",
  "lease",
  "lifecycle",
  "manifest",
  "recovery",
  "storageRef",
  "writerEpoch",
]);
const RESERVATION_KEYS = objectFreeze([
  "conflictClass",
  "createdAt",
  "expectedSessionRevision",
  "expiresAt",
  "kind",
  "operationId",
  "releasedAt",
  "requestSha256",
  "reservationId",
  "sessionId",
  "state",
  "updatedAt",
]);
const GENERATION_KEYS = objectFreeze([
  "binding",
  "checkpointId",
  "claimedAt",
  "committedAt",
  "document",
  "generationId",
  "operationId",
  "sessionId",
  "state",
]);
const GENERATION_DOCUMENT_KEYS = objectFreeze([
  "artifactProof",
  "contractVersion",
  "materialization",
  "result",
]);
const CATALOGUE_KEYS = objectFreeze([
  "captureAttemptId",
  "checkpointId",
  "committedAt",
  "document",
  "sessionId",
]);
const GENERATION_READ_RECEIPT_KEYS = objectFreeze([
  "catalogue",
  "generation",
  "operation",
  "reservation",
  "session",
  "status",
]);
const GENERATION_FINALIZE_RECEIPT_KEYS = objectFreeze([
  "catalogue",
  "finalized",
  "generation",
  "operation",
  "reservation",
  "session",
  "status",
]);
const ACTIVATION_READ_RECEIPT_KEYS = objectFreeze([
  "activationRequest",
  "generation",
  "operation",
  "reservation",
  "session",
  "status",
]);
const ACTIVATION_HANDOFF_RECEIPT_KEYS = objectFreeze([
  "activation",
  "generation",
  "launch",
  "session",
  "status",
]);
const ACTIVATION_RECEIPT_KEYS = objectFreeze([
  "finalized",
  "operation",
  "reservation",
]);
const LAUNCH_RECEIPT_KEYS = objectFreeze([
  "attempt",
  "operation",
  "reservation",
]);
const LAUNCH_ATTEMPT_KEYS = objectFreeze([
  "contractVersion",
  "launchAttemptId",
  "request",
  "result",
  "state",
]);
const ACTIVATION_TERMINAL_RESULT_KEYS = objectFreeze([
  "activationRequest",
  "activationResult",
  "outcome",
  "resultVersion",
]);
const GENERATION_OPERATION_REQUEST_KEYS = objectFreeze([
  "admission",
  "contractVersion",
  "predeterminedResult",
]);
const GENERATION_ADMISSION_KEYS = objectFreeze(["checkpoint", "request"]);
const ACTIVATION_OPERATION_REQUEST_KEYS = objectFreeze([
  "contractVersion",
  "destinationRootPath",
  "generation",
  "holderId",
  "launchIntent",
  "leaseDurationMilliseconds",
  "predecessor",
]);

const ERROR_MESSAGES = objectFreeze({
  invalid_postgres_restore_activation_recovery_coordinator_options:
    "PostgreSQL restore activation recovery coordinator options are invalid",
  invalid_postgres_restore_activation_recovery_coordinator_request:
    "PostgreSQL restore activation recovery coordinator request is invalid",
  postgres_restore_activation_recovery_coordinator_outcome_uncertain:
    "PostgreSQL restore activation recovery coordinator outcome is uncertain",
});

function callIntrinsic(intrinsic, receiver, args) {
  return reflectApply(intrinsic, receiver, args);
}

function arrayIncludes(value, candidate) {
  return callIntrinsic(arrayIncludesIntrinsic, value, [candidate]);
}

function regexpTest(pattern, value) {
  return callIntrinsic(regexpExecIntrinsic, pattern, [value]) !== null;
}

function sha256(value) {
  const hash = callIntrinsic(createHashIntrinsic, undefined, ["sha256"]);
  callIntrinsic(hashUpdateIntrinsic, hash, [value, "utf8"]);
  return callIntrinsic(hashDigestIntrinsic, hash, ["hex"]);
}

function fail(code) {
  throw new PostgresRestoreActivationRecoveryCoordinatorError(code);
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

function clonePlainData(value, code) {
  const state = { nodes: 0 };

  function visit(candidate, depth) {
    state.nodes += 1;
    ensure(depth <= MAX_DATA_DEPTH && state.nodes <= MAX_DATA_NODES, code);
    if (candidate === null || typeof candidate === "boolean") {
      return candidate;
    }
    if (typeof candidate === "string") {
      ensure(candidate.length <= MAX_DATA_STRING_LENGTH, code);
      return candidate;
    }
    if (typeof candidate === "number") {
      ensure(numberIsFinite(candidate), code);
      return candidate;
    }
    ensure(
      typeof candidate === "object" && !isProxyValue(candidate),
      code,
    );
    let prototype;
    let keys;
    try {
      prototype = objectGetPrototypeOf(candidate);
      keys = reflectOwnKeys(candidate);
    } catch {
      fail(code);
    }
    if (arrayIsArray(candidate)) {
      ensure(
        (prototype === arrayPrototype || prototype === null) &&
          candidate.length <= MAX_DATA_ARRAY_ENTRIES &&
          keys.length === candidate.length + 1 &&
          keys[keys.length - 1] === "length",
        code,
      );
      const result = [];
      for (let index = 0; index < candidate.length; index += 1) {
        const key = `${index}`;
        ensure(keys[index] === key, code);
        const descriptor = objectGetOwnPropertyDescriptor(candidate, key);
        ensure(
          descriptor?.enumerable === true &&
            objectHasOwn(descriptor, "value"),
          code,
        );
        result[index] = visit(descriptor.value, depth + 1);
      }
      return objectFreeze(result);
    }
    ensure(
      (prototype === objectPrototype || prototype === null) &&
        keys.length <= MAX_DATA_OBJECT_KEYS,
      code,
    );
    const result = objectCreate(null);
    for (let index = 0; index < keys.length; index += 1) {
      const key = keys[index];
      ensure(typeof key === "string", code);
      const descriptor = objectGetOwnPropertyDescriptor(candidate, key);
      ensure(
        descriptor?.enumerable === true &&
          objectHasOwn(descriptor, "value"),
        code,
      );
      objectDefineProperty(result, key, {
        enumerable: true,
        value: visit(descriptor.value, depth + 1),
      });
    }
    return objectFreeze(result);
  }

  return visit(value, 0);
}

function canonicalTimestamp(value, code, nullable = false) {
  if (nullable && value === null) return null;
  ensure(typeof value === "string", code);
  let milliseconds;
  let canonical;
  try {
    milliseconds = callIntrinsic(dateParseIntrinsic, DateConstructor, [
      value,
    ]);
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

function timestampMilliseconds(value, code) {
  canonicalTimestamp(value, code);
  const milliseconds = callIntrinsic(dateParseIntrinsic, DateConstructor, [
    value,
  ]);
  ensure(numberIsFinite(milliseconds), code);
  return milliseconds;
}

function canonicalRevision(value, code) {
  ensure(typeof value === "string" && regexpTest(UINT64_PATTERN, value), code);
  return value;
}

function canonicalSha256(value, code) {
  ensure(typeof value === "string" && regexpTest(SHA256_PATTERN, value), code);
  return value;
}

function opaqueId(value, code) {
  ensure(
    typeof value === "string" && regexpTest(OPAQUE_ID_PATTERN, value),
    code,
  );
  return value;
}

function trustedFunction(value, code) {
  ensure(
    typeof value === "function" &&
      !isProxyValue(value) &&
      !isGeneratorFunctionValue(value),
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

async function settleTrusted(value, code) {
  if (isGeneratorObjectValue(value)) fail(code);
  if (!isPromiseValue(value)) return value;
  const normalized = normalizeSafeNativePromise(value);
  ensure(normalized !== null, code);
  try {
    const settled = await normalized;
    if (isGeneratorObjectValue(settled)) fail(code);
    return settled;
  } catch (error) {
    if (
      error instanceof PostgresRestoreActivationRecoveryCoordinatorError
    ) {
      throw error;
    }
    fail(code);
  }
}

function sameJson(left, right, code) {
  function compare(a, b, depth) {
    ensure(depth <= 32, code);
    if (a === null || b === null || typeof a !== "object" || typeof b !== "object") {
      return objectIs(a, b);
    }
    ensure(
      !isProxyValue(a) &&
        !isProxyValue(b) &&
        arrayIsArray(a) === arrayIsArray(b),
      code,
    );
    const aKeys = reflectOwnKeys(a);
    const bKeys = reflectOwnKeys(b);
    if (aKeys.length !== bKeys.length) return false;
    for (let index = 0; index < aKeys.length; index += 1) {
      const key = aKeys[index];
      if (key !== bKeys[index] || typeof key !== "string") return false;
      const aDescriptor = objectGetOwnPropertyDescriptor(a, key);
      const bDescriptor = objectGetOwnPropertyDescriptor(b, key);
      ensure(
        objectHasOwn(aDescriptor ?? {}, "value") &&
          objectHasOwn(bDescriptor ?? {}, "value"),
        code,
      );
      if (!compare(aDescriptor.value, bDescriptor.value, depth + 1)) {
        return false;
      }
    }
    return true;
  }
  return compare(left, right, 0);
}

function normalizeSnapshot(value, expectedSessionId, code) {
  const snapshot = exactDataObject(value, SNAPSHOT_KEYS, code);
  exactDataObject(snapshot.document, DOCUMENT_KEYS, code);
  ensure(
    snapshot.sessionId === expectedSessionId &&
      typeof snapshot.document.lifecycle === "string" &&
      regexpTest(UINT64_PATTERN, snapshot.document.writerEpoch),
    code,
  );
  canonicalRevision(snapshot.revision, code);
  canonicalTimestamp(snapshot.createdAt, code);
  canonicalTimestamp(snapshot.updatedAt, code);
  return snapshot;
}

function normalizeOperation(value, expected, code) {
  const operation = exactDataObject(value, OPERATION_KEYS, code);
  const expectedSession = normalizeSnapshot(
    operation.expectedSession,
    operation.sessionId,
    code,
  );
  ensure(
    operation.conflictClass === SESSION_OPERATION_CONFLICT_CLASS &&
      operation.operationId === expected.operationId &&
      operation.kind === expected.kind &&
      operation.sessionId === expectedSession.sessionId &&
      arrayIncludes(expected.states, operation.state) &&
      sameJson(operation.request, expected.request, code),
    code,
  );
  canonicalSha256(operation.requestSha256, code);
  canonicalTimestamp(operation.createdAt, code);
  canonicalTimestamp(operation.updatedAt, code);
  canonicalTimestamp(operation.retiredAt, code, true);
  canonicalRevision(operation.revision, code);
  if (operation.state === "prepared") {
    ensure(
      operation.revision === "0" &&
        operation.result === null &&
        operation.retiredAt === null,
      code,
    );
  } else if (operation.state === "starting") {
    ensure(
      operation.revision === "1" &&
        operation.result === null &&
        operation.retiredAt === null,
      code,
    );
  } else if (operation.state === "uncertain") {
    ensure(
      operation.revision === "2" &&
        operation.result === null &&
        operation.retiredAt === null,
      code,
    );
  } else {
    ensure(
      operation.state === "committed" &&
        (operation.revision === "1" ||
          operation.revision === "2" ||
          operation.revision === "3") &&
        operation.result !== null &&
        operation.retiredAt === operation.updatedAt,
      code,
    );
  }
  return operation;
}

function normalizeReservation(value, operation, code) {
  const reservation = exactDataObject(value, RESERVATION_KEYS, code);
  ensure(
    reservation.conflictClass === SESSION_OPERATION_CONFLICT_CLASS &&
      reservation.operationId === operation.operationId &&
      reservation.sessionId === operation.sessionId &&
      reservation.kind === operation.kind &&
      reservation.expectedSessionRevision ===
        operation.expectedSession.revision &&
      reservation.requestSha256 === operation.requestSha256 &&
      reservation.expiresAt === null,
    code,
  );
  opaqueId(reservation.reservationId, code);
  canonicalTimestamp(reservation.createdAt, code);
  canonicalTimestamp(reservation.updatedAt, code);
  canonicalTimestamp(reservation.releasedAt, code, true);
  if (operation.state === "committed") {
    ensure(
      reservation.state === "released" &&
        reservation.releasedAt === reservation.updatedAt,
      code,
    );
  } else {
    ensure(
      reservation.state === operation.state &&
        reservation.releasedAt === null,
      code,
    );
  }
  return reservation;
}

function normalizeGeneration(value, expected, code) {
  const generation = exactDataObject(value, GENERATION_KEYS, code);
  ensure(
    generation.checkpointId === expected.checkpointId &&
      generation.generationId === expected.generationId &&
      generation.operationId === expected.operationId &&
      generation.sessionId === expected.sessionId &&
      generation.state === expected.state,
    code,
  );
  canonicalTimestamp(generation.claimedAt, code);
  canonicalTimestamp(generation.committedAt, code, true);
  ensure(
    generation.binding !== null &&
      typeof generation.binding === "object" &&
      !arrayIsArray(generation.binding),
    code,
  );
  if (generation.state === "authorized") {
    ensure(generation.committedAt === null && generation.document === null, code);
  } else {
    ensure(
      generation.state === "committed" &&
        generation.committedAt !== null &&
        generation.document !== null &&
        typeof generation.document === "object" &&
        !arrayIsArray(generation.document),
      code,
    );
  }
  return generation;
}

function normalizeCatalogue(value, candidate, code) {
  const catalogue = exactDataObject(value, CATALOGUE_KEYS, code);
  ensure(
    catalogue.checkpointId === candidate.checkpoint.checkpointId &&
      catalogue.sessionId === candidate.checkpoint.sessionId &&
      catalogue.document !== null &&
      typeof catalogue.document === "object" &&
      !arrayIsArray(catalogue.document),
    code,
  );
  opaqueId(catalogue.captureAttemptId, code);
  canonicalTimestamp(catalogue.committedAt, code);
  return catalogue;
}

function normalizeGenerationOperationRequest(value, candidate, code) {
  const request = exactDataObject(
    value,
    GENERATION_OPERATION_REQUEST_KEYS,
    code,
  );
  const admission = exactDataObject(
    request.admission,
    GENERATION_ADMISSION_KEYS,
    code,
  );
  ensure(
    request.contractVersion === 1 &&
      sameJson(admission.checkpoint, candidate.checkpoint, code) &&
      sameJson(admission.request, candidate.request, code) &&
      request.predeterminedResult !== null &&
      typeof request.predeterminedResult === "object" &&
      !arrayIsArray(request.predeterminedResult),
    code,
  );
  return request;
}

function normalizeGenerationReceipt(value, candidate, finalized, code) {
  const receipt = exactDataObject(
    value,
    finalized
      ? GENERATION_FINALIZE_RECEIPT_KEYS
      : GENERATION_READ_RECEIPT_KEYS,
    code,
  );
  const request = normalizeGenerationOperationRequest(
    receipt.operation?.request,
    candidate,
    code,
  );
  const operation = normalizeOperation(
    receipt.operation,
    {
      kind: RESTORE_DESTINATION_GENERATION_OPERATION_KIND,
      operationId: candidate.request.operationId,
      request,
      states: ["starting", "uncertain", "committed"],
    },
    code,
  );
  const generation = normalizeGeneration(
    receipt.generation,
    {
      checkpointId: candidate.checkpoint.checkpointId,
      generationId: candidate.generationId,
      operationId: operation.operationId,
      sessionId: operation.sessionId,
      state: operation.state === "committed" ? "committed" : "authorized",
    },
    code,
  );
  normalizeCatalogue(receipt.catalogue, candidate, code);
  normalizeReservation(receipt.reservation, operation, code);
  normalizeSnapshot(receipt.session, operation.sessionId, code);
  ensure(
    receipt.status === (finalized ? operation.state : generation.state),
    code,
  );
  if (finalized) ensure(typeof receipt.finalized === "boolean", code);
  return receipt;
}

function validateCommittedGenerationCompletion(receipt, completion, code) {
  const document = exactDataObject(
    receipt.generation.document,
    GENERATION_DOCUMENT_KEYS,
    code,
  );
  ensure(
    receipt.operation.state === "committed" &&
      receipt.generation.state === "committed" &&
      document.contractVersion ===
        RESTORE_DESTINATION_GENERATION_DOCUMENT_CONTRACT_VERSION &&
      sameJson(document.materialization, completion.materialization, code) &&
      sameJson(document.result, completion.result, code) &&
      sameJson(
        document.artifactProof,
        receipt.catalogue.document.artifactProof,
        code,
      ),
    code,
  );
  return receipt;
}

function normalizeActivationOperationRequest(
  value,
  expectedSession,
  generation,
  code,
) {
  const request = exactDataObject(
    value,
    ACTIVATION_OPERATION_REQUEST_KEYS,
    code,
  );
  let canonical;
  try {
    canonical = createRestoreAttachmentActivationOperationRequest({
      destinationRootPath: request.destinationRootPath,
      expectedSession,
      generation,
      holderId: request.holderId,
      launchIntent: request.launchIntent,
      leaseDurationMilliseconds: request.leaseDurationMilliseconds,
      predecessor: request.predecessor,
    });
  } catch {
    fail(code);
  }
  ensure(sameJson(request, canonical, code), code);
  return canonical;
}

function normalizeActivationTerminalResult(
  value,
  activationRequest,
  activationResult,
  code,
) {
  const result = exactDataObject(
    value,
    ACTIVATION_TERMINAL_RESULT_KEYS,
    code,
  );
  ensure(
    result.outcome === "restore-attachment-activated" &&
      result.resultVersion === 1 &&
      sameJson(result.activationRequest, activationRequest, code) &&
      sameJson(result.activationResult, activationResult, code),
    code,
  );
  return result;
}

function validateActivationRequestAuthorityBinding(
  activationRequest,
  operationRequest,
  operation,
  code,
) {
  const expectedSession = operation.expectedSession;
  let expectedEpoch;
  try {
    expectedEpoch = callIntrinsic(
      bigIntToStringIntrinsic,
      BigIntConstructor(expectedSession.document.writerEpoch) + 1n,
      [],
    );
  } catch {
    fail(code);
  }
  const expectedLeaseId = `lease-${sha256(
    `writer-lease:${operation.operationId}`,
  )}`;
  const expectedAttachmentId = `attachment-${sha256(
    `writer-attachment:${operation.operationId}`,
  )}`;
  ensure(
    sameJson(
      activationRequest.manifest,
      expectedSession.document.manifest,
      code,
    ) &&
      sameJson(
        activationRequest.storageRef,
        expectedSession.document.storageRef,
        code,
      ) &&
      activationRequest.lease.sessionId === expectedSession.sessionId &&
      activationRequest.lease.leaseId === expectedLeaseId &&
      activationRequest.lease.holderId === operationRequest.holderId &&
      activationRequest.lease.fencingEpoch === expectedEpoch &&
      timestampMilliseconds(activationRequest.lease.expiresAt, code) -
          operationRequest.leaseDurationMilliseconds >=
        timestampMilliseconds(expectedSession.updatedAt, code) &&
      activationRequest.mutationRequest.operationId ===
        operation.operationId &&
      activationRequest.mutationRequest.target.attachmentId ===
        expectedAttachmentId &&
      activationRequest.publication.root.rootPath ===
        operationRequest.destinationRootPath,
    code,
  );
}

function normalizeActivationReadReceipt(value, candidate, code) {
  const receipt = exactDataObject(
    value,
    ACTIVATION_READ_RECEIPT_KEYS,
    code,
  );
  const rawOperation = exactDataObject(
    receipt.operation,
    OPERATION_KEYS,
    code,
  );
  const request = normalizeActivationOperationRequest(
    rawOperation.request,
    rawOperation.expectedSession,
    receipt.generation,
    code,
  );
  ensure(sameJson(request, candidate.request, code), code);
  const operation = normalizeOperation(
    receipt.operation,
    {
      kind: RESTORE_ATTACHMENT_ACTIVATION_OPERATION_KIND,
      operationId: candidate.activationOperationId,
      request,
      states: ["starting", "uncertain", "committed"],
    },
    code,
  );
  const generationReference = request.generation;
  const generation = normalizeGeneration(
    receipt.generation,
    {
      checkpointId: generationReference.checkpointId,
      generationId: generationReference.generationId,
      operationId: generationReference.operationId,
      sessionId: generationReference.sessionId,
      state: "committed",
    },
    code,
  );
  normalizeReservation(receipt.reservation, operation, code);
  normalizeSnapshot(receipt.session, operation.sessionId, code);
  let activationRequest;
  try {
    activationRequest = assertRestoreAttachmentActivationRequest(
      receipt.activationRequest,
    );
  } catch {
    fail(code);
  }
  validateActivationRequestAuthorityBinding(
    activationRequest,
    request,
    operation,
    code,
  );
  ensure(receipt.status === operation.state, code);
  if (operation.state === "committed") {
    let terminalActivationResult;
    try {
      terminalActivationResult = assertRestoreAttachmentActivationResult(
        operation.result?.activationResult,
        { request: activationRequest },
      );
    } catch {
      fail(code);
    }
    normalizeActivationTerminalResult(
      operation.result,
      activationRequest,
      terminalActivationResult,
      code,
    );
  }
  return exactFrozenRecord({
    activationRequest,
    generation,
    operation,
    reservation: receipt.reservation,
    session: receipt.session,
    status: receipt.status,
  });
}

function normalizeActivationHandoffReceipt(
  value,
  candidate,
  activationRequest,
  activationResult,
  code,
) {
  const receipt = exactDataObject(
    value,
    ACTIVATION_HANDOFF_RECEIPT_KEYS,
    code,
  );
  const activation = exactDataObject(
    receipt.activation,
    ACTIVATION_RECEIPT_KEYS,
    code,
  );
  const rawOperation = exactDataObject(
    activation.operation,
    OPERATION_KEYS,
    code,
  );
  const request = normalizeActivationOperationRequest(
    rawOperation.request,
    rawOperation.expectedSession,
    receipt.generation,
    code,
  );
  ensure(sameJson(request, candidate.request, code), code);
  const operation = normalizeOperation(
    activation.operation,
    {
      kind: RESTORE_ATTACHMENT_ACTIVATION_OPERATION_KIND,
      operationId: candidate.activationOperationId,
      request,
      states: ["committed"],
    },
    code,
  );
  normalizeActivationTerminalResult(
    operation.result,
    activationRequest,
    activationResult,
    code,
  );
  normalizeReservation(activation.reservation, operation, code);
  ensure(typeof activation.finalized === "boolean", code);
  const generationReference = request.generation;
  normalizeGeneration(
    receipt.generation,
    {
      checkpointId: generationReference.checkpointId,
      generationId: generationReference.generationId,
      operationId: generationReference.operationId,
      sessionId: generationReference.sessionId,
      state: "committed",
    },
    code,
  );
  const launch = exactDataObject(receipt.launch, LAUNCH_RECEIPT_KEYS, code);
  const launchOperation = normalizeOperation(
    launch.operation,
    {
      kind: WRITER_LAUNCH_ATTEMPT_OPERATION_KIND,
      operationId: request.launchIntent.launchAttemptId,
      request: launch.operation?.request,
      states: ["prepared", "starting", "uncertain", "committed"],
    },
    code,
  );
  normalizeReservation(launch.reservation, launchOperation, code);
  const attempt = exactDataObject(
    launch.attempt,
    LAUNCH_ATTEMPT_KEYS,
    code,
  );
  ensure(
    attempt.contractVersion === 1 &&
      attempt.launchAttemptId === request.launchIntent.launchAttemptId &&
      attempt.state === launchOperation.state &&
      sameJson(attempt.request, launchOperation.request, code) &&
      sameJson(attempt.result, launchOperation.result, code),
    code,
  );
  normalizeSnapshot(receipt.session, operation.sessionId, code);
  ensure(receipt.status === attempt.state, code);
  return receipt;
}

function normalizeGenerationCandidate(value, code) {
  const hasLaunchIntent =
    value !== null &&
    typeof value === "object" &&
    !isProxyValue(value) &&
    objectHasOwn(value, "launchIntent");
  const candidate = exactDataObject(
    value,
    hasLaunchIntent
      ? GENERATION_CANDIDATE_V2_KEYS
      : GENERATION_CANDIDATE_KEYS,
    code,
  );
  let checkpoint;
  let request;
  try {
    checkpoint = assertCheckpointDescriptor(candidate.checkpoint);
    request = assertStorageMutationRequest(candidate.request);
  } catch {
    fail(code);
  }
  ensure(
    request.operation === "restore" &&
      request.sessionId === checkpoint.sessionId &&
      request.backendId === checkpoint.backendId &&
      request.storageId === checkpoint.storageId &&
      request.target.checkpointId === checkpoint.checkpointId &&
      request.target.artifactId === checkpoint.artifactId,
    code,
  );
  return exactFrozenRecord({
    checkpoint,
    generationId: opaqueId(candidate.generationId, code),
    ...(hasLaunchIntent ? { launchIntent: candidate.launchIntent } : {}),
    request,
  });
}

function normalizeActivationCandidate(value, code) {
  const candidate = exactDataObject(value, ACTIVATION_CANDIDATE_KEYS, code);
  ensure(
    candidate.state === "starting" || candidate.state === "uncertain",
    code,
  );
  return exactFrozenRecord({
    activationOperationId: opaqueId(
      candidate.activationOperationId,
      code,
    ),
    request: candidate.request,
    state: candidate.state,
  });
}

function normalizeDestination(value, code) {
  const destination = exactDataObject(value, DESTINATION_KEYS, code);
  ensure(
    typeof destination.destinationDirectory === "string" &&
      typeof destination.destinationOwnedRoot === "string",
    code,
  );
  return exactFrozenRecord({
    destinationDirectory: destination.destinationDirectory,
    destinationOwnedRoot: destination.destinationOwnedRoot,
  });
}

function operationInput(operation, code) {
  ensure(
    operation !== null &&
      typeof operation === "object" &&
      !isProxyValue(operation),
    code,
  );
  return exactFrozenRecord({
    expectedSession: operation.expectedSession,
    kind: operation.kind,
    operationId: operation.operationId,
    request: operation.request,
  });
}

function operationTransitionInput(operation, expectedOperationRevision, code) {
  return exactFrozenRecord({
    ...operationInput(operation, code),
    expectedOperationRevision,
  });
}

function generationVerifierInput(read, destination, code) {
  ensure(
    read.generation !== null &&
      read.generation.document === null &&
      read.generation.state === "authorized" &&
      read.catalogue?.document?.artifactProof !== null &&
      read.catalogue?.document?.artifactProof !== undefined &&
      read.operation?.request?.predeterminedResult !== null &&
      read.operation?.request?.predeterminedResult !== undefined,
    code,
  );
  return exactFrozenRecord({
    artifactProof: read.catalogue.document.artifactProof,
    binding: read.generation.binding,
    destinationDirectory: destination.destinationDirectory,
    destinationOwnedRoot: destination.destinationOwnedRoot,
    operationId: read.operation.operationId,
    request: read.operation.request.admission.request,
    result: read.operation.request.predeterminedResult,
  });
}

function activationVerifierInput(read, destination, code) {
  const generation = read.generation;
  ensure(
    generation !== null &&
      generation.state === "committed" &&
      generation.document !== null,
    code,
  );
  return exactFrozenRecord({
    artifactProof: generation.document.artifactProof,
    binding: generation.binding,
    destinationDirectory: destination.destinationDirectory,
    destinationOwnedRoot: destination.destinationOwnedRoot,
    operationId: generation.operationId,
    request: generation.binding.request,
    result: generation.document.result,
  });
}

function validateActivationPublication(
  activationRequest,
  completion,
  destination,
  code,
) {
  let request;
  try {
    request = assertRestoreAttachmentActivationRequest(activationRequest);
  } catch {
    fail(code);
  }
  const publication = request.publication;
  const materialization = completion.materialization;
  ensure(
    materialization !== null &&
      typeof materialization === "object" &&
      materialization.publicationKind === "restore-destination" &&
      publication.publicationKind === materialization.publicationKind &&
      publication.publicationId === materialization.publicationId &&
      publication.artifactManifestDigest ===
        materialization.artifactManifestDigest &&
      publication.coordinatorBindingSha256 ===
        materialization.coordinatorBindingSha256 &&
      publication.modeledDigest === materialization.modeledDigest &&
      publication.treeIdentityDigest ===
        materialization.treeIdentityDigest &&
      publication.root.filesystemId ===
        materialization.stagedRoot.filesystemId &&
      publication.root.objectIdentityScheme ===
        materialization.stagedRoot.objectIdentityScheme &&
      publication.root.objectId === materialization.stagedRoot.objectId &&
      publication.root.rootPath === destination.destinationDirectory,
    code,
  );
  return request;
}

export class PostgresRestoreActivationRecoveryCoordinatorError extends ErrorConstructor {
  constructor(code) {
    if (typeof code !== "string" || !objectHasOwn(ERROR_MESSAGES, code)) {
      throw new TypeErrorConstructor(
        "Unsupported PostgreSQL restore activation recovery coordinator error",
      );
    }
    const message = ERROR_MESSAGES[code];
    super(message);
    objectDefineProperty(this, "name", {
      enumerable: false,
      value: "PostgresRestoreActivationRecoveryCoordinatorError",
    });
    objectDefineProperty(this, "code", { enumerable: true, value: code });
    objectDefineProperty(this, "retryable", {
      enumerable: true,
      value: false,
    });
    objectDefineProperty(this, "stack", {
      enumerable: false,
      value: `PostgresRestoreActivationRecoveryCoordinatorError: ${message}`,
    });
    objectFreeze(this);
  }
}

export function createPostgresRestoreActivationRecoveryCoordinator(...args) {
  const optionCode =
    "invalid_postgres_restore_activation_recovery_coordinator_options";
  ensure(args.length === 1, optionCode);
  const options = exactDataObject(args[0], OPTION_KEYS, optionCode);
  const authorityValue = exactDataObject(
    options.authority,
    AUTHORITY_KEYS,
    optionCode,
  );
  const authority = exactFrozenRecord({
    receiver: options.authority,
    finalizeRestoreAttachmentActivationAndReserveWriterLaunchAttempt:
      trustedFunction(
        authorityValue
          .finalizeRestoreAttachmentActivationAndReserveWriterLaunchAttempt,
        optionCode,
      ),
    finalizeRestoreDestinationGeneration: trustedFunction(
      authorityValue.finalizeRestoreDestinationGeneration,
      optionCode,
    ),
    markOperationUncertain: trustedFunction(
      authorityValue.markOperationUncertain,
      optionCode,
    ),
    readRestoreAttachmentActivation: trustedFunction(
      authorityValue.readRestoreAttachmentActivation,
      optionCode,
    ),
    readRestoreDestinationGeneration: trustedFunction(
      authorityValue.readRestoreDestinationGeneration,
      optionCode,
    ),
  });
  ensure(
    !isProxyValue(options.operationGuard) &&
      options.operationGuard instanceof PostgresOperationGuard,
    optionCode,
  );
  ensure(
    !isProxyValue(options.publication) &&
      options.publication instanceof StoppedDirectoryPublication,
    optionCode,
  );
  let storageBackend;
  try {
    storageBackend = assertRestoreAttachmentActivationBackend(
      options.storageBackend,
    );
  } catch {
    fail(optionCode);
  }
  const prepareRestoreAttachment = trustedFunction(
    objectGetOwnPropertyDescriptor(
      storageBackend,
      "prepareRestoreAttachment",
    )?.value,
    optionCode,
  );
  const resolveRestoreDestination = trustedFunction(
    options.resolveRestoreDestination,
    optionCode,
  );
  const requestCode =
    "invalid_postgres_restore_activation_recovery_coordinator_request";
  const outcomeCode =
    "postgres_restore_activation_recovery_coordinator_outcome_uncertain";

  async function invokeAuthority(method, input) {
    let pending;
    try {
      pending = callIntrinsic(authority[method], authority.receiver, [input]);
    } catch {
      fail(outcomeCode);
    }
    const settled = await settleTrusted(pending, outcomeCode);
    try {
      return clonePlainData(settled, outcomeCode);
    } catch (error) {
      if (
        error instanceof PostgresRestoreActivationRecoveryCoordinatorError
      ) {
        throw error;
      }
      fail(outcomeCode);
    }
  }

  async function resolveDestination(kind, candidate, read) {
    let pending;
    try {
      pending = callIntrinsic(resolveRestoreDestination, undefined, [
        exactFrozenRecord({ candidate, generation: read.generation, kind }),
      ]);
    } catch {
      fail(outcomeCode);
    }
    return normalizeDestination(
      await settleTrusted(pending, outcomeCode),
      outcomeCode,
    );
  }

  async function probeGuard(probe) {
    ensure(
      typeof probe === "function" &&
        !isProxyValue(probe) &&
        !isGeneratorFunctionValue(probe),
      outcomeCode,
    );
    let pending;
    try {
      pending = callIntrinsic(probe, undefined, []);
    } catch {
      fail(outcomeCode);
    }
    await settleTrusted(pending, outcomeCode);
  }

  async function markStartingUncertain(read) {
    if (read.operation.state !== "starting") return read;
    const transition = operationTransitionInput(
      read.operation,
      "1",
      outcomeCode,
    );
    try {
      await invokeAuthority("markOperationUncertain", transition);
    } catch {
      // Readback below decides whether the write committed or another actor
      // completed the operation. The original error is intentionally hidden.
    }
    return null;
  }

  async function runGuarded(operationId, callback) {
    let pending;
    try {
      pending = callIntrinsic(runExclusiveIntrinsic, options.operationGuard, [
        operationId,
        async (probeValue) => {
          const probe = exactDataObject(
            probeValue,
            OPERATION_GUARD_PROBE_KEYS,
            outcomeCode,
          );
          const assertHeld = trustedFunction(
            probe.assertHeld,
            outcomeCode,
          );
          return callback(assertHeld);
        },
      ]);
    } catch {
      fail(outcomeCode);
    }
    return settleTrusted(pending, outcomeCode);
  }

  async function reconcileRestoreGenerationInternal(candidateValue) {
    const candidate = normalizeGenerationCandidate(candidateValue, requestCode);
    // Historical v2 generations belong to their legacy atomic handoff. This
    // coordinator must never materialize a launch against the old attachment.
    ensure(!objectHasOwn(candidate, "launchIntent"), outcomeCode);
    return runGuarded(candidate.request.operationId, async (probe) => {
      await probeGuard(probe);
      let read = normalizeGenerationReceipt(
        await invokeAuthority("readRestoreDestinationGeneration", {
          checkpoint: candidate.checkpoint,
          generationId: candidate.generationId,
          request: candidate.request,
        }),
        candidate,
        false,
        outcomeCode,
      );
      if (read.operation.state === "committed") return read;
      if (read.operation.state === "starting") {
        await markStartingUncertain(read);
        read = normalizeGenerationReceipt(
          await invokeAuthority("readRestoreDestinationGeneration", {
            checkpoint: candidate.checkpoint,
            generationId: candidate.generationId,
            request: candidate.request,
          }),
          candidate,
          false,
          outcomeCode,
        );
        if (read.operation.state === "committed") return read;
      }
      ensure(read.operation.state === "uncertain", outcomeCode);
      const destination = await resolveDestination(
        "generation",
        candidate,
        read,
      );
      await probeGuard(probe);
      let completion;
      try {
        completion = await callIntrinsic(
          verifyCommittedRestoreDestinationIntrinsic,
          options.publication,
          [generationVerifierInput(read, destination, outcomeCode)],
        );
      } catch {
        fail(outcomeCode);
      }
      await probeGuard(probe);
      ensure(
        sameJson(
          completion.result,
          read.operation.request.predeterminedResult,
          outcomeCode,
        ),
        outcomeCode,
      );
      try {
        const finalized = normalizeGenerationReceipt(
          await invokeAuthority("finalizeRestoreDestinationGeneration", {
            ...operationTransitionInput(read.operation, "2", outcomeCode),
            completion,
          }),
          candidate,
          true,
          outcomeCode,
        );
        return validateCommittedGenerationCompletion(
          finalized,
          completion,
          outcomeCode,
        );
      } catch {
        const replay = normalizeGenerationReceipt(
          await invokeAuthority("readRestoreDestinationGeneration", {
            checkpoint: candidate.checkpoint,
            generationId: candidate.generationId,
            request: candidate.request,
          }),
          candidate,
          false,
          outcomeCode,
        );
        ensure(replay.operation.state === "committed", outcomeCode);
        return validateCommittedGenerationCompletion(
          replay,
          completion,
          outcomeCode,
        );
      }
    });
  }

  async function reconcileRestoreActivationInternal(candidateValue) {
    const candidate = normalizeActivationCandidate(candidateValue, requestCode);
    return runGuarded(candidate.activationOperationId, async (probe) => {
      await probeGuard(probe);
      let read = normalizeActivationReadReceipt(
        await invokeAuthority("readRestoreAttachmentActivation", {
          operationId: candidate.activationOperationId,
        }),
        candidate,
        outcomeCode,
      );
      if (read.operation.state === "committed") return read;
      if (read.operation.state === "starting") {
        await markStartingUncertain(read);
        read = normalizeActivationReadReceipt(
          await invokeAuthority("readRestoreAttachmentActivation", {
            operationId: candidate.activationOperationId,
          }),
          candidate,
          outcomeCode,
        );
        if (read.operation.state === "committed") return read;
      }
      ensure(
        read.operation.state === "uncertain" &&
          read.activationRequest !== null &&
          read.generation !== null,
        outcomeCode,
      );
      const destination = await resolveDestination(
        "activation",
        candidate,
        read,
      );
      await probeGuard(probe);
      let completion;
      try {
        completion = await callIntrinsic(
          verifyCommittedRestoreDestinationIntrinsic,
          options.publication,
          [activationVerifierInput(read, destination, outcomeCode)],
        );
      } catch {
        fail(outcomeCode);
      }
      ensure(
        sameJson(
          completion.materialization,
          read.generation.document.materialization,
          outcomeCode,
        ) &&
          sameJson(
            completion.result,
            read.generation.document.result,
            outcomeCode,
          ),
        outcomeCode,
      );
      const activationRequest = validateActivationPublication(
        read.activationRequest,
        completion,
        destination,
        outcomeCode,
      );
      await probeGuard(probe);
      let rawActivationResult;
      try {
        rawActivationResult = callIntrinsic(
          prepareRestoreAttachment,
          storageBackend,
          [activationRequest],
        );
      } catch {
        fail(outcomeCode);
      }
      rawActivationResult = await settleTrusted(
        rawActivationResult,
        outcomeCode,
      );
      let activationResult;
      try {
        activationResult = assertRestoreAttachmentActivationResult(
          rawActivationResult,
          { request: activationRequest },
        );
      } catch {
        fail(outcomeCode);
      }
      await probeGuard(probe);
      try {
        return normalizeActivationHandoffReceipt(
          await invokeAuthority(
            "finalizeRestoreAttachmentActivationAndReserveWriterLaunchAttempt",
            {
              ...operationTransitionInput(read.operation, "2", outcomeCode),
              activationResult,
            },
          ),
          candidate,
          activationRequest,
          activationResult,
          outcomeCode,
        );
      } catch {
        const replay = normalizeActivationReadReceipt(
          await invokeAuthority(
            "readRestoreAttachmentActivation",
            { operationId: candidate.activationOperationId },
          ),
          candidate,
          outcomeCode,
        );
        ensure(replay.operation.state === "committed", outcomeCode);
        normalizeActivationTerminalResult(
          replay.operation.result,
          activationRequest,
          activationResult,
          outcomeCode,
        );
        return replay;
      }
    });
  }

  const reconcileRestoreGeneration = function reconcileRestoreGeneration(
    ...reconcileArgs
  ) {
    ensure(reconcileArgs.length === 1, requestCode);
    return reconcileRestoreGenerationInternal(reconcileArgs[0]);
  };
  const reconcileRestoreAttachmentActivation =
    function reconcileRestoreAttachmentActivation(...reconcileArgs) {
      ensure(reconcileArgs.length === 1, requestCode);
      return reconcileRestoreActivationInternal(reconcileArgs[0]);
    };
  objectFreeze(reconcileRestoreGeneration);
  objectFreeze(reconcileRestoreAttachmentActivation);
  return exactFrozenRecord({
    reconcileRestoreAttachmentActivation,
    reconcileRestoreGeneration,
  });
}
