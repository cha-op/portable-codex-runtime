import { createHash } from "node:crypto";
import { types as utilTypes } from "node:util";

import { PostgresOperationGuard } from "./postgres-operation-guard.mjs";
import {
  RESTORE_ATTACHMENT_ACTIVATION_OPERATION_KIND,
  RESTORE_DESTINATION_GENERATION_DOCUMENT_CONTRACT_VERSION,
  RESTORE_DESTINATION_GENERATION_OPERATION_KIND,
  SESSION_AUTHORITY_DOCUMENT_VERSION,
  SESSION_OPERATION_CONFLICT_CLASS,
  WRITER_LAUNCH_ATTEMPT_OPERATION_KIND,
  WRITER_LAUNCH_PRE_DISPATCH_CANCELLATION_REASON,
  assertSessionAuthoritySnapshot,
  assertSessionOperationBinding,
  createRestoreAttachmentActivationOperationRequest,
  createRestoreAttachmentActivationOperationRequestV2,
  createRestoreDestinationGenerationOperationRequest,
  createWriterLaunchAttemptOperationRequest,
} from "./postgres-session-authority.mjs";
import {
  assertCheckpointDescriptor,
  assertRestoreAttachmentActivationBackend,
  assertRestoreAttachmentActivationRequest,
  assertRestoreAttachmentActivationResult,
  assertStorageMutationRequest,
  assertStorageMutationResult,
} from "./session-storage-contracts.mjs";
import { StoppedDirectoryPublication } from "./stopped-directory-publication.mjs";

const arrayIncludesIntrinsic = Array.prototype.includes;
const arrayIsArray = Array.isArray;
const arrayPrototype = Array.prototype;
const arraySortIntrinsic = Array.prototype.sort;
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
const JsonObject = JSON;
const jsonStringifyIntrinsic = JSON.stringify;
const PromiseConstructor = Promise;
const promisePrototype = Promise.prototype;
const promiseSpeciesSymbol = Symbol.species;
const promiseThenIntrinsic = Promise.prototype.then;
const reflectApply = Reflect.apply;
const reflectOwnKeys = Reflect.ownKeys;
const regexpExecIntrinsic = RegExp.prototype.exec;
const TypeErrorConstructor = TypeError;
const authorityInvocationUncertain = objectFreeze(objectCreate(null));
const promiseSettlementBrand = objectFreeze(objectCreate(null));

const runExclusiveIntrinsic = PostgresOperationGuard.prototype.runExclusive;
const verifyCommittedRestoreDestinationIntrinsic =
  StoppedDirectoryPublication.prototype.verifyCommittedRestoreDestination;

const OPAQUE_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u;
const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u;
const SHA256_PATTERN = /^[0-9a-f]{64}$/u;
const PERSISTENT_OBJECT_ID_PATTERN =
  /^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$/u;
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
const GENERATION_BINDING_KEYS = objectFreeze([
  "attachment",
  "captureAttemptId",
  "captureOperationId",
  "catalogueSha256",
  "checkpoint",
  "contractVersion",
  "destinationIsolationProofId",
  "destinationState",
  "generationId",
  "request",
  "reservationId",
]);
const GENERATION_TERMINAL_RESULT_KEYS = objectFreeze([
  "catalogueSha256",
  "checkpointId",
  "generationDocumentSha256",
  "generationId",
  "outcome",
  "resultVersion",
]);
const CATALOGUE_KEYS = objectFreeze([
  "captureAttemptId",
  "checkpointId",
  "committedAt",
  "document",
  "sessionId",
]);
const CATALOGUE_DOCUMENT_KEYS = objectFreeze([
  "artifactProof",
  "contractVersion",
  "materialization",
  "result",
]);
const CHECKPOINT_ARTIFACT_PROOF_KEYS = objectFreeze([
  "artifactManifestDigest",
  "captureOperationId",
  "modeledDigest",
]);
const CHECKPOINT_MATERIALIZATION_KEYS = objectFreeze([
  "artifactManifestDigest",
  "contractVersion",
  "modeledDigest",
  "publicationId",
  "publicationKind",
  "stagedRoot",
  "treeIdentityDigest",
]);
const RESTORE_MATERIALIZATION_KEYS = objectFreeze([
  "artifactManifestDigest",
  "contractVersion",
  "coordinatorBindingSha256",
  "modeledDigest",
  "publicationId",
  "publicationKind",
  "stagedRoot",
  "treeIdentityDigest",
]);
const STAGED_ROOT_KEYS = objectFreeze([
  "filesystemId",
  "objectIdentityScheme",
  "objectId",
]);
const CHECKPOINT_CAPTURE_RESULT_KEYS = objectFreeze([
  "checkpoint",
  "mutation",
]);
const STORAGE_MUTATION_RESULT_KEYS = objectFreeze([
  "backendId",
  "contractVersion",
  "fencingEpoch",
  "holderId",
  "leaseId",
  "operation",
  "operationId",
  "proofId",
  "sessionId",
  "status",
  "storageId",
  "target",
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
const ACTIVE_OPERATION_POINTER_KEYS = objectFreeze([
  "conflictClass",
  "expectedSessionRevision",
  "kind",
  "operationId",
  "operationRevision",
  "requestSha256",
  "reservationId",
  "state",
]);
const WRITER_LAUNCH_RESULT_KEYS = objectFreeze([
  "evidence",
  "outcome",
  "resultVersion",
]);
const CANCELLATION_RESULT_KEYS = objectFreeze([
  "outcome",
  "reason",
  "resultVersion",
]);
const WRITER_LAUNCH_EVIDENCE_KEYS = objectFreeze([
  "contractVersion",
  "launchAttemptId",
  "processIncarnationId",
  "proofId",
  "status",
  "supervisorId",
  "writerIncarnationId",
]);
const WRITER_LAUNCH_POINTER_KEYS = objectFreeze([
  "attachmentId",
  "attachmentSha256",
  "contractVersion",
  "fencingEpoch",
  "generation",
  "launchAttemptId",
  "launchResultSha256",
  "leaseId",
  "leaseSha256",
  "measuredImageSha256",
  "processIncarnationId",
  "startedAt",
  "supervisorId",
  "supervisorProofId",
  "writerIncarnationId",
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

function canonicalSerialize(value, code) {
  const state = { nodes: 0 };

  function visit(candidate, depth) {
    state.nodes += 1;
    ensure(depth <= MAX_DATA_DEPTH && state.nodes <= MAX_DATA_NODES, code);
    if (
      candidate === null ||
      typeof candidate === "boolean" ||
      typeof candidate === "string"
    ) {
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
    const keys = reflectOwnKeys(candidate);
    if (arrayIsArray(candidate)) {
      ensure(
        candidate.length <= MAX_DATA_ARRAY_ENTRIES &&
          keys.length === candidate.length + 1 &&
          keys[keys.length - 1] === "length",
        code,
      );
      const result = [];
      for (let index = 0; index < candidate.length; index += 1) {
        ensure(keys[index] === `${index}`, code);
        const descriptor = objectGetOwnPropertyDescriptor(
          candidate,
          keys[index],
        );
        ensure(
          descriptor?.enumerable === true &&
            objectHasOwn(descriptor, "value"),
          code,
        );
        result[index] = visit(descriptor.value, depth + 1);
      }
      return result;
    }
    ensure(keys.length <= MAX_DATA_OBJECT_KEYS, code);
    for (let index = 0; index < keys.length; index += 1) {
      ensure(typeof keys[index] === "string", code);
    }
    const result = objectCreate(null);
    for (let index = 0; index < keys.length; index += 1) {
      const key = keys[index];
      const descriptor = objectGetOwnPropertyDescriptor(candidate, key);
      ensure(
        descriptor?.enumerable === true &&
          objectHasOwn(descriptor, "value"),
        code,
      );
      result[key] = visit(descriptor.value, depth + 1);
    }
    return result;
  }

  let serialized;
  try {
    serialized = callIntrinsic(jsonStringifyIntrinsic, JsonObject, [
      visit(value, 0),
    ]);
  } catch (error) {
    if (
      error instanceof PostgresRestoreActivationRecoveryCoordinatorError
    ) {
      throw error;
    }
    fail(code);
  }
  ensure(typeof serialized === "string", code);
  return serialized;
}

function canonicalJsonData(value, code) {
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
    const keys = reflectOwnKeys(candidate);
    if (arrayIsArray(candidate)) {
      ensure(
        candidate.length <= MAX_DATA_ARRAY_ENTRIES &&
          keys.length === candidate.length + 1 &&
          keys[keys.length - 1] === "length",
        code,
      );
      const result = [];
      for (let index = 0; index < candidate.length; index += 1) {
        ensure(keys[index] === `${index}`, code);
        const descriptor = objectGetOwnPropertyDescriptor(
          candidate,
          keys[index],
        );
        ensure(
          descriptor?.enumerable === true &&
            objectHasOwn(descriptor, "value"),
          code,
        );
        result[index] = visit(descriptor.value, depth + 1);
      }
      return result;
    }
    ensure(keys.length <= MAX_DATA_OBJECT_KEYS, code);
    for (let index = 0; index < keys.length; index += 1) {
      ensure(typeof keys[index] === "string", code);
    }
    callIntrinsic(arraySortIntrinsic, keys, []);
    const result = objectCreate(null);
    for (let index = 0; index < keys.length; index += 1) {
      const key = keys[index];
      const descriptor = objectGetOwnPropertyDescriptor(candidate, key);
      ensure(
        descriptor?.enumerable === true &&
          objectHasOwn(descriptor, "value"),
        code,
      );
      result[key] = visit(descriptor.value, depth + 1);
    }
    return result;
  }

  return visit(value, 0);
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

function canonicalUuid(value, code) {
  ensure(typeof value === "string" && regexpTest(UUID_PATTERN, value), code);
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

function promiseSettlementCarrier(status, value) {
  return exactFrozenRecord({
    brand: promiseSettlementBrand,
    status,
    value,
  });
}

function promiseFulfillmentCarrier(value) {
  return promiseSettlementCarrier("fulfilled", value);
}

function promiseRejectionCarrier() {
  return promiseSettlementCarrier("rejected", null);
}

objectFreeze(promiseFulfillmentCarrier);
objectFreeze(promiseRejectionCarrier);

function unwrapPromiseSettlementCarrier(value, code) {
  ensure(
    value !== null &&
      typeof value === "object" &&
      !isProxyValue(value) &&
      objectGetPrototypeOf(value) === null &&
      objectIsFrozen(value),
    code,
  );
  const keys = reflectOwnKeys(value);
  ensure(
    keys.length === 3 &&
      keys[0] === "brand" &&
      keys[1] === "status" &&
      keys[2] === "value",
    code,
  );
  const brand = objectGetOwnPropertyDescriptor(value, "brand");
  const status = objectGetOwnPropertyDescriptor(value, "status");
  const payload = objectGetOwnPropertyDescriptor(value, "value");
  ensure(
    brand?.value === promiseSettlementBrand &&
      status?.enumerable === true &&
      objectHasOwn(status, "value") &&
      (status.value === "fulfilled" || status.value === "rejected") &&
      payload?.enumerable === true &&
      objectHasOwn(payload, "value"),
    code,
  );
  return exactFrozenRecord({
    status: status.value,
    value: payload.value,
  });
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
  if (descriptor.value === PromiseConstructor) {
    return exactFrozenRecord({ promise: value, wrapped: false });
  }
  if (!isSafePromiseSpeciesHolder(descriptor.value)) return null;
  try {
    const normalized = callIntrinsic(promiseThenIntrinsic, value, [
      promiseFulfillmentCarrier,
      promiseRejectionCarrier,
    ]);
    objectDefineProperty(normalized, "constructor", {
      configurable: false,
      enumerable: false,
      value: PromiseConstructor,
      writable: false,
    });
    return exactFrozenRecord({ promise: normalized, wrapped: true });
  } catch {
    return null;
  }
}

function hasThenProperty(value, code) {
  if (
    value === null ||
    (typeof value !== "object" && typeof value !== "function")
  ) {
    return false;
  }
  let candidate = value;
  let depth = 0;
  while (candidate !== null) {
    ensure(depth <= MAX_DATA_DEPTH && !isProxyValue(candidate), code);
    let descriptor;
    try {
      descriptor = objectGetOwnPropertyDescriptor(candidate, "then");
      candidate = objectGetPrototypeOf(candidate);
    } catch {
      fail(code);
    }
    if (descriptor !== undefined) return true;
    depth += 1;
  }
  return false;
}

function createTrustedSettlement(value, code) {
  ensure(!isProxyValue(value) && !isGeneratorObjectValue(value), code);
  if (isPromiseValue(value)) {
    const normalized = normalizeSafeNativePromise(value);
    ensure(normalized !== null, code);
    return exactFrozenRecord({ kind: "promise", value: normalized });
  }
  return createTrustedValueSettlement(value, code);
}

function createTrustedValueSettlement(value, code) {
  ensure(
    !isProxyValue(value) &&
      !isGeneratorObjectValue(value) &&
      !isPromiseValue(value),
    code,
  );
  ensure(!hasThenProperty(value, code), code);
  return exactFrozenRecord({ kind: "value", value });
}

function rejectTrustedSettlement(code, rejectionMarker) {
  if (rejectionMarker !== null) throw rejectionMarker;
  fail(code);
}

async function awaitTrustedSettlement(
  settlement,
  code,
  rejectionMarker = null,
) {
  if (settlement.kind === "value") return settlement;
  let settled;
  try {
    settled = await settlement.value.promise;
  } catch {
    rejectTrustedSettlement(code, rejectionMarker);
  }
  if (settlement.value.wrapped) {
    const carrier = unwrapPromiseSettlementCarrier(settled, code);
    if (carrier.status === "rejected") {
      rejectTrustedSettlement(code, rejectionMarker);
    }
    settled = carrier.value;
  }
  return createTrustedValueSettlement(settled, code);
}

function settleTrusted(value, code) {
  return createTrustedSettlement(value, code);
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

function sameCanonicalJson(left, right, code) {
  return sameJson(
    canonicalJsonData(left, code),
    canonicalJsonData(right, code),
    code,
  );
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

function normalizeCanonicalSnapshot(value, expectedSessionId, code) {
  const snapshot = normalizeSnapshot(value, expectedSessionId, code);
  let canonical;
  try {
    canonical = assertSessionAuthoritySnapshot(value);
  } catch {
    fail(code);
  }
  ensure(sameJson(snapshot, canonical, code), code);
  return snapshot;
}

function authorityDocumentWithState(
  value,
  {
    activeOperation = value.activeOperation,
    attachment = value.attachment,
    lastOperation = value.lastOperation,
    launch = value.launch,
    lease = value.lease,
    lifecycle = value.lifecycle,
    writerEpoch = value.writerEpoch,
  },
  code,
) {
  const document = exactDataObject(value, DOCUMENT_KEYS, code);
  return {
    documentVersion: SESSION_AUTHORITY_DOCUMENT_VERSION,
    manifest: document.manifest,
    storageRef: document.storageRef,
    backendCapabilities: document.backendCapabilities,
    lifecycle,
    writerEpoch,
    lease,
    attachment,
    activeOperation,
    lastOperation,
    recovery: document.recovery,
    launch,
  };
}

function authoritySnapshotWithState(
  value,
  { document = value.document, revision = value.revision, updatedAt },
  code,
) {
  const snapshot = normalizeSnapshot(value, value.sessionId, code);
  const candidate = {
    sessionId: snapshot.sessionId,
    revision,
    document,
    createdAt: snapshot.createdAt,
    updatedAt,
  };
  try {
    return assertSessionAuthoritySnapshot(candidate);
  } catch {
    fail(code);
  }
}

function revisionAfter(value, increments, code) {
  canonicalRevision(value, code);
  let revision;
  try {
    revision = callIntrinsic(
      bigIntToStringIntrinsic,
      BigIntConstructor(value) + BigIntConstructor(increments),
      [],
    );
  } catch {
    fail(code);
  }
  canonicalRevision(revision, code);
  return revision;
}

function activeOperationPointer(operation, reservation) {
  return {
    conflictClass: SESSION_OPERATION_CONFLICT_CLASS,
    expectedSessionRevision: operation.expectedSession.revision,
    kind: operation.kind,
    operationId: operation.operationId,
    operationRevision: operation.revision,
    requestSha256: operation.requestSha256,
    reservationId: reservation.reservationId,
    state: operation.state,
  };
}

function lastOperationPointer(operation, reservation, code) {
  return {
    conflictClass: SESSION_OPERATION_CONFLICT_CLASS,
    expectedSessionRevision: operation.expectedSession.revision,
    kind: operation.kind,
    operationId: operation.operationId,
    operationRevision: operation.revision,
    requestSha256: operation.requestSha256,
    reservationId: reservation.reservationId,
    resultSha256: sha256(canonicalSerialize(operation.result, code)),
    state: operation.state,
  };
}

function validateActiveOperationSession(
  session,
  operation,
  reservation,
  expectedDocument,
  code,
) {
  const activeOperation = exactDataObject(
    session.document.activeOperation,
    ACTIVE_OPERATION_POINTER_KEYS,
    code,
  );
  ensure(
    operation.state !== "committed" &&
      reservation.state === operation.state &&
      reservation.createdAt === operation.createdAt &&
      reservation.updatedAt === operation.updatedAt &&
      session.sessionId === operation.sessionId &&
      session.createdAt === operation.expectedSession.createdAt &&
      session.updatedAt === operation.updatedAt &&
      session.revision ===
        revisionAfter(
          operation.expectedSession.revision,
          BigIntConstructor(operation.revision) + 1n,
          code,
        ) &&
      sameJson(
        activeOperation,
        activeOperationPointer(operation, reservation),
        code,
      ) &&
      sameJson(session.document, expectedDocument, code),
    code,
  );
}

function validateLaunchOperationTiming(operation, reservation, code) {
  const createdAt = timestampMilliseconds(operation.createdAt, code);
  const updatedAt = timestampMilliseconds(operation.updatedAt, code);
  ensure(
    (operation.state === "prepared"
      ? updatedAt === createdAt
      : updatedAt >= createdAt) &&
      reservation.createdAt === operation.createdAt &&
      reservation.updatedAt === operation.updatedAt,
    code,
  );
}

function validateActivationOperationBindingAndTiming(
  operation,
  reservation,
  code,
) {
  ensure(
    timestampMilliseconds(operation.updatedAt, code) >=
        timestampMilliseconds(operation.createdAt, code) &&
      reservation.createdAt === operation.createdAt &&
      reservation.updatedAt === operation.updatedAt,
    code,
  );
}

function normalizeWriterLaunchCancellationResult(value, code) {
  const result = exactDataObject(value, CANCELLATION_RESULT_KEYS, code);
  const canonicalResult = {
    resultVersion: 1,
    outcome: "cancelled-before-dispatch",
    reason: WRITER_LAUNCH_PRE_DISPATCH_CANCELLATION_REASON,
  };
  ensure(
    result.resultVersion === canonicalResult.resultVersion &&
      result.outcome === canonicalResult.outcome &&
      result.reason === canonicalResult.reason &&
      sameJson(result, canonicalResult, code),
    code,
  );
  return canonicalResult;
}

function normalizeWriterLaunchTerminalResult(
  value,
  launchOperation,
  code,
) {
  const result = exactDataObject(value, WRITER_LAUNCH_RESULT_KEYS, code);
  const evidence = exactDataObject(
    result.evidence,
    WRITER_LAUNCH_EVIDENCE_KEYS,
    code,
  );
  const status = evidence.status;
  const outcome =
    status === "started"
      ? "writer-launch-started"
      : status === "not-started"
        ? "writer-launch-not-started"
        : status === "complete-stopped"
          ? "writer-launch-complete-stopped"
          : null;
  const processIncarnationId = evidence.processIncarnationId;
  const writerIncarnationId = evidence.writerIncarnationId;
  ensure(
    result.resultVersion === 1 &&
      result.outcome === outcome &&
      evidence.contractVersion === 1 &&
      evidence.launchAttemptId === launchOperation.operationId &&
      evidence.supervisorId ===
        launchOperation.request.supervisor.supervisorId &&
      ((status === "not-started" &&
        processIncarnationId === null &&
        writerIncarnationId === null) ||
        ((status === "started" || status === "complete-stopped") &&
          processIncarnationId !== null &&
          writerIncarnationId !== null)),
    code,
  );
  opaqueId(evidence.proofId, code);
  if (processIncarnationId !== null) opaqueId(processIncarnationId, code);
  if (writerIncarnationId !== null) opaqueId(writerIncarnationId, code);
  const canonicalEvidence = {
    contractVersion: 1,
    launchAttemptId: launchOperation.operationId,
    processIncarnationId,
    proofId: evidence.proofId,
    status,
    supervisorId: launchOperation.request.supervisor.supervisorId,
    writerIncarnationId,
  };
  const canonicalResult = {
    evidence: canonicalEvidence,
    outcome,
    resultVersion: 1,
  };
  ensure(
    sameJson(evidence, canonicalEvidence, code) &&
      sameJson(result, canonicalResult, code),
    code,
  );
  return canonicalResult;
}

function writerLaunchPointer(launchOperation, result, code) {
  const request = launchOperation.request;
  const evidence = result.evidence;
  ensure(result.outcome === "writer-launch-started", code);
  return {
    attachmentId: request.attachment.attachmentId,
    attachmentSha256: sha256(
      canonicalSerialize(request.attachment, code),
    ),
    contractVersion: 1,
    fencingEpoch: request.fencingEpoch,
    generation: request.generation,
    launchAttemptId: launchOperation.operationId,
    launchResultSha256: sha256(canonicalSerialize(result, code)),
    leaseId: request.lease.leaseId,
    leaseSha256: sha256(canonicalSerialize(request.lease, code)),
    measuredImageSha256: sha256(
      canonicalSerialize(request.measuredImage, code),
    ),
    processIncarnationId: evidence.processIncarnationId,
    startedAt: launchOperation.updatedAt,
    supervisorId: request.supervisor.supervisorId,
    supervisorProofId: evidence.proofId,
    writerIncarnationId: evidence.writerIncarnationId,
  };
}

function validateCommittedLaunchSession(
  session,
  launchOperation,
  reservation,
  code,
) {
  const cancelledBeforeDispatch =
    launchOperation.result?.outcome === "cancelled-before-dispatch";
  const result = cancelledBeforeDispatch
    ? normalizeWriterLaunchCancellationResult(
        launchOperation.result,
        code,
      )
    : normalizeWriterLaunchTerminalResult(
        launchOperation.result,
        launchOperation,
        code,
      );
  if (cancelledBeforeDispatch) {
    ensure(
      launchOperation.revision === "1" &&
        timestampMilliseconds(launchOperation.updatedAt, code) >=
          timestampMilliseconds(
            launchOperation.request.lease.expiresAt,
            code,
          ),
      code,
    );
  } else {
    ensure(
      launchOperation.revision === "2" ||
        launchOperation.revision === "3",
      code,
    );
  }
  const expectedLaunch =
    result.outcome === "writer-launch-started"
      ? writerLaunchPointer(launchOperation, result, code)
      : launchOperation.expectedSession.document.launch;
  if (expectedLaunch !== null) {
    exactDataObject(expectedLaunch, WRITER_LAUNCH_POINTER_KEYS, code);
  }
  const expectedDocument = authorityDocumentWithState(
    launchOperation.expectedSession.document,
    {
      activeOperation: null,
      lastOperation: lastOperationPointer(
        launchOperation,
        reservation,
        code,
      ),
      launch: expectedLaunch,
    },
    code,
  );
  const terminalRevision = revisionAfter(
    launchOperation.expectedSession.revision,
    BigIntConstructor(launchOperation.revision) + 1n,
    code,
  );
  const atTerminalRevision =
    BigIntConstructor(session.revision) ===
    BigIntConstructor(terminalRevision);
  ensure(
    reservation.state === "released" &&
      reservation.releasedAt === launchOperation.updatedAt &&
      launchOperation.retiredAt === launchOperation.updatedAt &&
      session.sessionId === launchOperation.sessionId &&
      session.createdAt === launchOperation.expectedSession.createdAt &&
      BigIntConstructor(session.revision) >=
        BigIntConstructor(terminalRevision) &&
      sameJson(
        session.document.manifest,
        launchOperation.expectedSession.document.manifest,
        code,
      ) &&
      sameJson(
        session.document.storageRef,
        launchOperation.expectedSession.document.storageRef,
        code,
      ) &&
      sameJson(
        session.document.backendCapabilities,
        launchOperation.expectedSession.document.backendCapabilities,
        code,
      ) &&
      (!atTerminalRevision ||
        (session.updatedAt === launchOperation.updatedAt &&
          sameJson(session.document, expectedDocument, code))),
    code,
  );
}

function validateActivationReadSession(
  session,
  activationRequest,
  operation,
  reservation,
  code,
) {
  if (operation.state === "committed") return;
  const expectedDocument = authorityDocumentWithState(
    operation.expectedSession.document,
    {
      activeOperation: activeOperationPointer(operation, reservation),
      attachment: null,
      launch: null,
      lease: activationRequest.lease,
      lifecycle: "ATTACHING",
      writerEpoch: activationRequest.lease.fencingEpoch,
    },
    code,
  );
  const expectedSession = authoritySnapshotWithState(
    operation.expectedSession,
    {
      document: expectedDocument,
      revision: revisionAfter(
        operation.expectedSession.revision,
        BigIntConstructor(operation.revision) + 1n,
        code,
      ),
      updatedAt: operation.updatedAt,
    },
    code,
  );
  validateActiveOperationSession(
    session,
    operation,
    reservation,
    expectedSession.document,
    code,
  );
}

function validateCommittedActivationSession(
  session,
  activationRequest,
  activationResult,
  operation,
  reservation,
  code,
) {
  const terminalSession = activationTerminalSession(
    operation,
    reservation,
    activationRequest,
    activationResult,
    code,
  );
  const atTerminalRevision =
    BigIntConstructor(session.revision) ===
    BigIntConstructor(terminalSession.revision);
  ensure(
    session.sessionId === operation.sessionId &&
      session.createdAt === operation.expectedSession.createdAt &&
      BigIntConstructor(session.revision) >=
        BigIntConstructor(terminalSession.revision) &&
      sameJson(
        session.document.manifest,
        operation.expectedSession.document.manifest,
        code,
      ) &&
      sameJson(
        session.document.storageRef,
        operation.expectedSession.document.storageRef,
        code,
      ) &&
      sameJson(
        session.document.backendCapabilities,
        operation.expectedSession.document.backendCapabilities,
        code,
      ) &&
      (!atTerminalRevision ||
        (session.updatedAt === operation.updatedAt &&
          sameJson(session.document, terminalSession.document, code))),
    code,
  );
}

function activationTerminalSession(
  operation,
  reservation,
  activationRequest,
  activationResult,
  code,
) {
  const expectedDocument = authorityDocumentWithState(
    operation.expectedSession.document,
    {
      activeOperation: null,
      attachment: activationResult.attachment,
      lastOperation: lastOperationPointer(operation, reservation, code),
      launch: null,
      lease: activationRequest.lease,
      lifecycle: "ATTACHED",
      writerEpoch: activationRequest.lease.fencingEpoch,
    },
    code,
  );
  return authoritySnapshotWithState(
    operation.expectedSession,
    {
      document: expectedDocument,
      revision: revisionAfter(
        operation.expectedSession.revision,
        BigIntConstructor(operation.revision) + 1n,
        code,
      ),
      updatedAt: operation.updatedAt,
    },
    code,
  );
}

function validateActivationCommitTransition(
  read,
  operation,
  reservation,
  activationRequest,
  activationResult,
  code,
) {
  const previousOperation = read.operation;
  const previousReservation = read.reservation;
  if (previousOperation.state === "committed") {
    ensure(
      sameJson(operation, previousOperation, code) &&
        sameJson(reservation, previousReservation, code),
      code,
    );
    return activationTerminalSession(
      operation,
      reservation,
      activationRequest,
      activationResult,
      code,
    );
  }
  ensure(
    previousOperation.state !== "committed" &&
      operation.state === "committed" &&
      operation.conflictClass === previousOperation.conflictClass &&
      operation.operationId === previousOperation.operationId &&
      operation.sessionId === previousOperation.sessionId &&
      operation.kind === previousOperation.kind &&
      operation.requestSha256 === previousOperation.requestSha256 &&
      operation.createdAt === previousOperation.createdAt &&
      operation.revision ===
        revisionAfter(previousOperation.revision, 1, code) &&
      sameJson(
        operation.expectedSession,
        previousOperation.expectedSession,
        code,
      ) &&
      sameJson(operation.request, previousOperation.request, code) &&
      reservation.reservationId === previousReservation.reservationId &&
      reservation.createdAt === previousReservation.createdAt &&
      reservation.updatedAt === operation.updatedAt &&
      reservation.releasedAt === operation.updatedAt,
    code,
  );
  return activationTerminalSession(
    operation,
    reservation,
    activationRequest,
    activationResult,
    code,
  );
}

function normalizeOperation(value, expected, code) {
  const operation = exactDataObject(value, OPERATION_KEYS, code);
  const expectedSession = normalizeSnapshot(
    operation.expectedSession,
    operation.sessionId,
    code,
  );
  let binding;
  try {
    binding = assertSessionOperationBinding({
      expectedSession: operation.expectedSession,
      kind: operation.kind,
      operationId: operation.operationId,
      request: operation.request,
    });
  } catch {
    fail(code);
  }
  ensure(
    operation.conflictClass === SESSION_OPERATION_CONFLICT_CLASS &&
      operation.operationId === expected.operationId &&
      operation.kind === expected.kind &&
      operation.sessionId === expectedSession.sessionId &&
      arrayIncludes(expected.states, operation.state) &&
      sameJson(operation.expectedSession, binding.expectedSession, code) &&
      sameJson(operation.request, expected.request, code) &&
      operation.requestSha256 === binding.requestSha256,
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
  return exactFrozenRecord({ binding, operation });
}

function normalizeReservation(value, operation, binding, code) {
  const reservation = exactDataObject(value, RESERVATION_KEYS, code);
  ensure(
    reservation.conflictClass === SESSION_OPERATION_CONFLICT_CLASS &&
      reservation.operationId === operation.operationId &&
      reservation.sessionId === operation.sessionId &&
      reservation.kind === operation.kind &&
      reservation.expectedSessionRevision ===
        operation.expectedSession.revision &&
      reservation.requestSha256 === operation.requestSha256 &&
      reservation.reservationId === binding.reservationId &&
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

function operationJournalBindingSha256(value, code) {
  return sha256(
    `portable-codex-operation-journal-binding-v1\0${canonicalSerialize(
      canonicalJsonData(value, code),
      code,
    )}\n`,
  );
}

function normalizeCheckpointArtifactProof(value, code) {
  const proof = exactDataObject(
    value,
    CHECKPOINT_ARTIFACT_PROOF_KEYS,
    code,
  );
  canonicalSha256(proof.artifactManifestDigest, code);
  opaqueId(proof.captureOperationId, code);
  canonicalSha256(proof.modeledDigest, code);
  const canonical = {
    artifactManifestDigest: proof.artifactManifestDigest,
    captureOperationId: proof.captureOperationId,
    modeledDigest: proof.modeledDigest,
  };
  ensure(sameJson(proof, canonical, code), code);
  return canonical;
}

function normalizePublicationMaterialization(
  value,
  artifactProof,
  publicationKind,
  coordinatorBinding,
  code,
  requireCanonicalOrder = true,
) {
  const restoreDestination = publicationKind === "restore-destination";
  const materialization = exactDataObject(
    value,
    restoreDestination
      ? RESTORE_MATERIALIZATION_KEYS
      : CHECKPOINT_MATERIALIZATION_KEYS,
    code,
  );
  const stagedRoot = exactDataObject(
    materialization.stagedRoot,
    STAGED_ROOT_KEYS,
    code,
  );
  canonicalSha256(materialization.artifactManifestDigest, code);
  canonicalSha256(materialization.modeledDigest, code);
  canonicalSha256(materialization.treeIdentityDigest, code);
  opaqueId(materialization.publicationId, code);
  opaqueId(stagedRoot.filesystemId, code);
  opaqueId(stagedRoot.objectIdentityScheme, code);
  ensure(
    regexpTest(PERSISTENT_OBJECT_ID_PATTERN, stagedRoot.objectId) &&
      materialization.artifactManifestDigest ===
        artifactProof.artifactManifestDigest &&
      materialization.modeledDigest === artifactProof.modeledDigest &&
      materialization.publicationKind === publicationKind &&
      materialization.contractVersion ===
        (restoreDestination ? 3 : 2),
    code,
  );
  if (restoreDestination) {
    canonicalSha256(materialization.coordinatorBindingSha256, code);
    ensure(
      materialization.coordinatorBindingSha256 ===
        operationJournalBindingSha256(coordinatorBinding, code),
      code,
    );
  }
  const canonicalStagedRoot = {
    filesystemId: stagedRoot.filesystemId,
    objectIdentityScheme: stagedRoot.objectIdentityScheme,
    objectId: stagedRoot.objectId,
  };
  const canonical = {
    artifactManifestDigest: materialization.artifactManifestDigest,
    ...(restoreDestination
      ? {
          coordinatorBindingSha256:
            materialization.coordinatorBindingSha256,
        }
      : {}),
    contractVersion: materialization.contractVersion,
    modeledDigest: materialization.modeledDigest,
    publicationId: materialization.publicationId,
    publicationKind,
    stagedRoot: canonicalStagedRoot,
    treeIdentityDigest: materialization.treeIdentityDigest,
  };
  if (requireCanonicalOrder) {
    ensure(sameJson(materialization, canonical, code), code);
  }
  return canonical;
}

function normalizeCatalogueDocument(value, candidate, code) {
  const document = exactDataObject(
    value,
    CATALOGUE_DOCUMENT_KEYS,
    code,
  );
  const artifactProof = normalizeCheckpointArtifactProof(
    document.artifactProof,
    code,
  );
  const materialization = normalizePublicationMaterialization(
    document.materialization,
    artifactProof,
    "checkpoint-artifact",
    null,
    code,
  );
  const result = exactDataObject(
    document.result,
    CHECKPOINT_CAPTURE_RESULT_KEYS,
    code,
  );
  let resultCheckpoint;
  try {
    resultCheckpoint = assertCheckpointDescriptor(result.checkpoint);
  } catch {
    fail(code);
  }
  ensure(sameJson(resultCheckpoint, candidate.checkpoint, code), code);
  const rawMutation = exactDataObject(
    result.mutation,
    STORAGE_MUTATION_RESULT_KEYS,
    code,
  );
  const captureRequestValue = {
    backendId: rawMutation.backendId,
    contractVersion: rawMutation.contractVersion,
    fencingEpoch: rawMutation.fencingEpoch,
    holderId: rawMutation.holderId,
    leaseId: rawMutation.leaseId,
    operation: rawMutation.operation,
    operationId: rawMutation.operationId,
    sessionId: rawMutation.sessionId,
    storageId: rawMutation.storageId,
    target: rawMutation.target,
  };
  let captureRequest;
  let captureResult;
  try {
    captureRequest = assertStorageMutationRequest(captureRequestValue);
    captureResult = assertStorageMutationResult(rawMutation, {
      request: captureRequest,
    });
  } catch {
    fail(code);
  }
  const checkpoint = candidate.checkpoint;
  ensure(
    captureRequest.operation === "checkpoint" &&
      captureRequest.operationId === artifactProof.captureOperationId &&
      captureRequest.sessionId === checkpoint.sessionId &&
      captureRequest.backendId === checkpoint.backendId &&
      captureRequest.storageId === checkpoint.storageId &&
      captureRequest.fencingEpoch === checkpoint.sourceFencingEpoch &&
      captureRequest.target.artifactId === checkpoint.artifactId &&
      captureRequest.target.checkpointId === checkpoint.checkpointId &&
      captureResult.proofId ===
        `proof-checkpoint-${sha256(
          `checkpoint-capture-proof:${captureRequest.operationId}`,
        )}`,
    code,
  );
  const canonicalMutation = {
    backendId: captureResult.backendId,
    contractVersion: captureResult.contractVersion,
    fencingEpoch: captureResult.fencingEpoch,
    holderId: captureResult.holderId,
    leaseId: captureResult.leaseId,
    operation: captureResult.operation,
    operationId: captureResult.operationId,
    proofId: captureResult.proofId,
    sessionId: captureResult.sessionId,
    status: captureResult.status,
    storageId: captureResult.storageId,
    target: captureResult.target,
  };
  const canonicalResult = {
    checkpoint: candidate.checkpoint,
    mutation: canonicalMutation,
  };
  const canonical = {
    artifactProof,
    contractVersion: 1,
    materialization,
    result: canonicalResult,
  };
  ensure(
    document.contractVersion === 1 &&
      sameJson(result, canonicalResult, code) &&
      sameJson(document, canonical, code),
    code,
  );
  return canonical;
}

function normalizeCatalogue(value, candidate, code) {
  const catalogue = exactDataObject(value, CATALOGUE_KEYS, code);
  const document = normalizeCatalogueDocument(
    catalogue.document,
    candidate,
    code,
  );
  canonicalUuid(catalogue.captureAttemptId, code);
  canonicalTimestamp(catalogue.committedAt, code);
  const canonical = {
    captureAttemptId: catalogue.captureAttemptId,
    checkpointId: candidate.checkpoint.checkpointId,
    committedAt: catalogue.committedAt,
    document,
    sessionId: candidate.checkpoint.sessionId,
  };
  ensure(sameJson(catalogue, canonical, code), code);
  return canonical;
}

function normalizeGenerationOperationRequest(
  value,
  expectedSession,
  candidate,
  code,
) {
  const request = exactDataObject(
    value,
    GENERATION_OPERATION_REQUEST_KEYS,
    code,
  );
  exactDataObject(request.admission, GENERATION_ADMISSION_KEYS, code);
  let canonical;
  try {
    canonical = createRestoreDestinationGenerationOperationRequest({
      admission: {
        checkpoint: candidate.checkpoint,
        request: candidate.request,
      },
      expectedSession,
    });
  } catch {
    fail(code);
  }
  ensure(sameJson(request, canonical, code), code);
  return canonical;
}

function normalizeGenerationBinding(
  value,
  operation,
  reservation,
  candidate,
  catalogue,
  generationId,
  code,
) {
  const binding = exactDataObject(value, GENERATION_BINDING_KEYS, code);
  opaqueId(binding.destinationIsolationProofId, code);
  const canonical = canonicalJsonData({
    attachment: operation.expectedSession.document.attachment,
    captureAttemptId: catalogue.captureAttemptId,
    captureOperationId:
      catalogue.document.artifactProof.captureOperationId,
    catalogueSha256: sha256(canonicalSerialize(catalogue.document, code)),
    checkpoint: candidate.checkpoint,
    contractVersion: 1,
    destinationIsolationProofId: binding.destinationIsolationProofId,
    destinationState: "detached",
    generationId,
    request: candidate.request,
    reservationId: reservation.reservationId,
  }, code);
  ensure(sameJson(binding, canonical, code), code);
  return canonical;
}

function normalizeGenerationDocument(
  value,
  operation,
  catalogue,
  generationBinding,
  code,
) {
  const document = exactDataObject(
    value,
    GENERATION_DOCUMENT_KEYS,
    code,
  );
  const artifactProof = normalizeCheckpointArtifactProof(
    document.artifactProof,
    code,
  );
  ensure(
    sameJson(artifactProof, catalogue.document.artifactProof, code),
    code,
  );
  const materialization = normalizePublicationMaterialization(
    document.materialization,
    artifactProof,
    "restore-destination",
    generationBinding,
    code,
  );
  const canonical = {
    artifactProof,
    contractVersion:
      RESTORE_DESTINATION_GENERATION_DOCUMENT_CONTRACT_VERSION,
    materialization,
    result: operation.request.predeterminedResult,
  };
  ensure(
    document.contractVersion ===
        RESTORE_DESTINATION_GENERATION_DOCUMENT_CONTRACT_VERSION &&
      sameJson(document, canonical, code),
    code,
  );
  return canonical;
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
  if (generation.state === "authorized") {
    ensure(generation.committedAt === null && generation.document === null, code);
  } else {
    ensure(
      generation.state === "committed" &&
        generation.committedAt !== null &&
        generation.document !== null,
      code,
    );
  }
  return generation;
}

function normalizeGenerationTerminalResult(
  value,
  candidate,
  catalogue,
  generation,
  code,
) {
  const result = exactDataObject(
    value,
    GENERATION_TERMINAL_RESULT_KEYS,
    code,
  );
  const canonical = {
    catalogueSha256: sha256(canonicalSerialize(catalogue.document, code)),
    checkpointId: candidate.checkpoint.checkpointId,
    generationDocumentSha256: sha256(
      canonicalSerialize(generation.document, code),
    ),
    generationId: generation.generationId,
    outcome: "restore-generation-committed",
    resultVersion: 1,
  };
  ensure(sameJson(result, canonical, code), code);
  return canonical;
}

function validateGenerationTiming(operation, reservation, generation, code) {
  const createdAt = timestampMilliseconds(operation.createdAt, code);
  const updatedAt = timestampMilliseconds(operation.updatedAt, code);
  const claimedAt = timestampMilliseconds(generation.claimedAt, code);
  ensure(
    updatedAt >= createdAt &&
      reservation.createdAt === operation.createdAt &&
      reservation.updatedAt === operation.updatedAt &&
      claimedAt >= createdAt &&
      claimedAt <= updatedAt &&
      (operation.state !== "starting" ||
        generation.claimedAt === operation.updatedAt),
    code,
  );
  if (operation.state === "committed") {
    ensure(
      generation.committedAt === operation.updatedAt &&
        timestampMilliseconds(generation.committedAt, code) >= claimedAt,
      code,
    );
  }
}

function validateCommittedGenerationSession(
  session,
  operation,
  reservation,
  code,
) {
  const expectedDocument = authorityDocumentWithState(
    operation.expectedSession.document,
    {
      activeOperation: null,
      lastOperation: lastOperationPointer(operation, reservation, code),
    },
    code,
  );
  const terminalRevision = revisionAfter(
    operation.expectedSession.revision,
    BigIntConstructor(operation.revision) + 1n,
    code,
  );
  const atTerminalRevision = session.revision === terminalRevision;
  ensure(
    session.sessionId === operation.sessionId &&
      session.createdAt === operation.expectedSession.createdAt &&
      BigIntConstructor(session.revision) >=
        BigIntConstructor(terminalRevision) &&
      sameJson(
        session.document.manifest,
        operation.expectedSession.document.manifest,
        code,
      ) &&
      sameJson(
        session.document.storageRef,
        operation.expectedSession.document.storageRef,
        code,
      ) &&
      sameJson(
        session.document.backendCapabilities,
        operation.expectedSession.document.backendCapabilities,
        code,
      ) &&
      (!atTerminalRevision ||
        (session.updatedAt === operation.updatedAt &&
          sameJson(session.document, expectedDocument, code))),
    code,
  );
}

function validateCommittedGenerationTransition(
  previous,
  operation,
  reservation,
  generation,
  code,
) {
  if (previous === null) return;
  const previousOperation = previous.operation;
  const previousReservation = previous.reservation;
  const previousGeneration = previous.generation;
  ensure(
    previousOperation.state === "uncertain" &&
      operation.revision ===
        revisionAfter(previousOperation.revision, 1, code) &&
      operation.conflictClass === previousOperation.conflictClass &&
      operation.operationId === previousOperation.operationId &&
      operation.sessionId === previousOperation.sessionId &&
      operation.kind === previousOperation.kind &&
      operation.requestSha256 === previousOperation.requestSha256 &&
      operation.createdAt === previousOperation.createdAt &&
      sameJson(operation.expectedSession, previousOperation.expectedSession, code) &&
      sameJson(operation.request, previousOperation.request, code) &&
      reservation.reservationId === previousReservation.reservationId &&
      reservation.createdAt === previousReservation.createdAt &&
      generation.claimedAt === previousGeneration.claimedAt &&
      sameJson(generation.binding, previousGeneration.binding, code),
    code,
  );
}

function normalizeGenerationReceipt(
  value,
  candidate,
  { completion = null, finalized = false, previous = null } = {},
  code,
) {
  const receipt = exactDataObject(
    value,
    finalized
      ? GENERATION_FINALIZE_RECEIPT_KEYS
      : GENERATION_READ_RECEIPT_KEYS,
    code,
  );
  const rawOperation = exactDataObject(receipt.operation, OPERATION_KEYS, code);
  const request = normalizeGenerationOperationRequest(
    rawOperation.request,
    rawOperation.expectedSession,
    candidate,
    code,
  );
  const normalizedOperation = normalizeOperation(
    receipt.operation,
    {
      kind: RESTORE_DESTINATION_GENERATION_OPERATION_KIND,
      operationId: candidate.request.operationId,
      request,
      states: ["starting", "uncertain", "committed"],
    },
    code,
  );
  const operation = normalizedOperation.operation;
  const catalogue = normalizeCatalogue(receipt.catalogue, candidate, code);
  const reservation = normalizeReservation(
    receipt.reservation,
    operation,
    normalizedOperation.binding,
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
  const generationBinding = normalizeGenerationBinding(
    generation.binding,
    operation,
    reservation,
    candidate,
    catalogue,
    generation.generationId,
    code,
  );
  const session = normalizeCanonicalSnapshot(
    receipt.session,
    operation.sessionId,
    code,
  );
  validateGenerationTiming(operation, reservation, generation, code);
  ensure(
    receipt.status === (finalized ? operation.state : generation.state),
    code,
  );
  if (finalized) ensure(typeof receipt.finalized === "boolean", code);
  if (operation.state === "committed") {
    ensure(operation.revision === "2" || operation.revision === "3", code);
    const generationDocument = normalizeGenerationDocument(
      generation.document,
      operation,
      catalogue,
      generationBinding,
      code,
    );
    generation.document = generationDocument;
    normalizeGenerationTerminalResult(
      operation.result,
      candidate,
      catalogue,
      generation,
      code,
    );
    if (completion !== null) {
      const completionMaterialization = normalizePublicationMaterialization(
        completion.materialization,
        generationDocument.artifactProof,
        "restore-destination",
        generationBinding,
        code,
        false,
      );
      ensure(
        sameJson(generationDocument.materialization, completionMaterialization, code) &&
          sameJson(generationDocument.result, completion.result, code),
        code,
      );
    }
    validateCommittedGenerationTransition(
      previous,
      operation,
      reservation,
      generation,
      code,
    );
    validateCommittedGenerationSession(session, operation, reservation, code);
  } else {
    ensure(
      operation.state === "starting" || operation.state === "uncertain",
      code,
    );
    validateActiveOperationSession(
      session,
      operation,
      reservation,
      authorityDocumentWithState(
        operation.expectedSession.document,
        {
          activeOperation: activeOperationPointer(operation, reservation),
        },
        code,
      ),
      code,
    );
  }
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
  let createRequest;
  if (request.contractVersion === 1) {
    createRequest = createRestoreAttachmentActivationOperationRequest;
  } else if (request.contractVersion === 2) {
    createRequest = createRestoreAttachmentActivationOperationRequestV2;
  } else {
    fail(code);
  }
  let canonical;
  try {
    canonical = createRequest({
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
      sameCanonicalJson(
        result.activationRequest,
        activationRequest,
        code,
      ) &&
      sameCanonicalJson(
        result.activationResult,
        activationResult,
        code,
      ),
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
    sameCanonicalJson(
      activationRequest.manifest,
      expectedSession.document.manifest,
      code,
    ) &&
      sameCanonicalJson(
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
  const normalizedOperation = normalizeOperation(
    receipt.operation,
    {
      kind: RESTORE_ATTACHMENT_ACTIVATION_OPERATION_KIND,
      operationId: candidate.activationOperationId,
      request,
      states: ["starting", "uncertain", "committed"],
    },
    code,
  );
  const operation = normalizedOperation.operation;
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
  const reservation = normalizeReservation(
    receipt.reservation,
    operation,
    normalizedOperation.binding,
    code,
  );
  const session = normalizeCanonicalSnapshot(
    receipt.session,
    operation.sessionId,
    code,
  );
  validateActivationOperationBindingAndTiming(
    operation,
    reservation,
    code,
  );
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
    ensure(
      operation.revision === "2" || operation.revision === "3",
      code,
    );
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
    validateCommittedActivationSession(
      session,
      activationRequest,
      terminalActivationResult,
      operation,
      reservation,
      code,
    );
  } else {
    validateActivationReadSession(
      session,
      activationRequest,
      operation,
      reservation,
      code,
    );
  }
  return exactFrozenRecord({
    activationRequest,
    generation,
    operation,
    reservation,
    session,
    status: receipt.status,
  });
}

function normalizeActivationHandoffReceipt(
  value,
  candidate,
  activationRequest,
  activationResult,
  read,
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
  const normalizedOperation = normalizeOperation(
    activation.operation,
    {
      kind: RESTORE_ATTACHMENT_ACTIVATION_OPERATION_KIND,
      operationId: candidate.activationOperationId,
      request,
      states: ["committed"],
    },
    code,
  );
  const operation = normalizedOperation.operation;
  normalizeActivationTerminalResult(
    operation.result,
    activationRequest,
    activationResult,
    code,
  );
  const activationReservation = normalizeReservation(
    activation.reservation,
    operation,
    normalizedOperation.binding,
    code,
  );
  validateActivationOperationBindingAndTiming(
    operation,
    activationReservation,
    code,
  );
  ensure(typeof activation.finalized === "boolean", code);
  if (read.operation.state === "committed") {
    ensure(activation.finalized === false, code);
  }
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
  ensure(sameJson(generation, read.generation, code), code);
  const expectedLaunchSession = validateActivationCommitTransition(
    read,
    operation,
    activationReservation,
    activationRequest,
    activationResult,
    code,
  );
  let expectedLaunchRequest;
  try {
    expectedLaunchRequest = createWriterLaunchAttemptOperationRequest({
      expectedSession: expectedLaunchSession,
      generation: read.generation,
      measuredImage: request.launchIntent.measuredImage,
      supervisor: request.launchIntent.supervisor,
    });
  } catch {
    fail(code);
  }
  const launch = exactDataObject(receipt.launch, LAUNCH_RECEIPT_KEYS, code);
  const normalizedLaunchOperation = normalizeOperation(
    launch.operation,
    {
      kind: WRITER_LAUNCH_ATTEMPT_OPERATION_KIND,
      operationId: request.launchIntent.launchAttemptId,
      request: expectedLaunchRequest,
      states: ["prepared", "starting", "uncertain", "committed"],
    },
    code,
  );
  const launchOperation = normalizedLaunchOperation.operation;
  ensure(
    activation.finalized === false ||
      (launchOperation.state === "prepared" &&
        launchOperation.revision === "0"),
    code,
  );
  ensure(
    sameJson(
      launchOperation.expectedSession,
      expectedLaunchSession,
      code,
    ) &&
      launchOperation.createdAt === operation.updatedAt,
    code,
  );
  const launchReservation = normalizeReservation(
    launch.reservation,
    launchOperation,
    normalizedLaunchOperation.binding,
    code,
  );
  ensure(
    launchReservation.createdAt === launchOperation.createdAt,
    code,
  );
  validateLaunchOperationTiming(
    launchOperation,
    launchReservation,
    code,
  );
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
  const session = normalizeCanonicalSnapshot(
    receipt.session,
    operation.sessionId,
    code,
  );
  if (launchOperation.state === "committed") {
    validateCommittedLaunchSession(
      session,
      launchOperation,
      launchReservation,
      code,
    );
  } else {
    validateActiveOperationSession(
      session,
      launchOperation,
      launchReservation,
      authorityDocumentWithState(
        expectedLaunchSession.document,
        {
          activeOperation: activeOperationPointer(
            launchOperation,
            launchReservation,
          ),
        },
        code,
      ),
      code,
    );
  }
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
  checkpoint = clonePlainData(canonicalJsonData(checkpoint, code), code);
  request = clonePlainData(canonicalJsonData(request, code), code);
  let newerFencingEpoch;
  try {
    newerFencingEpoch =
      BigIntConstructor(request.fencingEpoch) >
      BigIntConstructor(checkpoint.sourceFencingEpoch);
  } catch {
    fail(code);
  }
  ensure(
    request.operation === "restore" &&
      request.sessionId === checkpoint.sessionId &&
      request.backendId === checkpoint.backendId &&
      newerFencingEpoch &&
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

  async function invokeAuthoritySettlement(method, input) {
    let pending;
    try {
      pending = callIntrinsic(authority[method], authority.receiver, [input]);
    } catch {
      throw authorityInvocationUncertain;
    }
    return awaitTrustedSettlement(
      settleTrusted(pending, outcomeCode),
      outcomeCode,
      authorityInvocationUncertain,
    );
  }

  function cloneAuthoritySettlement(settled) {
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

  async function invokeAuthority(method, input) {
    let settlement;
    try {
      settlement = await invokeAuthoritySettlement(method, input);
    } catch (error) {
      if (error === authorityInvocationUncertain) fail(outcomeCode);
      throw error;
    }
    return cloneAuthoritySettlement(settlement.value);
  }

  async function invokeActivationFinalizerWithOneRetry(input) {
    try {
      return await invokeAuthoritySettlement(
        "finalizeRestoreAttachmentActivationAndReserveWriterLaunchAttempt",
        input,
      );
    } catch (error) {
      if (error !== authorityInvocationUncertain) throw error;
    }
    try {
      return await invokeAuthoritySettlement(
        "finalizeRestoreAttachmentActivationAndReserveWriterLaunchAttempt",
        input,
      );
    } catch (error) {
      if (error === authorityInvocationUncertain) fail(outcomeCode);
      throw error;
    }
  }

  async function finalizeActivationHandoff(
    read,
    candidate,
    activationRequest,
    activationResult,
    expectedOperationRevision,
  ) {
    const input = exactFrozenRecord({
      ...operationTransitionInput(
        read.operation,
        expectedOperationRevision,
        outcomeCode,
      ),
      activationResult,
    });
    const settlement = await invokeActivationFinalizerWithOneRetry(input);
    return normalizeActivationHandoffReceipt(
      cloneAuthoritySettlement(settlement.value),
      candidate,
      activationRequest,
      activationResult,
      read,
      outcomeCode,
    );
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
    const settlement = await awaitTrustedSettlement(
      settleTrusted(pending, outcomeCode),
      outcomeCode,
    );
    return normalizeDestination(settlement.value, outcomeCode);
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
    await awaitTrustedSettlement(
      settleTrusted(pending, outcomeCode),
      outcomeCode,
    );
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
        async (probeValue, completeValue) => {
          const probe = exactDataObject(
            probeValue,
            OPERATION_GUARD_PROBE_KEYS,
            outcomeCode,
          );
          const assertHeld = trustedFunction(
            probe.assertHeld,
            outcomeCode,
          );
          const complete = trustedFunction(completeValue, outcomeCode);
          ensure(objectIsFrozen(completeValue), outcomeCode);
          return callIntrinsic(complete, undefined, [
            await callback(assertHeld),
          ]);
        },
      ]);
    } catch {
      fail(outcomeCode);
    }
    return (
      await awaitTrustedSettlement(
        settleTrusted(pending, outcomeCode),
        outcomeCode,
      )
    ).value;
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
        {},
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
          {},
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
      let finalizationSettlement;
      try {
        finalizationSettlement = await invokeAuthoritySettlement(
          "finalizeRestoreDestinationGeneration",
          {
            ...operationTransitionInput(read.operation, "2", outcomeCode),
            completion,
          },
        );
      } catch (error) {
        if (error !== authorityInvocationUncertain) throw error;
        const replay = normalizeGenerationReceipt(
          await invokeAuthority("readRestoreDestinationGeneration", {
            checkpoint: candidate.checkpoint,
            generationId: candidate.generationId,
            request: candidate.request,
          }),
          candidate,
          { completion, previous: read },
          outcomeCode,
        );
        ensure(replay.operation.state === "committed", outcomeCode);
        return replay;
      }
      const finalized = normalizeGenerationReceipt(
        cloneAuthoritySettlement(finalizationSettlement.value),
        candidate,
        { completion, finalized: true, previous: read },
        outcomeCode,
      );
      ensure(finalized.operation.state === "committed", outcomeCode);
      return finalized;
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
      if (read.operation.state === "committed") {
        return finalizeActivationHandoff(
          read,
          candidate,
          read.activationRequest,
          read.operation.result.activationResult,
          revisionAfter(read.operation.revision, -1, outcomeCode),
        );
      }
      if (read.operation.state === "starting") {
        await markStartingUncertain(read);
        read = normalizeActivationReadReceipt(
          await invokeAuthority("readRestoreAttachmentActivation", {
            operationId: candidate.activationOperationId,
          }),
          candidate,
          outcomeCode,
        );
        if (read.operation.state === "committed") {
          return finalizeActivationHandoff(
            read,
            candidate,
            read.activationRequest,
            read.operation.result.activationResult,
            revisionAfter(read.operation.revision, -1, outcomeCode),
          );
        }
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
      const completionMaterialization = normalizePublicationMaterialization(
        completion.materialization,
        read.generation.document.artifactProof,
        "restore-destination",
        read.generation.binding,
        outcomeCode,
        false,
      );
      ensure(
        sameJson(
          completionMaterialization,
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
      rawActivationResult = (
        await awaitTrustedSettlement(
          settleTrusted(rawActivationResult, outcomeCode),
          outcomeCode,
        )
      ).value;
      let activationResult;
      try {
        activationResult = assertRestoreAttachmentActivationResult(
          rawActivationResult,
          { request: activationRequest },
        );
      } catch {
        fail(outcomeCode);
      }
      activationResult = clonePlainData(
        canonicalJsonData(activationResult, outcomeCode),
        outcomeCode,
      );
      await probeGuard(probe);
      return finalizeActivationHandoff(
        read,
        candidate,
        activationRequest,
        activationResult,
        "2",
      );
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
