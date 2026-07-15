import {
  basename as pathBasenameExport,
  dirname as pathDirnameExport,
  isAbsolute as pathIsAbsoluteExport,
  parse as pathParseExport,
  resolve as pathResolveExport,
} from "node:path";
import { types as utilTypes } from "node:util";

import {
  CHECKPOINT_CAPTURE_RECONCILIATION_CONTRACT_VERSION,
  STORAGE_CONTRACT_VERSION,
  assertCanonicalFenceMatch,
  assertCheckpointDescriptor,
  assertLeaseGrant,
  assertSessionAttachment,
  assertSessionStorageRef,
  assertStorageBackend,
  assertStorageMutationMatchesLeaseSnapshot,
  assertStorageMutationRequest,
  assertStorageMutationResult,
} from "./session-storage-contracts.mjs";
import { StoppedDirectoryPublication } from "./stopped-directory-publication.mjs";
import { StoppedWriterCapabilityCoordinator } from "./stopped-writer-capability.mjs";

const arrayEveryIntrinsic = Array.prototype.every;
const arrayIncludesIntrinsic = Array.prototype.includes;
const arrayIsArray = Array.isArray;
const BigIntConstructor = BigInt;
const DateConstructor = Date;
const dateParseIntrinsic = Date.parse;
const dateToISOStringIntrinsic = Date.prototype.toISOString;
const functionToStringIntrinsic = Function.prototype.toString;
const numberIsFinite = Number.isFinite;
const numberIsSafeInteger = Number.isSafeInteger;
const objectCreate = Object.create;
const objectDefineProperty = Object.defineProperty;
const objectFreeze = Object.freeze;
const objectGetOwnPropertyDescriptor = Object.getOwnPropertyDescriptor;
const objectGetPrototypeOf = Object.getPrototypeOf;
const objectHasOwn = Object.hasOwn;
const objectKeys = Object.keys;
const objectPrototype = Object.prototype;
const pathBasename = pathBasenameExport;
const pathDirname = pathDirnameExport;
const pathIsAbsolute = pathIsAbsoluteExport;
const pathParse = pathParseExport;
const pathResolve = pathResolveExport;
const PromiseConstructor = Promise;
const reflectApply = Reflect.apply;
const reflectOwnKeys = Reflect.ownKeys;
const regexpExecIntrinsic = RegExp.prototype.exec;
const stringStartsWithIntrinsic = String.prototype.startsWith;
const {
  isAsyncFunction: isAsyncFunctionValue,
  isGeneratorFunction: isGeneratorFunctionValue,
  isGeneratorObject: isGeneratorObjectValue,
  isPromise: isPromiseValue,
  isProxy: isProxyValue,
} = utilTypes;
const TypeErrorConstructor = TypeError;

const consumeCapabilityIntrinsic =
  StoppedWriterCapabilityCoordinator.prototype.consumeCapability;
const publishFreshCheckpointArtifactIntrinsic =
  StoppedDirectoryPublication.prototype.publishFreshCheckpointArtifact;
const publishRestoreDestinationIntrinsic =
  StoppedDirectoryPublication.prototype.publishRestoreDestination;
const verifyCommittedCheckpointArtifactIntrinsic =
  StoppedDirectoryPublication.prototype.verifyCommittedCheckpointArtifact;

function callIntrinsic(intrinsic, receiver, args) {
  return reflectApply(intrinsic, receiver, args);
}

function arrayEvery(value, callback) {
  return callIntrinsic(arrayEveryIntrinsic, value, [callback]);
}

function arrayIncludes(value, candidate) {
  return callIntrinsic(arrayIncludesIntrinsic, value, [candidate]);
}

function regexpTest(pattern, value) {
  return callIntrinsic(regexpExecIntrinsic, pattern, [value]) !== null;
}

function stringStartsWith(value, prefix) {
  return callIntrinsic(stringStartsWithIntrinsic, value, [prefix]);
}

const OPAQUE_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u;
const PERSISTENT_OBJECT_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$/u;
const DIGEST_PATTERN = /^[0-9a-f]{64}$/u;
const FENCING_EPOCH_PATTERN = /^[1-9][0-9]{0,19}$/u;
const OCI_DIGEST_PATTERN = /^sha256:[0-9a-f]{64}$/u;
const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u;
const NUL_PATTERN = /\0/u;
const NATIVE_FUNCTION_SOURCE_PATTERN =
  /\{\s*\[\s*native\s+code\s*\]\s*\}\s*$/u;
const UINT64_MAX = 18_446_744_073_709_551_615n;

const ATTACHMENT_RECORD_KEYS = objectFreeze([
  "attachmentId",
  "backendId",
  "contractVersion",
  "fencingEpoch",
  "holderId",
  "kind",
  "leaseId",
  "mode",
  "operationId",
  "proofId",
  "rootPath",
  "sessionId",
  "storageId",
]);
const CHECKPOINT_RECORD_KEYS = objectFreeze([
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
const LEASE_RECORD_KEYS = objectFreeze([
  "contractVersion",
  "expiresAt",
  "fencingEpoch",
  "holderId",
  "leaseId",
  "sessionId",
]);
const STORAGE_REF_RECORD_KEYS = objectFreeze([
  "backendId",
  "contractVersion",
  "sessionId",
  "storageId",
]);
const MUTATION_REQUEST_RECORD_KEYS = objectFreeze([
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
const MUTATION_RESULT_RECORD_KEYS = objectFreeze([
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
const MUTATION_TARGET_RECORD_KEYS = objectFreeze([
  "artifactId",
  "checkpointId",
  "kind",
]);

const CAPTURE_KEYS = objectFreeze([
  "attachment",
  "checkpoint",
  "request",
  "stoppedWriterEvidence",
]);
const CAPTURE_RECONCILIATION_KEYS = objectFreeze(["checkpoint", "request"]);
const RESTORE_KEYS = objectFreeze(["checkpoint", "request"]);
const RESOLVED_WRITER_KEYS = objectFreeze([
  "canonicalLeaseAtRegistration",
  "processIncarnationId",
  "stopOperationId",
  "writer",
  "writerIncarnationId",
]);
const COORDINATOR_BINDING_KEYS = objectFreeze([
  "attachment",
  "processIncarnationId",
  "stopOperationId",
  "writerFence",
  "writerIncarnationId",
]);
const WRITER_FENCE_KEYS = objectFreeze([
  "contractVersion",
  "fencingEpoch",
  "holderId",
  "leaseId",
  "sessionId",
]);
const CAPTURE_CONTEXT_KEYS = objectFreeze([
  "artifactDirectory",
  "artifactOwnedRoot",
  "canonicalAttachment",
  "canonicalLease",
  "captureAttemptId",
  "now",
  "reservationId",
  "result",
  "sourceDirectory",
  "sourceOwnedRoot",
  "storageRef",
]);
const CAPTURE_RECONCILIATION_CONTEXT_KEYS = objectFreeze([
  "artifactDirectory",
  "artifactOwnedRoot",
  "captureAttempt",
]);
const CAPTURE_ATTEMPT_RECORD_KEYS = objectFreeze([
  "binding",
  "captureAttemptId",
  "contractVersion",
  "operationId",
  "request",
  "result",
  "state",
]);
const CAPTURE_JOURNAL_BINDING_KEYS = objectFreeze([
  "attachmentId",
  "attachmentOperationId",
  "attachmentProofId",
  "captureAttemptId",
  "checkpoint",
  "contractVersion",
  "processIncarnationId",
  "reservationId",
  "stopOperationId",
  "writerIncarnationId",
]);
const RESTORE_CONTEXT_KEYS = objectFreeze([
  "artifactDirectory",
  "artifactOwnedRoot",
  "artifactProof",
  "canonicalLease",
  "destinationDirectory",
  "destinationIsolationProofId",
  "destinationOwnedRoot",
  "destinationState",
  "now",
  "reservationId",
  "result",
  "storageRef",
]);
const RESULT_KEYS = objectFreeze(["checkpoint", "mutation"]);
const PUBLICATION_OUTCOME_KEYS = objectFreeze([
  "materialization",
  "replayed",
  "result",
]);
const MATERIALIZATION_KEYS = objectFreeze([
  "artifactManifestDigest",
  "contractVersion",
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
const ARTIFACT_PROOF_KEYS = objectFreeze([
  "artifactManifestDigest",
  "captureOperationId",
  "modeledDigest",
]);
const LIFECYCLE_METHODS = objectFreeze([
  "destroySession",
  "detachAttachment",
  "forceFence",
  "prepareWritableAttachment",
  "provisionSession",
]);

const ERROR_MESSAGES = objectFreeze({
  invalid_stopped_directory_backend_request:
    "Stopped-directory backend request is invalid",
  stopped_directory_backend_outcome_uncertain:
    "Stopped-directory backend outcome is uncertain",
});

const CAPTURE_JOURNAL_BINDING_CONTRACT_VERSION = 2;
const RESTORE_JOURNAL_BINDING_CONTRACT_VERSION = 1;

export const STOPPED_DIRECTORY_BACKEND_CONTRACT_VERSION = 2;

export class StoppedDirectoryBackendError extends Error {
  constructor(code) {
    if (typeof code !== "string" || !objectHasOwn(ERROR_MESSAGES, code)) {
      throw new TypeErrorConstructor(
        "unsupported stopped-directory backend error code",
      );
    }
    const message = ERROR_MESSAGES[code];
    super(message);
    objectDefineProperty(this, "name", {
      configurable: true,
      enumerable: true,
      value: "StoppedDirectoryBackendError",
      writable: true,
    });
    objectDefineProperty(this, "code", {
      configurable: true,
      enumerable: true,
      value: code,
      writable: true,
    });
    objectDefineProperty(this, "retryable", {
      configurable: true,
      enumerable: true,
      value: false,
      writable: true,
    });
    objectDefineProperty(this, "stack", {
      configurable: false,
      enumerable: false,
      value: `StoppedDirectoryBackendError: ${message}`,
      writable: false,
    });
    objectFreeze(this);
  }
}

function makeError(code) {
  return new StoppedDirectoryBackendError(code);
}

function failInvalid() {
  throw makeError("invalid_stopped_directory_backend_request");
}

function failUncertain() {
  throw makeError("stopped_directory_backend_outcome_uncertain");
}

function ensureInvalid(condition) {
  if (!condition) failInvalid();
}

function ensureUncertain(condition) {
  if (!condition) failUncertain();
}

function assertExactDataObject(value, keys, failure = failInvalid) {
  if (
    isProxyValue(value) ||
    value === null ||
    typeof value !== "object" ||
    arrayIsArray(value)
  ) {
    failure();
  }

  let prototype;
  let actual;
  try {
    prototype = objectGetPrototypeOf(value);
    actual = reflectOwnKeys(value);
  } catch {
    failure();
  }
  if (
    (prototype !== objectPrototype && prototype !== null) ||
    actual.length !== keys.length ||
    !arrayEvery(
      actual,
      (key) => typeof key === "string" && arrayIncludes(keys, key),
    )
  ) {
    failure();
  }

  const normalized = objectCreate(null);
  for (let index = 0; index < actual.length; index += 1) {
    const key = actual[index];
    let descriptor;
    try {
      descriptor = objectGetOwnPropertyDescriptor(value, key);
    } catch {
      failure();
    }
    if (
      descriptor?.enumerable !== true ||
      !objectHasOwn(descriptor, "value")
    ) {
      failure();
    }
    normalized[key] = descriptor.value;
  }
  return normalized;
}

function assertTrustedFunction(value, failure = failInvalid) {
  if (
    typeof value !== "function" ||
    isProxyValue(value) ||
    isGeneratorFunctionValue(value)
  ) {
    failure();
  }
  return value;
}

function assertTrustedSynchronousFunction(value, failure = failInvalid) {
  const operation = assertTrustedFunction(value, failure);
  if (isAsyncFunctionValue(operation)) failure();
  let source;
  try {
    source = callIntrinsic(functionToStringIntrinsic, operation, []);
  } catch {
    failure();
  }
  if (regexpTest(NATIVE_FUNCTION_SOURCE_PATTERN, source)) failure();
  return operation;
}

function assertOpaqueId(value, failure = failInvalid) {
  if (typeof value !== "string" || !regexpTest(OPAQUE_ID_PATTERN, value)) {
    failure();
  }
  return value;
}

function assertUuid(value, failure) {
  if (typeof value !== "string" || !regexpTest(UUID_PATTERN, value)) {
    failure();
  }
  return value;
}

function parseFencingEpoch(value, failure) {
  if (
    typeof value !== "string" ||
    !regexpTest(FENCING_EPOCH_PATTERN, value)
  ) {
    failure();
  }
  let epoch;
  try {
    epoch = BigIntConstructor(value);
  } catch {
    failure();
  }
  if (epoch > UINT64_MAX) failure();
  return epoch;
}

function assertCanonicalTimestamp(value, failure) {
  if (typeof value !== "string") failure();
  let timestamp;
  let canonical;
  try {
    timestamp = callIntrinsic(dateParseIntrinsic, DateConstructor, [value]);
    canonical = callIntrinsic(
      dateToISOStringIntrinsic,
      new DateConstructor(timestamp),
      [],
    );
  } catch {
    failure();
  }
  if (!numberIsFinite(timestamp) || canonical !== value) failure();
  return timestamp;
}

function assertRobustAttachment(value, failure) {
  assertExactDataObject(value, ATTACHMENT_RECORD_KEYS, failure);
  if (
    value.contractVersion !== STORAGE_CONTRACT_VERSION ||
    value.kind !== "directory" ||
    value.mode !== "read-write"
  ) {
    failure();
  }
  assertOpaqueId(value.attachmentId, failure);
  assertOpaqueId(value.backendId, failure);
  assertOpaqueId(value.holderId, failure);
  assertOpaqueId(value.leaseId, failure);
  assertOpaqueId(value.operationId, failure);
  assertOpaqueId(value.proofId, failure);
  assertOpaqueId(value.storageId, failure);
  assertUuid(value.sessionId, failure);
  parseFencingEpoch(value.fencingEpoch, failure);
  assertCanonicalDirectory(value.rootPath, failure);
}

function assertRobustCheckpoint(value, failure) {
  assertExactDataObject(value, CHECKPOINT_RECORD_KEYS, failure);
  if (
    value.contractVersion !== STORAGE_CONTRACT_VERSION ||
    value.checkpointClass !== "clean" ||
    typeof value.imageDigest !== "string" ||
    !regexpTest(OCI_DIGEST_PATTERN, value.imageDigest)
  ) {
    failure();
  }
  assertOpaqueId(value.artifactId, failure);
  assertOpaqueId(value.backendId, failure);
  assertOpaqueId(value.checkpointId, failure);
  assertOpaqueId(value.storageId, failure);
  assertUuid(value.codexSessionId, failure);
  assertUuid(value.codexThreadId, failure);
  assertUuid(value.sessionId, failure);
  parseFencingEpoch(value.sourceFencingEpoch, failure);
  assertCanonicalTimestamp(value.createdAt, failure);
}

function assertRobustLease(value, failure) {
  assertExactDataObject(value, LEASE_RECORD_KEYS, failure);
  if (value.contractVersion !== STORAGE_CONTRACT_VERSION) failure();
  assertUuid(value.sessionId, failure);
  assertOpaqueId(value.holderId, failure);
  assertOpaqueId(value.leaseId, failure);
  parseFencingEpoch(value.fencingEpoch, failure);
  return assertCanonicalTimestamp(value.expiresAt, failure);
}

function assertRobustStorageRef(value, failure) {
  assertExactDataObject(value, STORAGE_REF_RECORD_KEYS, failure);
  if (value.contractVersion !== STORAGE_CONTRACT_VERSION) failure();
  assertOpaqueId(value.backendId, failure);
  assertOpaqueId(value.storageId, failure);
  assertUuid(value.sessionId, failure);
}

function assertRobustMutationRequest(value, failure) {
  assertExactDataObject(value, MUTATION_REQUEST_RECORD_KEYS, failure);
  const target = assertExactDataObject(
    value.target,
    MUTATION_TARGET_RECORD_KEYS,
    failure,
  );
  if (
    value.contractVersion !== STORAGE_CONTRACT_VERSION ||
    !arrayIncludes(["checkpoint", "restore"], value.operation) ||
    target.kind !== "checkpoint"
  ) {
    failure();
  }
  assertOpaqueId(value.backendId, failure);
  assertOpaqueId(value.holderId, failure);
  assertOpaqueId(value.leaseId, failure);
  assertOpaqueId(value.operationId, failure);
  assertOpaqueId(value.storageId, failure);
  assertOpaqueId(target.artifactId, failure);
  assertOpaqueId(target.checkpointId, failure);
  assertUuid(value.sessionId, failure);
  parseFencingEpoch(value.fencingEpoch, failure);
}

function assertRobustMutationResult(value, request, failure) {
  assertExactDataObject(value, MUTATION_RESULT_RECORD_KEYS, failure);
  const target = assertExactDataObject(
    value.target,
    MUTATION_TARGET_RECORD_KEYS,
    failure,
  );
  assertOpaqueId(value.proofId, failure);
  if (
    value.backendId !== request.backendId ||
    value.contractVersion !== request.contractVersion ||
    value.fencingEpoch !== request.fencingEpoch ||
    value.holderId !== request.holderId ||
    value.leaseId !== request.leaseId ||
    value.operation !== request.operation ||
    value.operationId !== request.operationId ||
    value.sessionId !== request.sessionId ||
    value.storageId !== request.storageId ||
    target.artifactId !== request.target.artifactId ||
    target.checkpointId !== request.target.checkpointId ||
    target.kind !== request.target.kind ||
    value.status !==
      (request.operation === "checkpoint" ? "checkpoint-created" : "restored")
  ) {
    failure();
  }
}

function assertOpaqueHandle(value, failure = failInvalid) {
  if (
    value === null ||
    typeof value !== "object" ||
    isProxyValue(value) ||
    arrayIsArray(value)
  ) {
    failure();
  }
  return value;
}

function runContractValidator(operation) {
  try {
    return operation();
  } catch {
    failInvalid();
  }
}

function runRuntimeValidator(operation) {
  try {
    return operation();
  } catch {
    failUncertain();
  }
}

function sameFlatRecord(left, right) {
  const leftKeys = objectKeys(left);
  const rightKeys = objectKeys(right);
  return (
    leftKeys.length === rightKeys.length &&
    arrayEvery(
      leftKeys,
      (key) => objectHasOwn(right, key) && left[key] === right[key],
    )
  );
}

function sameResult(left, right) {
  const leftMutationKeys = objectKeys(left.mutation);
  const rightMutationKeys = objectKeys(right.mutation);
  return (
    sameFlatRecord(left.checkpoint, right.checkpoint) &&
    leftMutationKeys.length === rightMutationKeys.length &&
    arrayEvery(
      leftMutationKeys,
      (key) =>
        objectHasOwn(right.mutation, key) &&
        (key === "target" || left.mutation[key] === right.mutation[key]),
    ) &&
    sameFlatRecord(left.mutation.target, right.mutation.target)
  );
}

function sameMutationRequest(left, right) {
  const leftKeys = objectKeys(left);
  const rightKeys = objectKeys(right);
  return (
    leftKeys.length === rightKeys.length &&
    arrayEvery(
      leftKeys,
      (key) =>
        objectHasOwn(right, key) &&
        (key === "target" || left[key] === right[key]),
    ) &&
    sameFlatRecord(left.target, right.target)
  );
}

function sameCaptureJournalBinding(left, right) {
  const leftKeys = objectKeys(left);
  const rightKeys = objectKeys(right);
  return (
    leftKeys.length === rightKeys.length &&
    arrayEvery(
      leftKeys,
      (key) =>
        objectHasOwn(right, key) &&
        (key === "checkpoint" || left[key] === right[key]),
    ) &&
    sameFlatRecord(left.checkpoint, right.checkpoint)
  );
}

function exactFrozenRecord(value) {
  return objectFreeze(value);
}

function normalizeResult(value, request, failure) {
  const envelope = assertExactDataObject(value, RESULT_KEYS, failure);
  let checkpoint;
  let mutation;
  try {
    checkpoint = assertCheckpointDescriptor(envelope.checkpoint);
    mutation = assertStorageMutationResult(envelope.mutation, { request });
  } catch {
    failure();
  }
  assertRobustCheckpoint(checkpoint, failure);
  assertRobustMutationResult(mutation, request, failure);
  return exactFrozenRecord({ checkpoint, mutation });
}

function assertCheckpointEquals(left, right, failure) {
  if (!sameFlatRecord(left, right)) failure();
}

function normalizeCaptureRequest(value, backendId) {
  const options = assertExactDataObject(value, CAPTURE_KEYS);
  const stoppedWriterEvidence = assertOpaqueHandle(options.stoppedWriterEvidence);
  const attachment = runContractValidator(() =>
    assertSessionAttachment(options.attachment),
  );
  const checkpoint = runContractValidator(() =>
    assertCheckpointDescriptor(options.checkpoint),
  );
  const request = runContractValidator(() =>
    assertStorageMutationRequest(options.request),
  );
  assertRobustAttachment(attachment, failInvalid);
  assertRobustCheckpoint(checkpoint, failInvalid);
  assertRobustMutationRequest(request, failInvalid);

  ensureInvalid(
    attachment.backendId === backendId &&
      attachment.backendId === checkpoint.backendId &&
      attachment.backendId === request.backendId &&
      attachment.storageId === checkpoint.storageId &&
      attachment.storageId === request.storageId &&
      attachment.sessionId === checkpoint.sessionId &&
      attachment.sessionId === request.sessionId &&
      attachment.leaseId === request.leaseId &&
      attachment.holderId === request.holderId &&
      attachment.fencingEpoch === request.fencingEpoch &&
      attachment.fencingEpoch === checkpoint.sourceFencingEpoch &&
      checkpoint.checkpointClass === "clean" &&
      request.operation === "checkpoint" &&
      request.target.kind === "checkpoint" &&
      request.target.checkpointId === checkpoint.checkpointId &&
      request.target.artifactId === checkpoint.artifactId,
  );

  return exactFrozenRecord({
    attachment,
    checkpoint,
    request,
    stoppedWriterEvidence,
  });
}

function normalizeCaptureReconciliationRequest(value, backendId) {
  const options = assertExactDataObject(value, CAPTURE_RECONCILIATION_KEYS);
  const checkpoint = runContractValidator(() =>
    assertCheckpointDescriptor(options.checkpoint),
  );
  const request = runContractValidator(() =>
    assertStorageMutationRequest(options.request),
  );
  assertRobustCheckpoint(checkpoint, failInvalid);
  assertRobustMutationRequest(request, failInvalid);
  ensureInvalid(
    checkpoint.backendId === backendId &&
      request.backendId === backendId &&
      checkpoint.storageId === request.storageId &&
      checkpoint.sessionId === request.sessionId &&
      checkpoint.sourceFencingEpoch === request.fencingEpoch &&
      checkpoint.checkpointClass === "clean" &&
      request.operation === "checkpoint" &&
      request.target.kind === "checkpoint" &&
      request.target.checkpointId === checkpoint.checkpointId &&
      request.target.artifactId === checkpoint.artifactId,
  );
  return exactFrozenRecord({ checkpoint, request });
}

function normalizeRestoreRequest(value, backendId) {
  const options = assertExactDataObject(value, RESTORE_KEYS);
  const checkpoint = runContractValidator(() =>
    assertCheckpointDescriptor(options.checkpoint),
  );
  const request = runContractValidator(() =>
    assertStorageMutationRequest(options.request),
  );
  assertRobustCheckpoint(checkpoint, failInvalid);
  assertRobustMutationRequest(request, failInvalid);
  ensureInvalid(
    checkpoint.backendId === backendId &&
      request.backendId === backendId &&
      checkpoint.sessionId === request.sessionId &&
      checkpoint.checkpointClass === "clean" &&
      request.operation === "restore" &&
      request.target.kind === "checkpoint" &&
      request.target.checkpointId === checkpoint.checkpointId &&
      request.target.artifactId === checkpoint.artifactId &&
      parseFencingEpoch(request.fencingEpoch, failInvalid) >
        parseFencingEpoch(checkpoint.sourceFencingEpoch, failInvalid),
  );
  return exactFrozenRecord({ checkpoint, request });
}

function normalizeResolvedWriter(value, attachment) {
  const resolved = assertExactDataObject(
    value,
    RESOLVED_WRITER_KEYS,
    failUncertain,
  );
  const canonicalLeaseAtRegistration = runRuntimeValidator(() =>
    assertLeaseGrant(resolved.canonicalLeaseAtRegistration),
  );
  assertRobustLease(canonicalLeaseAtRegistration, failUncertain);
  const processIncarnationId = assertOpaqueId(
    resolved.processIncarnationId,
    failUncertain,
  );
  const stopOperationId = assertOpaqueId(
    resolved.stopOperationId,
    failUncertain,
  );
  const writer = assertOpaqueHandle(resolved.writer, failUncertain);
  const writerIncarnationId = assertOpaqueId(
    resolved.writerIncarnationId,
    failUncertain,
  );
  ensureUncertain(
    canonicalLeaseAtRegistration.sessionId === attachment.sessionId &&
      canonicalLeaseAtRegistration.leaseId === attachment.leaseId &&
      canonicalLeaseAtRegistration.holderId === attachment.holderId &&
      canonicalLeaseAtRegistration.fencingEpoch === attachment.fencingEpoch,
  );
  return exactFrozenRecord({
    canonicalLeaseAtRegistration,
    processIncarnationId,
    stopOperationId,
    writer,
    writerIncarnationId,
  });
}

function validateCoordinatorBinding(value, request, resolved) {
  const binding = assertExactDataObject(
    value,
    COORDINATOR_BINDING_KEYS,
    failUncertain,
  );
  const fence = assertExactDataObject(
    binding.writerFence,
    WRITER_FENCE_KEYS,
    failUncertain,
  );
  ensureUncertain(
    sameFlatRecord(binding.attachment, request.attachment) &&
      binding.processIncarnationId === resolved.processIncarnationId &&
      binding.stopOperationId === resolved.stopOperationId &&
      binding.writerIncarnationId === resolved.writerIncarnationId &&
      fence.contractVersion === resolved.canonicalLeaseAtRegistration.contractVersion &&
      fence.sessionId === resolved.canonicalLeaseAtRegistration.sessionId &&
      fence.leaseId === resolved.canonicalLeaseAtRegistration.leaseId &&
      fence.holderId === resolved.canonicalLeaseAtRegistration.holderId &&
      fence.fencingEpoch === resolved.canonicalLeaseAtRegistration.fencingEpoch,
  );
}

function assertCanonicalDirectory(value, failure) {
  if (
    typeof value !== "string" ||
    regexpTest(NUL_PATTERN, value) ||
    !pathIsAbsolute(value) ||
    pathResolve(value) !== value ||
    value === pathParse(value).root
  ) {
    failure();
  }
  return value;
}

function assertDirectPathPlan(directoryValue, ownedRootValue, failure) {
  const directory = assertCanonicalDirectory(directoryValue, failure);
  const ownedRoot = assertCanonicalDirectory(ownedRootValue, failure);
  const name = pathBasename(directory);
  if (
    pathDirname(directory) !== ownedRoot ||
    name.length === 0 ||
    name === "." ||
    name === ".." ||
    pathBasename(name) !== name
  ) {
    failure();
  }
  return exactFrozenRecord({ directory, ownedRoot });
}

function pathsAreDisjoint(left, right) {
  return (
    left !== right &&
    !stringStartsWith(left, `${right}/`) &&
    !stringStartsWith(right, `${left}/`)
  );
}

function normalizeStorageAndFence({
  canonicalLease: leaseValue,
  now,
  request,
  storageRef: storageValue,
}) {
  const canonicalLease = runRuntimeValidator(() => assertLeaseGrant(leaseValue));
  const storageRef = runRuntimeValidator(() =>
    assertSessionStorageRef(storageValue),
  );
  const expiration = assertRobustLease(canonicalLease, failUncertain);
  assertRobustStorageRef(storageRef, failUncertain);
  assertRobustMutationRequest(request, failUncertain);
  ensureUncertain(numberIsFinite(now));
  ensureUncertain(
    expiration > now &&
      request.sessionId === canonicalLease.sessionId &&
      request.leaseId === canonicalLease.leaseId &&
      request.holderId === canonicalLease.holderId &&
      request.fencingEpoch === canonicalLease.fencingEpoch &&
      request.sessionId === storageRef.sessionId &&
      request.backendId === storageRef.backendId &&
      request.storageId === storageRef.storageId,
  );
  runRuntimeValidator(() =>
    assertStorageMutationMatchesLeaseSnapshot({
      canonicalLease,
      now,
      request,
      storageRef,
    }),
  );
  return exactFrozenRecord({ canonicalLease, storageRef });
}

function normalizeMaterialization(value, kind, artifactProof = undefined) {
  const materialization = assertExactDataObject(
    value,
    MATERIALIZATION_KEYS,
    failUncertain,
  );
  const stagedRoot = assertExactDataObject(
    materialization.stagedRoot,
    STAGED_ROOT_KEYS,
    failUncertain,
  );
  ensureUncertain(
    numberIsSafeInteger(materialization.contractVersion) &&
      materialization.contractVersion > 0 &&
      typeof materialization.artifactManifestDigest === "string" &&
      regexpTest(DIGEST_PATTERN, materialization.artifactManifestDigest) &&
      typeof materialization.modeledDigest === "string" &&
      regexpTest(DIGEST_PATTERN, materialization.modeledDigest) &&
      typeof materialization.treeIdentityDigest === "string" &&
      regexpTest(DIGEST_PATTERN, materialization.treeIdentityDigest) &&
      typeof materialization.publicationId === "string" &&
      regexpTest(OPAQUE_ID_PATTERN, materialization.publicationId) &&
      materialization.publicationKind === kind &&
      typeof stagedRoot.filesystemId === "string" &&
      regexpTest(OPAQUE_ID_PATTERN, stagedRoot.filesystemId) &&
      typeof stagedRoot.objectIdentityScheme === "string" &&
      regexpTest(OPAQUE_ID_PATTERN, stagedRoot.objectIdentityScheme) &&
      typeof stagedRoot.objectId === "string" &&
      regexpTest(PERSISTENT_OBJECT_ID_PATTERN, stagedRoot.objectId) &&
      (artifactProof === undefined ||
        (materialization.artifactManifestDigest ===
          artifactProof.artifactManifestDigest &&
          materialization.modeledDigest === artifactProof.modeledDigest)),
  );
  return exactFrozenRecord({
    artifactManifestDigest: materialization.artifactManifestDigest,
    contractVersion: materialization.contractVersion,
    modeledDigest: materialization.modeledDigest,
    publicationId: materialization.publicationId,
    publicationKind: materialization.publicationKind,
    stagedRoot: exactFrozenRecord({
      filesystemId: stagedRoot.filesystemId,
      objectIdentityScheme: stagedRoot.objectIdentityScheme,
      objectId: stagedRoot.objectId,
    }),
    treeIdentityDigest: materialization.treeIdentityDigest,
  });
}

function normalizeArtifactProof(value) {
  const proof = assertExactDataObject(
    value,
    ARTIFACT_PROOF_KEYS,
    failUncertain,
  );
  ensureUncertain(
    typeof proof.artifactManifestDigest === "string" &&
      regexpTest(DIGEST_PATTERN, proof.artifactManifestDigest) &&
      typeof proof.captureOperationId === "string" &&
      regexpTest(OPAQUE_ID_PATTERN, proof.captureOperationId) &&
      typeof proof.modeledDigest === "string" &&
      regexpTest(DIGEST_PATTERN, proof.modeledDigest),
  );
  return exactFrozenRecord({
    artifactManifestDigest: proof.artifactManifestDigest,
    captureOperationId: proof.captureOperationId,
    modeledDigest: proof.modeledDigest,
  });
}

function normalizePublicationOutcome(
  value,
  expectedResult,
  request,
  kind,
  artifactProof,
) {
  const outcome = assertExactDataObject(
    value,
    PUBLICATION_OUTCOME_KEYS,
    failUncertain,
  );
  ensureUncertain(typeof outcome.replayed === "boolean");
  const result = normalizeResult(
    outcome.result,
    request,
    failUncertain,
  );
  ensureUncertain(sameResult(result, expectedResult));
  const materialization = normalizeMaterialization(
    outcome.materialization,
    kind,
    artifactProof,
  );
  return exactFrozenRecord({
    materialization,
    replayed: outcome.replayed,
    result: expectedResult,
  });
}

function normalizeCaptureAttempt(value, request) {
  const attempt = assertExactDataObject(
    value,
    CAPTURE_ATTEMPT_RECORD_KEYS,
    failUncertain,
  );
  const binding = assertExactDataObject(
    attempt.binding,
    CAPTURE_JOURNAL_BINDING_KEYS,
    failUncertain,
  );
  const captureAttemptId = assertOpaqueId(
    attempt.captureAttemptId,
    failUncertain,
  );
  const operationId = assertOpaqueId(attempt.operationId, failUncertain);
  const durableRequest = runRuntimeValidator(() =>
    assertStorageMutationRequest(attempt.request),
  );
  assertRobustMutationRequest(durableRequest, failUncertain);
  const result = normalizeResult(
    attempt.result,
    durableRequest,
    failUncertain,
  );
  const boundCheckpoint = runRuntimeValidator(() =>
    assertCheckpointDescriptor(binding.checkpoint),
  );
  assertRobustCheckpoint(boundCheckpoint, failUncertain);
  ensureUncertain(
    attempt.contractVersion ===
      CHECKPOINT_CAPTURE_RECONCILIATION_CONTRACT_VERSION &&
      arrayIncludes(["authorized", "committed"], attempt.state) &&
      operationId === request.request.operationId &&
      durableRequest.operation === "checkpoint" &&
      sameMutationRequest(durableRequest, request.request) &&
      sameFlatRecord(boundCheckpoint, request.checkpoint) &&
      sameFlatRecord(result.checkpoint, request.checkpoint) &&
      binding.contractVersion === CAPTURE_JOURNAL_BINDING_CONTRACT_VERSION &&
      binding.captureAttemptId === captureAttemptId,
  );
  for (const value of [
    binding.attachmentId,
    binding.attachmentOperationId,
    binding.attachmentProofId,
    binding.processIncarnationId,
    binding.reservationId,
    binding.stopOperationId,
    binding.writerIncarnationId,
  ]) {
    assertOpaqueId(value, failUncertain);
  }
  const normalizedBinding = exactFrozenRecord({
    attachmentId: binding.attachmentId,
    attachmentOperationId: binding.attachmentOperationId,
    attachmentProofId: binding.attachmentProofId,
    captureAttemptId,
    checkpoint: boundCheckpoint,
    contractVersion: binding.contractVersion,
    processIncarnationId: binding.processIncarnationId,
    reservationId: binding.reservationId,
    stopOperationId: binding.stopOperationId,
    writerIncarnationId: binding.writerIncarnationId,
  });
  ensureUncertain(sameCaptureJournalBinding(normalizedBinding, binding));
  return exactFrozenRecord({
    binding: normalizedBinding,
    captureAttemptId,
    contractVersion: attempt.contractVersion,
    operationId,
    request: durableRequest,
    result,
    state: attempt.state,
  });
}

function captureJournalBinding(context, request, resolved) {
  return exactFrozenRecord({
    attachmentId: request.attachment.attachmentId,
    attachmentOperationId: request.attachment.operationId,
    attachmentProofId: request.attachment.proofId,
    captureAttemptId: context.captureAttemptId,
    checkpoint: request.checkpoint,
    contractVersion: CAPTURE_JOURNAL_BINDING_CONTRACT_VERSION,
    processIncarnationId: resolved.processIncarnationId,
    reservationId: context.reservationId,
    stopOperationId: resolved.stopOperationId,
    writerIncarnationId: resolved.writerIncarnationId,
  });
}

function restoreJournalBinding(context, request) {
  return exactFrozenRecord({
    checkpoint: request.checkpoint,
    contractVersion: RESTORE_JOURNAL_BINDING_CONTRACT_VERSION,
    destinationIsolationProofId: context.destinationIsolationProofId,
    reservationId: context.reservationId,
  });
}

function isSafeAuthorityPromise(value) {
  if (
    !isPromiseValue(value) ||
    isProxyValue(value) ||
    isGeneratorObjectValue(value)
  ) {
    return false;
  }

  let current = value;
  while (current !== null) {
    if (isProxyValue(current)) return false;
    let descriptor;
    try {
      descriptor = objectGetOwnPropertyDescriptor(current, "constructor");
    } catch {
      return false;
    }
    if (descriptor !== undefined) {
      return (
        objectHasOwn(descriptor, "value") &&
        descriptor.value === PromiseConstructor
      );
    }
    try {
      current = objectGetPrototypeOf(current);
    } catch {
      return false;
    }
  }
  return false;
}

async function observeSafeNativePromiseRejection(value) {
  try {
    await value;
  } catch {
    // This side observer contains a same-realm native rejection without
    // replacing the original Promise or changing its settlement.
  }
}

async function runAuthorityMethod(authority, method, admission, publish) {
  let callbackCalls = 0;
  let callbackCompleted = false;
  let callbackResult;
  let open = true;

  const runGuardedPublish = async (context) => {
    callbackCalls += 1;
    if (!open || callbackCalls !== 1) failUncertain();
    const result = await publish(context);
    if (!open || callbackCalls !== 1) failUncertain();
    callbackResult = result;
    callbackCompleted = true;
    return result;
  };
  const guardedPublish = (context) => {
    const pending = runGuardedPublish(context);
    void observeSafeNativePromiseRejection(pending);
    return pending;
  };

  try {
    const pending = reflectApply(method, authority, [admission, guardedPublish]);
    ensureUncertain(isSafeAuthorityPromise(pending));
    const result = await pending;
    open = false;
    ensureUncertain(
      callbackCalls === 1 && callbackCompleted && result === callbackResult,
    );
    return callbackResult;
  } catch {
    open = false;
    failUncertain();
  }
}

function normalizeCaptureContext(value, request, resolved) {
  const context = assertExactDataObject(
    value,
    CAPTURE_CONTEXT_KEYS,
    failUncertain,
  );
  const canonicalAttachment = runRuntimeValidator(() =>
    assertSessionAttachment(context.canonicalAttachment),
  );
  assertRobustAttachment(canonicalAttachment, failUncertain);
  ensureUncertain(sameFlatRecord(canonicalAttachment, request.attachment));
  const { canonicalLease, storageRef } = normalizeStorageAndFence({
    canonicalLease: context.canonicalLease,
    now: context.now,
    request: request.request,
    storageRef: context.storageRef,
  });
  runRuntimeValidator(() =>
    assertCanonicalFenceMatch({
      canonical: canonicalLease,
      now: context.now,
      presented: resolved.canonicalLeaseAtRegistration,
    }),
  );
  ensureUncertain(
    canonicalLease.sessionId ===
      resolved.canonicalLeaseAtRegistration.sessionId &&
      canonicalLease.leaseId ===
        resolved.canonicalLeaseAtRegistration.leaseId &&
      canonicalLease.holderId ===
        resolved.canonicalLeaseAtRegistration.holderId &&
      canonicalLease.fencingEpoch ===
        resolved.canonicalLeaseAtRegistration.fencingEpoch &&
    storageRef.backendId === request.attachment.backendId &&
      storageRef.storageId === request.attachment.storageId &&
      storageRef.sessionId === request.attachment.sessionId,
  );
  const result = normalizeResult(context.result, request.request, failUncertain);
  assertCheckpointEquals(result.checkpoint, request.checkpoint, failUncertain);
  const source = assertDirectPathPlan(
    context.sourceDirectory,
    context.sourceOwnedRoot,
    failUncertain,
  );
  const artifact = assertDirectPathPlan(
    context.artifactDirectory,
    context.artifactOwnedRoot,
    failUncertain,
  );
  ensureUncertain(
    source.directory === canonicalAttachment.rootPath &&
      pathsAreDisjoint(source.ownedRoot, artifact.ownedRoot),
  );
  const captureAttemptId = assertOpaqueId(
    context.captureAttemptId,
    failUncertain,
  );
  const reservationId = assertOpaqueId(context.reservationId, failUncertain);
  return exactFrozenRecord({
    artifact,
    canonicalAttachment,
    canonicalLease,
    captureAttemptId,
    now: context.now,
    reservationId,
    result,
    source,
    storageRef,
  });
}

function normalizeCaptureReconciliationContext(value, request) {
  const context = assertExactDataObject(
    value,
    CAPTURE_RECONCILIATION_CONTEXT_KEYS,
    failUncertain,
  );
  const artifact = assertDirectPathPlan(
    context.artifactDirectory,
    context.artifactOwnedRoot,
    failUncertain,
  );
  const captureAttempt = normalizeCaptureAttempt(
    context.captureAttempt,
    request,
  );
  return exactFrozenRecord({ artifact, captureAttempt });
}

function normalizeRestoreContext(value, request) {
  const context = assertExactDataObject(
    value,
    RESTORE_CONTEXT_KEYS,
    failUncertain,
  );
  const { canonicalLease, storageRef } = normalizeStorageAndFence({
    canonicalLease: context.canonicalLease,
    now: context.now,
    request: request.request,
    storageRef: context.storageRef,
  });
  ensureUncertain(
    storageRef.backendId === request.checkpoint.backendId &&
      storageRef.sessionId === request.checkpoint.sessionId &&
      parseFencingEpoch(canonicalLease.fencingEpoch, failUncertain) >
        parseFencingEpoch(
          request.checkpoint.sourceFencingEpoch,
          failUncertain,
        ) &&
      context.destinationState === "detached",
  );
  const destinationIsolationProofId = assertOpaqueId(
    context.destinationIsolationProofId,
    failUncertain,
  );
  const reservationId = assertOpaqueId(context.reservationId, failUncertain);
  const artifactProof = normalizeArtifactProof(context.artifactProof);
  const result = normalizeResult(context.result, request.request, failUncertain);
  assertCheckpointEquals(result.checkpoint, request.checkpoint, failUncertain);
  const artifact = assertDirectPathPlan(
    context.artifactDirectory,
    context.artifactOwnedRoot,
    failUncertain,
  );
  const destination = assertDirectPathPlan(
    context.destinationDirectory,
    context.destinationOwnedRoot,
    failUncertain,
  );
  ensureUncertain(
    pathsAreDisjoint(artifact.ownedRoot, destination.ownedRoot),
  );
  return exactFrozenRecord({
    artifact,
    artifactProof,
    canonicalLease,
    destination,
    destinationIsolationProofId,
    now: context.now,
    reservationId,
    result,
    storageRef,
  });
}

export class StoppedDirectoryBackend {
  #authority;

  #coordinator;

  #lifecycleBackend;

  #lifecycleMethods;

  #publication;

  #resolveStoppedWriter;

  constructor(...args) {
    ensureInvalid(args.length === 1);
    const options = assertExactDataObject(args[0], [
      "backendId",
      "coordinator",
      "lifecycleBackend",
      "mutationAuthority",
      "publication",
      "resolveStoppedWriter",
    ]);
    const backendId = assertOpaqueId(options.backendId);
    ensureInvalid(
      !isProxyValue(options.coordinator) &&
        options.coordinator instanceof StoppedWriterCapabilityCoordinator,
    );
    ensureInvalid(
      !isProxyValue(options.publication) &&
        options.publication instanceof StoppedDirectoryPublication,
    );
    ensureInvalid(
      options.lifecycleBackend !== null &&
        typeof options.lifecycleBackend === "object" &&
        !isProxyValue(options.lifecycleBackend) &&
        !arrayIsArray(options.lifecycleBackend),
    );
    const lifecycleBackend = runContractValidator(() =>
      assertStorageBackend(options.lifecycleBackend),
    );
    const lifecycleBackendId = runContractValidator(
      () => lifecycleBackend.backendId,
    );
    ensureInvalid(lifecycleBackendId === backendId);
    const authorityOptions = assertExactDataObject(options.mutationAuthority, [
      "runCapture",
      "runCaptureReconciliation",
      "runRestore",
    ]);
    const authority = exactFrozenRecord({
      runCapture: assertTrustedFunction(authorityOptions.runCapture),
      runCaptureReconciliation: assertTrustedFunction(
        authorityOptions.runCaptureReconciliation,
      ),
      runRestore: assertTrustedFunction(authorityOptions.runRestore),
    });
    const lifecycleMethods = objectCreate(null);
    for (let index = 0; index < LIFECYCLE_METHODS.length; index += 1) {
      const method = LIFECYCLE_METHODS[index];
      lifecycleMethods[method] = runContractValidator(() =>
        assertTrustedFunction(lifecycleBackend[method]),
      );
    }

    this.#authority = authority;
    this.#coordinator = options.coordinator;
    this.#lifecycleBackend = lifecycleBackend;
    this.#lifecycleMethods = exactFrozenRecord(lifecycleMethods);
    this.#publication = options.publication;
    this.#resolveStoppedWriter = assertTrustedSynchronousFunction(
      options.resolveStoppedWriter,
    );
    objectDefineProperty(this, "backendId", {
      enumerable: true,
      value: backendId,
    });
    objectDefineProperty(this, "capabilities", {
      enumerable: true,
      value: exactFrozenRecord({
        atomicPointInTimeCheckpoint: false,
        exclusiveWriterAttachment: true,
        fencing: "manual",
        normalDirectoryAttachment: true,
      }),
    });
    objectDefineProperty(this, "contractVersion", {
      enumerable: true,
      value: STORAGE_CONTRACT_VERSION,
    });
    objectDefineProperty(this, "captureReconciliationContractVersion", {
      enumerable: true,
      value: CHECKPOINT_CAPTURE_RECONCILIATION_CONTRACT_VERSION,
    });
    objectFreeze(this);
  }

  async provisionSession(...args) {
    return this.#delegateLifecycle("provisionSession", args);
  }

  async prepareWritableAttachment(...args) {
    return this.#delegateLifecycle("prepareWritableAttachment", args);
  }

  async detachAttachment(...args) {
    return this.#delegateLifecycle("detachAttachment", args);
  }

  async forceFence(...args) {
    return this.#delegateLifecycle("forceFence", args);
  }

  async destroySession(...args) {
    return this.#delegateLifecycle("destroySession", args);
  }

  async captureCheckpoint(...args) {
    ensureInvalid(args.length === 1);
    const request = normalizeCaptureRequest(args[0], this.backendId);

    let resolved;
    try {
      const resolverInput = exactFrozenRecord({
        attachment: request.attachment,
        checkpoint: request.checkpoint,
        request: request.request,
      });
      const resolution = reflectApply(this.#resolveStoppedWriter, undefined, [
        resolverInput,
      ]);
      if (isPromiseValue(resolution)) {
        if (isSafeAuthorityPromise(resolution)) {
          void observeSafeNativePromiseRejection(resolution);
        }
        failUncertain();
      }
      ensureUncertain(!isGeneratorObjectValue(resolution));
      resolved = normalizeResolvedWriter(resolution, request.attachment);
    } catch {
      failUncertain();
    }

    const admission = exactFrozenRecord({
      attachment: request.attachment,
      checkpoint: request.checkpoint,
      processIncarnationId: resolved.processIncarnationId,
      request: request.request,
      stopOperationId: resolved.stopOperationId,
      writerIncarnationId: resolved.writerIncarnationId,
    });

    try {
      const completion = await callIntrinsic(
        consumeCapabilityIntrinsic,
        this.#coordinator,
        [
          exactFrozenRecord({
            attachment: request.attachment,
            canonicalLease: resolved.canonicalLeaseAtRegistration,
            capability: request.stoppedWriterEvidence,
            processIncarnationId: resolved.processIncarnationId,
            runSnapshot: async (binding) => {
              validateCoordinatorBinding(binding, request, resolved);
              return runAuthorityMethod(
                this.#authority,
                this.#authority.runCapture,
                admission,
                async (rawContext) => {
                  const context = normalizeCaptureContext(
                    rawContext,
                    request,
                    resolved,
                  );
                  const bindingRecord = captureJournalBinding(
                    context,
                    request,
                    resolved,
                  );
                  const publicationOutcome = await callIntrinsic(
                    publishFreshCheckpointArtifactIntrinsic,
                    this.#publication,
                    [
                      exactFrozenRecord({
                        artifactDirectory: context.artifact.directory,
                        artifactOwnedRoot: context.artifact.ownedRoot,
                        binding: bindingRecord,
                        operationId: request.request.operationId,
                        request: request.request,
                        result: context.result,
                        sourceDirectory: context.source.directory,
                        sourceOwnedRoot: context.source.ownedRoot,
                      }),
                    ],
                  );
                  const outcome = normalizePublicationOutcome(
                    publicationOutcome,
                    context.result,
                    request.request,
                    "checkpoint-artifact",
                  );
                  ensureUncertain(outcome.replayed === false);
                  const artifactProof = exactFrozenRecord({
                    artifactManifestDigest:
                      outcome.materialization.artifactManifestDigest,
                    captureOperationId: request.request.operationId,
                    modeledDigest: outcome.materialization.modeledDigest,
                  });
                  return exactFrozenRecord({
                    artifactProof,
                    materialization: outcome.materialization,
                    replayed: outcome.replayed,
                    result: context.result,
                  });
                },
              );
            },
            stopOperationId: resolved.stopOperationId,
            writer: resolved.writer,
            writerIncarnationId: resolved.writerIncarnationId,
          }),
        ],
      );
      ensureUncertain(
        completion !== null &&
          typeof completion === "object" &&
          !isProxyValue(completion) &&
          objectHasOwn(completion, "result"),
      );
      return completion.result;
    } catch {
      failUncertain();
    }
  }

  async reconcileCheckpointCapture(...args) {
    ensureInvalid(args.length === 1);
    const request = normalizeCaptureReconciliationRequest(
      args[0],
      this.backendId,
    );
    const admission = exactFrozenRecord({
      checkpoint: request.checkpoint,
      request: request.request,
    });

    try {
      const completion = await runAuthorityMethod(
        this.#authority,
        this.#authority.runCaptureReconciliation,
        admission,
        async (rawContext) => {
          const context = normalizeCaptureReconciliationContext(
            rawContext,
            request,
          );
          const publicationOutcome = await callIntrinsic(
            verifyCommittedCheckpointArtifactIntrinsic,
            this.#publication,
            [
              exactFrozenRecord({
                artifactDirectory: context.artifact.directory,
                artifactOwnedRoot: context.artifact.ownedRoot,
                binding: context.captureAttempt.binding,
                operationId: context.captureAttempt.operationId,
                request: context.captureAttempt.request,
                result: context.captureAttempt.result,
              }),
            ],
          );
          const outcome = normalizePublicationOutcome(
            publicationOutcome,
            context.captureAttempt.result,
            context.captureAttempt.request,
            "checkpoint-artifact",
          );
          ensureUncertain(outcome.replayed === true);
          const artifactProof = exactFrozenRecord({
            artifactManifestDigest:
              outcome.materialization.artifactManifestDigest,
            captureOperationId: context.captureAttempt.operationId,
            modeledDigest: outcome.materialization.modeledDigest,
          });
          return exactFrozenRecord({
            artifactProof,
            materialization: outcome.materialization,
            replayed: outcome.replayed,
            result: context.captureAttempt.result,
          });
        },
      );
      return completion.result;
    } catch {
      failUncertain();
    }
  }

  async restoreCheckpoint(...args) {
    ensureInvalid(args.length === 1);
    const request = normalizeRestoreRequest(args[0], this.backendId);
    const admission = exactFrozenRecord({
      checkpoint: request.checkpoint,
      request: request.request,
    });

    try {
      const completion = await runAuthorityMethod(
        this.#authority,
        this.#authority.runRestore,
        admission,
        async (rawContext) => {
          const context = normalizeRestoreContext(rawContext, request);
          const bindingRecord = restoreJournalBinding(context, request);
          const publicationOutcome = await callIntrinsic(
            publishRestoreDestinationIntrinsic,
            this.#publication,
            [
              exactFrozenRecord({
                artifactDirectory: context.artifact.directory,
                artifactOwnedRoot: context.artifact.ownedRoot,
                artifactProof: context.artifactProof,
                binding: bindingRecord,
                destinationDirectory: context.destination.directory,
                destinationOwnedRoot: context.destination.ownedRoot,
                operationId: request.request.operationId,
                request: request.request,
                result: context.result,
              }),
            ],
          );
          const outcome = normalizePublicationOutcome(
            publicationOutcome,
            context.result,
            request.request,
            "restore-destination",
            context.artifactProof,
          );
          return exactFrozenRecord({
            materialization: outcome.materialization,
            replayed: outcome.replayed,
            result: context.result,
          });
        },
      );
      return completion.result;
    } catch {
      failUncertain();
    }
  }

  async #delegateLifecycle(method, args) {
    ensureInvalid(args.length === 1);
    try {
      return await reflectApply(
        this.#lifecycleMethods[method],
        this.#lifecycleBackend,
        [args[0]],
      );
    } catch {
      failUncertain();
    }
  }
}

objectFreeze(StoppedDirectoryBackend.prototype);
