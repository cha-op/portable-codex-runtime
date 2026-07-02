import { types as utilTypes } from "node:util";

import {
  SessionStorageContractError,
  assertCheckpointClass,
  assertCheckpointDescriptor,
  assertLeaseGrant,
  assertSessionAttachmentMatches,
  assertSessionStorageRef,
  assertStorageBackend,
  assertStorageMutationMatchesLeaseSnapshot,
  assertStorageMutationResult,
  compareFencingEpochs,
} from "./session-storage-contracts.mjs";

// Each API dispatches at most once per invocation. The backend owns durable
// operationId replay and must atomically recheck its authoritative writer fence.

const CORE_ERROR_MESSAGES = Object.freeze({
  checkpoint_outcome_uncertain: "Checkpoint capture outcome is uncertain",
  restore_outcome_uncertain: "Checkpoint restore outcome is uncertain",
  unsupported_checkpoint_class: "Checkpoint class is not supported by the clean snapshot core",
});

export class SessionSnapshotCoreError extends Error {
  constructor(code) {
    if (!Object.hasOwn(CORE_ERROR_MESSAGES, code)) {
      throw new TypeError("unsupported session snapshot core error code");
    }
    super(CORE_ERROR_MESSAGES[code]);
    this.name = "SessionSnapshotCoreError";
    this.code = code;
    this.retryable = false;
    Object.freeze(this);
  }
}

function failContract(code, message) {
  throw new SessionStorageContractError(code, message);
}

function ensureContract(condition, code, message) {
  if (!condition) failContract(code, message);
}

function assertExactOptions(value, keys, label) {
  if (
    utilTypes.isProxy(value) ||
    value === null ||
    typeof value !== "object" ||
    Array.isArray(value)
  ) {
    failContract("invalid_checkpoint", `${label} must be a plain object`);
  }

  let prototype;
  let actual;
  try {
    prototype = Object.getPrototypeOf(value);
    actual = Reflect.ownKeys(value);
  } catch {
    failContract("invalid_checkpoint", `${label} must be a plain object`);
  }
  ensureContract(
    [Object.prototype, null].includes(prototype),
    "invalid_checkpoint",
    `${label} must be a plain object`,
  );
  ensureContract(
    actual.length === keys.length &&
      actual.every((key) => typeof key === "string" && keys.includes(key)),
    "invalid_checkpoint",
    `${label} contains unexpected or missing fields`,
  );

  const normalized = Object.create(null);
  for (const key of actual) {
    let descriptor;
    try {
      descriptor = Object.getOwnPropertyDescriptor(value, key);
    } catch {
      failContract("invalid_checkpoint", `${label} fields must be plain data properties`);
    }
    ensureContract(
      descriptor?.enumerable === true && Object.hasOwn(descriptor, "value"),
      "invalid_checkpoint",
      `${label} fields must be enumerable plain data properties`,
    );
    normalized[key] = descriptor.value;
  }
  return normalized;
}

function validateContract(operation, code, message) {
  try {
    return operation();
  } catch (error) {
    if (error instanceof SessionStorageContractError) throw error;
    failContract(code, message);
  }
}

function assertCleanCheckpointClass(value) {
  const checkpointClass = validateContract(
    () => assertCheckpointClass(value),
    "invalid_checkpoint",
    "checkpoint class is invalid",
  );
  if (checkpointClass !== "clean") {
    throw new SessionSnapshotCoreError("unsupported_checkpoint_class");
  }
  return checkpointClass;
}

function checkedBackend(value) {
  return validateContract(
    () => assertStorageBackend(value),
    "invalid_storage_backend",
    "storage backend is invalid",
  );
}

function checkedBackendMethod(backend, method) {
  return validateContract(
    () => {
      const operation = backend[method];
      ensureContract(
        typeof operation === "function",
        "invalid_storage_backend",
        "storage backend operation is invalid",
      );
      return operation;
    },
    "invalid_storage_backend",
    "storage backend operation is invalid",
  );
}

function assertStoppedWriterEvidence(value) {
  // Deliberately opaque: only the backend can authenticate this handle and
  // bind it to the stopped writer, attachment, and canonical fence.
  ensureContract(
    value !== null &&
      typeof value === "object" &&
      !Array.isArray(value) &&
      !utilTypes.isProxy(value),
    "invalid_checkpoint",
    "stopped writer evidence must be an opaque non-proxy object handle",
  );
  return value;
}

function assertBackendMatchesStorage(backend, storageRef) {
  ensureContract(
    backend.backendId === storageRef.backendId,
    "invalid_storage_backend",
    "storage backend does not match canonical storage",
  );
}

function assertOperation(request, operation) {
  ensureContract(
    request.operation === operation,
    "invalid_storage_mutation",
    `storage mutation must be ${operation}`,
  );
}

function assertCheckpointTarget(request, checkpoint) {
  ensureContract(
    request.target.kind === "checkpoint" &&
      request.target.checkpointId === checkpoint.checkpointId &&
      request.target.artifactId === checkpoint.artifactId,
    "invalid_storage_mutation",
    "storage mutation target does not match the checkpoint descriptor",
  );
}

function frozenResult(checkpoint, mutation) {
  return Object.freeze({ checkpoint, mutation });
}

/**
 * Structural orchestration only. The backend must atomically recheck the
 * canonical writer fence while capturing the checkpoint.
 */
export async function captureCleanCheckpoint(options) {
  const {
    attachment,
    backend,
    canonicalLease,
    checkpointClass,
    createdAt,
    manifest,
    now,
    request,
    stoppedWriterEvidence,
    storageRef,
  } = assertExactOptions(
    options,
    [
      "attachment",
      "backend",
      "canonicalLease",
      "checkpointClass",
      "createdAt",
      "manifest",
      "now",
      "request",
      "stoppedWriterEvidence",
      "storageRef",
    ],
    "checkpoint capture options",
  );

  const cleanClass = assertCleanCheckpointClass(checkpointClass);
  const writerEvidence = assertStoppedWriterEvidence(stoppedWriterEvidence);
  const storageBackend = checkedBackend(backend);
  const matched = validateContract(
    () =>
      assertSessionAttachmentMatches({
        attachment,
        lease: canonicalLease,
        manifest,
        storageRef,
      }),
    "invalid_storage_attachment",
    "session attachment does not match canonical writer authority",
  );
  assertBackendMatchesStorage(storageBackend, matched.storageRef);
  const mutationRequest = validateContract(
    () =>
      assertStorageMutationMatchesLeaseSnapshot({
        canonicalLease: matched.lease,
        now,
        request,
        storageRef: matched.storageRef,
      }),
    "invalid_storage_mutation",
    "checkpoint mutation request is invalid",
  );
  assertOperation(mutationRequest, "checkpoint");

  const checkpoint = validateContract(
    () =>
      assertCheckpointDescriptor(
        {
          artifactId: mutationRequest.target.artifactId,
          backendId: matched.storageRef.backendId,
          checkpointClass: cleanClass,
          checkpointId: mutationRequest.target.checkpointId,
          codexSessionId: matched.manifest.codex.sessionId,
          codexThreadId: matched.manifest.codex.rootThreadId,
          contractVersion: matched.storageRef.contractVersion,
          createdAt,
          imageDigest: matched.manifest.runtime.imageDigest,
          sessionId: matched.manifest.sessionId,
          sourceFencingEpoch: matched.lease.fencingEpoch,
          storageId: matched.storageRef.storageId,
        },
        { manifest: matched.manifest, storageRef: matched.storageRef },
      ),
    "invalid_checkpoint",
    "checkpoint descriptor is invalid",
  );

  const capture = checkedBackendMethod(storageBackend, "captureCheckpoint");
  try {
    const result = await capture.call(
      storageBackend,
      Object.freeze({
        attachment: matched.attachment,
        checkpoint,
        request: mutationRequest,
        stoppedWriterEvidence: writerEvidence,
      }),
    );
    const mutation = assertStorageMutationResult(result, { request: mutationRequest });
    return frozenResult(checkpoint, mutation);
  } catch {
    throw new SessionSnapshotCoreError("checkpoint_outcome_uncertain");
  }
}

/**
 * Structural orchestration only. The backend must atomically recheck the new
 * canonical writer fence while restoring the checkpoint.
 */
export async function restoreCleanCheckpoint(options) {
  const { backend, canonicalLease, checkpoint, manifest, now, request, storageRef } =
    assertExactOptions(
      options,
      ["backend", "canonicalLease", "checkpoint", "manifest", "now", "request", "storageRef"],
      "checkpoint restore options",
    );

  const storage = validateContract(
    () => assertSessionStorageRef(storageRef),
    "invalid_storage_ref",
    "session storage reference is invalid",
  );
  const descriptor = validateContract(
    () => assertCheckpointDescriptor(checkpoint, { manifest }),
    "invalid_checkpoint",
    "checkpoint descriptor is invalid",
  );
  assertCleanCheckpointClass(descriptor.checkpointClass);
  ensureContract(
    descriptor.sessionId === storage.sessionId && descriptor.backendId === storage.backendId,
    "invalid_checkpoint",
    "checkpoint source does not match the destination session and backend",
  );
  const storageBackend = checkedBackend(backend);
  assertBackendMatchesStorage(storageBackend, storage);
  const lease = validateContract(
    () => assertLeaseGrant(canonicalLease),
    "invalid_fence",
    "canonical writer lease is invalid",
  );
  const mutationRequest = validateContract(
    () =>
      assertStorageMutationMatchesLeaseSnapshot({
        canonicalLease: lease,
        now,
        request,
        storageRef: storage,
      }),
    "invalid_storage_mutation",
    "restore mutation request is invalid",
  );
  assertOperation(mutationRequest, "restore");
  assertCheckpointTarget(mutationRequest, descriptor);
  ensureContract(
    compareFencingEpochs(lease.fencingEpoch, descriptor.sourceFencingEpoch) > 0,
    "stale_fence",
    "restore requires a writer fence newer than the checkpoint source fence",
  );

  const restore = checkedBackendMethod(storageBackend, "restoreCheckpoint");
  try {
    const result = await restore.call(
      storageBackend,
      Object.freeze({ checkpoint: descriptor, request: mutationRequest }),
    );
    const mutation = assertStorageMutationResult(result, { request: mutationRequest });
    return frozenResult(descriptor, mutation);
  } catch {
    throw new SessionSnapshotCoreError("restore_outcome_uncertain");
  }
}
