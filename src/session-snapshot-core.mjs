import { types as utilTypes } from "node:util";

import {
  SessionStorageContractError,
  assertCheckpointBackend,
  assertCheckpointCaptureReconciliationBackend,
  assertCheckpointClass,
  assertCheckpointDescriptor,
  assertLeaseGrant,
  assertSessionAttachmentMatches,
  assertSessionManifest,
  assertSessionStorageRef,
  assertStorageMutationMatchesLeaseSnapshot,
  assertStorageMutationRequest,
  assertStorageMutationResult,
  compareFencingEpochs,
} from "./session-storage-contracts.mjs";

// Map membership, one-use state, and the captured backend function are the
// dispatch authority. A stop collaborator must not replace those identities by
// mutating shared intrinsics after preparation.
const arrayEveryIntrinsic = Array.prototype.every;
const arrayIncludesIntrinsic = Array.prototype.includes;
const arrayIsArray = Array.isArray;
const objectCreate = Object.create;
const objectFreeze = Object.freeze;
const objectGetOwnPropertyDescriptor = Object.getOwnPropertyDescriptor;
const objectGetPrototypeOf = Object.getPrototypeOf;
const objectHasOwn = Object.hasOwn;
const objectIsFrozen = Object.isFrozen;
const objectKeys = Object.keys;
const objectPrototype = Object.prototype;
const reflectApply = Reflect.apply;
const reflectOwnKeys = Reflect.ownKeys;
const { isProxy: isProxyValue } = utilTypes;
const weakMapGetIntrinsic = WeakMap.prototype.get;
const weakMapHasIntrinsic = WeakMap.prototype.has;
const weakMapSetIntrinsic = WeakMap.prototype.set;

// Each API dispatches at most once per invocation. The backend owns durable
// operationId replay and must atomically recheck its authoritative writer fence.

const CORE_ERROR_MESSAGES = objectFreeze({
  checkpoint_outcome_uncertain: "Checkpoint capture outcome is uncertain",
  checkpoint_reconciliation_outcome_uncertain:
    "Checkpoint capture reconciliation outcome is uncertain",
  restore_outcome_uncertain: "Checkpoint restore outcome is uncertain",
  unsupported_checkpoint_class: "Checkpoint class is not supported by the clean snapshot core",
});

function weakMapGet(value, key) {
  return reflectApply(weakMapGetIntrinsic, value, [key]);
}

function weakMapHas(value, key) {
  return reflectApply(weakMapHasIntrinsic, value, [key]);
}

function weakMapSet(value, key, entry) {
  reflectApply(weakMapSetIntrinsic, value, [key, entry]);
}

function arrayEvery(value, callback) {
  return reflectApply(arrayEveryIntrinsic, value, [callback]);
}

function arrayIncludes(value, candidate) {
  return reflectApply(arrayIncludesIntrinsic, value, [candidate]);
}

export class SessionSnapshotCoreError extends Error {
  constructor(code) {
    if (!objectHasOwn(CORE_ERROR_MESSAGES, code)) {
      throw new TypeError("unsupported session snapshot core error code");
    }
    super(CORE_ERROR_MESSAGES[code]);
    this.name = "SessionSnapshotCoreError";
    this.code = code;
    this.retryable = false;
    objectFreeze(this);
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
    isProxyValue(value) ||
    value === null ||
    typeof value !== "object" ||
    arrayIsArray(value)
  ) {
    failContract("invalid_checkpoint", `${label} must be a plain object`);
  }

  let prototype;
  let actual;
  try {
    prototype = objectGetPrototypeOf(value);
    actual = reflectOwnKeys(value);
  } catch {
    failContract("invalid_checkpoint", `${label} must be a plain object`);
  }
  ensureContract(
    arrayIncludes([objectPrototype, null], prototype),
    "invalid_checkpoint",
    `${label} must be a plain object`,
  );
  ensureContract(
    actual.length === keys.length &&
      arrayEvery(
        actual,
        (key) => typeof key === "string" && arrayIncludes(keys, key),
      ),
    "invalid_checkpoint",
    `${label} contains unexpected or missing fields`,
  );

  const normalized = objectCreate(null);
  for (let index = 0; index < actual.length; index += 1) {
    const key = actual[index];
    let descriptor;
    try {
      descriptor = objectGetOwnPropertyDescriptor(value, key);
    } catch {
      failContract("invalid_checkpoint", `${label} fields must be plain data properties`);
    }
    ensureContract(
      descriptor?.enumerable === true && objectHasOwn(descriptor, "value"),
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
    let isContractError = false;
    try {
      isContractError = error instanceof SessionStorageContractError;
    } catch {
      // Hostile external thrown values are normalized to the fixed contract error.
    }
    if (isContractError) throw error;
    failContract(code, message);
  }
}

function validateExternalOperation(operation, code, message) {
  try {
    return operation();
  } catch {
    // Operational collaborators cannot forge or leak public contract errors.
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
  return validateExternalOperation(
    () => assertCheckpointBackend(value),
    "invalid_storage_backend",
    "storage backend is invalid",
  );
}

function checkedCaptureReconciliationBackend(value) {
  return validateExternalOperation(
    () => assertCheckpointCaptureReconciliationBackend(value),
    "invalid_storage_backend",
    "storage backend does not support checkpoint capture reconciliation",
  );
}

function checkedBackendMethod(backend, method) {
  return validateExternalOperation(
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
      !isProxyValue(value) &&
      !arrayIsArray(value),
    "invalid_checkpoint",
    "stopped writer evidence must be an opaque non-proxy object handle",
  );
  return value;
}

function assertBackendMatchesStorage(backend, storageRef) {
  validateExternalOperation(
    () =>
      ensureContract(
        backend.backendId === storageRef.backendId,
        "invalid_storage_backend",
        "storage backend does not match canonical storage",
      ),
    "invalid_storage_backend",
    "storage backend identity is invalid",
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

function assertBackendCheckpointResult(
  value,
  { expectedCheckpoint, manifest, request, storageRef },
) {
  const envelope = assertExactOptions(
    value,
    ["checkpoint", "mutation"],
    "storage backend checkpoint result",
  );
  const checkpointOptions =
    storageRef === undefined ? { manifest } : { manifest, storageRef };
  const checkpoint = assertCheckpointDescriptor(envelope.checkpoint, checkpointOptions);
  ensureContract(
    arrayEvery(
      objectKeys(expectedCheckpoint),
      (key) => checkpoint[key] === expectedCheckpoint[key],
    ),
    "invalid_checkpoint",
    "storage backend checkpoint result does not match the dispatched descriptor",
  );
  const mutation = assertStorageMutationResult(envelope.mutation, { request });
  return frozenResult(checkpoint, mutation);
}

function frozenResult(checkpoint, mutation) {
  return objectFreeze({ checkpoint, mutation });
}

const preparedCleanCheckpointCaptures = new WeakMap();

/**
 * Validates and canonicalizes one deterministic clean-checkpoint capture
 * before any stopped-writer capability is requested. The returned tuple is a
 * same-process token: capturePreparedCleanCheckpoint accepts only an object
 * produced by this helper, so a caller cannot substitute a different tuple
 * between writer stop and backend dispatch.
 */
export function prepareCleanCheckpointCapture(options) {
  const {
    attachment,
    backend,
    canonicalLease,
    checkpointClass,
    createdAt,
    manifest,
    now,
    request,
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
      "storageRef",
    ],
    "checkpoint capture preparation options",
  );

  const cleanClass = assertCleanCheckpointClass(checkpointClass);
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
  const prepared = objectFreeze({
    attachment: matched.attachment,
    backend,
    checkpoint,
    manifest: matched.manifest,
    request: mutationRequest,
    storageRef: matched.storageRef,
  });
  weakMapSet(preparedCleanCheckpointCaptures, prepared, {
    capture,
    state: "prepared",
  });
  return prepared;
}

/**
 * Dispatches one previously prepared clean capture with the exact canonical
 * tuple that was presented to the stopped-writer authority.
 */
export async function capturePreparedCleanCheckpoint(options) {
  const { preparedCapture, stoppedWriterEvidence } = assertExactOptions(
    options,
    ["preparedCapture", "stoppedWriterEvidence"],
    "prepared checkpoint capture options",
  );
  const writerEvidence = assertStoppedWriterEvidence(stoppedWriterEvidence);
  ensureContract(
    preparedCapture !== null &&
      typeof preparedCapture === "object" &&
      !isProxyValue(preparedCapture) &&
      !arrayIsArray(preparedCapture) &&
      objectIsFrozen(preparedCapture) &&
      weakMapHas(preparedCleanCheckpointCaptures, preparedCapture),
    "invalid_checkpoint",
    "prepared checkpoint capture is invalid",
  );
  const preparedState = weakMapGet(
    preparedCleanCheckpointCaptures,
    preparedCapture,
  );
  ensureContract(
    preparedState.state === "prepared",
    "invalid_checkpoint",
    "prepared checkpoint capture was already dispatched",
  );
  // One token authorizes one dispatch attempt. Transition synchronously before
  // invoking the backend so rejection or acknowledgement loss cannot reopen it.
  preparedState.state = "dispatched";
  const { capture } = preparedState;
  const {
    attachment,
    backend,
    checkpoint,
    manifest,
    request,
    storageRef,
  } = preparedCapture;

  try {
    const result = await reflectApply(capture, backend, [
      objectFreeze({
        attachment,
        checkpoint,
        request,
        stoppedWriterEvidence: writerEvidence,
      }),
    ]);
    return assertBackendCheckpointResult(result, {
      expectedCheckpoint: checkpoint,
      manifest,
      request,
      storageRef,
    });
  } catch {
    throw new SessionSnapshotCoreError("checkpoint_outcome_uncertain");
  }
}

/**
 * Validates the result of a durable prepared-capture resume without treating
 * the process-local preparation token as publication authority. The durable
 * PostgreSQL dispatch grant owns that decision; this helper only preserves
 * the exact checkpoint/result binding established before the writer stop.
 */
export function assertPreparedCleanCheckpointResult(options) {
  const { preparedCapture, result } = assertExactOptions(
    options,
    ["preparedCapture", "result"],
    "prepared checkpoint capture result options",
  );
  ensureContract(
    preparedCapture !== null &&
      typeof preparedCapture === "object" &&
      !isProxyValue(preparedCapture) &&
      !arrayIsArray(preparedCapture) &&
      objectIsFrozen(preparedCapture) &&
      weakMapHas(preparedCleanCheckpointCaptures, preparedCapture),
    "invalid_checkpoint",
    "prepared checkpoint capture is invalid",
  );
  const preparedState = weakMapGet(
    preparedCleanCheckpointCaptures,
    preparedCapture,
  );
  ensureContract(
    preparedState.state === "prepared",
    "invalid_checkpoint",
    "prepared checkpoint capture was already dispatched",
  );
  const { checkpoint, manifest, request, storageRef } = preparedCapture;
  return assertBackendCheckpointResult(result, {
    expectedCheckpoint: checkpoint,
    manifest,
    request,
    storageRef,
  });
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

  const writerEvidence = assertStoppedWriterEvidence(stoppedWriterEvidence);
  const preparedCapture = prepareCleanCheckpointCapture({
    attachment,
    backend,
    canonicalLease,
    checkpointClass,
    createdAt,
    manifest,
    now,
    request,
    storageRef,
  });
  return capturePreparedCleanCheckpoint({
    preparedCapture,
    stoppedWriterEvidence: writerEvidence,
  });
}

/**
 * Reconciles one exact durable clean-capture attempt without presenting new
 * writer authority. The backend must authenticate the original attempt and
 * must not start a new physical mutation when no such attempt exists.
 */
export async function reconcileCleanCheckpointCapture(options) {
  const { backend, checkpoint, manifest, request, storageRef } =
    assertExactOptions(
      options,
      ["backend", "checkpoint", "manifest", "request", "storageRef"],
      "checkpoint capture reconciliation options",
    );

  const storage = validateContract(
    () => assertSessionStorageRef(storageRef),
    "invalid_storage_ref",
    "session storage reference is invalid",
  );
  const sessionManifest = validateContract(
    () => assertSessionManifest(manifest),
    "invalid_session_manifest",
    "session manifest is invalid",
  );
  const descriptor = validateContract(
    () =>
      assertCheckpointDescriptor(checkpoint, {
        manifest: sessionManifest,
        storageRef: storage,
      }),
    "invalid_checkpoint",
    "checkpoint descriptor is invalid",
  );
  assertCleanCheckpointClass(descriptor.checkpointClass);
  const mutationRequest = validateContract(
    () => assertStorageMutationRequest(request),
    "invalid_storage_mutation",
    "checkpoint reconciliation mutation request is invalid",
  );
  assertOperation(mutationRequest, "checkpoint");
  assertCheckpointTarget(mutationRequest, descriptor);
  ensureContract(
    mutationRequest.backendId === descriptor.backendId &&
      mutationRequest.storageId === descriptor.storageId &&
      mutationRequest.sessionId === descriptor.sessionId,
    "invalid_storage_mutation",
    "checkpoint reconciliation request does not match its source storage",
  );
  ensureContract(
    mutationRequest.fencingEpoch === descriptor.sourceFencingEpoch,
    "stale_fence",
    "checkpoint reconciliation request fence does not match its source fence",
  );

  const storageBackend = checkedCaptureReconciliationBackend(backend);
  assertBackendMatchesStorage(storageBackend, storage);
  const reconcile = checkedBackendMethod(
    storageBackend,
    "reconcileCheckpointCapture",
  );
  try {
    const result = await reflectApply(reconcile, storageBackend, [
      objectFreeze({ checkpoint: descriptor, request: mutationRequest }),
    ]);
    return assertBackendCheckpointResult(result, {
      expectedCheckpoint: descriptor,
      manifest: sessionManifest,
      request: mutationRequest,
      storageRef: storage,
    });
  } catch {
    throw new SessionSnapshotCoreError(
      "checkpoint_reconciliation_outcome_uncertain",
    );
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
  const sessionManifest = validateContract(
    () => assertSessionManifest(manifest),
    "invalid_session_manifest",
    "session manifest is invalid",
  );
  const descriptor = validateContract(
    () => assertCheckpointDescriptor(checkpoint, { manifest: sessionManifest }),
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
    const result = await reflectApply(restore, storageBackend, [
      objectFreeze({ checkpoint: descriptor, request: mutationRequest }),
    ]);
    return assertBackendCheckpointResult(result, {
      expectedCheckpoint: descriptor,
      manifest: sessionManifest,
      request: mutationRequest,
      storageRef: undefined,
    });
  } catch {
    throw new SessionSnapshotCoreError("restore_outcome_uncertain");
  }
}
