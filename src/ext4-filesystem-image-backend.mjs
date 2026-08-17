import { Buffer } from "node:buffer";
import { createHash } from "node:crypto";
import { basename, dirname, isAbsolute, resolve } from "node:path";
import { types as utilTypes } from "node:util";

import {
  EXT4_FILESYSTEM_IMAGE_PATHS_CONTRACT_VERSION,
  assertExt4FilesystemImageMountPathCapacity,
} from "./ext4-filesystem-image-paths.mjs";
import {
  FILESYSTEM_IMAGE_PROVIDER_STATE_CONTRACT_VERSION,
  FilesystemImageProviderState,
} from "./filesystem-image-provider-state.mjs";
import {
  LINUX_EXT4_IMAGE_DRIVER_CONTRACT_VERSION,
} from "./linux-ext4-image-driver.mjs";
import {
  RESTORE_ATTACHMENT_ACTIVATION_CONTRACT_VERSION,
  RESTORE_ATTACHMENT_RECONCILIATION_CONTRACT_VERSION,
  STORAGE_CONTRACT_VERSION,
  assertRestoreAttachmentActivationRequest,
  assertRestoreAttachmentActivationResult,
  assertRestoreAttachmentReconciliationResult,
  assertSessionProvisionRequest,
  assertSessionProvisionResult,
  assertStorageForceFenceRequest,
  assertStorageMutationRequest,
  assertStorageMutationResult,
  assertWriterAttachmentMutationResult,
} from "./session-storage-contracts.mjs";

const arrayIsArrayIntrinsic = Array.isArray;
const arrayIncludesIntrinsic = Array.prototype.includes;
const bufferByteLengthIntrinsic = Buffer.byteLength;
const objectCreateIntrinsic = Object.create;
const objectFreezeIntrinsic = Object.freeze;
const objectGetOwnPropertyDescriptorIntrinsic = Object.getOwnPropertyDescriptor;
const objectGetPrototypeOfIntrinsic = Object.getPrototypeOf;
const objectHasOwnIntrinsic = Object.hasOwn;
const objectIsFrozenIntrinsic = Object.isFrozen;
const objectPrototype = Object.prototype;
const promisePrototype = Promise.prototype;
const reflectApplyIntrinsic = Reflect.apply;
const reflectOwnKeysIntrinsic = Reflect.ownKeys;
const regexpExecIntrinsic = RegExp.prototype.exec;
const jsonStringifyIntrinsic = JSON.stringify;
const weakSetAddIntrinsic = WeakSet.prototype.add;
const weakSetHasIntrinsic = WeakSet.prototype.has;
const WeakSetConstructor = WeakSet;
const hashPrototype = objectGetPrototypeOfIntrinsic(createHash("sha256"));
const hashUpdateIntrinsic = hashPrototype.update;
const hashDigestIntrinsic = hashPrototype.digest;
const { isGeneratorFunction, isPromise, isProxy } = utilTypes;

const statePrepareOperationIntrinsic =
  FilesystemImageProviderState.prototype.prepareOperation;
const stateCommitOperationIntrinsic =
  FilesystemImageProviderState.prototype.commitOperation;
const stateReadOperationIntrinsic =
  FilesystemImageProviderState.prototype.readOperation;
const stateReadStorageIntrinsic =
  FilesystemImageProviderState.prototype.readStorage;
const stateReadStorageByMountPathIntrinsic =
  FilesystemImageProviderState.prototype.readStorageByMountPath;
const stateSnapshotIntrinsic = FilesystemImageProviderState.prototype.snapshot;
const assertMountPathCapacityIntrinsic =
  assertExt4FilesystemImageMountPathCapacity;
const abortSignalAbortedGetter = objectGetOwnPropertyDescriptorIntrinsic(
  AbortSignal.prototype,
  "aborted",
)?.get;

export const EXT4_FILESYSTEM_IMAGE_BACKEND_CONTRACT_VERSION = 1;
export const EXT4_FILESYSTEM_IMAGE_PHYSICAL_INVOCATION_CONTRACT_VERSION = 1;

const MAX_CLONE_DEPTH = 32;
const MAX_CLONE_NODES = 16_384;
const MAX_PATH_BYTES = 4095;
const IMAGE_SIZE_ALIGNMENT_BYTES = 512;
const MIN_IMAGE_SIZE_BYTES = 1024 * 1024;
const MAX_IMAGE_SIZE_BYTES = 8 * 1024 * 1024 * 1024 * 1024;
const OPAQUE_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u;
const DECIMAL_PATTERN = /^(?:0|[1-9][0-9]*)$/u;
const POSITIVE_DECIMAL_PATTERN = /^[1-9][0-9]*$/u;
const DATA_CHILD_PATTERN = /^data-[0-9a-f]{48}$/u;
const RESTORE_CHILD_PATTERN = /^generation-[0-9a-f]{48}$/u;
const rawBackendSurfaces = new WeakSetConstructor();

const OPTION_KEYS = objectFreezeIntrinsic([
  "backendId",
  "driver",
  "imageSizeBytes",
  "paths",
  "state",
]);
const DRIVER_METHOD_KEYS = objectFreezeIntrinsic([
  "destroy",
  "ensureAttachmentRoot",
  "ensurePublicationRoot",
  "observeAttachmentRoot",
  "observeMount",
  "provision",
  "quiesce",
  "remount",
  "syncFilesystem",
]);
const LIFECYCLE_METHOD_KEYS = objectFreezeIntrinsic([
  "captureCheckpoint",
  "destroySession",
  "detachAttachment",
  "forceFence",
  "prepareRestoreAttachment",
  "prepareWritableAttachment",
  "provisionSession",
  "reconcileRestoreAttachment",
  "restoreCheckpoint",
]);
const LIFECYCLE_METADATA_KEYS = objectFreezeIntrinsic([
  "backendId",
  "capabilities",
  "contractVersion",
  "physicalInvocationContractVersion",
  "restoreAttachmentActivationContractVersion",
  "restoreAttachmentReconciliationContractVersion",
]);
const PATH_METHOD_KEYS = objectFreezeIntrinsic([
  "planProvision",
  "planRestoreDestination",
  "planWritableAttachment",
]);
const PHYSICAL_CONTEXT_KEYS = objectFreezeIntrinsic([
  "contractVersion",
  "invocation",
  "signal",
]);
const RESOLVER_KEYS = objectFreezeIntrinsic([
  "candidate",
  "contractVersion",
  "generation",
  "invocation",
  "kind",
  "signal",
]);
const MOUNT_REQUEST_KEYS = objectFreezeIntrinsic(["imagePath", "mountPath"]);
const ATTACHMENT_REQUEST_KEYS = objectFreezeIntrinsic([
  "attachmentRootPath",
  "imagePath",
  "mountPath",
]);
const IDENTITY_KEYS = objectFreezeIntrinsic([
  "filesystemId",
  "objectIdentityScheme",
  "objectId",
]);
const IDENTITY_WITH_ROOT_PATH_KEYS = objectFreezeIntrinsic([
  "filesystemId",
  "objectIdentityScheme",
  "objectId",
  "rootPath",
]);
const CONTROL_FILE_IDENTITY_KEYS = objectFreezeIntrinsic([
  "device",
  "inode",
  "filesystemId",
  "objectIdentityScheme",
  "objectId",
]);
const MOUNT_OBSERVATION_KEYS = objectFreezeIntrinsic([
  "filesystem",
  "imageIdentity",
  "imagePath",
  "loopDevice",
  "mountEvidence",
  "mountPath",
  "rootIdentity",
]);
const PUBLICATION_CONTROL_RECEIPT_KEYS = objectFreezeIntrinsic([
  "controlFileIdentity",
  "controlFileName",
  "created",
  "filesystem",
  "imageIdentity",
  "imagePath",
  "loopDevice",
  "mountEvidence",
  "mountPath",
  "mountRootIdentity",
  "publicationControlIdentity",
]);
const PUBLICATION_CONTROL_FILE_NAME =
  ".stopped-directory-publication.lock";
const ATTACHMENT_OBSERVATION_KEYS = objectFreezeIntrinsic([
  "attachmentRootPath",
  "filesystem",
  "imageIdentity",
  "imagePath",
  "loopDevice",
  "mountEvidence",
  "mountPath",
  "mountRootIdentity",
  "rootIdentity",
]);
const ATTACHMENT_OBSERVATION_WITH_CREATED_KEYS = objectFreezeIntrinsic([
  "attachmentRootPath",
  "created",
  "filesystem",
  "imageIdentity",
  "imagePath",
  "loopDevice",
  "mountEvidence",
  "mountPath",
  "mountRootIdentity",
  "rootIdentity",
]);

const ERROR_MESSAGES = objectFreezeIntrinsic({
  attachment_root_absent:
    "Ext4 filesystem image attachment root is conclusively absent",
  checkpoint_unsupported:
    "Ext4 filesystem image checkpoint operations are unsupported",
  cold_open_failed: "Ext4 filesystem image cold-open validation failed",
  fence_unavailable: "Ext4 filesystem image manual fencing is unavailable",
  invalid_options: "Ext4 filesystem image backend options are invalid",
  invalid_request: "Ext4 filesystem image backend request is invalid",
  physical_effect_ambiguous:
    "Ext4 filesystem image physical effect is ambiguous",
  physical_state_mismatch:
    "Ext4 filesystem image physical state does not match durable state",
});

export class Ext4FilesystemImageBackendError extends Error {
  constructor(code) {
    if (typeof code !== "string" || !objectHasOwnIntrinsic(ERROR_MESSAGES, code)) {
      throw new TypeError("unsupported ext4 filesystem image backend error");
    }
    super(ERROR_MESSAGES[code]);
    this.name = "Ext4FilesystemImageBackendError";
    this.code = code;
    this.retryable = false;
    objectFreezeIntrinsic(this);
  }
}

function callIntrinsic(intrinsic, receiver, args) {
  return reflectApplyIntrinsic(intrinsic, receiver, args);
}

function arrayIncludes(values, candidate) {
  return callIntrinsic(arrayIncludesIntrinsic, values, [candidate]);
}

function weakSetAdd(values, candidate) {
  return callIntrinsic(weakSetAddIntrinsic, values, [candidate]);
}

function weakSetHas(values, candidate) {
  return callIntrinsic(weakSetHasIntrinsic, values, [candidate]);
}

function regexpTest(pattern, value) {
  return callIntrinsic(regexpExecIntrinsic, pattern, [value]) !== null;
}

function fail(code) {
  throw new Ext4FilesystemImageBackendError(code);
}

function ensure(condition, code) {
  if (!condition) fail(code);
}

function exactFrozenRecord(values) {
  const result = objectCreateIntrinsic(null);
  const keys = reflectOwnKeysIntrinsic(values);
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
    result[key] = values[key];
  }
  return objectFreezeIntrinsic(result);
}

function inspectExactDataObject(value, keys, code, { frozen = false } = {}) {
  ensure(
    value !== null &&
      typeof value === "object" &&
      !arrayIsArrayIntrinsic(value) &&
      !isProxy(value),
    code,
  );
  let prototype;
  let actual;
  try {
    prototype = objectGetPrototypeOfIntrinsic(value);
    actual = reflectOwnKeysIntrinsic(value);
  } catch {
    fail(code);
  }
  ensure(
    (prototype === objectPrototype || prototype === null) &&
      actual.length === keys.length &&
      (!frozen || objectIsFrozenIntrinsic(value)),
    code,
  );
  const result = objectCreateIntrinsic(null);
  for (let index = 0; index < actual.length; index += 1) {
    const key = actual[index];
    ensure(typeof key === "string" && arrayIncludes(keys, key), code);
    let descriptor;
    try {
      descriptor = objectGetOwnPropertyDescriptorIntrinsic(value, key);
    } catch {
      fail(code);
    }
    ensure(
      descriptor?.enumerable === true &&
        objectHasOwnIntrinsic(descriptor, "value") &&
        !objectHasOwnIntrinsic(result, key),
      code,
    );
    result[key] = descriptor.value;
  }
  return result;
}

function ownDataValue(value, key, code) {
  let descriptor;
  try {
    descriptor = objectGetOwnPropertyDescriptorIntrinsic(value, key);
  } catch {
    fail(code);
  }
  ensure(
    descriptor?.enumerable === true && objectHasOwnIntrinsic(descriptor, "value"),
    code,
  );
  return descriptor.value;
}

function trustedFunction(value, code) {
  ensure(
    typeof value === "function" &&
      !isProxy(value) &&
      !isGeneratorFunction(value) &&
      objectIsFrozenIntrinsic(value),
    code,
  );
  return value;
}

function clonePlainData(value, code, state = { nodes: 0 }, depth = 0) {
  state.nodes += 1;
  ensure(state.nodes <= MAX_CLONE_NODES && depth <= MAX_CLONE_DEPTH, code);
  const type = typeof value;
  if (value === null || type === "boolean" || type === "number" || type === "string") {
    return value;
  }
  ensure(typeof value === "object" && !isProxy(value), code);
  if (arrayIsArrayIntrinsic(value)) {
    const keys = reflectOwnKeysIntrinsic(value);
    ensure(keys.length === value.length + 1, code);
    const result = [];
    for (let index = 0; index < value.length; index += 1) {
      const descriptor = objectGetOwnPropertyDescriptorIntrinsic(value, String(index));
      ensure(
        descriptor?.enumerable === true &&
          objectHasOwnIntrinsic(descriptor, "value"),
        code,
      );
      result[index] = clonePlainData(descriptor.value, code, state, depth + 1);
    }
    return objectFreezeIntrinsic(result);
  }
  const prototype = objectGetPrototypeOfIntrinsic(value);
  ensure(prototype === objectPrototype || prototype === null, code);
  const keys = reflectOwnKeysIntrinsic(value);
  const result = objectCreateIntrinsic(null);
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
    ensure(typeof key === "string", code);
    const descriptor = objectGetOwnPropertyDescriptorIntrinsic(value, key);
    ensure(
      descriptor?.enumerable === true && objectHasOwnIntrinsic(descriptor, "value"),
      code,
    );
    result[key] = clonePlainData(descriptor.value, code, state, depth + 1);
  }
  return objectFreezeIntrinsic(result);
}

function isExactNativePromise(value) {
  if (
    value === null ||
    typeof value !== "object" ||
    isProxy(value) ||
    !isPromise(value)
  ) {
    return false;
  }
  try {
    return (
      objectGetPrototypeOfIntrinsic(value) === promisePrototype &&
      objectGetOwnPropertyDescriptorIntrinsic(value, "catch") === undefined &&
      objectGetOwnPropertyDescriptorIntrinsic(value, "constructor") === undefined &&
      objectGetOwnPropertyDescriptorIntrinsic(value, "finally") === undefined &&
      objectGetOwnPropertyDescriptorIntrinsic(value, "then") === undefined
    );
  } catch {
    return false;
  }
}

function nativePromise(value, code) {
  ensure(isExactNativePromise(value), code);
  return value;
}

function signalAborted(signal, code) {
  ensure(
    signal !== null &&
      typeof signal === "object" &&
      !isProxy(signal) &&
      objectGetPrototypeOfIntrinsic(signal) === AbortSignal.prototype &&
      typeof abortSignalAbortedGetter === "function",
    code,
  );
  try {
    return callIntrinsic(abortSignalAbortedGetter, signal, []);
  } catch {
    fail(code);
  }
}

function physicalContext(value) {
  const context = inspectExactDataObject(
    value,
    PHYSICAL_CONTEXT_KEYS,
    "invalid_request",
    { frozen: true },
  );
  ensure(
    context.contractVersion ===
      EXT4_FILESYSTEM_IMAGE_PHYSICAL_INVOCATION_CONTRACT_VERSION,
    "invalid_request",
  );
  inspectExactDataObject(context.invocation, [], "invalid_request", {
    frozen: true,
  });
  ensure(!signalAborted(context.signal, "invalid_request"), "invalid_request");
  return context;
}

function safeErrorCode(error) {
  if (error === null || typeof error !== "object" || isProxy(error)) return null;
  try {
    const descriptor = objectGetOwnPropertyDescriptorIntrinsic(error, "code");
    return descriptor && objectHasOwnIntrinsic(descriptor, "value")
      ? descriptor.value
      : null;
  } catch {
    return null;
  }
}

function identity(value, code = "physical_state_mismatch") {
  let hasRootPath = false;
  try {
    hasRootPath = objectHasOwnIntrinsic(value, "rootPath");
  } catch {
    fail(code);
  }
  const data = inspectExactDataObject(
    value,
    hasRootPath ? IDENTITY_WITH_ROOT_PATH_KEYS : IDENTITY_KEYS,
    code,
  );
  ensure(
    typeof data.filesystemId === "string" &&
      typeof data.objectIdentityScheme === "string" &&
      typeof data.objectId === "string",
    code,
  );
  return data;
}

function sameIdentity(left, right) {
  const a = identity(left);
  const b = identity(right);
  return (
    a.filesystemId === b.filesystemId &&
    a.objectIdentityScheme === b.objectIdentityScheme &&
    a.objectId === b.objectId
  );
}

function sameMountObservation(observation, storage, plan) {
  const mounted = inspectExactDataObject(
    observation,
    MOUNT_OBSERVATION_KEYS,
    "physical_state_mismatch",
  );
  ensure(
    storage !== null &&
      storage.mount !== null &&
      mounted.imagePath === plan.imagePath &&
      mounted.mountPath === plan.mountPath &&
      storage.imagePath === plan.imagePath &&
      storage.mount.mountPath === plan.mountPath &&
      sameIdentity(mounted.imageIdentity, storage.mount.imageIdentity) &&
      sameIdentity(mounted.rootIdentity, storage.mount.rootIdentity),
    "physical_state_mismatch",
  );
  return mounted;
}

function sameAttachmentObservation(observation, storage, plan) {
  let includesCreated = false;
  try {
    includesCreated = objectHasOwnIntrinsic(observation, "created");
  } catch {
    fail("physical_state_mismatch");
  }
  const attached = inspectExactDataObject(
    observation,
    includesCreated
      ? ATTACHMENT_OBSERVATION_WITH_CREATED_KEYS
      : ATTACHMENT_OBSERVATION_KEYS,
    "physical_state_mismatch",
  );
  ensure(
    !includesCreated || typeof attached.created === "boolean",
    "physical_state_mismatch",
  );
  ensure(
    attached.attachmentRootPath === plan.attachmentRootPath &&
      attached.imagePath === plan.imagePath &&
      attached.mountPath === plan.mountPath &&
      sameIdentity(attached.imageIdentity, storage.mount.imageIdentity) &&
      sameIdentity(attached.mountRootIdentity, storage.mount.rootIdentity),
    "physical_state_mismatch",
  );
  return attached;
}

function stateMount(observation) {
  return exactFrozenRecord({
    imageIdentity: exactFrozenRecord(identity(observation.imageIdentity)),
    mountPath: observation.mountPath,
    rootIdentity: exactFrozenRecord(identity(observation.rootIdentity)),
  });
}

function provisionObservation(observation, plan) {
  const mounted = inspectExactDataObject(
    observation,
    MOUNT_OBSERVATION_KEYS,
    "physical_state_mismatch",
  );
  const filesystem = inspectExactDataObject(
    mounted.filesystem,
    ["durability", "filesystemId", "objectIdentityScheme", "type"],
    "physical_state_mismatch",
  );
  const image = identity(mounted.imageIdentity);
  const root = identity(mounted.rootIdentity);
  ensure(
    mounted.imagePath === plan.imagePath &&
      mounted.mountPath === plan.mountPath &&
      filesystem.filesystemId === root.filesystemId &&
      filesystem.objectIdentityScheme === root.objectIdentityScheme &&
      image.objectId !== root.objectId,
    "physical_state_mismatch",
  );
  return mounted;
}

function publicationControlReceipt(value, plan, mounted, expectedIdentity) {
  const receipt = inspectExactDataObject(
    value,
    PUBLICATION_CONTROL_RECEIPT_KEYS,
    "physical_state_mismatch",
  );
  const filesystem = inspectExactDataObject(
    receipt.filesystem,
    ["durability", "filesystemId", "objectIdentityScheme", "type"],
    "physical_state_mismatch",
  );
  const control = inspectExactDataObject(
    receipt.controlFileIdentity,
    CONTROL_FILE_IDENTITY_KEYS,
    "physical_state_mismatch",
  );
  const publicationIdentity = identity(receipt.publicationControlIdentity);
  const image = identity(receipt.imageIdentity);
  const mountRoot = identity(receipt.mountRootIdentity);
  const mountedImage = identity(mounted.imageIdentity);
  const mountedRoot = identity(mounted.rootIdentity);
  ensure(
    receipt.controlFileName === PUBLICATION_CONTROL_FILE_NAME &&
      typeof receipt.created === "boolean" &&
      typeof control.device === "string" &&
      regexpTest(DECIMAL_PATTERN, control.device) &&
      typeof control.inode === "string" &&
      regexpTest(POSITIVE_DECIMAL_PATTERN, control.inode) &&
      receipt.imagePath === plan.imagePath &&
      receipt.mountPath === plan.mountPath &&
      filesystem.durability === mounted.filesystem.durability &&
      filesystem.filesystemId === mounted.filesystem.filesystemId &&
      filesystem.objectIdentityScheme ===
        mounted.filesystem.objectIdentityScheme &&
      filesystem.type === mounted.filesystem.type &&
      sameIdentity(image, mountedImage) &&
      sameIdentity(mountRoot, mountedRoot) &&
      control.filesystemId === publicationIdentity.filesystemId &&
      control.objectIdentityScheme ===
        publicationIdentity.objectIdentityScheme &&
      control.objectId === publicationIdentity.objectId &&
      publicationIdentity.filesystemId === filesystem.filesystemId &&
      publicationIdentity.objectIdentityScheme ===
        filesystem.objectIdentityScheme &&
      publicationIdentity.objectId !== mountRoot.objectId &&
      (expectedIdentity === null ||
        sameIdentity(publicationIdentity, expectedIdentity)),
    "physical_state_mismatch",
  );
  return exactFrozenRecord({
    filesystem: exactFrozenRecord(filesystem),
    identity: exactFrozenRecord({
      device: control.device,
      inode: control.inode,
      objectId: control.objectId,
    }),
    publicationControlIdentity: exactFrozenRecord(publicationIdentity),
  });
}

function increment(value) {
  try {
    return String(BigInt(value) + 1n);
  } catch {
    fail("physical_state_mismatch");
  }
}

function laterEpoch(candidate, current) {
  try {
    return BigInt(candidate) > BigInt(current);
  } catch {
    fail("physical_state_mismatch");
  }
}

function proofId(backendId, kind, operationId, objectId = "none") {
  const hash = createHash("sha256");
  callIntrinsic(hashUpdateIntrinsic, hash, [
    "portable-codex-runtime/ext4-backend-proof/v1\0",
    "utf8",
  ]);
  const values = [backendId, kind, operationId, objectId];
  for (let index = 0; index < values.length; index += 1) {
    const value = values[index];
    callIntrinsic(hashUpdateIntrinsic, hash, [String(value.length), "utf8"]);
    callIntrinsic(hashUpdateIntrinsic, hash, ["\0", "utf8"]);
    callIntrinsic(hashUpdateIntrinsic, hash, [value, "utf8"]);
  }
  return `ext4-proof-${callIntrinsic(hashDigestIntrinsic, hash, ["hex"])}`;
}

function sameMutation(left, right) {
  return (
    left.backendId === right.backendId &&
    left.contractVersion === right.contractVersion &&
    left.fencingEpoch === right.fencingEpoch &&
    left.holderId === right.holderId &&
    left.leaseId === right.leaseId &&
    left.operation === right.operation &&
    left.operationId === right.operationId &&
    left.sessionId === right.sessionId &&
    left.storageId === right.storageId &&
    left.target.artifactId === right.target.artifactId &&
    left.target.checkpointId === right.target.checkpointId &&
    left.target.kind === right.target.kind
  );
}

function sha256Json(value) {
  let serialized;
  try {
    serialized = callIntrinsic(jsonStringifyIntrinsic, JSON, [value]);
  } catch {
    fail("invalid_request");
  }
  ensure(typeof serialized === "string", "invalid_request");
  const hash = createHash("sha256");
  callIntrinsic(hashUpdateIntrinsic, hash, [serialized, "utf8"]);
  return callIntrinsic(hashDigestIntrinsic, hash, ["hex"]);
}

function mutationResult(request, proof, rootPath) {
  const values = {
    backendId: request.backendId,
    contractVersion: request.contractVersion,
    fencingEpoch: request.fencingEpoch,
    holderId: request.holderId,
    leaseId: request.leaseId,
    operation: request.operation,
    operationId: request.operationId,
    proofId: proof,
    ...(rootPath === undefined ? {} : { rootPath }),
    sessionId: request.sessionId,
    status: {
      attach: "attached",
      destroy: "destroyed",
      detach: "detached",
    }[request.operation],
    storageId: request.storageId,
    target: request.target,
  };
  const result = exactFrozenRecord(values);
  if (rootPath === undefined) {
    assertStorageMutationResult(result, { request });
  } else {
    assertWriterAttachmentMutationResult(result, { request });
  }
  return result;
}

function provisionResult(request, storageId, proof) {
  const result = exactFrozenRecord({
    backendId: request.backendId,
    contractVersion: request.contractVersion,
    operationId: request.operationId,
    proofId: proof,
    sessionId: request.sessionId,
    status: "provisioned",
    storageId,
  });
  assertSessionProvisionResult(result, { request });
  return result;
}

function sessionAttachment(request, result) {
  return exactFrozenRecord({
    attachmentId: request.target.attachmentId,
    backendId: request.backendId,
    contractVersion: request.contractVersion,
    fencingEpoch: request.fencingEpoch,
    holderId: request.holderId,
    kind: "directory",
    leaseId: request.leaseId,
    mode: "read-write",
    operationId: request.operationId,
    proofId: result.proofId,
    rootPath: result.rootPath,
    sessionId: request.sessionId,
    storageId: request.storageId,
  });
}

function planProvisionForStorage(paths, backendId, storage) {
  const plan = callIntrinsic(paths.planProvision, paths.receiver, [
    exactFrozenRecord({
      backendId,
      contractVersion: STORAGE_CONTRACT_VERSION,
      operationId: `cold-open-${storage.storageId}`,
      sessionId: storage.sessionId,
    }),
  ]);
  ensure(
    plan.storageId === storage.storageId &&
      plan.imagePath === storage.imagePath &&
      plan.mountPath === storage.mount?.mountPath,
    "physical_state_mismatch",
  );
  return plan;
}

export function createExt4FilesystemImageBackend(...args) {
  ensure(args.length === 1, "invalid_options");
  const options = inspectExactDataObject(args[0], OPTION_KEYS, "invalid_options");
  ensure(
    typeof options.backendId === "string" &&
      regexpTest(OPAQUE_ID_PATTERN, options.backendId) &&
      Number.isSafeInteger(options.imageSizeBytes) &&
      options.imageSizeBytes >= MIN_IMAGE_SIZE_BYTES &&
      options.imageSizeBytes <= MAX_IMAGE_SIZE_BYTES &&
      options.imageSizeBytes % IMAGE_SIZE_ALIGNMENT_BYTES === 0,
    "invalid_options",
  );

  const pathsValue = options.paths;
  ensure(
    pathsValue !== null &&
      typeof pathsValue === "object" &&
      !isProxy(pathsValue) &&
      objectGetPrototypeOfIntrinsic(pathsValue) === null &&
      objectIsFrozenIntrinsic(pathsValue) &&
      ownDataValue(pathsValue, "contractVersion", "invalid_options") ===
        EXT4_FILESYSTEM_IMAGE_PATHS_CONTRACT_VERSION &&
      ownDataValue(pathsValue, "backendId", "invalid_options") ===
        options.backendId,
    "invalid_options",
  );
  const paths = objectCreateIntrinsic(null);
  paths.receiver = pathsValue;
  for (let index = 0; index < PATH_METHOD_KEYS.length; index += 1) {
    const name = PATH_METHOD_KEYS[index];
    paths[name] = trustedFunction(
      ownDataValue(pathsValue, name, "invalid_options"),
      "invalid_options",
    );
  }
  objectFreezeIntrinsic(paths);

  const driverValue = options.driver;
  ensure(
    driverValue !== null &&
      typeof driverValue === "object" &&
      !isProxy(driverValue) &&
      objectGetPrototypeOfIntrinsic(driverValue) === null &&
      objectIsFrozenIntrinsic(driverValue) &&
      ownDataValue(driverValue, "contractVersion", "invalid_options") ===
        LINUX_EXT4_IMAGE_DRIVER_CONTRACT_VERSION,
    "invalid_options",
  );
  const driver = objectCreateIntrinsic(null);
  driver.receiver = driverValue;
  for (let index = 0; index < DRIVER_METHOD_KEYS.length; index += 1) {
    const name = DRIVER_METHOD_KEYS[index];
    driver[name] = trustedFunction(
      ownDataValue(driverValue, name, "invalid_options"),
      "invalid_options",
    );
  }
  objectFreezeIntrinsic(driver);

  ensure(
    !isProxy(options.state) &&
      options.state instanceof FilesystemImageProviderState &&
      objectIsFrozenIntrinsic(options.state),
    "invalid_options",
  );
  const state = options.state;
  const backendId = options.backendId;

  async function callState(method, input) {
    return nativePromise(callIntrinsic(method, state, [input]), "physical_effect_ambiguous");
  }

  async function callDriver(method, input) {
    return nativePromise(
      callIntrinsic(driver[method], driver.receiver, [input]),
      "physical_effect_ambiguous",
    );
  }

  function callPath(method, input) {
    try {
      return callIntrinsic(paths[method], paths.receiver, [input]);
    } catch {
      fail("invalid_request");
    }
  }

  function mountRequest(plan) {
    return exactFrozenRecord({
      imagePath: plan.imagePath,
      mountPath: plan.mountPath,
    });
  }

  function remountRequest(plan, expectedPublicationControlIdentity) {
    return exactFrozenRecord({
      expectedPublicationControlIdentity,
      imagePath: plan.imagePath,
      mountPath: plan.mountPath,
    });
  }

  function attachmentRequest(plan) {
    return exactFrozenRecord({
      attachmentRootPath: plan.attachmentRootPath,
      imagePath: plan.imagePath,
      mountPath: plan.mountPath,
    });
  }

  async function observeMount(plan) {
    return callDriver("observeMount", mountRequest(plan));
  }

  async function observeAttachment(plan) {
    return callDriver("observeAttachmentRoot", attachmentRequest(plan));
  }

  async function ensurePublicationControl(plan, mounted, expectedIdentity) {
    const result = await callDriver(
      "ensurePublicationRoot",
      exactFrozenRecord({
        expectedPublicationControlIdentity:
          expectedIdentity === null
            ? null
            : exactFrozenRecord(identity(expectedIdentity)),
        imagePath: plan.imagePath,
        mountPath: plan.mountPath,
      }),
    );
    return publicationControlReceipt(
      result,
      plan,
      mounted,
      expectedIdentity,
    );
  }

  function ensureBackendStorage(storage, storageId, sessionId) {
    ensure(
      storage !== null &&
        storage.backendId === backendId &&
        storage.storageId === storageId &&
        storage.sessionId === sessionId &&
        storage.lifecycle !== "destroyed" &&
        storage.mount !== null,
      "physical_state_mismatch",
    );
    return storage;
  }

  function canonicalOwnedRoot(value) {
    ensure(
      typeof value === "string" &&
        isAbsolute(value) &&
        resolve(value) === value,
      "invalid_request",
    );
    return value;
  }

  async function storageForPublicationRoot(rootValue) {
    const root = canonicalOwnedRoot(rootValue);
    try {
      return await callState(
        stateReadStorageByMountPathIntrinsic,
        exactFrozenRecord({ backendId, mountPath: root }),
      );
    } catch (error) {
      if (safeErrorCode(error) === "storage_lookup_ambiguous") {
        fail("physical_state_mismatch");
      }
      throw error;
    }
  }

  function expectedPublicationControl(storage) {
    ensure(
      storage !== null && storage.publicationControlIdentity !== null,
      "physical_state_mismatch",
    );
    const control = identity(storage.publicationControlIdentity);
    return exactFrozenRecord({
      filesystem: exactFrozenRecord({
        durability: "local-fsync-rename",
        filesystemId: control.filesystemId,
        objectIdentityScheme: control.objectIdentityScheme,
        type: "ext4",
      }),
      objectId: control.objectId,
    });
  }

  async function committedOrPrepared(
    kind,
    operationId,
    request,
    storageId,
    expectedStorageState,
  ) {
    const input = {
      kind,
      operationId,
      request,
      storageId,
    };
    if (expectedStorageState !== undefined) {
      input.expectedStorageState = expectedStorageState;
    }
    return callState(statePrepareOperationIntrinsic, exactFrozenRecord(input));
  }

  function writerAuthority(request) {
    return exactFrozenRecord({
      fencingEpoch: request.fencingEpoch,
      holderId: request.holderId,
      leaseId: request.leaseId,
    });
  }

  function sameWriterAuthority(authority, request) {
    return (
      authority !== null &&
      authority.fencingEpoch === request.fencingEpoch &&
      authority.holderId === request.holderId &&
      authority.leaseId === request.leaseId
    );
  }

  function validateAttachPrecondition(storageValue, request) {
    const storage = ensureBackendStorage(
      storageValue,
      request.storageId,
      request.sessionId,
    );
    ensure(
      (storage.lifecycle === "provisioned" ||
        storage.lifecycle === "detached") &&
        laterEpoch(request.fencingEpoch, storage.writerEpoch),
      "physical_state_mismatch",
    );
    return storage;
  }

  function validateDetachPrecondition(storageValue, request) {
    const storage = ensureBackendStorage(
      storageValue,
      request.storageId,
      request.sessionId,
    );
    ensure(
      storage.lifecycle === "attached" &&
        storage.attachment !== null &&
        storage.attachment.attachmentId === request.target.attachmentId &&
        sameWriterAuthority(storage.writerAuthority, request) &&
        storage.attachment.fencingEpoch === request.fencingEpoch &&
        storage.attachment.holderId === request.holderId &&
        storage.attachment.leaseId === request.leaseId,
      "physical_state_mismatch",
    );
    return storage;
  }

  function validateDestroyPrecondition(storageValue, request) {
    const storage = ensureBackendStorage(
      storageValue,
      request.storageId,
      request.sessionId,
    );
    ensure(
      storage.lifecycle === "provisioned" || storage.lifecycle === "detached",
      "physical_state_mismatch",
    );
    if (storage.writerAuthority === null) {
      ensure(
        storage.lifecycle === "provisioned" &&
          laterEpoch(request.fencingEpoch, storage.writerEpoch),
        "physical_state_mismatch",
      );
    } else {
      ensure(
        sameWriterAuthority(storage.writerAuthority, request),
        "physical_state_mismatch",
      );
    }
    return storage;
  }

  async function inspectMutationBeforePrepare(
    mutationRequest,
    validateStorage,
    operationRequest = mutationRequest,
  ) {
    const existing = await callState(
      stateReadOperationIntrinsic,
      exactFrozenRecord({
        operationId: mutationRequest.operationId,
        request: operationRequest,
      }),
    );
    if (existing?.state === "committed") {
      return { existing, expectedStorageState: null, storage: null };
    }

    const expectedStorageState =
      existing === null
        ? await callState(stateReadStorageIntrinsic, mutationRequest.storageId)
        : existing.storageStateBefore;
    const storage = validateStorage(expectedStorageState, mutationRequest);
    return { existing, expectedStorageState, storage };
  }

  async function preflightMutation(
    kind,
    mutationRequest,
    validateStorage,
    operationRequest = mutationRequest,
    inspectedValue = null,
  ) {
    const inspected =
      inspectedValue ??
      await inspectMutationBeforePrepare(
        mutationRequest,
        validateStorage,
        operationRequest,
      );
    if (inspected.existing?.state === "committed") return inspected.existing;

    const prepared = await committedOrPrepared(
      kind,
      mutationRequest.operationId,
      operationRequest,
      mutationRequest.storageId,
      inspected.expectedStorageState,
    );
    if (prepared.state !== "committed") {
      validateStorage(prepared.storageStateBefore, mutationRequest);
    }
    return prepared;
  }

  async function commit(operationId, request, result, storageState) {
    const receipt = await callState(stateCommitOperationIntrinsic, exactFrozenRecord({
      operationId,
      request,
      result,
      storageState,
    }));
    return receipt.result;
  }

  function replayIfCommitted(prepared) {
    return prepared.state === "committed" ? prepared.result : null;
  }

  async function provisionSession(requestValue, contextValue) {
    physicalContext(contextValue);
    const request = assertSessionProvisionRequest(
      clonePlainData(requestValue, "invalid_request"),
    );
    ensure(request.backendId === backendId, "invalid_request");
    const plan = callPath("planProvision", request);
    const existing = await callState(
      stateReadOperationIntrinsic,
      exactFrozenRecord({
        operationId: request.operationId,
        request,
      }),
    );
    if (existing === null) {
      try {
        callIntrinsic(assertMountPathCapacityIntrinsic, undefined, [
          plan.mountPath,
        ]);
      } catch {
        fail("invalid_request");
      }
    }
    const prepared = await committedOrPrepared(
      "provision",
      request.operationId,
      request,
      plan.storageId,
    );
    const replay = replayIfCommitted(prepared);
    if (replay !== null) return replay;
    let observation;
    if (prepared.replayed) {
      try {
        observation = await observeMount(plan);
      } catch {
        fail("physical_effect_ambiguous");
      }
    } else {
      observation = await callDriver("provision", exactFrozenRecord({
        imagePath: plan.imagePath,
        imageSizeBytes: options.imageSizeBytes,
        mountPath: plan.mountPath,
      }));
    }
    observation = provisionObservation(observation, plan);
    const publicationControl = await ensurePublicationControl(
      plan,
      observation,
      null,
    );
    const mount = stateMount(observation);
    const result = provisionResult(
      request,
      plan.storageId,
      proofId(backendId, "provision", request.operationId, mount.imageIdentity.objectId),
    );
    const storageState = exactFrozenRecord({
      attachment: null,
      backendId,
      dataRoot: null,
      filesystemId: mount.rootIdentity.filesystemId,
      imagePath: plan.imagePath,
      lifecycle: "provisioned",
      mount,
      publicationControlIdentity:
        publicationControl.publicationControlIdentity,
      revision: "1",
      sessionId: request.sessionId,
      storageId: plan.storageId,
      writerAuthority: null,
      writerEpoch: "0",
    });
    return commit(request.operationId, request, result, storageState);
  }

  async function prepareWritableAttachment(requestValue, contextValue) {
    physicalContext(contextValue);
    const request = assertStorageMutationRequest(
      clonePlainData(requestValue, "invalid_request"),
    );
    ensure(
      request.backendId === backendId && request.operation === "attach",
      "invalid_request",
    );
    const planned = callPath("planWritableAttachment", request);
    const inspected = await inspectMutationBeforePrepare(
      request,
      validateAttachPrecondition,
    );
    if (inspected.storage !== null) {
      ensure(
        planned.storageId === inspected.storage.storageId &&
          planned.imagePath === inspected.storage.imagePath &&
          planned.mountPath === inspected.storage.mount.mountPath &&
          typeof planned.attachmentRootPath === "string" &&
          dirname(planned.attachmentRootPath) === planned.mountPath &&
          regexpTest(
            DATA_CHILD_PATTERN,
            basename(planned.attachmentRootPath),
          ),
        "physical_state_mismatch",
      );
    }
    const prepared = await preflightMutation(
      "attach",
      request,
      validateAttachPrecondition,
      request,
      inspected,
    );
    const replay = replayIfCommitted(prepared);
    if (replay !== null) return replay;
    const storage = ensureBackendStorage(
      prepared.storageStateBefore,
      request.storageId,
      request.sessionId,
    );
    const plan = exactFrozenRecord({
      attachmentRootPath:
        storage.dataRoot === null
          ? planned.attachmentRootPath
          : storage.dataRoot.rootPath,
      imagePath: planned.imagePath,
      mountPath: planned.mountPath,
      storageId: planned.storageId,
    });
    let observation;
    if (prepared.replayed || storage.dataRoot !== null) {
      try {
        observation = await observeAttachment(plan);
      } catch {
        fail("physical_effect_ambiguous");
      }
    } else {
      observation = await callDriver("ensureAttachmentRoot", attachmentRequest(plan));
    }
    const attached = sameAttachmentObservation(observation, storage, plan);
    const proof = proofId(
      backendId,
      "attach",
      request.operationId,
      attached.rootIdentity.objectId,
    );
    const result = mutationResult(request, proof, plan.attachmentRootPath);
    const storageState = exactFrozenRecord({
      ...storage,
      attachment: exactFrozenRecord({
        attachmentId: request.target.attachmentId,
        fencingEpoch: request.fencingEpoch,
        holderId: request.holderId,
        imageIdentity: exactFrozenRecord(identity(attached.imageIdentity)),
        leaseId: request.leaseId,
        proofId: proof,
        rootIdentity: exactFrozenRecord(identity(attached.rootIdentity)),
        rootPath: plan.attachmentRootPath,
      }),
      dataRoot: exactFrozenRecord({
        imageIdentity: exactFrozenRecord(identity(attached.imageIdentity)),
        rootIdentity: exactFrozenRecord(identity(attached.rootIdentity)),
        rootPath: plan.attachmentRootPath,
      }),
      lifecycle: "attached",
      revision: increment(storage.revision),
      writerAuthority: writerAuthority(request),
      writerEpoch: request.fencingEpoch,
    });
    return commit(request.operationId, request, result, storageState);
  }

  async function detachAttachment(requestValue, contextValue) {
    physicalContext(contextValue);
    const request = assertStorageMutationRequest(
      clonePlainData(requestValue, "invalid_request"),
    );
    ensure(
      request.backendId === backendId && request.operation === "detach",
      "invalid_request",
    );
    const prepared = await preflightMutation(
      "detach",
      request,
      validateDetachPrecondition,
    );
    const replay = replayIfCommitted(prepared);
    if (replay !== null) return replay;
    const storage = ensureBackendStorage(
      prepared.storageStateBefore,
      request.storageId,
      request.sessionId,
    );
    validateDetachPrecondition(storage, request);
    const plan = planProvisionForStorage(paths, backendId, storage);
    if (prepared.replayed) {
      try {
        sameMountObservation(await observeMount(plan), storage, plan);
        const attachmentPlan = exactFrozenRecord({
          attachmentRootPath: storage.dataRoot.rootPath,
          imagePath: plan.imagePath,
          mountPath: plan.mountPath,
        });
        const observed = await observeAttachment(attachmentPlan);
        sameAttachmentObservation(observed, storage, attachmentPlan);
      } catch {
        // Observation cannot prove whether the required sync barrier completed.
      }
      fail("physical_effect_ambiguous");
    }
    await callDriver("syncFilesystem", mountRequest(plan));
    const result = mutationResult(
      request,
      proofId(backendId, "detach", request.operationId, storage.mount.rootIdentity.objectId),
    );
    const storageState = exactFrozenRecord({
      ...storage,
      attachment: null,
      lifecycle: "detached",
      revision: increment(storage.revision),
    });
    return commit(request.operationId, request, result, storageState);
  }

  async function destroySession(requestValue, contextValue) {
    physicalContext(contextValue);
    const request = assertStorageMutationRequest(
      clonePlainData(requestValue, "invalid_request"),
    );
    ensure(
      request.backendId === backendId && request.operation === "destroy",
      "invalid_request",
    );
    const prepared = await preflightMutation(
      "destroy",
      request,
      validateDestroyPrecondition,
    );
    const replay = replayIfCommitted(prepared);
    if (replay !== null) return replay;
    const storage = ensureBackendStorage(
      prepared.storageStateBefore,
      request.storageId,
      request.sessionId,
    );
    validateDestroyPrecondition(storage, request);
    const plan = planProvisionForStorage(paths, backendId, storage);
    if (prepared.replayed) {
      try {
        await observeMount(plan);
      } catch {
        // Neither a missing mount nor a mismatching mount proves image removal.
      }
      fail("physical_effect_ambiguous");
    }
    await callDriver("destroy", mountRequest(plan));
    const result = mutationResult(
      request,
      proofId(backendId, "destroy", request.operationId, storage.mount.imageIdentity.objectId),
    );
    const storageState = exactFrozenRecord({
      ...storage,
      attachment: null,
      dataRoot: null,
      lifecycle: "destroyed",
      mount: null,
      publicationControlIdentity: null,
      revision: increment(storage.revision),
    });
    return commit(request.operationId, request, result, storageState);
  }

  async function prepareRestoreAttachment(requestValue, contextValue) {
    physicalContext(contextValue);
    const request = assertRestoreAttachmentActivationRequest(
      clonePlainData(requestValue, "invalid_request"),
    );
    ensure(
      request.storageRef.backendId === backendId &&
        request.mutationRequest.backendId === backendId,
      "invalid_request",
    );
    const mutation = request.mutationRequest;
    const rootPath = request.publication.root.rootPath;
    const inspected = await inspectMutationBeforePrepare(
      mutation,
      validateAttachPrecondition,
      request,
    );
    let plan = null;
    if (inspected.storage !== null) {
      plan = planProvisionForStorage(paths, backendId, inspected.storage);
      ensure(
        rootPath.length <= MAX_PATH_BYTES &&
          callIntrinsic(bufferByteLengthIntrinsic, Buffer, [rootPath, "utf8"]) <=
            MAX_PATH_BYTES &&
          dirname(rootPath) === plan.mountPath &&
          regexpTest(RESTORE_CHILD_PATTERN, basename(rootPath)),
        "physical_state_mismatch",
      );
    }
    const prepared = await preflightMutation(
      "restore-attach",
      mutation,
      validateAttachPrecondition,
      request,
      inspected,
    );
    const replay = replayIfCommitted(prepared);
    if (replay !== null) return replay;
    const storage = ensureBackendStorage(
      prepared.storageStateBefore,
      mutation.storageId,
      mutation.sessionId,
    );
    ensure(plan !== null, "physical_state_mismatch");
    const restorePlan = exactFrozenRecord({
      attachmentRootPath: rootPath,
      imagePath: plan.imagePath,
      mountPath: plan.mountPath,
    });
    let observation;
    try {
      observation = await observeAttachment(restorePlan);
    } catch {
      fail("physical_effect_ambiguous");
    }
    const attached = sameAttachmentObservation(observation, storage, restorePlan);
    ensure(
      sameIdentity(attached.rootIdentity, request.publication.root),
      "physical_state_mismatch",
    );
    const proof = proofId(
      backendId,
      "restore-attach",
      mutation.operationId,
      attached.rootIdentity.objectId,
    );
    const mutationResultValue = mutationResult(mutation, proof);
    const attachment = sessionAttachment(
      mutation,
      exactFrozenRecord({ ...mutationResultValue, rootPath }),
    );
    const result = exactFrozenRecord({
      attachment,
      contractVersion: RESTORE_ATTACHMENT_ACTIVATION_CONTRACT_VERSION,
      mutationResult: mutationResultValue,
      publication: request.publication,
    });
    assertRestoreAttachmentActivationResult(result, { request });
    const storageState = exactFrozenRecord({
      ...storage,
      attachment: exactFrozenRecord({
        attachmentId: mutation.target.attachmentId,
        fencingEpoch: mutation.fencingEpoch,
        holderId: mutation.holderId,
        imageIdentity: exactFrozenRecord(identity(attached.imageIdentity)),
        leaseId: mutation.leaseId,
        proofId: proof,
        rootIdentity: exactFrozenRecord(identity(attached.rootIdentity)),
        rootPath,
      }),
      dataRoot: exactFrozenRecord({
        imageIdentity: exactFrozenRecord(identity(attached.imageIdentity)),
        rootIdentity: exactFrozenRecord(identity(attached.rootIdentity)),
        rootPath,
      }),
      lifecycle: "attached",
      revision: increment(storage.revision),
      writerAuthority: writerAuthority(mutation),
      writerEpoch: mutation.fencingEpoch,
    });
    return commit(mutation.operationId, request, result, storageState);
  }

  async function reconcileRestoreAttachment(requestValue, contextValue) {
    physicalContext(contextValue);
    const request = assertRestoreAttachmentActivationRequest(
      clonePlainData(requestValue, "invalid_request"),
    );
    ensure(request.storageRef.backendId === backendId, "invalid_request");
    const mutation = request.mutationRequest;
    let operation;
    try {
      operation = await callState(stateReadOperationIntrinsic, exactFrozenRecord({
        operationId: mutation.operationId,
        request,
      }));
    } catch {
      const unknown = exactFrozenRecord({
        contractVersion: RESTORE_ATTACHMENT_RECONCILIATION_CONTRACT_VERSION,
        outcome: "unknown",
      });
      assertRestoreAttachmentReconciliationResult(unknown, { request });
      return unknown;
    }
    let result;
    if (operation?.state === "committed") {
      result = exactFrozenRecord({
        contractVersion: RESTORE_ATTACHMENT_RECONCILIATION_CONTRACT_VERSION,
        outcome: "applied",
        result: operation.result,
      });
    } else if (operation !== null) {
      result = exactFrozenRecord({
        contractVersion: RESTORE_ATTACHMENT_RECONCILIATION_CONTRACT_VERSION,
        outcome: "unknown",
      });
    } else {
      const storage = await callState(stateReadStorageIntrinsic, mutation.storageId);
      try {
        ensureBackendStorage(storage, mutation.storageId, mutation.sessionId);
        ensure(
          storage.lifecycle === "provisioned" || storage.lifecycle === "detached",
          "physical_state_mismatch",
        );
        const plan = planProvisionForStorage(paths, backendId, storage);
        sameMountObservation(await observeMount(plan), storage, plan);
        const rootPath = request.publication.root.rootPath;
        ensure(
          dirname(rootPath) === plan.mountPath &&
            regexpTest(RESTORE_CHILD_PATTERN, basename(rootPath)),
          "physical_state_mismatch",
        );
        await observeAttachment(exactFrozenRecord({
          attachmentRootPath: rootPath,
          imagePath: plan.imagePath,
          mountPath: plan.mountPath,
        }));
        result = exactFrozenRecord({
          contractVersion: RESTORE_ATTACHMENT_RECONCILIATION_CONTRACT_VERSION,
          outcome: "unknown",
        });
      } catch (error) {
        result = exactFrozenRecord({
          contractVersion: RESTORE_ATTACHMENT_RECONCILIATION_CONTRACT_VERSION,
          outcome:
            safeErrorCode(error) === "attachment_root_absent"
              ? "absent-and-quiescent"
              : "unknown",
        });
      }
    }
    assertRestoreAttachmentReconciliationResult(result, { request });
    return result;
  }

  async function forceFence(requestValue, contextValue) {
    physicalContext(contextValue);
    const request = assertStorageForceFenceRequest(
      clonePlainData(requestValue, "invalid_request"),
    );
    ensure(request.backendId === backendId, "invalid_request");
    fail("fence_unavailable");
  }

  async function unsupportedCheckpoint(requestValue, contextValue) {
    physicalContext(contextValue);
    clonePlainData(requestValue, "invalid_request");
    fail("checkpoint_unsupported");
  }

  let lifecycleBackend;
  const lifecycleMethod = (operation) =>
    objectFreezeIntrinsic(function lifecycleOperation(request, context) {
      if (this !== lifecycleBackend || arguments.length !== 2) {
        throw new TypeError("invalid ext4 filesystem image backend receiver");
      }
      return operation(request, context);
    });

  const capabilities = exactFrozenRecord({
    atomicPointInTimeCheckpoint: false,
    exclusiveWriterAttachment: true,
    fencing: "manual",
    normalDirectoryAttachment: true,
  });
  lifecycleBackend = exactFrozenRecord({
    backendId,
    capabilities,
    contractVersion: STORAGE_CONTRACT_VERSION,
    physicalInvocationContractVersion:
      EXT4_FILESYSTEM_IMAGE_PHYSICAL_INVOCATION_CONTRACT_VERSION,
    restoreAttachmentActivationContractVersion:
      RESTORE_ATTACHMENT_ACTIVATION_CONTRACT_VERSION,
    restoreAttachmentReconciliationContractVersion:
      RESTORE_ATTACHMENT_RECONCILIATION_CONTRACT_VERSION,
    captureCheckpoint: lifecycleMethod(unsupportedCheckpoint),
    destroySession: lifecycleMethod(destroySession),
    detachAttachment: lifecycleMethod(detachAttachment),
    forceFence: lifecycleMethod(forceFence),
    prepareRestoreAttachment: lifecycleMethod(prepareRestoreAttachment),
    prepareWritableAttachment: lifecycleMethod(prepareWritableAttachment),
    provisionSession: lifecycleMethod(provisionSession),
    reconcileRestoreAttachment: lifecycleMethod(reconcileRestoreAttachment),
    restoreCheckpoint: lifecycleMethod(unsupportedCheckpoint),
  });

  async function initializeInternal() {
    const snapshot = await callState(stateSnapshotIntrinsic, undefined);
    for (let index = 0; index < snapshot.storages.length; index += 1) {
      const storage = snapshot.storages[index];
      if (storage.backendId !== backendId || storage.lifecycle === "destroyed") continue;
      const plan = planProvisionForStorage(paths, backendId, storage);
      let mounted;
      try {
        mounted = await observeMount(plan);
      } catch (error) {
        if (
          safeErrorCode(error) !== "mount_absent" ||
          storage.lifecycle === "attached"
        ) {
          fail("cold_open_failed");
        }
        await callDriver(
          "remount",
          remountRequest(plan, storage.publicationControlIdentity),
        );
        try {
          mounted = await observeMount(plan);
        } catch {
          fail("cold_open_failed");
        }
      }
      try {
        mounted = sameMountObservation(mounted, storage, plan);
        await ensurePublicationControl(
          plan,
          mounted,
          storage.publicationControlIdentity,
        );
        if (storage.dataRoot !== null) {
          const dataRootPlan = exactFrozenRecord({
            attachmentRootPath: storage.dataRoot.rootPath,
            imagePath: plan.imagePath,
            mountPath: plan.mountPath,
          });
          const observed = await observeAttachment(dataRootPlan);
          sameAttachmentObservation(observed, storage, dataRootPlan);
          ensure(
            sameIdentity(observed.rootIdentity, storage.dataRoot.rootIdentity),
            "cold_open_failed",
          );
        }
      } catch {
        fail("cold_open_failed");
      }
    }
    return exactFrozenRecord({ status: "initialized" });
  }

  async function quiesceStorageInternal(storageIdValue) {
    ensure(
      typeof storageIdValue === "string" &&
        regexpTest(OPAQUE_ID_PATTERN, storageIdValue),
      "invalid_request",
    );
    const storage = await callState(stateReadStorageIntrinsic, storageIdValue);
    ensureBackendStorage(storage, storageIdValue, storage?.sessionId);
    ensure(
      storage.lifecycle === "provisioned" || storage.lifecycle === "detached",
      "physical_state_mismatch",
    );
    const plan = planProvisionForStorage(paths, backendId, storage);
    const mounted = sameMountObservation(
      await observeMount(plan),
      storage,
      plan,
    );
    await ensurePublicationControl(
      plan,
      mounted,
      storage.publicationControlIdentity,
    );
    const result = await callDriver("quiesce", mountRequest(plan));
    const receipt = inspectExactDataObject(
      result,
      ["imagePath", "mountPath", "status"],
      "physical_effect_ambiguous",
    );
    ensure(
      receipt.imagePath === plan.imagePath &&
        receipt.mountPath === plan.mountPath &&
        receipt.status === "quiesced",
      "physical_effect_ambiguous",
    );
    return exactFrozenRecord({ status: "quiesced", storageId: storageIdValue });
  }

  async function resolveRestoreDestinationInternal(value) {
    const raw = inspectExactDataObject(value, RESOLVER_KEYS, "invalid_request", {
      frozen: true,
    });
    ensure(
      raw.contractVersion ===
        EXT4_FILESYSTEM_IMAGE_PHYSICAL_INVOCATION_CONTRACT_VERSION &&
        (raw.kind === "generation" || raw.kind === "activation"),
      "invalid_request",
    );
    inspectExactDataObject(raw.invocation, [], "invalid_request", { frozen: true });
    ensure(!signalAborted(raw.signal, "invalid_request"), "invalid_request");
    const candidate = clonePlainData(raw.candidate, "invalid_request");
    const generation = clonePlainData(raw.generation, "invalid_request");
    const generationRequest = assertStorageMutationRequest(
      generation.binding.request,
    );
    ensure(
      generationRequest.operation === "restore" &&
        generationRequest.backendId === backendId &&
        generationRequest.sessionId === generation.sessionId &&
        generationRequest.operationId === generation.operationId &&
        generationRequest.target.checkpointId === generation.checkpointId &&
        generation.binding.generationId === generation.generationId &&
        typeof generation.generationId === "string" &&
        regexpTest(OPAQUE_ID_PATTERN, generation.generationId),
      "invalid_request",
    );
    if (raw.kind === "generation") {
      const candidateRequest = assertStorageMutationRequest(candidate.request);
      ensure(
        candidate.generationId === generation.generationId &&
          sameMutation(candidateRequest, generationRequest),
        "invalid_request",
      );
    } else {
      const reference = candidate.request?.generation;
      ensure(
        reference !== null &&
          typeof reference === "object" &&
          reference.generationId === generation.generationId &&
          reference.operationId === generation.operationId &&
          reference.sessionId === generation.sessionId &&
          reference.checkpointId === generation.checkpointId &&
          reference.claimedAt === generation.claimedAt &&
          reference.committedAt === generation.committedAt &&
          reference.state === "committed" &&
          generation.state === "committed" &&
          reference.bindingSha256 === sha256Json(generation.binding) &&
          reference.documentSha256 === sha256Json(generation.document),
        "invalid_request",
      );
    }
    const storageId = generationRequest.storageId;
    const destination = callPath("planRestoreDestination", generationRequest);
    if (raw.kind === "activation") {
      ensure(
        candidate.request.destinationRootPath ===
          destination.destinationDirectory,
        "invalid_request",
      );
    }
    const storage = await callState(stateReadStorageIntrinsic, storageId);
    ensureBackendStorage(storage, storageId, generationRequest.sessionId);
    ensure(
      storage.lifecycle === "provisioned" || storage.lifecycle === "detached",
      "physical_state_mismatch",
    );
    const provisionPlan = planProvisionForStorage(paths, backendId, storage);
    ensure(
      destination.destinationOwnedRoot === provisionPlan.mountPath,
      "physical_state_mismatch",
    );
    const mounted = sameMountObservation(
      await observeMount(provisionPlan),
      storage,
      provisionPlan,
    );
    await ensurePublicationControl(
      provisionPlan,
      mounted,
      storage.publicationControlIdentity,
    );
    if (raw.kind === "activation") {
      let materializedIdentity;
      try {
        materializedIdentity = generation.document.materialization.stagedRoot;
      } catch {
        fail("invalid_request");
      }
      const restored = await observeAttachment(exactFrozenRecord({
        attachmentRootPath: destination.destinationDirectory,
        imagePath: provisionPlan.imagePath,
        mountPath: provisionPlan.mountPath,
      }));
      ensure(
        sameIdentity(restored.imageIdentity, storage.mount.imageIdentity) &&
          sameIdentity(restored.mountRootIdentity, storage.mount.rootIdentity) &&
          sameIdentity(restored.rootIdentity, materializedIdentity),
        "physical_state_mismatch",
      );
    }
    return exactFrozenRecord({
      destinationDirectory: destination.destinationDirectory,
      destinationOwnedRoot: destination.destinationOwnedRoot,
    });
  }

  async function resolveExpectedPublicationControlInternal(rootValue) {
    const storage = await storageForPublicationRoot(rootValue);
    return storage === null ? null : expectedPublicationControl(storage);
  }

  async function inspectPublicationControlInternal(lockPathValue) {
    const lockPath = canonicalOwnedRoot(lockPathValue);
    ensure(
      basename(lockPath) === PUBLICATION_CONTROL_FILE_NAME,
      "invalid_request",
    );
    const root = dirname(lockPath);
    const storage = await storageForPublicationRoot(root);
    ensure(storage !== null, "physical_state_mismatch");
    const plan = planProvisionForStorage(paths, backendId, storage);
    const mounted = sameMountObservation(
      await observeMount(plan),
      storage,
      plan,
    );
    const control = await ensurePublicationControl(
      plan,
      mounted,
      storage.publicationControlIdentity,
    );
    return exactFrozenRecord({
      filesystem: control.filesystem,
      identity: control.identity,
    });
  }

  let surface;
  const initialize = objectFreezeIntrinsic(function initialize() {
    if (this !== surface || arguments.length !== 0) {
      throw new TypeError("invalid ext4 filesystem image backend receiver");
    }
    return initializeInternal();
  });
  const quiesceStorage = objectFreezeIntrinsic(function quiesceStorage(storageId) {
    if (this !== surface || arguments.length !== 1) {
      throw new TypeError("invalid ext4 filesystem image backend receiver");
    }
    return quiesceStorageInternal(storageId);
  });
  const resolveRestoreDestination = objectFreezeIntrinsic(
    function resolveRestoreDestination(value) {
      if (this !== undefined || arguments.length !== 1) {
        throw new TypeError("invalid ext4 restore destination resolver receiver");
      }
      return resolveRestoreDestinationInternal(value);
    },
  );
  const resolveExpectedPublicationControl = objectFreezeIntrinsic(
    function resolveExpectedPublicationControl(value) {
      if (this !== undefined || arguments.length !== 1) {
        throw new TypeError(
          "invalid ext4 publication control resolver receiver",
        );
      }
      return resolveExpectedPublicationControlInternal(value);
    },
  );
  const inspectPublicationControl = objectFreezeIntrinsic(
    function inspectPublicationControl(value) {
      if (this !== undefined || arguments.length !== 1) {
        throw new TypeError(
          "invalid ext4 publication control inspector receiver",
        );
      }
      return inspectPublicationControlInternal(value);
    },
  );
  surface = exactFrozenRecord({
    initialize,
    inspectPublicationControl,
    lifecycleBackend,
    quiesceStorage,
    resolveExpectedPublicationControl,
    resolveRestoreDestination,
  });
  weakSetAdd(rawBackendSurfaces, surface);
  return surface;
}

export function createInitializedExt4FilesystemImageBackend(...args) {
  ensure(args.length === 1, "invalid_options");
  const options = inspectExactDataObject(
    args[0],
    ["backend"],
    "invalid_options",
  );
  ensure(weakSetHas(rawBackendSurfaces, options.backend), "invalid_options");

  const backend = options.backend;
  const rawLifecycleBackend = ownDataValue(
    backend,
    "lifecycleBackend",
    "invalid_options",
  );
  const rawInitialize = trustedFunction(
    ownDataValue(backend, "initialize", "invalid_options"),
    "invalid_options",
  );
  const rawQuiesceStorage = trustedFunction(
    ownDataValue(backend, "quiesceStorage", "invalid_options"),
    "invalid_options",
  );
  const rawResolveRestoreDestination = trustedFunction(
    ownDataValue(backend, "resolveRestoreDestination", "invalid_options"),
    "invalid_options",
  );
  const rawResolveExpectedPublicationControl = trustedFunction(
    ownDataValue(
      backend,
      "resolveExpectedPublicationControl",
      "invalid_options",
    ),
    "invalid_options",
  );
  const rawInspectPublicationControl = trustedFunction(
    ownDataValue(backend, "inspectPublicationControl", "invalid_options"),
    "invalid_options",
  );

  let initializationPromise = null;
  function initializeOnce() {
    if (initializationPromise === null) {
      initializationPromise = nativePromise(
        callIntrinsic(rawInitialize, backend, []),
        "physical_effect_ambiguous",
      );
    }
    return initializationPromise;
  }

  async function dispatchLifecycle(method, request, context) {
    await initializeOnce();
    return callIntrinsic(method, rawLifecycleBackend, [request, context]);
  }

  let lifecycleBackend;
  const lifecycleValues = objectCreateIntrinsic(null);
  for (let index = 0; index < LIFECYCLE_METADATA_KEYS.length; index += 1) {
    const name = LIFECYCLE_METADATA_KEYS[index];
    lifecycleValues[name] = ownDataValue(
      rawLifecycleBackend,
      name,
      "invalid_options",
    );
  }
  for (let index = 0; index < LIFECYCLE_METHOD_KEYS.length; index += 1) {
    const name = LIFECYCLE_METHOD_KEYS[index];
    const rawMethod = trustedFunction(
      ownDataValue(rawLifecycleBackend, name, "invalid_options"),
      "invalid_options",
    );
    lifecycleValues[name] = objectFreezeIntrinsic(
      function initializedLifecycleOperation(request, context) {
        if (this !== lifecycleBackend || arguments.length !== 2) {
          throw new TypeError("invalid ext4 filesystem image backend receiver");
        }
        return dispatchLifecycle(rawMethod, request, context);
      },
    );
  }
  lifecycleBackend = exactFrozenRecord(lifecycleValues);

  let surface;
  const initialize = objectFreezeIntrinsic(function initialize() {
    if (this !== surface || arguments.length !== 0) {
      throw new TypeError("invalid ext4 filesystem image backend receiver");
    }
    return initializeOnce();
  });
  const quiesceStorage = objectFreezeIntrinsic(function quiesceStorage(storageId) {
    if (this !== surface || arguments.length !== 1) {
      throw new TypeError("invalid ext4 filesystem image backend receiver");
    }
    return (async () => {
      await initializeOnce();
      return callIntrinsic(rawQuiesceStorage, backend, [storageId]);
    })();
  });
  const resolveRestoreDestination = objectFreezeIntrinsic(
    function resolveRestoreDestination(value) {
      if (this !== undefined || arguments.length !== 1) {
        throw new TypeError("invalid ext4 restore destination resolver receiver");
      }
      return (async () => {
        await initializeOnce();
        return callIntrinsic(rawResolveRestoreDestination, undefined, [value]);
      })();
    },
  );
  const resolveExpectedPublicationControl = objectFreezeIntrinsic(
    function resolveExpectedPublicationControl(value) {
      if (this !== undefined || arguments.length !== 1) {
        throw new TypeError(
          "invalid ext4 publication control resolver receiver",
        );
      }
      return (async () => {
        await initializeOnce();
        return callIntrinsic(
          rawResolveExpectedPublicationControl,
          undefined,
          [value],
        );
      })();
    },
  );
  const inspectPublicationControl = objectFreezeIntrinsic(
    function inspectPublicationControl(value) {
      if (this !== undefined || arguments.length !== 1) {
        throw new TypeError(
          "invalid ext4 publication control inspector receiver",
        );
      }
      return (async () => {
        await initializeOnce();
        return callIntrinsic(rawInspectPublicationControl, undefined, [value]);
      })();
    },
  );
  surface = exactFrozenRecord({
    initialize,
    inspectPublicationControl,
    lifecycleBackend,
    quiesceStorage,
    resolveExpectedPublicationControl,
    resolveRestoreDestination,
  });
  return surface;
}

objectFreezeIntrinsic(Ext4FilesystemImageBackendError.prototype);
objectFreezeIntrinsic(Ext4FilesystemImageBackendError);
objectFreezeIntrinsic(createExt4FilesystemImageBackend);
objectFreezeIntrinsic(createInitializedExt4FilesystemImageBackend);
