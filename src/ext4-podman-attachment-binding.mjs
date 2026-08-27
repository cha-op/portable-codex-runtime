import { Buffer } from "node:buffer";
import { Hash, createHash } from "node:crypto";
import { types as utilTypes } from "node:util";

import {
  createExt4FilesystemImageBackend,
  createInitializedExt4FilesystemImageBackend,
} from "./ext4-filesystem-image-backend.mjs";
import {
  FilesystemImageProviderState,
} from "./filesystem-image-provider-state.mjs";
import {
  LINUX_EXT4_ATTACHMENT_ROOT_AUTHORITY_CONTRACT_VERSION,
  LINUX_EXT4_IMAGE_DRIVER_CONTRACT_VERSION,
  LinuxExt4ImageDriverError,
} from "./linux-ext4-image-driver.mjs";
import {
  createPodmanWriterFilesystemAuthorityComposition,
} from "./podman-writer-supervisor.mjs";
import {
  assertRestoreAttachmentActivationRequest,
  assertRestoreAttachmentActivationResult,
  assertSessionAttachment,
  assertStorageMutationRequest,
  assertWriterAttachmentMutationResult,
} from "./session-storage-contracts.mjs";

const { isGeneratorFunction, isPromise, isProxy } = utilTypes;
const arrayIncludesIntrinsic = Array.prototype.includes;
const arrayIsArrayIntrinsic = Array.isArray;
const arrayJoinIntrinsic = Array.prototype.join;
const arrayPushIntrinsic = Array.prototype.push;
const arraySortIntrinsic = Array.prototype.sort;
const bufferByteLengthIntrinsic = Buffer.byteLength;
const BigIntConstructor = BigInt;
const createHashIntrinsic = createHash;
const hashDigestIntrinsic = Hash.prototype.digest;
const hashUpdateIntrinsic = Hash.prototype.update;
const jsonStringifyIntrinsic = JSON.stringify;
const numberIsFiniteIntrinsic = Number.isFinite;
const objectAssignIntrinsic = Object.assign;
const objectCreateIntrinsic = Object.create;
const objectFreezeIntrinsic = Object.freeze;
const objectGetOwnPropertyDescriptorIntrinsic = Object.getOwnPropertyDescriptor;
const objectGetPrototypeOfIntrinsic = Object.getPrototypeOf;
const objectHasOwnIntrinsic = Object.hasOwn;
const objectIsIntrinsic = Object.is;
const objectIsFrozenIntrinsic = Object.isFrozen;
const objectPrototype = Object.prototype;
const promisePrototype = Promise.prototype;
const reflectApplyIntrinsic = Reflect.apply;
const reflectOwnKeysIntrinsic = Reflect.ownKeys;
const regexpExecIntrinsic = RegExp.prototype.exec;
const stringConstructor = String;
const stateReadOperationIntrinsic =
  FilesystemImageProviderState.prototype.readOperation;
const EMPTY_ARGUMENTS = objectFreezeIntrinsic([]);

const BACKEND_OPTION_KEYS = objectFreezeIntrinsic([
  "backendId",
  "driver",
  "imageSizeBytes",
  "paths",
  "state",
]);
const PERSISTENT_AUTHORITY_INPUT_KEYS = objectFreezeIntrinsic(["attachment"]);
const DRIVER_AUTHORITY_OBSERVATION_KEYS = objectFreezeIntrinsic([
  "attachmentRootPath",
  "filesystem",
  "imageIdentity",
  "imagePath",
  "loopDevice",
  "mountEvidence",
  "mountPath",
  "mountRootIdentity",
  "rootIdentity",
  "rootRuntimeIdentity",
]);
const FILESYSTEM_KEYS = objectFreezeIntrinsic([
  "durability",
  "filesystemId",
  "objectIdentityScheme",
  "type",
]);
const RUNTIME_IDENTITY_KEYS = objectFreezeIntrinsic(["device", "inode"]);
const IDENTITY_KEYS = objectFreezeIntrinsic([
  "filesystemId",
  "objectIdentityScheme",
  "objectId",
]);
const DECIMAL_PATTERN = /^(?:0|[1-9][0-9]*)$/u;
const POSITIVE_DECIMAL_PATTERN = /^[1-9][0-9]*$/u;
const MAX_CANONICAL_DEPTH = 32;
const MAX_CANONICAL_NODES = 32_768;
const MAX_CANONICAL_BYTES = 4 * 1024 * 1024;

export const EXT4_PODMAN_ATTACHMENT_BINDING_CONTRACT_VERSION = 2;
export const EXT4_PODMAN_PERSISTENT_AUTHORITY_CONTRACT_VERSION = 2;

function callIntrinsic(intrinsic, receiver, args) {
  return reflectApplyIntrinsic(intrinsic, receiver, args);
}

function regexpTest(pattern, value) {
  return callIntrinsic(regexpExecIntrinsic, pattern, [value]) !== null;
}

function frozenRecord(values) {
  return callIntrinsic(objectFreezeIntrinsic, Object, [
    callIntrinsic(objectAssignIntrinsic, Object, [
      callIntrinsic(objectCreateIntrinsic, Object, [null]),
      values,
    ]),
  ]);
}

function dataObject(value, allowedKeys, requiredKeys, code) {
  if (
    value === null ||
    typeof value !== "object" ||
    arrayIsArrayIntrinsic(value) ||
    isProxy(value)
  ) {
    throw new TypeError(code);
  }
  let prototype;
  let keys;
  try {
    prototype = callIntrinsic(objectGetPrototypeOfIntrinsic, Object, [value]);
    keys = reflectOwnKeysIntrinsic(value);
  } catch {
    throw new TypeError(code);
  }
  if (prototype !== objectPrototype && prototype !== null) {
    throw new TypeError(code);
  }
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
    if (
      typeof key !== "string" ||
      !callIntrinsic(arrayIncludesIntrinsic, allowedKeys, [key])
    ) {
      throw new TypeError(code);
    }
  }
  for (let index = 0; index < requiredKeys.length; index += 1) {
    if (!callIntrinsic(arrayIncludesIntrinsic, keys, [requiredKeys[index]])) {
      throw new TypeError(code);
    }
  }
  const result = callIntrinsic(objectCreateIntrinsic, Object, [null]);
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
    let descriptor;
    try {
      descriptor = callIntrinsic(objectGetOwnPropertyDescriptorIntrinsic, Object, [
        value,
        key,
      ]);
    } catch {
      throw new TypeError(code);
    }
    if (
      descriptor?.enumerable !== true ||
      !objectHasOwnIntrinsic(descriptor, "value")
    ) {
      throw new TypeError(code);
    }
    result[key] = descriptor.value;
  }
  return result;
}

function exactDataObject(value, keys, code) {
  const result = dataObject(value, keys, keys, code);
  if (reflectOwnKeysIntrinsic(result).length !== keys.length) {
    throw new TypeError(code);
  }
  return result;
}

function ownDataValue(value, key, code) {
  let descriptor;
  try {
    descriptor = callIntrinsic(objectGetOwnPropertyDescriptorIntrinsic, Object, [
      value,
      key,
    ]);
  } catch {
    throw new TypeError(code);
  }
  if (
    descriptor?.enumerable !== true ||
    !objectHasOwnIntrinsic(descriptor, "value")
  ) {
    throw new TypeError(code);
  }
  return descriptor.value;
}

function trustedFunction(value, code) {
  if (
    typeof value !== "function" ||
    isProxy(value) ||
    isGeneratorFunction(value) ||
    !objectIsFrozenIntrinsic(value)
  ) {
    throw new TypeError(code);
  }
  return value;
}

function canonicalValue(value, parts, state, depth = 0) {
  state.nodes += 1;
  if (state.nodes > MAX_CANONICAL_NODES || depth > MAX_CANONICAL_DEPTH) {
    throw new TypeError("invalid ext4 Podman authority data");
  }
  if (value === null) {
    callIntrinsic(arrayPushIntrinsic, parts, ["null"]);
    return;
  }
  if (typeof value === "boolean") {
    callIntrinsic(arrayPushIntrinsic, parts, [value ? "true" : "false"]);
    return;
  }
  if (typeof value === "string") {
    callIntrinsic(arrayPushIntrinsic, parts, [
      callIntrinsic(jsonStringifyIntrinsic, JSON, [value]),
    ]);
    return;
  }
  if (typeof value === "number") {
    if (!numberIsFiniteIntrinsic(value) || objectIsIntrinsic(value, -0)) {
      throw new TypeError("invalid ext4 Podman authority data");
    }
    callIntrinsic(arrayPushIntrinsic, parts, [stringConstructor(value)]);
    return;
  }
  if (typeof value !== "object" || isProxy(value)) {
    throw new TypeError("invalid ext4 Podman authority data");
  }
  if (arrayIsArrayIntrinsic(value)) {
    const keys = reflectOwnKeysIntrinsic(value);
    if (
      keys.length !== value.length + 1 ||
      !callIntrinsic(arrayIncludesIntrinsic, keys, ["length"])
    ) {
      throw new TypeError("invalid ext4 Podman authority data");
    }
    callIntrinsic(arrayPushIntrinsic, parts, ["["]);
    for (let index = 0; index < value.length; index += 1) {
      const descriptor = callIntrinsic(
        objectGetOwnPropertyDescriptorIntrinsic,
        Object,
        [value, String(index)],
      );
      if (
        descriptor?.enumerable !== true ||
        !objectHasOwnIntrinsic(descriptor, "value")
      ) {
        throw new TypeError("invalid ext4 Podman authority data");
      }
      if (index !== 0) callIntrinsic(arrayPushIntrinsic, parts, [","]);
      canonicalValue(descriptor.value, parts, state, depth + 1);
    }
    callIntrinsic(arrayPushIntrinsic, parts, ["]"]);
    return;
  }
  const prototype = callIntrinsic(objectGetPrototypeOfIntrinsic, Object, [value]);
  if (prototype !== objectPrototype && prototype !== null) {
    throw new TypeError("invalid ext4 Podman authority data");
  }
  const keys = reflectOwnKeysIntrinsic(value);
  for (let index = 0; index < keys.length; index += 1) {
    if (typeof keys[index] !== "string") {
      throw new TypeError("invalid ext4 Podman authority data");
    }
  }
  callIntrinsic(arraySortIntrinsic, keys, []);
  callIntrinsic(arrayPushIntrinsic, parts, ["{"]);
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
    const descriptor = callIntrinsic(
      objectGetOwnPropertyDescriptorIntrinsic,
      Object,
      [value, key],
    );
    if (
      descriptor?.enumerable !== true ||
      !objectHasOwnIntrinsic(descriptor, "value")
    ) {
      throw new TypeError("invalid ext4 Podman authority data");
    }
    if (index !== 0) callIntrinsic(arrayPushIntrinsic, parts, [","]);
    callIntrinsic(arrayPushIntrinsic, parts, [
      callIntrinsic(jsonStringifyIntrinsic, JSON, [key]),
      ":",
    ]);
    canonicalValue(descriptor.value, parts, state, depth + 1);
  }
  callIntrinsic(arrayPushIntrinsic, parts, ["}"]);
}

function canonicalSerialize(value) {
  const parts = [];
  canonicalValue(value, parts, { nodes: 0 });
  const serialized = callIntrinsic(arrayJoinIntrinsic, parts, [""]);
  if (
    callIntrinsic(bufferByteLengthIntrinsic, Buffer, [serialized, "utf8"]) >
    MAX_CANONICAL_BYTES
  ) {
    throw new TypeError("invalid ext4 Podman authority data");
  }
  return serialized;
}

function sha256(value) {
  const hash = callIntrinsic(createHashIntrinsic, undefined, ["sha256"]);
  callIntrinsic(hashUpdateIntrinsic, hash, [
    "portable-codex-runtime/ext4-podman-attachment-binding/v2\0",
    "utf8",
  ]);
  callIntrinsic(hashUpdateIntrinsic, hash, [value, "utf8"]);
  return callIntrinsic(hashDigestIntrinsic, hash, ["hex"]);
}

function isNativePromise(value) {
  if (value === null || typeof value !== "object" || isProxy(value) || !isPromise(value)) {
    return false;
  }
  try {
    return (
      callIntrinsic(objectGetPrototypeOfIntrinsic, Object, [value]) ===
        promisePrototype &&
      callIntrinsic(objectGetOwnPropertyDescriptorIntrinsic, Object, [
        value,
        "then",
      ]) === undefined &&
      callIntrinsic(objectGetOwnPropertyDescriptorIntrinsic, Object, [
        value,
        "catch",
      ]) === undefined &&
      callIntrinsic(objectGetOwnPropertyDescriptorIntrinsic, Object, [
        value,
        "finally",
      ]) === undefined &&
      callIntrinsic(objectGetOwnPropertyDescriptorIntrinsic, Object, [
        value,
        "constructor",
      ]) === undefined
    );
  } catch {
    return false;
  }
}

async function invokeNative(method, receiver, args) {
  const pending = callIntrinsic(method, receiver, args);
  if (!isNativePromise(pending)) {
    throw new TypeError("invalid ext4 Podman authority collaborator");
  }
  return await pending;
}

function identity(value) {
  const normalized = exactDataObject(
    value,
    IDENTITY_KEYS,
    "invalid ext4 Podman persistent identity",
  );
  for (let index = 0; index < IDENTITY_KEYS.length; index += 1) {
    if (typeof normalized[IDENTITY_KEYS[index]] !== "string") {
      throw new TypeError("invalid ext4 Podman persistent identity");
    }
  }
  return normalized;
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

function sessionAttachmentFromOperation(record) {
  if (record.kind === "attach") {
    const request = assertStorageMutationRequest(record.request);
    const result = assertWriterAttachmentMutationResult(record.result, { request });
    return assertSessionAttachment({
      attachmentId: result.target.attachmentId,
      backendId: result.backendId,
      contractVersion: result.contractVersion,
      fencingEpoch: result.fencingEpoch,
      holderId: result.holderId,
      kind: "directory",
      leaseId: result.leaseId,
      mode: "read-write",
      operationId: result.operationId,
      proofId: result.proofId,
      rootPath: result.rootPath,
      sessionId: result.sessionId,
      storageId: result.storageId,
    });
  }
  if (record.kind === "restore-attach") {
    const request = assertRestoreAttachmentActivationRequest(record.request);
    return assertRestoreAttachmentActivationResult(record.result, {
      request,
    }).attachment;
  }
  return null;
}

function sameStorageAtOriginRevision(committed, current) {
  if (
    typeof committed.revision !== "string" ||
    !regexpTest(POSITIVE_DECIMAL_PATTERN, committed.revision) ||
    typeof current.revision !== "string" ||
    !regexpTest(POSITIVE_DECIMAL_PATTERN, current.revision)
  ) {
    return false;
  }
  let committedRevision;
  let currentRevision;
  try {
    committedRevision = BigIntConstructor(committed.revision);
    currentRevision = BigIntConstructor(current.revision);
  } catch {
    return false;
  }
  if (currentRevision < committedRevision) return false;
  const normalizedCurrent = frozenRecord({
    ...current,
    revision: committed.revision,
  });
  return canonicalSerialize(committed) === canonicalSerialize(normalizedCurrent);
}

function currentStorageStatus(record, attachment) {
  if (record === null) return "missing";
  if (record.state === "prepared") return "mismatch";
  if (record.state !== "committed") {
    throw new TypeError("invalid ext4 Podman provider operation state");
  }
  if (record.currentAttachmentOriginOperationId !== record.operationId) {
    return "mismatch";
  }
  const operationAttachment = sessionAttachmentFromOperation(record);
  if (operationAttachment === null) return "mismatch";
  if (canonicalSerialize(operationAttachment) !== canonicalSerialize(attachment)) {
    return "mismatch";
  }
  const committed = record.storageState;
  const current = record.currentStorageState;
  if (current === null) return "missing";
  if (current.lifecycle !== "attached" || current.attachment === null) {
    return "missing";
  }
  if (
    committed === null ||
    committed.lifecycle !== "attached" ||
    committed.attachment === null ||
    !sameStorageAtOriginRevision(committed, current)
  ) {
    return "mismatch";
  }
  if (
    current.backendId !== attachment.backendId ||
    current.storageId !== attachment.storageId ||
    current.sessionId !== attachment.sessionId ||
    current.attachment.attachmentId !== attachment.attachmentId ||
    current.attachment.leaseId !== attachment.leaseId ||
    current.attachment.holderId !== attachment.holderId ||
    current.attachment.fencingEpoch !== attachment.fencingEpoch ||
    current.attachment.proofId !== attachment.proofId ||
    current.attachment.rootPath !== attachment.rootPath ||
    current.writerAuthority === null ||
    current.writerAuthority.leaseId !== attachment.leaseId ||
    current.writerAuthority.holderId !== attachment.holderId ||
    current.writerAuthority.fencingEpoch !== attachment.fencingEpoch ||
    current.dataRoot === null ||
    current.mount === null ||
    !sameIdentity(current.attachment.imageIdentity, current.dataRoot.imageIdentity) ||
    !sameIdentity(current.attachment.rootIdentity, current.dataRoot.rootIdentity) ||
    !sameIdentity(current.attachment.imageIdentity, current.mount.imageIdentity)
  ) {
    return "mismatch";
  }
  return "current";
}

function normalizeAuthorityObservation(value) {
  const observation = exactDataObject(
    value,
    DRIVER_AUTHORITY_OBSERVATION_KEYS,
    "invalid ext4 Podman authority observation",
  );
  const runtime = exactDataObject(
    observation.rootRuntimeIdentity,
    RUNTIME_IDENTITY_KEYS,
    "invalid ext4 Podman runtime identity",
  );
  if (
    typeof runtime.device !== "string" ||
    !regexpTest(DECIMAL_PATTERN, runtime.device) ||
    typeof runtime.inode !== "string" ||
    !regexpTest(POSITIVE_DECIMAL_PATTERN, runtime.inode)
  ) {
    throw new TypeError("invalid ext4 Podman runtime identity");
  }
  const filesystem = exactDataObject(
    observation.filesystem,
    FILESYSTEM_KEYS,
    "invalid ext4 Podman authority filesystem",
  );
  if (
    filesystem.type !== "ext4" ||
    filesystem.durability !== "local-fsync-rename" ||
    typeof filesystem.filesystemId !== "string" ||
    typeof filesystem.objectIdentityScheme !== "string"
  ) {
    throw new TypeError("invalid ext4 Podman authority filesystem");
  }
  identity(observation.imageIdentity);
  identity(observation.mountRootIdentity);
  identity(observation.rootIdentity);
  return frozenRecord({
    ...observation,
    filesystem: frozenRecord(filesystem),
    rootRuntimeIdentity: frozenRecord(runtime),
  });
}

function observationMatchesStorage(observation, storage) {
  return (
    observation.attachmentRootPath === storage.attachment.rootPath &&
    observation.imagePath === storage.imagePath &&
    observation.mountPath === storage.mount.mountPath &&
    observation.filesystem.filesystemId === storage.filesystemId &&
    sameIdentity(observation.imageIdentity, storage.attachment.imageIdentity) &&
    sameIdentity(observation.mountRootIdentity, storage.mount.rootIdentity) &&
    sameIdentity(observation.rootIdentity, storage.attachment.rootIdentity)
  );
}

function conclusiveDriverStatus(error) {
  if (!(error instanceof LinuxExt4ImageDriverError)) return null;
  if (error.code === "attachment_root_absent") return "missing";
  if (
    error.code === "attachment_root_unsafe" ||
    error.code === "access_policy_mismatch" ||
    error.code === "mount_absent" ||
    error.code === "mount_mismatch"
  ) {
    return "mismatch";
  }
  return null;
}

function receipt(status, bindingSha256 = null, rootRuntimeIdentity = null) {
  return frozenRecord({ bindingSha256, rootRuntimeIdentity, status });
}

export function createExt4PodmanAttachmentBinding(...args) {
  if (args.length !== 1) {
    throw new TypeError("expected one ext4 Podman binding options argument");
  }
  const options = exactDataObject(
    args[0],
    BACKEND_OPTION_KEYS,
    "invalid ext4 Podman binding options",
  );
  const backendOptions = frozenRecord(options);
  const rawBackend = createExt4FilesystemImageBackend(backendOptions);
  const backend = createInitializedExt4FilesystemImageBackend({
    backend: rawBackend,
  });
  const state = options.state;
  const driver = options.driver;
  const initialize = trustedFunction(
    ownDataValue(
      backend,
      "initialize",
      "invalid initialized ext4 backend",
    ),
    "invalid initialized ext4 backend",
  );
  const observeAttachmentRootAuthority = trustedFunction(
    ownDataValue(
      driver,
      "observeAttachmentRootAuthority",
      "invalid Linux ext4 authority driver",
    ),
    "invalid Linux ext4 authority driver",
  );
  if (
    ownDataValue(
      driver,
      "contractVersion",
      "invalid Linux ext4 authority driver",
    ) !== LINUX_EXT4_IMAGE_DRIVER_CONTRACT_VERSION ||
    ownDataValue(
      driver,
      "attachmentRootAuthorityContractVersion",
      "invalid Linux ext4 authority driver",
    ) !== LINUX_EXT4_ATTACHMENT_ROOT_AUTHORITY_CONTRACT_VERSION
  ) {
    throw new TypeError("invalid Linux ext4 authority driver");
  }

  const verify = objectFreezeIntrinsic(async function verify(inputValue) {
    if (this !== undefined || arguments.length !== 1) {
      throw new TypeError("invalid ext4 Podman persistent authority receiver");
    }
    const input = exactDataObject(
      inputValue,
      PERSISTENT_AUTHORITY_INPUT_KEYS,
      "invalid ext4 Podman persistent authority input",
    );
    const attachment = assertSessionAttachment(input.attachment);
    await invokeNative(initialize, backend, EMPTY_ARGUMENTS);
    const first = await invokeNative(
      stateReadOperationIntrinsic,
      state,
      [frozenRecord({ operationId: attachment.operationId })],
    );
    const firstStatus = currentStorageStatus(first, attachment);
    if (firstStatus !== "current") return receipt(firstStatus);
    const firstSerialized = canonicalSerialize(first);
    let observation;
    let driverStatus = null;
    try {
      observation = normalizeAuthorityObservation(
        await invokeNative(
          observeAttachmentRootAuthority,
          driver,
          [frozenRecord({
            attachmentRootPath: first.currentStorageState.attachment.rootPath,
            imagePath: first.currentStorageState.imagePath,
            mountPath: first.currentStorageState.mount.mountPath,
          })],
        ),
      );
    } catch (error) {
      driverStatus = conclusiveDriverStatus(error);
      if (driverStatus === null) throw error;
    }
    const second = await invokeNative(
      stateReadOperationIntrinsic,
      state,
      [frozenRecord({ operationId: attachment.operationId })],
    );
    const secondStatus = currentStorageStatus(second, attachment);
    if (
      secondStatus !== "current" ||
      canonicalSerialize(second) !== firstSerialized
    ) {
      return receipt("mismatch");
    }
    if (driverStatus !== null) return receipt(driverStatus);
    if (!observationMatchesStorage(observation, second.currentStorageState)) {
      return receipt("mismatch");
    }
    return receipt(
      "current",
      sha256(firstSerialized),
      observation.rootRuntimeIdentity,
    );
  });
  const persistentAuthority = frozenRecord({
    contractVersion: EXT4_PODMAN_PERSISTENT_AUTHORITY_CONTRACT_VERSION,
    verify,
  });
  const filesystemAuthority = createPodmanWriterFilesystemAuthorityComposition({
    persistentAuthority,
  });
  return frozenRecord({
    attachmentAuthority: persistentAuthority,
    backend,
    contractVersion: EXT4_PODMAN_ATTACHMENT_BINDING_CONTRACT_VERSION,
    filesystemAuthority,
  });
}

objectFreezeIntrinsic(createExt4PodmanAttachmentBinding);
