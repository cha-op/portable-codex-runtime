import { Buffer } from "node:buffer";
import { Hash, createHash } from "node:crypto";
import {
  basename,
  dirname,
  isAbsolute,
  join,
  parse,
  resolve,
  sep,
} from "node:path";
import { types as utilTypes } from "node:util";

import {
  assertCheckpointDescriptor,
  assertSessionAttachment,
  assertSessionProvisionRequest,
  assertStorageMutationRequest,
} from "./session-storage-contracts.mjs";

const arrayIsArray = Array.isArray;
const arrayIncludesIntrinsic = Array.prototype.includes;
const bufferAllocUnsafeIntrinsic = Buffer.allocUnsafe;
const bufferByteLengthIntrinsic = Buffer.byteLength;
const bufferFromIntrinsic = Buffer.from;
const bufferToStringIntrinsic = Buffer.prototype.toString;
const bufferWriteUInt32BEIntrinsic = Buffer.prototype.writeUInt32BE;
const hashDigestIntrinsic = Hash.prototype.digest;
const hashUpdateIntrinsic = Hash.prototype.update;
const objectAssign = Object.assign;
const objectCreate = Object.create;
const objectFreeze = Object.freeze;
const objectGetOwnPropertyDescriptor = Object.getOwnPropertyDescriptor;
const objectGetPrototypeOf = Object.getPrototypeOf;
const objectHasOwn = Object.hasOwn;
const objectPrototype = Object.prototype;
const reflectOwnKeys = Reflect.ownKeys;
const reflectApply = Reflect.apply;
const regexpTestIntrinsic = RegExp.prototype.test;
const stringStartsWithIntrinsic = String.prototype.startsWith;
const { isProxy } = utilTypes;

export const EXT4_FILESYSTEM_IMAGE_PATHS_CONTRACT_VERSION = 1;

const OPTION_KEYS = objectFreeze([
  "archiveRoot",
  "backendId",
  "imageRoot",
  "mountRoot",
]);
const OPAQUE_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u;
const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u;
const RESTORED_ATTACHMENT_NAME_PATTERN = /^generation-[0-9a-f]{48}$/u;
const NUL_PATTERN = /\0/u;
const MAX_PATH_BYTES = 4096;

const ERROR_MESSAGES = objectFreeze({
  invalid_ext4_filesystem_image_path_options:
    "Ext4 filesystem image path options are invalid",
  invalid_ext4_filesystem_image_path_request:
    "Ext4 filesystem image path request is invalid",
});

export class Ext4FilesystemImagePathsError extends Error {
  constructor(code) {
    if (!objectHasOwn(ERROR_MESSAGES, code)) {
      throw new TypeError("Unsupported ext4 filesystem image path error");
    }
    super(ERROR_MESSAGES[code]);
    this.name = "Ext4FilesystemImagePathsError";
    this.code = code;
    objectFreeze(this);
  }
}

function fail(code) {
  throw new Ext4FilesystemImagePathsError(code);
}

function ensure(condition, code) {
  if (!condition) fail(code);
}

function exactDataObject(value, keys, code) {
  ensure(
    value !== null &&
      typeof value === "object" &&
      !arrayIsArray(value) &&
      !isProxy(value),
    code,
  );
  let prototype;
  let actual;
  try {
    prototype = objectGetPrototypeOf(value);
    actual = reflectOwnKeys(value);
  } catch {
    fail(code);
  }
  ensure(
    (prototype === objectPrototype || prototype === null) &&
      actual.length === keys.length,
    code,
  );
  for (let index = 0; index < actual.length; index += 1) {
    const key = actual[index];
    ensure(
      typeof key === "string" &&
        reflectApply(arrayIncludesIntrinsic, keys, [key]),
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
  }
  return value;
}

function exactFrozenRecord(value) {
  return objectFreeze(objectAssign(objectCreate(null), value));
}

function canonicalRoot(value, code) {
  ensure(
    typeof value === "string" && value.length <= MAX_PATH_BYTES,
    code,
  );
  const encoded = reflectApply(bufferFromIntrinsic, Buffer, [value, "utf8"]);
  ensure(
    encoded.length <= MAX_PATH_BYTES &&
      reflectApply(bufferToStringIntrinsic, encoded, ["utf8"]) === value &&
      !reflectApply(regexpTestIntrinsic, NUL_PATTERN, [value]) &&
      isAbsolute(value) &&
      resolve(value) === value &&
      value !== parse(value).root,
    code,
  );
  return value;
}

function boundedPath(value, code) {
  ensure(value.length <= MAX_PATH_BYTES, code);
  const encoded = reflectApply(bufferFromIntrinsic, Buffer, [value, "utf8"]);
  ensure(
    encoded.length <= MAX_PATH_BYTES &&
      reflectApply(bufferToStringIntrinsic, encoded, ["utf8"]) === value,
    code,
  );
  return value;
}

function pathContains(parent, child) {
  return (
    child === parent ||
    reflectApply(stringStartsWithIntrinsic, child, [`${parent}${sep}`])
  );
}

function assertDisjointRoots(roots, code) {
  for (let left = 0; left < roots.length; left += 1) {
    for (let right = left + 1; right < roots.length; right += 1) {
      ensure(
        !pathContains(roots[left], roots[right]) &&
          !pathContains(roots[right], roots[left]),
        code,
      );
    }
  }
}

function opaqueId(value, code) {
  ensure(
    typeof value === "string" &&
      reflectApply(regexpTestIntrinsic, OPAQUE_ID_PATTERN, [value]),
    code,
  );
  return value;
}

function runtimeSessionId(value, code) {
  ensure(
    typeof value === "string" &&
      reflectApply(regexpTestIntrinsic, UUID_PATTERN, [value]),
    code,
  );
  return value;
}

function digest(parts) {
  const hash = reflectApply(createHash, undefined, ["sha256"]);
  reflectApply(hashUpdateIntrinsic, hash, [
    "portable-codex-runtime/ext4-path/v1\0",
    "utf8",
  ]);
  for (const part of parts) {
    const byteLength = reflectApply(bufferByteLengthIntrinsic, Buffer, [
      part,
      "utf8",
    ]);
    const length = reflectApply(bufferAllocUnsafeIntrinsic, Buffer, [4]);
    reflectApply(bufferWriteUInt32BEIntrinsic, length, [byteLength]);
    reflectApply(hashUpdateIntrinsic, hash, [length]);
    reflectApply(hashUpdateIntrinsic, hash, [part, "utf8"]);
  }
  return reflectApply(hashDigestIntrinsic, hash, ["hex"]);
}

function storageToken(backendId, storageId) {
  return digest(["storage", backendId, storageId]);
}

function checkpointAndRequest(input, backendId, code) {
  exactDataObject(input, ["checkpoint", "request"], code);
  let checkpoint;
  let request;
  try {
    checkpoint = assertCheckpointDescriptor(input.checkpoint);
    request = assertStorageMutationRequest(input.request);
  } catch {
    fail(code);
  }
  ensure(
    checkpoint.backendId === backendId &&
      request.backendId === backendId &&
      request.operation === "checkpoint" &&
      request.sessionId === checkpoint.sessionId &&
      request.storageId === checkpoint.storageId &&
      request.target.kind === "checkpoint" &&
      request.target.artifactId === checkpoint.artifactId &&
      request.target.checkpointId === checkpoint.checkpointId,
    code,
  );
  return { checkpoint, request };
}

export function createExt4FilesystemImagePaths(...args) {
  const optionCode = "invalid_ext4_filesystem_image_path_options";
  const requestCode = "invalid_ext4_filesystem_image_path_request";
  ensure(args.length === 1, optionCode);
  const options = exactDataObject(args[0], OPTION_KEYS, optionCode);
  const backendId = opaqueId(options.backendId, optionCode);
  const archiveRoot = canonicalRoot(options.archiveRoot, optionCode);
  const imageRoot = canonicalRoot(options.imageRoot, optionCode);
  const mountRoot = canonicalRoot(options.mountRoot, optionCode);
  assertDisjointRoots([archiveRoot, imageRoot, mountRoot], optionCode);

  const storageIdForSession = objectFreeze(function storageIdForSession(
    sessionIdValue,
  ) {
    const sessionId = runtimeSessionId(sessionIdValue, requestCode);
    return `ext4-storage:${digest(["session", backendId, sessionId]).slice(0, 48)}`;
  });

  const planProvision = objectFreeze(function planProvision(input) {
    let request;
    try {
      request = assertSessionProvisionRequest(input);
    } catch {
      fail(requestCode);
    }
    ensure(request.backendId === backendId, requestCode);
    const storageId = storageIdForSession(request.sessionId);
    const token = storageToken(backendId, storageId);
    return exactFrozenRecord({
      imagePath: boundedPath(join(imageRoot, `${token}.ext4`), requestCode),
      mountPath: boundedPath(join(mountRoot, token), requestCode),
      storageId,
    });
  });

  const planWritableAttachment = objectFreeze(
    function planWritableAttachment(input) {
      let request;
      try {
        request = assertStorageMutationRequest(input);
      } catch {
        fail(requestCode);
      }
      ensure(
        request.backendId === backendId &&
          request.operation === "attach" &&
          request.target.kind === "attachment",
        requestCode,
      );
      const token = storageToken(backendId, request.storageId);
      return exactFrozenRecord({
        attachmentRootPath: boundedPath(
          join(
            mountRoot,
            token,
            `data-${digest(["data-root", backendId, request.storageId]).slice(0, 48)}`,
          ),
          requestCode,
        ),
        imagePath: boundedPath(join(imageRoot, `${token}.ext4`), requestCode),
        mountPath: boundedPath(join(mountRoot, token), requestCode),
        storageId: request.storageId,
      });
    },
  );

  const planRestoreDestination = objectFreeze(
    function planRestoreDestination(input) {
      let request;
      try {
        request = assertStorageMutationRequest(input);
      } catch {
        fail(requestCode);
      }
      ensure(
        request.backendId === backendId &&
          request.operation === "restore" &&
          request.target.kind === "checkpoint",
        requestCode,
      );
      const storageId = request.storageId;
      const token = storageToken(backendId, storageId);
      const destinationOwnedRoot = boundedPath(
        join(mountRoot, token),
        requestCode,
      );
      return exactFrozenRecord({
        destinationDirectory: boundedPath(
          join(
            destinationOwnedRoot,
            `generation-${digest([
              "restore-operation",
              request.operationId,
            ]).slice(0, 48)}`,
          ),
          requestCode,
        ),
        destinationOwnedRoot,
      });
    },
  );

  const resolveArtifactPaths = objectFreeze(function resolveArtifactPaths(
    input,
  ) {
    const { checkpoint, request } = checkpointAndRequest(
      input,
      backendId,
      requestCode,
    );
    const token = digest([
      "artifact",
      backendId,
      checkpoint.sessionId,
      checkpoint.storageId,
      checkpoint.artifactId,
      checkpoint.checkpointId,
    ]);
    return exactFrozenRecord({
      artifactDirectory: boundedPath(
        join(archiveRoot, `artifact-${token.slice(0, 48)}`),
        requestCode,
      ),
      artifactOwnedRoot: archiveRoot,
    });
  });

  const resolveSourceOwnedRoot = objectFreeze(
    function resolveSourceOwnedRoot(input) {
      exactDataObject(
        input,
        ["canonicalAttachment", "checkpoint", "request"],
        requestCode,
      );
      const { checkpoint, request } = checkpointAndRequest(
        { checkpoint: input.checkpoint, request: input.request },
        backendId,
        requestCode,
      );
      let attachment;
      try {
        attachment = assertSessionAttachment(input.canonicalAttachment);
      } catch {
        fail(requestCode);
      }
      ensure(
        attachment.backendId === backendId &&
          attachment.sessionId === checkpoint.sessionId &&
          attachment.storageId === request.storageId,
        requestCode,
      );
      const expectedOwnedRoot = join(
        mountRoot,
        storageToken(backendId, attachment.storageId),
      );
      const expectedSourceDirectory = join(
        expectedOwnedRoot,
        `data-${digest(["data-root", backendId, attachment.storageId]).slice(0, 48)}`,
      );
      ensure(
        dirname(attachment.rootPath) === expectedOwnedRoot &&
          (attachment.rootPath === expectedSourceDirectory ||
            reflectApply(regexpTestIntrinsic, RESTORED_ATTACHMENT_NAME_PATTERN, [
              basename(attachment.rootPath),
            ])),
        requestCode,
      );
      return exactFrozenRecord({
        sourceDirectory: attachment.rootPath,
        sourceOwnedRoot: expectedOwnedRoot,
      });
    },
  );

  return exactFrozenRecord({
    backendId,
    contractVersion: EXT4_FILESYSTEM_IMAGE_PATHS_CONTRACT_VERSION,
    planProvision,
    planRestoreDestination,
    planWritableAttachment,
    resolveArtifactPaths,
    resolveSourceOwnedRoot,
    storageIdForSession,
  });
}
