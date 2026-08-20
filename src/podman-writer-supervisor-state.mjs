import { Buffer } from "node:buffer";
import { Hash, createHash, randomBytes } from "node:crypto";
import { constants as fsConstants } from "node:fs";
import {
  chmod,
  link,
  lstat,
  mkdir,
  open,
  readdir,
  rename,
  rmdir,
  unlink,
} from "node:fs/promises";
import { basename, dirname, isAbsolute, resolve } from "node:path";
import { types as utilTypes } from "node:util";

const { isPromise, isProxy } = utilTypes;
const arrayIncludesIntrinsic = Array.prototype.includes;
const arrayIsArrayIntrinsic = Array.isArray;
const arrayEveryIntrinsic = Array.prototype.every;
const bufferAllocIntrinsic = Buffer.alloc;
const bufferEqualsIntrinsic = Buffer.prototype.equals;
const bufferFromIntrinsic = Buffer.from;
const bufferToStringIntrinsic = Buffer.prototype.toString;
const createHashIntrinsic = createHash;
const randomBytesIntrinsic = randomBytes;
const jsonParseIntrinsic = JSON.parse;
const jsonStringifyIntrinsic = JSON.stringify;
const JsonObject = JSON;
const hashDigestIntrinsic = Hash.prototype.digest;
const hashUpdateIntrinsic = Hash.prototype.update;
const mapDeleteIntrinsic = Map.prototype.delete;
const mapGetIntrinsic = Map.prototype.get;
const mapSetIntrinsic = Map.prototype.set;
const MapConstructor = Map;
const numberIsSafeIntegerIntrinsic = Number.isSafeInteger;
const objectAssignIntrinsic = Object.assign;
const objectCreateIntrinsic = Object.create;
const objectDefinePropertiesIntrinsic = Object.defineProperties;
const objectFreezeIntrinsic = Object.freeze;
const objectGetOwnPropertyDescriptorIntrinsic = Object.getOwnPropertyDescriptor;
const objectGetPrototypeOfIntrinsic = Object.getPrototypeOf;
const objectHasOwnIntrinsic = Object.hasOwn;
const objectPrototype = Object.prototype;
const pathBasenameIntrinsic = basename;
const promiseResolveIntrinsic = Promise.resolve;
const PromiseConstructor = Promise;
const pathDirnameIntrinsic = dirname;
const pathIsAbsoluteIntrinsic = isAbsolute;
const pathResolveIntrinsic = resolve;
const promisePrototype = Promise.prototype;
const reflectApplyIntrinsic = Reflect.apply;
const reflectOwnKeysIntrinsic = Reflect.ownKeys;
const regexpExecIntrinsic = RegExp.prototype.exec;
const stringEndsWithIntrinsic = String.prototype.endsWith;
const stringIncludesIntrinsic = String.prototype.includes;
const stringIndexOfIntrinsic = String.prototype.indexOf;
const weakSetAddIntrinsic = WeakSet.prototype.add;
const weakSetHasIntrinsic = WeakSet.prototype.has;
const weakMapGetIntrinsic = WeakMap.prototype.get;
const weakMapSetIntrinsic = WeakMap.prototype.set;
const WeakMapConstructor = WeakMap;
const runtimePlatform = process.platform;

function callIntrinsic(intrinsic, receiver, arguments_) {
  return reflectApplyIntrinsic(intrinsic, receiver, arguments_);
}

function arrayIncludes(value, candidate) {
  return callIntrinsic(arrayIncludesIntrinsic, value, [candidate]);
}

function arrayEvery(value, callback) {
  return callIntrinsic(arrayEveryIntrinsic, value, [callback]);
}

function regexpTest(pattern, value) {
  return callIntrinsic(regexpExecIntrinsic, pattern, [value]) !== null;
}

export const PODMAN_WRITER_SUPERVISOR_STATE_CONTRACT_VERSION = 1;
export const PODMAN_WRITER_SUPERVISOR_STATE_COLLECTION_CONTRACT_VERSION = 2;
export const PODMAN_WRITER_SUPERVISOR_STATE_OWNER_CONTRACT_VERSION = 1;

const MAX_RECORD_BYTES = 16 * 1024;
const MAX_NATIVE_PATH_BYTES = 4_095;
// `/<64-hex>.<revision>.json.pending` is the longest derived suffix. The
// contract's revisions are single decimal digits, so it consumes 80 bytes.
const MAX_STATE_ROOT_BYTES = MAX_NATIVE_PATH_BYTES - 80;
const OPAQUE_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u;
const SHA256_PATTERN = /^[0-9a-f]{64}$/u;
const CONTAINER_NAME_PATTERN = /^[a-z0-9][a-z0-9_.-]{0,127}$/u;
const CONTAINER_ID_PATTERN = /^[0-9a-f]{12,64}$/u;
const STATE_OWNER_ID_PATTERN = /^state-owner:[0-9a-f]{64}$/u;
const STATE_OWNER_MARKER_NAME = ".state-owner-v1.json";
// The fixed 48-byte basename leaves room for `/.state-owner-v1.json` even when
// the final root has the shortest possible basename at MAX_STATE_ROOT_BYTES.
// The 16 random bytes provide a 128-bit namespace token.
const STATE_OWNER_STAGING_PREFIX = ".pws-owner-init-";
const STATE_OWNER_STAGING_RANDOM_BYTES = 16;
const STATE_OWNER_STAGING_BASENAME_BYTES =
  STATE_OWNER_STAGING_PREFIX.length + STATE_OWNER_STAGING_RANDOM_BYTES * 2;
const STATE_OWNER_STAGING_MARKER_WORST_CASE_BYTES =
  MAX_STATE_ROOT_BYTES - 2 +
  1 + STATE_OWNER_STAGING_BASENAME_BYTES +
  1 + STATE_OWNER_MARKER_NAME.length;
const RECORD_KEYS = Object.freeze([
  "containerId",
  "containerName",
  "contractVersion",
  "launchAttemptId",
  "processIncarnationId",
  "proofId",
  "requestSha256",
  "revision",
  "status",
  "stopOperationId",
  "stopProofId",
  "writerIncarnationId",
]);
const CLAIM_KEYS = Object.freeze(["record"]);
const READ_KEYS = Object.freeze(["launchAttemptId"]);
const TRANSITION_KEYS = Object.freeze([
  "expectedRevision",
  "expectedStatus",
  "record",
]);
const COLLECTION_KEYS = Object.freeze(["stateOwnerId", "terminalRecord"]);
const LEGACY_OPTION_KEYS = Object.freeze(["faultHooks", "root"]);
const LEGACY_REQUIRED_OPTION_KEYS = Object.freeze(["root"]);
const BUNDLE_OPTION_KEYS = Object.freeze(["faultHooks", "owner"]);
const BUNDLE_REQUIRED_OPTION_KEYS = Object.freeze(["owner"]);
const STATE_OWNER_PREPARATION_KEYS = Object.freeze([
  "expectedStateOwnerId",
  "root",
]);
const FAULT_HOOK_KEYS = Object.freeze([
  "afterCleanup",
  "afterCleanupDirectorySync",
  "afterParentDirectorySync",
  "afterPublish",
  "afterPublishDirectorySync",
  "afterRootDirectorySync",
  "afterTemporarySync",
  "afterTemporaryWrite",
  "afterCollectionArtifactUnlink",
  "afterCollectionFileFirstRead",
  "afterCollectionFirstDirectorySync",
  "afterCollectionTerminalRevalidation",
  "afterCollectionTerminalUnlink",
  "afterCollectionFinalDirectorySync",
  "beforeCollectionArtifactUnlink",
  "afterPrivateParentHold",
]);
const STATUS_REVISION = Object.freeze({
  created: 1,
  preparing: 0,
  started: 2,
  stopped: 4,
  stopping: 3,
});
const NEXT_STATUS = Object.freeze({
  created: Object.freeze(["started"]),
  preparing: Object.freeze(["created"]),
  started: Object.freeze(["stopping"]),
  stopping: Object.freeze(["stopped"]),
});
const ERROR_MESSAGES = Object.freeze({
  podman_writer_state_conflict:
    "Podman writer supervisor state conflicts with the requested transition",
  podman_writer_state_invalid:
    "Podman writer supervisor state input or durable record is invalid",
  podman_writer_state_io_failed:
    "Podman writer supervisor state could not be read or persisted safely",
  podman_writer_state_collection_outcome_uncertain:
    "Podman writer supervisor state collection outcome is uncertain",
});

const stateBrands = new WeakSet();
const stateBundleBrands = new WeakSet();
const stateCollectorBrands = new WeakSet();
const stateErrorBrands = new WeakSet();
const stateOwnerBrands = new WeakSet();
const stateOwnerBindings = new WeakMapConstructor();

export class PodmanWriterSupervisorStateError extends Error {
  constructor(code) {
    if (typeof code !== "string" || !objectHasOwnIntrinsic(ERROR_MESSAGES, code)) {
      throw new TypeError("unsupported Podman writer supervisor state error code");
    }
    super(ERROR_MESSAGES[code]);
    callIntrinsic(objectDefinePropertiesIntrinsic, Object, [this, {
      code: {
        enumerable: true,
        value: code,
      },
      name: {
        enumerable: true,
        value: "PodmanWriterSupervisorStateError",
      },
      retryable: {
        enumerable: true,
        value: false,
      },
      stack: {
        configurable: false,
        enumerable: false,
        value: `PodmanWriterSupervisorStateError: ${ERROR_MESSAGES[code]}`,
        writable: false,
      },
    }]);
    callIntrinsic(weakSetAddIntrinsic, stateErrorBrands, [this]);
    callIntrinsic(objectFreezeIntrinsic, Object, [this]);
  }
}

function isStateError(value) {
  return callIntrinsic(weakSetHasIntrinsic, stateErrorBrands, [value]);
}

function errorCode(value) {
  if (
    value === null ||
    (typeof value !== "object" && typeof value !== "function") ||
    isProxy(value)
  ) {
    return null;
  }
  try {
    const descriptor = callIntrinsic(objectGetOwnPropertyDescriptorIntrinsic, Object, [
      value,
      "code",
    ]);
    return descriptor && objectHasOwnIntrinsic(descriptor, "value")
      ? descriptor.value
      : null;
  } catch {
    return null;
  }
}

function fail(code) {
  throw new PodmanWriterSupervisorStateError(code);
}

function ensure(condition, code = "podman_writer_state_invalid") {
  if (!condition) fail(code);
}

function exactDataObject(value, keys, code = "podman_writer_state_invalid") {
  ensure(
    value !== null &&
      typeof value === "object" &&
      !arrayIsArrayIntrinsic(value) &&
      !isProxy(value),
    code,
  );
  let prototype;
  let ownKeys;
  try {
    prototype = callIntrinsic(objectGetPrototypeOfIntrinsic, Object, [value]);
    ownKeys = reflectOwnKeysIntrinsic(value);
  } catch {
    fail(code);
  }
  ensure(
    (prototype === objectPrototype || prototype === null) &&
      ownKeys.length === keys.length &&
      arrayEvery(
        ownKeys,
        (key) => typeof key === "string" && arrayIncludes(keys, key),
      ),
    code,
  );
  const normalized = callIntrinsic(objectCreateIntrinsic, Object, [null]);
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
    let descriptor;
    try {
      descriptor = callIntrinsic(objectGetOwnPropertyDescriptorIntrinsic, Object, [value, key]);
    } catch {
      fail(code);
    }
    ensure(
      descriptor?.enumerable === true && objectHasOwnIntrinsic(descriptor, "value"),
      code,
    );
    normalized[key] = descriptor.value;
  }
  return normalized;
}

function optionalDataObject(
  value,
  allowedKeys,
  requiredKeys,
  code = "podman_writer_state_invalid",
) {
  ensure(
    value !== null &&
      typeof value === "object" &&
      !arrayIsArrayIntrinsic(value) &&
      !isProxy(value),
    code,
  );
  let prototype;
  let ownKeys;
  try {
    prototype = callIntrinsic(objectGetPrototypeOfIntrinsic, Object, [value]);
    ownKeys = reflectOwnKeysIntrinsic(value);
  } catch {
    fail(code);
  }
  ensure(
    (prototype === objectPrototype || prototype === null) &&
      ownKeys.length >= requiredKeys.length &&
      ownKeys.length <= allowedKeys.length &&
      arrayEvery(
        ownKeys,
        (key) => typeof key === "string" && arrayIncludes(allowedKeys, key),
      ) &&
      arrayEvery(requiredKeys, (key) => arrayIncludes(ownKeys, key)),
    code,
  );
  const normalized = callIntrinsic(objectCreateIntrinsic, Object, [null]);
  for (let index = 0; index < ownKeys.length; index += 1) {
    const key = ownKeys[index];
    let descriptor;
    try {
      descriptor = callIntrinsic(objectGetOwnPropertyDescriptorIntrinsic, Object, [
        value,
        key,
      ]);
    } catch {
      fail(code);
    }
    ensure(
      descriptor?.enumerable === true && objectHasOwnIntrinsic(descriptor, "value"),
      code,
    );
    normalized[key] = descriptor.value;
  }
  return normalized;
}

function frozenRecord(value) {
  return callIntrinsic(objectFreezeIntrinsic, Object, [
    callIntrinsic(objectAssignIntrinsic, Object, [
      callIntrinsic(objectCreateIntrinsic, Object, [null]),
      value,
    ]),
  ]);
}

function frozenFunction(value) {
  return callIntrinsic(objectFreezeIntrinsic, Object, [value]);
}

function assertOpaqueId(value) {
  ensure(typeof value === "string" && regexpTest(OPAQUE_ID_PATTERN, value));
  return value;
}

function nullableOpaqueId(value) {
  ensure(value === null || (typeof value === "string" && regexpTest(OPAQUE_ID_PATTERN, value)));
  return value;
}

function normalizeRecord(value) {
  const record = exactDataObject(value, RECORD_KEYS);
  ensure(
    record.contractVersion === PODMAN_WRITER_SUPERVISOR_STATE_CONTRACT_VERSION &&
      numberIsSafeIntegerIntrinsic(record.revision) &&
      record.revision >= 0 &&
      record.revision <= 4 &&
      typeof record.status === "string" &&
      objectHasOwnIntrinsic(STATUS_REVISION, record.status) &&
      STATUS_REVISION[record.status] === record.revision &&
      typeof record.requestSha256 === "string" &&
      regexpTest(SHA256_PATTERN, record.requestSha256) &&
      typeof record.containerName === "string" &&
      regexpTest(CONTAINER_NAME_PATTERN, record.containerName),
  );
  assertOpaqueId(record.launchAttemptId);
  nullableOpaqueId(record.processIncarnationId);
  nullableOpaqueId(record.proofId);
  nullableOpaqueId(record.stopOperationId);
  nullableOpaqueId(record.stopProofId);
  nullableOpaqueId(record.writerIncarnationId);
  ensure(
    record.containerId === null ||
      (typeof record.containerId === "string" &&
        regexpTest(CONTAINER_ID_PATTERN, record.containerId)),
  );

  if (record.status === "preparing") {
    ensure(
      record.containerId === null &&
        record.processIncarnationId === null &&
        record.proofId === null &&
        record.stopOperationId === null &&
        record.stopProofId === null &&
        record.writerIncarnationId === null,
    );
  } else {
    ensure(
      record.containerId !== null &&
        record.processIncarnationId !== null &&
        record.writerIncarnationId !== null,
    );
    if (record.status === "created") {
      ensure(
        record.proofId === null &&
          record.stopOperationId === null &&
          record.stopProofId === null,
      );
    } else {
      ensure(record.proofId !== null);
      if (record.status === "started") {
        ensure(record.stopOperationId === null && record.stopProofId === null);
      } else if (record.status === "stopping") {
        ensure(record.stopOperationId !== null && record.stopProofId === null);
      } else {
        ensure(record.stopOperationId !== null && record.stopProofId !== null);
      }
    }
  }
  return frozenRecord(record);
}

export function assertPodmanWriterSupervisorStateRecord(value) {
  return normalizeRecord(value);
}

function sameRecord(left, right) {
  return (
    callIntrinsic(jsonStringifyIntrinsic, JsonObject, [left]) ===
    callIntrinsic(jsonStringifyIntrinsic, JsonObject, [right])
  );
}

function isNextStatus(status, candidate) {
  return objectHasOwnIntrinsic(NEXT_STATUS, status) &&
    arrayIncludes(NEXT_STATUS[status], candidate);
}

function effectiveUid() {
  if (typeof process.geteuid === "function") return BigInt(process.geteuid());
  if (typeof process.getuid === "function") return BigInt(process.getuid());
  return null;
}

function sameIdentity(left, right) {
  return left.dev === right.dev && left.ino === right.ino;
}

function safeDirectoryStat(stat) {
  const uid = effectiveUid();
  return (
    stat.isDirectory() &&
    !stat.isSymbolicLink() &&
    uid !== null &&
    stat.uid === uid &&
    Number(stat.mode & 0o7777n) === 0o700
  );
}

function safeTraversalDirectoryStat(stat) {
  const mode = stat.mode & 0o7777n;
  const uid = effectiveUid();
  return (
    stat.isDirectory() &&
    !stat.isSymbolicLink() &&
    uid !== null &&
    (stat.uid === 0n || stat.uid === uid) &&
    ((mode & 0o022n) === 0n || (mode & 0o1000n) !== 0n)
  );
}

function safeFileStat(stat, maximumLinks = 1n) {
  const uid = effectiveUid();
  return (
    stat.isFile() &&
    !stat.isSymbolicLink() &&
    stat.nlink >= 1n &&
    stat.nlink <= maximumLinks &&
    uid !== null &&
    stat.uid === uid &&
    Number(stat.mode & 0o7777n) === 0o600 &&
    stat.size > 0n &&
    stat.size <= BigInt(MAX_RECORD_BYTES)
  );
}

function safeStateOwnerMarkerStat(stat, expectedSize) {
  const uid = effectiveUid();
  return (
    stat.isFile() &&
    !stat.isSymbolicLink() &&
    stat.nlink === 1n &&
    uid !== null &&
    stat.uid === uid &&
    Number(stat.mode & 0o7777n) === 0o600 &&
    stat.size === BigInt(expectedSize)
  );
}

function assertStateOwnerId(value) {
  ensure(
    typeof value === "string" && regexpTest(STATE_OWNER_ID_PATTERN, value),
  );
  return value;
}

function stateOwnerMarkerBytes(stateOwnerId) {
  assertStateOwnerId(stateOwnerId);
  return callIntrinsic(bufferFromIntrinsic, Buffer, [
    `{"contractVersion":${PODMAN_WRITER_SUPERVISOR_STATE_OWNER_CONTRACT_VERSION},"stateOwnerId":"${stateOwnerId}"}\n`,
    "utf8",
  ]);
}

function parseStateOwnerMarker(bytes) {
  ensure(
    bytes.length > 0 && bytes.length <= MAX_RECORD_BYTES,
    "podman_writer_state_io_failed",
  );
  const raw = callIntrinsic(bufferToStringIntrinsic, bytes, ["utf8"]);
  let parsed;
  try {
    parsed = callIntrinsic(jsonParseIntrinsic, JsonObject, [raw]);
  } catch {
    fail("podman_writer_state_io_failed");
  }
  const marker = exactDataObject(
    parsed,
    ["contractVersion", "stateOwnerId"],
    "podman_writer_state_io_failed",
  );
  ensure(
    marker.contractVersion ===
        PODMAN_WRITER_SUPERVISOR_STATE_OWNER_CONTRACT_VERSION &&
      typeof marker.stateOwnerId === "string" &&
      regexpTest(STATE_OWNER_ID_PATTERN, marker.stateOwnerId),
    "podman_writer_state_io_failed",
  );
  const canonical = stateOwnerMarkerBytes(marker.stateOwnerId);
  ensure(
    callIntrinsic(bufferEqualsIntrinsic, bytes, [canonical]),
    "podman_writer_state_io_failed",
  );
  return marker.stateOwnerId;
}

function recordKey(launchAttemptId) {
  const hash = createHashIntrinsic("sha256");
  callIntrinsic(hashUpdateIntrinsic, hash, [
    "portable-codex-runtime:podman-writer-state:v1\0",
    "utf8",
  ]);
  callIntrinsic(hashUpdateIntrinsic, hash, [launchAttemptId, "utf8"]);
  return callIntrinsic(hashDigestIntrinsic, hash, ["hex"]);
}

function recordPath(root, launchAttemptId, revision) {
  return `${root}/${recordKey(launchAttemptId)}.${revision}.json`;
}

function pendingRecordPath(path) {
  return `${path}.pending`;
}

// The protected properties are directory object identity and access policy.
// Every named ancestor must remain a root/current-uid directory; shared
// writable ancestors are accepted only with sticky deletion protection, while
// the immediate parent and state root must be owned by this uid with mode 0700.
// Held/path dev+ino comparisons detect replacement; uid/mode checks detect
// access-policy drift.
async function validateParentChain(parentPath) {
  const components = [];
  let current = parentPath;
  while (current !== "/") {
    ensure(components.length < 256, "podman_writer_state_io_failed");
    components[components.length] = current;
    const next = pathDirnameIntrinsic(current);
    ensure(next !== current, "podman_writer_state_io_failed");
    current = next;
  }
  let parentStat = null;
  for (let index = components.length - 1; index >= 0; index -= 1) {
    const path = components[index];
    const stat = await lstat(path, { bigint: true });
    ensure(
      index === 0 ? safeDirectoryStat(stat) : safeTraversalDirectoryStat(stat),
      "podman_writer_state_io_failed",
    );
    if (index === 0) parentStat = stat;
  }
  ensure(parentStat !== null, "podman_writer_state_io_failed");
  return parentStat;
}

async function openPrivateParent(root) {
  const parentPath = pathDirnameIntrinsic(root);
  let handle;
  try {
    const before = await validateParentChain(parentPath);
    handle = await open(
      parentPath,
      fsConstants.O_RDONLY | fsConstants.O_DIRECTORY | fsConstants.O_NOFOLLOW,
    );
    const held = await handle.stat({ bigint: true });
    const current = await lstat(parentPath, { bigint: true });
    ensure(
      safeDirectoryStat(held) &&
        safeDirectoryStat(current) &&
        sameIdentity(before, held) &&
        sameIdentity(before, current),
      "podman_writer_state_io_failed",
    );
    return callIntrinsic(objectFreezeIntrinsic, Object, [
      { handle, identity: held, path: parentPath },
    ]);
  } catch (error) {
    if (handle) {
      try {
        await handle.close();
      } catch {
        fail("podman_writer_state_io_failed");
      }
    }
    throw error;
  }
}

async function assertPrivateParentHeld(parent) {
  const held = await parent.handle.stat({ bigint: true });
  const current = await validateParentChain(parent.path);
  ensure(
    safeDirectoryStat(held) &&
      safeDirectoryStat(current) &&
      sameIdentity(parent.identity, held) &&
      sameIdentity(parent.identity, current),
    "podman_writer_state_io_failed",
  );
}

async function rootAbsentFromHeldParent(root, parent) {
  let descriptorHandle;
  try {
    let path = root;
    if (runtimePlatform === "linux") {
      const descriptor = parent.handle.fd;
      ensure(
        numberIsSafeIntegerIntrinsic(descriptor) && descriptor >= 0,
        "podman_writer_state_io_failed",
      );
      const name = pathBasenameIntrinsic(root);
      ensure(
        name.length > 0 &&
          name !== "." &&
          name !== ".." &&
          pathDirnameIntrinsic(root) === parent.path,
        "podman_writer_state_io_failed",
      );
      const descriptorPath = `/proc/self/fd/${descriptor}`;
      descriptorHandle = await open(
        descriptorPath,
        fsConstants.O_RDONLY | fsConstants.O_DIRECTORY,
      );
      const descriptorStat = await descriptorHandle.stat({ bigint: true });
      const parentStat = await parent.handle.stat({ bigint: true });
      ensure(
        safeDirectoryStat(descriptorStat) &&
          safeDirectoryStat(parentStat) &&
          sameIdentity(parent.identity, descriptorStat) &&
          sameIdentity(parent.identity, parentStat),
        "podman_writer_state_io_failed",
      );
      const anchoredDescriptor = descriptorHandle.fd;
      ensure(
        numberIsSafeIntegerIntrinsic(anchoredDescriptor) &&
          anchoredDescriptor >= 0,
        "podman_writer_state_io_failed",
      );
      path = `/proc/self/fd/${anchoredDescriptor}/${name}`;
    }
    try {
      await lstat(path, { bigint: true });
      return false;
    } catch (error) {
      if (errorCode(error) === "ENOENT") return true;
      throw error;
    }
  } finally {
    if (descriptorHandle) {
      try {
        await descriptorHandle.close();
      } catch {
        fail("podman_writer_state_io_failed");
      }
    }
  }
}

// The protected properties for a missing root are the held parent object's
// identity, its access policy, and the stability/durability of the missing
// child entry. Linux can query the child through a validated clone of the held
// fd. Node exposes no portable fstatat, so other hosts, including macOS,
// bracket the pathname query with exact held versus named-parent identity and
// policy checks. Parent stat timestamp churn is not mutation evidence and is
// intentionally ignored. The non-Linux bracket detects one-way replacement;
// it does not claim to exclude an active same-uid ABA.
async function assertRootAbsentHeld(root, parent, faultHooks) {
  ensure(
    await rootAbsentFromHeldParent(root, parent),
    "podman_writer_state_io_failed",
  );
  await assertPrivateParentHeld(parent);
  await parent.handle.sync();
  await runFaultHook(faultHooks, "afterParentDirectorySync");
  ensure(
    await rootAbsentFromHeldParent(root, parent),
    "podman_writer_state_io_failed",
  );
  await assertPrivateParentHeld(parent);
}

async function heldPrivateDirectory(root, { create }, faultHooks) {
  let handle;
  let parent;
  try {
    parent = await openPrivateParent(root);
    await runFaultHook(faultHooks, "afterPrivateParentHold");
    let created = false;
    if (create) {
      try {
        await mkdir(root, { mode: 0o700 });
        created = true;
      } catch (error) {
        if (errorCode(error) !== "EEXIST") throw error;
      }
      if (created) await chmod(root, 0o700);
    }
    let before;
    try {
      before = await lstat(root, { bigint: true });
    } catch (error) {
      if (!create && errorCode(error) === "ENOENT") {
        await assertRootAbsentHeld(root, parent, faultHooks);
        await parent.handle.close();
        return null;
      }
      throw error;
    }
    ensure(
      before.isDirectory() &&
        !before.isSymbolicLink() &&
        before.uid === effectiveUid(),
      "podman_writer_state_io_failed",
    );
    ensure(safeDirectoryStat(before), "podman_writer_state_io_failed");
    handle = await open(
      root,
      fsConstants.O_RDONLY | fsConstants.O_DIRECTORY | fsConstants.O_NOFOLLOW,
    );
    const held = await handle.stat({ bigint: true });
    const current = await lstat(root, { bigint: true });
    ensure(
      safeDirectoryStat(held) &&
        safeDirectoryStat(current) &&
        sameIdentity(before, held) &&
        sameIdentity(before, current),
      "podman_writer_state_io_failed",
    );
    await assertPrivateParentHeld(parent);
    const heldDirectory = callIntrinsic(objectFreezeIntrinsic, Object, [
      {
        created,
        handle,
        identity: held,
        parentHandle: parent.handle,
        parentIdentity: parent.identity,
        parentPath: parent.path,
      },
    ]);
    if (create) {
      // The root inode/mode and its name in the immediate parent are separate
      // durability properties. Repeat both barriers for create-mode acquisition
      // so an earlier fsync acknowledgement loss cannot be bypassed by retry.
      await handle.sync();
      await runFaultHook(faultHooks, "afterRootDirectorySync");
      await parent.handle.sync();
      await runFaultHook(faultHooks, "afterParentDirectorySync");
      await assertDirectoryHeld(root, heldDirectory);
    }
    return heldDirectory;
  } catch (error) {
    if (handle) {
      try {
        await handle.close();
      } catch {
        // Preserve the original fail-closed outcome.
      }
    }
    if (parent?.handle) {
      try {
        await parent.handle.close();
      } catch {
        // Preserve the original fail-closed outcome.
      }
    }
    if (isStateError(error)) throw error;
    fail("podman_writer_state_io_failed");
  }
}

async function assertDirectoryHeld(root, held) {
  try {
    const handleStat = await held.handle.stat({ bigint: true });
    const pathStat = await lstat(root, { bigint: true });
    const parentHandleStat = await held.parentHandle.stat({ bigint: true });
    const parentPathStat = await validateParentChain(held.parentPath);
    ensure(
      safeDirectoryStat(handleStat) &&
        safeDirectoryStat(pathStat) &&
        sameIdentity(held.identity, handleStat) &&
        sameIdentity(held.identity, pathStat) &&
        safeDirectoryStat(parentHandleStat) &&
        sameIdentity(held.parentIdentity, parentHandleStat) &&
        sameIdentity(held.parentIdentity, parentPathStat),
      "podman_writer_state_io_failed",
    );
  } catch (error) {
    if (isStateError(error)) throw error;
    fail("podman_writer_state_io_failed");
  }
}

async function closeHeldDirectory(held) {
  let failed = false;
  try {
    await held.handle.close();
  } catch {
    failed = true;
  }
  try {
    await held.parentHandle.close();
  } catch {
    failed = true;
  }
  if (failed) fail("podman_writer_state_io_failed");
}

function stateOwnerMarkerPath(root) {
  return `${root}/${STATE_OWNER_MARKER_NAME}`;
}

// Linux entry operations are resolved through a validated clone of the held
// state-root fd. The clone keeps the lookup bound to the original directory
// object even if its absolute name is replaced after validation. The held and
// clone dev+ino pairs protect directory identity; safeDirectoryStat plus the
// named ancestor checks protect uid/mode/link/traversal policy. File callers
// separately bind dev+ino, uid/mode/nlink, and exact bytes read positionally.
//
// Node exposes no portable openat/fstatat/unlinkat. Non-Linux hosts therefore
// retain the held-versus-named root/ancestor brackets around the pathname
// operation. This detects one-way replacement, but does not claim to exclude
// an active same-uid ABA. In particular, /dev/fd is not assumed traversable.
async function withHeldDirectoryEntryPath(root, held, entryPath, callback) {
  let descriptorHandle = null;
  let callbackMissing = false;
  let primaryError = null;
  let result;
  const name = pathBasenameIntrinsic(entryPath);
  ensure(
    name.length > 0 &&
      name !== "." &&
      name !== ".." &&
      pathDirnameIntrinsic(entryPath) === root &&
      entryPath === `${root}/${name}`,
    "podman_writer_state_io_failed",
  );
  try {
    await assertDirectoryHeld(root, held);
    let path = entryPath;
    if (runtimePlatform === "linux") {
      const descriptor = held.handle.fd;
      ensure(
        numberIsSafeIntegerIntrinsic(descriptor) && descriptor >= 0,
        "podman_writer_state_io_failed",
      );
      descriptorHandle = await open(
        `/proc/self/fd/${descriptor}`,
        fsConstants.O_RDONLY | fsConstants.O_DIRECTORY,
      );
      const descriptorStat = await descriptorHandle.stat({ bigint: true });
      const rootStat = await held.handle.stat({ bigint: true });
      ensure(
        safeDirectoryStat(descriptorStat) &&
          safeDirectoryStat(rootStat) &&
          sameIdentity(held.identity, descriptorStat) &&
          sameIdentity(held.identity, rootStat),
        "podman_writer_state_io_failed",
      );
      const anchoredDescriptor = descriptorHandle.fd;
      ensure(
        numberIsSafeIntegerIntrinsic(anchoredDescriptor) &&
          anchoredDescriptor >= 0,
        "podman_writer_state_io_failed",
      );
      path = `/proc/self/fd/${anchoredDescriptor}/${name}`;
    }
    try {
      result = await callback(path);
    } catch (error) {
      primaryError = error;
      callbackMissing = errorCode(error) === "ENOENT";
    }
    // A post-operation identity or policy failure is authoritative even when
    // the entry callback also failed: its result cannot be trusted outside the
    // validated directory bracket.
    if (descriptorHandle !== null) {
      try {
        const descriptorStat = await descriptorHandle.stat({ bigint: true });
        const rootStat = await held.handle.stat({ bigint: true });
        ensure(
          safeDirectoryStat(descriptorStat) &&
            safeDirectoryStat(rootStat) &&
            sameIdentity(held.identity, descriptorStat) &&
            sameIdentity(held.identity, rootStat),
          "podman_writer_state_io_failed",
        );
      } catch (error) {
        primaryError = isStateError(error)
          ? error
          : new PodmanWriterSupervisorStateError(
              "podman_writer_state_io_failed",
            );
        callbackMissing = false;
      }
    }
    try {
      await assertDirectoryHeld(root, held);
    } catch (error) {
      primaryError = isStateError(error)
        ? error
        : new PodmanWriterSupervisorStateError(
            "podman_writer_state_io_failed",
          );
      callbackMissing = false;
    }
  } catch (error) {
    primaryError = isStateError(error)
      ? error
      : new PodmanWriterSupervisorStateError(
          "podman_writer_state_io_failed",
        );
    callbackMissing = false;
  }
  if (descriptorHandle !== null) {
    try {
      await descriptorHandle.close();
    } catch (error) {
      // A callback ENOENT is an expected missing-entry signal, not a stronger
      // failure than losing the clone-fd close acknowledgement. Preserve every
      // other callback/post-bracket error; normalize close fallout so an
      // ENOENT-shaped close error cannot be mistaken for entry absence.
      if (primaryError === null || callbackMissing) {
        primaryError = isStateError(error)
          ? error
          : new PodmanWriterSupervisorStateError(
              "podman_writer_state_io_failed",
            );
      }
    }
  }
  if (primaryError !== null) {
    throw primaryError;
  }
  return result;
}

async function withStateOwnerMarkerLookupPath(root, held, callback) {
  try {
    return await withHeldDirectoryEntryPath(
      root,
      held,
      stateOwnerMarkerPath(root),
      callback,
    );
  } catch (error) {
    if (isStateError(error)) throw error;
    fail("podman_writer_state_io_failed");
  }
}

function lstatStateOwnerMarker(root, held) {
  return withStateOwnerMarkerLookupPath(root, held, (path) =>
    lstat(path, { bigint: true }));
}

async function openStateOwnerMarkerPath(root, held, flags, mode = undefined) {
  let handle = null;
  try {
    await withStateOwnerMarkerLookupPath(root, held, async (path) => {
      handle = mode === undefined
        ? await open(path, flags)
        : await open(path, flags, mode);
    });
    return handle;
  } catch (error) {
    if (handle !== null) {
      try {
        await handle.close();
      } catch {
        // Preserve the root/parent revalidation failure.
      }
    }
    throw error;
  }
}

function stateOwnerBinding(root, held, marker) {
  return frozenRecord({
    markerBytes: callIntrinsic(bufferFromIntrinsic, Buffer, [marker.bytes]),
    markerDev: marker.stat.dev,
    markerIno: marker.stat.ino,
    markerPath: marker.path,
    parentDev: held.parentIdentity.dev,
    parentIno: held.parentIdentity.ino,
    root,
    rootDev: held.identity.dev,
    rootIno: held.identity.ino,
    stateOwnerId: marker.stateOwnerId,
  });
}

function assertStateOwnerRootBaseline(held, binding) {
  ensure(
    held.identity.dev === binding.rootDev &&
      held.identity.ino === binding.rootIno &&
      held.parentIdentity.dev === binding.parentDev &&
      held.parentIdentity.ino === binding.parentIno,
    "podman_writer_state_io_failed",
  );
}

async function readHeldFileExactly(handle, size) {
  ensure(
    numberIsSafeIntegerIntrinsic(size) && size > 0 && size <= MAX_RECORD_BYTES,
    "podman_writer_state_io_failed",
  );
  const bytes = callIntrinsic(bufferAllocIntrinsic, Buffer, [size]);
  const result = await handle.read(bytes, 0, size, 0);
  ensure(result.bytesRead === size, "podman_writer_state_io_failed");
  return bytes;
}

// The owner marker protects three independent properties. Object identity is
// the held root/marker fd pair plus dev+ino equality with their names. Content
// stability is the exact canonical marker byte string. Access policy is the
// current-uid 0700 root/parent and current-uid 0600, nlink-one marker. Ordinary
// timestamp or child-entry churn is not mutation evidence; it only causes the
// content and policy properties to be revalidated through held descriptors.
async function assertStateOwnerMarkerHeld(root, held, marker, binding = null) {
  await assertDirectoryHeld(root, held);
  if (binding !== null) assertStateOwnerRootBaseline(held, binding);
  const handleStat = await marker.handle.stat({ bigint: true });
  const pathStat = await lstatStateOwnerMarker(root, held);
  const expectedBytes = binding === null ? marker.bytes : binding.markerBytes;
  ensure(
    safeStateOwnerMarkerStat(handleStat, expectedBytes.length) &&
      safeStateOwnerMarkerStat(pathStat, expectedBytes.length) &&
      sameIdentity(marker.stat, handleStat) &&
      sameIdentity(marker.stat, pathStat) &&
      (binding === null ||
        (handleStat.dev === binding.markerDev &&
          handleStat.ino === binding.markerIno)),
    "podman_writer_state_io_failed",
  );
  const bytes = await readHeldFileExactly(marker.handle, expectedBytes.length);
  const finalStat = await marker.handle.stat({ bigint: true });
  ensure(
    safeStateOwnerMarkerStat(finalStat, expectedBytes.length) &&
      sameIdentity(marker.stat, finalStat) &&
      callIntrinsic(bufferEqualsIntrinsic, bytes, [expectedBytes]),
    "podman_writer_state_io_failed",
  );
  const stateOwnerId = parseStateOwnerMarker(bytes);
  ensure(
    stateOwnerId === marker.stateOwnerId &&
      (binding === null || stateOwnerId === binding.stateOwnerId),
    "podman_writer_state_io_failed",
  );
  await assertDirectoryHeld(root, held);
  if (binding !== null) assertStateOwnerRootBaseline(held, binding);
}

async function openStateOwnerMarker(
  root,
  held,
  binding = null,
  { writable = false } = {},
) {
  let handle;
  const path = stateOwnerMarkerPath(root);
  try {
    await assertDirectoryHeld(root, held);
    if (binding !== null) {
      assertStateOwnerRootBaseline(held, binding);
      ensure(path === binding.markerPath, "podman_writer_state_io_failed");
    }
    const before = await lstatStateOwnerMarker(root, held);
    ensure(safeFileStat(before), "podman_writer_state_io_failed");
    handle = await openStateOwnerMarkerPath(
      root,
      held,
      (writable ? fsConstants.O_RDWR : fsConstants.O_RDONLY) |
        fsConstants.O_NOFOLLOW,
    );
    const opened = await handle.stat({ bigint: true });
    ensure(
      safeFileStat(opened) && sameIdentity(before, opened),
      "podman_writer_state_io_failed",
    );
    const initialSize = Number(opened.size);
    const bytes = await readHeldFileExactly(handle, initialSize);
    const stateOwnerId = parseStateOwnerMarker(bytes);
    const after = await handle.stat({ bigint: true });
    const current = await lstatStateOwnerMarker(root, held);
    ensure(
      safeStateOwnerMarkerStat(after, bytes.length) &&
        safeStateOwnerMarkerStat(current, bytes.length) &&
        sameIdentity(before, after) &&
        sameIdentity(before, current),
      "podman_writer_state_io_failed",
    );
    const marker = callIntrinsic(objectFreezeIntrinsic, Object, [
      { bytes, handle, path, stat: after, stateOwnerId },
    ]);
    await assertStateOwnerMarkerHeld(root, held, marker, binding);
    return marker;
  } catch (error) {
    if (handle) {
      try {
        await handle.close();
      } catch {
        // Preserve the primary fail-closed result.
      }
    }
    if (isStateError(error)) throw error;
    fail("podman_writer_state_io_failed");
  }
}

async function createStateOwnerMarker(root, held, stateOwnerId) {
  let handle;
  const path = stateOwnerMarkerPath(root);
  const bytes = stateOwnerMarkerBytes(stateOwnerId);
  try {
    await assertDirectoryHeld(root, held);
    handle = await openStateOwnerMarkerPath(
      root,
      held,
      fsConstants.O_RDWR |
        fsConstants.O_CREAT |
        fsConstants.O_EXCL |
        fsConstants.O_NOFOLLOW,
      0o600,
    );
    await handle.writeFile(bytes);
    await handle.chmod(0o600);
    await handle.sync();
    const opened = await handle.stat({ bigint: true });
    const current = await lstatStateOwnerMarker(root, held);
    ensure(
      safeStateOwnerMarkerStat(opened, bytes.length) &&
        safeStateOwnerMarkerStat(current, bytes.length) &&
        sameIdentity(opened, current),
      "podman_writer_state_io_failed",
    );
    const marker = callIntrinsic(objectFreezeIntrinsic, Object, [
      { bytes, handle, path, stat: opened, stateOwnerId },
    ]);
    await assertStateOwnerMarkerHeld(root, held, marker);
    await held.handle.sync();
    await held.parentHandle.sync();
    await assertStateOwnerMarkerHeld(root, held, marker);
    return marker;
  } catch (error) {
    if (handle) {
      try {
        await handle.close();
      } catch {
        // Preserve the primary fail-closed result.
      }
    }
    if (isStateError(error)) throw error;
    fail("podman_writer_state_io_failed");
  }
}

async function closeStateOwnerMarker(marker) {
  try {
    await marker.handle.close();
  } catch {
    fail("podman_writer_state_io_failed");
  }
}

function privateDirectoryChildPath(parent, name) {
  ensure(
    typeof parent === "string" &&
      typeof name === "string" &&
      name.length > 0 &&
      name !== "." &&
      name !== "..",
    "podman_writer_state_io_failed",
  );
  const path = parent === "/" ? `/${name}` : `${parent}/${name}`;
  ensure(
    pathDirnameIntrinsic(path) === parent &&
      pathBasenameIntrinsic(path) === name &&
      pathResolveIntrinsic(path) === path,
    "podman_writer_state_io_failed",
  );
  return path;
}

function stateOwnerStagingPath(root) {
  const random = callIntrinsic(randomBytesIntrinsic, undefined, [
    STATE_OWNER_STAGING_RANDOM_BYTES,
  ]);
  const suffix = callIntrinsic(bufferToStringIntrinsic, random, ["hex"]);
  const name = `${STATE_OWNER_STAGING_PREFIX}${suffix}`;
  ensure(
    name.length === STATE_OWNER_STAGING_BASENAME_BYTES &&
      STATE_OWNER_STAGING_BASENAME_BYTES === 48 &&
      STATE_OWNER_STAGING_MARKER_WORST_CASE_BYTES === 4_083 &&
      STATE_OWNER_STAGING_MARKER_WORST_CASE_BYTES <= MAX_NATIVE_PATH_BYTES,
    "podman_writer_state_io_failed",
  );
  const path = privateDirectoryChildPath(pathDirnameIntrinsic(root), name);
  ensure(path !== root, "podman_writer_state_io_failed");
  const markerPath = `${path}/${STATE_OWNER_MARKER_NAME}`;
  // Worst case: (MAX_STATE_ROOT_BYTES - "/x") + "/" + 48-byte
  // basename + "/" + 20-byte marker = 4083 bytes, below 4095.
  ensure(
    callIntrinsic(bufferFromIntrinsic, Buffer, [markerPath, "utf8"]).length <=
      MAX_NATIVE_PATH_BYTES,
    "podman_writer_state_io_failed",
  );
  return path;
}

function privateParentEntryName(parent, path) {
  const name = pathBasenameIntrinsic(path);
  ensure(
    name.length > 0 &&
      name !== "." &&
      name !== ".." &&
      pathDirnameIntrinsic(path) === parent.path &&
      path === privateDirectoryChildPath(parent.path, name),
    "podman_writer_state_io_failed",
  );
  return name;
}

// Initial owner publication needs two sibling names resolved under one held
// private parent. Linux binds both lookups to a validated clone of that parent
// fd. Other hosts retain the held-versus-named parent bracket and, as with the
// existing directory helpers, do not claim to exclude an active same-uid ABA.
async function withHeldPrivateParentEntryPaths(
  parent,
  firstPath,
  secondPath,
  callback,
) {
  const firstName = privateParentEntryName(parent, firstPath);
  const secondName = secondPath === null
    ? null
    : privateParentEntryName(parent, secondPath);
  let descriptorHandle = null;
  let primaryError = null;
  let result;
  try {
    await assertPrivateParentHeld(parent);
    let basePath = parent.path;
    if (runtimePlatform === "linux") {
      const descriptor = parent.handle.fd;
      ensure(
        numberIsSafeIntegerIntrinsic(descriptor) && descriptor >= 0,
        "podman_writer_state_io_failed",
      );
      descriptorHandle = await open(
        `/proc/self/fd/${descriptor}`,
        fsConstants.O_RDONLY | fsConstants.O_DIRECTORY,
      );
      const descriptorStat = await descriptorHandle.stat({ bigint: true });
      const parentStat = await parent.handle.stat({ bigint: true });
      ensure(
        safeDirectoryStat(descriptorStat) &&
          safeDirectoryStat(parentStat) &&
          sameIdentity(parent.identity, descriptorStat) &&
          sameIdentity(parent.identity, parentStat),
        "podman_writer_state_io_failed",
      );
      const anchoredDescriptor = descriptorHandle.fd;
      ensure(
        numberIsSafeIntegerIntrinsic(anchoredDescriptor) &&
          anchoredDescriptor >= 0,
        "podman_writer_state_io_failed",
      );
      basePath = `/proc/self/fd/${anchoredDescriptor}`;
    }
    try {
      result = await callback(
        privateDirectoryChildPath(basePath, firstName),
        secondName === null
          ? null
          : privateDirectoryChildPath(basePath, secondName),
      );
    } catch (error) {
      primaryError = error;
    }
    if (descriptorHandle !== null) {
      try {
        const descriptorStat = await descriptorHandle.stat({ bigint: true });
        const parentStat = await parent.handle.stat({ bigint: true });
        ensure(
          safeDirectoryStat(descriptorStat) &&
            safeDirectoryStat(parentStat) &&
            sameIdentity(parent.identity, descriptorStat) &&
            sameIdentity(parent.identity, parentStat),
          "podman_writer_state_io_failed",
        );
      } catch (error) {
        primaryError = isStateError(error)
          ? error
          : new PodmanWriterSupervisorStateError(
              "podman_writer_state_io_failed",
            );
      }
    }
    try {
      await assertPrivateParentHeld(parent);
    } catch (error) {
      primaryError = isStateError(error)
        ? error
        : new PodmanWriterSupervisorStateError(
            "podman_writer_state_io_failed",
          );
    }
  } catch (error) {
    primaryError = isStateError(error)
      ? error
      : new PodmanWriterSupervisorStateError(
          "podman_writer_state_io_failed",
        );
  }
  if (descriptorHandle !== null) {
    try {
      await descriptorHandle.close();
    } catch {
      primaryError = new PodmanWriterSupervisorStateError(
        "podman_writer_state_io_failed",
      );
    }
  }
  if (primaryError !== null) throw primaryError;
  return result;
}

function privateParentFromHeldDirectory(held) {
  return callIntrinsic(objectFreezeIntrinsic, Object, [{
    handle: held.parentHandle,
    identity: held.parentIdentity,
    path: held.parentPath,
  }]);
}

// Owner initialization protects exactly three properties. Directory and
// marker object identity are held-fd plus dev+ino equality with their names;
// marker content is the exact canonical byte string; access policy is the
// current-uid 0700 parent/root and current-uid 0600, nlink-one marker. ctime,
// mtime, and benign sibling-entry churn are not mutation evidence.
async function assertStateOwnerDirectoryEntryHeld(root, held) {
  const parent = privateParentFromHeldDirectory(held);
  try {
    const handleStat = await held.handle.stat({ bigint: true });
    const pathStat = await withHeldPrivateParentEntryPaths(
      parent,
      root,
      null,
      (path) => lstat(path, { bigint: true }),
    );
    ensure(
      safeDirectoryStat(handleStat) &&
        safeDirectoryStat(pathStat) &&
        sameIdentity(held.identity, handleStat) &&
        sameIdentity(held.identity, pathStat),
      "podman_writer_state_io_failed",
    );
  } catch (error) {
    if (isStateError(error)) throw error;
    fail("podman_writer_state_io_failed");
  }
}

async function createStateOwnerStagingDirectory(root, parent, stagingPath) {
  let handle = null;
  let identity = null;
  try {
    await withHeldPrivateParentEntryPaths(
      parent,
      root,
      stagingPath,
      async (_rootPath, heldStagingPath) => {
        await mkdir(heldStagingPath, { mode: 0o700 });
        await chmod(heldStagingPath, 0o700);
        const before = await lstat(heldStagingPath, { bigint: true });
        ensure(safeDirectoryStat(before), "podman_writer_state_io_failed");
        handle = await open(
          heldStagingPath,
          fsConstants.O_RDONLY |
            fsConstants.O_DIRECTORY |
            fsConstants.O_NOFOLLOW,
        );
        const opened = await handle.stat({ bigint: true });
        const current = await lstat(heldStagingPath, { bigint: true });
        ensure(
          safeDirectoryStat(opened) &&
            safeDirectoryStat(current) &&
            sameIdentity(before, opened) &&
            sameIdentity(before, current),
          "podman_writer_state_io_failed",
        );
        identity = opened;
      },
    );
    ensure(handle !== null && identity !== null, "podman_writer_state_io_failed");
    const held = callIntrinsic(objectFreezeIntrinsic, Object, [{
      created: true,
      handle,
      identity,
      parentHandle: parent.handle,
      parentIdentity: parent.identity,
      parentPath: parent.path,
    }]);
    await assertStateOwnerDirectoryEntryHeld(stagingPath, held);
    return held;
  } catch (error) {
    if (handle !== null) {
      try {
        await handle.close();
      } catch {
        // Preserve the primary fail-closed result.
      }
    }
    if (isStateError(error)) throw error;
    fail("podman_writer_state_io_failed");
  }
}

function stateOwnerMarkerAtRoot(root, marker) {
  return callIntrinsic(objectFreezeIntrinsic, Object, [{
    bytes: marker.bytes,
    handle: marker.handle,
    path: stateOwnerMarkerPath(root),
    stat: marker.stat,
    stateOwnerId: marker.stateOwnerId,
  }]);
}

async function reconcilePublishedStateOwnerCandidate(root, held, marker) {
  const parent = privateParentFromHeldDirectory(held);
  const status = await withHeldPrivateParentEntryPaths(
    parent,
    root,
    null,
    async (heldRootPath) => {
      let pathStat;
      try {
        pathStat = await lstat(heldRootPath, { bigint: true });
      } catch (error) {
        if (errorCode(error) === "ENOENT") return "absent";
        throw error;
      }
      const handleStat = await held.handle.stat({ bigint: true });
      ensure(
        safeDirectoryStat(handleStat) &&
          sameIdentity(held.identity, handleStat),
        "podman_writer_state_io_failed",
      );
      if (!sameIdentity(held.identity, pathStat)) return "different";
      ensure(safeDirectoryStat(pathStat), "podman_writer_state_io_failed");
      return "ours";
    },
  );
  if (status !== "ours") return frozenRecord({ marker: null, status });
  const publishedMarker = stateOwnerMarkerAtRoot(root, marker);
  // Once the final name resolves to the held candidate, every marker or guard
  // failure is authoritative. It must never be downgraded to "different" and
  // routed through winner adoption.
  await assertStateOwnerMarkerHeld(root, held, publishedMarker);
  return frozenRecord({ marker: publishedMarker, status });
}

// `heldStagingPath` must come from withHeldPrivateParentEntryPaths. The first
// and final directory listings bracket exact marker identity/content checks,
// proving that the rename source namespace contains only the canonical marker.
async function assertInitialStateOwnerCandidateNamespace(
  heldStagingPath,
  held,
  marker,
) {
  const candidatePathStat = await lstat(heldStagingPath, { bigint: true });
  const candidateHandleStat = await held.handle.stat({ bigint: true });
  ensure(
    safeDirectoryStat(candidatePathStat) &&
      safeDirectoryStat(candidateHandleStat) &&
      sameIdentity(held.identity, candidatePathStat) &&
      sameIdentity(held.identity, candidateHandleStat),
    "podman_writer_state_io_failed",
  );
  const firstEntries = await readdir(heldStagingPath);
  ensure(
    firstEntries.length === 1 &&
      firstEntries[0] === STATE_OWNER_MARKER_NAME,
    "podman_writer_state_io_failed",
  );
  const heldMarkerPath = `${heldStagingPath}/${STATE_OWNER_MARKER_NAME}`;
  const markerPathStat = await lstat(heldMarkerPath, { bigint: true });
  const markerHandleStat = await marker.handle.stat({ bigint: true });
  const markerBytes = await readHeldFileExactly(
    marker.handle,
    marker.bytes.length,
  );
  const markerFinalStat = await marker.handle.stat({ bigint: true });
  const finalEntries = await readdir(heldStagingPath);
  const candidateFinalStat = await lstat(heldStagingPath, { bigint: true });
  ensure(
    safeStateOwnerMarkerStat(markerPathStat, marker.bytes.length) &&
      safeStateOwnerMarkerStat(markerHandleStat, marker.bytes.length) &&
      safeStateOwnerMarkerStat(markerFinalStat, marker.bytes.length) &&
      sameIdentity(marker.stat, markerPathStat) &&
      sameIdentity(marker.stat, markerHandleStat) &&
      sameIdentity(marker.stat, markerFinalStat) &&
      callIntrinsic(bufferEqualsIntrinsic, markerBytes, [marker.bytes]) &&
      finalEntries.length === 1 &&
      finalEntries[0] === STATE_OWNER_MARKER_NAME &&
      safeDirectoryStat(candidateFinalStat) &&
      sameIdentity(held.identity, candidateFinalStat),
    "podman_writer_state_io_failed",
  );
}

function unlinkedStateOwnerMarkerStat(stat, expectedSize) {
  const uid = effectiveUid();
  return (
    stat.isFile() &&
    !stat.isSymbolicLink() &&
    stat.nlink === 0n &&
    uid !== null &&
    stat.uid === uid &&
    Number(stat.mode & 0o7777n) === 0o600 &&
    stat.size === BigInt(expectedSize)
  );
}

async function assertStateOwnerCandidateNameAbsent(
  root,
  stagingPath,
  parent,
  winner,
) {
  const stagingAbsent = await withHeldPrivateParentEntryPaths(
    parent,
    root,
    stagingPath,
    async (heldRootPath, heldStagingPath) => {
      const winnerPathStat = await lstat(heldRootPath, { bigint: true });
      const winnerHandleStat = await winner.held.handle.stat({ bigint: true });
      ensure(
        safeDirectoryStat(winnerPathStat) &&
          safeDirectoryStat(winnerHandleStat) &&
          sameIdentity(winner.held.identity, winnerPathStat) &&
          sameIdentity(winner.held.identity, winnerHandleStat),
        "podman_writer_state_io_failed",
      );
      try {
        await lstat(heldStagingPath, { bigint: true });
        return false;
      } catch (error) {
        if (errorCode(error) === "ENOENT") return true;
        throw error;
      }
    },
  );
  ensure(stagingAbsent, "podman_writer_state_io_failed");
}

async function cleanupExactLosingStateOwnerCandidate(
  root,
  stagingPath,
  held,
  marker,
  winner,
) {
  const parent = privateParentFromHeldDirectory(held);
  ensure(
    sameIdentity(held.parentIdentity, winner.held.parentIdentity) &&
      !sameIdentity(held.identity, winner.held.identity),
    "podman_writer_state_io_failed",
  );
  await assertStateOwnerDirectoryEntryHeld(root, winner.held);
  await assertStateOwnerMarkerHeld(root, winner.held, winner.marker);
  await assertStateOwnerDirectoryEntryHeld(stagingPath, held);
  await assertStateOwnerMarkerHeld(stagingPath, held, marker);
  await withHeldPrivateParentEntryPaths(
    parent,
    root,
    stagingPath,
    async (heldRootPath, heldStagingPath) => {
      const winnerPathStat = await lstat(heldRootPath, { bigint: true });
      const winnerHandleStat = await winner.held.handle.stat({ bigint: true });
      ensure(
        safeDirectoryStat(winnerPathStat) &&
          safeDirectoryStat(winnerHandleStat) &&
          sameIdentity(winner.held.identity, winnerPathStat) &&
          sameIdentity(winner.held.identity, winnerHandleStat),
        "podman_writer_state_io_failed",
      );
      await assertInitialStateOwnerCandidateNamespace(
        heldStagingPath,
        held,
        marker,
      );
    },
  );

  await withHeldDirectoryEntryPath(
    stagingPath,
    held,
    stateOwnerMarkerPath(stagingPath),
    async (path) => {
      const markerPathStat = await lstat(path, { bigint: true });
      const markerHandleStat = await marker.handle.stat({ bigint: true });
      const markerBytes = await readHeldFileExactly(
        marker.handle,
        marker.bytes.length,
      );
      const markerFinalStat = await marker.handle.stat({ bigint: true });
      ensure(
        safeStateOwnerMarkerStat(markerPathStat, marker.bytes.length) &&
          safeStateOwnerMarkerStat(markerHandleStat, marker.bytes.length) &&
          safeStateOwnerMarkerStat(markerFinalStat, marker.bytes.length) &&
          sameIdentity(marker.stat, markerPathStat) &&
          sameIdentity(marker.stat, markerHandleStat) &&
          sameIdentity(marker.stat, markerFinalStat) &&
          callIntrinsic(bufferEqualsIntrinsic, markerBytes, [marker.bytes]),
        "podman_writer_state_io_failed",
      );
      await unlink(path);
    },
  );
  // Node has no conditioned unlink-by-inode. The held marker's zero link count
  // below is a post-operation success proof, not prevention: an active same-uid
  // process can swap in a bait entry after the final check, causing this call
  // to delete the bait before the retained marker fd makes us fail closed.
  const unlinkedMarkerStat = await marker.handle.stat({ bigint: true });
  const unlinkedMarkerBytes = await readHeldFileExactly(
    marker.handle,
    marker.bytes.length,
  );
  ensure(
    unlinkedStateOwnerMarkerStat(unlinkedMarkerStat, marker.bytes.length) &&
      sameIdentity(marker.stat, unlinkedMarkerStat) &&
      callIntrinsic(bufferEqualsIntrinsic, unlinkedMarkerBytes, [marker.bytes]),
    "podman_writer_state_io_failed",
  );
  await held.handle.sync();

  await withHeldPrivateParentEntryPaths(
    parent,
    root,
    stagingPath,
    async (heldRootPath, heldStagingPath) => {
      const winnerPathStat = await lstat(heldRootPath, { bigint: true });
      const winnerHandleStat = await winner.held.handle.stat({ bigint: true });
      const candidatePathStat = await lstat(heldStagingPath, { bigint: true });
      const candidateHandleStat = await held.handle.stat({ bigint: true });
      const entries = await readdir(heldStagingPath);
      ensure(
        safeDirectoryStat(winnerPathStat) &&
          safeDirectoryStat(winnerHandleStat) &&
          sameIdentity(winner.held.identity, winnerPathStat) &&
          sameIdentity(winner.held.identity, winnerHandleStat) &&
          safeDirectoryStat(candidatePathStat) &&
          safeDirectoryStat(candidateHandleStat) &&
          sameIdentity(held.identity, candidatePathStat) &&
          sameIdentity(held.identity, candidateHandleStat) &&
          entries.length === 0,
        "podman_writer_state_io_failed",
      );
      await rmdir(heldStagingPath);
    },
  );
  const removedCandidateStat = await held.handle.stat({ bigint: true });
  ensure(
    safeDirectoryStat(removedCandidateStat) &&
      sameIdentity(held.identity, removedCandidateStat) &&
      (runtimePlatform !== "linux" || removedCandidateStat.nlink === 0n),
    "podman_writer_state_io_failed",
  );
  // Node likewise has no conditioned rmdir-by-inode. Linux's zero-link held fd
  // proves successful rmdir removed this exact inode only after the syscall;
  // a same-uid bait swap can be detected after destructive action, not
  // prevented. Darwin keeps nlink=2 after rmdir (verified on APFS), so
  // non-Linux instead relies on held identity/policy plus bracketed name
  // absence and makes the same active-ABA non-guarantee.
  await assertStateOwnerCandidateNameAbsent(
    root,
    stagingPath,
    parent,
    winner,
  );
  await held.parentHandle.sync();
  await assertStateOwnerCandidateNameAbsent(
    root,
    stagingPath,
    parent,
    winner,
  );
  await assertStateOwnerDirectoryEntryHeld(root, winner.held);
  await assertStateOwnerMarkerHeld(root, winner.held, winner.marker);
}

async function closeUnpublishedStateOwnerCandidate(held, marker) {
  let primaryError = null;
  if (marker !== null) {
    try {
      await closeStateOwnerMarker(marker);
    } catch (error) {
      primaryError = error;
    }
  }
  if (held !== null) {
    try {
      await closeHeldDirectory(held);
    } catch (error) {
      primaryError ??= error;
    }
  }
  if (primaryError !== null) throw primaryError;
}

async function openPreparedStateOwnerRoot(root) {
  let held = null;
  let marker = null;
  let primaryError = null;
  try {
    held = await heldPrivateDirectory(root, { create: false }, frozenRecord({}));
    if (held === null) return null;
    marker = await openStateOwnerMarker(root, held, null, { writable: true });
    return frozenRecord({ held, marker });
  } catch (error) {
    primaryError = error;
  }
  try {
    await closeUnpublishedStateOwnerCandidate(held, marker);
  } catch (error) {
    primaryError ??= error;
  }
  if (isStateError(primaryError)) throw primaryError;
  fail("podman_writer_state_io_failed");
}

async function createAndPublishStateOwnerRoot(root) {
  let parent = null;
  let held = null;
  let marker = null;
  let primaryError = null;
  let prepared = null;
  try {
    parent = await openPrivateParent(root);
    if (!(await rootAbsentFromHeldParent(root, parent))) {
      await assertPrivateParentHeld(parent);
      await parent.handle.close();
      parent = null;
      prepared = await openPreparedStateOwnerRoot(root);
      ensure(prepared !== null, "podman_writer_state_io_failed");
      return prepared;
    }
    await assertPrivateParentHeld(parent);
    const stagingPath = stateOwnerStagingPath(root);
    held = await createStateOwnerStagingDirectory(root, parent, stagingPath);
    parent = null;
    const random = callIntrinsic(randomBytesIntrinsic, undefined, [32]);
    const stateOwnerId = `state-owner:${callIntrinsic(
      bufferToStringIntrinsic,
      random,
      ["hex"],
    )}`;
    assertStateOwnerId(stateOwnerId);

    // Candidate durability is file -> candidate root -> parent. The marker is
    // complete and canonical before the final root name can become visible.
    marker = await createStateOwnerMarker(stagingPath, held, stateOwnerId);
    await assertStateOwnerDirectoryEntryHeld(stagingPath, held);
    await assertStateOwnerMarkerHeld(stagingPath, held, marker);

    let publication = null;
    let renameError = null;
    try {
      publication = await withHeldPrivateParentEntryPaths(
        privateParentFromHeldDirectory(held),
        root,
        stagingPath,
        async (heldRootPath, heldStagingPath) => {
          // Bind the rename source namespace entry to the candidate fd after
          // the candidate durability barriers and under this same parent-fd
          // clone. Metadata timestamps are deliberately irrelevant.
          await assertInitialStateOwnerCandidateNamespace(
            heldStagingPath,
            held,
            marker,
          );
          try {
            await lstat(heldRootPath, { bigint: true });
            return "winner";
          } catch (error) {
            if (errorCode(error) !== "ENOENT") throw error;
          }
          // Node exposes ordinary rename, not renameat2(RENAME_NOREPLACE).
          // Cooperative initializers publish non-empty complete directories,
          // so only one wins. A non-cooperative same-uid process can still
          // insert an empty root after the last absence check; this code does
          // not claim to exclude that final race.
          await rename(heldStagingPath, heldRootPath);
          return "renamed";
        },
      );
    } catch (error) {
      // Clone-close or parent post-bracket failures are authoritative safety
      // failures from the path guard, not ambiguous rename acknowledgements.
      if (isStateError(error)) throw error;
      renameError = error;
    }

    if (publication === "renamed" || renameError !== null) {
      const reconciliation = await reconcilePublishedStateOwnerCandidate(
        root,
        held,
        marker,
      );
      if (reconciliation.status === "ours") {
        marker = reconciliation.marker;
        // Final durability repeats file -> final root -> parent, followed by a
        // second exact identity/content/access-policy revalidation.
        await marker.handle.sync();
        await held.handle.sync();
        await held.parentHandle.sync();
        await assertStateOwnerDirectoryEntryHeld(root, held);
        await assertStateOwnerMarkerHeld(root, held, marker);
        prepared = frozenRecord({ held, marker });
        return prepared;
      }
      if (
        publication === "renamed" ||
        !arrayIncludes(["EEXIST", "ENOTEMPTY", "ENOENT"], errorCode(renameError))
      ) {
        throw renameError ?? new PodmanWriterSupervisorStateError(
          "podman_writer_state_io_failed",
        );
      }
    }

    // A strictly validated distinct winner makes this candidate a proved
    // loser. Only that case permits exact, anchored, non-recursive cleanup.
    // Crash debris and every uncertain topology remain inert and untouched; a
    // fresh retry always uses a new high-entropy basename.
    prepared = await openPreparedStateOwnerRoot(root);
    ensure(prepared !== null, "podman_writer_state_io_failed");
    await cleanupExactLosingStateOwnerCandidate(
      root,
      stagingPath,
      held,
      marker,
      prepared,
    );
    await closeUnpublishedStateOwnerCandidate(held, marker);
    held = null;
    marker = null;
    return prepared;
  } catch (error) {
    primaryError = error;
  }
  if (marker !== null || held !== null) {
    try {
      await closeUnpublishedStateOwnerCandidate(held, marker);
    } catch (error) {
      primaryError ??= error;
    }
  } else if (parent !== null) {
    try {
      await parent.handle.close();
    } catch (error) {
      primaryError ??= error;
    }
  }
  if (prepared !== null) {
    try {
      await closeUnpublishedStateOwnerCandidate(
        prepared.held,
        prepared.marker,
      );
    } catch (error) {
      primaryError ??= error;
    }
  }
  if (isStateError(primaryError)) throw primaryError;
  fail("podman_writer_state_io_failed");
}

async function withStateDirectory(
  root,
  create,
  faultHooks,
  ownerBinding,
  callback,
) {
  let held = null;
  let marker = null;
  let primaryError = null;
  let result;
  try {
    held = await heldPrivateDirectory(
      root,
      { create: ownerBinding === null && create },
      faultHooks,
    );
    if (ownerBinding !== null) {
      ensure(held !== null, "podman_writer_state_io_failed");
      assertStateOwnerRootBaseline(held, ownerBinding);
      marker = await openStateOwnerMarker(root, held, ownerBinding);
    }
    result = await callback(held);
  } catch (error) {
    primaryError = error;
  }
  if (marker !== null) {
    try {
      await assertStateOwnerMarkerHeld(root, held, marker, ownerBinding);
    } catch (error) {
      primaryError = error;
    }
    try {
      await closeStateOwnerMarker(marker);
    } catch (error) {
      primaryError ??= error;
    }
  }
  if (held !== null) {
    try {
      await closeHeldDirectory(held);
    } catch (error) {
      primaryError ??= error;
    }
  }
  if (primaryError !== null) {
    if (isStateError(primaryError)) throw primaryError;
    fail("podman_writer_state_io_failed");
  }
  return result;
}

async function readPlainSafeFile(path, maximumLinks = 1n) {
  let handle;
  try {
    const before = await lstat(path, { bigint: true });
    ensure(
      safeFileStat(before, maximumLinks),
      "podman_writer_state_io_failed",
    );
    handle = await open(path, fsConstants.O_RDONLY | fsConstants.O_NOFOLLOW);
    const opened = await handle.stat({ bigint: true });
    ensure(
      safeFileStat(opened, maximumLinks) && sameIdentity(before, opened),
      "podman_writer_state_io_failed",
    );
    const first = await handle.readFile();
    const after = await handle.stat({ bigint: true });
    ensure(
      safeFileStat(after, maximumLinks) && sameIdentity(before, after),
      "podman_writer_state_io_failed",
    );
    if (
      before.size !== after.size ||
      before.mtimeNs !== after.mtimeNs ||
      before.ctimeNs !== after.ctimeNs ||
      before.nlink !== after.nlink
    ) {
      await handle.close();
      handle = undefined;
      handle = await open(path, fsConstants.O_RDONLY | fsConstants.O_NOFOLLOW);
      const repeatedIdentity = await handle.stat({ bigint: true });
      ensure(
        safeFileStat(repeatedIdentity, maximumLinks) &&
          sameIdentity(before, repeatedIdentity),
        "podman_writer_state_io_failed",
      );
      const repeated = await handle.readFile();
      const repeatedAfter = await handle.stat({ bigint: true });
      ensure(
        safeFileStat(repeatedAfter, maximumLinks) &&
          sameIdentity(before, repeatedAfter) &&
          repeatedIdentity.size === repeatedAfter.size &&
          repeatedIdentity.mtimeNs === repeatedAfter.mtimeNs &&
          callIntrinsic(bufferEqualsIntrinsic, first, [repeated]),
        "podman_writer_state_io_failed",
      );
      return { bytes: repeated, stat: repeatedAfter };
    }
    return { bytes: first, stat: after };
  } catch (error) {
    if (errorCode(error) === "ENOENT") return null;
    if (isStateError(error)) throw error;
    fail("podman_writer_state_io_failed");
  } finally {
    if (handle) {
      try {
        await handle.close();
      } catch {
        fail("podman_writer_state_io_failed");
      }
    }
  }
}

function readPlainSafeFileHeld(
  root,
  held,
  path,
  maximumLinks = 1n,
) {
  return withHeldDirectoryEntryPath(root, held, path, (lookupPath) =>
    readPlainSafeFile(lookupPath, maximumLinks));
}

async function openCollectionFile(
  root,
  held,
  path,
  maximumLinks,
  faultHooks,
) {
  let handle;
  try {
    const before = await withHeldDirectoryEntryPath(
      root,
      held,
      path,
      (lookupPath) => lstat(lookupPath, { bigint: true }),
    );
    ensure(safeFileStat(before, maximumLinks), "podman_writer_state_io_failed");
    await withHeldDirectoryEntryPath(root, held, path, async (lookupPath) => {
      handle = await open(
        lookupPath,
        fsConstants.O_RDONLY | fsConstants.O_NOFOLLOW,
      );
    });
    const opened = await handle.stat({ bigint: true });
    ensure(
      safeFileStat(opened, maximumLinks) && sameIdentity(before, opened),
      "podman_writer_state_io_failed",
    );
    const bytes = await handle.readFile();
    await runFaultHook(faultHooks, "afterCollectionFileFirstRead");
    const after = await handle.stat({ bigint: true });
    ensure(
      safeFileStat(after, maximumLinks) &&
        sameIdentity(before, after),
      "podman_writer_state_io_failed",
    );
    const repeated = callIntrinsic(bufferAllocIntrinsic, Buffer, [
      bytes.length,
    ]);
    const repeatedRead = await handle.read(
      repeated,
      0,
      repeated.length,
      0,
    );
    const finalHeld = await handle.stat({ bigint: true });
    const current = await readPlainSafeFileHeld(
      root,
      held,
      path,
      maximumLinks,
    );
    ensure(
      current !== null &&
        safeFileStat(finalHeld, maximumLinks) &&
        sameIdentity(before, finalHeld) &&
        sameIdentity(finalHeld, current.stat) &&
        finalHeld.size === BigInt(bytes.length) &&
        current.stat.size === finalHeld.size &&
        repeatedRead.bytesRead === bytes.length &&
        callIntrinsic(bufferEqualsIntrinsic, bytes, [repeated]) &&
        callIntrinsic(bufferEqualsIntrinsic, bytes, [current.bytes]),
      "podman_writer_state_io_failed",
    );
    return { bytes, handle, path, stat: finalHeld };
  } catch (error) {
    if (handle) {
      try {
        await handle.close();
      } catch {
        fail("podman_writer_state_io_failed");
      }
    }
    if (errorCode(error) === "ENOENT") return null;
    if (isStateError(error)) throw error;
    fail("podman_writer_state_io_failed");
  }
}

async function closeCollectionFiles(files) {
  let failed = false;
  for (let index = 0; index < files.length; index += 1) {
    try {
      await files[index].handle.close();
    } catch {
      failed = true;
    }
  }
  return failed;
}

function collectionRecordRaw(artifact, launchAttemptId, revision) {
  if (artifact === null) return null;
  try {
    return parseRecord(
      callIntrinsic(bufferToStringIntrinsic, artifact.bytes, ["utf8"]),
      launchAttemptId,
      revision,
    );
  } catch (error) {
    if (
      isStateError(error) &&
      error.code === "podman_writer_state_invalid"
    ) {
      fail("podman_writer_state_conflict");
    }
    throw error;
  }
}

function validateCollectionChainRecord(record, terminalRecord, revision) {
  ensure(
    record.revision === revision &&
      record.launchAttemptId === terminalRecord.launchAttemptId &&
      record.requestSha256 === terminalRecord.requestSha256 &&
      record.containerName === terminalRecord.containerName,
    "podman_writer_state_conflict",
  );
  if (revision === 0) return;
  ensure(
    record.containerId === terminalRecord.containerId &&
      record.processIncarnationId === terminalRecord.processIncarnationId &&
      record.writerIncarnationId === terminalRecord.writerIncarnationId,
    "podman_writer_state_conflict",
  );
  if (revision >= 2) {
    ensure(
      record.proofId === terminalRecord.proofId,
      "podman_writer_state_conflict",
    );
  }
  if (revision >= 3) {
    ensure(
      record.stopOperationId === terminalRecord.stopOperationId,
      "podman_writer_state_conflict",
    );
  }
}

async function openCollectionRevision(
  root,
  launchAttemptId,
  revision,
  held,
  files,
  faultHooks,
) {
  const path = recordPath(root, launchAttemptId, revision);
  const record = await openCollectionFile(
    root,
    held,
    path,
    2n,
    faultHooks,
  );
  if (record !== null) files[files.length] = record;
  const pending = await openCollectionFile(
    root,
    held,
    pendingRecordPath(path),
    2n,
    faultHooks,
  );
  if (pending !== null) files[files.length] = pending;
  const ready = await openCollectionFile(
    root,
    held,
    `${path}.ready`,
    1n,
    faultHooks,
  );
  if (ready !== null) files[files.length] = ready;

  if (record === null) {
    ensure(
      pending === null && ready === null,
      "podman_writer_state_conflict",
    );
    return { path, pending, ready, record, parsed: null };
  }

  if (pending === null) {
    const current = await record.handle.stat({ bigint: true });
    ensure(
      safeFileStat(current) && sameIdentity(record.stat, current),
      "podman_writer_state_io_failed",
    );
  } else {
    ensure(
      record.stat.nlink === 2n &&
        pending.stat.nlink === 2n &&
        sameIdentity(record.stat, pending.stat) &&
        callIntrinsic(bufferEqualsIntrinsic, record.bytes, [pending.bytes]),
      "podman_writer_state_conflict",
    );
  }
  if (ready !== null) {
    const readyHash = createHashIntrinsic("sha256");
    callIntrinsic(hashUpdateIntrinsic, readyHash, [record.bytes]);
    const expected = `${callIntrinsic(hashDigestIntrinsic, readyHash, ["hex"])}\n`;
    ensure(
      callIntrinsic(bufferToStringIntrinsic, ready.bytes, ["utf8"]) === expected,
      "podman_writer_state_conflict",
    );
  }
  await assertDirectoryHeld(root, held);
  return {
    path,
    pending,
    ready,
    record,
    parsed: collectionRecordRaw(record, launchAttemptId, revision),
  };
}

async function revalidateUnlinkedCollectionArtifact(
  artifact,
  maximumLinkCount,
) {
  // Another collector for the same immutable authorization may remove the
  // recognized sibling alias after this collector's unlink. That transition
  // can only lower nlink. Object identity, exact content, UID, and mode remain
  // fixed here, and the final collection boundary still requires nlink zero
  // for every held artifact.
  const before = await artifact.handle.stat({ bigint: true });
  const bytes = callIntrinsic(bufferAllocIntrinsic, Buffer, [
    artifact.bytes.length,
  ]);
  const readResult = await artifact.handle.read(
    bytes,
    0,
    bytes.length,
    0,
  );
  const after = await artifact.handle.stat({ bigint: true });
  const uid = effectiveUid();
  ensure(
    before.isFile() &&
      !before.isSymbolicLink() &&
      sameIdentity(artifact.stat, before) &&
      sameIdentity(before, after) &&
      before.nlink >= 0n &&
      before.nlink <= maximumLinkCount &&
      after.nlink >= 0n &&
      after.nlink <= before.nlink &&
      uid !== null &&
      before.uid === uid &&
      after.uid === uid &&
      Number(before.mode & 0o7777n) === 0o600 &&
      Number(after.mode & 0o7777n) === 0o600 &&
      before.size === BigInt(artifact.bytes.length) &&
      after.size === before.size &&
      readResult.bytesRead === artifact.bytes.length &&
      callIntrinsic(bufferEqualsIntrinsic, artifact.bytes, [bytes]),
    "podman_writer_state_io_failed",
  );
}

async function unlinkCollectionArtifact(
  root,
  held,
  artifact,
  faultHooks,
) {
  if (artifact === null) return false;
  let removed = false;
  let current = null;
  let disappeared = false;
  try {
    // The Linux clone remains open across exact named-file revalidation, the
    // final race hook, and unlink. `removed` survives a post-bracket or clone
    // close failure so the caller reports an uncertain destructive outcome.
    await withHeldDirectoryEntryPath(
      root,
      held,
      artifact.path,
      async (lookupPath) => {
        current = await readPlainSafeFile(lookupPath, 2n);
        if (current === null) return;
        ensure(
          sameIdentity(current.stat, artifact.stat) &&
            callIntrinsic(bufferEqualsIntrinsic, current.bytes, [artifact.bytes]),
          "podman_writer_state_io_failed",
        );
        await runFaultHook(faultHooks, "beforeCollectionArtifactUnlink");
        try {
          await unlink(lookupPath);
          removed = true;
        } catch (error) {
          if (errorCode(error) === "ENOENT") {
            disappeared = true;
            return;
          }
          throw error;
        }
      },
    );
  } catch (error) {
    if (removed) fail("podman_writer_state_collection_outcome_uncertain");
    throw error;
  }
  if (current === null || disappeared) return false;
  try {
    await runFaultHook(faultHooks, "afterCollectionArtifactUnlink");
    ensure(
      await collectionPathAbsent(root, held, artifact.path),
      "podman_writer_state_io_failed",
    );
    await revalidateUnlinkedCollectionArtifact(
      artifact,
      current.stat.nlink - 1n,
    );
    await assertDirectoryHeld(root, held);
    return true;
  } catch (error) {
    if (removed) fail("podman_writer_state_collection_outcome_uncertain");
    throw error;
  }
}

function collectionReceipt(stateOwnerId, terminalRecord, status) {
  const hash = createHashIntrinsic("sha256");
  callIntrinsic(hashUpdateIntrinsic, hash, [
    "portable-codex-runtime:podman-writer-state-collection:v2\0",
    "utf8",
  ]);
  callIntrinsic(hashUpdateIntrinsic, hash, [stateOwnerId, "utf8"]);
  callIntrinsic(hashUpdateIntrinsic, hash, ["\0", "utf8"]);
  callIntrinsic(hashUpdateIntrinsic, hash, [
    callIntrinsic(jsonStringifyIntrinsic, JsonObject, [terminalRecord]),
    "utf8",
  ]);
  return frozenRecord({
    contractVersion: PODMAN_WRITER_SUPERVISOR_STATE_COLLECTION_CONTRACT_VERSION,
    launchAttemptId: terminalRecord.launchAttemptId,
    stateOwnerId,
    status,
    terminalRecordSha256: callIntrinsic(hashDigestIntrinsic, hash, ["hex"]),
  });
}

async function collectionPathAbsent(root, held, path) {
  try {
    await withHeldDirectoryEntryPath(
      root,
      held,
      path,
      (lookupPath) => lstat(lookupPath, { bigint: true }),
    );
  } catch (error) {
    if (errorCode(error) === "ENOENT") return true;
    throw error;
  }
  return false;
}

async function assertCollectionRevisionAbsent(
  root,
  launchAttemptId,
  revision,
  held,
) {
  const path = recordPath(root, launchAttemptId, revision);
  ensure(
    (await collectionPathAbsent(root, held, path)) &&
      (await collectionPathAbsent(root, held, pendingRecordPath(path))) &&
      (await collectionPathAbsent(root, held, `${path}.ready`)),
    "podman_writer_state_io_failed",
  );
}

async function assertFutureCollectionRevisionsAbsent(
  root,
  launchAttemptId,
  held,
) {
  for (let revision = 5; revision <= 9; revision += 1) {
    const path = recordPath(root, launchAttemptId, revision);
    ensure(
      (await collectionPathAbsent(root, held, path)) &&
        (await collectionPathAbsent(root, held, pendingRecordPath(path))) &&
        (await collectionPathAbsent(root, held, `${path}.ready`)),
      "podman_writer_state_conflict",
    );
  }
}

async function assertCollectionFilesUnlinked(files) {
  for (let index = 0; index < files.length; index += 1) {
    await revalidateUnlinkedCollectionArtifact(files[index], 0n);
  }
}

// Published record content is immutable. A normal publication has one link;
// the only accepted two-link transition is the byte-identical pending alias in
// this owner-private directory. Link-count churn alone is not content mutation,
// but an unrecognized alias is an access-policy expansion and fails closed.
async function cleanupPublishedAlias(path, published, held) {
  const root = pathDirnameIntrinsic(path);
  const currentPublished = await readPlainSafeFileHeld(
    root,
    held,
    path,
    2n,
  );
  ensure(
    currentPublished !== null &&
      sameIdentity(published.stat, currentPublished.stat) &&
      callIntrinsic(bufferEqualsIntrinsic, published.bytes, [currentPublished.bytes]),
    "podman_writer_state_io_failed",
  );
  if (currentPublished.stat.nlink === 1n) return currentPublished;

  const pendingPath = pendingRecordPath(path);
  const currentPending = await readPlainSafeFileHeld(
    root,
    held,
    pendingPath,
    2n,
  );
  if (currentPending === null) {
    const concurrentlyCleaned = await readPlainSafeFileHeld(
      root,
      held,
      path,
    );
    ensure(
      concurrentlyCleaned !== null &&
        sameIdentity(published.stat, concurrentlyCleaned.stat) &&
        callIntrinsic(bufferEqualsIntrinsic, published.bytes, [
          concurrentlyCleaned.bytes,
        ]),
      "podman_writer_state_io_failed",
    );
    return concurrentlyCleaned;
  }
  ensure(
    currentPending.stat.nlink === 2n &&
      sameIdentity(currentPublished.stat, currentPending.stat) &&
      callIntrinsic(bufferEqualsIntrinsic, currentPublished.bytes, [
        currentPending.bytes,
      ]),
    "podman_writer_state_io_failed",
  );
  let concurrentlyCleaned = null;
  try {
    concurrentlyCleaned = await withHeldDirectoryEntryPath(
      root,
      held,
      pendingPath,
      async (lookupPath) => {
        const lookupRoot = pathDirnameIntrinsic(lookupPath);
        const publishedLookupPath =
          `${lookupRoot}/${pathBasenameIntrinsic(path)}`;
        const bracketPublished = await readPlainSafeFile(
          publishedLookupPath,
          2n,
        );
        const bracketPending = await readPlainSafeFile(lookupPath, 2n);
        if (bracketPending === null) {
          const cleanedPublished = await readPlainSafeFile(
            publishedLookupPath,
          );
          ensure(
            cleanedPublished !== null &&
              cleanedPublished.stat.nlink === 1n &&
              sameIdentity(currentPublished.stat, cleanedPublished.stat) &&
              callIntrinsic(bufferEqualsIntrinsic, currentPublished.bytes, [
                cleanedPublished.bytes,
              ]),
            "podman_writer_state_io_failed",
          );
          return cleanedPublished;
        }
        ensure(
          bracketPublished !== null &&
            bracketPublished.stat.nlink === 2n &&
            bracketPending.stat.nlink === 2n &&
            sameIdentity(currentPublished.stat, bracketPublished.stat) &&
            sameIdentity(currentPending.stat, bracketPending.stat) &&
            sameIdentity(bracketPublished.stat, bracketPending.stat) &&
            callIntrinsic(bufferEqualsIntrinsic, currentPublished.bytes, [
              bracketPublished.bytes,
            ]) &&
            callIntrinsic(bufferEqualsIntrinsic, currentPending.bytes, [
              bracketPending.bytes,
            ]),
          "podman_writer_state_io_failed",
        );
        await unlink(lookupPath);
        return null;
      },
    );
  } catch (error) {
    if (errorCode(error) !== "ENOENT") fail("podman_writer_state_io_failed");
  }
  if (concurrentlyCleaned !== null) return concurrentlyCleaned;
  try {
    await held.handle.sync();
  } catch {
    fail("podman_writer_state_io_failed");
  }
  const cleaned = await readPlainSafeFileHeld(root, held, path);
  ensure(
    cleaned !== null &&
      sameIdentity(published.stat, cleaned.stat) &&
      callIntrinsic(bufferEqualsIntrinsic, published.bytes, [cleaned.bytes]),
    "podman_writer_state_io_failed",
  );
  return cleaned;
}

async function readPublishedRecordFile(path, held) {
  const root = pathDirnameIntrinsic(path);
  const published = await readPlainSafeFileHeld(root, held, path, 2n);
  if (published === null || published.stat.nlink === 1n) return published;

  const pending = await readPlainSafeFileHeld(
    root,
    held,
    pendingRecordPath(path),
    2n,
  );
  if (
    pending !== null &&
    sameIdentity(published.stat, pending.stat) &&
    callIntrinsic(bufferEqualsIntrinsic, published.bytes, [pending.bytes])
  ) {
    const revalidated = await readPlainSafeFileHeld(root, held, path, 2n);
    ensure(
      revalidated !== null &&
        sameIdentity(published.stat, revalidated.stat) &&
        callIntrinsic(bufferEqualsIntrinsic, published.bytes, [revalidated.bytes]),
      "podman_writer_state_io_failed",
    );
    if (revalidated.stat.nlink === 1n) return revalidated;
    ensure(pending.stat.nlink === 2n, "podman_writer_state_io_failed");
    return cleanupPublishedAlias(path, revalidated, held);
  }

  // The publisher may have removed the pending alias between observations.
  const revalidated = await readPlainSafeFileHeld(root, held, path, 2n);
  ensure(
    revalidated !== null &&
      revalidated.stat.nlink === 1n &&
      sameIdentity(published.stat, revalidated.stat) &&
      callIntrinsic(bufferEqualsIntrinsic, published.bytes, [revalidated.bytes]),
    "podman_writer_state_io_failed",
  );
  return revalidated;
}

async function readImmutableFile(path, held) {
  const published = await readPublishedRecordFile(path, held);
  const ready = await readPlainSafeFile(`${path}.ready`);
  if (published === null) {
    ensure(ready === null, "podman_writer_state_io_failed");
    return null;
  }
  if (ready !== null) {
    const readyHash = createHashIntrinsic("sha256");
    callIntrinsic(hashUpdateIntrinsic, readyHash, [published.bytes]);
    const expected = `${callIntrinsic(hashDigestIntrinsic, readyHash, ["hex"])}\n`;
    ensure(
      callIntrinsic(bufferToStringIntrinsic, ready.bytes, ["utf8"]) === expected,
      "podman_writer_state_io_failed",
    );
  }
  return callIntrinsic(bufferToStringIntrinsic, published.bytes, ["utf8"]);
}

function parseRecord(raw, launchAttemptId, revision) {
  ensure(
    typeof raw === "string" &&
      callIntrinsic(stringEndsWithIntrinsic, raw, ["\n"]) &&
      callIntrinsic(stringIndexOfIntrinsic, raw, ["\n"]) === raw.length - 1,
  );
  let parsed;
  try {
    parsed = callIntrinsic(jsonParseIntrinsic, JsonObject, [raw]);
  } catch {
    fail("podman_writer_state_invalid");
  }
  const record = normalizeRecord(parsed);
  ensure(
    record.launchAttemptId === launchAttemptId && record.revision === revision,
  );
  ensure(
    raw === `${callIntrinsic(jsonStringifyIntrinsic, JsonObject, [record])}\n`,
  );
  return record;
}

async function readCurrent(root, launchAttemptId, held) {
  let current = null;
  let gap = false;
  for (let revision = 0; revision <= 5; revision += 1) {
    const raw = await readImmutableFile(
      recordPath(root, launchAttemptId, revision),
      held,
    );
    if (raw === null) {
      gap = true;
      continue;
    }
    ensure(!gap && revision <= 4, "podman_writer_state_invalid");
    const record = parseRecord(raw, launchAttemptId, revision);
    if (current !== null) {
      ensure(
        record.requestSha256 === current.requestSha256 &&
          record.containerName === current.containerName &&
          record.launchAttemptId === current.launchAttemptId &&
          isNextStatus(current.status, record.status),
      );
      if (record.status !== "created") {
        ensure(
          record.containerId === current.containerId &&
            record.processIncarnationId === current.processIncarnationId &&
            record.writerIncarnationId === current.writerIncarnationId,
        );
      }
      if (record.status === "stopping") {
        ensure(record.proofId === current.proofId);
      }
      if (record.status === "stopped") {
        ensure(
          record.proofId === current.proofId &&
            record.stopOperationId === current.stopOperationId,
        );
      }
    }
    current = record;
  }
  await assertDirectoryHeld(root, held);
  return current;
}

function normalizeFaultHooks(value) {
  if (value === undefined) return frozenRecord({});
  const hooks = optionalDataObject(value, FAULT_HOOK_KEYS, []);
  const keys = reflectOwnKeysIntrinsic(hooks);
  for (let index = 0; index < keys.length; index += 1) {
    const hook = hooks[keys[index]];
    ensure(typeof hook === "function" && !isProxy(hook));
  }
  return frozenRecord(hooks);
}

function isNativePromise(value) {
  if (!isPromise(value) || isProxy(value)) return false;
  try {
    return (
      callIntrinsic(objectGetPrototypeOfIntrinsic, Object, [value]) ===
        promisePrototype &&
      callIntrinsic(objectGetOwnPropertyDescriptorIntrinsic, Object, [
        value,
        "catch",
      ]) === undefined &&
      callIntrinsic(objectGetOwnPropertyDescriptorIntrinsic, Object, [
        value,
        "constructor",
      ]) === undefined &&
      callIntrinsic(objectGetOwnPropertyDescriptorIntrinsic, Object, [
        value,
        "finally",
      ]) === undefined &&
      callIntrinsic(objectGetOwnPropertyDescriptorIntrinsic, Object, [
        value,
        "then",
      ]) === undefined
    );
  } catch {
    return false;
  }
}

async function runFaultHook(faultHooks, name) {
  if (!objectHasOwnIntrinsic(faultHooks, name)) return;
  const result = callIntrinsic(faultHooks[name], undefined, []);
  if (result === undefined) return;
  ensure(isNativePromise(result), "podman_writer_state_io_failed");
  await result;
}

// The protected property is publication content stability: before the final
// no-replace link exists, readers ignore the pending precursor and retain the
// old revision; after it exists, the final name always refers to a fully written
// and fsynced record. Directory fsync makes that publication durable. A retry
// may adopt only the exact canonical precursor bytes; partial or mismatched
// precursors fail closed.
async function writeExclusive(root, record, held, faultHooks) {
  const path = recordPath(root, record.launchAttemptId, record.revision);
  const pendingPath = pendingRecordPath(path);
  const bytes = callIntrinsic(bufferFromIntrinsic, Buffer, [
    `${callIntrinsic(jsonStringifyIntrinsic, JsonObject, [record])}\n`,
    "utf8",
  ]);
  ensure(bytes.length <= MAX_RECORD_BYTES);

  const existing = await readImmutableFile(path, held);
  if (existing !== null) return false;

  let handle;
  try {
    let createdPending = false;
    try {
      handle = await open(
        pendingPath,
        fsConstants.O_RDWR |
          fsConstants.O_CREAT |
          fsConstants.O_EXCL |
          fsConstants.O_NOFOLLOW,
        0o600,
      );
      createdPending = true;
    } catch (error) {
      if (errorCode(error) !== "EEXIST") throw error;
      const before = await lstat(pendingPath, { bigint: true });
      ensure(
        safeFileStat(before, 2n),
        "podman_writer_state_io_failed",
      );
      handle = await open(
        pendingPath,
        fsConstants.O_RDWR | fsConstants.O_NOFOLLOW,
      );
      const opened = await handle.stat({ bigint: true });
      ensure(
        safeFileStat(opened, 2n) && sameIdentity(before, opened),
        "podman_writer_state_io_failed",
      );
      const precursor = await handle.readFile();
      const afterRead = await handle.stat({ bigint: true });
      ensure(
        safeFileStat(afterRead, 2n) &&
          sameIdentity(before, afterRead) &&
          before.size === afterRead.size &&
          before.mtimeNs === afterRead.mtimeNs &&
          callIntrinsic(bufferEqualsIntrinsic, precursor, [bytes]),
        "podman_writer_state_io_failed",
      );
      if (afterRead.nlink === 2n) {
        const concurrent = await readImmutableFile(path, held);
        ensure(concurrent !== null, "podman_writer_state_io_failed");
        return false;
      }
    }

    if (createdPending) {
      await handle.writeFile(bytes);
      await runFaultHook(faultHooks, "afterTemporaryWrite");
      await handle.chmod(0o600);
    }
    await handle.sync();
    await runFaultHook(faultHooks, "afterTemporarySync");

    const pendingStat = await handle.stat({ bigint: true });
    const namedPendingStat = await lstat(pendingPath, { bigint: true });
    ensure(
      safeFileStat(pendingStat) &&
        safeFileStat(namedPendingStat) &&
        sameIdentity(pendingStat, namedPendingStat) &&
        pendingStat.size === BigInt(bytes.length),
      "podman_writer_state_io_failed",
    );
    await assertDirectoryHeld(root, held);

    try {
      await link(pendingPath, path);
    } catch (error) {
      if (errorCode(error) !== "EEXIST" && errorCode(error) !== "ENOENT") {
        throw error;
      }
      const concurrent = await readImmutableFile(path, held);
      ensure(concurrent !== null, "podman_writer_state_io_failed");
      return false;
    }
    await runFaultHook(faultHooks, "afterPublish");

    // A concurrent reader may already have revalidated and durably removed the
    // pending alias. That benign nlink 2 -> 1 transition keeps this publisher as
    // the winner; byte and inode identity still have to match exactly.
    const published = await readPlainSafeFile(path, 2n);
    ensure(
      published !== null &&
        sameIdentity(pendingStat, published.stat) &&
        callIntrinsic(bufferEqualsIntrinsic, bytes, [published.bytes]),
      "podman_writer_state_io_failed",
    );
    const publishedStat = published.stat;

    await held.handle.sync();
    await runFaultHook(faultHooks, "afterPublishDirectorySync");

    await withHeldDirectoryEntryPath(
      root,
      held,
      pendingPath,
      async (lookupPath) => {
        let currentPendingStat;
        try {
          currentPendingStat = await lstat(lookupPath, { bigint: true });
        } catch (error) {
          if (errorCode(error) !== "ENOENT") throw error;
          return;
        }
        ensure(
          safeFileStat(currentPendingStat, 2n) &&
            currentPendingStat.nlink === 2n &&
            sameIdentity(publishedStat, currentPendingStat),
          "podman_writer_state_io_failed",
        );
        try {
          await unlink(lookupPath);
        } catch (error) {
          if (errorCode(error) !== "ENOENT") throw error;
        }
      },
    );
    const cleanedStat = await handle.stat({ bigint: true });
    const namedPublishedStat = await withHeldDirectoryEntryPath(
      root,
      held,
      path,
      (lookupPath) => lstat(lookupPath, { bigint: true }),
    );
    ensure(
      safeFileStat(cleanedStat) &&
        safeFileStat(namedPublishedStat) &&
        sameIdentity(cleanedStat, namedPublishedStat),
      "podman_writer_state_io_failed",
    );
    await runFaultHook(faultHooks, "afterCleanup");

    await held.handle.sync();
    await runFaultHook(faultHooks, "afterCleanupDirectorySync");
  } catch (error) {
    if (isStateError(error)) throw error;
    fail("podman_writer_state_io_failed");
  } finally {
    if (handle) {
      try {
        await handle.close();
      } catch {
        fail("podman_writer_state_io_failed");
      }
    }
  }
  return true;
}

export function isPodmanWriterSupervisorState(value) {
  return (
    value !== null &&
    typeof value === "object" &&
    !isProxy(value) &&
    callIntrinsic(weakSetHasIntrinsic, stateBrands, [value])
  );
}

function validStateRoot(value) {
  if (
    typeof value !== "string" ||
    value.length <= 1 ||
    value.length > MAX_STATE_ROOT_BYTES ||
    callIntrinsic(stringIncludesIntrinsic, value, ["\0"])
  ) {
    return false;
  }
  const encoded = callIntrinsic(bufferFromIntrinsic, Buffer, [value, "utf8"]);
  return (
    encoded.length <= MAX_STATE_ROOT_BYTES &&
    callIntrinsic(bufferToStringIntrinsic, encoded, ["utf8"]) === value &&
    pathIsAbsoluteIntrinsic(value) &&
    pathResolveIntrinsic(value) === value
  );
}

export async function preparePodmanWriterSupervisorStateOwner(inputValue) {
  ensure(arguments.length === 1);
  const input = exactDataObject(inputValue, STATE_OWNER_PREPARATION_KEYS);
  ensure(validStateRoot(input.root));
  ensure(
    input.expectedStateOwnerId === null ||
      (typeof input.expectedStateOwnerId === "string" &&
        regexpTest(STATE_OWNER_ID_PATTERN, input.expectedStateOwnerId)),
  );
  const root = input.root;
  let held = null;
  let marker = null;
  let primaryError = null;
  let owner = null;
  try {
    let prepared = await openPreparedStateOwnerRoot(root);
    if (prepared === null) {
      ensure(
        input.expectedStateOwnerId === null,
        "podman_writer_state_io_failed",
      );
      prepared = await createAndPublishStateOwnerRoot(root);
    }
    held = prepared.held;
    marker = prepared.marker;
    if (input.expectedStateOwnerId !== null) {
      ensure(
        marker.stateOwnerId === input.expectedStateOwnerId,
        "podman_writer_state_io_failed",
      );
    }
    // Repeat all three durability barriers when adopting an existing marker.
    // A previous initialization may have completed but lost any one fsync
    // acknowledgement before returning its capability.
    await marker.handle.sync();
    await held.handle.sync();
    await held.parentHandle.sync();
    await assertStateOwnerMarkerHeld(root, held, marker);
    const binding = stateOwnerBinding(root, held, marker);
    owner = frozenRecord({
      contractVersion: PODMAN_WRITER_SUPERVISOR_STATE_OWNER_CONTRACT_VERSION,
      stateOwnerId: binding.stateOwnerId,
    });
    callIntrinsic(weakSetAddIntrinsic, stateOwnerBrands, [owner]);
    callIntrinsic(weakMapSetIntrinsic, stateOwnerBindings, [owner, binding]);
  } catch (error) {
    primaryError = error;
  }
  if (marker !== null) {
    try {
      await closeStateOwnerMarker(marker);
    } catch (error) {
      primaryError ??= error;
    }
  }
  if (held !== null) {
    try {
      await closeHeldDirectory(held);
    } catch (error) {
      primaryError ??= error;
    }
  }
  if (primaryError !== null) {
    if (isStateError(primaryError)) throw primaryError;
    fail("podman_writer_state_io_failed");
  }
  ensure(owner !== null, "podman_writer_state_io_failed");
  return owner;
}

export function isPodmanWriterSupervisorStateOwner(value) {
  return (
    value !== null &&
    typeof value === "object" &&
    !isProxy(value) &&
    callIntrinsic(weakSetHasIntrinsic, stateOwnerBrands, [value])
  );
}

function createPodmanWriterSupervisorStateInternal(root, faultHooks, ownerBinding) {
  const operationTails = new MapConstructor();

  async function withAttemptLock(launchAttemptId, callback) {
    const previous =
      callIntrinsic(mapGetIntrinsic, operationTails, [launchAttemptId]) ??
      callIntrinsic(promiseResolveIntrinsic, PromiseConstructor, []);
    let release;
    const gate = new PromiseConstructor((resolveGate) => {
      release = resolveGate;
    });
    callIntrinsic(mapSetIntrinsic, operationTails, [launchAttemptId, gate]);
    await previous;
    try {
      return await callback();
    } finally {
      release();
      if (callIntrinsic(mapGetIntrinsic, operationTails, [launchAttemptId]) === gate) {
        callIntrinsic(mapDeleteIntrinsic, operationTails, [launchAttemptId]);
      }
    }
  }

  const read = frozenFunction(async function read(inputValue) {
    ensure(arguments.length === 1);
    const input = exactDataObject(inputValue, READ_KEYS);
    const launchAttemptId = assertOpaqueId(input.launchAttemptId);
    return withAttemptLock(launchAttemptId, () =>
      withStateDirectory(
        root,
        false,
        faultHooks,
        ownerBinding,
        async (held) => {
          if (held === null) return null;
          return await readCurrent(root, launchAttemptId, held);
        },
      ));
  });

  const claim = frozenFunction(async function claim(inputValue) {
    ensure(arguments.length === 1);
    const input = exactDataObject(inputValue, CLAIM_KEYS);
    const record = normalizeRecord(input.record);
    ensure(record.status === "preparing" && record.revision === 0);
    return withAttemptLock(record.launchAttemptId, () =>
      withStateDirectory(
        root,
        true,
        faultHooks,
        ownerBinding,
        async (held) => {
          ensure(held !== null, "podman_writer_state_io_failed");
          const created = await writeExclusive(root, record, held, faultHooks);
          const current = await readCurrent(root, record.launchAttemptId, held);
          ensure(current !== null, "podman_writer_state_io_failed");
          return frozenRecord({ created, record: current });
        },
      ));
  });

  const transition = frozenFunction(async function transition(inputValue) {
    ensure(arguments.length === 1);
    const input = exactDataObject(inputValue, TRANSITION_KEYS);
    ensure(
      numberIsSafeIntegerIntrinsic(input.expectedRevision) &&
        input.expectedRevision >= 0 &&
        input.expectedRevision < 4 &&
        typeof input.expectedStatus === "string" &&
        objectHasOwnIntrinsic(NEXT_STATUS, input.expectedStatus),
    );
    const record = normalizeRecord(input.record);
    ensure(
      record.revision === input.expectedRevision + 1 &&
        isNextStatus(input.expectedStatus, record.status),
    );
    return withAttemptLock(record.launchAttemptId, () =>
      withStateDirectory(
        root,
        false,
        faultHooks,
        ownerBinding,
        async (held) => {
          ensure(held !== null, "podman_writer_state_conflict");
          const current = await readCurrent(root, record.launchAttemptId, held);
          if (
            current === null ||
            current.revision !== input.expectedRevision ||
            current.status !== input.expectedStatus
          ) {
            if (current !== null && sameRecord(current, record)) return current;
            fail("podman_writer_state_conflict");
          }
          const created = await writeExclusive(root, record, held, faultHooks);
          const after = await readCurrent(root, record.launchAttemptId, held);
          ensure(after !== null, "podman_writer_state_io_failed");
          if (!created && !sameRecord(after, record)) {
            fail("podman_writer_state_conflict");
          }
          ensure(sameRecord(after, record), "podman_writer_state_conflict");
          return after;
        },
      ));
  });

  const collect = frozenFunction(async function collect(inputValue) {
    ensure(arguments.length === 1);
    const input = exactDataObject(inputValue, COLLECTION_KEYS);
    ensure(
      ownerBinding !== null &&
        input.stateOwnerId === ownerBinding.stateOwnerId,
    );
    const stateOwnerId = ownerBinding.stateOwnerId;
    const terminalRecord = normalizeRecord(input.terminalRecord);
    ensure(
      terminalRecord.status === "stopped" && terminalRecord.revision === 4,
    );
    return withAttemptLock(terminalRecord.launchAttemptId, async () => {
      const files = [];
      let held = null;
      let marker = null;
      let mutationStarted = false;
      let primaryError = null;
      let result = null;
      try {
        held = await heldPrivateDirectory(
          root,
          { create: false },
          faultHooks,
        );
        ensure(held !== null, "podman_writer_state_io_failed");
        assertStateOwnerRootBaseline(held, ownerBinding);
        marker = await openStateOwnerMarker(root, held, ownerBinding);
        await assertFutureCollectionRevisionsAbsent(
          root,
          terminalRecord.launchAttemptId,
          held,
        );
        const revisions = [];
        for (let revision = 0; revision <= 4; revision += 1) {
          revisions[revision] = await openCollectionRevision(
            root,
            terminalRecord.launchAttemptId,
            revision,
            held,
            files,
            faultHooks,
          );
        }
        const terminal = revisions[4];
        if (terminal.record === null) {
          for (let revision = 0; revision < 4; revision += 1) {
            ensure(
              revisions[revision].record === null,
              "podman_writer_state_conflict",
            );
          }
          await assertDirectoryHeld(root, held);
          await held.handle.sync();
          await runFaultHook(
            faultHooks,
            "afterCollectionFinalDirectorySync",
          );
          for (let revision = 0; revision <= 9; revision += 1) {
            await assertCollectionRevisionAbsent(
              root,
              terminalRecord.launchAttemptId,
              revision,
              held,
            );
          }
          await assertDirectoryHeld(root, held);
          result = collectionReceipt(stateOwnerId, terminalRecord, "absent");
        } else {
          ensure(
            sameRecord(terminal.parsed, terminalRecord),
            "podman_writer_state_conflict",
          );
          let lowerRecordObserved = false;
          let partialPrefixObserved = false;
          for (let revision = 0; revision < 4; revision += 1) {
            const current = revisions[revision].parsed;
            if (current === null) {
              partialPrefixObserved = true;
              ensure(
                !lowerRecordObserved,
                "podman_writer_state_conflict",
              );
            } else {
              lowerRecordObserved = true;
              validateCollectionChainRecord(
                current,
                terminalRecord,
                revision,
              );
            }
          }
          mutationStarted = partialPrefixObserved;

          // Phase A preserves revision 4 as the durable terminal anchor. A
          // missing lower prefix is an allowed retry shape because lower
          // revisions are removed oldest-first. All sidecars were validated
          // before the first unlink, so no mismatched alias can be hidden by
          // the collection itself.
          for (let revision = 0; revision < 4; revision += 1) {
            const current = revisions[revision];
            mutationStarted =
              (await unlinkCollectionArtifact(
                root,
                held,
                current.ready,
                faultHooks,
              )) ||
              mutationStarted;
            mutationStarted =
              (await unlinkCollectionArtifact(
                root,
                held,
                current.pending,
                faultHooks,
              )) ||
              mutationStarted;
            mutationStarted =
              (await unlinkCollectionArtifact(
                root,
                held,
                current.record,
                faultHooks,
              )) ||
              mutationStarted;
          }
          mutationStarted =
            (await unlinkCollectionArtifact(
              root,
              held,
              terminal.ready,
              faultHooks,
            )) ||
            mutationStarted;
          mutationStarted =
            (await unlinkCollectionArtifact(
              root,
              held,
              terminal.pending,
              faultHooks,
            )) ||
            mutationStarted;

          for (let revision = 0; revision < 4; revision += 1) {
            await assertCollectionRevisionAbsent(
              root,
              terminalRecord.launchAttemptId,
              revision,
              held,
            );
          }
          await assertDirectoryHeld(root, held);
          await held.handle.sync();
          await runFaultHook(
            faultHooks,
            "afterCollectionFirstDirectorySync",
          );
          await assertDirectoryHeld(root, held);

          let terminalPathStat;
          try {
            terminalPathStat = await withHeldDirectoryEntryPath(
              root,
              held,
              terminal.path,
              (lookupPath) => lstat(lookupPath, { bigint: true }),
            );
          } catch (error) {
            if (errorCode(error) !== "ENOENT") throw error;
            terminalPathStat = null;
          }
          if (terminalPathStat !== null) {
            const terminalHandleStat = await terminal.record.handle.stat({
              bigint: true,
            });
            const terminalCurrent = await readPlainSafeFileHeld(
              root,
              held,
              terminal.path,
            );
            ensure(
              terminalCurrent !== null &&
                safeFileStat(terminalPathStat) &&
                safeFileStat(terminalHandleStat) &&
                sameIdentity(terminal.record.stat, terminalPathStat) &&
                sameIdentity(terminal.record.stat, terminalHandleStat) &&
                sameIdentity(terminal.record.stat, terminalCurrent.stat) &&
                callIntrinsic(bufferEqualsIntrinsic, terminal.record.bytes, [
                  terminalCurrent.bytes,
                ]),
              "podman_writer_state_io_failed",
            );
            await runFaultHook(
              faultHooks,
              "afterCollectionTerminalRevalidation",
            );
            const removedTerminal = await unlinkCollectionArtifact(
              root,
              held,
              terminal.record,
              faultHooks,
            );
            mutationStarted = removedTerminal || mutationStarted;
            if (removedTerminal) {
              await runFaultHook(
                faultHooks,
                "afterCollectionTerminalUnlink",
              );
            }
          }

          for (let revision = 0; revision <= 9; revision += 1) {
            await assertCollectionRevisionAbsent(
              root,
              terminalRecord.launchAttemptId,
              revision,
              held,
            );
          }
          await assertCollectionFilesUnlinked(files);
          await assertDirectoryHeld(root, held);
          await held.handle.sync();
          await runFaultHook(
            faultHooks,
            "afterCollectionFinalDirectorySync",
          );
          await assertDirectoryHeld(root, held);
          result = collectionReceipt(stateOwnerId, terminalRecord, "collected");
        }
      } catch (error) {
        primaryError = error;
        if (
          isStateError(error) &&
          error.code === "podman_writer_state_collection_outcome_uncertain"
        ) {
          mutationStarted = true;
        }
      }

      if (await closeCollectionFiles(files)) {
        primaryError ??= new PodmanWriterSupervisorStateError(
          "podman_writer_state_io_failed",
        );
      }
      if (marker !== null) {
        try {
          await assertStateOwnerMarkerHeld(root, held, marker, ownerBinding);
        } catch (error) {
          primaryError = error;
        }
        try {
          await closeStateOwnerMarker(marker);
        } catch (error) {
          primaryError ??= error;
        }
      }
      if (held !== null) {
        try {
          await closeHeldDirectory(held);
        } catch (error) {
          primaryError ??= error;
        }
      }
      if (primaryError !== null) {
        if (mutationStarted) {
          fail("podman_writer_state_collection_outcome_uncertain");
        }
        if (isStateError(primaryError)) throw primaryError;
        fail("podman_writer_state_io_failed");
      }
      ensure(result !== null, "podman_writer_state_io_failed");
      return result;
    });
  });

  const state = frozenRecord({
    claim,
    contractVersion: PODMAN_WRITER_SUPERVISOR_STATE_CONTRACT_VERSION,
    read,
    transition,
  });
  callIntrinsic(weakSetAddIntrinsic, stateBrands, [state]);
  if (ownerBinding === null) return frozenRecord({ state });
  const stateOwnerId = ownerBinding.stateOwnerId;
  const terminalCollector = frozenRecord({
    collect,
    contractVersion:
      PODMAN_WRITER_SUPERVISOR_STATE_COLLECTION_CONTRACT_VERSION,
    stateOwnerId,
  });
  callIntrinsic(weakSetAddIntrinsic, stateCollectorBrands, [terminalCollector]);
  const bundle = frozenRecord({ state, stateOwnerId, terminalCollector });
  callIntrinsic(weakSetAddIntrinsic, stateBundleBrands, [bundle]);
  return bundle;
}

export function createPodmanWriterSupervisorState(...args) {
  ensure(args.length === 1);
  const options = optionalDataObject(
    args[0],
    LEGACY_OPTION_KEYS,
    LEGACY_REQUIRED_OPTION_KEYS,
  );
  ensure(validStateRoot(options.root));
  return createPodmanWriterSupervisorStateInternal(
    options.root,
    normalizeFaultHooks(options.faultHooks),
    null,
  ).state;
}

export function createPodmanWriterSupervisorStateBundle(...args) {
  ensure(args.length === 1);
  const options = optionalDataObject(
    args[0],
    BUNDLE_OPTION_KEYS,
    BUNDLE_REQUIRED_OPTION_KEYS,
  );
  ensure(isPodmanWriterSupervisorStateOwner(options.owner));
  const ownerBinding = callIntrinsic(weakMapGetIntrinsic, stateOwnerBindings, [
    options.owner,
  ]);
  ensure(ownerBinding !== undefined, "podman_writer_state_io_failed");
  return createPodmanWriterSupervisorStateInternal(
    ownerBinding.root,
    normalizeFaultHooks(options.faultHooks),
    ownerBinding,
  );
}

export function isPodmanWriterSupervisorStateBundle(value) {
  return (
    value !== null &&
    typeof value === "object" &&
    !isProxy(value) &&
    callIntrinsic(weakSetHasIntrinsic, stateBundleBrands, [value])
  );
}

export function isPodmanWriterSupervisorStateTerminalCollector(value) {
  return (
    value !== null &&
    typeof value === "object" &&
    !isProxy(value) &&
    callIntrinsic(weakSetHasIntrinsic, stateCollectorBrands, [value])
  );
}

callIntrinsic(objectFreezeIntrinsic, Object, [PodmanWriterSupervisorStateError.prototype]);
callIntrinsic(objectFreezeIntrinsic, Object, [PodmanWriterSupervisorStateError]);
callIntrinsic(objectFreezeIntrinsic, Object, [assertPodmanWriterSupervisorStateRecord]);
callIntrinsic(objectFreezeIntrinsic, Object, [createPodmanWriterSupervisorState]);
callIntrinsic(objectFreezeIntrinsic, Object, [createPodmanWriterSupervisorStateBundle]);
callIntrinsic(objectFreezeIntrinsic, Object, [preparePodmanWriterSupervisorStateOwner]);
callIntrinsic(objectFreezeIntrinsic, Object, [isPodmanWriterSupervisorStateBundle]);
callIntrinsic(objectFreezeIntrinsic, Object, [isPodmanWriterSupervisorState]);
callIntrinsic(objectFreezeIntrinsic, Object, [isPodmanWriterSupervisorStateOwner]);
callIntrinsic(objectFreezeIntrinsic, Object, [isPodmanWriterSupervisorStateTerminalCollector]);
