import { Buffer } from "node:buffer";
import { Hash, createHash } from "node:crypto";
import { constants as fsConstants } from "node:fs";
import {
  chmod,
  link,
  lstat,
  mkdir,
  open,
  unlink,
} from "node:fs/promises";
import { dirname, isAbsolute, resolve } from "node:path";
import { types as utilTypes } from "node:util";

const { isPromise, isProxy } = utilTypes;
const arrayIncludesIntrinsic = Array.prototype.includes;
const arrayIsArrayIntrinsic = Array.isArray;
const arrayEveryIntrinsic = Array.prototype.every;
const bufferEqualsIntrinsic = Buffer.prototype.equals;
const bufferFromIntrinsic = Buffer.from;
const bufferToStringIntrinsic = Buffer.prototype.toString;
const createHashIntrinsic = createHash;
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

const MAX_RECORD_BYTES = 16 * 1024;
const MAX_NATIVE_PATH_BYTES = 4_095;
// `/<64-hex>.<revision>.json.pending` is the longest derived suffix. The
// contract's revisions are single decimal digits, so it consumes 80 bytes.
const MAX_STATE_ROOT_BYTES = MAX_NATIVE_PATH_BYTES - 80;
const OPAQUE_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u;
const SHA256_PATTERN = /^[0-9a-f]{64}$/u;
const CONTAINER_NAME_PATTERN = /^[a-z0-9][a-z0-9_.-]{0,127}$/u;
const CONTAINER_ID_PATTERN = /^[0-9a-f]{12,64}$/u;
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
const OPTION_KEYS = Object.freeze(["faultHooks", "root"]);
const REQUIRED_OPTION_KEYS = Object.freeze(["root"]);
const FAULT_HOOK_KEYS = Object.freeze([
  "afterCleanup",
  "afterCleanupDirectorySync",
  "afterParentDirectorySync",
  "afterPublish",
  "afterPublishDirectorySync",
  "afterRootDirectorySync",
  "afterTemporarySync",
  "afterTemporaryWrite",
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
});

const stateBrands = new WeakSet();
const stateErrorBrands = new WeakSet();

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

async function heldPrivateDirectory(root, { create }, faultHooks) {
  let handle;
  let parent;
  try {
    parent = await openPrivateParent(root);
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
    const parentHeld = await parent.handle.stat({ bigint: true });
    const parentCurrent = await validateParentChain(parent.path);
    ensure(
      safeDirectoryStat(parentHeld) &&
        sameIdentity(parent.identity, parentHeld) &&
        sameIdentity(parent.identity, parentCurrent),
      "podman_writer_state_io_failed",
    );
    const heldDirectory = callIntrinsic(objectFreezeIntrinsic, Object, [
      {
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

// Published record content is immutable. A normal publication has one link;
// the only accepted two-link transition is the byte-identical pending alias in
// this owner-private directory. Link-count churn alone is not content mutation,
// but an unrecognized alias is an access-policy expansion and fails closed.
async function cleanupPublishedAlias(path, published, held) {
  await assertDirectoryHeld(pathDirnameIntrinsic(path), held);
  const currentPublished = await readPlainSafeFile(path, 2n);
  ensure(
    currentPublished !== null &&
      sameIdentity(published.stat, currentPublished.stat) &&
      callIntrinsic(bufferEqualsIntrinsic, published.bytes, [currentPublished.bytes]),
    "podman_writer_state_io_failed",
  );
  if (currentPublished.stat.nlink === 1n) return currentPublished;

  const pendingPath = pendingRecordPath(path);
  const currentPending = await readPlainSafeFile(pendingPath, 2n);
  if (currentPending === null) {
    const concurrentlyCleaned = await readPlainSafeFile(path);
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
  try {
    await unlink(pendingPath);
  } catch (error) {
    if (errorCode(error) !== "ENOENT") fail("podman_writer_state_io_failed");
  }
  try {
    await held.handle.sync();
  } catch {
    fail("podman_writer_state_io_failed");
  }
  const cleaned = await readPlainSafeFile(path);
  ensure(
    cleaned !== null &&
      sameIdentity(published.stat, cleaned.stat) &&
      callIntrinsic(bufferEqualsIntrinsic, published.bytes, [cleaned.bytes]),
    "podman_writer_state_io_failed",
  );
  return cleaned;
}

async function readPublishedRecordFile(path, held) {
  const published = await readPlainSafeFile(path, 2n);
  if (published === null || published.stat.nlink === 1n) return published;

  const pending = await readPlainSafeFile(pendingRecordPath(path), 2n);
  if (
    pending !== null &&
    sameIdentity(published.stat, pending.stat) &&
    callIntrinsic(bufferEqualsIntrinsic, published.bytes, [pending.bytes])
  ) {
    const revalidated = await readPlainSafeFile(path, 2n);
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
  const revalidated = await readPlainSafeFile(path, 2n);
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

    let pendingAlreadyCleaned = false;
    try {
      const currentPendingStat = await lstat(pendingPath, { bigint: true });
      ensure(
        safeFileStat(currentPendingStat, 2n) &&
          currentPendingStat.nlink === 2n &&
          sameIdentity(publishedStat, currentPendingStat),
        "podman_writer_state_io_failed",
      );
    } catch (error) {
      if (errorCode(error) !== "ENOENT") throw error;
      pendingAlreadyCleaned = true;
    }
    if (!pendingAlreadyCleaned) {
      try {
        await unlink(pendingPath);
      } catch (error) {
        if (errorCode(error) !== "ENOENT") throw error;
      }
    }
    const cleanedStat = await handle.stat({ bigint: true });
    const namedPublishedStat = await lstat(path, { bigint: true });
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

export function createPodmanWriterSupervisorState(...args) {
  ensure(args.length === 1);
  const options = optionalDataObject(
    args[0],
    OPTION_KEYS,
    REQUIRED_OPTION_KEYS,
  );
  ensure(validStateRoot(options.root));
  const root = options.root;
  const faultHooks = normalizeFaultHooks(options.faultHooks);
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
    return withAttemptLock(launchAttemptId, async () => {
      const held = await heldPrivateDirectory(
        root,
        { create: false },
        faultHooks,
      );
      if (held === null) return null;
      try {
        return await readCurrent(root, launchAttemptId, held);
      } finally {
        await closeHeldDirectory(held);
      }
    });
  });

  const claim = frozenFunction(async function claim(inputValue) {
    ensure(arguments.length === 1);
    const input = exactDataObject(inputValue, CLAIM_KEYS);
    const record = normalizeRecord(input.record);
    ensure(record.status === "preparing" && record.revision === 0);
    return withAttemptLock(record.launchAttemptId, async () => {
      const held = await heldPrivateDirectory(
        root,
        { create: true },
        faultHooks,
      );
      try {
        const created = await writeExclusive(root, record, held, faultHooks);
        const current = await readCurrent(root, record.launchAttemptId, held);
        ensure(current !== null, "podman_writer_state_io_failed");
        return frozenRecord({ created, record: current });
      } finally {
        await closeHeldDirectory(held);
      }
    });
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
    return withAttemptLock(record.launchAttemptId, async () => {
      const held = await heldPrivateDirectory(
        root,
        { create: false },
        faultHooks,
      );
      ensure(held !== null, "podman_writer_state_conflict");
      try {
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
      } finally {
        await closeHeldDirectory(held);
      }
    });
  });

  const state = frozenRecord({
    claim,
    contractVersion: PODMAN_WRITER_SUPERVISOR_STATE_CONTRACT_VERSION,
    read,
    transition,
  });
  callIntrinsic(weakSetAddIntrinsic, stateBrands, [state]);
  return state;
}

callIntrinsic(objectFreezeIntrinsic, Object, [PodmanWriterSupervisorStateError.prototype]);
callIntrinsic(objectFreezeIntrinsic, Object, [PodmanWriterSupervisorStateError]);
callIntrinsic(objectFreezeIntrinsic, Object, [assertPodmanWriterSupervisorStateRecord]);
callIntrinsic(objectFreezeIntrinsic, Object, [createPodmanWriterSupervisorState]);
callIntrinsic(objectFreezeIntrinsic, Object, [isPodmanWriterSupervisorState]);
