import { createHash, randomUUID } from "node:crypto";
import { constants as fsConstants } from "node:fs";
import {
  lstat,
  open,
  readdir,
  realpath,
  rename,
  unlink,
} from "node:fs/promises";
import { isAbsolute, join, relative, resolve, sep } from "node:path";
import { TextDecoder, types as utilTypes } from "node:util";

const objectFreeze = Object.freeze;
const objectCreate = Object.create;
const objectHasOwn = Object.hasOwn;
const reflectOwnKeys = Reflect.ownKeys;
const arrayIsArray = Array.isArray;

const PINNED_CODEX_VERSION = "codex-cli 0.144.1";
const PINNED_ROLLOUT_CLI_VERSION = "0.144.1";
const PINNED_SOURCE_COMMIT =
  "db887d03e1f907467e33271572dffb73bceecd6b";
const MAX_FILE_BYTES = 64 * 1024 * 1024;
const MAX_TOTAL_BYTES = 256 * 1024 * 1024;
const MAX_ROLLOUT_FILES = 256;
const MAX_DIRECTORIES = 1_024;
const MAX_DIRECTORY_DEPTH = 8;
const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u;
const SHA256_PATTERN = /^[0-9a-f]{64}$/u;
const INTERNAL_ERRORS = new WeakSet();
const UTF8_DECODER = new TextDecoder("utf-8", { fatal: true });

const ERROR_MESSAGES = objectFreeze({
  invalid_request: "Rollout-tail repair request is invalid",
  repair_failed: "Rollout-tail repair failed before publication",
  repair_outcome_uncertain: "Rollout-tail repair outcome is uncertain",
  rollout_content_invalid: "Rollout content is invalid",
  rollout_set_invalid: "Rollout set is invalid",
  runtime_identity_mismatch: "Codex runtime identity is not supported",
  unsafe_filesystem: "Rollout filesystem authority is unsafe",
  unsupported_rollout_object: "Rollout object is not supported",
});

export const ROLLOUT_TAIL_REPAIR_COMPATIBILITY = deepFreeze({
  codexVersion: PINNED_CODEX_VERSION,
  rolloutCliVersion: PINNED_ROLLOUT_CLI_VERSION,
  sourceAnalysisCommit: PINNED_SOURCE_COMMIT,
});

export class RolloutTailRepairError extends Error {
  constructor(code) {
    if (typeof code !== "string" || !objectHasOwn(ERROR_MESSAGES, code)) {
      throw new TypeError("unsupported rollout-tail repair error code");
    }
    super(ERROR_MESSAGES[code]);
    Object.defineProperties(this, {
      code: { enumerable: true, value: code },
      name: { enumerable: true, value: "RolloutTailRepairError" },
      retryable: { enumerable: true, value: false },
      stack: {
        configurable: false,
        enumerable: false,
        value: `RolloutTailRepairError: ${ERROR_MESSAGES[code]}`,
        writable: false,
      },
    });
    objectFreeze(this);
  }
}

function deepFreeze(value) {
  if (value !== null && typeof value === "object") {
    for (const key of reflectOwnKeys(value)) deepFreeze(value[key]);
    objectFreeze(value);
  }
  return value;
}

function makeError(code) {
  const error = new RolloutTailRepairError(code);
  INTERNAL_ERRORS.add(error);
  return error;
}

function fail(code) {
  throw makeError(code);
}

function isInternalError(error) {
  return (
    error !== null &&
    (typeof error === "object" || typeof error === "function") &&
    INTERNAL_ERRORS.has(error)
  );
}

function ensure(condition, code) {
  if (!condition) fail(code);
}

function exactPlainObject(value, keys, code = "invalid_request") {
  ensure(
    value !== null &&
      typeof value === "object" &&
      !arrayIsArray(value) &&
      !utilTypes.isProxy(value),
    code,
  );
  let prototype;
  let actual;
  try {
    prototype = Object.getPrototypeOf(value);
    actual = reflectOwnKeys(value);
  } catch {
    fail(code);
  }
  ensure(prototype === Object.prototype || prototype === null, code);
  ensure(
    actual.length === keys.length &&
      actual.every(
        (key) => typeof key === "string" && keys.includes(key),
      ),
    code,
  );
  const normalized = objectCreate(null);
  for (const key of actual) {
    let descriptor;
    try {
      descriptor = Object.getOwnPropertyDescriptor(value, key);
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

function normalizeRequest(options) {
  const request = exactPlainObject(options, [
    "codexHome",
    "rootSessionId",
    "runtimeIdentity",
  ]);
  ensure(
    typeof request.codexHome === "string" &&
      request.codexHome.length > 0 &&
      !request.codexHome.includes("\0") &&
      isAbsolute(request.codexHome),
    "invalid_request",
  );
  ensure(
    typeof request.rootSessionId === "string" &&
      UUID_PATTERN.test(request.rootSessionId),
    "invalid_request",
  );
  const runtime = exactPlainObject(
    request.runtimeIdentity,
    ["codexBinarySha256", "codexVersion", "sourceAnalysisCommit"],
    "invalid_request",
  );
  // The trusted launcher authenticates the executable digest and reserves the
  // matching executable for later admission. This primitive validates only a
  // canonical proof binding; it does not treat caller-controlled text as
  // executable or image authority.
  ensure(
    runtime.codexVersion === PINNED_CODEX_VERSION &&
      runtime.sourceAnalysisCommit === PINNED_SOURCE_COMMIT &&
      typeof runtime.codexBinarySha256 === "string" &&
      SHA256_PATTERN.test(runtime.codexBinarySha256),
    "runtime_identity_mismatch",
  );
  return {
    codexHome: resolve(request.codexHome),
    rootSessionId: request.rootSessionId,
    runtimeIdentity: {
      codexBinarySha256: runtime.codexBinarySha256,
      codexVersion: runtime.codexVersion,
      sourceAnalysisCommit: runtime.sourceAnalysisCommit,
    },
  };
}

function statType(metadata) {
  if (metadata.isDirectory()) return "directory";
  if (metadata.isFile()) return "file";
  return "other";
}

function fingerprint(metadata) {
  return objectFreeze({
    ctimeNs: metadata.ctimeNs,
    dev: metadata.dev,
    gid: metadata.gid,
    ino: metadata.ino,
    mode: metadata.mode,
    mtimeNs: metadata.mtimeNs,
    nlink: metadata.nlink,
    size: metadata.size,
    type: statType(metadata),
    uid: metadata.uid,
  });
}

function sameFingerprint(left, right) {
  return (
    left.ctimeNs === right.ctimeNs &&
    left.dev === right.dev &&
    left.gid === right.gid &&
    left.ino === right.ino &&
    left.mode === right.mode &&
    left.mtimeNs === right.mtimeNs &&
    left.nlink === right.nlink &&
    left.size === right.size &&
    left.type === right.type &&
    left.uid === right.uid
  );
}

function sameDirectoryAuthority(left, right) {
  return (
    left.dev === right.dev &&
    left.gid === right.gid &&
    left.ino === right.ino &&
    left.mode === right.mode &&
    left.nlink === right.nlink &&
    left.type === right.type &&
    left.uid === right.uid
  );
}

function sameDirectoryIdentityWhileEditing(left, right) {
  return (
    left.dev === right.dev &&
    left.gid === right.gid &&
    left.ino === right.ino &&
    left.mode === right.mode &&
    left.type === right.type &&
    left.uid === right.uid
  );
}

function currentUid() {
  const uid = process.geteuid?.() ?? process.getuid?.();
  ensure(Number.isSafeInteger(uid) && uid >= 0, "unsafe_filesystem");
  return BigInt(uid);
}

function assertSafeDirectory(metadata, uid) {
  ensure(metadata.isDirectory() && !metadata.isSymbolicLink(), "unsafe_filesystem");
  ensure(metadata.uid === uid, "unsafe_filesystem");
  const mode = metadata.mode & 0o7777n;
  ensure(mode === 0o700n, "unsafe_filesystem");
}

function assertSafeFile(metadata, uid) {
  ensure(metadata.isFile() && !metadata.isSymbolicLink(), "unsafe_filesystem");
  ensure(metadata.uid === uid && metadata.nlink === 1n, "unsafe_filesystem");
  const mode = metadata.mode & 0o7777n;
  ensure(mode === 0o600n, "unsafe_filesystem");
}

function openDirectoryFlags() {
  return (
    fsConstants.O_RDONLY |
    (fsConstants.O_DIRECTORY ?? 0) |
    (fsConstants.O_NOFOLLOW ?? 0) |
    (fsConstants.O_CLOEXEC ?? 0)
  );
}

function openFileFlags() {
  return (
    fsConstants.O_RDONLY |
    (fsConstants.O_NOFOLLOW ?? 0) |
    (fsConstants.O_NONBLOCK ?? 0) |
    (fsConstants.O_CLOEXEC ?? 0)
  );
}

async function openPinnedDirectory(path, uid, expectedDev) {
  const before = await lstat(path, { bigint: true });
  assertSafeDirectory(before, uid);
  if (expectedDev !== undefined) ensure(before.dev === expectedDev, "unsafe_filesystem");
  const handle = await open(path, openDirectoryFlags());
  try {
    const opened = await handle.stat({ bigint: true });
    assertSafeDirectory(opened, uid);
    if (expectedDev !== undefined) ensure(opened.dev === expectedDev, "unsafe_filesystem");
    ensure(
      sameFingerprint(fingerprint(before), fingerprint(opened)),
      "unsafe_filesystem",
    );
    return {
      entries: null,
      fingerprint: fingerprint(opened),
      handle,
      path,
    };
  } catch (error) {
    await handle.close().catch(() => {});
    throw error;
  }
}

async function readPinnedFile(path, uid, expectedDev) {
  const before = await lstat(path, { bigint: true });
  assertSafeFile(before, uid);
  ensure(before.dev === expectedDev, "unsafe_filesystem");
  ensure(before.size > 0n && before.size <= BigInt(MAX_FILE_BYTES), "rollout_content_invalid");
  const handle = await open(path, openFileFlags());
  try {
    const opened = await handle.stat({ bigint: true });
    assertSafeFile(opened, uid);
    ensure(opened.dev === expectedDev, "unsafe_filesystem");
    ensure(
      sameFingerprint(fingerprint(before), fingerprint(opened)),
      "unsafe_filesystem",
    );
    const bytes = await readExactBytes(handle, Number(opened.size));
    const after = await handle.stat({ bigint: true });
    ensure(
      sameFingerprint(fingerprint(opened), fingerprint(after)),
      "unsafe_filesystem",
    );
    return { bytes, fingerprint: fingerprint(after), handle, path };
  } catch (error) {
    await handle.close().catch(() => {});
    throw error;
  }
}

async function readExactBytes(handle, size) {
  const bytes = Buffer.allocUnsafe(size);
  let offset = 0;
  while (offset < size) {
    const { bytesRead } = await handle.read(bytes, offset, size - offset, offset);
    ensure(bytesRead > 0, "unsafe_filesystem");
    offset += bytesRead;
  }
  const extra = Buffer.allocUnsafe(1);
  const { bytesRead: extraBytes } = await handle.read(extra, 0, 1, size);
  ensure(extraBytes === 0, "unsafe_filesystem");
  return bytes;
}

function jsonValue(bytes) {
  try {
    const text = UTF8_DECODER.decode(bytes);
    if (text.length === 0) return { ok: false };
    return { ok: true, value: JSON.parse(text) };
  } catch {
    return { ok: false };
  }
}

function assertSessionMeta(value) {
  ensure(
    value !== null &&
      typeof value === "object" &&
      !arrayIsArray(value) &&
      value.type === "session_meta" &&
      value.payload !== null &&
      typeof value.payload === "object" &&
      !arrayIsArray(value.payload),
    "rollout_content_invalid",
  );
  const { cli_version: cliVersion, id, session_id: sessionId } = value.payload;
  ensure(
    cliVersion === PINNED_ROLLOUT_CLI_VERSION &&
      typeof id === "string" &&
      UUID_PATTERN.test(id) &&
      typeof sessionId === "string" &&
      UUID_PATTERN.test(sessionId),
    "rollout_content_invalid",
  );
  return { sessionId, threadId: id };
}

function analyzeBytes(bytes) {
  const terminated = bytes.at(-1) === 0x0a;
  const finalLf = bytes.lastIndexOf(0x0a);
  const completeEnd = terminated ? bytes.length : finalLf + 1;
  const completeLines = [];
  let start = 0;
  for (let index = 0; index < completeEnd; index += 1) {
    if (bytes[index] !== 0x0a) continue;
    const parsed = jsonValue(bytes.subarray(start, index));
    ensure(parsed.ok, "rollout_content_invalid");
    completeLines.push(parsed.value);
    start = index + 1;
  }

  let action = "unchanged";
  let after = bytes;
  let removedBytes = 0;
  if (!terminated) {
    const tail = bytes.subarray(finalLf + 1);
    const parsed = jsonValue(tail);
    if (parsed.ok) {
      completeLines.push(parsed.value);
      after = Buffer.concat([bytes, Buffer.from("\n")]);
      action = "append_lf";
    } else {
      ensure(completeLines.length > 0, "rollout_content_invalid");
      after = bytes.subarray(0, finalLf + 1);
      removedBytes = tail.length;
      ensure(removedBytes > 0, "rollout_content_invalid");
      action = "truncate_partial_tail";
    }
  }
  ensure(completeLines.length > 0, "rollout_content_invalid");
  const meta = assertSessionMeta(completeLines[0]);
  return { action, after, meta, removedBytes };
}

function sha256(bytes) {
  return createHash("sha256").update(bytes).digest("hex");
}

function proofState(bytes) {
  const digest = sha256(bytes);
  ensure(SHA256_PATTERN.test(digest), "repair_outcome_uncertain");
  return { sha256: digest, size: bytes.length };
}

function relativeRolloutPath(sessionsPath, path) {
  const candidate = relative(sessionsPath, path);
  ensure(
    candidate.length > 0 &&
      !candidate.startsWith(`..${sep}`) &&
      candidate !== ".." &&
      !isAbsolute(candidate),
    "unsafe_filesystem",
  );
  return candidate.split(sep).join("/");
}

async function scanTree(codexHome, uid) {
  const requestedHome = await lstat(codexHome, { bigint: true });
  ensure(!requestedHome.isSymbolicLink(), "unsafe_filesystem");
  const canonicalHome = await realpath(codexHome);
  const home = await openPinnedDirectory(canonicalHome, uid);
  ensure(
    sameFingerprint(fingerprint(requestedHome), home.fingerprint),
    "unsafe_filesystem",
  );
  const filesystemDev = home.fingerprint.dev;
  const sessionsPath = join(canonicalHome, "sessions");
  let sessions;
  const directories = [home];
  const files = [];
  let totalBytes = 0;
  try {
    ensure((await realpath(sessionsPath)) === sessionsPath, "unsafe_filesystem");
    sessions = await openPinnedDirectory(sessionsPath, uid, filesystemDev);
    directories.push(sessions);

    async function visit(directory, depth) {
      ensure(depth <= MAX_DIRECTORY_DEPTH, "rollout_set_invalid");
      const entries = await readdir(directory.path, { withFileTypes: true });
      entries.sort((left, right) => left.name.localeCompare(right.name, "en"));
      directory.entries = entries.map((entry) => entry.name);
      for (const entry of entries) {
        ensure(entry.name !== "." && entry.name !== "..", "unsafe_filesystem");
        const path = join(directory.path, entry.name);
        const metadata = await lstat(path, { bigint: true });
        if (metadata.isSymbolicLink()) fail("unsafe_filesystem");
        if (metadata.isDirectory()) {
          ensure(directories.length < MAX_DIRECTORIES, "rollout_set_invalid");
          const child = await openPinnedDirectory(path, uid, filesystemDev);
          directories.push(child);
          await visit(child, depth + 1);
          continue;
        }
        if (!metadata.isFile()) fail("unsupported_rollout_object");
        if (!entry.name.endsWith(".jsonl")) fail("unsupported_rollout_object");
        ensure(files.length < MAX_ROLLOUT_FILES, "rollout_set_invalid");
        const file = await readPinnedFile(path, uid, filesystemDev);
        file.parent = directory;
        file.relativePath = relativeRolloutPath(sessionsPath, path);
        files.push(file);
        totalBytes += file.bytes.length;
        ensure(totalBytes <= MAX_TOTAL_BYTES, "rollout_set_invalid");
      }
    }

    await visit(sessions, 0);
    ensure(files.length > 0, "rollout_set_invalid");
    return { directories, files, home, sessions, sessionsPath };
  } catch (error) {
    await Promise.allSettled(files.map((file) => file.handle.close()));
    await Promise.allSettled(directories.map((directory) => directory.handle.close()));
    throw error;
  }
}

async function verifyDirectory(directory, uid, { includeTimestamps = false } = {}) {
  const pathMetadata = await lstat(directory.path, { bigint: true });
  const handleMetadata = await directory.handle.stat({ bigint: true });
  assertSafeDirectory(pathMetadata, uid);
  assertSafeDirectory(handleMetadata, uid);
  const pathFingerprint = fingerprint(pathMetadata);
  const handleFingerprint = fingerprint(handleMetadata);
  const compare = includeTimestamps ? sameFingerprint : sameDirectoryAuthority;
  ensure(compare(directory.fingerprint, pathFingerprint), "unsafe_filesystem");
  ensure(compare(directory.fingerprint, handleFingerprint), "unsafe_filesystem");
  if (directory.entries === null) return;
  const names = (await readdir(directory.path)).sort((left, right) =>
    left.localeCompare(right, "en"),
  );
  ensure(
    names.length === directory.entries.length &&
      names.every((name, index) => name === directory.entries[index]),
    "unsafe_filesystem",
  );
}

async function verifyFile(file, uid, expectedFingerprint = file.fingerprint) {
  const pathMetadata = await lstat(file.path, { bigint: true });
  assertSafeFile(pathMetadata, uid);
  ensure(
    sameFingerprint(expectedFingerprint, fingerprint(pathMetadata)),
    "unsafe_filesystem",
  );
  if (file.handle !== null) {
    const handleMetadata = await file.handle.stat({ bigint: true });
    ensure(
      sameFingerprint(file.fingerprint, fingerprint(handleMetadata)),
      "unsafe_filesystem",
    );
  }
}

async function closeScan(scan) {
  await Promise.allSettled(
    scan.files.map((file) => (file.handle === null ? undefined : file.handle.close())),
  );
  await Promise.allSettled(scan.directories.map((directory) => directory.handle.close()));
}

async function safelyUnlinkTemporary(path, handleFingerprint) {
  try {
    const metadata = await lstat(path, { bigint: true });
    if (sameFingerprint(handleFingerprint, fingerprint(metadata))) await unlink(path);
  } catch {
    // Cleanup is best effort; the private temporary file is never mistaken for
    // a successful repair, and a future scan rejects it.
  }
}

const DEFAULT_HOOKS = objectFreeze({
  afterRename: async () => {},
  beforeRename: async () => {},
  syncDirectory: async (handle) => handle.sync(),
});

function normalizeHooks(hooks) {
  if (hooks === undefined) return DEFAULT_HOOKS;
  const normalized = exactPlainObject(
    hooks,
    ["afterRename", "beforeRename", "syncDirectory"],
    "invalid_request",
  );
  for (const name of ["afterRename", "beforeRename", "syncDirectory"]) {
    ensure(typeof normalized[name] === "function", "invalid_request");
  }
  return normalized;
}

async function replaceFile(file, analysis, uid, hooks) {
  const temporaryPath = join(
    file.parent.path,
    `.rollout-tail-repair-${randomUUID()}.tmp`,
  );
  let temporaryHandle;
  let temporaryFingerprint;
  let publicationAttempted = false;
  try {
    temporaryHandle = await open(
      temporaryPath,
      fsConstants.O_RDWR |
        fsConstants.O_CREAT |
        fsConstants.O_EXCL |
        (fsConstants.O_NOFOLLOW ?? 0) |
        (fsConstants.O_CLOEXEC ?? 0),
      0o600,
    );
    await temporaryHandle.chmod(0o600);
    await temporaryHandle.writeFile(analysis.after);
    await temporaryHandle.sync();
    const temporaryMetadata = await temporaryHandle.stat({ bigint: true });
    assertSafeFile(temporaryMetadata, uid);
    ensure(
      temporaryMetadata.size === BigInt(analysis.after.length),
      "repair_failed",
    );
    const temporaryBytes = await readExactBytes(
      temporaryHandle,
      analysis.after.length,
    );
    ensure(
      sha256(temporaryBytes) === sha256(analysis.after),
      "repair_failed",
    );
    const temporaryReadbackMetadata = await temporaryHandle.stat({ bigint: true });
    ensure(
      sameFingerprint(
        fingerprint(temporaryMetadata),
        fingerprint(temporaryReadbackMetadata),
      ),
      "repair_failed",
    );
    temporaryFingerprint = fingerprint(temporaryReadbackMetadata);

    await hooks.beforeRename();
    await verifyFile(file, uid);
    const parentPath = await lstat(file.parent.path, { bigint: true });
    ensure(
      sameDirectoryIdentityWhileEditing(
        file.parent.fingerprint,
        fingerprint(parentPath),
      ),
      "unsafe_filesystem",
    );

    publicationAttempted = true;
    await rename(temporaryPath, file.path);
    await hooks.afterRename();
    await hooks.syncDirectory(file.parent.handle);
    await temporaryHandle.close();
    temporaryHandle = undefined;
    await file.handle.close();
    file.handle = null;

    const replacement = await readPinnedFile(file.path, uid, file.fingerprint.dev);
    try {
      ensure(
        replacement.bytes.length === analysis.after.length &&
          sha256(replacement.bytes) === sha256(analysis.after),
        "repair_outcome_uncertain",
      );
      file.fingerprint = replacement.fingerprint;
    } finally {
      await replacement.handle.close();
    }
  } catch (error) {
    if (temporaryHandle !== undefined) {
      if (temporaryFingerprint === undefined) {
        try {
          temporaryFingerprint = fingerprint(
            await temporaryHandle.stat({ bigint: true }),
          );
        } catch {
          // Without a pinned inode, cleanup must leave the path untouched.
        }
      }
      await temporaryHandle.close().catch(() => {});
    }
    if (!publicationAttempted && temporaryFingerprint !== undefined) {
      await safelyUnlinkTemporary(temporaryPath, temporaryFingerprint);
    }
    if (isInternalError(error) && error.code === "repair_outcome_uncertain") throw error;
    fail(publicationAttempted ? "repair_outcome_uncertain" : "repair_failed");
  }
}

async function repairWithHooks(options, hooks) {
  let request;
  try {
    request = normalizeRequest(options);
  } catch (error) {
    if (isInternalError(error)) throw error;
    fail("invalid_request");
  }

  const uid = currentUid();
  let scan;
  let published = false;
  try {
    scan = await scanTree(request.codexHome, uid);
    const analyzed = scan.files.map((file) => ({
      ...analyzeBytes(file.bytes),
      file,
    }));
    const roots = analyzed.filter(
      ({ meta }) => meta.threadId === request.rootSessionId,
    );
    ensure(roots.length === 1, "rollout_set_invalid");
    ensure(
      roots[0].meta.sessionId === request.rootSessionId,
      "rollout_set_invalid",
    );
    const threadIds = new Set();
    for (const entry of analyzed) {
      ensure(
        entry.meta.sessionId === request.rootSessionId &&
          !threadIds.has(entry.meta.threadId),
        "rollout_set_invalid",
      );
      threadIds.add(entry.meta.threadId);
    }

    for (const directory of scan.directories) {
      await verifyDirectory(directory, uid, { includeTimestamps: true });
    }
    for (const file of scan.files) await verifyFile(file, uid);

    const proofs = [];
    for (const entry of analyzed) {
      const before = proofState(entry.file.bytes);
      const after = proofState(entry.after);
      if (entry.action === "unchanged") {
        await verifyFile(entry.file, uid);
      } else {
        await verifyDirectory(entry.file.parent, uid);
        await replaceFile(entry.file, entry, uid, hooks);
        published = true;
      }
      proofs.push({
        action: entry.action,
        after,
        before,
        relativePath: entry.file.relativePath,
        removedBytes: entry.removedBytes,
      });
    }

    for (const directory of scan.directories) await verifyDirectory(directory, uid);
    for (const file of scan.files) await verifyFile(file, uid, file.fingerprint);

    proofs.sort((left, right) =>
      left.relativePath.localeCompare(right.relativePath, "en"),
    );
    return deepFreeze({
      compatibility: {
        codexBinarySha256: request.runtimeIdentity.codexBinarySha256,
        codexVersion: request.runtimeIdentity.codexVersion,
        sourceAnalysisCommit: request.runtimeIdentity.sourceAnalysisCommit,
      },
      files: proofs,
      rootSessionId: request.rootSessionId,
    });
  } catch (error) {
    if (isInternalError(error)) {
      if (published && error.code !== "repair_outcome_uncertain") {
        throw makeError("repair_outcome_uncertain");
      }
      throw error;
    }
    throw makeError(published ? "repair_outcome_uncertain" : "unsafe_filesystem");
  } finally {
    if (scan !== undefined) await closeScan(scan);
  }
}

/**
 * Repair only a stopped/detached restored copy before writer admission.
 *
 * The caller must hold the external stopped-writer/attachment authority for
 * the entire call and must authenticate runtimeIdentity.codexBinarySha256
 * against the executable reserved for later admission. This primitive neither
 * hashes or authorizes that executable, stops Codex, nor mutates the original
 * immutable crash checkpoint. It only validates and repairs the restored copy.
 */
export async function repairStoppedRolloutTails(options) {
  return repairWithHooks(options, DEFAULT_HOOKS);
}

// Fault injection is intentionally separate from the production entry point.
// Hooks receive no path, bytes, identifiers, or proof content.
export const __testing = objectFreeze({
  createRepair(hooks) {
    const checkedHooks = normalizeHooks(hooks);
    return async (options) => repairWithHooks(options, checkedHooks);
  },
});
