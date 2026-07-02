import {
  KeyObject,
  createCipheriv,
  createDecipheriv,
  randomBytes,
} from "node:crypto";
import {
  closeSync,
  constants,
  fstatSync,
  openSync,
  realpathSync,
} from "node:fs";
import { lstat, open, readdir, realpath } from "node:fs/promises";
import { dirname, join, resolve } from "node:path";

import { acquireAdvisoryLock, sameFileIdentity } from "./advisory-lock.mjs";
import {
  authorityDirectoryPermissionsAreSafe,
  pathHasExtendedAcl,
  pathHasUnsafeAncestorAcl,
} from "./managed-auth-refresh.mjs";

const ALGORITHM = "aes-256-gcm";
const AUTHORITY_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u;
const CANONICAL_FILE = "auth-state.enc";
const ENVELOPE_FIELDS = Object.freeze([
  "formatVersion",
  "algorithm",
  "authorityId",
  "keyId",
  "nonce",
  "ciphertext",
  "tag",
]);
const FORMAT_VERSION = 1;
const GENERATION_PATTERN = /^(?:0|[1-9][0-9]{0,19})$/u;
const KEY_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u;
const LOCK_FILE = ".auth-state.lock";
const MAX_ENVELOPE_BYTES = 2 * 1024 * 1024;
const MAX_GENERATION = 18_446_744_073_709_551_615n;
const MAX_PAYLOAD_BYTES = 1024 * 1024;
const NEXT_PREFIX = `.${CANONICAL_FILE}.next-`;
const pinnedAuthorityDirectories = new Map();
const UNSAFE_LOCK_CODES = new Set([
  "lock_cleanup_failed",
  "lock_lost",
  "lock_replaced",
  "unsafe_lock_file",
]);
const RECORD_FIELDS = Object.freeze([
  "recordVersion",
  "generation",
  "baseGeneration",
  "commitId",
  "operation",
  "payload",
]);
const RECORD_VERSION = 1;

const processQueues = new Map();

function currentBrokerUid() {
  if (typeof process.geteuid === "function") return BigInt(process.geteuid());
  if (typeof process.getuid === "function") return BigInt(process.getuid());
  return null;
}

function fail(code, message, options) {
  throw new AuthStateStoreError(code, message, options);
}

function ensure(condition, code, message, options) {
  if (!condition) fail(code, message, options);
}

function exactKeys(value, expected) {
  if (value === null || typeof value !== "object" || Array.isArray(value)) return false;
  const actual = Object.keys(value).sort();
  const sortedExpected = [...expected].sort();
  return (
    actual.length === sortedExpected.length &&
    actual.every((key, index) => key === sortedExpected[index])
  );
}

function ownDataProperty(value, key) {
  try {
    if (value === null || !["function", "object"].includes(typeof value)) return undefined;
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    return descriptor && Object.hasOwn(descriptor, "value") ? descriptor.value : undefined;
  } catch {
    return undefined;
  }
}

function canonicalJson(value, newline = false) {
  return `${JSON.stringify(value)}${newline ? "\n" : ""}`;
}

function parseCanonicalJson(raw, expectedFields, code, label, newline = false) {
  let value;
  try {
    value = JSON.parse(raw);
  } catch {
    fail(code, `${label} is not valid JSON`);
  }
  ensure(exactKeys(value, expectedFields), code, `${label} has an invalid shape`);
  const canonical = Object.fromEntries(expectedFields.map((key) => [key, value[key]]));
  ensure(
    canonicalJson(canonical, newline) === raw,
    code,
    `${label} is not canonical JSON`,
  );
  return canonical;
}

function parseGeneration(value, { allowZero = true } = {}) {
  ensure(
    typeof value === "string" && GENERATION_PATTERN.test(value),
    "invalid_generation",
    "auth state generation is not canonical",
  );
  const generation = BigInt(value);
  ensure(
    generation <= MAX_GENERATION && (allowZero || generation > 0n),
    "invalid_generation",
    "auth state generation is out of range",
  );
  return generation;
}

function validateOpaqueId(value, pattern, code, label) {
  ensure(typeof value === "string" && pattern.test(value), code, `${label} is invalid`);
  return value;
}

function validatePayload(payload) {
  ensure(typeof payload === "string", "invalid_payload", "auth state payload must be a string");
  const bytes = Buffer.from(payload, "utf8");
  ensure(
    bytes.length <= MAX_PAYLOAD_BYTES && bytes.toString("utf8") === payload,
    "invalid_payload",
    "auth state payload is not bounded exact UTF-8",
  );
  return payload;
}

function aclMetadataSignature(metadata, { includeChangeTime }) {
  let isDirectory;
  try {
    isDirectory = metadata.isDirectory();
  } catch {
    fail("invalid_store_directory", "auth state directory metadata is invalid");
  }
  const fields = [metadata.dev, metadata.ino, metadata.uid, metadata.gid, metadata.mode];
  if (includeChangeTime) fields.push(metadata.ctimeNs);
  ensure(
    typeof isDirectory === "boolean" && fields.every((value) => typeof value === "bigint"),
    "invalid_store_directory",
    "auth state directory metadata is invalid",
  );
  return JSON.stringify([isDirectory, ...fields.map((value) => value.toString())]);
}

function validateSecretKey(key) {
  ensure(
    key instanceof KeyObject && key.type === "secret" && key.symmetricKeySize === 32,
    "invalid_key_material",
    "auth state key must be a 256-bit secret KeyObject",
  );
  return key;
}

function decodeBase64Url(value, expectedLength, label) {
  ensure(
    typeof value === "string" && /^[A-Za-z0-9_-]+$/u.test(value),
    "invalid_auth_state",
    `${label} is invalid`,
  );
  const decoded = Buffer.from(value, "base64url");
  ensure(
    decoded.length === expectedLength && decoded.toString("base64url") === value,
    "invalid_auth_state",
    `${label} is invalid`,
  );
  return decoded;
}

function resultFromRecord(record, keyId, replayed) {
  const result = {
    generation: record.generation,
    commitId: record.commitId,
    keyId,
    replayed,
  };
  Object.defineProperty(result, "payload", {
    enumerable: false,
    value: record.payload,
  });
  return Object.freeze(result);
}

async function runProcessExclusive(key, operation) {
  const previous = processQueues.get(key) ?? Promise.resolve();
  let release;
  const gate = new Promise((resolveGate) => {
    release = resolveGate;
  });
  const tail = previous.catch(() => {}).then(() => gate);
  processQueues.set(key, tail);
  await previous.catch(() => {});
  try {
    return await operation();
  } finally {
    release();
    if (processQueues.get(key) === tail) processQueues.delete(key);
  }
}

async function defaultSyncDirectory({ handle }) {
  await handle.sync();
}

function wrapExternalError(error, phase) {
  const code = ownDataProperty(error, "code");
  const renameOutcome = ownDataProperty(error, "renameOutcome");
  if (phase === "rename-attempted" && renameOutcome === "not-committed") {
    if (UNSAFE_LOCK_CODES.has(code)) {
      return new AuthStateStoreError(
        "recovery_required",
        "auth state advisory lock requires operator recovery",
        { commitState: "not-committed", retryable: false },
      );
    }
    return new AuthStateStoreError(
      "auth_state_io_failed",
      "auth state storage operation failed before commit",
      { commitState: "not-committed", retryable: false },
    );
  }
  if (
    phase === "rename-attempted" ||
    phase === "renamed" ||
    phase === "synced" ||
    code === "lock_commit_uncertain"
  ) {
    return new AuthStateStoreError(
      "commit_outcome_uncertain",
      "auth state commit outcome is uncertain",
      { commitState: "uncertain", retryable: false },
    );
  }
  if (UNSAFE_LOCK_CODES.has(code)) {
    return new AuthStateStoreError(
      "recovery_required",
      "auth state advisory lock requires operator recovery",
      { commitState: "not-committed", retryable: false },
    );
  }
  if (code === "unsupported_platform") {
    return new AuthStateStoreError(
      "invalid_lock_provider",
      "auth state advisory lock is unsupported",
      { commitState: "not-committed", retryable: false },
    );
  }
  try {
    if (error instanceof AuthStateStoreError) return error;
  } catch {
    // Treat hostile external error objects as unclassified I/O failures.
  }
  return new AuthStateStoreError(
    "auth_state_io_failed",
    "auth state storage operation failed",
    { commitState: "not-committed", retryable: false },
  );
}

export class AuthStateStoreError extends Error {
  constructor(code, message, { commitState, retryable = false } = {}) {
    super(message);
    this.name = "AuthStateStoreError";
    this.code = code;
    this.retryable = retryable;
    if (commitState) this.commitState = commitState;
  }
}

export class EncryptedAuthStateStore {
  #directoryIdentity;

  constructor({
    acquireLock = (lockPath) => acquireAdvisoryLock(lockPath),
    authorityId,
    directory,
    faults = {},
    inspectAncestorAcl = pathHasUnsafeAncestorAcl,
    inspectExtendedAcl = pathHasExtendedAcl,
    keyProvider,
    openFile = open,
    randomBytesFn = randomBytes,
    readDirectory = readdir,
    readPathMetadata = lstat,
    realPath = realpath,
    syncDirectory = defaultSyncDirectory,
  }) {
    validateOpaqueId(authorityId, AUTHORITY_ID_PATTERN, "invalid_authority_id", "authority ID");
    ensure(
      typeof directory === "string" && directory.length > 0,
      "invalid_store_directory",
      "auth state directory is invalid",
    );
    ensure(
      keyProvider &&
        typeof keyProvider.activeKey === "function" &&
        typeof keyProvider.getKey === "function",
      "invalid_key_provider",
      "auth state key provider is invalid",
    );
    ensure(typeof acquireLock === "function", "invalid_lock_provider", "lock provider is invalid");

    this.authorityId = authorityId;
    let directoryHandle;
    let directoryIdentity;
    try {
      this.directory = realpathSync(resolve(directory));
      ensure(
        typeof constants.O_DIRECTORY === "number" && typeof constants.O_NOFOLLOW === "number",
        "invalid_store_directory",
        "secure directory open flags are unavailable",
      );
      directoryHandle = openSync(
        this.directory,
        constants.O_RDONLY | constants.O_DIRECTORY | constants.O_NOFOLLOW,
      );
      directoryIdentity = fstatSync(directoryHandle, { bigint: true });
      ensure(
        directoryIdentity.isDirectory(),
        "invalid_store_directory",
        "auth state directory is invalid",
      );
    } catch {
      fail("invalid_store_directory", "auth state directory could not be canonicalized");
    } finally {
      if (directoryHandle !== undefined) {
        try {
          closeSync(directoryHandle);
        } catch {
          fail("invalid_store_directory", "auth state directory identity could not be pinned");
        }
      }
    }
    this.coordinationKey = JSON.stringify([this.directory, authorityId]);
    const pinnedIdentity = pinnedAuthorityDirectories.get(this.coordinationKey);
    ensure(
      pinnedIdentity === undefined || sameFileIdentity(directoryIdentity, pinnedIdentity),
      "invalid_store_directory",
      "auth state directory identity changed",
    );
    if (pinnedIdentity === undefined) {
      const identity = Object.freeze({ dev: directoryIdentity.dev, ino: directoryIdentity.ino });
      pinnedAuthorityDirectories.set(this.coordinationKey, identity);
      this.#directoryIdentity = identity;
    } else {
      this.#directoryIdentity = pinnedIdentity;
    }
    this.keyProvider = keyProvider;
    this.acquireLock = acquireLock;
    this.faults = faults;
    this.inspectAncestorAcl = inspectAncestorAcl;
    this.inspectExtendedAcl = inspectExtendedAcl;
    this.openFile = openFile;
    this.randomBytesFn = randomBytesFn;
    this.readDirectory = readDirectory;
    this.readPathMetadata = readPathMetadata;
    this.realPath = realPath;
    this.syncDirectory = syncDirectory;
  }

  read() {
    return this.#withTransaction({ mutation: false }, async (lock, directoryGuard) => {
      await this.#assertNoRecoveryArtifacts(directoryGuard);
      await lock.assertHeld();
      const current = await this.#readCanonical({ allowMissing: true, directoryGuard });
      await this.#proveDirectoryDurable(lock, directoryGuard);
      return {
        committed: false,
        value: current ? resultFromRecord(current.record, current.envelope.keyId, false) : null,
      };
    });
  }

  compareAndSwap({ expectedGeneration, commitId, payload }) {
    validateOpaqueId(commitId, AUTHORITY_ID_PATTERN, "invalid_commit_id", "commit ID");
    validatePayload(payload);
    const parsedExpectedGeneration = parseGeneration(expectedGeneration);
    return this.#withTransaction({ mutation: true }, async (lock, directoryGuard) => {
      await this.#assertNoRecoveryArtifacts(directoryGuard);
      await lock.assertHeld();
      const current = await this.#readCanonical({ allowMissing: true, directoryGuard });
      if (current?.record.commitId === commitId) {
        ensure(
          current.record.operation === "compare-and-swap" &&
            current.record.payload === payload,
          "commit_id_conflict",
          "auth state commit ID was reused for a different operation or content",
        );
        ensure(
          parseGeneration(current.record.baseGeneration) === parsedExpectedGeneration,
          "generation_conflict",
          "auth state replay generation does not match",
          { retryable: true },
        );
        await this.#proveDirectoryDurable(lock, directoryGuard);
        return {
          committed: false,
          value: resultFromRecord(current.record, current.envelope.keyId, true),
        };
      }

      const currentGeneration = current ? parseGeneration(current.record.generation) : 0n;
      ensure(
        currentGeneration === parsedExpectedGeneration,
        "generation_conflict",
        "auth state generation does not match",
        { retryable: true },
      );
      ensure(
        currentGeneration < MAX_GENERATION,
        "generation_exhausted",
        "auth state generation is exhausted",
      );
      const record = {
        recordVersion: RECORD_VERSION,
        generation: (currentGeneration + 1n).toString(),
        baseGeneration: currentGeneration.toString(),
        commitId,
        operation: "compare-and-swap",
        payload,
      };
      const committed = await this.#commit(record, lock, directoryGuard);
      return {
        committed: true,
        value: resultFromRecord(committed.record, committed.envelope.keyId, false),
      };
    });
  }

  rotateEncryption({ expectedGeneration, commitId }) {
    validateOpaqueId(commitId, AUTHORITY_ID_PATTERN, "invalid_commit_id", "commit ID");
    const parsedExpectedGeneration = parseGeneration(expectedGeneration);
    return this.#withTransaction({ mutation: true }, async (lock, directoryGuard) => {
      await this.#assertNoRecoveryArtifacts(directoryGuard);
      await lock.assertHeld();
      const current = await this.#readCanonical({ allowMissing: false, directoryGuard });
      if (current.record.commitId === commitId) {
        ensure(
          current.record.operation === "rotate-encryption",
          "commit_id_conflict",
          "auth state commit ID was reused for a different operation",
        );
        ensure(
          parseGeneration(current.record.baseGeneration) === parsedExpectedGeneration,
          "generation_conflict",
          "auth state replay generation does not match",
          { retryable: true },
        );
        await this.#proveDirectoryDurable(lock, directoryGuard);
        return {
          committed: false,
          value: resultFromRecord(current.record, current.envelope.keyId, true),
        };
      }
      const currentGeneration = parseGeneration(current.record.generation, { allowZero: false });
      ensure(
        currentGeneration === parsedExpectedGeneration,
        "generation_conflict",
        "auth state generation does not match",
        { retryable: true },
      );
      ensure(
        currentGeneration < MAX_GENERATION,
        "generation_exhausted",
        "auth state generation is exhausted",
      );
      const record = {
        recordVersion: RECORD_VERSION,
        generation: (currentGeneration + 1n).toString(),
        baseGeneration: currentGeneration.toString(),
        commitId,
        operation: "rotate-encryption",
        payload: current.record.payload,
      };
      const committed = await this.#commit(record, lock, directoryGuard);
      return {
        committed: true,
        value: resultFromRecord(committed.record, committed.envelope.keyId, false),
      };
    });
  }

  async #withTransaction({ mutation }, operation) {
    return runProcessExclusive(this.coordinationKey, async () => {
      const directoryGuard = await this.#openDirectoryGuard();
      let lock;
      let outcome;
      let primaryError;
      try {
        lock = await this.acquireLock(join(directoryGuard.path, LOCK_FILE));
        this.#invalidateAncestorAclCache(directoryGuard);
        await this.#assertDirectoryCurrent(directoryGuard);
        outcome = await operation(lock, directoryGuard);
      } catch (error) {
        primaryError = wrapExternalError(error, "not-renamed");
      }
      if (lock) {
        try {
          await lock.release();
        } catch {
          const reportedCommitState = ownDataProperty(primaryError, "commitState");
          const commitState = ["not-committed", "uncertain"].includes(reportedCommitState)
            ? reportedCommitState
            : mutation && outcome?.committed
              ? "committed"
              : "not-committed";
          primaryError = new AuthStateStoreError(
            "lock_release_failed",
            "auth state lock release failed after the operation",
            {
              commitState,
              retryable: false,
            },
          );
        }
      }
      await directoryGuard.handle.close().catch(() => {});
      if (primaryError) throw primaryError;
      return outcome.value;
    });
  }

  async #openDirectoryGuard() {
    ensure(
      typeof constants.O_DIRECTORY === "number" && typeof constants.O_NOFOLLOW === "number",
      "invalid_store_directory",
      "secure directory open flags are unavailable",
    );
    let path;
    let handle;
    try {
      path = await this.realPath(this.directory);
      ensure(
        path === this.directory,
        "invalid_store_directory",
        "auth state directory canonical path changed",
      );
      handle = await this.openFile(
        path,
        constants.O_RDONLY | constants.O_DIRECTORY | constants.O_NOFOLLOW,
      );
      const identity = await handle.stat({ bigint: true });
      ensure(
        sameFileIdentity(identity, this.#directoryIdentity),
        "invalid_store_directory",
        "auth state directory identity changed",
      );
      const directoryGuard = {
        aclCache: new Map(),
        ancestorAclCacheKeys: new Set(),
        handle,
        identity,
        path,
      };
      await this.#assertDirectoryMetadataSafe(directoryGuard, path, identity);
      await this.#assertDirectoryAncestorsSafe(directoryGuard, path, identity.uid);
      await this.#assertDirectoryCurrent(directoryGuard);
      return directoryGuard;
    } catch {
      await handle?.close().catch(() => {});
      fail("invalid_store_directory", "auth state directory is unavailable or unsafe");
    }
  }

  #invalidateAncestorAclCache(directoryGuard) {
    for (const cacheKey of directoryGuard.ancestorAclCacheKeys) {
      directoryGuard.aclCache.delete(cacheKey);
    }
    directoryGuard.ancestorAclCacheKeys.clear();
  }

  async #proveDirectoryDurable(lock, directoryGuard) {
    this.#invalidateAncestorAclCache(directoryGuard);
    await this.#assertDirectoryCurrent(directoryGuard);
    await lock.assertHeld();
    await this.syncDirectory({ handle: directoryGuard.handle, path: directoryGuard.path });
    await this.#assertDirectoryCurrent(directoryGuard);
    await lock.assertHeld();
  }

  async #assertAclSafe({
    cacheKey,
    directoryGuard,
    includeChangeTime,
    inspect,
    metadata,
    path,
    unsafeMessage,
  }) {
    const signature = aclMetadataSignature(metadata, { includeChangeTime });
    if (directoryGuard.aclCache.get(cacheKey) === signature) return;
    let hasUnsafeAcl;
    let currentMetadata;
    try {
      hasUnsafeAcl = await inspect(path);
      currentMetadata = await this.readPathMetadata(path, { bigint: true });
    } catch {
      fail("invalid_store_directory", `${unsafeMessage} could not be verified`);
    }
    ensure(
      aclMetadataSignature(currentMetadata, { includeChangeTime }) === signature,
      "invalid_store_directory",
      "auth state directory policy metadata changed during ACL inspection",
    );
    ensure(hasUnsafeAcl === false, "invalid_store_directory", unsafeMessage);
    directoryGuard.aclCache.set(cacheKey, signature);
  }

  async #assertDirectoryMetadataSafe(directoryGuard, path, metadata) {
    const brokerUid = currentBrokerUid();
    ensure(
      authorityDirectoryPermissionsAreSafe(
        {
          isDirectory: metadata.isDirectory(),
          mode: metadata.mode,
          uid: metadata.uid,
        },
        {
          brokerUid,
          childUid: metadata.uid,
          disallowedModeBits: 0o077,
          requiredModeBits: 0o700,
        },
      ),
      "invalid_store_directory",
      "auth state directory permissions are unsafe",
    );
    await this.#assertAclSafe({
      cacheKey: JSON.stringify(["directory", path]),
      directoryGuard,
      includeChangeTime: true,
      inspect: this.inspectExtendedAcl,
      metadata,
      path,
      unsafeMessage: "auth state directory has an extended ACL",
    });
  }

  async #assertDirectoryAncestorsSafe(directoryGuard, path, initialChildUid) {
    const brokerUid = currentBrokerUid();
    let childUid = initialChildUid;
    let ancestor = dirname(path);
    while (true) {
      let metadata;
      try {
        metadata = await this.readPathMetadata(ancestor, { bigint: true });
      } catch {
        fail("invalid_store_directory", "auth state directory ancestor could not be verified");
      }
      ensure(
        authorityDirectoryPermissionsAreSafe(
          {
            isDirectory: metadata.isDirectory(),
            mode: metadata.mode,
            uid: metadata.uid,
          },
          {
            allowRootOwner: true,
            allowStickyShared: true,
            brokerUid,
            childUid,
            disallowedModeBits: 0o022,
          },
        ),
        "invalid_store_directory",
        "auth state directory ancestor is unsafe",
      );
      const cacheKey = JSON.stringify(["ancestor", ancestor]);
      await this.#assertAclSafe({
        cacheKey,
        directoryGuard,
        includeChangeTime: false,
        inspect: this.inspectAncestorAcl,
        metadata,
        path: ancestor,
        unsafeMessage: "auth state directory ancestor is unsafe",
      });
      directoryGuard.ancestorAclCacheKeys.add(cacheKey);
      if (ancestor === dirname(ancestor)) return;
      childUid = metadata.uid;
      ancestor = dirname(ancestor);
    }
  }

  async #assertDirectoryCurrent(directoryGuard) {
    let currentHandle;
    try {
      const heldMetadata = await directoryGuard.handle.stat({ bigint: true });
      currentHandle = await this.openFile(
        directoryGuard.path,
        constants.O_RDONLY | constants.O_DIRECTORY | constants.O_NOFOLLOW,
      );
      const currentMetadata = await currentHandle.stat({ bigint: true });
      ensure(
        sameFileIdentity(heldMetadata, directoryGuard.identity) &&
          sameFileIdentity(currentMetadata, directoryGuard.identity),
        "invalid_store_directory",
        "auth state directory identity changed",
      );
      await this.#assertDirectoryMetadataSafe(
        directoryGuard,
        directoryGuard.path,
        currentMetadata,
      );
      await this.#assertDirectoryAncestorsSafe(
        directoryGuard,
        directoryGuard.path,
        currentMetadata.uid,
      );
    } catch (error) {
      if (error instanceof AuthStateStoreError) throw error;
      fail("invalid_store_directory", "auth state directory identity could not be verified");
    } finally {
      await currentHandle?.close().catch(() => {});
    }
  }

  async #assertNoRecoveryArtifacts(directoryGuard) {
    await this.#assertDirectoryCurrent(directoryGuard);
    let entries;
    try {
      entries = await this.readDirectory(directoryGuard.path);
    } catch {
      fail("auth_state_io_failed", "auth state directory could not be inspected");
    }
    await this.#assertDirectoryCurrent(directoryGuard);
    ensure(
      !entries.some((entry) => entry.startsWith(NEXT_PREFIX)),
      "recovery_required",
      "auth state directory contains an unresolved promotion candidate",
    );
  }

  async #readCanonical({ allowMissing, directoryGuard }) {
    await this.#assertDirectoryCurrent(directoryGuard);
    let handle;
    let raw;
    try {
      handle = await this.openFile(
        join(directoryGuard.path, CANONICAL_FILE),
        constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK,
      );
      const metadata = await handle.stat({ bigint: true });
      ensure(
        metadata.isFile() && metadata.nlink === 1n && (metadata.mode & 0o077n) === 0n,
        "invalid_auth_state",
        "auth state file is not a protected regular file",
      );
      const brokerUid = currentBrokerUid();
      if (brokerUid !== null) {
        ensure(
          metadata.uid === brokerUid,
          "invalid_auth_state",
          "auth state file must be owned by the broker",
        );
      }
      ensure(
        metadata.size <= BigInt(MAX_ENVELOPE_BYTES),
        "invalid_auth_state",
        "auth state envelope is too large",
      );
      raw = await handle.readFile("utf8");
    } catch (error) {
      if (allowMissing && ownDataProperty(error, "code") === "ENOENT") {
        await this.#assertDirectoryCurrent(directoryGuard);
        return null;
      }
      if (error instanceof AuthStateStoreError) throw error;
      if (ownDataProperty(error, "code") === "ELOOP") {
        fail("invalid_auth_state", "auth state canonical path must not be a symbolic link");
      }
      fail("auth_state_io_failed", "auth state file could not be read");
    } finally {
      await handle?.close().catch(() => {});
    }
    await this.#assertDirectoryCurrent(directoryGuard);
    return this.#decryptEnvelope(raw);
  }

  async #activeKey() {
    let active;
    try {
      active = await this.keyProvider.activeKey();
    } catch {
      fail("key_unavailable", "active auth state key is unavailable", { retryable: true });
    }
    ensure(
      active !== null && typeof active === "object" && !Array.isArray(active),
      "invalid_key_provider",
      "active key response is invalid",
    );
    validateOpaqueId(active.keyId, KEY_ID_PATTERN, "invalid_key_id", "key ID");
    return { keyId: active.keyId, key: validateSecretKey(active.key) };
  }

  async #keyById(keyId) {
    let key;
    try {
      key = await this.keyProvider.getKey(keyId);
    } catch {
      fail("key_unavailable", "auth state key is unavailable", { retryable: true });
    }
    ensure(key !== undefined && key !== null, "key_unavailable", "auth state key is unavailable", {
      retryable: true,
    });
    return validateSecretKey(key);
  }

  async #encryptRecord(record) {
    const { keyId, key } = await this.#activeKey();
    const header = {
      formatVersion: FORMAT_VERSION,
      algorithm: ALGORITHM,
      authorityId: this.authorityId,
      keyId,
    };
    const nonce = this.randomBytesFn(12);
    ensure(
      Buffer.isBuffer(nonce) && nonce.length === 12,
      "invalid_random_source",
      "auth state nonce source is invalid",
    );
    const plaintext = Buffer.from(canonicalJson(record), "utf8");
    try {
      const cipher = createCipheriv("aes-256-gcm", key, nonce, { authTagLength: 16 });
      cipher.setAAD(Buffer.from(canonicalJson(header), "utf8"));
      const ciphertext = Buffer.concat([cipher.update(plaintext), cipher.final()]);
      const envelope = {
        formatVersion: FORMAT_VERSION,
        algorithm: ALGORITHM,
        authorityId: this.authorityId,
        keyId,
        nonce: nonce.toString("base64url"),
        ciphertext: ciphertext.toString("base64url"),
        tag: cipher.getAuthTag().toString("base64url"),
      };
      const raw = canonicalJson(envelope, true);
      ensure(
        Buffer.byteLength(raw, "utf8") <= MAX_ENVELOPE_BYTES,
        "invalid_payload",
        "auth state payload produces an oversized encrypted envelope",
      );
      return { envelope, raw };
    } finally {
      plaintext.fill(0);
    }
  }

  async #decryptEnvelope(raw) {
    const envelope = parseCanonicalJson(
      raw,
      ENVELOPE_FIELDS,
      "invalid_auth_state",
      "auth state envelope",
      true,
    );
    ensure(
      envelope.formatVersion === FORMAT_VERSION && envelope.algorithm === ALGORITHM,
      "unsupported_auth_state",
      "auth state format is unsupported",
    );
    validateOpaqueId(envelope.authorityId, AUTHORITY_ID_PATTERN, "invalid_auth_state", "authority ID");
    ensure(
      envelope.authorityId === this.authorityId,
      "authority_binding_mismatch",
      "auth state belongs to a different authority",
    );
    validateOpaqueId(envelope.keyId, KEY_ID_PATTERN, "invalid_auth_state", "key ID");
    const nonce = decodeBase64Url(envelope.nonce, 12, "auth state nonce");
    const tag = decodeBase64Url(envelope.tag, 16, "auth state tag");
    ensure(
      typeof envelope.ciphertext === "string" && /^[A-Za-z0-9_-]+$/u.test(envelope.ciphertext),
      "invalid_auth_state",
      "auth state ciphertext is invalid",
    );
    const ciphertext = Buffer.from(envelope.ciphertext, "base64url");
    ensure(
      ciphertext.length > 0 && ciphertext.toString("base64url") === envelope.ciphertext,
      "invalid_auth_state",
      "auth state ciphertext is invalid",
    );
    const key = await this.#keyById(envelope.keyId);
    let plaintext;
    try {
      const decipher = createDecipheriv("aes-256-gcm", key, nonce, { authTagLength: 16 });
      decipher.setAAD(
        Buffer.from(
          canonicalJson({
            formatVersion: envelope.formatVersion,
            algorithm: envelope.algorithm,
            authorityId: envelope.authorityId,
            keyId: envelope.keyId,
          }),
          "utf8",
        ),
      );
      decipher.setAuthTag(tag);
      plaintext = Buffer.concat([decipher.update(ciphertext), decipher.final()]);
    } catch {
      fail("auth_state_integrity_failed", "auth state authentication failed");
    }
    try {
      const rawRecord = plaintext.toString("utf8");
      ensure(
        Buffer.from(rawRecord, "utf8").equals(plaintext),
        "invalid_auth_state",
        "auth state plaintext is not exact UTF-8",
      );
      const record = parseCanonicalJson(
        rawRecord,
        RECORD_FIELDS,
        "invalid_auth_state",
        "auth state record",
      );
      ensure(
        record.recordVersion === RECORD_VERSION,
        "unsupported_auth_state",
        "auth state record format is unsupported",
      );
      const generation = parseGeneration(record.generation, { allowZero: false });
      const baseGeneration = parseGeneration(record.baseGeneration);
      ensure(
        baseGeneration < MAX_GENERATION && baseGeneration + 1n === generation,
        "invalid_generation",
        "auth state base generation is inconsistent",
      );
      validateOpaqueId(record.commitId, AUTHORITY_ID_PATTERN, "invalid_auth_state", "commit ID");
      ensure(
        ["compare-and-swap", "rotate-encryption"].includes(record.operation),
        "invalid_auth_state",
        "auth state operation is invalid",
      );
      validatePayload(record.payload);
      return { envelope, record };
    } finally {
      plaintext.fill(0);
    }
  }

  async #commit(record, lock, directoryGuard) {
    const encrypted = await this.#encryptRecord(record);
    const suffix = this.randomBytesFn(12);
    ensure(
      Buffer.isBuffer(suffix) && suffix.length === 12,
      "invalid_random_source",
      "auth state temporary-name source is invalid",
    );
    const canonicalPath = join(directoryGuard.path, CANONICAL_FILE);
    const temporary = join(directoryGuard.path, `${NEXT_PREFIX}${suffix.toString("hex")}`);
    let handle;
    let phase = "not-renamed";
    try {
      this.#invalidateAncestorAclCache(directoryGuard);
      await this.#assertDirectoryCurrent(directoryGuard);
      handle = await this.openFile(
        temporary,
        constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW,
        0o600,
      );
      await handle.chmod(0o600);
      await handle.writeFile(encrypted.raw, "utf8");
      await handle.sync();
      await handle.close();
      handle = undefined;
      await this.faults.afterTempSync?.({ temporary });
      this.#invalidateAncestorAclCache(directoryGuard);
      await this.#assertDirectoryCurrent(directoryGuard);
      await lock.assertHeld();
      phase = "rename-attempted";
      await lock.renameWhileHeld(temporary, canonicalPath);
      phase = "renamed";
      await this.faults.afterRename?.({ canonicalPath });
      this.#invalidateAncestorAclCache(directoryGuard);
      await this.#assertDirectoryCurrent(directoryGuard);
      await lock.assertHeld();
      await this.syncDirectory({ handle: directoryGuard.handle, path: directoryGuard.path });
      phase = "synced";
      await this.faults.afterDirectorySync?.({ canonicalPath });
      await this.#assertDirectoryCurrent(directoryGuard);
      await lock.assertHeld();
      const committed = await this.#readCanonical({ allowMissing: false, directoryGuard });
      ensure(
        committed.record.generation === record.generation &&
          committed.record.baseGeneration === record.baseGeneration &&
          committed.record.commitId === record.commitId &&
          committed.record.operation === record.operation &&
          committed.record.payload === record.payload &&
          committed.envelope.keyId === encrypted.envelope.keyId,
        "commit_verification_failed",
        "committed auth state failed readback verification",
        { commitState: "uncertain" },
      );
      await this.#assertDirectoryCurrent(directoryGuard);
      return committed;
    } catch (error) {
      throw wrapExternalError(error, phase);
    } finally {
      await handle?.close().catch(() => {});
    }
  }
}
