import { createHash } from "node:crypto";
import { constants as fsConstants } from "node:fs";
import {
  chmod,
  lstat,
  mkdir,
  open,
  realpath,
  statfs,
} from "node:fs/promises";
import { basename, dirname, isAbsolute, join, resolve } from "node:path";

import { acquireAdvisoryLock } from "./advisory-lock.mjs";
import {
  FilesystemOperationJournal,
  OperationJournalError,
} from "./filesystem-operation-journal.mjs";
import {
  assertCheckpointDescriptor,
  assertStorageMutationRequest,
  assertStorageMutationResult,
} from "./session-storage-contracts.mjs";
import {
  copyStoppedTreeBetweenRoots,
  digestTree,
  openStoppedTreeRootAuthority,
  sameFileIdentity,
  syncStoppedTree,
} from "./stopped-tree.mjs";

export const STOPPED_DIRECTORY_ARTIFACT_VERSION = 1;

const PUBLICATION_CONTRACT_VERSION = 1;
const ARTIFACT_FORMAT = "portable-codex-stopped-directory";
const LOCAL_DURABILITY_PROFILE = "local-fsync-rename";
const OPAQUE_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u;
const DIGEST_PATTERN = /^[0-9a-f]{64}$/u;
const COMMIT_STATES = new Set(["committed", "not-committed", "uncertain"]);
const internalErrors = new WeakSet();
const publicationQueues = new Map();

const ERROR_MESSAGES = Object.freeze({
  invalid_publication_request: "Stopped-directory publication request is invalid",
  unsupported_publication_filesystem:
    "Stopped-directory publication filesystem is unsupported",
  publication_conflict: "Stopped-directory publication conflicts with durable state",
  publication_integrity_failed: "Stopped-directory publication integrity check failed",
  publication_recovery_required: "Stopped-directory publication requires recovery",
  publication_io_failed: "Stopped-directory publication failed before commit",
  publication_outcome_uncertain: "Stopped-directory publication outcome is uncertain",
  publication_lock_release_failed:
    "Stopped-directory publication lock release failed",
  published_state_invalid: "Committed stopped-directory publication is invalid",
});

const FAULT_NAMES = Object.freeze([
  "afterJournalPrepared",
  "afterSourceBarrier",
  "afterCandidateCreated",
  "afterCopy",
  "afterCandidateBarrier",
  "afterMaterialized",
  "beforeRename",
  "afterRename",
  "afterParentSync",
  "afterFinalReadback",
  "beforeCommit",
]);

const SUPPORTED_LINUX_FILESYSTEM_TYPES = new Set([
  0x2fc12fc1n, // zfs
  0x58465342n, // xfs
  0x9123683en, // btrfs
  0xef53n, // ext2/3/4
  0xf2f52010n, // f2fs
]);

export class StoppedDirectoryPublicationError extends Error {
  constructor(code, commitState = "not-committed") {
    if (!Object.hasOwn(ERROR_MESSAGES, code) || !COMMIT_STATES.has(commitState)) {
      throw new TypeError("unsupported stopped-directory publication error");
    }
    super(ERROR_MESSAGES[code]);
    this.name = "StoppedDirectoryPublicationError";
    this.code = code;
    this.commitState = commitState;
    this.retryable = false;
    Object.freeze(this);
  }
}

function createPublicationError(code, commitState = "not-committed") {
  const error = new StoppedDirectoryPublicationError(code, commitState);
  internalErrors.add(error);
  return error;
}

function fail(code, commitState = "not-committed") {
  throw createPublicationError(code, commitState);
}

function ensure(condition, code, commitState = "not-committed") {
  if (!condition) fail(code, commitState);
}

function ownEnumerableObject(value, code = "invalid_publication_request") {
  ensure(value !== null && typeof value === "object" && !Array.isArray(value), code);
  let descriptors;
  try {
    descriptors = Object.getOwnPropertyDescriptors(value);
  } catch {
    fail(code);
  }
  const normalized = Object.create(null);
  for (const key of Reflect.ownKeys(descriptors)) {
    ensure(typeof key === "string", code);
    const descriptor = descriptors[key];
    ensure(
      descriptor.enumerable === true &&
        Object.hasOwn(descriptor, "value") &&
        !Object.hasOwn(descriptor, "get") &&
        !Object.hasOwn(descriptor, "set"),
      code,
    );
    Object.defineProperty(normalized, key, {
      configurable: true,
      enumerable: true,
      value: descriptor.value,
      writable: true,
    });
  }
  return Object.freeze(normalized);
}

function exactOptions(value, allowed, required = allowed) {
  const options = ownEnumerableObject(value);
  const keys = Object.keys(options);
  ensure(keys.every((key) => allowed.includes(key)), "invalid_publication_request");
  ensure(required.every((key) => Object.hasOwn(options, key)), "invalid_publication_request");
  return options;
}

function normalizeFaults(value = {}) {
  const faults = exactOptions(value, FAULT_NAMES, []);
  return Object.freeze(
    Object.fromEntries(
      FAULT_NAMES.map((name) => {
        const operation = faults[name] ?? (async () => {});
        ensure(typeof operation === "function", "invalid_publication_request");
        return [name, operation];
      }),
    ),
  );
}

async function runFault(operation) {
  try {
    await operation();
  } catch {
    fail("publication_io_failed");
  }
}

async function assertUntrustedLockHeld(lock) {
  try {
    await lock.assertHeld();
  } catch {
    fail("publication_io_failed");
  }
}

function assertOpaqueId(value) {
  ensure(
    typeof value === "string" && OPAQUE_ID_PATTERN.test(value),
    "invalid_publication_request",
  );
  return value;
}

function assertDirectName(value) {
  ensure(
    typeof value === "string" &&
      value.length > 0 &&
      value !== "." &&
      value !== ".." &&
      basename(value) === value,
    "invalid_publication_request",
  );
  return value;
}

function sha256(...values) {
  const hash = createHash("sha256");
  for (const value of values) hash.update(value, "utf8");
  return hash.digest("hex");
}

export function stoppedDirectoryPublicationCandidateName(operationId, finalName) {
  const operation = assertOpaqueId(operationId);
  const destination = assertDirectName(finalName);
  return `.publication-${sha256(
    "portable-codex-stopped-directory-candidate\0",
    operation,
    "\0",
    destination,
  )}.stage`;
}

function publicationId(operationId, finalName) {
  return `publication-sha256-${sha256(
    "portable-codex-stopped-directory-publication\0",
    operationId,
    "\0",
    finalName,
  )}`;
}

function runPublicationQueued(key, operation) {
  const previous = publicationQueues.get(key) ?? Promise.resolve();
  const current = previous.catch(() => {}).then(operation);
  publicationQueues.set(key, current);
  return current.finally(() => {
    if (publicationQueues.get(key) === current) publicationQueues.delete(key);
  });
}

async function publicationQueueKey(root) {
  ensure(
    typeof root === "string" && isAbsolute(root) && resolve(root) === root,
    "invalid_publication_request",
  );
  try {
    const path = await realpath(root);
    const identity = await lstat(path, { bigint: true });
    ensure(identity.isDirectory(), "invalid_publication_request");
    return `${identity.dev.toString()}\0${identity.ino.toString()}`;
  } catch (error) {
    if (internalErrors.has(error)) throw error;
    fail("invalid_publication_request");
  }
}

function pathIsInside(root, candidate) {
  return candidate.startsWith(`${root}/`);
}

function pathIsAtOrInside(root, candidate) {
  return candidate === root || pathIsInside(root, candidate);
}

function rootIdentity(authority) {
  return Object.freeze({
    device: authority.identity.dev.toString(),
    inode: authority.identity.ino.toString(),
  });
}

async function inspectDirectDirectory(authority, value, { mustExist = true } = {}) {
  ensure(
    typeof value === "string" && isAbsolute(value) && resolve(value) === value,
    "invalid_publication_request",
  );
  let parent;
  try {
    parent = await realpath(dirname(value));
  } catch {
    fail("invalid_publication_request");
  }
  ensure(parent === authority.path, "invalid_publication_request");
  const path = join(parent, assertDirectName(basename(value)));
  let identity;
  try {
    identity = await lstat(path, { bigint: true });
  } catch (error) {
    if (!mustExist && error?.code === "ENOENT") {
      await authority.assertCurrent().catch(() => fail("invalid_publication_request"));
      return Object.freeze({ identity: null, name: basename(path), path });
    }
    fail("invalid_publication_request");
  }
  ensure(
    identity.isDirectory() && !identity.isSymbolicLink(),
    "invalid_publication_request",
  );
  try {
    await authority.assertCurrent();
  } catch {
    fail("invalid_publication_request");
  }
  return Object.freeze({ identity, name: basename(path), path });
}

async function inspectPath(
  path,
  code = "publication_io_failed",
  commitState = "not-committed",
) {
  try {
    return await lstat(path, { bigint: true });
  } catch (error) {
    if (error?.code === "ENOENT") return null;
    fail(code, commitState);
  }
}

async function assertPathAbsent(
  path,
  code = "publication_recovery_required",
  commitState = "not-committed",
) {
  ensure((await inspectPath(path, code, commitState)) === null, code, commitState);
}

async function defaultInspectFilesystem(path) {
  let info;
  try {
    info = await statfs(path, { bigint: true });
  } catch {
    fail("unsupported_publication_filesystem");
  }
  const type = BigInt.asUintN(32, info.type);
  const supported =
    (process.platform === "darwin" && type === 0x1an) ||
    (process.platform === "linux" && SUPPORTED_LINUX_FILESYSTEM_TYPES.has(type));
  ensure(supported, "unsupported_publication_filesystem");
  return Object.freeze({
    durability: LOCAL_DURABILITY_PROFILE,
    type: `statfs-0x${type.toString(16)}`,
  });
}

async function inspectFilesystem(
  inspector,
  path,
  commitState = "not-committed",
) {
  let raw;
  try {
    raw = await inspector(path);
  } catch {
    fail("unsupported_publication_filesystem", commitState);
  }
  try {
    return normalizeFilesystemProfile(raw);
  } catch {
    fail("unsupported_publication_filesystem", commitState);
  }
}

function normalizeFilesystemProfile(raw) {
  const profile = exactOptions(raw, ["durability", "type"]);
  ensure(
    profile.durability === LOCAL_DURABILITY_PROFILE &&
      typeof profile.type === "string" &&
      /^[A-Za-z0-9][A-Za-z0-9._:-]{0,63}$/u.test(profile.type),
    "unsupported_publication_filesystem",
  );
  return Object.freeze({ durability: profile.durability, type: profile.type });
}

function validateResult(request, value) {
  const result = exactOptions(value, ["checkpoint", "mutation"]);
  let checkpoint;
  let mutation;
  try {
    checkpoint = assertCheckpointDescriptor(result.checkpoint);
    mutation = assertStorageMutationResult(result.mutation, { request });
  } catch {
    fail("invalid_publication_request");
  }
  return Object.freeze({ checkpoint, mutation });
}

function normalizeArtifactProof(value) {
  const proof = exactOptions(value, [
    "artifactManifestDigest",
    "captureOperationId",
    "modeledDigest",
  ]);
  ensure(
    typeof proof.artifactManifestDigest === "string" &&
      DIGEST_PATTERN.test(proof.artifactManifestDigest) &&
      typeof proof.captureOperationId === "string" &&
      OPAQUE_ID_PATTERN.test(proof.captureOperationId) &&
      typeof proof.modeledDigest === "string" &&
      DIGEST_PATTERN.test(proof.modeledDigest),
    "invalid_publication_request",
  );
  return Object.freeze({
    artifactManifestDigest: proof.artifactManifestDigest,
    captureOperationId: proof.captureOperationId,
    modeledDigest: proof.modeledDigest,
  });
}

function artifactManifest(checkpoint, operationId, modeledDigest) {
  return Object.freeze({
    format: ARTIFACT_FORMAT,
    formatVersion: STOPPED_DIRECTORY_ARTIFACT_VERSION,
    digestAlgorithm: "sha256",
    payloadKind: "portable-stopped-tree",
    captureOperationId: operationId,
    modeledDigest,
    checkpoint,
  });
}

function manifestBytes(manifest) {
  return Buffer.from(`${JSON.stringify(manifest)}\n`, "utf8");
}

function sameCanonicalValue(left, right) {
  try {
    return JSON.stringify(left) === JSON.stringify(right);
  } catch {
    return false;
  }
}

async function writeArtifactManifest(path, manifest) {
  const bytes = manifestBytes(manifest);
  let handle;
  try {
    handle = await open(
      path,
      fsConstants.O_WRONLY |
        fsConstants.O_CREAT |
        fsConstants.O_EXCL |
        fsConstants.O_NOFOLLOW,
      0o600,
    );
    await handle.writeFile(bytes);
    await handle.chmod(0o600);
    await handle.sync();
    const identity = await handle.stat({ bigint: true });
    ensure(
      identity.isFile() && identity.nlink === 1n && Number(identity.mode & 0o777n) === 0o600,
      "publication_integrity_failed",
    );
    return sha256(bytes.toString("utf8"));
  } catch (error) {
    if (internalErrors.has(error)) throw error;
    fail("publication_io_failed");
  } finally {
    if (handle) {
      try {
        await handle.close();
      } catch {
        fail("publication_io_failed");
      }
    }
  }
}

async function createPrivateDirectory(path) {
  let handle;
  let identity;
  try {
    await mkdir(path, { mode: 0o700 });
    identity = await lstat(path, { bigint: true });
    ensure(
      identity.isDirectory() && !identity.isSymbolicLink(),
      "publication_io_failed",
    );
    await chmod(path, 0o700);
    ensure(
      sameFileIdentity(identity, await lstat(path, { bigint: true })),
      "publication_io_failed",
    );
    handle = await open(
      path,
      fsConstants.O_RDONLY | fsConstants.O_DIRECTORY | fsConstants.O_NOFOLLOW,
    );
    ensure(
      sameFileIdentity(identity, await handle.stat({ bigint: true })),
      "publication_io_failed",
    );
    await handle.chmod(0o700);
    const held = await handle.stat({ bigint: true });
    const current = await lstat(path, { bigint: true });
    ensure(
      sameFileIdentity(identity, held) &&
        sameFileIdentity(identity, current) &&
        Number(held.mode & 0o777n) === 0o700,
      "publication_io_failed",
    );
  } catch (error) {
    if (internalErrors.has(error)) throw error;
    fail("publication_io_failed");
  } finally {
    if (handle) {
      try {
        await handle.close();
      } catch {
        fail("publication_io_failed");
      }
    }
  }
}

async function readArtifactManifest(path, expectedCheckpoint, expectedProof) {
  let metadata;
  let bytes;
  let handle;
  try {
    metadata = await lstat(path, { bigint: true });
    ensure(
      metadata.isFile() &&
        !metadata.isSymbolicLink() &&
        metadata.nlink === 1n &&
        Number(metadata.mode & 0o777n) === 0o600 &&
        metadata.size <= 512n * 1024n,
      "publication_integrity_failed",
    );
    handle = await open(path, fsConstants.O_RDONLY | fsConstants.O_NOFOLLOW);
    ensure(
      sameFileIdentity(metadata, await handle.stat({ bigint: true })),
      "publication_integrity_failed",
    );
    bytes = await handle.readFile();
    const current = await handle.stat({ bigint: true });
    ensure(
      sameFileIdentity(metadata, current) &&
        metadata.size === current.size &&
        metadata.mtimeNs === current.mtimeNs &&
        metadata.ctimeNs === current.ctimeNs,
      "publication_integrity_failed",
    );
  } catch (error) {
    if (internalErrors.has(error)) throw error;
    fail("publication_integrity_failed");
  } finally {
    if (handle) {
      try {
        await handle.close();
      } catch {
        fail("publication_integrity_failed");
      }
    }
  }
  let parsed;
  try {
    ensure(bytes.at(-1) === 0x0a, "publication_integrity_failed");
    parsed = JSON.parse(bytes.subarray(0, -1).toString("utf8"));
  } catch (error) {
    if (internalErrors.has(error)) throw error;
    fail("publication_integrity_failed");
  }
  let manifest;
  try {
    manifest = exactOptions(
      parsed,
      [
        "format",
        "formatVersion",
        "digestAlgorithm",
        "payloadKind",
        "captureOperationId",
        "modeledDigest",
        "checkpoint",
      ],
    );
  } catch {
    fail("publication_integrity_failed");
  }
  ensure(
    manifest.format === ARTIFACT_FORMAT &&
      manifest.formatVersion === STOPPED_DIRECTORY_ARTIFACT_VERSION &&
      manifest.digestAlgorithm === "sha256" &&
      manifest.payloadKind === "portable-stopped-tree" &&
      OPAQUE_ID_PATTERN.test(manifest.captureOperationId) &&
      typeof manifest.modeledDigest === "string" &&
      DIGEST_PATTERN.test(manifest.modeledDigest) &&
      sameCanonicalValue(manifest.checkpoint, expectedCheckpoint),
    "publication_integrity_failed",
  );
  const canonicalManifest = artifactManifest(
    expectedCheckpoint,
    manifest.captureOperationId,
    manifest.modeledDigest,
  );
  ensure(
    manifestBytes(canonicalManifest).equals(bytes),
    "publication_integrity_failed",
  );
  const digest = sha256(bytes.toString("utf8"));
  if (expectedProof !== undefined) {
    ensure(
      manifest.captureOperationId === expectedProof.captureOperationId &&
        manifest.modeledDigest === expectedProof.modeledDigest &&
        digest === expectedProof.artifactManifestDigest,
      "publication_integrity_failed",
    );
  }
  return Object.freeze({
    digest,
    manifest: canonicalManifest,
  });
}

function validateMaterialization(
  value,
  { artifactProof, kind, operationId, finalName },
) {
  const materialization = exactOptions(value, [
    "contractVersion",
    "artifactManifestDigest",
    "modeledDigest",
    "publicationId",
    "publicationKind",
    "stagedRoot",
  ]);
  const stagedRoot = exactOptions(materialization.stagedRoot, ["device", "inode"]);
  ensure(
    materialization.contractVersion === PUBLICATION_CONTRACT_VERSION &&
      materialization.publicationKind === kind &&
      materialization.publicationId === publicationId(operationId, finalName) &&
      typeof materialization.modeledDigest === "string" &&
      DIGEST_PATTERN.test(materialization.modeledDigest) &&
      typeof materialization.artifactManifestDigest === "string" &&
      DIGEST_PATTERN.test(materialization.artifactManifestDigest) &&
      (kind !== "restore-destination" ||
        (artifactProof !== null &&
          materialization.artifactManifestDigest ===
            artifactProof.artifactManifestDigest &&
          materialization.modeledDigest === artifactProof.modeledDigest)) &&
      typeof stagedRoot.device === "string" &&
      /^(?:0|[1-9][0-9]*)$/u.test(stagedRoot.device) &&
      typeof stagedRoot.inode === "string" &&
      /^[1-9][0-9]*$/u.test(stagedRoot.inode),
    "publication_integrity_failed",
  );
  return materialization;
}

function materializationFor({
  artifactManifestDigest,
  finalName,
  identity,
  kind,
  modeledDigest,
  operationId,
}) {
  return Object.freeze({
    contractVersion: PUBLICATION_CONTRACT_VERSION,
    artifactManifestDigest,
    modeledDigest,
    publicationId: publicationId(operationId, finalName),
    publicationKind: kind,
    stagedRoot: Object.freeze({
      device: identity.dev.toString(),
      inode: identity.ino.toString(),
    }),
  });
}

function identityMatchesMaterialization(identity, materialization) {
  return (
    identity.dev.toString() === materialization.stagedRoot.device &&
    identity.ino.toString() === materialization.stagedRoot.inode
  );
}

async function openPinnedDirectory(path, materialization, committed = false) {
  let metadata;
  let handle;
  try {
    metadata = await lstat(path, { bigint: true });
    ensure(
      metadata.isDirectory() &&
        !metadata.isSymbolicLink() &&
        identityMatchesMaterialization(metadata, materialization),
      committed ? "published_state_invalid" : "publication_recovery_required",
      committed ? "committed" : "not-committed",
    );
    handle = await open(
      path,
      fsConstants.O_RDONLY | fsConstants.O_DIRECTORY | fsConstants.O_NOFOLLOW,
    );
    ensure(
      sameFileIdentity(metadata, await handle.stat({ bigint: true })),
      committed ? "published_state_invalid" : "publication_recovery_required",
      committed ? "committed" : "not-committed",
    );
    return { handle, identity: metadata };
  } catch (error) {
    if (handle) await handle.close().catch(() => {});
    if (internalErrors.has(error)) throw error;
    fail(
      committed ? "published_state_invalid" : "publication_recovery_required",
      committed ? "committed" : "not-committed",
    );
  }
}

async function assertPinnedPath(path, pinned, code, commitState) {
  let current;
  let held;
  try {
    [current, held] = await Promise.all([
      lstat(path, { bigint: true }),
      pinned.handle.stat({ bigint: true }),
    ]);
  } catch {
    fail(code, commitState);
  }
  ensure(
    current.isDirectory() &&
      sameFileIdentity(current, pinned.identity) &&
      sameFileIdentity(held, pinned.identity),
    code,
    commitState,
  );
}

function normalizeJournalError(
  error,
  {
    finalJournalCommitAttempted,
    historicalCommitConfirmed,
    publicationMayHaveOccurred,
  },
) {
  if (!(error instanceof OperationJournalError)) return null;
  const commitState =
    historicalCommitConfirmed ||
    (publicationMayHaveOccurred &&
    finalJournalCommitAttempted &&
    error.commitState === "committed")
      ? "committed"
      : publicationMayHaveOccurred
        ? "uncertain"
        : "not-committed";
  if (error.code === "operation_conflict") {
    return createPublicationError("publication_conflict", commitState);
  }
  if (error.code === "journal_recovery_required") {
    return createPublicationError(
      "publication_recovery_required",
      commitState,
    );
  }
  if (error.code === "journal_lock_release_failed") {
    return createPublicationError(
      "publication_lock_release_failed",
      commitState,
    );
  }
  if (commitState === "uncertain") {
    return createPublicationError(
      "publication_outcome_uncertain",
      commitState,
    );
  }
  if (["invalid_journal_request", "invalid_state_transition"].includes(error.code)) {
    return createPublicationError(
      "invalid_publication_request",
      commitState,
    );
  }
  return createPublicationError("publication_io_failed", commitState);
}

function frozenOutcome(outcome) {
  return Object.freeze({
    materialization: outcome.record.materialization,
    replayed: outcome.replayed,
    result: outcome.record.result,
  });
}

export class StoppedDirectoryPublication {
  #acquireLock;
  #faults;
  #inspectFilesystem;
  #journal;
  #trustRenameOutcome;

  constructor(options) {
    const normalized = exactOptions(
      options,
      ["acquireLock", "faults", "inspectFilesystem", "journal"],
      ["journal"],
    );
    ensure(
      normalized.journal instanceof FilesystemOperationJournal,
      "invalid_publication_request",
    );
    this.#journal = normalized.journal;
    this.#acquireLock = normalized.acquireLock ?? acquireAdvisoryLock;
    this.#trustRenameOutcome = this.#acquireLock === acquireAdvisoryLock;
    this.#inspectFilesystem = normalized.inspectFilesystem ?? defaultInspectFilesystem;
    this.#faults = normalizeFaults(normalized.faults);
    ensure(
      typeof this.#acquireLock === "function" &&
        typeof this.#inspectFilesystem === "function",
      "invalid_publication_request",
    );
    Object.freeze(this);
  }

  async publishCheckpointArtifact(options) {
    const normalized = exactOptions(options, [
      "artifactDirectory",
      "artifactOwnedRoot",
      "binding",
      "operationId",
      "request",
      "result",
      "sourceDirectory",
      "sourceOwnedRoot",
    ]);
    const publication = {
      binding: normalized.binding,
      finalDirectory: normalized.artifactDirectory,
      kind: "checkpoint-artifact",
      operationId: normalized.operationId,
      request: normalized.request,
      result: normalized.result,
      sourceDirectory: normalized.sourceDirectory,
      sourceOwnedRoot: normalized.sourceOwnedRoot,
      targetOwnedRoot: normalized.artifactOwnedRoot,
    };
    const queueKey = await publicationQueueKey(publication.targetOwnedRoot);
    return runPublicationQueued(queueKey, () => this.#publish(publication));
  }

  async publishRestoreDestination(options) {
    const normalized = exactOptions(options, [
      "artifactDirectory",
      "artifactOwnedRoot",
      "artifactProof",
      "binding",
      "destinationDirectory",
      "destinationOwnedRoot",
      "operationId",
      "request",
      "result",
    ]);
    const publication = {
      binding: normalized.binding,
      artifactProof: normalized.artifactProof,
      finalDirectory: normalized.destinationDirectory,
      kind: "restore-destination",
      operationId: normalized.operationId,
      request: normalized.request,
      result: normalized.result,
      sourceDirectory: normalized.artifactDirectory,
      sourceOwnedRoot: normalized.artifactOwnedRoot,
      targetOwnedRoot: normalized.destinationOwnedRoot,
    };
    const queueKey = await publicationQueueKey(publication.targetOwnedRoot);
    return runPublicationQueued(queueKey, () => this.#publish(publication));
  }

  async #publish(options) {
    const operationId = assertOpaqueId(options.operationId);
    const coordinatorBinding = ownEnumerableObject(options.binding);
    let request;
    try {
      request = assertStorageMutationRequest(options.request);
    } catch {
      fail("invalid_publication_request");
    }
    ensure(
      request.operationId === operationId &&
        ((options.kind === "checkpoint-artifact" && request.operation === "checkpoint") ||
          (options.kind === "restore-destination" && request.operation === "restore")),
      "invalid_publication_request",
    );
    const result = validateResult(request, options.result);
    const artifactProof =
      options.kind === "restore-destination"
        ? normalizeArtifactProof(options.artifactProof)
        : null;

    let sourceAuthority;
    let targetAuthority;
    let lock;
    let pinnedPublication;
    let primaryError;
    let successfulOutcome;
    let publicationMayHaveOccurred = false;
    let finalJournalCommitAttempted = false;
    let historicalCommitConfirmed = false;
    let commitState = "not-committed";
    try {
      try {
        sourceAuthority = await openStoppedTreeRootAuthority(options.sourceOwnedRoot);
        targetAuthority = await openStoppedTreeRootAuthority(options.targetOwnedRoot);
      } catch {
        fail("invalid_publication_request");
      }
      ensure(
        sourceAuthority.path === targetAuthority.path ||
          (!pathIsInside(sourceAuthority.path, targetAuthority.path) &&
            !pathIsInside(targetAuthority.path, sourceAuthority.path) &&
            !sameFileIdentity(sourceAuthority.identity, targetAuthority.identity)),
        "invalid_publication_request",
      );
      const source = await inspectDirectDirectory(
        sourceAuthority,
        options.sourceDirectory,
        { mustExist: false },
      );
      ensure(
        typeof options.finalDirectory === "string" &&
          isAbsolute(options.finalDirectory) &&
          resolve(options.finalDirectory) === options.finalDirectory,
        "invalid_publication_request",
      );
      let finalParent;
      try {
        finalParent = await realpath(dirname(options.finalDirectory));
      } catch {
        fail("invalid_publication_request");
      }
      ensure(finalParent === targetAuthority.path, "invalid_publication_request");
      const finalName = assertDirectName(basename(options.finalDirectory));
      const finalPath = join(finalParent, finalName);
      const candidateName = stoppedDirectoryPublicationCandidateName(
        operationId,
        finalName,
      );
      const candidatePath = join(finalParent, candidateName);
      ensure(
        source.path !== finalPath &&
          source.path !== candidatePath &&
          finalPath !== candidatePath,
        "invalid_publication_request",
      );

      try {
        lock = await this.#acquireLock(
          join(targetAuthority.path, ".stopped-directory-publication.lock"),
        );
        await lock.assertHeld();
        await targetAuthority.assertCurrent();
      } catch {
        fail("publication_outcome_uncertain", "uncertain");
      }

      publicationMayHaveOccurred = true;
      const journalAuthority = await this.#journal.describeAuthority();
      const journalFilesystem = await inspectFilesystem(
        this.#inspectFilesystem,
        journalAuthority.path,
        "uncertain",
      );
      ensure(
        !pathIsAtOrInside(source.path, journalAuthority.path) &&
          !pathIsAtOrInside(candidatePath, journalAuthority.path) &&
          !pathIsAtOrInside(finalPath, journalAuthority.path) &&
          (source.identity === null ||
            source.identity.dev.toString() !== journalAuthority.device ||
            source.identity.ino.toString() !== journalAuthority.inode),
        "invalid_publication_request",
        "uncertain",
      );
      const observed = await this.#journal.read({ operationId });
      publicationMayHaveOccurred = false;
      if (observed.record?.state === "committed") {
        historicalCommitConfirmed = true;
        commitState = "committed";
      }
      const observedCandidate = await inspectPath(candidatePath);
      const observedFinal = await inspectPath(finalPath);
      if (observed.record === null) {
        ensure(
          observedCandidate === null && observedFinal === null,
          "publication_recovery_required",
        );
      } else if (observedFinal !== null) {
        publicationMayHaveOccurred = true;
      }
      const targetFilesystem = await inspectFilesystem(
        this.#inspectFilesystem,
        targetAuthority.path,
      );
      let sourceFilesystem;
      if (source.identity !== null) {
        sourceFilesystem = await inspectFilesystem(this.#inspectFilesystem, source.path);
      } else {
        ensure(
          observed.record !== null &&
            ["materialized", "committed"].includes(observed.record.state),
          "publication_recovery_required",
        );
        try {
          const recordedBinding = exactOptions(observed.record.binding, [
            "coordinator",
            "publication",
          ]);
          const recordedPublication = exactOptions(recordedBinding.publication, [
            "contractVersion",
            "destination",
            "journal",
            "publicationKind",
            "source",
          ]);
          const recordedSource = exactOptions(recordedPublication.source, [
            "artifactProof",
            "filesystem",
            "name",
            "root",
          ]);
          sourceFilesystem = normalizeFilesystemProfile(recordedSource.filesystem);
        } catch {
          fail("publication_integrity_failed");
        }
      }
      const publicationBinding = Object.freeze({
        contractVersion: PUBLICATION_CONTRACT_VERSION,
        journal: Object.freeze({
          filesystem: journalFilesystem,
          root: Object.freeze({
            device: journalAuthority.device,
            inode: journalAuthority.inode,
          }),
        }),
        publicationKind: options.kind,
        source: Object.freeze({
          artifactProof,
          filesystem: sourceFilesystem,
          name: source.name,
          root: rootIdentity(sourceAuthority),
        }),
        destination: Object.freeze({
          candidateName,
          filesystem: targetFilesystem,
          name: finalName,
          root: rootIdentity(targetAuthority),
        }),
      });
      const journalInput = Object.freeze({
        binding: Object.freeze({
          coordinator: coordinatorBinding,
          publication: publicationBinding,
        }),
        operationId,
        request,
        result,
      });
      const prepared = await this.#journal.prepare(journalInput);
      const state = prepared.record.state;
      if (state === "prepared" && !prepared.replayed) {
        await runFault(this.#faults.afterJournalPrepared);
      }
      if (state === "committed") {
        commitState = "committed";
        const materialization = validateMaterialization(prepared.record.materialization, {
          artifactProof,
          finalName,
          kind: options.kind,
          operationId,
        });
        ensure(
          (await inspectPath(
            candidatePath,
            "published_state_invalid",
            "committed",
          )) === null,
          "published_state_invalid",
          "committed",
        );
        const finalIdentity = await inspectPath(
          finalPath,
          "published_state_invalid",
          "committed",
        );
        ensure(finalIdentity !== null, "published_state_invalid", "committed");
        pinnedPublication = await openPinnedDirectory(finalPath, materialization, true);
        await this.#verifyPublishedTree({
          checkpoint: prepared.record.result.checkpoint,
          kind: options.kind,
          materialization,
          path: finalPath,
          committed: true,
        });
        await assertPinnedPath(
          finalPath,
          pinnedPublication,
          "published_state_invalid",
          "committed",
        );
        successfulOutcome = frozenOutcome(prepared);
      }

      if (successfulOutcome === undefined) {
        let materialized = prepared;
        if (state === "prepared") {
          ensure(source.identity !== null, "publication_recovery_required");
          ensure(
            (await inspectPath(candidatePath)) === null &&
              (await inspectPath(finalPath)) === null,
            "publication_recovery_required",
          );
          const created = await this.#materializeCandidate({
            candidatePath,
            checkpoint: prepared.record.result.checkpoint,
            finalName,
            journalAuthority,
            kind: options.kind,
            operationId,
            artifactProof,
            source,
            sourceAuthority,
            targetAuthority,
          });
          pinnedPublication = created.pinned;
          await runFault(this.#faults.afterCandidateBarrier);
          materialized = await this.#journal.markMaterialized({
            ...journalInput,
            materialization: created.materialization,
          });
          await runFault(this.#faults.afterMaterialized);
        }

        const materialization = validateMaterialization(
          materialized.record.materialization,
          {
            artifactProof,
            finalName,
            kind: options.kind,
            operationId,
          },
        );
        const candidateIdentity = await inspectPath(candidatePath);
        const finalIdentity = await inspectPath(finalPath);
        ensure(
          (candidateIdentity === null) !== (finalIdentity === null),
          "publication_recovery_required",
        );
        if (candidateIdentity !== null) {
          pinnedPublication ??= await openPinnedDirectory(
            candidatePath,
            materialization,
          );
          // A durable materialized record with a mismatched candidate must be
          // reconciled by a trusted recovery path; it is never recopied.
          try {
            await this.#verifyPublishedTree({
              checkpoint: materialized.record.result.checkpoint,
              kind: options.kind,
              materialization,
              path: candidatePath,
            });
          } catch (error) {
            if (internalErrors.has(error)) {
              fail("publication_recovery_required");
            }
            throw error;
          }
          await assertPinnedPath(
            candidatePath,
            pinnedPublication,
            "publication_recovery_required",
            "not-committed",
          );
          await runFault(this.#faults.beforeRename);
          await targetAuthority.assertCurrent();
          await assertUntrustedLockHeld(lock);
          await assertPinnedPath(
            candidatePath,
            pinnedPublication,
            "publication_recovery_required",
            "not-committed",
          );
          publicationMayHaveOccurred = true;
          try {
            await lock.renameWhileHeld(candidatePath, finalPath, { kind: "absent" });
          } catch (error) {
            if (
              this.#trustRenameOutcome &&
              error !== null &&
              typeof error === "object" &&
              error.renameOutcome === "not-committed"
            ) {
              publicationMayHaveOccurred = false;
              fail("publication_io_failed");
            }
            fail("publication_outcome_uncertain", "uncertain");
          }
          await runFault(this.#faults.afterRename);
        } else {
          publicationMayHaveOccurred = true;
          pinnedPublication = await openPinnedDirectory(finalPath, materialization);
        }

        await assertPathAbsent(
          candidatePath,
          "publication_outcome_uncertain",
          "uncertain",
        );
        const visibleIdentity = await inspectPath(
          finalPath,
          "publication_outcome_uncertain",
          "uncertain",
        );
        ensure(
          visibleIdentity !== null &&
            sameFileIdentity(visibleIdentity, pinnedPublication.identity) &&
            identityMatchesMaterialization(visibleIdentity, materialization),
          "publication_outcome_uncertain",
          "uncertain",
        );
        try {
          await targetAuthority.assertCurrent();
          await targetAuthority.handle.sync();
        } catch {
          fail("publication_outcome_uncertain", "uncertain");
        }
        await runFault(this.#faults.afterParentSync);
        try {
          await this.#verifyPublishedTree({
            checkpoint: materialized.record.result.checkpoint,
            kind: options.kind,
            materialization,
            path: finalPath,
          });
          await assertPinnedPath(
            finalPath,
            pinnedPublication,
            "publication_outcome_uncertain",
            "uncertain",
          );
        } catch (error) {
          if (internalErrors.has(error)) {
            fail("publication_outcome_uncertain", "uncertain");
          }
          throw error;
        }
        await runFault(this.#faults.afterFinalReadback);
        await runFault(this.#faults.beforeCommit);
        try {
          await this.#verifyPublishedTree({
            checkpoint: materialized.record.result.checkpoint,
            kind: options.kind,
            materialization,
            path: finalPath,
          });
          await assertPinnedPath(
            finalPath,
            pinnedPublication,
            "publication_outcome_uncertain",
            "uncertain",
          );
        } catch (error) {
          if (internalErrors.has(error)) {
            fail("publication_outcome_uncertain", "uncertain");
          }
          throw error;
        }
        finalJournalCommitAttempted = true;
        const committed = await this.#journal.commit({
          ...journalInput,
          materialization,
        });
        commitState = "committed";
        successfulOutcome = frozenOutcome(committed);
      }
    } catch (error) {
      const journalError = normalizeJournalError(error, {
        finalJournalCommitAttempted,
        historicalCommitConfirmed,
        publicationMayHaveOccurred,
      });
      if (journalError) primaryError = journalError;
      else if (internalErrors.has(error)) {
        if (historicalCommitConfirmed && error.commitState !== "committed") {
          primaryError = createPublicationError(error.code, "committed");
        } else if (
          publicationMayHaveOccurred &&
          error.commitState === "not-committed"
        ) {
          primaryError = createPublicationError(
            "publication_outcome_uncertain",
            "uncertain",
          );
        } else {
          primaryError = error;
        }
      }
      else {
        primaryError = createPublicationError(
          historicalCommitConfirmed
            ? "publication_io_failed"
            : publicationMayHaveOccurred
            ? "publication_outcome_uncertain"
            : "publication_io_failed",
          historicalCommitConfirmed
            ? "committed"
            : publicationMayHaveOccurred
              ? "uncertain"
              : "not-committed",
        );
      }
      commitState = primaryError.commitState;
    }

    let cleanupFailed = false;
    for (const operation of [
      () => pinnedPublication?.handle.close(),
      () => lock?.release(),
      () => targetAuthority?.handle.close(),
      () => sourceAuthority?.handle.close(),
    ]) {
      try {
        await operation();
      } catch {
        cleanupFailed = true;
      }
    }
    if (cleanupFailed) {
      throw createPublicationError(
        "publication_lock_release_failed",
        commitState,
      );
    }
    if (primaryError) throw primaryError;
    if (successfulOutcome !== undefined) return successfulOutcome;
    fail("publication_io_failed");
  }

  async #materializeCandidate({
    artifactProof,
    candidatePath,
    checkpoint,
    finalName,
    journalAuthority,
    kind,
    operationId,
    source,
    sourceAuthority,
    targetAuthority,
  }) {
    let sourceTree = source.path;
    let sourceOwnedRoot = sourceAuthority.path;
    let sourceDigest;
    let artifactManifestDigest;
    if (kind === "restore-destination") {
      const manifest = await readArtifactManifest(
        join(source.path, "artifact.json"),
        checkpoint,
        artifactProof,
      );
      sourceTree = join(source.path, "payload");
      sourceOwnedRoot = source.path;
      await syncStoppedTree(source.path, { allowRootMount: true });
      await sourceAuthority.handle.sync();
      const stableManifest = await readArtifactManifest(
        join(source.path, "artifact.json"),
        checkpoint,
        artifactProof,
      );
      ensure(
        stableManifest.digest === manifest.digest,
        "publication_integrity_failed",
      );
      sourceDigest = await digestTree(sourceTree);
      ensure(
        sourceDigest === manifest.manifest.modeledDigest,
        "publication_integrity_failed",
      );
      artifactManifestDigest = manifest.digest;
    } else {
      await syncStoppedTree(source.path, { allowRootMount: true });
      await sourceAuthority.handle.sync();
      sourceDigest = await digestTree(source.path, { allowRootMount: true });
      artifactManifestDigest = sha256("pending-artifact-manifest\0", operationId);
    }
    await runFault(this.#faults.afterSourceBarrier);

    if (kind === "checkpoint-artifact") {
      await createPrivateDirectory(candidatePath);
      await runFault(this.#faults.afterCandidateCreated);
      const payload = join(candidatePath, "payload");
      await copyStoppedTreeBetweenRoots({
        allowAbsoluteSymlinks: false,
        allowSourceRootMount: true,
        destination: payload,
        destinationOwnedRoot: candidatePath,
        forbiddenAbsoluteSymlinkAuthorities: [journalAuthority],
        source: source.path,
        sourceOwnedRoot: sourceAuthority.path,
      });
      await runFault(this.#faults.afterCopy);
      const copiedDigest = await digestTree(payload);
      ensure(copiedDigest === sourceDigest, "publication_integrity_failed");
      const manifest = artifactManifest(checkpoint, operationId, copiedDigest);
      artifactManifestDigest = await writeArtifactManifest(
        join(candidatePath, "artifact.json"),
        manifest,
      );
    } else {
      await copyStoppedTreeBetweenRoots({
        allowAbsoluteSymlinks: false,
        afterDestinationRootCreated: async () =>
          runFault(this.#faults.afterCandidateCreated),
        destination: candidatePath,
        destinationOwnedRoot: targetAuthority.path,
        forbiddenAbsoluteSymlinkAuthorities: [journalAuthority],
        source: sourceTree,
        sourceOwnedRoot,
      });
      await runFault(this.#faults.afterCopy);
    }

    await syncStoppedTree(candidatePath);
    await targetAuthority.handle.sync();
    const modeledPath =
      kind === "checkpoint-artifact" ? join(candidatePath, "payload") : candidatePath;
    const copiedDigest = await digestTree(modeledPath);
    const sourceDigestAfterCopy = await digestTree(sourceTree, {
      allowRootMount: kind === "checkpoint-artifact",
    });
    ensure(
      copiedDigest === sourceDigest && sourceDigestAfterCopy === sourceDigest,
      "publication_integrity_failed",
    );
    if (kind === "checkpoint-artifact") {
      const manifest = await readArtifactManifest(
        join(candidatePath, "artifact.json"),
        checkpoint,
      );
      ensure(
        manifest.digest === artifactManifestDigest &&
          manifest.manifest.modeledDigest === copiedDigest,
        "publication_integrity_failed",
      );
    } else {
      const manifest = await readArtifactManifest(
        join(source.path, "artifact.json"),
        checkpoint,
        artifactProof,
      );
      ensure(
        manifest.digest === artifactManifestDigest &&
          manifest.manifest.modeledDigest === copiedDigest,
        "publication_integrity_failed",
      );
    }
    const identity = await lstat(candidatePath, { bigint: true });
    ensure(identity.isDirectory(), "publication_integrity_failed");
    const materialization = materializationFor({
      artifactManifestDigest,
      finalName,
      identity,
      kind,
      modeledDigest: copiedDigest,
      operationId,
    });
    return Object.freeze({
      materialization,
      pinned: await openPinnedDirectory(candidatePath, materialization),
    });
  }

  async #verifyPublishedTree({
    checkpoint,
    committed = false,
    kind,
    materialization,
    path,
  }) {
    try {
      if (kind === "checkpoint-artifact") {
        const manifest = await readArtifactManifest(join(path, "artifact.json"), checkpoint);
        const digest = await digestTree(join(path, "payload"));
        ensure(
          manifest.digest === materialization.artifactManifestDigest &&
            manifest.manifest.modeledDigest === materialization.modeledDigest &&
            digest === materialization.modeledDigest,
          "publication_integrity_failed",
        );
      } else {
        ensure(
          (await digestTree(path)) === materialization.modeledDigest,
          "publication_integrity_failed",
        );
      }
    } catch (error) {
      if (committed) fail("published_state_invalid", "committed");
      if (internalErrors.has(error)) throw error;
      fail("publication_integrity_failed");
    }
  }
}
