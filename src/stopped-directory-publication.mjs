import { createHash } from "node:crypto";
import { constants as fsConstants } from "node:fs";
import {
  chmod,
  lstat,
  mkdir,
  open,
  readdir,
  realpath,
  statfs,
} from "node:fs/promises";
import { basename, dirname, isAbsolute, join, resolve } from "node:path";

import { acquireAdvisoryLock } from "./advisory-lock.mjs";
import {
  FilesystemOperationJournal,
  OperationJournalError,
  snapshotOperationJournalBinding,
} from "./filesystem-operation-journal.mjs";
import {
  assertCheckpointDescriptor,
  assertStorageMutationRequest,
  assertStorageMutationResult,
} from "./session-storage-contracts.mjs";
import {
  copyStoppedTreeBetweenRoots,
  digestStoppedTreeIdentities,
  digestTree,
  inspectStoppedTreeObjectIdentity,
  openStoppedTreeModeledRootAuthority,
  openStoppedTreeRootAuthority,
  sameFileIdentity,
  stoppedTreeContainsAnyIdentity,
  stoppedTreesShareAnyIdentity,
  syncStoppedTree,
} from "./stopped-tree.mjs";

const journalPrepareFreshIntrinsic =
  FilesystemOperationJournal.prototype.prepareFresh;
const reflectApply = Reflect.apply;

export const STOPPED_DIRECTORY_ARTIFACT_VERSION = 1;

const PUBLICATION_CONTRACT_VERSION = 2;
const ARTIFACT_FORMAT = "portable-codex-stopped-directory";
const LOCAL_DURABILITY_PROFILE = "local-fsync-rename";
const PUBLICATION_CANDIDATE_PREFIX = ".publication-";
export const STOPPED_DIRECTORY_PUBLICATION_LOCK_NAME =
  ".stopped-directory-publication.lock";
const OPAQUE_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u;
const DIGEST_PATTERN = /^[0-9a-f]{64}$/u;
const COMMIT_STATES = new Set(["committed", "not-committed", "uncertain"]);
const PUBLICATION_STATE_RANK = Object.freeze({
  absent: 0,
  prepared: 1,
  materialized: 2,
  committed: 3,
});
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

function exactOptions(
  value,
  allowed,
  required = allowed,
  code = "invalid_publication_request",
) {
  const options = ownEnumerableObject(value, code);
  const keys = Object.keys(options);
  ensure(keys.every((key) => allowed.includes(key)), code);
  ensure(required.every((key) => Object.hasOwn(options, key)), code);
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
      !value.includes("\0") &&
      value !== "." &&
      value !== ".." &&
      basename(value) === value,
    "invalid_publication_request",
  );
  return value;
}

function assertPublicationFinalName(value) {
  const name = assertDirectName(value);
  ensure(
    name !== STOPPED_DIRECTORY_PUBLICATION_LOCK_NAME &&
      !name.startsWith(PUBLICATION_CANDIDATE_PREFIX),
    "invalid_publication_request",
  );
  return name;
}

function sha256(...values) {
  const hash = createHash("sha256");
  for (const value of values) hash.update(value, "utf8");
  return hash.digest("hex");
}

export function stoppedDirectoryPublicationCandidateName(operationId, finalName) {
  const operation = assertOpaqueId(operationId);
  const destination = assertPublicationFinalName(finalName);
  return `${PUBLICATION_CANDIDATE_PREFIX}${sha256(
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
    ensure(
      identity.isDirectory(),
      "publication_outcome_uncertain",
      "uncertain",
    );
    return `${identity.dev.toString()}\0${identity.ino.toString()}`;
  } catch (error) {
    if (internalErrors.has(error)) throw error;
    fail("publication_outcome_uncertain", "uncertain");
  }
}

function pathIsInside(root, candidate) {
  return candidate.startsWith(`${root}/`);
}

function pathIsAtOrInside(root, candidate) {
  return candidate === root || pathIsInside(root, candidate);
}

function pathsAreDisjoint(left, right) {
  return !pathIsAtOrInside(left, right) && !pathIsAtOrInside(right, left);
}

function persistentFileIdentityRecord(objectId, filesystem) {
  return Object.freeze({
    filesystemId: filesystem.filesystemId,
    objectIdentityScheme: filesystem.objectIdentityScheme,
    objectId,
  });
}

function parseFileIdentityRecord(value) {
  const record = exactOptions(
    value,
    ["filesystemId", "objectIdentityScheme", "objectId"],
    ["filesystemId", "objectIdentityScheme", "objectId"],
    "publication_integrity_failed",
  );
  ensure(
    typeof record.filesystemId === "string" &&
      /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u.test(record.filesystemId) &&
      typeof record.objectIdentityScheme === "string" &&
      /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u.test(
        record.objectIdentityScheme,
      ) &&
      typeof record.objectId === "string" &&
      /^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$/u.test(record.objectId),
    "publication_integrity_failed",
  );
  return Object.freeze({
    filesystemId: record.filesystemId,
    objectIdentityScheme: record.objectIdentityScheme,
    objectId: record.objectId,
  });
}

async function inspectBoundObjectIdentity(
  inspector,
  observedObjectIdentities,
  path,
  expectedIdentity,
  filesystem,
  code = "publication_io_failed",
  commitState = "not-committed",
) {
  try {
    const objectId = await inspectStoppedTreeObjectIdentity(
      path,
      inspector,
      expectedIdentity,
    );
    const key = `${filesystem.filesystemId}\0${filesystem.objectIdentityScheme}\0${objectId}`;
    const runtimeIdentity = `${expectedIdentity.dev}:${expectedIdentity.ino}`;
    const persistentKey = `persistent\0${key}`;
    const runtimeKey = `runtime\0${runtimeIdentity}`;
    const previousRuntimeIdentity = observedObjectIdentities.get(persistentKey);
    const previousPersistentIdentity = observedObjectIdentities.get(runtimeKey);
    ensure(
      (previousRuntimeIdentity === undefined ||
        previousRuntimeIdentity === runtimeIdentity) &&
        (previousPersistentIdentity === undefined ||
          previousPersistentIdentity === key),
      code,
      commitState,
    );
    observedObjectIdentities.set(persistentKey, runtimeIdentity);
    observedObjectIdentities.set(runtimeKey, key);
    return objectId;
  } catch {
    fail(code, commitState);
  }
}

async function directSourceLocation(authority, value) {
  ensure(
    typeof value === "string" && isAbsolute(value) && resolve(value) === value,
    "invalid_publication_request",
  );
  let parent;
  try {
    parent = await realpath(dirname(value));
  } catch {
    fail("publication_outcome_uncertain", "uncertain");
  }
  ensure(parent === authority.path, "invalid_publication_request");
  const path = join(parent, assertDirectName(basename(value)));
  return Object.freeze({
    name: basename(path),
    path,
  });
}

async function inspectDirectSource(authority, location) {
  const { name, path } = location;
  let observedIdentity;
  try {
    observedIdentity = await lstat(path, { bigint: true });
  } catch (error) {
    if (error?.code !== "ENOENT") {
      fail("publication_outcome_uncertain", "uncertain");
    }
    observedIdentity = null;
  }
  try {
    await authority.assertCurrent();
  } catch {
    fail("publication_outcome_uncertain", "uncertain");
  }
  const isDirectory =
    observedIdentity !== null &&
    observedIdentity.isDirectory() &&
    !observedIdentity.isSymbolicLink();
  return Object.freeze({
    identity: isDirectory ? observedIdentity : null,
    kind: observedIdentity === null ? "missing" : isDirectory ? "directory" : "other",
    name,
    path,
  });
}

async function assertPublicationAuthoritiesCurrent(
  sourceAuthority,
  targetAuthority,
  code = "publication_integrity_failed",
  commitState = "not-committed",
) {
  try {
    await sourceAuthority.assertCurrent();
    await targetAuthority.assertCurrent();
  } catch {
    fail(code, commitState);
  }
}

async function assertPublicationAuthoritiesForState(
  sourceAuthority,
  targetAuthority,
  state,
) {
  if (state === "committed") {
    return assertPublicationAuthoritiesCurrent(
      sourceAuthority,
      targetAuthority,
      "published_state_invalid",
      "committed",
    );
  }
  if (state === "materialized") {
    return assertPublicationAuthoritiesCurrent(
      sourceAuthority,
      targetAuthority,
      "publication_outcome_uncertain",
      "uncertain",
    );
  }
  return assertPublicationAuthoritiesCurrent(
    sourceAuthority,
    targetAuthority,
  );
}

async function assertPublicationTopology({
  code = "invalid_publication_request",
  commitState = "uncertain",
  journalAuthority,
  journalIdentity,
  listMountPoints,
  source,
  sourceAuthority,
  targetAuthority,
}) {
  await assertPublicationAuthoritiesCurrent(
    sourceAuthority,
    targetAuthority,
    code,
    commitState,
  );
  ensure(
    pathsAreDisjoint(sourceAuthority.path, journalAuthority.path) &&
      pathsAreDisjoint(targetAuthority.path, journalAuthority.path) &&
      !sameFileIdentity(sourceAuthority.identity, journalIdentity) &&
      !sameFileIdentity(targetAuthority.identity, journalIdentity) &&
      (source.identity === null ||
        !sameFileIdentity(source.identity, journalIdentity)),
    code,
    commitState,
  );
  await assertDirectoryIdentity(
    journalAuthority.path,
    journalIdentity,
    code,
    commitState,
  );
  let journalContainsAuthorityIdentity;
  try {
    journalContainsAuthorityIdentity = await stoppedTreeContainsAnyIdentity(
      journalAuthority.path,
      [
        sourceAuthority.identity,
        targetAuthority.identity,
        ...(source.identity === null ? [] : [source.identity]),
      ],
      { allowRootMount: true, listMountPoints },
    );
  } catch {
    fail(code, commitState);
  }
  ensure(!journalContainsAuthorityIdentity, code, commitState);
  await assertDirectoryIdentity(
    journalAuthority.path,
    journalIdentity,
    code,
    commitState,
  );
  if (source.identity !== null) {
    await assertDirectoryIdentity(
      source.path,
      source.identity,
      code,
      commitState,
    );
    let containsAuthorityIdentity;
    try {
      containsAuthorityIdentity = await stoppedTreeContainsAnyIdentity(
        source.path,
        [targetAuthority.identity, journalIdentity],
        { allowRootMount: true, listMountPoints },
      );
    } catch {
      fail(code, commitState);
    }
    ensure(
      !containsAuthorityIdentity,
      code,
      commitState,
    );
    await assertDirectoryIdentity(
      source.path,
      source.identity,
      code,
      commitState,
    );
  }
  await assertPublicationAuthoritiesCurrent(
    sourceAuthority,
    targetAuthority,
    code,
    commitState,
  );
}

async function assertPublicationTopologyForState(options, state) {
  if (state === "unknown") return assertPublicationTopology(options);
  if (state === "committed") {
    return assertPublicationTopology({
      ...options,
      code: "published_state_invalid",
      commitState: "committed",
    });
  }
  if (state === "materialized") {
    return assertPublicationTopology({
      ...options,
      code: "publication_outcome_uncertain",
      commitState: "uncertain",
    });
  }
  return assertPublicationTopology({
    ...options,
    code: "publication_integrity_failed",
    commitState: "not-committed",
  });
}

async function assertSourcePublicationIdentityDisjoint(
  source,
  publicationPath,
  code,
  commitState,
  listMountPoints,
) {
  if (source.identity === null) return;
  let overlaps;
  try {
    overlaps = await stoppedTreesShareAnyIdentity(source.path, publicationPath, {
      allowLeftRootMount: true,
      listMountPoints,
    });
  } catch {
    fail(code, commitState);
  }
  ensure(!overlaps, code, commitState);
}

async function assertExactCheckpointBundleRoot(
  path,
  code = "publication_integrity_failed",
  commitState = "not-committed",
) {
  let entries;
  try {
    entries = await readdir(path, { encoding: "buffer" });
  } catch {
    fail(code, commitState);
  }
  const artifactName = Buffer.from("artifact.json");
  const payloadName = Buffer.from("payload");
  ensure(
    entries.length === 2 &&
      entries.some((entry) => entry.equals(artifactName)) &&
      entries.some((entry) => entry.equals(payloadName)),
    code,
    commitState,
  );
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

function samePathObservation(left, right) {
  return (
    (left === null && right === null) ||
    (left !== null && right !== null && sameFileIdentity(left, right))
  );
}

async function inspectPublicationNamespace(candidatePath, finalPath) {
  return Object.freeze({
    candidate: await inspectPath(
      candidatePath,
      "publication_outcome_uncertain",
      "uncertain",
    ),
    final: await inspectPath(
      finalPath,
      "publication_outcome_uncertain",
      "uncertain",
    ),
  });
}

function samePublicationNamespace(left, right) {
  return (
    samePathObservation(left.candidate, right.candidate) &&
    samePathObservation(left.final, right.final)
  );
}

async function assertDirectoryIdentity(path, expected, code, commitState) {
  let current;
  try {
    current = await lstat(path, { bigint: true });
  } catch {
    fail(code, commitState);
  }
  ensure(
    current.isDirectory() && sameFileIdentity(current, expected),
    code,
    commitState,
  );
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
  // Node's statfs surface does not expose a cross-host filesystem incarnation
  // identifier. Production publication therefore requires a trusted adapter.
  fail("unsupported_publication_filesystem");
}

async function inspectFilesystem(
  inspector,
  path,
  code = "unsupported_publication_filesystem",
  commitState = "not-committed",
) {
  let raw;
  try {
    raw = await inspector(path);
  } catch {
    fail(code, commitState);
  }
  try {
    return normalizeFilesystemProfile(raw);
  } catch {
    fail(code, commitState);
  }
}

function normalizeFilesystemProfile(raw) {
  const profile = exactOptions(raw, [
    "durability",
    "filesystemId",
    "objectIdentityScheme",
    "type",
  ]);
  ensure(
    profile.durability === LOCAL_DURABILITY_PROFILE &&
      typeof profile.filesystemId === "string" &&
      /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u.test(profile.filesystemId) &&
      typeof profile.objectIdentityScheme === "string" &&
      /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u.test(
        profile.objectIdentityScheme,
      ) &&
      typeof profile.type === "string" &&
      /^[A-Za-z0-9][A-Za-z0-9._:-]{0,63}$/u.test(profile.type),
    "unsupported_publication_filesystem",
  );
  return Object.freeze({
    durability: profile.durability,
    filesystemId: profile.filesystemId,
    objectIdentityScheme: profile.objectIdentityScheme,
    type: profile.type,
  });
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

function snapshotPublicationQueueInput(kind, normalized) {
  const operationId = assertOpaqueId(normalized.operationId);
  let request;
  try {
    request = assertStorageMutationRequest(normalized.request);
  } catch {
    fail("invalid_publication_request");
  }
  ensure(
    request.operationId === operationId &&
      ((kind === "checkpoint-artifact" && request.operation === "checkpoint") ||
        (kind === "restore-destination" && request.operation === "restore")),
    "invalid_publication_request",
  );
  let binding;
  try {
    binding = snapshotOperationJournalBinding(normalized.binding);
  } catch {
    fail("invalid_publication_request");
  }
  return Object.freeze({
    artifactProof:
      kind === "restore-destination"
        ? normalizeArtifactProof(normalized.artifactProof)
        : null,
    binding,
    operationId,
    request,
    result: validateResult(request, normalized.result),
  });
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
  { artifactProof, committed = false, kind, operationId, finalName },
) {
  const code = committed ? "published_state_invalid" : "publication_integrity_failed";
  const commitState = committed ? "committed" : "not-committed";
  const keys = [
    "contractVersion",
    "artifactManifestDigest",
    "modeledDigest",
    "publicationId",
    "publicationKind",
    "stagedRoot",
    "treeIdentityDigest",
  ];
  const materialization = exactOptions(value, keys, keys, code);
  const stagedRoot = exactOptions(
    materialization.stagedRoot,
    ["filesystemId", "objectIdentityScheme", "objectId"],
    ["filesystemId", "objectIdentityScheme", "objectId"],
    code,
  );
  ensure(
    materialization.contractVersion === PUBLICATION_CONTRACT_VERSION &&
      materialization.publicationKind === kind &&
      materialization.publicationId === publicationId(operationId, finalName) &&
      typeof materialization.modeledDigest === "string" &&
      DIGEST_PATTERN.test(materialization.modeledDigest) &&
      typeof materialization.artifactManifestDigest === "string" &&
      DIGEST_PATTERN.test(materialization.artifactManifestDigest) &&
      typeof materialization.treeIdentityDigest === "string" &&
      DIGEST_PATTERN.test(materialization.treeIdentityDigest) &&
      (kind !== "restore-destination" ||
        (artifactProof !== null &&
          materialization.artifactManifestDigest ===
            artifactProof.artifactManifestDigest &&
          materialization.modeledDigest === artifactProof.modeledDigest)) &&
      typeof stagedRoot.filesystemId === "string" &&
      /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u.test(
        stagedRoot.filesystemId,
      ) &&
      typeof stagedRoot.objectIdentityScheme === "string" &&
      /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u.test(
        stagedRoot.objectIdentityScheme,
      ) &&
      typeof stagedRoot.objectId === "string" &&
      /^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$/u.test(stagedRoot.objectId),
    code,
    commitState,
  );
  return materialization;
}

function materializationFor({
  artifactManifestDigest,
  filesystem,
  finalName,
  kind,
  modeledDigest,
  objectId,
  operationId,
  treeIdentityDigest,
}) {
  return Object.freeze({
    contractVersion: PUBLICATION_CONTRACT_VERSION,
    artifactManifestDigest,
    modeledDigest,
    publicationId: publicationId(operationId, finalName),
    publicationKind: kind,
    stagedRoot: Object.freeze({
      filesystemId: filesystem.filesystemId,
      objectIdentityScheme: filesystem.objectIdentityScheme,
      objectId,
    }),
    treeIdentityDigest,
  });
}

function identityMatchesMaterialization(objectId, materialization, filesystem) {
  return (
    filesystem.filesystemId === materialization.stagedRoot.filesystemId &&
    filesystem.objectIdentityScheme ===
      materialization.stagedRoot.objectIdentityScheme &&
    objectId === materialization.stagedRoot.objectId
  );
}

async function openPinnedDirectory(
  path,
  materialization,
  {
    committed = false,
    filesystem,
    inspectPersistentObjectIdentity,
    observedObjectIdentities,
    inspectOwnedRootAcl,
    inspectOwnedRootAncestorAcl,
  } = {},
) {
  let authority;
  const modeledRoot = materialization.publicationKind === "restore-destination";
  try {
    const openAuthority = modeledRoot
      ? openStoppedTreeModeledRootAuthority
      : openStoppedTreeRootAuthority;
    authority = await openAuthority(path, {
      inspectOwnedRootAcl,
      inspectOwnedRootAncestorAcl,
    });
    const metadata = authority.identity;
    const objectId = await inspectBoundObjectIdentity(
      inspectPersistentObjectIdentity,
      observedObjectIdentities,
      path,
      metadata,
      filesystem,
      committed ? "published_state_invalid" : "publication_recovery_required",
      committed ? "committed" : "not-committed",
    );
    await authority.assertCurrent();
    ensure(
      identityMatchesMaterialization(objectId, materialization, filesystem),
      committed ? "published_state_invalid" : "publication_recovery_required",
      committed ? "committed" : "not-committed",
    );
    return {
      handle: authority.handle,
      identity: metadata,
      inspectOwnedRootAcl,
      inspectOwnedRootAncestorAcl,
      mode: authority.mode,
      modeledRoot,
    };
  } catch (error) {
    if (authority) await authority.handle.close().catch(() => {});
    if (internalErrors.has(error)) throw error;
    fail(
      committed ? "published_state_invalid" : "publication_recovery_required",
      committed ? "committed" : "not-committed",
    );
  }
}

async function assertPinnedPath(path, pinned, code, commitState) {
  let authority;
  let held;
  let valid = false;
  try {
    const openAuthority = pinned.modeledRoot
      ? openStoppedTreeModeledRootAuthority
      : openStoppedTreeRootAuthority;
    authority = await openAuthority(path, {
      inspectOwnedRootAcl: pinned.inspectOwnedRootAcl,
      inspectOwnedRootAncestorAcl: pinned.inspectOwnedRootAncestorAcl,
    });
    held = await pinned.handle.stat({ bigint: true });
    await authority.assertCurrent();
    valid =
      sameFileIdentity(authority.identity, pinned.identity) &&
      sameFileIdentity(held, pinned.identity) &&
      authority.mode === pinned.mode &&
      Number(held.mode & 0o777n) === pinned.mode &&
      (held.mode & 0o7000n) === 0n;
  } catch {}
  let closeFailed = false;
  try {
    await authority?.handle.close();
  } catch {
    closeFailed = true;
  }
  ensure(valid && !closeFailed, code, commitState);
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
  if (
    error.code === "operation_conflict" ||
    error.code === "operation_already_started"
  ) {
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
  #inspectPersistentObjectIdentity;
  #inspectOwnedRootAcl;
  #inspectOwnedRootAncestorAcl;
  #journal;
  #listMountPoints;
  #trustRenameOutcome;

  constructor(options) {
    const normalized = exactOptions(
      options,
      [
        "acquireLock",
        "faults",
        "inspectFilesystem",
        "inspectPersistentObjectIdentity",
        "inspectOwnedRootAcl",
        "inspectOwnedRootAncestorAcl",
        "journal",
        "listMountPoints",
      ],
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
    this.#inspectPersistentObjectIdentity =
      normalized.inspectPersistentObjectIdentity ??
      (async () => {
        throw new Error("persistent object identity is unavailable");
      });
    this.#inspectOwnedRootAcl = normalized.inspectOwnedRootAcl;
    this.#inspectOwnedRootAncestorAcl = normalized.inspectOwnedRootAncestorAcl;
    this.#listMountPoints = normalized.listMountPoints;
    this.#faults = normalizeFaults(normalized.faults);
    ensure(
      typeof this.#acquireLock === "function" &&
        typeof this.#inspectFilesystem === "function" &&
        typeof this.#inspectPersistentObjectIdentity === "function" &&
        (this.#inspectOwnedRootAcl === undefined ||
          typeof this.#inspectOwnedRootAcl === "function") &&
        (this.#inspectOwnedRootAncestorAcl === undefined ||
          typeof this.#inspectOwnedRootAncestorAcl === "function") &&
        (this.#listMountPoints === undefined ||
          typeof this.#listMountPoints === "function"),
      "invalid_publication_request",
    );
    Object.freeze(this);
  }

  async publishCheckpointArtifact(options) {
    return this.#publishCheckpointArtifact(options, false);
  }

  async publishFreshCheckpointArtifact(options) {
    return this.#publishCheckpointArtifact(options, true);
  }

  async #publishCheckpointArtifact(options, requireFreshOperation) {
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
    const snapshot = snapshotPublicationQueueInput(
      "checkpoint-artifact",
      normalized,
    );
    const publication = {
      binding: snapshot.binding,
      finalDirectory: normalized.artifactDirectory,
      kind: "checkpoint-artifact",
      operationId: snapshot.operationId,
      request: snapshot.request,
      requireFreshOperation,
      result: snapshot.result,
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
    const snapshot = snapshotPublicationQueueInput(
      "restore-destination",
      normalized,
    );
    const publication = {
      binding: snapshot.binding,
      artifactProof: snapshot.artifactProof,
      finalDirectory: normalized.destinationDirectory,
      kind: "restore-destination",
      operationId: snapshot.operationId,
      request: snapshot.request,
      requireFreshOperation: false,
      result: snapshot.result,
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
    const observedObjectIdentities = new Map();
    try {
      ensure(
        typeof options.sourceOwnedRoot === "string" &&
          isAbsolute(options.sourceOwnedRoot) &&
          resolve(options.sourceOwnedRoot) === options.sourceOwnedRoot &&
          typeof options.targetOwnedRoot === "string" &&
          isAbsolute(options.targetOwnedRoot) &&
          resolve(options.targetOwnedRoot) === options.targetOwnedRoot,
        "invalid_publication_request",
      );
      try {
        const rootAuthorityOptions = {
          inspectOwnedRootAcl: this.#inspectOwnedRootAcl,
          inspectOwnedRootAncestorAcl: this.#inspectOwnedRootAncestorAcl,
        };
        sourceAuthority = await openStoppedTreeRootAuthority(
          options.sourceOwnedRoot,
          rootAuthorityOptions,
        );
        targetAuthority = await openStoppedTreeRootAuthority(
          options.targetOwnedRoot,
          rootAuthorityOptions,
        );
      } catch {
        fail("publication_outcome_uncertain", "uncertain");
      }
      ensure(
        sourceAuthority.path !== targetAuthority.path &&
          !pathIsInside(sourceAuthority.path, targetAuthority.path) &&
          !pathIsInside(targetAuthority.path, sourceAuthority.path) &&
          !sameFileIdentity(sourceAuthority.identity, targetAuthority.identity),
        "invalid_publication_request",
      );
      const sourceLocation = await directSourceLocation(
        sourceAuthority,
        options.sourceDirectory,
      );
      const unobservedSource = Object.freeze({
        identity: null,
        kind: "unobserved",
        ...sourceLocation,
      });
      let source = unobservedSource;
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
      const finalName = assertPublicationFinalName(
        basename(options.finalDirectory),
      );
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

      publicationMayHaveOccurred = true;
      const journalAuthority = await this.#journal.describeAuthority();
      const journalFilesystem = await inspectFilesystem(
        this.#inspectFilesystem,
        journalAuthority.path,
        "unsupported_publication_filesystem",
        "uncertain",
      );
      let journalIdentity;
      try {
        ensure(
          /^(?:0|[1-9][0-9]*)$/u.test(journalAuthority.device) &&
            /^[1-9][0-9]*$/u.test(journalAuthority.inode),
          "invalid_publication_request",
          "uncertain",
        );
        journalIdentity = Object.freeze({
          dev: BigInt(journalAuthority.device),
          ino: BigInt(journalAuthority.inode),
        });
      } catch (error) {
        if (internalErrors.has(error)) throw error;
        fail("invalid_publication_request", "uncertain");
      }
      const publicationTopologyForSource = (currentSource) =>
        Object.freeze({
          journalAuthority,
          journalIdentity,
          listMountPoints: this.#listMountPoints,
          source: currentSource,
          sourceAuthority,
          targetAuthority,
        });
      let publicationTopology = publicationTopologyForSource(source);
      let revalidateTargetPersistentIdentity = async () => {};
      const revalidateLockedTopology = async (
        state,
        publicationPath = null,
        publicationPhase = null,
      ) => {
        await assertUntrustedLockHeld(lock);
        if (state === "materialized" && publicationPhase === "candidate") {
          await assertPublicationTopology({
            ...publicationTopology,
            code: "publication_recovery_required",
            commitState: "not-committed",
          });
        } else {
          await assertPublicationTopologyForState(publicationTopology, state);
        }
        await revalidateTargetPersistentIdentity(state);
        if (publicationPath !== null) {
          const code =
            state === "committed"
              ? "published_state_invalid"
              : state === "materialized" && publicationPhase === "candidate"
                ? "publication_recovery_required"
                : state === "materialized"
                ? "publication_outcome_uncertain"
                : "publication_integrity_failed";
          const stateClassification =
            state === "committed"
              ? "committed"
              : state === "materialized" && publicationPhase === "candidate"
                ? "not-committed"
                : state === "materialized"
                ? "uncertain"
                : "not-committed";
          await assertSourcePublicationIdentityDisjoint(
            source,
            publicationPath,
            code,
            stateClassification,
            this.#listMountPoints,
          );
        }
      };
      const runCandidateObservableOperation = async (
        operation,
        { fault = false } = {},
      ) => {
        publicationMayHaveOccurred = true;
        let callbackError;
        try {
          if (fault) await runFault(operation);
          else await operation();
        } catch (error) {
          callbackError = error;
        }
        await assertUntrustedLockHeld(lock);
        try {
          await targetAuthority.assertCurrent();
        } catch {
          fail("publication_outcome_uncertain", "uncertain");
        }
        await revalidateTargetPersistentIdentity("materialized");
        const callbackFinalIdentity = await inspectPath(
          finalPath,
          "publication_outcome_uncertain",
          "uncertain",
        );
        if (callbackFinalIdentity !== null) {
          fail("publication_outcome_uncertain", "uncertain");
        }
        publicationMayHaveOccurred = false;
        if (callbackError !== undefined) throw callbackError;
      };
      await assertPublicationTopology(publicationTopology);
      try {
        lock = await this.#acquireLock(
          join(targetAuthority.path, STOPPED_DIRECTORY_PUBLICATION_LOCK_NAME),
          { requireExisting: true },
        );
        await lock.assertHeld();
      } catch {
        fail("publication_outcome_uncertain", "uncertain");
      }
      await assertUntrustedLockHeld(lock);
      const lockedRootOnlyTopology =
        publicationTopologyForSource(unobservedSource);
      await assertPublicationTopology({
        ...lockedRootOnlyTopology,
        code: "publication_outcome_uncertain",
        commitState: "uncertain",
      });
      const hinted = await this.#journal.readStateHint({ operationId });
      const hintedState = hinted.record?.state ?? "absent";
      ensure(
        Object.hasOwn(PUBLICATION_STATE_RANK, hintedState),
        "publication_outcome_uncertain",
        "uncertain",
      );
      await assertUntrustedLockHeld(lock);
      const assertHintedTopology = async (topology) => {
        if (hintedState === "absent") {
          return assertPublicationTopology(topology);
        }
        return assertPublicationTopology({
          ...topology,
          code: "publication_outcome_uncertain",
          commitState: "uncertain",
        });
      };
      const hintedPublicationMayHaveOccurred = [
        "prepared",
        "materialized",
      ].includes(hintedState);
      publicationMayHaveOccurred = true;
      let sourcePreflightError;
      try {
        if (!["materialized", "committed"].includes(hintedState)) {
          source = await inspectDirectSource(sourceAuthority, sourceLocation);
          publicationTopology = publicationTopologyForSource(source);
        }
        await assertHintedTopology(publicationTopology);
      } catch (error) {
        sourcePreflightError = error;
      }
      await assertUntrustedLockHeld(lock);
      publicationMayHaveOccurred = hintedPublicationMayHaveOccurred;
      if (sourcePreflightError !== undefined) throw sourcePreflightError;
      publicationMayHaveOccurred = true;
      const namespaceBeforeJournalRead = await inspectPublicationNamespace(
        candidatePath,
        finalPath,
      );
      const observed = await this.#journal.read({ operationId });
      await assertUntrustedLockHeld(lock);
      try {
        await targetAuthority.assertCurrent();
      } catch {
        fail("publication_outcome_uncertain", "uncertain");
      }
      const namespaceAfterJournalRead = await inspectPublicationNamespace(
        candidatePath,
        finalPath,
      );
      ensure(
        samePublicationNamespace(
          namespaceBeforeJournalRead,
          namespaceAfterJournalRead,
        ),
        "publication_outcome_uncertain",
        "uncertain",
      );
      publicationMayHaveOccurred = false;
      const observedState = observed.record?.state ?? "absent";
      let sourceContinuityFailed = false;
      ensure(
        Object.hasOwn(PUBLICATION_STATE_RANK, observedState),
        "publication_outcome_uncertain",
        "uncertain",
      );
      if (
        PUBLICATION_STATE_RANK[observedState] <
        PUBLICATION_STATE_RANK[hintedState]
      ) {
        fail("publication_outcome_uncertain", "uncertain");
      }
      if (["materialized", "committed"].includes(observedState)) {
        source = unobservedSource;
      } else {
        const lockedSource = await inspectDirectSource(
          sourceAuthority,
          sourceLocation,
        );
        sourceContinuityFailed = !(
          source.kind === lockedSource.kind &&
          (source.identity === null
            ? lockedSource.identity === null
            : lockedSource.identity !== null &&
              sameFileIdentity(source.identity, lockedSource.identity))
        );
        source = sourceContinuityFailed ? unobservedSource : lockedSource;
      }
      publicationTopology = publicationTopologyForSource(source);
      if (observedState === "committed") {
        historicalCommitConfirmed = true;
        commitState = "committed";
      } else if (["prepared", "materialized"].includes(observedState)) {
        publicationMayHaveOccurred = true;
      }
      await revalidateLockedTopology(observedState);
      await assertPublicationAuthoritiesForState(
        sourceAuthority,
        targetAuthority,
        observed.record?.state,
      );
      const failStorageContinuity = () => {
        if (observed.record?.state === "committed") {
          fail("published_state_invalid", "committed");
        }
        fail("publication_recovery_required");
      };
      const profileMatches = (left, right) =>
        left.durability === right.durability &&
        left.filesystemId === right.filesystemId &&
        left.objectIdentityScheme === right.objectIdentityScheme &&
        left.type === right.type;
      const persistentIdentityMatches = (recorded, objectId, filesystem) =>
        recorded.filesystemId === filesystem.filesystemId &&
        recorded.objectIdentityScheme === filesystem.objectIdentityScheme &&
        recorded.objectId === objectId;
      let recordedDestinationFilesystem = null;
      let recordedDestinationRoot = null;
      let recordedJournalFilesystem = null;
      let recordedJournalRoot = null;
      let recordedSource = null;
      let recordedSourceDirectoryIdentity = null;
      let recordedSourceFilesystem = null;
      let recordedSourceRoot = null;
      let recordedSourceRootFilesystem = null;
      if (observed.record !== null) {
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
          const recordedDestination = exactOptions(
            recordedPublication.destination,
            ["candidateName", "filesystem", "name", "root"],
          );
          const recordedJournal = exactOptions(
            recordedPublication.journal,
            ["filesystem", "root"],
          );
          recordedSource = exactOptions(recordedPublication.source, [
            "artifactProof",
            "directoryIdentity",
            "filesystem",
            "name",
            "root",
            "rootFilesystem",
          ]);
          ensure(
            recordedPublication.contractVersion ===
                PUBLICATION_CONTRACT_VERSION &&
              recordedPublication.publicationKind === options.kind &&
              recordedDestination.name === finalName &&
              recordedDestination.candidateName === candidateName,
            "publication_integrity_failed",
          );
          recordedDestinationFilesystem = normalizeFilesystemProfile(
            recordedDestination.filesystem,
          );
          recordedDestinationRoot = parseFileIdentityRecord(
            recordedDestination.root,
          );
          recordedJournalFilesystem = normalizeFilesystemProfile(
            recordedJournal.filesystem,
          );
          recordedJournalRoot = parseFileIdentityRecord(recordedJournal.root);
          recordedSourceDirectoryIdentity = parseFileIdentityRecord(
            recordedSource.directoryIdentity,
          );
          recordedSourceFilesystem = normalizeFilesystemProfile(
            recordedSource.filesystem,
          );
          recordedSourceRoot = parseFileIdentityRecord(recordedSource.root);
          recordedSourceRootFilesystem = normalizeFilesystemProfile(
            recordedSource.rootFilesystem,
          );
        } catch {
          failStorageContinuity();
        }
      }
      const topologyProbeCode =
        observed.record?.state === "committed"
          ? "published_state_invalid"
          : ["prepared", "materialized"].includes(observed.record?.state)
            ? "publication_outcome_uncertain"
            : "publication_io_failed";
      const topologyProbeState =
        observed.record?.state === "committed"
          ? "committed"
          : ["prepared", "materialized"].includes(observed.record?.state)
            ? "uncertain"
            : "not-committed";
      const objectIdentityProbeCode =
        observed.record === null
          ? "unsupported_publication_filesystem"
          : topologyProbeCode;
      const targetFilesystem = await inspectFilesystem(
        this.#inspectFilesystem,
        targetAuthority.path,
        objectIdentityProbeCode,
        topologyProbeState,
      );
      const targetRootObjectId = await inspectBoundObjectIdentity(
        this.#inspectPersistentObjectIdentity,
        observedObjectIdentities,
        targetAuthority.path,
        targetAuthority.identity,
        targetFilesystem,
        objectIdentityProbeCode,
        topologyProbeState,
      );
      const stableTargetFilesystem = await inspectFilesystem(
        this.#inspectFilesystem,
        targetAuthority.path,
        objectIdentityProbeCode,
        topologyProbeState,
      );
      ensure(
        profileMatches(targetFilesystem, stableTargetFilesystem),
        objectIdentityProbeCode,
        topologyProbeState,
      );
      if (
        observed.record !== null &&
        (!profileMatches(recordedDestinationFilesystem, targetFilesystem) ||
          !persistentIdentityMatches(
            recordedDestinationRoot,
            targetRootObjectId,
            targetFilesystem,
          ))
      ) {
        failStorageContinuity();
      }
      revalidateTargetPersistentIdentity = async (state) => {
        const committed = state === "committed";
        const uncertain =
          !committed &&
          (state === "materialized" || publicationMayHaveOccurred);
        const code = committed
          ? "published_state_invalid"
          : uncertain
            ? "publication_outcome_uncertain"
            : "publication_integrity_failed";
        const stateClassification = committed
          ? "committed"
          : uncertain
            ? "uncertain"
            : "not-committed";
        const currentFilesystem = await inspectFilesystem(
          this.#inspectFilesystem,
          targetAuthority.path,
          code,
          stateClassification,
        );
        const currentObjectId = await inspectBoundObjectIdentity(
          this.#inspectPersistentObjectIdentity,
          observedObjectIdentities,
          targetAuthority.path,
          targetAuthority.identity,
          currentFilesystem,
          code,
          stateClassification,
        );
        const stableCurrentFilesystem = await inspectFilesystem(
          this.#inspectFilesystem,
          targetAuthority.path,
          code,
          stateClassification,
        );
        ensure(
          profileMatches(currentFilesystem, stableCurrentFilesystem) &&
            profileMatches(targetFilesystem, currentFilesystem) &&
            targetRootObjectId === currentObjectId,
          code,
          stateClassification,
        );
        try {
          await targetAuthority.assertCurrent();
        } catch {
          fail(code, stateClassification);
        }
      };
      const observedCandidate = await inspectPath(
        candidatePath,
        topologyProbeCode,
        topologyProbeState,
      );
      const observedFinal = await inspectPath(
        finalPath,
        topologyProbeCode,
        topologyProbeState,
      );
      if (observed.record === null) {
        ensure(
          observedCandidate === null && observedFinal === null,
          "publication_recovery_required",
        );
      } else if (
        observedFinal !== null &&
        ["prepared", "materialized", "committed"].includes(
          observed.record.state,
        )
      ) {
        publicationMayHaveOccurred = true;
      } else if (
        observed.record.state === "prepared" &&
        observedFinal === null
      ) {
        await assertPublicationAuthoritiesForState(
          sourceAuthority,
          targetAuthority,
          observed.record.state,
        );
        publicationMayHaveOccurred = false;
      } else if (
        observed.record.state === "materialized" &&
        observedCandidate !== null &&
        observedFinal === null
      ) {
        await assertPublicationAuthoritiesForState(
          sourceAuthority,
          targetAuthority,
          observed.record.state,
        );
        publicationMayHaveOccurred = false;
      }
      if (sourceContinuityFailed) {
        if (publicationMayHaveOccurred) {
          fail("publication_outcome_uncertain", "uncertain");
        }
        fail(
          observedState === "prepared"
            ? "publication_recovery_required"
            : "publication_integrity_failed",
          "not-committed",
        );
      }
      const continuityCode =
        observed.record?.state === "committed"
          ? "published_state_invalid"
          : publicationMayHaveOccurred
            ? "publication_outcome_uncertain"
            : observed.record === null
              ? "unsupported_publication_filesystem"
              : "publication_recovery_required";
      const continuityState =
        observed.record?.state === "committed"
          ? "committed"
          : publicationMayHaveOccurred
            ? "uncertain"
            : "not-committed";
      const sourceRootFilesystem = await inspectFilesystem(
        this.#inspectFilesystem,
        sourceAuthority.path,
        continuityCode,
        continuityState,
      );
      const journalRootObjectId = await inspectBoundObjectIdentity(
        this.#inspectPersistentObjectIdentity,
        observedObjectIdentities,
        journalAuthority.path,
        journalIdentity,
        journalFilesystem,
        continuityCode,
        continuityState,
      );
      const sourceRootObjectId = await inspectBoundObjectIdentity(
        this.#inspectPersistentObjectIdentity,
        observedObjectIdentities,
        sourceAuthority.path,
        sourceAuthority.identity,
        sourceRootFilesystem,
        continuityCode,
        continuityState,
      );
      const stableJournalFilesystem = await inspectFilesystem(
        this.#inspectFilesystem,
        journalAuthority.path,
        continuityCode,
        continuityState,
      );
      const stableSourceRootFilesystem = await inspectFilesystem(
        this.#inspectFilesystem,
        sourceAuthority.path,
        continuityCode,
        continuityState,
      );
      ensure(
        profileMatches(journalFilesystem, stableJournalFilesystem) &&
          profileMatches(sourceRootFilesystem, stableSourceRootFilesystem),
        continuityCode,
        continuityState,
      );
      if (observed.record !== null) {
        if (
          !profileMatches(recordedJournalFilesystem, journalFilesystem) ||
          !persistentIdentityMatches(
            recordedJournalRoot,
            journalRootObjectId,
            journalFilesystem,
          ) ||
          !persistentIdentityMatches(
            recordedSourceRoot,
            sourceRootObjectId,
            sourceRootFilesystem,
          ) ||
          !profileMatches(
            recordedSourceRootFilesystem,
            sourceRootFilesystem,
          )
        ) {
          failStorageContinuity();
        }
      }
      await assertPublicationAuthoritiesForState(
        sourceAuthority,
        targetAuthority,
        observed.record?.state,
      );
      let sourceFilesystem;
      let sourceDirectoryObjectId;
      if (
        observed.record !== null &&
        ["materialized", "committed"].includes(observed.record.state)
      ) {
        sourceFilesystem = recordedSourceFilesystem;
        sourceDirectoryObjectId = recordedSourceDirectoryIdentity.objectId;
      } else if (source.identity !== null) {
        sourceFilesystem = await inspectFilesystem(
          this.#inspectFilesystem,
          source.path,
          continuityCode,
          continuityState,
        );
        sourceDirectoryObjectId = await inspectBoundObjectIdentity(
          this.#inspectPersistentObjectIdentity,
          observedObjectIdentities,
          source.path,
          source.identity,
          sourceFilesystem,
          continuityCode,
          continuityState,
        );
        const stableSourceFilesystem = await inspectFilesystem(
          this.#inspectFilesystem,
          source.path,
          continuityCode,
          continuityState,
        );
        ensure(
          profileMatches(sourceFilesystem, stableSourceFilesystem),
          continuityCode,
          continuityState,
        );
        await assertPublicationAuthoritiesForState(
          sourceAuthority,
          targetAuthority,
          observed.record?.state,
        );
      } else {
        if (observed.record === null) fail("invalid_publication_request");
        fail("publication_recovery_required");
      }
      if (observed.record?.state === "prepared") {
        ensure(
          persistentIdentityMatches(
            recordedSourceDirectoryIdentity,
            sourceDirectoryObjectId,
            sourceFilesystem,
          ) && profileMatches(recordedSourceFilesystem, sourceFilesystem),
          "publication_recovery_required",
        );
      }
      const sourceDirectoryIdentity =
        recordedSourceDirectoryIdentity ??
        persistentFileIdentityRecord(
          sourceDirectoryObjectId,
          sourceFilesystem,
        );
      await revalidateLockedTopology(observed.record?.state ?? "absent");
      const publicationBinding = Object.freeze({
        contractVersion: PUBLICATION_CONTRACT_VERSION,
        journal: Object.freeze({
          filesystem: journalFilesystem,
          root: persistentFileIdentityRecord(
            journalRootObjectId,
            journalFilesystem,
          ),
        }),
        publicationKind: options.kind,
        source: Object.freeze({
          artifactProof,
          directoryIdentity: sourceDirectoryIdentity,
          filesystem: sourceFilesystem,
          name: source.name,
          root: persistentFileIdentityRecord(
            sourceRootObjectId,
            sourceRootFilesystem,
          ),
          rootFilesystem: sourceRootFilesystem,
        }),
        destination: Object.freeze({
          candidateName,
          filesystem: targetFilesystem,
          name: finalName,
          root: persistentFileIdentityRecord(
            targetRootObjectId,
            targetFilesystem,
          ),
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
      const runJournalObservableOperation = async (operation) => {
        const previousPublicationMayHaveOccurred = publicationMayHaveOccurred;
        publicationMayHaveOccurred = true;
        let operationError;
        let operationResult;
        try {
          operationResult = await operation();
        } catch (error) {
          operationError = error;
        }
        const committed = observed.record?.state === "committed";
        const code = committed
          ? "published_state_invalid"
          : "publication_outcome_uncertain";
        const stateClassification = committed ? "committed" : "uncertain";
        await assertUntrustedLockHeld(lock);
        await revalidateTargetPersistentIdentity(
          committed ? "committed" : "materialized",
        );
        const currentCandidate = await inspectPath(
          candidatePath,
          code,
          stateClassification,
        );
        const currentFinal = await inspectPath(
          finalPath,
          code,
          stateClassification,
        );
        const sameObservedIdentity = (before, after) =>
          before === null
            ? after === null
            : after !== null && sameFileIdentity(before, after);
        ensure(
          sameObservedIdentity(observedCandidate, currentCandidate) &&
            sameObservedIdentity(observedFinal, currentFinal),
          code,
          stateClassification,
        );
        publicationMayHaveOccurred = previousPublicationMayHaveOccurred;
        if (operationError !== undefined) throw operationError;
        return operationResult;
      };
      const verifyCommittedPublication = async ({ checkpoint, materialization }) => {
        try {
          await assertPathAbsent(
            candidatePath,
            "published_state_invalid",
            "committed",
          );
          await revalidateLockedTopology("committed", finalPath, "final");
          pinnedPublication ??= await openPinnedDirectory(
            finalPath,
            materialization,
            {
              committed: true,
              filesystem: targetFilesystem,
              inspectPersistentObjectIdentity:
                this.#inspectPersistentObjectIdentity,
              observedObjectIdentities,
              inspectOwnedRootAcl: this.#inspectOwnedRootAcl,
              inspectOwnedRootAncestorAcl: this.#inspectOwnedRootAncestorAcl,
            },
          );
          await assertPinnedPath(
            finalPath,
            pinnedPublication,
            "published_state_invalid",
            "committed",
          );
          await syncStoppedTree(finalPath, {
            listMountPoints: this.#listMountPoints,
          });
          await targetAuthority.assertCurrent();
          await targetAuthority.handle.sync();
          await this.#verifyPublishedTree({
            checkpoint,
            committed: true,
            kind: options.kind,
            materialization,
            observedObjectIdentities,
            path: finalPath,
          });
          await assertPinnedPath(
            finalPath,
            pinnedPublication,
            "published_state_invalid",
            "committed",
          );
        } catch (error) {
          if (
            internalErrors.has(error) &&
            error.code === "published_state_invalid" &&
            error.commitState === "committed"
          ) {
            throw error;
          }
          fail("published_state_invalid", "committed");
        }
      };
      const prepared = await runJournalObservableOperation(() =>
        options.requireFreshOperation
          ? reflectApply(journalPrepareFreshIntrinsic, this.#journal, [
              journalInput,
            ])
          : this.#journal.prepare(journalInput),
      );
      const state = prepared.record.state;
      await revalidateLockedTopology(state);
      if (state === "prepared" && !prepared.replayed) {
        await runFault(this.#faults.afterJournalPrepared);
        await revalidateLockedTopology(state);
      }
      if (state === "committed") {
        commitState = "committed";
        const materialization = validateMaterialization(prepared.record.materialization, {
          artifactProof,
          committed: true,
          finalName,
          kind: options.kind,
          operationId,
        });
        await verifyCommittedPublication({
          checkpoint: prepared.record.result.checkpoint,
          materialization,
        });
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
            journalIdentity,
            kind: options.kind,
            lock,
            operationId,
            artifactProof,
            runCandidateObservableOperation,
            source,
            sourceAuthority,
            sourceFilesystem,
            sourcePersistentIdentity: sourceDirectoryIdentity,
            targetAuthority,
            observedObjectIdentities,
            revalidateTargetPersistentIdentity,
            targetFilesystem,
          });
          pinnedPublication = created.pinned;
          await runCandidateObservableOperation(
            this.#faults.afterCandidateBarrier,
            { fault: true },
          );
          await revalidateLockedTopology(state, candidatePath, "candidate");
          try {
            await assertPinnedPath(
              candidatePath,
              pinnedPublication,
              "publication_integrity_failed",
              "not-committed",
            );
            await syncStoppedTree(candidatePath, {
              listMountPoints: this.#listMountPoints,
            });
            await targetAuthority.assertCurrent();
            await targetAuthority.handle.sync();
            await this.#verifyPublishedTree({
              checkpoint: prepared.record.result.checkpoint,
              kind: options.kind,
              materialization: created.materialization,
              observedObjectIdentities,
              path: candidatePath,
            });
            await assertPinnedPath(
              candidatePath,
              pinnedPublication,
              "publication_integrity_failed",
              "not-committed",
            );
          } catch (error) {
            if (internalErrors.has(error)) throw error;
            fail("publication_integrity_failed");
          }
          await runCandidateObservableOperation(
            async () => {
              materialized = await this.#journal.markMaterialized({
                ...journalInput,
                materialization: created.materialization,
              });
            },
          );
          await runCandidateObservableOperation(
            this.#faults.afterMaterialized,
            { fault: true },
          );
          await revalidateLockedTopology(
            materialized.record.state,
            candidatePath,
            "candidate",
          );
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
          await revalidateLockedTopology(
            materialized.record.state,
            candidatePath,
            "candidate",
          );
          pinnedPublication ??= await openPinnedDirectory(
            candidatePath,
            materialization,
            {
              filesystem: targetFilesystem,
              inspectPersistentObjectIdentity:
                this.#inspectPersistentObjectIdentity,
              observedObjectIdentities,
              inspectOwnedRootAcl: this.#inspectOwnedRootAcl,
              inspectOwnedRootAncestorAcl: this.#inspectOwnedRootAncestorAcl,
            },
          );
          // A durable materialized record with a mismatched candidate must be
          // reconciled by a trusted recovery path; it is never recopied.
          try {
            await this.#verifyPublishedTree({
              checkpoint: materialized.record.result.checkpoint,
              kind: options.kind,
              materialization,
              observedObjectIdentities,
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
          await runCandidateObservableOperation(
            this.#faults.beforeRename,
            { fault: true },
          );
          await revalidateLockedTopology(
            materialized.record.state,
            candidatePath,
            "candidate",
          );
          try {
            await assertPinnedPath(
              candidatePath,
              pinnedPublication,
              "publication_recovery_required",
              "not-committed",
            );
            await syncStoppedTree(candidatePath, {
              listMountPoints: this.#listMountPoints,
            });
            await targetAuthority.assertCurrent();
            await targetAuthority.handle.sync();
            await this.#verifyPublishedTree({
              checkpoint: materialized.record.result.checkpoint,
              kind: options.kind,
              materialization,
              observedObjectIdentities,
              path: candidatePath,
            });
            await assertPinnedPath(
              candidatePath,
              pinnedPublication,
              "publication_recovery_required",
              "not-committed",
            );
          } catch {
            fail("publication_recovery_required");
          }
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
          await revalidateLockedTopology(
            materialized.record.state,
            finalPath,
            "final",
          );
        } else {
          publicationMayHaveOccurred = true;
          await revalidateLockedTopology(
            materialized.record.state,
            finalPath,
            "final",
          );
          pinnedPublication = await openPinnedDirectory(finalPath, materialization, {
            filesystem: targetFilesystem,
            inspectPersistentObjectIdentity:
              this.#inspectPersistentObjectIdentity,
            observedObjectIdentities,
            inspectOwnedRootAcl: this.#inspectOwnedRootAcl,
            inspectOwnedRootAncestorAcl: this.#inspectOwnedRootAncestorAcl,
          });
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
            sameFileIdentity(visibleIdentity, pinnedPublication.identity),
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
        await revalidateLockedTopology(
          materialized.record.state,
          finalPath,
          "final",
        );
        try {
          await this.#verifyPublishedTree({
            checkpoint: materialized.record.result.checkpoint,
            kind: options.kind,
            materialization,
            observedObjectIdentities,
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
        await revalidateLockedTopology(
          materialized.record.state,
          finalPath,
          "final",
        );
        await runFault(this.#faults.beforeCommit);
        await revalidateLockedTopology(
          materialized.record.state,
          finalPath,
          "final",
        );
        try {
          await assertPinnedPath(
            finalPath,
            pinnedPublication,
            "publication_outcome_uncertain",
            "uncertain",
          );
          await syncStoppedTree(finalPath, {
            listMountPoints: this.#listMountPoints,
          });
          await targetAuthority.assertCurrent();
          await targetAuthority.handle.sync();
          await this.#verifyPublishedTree({
            checkpoint: materialized.record.result.checkpoint,
            kind: options.kind,
            materialization,
            observedObjectIdentities,
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
        await verifyCommittedPublication({
          checkpoint: committed.record.result.checkpoint,
          materialization,
        });
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
    journalIdentity,
    kind,
    lock,
    operationId,
    runCandidateObservableOperation,
    source,
    sourceAuthority,
    sourceFilesystem,
    sourcePersistentIdentity,
    targetAuthority,
    observedObjectIdentities,
    revalidateTargetPersistentIdentity,
    targetFilesystem,
  }) {
    let sourceTree = source.path;
    let sourceOwnedRoot = sourceAuthority.path;
    let sourceDigest;
    let artifactManifestDigest;
    let provisionalCandidate;
    let provisionalCandidateClosed = false;
    const publicationTopology = Object.freeze({
      journalAuthority,
      journalIdentity,
      listMountPoints: this.#listMountPoints,
      source,
      sourceAuthority,
      targetAuthority,
    });
    const assertSourcePersistentIdentity = async () => {
      let currentSourceFilesystem;
      try {
        currentSourceFilesystem = normalizeFilesystemProfile(
          await this.#inspectFilesystem(source.path),
        );
      } catch {
        fail("publication_integrity_failed");
      }
      ensure(
        currentSourceFilesystem.durability === sourceFilesystem.durability &&
          currentSourceFilesystem.filesystemId === sourceFilesystem.filesystemId &&
          currentSourceFilesystem.objectIdentityScheme ===
            sourceFilesystem.objectIdentityScheme &&
          currentSourceFilesystem.type === sourceFilesystem.type,
        "publication_integrity_failed",
      );
      await assertDirectoryIdentity(
        source.path,
        source.identity,
        "publication_integrity_failed",
        "not-committed",
      );
      const objectId = await inspectBoundObjectIdentity(
        this.#inspectPersistentObjectIdentity,
        observedObjectIdentities,
        source.path,
        source.identity,
        currentSourceFilesystem,
        "publication_integrity_failed",
        "not-committed",
      );
      let stableSourceFilesystem;
      try {
        stableSourceFilesystem = normalizeFilesystemProfile(
          await this.#inspectFilesystem(source.path),
        );
      } catch {
        fail("publication_integrity_failed");
      }
      ensure(
        stableSourceFilesystem.durability ===
            currentSourceFilesystem.durability &&
          stableSourceFilesystem.filesystemId ===
            currentSourceFilesystem.filesystemId &&
          stableSourceFilesystem.objectIdentityScheme ===
            currentSourceFilesystem.objectIdentityScheme &&
          stableSourceFilesystem.type === currentSourceFilesystem.type &&
          sourcePersistentIdentity.filesystemId === sourceFilesystem.filesystemId &&
          sourcePersistentIdentity.objectIdentityScheme ===
            sourceFilesystem.objectIdentityScheme &&
          sourcePersistentIdentity.objectId === objectId,
        "publication_integrity_failed",
      );
    };
    const revalidateAfterCallback = async () => {
      await assertUntrustedLockHeld(lock);
      await assertPublicationTopologyForState(publicationTopology, "prepared");
      await revalidateTargetPersistentIdentity("prepared");
      await assertSourcePersistentIdentity();
    };
    const pinCandidate = async () => {
      ensure(provisionalCandidate === undefined, "publication_integrity_failed");
      try {
        provisionalCandidate = await openStoppedTreeRootAuthority(candidatePath, {
          inspectOwnedRootAcl: this.#inspectOwnedRootAcl,
          inspectOwnedRootAncestorAcl: this.#inspectOwnedRootAncestorAcl,
        });
      } catch {
        fail("publication_integrity_failed");
      }
    };
    const runObservableCandidateFault = async (operation) => {
      ensure(provisionalCandidate !== undefined, "publication_integrity_failed");
      await runCandidateObservableOperation(operation, {
        fault: true,
      });
    };
    try {
      await revalidateAfterCallback();
    await assertSourcePersistentIdentity();
    if (kind === "restore-destination") {
      await assertExactCheckpointBundleRoot(source.path);
      const manifest = await readArtifactManifest(
        join(source.path, "artifact.json"),
        checkpoint,
        artifactProof,
      );
      sourceTree = join(source.path, "payload");
      sourceOwnedRoot = source.path;
      await syncStoppedTree(source.path, {
        allowRootMount: true,
        listMountPoints: this.#listMountPoints,
      });
      await sourceAuthority.handle.sync();
      await assertExactCheckpointBundleRoot(source.path);
      const stableManifest = await readArtifactManifest(
        join(source.path, "artifact.json"),
        checkpoint,
        artifactProof,
      );
      ensure(
        stableManifest.digest === manifest.digest,
        "publication_integrity_failed",
      );
      sourceDigest = await digestTree(sourceTree, {
        listMountPoints: this.#listMountPoints,
      });
      ensure(
        sourceDigest === manifest.manifest.modeledDigest,
        "publication_integrity_failed",
      );
      artifactManifestDigest = manifest.digest;
    } else {
      await syncStoppedTree(source.path, {
        allowRootMount: true,
        listMountPoints: this.#listMountPoints,
      });
      await sourceAuthority.handle.sync();
      sourceDigest = await digestTree(source.path, {
        allowRootMount: true,
        listMountPoints: this.#listMountPoints,
      });
      artifactManifestDigest = sha256("pending-artifact-manifest\0", operationId);
    }
    await runFault(this.#faults.afterSourceBarrier);
    await revalidateAfterCallback();
    await assertSourcePersistentIdentity();
    if (kind === "restore-destination") {
      await assertExactCheckpointBundleRoot(source.path);
    }

    if (kind === "checkpoint-artifact") {
      await createPrivateDirectory(candidatePath);
      await pinCandidate();
      await runObservableCandidateFault(this.#faults.afterCandidateCreated);
      await revalidateAfterCallback();
      await assertSourcePersistentIdentity();
      const payload = join(candidatePath, "payload");
      await copyStoppedTreeBetweenRoots({
        allowAbsoluteSymlinks: false,
        allowSourceRootMount: true,
        destination: payload,
        destinationOwnedRoot: candidatePath,
        forbiddenAbsoluteSymlinkAuthorities: [journalAuthority],
        expectedSourceRootIdentity: source.identity,
        inspectOwnedRootAcl: this.#inspectOwnedRootAcl,
        inspectOwnedRootAncestorAcl: this.#inspectOwnedRootAncestorAcl,
        listMountPoints: this.#listMountPoints,
        source: source.path,
        sourceOwnedRoot: sourceAuthority.path,
      });
      await runObservableCandidateFault(this.#faults.afterCopy);
      await revalidateAfterCallback();
      await assertSourcePersistentIdentity();
      const copiedDigest = await digestTree(payload, {
        listMountPoints: this.#listMountPoints,
      });
      ensure(copiedDigest === sourceDigest, "publication_integrity_failed");
      const manifest = artifactManifest(checkpoint, operationId, copiedDigest);
      artifactManifestDigest = await writeArtifactManifest(
        join(candidatePath, "artifact.json"),
        manifest,
      );
    } else {
      await copyStoppedTreeBetweenRoots({
        allowAbsoluteSymlinks: false,
        afterDestinationRootCreated: async () => {
          await pinCandidate();
          await runObservableCandidateFault(this.#faults.afterCandidateCreated);
          await revalidateAfterCallback();
          await assertSourcePersistentIdentity();
          await assertExactCheckpointBundleRoot(source.path);
        },
        destination: candidatePath,
        destinationOwnedRoot: targetAuthority.path,
        forbiddenAbsoluteSymlinkAuthorities: [journalAuthority],
        inspectOwnedRootAcl: this.#inspectOwnedRootAcl,
        inspectOwnedRootAncestorAcl: this.#inspectOwnedRootAncestorAcl,
        listMountPoints: this.#listMountPoints,
        source: sourceTree,
        sourceOwnedRoot,
      });
      await runObservableCandidateFault(this.#faults.afterCopy);
      await revalidateAfterCallback();
      await assertSourcePersistentIdentity();
      await assertExactCheckpointBundleRoot(source.path);
    }

    await syncStoppedTree(candidatePath, {
      listMountPoints: this.#listMountPoints,
    });
    await targetAuthority.handle.sync();
    const modeledPath =
      kind === "checkpoint-artifact" ? join(candidatePath, "payload") : candidatePath;
    const copiedDigest = await digestTree(modeledPath, {
      listMountPoints: this.#listMountPoints,
    });
    const sourceDigestAfterCopy = await digestTree(sourceTree, {
      allowRootMount: kind === "checkpoint-artifact",
      listMountPoints: this.#listMountPoints,
    });
    await assertSourcePersistentIdentity();
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
      await assertExactCheckpointBundleRoot(source.path);
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
    ensure(
      identity.isDirectory() &&
        provisionalCandidate !== undefined &&
        sameFileIdentity(identity, provisionalCandidate.identity) &&
        sameFileIdentity(
          identity,
          await provisionalCandidate.handle.stat({ bigint: true }),
        ),
      "publication_integrity_failed",
    );
    await assertSourcePublicationIdentityDisjoint(
      source,
      candidatePath,
      "publication_integrity_failed",
      "not-committed",
      this.#listMountPoints,
    );
    let treeIdentityDigest;
    let objectId;
    try {
      objectId = await inspectBoundObjectIdentity(
        this.#inspectPersistentObjectIdentity,
        observedObjectIdentities,
        candidatePath,
        identity,
        targetFilesystem,
        "publication_integrity_failed",
        "not-committed",
      );
      treeIdentityDigest = await digestStoppedTreeIdentities(
        candidatePath,
        targetFilesystem.filesystemId,
        targetFilesystem.objectIdentityScheme,
        this.#inspectPersistentObjectIdentity,
        observedObjectIdentities,
        { listMountPoints: this.#listMountPoints },
      );
    } catch {
      fail("publication_integrity_failed");
    }
    const materialization = materializationFor({
      artifactManifestDigest,
      filesystem: targetFilesystem,
      finalName,
      kind,
      modeledDigest: copiedDigest,
      objectId,
      operationId,
      treeIdentityDigest,
    });
      const pinned = await openPinnedDirectory(candidatePath, materialization, {
        filesystem: targetFilesystem,
        inspectPersistentObjectIdentity:
          this.#inspectPersistentObjectIdentity,
        observedObjectIdentities,
        inspectOwnedRootAcl: this.#inspectOwnedRootAcl,
        inspectOwnedRootAncestorAcl: this.#inspectOwnedRootAncestorAcl,
      });
      if (
        provisionalCandidate === undefined ||
        !sameFileIdentity(pinned.identity, provisionalCandidate.identity)
      ) {
        await pinned.handle.close().catch(() => {});
        fail("publication_integrity_failed");
      }
      try {
        await provisionalCandidate.handle.close();
        provisionalCandidateClosed = true;
      } catch {
        await pinned.handle.close().catch(() => {});
        fail("publication_integrity_failed");
      }
      return Object.freeze({ materialization, pinned });
    } finally {
      if (!provisionalCandidateClosed) {
        await provisionalCandidate?.handle.close().catch(() => {});
      }
    }
  }

  async #verifyPublishedTree({
    checkpoint,
    committed = false,
    kind,
    materialization,
    observedObjectIdentities,
    path,
  }) {
    try {
      ensure(
        (await digestStoppedTreeIdentities(
          path,
          materialization.stagedRoot.filesystemId,
          materialization.stagedRoot.objectIdentityScheme,
          this.#inspectPersistentObjectIdentity,
          observedObjectIdentities,
          { listMountPoints: this.#listMountPoints },
        )) ===
          materialization.treeIdentityDigest,
        "publication_integrity_failed",
      );
      if (kind === "checkpoint-artifact") {
        await assertExactCheckpointBundleRoot(path);
        const manifest = await readArtifactManifest(join(path, "artifact.json"), checkpoint);
        const digest = await digestTree(join(path, "payload"), {
          listMountPoints: this.#listMountPoints,
        });
        ensure(
          manifest.digest === materialization.artifactManifestDigest &&
            manifest.manifest.modeledDigest === materialization.modeledDigest &&
            digest === materialization.modeledDigest,
          "publication_integrity_failed",
        );
      } else {
        ensure(
          (await digestTree(path, {
            listMountPoints: this.#listMountPoints,
          })) === materialization.modeledDigest,
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
