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
  openStoppedTreeModeledRootAuthority,
  openStoppedTreeRootAuthority,
  sameFileIdentity,
  stoppedTreeContainsAnyIdentity,
  stoppedTreesShareAnyIdentity,
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

function fileIdentityRecord(identity) {
  return Object.freeze({
    device: identity.dev.toString(),
    inode: identity.ino.toString(),
  });
}

function rootIdentity(authority) {
  return fileIdentityRecord(authority.identity);
}

function parseFileIdentityRecord(value) {
  const record = exactOptions(
    value,
    ["device", "inode"],
    ["device", "inode"],
    "publication_integrity_failed",
  );
  ensure(
    typeof record.device === "string" &&
      /^(?:0|[1-9][0-9]*)$/u.test(record.device) &&
      typeof record.inode === "string" &&
      /^[1-9][0-9]*$/u.test(record.inode),
    "publication_integrity_failed",
  );
  return Object.freeze({
    identity: Object.freeze({
      dev: BigInt(record.device),
      ino: BigInt(record.inode),
    }),
    record: Object.freeze({
      device: record.device,
      inode: record.inode,
    }),
  });
}

async function inspectDirectSource(authority, value) {
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
    name: basename(path),
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
) {
  if (source.identity === null) return;
  let overlaps;
  try {
    overlaps = await stoppedTreesShareAnyIdentity(source.path, publicationPath);
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
    ["device", "inode"],
    ["device", "inode"],
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
      typeof stagedRoot.device === "string" &&
      /^(?:0|[1-9][0-9]*)$/u.test(stagedRoot.device) &&
      typeof stagedRoot.inode === "string" &&
      /^[1-9][0-9]*$/u.test(stagedRoot.inode),
    code,
    commitState,
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
  treeIdentityDigest,
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
    treeIdentityDigest,
  });
}

function identityMatchesMaterialization(identity, materialization) {
  return (
    identity.dev.toString() === materialization.stagedRoot.device &&
    identity.ino.toString() === materialization.stagedRoot.inode
  );
}

async function openPinnedDirectory(
  path,
  materialization,
  {
    committed = false,
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
    ensure(
      identityMatchesMaterialization(metadata, materialization),
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
  #inspectOwnedRootAcl;
  #inspectOwnedRootAncestorAcl;
  #journal;
  #trustRenameOutcome;

  constructor(options) {
    const normalized = exactOptions(
      options,
      [
        "acquireLock",
        "faults",
        "inspectFilesystem",
        "inspectOwnedRootAcl",
        "inspectOwnedRootAncestorAcl",
        "journal",
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
    this.#inspectOwnedRootAcl = normalized.inspectOwnedRootAcl;
    this.#inspectOwnedRootAncestorAcl = normalized.inspectOwnedRootAncestorAcl;
    this.#faults = normalizeFaults(normalized.faults);
    ensure(
      typeof this.#acquireLock === "function" &&
        typeof this.#inspectFilesystem === "function" &&
        (this.#inspectOwnedRootAcl === undefined ||
          typeof this.#inspectOwnedRootAcl === "function") &&
        (this.#inspectOwnedRootAncestorAcl === undefined ||
          typeof this.#inspectOwnedRootAncestorAcl === "function"),
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
      const source = await inspectDirectSource(
        sourceAuthority,
        options.sourceDirectory,
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

      publicationMayHaveOccurred = true;
      const journalAuthority = await this.#journal.describeAuthority();
      const journalFilesystem = await inspectFilesystem(
        this.#inspectFilesystem,
        journalAuthority.path,
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
      const publicationTopology = Object.freeze({
        journalAuthority,
        journalIdentity,
        source,
        sourceAuthority,
        targetAuthority,
      });
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
          );
        }
      };
      await assertPublicationTopology(publicationTopology);

      try {
        lock = await this.#acquireLock(
          join(targetAuthority.path, ".stopped-directory-publication.lock"),
        );
        await lock.assertHeld();
      } catch {
        fail("publication_outcome_uncertain", "uncertain");
      }
      await revalidateLockedTopology("unknown");
      const observed = await this.#journal.read({ operationId });
      publicationMayHaveOccurred = false;
      if (observed.record?.state === "committed") {
        historicalCommitConfirmed = true;
        commitState = "committed";
      } else if (observed.record?.state === "materialized") {
        publicationMayHaveOccurred = true;
      }
      await assertPublicationAuthoritiesForState(
        sourceAuthority,
        targetAuthority,
        observed.record?.state,
      );
      const topologyProbeCode =
        observed.record?.state === "committed"
          ? "published_state_invalid"
          : observed.record?.state === "materialized"
            ? "publication_outcome_uncertain"
            : "publication_io_failed";
      const topologyProbeState =
        observed.record?.state === "committed"
          ? "committed"
          : observed.record?.state === "materialized"
            ? "uncertain"
            : "not-committed";
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
        ["materialized", "committed"].includes(observed.record.state)
      ) {
        publicationMayHaveOccurred = true;
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
      let recordedSource = null;
      let recordedSourceDirectoryIdentity = null;
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
          recordedSource = exactOptions(recordedPublication.source, [
            "artifactProof",
            "directoryIdentity",
            "filesystem",
            "name",
            "root",
          ]);
          recordedSourceDirectoryIdentity = parseFileIdentityRecord(
            recordedSource.directoryIdentity,
          );
        } catch {
          if (observed.record.state === "committed") {
            fail("published_state_invalid", "committed");
          }
          fail("publication_recovery_required");
        }
      }
      if (observed.record?.state === "prepared") {
        ensure(
          source.identity !== null &&
            sameFileIdentity(
              source.identity,
              recordedSourceDirectoryIdentity.identity,
            ),
          "publication_recovery_required",
        );
      }
      const targetFilesystem = await inspectFilesystem(
        this.#inspectFilesystem,
        targetAuthority.path,
      );
      await assertPublicationAuthoritiesForState(
        sourceAuthority,
        targetAuthority,
        observed.record?.state,
      );
      let sourceFilesystem;
      if (
        observed.record !== null &&
        ["materialized", "committed"].includes(observed.record.state)
      ) {
        try {
          sourceFilesystem = normalizeFilesystemProfile(recordedSource.filesystem);
        } catch {
          if (observed.record.state === "committed") {
            fail("published_state_invalid", "committed");
          }
          fail("publication_recovery_required");
        }
      } else if (source.identity !== null) {
        sourceFilesystem = await inspectFilesystem(this.#inspectFilesystem, source.path);
        await assertPublicationAuthoritiesForState(
          sourceAuthority,
          targetAuthority,
          observed.record?.state,
        );
      } else {
        if (observed.record === null) fail("invalid_publication_request");
        fail("publication_recovery_required");
      }
      const sourceDirectoryIdentity =
        recordedSourceDirectoryIdentity?.record ??
        fileIdentityRecord(source.identity);
      await revalidateLockedTopology(observed.record?.state ?? "absent");
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
          directoryIdentity: sourceDirectoryIdentity,
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
        await revalidateLockedTopology(state, finalPath, "final");
        pinnedPublication = await openPinnedDirectory(finalPath, materialization, {
          committed: true,
          inspectOwnedRootAcl: this.#inspectOwnedRootAcl,
          inspectOwnedRootAncestorAcl: this.#inspectOwnedRootAncestorAcl,
        });
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
            journalIdentity,
            kind: options.kind,
            lock,
            operationId,
            artifactProof,
            source,
            sourceAuthority,
            targetAuthority,
          });
          pinnedPublication = created.pinned;
          await runFault(this.#faults.afterCandidateBarrier);
          await revalidateLockedTopology(state, candidatePath, "candidate");
          try {
            await assertPinnedPath(
              candidatePath,
              pinnedPublication,
              "publication_integrity_failed",
              "not-committed",
            );
            await syncStoppedTree(candidatePath);
            await targetAuthority.assertCurrent();
            await targetAuthority.handle.sync();
            await this.#verifyPublishedTree({
              checkpoint: prepared.record.result.checkpoint,
              kind: options.kind,
              materialization: created.materialization,
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
          materialized = await this.#journal.markMaterialized({
            ...journalInput,
            materialization: created.materialization,
          });
          await runFault(this.#faults.afterMaterialized);
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
          publicationMayHaveOccurred = true;
          let beforeRenameError;
          try {
            await runFault(this.#faults.beforeRename);
          } catch (error) {
            beforeRenameError = error;
          }
          await assertUntrustedLockHeld(lock);
          try {
            await targetAuthority.assertCurrent();
          } catch {
            fail("publication_outcome_uncertain", "uncertain");
          }
          const callbackFinalIdentity = await inspectPath(
            finalPath,
            "publication_outcome_uncertain",
            "uncertain",
          );
          if (
            callbackFinalIdentity !== null &&
            sameFileIdentity(callbackFinalIdentity, pinnedPublication.identity)
          ) {
            fail("publication_outcome_uncertain", "uncertain");
          }
          publicationMayHaveOccurred = false;
          if (beforeRenameError !== undefined) throw beforeRenameError;
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
            await syncStoppedTree(candidatePath);
            await targetAuthority.assertCurrent();
            await targetAuthority.handle.sync();
            await this.#verifyPublishedTree({
              checkpoint: materialized.record.result.checkpoint,
              kind: options.kind,
              materialization,
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
          await syncStoppedTree(finalPath);
          await targetAuthority.assertCurrent();
          await targetAuthority.handle.sync();
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
    journalIdentity,
    kind,
    lock,
    operationId,
    source,
    sourceAuthority,
    targetAuthority,
  }) {
    let sourceTree = source.path;
    let sourceOwnedRoot = sourceAuthority.path;
    let sourceDigest;
    let artifactManifestDigest;
    const publicationTopology = Object.freeze({
      journalAuthority,
      journalIdentity,
      source,
      sourceAuthority,
      targetAuthority,
    });
    const revalidateAfterCallback = async () => {
      await assertUntrustedLockHeld(lock);
      await assertPublicationTopologyForState(publicationTopology, "prepared");
    };
    await revalidateAfterCallback();
    await assertDirectoryIdentity(
      source.path,
      source.identity,
      "publication_integrity_failed",
      "not-committed",
    );
    if (kind === "restore-destination") {
      await assertExactCheckpointBundleRoot(source.path);
      const manifest = await readArtifactManifest(
        join(source.path, "artifact.json"),
        checkpoint,
        artifactProof,
      );
      sourceTree = join(source.path, "payload");
      sourceOwnedRoot = source.path;
      await syncStoppedTree(source.path, { allowRootMount: true });
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
    await revalidateAfterCallback();
    await assertDirectoryIdentity(
      source.path,
      source.identity,
      "publication_integrity_failed",
      "not-committed",
    );
    if (kind === "restore-destination") {
      await assertExactCheckpointBundleRoot(source.path);
    }

    if (kind === "checkpoint-artifact") {
      await createPrivateDirectory(candidatePath);
      await runFault(this.#faults.afterCandidateCreated);
      await revalidateAfterCallback();
      await assertDirectoryIdentity(
        source.path,
        source.identity,
        "publication_integrity_failed",
        "not-committed",
      );
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
        source: source.path,
        sourceOwnedRoot: sourceAuthority.path,
      });
      await runFault(this.#faults.afterCopy);
      await revalidateAfterCallback();
      await assertDirectoryIdentity(
        source.path,
        source.identity,
        "publication_integrity_failed",
        "not-committed",
      );
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
        afterDestinationRootCreated: async () => {
          await runFault(this.#faults.afterCandidateCreated);
          await revalidateAfterCallback();
          await assertDirectoryIdentity(
            source.path,
            source.identity,
            "publication_integrity_failed",
            "not-committed",
          );
          await assertExactCheckpointBundleRoot(source.path);
        },
        destination: candidatePath,
        destinationOwnedRoot: targetAuthority.path,
        forbiddenAbsoluteSymlinkAuthorities: [journalAuthority],
        inspectOwnedRootAcl: this.#inspectOwnedRootAcl,
        inspectOwnedRootAncestorAcl: this.#inspectOwnedRootAncestorAcl,
        source: sourceTree,
        sourceOwnedRoot,
      });
      await runFault(this.#faults.afterCopy);
      await revalidateAfterCallback();
      await assertDirectoryIdentity(
        source.path,
        source.identity,
        "publication_integrity_failed",
        "not-committed",
      );
      await assertExactCheckpointBundleRoot(source.path);
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
    ensure(identity.isDirectory(), "publication_integrity_failed");
    await assertSourcePublicationIdentityDisjoint(
      source,
      candidatePath,
      "publication_integrity_failed",
      "not-committed",
    );
    let treeIdentityDigest;
    try {
      treeIdentityDigest = await digestStoppedTreeIdentities(candidatePath);
    } catch {
      fail("publication_integrity_failed");
    }
    const materialization = materializationFor({
      artifactManifestDigest,
      finalName,
      identity,
      kind,
      modeledDigest: copiedDigest,
      operationId,
      treeIdentityDigest,
    });
    return Object.freeze({
      materialization,
      pinned: await openPinnedDirectory(candidatePath, materialization, {
        inspectOwnedRootAcl: this.#inspectOwnedRootAcl,
        inspectOwnedRootAncestorAcl: this.#inspectOwnedRootAncestorAcl,
      }),
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
      ensure(
        (await digestStoppedTreeIdentities(path)) ===
          materialization.treeIdentityDigest,
        "publication_integrity_failed",
      );
      if (kind === "checkpoint-artifact") {
        await assertExactCheckpointBundleRoot(path);
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
