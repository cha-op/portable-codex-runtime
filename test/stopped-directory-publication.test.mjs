import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import {
  access,
  lstat,
  mkdir,
  mkdtemp,
  readFile,
  realpath,
  rename,
  rm,
  symlink,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";
import test from "node:test";

import {
  FilesystemOperationJournal,
  operationJournalRecordFilename,
} from "../src/filesystem-operation-journal.mjs";
import {
  STOPPED_DIRECTORY_ARTIFACT_VERSION,
  StoppedDirectoryPublication,
  StoppedDirectoryPublicationError,
  stoppedDirectoryPublicationCandidateName,
} from "../src/stopped-directory-publication.mjs";
import { digestTree } from "../src/stopped-tree.mjs";

const SESSION_ID = "019f2100-0000-7000-8000-000000000001";
const THREAD_ID = "019f2100-0000-7000-8000-000000000002";
const IMAGE_DIGEST = `sha256:${"a".repeat(64)}`;
const CHECKPOINT_ID = "checkpoint-001";
const ARTIFACT_ID = "artifact-001";
const CAPTURE_OPERATION_ID = "operation-checkpoint-001";
const RESTORE_OPERATION_ID = "operation-restore-001";

const TRUSTED_JOURNAL_ACL_INSPECTORS = Object.freeze({
  inspectAncestorAcl: async () => false,
  inspectDirectoryAcl: async () => false,
});

function binding(operation, operationId, storageId, overrides = {}) {
  return {
    backendId: "single-attach-test",
    operation,
    operationId,
    sessionId: SESSION_ID,
    storageId,
    ...overrides,
  };
}

function request(operation, operationId, storageId, overrides = {}) {
  return {
    contractVersion: 1,
    backendId: "single-attach-test",
    storageId,
    sessionId: SESSION_ID,
    leaseId: operation === "checkpoint" ? "lease-001" : "lease-002",
    holderId: operation === "checkpoint" ? "host-001" : "host-002",
    fencingEpoch: operation === "checkpoint" ? "11" : "12",
    operation,
    operationId,
    target: {
      artifactId: ARTIFACT_ID,
      checkpointId: CHECKPOINT_ID,
      kind: "checkpoint",
    },
    ...overrides,
  };
}

function checkpoint(overrides = {}) {
  return {
    contractVersion: 1,
    checkpointId: CHECKPOINT_ID,
    artifactId: ARTIFACT_ID,
    backendId: "single-attach-test",
    storageId: "volume-001",
    sessionId: SESSION_ID,
    codexThreadId: THREAD_ID,
    codexSessionId: THREAD_ID,
    imageDigest: IMAGE_DIGEST,
    sourceFencingEpoch: "11",
    checkpointClass: "clean",
    createdAt: "2026-07-02T12:00:00.000Z",
    ...overrides,
  };
}

function mutationResult(mutationRequest, overrides = {}) {
  return {
    ...mutationRequest,
    proofId: `proof-${mutationRequest.operation}-001`,
    status:
      mutationRequest.operation === "checkpoint"
        ? "checkpoint-created"
        : "restored",
    ...overrides,
  };
}

function fixedResult(mutationRequest, fixedCheckpoint = checkpoint()) {
  return {
    checkpoint: fixedCheckpoint,
    mutation: mutationResult(mutationRequest),
  };
}

function destinationChangedError() {
  const error = new Error("rename destination changed");
  error.code = "destination_changed";
  Object.defineProperty(error, "renameOutcome", { value: "not-committed" });
  return error;
}

function simpleLockProvider({ onRelease, renamePath = rename } = {}) {
  return async () => ({
    async assertHeld() {},
    async release() {
      await onRelease?.();
    },
    async renameWhileHeld(source, destination, expectedDestination) {
      if (expectedDestination?.kind === "absent") {
        try {
          await lstat(destination);
        } catch (error) {
          if (error?.code !== "ENOENT") throw destinationChangedError();
          await renamePath(source, destination);
          return;
        }
        throw destinationChangedError();
      }
      await renamePath(source, destination);
    },
  });
}

async function pathExists(path) {
  try {
    await lstat(path);
    return true;
  } catch (error) {
    if (error?.code === "ENOENT") return false;
    throw error;
  }
}

async function assertPathAbsent(path) {
  await assert.rejects(access(path), (error) => error?.code === "ENOENT");
}

async function createFixture(t, publicationOptions = {}) {
  const root = await mkdtemp(
    join(tmpdir(), "stopped-directory-publication-test-"),
  );
  const sourceOwnedRoot = join(root, "source-root");
  const artifactOwnedRoot = join(root, "artifact-root");
  const destinationOwnedRoot = join(root, "destination-root");
  const journalDirectory = join(root, "journal");
  for (const directory of [
    sourceOwnedRoot,
    artifactOwnedRoot,
    destinationOwnedRoot,
    journalDirectory,
  ]) {
    await mkdir(directory, { mode: 0o700 });
  }
  t.after(() => rm(root, { force: true, recursive: true }));

  const sourceDirectory = join(sourceOwnedRoot, "session");
  await mkdir(join(sourceDirectory, "workspace", "nested"), {
    mode: 0o700,
    recursive: true,
  });
  await writeFile(join(sourceDirectory, "workspace", "README.md"), "portable\n", {
    mode: 0o640,
  });
  await writeFile(
    join(sourceDirectory, "workspace", "nested", "state.jsonl"),
    '{"type":"turn","state":"completed"}\n',
    { mode: 0o600 },
  );
  await symlink("README.md", join(sourceDirectory, "workspace", "current"));

  const journal = new FilesystemOperationJournal({
    directory: journalDirectory,
    acquireLock: simpleLockProvider(),
    ...TRUSTED_JOURNAL_ACL_INSPECTORS,
  });
  const publication = new StoppedDirectoryPublication({
    journal,
    acquireLock: simpleLockProvider(),
    inspectFilesystem: async () => ({
      durability: "local-fsync-rename",
      type: "test-local",
    }),
    ...publicationOptions,
  });
  const artifactDirectory = join(artifactOwnedRoot, ARTIFACT_ID);
  const destinationDirectory = join(destinationOwnedRoot, "restored-session");
  return {
    artifactDirectory,
    artifactOwnedRoot,
    destinationDirectory,
    destinationOwnedRoot,
    journal,
    journalDirectory,
    publication,
    root,
    sourceDirectory,
    sourceOwnedRoot,
  };
}

function captureOptions(fixture, overrides = {}) {
  const mutationRequest =
    overrides.request ??
    request("checkpoint", CAPTURE_OPERATION_ID, "volume-001");
  return {
    binding:
      overrides.binding ??
      binding("checkpoint", CAPTURE_OPERATION_ID, "volume-001"),
    operationId: CAPTURE_OPERATION_ID,
    request: mutationRequest,
    result: overrides.result ?? fixedResult(mutationRequest),
    sourceOwnedRoot: fixture.sourceOwnedRoot,
    sourceDirectory: fixture.sourceDirectory,
    artifactOwnedRoot: fixture.artifactOwnedRoot,
    artifactDirectory: fixture.artifactDirectory,
    ...overrides,
  };
}

function restoreOptions(fixture, overrides = {}) {
  const mutationRequest =
    overrides.request ??
    request("restore", RESTORE_OPERATION_ID, "volume-002");
  return {
    artifactProof: overrides.artifactProof ?? fixture.artifactProof,
    binding:
      overrides.binding ??
      binding("restore", RESTORE_OPERATION_ID, "volume-002"),
    operationId: RESTORE_OPERATION_ID,
    request: mutationRequest,
    result: overrides.result ?? fixedResult(mutationRequest),
    artifactOwnedRoot: fixture.artifactOwnedRoot,
    artifactDirectory: fixture.artifactDirectory,
    destinationOwnedRoot: fixture.destinationOwnedRoot,
    destinationDirectory: fixture.destinationDirectory,
    ...overrides,
  };
}

function candidatePath(ownedRoot, operationId, finalPath) {
  return join(
    ownedRoot,
    stoppedDirectoryPublicationCandidateName(operationId, basename(finalPath)),
  );
}

function publicationId(operationId, finalPath) {
  return `publication-sha256-${createHash("sha256")
    .update("portable-codex-stopped-directory-publication\0", "utf8")
    .update(operationId, "utf8")
    .update("\0", "utf8")
    .update(basename(finalPath), "utf8")
    .digest("hex")}`;
}

function assertPublicationError(error, code, commitState) {
  assert(error instanceof StoppedDirectoryPublicationError);
  if (code !== undefined) assert.equal(error.code, code);
  if (commitState !== undefined) assert.equal(error.commitState, commitState);
  assert.equal(error.retryable, false);
  assert.equal(Object.isFrozen(error), true);
  assert.equal(Object.hasOwn(error, "cause"), false);
  assert.equal(Object.hasOwn(error, "details"), false);
  return true;
}

async function readArtifactManifest(artifactDirectory) {
  return JSON.parse(await readFile(join(artifactDirectory, "artifact.json"), "utf8"));
}

async function publishFixtureArtifact(fixture) {
  const outcome = await fixture.publication.publishCheckpointArtifact(
    captureOptions(fixture),
  );
  fixture.artifactProof = Object.freeze({
    artifactManifestDigest: outcome.materialization.artifactManifestDigest,
    captureOperationId: CAPTURE_OPERATION_ID,
    modeledDigest: outcome.materialization.modeledDigest,
  });
  return outcome;
}

test("candidate names are deterministic, hashed, and path-safe", () => {
  const first = stoppedDirectoryPublicationCandidateName(
    CAPTURE_OPERATION_ID,
    ARTIFACT_ID,
  );
  const replay = stoppedDirectoryPublicationCandidateName(
    CAPTURE_OPERATION_ID,
    ARTIFACT_ID,
  );
  const otherOperation = stoppedDirectoryPublicationCandidateName(
    `${CAPTURE_OPERATION_ID}-other`,
    ARTIFACT_ID,
  );
  const otherFinal = stoppedDirectoryPublicationCandidateName(
    CAPTURE_OPERATION_ID,
    `${ARTIFACT_ID}-other`,
  );

  assert.equal(first, replay);
  assert.notEqual(first, otherOperation);
  assert.notEqual(first, otherFinal);
  assert.match(first, /^\.[A-Za-z0-9._-]+$/u);
  assert.equal(first.includes("/"), false);
  assert.equal(first.includes("\\"), false);
  assert.equal(first.includes(CAPTURE_OPERATION_ID), false);
});

test("the default filesystem inspector accepts the supported local test volume", async (t) => {
  const fixture = await createFixture(t, { inspectFilesystem: undefined });

  const outcome = await fixture.publication.publishCheckpointArtifact(
    captureOptions(fixture),
  );

  assert.equal(outcome.result.mutation.status, "checkpoint-created");
});

test("a remote or unapproved filesystem profile is rejected before prepare", async (t) => {
  let fixture;
  let journalPath;
  fixture = await createFixture(t, {
    inspectFilesystem: async (path) =>
      path === journalPath
        ? { durability: "local-fsync-rename", type: "test-local" }
        : { durability: "shared-remote", type: "nfs" },
  });
  journalPath = await realpath(fixture.journalDirectory);

  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(captureOptions(fixture)),
    (error) =>
      assertPublicationError(
        error,
        "unsupported_publication_filesystem",
        "not-committed",
      ),
  );
  assert.equal(
    (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record,
    null,
  );
  assert.equal(await pathExists(fixture.artifactDirectory), false);
});

test("an unsupported journal filesystem is publication-uncertain before prepare", async (t) => {
  let fixture;
  let journalPath;
  fixture = await createFixture(t, {
    inspectFilesystem: async (path) =>
      path === journalPath
        ? { durability: "shared-remote", type: "nfs" }
        : { durability: "local-fsync-rename", type: "test-local" },
  });
  journalPath = await realpath(fixture.journalDirectory);

  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(captureOptions(fixture)),
    (error) =>
      assertPublicationError(
        error,
        "unsupported_publication_filesystem",
        "uncertain",
      ),
  );
  assert.equal(
    (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record,
    null,
  );
  assert.equal(await pathExists(fixture.artifactDirectory), false);
});

test("a prepared operation cannot be rebound to a different journal profile", async (t) => {
  let failAfterJournalPrepared = true;
  let fixture;
  let journalPath;
  let journalType = "journal-local-a";
  fixture = await createFixture(t, {
    faults: {
      async afterJournalPrepared() {
        if (failAfterJournalPrepared) throw new Error("sensitive prepared fault");
      },
    },
    inspectFilesystem: async (path) => ({
      durability: "local-fsync-rename",
      type: path === journalPath ? journalType : "test-local",
    }),
  });
  journalPath = await realpath(fixture.journalDirectory);
  const options = captureOptions(fixture);
  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(options),
    (error) =>
      assertPublicationError(error, "publication_io_failed", "not-committed"),
  );

  failAfterJournalPrepared = false;
  journalType = "journal-local-b";
  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(options),
    (error) => assertPublicationError(error, "publication_conflict"),
  );
  assert.equal(
    (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record.state,
    "prepared",
  );
});

test("the journal directory cannot be captured inside the source tree", async (t) => {
  const fixture = await createFixture(t);
  const nestedJournalDirectory = join(
    fixture.sourceDirectory,
    ".portable-runtime",
    "journal",
  );
  await mkdir(nestedJournalDirectory, { mode: 0o700, recursive: true });
  const nestedJournal = new FilesystemOperationJournal({
    directory: nestedJournalDirectory,
    acquireLock: simpleLockProvider(),
    ...TRUSTED_JOURNAL_ACL_INSPECTORS,
  });
  const publication = new StoppedDirectoryPublication({
    journal: nestedJournal,
    acquireLock: simpleLockProvider(),
    inspectFilesystem: async () => ({
      durability: "local-fsync-rename",
      type: "test-local",
    }),
  });

  await assert.rejects(
    publication.publishCheckpointArtifact(captureOptions(fixture)),
    (error) =>
      assertPublicationError(
        error,
        "invalid_publication_request",
        "uncertain",
      ),
  );
  assert.equal(await pathExists(fixture.artifactDirectory), false);
});

test("the journal authority cannot also be the publication root", async (t) => {
  const fixture = await createFixture(t);
  const artifactDirectory = join(fixture.journalDirectory, "published-artifact");

  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(
      captureOptions(fixture, {
        artifactDirectory,
        artifactOwnedRoot: fixture.journalDirectory,
      }),
    ),
    (error) =>
      assertPublicationError(
        error,
        "invalid_publication_request",
        "uncertain",
      ),
  );
  assert.equal(await pathExists(artifactDirectory), false);
  assert.equal(
    (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record,
    null,
  );
});

test("a source identity alias of the journal authority is rejected", async (t) => {
  const fixture = await createFixture(t);
  const sourceIdentity = await lstat(fixture.sourceDirectory, { bigint: true });
  const journalPath = await realpath(fixture.journalDirectory);
  class SourceAliasedJournal extends FilesystemOperationJournal {
    async describeAuthority() {
      return Object.freeze({
        device: sourceIdentity.dev.toString(),
        inode: sourceIdentity.ino.toString(),
        path: journalPath,
      });
    }
  }
  const journal = new SourceAliasedJournal({
    directory: fixture.journalDirectory,
    acquireLock: simpleLockProvider(),
    ...TRUSTED_JOURNAL_ACL_INSPECTORS,
  });
  const publication = new StoppedDirectoryPublication({
    journal,
    acquireLock: simpleLockProvider(),
    inspectFilesystem: async () => ({
      durability: "local-fsync-rename",
      type: "test-local",
    }),
  });

  await assert.rejects(
    publication.publishCheckpointArtifact(captureOptions(fixture)),
    (error) =>
      assertPublicationError(
        error,
        "invalid_publication_request",
        "uncertain",
      ),
  );
  assert.equal(await pathExists(fixture.artifactDirectory), false);
  assert.equal(
    (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record,
    null,
  );
});

test("checkpoint copy rejects absolute symlinks into the journal authority", async (t) => {
  const fixture = await createFixture(t);
  const recordPath = join(
    fixture.journalDirectory,
    operationJournalRecordFilename(CAPTURE_OPERATION_ID),
  );
  await symlink(recordPath, join(fixture.sourceDirectory, "journal-record"));

  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(captureOptions(fixture)),
    (error) =>
      assertPublicationError(error, "publication_io_failed", "not-committed"),
  );

  const candidate = candidatePath(
    fixture.artifactOwnedRoot,
    CAPTURE_OPERATION_ID,
    fixture.artifactDirectory,
  );
  assert.equal(await pathExists(candidate), true);
  assert.equal(await pathExists(fixture.artifactDirectory), false);
  assert.equal(
    (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record.state,
    "prepared",
  );
});

test("journal authority discovery failure is publication-uncertain", async (t) => {
  const fixture = await createFixture(t);
  const missingJournalDirectory = join(fixture.root, "missing-journal");
  const missingJournal = new FilesystemOperationJournal({
    directory: missingJournalDirectory,
    acquireLock: simpleLockProvider(),
    ...TRUSTED_JOURNAL_ACL_INSPECTORS,
  });
  const publication = new StoppedDirectoryPublication({
    journal: missingJournal,
    acquireLock: simpleLockProvider(),
    inspectFilesystem: async () => ({
      durability: "local-fsync-rename",
      type: "test-local",
    }),
  });

  await assert.rejects(
    publication.publishCheckpointArtifact(captureOptions(fixture)),
    (error) =>
      assertPublicationError(
        error,
        "publication_outcome_uncertain",
        "uncertain",
      ),
  );
});

test("checkpoint publication creates one durable exact artifact bundle and commits", async (t) => {
  const fixture = await createFixture(t);
  const options = captureOptions(fixture);
  const result = await fixture.publication.publishCheckpointArtifact(options);

  assert.deepEqual(result.result, options.result);
  assert.equal(result.replayed, false);
  assert.equal(Object.isFrozen(result), true);
  const manifest = await readArtifactManifest(fixture.artifactDirectory);
  assert.equal(manifest.format, "portable-codex-stopped-directory");
  assert.equal(manifest.formatVersion, STOPPED_DIRECTORY_ARTIFACT_VERSION);
  assert.equal(manifest.digestAlgorithm, "sha256");
  assert.equal(manifest.payloadKind, "portable-stopped-tree");
  assert.match(manifest.modeledDigest, /^[a-f0-9]{64}$/u);
  assert.equal(
    manifest.modeledDigest,
    await digestTree(join(fixture.artifactDirectory, "payload")),
  );
  assert.equal(
    await digestTree(join(fixture.artifactDirectory, "payload")),
    await digestTree(fixture.sourceDirectory),
  );
  assert.equal(
    await readFile(
      join(fixture.artifactDirectory, "payload", "workspace", "README.md"),
      "utf8",
    ),
    "portable\n",
  );
  const serializedManifest = JSON.stringify(manifest);
  for (const forbidden of [
    fixture.root,
    fixture.sourceDirectory,
    fixture.artifactDirectory,
    "accessToken",
    "refreshToken",
    "auth.json",
    "gitSummary",
  ]) {
    assert.equal(serializedManifest.includes(forbidden), false);
  }
  const journalRecord = (
    await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })
  ).record;
  assert.equal(journalRecord.state, "committed");
  assert.deepEqual(journalRecord.result, options.result);
  assert.equal(journalRecord.materialization.modeledDigest, manifest.modeledDigest);
  assert.deepEqual(result.materialization, journalRecord.materialization);
  await assertPathAbsent(
    candidatePath(
      fixture.artifactOwnedRoot,
      CAPTURE_OPERATION_ID,
      fixture.artifactDirectory,
    ),
  );
});

test("checkpoint publication restores private metadata modes under umask 777", async (t) => {
  const fixture = await createFixture(t);
  // macOS applies the process umask to symlink permission bits but exposes no
  // portable no-follow chmod for symlinks. This mode test therefore uses the
  // regular-file/directory subset while symlink behavior remains covered by
  // the stopped-tree compatibility tests.
  await rm(join(fixture.sourceDirectory, "workspace", "current"));
  const previousUmask = process.umask(0o777);
  t.after(() => process.umask(previousUmask));

  await fixture.publication.publishCheckpointArtifact(captureOptions(fixture));

  const artifactMode = (await lstat(fixture.artifactDirectory)).mode & 0o777;
  const manifestMode =
    (await lstat(join(fixture.artifactDirectory, "artifact.json"))).mode & 0o777;
  assert.equal(artifactMode, 0o700);
  assert.equal(manifestMode, 0o600);
});

test("restore publication publishes a raw isolated payload and preserves its artifact", async (t) => {
  const fixture = await createFixture(t);
  await publishFixtureArtifact(fixture);
  const artifactManifestBefore = await readFile(
    join(fixture.artifactDirectory, "artifact.json"),
  );
  const artifactDigestBefore = await digestTree(fixture.artifactDirectory);
  const options = restoreOptions(fixture);

  const result = await fixture.publication.publishRestoreDestination(options);

  assert.deepEqual(result.result, options.result);
  assert.equal(result.replayed, false);
  assert.equal(
    await digestTree(fixture.destinationDirectory),
    await digestTree(fixture.sourceDirectory),
  );
  assert.equal(
    await readFile(join(fixture.destinationDirectory, "workspace", "README.md"), "utf8"),
    "portable\n",
  );
  await assertPathAbsent(join(fixture.destinationDirectory, "artifact.json"));
  assert.deepEqual(
    await readFile(join(fixture.artifactDirectory, "artifact.json")),
    artifactManifestBefore,
  );
  assert.equal(await digestTree(fixture.artifactDirectory), artifactDigestBefore);
  assert.equal(
    (await fixture.journal.read({ operationId: RESTORE_OPERATION_ID })).record.state,
    "committed",
  );
});

test("an exact committed replay returns the fixed result without recopying", async (t) => {
  let afterCopyCalls = 0;
  const fixture = await createFixture(t, {
    faults: {
      async afterCopy() {
        afterCopyCalls += 1;
      },
    },
  });
  const options = captureOptions(fixture);
  const first = await fixture.publication.publishCheckpointArtifact(options);
  const artifactIdentity = await lstat(fixture.artifactDirectory, { bigint: true });
  await rm(fixture.sourceDirectory, { force: true, recursive: true });

  const replay = await fixture.publication.publishCheckpointArtifact(options);

  assert.deepEqual(replay.result, first.result);
  assert.deepEqual(replay.materialization, first.materialization);
  assert.equal(replay.replayed, true);
  assert.equal(afterCopyCalls, 1);
  const replayedIdentity = await lstat(fixture.artifactDirectory, { bigint: true });
  assert.equal(replayedIdentity.dev, artifactIdentity.dev);
  assert.equal(replayedIdentity.ino, artifactIdentity.ino);
});

test("committed checkpoint replay rejects extra bundle-root entries", async (t) => {
  const fixture = await createFixture(t);
  const options = captureOptions(fixture);
  await fixture.publication.publishCheckpointArtifact(options);
  await writeFile(join(fixture.artifactDirectory, "extra"), "unexpected\n", {
    mode: 0o600,
  });

  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(options),
    (error) =>
      assertPublicationError(error, "published_state_invalid", "committed"),
  );
  assert.equal(
    (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record.state,
    "committed",
  );
});

test("a missing committed artifact remains classified as committed corruption", async (t) => {
  const fixture = await createFixture(t);
  const options = captureOptions(fixture);
  await fixture.publication.publishCheckpointArtifact(options);
  await rm(fixture.artifactDirectory, { force: true, recursive: true });

  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(options),
    (error) =>
      assertPublicationError(error, "published_state_invalid", "committed"),
  );
  assert.equal(
    (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record.state,
    "committed",
  );
});

test("successful publication and committed replay release their publication locks", async (t) => {
  let releases = 0;
  const fixture = await createFixture(t, {
    acquireLock: simpleLockProvider({
      async onRelease() {
        releases += 1;
      },
    }),
  });
  const options = captureOptions(fixture);

  await fixture.publication.publishCheckpointArtifact(options);
  assert.equal(releases, 1);

  await fixture.publication.publishCheckpointArtifact(options);
  assert.equal(releases, 2);
});

test("exact concurrent retries serialize by publication-root identity", async (t) => {
  let activeLocks = 0;
  let maximumActiveLocks = 0;
  let releaseFirst;
  let observeFirst;
  const firstPrepared = new Promise((resolve) => {
    observeFirst = resolve;
  });
  const continueFirst = new Promise((resolve) => {
    releaseFirst = resolve;
  });
  const baseLockProvider = simpleLockProvider();
  const fixture = await createFixture(t, {
    acquireLock: async (...args) => {
      const lock = await baseLockProvider(...args);
      activeLocks += 1;
      maximumActiveLocks = Math.max(maximumActiveLocks, activeLocks);
      return {
        ...lock,
        async release() {
          try {
            await lock.release();
          } finally {
            activeLocks -= 1;
          }
        },
      };
    },
    faults: {
      async afterJournalPrepared() {
        observeFirst();
        await continueFirst;
      },
    },
  });
  const mutableBinding = binding(
    "checkpoint",
    CAPTURE_OPERATION_ID,
    "volume-001",
    { metadata: { lane: "original" } },
  );
  const options = captureOptions(fixture, { binding: mutableBinding });
  const first = fixture.publication.publishCheckpointArtifact(options);
  await firstPrepared;
  const second = fixture.publication.publishCheckpointArtifact(options);
  mutableBinding.metadata.lane = "mutated";
  options.request.target.checkpointId = "mutated-checkpoint";
  options.result.checkpoint.checkpointId = "mutated-checkpoint";
  options.result.mutation.operationId = "mutated-operation";
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(activeLocks, 1);
  releaseFirst();

  const outcomes = await Promise.all([first, second]);
  assert.equal(maximumActiveLocks, 1);
  assert.deepEqual(
    outcomes.map((outcome) => outcome.replayed).sort(),
    [false, true],
  );
  const record = (
    await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })
  ).record;
  assert.equal(record.binding.coordinator.metadata.lane, "original");
  assert.equal(record.request.target.checkpointId, CHECKPOINT_ID);
  assert.equal(record.result.checkpoint.checkpointId, CHECKPOINT_ID);
  assert.equal(record.result.mutation.operationId, CAPTURE_OPERATION_ID);
});

test("an existing final artifact is preserved without replacement", async (t) => {
  const fixture = await createFixture(t);
  const sentinel = join(fixture.artifactDirectory, "sentinel");
  await mkdir(fixture.artifactDirectory, { mode: 0o700 });
  await writeFile(sentinel, "preserve\n", { mode: 0o600 });

  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(captureOptions(fixture)),
    (error) =>
      assertPublicationError(
        error,
        "publication_recovery_required",
        "not-committed",
      ),
  );

  assert.equal(await readFile(sentinel, "utf8"), "preserve\n");
  assert.equal((await lstat(fixture.artifactDirectory)).isDirectory(), true);
});

test("source and publication roots must be distinct before journaling", async (t) => {
  const fixture = await createFixture(t);
  const artifactDirectory = join(fixture.sourceOwnedRoot, "artifact");

  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(
      captureOptions(fixture, {
        artifactDirectory,
        artifactOwnedRoot: fixture.sourceOwnedRoot,
      }),
    ),
    (error) =>
      assertPublicationError(
        error,
        "invalid_publication_request",
        "not-committed",
      ),
  );
  assert.equal(await pathExists(artifactDirectory), false);
  assert.equal(
    (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record,
    null,
  );
});

test("a second operation ID cannot adopt an existing published artifact", async (t) => {
  const fixture = await createFixture(t);
  const first = await fixture.publication.publishCheckpointArtifact(
    captureOptions(fixture),
  );
  const firstIdentity = await lstat(fixture.artifactDirectory, { bigint: true });
  const secondOperationId = "operation-checkpoint-002";
  const secondRequest = request(
    "checkpoint",
    secondOperationId,
    "volume-001",
  );

  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(
      captureOptions(fixture, {
        binding: binding("checkpoint", secondOperationId, "volume-001"),
        operationId: secondOperationId,
        request: secondRequest,
        result: fixedResult(secondRequest),
      }),
    ),
    (error) =>
      assertPublicationError(
        error,
        "publication_recovery_required",
        "not-committed",
      ),
  );

  assert.deepEqual(
    (await fixture.publication.publishCheckpointArtifact(captureOptions(fixture)))
      .result,
    first.result,
  );
  const finalIdentity = await lstat(fixture.artifactDirectory, { bigint: true });
  assert.equal(finalIdentity.dev, firstIdentity.dev);
  assert.equal(finalIdentity.ino, firstIdentity.ino);
  assert.equal(
    (await fixture.journal.read({ operationId: secondOperationId })).record,
    null,
  );
});

test("the production lock preserves a destination created before rename", async (t) => {
  let fixture;
  const sentinelName = "foreign-sentinel";
  fixture = await createFixture(t, {
    acquireLock: undefined,
    faults: {
      async beforeRename() {
        await mkdir(fixture.artifactDirectory, { mode: 0o700 });
        await writeFile(
          join(fixture.artifactDirectory, sentinelName),
          "preserve\n",
          { mode: 0o600 },
        );
      },
    },
  });

  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(captureOptions(fixture)),
    (error) =>
      assertPublicationError(error, "publication_io_failed", "not-committed"),
  );

  assert.equal(
    await readFile(join(fixture.artifactDirectory, sentinelName), "utf8"),
    "preserve\n",
  );
  assert.equal(
    await pathExists(
      candidatePath(
        fixture.artifactOwnedRoot,
        CAPTURE_OPERATION_ID,
        fixture.artifactDirectory,
      ),
    ),
    true,
  );
  assert.equal(
    (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record.state,
    "materialized",
  );
});

test("a candidate replacement before rename is definitely not committed", async (t) => {
  let fixture;
  let displaced;
  fixture = await createFixture(t, {
    faults: {
      async beforeRename() {
        const candidate = candidatePath(
          fixture.artifactOwnedRoot,
          CAPTURE_OPERATION_ID,
          fixture.artifactDirectory,
        );
        displaced = join(fixture.artifactOwnedRoot, "displaced-candidate");
        await rename(candidate, displaced);
        await mkdir(candidate, { mode: 0o700 });
      },
    },
  });

  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(captureOptions(fixture)),
    (error) =>
      assertPublicationError(
        error,
        "publication_recovery_required",
        "not-committed",
      ),
  );
  assert.equal(await pathExists(fixture.artifactDirectory), false);
  assert.equal(await pathExists(displaced), true);
  assert.equal(
    (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record.state,
    "materialized",
  );
});

test("a materialized stage-only operation resumes without recopying", async (t) => {
  let failAfterMaterialized = true;
  let afterCopyCalls = 0;
  const fixture = await createFixture(t, {
    faults: {
      async afterCopy() {
        afterCopyCalls += 1;
      },
      async afterMaterialized() {
        if (failAfterMaterialized) throw new Error("sensitive stage-only fault");
      },
    },
  });
  const options = captureOptions(fixture);
  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(options),
    (error) =>
      assertPublicationError(error, "publication_io_failed", "not-committed"),
  );
  const candidate = candidatePath(
    fixture.artifactOwnedRoot,
    CAPTURE_OPERATION_ID,
    fixture.artifactDirectory,
  );
  assert.equal(await pathExists(candidate), true);
  assert.equal(await pathExists(fixture.artifactDirectory), false);
  assert.equal(
    (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record.state,
    "materialized",
  );

  failAfterMaterialized = false;
  const result = await fixture.publication.publishCheckpointArtifact(options);

  assert.deepEqual(result.result, options.result);
  assert.equal(result.replayed, false);
  assert.equal(afterCopyCalls, 1);
  assert.equal(await pathExists(candidate), false);
  assert.equal(await pathExists(fixture.artifactDirectory), true);
  assert.equal(
    (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record.state,
    "committed",
  );
});

test("a materialized final-only operation resumes after post-rename uncertainty", async (t) => {
  let failAfterParentSync = true;
  let afterCopyCalls = 0;
  const fixture = await createFixture(t, {
    faults: {
      async afterCopy() {
        afterCopyCalls += 1;
      },
      async afterParentSync() {
        if (failAfterParentSync) throw new Error("sensitive final-only fault");
      },
    },
  });
  const options = captureOptions(fixture);
  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(options),
    (error) =>
      assertPublicationError(error, "publication_outcome_uncertain", "uncertain"),
  );
  const candidate = candidatePath(
    fixture.artifactOwnedRoot,
    CAPTURE_OPERATION_ID,
    fixture.artifactDirectory,
  );
  assert.equal(await pathExists(candidate), false);
  assert.equal(await pathExists(fixture.artifactDirectory), true);
  assert.equal(
    (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record.state,
    "materialized",
  );

  failAfterParentSync = false;
  const result = await fixture.publication.publishCheckpointArtifact(options);

  assert.deepEqual(result.result, options.result);
  assert.equal(afterCopyCalls, 1);
  assert.equal(
    (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record.state,
    "committed",
  );
});

test("a prepared leftover candidate fails closed and is retained", async (t) => {
  let failAfterCandidateCreated = true;
  const fixture = await createFixture(t, {
    faults: {
      async afterCandidateCreated() {
        if (failAfterCandidateCreated) {
          throw new Error("sensitive partial-candidate fault");
        }
      },
    },
  });
  const options = captureOptions(fixture);
  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(options),
    (error) =>
      assertPublicationError(error, "publication_io_failed", "not-committed"),
  );
  const candidate = candidatePath(
    fixture.artifactOwnedRoot,
    CAPTURE_OPERATION_ID,
    fixture.artifactDirectory,
  );
  const retainedIdentity = await lstat(candidate, { bigint: true });
  assert.equal(
    (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record.state,
    "prepared",
  );

  failAfterCandidateCreated = false;
  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(options),
    (error) =>
      assertPublicationError(
        error,
        "publication_recovery_required",
        "not-committed",
      ),
  );

  const rereadIdentity = await lstat(candidate, { bigint: true });
  assert.equal(rereadIdentity.dev, retainedIdentity.dev);
  assert.equal(rereadIdentity.ino, retainedIdentity.ino);
  assert.equal(await pathExists(fixture.artifactDirectory), false);
});

test("rename acknowledgement loss keeps a materialized operation recoverable", async (t) => {
  let failRenameAcknowledgement = true;
  const renamePath = async (source, destination) => {
    await rename(source, destination);
    if (failRenameAcknowledgement) {
      const error = new Error("sensitive rename acknowledgement fault");
      error.code = "lock_commit_uncertain";
      throw error;
    }
  };
  const fixture = await createFixture(t, {
    acquireLock: simpleLockProvider({ renamePath }),
  });
  const options = captureOptions(fixture);

  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(options),
    (error) =>
      assertPublicationError(error, "publication_outcome_uncertain", "uncertain"),
  );
  assert.equal(await pathExists(fixture.artifactDirectory), true);
  assert.equal(
    (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record.state,
    "materialized",
  );

  failRenameAcknowledgement = false;
  assert.deepEqual(
    (await fixture.publication.publishCheckpointArtifact(options)).result,
    options.result,
  );
  assert.equal(
    (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record.state,
    "committed",
  );
});

test("a post-rename fault retains the final artifact for exact restart recovery", async (t) => {
  let failAfterRename = true;
  const fixture = await createFixture(t, {
    faults: {
      async afterRename() {
        if (failAfterRename) throw new Error("sensitive post-rename fault");
      },
    },
  });
  const options = captureOptions(fixture);

  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(options),
    (error) =>
      assertPublicationError(error, "publication_outcome_uncertain", "uncertain"),
  );
  assert.equal(await pathExists(fixture.artifactDirectory), true);
  assert.equal(
    (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record.state,
    "materialized",
  );

  failAfterRename = false;
  assert.deepEqual(
    (await fixture.publication.publishCheckpointArtifact(options)).result,
    options.result,
  );
});

test("final mutation after readback cannot advance the journal to committed", async (t) => {
  let fixture;
  fixture = await createFixture(t, {
    faults: {
      async afterFinalReadback() {
        await writeFile(
          join(fixture.artifactDirectory, "payload", "workspace", "README.md"),
          "mutated after readback\n",
          { mode: 0o640 },
        );
      },
    },
  });

  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(captureOptions(fixture)),
    (error) =>
      assertPublicationError(
        error,
        "publication_outcome_uncertain",
        "uncertain",
      ),
  );
  assert.equal(
    (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record.state,
    "materialized",
  );
});

test("a journal read failure before state discovery is publication-uncertain", async (t) => {
  const fixture = await createFixture(t);
  const options = captureOptions(fixture);
  await fixture.publication.publishCheckpointArtifact(options);
  const failingJournal = new FilesystemOperationJournal({
    directory: fixture.journalDirectory,
    acquireLock: simpleLockProvider(),
    faults: {
      async afterRecordRead() {
        throw new Error("sensitive journal read failure");
      },
    },
    ...TRUSTED_JOURNAL_ACL_INSPECTORS,
  });
  const publication = new StoppedDirectoryPublication({
    journal: failingJournal,
    acquireLock: simpleLockProvider(),
    inspectFilesystem: async () => ({
      durability: "local-fsync-rename",
      type: "test-local",
    }),
  });

  await assert.rejects(
    publication.publishCheckpointArtifact(options),
    (error) =>
      assertPublicationError(
        error,
        "publication_outcome_uncertain",
        "uncertain",
      ),
  );
});

test("restore rejects a tampered artifact without creating a destination", async (t) => {
  const fixture = await createFixture(t);
  await publishFixtureArtifact(fixture);
  const tamperedPath = join(
    fixture.artifactDirectory,
    "payload",
    "workspace",
    "README.md",
  );
  await writeFile(tamperedPath, "tampered\n", { mode: 0o640 });

  await assert.rejects(
    fixture.publication.publishRestoreDestination(restoreOptions(fixture)),
    (error) =>
      assertPublicationError(
        error,
        "publication_integrity_failed",
        "not-committed",
      ),
  );

  assert.equal(await readFile(tamperedPath, "utf8"), "tampered\n");
  assert.equal(await pathExists(fixture.destinationDirectory), false);
});

test("restore rejects payload and manifest tampering against the trusted capture proof", async (t) => {
  const fixture = await createFixture(t);
  await publishFixtureArtifact(fixture);
  const payload = join(fixture.artifactDirectory, "payload");
  await writeFile(
    join(payload, "workspace", "README.md"),
    "tampered with matching manifest\n",
    { mode: 0o640 },
  );
  const manifestPath = join(fixture.artifactDirectory, "artifact.json");
  const manifest = JSON.parse(await readFile(manifestPath, "utf8"));
  manifest.modeledDigest = await digestTree(payload);
  await writeFile(manifestPath, `${JSON.stringify(manifest)}\n`, { mode: 0o600 });

  await assert.rejects(
    fixture.publication.publishRestoreDestination(restoreOptions(fixture)),
    (error) =>
      assertPublicationError(
        error,
        "publication_integrity_failed",
        "not-committed",
      ),
  );
  assert.equal(await pathExists(fixture.destinationDirectory), false);
});

test("restore replay rejects a materialized tree outside the trusted artifact proof", async (t) => {
  let failAfterJournalPrepared = false;
  const fixture = await createFixture(t, {
    faults: {
      async afterJournalPrepared() {
        if (failAfterJournalPrepared) {
          throw new Error("sensitive restore prepared fault");
        }
      },
    },
  });
  await publishFixtureArtifact(fixture);
  failAfterJournalPrepared = true;
  const options = restoreOptions(fixture);
  await assert.rejects(
    fixture.publication.publishRestoreDestination(options),
    (error) =>
      assertPublicationError(error, "publication_io_failed", "not-committed"),
  );

  const prepared = (
    await fixture.journal.read({ operationId: RESTORE_OPERATION_ID })
  ).record;
  assert.equal(prepared.state, "prepared");
  const candidate = candidatePath(
    fixture.destinationOwnedRoot,
    RESTORE_OPERATION_ID,
    fixture.destinationDirectory,
  );
  await mkdir(candidate, { mode: 0o700 });
  await writeFile(join(candidate, "untrusted"), "attacker-controlled\n", {
    mode: 0o600,
  });
  const identity = await lstat(candidate, { bigint: true });
  const modeledDigest = await digestTree(candidate);
  const mismatchedManifestDigest =
    fixture.artifactProof.artifactManifestDigest === "f".repeat(64)
      ? "e".repeat(64)
      : "f".repeat(64);
  await fixture.journal.markMaterialized({
    binding: prepared.binding,
    materialization: {
      contractVersion: 1,
      artifactManifestDigest: mismatchedManifestDigest,
      modeledDigest,
      publicationId: publicationId(
        RESTORE_OPERATION_ID,
        fixture.destinationDirectory,
      ),
      publicationKind: "restore-destination",
      stagedRoot: {
        device: identity.dev.toString(),
        inode: identity.ino.toString(),
      },
    },
    operationId: prepared.operationId,
    request: prepared.request,
    result: prepared.result,
  });
  await rm(fixture.artifactDirectory, { force: true, recursive: true });
  failAfterJournalPrepared = false;

  await assert.rejects(
    fixture.publication.publishRestoreDestination(options),
    (error) =>
      assertPublicationError(
        error,
        "publication_integrity_failed",
        "not-committed",
      ),
  );
  assert.equal(await pathExists(candidate), true);
  assert.equal(await pathExists(fixture.destinationDirectory), false);
});

test("restore rejects a reordered manifest even when its supplied digest matches", async (t) => {
  const fixture = await createFixture(t);
  await publishFixtureArtifact(fixture);
  const manifestPath = join(fixture.artifactDirectory, "artifact.json");
  const manifest = JSON.parse(await readFile(manifestPath, "utf8"));
  const reordered = {
    checkpoint: manifest.checkpoint,
    modeledDigest: manifest.modeledDigest,
    captureOperationId: manifest.captureOperationId,
    payloadKind: manifest.payloadKind,
    digestAlgorithm: manifest.digestAlgorithm,
    formatVersion: manifest.formatVersion,
    format: manifest.format,
  };
  const reorderedBytes = Buffer.from(`${JSON.stringify(reordered)}\n`, "utf8");
  await writeFile(manifestPath, reorderedBytes, { mode: 0o600 });
  const matchingProof = {
    ...fixture.artifactProof,
    artifactManifestDigest: createHash("sha256")
      .update(reorderedBytes)
      .digest("hex"),
  };

  await assert.rejects(
    fixture.publication.publishRestoreDestination(
      restoreOptions(fixture, { artifactProof: matchingProof }),
    ),
    (error) =>
      assertPublicationError(
        error,
        "publication_integrity_failed",
        "not-committed",
      ),
  );
});

test("one operation cannot be rebound to a different filesystem path", async (t) => {
  let failAfterJournalPrepared = true;
  const fixture = await createFixture(t, {
    faults: {
      async afterJournalPrepared() {
        if (failAfterJournalPrepared) throw new Error("sensitive prepared fault");
      },
    },
  });
  const first = captureOptions(fixture);
  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(first),
    (error) =>
      assertPublicationError(error, "publication_io_failed", "not-committed"),
  );
  failAfterJournalPrepared = false;
  const differentArtifactDirectory = join(
    fixture.artifactOwnedRoot,
    "different-artifact",
  );

  await assert.rejects(
    fixture.publication.publishCheckpointArtifact({
      ...first,
      artifactDirectory: differentArtifactDirectory,
    }),
    (error) => assertPublicationError(error, "publication_conflict"),
  );

  assert.equal(await pathExists(differentArtifactDirectory), false);
  assert.equal(
    (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record.state,
    "prepared",
  );
});

test("coordinator bindings preserve an own __proto__ key for conflict checks", async (t) => {
  let failAfterJournalPrepared = true;
  const fixture = await createFixture(t, {
    faults: {
      async afterJournalPrepared() {
        if (failAfterJournalPrepared) throw new Error("sensitive prepared fault");
      },
    },
  });
  const firstBinding = binding(
    "checkpoint",
    CAPTURE_OPERATION_ID,
    "volume-001",
  );
  Object.defineProperty(firstBinding, "__proto__", {
    enumerable: true,
    value: "lane-a",
  });
  const first = captureOptions(fixture, { binding: firstBinding });
  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(first),
    (error) =>
      assertPublicationError(error, "publication_io_failed", "not-committed"),
  );
  const record = (
    await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })
  ).record;
  assert.equal(record.binding.coordinator.__proto__, "lane-a");

  failAfterJournalPrepared = false;
  const conflictingBinding = binding(
    "checkpoint",
    CAPTURE_OPERATION_ID,
    "volume-001",
  );
  Object.defineProperty(conflictingBinding, "__proto__", {
    enumerable: true,
    value: "lane-b",
  });
  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(
      captureOptions(fixture, { binding: conflictingBinding }),
    ),
    (error) => assertPublicationError(error, "publication_conflict"),
  );
});

test("checkpoint capture rejects source replacement after its barrier", async (t) => {
  let fixture;
  const displaced = "displaced-source";
  fixture = await createFixture(t, {
    faults: {
      async afterSourceBarrier() {
        await rename(fixture.sourceDirectory, join(fixture.sourceOwnedRoot, displaced));
        await mkdir(fixture.sourceDirectory, { mode: 0o700 });
        await writeFile(join(fixture.sourceDirectory, "replacement"), "foreign\n", {
          mode: 0o600,
        });
      },
    },
  });

  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(captureOptions(fixture)),
    (error) =>
      assertPublicationError(
        error,
        "publication_integrity_failed",
        "not-committed",
      ),
  );
  assert.equal(await pathExists(fixture.artifactDirectory), false);
  assert.equal(
    (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record.state,
    "prepared",
  );
});

test("operational failures become frozen sanitized publication errors", async (t) => {
  const secret = "sk-sensitive-publication-sentinel";
  let fixture;
  fixture = await createFixture(t, {
    faults: {
      async afterSourceBarrier() {
        throw new Error(`${secret} ${fixture.sourceDirectory}`);
      },
    },
  });

  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(captureOptions(fixture)),
    (error) => {
      assertPublicationError(error, "publication_io_failed", "not-committed");
      assert.equal(error.message.includes(secret), false);
      assert.equal(error.message.includes(fixture.sourceDirectory), false);
      assert.equal(JSON.stringify(error).includes(secret), false);
      assert.equal(JSON.stringify(error).includes(fixture.sourceDirectory), false);
      return true;
    },
  );
});

test("an injected collaborator cannot forge a committed publication error", async (t) => {
  const fixture = await createFixture(t, {
    faults: {
      async afterJournalPrepared() {
        throw new StoppedDirectoryPublicationError(
          "published_state_invalid",
          "committed",
        );
      },
    },
  });

  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(captureOptions(fixture)),
    (error) =>
      assertPublicationError(error, "publication_io_failed", "not-committed"),
  );
});

test("an internal error from an earlier operation cannot be replayed by a fault", async (t) => {
  const committedFixture = await createFixture(t);
  const committedOptions = captureOptions(committedFixture);
  await committedFixture.publication.publishCheckpointArtifact(committedOptions);
  await rm(committedFixture.artifactDirectory, { force: true, recursive: true });
  let captured;
  try {
    await committedFixture.publication.publishCheckpointArtifact(committedOptions);
  } catch (error) {
    captured = error;
  }
  assertPublicationError(captured, "published_state_invalid", "committed");

  const fixture = await createFixture(t, {
    faults: {
      async afterJournalPrepared() {
        throw captured;
      },
    },
  });
  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(captureOptions(fixture)),
    (error) =>
      assertPublicationError(error, "publication_io_failed", "not-committed"),
  );
});

test("top-level option proxies are inspected once and failures are sanitized", async (t) => {
  const fixture = await createFixture(t);
  const options = captureOptions(fixture);
  let ownKeysCalls = 0;
  const stateful = new Proxy(options, {
    getOwnPropertyDescriptor(target, key) {
      return Reflect.getOwnPropertyDescriptor(target, key);
    },
    ownKeys(target) {
      ownKeysCalls += 1;
      if (ownKeysCalls > 1) throw new Error("sensitive second ownKeys failure");
      return Reflect.ownKeys(target);
    },
  });

  const outcome = await fixture.publication.publishCheckpointArtifact(stateful);
  assert.equal(outcome.result.mutation.status, "checkpoint-created");
  assert.equal(ownKeysCalls, 1);

  const secret = "sensitive first ownKeys failure";
  const hostile = new Proxy({}, {
    ownKeys() {
      throw new Error(secret);
    },
  });
  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(hostile),
    (error) => {
      assertPublicationError(error, "invalid_publication_request", "not-committed");
      assert.equal(error.message.includes(secret), false);
      return true;
    },
  );
});
