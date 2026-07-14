import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import {
  access,
  chmod,
  link,
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
  STOPPED_DIRECTORY_PUBLICATION_LOCK_NAME,
  StoppedDirectoryPublication,
  StoppedDirectoryPublicationError,
  stoppedDirectoryPublicationCandidateName,
} from "../src/stopped-directory-publication.mjs";
import {
  digestStoppedTreeIdentities,
  digestTree,
  inspectStoppedTreeObjectIdentity,
} from "../src/stopped-tree.mjs";

const SESSION_ID = "019f2100-0000-7000-8000-000000000001";
const THREAD_ID = "019f2100-0000-7000-8000-000000000002";
const IMAGE_DIGEST = `sha256:${"a".repeat(64)}`;
const CHECKPOINT_ID = "checkpoint-001";
const ARTIFACT_ID = "artifact-001";
const CAPTURE_OPERATION_ID = "operation-checkpoint-001";
const RESTORE_OPERATION_ID = "operation-restore-001";
const TEST_OBJECT_IDENTITY_SCHEME = "test-object-generation-v1";

const TRUSTED_JOURNAL_ACL_INSPECTORS = Object.freeze({
  inspectAncestorAcl: async () => false,
  inspectDirectoryAcl: async () => false,
});

const TRUSTED_PUBLICATION_ACL_INSPECTORS = Object.freeze({
  inspectOwnedRootAcl: async () => false,
  inspectOwnedRootAncestorAcl: async () => false,
});
const TRUSTED_PUBLICATION_MOUNT_INSPECTOR = Object.freeze({
  listMountPoints: async () => ["/"],
});

async function inspectTestPersistentObjectIdentity(path) {
  const metadata = await lstat(path, { bigint: true });
  return {
    device: metadata.dev.toString(),
    inode: metadata.ino.toString(),
    objectId: `test-object-${metadata.dev}-${metadata.ino}-${metadata.birthtimeNs}`,
  };
}

async function inspectTestPersistentObjectIdentityAs(path, objectId) {
  return {
    ...(await inspectTestPersistentObjectIdentity(path)),
    objectId,
  };
}

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

function armableJournalClass(guard) {
  const rejectIfArmed = (name) => {
    if (!guard.armed) return;
    guard.calls.push(name);
    throw new Error(`unexpected journal transition: ${name}`);
  };
  return class ArmableOperationJournal extends FilesystemOperationJournal {
    async prepare(options) {
      rejectIfArmed("prepare");
      return super.prepare(options);
    }

    async prepareFresh(options) {
      rejectIfArmed("prepareFresh");
      return super.prepareFresh(options);
    }

    async markMaterialized(options) {
      rejectIfArmed("markMaterialized");
      return super.markMaterialized(options);
    }

    async commit(options) {
      rejectIfArmed("commit");
      return super.commit(options);
    }
  };
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
  const {
    JournalClass = FilesystemOperationJournal,
    journalFaults,
    ...stoppedPublicationOptions
  } = publicationOptions;
  if (typeof stoppedPublicationOptions.inspectFilesystem === "function") {
    const inspectFilesystem = stoppedPublicationOptions.inspectFilesystem;
    stoppedPublicationOptions.inspectFilesystem = async (path) => ({
      filesystemId: "test-filesystem-001",
      objectIdentityScheme: TEST_OBJECT_IDENTITY_SCHEME,
      ...(await inspectFilesystem(path)),
    });
  }
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

  const journal = new JournalClass({
    directory: journalDirectory,
    acquireLock: simpleLockProvider(),
    ...(journalFaults === undefined ? {} : { faults: journalFaults }),
    ...TRUSTED_JOURNAL_ACL_INSPECTORS,
  });
  const publication = new StoppedDirectoryPublication({
    journal,
    acquireLock: simpleLockProvider(),
    inspectFilesystem: async () => ({
      durability: "local-fsync-rename",
      filesystemId: "test-filesystem-001",
      objectIdentityScheme: TEST_OBJECT_IDENTITY_SCHEME,
      type: "test-local",
    }),
    inspectPersistentObjectIdentity: inspectTestPersistentObjectIdentity,
    ...TRUSTED_PUBLICATION_ACL_INSPECTORS,
    ...TRUSTED_PUBLICATION_MOUNT_INSPECTOR,
    ...stoppedPublicationOptions,
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

function committedVerificationOptions(fixture, published, overrides = {}) {
  return {
    artifactDirectory: fixture.artifactDirectory,
    artifactOwnedRoot: fixture.artifactOwnedRoot,
    binding: published.binding,
    operationId: published.operationId,
    request: published.request,
    result: published.result,
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
  assert.doesNotThrow(() =>
    stoppedDirectoryPublicationCandidateName(
      CAPTURE_OPERATION_ID,
      ".publication_data",
    ),
  );
  for (const reserved of [
    ".stopped-directory-publication.lock",
    ".publication-reserved",
  ]) {
    assert.throws(
      () =>
        stoppedDirectoryPublicationCandidateName(
          CAPTURE_OPERATION_ID,
          reserved,
        ),
      (error) =>
        assertPublicationError(
          error,
          "invalid_publication_request",
          "not-committed",
        ),
    );
  }
});

test("persistent tree identity binds object generations to relative paths", async (t) => {
  const fixture = await createFixture(t);
  const left = join(fixture.sourceDirectory, "workspace", "left.txt");
  const right = join(fixture.sourceDirectory, "workspace", "right.txt");
  const temporary = join(fixture.sourceDirectory, "workspace", "temporary.txt");
  await writeFile(left, "identical\n", { mode: 0o600 });
  await writeFile(right, "identical\n", { mode: 0o600 });
  const modeledBefore = await digestTree(fixture.sourceDirectory);
  const before = await digestStoppedTreeIdentities(
    fixture.sourceDirectory,
    "test-filesystem-001",
    TEST_OBJECT_IDENTITY_SCHEME,
    inspectTestPersistentObjectIdentity,
  );

  await rename(left, temporary);
  await rename(right, left);
  await rename(temporary, right);
  const after = await digestStoppedTreeIdentities(
    fixture.sourceDirectory,
    "test-filesystem-001",
    TEST_OBJECT_IDENTITY_SCHEME,
    inspectTestPersistentObjectIdentity,
  );

  assert.notEqual(after, before);
  assert.equal(await digestTree(fixture.sourceDirectory), modeledBefore);
});

test("persistent tree identity rejects adapter object-ID collisions", async (t) => {
  const fixture = await createFixture(t);
  await assert.rejects(
    digestStoppedTreeIdentities(
      fixture.sourceDirectory,
      "test-filesystem-001",
      TEST_OBJECT_IDENTITY_SCHEME,
      (path) =>
        inspectTestPersistentObjectIdentityAs(path, "colliding-object-id"),
    ),
    /aliases distinct runtime objects/u,
  );
});

test("persistent identity rejects an adapter-side path ABA", async (t) => {
  const fixture = await createFixture(t);
  const victim = join(fixture.sourceDirectory, "workspace", "README.md");
  const oldObject = join(fixture.sourceDirectory, "workspace", "old-object");
  const displaced = join(fixture.sourceDirectory, "workspace", "displaced");
  await writeFile(oldObject, "old\n", { mode: 0o600 });
  const expectedIdentity = await lstat(victim, { bigint: true });

  await assert.rejects(
    inspectStoppedTreeObjectIdentity(
      victim,
      async (path) => {
        await rename(path, displaced);
        await rename(oldObject, path);
        const inspected = await inspectTestPersistentObjectIdentity(path);
        await rename(path, oldObject);
        await rename(displaced, path);
        return inspected;
      },
      expectedIdentity,
    ),
    /does not match runtime object/u,
  );
  assert.equal(await readFile(victim, "utf8"), "portable\n");
});

test("persistent identity rejects accessor-based adapter results", async (t) => {
  const fixture = await createFixture(t);
  const path = join(fixture.sourceDirectory, "workspace", "README.md");
  const expectedIdentity = await lstat(path, { bigint: true });
  let getterCalls = 0;
  const result = {};
  for (const [key, value] of Object.entries({
    device: expectedIdentity.dev.toString(),
    inode: expectedIdentity.ino.toString(),
    objectId: "changing-object-id",
  })) {
    Object.defineProperty(result, key, {
      enumerable: true,
      get() {
        getterCalls += 1;
        return value;
      },
    });
  }

  await assert.rejects(
    inspectStoppedTreeObjectIdentity(path, async () => result, expectedIdentity),
    /persistent object identity is invalid/u,
  );
  assert.equal(getterCalls, 0);
});

test("reserved publication names fail before lock or journal mutation", async (t) => {
  for (const finalName of [
    ".stopped-directory-publication.lock",
    ".publication-reserved",
  ]) {
    await t.test(finalName, async (t) => {
      let publicationLockAcquisitions = 0;
      const fixture = await createFixture(t, {
        acquireLock: async (...args) => {
          publicationLockAcquisitions += 1;
          return simpleLockProvider()(...args);
        },
      });
      await assert.rejects(
        fixture.publication.publishCheckpointArtifact(
          captureOptions(fixture, {
            artifactDirectory: join(fixture.artifactOwnedRoot, finalName),
          }),
        ),
        (error) =>
          assertPublicationError(
            error,
            "invalid_publication_request",
            "not-committed",
          ),
      );
      assert.equal(publicationLockAcquisitions, 0);
      assert.equal(
        (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record,
        null,
      );
    });
  }
});

test("NUL-containing publication names fail before lock or journal mutation", async (t) => {
  for (const scenario of [
    "checkpoint final",
    "checkpoint source",
    "restore final",
  ]) {
    await t.test(scenario, async (t) => {
      let publicationLockAcquisitions = 0;
      const fixture = await createFixture(t, {
        acquireLock: async (...args) => {
          publicationLockAcquisitions += 1;
          return simpleLockProvider()(...args);
        },
      });
      if (scenario === "restore final") {
        await publishFixtureArtifact(fixture);
        publicationLockAcquisitions = 0;
      }

      const operation =
        scenario === "restore final"
          ? fixture.publication.publishRestoreDestination(
              restoreOptions(fixture, {
                destinationDirectory: join(
                  fixture.destinationOwnedRoot,
                  "invalid\0name",
                ),
              }),
            )
          : fixture.publication.publishCheckpointArtifact(
              captureOptions(
                fixture,
                scenario === "checkpoint source"
                  ? {
                      sourceDirectory: join(
                        fixture.sourceOwnedRoot,
                        "invalid\0name",
                      ),
                    }
                  : {
                      artifactDirectory: join(
                        fixture.artifactOwnedRoot,
                        "invalid\0name",
                      ),
                    },
              ),
            );
      await assert.rejects(operation, (error) =>
        assertPublicationError(
          error,
          "invalid_publication_request",
          "not-committed",
        ),
      );
      assert.equal(publicationLockAcquisitions, 0);
      assert.equal(
        (
          await fixture.journal.read({
            operationId:
              scenario === "restore final"
                ? RESTORE_OPERATION_ID
                : CAPTURE_OPERATION_ID,
          })
        ).record,
        null,
      );
    });
  }
});

test("restore destinations share the reserved publication namespace", async (t) => {
  let publicationLockAcquisitions = 0;
  const fixture = await createFixture(t, {
    acquireLock: async (...args) => {
      publicationLockAcquisitions += 1;
      return simpleLockProvider()(...args);
    },
  });
  await publishFixtureArtifact(fixture);
  publicationLockAcquisitions = 0;

  await assert.rejects(
    fixture.publication.publishRestoreDestination(
      restoreOptions(fixture, {
        destinationDirectory: join(
          fixture.destinationOwnedRoot,
          ".stopped-directory-publication.lock",
        ),
      }),
    ),
    (error) =>
      assertPublicationError(
        error,
        "invalid_publication_request",
        "not-committed",
      ),
  );
  assert.equal(publicationLockAcquisitions, 0);
  assert.equal(
    (await fixture.journal.read({ operationId: RESTORE_OPERATION_ID })).record,
    null,
  );
});

test("the default filesystem inspector fails closed without a stable ID", async (t) => {
  const fixture = await createFixture(t, { inspectFilesystem: undefined });

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
});

test("publication fails closed without a persistent object identity provider", async (t) => {
  const fixture = await createFixture(t, {
    inspectPersistentObjectIdentity: undefined,
  });

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

test("publication rejects object-ID collisions across storage authorities", async (t) => {
  const fixture = await createFixture(t, {
    inspectPersistentObjectIdentity: (path) =>
      inspectTestPersistentObjectIdentityAs(path, "colliding-authority-id"),
  });

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

test("publication rejects multiple object IDs for one staged object", async (t) => {
  let candidate;
  let candidateInspections = 0;
  const fixture = await createFixture(t, {
    inspectPersistentObjectIdentity: async (path) => {
      if (path !== candidate) return inspectTestPersistentObjectIdentity(path);
      candidateInspections += 1;
      return inspectTestPersistentObjectIdentityAs(
        path,
        candidateInspections % 2 === 1 ? "candidate-object-a" : "candidate-object-b",
      );
    },
  });
  candidate = candidatePath(
    await realpath(fixture.artifactOwnedRoot),
    CAPTURE_OPERATION_ID,
    fixture.artifactDirectory,
  );

  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(captureOptions(fixture)),
    (error) =>
      assertPublicationError(
        error,
        "publication_integrity_failed",
        "not-committed",
      ),
  );
  assert.equal(candidateInspections >= 2, true);
  assert.equal(
    (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record.state,
    "prepared",
  );
  assert.equal(await pathExists(fixture.artifactDirectory), false);
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

test("a prepared operation requires stable journal profile continuity", async (t) => {
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
    (error) =>
      assertPublicationError(
        error,
        "publication_recovery_required",
        "not-committed",
      ),
  );
  assert.equal(
    (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record.state,
    "prepared",
  );
});

test("prepared replay requires source-root object identity scheme continuity", async (t) => {
  let failAfterJournalPrepared = true;
  let fixture;
  let sourceRootPath;
  let sourceRootScheme = "source-root-generation-v1";
  fixture = await createFixture(t, {
    faults: {
      async afterJournalPrepared() {
        if (failAfterJournalPrepared) throw new Error("prepared fault");
      },
    },
    inspectFilesystem: async (path) => ({
      durability: "local-fsync-rename",
      objectIdentityScheme:
        path === sourceRootPath
          ? sourceRootScheme
          : TEST_OBJECT_IDENTITY_SCHEME,
      type: "test-local",
    }),
  });
  sourceRootPath = await realpath(fixture.sourceOwnedRoot);
  const options = captureOptions(fixture);
  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(options),
    (error) =>
      assertPublicationError(error, "publication_io_failed", "not-committed"),
  );
  const prepared = await fixture.journal.read({
    operationId: CAPTURE_OPERATION_ID,
  });
  assert.equal(
    prepared.record.binding.publication.source.root.objectIdentityScheme,
    "source-root-generation-v1",
  );
  assert.equal(
    prepared.record.binding.publication.source.rootFilesystem
      .objectIdentityScheme,
    "source-root-generation-v1",
  );

  sourceRootScheme = "source-root-generation-v2";
  failAfterJournalPrepared = false;
  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(options),
    (error) =>
      assertPublicationError(
        error,
        "publication_recovery_required",
        "not-committed",
      ),
  );
  assert.equal(
    (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record.state,
    "prepared",
  );
});

test("source-root profile failures use durable-state classification", async (t) => {
  await t.test("prepared", async (t) => {
    let failAfterJournalPrepared = true;
    let failSourceRootInspection = false;
    let fixture;
    let sourceRootPath;
    fixture = await createFixture(t, {
      faults: {
        async afterJournalPrepared() {
          if (failAfterJournalPrepared) throw new Error("prepared fault");
        },
      },
      inspectFilesystem: async (path) => {
        if (failSourceRootInspection && path === sourceRootPath) {
          throw new Error("source root inspection failed");
        }
        return { durability: "local-fsync-rename", type: "test-local" };
      },
    });
    sourceRootPath = await realpath(fixture.sourceOwnedRoot);
    const options = captureOptions(fixture);
    await assert.rejects(
      fixture.publication.publishCheckpointArtifact(options),
      (error) =>
        assertPublicationError(error, "publication_io_failed", "not-committed"),
    );
    failAfterJournalPrepared = false;
    failSourceRootInspection = true;
    await assert.rejects(
      fixture.publication.publishCheckpointArtifact(options),
      (error) =>
        assertPublicationError(
          error,
          "publication_recovery_required",
          "not-committed",
        ),
    );
  });

  await t.test("committed", async (t) => {
    let failSourceRootInspection = false;
    let fixture;
    let sourceRootPath;
    fixture = await createFixture(t, {
      inspectFilesystem: async (path) => {
        if (failSourceRootInspection && path === sourceRootPath) {
          throw new Error("source root inspection failed");
        }
        return { durability: "local-fsync-rename", type: "test-local" };
      },
    });
    sourceRootPath = await realpath(fixture.sourceOwnedRoot);
    const options = captureOptions(fixture);
    await fixture.publication.publishCheckpointArtifact(options);
    failSourceRootInspection = true;
    await assert.rejects(
      fixture.publication.publishCheckpointArtifact(options),
      (error) =>
        assertPublicationError(error, "published_state_invalid", "committed"),
    );
  });
});

test("committed replay rejects a changed stable filesystem identity", async (t) => {
  let filesystemId = "test-filesystem-001";
  const fixture = await createFixture(t, {
    inspectFilesystem: async () => ({
      durability: "local-fsync-rename",
      filesystemId,
      objectIdentityScheme: TEST_OBJECT_IDENTITY_SCHEME,
      type: "test-local",
    }),
  });
  const options = captureOptions(fixture);
  await fixture.publication.publishCheckpointArtifact(options);

  filesystemId = "test-filesystem-002";
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

test("committed replay rejects a reused final-root inode generation", async (t) => {
  let candidate;
  let finalPath;
  let finalGeneration = "generation-a";
  let fixture;
  fixture = await createFixture(t, {
    inspectPersistentObjectIdentity: async (path) =>
      path === finalPath || path === candidate
        ? inspectTestPersistentObjectIdentityAs(
            path,
            `final-root-${finalGeneration}`,
          )
        : inspectTestPersistentObjectIdentity(path),
  });
  finalPath = join(
    await realpath(fixture.artifactOwnedRoot),
    basename(fixture.artifactDirectory),
  );
  candidate = candidatePath(
    await realpath(fixture.artifactOwnedRoot),
    CAPTURE_OPERATION_ID,
    fixture.artifactDirectory,
  );
  const options = captureOptions(fixture);
  await fixture.publication.publishCheckpointArtifact(options);
  const runtimeIdentity = await lstat(fixture.artifactDirectory, { bigint: true });

  finalGeneration = "generation-b";
  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(options),
    (error) =>
      assertPublicationError(error, "published_state_invalid", "committed"),
  );

  assert.equal(
    (await lstat(fixture.artifactDirectory, { bigint: true })).ino,
    runtimeIdentity.ino,
  );
  assert.equal(
    (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record.state,
    "committed",
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
      filesystemId: "test-filesystem-001",
      objectIdentityScheme: TEST_OBJECT_IDENTITY_SCHEME,
      type: "test-local",
    }),
    inspectPersistentObjectIdentity: inspectTestPersistentObjectIdentity,
    ...TRUSTED_PUBLICATION_ACL_INSPECTORS,
    ...TRUSTED_PUBLICATION_MOUNT_INSPECTOR,
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

test("a journal descendant mount is rejected before publication mutation", async (t) => {
  let nestedMount;
  let publicationLockAcquisitions = 0;
  const baseLockProvider = simpleLockProvider();
  const fixture = await createFixture(t, {
    acquireLock: async (...args) => {
      publicationLockAcquisitions += 1;
      return baseLockProvider(...args);
    },
    listMountPoints: async () => ["/", nestedMount],
  });
  nestedMount = join(fixture.journalDirectory, "bound-publication-root");
  await mkdir(nestedMount, { mode: 0o700 });

  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(captureOptions(fixture)),
    (error) =>
      assertPublicationError(
        error,
        "invalid_publication_request",
        "uncertain",
      ),
  );
  assert.equal(publicationLockAcquisitions, 0);
  assert.equal(await pathExists(fixture.artifactDirectory), false);
  assert.equal(
    (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record,
    null,
  );
});

test("the injected mount inspector rejects a candidate descendant mount after copy", async (t) => {
  let exposeCandidateMount = false;
  let candidateMount;
  const fixture = await createFixture(t, {
    faults: {
      async afterCopy() {
        exposeCandidateMount = true;
      },
    },
    listMountPoints: async () =>
      exposeCandidateMount ? ["/", candidateMount] : ["/"],
  });
  const candidate = candidatePath(
    await realpath(fixture.artifactOwnedRoot),
    CAPTURE_OPERATION_ID,
    fixture.artifactDirectory,
  );
  candidateMount = join(candidate, "payload", "workspace");

  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(captureOptions(fixture)),
    (error) =>
      assertPublicationError(
        error,
        "publication_io_failed",
        "not-committed",
      ),
  );
  assert.equal(await pathExists(candidate), true);
  assert.equal(await pathExists(fixture.artifactDirectory), false);
  assert.equal(
    (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record.state,
    "prepared",
  );
});

test("the injected mount inspector reaches candidate copy before traversal", async (t) => {
  let exposeCandidateMount = false;
  let candidateMount;
  const fixture = await createFixture(t, {
    faults: {
      async afterCandidateCreated() {
        exposeCandidateMount = true;
      },
    },
    listMountPoints: async () =>
      exposeCandidateMount ? ["/", candidateMount] : ["/"],
  });
  const candidate = candidatePath(
    await realpath(fixture.artifactOwnedRoot),
    CAPTURE_OPERATION_ID,
    fixture.artifactDirectory,
  );
  candidateMount = join(candidate, "payload", "workspace");

  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(captureOptions(fixture)),
    (error) =>
      assertPublicationError(
        error,
        "publication_io_failed",
        "not-committed",
      ),
  );
  assert.equal(await pathExists(join(candidate, "payload")), true);
  assert.equal(await pathExists(join(candidate, "payload", "workspace")), false);
  assert.equal(await pathExists(fixture.artifactDirectory), false);
  assert.equal(
    (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record.state,
    "prepared",
  );
});

test("the injected mount inspector reaches materialized candidate sync", async (t) => {
  let failAfterMaterialized = true;
  let exposeCandidateMount = false;
  let candidateMount;
  let candidateRoot;
  let candidateIdentityVisitedAfterBeforeRename = false;
  let mountChecksAfterBeforeRename = 0;
  const fixture = await createFixture(t, {
    faults: {
      async afterMaterialized() {
        if (failAfterMaterialized) throw new Error("materialized fixture fault");
      },
      async beforeRename() {
        exposeCandidateMount = true;
      },
    },
    inspectPersistentObjectIdentity: async (path) => {
      const identity = await inspectTestPersistentObjectIdentity(path);
      if (
        exposeCandidateMount &&
        (path === candidateRoot || path.startsWith(`${candidateRoot}/`))
      ) {
        candidateIdentityVisitedAfterBeforeRename = true;
      }
      return identity;
    },
    listMountPoints: async () => {
      if (!exposeCandidateMount) return ["/"];
      mountChecksAfterBeforeRename += 1;
      // Materialized replay performs two journal-topology checks, then the
      // candidate sync pre-check. The fourth call is the sync post-check.
      return mountChecksAfterBeforeRename === 4
        ? ["/", candidateMount]
        : ["/"];
    },
  });
  const options = captureOptions(fixture);
  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(options),
    (error) =>
      assertPublicationError(error, "publication_io_failed", "not-committed"),
  );
  const candidate = candidatePath(
    await realpath(fixture.artifactOwnedRoot),
    CAPTURE_OPERATION_ID,
    fixture.artifactDirectory,
  );
  candidateRoot = candidate;
  candidateMount = join(candidate, "payload", "workspace");
  failAfterMaterialized = false;

  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(options),
    (error) =>
      assertPublicationError(
        error,
        "publication_recovery_required",
        "not-committed",
      ),
  );
  assert.equal(mountChecksAfterBeforeRename, 4);
  assert.equal(candidateIdentityVisitedAfterBeforeRename, false);
  assert.equal(await pathExists(candidate), true);
  assert.equal(await pathExists(fixture.artifactDirectory), false);
  assert.equal(
    (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record.state,
    "materialized",
  );
});

test("the injected mount inspector rejects a final descendant mount after rename", async (t) => {
  let exposeFinalMount = false;
  let finalMount;
  const fixture = await createFixture(t, {
    faults: {
      async afterParentSync() {
        exposeFinalMount = true;
      },
    },
    listMountPoints: async () =>
      exposeFinalMount ? ["/", finalMount] : ["/"],
  });
  finalMount = join(fixture.artifactDirectory, "payload", "workspace");

  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(captureOptions(fixture)),
    (error) =>
      assertPublicationError(
        error,
        "publication_outcome_uncertain",
        "uncertain",
      ),
  );
  assert.equal(await pathExists(fixture.artifactDirectory), true);
  assert.equal(
    (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record.state,
    "materialized",
  );
});

test("committed replay rejects a final descendant mount from the injected inspector", async (t) => {
  let exposeFinalMount = false;
  let finalMount;
  const fixture = await createFixture(t, {
    listMountPoints: async () =>
      exposeFinalMount ? ["/", finalMount] : ["/"],
  });
  const options = captureOptions(fixture);
  await fixture.publication.publishCheckpointArtifact(options);
  finalMount = join(fixture.artifactDirectory, "payload", "workspace");
  exposeFinalMount = true;

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

test("committed replay passes the mount inspector to the identity digest", async (t) => {
  let replaying = false;
  let exposeIdentityDigestMount = false;
  let identityDigestTrigger;
  let identityDigestMount;
  let mountChecksAfterTrigger = 0;
  const fixture = await createFixture(t, {
    inspectPersistentObjectIdentity: async (path) => {
      const identity = await inspectTestPersistentObjectIdentity(path);
      if (replaying && path === identityDigestTrigger) {
        exposeIdentityDigestMount = true;
      }
      return identity;
    },
    listMountPoints: async () => {
      if (!exposeIdentityDigestMount) return ["/"];
      mountChecksAfterTrigger += 1;
      return ["/", identityDigestMount];
    },
  });
  const options = captureOptions(fixture);
  await fixture.publication.publishCheckpointArtifact(options);
  const publishedRoot = await realpath(fixture.artifactDirectory);
  identityDigestTrigger = join(
    publishedRoot,
    "payload",
    "workspace",
    "README.md",
  );
  // This sibling is inside the identity-digest root but outside the modeled
  // payload, so only the identity-digest mount check can reject it.
  identityDigestMount = join(publishedRoot, "artifact.json");
  replaying = true;

  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(options),
    (error) =>
      assertPublicationError(error, "published_state_invalid", "committed"),
  );
  assert.equal(exposeIdentityDigestMount, true);
  assert.equal(mountChecksAfterTrigger, 1);
});

test("committed replay passes the mount inspector to the modeled digest", async (t) => {
  let replaying = false;
  let modeledDigestArmed = false;
  let modeledDigestTrigger;
  let modeledDigestMount;
  let mountChecksAfterTrigger = 0;
  const fixture = await createFixture(t, {
    inspectPersistentObjectIdentity: async (path) => {
      const identity = await inspectTestPersistentObjectIdentity(path);
      if (replaying && path === modeledDigestTrigger) modeledDigestArmed = true;
      return identity;
    },
    listMountPoints: async () => {
      if (!modeledDigestArmed) return ["/"];
      mountChecksAfterTrigger += 1;
      return mountChecksAfterTrigger === 1 ? ["/"] : ["/", modeledDigestMount];
    },
  });
  const options = captureOptions(fixture);
  await fixture.publication.publishCheckpointArtifact(options);
  const publishedRoot = await realpath(fixture.artifactDirectory);
  modeledDigestTrigger = join(
    publishedRoot,
    "payload",
    "workspace",
    "README.md",
  );
  modeledDigestMount = join(publishedRoot, "payload", "workspace");
  replaying = true;

  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(options),
    (error) =>
      assertPublicationError(error, "published_state_invalid", "committed"),
  );
  assert.equal(modeledDigestArmed, true);
  assert.equal(mountChecksAfterTrigger, 2);
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
  const sourceIdentity = await lstat(
    join(fixture.sourceDirectory, "workspace"),
    { bigint: true },
  );
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
  let publicationLockAcquisitions = 0;
  const baseLockProvider = simpleLockProvider();
  const publication = new StoppedDirectoryPublication({
    journal,
    acquireLock: async (...args) => {
      publicationLockAcquisitions += 1;
      return baseLockProvider(...args);
    },
    inspectFilesystem: async () => ({
      durability: "local-fsync-rename",
      filesystemId: "test-filesystem-001",
      objectIdentityScheme: TEST_OBJECT_IDENTITY_SCHEME,
      type: "test-local",
    }),
    inspectPersistentObjectIdentity: inspectTestPersistentObjectIdentity,
    ...TRUSTED_PUBLICATION_ACL_INSPECTORS,
    ...TRUSTED_PUBLICATION_MOUNT_INSPECTOR,
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
  assert.equal(publicationLockAcquisitions, 0);
  assert.equal(await pathExists(fixture.artifactDirectory), false);
  assert.equal(
    (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record,
    null,
  );
});

test("publication topology is revalidated after lock acquisition", async (t) => {
  let fixture;
  const displaced = "displaced-artifact-root-after-lock";
  const baseLockProvider = simpleLockProvider();
  fixture = await createFixture(t, {
    acquireLock: async (...args) => {
      await rename(
        fixture.artifactOwnedRoot,
        join(fixture.root, displaced),
      );
      await mkdir(fixture.artifactOwnedRoot, { mode: 0o700 });
      return baseLockProvider(...args);
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
    (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record,
    null,
  );
  assert.equal(await pathExists(fixture.artifactDirectory), false);
  assert.equal(
    await pathExists(
      join(fixture.root, displaced, basename(fixture.artifactDirectory)),
    ),
    false,
  );
});

test("journal filesystem inspection cannot swap a root before lock creation", async (t) => {
  let fixture;
  let publicationLockAcquisitions = 0;
  let swapped = false;
  const baseLockProvider = simpleLockProvider();
  fixture = await createFixture(t, {
    acquireLock: async (...args) => {
      publicationLockAcquisitions += 1;
      return baseLockProvider(...args);
    },
    inspectFilesystem: async () => {
      if (!swapped) {
        swapped = true;
        await rename(
          fixture.artifactOwnedRoot,
          join(fixture.root, "displaced-artifact-root-during-inspection"),
        );
        await mkdir(fixture.artifactOwnedRoot, { mode: 0o700 });
      }
      return {
        durability: "local-fsync-rename",
        type: "test-local",
      };
    },
  });

  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(captureOptions(fixture)),
    (error) =>
      assertPublicationError(
        error,
        "invalid_publication_request",
        "uncertain",
      ),
  );
  assert.equal(publicationLockAcquisitions, 0);
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
      filesystemId: "test-filesystem-001",
      objectIdentityScheme: TEST_OBJECT_IDENTITY_SCHEME,
      type: "test-local",
    }),
    inspectPersistentObjectIdentity: inspectTestPersistentObjectIdentity,
    ...TRUSTED_PUBLICATION_ACL_INSPECTORS,
    ...TRUSTED_PUBLICATION_MOUNT_INSPECTOR,
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
  const durablePublication = JSON.stringify({
    binding: journalRecord.binding.publication,
    materialization: journalRecord.materialization,
  });
  assert.equal(durablePublication.includes('"device"'), false);
  assert.equal(durablePublication.includes('"inode"'), false);
  assert.equal(durablePublication.includes('"filesystemId"'), true);
  assert.equal(durablePublication.includes('"objectId"'), true);
  assert.equal(durablePublication.includes('"objectIdentityScheme"'), true);
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

test("restore preserves a modeled payload-root mode inside private storage", async (t) => {
  let interruptAfterMaterialized = false;
  let afterCopyCalls = 0;
  const fixture = await createFixture(t, {
    faults: {
      async afterCopy() {
        afterCopyCalls += 1;
      },
      async afterMaterialized() {
        if (interruptAfterMaterialized) throw new Error("materialized fault");
      },
    },
  });
  await chmod(fixture.sourceDirectory, 0o755);
  await publishFixtureArtifact(fixture);
  assert.equal((await lstat(fixture.artifactDirectory)).mode & 0o777, 0o700);
  assert.equal(
    (await lstat(join(fixture.artifactDirectory, "payload"))).mode & 0o777,
    0o755,
  );
  const copyCallsAfterCapture = afterCopyCalls;
  const options = restoreOptions(fixture);
  interruptAfterMaterialized = true;

  await assert.rejects(
    fixture.publication.publishRestoreDestination(options),
    (error) =>
      assertPublicationError(error, "publication_io_failed", "not-committed"),
  );
  const candidate = candidatePath(
    fixture.destinationOwnedRoot,
    RESTORE_OPERATION_ID,
    fixture.destinationDirectory,
  );
  assert.equal((await lstat(candidate)).mode & 0o777, 0o755);
  assert.equal(
    (await fixture.journal.read({ operationId: RESTORE_OPERATION_ID })).record.state,
    "materialized",
  );

  interruptAfterMaterialized = false;
  const restored = await fixture.publication.publishRestoreDestination(options);
  assert.deepEqual(restored.result, options.result);
  assert.equal(afterCopyCalls, copyCallsAfterCapture + 1);
  assert.equal((await lstat(fixture.destinationDirectory)).mode & 0o777, 0o755);
  assert.equal(
    await digestTree(fixture.destinationDirectory),
    await digestTree(fixture.sourceDirectory),
  );
});

test("an exact committed replay returns the fixed result without recopying", async (t) => {
  let afterCopyCalls = 0;
  const inspectedFilesystems = [];
  const fixture = await createFixture(t, {
    faults: {
      async afterCopy() {
        afterCopyCalls += 1;
      },
    },
    inspectFilesystem: async (path) => {
      inspectedFilesystems.push(path);
      return {
        durability: "local-fsync-rename",
        type: "test-local",
      };
    },
  });
  const options = captureOptions(fixture);
  const first = await fixture.publication.publishCheckpointArtifact(options);
  const artifactIdentity = await lstat(fixture.artifactDirectory, { bigint: true });
  await rm(fixture.sourceDirectory, { force: true, recursive: true });
  await writeFile(fixture.sourceDirectory, "reused source pathname\n", {
    mode: 0o600,
  });
  inspectedFilesystems.length = 0;

  const replay = await fixture.publication.publishCheckpointArtifact(options);

  assert.deepEqual(replay.result, first.result);
  assert.deepEqual(replay.materialization, first.materialization);
  assert.equal(replay.replayed, true);
  assert.equal(afterCopyCalls, 1);
  assert.equal(inspectedFilesystems.includes(fixture.sourceDirectory), false);
  const replayedIdentity = await lstat(fixture.artifactDirectory, { bigint: true });
  assert.equal(replayedIdentity.dev, artifactIdentity.dev);
  assert.equal(replayedIdentity.ino, artifactIdentity.ino);
});

test("committed verification is source-independent and never transitions the journal", async (t) => {
  const guard = { armed: false, calls: [] };
  const fixture = await createFixture(t, {
    JournalClass: armableJournalClass(guard),
  });
  const options = captureOptions(fixture);
  const published = await fixture.publication.publishCheckpointArtifact(options);
  const journalBefore = await fixture.journal.read({
    operationId: CAPTURE_OPERATION_ID,
  });
  const artifactBefore = await lstat(fixture.artifactDirectory, {
    bigint: true,
  });
  guard.armed = true;
  await rm(fixture.sourceOwnedRoot, { force: true, recursive: true });

  await assert.rejects(
    fixture.publication.verifyCommittedCheckpointArtifact({
      ...committedVerificationOptions(fixture, options),
      sourceDirectory: fixture.sourceDirectory,
    }),
    (error) =>
      assertPublicationError(
        error,
        "invalid_publication_request",
        "not-committed",
      ),
  );

  const verified = await fixture.publication.verifyCommittedCheckpointArtifact(
    committedVerificationOptions(fixture, options),
  );

  assert.deepEqual(verified.result, published.result);
  assert.deepEqual(verified.materialization, published.materialization);
  assert.equal(verified.replayed, true);
  assert.equal(Object.isFrozen(verified), true);
  assert.equal(Object.isFrozen(verified.materialization), true);
  assert.equal(Object.isFrozen(verified.result), true);
  assert.deepEqual(guard.calls, []);
  assert.deepEqual(
    await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID }),
    journalBefore,
  );
  const artifactAfter = await lstat(fixture.artifactDirectory, {
    bigint: true,
  });
  assert.equal(artifactAfter.dev, artifactBefore.dev);
  assert.equal(artifactAfter.ino, artifactBefore.ino);
});

test("committed verification preserves commit state when its lock fails after journal read", async (t) => {
  const fixture = await createFixture(t);
  const options = captureOptions(fixture);
  await fixture.publication.publishCheckpointArtifact(options);
  let publicationLockLost = false;
  let lockAssertions = 0;
  const journal = new FilesystemOperationJournal({
    directory: fixture.journalDirectory,
    acquireLock: simpleLockProvider(),
    faults: {
      async beforeLockRelease({ operationId }) {
        if (operationId === CAPTURE_OPERATION_ID) publicationLockLost = true;
      },
    },
    ...TRUSTED_JOURNAL_ACL_INSPECTORS,
  });
  const publication = new StoppedDirectoryPublication({
    journal,
    acquireLock: async () => ({
      async assertHeld() {
        lockAssertions += 1;
        if (publicationLockLost) throw new Error("publication lock was lost");
      },
      async release() {},
    }),
    inspectFilesystem: async () => ({
      durability: "local-fsync-rename",
      filesystemId: "test-filesystem-001",
      objectIdentityScheme: TEST_OBJECT_IDENTITY_SCHEME,
      type: "test-local",
    }),
    inspectPersistentObjectIdentity: inspectTestPersistentObjectIdentity,
    ...TRUSTED_PUBLICATION_ACL_INSPECTORS,
    ...TRUSTED_PUBLICATION_MOUNT_INSPECTOR,
  });
  await rm(fixture.sourceOwnedRoot, { force: true, recursive: true });

  await assert.rejects(
    publication.verifyCommittedCheckpointArtifact(
      committedVerificationOptions(fixture, options),
    ),
    (error) =>
      assertPublicationError(error, "published_state_invalid", "committed"),
  );
  assert.equal(publicationLockLost, true);
  assert.equal(lockAssertions >= 3, true);
});

test("committed verification refuses non-committed phases without advancing state", async (t) => {
  for (const phase of ["absent", "prepared", "materialized"]) {
    await t.test(phase, async (t) => {
      const guard = { armed: false, calls: [] };
      let seedFaultEnabled = phase !== "absent";
      const faults =
        phase === "prepared"
          ? {
              async afterJournalPrepared() {
                if (seedFaultEnabled) throw new Error("retain prepared state");
              },
            }
          : phase === "materialized"
            ? {
                async afterMaterialized() {
                  if (seedFaultEnabled) throw new Error("retain materialized state");
                },
              }
            : {};
      const fixture = await createFixture(t, {
        JournalClass: armableJournalClass(guard),
        faults,
      });
      const options = captureOptions(fixture);
      if (phase !== "absent") {
        await assert.rejects(
          fixture.publication.publishCheckpointArtifact(options),
          (error) => assertPublicationError(error),
        );
      }
      seedFaultEnabled = false;
      const candidate = candidatePath(
        fixture.artifactOwnedRoot,
        CAPTURE_OPERATION_ID,
        fixture.artifactDirectory,
      );
      const recordBefore = await fixture.journal.read({
        operationId: CAPTURE_OPERATION_ID,
      });
      const candidateBefore = await pathExists(candidate);
      const finalBefore = await pathExists(fixture.artifactDirectory);
      guard.armed = true;
      await rm(fixture.sourceOwnedRoot, { force: true, recursive: true });

      await assert.rejects(
        fixture.publication.verifyCommittedCheckpointArtifact(
          committedVerificationOptions(fixture, options),
        ),
        (error) =>
          assertPublicationError(
            error,
            "publication_recovery_required",
            "not-committed",
          ),
      );

      assert.deepEqual(guard.calls, []);
      assert.deepEqual(
        await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID }),
        recordBefore,
      );
      assert.equal(await pathExists(candidate), candidateBefore);
      assert.equal(await pathExists(fixture.artifactDirectory), finalBefore);
    });
  }
});

test("committed verification rejects binding, result, and artifact tampering", async (t) => {
  const fixture = await createFixture(t);
  const options = captureOptions(fixture);
  await fixture.publication.publishCheckpointArtifact(options);
  await rm(fixture.sourceOwnedRoot, { force: true, recursive: true });

  await assert.rejects(
    fixture.publication.verifyCommittedCheckpointArtifact(
      committedVerificationOptions(fixture, options, {
        binding: { ...options.binding, storageId: "volume-other" },
      }),
    ),
    (error) =>
      assertPublicationError(error, "publication_conflict", "committed"),
  );

  await assert.rejects(
    fixture.publication.verifyCommittedCheckpointArtifact(
      committedVerificationOptions(fixture, options, {
        result: {
          ...options.result,
          mutation: {
            ...options.result.mutation,
            proofId: "proof-checkpoint-other",
          },
        },
      }),
    ),
    (error) =>
      assertPublicationError(error, "publication_conflict", "committed"),
  );

  await writeFile(join(fixture.artifactDirectory, "unexpected"), "tampered\n", {
    mode: 0o600,
  });
  await assert.rejects(
    fixture.publication.verifyCommittedCheckpointArtifact(
      committedVerificationOptions(fixture, options),
    ),
    (error) =>
      assertPublicationError(error, "published_state_invalid", "committed"),
  );
});

test("committed replay does not inspect a replacement source directory", async (t) => {
  let rejectReplacementInspection = false;
  let fixture;
  const replacementWasInspected = (path) =>
    rejectReplacementInspection &&
    (path === fixture.sourceDirectory ||
      path.startsWith(`${fixture.sourceDirectory}/`));
  fixture = await createFixture(t, {
    inspectFilesystem: async (path) => {
      if (replacementWasInspected(path)) {
        throw new Error("replacement source filesystem was inspected");
      }
      return {
        durability: "local-fsync-rename",
        type: "test-local",
      };
    },
    inspectPersistentObjectIdentity: async (path) => {
      if (replacementWasInspected(path)) {
        throw new Error("replacement source object identity was inspected");
      }
      return inspectTestPersistentObjectIdentity(path);
    },
  });
  const options = captureOptions(fixture);
  const first = await fixture.publication.publishCheckpointArtifact(options);
  await rm(fixture.sourceDirectory, { force: true, recursive: true });
  await mkdir(fixture.sourceDirectory, { mode: 0o700 });
  await writeFile(
    join(fixture.sourceDirectory, "nonportable-\u00c9"),
    "replacement\n",
    { mode: 0o600 },
  );
  rejectReplacementInspection = true;

  const replay = await fixture.publication.publishCheckpointArtifact(options);

  assert.deepEqual(replay.result, first.result);
  assert.deepEqual(replay.materialization, first.materialization);
  assert.equal(replay.replayed, true);
});

test("materialized replay does not inspect a replacement source directory", async (t) => {
  let interruptAfterMaterialized = true;
  let rejectReplacementInspection = false;
  let fixture;
  const replacementWasInspected = (path) =>
    rejectReplacementInspection &&
    (path === fixture.sourceDirectory ||
      path.startsWith(`${fixture.sourceDirectory}/`));
  fixture = await createFixture(t, {
    faults: {
      async afterMaterialized() {
        if (interruptAfterMaterialized) {
          throw new Error("materialized replay fixture fault");
        }
      },
    },
    inspectFilesystem: async (path) => {
      if (replacementWasInspected(path)) {
        throw new Error("replacement source filesystem was inspected");
      }
      return {
        durability: "local-fsync-rename",
        type: "test-local",
      };
    },
    inspectPersistentObjectIdentity: async (path) => {
      if (replacementWasInspected(path)) {
        throw new Error("replacement source object identity was inspected");
      }
      return inspectTestPersistentObjectIdentity(path);
    },
  });
  const options = captureOptions(fixture);
  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(options),
    (error) =>
      assertPublicationError(error, "publication_io_failed", "not-committed"),
  );
  assert.equal(
    (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record.state,
    "materialized",
  );
  await rm(fixture.sourceDirectory, { force: true, recursive: true });
  await mkdir(fixture.sourceDirectory, { mode: 0o700 });
  await writeFile(
    join(fixture.sourceDirectory, "nonportable-\u00c9"),
    "replacement\n",
    { mode: 0o600 },
  );
  interruptAfterMaterialized = false;
  rejectReplacementInspection = true;

  const replay = await fixture.publication.publishCheckpointArtifact(options);

  assert.deepEqual(replay.result, options.result);
  assert.equal(
    (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record.state,
    "committed",
  );
});

test("a fresh publication rejects a non-directory source leaf", async (t) => {
  const fixture = await createFixture(t);
  await rm(fixture.sourceDirectory, { force: true, recursive: true });
  await writeFile(fixture.sourceDirectory, "reused source pathname\n", {
    mode: 0o600,
  });

  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(captureOptions(fixture)),
    (error) =>
      assertPublicationError(
        error,
        "invalid_publication_request",
        "not-committed",
      ),
  );
  assert.equal(
    (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record,
    null,
  );
  assert.equal(await pathExists(fixture.artifactDirectory), false);
});

test("prepared replay requires a usable source leaf without following symlinks", async (t) => {
  for (const sourceKind of ["missing", "file", "symlink"]) {
    await t.test(sourceKind, async (t) => {
      let failAfterJournalPrepared = true;
      const inspectedFilesystems = [];
      const fixture = await createFixture(t, {
        faults: {
          async afterJournalPrepared() {
            if (failAfterJournalPrepared) throw new Error("prepared fault");
          },
        },
        inspectFilesystem: async (path) => {
          inspectedFilesystems.push(path);
          return {
            durability: "local-fsync-rename",
            type: "test-local",
          };
        },
      });
      const options = captureOptions(fixture);
      await assert.rejects(
        fixture.publication.publishCheckpointArtifact(options),
        (error) =>
          assertPublicationError(
            error,
            "publication_io_failed",
            "not-committed",
          ),
      );
      await rm(fixture.sourceDirectory, { force: true, recursive: true });
      let symlinkTarget;
      if (sourceKind === "file") {
        await writeFile(fixture.sourceDirectory, "reused source pathname\n", {
          mode: 0o600,
        });
      } else if (sourceKind === "symlink") {
        symlinkTarget = join(fixture.root, "unrelated-symlink-target");
        await mkdir(symlinkTarget, { mode: 0o700 });
        await symlink(symlinkTarget, fixture.sourceDirectory);
      }
      inspectedFilesystems.length = 0;
      failAfterJournalPrepared = false;

      await assert.rejects(
        fixture.publication.publishCheckpointArtifact(options),
        (error) =>
          assertPublicationError(
            error,
            "publication_recovery_required",
            "not-committed",
          ),
      );
      assert.equal(
        (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record
          .state,
        "prepared",
      );
      if (symlinkTarget !== undefined) {
        assert.equal(inspectedFilesystems.includes(fixture.sourceDirectory), false);
        assert.equal(inspectedFilesystems.includes(symlinkTarget), false);
      }
    });
  }
});

test("prepared replay rejects replacement of the recorded source leaf", async (t) => {
  let failAfterJournalPrepared = true;
  let afterCopyCalls = 0;
  const fixture = await createFixture(t, {
    faults: {
      async afterCopy() {
        afterCopyCalls += 1;
      },
      async afterJournalPrepared() {
        if (failAfterJournalPrepared) throw new Error("prepared fault");
      },
    },
  });
  const options = captureOptions(fixture);
  const { objectId: sourceObjectId } =
    await inspectTestPersistentObjectIdentity(fixture.sourceDirectory);
  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(options),
    (error) =>
      assertPublicationError(error, "publication_io_failed", "not-committed"),
  );
  const prepared = await fixture.journal.read({
    operationId: CAPTURE_OPERATION_ID,
  });
  assert.equal(
    prepared.record.binding.publication.source.directoryIdentity.filesystemId,
    "test-filesystem-001",
  );
  assert.equal(
    prepared.record.binding.publication.source.directoryIdentity.objectId,
    sourceObjectId,
  );

  await rename(
    fixture.sourceDirectory,
    join(fixture.root, "displaced-source-leaf"),
  );
  await mkdir(join(fixture.sourceDirectory, "workspace"), {
    mode: 0o700,
    recursive: true,
  });
  await writeFile(
    join(fixture.sourceDirectory, "workspace", "README.md"),
    "replacement\n",
    { mode: 0o600 },
  );
  failAfterJournalPrepared = false;

  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(options),
    (error) =>
      assertPublicationError(
        error,
        "publication_recovery_required",
        "not-committed",
      ),
  );
  assert.equal(afterCopyCalls, 0);
  assert.equal(
    (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record.state,
    "prepared",
  );
  assert.equal(
    await pathExists(
      candidatePath(
        fixture.artifactOwnedRoot,
        CAPTURE_OPERATION_ID,
        fixture.artifactDirectory,
      ),
    ),
    false,
  );
  assert.equal(await pathExists(fixture.artifactDirectory), false);
});

test("prepared replay rejects a reused source-leaf inode generation", async (t) => {
  let afterCopyCalls = 0;
  let failAfterJournalPrepared = true;
  let fixture;
  let sourcePath;
  let sourceGeneration = "generation-a";
  fixture = await createFixture(t, {
    faults: {
      async afterCopy() {
        afterCopyCalls += 1;
      },
      async afterJournalPrepared() {
        if (failAfterJournalPrepared) throw new Error("prepared fault");
      },
    },
    inspectPersistentObjectIdentity: async (path) =>
      path === sourcePath
        ? inspectTestPersistentObjectIdentityAs(
            path,
            `source-leaf-${sourceGeneration}`,
          )
        : inspectTestPersistentObjectIdentity(path),
  });
  sourcePath = await realpath(fixture.sourceDirectory);
  const options = captureOptions(fixture);
  const runtimeIdentity = await lstat(fixture.sourceDirectory, { bigint: true });
  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(options),
    (error) =>
      assertPublicationError(error, "publication_io_failed", "not-committed"),
  );

  sourceGeneration = "generation-b";
  failAfterJournalPrepared = false;
  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(options),
    (error) =>
      assertPublicationError(
        error,
        "publication_recovery_required",
        "not-committed",
      ),
  );

  assert.equal(
    (await lstat(fixture.sourceDirectory, { bigint: true })).ino,
    runtimeIdentity.ino,
  );
  assert.equal(afterCopyCalls, 0);
  assert.equal(
    (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record.state,
    "prepared",
  );
});

test("source object generation changes after its barrier abort materialization", async (t) => {
  let fixture;
  let sourceGeneration = "generation-a";
  let sourcePath;
  fixture = await createFixture(t, {
    faults: {
      async afterSourceBarrier() {
        sourceGeneration = "generation-b";
      },
    },
    inspectPersistentObjectIdentity: async (path) =>
      path === sourcePath
        ? inspectTestPersistentObjectIdentityAs(
            path,
            `source-leaf-${sourceGeneration}`,
          )
        : inspectTestPersistentObjectIdentity(path),
  });
  sourcePath = await realpath(fixture.sourceDirectory);

  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(captureOptions(fixture)),
    (error) =>
      assertPublicationError(
        error,
        "publication_integrity_failed",
        "not-committed",
      ),
  );
  assert.equal(
    (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record.state,
    "prepared",
  );
  assert.equal(await pathExists(fixture.artifactDirectory), false);
});

test("source filesystem identity changes after its barrier abort materialization", async (t) => {
  let fixture;
  let sourceIdentityScheme = "source-generation-v1";
  let sourcePath;
  fixture = await createFixture(t, {
    faults: {
      async afterSourceBarrier() {
        sourceIdentityScheme = "source-generation-v2";
      },
    },
    inspectFilesystem: async (path) => ({
      durability: "local-fsync-rename",
      objectIdentityScheme:
        path === sourcePath
          ? sourceIdentityScheme
          : TEST_OBJECT_IDENTITY_SCHEME,
      type: "test-local",
    }),
  });
  sourcePath = await realpath(fixture.sourceDirectory);

  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(captureOptions(fixture)),
    (error) =>
      assertPublicationError(
        error,
        "publication_integrity_failed",
        "not-committed",
      ),
  );
  assert.equal(
    (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record.state,
    "prepared",
  );
  assert.equal(await pathExists(fixture.artifactDirectory), false);
});

test("target persistent identity changes after the source barrier abort publication", async (t) => {
  for (const change of ["filesystem", "object"]) {
    await t.test(change, async (t) => {
      let fixture;
      let targetFilesystemId = "target-filesystem-a";
      let targetGeneration = "target-generation-a";
      let targetRootPath;
      fixture = await createFixture(t, {
        faults: {
          async afterSourceBarrier() {
            if (change === "filesystem") {
              targetFilesystemId = "target-filesystem-b";
            } else {
              targetGeneration = "target-generation-b";
            }
          },
        },
        inspectFilesystem: async (path) => ({
          durability: "local-fsync-rename",
          filesystemId:
            path === targetRootPath
              ? targetFilesystemId
              : "test-filesystem-001",
          objectIdentityScheme: TEST_OBJECT_IDENTITY_SCHEME,
          type: "test-local",
        }),
        inspectPersistentObjectIdentity: async (path) =>
          path === targetRootPath
            ? inspectTestPersistentObjectIdentityAs(path, targetGeneration)
            : inspectTestPersistentObjectIdentity(path),
      });
      targetRootPath = await realpath(fixture.artifactOwnedRoot);

      await assert.rejects(
        fixture.publication.publishCheckpointArtifact(captureOptions(fixture)),
        (error) =>
          assertPublicationError(
            error,
            "publication_integrity_failed",
            "not-committed",
          ),
      );
      assert.equal(
        (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record
          .state,
        "prepared",
      );
      assert.equal(await pathExists(fixture.artifactDirectory), false);
    });
  }
});

test("prepared topology probe failures remain publication-uncertain", async (t) => {
  let failAfterJournalPrepared = true;
  const fixture = await createFixture(t, {
    faults: {
      async afterJournalPrepared() {
        if (failAfterJournalPrepared) throw new Error("prepared fault");
      },
    },
  });
  const options = captureOptions(fixture);
  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(options),
    (error) =>
      assertPublicationError(error, "publication_io_failed", "not-committed"),
  );
  failAfterJournalPrepared = false;

  class TargetBlockingJournal extends FilesystemOperationJournal {
    async read(readOptions) {
      const outcome = await super.read(readOptions);
      if (outcome.record?.state === "prepared") {
        await chmod(fixture.artifactOwnedRoot, 0o600);
      }
      return outcome;
    }
  }
  const blockingJournal = new TargetBlockingJournal({
    directory: fixture.journalDirectory,
    acquireLock: simpleLockProvider(),
    ...TRUSTED_JOURNAL_ACL_INSPECTORS,
  });
  const publication = new StoppedDirectoryPublication({
    journal: blockingJournal,
    acquireLock: simpleLockProvider(),
    inspectFilesystem: async () => ({
      durability: "local-fsync-rename",
      filesystemId: "test-filesystem-001",
      objectIdentityScheme: TEST_OBJECT_IDENTITY_SCHEME,
      type: "test-local",
    }),
    inspectPersistentObjectIdentity: inspectTestPersistentObjectIdentity,
    inspectOwnedRootAcl: async () => false,
    inspectOwnedRootAncestorAcl: async () => false,
    ...TRUSTED_PUBLICATION_MOUNT_INSPECTOR,
  });

  try {
    await assert.rejects(
      publication.publishCheckpointArtifact(options),
      (error) =>
        assertPublicationError(
          error,
          "publication_outcome_uncertain",
          "uncertain",
        ),
    );
  } finally {
    await chmod(fixture.artifactOwnedRoot, 0o700);
  }
  assert.equal(
    (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record.state,
    "prepared",
  );
});

test("publication accepts trusted ACL inspectors for every owned root", async (t) => {
  const inspectedRoots = [];
  const inspectedAncestors = [];
  const fixture = await createFixture(t, {
    async inspectOwnedRootAcl(path) {
      inspectedRoots.push(path);
      return false;
    },
    async inspectOwnedRootAncestorAcl(path) {
      inspectedAncestors.push(path);
      return false;
    },
  });
  const sourceOwnedRoot = await realpath(fixture.sourceOwnedRoot);
  const artifactOwnedRoot = await realpath(fixture.artifactOwnedRoot);

  const outcome = await fixture.publication.publishCheckpointArtifact(
    captureOptions(fixture),
  );

  assert.equal(outcome.result.mutation.status, "checkpoint-created");
  assert(inspectedRoots.includes(sourceOwnedRoot));
  assert(inspectedRoots.includes(artifactOwnedRoot));
  assert(
    inspectedRoots.includes(
      candidatePath(
        artifactOwnedRoot,
        CAPTURE_OPERATION_ID,
        fixture.artifactDirectory,
      ),
    ),
  );
  assert(inspectedAncestors.length > 0);
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

test("committed checkpoint replay rejects a non-private bundle root", async (t) => {
  const fixture = await createFixture(t);
  const options = captureOptions(fixture);
  await fixture.publication.publishCheckpointArtifact(options);
  await chmod(fixture.artifactDirectory, 0o755);

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

test("a missing publication root cannot downgrade committed replay", async (t) => {
  const fixture = await createFixture(t);
  const options = captureOptions(fixture);
  await fixture.publication.publishCheckpointArtifact(options);
  await rm(fixture.artifactOwnedRoot, { force: true, recursive: true });

  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(options),
    (error) =>
      assertPublicationError(
        error,
        "publication_outcome_uncertain",
        "uncertain",
      ),
  );
});

test("a missing source root cannot downgrade committed replay", async (t) => {
  const fixture = await createFixture(t);
  const options = captureOptions(fixture);
  await fixture.publication.publishCheckpointArtifact(options);
  await rm(fixture.sourceOwnedRoot, { force: true, recursive: true });

  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(options),
    (error) =>
      assertPublicationError(
        error,
        "publication_outcome_uncertain",
        "uncertain",
      ),
  );
});

test("malformed owned-root paths remain caller errors", async (t) => {
  const fixture = await createFixture(t);
  const options = captureOptions(fixture);

  await assert.rejects(
    fixture.publication.publishCheckpointArtifact({
      ...options,
      sourceOwnedRoot: "relative-source-root",
    }),
    (error) =>
      assertPublicationError(
        error,
        "invalid_publication_request",
        "not-committed",
      ),
  );
  await assert.rejects(
    fixture.publication.publishCheckpointArtifact({
      ...options,
      artifactOwnedRoot: "relative-artifact-root",
    }),
    (error) =>
      assertPublicationError(
        error,
        "invalid_publication_request",
        "not-committed",
      ),
  );
});

test("malformed committed materialization is committed-state corruption", async (t) => {
  const fixture = await createFixture(t);
  const options = captureOptions(fixture);
  await fixture.publication.publishCheckpointArtifact(options);
  const recordPath = join(
    fixture.journalDirectory,
    operationJournalRecordFilename(CAPTURE_OPERATION_ID),
  );
  const record = JSON.parse(await readFile(recordPath, "utf8"));
  const materialization = record.materialization;
  record.materialization = {
    artifactManifestDigest: materialization.artifactManifestDigest,
    contractVersion: materialization.contractVersion,
    extra: true,
    modeledDigest: materialization.modeledDigest,
    publicationId: materialization.publicationId,
    publicationKind: materialization.publicationKind,
    stagedRoot: materialization.stagedRoot,
    treeIdentityDigest: materialization.treeIdentityDigest,
  };
  await writeFile(recordPath, `${JSON.stringify(record)}\n`, { mode: 0o600 });

  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(options),
    (error) =>
      assertPublicationError(error, "published_state_invalid", "committed"),
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

test("default publication requires a preprovisioned lock without creating it", async (t) => {
  const fixture = await createFixture(t, { acquireLock: undefined });
  const lockPath = join(
    fixture.artifactOwnedRoot,
    STOPPED_DIRECTORY_PUBLICATION_LOCK_NAME,
  );

  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(captureOptions(fixture)),
    (error) =>
      assertPublicationError(
        error,
        "publication_outcome_uncertain",
        "uncertain",
      ),
  );
  assert.equal(await pathExists(lockPath), false);
  assert.equal(await pathExists(fixture.artifactDirectory), false);
  assert.equal(
    (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record,
    null,
  );
});

test("source topology preflight runs while the publication lock is held", async (t) => {
  let fixture;
  let sourcePath;
  let lockActive = false;
  let sourceFilesystemInspections = 0;
  let acquisitionOptions;
  const baseLockProvider = simpleLockProvider();
  fixture = await createFixture(t, {
    acquireLock: async (path, options) => {
      acquisitionOptions = options;
      const acquired = await baseLockProvider(path, options);
      lockActive = true;
      return {
        ...acquired,
        async release() {
          try {
            await acquired.release();
          } finally {
            lockActive = false;
          }
        },
      };
    },
    inspectFilesystem: async (path) => {
      if (sourcePath !== undefined && path === sourcePath) {
        sourceFilesystemInspections += 1;
        assert.equal(lockActive, true);
      }
      return { durability: "local-fsync-rename", type: "test-local" };
    },
  });
  sourcePath = await realpath(fixture.sourceDirectory);

  await fixture.publication.publishCheckpointArtifact(captureOptions(fixture));

  assert.deepEqual(acquisitionOptions, { requireExisting: true });
  assert(sourceFilesystemInspections > 0);
  assert.equal(lockActive, false);
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

test("callback-created destinations remain outcome-uncertain and preserved", async (t) => {
  for (const throwAfterCreate of [false, true]) {
    await t.test(throwAfterCreate ? "callback throws" : "callback returns", async (t) => {
      let candidateIdentity;
      let fixture;
      const sentinelName = "foreign-sentinel";
      fixture = await createFixture(t, {
        acquireLock: undefined,
        faults: {
          async beforeRename() {
            const candidate = candidatePath(
              fixture.artifactOwnedRoot,
              CAPTURE_OPERATION_ID,
              fixture.artifactDirectory,
            );
            candidateIdentity = await lstat(candidate, { bigint: true });
            await mkdir(fixture.artifactDirectory, { mode: 0o700 });
            await writeFile(
              join(fixture.artifactDirectory, sentinelName),
              "preserve\n",
              { mode: 0o600 },
            );
            if (throwAfterCreate) throw new Error("fresh final callback fault");
          },
        },
      });
      await writeFile(
        join(
          fixture.artifactOwnedRoot,
          STOPPED_DIRECTORY_PUBLICATION_LOCK_NAME,
        ),
        "",
        { mode: 0o600 },
      );

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
        await readFile(join(fixture.artifactDirectory, sentinelName), "utf8"),
        "preserve\n",
      );
      const finalIdentity = await lstat(fixture.artifactDirectory, {
        bigint: true,
      });
      assert.equal(
        finalIdentity.dev === candidateIdentity.dev &&
          finalIdentity.ino === candidateIdentity.ino,
        false,
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
        (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record
          .state,
        "materialized",
      );
    });
  }
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

test("pre-rename callback publication remains outcome-uncertain", async (t) => {
  for (const throwAfterRename of [false, true]) {
    await t.test(throwAfterRename ? "callback throws" : "callback returns", async (t) => {
      let afterCopyCalls = 0;
      let candidateIdentity;
      let fixture;
      fixture = await createFixture(t, {
        faults: {
          async afterCopy() {
            afterCopyCalls += 1;
          },
          async beforeRename() {
            const candidate = candidatePath(
              fixture.artifactOwnedRoot,
              CAPTURE_OPERATION_ID,
              fixture.artifactDirectory,
            );
            candidateIdentity = await lstat(candidate, { bigint: true });
            await rename(candidate, fixture.artifactDirectory);
            if (throwAfterRename) throw new Error("pre-rename callback fault");
          },
        },
      });
      const options = captureOptions(fixture);

      await assert.rejects(
        fixture.publication.publishCheckpointArtifact(options),
        (error) =>
          assertPublicationError(
            error,
            "publication_outcome_uncertain",
            "uncertain",
          ),
      );
      assert.equal(
        await pathExists(
          candidatePath(
            fixture.artifactOwnedRoot,
            CAPTURE_OPERATION_ID,
            fixture.artifactDirectory,
          ),
        ),
        false,
      );
      const finalIdentity = await lstat(fixture.artifactDirectory, {
        bigint: true,
      });
      assert.equal(finalIdentity.dev, candidateIdentity.dev);
      assert.equal(finalIdentity.ino, candidateIdentity.ino);
      assert.equal(
        (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record
          .state,
        "materialized",
      );

      const replayed = await fixture.publication.publishCheckpointArtifact(options);
      assert.deepEqual(replayed.result, options.result);
      assert.equal(afterCopyCalls, 1);
      assert.equal(
        (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record
          .state,
        "committed",
      );
    });
  }
});

test("post-candidate-barrier publication remains outcome-uncertain", async (t) => {
  for (const throwAfterRename of [false, true]) {
    await t.test(throwAfterRename ? "callback throws" : "callback returns", async (t) => {
      let afterCopyCalls = 0;
      let candidateIdentity;
      let fixture;
      fixture = await createFixture(t, {
        faults: {
          async afterCandidateBarrier() {
            const candidate = candidatePath(
              fixture.artifactOwnedRoot,
              CAPTURE_OPERATION_ID,
              fixture.artifactDirectory,
            );
            candidateIdentity = await lstat(candidate, { bigint: true });
            await rename(candidate, fixture.artifactDirectory);
            if (throwAfterRename) throw new Error("candidate callback fault");
          },
          async afterCopy() {
            afterCopyCalls += 1;
          },
        },
      });
      const options = captureOptions(fixture);

      await assert.rejects(
        fixture.publication.publishCheckpointArtifact(options),
        (error) =>
          assertPublicationError(
            error,
            "publication_outcome_uncertain",
            "uncertain",
          ),
      );
      assert.equal(
        await pathExists(
          candidatePath(
            fixture.artifactOwnedRoot,
            CAPTURE_OPERATION_ID,
            fixture.artifactDirectory,
          ),
        ),
        false,
      );
      const finalIdentity = await lstat(fixture.artifactDirectory, {
        bigint: true,
      });
      assert.equal(finalIdentity.dev, candidateIdentity.dev);
      assert.equal(finalIdentity.ino, candidateIdentity.ino);
      assert.equal(
        (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record
          .state,
        "prepared",
      );

      await assert.rejects(
        fixture.publication.publishCheckpointArtifact(options),
        (error) =>
          assertPublicationError(
            error,
            "publication_outcome_uncertain",
            "uncertain",
          ),
      );
      assert.equal(afterCopyCalls, 1);
    });
  }
});

test("early candidate callbacks cannot publish with a not-committed result", async (t) => {
  for (const kind of ["checkpoint", "restore"]) {
    for (const callbackName of ["afterCandidateCreated", "afterCopy"]) {
      for (const throwAfterRename of [false, true]) {
        await t.test(
          `${kind} ${callbackName} ${throwAfterRename ? "throws" : "returns"}`,
          async (t) => {
            let armed = kind === "checkpoint";
            let callbackCalls = 0;
            let candidateIdentity;
            let fixture;
            const faults = {
              async [callbackName]() {
                if (!armed) return;
                callbackCalls += 1;
                const operationId =
                  kind === "checkpoint"
                    ? CAPTURE_OPERATION_ID
                    : RESTORE_OPERATION_ID;
                const ownedRoot =
                  kind === "checkpoint"
                    ? fixture.artifactOwnedRoot
                    : fixture.destinationOwnedRoot;
                const finalPath =
                  kind === "checkpoint"
                    ? fixture.artifactDirectory
                    : fixture.destinationDirectory;
                const candidate = candidatePath(
                  ownedRoot,
                  operationId,
                  finalPath,
                );
                candidateIdentity = await lstat(candidate, { bigint: true });
                await rename(candidate, finalPath);
                if (throwAfterRename) throw new Error("pre-pin callback fault");
              },
            };
            fixture = await createFixture(t, { faults });
            if (kind === "restore") {
              await publishFixtureArtifact(fixture);
              armed = true;
            }
            const operationId =
              kind === "checkpoint" ? CAPTURE_OPERATION_ID : RESTORE_OPERATION_ID;
            const ownedRoot =
              kind === "checkpoint"
                ? fixture.artifactOwnedRoot
                : fixture.destinationOwnedRoot;
            const finalPath =
              kind === "checkpoint"
                ? fixture.artifactDirectory
                : fixture.destinationDirectory;
            const options =
              kind === "checkpoint"
                ? captureOptions(fixture)
                : restoreOptions(fixture);
            const publish = () =>
              kind === "checkpoint"
                ? fixture.publication.publishCheckpointArtifact(options)
                : fixture.publication.publishRestoreDestination(options);

            await assert.rejects(
              publish(),
              (error) =>
                assertPublicationError(
                  error,
                  "publication_outcome_uncertain",
                  "uncertain",
                ),
            );
            assert.equal(
              await pathExists(candidatePath(ownedRoot, operationId, finalPath)),
              false,
            );
            const finalIdentity = await lstat(finalPath, { bigint: true });
            assert.equal(finalIdentity.dev, candidateIdentity.dev);
            assert.equal(finalIdentity.ino, candidateIdentity.ino);
            assert.equal(callbackCalls, 1);
            assert.equal(
              (await fixture.journal.read({ operationId })).record.state,
              "prepared",
            );
            await assert.rejects(
              publish(),
              (error) =>
                assertPublicationError(
                  error,
                  "publication_outcome_uncertain",
                  "uncertain",
                ),
            );
            assert.equal(callbackCalls, 1);
          },
        );
      }
    }
  }
});

test("materialization callbacks cannot publish with a not-committed result", async (t) => {
  for (const callbackKind of ["journal", "publication"]) {
    for (const throwAfterRename of [false, true]) {
      await t.test(
        `${callbackKind} callback ${throwAfterRename ? "throws" : "returns"}`,
        async (t) => {
          let afterCopyCalls = 0;
          let armed = false;
          let candidateIdentity;
          let fixture;
          const publishCandidate = async () => {
            const candidate = candidatePath(
              fixture.artifactOwnedRoot,
              CAPTURE_OPERATION_ID,
              fixture.artifactDirectory,
            );
            candidateIdentity = await lstat(candidate, { bigint: true });
            await rename(candidate, fixture.artifactDirectory);
            if (throwAfterRename) throw new Error("materialization callback fault");
          };
          const journalFaults =
            callbackKind === "journal"
              ? {
                  async beforeRename({ record }) {
                    if (armed && record.state === "materialized") {
                      await publishCandidate();
                    }
                  },
                }
              : undefined;
          fixture = await createFixture(t, {
            faults: {
              async afterCandidateBarrier() {
                armed = true;
              },
              async afterCopy() {
                afterCopyCalls += 1;
              },
              async afterMaterialized() {
                if (callbackKind === "publication") await publishCandidate();
              },
            },
            journalFaults,
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
            await pathExists(
              candidatePath(
                fixture.artifactOwnedRoot,
                CAPTURE_OPERATION_ID,
                fixture.artifactDirectory,
              ),
            ),
            false,
          );
          const finalIdentity = await lstat(fixture.artifactDirectory, {
            bigint: true,
          });
          assert.equal(finalIdentity.dev, candidateIdentity.dev);
          assert.equal(finalIdentity.ino, candidateIdentity.ino);
          assert.equal(afterCopyCalls, 1);
          const journalState =
            callbackKind === "journal" && throwAfterRename
              ? JSON.parse(
                  await readFile(
                    join(
                      fixture.journalDirectory,
                      operationJournalRecordFilename(CAPTURE_OPERATION_ID),
                    ),
                    "utf8",
                  ),
                ).state
              : (
                  await fixture.journal.read({
                    operationId: CAPTURE_OPERATION_ID,
                  })
                ).record.state;
          assert.equal(
            journalState,
            callbackKind === "journal" && throwAfterRename
              ? "prepared"
              : "materialized",
          );
        },
      );
    }
  }
});

test("candidate mutation after its barrier cannot advance to materialized", async (t) => {
  let fixture;
  fixture = await createFixture(t, {
    faults: {
      async afterCandidateBarrier() {
        const candidate = candidatePath(
          fixture.artifactOwnedRoot,
          CAPTURE_OPERATION_ID,
          fixture.artifactDirectory,
        );
        await writeFile(
          join(candidate, "payload", "workspace", "README.md"),
          "mutated after candidate barrier\n",
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
        "publication_integrity_failed",
        "not-committed",
      ),
  );
  assert.equal(
    (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record.state,
    "prepared",
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
  assert.equal(await pathExists(fixture.artifactDirectory), false);
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
    await realpath(fixture.artifactOwnedRoot),
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

test("fresh checkpoint publication rejects every pre-existing journal phase", async (t) => {
  for (const phase of ["prepared", "materialized", "committed"]) {
    await t.test(phase, async (t) => {
      let seedFaultEnabled = phase !== "committed";
      let afterCopyCalls = 0;
      const fixture = await createFixture(t, {
        faults: {
          async afterCopy() {
            afterCopyCalls += 1;
          },
          async afterJournalPrepared() {
            if (phase === "prepared" && seedFaultEnabled) {
              throw new Error("prepared seed fault");
            }
          },
          async afterMaterialized() {
            if (phase === "materialized" && seedFaultEnabled) {
              throw new Error("materialized seed fault");
            }
          },
        },
      });
      const options = captureOptions(fixture);

      if (phase === "committed") {
        await fixture.publication.publishCheckpointArtifact(options);
      } else {
        await assert.rejects(
          fixture.publication.publishCheckpointArtifact(options),
          (error) =>
            assertPublicationError(
              error,
              "publication_io_failed",
              "not-committed",
            ),
        );
      }
      seedFaultEnabled = false;

      const before = await fixture.journal.read({
        operationId: CAPTURE_OPERATION_ID,
      });
      assert.equal(before.record.state, phase);
      const candidate = candidatePath(
        fixture.artifactOwnedRoot,
        CAPTURE_OPERATION_ID,
        fixture.artifactDirectory,
      );
      const durablePath =
        phase === "materialized"
          ? candidate
          : phase === "committed"
            ? fixture.artifactDirectory
            : null;
      const durableIdentity =
        durablePath === null ? null : await lstat(durablePath, { bigint: true });
      const copyCallsBeforeFreshAttempt = afterCopyCalls;

      await assert.rejects(
        fixture.publication.publishFreshCheckpointArtifact(options),
        (error) =>
          assertPublicationError(
            error,
            "publication_conflict",
            phase === "committed" ? "committed" : "not-committed",
          ),
      );

      assert.deepEqual(
        (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record,
        before.record,
      );
      assert.equal(afterCopyCalls, copyCallsBeforeFreshAttempt);
      if (durablePath === null) {
        assert.equal(await pathExists(candidate), false);
        assert.equal(await pathExists(fixture.artifactDirectory), false);
      } else {
        const after = await lstat(durablePath, { bigint: true });
        assert.equal(after.dev, durableIdentity.dev);
        assert.equal(after.ino, durableIdentity.ino);
        assert.equal(after.birthtimeNs, durableIdentity.birthtimeNs);
      }

      const replay = await fixture.publication.publishCheckpointArtifact(options);
      assert.equal(replay.replayed, phase === "committed");
      assert.equal(afterCopyCalls, 1);
      assert.equal(
        (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record
          .state,
        "committed",
      );
    });
  }
});

test("fresh checkpoint publication closes the authoritative read-to-prepare race", async (t) => {
  let armCompetingPrepare = false;
  let competingInput;
  let competingPreparePromise;
  class ReadToPrepareRaceJournal extends FilesystemOperationJournal {
    async read(options) {
      const observed = await super.read(options);
      if (!armCompetingPrepare || observed.record !== null) return observed;
      armCompetingPrepare = false;
      competingPreparePromise = competingInput.journal.prepare(
        competingInput.options,
      );
      await competingPreparePromise;
      return observed;
    }
  }
  const fixture = await createFixture(t, {
    JournalClass: ReadToPrepareRaceJournal,
  });
  const options = captureOptions(fixture);
  const competingJournal = new FilesystemOperationJournal({
    directory: fixture.journalDirectory,
    acquireLock: simpleLockProvider(),
    ...TRUSTED_JOURNAL_ACL_INSPECTORS,
  });
  competingInput = {
    journal: competingJournal,
    options: {
      binding: { competingStarter: true },
      operationId: CAPTURE_OPERATION_ID,
      request: options.request,
      result: options.result,
    },
  };
  armCompetingPrepare = true;

  await assert.rejects(
    fixture.publication.publishFreshCheckpointArtifact(options),
    (error) =>
      assertPublicationError(
        error,
        "publication_conflict",
        "not-committed",
      ),
  );

  assert.notEqual(competingPreparePromise, undefined);
  await competingPreparePromise;
  assert.equal(
    (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record
      .state,
    "prepared",
  );
  assert.equal(
    await pathExists(
      candidatePath(
        fixture.artifactOwnedRoot,
        CAPTURE_OPERATION_ID,
        fixture.artifactDirectory,
      ),
    ),
    false,
  );
  assert.equal(await pathExists(fixture.artifactDirectory), false);
});

test("a non-private materialized candidate cannot be renamed", async (t) => {
  let failAfterMaterialized = true;
  const fixture = await createFixture(t, {
    faults: {
      async afterMaterialized() {
        if (failAfterMaterialized) throw new Error("materialized fault");
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
  await chmod(candidate, 0o755);

  failAfterMaterialized = false;
  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(options),
    (error) =>
      assertPublicationError(
        error,
        "publication_recovery_required",
        "not-committed",
      ),
  );
  assert.equal(await pathExists(candidate), true);
  assert.equal(await pathExists(fixture.artifactDirectory), false);
  assert.equal(
    (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record.state,
    "materialized",
  );
});

test("a materialized candidate overlapping the source requires recovery", async (t) => {
  let failAfterMaterialized = true;
  const fixture = await createFixture(t, {
    faults: {
      async afterMaterialized() {
        if (failAfterMaterialized) throw new Error("materialized fault");
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
  await link(
    join(candidate, "payload", "workspace", "README.md"),
    join(fixture.sourceDirectory, "workspace", "candidate-alias"),
  );

  failAfterMaterialized = false;
  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(options),
    (error) =>
      assertPublicationError(
        error,
        "publication_recovery_required",
        "not-committed",
      ),
  );
  assert.equal(await pathExists(candidate), true);
  assert.equal(await pathExists(fixture.artifactDirectory), false);
  assert.equal(
    (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record.state,
    "materialized",
  );
});

test("materialized topology stays uncertain until candidate-only is proven", async (t) => {
  let failAfterMaterialized = true;
  const fixture = await createFixture(t, {
    faults: {
      async afterMaterialized() {
        if (failAfterMaterialized) throw new Error("materialized fault");
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
  await rm(candidate, { force: true, recursive: true });
  assert.equal(await pathExists(candidate), false);
  assert.equal(await pathExists(fixture.artifactDirectory), false);

  failAfterMaterialized = false;
  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(options),
    (error) =>
      assertPublicationError(error, "publication_outcome_uncertain", "uncertain"),
  );
  assert.equal(
    (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record.state,
    "materialized",
  );
});

test("missing materialized paths stay uncertain after a prior rename", async (t) => {
  let failAfterRename = true;
  const fixture = await createFixture(t, {
    faults: {
      async afterRename() {
        if (failAfterRename) throw new Error("rename fault");
      },
    },
  });
  const options = captureOptions(fixture);
  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(options),
    (error) =>
      assertPublicationError(
        error,
        "publication_outcome_uncertain",
        "uncertain",
      ),
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

  await rm(fixture.artifactDirectory, { force: true, recursive: true });
  assert.equal(await pathExists(candidate), false);
  assert.equal(await pathExists(fixture.artifactDirectory), false);
  failAfterRename = false;

  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(options),
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

test("materialized topology probe errors remain publication-uncertain", async (t) => {
  let failAfterMaterialized = true;
  const fixture = await createFixture(t, {
    faults: {
      async afterMaterialized() {
        if (failAfterMaterialized) throw new Error("materialized fault");
      },
    },
  });
  const options = captureOptions(fixture);
  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(options),
    (error) =>
      assertPublicationError(error, "publication_io_failed", "not-committed"),
  );
  failAfterMaterialized = false;

  class TargetBlockingJournal extends FilesystemOperationJournal {
    async read(readOptions) {
      const outcome = await super.read(readOptions);
      if (outcome.record?.state === "materialized") {
        await chmod(fixture.artifactOwnedRoot, 0o600);
      }
      return outcome;
    }
  }
  const blockingJournal = new TargetBlockingJournal({
    directory: fixture.journalDirectory,
    acquireLock: simpleLockProvider(),
    ...TRUSTED_JOURNAL_ACL_INSPECTORS,
  });
  const publication = new StoppedDirectoryPublication({
    journal: blockingJournal,
    acquireLock: simpleLockProvider(),
    inspectFilesystem: async () => ({
      durability: "local-fsync-rename",
      filesystemId: "test-filesystem-001",
      objectIdentityScheme: TEST_OBJECT_IDENTITY_SCHEME,
      type: "test-local",
    }),
    inspectPersistentObjectIdentity: inspectTestPersistentObjectIdentity,
    inspectOwnedRootAcl: async () => false,
    inspectOwnedRootAncestorAcl: async () => false,
    ...TRUSTED_PUBLICATION_MOUNT_INSPECTOR,
  });

  try {
    await assert.rejects(
      publication.publishCheckpointArtifact(options),
      (error) =>
        assertPublicationError(
          error,
          "publication_outcome_uncertain",
          "uncertain",
        ),
    );
  } finally {
    await chmod(fixture.artifactOwnedRoot, 0o700);
  }
  assert.equal(
    (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record.state,
    "materialized",
  );
});

test("materialized replay rejects same-byte retained-tree object replacement", async (t) => {
  let failAfterMaterialized = true;
  const fixture = await createFixture(t, {
    faults: {
      async afterMaterialized() {
        if (failAfterMaterialized) throw new Error("materialized fault");
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
  const retainedFile = join(candidate, "payload", "workspace", "README.md");
  await rm(retainedFile);
  await writeFile(retainedFile, "portable\n", { mode: 0o640 });
  await rm(fixture.sourceDirectory, { force: true, recursive: true });

  failAfterMaterialized = false;
  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(options),
    (error) =>
      assertPublicationError(
        error,
        "publication_recovery_required",
        "not-committed",
      ),
  );
  assert.equal(await pathExists(candidate), true);
  assert.equal(await pathExists(fixture.artifactDirectory), false);
  assert.equal(
    (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record.state,
    "materialized",
  );
});

test("materialized replay rejects a reused retained-file inode generation", async (t) => {
  let failAfterMaterialized = true;
  let fixture;
  let retainedFile;
  let retainedGeneration = "generation-a";
  fixture = await createFixture(t, {
    faults: {
      async afterMaterialized() {
        if (failAfterMaterialized) throw new Error("materialized fault");
      },
    },
    inspectPersistentObjectIdentity: async (path) =>
      path === retainedFile
        ? inspectTestPersistentObjectIdentityAs(
            path,
            `retained-file-${retainedGeneration}`,
          )
        : inspectTestPersistentObjectIdentity(path),
  });
  const options = captureOptions(fixture);
  const candidate = candidatePath(
    await realpath(fixture.artifactOwnedRoot),
    CAPTURE_OPERATION_ID,
    fixture.artifactDirectory,
  );
  retainedFile = join(candidate, "payload", "workspace", "README.md");
  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(options),
    (error) =>
      assertPublicationError(error, "publication_io_failed", "not-committed"),
  );
  const runtimeIdentity = await lstat(retainedFile, { bigint: true });

  retainedGeneration = "generation-b";
  failAfterMaterialized = false;
  await rm(fixture.sourceDirectory, { force: true, recursive: true });
  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(options),
    (error) =>
      assertPublicationError(
        error,
        "publication_recovery_required",
        "not-committed",
      ),
  );

  assert.equal((await lstat(retainedFile, { bigint: true })).ino, runtimeIdentity.ino);
  assert.equal(await pathExists(candidate), true);
  assert.equal(await pathExists(fixture.artifactDirectory), false);
  assert.equal(
    (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record.state,
    "materialized",
  );
});

test("a materialized final-only operation resumes after post-rename uncertainty", async (t) => {
  let failAfterParentSync = true;
  let afterCopyCalls = 0;
  const inspectedFilesystems = [];
  const fixture = await createFixture(t, {
    faults: {
      async afterCopy() {
        afterCopyCalls += 1;
      },
      async afterParentSync() {
        if (failAfterParentSync) throw new Error("sensitive final-only fault");
      },
    },
    inspectFilesystem: async (path) => {
      inspectedFilesystems.push(path);
      return {
        durability: "local-fsync-rename",
        type: "test-local",
      };
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

  await rm(fixture.sourceDirectory, { force: true, recursive: true });
  const symlinkTarget = join(fixture.root, "unrelated-materialized-source-target");
  await mkdir(symlinkTarget, { mode: 0o700 });
  await symlink(symlinkTarget, fixture.sourceDirectory);
  inspectedFilesystems.length = 0;
  failAfterParentSync = false;
  const result = await fixture.publication.publishCheckpointArtifact(options);

  assert.deepEqual(result.result, options.result);
  assert.equal(afterCopyCalls, 1);
  assert.equal(inspectedFilesystems.includes(fixture.sourceDirectory), false);
  assert.equal(inspectedFilesystems.includes(symlinkTarget), false);
  assert.equal(
    (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record.state,
    "committed",
  );
});

test("a rebound destination root cannot downgrade final-only recovery", async (t) => {
  let failAfterParentSync = true;
  const fixture = await createFixture(t, {
    faults: {
      async afterParentSync() {
        if (failAfterParentSync) throw new Error("final-only fault");
      },
    },
  });
  const options = captureOptions(fixture);
  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(options),
    (error) =>
      assertPublicationError(error, "publication_outcome_uncertain", "uncertain"),
  );

  const displacedRoot = join(fixture.root, "displaced-artifact-root");
  await rename(fixture.artifactOwnedRoot, displacedRoot);
  const durableFinal = join(displacedRoot, basename(fixture.artifactDirectory));
  await mkdir(fixture.artifactOwnedRoot, { mode: 0o700 });
  const reboundCandidate = candidatePath(
    fixture.artifactOwnedRoot,
    CAPTURE_OPERATION_ID,
    fixture.artifactDirectory,
  );
  await mkdir(reboundCandidate, { mode: 0o700 });
  failAfterParentSync = false;

  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(options),
    (error) =>
      assertPublicationError(
        error,
        "publication_outcome_uncertain",
        "uncertain",
      ),
  );

  assert.equal(await pathExists(durableFinal), true);
  assert.equal(await pathExists(reboundCandidate), true);
  assert.equal(await pathExists(fixture.artifactDirectory), false);
  assert.equal(
    (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record.state,
    "materialized",
  );
});

test("a non-private materialized final remains publication-uncertain", async (t) => {
  let failAfterParentSync = true;
  const fixture = await createFixture(t, {
    faults: {
      async afterParentSync() {
        if (failAfterParentSync) throw new Error("final-only fault");
      },
    },
  });
  const options = captureOptions(fixture);
  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(options),
    (error) =>
      assertPublicationError(error, "publication_outcome_uncertain", "uncertain"),
  );
  await chmod(fixture.artifactDirectory, 0o755);

  failAfterParentSync = false;
  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(options),
    (error) =>
      assertPublicationError(error, "publication_outcome_uncertain", "uncertain"),
  );
  assert.equal(
    (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record.state,
    "materialized",
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

test("a final beside a prepared record is publication-uncertain", async (t) => {
  let failAfterJournalPrepared = true;
  const fixture = await createFixture(t, {
    faults: {
      async afterJournalPrepared() {
        if (failAfterJournalPrepared) throw new Error("prepared fault");
      },
    },
  });
  const options = captureOptions(fixture);
  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(options),
    (error) =>
      assertPublicationError(error, "publication_io_failed", "not-committed"),
  );
  await mkdir(fixture.artifactDirectory, { mode: 0o700 });

  failAfterJournalPrepared = false;
  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(options),
    (error) =>
      assertPublicationError(
        error,
        "publication_outcome_uncertain",
        "uncertain",
      ),
  );
  assert.equal(
    (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record.state,
    "prepared",
  );
  assert.equal(await pathExists(fixture.artifactDirectory), true);
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

test("journal commit callbacks cannot bypass final publication readback", async (t) => {
  for (const hook of ["beforeRename", "beforeLockRelease"]) {
    await t.test(hook, async (t) => {
      let armed = false;
      let fixture;
      const journalFaults = {
        async [hook]({ record } = {}) {
          if (!armed || (record !== undefined && record.state !== "committed")) {
            return;
          }
          await writeFile(
            join(fixture.artifactDirectory, "payload", "workspace", "README.md"),
            "journal callback mutation\n",
            { mode: 0o640 },
          );
        },
      };
      fixture = await createFixture(t, {
        faults: {
          async beforeCommit() {
            armed = true;
          },
        },
        journalFaults,
      });
      const options = captureOptions(fixture);

      await assert.rejects(
        fixture.publication.publishCheckpointArtifact(options),
        (error) =>
          assertPublicationError(
            error,
            "published_state_invalid",
            "committed",
          ),
      );
      armed = false;
      assert.equal(
        (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record
          .state,
        "committed",
      );
      await assert.rejects(
        fixture.publication.publishCheckpointArtifact(options),
        (error) =>
          assertPublicationError(
            error,
            "published_state_invalid",
            "committed",
          ),
      );
    });
  }
});

test("same-byte callback rewrites pass through the final durability barriers", async (t) => {
  let fixture;
  fixture = await createFixture(t, {
    faults: {
      async beforeRename() {
        const candidate = candidatePath(
          fixture.artifactOwnedRoot,
          CAPTURE_OPERATION_ID,
          fixture.artifactDirectory,
        );
        await writeFile(
          join(candidate, "payload", "workspace", "README.md"),
          "portable\n",
          { mode: 0o640 },
        );
      },
      async beforeCommit() {
        await writeFile(
          join(fixture.artifactDirectory, "payload", "workspace", "README.md"),
          "portable\n",
          { mode: 0o640 },
        );
      },
    },
  });

  const outcome = await fixture.publication.publishCheckpointArtifact(
    captureOptions(fixture),
  );
  assert.equal(outcome.result.mutation.status, "checkpoint-created");
  assert.equal(
    (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record.state,
    "committed",
  );
});

test("lock assertions run before the final candidate durability barrier", async (t) => {
  let fixture;
  let assertions = 0;
  let mutateOnNextAssertion = false;
  let rewroteCandidate = false;
  const baseLockProvider = simpleLockProvider();
  fixture = await createFixture(t, {
    acquireLock: async (...args) => {
      const lock = await baseLockProvider(...args);
      return {
        ...lock,
        async assertHeld() {
          assertions += 1;
          if (!mutateOnNextAssertion || rewroteCandidate) return;
          const candidate = candidatePath(
            fixture.artifactOwnedRoot,
            CAPTURE_OPERATION_ID,
            fixture.artifactDirectory,
          );
          await writeFile(
            join(candidate, "payload", "workspace", "README.md"),
            "portable\n",
            { mode: 0o640 },
          );
          rewroteCandidate = true;
        },
      };
    },
    faults: {
      async beforeRename() {
        mutateOnNextAssertion = true;
      },
    },
  });

  const outcome = await fixture.publication.publishCheckpointArtifact(
    captureOptions(fixture),
  );
  assert.equal(outcome.result.mutation.status, "checkpoint-created");
  assert(assertions > 0);
  assert.equal(rewroteCandidate, true);
  assert.equal(
    (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record.state,
    "committed",
  );
});

test("lock loss in the pre-rename callback is publication-uncertain", async (t) => {
  let lockHeld = true;
  let renameCalls = 0;
  const fixture = await createFixture(t, {
    acquireLock: async () => ({
      async assertHeld() {
        if (!lockHeld) throw new Error("lock lost");
      },
      async release() {},
      async renameWhileHeld() {
        renameCalls += 1;
      },
    }),
    faults: {
      async beforeRename() {
        lockHeld = false;
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
  assert.equal(renameCalls, 0);
  assert.equal(await pathExists(fixture.artifactDirectory), false);
  assert.equal(
    (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record.state,
    "materialized",
  );
});

test("the publication lock covers state hints and rejects a state regression", async (t) => {
  const fixture = await createFixture(t);
  let lockActive = false;
  let acquisitionOptions;
  class RegressingJournal extends FilesystemOperationJournal {
    async readStateHint() {
      assert.equal(lockActive, true);
      return Object.freeze({
        record: Object.freeze({ state: "committed" }),
        replayed: false,
      });
    }
  }
  const journal = new RegressingJournal({
    directory: fixture.journalDirectory,
    acquireLock: simpleLockProvider(),
    ...TRUSTED_JOURNAL_ACL_INSPECTORS,
  });
  const baseLockProvider = simpleLockProvider();
  const publication = new StoppedDirectoryPublication({
    journal,
    acquireLock: async (path, options) => {
      acquisitionOptions = options;
      const acquired = await baseLockProvider(path, options);
      lockActive = true;
      return {
        ...acquired,
        async release() {
          try {
            await acquired.release();
          } finally {
            lockActive = false;
          }
        },
      };
    },
    inspectFilesystem: async () => ({
      durability: "local-fsync-rename",
      filesystemId: "test-filesystem-001",
      objectIdentityScheme: TEST_OBJECT_IDENTITY_SCHEME,
      type: "test-local",
    }),
    inspectPersistentObjectIdentity: inspectTestPersistentObjectIdentity,
    ...TRUSTED_PUBLICATION_ACL_INSPECTORS,
    ...TRUSTED_PUBLICATION_MOUNT_INSPECTOR,
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
  assert.deepEqual(acquisitionOptions, { requireExisting: true });
  assert.equal(lockActive, false);
  assert.equal(await pathExists(fixture.artifactDirectory), false);
  assert.equal(
    (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record,
    null,
  );
});

test("publication lock loss during state planning stops before authoritative journal read", async (t) => {
  for (const lossPhase of ["hint", "source preflight"]) {
    await t.test(lossPhase, async (t) => {
      const fixture = await createFixture(t);
      let authoritativeReads = 0;
      let hintReturned = false;
      let lockHeld = false;
      let lostLock = false;
      class LockLossJournal extends FilesystemOperationJournal {
        async readStateHint(options) {
          const outcome = await super.readStateHint(options);
          hintReturned = true;
          if (lossPhase === "hint") {
            lockHeld = false;
            lostLock = true;
          }
          return outcome;
        }

        async read(options) {
          authoritativeReads += 1;
          return super.read(options);
        }
      }
      const journal = new LockLossJournal({
        directory: fixture.journalDirectory,
        acquireLock: simpleLockProvider(),
        ...TRUSTED_JOURNAL_ACL_INSPECTORS,
      });
      const publication = new StoppedDirectoryPublication({
        journal,
        acquireLock: async () => {
          lockHeld = true;
          return {
            async assertHeld() {
              if (!lockHeld) throw new Error("publication lock lost");
            },
            async release() {},
            async renameWhileHeld() {
              throw new Error("rename must not be reached");
            },
          };
        },
        inspectFilesystem: async () => ({
          durability: "local-fsync-rename",
          filesystemId: "test-filesystem-001",
          objectIdentityScheme: TEST_OBJECT_IDENTITY_SCHEME,
          type: "test-local",
        }),
        inspectPersistentObjectIdentity: inspectTestPersistentObjectIdentity,
        listMountPoints: async () => {
          if (
            lossPhase === "source preflight" &&
            hintReturned &&
            !lostLock
          ) {
            lockHeld = false;
            lostLock = true;
          }
          return ["/"];
        },
        ...TRUSTED_PUBLICATION_ACL_INSPECTORS,
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
      assert.equal(lostLock, true);
      assert.equal(authoritativeReads, 0);
      assert.equal(await pathExists(fixture.artifactDirectory), false);
      assert.equal(
        (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record,
        null,
      );
    });
  }
});

test("a journal read failure after a state hint remains publication-uncertain", async (t) => {
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
      filesystemId: "test-filesystem-001",
      objectIdentityScheme: TEST_OBJECT_IDENTITY_SCHEME,
      type: "test-local",
    }),
    inspectPersistentObjectIdentity: inspectTestPersistentObjectIdentity,
    ...TRUSTED_PUBLICATION_ACL_INSPECTORS,
    ...TRUSTED_PUBLICATION_MOUNT_INSPECTOR,
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

test("a returning journal read callback that creates a fresh final is publication-uncertain", async (t) => {
  const fixture = await createFixture(t);
  const options = captureOptions(fixture);
  let createdFinal = false;
  const publishingJournal = new FilesystemOperationJournal({
    directory: fixture.journalDirectory,
    acquireLock: simpleLockProvider(),
    faults: {
      async afterRecordRead() {
        if (createdFinal) return;
        createdFinal = true;
        await mkdir(fixture.artifactDirectory, { mode: 0o700 });
      },
    },
    ...TRUSTED_JOURNAL_ACL_INSPECTORS,
  });
  const publication = new StoppedDirectoryPublication({
    journal: publishingJournal,
    acquireLock: simpleLockProvider(),
    inspectFilesystem: async () => ({
      durability: "local-fsync-rename",
      filesystemId: "test-filesystem-001",
      objectIdentityScheme: TEST_OBJECT_IDENTITY_SCHEME,
      type: "test-local",
    }),
    inspectPersistentObjectIdentity: inspectTestPersistentObjectIdentity,
    ...TRUSTED_PUBLICATION_ACL_INSPECTORS,
    ...TRUSTED_PUBLICATION_MOUNT_INSPECTOR,
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
  assert.equal(createdFinal, true);
  assert.equal(await pathExists(fixture.artifactDirectory), true);
  assert.equal(
    (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record,
    null,
  );
});

test("a returning journal read callback that publishes a prepared candidate is publication-uncertain", async (t) => {
  let failAfterJournalPrepared = true;
  const fixture = await createFixture(t, {
    faults: {
      async afterJournalPrepared() {
        if (failAfterJournalPrepared) throw new Error("retain prepared record");
      },
    },
  });
  const options = captureOptions(fixture);
  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(options),
    (error) =>
      assertPublicationError(error, "publication_io_failed", "not-committed"),
  );
  failAfterJournalPrepared = false;
  const candidate = candidatePath(
    await realpath(fixture.artifactOwnedRoot),
    CAPTURE_OPERATION_ID,
    fixture.artifactDirectory,
  );
  await mkdir(candidate, { mode: 0o700 });
  let publishedCandidate = false;
  const publishingJournal = new FilesystemOperationJournal({
    directory: fixture.journalDirectory,
    acquireLock: simpleLockProvider(),
    faults: {
      async afterRecordRead() {
        if (publishedCandidate) return;
        publishedCandidate = true;
        await rename(candidate, fixture.artifactDirectory);
      },
    },
    ...TRUSTED_JOURNAL_ACL_INSPECTORS,
  });
  const publication = new StoppedDirectoryPublication({
    journal: publishingJournal,
    acquireLock: simpleLockProvider(),
    inspectFilesystem: async () => ({
      durability: "local-fsync-rename",
      filesystemId: "test-filesystem-001",
      objectIdentityScheme: TEST_OBJECT_IDENTITY_SCHEME,
      type: "test-local",
    }),
    inspectPersistentObjectIdentity: inspectTestPersistentObjectIdentity,
    ...TRUSTED_PUBLICATION_ACL_INSPECTORS,
    ...TRUSTED_PUBLICATION_MOUNT_INSPECTOR,
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
  assert.equal(publishedCandidate, true);
  assert.equal(await pathExists(candidate), false);
  assert.equal(await pathExists(fixture.artifactDirectory), true);
  assert.equal(
    (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record.state,
    "prepared",
  );
});

test("a prepared source replacement stays uncertain when a final is already visible", async (t) => {
  const fixture = await createFixture(t, {
    faults: {
      async afterJournalPrepared() {
        throw new Error("retain prepared record");
      },
    },
  });
  const options = captureOptions(fixture);
  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(options),
    (error) =>
      assertPublicationError(error, "publication_io_failed", "not-committed"),
  );
  await mkdir(fixture.artifactDirectory, { mode: 0o700 });
  const displacedSource = join(fixture.root, "source-before-journal-read");
  let replacedSource = false;
  const replacingJournal = new FilesystemOperationJournal({
    directory: fixture.journalDirectory,
    acquireLock: simpleLockProvider(),
    faults: {
      async afterRecordRead() {
        if (replacedSource) return;
        replacedSource = true;
        await rename(fixture.sourceDirectory, displacedSource);
        await mkdir(fixture.sourceDirectory, { mode: 0o700 });
      },
    },
    ...TRUSTED_JOURNAL_ACL_INSPECTORS,
  });
  const publication = new StoppedDirectoryPublication({
    journal: replacingJournal,
    acquireLock: simpleLockProvider(),
    inspectFilesystem: async () => ({
      durability: "local-fsync-rename",
      filesystemId: "test-filesystem-001",
      objectIdentityScheme: TEST_OBJECT_IDENTITY_SCHEME,
      type: "test-local",
    }),
    inspectPersistentObjectIdentity: inspectTestPersistentObjectIdentity,
    ...TRUSTED_PUBLICATION_ACL_INSPECTORS,
    ...TRUSTED_PUBLICATION_MOUNT_INSPECTOR,
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
  assert.equal(replacedSource, true);
  assert.equal(await pathExists(displacedSource), true);
  assert.equal(await pathExists(fixture.artifactDirectory), true);
  assert.equal(
    (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record.state,
    "prepared",
  );
});

test("journal prepare callbacks cannot publish after recovery was downgraded", async (t) => {
  const fixture = await createFixture(t, {
    faults: {
      async afterMaterialized() {
        throw new Error("retain materialized candidate");
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
    await realpath(fixture.artifactOwnedRoot),
    CAPTURE_OPERATION_ID,
    fixture.artifactDirectory,
  );
  let recordReads = 0;
  const publishingJournal = new FilesystemOperationJournal({
    directory: fixture.journalDirectory,
    acquireLock: simpleLockProvider(),
    faults: {
      async afterRecordRead() {
        recordReads += 1;
        if (recordReads === 2) {
          await rename(candidate, fixture.artifactDirectory);
          throw new Error("journal callback published candidate");
        }
      },
    },
    ...TRUSTED_JOURNAL_ACL_INSPECTORS,
  });
  const publication = new StoppedDirectoryPublication({
    journal: publishingJournal,
    acquireLock: simpleLockProvider(),
    inspectFilesystem: async () => ({
      durability: "local-fsync-rename",
      filesystemId: "test-filesystem-001",
      objectIdentityScheme: TEST_OBJECT_IDENTITY_SCHEME,
      type: "test-local",
    }),
    inspectPersistentObjectIdentity: inspectTestPersistentObjectIdentity,
    ...TRUSTED_PUBLICATION_ACL_INSPECTORS,
    ...TRUSTED_PUBLICATION_MOUNT_INSPECTOR,
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
  assert.equal(recordReads, 2);
  assert.equal(await pathExists(candidate), false);
  assert.equal(await pathExists(fixture.artifactDirectory), true);
  assert.equal(
    (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record.state,
    "materialized",
  );
});

test("restore rejects extra checkpoint bundle-root entries", async (t) => {
  const fixture = await createFixture(t);
  await publishFixtureArtifact(fixture);
  await writeFile(join(fixture.artifactDirectory, "extra"), "unexpected\n", {
    mode: 0o600,
  });

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
  assert.equal(
    (await fixture.journal.read({ operationId: RESTORE_OPERATION_ID })).record.state,
    "prepared",
  );
});

test("restore rechecks bundle-root shape after the source barrier", async (t) => {
  let mutateRestoreSource = false;
  let fixture;
  fixture = await createFixture(t, {
    faults: {
      async afterSourceBarrier() {
        if (mutateRestoreSource) {
          await writeFile(
            join(fixture.artifactDirectory, "extra-after-barrier"),
            "unexpected\n",
            { mode: 0o600 },
          );
        }
      },
    },
  });
  await publishFixtureArtifact(fixture);
  mutateRestoreSource = true;

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
  assert.equal(
    await pathExists(
      candidatePath(
        fixture.destinationOwnedRoot,
        RESTORE_OPERATION_ID,
        fixture.destinationDirectory,
      ),
    ),
    false,
  );
  assert.equal(
    (await fixture.journal.read({ operationId: RESTORE_OPERATION_ID })).record.state,
    "prepared",
  );
});

test("restore rechecks bundle-root shape after candidate copy", async (t) => {
  let mutateRestoreSource = false;
  let fixture;
  fixture = await createFixture(t, {
    faults: {
      async afterCopy() {
        if (mutateRestoreSource) {
          await writeFile(
            join(fixture.artifactDirectory, "extra-after-copy"),
            "unexpected\n",
            { mode: 0o600 },
          );
        }
      },
    },
  });
  await publishFixtureArtifact(fixture);
  mutateRestoreSource = true;

  await assert.rejects(
    fixture.publication.publishRestoreDestination(restoreOptions(fixture)),
    (error) =>
      assertPublicationError(
        error,
        "publication_integrity_failed",
        "not-committed",
      ),
  );
  const candidate = candidatePath(
    fixture.destinationOwnedRoot,
    RESTORE_OPERATION_ID,
    fixture.destinationDirectory,
  );
  assert.equal(await pathExists(candidate), true);
  assert.equal(await pathExists(fixture.destinationDirectory), false);
  assert.equal(
    (await fixture.journal.read({ operationId: RESTORE_OPERATION_ID })).record.state,
    "prepared",
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
  const modeledDigest = await digestTree(candidate);
  const mismatchedManifestDigest =
    fixture.artifactProof.artifactManifestDigest === "f".repeat(64)
      ? "e".repeat(64)
      : "f".repeat(64);
  await fixture.journal.markMaterialized({
    binding: prepared.binding,
    materialization: {
      contractVersion: 2,
      artifactManifestDigest: mismatchedManifestDigest,
      modeledDigest,
      publicationId: publicationId(
        RESTORE_OPERATION_ID,
        fixture.destinationDirectory,
      ),
      publicationKind: "restore-destination",
      stagedRoot: {
        filesystemId: "test-filesystem-001",
        objectIdentityScheme: TEST_OBJECT_IDENTITY_SCHEME,
        objectId: (await inspectTestPersistentObjectIdentity(candidate)).objectId,
      },
      treeIdentityDigest: await digestStoppedTreeIdentities(
        candidate,
        "test-filesystem-001",
        TEST_OBJECT_IDENTITY_SCHEME,
        inspectTestPersistentObjectIdentity,
      ),
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

test("a prepared operation cannot be downgraded through a rebound final path", async (t) => {
  let failAfterCandidateBarrier = true;
  const fixture = await createFixture(t, {
    faults: {
      async afterCandidateBarrier() {
        if (failAfterCandidateBarrier) throw new Error("sensitive prepared fault");
      },
    },
  });
  const first = captureOptions(fixture);
  await assert.rejects(
    fixture.publication.publishCheckpointArtifact(first),
    (error) =>
      assertPublicationError(error, "publication_io_failed", "not-committed"),
  );
  const durableCandidate = candidatePath(
    await realpath(fixture.artifactOwnedRoot),
    CAPTURE_OPERATION_ID,
    fixture.artifactDirectory,
  );
  await rename(durableCandidate, fixture.artifactDirectory);
  failAfterCandidateBarrier = false;
  const differentArtifactDirectory = join(
    fixture.artifactOwnedRoot,
    "different-artifact",
  );

  await assert.rejects(
    fixture.publication.publishCheckpointArtifact({
      ...first,
      artifactDirectory: differentArtifactDirectory,
    }),
    (error) =>
      assertPublicationError(
        error,
        "publication_outcome_uncertain",
        "uncertain",
      ),
  );

  assert.equal(await pathExists(differentArtifactDirectory), false);
  assert.equal(await pathExists(fixture.artifactDirectory), true);
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

test("checkpoint capture rejects a source-root swap after prepare", async (t) => {
  let fixture;
  const displaced = "displaced-source-root-after-prepare";
  fixture = await createFixture(t, {
    faults: {
      async afterJournalPrepared() {
        const displacedRoot = join(fixture.root, displaced);
        await rename(fixture.sourceOwnedRoot, displacedRoot);
        await mkdir(fixture.sourceOwnedRoot, { mode: 0o700 });
        await rename(
          join(displacedRoot, basename(fixture.sourceDirectory)),
          fixture.sourceDirectory,
        );
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

test("checkpoint capture rejects a target-root swap after the source barrier", async (t) => {
  let fixture;
  const displaced = "displaced-artifact-root-after-source-barrier";
  fixture = await createFixture(t, {
    faults: {
      async afterSourceBarrier() {
        await rename(
          fixture.artifactOwnedRoot,
          join(fixture.root, displaced),
        );
        await mkdir(fixture.artifactOwnedRoot, { mode: 0o700 });
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
  const candidateName = stoppedDirectoryPublicationCandidateName(
    CAPTURE_OPERATION_ID,
    basename(fixture.artifactDirectory),
  );
  assert.equal(await pathExists(join(fixture.artifactOwnedRoot, candidateName)), false);
  assert.equal(
    await pathExists(join(fixture.root, displaced, candidateName)),
    false,
  );
  assert.equal(
    (await fixture.journal.read({ operationId: CAPTURE_OPERATION_ID })).record.state,
    "prepared",
  );
});

test("checkpoint capture revalidates the source root before candidate copy", async (t) => {
  let fixture;
  const displaced = "displaced-source-root-after-candidate";
  fixture = await createFixture(t, {
    faults: {
      async afterCandidateCreated() {
        const displacedRoot = join(fixture.root, displaced);
        await rename(fixture.sourceOwnedRoot, displacedRoot);
        await mkdir(fixture.sourceOwnedRoot, { mode: 0o700 });
        await rename(
          join(displacedRoot, basename(fixture.sourceDirectory)),
          fixture.sourceDirectory,
        );
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
  const candidate = candidatePath(
    fixture.artifactOwnedRoot,
    CAPTURE_OPERATION_ID,
    fixture.artifactDirectory,
  );
  assert.equal(await pathExists(candidate), true);
  assert.equal(await pathExists(join(candidate, "payload")), false);
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
