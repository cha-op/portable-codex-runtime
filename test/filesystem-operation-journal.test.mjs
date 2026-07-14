import assert from "node:assert/strict";
import {
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
import { join } from "node:path";
import test from "node:test";

import {
  FilesystemOperationJournal,
  OPERATION_JOURNAL_LOCK_NAME,
  OPERATION_JOURNAL_RECORD_VERSION,
  OperationJournalError,
  operationJournalRecordFilename,
  snapshotOperationJournalBinding,
} from "../src/filesystem-operation-journal.mjs";

const SESSION_ID = "019f2100-0000-7000-8000-000000000001";
const THREAD_ID = "019f2100-0000-7000-8000-000000000002";
const CHECKPOINT_ID = "checkpoint-001";
const ARTIFACT_ID = "artifact-001";
const OPERATION_ID = "operation-checkpoint-001";
const IMAGE_DIGEST = `sha256:${"a".repeat(64)}`;
const MODELED_DIGEST = `sha256:${"b".repeat(64)}`;

function operationTemporaryRecordFilename(operationId) {
  return `.${operationJournalRecordFilename(operationId).slice(0, -5)}.tmp-current`;
}

const TRUSTED_ACL_INSPECTORS = Object.freeze({
  inspectAncestorAcl: async () => false,
  inspectDirectoryAcl: async () => false,
});

test("journal binding snapshots detach and freeze nested caller state", () => {
  const source = {
    backendId: "single-attach-test",
    metadata: { lane: "original" },
  };
  const snapshot = snapshotOperationJournalBinding(source);
  source.metadata.lane = "mutated";

  assert.equal(snapshot.metadata.lane, "original");
  assert.equal(Object.isFrozen(snapshot), true);
  assert.equal(Object.isFrozen(snapshot.metadata), true);
});

function binding(overrides = {}) {
  return {
    backendId: "single-attach-test",
    operation: "checkpoint",
    operationId: OPERATION_ID,
    sessionId: SESSION_ID,
    storageId: "volume-001",
    ...overrides,
  };
}

function request(overrides = {}) {
  return {
    contractVersion: 1,
    backendId: "single-attach-test",
    storageId: "volume-001",
    sessionId: SESSION_ID,
    leaseId: "lease-001",
    holderId: "host-001",
    fencingEpoch: "11",
    operation: "checkpoint",
    operationId: OPERATION_ID,
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

function materialization(overrides = {}) {
  return {
    modeledDigest: MODELED_DIGEST,
    publicationId: "publication-001",
    ...overrides,
  };
}

function mutationResult(mutationRequest = request(), overrides = {}) {
  return {
    ...mutationRequest,
    proofId: "proof-checkpoint-001",
    status: mutationRequest.operation === "checkpoint" ? "checkpoint-created" : "restored",
    ...overrides,
  };
}

function result(
  fixedCheckpoint = checkpoint(),
  fixedRequest = request(),
  overrides = {},
) {
  return {
    checkpoint: fixedCheckpoint,
    mutation: mutationResult(fixedRequest),
    ...overrides,
  };
}

function prepareOptions(overrides = {}) {
  return {
    operationId: OPERATION_ID,
    binding: binding(),
    request: request(),
    result: result(),
    ...overrides,
  };
}

function assertJournalError(code, commitState) {
  return (error) =>
    error instanceof OperationJournalError &&
    error.code === code &&
    error.retryable === false &&
    (commitState === undefined || error.commitState === commitState) &&
    Object.isFrozen(error) &&
    !Object.hasOwn(error, "cause") &&
    !Object.hasOwn(error, "details");
}

function simpleLockProvider({ onRelease, renamePath = rename } = {}) {
  return async () => ({
    async assertHeld() {},
    async release() {
      await onRelease?.();
    },
    async renameWhileHeld(source, destination) {
      await renamePath(source, destination);
    },
  });
}

async function createFixture(t, options = {}) {
  const { useDefaultLock = false, ...journalOptions } = options;
  const root = await mkdtemp(join(tmpdir(), "filesystem-operation-journal-test-"));
  const directory = join(root, "journal");
  await mkdir(directory, { mode: 0o700 });
  await writeFile(join(directory, OPERATION_JOURNAL_LOCK_NAME), "", {
    mode: 0o600,
  });
  t.after(() => rm(root, { force: true, recursive: true }));
  const journal = new FilesystemOperationJournal({
    directory,
    ...(useDefaultLock ? {} : { acquireLock: simpleLockProvider() }),
    ...TRUSTED_ACL_INSPECTORS,
    ...journalOptions,
  });
  return { directory, journal, root };
}

async function prepare(journal, options = prepareOptions()) {
  return journal.prepare(options);
}

async function markMaterialized(
  journal,
  fixedMaterialization = materialization(),
  options = prepareOptions(),
) {
  return journal.markMaterialized({ ...options, materialization: fixedMaterialization });
}

async function commit(
  journal,
  fixedResult = result(),
  fixedMaterialization = materialization(),
  options = prepareOptions(),
) {
  return journal.commit({
    ...options,
    materialization: fixedMaterialization,
    result: fixedResult,
  });
}

test("journal authority description is frozen and identity-pinned", async (t) => {
  const { directory, journal } = await createFixture(t);
  const identity = await lstat(directory, { bigint: true });

  const authority = await journal.describeAuthority();

  assert.deepEqual(authority, {
    device: identity.dev.toString(),
    inode: identity.ino.toString(),
    path: await realpath(directory),
  });
  assert.equal(Object.isFrozen(authority), true);
  assert.deepEqual(await journal.describeAuthority(), authority);
});

test("default journal requires a preprovisioned lock without creating it", async (t) => {
  const root = await mkdtemp(join(tmpdir(), "filesystem-journal-lock-test-"));
  const directory = join(root, "journal");
  const lockPath = join(directory, OPERATION_JOURNAL_LOCK_NAME);
  await mkdir(directory, { mode: 0o700 });
  t.after(() => rm(root, { force: true, recursive: true }));
  const journal = new FilesystemOperationJournal({
    directory,
    ...TRUSTED_ACL_INSPECTORS,
  });

  await assert.rejects(
    journal.read({ operationId: OPERATION_ID }),
    assertJournalError("journal_io_failed", "not-committed"),
  );
  await assert.rejects(lstat(lockPath), (error) => error?.code === "ENOENT");
});

test("journal lock acquisition requests an existing-only authority", async (t) => {
  let acquisitionOptions;
  const baseLockProvider = simpleLockProvider();
  const { journal } = await createFixture(t, {
    acquireLock: async (path, options) => {
      acquisitionOptions = options;
      return baseLockProvider(path, options);
    },
  });

  assert.equal((await journal.read({ operationId: OPERATION_ID })).record, null);
  assert.deepEqual(acquisitionOptions, { requireExisting: true });
});

test("journal state hints do not acquire locks or run journal fault callbacks", async (t) => {
  let lockAcquisitions = 0;
  let recordReadCallbacks = 0;
  let lockReleaseCallbacks = 0;
  const baseLockProvider = simpleLockProvider();
  const { journal } = await createFixture(t, {
    acquireLock: async (...args) => {
      lockAcquisitions += 1;
      return baseLockProvider(...args);
    },
    faults: {
      async afterRecordRead() {
        recordReadCallbacks += 1;
      },
      async beforeLockRelease() {
        lockReleaseCallbacks += 1;
      },
    },
  });

  const absent = await journal.readStateHint({ operationId: OPERATION_ID });
  assert.equal(absent.record, null);
  assert.equal(lockAcquisitions, 0);
  assert.equal(recordReadCallbacks, 0);
  assert.equal(lockReleaseCallbacks, 0);

  await prepare(journal);
  lockAcquisitions = 0;
  recordReadCallbacks = 0;
  lockReleaseCallbacks = 0;
  const prepared = await journal.readStateHint({ operationId: OPERATION_ID });
  assert.equal(prepared.record.state, "prepared");
  assert.equal(prepared.replayed, false);
  assert.equal(lockAcquisitions, 0);
  assert.equal(recordReadCallbacks, 0);
  assert.equal(lockReleaseCallbacks, 0);
});

test("journal persists canonical prepared, materialized, and committed states", async (t) => {
  const { directory, journal } = await createFixture(t);
  const absent = await journal.read({ operationId: OPERATION_ID });
  assert.equal(absent.record, null);
  assert.equal(absent.replayed, false);

  const prepared = await prepare(journal);
  assert.equal(prepared.record.recordVersion, OPERATION_JOURNAL_RECORD_VERSION);
  assert.equal(prepared.record.state, "prepared");
  assert.equal(prepared.record.revision, "1");
  assert.equal(prepared.record.materialization, null);
  assert.equal(prepared.replayed, false);
  assert(Object.isFrozen(prepared));

  const materialized = await markMaterialized(journal);
  assert.equal(materialized.record.state, "materialized");
  assert.equal(materialized.record.revision, "2");
  assert.deepEqual(materialized.record.materialization, materialization());

  const committed = await commit(journal);
  assert.equal(committed.record.state, "committed");
  assert.equal(committed.record.revision, "3");
  assert.deepEqual(committed.record.result, result());

  const recordPath = join(directory, operationJournalRecordFilename(OPERATION_ID));
  const raw = await readFile(recordPath, "utf8");
  assert.equal(raw, `${JSON.stringify(committed.record)}\n`);
  const metadata = await lstat(recordPath);
  assert.equal(metadata.isFile(), true);
  assert.equal(metadata.nlink, 1);
  assert.equal(metadata.mode & 0o777, 0o600);
});

test("non-ASCII canonical records persist through every journal state", async (t) => {
  const { directory, journal } = await createFixture(t);
  const options = prepareOptions({
    binding: binding({ label: "migración-迁移-🚚" }),
  });
  const fixedMaterialization = materialization({ label: "réplique-副本-🧊" });

  await prepare(journal, options);
  await markMaterialized(journal, fixedMaterialization, options);
  const committed = await commit(
    journal,
    options.result,
    fixedMaterialization,
    options,
  );
  assert.equal(committed.record.binding.label, "migración-迁移-🚚");
  assert.equal(committed.record.materialization.label, "réplique-副本-🧊");

  const fresh = new FilesystemOperationJournal({
    directory,
    ...TRUSTED_ACL_INSPECTORS,
  });
  assert.deepEqual(
    (await fresh.read({ operationId: OPERATION_ID })).record,
    committed.record,
  );
});

test("fresh journal instances read every durable state", async (t) => {
  const { directory, journal } = await createFixture(t);
  const fresh = () =>
    new FilesystemOperationJournal({ directory, ...TRUSTED_ACL_INSPECTORS });

  const prepared = await prepare(journal);
  assert.deepEqual((await fresh().read({ operationId: OPERATION_ID })).record, prepared.record);

  const materialized = await markMaterialized(journal);
  assert.deepEqual(
    (await fresh().read({ operationId: OPERATION_ID })).record,
    materialized.record,
  );

  const committed = await commit(journal);
  assert.deepEqual((await fresh().read({ operationId: OPERATION_ID })).record, committed.record);
});

test("exact replay returns the canonical state without rewriting it", async (t) => {
  const { directory, journal } = await createFixture(t);
  const prepared = await prepare(journal);
  const recordPath = join(directory, operationJournalRecordFilename(OPERATION_ID));
  const preparedIdentity = await lstat(recordPath, { bigint: true });
  const preparedReplay = await prepare(journal);
  assert.deepEqual(preparedReplay.record, prepared.record);
  assert.equal(preparedReplay.replayed, true);
  const replayedPreparedIdentity = await lstat(recordPath, { bigint: true });
  assert.equal(replayedPreparedIdentity.dev, preparedIdentity.dev);
  assert.equal(replayedPreparedIdentity.ino, preparedIdentity.ino);

  const materialized = await markMaterialized(journal);
  const materializedIdentity = await lstat(recordPath, { bigint: true });
  const materializedReplay = await markMaterialized(journal);
  assert.deepEqual(materializedReplay.record, materialized.record);
  assert.equal(materializedReplay.replayed, true);
  assert.equal((await lstat(recordPath, { bigint: true })).ino, materializedIdentity.ino);

  const committed = await commit(journal);
  const committedIdentity = await lstat(recordPath, { bigint: true });
  const committedReplay = await commit(journal);
  assert.deepEqual(committedReplay.record, committed.record);
  assert.equal(committedReplay.replayed, true);
  assert.equal((await lstat(recordPath, { bigint: true })).ino, committedIdentity.ino);
});

test("replays requesting an earlier exact phase observe progressed canonical state", async (t) => {
  const { journal } = await createFixture(t);
  await prepare(journal);
  const materialized = await markMaterialized(journal);
  assert.deepEqual((await prepare(journal)).record, materialized.record);
  const committed = await commit(journal);
  assert.deepEqual((await prepare(journal)).record, committed.record);
  assert.deepEqual((await markMaterialized(journal)).record, committed.record);
});

test("commit cannot skip the materialized state", async (t) => {
  const { journal } = await createFixture(t);
  await prepare(journal);
  await assert.rejects(
    commit(journal),
    assertJournalError("invalid_state_transition", "not-committed"),
  );
  assert.equal((await journal.read({ operationId: OPERATION_ID })).record.state, "prepared");
});

test("returned records are frozen defensive copies", async (t) => {
  const { journal } = await createFixture(t);
  const options = prepareOptions();
  const prepared = await prepare(journal, options);
  options.binding.storageId = "mutated-volume";
  options.request.target.checkpointId = "mutated-checkpoint";
  options.result.checkpoint.createdAt = "2026-07-02T12:00:01.000Z";
  const reread = (await journal.read({ operationId: OPERATION_ID })).record;
  assert.equal(reread.binding.storageId, "volume-001");
  assert.equal(reread.request.target.checkpointId, CHECKPOINT_ID);
  assert.equal(reread.result.checkpoint.createdAt, "2026-07-02T12:00:00.000Z");
  for (const value of [
    prepared,
    prepared.record,
    reread,
    reread.binding,
    reread.request,
    reread.result,
    reread.result.checkpoint,
  ]) {
    assert(Object.isFrozen(value));
  }
  assert.notStrictEqual(reread, prepared.record);
});

test("record filenames separate operation identifiers without path ambiguity", () => {
  const first = operationJournalRecordFilename("operation:a-b");
  const second = operationJournalRecordFilename("operation-a:b");
  assert.notEqual(first, second);
  for (const filename of [first, second, operationJournalRecordFilename(OPERATION_ID)]) {
    assert.equal(filename.includes("/"), false);
    assert.equal(filename.includes("\\"), false);
    assert.match(filename, /^[A-Za-z0-9._-]+$/u);
  }
  for (const invalid of ["", "../operation", "operation/child", "\ud800", "x".repeat(129)]) {
    assert.throws(
      () => operationJournalRecordFilename(invalid),
      assertJournalError("invalid_journal_request", "not-committed"),
    );
  }
});

test("operation IDs bind the exact binding, request, result, and materialization", async (t) => {
  const { journal } = await createFixture(t);
  await prepare(journal);
  const differentRequest = request({ holderId: "host-002" });

  const conflicts = [
    prepareOptions({ binding: binding({ storageId: "volume-002" }) }),
    prepareOptions({
      request: differentRequest,
      result: result(checkpoint(), differentRequest),
    }),
    prepareOptions({
      result: result(checkpoint({ createdAt: "2026-07-02T12:00:01.000Z" })),
    }),
  ];
  for (const candidate of conflicts) {
    await assert.rejects(
      prepare(journal, candidate),
      assertJournalError("operation_conflict", "not-committed"),
    );
  }

  await markMaterialized(journal);
  await assert.rejects(
    markMaterialized(journal, materialization({ publicationId: "publication-002" })),
    assertJournalError("operation_conflict", "not-committed"),
  );
  await commit(journal);
  await assert.rejects(
    commit(
      journal,
      result(checkpoint(), request(), {
        mutation: mutationResult(request(), { proofId: "proof-checkpoint-002" }),
      }),
    ),
    assertJournalError("operation_conflict", "not-committed"),
  );
});

test("prepare rejects materialization outside its exact request envelope", async (t) => {
  const { journal } = await createFixture(t);
  await assert.rejects(
    journal.prepare({ ...prepareOptions(), materialization: materialization() }),
    assertJournalError("invalid_journal_request", "not-committed"),
  );
  assert.equal((await journal.read({ operationId: OPERATION_ID })).record, null);
});

test("capture results must match the request target, writer storage, and fence", async (t) => {
  const scenarios = [
    checkpoint({ checkpointId: "checkpoint-002" }),
    checkpoint({ artifactId: "artifact-002" }),
    checkpoint({ sessionId: "019f2100-0000-7000-8000-000000000099" }),
    checkpoint({ backendId: "other-backend" }),
    checkpoint({ storageId: "volume-002" }),
    checkpoint({ sourceFencingEpoch: "12" }),
  ];
  for (const [index, mismatchedCheckpoint] of scenarios.entries()) {
    const isolatedRoot = await mkdtemp(
      join(tmpdir(), `filesystem-operation-journal-binding-${index}-`),
    );
    const directory = join(isolatedRoot, "journal");
    await mkdir(directory, { mode: 0o700 });
    t.after(() => rm(isolatedRoot, { force: true, recursive: true }));
    const isolated = new FilesystemOperationJournal({
      directory,
      ...TRUSTED_ACL_INSPECTORS,
    });
    await assert.rejects(
      isolated.prepare(
        prepareOptions({ result: result(mismatchedCheckpoint) }),
      ),
      assertJournalError("invalid_journal_request", "not-committed"),
    );
  }
});

test("restore results may retain source-side checkpoint storage and fence", async (t) => {
  const { journal } = await createFixture(t);
  const restoreRequest = request({
    operation: "restore",
    operationId: "operation-restore-001",
    storageId: "volume-restore-001",
  });
  const restoreBinding = binding({
    operation: "restore",
    operationId: restoreRequest.operationId,
    storageId: restoreRequest.storageId,
  });
  const sourceCheckpoint = checkpoint({
    sourceFencingEpoch: "9",
    storageId: "volume-source-001",
  });
  const restoreResult = result(sourceCheckpoint, restoreRequest);
  const prepared = await journal.prepare({
    operationId: restoreRequest.operationId,
    binding: restoreBinding,
    request: restoreRequest,
    result: restoreResult,
  });
  assert.equal(prepared.record.state, "prepared");
  assert.equal(prepared.record.result.checkpoint.storageId, "volume-source-001");
  assert.equal(prepared.record.request.storageId, "volume-restore-001");
});

test("restore requires a request fence newer than the checkpoint source fence", async (t) => {
  const { journal } = await createFixture(t);
  const restoreRequest = request({
    operation: "restore",
    operationId: "operation-restore-001",
    storageId: "volume-restore-001",
  });
  const restoreBinding = binding({
    operation: "restore",
    operationId: restoreRequest.operationId,
    storageId: restoreRequest.storageId,
  });
  for (const sourceFencingEpoch of ["11", "12"]) {
    const sourceCheckpoint = checkpoint({
      sourceFencingEpoch,
      storageId: "volume-source-001",
    });
    await assert.rejects(
      journal.prepare({
        operationId: restoreRequest.operationId,
        binding: restoreBinding,
        request: restoreRequest,
        result: result(sourceCheckpoint, restoreRequest),
      }),
      assertJournalError("invalid_journal_request", "not-committed"),
    );
  }
  assert.equal(
    (await journal.read({ operationId: restoreRequest.operationId })).record,
    null,
  );
});

test("hostile option, proxy, accessor, symbol, and non-enumerable inputs fail closed", async (t) => {
  const { journal } = await createFixture(t);
  const accessorRequest = request();
  Object.defineProperty(accessorRequest, "holderId", {
    enumerable: true,
    get() {
      throw new Error("sensitive accessor sentinel");
    },
  });
  const symbolResult = result();
  symbolResult[Symbol("unexpected")] = true;
  const hiddenBinding = binding();
  Object.defineProperty(hiddenBinding, "hidden", { value: true });
  const hostileProxy = new Proxy(binding(), {
    ownKeys() {
      throw new Error("sensitive proxy sentinel");
    },
  });
  const cases = [
    new Proxy(prepareOptions(), {}),
    prepareOptions({ binding: hostileProxy }),
    prepareOptions({ request: accessorRequest }),
    prepareOptions({ result: symbolResult }),
    prepareOptions({ binding: hiddenBinding }),
  ];
  for (const candidate of cases) {
    await assert.rejects(
      journal.prepare(candidate),
      (error) =>
        assertJournalError("invalid_journal_request", "not-committed")(error) &&
        !error.message.includes("sensitive"),
    );
  }
  assert.equal((await journal.read({ operationId: OPERATION_ID })).record, null);
});

test("operational collaborators cannot forge public journal errors", async (t) => {
  let escapedInternalError;
  try {
    operationJournalRecordFilename("");
  } catch (error) {
    escapedInternalError = error;
  }
  assert(escapedInternalError instanceof OperationJournalError);

  await t.test("fault hook", async (t) => {
    const { journal } = await createFixture(t, {
      faults: {
        async afterRecordRead() {
          throw new OperationJournalError("operation_conflict", "committed");
        },
      },
    });
    await assert.rejects(
      prepare(journal),
      assertJournalError("journal_io_failed", "not-committed"),
    );
  });

  await t.test("lock provider", async (t) => {
    const { journal } = await createFixture(t, {
      acquireLock: async () => ({
        async assertHeld() {
          throw new OperationJournalError("operation_conflict", "committed");
        },
        async release() {},
        async renameWhileHeld() {},
      }),
    });
    await assert.rejects(
      prepare(journal),
      assertJournalError("journal_io_failed", "not-committed"),
    );
  });

  await t.test("directory sync provider", async (t) => {
    const { journal } = await createFixture(t, {
      syncDirectory: async () => {
        throw new OperationJournalError("operation_conflict", "committed");
      },
    });
    await assert.rejects(
      prepare(journal),
      assertJournalError("journal_commit_outcome_uncertain", "uncertain"),
    );
  });

  await t.test("temporary-record inspector", async (t) => {
    const { journal } = await createFixture(t, {
      inspectTemporaryRecord: async () => {
        throw new OperationJournalError("operation_conflict", "committed");
      },
    });
    await assert.rejects(
      prepare(journal),
      assertJournalError("journal_io_failed", "not-committed"),
    );
  });

  await t.test("fault hook cannot replay an escaped internal error", async (t) => {
    const { journal } = await createFixture(t, {
      faults: {
        async afterRecordRead() {
          throw escapedInternalError;
        },
      },
    });
    await assert.rejects(
      prepare(journal),
      assertJournalError("journal_io_failed", "not-committed"),
    );
  });

  await t.test("lock provider cannot replay an escaped internal error", async (t) => {
    const { journal } = await createFixture(t, {
      acquireLock: async () => ({
        async assertHeld() {
          throw escapedInternalError;
        },
        async release() {},
        async renameWhileHeld() {},
      }),
    });
    await assert.rejects(
      prepare(journal),
      assertJournalError("journal_io_failed", "not-committed"),
    );
  });
});

test("noncanonical, deeply nested, cyclic, and oversized inputs fail before publication", async (t) => {
  const { journal } = await createFixture(t);
  let deep = "leaf";
  for (let index = 0; index < 64; index += 1) deep = { child: deep };
  const cyclic = binding();
  cyclic.self = cyclic;
  const hugeSparseArray = new Array(2 ** 32 - 1);
  const excessiveDenseArray = new Array(10_000).fill(null);
  const repeatedLongString = "z".repeat(100 * 1024);
  const repeatedLongStrings = new Array(8_000).fill(repeatedLongString);
  const excessiveSiblingNodes = {
    left: new Array(4_100).fill(null),
    right: new Array(4_100).fill(null),
  };
  const cases = [
    prepareOptions({ binding: binding({ label: "\ud800" }) }),
    prepareOptions({ binding: binding({ accessToken: "forbidden" }) }),
    prepareOptions({ binding: binding({ apiKey: "forbidden" }) }),
    prepareOptions({ binding: binding({ githubToken: "forbidden" }) }),
    prepareOptions({ binding: binding({ private_key: "forbidden" }) }),
    prepareOptions({ binding: binding({ token: "forbidden" }) }),
    prepareOptions({ binding: binding({ opaque: "sk-sensitive1234" }) }),
    prepareOptions({ binding: binding({ opaque: "eyJheader123.payload.signature" }) }),
    prepareOptions({ binding: binding({ opaque: `AIza${"a".repeat(32)}` }) }),
    prepareOptions({ binding: binding({ opaque: `ghp_${"b".repeat(32)}` }) }),
    prepareOptions({
      binding: binding({ opaque: `github_pat_${"c".repeat(32)}` }),
    }),
    prepareOptions({ binding: binding({ opaque: `AKIA${"C".repeat(16)}` }) }),
    prepareOptions({ binding: binding({ opaque: `xoxb-${"d".repeat(24)}` }) }),
    prepareOptions({ binding: binding({ opaque: "Bearer opaquecredential" }) }),
    prepareOptions({
      binding: binding({
        opaque: "-----BEGIN " + "ENCRYPTED PRIVATE KEY-----",
      }),
    }),
    prepareOptions({ binding: binding({ deep }) }),
    prepareOptions({ binding: cyclic }),
    prepareOptions({ binding: binding({ values: hugeSparseArray }) }),
    prepareOptions({ binding: binding({ values: excessiveDenseArray }) }),
    prepareOptions({ binding: binding({ values: repeatedLongStrings }) }),
    prepareOptions({ binding: binding({ values: excessiveSiblingNodes }) }),
    prepareOptions({ binding: binding({ oversized: "x".repeat(2 * 1024 * 1024) }) }),
  ];
  for (const candidate of cases) {
    await assert.rejects(
      journal.prepare(candidate),
      assertJournalError("invalid_journal_request", "not-committed"),
    );
  }
  assert.equal((await journal.read({ operationId: OPERATION_ID })).record, null);
});

test("hostile materialization metadata fails without advancing prepared state", async (t) => {
  const { journal } = await createFixture(t);
  await prepare(journal);
  const accessor = materialization();
  Object.defineProperty(accessor, "publicationId", {
    enumerable: true,
    get() {
      throw new Error("sensitive materialization accessor");
    },
  });
  const symbol = materialization();
  symbol[Symbol("unexpected")] = true;
  let deep = "leaf";
  for (let index = 0; index < 64; index += 1) deep = { child: deep };
  for (const value of [
    new Proxy(materialization(), {}),
    accessor,
    symbol,
    { deep },
    { oversized: "x".repeat(2 * 1024 * 1024) },
  ]) {
    await assert.rejects(
      markMaterialized(journal, value),
      assertJournalError("invalid_journal_request", "not-committed"),
    );
  }
  assert.equal((await journal.read({ operationId: OPERATION_ID })).record.state, "prepared");
});

test("corrupt, reordered, duplicate-key, unsupported, and oversized records fail closed", async (t) => {
  const mutations = [
    {
      name: "corrupt JSON",
      expected: "invalid_journal_record",
      mutate: () => "{not-json}\n",
    },
    {
      name: "reordered fields",
      expected: "invalid_journal_record",
      mutate: (raw) => {
        const record = JSON.parse(raw);
        const reversed = Object.fromEntries(Object.entries(record).reverse());
        return `${JSON.stringify(reversed)}\n`;
      },
    },
    {
      name: "duplicate key",
      expected: "invalid_journal_record",
      mutate: (raw) => raw.replace(
        '{"recordVersion":1,',
        '{"recordVersion":1,"recordVersion":1,',
      ),
    },
    {
      name: "unsupported version",
      expected: "unsupported_journal_record",
      mutate: (raw) => raw.replace('{"recordVersion":1,', '{"recordVersion":2,'),
    },
    {
      name: "malformed version",
      expected: "invalid_journal_record",
      mutate: (raw) => raw.replace('{"recordVersion":1,', '{"recordVersion":null,'),
    },
    {
      name: "unsupported future schema",
      expected: "unsupported_journal_record",
      mutate: () => '{"recordVersion":2,"futureField":true}\n',
    },
    {
      name: "oversized record",
      expected: "invalid_journal_record",
      mutate: () => `${"x".repeat(2 * 1024 * 1024)}\n`,
    },
  ];

  for (const [index, scenario] of mutations.entries()) {
    await t.test(scenario.name, async (t) => {
      const { directory, journal } = await createFixture(t);
      await prepare(journal);
      const recordPath = join(directory, operationJournalRecordFilename(OPERATION_ID));
      const raw = await readFile(recordPath, "utf8");
      await writeFile(recordPath, scenario.mutate(raw), { mode: 0o600 });
      await chmod(recordPath, 0o600);
      const fresh = new FilesystemOperationJournal({
        directory,
        ...TRUSTED_ACL_INSPECTORS,
      });
      await assert.rejects(
        fresh.read({ operationId: OPERATION_ID }),
        assertJournalError(scenario.expected, "not-committed"),
      );
      assert(Number.isSafeInteger(index));
    });
  }
});

test("journal rejects unsafe record and directory filesystem topology", async (t) => {
  await t.test("record mode", async (t) => {
    const { directory, journal } = await createFixture(t);
    await prepare(journal);
    const recordPath = join(directory, operationJournalRecordFilename(OPERATION_ID));
    await chmod(recordPath, 0o644);
    await assert.rejects(
      journal.read({ operationId: OPERATION_ID }),
      assertJournalError("invalid_journal_record", "not-committed"),
    );
  });

  await t.test("record symlink", async (t) => {
    const { directory, journal, root } = await createFixture(t);
    const outside = join(root, "outside");
    await writeFile(outside, "{}\n", { mode: 0o600 });
    const recordPath = join(directory, operationJournalRecordFilename(OPERATION_ID));
    await symlink(outside, recordPath);
    await assert.rejects(
      journal.read({ operationId: OPERATION_ID }),
      assertJournalError("invalid_journal_record", "not-committed"),
    );
  });

  await t.test("record hard link", async (t) => {
    const { directory, journal } = await createFixture(t);
    await prepare(journal);
    const recordPath = join(directory, operationJournalRecordFilename(OPERATION_ID));
    await link(recordPath, join(directory, "record-alias"));
    await assert.rejects(
      journal.read({ operationId: OPERATION_ID }),
      assertJournalError("invalid_journal_record", "not-committed"),
    );
  });

  await t.test("permissive private directory", async (t) => {
    const root = await mkdtemp(join(tmpdir(), "filesystem-journal-directory-mode-"));
    const directory = join(root, "journal");
    await mkdir(directory, { mode: 0o755 });
    await chmod(directory, 0o755);
    t.after(() => rm(root, { force: true, recursive: true }));
    const journal = new FilesystemOperationJournal({
      directory,
      ...TRUSTED_ACL_INSPECTORS,
    });
    await assert.rejects(
      journal.read({ operationId: OPERATION_ID }),
      assertJournalError("invalid_journal_directory", "not-committed"),
    );
  });

  await t.test("directory symlink", async (t) => {
    const root = await mkdtemp(join(tmpdir(), "filesystem-journal-directory-link-"));
    const target = join(root, "target");
    const directory = join(root, "journal");
    await mkdir(target, { mode: 0o700 });
    await symlink(target, directory);
    t.after(() => rm(root, { force: true, recursive: true }));
    const journal = new FilesystemOperationJournal({
      directory,
      ...TRUSTED_ACL_INSPECTORS,
    });
    await assert.rejects(
      journal.read({ operationId: OPERATION_ID }),
      assertJournalError("invalid_journal_directory", "not-committed"),
    );
  });

  await t.test("directory and ancestor ACLs", async (t) => {
    const root = await mkdtemp(join(tmpdir(), "filesystem-journal-directory-acl-"));
    const directory = join(root, "journal");
    await mkdir(directory, { mode: 0o700 });
    t.after(() => rm(root, { force: true, recursive: true }));
    for (const inspectors of [
      { inspectDirectoryAcl: async () => true, inspectAncestorAcl: async () => false },
      { inspectDirectoryAcl: async () => false, inspectAncestorAcl: async () => true },
    ]) {
      const journal = new FilesystemOperationJournal({ directory, ...inspectors });
      await assert.rejects(
        journal.read({ operationId: OPERATION_ID }),
        assertJournalError("invalid_journal_directory", "not-committed"),
      );
    }
  });
});

test("a journal instance rejects replacement of its pinned directory between calls", async (t) => {
  for (const operation of ["read", "prepare"]) {
    await t.test(operation, async (t) => {
      const { directory, journal, root } = await createFixture(t);
      await prepare(journal);
      await rename(directory, join(root, "original-journal"));
      await mkdir(directory, { mode: 0o700 });
      await chmod(directory, 0o700);

      const action = operation === "read"
        ? journal.read({ operationId: OPERATION_ID })
        : prepare(journal);
      await assert.rejects(
        action,
        assertJournalError("invalid_journal_directory", "not-committed"),
      );
    });
  }
});

test("a failed first pin can retry after the directory becomes valid", async (t) => {
  const { directory, journal } = await createFixture(t);
  await chmod(directory, 0o755);
  await assert.rejects(
    journal.read({ operationId: OPERATION_ID }),
    assertJournalError("invalid_journal_directory", "not-committed"),
  );
  await chmod(directory, 0o700);
  assert.equal((await journal.read({ operationId: OPERATION_ID })).record, null);
});

test("concurrent first calls share one directory pin attempt", async (t) => {
  let enterFirstInspection;
  let releaseFirstInspection;
  let inspectionCalls = 0;
  const firstInspectionEntered = new Promise((resolve) => {
    enterFirstInspection = resolve;
  });
  const firstInspectionReleased = new Promise((resolve) => {
    releaseFirstInspection = resolve;
  });
  const { journal } = await createFixture(t, {
    async inspectDirectoryAcl() {
      inspectionCalls += 1;
      if (inspectionCalls === 1) {
        enterFirstInspection();
        await firstInspectionReleased;
      }
      return false;
    },
  });

  const first = journal.read({ operationId: OPERATION_ID });
  await firstInspectionEntered;
  const second = journal.read({ operationId: "operation-checkpoint-002" });
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(inspectionCalls, 1);
  releaseFirstInspection();
  const results = await Promise.all([first, second]);
  assert.deepEqual(results.map(({ record }) => record), [null, null]);
});

test("different operation IDs serialize through one real directory lock", async (t) => {
  let enterFirstRead;
  let releaseFirstRead;
  let readCalls = 0;
  const firstReadEntered = new Promise((resolve) => {
    enterFirstRead = resolve;
  });
  const firstReadReleased = new Promise((resolve) => {
    releaseFirstRead = resolve;
  });
  const { journal } = await createFixture(t, {
    faults: {
      async afterRecordRead() {
        readCalls += 1;
        if (readCalls === 1) {
          enterFirstRead();
          await firstReadReleased;
        }
      },
    },
    useDefaultLock: true,
  });

  const first = journal.read({ operationId: OPERATION_ID });
  await firstReadEntered;
  const second = journal.read({ operationId: "operation-checkpoint-002" });
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(readCalls, 1);
  releaseFirstRead();
  const results = await Promise.all([first, second]);
  assert.deepEqual(results.map(({ record }) => record), [null, null]);
});

test("an absent read rejects directory replacement during record lookup", async (t) => {
  let directory;
  let root;
  let journal;
  ({ directory, journal, root } = await createFixture(t, {
    faults: {
      async afterRecordRead({ record }) {
        if (record !== null) return;
        await rename(directory, join(root, "original-absent-journal"));
        await mkdir(directory, { mode: 0o700 });
        await chmod(directory, 0o700);
      },
    },
  }));

  await assert.rejects(
    journal.read({ operationId: OPERATION_ID }),
    assertJournalError("invalid_journal_directory", "not-committed"),
  );
});

test("pre-rename temp sync and before-rename failures are definitely not committed", async (t) => {
  for (const hook of ["afterRecordRead", "afterTempSync", "beforeRename"]) {
    await t.test(hook, async (t) => {
      const { directory, journal } = await createFixture(t, {
        acquireLock: simpleLockProvider(),
        faults: {
          async [hook]() {
            throw new Error("sensitive pre-rename sentinel");
          },
        },
      });
      await assert.rejects(
        prepare(journal),
        (error) =>
          assertJournalError("journal_io_failed", "not-committed")(error) &&
          !error.message.includes("sensitive"),
      );
      await assert.rejects(
        lstat(join(directory, operationJournalRecordFilename(OPERATION_ID))),
        /ENOENT/u,
      );
      const fresh = new FilesystemOperationJournal({
        acquireLock: simpleLockProvider(),
        directory,
        ...TRUSTED_ACL_INSPECTORS,
      });
      if (hook === "afterRecordRead") {
        assert.equal((await fresh.read({ operationId: OPERATION_ID })).record, null);
      } else {
        await assert.rejects(
          fresh.read({ operationId: OPERATION_ID }),
          assertJournalError("journal_recovery_required", "not-committed"),
        );
      }
    });
  }
});

test("retained recovery evidence is checked by deterministic operation path", async (t) => {
  const inspectedPaths = [];
  const { directory, journal } = await createFixture(t, {
    async inspectTemporaryRecord(path) {
      inspectedPaths.push(path);
      try {
        await lstat(path, { bigint: true });
        return true;
      } catch (error) {
        if (error?.code === "ENOENT") return false;
        throw error;
      }
    },
  });
  const otherOperationId = "operation-checkpoint-002";
  await Promise.all(
    Array.from({ length: 64 }, (_, index) =>
      writeFile(
        join(
          directory,
          operationJournalRecordFilename(`historical-operation-${index}`),
        ),
        "historical record\n",
        { flag: "wx", mode: 0o600 },
      ),
    ),
  );
  await writeFile(
    join(directory, operationTemporaryRecordFilename(otherOperationId)),
    "retained recovery evidence\n",
    { flag: "wx", mode: 0o600 },
  );

  assert.equal((await journal.read({ operationId: OPERATION_ID })).record, null);
  const expectedCurrentPath = join(
    await realpath(directory),
    operationTemporaryRecordFilename(OPERATION_ID),
  );
  assert.deepEqual(inspectedPaths, [
    expectedCurrentPath,
    expectedCurrentPath,
    expectedCurrentPath,
  ]);
  await assert.rejects(
    journal.read({ operationId: otherOperationId }),
    assertJournalError("journal_recovery_required", "not-committed"),
  );
  assert.equal(
    inspectedPaths.at(-1),
    join(
      await realpath(directory),
      operationTemporaryRecordFilename(otherOperationId),
    ),
  );
});

test("pre-rename publication refuses a canonical record created after an absent read", async (t) => {
  const donor = await createFixture(t);
  await prepare(donor.journal);
  const donorBytes = await readFile(
    join(donor.directory, operationJournalRecordFilename(OPERATION_ID)),
  );

  let directory;
  let journal;
  let renameCalls = 0;
  ({ directory, journal } = await createFixture(t, {
    acquireLock: simpleLockProvider({
      async renamePath(source, destination) {
        renameCalls += 1;
        await rename(source, destination);
      },
    }),
    faults: {
      async beforeRename({ record }) {
        if (record.state !== "prepared") return;
        await writeFile(
          join(directory, operationJournalRecordFilename(OPERATION_ID)),
          donorBytes,
          { flag: "wx", mode: 0o600 },
        );
        await chmod(
          join(directory, operationJournalRecordFilename(OPERATION_ID)),
          0o600,
        );
      },
    },
  }));

  await assert.rejects(
    prepare(journal),
    assertJournalError("journal_io_failed", "not-committed"),
  );
  assert.equal(renameCalls, 0);
  assert.deepEqual(
    await readFile(join(directory, operationJournalRecordFilename(OPERATION_ID))),
    donorBytes,
  );
  const fresh = new FilesystemOperationJournal({
    acquireLock: simpleLockProvider(),
    directory,
    ...TRUSTED_ACL_INSPECTORS,
  });
  await assert.rejects(
    fresh.read({ operationId: OPERATION_ID }),
    assertJournalError("journal_recovery_required", "not-committed"),
  );
});

test("pre-rename publication cannot replace the predecessor with a new inode", async (t) => {
  let armed = false;
  let directory;
  let journal;
  let root;
  let replacementIdentity;
  ({ directory, journal, root } = await createFixture(t, {
    faults: {
      async beforeRename({ record }) {
        if (!armed || record.state !== "materialized") return;
        const recordPath = join(
          directory,
          operationJournalRecordFilename(OPERATION_ID),
        );
        const exactBytes = await readFile(recordPath);
        await rename(recordPath, join(root, "original-predecessor.json"));
        await writeFile(recordPath, exactBytes, { flag: "wx", mode: 0o600 });
        await chmod(recordPath, 0o600);
        replacementIdentity = await lstat(recordPath, { bigint: true });
      },
    },
  }));
  await prepare(journal);
  armed = true;

  await assert.rejects(
    markMaterialized(journal),
    assertJournalError("journal_io_failed", "not-committed"),
  );
  const visibleIdentity = await lstat(
    join(directory, operationJournalRecordFilename(OPERATION_ID)),
    { bigint: true },
  );
  assert.equal(visibleIdentity.dev, replacementIdentity.dev);
  assert.equal(visibleIdentity.ino, replacementIdentity.ino);
  assert.equal(
    JSON.parse(
      await readFile(
        join(directory, operationJournalRecordFilename(OPERATION_ID)),
        "utf8",
      ),
    ).state,
    "prepared",
  );
});

test("pre-rename publication cannot roll back a concurrently advanced record", async (t) => {
  const donor = await createFixture(t);
  await prepare(donor.journal);
  await markMaterialized(donor.journal);
  await commit(donor.journal);
  const committedBytes = await readFile(
    join(donor.directory, operationJournalRecordFilename(OPERATION_ID)),
  );

  let armed = false;
  let directory;
  let journal;
  let root;
  ({ directory, journal, root } = await createFixture(t, {
    faults: {
      async beforeRename({ record }) {
        if (!armed || record.state !== "materialized") return;
        const recordPath = join(
          directory,
          operationJournalRecordFilename(OPERATION_ID),
        );
        await rename(recordPath, join(root, "prepared-predecessor.json"));
        await writeFile(recordPath, committedBytes, { flag: "wx", mode: 0o600 });
        await chmod(recordPath, 0o600);
      },
    },
  }));
  await prepare(journal);
  armed = true;

  await assert.rejects(
    markMaterialized(journal),
    assertJournalError("journal_io_failed", "not-committed"),
  );
  assert.deepEqual(
    await readFile(join(directory, operationJournalRecordFilename(OPERATION_ID))),
    committedBytes,
  );
});

test("publication passes the exact predecessor identity to the rename authority", async (t) => {
  const expectations = [];
  const { directory, journal } = await createFixture(t, {
    acquireLock: async () => ({
      async assertHeld() {},
      async release() {},
      async renameWhileHeld(source, destination, expectedDestination) {
        expectations.push(expectedDestination);
        await rename(source, destination);
      },
    }),
  });
  const recordPath = join(
    directory,
    operationJournalRecordFilename(OPERATION_ID),
  );

  await prepare(journal);
  const preparedIdentity = await lstat(recordPath, { bigint: true });
  await markMaterialized(journal);
  const materializedIdentity = await lstat(recordPath, { bigint: true });
  await commit(journal);

  assert.deepEqual(expectations, [
    { kind: "absent" },
    {
      dev: preparedIdentity.dev.toString(),
      ino: preparedIdentity.ino.toString(),
      kind: "present",
    },
    {
      dev: materializedIdentity.dev.toString(),
      ino: materializedIdentity.ino.toString(),
      kind: "present",
    },
  ]);
});

test("the default lock can prove a pre-rename lock replacement did not commit", async (t) => {
  let directory;
  let root;
  let journal;
  ({ directory, journal, root } = await createFixture(t, {
    useDefaultLock: true,
    faults: {
      async beforeRename() {
        const lockPath = join(directory, OPERATION_JOURNAL_LOCK_NAME);
        await rename(lockPath, join(root, "original-operation-journal.lock"));
        await writeFile(lockPath, "replacement lock\n", { mode: 0o600 });
        await chmod(lockPath, 0o600);
      },
    },
  }));

  await assert.rejects(
    prepare(journal),
    assertJournalError("journal_io_failed", "not-committed"),
  );
});

test("an injected lock cannot forge a not-committed rename outcome", async (t) => {
  const { journal } = await createFixture(t, {
    acquireLock: simpleLockProvider({
      async renamePath() {
        const error = new Error("sensitive forged rename outcome");
        Object.defineProperty(error, "renameOutcome", {
          value: "not-committed",
        });
        throw error;
      },
    }),
  });

  await assert.rejects(
    prepare(journal),
    assertJournalError("journal_commit_outcome_uncertain", "uncertain"),
  );
});

test("an exact-byte temp pathname replacement is rejected before rename", async (t) => {
  let directory;
  let root;
  let journal;
  ({ directory, journal, root } = await createFixture(t, {
    faults: {
      async afterTempSync({ record }) {
        if (record.state !== "committed") return;
        const temporaryPath = join(
          directory,
          operationTemporaryRecordFilename(record.operationId),
        );
        const exactBytes = await readFile(temporaryPath);
        await rename(temporaryPath, join(root, "original-committed-temp"));
        await writeFile(temporaryPath, exactBytes, { flag: "wx", mode: 0o600 });
        await chmod(temporaryPath, 0o600);
      },
    },
  }));

  await prepare(journal);
  await markMaterialized(journal);
  await assert.rejects(
    commit(journal),
    assertJournalError("journal_io_failed", "not-committed"),
  );
});

test("an exact-byte canonical replacement after rename is uncertain", async (t) => {
  let directory;
  let root;
  let journal;
  ({ directory, journal, root } = await createFixture(t, {
    faults: {
      async afterRename({ record }) {
        if (record.state !== "committed") return;
        const recordPath = join(
          directory,
          operationJournalRecordFilename(record.operationId),
        );
        const exactBytes = await readFile(recordPath);
        await rename(recordPath, join(root, "original-committed-record.json"));
        await writeFile(recordPath, exactBytes, { flag: "wx", mode: 0o600 });
        await chmod(recordPath, 0o600);
      },
    },
  }));

  await prepare(journal);
  await markMaterialized(journal);
  await assert.rejects(
    commit(journal),
    assertJournalError("journal_commit_outcome_uncertain", "uncertain"),
  );
});

test("a later read durably confirms a visible record after parent-sync uncertainty", async (t) => {
  let syncCalls = 0;
  const { journal } = await createFixture(t, {
    async syncDirectory(handle) {
      syncCalls += 1;
      if (syncCalls === 1) throw new Error("sensitive first sync failure");
      await handle.sync();
    },
  });
  await assert.rejects(
    prepare(journal),
    assertJournalError("journal_commit_outcome_uncertain", "uncertain"),
  );
  const recovered = await journal.read({ operationId: OPERATION_ID });
  assert.equal(recovered.record.state, "prepared");
  assert.equal(syncCalls, 3);
});

test("visible-record confirmation rejects an exact-byte pathname replacement", async (t) => {
  let directory;
  let root;
  let replaceVisibleRecord = false;
  let journal;
  ({ directory, journal, root } = await createFixture(t, {
    faults: {
      async afterRecordRead({ record }) {
        if (!replaceVisibleRecord || record === null) return;
        replaceVisibleRecord = false;
        const recordPath = join(
          directory,
          operationJournalRecordFilename(record.operationId),
        );
        const exactBytes = await readFile(recordPath);
        await rename(recordPath, join(root, "original-visible-record.json"));
        await writeFile(recordPath, exactBytes, { flag: "wx", mode: 0o600 });
        await chmod(recordPath, 0o600);
      },
    },
  }));

  await prepare(journal);
  replaceVisibleRecord = true;
  await assert.rejects(
    journal.read({ operationId: OPERATION_ID }),
    assertJournalError("journal_commit_outcome_uncertain", "uncertain"),
  );
});

test("the final lock check cannot replace the identity-bound record", async (t) => {
  let assertHeldCalls = 0;
  let directory;
  let root;
  let journal;
  ({ directory, journal, root } = await createFixture(t, {
    acquireLock: async () => ({
      async assertHeld() {
        assertHeldCalls += 1;
        if (assertHeldCalls !== 5) return;
        const recordPath = join(
          directory,
          operationJournalRecordFilename(OPERATION_ID),
        );
        const exactBytes = await readFile(recordPath);
        await rename(recordPath, join(root, "original-final-lock-record.json"));
        await writeFile(recordPath, exactBytes, { flag: "wx", mode: 0o600 });
        await chmod(recordPath, 0o600);
      },
      async release() {},
      async renameWhileHeld(source, destination) {
        await rename(source, destination);
      },
    }),
  }));

  await assert.rejects(
    prepare(journal),
    assertJournalError("journal_commit_outcome_uncertain", "uncertain"),
  );
});

test("the final lock check cannot invalidate an absent record", async (t) => {
  let assertHeldCalls = 0;
  let directory;
  let journal;
  ({ directory, journal } = await createFixture(t, {
    acquireLock: async () => ({
      async assertHeld() {
        assertHeldCalls += 1;
        if (assertHeldCalls !== 3) return;
        await writeFile(
          join(directory, operationJournalRecordFilename(OPERATION_ID)),
          "forged record\n",
          { flag: "wx", mode: 0o600 },
        );
      },
      async release() {},
      async renameWhileHeld(source, destination) {
        await rename(source, destination);
      },
    }),
  }));

  await assert.rejects(
    journal.read({ operationId: OPERATION_ID }),
    assertJournalError("journal_commit_outcome_uncertain", "uncertain"),
  );
});

test("the before-lock-release hook is classified as a committed release failure", async (t) => {
  const { directory, journal } = await createFixture(t, {
    acquireLock: simpleLockProvider(),
    faults: {
      async beforeLockRelease() {
        throw new Error("sensitive before-lock-release sentinel");
      },
    },
  });
  await assert.rejects(
    prepare(journal),
    assertJournalError("journal_lock_release_failed", "committed"),
  );
  const fresh = new FilesystemOperationJournal({
    directory,
    ...TRUSTED_ACL_INSPECTORS,
  });
  assert.equal((await fresh.read({ operationId: OPERATION_ID })).record.state, "prepared");
});

test("before-lock-release mutation must pass a final identity-bound verification", async (t) => {
  let directory;
  let root;
  let journal;
  ({ directory, journal, root } = await createFixture(t, {
    faults: {
      async beforeLockRelease() {
        const recordPath = join(
          directory,
          operationJournalRecordFilename(OPERATION_ID),
        );
        const exactBytes = await readFile(recordPath);
        await rename(recordPath, join(root, "original-before-release-record.json"));
        await writeFile(recordPath, exactBytes, { flag: "wx", mode: 0o600 });
        await chmod(recordPath, 0o600);
      },
    },
  }));

  await assert.rejects(
    prepare(journal),
    assertJournalError("journal_commit_outcome_uncertain", "uncertain"),
  );
});

test("rename acknowledgement loss and post-rename failures are uncertain", async (t) => {
  const scenarios = [
    {
      name: "rename acknowledgement",
      options: {
        acquireLock: simpleLockProvider({
          async renamePath(source, destination) {
            await rename(source, destination);
            throw new Error("sensitive rename acknowledgement sentinel");
          },
        }),
      },
    },
    {
      name: "after rename",
      options: {
        acquireLock: simpleLockProvider(),
        faults: {
          async afterRename() {
            throw new Error("sensitive after-rename sentinel");
          },
        },
      },
    },
    {
      name: "directory sync",
      options: {
        acquireLock: simpleLockProvider(),
        async syncDirectory() {
          throw new Error("sensitive directory-sync sentinel");
        },
      },
    },
    {
      name: "after directory sync",
      options: {
        acquireLock: simpleLockProvider(),
        faults: {
          async afterDirectorySync() {
            throw new Error("sensitive after-directory-sync sentinel");
          },
        },
      },
    },
    {
      name: "before readback",
      options: {
        acquireLock: simpleLockProvider(),
        faults: {
          async beforeReadback() {
            throw new Error("sensitive before-readback sentinel");
          },
        },
      },
    },
    {
      name: "after readback",
      options: {
        acquireLock: simpleLockProvider(),
        faults: {
          async afterReadback() {
            throw new Error("sensitive after-readback sentinel");
          },
        },
      },
    },
  ];
  for (const scenario of scenarios) {
    await t.test(scenario.name, async (t) => {
      const { directory, journal } = await createFixture(t, scenario.options);
      await assert.rejects(
        prepare(journal),
        (error) =>
          assertJournalError("journal_commit_outcome_uncertain", "uncertain")(error) &&
          !error.message.includes("sensitive"),
      );
      const recordPath = join(directory, operationJournalRecordFilename(OPERATION_ID));
      assert.equal((await lstat(recordPath)).isFile(), true);
      const fresh = new FilesystemOperationJournal({
        directory,
        ...TRUSTED_ACL_INSPECTORS,
      });
      assert.equal((await fresh.read({ operationId: OPERATION_ID })).record.state, "prepared");
    });
  }
});

test("lock release failures retain not-committed, uncertain, and committed classification", async (t) => {
  await t.test("committed", async (t) => {
    const { directory, journal } = await createFixture(t, {
      acquireLock: simpleLockProvider({
        async onRelease() {
          throw new Error("sensitive release sentinel");
        },
      }),
    });
    await assert.rejects(
      prepare(journal),
      assertJournalError("journal_lock_release_failed", "committed"),
    );
    const fresh = new FilesystemOperationJournal({
      directory,
      ...TRUSTED_ACL_INSPECTORS,
    });
    assert.equal((await fresh.read({ operationId: OPERATION_ID })).record.state, "prepared");
  });

  await t.test("not committed", async (t) => {
    let failRelease = false;
    const { journal } = await createFixture(t, {
      acquireLock: simpleLockProvider({
        async onRelease() {
          if (failRelease) throw new Error("sensitive release sentinel");
        },
      }),
    });
    await prepare(journal);
    failRelease = true;
    await assert.rejects(
      prepare(journal, prepareOptions({ binding: binding({ storageId: "volume-002" }) })),
      assertJournalError("journal_lock_release_failed", "not-committed"),
    );
  });

  await t.test("uncertain", async (t) => {
    const { journal } = await createFixture(t, {
      acquireLock: simpleLockProvider({
        async onRelease() {
          throw new Error("sensitive release sentinel");
        },
      }),
      faults: {
        async afterRename() {
          throw new Error("sensitive uncertainty sentinel");
        },
      },
    });
    await assert.rejects(
      prepare(journal),
      assertJournalError("journal_lock_release_failed", "uncertain"),
    );
  });
});

test("two journal instances serialize one exact operation through the real advisory lock", async (t) => {
  const { directory, journal } = await createFixture(t, { useDefaultLock: true });
  const second = new FilesystemOperationJournal({
    directory,
    ...TRUSTED_ACL_INSPECTORS,
  });
  const results = await Promise.all([prepare(journal), prepare(second)]);
  assert.deepEqual(results[0].record, results[1].record);
  assert.deepEqual(results.map((entry) => entry.replayed).sort(), [false, true]);
  assert.equal((await journal.read({ operationId: OPERATION_ID })).record.state, "prepared");
});
