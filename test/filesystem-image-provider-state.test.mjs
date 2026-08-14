import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import {
  appendFile,
  chmod,
  link,
  lstat,
  mkdir,
  mkdtemp,
  readFile,
  rename,
  rm,
  symlink,
  truncate,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import test from "node:test";
import { promisify } from "node:util";

import {
  FILESYSTEM_IMAGE_PROVIDER_STATE_CONTRACT_VERSION,
  FILESYSTEM_IMAGE_PROVIDER_STATE_HEAD_CONTRACT_VERSION,
  FILESYSTEM_IMAGE_PROVIDER_STATE_LEDGER_NAME,
  FILESYSTEM_IMAGE_PROVIDER_STATE_LOCK_NAME,
  FilesystemImageProviderState,
  FilesystemImageProviderStateError,
  normalizeFilesystemImageProviderStateHead,
} from "../src/filesystem-image-provider-state.mjs";

const TRUSTED_ACL_INSPECTORS = Object.freeze({
  inspectAncestorAcl: async () => false,
  inspectDirectoryAcl: async () => false,
});
const execFileAsync = promisify(execFile);

function stateError(code) {
  return (error) =>
    error instanceof FilesystemImageProviderStateError &&
    error.code === code &&
    error.retryable === false &&
    error.commitState ===
      (code === "commit_outcome_uncertain" ? "uncertain" : "not-committed") &&
    Object.isFrozen(error);
}

function promiseSettlementCases(resolvedValue) {
  const cases = [
    {
      name: "ordinary thenable",
      create() {
        let executions = 0;
        return {
          assertUntouched: () => assert.equal(executions, 0),
          value: {
            then() {
              executions += 1;
              throw new Error("ordinary thenable must not execute");
            },
          },
        };
      },
    },
    {
      name: "Promise Proxy",
      create() {
        let executions = 0;
        const trap = () => {
          executions += 1;
          throw new Error("Promise Proxy trap must not execute");
        };
        return {
          assertUntouched: () => assert.equal(executions, 0),
          value: new Proxy(Promise.resolve(resolvedValue), {
            get: trap,
            getOwnPropertyDescriptor: trap,
            getPrototypeOf: trap,
          }),
        };
      },
    },
    {
      name: "Promise subclass",
      create() {
        let executions = 0;
        class SettlementPromise extends Promise {
          then(...args) {
            executions += 1;
            return super.then(...args);
          }
        }
        return {
          assertUntouched: () => assert.equal(executions, 0),
          value: SettlementPromise.resolve(resolvedValue),
        };
      },
    },
  ];
  for (const key of ["then", "catch", "finally", "constructor"]) {
    cases.push({
      name: `own ${key} accessor`,
      create() {
        let executions = 0;
        const value = Promise.resolve(resolvedValue);
        Object.defineProperty(value, key, {
          configurable: true,
          get() {
            executions += 1;
            throw new Error(`own ${key} accessor must not execute`);
          },
        });
        return {
          assertUntouched: () => assert.equal(executions, 0),
          value,
        };
      },
    });
  }
  return cases;
}

function createSerializedLockProvider(tracker = {}) {
  let held = false;
  tracker.active = 0;
  tracker.maxActive = 0;
  tracker.acquisitions = 0;
  return async () => {
    if (held) {
      const error = new Error("busy");
      error.code = "lock_unavailable";
      throw error;
    }
    held = true;
    tracker.active += 1;
    tracker.maxActive = Math.max(tracker.maxActive, tracker.active);
    tracker.acquisitions += 1;
    let released = false;
    return {
      async assertHeld() {
        if (!held || released) throw new Error("lock lost");
      },
      async release() {
        if (released) throw new Error("double release");
        released = true;
        held = false;
        tracker.active -= 1;
      },
    };
  };
}

function copyLedgerHead(head) {
  return {
    contractVersion: head.contractVersion,
    sequence: head.sequence,
    lastChecksum: head.lastChecksum,
    ledgerBytes: head.ledgerBytes,
  };
}

function sameLedgerHead(left, right) {
  return (
    left.contractVersion === right.contractVersion &&
    left.sequence === right.sequence &&
    left.lastChecksum === right.lastChecksum &&
    left.ledgerBytes === right.ledgerBytes
  );
}

function createTrustedHeadAnchor(tracker = {}) {
  let head = normalizeFilesystemImageProviderStateHead({
    contractVersion: FILESYSTEM_IMAGE_PROVIDER_STATE_HEAD_CONTRACT_VERSION,
    sequence: 0,
    lastChecksum: null,
    ledgerBytes: 0,
  });
  tracker.reads = 0;
  tracker.advances = 0;
  tracker.head = copyLedgerHead(head);
  return Object.freeze({
    async readHead() {
      tracker.reads += 1;
      if (tracker.failRead === true) throw new Error("trusted anchor unavailable");
      return copyLedgerHead(head);
    },
    async compareAndAdvance({ expectedHead, nextHead }) {
      tracker.advances += 1;
      if (!sameLedgerHead(head, expectedHead)) return false;
      if (tracker.failAdvanceBeforeCommit === true) {
        throw new Error("trusted anchor unavailable");
      }
      head = normalizeFilesystemImageProviderStateHead(nextHead);
      tracker.head = copyLedgerHead(head);
      if (tracker.loseNextAdvanceAcknowledgement === true) {
        tracker.loseNextAdvanceAcknowledgement = false;
        throw new Error("trusted anchor acknowledgement lost");
      }
      return true;
    },
  });
}

function createFixedTrustedHeadAnchor(value) {
  const head = normalizeFilesystemImageProviderStateHead(value);
  return Object.freeze({
    async readHead() {
      return copyLedgerHead(head);
    },
    async compareAndAdvance() {
      return false;
    },
  });
}

function corruptPreviousChecksumWithValidEnvelope(ledger, frameStart) {
  const payloadLength = ledger.readUInt32BE(frameStart + 8);
  const sequence = ledger.readUInt32BE(frameStart + 12);
  const payloadStart = frameStart + 48;
  const footerStart = payloadStart + payloadLength;
  const previousChecksumMarker = Buffer.from('"previousChecksum":"', "utf8");
  const markerOffset = ledger.indexOf(previousChecksumMarker, payloadStart);
  assert(markerOffset >= payloadStart && markerOffset < footerStart);
  const checksumValueOffset = markerOffset + previousChecksumMarker.length;
  ledger[checksumValueOffset] =
    ledger[checksumValueOffset] === 0x30 ? 0x31 : 0x30;

  const checksumMetadata = Buffer.allocUnsafe(8);
  checksumMetadata.writeUInt32BE(payloadLength, 0);
  checksumMetadata.writeUInt32BE(sequence, 4);
  const checksum = createHash("sha256")
    .update(
      Buffer.from(
        "portable-codex/filesystem-image-provider-state/frame/v1\0",
        "utf8",
      ),
    )
    .update(checksumMetadata)
    .update(ledger.subarray(payloadStart, footerStart))
    .digest();
  checksum.copy(ledger, frameStart + 16);
  checksum.copy(ledger, footerStart + 16);
}

async function createFixture(t, options = {}) {
  const root = await mkdtemp(join(tmpdir(), "filesystem-image-provider-state-test-"));
  const directory = join(root, "state");
  await mkdir(directory, { mode: 0o700 });
  t.after(() => rm(root, { force: true, recursive: true }));
  const tracker = {};
  const headAnchorTracker = {};
  const acquireLock = options.acquireLock ?? createSerializedLockProvider(tracker);
  const headAnchor =
    options.headAnchor ?? createTrustedHeadAnchor(headAnchorTracker);
  const createState = (overrides = {}) =>
    new FilesystemImageProviderState({
      acquireLock,
      directory,
      headAnchor,
      ...TRUSTED_ACL_INSPECTORS,
      ...overrides,
    });
  return {
    acquireLock,
    createState,
    directory,
    headAnchor,
    headAnchorTracker,
    ledgerPath: join(directory, FILESYSTEM_IMAGE_PROVIDER_STATE_LEDGER_NAME),
    lockPath: join(directory, FILESYSTEM_IMAGE_PROVIDER_STATE_LOCK_NAME),
    root,
    state: createState(),
    tracker,
  };
}

function physicalIdentity({
  filesystemId = "ext4fs:11111111-1111-4111-8111-111111111111",
  objectId = "ext4fh1:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
} = {}) {
  return {
    filesystemId,
    objectIdentityScheme: "linux-ext4-file-handle-sha256-v1",
    objectId,
  };
}

function mount(storageId = "storage-001") {
  return {
    mountPath: `/var/lib/portable-codex/mounts/${storageId}`,
    imageIdentity: physicalIdentity({
      filesystemId: "hostfs:aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
      objectId: `ext4fh1:image-${storageId}`,
    }),
    rootIdentity: physicalIdentity({ objectId: `ext4fh1:mount-${storageId}` }),
  };
}

function attachment(fixedMount = mount(), overrides = {}) {
  return {
    attachmentId: "attachment-001",
    leaseId: "lease-001",
    holderId: "holder-001",
    fencingEpoch: "1",
    rootPath: join(fixedMount.mountPath, "attachment-001"),
    proofId: "proof-001",
    imageIdentity: fixedMount.imageIdentity,
    rootIdentity: physicalIdentity({ objectId: "ext4fh1:attachment-001" }),
    ...overrides,
  };
}

function dataRootFromAttachment(fixedAttachment) {
  return {
    rootPath: fixedAttachment.rootPath,
    imageIdentity: fixedAttachment.imageIdentity,
    rootIdentity: fixedAttachment.rootIdentity,
  };
}

function writerAuthorityFromAttachment(fixedAttachment) {
  return {
    fencingEpoch: fixedAttachment.fencingEpoch,
    holderId: fixedAttachment.holderId,
    leaseId: fixedAttachment.leaseId,
  };
}

function publicationControlIdentity(fixedMount = mount()) {
  return physicalIdentity({
    filesystemId: fixedMount.rootIdentity.filesystemId,
    objectIdentityScheme: fixedMount.rootIdentity.objectIdentityScheme,
    objectId: "ext4fh1:publication-control",
  });
}

function storageState(options = {}) {
  const {
    attachment: fixedAttachment = null,
    lifecycle = "provisioned",
    mount: fixedMount = mount(),
    revision = "1",
    storageId = "storage-001",
    writerEpoch = "0",
  } = options;
  const fixedDataRoot = Object.hasOwn(options, "dataRoot")
    ? options.dataRoot
    : fixedAttachment === null
      ? null
      : dataRootFromAttachment(fixedAttachment);
  const writerAuthority = Object.hasOwn(options, "writerAuthority")
    ? options.writerAuthority
    : fixedAttachment === null
      ? null
      : writerAuthorityFromAttachment(fixedAttachment);
  const fixedPublicationControlIdentity = Object.hasOwn(
    options,
    "publicationControlIdentity",
  )
    ? options.publicationControlIdentity
    : lifecycle === "destroyed"
      ? null
      : publicationControlIdentity(fixedMount);
  return {
    storageId,
    sessionId: `session-${storageId}`,
    backendId: "filesystem-image-ext4",
    filesystemId: "ext4fs:11111111-1111-4111-8111-111111111111",
    imagePath: `/var/lib/portable-codex/images/${storageId}.img`,
    lifecycle,
    revision,
    writerEpoch,
    writerAuthority,
    mount: lifecycle === "destroyed" ? null : fixedMount,
    publicationControlIdentity: fixedPublicationControlIdentity,
    dataRoot: lifecycle === "destroyed" ? null : fixedDataRoot,
    attachment: fixedAttachment,
  };
}

function operationRequest(kind, storageId = "storage-001", overrides = {}) {
  return {
    contractVersion: 1,
    kind,
    storageId,
    marker: { lane: "provider" },
    ...overrides,
  };
}

async function prepareAndCommit(
  state,
  {
    kind = "provision",
    operationId = "operation-provision-001",
    request = operationRequest(kind),
    result = { proofId: `result-${operationId}`, status: `${kind}-complete` },
    storage = storageState(),
  } = {},
) {
  const prepared = await state.prepareOperation({
    kind,
    operationId,
    request,
    storageId: storage.storageId,
  });
  const committed = await state.commitOperation({
    operationId,
    request,
    result,
    storageState: storage,
  });
  return { committed, prepared };
}

test("replays committed operations and complete mount state after restart", async (t) => {
  const fixture = await createFixture(t);
  const request = operationRequest("provision");
  const result = { proofId: "provision-proof-001", status: "provisioned" };
  const storage = storageState();

  const prepared = await fixture.state.prepareOperation({
    kind: "provision",
    operationId: "operation-provision-001",
    request,
    storageId: storage.storageId,
  });
  assert.equal(prepared.state, "prepared");
  assert.equal(prepared.shouldDispatch, true);
  assert.equal(prepared.replayed, false);
  assert.equal(prepared.storageStateBefore, null);
  assert.equal(prepared.currentStorageState, null);

  const committed = await fixture.state.commitOperation({
    operationId: "operation-provision-001",
    request,
    result,
    storageState: storage,
  });
  assert.equal(committed.state, "committed");
  assert.equal(committed.replayed, false);
  assert.deepEqual(committed.result, result);
  assert.deepEqual(committed.storageState, storage);

  const restarted = fixture.createState();
  const replayed = await restarted.readOperation({
    operationId: "operation-provision-001",
    request,
  });
  assert.equal(replayed.state, "committed");
  assert.deepEqual(replayed.result, result);
  assert.deepEqual(await restarted.readStorage(storage.storageId), storage);

  const snapshotPromise = restarted.snapshot();
  assert.equal(snapshotPromise instanceof Promise, true);
  const snapshot = await snapshotPromise;
  assert.equal(snapshot.contractVersion, FILESYSTEM_IMAGE_PROVIDER_STATE_CONTRACT_VERSION);
  assert.equal(snapshot.sequence, 2);
  assert.equal(snapshot.operations.length, 1);
  assert.equal(snapshot.storages.length, 1);
  assert.deepEqual(snapshot.storages[0], storage);

  const ledgerMetadata = await lstat(fixture.ledgerPath, { bigint: true });
  const lockMetadata = await lstat(fixture.lockPath, { bigint: true });
  for (const metadata of [ledgerMetadata, lockMetadata]) {
    assert.equal(metadata.isFile(), true);
    assert.equal(metadata.nlink, 1n);
    assert.equal(Number(metadata.mode & 0o7777n), 0o600);
  }
});

test("provision cannot smuggle a lease-independent data root into revision one", async (t) => {
  const fixture = await createFixture(t);
  const request = operationRequest("provision");
  const fixedMount = mount();
  const injectedAttachment = attachment(fixedMount);
  await fixture.state.prepareOperation({
    kind: "provision",
    operationId: "operation-provision-data-root-001",
    request,
    storageId: "storage-001",
  });

  await assert.rejects(
    fixture.state.commitOperation({
      operationId: "operation-provision-data-root-001",
      request,
      result: { status: "provisioned" },
      storageState: storageState({
        dataRoot: dataRootFromAttachment(injectedAttachment),
        mount: fixedMount,
      }),
    }),
    stateError("operation_conflict"),
  );
});

test("anchors the publication control identity at provision and preserves it", async (t) => {
  const fixture = await createFixture(t);
  const provisionRequest = operationRequest("provision");
  await fixture.state.prepareOperation({
    kind: "provision",
    operationId: "operation-provision-control-missing",
    request: provisionRequest,
    storageId: "storage-001",
  });
  await assert.rejects(
    fixture.state.commitOperation({
      operationId: "operation-provision-control-missing",
      request: provisionRequest,
      result: { status: "provisioned" },
      storageState: storageState({ publicationControlIdentity: null }),
    }),
    stateError("invalid_request"),
  );

  const storageId = "storage-002";
  const fixedMount = mount(storageId);
  const provisioned = storageState({ mount: fixedMount, storageId });
  await prepareAndCommit(fixture.state, {
    operationId: "operation-provision-control-002",
    request: operationRequest("provision", storageId),
    storage: provisioned,
  });
  const attachRequest = operationRequest("attach", storageId);
  await fixture.state.prepareOperation({
    kind: "attach",
    operationId: "operation-attach-control-replaced",
    request: attachRequest,
    storageId,
  });
  const fixedAttachment = attachment(fixedMount);
  await assert.rejects(
    fixture.state.commitOperation({
      operationId: "operation-attach-control-replaced",
      request: attachRequest,
      result: { status: "attached" },
      storageState: storageState({
        attachment: fixedAttachment,
        lifecycle: "attached",
        mount: fixedMount,
        publicationControlIdentity: physicalIdentity({
          filesystemId: fixedMount.rootIdentity.filesystemId,
          objectIdentityScheme:
            fixedMount.rootIdentity.objectIdentityScheme,
          objectId: "ext4fh1:replacement-publication-control",
        }),
        revision: "2",
        storageId,
        writerEpoch: "1",
      }),
    }),
    stateError("operation_conflict"),
  );
});

test("exact committed replay returns the original result without appending", async (t) => {
  const fixture = await createFixture(t);
  const request = operationRequest("provision");
  const original = await prepareAndCommit(fixture.state, {
    request,
    result: { proofId: "original-proof", status: "provisioned" },
  });
  const before = await readFile(fixture.ledgerPath);

  const replayed = await fixture.state.commitOperation({
    operationId: "operation-provision-001",
    request,
    result: { proofId: "different-retry-proof", status: "provisioned" },
    storageState: storageState(),
  });

  assert.equal(replayed.replayed, true);
  assert.equal(replayed.shouldDispatch, false);
  assert.deepEqual(replayed.result, original.committed.result);
  assert.deepEqual(replayed.storageState, original.committed.storageState);
  assert.deepEqual(await readFile(fixture.ledgerPath), before);
});

test("expected storage state is checked under the append lock only for new operations", async (t) => {
  const fixture = await createFixture(t);
  const current = storageState();
  await prepareAndCommit(fixture.state, { storage: current });
  const beforeMismatch = await readFile(fixture.ledgerPath);

  await assert.rejects(
    fixture.state.prepareOperation({
      kind: "checkpoint",
      operationId: "operation-stale-before-append-001",
      request: operationRequest("checkpoint"),
      storageId: current.storageId,
      expectedStorageState: null,
    }),
    stateError("operation_conflict"),
  );
  assert.deepEqual(await readFile(fixture.ledgerPath), beforeMismatch);

  const first = fixture.createState();
  const second = fixture.createState();
  await Promise.all([
    first.readStorage(current.storageId),
    second.readStorage(current.storageId),
  ]);
  const request = operationRequest("checkpoint", current.storageId, {
    generation: 2,
  });
  const prepared = await first.prepareOperation({
    kind: "checkpoint",
    operationId: "operation-expected-current-001",
    request,
    storageId: current.storageId,
    expectedStorageState: current,
  });
  assert.equal(prepared.shouldDispatch, true);

  const afterPrepared = await readFile(fixture.ledgerPath);
  const preparedReplay = await second.prepareOperation({
    kind: "checkpoint",
    operationId: "operation-expected-current-001",
    request,
    storageId: current.storageId,
    expectedStorageState: null,
  });
  assert.equal(preparedReplay.replayed, true);
  assert.equal(preparedReplay.state, "prepared");
  assert.deepEqual(await readFile(fixture.ledgerPath), afterPrepared);

  const next = storageState({ revision: "2" });
  const commitPromise = first.commitOperation({
    operationId: "operation-expected-current-001",
    request,
    result: { status: "checkpointed" },
    storageState: next,
  });
  const stalePreparePromise = assert.rejects(
    second.prepareOperation({
      kind: "checkpoint",
      operationId: "operation-concurrent-stale-001",
      request: operationRequest("checkpoint", current.storageId, {
        generation: 3,
      }),
      storageId: current.storageId,
      expectedStorageState: current,
    }),
    stateError("operation_conflict"),
  );
  await commitPromise;
  const afterCommit = await readFile(fixture.ledgerPath);
  await stalePreparePromise;
  assert.deepEqual(await readFile(fixture.ledgerPath), afterCommit);
  assert.equal(fixture.tracker.maxActive, 1);

  const committedReplay = await second.prepareOperation({
    kind: "checkpoint",
    operationId: "operation-expected-current-001",
    request,
    storageId: current.storageId,
    expectedStorageState: null,
  });
  assert.equal(committedReplay.replayed, true);
  assert.equal(committedReplay.state, "committed");
  assert.deepEqual(await readFile(fixture.ledgerPath), afterCommit);
});

test("conflicting operation ID reuse fails closed", async (t) => {
  const { state } = await createFixture(t);
  await state.prepareOperation({
    kind: "provision",
    operationId: "operation-conflict-001",
    request: operationRequest("provision"),
    storageId: "storage-001",
  });

  await assert.rejects(
    state.prepareOperation({
      kind: "provision",
      operationId: "operation-conflict-001",
      request: operationRequest("provision", "storage-001", { marker: { lane: "other" } }),
      storageId: "storage-001",
    }),
    stateError("operation_conflict"),
  );
  await assert.rejects(
    state.commitOperation({
      operationId: "operation-conflict-001",
      request: operationRequest("provision", "storage-001", { marker: { lane: "other" } }),
      result: { status: "provisioned" },
      storageState: storageState(),
    }),
    stateError("operation_conflict"),
  );
});

test("prepared replay preserves ambiguity and never grants blind redispatch", async (t) => {
  const fixture = await createFixture(t);
  const request = operationRequest("provision");
  const first = await fixture.state.prepareOperation({
    kind: "provision",
    operationId: "operation-ambiguous-001",
    request,
    storageId: "storage-001",
  });
  const afterFirst = await readFile(fixture.ledgerPath);
  const replay = await fixture.state.prepareOperation({
    kind: "provision",
    operationId: "operation-ambiguous-001",
    request,
    storageId: "storage-001",
  });

  assert.equal(first.shouldDispatch, true);
  assert.equal(replay.state, "prepared");
  assert.equal(replay.shouldDispatch, false);
  assert.equal(replay.replayed, true);
  assert.deepEqual(await readFile(fixture.ledgerPath), afterFirst);

  const restarted = fixture.createState();
  const durable = await restarted.readOperation({
    operationId: "operation-ambiguous-001",
  });
  assert.equal(durable.state, "prepared");
  assert.equal(durable.storageStateBefore, null);
  assert.equal(durable.currentStorageState, null);

  await assert.rejects(
    restarted.prepareOperation({
      kind: "provision",
      operationId: "operation-competing-001",
      request: operationRequest("provision"),
      storageId: "storage-001",
    }),
    stateError("operation_already_prepared"),
  );
});

test("concurrent instances serialize provider-state calls", async (t) => {
  const fixture = await createFixture(t);
  const first = fixture.createState();
  const second = fixture.createState();

  await Promise.all(
    [
      [first, "storage-a", "operation-a"],
      [second, "storage-b", "operation-b"],
    ].map(async ([state, storageId, operationId]) => {
      const request = operationRequest("provision", storageId);
      await prepareAndCommit(state, {
        operationId,
        request,
        storage: storageState({
          mount: mount(storageId),
          storageId,
        }),
      });
    }),
  );

  assert.equal(fixture.tracker.maxActive, 1);
  const snapshot = await fixture.state.snapshot();
  assert.equal(snapshot.sequence, 4);
  assert.deepEqual(
    snapshot.storages.map((storage) => storage.storageId),
    ["storage-a", "storage-b"],
  );
});

test("default runtime uses the preprovisioned existing-only advisory lock", {
  skip: !["darwin", "linux"].includes(process.platform),
}, async (t) => {
  const root = await mkdtemp(join(tmpdir(), "filesystem-image-provider-default-lock-test-"));
  const directory = join(root, "state");
  await mkdir(directory, { mode: 0o700 });
  t.after(() => rm(root, { force: true, recursive: true }));
  const state = new FilesystemImageProviderState({
    directory,
    headAnchor: createTrustedHeadAnchor(),
    ...TRUSTED_ACL_INSPECTORS,
  });

  const snapshot = await state.snapshot();
  assert.equal(snapshot.sequence, 0);
  const lockMetadata = await lstat(
    join(directory, FILESYSTEM_IMAGE_PROVIDER_STATE_LOCK_NAME),
    { bigint: true },
  );
  assert.equal(lockMetadata.isFile(), true);
  assert.equal(lockMetadata.nlink, 1n);
  assert.equal(Number(lockMetadata.mode & 0o7777n), 0o600);
});

test("trusted external heads reject cold-start deletion, replacement, and rollback", async (t) => {
  await t.test("deleted ledger", async (t) => {
    const fixture = await createFixture(t);
    await prepareAndCommit(fixture.state);
    await rm(fixture.ledgerPath);

    await assert.rejects(
      fixture.createState().snapshot(),
      stateError("corrupt_ledger"),
    );
  });

  await t.test("empty replacement", async (t) => {
    const fixture = await createFixture(t);
    await prepareAndCommit(fixture.state);
    await rename(fixture.ledgerPath, join(fixture.directory, "rolled-forward.log"));
    await writeFile(fixture.ledgerPath, "", { mode: 0o600 });

    await assert.rejects(
      fixture.createState().snapshot(),
      stateError("corrupt_ledger"),
    );
  });

  await t.test("valid committed prefix rollback", async (t) => {
    const fixture = await createFixture(t);
    await prepareAndCommit(fixture.state);
    const committedPrefix = await readFile(fixture.ledgerPath);
    await prepareAndCommit(fixture.state, {
      kind: "checkpoint",
      operationId: "operation-checkpoint-rollback-001",
      request: operationRequest("checkpoint"),
      storage: storageState({ revision: "2" }),
    });
    await writeFile(fixture.ledgerPath, committedPrefix);

    await assert.rejects(
      fixture.createState().snapshot(),
      stateError("corrupt_ledger"),
    );
  });
});

test("cold restart truncates one complete prepared frame left before head CAS", async (t) => {
  const fixture = await createFixture(t);
  fixture.headAnchorTracker.failAdvanceBeforeCommit = true;
  await assert.rejects(
    fixture.state.prepareOperation({
      kind: "provision",
      operationId: "operation-full-prepared-before-cas-001",
      request: operationRequest("provision"),
      storageId: "storage-001",
    }),
    stateError("commit_outcome_uncertain"),
  );
  fixture.headAnchorTracker.failAdvanceBeforeCommit = false;
  const unanchored = await readFile(fixture.ledgerPath);
  assert(unanchored.length > 0);
  assert.equal(fixture.headAnchorTracker.head.sequence, 0);

  const restarted = fixture.createState();
  const snapshot = await restarted.snapshot();
  assert.equal(snapshot.sequence, 0);
  assert.deepEqual(snapshot.operations, []);
  assert.deepEqual(await readFile(fixture.ledgerPath), Buffer.alloc(0));
  assert.equal(
    await restarted.readOperation({
      operationId: "operation-full-prepared-before-cas-001",
    }),
    null,
  );
});

test("cold restart truncates one complete committed frame left before head CAS", async (t) => {
  const fixture = await createFixture(t);
  const request = operationRequest("provision");
  await fixture.state.prepareOperation({
    kind: "provision",
    operationId: "operation-full-commit-before-cas-001",
    request,
    storageId: "storage-001",
  });
  const anchoredPrepared = await readFile(fixture.ledgerPath);
  assert.equal(fixture.headAnchorTracker.head.sequence, 1);

  fixture.headAnchorTracker.failAdvanceBeforeCommit = true;
  await assert.rejects(
    fixture.state.commitOperation({
      operationId: "operation-full-commit-before-cas-001",
      request,
      result: { proofId: "proof-before-cas", status: "provisioned" },
      storageState: storageState(),
    }),
    stateError("commit_outcome_uncertain"),
  );
  fixture.headAnchorTracker.failAdvanceBeforeCommit = false;
  const unanchored = await readFile(fixture.ledgerPath);
  assert(unanchored.length > anchoredPrepared.length);
  assert.equal(fixture.headAnchorTracker.head.sequence, 1);

  const restarted = fixture.createState();
  const snapshot = await restarted.snapshot();
  assert.equal(snapshot.sequence, 1);
  assert.equal(snapshot.operations.length, 1);
  assert.equal(snapshot.operations[0].state, "prepared");
  assert.deepEqual(snapshot.storages, []);
  assert.deepEqual(await readFile(fixture.ledgerPath), anchoredPrepared);
});

test("cold restart rejects multiple or invalid unanchored frames without truncation", async (t) => {
  await t.test("multiple complete frames", async (t) => {
    const fixture = await createFixture(t);
    await prepareAndCommit(fixture.state);
    const before = await readFile(fixture.ledgerPath);
    const staleAnchor = createFixedTrustedHeadAnchor({
      contractVersion: FILESYSTEM_IMAGE_PROVIDER_STATE_HEAD_CONTRACT_VERSION,
      sequence: 0,
      lastChecksum: null,
      ledgerBytes: 0,
    });

    await assert.rejects(
      fixture.createState({ headAnchor: staleAnchor }).snapshot(),
      stateError("corrupt_ledger"),
    );
    assert.deepEqual(await readFile(fixture.ledgerPath), before);
  });

  await t.test("checksum-chain mismatch in one complete frame", async (t) => {
    const fixture = await createFixture(t);
    await prepareAndCommit(fixture.state);
    const anchored = await readFile(fixture.ledgerPath);
    fixture.headAnchorTracker.failAdvanceBeforeCommit = true;
    await assert.rejects(
      fixture.state.prepareOperation({
        kind: "checkpoint",
        operationId: "operation-invalid-unanchored-chain-001",
        request: operationRequest("checkpoint"),
        storageId: "storage-001",
      }),
      stateError("commit_outcome_uncertain"),
    );
    fixture.headAnchorTracker.failAdvanceBeforeCommit = false;
    const corrupted = await readFile(fixture.ledgerPath);
    corruptPreviousChecksumWithValidEnvelope(corrupted, anchored.length);
    await writeFile(fixture.ledgerPath, corrupted);
    const before = await readFile(fixture.ledgerPath);

    await assert.rejects(
      fixture.createState().snapshot(),
      stateError("corrupt_ledger"),
    );
    assert.deepEqual(await readFile(fixture.ledgerPath), before);
  });
});

test("recovers a safely truncated final frame under the lock", async (t) => {
  const fixture = await createFixture(t);
  await prepareAndCommit(fixture.state);
  const committedPrefix = await readFile(fixture.ledgerPath);
  fixture.headAnchorTracker.failAdvanceBeforeCommit = true;
  await assert.rejects(
    fixture.state.prepareOperation({
      kind: "checkpoint",
      operationId: "operation-torn-001",
      request: operationRequest("checkpoint"),
      storageId: "storage-001",
    }),
    stateError("commit_outcome_uncertain"),
  );
  fixture.headAnchorTracker.failAdvanceBeforeCommit = false;
  const withThirdFrame = await readFile(fixture.ledgerPath);
  assert(withThirdFrame.length > committedPrefix.length + 16);
  await truncate(fixture.ledgerPath, withThirdFrame.length - 13);

  const restarted = fixture.createState();
  const snapshot = await restarted.snapshot();
  assert.equal(snapshot.sequence, 2);
  assert.equal(snapshot.operations.length, 1);
  assert.deepEqual(await readFile(fixture.ledgerPath), committedPrefix);
  assert.equal(
    await restarted.readOperation({ operationId: "operation-torn-001" }),
    null,
  );
});

test("checksum corruption and non-frame garbage fail closed without truncation", async (t) => {
  await t.test("checksum", async (t) => {
    const fixture = await createFixture(t);
    await prepareAndCommit(fixture.state);
    const corrupted = await readFile(fixture.ledgerPath);
    corrupted[16] ^= 0xff;
    await writeFile(fixture.ledgerPath, corrupted);
    const before = await readFile(fixture.ledgerPath);

    await assert.rejects(fixture.createState().snapshot(), stateError("corrupt_ledger"));
    assert.deepEqual(await readFile(fixture.ledgerPath), before);
  });

  await t.test("garbage tail", async (t) => {
    const fixture = await createFixture(t);
    await prepareAndCommit(fixture.state);
    await appendFile(fixture.ledgerPath, Buffer.from("not-a-frame", "utf8"));
    const before = await readFile(fixture.ledgerPath);

    await assert.rejects(fixture.createState().snapshot(), stateError("corrupt_ledger"));
    assert.deepEqual(await readFile(fixture.ledgerPath), before);
  });

  await t.test("committed middle cannot masquerade as an incomplete tail", async (t) => {
    const fixture = await createFixture(t);
    await prepareAndCommit(fixture.state);
    const corrupted = await readFile(fixture.ledgerPath);
    // The first frame's claimed payload now extends beyond EOF. Its original
    // footer and the following committed frame remain as impossible sentinels,
    // so replay must reject rather than truncate the whole committed suffix.
    corrupted.writeUInt32BE(corrupted.length + 1_024, 8);
    await writeFile(fixture.ledgerPath, corrupted);
    const before = await readFile(fixture.ledgerPath);

    await assert.rejects(fixture.createState().snapshot(), stateError("corrupt_ledger"));
    assert.deepEqual(await readFile(fixture.ledgerPath), before);
  });

  await t.test("a complete chained payload with a torn footer still validates semantics", async (t) => {
    const fixture = await createFixture(t);
    await prepareAndCommit(fixture.state);
    const committedPrefix = await readFile(fixture.ledgerPath);
    await fixture.state.prepareOperation({
      kind: "checkpoint",
      operationId: "operation-corrupt-chain-001",
      request: operationRequest("checkpoint"),
      storageId: "storage-001",
    });
    const corrupted = await readFile(fixture.ledgerPath);
    const frameStart = committedPrefix.length;
    const payloadLength = corrupted.readUInt32BE(frameStart + 8);
    const sequence = corrupted.readUInt32BE(frameStart + 12);
    const payloadStart = frameStart + 48;
    const footerStart = payloadStart + payloadLength;
    const previousChecksumMarker = Buffer.from('"previousChecksum":"', "utf8");
    const markerOffset = corrupted.indexOf(previousChecksumMarker, payloadStart);
    assert(markerOffset >= payloadStart && markerOffset < footerStart);
    const checksumValueOffset = markerOffset + previousChecksumMarker.length;
    corrupted[checksumValueOffset] =
      corrupted[checksumValueOffset] === 0x30 ? 0x31 : 0x30;

    const checksumMetadata = Buffer.allocUnsafe(8);
    checksumMetadata.writeUInt32BE(payloadLength, 0);
    checksumMetadata.writeUInt32BE(sequence, 4);
    const payload = corrupted.subarray(payloadStart, footerStart);
    const checksum = createHash("sha256")
      .update(
        Buffer.from(
          "portable-codex/filesystem-image-provider-state/frame/v1\0",
          "utf8",
        ),
      )
      .update(checksumMetadata)
      .update(payload)
      .digest();
    checksum.copy(corrupted, frameStart + 16);
    const torn = corrupted.subarray(0, footerStart + 4);
    await writeFile(fixture.ledgerPath, torn);
    const before = await readFile(fixture.ledgerPath);

    await assert.rejects(fixture.createState().snapshot(), stateError("corrupt_ledger"));
    assert.deepEqual(await readFile(fixture.ledgerPath), before);
  });
});

test("reports an uncertain outcome when acknowledgement fails after frame fsync", async (t) => {
  const fixture = await createFixture(t);
  const request = operationRequest("provision");
  await fixture.state.prepareOperation({
    kind: "provision",
    operationId: "operation-uncertain-001",
    request,
    storageId: "storage-001",
  });
  const failingAcknowledger = fixture.createState({
    syncDirectory: async () => {
      throw new Error("directory acknowledgement unavailable");
    },
  });

  await assert.rejects(
    failingAcknowledger.commitOperation({
      operationId: "operation-uncertain-001",
      request,
      result: { proofId: "proof-visible", status: "provisioned" },
      storageState: storageState(),
    }),
    stateError("commit_outcome_uncertain"),
  );

  const visible = await fixture.createState().readOperation({
    operationId: "operation-uncertain-001",
    request,
  });
  assert.equal(visible.state, "committed");
  assert.deepEqual(visible.result, {
    proofId: "proof-visible",
    status: "provisioned",
  });
});

test("lost trusted-anchor acknowledgement is uncertain but exact replay resolves it", async (t) => {
  const fixture = await createFixture(t);
  const request = operationRequest("provision");
  await fixture.state.prepareOperation({
    kind: "provision",
    operationId: "operation-anchor-ack-loss-001",
    request,
    storageId: "storage-001",
  });
  fixture.headAnchorTracker.loseNextAdvanceAcknowledgement = true;

  await assert.rejects(
    fixture.state.commitOperation({
      operationId: "operation-anchor-ack-loss-001",
      request,
      result: { proofId: "proof-anchor-ack-loss", status: "provisioned" },
      storageState: storageState(),
    }),
    stateError("commit_outcome_uncertain"),
  );
  assert.equal(fixture.headAnchorTracker.head.sequence, 2);

  const beforeReplay = await readFile(fixture.ledgerPath);
  const replayed = await fixture.createState().prepareOperation({
    kind: "provision",
    operationId: "operation-anchor-ack-loss-001",
    request,
    storageId: "storage-001",
    expectedStorageState: null,
  });
  assert.equal(replayed.state, "committed");
  assert.equal(replayed.replayed, true);
  assert.deepEqual(await readFile(fixture.ledgerPath), beforeReplay);
});

test("rejects unsafe directories, symlinks, hard links, permissions, and replacement", async (t) => {
  await t.test("directory permissions", async (t) => {
    const fixture = await createFixture(t);
    await chmod(fixture.directory, 0o755);
    await assert.rejects(fixture.state.snapshot(), stateError("unsafe_directory"));
  });

  await t.test("ledger symlink", async (t) => {
    const fixture = await createFixture(t);
    const target = join(fixture.root, "target.log");
    await writeFile(target, "", { mode: 0o600 });
    await symlink(target, fixture.ledgerPath);
    await assert.rejects(fixture.state.snapshot(), stateError("corrupt_ledger"));
  });

  await t.test("ledger hard link", async (t) => {
    const fixture = await createFixture(t);
    const target = join(fixture.root, "target.log");
    await writeFile(target, "", { mode: 0o600 });
    await link(target, fixture.ledgerPath);
    await assert.rejects(fixture.state.snapshot(), stateError("corrupt_ledger"));
  });

  await t.test("ledger permissions", async (t) => {
    const fixture = await createFixture(t);
    await writeFile(fixture.ledgerPath, "", { mode: 0o644 });
    await assert.rejects(fixture.state.snapshot(), stateError("corrupt_ledger"));
  });

  await t.test("pinned ledger replacement", async (t) => {
    const fixture = await createFixture(t);
    await fixture.state.snapshot();
    await rename(fixture.ledgerPath, join(fixture.directory, "replaced.log"));
    await writeFile(fixture.ledgerPath, "", { mode: 0o600 });
    await assert.rejects(fixture.state.snapshot(), stateError("corrupt_ledger"));
  });
});

test("trusted head collaborators and values are exact, receiver-safe, and promise-bound", async (t) => {
  const canonical = normalizeFilesystemImageProviderStateHead({
    contractVersion: FILESYSTEM_IMAGE_PROVIDER_STATE_HEAD_CONTRACT_VERSION,
    sequence: 0,
    lastChecksum: null,
    ledgerBytes: 0,
  });
  assert.equal(Object.isFrozen(canonical), true);
  assert.throws(
    () =>
      normalizeFilesystemImageProviderStateHead({
        ...canonical,
        siblingLedgerPath: "/untrusted/state/head",
      }),
    stateError("invalid_request"),
  );

  const fixture = await createFixture(t);
  const baseOptions = {
    acquireLock: fixture.acquireLock,
    directory: fixture.directory,
    ...TRUSTED_ACL_INSPECTORS,
  };
  assert.throws(
    () => new FilesystemImageProviderState(baseOptions),
    stateError("invalid_request"),
  );

  let getterCalls = 0;
  const accessorAnchor = {
    compareAndAdvance: async () => true,
  };
  Object.defineProperty(accessorAnchor, "readHead", {
    enumerable: true,
    get() {
      getterCalls += 1;
      return async () => canonical;
    },
  });
  assert.throws(
    () =>
      new FilesystemImageProviderState({
        ...baseOptions,
        headAnchor: accessorAnchor,
      }),
    stateError("invalid_request"),
  );
  assert.equal(getterCalls, 0);

  let proxyTrapCalls = 0;
  const proxyAnchor = new Proxy(
    {
      readHead: async () => canonical,
      compareAndAdvance: async () => true,
    },
    {
      ownKeys() {
        proxyTrapCalls += 1;
        throw new Error("must not inspect proxy collaborator");
      },
    },
  );
  assert.throws(
    () =>
      new FilesystemImageProviderState({
        ...baseOptions,
        headAnchor: proxyAnchor,
      }),
    stateError("invalid_request"),
  );
  assert.equal(proxyTrapCalls, 0);

  let applyCalls = 0;
  const proxyFunction = new Proxy(async () => canonical, {
    apply() {
      applyCalls += 1;
      throw new Error("must not invoke proxy function");
    },
  });
  assert.throws(
    () =>
      new FilesystemImageProviderState({
        ...baseOptions,
        headAnchor: {
          readHead: proxyFunction,
          compareAndAdvance: async () => true,
        },
      }),
    stateError("invalid_request"),
  );
  assert.equal(applyCalls, 0);

  await t.test("callbacks never receive the state object as their receiver", async (t) => {
    const nested = await createFixture(t);
    const receivers = [];
    let head = canonical;
    const state = nested.createState({
      headAnchor: {
        readHead: async function readHead() {
          receivers.push(this);
          return copyLedgerHead(head);
        },
        compareAndAdvance: async function compareAndAdvance({
          expectedHead,
          nextHead,
        }) {
          receivers.push(this);
          if (!sameLedgerHead(head, expectedHead)) return false;
          head = normalizeFilesystemImageProviderStateHead(nextHead);
          return true;
        },
      },
    });
    await state.snapshot();
    await state.prepareOperation({
      kind: "provision",
      operationId: "operation-receiver-safe-anchor-001",
      request: operationRequest("provision"),
      storageId: "storage-001",
    });
    assert.deepEqual(receivers, [undefined, undefined, undefined]);
  });

  await t.test("readHead must return a native promise", async (t) => {
    const nested = await createFixture(t);
    const state = nested.createState({
      headAnchor: {
        readHead: () => canonical,
        compareAndAdvance: async () => true,
      },
    });
    await assert.rejects(state.snapshot(), stateError("io_failed"));
  });

  await t.test("compareAndAdvance must return a native promise", async (t) => {
    const nested = await createFixture(t);
    const state = nested.createState({
      headAnchor: {
        readHead: async () => canonical,
        compareAndAdvance: () => true,
      },
    });
    await assert.rejects(
      state.prepareOperation({
        kind: "provision",
        operationId: "operation-nonnative-anchor-promise-001",
        request: operationRequest("provision"),
        storageId: "storage-001",
      }),
      stateError("commit_outcome_uncertain"),
    );
  });

  await t.test("hostile Promise settlements cannot forge trusted-head reads", async (t) => {
    for (const unsafe of promiseSettlementCases(canonical)) {
      await t.test(unsafe.name, async (t) => {
        const nested = await createFixture(t);
        const settlement = unsafe.create();
        const state = nested.createState({
          headAnchor: {
            readHead: () => settlement.value,
            compareAndAdvance: async () => true,
          },
        });
        await assert.rejects(state.snapshot(), stateError("io_failed"));
        settlement.assertUntouched();
      });
    }
  });

  await t.test("hostile Promise settlements cannot forge trusted-head CAS", async (t) => {
    let index = 0;
    for (const unsafe of promiseSettlementCases(true)) {
      await t.test(unsafe.name, async (t) => {
        const nested = await createFixture(t);
        const settlement = unsafe.create();
        const state = nested.createState({
          headAnchor: {
            readHead: async () => copyLedgerHead(canonical),
            compareAndAdvance: () => settlement.value,
          },
        });
        index += 1;
        await assert.rejects(
          state.prepareOperation({
            kind: "provision",
            operationId: `operation-hostile-anchor-cas-${index}`,
            request: operationRequest("provision"),
            storageId: "storage-001",
          }),
          stateError("commit_outcome_uncertain"),
        );
        settlement.assertUntouched();
      });
    }
  });
});

test("returns frozen defensive snapshots and rejects hostile objects without invoking them", async (t) => {
  const { state } = await createFixture(t);
  assert.equal(Object.isFrozen(FilesystemImageProviderState), true);
  assert.equal(Object.isFrozen(FilesystemImageProviderState.prototype), true);
  assert.equal(Object.isFrozen(FilesystemImageProviderStateError), true);
  assert.equal(Object.isFrozen(FilesystemImageProviderStateError.prototype), true);
  const request = operationRequest("provision");
  const prepared = await state.prepareOperation({
    kind: "provision",
    operationId: "operation-defensive-001",
    request,
    storageId: "storage-001",
  });
  request.marker.lane = "mutated";
  assert.equal(prepared.request.marker.lane, "provider");
  assert.equal(Object.isFrozen(prepared), true);
  assert.equal(Object.isFrozen(prepared.request), true);
  assert.equal(Object.isFrozen(prepared.request.marker), true);

  await state.commitOperation({
    operationId: "operation-defensive-001",
    request: operationRequest("provision"),
    result: { nested: { proof: "original" } },
    storageState: storageState(),
  });
  const snapshot = await state.snapshot();
  assert.equal(Object.isFrozen(snapshot), true);
  assert.equal(Object.isFrozen(snapshot.operations), true);
  assert.equal(Object.isFrozen(snapshot.storages), true);
  assert.equal(Object.isFrozen(snapshot.storages[0]), true);
  assert.equal(Object.isFrozen(snapshot.storages[0].mount), true);

  let getterCalls = 0;
  const hostileRequest = {};
  Object.defineProperty(hostileRequest, "payload", {
    enumerable: true,
    get() {
      getterCalls += 1;
      return "secret-side-effect";
    },
  });
  await assert.rejects(
    state.prepareOperation({
      kind: "checkpoint",
      operationId: "operation-hostile-getter",
      request: hostileRequest,
      storageId: "storage-001",
    }),
    stateError("invalid_request"),
  );
  assert.equal(getterCalls, 0);

  const proxyRequest = new Proxy({}, {
    ownKeys() {
      throw new Error("must not execute proxy traps");
    },
  });
  await assert.rejects(
    state.prepareOperation({
      kind: "checkpoint",
      operationId: "operation-hostile-proxy",
      request: proxyRequest,
      storageId: "storage-001",
    }),
    stateError("invalid_request"),
  );
});

test("validation and replay survive post-import intrinsic poisoning in isolation", async (t) => {
  const fixture = await createFixture(t);
  await fixture.state.snapshot();
  const moduleUrl = new URL(
    "../src/filesystem-image-provider-state.mjs",
    import.meta.url,
  ).href;
  const script = `
    const { FilesystemImageProviderState } = await import(${JSON.stringify(moduleUrl)});
    const directory = process.argv[1];
    const caseIndex = Number(process.argv[2]);
    let anchoredHead = JSON.parse(process.argv[3]);
    const suffix = String(caseIndex);
    const objectGetOwnPropertyDescriptor = Object.getOwnPropertyDescriptor;
    const objectDefineProperty = Object.defineProperty;
    const specifications = [
      [Object, "hasOwn", "Object.hasOwn"],
      [Object, "getPrototypeOf", "Object.getPrototypeOf"],
      [Object, "getOwnPropertyDescriptor", "Object.getOwnPropertyDescriptor"],
      [Object, "defineProperty", "Object.defineProperty"],
      [Object, "freeze", "Object.freeze"],
      [Object, "create", "Object.create"],
      [Object, "is", "Object.is"],
      [Array, "isArray", "Array.isArray"],
      [Array.prototype, "every", "Array.prototype.every"],
      [Array.prototype, "includes", "Array.prototype.includes"],
      [RegExp.prototype, "test", "RegExp.prototype.test"],
      [Map.prototype, "delete", "Map.prototype.delete"],
      [Map.prototype, "forEach", "Map.prototype.forEach"],
      [Map.prototype, "get", "Map.prototype.get"],
      [Map.prototype, "has", "Map.prototype.has"],
      [Map.prototype, "set", "Map.prototype.set"],
      [Set.prototype, "add", "Set.prototype.add"],
      [Set.prototype, "has", "Set.prototype.has"],
      [WeakSet.prototype, "add", "WeakSet.prototype.add"],
      [WeakSet.prototype, "has", "WeakSet.prototype.has"],
      [Buffer.prototype, "equals", "Buffer.prototype.equals"],
      [Buffer.prototype, "subarray", "Buffer.prototype.subarray"],
      [Reflect, "ownKeys", "Reflect.ownKeys"],
    ];
    const poisonAll = caseIndex === specifications.length;
    const poisonLabel = poisonAll ? "all" : specifications[caseIndex][2];
    const originals = new Array(specifications.length);
    for (let index = 0; index < specifications.length; index += 1) {
      const target = specifications[index][0];
      const key = specifications[index][1];
      originals[index] = objectGetOwnPropertyDescriptor(target, key);
    }
    const acquireLock = async () => {
      let released = false;
      return {
        async assertHeld() {
          if (released) throw new Error("lock lost");
        },
        async release() {
          released = true;
        },
      };
    };
    const headAnchor = {
      async readHead() {
        return {
          contractVersion: anchoredHead.contractVersion,
          sequence: anchoredHead.sequence,
          lastChecksum: anchoredHead.lastChecksum,
          ledgerBytes: anchoredHead.ledgerBytes,
        };
      },
      async compareAndAdvance({ expectedHead, nextHead }) {
        if (
          anchoredHead.contractVersion !== expectedHead.contractVersion ||
          anchoredHead.sequence !== expectedHead.sequence ||
          anchoredHead.lastChecksum !== expectedHead.lastChecksum ||
          anchoredHead.ledgerBytes !== expectedHead.ledgerBytes
        ) return false;
        anchoredHead = nextHead;
        return true;
      },
    };
    const options = {
      acquireLock,
      directory,
      headAnchor,
      inspectAncestorAcl: async () => false,
      inspectDirectoryAcl: async () => false,
    };
    const request = {
      contractVersion: 1,
      kind: "provision",
      nested: { lanes: ["one", "two"] },
      storageId: "storage-poisoned-" + suffix,
    };
    const imageIdentity = {
      filesystemId: "hostfs:poisoned",
      objectIdentityScheme: "linux-ext4-file-handle-sha256-v1",
      objectId: "ext4fh1:image-poisoned-" + suffix,
    };
    const rootIdentity = {
      filesystemId: "ext4fs:poisoned",
      objectIdentityScheme: "linux-ext4-file-handle-sha256-v1",
      objectId: "ext4fh1:root-poisoned-" + suffix,
    };
    const storageState = {
      storageId: "storage-poisoned-" + suffix,
      sessionId: "session-poisoned-" + suffix,
      backendId: "filesystem-image-ext4",
      filesystemId: "ext4fs:poisoned",
      imagePath: "/var/lib/portable-codex/images/storage-poisoned-" + suffix + ".img",
      lifecycle: "provisioned",
      revision: "1",
      writerEpoch: "0",
      writerAuthority: null,
      mount: {
        mountPath: "/var/lib/portable-codex/mounts/storage-poisoned-" + suffix,
        imageIdentity,
        rootIdentity,
      },
      publicationControlIdentity: {
        filesystemId: rootIdentity.filesystemId,
        objectIdentityScheme: rootIdentity.objectIdentityScheme,
        objectId: "ext4fh1:publication-control-poisoned-" + suffix,
      },
      dataRoot: null,
      attachment: null,
    };
    let outcome;
    try {
      for (let index = 0; index < specifications.length; index += 1) {
        if (!poisonAll && index !== caseIndex) continue;
        const descriptor = originals[index];
        if (descriptor === undefined || !("value" in descriptor)) {
          throw new Error("intrinsic cannot be poisoned: " + specifications[index][2]);
        }
        objectDefineProperty(specifications[index][0], specifications[index][1], {
          ...descriptor,
          value: function poisonedIntrinsic() {
            throw new Error("ambient intrinsic was invoked: " + specifications[index][2]);
          },
        });
      }
      const state = new FilesystemImageProviderState(options);
      const prepared = await state.prepareOperation({
        kind: "provision",
        operationId: "operation-poisoned-" + suffix,
        request,
        storageId: "storage-poisoned-" + suffix,
      });
      const committed = await state.commitOperation({
        operationId: "operation-poisoned-" + suffix,
        request,
        result: { status: "provisioned" },
        storageState,
      });
      const restarted = new FilesystemImageProviderState(options);
      const snapshot = await restarted.snapshot();
      outcome = { committed, prepared, snapshot };
    } finally {
      for (let index = 0; index < specifications.length; index += 1) {
        if (!poisonAll && index !== caseIndex) continue;
        const descriptor = originals[index];
        if (descriptor !== undefined) {
          objectDefineProperty(specifications[index][0], specifications[index][1], descriptor);
        }
      }
    }
    if (
      outcome.prepared.shouldDispatch !== true ||
      outcome.committed.state !== "committed" ||
      outcome.snapshot.sequence !== (caseIndex + 1) * 2 ||
      outcome.snapshot.storages.length !== caseIndex + 1 ||
      !Object.isFrozen(outcome.snapshot.storages[caseIndex])
    ) {
      throw new Error("poisoned-intrinsics replay result was invalid");
    }
    process.stdout.write(JSON.stringify({ head: anchoredHead, label: poisonLabel }) + "\\n");
  `;

  const poisonLabels = [
    "Object.hasOwn",
    "Object.getPrototypeOf",
    "Object.getOwnPropertyDescriptor",
    "Object.defineProperty",
    "Object.freeze",
    "Object.create",
    "Object.is",
    "Array.isArray",
    "Array.prototype.every",
    "Array.prototype.includes",
    "RegExp.prototype.test",
    "Map.prototype.delete",
    "Map.prototype.forEach",
    "Map.prototype.get",
    "Map.prototype.has",
    "Map.prototype.set",
    "Set.prototype.add",
    "Set.prototype.has",
    "WeakSet.prototype.add",
    "WeakSet.prototype.has",
    "Buffer.prototype.equals",
    "Buffer.prototype.subarray",
    "Reflect.ownKeys",
    "all",
  ];
  let trustedHead = normalizeFilesystemImageProviderStateHead({
    contractVersion: FILESYSTEM_IMAGE_PROVIDER_STATE_HEAD_CONTRACT_VERSION,
    sequence: 0,
    lastChecksum: null,
    ledgerBytes: 0,
  });
  for (let caseIndex = 0; caseIndex < poisonLabels.length; caseIndex += 1) {
    const poisonLabel = poisonLabels[caseIndex];
    const { stderr, stdout } = await execFileAsync(
      process.execPath,
      [
        "--input-type=module",
        "--eval",
        script,
        fixture.directory,
        String(caseIndex),
        JSON.stringify(trustedHead),
      ],
      {
        encoding: "utf8",
        maxBuffer: 64 * 1024,
        timeout: 10_000,
      },
    );
    assert.equal(stderr, "");
    const output = JSON.parse(stdout);
    assert.equal(output.label, poisonLabel);
    trustedHead = normalizeFilesystemImageProviderStateHead(output.head);
  }
});

test("retains lease-independent data roots and replaces them only for restore attach", async (t) => {
  const { state } = await createFixture(t);
  const fixedMount = mount();
  await prepareAndCommit(state, {
    storage: storageState({ mount: fixedMount }),
  });

  const attachRequest = operationRequest("attach");
  const firstAttachment = attachment(fixedMount);
  const attached = storageState({
    attachment: firstAttachment,
    lifecycle: "attached",
    mount: fixedMount,
    revision: "2",
    writerEpoch: "1",
  });
  await prepareAndCommit(state, {
    kind: "attach",
    operationId: "operation-attach-001",
    request: attachRequest,
    storage: attached,
  });
  assert.deepEqual(
    attached.writerAuthority,
    writerAuthorityFromAttachment(firstAttachment),
  );

  const detached = storageState({
    dataRoot: attached.dataRoot,
    lifecycle: "detached",
    mount: fixedMount,
    revision: "3",
    writerAuthority: attached.writerAuthority,
    writerEpoch: "1",
  });
  await prepareAndCommit(state, {
    kind: "detach",
    operationId: "operation-detach-001",
    request: operationRequest("detach"),
    storage: detached,
  });
  assert.deepEqual(detached.writerAuthority, attached.writerAuthority);

  const reattachRequest = operationRequest("attach", "storage-001", {
    attempt: "reattach",
  });
  await state.prepareOperation({
    kind: "attach",
    operationId: "operation-reattach-001",
    request: reattachRequest,
    storageId: "storage-001",
  });
  const replacedByOrdinaryAttach = attachment(fixedMount, {
    attachmentId: "attachment-wrong-root",
    fencingEpoch: "2",
    leaseId: "lease-wrong-root",
    proofId: "proof-wrong-root",
    rootPath: join(fixedMount.mountPath, "generation-wrong-root"),
    rootIdentity: physicalIdentity({ objectId: "ext4fh1:generation-wrong-root" }),
  });
  await assert.rejects(
    state.commitOperation({
      operationId: "operation-reattach-001",
      request: reattachRequest,
      result: { status: "attached" },
      storageState: storageState({
        attachment: replacedByOrdinaryAttach,
        lifecycle: "attached",
        mount: fixedMount,
        revision: "4",
        writerEpoch: "2",
      }),
    }),
    stateError("operation_conflict"),
  );

  const reattachment = attachment(fixedMount, {
    attachmentId: "attachment-002",
    fencingEpoch: "2",
    leaseId: "lease-002",
    proofId: "proof-002",
  });
  const reattached = storageState({
    attachment: reattachment,
    lifecycle: "attached",
    mount: fixedMount,
    revision: "4",
    writerEpoch: "2",
  });
  await state.commitOperation({
    operationId: "operation-reattach-001",
    request: reattachRequest,
    result: { status: "attached" },
    storageState: reattached,
  });
  assert.deepEqual(reattached.dataRoot, detached.dataRoot);

  const detachedAgain = storageState({
    dataRoot: reattached.dataRoot,
    lifecycle: "detached",
    mount: fixedMount,
    revision: "5",
    writerAuthority: reattached.writerAuthority,
    writerEpoch: "2",
  });
  await prepareAndCommit(state, {
    kind: "detach",
    operationId: "operation-detach-002",
    request: operationRequest("detach", "storage-001", { attempt: 2 }),
    storage: detachedAgain,
  });

  const restoreAttachRequest = operationRequest("restore-attach");
  await state.prepareOperation({
    kind: "restore-attach",
    operationId: "operation-restore-attach-001",
    request: restoreAttachRequest,
    storageId: "storage-001",
  });
  const sameRootRestoreAttachment = attachment(fixedMount, {
    attachmentId: "attachment-restore-same-root",
    fencingEpoch: "3",
    leaseId: "lease-restore-same-root",
    proofId: "proof-restore-same-root",
  });
  await assert.rejects(
    state.commitOperation({
      operationId: "operation-restore-attach-001",
      request: restoreAttachRequest,
      result: { status: "restore-attached" },
      storageState: storageState({
        attachment: sameRootRestoreAttachment,
        lifecycle: "attached",
        mount: fixedMount,
        revision: "6",
        writerEpoch: "3",
      }),
    }),
    stateError("operation_conflict"),
  );

  const generationAttachment = attachment(fixedMount, {
    attachmentId: "attachment-restore-001",
    fencingEpoch: "3",
    leaseId: "lease-restore-001",
    proofId: "proof-restore-001",
    rootPath: join(fixedMount.mountPath, "generation-002"),
    rootIdentity: physicalIdentity({ objectId: "ext4fh1:generation-002" }),
  });
  const restoreAttached = storageState({
    attachment: generationAttachment,
    lifecycle: "attached",
    mount: fixedMount,
    revision: "6",
    writerEpoch: "3",
  });
  await state.commitOperation({
    operationId: "operation-restore-attach-001",
    request: restoreAttachRequest,
    result: { status: "restore-attached" },
    storageState: restoreAttached,
  });
  assert.notDeepEqual(restoreAttached.dataRoot, detachedAgain.dataRoot);
  assert.deepEqual(
    restoreAttached.dataRoot,
    dataRootFromAttachment(restoreAttached.attachment),
  );

  const detachedAfterRestore = storageState({
    dataRoot: restoreAttached.dataRoot,
    lifecycle: "detached",
    mount: fixedMount,
    revision: "7",
    writerAuthority: restoreAttached.writerAuthority,
    writerEpoch: "3",
  });
  await prepareAndCommit(state, {
    kind: "detach",
    operationId: "operation-detach-003",
    request: operationRequest("detach", "storage-001", { attempt: 3 }),
    storage: detachedAfterRestore,
  });

  const destroyed = storageState({
    lifecycle: "destroyed",
    mount: null,
    revision: "8",
    writerAuthority: detachedAfterRestore.writerAuthority,
    writerEpoch: "3",
  });
  await prepareAndCommit(state, {
    kind: "destroy",
    operationId: "operation-destroy-001",
    request: operationRequest("destroy"),
    storage: destroyed,
  });
  assert.deepEqual(destroyed.writerAuthority, detachedAfterRestore.writerAuthority);
  assert.deepEqual(await state.readStorage("storage-001"), destroyed);
  assert.equal(destroyed.dataRoot, null);

  await assert.rejects(
    state.prepareOperation({
      kind: "provision",
      operationId: "operation-revive-001",
      request: operationRequest("provision"),
      storageId: "storage-001",
    }),
    stateError("operation_conflict"),
  );
  assert.deepEqual(await state.readStorage("storage-001"), destroyed);
});

test("accepts only canonical direct-child attachment roots with distinct identity", async (t) => {
  const fixture = await createFixture(t);
  const fixedMount = mount();
  await prepareAndCommit(fixture.state, {
    storage: storageState({ mount: fixedMount }),
  });

  const operationId = "operation-attach-validate-root";
  const request = operationRequest("attach", "storage-001", { operationId });
  await fixture.state.prepareOperation({
    kind: "attach",
    operationId,
    request,
    storageId: "storage-001",
  });

  async function rejectAttachment(fixedAttachment) {
    await assert.rejects(
      fixture.state.commitOperation({
        operationId,
        request,
        result: { status: "attached" },
        storageState: storageState({
          attachment: fixedAttachment,
          lifecycle: "attached",
          mount: fixedMount,
          revision: "2",
          writerEpoch: "1",
        }),
      }),
      stateError("invalid_request"),
    );
  }

  await rejectAttachment(
    attachment(fixedMount, {
      rootPath: fixedMount.mountPath,
      rootIdentity: fixedMount.rootIdentity,
    }),
  );
  await rejectAttachment(
    attachment(fixedMount, {
      rootPath: join(dirname(fixedMount.mountPath), "sibling", "attachment-001"),
    }),
  );
  await rejectAttachment(
    attachment(fixedMount, {
      rootPath: join(fixedMount.mountPath, "nested", "attachment-001"),
    }),
  );

  const fixedAttachment = attachment(fixedMount);
  await assert.rejects(
    fixture.state.commitOperation({
      operationId,
      request,
      result: { status: "attached" },
      storageState: storageState({
        attachment: fixedAttachment,
        lifecycle: "attached",
        mount: fixedMount,
        revision: "2",
        writerAuthority: {
          ...writerAuthorityFromAttachment(fixedAttachment),
          leaseId: "lease-mismatched",
        },
        writerEpoch: "1",
      }),
    }),
    stateError("invalid_request"),
  );

  const valid = await fixture.state.commitOperation({
    operationId,
    request,
    result: { status: "attached" },
    storageState: storageState({
      attachment: fixedAttachment,
      lifecycle: "attached",
      mount: fixedMount,
      revision: "2",
      writerEpoch: "1",
    }),
  });
  assert.equal(valid.storageState.attachment.rootPath, join(fixedMount.mountPath, "attachment-001"));
  assert.notDeepEqual(
    valid.storageState.attachment.rootIdentity,
    valid.storageState.mount.rootIdentity,
  );
});
