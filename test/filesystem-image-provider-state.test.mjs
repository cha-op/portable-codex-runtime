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
  open,
  readFile,
  rename,
  rm,
  symlink,
  truncate,
  utimes,
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
  filesystemImageProviderStateCheckpointName,
  filesystemImageProviderStateHeadChecksum,
  filesystemImageProviderStateLedgerName,
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
    anchorRevision: head.anchorRevision,
    generation: head.generation,
    stateRevision: head.stateRevision,
    baseHeadChecksum: head.baseHeadChecksum,
    checkpointStateRevision: head.checkpointStateRevision,
    checkpointFrameCount: head.checkpointFrameCount,
    checkpointChecksum: head.checkpointChecksum,
    checkpointBytes: head.checkpointBytes,
    frameCount: head.frameCount,
    lastChecksum: head.lastChecksum,
    ledgerBytes: head.ledgerBytes,
  };
}

function sameLedgerHead(left, right) {
  return (
    left.contractVersion === right.contractVersion &&
    left.anchorRevision === right.anchorRevision &&
    left.generation === right.generation &&
    left.stateRevision === right.stateRevision &&
    left.baseHeadChecksum === right.baseHeadChecksum &&
    left.checkpointStateRevision === right.checkpointStateRevision &&
    left.checkpointFrameCount === right.checkpointFrameCount &&
    left.checkpointChecksum === right.checkpointChecksum &&
    left.checkpointBytes === right.checkpointBytes &&
    left.frameCount === right.frameCount &&
    left.lastChecksum === right.lastChecksum &&
    left.ledgerBytes === right.ledgerBytes
  );
}

function genesisHead() {
  return normalizeFilesystemImageProviderStateHead({
    contractVersion: FILESYSTEM_IMAGE_PROVIDER_STATE_HEAD_CONTRACT_VERSION,
    anchorRevision: "0",
    generation: "0",
    stateRevision: "0",
    baseHeadChecksum: null,
    checkpointStateRevision: "0",
    checkpointFrameCount: 0,
    checkpointChecksum: null,
    checkpointBytes: 0,
    frameCount: 0,
    lastChecksum: null,
    ledgerBytes: 0,
  });
}

function createTrustedHeadAnchor(tracker = {}) {
  let head = genesisHead();
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
        if (tracker.failReadAfterFailedAdvance === true) tracker.failRead = true;
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
        "portable-codex/filesystem-image-provider-state/frame/v2\0",
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

async function createRotatedFixture(t) {
  const fixture = await createFixture(t);
  const rotationPolicy = {
    activeLedgerBytesWatermark: 64 * 1024 * 1024,
    activeFrameCountWatermark: 1,
  };
  const state = fixture.createState({ rotationPolicy });
  await prepareAndCommit(state);
  assert.equal(fixture.headAnchorTracker.head.generation, "1");
  return {
    checkpointPath: join(
      fixture.directory,
      filesystemImageProviderStateCheckpointName("1"),
    ),
    fixture,
    ledgerPath: join(
      fixture.directory,
      filesystemImageProviderStateLedgerName("1"),
    ),
    rotationPolicy,
    state,
  };
}

test("rotates a prepared operation into a checkpoint before its commit", async (t) => {
  const fixture = await createFixture(t);
  const state = fixture.createState({
    rotationPolicy: {
      activeLedgerBytesWatermark: 64 * 1024 * 1024,
      activeFrameCountWatermark: 1,
    },
  });
  const request = operationRequest("provision");
  await state.prepareOperation({
    kind: "provision",
    operationId: "operation-prepared-across-rotation-001",
    request,
    storageId: "storage-001",
  });
  const preparedHead = copyLedgerHead(fixture.headAnchorTracker.head);
  assert.equal(preparedHead.generation, "0");
  assert.equal(preparedHead.stateRevision, "1");
  assert.equal(preparedHead.frameCount, 1);

  const committed = await state.commitOperation({
    operationId: "operation-prepared-across-rotation-001",
    request,
    result: { proofId: "proof-across-rotation", status: "provisioned" },
    storageState: storageState(),
  });
  assert.equal(committed.state, "committed");

  const head = fixture.headAnchorTracker.head;
  assert.equal(head.anchorRevision, "3");
  assert.equal(head.generation, "1");
  assert.equal(head.stateRevision, "2");
  assert.equal(head.baseHeadChecksum, filesystemImageProviderStateHeadChecksum(preparedHead));
  assert.equal(head.checkpointStateRevision, "1");
  assert.equal(head.checkpointFrameCount, 3);
  assert.equal(head.frameCount, 1);
  assert.notEqual(head.checkpointChecksum, null);
  assert.notEqual(head.lastChecksum, head.checkpointChecksum);

  const checkpointPath = join(
    fixture.directory,
    filesystemImageProviderStateCheckpointName("1"),
  );
  const ledgerPath = join(
    fixture.directory,
    filesystemImageProviderStateLedgerName("1"),
  );
  assert.equal((await readFile(checkpointPath)).length, head.checkpointBytes);
  const active = await readFile(ledgerPath);
  assert.equal(active.length, head.ledgerBytes);
  assert.equal(active.includes(Buffer.from('"request":', "utf8")), false);
  await assert.rejects(
    readFile(fixture.ledgerPath),
    (error) => error?.code === "ENOENT",
  );

  const replayed = await fixture.createState().readOperation({
    operationId: "operation-prepared-across-rotation-001",
    request,
  });
  assert.equal(replayed.state, "committed");
  assert.deepEqual(replayed.result, committed.result);
});

test("repeated rotation preserves committed evidence, tombstones, and prepared ambiguity", async (t) => {
  const fixture = await createFixture(t);
  const rotationPolicy = {
    activeLedgerBytesWatermark: 64 * 1024 * 1024,
    activeFrameCountWatermark: 1,
  };
  const state = fixture.createState({ rotationPolicy });
  await prepareAndCommit(state);

  const destroyRequest = operationRequest("destroy");
  await state.prepareOperation({
    kind: "destroy",
    operationId: "operation-destroy-across-rotation-001",
    request: destroyRequest,
    storageId: "storage-001",
  });
  const destroyed = storageState({
    lifecycle: "destroyed",
    mount: null,
    revision: "2",
  });
  await state.commitOperation({
    operationId: "operation-destroy-across-rotation-001",
    request: destroyRequest,
    result: { proofId: "proof-destroyed", status: "destroyed" },
    storageState: destroyed,
  });

  const pendingRequest = operationRequest("provision", "storage-002");
  await state.prepareOperation({
    kind: "provision",
    operationId: "operation-pending-across-rotation-002",
    request: pendingRequest,
    storageId: "storage-002",
  });
  await state.prepareOperation({
    kind: "provision",
    operationId: "operation-pending-across-rotation-003",
    request: operationRequest("provision", "storage-003"),
    storageId: "storage-003",
  });

  assert.equal(fixture.headAnchorTracker.head.generation, "5");
  const restarted = fixture.createState({ rotationPolicy });
  assert.deepEqual(await restarted.readStorage("storage-001"), destroyed);
  const committedProvision = await restarted.prepareOperation({
    kind: "provision",
    operationId: "operation-provision-001",
    request: operationRequest("provision"),
    storageId: "storage-001",
  });
  assert.equal(committedProvision.state, "committed");
  assert.equal(committedProvision.replayed, true);

  const replayedPending = await restarted.prepareOperation({
    kind: "provision",
    operationId: "operation-pending-across-rotation-002",
    request: pendingRequest,
    storageId: "storage-002",
  });
  assert.equal(replayedPending.state, "prepared");
  assert.equal(replayedPending.shouldDispatch, false);
  await assert.rejects(
    restarted.prepareOperation({
      kind: "provision",
      operationId: "operation-conflicting-pending-storage-002",
      request: operationRequest("provision", "storage-002"),
      storageId: "storage-002",
    }),
    stateError("operation_already_prepared"),
  );

  const committedPending = await restarted.commitOperation({
    operationId: "operation-pending-across-rotation-002",
    request: pendingRequest,
    result: { proofId: "proof-storage-002", status: "provisioned" },
    storageState: storageState({ storageId: "storage-002" }),
  });
  assert.equal(committedPending.state, "committed");
  assert.equal(fixture.headAnchorTracker.head.generation, "6");
  assert.equal(
    (await restarted.readOperation({
      operationId: "operation-pending-across-rotation-003",
    })).state,
    "prepared",
  );
});

test("capacity inspection reports exact soft and hard boundaries", async (t) => {
  const fixture = await createFixture(t);
  const state = fixture.createState({
    rotationPolicy: {
      activeLedgerBytesWatermark: 64 * 1024 * 1024,
      activeFrameCountWatermark: 2,
    },
  });
  const initialPromise = state.inspectCapacity();
  assert.equal(Object.getPrototypeOf(initialPromise), Promise.prototype);
  const initial = await initialPromise;
  assert.equal(Object.isFrozen(initial), true);
  assert.deepEqual(initial, {
    contractVersion: FILESYSTEM_IMAGE_PROVIDER_STATE_CONTRACT_VERSION,
    anchorRevision: "0",
    generation: "0",
    stateRevision: "0",
    checkpointStateRevision: "0",
    checkpointBytes: 0,
    checkpointFrameCount: 0,
    activeLedgerBytes: 0,
    activeFrameCount: 0,
    remainingLedgerBytes: 64 * 1024 * 1024,
    remainingFrameCount: 65_535,
    activeLedgerBytesWatermark: 64 * 1024 * 1024,
    activeFrameCountWatermark: 2,
    rotationRequired: false,
    retainedOperationCount: 0,
    preparedOperationCount: 0,
    storageCount: 0,
  });

  const request = operationRequest("provision");
  await state.prepareOperation({
    kind: "provision",
    operationId: "operation-capacity-boundary-001",
    request,
    storageId: "storage-001",
  });
  assert.equal((await state.inspectCapacity()).rotationRequired, false);
  await state.commitOperation({
    operationId: "operation-capacity-boundary-001",
    request,
    result: { proofId: "proof-capacity", status: "provisioned" },
    storageState: storageState(),
  });
  const atFrameBoundary = await state.inspectCapacity();
  assert.equal(atFrameBoundary.generation, "0");
  assert.equal(atFrameBoundary.activeFrameCount, 2);
  assert.equal(atFrameBoundary.rotationRequired, true);
  assert.equal(atFrameBoundary.retainedOperationCount, 1);
  assert.equal(atFrameBoundary.preparedOperationCount, 0);
  assert.equal(atFrameBoundary.storageCount, 1);

  await state.prepareOperation({
    kind: "checkpoint",
    operationId: "operation-capacity-boundary-002",
    request: operationRequest("checkpoint"),
    storageId: "storage-001",
  });
  const afterRotation = await state.inspectCapacity();
  assert.equal(afterRotation.generation, "1");
  assert.equal(afterRotation.checkpointStateRevision, "2");
  assert.equal(afterRotation.activeFrameCount, 1);
  assert.equal(afterRotation.rotationRequired, false);
  assert.equal(afterRotation.retainedOperationCount, 2);
  assert.equal(afterRotation.preparedOperationCount, 1);
});

test("rotation policy is exact and cannot relax hard active-log limits", async (t) => {
  const fixture = await createFixture(t);
  const invalidPolicies = [
    { activeLedgerBytesWatermark: 1 },
    { activeFrameCountWatermark: 1 },
    {
      activeLedgerBytesWatermark: 1,
      activeFrameCountWatermark: 1,
      extra: true,
    },
    {
      activeLedgerBytesWatermark: 0,
      activeFrameCountWatermark: 1,
    },
    {
      activeLedgerBytesWatermark: 64 * 1024 * 1024 + 1,
      activeFrameCountWatermark: 1,
    },
    {
      activeLedgerBytesWatermark: 1,
      activeFrameCountWatermark: 65_536,
    },
  ];
  for (const rotationPolicy of invalidPolicies) {
    assert.throws(
      () => fixture.createState({ rotationPolicy }),
      stateError("invalid_request"),
    );
  }
});

test("an empty active log accepts one legal frame beyond a byte watermark", async (t) => {
  const fixture = await createFixture(t);
  const state = fixture.createState({
    rotationPolicy: {
      activeLedgerBytesWatermark: 1,
      activeFrameCountWatermark: 65_535,
    },
  });
  const request = operationRequest("provision");
  await state.prepareOperation({
    kind: "provision",
    operationId: "operation-byte-watermark-001",
    request,
    storageId: "storage-001",
  });
  const overWatermark = await state.inspectCapacity();
  assert.equal(overWatermark.generation, "0");
  assert(overWatermark.activeLedgerBytes > 1);
  assert.equal(overWatermark.rotationRequired, true);

  await state.commitOperation({
    operationId: "operation-byte-watermark-001",
    request,
    result: { proofId: "proof-byte-watermark", status: "provisioned" },
    storageState: storageState(),
  });
  assert.equal(fixture.headAnchorTracker.head.generation, "1");
});

test("the next append rotates at the exact active-byte watermark", async (t) => {
  const fixture = await createFixture(t);
  const request = operationRequest("provision");
  await fixture.state.prepareOperation({
    kind: "provision",
    operationId: "operation-exact-byte-boundary-001",
    request,
    storageId: "storage-001",
  });
  const activeLedgerBytes = fixture.headAnchorTracker.head.ledgerBytes;
  const state = fixture.createState({
    rotationPolicy: {
      activeLedgerBytesWatermark: activeLedgerBytes,
      activeFrameCountWatermark: 65_535,
    },
  });
  assert.equal((await state.inspectCapacity()).rotationRequired, true);
  await state.commitOperation({
    operationId: "operation-exact-byte-boundary-001",
    request,
    result: { proofId: "proof-exact-byte-boundary", status: "provisioned" },
    storageState: storageState(),
  });
  assert.equal(fixture.headAnchorTracker.head.generation, "1");
  assert.equal(fixture.headAnchorTracker.head.frameCount, 1);
});

test("cache avoids unchanged-head replay and catches up append and rotation", async (t) => {
  const fixture = await createFixture(t);
  const rotationPolicy = {
    activeLedgerBytesWatermark: 64 * 1024 * 1024,
    activeFrameCountWatermark: 2,
  };
  const first = fixture.createState({ rotationPolicy });
  const second = fixture.createState({ rotationPolicy });
  const request = operationRequest("provision");
  assert.equal((await first.snapshot()).sequence, 0);
  await second.prepareOperation({
    kind: "provision",
    operationId: "operation-cache-001",
    request,
    storageId: "storage-001",
  });
  assert.equal(
    (await first.readOperation({ operationId: "operation-cache-001" })).state,
    "prepared",
  );

  const probe = await open(fixture.ledgerPath, "r");
  const fileHandlePrototype = Object.getPrototypeOf(probe);
  await probe.close();
  const readDescriptor = Object.getOwnPropertyDescriptor(
    fileHandlePrototype,
    "read",
  );
  assert.notEqual(readDescriptor, undefined);
  Object.defineProperty(fileHandlePrototype, "read", {
    ...readDescriptor,
    value() {
      throw new Error("unchanged head must not replay active bytes");
    },
  });
  try {
    assert.equal((await first.snapshot()).sequence, 1);
  } finally {
    Object.defineProperty(fileHandlePrototype, "read", readDescriptor);
  }

  await second.commitOperation({
    operationId: "operation-cache-001",
    request,
    result: { proofId: "proof-cache", status: "provisioned" },
    storageState: storageState(),
  });
  assert.deepEqual(await first.readStorage("storage-001"), storageState());

  await second.prepareOperation({
    kind: "checkpoint",
    operationId: "operation-cache-rotation-002",
    request: operationRequest("checkpoint"),
    storageId: "storage-001",
  });
  assert.equal(fixture.headAnchorTracker.head.generation, "1");
  const caughtUp = await first.readOperation({
    operationId: "operation-cache-rotation-002",
  });
  assert.equal(caughtUp.state, "prepared");
});

test("cache consumes path metadata observed after held-file revalidation", async (t) => {
  const { checkpointPath, fixture, state } = await createRotatedFixture(t);
  await state.snapshot();
  const trustedHead = copyLedgerHead(fixture.headAnchorTracker.head);
  const targetMetadata = await lstat(checkpointPath, { bigint: true });
  const corrupted = await readFile(checkpointPath);
  corrupted[0] ^= 0x01;

  const probe = await open(checkpointPath, "r");
  const fileHandlePrototype = Object.getPrototypeOf(probe);
  await probe.close();
  const statDescriptor = Object.getOwnPropertyDescriptor(
    fileHandlePrototype,
    "stat",
  );
  assert.notEqual(statDescriptor, undefined);
  let targetStatCount = 0;
  let injected = false;
  Object.defineProperty(fileHandlePrototype, "stat", {
    ...statDescriptor,
    async value(...args) {
      const metadata = await Reflect.apply(statDescriptor.value, this, args);
      if (metadata.ino === targetMetadata.ino) {
        targetStatCount += 1;
        if (!injected && targetStatCount === 2) {
          injected = true;
          await writeFile(checkpointPath, corrupted);
        }
      }
      return metadata;
    },
  });
  try {
    await assert.rejects(state.snapshot(), stateError("corrupt_ledger"));
  } finally {
    Object.defineProperty(fileHandlePrototype, "stat", statDescriptor);
  }
  assert.equal(injected, true);
  assert.deepEqual(fixture.headAnchorTracker.head, trustedHead);
});

test("benign file and directory metadata churn preserves exact content", async (t) => {
  const fixture = await createFixture(t);
  await fixture.state.prepareOperation({
    kind: "provision",
    operationId: "operation-metadata-churn-001",
    request: operationRequest("provision"),
    storageId: "storage-001",
  });
  const before = await readFile(fixture.ledgerPath);
  const changedTime = new Date(Date.now() - 60_000);
  await utimes(fixture.ledgerPath, changedTime, changedTime);
  await writeFile(join(fixture.directory, "unrelated-entry"), "unchanged", {
    mode: 0o600,
  });

  const snapshot = await fixture.state.snapshot();
  assert.equal(snapshot.sequence, 1);
  assert.deepEqual(await readFile(fixture.ledgerPath), before);
});

test("maximum canonical request and result fit commit and checkpoint frames", async (t) => {
  const fixture = await createFixture(t);
  const state = fixture.createState({
    rotationPolicy: {
      activeLedgerBytesWatermark: 64 * 1024 * 1024,
      activeFrameCountWatermark: 2,
    },
  });
  const envelopeBytes = Buffer.byteLength('{"payload":""}', "utf8");
  const payload = "x".repeat(768 * 1024 - envelopeBytes);
  const request = { payload };
  const result = { payload };
  await state.prepareOperation({
    kind: "provision",
    operationId: "operation-maximum-frame-001",
    request,
    storageId: "storage-001",
  });
  const committed = await state.commitOperation({
    operationId: "operation-maximum-frame-001",
    request,
    result,
    storageState: storageState(),
  });
  assert.equal(committed.result.payload.length, payload.length);

  await state.prepareOperation({
    kind: "provision",
    operationId: "operation-after-maximum-frame-002",
    request: operationRequest("provision", "storage-002"),
    storageId: "storage-002",
  });
  assert.equal(fixture.headAnchorTracker.head.generation, "1");
  const replayed = await fixture.createState().readOperation({
    operationId: "operation-maximum-frame-001",
    request,
  });
  assert.equal(replayed.state, "committed");
  assert.equal(replayed.result.payload.length, payload.length);

  await assert.rejects(
    state.prepareOperation({
      kind: "provision",
      operationId: "operation-over-maximum-frame-003",
      request: { payload: `${payload}x` },
      storageId: "storage-003",
    }),
    stateError("invalid_request"),
  );
});

test("rotation resolves CAS old, new acknowledgement loss, and unknown readback", async (t) => {
  await t.test("CAS old cleans the candidate and permits retry", async (t) => {
    const fixture = await createFixture(t);
    const state = fixture.createState({
      rotationPolicy: {
        activeLedgerBytesWatermark: 64 * 1024 * 1024,
        activeFrameCountWatermark: 1,
      },
    });
    const request = operationRequest("provision");
    await state.prepareOperation({
      kind: "provision",
      operationId: "operation-rotation-cas-old-001",
      request,
      storageId: "storage-001",
    });
    fixture.headAnchorTracker.failAdvanceBeforeCommit = true;
    await assert.rejects(
      state.commitOperation({
        operationId: "operation-rotation-cas-old-001",
        request,
        result: { proofId: "proof-cas-old", status: "provisioned" },
        storageState: storageState(),
      }),
      stateError("maintenance_failed"),
    );
    fixture.headAnchorTracker.failAdvanceBeforeCommit = false;
    assert.equal(fixture.headAnchorTracker.head.generation, "0");
    await assert.rejects(
      readFile(
        join(
          fixture.directory,
          filesystemImageProviderStateCheckpointName("1"),
        ),
      ),
      (error) => error?.code === "ENOENT",
    );
    assert.equal(
      (await state.commitOperation({
        operationId: "operation-rotation-cas-old-001",
        request,
        result: { proofId: "proof-cas-old", status: "provisioned" },
        storageState: storageState(),
      })).state,
      "committed",
    );
  });

  await t.test("CAS new acknowledgement loss is read back and succeeds", async (t) => {
    const fixture = await createFixture(t);
    const state = fixture.createState({
      rotationPolicy: {
        activeLedgerBytesWatermark: 64 * 1024 * 1024,
        activeFrameCountWatermark: 1,
      },
    });
    const request = operationRequest("provision");
    await state.prepareOperation({
      kind: "provision",
      operationId: "operation-rotation-cas-new-001",
      request,
      storageId: "storage-001",
    });
    fixture.headAnchorTracker.loseNextAdvanceAcknowledgement = true;
    const committed = await state.commitOperation({
      operationId: "operation-rotation-cas-new-001",
      request,
      result: { proofId: "proof-cas-new", status: "provisioned" },
      storageState: storageState(),
    });
    assert.equal(committed.state, "committed");
    assert.equal(fixture.headAnchorTracker.head.generation, "1");
    assert.equal(fixture.headAnchorTracker.head.stateRevision, "2");
  });

  await t.test("unknown CAS leaves a candidate selected only by later head read", async (t) => {
    const fixture = await createFixture(t);
    const state = fixture.createState({
      rotationPolicy: {
        activeLedgerBytesWatermark: 64 * 1024 * 1024,
        activeFrameCountWatermark: 1,
      },
    });
    const request = operationRequest("provision");
    await state.prepareOperation({
      kind: "provision",
      operationId: "operation-rotation-cas-unknown-001",
      request,
      storageId: "storage-001",
    });
    fixture.headAnchorTracker.failAdvanceBeforeCommit = true;
    fixture.headAnchorTracker.failReadAfterFailedAdvance = true;
    await assert.rejects(
      state.commitOperation({
        operationId: "operation-rotation-cas-unknown-001",
        request,
        result: { proofId: "proof-cas-unknown", status: "provisioned" },
        storageState: storageState(),
      }),
      stateError("maintenance_failed"),
    );
    assert(
      (await readFile(
        join(
          fixture.directory,
          filesystemImageProviderStateCheckpointName("1"),
        ),
      )).length > 0,
    );
    fixture.headAnchorTracker.failAdvanceBeforeCommit = false;
    fixture.headAnchorTracker.failReadAfterFailedAdvance = false;
    fixture.headAnchorTracker.failRead = false;
    assert.equal(fixture.headAnchorTracker.head.generation, "0");
    assert.equal(
      (await state.commitOperation({
        operationId: "operation-rotation-cas-unknown-001",
        request,
        result: { proofId: "proof-cas-unknown", status: "provisioned" },
        storageState: storageState(),
      })).state,
      "committed",
    );
  });
});

test("post-CAS rotation cleanup failure keeps the maintenance head authoritative", async (t) => {
  const fixture = await createFixture(t);
  const request = operationRequest("provision");
  await fixture.state.prepareOperation({
    kind: "provision",
    operationId: "operation-rotation-cleanup-001",
    request,
    storageId: "storage-001",
  });
  let directorySyncs = 0;
  const failingCleanup = fixture.createState({
    rotationPolicy: {
      activeLedgerBytesWatermark: 64 * 1024 * 1024,
      activeFrameCountWatermark: 1,
    },
    syncDirectory: async () => {
      directorySyncs += 1;
      if (directorySyncs === 2) throw new Error("old generation cleanup failed");
    },
  });
  await assert.rejects(
    failingCleanup.commitOperation({
      operationId: "operation-rotation-cleanup-001",
      request,
      result: { proofId: "proof-cleanup", status: "provisioned" },
      storageState: storageState(),
    }),
    stateError("maintenance_failed"),
  );
  assert.equal(fixture.headAnchorTracker.head.generation, "1");
  assert.equal(fixture.headAnchorTracker.head.stateRevision, "1");
  assert.equal(fixture.headAnchorTracker.head.frameCount, 0);

  const committed = await fixture.createState().commitOperation({
    operationId: "operation-rotation-cleanup-001",
    request,
    result: { proofId: "proof-cleanup", status: "provisioned" },
    storageState: storageState(),
  });
  assert.equal(committed.state, "committed");
});

test("a definite user commit survives lock-release cleanup failure", async (t) => {
  let held = false;
  const acquireLock = async () => {
    if (held) {
      const error = new Error("busy");
      error.code = "lock_unavailable";
      throw error;
    }
    held = true;
    return {
      async assertHeld() {
        if (!held) throw new Error("lock lost");
      },
      async release() {
        held = false;
        throw new Error("release acknowledgement lost");
      },
    };
  };
  const fixture = await createFixture(t, { acquireLock });
  const prepared = await fixture.state.prepareOperation({
    kind: "provision",
    operationId: "operation-release-cleanup-001",
    request: operationRequest("provision"),
    storageId: "storage-001",
  });
  assert.equal(prepared.state, "prepared");
  assert.equal(fixture.headAnchorTracker.head.stateRevision, "1");
});

test("concurrent instances expose one prepared CAS winner", async (t) => {
  const fixture = await createFixture(t);
  const first = fixture.createState();
  const second = fixture.createState();
  const input = {
    kind: "provision",
    operationId: "operation-concurrent-cas-001",
    request: operationRequest("provision"),
    storageId: "storage-001",
  };
  const results = await Promise.all([
    first.prepareOperation(input),
    second.prepareOperation(input),
  ]);
  assert.deepEqual(
    results.map((result) => result.shouldDispatch).sort(),
    [false, true],
  );
  assert.equal(fixture.headAnchorTracker.advances, 1);
  assert.equal(fixture.headAnchorTracker.head.stateRevision, "1");
});

test("external head selects generations without scanning orphan names", async (t) => {
  await t.test("generation-zero active state removes only its orphan checkpoint", async (t) => {
    const fixture = await createFixture(t);
    await fixture.state.prepareOperation({
      kind: "provision",
      operationId: "operation-orphan-checkpoint-zero-001",
      request: operationRequest("provision"),
      storageId: "storage-001",
    });
    const checkpointPath = join(
      fixture.directory,
      filesystemImageProviderStateCheckpointName("0"),
    );
    await writeFile(checkpointPath, "orphan", { mode: 0o600 });
    assert.equal((await fixture.state.snapshot()).sequence, 1);
    await assert.rejects(
      readFile(checkpointPath),
      (error) => error?.code === "ENOENT",
    );
    assert((await readFile(fixture.ledgerPath)).length > 0);
  });

  await t.test("mixed next-generation candidate is removed before rotation", async (t) => {
    const fixture = await createFixture(t);
    const state = fixture.createState({
      rotationPolicy: {
        activeLedgerBytesWatermark: 64 * 1024 * 1024,
        activeFrameCountWatermark: 1,
      },
    });
    const request = operationRequest("provision");
    await state.prepareOperation({
      kind: "provision",
      operationId: "operation-mixed-next-generation-001",
      request,
      storageId: "storage-001",
    });
    await writeFile(
      join(
        fixture.directory,
        filesystemImageProviderStateCheckpointName("1"),
      ),
      "partial-candidate",
      { mode: 0o600 },
    );
    const committed = await state.commitOperation({
      operationId: "operation-mixed-next-generation-001",
      request,
      result: { proofId: "proof-mixed-next", status: "provisioned" },
      storageState: storageState(),
    });
    assert.equal(committed.state, "committed");
    assert.equal(fixture.headAnchorTracker.head.generation, "1");
  });

  await t.test("old generation is cleaned while a distant orphan is ignored", async (t) => {
    const { checkpointPath, fixture, ledgerPath, state } =
      await createRotatedFixture(t);
    const oldCheckpoint = join(
      fixture.directory,
      filesystemImageProviderStateCheckpointName("0"),
    );
    await writeFile(oldCheckpoint, "old-checkpoint", { mode: 0o600 });
    await writeFile(fixture.ledgerPath, "old-ledger", { mode: 0o600 });
    const distantCheckpoint = join(
      fixture.directory,
      filesystemImageProviderStateCheckpointName("9"),
    );
    const distantLedger = join(
      fixture.directory,
      filesystemImageProviderStateLedgerName("9"),
    );
    await writeFile(distantCheckpoint, "distant-checkpoint", { mode: 0o600 });
    await writeFile(distantLedger, "distant-ledger", { mode: 0o600 });

    assert.equal((await state.snapshot()).sequence, 2);
    for (const oldPath of [oldCheckpoint, fixture.ledgerPath]) {
      await assert.rejects(
        readFile(oldPath),
        (error) => error?.code === "ENOENT",
      );
    }
    assert.equal(await readFile(distantCheckpoint, "utf8"), "distant-checkpoint");
    assert.equal(await readFile(distantLedger, "utf8"), "distant-ledger");
    assert((await readFile(checkpointPath)).length > 0);
    assert((await readFile(ledgerPath)).length > 0);
  });

  await t.test("missing selected active log fails closed", async (t) => {
    const { fixture, ledgerPath } = await createRotatedFixture(t);
    await rm(ledgerPath);
    await assert.rejects(
      fixture.createState().snapshot(),
      stateError("corrupt_ledger"),
    );
  });
});

test("checkpoint access policy and content revalidation fail closed", async (t) => {
  await t.test("symlink", async (t) => {
    const { checkpointPath, fixture } = await createRotatedFixture(t);
    const target = join(fixture.root, "checkpoint-target");
    await rename(checkpointPath, target);
    await symlink(target, checkpointPath);
    await assert.rejects(
      fixture.createState().snapshot(),
      stateError("corrupt_ledger"),
    );
  });

  await t.test("hard link", async (t) => {
    const { checkpointPath, fixture } = await createRotatedFixture(t);
    const target = join(fixture.root, "checkpoint-target");
    await rename(checkpointPath, target);
    await link(target, checkpointPath);
    await assert.rejects(
      fixture.createState().snapshot(),
      stateError("corrupt_ledger"),
    );
  });

  await t.test("mode change", async (t) => {
    const { checkpointPath, fixture } = await createRotatedFixture(t);
    await chmod(checkpointPath, 0o644);
    await assert.rejects(
      fixture.createState().snapshot(),
      stateError("corrupt_ledger"),
    );
  });

  await t.test("same-instance object replacement", async (t) => {
    const { checkpointPath, fixture, state } = await createRotatedFixture(t);
    const bytes = await readFile(checkpointPath);
    await rename(checkpointPath, join(fixture.root, "replaced-checkpoint"));
    await writeFile(checkpointPath, bytes, { mode: 0o600 });
    await assert.rejects(state.snapshot(), stateError("corrupt_ledger"));
  });

  await t.test("metadata-only churn with exact bytes", async (t) => {
    const { checkpointPath, state } = await createRotatedFixture(t);
    const bytes = await readFile(checkpointPath);
    const changedTime = new Date(Date.now() - 60_000);
    await utimes(checkpointPath, changedTime, changedTime);
    assert.equal((await state.snapshot()).sequence, 2);
    assert.deepEqual(await readFile(checkpointPath), bytes);
  });

  await t.test("content mutation", async (t) => {
    const { checkpointPath, fixture } = await createRotatedFixture(t);
    const bytes = await readFile(checkpointPath);
    bytes[48] ^= 0xff;
    await writeFile(checkpointPath, bytes);
    await assert.rejects(
      fixture.createState().snapshot(),
      stateError("corrupt_ledger"),
    );
  });
});

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

test("reads storage by mount path without traversing permanent operation history", async (t) => {
  const forEachDescriptor = Object.getOwnPropertyDescriptor(
    Map.prototype,
    "forEach",
  );
  let countOperationIterations = false;
  let operationIterations = 0;
  Object.defineProperty(Map.prototype, "forEach", {
    ...forEachDescriptor,
    value(callback, thisArg) {
      if (countOperationIterations) {
        let operationShaped = false;
        Reflect.apply(forEachDescriptor.value, this, [
          (value) => {
            if (
              value !== null &&
              typeof value === "object" &&
              Object.hasOwn(value, "operationId")
            ) {
              operationShaped = true;
            }
          },
        ]);
        if (operationShaped) operationIterations += 1;
      }
      return Reflect.apply(forEachDescriptor.value, this, [callback, thisArg]);
    },
  });
  let LookupState;
  try {
    ({ FilesystemImageProviderState: LookupState } = await import(
      "../src/filesystem-image-provider-state.mjs?storage-lookup-tripwire"
    ));
  } finally {
    Object.defineProperty(Map.prototype, "forEach", forEachDescriptor);
  }

  const fixture = await createFixture(t);
  const state = new LookupState({
    acquireLock: fixture.acquireLock,
    directory: fixture.directory,
    headAnchor: fixture.headAnchor,
    ...TRUSTED_ACL_INSPECTORS,
  });
  await prepareAndCommit(state);
  for (let index = 0; index < 128; index += 1) {
    const operationId = `operation-history-${String(index).padStart(4, "0")}`;
    const request = operationRequest("checkpoint", "storage-001", { index });
    await state.prepareOperation({
      kind: "checkpoint",
      operationId,
      request,
      storageId: "storage-001",
    });
    await state.commitOperation({
      operationId,
      request,
      result: { status: "checkpointed" },
      storageState: storageState({ revision: String(index + 2) }),
    });
  }
  assert.equal((await state.inspectCapacity()).retainedOperationCount, 129);

  countOperationIterations = true;
  const storage = await state.readStorageByMountPath({
    backendId: "filesystem-image-ext4",
    mountPath: mount().mountPath,
  });
  countOperationIterations = false;
  assert.equal(storage.storageId, "storage-001");
  assert.equal(operationIterations, 0);
});

test("mount-path lookup rejects ambiguity and uses the 4095-byte path domain", async (t) => {
  const fixture = await createFixture(t);
  const validMount = {
    ...mount(),
    mountPath: `/${"m".repeat(4_094)}`,
  };
  assert.equal(Buffer.byteLength(validMount.mountPath, "utf8"), 4_095);
  await prepareAndCommit(fixture.state, {
    storage: storageState({ mount: validMount }),
  });
  assert.equal(
    (await fixture.state.readStorageByMountPath({
      backendId: "filesystem-image-ext4",
      mountPath: validMount.mountPath,
    })).storageId,
    "storage-001",
  );
  assert.equal(
    await fixture.state.readStorageByMountPath({
      backendId: "another-backend",
      mountPath: validMount.mountPath,
    }),
    null,
  );
  await assert.rejects(
    fixture.state.readStorageByMountPath({
      backendId: "filesystem-image-ext4",
      mountPath: `/${"m".repeat(4_095)}`,
    }),
    stateError("invalid_request"),
  );

  await prepareAndCommit(fixture.state, {
    operationId: "operation-provision-duplicate-mount-002",
    request: operationRequest("provision", "storage-002"),
    storage: storageState({
      mount: validMount,
      storageId: "storage-002",
    }),
  });
  await assert.rejects(
    fixture.state.readStorageByMountPath({
      backendId: "filesystem-image-ext4",
      mountPath: validMount.mountPath,
    }),
    stateError("storage_lookup_ambiguous"),
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
  fixture.headAnchorTracker.failReadAfterFailedAdvance = true;
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
  fixture.headAnchorTracker.failReadAfterFailedAdvance = false;
  fixture.headAnchorTracker.failRead = false;
  const unanchored = await readFile(fixture.ledgerPath);
  assert(unanchored.length > 0);
  assert.equal(fixture.headAnchorTracker.head.stateRevision, "0");

  const restarted = fixture.createState();
  const snapshot = await restarted.snapshot();
  assert.equal(snapshot.sequence, 0);
  assert.deepEqual(snapshot.operations, []);
  await assert.rejects(
    readFile(fixture.ledgerPath),
    (error) => error?.code === "ENOENT",
  );
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
  assert.equal(fixture.headAnchorTracker.head.stateRevision, "1");

  fixture.headAnchorTracker.failAdvanceBeforeCommit = true;
  fixture.headAnchorTracker.failReadAfterFailedAdvance = true;
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
  fixture.headAnchorTracker.failReadAfterFailedAdvance = false;
  fixture.headAnchorTracker.failRead = false;
  const unanchored = await readFile(fixture.ledgerPath);
  assert(unanchored.length > anchoredPrepared.length);
  assert.equal(fixture.headAnchorTracker.head.stateRevision, "1");

  const restarted = fixture.createState();
  const snapshot = await restarted.snapshot();
  assert.equal(snapshot.sequence, 1);
  assert.equal(snapshot.operations.length, 1);
  assert.equal(snapshot.operations[0].state, "prepared");
  assert.deepEqual(snapshot.storages, []);
  assert.deepEqual(await readFile(fixture.ledgerPath), anchoredPrepared);
});

test("cold restart follows the external head for orphan and invalid tails", async (t) => {
  await t.test("genesis head discards an orphan generation-zero log", async (t) => {
    const fixture = await createFixture(t);
    await prepareAndCommit(fixture.state);
    const staleAnchor = createFixedTrustedHeadAnchor(genesisHead());

    const snapshot = await fixture
      .createState({ headAnchor: staleAnchor })
      .snapshot();
    assert.equal(snapshot.sequence, 0);
    assert.deepEqual(snapshot.operations, []);
    await assert.rejects(
      readFile(fixture.ledgerPath),
      (error) => error?.code === "ENOENT",
    );
  });

  await t.test("checksum-chain mismatch in one complete frame", async (t) => {
    const fixture = await createFixture(t);
    await prepareAndCommit(fixture.state);
    const anchored = await readFile(fixture.ledgerPath);
    fixture.headAnchorTracker.failAdvanceBeforeCommit = true;
    fixture.headAnchorTracker.failReadAfterFailedAdvance = true;
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
    fixture.headAnchorTracker.failReadAfterFailedAdvance = false;
    fixture.headAnchorTracker.failRead = false;
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
  fixture.headAnchorTracker.failReadAfterFailedAdvance = true;
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
  fixture.headAnchorTracker.failReadAfterFailedAdvance = false;
  fixture.headAnchorTracker.failRead = false;
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
          "portable-codex/filesystem-image-provider-state/frame/v2\0",
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

test("directory fsync failure before genesis CAS reports failed cleanup without commit", async (t) => {
  const fixture = await createFixture(t);
  await fixture.state.snapshot();
  const failingAcknowledger = fixture.createState({
    syncDirectory: async () => {
      throw new Error("directory acknowledgement unavailable");
    },
  });

  await assert.rejects(
    failingAcknowledger.prepareOperation({
      kind: "provision",
      operationId: "operation-dir-fsync-before-cas-001",
      request: operationRequest("provision"),
      storageId: "storage-001",
    }),
    stateError("maintenance_failed"),
  );
  assert.deepEqual(fixture.headAnchorTracker.head, copyLedgerHead(genesisHead()));
  assert.equal(
    await fixture.createState().readOperation({
      operationId: "operation-dir-fsync-before-cas-001",
    }),
    null,
  );
});

test("lost trusted-anchor acknowledgement resolves by exact head readback", async (t) => {
  const fixture = await createFixture(t);
  const request = operationRequest("provision");
  await fixture.state.prepareOperation({
    kind: "provision",
    operationId: "operation-anchor-ack-loss-001",
    request,
    storageId: "storage-001",
  });
  fixture.headAnchorTracker.loseNextAdvanceAcknowledgement = true;

  const committed = await fixture.state.commitOperation({
    operationId: "operation-anchor-ack-loss-001",
    request,
    result: { proofId: "proof-anchor-ack-loss", status: "provisioned" },
    storageState: storageState(),
  });
  assert.equal(committed.state, "committed");
  assert.equal(committed.replayed, false);
  assert.equal(fixture.headAnchorTracker.head.stateRevision, "2");

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
    await fixture.state.prepareOperation({
      kind: "provision",
      operationId: "operation-replacement-pin-001",
      request: operationRequest("provision"),
      storageId: "storage-001",
    });
    await rename(fixture.ledgerPath, join(fixture.directory, "replaced.log"));
    await writeFile(fixture.ledgerPath, "", { mode: 0o600 });
    await assert.rejects(fixture.state.snapshot(), stateError("corrupt_ledger"));
  });
});

test("trusted head collaborators and values are exact, receiver-safe, and promise-bound", async (t) => {
  const canonical = genesisHead();
  assert.equal(Object.isFrozen(canonical), true);
  assert.equal(
    filesystemImageProviderStateHeadChecksum(canonical),
    "7bece1a12f8c03f4caf77f9c74e8f839dfeef2e7a1c5beb85ec69a27971d1bb2",
  );
  assert.equal(filesystemImageProviderStateLedgerName("0"), "state.g0.log");
  assert.equal(
    filesystemImageProviderStateCheckpointName("42"),
    "state.g42.checkpoint",
  );
  assert.throws(
    () => filesystemImageProviderStateLedgerName("01"),
    stateError("invalid_request"),
  );
  const generationZeroActive = normalizeFilesystemImageProviderStateHead({
    ...canonical,
    anchorRevision: "1",
    stateRevision: "1",
    frameCount: 1,
    lastChecksum: "a".repeat(64),
    ledgerBytes: 1,
  });
  assert.equal(generationZeroActive.generation, "0");
  assert.equal(generationZeroActive.checkpointChecksum, null);
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
    assert.deepEqual(receivers, [undefined, undefined, undefined, undefined]);
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
      stateError("io_failed"),
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
          stateError("io_failed"),
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
  assert.equal(Object.isFrozen(filesystemImageProviderStateHeadChecksum), true);
  assert.equal(Object.isFrozen(filesystemImageProviderStateCheckpointName), true);
  assert.equal(Object.isFrozen(filesystemImageProviderStateLedgerName), true);
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
          anchorRevision: anchoredHead.anchorRevision,
          generation: anchoredHead.generation,
          stateRevision: anchoredHead.stateRevision,
          baseHeadChecksum: anchoredHead.baseHeadChecksum,
          checkpointStateRevision: anchoredHead.checkpointStateRevision,
          checkpointFrameCount: anchoredHead.checkpointFrameCount,
          checkpointChecksum: anchoredHead.checkpointChecksum,
          checkpointBytes: anchoredHead.checkpointBytes,
          frameCount: anchoredHead.frameCount,
          lastChecksum: anchoredHead.lastChecksum,
          ledgerBytes: anchoredHead.ledgerBytes,
        };
      },
      async compareAndAdvance({ expectedHead, nextHead }) {
        if (
          anchoredHead.contractVersion !== expectedHead.contractVersion ||
          anchoredHead.anchorRevision !== expectedHead.anchorRevision ||
          anchoredHead.generation !== expectedHead.generation ||
          anchoredHead.stateRevision !== expectedHead.stateRevision ||
          anchoredHead.baseHeadChecksum !== expectedHead.baseHeadChecksum ||
          anchoredHead.checkpointStateRevision !== expectedHead.checkpointStateRevision ||
          anchoredHead.checkpointFrameCount !== expectedHead.checkpointFrameCount ||
          anchoredHead.checkpointChecksum !== expectedHead.checkpointChecksum ||
          anchoredHead.checkpointBytes !== expectedHead.checkpointBytes ||
          anchoredHead.frameCount !== expectedHead.frameCount ||
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
  let trustedHead = genesisHead();
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
