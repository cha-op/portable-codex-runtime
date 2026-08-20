import assert from "node:assert/strict";
import { lstat, mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import { FilesystemOperationJournal } from "../src/filesystem-operation-journal.mjs";
import { createPhysicalCollaboratorSettlement } from "../src/physical-collaborator-settlement.mjs";
import {
  POSTGRES_DETACHED_RESTORE_PHYSICAL_BINDINGS_CONTRACT_VERSION,
  POSTGRES_LOGICAL_WRITER_LAUNCH_PHYSICAL_RECEIPT_VERSION,
  POSTGRES_LOGICAL_WRITER_RECONCILE_PHYSICAL_RECEIPT_VERSION,
  POSTGRES_LOGICAL_WRITER_SUPERVISOR_FACADE_CONTRACT_VERSION,
  POSTGRES_LOGICAL_WRITER_SUPERVISOR_PHYSICAL_CONTRACT_VERSION,
  POSTGRES_RESTORE_DESTINATION_RESOLVER_PHYSICAL_CONTRACT_VERSION,
  POSTGRES_SESSION_STORAGE_PHYSICAL_INVOCATION_CONTRACT_VERSION,
  POSTGRES_WRITER_SUPERVISOR_STATE_COLLECTION_PHYSICAL_CONTRACT_VERSION,
  PostgresDetachedRestorePhysicalBindingsError,
  createPostgresDetachedRestorePhysicalBindings,
  isPostgresDetachedRestorePhysicalBindings,
  isPostgresDetachedRestorePublicationBinding,
} from "../src/postgres-detached-restore-physical-bindings.mjs";
import { StoppedDirectoryPublication } from "../src/stopped-directory-publication.mjs";
import { STOPPED_WRITER_STOP_CONFIRMED } from "../src/stopped-writer-capability.mjs";

const LIFECYCLE_METHODS = Object.freeze([
  "captureCheckpoint",
  "destroySession",
  "detachAttachment",
  "forceFence",
  "prepareRestoreAttachment",
  "prepareWritableAttachment",
  "provisionSession",
  "reconcileRestoreAttachment",
  "restoreCheckpoint",
]);
const PUBLICATION_METHODS = Object.freeze([
  "publishFreshCheckpointArtifact",
  "publishRestoreDestination",
  "verifyCommittedCheckpointArtifact",
  "verifyCommittedRestoreDestination",
]);
const SUPERVISOR_METHODS = Object.freeze([
  "launchWriter",
  "reconcileWriterLaunch",
  "stopWriter",
]);
const POLICY = Object.freeze({
  deadlineMilliseconds: 1_000,
  settlementGraceMilliseconds: 1_000,
});
const STATE_OWNER_ID = `state-owner:${"d".repeat(64)}`;

function terminalRecord({
  launchAttemptId = "attempt-001",
  processIncarnationId = "process-001",
  proofId = "proof-001",
  stopOperationId = "stop-001",
  writerIncarnationId = "writer-001",
} = {}) {
  return exact({
    containerId: "a".repeat(64),
    containerName: "codex-writer-fixture",
    contractVersion: 1,
    launchAttemptId,
    processIncarnationId,
    proofId,
    requestSha256: "b".repeat(64),
    revision: 4,
    status: "stopped",
    stopOperationId,
    stopProofId: "stop-proof-001",
    writerIncarnationId,
  });
}

function exact(value) {
  return Object.freeze(Object.assign(Object.create(null), value));
}

function deferred() {
  let resolve;
  const promise = new Promise((resolvePromise) => {
    resolve = resolvePromise;
  });
  return { promise, resolve };
}

function policyRegistry(methods, override = {}) {
  const value = Object.create(null);
  for (const method of methods) value[method] = override[method] ?? POLICY;
  return Object.freeze(value);
}

class PrototypeLifecycleBackend {
  constructor(events) {
    Object.defineProperties(this, {
      physicalInvocationContractVersion: {
        enumerable: true,
        value: POSTGRES_SESSION_STORAGE_PHYSICAL_INVOCATION_CONTRACT_VERSION,
      },
      prepareRestoreAttachment: {
        enumerable: true,
        value: PrototypeLifecycleBackend.prototype.prepareRestoreAttachment,
      },
      reconcileRestoreAttachment: {
        enumerable: true,
        value: PrototypeLifecycleBackend.prototype.reconcileRestoreAttachment,
      },
      restoreAttachmentActivationContractVersion: {
        enumerable: true,
        value: 1,
      },
      restoreAttachmentReconciliationContractVersion: {
        enumerable: true,
        value: 1,
      },
      events: { value: events },
    });
    Object.freeze(this);
  }
}

Object.assign(PrototypeLifecycleBackend.prototype, {
  backendId: "backend-001",
  capabilities: Object.freeze({
    atomicPointInTimeCheckpoint: true,
    exclusiveWriterAttachment: true,
    fencing: "epoch-enforced",
    normalDirectoryAttachment: true,
  }),
  contractVersion: 1,
  restoreAttachmentActivationContractVersion: 1,
  restoreAttachmentReconciliationContractVersion: 1,
});
for (const method of LIFECYCLE_METHODS) {
  Object.defineProperty(PrototypeLifecycleBackend.prototype, method, {
    configurable: true,
    value: async function lifecycleMethod(request, context) {
      this.events.push({ argumentsLength: arguments.length, context, method, request });
      return exact({ method, status: "ok" });
    },
    writable: true,
  });
}

async function fixture(overrides = {}) {
  const events = [];
  const directory = await mkdtemp(join(tmpdir(), "physical-bindings-"));
  const publication = new StoppedDirectoryPublication({
    journal: new FilesystemOperationJournal({ directory }),
  });
  const supervisor = {
    contractVersion: POSTGRES_LOGICAL_WRITER_SUPERVISOR_PHYSICAL_CONTRACT_VERSION,
    stateOwnerId: STATE_OWNER_ID,
    supervisorId: "supervisor-001",
    async launchWriter(input) {
      events.push({ argumentsLength: arguments.length, input, method: "launchWriter" });
      return exact({
        evidence: exact({
          contractVersion:
            POSTGRES_LOGICAL_WRITER_SUPERVISOR_PHYSICAL_CONTRACT_VERSION,
          launchAttemptId: input.attempt.launchAttemptId,
          processIncarnationId: "process-001",
          proofId: "proof-001",
          status: "started",
          supervisorId: "supervisor-001",
          writerIncarnationId: "writer-001",
        }),
        receiptVersion:
          POSTGRES_LOGICAL_WRITER_LAUNCH_PHYSICAL_RECEIPT_VERSION,
        stopWriter: async function stopWriter(stopInput) {
          events.push({ argumentsLength: arguments.length, input: stopInput, method: "stopWriter" });
          return exact({
            contractVersion:
              POSTGRES_LOGICAL_WRITER_SUPERVISOR_PHYSICAL_CONTRACT_VERSION,
            status: "stopped",
            terminalRecord: terminalRecord({
              launchAttemptId: input.attempt.launchAttemptId,
              processIncarnationId: stopInput.processIncarnationId,
              stopOperationId: stopInput.stopOperationId,
              writerIncarnationId: stopInput.writerIncarnationId,
            }),
          });
        },
        terminalRecord: null,
      });
    },
    async reconcileWriterLaunch(input) {
      events.push({ argumentsLength: arguments.length, input, method: "reconcileWriterLaunch" });
      return exact({
        evidence: exact({
          contractVersion:
            POSTGRES_LOGICAL_WRITER_SUPERVISOR_PHYSICAL_CONTRACT_VERSION,
          launchAttemptId: input.attempt.launchAttemptId,
          processIncarnationId: null,
          proofId: "proof-002",
          status: "not-started",
          supervisorId: "supervisor-001",
          writerIncarnationId: null,
        }),
        receiptVersion:
          POSTGRES_LOGICAL_WRITER_RECONCILE_PHYSICAL_RECEIPT_VERSION,
        terminalRecord: null,
      });
    },
  };
  const supervisorStateCollector = Object.freeze({
    async collectTerminalState(input) {
      events.push({
        argumentsLength: arguments.length,
        input,
        method: "collectTerminalState",
      });
      return exact({
        contractVersion:
          POSTGRES_WRITER_SUPERVISOR_STATE_COLLECTION_PHYSICAL_CONTRACT_VERSION,
        launchAttemptId: input.terminalRecord.launchAttemptId,
        stateOwnerId: STATE_OWNER_ID,
        status: "collected",
        terminalRecordSha256: "c".repeat(64),
      });
    },
    contractVersion:
      POSTGRES_WRITER_SUPERVISOR_STATE_COLLECTION_PHYSICAL_CONTRACT_VERSION,
    stateOwnerId: STATE_OWNER_ID,
    supervisorId: "supervisor-001",
  });
  const options = {
    lifecycleBackend: new PrototypeLifecycleBackend(events),
    lifecycleSettlement: policyRegistry(LIFECYCLE_METHODS),
    onFatal() {
      events.push({ method: "fatal" });
    },
    publication,
    publicationSettlement: policyRegistry(PUBLICATION_METHODS),
    async resolveRestoreDestination(input) {
      events.push({ argumentsLength: arguments.length, input, method: "resolver" });
      return exact({
        destinationDirectory: "/tmp/destination",
        destinationOwnedRoot: "/tmp",
      });
    },
    resolveRestoreDestinationContractVersion:
      POSTGRES_RESTORE_DESTINATION_RESOLVER_PHYSICAL_CONTRACT_VERSION,
    resolveRestoreDestinationSettlement: POLICY,
    supervisor: Object.freeze(supervisor),
    supervisorSettlement: policyRegistry(SUPERVISOR_METHODS),
    supervisorStateCollectionSettlement: POLICY,
    supervisorStateCollector,
    ...overrides,
  };
  return {
    binding: createPostgresDetachedRestorePhysicalBindings(options),
    events,
    options,
  };
}

function ownKeys(value) {
  return Reflect.ownKeys(value).sort();
}

function assertFrozenNullRecord(value, keys) {
  assert.equal(Object.getPrototypeOf(value), null);
  assert.equal(Object.isFrozen(value), true);
  assert.deepEqual(ownKeys(value), [...keys].sort());
}

function physicalInput(base) {
  assertFrozenNullRecord(base, [
    "attempt",
    "contractVersion",
    "invocation",
    "signal",
  ]);
  assert.equal(
    base.contractVersion,
    POSTGRES_LOGICAL_WRITER_SUPERVISOR_PHYSICAL_CONTRACT_VERSION,
  );
  assertFrozenNullRecord(base.invocation, []);
  assert.equal(base.signal instanceof AbortSignal, true);
  assert.equal(base.signal.aborted, false);
}

function lifecycleWithMethod(
  events,
  selectedMethod,
  callback,
  capabilities = PrototypeLifecycleBackend.prototype.capabilities,
) {
  const backend = Object.create(null);
  Object.assign(backend, {
    backendId: "backend-001",
    capabilities,
    contractVersion: 1,
    physicalInvocationContractVersion: 1,
    restoreAttachmentActivationContractVersion: 1,
    restoreAttachmentReconciliationContractVersion: 1,
  });
  for (const method of LIFECYCLE_METHODS) {
    backend[method] =
      method === selectedMethod
        ? callback
        : async function lifecycleMethod(request, context) {
            events.push({ context, method, request });
            return exact({ method, status: "ok" });
          };
  }
  return Object.freeze(backend);
}

async function physicalPublicationFixture(t) {
  const root = await mkdtemp(join(tmpdir(), "physical-publication-"));
  t.after(() => rm(root, { force: true, recursive: true }));
  const sourceOwnedRoot = join(root, "source");
  const artifactOwnedRoot = join(root, "artifact");
  const journalDirectory = join(root, "journal");
  await mkdir(join(sourceOwnedRoot, "session", "workspace"), {
    mode: 0o700,
    recursive: true,
  });
  await mkdir(artifactOwnedRoot, { mode: 0o700 });
  await mkdir(journalDirectory, { mode: 0o700 });
  await writeFile(
    join(sourceOwnedRoot, "session", "workspace", "state"),
    "ok\n",
    { mode: 0o600 },
  );
  let enterAcquire;
  const acquireEntered = new Promise((resolve) => {
    enterAcquire = resolve;
  });
  const lock = async () => {
    enterAcquire();
    return new Promise(() => {});
  };
  const journal = new FilesystemOperationJournal({
    acquireLock: lock,
    directory: journalDirectory,
    inspectAncestorAcl: async () => false,
    inspectDirectoryAcl: async () => false,
  });
  const publication = new StoppedDirectoryPublication({
    acquireLock: lock,
    faults: {},
    inspectFilesystem: async () => ({
      durability: "local-fsync-rename",
      filesystemId: "filesystem-001",
      objectIdentityScheme: "test-object-v1",
      type: "test-local",
    }),
    inspectOwnedRootAcl: async () => false,
    inspectOwnedRootAncestorAcl: async () => false,
    inspectPersistentObjectIdentity: async (path) => {
      const metadata = await lstat(path, { bigint: true });
      return {
        device: metadata.dev.toString(),
        inode: metadata.ino.toString(),
        objectId: `object-${metadata.dev}-${metadata.ino}`,
      };
    },
    journal,
    listMountPoints: async () => ["/"],
  });
  const request = {
    backendId: "backend-001",
    contractVersion: 1,
    fencingEpoch: "1",
    holderId: "holder-001",
    leaseId: "lease-001",
    operation: "checkpoint",
    operationId: "capture-001",
    sessionId: "019f2100-0000-7000-8000-000000000001",
    storageId: "storage-001",
    target: {
      artifactId: "artifact-001",
      checkpointId: "checkpoint-001",
      kind: "checkpoint",
    },
  };
  const checkpoint = {
    artifactId: "artifact-001",
    backendId: "backend-001",
    checkpointClass: "clean",
    checkpointId: "checkpoint-001",
    codexSessionId: "019f2100-0000-7000-8000-000000000002",
    codexThreadId: "019f2100-0000-7000-8000-000000000002",
    contractVersion: 1,
    createdAt: "2026-08-12T12:00:00.000Z",
    imageDigest: `sha256:${"a".repeat(64)}`,
    sessionId: request.sessionId,
    sourceFencingEpoch: "1",
    storageId: request.storageId,
  };
  return {
    acquireEntered,
    options: {
      artifactDirectory: join(artifactOwnedRoot, "artifact-001"),
      artifactOwnedRoot,
      binding: {
        backendId: request.backendId,
        operation: request.operation,
        operationId: request.operationId,
        sessionId: request.sessionId,
        storageId: request.storageId,
      },
      operationId: request.operationId,
      request,
      result: {
        checkpoint,
        mutation: { ...request, proofId: "proof-001", status: "checkpoint-created" },
      },
      sourceDirectory: join(sourceOwnedRoot, "session"),
      sourceOwnedRoot,
    },
    publication,
  };
}

test("constructs exact branded facades and maps physical supervisor v5 to launcher v4", async () => {
  const { binding, events } = await fixture();
  assert.equal(POSTGRES_DETACHED_RESTORE_PHYSICAL_BINDINGS_CONTRACT_VERSION, 4);
  assert.equal(POSTGRES_LOGICAL_WRITER_SUPERVISOR_PHYSICAL_CONTRACT_VERSION, 5);
  assert.equal(POSTGRES_LOGICAL_WRITER_SUPERVISOR_FACADE_CONTRACT_VERSION, 4);
  assert.equal(POSTGRES_LOGICAL_WRITER_LAUNCH_PHYSICAL_RECEIPT_VERSION, 2);
  assert.equal(POSTGRES_LOGICAL_WRITER_RECONCILE_PHYSICAL_RECEIPT_VERSION, 2);
  assert.equal(
    POSTGRES_WRITER_SUPERVISOR_STATE_COLLECTION_PHYSICAL_CONTRACT_VERSION,
    2,
  );
  assert.equal(POSTGRES_SESSION_STORAGE_PHYSICAL_INVOCATION_CONTRACT_VERSION, 1);
  assert.equal(POSTGRES_RESTORE_DESTINATION_RESOLVER_PHYSICAL_CONTRACT_VERSION, 1);
  assertFrozenNullRecord(binding, [
    "contractVersion",
    "lifecycleBackend",
    "publication",
    "resolveRestoreDestination",
    "stop",
    "supervisor",
    "supervisorStateCollector",
  ]);
  assert.equal(isPostgresDetachedRestorePhysicalBindings(binding), true);
  assert.equal(isPostgresDetachedRestorePublicationBinding(binding.publication), true);
  assertFrozenNullRecord(binding.supervisor, [
    "contractVersion",
    "launchWriter",
    "reconcileWriterLaunch",
    "stateOwnerId",
    "supervisorId",
  ]);
  assert.equal(binding.supervisor.contractVersion, 4);
  assert.equal(binding.supervisor.stateOwnerId, STATE_OWNER_ID);
  assertFrozenNullRecord(binding.supervisorStateCollector, [
    "collectTerminalState",
    "contractVersion",
    "stateOwnerId",
    "supervisorId",
  ]);
  assert.equal(binding.supervisorStateCollector.contractVersion, 2);
  assert.equal(
    binding.supervisorStateCollector.stateOwnerId,
    binding.supervisor.stateOwnerId,
  );
  assert.equal(
    binding.supervisorStateCollector.supervisorId,
    binding.supervisor.supervisorId,
  );
  assertFrozenNullRecord(binding.lifecycleBackend, [
    "backendId",
    "capabilities",
    "captureCheckpoint",
    "contractVersion",
    "destroySession",
    "detachAttachment",
    "forceFence",
    "prepareRestoreAttachment",
    "prepareWritableAttachment",
    "provisionSession",
    "reconcileRestoreAttachment",
    "restoreAttachmentActivationContractVersion",
    "restoreAttachmentReconciliationContractVersion",
    "restoreCheckpoint",
  ]);
  assertFrozenNullRecord(binding.publication, PUBLICATION_METHODS);

  const launch = await binding.supervisor.launchWriter(
    exact({ attempt: exact({ launchAttemptId: "attempt-001" }), contractVersion: 1 }),
  );
  assertFrozenNullRecord(launch, [
    "evidence",
    "receiptVersion",
    "stopWriter",
    "terminalRecord",
  ]);
  assert.equal(launch.receiptVersion, 2);
  assert.equal(launch.evidence.contractVersion, 1);
  assert.equal(launch.evidence.status, "started");
  assert.equal(launch.terminalRecord, null);
  const launchEvent = events.find((event) => event.method === "launchWriter");
  assert.equal(launchEvent.argumentsLength, 1);
  physicalInput(launchEvent.input);

  const stopped = await launch.stopWriter(exact({
    attachment: exact({ attachmentId: "attachment-001" }),
    processIncarnationId: "process-001",
    stopOperationId: "stop-001",
    writerFence: exact({ fencingEpoch: "1" }),
    writerIncarnationId: "writer-001",
  }));
  assertFrozenNullRecord(stopped, [
    "confirmation",
    "contractVersion",
    "terminalRecord",
  ]);
  assert.equal(stopped.confirmation, STOPPED_WRITER_STOP_CONFIRMED);
  assert.equal(stopped.contractVersion, 4);
  assert.equal(stopped.terminalRecord.status, "stopped");
  assert.equal(stopped.terminalRecord.revision, 4);
  const stopEvent = events.find((event) => event.method === "stopWriter");
  assert.equal(stopEvent.argumentsLength, 1);
  assert.equal(stopEvent.input.contractVersion, 5);
  assertFrozenNullRecord(stopEvent.input, [
    "attachment",
    "contractVersion",
    "invocation",
    "processIncarnationId",
    "signal",
    "stopOperationId",
    "writerFence",
    "writerIncarnationId",
  ]);
  assertFrozenNullRecord(stopEvent.input.invocation, []);

  const collected = await binding.supervisorStateCollector.collectTerminalState(
    exact({
      stateOwnerId: binding.supervisor.stateOwnerId,
      terminalRecord: stopped.terminalRecord,
    }),
  );
  assertFrozenNullRecord(collected, [
    "contractVersion",
    "launchAttemptId",
    "stateOwnerId",
    "status",
    "terminalRecordSha256",
  ]);
  assert.equal(collected.status, "collected");
  const collectionEvent = events.find(
    (event) => event.method === "collectTerminalState",
  );
  assert.equal(collectionEvent.argumentsLength, 1);
  assertFrozenNullRecord(collectionEvent.input, [
    "contractVersion",
    "invocation",
    "signal",
    "stateOwnerId",
    "terminalRecord",
  ]);
  assert.equal(collectionEvent.input.contractVersion, 2);
  assert.equal(collectionEvent.input.stateOwnerId, STATE_OWNER_ID);
  assertFrozenNullRecord(collectionEvent.input.invocation, []);
  assert.equal(collectionEvent.input.signal instanceof AbortSignal, true);

  const reconcile = await binding.supervisor.reconcileWriterLaunch(
    exact({ attempt: exact({ launchAttemptId: "attempt-002" }), contractVersion: 1 }),
  );
  assert.equal(reconcile.evidence.contractVersion, 1);
  assert.equal(reconcile.evidence.status, "not-started");
  assert.equal(
    reconcile.receiptVersion,
    POSTGRES_LOGICAL_WRITER_RECONCILE_PHYSICAL_RECEIPT_VERSION,
  );
  assert.equal(reconcile.terminalRecord, null);
  assert.equal(events.find((event) => event.method === "reconcileWriterLaunch").argumentsLength, 1);

  const lifecycle = await binding.lifecycleBackend.provisionSession(exact({ operationId: "op-001" }));
  assertFrozenNullRecord(lifecycle, ["method", "status"]);
  const lifecycleEvent = events.find((event) => event.method === "provisionSession");
  assert.equal(lifecycleEvent.argumentsLength, 2);
  assertFrozenNullRecord(lifecycleEvent.context, ["contractVersion", "invocation", "signal"]);

  const resolved = await binding.resolveRestoreDestination(
    exact({ candidate: exact({ id: "candidate" }), generation: exact({ id: "generation" }), kind: "fresh" }),
  );
  assertFrozenNullRecord(resolved, ["destinationDirectory", "destinationOwnedRoot"]);
  const resolverEvent = events.find((event) => event.method === "resolver");
  assert.equal(resolverEvent.argumentsLength, 1);
  assert.equal(resolverEvent.input.contractVersion, 1);
  assertFrozenNullRecord(resolverEvent.input.invocation, []);
});

test("classifies launch terminal records only for owner complete-stopped receipts", async () => {
  const base = await fixture();
  const completeRecord = terminalRecord({ launchAttemptId: "attempt-complete" });
  const complete = await fixture({
    supervisor: Object.freeze({
      ...base.options.supervisor,
      async launchWriter(input) {
        return exact({
          evidence: exact({
            contractVersion:
              POSTGRES_LOGICAL_WRITER_SUPERVISOR_PHYSICAL_CONTRACT_VERSION,
            launchAttemptId: input.attempt.launchAttemptId,
            processIncarnationId: completeRecord.processIncarnationId,
            proofId: completeRecord.stopProofId,
            status: "complete-stopped",
            supervisorId: "supervisor-001",
            writerIncarnationId: completeRecord.writerIncarnationId,
          }),
          receiptVersion:
            POSTGRES_LOGICAL_WRITER_LAUNCH_PHYSICAL_RECEIPT_VERSION,
          stopWriter: null,
          terminalRecord: completeRecord,
        });
      },
    }),
  });
  const completed = await complete.binding.supervisor.launchWriter(
    exact({
      attempt: exact({ launchAttemptId: "attempt-complete" }),
      contractVersion: 1,
    }),
  );
  assert.equal(completed.evidence.status, "complete-stopped");
  assert.deepEqual(completed.terminalRecord, completeRecord);
  assert.equal(completed.stopWriter, null);

  const notStarted = await fixture({
    supervisor: Object.freeze({
      ...base.options.supervisor,
      async launchWriter(input) {
        return exact({
          evidence: exact({
            contractVersion:
              POSTGRES_LOGICAL_WRITER_SUPERVISOR_PHYSICAL_CONTRACT_VERSION,
            launchAttemptId: input.attempt.launchAttemptId,
            processIncarnationId: null,
            proofId: "not-started-proof",
            status: "not-started",
            supervisorId: "supervisor-001",
            writerIncarnationId: null,
          }),
          receiptVersion:
            POSTGRES_LOGICAL_WRITER_LAUNCH_PHYSICAL_RECEIPT_VERSION,
          stopWriter: null,
          terminalRecord: null,
        });
      },
    }),
  });
  const absent = await notStarted.binding.supervisor.launchWriter(
    exact({
      attempt: exact({ launchAttemptId: "attempt-absent" }),
      contractVersion: 1,
    }),
  );
  assert.equal(absent.evidence.status, "not-started");
  assert.equal(absent.terminalRecord, null);

  for (const invalidReceipt of [
    exact({
      evidence: exact({
        contractVersion:
          POSTGRES_LOGICAL_WRITER_SUPERVISOR_PHYSICAL_CONTRACT_VERSION,
        launchAttemptId: "attempt-invalid",
        processIncarnationId: completeRecord.processIncarnationId,
        proofId: completeRecord.stopProofId,
        status: "complete-stopped",
        supervisorId: "supervisor-001",
        writerIncarnationId: completeRecord.writerIncarnationId,
      }),
      receiptVersion:
        POSTGRES_LOGICAL_WRITER_LAUNCH_PHYSICAL_RECEIPT_VERSION,
      stopWriter: null,
      terminalRecord: null,
    }),
    exact({
      evidence: exact({
        contractVersion:
          POSTGRES_LOGICAL_WRITER_SUPERVISOR_PHYSICAL_CONTRACT_VERSION,
        launchAttemptId: "attempt-invalid",
        processIncarnationId: null,
        proofId: "not-started-proof",
        status: "not-started",
        supervisorId: "supervisor-001",
        writerIncarnationId: null,
      }),
      receiptVersion:
        POSTGRES_LOGICAL_WRITER_LAUNCH_PHYSICAL_RECEIPT_VERSION,
      stopWriter: null,
      terminalRecord: completeRecord,
    }),
  ]) {
    const invalid = await fixture({
      supervisor: Object.freeze({
        ...base.options.supervisor,
        async launchWriter() {
          return invalidReceipt;
        },
      }),
    });
    await assert.rejects(
      invalid.binding.supervisor.launchWriter(exact({
        attempt: exact({ launchAttemptId: "attempt-invalid" }),
        contractVersion: 1,
      })),
      { code: "postgres_detached_restore_physical_bindings_outcome_uncertain" },
    );
  }
});

test("forwards reconciliation terminal records only for matching owner complete-stopped receipts", async () => {
  const base = await fixture();
  const completeRecord = terminalRecord({
    launchAttemptId: "attempt-reconcile-complete",
  });
  const complete = await fixture({
    supervisor: Object.freeze({
      ...base.options.supervisor,
      async reconcileWriterLaunch(input) {
        return exact({
          evidence: exact({
            contractVersion:
              POSTGRES_LOGICAL_WRITER_SUPERVISOR_PHYSICAL_CONTRACT_VERSION,
            launchAttemptId: input.attempt.launchAttemptId,
            processIncarnationId: completeRecord.processIncarnationId,
            proofId: completeRecord.stopProofId,
            status: "complete-stopped",
            supervisorId: "supervisor-001",
            writerIncarnationId: completeRecord.writerIncarnationId,
          }),
          receiptVersion:
            POSTGRES_LOGICAL_WRITER_RECONCILE_PHYSICAL_RECEIPT_VERSION,
          terminalRecord: completeRecord,
        });
      },
    }),
  });
  const completed = await complete.binding.supervisor.reconcileWriterLaunch(
    exact({
      attempt: exact({ launchAttemptId: completeRecord.launchAttemptId }),
      contractVersion: 1,
    }),
  );
  assert.equal(completed.evidence.status, "complete-stopped");
  assert.deepEqual(completed.terminalRecord, completeRecord);

  for (const { evidenceStatus, terminal } of [
    { evidenceStatus: "not-started", terminal: completeRecord },
    {
      evidenceStatus: "complete-stopped",
      terminal: terminalRecord({ launchAttemptId: "attempt-other" }),
    },
  ]) {
    const invalid = await fixture({
      supervisor: Object.freeze({
        ...base.options.supervisor,
        async reconcileWriterLaunch(input) {
          return exact({
            evidence: exact({
              contractVersion:
                POSTGRES_LOGICAL_WRITER_SUPERVISOR_PHYSICAL_CONTRACT_VERSION,
              launchAttemptId: input.attempt.launchAttemptId,
              processIncarnationId:
                evidenceStatus === "complete-stopped"
                  ? completeRecord.processIncarnationId
                  : null,
              proofId:
                evidenceStatus === "complete-stopped"
                  ? completeRecord.stopProofId
                  : "not-started-proof",
              status: evidenceStatus,
              supervisorId: "supervisor-001",
              writerIncarnationId:
                evidenceStatus === "complete-stopped"
                  ? completeRecord.writerIncarnationId
                  : null,
            }),
            receiptVersion:
              POSTGRES_LOGICAL_WRITER_RECONCILE_PHYSICAL_RECEIPT_VERSION,
            terminalRecord: terminal,
          });
        },
      }),
    });
    await assert.rejects(
      invalid.binding.supervisor.reconcileWriterLaunch(exact({
        attempt: exact({
          launchAttemptId: "attempt-reconcile-complete",
        }),
        contractVersion: 1,
      })),
      { code: "postgres_detached_restore_physical_bindings_outcome_uncertain" },
    );
  }
});

test("does not consult a poisoned Array iterator after module initialization", async () => {
  const base = await fixture();
  const descriptor = Object.getOwnPropertyDescriptor(
    Array.prototype,
    Symbol.iterator,
  );
  let poisonCalls = 0;
  Object.defineProperty(Array.prototype, Symbol.iterator, {
    configurable: true,
    value() {
      poisonCalls += 1;
      throw new Error("array iteration must not run");
    },
    writable: true,
  });
  try {
    const binding = createPostgresDetachedRestorePhysicalBindings(base.options);
    assert.equal(poisonCalls, 0);
    const stopped = await binding.stop();
    assert.equal(stopped.status, "stopped");
    assert.equal(Object.getPrototypeOf(stopped), null);
    assert.equal(Object.isFrozen(stopped), true);
    assert.equal(Reflect.ownKeys(stopped).length, 1);
    assert.equal(poisonCalls, 0);
  } finally {
    Object.defineProperty(Array.prototype, Symbol.iterator, descriptor);
  }
});

test("shallow snapshots raw results before facade promise fulfillment without inherited then", async () => {
  let thenCalls = 0;
  const { binding } = await fixture({
    resolveRestoreDestination: () => {
      const result = {
        destinationDirectory: "/tmp/destination",
        destinationOwnedRoot: "/tmp",
      };
      return new Promise((resolve) => {
        resolve(result);
        queueMicrotask(() => {
          Object.defineProperty(Object.prototype, "then", {
            configurable: true,
            value() { thenCalls += 1; },
          });
        });
      });
    },
  });
  try {
    const result = await binding.resolveRestoreDestination(exact({ candidate: 1, generation: 2, kind: "fresh" }));
    assert.equal(thenCalls, 0);
    assertFrozenNullRecord(result, ["destinationDirectory", "destinationOwnedRoot"]);
  } finally {
    delete Object.prototype.then;
  }
});

test("rejects a late own then without executing it or escaping settlement drain", async () => {
  let thenCalls = 0;
  const { binding } = await fixture({
    resolveRestoreDestination: () => {
      const result = {
        destinationDirectory: "/tmp/destination",
        destinationOwnedRoot: "/tmp",
      };
      return new Promise((resolve) => {
        resolve(result);
        queueMicrotask(() => {
          Object.defineProperty(result, "then", {
            enumerable: true,
            value() {
              thenCalls += 1;
            },
          });
        });
      });
    },
  });
  await assert.rejects(
    binding.resolveRestoreDestination(
      exact({ candidate: 1, generation: 2, kind: "fresh" }),
    ),
    { code: "postgres_detached_restore_physical_bindings_outcome_uncertain" },
  );
  assert.equal(thenCalls, 0);
  assert.deepEqual(await binding.stop(), exact({ status: "stopped" }));
});

test("rejects an invalid facade version before dispatch without poisoning settlement", async () => {
  const { binding, events } = await fixture();
  await assert.rejects(
    binding.supervisor.launchWriter(
      exact({ attempt: exact({ launchAttemptId: "attempt-invalid" }), contractVersion: 2 }),
    ),
    { code: "invalid_postgres_detached_restore_physical_bindings_request" },
  );
  assert.equal(events.some((event) => event.method === "launchWriter"), false);
  assert.equal(events.some((event) => event.method === "fatal"), false);

  const valid = await binding.supervisor.launchWriter(
    exact({ attempt: exact({ launchAttemptId: "attempt-valid" }), contractVersion: 1 }),
  );
  assert.equal(valid.evidence.status, "started");
  assert.equal(events.filter((event) => event.method === "launchWriter").length, 1);
  assert.deepEqual(await binding.stop(), exact({ status: "stopped" }));
});

test("rejects hostile values and exact option, policy, request, and outcome violations", async () => {
  const { options, binding } = await fixture();
  assert.throws(
    () =>
      createPostgresDetachedRestorePhysicalBindings({
        ...options,
        supervisor: Object.freeze({
          ...options.supervisor,
          contractVersion: 2,
        }),
      }),
    {
      code: "invalid_postgres_detached_restore_physical_bindings_options",
    },
  );
  assert.throws(
    () => createPostgresDetachedRestorePhysicalBindings({
      ...options,
      supervisor: Object.freeze({
        ...options.supervisor,
        stateOwnerId: "state-owner:short",
      }),
    }),
    { code: "invalid_postgres_detached_restore_physical_bindings_options" },
  );
  assert.throws(
    () => createPostgresDetachedRestorePhysicalBindings({
      ...options,
      supervisorStateCollector: Object.freeze({
        ...options.supervisorStateCollector,
        stateOwnerId: `state-owner:${"e".repeat(64)}`,
      }),
    }),
    { code: "invalid_postgres_detached_restore_physical_bindings_options" },
  );
  assert.throws(
    () => createPostgresDetachedRestorePhysicalBindings({ ...options, extra: true }),
    (error) => error instanceof PostgresDetachedRestorePhysicalBindingsError &&
      error.code === "invalid_postgres_detached_restore_physical_bindings_options" &&
      !Object.hasOwn(error, "cause"),
  );
  assert.throws(
    () => createPostgresDetachedRestorePhysicalBindings({
      ...options,
      resolveRestoreDestinationSettlement: {
        deadlineMilliseconds: 0,
        settlementGraceMilliseconds: 1,
      },
    }),
    { code: "invalid_postgres_detached_restore_physical_bindings_options" },
  );
  await assert.rejects(
    binding.supervisor.launchWriter(),
    { code: "invalid_postgres_detached_restore_physical_bindings_request" },
  );
  await assert.rejects(
    binding.supervisorStateCollector.collectTerminalState(exact({
      stateOwnerId: `state-owner:${"e".repeat(64)}`,
      terminalRecord: terminalRecord(),
    })),
    { code: "invalid_postgres_detached_restore_physical_bindings_request" },
  );
  const wrongReceiptOwner = await fixture({
    supervisorStateCollector: Object.freeze({
      ...options.supervisorStateCollector,
      async collectTerminalState(input) {
        return exact({
          contractVersion:
            POSTGRES_WRITER_SUPERVISOR_STATE_COLLECTION_PHYSICAL_CONTRACT_VERSION,
          launchAttemptId: input.terminalRecord.launchAttemptId,
          stateOwnerId: `state-owner:${"e".repeat(64)}`,
          status: "collected",
          terminalRecordSha256: "c".repeat(64),
        });
      },
    }),
  });
  await assert.rejects(
    wrongReceiptOwner.binding.supervisorStateCollector.collectTerminalState(
      exact({ stateOwnerId: STATE_OWNER_ID, terminalRecord: terminalRecord() }),
    ),
    { code: "postgres_detached_restore_physical_bindings_outcome_uncertain" },
  );
  await wrongReceiptOwner.binding.stop();
  assert.equal(isPostgresDetachedRestorePhysicalBindings(new Proxy({}, {})), false);
  assert.equal(isPostgresDetachedRestorePublicationBinding(Object.freeze(Object.create(null))), false);

  const malformed = await fixture({
    supervisor: Object.freeze({
      ...options.supervisor,
      async launchWriter() {
        return exact({ evidence: exact({ contractVersion: 1 }), receiptVersion: 1, stopWriter: null });
      },
    }),
  });
  await assert.rejects(
    malformed.binding.supervisor.launchWriter(
      exact({ attempt: exact({ launchAttemptId: "attempt-001" }), contractVersion: 1 }),
    ),
    { code: "postgres_detached_restore_physical_bindings_outcome_uncertain" },
  );
});

test("lifecycle accessor metadata is rejected without execution", async () => {
  const base = await fixture();
  let backendReads = 0;
  const lifecycleBackend = Object.create(null);
  Object.defineProperties(lifecycleBackend, {
    backendId: {
      enumerable: true,
      get() {
        backendReads += 1;
        return "backend-001";
      },
    },
    capabilities: {
      enumerable: true,
      value: PrototypeLifecycleBackend.prototype.capabilities,
    },
    contractVersion: { enumerable: true, value: 1 },
    physicalInvocationContractVersion: { enumerable: true, value: 1 },
    prepareRestoreAttachment: { enumerable: true, value: async () => exact({ status: "ok" }) },
    reconcileRestoreAttachment: { enumerable: true, value: async () => exact({ status: "ok" }) },
    restoreAttachmentActivationContractVersion: { enumerable: true, value: 1 },
    restoreAttachmentReconciliationContractVersion: { enumerable: true, value: 1 },
  });
  for (const method of LIFECYCLE_METHODS) {
    if (!Object.hasOwn(lifecycleBackend, method)) {
      Object.defineProperty(lifecycleBackend, method, {
        enumerable: true,
        value: async () => exact({ status: "ok" }),
      });
    }
  }
  assert.throws(
    () => createPostgresDetachedRestorePhysicalBindings({
      ...base.options,
      lifecycleBackend,
    }),
    { code: "invalid_postgres_detached_restore_physical_bindings_options" },
  );
  assert.equal(backendReads, 0);
});

test("snapshots lifecycle capabilities before exposing the frozen facade", async () => {
  const capabilities = {
    atomicPointInTimeCheckpoint: true,
    exclusiveWriterAttachment: true,
    fencing: "epoch-enforced",
    normalDirectoryAttachment: true,
  };
  const base = await fixture();
  const lifecycleBackend = lifecycleWithMethod(
    [],
    "captureCheckpoint",
    async () => exact({ status: "ok" }),
    capabilities,
  );
  const binding = createPostgresDetachedRestorePhysicalBindings({
    ...base.options,
    lifecycleBackend,
  });
  capabilities.fencing = "mutated-after-construction";
  capabilities.normalDirectoryAttachment = false;

  assertFrozenNullRecord(binding.lifecycleBackend.capabilities, [
    "atomicPointInTimeCheckpoint",
    "exclusiveWriterAttachment",
    "fencing",
    "normalDirectoryAttachment",
  ]);
  assert.equal(binding.lifecycleBackend.capabilities.fencing, "epoch-enforced");
  assert.equal(binding.lifecycleBackend.capabilities.normalDirectoryAttachment, true);
  assert.deepEqual(await binding.stop(), exact({ status: "stopped" }));
});

test("a post-registry assembly rejection runs construction cleanup and stays fixed", async () => {
  const base = await fixture();
  const lifecycleBackend = lifecycleWithMethod(
    [],
    "captureCheckpoint",
    new Proxy(async () => exact({ status: "ok" }), {}),
  );
  assert.throws(
    () => createPostgresDetachedRestorePhysicalBindings({
      ...base.options,
      lifecycleBackend,
    }),
    { code: "invalid_postgres_detached_restore_physical_bindings_options" },
  );
});

test("memoizes aggregate stop after all fixed settlements are idle", async () => {
  const { binding } = await fixture();
  const first = binding.stop();
  const second = binding.stop();
  assert.equal(first, second);
  assert.deepEqual(await first, exact({ status: "stopped" }));
});

test("protected aggregate stop reactions preserve finally semantics and adopt foreign protected promises", async () => {
  const fulfilled = await fixture();
  const rejected = await fixture({
    resolveRestoreDestination: () => undefined,
  });
  await assert.rejects(
    rejected.binding.resolveRestoreDestination(
      exact({ candidate: 1, generation: 2, kind: "fresh" }),
    ),
    { code: "postgres_detached_restore_physical_bindings_outcome_uncertain" },
  );

  const foreignSettlement = createPhysicalCollaboratorSettlement({
    deadlineMilliseconds: 1_000,
    onFatal() {},
    settlementGraceMilliseconds: 1_000,
  });
  const foreignProtectedPromise = foreignSettlement.invoke({
    start: () => Promise.resolve("foreign-value"),
  });
  const fulfilledStop = fulfilled.binding.stop();
  const rejectedStop = rejected.binding.stop();
  const callbackFailure = new Error("finally callback rejected");
  const constructorDescriptor = Object.getOwnPropertyDescriptor(
    Promise.prototype,
    "constructor",
  );
  const unhandledCallbackRejections = [];
  const observeUnhandledRejection = (reason, promise) => {
    if (reason === callbackFailure) unhandledCallbackRejections.push(promise);
  };
  let poisonReads = 0;
  let fulfilledResult;
  let preservedReason;
  let overriddenReason;
  let foreignResult;
  process.on("unhandledRejection", observeUnhandledRejection);
  try {
    Object.defineProperty(Promise.prototype, "constructor", {
      configurable: true,
      get() {
        poisonReads += 1;
        throw new Error("Promise prototype constructor must not be read");
      },
    });
    try {
      fulfilledResult = await fulfilledStop.finally(
        () => Promise.resolve("ignored-finally-value"),
      );
      try {
        await rejectedStop.finally(() => Promise.resolve("ignored-finally-value"));
      } catch (error) {
        preservedReason = error;
      }
      try {
        await rejectedStop.finally(() => Promise.reject(callbackFailure));
      } catch (error) {
        overriddenReason = error;
      }
      foreignResult = await fulfilledStop.then(() => foreignProtectedPromise);
    } finally {
      Object.defineProperty(
        Promise.prototype,
        "constructor",
        constructorDescriptor,
      );
    }
    await new Promise((resolve) => setImmediate(resolve));
  } finally {
    process.off("unhandledRejection", observeUnhandledRejection);
  }

  assert.deepEqual(fulfilledResult, exact({ status: "stopped" }));
  assert.equal(
    preservedReason.code,
    "postgres_detached_restore_physical_bindings_outcome_uncertain",
  );
  assert.equal(overriddenReason, callbackFailure);
  assert.equal(foreignResult.value, "foreign-value");
  assert.equal(poisonReads, 0);
  assert.deepEqual(unhandledCallbackRejections, []);
});

test("protected aggregate stop fails closed without reading poisoned Promise species", async () => {
  const { binding } = await fixture();
  const stopped = binding.stop();
  const candidate = Promise.resolve("candidate-value");
  let reactionCalls = 0;
  const untrustedReaction = Object.freeze(function untrustedReaction() {
    reactionCalls += 1;
    throw new Error("untrusted Promise reaction must not run");
  });
  Object.defineProperties(candidate, {
    catch: {
      configurable: false,
      enumerable: false,
      value: untrustedReaction,
      writable: false,
    },
    constructor: {
      configurable: false,
      enumerable: false,
      value: Promise,
      writable: false,
    },
    finally: {
      configurable: false,
      enumerable: false,
      value: untrustedReaction,
      writable: false,
    },
    then: {
      configurable: false,
      enumerable: false,
      value: untrustedReaction,
      writable: false,
    },
  });
  const safeSpeciesHolder = Object.freeze(Object.create(null, {
    [Symbol.species]: {
      configurable: false,
      enumerable: false,
      value: Promise,
      writable: false,
    },
  }));
  const mutableConstructorCandidate = Promise.resolve("mutable-candidate-value");
  Object.defineProperty(mutableConstructorCandidate, "constructor", {
    configurable: true,
    enumerable: false,
    value: safeSpeciesHolder,
    writable: true,
  });
  const speciesDescriptor = Object.getOwnPropertyDescriptor(
    Promise,
    Symbol.species,
  );
  let speciesReads = 0;
  let mutableConstructorReads = 0;
  let thenReason;
  let finallyReason;
  let mutableConstructorReason;
  Object.defineProperty(Promise, Symbol.species, {
    configurable: speciesDescriptor.configurable,
    enumerable: speciesDescriptor.enumerable,
    get() {
      speciesReads += 1;
      throw new Error("Promise species must not be read");
    },
  });
  try {
    try {
      await stopped.then(() => candidate);
    } catch (error) {
      thenReason = error;
    }
    try {
      await stopped.finally(() => candidate);
    } catch (error) {
      finallyReason = error;
    }
    try {
      await stopped.then(() => {
        queueMicrotask(() => {
          Object.defineProperty(mutableConstructorCandidate, "constructor", {
            configurable: true,
            enumerable: false,
            get() {
              mutableConstructorReads += 1;
              throw new Error("replaced Promise constructor must not be read");
            },
          });
        });
        return mutableConstructorCandidate;
      });
    } catch (error) {
      mutableConstructorReason = error;
    }
  } finally {
    Object.defineProperty(Promise, Symbol.species, speciesDescriptor);
  }

  assert.equal(
    thenReason.code,
    "postgres_detached_restore_physical_bindings_outcome_uncertain",
  );
  assert.equal(
    finallyReason.code,
    "postgres_detached_restore_physical_bindings_outcome_uncertain",
  );
  assert.equal(
    mutableConstructorReason.code,
    "postgres_detached_restore_physical_bindings_outcome_uncertain",
  );
  assert.equal(speciesReads, 0);
  assert.equal(mutableConstructorReads, 0);
  assert.equal(reactionCalls, 0);
});

test("aggregate stop is single-flight during synchronous fatal reentry", async () => {
  const base = await fixture();
  const never = new Promise(() => {});
  let binding;
  let reentrantStop = null;
  let fatalCalls = 0;
  let startCalls = 0;
  let abortCalls = 0;
  const supervisor = Object.freeze({
    ...base.options.supervisor,
    launchWriter(input) {
      startCalls += 1;
      input.signal.addEventListener("abort", () => {
        abortCalls += 1;
      });
      return never;
    },
  });
  binding = createPostgresDetachedRestorePhysicalBindings({
    ...base.options,
    onFatal() {
      fatalCalls += 1;
      if (reentrantStop === null) reentrantStop = binding.stop();
    },
    supervisor,
    supervisorSettlement: policyRegistry(SUPERVISOR_METHODS, {
      launchWriter: {
        deadlineMilliseconds: 1,
        settlementGraceMilliseconds: 1,
      },
    }),
  });
  const active = binding.supervisor.launchWriter(
    exact({ attempt: exact({ launchAttemptId: "attempt-active" }), contractVersion: 1 }),
  );
  const activeOutcome = active.catch((error) => error);

  const blockingDeadline = performance.now() + 20;
  while (performance.now() < blockingDeadline) {
    // Keep the deadline timer queued so stop observes the breach synchronously.
  }
  const outerStop = binding.stop();

  assert.notEqual(reentrantStop, null);
  assert.strictEqual(outerStop, reentrantStop);
  assert.strictEqual(binding.stop(), outerStop);
  await assert.rejects(outerStop, {
    code: "postgres_detached_restore_physical_bindings_outcome_uncertain",
  });
  assert.equal(
    (await activeOutcome).code,
    "postgres_detached_restore_physical_bindings_outcome_uncertain",
  );
  assert.equal(startCalls, 1);
  assert.equal(abortCalls, 1);
  assert.equal(fatalCalls, 1);
});

test("aggregate stop retains state collection quiescence while aborting every physical method family", async (t) => {
  const rawPublication = await physicalPublicationFixture(t);
  const never = new Promise(() => {});
  const rawCollection = deferred();
  const observedSignals = [];
  const fatalContexts = [];
  const base = await fixture();
  const supervisor = Object.freeze({
    ...base.options.supervisor,
    launchWriter(input) {
      observedSignals.push(input.signal);
      return never;
    },
  });
  const supervisorStateCollector = Object.freeze({
    ...base.options.supervisorStateCollector,
    collectTerminalState(input) {
      observedSignals.push(input.signal);
      return rawCollection.promise;
    },
  });
  const lifecycleBackend = lifecycleWithMethod(
    [],
    "provisionSession",
    (request, context) => {
      observedSignals.push(context.signal);
      return never;
    },
  );
  const policy = {
    deadlineMilliseconds: 1_000,
    settlementGraceMilliseconds: 5,
  };
  const { binding } = await fixture({
    lifecycleBackend,
    lifecycleSettlement: policyRegistry(LIFECYCLE_METHODS, {
      provisionSession: policy,
    }),
    onFatal(context) {
      fatalContexts.push(context);
    },
    publication: rawPublication.publication,
    publicationSettlement: policyRegistry(PUBLICATION_METHODS, {
      publishFreshCheckpointArtifact: policy,
    }),
    resolveRestoreDestination(input) {
      observedSignals.push(input.signal);
      return never;
    },
    resolveRestoreDestinationSettlement: policy,
    supervisor,
    supervisorSettlement: policyRegistry(SUPERVISOR_METHODS, {
      launchWriter: policy,
    }),
    supervisorStateCollectionSettlement: policy,
    supervisorStateCollector,
  });

  const active = [
    binding.supervisor.launchWriter(
      exact({ attempt: exact({ launchAttemptId: "attempt-active" }), contractVersion: 1 }),
    ),
    binding.lifecycleBackend.provisionSession(exact({ operationId: "op-active" })),
    binding.publication.publishFreshCheckpointArtifact(rawPublication.options),
    binding.resolveRestoreDestination(
      exact({ candidate: 1, generation: 2, kind: "fresh" }),
    ),
    binding.supervisorStateCollector.collectTerminalState(
      exact({ stateOwnerId: STATE_OWNER_ID, terminalRecord: terminalRecord() }),
    ),
  ];
  const activeResults = Promise.allSettled(active);
  assert.equal(observedSignals.length, 4);
  await rawPublication.acquireEntered;
  const stopped = binding.stop();
  const stoppedResult = Promise.allSettled([stopped]);
  assert.equal(observedSignals.every((signal) => signal.aborted), true);
  await new Promise((resolve) => setTimeout(resolve, 20));

  let stopSettled = false;
  void stopped.finally(() => {
    stopSettled = true;
  }).catch(() => {});
  await new Promise((resolve) => setTimeout(resolve, 0));
  assert.equal(stopSettled, false);
  rawCollection.resolve(exact({
    contractVersion:
      POSTGRES_WRITER_SUPERVISOR_STATE_COLLECTION_PHYSICAL_CONTRACT_VERSION,
    launchAttemptId: "attempt-001",
    stateOwnerId: STATE_OWNER_ID,
    status: "collected",
    terminalRecordSha256: "d".repeat(64),
  }));

  const results = [...(await activeResults), ...(await stoppedResult)];
  assert.equal(results.every((result) => result.status === "rejected"), true);
  for (const result of results) {
    assert.equal(
      result.reason.code,
      "postgres_detached_restore_physical_bindings_outcome_uncertain",
    );
  }
  assert.equal(fatalContexts.length, 5);
  assert.equal(
    fatalContexts.every((context) => context.outcome === "no-settlement"),
    true,
  );
});

test("deadline and grace breach rejects one family invocation, signals fatal, and makes aggregate stop fail", async () => {
  let fatalContext;
  const never = new Promise(() => {});
  const { binding } = await fixture({
    onFatal(context) {
      fatalContext = context;
    },
    resolveRestoreDestination: () => never,
    resolveRestoreDestinationSettlement: {
      deadlineMilliseconds: 1,
      settlementGraceMilliseconds: 1,
    },
  });
  await assert.rejects(
    binding.resolveRestoreDestination(exact({ candidate: 1, generation: 2, kind: "fresh" })),
    { code: "postgres_detached_restore_physical_bindings_outcome_uncertain" },
  );
  assert.equal(fatalContext.outcome, "no-settlement");
  await assert.rejects(binding.stop(), {
    code: "postgres_detached_restore_physical_bindings_outcome_uncertain",
  });
});

test("state collection settlement retains quiescence through late and grace-breached outcomes", async () => {
  const base = await fixture();
  let lateFatalCalls = 0;
  const late = await fixture({
    onFatal() {
      lateFatalCalls += 1;
    },
    supervisorStateCollectionSettlement: {
      deadlineMilliseconds: 1,
      settlementGraceMilliseconds: 100,
    },
    supervisorStateCollector: Object.freeze({
      ...base.options.supervisorStateCollector,
      collectTerminalState(input) {
        return new Promise((resolve) => {
          setTimeout(() => resolve(exact({
            contractVersion:
              POSTGRES_WRITER_SUPERVISOR_STATE_COLLECTION_PHYSICAL_CONTRACT_VERSION,
            launchAttemptId: input.terminalRecord.launchAttemptId,
            stateOwnerId: STATE_OWNER_ID,
            status: "collected",
            terminalRecordSha256: "d".repeat(64),
          })), 10);
        });
      },
    }),
  });
  await assert.rejects(
    late.binding.supervisorStateCollector.collectTerminalState(
      exact({ stateOwnerId: STATE_OWNER_ID, terminalRecord: terminalRecord() }),
    ),
    { code: "postgres_detached_restore_physical_bindings_outcome_uncertain" },
  );
  assert.equal(lateFatalCalls, 0);
  assert.deepEqual(await late.binding.stop(), exact({ status: "stopped" }));

  let fatalContext = null;
  let observedSignal = null;
  const raw = deferred();
  const breached = await fixture({
    onFatal(context) {
      fatalContext = context;
    },
    supervisorStateCollectionSettlement: {
      deadlineMilliseconds: 1,
      settlementGraceMilliseconds: 1,
    },
    supervisorStateCollector: Object.freeze({
      ...base.options.supervisorStateCollector,
      collectTerminalState(input) {
        observedSignal = input.signal;
        return raw.promise;
      },
    }),
  });
  let collectionSettled = false;
  const collection = breached.binding.supervisorStateCollector
    .collectTerminalState(exact({
      stateOwnerId: STATE_OWNER_ID,
      terminalRecord: terminalRecord(),
    }))
    .finally(() => {
      collectionSettled = true;
    });
  const collectionOutcome = collection.then(
    () => null,
    (error) => error,
  );
  await new Promise((resolve) => setTimeout(resolve, 20));
  assert.equal(observedSignal.aborted, true);
  assert.equal(fatalContext.outcome, "no-settlement");
  assert.equal(collectionSettled, false);
  let stopSettled = false;
  const stop = breached.binding.stop().finally(() => {
    stopSettled = true;
  });
  const stopOutcome = stop.then(
    () => null,
    (error) => error,
  );
  await new Promise((resolve) => setTimeout(resolve, 0));
  assert.equal(stopSettled, false);

  raw.resolve(exact({
    contractVersion:
      POSTGRES_WRITER_SUPERVISOR_STATE_COLLECTION_PHYSICAL_CONTRACT_VERSION,
    launchAttemptId: "attempt-001",
    stateOwnerId: STATE_OWNER_ID,
    status: "collected",
    terminalRecordSha256: "d".repeat(64),
  }));
  assert.equal(
    (await collectionOutcome).code,
    "postgres_detached_restore_physical_bindings_outcome_uncertain",
  );
  assert.equal(
    (await stopOutcome).code,
    "postgres_detached_restore_physical_bindings_outcome_uncertain",
  );
  assert.equal(collectionSettled, true);
  assert.equal(stopSettled, true);
});

test("supervisor and lifecycle no-settlement breaches use their private policies and fatal hook", async () => {
  const never = new Promise(() => {});
  for (const family of ["supervisor", "lifecycle"]) {
    let fatalCalls = 0;
    const events = [];
    const base = await fixture();
    const overrides = {
      onFatal() { fatalCalls += 1; },
    };
    if (family === "supervisor") {
      overrides.supervisor = Object.freeze({
        ...base.options.supervisor,
        launchWriter: () => never,
      });
      overrides.supervisorSettlement = policyRegistry(SUPERVISOR_METHODS, {
        launchWriter: {
          deadlineMilliseconds: 1,
          settlementGraceMilliseconds: 1,
        },
      });
    } else {
      overrides.lifecycleBackend = lifecycleWithMethod(
        events,
        "provisionSession",
        () => never,
      );
      overrides.lifecycleSettlement = policyRegistry(LIFECYCLE_METHODS, {
        provisionSession: {
          deadlineMilliseconds: 1,
          settlementGraceMilliseconds: 1,
        },
      });
    }
    const current = await fixture(overrides);
    const pending =
      family === "supervisor"
        ? current.binding.supervisor.launchWriter(
            exact({ attempt: exact({ launchAttemptId: "attempt-001" }), contractVersion: 1 }),
          )
        : current.binding.lifecycleBackend.provisionSession(exact({ operationId: "op-001" }));
    await assert.rejects(pending, {
      code: "postgres_detached_restore_physical_bindings_outcome_uncertain",
    });
    assert.equal(fatalCalls, 1);
    await assert.rejects(current.binding.stop(), {
      code: "postgres_detached_restore_physical_bindings_outcome_uncertain",
    });
  }
});

test("a result after its deadline but within grace stays late and does not invoke fatal", async () => {
  let fatalCalls = 0;
  const { binding } = await fixture({
    onFatal() { fatalCalls += 1; },
    resolveRestoreDestination: () =>
      new Promise((resolve) => {
        setTimeout(() => resolve(exact({
          destinationDirectory: "/tmp/destination",
          destinationOwnedRoot: "/tmp",
        })), 10);
      }),
    resolveRestoreDestinationSettlement: {
      deadlineMilliseconds: 1,
      settlementGraceMilliseconds: 100,
    },
  });
  await assert.rejects(
    binding.resolveRestoreDestination(exact({ candidate: 1, generation: 2, kind: "fresh" })),
    { code: "postgres_detached_restore_physical_bindings_outcome_uncertain" },
  );
  assert.equal(fatalCalls, 0);
  assert.deepEqual(await binding.stop(), exact({ status: "stopped" }));
});

test("publication stop no-settlement is bounded by grace and the fatal hook", async (t) => {
  let fatalCalls = 0;
  const raw = await physicalPublicationFixture(t);
  const { binding } = await fixture({
    onFatal() { fatalCalls += 1; },
    publication: raw.publication,
    publicationSettlement: policyRegistry(PUBLICATION_METHODS, {
      publishFreshCheckpointArtifact: {
        deadlineMilliseconds: 1_000,
        settlementGraceMilliseconds: 5,
      },
    }),
  });
  const active = binding.publication.publishFreshCheckpointArtifact(raw.options);
  const activeRejection = assert.rejects(
    active,
    { code: "postgres_detached_restore_physical_bindings_outcome_uncertain" },
  );
  const publicationState = await Promise.race([
    raw.acquireEntered.then(() => "acquire-entered"),
    active.then(
      () => "fulfilled-before-acquire",
      (error) => `rejected-before-acquire:${error.code}`,
    ),
    new Promise((resolve) =>
      setTimeout(() => resolve("timed-out-before-acquire"), 1_000),
    ),
  ]);
  assert.equal(publicationState, "acquire-entered");
  const stopped = binding.stop();
  const stopRejection = assert.rejects(stopped, {
    code: "postgres_detached_restore_physical_bindings_outcome_uncertain",
  });
  await new Promise((resolve) => setTimeout(resolve, 20));
  await activeRejection;
  assert.equal(fatalCalls, 1);
  await stopRejection;
});
