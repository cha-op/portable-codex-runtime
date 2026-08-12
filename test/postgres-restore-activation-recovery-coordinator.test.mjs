import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import {
  lstat,
  mkdir,
  mkdtemp,
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
  snapshotOperationJournalBinding,
} from "../src/filesystem-operation-journal.mjs";
import { PostgresOperationGuard } from "../src/postgres-operation-guard.mjs";
import {
  createPostgresDetachedRestorePhysicalBindings,
  isPostgresDetachedRestorePublicationBinding,
} from "../src/postgres-detached-restore-physical-bindings.mjs";
import {
  PostgresRestoreActivationRecoveryCoordinatorError,
  createPostgresRestoreActivationRecoveryCoordinator,
} from "../src/postgres-restore-activation-recovery-coordinator.mjs";
import {
  RESTORE_ATTACHMENT_ACTIVATION_OPERATION_KIND,
  RESTORE_DESTINATION_GENERATION_DOCUMENT_CONTRACT_VERSION,
  RESTORE_DESTINATION_GENERATION_OPERATION_KIND,
  WRITER_LAUNCH_ATTEMPT_OPERATION_KIND,
  WRITER_LAUNCH_PRE_DISPATCH_CANCELLATION_REASON,
  assertSessionAuthoritySnapshot,
  assertSessionOperationBinding,
  createRestoreAttachmentActivationOperationRequest,
  createRestoreAttachmentActivationOperationRequestV2,
  createRestoreDestinationGenerationOperationRequest,
  createWriterLaunchAttemptOperationRequest,
} from "../src/postgres-session-authority.mjs";
import {
  RESTORE_ATTACHMENT_RECONCILIATION_CONTRACT_VERSION,
  createSessionManifest,
} from "../src/session-storage-contracts.mjs";
import { StoppedDirectoryPublication } from "../src/stopped-directory-publication.mjs";

const SESSION_ID = "019f2100-0000-7000-8000-000000000001";
const THREAD_ID = "019f2100-0000-7000-8000-000000000002";
const IMAGE_DIGEST = `sha256:${"a".repeat(64)}`;
const BACKEND_ID = "single-attach-test";
const STORAGE_ID = "volume-001";
const CHECKPOINT_ID = "checkpoint-001";
const ARTIFACT_ID = "artifact-001";
const CAPTURE_ATTEMPT_ID = "019f2100-0000-7000-8000-000000000003";
const CAPTURE_OPERATION_ID = "capture-operation-001";
const RESTORE_OPERATION_ID = "restore-operation-001";
const GENERATION_ID = "restore-generation-001";
const ACTIVATION_OPERATION_ID = "restore-activation-001";
const ATTACHMENT_ID = `attachment-${createHash("sha256")
  .update(`writer-attachment:${ACTIVATION_OPERATION_ID}`, "utf8")
  .digest("hex")}`;
const ACTIVATION_LEASE_ID = `lease-${createHash("sha256")
  .update(`writer-lease:${ACTIVATION_OPERATION_ID}`, "utf8")
  .digest("hex")}`;
const TEST_OBJECT_IDENTITY_SCHEME = "test-object-generation-v1";
const PHYSICAL_PUBLICATION_METHODS = Object.freeze([
  "publishFreshCheckpointArtifact",
  "publishRestoreDestination",
  "verifyCommittedCheckpointArtifact",
  "verifyCommittedRestoreDestination",
]);
const PHYSICAL_LIFECYCLE_METHODS = Object.freeze([
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
const PHYSICAL_SUPERVISOR_METHODS = Object.freeze([
  "launchWriter",
  "reconcileWriterLaunch",
  "stopWriter",
]);
const ignorePhysicalFatal = Object.freeze(() => undefined);

function serializedSha256(value) {
  return createHash("sha256")
    .update(JSON.stringify(value), "utf8")
    .digest("hex");
}

function requestSha256(expectedSession, request) {
  return serializedSha256({
    requestVersion: 1,
    conflictClass: "session-mutation",
    expectedSession,
    payload: request,
  });
}

function reversedDataObject(value) {
  return Object.fromEntries(Object.entries(value).reverse());
}

function canonicalJsonFixture(value) {
  if (Array.isArray(value)) {
    return value.map((entry) => canonicalJsonFixture(entry));
  }
  if (value === null || typeof value !== "object") return value;
  return Object.fromEntries(
    Object.keys(value)
      .sort()
      .map((key) => [key, canonicalJsonFixture(value[key])]),
  );
}

function reversedJsonFixture(value) {
  if (Array.isArray(value)) {
    return value.map((entry) => reversedJsonFixture(entry));
  }
  if (value === null || typeof value !== "object") return value;
  return Object.fromEntries(
    Object.keys(value)
      .reverse()
      .map((key) => [key, reversedJsonFixture(value[key])]),
  );
}

function synchronizeForgedOperationDigest(receipt) {
  const digest = requestSha256(
    receipt.operation.expectedSession,
    receipt.operation.request,
  );
  receipt.operation.requestSha256 = digest;
  receipt.reservation.requestSha256 = digest;
  receipt.session.document.activeOperation.requestSha256 = digest;
}

function synchronizeCommittedGenerationOperation(receipt) {
  const digest = authorityOperationBinding(
    receipt.operation.kind,
    receipt.operation.operationId,
    receipt.operation.expectedSession,
    receipt.operation.request,
  ).requestSha256;
  receipt.operation.requestSha256 = digest;
  receipt.reservation.requestSha256 = digest;
  receipt.operation.result.generationDocumentSha256 = serializedSha256(
    receipt.generation.document,
  );
  receipt.session.document.lastOperation.requestSha256 = digest;
  receipt.session.document.lastOperation.resultSha256 = serializedSha256(
    receipt.operation.result,
  );
}

function authorityOperationBinding(
  kind,
  operationId,
  expectedSession,
  request,
) {
  return assertSessionOperationBinding({
    expectedSession,
    kind,
    operationId,
    request,
  });
}

function reservationIdForOperation(operationId) {
  return `reservation-${createHash("sha256")
    .update(operationId, "utf8")
    .digest("hex")}`;
}

function safePromiseSpeciesHolder() {
  const holder = Object.create(null);
  Object.defineProperty(holder, Symbol.species, {
    configurable: false,
    enumerable: false,
    value: Promise,
    writable: false,
  });
  return Object.freeze(holder);
}

function withSafePromiseSpecies(promise) {
  Object.defineProperty(promise, "constructor", {
    configurable: false,
    enumerable: false,
    value: safePromiseSpeciesHolder(),
    writable: false,
  });
  return promise;
}

function activeOperationPointer(operationValue, reservationValue) {
  return {
    conflictClass: "session-mutation",
    expectedSessionRevision: operationValue.expectedSession.revision,
    kind: operationValue.kind,
    operationId: operationValue.operationId,
    operationRevision: operationValue.revision,
    requestSha256: operationValue.requestSha256,
    reservationId: reservationValue.reservationId,
    state: operationValue.state,
  };
}

function lastOperationPointer(operationValue, reservationValue) {
  return {
    conflictClass: "session-mutation",
    expectedSessionRevision: operationValue.expectedSession.revision,
    kind: operationValue.kind,
    operationId: operationValue.operationId,
    operationRevision: operationValue.revision,
    requestSha256: operationValue.requestSha256,
    reservationId: reservationValue.reservationId,
    resultSha256: serializedSha256(operationValue.result),
    state: operationValue.state,
  };
}

function writerLaunchPointer(operationValue) {
  const request = operationValue.request;
  const evidence = operationValue.result.evidence;
  return {
    attachmentId: request.attachment.attachmentId,
    attachmentSha256: serializedSha256(request.attachment),
    contractVersion: 1,
    fencingEpoch: request.fencingEpoch,
    generation: request.generation,
    launchAttemptId: operationValue.operationId,
    launchResultSha256: serializedSha256(operationValue.result),
    leaseId: request.lease.leaseId,
    leaseSha256: serializedSha256(request.lease),
    measuredImageSha256: serializedSha256(request.measuredImage),
    processIncarnationId: evidence.processIncarnationId,
    startedAt: operationValue.updatedAt,
    supervisorId: request.supervisor.supervisorId,
    supervisorProofId: evidence.proofId,
    writerIncarnationId: evidence.writerIncarnationId,
  };
}

function synchronizeLaunchRequestBindings(receipt) {
  receipt.launch.attempt.request = structuredClone(
    receipt.launch.operation.request,
  );
  let digest;
  try {
    digest = authorityOperationBinding(
      receipt.launch.operation.kind,
      receipt.launch.operation.operationId,
      receipt.launch.operation.expectedSession,
      receipt.launch.operation.request,
    ).requestSha256;
  } catch {
    digest = requestSha256(
      receipt.launch.operation.expectedSession,
      receipt.launch.operation.request,
    );
  }
  receipt.launch.operation.requestSha256 = digest;
  receipt.launch.reservation.requestSha256 = digest;
  receipt.session.document.activeOperation.requestSha256 = digest;
}

const TRUSTED_JOURNAL_ACL_INSPECTORS = Object.freeze({
  inspectAncestorAcl: async () => false,
  inspectDirectoryAcl: async () => false,
});

const TRUSTED_PUBLICATION_INSPECTORS = Object.freeze({
  inspectOwnedRootAcl: async () => false,
  inspectOwnedRootAncestorAcl: async () => false,
  listMountPoints: async () => ["/"],
});

function simpleLockProvider() {
  return async () => ({
    async assertHeld() {},
    async release() {},
    async renameWhileHeld(source, destination) {
      await rename(source, destination);
    },
  });
}

function armableJournalClass(guard) {
  const rejectIfArmed = (name) => {
    if (!guard.armed) return;
    guard.calls.push(name);
    throw new Error(`unexpected source transition: ${name}`);
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

async function inspectPersistentObjectIdentity(path) {
  const metadata = await lstat(path, { bigint: true });
  return {
    device: metadata.dev.toString(),
    inode: metadata.ino.toString(),
    objectId: `test-object-${metadata.dev}-${metadata.ino}-${metadata.birthtimeNs}`,
  };
}

function mutationRequest(
  operation,
  operationId,
  {
    attachmentId = ATTACHMENT_ID,
    fencingEpoch,
    holderId,
    leaseId,
    storageId = STORAGE_ID,
  },
) {
  return {
    backendId: BACKEND_ID,
    contractVersion: 1,
    fencingEpoch,
    holderId,
    leaseId,
    operation,
    operationId,
    sessionId: SESSION_ID,
    storageId,
    target:
      operation === "attach"
        ? { attachmentId, kind: "attachment" }
        : {
            artifactId: ARTIFACT_ID,
            checkpointId: CHECKPOINT_ID,
            kind: "checkpoint",
          },
  };
}

function checkpoint(storageId = STORAGE_ID) {
  return {
    artifactId: ARTIFACT_ID,
    backendId: BACKEND_ID,
    checkpointClass: "clean",
    checkpointId: CHECKPOINT_ID,
    codexSessionId: THREAD_ID,
    codexThreadId: THREAD_ID,
    contractVersion: 1,
    createdAt: "2026-08-05T00:00:00.000Z",
    imageDigest: IMAGE_DIGEST,
    sessionId: SESSION_ID,
    sourceFencingEpoch: "11",
    storageId,
  };
}

function mutationResult(request) {
  const proofId =
    request.operation === "checkpoint"
      ? `proof-checkpoint-${createHash("sha256")
          .update(
            `checkpoint-capture-proof:${request.operationId}`,
            "utf8",
          )
          .digest("hex")}`
      : request.operation === "restore"
        ? `proof-restore-${createHash("sha256")
            .update(
              `restore-destination-proof:${request.operationId}`,
              "utf8",
            )
            .digest("hex")}`
        : `proof-${request.operation}-001`;
  return {
    ...request,
    proofId,
    status:
      request.operation === "checkpoint"
        ? "checkpoint-created"
        : request.operation === "restore"
          ? "restored"
          : "attached",
  };
}

function publicationResult(request, checkpointValue = checkpoint()) {
  return {
    checkpoint: checkpointValue,
    mutation: canonicalMutationResult(request),
  };
}

function canonicalMutationResult(request) {
  const result = mutationResult(request);
  return {
    backendId: result.backendId,
    contractVersion: result.contractVersion,
    fencingEpoch: result.fencingEpoch,
    holderId: result.holderId,
    leaseId: result.leaseId,
    operation: result.operation,
    operationId: result.operationId,
    proofId: result.proofId,
    sessionId: result.sessionId,
    status: result.status,
    storageId: result.storageId,
    target: result.target,
  };
}

function canonicalMaterialization(materialization, restoreDestination) {
  return {
    artifactManifestDigest: materialization.artifactManifestDigest,
    ...(restoreDestination
      ? {
          coordinatorBindingSha256:
            materialization.coordinatorBindingSha256,
        }
      : {}),
    contractVersion: materialization.contractVersion,
    modeledDigest: materialization.modeledDigest,
    publicationId: materialization.publicationId,
    publicationKind: materialization.publicationKind,
    stagedRoot: {
      filesystemId: materialization.stagedRoot.filesystemId,
      objectIdentityScheme:
        materialization.stagedRoot.objectIdentityScheme,
      objectId: materialization.stagedRoot.objectId,
    },
    treeIdentityDigest: materialization.treeIdentityDigest,
  };
}

function checkpointCatalogueDocument(
  capture,
  captureRequest,
  artifactProof,
  checkpointValue,
) {
  return {
    artifactProof,
    contractVersion: 1,
    materialization: canonicalMaterialization(
      capture.materialization,
      false,
    ),
    result: {
      checkpoint: checkpointValue,
      mutation: canonicalMutationResult(captureRequest),
    },
  };
}

function restoreGenerationBinding(
  catalogueDocument,
  restoreRequest,
  checkpointValue,
) {
  return structuredClone(snapshotOperationJournalBinding({
    attachment: restoreGenerationExpectedSession().document.attachment,
    captureAttemptId: CAPTURE_ATTEMPT_ID,
    captureOperationId: CAPTURE_OPERATION_ID,
    catalogueSha256: serializedSha256(catalogueDocument),
    checkpoint: checkpointValue,
    contractVersion: 1,
    destinationIsolationProofId: "destination-isolation-proof-001",
    destinationState: "detached",
    generationId: GENERATION_ID,
    request: restoreRequest,
    reservationId: reservationIdForOperation(RESTORE_OPERATION_ID),
  }));
}

async function createPublishedRestoreFixture(
  t,
  { sourceStorageId = STORAGE_ID } = {},
) {
  const root = await mkdtemp(
    join(tmpdir(), "restore-activation-coordinator-test-"),
  );
  t.after(() => rm(root, { force: true, recursive: true }));
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
  const sourceDirectory = join(sourceOwnedRoot, "session");
  await mkdir(join(sourceDirectory, "workspace", "nested"), {
    mode: 0o700,
    recursive: true,
  });
  await writeFile(
    join(sourceDirectory, "workspace", "README.md"),
    "portable\n",
    { mode: 0o640 },
  );
  await writeFile(
    join(sourceDirectory, "workspace", "nested", "state.jsonl"),
    '{"type":"turn","state":"completed"}\n',
    { mode: 0o600 },
  );
  await symlink("README.md", join(sourceDirectory, "workspace", "current"));

  const transitionGuard = { armed: false, calls: [] };
  const JournalClass = armableJournalClass(transitionGuard);
  const journal = new JournalClass({
    acquireLock: simpleLockProvider(),
    directory: journalDirectory,
    ...TRUSTED_JOURNAL_ACL_INSPECTORS,
  });
  const publication = new StoppedDirectoryPublication({
    acquireLock: simpleLockProvider(),
    inspectFilesystem: async () => ({
      durability: "local-fsync-rename",
      filesystemId: "test-filesystem-001",
      objectIdentityScheme: TEST_OBJECT_IDENTITY_SCHEME,
      type: "test-local",
    }),
    inspectPersistentObjectIdentity,
    journal,
    ...TRUSTED_PUBLICATION_INSPECTORS,
  });
  const artifactDirectory = join(artifactOwnedRoot, ARTIFACT_ID);
  const destinationDirectory = join(
    destinationOwnedRoot,
    "restored-session",
  );
  const sourceCheckpoint = checkpoint(sourceStorageId);
  const captureRequest = mutationRequest(
    "checkpoint",
    CAPTURE_OPERATION_ID,
    {
      fencingEpoch: "11",
      holderId: "capture-host-001",
      leaseId: "capture-lease-001",
      storageId: sourceStorageId,
    },
  );
  const capture = await publication.publishCheckpointArtifact({
    artifactDirectory,
    artifactOwnedRoot,
    binding: {
      backendId: BACKEND_ID,
      operation: "checkpoint",
      operationId: CAPTURE_OPERATION_ID,
      sessionId: SESSION_ID,
      storageId: sourceStorageId,
    },
    operationId: CAPTURE_OPERATION_ID,
    request: captureRequest,
    result: publicationResult(captureRequest, sourceCheckpoint),
    sourceDirectory,
    sourceOwnedRoot,
  });
  const artifactProof = {
    artifactManifestDigest: capture.materialization.artifactManifestDigest,
    captureOperationId: CAPTURE_OPERATION_ID,
    modeledDigest: capture.materialization.modeledDigest,
  };
  const catalogueDocument = checkpointCatalogueDocument(
    capture,
    captureRequest,
    artifactProof,
    sourceCheckpoint,
  );
  const restoreRequest = mutationRequest("restore", RESTORE_OPERATION_ID, {
    fencingEpoch: "12",
    holderId: "restore-host-001",
    leaseId: "restore-lease-001",
  });
  const binding = restoreGenerationBinding(
    catalogueDocument,
    restoreRequest,
    sourceCheckpoint,
  );
  const restoreResult = publicationResult(
    restoreRequest,
    sourceCheckpoint,
  );
  const restore = await publication.publishRestoreDestination({
    artifactDirectory,
    artifactOwnedRoot,
    artifactProof,
    binding,
    destinationDirectory,
    destinationOwnedRoot,
    operationId: RESTORE_OPERATION_ID,
    request: restoreRequest,
    result: restoreResult,
  });
  return {
    artifactOwnedRoot,
    artifactProof,
    binding,
    catalogueDocument,
    checkpoint: sourceCheckpoint,
    destinationDirectory,
    destinationOwnedRoot,
    journal,
    publication,
    restore,
    restoreRequest,
    restoreResult: restore.result,
    root,
    sourceOwnedRoot,
    transitionGuard,
  };
}

async function makeSourceUnavailable(fixture) {
  await rm(fixture.sourceOwnedRoot, { force: true, recursive: true });
  await rm(fixture.artifactOwnedRoot, { force: true, recursive: true });
  fixture.transitionGuard.armed = true;
}

class GuardClient {
  constructor(pid) {
    this.held = false;
    this.pid = pid;
  }

  query(query) {
    const callback = query?.callback;
    const text = query?.text;
    if (text === "DISCARD ALL") {
      this.held = false;
      callback(null, { command: "DISCARD", rows: [] });
      return undefined;
    }
    if (text.includes("pg_try_advisory_lock")) {
      this.held = true;
      callback(null, {
        command: "SELECT",
        rows: [{ acquired: true, backend_pid: this.pid }],
      });
      return undefined;
    }
    if (text.includes("FROM pg_catalog.pg_locks")) {
      callback(null, {
        command: "SELECT",
        rows: [{ backend_pid: this.pid, lock_held: this.held }],
      });
      return undefined;
    }
    if (text.includes("pg_advisory_unlock")) {
      const unlocked = this.held;
      this.held = false;
      callback(null, {
        command: "SELECT",
        rows: [{ backend_pid: this.pid, unlocked }],
      });
      return undefined;
    }
    callback(new Error(`unexpected guard query: ${text}`));
    return undefined;
  }

  release() {
    this.held = false;
    return undefined;
  }
}

class GuardPool {
  constructor() {
    this.connectCalls = 0;
  }

  connect(callback) {
    this.connectCalls += 1;
    callback(null, new GuardClient(1000 + this.connectCalls));
    return undefined;
  }
}

class SharedExclusiveGuardClient {
  constructor(pool, pid) {
    this.held = false;
    this.pid = pid;
    this.pool = pool;
  }

  query(query) {
    const callback = query?.callback;
    const text = query?.text;
    if (text === "DISCARD ALL") {
      callback(null, { command: "DISCARD", rows: [] });
      return undefined;
    }
    if (text.includes("pg_try_advisory_lock")) {
      const acquired = this.pool.holder === null;
      if (acquired) {
        this.pool.holder = this;
        this.held = true;
      }
      callback(null, {
        command: "SELECT",
        rows: [{ acquired, backend_pid: this.pid }],
      });
      return undefined;
    }
    if (text.includes("FROM pg_catalog.pg_locks")) {
      callback(null, {
        command: "SELECT",
        rows: [{
          backend_pid: this.pid,
          lock_held: this.held && this.pool.holder === this,
        }],
      });
      return undefined;
    }
    if (text.includes("pg_advisory_unlock")) {
      const unlocked = this.held && this.pool.holder === this;
      if (unlocked) this.pool.holder = null;
      this.held = false;
      callback(null, {
        command: "SELECT",
        rows: [{ backend_pid: this.pid, unlocked }],
      });
      return undefined;
    }
    callback(new Error(`unexpected shared guard query: ${text}`));
    return undefined;
  }

  release() {
    if (this.pool.holder === this) this.pool.holder = null;
    this.held = false;
    return undefined;
  }
}

class SharedExclusiveGuardPool {
  constructor() {
    this.connectCalls = 0;
    this.holder = null;
  }

  connect(callback) {
    this.connectCalls += 1;
    callback(
      null,
      new SharedExclusiveGuardClient(
        this,
        2000 + this.connectCalls,
      ),
    );
    return undefined;
  }

  isHeld(holder) {
    return holder !== null && this.holder === holder && holder.held;
  }
}

function sessionManifest() {
  return createSessionManifest({
    codex: {
      ephemeral: false,
      historyMode: "paginated",
      rootThreadId: THREAD_ID,
      sessionId: THREAD_ID,
    },
    runtime: {
      codexSandbox: "danger-full-access",
      codexVersion: "codex-cli 0.142.4",
      imageDigest: IMAGE_DIGEST,
      imageMediaType: "application/vnd.oci.image.manifest.v1+json",
      platform: "linux/arm64",
    },
    sessionId: SESSION_ID,
  });
}

function activationRequest(fixture) {
  const expectedSession = authoritySnapshot();
  const lease = {
    contractVersion: 1,
    sessionId: SESSION_ID,
    leaseId: ACTIVATION_LEASE_ID,
    holderId: "activation-host-001",
    fencingEpoch: "13",
    expiresAt: "2026-08-05T00:13:00.000Z",
  };
  return {
    contractVersion: 1,
    lease,
    manifest: structuredClone(expectedSession.document.manifest),
    mutationRequest: mutationRequest("attach", ACTIVATION_OPERATION_ID, {
      attachmentId: ATTACHMENT_ID,
      fencingEpoch: lease.fencingEpoch,
      holderId: lease.holderId,
      leaseId: lease.leaseId,
    }),
    publication: {
      artifactManifestDigest:
        fixture.restore.materialization.artifactManifestDigest,
      coordinatorBindingSha256:
        fixture.restore.materialization.coordinatorBindingSha256,
      modeledDigest: fixture.restore.materialization.modeledDigest,
      publicationId: fixture.restore.materialization.publicationId,
      publicationKind: "restore-destination",
      root: {
        ...fixture.restore.materialization.stagedRoot,
        rootPath: fixture.destinationDirectory,
      },
      treeIdentityDigest:
        fixture.restore.materialization.treeIdentityDigest,
    },
    storageRef: structuredClone(expectedSession.document.storageRef),
  };
}

function activationResult(request) {
  const mutation = mutationResult(request.mutationRequest);
  return {
    attachment: {
      contractVersion: 1,
      backendId: BACKEND_ID,
      storageId: STORAGE_ID,
      sessionId: SESSION_ID,
      attachmentId: ATTACHMENT_ID,
      leaseId: request.lease.leaseId,
      holderId: request.lease.holderId,
      fencingEpoch: request.lease.fencingEpoch,
      operationId: request.mutationRequest.operationId,
      proofId: mutation.proofId,
      kind: "directory",
      rootPath: request.publication.root.rootPath,
      mode: "read-write",
    },
    contractVersion: 1,
    mutationResult: mutation,
    publication: structuredClone(request.publication),
  };
}

function activationReconciliation(outcome, result) {
  return {
    contractVersion: RESTORE_ATTACHMENT_RECONCILIATION_CONTRACT_VERSION,
    outcome,
    ...(outcome === "applied" ? { result } : {}),
  };
}

function committedGeneration(fixture) {
  return {
    binding: fixture.binding,
    checkpointId: CHECKPOINT_ID,
    claimedAt: "2026-08-05T00:03:00.000Z",
    committedAt: "2026-08-05T00:04:00.000Z",
    document: {
      artifactProof: fixture.artifactProof,
      contractVersion:
        RESTORE_DESTINATION_GENERATION_DOCUMENT_CONTRACT_VERSION,
      materialization: canonicalMaterialization(
        fixture.restore.materialization,
        true,
      ),
      result: fixture.restoreResult,
    },
    generationId: GENERATION_ID,
    operationId: RESTORE_OPERATION_ID,
    sessionId: SESSION_ID,
    state: "committed",
  };
}

function authorityDocument() {
  return {
    documentVersion: 3,
    manifest: sessionManifest(),
    storageRef: {
      backendId: BACKEND_ID,
      contractVersion: 1,
      sessionId: SESSION_ID,
      storageId: STORAGE_ID,
    },
    backendCapabilities: {
      atomicPointInTimeCheckpoint: true,
      exclusiveWriterAttachment: true,
      fencing: "epoch-enforced",
      normalDirectoryAttachment: true,
    },
    lifecycle: "DETACHED",
    writerEpoch: "12",
    lease: null,
    attachment: null,
    activeOperation: null,
    lastOperation: {
      conflictClass: "session-mutation",
      expectedSessionRevision: "17",
      kind: "writer-release-v1",
      operationId: "detach-operation-001",
      operationRevision: "2",
      requestSha256: "8".repeat(64),
      reservationId: reservationIdForOperation("detach-operation-001"),
      resultSha256: "9".repeat(64),
      state: "committed",
    },
    recovery: null,
    launch: null,
  };
}

function authoritySnapshot(revision = "20") {
  const snapshot = {
    sessionId: SESSION_ID,
    revision: "20",
    document: authorityDocument(),
    createdAt: "2026-08-05T00:00:00.000Z",
    updatedAt: "2026-08-05T00:03:00.000Z",
  };
  const canonical = structuredClone(
    authorityOperationBinding(
      "test-operation-v1",
      "canonical-snapshot-operation-001",
      snapshot,
      {},
    ).expectedSession,
  );
  canonical.revision = revision;
  return canonical;
}

function restoreGenerationExpectedSession(revision = "20") {
  const snapshot = authoritySnapshot(revision);
  snapshot.document.lifecycle = "ATTACHED";
  snapshot.document.writerEpoch = "12";
  snapshot.document.lease = {
    contractVersion: 1,
    sessionId: SESSION_ID,
    leaseId: "restore-lease-001",
    holderId: "restore-host-001",
    fencingEpoch: "12",
    expiresAt: "2026-08-05T00:13:00.000Z",
  };
  snapshot.document.attachment = {
    contractVersion: 1,
    backendId: BACKEND_ID,
    storageId: STORAGE_ID,
    sessionId: SESSION_ID,
    attachmentId: "restore-source-attachment-001",
    leaseId: "restore-lease-001",
    holderId: "restore-host-001",
    fencingEpoch: "12",
    operationId: "restore-source-attach-operation-001",
    proofId: "restore-source-attach-proof-001",
    kind: "directory",
    rootPath: "/restore/source",
    mode: "read-write",
  };
  snapshot.document.lastOperation = {
    conflictClass: "session-mutation",
    expectedSessionRevision: "17",
    kind: "writer-attachment-acquire-v1",
    operationId: "restore-source-attach-operation-001",
    operationRevision: "2",
    requestSha256: "a".repeat(64),
    reservationId: reservationIdForOperation(
      "restore-source-attach-operation-001",
    ),
    resultSha256: "b".repeat(64),
    state: "committed",
  };
  return structuredClone(
    authorityOperationBinding(
      "test-operation-v1",
      "canonical-generation-snapshot-operation-001",
      snapshot,
      {},
    ).expectedSession,
  );
}

function operation(
  state,
  kind,
  operationId,
  request,
  result = null,
  expectedSessionValue = authoritySnapshot(),
) {
  const committed = state === "committed";
  const expectedSession = expectedSessionValue;
  const binding = authorityOperationBinding(
    kind,
    operationId,
    expectedSession,
    request,
  );
  return {
    conflictClass: "session-mutation",
    createdAt: "2026-08-05T00:03:00.000Z",
    expectedSession,
    kind,
    operationId,
    request,
    requestSha256: binding.requestSha256,
    result,
    retiredAt: committed ? "2026-08-05T00:04:00.000Z" : null,
    revision: state === "starting" ? "1" : state === "uncertain" ? "2" : committed ? "3" : "0",
    sessionId: SESSION_ID,
    state,
    updatedAt: committed
      ? "2026-08-05T00:04:00.000Z"
      : "2026-08-05T00:03:00.000Z",
  };
}

function reservationFor(operationValue) {
  const committed = operationValue.state === "committed";
  return {
    conflictClass: "session-mutation",
    createdAt: operationValue.createdAt,
    expectedSessionRevision: operationValue.expectedSession.revision,
    expiresAt: null,
    kind: operationValue.kind,
    operationId: operationValue.operationId,
    releasedAt: committed ? operationValue.updatedAt : null,
    requestSha256: operationValue.requestSha256,
    reservationId: reservationIdForOperation(operationValue.operationId),
    sessionId: SESSION_ID,
    state: committed ? "released" : operationValue.state,
    updatedAt: operationValue.updatedAt,
  };
}

function catalogue(fixture) {
  return {
    captureAttemptId: CAPTURE_ATTEMPT_ID,
    checkpointId: CHECKPOINT_ID,
    committedAt: "2026-08-05T00:01:00.000Z",
    document: fixture.catalogueDocument,
    sessionId: SESSION_ID,
  };
}

function generationRead(fixture, state, finalized = false) {
  const expectedSession = restoreGenerationExpectedSession();
  const operationRequest = structuredClone(
    createRestoreDestinationGenerationOperationRequest({
      admission: {
        checkpoint: fixture.checkpoint,
        request: fixture.restoreRequest,
      },
      expectedSession,
    }),
  );
  const operationValue = operation(
    state,
    RESTORE_DESTINATION_GENERATION_OPERATION_KIND,
    RESTORE_OPERATION_ID,
    operationRequest,
    null,
    expectedSession,
  );
  const reservationValue = reservationFor(operationValue);
  const generationValue =
    state === "committed"
      ? committedGeneration(fixture)
      : {
          binding: fixture.binding,
          checkpointId: CHECKPOINT_ID,
          claimedAt: operationValue.createdAt,
          committedAt: null,
          document: null,
          generationId: GENERATION_ID,
          operationId: RESTORE_OPERATION_ID,
          sessionId: SESSION_ID,
          state: "authorized",
        };
  if (state === "committed") {
    operationValue.result = {
      catalogueSha256: serializedSha256(fixture.catalogueDocument),
      checkpointId: CHECKPOINT_ID,
      generationDocumentSha256: serializedSha256(
        generationValue.document,
      ),
      generationId: GENERATION_ID,
      outcome: "restore-generation-committed",
      resultVersion: 1,
    };
  }
  const session = structuredClone(assertSessionAuthoritySnapshot({
    sessionId: SESSION_ID,
    revision: (
      BigInt(operationValue.expectedSession.revision) +
      BigInt(operationValue.revision) +
      1n
    ).toString(),
    document: {
      ...operationValue.expectedSession.document,
      activeOperation:
        state === "committed"
          ? null
          : activeOperationPointer(operationValue, reservationValue),
      lastOperation:
        state === "committed"
          ? lastOperationPointer(operationValue, reservationValue)
          : operationValue.expectedSession.document.lastOperation,
    },
    createdAt: operationValue.expectedSession.createdAt,
    updatedAt: operationValue.updatedAt,
  }));
  const receipt = {
    catalogue: catalogue(fixture),
    generation: generationValue,
    operation: operationValue,
    reservation: reservationValue,
    session,
    status: state === "committed" ? "committed" : "authorized",
  };
  return finalized ? { ...receipt, finalized: true } : receipt;
}

function directCommittedGenerationRead(fixture, finalized = false) {
  const receipt = generationRead(fixture, "committed", finalized);
  receipt.operation.revision = "2";
  receipt.session.revision = (
    BigInt(receipt.operation.expectedSession.revision) + 3n
  ).toString();
  receipt.session.document.lastOperation.operationRevision = "2";
  return receipt;
}

function activationMeasuredImage() {
  return {
    projection: {
      codexSandbox: "danger-full-access",
      codexVersion: "codex-cli 0.142.4",
      platformImage: {
        architecture: "arm64",
        config: {
          digest: `sha256:${"d".repeat(64)}`,
          mediaType: "application/vnd.oci.image.config.v1+json",
          size: 1_024,
        },
        digest: IMAGE_DIGEST,
        mediaType: "application/vnd.oci.image.manifest.v1+json",
        os: "linux",
        size: 2_048,
      },
    },
    runtimeIdentity: {
      codexBinaryPath: "/usr/local/bin/codex",
      codexBinarySha256: "e".repeat(64),
      codexVersion: "codex-cli 0.142.4",
      platformImageDigest: IMAGE_DIGEST,
    },
  };
}

function activationOperationRequest(fixture, contractVersion = 1) {
  const createRequest =
    contractVersion === 1
      ? createRestoreAttachmentActivationOperationRequest
      : createRestoreAttachmentActivationOperationRequestV2;
  assert(contractVersion === 1 || contractVersion === 2);
  return createRequest({
    destinationRootPath: fixture.destinationDirectory,
    expectedSession: authoritySnapshot(),
    generation: committedGeneration(fixture),
    holderId: "activation-host-001",
    launchIntent: {
      launchAttemptId: "launch-attempt-001",
      measuredImage: activationMeasuredImage(),
      supervisor: {
        contractVersion: 1,
        supervisorId: "supervisor-001",
      },
    },
    leaseDurationMilliseconds: 600_000,
    predecessor: {
      attachmentId: "old-attachment-001",
      ...(contractVersion === 2
        ? { captureOperationId: CAPTURE_OPERATION_ID }
        : {}),
      detachOperationId: "detach-operation-001",
      stopOperationId: "stop-operation-001",
    },
  });
}

function activationRead(
  fixture,
  state,
  request,
  resultOverride = null,
  operationRevision = null,
  requestContractVersion = 1,
) {
  const operationRequest = activationOperationRequest(
    fixture,
    requestContractVersion,
  );
  const providerResult = resultOverride ?? activationResult(request);
  const storedActivationRequest = canonicalJsonFixture(request);
  const storedProviderResult = canonicalJsonFixture(providerResult);
  const operationValue = operation(
    state,
    RESTORE_ATTACHMENT_ACTIVATION_OPERATION_KIND,
    ACTIVATION_OPERATION_ID,
    operationRequest,
    state === "committed"
      ? {
          activationRequest: storedActivationRequest,
          activationResult: storedProviderResult,
          outcome: "restore-attachment-activated",
          resultVersion: 1,
        }
      : null,
  );
  if (operationRevision !== null) {
    operationValue.revision = operationRevision;
  }
  const reservation = reservationFor(operationValue);
  const terminalRevision = (
    BigInt(operationValue.expectedSession.revision) +
    BigInt(operationValue.revision) +
    1n
  ).toString();
  const session = structuredClone(assertSessionAuthoritySnapshot(
    state === "committed"
      ? {
          sessionId: SESSION_ID,
          revision: (BigInt(terminalRevision) + 3n).toString(),
          document: {
            ...operationValue.expectedSession.document,
            activeOperation: null,
            attachment: providerResult.attachment,
            lastOperation: {
              conflictClass: "session-mutation",
              expectedSessionRevision: terminalRevision,
              kind: "writer-launch-attempt-v1",
              operationId: "later-activation-launch-attempt-001",
              operationRevision: "2",
              requestSha256: "2".repeat(64),
              reservationId: reservationIdForOperation(
                "later-activation-launch-attempt-001",
              ),
              resultSha256: "3".repeat(64),
              state: "committed",
            },
            launch: null,
            lease: request.lease,
            lifecycle: "ATTACHED",
            writerEpoch: request.lease.fencingEpoch,
          },
          createdAt: operationValue.expectedSession.createdAt,
          updatedAt: "2026-08-05T00:06:00.000Z",
        }
      : {
          sessionId: SESSION_ID,
          revision: terminalRevision,
          document: {
            ...operationValue.expectedSession.document,
            activeOperation: activeOperationPointer(
              operationValue,
              reservation,
            ),
            attachment: null,
            launch: null,
            lease: request.lease,
            lifecycle: "ATTACHING",
            writerEpoch: request.lease.fencingEpoch,
          },
          createdAt: operationValue.expectedSession.createdAt,
          updatedAt: operationValue.updatedAt,
        },
  ));
  return {
    activationRequest: storedActivationRequest,
    generation: committedGeneration(fixture),
    operation: operationValue,
    reservation,
    session,
    status: state,
  };
}

function activationHandoff(
  fixture,
  request,
  operationRevision = "3",
  requestContractVersion = 1,
) {
  const activation = activationRead(
    fixture,
    "committed",
    request,
    null,
    operationRevision,
    requestContractVersion,
  );
  const prepared = activationResult(request);
  const terminalSession = structuredClone(assertSessionAuthoritySnapshot({
    sessionId: SESSION_ID,
    revision: (
      BigInt(activation.operation.expectedSession.revision) +
      BigInt(activation.operation.revision) +
      1n
    ).toString(),
    document: {
      ...activation.operation.expectedSession.document,
      activeOperation: null,
      attachment: prepared.attachment,
      lastOperation: lastOperationPointer(
        activation.operation,
        activation.reservation,
      ),
      launch: null,
      lease: request.lease,
      lifecycle: "ATTACHED",
      writerEpoch: request.lease.fencingEpoch,
    },
    createdAt: activation.operation.expectedSession.createdAt,
    updatedAt: activation.operation.updatedAt,
  }));
  const launchIntent = activationOperationRequest(
    fixture,
    requestContractVersion,
  ).launchIntent;
  const launchRequest = createWriterLaunchAttemptOperationRequest({
    expectedSession: terminalSession,
    generation: committedGeneration(fixture),
    measuredImage: launchIntent.measuredImage,
    supervisor: launchIntent.supervisor,
  });
  const launchOperation = operation(
    "prepared",
    WRITER_LAUNCH_ATTEMPT_OPERATION_KIND,
    launchIntent.launchAttemptId,
    launchRequest,
  );
  launchOperation.createdAt = activation.operation.updatedAt;
  launchOperation.expectedSession = terminalSession;
  launchOperation.requestSha256 = authorityOperationBinding(
    launchOperation.kind,
    launchOperation.operationId,
    terminalSession,
    launchRequest,
  ).requestSha256;
  launchOperation.updatedAt = activation.operation.updatedAt;
  const launchReservation = reservationFor(launchOperation);
  launchReservation.reservationId = reservationIdForOperation(
    launchOperation.operationId,
  );
  const receipt = {
    activation: {
      finalized: true,
      operation: activation.operation,
      reservation: activation.reservation,
    },
    generation: committedGeneration(fixture),
    launch: {
      attempt: {
        contractVersion: 1,
        launchAttemptId: launchOperation.operationId,
        request: launchRequest,
        result: null,
        state: "prepared",
      },
      operation: launchOperation,
      reservation: launchReservation,
    },
    session: {
      ...terminalSession,
      document: {
        ...terminalSession.document,
        activeOperation: activeOperationPointer(
          launchOperation,
          launchReservation,
        ),
      },
      revision: (BigInt(terminalSession.revision) + 1n).toString(),
      updatedAt: launchOperation.updatedAt,
    },
    status: "prepared",
  };
  receipt.session = structuredClone(
    assertSessionAuthoritySnapshot(receipt.session),
  );
  return receipt;
}

function committedLaunchHandoff(
  fixture,
  request,
  { operationRevision = "2", status = "started" } = {},
) {
  const receipt = structuredClone(activationHandoff(fixture, request));
  const operationValue = receipt.launch.operation;
  const reservationValue = receipt.launch.reservation;
  const processIncarnationId =
    status === "not-started" ? null : "process-incarnation-001";
  const writerIncarnationId =
    status === "not-started" ? null : "writer-incarnation-001";
  const outcome =
    status === "started"
      ? "writer-launch-started"
      : status === "not-started"
        ? "writer-launch-not-started"
        : "writer-launch-complete-stopped";
  const evidence = {
    contractVersion: 1,
    launchAttemptId: operationValue.operationId,
    processIncarnationId,
    proofId: `launch-proof-${status}-001`,
    status,
    supervisorId: operationValue.request.supervisor.supervisorId,
    writerIncarnationId,
  };
  operationValue.result = {
    evidence,
    outcome,
    resultVersion: 1,
  };
  operationValue.retiredAt = "2026-08-05T00:05:00.000Z";
  operationValue.revision = operationRevision;
  operationValue.state = "committed";
  operationValue.updatedAt = operationValue.retiredAt;
  reservationValue.releasedAt = operationValue.updatedAt;
  reservationValue.state = "released";
  reservationValue.updatedAt = operationValue.updatedAt;
  receipt.activation.finalized = false;
  receipt.launch.attempt.result = structuredClone(operationValue.result);
  receipt.launch.attempt.state = "committed";
  receipt.session = {
    sessionId: SESSION_ID,
    revision: (
      BigInt(operationValue.expectedSession.revision) +
      BigInt(operationValue.revision) +
      1n
    ).toString(),
    document: {
      ...operationValue.expectedSession.document,
      activeOperation: null,
      lastOperation: lastOperationPointer(
        operationValue,
        reservationValue,
      ),
      launch:
        status === "started"
          ? writerLaunchPointer(operationValue)
          : operationValue.expectedSession.document.launch,
    },
    createdAt: operationValue.expectedSession.createdAt,
    updatedAt: operationValue.updatedAt,
  };
  receipt.status = "committed";
  receipt.session = structuredClone(
    assertSessionAuthoritySnapshot(receipt.session),
  );
  return receipt;
}

function synchronizeCommittedLaunchReceipt(receipt) {
  const operationValue = receipt.launch.operation;
  const reservationValue = receipt.launch.reservation;
  receipt.launch.attempt.result = structuredClone(operationValue.result);
  receipt.launch.attempt.state = "committed";
  receipt.session.revision = (
    BigInt(operationValue.expectedSession.revision) +
    BigInt(operationValue.revision) +
    1n
  ).toString();
  receipt.session.document.activeOperation = null;
  receipt.session.document.lastOperation = lastOperationPointer(
    operationValue,
    reservationValue,
  );
  receipt.session.updatedAt = operationValue.updatedAt;
  receipt.status = "committed";
  receipt.session = structuredClone(
    assertSessionAuthoritySnapshot(receipt.session),
  );
}

function cancelledLaunchHandoff(fixture, request) {
  const receipt = structuredClone(activationHandoff(fixture, request));
  const operationValue = receipt.launch.operation;
  const reservationValue = receipt.launch.reservation;
  operationValue.result = {
    resultVersion: 1,
    outcome: "cancelled-before-dispatch",
    reason: WRITER_LAUNCH_PRE_DISPATCH_CANCELLATION_REASON,
  };
  operationValue.retiredAt = operationValue.request.lease.expiresAt;
  operationValue.revision = "1";
  operationValue.state = "committed";
  operationValue.updatedAt = operationValue.retiredAt;
  reservationValue.releasedAt = operationValue.updatedAt;
  reservationValue.state = "released";
  reservationValue.updatedAt = operationValue.updatedAt;
  receipt.activation.finalized = false;
  receipt.session.document.launch = null;
  synchronizeCommittedLaunchReceipt(receipt);
  return receipt;
}

function activeLaunchHandoff(
  fixture,
  request,
  state,
  updatedAt = "2026-08-05T00:05:00.000Z",
) {
  const receipt = structuredClone(activationHandoff(fixture, request));
  const operationValue = receipt.launch.operation;
  const reservationValue = receipt.launch.reservation;
  operationValue.revision = state === "starting" ? "1" : "2";
  operationValue.state = state;
  operationValue.updatedAt = updatedAt;
  reservationValue.state = state;
  reservationValue.updatedAt = updatedAt;
  receipt.activation.finalized = false;
  receipt.launch.attempt.state = state;
  receipt.session.revision = (
    BigInt(operationValue.expectedSession.revision) +
    BigInt(operationValue.revision) +
    1n
  ).toString();
  receipt.session.document.activeOperation = activeOperationPointer(
    operationValue,
    reservationValue,
  );
  receipt.session.updatedAt = updatedAt;
  receipt.status = state;
  receipt.session = structuredClone(
    assertSessionAuthoritySnapshot(receipt.session),
  );
  return receipt;
}

function advanceCommittedLaunchCurrentSession(
  receipt,
  { detached = false } = {},
) {
  const terminalRevision = receipt.session.revision;
  const laterOperationId = detached
    ? "later-writer-release-001"
    : "later-writer-launch-stop-001";
  const updatedAt = new Date(
    Date.parse(receipt.launch.operation.updatedAt) + 60_000,
  ).toISOString();
  receipt.session.revision = (BigInt(terminalRevision) + 3n).toString();
  receipt.session.document.lifecycle = detached ? "DETACHED" : "ATTACHED";
  receipt.session.document.lease = detached
    ? null
    : receipt.launch.operation.request.lease;
  receipt.session.document.attachment = detached
    ? null
    : receipt.launch.operation.request.attachment;
  receipt.session.document.activeOperation = null;
  receipt.session.document.lastOperation = {
    conflictClass: "session-mutation",
    expectedSessionRevision: terminalRevision,
    kind: detached ? "writer-release-v1" : "writer-launch-stop-v1",
    operationId: laterOperationId,
    operationRevision: "2",
    requestSha256: "6".repeat(64),
    reservationId: reservationIdForOperation(laterOperationId),
    resultSha256: "7".repeat(64),
    state: "committed",
  };
  receipt.session.document.launch = null;
  receipt.session.updatedAt = updatedAt;
  receipt.session = structuredClone(
    assertSessionAuthoritySnapshot(receipt.session),
  );
  return receipt;
}

function generationCandidate(fixture) {
  return {
    checkpoint: fixture.checkpoint,
    generationId: GENERATION_ID,
    request: fixture.restoreRequest,
  };
}

function advanceCommittedGenerationCurrentSession(receipt) {
  const terminalRevision = receipt.session.revision;
  receipt.session.revision = (BigInt(terminalRevision) + 3n).toString();
  receipt.session.document.activeOperation = null;
  receipt.session.document.lastOperation = {
    conflictClass: "session-mutation",
    expectedSessionRevision: terminalRevision,
    kind: "writer-launch-attempt-v1",
    operationId: "later-generation-launch-attempt-001",
    operationRevision: "2",
    requestSha256: "4".repeat(64),
    reservationId: reservationIdForOperation(
      "later-generation-launch-attempt-001",
    ),
    resultSha256: "5".repeat(64),
    state: "committed",
  };
  receipt.session.updatedAt = "2026-08-05T00:06:00.000Z";
  return receipt;
}

function activationCandidate(
  fixture,
  state = "uncertain",
  requestContractVersion = 1,
) {
  return {
    activationOperationId: ACTIVATION_OPERATION_ID,
    request: activationOperationRequest(fixture, requestContractVersion),
    state,
  };
}

function activationClaimBase(fixture, requestContractVersion = 1) {
  return {
    expectedSession: authoritySnapshot(),
    kind: RESTORE_ATTACHMENT_ACTIVATION_OPERATION_KIND,
    operationId: ACTIVATION_OPERATION_ID,
    request: activationOperationRequest(fixture, requestContractVersion),
  };
}

function activationClaimReceipt(
  fixture,
  request,
  { dispatchGranted, state = "starting" },
) {
  const read = activationRead(fixture, state, request);
  return {
    ...read,
    ...(dispatchGranted
      ? { authorityNow: read.operation.updatedAt }
      : {}),
    dispatchGranted,
  };
}

function authorityHarness(handlers = {}) {
  const calls = [];
  const invoke = async (name, input) => {
    calls.push([name, input]);
    const handler = handlers[name];
    if (typeof handler !== "function") {
      throw new Error(`unexpected authority call: ${name}`);
    }
    return handler(input);
  };
  return {
    calls,
    authority: {
      async claimRestoreAttachmentActivationDispatch(input) {
        return invoke("claim-activation", input);
      },
      async finalizeRestoreAttachmentActivationAndReserveWriterLaunchAttempt(
        input,
      ) {
        return invoke("finalize-activation", input);
      },
      async finalizeRestoreDestinationGeneration(input) {
        return invoke("finalize-generation", input);
      },
      async markOperationUncertain(input) {
        return invoke("mark-uncertain", input);
      },
      async readRestoreAttachmentActivation(input) {
        return invoke("read-activation", input);
      },
      async readRestoreDestinationGeneration(input) {
        return invoke("read-generation", input);
      },
    },
  };
}

function storageBackendHarness(
  prepare,
  { rawReconciliation = false, reconcile } = {},
) {
  const forbiddenCalls = [];
  const prepareCalls = [];
  const providerCalls = [];
  const reconciliationCalls = [];
  const forbidden = (name) => async () => {
    forbiddenCalls.push(name);
    throw new Error(`forbidden backend operation: ${name}`);
  };
  const reconcileProvider =
    reconcile ??
    (rawReconciliation
      ? prepare
      : async (request) => ({
          ...activationReconciliation("applied", await prepare(request)),
        }));
  return {
    backend: {
      backendId: BACKEND_ID,
      capabilities: {
        atomicPointInTimeCheckpoint: true,
        exclusiveWriterAttachment: true,
        fencing: "epoch-enforced",
        normalDirectoryAttachment: true,
      },
      captureCheckpoint: forbidden("captureCheckpoint"),
      contractVersion: 1,
      destroySession: forbidden("destroySession"),
      detachAttachment: forbidden("detachAttachment"),
      forceFence: forbidden("forceFence"),
      prepareRestoreAttachment(request) {
        providerCalls.push(request);
        prepareCalls.push(request);
        return prepare(request);
      },
      prepareWritableAttachment: forbidden("prepareWritableAttachment"),
      provisionSession: forbidden("provisionSession"),
      reconcileRestoreAttachment(request) {
        providerCalls.push(request);
        reconciliationCalls.push(request);
        return reconcileProvider(request);
      },
      restoreAttachmentActivationContractVersion: 1,
      restoreAttachmentReconciliationContractVersion:
        RESTORE_ATTACHMENT_RECONCILIATION_CONTRACT_VERSION,
      restoreCheckpoint: forbidden("restoreCheckpoint"),
    },
    forbiddenCalls,
    prepareCalls,
    providerCalls,
    reconciliationCalls,
  };
}

function physicalPolicies(methods) {
  return Object.fromEntries(
    methods.map((method) => [
      method,
      Object.freeze({
        deadlineMilliseconds: 30_000,
        settlementGraceMilliseconds: 1_000,
      }),
    ]),
  );
}

function createPhysicalPublicationBinding(rawPublication, rawLifecycle) {
  const unexpected = async function unexpectedPhysicalProvider() {
    throw new Error("unrelated physical provider must not run");
  };
  return createPostgresDetachedRestorePhysicalBindings({
    lifecycleBackend: Object.freeze({
      ...rawLifecycle,
      physicalInvocationContractVersion: 1,
    }),
    lifecycleSettlement: physicalPolicies(PHYSICAL_LIFECYCLE_METHODS),
    onFatal: ignorePhysicalFatal,
    publication: rawPublication,
    publicationSettlement: physicalPolicies(PHYSICAL_PUBLICATION_METHODS),
    resolveRestoreDestination: unexpected,
    resolveRestoreDestinationContractVersion: 1,
    resolveRestoreDestinationSettlement: Object.freeze({
      deadlineMilliseconds: 30_000,
      settlementGraceMilliseconds: 1_000,
    }),
    supervisor: Object.freeze({
      contractVersion: 2,
      launchWriter: unexpected,
      reconcileWriterLaunch: unexpected,
      supervisorId: "coordinator-physical-supervisor-001",
    }),
    supervisorSettlement: physicalPolicies(PHYSICAL_SUPERVISOR_METHODS),
  }).publication;
}

function createCoordinator(
  fixture,
  authority,
  storage,
  guardPool = new GuardPool(),
  publication = fixture.publication,
) {
  const destinations = [];
  const coordinator = createPostgresRestoreActivationRecoveryCoordinator({
    authority,
    operationGuard: new PostgresOperationGuard({ dedicatedPool: guardPool }),
    publication,
    resolveRestoreDestination: async (input) => {
      destinations.push(input);
      return {
        destinationDirectory: fixture.destinationDirectory,
        destinationOwnedRoot: fixture.destinationOwnedRoot,
      };
    },
    storageBackend: storage.backend,
  });
  return { coordinator, destinations, guardPool };
}

function assertCoordinatorCode(code) {
  return (error) => {
    assert(error instanceof PostgresRestoreActivationRecoveryCoordinatorError);
    assert.equal(error.code, code);
    assert.equal(error.retryable, false);
    assert.equal(Object.isFrozen(error), true);
    assert.equal(Object.hasOwn(error, "cause"), false);
    return true;
  };
}

async function assertPromptCoordinatorRejection(promise, code) {
  let timeout;
  const deadline = new Promise((_, reject) => {
    timeout = setTimeout(
      () => reject(new Error("coordinator settlement timed out")),
      2_000,
    );
  });
  try {
    await assert.rejects(
      Promise.race([promise, deadline]),
      assertCoordinatorCode(code),
    );
  } finally {
    clearTimeout(timeout);
  }
}

test("uses the authority's fixed operation-envelope serialization order", () => {
  assert.equal(
    requestSha256(
      { sessionId: "session-001" },
      { contractVersion: 1 },
    ),
    "9f129ee7ec8f27ce24f9e1e751d7ffaba48c8fa434f5a82bca58e7ba3d38b772",
  );
});

test(
  "coordinator accepts only authentic legacy or branded publication verifiers",
  { concurrency: false },
  async (t) => {
    await t.test("rejects a forged four-method publication duck", async (t) => {
      const fixture = await createPublishedRestoreFixture(t);
      const authority = authorityHarness({});
      const storage = storageBackendHarness(async () => {
        throw new Error("storage provider must not run");
      });
      const forged = Object.freeze(
        Object.fromEntries(
          PHYSICAL_PUBLICATION_METHODS.map((method) => [
            method,
            Object.freeze(async function forgedPublicationMethod() {
              throw new Error("forged publication must not run");
            }),
          ]),
        ),
      );
      assert.equal(isPostgresDetachedRestorePublicationBinding(forged), false);
      assert.throws(
        () =>
          createCoordinator(
            fixture,
            authority.authority,
            storage,
            new GuardPool(),
            forged,
          ),
        assertCoordinatorCode(
          "invalid_postgres_restore_activation_recovery_coordinator_options",
        ),
      );
      assert.deepEqual(authority.calls, []);
      assert.deepEqual(storage.providerCalls, []);
    });

    await t.test("delegates generation verification through a real binding", async (t) => {
      const fixture = await createPublishedRestoreFixture(t);
      await makeSourceUnavailable(fixture);
      const committed = generationRead(fixture, "committed", true);
      const authority = authorityHarness({
        "finalize-generation": async () => committed,
        "read-generation": async () => generationRead(fixture, "uncertain"),
      });
      const storage = storageBackendHarness(async () => {
        throw new Error("generation replay must not attach");
      });
      const publication = createPhysicalPublicationBinding(
        fixture.publication,
        storage.backend,
      );
      const { coordinator } = createCoordinator(
        fixture,
        authority.authority,
        storage,
        new GuardPool(),
        publication,
      );

      const result = await coordinator.reconcileRestoreGeneration(
        generationCandidate(fixture),
      );

      assert.equal(result.operation.state, "committed");
      assert.deepEqual(storage.providerCalls, []);
    });

    await t.test("delegates activation verification through a real binding", async (t) => {
      const fixture = await createPublishedRestoreFixture(t);
      await makeSourceUnavailable(fixture);
      const request = activationRequest(fixture);
      const prepared = activationResult(request);
      const authority = authorityHarness({
        "finalize-activation": async () =>
          activationHandoff(fixture, request),
        "read-activation": async () =>
          activationRead(fixture, "uncertain", request),
      });
      const storage = storageBackendHarness(async () => prepared);
      const publication = createPhysicalPublicationBinding(
        fixture.publication,
        storage.backend,
      );
      const { coordinator } = createCoordinator(
        fixture,
        authority.authority,
        storage,
        new GuardPool(),
        publication,
      );

      const result = await coordinator.reconcileRestoreAttachmentActivation(
        activationCandidate(fixture),
      );

      assert.equal(result.activation.operation.state, "committed");
      assert.equal(storage.providerCalls.length, 1);
    });

    await t.test("maps branded verifier rejection to coordinator uncertainty", async (t) => {
      const fixture = await createPublishedRestoreFixture(t);
      await makeSourceUnavailable(fixture);
      const rejectingPublication = new StoppedDirectoryPublication({
        acquireLock: async () => {
          throw new Error("physical verification rejected");
        },
        inspectFilesystem: async () => ({
          durability: "local-fsync-rename",
          filesystemId: "test-filesystem-001",
          objectIdentityScheme: TEST_OBJECT_IDENTITY_SCHEME,
          type: "test-local",
        }),
        inspectPersistentObjectIdentity,
        journal: fixture.journal,
        ...TRUSTED_PUBLICATION_INSPECTORS,
      });
      const authority = authorityHarness({
        "finalize-generation": async () => {
          throw new Error("rejected verification must not finalize");
        },
        "read-generation": async () => generationRead(fixture, "uncertain"),
      });
      const storage = storageBackendHarness(async () => {
        throw new Error("generation replay must not attach");
      });
      const publication = createPhysicalPublicationBinding(
        rejectingPublication,
        storage.backend,
      );
      const { coordinator } = createCoordinator(
        fixture,
        authority.authority,
        storage,
        new GuardPool(),
        publication,
      );

      await assert.rejects(
        coordinator.reconcileRestoreGeneration(generationCandidate(fixture)),
        assertCoordinatorCode(
          "postgres_restore_activation_recovery_coordinator_outcome_uncertain",
        ),
      );
      assert.deepEqual(
        authority.calls.map(([name]) => name),
        ["read-generation"],
      );
    });
  },
);

test("reconciles one committed restore generation without source, image, or launch work", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const committed = generationRead(fixture, "committed", true);
  const authority = authorityHarness({
    "finalize-generation": async (input) => {
      assert.equal(input.expectedOperationRevision, "2");
      assert.deepEqual(input.completion, {
        ...fixture.restore,
        replayed: true,
      });
      assert.equal(Object.hasOwn(input, "launch"), false);
      return committed;
    },
    "read-generation": async () => generationRead(fixture, "uncertain"),
  });
  const storage = storageBackendHarness(async () => {
    throw new Error("activation provider must not run for generation replay");
  });
  const { coordinator, destinations } = createCoordinator(
    fixture,
    authority.authority,
    storage,
  );

  const result = await coordinator.reconcileRestoreGeneration(
    generationCandidate(fixture),
  );

  assert.equal(result.operation.state, "committed");
  assert.deepEqual(
    authority.calls.map(([name]) => name),
    ["read-generation", "finalize-generation"],
  );
  assert.equal(destinations.length, 1);
  assert.equal(destinations[0].kind, "generation");
  assert.deepEqual(storage.providerCalls, []);
  assert.deepEqual(storage.forbiddenCalls, []);
  assert.deepEqual(fixture.transitionGuard.calls, []);
});

test("accepts a committed restore generation copied across storage IDs", async (t) => {
  const fixture = await createPublishedRestoreFixture(t, {
    sourceStorageId: "source-volume-001",
  });
  await makeSourceUnavailable(fixture);
  assert.equal(fixture.checkpoint.storageId, "source-volume-001");
  assert.equal(fixture.restoreRequest.storageId, STORAGE_ID);
  const authority = authorityHarness({
    "read-generation": async () =>
      generationRead(fixture, "committed"),
  });
  const storage = storageBackendHarness(async () => {
    throw new Error("activation provider must not run for generation replay");
  });
  const { coordinator, destinations } = createCoordinator(
    fixture,
    authority.authority,
    storage,
  );

  const result = await coordinator.reconcileRestoreGeneration(
    generationCandidate(fixture),
  );

  assert.equal(result.operation.state, "committed");
  assert.deepEqual(destinations, []);
  assert.deepEqual(storage.providerCalls, []);
});

test("canonicalizes reordered generation candidates before authority reads", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const candidate = generationCandidate(fixture);
  candidate.checkpoint = reversedDataObject(candidate.checkpoint);
  candidate.request = reversedDataObject(candidate.request);
  candidate.request.target = reversedDataObject(candidate.request.target);
  const authority = authorityHarness({
    "read-generation": async (input) => {
      assert.equal(
        JSON.stringify(input.checkpoint),
        JSON.stringify(fixture.checkpoint),
      );
      assert.equal(
        JSON.stringify(input.request),
        JSON.stringify(fixture.restoreRequest),
      );
      return generationRead(fixture, "committed");
    },
  });
  const storage = storageBackendHarness(async () => {
    throw new Error("committed generation replay must not attach");
  });
  const { coordinator } = createCoordinator(
    fixture,
    authority.authority,
    storage,
  );

  const result = await coordinator.reconcileRestoreGeneration(candidate);

  assert.equal(result.operation.state, "committed");
  assert.equal(storage.providerCalls.length, 0);
});

test("rejects a stale restore-generation fence before authority work", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  const candidate = structuredClone(generationCandidate(fixture));
  candidate.request.fencingEpoch = fixture.checkpoint.sourceFencingEpoch;
  const authority = authorityHarness({});
  const storage = storageBackendHarness(async () => {
    throw new Error("a stale generation request must not attach");
  });
  const { coordinator } = createCoordinator(
    fixture,
    authority.authority,
    storage,
  );

  await assert.rejects(
    coordinator.reconcileRestoreGeneration(candidate),
    assertCoordinatorCode(
      "invalid_postgres_restore_activation_recovery_coordinator_request",
    ),
  );

  assert.deepEqual(authority.calls, []);
  assert.equal(storage.providerCalls.length, 0);
});

test("accepts a direct committed generation at operation revision two", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const authority = authorityHarness({
    "read-generation": async () => directCommittedGenerationRead(fixture),
  });
  const storage = storageBackendHarness(async () => {
    throw new Error("committed generation replay must not attach");
  });
  const { coordinator } = createCoordinator(
    fixture,
    authority.authority,
    storage,
  );

  const result = await coordinator.reconcileRestoreGeneration(
    generationCandidate(fixture),
  );

  assert.equal(result.operation.revision, "2");
  assert.equal(result.session.revision, "23");
  assert.equal(storage.providerCalls.length, 0);
});

test("accepts valid later current-session history for a committed generation", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const current = advanceCommittedGenerationCurrentSession(
    generationRead(fixture, "committed"),
  );
  const authority = authorityHarness({
    "read-generation": async () => current,
  });
  const storage = storageBackendHarness(async () => {
    throw new Error("generation history replay must not attach");
  });
  const { coordinator } = createCoordinator(
    fixture,
    authority.authority,
    storage,
  );

  const result = await coordinator.reconcileRestoreGeneration(
    generationCandidate(fixture),
  );

  assert.equal(result.session.revision, "27");
  assert.equal(storage.providerCalls.length, 0);
});

test("rejects inconsistent later current-session history for a committed generation", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const forged = advanceCommittedGenerationCurrentSession(
    generationRead(fixture, "committed"),
  );
  forged.session.document.lastOperation.operationRevision = "1";
  const authority = authorityHarness({
    "read-generation": async () => forged,
  });
  const storage = storageBackendHarness(async () => {
    throw new Error("invalid generation history must not attach");
  });
  const { coordinator } = createCoordinator(
    fixture,
    authority.authority,
    storage,
  );

  await assert.rejects(
    coordinator.reconcileRestoreGeneration(generationCandidate(fixture)),
    assertCoordinatorCode(
      "postgres_restore_activation_recovery_coordinator_outcome_uncertain",
    ),
  );

  assert.equal(storage.providerCalls.length, 0);
});

test("rejects incomplete committed generation authority relations on fast return", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const scenarios = [
    ["binding", (receipt) => {
      receipt.generation.binding.destinationState = "attached";
    }],
    ["catalogue", (receipt) => {
      delete receipt.catalogue.document.result;
    }],
    ["generation document", (receipt) => {
      receipt.generation.document.materialization.publicationKind =
        "checkpoint-artifact";
    }],
    ["terminal result", (receipt) => {
      delete receipt.operation.result.resultVersion;
    }],
    ["generation timestamp", (receipt) => {
      receipt.generation.claimedAt = "2026-08-05T00:02:00.000Z";
    }],
    ["typed request", (receipt) => {
      receipt.operation.request.predeterminedResult.mutation.proofId =
        "forged-restore-proof-001";
      receipt.generation.document.result = structuredClone(
        receipt.operation.request.predeterminedResult,
      );
      synchronizeCommittedGenerationOperation(receipt);
    }],
  ];

  for (const [scenario, mutate] of scenarios) {
    const forged = generationRead(fixture, "committed");
    mutate(forged);
    const authority = authorityHarness({
      "read-generation": async () => forged,
    });
    const storage = storageBackendHarness(async () => {
      throw new Error("invalid generation receipt must not attach");
    });
    const { coordinator } = createCoordinator(
      fixture,
      authority.authority,
      storage,
    );

    await assert.rejects(
      coordinator.reconcileRestoreGeneration(generationCandidate(fixture)),
      assertCoordinatorCode(
        "postgres_restore_activation_recovery_coordinator_outcome_uncertain",
      ),
      scenario,
    );

    assert.equal(storage.providerCalls.length, 0, scenario);
  }
});

test("does not launder a malformed generation finalizer receipt through readback", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const malformed = generationRead(fixture, "committed", true);
  delete malformed.operation.result.resultVersion;
  let readCalls = 0;
  const authority = authorityHarness({
    "finalize-generation": async () => malformed,
    "read-generation": async () => {
      readCalls += 1;
      return readCalls === 1
        ? generationRead(fixture, "uncertain")
        : generationRead(fixture, "committed");
    },
  });
  const storage = storageBackendHarness(async () => {
    throw new Error("generation reconciliation must not attach");
  });
  const { coordinator } = createCoordinator(
    fixture,
    authority.authority,
    storage,
  );

  await assert.rejects(
    coordinator.reconcileRestoreGeneration(generationCandidate(fixture)),
    assertCoordinatorCode(
      "postgres_restore_activation_recovery_coordinator_outcome_uncertain",
    ),
  );

  assert.equal(readCalls, 1);
  assert.equal(storage.providerCalls.length, 0);
});

test("rejects a revision-two generation receipt from the uncertain finalizer path", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const forged = directCommittedGenerationRead(fixture, true);
  const authority = authorityHarness({
    "finalize-generation": async () => forged,
    "read-generation": async () => generationRead(fixture, "uncertain"),
  });
  const storage = storageBackendHarness(async () => {
    throw new Error("generation reconciliation must not attach");
  });
  const { coordinator } = createCoordinator(
    fixture,
    authority.authority,
    storage,
  );

  await assert.rejects(
    coordinator.reconcileRestoreGeneration(generationCandidate(fixture)),
    assertCoordinatorCode(
      "postgres_restore_activation_recovery_coordinator_outcome_uncertain",
    ),
  );

  assert.equal(storage.providerCalls.length, 0);
});

test("accepts a revision-three idempotent generation finalization replay", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const replay = generationRead(fixture, "committed", true);
  replay.finalized = false;
  const authority = authorityHarness({
    "finalize-generation": async () => replay,
    "read-generation": async () => generationRead(fixture, "uncertain"),
  });
  const storage = storageBackendHarness(async () => {
    throw new Error("generation reconciliation must not attach");
  });
  const { coordinator } = createCoordinator(
    fixture,
    authority.authority,
    storage,
  );

  const result = await coordinator.reconcileRestoreGeneration(
    generationCandidate(fixture),
  );

  assert.equal(result.finalized, false);
  assert.equal(result.operation.revision, "3");
  assert.equal(storage.providerCalls.length, 0);
});

test("converts a starting generation to uncertain before committed replay", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  let state = "starting";
  const authority = authorityHarness({
    "finalize-generation": async () =>
      generationRead(fixture, "committed", true),
    "mark-uncertain": async (input) => {
      assert.equal(input.expectedOperationRevision, "1");
      state = "uncertain";
      throw new Error("uncertain transition acknowledgement lost");
    },
    "read-generation": async () => generationRead(fixture, state),
  });
  const storage = storageBackendHarness(async () => {
    throw new Error("activation provider must not run for generation replay");
  });
  const { coordinator } = createCoordinator(
    fixture,
    authority.authority,
    storage,
  );

  const result = await coordinator.reconcileRestoreGeneration(
    generationCandidate(fixture),
  );

  assert.equal(result.operation.state, "committed");
  assert.deepEqual(
    authority.calls.map(([name]) => name),
    [
      "read-generation",
      "mark-uncertain",
      "read-generation",
      "finalize-generation",
    ],
  );
  assert.deepEqual(storage.providerCalls, []);
  assert.deepEqual(storage.forbiddenCalls, []);
  assert.deepEqual(fixture.transitionGuard.calls, []);
});

test("accepts committed readback when the starting-to-uncertain acknowledgement is lost", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  let state = "starting";
  const authority = authorityHarness({
    "mark-uncertain": async (input) => {
      assert.equal(input.expectedOperationRevision, "1");
      state = "committed";
      throw new Error("uncertain transition returned a lost acknowledgement");
    },
    "read-generation": async () => generationRead(fixture, state),
  });
  const storage = storageBackendHarness(() => {
    throw new Error("provider must not run after committed readback");
  });
  const { coordinator, destinations } = createCoordinator(
    fixture,
    authority.authority,
    storage,
  );

  const result = await coordinator.reconcileRestoreGeneration(
    generationCandidate(fixture),
  );

  assert.equal(result.operation.state, "committed");
  assert.deepEqual(
    authority.calls.map(([name]) => name),
    ["read-generation", "mark-uncertain", "read-generation"],
  );
  assert.deepEqual(destinations, []);
  assert.deepEqual(storage.providerCalls, []);
  assert.deepEqual(storage.forbiddenCalls, []);
  assert.deepEqual(fixture.transitionGuard.calls, []);
});

test("rejects a conflicting committed generation readback after lost finalization acknowledgement", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const conflicting = generationRead(fixture, "committed");
  conflicting.generation.document.result = {
    ...conflicting.generation.document.result,
    proofId: "proof-restore-conflict",
  };
  let committed = false;
  let finalizeCalls = 0;
  const authority = authorityHarness({
    "finalize-generation": async () => {
      finalizeCalls += 1;
      committed = true;
      throw new Error("generation finalization acknowledgement lost");
    },
    "read-generation": async () =>
      committed
        ? conflicting
        : generationRead(fixture, "uncertain"),
  });
  const storage = storageBackendHarness(async () => {
    throw new Error("activation provider must not run for generation replay");
  });
  const { coordinator } = createCoordinator(
    fixture,
    authority.authority,
    storage,
  );

  await assert.rejects(
    coordinator.reconcileRestoreGeneration(generationCandidate(fixture)),
    assertCoordinatorCode(
      "postgres_restore_activation_recovery_coordinator_outcome_uncertain",
    ),
  );

  assert.equal(finalizeCalls, 1);
  assert.deepEqual(storage.providerCalls, []);
  assert.deepEqual(storage.forbiddenCalls, []);
  assert.deepEqual(fixture.transitionGuard.calls, []);
});

test("fresh activation reconciliation finalizes an already applied attachment without prepare", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const request = activationRequest(fixture);
  const prepared = activationResult(request);
  const base = activationClaimBase(fixture);
  let finalizerInput;
  const authority = authorityHarness({
    "claim-activation": async (input) => {
      assert.equal(input.expectedOperationRevision, "0");
      return activationClaimReceipt(fixture, request, {
        dispatchGranted: true,
      });
    },
    "finalize-activation": async (input) => {
      finalizerInput = input;
      return activationHandoff(fixture, request, "2");
    },
  });
  const storage = storageBackendHarness(
    async () => assert.fail("applied reconciliation must not prepare"),
    {
      reconcile: async (input) => {
        assert.deepEqual(input, request);
        return activationReconciliation("applied", prepared);
      },
    },
  );
  const { coordinator } = createCoordinator(
    fixture,
    authority.authority,
    storage,
  );

  const result = await coordinator.claimAndReconcileRestoreAttachmentActivation(
    base,
  );

  assert.equal(result.activation.operation.revision, "2");
  assert.equal(result.launch.operation.state, "prepared");
  assert.equal(finalizerInput.expectedOperationRevision, "1");
  assert.equal(
    JSON.stringify(finalizerInput.activationResult),
    JSON.stringify(canonicalJsonFixture(prepared)),
  );
  assert.equal(storage.reconciliationCalls.length, 1);
  assert.equal(storage.prepareCalls.length, 0);
  assert.deepEqual(
    authority.calls.map(([name]) => name),
    ["claim-activation", "finalize-activation"],
  );
});

test("fresh absent-and-quiescent reconciliation grants the only physical prepare", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const request = activationRequest(fixture);
  const prepared = activationResult(request);
  const authority = authorityHarness({
    "claim-activation": async () =>
      activationClaimReceipt(fixture, request, {
        dispatchGranted: true,
      }),
    "finalize-activation": async (input) => {
      assert.equal(input.expectedOperationRevision, "1");
      return activationHandoff(fixture, request, "2");
    },
  });
  const storage = storageBackendHarness(async (input) => {
    assert.deepEqual(input, request);
    return prepared;
  }, {
    reconcile: async () =>
      activationReconciliation("absent-and-quiescent"),
  });
  const { coordinator } = createCoordinator(
    fixture,
    authority.authority,
    storage,
  );

  const result = await coordinator.claimAndReconcileRestoreAttachmentActivation(
    activationClaimBase(fixture),
  );

  assert.equal(result.activation.operation.revision, "2");
  assert.equal(storage.reconciliationCalls.length, 1);
  assert.equal(storage.prepareCalls.length, 1);
});

test("fresh unknown reconciliation fails without prepare or uncertainty mutation", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const request = activationRequest(fixture);
  const authority = authorityHarness({
    "claim-activation": async () =>
      activationClaimReceipt(fixture, request, {
        dispatchGranted: true,
      }),
  });
  const storage = storageBackendHarness(
    async () => assert.fail("unknown reconciliation must not prepare"),
    {
      reconcile: async () => activationReconciliation("unknown"),
    },
  );
  const { coordinator } = createCoordinator(
    fixture,
    authority.authority,
    storage,
  );

  await assert.rejects(
    coordinator.claimAndReconcileRestoreAttachmentActivation(
      activationClaimBase(fixture),
    ),
    assertCoordinatorCode(
      "postgres_restore_activation_recovery_coordinator_outcome_uncertain",
    ),
  );

  assert.equal(storage.reconciliationCalls.length, 1);
  assert.equal(storage.prepareCalls.length, 0);
  assert.deepEqual(
    authority.calls.map(([name]) => name),
    ["claim-activation"],
  );
});

test("retained starting activation becomes uncertain before applied reconciliation", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const request = activationRequest(fixture);
  const prepared = activationResult(request);
  let state = "starting";
  const authority = authorityHarness({
    "finalize-activation": async (input) => {
      assert.equal(input.expectedOperationRevision, "2");
      return activationHandoff(fixture, request);
    },
    "mark-uncertain": async (input) => {
      assert.equal(input.expectedOperationRevision, "1");
      state = "uncertain";
      return { marked: true };
    },
    "read-activation": async () => activationRead(fixture, state, request),
  });
  const storage = storageBackendHarness(
    async () => assert.fail("retained activation must not prepare"),
    {
      reconcile: async () => activationReconciliation("applied", prepared),
    },
  );
  const { coordinator } = createCoordinator(
    fixture,
    authority.authority,
    storage,
  );

  const result = await coordinator.reconcileRestoreAttachmentActivation(
    activationCandidate(fixture, "starting"),
  );

  assert.equal(result.activation.operation.revision, "3");
  assert.equal(storage.reconciliationCalls.length, 1);
  assert.equal(storage.prepareCalls.length, 0);
  assert.deepEqual(
    authority.calls.map(([name]) => name),
    [
      "read-activation",
      "mark-uncertain",
      "read-activation",
      "finalize-activation",
    ],
  );
});

test("claim acknowledgement-loss retained starting reconciles absent state without redispatch", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const request = activationRequest(fixture);
  let state = "starting";
  const authority = authorityHarness({
    "mark-uncertain": async (input) => {
      assert.equal(input.expectedOperationRevision, "1");
      state = "uncertain";
      return { marked: true };
    },
    "read-activation": async () => activationRead(fixture, state, request),
  });
  const storage = storageBackendHarness(
    async () => assert.fail("a retained claim must not redispatch prepare"),
    {
      reconcile: async (input) => {
        assert.deepEqual(input, request);
        return activationReconciliation("absent-and-quiescent");
      },
    },
  );
  const { coordinator } = createCoordinator(
    fixture,
    authority.authority,
    storage,
  );
  const candidate = activationCandidate(fixture, "starting");

  assert.deepEqual(Reflect.ownKeys(candidate), [
    "activationOperationId",
    "request",
    "state",
  ]);
  await assert.rejects(
    coordinator.reconcileRestoreAttachmentActivation(candidate),
    assertCoordinatorCode(
      "postgres_restore_activation_recovery_coordinator_outcome_uncertain",
    ),
  );

  assert.equal(storage.reconciliationCalls.length, 1);
  assert.equal(storage.prepareCalls.length, 0);
  assert.deepEqual(
    authority.calls.map(([name]) => name),
    ["read-activation", "mark-uncertain", "read-activation"],
  );
});

for (const outcome of ["absent-and-quiescent", "unknown"]) {
  test(`retained uncertain ${outcome} reconciliation never prepares`, async (t) => {
    const fixture = await createPublishedRestoreFixture(t);
    await makeSourceUnavailable(fixture);
    const request = activationRequest(fixture);
    const authority = authorityHarness({
      "read-activation": async () =>
        activationRead(fixture, "uncertain", request),
    });
    const storage = storageBackendHarness(
      async () => assert.fail("retained activation must not prepare"),
      {
        reconcile: async () => activationReconciliation(outcome),
      },
    );
    const { coordinator } = createCoordinator(
      fixture,
      authority.authority,
      storage,
    );

    await assert.rejects(
      coordinator.reconcileRestoreAttachmentActivation(
        activationCandidate(fixture),
      ),
      assertCoordinatorCode(
        "postgres_restore_activation_recovery_coordinator_outcome_uncertain",
      ),
    );

    assert.equal(storage.reconciliationCalls.length, 1, outcome);
    assert.equal(storage.prepareCalls.length, 0, outcome);
  });
}

test("a fulfilled replay claim has no local grant and cannot prepare", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const request = activationRequest(fixture);
  let state = "starting";
  const authority = authorityHarness({
    "claim-activation": async () =>
      activationClaimReceipt(fixture, request, {
        dispatchGranted: false,
        state: "starting",
      }),
    "mark-uncertain": async (input) => {
      assert.equal(input.expectedOperationRevision, "1");
      state = "uncertain";
      return { marked: true };
    },
    "read-activation": async () => activationRead(fixture, state, request),
  });
  const storage = storageBackendHarness(
    async () => assert.fail("an uncertain activation must not prepare"),
    {
      reconcile: async () =>
        activationReconciliation("absent-and-quiescent"),
    },
  );
  const { coordinator } = createCoordinator(
    fixture,
    authority.authority,
    storage,
  );

  await assert.rejects(
    coordinator.claimAndReconcileRestoreAttachmentActivation(
      activationClaimBase(fixture),
    ),
    assertCoordinatorCode(
      "postgres_restore_activation_recovery_coordinator_outcome_uncertain",
    ),
  );

  assert.equal(storage.reconciliationCalls.length, 1);
  assert.equal(storage.prepareCalls.length, 0);
  assert.deepEqual(
    authority.calls.map(([name]) => name),
    ["claim-activation", "mark-uncertain", "read-activation"],
  );
});

test("claim acknowledgement loss uses a no-grant readback and cannot prepare", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const request = activationRequest(fixture);
  let state = "starting";
  const authority = authorityHarness({
    "claim-activation": async () => {
      throw new Error("claim acknowledgement lost");
    },
    "mark-uncertain": async () => {
      state = "uncertain";
      return { marked: true };
    },
    "read-activation": async () => activationRead(fixture, state, request),
  });
  const storage = storageBackendHarness(
    async () => assert.fail("claim readback must not prepare"),
    {
      reconcile: async () =>
        activationReconciliation("absent-and-quiescent"),
    },
  );
  const { coordinator } = createCoordinator(
    fixture,
    authority.authority,
    storage,
  );

  await assert.rejects(
    coordinator.claimAndReconcileRestoreAttachmentActivation(
      activationClaimBase(fixture),
    ),
    assertCoordinatorCode(
      "postgres_restore_activation_recovery_coordinator_outcome_uncertain",
    ),
  );

  assert.equal(storage.reconciliationCalls.length, 1);
  assert.equal(storage.prepareCalls.length, 0);
  assert.deepEqual(
    authority.calls.map(([name]) => name),
    [
      "claim-activation",
      "read-activation",
      "mark-uncertain",
      "read-activation",
    ],
  );
});

test("claim grant and physical prepare remain in one guarded critical section", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const request = activationRequest(fixture);
  const prepared = activationResult(request);
  let claimInFlight = false;
  let claimSettled = false;
  const sequence = [];
  const authority = authorityHarness({
    "claim-activation": async () => {
      assert.equal(claimInFlight, false);
      claimInFlight = true;
      sequence.push("claim-start");
      await Promise.resolve();
      claimSettled = true;
      sequence.push("claim-end");
      return activationClaimReceipt(fixture, request, {
        dispatchGranted: true,
      });
    },
    "finalize-activation": async () => {
      sequence.push("finalize");
      claimInFlight = false;
      return activationHandoff(fixture, request, "2");
    },
  });
  const storage = storageBackendHarness(
    async () => {
      assert.equal(claimInFlight, true);
      assert.equal(claimSettled, true);
      sequence.push("prepare");
      return prepared;
    },
    {
      reconcile: async () => {
        assert.equal(claimInFlight, true);
        assert.equal(claimSettled, true);
        sequence.push("reconcile");
        return activationReconciliation("absent-and-quiescent");
      },
    },
  );
  const { coordinator, guardPool } = createCoordinator(
    fixture,
    authority.authority,
    storage,
  );

  await coordinator.claimAndReconcileRestoreAttachmentActivation(
    activationClaimBase(fixture),
  );

  assert.deepEqual(sequence, [
    "claim-start",
    "claim-end",
    "reconcile",
    "prepare",
    "finalize",
  ]);
  assert.equal(guardPool.connectCalls, 1);
  assert.equal(storage.prepareCalls.length, 1);
});

test("a concurrent retained recovery cannot race a guarded fresh claim", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const request = activationRequest(fixture);
  const prepared = activationResult(request);
  let enterReconciliation;
  let releaseReconciliation;
  const reconciliationEntered = new Promise((resolve) => {
    enterReconciliation = resolve;
  });
  const reconciliationGate = new Promise((resolve) => {
    releaseReconciliation = resolve;
  });
  const guardPool = new SharedExclusiveGuardPool();
  let freshHolder = null;
  const authority = authorityHarness({
    "claim-activation": async () => {
      freshHolder = guardPool.holder;
      assert.equal(guardPool.isHeld(freshHolder), true);
      return activationClaimReceipt(fixture, request, {
        dispatchGranted: true,
      });
    },
    "finalize-activation": async () => {
      assert.equal(guardPool.isHeld(freshHolder), true);
      return activationHandoff(fixture, request, "2");
    },
  });
  const storage = storageBackendHarness(
    async () => {
      assert.equal(guardPool.isHeld(freshHolder), true);
      return prepared;
    },
    {
      reconcile: async () => {
        assert.equal(guardPool.isHeld(freshHolder), true);
        enterReconciliation();
        await reconciliationGate;
        assert.equal(guardPool.isHeld(freshHolder), true);
        return activationReconciliation("absent-and-quiescent");
      },
    },
  );
  const { coordinator } = createCoordinator(
    fixture,
    authority.authority,
    storage,
    guardPool,
  );

  const fresh = coordinator.claimAndReconcileRestoreAttachmentActivation(
    activationClaimBase(fixture),
  );
  await reconciliationEntered;

  let retainedError = null;
  try {
    await coordinator.reconcileRestoreAttachmentActivation(
      activationCandidate(fixture, "starting"),
    );
  } catch (error) {
    retainedError = error;
  } finally {
    releaseReconciliation();
  }

  assertCoordinatorCode(
    "postgres_restore_activation_recovery_coordinator_outcome_uncertain",
  )(retainedError);
  assert.deepEqual(
    authority.calls.map(([name]) => name),
    ["claim-activation"],
  );
  assert.equal(storage.reconciliationCalls.length, 1);
  assert.equal(storage.prepareCalls.length, 0);
  assert.equal(guardPool.isHeld(freshHolder), true);

  const result = await fresh;
  assert.equal(result.activation.operation.state, "committed");
  assert.deepEqual(
    authority.calls.map(([name]) => name),
    ["claim-activation", "finalize-activation"],
  );
  assert.equal(storage.prepareCalls.length, 1);
  assert.equal(guardPool.connectCalls, 2);
  assert.equal(guardPool.holder, null);
});

test("post-prepare finalization failure marks uncertainty and recovery never prepares again", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const request = activationRequest(fixture);
  let state = "starting";
  let reconciliationCalls = 0;
  const authority = authorityHarness({
    "claim-activation": async () =>
      activationClaimReceipt(fixture, request, {
        dispatchGranted: true,
      }),
    "finalize-activation": async () => {
      throw new Error("finalization acknowledgement remains uncertain");
    },
    "mark-uncertain": async (input) => {
      assert.equal(input.expectedOperationRevision, "1");
      state = "uncertain";
      return { marked: true };
    },
    "read-activation": async () => activationRead(fixture, state, request),
  });
  const storage = storageBackendHarness(
    async () => activationResult(request),
    {
      reconcile: async () => {
        reconciliationCalls += 1;
        return activationReconciliation(
          reconciliationCalls === 1 ? "absent-and-quiescent" : "unknown",
        );
      },
    },
  );
  const { coordinator } = createCoordinator(
    fixture,
    authority.authority,
    storage,
  );

  await assert.rejects(
    coordinator.claimAndReconcileRestoreAttachmentActivation(
      activationClaimBase(fixture),
    ),
    assertCoordinatorCode(
      "postgres_restore_activation_recovery_coordinator_outcome_uncertain",
    ),
  );
  await assert.rejects(
    coordinator.reconcileRestoreAttachmentActivation(
      activationCandidate(fixture),
    ),
    assertCoordinatorCode(
      "postgres_restore_activation_recovery_coordinator_outcome_uncertain",
    ),
  );

  assert.equal(storage.reconciliationCalls.length, 2);
  assert.equal(storage.prepareCalls.length, 1);
  assert.deepEqual(
    authority.calls.map(([name]) => name),
    [
      "claim-activation",
      "finalize-activation",
      "finalize-activation",
      "mark-uncertain",
      "read-activation",
    ],
  );
});

test("every ambiguous prepare settlement marks the fresh activation uncertain", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const request = activationRequest(fixture);
  const scenarios = [
    ["synchronous throw", () => {
      throw new Error("prepare failed synchronously");
    }],
    ["rejected promise", () => Promise.reject(new Error("prepare rejected"))],
    ["invalid fulfillment", () => ({ invalid: true })],
  ];

  for (const [scenario, prepare] of scenarios) {
    let markCalls = 0;
    const authority = authorityHarness({
      "claim-activation": async () =>
        activationClaimReceipt(fixture, request, {
          dispatchGranted: true,
        }),
      "mark-uncertain": async (input) => {
        markCalls += 1;
        assert.equal(input.expectedOperationRevision, "1");
        return { marked: true };
      },
      "read-activation": async () =>
        activationRead(fixture, "starting", request),
    });
    const storage = storageBackendHarness(prepare, {
      reconcile: async () =>
        activationReconciliation("absent-and-quiescent"),
    });
    const { coordinator } = createCoordinator(
      fixture,
      authority.authority,
      storage,
    );

    await assert.rejects(
      coordinator.claimAndReconcileRestoreAttachmentActivation(
        activationClaimBase(fixture),
      ),
      assertCoordinatorCode(
        "postgres_restore_activation_recovery_coordinator_outcome_uncertain",
      ),
      scenario,
    );

    assert.equal(storage.reconciliationCalls.length, 1, scenario);
    assert.equal(storage.prepareCalls.length, 1, scenario);
    assert.equal(markCalls, 1, scenario);
  }
});

test("caller-supplied dispatch grants are rejected before authority or provider calls", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const authority = authorityHarness({});
  const storage = storageBackendHarness(async () => {
    throw new Error("invalid candidate must not reach the backend");
  });
  const { coordinator } = createCoordinator(
    fixture,
    authority.authority,
    storage,
  );
  const invalid = [
    { ...activationCandidate(fixture, "starting"), dispatchGranted: false },
    { ...activationCandidate(fixture, "starting"), dispatchGranted: true },
    { ...activationCandidate(fixture), dispatchGranted: true },
  ];

  for (const candidate of invalid) {
    await assert.rejects(
      coordinator.reconcileRestoreAttachmentActivation(candidate),
      assertCoordinatorCode(
        "invalid_postgres_restore_activation_recovery_coordinator_request",
      ),
    );
  }
  await assert.rejects(
    coordinator.claimAndReconcileRestoreAttachmentActivation({
      ...activationClaimBase(fixture),
      dispatchGranted: true,
    }),
    assertCoordinatorCode(
      "invalid_postgres_restore_activation_recovery_coordinator_request",
    ),
  );

  assert.deepEqual(authority.calls, []);
  assert.deepEqual(storage.providerCalls, []);
});

test("rejects malformed activation claim receipts before provider work", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const request = activationRequest(fixture);
  const scenarios = [
    {
      name: "true grant without authority clock",
      mutate(receipt) {
        delete receipt.authorityNow;
      },
    },
    {
      name: "false grant with authority clock",
      mutate(receipt) {
        receipt.dispatchGranted = false;
      },
    },
    {
      name: "true grant for an uncertain operation",
      create() {
        return {
          ...activationClaimReceipt(fixture, request, {
            dispatchGranted: false,
            state: "uncertain",
          }),
          authorityNow: "2026-08-05T00:03:00.000Z",
          dispatchGranted: true,
        };
      },
    },
  ];

  for (const scenario of scenarios) {
    const authority = authorityHarness({
      "claim-activation": async () => {
        const receipt = scenario.create?.() ??
          activationClaimReceipt(fixture, request, {
            dispatchGranted: true,
          });
        scenario.mutate?.(receipt);
        return receipt;
      },
    });
    const storage = storageBackendHarness(async () => {
      assert.fail("malformed claim must not reach the backend");
    });
    const { coordinator } = createCoordinator(
      fixture,
      authority.authority,
      storage,
    );

    await assert.rejects(
      coordinator.claimAndReconcileRestoreAttachmentActivation(
        activationClaimBase(fixture),
      ),
      assertCoordinatorCode(
        "postgres_restore_activation_recovery_coordinator_outcome_uncertain",
      ),
      scenario.name,
    );
    assert.deepEqual(storage.providerCalls, [], scenario.name);
  }
});

test("retains version 1 restore attachment activation recovery", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const request = activationRequest(fixture);
  const prepared = activationResult(request);
  const committed = activationHandoff(fixture, request);
  const candidate = activationCandidate(fixture);
  assert.equal(candidate.request.contractVersion, 1);
  assert.deepEqual(Reflect.ownKeys(candidate.request.predecessor).sort(), [
    "attachmentId",
    "detachOperationId",
    "stopOperationId",
  ]);
  const authority = authorityHarness({
    "finalize-activation": async (input) => {
      assert.equal(input.expectedOperationRevision, "2");
      assert.equal(
        JSON.stringify(input.activationResult),
        JSON.stringify(canonicalJsonFixture(prepared)),
      );
      assert.deepEqual(Reflect.ownKeys(input).sort(), [
        "activationResult",
        "expectedOperationRevision",
        "expectedSession",
        "kind",
        "operationId",
        "request",
      ]);
      return committed;
    },
    "read-activation": async () =>
      activationRead(fixture, "uncertain", request),
  });
  const storage = storageBackendHarness(async (input) => {
    assert.deepEqual(input, request);
    return prepared;
  });
  const { coordinator, destinations } = createCoordinator(
    fixture,
    authority.authority,
    storage,
  );

  const result = await coordinator.reconcileRestoreAttachmentActivation(
    candidate,
  );

  assert.equal(result.activation.operation.state, "committed");
  assert.equal(result.launch.operation.state, "prepared");
  assert.deepEqual(
    authority.calls.map(([name]) => name),
    ["read-activation", "finalize-activation"],
  );
  assert.equal(storage.providerCalls.length, 1);
  assert.equal(destinations.length, 1);
  assert.equal(destinations[0].kind, "activation");
  assert.deepEqual(storage.forbiddenCalls, []);
  assert.deepEqual(fixture.transitionGuard.calls, []);
});

test("reconstructs and recovers an exact version 2 activation candidate", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const request = activationRequest(fixture);
  const prepared = activationResult(request);
  const candidate = activationCandidate(fixture, "uncertain", 2);
  const authority = authorityHarness({
    "finalize-activation": async (input) => {
      assert.deepEqual(input.request, candidate.request);
      return activationHandoff(fixture, request, "3", 2);
    },
    "read-activation": async () =>
      activationRead(fixture, "uncertain", request, null, null, 2),
  });
  const storage = storageBackendHarness(async (input) => {
    assert.deepEqual(input, request);
    return prepared;
  });
  const { coordinator, destinations } = createCoordinator(
    fixture,
    authority.authority,
    storage,
  );

  const result = await coordinator.reconcileRestoreAttachmentActivation(
    candidate,
  );

  assert.equal(candidate.request.contractVersion, 2);
  assert.deepEqual({ ...candidate.request.predecessor }, {
    attachmentId: "old-attachment-001",
    captureOperationId: CAPTURE_OPERATION_ID,
    detachOperationId: "detach-operation-001",
    stopOperationId: "stop-operation-001",
  });
  assert.equal(result.activation.operation.request.contractVersion, 2);
  assert.equal(result.activation.operation.state, "committed");
  assert.equal(result.launch.operation.state, "prepared");
  assert.deepEqual(
    authority.calls.map(([name]) => name),
    ["read-activation", "finalize-activation"],
  );
  assert.equal(destinations.length, 1);
  assert.equal(storage.providerCalls.length, 1);
  assert.deepEqual(storage.forbiddenCalls, []);
  assert.deepEqual(fixture.transitionGuard.calls, []);
});

test("rejects hostile or mutated version 2 activation predecessors before provider work", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const request = activationRequest(fixture);
  const mutations = [
    ["attachment", (receipt) => {
      receipt.operation.request.predecessor.attachmentId =
        "forged-attachment-001";
      synchronizeForgedOperationDigest(receipt);
    }],
    ["stop", (receipt) => {
      receipt.operation.request.predecessor.stopOperationId =
        "forged-stop-operation-001";
      synchronizeForgedOperationDigest(receipt);
    }],
    ["capture", (receipt) => {
      receipt.operation.request.predecessor.captureOperationId =
        "forged-capture-operation-001";
      synchronizeForgedOperationDigest(receipt);
    }],
    ["detach", (receipt) => {
      receipt.operation.request.predecessor.detachOperationId =
        "forged-detach-operation-001";
      synchronizeForgedOperationDigest(receipt);
    }],
  ];

  for (const [scenario, mutate] of mutations) {
    const forged = structuredClone(
      activationRead(
        fixture,
        "uncertain",
        request,
        null,
        null,
        2,
      ),
    );
    mutate(forged);
    const authority = authorityHarness({
      "read-activation": async () => forged,
    });
    const storage = storageBackendHarness(async () =>
      activationResult(request));
    const { coordinator, destinations } = createCoordinator(
      fixture,
      authority.authority,
      storage,
    );

    await assert.rejects(
      coordinator.reconcileRestoreAttachmentActivation(
        activationCandidate(fixture, "uncertain", 2),
      ),
      assertCoordinatorCode(
        "postgres_restore_activation_recovery_coordinator_outcome_uncertain",
      ),
      scenario,
    );

    assert.equal(storage.providerCalls.length, 0, scenario);
    assert.equal(destinations.length, 0, scenario);
  }

  const hostile = structuredClone(
    activationRead(
      fixture,
      "uncertain",
      request,
      null,
      null,
      2,
    ),
  );
  let getterCalls = 0;
  Object.defineProperty(
    hostile.operation.request.predecessor,
    "captureOperationId",
    {
      enumerable: true,
      get() {
        getterCalls += 1;
        throw new Error("hostile predecessor getter must not run");
      },
    },
  );
  const authority = authorityHarness({
    "read-activation": async () => hostile,
  });
  const storage = storageBackendHarness(async () => activationResult(request));
  const { coordinator, destinations } = createCoordinator(
    fixture,
    authority.authority,
    storage,
  );

  await assert.rejects(
    coordinator.reconcileRestoreAttachmentActivation(
      activationCandidate(fixture, "uncertain", 2),
    ),
    assertCoordinatorCode(
      "postgres_restore_activation_recovery_coordinator_outcome_uncertain",
    ),
  );

  assert.equal(getterCalls, 0);
  assert.equal(storage.providerCalls.length, 0);
  assert.equal(destinations.length, 0);
});

test("accepts a reordered valid activation provider result with a canonical authority handoff", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const request = activationRequest(fixture);
  const prepared = activationResult(request);
  const reordered = reversedJsonFixture(prepared);
  const authority = authorityHarness({
    "finalize-activation": async (input) => {
      assert.equal(
        JSON.stringify(input.activationResult),
        JSON.stringify(canonicalJsonFixture(prepared)),
      );
      return activationHandoff(fixture, request);
    },
    "read-activation": async () =>
      activationRead(fixture, "uncertain", request),
  });
  const storage = storageBackendHarness(async () => reordered);
  const { coordinator } = createCoordinator(
    fixture,
    authority.authority,
    storage,
  );

  const result = await coordinator.reconcileRestoreAttachmentActivation(
    activationCandidate(fixture),
  );

  assert.equal(result.activation.operation.state, "committed");
  assert.equal(result.launch.operation.state, "prepared");
  assert.equal(storage.providerCalls.length, 1);
});

test("accepts a bound idempotent handoff replay with finalized false", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const request = activationRequest(fixture);
  const replay = structuredClone(activationHandoff(fixture, request));
  replay.activation.finalized = false;
  const authority = authorityHarness({
    "finalize-activation": async () => replay,
    "read-activation": async () =>
      activationRead(fixture, "uncertain", request),
  });
  const storage = storageBackendHarness(async () => activationResult(request));
  const { coordinator } = createCoordinator(
    fixture,
    authority.authority,
    storage,
  );

  const result = await coordinator.reconcileRestoreAttachmentActivation(
    activationCandidate(fixture),
  );

  assert.equal(result.activation.finalized, false);
  assert.equal(result.launch.operation.state, "prepared");
  assert.equal(storage.providerCalls.length, 1);
});

test("accepts bound starting and uncertain launch replays after createdAt", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const request = activationRequest(fixture);

  for (const state of ["starting", "uncertain"]) {
    const replay = activeLaunchHandoff(fixture, request, state);
    const authority = authorityHarness({
      "finalize-activation": async () => replay,
      "read-activation": async () =>
        activationRead(fixture, "uncertain", request),
    });
    const storage = storageBackendHarness(async () => activationResult(request));
    const { coordinator } = createCoordinator(
      fixture,
      authority.authority,
      storage,
    );

    const result = await coordinator.reconcileRestoreAttachmentActivation(
      activationCandidate(fixture),
    );

    assert.equal(result.activation.finalized, false, state);
    assert.equal(result.launch.operation.state, state, state);
  }
});

test("rejects finalized true once the launch has advanced past prepared", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const request = activationRequest(fixture);
  const forged = activeLaunchHandoff(fixture, request, "starting");
  forged.activation.finalized = true;
  let readCalls = 0;
  const authority = authorityHarness({
    "finalize-activation": async () => forged,
    "read-activation": async () => {
      readCalls += 1;
      return activationRead(fixture, "uncertain", request);
    },
  });
  const storage = storageBackendHarness(async () => activationResult(request));
  const { coordinator } = createCoordinator(
    fixture,
    authority.authority,
    storage,
  );

  await assert.rejects(
    coordinator.reconcileRestoreAttachmentActivation(
      activationCandidate(fixture),
    ),
    assertCoordinatorCode(
      "postgres_restore_activation_recovery_coordinator_outcome_uncertain",
    ),
  );

  assert.equal(readCalls, 1);
  assert.equal(storage.providerCalls.length, 1);
});

test("accepts an idempotent replay whose launch already committed", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const request = activationRequest(fixture);
  const replay = committedLaunchHandoff(fixture, request);
  const authority = authorityHarness({
    "finalize-activation": async () => replay,
    "read-activation": async () =>
      activationRead(fixture, "uncertain", request),
  });
  const storage = storageBackendHarness(async () => activationResult(request));
  const { coordinator } = createCoordinator(
    fixture,
    authority.authority,
    storage,
  );

  const result = await coordinator.reconcileRestoreAttachmentActivation(
    activationCandidate(fixture),
  );

  assert.equal(result.activation.finalized, false);
  assert.equal(result.launch.operation.state, "committed");
  assert.equal(result.session.document.activeOperation, null);
  assert.equal(
    result.session.document.launch.launchAttemptId,
    result.launch.operation.operationId,
  );
  assert.equal(
    result.session.document.lastOperation.operationId,
    result.launch.operation.operationId,
  );
  assert.equal(storage.providerCalls.length, 1);
});

test("accepts non-started and complete-stopped launch terminal replays", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const request = activationRequest(fixture);
  const scenarios = [
    {
      operationRevision: "2",
      outcome: "writer-launch-not-started",
      status: "not-started",
    },
    {
      operationRevision: "3",
      outcome: "writer-launch-complete-stopped",
      status: "complete-stopped",
    },
  ];

  for (const scenario of scenarios) {
    const replay = committedLaunchHandoff(fixture, request, scenario);
    const authority = authorityHarness({
      "finalize-activation": async () => replay,
      "read-activation": async () =>
        activationRead(fixture, "uncertain", request),
    });
    const storage = storageBackendHarness(async () =>
      activationResult(request));
    const { coordinator } = createCoordinator(
      fixture,
      authority.authority,
      storage,
    );

    const result = await coordinator.reconcileRestoreAttachmentActivation(
      activationCandidate(fixture),
    );

    assert.equal(
      result.launch.operation.revision,
      scenario.operationRevision,
      scenario.status,
    );
    assert.equal(
      result.launch.operation.result.outcome,
      scenario.outcome,
      scenario.status,
    );
    assert.equal(result.session.document.launch, null, scenario.status);
    assert.equal(
      result.launch.operation.result.evidence.processIncarnationId,
      scenario.status === "not-started"
        ? null
        : "process-incarnation-001",
      scenario.status,
    );
    assert.equal(
      result.launch.operation.result.evidence.writerIncarnationId,
      scenario.status === "not-started"
        ? null
        : "writer-incarnation-001",
      scenario.status,
    );
    assert.equal(storage.providerCalls.length, 1, scenario.status);
  }
});

test("accepts an expired pre-dispatch launch cancellation replay", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const request = activationRequest(fixture);
  const replay = cancelledLaunchHandoff(fixture, request);
  const authority = authorityHarness({
    "finalize-activation": async () => replay,
    "read-activation": async () =>
      activationRead(fixture, "uncertain", request),
  });
  const storage = storageBackendHarness(async () => activationResult(request));
  const { coordinator } = createCoordinator(
    fixture,
    authority.authority,
    storage,
  );

  const result = await coordinator.reconcileRestoreAttachmentActivation(
    activationCandidate(fixture),
  );

  assert.equal(result.launch.operation.state, "committed");
  assert.equal(
    result.launch.operation.result.reason,
    WRITER_LAUNCH_PRE_DISPATCH_CANCELLATION_REASON,
  );
  assert.equal(result.session.document.activeOperation, null);
  assert.equal(
    result.session.document.lastOperation.operationId,
    result.launch.operation.operationId,
  );
});

test("accepts later current-session history for committed launch outcomes", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const request = activationRequest(fixture);
  const scenarios = [
    [
      "started",
      advanceCommittedLaunchCurrentSession(
        committedLaunchHandoff(fixture, request),
      ),
      "2026-08-05T00:06:00.000Z",
    ],
    [
      "cancelled",
      advanceCommittedLaunchCurrentSession(
        cancelledLaunchHandoff(fixture, request),
        { detached: true },
      ),
      "2026-08-05T00:14:00.000Z",
    ],
  ];

  for (const [scenario, replay, expectedUpdatedAt] of scenarios) {
    const authority = authorityHarness({
      "finalize-activation": async () => replay,
      "read-activation": async () =>
        activationRead(fixture, "uncertain", request),
    });
    const storage = storageBackendHarness(async () =>
      activationResult(request));
    const { coordinator } = createCoordinator(
      fixture,
      authority.authority,
      storage,
    );

    const result = await coordinator.reconcileRestoreAttachmentActivation(
      activationCandidate(fixture),
    );

    assert.equal(result.launch.operation.state, "committed", scenario);
    assert.equal(result.session.revision, replay.session.revision, scenario);
    assert.equal(result.session.updatedAt, expectedUpdatedAt, scenario);
  }
});

test("rejects later committed launch history with a different session identity", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const request = activationRequest(fixture);
  const replay = advanceCommittedLaunchCurrentSession(
    committedLaunchHandoff(fixture, request),
  );
  replay.session.document.storageRef.storageId = "forged-volume-001";
  let readCalls = 0;
  const authority = authorityHarness({
    "finalize-activation": async () => replay,
    "read-activation": async () => {
      readCalls += 1;
      return activationRead(fixture, "uncertain", request);
    },
  });
  const storage = storageBackendHarness(async () => activationResult(request));
  const { coordinator } = createCoordinator(
    fixture,
    authority.authority,
    storage,
  );

  await assert.rejects(
    coordinator.reconcileRestoreAttachmentActivation(
      activationCandidate(fixture),
    ),
    assertCoordinatorCode(
      "postgres_restore_activation_recovery_coordinator_outcome_uncertain",
    ),
  );

  assert.equal(readCalls, 1);
});

test("rejects committed writer evidence at the cancellation-only revision", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const request = activationRequest(fixture);
  const replay = committedLaunchHandoff(fixture, request);
  replay.launch.operation.revision = "1";
  synchronizeCommittedLaunchReceipt(replay);
  let readCalls = 0;
  const authority = authorityHarness({
    "finalize-activation": async () => replay,
    "read-activation": async () => {
      readCalls += 1;
      return activationRead(fixture, "uncertain", request);
    },
  });
  const storage = storageBackendHarness(async () => activationResult(request));
  const { coordinator } = createCoordinator(
    fixture,
    authority.authority,
    storage,
  );

  await assert.rejects(
    coordinator.reconcileRestoreAttachmentActivation(
      activationCandidate(fixture),
    ),
    assertCoordinatorCode(
      "postgres_restore_activation_recovery_coordinator_outcome_uncertain",
    ),
  );

  assert.equal(readCalls, 1);
});

test("rejects malformed or premature launch cancellation replays", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const request = activationRequest(fixture);
  const scenarios = [
    ["wrong reason", (receipt) => {
      receipt.launch.operation.result.reason =
        "caller-abandoned-before-dispatch";
    }],
    ["extra result field", (receipt) => {
      receipt.launch.operation.result.unexpected = true;
    }],
    ["before lease expiry", (receipt) => {
      const updatedAt = "2026-08-05T00:12:59.999Z";
      receipt.launch.operation.updatedAt = updatedAt;
      receipt.launch.operation.retiredAt = updatedAt;
      receipt.launch.reservation.updatedAt = updatedAt;
      receipt.launch.reservation.releasedAt = updatedAt;
    }],
    ["wrong revision", (receipt) => {
      receipt.launch.operation.revision = "2";
    }],
  ];

  for (const [scenario, mutate] of scenarios) {
    const forged = cancelledLaunchHandoff(fixture, request);
    mutate(forged);
    synchronizeCommittedLaunchReceipt(forged);
    let readCalls = 0;
    const authority = authorityHarness({
      "finalize-activation": async () => forged,
      "read-activation": async () => {
        readCalls += 1;
        return activationRead(fixture, "uncertain", request);
      },
    });
    const storage = storageBackendHarness(async () => activationResult(request));
    const { coordinator } = createCoordinator(
      fixture,
      authority.authority,
      storage,
    );

    await assert.rejects(
      coordinator.reconcileRestoreAttachmentActivation(
        activationCandidate(fixture),
      ),
      assertCoordinatorCode(
        "postgres_restore_activation_recovery_coordinator_outcome_uncertain",
      ),
      scenario,
    );

    assert.equal(readCalls, 1, scenario);
  }
});

test("rejects launch receipts whose updatedAt predates createdAt", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const request = activationRequest(fixture);
  const forgedUpdatedAt = "2026-08-05T00:03:59.000Z";
  const scenarios = [
    ["prepared", structuredClone(activationHandoff(fixture, request))],
    [
      "starting",
      activeLaunchHandoff(
        fixture,
        request,
        "starting",
        forgedUpdatedAt,
      ),
    ],
    [
      "uncertain",
      activeLaunchHandoff(
        fixture,
        request,
        "uncertain",
        forgedUpdatedAt,
      ),
    ],
  ];
  scenarios[0][1].launch.operation.updatedAt = forgedUpdatedAt;
  scenarios[0][1].launch.reservation.updatedAt = forgedUpdatedAt;
  scenarios[0][1].session.updatedAt = forgedUpdatedAt;

  for (const [scenario, forged] of scenarios) {
    const authority = authorityHarness({
      "finalize-activation": async () => forged,
      "read-activation": async () =>
        activationRead(fixture, "uncertain", request),
    });
    const storage = storageBackendHarness(async () => activationResult(request));
    const { coordinator } = createCoordinator(
      fixture,
      authority.authority,
      storage,
    );

    await assert.rejects(
      coordinator.reconcileRestoreAttachmentActivation(
        activationCandidate(fixture),
      ),
      assertCoordinatorCode(
        "postgres_restore_activation_recovery_coordinator_outcome_uncertain",
      ),
      scenario,
    );
  }
});

test("rejects launch handoffs that are only receipt-internally consistent", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const request = activationRequest(fixture);
  const prepared = activationResult(request);
  const scenarios = [
    ["attachment", (receipt) => {
      receipt.launch.operation.request.attachment.attachmentId =
        "forged-attachment-001";
      receipt.launch.operation.expectedSession.document.attachment
        .attachmentId = "forged-attachment-001";
      receipt.session.document.attachment.attachmentId =
        "forged-attachment-001";
    }],
    ["lease", (receipt) => {
      receipt.launch.operation.request.lease.leaseId = "forged-lease-001";
      receipt.launch.operation.expectedSession.document.lease.leaseId =
        "forged-lease-001";
      receipt.session.document.lease.leaseId = "forged-lease-001";
    }],
    ["generation", (receipt) => {
      receipt.launch.operation.request.generation.generationId =
        "forged-generation-001";
    }],
    ["measured-image", (receipt) => {
      receipt.launch.operation.request.measuredImage.runtimeIdentity
        .codexBinarySha256 = "b".repeat(64);
    }],
    ["supervisor", (receipt) => {
      receipt.launch.operation.request.supervisor.supervisorId =
        "forged-supervisor-001";
    }],
    ["expected-session", (receipt) => {
      receipt.launch.operation.expectedSession.document.attachment
        .attachmentId = "forged-attachment-001";
    }],
    ["reservation", (receipt) => {
      receipt.launch.reservation.reservationId = "forged-reservation-001";
      receipt.session.document.activeOperation.reservationId =
        "forged-reservation-001";
    }],
    ["active-pointer", (receipt) => {
      receipt.session.document.activeOperation.operationRevision = "1";
    }],
  ];

  for (const [scenario, mutate] of scenarios) {
    const forged = structuredClone(activationHandoff(fixture, request));
    mutate(forged);
    synchronizeLaunchRequestBindings(forged);
    const authority = authorityHarness({
      "finalize-activation": async () => forged,
      "read-activation": async () =>
        activationRead(fixture, "uncertain", request),
    });
    const storage = storageBackendHarness(async () => prepared);
    const { coordinator } = createCoordinator(
      fixture,
      authority.authority,
      storage,
    );

    await assert.rejects(
      coordinator.reconcileRestoreAttachmentActivation(
        activationCandidate(fixture),
      ),
      assertCoordinatorCode(
        "postgres_restore_activation_recovery_coordinator_outcome_uncertain",
      ),
      scenario,
    );

    assert.equal(storage.providerCalls.length, 1, scenario);
  }
});

test("does not launder a malformed handoff through committed readback", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const request = activationRequest(fixture);
  const prepared = activationResult(request);
  const malformed = structuredClone(activationHandoff(fixture, request));
  delete malformed.launch;
  let readCalls = 0;
  const authority = authorityHarness({
    "finalize-activation": async () => malformed,
    "read-activation": async () => {
      readCalls += 1;
      return activationRead(
        fixture,
        readCalls === 1 ? "uncertain" : "committed",
        request,
      );
    },
  });
  const storage = storageBackendHarness(async () => prepared);
  const { coordinator } = createCoordinator(
    fixture,
    authority.authority,
    storage,
  );

  await assert.rejects(
    coordinator.reconcileRestoreAttachmentActivation(
      activationCandidate(fixture),
    ),
    assertCoordinatorCode(
      "postgres_restore_activation_recovery_coordinator_outcome_uncertain",
    ),
  );

  assert.equal(readCalls, 1);
  assert.equal(storage.providerCalls.length, 1);
});

test("does not treat malformed immediate or fulfilled handoffs as acknowledgement loss", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const request = activationRequest(fixture);
  const scenarios = [
    [
      "immediate thenable",
      (calls) => ({
        then() {
          calls.then += 1;
        },
      }),
    ],
    [
      "fulfilled generator",
      (calls) =>
        Promise.resolve((function* invalidHandoff() {
          calls.body += 1;
          yield activationHandoff(fixture, request);
        })()),
    ],
  ];

  for (const [scenario, makeHandoff] of scenarios) {
    const calls = { body: 0, then: 0 };
    let readCalls = 0;
    const harness = authorityHarness({
      "read-activation": async () => {
        readCalls += 1;
        return activationRead(
          fixture,
          readCalls === 1 ? "uncertain" : "committed",
          request,
        );
      },
    });
    harness.authority
      .finalizeRestoreAttachmentActivationAndReserveWriterLaunchAttempt =
        function finalizeRestoreAttachmentActivation() {
          return makeHandoff(calls);
        };
    const storage = storageBackendHarness(async () =>
      activationResult(request));
    const { coordinator } = createCoordinator(
      fixture,
      harness.authority,
      storage,
    );

    await assert.rejects(
      coordinator.reconcileRestoreAttachmentActivation(
        activationCandidate(fixture),
      ),
      assertCoordinatorCode(
        "postgres_restore_activation_recovery_coordinator_outcome_uncertain",
      ),
      scenario,
    );

    assert.equal(readCalls, 1, scenario);
    assert.deepEqual(calls, { body: 0, then: 0 }, scenario);
  }
});

test("rejects forged activation operation bindings before provider work", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const request = activationRequest(fixture);
  const scenarios = [
    ["request digest", (receipt) => {
      const forged = "a".repeat(64);
      receipt.operation.requestSha256 = forged;
      receipt.reservation.requestSha256 = forged;
      receipt.session.document.activeOperation.requestSha256 = forged;
    }],
    ["reservation id", (receipt) => {
      const forged = "forged-reservation-001";
      receipt.reservation.reservationId = forged;
      receipt.session.document.activeOperation.reservationId = forged;
    }],
    ["reservation timestamp", (receipt) => {
      receipt.reservation.updatedAt = "2026-08-05T00:03:01.000Z";
    }],
    ["reordered expected snapshot", (receipt) => {
      receipt.operation.expectedSession = reversedDataObject(
        receipt.operation.expectedSession,
      );
      synchronizeForgedOperationDigest(receipt);
    }],
    ["reordered nested expected-session identity", (receipt) => {
      const manifest = receipt.operation.expectedSession.document.manifest;
      manifest.runtime = reversedDataObject(manifest.runtime);
      receipt.session.document.manifest = structuredClone(manifest);
      receipt.activationRequest.manifest = structuredClone(manifest);
      synchronizeForgedOperationDigest(receipt);
    }],
  ];

  for (const [scenario, mutate] of scenarios) {
    const forged = activationRead(fixture, "uncertain", request);
    mutate(forged);
    let finalizeCalls = 0;
    const authority = authorityHarness({
      "finalize-activation": async () => {
        finalizeCalls += 1;
        return activationHandoff(fixture, request);
      },
      "read-activation": async () => forged,
    });
    const storage = storageBackendHarness(async () =>
      activationResult(request));
    const { coordinator } = createCoordinator(
      fixture,
      authority.authority,
      storage,
    );

    await assert.rejects(
      coordinator.reconcileRestoreAttachmentActivation(
        activationCandidate(fixture),
      ),
      assertCoordinatorCode(
        "postgres_restore_activation_recovery_coordinator_outcome_uncertain",
      ),
      scenario,
    );

    assert.equal(finalizeCalls, 0, scenario);
    assert.equal(storage.providerCalls.length, 0, scenario);
  }
});

test("rejects activation operation timestamps that run backwards", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const request = activationRequest(fixture);

  for (const state of ["starting", "uncertain", "committed"]) {
    const forged = activationRead(fixture, state, request);
    const updatedAt = "2026-08-05T00:02:59.000Z";
    forged.operation.updatedAt = updatedAt;
    forged.reservation.updatedAt = updatedAt;
    if (state === "committed") {
      forged.operation.retiredAt = updatedAt;
      forged.reservation.releasedAt = updatedAt;
    } else {
      forged.session.updatedAt = updatedAt;
    }
    const authority = authorityHarness({
      "read-activation": async () => forged,
    });
    const storage = storageBackendHarness(async () =>
      activationResult(request));
    const { coordinator } = createCoordinator(
      fixture,
      authority.authority,
      storage,
    );

    await assert.rejects(
      coordinator.reconcileRestoreAttachmentActivation(
        activationCandidate(fixture),
      ),
      assertCoordinatorCode(
        "postgres_restore_activation_recovery_coordinator_outcome_uncertain",
      ),
      state,
    );

    assert.equal(storage.providerCalls.length, 0, state);
  }
});

test("accepts an activation commit whose wall clock precedes the prior read", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const request = activationRequest(fixture);
  const prior = activationRead(fixture, "uncertain", request);
  prior.operation.updatedAt = "2026-08-05T00:05:00.000Z";
  prior.reservation.updatedAt = prior.operation.updatedAt;
  prior.session.updatedAt = prior.operation.updatedAt;
  const authority = authorityHarness({
    "finalize-activation": async () =>
      activationHandoff(fixture, request),
    "read-activation": async () => prior,
  });
  const storage = storageBackendHarness(async () =>
    activationResult(request));
  const { coordinator } = createCoordinator(
    fixture,
    authority.authority,
    storage,
  );

  const result = await coordinator.reconcileRestoreAttachmentActivation(
    activationCandidate(fixture),
  );

  assert.equal(result.activation.operation.updatedAt, "2026-08-05T00:04:00.000Z");
  assert.equal(result.launch.operation.state, "prepared");
  assert.equal(storage.providerCalls.length, 1);
});

test("rejects committed activation history below its terminal revision", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const request = activationRequest(fixture);
  const forged = activationRead(fixture, "committed", request);
  forged.session.revision = "23";
  const authority = authorityHarness({
    "read-activation": async () => forged,
  });
  const storage = storageBackendHarness(async () => activationResult(request));
  const { coordinator } = createCoordinator(
    fixture,
    authority.authority,
    storage,
  );

  await assert.rejects(
    coordinator.reconcileRestoreAttachmentActivation(
      activationCandidate(fixture),
    ),
    assertCoordinatorCode(
      "postgres_restore_activation_recovery_coordinator_outcome_uncertain",
    ),
  );

  assert.equal(storage.providerCalls.length, 0);
});

test("rejects a committed activation at the pre-terminal revision", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const request = activationRequest(fixture);
  const forged = activationRead(fixture, "committed", request);
  forged.operation.revision = "1";
  forged.session.revision = "22";
  const authority = authorityHarness({
    "read-activation": async () => forged,
  });
  const storage = storageBackendHarness(async () => activationResult(request));
  const { coordinator } = createCoordinator(
    fixture,
    authority.authority,
    storage,
  );

  await assert.rejects(
    coordinator.reconcileRestoreAttachmentActivation(
      activationCandidate(fixture),
    ),
    assertCoordinatorCode(
      "postgres_restore_activation_recovery_coordinator_outcome_uncertain",
    ),
  );

  assert.equal(storage.providerCalls.length, 0);
});

test("rejects inconsistent later current-session history for a committed activation", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const request = activationRequest(fixture);
  const forged = activationRead(fixture, "committed", request);
  forged.session.document.lastOperation.operationRevision = "1";
  let finalizeCalls = 0;
  const authority = authorityHarness({
    "finalize-activation": async () => {
      finalizeCalls += 1;
      return activationHandoff(fixture, request);
    },
    "read-activation": async () => forged,
  });
  const storage = storageBackendHarness(async () =>
    activationResult(request));
  const { coordinator } = createCoordinator(
    fixture,
    authority.authority,
    storage,
  );

  await assert.rejects(
    coordinator.reconcileRestoreAttachmentActivation(
      activationCandidate(fixture),
    ),
    assertCoordinatorCode(
      "postgres_restore_activation_recovery_coordinator_outcome_uncertain",
    ),
  );

  assert.equal(finalizeCalls, 0);
  assert.equal(storage.providerCalls.length, 0);
});

test("replays the atomic finalizer for an already committed activation", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const request = activationRequest(fixture);
  const replay = structuredClone(activationHandoff(fixture, request));
  replay.activation.finalized = false;
  let finalizeCalls = 0;
  const authority = authorityHarness({
    "finalize-activation": async (input) => {
      finalizeCalls += 1;
      assert.equal(input.expectedOperationRevision, "2");
      return replay;
    },
    "read-activation": async () =>
      activationRead(fixture, "committed", request),
  });
  const storage = storageBackendHarness(async () => {
    throw new Error("an already committed activation must not reattach");
  });
  const { coordinator } = createCoordinator(
    fixture,
    authority.authority,
    storage,
  );

  const result = await coordinator.reconcileRestoreAttachmentActivation(
    activationCandidate(fixture),
  );

  assert.equal(result.activation.finalized, false);
  assert.equal(result.launch.operation.state, "prepared");
  assert.equal(finalizeCalls, 1);
  assert.equal(storage.providerCalls.length, 0);
});

test("replays a revision-two activation committed during uncertain-transition acknowledgement loss", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const request = activationRequest(fixture);
  const replay = structuredClone(activationHandoff(fixture, request, "2"));
  replay.activation.finalized = false;
  let readCalls = 0;
  const authority = authorityHarness({
    "finalize-activation": async (input) => {
      assert.equal(input.expectedOperationRevision, "1");
      return replay;
    },
    "mark-uncertain": async (input) => {
      assert.equal(input.expectedOperationRevision, "1");
      throw new Error("uncertain transition acknowledgement lost");
    },
    "read-activation": async () => {
      readCalls += 1;
      return activationRead(
        fixture,
        readCalls === 1 ? "starting" : "committed",
        request,
        null,
        readCalls === 1 ? null : "2",
      );
    },
  });
  const storage = storageBackendHarness(async () => {
    throw new Error("a committed activation readback must not reattach");
  });
  const { coordinator } = createCoordinator(
    fixture,
    authority.authority,
    storage,
  );

  const result = await coordinator.reconcileRestoreAttachmentActivation(
    activationCandidate(fixture),
  );

  assert.equal(result.activation.operation.revision, "2");
  assert.equal(result.launch.operation.state, "prepared");
  assert.deepEqual(
    authority.calls.map(([name]) => name),
    [
      "read-activation",
      "mark-uncertain",
      "read-activation",
      "finalize-activation",
    ],
  );
  assert.equal(storage.providerCalls.length, 0);
});

test("rejects finalized true when replaying an already committed activation", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const request = activationRequest(fixture);
  const authority = authorityHarness({
    "finalize-activation": async () => activationHandoff(fixture, request),
    "read-activation": async () =>
      activationRead(fixture, "committed", request),
  });
  const storage = storageBackendHarness(async () => {
    throw new Error("an already committed activation must not reattach");
  });
  const { coordinator } = createCoordinator(
    fixture,
    authority.authority,
    storage,
  );

  await assert.rejects(
    coordinator.reconcileRestoreAttachmentActivation(
      activationCandidate(fixture),
    ),
    assertCoordinatorCode(
      "postgres_restore_activation_recovery_coordinator_outcome_uncertain",
    ),
  );

  assert.equal(storage.providerCalls.length, 0);
});

test("recovers activation finalization acknowledgement loss without a second attach", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const request = activationRequest(fixture);
  const prepared = activationResult(request);
  const replay = structuredClone(activationHandoff(fixture, request));
  replay.activation.finalized = false;
  let finalizeCalls = 0;
  const authority = authorityHarness({
    "finalize-activation": async () => {
      finalizeCalls += 1;
      if (finalizeCalls === 1) {
        throw new Error("activation finalization acknowledgement lost");
      }
      return replay;
    },
    "read-activation": async () =>
      activationRead(fixture, "uncertain", request),
  });
  const storage = storageBackendHarness(async () => prepared);
  const { coordinator } = createCoordinator(
    fixture,
    authority.authority,
    storage,
  );

  const result = await coordinator.reconcileRestoreAttachmentActivation(
    activationCandidate(fixture),
  );

  assert.equal(result.activation.operation.state, "committed");
  assert.equal(result.activation.finalized, false);
  assert.equal(result.launch.operation.state, "prepared");
  assert.equal(finalizeCalls, 2);
  assert.equal(storage.providerCalls.length, 1);
  assert.deepEqual(
    authority.calls.map(([name]) => name),
    [
      "read-activation",
      "finalize-activation",
      "finalize-activation",
    ],
  );
  assert.deepEqual(storage.forbiddenCalls, []);
  assert.deepEqual(fixture.transitionGuard.calls, []);
});

test("fails after two uncertain activation finalization attempts without a second attach", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const request = activationRequest(fixture);
  let finalizeCalls = 0;
  let readCalls = 0;
  const authority = authorityHarness({
    "finalize-activation": async () => {
      finalizeCalls += 1;
      throw new Error("activation finalization outcome remains uncertain");
    },
    "read-activation": async () => {
      readCalls += 1;
      return activationRead(fixture, "uncertain", request);
    },
  });
  const storage = storageBackendHarness(async () =>
    activationResult(request));
  const { coordinator } = createCoordinator(
    fixture,
    authority.authority,
    storage,
  );

  await assert.rejects(
    coordinator.reconcileRestoreAttachmentActivation(
      activationCandidate(fixture),
    ),
    assertCoordinatorCode(
      "postgres_restore_activation_recovery_coordinator_outcome_uncertain",
    ),
  );

  assert.equal(readCalls, 1);
  assert.equal(finalizeCalls, 2);
  assert.equal(storage.providerCalls.length, 1);
  assert.deepEqual(storage.forbiddenCalls, []);
  assert.deepEqual(fixture.transitionGuard.calls, []);
});

test("rejects a provider Promise subclass without invoking its overridden then", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const request = activationRequest(fixture);
  const prepared = activationResult(request);
  let thenCalls = 0;
  let finalizeCalls = 0;
  class HostileProviderPromise extends Promise {
    then(...args) {
      thenCalls += 1;
      return super.then(...args);
    }
  }
  const hostileResult = new HostileProviderPromise((resolve) => {
    resolve(prepared);
  });
  const authority = authorityHarness({
    "finalize-activation": async () => {
      finalizeCalls += 1;
      throw new Error("finalization must not run for a hostile promise");
    },
    "read-activation": async () =>
      activationRead(fixture, "uncertain", request),
  });
  const storage = storageBackendHarness(() => hostileResult, {
    rawReconciliation: true,
  });
  const { coordinator } = createCoordinator(
    fixture,
    authority.authority,
    storage,
  );

  await assert.rejects(
    coordinator.reconcileRestoreAttachmentActivation(
      activationCandidate(fixture),
    ),
    assertCoordinatorCode(
      "postgres_restore_activation_recovery_coordinator_outcome_uncertain",
    ),
  );

  assert.equal(thenCalls, 0);
  assert.equal(finalizeCalls, 0);
  assert.equal(storage.providerCalls.length, 1);
  assert.deepEqual(
    authority.calls.map(([name]) => name),
    ["read-activation"],
  );
  assert.deepEqual(storage.forbiddenCalls, []);
  assert.deepEqual(fixture.transitionGuard.calls, []);
});

test("maps provider-created coordinator-error Promise rejections to uncertainty", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const request = activationRequest(fixture);
  const forgedReason =
    new PostgresRestoreActivationRecoveryCoordinatorError(
      "invalid_postgres_restore_activation_recovery_coordinator_request",
    );
  const authority = authorityHarness({
    "finalize-activation": async () => {
      throw new Error("rejected provider result must not finalize");
    },
    "read-activation": async () =>
      activationRead(fixture, "uncertain", request),
  });
  const storage = storageBackendHarness(() => Promise.reject(forgedReason), {
    rawReconciliation: true,
  });
  const { coordinator } = createCoordinator(
    fixture,
    authority.authority,
    storage,
  );

  await assert.rejects(
    coordinator.reconcileRestoreAttachmentActivation(
      activationCandidate(fixture),
    ),
    (error) => {
      assert.notEqual(error, forgedReason);
      return assertCoordinatorCode(
        "postgres_restore_activation_recovery_coordinator_outcome_uncertain",
      )(error);
    },
  );
});

test("rejects an ordinary provider thenable before invoking then", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const request = activationRequest(fixture);
  const prepared = activationResult(request);
  let thenCalls = 0;
  let finalizeCalls = 0;
  const thenable = {
    then(resolve) {
      thenCalls += 1;
      resolve(prepared);
    },
  };
  const authority = authorityHarness({
    "finalize-activation": async () => {
      finalizeCalls += 1;
      throw new Error("ordinary thenable must not reach finalization");
    },
    "read-activation": async () =>
      activationRead(fixture, "uncertain", request),
  });
  const storage = storageBackendHarness(() => thenable, {
    rawReconciliation: true,
  });
  const { coordinator } = createCoordinator(
    fixture,
    authority.authority,
    storage,
  );

  await assert.rejects(
    coordinator.reconcileRestoreAttachmentActivation(
      activationCandidate(fixture),
    ),
    assertCoordinatorCode(
      "postgres_restore_activation_recovery_coordinator_outcome_uncertain",
    ),
  );

  assert.equal(thenCalls, 0);
  assert.equal(finalizeCalls, 0);
  assert.equal(storage.providerCalls.length, 1);
});

test("rejects a top-level provider Proxy without invoking a trap", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const request = activationRequest(fixture);
  const trapCalls = {
    get: 0,
    getOwnPropertyDescriptor: 0,
    getPrototypeOf: 0,
  };
  let finalizeCalls = 0;
  const proxied = new Proxy(activationResult(request), {
    get(target, key, receiver) {
      trapCalls.get += 1;
      return Reflect.get(target, key, receiver);
    },
    getOwnPropertyDescriptor(target, key) {
      trapCalls.getOwnPropertyDescriptor += 1;
      return Reflect.getOwnPropertyDescriptor(target, key);
    },
    getPrototypeOf(target) {
      trapCalls.getPrototypeOf += 1;
      return Reflect.getPrototypeOf(target);
    },
  });
  const authority = authorityHarness({
    "finalize-activation": async () => {
      finalizeCalls += 1;
      throw new Error("provider Proxy must not reach finalization");
    },
    "read-activation": async () =>
      activationRead(fixture, "uncertain", request),
  });
  const storage = storageBackendHarness(() => proxied, {
    rawReconciliation: true,
  });
  const { coordinator } = createCoordinator(
    fixture,
    authority.authority,
    storage,
  );

  await assert.rejects(
    coordinator.reconcileRestoreAttachmentActivation(
      activationCandidate(fixture),
    ),
    assertCoordinatorCode(
      "postgres_restore_activation_recovery_coordinator_outcome_uncertain",
    ),
  );

  assert.deepEqual(trapCalls, {
    get: 0,
    getOwnPropertyDescriptor: 0,
    getPrototypeOf: 0,
  });
  assert.equal(finalizeCalls, 0);
  assert.equal(storage.providerCalls.length, 1);
});

test("rejects a never-completing ordinary thenable without hanging", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const request = activationRequest(fixture);
  let thenCalls = 0;
  let finalizeCalls = 0;
  const neverCompletes = {
    then() {
      thenCalls += 1;
    },
  };
  const authority = authorityHarness({
    "finalize-activation": async () => {
      finalizeCalls += 1;
      throw new Error("never-completing thenable must not finalize");
    },
    "read-activation": async () =>
      activationRead(fixture, "uncertain", request),
  });
  const storage = storageBackendHarness(() => neverCompletes, {
    rawReconciliation: true,
  });
  const { coordinator } = createCoordinator(
    fixture,
    authority.authority,
    storage,
  );

  await assertPromptCoordinatorRejection(
    coordinator.reconcileRestoreAttachmentActivation(
      activationCandidate(fixture),
    ),
    "postgres_restore_activation_recovery_coordinator_outcome_uncertain",
  );

  assert.equal(thenCalls, 0);
  assert.equal(finalizeCalls, 0);
  assert.equal(storage.providerCalls.length, 1);
});

test("rejects direct hostile async-shaped provider values without executing user code", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const request = activationRequest(fixture);
  const prepared = activationResult(request);
  const scenarios = [
    ["own then accessor", (calls) => {
      const value = {};
      Object.defineProperty(value, "then", {
        get() {
          calls.getter += 1;
          return () => {
            calls.then += 1;
          };
        },
      });
      return value;
    }],
    ["inherited then accessor", (calls) => {
      const prototype = {};
      Object.defineProperty(prototype, "then", {
        get() {
          calls.getter += 1;
          return () => {
            calls.then += 1;
          };
        },
      });
      return Object.create(prototype);
    }],
    ["own callable then", (calls) => ({
      then() {
        calls.then += 1;
      },
    })],
    ["own non-callable then", () => ({ then: 1 })],
    ["inherited callable then", (calls) =>
      Object.create({
        then() {
          calls.then += 1;
        },
      })],
    ["inherited non-callable then", () =>
      Object.create({ then: 1 })],
    ["generator with hostile then", (calls) => {
      function* hostileGenerator() {
        calls.body += 1;
        yield prepared;
      }
      const value = hostileGenerator();
      value.then = () => {
        calls.then += 1;
      };
      return value;
    }],
    ["async generator with hostile then", (calls) => {
      async function* hostileAsyncGenerator() {
        calls.body += 1;
        yield prepared;
      }
      const value = hostileAsyncGenerator();
      value.then = () => {
        calls.then += 1;
      };
      return value;
    }],
    ["Promise constructor accessor", (calls) => {
      const value = Promise.resolve(prepared);
      Object.defineProperty(value, "constructor", {
        get() {
          calls.getter += 1;
          return Promise;
        },
      });
      return value;
    }],
    ["proxy prototype", (calls) => {
      const prototype = new Proxy({}, {
        get() {
          calls.trap += 1;
        },
        getOwnPropertyDescriptor() {
          calls.trap += 1;
        },
        getPrototypeOf() {
          calls.trap += 1;
        },
      });
      return Object.create(prototype);
    }],
    ["Promise proxy prototype", (calls) => {
      const prototype = new Proxy({}, {
        get() {
          calls.trap += 1;
        },
        getOwnPropertyDescriptor() {
          calls.trap += 1;
        },
        getPrototypeOf() {
          calls.trap += 1;
        },
      });
      const value = Promise.resolve(prepared);
      Object.setPrototypeOf(value, prototype);
      return value;
    }],
    ["Promise prototype reset", () => {
      const value = Promise.resolve(prepared);
      Object.setPrototypeOf(value, null);
      return value;
    }],
    ["revoked top-level Proxy", (calls) => {
      const revocable = Proxy.revocable(prepared, {
        get() {
          calls.trap += 1;
        },
        getOwnPropertyDescriptor() {
          calls.trap += 1;
        },
        getPrototypeOf() {
          calls.trap += 1;
        },
      });
      revocable.revoke();
      return revocable.proxy;
    }],
  ];

  for (const [scenario, makeValue] of scenarios) {
    const calls = { body: 0, getter: 0, then: 0, trap: 0 };
    let finalizeCalls = 0;
    const value = makeValue(calls);
    const authority = authorityHarness({
      "finalize-activation": async () => {
        finalizeCalls += 1;
        throw new Error("hostile provider value must not finalize");
      },
      "read-activation": async () =>
        activationRead(fixture, "uncertain", request),
    });
    const storage = storageBackendHarness(() => value, {
      rawReconciliation: true,
    });
    const { coordinator } = createCoordinator(
      fixture,
      authority.authority,
      storage,
    );

    await assert.rejects(
      coordinator.reconcileRestoreAttachmentActivation(
        activationCandidate(fixture),
      ),
      assertCoordinatorCode(
        "postgres_restore_activation_recovery_coordinator_outcome_uncertain",
      ),
      scenario,
    );

    assert.deepEqual(
      calls,
      { body: 0, getter: 0, then: 0, trap: 0 },
      scenario,
    );
    assert.equal(finalizeCalls, 0, scenario);
    assert.equal(storage.providerCalls.length, 1, scenario);
  }
});

test("accepts native Promises without reading an own hostile then accessor", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const request = activationRequest(fixture);

  for (const species of ["intrinsic", "safe holder"]) {
    const prepared = activationResult(request);
    let thenGetterCalls = 0;
    let thenCalls = 0;
    let pending = Promise.resolve(
      activationReconciliation("applied", prepared),
    );
    Object.defineProperty(pending, "then", {
      configurable: true,
      get() {
        thenGetterCalls += 1;
        return () => {
          thenCalls += 1;
        };
      },
    });
    if (species === "safe holder") {
      pending = withSafePromiseSpecies(pending);
    }
    const authority = authorityHarness({
      "finalize-activation": async () =>
        activationHandoff(fixture, request),
      "read-activation": async () =>
        activationRead(fixture, "uncertain", request),
    });
    const storage = storageBackendHarness(() => pending, {
      rawReconciliation: true,
    });
    const { coordinator } = createCoordinator(
      fixture,
      authority.authority,
      storage,
    );

    const result = await coordinator.reconcileRestoreAttachmentActivation(
      activationCandidate(fixture),
    );

    assert.equal(result.launch.operation.state, "prepared", species);
    assert.equal(thenGetterCalls, 0, species);
    assert.equal(thenCalls, 0, species);
  }
});

test("rejects safe-species fulfillment mutated into a thenable without invoking it", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const request = activationRequest(fixture);
  const prepared = activationResult(request);
  let thenGetterCalls = 0;
  let thenCalls = 0;
  let finalizeCalls = 0;
  const pending = withSafePromiseSpecies(Promise.resolve(prepared));
  const authority = authorityHarness({
    "finalize-activation": async () => {
      finalizeCalls += 1;
      return activationHandoff(fixture, request);
    },
    "read-activation": async () =>
      activationRead(fixture, "uncertain", request),
  });
  const storage = storageBackendHarness(
    () => {
      queueMicrotask(() => {
        Object.defineProperty(prepared, "then", {
          configurable: true,
          get() {
            thenGetterCalls += 1;
            return () => {
              thenCalls += 1;
            };
          },
        });
      });
      return pending;
    },
    { rawReconciliation: true },
  );
  const { coordinator } = createCoordinator(
    fixture,
    authority.authority,
    storage,
  );

  await assert.rejects(
    coordinator.reconcileRestoreAttachmentActivation(
      activationCandidate(fixture),
    ),
    assertCoordinatorCode(
      "postgres_restore_activation_recovery_coordinator_outcome_uncertain",
    ),
  );

  assert.equal(thenGetterCalls, 0);
  assert.equal(thenCalls, 0);
  assert.equal(finalizeCalls, 0);
});

test("rejects a Promise smuggled through outer fulfillment before reading its then", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const request = activationRequest(fixture);
  const inner = Promise.resolve(activationResult(request));
  Object.setPrototypeOf(inner, null);
  let resolveOuter;
  const outer = new Promise((resolve) => {
    resolveOuter = resolve;
  });
  resolveOuter(inner);
  Object.setPrototypeOf(inner, Promise.prototype);
  let thenGetterCalls = 0;
  let thenCalls = 0;
  Object.defineProperty(inner, "then", {
    configurable: true,
    get() {
      thenGetterCalls += 1;
      return () => {
        thenCalls += 1;
      };
    },
  });
  let finalizeCalls = 0;
  const authority = authorityHarness({
    "finalize-activation": async () => {
      finalizeCalls += 1;
      return activationHandoff(fixture, request);
    },
    "read-activation": async () =>
      activationRead(fixture, "uncertain", request),
  });
  const storage = storageBackendHarness(() => outer, {
    rawReconciliation: true,
  });
  const { coordinator } = createCoordinator(
    fixture,
    authority.authority,
    storage,
  );

  await assert.rejects(
    coordinator.reconcileRestoreAttachmentActivation(
      activationCandidate(fixture),
    ),
    assertCoordinatorCode(
      "postgres_restore_activation_recovery_coordinator_outcome_uncertain",
    ),
  );

  assert.equal(thenGetterCalls, 0);
  assert.equal(thenCalls, 0);
  assert.equal(finalizeCalls, 0);
});

test("maps safe-species Promise rejection to a fresh uncertainty error", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const request = activationRequest(fixture);
  const forgedReason =
    new PostgresRestoreActivationRecoveryCoordinatorError(
      "invalid_postgres_restore_activation_recovery_coordinator_request",
    );
  const authority = authorityHarness({
    "read-activation": async () =>
      activationRead(fixture, "uncertain", request),
  });
  const storage = storageBackendHarness(
    () => withSafePromiseSpecies(Promise.reject(forgedReason)),
    { rawReconciliation: true },
  );
  const { coordinator } = createCoordinator(
    fixture,
    authority.authority,
    storage,
  );

  await assert.rejects(
    coordinator.reconcileRestoreAttachmentActivation(
      activationCandidate(fixture),
    ),
    (error) => {
      assert.notEqual(error, forgedReason);
      return assertCoordinatorCode(
        "postgres_restore_activation_recovery_coordinator_outcome_uncertain",
      )(error);
    },
  );
});

test("rejects executable and incomplete authority receipts before provider work", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const request = activationRequest(fixture);
  const scenarios = ["accessor", "proxy", "incomplete", "cross-field"];

  for (let index = 0; index < scenarios.length; index += 1) {
    const scenario = scenarios[index];
    let trapCalls = 0;
    let finalizeCalls = 0;
    const receiptRequest = structuredClone(request);
    if (scenario === "cross-field") {
      receiptRequest.lease.holderId = "forged-activation-host-001";
      receiptRequest.mutationRequest.holderId =
        "forged-activation-host-001";
    }
    const receipt = activationRead(
      fixture,
      "uncertain",
      receiptRequest,
    );
    if (scenario === "accessor") {
      Object.defineProperty(receipt.operation, "request", {
        configurable: true,
        enumerable: true,
        get() {
          trapCalls += 1;
          throw new Error("authority request getter must not run");
        },
      });
    } else if (scenario === "proxy") {
      receipt.operation = new Proxy(receipt.operation, {
        get(target, key, receiver) {
          trapCalls += 1;
          return Reflect.get(target, key, receiver);
        },
      });
    } else if (scenario === "incomplete") {
      delete receipt.reservation;
    }
    const authority = authorityHarness({
      "finalize-activation": async () => {
        finalizeCalls += 1;
        throw new Error("malformed receipt must not reach finalization");
      },
      "read-activation": async () => receipt,
    });
    const storage = storageBackendHarness(async () => {
      throw new Error("malformed receipt must not reach provider attach");
    });
    const { coordinator } = createCoordinator(
      fixture,
      authority.authority,
      storage,
    );

    await assert.rejects(
      coordinator.reconcileRestoreAttachmentActivation(
        activationCandidate(fixture),
      ),
      assertCoordinatorCode(
        "postgres_restore_activation_recovery_coordinator_outcome_uncertain",
      ),
    );

    assert.equal(trapCalls, 0, scenario);
    assert.equal(finalizeCalls, 0, scenario);
    assert.equal(storage.providerCalls.length, 0, scenario);
    assert.deepEqual(storage.forbiddenCalls, [], scenario);
  }
});

test("rejects a conflicting activation handoff returned by the acknowledgement-loss retry", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const request = activationRequest(fixture);
  const prepared = activationResult(request);
  const conflicting = structuredClone(activationHandoff(fixture, request));
  conflicting.activation.operation.result.activationResult.attachment.proofId =
    "proof-attach-conflict";
  conflicting.activation.operation.result.activationResult.mutationResult.proofId =
    "proof-attach-conflict";
  let finalizeCalls = 0;
  const authority = authorityHarness({
    "finalize-activation": async () => {
      finalizeCalls += 1;
      if (finalizeCalls === 1) {
        throw new Error("activation finalization acknowledgement lost");
      }
      return conflicting;
    },
    "read-activation": async () =>
      activationRead(fixture, "uncertain", request),
  });
  const storage = storageBackendHarness(async () => prepared);
  const { coordinator } = createCoordinator(
    fixture,
    authority.authority,
    storage,
  );

  await assert.rejects(
    coordinator.reconcileRestoreAttachmentActivation(
      activationCandidate(fixture),
    ),
    assertCoordinatorCode(
      "postgres_restore_activation_recovery_coordinator_outcome_uncertain",
    ),
  );

  assert.equal(finalizeCalls, 2);
  assert.equal(storage.providerCalls.length, 1);
  assert.deepEqual(storage.forbiddenCalls, []);
  assert.deepEqual(fixture.transitionGuard.calls, []);
});

test("rejects a tampered committed destination before provider attach or finalization", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  await writeFile(
    join(fixture.destinationDirectory, "workspace", "README.md"),
    "tampered\n",
    { mode: 0o640 },
  );
  const request = activationRequest(fixture);
  const authority = authorityHarness({
    "finalize-activation": async () => {
      throw new Error("finalization must not run for a tampered destination");
    },
    "read-activation": async () =>
      activationRead(fixture, "uncertain", request),
  });
  const storage = storageBackendHarness(async () => {
    throw new Error("provider must not attach a tampered destination");
  });
  const { coordinator } = createCoordinator(
    fixture,
    authority.authority,
    storage,
  );

  await assert.rejects(
    coordinator.reconcileRestoreAttachmentActivation(
      activationCandidate(fixture),
    ),
    assertCoordinatorCode(
      "postgres_restore_activation_recovery_coordinator_outcome_uncertain",
    ),
  );

  assert.deepEqual(
    authority.calls.map(([name]) => name),
    ["read-activation"],
  );
  assert.deepEqual(storage.providerCalls, []);
  assert.deepEqual(storage.forbiddenCalls, []);
  assert.deepEqual(fixture.transitionGuard.calls, []);
});
