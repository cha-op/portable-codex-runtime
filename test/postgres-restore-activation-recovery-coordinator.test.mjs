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

import { FilesystemOperationJournal } from "../src/filesystem-operation-journal.mjs";
import { PostgresOperationGuard } from "../src/postgres-operation-guard.mjs";
import {
  PostgresRestoreActivationRecoveryCoordinatorError,
  createPostgresRestoreActivationRecoveryCoordinator,
} from "../src/postgres-restore-activation-recovery-coordinator.mjs";
import {
  RESTORE_ATTACHMENT_ACTIVATION_OPERATION_KIND,
  RESTORE_DESTINATION_GENERATION_DOCUMENT_CONTRACT_VERSION,
  RESTORE_DESTINATION_GENERATION_OPERATION_KIND,
  WRITER_LAUNCH_ATTEMPT_OPERATION_KIND,
  createRestoreAttachmentActivationOperationRequest,
} from "../src/postgres-session-authority.mjs";
import { createSessionManifest } from "../src/session-storage-contracts.mjs";
import { StoppedDirectoryPublication } from "../src/stopped-directory-publication.mjs";

const SESSION_ID = "019f2100-0000-7000-8000-000000000001";
const THREAD_ID = "019f2100-0000-7000-8000-000000000002";
const IMAGE_DIGEST = `sha256:${"a".repeat(64)}`;
const BACKEND_ID = "single-attach-test";
const STORAGE_ID = "volume-001";
const CHECKPOINT_ID = "checkpoint-001";
const ARTIFACT_ID = "artifact-001";
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
    storageId: STORAGE_ID,
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

function checkpoint() {
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
    storageId: STORAGE_ID,
  };
}

function mutationResult(request) {
  return {
    ...request,
    proofId: `proof-${request.operation}-001`,
    status:
      request.operation === "checkpoint"
        ? "checkpoint-created"
        : request.operation === "restore"
          ? "restored"
          : "attached",
  };
}

function publicationResult(request) {
  return {
    checkpoint: checkpoint(),
    mutation: mutationResult(request),
  };
}

function coordinatorBinding(request) {
  return {
    backendId: BACKEND_ID,
    operation: "restore",
    operationId: request.operationId,
    request,
    sessionId: SESSION_ID,
    storageId: STORAGE_ID,
  };
}

async function createPublishedRestoreFixture(t) {
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
  const captureRequest = mutationRequest(
    "checkpoint",
    CAPTURE_OPERATION_ID,
    {
      fencingEpoch: "11",
      holderId: "capture-host-001",
      leaseId: "capture-lease-001",
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
      storageId: STORAGE_ID,
    },
    operationId: CAPTURE_OPERATION_ID,
    request: captureRequest,
    result: publicationResult(captureRequest),
    sourceDirectory,
    sourceOwnedRoot,
  });
  const artifactProof = {
    artifactManifestDigest: capture.materialization.artifactManifestDigest,
    captureOperationId: CAPTURE_OPERATION_ID,
    modeledDigest: capture.materialization.modeledDigest,
  };
  const restoreRequest = mutationRequest("restore", RESTORE_OPERATION_ID, {
    fencingEpoch: "12",
    holderId: "restore-host-001",
    leaseId: "restore-lease-001",
  });
  const binding = coordinatorBinding(restoreRequest);
  const restoreResult = publicationResult(restoreRequest);
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

  async query(query) {
    const text = typeof query === "string" ? query : query.text;
    if (text === "DISCARD ALL") {
      this.held = false;
      return { command: "DISCARD", rows: [] };
    }
    if (text.includes("pg_try_advisory_lock")) {
      this.held = true;
      return {
        command: "SELECT",
        rows: [{ acquired: true, backend_pid: this.pid }],
      };
    }
    if (text.includes("FROM pg_catalog.pg_locks")) {
      return {
        command: "SELECT",
        rows: [{ backend_pid: this.pid, lock_held: this.held }],
      };
    }
    if (text.includes("pg_advisory_unlock")) {
      const unlocked = this.held;
      this.held = false;
      return {
        command: "SELECT",
        rows: [{ backend_pid: this.pid, unlocked }],
      };
    }
    throw new Error(`unexpected guard query: ${text}`);
  }

  async release() {}
}

class GuardPool {
  constructor() {
    this.connectCalls = 0;
  }

  async connect() {
    this.connectCalls += 1;
    return new GuardClient(1000 + this.connectCalls);
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
  const lease = {
    contractVersion: 1,
    expiresAt: "2026-08-05T00:13:00.000Z",
    fencingEpoch: "13",
    holderId: "activation-host-001",
    leaseId: ACTIVATION_LEASE_ID,
    sessionId: SESSION_ID,
  };
  return {
    contractVersion: 1,
    lease,
    manifest: sessionManifest(),
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
    storageRef: {
      backendId: BACKEND_ID,
      contractVersion: 1,
      sessionId: SESSION_ID,
      storageId: STORAGE_ID,
    },
  };
}

function activationResult(request) {
  const mutation = mutationResult(request.mutationRequest);
  return {
    attachment: {
      attachmentId: ATTACHMENT_ID,
      backendId: BACKEND_ID,
      contractVersion: 1,
      fencingEpoch: request.lease.fencingEpoch,
      holderId: request.lease.holderId,
      kind: "directory",
      leaseId: request.lease.leaseId,
      mode: "read-write",
      operationId: request.mutationRequest.operationId,
      proofId: mutation.proofId,
      rootPath: request.publication.root.rootPath,
      sessionId: SESSION_ID,
      storageId: STORAGE_ID,
    },
    contractVersion: 1,
    mutationResult: mutation,
    publication: structuredClone(request.publication),
  };
}

function committedGeneration(fixture) {
  return {
    binding: fixture.binding,
    checkpointId: CHECKPOINT_ID,
    claimedAt: "2026-08-05T00:01:00.000Z",
    committedAt: "2026-08-05T00:02:00.000Z",
    document: {
      artifactProof: fixture.artifactProof,
      contractVersion:
        RESTORE_DESTINATION_GENERATION_DOCUMENT_CONTRACT_VERSION,
      materialization: fixture.restore.materialization,
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
    activeOperation: null,
    attachment: null,
    backendCapabilities: {
      atomicPointInTimeCheckpoint: true,
      exclusiveWriterAttachment: true,
      fencing: "epoch-enforced",
      normalDirectoryAttachment: true,
    },
    documentVersion: 3,
    lastOperation: {
      conflictClass: "session-mutation",
      expectedSessionRevision: "17",
      kind: "writer-release-v1",
      operationId: "detach-operation-001",
      operationRevision: "2",
      requestSha256: "8".repeat(64),
      reservationId: "detach-reservation-001",
      resultSha256: "9".repeat(64),
      state: "committed",
    },
    launch: null,
    lease: null,
    lifecycle: "DETACHED",
    manifest: sessionManifest(),
    recovery: null,
    storageRef: {
      backendId: BACKEND_ID,
      contractVersion: 1,
      sessionId: SESSION_ID,
      storageId: STORAGE_ID,
    },
    writerEpoch: "12",
  };
}

function authoritySnapshot(revision = "20") {
  return {
    createdAt: "2026-08-05T00:00:00.000Z",
    document: authorityDocument(),
    revision,
    sessionId: SESSION_ID,
    updatedAt: "2026-08-05T00:03:00.000Z",
  };
}

function operation(state, kind, operationId, request, result = null) {
  const committed = state === "committed";
  return {
    conflictClass: "session-mutation",
    createdAt: "2026-08-05T00:03:00.000Z",
    expectedSession: authoritySnapshot(),
    kind,
    operationId,
    request,
    requestSha256: "f".repeat(64),
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
    reservationId: `reservation-${operationValue.operationId}`,
    sessionId: SESSION_ID,
    state: committed ? "released" : operationValue.state,
    updatedAt: operationValue.updatedAt,
  };
}

function catalogue(fixture) {
  return {
    captureAttemptId: "capture-attempt-001",
    checkpointId: CHECKPOINT_ID,
    committedAt: "2026-08-05T00:01:00.000Z",
    document: { artifactProof: fixture.artifactProof },
    sessionId: SESSION_ID,
  };
}

function generationRead(fixture, state, finalized = false) {
  const operationRequest = {
    predeterminedResult: fixture.restoreResult,
    admission: {
      checkpoint: checkpoint(),
      request: fixture.restoreRequest,
    },
    contractVersion: 1,
  };
  const operationValue = operation(
    state,
    RESTORE_DESTINATION_GENERATION_OPERATION_KIND,
    RESTORE_OPERATION_ID,
    operationRequest,
    state === "committed"
      ? { outcome: "restore-generation-committed" }
      : null,
  );
  const receipt = {
    catalogue: catalogue(fixture),
    generation:
      state === "committed"
        ? committedGeneration(fixture)
        : {
            binding: fixture.binding,
            checkpointId: CHECKPOINT_ID,
            claimedAt: "2026-08-05T00:01:00.000Z",
            committedAt: null,
            document: null,
            generationId: GENERATION_ID,
            operationId: RESTORE_OPERATION_ID,
            sessionId: SESSION_ID,
            state: "authorized",
          },
    operation: operationValue,
    reservation: reservationFor(operationValue),
    session: authoritySnapshot(state === "committed" ? "23" : "21"),
    status: state === "committed" ? "committed" : "authorized",
  };
  return finalized ? { ...receipt, finalized: true } : receipt;
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

function activationOperationRequest(fixture) {
  return createRestoreAttachmentActivationOperationRequest({
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
      detachOperationId: "detach-operation-001",
      stopOperationId: "stop-operation-001",
    },
  });
}

function activationRead(fixture, state, request, resultOverride = null) {
  const operationRequest = activationOperationRequest(fixture);
  const providerResult = resultOverride ?? activationResult(request);
  const operationValue = operation(
    state,
    RESTORE_ATTACHMENT_ACTIVATION_OPERATION_KIND,
    ACTIVATION_OPERATION_ID,
    operationRequest,
    state === "committed"
      ? {
          activationRequest: request,
          activationResult: providerResult,
          outcome: "restore-attachment-activated",
          resultVersion: 1,
        }
      : null,
  );
  return {
    activationRequest: request,
    generation: committedGeneration(fixture),
    operation: operationValue,
    reservation: reservationFor(operationValue),
    session: authoritySnapshot(state === "committed" ? "25" : "23"),
    status: state,
  };
}

function activationHandoff(fixture, request) {
  const activation = activationRead(fixture, "committed", request);
  const launchRequest = { source: "restore-activation" };
  const launchOperation = operation(
    "prepared",
    WRITER_LAUNCH_ATTEMPT_OPERATION_KIND,
    activationOperationRequest(fixture).launchIntent.launchAttemptId,
    launchRequest,
  );
  return {
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
      reservation: reservationFor(launchOperation),
    },
    session: authoritySnapshot("26"),
    status: "prepared",
  };
}

function generationCandidate(fixture) {
  return {
    checkpoint: checkpoint(),
    generationId: GENERATION_ID,
    request: fixture.restoreRequest,
  };
}

function activationCandidate(fixture, state = "uncertain") {
  return {
    activationOperationId: ACTIVATION_OPERATION_ID,
    request: activationOperationRequest(fixture),
    state,
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

function storageBackendHarness(prepare) {
  const forbiddenCalls = [];
  const providerCalls = [];
  const forbidden = (name) => async () => {
    forbiddenCalls.push(name);
    throw new Error(`forbidden backend operation: ${name}`);
  };
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
        return prepare(request);
      },
      prepareWritableAttachment: forbidden("prepareWritableAttachment"),
      provisionSession: forbidden("provisionSession"),
      restoreAttachmentActivationContractVersion: 1,
      restoreCheckpoint: forbidden("restoreCheckpoint"),
    },
    forbiddenCalls,
    providerCalls,
  };
}

function createCoordinator(fixture, authority, storage) {
  const guardPool = new GuardPool();
  const destinations = [];
  const coordinator = createPostgresRestoreActivationRecoveryCoordinator({
    authority,
    operationGuard: new PostgresOperationGuard({ dedicatedPool: guardPool }),
    publication: fixture.publication,
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

test("verifies, prepares, and atomically finalizes one restore attachment activation", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const request = activationRequest(fixture);
  const prepared = activationResult(request);
  const committed = activationHandoff(fixture, request);
  const authority = authorityHarness({
    "finalize-activation": async (input) => {
      assert.equal(input.expectedOperationRevision, "2");
      assert.deepEqual(input.activationResult, prepared);
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
    activationCandidate(fixture),
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

test("recovers activation finalization acknowledgement loss without a second attach", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const request = activationRequest(fixture);
  const prepared = activationResult(request);
  let state = "uncertain";
  let finalizeCalls = 0;
  const authority = authorityHarness({
    "finalize-activation": async () => {
      finalizeCalls += 1;
      state = "committed";
      throw new Error("activation finalization acknowledgement lost");
    },
    "read-activation": async () => activationRead(fixture, state, request),
  });
  const storage = storageBackendHarness(async () => prepared);
  const { coordinator } = createCoordinator(
    fixture,
    authority.authority,
    storage,
  );

  const first = await coordinator.reconcileRestoreAttachmentActivation(
    activationCandidate(fixture),
  );
  const second = await coordinator.reconcileRestoreAttachmentActivation(
    activationCandidate(fixture),
  );

  assert.equal(first.operation.state, "committed");
  assert.equal(second.operation.state, "committed");
  assert.equal(finalizeCalls, 1);
  assert.equal(storage.providerCalls.length, 1);
  assert.deepEqual(
    authority.calls.map(([name]) => name),
    [
      "read-activation",
      "finalize-activation",
      "read-activation",
      "read-activation",
    ],
  );
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
  const storage = storageBackendHarness(() => hostileResult);
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
    } else {
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

test("rejects a conflicting committed activation readback after lost finalization acknowledgement", async (t) => {
  const fixture = await createPublishedRestoreFixture(t);
  await makeSourceUnavailable(fixture);
  const request = activationRequest(fixture);
  const prepared = activationResult(request);
  const conflicting = structuredClone(prepared);
  conflicting.attachment.proofId = "proof-attach-conflict";
  conflicting.mutationResult.proofId = "proof-attach-conflict";
  let committed = false;
  let finalizeCalls = 0;
  const authority = authorityHarness({
    "finalize-activation": async () => {
      finalizeCalls += 1;
      committed = true;
      throw new Error("activation finalization acknowledgement lost");
    },
    "read-activation": async () =>
      activationRead(
        fixture,
        committed ? "committed" : "uncertain",
        request,
        committed ? conflicting : null,
      ),
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

  assert.equal(finalizeCalls, 1);
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
