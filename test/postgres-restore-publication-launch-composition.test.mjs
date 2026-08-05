import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import {
  lstat,
  mkdir,
  mkdtemp,
  rename,
  rm,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import {
  FilesystemOperationJournal,
  operationJournalBindingSha256,
} from "../src/filesystem-operation-journal.mjs";
import {
  PlatformImageReservationCoordinator,
} from "../src/platform-image-reservation.mjs";
import {
  PostgresRestorePublicationLaunchCompositionError,
  RESTORE_LAUNCH_V2_FLEET_CONFIRMED,
  createPostgresRestorePublicationLaunchComposition,
} from "../src/postgres-restore-publication-launch-composition.mjs";
import {
  SESSION_AUTHORITY_DOCUMENT_VERSION,
  SESSION_OPERATION_CONFLICT_CLASS,
  WRITER_LAUNCH_ATTEMPT_OPERATION_KIND,
  createWriterLaunchAttemptOperationRequest,
} from "../src/postgres-session-authority.mjs";
import { createSessionManifest } from "../src/session-storage-contracts.mjs";
import { StoppedDirectoryBackend } from "../src/stopped-directory-backend.mjs";
import { StoppedDirectoryPublication } from "../src/stopped-directory-publication.mjs";
import {
  StoppedWriterCapabilityCoordinator,
} from "../src/stopped-writer-capability.mjs";

const SESSION_ID = "9cb5643f-21c4-4fe3-8cf7-739af2f43246";
const CAPTURE_ATTEMPT_ID = "ce534fe4-48a7-424f-a62c-65f1de0019f7";
const RESTORE_OPERATION_ID = "restore-operation-001";
const LAUNCH_ATTEMPT_ID = "writer-launch-001";
const GENERATION_ID = "restore-generation-001";
const DESTINATION_ISOLATION_PROOF_ID = "destination-isolation-proof-001";
const NOW = "2026-08-04T12:00:00.000Z";
const OCI_MANIFEST_MEDIA_TYPE =
  "application/vnd.oci.image.manifest.v1+json";
const OCI_CONFIG_MEDIA_TYPE = "application/vnd.oci.image.config.v1+json";
const OCI_LAYER_MEDIA_TYPE = "application/vnd.oci.image.layer.v1.tar+gzip";
const CODEX_VERSION = "codex-cli 0.142.4";
const IMAGE_CONFIG_BYTES = Buffer.from(
  JSON.stringify({
    architecture: "arm64",
    config: { Env: ["PATH=/usr/local/bin:/usr/bin:/bin"] },
    os: "linux",
    rootfs: {
      diff_ids: [`sha256:${"d".repeat(64)}`],
      type: "layers",
    },
  }),
  "utf8",
);
const IMAGE_MANIFEST_BYTES = Buffer.from(
  JSON.stringify({
    config: {
      digest: `sha256:${sha256(IMAGE_CONFIG_BYTES)}`,
      mediaType: OCI_CONFIG_MEDIA_TYPE,
      size: IMAGE_CONFIG_BYTES.byteLength,
    },
    layers: [
      {
        digest: `sha256:${"c".repeat(64)}`,
        mediaType: OCI_LAYER_MEDIA_TYPE,
        size: 1024,
      },
    ],
    mediaType: OCI_MANIFEST_MEDIA_TYPE,
    schemaVersion: 2,
  }),
  "utf8",
);
const IMAGE_DESCRIPTOR = Object.freeze({
  bytes: IMAGE_MANIFEST_BYTES,
  digest: `sha256:${sha256(IMAGE_MANIFEST_BYTES)}`,
  mediaType: OCI_MANIFEST_MEDIA_TYPE,
  size: IMAGE_MANIFEST_BYTES.byteLength,
});
const IMAGE_DIGEST = IMAGE_DESCRIPTOR.digest;
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

function deepFreeze(value) {
  if (value === null || typeof value !== "object" || Object.isFrozen(value)) {
    return value;
  }
  for (const key of Reflect.ownKeys(value)) deepFreeze(value[key]);
  return Object.freeze(value);
}

function clone(value) {
  return structuredClone(value);
}

function sha256(value) {
  return createHash("sha256").update(value).digest("hex");
}

function jsonSha256(value) {
  return sha256(JSON.stringify(value));
}

function canonicalJsonData(value) {
  if (value === null || typeof value !== "object") return value;
  if (Array.isArray(value)) return value.map(canonicalJsonData);
  const result = Object.create(null);
  for (const key of Object.keys(value).sort()) {
    result[key] = canonicalJsonData(value[key]);
  }
  return result;
}

function authorityLease(value) {
  return {
    contractVersion: 1,
    sessionId: value.sessionId,
    leaseId: value.leaseId,
    holderId: value.holderId,
    fencingEpoch: value.fencingEpoch,
    expiresAt: value.expiresAt,
  };
}

function authorityAttachment(value) {
  return {
    contractVersion: 1,
    backendId: value.backendId,
    storageId: value.storageId,
    sessionId: value.sessionId,
    attachmentId: value.attachmentId,
    leaseId: value.leaseId,
    holderId: value.holderId,
    fencingEpoch: value.fencingEpoch,
    operationId: value.operationId,
    proofId: value.proofId,
    kind: value.kind,
    rootPath: value.rootPath,
    mode: value.mode,
  };
}

function authoritySession(value) {
  const document = value.document;
  const manifestValue = document.manifest;
  const canonicalManifest = {
    schemaVersion: manifestValue.schemaVersion,
    sessionId: manifestValue.sessionId,
    codex: {
      rootThreadId: manifestValue.codex.rootThreadId,
      sessionId: manifestValue.codex.sessionId,
      ephemeral: manifestValue.codex.ephemeral,
      historyMode: manifestValue.codex.historyMode,
    },
    runtime: {
      imageDigest: manifestValue.runtime.imageDigest,
      imageMediaType: manifestValue.runtime.imageMediaType,
      platform: manifestValue.runtime.platform,
      codexVersion: manifestValue.runtime.codexVersion,
      codexSandbox: manifestValue.runtime.codexSandbox,
    },
    layoutVersion: manifestValue.layoutVersion,
    authMode: manifestValue.authMode,
    agents: {
      defaultMaxSubagents: manifestValue.agents.defaultMaxSubagents,
      maxSubagents: manifestValue.agents.maxSubagents,
      maxDepth: manifestValue.agents.maxDepth,
    },
  };
  return deepFreeze({
    sessionId: value.sessionId,
    revision: value.revision,
    document: {
      documentVersion: document.documentVersion,
      manifest: canonicalManifest,
      storageRef: {
        contractVersion: document.storageRef.contractVersion,
        backendId: document.storageRef.backendId,
        storageId: document.storageRef.storageId,
        sessionId: document.storageRef.sessionId,
      },
      backendCapabilities: {
        atomicPointInTimeCheckpoint:
          document.backendCapabilities.atomicPointInTimeCheckpoint,
        exclusiveWriterAttachment:
          document.backendCapabilities.exclusiveWriterAttachment,
        fencing: document.backendCapabilities.fencing,
        normalDirectoryAttachment:
          document.backendCapabilities.normalDirectoryAttachment,
      },
      lifecycle: document.lifecycle,
      writerEpoch: document.writerEpoch,
      lease: document.lease === null ? null : authorityLease(document.lease),
      attachment:
        document.attachment === null
          ? null
          : authorityAttachment(document.attachment),
      activeOperation:
        document.activeOperation === null
          ? null
          : clone(document.activeOperation),
      lastOperation:
        document.lastOperation === null
          ? null
          : {
              conflictClass: document.lastOperation.conflictClass,
              expectedSessionRevision:
                document.lastOperation.expectedSessionRevision,
              kind: document.lastOperation.kind,
              operationId: document.lastOperation.operationId,
              operationRevision: document.lastOperation.operationRevision,
              requestSha256: document.lastOperation.requestSha256,
              reservationId: document.lastOperation.reservationId,
              resultSha256: document.lastOperation.resultSha256,
              state: document.lastOperation.state,
            },
      recovery: null,
      launch: document.launch === null ? null : clone(document.launch),
    },
    createdAt: value.createdAt,
    updatedAt: value.updatedAt,
  });
}

function operationRequestSha256(input) {
  const envelope = {
    requestVersion: 1,
    conflictClass: SESSION_OPERATION_CONFLICT_CLASS,
    expectedSession: authoritySession(input.expectedSession),
    payload: canonicalJsonData(input.request),
  };
  return jsonSha256(envelope);
}

function reservationId(operationId) {
  return `reservation-${sha256(operationId)}`;
}

function destinationChangedError() {
  const error = new Error("rename destination changed");
  error.code = "destination_changed";
  Object.defineProperty(error, "renameOutcome", { value: "not-committed" });
  return error;
}

function simpleLockProvider() {
  return async () => ({
    async assertHeld() {},
    async release() {},
    async renameWhileHeld(source, destination, expectedDestination) {
      if (expectedDestination?.kind === "absent") {
        try {
          await lstat(destination);
        } catch (error) {
          if (error?.code !== "ENOENT") throw destinationChangedError();
          await rename(source, destination);
          return;
        }
        throw destinationChangedError();
      }
      await rename(source, destination);
    },
  });
}

async function inspectTestPersistentObjectIdentity(path) {
  const metadata = await lstat(path, { bigint: true });
  return {
    device: metadata.dev.toString(),
    inode: metadata.ino.toString(),
    objectId: `test-object-${metadata.dev}-${metadata.ino}-${metadata.birthtimeNs}`,
  };
}

function manifest() {
  return createSessionManifest({
    sessionId: SESSION_ID,
    codex: {
      ephemeral: false,
      historyMode: "paginated",
      rootThreadId: SESSION_ID,
      sessionId: SESSION_ID,
    },
    runtime: {
      codexSandbox: "danger-full-access",
      codexVersion: CODEX_VERSION,
      imageDigest: IMAGE_DIGEST,
      imageMediaType: OCI_MANIFEST_MEDIA_TYPE,
      platform: "linux/arm64",
    },
  });
}

function lease() {
  return {
    contractVersion: 1,
    expiresAt: "2026-08-04T13:00:00.000Z",
    fencingEpoch: "2",
    holderId: "host-restore-001",
    leaseId: "lease-restore-001",
    sessionId: SESSION_ID,
  };
}

function attachment() {
  const currentLease = lease();
  return {
    attachmentId: "attachment-restore-001",
    backendId: "single-attach-test",
    contractVersion: 1,
    fencingEpoch: currentLease.fencingEpoch,
    holderId: currentLease.holderId,
    kind: "directory",
    leaseId: currentLease.leaseId,
    mode: "read-write",
    operationId: "attachment-operation-001",
    proofId: "attachment-proof-001",
    rootPath: "/var/lib/portable-codex/restore/session-001",
    sessionId: SESSION_ID,
    storageId: "volume-restore-001",
  };
}

function expectedSession(
  documentVersion = SESSION_AUTHORITY_DOCUMENT_VERSION,
) {
  return deepFreeze({
    createdAt: "2026-08-04T11:00:00.000Z",
    document: {
      activeOperation: null,
      attachment: attachment(),
      backendCapabilities: {
        atomicPointInTimeCheckpoint: true,
        exclusiveWriterAttachment: true,
        fencing: "epoch-enforced",
        normalDirectoryAttachment: true,
      },
      documentVersion,
      lastOperation: {
        conflictClass: "session-mutation",
        expectedSessionRevision: "0",
        kind: "writer-attachment-acquire-v1",
        operationId: "attachment-operation-001",
        operationRevision: "2",
        requestSha256: "d".repeat(64),
        reservationId: "attachment-reservation-001",
        resultSha256: "e".repeat(64),
        state: "committed",
      },
      launch: null,
      lease: lease(),
      lifecycle: "ATTACHED",
      manifest: manifest(),
      recovery: null,
      storageRef: {
        backendId: "single-attach-test",
        contractVersion: 1,
        sessionId: SESSION_ID,
        storageId: "volume-restore-001",
      },
      writerEpoch: "2",
    },
    revision: "3",
    sessionId: SESSION_ID,
    updatedAt: "2026-08-04T11:30:00.000Z",
  });
}

function admission() {
  const session = expectedSession();
  const checkpoint = {
    artifactId: "artifact-001",
    backendId: session.document.storageRef.backendId,
    checkpointClass: "clean",
    checkpointId: "checkpoint-001",
    codexSessionId: session.document.manifest.codex.sessionId,
    codexThreadId: session.document.manifest.codex.rootThreadId,
    contractVersion: 1,
    createdAt: "2026-08-04T10:00:00.000Z",
    imageDigest: session.document.manifest.runtime.imageDigest,
    sessionId: SESSION_ID,
    sourceFencingEpoch: "1",
    storageId: session.document.storageRef.storageId,
  };
  return deepFreeze({
    checkpoint,
    request: {
      backendId: checkpoint.backendId,
      contractVersion: 1,
      fencingEpoch: session.document.lease.fencingEpoch,
      holderId: session.document.lease.holderId,
      leaseId: session.document.lease.leaseId,
      operation: "restore",
      operationId: RESTORE_OPERATION_ID,
      sessionId: SESSION_ID,
      storageId: checkpoint.storageId,
      target: {
        artifactId: checkpoint.artifactId,
        checkpointId: checkpoint.checkpointId,
        kind: "checkpoint",
      },
    },
  });
}

async function createPublicationFixture(t) {
  const root = await mkdtemp(join(tmpdir(), "restore-launch-composition-"));
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
  await mkdir(join(sourceDirectory, "workspace"), {
    mode: 0o700,
    recursive: true,
  });
  await writeFile(
    join(sourceDirectory, "workspace", "README.md"),
    "portable restore\n",
    { mode: 0o600 },
  );
  const journal = new FilesystemOperationJournal({
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
    inspectPersistentObjectIdentity: inspectTestPersistentObjectIdentity,
    journal,
    ...TRUSTED_PUBLICATION_INSPECTORS,
  });
  const currentAdmission = admission();
  const captureOperationId = "capture-operation-001";
  const captureRequest = {
    backendId: currentAdmission.checkpoint.backendId,
    contractVersion: 1,
    fencingEpoch: currentAdmission.checkpoint.sourceFencingEpoch,
    holderId: "host-capture-001",
    leaseId: "lease-capture-001",
    operation: "checkpoint",
    operationId: captureOperationId,
    sessionId: currentAdmission.checkpoint.sessionId,
    storageId: currentAdmission.checkpoint.storageId,
    target: {
      artifactId: currentAdmission.checkpoint.artifactId,
      checkpointId: currentAdmission.checkpoint.checkpointId,
      kind: "checkpoint",
    },
  };
  const captureResult = {
    checkpoint: currentAdmission.checkpoint,
    mutation: {
      ...captureRequest,
      proofId: "proof-checkpoint-001",
      status: "checkpoint-created",
    },
  };
  const artifactDirectory = join(
    artifactOwnedRoot,
    currentAdmission.checkpoint.artifactId,
  );
  const captured = await publication.publishFreshCheckpointArtifact({
    artifactDirectory,
    artifactOwnedRoot,
    binding: { contractVersion: 1, operationId: captureOperationId },
    operationId: captureOperationId,
    request: captureRequest,
    result: captureResult,
    sourceDirectory,
    sourceOwnedRoot,
  });
  return {
    artifactDirectory,
    artifactOwnedRoot,
    artifactProof: deepFreeze({
      artifactManifestDigest: captured.materialization.artifactManifestDigest,
      captureOperationId,
      modeledDigest: captured.materialization.modeledDigest,
    }),
    destinationDirectory: join(destinationOwnedRoot, "restored-session"),
    destinationOwnedRoot,
    journal,
    publication,
  };
}

function measuredImage() {
  const runtime = expectedSession().document.manifest.runtime;
  return deepFreeze({
    projection: {
      codexSandbox: runtime.codexSandbox,
      codexVersion: runtime.codexVersion,
      platformImage: {
        architecture: "arm64",
        config: {
          digest: `sha256:${"b".repeat(64)}`,
          mediaType: "application/vnd.oci.image.config.v1+json",
          size: 512,
        },
        digest: runtime.imageDigest,
        mediaType: runtime.imageMediaType,
        os: "linux",
        size: 1024,
      },
    },
    runtimeIdentity: {
      codexBinaryPath: "/opt/portable-codex/bin/codex",
      codexBinarySha256: "c".repeat(64),
      codexVersion: runtime.codexVersion,
      platformImageDigest: runtime.imageDigest,
    },
  });
}

function launchIntent() {
  return deepFreeze({
    launchAttemptId: LAUNCH_ATTEMPT_ID,
    measuredImage: measuredImage(),
    supervisor: {
      contractVersion: 1,
      supervisorId: "supervisor-001",
    },
  });
}

function activePointer(operation, reservation) {
  return {
    conflictClass: operation.conflictClass,
    expectedSessionRevision: operation.expectedSession.revision,
    kind: operation.kind,
    operationId: operation.operationId,
    operationRevision: operation.revision,
    requestSha256: operation.requestSha256,
    reservationId: reservation.reservationId,
    state: operation.state,
  };
}

function terminalPointer(operationValue, reservationValue) {
  return {
    conflictClass: operationValue.conflictClass,
    expectedSessionRevision: operationValue.expectedSession.revision,
    kind: operationValue.kind,
    operationId: operationValue.operationId,
    operationRevision: operationValue.revision,
    requestSha256: operationValue.requestSha256,
    reservationId: reservationValue.reservationId,
    resultSha256: jsonSha256(operationValue.result),
    state: operationValue.state,
  };
}

function operation(input, state, revision, updatedAt) {
  const expected = authoritySession(input.expectedSession);
  return deepFreeze({
    conflictClass: SESSION_OPERATION_CONFLICT_CLASS,
    createdAt: "2026-08-04T12:00:00.000Z",
    expectedSession: expected,
    kind: input.kind,
    operationId: input.operationId,
    request: canonicalJsonData(input.request),
    requestSha256: operationRequestSha256({
      ...input,
      expectedSession: expected,
    }),
    result: null,
    retiredAt: null,
    revision,
    sessionId: input.expectedSession.sessionId,
    state,
    updatedAt,
  });
}

function reservation(operationValue, state) {
  return deepFreeze({
    conflictClass: operationValue.conflictClass,
    createdAt: operationValue.createdAt,
    expectedSessionRevision: operationValue.expectedSession.revision,
    expiresAt: null,
    kind: operationValue.kind,
    operationId: operationValue.operationId,
    releasedAt: null,
    requestSha256: operationValue.requestSha256,
    reservationId: reservationId(operationValue.operationId),
    sessionId: operationValue.sessionId,
    state,
    updatedAt: operationValue.updatedAt,
  });
}

function activeSession(base, operationValue, reservationValue, revision) {
  const session = clone(authoritySession(base));
  session.revision = revision;
  session.updatedAt = operationValue.updatedAt;
  session.document.documentVersion = SESSION_AUTHORITY_DOCUMENT_VERSION;
  session.document.activeOperation = activePointer(
    operationValue,
    reservationValue,
  );
  return deepFreeze(session);
}

function terminalSession(base, operationValue, reservationValue, revision) {
  const session = clone(authoritySession(base));
  session.revision = revision;
  session.updatedAt = operationValue.updatedAt;
  session.document.documentVersion = SESSION_AUTHORITY_DOCUMENT_VERSION;
  session.document.activeOperation = null;
  session.document.lastOperation = terminalPointer(
    operationValue,
    reservationValue,
  );
  return deepFreeze(session);
}

function artifactProof() {
  return deepFreeze({
    artifactManifestDigest: "2".repeat(64),
    captureOperationId: "capture-operation-001",
    modeledDigest: "3".repeat(64),
  });
}

function generationBinding(input, reservationValue, catalogueDocument) {
  return deepFreeze({
    attachment: input.expectedSession.document.attachment,
    captureAttemptId: CAPTURE_ATTEMPT_ID,
    captureOperationId: "capture-operation-001",
    catalogueSha256: jsonSha256(catalogueDocument),
    checkpoint: input.request.admission.checkpoint,
    contractVersion: 1,
    destinationIsolationProofId: DESTINATION_ISOLATION_PROOF_ID,
    destinationState: "detached",
    generationId: GENERATION_ID,
    request: input.request.admission.request,
    reservationId: reservationValue.reservationId,
  });
}

function materialization(binding, overrides = {}) {
  const proof = artifactProof();
  return deepFreeze({
    artifactManifestDigest: proof.artifactManifestDigest,
    contractVersion: 3,
    coordinatorBindingSha256: operationJournalBindingSha256(binding),
    modeledDigest: proof.modeledDigest,
    publicationId: `publication-${RESTORE_OPERATION_ID}`,
    publicationKind: "restore-destination",
    stagedRoot: {
      filesystemId: "restore-filesystem-001",
      objectIdentityScheme: "persistent-file-id-v1",
      objectId: "restore-object-001",
    },
    treeIdentityDigest: "5".repeat(64),
    ...overrides,
  });
}

class MemoryAuthority {
  constructor(events, options = {}) {
    this.afterReserveReceiptCreated =
      options.afterReserveReceiptCreated ?? null;
    this.events = events;
    this.artifactProof = options.artifactProof ?? artifactProof();
    this.claimAcknowledgementLoss = options.claimAcknowledgementLoss === true;
    this.claimAcknowledgementLost = false;
    this.claimFailureBeforeDispatch =
      options.claimFailureBeforeDispatch === true;
    this.expectedSession = expectedSession(
      options.expectedDocumentVersion ?? SESSION_AUTHORITY_DOCUMENT_VERSION,
    );
    this.handoffFailures = options.handoffFailures ?? 0;
    this.handoffReceiptMutation = options.handoffReceiptMutation ?? null;
    this.handoffCalls = 0;
    this.handoffRevisions = [];
    this.input = null;
    this.claim = null;
    this.cancelled = null;
    this.committed = null;
    this.handoffReceipt = null;
    this.phase = "absent";
    this.reserveAcquired = options.reserveAcquired !== false;
    this.reserveReceiptMutation = options.reserveReceiptMutation ?? null;
  }

  async readSession() {
    this.events.push("authority.read-session");
    return this.expectedSession;
  }

  async reserveOperation(input) {
    this.events.push("authority.reserve");
    if (this.phase !== "absent") {
      const current = await this.readRestoreDestinationGenerationOperation();
      return deepFreeze({ acquired: false, ...clone(current) });
    }
    this.input = input;
    const reservedOperation = operation(input, "prepared", "0", NOW);
    const reservedReservation = reservation(reservedOperation, "prepared");
    const prepared = {
      acquired: this.reserveAcquired,
      operation: reservedOperation,
      reservation: reservedReservation,
      session: activeSession(
        input.expectedSession,
        reservedOperation,
        reservedReservation,
        "4",
      ),
      status: "prepared",
    };
    this.prepared = deepFreeze(
      this.reserveReceiptMutation === null
        ? prepared
        : this.reserveReceiptMutation(clone(prepared)),
    );
    this.phase = "prepared";
    if (this.afterReserveReceiptCreated !== null) {
      this.afterReserveReceiptCreated(this.prepared);
    }
    return this.prepared;
  }

  async claimRestoreDestinationGenerationDispatch(input) {
    this.events.push("authority.claim");
    if (this.claimFailureBeforeDispatch && this.phase === "prepared") {
      throw new Error("claim failed before durable dispatch");
    }
    if (this.phase !== "prepared") {
      const current = await this.readRestoreDestinationGenerationOperation();
      return deepFreeze({
        authorityNow: current.generation.claimedAt,
        catalogue: current.catalogue,
        dispatchGranted: false,
        generation: current.generation,
        operation: current.operation,
        reservation: current.reservation,
        session: current.session,
        status: current.status,
      });
    }
    const startingOperation = operation(input, "starting", "1", NOW);
    const startingReservation = reservation(startingOperation, "starting");
    const catalogue = deepFreeze({
      captureAttemptId: CAPTURE_ATTEMPT_ID,
      checkpointId: input.request.admission.checkpoint.checkpointId,
      committedAt: "2026-08-04T10:30:00.000Z",
      document: {
        artifactProof: this.artifactProof,
        contractVersion: 1,
        materialization: {},
        result: {},
      },
      sessionId: input.expectedSession.sessionId,
    });
    const binding = generationBinding(
      input,
      startingReservation,
      catalogue.document,
    );
    const generation = deepFreeze({
      binding,
      checkpointId: input.request.admission.checkpoint.checkpointId,
      claimedAt: NOW,
      committedAt: null,
      document: null,
      generationId: input.generationId,
      operationId: input.operationId,
      sessionId: input.expectedSession.sessionId,
      state: "authorized",
    });
    this.claim = deepFreeze({
      authorityNow: NOW,
      catalogue,
      dispatchGranted: true,
      generation,
      operation: startingOperation,
      reservation: startingReservation,
      session: activeSession(
        input.expectedSession,
        startingOperation,
        startingReservation,
        "5",
      ),
      status: "starting",
    });
    this.phase = "starting";
    if (this.claimAcknowledgementLoss && !this.claimAcknowledgementLost) {
      this.claimAcknowledgementLost = true;
      throw new Error("claim acknowledgement lost");
    }
    return this.claim;
  }

  async readRestoreDestinationGenerationOperation() {
    this.events.push("authority.read-restore");
    if (this.phase === "absent") {
      return deepFreeze({
        catalogue: null,
        generation: null,
        operation: null,
        reservation: null,
        session: this.expectedSession,
        status: "absent",
      });
    }
    if (this.phase === "prepared") {
      return deepFreeze({
        catalogue: null,
        generation: null,
        operation: this.prepared.operation,
        reservation: this.prepared.reservation,
        session: this.prepared.session,
        status: "prepared",
      });
    }
    if (this.phase === "starting") {
      return deepFreeze({
        catalogue: this.claim.catalogue,
        generation: this.claim.generation,
        operation: this.claim.operation,
        reservation: this.claim.reservation,
        session: this.claim.session,
        status: "starting",
      });
    }
    if (this.phase === "uncertain") return this.uncertain;
    if (this.phase === "cancelled") {
      return deepFreeze({
        catalogue: null,
        generation: null,
        operation: this.cancelled.operation,
        reservation: this.cancelled.reservation,
        session: this.cancelled.session,
        status: "cancelled-before-dispatch",
      });
    }
    return this.committed;
  }

  async finalizeRestoreDestinationGenerationAndReserveWriterLaunchAttempt(
    handoffInput,
  ) {
    this.handoffCalls += 1;
    this.handoffRevisions.push(
      handoffInput.restore.expectedOperationRevision,
    );
    this.events.push(`authority.handoff:${this.handoffCalls}`);
    if (this.committed !== null) return this.handoffReceipt;
    const completion = handoffInput.restore.completion;
    const generationDocument = deepFreeze({
      artifactProof: clone(this.artifactProof),
      contractVersion: 2,
      materialization: completion.materialization,
      result: completion.result,
    });
    const committedGeneration = deepFreeze({
      ...clone(this.claim.generation),
      committedAt: NOW,
      document: generationDocument,
      state: "committed",
    });
    const restoreResult = deepFreeze({
      catalogueSha256: jsonSha256(this.claim.catalogue.document),
      checkpointId: committedGeneration.checkpointId,
      generationDocumentSha256: jsonSha256(generationDocument),
      generationId: committedGeneration.generationId,
      outcome: "restore-generation-committed",
      resultVersion: 1,
    });
    const committedOperation = deepFreeze({
      ...clone(this.claim.operation),
      result: restoreResult,
      retiredAt: NOW,
      revision: (
        BigInt(handoffInput.restore.expectedOperationRevision) + 1n
      ).toString(),
      state: "committed",
    });
    const committedReservation = deepFreeze({
      ...clone(this.claim.reservation),
      releasedAt: NOW,
      state: "released",
    });
    const restoreTerminalRevision = (
      BigInt(committedOperation.expectedSession.revision) +
      BigInt(committedOperation.revision) +
      1n
    ).toString();
    const restoreTerminal = terminalSession(
      committedOperation.expectedSession,
      committedOperation,
      committedReservation,
      restoreTerminalRevision,
    );
    const launchRequest = createWriterLaunchAttemptOperationRequest({
      expectedSession: restoreTerminal,
      generation: committedGeneration,
      measuredImage: handoffInput.launch.measuredImage,
      supervisor: handoffInput.launch.supervisor,
    });
    const launchInput = deepFreeze({
      expectedSession: restoreTerminal,
      kind: WRITER_LAUNCH_ATTEMPT_OPERATION_KIND,
      operationId: handoffInput.launch.launchAttemptId,
      request: launchRequest,
    });
    const launchOperation = operation(launchInput, "prepared", "0", NOW);
    const launchReservation = reservation(launchOperation, "prepared");
    const handoffSession = activeSession(
      restoreTerminal,
      launchOperation,
      launchReservation,
      (BigInt(restoreTerminal.revision) + 1n).toString(),
    );
    const handoff = deepFreeze({
      generation: committedGeneration,
      launch: {
        attempt: {
          contractVersion: 1,
          launchAttemptId: handoffInput.launch.launchAttemptId,
          request: launchRequest,
          result: null,
          state: "prepared",
        },
        operation: launchOperation,
        reservation: launchReservation,
      },
      restore: {
        catalogue: clone(this.claim.catalogue),
        finalized: true,
        operation: committedOperation,
        reservation: committedReservation,
      },
      session: handoffSession,
      status: "prepared",
    });
    this.handoffReceipt =
      this.handoffReceiptMutation === null
        ? handoff
        : deepFreeze(this.handoffReceiptMutation(clone(handoff)));
    this.committed = deepFreeze({
      catalogue: this.claim.catalogue,
      generation: committedGeneration,
      operation: committedOperation,
      reservation: committedReservation,
      session: handoffSession,
      status: "committed",
    });
    this.phase = "committed";
    if (this.handoffCalls <= this.handoffFailures) {
      throw new Error("handoff acknowledgement lost");
    }
    return this.handoffReceipt;
  }

  async markOperationUncertain() {
    this.events.push("authority.mark-uncertain");
    if (this.phase === "starting") {
      const uncertainOperation = operation(
        this.input,
        "uncertain",
        "2",
        NOW,
      );
      const uncertainReservation = reservation(
        uncertainOperation,
        "uncertain",
      );
      this.uncertain = deepFreeze({
        catalogue: this.claim.catalogue,
        generation: this.claim.generation,
        operation: uncertainOperation,
        reservation: uncertainReservation,
        session: activeSession(
          this.input.expectedSession,
          uncertainOperation,
          uncertainReservation,
          "6",
        ),
        status: "uncertain",
      });
      this.phase = "uncertain";
    }
    return deepFreeze({});
  }

  async reconcileOperation() {
    this.events.push("authority.reconcile");
    if (this.phase === "prepared") {
      return deepFreeze({
        operation: this.prepared.operation,
        reservation: this.prepared.reservation,
        session: this.prepared.session,
        status: "prepared",
      });
    }
    if (this.phase === "cancelled") {
      return deepFreeze({
        operation: this.cancelled.operation,
        reservation: this.cancelled.reservation,
        session: this.cancelled.session,
        status: "committed",
      });
    }
    return deepFreeze({
      operation: null,
      reservation: null,
      session: this.expectedSession,
      status: "absent",
    });
  }

  async cancelPreparedOperation(input) {
    this.events.push("authority.cancel");
    if (this.phase === "cancelled") {
      return deepFreeze({ cancelled: false, ...clone(this.cancelled) });
    }
    assert.equal(this.phase, "prepared");
    assert.equal(input.operationId, this.prepared.operation.operationId);
    assert.equal(input.expectedOperationRevision, "0");
    const result = deepFreeze({
      resultVersion: 1,
      outcome: "cancelled-before-dispatch",
      reason: input.reason,
    });
    const cancelledOperation = deepFreeze({
      ...clone(this.prepared.operation),
      result,
      retiredAt: NOW,
      revision: "1",
      state: "committed",
      updatedAt: NOW,
    });
    const cancelledReservation = deepFreeze({
      ...clone(this.prepared.reservation),
      releasedAt: NOW,
      state: "released",
      updatedAt: NOW,
    });
    const cancelledSession = terminalSession(
      cancelledOperation.expectedSession,
      cancelledOperation,
      cancelledReservation,
      (BigInt(cancelledOperation.expectedSession.revision) + 2n).toString(),
    );
    this.cancelled = deepFreeze({
      operation: cancelledOperation,
      reservation: cancelledReservation,
      session: cancelledSession,
      status: "committed",
    });
    this.phase = "cancelled";
    return deepFreeze({ cancelled: true, ...clone(this.cancelled) });
  }
}

class MemoryLauncher {
  constructor(events, imageReservation, authority, options = {}) {
    this.afterPrepareLaunchIntent = options.afterPrepareLaunchIntent ?? null;
    this.events = events;
    this.imageReservation = imageReservation;
    this.authority = authority;
    this.resultMutation = options.launchResultMutation ?? null;
    this.launchCalls = 0;
    this.prepareImageReservation = null;
    this.prepareOpaqueReservation = null;
    this.runCalls = 0;
    this.runImageReservation = null;
    this.runOpaqueReservation = null;
    this.started = false;
  }

  prepareLaunchIntent(input) {
    this.events.push("launcher.prepare");
    assert.notStrictEqual(input.imageReservation, this.imageReservation);
    assert.equal(Object.isFrozen(input.imageReservation), true);
    assert.strictEqual(
      input.imageReservation.configBytes,
      this.imageReservation.configBytes,
    );
    assert.strictEqual(
      input.imageReservation.descriptor,
      this.imageReservation.descriptor,
    );
    assert.strictEqual(
      input.imageReservation.inspectCodex,
      this.imageReservation.inspectCodex,
    );
    assert.strictEqual(
      input.imageReservation.reservation,
      this.imageReservation.reservation,
    );
    this.prepareImageReservation = input.imageReservation;
    this.prepareOpaqueReservation = input.imageReservation.reservation;
    if (this.afterPrepareLaunchIntent !== null) {
      this.afterPrepareLaunchIntent(input);
    }
    return Promise.resolve(launchIntent());
  }

  runPreparedLaunch(input) {
    this.events.push("launcher.run");
    this.runCalls += 1;
    if (!this.started) {
      this.launchCalls += 1;
      this.started = true;
    }
    assert.strictEqual(
      input.imageReservation.configBytes,
      this.prepareImageReservation.configBytes,
    );
    assert.strictEqual(
      input.imageReservation.descriptor,
      this.prepareImageReservation.descriptor,
    );
    assert.strictEqual(
      input.imageReservation.inspectCodex,
      this.prepareImageReservation.inspectCodex,
    );
    this.runImageReservation = input.imageReservation;
    this.runOpaqueReservation = input.imageReservation.reservation;
    assert.strictEqual(
      this.runOpaqueReservation,
      this.prepareOpaqueReservation,
    );
    const prepared = this.authority.handoffReceipt.launch.operation;
    const launchEvidence = deepFreeze({
      contractVersion: 1,
      launchAttemptId: input.launchAttemptId,
      processIncarnationId: "process-incarnation-001",
      proofId: "supervisor-proof-001",
      status: "started",
      supervisorId: prepared.request.supervisor.supervisorId,
      writerIncarnationId: "writer-incarnation-001",
    });
    const terminalResult = deepFreeze({
      evidence: launchEvidence,
      outcome: "writer-launch-started",
      resultVersion: 1,
    });
    const committedAt = "2026-08-04T12:00:01.000Z";
    const committedOperation = deepFreeze({
      ...clone(prepared),
      result: terminalResult,
      retiredAt: committedAt,
      revision: "2",
      state: "committed",
      updatedAt: committedAt,
    });
    const committedReservation = deepFreeze({
      ...clone(this.authority.handoffReceipt.launch.reservation),
      releasedAt: committedAt,
      state: "released",
      updatedAt: committedAt,
    });
    const launch = deepFreeze({
      attachmentId: prepared.request.attachment.attachmentId,
      attachmentSha256: jsonSha256(prepared.request.attachment),
      contractVersion: 1,
      fencingEpoch: prepared.request.fencingEpoch,
      generation: clone(prepared.request.generation),
      launchAttemptId: input.launchAttemptId,
      launchResultSha256: jsonSha256(terminalResult),
      leaseId: prepared.request.lease.leaseId,
      leaseSha256: jsonSha256(prepared.request.lease),
      measuredImageSha256: jsonSha256(prepared.request.measuredImage),
      processIncarnationId: launchEvidence.processIncarnationId,
      startedAt: committedAt,
      supervisorId: launchEvidence.supervisorId,
      supervisorProofId: launchEvidence.proofId,
      writerIncarnationId: launchEvidence.writerIncarnationId,
    });
    const launchedSession = clone(
      terminalSession(
        committedOperation.expectedSession,
        committedOperation,
        committedReservation,
        (
          BigInt(committedOperation.expectedSession.revision) +
          BigInt(committedOperation.revision) +
          1n
        ).toString(),
      ),
    );
    launchedSession.document.launch = launch;
    const result = {
      attempt: {
        contractVersion: 1,
        launchAttemptId: input.launchAttemptId,
        request: clone(committedOperation.request),
        result: terminalResult,
        state: "committed",
      },
      contractVersion: 1,
      evidence: launchEvidence,
      launch,
      operation: committedOperation,
      reservation: committedReservation,
      session: launchedSession,
      status: "started",
      writer: Object.freeze(Object.create(null)),
    };
    this.authority.handoffReceipt = deepFreeze({
      generation: this.authority.handoffReceipt.generation,
      launch: {
        attempt: result.attempt,
        operation: result.operation,
        reservation: result.reservation,
      },
      restore: this.authority.handoffReceipt.restore,
      session: result.session,
      status: "committed",
    });
    this.authority.committed = deepFreeze({
      ...this.authority.committed,
      session: result.session,
    });
    const mutableResult = clone(result);
    mutableResult.writer = result.writer;
    return Promise.resolve(
      this.resultMutation === null
        ? deepFreeze(result)
        : deepFreeze(this.resultMutation(mutableResult)),
    );
  }
}

class OneUseImageCapabilityLauncher extends MemoryLauncher {
  constructor(events, imageReservation, authority, imageReservations) {
    super(events, imageReservation, authority);
    this.imageReservations = imageReservations;
    this.prepareCalls = 0;
  }

  async prepareLaunchIntent(input) {
    this.events.push("launcher.prepare");
    this.prepareCalls += 1;
    assert.notStrictEqual(input.imageReservation, this.imageReservation);
    assert.equal(Object.isFrozen(input.imageReservation), true);
    assert.strictEqual(
      input.imageReservation.configBytes,
      this.imageReservation.configBytes,
    );
    assert.strictEqual(
      input.imageReservation.descriptor,
      this.imageReservation.descriptor,
    );
    assert.strictEqual(
      input.imageReservation.inspectCodex,
      this.imageReservation.inspectCodex,
    );
    assert.strictEqual(
      input.imageReservation.reservation,
      this.imageReservation.reservation,
    );
    this.prepareImageReservation = input.imageReservation;
    this.prepareOpaqueReservation = input.imageReservation.reservation;
    const measuredImage = await this.imageReservations.revalidateReservation(
      input.imageReservation,
    );
    return deepFreeze({
      launchAttemptId: input.launchAttemptId,
      measuredImage,
      supervisor: {
        contractVersion: 1,
        supervisorId: "supervisor-001",
      },
    });
  }

  async runPreparedLaunch(input) {
    if (!this.started) {
      await this.imageReservations.consumeReservation(
        input.imageReservation,
      );
    }
    return await super.runPreparedLaunch(input);
  }
}

class MemoryGuard {
  constructor(events, options = {}) {
    this.events = events;
    this.failAssertCall = options.guardFailAssertCall ?? null;
    this.held = false;
    this.assertCalls = 0;
  }

  async runExclusive(operationId, callback) {
    assert.equal(operationId, RESTORE_OPERATION_ID);
    this.held = true;
    this.events.push("guard.enter");
    const probe = Object.freeze({
      assertHeld: async () => {
        this.assertCalls += 1;
        if (this.assertCalls === this.failAssertCall) {
          throw new Error("operation guard ownership lost");
        }
        assert.equal(this.held, true);
      },
    });
    try {
      return await callback(probe);
    } finally {
      this.held = false;
      this.events.push("guard.exit");
    }
  }
}

async function fixture(t, options = {}) {
  const events = [];
  const publicationFixture = await createPublicationFixture(t);
  const imageReservation =
    options.imageReservation ??
    Object.freeze({
      configBytes: Object.freeze(Object.create(null)),
      descriptor: Object.freeze(Object.create(null)),
      inspectCodex() {},
      reservation: Object.freeze(Object.create(null)),
    });
  const authority = new MemoryAuthority(events, {
    ...options,
    artifactProof: publicationFixture.artifactProof,
  });
  const launcher =
    options.launcherFactory?.({ authority, events, imageReservation }) ??
    new MemoryLauncher(events, imageReservation, authority, options);
  const guard = new MemoryGuard(events, options);
  let gateCalls = 0;
  let preparationCalls = 0;
  let publicationCalls = 0;
  let publicationAcknowledgementLosses =
    options.publicationAcknowledgementLosses ?? 0;
  let gateResult =
    options.gateResult ?? RESTORE_LAUNCH_V2_FLEET_CONFIRMED;
  const fleetCapabilityGate = async () => {
    gateCalls += 1;
    events.push("fleet.gate");
    return gateResult;
  };
  const prepareRestore = async () => {
    preparationCalls += 1;
    events.push("restore.prepare");
    return {
      artifactDirectory: publicationFixture.artifactDirectory,
      artifactOwnedRoot: publicationFixture.artifactOwnedRoot,
      destinationDirectory: publicationFixture.destinationDirectory,
      destinationIsolationProofId: DESTINATION_ISOLATION_PROOF_ID,
      destinationOwnedRoot: publicationFixture.destinationOwnedRoot,
      generationId: GENERATION_ID,
      imageReservation,
      launchAttemptId: LAUNCH_ATTEMPT_ID,
    };
  };
  const composer = createPostgresRestorePublicationLaunchComposition({
    authority,
    fleetCapabilityGate,
    launcher,
    operationGuard: guard,
    prepareRestore,
    publication: publicationFixture.publication,
  });
  const publish = async (context) => {
    publicationCalls += 1;
    events.push("publication.commit");
    assert.equal(guard.held, true);
    assert.equal(context.destinationState, "detached");
    assert.equal(
      context.generationBinding.generationId,
      GENERATION_ID,
    );
    if (options.forgePublicationCompletion) {
      return deepFreeze({
        materialization: materialization(context.generationBinding),
        replayed: false,
        result: context.result,
      });
    }
    const input = {
      artifactDirectory: context.artifactDirectory,
      artifactOwnedRoot: context.artifactOwnedRoot,
      artifactProof: context.artifactProof,
      binding: context.generationBinding,
      destinationDirectory: context.destinationDirectory,
      destinationOwnedRoot: context.destinationOwnedRoot,
      operationId: RESTORE_OPERATION_ID,
      request: admission().request,
      result: context.result,
    };
    const completion = await (context.publicationMode === "committed-only"
      ? publicationFixture.publication.verifyCommittedRestoreDestination(input)
      : publicationFixture.publication.publishRestoreDestination(input));
    if (publicationAcknowledgementLosses > 0) {
      publicationAcknowledgementLosses -= 1;
      throw new Error("publication acknowledgement lost");
    }
    return completion;
  };
  return {
    authority,
    composer,
    events,
    get gateCalls() {
      return gateCalls;
    },
    get preparationCalls() {
      return preparationCalls;
    },
    get publicationCalls() {
      return publicationCalls;
    },
    guard,
    imageReservation,
    launcher,
    publication: publicationFixture,
    publish,
    setGateResult(value) {
      gateResult = value;
    },
  };
}

function assertCompositionError(code) {
  return (error) =>
    error instanceof PostgresRestorePublicationLaunchCompositionError &&
    error.code === code &&
    error.retryable === false;
}

function createUnusedLifecycleBackend() {
  const fail = async () => {
    throw new Error("lifecycle operation is not used by restore composition");
  };
  return Object.freeze({
    backendId: "single-attach-test",
    capabilities: Object.freeze({
      atomicPointInTimeCheckpoint: false,
      exclusiveWriterAttachment: true,
      fencing: "manual",
      normalDirectoryAttachment: true,
    }),
    captureCheckpoint: fail,
    contractVersion: 1,
    destroySession: fail,
    detachAttachment: fail,
    forceFence: fail,
    prepareWritableAttachment: fail,
    provisionSession: fail,
    restoreCheckpoint: fail,
  });
}

async function unsupportedCaptureAuthority() {
  throw new Error("capture authority is not used by restore composition");
}

function unusedStoppedWriterResolver() {
  throw new Error("restore does not resolve a stopped writer");
}

test("committed restore publication hands off atomically before prepared launch", async (t) => {
  const value = await fixture(t);

  assert.equal(Object.isFrozen(value.composer), true);
  assert.equal(value.composer.restoreContextContractVersion, 3);
  assert.equal(Object.isFrozen(RESTORE_LAUNCH_V2_FLEET_CONFIRMED), true);

  const completion = await value.composer.runRestore(
    admission(),
    value.publish,
  );

  assert.equal(completion.replayed, false);
  assert.equal(value.gateCalls, 1);
  assert.equal(value.preparationCalls, 1);
  assert.equal(value.publicationCalls, 1);
  assert.equal(value.authority.handoffCalls, 1);
  assert.equal(value.launcher.launchCalls, 1);
  assert.deepEqual(value.events, [
    "authority.read-restore",
    "fleet.gate",
    "restore.prepare",
    "launcher.prepare",
    "guard.enter",
    "authority.read-restore",
    "authority.reserve",
    "authority.claim",
    "publication.commit",
    "authority.handoff:1",
    "guard.exit",
    "launcher.run",
  ]);
});

test("v2 expected session upgrades authority receipts to v3 without rewriting the operation input", async (t) => {
  const value = await fixture(t, { expectedDocumentVersion: 2 });

  const completion = await value.composer.runRestore(
    admission(),
    value.publish,
  );

  assert.equal(completion.replayed, false);
  assert.equal(
    value.authority.input.expectedSession.document.documentVersion,
    2,
  );
  assert.equal(
    value.authority.prepared.operation.expectedSession.document.documentVersion,
    2,
  );
  assert.equal(value.authority.prepared.session.document.documentVersion, 3);
  assert.equal(
    value.authority.claim.operation.expectedSession.document.documentVersion,
    2,
  );
  assert.equal(value.authority.claim.session.document.documentVersion, 3);
  assert.equal(value.authority.handoffCalls, 1);
  assert.equal(value.launcher.launchCalls, 1);
});

test("v1 initial restore session fails before the fleet gate or durable work", async (t) => {
  const value = await fixture(t, { expectedDocumentVersion: 1 });

  await assert.rejects(
    value.composer.runRestore(admission(), value.publish),
    assertCompositionError(
      "postgres_restore_publication_launch_composition_outcome_uncertain",
    ),
  );

  assert.equal(value.gateCalls, 0);
  assert.equal(value.preparationCalls, 0);
  assert.equal(value.events.includes("authority.reserve"), false);
  assert.equal(value.events.includes("authority.claim"), false);
  assert.equal(value.publicationCalls, 0);
  assert.equal(value.launcher.runCalls, 0);
});

test("preparation snapshots one image reservation capability across prepare and run", async (t) => {
  const reservationA = Object.freeze(Object.create(null));
  const reservationB = Object.freeze(Object.create(null));
  const imageReservation = {
    configBytes: Object.freeze(Object.create(null)),
    descriptor: Object.freeze(Object.create(null)),
    inspectCodex() {},
    reservation: reservationA,
  };
  const value = await fixture(t, {
    afterPrepareLaunchIntent() {
      imageReservation.reservation = reservationB;
    },
    imageReservation,
  });

  await value.composer.runRestore(admission(), value.publish);

  assert.strictEqual(imageReservation.reservation, reservationB);
  assert.notStrictEqual(
    value.launcher.prepareImageReservation,
    imageReservation,
  );
  assert.equal(
    Object.isFrozen(value.launcher.prepareImageReservation),
    true,
  );
  assert.strictEqual(
    value.launcher.prepareOpaqueReservation,
    reservationA,
  );
  assert.strictEqual(
    value.launcher.runImageReservation,
    value.launcher.prepareImageReservation,
  );
  assert.strictEqual(value.launcher.runOpaqueReservation, reservationA);
  assert.equal(value.authority.handoffCalls, 1);
  assert.equal(value.launcher.launchCalls, 1);
});

test("stopped-directory backend composes real publication through atomic handoff and prepared launch", async (t) => {
  const value = await fixture(t);
  const backend = new StoppedDirectoryBackend({
    backendId: "single-attach-test",
    coordinator: new StoppedWriterCapabilityCoordinator(),
    lifecycleBackend: createUnusedLifecycleBackend(),
    mutationAuthority: Object.freeze({
      restoreContextContractVersion: value.composer.restoreContextContractVersion,
      runCapture: unsupportedCaptureAuthority,
      runCaptureReconciliation: unsupportedCaptureAuthority,
      runRestore: value.composer.runRestore,
    }),
    publication: value.publication.publication,
    resolveStoppedWriter: unusedStoppedWriterResolver,
  });

  const result = await backend.restoreCheckpoint(admission());
  const observed = await value.publication.journal.read({
    operationId: RESTORE_OPERATION_ID,
  });

  assert.equal(result.mutation.status, "restored");
  assert.strictEqual(
    result,
    value.authority.committed.generation.document.result,
  );
  assert.equal(observed.record.state, "committed");
  assert.equal(
    operationJournalBindingSha256(observed.record.result),
    operationJournalBindingSha256(result),
  );
  assert.equal(
    (
      await lstat(
        join(
          value.publication.destinationDirectory,
          "workspace",
          "README.md",
        ),
      )
    ).isFile(),
    true,
  );
  assert.equal(
    operationJournalBindingSha256(observed.record.binding.coordinator),
    operationJournalBindingSha256(value.authority.claim.generation.binding),
  );
  assert.equal(
    observed.record.materialization.coordinatorBindingSha256,
    operationJournalBindingSha256(observed.record.binding.coordinator),
  );
  assert.equal(value.publicationCalls, 0);
  assert.equal(value.gateCalls, 1);
  assert.equal(value.preparationCalls, 1);
  assert.equal(value.authority.handoffCalls, 1);
  assert.equal(value.launcher.launchCalls, 1);
  assert.ok(
    value.events.indexOf("authority.handoff:1") <
      value.events.indexOf("launcher.run"),
  );
});

test("fleet gate fails before restore preparation or durable work", async (t) => {
  const value = await fixture(t, {
    gateResult: Object.freeze(Object.create(null)),
  });

  await assert.rejects(
    value.composer.runRestore(admission(), value.publish),
    assertCompositionError("restore_launch_v2_fleet_capability_required"),
  );

  assert.equal(value.gateCalls, 1);
  assert.equal(value.preparationCalls, 0);
  assert.equal(value.publicationCalls, 0);
  assert.equal(value.launcher.launchCalls, 0);
  assert.deepEqual(value.events, ["authority.read-restore", "fleet.gate"]);
});

test("forged publication completion fails before handoff or launch", async (t) => {
  const value = await fixture(t, { forgePublicationCompletion: true });

  await assert.rejects(
    value.composer.runRestore(admission(), value.publish),
    assertCompositionError(
      "postgres_restore_publication_launch_composition_outcome_uncertain",
    ),
  );

  assert.equal(value.publicationCalls, 1);
  assert.equal(value.authority.handoffCalls, 0);
  assert.equal(value.launcher.launchCalls, 0);
  assert.equal(value.events.includes("authority.mark-uncertain"), true);
});

test("claim acknowledgement loss never authorizes a fresh publication", async (t) => {
  const value = await fixture(t, { claimAcknowledgementLoss: true });

  await assert.rejects(
    value.composer.runRestore(admission(), value.publish),
    assertCompositionError(
      "postgres_restore_publication_launch_composition_outcome_uncertain",
    ),
  );

  assert.equal(value.publicationCalls, 1);
  assert.equal(value.authority.handoffCalls, 0);
  assert.equal(value.launcher.launchCalls, 0);
  assert.equal(value.events.includes("authority.mark-uncertain"), true);
  assert.equal(
    (
      await value.publication.journal.read({
        operationId: RESTORE_OPERATION_ID,
      })
    ).record,
    null,
  );
});

test("an observed reserve replay is never cancelled as invocation-owned", async (t) => {
  const value = await fixture(t, {
    claimFailureBeforeDispatch: true,
    reserveAcquired: false,
  });

  await assert.rejects(
    value.composer.runRestore(admission(), value.publish),
    assertCompositionError(
      "postgres_restore_publication_launch_composition_outcome_uncertain",
    ),
  );

  assert.equal(value.authority.phase, "prepared");
  assert.equal(value.events.includes("authority.cancel"), false);
  assert.equal(value.publicationCalls, 0);
  assert.equal(value.launcher.launchCalls, 0);
});

test("a pre-dispatch cancellation is terminal on the next exact restore read", async (t) => {
  const value = await fixture(t, { claimFailureBeforeDispatch: true });

  await assert.rejects(
    value.composer.runRestore(admission(), value.publish),
    assertCompositionError(
      "postgres_restore_publication_launch_composition_outcome_uncertain",
    ),
  );

  assert.equal(value.authority.phase, "cancelled");
  assert.equal(value.authority.cancelled.operation.state, "committed");
  assert.equal(value.authority.cancelled.operation.revision, "1");
  assert.deepEqual(value.authority.cancelled.operation.result, {
    outcome: "cancelled-before-dispatch",
    reason: "restore-publication-not-started",
    resultVersion: 1,
  });
  assert.equal(value.preparationCalls, 1);
  assert.equal(value.publicationCalls, 0);
  assert.equal(value.launcher.launchCalls, 0);

  const retryEventIndex = value.events.length;
  await assert.rejects(
    value.composer.runRestore(admission(), value.publish),
    assertCompositionError(
      "postgres_restore_publication_launch_composition_outcome_uncertain",
    ),
  );

  assert.equal(value.authority.phase, "cancelled");
  assert.equal(value.preparationCalls, 1);
  assert.equal(value.publicationCalls, 0);
  assert.equal(value.launcher.launchCalls, 0);
  assert.deepEqual(value.events.slice(retryEventIndex), [
    "authority.read-restore",
  ]);
});

test("malformed pre-dispatch cancellation relations fail before retry work", async (t) => {
  const cases = [
    {
      mutate(receipt) {
        receipt.session.revision = "999";
      },
      name: "terminal session revision",
    },
    {
      mutate(receipt) {
        receipt.session.updatedAt = "2026-08-04T12:00:01.000Z";
      },
      name: "terminal session timestamp",
    },
    {
      mutate(receipt) {
        receipt.session.document.writerEpoch = "999";
      },
      name: "terminal session stable content",
    },
    {
      mutate(receipt) {
        receipt.session.document.lastOperation.operationId =
          "forged-restore-operation";
      },
      name: "terminal last-operation identity",
    },
    {
      mutate(receipt) {
        receipt.session.document.lastOperation.resultSha256 = "f".repeat(64);
      },
      name: "terminal result digest",
    },
    {
      mutate(receipt) {
        receipt.reservation.releasedAt = "2026-08-04T12:00:01.000Z";
      },
      name: "released reservation timestamp",
    },
    {
      mutate(receipt) {
        receipt.reservation.reservationId = "forged-reservation";
      },
      name: "released reservation pointer",
    },
    {
      mutate(receipt) {
        const forgedRequestSha256 = "e".repeat(64);
        receipt.operation.requestSha256 = forgedRequestSha256;
        receipt.reservation.requestSha256 = forgedRequestSha256;
        receipt.session.document.lastOperation.requestSha256 =
          forgedRequestSha256;
      },
      name: "synchronized request digest",
    },
    {
      mutate(receipt) {
        receipt.reservation.reservationId = "forged-reservation";
        receipt.session.document.lastOperation.reservationId =
          "forged-reservation";
      },
      name: "synchronized reservation pointer",
    },
    {
      mutate(receipt) {
        receipt.operation.request.predeterminedResult.mutation.proofId =
          "proof-forged-restore-result";
      },
      name: "V2 predetermined request relation",
    },
  ];

  for (const { mutate, name } of cases) {
    await t.test(name, async (t) => {
      const value = await fixture(t, { claimFailureBeforeDispatch: true });
      await assert.rejects(
        value.composer.runRestore(admission(), value.publish),
        assertCompositionError(
          "postgres_restore_publication_launch_composition_outcome_uncertain",
        ),
      );
      assert.equal(value.authority.phase, "cancelled");

      const forged = clone(value.authority.cancelled);
      mutate(forged);
      value.authority.cancelled = deepFreeze(forged);
      const retryEventIndex = value.events.length;

      await assert.rejects(
        value.composer.runRestore(admission(), value.publish),
        assertCompositionError(
          "postgres_restore_publication_launch_composition_cancellation_receipt_invalid",
        ),
      );

      assert.equal(value.preparationCalls, 1);
      assert.equal(value.publicationCalls, 0);
      assert.equal(value.launcher.launchCalls, 0);
      assert.deepEqual(value.events.slice(retryEventIndex), [
        "authority.read-restore",
      ]);
    });
  }
});

test("active session relation rejects stable-field forgery with a poisoned Array iterator", async (t) => {
  const iteratorDescriptor = Object.getOwnPropertyDescriptor(
    Array.prototype,
    Symbol.iterator,
  );
  const value = await fixture(t, {
    afterReserveReceiptCreated() {
      Object.defineProperty(Array.prototype, Symbol.iterator, {
        ...iteratorDescriptor,
        value: function* emptyArrayIterator() {},
      });
    },
    reserveReceiptMutation(receipt) {
      receipt.session.document.writerEpoch = "999";
      return receipt;
    },
  });
  let observedError = null;

  try {
    try {
      await value.composer.runRestore(admission(), value.publish);
    } catch (error) {
      observedError = error;
    }
  } finally {
    Object.defineProperty(
      Array.prototype,
      Symbol.iterator,
      iteratorDescriptor,
    );
  }

  assert.ok(
    observedError instanceof PostgresRestorePublicationLaunchCompositionError,
  );
  assert.equal(
    observedError.code,
    "postgres_restore_publication_launch_composition_outcome_uncertain",
  );
  assert.equal(value.authority.phase, "prepared");
  assert.equal(value.events.includes("authority.claim"), false);
  assert.equal(value.publicationCalls, 0);
  assert.equal(value.authority.handoffCalls, 0);
  assert.equal(value.launcher.runCalls, 0);
});

test("active session relation rejects a downgraded current document version before claim", async (t) => {
  const value = await fixture(t, {
    expectedDocumentVersion: 2,
    reserveReceiptMutation(receipt) {
      receipt.session.document.documentVersion = 2;
      return receipt;
    },
  });

  await assert.rejects(
    value.composer.runRestore(admission(), value.publish),
    assertCompositionError(
      "postgres_restore_publication_launch_composition_outcome_uncertain",
    ),
  );

  assert.equal(
    value.authority.prepared.operation.expectedSession.document.documentVersion,
    2,
  );
  assert.equal(value.authority.prepared.session.document.documentVersion, 2);
  assert.equal(value.authority.phase, "prepared");
  assert.equal(value.events.includes("authority.claim"), false);
  assert.equal(value.publicationCalls, 0);
  assert.equal(value.authority.handoffCalls, 0);
  assert.equal(value.launcher.runCalls, 0);
});

test("a forged handoff session relation is rejected before launch", async (t) => {
  const value = await fixture(t, {
    handoffReceiptMutation(receipt) {
      receipt.session.document.activeOperation.reservationId =
        "forged-launch-reservation";
      return receipt;
    },
  });

  await assert.rejects(
    value.composer.runRestore(admission(), value.publish),
    assertCompositionError(
      "postgres_restore_publication_launch_composition_outcome_uncertain",
    ),
  );

  assert.equal(value.authority.handoffCalls, 1);
  assert.equal(value.authority.phase, "committed");
  assert.equal(value.launcher.runCalls, 0);
  assert.equal(value.launcher.launchCalls, 0);
});

test("an invalid restore terminal result cannot self-consistently forge handoff", async (t) => {
  const value = await fixture(t, {
    handoffReceiptMutation(receipt) {
      receipt.restore.operation.result.resultVersion = 2;
      const resultSha256 = jsonSha256(receipt.restore.operation.result);
      receipt.launch.operation.expectedSession.document.lastOperation.resultSha256 =
        resultSha256;
      receipt.session.document.lastOperation.resultSha256 = resultSha256;
      const requestSha256 = operationRequestSha256({
        expectedSession: receipt.launch.operation.expectedSession,
        kind: receipt.launch.operation.kind,
        operationId: receipt.launch.operation.operationId,
        request: receipt.launch.operation.request,
      });
      receipt.launch.operation.requestSha256 = requestSha256;
      receipt.launch.reservation.requestSha256 = requestSha256;
      receipt.session.document.activeOperation.requestSha256 = requestSha256;
      return receipt;
    },
  });

  await assert.rejects(
    value.composer.runRestore(admission(), value.publish),
    assertCompositionError(
      "postgres_restore_publication_launch_composition_outcome_uncertain",
    ),
  );

  assert.equal(value.authority.handoffCalls, 1);
  assert.equal(value.launcher.runCalls, 0);
});

test("a forged launch success relation is rejected after durable handoff", async (t) => {
  const value = await fixture(t, {
    launchResultMutation(result) {
      result.operation.expectedSession.revision = "5";
      result.reservation.expectedSessionRevision = "5";
      result.session.document.lastOperation.expectedSessionRevision = "5";
      return result;
    },
  });

  await assert.rejects(
    value.composer.runRestore(admission(), value.publish),
    assertCompositionError(
      "postgres_restore_publication_launch_composition_outcome_uncertain",
    ),
  );

  assert.equal(value.authority.handoffCalls, 1);
  assert.equal(value.authority.phase, "committed");
  assert.equal(value.launcher.runCalls, 1);
  assert.equal(value.launcher.launchCalls, 1);
});

test("a forged started session identity is rejected after durable handoff", async (t) => {
  const value = await fixture(t, {
    launchResultMutation(result) {
      result.session.document.writerEpoch = "999";
      return result;
    },
  });

  await assert.rejects(
    value.composer.runRestore(admission(), value.publish),
    assertCompositionError(
      "postgres_restore_publication_launch_composition_outcome_uncertain",
    ),
  );

  assert.equal(value.authority.handoffCalls, 1);
  assert.equal(value.authority.phase, "committed");
  assert.equal(value.launcher.runCalls, 1);
  assert.equal(value.launcher.launchCalls, 1);
});

test("handoff acknowledgement loss rechecks the guard before retry", async (t) => {
  const value = await fixture(t, {
    guardFailAssertCall: 4,
    handoffFailures: 1,
  });

  await assert.rejects(
    value.composer.runRestore(admission(), value.publish),
    assertCompositionError(
      "postgres_restore_publication_launch_composition_outcome_uncertain",
    ),
  );

  assert.equal(value.guard.assertCalls, 4);
  assert.equal(value.authority.handoffCalls, 1);
  assert.equal(value.events.includes("authority.handoff:2"), false);
  assert.equal(value.launcher.runCalls, 0);
});

test("committed publication acknowledgement loss resumes without the fresh gate", async (t) => {
  const value = await fixture(t, {
    publicationAcknowledgementLosses: 1,
  });

  await assert.rejects(
    value.composer.runRestore(admission(), value.publish),
    assertCompositionError(
      "postgres_restore_publication_launch_composition_outcome_uncertain",
    ),
  );
  assert.equal(value.authority.phase, "uncertain");
  assert.equal(
    (
      await value.publication.journal.read({
        operationId: RESTORE_OPERATION_ID,
      })
    ).record.state,
    "committed",
  );
  value.setGateResult(Object.freeze(Object.create(null)));

  const replay = await value.composer.runRestore(admission(), value.publish);

  assert.equal(replay.replayed, true);
  assert.equal(value.gateCalls, 1);
  assert.equal(value.publicationCalls, 2);
  assert.deepEqual(value.authority.handoffRevisions, ["2"]);
  assert.equal(value.launcher.launchCalls, 1);
});

test("committed replay bypasses the fresh gate and does not relaunch", async (t) => {
  const value = await fixture(t);
  await value.composer.runRestore(admission(), value.publish);
  assert.equal(value.authority.handoffReceipt.status, "committed");
  assert.notEqual(value.authority.handoffReceipt.session.document.launch, null);
  assert.equal(
    value.authority.committed.operation.requestSha256,
    operationRequestSha256({
      expectedSession: value.authority.committed.operation.expectedSession,
      request: value.authority.committed.operation.request,
    }),
  );
  value.setGateResult(Object.freeze(Object.create(null)));

  const replay = await value.composer.runRestore(admission(), value.publish);

  assert.equal(replay.replayed, true);
  assert.equal(value.gateCalls, 1);
  assert.equal(value.authority.handoffCalls, 2);
  assert.deepEqual(value.authority.handoffRevisions, ["1", "1"]);
  assert.equal(value.launcher.runCalls, 2);
  assert.equal(value.launcher.launchCalls, 1);
});

test("committed replay uses durable launch intent after image capability consumption", async (t) => {
  const imageReservations = new PlatformImageReservationCoordinator();
  let imageAvailable = true;
  let inspectionCalls = 0;
  const inspectCodex = async () => {
    inspectionCalls += 1;
    if (!imageAvailable) throw new Error("platform image unavailable");
    return {
      codexBinaryPath: "/opt/portable-codex/bin/codex",
      codexBinarySha256: "c".repeat(64),
      codexVersion: CODEX_VERSION,
    };
  };
  const reservation = await imageReservations.reservePlatformImage({
    configBytes: IMAGE_CONFIG_BYTES,
    descriptor: IMAGE_DESCRIPTOR,
    inspectCodex,
    sessionManifest: manifest(),
  });
  const imageReservation = Object.freeze({
    configBytes: IMAGE_CONFIG_BYTES,
    descriptor: IMAGE_DESCRIPTOR,
    inspectCodex,
    reservation: reservation.reservation,
  });
  const value = await fixture(t, {
    imageReservation,
    launcherFactory({ authority, events }) {
      return new OneUseImageCapabilityLauncher(
        events,
        imageReservation,
        authority,
        imageReservations,
      );
    },
  });

  await value.composer.runRestore(admission(), value.publish);

  assert.equal(value.authority.handoffReceipt.status, "committed");
  assert.equal(value.launcher.prepareCalls, 1);
  assert.equal(value.launcher.launchCalls, 1);
  assert.equal(inspectionCalls, 3);
  await assert.rejects(
    imageReservations.revalidateReservation(imageReservation),
    (error) => error?.code === "platform_image_reservation_rejected",
  );
  imageAvailable = false;

  const replay = await value.composer.runRestore(admission(), value.publish);

  assert.equal(replay.replayed, true);
  assert.equal(value.launcher.prepareCalls, 1);
  assert.equal(value.launcher.runCalls, 2);
  assert.equal(value.launcher.launchCalls, 1);
  assert.equal(inspectionCalls, 3);
});

test("committed replay rejects a forged durable launch intent before capability use", async (t) => {
  const value = await fixture(t);
  await value.composer.runRestore(admission(), value.publish);
  const forged = clone(value.authority.committed);
  forged.operation.request.launchIntent.supervisor.supervisorId =
    "forged-supervisor";
  value.authority.committed = deepFreeze(forged);
  const retryEventIndex = value.events.length;

  await assert.rejects(
    value.composer.runRestore(admission(), value.publish),
    assertCompositionError(
      "postgres_restore_publication_launch_composition_outcome_uncertain",
    ),
  );

  assert.equal(value.launcher.runCalls, 1);
  assert.equal(value.launcher.launchCalls, 1);
  assert.deepEqual(value.events.slice(retryEventIndex), [
    "authority.read-restore",
  ]);
});

test("handoff acknowledgement loss replays the same handoff before one launch", async (t) => {
  const value = await fixture(t, { handoffFailures: 1 });

  await value.composer.runRestore(admission(), value.publish);

  assert.equal(value.authority.handoffCalls, 2);
  assert.equal(value.publicationCalls, 1);
  assert.equal(value.launcher.launchCalls, 1);
  assert.ok(
    value.events.indexOf("authority.handoff:2") <
      value.events.indexOf("guard.exit"),
  );
  assert.ok(
    value.events.indexOf("guard.exit") < value.events.indexOf("launcher.run"),
  );
});
