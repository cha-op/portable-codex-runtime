import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import {
  lstat,
  mkdir,
  open,
  readFile,
  rename,
  stat,
  writeFile,
} from "node:fs/promises";
import { dirname, join, resolve } from "node:path";
import test from "node:test";
import { setTimeout as delay } from "node:timers/promises";

import {
  FilesystemOperationJournal,
} from "../src/filesystem-operation-journal.mjs";
import {
  createExt4PodmanAttachmentBinding,
} from "../src/ext4-podman-attachment-binding.mjs";
import {
  createExt4FilesystemImagePaths,
} from "../src/ext4-filesystem-image-paths.mjs";
import {
  FILESYSTEM_IMAGE_PROVIDER_STATE_HEAD_CONTRACT_VERSION,
  FilesystemImageProviderState,
  normalizeFilesystemImageProviderStateHead,
} from "../src/filesystem-image-provider-state.mjs";
import {
  createLinuxExt4ImageDriver,
} from "../src/linux-ext4-image-driver.mjs";
import {
  createLinuxExt4Inspector,
} from "../src/linux-ext4-inspector.mjs";
import {
  STOPPED_DIRECTORY_PUBLICATION_LOCK_NAME,
  StoppedDirectoryPublication,
} from "../src/stopped-directory-publication.mjs";
import {
  retireExt4PodmanRootlessNamespaceForConformance,
  runExt4PodmanWriterIntegration,
} from "./ext4-podman-writer.mjs";

const BACKEND_ID = "linux-ext4-physical-v1";
const SESSION_ID = "019f2100-0000-7000-8000-000000000001";
const THREAD_ID = "019f2100-0000-7000-8000-000000000002";
const CHECKPOINT_ID = "checkpoint-physical-001";
const ARTIFACT_ID = "artifact-physical-001";
const CHECKPOINT_OPERATION_ID = "physical-checkpoint-001";
const RESTORE_OPERATION_ID = "physical-restore-001";
const RESTORE_CHILD = `generation-${"a".repeat(48)}`;
const IMAGE_DIGEST = `sha256:${"b".repeat(64)}`;
const IMAGE_SIZE_BYTES = 64 * 1024 * 1024;
const PAYLOAD = Buffer.from(
  "portable-codex-runtime linux ext4 cross-host evidence\n",
  "utf8",
);
const PODMAN_MARKER = Buffer.from("ready\n", "utf8");
const MODE = process.env.LINUX_EXT4_TEST_MODE ?? "lifecycle";
const ROOT = resolve(
  process.env.LINUX_EXT4_TEST_ROOT ??
    "/var/tmp/portable-codex-runtime-linux-ext4",
);
const HELPER = resolve(
  process.env.LINUX_EXT4_INSPECTOR_HELPER ??
    "/usr/local/libexec/portable-codex-linux-ext4-inspector",
);
const PODMAN = resolve(process.env.PODMAN_EXECUTABLE ?? "/usr/bin/podman");
const PODMAN_ENGINE_EXCLUSIVE =
  process.env.LINUX_EXT4_PODMAN_ENGINE_EXCLUSIVE === "1";
const PODMAN_WRITER_IMAGE_DIGEST =
  process.env.PODMAN_WRITER_IMAGE_DIGEST ?? null;
const PODMAN_WRITER_IMAGE_REFERENCE =
  process.env.PODMAN_WRITER_IMAGE_REFERENCE ?? null;
const TRANSFER_PATH = join(ROOT, "transfer.json");
const PRIVATE_MOUNT_NAMESPACE =
  process.env.LINUX_EXT4_PRIVATE_MOUNT_NAMESPACE === "1";
const PEER_NAMESPACE_BARRIER =
  process.env.LINUX_EXT4_PEER_NAMESPACE_BARRIER === "1";
const PEER_READY_PATH = `${ROOT}.peer-ready`;
const PEER_READY_PENDING_PATH = `${PEER_READY_PATH}.pending`;
const PEER_RELEASE_PATH = `${ROOT}.peer-release`;
const CREATED_AT = "2026-08-14T00:00:00.000Z";
if (!PRIVATE_MOUNT_NAMESPACE) {
  throw new TypeError(
    "Linux ext4 integration requires a host-owned private mount namespace",
  );
}
const EXTERNAL_ARCHIVE_PUBLICATION_CONTROL_IDENTITY =
  process.env.LINUX_EXT4_ARCHIVE_PUBLICATION_CONTROL_IDENTITY === undefined
    ? null
    : publicationControlIdentity(
        JSON.parse(
          process.env.LINUX_EXT4_ARCHIVE_PUBLICATION_CONTROL_IDENTITY,
        ),
      );
const EXTERNAL_ARCHIVE_MOUNT_PUBLICATION_CONTROL_IDENTITY =
  process.env.LINUX_EXT4_ARCHIVE_MOUNT_PUBLICATION_CONTROL_IDENTITY ===
  undefined
    ? null
    : publicationControlIdentity(
        JSON.parse(
          process.env.LINUX_EXT4_ARCHIVE_MOUNT_PUBLICATION_CONTROL_IDENTITY,
        ),
      );
const EXTERNAL_PRODUCER_UID =
  process.env.LINUX_EXT4_PRODUCER_UID === undefined
    ? null
    : Number(process.env.LINUX_EXT4_PRODUCER_UID);
if (
  EXTERNAL_PRODUCER_UID !== null &&
  (!Number.isSafeInteger(EXTERNAL_PRODUCER_UID) || EXTERNAL_PRODUCER_UID <= 0)
) {
  throw new TypeError("invalid external Linux ext4 producer UID");
}
const GENESIS_PROVIDER_STATE_HEAD = exact({
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
let providerStateHead = process.env.LINUX_EXT4_PROVIDER_STATE_HEAD === undefined
  ? GENESIS_PROVIDER_STATE_HEAD
  : normalizeFilesystemImageProviderStateHead(
      JSON.parse(process.env.LINUX_EXT4_PROVIDER_STATE_HEAD),
    );

function exact(values) {
  return Object.freeze(Object.assign(Object.create(null), values));
}

function publicationControlIdentity(value) {
  assert.equal(value !== null && typeof value === "object", true);
  assert.deepEqual(Object.keys(value).sort(), [
    "filesystemId",
    "objectId",
    "objectIdentityScheme",
  ]);
  for (const key of [
    "filesystemId",
    "objectId",
    "objectIdentityScheme",
  ]) {
    assert.equal(typeof value[key], "string");
    assert.match(value[key], /^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$/u);
  }
  return exact({
    filesystemId: value.filesystemId,
    objectIdentityScheme: value.objectIdentityScheme,
    objectId: value.objectId,
  });
}

function sameCanonicalValue(left, right) {
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

function createProviderStateHeadAnchor() {
  return exact({
    async compareAndAdvance({ expectedHead, nextHead }) {
      if (!sameCanonicalValue(providerStateHead, expectedHead)) return false;
      providerStateHead = normalizeFilesystemImageProviderStateHead(nextHead);
      return true;
    },
    async readHead() {
      return providerStateHead;
    },
  });
}

function context() {
  return exact({
    contractVersion: 1,
    invocation: exact({}),
    signal: new AbortController().signal,
  });
}

function sha256(value) {
  return createHash("sha256").update(value).digest("hex");
}

function provisionRequest() {
  return exact({
    backendId: BACKEND_ID,
    contractVersion: 1,
    operationId: "physical-provision-001",
    sessionId: SESSION_ID,
  });
}

function attachmentRequest(storageId, suffix, fencingEpoch) {
  return exact({
    backendId: BACKEND_ID,
    contractVersion: 1,
    fencingEpoch,
    holderId: `physical-holder-${suffix}`,
    leaseId: `physical-lease-${suffix}`,
    operation: "attach",
    operationId: `physical-attach-${suffix}`,
    sessionId: SESSION_ID,
    storageId,
    target: exact({
      attachmentId: `physical-attachment-${suffix}`,
      kind: "attachment",
    }),
  });
}

function detachRequest(attachment, suffix) {
  return exact({
    backendId: attachment.backendId,
    contractVersion: attachment.contractVersion,
    fencingEpoch: attachment.fencingEpoch,
    holderId: attachment.holderId,
    leaseId: attachment.leaseId,
    operation: "detach",
    operationId: `physical-detach-${suffix}`,
    sessionId: attachment.sessionId,
    storageId: attachment.storageId,
    target: exact({
      attachmentId: attachment.target.attachmentId,
      kind: "attachment",
    }),
  });
}

function destroyRequest(storage, suffix) {
  assert.notEqual(storage.writerAuthority, null);
  return exact({
    backendId: storage.backendId,
    contractVersion: 1,
    fencingEpoch: storage.writerAuthority.fencingEpoch,
    holderId: storage.writerAuthority.holderId,
    leaseId: storage.writerAuthority.leaseId,
    operation: "destroy",
    operationId: `physical-destroy-${suffix}`,
    sessionId: storage.sessionId,
    storageId: storage.storageId,
    target: exact({ kind: "storage", storageId: storage.storageId }),
  });
}

function publicationBinding(operation, operationId, storageId) {
  return exact({
    backendId: BACKEND_ID,
    operation,
    operationId,
    sessionId: SESSION_ID,
    storageId,
  });
}

function publicationMutationRequest(
  operation,
  operationId,
  storage,
  authority,
) {
  assert.notEqual(authority, null);
  return exact({
    backendId: BACKEND_ID,
    contractVersion: 1,
    fencingEpoch: authority.fencingEpoch,
    holderId: authority.holderId,
    leaseId: authority.leaseId,
    operation,
    operationId,
    sessionId: SESSION_ID,
    storageId: storage.storageId,
    target: exact({
      artifactId: ARTIFACT_ID,
      checkpointId: CHECKPOINT_ID,
      kind: "checkpoint",
    }),
  });
}

function checkpointDescriptor(storage) {
  assert.notEqual(storage.writerAuthority, null);
  return exact({
    artifactId: ARTIFACT_ID,
    backendId: BACKEND_ID,
    checkpointClass: "clean",
    checkpointId: CHECKPOINT_ID,
    codexSessionId: THREAD_ID,
    codexThreadId: THREAD_ID,
    contractVersion: 1,
    createdAt: CREATED_AT,
    imageDigest: IMAGE_DIGEST,
    sessionId: SESSION_ID,
    sourceFencingEpoch: storage.writerAuthority.fencingEpoch,
    storageId: storage.storageId,
  });
}

function publicationResult(request, storage) {
  return exact({
    checkpoint: checkpointDescriptor(storage),
    mutation: exact({
      ...request,
      proofId: `proof-${request.operation}-physical-001`,
      status:
        request.operation === "checkpoint"
          ? "checkpoint-created"
          : "restored",
    }),
  });
}

function publicationInputs(storage, directories, artifactProof = null) {
  assert.notEqual(storage.mount, null);
  assert.notEqual(storage.dataRoot, null);
  assert.notEqual(storage.writerAuthority, null);
  assert.equal(storage.writerAuthority.fencingEpoch, "1");
  const restoreAuthority = exact({
    fencingEpoch: "2",
    holderId: "physical-restore-holder-001",
    leaseId: "physical-restore-lease-001",
  });
  const checkpointRequest = publicationMutationRequest(
    "checkpoint",
    CHECKPOINT_OPERATION_ID,
    storage,
    storage.writerAuthority,
  );
  const restoreRequest = publicationMutationRequest(
    "restore",
    RESTORE_OPERATION_ID,
    storage,
    restoreAuthority,
  );
  const artifactOwnedRoot = directories.archive;
  const artifactDirectory = join(artifactOwnedRoot, ARTIFACT_ID);
  const destinationOwnedRoot = storage.mount.mountPath;
  const destinationDirectory = join(destinationOwnedRoot, RESTORE_CHILD);
  return exact({
    capture: exact({
      artifactDirectory,
      artifactOwnedRoot,
      binding: publicationBinding(
        "checkpoint",
        CHECKPOINT_OPERATION_ID,
        storage.storageId,
      ),
      operationId: CHECKPOINT_OPERATION_ID,
      request: checkpointRequest,
      result: publicationResult(checkpointRequest, storage),
      sourceDirectory: storage.dataRoot.rootPath,
      sourceOwnedRoot: destinationOwnedRoot,
    }),
    restore:
      artifactProof === null
        ? null
        : exact({
            artifactDirectory,
            artifactOwnedRoot,
            artifactProof,
            binding: publicationBinding(
              "restore",
              RESTORE_OPERATION_ID,
              storage.storageId,
            ),
            destinationDirectory,
            destinationOwnedRoot,
            operationId: RESTORE_OPERATION_ID,
            request: restoreRequest,
            result: publicationResult(restoreRequest, storage),
          }),
  });
}

function checkpointVerificationInput(capture) {
  return exact({
    artifactDirectory: capture.artifactDirectory,
    artifactOwnedRoot: capture.artifactOwnedRoot,
    binding: capture.binding,
    operationId: capture.operationId,
    request: capture.request,
    result: capture.result,
  });
}

function restoreVerificationInput(restore) {
  assert.notEqual(restore, null);
  return exact({
    artifactProof: restore.artifactProof,
    binding: restore.binding,
    destinationDirectory: restore.destinationDirectory,
    destinationOwnedRoot: restore.destinationOwnedRoot,
    operationId: restore.operationId,
    request: restore.request,
    result: restore.result,
  });
}

async function publishAndVerify(fixed, storage) {
  const initial = publicationInputs(storage, fixed.directories);
  const checkpointPublished =
    await fixed.publication.publishFreshCheckpointArtifact(initial.capture);
  const checkpointVerified =
    await fixed.publication.verifyCommittedCheckpointArtifact(
      checkpointVerificationInput(initial.capture),
    );
  assert.deepEqual(
    checkpointVerified.materialization,
    checkpointPublished.materialization,
  );
  const artifactProof = exact({
    artifactManifestDigest:
      checkpointPublished.materialization.artifactManifestDigest,
    captureOperationId: CHECKPOINT_OPERATION_ID,
    modeledDigest: checkpointPublished.materialization.modeledDigest,
  });
  const inputs = publicationInputs(storage, fixed.directories, artifactProof);
  assert.notEqual(inputs.restore, null);
  const restorePublished = await fixed.publication.publishRestoreDestination(
    inputs.restore,
  );
  const restoreVerified =
    await fixed.publication.verifyCommittedRestoreDestination(
      restoreVerificationInput(inputs.restore),
    );
  assert.deepEqual(
    restoreVerified.materialization,
    restorePublished.materialization,
  );
  const restoredPayloadPath = join(
    inputs.restore.destinationDirectory,
    "portable.txt",
  );
  assert.deepEqual(await readFile(restoredPayloadPath), PAYLOAD);
  return exact({
    artifactDirectory: inputs.capture.artifactDirectory,
    artifactProof,
    destinationDirectory: inputs.restore.destinationDirectory,
    restoredPayloadPath,
  });
}

async function verifyTransferredPublication(fixed, storage, receipt) {
  const inputs = publicationInputs(
    storage,
    fixed.directories,
    exact(receipt.artifactProof),
  );
  assert.notEqual(inputs.restore, null);
  assert.equal(inputs.capture.artifactDirectory, receipt.artifactDirectory);
  assert.equal(
    inputs.restore.destinationDirectory,
    receipt.destinationDirectory,
  );
  const checkpointVerified =
    await fixed.publication.verifyCommittedCheckpointArtifact(
      checkpointVerificationInput(inputs.capture),
    );
  assert.equal(
    checkpointVerified.materialization.artifactManifestDigest,
    receipt.artifactProof.artifactManifestDigest,
  );
  const restoreVerified =
    await fixed.publication.verifyCommittedRestoreDestination(
      restoreVerificationInput(inputs.restore),
    );
  assert.equal(
    restoreVerified.materialization.artifactManifestDigest,
    receipt.artifactProof.artifactManifestDigest,
  );
  assert.deepEqual(await readFile(receipt.restoredPayloadPath), PAYLOAD);
  const markerPath = join(storage.dataRoot.rootPath, "podman-writer-ready");
  const restoredMarkerPath = join(
    receipt.destinationDirectory,
    "podman-writer-ready",
  );
  assert.equal(markerPath, receipt.podmanMarkerPath);
  assert.equal(receipt.podmanMarkerSha256, sha256(PODMAN_MARKER));
  assert.deepEqual(await readFile(markerPath), PODMAN_MARKER);
  assert.deepEqual(await readFile(restoredMarkerPath), PODMAN_MARKER);
}

async function syncFileAndDirectory(path) {
  const file = await open(path, "r");
  try {
    await file.sync();
  } finally {
    await file.close();
  }
  const directory = await open(dirname(path), "r");
  try {
    await directory.sync();
  } finally {
    await directory.close();
  }
}

async function assertMountAbsent(inspector, mountPath) {
  const mountPoints = await inspector.listMountPoints();
  assert.equal(
    mountPoints.includes(mountPath),
    false,
    `mount must be absent after clean unmount: ${mountPath}`,
  );
}

async function assertDedicatedMountRoots(inspector, directories) {
  if (!PRIVATE_MOUNT_NAMESPACE) return;
  const mountPoints = await inspector.listMountPoints();
  for (const mountRoot of [directories.archiveMounts, directories.mounts]) {
    assert.equal(
      mountPoints.includes(mountRoot),
      true,
      `dedicated mount root must be a mount point: ${mountRoot}`,
    );
  }
}

async function waitForPeerNamespaceProbe({ archiveMount, storageMount }) {
  if (!PEER_NAMESPACE_BARRIER) return;
  await writeFile(
    PEER_READY_PENDING_PATH,
    `${JSON.stringify({ archiveMount, storageMount })}\n`,
    { flag: "wx", mode: 0o600 },
  );
  await syncFileAndDirectory(PEER_READY_PENDING_PATH);
  await rename(PEER_READY_PENDING_PATH, PEER_READY_PATH);
  await syncFileAndDirectory(PEER_READY_PATH);
  const deadline = Date.now() + 120_000;
  while (Date.now() < deadline) {
    try {
      const release = await readFile(PEER_RELEASE_PATH, "utf8");
      assert.equal(release, "release\n");
      return;
    } catch (error) {
      if (
        error === null ||
        typeof error !== "object" ||
        error.code !== "ENOENT"
      ) {
        throw error;
      }
    }
    await delay(100);
  }
  throw new Error("timed out waiting for the parent mount-namespace probe");
}

async function ensureDirectory(path) {
  await mkdir(path, { mode: 0o700 });
  await assertSafeDirectory(path);
}

async function assertSafeDirectory(path) {
  const metadata = await lstat(path);
  assert.equal(metadata.isDirectory(), true);
  assert.equal(metadata.isSymbolicLink(), false);
  assert.equal(metadata.uid, process.getuid());
  assert.equal(metadata.mode & 0o777, 0o700);
}

async function prepareRoots({ existing }) {
  const precreated = existing || PRIVATE_MOUNT_NAMESPACE;
  if (precreated) await assertSafeDirectory(ROOT);
  else await ensureDirectory(ROOT);
  const ownedRoots = exact({
    archiveMounts: join(ROOT, "archive-mounts"),
    images: join(ROOT, "images"),
    mounts: join(ROOT, "mounts"),
    state: join(ROOT, "state"),
  });
  for (const path of Object.values(ownedRoots)) {
    if (precreated) await assertSafeDirectory(path);
    else await ensureDirectory(path);
  }
  return exact({
    ...ownedRoots,
    archive: join(
      ownedRoots.archiveMounts,
      "publication-archive",
      "publication-artifacts",
    ),
    archiveImage: join(ownedRoots.images, "publication-archive.img"),
    archiveMount: join(ownedRoots.archiveMounts, "publication-archive"),
    journal: join(
      ownedRoots.archiveMounts,
      "publication-archive",
      "operation-journal",
    ),
  });
}

async function createFixture({
  existing,
  expectedArchiveMountPublicationControlIdentity = null,
  expectedArchivePublicationControlIdentity = null,
}) {
  assert.equal(process.platform, "linux");
  assert.equal(typeof process.getuid, "function");
  assert.notEqual(
    process.getuid(),
    0,
    "ext4 test must run as the non-root service user",
  );
  assert.match(
    ROOT,
    /^\/var\/tmp\/portable-codex-runtime-linux-ext4-[1-9][0-9]*$/u,
  );
  const helper = await stat(HELPER);
  assert.equal(helper.isFile(), true);
  const directories = await prepareRoots({ existing });
  const paths = createExt4FilesystemImagePaths({
    archiveRoot: directories.archive,
    backendId: BACKEND_ID,
    imageRoot: directories.images,
    mountRoot: directories.mounts,
  });
  const inspector = createLinuxExt4Inspector({
    helperPath: HELPER,
    trustedRoots: [
      directories.archiveMounts,
      directories.images,
      directories.mounts,
    ],
  });
  await assertDedicatedMountRoots(inspector, directories);
  const driver = createLinuxExt4ImageDriver({ inspector });
  const archiveMountRequest = exact({
    imagePath: directories.archiveImage,
    mountPath: directories.archiveMount,
  });
  if (existing) {
    assert.notEqual(expectedArchiveMountPublicationControlIdentity, null);
    await driver.remount(exact({
      ...archiveMountRequest,
      expectedPublicationControlIdentity:
        expectedArchiveMountPublicationControlIdentity,
    }));
  } else {
    await driver.provision(exact({
      ...archiveMountRequest,
      imageSizeBytes: IMAGE_SIZE_BYTES,
    }));
  }
  const archiveMountControl = await driver.ensurePublicationRoot(exact({
    ...archiveMountRequest,
    expectedPublicationControlIdentity:
      expectedArchiveMountPublicationControlIdentity,
  }));
  const archiveMountPublicationControlIdentity = publicationControlIdentity(
    archiveMountControl.publicationControlIdentity,
  );
  for (const rootPath of [directories.archive, directories.journal]) {
    if (existing) await assertSafeDirectory(rootPath);
    else await ensureDirectory(rootPath);
  }
  const archiveLockPath = join(
    directories.archive,
    STOPPED_DIRECTORY_PUBLICATION_LOCK_NAME,
  );
  if (existing) {
    assert.notEqual(expectedArchivePublicationControlIdentity, null);
    const observed = await inspector.inspectFilesystemObject(archiveLockPath);
    assert.deepEqual(
      exact({
        filesystemId: observed.filesystem.filesystemId,
        objectIdentityScheme: observed.filesystem.objectIdentityScheme,
        objectId: observed.identity.objectId,
      }),
      expectedArchivePublicationControlIdentity,
    );
  }
  const archiveControl = await inspector.provisionControlRoot({
    kind: "publication",
    rootPath: directories.archive,
  });
  const archivePublicationControlIdentity = exact({
    filesystemId: archiveControl.controlFileIdentity.filesystemId,
    objectIdentityScheme:
      archiveControl.controlFileIdentity.objectIdentityScheme,
    objectId: archiveControl.controlFileIdentity.objectId,
  });
  if (expectedArchivePublicationControlIdentity !== null) {
    assert.deepEqual(
      archivePublicationControlIdentity,
      expectedArchivePublicationControlIdentity,
    );
  }
  await inspector.provisionControlRoot({
    kind: "journal",
    rootPath: directories.journal,
  });
  const state = new FilesystemImageProviderState({
    directory: directories.state,
    headAnchor: createProviderStateHeadAnchor(),
  });
  const binding = createExt4PodmanAttachmentBinding({
    backendId: BACKEND_ID,
    driver,
    imageSizeBytes: IMAGE_SIZE_BYTES,
    paths,
    state,
  });
  const backend = binding.backend;
  const journal = new FilesystemOperationJournal({
    directory: directories.journal,
  });
  const resolveExpectedPublicationControl = async (rootPath) => {
    if (rootPath !== directories.archive) {
      const resolved = await Reflect.apply(
        backend.resolveExpectedPublicationControl,
        undefined,
        [rootPath],
      );
      assert.notEqual(
        resolved,
        null,
        "publication target must be an anchored ext4 root",
      );
      return resolved;
    }
    return exact({
      filesystem: archiveControl.filesystem,
      objectId: archivePublicationControlIdentity.objectId,
    });
  };
  const inspectPublicationControl = async (lockPath) => {
    if (lockPath !== archiveLockPath) {
      return Reflect.apply(
        backend.inspectPublicationControl,
        undefined,
        [lockPath],
      );
    }
    return inspector.inspectFilesystemObject(lockPath);
  };
  const publication = new StoppedDirectoryPublication({
    inspectFilesystemObject: (path) =>
      inspector.inspectFilesystemObject(path),
    inspectPublicationControl,
    journal,
    listMountPoints: () => inspector.listMountPoints(),
    resolveExpectedPublicationControl,
  });
  return exact({
    archiveMountPublicationControlIdentity,
    archivePublicationControlIdentity:
      archivePublicationControlIdentity,
    backend,
    directories,
    driver,
    filesystemAuthority: binding.filesystemAuthority,
    inspector,
    journal,
    paths,
    publication,
    state,
  });
}

async function produce() {
  const fixed = await createFixture({
    existing: false,
    expectedArchiveMountPublicationControlIdentity: null,
    expectedArchivePublicationControlIdentity: null,
  });
  assert.deepEqual(await fixed.backend.initialize(), exact({
    status: "initialized",
  }));
  const provisioned = await fixed.backend.lifecycleBackend.provisionSession(
    provisionRequest(),
    context(),
  );
  const attach = attachmentRequest(provisioned.storageId, "001", "1");
  const attached = await fixed.backend.lifecycleBackend.prepareWritableAttachment(
    attach,
    context(),
  );
  const mountedStorage = await fixed.state.readStorage(provisioned.storageId);
  assert.equal(mountedStorage.lifecycle, "attached");
  assert.notEqual(mountedStorage.mount, null);
  await waitForPeerNamespaceProbe({
    archiveMount: fixed.directories.archiveMount,
    storageMount: mountedStorage.mount.mountPath,
  });
  const payloadPath = join(attached.rootPath, "portable.txt");
  await writeFile(payloadPath, PAYLOAD, { flag: "wx", mode: 0o600 });
  await syncFileAndDirectory(payloadPath);
  assert.notEqual(PODMAN_WRITER_IMAGE_DIGEST, null);
  assert.notEqual(PODMAN_WRITER_IMAGE_REFERENCE, null);
  const podmanAttachment = exact({
    attachmentId: attached.target.attachmentId,
    backendId: attached.backendId,
    contractVersion: attached.contractVersion,
    fencingEpoch: attached.fencingEpoch,
    holderId: attached.holderId,
    kind: "directory",
    leaseId: attached.leaseId,
    mode: "read-write",
    operationId: attached.operationId,
    proofId: attached.proofId,
    rootPath: attached.rootPath,
    sessionId: attached.sessionId,
    storageId: attached.storageId,
  });
  // The composition protects the committed persistent identity, the held
  // dev/inode identity, and the exact access policy. The writer marker is
  // intentional child/content churn and must remain permitted.
  const podmanEnvironment = exact({
    HOME: process.env.HOME,
    LANG: "C.UTF-8",
    XDG_RUNTIME_DIR: process.env.XDG_RUNTIME_DIR,
  });
  const podmanWriter = await runExt4PodmanWriterIntegration({
    attachment: podmanAttachment,
    configuredAttachmentRoot: fixed.directories.mounts,
    filesystemAuthority: fixed.filesystemAuthority,
    imageDigest: PODMAN_WRITER_IMAGE_DIGEST,
    imageReference: PODMAN_WRITER_IMAGE_REFERENCE,
    podmanEnvironment,
    podmanExecutable: PODMAN,
    stateRoot: join(ROOT, "podman-state"),
  });
  assert.equal(podmanWriter.servicePid, process.pid);
  assert.equal(podmanWriter.serviceUid, process.getuid());
  await syncFileAndDirectory(podmanWriter.markerPath);
  assert.deepEqual(await readFile(podmanWriter.markerPath), PODMAN_MARKER);
  assert.deepEqual(await readFile(payloadPath), PAYLOAD);
  // This dedicated hosted engine has no remaining workload. Retiring its
  // user-wide pause namespace must be the final Podman call before the ext4
  // attachment and loop devices enter physical quiescence.
  await retireExt4PodmanRootlessNamespaceForConformance({
    exclusiveRootlessEngine: PODMAN_ENGINE_EXCLUSIVE,
    podmanEnvironment,
    podmanExecutable: PODMAN,
  });
  await fixed.backend.lifecycleBackend.detachAttachment(
    detachRequest(attached, "001"),
    context(),
  );
  const storage = await fixed.state.readStorage(provisioned.storageId);
  assert.equal(storage.lifecycle, "detached");
  assert.equal(storage.dataRoot.rootPath, attached.rootPath);
  const publication = await publishAndVerify(fixed, storage);
  const receipt = exact({
    artifactDirectory: publication.artifactDirectory,
    artifactProof: publication.artifactProof,
    archiveMountPublicationControlIdentity:
      fixed.archiveMountPublicationControlIdentity,
    archivePublicationControlIdentity:
      fixed.archivePublicationControlIdentity,
    dataRoot: storage.dataRoot,
    destinationDirectory: publication.destinationDirectory,
    filesystemId: storage.filesystemId,
    payloadPath,
    payloadSha256: sha256(PAYLOAD),
    podmanMarkerPath: podmanWriter.markerPath,
    podmanMarkerSha256: sha256(PODMAN_MARKER),
    providerStateHead,
    restoredPayloadPath: publication.restoredPayloadPath,
    serviceUid: process.getuid(),
    storageId: storage.storageId,
  });
  await fixed.backend.quiesceStorage(storage.storageId);
  await assertMountAbsent(fixed.inspector, storage.mount.mountPath);
  await fixed.driver.quiesce(exact({
    imagePath: fixed.directories.archiveImage,
    mountPath: fixed.directories.archiveMount,
  }));
  await assertMountAbsent(fixed.inspector, fixed.directories.archiveMount);
  await writeFile(TRANSFER_PATH, `${JSON.stringify(receipt)}\n`, {
    flag: "wx",
    mode: 0o600,
  });
  await syncFileAndDirectory(TRANSFER_PATH);
  return receipt;
}

async function consume({ destroy }) {
  const receipt = JSON.parse(await readFile(TRANSFER_PATH, "utf8"));
  const receiptProviderStateHead =
    normalizeFilesystemImageProviderStateHead(receipt.providerStateHead);
  assert.equal(Number.isSafeInteger(receipt.serviceUid), true);
  assert.equal(receipt.serviceUid > 0, true);
  assert.equal(
    receipt.serviceUid,
    process.getuid(),
    "cross-host transfer requires the same numeric service UID",
  );
  if (EXTERNAL_PRODUCER_UID !== null) {
    assert.equal(EXTERNAL_PRODUCER_UID, receipt.serviceUid);
  }
  const receiptArchivePublicationControlIdentity =
    publicationControlIdentity(receipt.archivePublicationControlIdentity);
  const receiptArchiveMountPublicationControlIdentity =
    publicationControlIdentity(
      receipt.archiveMountPublicationControlIdentity,
    );
  assert.deepEqual(
    JSON.parse(JSON.stringify(providerStateHead)),
    JSON.parse(JSON.stringify(receiptProviderStateHead)),
  );
  if (EXTERNAL_ARCHIVE_PUBLICATION_CONTROL_IDENTITY !== null) {
    assert.deepEqual(
      EXTERNAL_ARCHIVE_PUBLICATION_CONTROL_IDENTITY,
      receiptArchivePublicationControlIdentity,
    );
  }
  if (EXTERNAL_ARCHIVE_MOUNT_PUBLICATION_CONTROL_IDENTITY !== null) {
    assert.deepEqual(
      EXTERNAL_ARCHIVE_MOUNT_PUBLICATION_CONTROL_IDENTITY,
      receiptArchiveMountPublicationControlIdentity,
    );
  }
  const fixed = await createFixture({
    existing: true,
    expectedArchiveMountPublicationControlIdentity:
      EXTERNAL_ARCHIVE_MOUNT_PUBLICATION_CONTROL_IDENTITY ??
      receiptArchiveMountPublicationControlIdentity,
    expectedArchivePublicationControlIdentity:
      EXTERNAL_ARCHIVE_PUBLICATION_CONTROL_IDENTITY ??
      receiptArchivePublicationControlIdentity,
  });
  assert.deepEqual(
    fixed.archiveMountPublicationControlIdentity,
    receiptArchiveMountPublicationControlIdentity,
  );
  assert.deepEqual(
    JSON.parse(JSON.stringify(fixed.archivePublicationControlIdentity)),
    JSON.parse(JSON.stringify(receiptArchivePublicationControlIdentity)),
  );
  assert.deepEqual(await fixed.backend.initialize(), exact({
    status: "initialized",
  }));
  const before = await fixed.state.readStorage(receipt.storageId);
  assert.equal(before.lifecycle, "detached");
  assert.equal(before.filesystemId, receipt.filesystemId);
  assert.deepEqual(JSON.parse(JSON.stringify(before.dataRoot)), receipt.dataRoot);
  await verifyTransferredPublication(fixed, before, receipt);
  const attach = attachmentRequest(receipt.storageId, "002", "3");
  const attached = await fixed.backend.lifecycleBackend.prepareWritableAttachment(
    attach,
    context(),
  );
  assert.equal(attached.rootPath, receipt.dataRoot.rootPath);
  const payload = await readFile(receipt.payloadPath);
  assert.equal(sha256(payload), receipt.payloadSha256);
  assert.deepEqual(payload, PAYLOAD);
  await fixed.backend.lifecycleBackend.detachAttachment(
    detachRequest(attached, "002"),
    context(),
  );
  const detached = await fixed.state.readStorage(receipt.storageId);
  assert.equal(detached.lifecycle, "detached");
  assert.deepEqual(JSON.parse(JSON.stringify(detached.dataRoot)), receipt.dataRoot);
  if (destroy) {
    await fixed.backend.lifecycleBackend.destroySession(
      destroyRequest(detached, "001"),
      context(),
    );
    await assertMountAbsent(fixed.inspector, detached.mount.mountPath);
    const destroyed = await fixed.state.readStorage(receipt.storageId);
    assert.equal(destroyed.lifecycle, "destroyed");
    assert.equal(destroyed.mount, null);
    assert.equal(destroyed.dataRoot, null);
    await fixed.driver.destroy(exact({
      imagePath: fixed.directories.archiveImage,
      mountPath: fixed.directories.archiveMount,
    }));
    await assertMountAbsent(fixed.inspector, fixed.directories.archiveMount);
  } else {
    await fixed.backend.quiesceStorage(receipt.storageId);
    await assertMountAbsent(fixed.inspector, detached.mount.mountPath);
    await fixed.driver.quiesce(exact({
      imagePath: fixed.directories.archiveImage,
      mountPath: fixed.directories.archiveMount,
    }));
    await assertMountAbsent(fixed.inspector, fixed.directories.archiveMount);
  }
}

test(`real Linux ext4 physical backend (${MODE})`, async () => {
  if (MODE === "producer") {
    await produce();
    return;
  }
  if (MODE === "consumer") {
    await consume({ destroy: true });
    return;
  }
  assert.equal(MODE, "lifecycle");
  await produce();
  await consume({ destroy: true });
});
