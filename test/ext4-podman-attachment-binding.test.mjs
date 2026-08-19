import assert from "node:assert/strict";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";
import test from "node:test";

import {
  EXT4_PODMAN_ATTACHMENT_BINDING_CONTRACT_VERSION,
  EXT4_PODMAN_PERSISTENT_AUTHORITY_CONTRACT_VERSION,
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
  LINUX_EXT4_ATTACHMENT_ROOT_AUTHORITY_CONTRACT_VERSION,
  LINUX_EXT4_IMAGE_DRIVER_CONTRACT_VERSION,
  LinuxExt4ImageDriverError,
} from "../src/linux-ext4-image-driver.mjs";
import { assertSessionAttachment } from "../src/session-storage-contracts.mjs";

const BACKEND_ID = "ext4-podman-binding-test";
const SESSION_ID = "019f3400-0000-7000-8000-000000000001";
const OTHER_SESSION_ID = "019f3400-0000-7000-8000-000000000002";

function exact(values) {
  return Object.freeze(Object.assign(Object.create(null), values));
}

function identity(
  objectId,
  filesystemId = "ext4fs:binding-test-filesystem-001",
) {
  return exact({
    filesystemId,
    objectIdentityScheme: "linux-ext4-file-handle-sha256-v1",
    objectId,
  });
}

function context() {
  return exact({
    contractVersion: 1,
    invocation: exact({}),
    signal: new AbortController().signal,
  });
}

function createLockProvider() {
  let held = false;
  return async () => {
    assert.equal(held, false);
    held = true;
    let released = false;
    return {
      async assertHeld() {
        assert.equal(held && !released, true);
      },
      async release() {
        assert.equal(released, false);
        released = true;
        held = false;
      },
    };
  };
}

function createHeadAnchor() {
  let head = normalizeFilesystemImageProviderStateHead({
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
  return Object.freeze({
    async compareAndAdvance({ expectedHead, nextHead }) {
      assert.deepEqual(expectedHead, head);
      head = normalizeFilesystemImageProviderStateHead(nextHead);
      return true;
    },
    async readHead() {
      return { ...head };
    },
  });
}

function createFakeDriver() {
  const calls = [];
  const children = new Map();
  const authoritySamples = [];
  const mountRoot = identity("ext4fh1:mount-root-001");
  const image = identity(
    "ext4fh1:image-001",
    "hostfs:binding-test-filesystem-001",
  );
  const publicationControl = identity("ext4fh1:publication-control-001");
  let mounted = false;
  let authorityError = null;
  let authorityHook = null;
  let authorityMalformed = undefined;
  let authorityRootIdentity = null;
  let authorityRuntimeIdentity = exact({ device: "7", inode: "7001" });
  let authoritySettlement = null;
  let surface;

  function mountObservation(request) {
    return exact({
      filesystem: exact({
        durability: "local-fsync-rename",
        filesystemId: mountRoot.filesystemId,
        objectIdentityScheme: mountRoot.objectIdentityScheme,
        type: "ext4",
      }),
      imageIdentity: image,
      imagePath: request.imagePath,
      loopDevice: "/dev/loop7",
      mountEvidence: exact({
        device: "7:7",
        mountId: "77",
        parentMountId: "1",
        propagation: "private",
        root: "/",
        rootDevice: "7",
        rootInode: "7",
      }),
      mountPath: request.mountPath,
      rootIdentity: mountRoot,
    });
  }

  function attachmentObservation(request, rootIdentity) {
    const mount = mountObservation(request);
    return exact({
      attachmentRootPath: request.attachmentRootPath,
      filesystem: mount.filesystem,
      imageIdentity: mount.imageIdentity,
      imagePath: mount.imagePath,
      loopDevice: mount.loopDevice,
      mountEvidence: mount.mountEvidence,
      mountPath: mount.mountPath,
      mountRootIdentity: mount.rootIdentity,
      rootIdentity,
    });
  }

  function method(name, callback) {
    return Object.freeze(async function driverMethod(request) {
      assert.equal(this, surface);
      assert.equal(arguments.length, 1);
      calls.push(name);
      return callback(request);
    });
  }

  const provision = method("provision", (request) => {
    mounted = true;
    return mountObservation(request);
  });
  const observeMount = method("observeMount", (request) => {
    if (!mounted) throw new LinuxExt4ImageDriverError("mount_absent");
    return mountObservation(request);
  });
  const remount = method("remount", (request) => {
    if (mounted) throw new LinuxExt4ImageDriverError("mount_mismatch");
    mounted = true;
    return mountObservation(request);
  });
  const ensureAttachmentRoot = method("ensureAttachmentRoot", (request) => {
    if (!mounted) throw new LinuxExt4ImageDriverError("mount_absent");
    let root = children.get(request.attachmentRootPath);
    const created = root === undefined;
    if (created) {
      root = identity(`ext4fh1:${basename(request.attachmentRootPath)}`);
      children.set(request.attachmentRootPath, root);
    }
    return exact({
      ...attachmentObservation(request, root),
      created,
    });
  });
  const observeAttachmentRoot = method("observeAttachmentRoot", (request) => {
    if (!mounted) throw new LinuxExt4ImageDriverError("mount_absent");
    const root = children.get(request.attachmentRootPath);
    if (root === undefined) {
      throw new LinuxExt4ImageDriverError("attachment_root_absent");
    }
    return attachmentObservation(request, root);
  });
  const ensurePublicationRoot = method("ensurePublicationRoot", (request) => {
    if (!mounted) throw new LinuxExt4ImageDriverError("mount_absent");
    const mount = mountObservation(request);
    return exact({
      controlFileIdentity: exact({
        device: "7",
        inode: "71",
        ...publicationControl,
      }),
      controlFileName: ".stopped-directory-publication.lock",
      created: request.expectedPublicationControlIdentity === null,
      filesystem: mount.filesystem,
      imageIdentity: mount.imageIdentity,
      imagePath: mount.imagePath,
      loopDevice: mount.loopDevice,
      mountEvidence: mount.mountEvidence,
      mountPath: mount.mountPath,
      mountRootIdentity: mount.rootIdentity,
      publicationControlIdentity: publicationControl,
    });
  });
  const syncFilesystem = method("syncFilesystem", (request) =>
    exact({ mount: mountObservation(request), status: "synced" }));
  const quiesce = method("quiesce", (request) => {
    mounted = false;
    return exact({
      imagePath: request.imagePath,
      mountPath: request.mountPath,
      status: "quiesced",
    });
  });
  const destroy = method("destroy", (request) => {
    mounted = false;
    return exact({
      imagePath: request.imagePath,
      mountPath: request.mountPath,
      status: "destroyed",
    });
  });
  const observeAttachmentRootAuthority = Object.freeze(
    function observeAttachmentRootAuthority(request) {
      assert.equal(this, surface);
      assert.equal(arguments.length, 1);
      calls.push("observeAttachmentRootAuthority");
      if (authoritySettlement !== null) {
        return authoritySettlement(request);
      }
      return (async () => {
        const hook = authorityHook;
        authorityHook = null;
        if (hook !== null) await hook(request);
        if (authorityError !== null) throw authorityError;
        if (authorityMalformed !== undefined) return authorityMalformed;
        if (!mounted) throw new LinuxExt4ImageDriverError("mount_absent");
        const storedRoot = children.get(request.attachmentRootPath);
        if (storedRoot === undefined) {
          throw new LinuxExt4ImageDriverError("attachment_root_absent");
        }
        const rootRuntimeIdentity = authorityRuntimeIdentity;
        authoritySamples.push(rootRuntimeIdentity);
        return exact({
          ...attachmentObservation(
            request,
            authorityRootIdentity ?? storedRoot,
          ),
          rootRuntimeIdentity,
        });
      })();
    },
  );

  surface = exact({
    attachmentRootAuthorityContractVersion:
      LINUX_EXT4_ATTACHMENT_ROOT_AUTHORITY_CONTRACT_VERSION,
    contractVersion: LINUX_EXT4_IMAGE_DRIVER_CONTRACT_VERSION,
    destroy,
    ensureAttachmentRoot,
    ensurePublicationRoot,
    observeAttachmentRoot,
    observeAttachmentRootAuthority,
    observeMount,
    provision,
    quiesce,
    remount,
    syncFilesystem,
  });
  return {
    authoritySamples,
    calls,
    children,
    driver: surface,
    image,
    mountRoot,
    rejectAuthority(error) {
      authorityError = error;
    },
    replaceAuthorityRoot(rootIdentity) {
      authorityRootIdentity = rootIdentity;
    },
    returnMalformedAuthority(value) {
      authorityMalformed = value;
    },
    setAuthorityHook(hook) {
      authorityHook = hook;
    },
    setAuthorityRuntimeIdentity(runtimeIdentity) {
      authorityRuntimeIdentity = exact(runtimeIdentity);
    },
    setAuthoritySettlement(settlement) {
      authoritySettlement = settlement;
    },
  };
}

function provisionRequest(operationId = "provision-operation-001") {
  return {
    backendId: BACKEND_ID,
    contractVersion: 1,
    operationId,
    sessionId: SESSION_ID,
  };
}

function mutationRequest(operation, storageId, overrides = {}) {
  const attachmentId = overrides.attachmentId ?? "attachment-001";
  return {
    backendId: BACKEND_ID,
    contractVersion: 1,
    fencingEpoch: overrides.fencingEpoch ?? "1",
    holderId: overrides.holderId ?? "holder-001",
    leaseId: overrides.leaseId ?? "lease-001",
    operation,
    operationId: overrides.operationId ?? `${operation}-operation-001`,
    sessionId: SESSION_ID,
    storageId,
    target: { attachmentId, kind: "attachment" },
  };
}

function attachmentFromResult(result) {
  return assertSessionAttachment({
    attachmentId: result.target.attachmentId,
    backendId: result.backendId,
    contractVersion: result.contractVersion,
    fencingEpoch: result.fencingEpoch,
    holderId: result.holderId,
    kind: "directory",
    leaseId: result.leaseId,
    mode: "read-write",
    operationId: result.operationId,
    proofId: result.proofId,
    rootPath: result.rootPath,
    sessionId: result.sessionId,
    storageId: result.storageId,
  });
}

async function fixture(t) {
  const root = await mkdtemp(join(tmpdir(), "ext4-podman-binding-test-"));
  t.after(() => rm(root, { force: true, recursive: true }));
  const directories = exact({
    archive: join(root, "archive"),
    images: join(root, "images"),
    mounts: join(root, "mounts"),
    state: join(root, "state"),
  });
  for (const directory of Object.values(directories)) {
    await mkdir(directory, { mode: 0o700 });
  }
  const paths = createExt4FilesystemImagePaths({
    archiveRoot: directories.archive,
    backendId: BACKEND_ID,
    imageRoot: directories.images,
    mountRoot: directories.mounts,
  });
  const state = new FilesystemImageProviderState({
    acquireLock: createLockProvider(),
    directory: directories.state,
    headAnchor: createHeadAnchor(),
    inspectAncestorAcl: async () => false,
    inspectDirectoryAcl: async () => false,
  });
  const fake = createFakeDriver();
  const binding = createExt4PodmanAttachmentBinding({
    backendId: BACKEND_ID,
    driver: fake.driver,
    imageSizeBytes: 16 * 1024 * 1024,
    paths,
    state,
  });
  const provisioned = await binding.backend.lifecycleBackend.provisionSession(
    provisionRequest(),
    context(),
  );
  const attachRequest = mutationRequest("attach", provisioned.storageId);
  const attached = await binding.backend.lifecycleBackend.prepareWritableAttachment(
    attachRequest,
    context(),
  );
  return {
    attachRequest,
    attachment: attachmentFromResult(attached),
    binding,
    fake,
    provisioned,
    root,
    state,
  };
}

function verify(fixed, attachment = fixed.attachment) {
  return Reflect.apply(fixed.binding.attachmentAuthority.verify, undefined, [
    exact({ attachment }),
  ]);
}

function assertNullReceipt(receipt, status) {
  assert.deepEqual(receipt, exact({
    bindingSha256: null,
    rootRuntimeIdentity: null,
    status,
  }));
}

test("exposes exact frozen binding, authority, backend, and composition surfaces", async (t) => {
  const fixed = await fixture(t);

  assert.equal(Object.getPrototypeOf(fixed.binding), null);
  assert.equal(Object.isFrozen(fixed.binding), true);
  assert.deepEqual(Object.keys(fixed.binding).sort(), [
    "attachmentAuthority",
    "backend",
    "contractVersion",
    "filesystemAuthority",
  ]);
  assert.equal(
    fixed.binding.contractVersion,
    EXT4_PODMAN_ATTACHMENT_BINDING_CONTRACT_VERSION,
  );

  const authority = fixed.binding.attachmentAuthority;
  assert.equal(Object.getPrototypeOf(authority), null);
  assert.equal(Object.isFrozen(authority), true);
  assert.deepEqual(Object.keys(authority).sort(), ["contractVersion", "verify"]);
  assert.equal(
    authority.contractVersion,
    EXT4_PODMAN_PERSISTENT_AUTHORITY_CONTRACT_VERSION,
  );
  assert.equal(Object.isFrozen(authority.verify), true);

  assert.equal(Object.getPrototypeOf(fixed.binding.backend), null);
  assert.equal(Object.isFrozen(fixed.binding.backend), true);
  assert.deepEqual(Object.keys(fixed.binding.backend).sort(), [
    "initialize",
    "inspectPublicationControl",
    "lifecycleBackend",
    "quiesceStorage",
    "resolveExpectedPublicationControl",
    "resolveRestoreDestination",
  ]);

  const composition = fixed.binding.filesystemAuthority;
  assert.equal(Object.getPrototypeOf(composition), null);
  assert.equal(Object.isFrozen(composition), true);
  assert.deepEqual(Object.keys(composition).sort(), [
    "acquire",
    "close",
    "contractVersion",
    "verifyCurrent",
    "verifyRunningMount",
  ]);
  assert.equal(composition.contractVersion, 1);
});

test("committed attachment returns a stable binding and same-sample runtime identity", async (t) => {
  const fixed = await fixture(t);

  const first = await verify(fixed);
  assert.equal(first.status, "current");
  assert.match(first.bindingSha256, /^[0-9a-f]{64}$/u);
  assert.deepEqual(first.rootRuntimeIdentity, exact({
    device: "7",
    inode: "7001",
  }));
  assert.strictEqual(
    fixed.fake.authoritySamples.at(-1),
    fixed.fake.authoritySamples[0],
  );
  assert.deepEqual(first.rootRuntimeIdentity, fixed.fake.authoritySamples[0]);

  fixed.fake.setAuthorityRuntimeIdentity({ device: "7", inode: "7002" });
  const second = await verify(fixed);
  assert.equal(second.status, "current");
  assert.equal(second.bindingSha256, first.bindingSha256);
  assert.deepEqual(second.rootRuntimeIdentity, exact({
    device: "7",
    inode: "7002",
  }));
  assert.deepEqual(second.rootRuntimeIdentity, fixed.fake.authoritySamples[1]);
  assert.equal(Object.getPrototypeOf(second), null);
  assert.equal(Object.isFrozen(second), true);
  assert.equal(Object.getPrototypeOf(second.rootRuntimeIdentity), null);
  assert.equal(Object.isFrozen(second.rootRuntimeIdentity), true);
});

test("valid mutations of the committed SessionAttachment tuple are mismatches", async (t) => {
  const fixed = await fixture(t);
  const mutations = {
    attachmentId: "attachment-replaced",
    backendId: "ext4-podman-binding-replaced",
    fencingEpoch: "2",
    holderId: "holder-replaced",
    leaseId: "lease-replaced",
    proofId: "proof-replaced",
    rootPath: join(fixed.attachment.rootPath, "replacement"),
    sessionId: OTHER_SESSION_ID,
    storageId: "storage-replaced",
  };

  for (const [key, value] of Object.entries(mutations)) {
    const mutated = assertSessionAttachment({
      ...fixed.attachment,
      [key]: value,
    });
    assertNullReceipt(await verify(fixed, mutated), "mismatch");
  }
});

test("absent origin operation and detached current storage are missing", async (t) => {
  const fixed = await fixture(t);
  const absent = assertSessionAttachment({
    ...fixed.attachment,
    operationId: "attach-operation-absent",
  });
  assertNullReceipt(await verify(fixed, absent), "missing");

  await fixed.binding.backend.lifecycleBackend.detachAttachment(
    mutationRequest("detach", fixed.provisioned.storageId),
    context(),
  );
  const authorityCallsBefore = fixed.fake.calls.filter(
    (name) => name === "observeAttachmentRootAuthority",
  ).length;
  assertNullReceipt(await verify(fixed), "missing");
  assert.equal(
    fixed.fake.calls.filter(
      (name) => name === "observeAttachmentRootAuthority",
    ).length,
    authorityCallsBefore,
  );
});

test("maps only conclusive driver absence and mismatch errors to receipts", async (t) => {
  const cases = [
    ["attachment_root_absent", "missing"],
    ["attachment_root_unsafe", "mismatch"],
    ["access_policy_mismatch", "mismatch"],
    ["mount_absent", "mismatch"],
    ["mount_mismatch", "mismatch"],
  ];
  for (const [code, status] of cases) {
    await t.test(code, async (t) => {
      const fixed = await fixture(t);
      fixed.fake.rejectAuthority(new LinuxExt4ImageDriverError(code));
      assertNullReceipt(await verify(fixed), status);
    });
  }
});

test("rejects unreadable, malformed, and non-native authority settlements", async (t) => {
  await t.test("unreadable native Promise rejection", async (t) => {
    const fixed = await fixture(t);
    const failure = new LinuxExt4ImageDriverError("inspection_failed");
    fixed.fake.rejectAuthority(failure);
    await assert.rejects(verify(fixed), (error) => error === failure);
  });

  await t.test("malformed observation", async (t) => {
    const fixed = await fixture(t);
    fixed.fake.returnMalformedAuthority(exact({}));
    await assert.rejects(verify(fixed), TypeError);
  });

  await t.test("ordinary thenable", async (t) => {
    const fixed = await fixture(t);
    let executions = 0;
    fixed.fake.setAuthoritySettlement(() => ({
      then() {
        executions += 1;
        throw new Error("thenable must not execute");
      },
    }));
    await assert.rejects(verify(fixed), TypeError);
    assert.equal(executions, 0);
  });

  await t.test("Promise subclass", async (t) => {
    const fixed = await fixture(t);
    let executions = 0;
    class AuthorityPromise extends Promise {
      then(...args) {
        executions += 1;
        return super.then(...args);
      }
    }
    const settlement = AuthorityPromise.resolve(exact({}));
    fixed.fake.setAuthoritySettlement(() => settlement);
    await assert.rejects(verify(fixed), TypeError);
    assert.equal(executions, 0);
  });
});

test("persistent root identity mismatch is conclusive", async (t) => {
  const fixed = await fixture(t);
  fixed.fake.replaceAuthorityRoot(
    identity("ext4fh1:replacement-root-001"),
  );
  assertNullReceipt(await verify(fixed), "mismatch");
});

test("state transition between C1 and C2 fails closed as mismatch", async (t) => {
  const fixed = await fixture(t);
  fixed.fake.setAuthorityHook(async () => {
    await fixed.binding.backend.lifecycleBackend.detachAttachment(
      mutationRequest("detach", fixed.provisioned.storageId, {
        operationId: "racing-detach-operation-001",
      }),
      context(),
    );
  });

  assertNullReceipt(await verify(fixed), "mismatch");
  assert.equal(
    (await fixed.state.readStorage(fixed.provisioned.storageId)).lifecycle,
    "detached",
  );
});

test("child-entry and content churn do not invalidate committed identity", async (t) => {
  const fixed = await fixture(t);
  await mkdir(fixed.attachment.rootPath, { mode: 0o700, recursive: true });
  fixed.fake.setAuthorityHook(async () => {
    await writeFile(
      join(fixed.attachment.rootPath, "live-child.txt"),
      "writer content\n",
      { mode: 0o600 },
    );
  });

  const current = await verify(fixed);
  assert.equal(current.status, "current");
  assert.match(current.bindingSha256, /^[0-9a-f]{64}$/u);
  assert.deepEqual(current.rootRuntimeIdentity, exact({
    device: "7",
    inode: "7001",
  }));
});
