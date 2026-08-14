import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { mkdir, mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, join } from "node:path";
import test from "node:test";

import {
  Ext4FilesystemImageBackendError,
  createExt4FilesystemImageBackend,
  createInitializedExt4FilesystemImageBackend,
} from "../src/ext4-filesystem-image-backend.mjs";
import {
  createExt4FilesystemImagePaths,
} from "../src/ext4-filesystem-image-paths.mjs";
import {
  FILESYSTEM_IMAGE_PROVIDER_STATE_HEAD_CONTRACT_VERSION,
  FILESYSTEM_IMAGE_PROVIDER_STATE_LEDGER_NAME,
  FilesystemImageProviderState,
  normalizeFilesystemImageProviderStateHead,
} from "../src/filesystem-image-provider-state.mjs";
import {
  LINUX_EXT4_IMAGE_DRIVER_CONTRACT_VERSION,
  LinuxExt4ImageDriverError,
} from "../src/linux-ext4-image-driver.mjs";
import { createSessionManifest } from "../src/session-storage-contracts.mjs";

const BACKEND_ID = "ext4-test-backend";
const SESSION_ID = "019f2100-0000-7000-8000-000000000001";
const THREAD_ID = "019f2100-0000-7000-8000-000000000002";
const IMAGE_DIGEST = `sha256:${"a".repeat(64)}`;

function exact(values) {
  return Object.freeze(Object.assign(Object.create(null), values));
}

function sha256Json(value) {
  return createHash("sha256").update(JSON.stringify(value)).digest("hex");
}

function backendError(code) {
  return (error) =>
    error instanceof Ext4FilesystemImageBackendError && error.code === code;
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

function context() {
  return exact({
    contractVersion: 1,
    invocation: exact({}),
    signal: new AbortController().signal,
  });
}

function identity(
  objectId,
  filesystemId = "ext4fs:test-filesystem-001",
) {
  return exact({
    filesystemId,
    objectIdentityScheme: "linux-ext4-file-handle-sha256-v1",
    objectId,
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
    lastChecksum: null,
    ledgerBytes: 0,
    sequence: 0,
  });
  return Object.freeze({
    async compareAndAdvance({ expectedHead, nextHead }) {
      if (
        expectedHead.contractVersion !== head.contractVersion ||
        expectedHead.lastChecksum !== head.lastChecksum ||
        expectedHead.ledgerBytes !== head.ledgerBytes ||
        expectedHead.sequence !== head.sequence
      ) {
        return false;
      }
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
  let mounted = false;
  const mountRoot = identity("ext4fh1:mount-root-001");
  let publicationControl = identity("ext4fh1:publication-control-001");
  const image = identity(
    "ext4fh1:image-001",
    "hostfs:test-filesystem-001",
  );
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

  const method = (name, callback) =>
    Object.freeze(async function driverMethod(request) {
      assert.equal(this, surface);
      assert.equal(arguments.length, 1);
      calls.push(name);
      return callback(request);
    });

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
    assert.deepEqual(
      { ...request.expectedPublicationControlIdentity },
      { ...publicationControl },
    );
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
    assert.deepEqual(
      request.expectedPublicationControlIdentity,
      request.expectedPublicationControlIdentity === null
        ? null
        : publicationControl,
    );
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

  surface = exact({
    contractVersion: LINUX_EXT4_IMAGE_DRIVER_CONTRACT_VERSION,
    destroy,
    ensureAttachmentRoot,
    ensurePublicationRoot,
    observeAttachmentRoot,
    observeMount,
    provision,
    quiesce,
    remount,
    syncFilesystem,
  });
  return {
    calls,
    children,
    driver: surface,
    image,
    mountRoot,
    publish(path, root = identity(`ext4fh1:${basename(path)}`)) {
      children.set(path, root);
      return root;
    },
    replacePublicationControl(
      next = identity("ext4fh1:publication-control-replaced"),
    ) {
      publicationControl = next;
    },
    setMounted(value) {
      mounted = value;
    },
  };
}

async function fixture(t, { driverMethodOverrides } = {}) {
  const root = await mkdtemp(join(tmpdir(), "ext4-backend-test-"));
  t.after(() => rm(root, { force: true, recursive: true }));
  const directories = {
    archive: join(root, "archive"),
    images: join(root, "images"),
    mounts: join(root, "mounts"),
    state: join(root, "state"),
  };
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
  const driver =
    driverMethodOverrides === undefined
      ? fake.driver
      : exact({ ...fake.driver, ...driverMethodOverrides });
  const backend = createExt4FilesystemImageBackend({
    backendId: BACKEND_ID,
    driver,
    imageSizeBytes: 16 * 1024 * 1024,
    paths,
    state,
  });
  return {
    backend,
    fake,
    ledgerPath: join(
      directories.state,
      FILESYSTEM_IMAGE_PROVIDER_STATE_LEDGER_NAME,
    ),
    paths,
    root,
    state,
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
  const targets = {
    attach: { attachmentId, kind: "attachment" },
    destroy: { kind: "storage", storageId },
    detach: { attachmentId, kind: "attachment" },
  };
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
    target: targets[operation],
  };
}

function restoreMutation(storageId, operationId = "restore-operation-001") {
  return {
    backendId: BACKEND_ID,
    contractVersion: 1,
    fencingEpoch: "2",
    holderId: "restore-holder-001",
    leaseId: "restore-lease-001",
    operation: "restore",
    operationId,
    sessionId: SESSION_ID,
    storageId,
    target: {
      artifactId: "artifact-001",
      checkpointId: "checkpoint-001",
      kind: "checkpoint",
    },
  };
}

async function provision(fixed) {
  return fixed.backend.lifecycleBackend.provisionSession(
    provisionRequest(),
    context(),
  );
}

async function assertStaleMutationHasNoEffect(fixed, operation) {
  const ledgerBefore = await readFile(fixed.ledgerPath);
  const driverCallsBefore = fixed.fake.calls.length;
  await assert.rejects(operation(), backendError("physical_state_mismatch"));
  assert.deepEqual(await readFile(fixed.ledgerPath), ledgerBefore);
  assert.equal(fixed.fake.calls.length, driverCallsBefore);
}

function manifest() {
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

function activationRequest(storageId, rootPath, rootIdentity, suffix = "001") {
  const lease = {
    contractVersion: 1,
    expiresAt: "2026-08-05T00:13:00.000Z",
    fencingEpoch: "1",
    holderId: `activation-holder-${suffix}`,
    leaseId: `activation-lease-${suffix}`,
    sessionId: SESSION_ID,
  };
  return {
    contractVersion: 1,
    lease,
    manifest: manifest(),
    mutationRequest: {
      backendId: BACKEND_ID,
      contractVersion: 1,
      fencingEpoch: lease.fencingEpoch,
      holderId: lease.holderId,
      leaseId: lease.leaseId,
      operation: "attach",
      operationId: `activation-operation-${suffix}`,
      sessionId: SESSION_ID,
      storageId,
      target: {
        attachmentId: `activation-attachment-${suffix}`,
        kind: "attachment",
      },
    },
    publication: {
      artifactManifestDigest: "b".repeat(64),
      coordinatorBindingSha256: "c".repeat(64),
      modeledDigest: "d".repeat(64),
      publicationId: `restore-publication-${suffix}`,
      publicationKind: "restore-destination",
      root: { ...rootIdentity, rootPath },
      treeIdentityDigest: "e".repeat(64),
    },
    storageRef: {
      backendId: BACKEND_ID,
      contractVersion: 1,
      sessionId: SESSION_ID,
      storageId,
    },
  };
}

test("exposes exact receiver-authentic raw surfaces and rejects hostile inputs", async (t) => {
  const fixed = await fixture(t);
  assert.equal(Object.getPrototypeOf(fixed.backend), null);
  assert.equal(Object.isFrozen(fixed.backend), true);
  assert.deepEqual(Object.keys(fixed.backend).sort(), [
    "initialize",
    "inspectPublicationControl",
    "lifecycleBackend",
    "quiesceStorage",
    "resolveExpectedPublicationControl",
    "resolveRestoreDestination",
  ]);
  assert.deepEqual(fixed.backend.lifecycleBackend.capabilities, exact({
    atomicPointInTimeCheckpoint: false,
    exclusiveWriterAttachment: true,
    fencing: "manual",
    normalDirectoryAttachment: true,
  }));
  assert.equal(Object.getPrototypeOf(fixed.backend.lifecycleBackend), null);
  assert.equal(Object.isFrozen(fixed.backend.lifecycleBackend), true);

  const detached = fixed.backend.lifecycleBackend.provisionSession;
  assert.throws(
    () => detached(provisionRequest(), context()),
    TypeError,
  );
  assert.throws(() => fixed.backend.initialize.call(null), TypeError);

  let traps = 0;
  const hostile = new Proxy(provisionRequest(), {
    get() {
      traps += 1;
      throw new Error("proxy trap must not run");
    },
  });
  const pending = fixed.backend.lifecycleBackend.provisionSession(
    hostile,
    context(),
  );
  assert.equal(Object.getPrototypeOf(pending), Promise.prototype);
  await assert.rejects(pending, backendError("invalid_request"));
  assert.equal(traps, 0);

  let reads = 0;
  const accessor = provisionRequest("accessor-operation-001");
  Object.defineProperty(accessor, "backendId", {
    enumerable: true,
    get() {
      reads += 1;
      return BACKEND_ID;
    },
  });
  await assert.rejects(
    fixed.backend.lifecycleBackend.provisionSession(accessor, context()),
    backendError("invalid_request"),
  );
  assert.equal(reads, 0);
});

test("driver effects require exact native Promise settlements", async (t) => {
  let index = 0;
  for (const unsafe of promiseSettlementCases(null)) {
    await t.test(unsafe.name, async (t) => {
      const settlement = unsafe.create();
      let driverCalls = 0;
      const provision = Object.freeze(function provision() {
        driverCalls += 1;
        return settlement.value;
      });
      const fixed = await fixture(t, {
        driverMethodOverrides: { provision },
      });
      index += 1;
      await assert.rejects(
        fixed.backend.lifecycleBackend.provisionSession(
          provisionRequest(`hostile-driver-settlement-${index}`),
          context(),
        ),
        backendError("physical_effect_ambiguous"),
      );
      assert.equal(driverCalls, 1);
      settlement.assertUntouched();
    });
  }
});

test("initialized wrapper gates lifecycle dispatch once and preserves contracts", async (t) => {
  const fixed = await fixture(t);
  const provisioned = await provision(fixed);
  await fixed.backend.quiesceStorage(provisioned.storageId);
  const backend = createInitializedExt4FilesystemImageBackend({
    backend: fixed.backend,
  });

  assert.deepEqual(Object.keys(backend), Object.keys(fixed.backend));
  assert.equal(Object.getPrototypeOf(backend), null);
  assert.equal(Object.isFrozen(backend), true);
  assert.deepEqual(
    Object.keys(backend.lifecycleBackend),
    Object.keys(fixed.backend.lifecycleBackend),
  );
  assert.equal(Object.getPrototypeOf(backend.lifecycleBackend), null);
  assert.equal(Object.isFrozen(backend.lifecycleBackend), true);
  for (const name of [
    "backendId",
    "capabilities",
    "contractVersion",
    "physicalInvocationContractVersion",
    "restoreAttachmentActivationContractVersion",
    "restoreAttachmentReconciliationContractVersion",
  ]) {
    assert.strictEqual(
      backend.lifecycleBackend[name],
      fixed.backend.lifecycleBackend[name],
    );
  }
  for (const name of [
    "captureCheckpoint",
    "destroySession",
    "detachAttachment",
    "forceFence",
    "prepareRestoreAttachment",
    "prepareWritableAttachment",
    "provisionSession",
    "reconcileRestoreAttachment",
    "restoreCheckpoint",
  ]) {
    assert.equal(
      backend.lifecycleBackend[name].length,
      fixed.backend.lifecycleBackend[name].length,
    );
    assert.equal(Object.isFrozen(backend.lifecycleBackend[name]), true);
    assert.throws(
      () => Reflect.apply(backend.lifecycleBackend[name], null, [{}, context()]),
      TypeError,
    );
  }
  assert.equal(backend.initialize.length, 0);
  assert.equal(backend.quiesceStorage.length, 1);
  assert.equal(backend.resolveRestoreDestination.length, 1);
  assert.throws(() => backend.initialize.call(null), TypeError);
  assert.throws(() => backend.quiesceStorage.call(null, "storage-001"), TypeError);
  assert.throws(
    () => Reflect.apply(backend.resolveRestoreDestination, backend, [{}]),
    TypeError,
  );

  const callsBeforeAttach = fixed.fake.calls.length;
  await backend.lifecycleBackend.prepareWritableAttachment(
    mutationRequest("attach", provisioned.storageId),
    context(),
  );
  assert.deepEqual(fixed.fake.calls.slice(callsBeforeAttach), [
    "observeMount",
    "remount",
    "observeMount",
    "ensurePublicationRoot",
    "ensureAttachmentRoot",
  ]);

  const firstInitialization = backend.initialize();
  assert.strictEqual(backend.initialize(), firstInitialization);
  assert.deepEqual(await firstInitialization, exact({ status: "initialized" }));
  const callsBeforeDetach = fixed.fake.calls.length;
  await backend.lifecycleBackend.detachAttachment(
    mutationRequest("detach", provisioned.storageId),
    context(),
  );
  assert.deepEqual(fixed.fake.calls.slice(callsBeforeDetach), ["syncFilesystem"]);
});

test("initialized wrapper permanently blocks lifecycle and resolver after cold-open failure", async (t) => {
  const fixed = await fixture(t);
  const provisioned = await provision(fixed);
  await fixed.backend.lifecycleBackend.prepareWritableAttachment(
    mutationRequest("attach", provisioned.storageId),
    context(),
  );
  fixed.fake.setMounted(false);
  const backend = createInitializedExt4FilesystemImageBackend({
    backend: fixed.backend,
  });
  const ledgerBefore = await readFile(fixed.ledgerPath);
  const callsBeforeFailure = fixed.fake.calls.length;
  const detachRequest = mutationRequest("detach", provisioned.storageId);
  await assert.rejects(
    backend.lifecycleBackend.detachAttachment(detachRequest, context()),
    backendError("cold_open_failed"),
  );
  assert.deepEqual(fixed.fake.calls.slice(callsBeforeFailure), ["observeMount"]);
  assert.deepEqual(await readFile(fixed.ledgerPath), ledgerBefore);

  fixed.fake.setMounted(true);
  const callsBeforeRetry = fixed.fake.calls.length;
  await assert.rejects(
    backend.lifecycleBackend.detachAttachment(
      structuredClone(detachRequest),
      context(),
    ),
    backendError("cold_open_failed"),
  );
  let proxyReads = 0;
  const hostileResolverInput = new Proxy({}, {
    get() {
      proxyReads += 1;
      throw new Error("resolver must remain behind initialization");
    },
  });
  await assert.rejects(
    Reflect.apply(backend.resolveRestoreDestination, undefined, [
      hostileResolverInput,
    ]),
    backendError("cold_open_failed"),
  );
  assert.equal(proxyReads, 0);
  assert.equal(fixed.fake.calls.length, callsBeforeRetry);
  assert.deepEqual(await readFile(fixed.ledgerPath), ledgerBefore);

  const failedInitialization = backend.initialize();
  assert.strictEqual(backend.initialize(), failedInitialization);
  await assert.rejects(
    failedInitialization,
    backendError("cold_open_failed"),
  );
});

test("provisions through prepare/effect/commit and replays the original result", async (t) => {
  const fixed = await fixture(t);
  const request = provisionRequest();
  const first = await fixed.backend.lifecycleBackend.provisionSession(
    request,
    context(),
  );
  const replay = await fixed.backend.lifecycleBackend.provisionSession(
    structuredClone(request),
    context(),
  );
  assert.deepEqual(replay, first);
  assert.equal(fixed.fake.calls.filter((call) => call === "provision").length, 1);
  const storage = await fixed.state.readStorage(first.storageId);
  assert.equal(storage.lifecycle, "provisioned");
  assert.equal(storage.revision, "1");
  assert.equal(storage.writerEpoch, "0");
  assert.equal(storage.writerAuthority, null);
  assert.equal(storage.dataRoot, null);

  await assert.rejects(
    fixed.backend.lifecycleBackend.provisionSession(
      { ...request, sessionId: "019f2100-0000-7000-8000-000000000099" },
      context(),
    ),
    (error) => error?.code === "operation_conflict",
  );
});

test("binds publication control callbacks to anchored provider state", async (t) => {
  const fixed = await fixture(t);
  assert.throws(
    () => Reflect.apply(
      fixed.backend.resolveExpectedPublicationControl,
      fixed.backend,
      [join(fixed.root, "archive")],
    ),
    TypeError,
  );
  assert.equal(
    await Reflect.apply(
      fixed.backend.resolveExpectedPublicationControl,
      undefined,
      [join(fixed.root, "archive")],
    ),
    null,
  );

  const provisioned = await provision(fixed);
  const storage = await fixed.state.readStorage(provisioned.storageId);
  const expected = await Reflect.apply(
    fixed.backend.resolveExpectedPublicationControl,
    undefined,
    [storage.mount.mountPath],
  );
  assert.deepEqual(expected, exact({
    filesystem: exact({
      durability: "local-fsync-rename",
      filesystemId: storage.publicationControlIdentity.filesystemId,
      objectIdentityScheme:
        storage.publicationControlIdentity.objectIdentityScheme,
      type: "ext4",
    }),
    objectId: storage.publicationControlIdentity.objectId,
  }));
  const inspected = await Reflect.apply(
    fixed.backend.inspectPublicationControl,
    undefined,
    [join(
      storage.mount.mountPath,
      ".stopped-directory-publication.lock",
    )],
  );
  assert.deepEqual(inspected, exact({
    filesystem: expected.filesystem,
    identity: exact({
      device: "7",
      inode: "71",
      objectId: expected.objectId,
    }),
  }));
});

test("a prepared provision retry observes the exact effect before commit", async (t) => {
  const fixed = await fixture(t);
  const request = provisionRequest("prepared-provision-001");
  const plan = fixed.paths.planProvision(request);
  await fixed.state.prepareOperation({
    kind: "provision",
    operationId: request.operationId,
    request,
    storageId: plan.storageId,
  });
  fixed.fake.setMounted(true);
  const result = await fixed.backend.lifecycleBackend.provisionSession(
    request,
    context(),
  );
  assert.equal(result.storageId, plan.storageId);
  assert.equal(fixed.fake.calls.includes("observeMount"), true);
  assert.equal(fixed.fake.calls.includes("provision"), false);
});

test("detach preserves dataRoot, reattach reuses it, and destroy tombstones storage", async (t) => {
  const fixed = await fixture(t);
  const provisioned = await provision(fixed);
  const firstRequest = mutationRequest("attach", provisioned.storageId);
  const first = await fixed.backend.lifecycleBackend.prepareWritableAttachment(
    firstRequest,
    context(),
  );
  const firstStorage = await fixed.state.readStorage(provisioned.storageId);
  assert.equal(firstStorage.dataRoot.rootPath, first.rootPath);

  await fixed.backend.lifecycleBackend.detachAttachment(
    mutationRequest("detach", provisioned.storageId),
    context(),
  );
  const detached = await fixed.state.readStorage(provisioned.storageId);
  assert.equal(detached.lifecycle, "detached");
  assert.equal(detached.dataRoot.rootPath, first.rootPath);
  assert.deepEqual(detached.writerAuthority, {
    fencingEpoch: "1",
    holderId: "holder-001",
    leaseId: "lease-001",
  });

  const secondRequest = mutationRequest("attach", provisioned.storageId, {
    attachmentId: "attachment-002",
    fencingEpoch: "2",
    holderId: "holder-002",
    leaseId: "lease-002",
    operationId: "attach-operation-002",
  });
  const second = await fixed.backend.lifecycleBackend.prepareWritableAttachment(
    secondRequest,
    context(),
  );
  assert.equal(second.rootPath, first.rootPath);
  assert.equal(
    fixed.fake.calls.filter((call) => call === "ensureAttachmentRoot").length,
    1,
  );
  await fixed.backend.lifecycleBackend.detachAttachment(
    mutationRequest("detach", provisioned.storageId, {
      attachmentId: "attachment-002",
      fencingEpoch: "2",
      holderId: "holder-002",
      leaseId: "lease-002",
      operationId: "detach-operation-002",
    }),
    context(),
  );
  await fixed.backend.lifecycleBackend.destroySession(
    mutationRequest("destroy", provisioned.storageId, {
      fencingEpoch: "2",
      holderId: "holder-002",
      leaseId: "lease-002",
      operationId: "destroy-operation-001",
    }),
    context(),
  );
  const destroyed = await fixed.state.readStorage(provisioned.storageId);
  assert.equal(destroyed.lifecycle, "destroyed");
  assert.equal(destroyed.mount, null);
  assert.equal(destroyed.dataRoot, null);
  assert.deepEqual(destroyed.writerAuthority, {
    fencingEpoch: "2",
    holderId: "holder-002",
    leaseId: "lease-002",
  });
});

test("stale attach and detach fences do not prepare or dispatch", async (t) => {
  const fixed = await fixture(t);
  const provisioned = await provision(fixed);
  const firstAttach = mutationRequest("attach", provisioned.storageId);
  await fixed.backend.lifecycleBackend.prepareWritableAttachment(
    firstAttach,
    context(),
  );
  await fixed.backend.lifecycleBackend.detachAttachment(
    mutationRequest("detach", provisioned.storageId),
    context(),
  );

  await assertStaleMutationHasNoEffect(fixed, () =>
    fixed.backend.lifecycleBackend.prepareWritableAttachment(
      mutationRequest("attach", provisioned.storageId, {
        attachmentId: "attachment-002",
        operationId: "stale-attach-operation-001",
      }),
      context(),
    ));

  const secondAttachRequest = mutationRequest("attach", provisioned.storageId, {
    attachmentId: "attachment-002",
    fencingEpoch: "2",
    holderId: "holder-002",
    leaseId: "lease-002",
    operationId: "attach-operation-002",
  });
  const secondAttach = await fixed.backend.lifecycleBackend.prepareWritableAttachment(
    secondAttachRequest,
    context(),
  );

  const staleDetachOverrides = [
    {
      fencingEpoch: "1",
      holderId: "holder-002",
      leaseId: "lease-002",
      operationId: "stale-detach-epoch-001",
    },
    {
      fencingEpoch: "2",
      holderId: "holder-stale",
      leaseId: "lease-002",
      operationId: "stale-detach-holder-001",
    },
    {
      fencingEpoch: "2",
      holderId: "holder-002",
      leaseId: "lease-stale",
      operationId: "stale-detach-lease-001",
    },
  ];
  for (const overrides of staleDetachOverrides) {
    await assertStaleMutationHasNoEffect(fixed, () =>
      fixed.backend.lifecycleBackend.detachAttachment(
        mutationRequest("detach", provisioned.storageId, {
          attachmentId: "attachment-002",
          ...overrides,
        }),
        context(),
      ));
  }

  const validDetachRequest = mutationRequest("detach", provisioned.storageId, {
    attachmentId: "attachment-002",
    fencingEpoch: "2",
    holderId: "holder-002",
    leaseId: "lease-002",
    operationId: "detach-operation-002",
  });
  const detached = await fixed.backend.lifecycleBackend.detachAttachment(
    validDetachRequest,
    context(),
  );
  const callsBeforeReplay = fixed.fake.calls.length;
  assert.deepEqual(
    await fixed.backend.lifecycleBackend.detachAttachment(
      structuredClone(validDetachRequest),
      context(),
    ),
    detached,
  );
  assert.equal(fixed.fake.calls.length, callsBeforeReplay);

  const callsBeforeOldAttachReplay = fixed.fake.calls.length;
  assert.deepEqual(
    await fixed.backend.lifecycleBackend.prepareWritableAttachment(
      structuredClone(secondAttachRequest),
      context(),
    ),
    secondAttach,
  );
  assert.equal(fixed.fake.calls.length, callsBeforeOldAttachReplay);
});

test("detached destroy requires exact epoch-2 writer authority after takeover", async (t) => {
  const fixed = await fixture(t);
  const provisioned = await provision(fixed);
  await fixed.backend.lifecycleBackend.prepareWritableAttachment(
    mutationRequest("attach", provisioned.storageId),
    context(),
  );
  await fixed.backend.lifecycleBackend.detachAttachment(
    mutationRequest("detach", provisioned.storageId),
    context(),
  );
  await fixed.backend.lifecycleBackend.prepareWritableAttachment(
    mutationRequest("attach", provisioned.storageId, {
      attachmentId: "attachment-002",
      fencingEpoch: "2",
      holderId: "holder-002",
      leaseId: "lease-002",
      operationId: "attach-operation-002",
    }),
    context(),
  );
  await fixed.backend.lifecycleBackend.detachAttachment(
    mutationRequest("detach", provisioned.storageId, {
      attachmentId: "attachment-002",
      fencingEpoch: "2",
      holderId: "holder-002",
      leaseId: "lease-002",
      operationId: "detach-operation-002",
    }),
    context(),
  );
  assert.deepEqual(
    (await fixed.state.readStorage(provisioned.storageId)).writerAuthority,
    {
      fencingEpoch: "2",
      holderId: "holder-002",
      leaseId: "lease-002",
    },
  );

  const staleDestroyOverrides = [
    {
      fencingEpoch: "1",
      holderId: "holder-001",
      leaseId: "lease-001",
      operationId: "stale-destroy-epoch-001",
    },
    {
      fencingEpoch: "2",
      holderId: "holder-stale",
      leaseId: "lease-002",
      operationId: "stale-destroy-holder-001",
    },
    {
      fencingEpoch: "2",
      holderId: "holder-002",
      leaseId: "lease-stale",
      operationId: "stale-destroy-lease-001",
    },
  ];
  for (const overrides of staleDestroyOverrides) {
    await assertStaleMutationHasNoEffect(fixed, () =>
      fixed.backend.lifecycleBackend.destroySession(
        mutationRequest("destroy", provisioned.storageId, overrides),
        context(),
      ));
  }

  const validDestroyRequest = mutationRequest("destroy", provisioned.storageId, {
    fencingEpoch: "2",
    holderId: "holder-002",
    leaseId: "lease-002",
    operationId: "destroy-operation-valid-001",
  });
  const destroyed = await fixed.backend.lifecycleBackend.destroySession(
    validDestroyRequest,
    context(),
  );
  const callsBeforeReplay = fixed.fake.calls.length;
  assert.deepEqual(
    await fixed.backend.lifecycleBackend.destroySession(
      structuredClone(validDestroyRequest),
      context(),
    ),
    destroyed,
  );
  assert.equal(fixed.fake.calls.length, callsBeforeReplay);
});

test("restore activation adopts only the already-published deterministic child", async (t) => {
  const fixed = await fixture(t);
  const provisioned = await provision(fixed);
  const restore = restoreMutation(provisioned.storageId);
  const destination = fixed.paths.planRestoreDestination(restore);
  const publishedIdentity = fixed.fake.publish(destination.destinationDirectory);
  const request = activationRequest(
    provisioned.storageId,
    destination.destinationDirectory,
    publishedIdentity,
  );
  const result = await fixed.backend.lifecycleBackend.prepareRestoreAttachment(
    request,
    context(),
  );
  assert.equal(result.attachment.rootPath, destination.destinationDirectory);
  assert.equal(result.publication.root.objectId, publishedIdentity.objectId);
  assert.equal(
    fixed.fake.calls.filter((call) => call === "ensureAttachmentRoot").length,
    0,
  );
  const storage = await fixed.state.readStorage(provisioned.storageId);
  assert.equal(storage.lifecycle, "attached");
  assert.equal(storage.dataRoot.rootPath, destination.destinationDirectory);

  const reconciled = await fixed.backend.lifecycleBackend.reconcileRestoreAttachment(
    request,
    context(),
  );
  assert.equal(reconciled.outcome, "applied");
  assert.deepEqual(reconciled.result, result);
});

test("restore reconciliation is observational for absent and prepared activation", async (t) => {
  const fixed = await fixture(t);
  const provisioned = await provision(fixed);
  const restore = restoreMutation(provisioned.storageId);
  const destination = fixed.paths.planRestoreDestination(restore);
  const request = activationRequest(
    provisioned.storageId,
    destination.destinationDirectory,
    identity("ext4fh1:unpublished-restore-root"),
  );
  const absent = await fixed.backend.lifecycleBackend.reconcileRestoreAttachment(
    request,
    context(),
  );
  assert.equal(absent.outcome, "absent-and-quiescent");
  assert.equal(
    await fixed.state.readOperation({
      operationId: request.mutationRequest.operationId,
    }),
    null,
  );

  await fixed.state.prepareOperation({
    kind: "restore-attach",
    operationId: request.mutationRequest.operationId,
    request,
    storageId: provisioned.storageId,
  });
  const before = fixed.fake.calls.length;
  const prepared = await fixed.backend.lifecycleBackend.reconcileRestoreAttachment(
    request,
    context(),
  );
  assert.equal(prepared.outcome, "unknown");
  assert.equal(fixed.fake.calls.length, before);
});

test("resolver is read-only, cross-checks both generation identities, and activation inspects the child", async (t) => {
  const fixed = await fixture(t);
  const provisioned = await provision(fixed);
  const restore = restoreMutation(provisioned.storageId);
  const generationId = "restore-generation-001";
  const generation = {
    binding: {
      generationId,
      request: structuredClone(restore),
    },
    checkpointId: "checkpoint-001",
    claimedAt: "2026-08-05T00:01:00.000Z",
    committedAt: "2026-08-05T00:02:00.000Z",
    generationId,
    operationId: restore.operationId,
    sessionId: SESSION_ID,
    state: "committed",
  };
  const generationCandidate = { generationId, request: restore };
  const resolverInput = (kind, candidate, value = generation) => exact({
    candidate,
    contractVersion: 1,
    generation: value,
    invocation: exact({}),
    kind,
    signal: new AbortController().signal,
  });
  const before = fixed.fake.calls.length;
  const destination = await Reflect.apply(
    fixed.backend.resolveRestoreDestination,
    undefined,
    [resolverInput("generation", generationCandidate)],
  );
  assert.equal(destination.destinationOwnedRoot, (await fixed.state.readStorage(
    provisioned.storageId,
  )).mount.mountPath);
  assert.deepEqual(fixed.fake.calls.slice(before), [
    "observeMount",
    "ensurePublicationRoot",
  ]);

  const publishedIdentity = fixed.fake.publish(destination.destinationDirectory);
  const activationGeneration = {
    ...generation,
    document: {
      materialization: { stagedRoot: publishedIdentity },
    },
  };
  const generationReference = {
    bindingSha256: sha256Json(activationGeneration.binding),
    checkpointId: activationGeneration.checkpointId,
    claimedAt: activationGeneration.claimedAt,
    committedAt: activationGeneration.committedAt,
    documentSha256: sha256Json(activationGeneration.document),
    generationId: activationGeneration.generationId,
    operationId: activationGeneration.operationId,
    sessionId: activationGeneration.sessionId,
    state: "committed",
  };
  const activationCandidate = {
    activationOperationId: "activation-operation-001",
    request: {
      contractVersion: 1,
      destinationRootPath: destination.destinationDirectory,
      generation: generationReference,
      holderId: "activation-holder-001",
      launchIntent: {},
      leaseDurationMilliseconds: 60_000,
      predecessor: {},
    },
    state: "starting",
  };
  await Reflect.apply(fixed.backend.resolveRestoreDestination, undefined, [
    resolverInput("activation", activationCandidate, activationGeneration),
  ]);
  assert.equal(fixed.fake.calls.at(-1), "observeAttachmentRoot");

  await assert.rejects(
    Reflect.apply(fixed.backend.resolveRestoreDestination, undefined, [
      resolverInput(
        "generation",
        generationCandidate,
        { ...generation, generationId: "other-generation" },
      ),
    ]),
    backendError("invalid_request"),
  );
});

test("cold-open remounts only detached/provisioned storage and quiesce is logical-state read-only", async (t) => {
  const fixed = await fixture(t);
  const provisioned = await provision(fixed);
  const before = await fixed.state.readStorage(provisioned.storageId);
  const quiesced = await fixed.backend.quiesceStorage(provisioned.storageId);
  assert.deepEqual(quiesced, exact({
    status: "quiesced",
    storageId: provisioned.storageId,
  }));
  assert.deepEqual(await fixed.state.readStorage(provisioned.storageId), before);
  await fixed.backend.initialize();
  assert.equal(fixed.fake.calls.includes("remount"), true);

  await fixed.backend.lifecycleBackend.prepareWritableAttachment(
    mutationRequest("attach", provisioned.storageId),
    context(),
  );
  fixed.fake.setMounted(false);
  const remounts = fixed.fake.calls.filter((call) => call === "remount").length;
  await assert.rejects(
    fixed.backend.initialize(),
    backendError("cold_open_failed"),
  );
  assert.equal(
    fixed.fake.calls.filter((call) => call === "remount").length,
    remounts,
  );
  await assert.rejects(
    fixed.backend.quiesceStorage(provisioned.storageId),
    backendError("physical_state_mismatch"),
  );
});

test("cold-open rejects a replaced publication control inode", async (t) => {
  const fixed = await fixture(t);
  const provisioned = await provision(fixed);
  const before = await fixed.state.readStorage(provisioned.storageId);
  fixed.fake.replacePublicationControl();
  await assert.rejects(
    fixed.backend.initialize(),
    backendError("cold_open_failed"),
  );
  assert.deepEqual(await fixed.state.readStorage(provisioned.storageId), before);
});

test("manual fencing and raw checkpoint leaves fail closed", async (t) => {
  const fixed = await fixture(t);
  const provisioned = await provision(fixed);
  await assert.rejects(
    fixed.backend.lifecycleBackend.forceFence(
      {
        backendId: BACKEND_ID,
        contractVersion: 1,
        fencingEpoch: "2",
        operationId: "force-fence-operation-001",
        revokedFence: {
          fencingEpoch: "1",
          holderId: "holder-001",
          leaseId: "lease-001",
        },
        sessionId: SESSION_ID,
        storageId: provisioned.storageId,
        target: { attachmentId: "attachment-001", kind: "attachment" },
      },
      context(),
    ),
    backendError("fence_unavailable"),
  );
  for (const method of ["captureCheckpoint", "restoreCheckpoint"]) {
    await assert.rejects(
      fixed.backend.lifecycleBackend[method]({}, context()),
      backendError("checkpoint_unsupported"),
    );
  }
});
