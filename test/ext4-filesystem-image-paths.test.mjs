import assert from "node:assert/strict";
import { Buffer } from "node:buffer";
import crypto from "node:crypto";
import { syncBuiltinESMExports } from "node:module";
import path from "node:path";
import test from "node:test";

import {
  EXT4_FILESYSTEM_IMAGE_PATHS_CONTRACT_VERSION,
  Ext4FilesystemImagePathsError,
  assertExt4FilesystemImageMountPathCapacity,
  createExt4FilesystemImagePaths,
} from "../src/ext4-filesystem-image-paths.mjs";
import {
  createPostgresDetachedRestorePlan,
} from "../src/postgres-detached-restore-plan.mjs";

const BACKEND_ID = "ext4-backend-001";
const SESSION_ID = "019f2100-0000-7000-8000-000000000001";
const SECOND_SESSION_ID = "019f2100-0000-7000-8000-000000000003";

function pathsError(code) {
  return (error) =>
    error instanceof Ext4FilesystemImagePathsError && error.code === code;
}

function fixture() {
  return createExt4FilesystemImagePaths({
    archiveRoot: "/var/lib/portable-codex-runtime/archive",
    backendId: BACKEND_ID,
    imageRoot: "/var/lib/portable-codex-runtime/images",
    mountRoot: "/run/portable-codex-runtime/mounts",
  });
}

function provisionRequest() {
  return {
    backendId: BACKEND_ID,
    contractVersion: 1,
    operationId: "provision-operation-001",
    sessionId: SESSION_ID,
  };
}

function checkpoint() {
  return {
    artifactId: "artifact-001",
    backendId: BACKEND_ID,
    checkpointClass: "clean",
    checkpointId: "checkpoint-001",
    codexSessionId: "019f2100-0000-7000-8000-000000000002",
    codexThreadId: "019f2100-0000-7000-8000-000000000002",
    contractVersion: 1,
    createdAt: "2026-08-14T12:00:00.000Z",
    imageDigest: `sha256:${"a".repeat(64)}`,
    sessionId: SESSION_ID,
    sourceFencingEpoch: "1",
    storageId: fixture().storageIdForSession(SESSION_ID),
  };
}

function mutation(operation, storageId = fixture().storageIdForSession(SESSION_ID)) {
  const target =
    operation === "attach" || operation === "detach"
      ? { attachmentId: "attachment-001", kind: "attachment" }
      : {
          artifactId: "artifact-001",
          checkpointId: "checkpoint-001",
          kind: "checkpoint",
        };
  return {
    backendId: BACKEND_ID,
    contractVersion: 1,
    fencingEpoch: "1",
    holderId: "holder-001",
    leaseId: "lease-001",
    operation,
    operationId: `${operation}-operation-001`,
    sessionId: SESSION_ID,
    storageId,
    target,
  };
}

function persistentPathSnapshot(paths) {
  const storageId = paths.storageIdForSession(SESSION_ID);
  const provision = paths.planProvision(provisionRequest());
  const attachment = paths.planWritableAttachment(
    mutation("attach", storageId),
  );
  const restore = paths.planRestoreDestination(mutation("restore", storageId));
  const artifact = paths.resolveArtifactPaths({
    checkpoint: checkpoint(),
    request: mutation("checkpoint", storageId),
  });
  const source = paths.resolveSourceOwnedRoot({
    canonicalAttachment: {
      attachmentId: "attachment-001",
      backendId: BACKEND_ID,
      contractVersion: 1,
      fencingEpoch: "1",
      holderId: "holder-001",
      kind: "directory",
      leaseId: "lease-001",
      mode: "read-write",
      operationId: "attach-operation-001",
      proofId: "proof-create-hash-hostile-001",
      rootPath: attachment.attachmentRootPath,
      sessionId: SESSION_ID,
      storageId,
    },
    checkpoint: checkpoint(),
    request: mutation("checkpoint", storageId),
  });
  return {
    artifact,
    attachment,
    provision,
    restore,
    source,
    storageId,
  };
}

function isolationPathSnapshot(paths, sessionId, suffix) {
  const storageId = paths.storageIdForSession(sessionId);
  const provision = paths.planProvision({
    backendId: BACKEND_ID,
    contractVersion: 1,
    operationId: `provision-operation-${suffix}`,
    sessionId,
  });
  const attachRequest = {
    ...mutation("attach", storageId),
    operationId: `attach-operation-${suffix}`,
    sessionId,
  };
  const attachment = paths.planWritableAttachment(attachRequest);
  const restore = paths.planRestoreDestination({
    ...mutation("restore", storageId),
    operationId: `restore-operation-${suffix}`,
    sessionId,
  });
  const descriptor = {
    artifactId: "artifact-001",
    backendId: BACKEND_ID,
    checkpointClass: "clean",
    checkpointId: "checkpoint-001",
    codexSessionId: sessionId,
    codexThreadId: sessionId,
    contractVersion: 1,
    createdAt: "2026-08-14T12:00:00.000Z",
    imageDigest: `sha256:${"a".repeat(64)}`,
    sessionId,
    sourceFencingEpoch: "1",
    storageId,
  };
  const checkpointRequest = {
    ...mutation("checkpoint", storageId),
    operationId: `checkpoint-operation-${suffix}`,
    sessionId,
  };
  const artifact = paths.resolveArtifactPaths({
    checkpoint: descriptor,
    request: checkpointRequest,
  });
  const source = paths.resolveSourceOwnedRoot({
    canonicalAttachment: {
      attachmentId: attachRequest.target.attachmentId,
      backendId: BACKEND_ID,
      contractVersion: 1,
      fencingEpoch: "1",
      holderId: "holder-001",
      kind: "directory",
      leaseId: "lease-001",
      mode: "read-write",
      operationId: attachRequest.operationId,
      proofId: `proof-iterator-isolation-${suffix}`,
      rootPath: attachment.attachmentRootPath,
      sessionId,
      storageId,
    },
    checkpoint: descriptor,
    request: checkpointRequest,
  });
  return {
    artifact,
    attachment,
    provision,
    restore,
    source,
    storageId,
  };
}

function arrayTargetsDigestDomain(value) {
  return (
    Array.isArray(value) &&
    (value[0] === "session" ||
      value[0] === "storage" ||
      value[0] === "data-root" ||
      value[0] === "restore-operation" ||
      value[0] === "artifact")
  );
}

test("derives deterministic disjoint direct-child image, mount, and artifact paths", () => {
  const paths = fixture();
  assert.equal(
    paths.contractVersion,
    EXT4_FILESYSTEM_IMAGE_PATHS_CONTRACT_VERSION,
  );
  assert.equal(Object.getPrototypeOf(paths), null);
  assert.equal(Object.isFrozen(paths), true);
  assert.deepEqual(Reflect.ownKeys(paths).sort(), [
    "backendId",
    "contractVersion",
    "planProvision",
    "planRestoreDestination",
    "planWritableAttachment",
    "resolveArtifactPaths",
    "resolveSourceOwnedRoot",
    "storageIdForSession",
  ]);
  for (const method of [
    "planProvision",
    "planRestoreDestination",
    "planWritableAttachment",
    "resolveArtifactPaths",
    "resolveSourceOwnedRoot",
    "storageIdForSession",
  ]) {
    assert.equal(Object.isFrozen(paths[method]), true);
    assert.doesNotMatch(
      Function.prototype.toString.call(paths[method]),
      /\[native code\]/u,
    );
  }
  const first = paths.planProvision(provisionRequest());
  const detachedPlanProvision = paths.planProvision;
  const replay = detachedPlanProvision({ ...provisionRequest() });
  assert.deepEqual(replay, first);
  assert.equal(first.storageId, paths.storageIdForSession(SESSION_ID));
  assert.match(first.imagePath, /^\/var\/lib\/portable-codex-runtime\/images\/[0-9a-f]{64}\.ext4$/u);
  assert.match(first.mountPath, /^\/run\/portable-codex-runtime\/mounts\/[0-9a-f]{64}$/u);

  const attach = paths.planWritableAttachment(mutation("attach", first.storageId));
  assert.equal(attach.mountPath, first.mountPath);
  assert.equal(attach.imagePath, first.imagePath);
  assert.match(attach.attachmentRootPath, /\/data-[0-9a-f]{48}$/u);

  const descriptor = checkpoint();
  const artifact = paths.resolveArtifactPaths({
    checkpoint: descriptor,
    request: mutation("checkpoint", first.storageId),
  });
  assert.equal(artifact.artifactOwnedRoot, "/var/lib/portable-codex-runtime/archive");
  assert.match(artifact.artifactDirectory, /\/artifact-[0-9a-f]{48}$/u);
});

test("matches the driver canonical path domain at UTF-8 and control boundaries", () => {
  const rootAtMaximum = `/${"r".repeat(4094)}`;
  assert.doesNotThrow(() =>
    createExt4FilesystemImagePaths({
      archiveRoot: rootAtMaximum,
      backendId: BACKEND_ID,
      imageRoot: "/var/lib/portable-codex-runtime/images",
      mountRoot: "/run/portable-codex-runtime/mounts",
    }));
  assert.throws(
    () =>
      createExt4FilesystemImagePaths({
        archiveRoot: `/${"r".repeat(4095)}`,
        backendId: BACKEND_ID,
        imageRoot: "/var/lib/portable-codex-runtime/images",
        mountRoot: "/run/portable-codex-runtime/mounts",
      }),
    pathsError("invalid_ext4_filesystem_image_path_options"),
  );

  for (const codePoint of [...Array(32).keys(), 0x7f]) {
    const control = String.fromCodePoint(codePoint);
    assert.throws(
      () =>
        createExt4FilesystemImagePaths({
          archiveRoot: `/var/lib/portable-codex-runtime/archive${control}`,
          backendId: BACKEND_ID,
          imageRoot: "/var/lib/portable-codex-runtime/images",
          mountRoot: "/run/portable-codex-runtime/mounts",
        }),
      pathsError("invalid_ext4_filesystem_image_path_options"),
    );
  }

  const unicodeRoots = {
    archiveRoot: "/var/lib/portable-codex-runtime/archive-雪",
    backendId: BACKEND_ID,
    imageRoot: "/var/lib/portable-codex-runtime/images-雪",
    mountRoot: "/run/portable-codex-runtime/mounts-雪",
  };
  const unicodePlan = createExt4FilesystemImagePaths(unicodeRoots).planProvision(
    provisionRequest(),
  );
  assert.equal(
    unicodePlan.imagePath.startsWith(`${unicodeRoots.imageRoot}/`),
    true,
  );
  assert.equal(
    unicodePlan.mountPath.startsWith(`${unicodeRoots.mountRoot}/`),
    true,
  );
});

test("accepts a generated 4095-byte path and rejects a 4096-byte path", () => {
  const createWithImageRootLength = (rootBytes) =>
    createExt4FilesystemImagePaths({
      archiveRoot: "/var/lib/portable-codex-runtime/archive",
      backendId: BACKEND_ID,
      imageRoot: `/${"i".repeat(rootBytes - 1)}`,
      mountRoot: "/run/portable-codex-runtime/mounts",
    });

  const maximumPlan = createWithImageRootLength(4025).planProvision(
    provisionRequest(),
  );
  assert.equal(Buffer.byteLength(maximumPlan.imagePath, "utf8"), 4095);

  const oversizedPaths = createWithImageRootLength(4026);
  assert.throws(
    () => oversizedPaths.planProvision(provisionRequest()),
    pathsError("invalid_ext4_filesystem_image_path_request"),
  );
});

test("provisioning reserves the native path budget for attachment children", () => {
  const createWithMountRootLength = (rootBytes) =>
    createExt4FilesystemImagePaths({
      archiveRoot: "/var/lib/portable-codex-runtime/archive",
      backendId: BACKEND_ID,
      imageRoot: "/var/lib/portable-codex-runtime/images",
      mountRoot: `/${"m".repeat(rootBytes - 1)}`,
    });

  const maximumPaths = createWithMountRootLength(3970);
  const maximumPlan = maximumPaths.planProvision(provisionRequest());
  assert.equal(
    assertExt4FilesystemImageMountPathCapacity(maximumPlan.mountPath),
    maximumPlan.mountPath,
  );
  const maximumRestore = maximumPaths.planRestoreDestination(
    mutation("restore", maximumPlan.storageId),
  );
  const maximumAttachment = maximumPaths.planWritableAttachment(
    mutation("attach", maximumPlan.storageId),
  );
  assert.equal(Buffer.byteLength(maximumPlan.mountPath, "utf8"), 4035);
  assert.equal(
    Buffer.byteLength(maximumRestore.destinationDirectory, "utf8"),
    4095,
  );
  assert.equal(
    Buffer.byteLength(maximumAttachment.attachmentRootPath, "utf8"),
    4089,
  );

  const oversizedPaths = createWithMountRootLength(3971);
  const oversizedPlan = oversizedPaths.planProvision(provisionRequest());
  assert.equal(Buffer.byteLength(oversizedPlan.mountPath, "utf8"), 4036);
  assert.throws(
    () =>
      assertExt4FilesystemImageMountPathCapacity(oversizedPlan.mountPath),
    pathsError("invalid_ext4_filesystem_image_path_request"),
  );
});

test(
  "captures path helpers before deriving storage-owned paths",
  { concurrency: false },
  () => {
    const functionNames = [
      "basename",
      "dirname",
      "isAbsolute",
      "join",
      "parse",
      "resolve",
    ];
    const descriptors = Object.fromEntries(
      [...functionNames, "sep"].map((name) => [
        name,
        Object.getOwnPropertyDescriptor(path, name),
      ]),
    );
    const poisonCalls = Object.fromEntries(
      functionNames.map((name) => [name, 0]),
    );
    const targetsPathDerivation = (value) =>
      typeof value === "string" &&
      (value.startsWith("/var/lib/portable-codex-runtime/") ||
        value.startsWith("/run/portable-codex-runtime/"));
    for (const name of functionNames) {
      assert.equal(typeof descriptors[name]?.value, "function");
    }
    assert.equal(typeof descriptors.sep?.value, "string");
    try {
      for (const name of functionNames) {
        const descriptor = descriptors[name];
        Object.defineProperty(path, name, {
          ...descriptor,
          value(...args) {
            if (args.some(targetsPathDerivation)) poisonCalls[name] += 1;
            return Reflect.apply(descriptor.value, this, args);
          },
        });
      }
      Object.defineProperty(path, "sep", {
        ...descriptors.sep,
        value: "!",
      });
      syncBuiltinESMExports();

      assert.throws(
        () =>
          createExt4FilesystemImagePaths({
            archiveRoot: "/var/lib/portable-codex-runtime/nested-root",
            backendId: BACKEND_ID,
            imageRoot:
              "/var/lib/portable-codex-runtime/nested-root/images",
            mountRoot: "/run/portable-codex-runtime/mounts",
          }),
        pathsError("invalid_ext4_filesystem_image_path_options"),
      );
      const paths = fixture();
      const provision = paths.planProvision(provisionRequest());
      const attachment = paths.planWritableAttachment(
        mutation("attach", provision.storageId),
      );
      const restore = paths.planRestoreDestination(
        mutation("restore", provision.storageId),
      );
      const artifact = paths.resolveArtifactPaths({
        checkpoint: checkpoint(),
        request: mutation("checkpoint", provision.storageId),
      });
      assert.equal(
        assertExt4FilesystemImageMountPathCapacity(provision.mountPath),
        provision.mountPath,
      );
      const attachRequest = mutation("attach", provision.storageId);
      const restoredRoot = `${provision.mountPath}/generation-${"a".repeat(48)}`;
      const source = paths.resolveSourceOwnedRoot({
        canonicalAttachment: {
          attachmentId: attachRequest.target.attachmentId,
          backendId: attachRequest.backendId,
          contractVersion: attachRequest.contractVersion,
          fencingEpoch: attachRequest.fencingEpoch,
          holderId: attachRequest.holderId,
          kind: "directory",
          leaseId: attachRequest.leaseId,
          mode: "read-write",
          operationId: attachRequest.operationId,
          proofId: "proof-hostile-path-001",
          rootPath: restoredRoot,
          sessionId: attachRequest.sessionId,
          storageId: attachRequest.storageId,
        },
        checkpoint: checkpoint(),
        request: mutation("checkpoint", provision.storageId),
      });
      assert.equal(
        provision.imagePath.startsWith(
          "/var/lib/portable-codex-runtime/images/",
        ),
        true,
      );
      assert.equal(
        provision.mountPath.startsWith(
          "/run/portable-codex-runtime/mounts/",
        ),
        true,
      );
      assert.equal(
        attachment.attachmentRootPath.startsWith(
          `${provision.mountPath}/data-`,
        ),
        true,
      );
      assert.equal(
        restore.destinationDirectory.startsWith(
          `${provision.mountPath}/generation-`,
        ),
        true,
      );
      assert.equal(
        artifact.artifactDirectory.startsWith(
          "/var/lib/portable-codex-runtime/archive/artifact-",
        ),
        true,
      );
      assert.equal(source.sourceOwnedRoot, provision.mountPath);
      assert.equal(source.sourceDirectory, restoredRoot);
    } finally {
      for (const name of [...functionNames, "sep"]) {
        Object.defineProperty(path, name, descriptors[name]);
      }
      syncBuiltinESMExports();
    }
    assert.deepEqual(poisonCalls, {
      basename: 0,
      dirname: 0,
      isAbsolute: 0,
      join: 0,
      parse: 0,
      resolve: 0,
    });
  },
);

test(
  "captures createHash before deriving persistent storage paths",
  { concurrency: false },
  () => {
    const createHashDescriptor = Object.getOwnPropertyDescriptor(
      crypto,
      "createHash",
    );
    assert.equal(typeof createHashDescriptor?.value, "function");
    const originalCreateHash = createHashDescriptor.value;
    const baselinePaths = fixture();
    const baseline = persistentPathSnapshot(baselinePaths);
    assert.equal(
      baseline.storageId,
      "ext4-storage:51321236acfd018134f7c07cbe395c5cab5a22d51d8497ec",
    );
    assert.equal(
      baseline.provision.mountPath,
      "/run/portable-codex-runtime/mounts/5164d37325ed8ac55a7a4aef55b6d67ed56a6e2b8022eecb86522012c505280d",
    );
    assert.equal(
      baseline.attachment.attachmentRootPath,
      `${baseline.provision.mountPath}/data-429a972a23f7d9abfa30150428692b6f22efbd80d9880bdd`,
    );
    assert.equal(
      baseline.restore.destinationDirectory,
      `${baseline.provision.mountPath}/generation-f622f9434ad63b7580be924559313a83f3e16af8482280c8`,
    );
    assert.equal(
      baseline.artifact.artifactDirectory,
      "/var/lib/portable-codex-runtime/archive/artifact-1473a9cc43316f9c682064ff140860c09d0700cc529f87ed",
    );
    let poisonCalls = 0;
    let observedError = null;
    let replay;
    let restarted;

    try {
      Object.defineProperty(crypto, "createHash", {
        ...createHashDescriptor,
        value(...args) {
          poisonCalls += 1;
          const hash = Reflect.apply(originalCreateHash, undefined, args);
          hash.update("hostile-ext4-path-prefix\0", "utf8");
          return hash;
        },
      });
      syncBuiltinESMExports();

      replay = persistentPathSnapshot(baselinePaths);
      restarted = persistentPathSnapshot(fixture());
    } catch (error) {
      observedError = error;
    } finally {
      Object.defineProperty(crypto, "createHash", createHashDescriptor);
      syncBuiltinESMExports();
    }

    assert.equal(observedError, null);
    assert.equal(poisonCalls, 0);
    assert.deepEqual(replay, baseline);
    assert.deepEqual(restarted, baseline);
  },
);

test(
  "keeps session and storage paths isolated after Array iterator pollution",
  { concurrency: false },
  () => {
    const iteratorDescriptor = Object.getOwnPropertyDescriptor(
      Array.prototype,
      Symbol.iterator,
    );
    assert.equal(typeof iteratorDescriptor?.value, "function");
    const originalIterator = iteratorDescriptor.value;
    const paths = fixture();
    const firstBaseline = isolationPathSnapshot(paths, SESSION_ID, "first");
    const secondBaseline = isolationPathSnapshot(
      paths,
      SECOND_SESSION_ID,
      "second",
    );
    assert.notEqual(firstBaseline.storageId, secondBaseline.storageId);
    assert.notEqual(
      firstBaseline.provision.imagePath,
      secondBaseline.provision.imagePath,
    );
    assert.notEqual(
      firstBaseline.provision.mountPath,
      secondBaseline.provision.mountPath,
    );
    assert.notEqual(
      firstBaseline.attachment.attachmentRootPath,
      secondBaseline.attachment.attachmentRootPath,
    );
    assert.notEqual(
      firstBaseline.restore.destinationDirectory,
      secondBaseline.restore.destinationDirectory,
    );
    assert.notEqual(
      firstBaseline.artifact.artifactDirectory,
      secondBaseline.artifact.artifactDirectory,
    );

    let firstReplay;
    let secondReplay;
    let observedError = null;
    let poisonCalls = 0;
    try {
      Object.defineProperty(Array.prototype, Symbol.iterator, {
        ...iteratorDescriptor,
        value: function hostileDigestPartsIterator() {
          if (arrayTargetsDigestDomain(this)) {
            poisonCalls += 1;
            return {
              next() {
                return { done: true, value: undefined };
              },
            };
          }
          return Reflect.apply(originalIterator, this, []);
        },
      });
      firstReplay = isolationPathSnapshot(paths, SESSION_ID, "first");
      secondReplay = isolationPathSnapshot(
        paths,
        SECOND_SESSION_ID,
        "second",
      );
    } catch (error) {
      observedError = error;
    } finally {
      Object.defineProperty(
        Array.prototype,
        Symbol.iterator,
        iteratorDescriptor,
      );
    }

    assert.equal(observedError, null);
    assert.equal(poisonCalls, 0);
    assert.deepEqual(firstReplay, firstBaseline);
    assert.deepEqual(secondReplay, secondBaseline);
    assert.notEqual(firstReplay.storageId, secondReplay.storageId);
    assert.notEqual(
      firstReplay.provision.mountPath,
      secondReplay.provision.mountPath,
    );
    assert.notEqual(
      firstReplay.attachment.attachmentRootPath,
      secondReplay.attachment.attachmentRootPath,
    );
    assert.notEqual(
      firstReplay.restore.destinationDirectory,
      secondReplay.restore.destinationDirectory,
    );
    assert.notEqual(
      firstReplay.artifact.artifactDirectory,
      secondReplay.artifact.artifactDirectory,
    );
  },
);

test("maps attachment and restore destinations to one storage-owned mount root", () => {
  const paths = fixture();
  const storageId = paths.storageIdForSession(SESSION_ID);
  const request = mutation("attach", storageId);
  const planned = paths.planWritableAttachment(request);
  const canonicalAttachment = {
    attachmentId: request.target.attachmentId,
    backendId: request.backendId,
    contractVersion: request.contractVersion,
    fencingEpoch: request.fencingEpoch,
    holderId: request.holderId,
    kind: "directory",
    leaseId: request.leaseId,
    mode: "read-write",
    operationId: request.operationId,
    proofId: "proof-001",
    rootPath: planned.attachmentRootPath,
    sessionId: request.sessionId,
    storageId: request.storageId,
  };
  const source = paths.resolveSourceOwnedRoot({
    canonicalAttachment,
    checkpoint: checkpoint(),
    request: mutation("checkpoint", storageId),
  });
  assert.equal(Object.getPrototypeOf(source), null);
  assert.equal(source.sourceDirectory, planned.attachmentRootPath);
  assert.equal(source.sourceOwnedRoot, planned.mountPath);
  const reattachedSource = paths.resolveSourceOwnedRoot({
    canonicalAttachment: {
      ...canonicalAttachment,
      attachmentId: "attachment-002",
      operationId: "reattach-operation-002",
    },
    checkpoint: checkpoint(),
    request: mutation("checkpoint", storageId),
  });
  assert.deepEqual(reattachedSource, source);
  const restoreRequest = mutation("restore", storageId);
  const destination = paths.planRestoreDestination(restoreRequest);
  assert.equal(destination.destinationOwnedRoot, planned.mountPath);
  assert.match(destination.destinationDirectory, /\/generation-[0-9a-f]{48}$/u);
  const restoredAttachment = {
    attachmentId: "restored-attachment-001",
    backendId: request.backendId,
    contractVersion: request.contractVersion,
    fencingEpoch: request.fencingEpoch,
    holderId: request.holderId,
    kind: "directory",
    leaseId: request.leaseId,
    mode: "read-write",
    operationId: "restore-activation-001",
    proofId: "restore-proof-001",
    rootPath: destination.destinationDirectory,
    sessionId: request.sessionId,
    storageId: request.storageId,
  };
  const restoredSource = paths.resolveSourceOwnedRoot({
    canonicalAttachment: restoredAttachment,
    checkpoint: checkpoint(),
    request: mutation("checkpoint", storageId),
  });
  assert.equal(restoredSource.sourceDirectory, destination.destinationDirectory);
  assert.equal(restoredSource.sourceOwnedRoot, destination.destinationOwnedRoot);
});

test("derives a restore destination before stable-plan identities exist", () => {
  const paths = fixture();
  const storageId = paths.storageIdForSession(SESSION_ID);
  const request = mutation("restore", storageId);
  const destination = paths.planRestoreDestination(request);
  const artifact = paths.resolveArtifactPaths({
    checkpoint: checkpoint(),
    request: mutation("checkpoint", storageId),
  });
  const plan = createPostgresDetachedRestorePlan({
    plan: {
      captureCreatedAt: checkpoint().createdAt,
      destinationDirectory: destination.destinationDirectory,
      destinationOwnedRoot: destination.destinationOwnedRoot,
      detachMode: "release",
      holderId: request.holderId,
      imagePlanId: "image-plan-001",
      leaseDurationMilliseconds: 60_000,
      sourceArtifactDirectory: artifact.artifactDirectory,
      sourceArtifactOwnedRoot: artifact.artifactOwnedRoot,
    },
    request,
  });
  assert.match(plan.generationId, /^restore-generation:/u);
  assert.deepEqual(paths.planRestoreDestination(plan.request), destination);
});

test("rejects nested roots, identity substitution, extra fields, and invalid planner inputs", () => {
  assert.throws(
    () =>
      createExt4FilesystemImagePaths({
        archiveRoot: "/tmp/\ud800",
        backendId: BACKEND_ID,
        imageRoot: "/tmp/�",
        mountRoot: "/run/portable-codex-runtime/mounts",
      }),
    Ext4FilesystemImagePathsError,
  );
  assert.throws(
    () =>
      createExt4FilesystemImagePaths({
        archiveRoot: "/var/lib/portable-codex-runtime",
        backendId: BACKEND_ID,
        imageRoot: "/var/lib/portable-codex-runtime/images",
        mountRoot: "/run/portable-codex-runtime/mounts",
      }),
    Ext4FilesystemImagePathsError,
  );
  assert.throws(
    () => createExt4FilesystemImagePaths({
      archiveRoot: "/var/lib/archive",
      backendId: BACKEND_ID,
      imageRoot: "/var/lib/images",
      mountRoot: "/run/mounts",
      rootAuthority: "not-authority",
    }),
    Ext4FilesystemImagePathsError,
  );
  const paths = fixture();
  assert.throws(
    () => paths.planProvision({ ...provisionRequest(), backendId: "other-backend" }),
    Ext4FilesystemImagePathsError,
  );
  assert.throws(
    () => paths.planWritableAttachment(mutation("detach")),
    Ext4FilesystemImagePathsError,
  );
  assert.throws(
    () =>
      paths.resolveArtifactPaths({
        checkpoint: checkpoint(),
        request: mutation("restore"),
      }),
    Ext4FilesystemImagePathsError,
  );
  assert.throws(
    () =>
      paths.resolveArtifactPaths({
        checkpoint: checkpoint(),
        request: mutation("checkpoint", "different-storage"),
      }),
    Ext4FilesystemImagePathsError,
  );
  assert.throws(
    () =>
      paths.planRestoreDestination({
        ...mutation("restore"),
        backendId: "other-backend",
      }),
    Ext4FilesystemImagePathsError,
  );
  const storageId = paths.storageIdForSession(SESSION_ID);
  const request = mutation("attach", storageId);
  const planned = paths.planWritableAttachment(request);
  const mismatchedAttachment = {
    attachmentId: request.target.attachmentId,
    backendId: request.backendId,
    contractVersion: request.contractVersion,
    fencingEpoch: request.fencingEpoch,
    holderId: request.holderId,
    kind: "directory",
    leaseId: request.leaseId,
    mode: "read-write",
    operationId: request.operationId,
    proofId: "proof-001",
    rootPath: `${planned.mountPath}/different-sibling`,
    sessionId: request.sessionId,
    storageId: request.storageId,
  };
  assert.throws(
    () =>
      paths.resolveSourceOwnedRoot({
        canonicalAttachment: mismatchedAttachment,
        checkpoint: checkpoint(),
        request: mutation("checkpoint", storageId),
      }),
    Ext4FilesystemImagePathsError,
  );
});
