import assert from "node:assert/strict";
import { Buffer } from "node:buffer";
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
