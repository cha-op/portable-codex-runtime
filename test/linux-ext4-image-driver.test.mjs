import assert from "node:assert/strict";
import { Buffer } from "node:buffer";
import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import {
  chmod,
  mkdtemp,
  mkdir,
  open,
  readFile,
  realpath,
  rm,
  rmdir,
  stat,
  unlink,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test, { afterEach } from "node:test";
import { promisify } from "node:util";

import {
  LINUX_EXT4_IMAGE_DRIVER_CONTRACT_VERSION,
  LinuxExt4ImageDriverError,
  createLinuxExt4ImageDriver,
} from "../src/linux-ext4-image-driver.mjs";

const FILESYSTEM_ID = "ext4fs:01234567-89ab-cdef-0123-456789abcdef";
const LOOP_DEVICE = "/dev/loop7";
const roots = [];
const execFileAsync = promisify(execFile);

afterEach(async () => {
  while (roots.length > 0) {
    await rm(roots.pop(), { force: true, recursive: true });
  }
});

function objectId(path) {
  return `ext4fh1:${createHash("sha256").update(path).digest("hex")}`;
}

function encodeMountField(value) {
  return value
    .replaceAll("\\", "\\134")
    .replaceAll(" ", "\\040")
    .replaceAll("\t", "\\011")
    .replaceAll("\n", "\\012");
}

function completion(stdout = "") {
  return {
    stderr: Buffer.alloc(0),
    stdout: Buffer.from(stdout, "utf8"),
  };
}

async function createPaths() {
  const root = await realpath(
    await mkdtemp(join(tmpdir(), "linux-ext4-driver-")),
  );
  roots.push(root);
  const imageRoot = join(root, "images");
  const mountRoot = join(root, "mounts");
  await mkdir(imageRoot, { mode: 0o700 });
  await mkdir(mountRoot, { mode: 0o700 });
  return {
    attachmentRootPath: join(mountRoot, "volume", "attachment-one"),
    imagePath: join(imageRoot, "volume.ext4"),
    mountPath: join(mountRoot, "volume"),
    root,
  };
}

function createInspector(paths, state, fdCalls, overrides = {}) {
  const loopDevice = overrides.loopDevice ?? LOOP_DEVICE;
  function loopReceipt(status = "present") {
    return stat(paths.imagePath, { bigint: true }).then((metadata) =>
      Object.freeze({
        backingDevice: String(metadata.dev),
        backingInode: String(metadata.ino),
        blockSize: "512",
        loopDevice,
        loopRdev: state.loopRdev,
        offset: "0",
        readOnly: false,
        sizeBytes: String(metadata.size),
        sizeLimit: "0",
        status,
      }),
    );
  }
  return Object.freeze({
    async inspectFilesystemObject(path) {
      const metadata = await stat(path, { bigint: true });
      return Object.freeze({
        filesystem: Object.freeze({
          durability: "local-fsync-rename",
          filesystemId: FILESYSTEM_ID,
          objectIdentityScheme: "linux-ext4-file-handle-sha256-v1",
          type: "ext4",
        }),
        identity: Object.freeze({
          device: String(metadata.dev),
          inode: String(metadata.ino),
          objectId: objectId(path),
        }),
      });
    },
    async runFdOperation(request) {
      fdCalls.push(request);
      if (overrides.beforeFdOperation !== undefined) {
        await overrides.beforeFdOperation(request, state);
      }
      if (overrides.fdOperationError?.(request)) {
        throw new Error("injected fd operation failure");
      }
      switch (request.operation) {
        case "create-image": {
          let handle;
          try {
            handle = await open(request.path, "wx", 0o600);
          } catch (error) {
            if (error.code === "EEXIST") error.code = "path_exists";
            throw error;
          }
          try {
            await handle.truncate(request.sizeBytes);
            await handle.chmod(0o600);
            await handle.sync();
          } finally {
            await handle.close();
          }
          const metadata = await stat(request.path, { bigint: true });
          return Object.freeze({
            device: String(metadata.dev),
            inode: String(metadata.ino),
            status: "ok",
          });
        }
        case "format-ext4":
          return Object.freeze({ status: "ok" });
        case "create-directory": {
          let created = false;
          try {
            await mkdir(request.path, { mode: 0o700 });
            created = true;
          } catch (error) {
            if (error.code !== "EEXIST" || request.exclusive) throw error;
          }
          await chmod(request.path, 0o700);
          const metadata = await stat(request.path, { bigint: true });
          return Object.freeze({
            created,
            device: String(metadata.dev),
            inode: String(metadata.ino),
            status: "ok",
          });
        }
        case "attach-loop":
          state.loopAttached = true;
          state.associatedLoops = [loopDevice];
          return loopReceipt("attached");
        case "find-loop":
          if (state.associatedLoops.length > 1) {
            const error = new Error("ambiguous");
            error.code = "path_mismatch";
            throw error;
          }
          return state.associatedLoops.length === 0
            ? Object.freeze({ status: "absent" })
            : loopReceipt("present");
        case "inspect-loop":
          if (!state.loopAttached || state.backingPath !== request.path) {
            const error = new Error("backing mismatch");
            error.code = "path_mismatch";
            throw error;
          }
          return loopReceipt("present");
        case "mount-ext4":
          state.mounted = true;
          state.private = true;
          state.safeOptions = true;
          return Object.freeze({ status: "ok" });
        case "syncfs":
          return Object.freeze({ status: "ok" });
        case "unmount-ext4":
          state.mounted = false;
          await rm(
            join(paths.mountPath, ".stopped-directory-publication.lock"),
            { force: true },
          );
          return Object.freeze({ status: "ok" });
        case "detach-loop-settle":
          state.loopAttached = false;
          state.associatedLoops = state.associatedLoops.filter(
            (loop) => loop !== request.loopDevice,
          );
          return Object.freeze({ status: "ok" });
        case "remove-file":
          await unlink(request.path);
          return Object.freeze({ status: "ok" });
        case "remove-directory":
          await rmdir(request.path);
          return Object.freeze({ status: "ok" });
        case "provision-control-root": {
          const controlPath = join(
            request.rootPath,
            ".stopped-directory-publication.lock",
          );
          let created = false;
          if (request.expectedControlFilesystemId === null) {
            try {
              await writeFile(controlPath, Buffer.alloc(0), {
                flag: "wx",
                mode: 0o600,
              });
              created = true;
            } catch (error) {
              if (error.code !== "EEXIST") throw error;
            }
          }
          let metadata;
          try {
            metadata = await stat(controlPath, { bigint: true });
          } catch {
            const error = new Error("expected control is absent");
            error.code = "operation_outcome_uncertain";
            throw error;
          }
          const persistentObjectId =
            state.controlObjectId ??
            objectId(`${controlPath}:${metadata.dev}:${metadata.ino}`);
          if (
            request.expectedControlFilesystemId !== null &&
            (request.expectedControlFilesystemId !== FILESYSTEM_ID ||
              request.expectedControlObjectId !== persistentObjectId)
          ) {
            const error = new Error("expected control identity mismatched");
            error.code = "operation_outcome_uncertain";
            throw error;
          }
          return Object.freeze({
            controlFileName: ".stopped-directory-publication.lock",
            controlFileIdentity: Object.freeze({
              device: String(metadata.dev),
              filesystemId: FILESYSTEM_ID,
              inode: String(metadata.ino),
              objectId: persistentObjectId,
              objectIdentityScheme: "linux-ext4-file-handle-sha256-v1",
            }),
            created,
            kind: "publication",
            status: "ok",
          });
        }
        default:
          throw new Error("unexpected fd operation");
      }
    },
  });
}

function createFixture(paths, overrides = {}) {
  const calls = [];
  const fdCalls = [];
  const loopDevice = overrides.loopDevice ?? LOOP_DEVICE;
  const state = {
    associatedLoops:
      overrides.associatedLoops ??
      (overrides.initiallyMounted === true ? [loopDevice] : []),
    backingPath: paths.imagePath,
    controlObjectId: null,
    loopAttached:
      overrides.loopAttached ??
      (overrides.initiallyMounted === true ||
        (overrides.associatedLoops?.length ?? 0) > 0),
    mounted: overrides.initiallyMounted === true,
    loopRdev: overrides.loopRdev ?? "7:7",
    private: overrides.private ?? true,
    safeOptions: overrides.safeOptions ?? true,
  };
  const commandRunner =
    overrides.commandRunner ??
    (async (executable, args, options) => {
      calls.push({ args, executable, options });
      if (overrides.beforeCommand !== undefined) {
        await overrides.beforeCommand(executable, args, state);
      }
      if (executable === "/usr/bin/getfacl") {
        return completion(
          overrides.aclOutput?.(args[3]) ??
            "user::rwx\ngroup::---\nother::---\n",
        );
      }
      if (executable === "/usr/sbin/mkfs.ext4") return completion();
      if (executable === "/usr/sbin/losetup") {
        if (args[0] === "--find") {
          state.loopAttached = true;
          state.associatedLoops = [loopDevice];
          return completion(`${loopDevice}\n`);
        }
        if (args[0] === "--noheadings") {
          if (args[3] === "NAME") {
            return completion(
              state.associatedLoops.length === 0
                ? ""
                : `${state.associatedLoops.join("\n")}\n`,
            );
          }
          if (!state.loopAttached) throw new Error("loop is detached");
          return completion(`${state.backingPath}\n`);
        }
        if (args[0] === "--detach") {
          state.loopAttached = false;
          state.associatedLoops = state.associatedLoops.filter(
            (loop) => loop !== args[2],
          );
          return completion();
        }
      }
      if (executable === "/usr/bin/mount") {
        if (args[0] === "--make-private") {
          state.private = true;
          return completion();
        }
        state.mounted = true;
        state.safeOptions = true;
        return completion();
      }
      if (executable === "/usr/bin/sync") return completion();
      if (executable === "/usr/bin/umount") {
        state.mounted = false;
        return completion();
      }
      throw new Error("unexpected command");
    });
  const readMountInfo =
    overrides.readMountInfo ??
    (async () => {
      const rootLine = "29 23 0:25 / / rw,relatime - ext4 /dev/root rw\n";
      if (!state.mounted) return Buffer.from(rootLine, "utf8");
      const options = state.safeOptions
        ? "rw,nosuid,nodev,noexec,noatime"
        : "ro,relatime";
      const optional = state.private ? "" : " shared:8";
      const superOptions = state.safeOptions
        ? "rw,errors=remount-ro"
        : "ro,errors=continue";
      return Buffer.from(
        `${rootLine}36 29 7:7 / ${encodeMountField(paths.mountPath)} ${options}${optional} - ext4 ${loopDevice} ${superOptions}\n`,
        "utf8",
      );
    });
  const driver = createLinuxExt4ImageDriver({
    commandRunner,
    inspector:
      overrides.inspector ?? createInspector(paths, state, fdCalls, overrides),
    platform: "linux",
    readMountInfo,
  });
  return { calls, driver, fdCalls, state };
}

function mountRequest(paths) {
  return { imagePath: paths.imagePath, mountPath: paths.mountPath };
}

function provisionRequest(paths) {
  return { ...mountRequest(paths), imageSizeBytes: 1024 * 1024 };
}

function driverError(code) {
  return (error) => {
    assert(error instanceof LinuxExt4ImageDriverError);
    assert.equal(error.code, code);
    assert.equal(error.retryable, false);
    assert.equal(Object.isFrozen(error), true);
    return true;
  };
}

test("surface and successful results are exact frozen data with native Promises", async () => {
  const paths = await createPaths();
  const fixture = createFixture(paths);
  assert.equal(Object.getPrototypeOf(fixture.driver), null);
  assert.equal(Object.isFrozen(fixture.driver), true);
  assert.deepEqual(Reflect.ownKeys(fixture.driver), [
    "contractVersion",
    "provision",
    "observeMount",
    "observeAttachmentRoot",
    "remount",
    "ensureAttachmentRoot",
    "ensurePublicationRoot",
    "syncFilesystem",
    "quiesce",
    "destroy",
  ]);
  assert.equal(
    fixture.driver.contractVersion,
    LINUX_EXT4_IMAGE_DRIVER_CONTRACT_VERSION,
  );
  for (const key of Reflect.ownKeys(fixture.driver).slice(1)) {
    assert.equal(Object.isFrozen(fixture.driver[key]), true);
  }

  const pending = fixture.driver.provision(provisionRequest(paths));
  assert.strictEqual(Object.getPrototypeOf(pending), Promise.prototype);
  const result = await pending;
  assert.equal(Object.getPrototypeOf(result), null);
  assert.equal(Object.isFrozen(result), true);
  assert.equal(result.imagePath, paths.imagePath);
  assert.equal(result.mountPath, paths.mountPath);
  assert.equal(result.loopDevice, LOOP_DEVICE);
  assert.equal(result.filesystem.filesystemId, FILESYSTEM_ID);
  assert.equal(result.imageIdentity.filesystemId, FILESYSTEM_ID);
  assert.equal(
    result.imageIdentity.objectIdentityScheme,
    "ext4-filesystem-image-v1",
  );
  assert.match(result.imageIdentity.objectId, /^ext4image1:[0-9a-f]{64}$/u);
  assert.equal(result.rootIdentity.filesystemId, FILESYSTEM_ID);
  assert.notEqual(
    result.imageIdentity.objectIdentityScheme,
    result.rootIdentity.objectIdentityScheme,
  );
  assert.equal(result.mountEvidence.propagation, "private");
  assert.equal(Object.isFrozen(result.filesystem), true);
  assert.equal(Object.isFrozen(result.imageIdentity), true);
  assert.equal(Object.isFrozen(result.rootIdentity), true);
  assert.equal(Object.isFrozen(result.mountEvidence), true);
});

test("provision rejects image sizes that cannot bind exact loop geometry", async () => {
  const paths = await createPaths();
  const fixture = createFixture(paths);
  await assert.rejects(
    fixture.driver.provision({
      ...mountRequest(paths),
      imageSizeBytes: 1024 * 1024 + 1,
    }),
    driverError("invalid_request"),
  );
  assert.equal(fixture.fdCalls.length, 0);
});

test("driver loop receipts use the canonical native 0..4095 domain", async () => {
  const acceptedPaths = await createPaths();
  const accepted = createFixture(acceptedPaths, {
    loopDevice: "/dev/loop4095",
  });
  assert.equal(
    (await accepted.driver.provision(provisionRequest(acceptedPaths))).loopDevice,
    "/dev/loop4095",
  );

  for (const loopDevice of ["/dev/loop4096", "/dev/loop04095"]) {
    const rejectedPaths = await createPaths();
    const rejected = createFixture(rejectedPaths, { loopDevice });
    await assert.rejects(
      rejected.driver.provision(provisionRequest(rejectedPaths)),
      driverError("operation_outcome_uncertain"),
    );
    assert.equal(
      rejected.fdCalls.some(({ operation }) => operation === "mount-ext4"),
      false,
    );
  }
});

test("provision dispatches every mutation through exact fd-bound requests", async () => {
  const paths = await createPaths();
  const fixture = createFixture(paths);
  await fixture.driver.provision(provisionRequest(paths));

  assert.deepEqual(
    fixture.fdCalls.slice(0, 5).map(({ operation }) => operation),
    [
      "create-image",
      "format-ext4",
      "create-directory",
      "attach-loop",
      "mount-ext4",
    ],
  );
  const mount = fixture.fdCalls.find(
    ({ operation }) => operation === "mount-ext4",
  );
  assert.deepEqual(Reflect.ownKeys(mount), [
    "backingDevice",
    "backingInode",
    "loopDevice",
    "loopRdev",
    "operation",
    "parentDevice",
    "parentInode",
    "path",
    "sizeBytes",
    "targetDevice",
    "targetInode",
  ]);
  assert.equal(mount.loopRdev, "7:7");
  assert.equal(mount.path, paths.mountPath);
  assert.equal(
    fixture.fdCalls.some(
      ({ operation }) => operation === "provision-control-root",
    ),
    true,
  );
  assert.equal(
    fixture.calls.every(({ executable }) => executable === "/usr/bin/getfacl"),
    true,
  );
  for (const call of fixture.calls) {
    assert.equal(call.executable.startsWith("/"), true);
    assert.equal(Object.isFrozen(call.args), true);
    assert.equal(Object.isFrozen(call.options), true);
    assert.equal(call.options.shell, false);
    assert.equal(call.options.cwd, "/");
    assert.equal(call.options.killSignal, "SIGTERM");
    assert.equal(Object.hasOwn(call.options.env, "PATH"), false);
    assert.deepEqual({ ...call.options.env }, { LANG: "C", LC_ALL: "C" });
  }
});

test("publication control identity is pinned, persistent, and replay-bound", async () => {
  const paths = await createPaths();
  const fixture = createFixture(paths);
  await fixture.driver.provision(provisionRequest(paths));

  const first = await fixture.driver.ensurePublicationRoot(mountRequest(paths));
  assert.deepEqual(Reflect.ownKeys(first), [
    "controlFileIdentity",
    "controlFileName",
    "created",
    "filesystem",
    "imageIdentity",
    "imagePath",
    "loopDevice",
    "mountEvidence",
    "mountPath",
    "mountRootIdentity",
    "publicationControlIdentity",
  ]);
  assert.deepEqual(Reflect.ownKeys(first.controlFileIdentity), [
    "device",
    "inode",
    "filesystemId",
    "objectIdentityScheme",
    "objectId",
  ]);
  assert.deepEqual({ ...first.publicationControlIdentity }, {
    filesystemId: first.controlFileIdentity.filesystemId,
    objectIdentityScheme: first.controlFileIdentity.objectIdentityScheme,
    objectId: first.controlFileIdentity.objectId,
  });
  assert.equal(Object.isFrozen(first.controlFileIdentity), true);
  assert.equal(Object.isFrozen(first.publicationControlIdentity), true);
  const controlRequest = fixture.fdCalls.findLast(
    ({ operation }) => operation === "provision-control-root",
  );
  assert.equal(
    controlRequest.filesystemId,
    first.mountRootIdentity.filesystemId,
  );
  assert.equal(controlRequest.objectId, first.mountRootIdentity.objectId);
  assert.equal(controlRequest.expectedControlFilesystemId, null);
  assert.equal(controlRequest.expectedControlObjectId, null);

  const explicitNull = await fixture.driver.ensurePublicationRoot({
    ...mountRequest(paths),
    expectedPublicationControlIdentity: null,
  });
  assert.deepEqual(
    explicitNull.publicationControlIdentity,
    first.publicationControlIdentity,
  );

  const replay = await fixture.driver.ensurePublicationRoot({
    ...mountRequest(paths),
    expectedPublicationControlIdentity: first.publicationControlIdentity,
  });
  assert.deepEqual(
    replay.publicationControlIdentity,
    first.publicationControlIdentity,
  );
  const replayRequest = fixture.fdCalls.findLast(
    ({ operation }) => operation === "provision-control-root",
  );
  assert.equal(
    replayRequest.expectedControlFilesystemId,
    first.publicationControlIdentity.filesystemId,
  );
  assert.equal(
    replayRequest.expectedControlObjectId,
    first.publicationControlIdentity.objectId,
  );

  const controlPath = join(
    paths.mountPath,
    ".stopped-directory-publication.lock",
  );
  await unlink(controlPath);
  await assert.rejects(
    fixture.driver.ensurePublicationRoot({
      ...mountRequest(paths),
      expectedPublicationControlIdentity: first.publicationControlIdentity,
    }),
    driverError("operation_outcome_uncertain"),
  );
  await assert.rejects(stat(controlPath), { code: "ENOENT" });

  await writeFile(controlPath, Buffer.alloc(0), { flag: "wx", mode: 0o600 });
  fixture.state.controlObjectId = `ext4fh1:${"c".repeat(64)}`;
  await assert.rejects(
    fixture.driver.ensurePublicationRoot({
      ...mountRequest(paths),
      expectedPublicationControlIdentity: first.publicationControlIdentity,
    }),
    driverError("operation_outcome_uncertain"),
  );
});

test("same-image operations are serialized across driver instances", async () => {
  const paths = await createPaths();
  let armed = false;
  let releaseFirst;
  let markFirst;
  const firstReached = new Promise((resolve) => {
    markFirst = resolve;
  });
  const release = new Promise((resolve) => {
    releaseFirst = resolve;
  });
  const fixture = createFixture(paths, {
    async beforeFdOperation(request) {
      if (!armed || request.operation !== "syncfs") return;
      armed = false;
      markFirst();
      await release;
    },
  });
  await fixture.driver.provision(provisionRequest(paths));
  fixture.fdCalls.length = 0;
  armed = true;
  const first = fixture.driver.syncFilesystem(mountRequest(paths));
  await firstReached;
  const callsAtGate = fixture.fdCalls.length;
  const secondDriver = createLinuxExt4ImageDriver({
    commandRunner: async (executable) => {
      if (executable === "/usr/bin/getfacl") {
        return completion("user::rwx\ngroup::---\nother::---\n");
      }
      throw new Error("unexpected external command");
    },
    inspector: createInspector(paths, fixture.state, fixture.fdCalls, {}),
    platform: "linux",
    readMountInfo: async () => {
      const options = "rw,nosuid,nodev,noexec,noatime";
      return Buffer.from(
        `29 23 0:25 / / rw - ext4 /dev/root rw\n36 29 7:7 / ${encodeMountField(paths.mountPath)} ${options} - ext4 ${LOOP_DEVICE} rw,errors=remount-ro\n`,
      );
    },
  });
  const second = secondDriver.syncFilesystem(mountRequest(paths));
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(fixture.fdCalls.length, callsAtGate);
  releaseFirst();
  await Promise.all([first, second]);
  assert.equal(
    fixture.fdCalls.filter(({ operation }) => operation === "syncfs").length,
    2,
  );
});

test("exclusive image creation never formats or attaches an existing path", async () => {
  const paths = await createPaths();
  await writeFile(paths.imagePath, "owned by someone else", { mode: 0o600 });
  const fixture = createFixture(paths);
  await assert.rejects(
    fixture.driver.provision(provisionRequest(paths)),
    driverError("image_exists"),
  );
  assert.deepEqual(
    fixture.fdCalls.map(({ operation }) => operation),
    ["create-image"],
  );
  assert.equal(
    fixture.calls.every(
      ({ executable }) => executable === "/usr/bin/getfacl",
    ),
    true,
  );
  assert.equal(await readFile(paths.imagePath, "utf8"), "owned by someone else");
});

test("a mutation dispatch failure is uncertain and retains its exclusively-created image", async () => {
  const paths = await createPaths();
  const fixture = createFixture(paths, {
    fdOperationError(request) {
      return request.operation === "format-ext4";
    },
  });
  await assert.rejects(
    fixture.driver.provision(provisionRequest(paths)),
    driverError("operation_outcome_uncertain"),
  );
  assert.deepEqual(
    fixture.fdCalls.map(({ operation }) => operation),
    ["create-image", "format-ext4"],
  );
  assert.equal((await stat(paths.imagePath)).isFile(), true);
});

test("post-dispatch access-policy uncertainty never becomes a conclusive mismatch", async () => {
  const paths = await createPaths();
  let dispatched = false;
  const fixture = createFixture(paths, {
    aclOutput(path) {
      if (dispatched && path === paths.mountPath) {
        return "user::rwx\nuser:123:rwx\ngroup::---\nmask::rwx\nother::---\n";
      }
      return "user::rwx\ngroup::---\nother::---\n";
    },
    beforeFdOperation(request) {
      if (request.operation === "mount-ext4") dispatched = true;
    },
  });
  await assert.rejects(
    fixture.driver.provision(provisionRequest(paths)),
    driverError("operation_outcome_uncertain"),
  );
  assert.equal(dispatched, true);
  assert.equal(fixture.state.mounted, true);
});

test("observeMount is read-only and fails closed on backing or mount-policy mismatch", async () => {
  const paths = await createPaths();
  const fixture = createFixture(paths);
  await fixture.driver.provision(provisionRequest(paths));
  fixture.calls.length = 0;
  fixture.fdCalls.length = 0;
  const observed = await fixture.driver.observeMount(mountRequest(paths));
  assert.equal(observed.loopDevice, LOOP_DEVICE);
  assert.equal(fixture.state.mounted, true);
  assert.equal(
    fixture.calls.every(
      ({ executable, args }) =>
        executable === "/usr/bin/getfacl" ||
        (executable === "/usr/sbin/losetup" && args[0] === "--noheadings"),
    ),
    true,
  );
  assert.deepEqual(
    fixture.fdCalls.map(({ operation }) => operation),
    ["inspect-loop", "inspect-loop"],
  );

  fixture.state.backingPath = `${paths.imagePath}.replacement`;
  await assert.rejects(
    fixture.driver.observeMount(mountRequest(paths)),
    driverError("backing_mismatch"),
  );
  assert.equal(fixture.state.mounted, true);

  fixture.state.backingPath = paths.imagePath;
  fixture.state.loopRdev = "7:8";
  await assert.rejects(
    fixture.driver.observeMount(mountRequest(paths)),
    driverError("backing_mismatch"),
  );
  fixture.state.loopRdev = "7:7";

  fixture.state.private = false;
  await assert.rejects(
    fixture.driver.observeMount(mountRequest(paths)),
    driverError("mount_mismatch"),
  );
  assert.equal(fixture.state.mounted, true);
});

test("mountinfo loop sources use the canonical native 0..4095 domain", async () => {
  async function mountedFixture(loopDevice) {
    const paths = await createPaths();
    await writeFile(paths.imagePath, Buffer.alloc(1024), { mode: 0o600 });
    await mkdir(paths.mountPath, { mode: 0o700 });
    return {
      fixture: createFixture(paths, { initiallyMounted: true, loopDevice }),
      paths,
    };
  }

  const accepted = await mountedFixture("/dev/loop4095");
  assert.equal(
    (await accepted.fixture.driver.observeMount(mountRequest(accepted.paths)))
      .loopDevice,
    "/dev/loop4095",
  );

  for (const loopDevice of ["/dev/loop4096", "/dev/loop04095"]) {
    const rejected = await mountedFixture(loopDevice);
    await assert.rejects(
      rejected.fixture.driver.observeMount(mountRequest(rejected.paths)),
      driverError("mount_mismatch"),
    );
    assert.equal(rejected.fixture.fdCalls.length, 0);
  }
});

test("cold remount reuses one exact existing loop and performs an ordinary safe mount", async () => {
  const paths = await createPaths();
  await writeFile(paths.imagePath, Buffer.alloc(1024), { mode: 0o600 });
  await mkdir(paths.mountPath, { mode: 0o700 });
  const controlPath = join(
    paths.mountPath,
    ".stopped-directory-publication.lock",
  );
  await writeFile(controlPath, Buffer.alloc(0), { mode: 0o600 });
  const controlMetadata = await stat(controlPath, { bigint: true });
  const expectedPublicationControlIdentity = Object.freeze({
    filesystemId: FILESYSTEM_ID,
    objectIdentityScheme: "linux-ext4-file-handle-sha256-v1",
    objectId: objectId(
      `${controlPath}:${controlMetadata.dev}:${controlMetadata.ino}`,
    ),
  });
  const fixture = createFixture(paths, {
    associatedLoops: [LOOP_DEVICE],
    initiallyMounted: false,
    loopAttached: true,
    private: false,
    safeOptions: false,
  });
  const result = await fixture.driver.remount({
    ...mountRequest(paths),
    expectedPublicationControlIdentity,
  });
  assert.equal(result.loopDevice, LOOP_DEVICE);
  assert.equal(result.mountEvidence.propagation, "private");
  assert.equal(fixture.state.mounted, true);
  assert.equal(fixture.state.loopAttached, true);
  assert.equal(
    fixture.fdCalls.some(({ operation }) => operation === "attach-loop"),
    false,
  );
  const remount = fixture.fdCalls.find(
    ({ operation }) => operation === "mount-ext4",
  );
  assert.equal(remount.loopDevice, LOOP_DEVICE);
  assert.equal(remount.loopRdev, "7:7");
  assert.equal(remount.path, paths.mountPath);
  const controlRequest = fixture.fdCalls.findLast(
    ({ operation }) => operation === "provision-control-root",
  );
  assert.equal(
    controlRequest.expectedControlFilesystemId,
    expectedPublicationControlIdentity.filesystemId,
  );
  assert.equal(
    controlRequest.expectedControlObjectId,
    expectedPublicationControlIdentity.objectId,
  );
  assert.equal(
    fixture.calls.every(({ executable }) => executable === "/usr/bin/getfacl"),
    true,
  );
});

test("cold remount attaches only when no loop exists and rejects ambiguous loops", async () => {
  const newPaths = await createPaths();
  await writeFile(newPaths.imagePath, Buffer.alloc(1024), { mode: 0o600 });
  await mkdir(newPaths.mountPath, { mode: 0o700 });
  const fresh = createFixture(newPaths);
  await fresh.driver.remount(mountRequest(newPaths));
  assert.equal(
    fresh.fdCalls.some(({ operation }) => operation === "attach-loop"),
    true,
  );
  assert.equal(fresh.state.mounted, true);

  const ambiguousPaths = await createPaths();
  await writeFile(ambiguousPaths.imagePath, Buffer.alloc(1024), { mode: 0o600 });
  await mkdir(ambiguousPaths.mountPath, { mode: 0o700 });
  const ambiguous = createFixture(ambiguousPaths, {
    associatedLoops: [LOOP_DEVICE, "/dev/loop8"],
    loopAttached: true,
  });
  await assert.rejects(
    ambiguous.driver.remount(mountRequest(ambiguousPaths)),
    driverError("loop_ambiguous"),
  );
  assert.equal(ambiguous.state.mounted, false);
  assert.equal(
    ambiguous.fdCalls.some(
      ({ operation }) =>
        operation === "mount-ext4" || operation === "attach-loop",
    ),
    false,
  );
});

test("observeMount reports a conclusive absent target separately", async () => {
  const paths = await createPaths();
  await writeFile(paths.imagePath, Buffer.alloc(1024), { mode: 0o600 });
  await mkdir(paths.mountPath, { mode: 0o700 });
  const fixture = createFixture(paths);
  await assert.rejects(
    fixture.driver.observeMount(mountRequest(paths)),
    driverError("mount_absent"),
  );
  assert.equal(fixture.calls.length, 0);
});

test("ensureAttachmentRoot accepts only an exact direct child and preserves the mount", async () => {
  const paths = await createPaths();
  const fixture = createFixture(paths);
  await fixture.driver.provision(provisionRequest(paths));
  const first = await fixture.driver.ensureAttachmentRoot({
    ...mountRequest(paths),
    attachmentRootPath: paths.attachmentRootPath,
  });
  assert.equal(first.created, true);
  assert.equal(first.attachmentRootPath, paths.attachmentRootPath);
  assert.equal(first.rootIdentity.filesystemId, FILESYSTEM_ID);
  assert.notEqual(first.rootIdentity.objectId, first.mountRootIdentity.objectId);
  assert.equal((await stat(paths.attachmentRootPath)).mode & 0o777, 0o700);
  const second = await fixture.driver.ensureAttachmentRoot({
    ...mountRequest(paths),
    attachmentRootPath: paths.attachmentRootPath,
  });
  assert.equal(second.created, false);
  assert.equal(fixture.state.mounted, true);
  assert.equal(
    fixture.calls.some(
      ({ executable, args }) =>
        executable === "/usr/bin/umount" || args[0] === "--detach",
    ),
    false,
  );

  await assert.rejects(
    fixture.driver.ensureAttachmentRoot({
      ...mountRequest(paths),
      attachmentRootPath: join(paths.attachmentRootPath, "nested"),
    }),
    driverError("invalid_request"),
  );
});

test("observeAttachmentRoot is read-only and distinguishes an absent child", async () => {
  const paths = await createPaths();
  const fixture = createFixture(paths);
  await fixture.driver.provision(provisionRequest(paths));
  fixture.calls.length = 0;
  await assert.rejects(
    fixture.driver.observeAttachmentRoot({
      ...mountRequest(paths),
      attachmentRootPath: paths.attachmentRootPath,
    }),
    driverError("attachment_root_absent"),
  );
  await assert.rejects(stat(paths.attachmentRootPath), { code: "ENOENT" });
  assert.equal(
    fixture.calls.every(
      ({ executable, args }) =>
        executable === "/usr/bin/getfacl" ||
        (executable === "/usr/sbin/losetup" && args[0] === "--noheadings"),
    ),
    true,
  );

  const ensured = await fixture.driver.ensureAttachmentRoot({
    ...mountRequest(paths),
    attachmentRootPath: paths.attachmentRootPath,
  });
  fixture.calls.length = 0;
  const observed = await fixture.driver.observeAttachmentRoot({
    ...mountRequest(paths),
    attachmentRootPath: paths.attachmentRootPath,
  });
  assert.equal(Object.hasOwn(observed, "created"), false);
  assert.equal(observed.rootIdentity.objectId, ensured.rootIdentity.objectId);
  assert.equal(fixture.state.mounted, true);
  assert.equal(
    fixture.calls.some(
      ({ executable, args }) =>
        executable === "/usr/bin/umount" ||
        executable === "/usr/bin/sync" ||
        args[0] === "--detach" ||
        args[0] === "--find",
    ),
    false,
  );
});

test("observeAttachmentRoot reports absence only across one stable mount", async () => {
  const paths = await createPaths();
  await writeFile(paths.imagePath, Buffer.alloc(1024), { mode: 0o600 });
  await mkdir(paths.mountPath, { mode: 0o700 });
  const rootLine = "29 23 0:25 / / rw,relatime - ext4 /dev/root rw\n";
  const mountLine = `36 29 7:7 / ${encodeMountField(paths.mountPath)} rw,nosuid,nodev,noexec,noatime - ext4 ${LOOP_DEVICE} rw,errors=remount-ro\n`;
  let mountReads = 0;
  const fixture = createFixture(paths, {
    initiallyMounted: true,
    async readMountInfo() {
      mountReads += 1;
      return Buffer.from(
        mountReads <= 2 ? `${rootLine}${mountLine}` : rootLine,
        "utf8",
      );
    },
  });

  await assert.rejects(
    fixture.driver.observeAttachmentRoot({
      ...mountRequest(paths),
      attachmentRootPath: paths.attachmentRootPath,
    }),
    driverError("observation_failed"),
  );
  assert.equal(mountReads, 3);
});

test("syncFilesystem uses pinned syncfs and keeps the mount", async () => {
  const paths = await createPaths();
  const fixture = createFixture(paths);
  await fixture.driver.provision(provisionRequest(paths));
  fixture.calls.length = 0;
  fixture.fdCalls.length = 0;
  const result = await fixture.driver.syncFilesystem(mountRequest(paths));
  assert.equal(result.status, "synced");
  assert.equal(result.mount.loopDevice, LOOP_DEVICE);
  assert.equal(fixture.state.mounted, true);
  const syncCall = fixture.fdCalls.find(
    ({ operation }) => operation === "syncfs",
  );
  assert.equal(syncCall.path, paths.mountPath);
  assert.equal(syncCall.device, result.mount.mountEvidence.rootDevice);
  assert.equal(syncCall.inode, result.mount.mountEvidence.rootInode);
  assert.equal(syncCall.filesystemId, result.mount.rootIdentity.filesystemId);
  assert.equal(syncCall.objectId, result.mount.rootIdentity.objectId);
  assert.equal(
    fixture.calls.some(
      ({ executable, args }) =>
        executable === "/usr/bin/umount" || args[0] === "--detach",
    ),
    false,
  );
});

test("quiesce orders fd-bound teardown, preserves storage, and is idempotent", async () => {
  const paths = await createPaths();
  const fixture = createFixture(paths);
  await fixture.driver.provision(provisionRequest(paths));
  fixture.calls.length = 0;
  fixture.fdCalls.length = 0;
  const first = await fixture.driver.quiesce(mountRequest(paths));
  assert.deepEqual({ ...first }, {
    imagePath: paths.imagePath,
    mountPath: paths.mountPath,
    status: "quiesced",
  });
  const mutations = fixture.fdCalls
    .map(({ operation }) => operation)
    .filter((operation) =>
      ["syncfs", "unmount-ext4", "detach-loop-settle"].includes(operation),
    );
  assert.deepEqual(mutations, [
    "syncfs",
    "unmount-ext4",
    "detach-loop-settle",
  ]);
  const unmount = fixture.fdCalls.find(
    ({ operation }) => operation === "unmount-ext4",
  );
  assert.equal(unmount.targetFilesystemId, FILESYSTEM_ID);
  assert.equal(unmount.targetObjectId, objectId(paths.mountPath));
  assert.equal((await stat(paths.imagePath)).isFile(), true);
  assert.equal((await stat(paths.mountPath)).isDirectory(), true);
  assert.equal(fixture.state.mounted, false);
  assert.equal(fixture.state.loopAttached, false);

  fixture.calls.length = 0;
  fixture.fdCalls.length = 0;
  const replay = await fixture.driver.quiesce(mountRequest(paths));
  assert.deepEqual({ ...replay }, { ...first });
  assert.equal(
    fixture.fdCalls.some(({ operation }) =>
      ["syncfs", "unmount-ext4", "detach-loop-settle"].includes(operation),
    ),
    false,
  );
});

test("destroy orders fd-bound sync, unmount, detach, unlink, and rmdir", async () => {
  const paths = await createPaths();
  const fixture = createFixture(paths);
  await fixture.driver.provision(provisionRequest(paths));
  fixture.calls.length = 0;
  fixture.fdCalls.length = 0;
  const result = await fixture.driver.destroy(mountRequest(paths));
  assert.deepEqual({ ...result }, {
    imagePath: paths.imagePath,
    mountPath: paths.mountPath,
    status: "destroyed",
  });
  const mutations = fixture.fdCalls
    .map(({ operation }) => operation)
    .filter((operation) =>
      [
        "syncfs",
        "unmount-ext4",
        "detach-loop-settle",
        "remove-file",
        "remove-directory",
      ].includes(operation),
    );
  assert.deepEqual(mutations, [
    "syncfs",
    "unmount-ext4",
    "detach-loop-settle",
    "remove-file",
    "remove-directory",
  ]);
  await assert.rejects(stat(paths.imagePath), { code: "ENOENT" });
  await assert.rejects(stat(paths.mountPath), { code: "ENOENT" });
  assert.equal(fixture.state.loopAttached, false);
  assert.equal(fixture.state.mounted, false);
  assert.equal(
    fixture.calls.every(({ executable }) => executable === "/usr/bin/getfacl"),
    true,
  );
});

test("non-Linux construction fails closed before any collaborator runs", async () => {
  let calls = 0;
  const inspector = createInspector();
  assert.throws(
    () =>
      createLinuxExt4ImageDriver({
        commandRunner: async () => {
          calls += 1;
          return completion();
        },
        inspector,
        platform: "darwin",
        readMountInfo: async () => {
          calls += 1;
          return Buffer.alloc(0);
        },
      }),
    driverError("unsupported_platform"),
  );
  assert.equal(calls, 0);
});

test("private owner/mode/ACL policy fails before provisioning mutations", async () => {
  const modePaths = await createPaths();
  await chmod(join(modePaths.root, "images"), 0o750);
  const modeFixture = createFixture(modePaths);
  await assert.rejects(
    modeFixture.driver.provision(provisionRequest(modePaths)),
    driverError("access_policy_mismatch"),
  );
  await assert.rejects(stat(modePaths.imagePath), { code: "ENOENT" });
  assert.equal(modeFixture.calls.length, 0);

  const aclPaths = await createPaths();
  const aclFixture = createFixture(aclPaths, {
    aclOutput(path) {
      return path === join(aclPaths.root, "images")
        ? "user::rwx\nuser:123:rwx\ngroup::---\nmask::rwx\nother::---\n"
        : "user::rwx\ngroup::---\nother::---\n";
    },
  });
  await assert.rejects(
    aclFixture.driver.provision(provisionRequest(aclPaths)),
    driverError("access_policy_mismatch"),
  );
  await assert.rejects(stat(aclPaths.imagePath), { code: "ENOENT" });
  assert.equal(
    aclFixture.calls.every(
      ({ executable }) => executable === "/usr/bin/getfacl",
    ),
    true,
  );
});

test("image and mount-directory replacement races are detected before later mutation", async () => {
  const imagePaths = await createPaths();
  let imageReplaced = false;
  const imageFixture = createFixture(imagePaths, {
    async beforeFdOperation(request) {
      if (request.operation !== "format-ext4" || imageReplaced) return;
      imageReplaced = true;
      await rm(imagePaths.imagePath, { force: true });
      await writeFile(imagePaths.imagePath, Buffer.alloc(1024 * 1024), {
        mode: 0o600,
      });
    },
  });
  await assert.rejects(
    imageFixture.driver.provision(provisionRequest(imagePaths)),
    driverError("operation_outcome_uncertain"),
  );
  assert.equal(imageReplaced, true);
  assert.equal(
    imageFixture.fdCalls.some(({ operation }) => operation === "attach-loop"),
    false,
  );

  const mountPaths = await createPaths();
  let mountReplaced = false;
  const mountFixture = createFixture(mountPaths, {
    async beforeFdOperation(request) {
      if (request.operation !== "attach-loop" || mountReplaced) {
        return;
      }
      mountReplaced = true;
      await rm(mountPaths.mountPath, { recursive: true });
      await mkdir(mountPaths.mountPath, { mode: 0o700 });
    },
  });
  await assert.rejects(
    mountFixture.driver.provision(provisionRequest(mountPaths)),
    driverError("operation_outcome_uncertain"),
  );
  assert.equal(mountReplaced, true);
  assert.equal(
    mountFixture.fdCalls.some(({ operation }) => operation === "mount-ext4"),
    false,
  );
});

test("hostile Proxy/accessor inputs execute no traps and receivers are authentic", async () => {
  const paths = await createPaths();
  const fixture = createFixture(paths);
  await fixture.driver.provision(provisionRequest(paths));
  let trapCalls = 0;
  const traps = {
    get() {
      trapCalls += 1;
      throw new Error("get trap must not run");
    },
    getOwnPropertyDescriptor() {
      trapCalls += 1;
      throw new Error("descriptor trap must not run");
    },
    getPrototypeOf() {
      trapCalls += 1;
      throw new Error("prototype trap must not run");
    },
    ownKeys() {
      trapCalls += 1;
      throw new Error("ownKeys trap must not run");
    },
  };
  const proxyPending = fixture.driver.observeMount(new Proxy({}, traps));
  assert.strictEqual(Object.getPrototypeOf(proxyPending), Promise.prototype);
  await assert.rejects(proxyPending, driverError("invalid_request"));
  assert.equal(trapCalls, 0);

  let getterCalls = 0;
  const accessor = { mountPath: paths.mountPath };
  Object.defineProperty(accessor, "imagePath", {
    enumerable: true,
    get() {
      getterCalls += 1;
      throw new Error("getter must not run");
    },
  });
  await assert.rejects(
    fixture.driver.observeMount(accessor),
    driverError("invalid_request"),
  );
  assert.equal(getterCalls, 0);

  const clone = Object.freeze({ ...fixture.driver });
  const requests = {
    destroy: mountRequest(paths),
    ensureAttachmentRoot: {
      ...mountRequest(paths),
      attachmentRootPath: paths.attachmentRootPath,
    },
    ensurePublicationRoot: mountRequest(paths),
    observeAttachmentRoot: {
      ...mountRequest(paths),
      attachmentRootPath: paths.attachmentRootPath,
    },
    observeMount: mountRequest(paths),
    provision: provisionRequest(paths),
    quiesce: mountRequest(paths),
    remount: mountRequest(paths),
    syncFilesystem: mountRequest(paths),
  };
  for (const method of Object.keys(requests)) {
    assert.equal(fixture.driver[method].length, 1);
    for (const receiver of [undefined, null, {}, clone]) {
      assert.throws(
        () =>
          Reflect.apply(fixture.driver[method], receiver, [requests[method]]),
        TypeError,
      );
    }
    assert.throws(() => fixture.driver[method](), TypeError);
  }
});

test("non-native collaborator Promises fail closed without thenable execution", async () => {
  const paths = await createPaths();
  await writeFile(paths.imagePath, Buffer.alloc(1024), { mode: 0o600 });
  await mkdir(paths.mountPath, { mode: 0o700 });
  let trapCalls = 0;
  const promise = Promise.resolve(
    Buffer.from(
      `29 23 0:25 / / rw - ext4 /dev/root rw\n36 29 7:7 / ${encodeMountField(paths.mountPath)} rw,nosuid,nodev,noexec,noatime - ext4 ${LOOP_DEVICE} rw,errors=remount-ro\n`,
      "utf8",
    ),
  );
  const proxiedPromise = new Proxy(promise, {
    get() {
      trapCalls += 1;
      throw new Error("then getter must not run");
    },
  });
  const fixture = createFixture(paths, {
    initiallyMounted: true,
    readMountInfo() {
      return proxiedPromise;
    },
  });
  await assert.rejects(
    fixture.driver.observeMount(mountRequest(paths)),
    driverError("observation_failed"),
  );
  assert.equal(trapCalls, 0);
});

test("collaborator accessor and Proxy records are rejected without field traps", async () => {
  const accessorPaths = await createPaths();
  let getterCalls = 0;
  const accessorCompletion = { stderr: Buffer.alloc(0) };
  Object.defineProperty(accessorCompletion, "stdout", {
    enumerable: true,
    get() {
      getterCalls += 1;
      throw new Error("stdout getter must not run");
    },
  });
  const accessorDriver = createLinuxExt4ImageDriver({
    commandRunner() {
      return Promise.resolve(accessorCompletion);
    },
    inspector: createInspector(),
    platform: "linux",
    readMountInfo: async () => Buffer.alloc(0),
  });
  await assert.rejects(
    accessorDriver.provision(provisionRequest(accessorPaths)),
    driverError("image_io_failed"),
  );
  assert.equal(getterCalls, 0);

  const proxyPaths = await createPaths();
  let trapCalls = 0;
  const proxiedCompletion = new Proxy(completion(), {
    get(target, key) {
      if (key === "then") return undefined;
      trapCalls += 1;
      throw new Error("completion trap must not run");
    },
    getOwnPropertyDescriptor() {
      trapCalls += 1;
      throw new Error("completion descriptor trap must not run");
    },
    getPrototypeOf() {
      trapCalls += 1;
      throw new Error("completion prototype trap must not run");
    },
    ownKeys() {
      trapCalls += 1;
      throw new Error("completion ownKeys trap must not run");
    },
  });
  const proxyDriver = createLinuxExt4ImageDriver({
    commandRunner() {
      return Promise.resolve(proxiedCompletion);
    },
    inspector: createInspector(),
    platform: "linux",
    readMountInfo: async () => Buffer.alloc(0),
  });
  await assert.rejects(
    proxyDriver.provision(provisionRequest(proxyPaths)),
    driverError("image_io_failed"),
  );
  assert.equal(trapCalls, 0);
});

test("captured intrinsics preserve observation after post-import poisoning", async () => {
  const paths = await createPaths();
  await writeFile(paths.imagePath, Buffer.alloc(1024), { mode: 0o600 });
  await mkdir(paths.mountPath, { mode: 0o700 });
  const metadata = await stat(paths.mountPath, { bigint: true });
  const imageMetadata = await stat(paths.imagePath, { bigint: true });
  const moduleUrl = new URL(
    "../src/linux-ext4-image-driver.mjs",
    import.meta.url,
  ).href;
  const mountInfo =
    `29 23 0:25 / / rw - ext4 /dev/root rw\n` +
    `36 29 7:7 / ${encodeMountField(paths.mountPath)} rw,nosuid,nodev,noexec,noatime - ext4 ${LOOP_DEVICE} rw,errors=remount-ro\n`;
  const script = `
import { Buffer } from "node:buffer";
import { Hash } from "node:crypto";
import { TextDecoder } from "node:util";
import { createLinuxExt4ImageDriver } from ${JSON.stringify(moduleUrl)};

const filesystem = Object.freeze({
  durability: "local-fsync-rename",
  filesystemId: ${JSON.stringify(FILESYSTEM_ID)},
  objectIdentityScheme: "linux-ext4-file-handle-sha256-v1",
  type: "ext4",
});
const identity = Object.freeze({
  device: ${JSON.stringify(String(metadata.dev))},
  inode: ${JSON.stringify(String(metadata.ino))},
  objectId: ${JSON.stringify(objectId(paths.mountPath))},
});
const inspected = Object.freeze({ filesystem, identity });
const aclCompletion = Object.freeze({
  stderr: Buffer.alloc(0),
  stdout: Buffer.from("user::rwx\\ngroup::---\\nother::---\\n"),
});
const loopReceipt = Object.freeze({
  backingDevice: ${JSON.stringify(String(imageMetadata.dev))},
  backingInode: ${JSON.stringify(String(imageMetadata.ino))},
  blockSize: "512",
  loopDevice: ${JSON.stringify(LOOP_DEVICE)},
  loopRdev: "7:7",
  offset: "0",
  readOnly: false,
  sizeBytes: ${JSON.stringify(String(imageMetadata.size))},
  sizeLimit: "0",
  status: "present",
});
const mountBytes = Buffer.from(${JSON.stringify(mountInfo)});
const inspectedPending = Promise.resolve(inspected);
const aclPending = Promise.resolve(aclCompletion);
const loopPending = Promise.resolve(loopReceipt);
const mountPending = Promise.resolve(mountBytes);
const options = {
  commandRunner(executable) {
    if (executable === "/usr/bin/getfacl") return aclPending;
    throw new Error("unexpected executable");
  },
  inspector: Object.freeze({
    inspectFilesystemObject() { return inspectedPending; },
    runFdOperation(request) {
      if (request.operation !== "inspect-loop") {
        throw new Error("unexpected fd operation");
      }
      return loopPending;
    },
  }),
  platform: "linux",
  readMountInfo() { return mountPending; },
};

const defineProperty = Object.defineProperty;
const getOwnPropertyDescriptor = Object.getOwnPropertyDescriptor;
function replaceIntrinsic(target, key, replacement) {
  const descriptor = getOwnPropertyDescriptor(target, key);
  defineProperty(target, key, { ...descriptor, value: replacement });
  return () => defineProperty(target, key, descriptor);
}
const targets = [
  [Array, "isArray"],
  [Array.prototype, "every"],
  [Array.prototype, "includes"],
  [Array.prototype, "push"],
  [Array.prototype, "slice"],
  [BigInt.prototype, "toString"],
  [Buffer, "byteLength"],
  [Buffer, "from"],
  [Buffer, "isBuffer"],
  [Hash.prototype, "digest"],
  [Hash.prototype, "update"],
  [Number, "isSafeInteger"],
  [Object, "create"],
  [Object, "defineProperties"],
  [Object, "freeze"],
  [Object, "getOwnPropertyDescriptor"],
  [Object, "getOwnPropertyDescriptors"],
  [Object, "getPrototypeOf"],
  [Object, "hasOwn"],
  [Reflect, "apply"],
  [Reflect, "ownKeys"],
  [RegExp.prototype, "exec"],
  [Set.prototype, "add"],
  [Set.prototype, "has"],
  [String, "fromCharCode"],
  [String.prototype, "charCodeAt"],
  [String.prototype, "endsWith"],
  [String.prototype, "includes"],
  [String.prototype, "slice"],
  [String.prototype, "split"],
  [String.prototype, "startsWith"],
  [TextDecoder.prototype, "decode"],
  [WeakSet.prototype, "add"],
  [WeakSet.prototype, "has"],
];
const restorations = [];
const poison = () => { throw new Error("poisoned intrinsic must not run"); };
let result;
try {
  for (let index = 0; index < targets.length; index += 1) {
    restorations[index] = replaceIntrinsic(targets[index][0], targets[index][1], poison);
  }
  const driver = createLinuxExt4ImageDriver(options);
  result = await driver.observeMount({
    imagePath: ${JSON.stringify(paths.imagePath)},
    mountPath: ${JSON.stringify(paths.mountPath)},
  });
} finally {
  for (let index = restorations.length - 1; index >= 0; index -= 1) {
    restorations[index]();
  }
}
if (
  result.imageIdentity.filesystemId !== ${JSON.stringify(FILESYSTEM_ID)} ||
  result.rootIdentity.objectId !== ${JSON.stringify(objectId(paths.mountPath))}
) {
  throw new Error("unexpected hardened driver result");
}
process.stdout.write("ok\\n");
`;
  const { stderr, stdout } = await execFileAsync(
    process.execPath,
    ["--input-type=module", "--eval", script],
    {
      encoding: "buffer",
      env: { LANG: "C", LC_ALL: "C" },
      maxBuffer: 32 * 1024,
      shell: false,
      timeout: 5_000,
    },
  );
  assert.deepEqual(stdout, Buffer.from("ok\n"));
  assert.equal(stderr.length, 0);
});

test("constructor rejects non-absolute executables and accessor configuration", () => {
  const inspector = createInspector();
  assert.throws(
    () => createLinuxExt4ImageDriver({ inspector, platform: "linux", mountExecutable: "mount" }),
    driverError("invalid_options"),
  );
  let getterCalls = 0;
  const options = { inspector, platform: "linux" };
  Object.defineProperty(options, "mountExecutable", {
    enumerable: true,
    get() {
      getterCalls += 1;
      throw new Error("configuration getter must not run");
    },
  });
  assert.throws(
    () => createLinuxExt4ImageDriver(options),
    driverError("invalid_options"),
  );
  assert.equal(getterCalls, 0);

  let inspectorGetterCalls = 0;
  const hostileInspector = {
    async runFdOperation() {
      throw new Error("must not run");
    },
  };
  Object.defineProperty(hostileInspector, "inspectFilesystemObject", {
    enumerable: true,
    get() {
      inspectorGetterCalls += 1;
      throw new Error("inspector getter must not run");
    },
  });
  assert.throws(
    () =>
      createLinuxExt4ImageDriver({
        inspector: hostileInspector,
        platform: "linux",
      }),
    driverError("invalid_options"),
  );
  assert.equal(inspectorGetterCalls, 0);
});
