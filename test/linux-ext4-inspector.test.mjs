import assert from "node:assert/strict";
import { Buffer } from "node:buffer";
import { execFile } from "node:child_process";
import { mkdtemp, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";
import { promisify } from "node:util";

import {
  LinuxExt4Inspector,
  LinuxExt4InspectorError,
  createLinuxExt4Inspector,
} from "../src/linux-ext4-inspector.mjs";

const HELPER_PATH = "/opt/portable-codex/libexec/linux-ext4-inspector";
const TRUSTED_ROOT = "/srv/portable-codex";
const FILESYSTEM_UUID = "01234567-89ab-cdef-0123-456789abcdef";
const OBJECT_ID = `ext4fh1:${"a".repeat(64)}`;
const execFileAsync = promisify(execFile);

function helperLine(overrides = {}) {
  return `${JSON.stringify({
    filesystemUuid: FILESYSTEM_UUID,
    device: "2049",
    inode: "42",
    objectId: OBJECT_ID,
    ...overrides,
  })}\n`;
}

function loopLine(loopDevice, status = "present") {
  return `${JSON.stringify({
    backingDevice: "2049",
    backingInode: "42",
    blockSize: "512",
    loopDevice,
    loopRdev: "7:7",
    offset: "0",
    readOnly: false,
    sizeBytes: "1048576",
    sizeLimit: "0",
    status,
  })}\n`;
}

function privatePathLine(overrides = {}) {
  return `${JSON.stringify({
    device: "2049",
    empty: true,
    inode: "42",
    private: true,
    status: "ok",
    ...overrides,
  })}\n`;
}

function completion({
  exitCode,
  signal,
  stderr = Buffer.alloc(0),
  stdout = Buffer.from(helperLine(), "utf8"),
} = {}) {
  return {
    ...(exitCode === undefined ? {} : { exitCode }),
    ...(signal === undefined ? {} : { signal }),
    stderr,
    stdout,
  };
}

function createFixture({
  calls = [],
  readMountInfo = async () =>
    Buffer.from("29 23 0:25 / / rw,relatime - ext4 /dev/root rw\n", "utf8"),
  runHelper = async (...args) => {
    calls.push(args);
    return completion();
  },
  trustedRoots = [TRUSTED_ROOT],
} = {}) {
  return {
    calls,
    inspector: createLinuxExt4Inspector({
      helperPath: HELPER_PATH,
      platform: "linux",
      readMountInfo,
      runHelper,
      trustedRoots,
    }),
  };
}

function inspectorError(code) {
  return (error) => {
    assert(error instanceof LinuxExt4InspectorError);
    assert.equal(error.code, code);
    assert.equal(error.retryable, false);
    assert.equal(Object.isFrozen(error), true);
    return true;
  };
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

test("inspector returns exact frozen filesystem and object identity records", async () => {
  const fixture = createFixture();
  const filesystemPending = fixture.inspector.inspectFilesystem(
    `${TRUSTED_ROOT}/sessions/one`,
  );
  assert.strictEqual(Object.getPrototypeOf(filesystemPending), Promise.prototype);
  const filesystem = await filesystemPending;
  assert.deepEqual(filesystem, {
    durability: "local-fsync-rename",
    filesystemId: `ext4fs:${FILESYSTEM_UUID}`,
    objectIdentityScheme: "linux-ext4-file-handle-sha256-v1",
    type: "ext4",
  });
  assert.equal(Object.isFrozen(filesystem), true);

  const identityPending = fixture.inspector.inspectPersistentObjectIdentity(
    `${TRUSTED_ROOT}/sessions/one`,
  );
  assert.strictEqual(Object.getPrototypeOf(identityPending), Promise.prototype);
  const identity = await identityPending;
  assert.deepEqual(identity, {
    device: "2049",
    inode: "42",
    objectId: OBJECT_ID,
  });
  assert.equal(Object.isFrozen(identity), true);
  assert.equal(fixture.calls.length, 2);

  for (const [helperPath, args, options] of fixture.calls) {
    assert.equal(helperPath, HELPER_PATH);
    assert.deepEqual(args, [
      "inspect",
      "--root",
      TRUSTED_ROOT,
      "--relative",
      "sessions/one",
    ]);
    assert.equal(Object.isFrozen(args), true);
    assert.equal(options.shell, false);
    assert.equal(options.encoding, "buffer");
    assert.equal(options.killSignal, "SIGKILL");
    assert.equal(options.maxBuffer, 4096);
    assert.equal(options.timeout, 60_000);
    assert.deepEqual(options.env, { LANG: "C", LC_ALL: "C" });
    assert.equal(Object.hasOwn(options.env, "PATH"), false);
    assert.equal(Object.isFrozen(options), true);
    assert.equal(Object.isFrozen(options.env), true);
  }
});

test("combined inspection returns filesystem and identity from one helper call", async () => {
  const fixture = createFixture();
  const inspected = await fixture.inspector.inspectFilesystemObject(
    `${TRUSTED_ROOT}/sessions/one`,
  );
  assert.deepEqual(inspected, {
    filesystem: {
      durability: "local-fsync-rename",
      filesystemId: `ext4fs:${FILESYSTEM_UUID}`,
      objectIdentityScheme: "linux-ext4-file-handle-sha256-v1",
      type: "ext4",
    },
    identity: {
      device: "2049",
      inode: "42",
      objectId: OBJECT_ID,
    },
  });
  assert.equal(fixture.calls.length, 1);
  assert.equal(Object.isFrozen(inspected), true);
  assert.equal(Object.isFrozen(inspected.filesystem), true);
  assert.equal(Object.isFrozen(inspected.identity), true);
});

test("inspection pins the deepest exact mount root before applying NO_XDEV", async () => {
  const mountRoot = `${TRUSTED_ROOT}/mounted-volume`;
  const fixture = createFixture({
    readMountInfo: async () =>
      Buffer.from(
        `29 23 0:25 / / rw - ext4 /dev/root rw\n` +
          `36 29 7:7 / ${mountRoot} rw - ext4 /dev/loop7 rw\n`,
      ),
  });
  await fixture.inspector.inspectFilesystemObject(`${mountRoot}/session`);
  assert.deepEqual(fixture.calls[0][1], [
    "inspect",
    "--root",
    mountRoot,
    "--relative",
    "session",
  ]);
  await fixture.inspector.inspectFilesystemObject(mountRoot);
  assert.deepEqual(fixture.calls[1][1], [
    "inspect",
    "--root",
    mountRoot,
    "--relative",
    ".",
  ]);
});

test("closed control-root API returns persistent identity from one operation receipt", async () => {
  const calls = [];
  const fixture = createFixture({
    calls,
    runHelper: async (...args) => {
      calls.push(args);
      const command = args[1][0];
      if (command === "inspect") return completion();
      return completion({
        stdout: Buffer.from(
          `${JSON.stringify({
            controlFileName: ".operation-journal.lock",
            created: true,
            device: "2049",
            filesystemUuid: FILESYSTEM_UUID,
            inode: "43",
            kind: "journal",
            objectId: `ext4fh1:${"b".repeat(64)}`,
            status: "ok",
          })}\n`,
        ),
      });
    },
  });
  const rootPath = `${TRUSTED_ROOT}/archive`;
  const result = await fixture.inspector.provisionControlRoot({
    kind: "journal",
    rootPath,
  });
  assert.deepEqual(Reflect.ownKeys(result), [
    "controlFileIdentity",
    "controlFileName",
    "created",
    "filesystem",
    "kind",
    "rootIdentity",
    "rootPath",
    "status",
  ]);
  assert.deepEqual({ ...result.controlFileIdentity }, {
    device: "2049",
    filesystemId: `ext4fs:${FILESYSTEM_UUID}`,
    inode: "43",
    objectId: `ext4fh1:${"b".repeat(64)}`,
    objectIdentityScheme: "linux-ext4-file-handle-sha256-v1",
  });
  assert.equal(Object.isFrozen(result.controlFileIdentity), true);
  assert.equal(calls.length, 3);
  assert.deepEqual(calls[1][1], [
    "operate",
    "--root",
    TRUSTED_ROOT,
    "--relative",
    "archive",
    "--verb",
    "provision-control-root",
    "--kind",
    "journal",
    "--filesystem-id",
    `ext4fs:${FILESYSTEM_UUID}`,
    "--object-id",
    OBJECT_ID,
    "--expected-control-filesystem-id",
    "-",
    "--expected-control-object-id",
    "-",
    "--device",
    "2049",
    "--inode",
    "42",
  ]);

  const expectedControlObjectId = `ext4fh1:${"b".repeat(64)}`;
  const verified = await fixture.inspector.runFdOperation({
    device: "2049",
    expectedControlFilesystemId: `ext4fs:${FILESYSTEM_UUID}`,
    expectedControlObjectId,
    filesystemId: `ext4fs:${FILESYSTEM_UUID}`,
    inode: "42",
    kind: "journal",
    objectId: OBJECT_ID,
    operation: "provision-control-root",
    rootPath,
  });
  assert.equal(verified.created, true);
  assert.equal(verified.controlFileIdentity.objectId, expectedControlObjectId);
  assert.deepEqual(calls[3][1].slice(13, 21), [
    "--expected-control-filesystem-id",
    `ext4fs:${FILESYSTEM_UUID}`,
    "--expected-control-object-id",
    expectedControlObjectId,
    "--device",
    "2049",
    "--inode",
    "42",
  ]);

  const callsBeforeInvalid = calls.length;
  await assert.rejects(
    fixture.inspector.provisionControlRoot({
      kind: "unexpected",
      rootPath,
    }),
    inspectorError("invalid_path"),
  );
  assert.equal(calls.length, callsBeforeInvalid);
});

test("trusted-root paths dispatch as a dot-relative helper lookup", async () => {
  const fixture = createFixture();
  await fixture.inspector.inspectFilesystem(TRUSTED_ROOT);
  assert.deepEqual(fixture.calls[0][1], [
    "inspect",
    "--root",
    TRUSTED_ROOT,
    "--relative",
    ".",
  ]);
});

test("private-path inspection uses the exact read-only ABI for root and nested paths", async () => {
  const calls = [];
  const outputs = [
    Buffer.from(privatePathLine({ empty: false }), "utf8"),
    Buffer.from(privatePathLine({ empty: null, private: false }), "utf8"),
  ];
  const fixture = createFixture({
    calls,
    runHelper: async (...args) => {
      calls.push(args);
      return completion({ stdout: outputs[calls.length - 1] });
    },
  });

  const rootReceipt = await fixture.inspector.runFdOperation({
    device: "2049",
    inode: "42",
    kind: "directory",
    linkPolicy: "positive",
    mode: "0700",
    operation: "inspect-private-path",
    path: TRUSTED_ROOT,
    requireEmpty: true,
    uid: "501",
  });
  assert.deepEqual(rootReceipt, {
    device: "2049",
    empty: false,
    inode: "42",
    private: true,
    status: "ok",
  });
  assert.deepEqual(Reflect.ownKeys(rootReceipt), [
    "device",
    "empty",
    "inode",
    "private",
    "status",
  ]);
  assert.equal(Object.isFrozen(rootReceipt), true);

  const nestedReceipt = await fixture.inspector.runFdOperation({
    device: "2049",
    inode: "42",
    kind: "file",
    linkPolicy: "single",
    mode: "0600",
    operation: "inspect-private-path",
    path: `${TRUSTED_ROOT}/sessions/one/image.ext4`,
    requireEmpty: false,
    uid: "501",
  });
  assert.deepEqual(nestedReceipt, {
    device: "2049",
    empty: null,
    inode: "42",
    private: false,
    status: "ok",
  });
  assert.equal(Object.isFrozen(nestedReceipt), true);

  assert.equal(calls.length, 2);
  assert.deepEqual(calls[0][1], [
    "operate",
    "--root",
    TRUSTED_ROOT,
    "--relative",
    ".",
    "--verb",
    "inspect-private-path",
    "--kind",
    "directory",
    "--uid",
    "501",
    "--mode",
    "0700",
    "--link-policy",
    "positive",
    "--require-empty",
    "yes",
    "--device",
    "2049",
    "--inode",
    "42",
  ]);
  assert.deepEqual(calls[1][1], [
    "operate",
    "--root",
    TRUSTED_ROOT,
    "--relative",
    "sessions/one/image.ext4",
    "--verb",
    "inspect-private-path",
    "--kind",
    "file",
    "--uid",
    "501",
    "--mode",
    "0600",
    "--link-policy",
    "single",
    "--require-empty",
    "no",
    "--device",
    "2049",
    "--inode",
    "42",
  ]);
  for (const [helperPath, args, options] of calls) {
    assert.equal(helperPath, HELPER_PATH);
    assert.equal(Object.isFrozen(args), true);
    assert.deepEqual(options, {
      encoding: "buffer",
      env: { LANG: "C", LC_ALL: "C" },
      killSignal: "SIGKILL",
      maxBuffer: 4096,
      shell: false,
      timeout: 60_000,
      windowsHide: true,
    });
    assert.equal(Object.isFrozen(options), true);
    assert.equal(Object.isFrozen(options.env), true);
  }
});

test("private-path inspection rejects non-canonical or semantically mismatched receipts", async () => {
  const request = {
    device: "2049",
    inode: "42",
    kind: "directory",
    linkPolicy: "positive",
    mode: "0700",
    operation: "inspect-private-path",
    path: `${TRUSTED_ROOT}/candidate`,
    requireEmpty: true,
    uid: "501",
  };
  const malformed = [
    Buffer.from("{}\n", "utf8"),
    Buffer.from(privatePathLine({ extra: true }), "utf8"),
    Buffer.from(privatePathLine({ device: 2049 }), "utf8"),
    Buffer.from(privatePathLine({ empty: null }), "utf8"),
    Buffer.from(privatePathLine({ empty: "true" }), "utf8"),
    Buffer.from(privatePathLine({ inode: "0" }), "utf8"),
    Buffer.from(privatePathLine({ private: null }), "utf8"),
    Buffer.from(privatePathLine({ status: "private" }), "utf8"),
    Buffer.from(
      `${JSON.stringify({
        empty: true,
        device: "2049",
        inode: "42",
        private: true,
        status: "ok",
      })}\n`,
      "utf8",
    ),
    Buffer.from(
      `${JSON.stringify(
        {
          device: "2049",
          empty: true,
          inode: "42",
          private: true,
          status: "ok",
        },
        null,
        2,
      )}\n`,
      "utf8",
    ),
  ];
  for (const stdout of malformed) {
    const fixture = createFixture({
      runHelper: async () => completion({ stdout }),
    });
    await assert.rejects(
      fixture.inspector.runFdOperation(request),
      inspectorError("helper_output_invalid"),
    );
  }

  const unexpectedBoolean = createFixture({
    runHelper: async () =>
      completion({ stdout: Buffer.from(privatePathLine(), "utf8") }),
  });
  await assert.rejects(
    unexpectedBoolean.inspector.runFdOperation({
      ...request,
      requireEmpty: false,
    }),
    inspectorError("helper_output_invalid"),
  );
});

test("private-path read failures retain inspection semantics", async () => {
  const request = {
    device: "2049",
    inode: "42",
    kind: "directory",
    linkPolicy: "positive",
    mode: "0700",
    operation: "inspect-private-path",
    path: `${TRUSTED_ROOT}/candidate`,
    requireEmpty: true,
    uid: "501",
  };
  const cases = [
    {
      code: "path_mismatch",
      runHelper: async () =>
        completion({ exitCode: 65, stdout: Buffer.alloc(0) }),
    },
    {
      code: "helper_failed",
      runHelper: async () =>
        completion({ exitCode: 74, stdout: Buffer.alloc(0) }),
    },
    {
      code: "helper_failed",
      runHelper: async () =>
        completion({ signal: "SIGKILL", stdout: Buffer.alloc(0) }),
    },
    {
      code: "helper_failed",
      runHelper: async () => {
        const error = new Error("helper timed out");
        error.code = "ETIMEDOUT";
        error.signal = "SIGKILL";
        error.stderr = Buffer.alloc(0);
        error.stdout = Buffer.alloc(0);
        throw error;
      },
    },
  ];
  for (const { code, runHelper } of cases) {
    const fixture = createFixture({ runHelper });
    await assert.rejects(
      fixture.inspector.runFdOperation(request),
      inspectorError(code),
    );
  }
});

test("native private-path inspection binds identity, policy, and emptiness to fixed FDs", async () => {
  const source = await readFile(
    new URL("../native/linux-ext4-inspector.c", import.meta.url),
    "utf8",
  );
  const handleOpenStart = source.indexOf(
    "static int open_relative_path_handle(",
  );
  const handleOpenEnd = source.indexOf(
    "static int open_direct_child(",
    handleOpenStart,
  );
  const aclStart = source.indexOf(
    "static int extended_acl_state(",
    handleOpenEnd,
  );
  const metadataStart = source.indexOf(
    "static int private_metadata_matches(",
    aclStart,
  );
  const policyStart = source.indexOf(
    "static int private_policy_status(",
    metadataStart,
  );
  const policyEnd = source.indexOf(
    "static int unlinked_private_policy_status(",
    policyStart,
  );
  const unlinkedPolicyEnd = source.indexOf(
    "static int require_private_policy(",
    policyEnd,
  );
  const emptyStart = source.indexOf("static int directory_empty_state(");
  const emptyEnd = source.indexOf(
    "static int format_proc_fd_path(",
    emptyStart,
  );
  const aclOpenStart = source.indexOf(
    "static int open_acl_fd_from_path_handle(",
    emptyEnd,
  );
  const aclOpenEnd = source.indexOf(
    "static int format_proc_fd_child_path(",
    aclOpenStart,
  );
  const inspectStart = source.indexOf("static int inspect_private_path(");
  const inspectEnd = source.indexOf("static void format_uuid(", inspectStart);
  assert(handleOpenStart >= 0);
  assert(handleOpenEnd > handleOpenStart);
  assert(aclStart > handleOpenEnd);
  assert(metadataStart > aclStart);
  assert(policyStart > metadataStart);
  assert(policyEnd > policyStart);
  assert(unlinkedPolicyEnd > policyEnd);
  assert(emptyStart > unlinkedPolicyEnd);
  assert(emptyEnd > emptyStart);
  assert(aclOpenStart > emptyEnd);
  assert(aclOpenEnd > aclOpenStart);
  assert(inspectStart > aclOpenEnd);
  assert(inspectEnd > inspectStart);

  const handleOpenBody = source.slice(handleOpenStart, handleOpenEnd);
  assert.match(
    handleOpenBody,
    /how\.flags = O_PATH \| O_NOFOLLOW \| O_CLOEXEC/u,
  );
  assert.doesNotMatch(
    handleOpenBody,
    /\bO_RDONLY\b|\bO_NONBLOCK\b|\bO_DIRECTORY\b/u,
  );
  assert.match(
    handleOpenBody,
    /RESOLVE_BENEATH \| RESOLVE_NO_MAGICLINKS \|\s+RESOLVE_NO_SYMLINKS \| RESOLVE_NO_XDEV/u,
  );
  assert.match(
    handleOpenBody,
    /return call_openat2\(root_fd, relative_path, &how\)/u,
  );

  const aclBody = source.slice(aclStart, metadataStart);
  assert.match(aclBody, /"system\.posix_acl_access"/u);
  assert.match(aclBody, /"system\.posix_acl_default"/u);
  assert.match(aclBody, /fgetxattr\(fd, names\[index\], NULL, 0U\)/u);
  assert.doesNotMatch(aclBody, /\bgetxattr\(/u);

  const metadataBody = source.slice(metadataStart, policyStart);
  assert.match(metadataBody, /\(metadata->st_mode & S_IFMT\) == type/u);
  assert.match(metadataBody, /metadata->st_uid == getuid\(\)/u);
  assert.match(metadataBody, /\(metadata->st_mode & 07777U\) == mode/u);
  assert.doesNotMatch(metadataBody, /\(metadata->st_mode & 0777U\)/u);
  assert.match(metadataBody, /metadata->st_nlink >= 1/u);
  assert.match(metadataBody, /!single_link \|\| metadata->st_nlink == 1/u);
  assert.doesNotMatch(metadataBody, /metadata->st_(?:ctim|mtim|size)/u);

  const policyBody = source.slice(policyStart, policyEnd);
  assert.match(policyBody, /fstat\(fd, &metadata\)/u);
  assert.match(
    policyBody,
    /private_metadata_matches\(&metadata, type, mode, single_link\)/u,
  );
  assert.match(policyBody, /acl_state = extended_acl_state\(fd\)/u);

  const unlinkedPolicyBody = source.slice(policyEnd, unlinkedPolicyEnd);
  assert.match(
    unlinkedPolicyBody,
    /\(metadata\.st_mode & 07777U\) != mode/u,
  );
  assert.doesNotMatch(
    unlinkedPolicyBody,
    /\(metadata\.st_mode & 0777U\)/u,
  );

  const emptyBody = source.slice(emptyStart, emptyEnd);
  assert.match(
    emptyBody,
    /scan_fd = openat\(directory_fd, "\.",\s+O_RDONLY \| O_DIRECTORY \| O_NOFOLLOW \| O_CLOEXEC\)/u,
  );
  assert.match(emptyBody, /directory = fdopendir\(scan_fd\)/u);
  assert.doesNotMatch(emptyBody, /dup\(/u);
  assert.match(
    emptyBody,
    /strcmp\(entry->d_name, "\."\) != 0 && strcmp\(entry->d_name, "\.\."\) != 0/u,
  );

  const aclOpenBody = source.slice(aclOpenStart, aclOpenEnd);
  const handleStat = aclOpenBody.indexOf(
    "fstat(path_fd, &handle_metadata)",
  );
  const handleType = aclOpenBody.indexOf(
    "(handle_metadata.st_mode & S_IFMT) != expected_type",
    handleStat,
  );
  const procPath = aclOpenBody.indexOf(
    "format_proc_fd_path(path_fd, proc_path)",
    handleType,
  );
  const pinnedOpen = aclOpenBody.indexOf(
    "opened_fd = open(proc_path, flags)",
    procPath,
  );
  const openedStat = aclOpenBody.indexOf(
    "fstat(opened_fd, &opened_metadata)",
    pinnedOpen,
  );
  const reopenedDevice = aclOpenBody.indexOf(
    "opened_metadata.st_dev != handle_metadata.st_dev",
    openedStat,
  );
  const reopenedInode = aclOpenBody.indexOf(
    "opened_metadata.st_ino != handle_metadata.st_ino",
    reopenedDevice,
  );
  const reopenedType = aclOpenBody.indexOf(
    "(opened_metadata.st_mode & S_IFMT) != expected_type",
    reopenedInode,
  );
  assert(handleStat >= 0);
  assert(handleType > handleStat);
  assert(procPath > handleType);
  assert(pinnedOpen > procPath);
  assert(openedStat > pinnedOpen);
  assert(reopenedDevice > openedStat);
  assert(reopenedInode > reopenedDevice);
  assert(reopenedType > reopenedInode);
  assert.match(
    aclOpenBody,
    /int flags = O_RDONLY \| O_NONBLOCK \| O_CLOEXEC/u,
  );
  assert.match(
    aclOpenBody,
    /if \(expected_type == S_IFDIR\) flags \|= O_DIRECTORY/u,
  );
  assert.doesNotMatch(aclOpenBody, /relative_path|root_fd/u);
  assert.match(
    aclOpenBody,
    /substituted FIFO, device, socket, or symlink cannot trigger its open\s+\* hook/u,
  );

  const inspectBody = source.slice(inspectStart, inspectEnd);
  const targetHandleOpen = inspectBody.indexOf(
    "target_handle_fd = open_relative_path_handle(root_fd, relative_path)",
  );
  const firstTargetIdentity = inspectBody.indexOf(
    "require_fd_identity(target_handle_fd, device, inode, 0)",
    targetHandleOpen,
  );
  const targetMetadata = inspectBody.indexOf(
    "fstat(target_handle_fd, &target_metadata)",
    firstTargetIdentity,
  );
  const targetTypeAndPolicy = inspectBody.indexOf(
    "private_metadata_matches(&target_metadata, expected_type,",
    targetMetadata,
  );
  const targetAclOpen = inspectBody.indexOf(
    "target_fd = open_acl_fd_from_path_handle(target_handle_fd, expected_type)",
    targetTypeAndPolicy,
  );
  const firstTargetPolicy = inspectBody.indexOf(
    "private_policy_status(target_fd, expected_type,",
    targetAclOpen,
  );
  const firstTargetEmpty = inspectBody.indexOf(
    "directory_empty_state(target_fd)",
    firstTargetPolicy,
  );
  const currentHandleOpen = inspectBody.indexOf(
    "current_handle_fd = open_relative_path_handle(root_fd, relative_path)",
    firstTargetEmpty,
  );
  const currentIdentity = inspectBody.indexOf(
    "require_fd_identity(current_handle_fd, device, inode, 0)",
    currentHandleOpen,
  );
  const currentMetadata = inspectBody.indexOf(
    "fstat(current_handle_fd, &current_metadata)",
    currentIdentity,
  );
  const currentTypeAndPolicy = inspectBody.indexOf(
    "private_metadata_matches(&current_metadata, expected_type,",
    currentMetadata,
  );
  const currentAclOpen = inspectBody.indexOf(
    "current_fd = open_acl_fd_from_path_handle(current_handle_fd,",
    currentTypeAndPolicy,
  );
  const currentPolicy = inspectBody.indexOf(
    "private_policy_status(current_fd, expected_type,",
    currentAclOpen,
  );
  const currentEmpty = inspectBody.indexOf(
    "directory_empty_state(current_fd)",
    currentPolicy,
  );
  const finalTargetPolicy = inspectBody.indexOf(
    "private_policy_status(target_fd, expected_type,",
    firstTargetPolicy + 1,
  );
  const finalTargetEmpty = inspectBody.indexOf(
    "directory_empty_state(target_fd)",
    firstTargetEmpty + 1,
  );
  const finalTargetIdentity = inspectBody.indexOf(
    "require_fd_identity(target_handle_fd, device, inode, 0)",
    firstTargetIdentity + 1,
  );
  const receiptStat = inspectBody.indexOf(
    "fstat(target_handle_fd, &target_metadata)",
    finalTargetIdentity,
  );
  assert(targetHandleOpen >= 0);
  assert(firstTargetIdentity > targetHandleOpen);
  assert(targetMetadata > firstTargetIdentity);
  assert(targetTypeAndPolicy > targetMetadata);
  assert(targetAclOpen > targetTypeAndPolicy);
  assert(firstTargetPolicy > targetAclOpen);
  assert(firstTargetEmpty > firstTargetPolicy);
  assert(currentHandleOpen > firstTargetEmpty);
  assert(currentIdentity > currentHandleOpen);
  assert(currentMetadata > currentIdentity);
  assert(currentTypeAndPolicy > currentMetadata);
  assert(currentAclOpen > currentTypeAndPolicy);
  assert(currentPolicy > currentAclOpen);
  assert(currentEmpty > currentPolicy);
  assert(finalTargetPolicy > currentEmpty);
  assert(finalTargetEmpty > finalTargetPolicy);
  assert(finalTargetIdentity > finalTargetEmpty);
  assert(receiptStat > finalTargetIdentity);
  assert.match(
    inspectBody,
    /if \(!private_metadata_matches\(&target_metadata, expected_type,[\s\S]*?\} else \{\s+target_fd = open_acl_fd_from_path_handle/u,
  );
  assert.match(
    inspectBody,
    /if \(!private_metadata_matches\(&current_metadata, expected_type,[\s\S]*?\} else \{\s+current_fd = open_acl_fd_from_path_handle/u,
  );
  assert.doesNotMatch(
    inspectBody,
    /open_relative_(?:private_path|directory)\(root_fd, relative_path/u,
  );
  assert.equal(
    (inspectBody.match(/open_relative_path_handle\(/gu) ?? []).length,
    2,
  );
  assert.equal(
    (inspectBody.match(/open_acl_fd_from_path_handle\(/gu) ?? []).length,
    2,
  );
  assert.equal(
    (inspectBody.match(/private_policy_status\(/gu) ?? []).length,
    3,
  );
  assert.equal(
    (inspectBody.match(/directory_empty_state\(/gu) ?? []).length,
    3,
  );
  assert.equal(
    (inspectBody.match(/if \(empty_state == 0\) empty = 0;/gu) ?? []).length,
    3,
  );
  assert.equal(
    (inspectBody.match(/if \(policy_state == 0\) private = 0;/gu) ?? [])
      .length,
    3,
  );
  assert.equal((inspectBody.match(/require_fd_identity\(/gu) ?? []).length, 3);
  assert.match(
    inspectBody,
    /Directory timestamps,\s+\* size, ctime, and exact link count are intentionally not compared/u,
  );
  assert.doesNotMatch(
    inspectBody,
    /metadata\.st_(?:ctim|mtim|size|nlink)/u,
  );
  assert.match(
    inspectBody.slice(receiptStat),
    /\(uintmax_t\)target_metadata\.st_dev[\s\S]*\(uintmax_t\)target_metadata\.st_ino/u,
  );

  // Deterministic source and ABI guards are used here because the production
  // helper has no synchronization hook for a non-flaky ABA or child-churn race.
});

test("native helper source binds mutation authority, loop geometry, and settle checks", async () => {
  const source = await readFile(
    new URL("../native/linux-ext4-inspector.c", import.meta.url),
    "utf8",
  );
  const ext4Ioctl = source.indexOf(
    "ioctl(root_fd, EXT4_IOC_GETFSUUID, ext4_uuid)",
  );
  const genericIoctl = source.indexOf(
    "ioctl(root_fd, FS_IOC_GETFSUUID, &generic_uuid)",
  );
  assert(ext4Ioctl >= 0);
  assert(genericIoctl > ext4Ioctl);
  assert.match(source, /__has_include\(<linux\/ext4\.h>\)/u);
  assert.match(
    source,
    /calloc\(1U, sizeof\(\*ext4_uuid\) \+ FILESYSTEM_UUID_BYTES\)/u,
  );
  assert.match(source, /ext4_uuid->fsu_len = FILESYSTEM_UUID_BYTES/u);
  assert.match(source, /ext4_uuid->fsu_flags = 0U/u);
  assert.match(
    source,
    /RESOLVE_BENEATH \| RESOLVE_NO_MAGICLINKS \|\s+RESOLVE_NO_SYMLINKS \| RESOLVE_NO_XDEV/u,
  );
  assert.match(
    source,
    /target_metadata\.st_dev != root_metadata\.st_dev/u,
  );
  assert.match(source, /read_persistent_identity_from_fd\(control_fd,/u);
  assert.match(
    source,
    /open_direct_child\(directory_fd, name, O_RDONLY, 0\)/u,
  );
  assert.match(source, /expects_control_identity/u);
  assert.match(source, /visible_metadata\.st_ino != metadata\.st_ino/u);
  assert.match(
    source,
    /require_persistent_identity\(directory_fd, filesystem_id,/u,
  );
  assert.match(
    source,
    /require_persistent_identity\(target_fd, target_filesystem_id,/u,
  );
  assert.match(source, /LOOP_CONFIGURE/u);
  assert.match(source, /configuration\.info\.lo_offset = 0U/u);
  assert.match(source, /configuration\.info\.lo_sizelimit = 0U/u);
  assert.match(source, /configuration\.info\.lo_flags = 0U/u);
  assert.match(
    source,
    /number < 0 \|\| \(unsigned int\)number >= LOOP_SCAN_LIMIT/u,
  );
  assert.match(source, /LOOP_CLR_FD/u);
  assert.match(source, /BLKGETDISKSEQ/u);
  assert.match(source, /\/sys\/dev\/block\/%u:%u\/loop\/backing_file/u);
  assert.match(source, /"\/proc\/self\/mountinfo"/u);
  assert.match(source, /STATX_MNT_ID/u);
  assert.match(source, /"shared:"/u);
  assert.match(source, /"master:"/u);
  assert.match(source, /"propagate_from:"/u);
  assert.match(source, /separator_index == SIZE_MAX/u);
  assert.match(source, /defined\(STATX_MNT_ID\)/u);
  assert.match(source, /defined\(UMOUNT_NOFOLLOW\)/u);
  assert.match(source, /mount\(source_proc_path, target_proc_path, "ext4"/u);
  assert.match(source, /umount2\(target_proc_path, UMOUNT_NOFOLLOW\)/u);
  assert.match(source, /syncfs\(target_fd\)/u);
  assert.match(source, /mkdirat\(parent_fd, name, DIRECTORY_MODE\)/u);
  assert.match(source, /unlinkat\(parent_fd, name,/u);
  assert.match(source, /unlinked_private_policy_status\(/u);
  assert.match(source, /O_CREAT \| O_EXCL \| O_NOFOLLOW/u);
  assert.match(source, /"root_owner=%ju:%ju"/u);
  assert.match(source, /\(uintmax_t\)getuid\(\)/u);
  assert.match(source, /\(uintmax_t\)getgid\(\)/u);
  assert.match(
    source,
    /\(char \*\)"-E", root_owner, \(char \*\)"--"/u,
  );
  assert.match(source, /execve\(executable, arguments, environment\)/u);
  assert.match(source, /FORMATTER_OPERATION_TIMEOUT_MS UINT64_C\(47000\)/u);
  assert.match(source, /FORMATTER_EXECUTION_TIMEOUT_MS UINT64_C\(40000\)/u);
  assert.match(source, /FORMATTER_TERM_GRACE_MS UINT64_C\(2000\)/u);
  assert.match(source, /FORMATTER_KILL_REAP_TIMEOUT_MS UINT64_C\(5000\)/u);
  assert.match(source, /FORMATTER_OUTER_RESERVE_MS UINT64_C\(10000\)/u);
  assert.match(
    source,
    /FORMATTER_OPERATION_TIMEOUT_MS \+ FORMATTER_OUTER_RESERVE_MS <\s+UINT64_C\(60000\)/u,
  );
  const inspectionAbiDefine = source.indexOf(
    "#define PORTABLE_CODEX_HAS_EXT4_INSPECTION_ABI 1",
  );
  const inspectionAbiGate = source.lastIndexOf(
    "#if defined(PORTABLE_CODEX_HAS_LINUX_HEADERS)",
    inspectionAbiDefine,
  );
  assert(inspectionAbiGate >= 0);
  assert(inspectionAbiDefine > inspectionAbiGate);
  assert.match(
    source.slice(inspectionAbiGate, inspectionAbiDefine),
    /defined\(PR_SET_CHILD_SUBREAPER\)/u,
  );
  assert.match(
    source,
    /deadline_after\(FORMATTER_OPERATION_TIMEOUT_MS, &hard_deadline\)/u,
  );
  assert.match(
    source,
    /force_formatter_group_cleanup\(leader, hard_deadline, wait_status\)/u,
  );
  const groupReapStart = source.indexOf(
    "static int reap_formatter_group_until(",
  );
  const finishGroupStart = source.indexOf(
    "static int finish_formatter_group(",
    groupReapStart,
  );
  const forceCleanupStart = source.indexOf(
    "static void force_formatter_group_cleanup(",
    finishGroupStart,
  );
  const supervisorStart = source.indexOf(
    "static int supervise_formatter(",
    forceCleanupStart,
  );
  const supervisionEnd = source.indexOf("\n}\n\n#endif", supervisorStart);
  assert(groupReapStart >= 0);
  assert(finishGroupStart > groupReapStart);
  assert(forceCleanupStart > finishGroupStart);
  assert(supervisorStart > forceCleanupStart);
  assert(supervisionEnd > supervisorStart);
  const forceCleanupBody = source.slice(forceCleanupStart, supervisorStart);
  const groupReapBody = source.slice(groupReapStart, finishGroupStart);
  const finishGroupBody = source.slice(finishGroupStart, forceCleanupStart);
  const supervisionBody = source.slice(groupReapStart, supervisionEnd);
  assert.match(
    groupReapBody,
    /caller must first observe this exact leader with waitid\(WNOWAIT\)/u,
  );
  assert.match(
    groupReapBody,
    /waitpid\(leader, &reaped_leader_status, WNOHANG\)/u,
  );
  assert.match(
    groupReapBody,
    /waitpid\(-leader, &discarded_descendant_status, WNOHANG\)/u,
  );
  assert.match(groupReapBody, /\*leader_wait_status = reaped_leader_status/u);
  assert.match(groupReapBody, /errno != ECHILD/u);
  assert.match(
    groupReapBody,
    /!no_group_children \|\| monotonic_milliseconds\(&now\) != 0 \|\|\s+now >= deadline/u,
  );
  const finalGroupSignal = finishGroupBody.indexOf(
    "signal_unreaped_formatter_group(leader, SIGKILL)",
  );
  const groupReap = finishGroupBody.indexOf(
    "reap_formatter_group_until(leader, deadline, wait_status)",
  );
  assert(finalGroupSignal >= 0);
  assert(groupReap > finalGroupSignal);
  assert.doesNotMatch(
    finishGroupBody.slice(groupReap),
    /signal_unreaped_formatter_group/u,
  );
  assert.match(
    forceCleanupBody,
    /formatter_child_terminal_until\(leader, hard_deadline\)/u,
  );
  assert.match(
    forceCleanupBody,
    /finish_formatter_group\(leader, hard_deadline, wait_status\)/u,
  );
  assert(
    forceCleanupBody.indexOf(
      "formatter_child_terminal_until(leader, hard_deadline)",
    ) <
      forceCleanupBody.indexOf(
        "finish_formatter_group(leader, hard_deadline, wait_status)",
      ),
  );
  assert.match(supervisionBody, /phase_deadline\(hard_deadline,/u);
  assert.doesNotMatch(supervisionBody, /deadline_after\(/u);
  assert.match(
    source,
    /PDEATHSIG covers only\s+\* this direct leader/u,
  );
  assert.match(source, /waitid\(P_PID, \(id_t\)child/u);
  assert.match(source, /WEXITED \| WNOHANG \| WNOWAIT/u);
  assert.match(
    source,
    /if \(information\.si_pid == child\) \{\s+if \(monotonic_milliseconds\(&now\) != 0\) return -1;\s+return now < deadline \? 1 : 0;/u,
  );
  assert.match(source, /kill\(-leader, signal_number\)/u);
  assert.match(source, /setpgid\(0, 0\)/u);
  assert.match(source, /setpgid\(child, child\)/u);
  assert.match(source, /prctl\(PR_SET_PDEATHSIG, SIGKILL\)/u);
  assert.match(source, /prctl\(PR_SET_CHILD_SUBREAPER, 1\)/u);
  assert.match(source, /getppid\(\) != expected_parent/u);
  const runFormatterStart = source.indexOf("static int run_mkfs_on_fd(");
  const runFormatterEnd = source.indexOf(
    "static int print_ok(",
    runFormatterStart,
  );
  assert(runFormatterStart >= 0);
  assert(runFormatterEnd > runFormatterStart);
  const runFormatterBody = source.slice(runFormatterStart, runFormatterEnd);
  const configureSubreaper = runFormatterBody.indexOf(
    "configure_formatter_subreaper()",
  );
  const formatterFork = runFormatterBody.indexOf("child = fork()");
  assert(configureSubreaper >= 0);
  assert(formatterFork > configureSubreaper);
  assert.match(source, /open\("\/dev\/null", O_RDWR \| O_NOFOLLOW \| O_CLOEXEC\)/u);
  assert.match(source, /redirect_formatter_standard_descriptors\(null_fd\)/u);
  assert.match(source, /dup3\(retained_fd, 3, 0\)/u);
  assert.match(source, /retained_fd <= STDERR_FILENO/u);
  assert.match(source, /standard_descriptors_present\(\)/u);
  assert.match(source, /getresuid\(&real_uid, &effective_uid, &saved_uid\)/u);
  assert.match(source, /getresgid\(&real_gid, &effective_gid, &saved_gid\)/u);
  assert.match(source, /real_uid == 0 \|\| real_uid != effective_uid/u);
  assert.match(
    source,
    /syscall\((?:SYS_close_range|__NR_close_range), 4U, UINT_MAX, CLOSE_RANGE_UNSHARE\)/u,
  );
  assert.match(source, /"\/proc\/self\/fd\/%d"/u);
  assert.doesNotMatch(source, /execlp?\(/u);
});

test("existing create-directory preserves read failures apart from proven mismatches", async () => {
  const source = await readFile(
    new URL("../native/linux-ext4-inspector.c", import.meta.url),
    "utf8",
  );
  const createStart = source.indexOf(
    "static int create_private_directory(",
  );
  const createEnd = source.indexOf(
    "static int format_ext4_image(",
    createStart,
  );
  assert(createStart >= 0);
  assert(createEnd > createStart);
  const createBody = source.slice(createStart, createEnd);
  assert.match(
    createBody,
    /if \(directory_fd < 0\) \{\s+status = errno == ENOTDIR \? INSPECTOR_EXIT_MISMATCH\s+: classify_path_errno\(errno\);/u,
  );
  assert.match(
    createBody,
    /status = require_private_policy\(directory_fd, S_IFDIR, DIRECTORY_MODE, 0,\s+created\);\s+if \(status != EXIT_SUCCESS\) goto cleanup;/u,
  );
  assert.match(
    createBody,
    /if \(fstat\(directory_fd, &metadata\) != 0\) \{\s+status = created \? INSPECTOR_EXIT_OUTCOME_UNCERTAIN\s+: classify_path_errno\(errno\);/u,
  );
  assert.match(
    createBody,
    /status = require_private_policy\(parent_fd, S_IFDIR, DIRECTORY_MODE, 0,\s+created\);\s+if \(status != EXIT_SUCCESS\)/u,
  );
  assert.doesNotMatch(
    createBody,
    /status = created \? INSPECTOR_EXIT_OUTCOME_UNCERTAIN\s+: INSPECTOR_EXIT_MISMATCH;/u,
  );
  assert.match(
    createBody,
    /if \(created && status != EXIT_SUCCESS\) return INSPECTOR_EXIT_OUTCOME_UNCERTAIN;/u,
  );
});

test("native formatter harness owns adopted-descendant evidence", async () => {
  const harness = await readFile(
    new URL(
      "./fixtures/linux-ext4-formatter-supervisor-harness.c",
      import.meta.url,
    ),
    "utf8",
  );
  const mainStart = harness.indexOf("int main(void)");
  const configureSubreaper = harness.indexOf(
    "configure_formatter_subreaper()",
    mainStart,
  );
  const firstFormatterFork = harness.indexOf("child = fork()", mainStart);
  const descendantCase = harness.indexOf(
    "formatter_test_descendant_group(image_fd)",
    mainStart,
  );
  assert(mainStart >= 0);
  assert(configureSubreaper > mainStart);
  assert(firstFormatterFork > configureSubreaper);
  assert(descendantCase > firstFormatterFork);
  assert.match(
    harness,
    /formatter_test_spawn_tree\(image_fd, &natural, 1, 0\)/u,
  );
  assert.match(
    harness,
    /supervision != FORMATTER_SUPERVISION_REAPED \|\|\s+!WIFEXITED\(wait_status\) \|\| WEXITSTATUS\(wait_status\) != 0/u,
  );
  assert.match(
    harness,
    /waitpid\(positive\.writer, &wait_status, WNOHANG\) != -1 \|\|\s+errno != ECHILD/u,
  );
  assert.match(harness, /kill\(-positive_group, 0\) != -1 \|\| errno != ESRCH/u);
  assert.match(
    harness,
    /formatter_test_wait_reap\(negative\.writer, UINT64_C\(1000\),\s+&wait_status\)/u,
  );
  assert.match(harness, /kill\(-negative_group, 0\) != -1 \|\| errno != ESRCH/u);
  assert.match(
    harness,
    /formatter_child_terminal_until\(child, expired_deadline\) != 0/u,
  );
  const treeGroup = harness.indexOf("tree->group = leader;");
  const treeValidation = harness.indexOf(
    "establish_formatter_process_group(leader)",
    treeGroup,
  );
  assert(treeGroup >= 0);
  assert(treeValidation > treeGroup);
  assert.match(
    harness,
    /formatter_test_spawn_tree\(image_fd, &tree, 0, 1\) == -2/u,
  );
  assert.match(
    harness,
    /waitpid\(-group, &discarded_status, WNOHANG\)/u,
  );
  assert.match(
    harness,
    /errno != ESRCH \|\| !no_group_children/u,
  );
  const cleanupStart = harness.indexOf(
    "static int formatter_test_cleanup_tree(",
  );
  const cleanupEnd = harness.indexOf(
    "static int formatter_test_unknown_writer_cleanup(",
    cleanupStart,
  );
  assert(cleanupStart >= 0);
  assert(cleanupEnd > cleanupStart);
  const cleanupBody = harness.slice(cleanupStart, cleanupEnd);
  assert.match(cleanupBody, /int process_cleanup_allowed = 1;/u);
  assert.match(
    cleanupBody,
    /if \(process_cleanup_allowed && !leader_reaped\)/u,
  );
  assert.match(
    cleanupBody,
    /if \(process_cleanup_allowed && leader_reaped && tree->group > 0\)/u,
  );
  const cleanupGroupSignal = cleanupBody.indexOf(
    "kill(-tree->group, SIGKILL)",
  );
  const cleanupLeaderReap = cleanupBody.indexOf(
    "formatter_test_wait_reap_until(tree->leader",
  );
  const cleanupGroupDrain = cleanupBody.indexOf(
    "formatter_test_drain_group_children_until(tree->group",
  );
  assert(cleanupGroupSignal >= 0);
  assert(cleanupLeaderReap > cleanupGroupSignal);
  assert(cleanupGroupDrain > cleanupLeaderReap);
  assert.doesNotMatch(
    cleanupBody.slice(cleanupLeaderReap),
    /kill\(-tree->group, SIG/u,
  );
  assert.doesNotMatch(
    cleanupBody,
    /if \(cleanup_valid && (?:!leader_reaped|leader_reaped)/u,
  );
});

test(
  "native formatter supervisor terminates the process group, reaps it, and stabilizes the image",
  { skip: process.platform !== "linux" },
  async () => {
    const directory = await mkdtemp(
      join(tmpdir(), "portable-codex-formatter-supervisor-"),
    );
    const executable = join(directory, "formatter-supervisor-test");
    const harness = fileURLToPath(
      new URL(
        "./fixtures/linux-ext4-formatter-supervisor-harness.c",
        import.meta.url,
      ),
    );
    try {
      await execFileAsync(
        "cc",
        [
          "-O2",
          "-std=c11",
          "-Wall",
          "-Wextra",
          "-Werror",
          "-DPORTABLE_CODEX_FORMATTER_SUPERVISOR_TEST",
          harness,
          "-o",
          executable,
        ],
        {
          encoding: "buffer",
          env: { LANG: "C", LC_ALL: "C", PATH: "/usr/bin:/bin" },
          maxBuffer: 16 * 1024,
          shell: false,
          timeout: 10_000,
        },
      );
      const { stderr, stdout } = await execFileAsync(executable, [], {
        encoding: "buffer",
        env: { LANG: "C", LC_ALL: "C" },
        maxBuffer: 16 * 1024,
        shell: false,
        timeout: 5_000,
      });
      assert.equal(stderr.length, 0);
      assert.deepEqual(JSON.parse(stdout.toString("utf8")), {
        adoptedDescendantReaped: true,
        descendantGroupStopped: true,
        execFailureReaped: true,
        expiredTerminalRejected: true,
        imageStable: true,
        leaderOnlyMutationDetected: true,
        naturalLeaderStatusPreserved: true,
        nonzeroReaped: true,
        pipeClosed: true,
        pgidAbsentAfterReap: true,
        reaped: true,
        parentDeathKill: true,
        termThenKill: true,
        unknownWriterCleanup: true,
      });
    } finally {
      await rm(directory, { force: true, recursive: true });
    }
  },
);

test("native loop detach settles on authoritative kernel state", async () => {
  const source = await readFile(
    new URL("../native/linux-ext4-inspector.c", import.meta.url),
    "utf8",
  );
  const start = source.indexOf("static int detached_loop_settled(");
  const end = source.indexOf("static int detach_loop_device(", start);
  assert(start >= 0);
  assert(end > start);
  const body = source.slice(start, end);
  const exactRdev = body.indexOf(
    "major(metadata.st_rdev) != receipt->device_major",
  );
  const exactRdevMinor = body.indexOf(
    "minor(metadata.st_rdev) != receipt->device_minor",
  );
  const unbound = body.indexOf(
    "ioctl(loop_fd, LOOP_GET_STATUS64, &information) == 0 || errno != ENXIO",
  );
  const diskSequence = body.indexOf(
    "read_disk_sequence(receipt->device_major, receipt->device_minor,",
  );
  const strictAdvance = body.indexOf(
    "current_sequence <= prior_disk_sequence",
  );
  const backingAbsent = body.indexOf("!loop_sysfs_backing_absent(");
  assert(exactRdev >= 0);
  assert(exactRdevMinor > exactRdev);
  assert(unbound > exactRdevMinor);
  assert(diskSequence > unbound);
  assert(strictAdvance > diskSequence);
  assert(backingAbsent > strictAdvance);
  assert.match(
    body,
    /major\(metadata\.st_rdev\) != receipt->device_major \|\|\s+minor\(metadata\.st_rdev\) != receipt->device_minor/u,
  );
  assert.match(
    body,
    /if \(ioctl\(loop_fd, LOOP_GET_STATUS64, &information\) == 0 \|\| errno != ENXIO\) \{\s+\(void\)close\(loop_fd\);\s+return 0;/u,
  );
  assert.match(
    body,
    /close\(loop_fd\) != 0 \|\|\s+read_disk_sequence\(receipt->device_major, receipt->device_minor,\s+&current_sequence\) != 0 \|\|\s+current_sequence <= prior_disk_sequence \|\|\s+!loop_sysfs_backing_absent\(receipt->device_major,\s+receipt->device_minor\)/u,
  );
  assert.doesNotMatch(body, /udev/u);
  assert.doesNotMatch(source, /udev_database_has_sequence|\/run\/udev\/data/u);
});

test("native unmount releases mounted-root authority before non-lazy unmount", async () => {
  const source = await readFile(
    new URL("../native/linux-ext4-inspector.c", import.meta.url),
    "utf8",
  );
  const start = source.indexOf("static int unmount_ext4_root(");
  const end = source.indexOf("static int remove_pinned_child(", start);
  assert(start >= 0);
  assert(end > start);
  const body = source.slice(start, end);
  const childPath = body.indexOf(
    "format_proc_fd_child_path(parent_fd, name, target_proc_path)",
  );
  const sync = body.indexOf("syncfs(target_fd)");
  const persistentRevalidation = body.indexOf(
    "require_persistent_identity(target_fd, target_filesystem_id,",
    sync,
  );
  const clearAuthority = body.indexOf("target_fd = -1", persistentRevalidation);
  const closeAuthority = body.indexOf("close(closing_fd)", clearAuthority);
  const unmount = body.indexOf(
    "umount2(target_proc_path, UMOUNT_NOFOLLOW)",
    closeAuthority,
  );
  const reopenHost = body.indexOf("host_fd = open_direct_child", unmount);
  assert(childPath >= 0);
  assert(sync > childPath);
  assert(persistentRevalidation > sync);
  assert(clearAuthority > persistentRevalidation);
  assert(closeAuthority > clearAuthority);
  assert(unmount > closeAuthority);
  assert(reopenHost > unmount);
  assert.match(body, /before_mount_id == parent_mount_id/u);
  assert.match(body, /after_mount_id != parent_mount_id/u);
  assert.doesNotMatch(body, /format_proc_fd_path\(target_fd, target_proc_path\)/u);
  assert.doesNotMatch(
    body.slice(unmount),
    /require_private_policy\(target_fd,/u,
  );
});

test("native mount and unmount require a private parent carrier before dispatch", async () => {
  const source = await readFile(
    new URL("../native/linux-ext4-inspector.c", import.meta.url),
    "utf8",
  );
  const mountStart = source.indexOf("static int mount_ext4_loop(");
  const mountEnd = source.indexOf("static int unmount_ext4_root(", mountStart);
  const unmountEnd = source.indexOf(
    "static int remove_pinned_child(",
    mountEnd,
  );
  assert(mountStart >= 0);
  assert(mountEnd > mountStart);
  assert(unmountEnd > mountEnd);
  const mountBody = source.slice(mountStart, mountEnd);
  const mountCarrierCheck = mountBody.indexOf(
    "require_private_mount_carrier(parent_fd, 0)",
  );
  const mountDispatch = mountBody.indexOf(
    'mount(source_proc_path, target_proc_path, "ext4"',
  );
  assert(mountCarrierCheck >= 0);
  assert(mountDispatch > mountCarrierCheck);
  assert.match(
    mountBody.slice(mountDispatch),
    /require_private_mount_carrier\(parent_fd, 1\)/u,
  );
  const unmountBody = source.slice(mountEnd, unmountEnd);
  const unmountCarrierCheck = unmountBody.indexOf(
    "require_private_mount_carrier(parent_fd, 0)",
  );
  const unmountDispatch = unmountBody.indexOf(
    "umount2(target_proc_path, UMOUNT_NOFOLLOW)",
  );
  assert(unmountCarrierCheck >= 0);
  assert(unmountDispatch > unmountCarrierCheck);
  assert.match(
    unmountBody.slice(unmountDispatch),
    /require_private_mount_carrier\(parent_fd, 1\)/u,
  );
});

test("listMountPoints parses current Linux mountinfo and freezes the result", async () => {
  const reads = [];
  const fixture = createFixture({
    readMountInfo: async (...args) => {
      reads.push(args);
      return Buffer.from(
        "29 23 0:25 / / rw,relatime - ext4 /dev/root rw\n" +
          "30 29 0:26 / /srv/portable\\040data rw - tmpfs tmpfs rw\n",
        "utf8",
      );
    },
  });
  const pending = fixture.inspector.listMountPoints();
  assert.strictEqual(Object.getPrototypeOf(pending), Promise.prototype);
  const mountPoints = await pending;
  assert.deepEqual(mountPoints, ["/", "/srv/portable data"]);
  assert.equal(Object.isFrozen(mountPoints), true);
  assert.deepEqual(reads, [["/proc/self/mountinfo"]]);
});

test("captured intrinsics preserve every external boundary after poisoning", async () => {
  const moduleUrl = new URL(
    "../src/linux-ext4-inspector.mjs",
    import.meta.url,
  ).href;
  const script = `
import { Buffer } from "node:buffer";
import { TextDecoder } from "node:util";
import { createLinuxExt4Inspector } from ${JSON.stringify(moduleUrl)};

const defineProperty = Object.defineProperty;
const getOwnPropertyDescriptor = Object.getOwnPropertyDescriptor;
function replaceIntrinsic(target, key, replacement) {
  const descriptor = getOwnPropertyDescriptor(target, key);
  defineProperty(target, key, { ...descriptor, value: replacement });
  return () => defineProperty(target, key, descriptor);
}

const fixedCompletion = {
  stderr: Buffer.alloc(0),
  stdout: Buffer.from(${JSON.stringify(helperLine())}, "utf8"),
};
const mountInfo = Buffer.from(
  "29 23 0:25 / / rw,relatime - ext4 /dev/root rw\\n",
  "utf8",
);
const options = {
  helperPath: ${JSON.stringify(HELPER_PATH)},
  platform: "linux",
  readMountInfo: async () => mountInfo,
  runHelper: async () => fixedCompletion,
  trustedRoots: [${JSON.stringify(TRUSTED_ROOT)}],
};
const poisonTargets = [
  [Array, "isArray"],
  [Array.prototype, "join"],
  [Array.prototype, "push"],
  [Buffer, "byteLength"],
  [Buffer, "from"],
  [Buffer, "isBuffer"],
  [JSON, "parse"],
  [JSON, "stringify"],
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
  [String.prototype, "indexOf"],
  [String.prototype, "slice"],
  [String.prototype, "split"],
  [String.prototype, "startsWith"],
  [TextDecoder.prototype, "decode"],
  [WeakSet.prototype, "add"],
  [WeakSet.prototype, "has"],
];
const restorations = [];
const poison = () => { throw new Error("poisoned intrinsic must not run"); };
let filesystem;
let identity;
let inspected;
let mountPoints;
try {
  for (let index = 0; index < poisonTargets.length; index += 1) {
    restorations[index] = replaceIntrinsic(
      poisonTargets[index][0],
      poisonTargets[index][1],
      poison,
    );
  }
  const inspector = createLinuxExt4Inspector(options);
  inspected = await inspector.inspectFilesystemObject(
    ${JSON.stringify(`${TRUSTED_ROOT}/session`)},
  );
  filesystem = inspected.filesystem;
  identity = inspected.identity;
  mountPoints = await inspector.listMountPoints();
} finally {
  for (let index = restorations.length - 1; index >= 0; index -= 1) {
    restorations[index]();
  }
}
if (
  filesystem.filesystemId !== ${JSON.stringify(`ext4fs:${FILESYSTEM_UUID}`)} ||
  identity.objectId !== ${JSON.stringify(OBJECT_ID)} ||
  mountPoints.length !== 1 ||
  mountPoints[0] !== "/" ||
  !Object.isFrozen(filesystem) ||
  !Object.isFrozen(identity) ||
  !Object.isFrozen(inspected) ||
  !Object.isFrozen(mountPoints)
) {
  throw new Error("unexpected hardened inspector result");
}
process.stdout.write("ok\\n");
`;
  const { stderr, stdout } = await execFileAsync(
    process.execPath,
    ["--input-type=module", "--eval", script],
    {
      encoding: "buffer",
      env: { LANG: "C", LC_ALL: "C" },
      maxBuffer: 16 * 1024,
      shell: false,
      timeout: 5_000,
    },
  );
  assert.deepEqual(stdout, Buffer.from("ok\n", "utf8"));
  assert.equal(stderr.length, 0);
});

test("invalid, escaped, and outside paths never dispatch the helper", async () => {
  const fixture = createFixture();
  const cases = [
    ["relative/path", "invalid_path"],
    [`${TRUSTED_ROOT}/../escape`, "invalid_path"],
    [`${TRUSTED_ROOT}//sessions`, "invalid_path"],
    [`${TRUSTED_ROOT}\0/sessions`, "invalid_path"],
    [`${TRUSTED_ROOT}/\ud800`, "invalid_path"],
    ["/srv/portable-codex-other/session", "path_mismatch"],
    ["/outside/session", "path_mismatch"],
  ];
  for (const [path, code] of cases) {
    const pending = fixture.inspector.inspectFilesystem(path);
    assert.strictEqual(Object.getPrototypeOf(pending), Promise.prototype);
    await assert.rejects(pending, inspectorError(code));
  }
  assert.equal(fixture.calls.length, 0);
});

test("constructor rejects pairwise-overlapping trusted roots before dispatch", () => {
  let calls = 0;
  const base = {
    helperPath: HELPER_PATH,
    platform: "linux",
    runHelper: async () => {
      calls += 1;
      return completion();
    },
  };
  for (const trustedRoots of [
    [TRUSTED_ROOT, `${TRUSTED_ROOT}/nested`],
    [`${TRUSTED_ROOT}/nested`, TRUSTED_ROOT],
    ["/", TRUSTED_ROOT],
  ]) {
    assert.throws(
      () => createLinuxExt4Inspector({ ...base, trustedRoots }),
      inspectorError("invalid_options"),
    );
  }
  assert.doesNotThrow(() =>
    createLinuxExt4Inspector({
      ...base,
      trustedRoots: [TRUSTED_ROOT, "/srv/portable-codex-other"],
    }),
  );
  assert.equal(calls, 0);
});

test("oversized UTF-16 paths reject before allocating a UTF-8 buffer", async () => {
  const originalBufferFrom = Buffer.from;
  let oversizedEncodes = 0;
  Buffer.from = function guardedBufferFrom(value, ...args) {
    if (typeof value === "string" && value.length > 4095) {
      oversizedEncodes += 1;
      throw new Error("oversized path reached Buffer.from");
    }
    return Reflect.apply(originalBufferFrom, Buffer, [value, ...args]);
  };
  let fresh;
  try {
    fresh = await import(
      new URL(
        "../src/linux-ext4-inspector.mjs?oversized-path-test",
        import.meta.url,
      ).href
    );
  } finally {
    Buffer.from = originalBufferFrom;
  }

  const calls = [];
  const inspector = fresh.createLinuxExt4Inspector({
    helperPath: HELPER_PATH,
    platform: "linux",
    readMountInfo: async () => {
      calls.push("mountinfo");
      return Buffer.alloc(0);
    },
    runHelper: async () => {
      calls.push("helper");
      return completion();
    },
    trustedRoots: [TRUSTED_ROOT],
  });
  const oversizedPath = `${TRUSTED_ROOT}/${"a".repeat(1024 * 1024)}`;
  await assert.rejects(
    inspector.inspectFilesystem(oversizedPath),
    (error) => {
      assert(error instanceof fresh.LinuxExt4InspectorError);
      assert.equal(error.code, "invalid_path");
      return true;
    },
  );
  assert.equal(oversizedEncodes, 0);
  assert.deepEqual(calls, []);
});

test("helper exit statuses preserve missing, unreadable, mismatch, and unsupported", async () => {
  const cases = [
    [66, "path_missing"],
    [77, "path_unreadable"],
    [65, "path_mismatch"],
    [69, "unsupported"],
  ];
  for (const [exitCode, code] of cases) {
    let calls = 0;
    const fixture = createFixture({
      runHelper: async () => {
        calls += 1;
        return completion({
          exitCode,
          stdout: Buffer.alloc(0),
        });
      },
    });
    await assert.rejects(
      fixture.inspector.inspectFilesystem(`${TRUSTED_ROOT}/session`),
      inspectorError(code),
    );
    assert.equal(calls, 1);
  }
});

test("existing create-directory read failures do not become path mismatches", async () => {
  const request = {
    exclusive: false,
    operation: "create-directory",
    parentDevice: "2049",
    parentInode: "42",
    path: `${TRUSTED_ROOT}/existing`,
  };
  const cases = [
    [65, "path_mismatch"],
    [74, "operation_outcome_uncertain"],
    [77, "operation_outcome_uncertain"],
  ];
  for (const [exitCode, code] of cases) {
    const fixture = createFixture({
      runHelper: async () =>
        completion({ exitCode, stdout: Buffer.alloc(0) }),
    });
    await assert.rejects(
      fixture.inspector.runFdOperation(request),
      inspectorError(code),
    );
  }
});

test("exclusive create reports a proven pre-dispatch existing target", async () => {
  const fixture = createFixture({
    runHelper: async () =>
      completion({ exitCode: 73, stdout: Buffer.alloc(0) }),
  });
  await assert.rejects(
    fixture.inspector.runFdOperation({
      operation: "create-image",
      parentDevice: "2049",
      parentInode: "42",
      path: `${TRUSTED_ROOT}/volume.ext4`,
      sizeBytes: 1024 * 1024,
    }),
    inspectorError("path_exists"),
  );
});

test("loop devices use the canonical native 0..4095 domain", async () => {
  const findRequest = {
    device: "2049",
    inode: "42",
    operation: "find-loop",
    parentDevice: "2049",
    parentInode: "41",
    path: `${TRUSTED_ROOT}/volume.ext4`,
  };
  const accepted = createFixture({
    runHelper: async () =>
      completion({
        stdout: Buffer.from(loopLine("/dev/loop4095"), "utf8"),
      }),
  });
  assert.equal(
    (await accepted.inspector.runFdOperation(findRequest)).loopDevice,
    "/dev/loop4095",
  );

  const rejectedOutput = createFixture({
    runHelper: async () =>
      completion({
        stdout: Buffer.from(loopLine("/dev/loop4096"), "utf8"),
      }),
  });
  await assert.rejects(
    rejectedOutput.inspector.runFdOperation(findRequest),
    inspectorError("helper_output_invalid"),
  );

  let dispatches = 0;
  const requestBoundary = createFixture({
    runHelper: async () => {
      dispatches += 1;
      return completion({
        stdout: Buffer.from(loopLine("/dev/loop4095"), "utf8"),
      });
    },
  });
  const inspectRequest = {
    ...findRequest,
    loopDevice: "/dev/loop4095",
    operation: "inspect-loop",
  };
  assert.equal(
    (await requestBoundary.inspector.runFdOperation(inspectRequest)).loopDevice,
    "/dev/loop4095",
  );
  for (const loopDevice of ["/dev/loop4096", "/dev/loop04095"]) {
    await assert.rejects(
      requestBoundary.inspector.runFdOperation({
        ...inspectRequest,
        loopDevice,
      }),
      inspectorError("invalid_path"),
    );
  }
  assert.equal(dispatches, 1);
});

test("malformed helper records and success diagnostics fail closed", async () => {
  const malformed = [
    completion({ stdout: "not bytes" }),
    completion({ stdout: Buffer.from("{}\n", "utf8") }),
    completion({ stdout: Buffer.from(`${helperLine()}\n`, "utf8") }),
    completion({ stdout: Buffer.from(helperLine().trimEnd(), "utf8") }),
    completion({ stdout: Buffer.from("\xff\n", "binary") }),
    completion({ stdout: Buffer.from(helperLine({ extra: true }), "utf8") }),
    completion({ stdout: Buffer.from(helperLine({ device: 2049 }), "utf8") }),
    completion({
      stdout: Buffer.from(
        helperLine({ device: "18446744073709551616" }),
        "utf8",
      ),
    }),
    completion({ stdout: Buffer.from(helperLine({ inode: "0" }), "utf8") }),
    completion({
      stdout: Buffer.from(
        helperLine({ filesystemUuid: FILESYSTEM_UUID.toUpperCase() }),
        "utf8",
      ),
    }),
    completion({
      stdout: Buffer.from(
        helperLine({
          filesystemUuid: "00000000-0000-0000-0000-000000000000",
        }),
        "utf8",
      ),
    }),
    completion({
      stdout: Buffer.from(helperLine({ objectId: OBJECT_ID.toUpperCase() }), "utf8"),
    }),
    completion({
      stdout: Buffer.from(
        `${JSON.stringify(
          {
            filesystemUuid: FILESYSTEM_UUID,
            device: "2049",
            inode: "42",
            objectId: OBJECT_ID,
          },
          null,
          2,
        )}\n`,
        "utf8",
      ),
    }),
    completion({ stderr: Buffer.from("unexpected diagnostics", "utf8") }),
  ];
  for (const value of malformed) {
    const fixture = createFixture({ runHelper: async () => value });
    await assert.rejects(
      fixture.inspector.inspectPersistentObjectIdentity(
        `${TRUSTED_ROOT}/session`,
      ),
      inspectorError("helper_output_invalid"),
    );
  }
});

test("stdout and stderr limits are independently enforced", async () => {
  for (const value of [
    completion({ stdout: Buffer.alloc(4097) }),
    completion({ stderr: Buffer.alloc(4097) }),
  ]) {
    const fixture = createFixture({ runHelper: async () => value });
    await assert.rejects(
      fixture.inspector.inspectFilesystem(`${TRUSTED_ROOT}/session`),
      inspectorError("helper_output_too_large"),
    );
  }
});

test("helper launch and unexpected execution failures remain distinct", async () => {
  const unavailable = createFixture({
    runHelper: async () => {
      const error = new Error("missing executable");
      error.code = "ENOENT";
      throw error;
    },
  });
  await assert.rejects(
    unavailable.inspector.inspectFilesystem(`${TRUSTED_ROOT}/session`),
    inspectorError("helper_unavailable"),
  );

  for (const runHelper of [
    async () => {
      throw new Error("unexpected child failure");
    },
    async () => completion({ exitCode: 74, stdout: Buffer.alloc(0) }),
    async () => completion({ signal: "SIGKILL", stdout: Buffer.alloc(0) }),
    async () => ({ ...completion(), code: "unexpected" }),
  ]) {
    const fixture = createFixture({ runHelper });
    await assert.rejects(
      fixture.inspector.inspectFilesystem(`${TRUSTED_ROOT}/session`),
      inspectorError("helper_failed"),
    );
  }
});

test("inspector methods enforce arity and their exact instance receiver", () => {
  const firstCalls = [];
  const secondCalls = [];
  const first = createFixture({ calls: firstCalls }).inspector;
  const second = createFixture({ calls: secondCalls }).inspector;
  assert.equal(first.inspectFilesystem.length, 1);
  assert.equal(first.inspectFilesystemObject.length, 1);
  assert.equal(first.inspectPersistentObjectIdentity.length, 1);
  assert.equal(first.listMountPoints.length, 0);
  assert.equal(first.provisionControlRoot.length, 1);
  assert.equal(first.runFdOperation.length, 1);
  assert.equal(Object.isFrozen(first), true);
  assert.equal(Object.isFrozen(first.inspectFilesystem), true);
  assert.equal(Object.isFrozen(first.inspectFilesystemObject), true);
  assert.equal(Object.isFrozen(first.inspectPersistentObjectIdentity), true);
  assert.equal(Object.isFrozen(first.listMountPoints), true);
  assert.equal(Object.isFrozen(first.provisionControlRoot), true);
  assert.equal(Object.isFrozen(first.runFdOperation), true);

  const clone = Object.freeze({ ...first });
  for (const receiver of [undefined, null, {}, clone, second]) {
    assert.throws(
      () =>
        Reflect.apply(first.inspectFilesystem, receiver, [
          `${TRUSTED_ROOT}/session`,
        ]),
      TypeError,
    );
    assert.throws(
      () =>
        Reflect.apply(first.inspectFilesystemObject, receiver, [
          `${TRUSTED_ROOT}/session`,
        ]),
      TypeError,
    );
    assert.throws(
      () =>
        Reflect.apply(first.inspectPersistentObjectIdentity, receiver, [
          `${TRUSTED_ROOT}/session`,
        ]),
      TypeError,
    );
    assert.throws(
      () => Reflect.apply(first.listMountPoints, receiver, []),
      TypeError,
    );
    assert.throws(
      () =>
        Reflect.apply(first.provisionControlRoot, receiver, [
          { kind: "journal", rootPath: TRUSTED_ROOT },
        ]),
      TypeError,
    );
    assert.throws(
      () =>
        Reflect.apply(first.runFdOperation, receiver, [
          { device: "1", inode: "1", operation: "syncfs", path: TRUSTED_ROOT },
        ]),
      TypeError,
    );
  }
  assert.throws(() => first.inspectFilesystem(), TypeError);
  assert.throws(() => first.inspectFilesystemObject(), TypeError);
  assert.throws(
    () => first.inspectFilesystem(`${TRUSTED_ROOT}/session`, null),
    TypeError,
  );
  assert.throws(() => first.inspectPersistentObjectIdentity(), TypeError);
  assert.throws(() => first.listMountPoints(null), TypeError);
  assert.throws(() => first.provisionControlRoot(), TypeError);
  assert.throws(() => first.runFdOperation(), TypeError);
  assert.equal(firstCalls.length, 0);
  assert.equal(secondCalls.length, 0);
});

test("constructor and factory reject malformed configuration before dispatch", () => {
  const valid = {
    helperPath: HELPER_PATH,
    platform: "linux",
    runHelper: async () => completion(),
    trustedRoots: [TRUSTED_ROOT],
  };
  assert.equal(LinuxExt4Inspector.length, 1);
  assert.equal(createLinuxExt4Inspector.length, 1);
  assert.throws(() => new LinuxExt4Inspector(), TypeError);
  assert.throws(() => createLinuxExt4Inspector(), TypeError);
  assert.throws(
    () => createLinuxExt4Inspector({ ...valid, helperPath: "relative-helper" }),
    inspectorError("invalid_options"),
  );
  assert.throws(
    () => createLinuxExt4Inspector({ ...valid, trustedRoots: [] }),
    inspectorError("invalid_options"),
  );
  assert.throws(
    () => createLinuxExt4Inspector({ ...valid, runHelper: null }),
    inspectorError("invalid_options"),
  );
  assert.throws(
    () => createLinuxExt4Inspector({ ...valid, readMountInfo: null }),
    inspectorError("invalid_options"),
  );
  assert.throws(
    () =>
      createLinuxExt4Inspector({
        ...valid,
        trustedRoots: [TRUSTED_ROOT, TRUSTED_ROOT],
      }),
    inspectorError("invalid_options"),
  );
  assert.throws(
    () =>
      createLinuxExt4Inspector({
        ...valid,
        helperRunner: async () => completion(),
      }),
    inspectorError("invalid_options"),
  );

  const hostile = { ...valid };
  Object.defineProperty(hostile, "helperPath", {
    enumerable: true,
    get() {
      assert.fail("option getter must not run");
    },
  });
  assert.throws(
    () => createLinuxExt4Inspector(hostile),
    inspectorError("invalid_options"),
  );
});

test("Proxy and accessor boundaries fail without invoking hostile traps", async () => {
  let trapCalls = 0;
  const traps = {
    get() {
      trapCalls += 1;
      throw new Error("hostile get trap must not run");
    },
    getOwnPropertyDescriptor() {
      trapCalls += 1;
      throw new Error("hostile descriptor trap must not run");
    },
    getPrototypeOf() {
      trapCalls += 1;
      throw new Error("hostile prototype trap must not run");
    },
    ownKeys() {
      trapCalls += 1;
      throw new Error("hostile ownKeys trap must not run");
    },
  };
  const valid = {
    helperPath: HELPER_PATH,
    platform: "linux",
    runHelper: async () => completion(),
    trustedRoots: [TRUSTED_ROOT],
  };
  assert.throws(
    () => createLinuxExt4Inspector(new Proxy({}, traps)),
    inspectorError("invalid_options"),
  );
  assert.throws(
    () =>
      createLinuxExt4Inspector({
        ...valid,
        trustedRoots: new Proxy([TRUSTED_ROOT], traps),
      }),
    inspectorError("invalid_options"),
  );
  assert.throws(
    () =>
      createLinuxExt4Inspector({
        ...valid,
        runHelper: new Proxy(async () => completion(), traps),
      }),
    inspectorError("invalid_options"),
  );
  assert.equal(trapCalls, 0);

  const pathFixture = createFixture();
  const hostilePath = new Proxy(new String(`${TRUSTED_ROOT}/session`), traps);
  await assert.rejects(
    pathFixture.inspector.inspectFilesystem(hostilePath),
    inspectorError("invalid_path"),
  );
  assert.equal(pathFixture.calls.length, 0);
  assert.equal(trapCalls, 0);

  const promiseSafeTraps = {
    ...traps,
    get(target, key) {
      void target;
      if (key === "then") return undefined;
      return traps.get();
    },
  };
  const completionProxy = new Proxy(completion(), promiseSafeTraps);
  const completionFixture = createFixture({
    runHelper: async () => completionProxy,
  });
  await assert.rejects(
    completionFixture.inspector.inspectFilesystem(`${TRUSTED_ROOT}/session`),
    inspectorError("helper_failed"),
  );
  assert.equal(trapCalls, 0);

  const mountInfoProxy = new Proxy(
    Buffer.from("29 23 0:25 / / rw - ext4 /dev/root rw\n", "utf8"),
    promiseSafeTraps,
  );
  const mountFixture = createFixture({
    readMountInfo: async () => mountInfoProxy,
  });
  await assert.rejects(
    mountFixture.inspector.listMountPoints(),
    inspectorError("mountinfo_failed"),
  );
  assert.equal(trapCalls, 0);

  let codeGetterCalls = 0;
  const accessorError = {};
  Object.defineProperty(accessorError, "code", {
    enumerable: true,
    get() {
      codeGetterCalls += 1;
      throw new Error("error code getter must not run");
    },
  });
  const accessorFixture = createFixture({
    runHelper: async () => {
      throw accessorError;
    },
  });
  await assert.rejects(
    accessorFixture.inspector.inspectFilesystem(`${TRUSTED_ROOT}/session`),
    inspectorError("helper_failed"),
  );
  assert.equal(codeGetterCalls, 0);
});

test("helper and mountinfo callbacks require exact native Promise settlements", async (t) => {
  for (const unsafe of promiseSettlementCases(completion())) {
    await t.test(`runHelper ${unsafe.name}`, async () => {
      const settlement = unsafe.create();
      let calls = 0;
      const fixture = createFixture({
        runHelper() {
          calls += 1;
          return settlement.value;
        },
      });
      await assert.rejects(
        fixture.inspector.inspectFilesystem(`${TRUSTED_ROOT}/session`),
        inspectorError("helper_failed"),
      );
      assert.equal(calls, 1);
      settlement.assertUntouched();
    });
  }

  const mountInfo = Buffer.from(
    "29 23 0:25 / / rw,relatime - ext4 /dev/root rw\n",
    "utf8",
  );
  for (const unsafe of promiseSettlementCases(mountInfo)) {
    await t.test(`readMountInfo ${unsafe.name}`, async () => {
      const settlement = unsafe.create();
      let calls = 0;
      const fixture = createFixture({
        readMountInfo() {
          calls += 1;
          return settlement.value;
        },
      });
      await assert.rejects(
        fixture.inspector.listMountPoints(),
        inspectorError("mountinfo_failed"),
      );
      assert.equal(calls, 1);
      settlement.assertUntouched();
    });
  }
});

test(
  "the default platform rejects non-Linux hosts",
  { skip: process.platform === "linux" },
  () => {
    assert.throws(
      () =>
        createLinuxExt4Inspector({
          helperPath: HELPER_PATH,
          trustedRoots: [TRUSTED_ROOT],
        }),
      inspectorError("unsupported_platform"),
    );
  },
);

test("malformed and oversized mountinfo fail without helper dispatch", async () => {
  for (const bytes of [
    Buffer.from("malformed\n", "utf8"),
    Buffer.alloc(1024 * 1024 + 1),
    "not bytes",
  ]) {
    const fixture = createFixture({ readMountInfo: async () => bytes });
    await assert.rejects(
      fixture.inspector.listMountPoints(),
      inspectorError("mountinfo_failed"),
    );
    assert.equal(fixture.calls.length, 0);
  }
});
