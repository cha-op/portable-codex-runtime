import assert from "node:assert/strict";
import { execFile, spawn } from "node:child_process";
import { createHash, randomBytes } from "node:crypto";
import { constants as fsConstants, createReadStream, createWriteStream } from "node:fs";
import {
  access,
  chmod,
  lstat,
  mkdir,
  open,
  readFile,
  rm,
} from "node:fs/promises";
import { basename, dirname, isAbsolute, join, resolve } from "node:path";
import { pipeline } from "node:stream/promises";
import test from "node:test";
import { setTimeout as delay } from "node:timers/promises";
import { fileURLToPath } from "node:url";
import { promisify, TextDecoder } from "node:util";

import {
  repairStoppedRolloutTails,
} from "../src/rollout-tail-repair.mjs";

const execFileAsync = promisify(execFile);
const CURRENT_FILE = fileURLToPath(import.meta.url);
const WRITER_MODE = process.argv[2] === "--writer";
const CONFORMANCE_ENABLED =
  process.env.LINUX_EXT4_CRASH_PREFIX_CONFORMANCE === "1";
const TEST_ROOT_PREFIX =
  "portable-codex-runtime-linux-ext4-crash-prefix-";
const ROOT_SESSION_ID = "019f2a00-0000-7000-8000-000000000001";
const RUNTIME_IDENTITY = Object.freeze({
  codexBinarySha256: "ab".repeat(32),
  codexVersion: "codex-cli 0.144.1",
  sourceAnalysisCommit: "db887d03e1f907467e33271572dffb73bceecd6b",
});
const FULL_PREFIX = Buffer.from(
  `${JSON.stringify(sessionMeta())}\n${JSON.stringify(event(1))}\n`,
  "utf8",
);
const PARTIAL_SUFFIX = Buffer.from(
  '{"type":"event_msg","payload":{"sequence":2',
  "utf8",
);
const CONTINUATION = Buffer.from(`${JSON.stringify(event(3))}\n`, "utf8");
const COMMAND_TIMEOUT_MILLISECONDS = 60_000;
const WRITER_READY_TIMEOUT_MILLISECONDS = 30_000;
const COMMANDS = Object.freeze({
  blockdev: "/usr/sbin/blockdev",
  dd: "/usr/bin/dd",
  dmsetup: "/usr/sbin/dmsetup",
  findmnt: "/usr/bin/findmnt",
  getfacl: "/usr/bin/getfacl",
  losetup: "/usr/sbin/losetup",
  lvchange: "/usr/sbin/lvchange",
  lvcreate: "/usr/sbin/lvcreate",
  lvremove: "/usr/sbin/lvremove",
  lvs: "/usr/sbin/lvs",
  mkfsExt4: "/usr/sbin/mkfs.ext4",
  mount: "/usr/bin/mount",
  pvcreate: "/usr/sbin/pvcreate",
  pvremove: "/usr/sbin/pvremove",
  udevadm: "/usr/bin/udevadm",
  umount: "/usr/bin/umount",
  vgcreate: "/usr/sbin/vgcreate",
  vgremove: "/usr/sbin/vgremove",
});
const UTF8_DECODER = new TextDecoder("utf-8", { fatal: true });

function sessionMeta() {
  return {
    timestamp: "2026-08-28T00:00:00.000Z",
    type: "session_meta",
    payload: {
      cli_version: "0.144.1",
      cwd: "/workspace",
      id: ROOT_SESSION_ID,
      originator: "linux-ext4-crash-prefix-conformance",
      session_id: ROOT_SESSION_ID,
      timestamp: "2026-08-28T00:00:00.000Z",
    },
  };
}

function event(sequence) {
  return { type: "event_msg", payload: { sequence } };
}

function parseCompleteJsonLines(bytes) {
  assert.equal(Buffer.isBuffer(bytes), true);
  assert.equal(bytes.length > 0, true);
  assert.equal(bytes.at(-1), 0x0a);
  const text = UTF8_DECODER.decode(bytes);
  const lines = text.split("\n");
  assert.equal(lines.pop(), "");
  return lines.map((line) => {
    assert.notEqual(line, "");
    return JSON.parse(line);
  });
}

function assertNoAbortMarker(bytes) {
  const text = UTF8_DECODER.decode(bytes);
  assert.equal(text.includes("<turn_aborted>"), false);
  assert.equal(text.includes("TurnAborted"), false);
}

function checkedTestRoot(value, suffix) {
  const candidate = value ?? join(
    "/var/tmp",
    `${TEST_ROOT_PREFIX}${process.pid}-${suffix}`,
  );
  if (
    typeof candidate !== "string" ||
    !isAbsolute(candidate) ||
    candidate !== resolve(candidate) ||
    dirname(candidate) !== "/var/tmp" ||
    !basename(candidate).startsWith(TEST_ROOT_PREFIX) ||
    !/^[-A-Za-z0-9._]+$/u.test(basename(candidate))
  ) {
    throw new TypeError("unsafe Linux ext4 crash-prefix test root");
  }
  return candidate;
}

function volumeGroupName(suffix) {
  const name = `pcrcp_${process.pid}_${suffix}`;
  assert.match(name, /^pcrcp_[0-9]+_[0-9a-f]{8}$/u);
  return name;
}

async function runCommand(executable, arguments_, options = {}) {
  return execFileAsync(executable, arguments_, {
    encoding: "utf8",
    env: { ...process.env, LANG: "C", LC_ALL: "C" },
    maxBuffer: 128 * 1024,
    timeout: COMMAND_TIMEOUT_MILLISECONDS,
    ...options,
  });
}

async function assertPrerequisites() {
  assert.equal(process.platform, "linux");
  assert.equal(process.geteuid?.(), 0);
  for (const executable of Object.values(COMMANDS)) {
    await access(executable, fsConstants.X_OK);
  }
}

async function syncDirectory(path) {
  const handle = await open(path, fsConstants.O_RDONLY | fsConstants.O_DIRECTORY);
  try {
    await handle.sync();
  } finally {
    await handle.close();
  }
}

async function writeFully(handle, bytes) {
  let offset = 0;
  while (offset < bytes.length) {
    const { bytesWritten } = await handle.write(
      bytes,
      offset,
      bytes.length - offset,
      null,
    );
    assert.equal(bytesWritten > 0, true);
    offset += bytesWritten;
  }
}

async function runCrashPrefixWriter(rolloutPath) {
  if (
    typeof rolloutPath !== "string" ||
    !isAbsolute(rolloutPath) ||
    rolloutPath !== resolve(rolloutPath)
  ) {
    throw new TypeError("invalid crash-prefix writer path");
  }
  const handle = await open(rolloutPath, "wx", 0o600);
  await writeFully(handle, FULL_PREFIX);
  await handle.sync();
  await writeFully(handle, PARTIAL_SUFFIX);
  await handle.sync();
  await syncDirectory(dirname(rolloutPath));
  if (typeof process.send !== "function") {
    throw new Error("crash-prefix writer requires an IPC owner");
  }
  process.send({
    prefixBytes: FULL_PREFIX.length,
    partialBytes: PARTIAL_SUFFIX.length,
    type: "ready",
  });
  await new Promise(() => {});
}

function waitForWriterReady(child) {
  return new Promise((resolvePromise, rejectPromise) => {
    let stderr = "";
    const timer = setTimeout(() => {
      settle(new Error("crash-prefix writer readiness timed out"));
    }, WRITER_READY_TIMEOUT_MILLISECONDS);
    timer.unref();

    function cleanup() {
      clearTimeout(timer);
      child.off("error", onError);
      child.off("exit", onExit);
      child.off("message", onMessage);
    }

    function settle(error, value) {
      cleanup();
      if (error) rejectPromise(error);
      else resolvePromise(value);
    }

    function onError(error) {
      settle(error);
    }

    function onExit(code, signal) {
      settle(new Error(
        `crash-prefix writer exited before ready: code=${code} signal=${signal} stderr=${stderr}`,
      ));
    }

    function onMessage(message) {
      try {
        assert.deepEqual(message, {
          prefixBytes: FULL_PREFIX.length,
          partialBytes: PARTIAL_SUFFIX.length,
          type: "ready",
        });
        settle(null, message);
      } catch (error) {
        settle(error);
      }
    }

    child.stderr.setEncoding("utf8");
    child.stderr.on("data", (chunk) => {
      stderr = `${stderr}${chunk}`.slice(-64 * 1024);
    });
    child.once("error", onError);
    child.once("exit", onExit);
    child.on("message", onMessage);
  });
}

async function killWriter(child) {
  assert.equal(child.exitCode, null);
  assert.equal(child.signalCode, null);
  const exited = new Promise((resolvePromise, rejectPromise) => {
    child.once("error", rejectPromise);
    child.once("exit", (code, signal) => resolvePromise({ code, signal }));
  });
  assert.equal(child.kill("SIGKILL"), true);
  const result = await exited;
  assert.deepEqual(result, { code: null, signal: "SIGKILL" });
  return result;
}

function contentProof(bytes) {
  return {
    sha256: createHash("sha256").update(bytes).digest("hex"),
    size: bytes.length,
  };
}

async function digestPath(path) {
  const hash = createHash("sha256");
  let size = 0;
  for await (const chunk of createReadStream(path, { highWaterMark: 1024 * 1024 })) {
    size += chunk.length;
    hash.update(chunk);
  }
  return Object.freeze({ sha256: hash.digest("hex"), size });
}

function regularFileIdentity(metadata) {
  assert.equal(metadata.isFile(), true);
  return Object.freeze({
    dev: metadata.dev,
    ino: metadata.ino,
    mode: metadata.mode & 0o7777n,
    nlink: metadata.nlink,
    size: metadata.size,
  });
}

async function fullCopy(source, destination) {
  await pipeline(
    createReadStream(source, { highWaterMark: 1024 * 1024 }),
    createWriteStream(destination, {
      flags: "wx",
      mode: 0o600,
    }),
  );
  const handle = await open(destination, "r+");
  try {
    await handle.sync();
  } finally {
    await handle.close();
  }
  await syncDirectory(dirname(destination));
}

async function settleUdev() {
  await runCommand(COMMANDS.udevadm, ["settle", "--timeout=30"]);
}

async function assertLoopAbsent(device) {
  const { stdout } = await runCommand(COMMANDS.losetup, [
    "--list",
    "--noheadings",
    "--output",
    "NAME",
  ]);
  assert.equal(
    stdout.split("\n").map((line) => line.trim()).includes(device),
    false,
  );
}

async function detachLoop(device) {
  await runCommand(COMMANDS.losetup, ["--detach", device]);
  await settleUdev();
  await assertLoopAbsent(device);
}

async function attachLoop(imagePath, readOnly = false) {
  const arguments_ = ["--find", "--show", "--nooverlap"];
  if (readOnly) arguments_.push("--read-only");
  arguments_.push(imagePath);
  const { stdout } = await runCommand(COMMANDS.losetup, arguments_);
  const device = stdout.trim();
  assert.match(device, /^\/dev\/loop[0-9]+$/u);
  return device;
}

async function assertSingleMount(device, mountPath, expectedMode) {
  const { stdout } = await runCommand(COMMANDS.findmnt, [
    "--noheadings",
    "--raw",
    "--source",
    device,
    "--output",
    "TARGET,OPTIONS",
  ]);
  const lines = stdout.trim().split("\n").filter((line) => line.length > 0);
  assert.equal(lines.length, 1);
  const match = lines[0].match(/^(\S+)\s+(\S+)$/u);
  assert.notEqual(match, null);
  assert.equal(match[1], mountPath);
  assert.equal(match[2].split(",").includes(expectedMode), true);
}

async function mountExt4(device, mountPath, readOnly) {
  const options = readOnly
    ? "ro,noload,nosuid,nodev,noexec"
    : "rw,nosuid,nodev,noexec";
  await runCommand(COMMANDS.mount, [
    "--types",
    "ext4",
    "--options",
    options,
    device,
    mountPath,
  ]);
}

function trimmedRecord(record) {
  return Object.fromEntries(
    Object.entries(record).map(([key, value]) => [
      key,
      typeof value === "string" ? value.trim() : value,
    ]),
  );
}

async function readDeviceMapperEvidence(device, expectedNameSuffix) {
  const { stdout } = await runCommand(COMMANDS.dmsetup, [
    "info",
    "--columns",
    "--noheadings",
    "--separator",
    "\t",
    "--options",
    "name,uuid,major,minor",
    device,
  ]);
  const rows = stdout
    .trim()
    .split("\n")
    .map((line) => line.split("\t").map((value) => value.trim()));
  assert.equal(rows.length, 1);
  assert.equal(rows[0].length, 4);
  assert.equal(rows[0][0].endsWith(expectedNameSuffix), true);
  assert.match(rows[0][1], /^LVM-/u);
  assert.match(rows[0][2], /^[0-9]+$/u);
  assert.match(rows[0][3], /^[0-9]+$/u);
  return Object.freeze({
    deviceMapperName: rows[0][0],
    deviceMapperUuid: rows[0][1],
    majorMinor: `${rows[0][2]}:${rows[0][3]}`,
  });
}

async function readSnapshotEvidence(
  volumeGroup,
  originDevice,
  snapshotDevice,
) {
  const { stdout: lvsStdout } = await runCommand(COMMANDS.lvs, [
    "--reportformat",
    "json",
    "--units",
    "b",
    "--nosuffix",
    "--options",
    "lv_name,lv_uuid,origin,lv_attr,lv_size,data_percent,segtype",
    "--select",
    `vg_name=${volumeGroup}`,
  ]);
  const report = JSON.parse(lvsStdout);
  const logicalVolumes = report.report?.flatMap((entry) => entry.lv ?? []) ?? [];
  const normalizedVolumes = logicalVolumes.map(trimmedRecord);
  const origin = normalizedVolumes.find((entry) => entry.lv_name === "origin");
  const snapshot = normalizedVolumes
    .find((entry) => entry.lv_name === "crash_snapshot");
  assert.notEqual(origin, undefined);
  assert.notEqual(snapshot, undefined);
  assert.equal(snapshot.origin, "origin");
  assert.equal(snapshot.segtype, "snapshot");
  assert.equal(snapshot.lv_attr[0], "s");
  assert.equal(snapshot.lv_attr[1], "r");
  assert.equal(snapshot.lv_attr[4], "a");
  assert.match(snapshot.data_percent, /^[0-9]+(?:\.[0-9]+)?$/u);
  assert.match(snapshot.lv_size, /^[0-9]+$/u);
  const dataPercent = Number(snapshot.data_percent);
  const size = Number(snapshot.lv_size);
  assert.equal(Number.isFinite(dataPercent), true);
  assert.equal(dataPercent >= 0 && dataPercent < 100, true);
  assert.equal(Number.isSafeInteger(size) && size > 0, true);
  assert.match(origin.lv_uuid, /^[A-Za-z0-9-]+$/u);
  assert.match(snapshot.lv_uuid, /^[A-Za-z0-9-]+$/u);
  assert.notEqual(origin.lv_uuid, snapshot.lv_uuid);
  const originDeviceMapper = await readDeviceMapperEvidence(
    originDevice,
    "-origin",
  );
  const snapshotDeviceMapper = await readDeviceMapperEvidence(
    snapshotDevice,
    "-crash_snapshot",
  );
  assert.notEqual(
    originDeviceMapper.deviceMapperUuid,
    snapshotDeviceMapper.deviceMapperUuid,
  );
  assert.notEqual(
    originDeviceMapper.majorMinor,
    snapshotDeviceMapper.majorMinor,
  );
  return Object.freeze({
    dataPercent,
    origin: Object.freeze({
      ...originDeviceMapper,
      lvmUuid: origin.lv_uuid,
    }),
    lvAttr: snapshot.lv_attr,
    originName: snapshot.origin,
    size,
    snapshot: Object.freeze({
      ...snapshotDeviceMapper,
      lvmUuid: snapshot.lv_uuid,
    }),
  });
}

async function appendContinuation(rolloutPath) {
  const handle = await open(rolloutPath, "a");
  try {
    await writeFully(handle, CONTINUATION);
    await handle.sync();
  } finally {
    await handle.close();
  }
  await syncDirectory(dirname(rolloutPath));
}

async function cleanupResources(resources) {
  const failures = [];
  async function attempt(label, operation) {
    try {
      await operation();
    } catch (error) {
      failures.push(new Error(`cleanup failed: ${label}`, { cause: error }));
    }
  }

  if (
    resources.writer !== null &&
    resources.writer.exitCode === null &&
    resources.writer.signalCode === null
  ) {
    await attempt("writer", async () => {
      const exited = new Promise((resolvePromise) => {
        resources.writer.once("exit", (code, signal) => {
          resolvePromise({ code, signal });
        });
      });
      assert.equal(resources.writer.kill("SIGKILL"), true);
      const joined = await Promise.race([
        exited,
        delay(5_000, null, { ref: false }),
      ]);
      assert.deepEqual(
        joined,
        { code: null, signal: "SIGKILL" },
        "cleanup could not join the killed writer",
      );
    });
  }
  for (const mount of [
    ["repair mount", "repairMounted", resources.repairMount],
    ["origin mount", "originMounted", resources.originMount],
  ]) {
    const [label, state, path] = mount;
    if (!resources[state]) continue;
    await attempt(label, async () => {
      await runCommand(COMMANDS.umount, ["--", path]);
      resources[state] = false;
    });
  }
  for (const loop of [["repair loop", "repairLoop"]]) {
    const [label, state] = loop;
    if (resources[state] === null) continue;
    await attempt(label, async () => {
      await detachLoop(resources[state]);
      resources[state] = null;
    });
  }
  if (resources.volumeGroupCreated) {
    if (resources.snapshotCreated) {
      await attempt("snapshot deactivation", async () => {
        await runCommand(COMMANDS.lvchange, [
          "--activate",
          "n",
          resources.snapshotDevice,
        ]);
      });
      await attempt("snapshot removal", async () => {
        await runCommand(COMMANDS.lvremove, [
          "--force",
          "--yes",
          resources.snapshotDevice,
        ]);
        resources.snapshotCreated = false;
      });
    }
    if (resources.originCreated) {
      await attempt("origin deactivation", async () => {
        await runCommand(COMMANDS.lvchange, [
          "--activate",
          "n",
          resources.originDevice,
        ]);
      });
      await attempt("origin removal", async () => {
        await runCommand(COMMANDS.lvremove, [
          "--force",
          "--yes",
          resources.originDevice,
        ]);
        resources.originCreated = false;
      });
    }
    await attempt("logical-volume udev settlement", async () => {
      await settleUdev();
    });
    await attempt("volume-group removal", async () => {
      await runCommand(COMMANDS.vgremove, [
        "--force",
        "--yes",
        resources.volumeGroup,
      ]);
      resources.volumeGroupCreated = false;
      await settleUdev();
    });
  }
  if (resources.physicalVolumeCreated) {
    await attempt("physical-volume removal", async () => {
      await runCommand(COMMANDS.pvremove, [
        "--force",
        "--force",
        "--yes",
        resources.originLoop,
      ]);
      resources.physicalVolumeCreated = false;
    });
  }
  if (resources.originLoop !== null) {
    await attempt("origin loop", async () => {
      await detachLoop(resources.originLoop);
      resources.originLoop = null;
    });
  }
  const allResourcesRetired =
    !resources.repairMounted &&
    !resources.originMounted &&
    resources.repairLoop === null &&
    resources.originLoop === null &&
    !resources.snapshotCreated &&
    !resources.originCreated &&
    !resources.volumeGroupCreated &&
    !resources.physicalVolumeCreated;
  if (resources.rootCreated && allResourcesRetired) {
    await attempt("test root", async () => {
      await rm(resources.root, { recursive: true });
      resources.rootCreated = false;
    });
  } else if (resources.rootCreated) {
    failures.push(new Error(
      "cleanup withheld test-root removal because a scoped resource remains",
    ));
  }
  if (failures.length > 0) {
    throw new AggregateError(failures, "resource cleanup failed");
  }
}

async function runConformance(t) {
  await assertPrerequisites();
  const suffix = randomBytes(4).toString("hex");
  const root = checkedTestRoot(
    process.env.LINUX_EXT4_CRASH_PREFIX_TEST_ROOT,
    suffix,
  );
  const volumeGroup = volumeGroupName(suffix);
  const originDevice = `/dev/${volumeGroup}/origin`;
  const snapshotDevice = `/dev/${volumeGroup}/crash_snapshot`;
  const resources = {
    originLoop: null,
    originMount: join(root, "origin-mount"),
    originMounted: false,
    originCreated: false,
    originDevice,
    physicalVolumeCreated: false,
    repairLoop: null,
    repairMount: join(root, "repair-mount"),
    repairMounted: false,
    root,
    rootCreated: false,
    snapshotCreated: false,
    snapshotDevice,
    volumeGroup,
    volumeGroupCreated: false,
    writer: null,
  };
  let primaryFailure;
  try {
    await mkdir(root, { mode: 0o700 });
    resources.rootCreated = true;
    await mkdir(resources.originMount, { mode: 0o700 });
    await mkdir(resources.repairMount, { mode: 0o700 });

    const backingPath = join(root, "lvm-backing.raw");
    const backing = await open(backingPath, "wx", 0o600);
    try {
      await backing.truncate(192 * 1024 * 1024);
      await backing.sync();
    } finally {
      await backing.close();
    }
    resources.originLoop = await attachLoop(backingPath);
    await runCommand(COMMANDS.pvcreate, [
      "--force",
      "--yes",
      resources.originLoop,
    ]);
    resources.physicalVolumeCreated = true;
    await runCommand(COMMANDS.vgcreate, [volumeGroup, resources.originLoop]);
    resources.volumeGroupCreated = true;
    await runCommand(COMMANDS.lvcreate, [
      "--yes",
      "--size",
      "64M",
      "--name",
      "origin",
      volumeGroup,
    ]);
    resources.originCreated = true;
    await runCommand(COMMANDS.mkfsExt4, [
      "-F",
      "-q",
      "-E",
      "lazy_itable_init=0,lazy_journal_init=0",
      originDevice,
    ]);
    await mountExt4(originDevice, resources.originMount, false);
    resources.originMounted = true;
    await assertSingleMount(originDevice, resources.originMount, "rw");

    const codexHome = join(resources.originMount, "codex-home");
    const rolloutDirectory = join(
      codexHome,
      "sessions",
      "2026",
      "08",
      "28",
    );
    const rolloutRelativePath = join(
      "codex-home",
      "sessions",
      "2026",
      "08",
      "28",
      "crash-prefix.jsonl",
    );
    const rolloutPath = join(resources.originMount, rolloutRelativePath);
    await mkdir(rolloutDirectory, { mode: 0o700, recursive: true });
    for (const directory of [
      codexHome,
      join(codexHome, "sessions"),
      join(codexHome, "sessions", "2026"),
      join(codexHome, "sessions", "2026", "08"),
      rolloutDirectory,
    ]) {
      await chmod(directory, 0o700);
    }
    await syncDirectory(rolloutDirectory);

    resources.writer = spawn(
      process.execPath,
      [CURRENT_FILE, "--writer", rolloutPath],
      { stdio: ["ignore", "ignore", "pipe", "ipc"] },
    );
    await waitForWriterReady(resources.writer);
    const writerPid = resources.writer.pid;
    assert.equal(Number.isSafeInteger(writerPid) && writerPid > 0, true);
    const writerExit = await killWriter(resources.writer);

    await runCommand(COMMANDS.lvcreate, [
      "--yes",
      "--snapshot",
      "--permission",
      "r",
      "--size",
      "64M",
      "--name",
      "crash_snapshot",
      originDevice,
    ]);
    resources.snapshotCreated = true;
    const snapshotEvidenceAtCapture = await readSnapshotEvidence(
      volumeGroup,
      originDevice,
      snapshotDevice,
    );
    await runCommand(COMMANDS.umount, ["--", resources.originMount]);
    resources.originMounted = false;
    const snapshotEvidenceBeforeExport = await readSnapshotEvidence(
      volumeGroup,
      originDevice,
      snapshotDevice,
    );
    assert.equal(snapshotEvidenceBeforeExport.dataPercent < 100, true);
    assert.equal(
      snapshotEvidenceBeforeExport.size,
      snapshotEvidenceAtCapture.size,
    );
    assert.deepEqual(
      snapshotEvidenceBeforeExport.origin,
      snapshotEvidenceAtCapture.origin,
    );
    assert.deepEqual(
      snapshotEvidenceBeforeExport.snapshot,
      snapshotEvidenceAtCapture.snapshot,
    );

    const artifactPath = join(root, "crash-prefix-artifact.raw");
    const { stdout: blockSizeStdout } = await runCommand(COMMANDS.blockdev, [
      "--getsize64",
      snapshotDevice,
    ]);
    const snapshotBlockSize = Number(blockSizeStdout.trim());
    assert.equal(Number.isSafeInteger(snapshotBlockSize), true);
    assert.equal(snapshotBlockSize, snapshotEvidenceAtCapture.size);
    const previousUmask = process.umask(0o377);
    try {
      await runCommand(COMMANDS.dd, [
        `if=${snapshotDevice}`,
        `of=${artifactPath}`,
        "bs=4M",
        "iflag=fullblock",
        "conv=excl,fsync,sparse",
        "status=none",
      ]);
    } finally {
      process.umask(previousUmask);
    }
    await chmod(artifactPath, 0o400);
    await syncDirectory(root);
    const artifactMetadata = await lstat(artifactPath, { bigint: true });
    const artifactIdentity = regularFileIdentity(artifactMetadata);
    assert.deepEqual(artifactIdentity, {
      dev: artifactMetadata.dev,
      ino: artifactMetadata.ino,
      mode: 0o400n,
      nlink: 1n,
      size: BigInt(snapshotBlockSize),
    });
    const artifactBefore = await digestPath(artifactPath);
    const snapshotBefore = await digestPath(snapshotDevice);
    assert.deepEqual(snapshotBefore, artifactBefore);
    assert.equal(artifactBefore.size, snapshotEvidenceAtCapture.size);

    const repairImagePath = join(root, "writable-repair-copy.raw");
    await fullCopy(artifactPath, repairImagePath);
    const repairMetadata = await lstat(repairImagePath, { bigint: true });
    assert.equal(repairMetadata.isFile(), true);
    assert.equal(repairMetadata.nlink, 1n);
    assert.equal(repairMetadata.mode & 0o7777n, 0o600n);
    assert.equal(repairMetadata.size, artifactMetadata.size);
    assert.equal(repairMetadata.dev, artifactMetadata.dev);
    assert.notEqual(repairMetadata.ino, artifactMetadata.ino);
    assert.deepEqual(await digestPath(repairImagePath), artifactBefore);

    resources.repairLoop = await attachLoop(repairImagePath);
    await mountExt4(resources.repairLoop, resources.repairMount, false);
    resources.repairMounted = true;
    await assertSingleMount(
      resources.repairLoop,
      resources.repairMount,
      "rw",
    );
    const repairCodexHome = join(resources.repairMount, "codex-home");
    const repairRolloutPath = join(resources.repairMount, rolloutRelativePath);
    const preRepairRollout = await readFile(repairRolloutPath);
    assert.deepEqual(
      preRepairRollout,
      Buffer.concat([FULL_PREFIX, PARTIAL_SUFFIX]),
    );
    assertNoAbortMarker(preRepairRollout);
    const repairRequest = {
      codexHome: repairCodexHome,
      rootSessionId: ROOT_SESSION_ID,
      runtimeIdentity: RUNTIME_IDENTITY,
    };
    // This structurally valid synthetic digest exercises the pinned adapter
    // binding; the harness does not claim to authenticate a Codex executable.
    const proof = await repairStoppedRolloutTails(repairRequest);
    assert.deepEqual(proof, {
      compatibility: RUNTIME_IDENTITY,
      files: [{
        action: "truncate_partial_tail",
        after: contentProof(FULL_PREFIX),
        before: contentProof(Buffer.concat([FULL_PREFIX, PARTIAL_SUFFIX])),
        relativePath: "2026/08/28/crash-prefix.jsonl",
        removedBytes: PARTIAL_SUFFIX.length,
      }],
      rootSessionId: ROOT_SESSION_ID,
    });
    assert.deepEqual(await readFile(repairRolloutPath), FULL_PREFIX);
    assertNoAbortMarker(await readFile(repairRolloutPath));
    const replay = await repairStoppedRolloutTails(repairRequest);
    assert.deepEqual(replay, {
      compatibility: RUNTIME_IDENTITY,
      files: [{
        action: "unchanged",
        after: contentProof(FULL_PREFIX),
        before: contentProof(FULL_PREFIX),
        relativePath: "2026/08/28/crash-prefix.jsonl",
        removedBytes: 0,
      }],
      rootSessionId: ROOT_SESSION_ID,
    });

    await appendContinuation(repairRolloutPath);
    const continued = await readFile(repairRolloutPath);
    assertNoAbortMarker(continued);
    assert.deepEqual(parseCompleteJsonLines(continued), [
      sessionMeta(),
      event(1),
      event(3),
    ]);
    await runCommand(COMMANDS.umount, ["--", resources.repairMount]);
    resources.repairMounted = false;
    await detachLoop(resources.repairLoop);
    resources.repairLoop = null;

    resources.repairLoop = await attachLoop(repairImagePath, true);
    await mountExt4(resources.repairLoop, resources.repairMount, true);
    resources.repairMounted = true;
    await assertSingleMount(
      resources.repairLoop,
      resources.repairMount,
      "ro",
    );
    const durableContinuation = await readFile(repairRolloutPath);
    assertNoAbortMarker(durableContinuation);
    assert.deepEqual(parseCompleteJsonLines(durableContinuation), [
      sessionMeta(),
      event(1),
      event(3),
    ]);

    const artifactAfter = await digestPath(artifactPath);
    const artifactMetadataAfter = await lstat(artifactPath, { bigint: true });
    assert.deepEqual(
      regularFileIdentity(artifactMetadataAfter),
      artifactIdentity,
    );
    const snapshotAfter = await digestPath(snapshotDevice);
    assert.deepEqual(artifactAfter, artifactBefore);
    assert.deepEqual(snapshotAfter, snapshotBefore);
    assert.deepEqual(snapshotAfter, artifactAfter);
    const snapshotEvidenceAfterRepair = await readSnapshotEvidence(
      volumeGroup,
      originDevice,
      snapshotDevice,
    );
    assert.equal(snapshotEvidenceAfterRepair.dataPercent < 100, true);
    assert.equal(snapshotEvidenceAfterRepair.lvAttr[1], "r");
    assert.deepEqual(
      snapshotEvidenceAfterRepair.origin,
      snapshotEvidenceAtCapture.origin,
    );
    assert.deepEqual(
      snapshotEvidenceAfterRepair.snapshot,
      snapshotEvidenceAtCapture.snapshot,
    );

    t.diagnostic(JSON.stringify({
      artifact: artifactAfter,
      artifactReadbackBoundary:
        "independent-writable-copy-after-ext4-journal-replay",
      artifactMode: "0400",
      checkpointBoundary: "lvm-mounted-origin-snapshot",
      controllerCachePowerLossClaimed: false,
      evidenceClass: "fsynced-synthetic-partial-application-prefix",
      filesystemFreezeVerified: false,
      repairAction: proof.files[0].action,
      repairReplayAction: replay.files[0].action,
      snapshot: snapshotEvidenceAfterRepair,
      syncBoundary: "fixture-file-and-rollout-directory-fsync",
      writableContinuationRecords: 3,
      writerExit,
      writerPid,
    }));
  } catch (error) {
    primaryFailure = error;
  }

  try {
    await cleanupResources(resources);
  } catch (cleanupError) {
    if (primaryFailure) {
      try {
        Object.defineProperty(primaryFailure, "cleanupError", {
          enumerable: false,
          value: cleanupError,
        });
      } catch {
        // A frozen primary error still takes precedence over cleanup detail.
      }
    } else {
      throw cleanupError;
    }
  }
  if (primaryFailure) throw primaryFailure;
}

if (WRITER_MODE) {
  await runCrashPrefixWriter(process.argv[3]);
} else {
  test("crash-prefix fixture has one valid prefix and one synthetic partial suffix", () => {
    assert.deepEqual(parseCompleteJsonLines(FULL_PREFIX), [sessionMeta(), event(1)]);
    assert.throws(() => parseCompleteJsonLines(
      Buffer.concat([FULL_PREFIX, PARTIAL_SUFFIX]),
    ));
    assertNoAbortMarker(Buffer.concat([FULL_PREFIX, PARTIAL_SUFFIX]));
    assert.equal(
      checkedTestRoot(
        "/var/tmp/portable-codex-runtime-linux-ext4-crash-prefix-local",
        "unused",
      ),
      "/var/tmp/portable-codex-runtime-linux-ext4-crash-prefix-local",
    );
    assert.throws(
      () => checkedTestRoot("/tmp/unscoped", "unused"),
      /unsafe Linux ext4 crash-prefix test root/u,
    );
  });

  test(
    "real Linux ext4 LVM crash-prefix artifact remains byte-stable while its copy is repaired",
    { skip: !CONFORMANCE_ENABLED },
    runConformance,
  );
}
