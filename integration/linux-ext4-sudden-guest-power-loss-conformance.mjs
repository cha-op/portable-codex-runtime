import assert from "node:assert/strict";
import { execFile, spawn } from "node:child_process";
import { createHash, randomBytes } from "node:crypto";
import { once } from "node:events";
import { constants as fsConstants, createReadStream, createWriteStream } from "node:fs";
import {
  access,
  lstat,
  mkdir,
  open,
  readFile,
  rm,
} from "node:fs/promises";
import { createConnection } from "node:net";
import { basename, dirname, isAbsolute, join, resolve } from "node:path";
import { createInterface } from "node:readline";
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
const FIXTURE_ROOT = join(
  dirname(CURRENT_FILE),
  "fixtures",
  "linux-ext4-sudden-guest-power-loss",
);
const GUEST_SOURCE = join(FIXTURE_ROOT, "guest.c");
const INITRAMFS_CONFIG = join(FIXTURE_ROOT, "initramfs-tools");
const CONFORMANCE_ENABLED =
  process.env.LINUX_EXT4_SUDDEN_GUEST_POWER_LOSS_CONFORMANCE === "1";
const TEST_ROOT_PREFIX =
  "portable-codex-runtime-linux-ext4-sudden-guest-power-loss-";
const CONTROL_ROOT_PREFIX = "pcrpl-qmp-";
const UNIX_SOCKET_PATH_MAX_BYTES = 107;
const ROOT_SESSION_ID = "019f2b00-0000-7000-8000-000000000001";
const RUNTIME_IDENTITY = Object.freeze({
  codexBinarySha256: "ac".repeat(32),
  codexVersion: "codex-cli 0.144.1",
  sourceAnalysisCommit: "db887d03e1f907467e33271572dffb73bceecd6b",
});
const SESSION_LINE = Buffer.from(
  '{"timestamp":"2026-08-28T00:00:00.000Z","type":"session_meta","payload":{"cli_version":"0.144.1","cwd":"/workspace","id":"019f2b00-0000-7000-8000-000000000001","originator":"linux-ext4-sudden-guest-power-loss-conformance","session_id":"019f2b00-0000-7000-8000-000000000001","timestamp":"2026-08-28T00:00:00.000Z"}}\n',
  "utf8",
);
const EVENT_START = Buffer.from(
  '{"type":"event_msg","payload":{"padding":"',
  "utf8",
);
const EVENT_END = Buffer.from('","sequence":1}}\n', "utf8");
const PREFIX_SIZE = 4096;
const PADDING_SIZE =
  PREFIX_SIZE - SESSION_LINE.length - EVENT_START.length - EVENT_END.length;
assert.equal(PADDING_SIZE > 0, true);
const FULL_PREFIX = Buffer.concat([
  SESSION_LINE,
  EVENT_START,
  Buffer.alloc(PADDING_SIZE, 0x78),
  EVENT_END,
]);
assert.equal(FULL_PREFIX.length, PREFIX_SIZE);
assert.equal(FULL_PREFIX.at(-1), 0x0a);
const PARTIAL_SUFFIX = Buffer.from(
  '{"type":"event_msg","payload":{"sequence":2',
  "utf8",
);
const CONTINUATION = Buffer.from(
  '{"type":"event_msg","payload":{"sequence":3}}\n',
  "utf8",
);
const UTF8_DECODER = new TextDecoder("utf-8", { fatal: true });
const DATA_IMAGE_SIZE = 128 * 1024 * 1024;
const COMMAND_TIMEOUT_MILLISECONDS = 180_000;
const QEMU_BOOT_TIMEOUT_MILLISECONDS = 120_000;
const QEMU_OUTPUT_LIMIT = 256 * 1024;
const QEMU_DIAGNOSTIC_TAIL_LIMIT = 8 * 1024;
const QEMU_LINE_BYTE_LIMIT = 8 * 1024;
const QEMU_LINE_HISTORY_LIMIT = 256;
const QEMU_PROTOCOL_HISTORY_LIMIT = 32;
const QEMU_LIVENESS_PROBE_MILLISECONDS = 100;
const QEMU_GUEST_ERROR_PATTERN =
  /^PCR_SUDDEN_GUEST_POWER_ERROR_V1 (?:unknown|[0-9A-Fa-f]{32}) [a-z0-9_]+ status=[0-9]+$/u;
const QEMU_PROTOCOL_LINE_PATTERN =
  /^PCR_SUDDEN_GUEST_POWER_(?:ERROR_V1|READY_V1|RECOVER_OK_V1|SETUP_OK_V1)(?: |$)/u;
const COMMANDS = Object.freeze({
  cc: "/usr/bin/cc",
  findmnt: "/usr/bin/findmnt",
  getfacl: "/usr/bin/getfacl",
  lsinitramfs: "/usr/bin/lsinitramfs",
  losetup: "/usr/sbin/losetup",
  mkfsExt4: "/usr/sbin/mkfs.ext4",
  mkinitramfs: "/usr/sbin/mkinitramfs",
  mount: "/usr/bin/mount",
  qemu: "/usr/bin/qemu-system-x86_64",
  tune2fs: "/usr/sbin/tune2fs",
  udevadm: "/usr/bin/udevadm",
  umount: "/usr/bin/umount",
});

function sessionMeta() {
  return {
    timestamp: "2026-08-28T00:00:00.000Z",
    type: "session_meta",
    payload: {
      cli_version: "0.144.1",
      cwd: "/workspace",
      id: ROOT_SESSION_ID,
      originator: "linux-ext4-sudden-guest-power-loss-conformance",
      session_id: ROOT_SESSION_ID,
      timestamp: "2026-08-28T00:00:00.000Z",
    },
  };
}

function firstEvent() {
  return {
    type: "event_msg",
    payload: { padding: "x".repeat(PADDING_SIZE), sequence: 1 },
  };
}

function continuationEvent() {
  return { type: "event_msg", payload: { sequence: 3 } };
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
    throw new TypeError("unsafe sudden guest power-loss test root");
  }
  return candidate;
}

function checkedControlRoot(value, suffix) {
  const candidate = value ?? join(
    "/var/tmp",
    `${CONTROL_ROOT_PREFIX}${process.pid}-${suffix}`,
  );
  if (
    typeof candidate !== "string" ||
    !isAbsolute(candidate) ||
    candidate !== resolve(candidate) ||
    dirname(candidate) !== "/var/tmp" ||
    !basename(candidate).startsWith(CONTROL_ROOT_PREFIX) ||
    !/^[-A-Za-z0-9._]+$/u.test(basename(candidate)) ||
    Buffer.byteLength(join(candidate, "r.sock")) > UNIX_SOCKET_PATH_MAX_BYTES
  ) {
    throw new TypeError("unsafe sudden guest power-loss control root");
  }
  return candidate;
}

function checkedAbsolutePath(value, label) {
  if (
    typeof value !== "string" ||
    !isAbsolute(value) ||
    value !== resolve(value)
  ) {
    throw new TypeError(`invalid ${label}`);
  }
  return value;
}

function checkedUnixSocketPath(value, label) {
  const candidate = checkedAbsolutePath(value, label);
  if (Buffer.byteLength(candidate) > UNIX_SOCKET_PATH_MAX_BYTES) {
    throw new TypeError(`${label} exceeds the Linux Unix-socket path limit`);
  }
  return candidate;
}

async function runCommand(executable, arguments_, options = {}) {
  const { env = {}, ...rest } = options;
  return execFileAsync(executable, arguments_, {
    encoding: "utf8",
    env: { ...process.env, LANG: "C", LC_ALL: "C", ...env },
    maxBuffer: 2 * 1024 * 1024,
    timeout: COMMAND_TIMEOUT_MILLISECONDS,
    ...rest,
  });
}

async function assertPrerequisites(kernelPath, kernelRelease) {
  assert.equal(process.platform, "linux");
  assert.equal(process.geteuid?.(), 0);
  for (const executable of Object.values(COMMANDS)) {
    await access(executable, fsConstants.X_OK);
  }
  await access(kernelPath, fsConstants.R_OK);
  await access(`/lib/modules/${kernelRelease}`, fsConstants.R_OK);
  await access(GUEST_SOURCE, fsConstants.R_OK);
  await access(INITRAMFS_CONFIG, fsConstants.R_OK);
  assert.equal(basename(kernelPath), `vmlinuz-${kernelRelease}`);
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

function parseCompleteJsonLines(bytes) {
  assert.equal(Buffer.isBuffer(bytes), true);
  assert.equal(bytes.length > 0, true);
  assert.equal(bytes.at(-1), 0x0a);
  const lines = UTF8_DECODER.decode(bytes).split("\n");
  assert.equal(lines.pop(), "");
  return lines.map((line) => {
    assert.notEqual(line, "");
    return JSON.parse(line);
  });
}

function assertNoAbortMarker(bytes) {
  assert.equal(bytes.includes(Buffer.from("<turn_aborted>", "utf8")), false);
  assert.equal(bytes.includes(Buffer.from("TurnAborted", "utf8")), false);
}

function assertAdmittedRecoveredCrashPrefix(bytes) {
  assert.equal(bytes.length >= FULL_PREFIX.length, true);
  assert.equal(
    bytes.length <= FULL_PREFIX.length + PARTIAL_SUFFIX.length,
    true,
  );
  assert.deepEqual(bytes.subarray(0, FULL_PREFIX.length), FULL_PREFIX);
  const tail = bytes.subarray(FULL_PREFIX.length);
  assert.equal(tail.includes(0x0a), false);
  assertNoAbortMarker(tail);
  // The guest protects P and bounds the observed tail. This host-side
  // admission deliberately excludes a complete no-LF JSON value so the
  // evidence covers only unchanged or truncate_partial_tail convergence.
  if (tail.length > 0) {
    let parsed = false;
    try {
      JSON.parse(UTF8_DECODER.decode(tail));
      parsed = true;
    } catch {
      // A crash tail is expected to be incomplete or otherwise invalid JSON.
    }
    assert.equal(parsed, false, "unsynced tail formed a complete JSON value");
  }
  return tail;
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

function regularFileEvidence(metadata) {
  assert.equal(metadata.isFile(), true);
  // dev+ino protect the named raw-file object, mode+nlink protect the scoped
  // access/alias policy, and size protects the fixed virtual-medium boundary.
  // mtime/ctime are intentionally excluded because QEMU writes and ext4
  // journal replay are benign metadata transitions for those properties.
  return Object.freeze({
    accessPolicy: Object.freeze({
      mode: metadata.mode & 0o7777n,
      nlink: metadata.nlink,
    }),
    contentBoundary: Object.freeze({ size: metadata.size }),
    objectIdentity: Object.freeze({ dev: metadata.dev, ino: metadata.ino }),
  });
}

async function fullCopy(source, destination) {
  await pipeline(
    createReadStream(source, { highWaterMark: 1024 * 1024 }),
    createWriteStream(destination, { flags: "wx", mode: 0o600 }),
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
  await runCommand(COMMANDS.mount, [
    "--types",
    "ext4",
    "--options",
    readOnly ? "ro,noload,nosuid,nodev,noexec" : "rw,nosuid,nodev,noexec",
    device,
    mountPath,
  ]);
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

function qemuArguments({
  dataPath,
  initramfsPath,
  kernelPath,
  marker,
  mode,
  nonce,
  pidfilePath,
  qmpPath,
}) {
  for (const [value, label] of [
    [dataPath, "QEMU data path"],
    [initramfsPath, "QEMU initramfs path"],
    [kernelPath, "QEMU kernel path"],
    [pidfilePath, "QEMU pidfile path"],
  ]) {
    checkedAbsolutePath(value, label);
  }
  checkedUnixSocketPath(qmpPath, "QEMU QMP path");
  assert.match(marker, /^pcrpl_[0-9]+_[0-9a-f]{8}$/u);
  assert.match(nonce, /^[0-9a-f]{32}$/u);
  assert.match(mode, /^(?:setup|armed|recover)$/u);
  const processName = `pcrpl_${marker.slice(-8)}`;
  return [
    "-nodefaults",
    "-machine",
    "q35",
    "-accel",
    "tcg,thread=multi",
    "-m",
    "256M",
    "-smp",
    "1",
    "-nic",
    "none",
    "-display",
    "none",
    "-monitor",
    "none",
    "-serial",
    "stdio",
    "-no-reboot",
    "-name",
    `guest=${marker},process=${processName}`,
    "-pidfile",
    pidfilePath,
    "-qmp",
    `unix:${qmpPath},server=on,wait=off`,
    "-kernel",
    kernelPath,
    "-initrd",
    initramfsPath,
    "-append",
    `console=ttyS0 loglevel=3 panic=-1 root=/dev/vda rootfstype=ext4 rootwait pcr_mode=${mode} pcr_nonce=${nonce}`,
    "-drive",
    `file=${dataPath},format=raw,if=none,id=pcrdata,cache=none,aio=threads,discard=ignore,detect-zeroes=off,snapshot=off`,
    "-device",
    "virtio-blk-pci,drive=pcrdata,serial=pcr-power-loss-v1,write-cache=on",
  ];
}

function boundedAppend(current, chunk, state) {
  state.bytes += Buffer.byteLength(chunk);
  if (state.bytes > QEMU_OUTPUT_LIMIT) state.overflow = true;
  return boundedUtf8Tail(`${current}${chunk}`, QEMU_OUTPUT_LIMIT);
}

function boundedUtf8Tail(value, maximumBytes) {
  const bytes = Buffer.from(value, "utf8");
  if (bytes.length <= maximumBytes) return value;
  const tail = bytes.subarray(bytes.length - maximumBytes);
  let offset = 0;
  while (offset < tail.length && (tail[offset] & 0xc0) === 0x80) {
    offset += 1;
  }
  return tail.subarray(offset).toString("utf8");
}

function qemuDiagnosticSummary(controller) {
  return [
    `guestErrorCount=${controller.guestErrorCount}`,
    `guestErrors=${JSON.stringify(controller.guestErrors)}`,
    `stdoutTail=${JSON.stringify(boundedUtf8Tail(
      controller.stdout,
      QEMU_DIAGNOSTIC_TAIL_LIMIT,
    ))}`,
    `stderrTail=${JSON.stringify(boundedUtf8Tail(
      controller.stderr,
      QEMU_DIAGNOSTIC_TAIL_LIMIT,
    ))}`,
    `outputOverflow=${controller.outputState.overflow}`,
    `protocolOverflow=${controller.outputState.protocolOverflow}`,
  ].join(" ");
}

function pushBounded(collection, value, limit) {
  if (collection.length === limit) collection.shift();
  collection.push(value);
}

function requestQemuOutputAbort(controller) {
  if (controller.outputState.abortRequested) return;
  controller.outputState.abortRequested = true;
  controller.child.kill("SIGKILL");
}

function recordQemuLine(controller, line) {
  const normalized = line.endsWith("\r") ? line.slice(0, -1) : line;
  pushBounded(controller.lines, normalized, QEMU_LINE_HISTORY_LIMIT);
  if (QEMU_PROTOCOL_LINE_PATTERN.test(normalized)) {
    controller.protocolLineCount += 1;
    if (controller.protocolLineCount > QEMU_PROTOCOL_HISTORY_LIMIT) {
      controller.outputState.protocolOverflow = true;
      requestQemuOutputAbort(controller);
      return;
    }
    controller.protocolLines.push(normalized);
  }
  if (QEMU_GUEST_ERROR_PATTERN.test(normalized)) {
    controller.guestErrorCount += 1;
    pushBounded(controller.guestErrors, normalized, 4);
  }
}

function consumeQemuStdout(controller, chunk) {
  let cursor = 0;
  while (cursor < chunk.length) {
    const newline = chunk.indexOf("\n", cursor);
    const end = newline === -1 ? chunk.length : newline;
    if (!controller.discardingLongLine) {
      const candidate = controller.partialLine + chunk.slice(cursor, end);
      if (Buffer.byteLength(candidate) > QEMU_LINE_BYTE_LIMIT) {
        controller.partialLine = "";
        controller.discardingLongLine = true;
      } else {
        controller.partialLine = candidate;
      }
    }
    if (newline === -1) return;
    if (!controller.discardingLongLine) {
      recordQemuLine(controller, controller.partialLine);
    }
    controller.discardingLongLine = false;
    controller.partialLine = "";
    cursor = newline + 1;
  }
}

function finishQemuStdout(controller) {
  if (!controller.discardingLongLine && controller.partialLine !== "") {
    recordQemuLine(controller, controller.partialLine);
  }
  controller.discardingLongLine = false;
  controller.partialLine = "";
}

function recordQemuChunk(controller, stream, chunk) {
  controller[stream] = boundedAppend(
    controller[stream],
    chunk,
    controller.outputState,
  );
  if (controller.outputState.overflow) {
    requestQemuOutputAbort(controller);
    return;
  }
  if (stream === "stdout") consumeQemuStdout(controller, chunk);
}

function findQemuLine(controller, predicate) {
  return controller.protocolLines.find(predicate) ?? controller.lines.find(predicate);
}

function assertQemuOutputHealthy(controller, label) {
  if (controller.guestErrorCount > 0) {
    throw new Error(
      `${label} reported guest failure: ${qemuDiagnosticSummary(controller)}`,
    );
  }
  if (controller.outputState.overflow) {
    throw new Error(
      `${label} exceeded the bounded QEMU output limit: ${qemuDiagnosticSummary(controller)}`,
    );
  }
  if (controller.outputState.protocolOverflow) {
    throw new Error(
      `${label} exceeded the bounded QEMU protocol history: ${qemuDiagnosticSummary(controller)}`,
    );
  }
}

function startQemu(arguments_) {
  const child = spawn(COMMANDS.qemu, arguments_, {
    stdio: ["ignore", "pipe", "pipe"],
  });
  const controller = {
    arguments_,
    child,
    discardingLongLine: false,
    guestErrorCount: 0,
    guestErrors: [],
    lines: [],
    outputState: {
      abortRequested: false,
      bytes: 0,
      overflow: false,
      protocolOverflow: false,
    },
    partialLine: "",
    protocolLineCount: 0,
    protocolLines: [],
    spawnError: null,
    stderr: "",
    stdout: "",
  };
  child.stdout.setEncoding("utf8");
  child.stderr.setEncoding("utf8");
  child.stdout.on("data", (chunk) => {
    recordQemuChunk(controller, "stdout", chunk);
  });
  child.stdout.on("end", () => finishQemuStdout(controller));
  child.stderr.on("data", (chunk) => {
    recordQemuChunk(controller, "stderr", chunk);
  });
  controller.exit = new Promise((resolvePromise) => {
    child.once("error", (error) => {
      controller.spawnError = error;
      resolvePromise({ error });
    });
    child.once("exit", (code, signal) => resolvePromise({ code, signal }));
  });
  controller.closed = new Promise((resolvePromise) => {
    child.once("close", (code, signal) => resolvePromise({ code, signal }));
  });
  return controller;
}

async function withDeadline(promise, milliseconds, label) {
  let timer;
  const timeout = new Promise((_, rejectPromise) => {
    timer = setTimeout(() => rejectPromise(new Error(`${label} timed out`)), milliseconds);
    timer.unref();
  });
  try {
    return await Promise.race([promise, timeout]);
  } finally {
    clearTimeout(timer);
  }
}

async function waitForQemuLine(controller, predicate, label) {
  const deadline = Date.now() + QEMU_BOOT_TIMEOUT_MILLISECONDS;
  while (Date.now() < deadline) {
    assertQemuOutputHealthy(controller, label);
    const line = findQemuLine(controller, predicate);
    if (line !== undefined) return line;
    if (controller.spawnError !== null) throw controller.spawnError;
    if (
      controller.child.exitCode !== null ||
      controller.child.signalCode !== null
    ) {
      await withDeadline(controller.closed, 10_000, `${label} stdio close`);
      assertQemuOutputHealthy(controller, label);
      const finalLine = findQemuLine(controller, predicate);
      if (finalLine !== undefined) return finalLine;
      throw new Error(
        `${label} exited before marker: code=${controller.child.exitCode} signal=${controller.child.signalCode} ${qemuDiagnosticSummary(controller)}`,
      );
    }
    await delay(20, undefined, { ref: false });
  }
  throw new Error(`${label} timed out: ${qemuDiagnosticSummary(controller)}`);
}

async function assertPidfile(controller, pidfilePath) {
  let value;
  for (let attempt = 0; attempt < 100; attempt += 1) {
    assertQemuOutputHealthy(controller, "QEMU pidfile wait");
    try {
      value = await readFile(pidfilePath, "utf8");
      break;
    } catch (error) {
      if (error?.code !== "ENOENT") throw error;
      await delay(20, undefined, { ref: false });
    }
  }
  assertQemuOutputHealthy(controller, "QEMU pidfile wait");
  assert.equal(value?.trim(), String(controller.child.pid));
}

async function nextQmpValue(controller, iterator, label) {
  const result = await withDeadline(
    iterator.next(),
    10_000,
    `QMP ${label}`,
  );
  assertQemuOutputHealthy(controller, `QMP ${label}`);
  assert.equal(result.done, false);
  return JSON.parse(result.value);
}

async function queryBlockCache(controller, qmpPath) {
  let socket;
  for (let attempt = 0; attempt < 100; attempt += 1) {
    assertQemuOutputHealthy(controller, "QMP connect");
    try {
      socket = createConnection({ path: qmpPath });
      await withDeadline(once(socket, "connect"), 1_000, "QMP connect");
      break;
    } catch (error) {
      socket?.destroy();
      socket = undefined;
      if (error?.code !== "ENOENT" && error?.code !== "ECONNREFUSED") {
        throw error;
      }
      await delay(20, undefined, { ref: false });
    }
  }
  assert.notEqual(socket, undefined, "QMP socket did not become available");
  socket.setEncoding("utf8");
  const reader = createInterface({ input: socket, crlfDelay: Infinity });
  const iterator = reader[Symbol.asyncIterator]();
  try {
    const greeting = await nextQmpValue(controller, iterator, "greeting");
    assert.equal(typeof greeting.QMP?.version?.qemu?.major, "number");
    socket.write(`${JSON.stringify({ execute: "qmp_capabilities", id: "caps" })}\r\n`);
    let capabilities;
    while (capabilities === undefined) {
      const message = await nextQmpValue(controller, iterator, "capabilities");
      if (message.id === "caps") capabilities = message;
    }
    assert.deepEqual(capabilities, { id: "caps", return: {} });
    socket.write(`${JSON.stringify({ execute: "query-block", id: "block" })}\r\n`);
    let response;
    while (response === undefined) {
      const message = await nextQmpValue(controller, iterator, "query-block");
      if (message.id === "block") response = message;
    }
    assert.equal(Array.isArray(response.return), true);
    const block = response.return.find((entry) => entry.device === "pcrdata");
    assert.notEqual(block, undefined);
    assert.deepEqual(block.inserted?.cache, {
      direct: true,
      "no-flush": false,
      writeback: true,
    });
    return Object.freeze({
      direct: block.inserted.cache.direct,
      noFlush: block.inserted.cache["no-flush"],
      writeback: block.inserted.cache.writeback,
    });
  } finally {
    reader.close();
    socket.destroy();
  }
}

async function waitForQemuExit(controller, label) {
  const { closed, exited } = await observeQemuTerminal(controller, label);
  if (exited.error) throw exited.error;
  assert.deepEqual(closed, { code: exited.code, signal: exited.signal });
  assertQemuOutputHealthy(controller, label);
  return { code: exited.code, signal: exited.signal };
}

async function observeQemuTerminal(controller, label) {
  const exited = await withDeadline(
    controller.exit,
    QEMU_BOOT_TIMEOUT_MILLISECONDS,
    `${label} exit`,
  );
  const closed = await withDeadline(
    controller.closed,
    10_000,
    `${label} stdio close`,
  );
  return Object.freeze({ closed, exited });
}

function markerCount(controller, predicate) {
  return controller.protocolLines.filter(predicate).length;
}

async function runCleanBoot(resources, configuration) {
  const arguments_ = qemuArguments(configuration);
  const controller = startQemu(arguments_);
  resources.qemu = controller;
  await assertPidfile(controller, configuration.pidfilePath);
  const expected = configuration.mode === "setup"
    ? `PCR_SUDDEN_GUEST_POWER_SETUP_OK_V1 ${configuration.nonce}`
    : null;
  let marker;
  if (expected !== null) {
    marker = await waitForQemuLine(
      controller,
      (line) => line === expected,
      `${configuration.mode} boot`,
    );
  } else {
    const pattern = new RegExp(
      `^PCR_SUDDEN_GUEST_POWER_RECOVER_OK_V1 ${configuration.nonce} ([0-9]+)$`,
      "u",
    );
    marker = await waitForQemuLine(
      controller,
      (line) => pattern.test(line),
      `${configuration.mode} boot`,
    );
  }
  const exit = await waitForQemuExit(controller, `${configuration.mode} boot`);
  assert.deepEqual(exit, { code: 0, signal: null });
  assert.equal(
    markerCount(controller, (line) => line === marker),
    1,
  );
  resources.qemu = null;
  return marker;
}

async function runArmedBoot(resources, configuration) {
  const arguments_ = qemuArguments(configuration);
  const controller = startQemu(arguments_);
  resources.qemu = controller;
  await assertPidfile(controller, configuration.pidfilePath);
  const qmpCache = await queryBlockCache(controller, configuration.qmpPath);
  const expected = `PCR_SUDDEN_GUEST_POWER_READY_V1 ${configuration.nonce}`;
  await waitForQemuLine(
    controller,
    (line) => line === expected,
    "armed boot",
  );
  await delay(QEMU_LIVENESS_PROBE_MILLISECONDS, undefined, { ref: false });
  assertQemuOutputHealthy(controller, "armed boot liveness probe");
  assert.equal(controller.child.exitCode, null);
  assert.equal(controller.child.signalCode, null);
  process.kill(controller.child.pid, 0);
  assert.equal(controller.child.kill("SIGKILL"), true);
  const exit = await waitForQemuExit(controller, "armed boot SIGKILL");
  assert.deepEqual(exit, { code: null, signal: "SIGKILL" });
  assert.equal(
    markerCount(controller, (line) => line === expected),
    1,
  );
  resources.qemu = null;
  return Object.freeze({ exit, qmpCache, qemuPid: controller.child.pid });
}

function parseFilesystemRecoveryEvidence(stdout) {
  const stateMatch = stdout.match(/^Filesystem state:\s+(.+)$/mu);
  const featuresMatch = stdout.match(/^Filesystem features:\s+(.+)$/mu);
  assert.notEqual(stateMatch, null);
  assert.notEqual(featuresMatch, null);
  const state = stateMatch[1].trim();
  assert.doesNotMatch(state, /error/iu);
  const features = featuresMatch[1].trim().split(/\s+/u);
  return Object.freeze({
    needsRecovery: features.includes("needs_recovery"),
    state,
  });
}

async function filesystemRecoveryEvidence(imagePath) {
  const { stdout } = await runCommand(COMMANDS.tune2fs, ["-l", imagePath]);
  return parseFilesystemRecoveryEvidence(stdout);
}

async function compileGuestAgent(destination) {
  await runCommand(COMMANDS.cc, [
    "-O2",
    "-std=c11",
    "-Wall",
    "-Wextra",
    "-Werror",
    "-static",
    GUEST_SOURCE,
    "-o",
    destination,
  ]);
  await access(destination, fsConstants.X_OK);
}

function assertInitramfsBootCapability(
  entries,
  modulesBuiltin,
  kernelRelease,
  moduleName,
) {
  const moduleFilenames = new Set([
    `${moduleName}.ko`,
    `${moduleName}.ko.gz`,
    `${moduleName}.ko.xz`,
    `${moduleName}.ko.zst`,
  ]);
  if (entries.some((entry) => moduleFilenames.has(basename(entry)))) {
    return "module";
  }
  const isBuiltin = modulesBuiltin
    .split("\n")
    .map((entry) => entry.trim())
    .some((entry) => basename(entry) === `${moduleName}.ko`);
  assert.equal(
    isBuiltin,
    true,
    `kernel/initramfs is missing ${moduleName}`,
  );
  assert.equal(
    [
      `lib/modules/${kernelRelease}/modules.builtin`,
      `usr/lib/modules/${kernelRelease}/modules.builtin`,
    ].some((entry) => entries.includes(entry)),
    true,
    `initramfs is missing modules.builtin for ${moduleName}`,
  );
  return "builtin";
}

async function buildInitramfs(destination, guestAgent, kernelRelease) {
  await runCommand(COMMANDS.mkinitramfs, [
    "-d",
    INITRAMFS_CONFIG,
    "-o",
    destination,
    kernelRelease,
  ], {
    env: { PCR_SUDDEN_GUEST_POWER_AGENT: guestAgent },
  });
  await access(destination, fsConstants.R_OK);
  const [{ stdout }, modulesBuiltin] = await Promise.all([
    runCommand(COMMANDS.lsinitramfs, [destination]),
    readFile(`/lib/modules/${kernelRelease}/modules.builtin`, "utf8"),
  ]);
  const entries = stdout
    .split("\n")
    .map((entry) => entry.trim().replace(/^\.\//u, ""))
    .filter((entry) => entry.length > 0);
  for (const requiredPath of [
    "usr/libexec/pcr-power-loss-agent",
    "scripts/init-premount/pcr-power-loss",
  ]) {
    assert.equal(
      entries.includes(requiredPath),
      true,
      `initramfs is missing ${requiredPath}`,
    );
  }
  for (const moduleName of ["ext4", "virtio_blk", "virtio_pci"]) {
    assertInitramfsBootCapability(
      entries,
      modulesBuiltin,
      kernelRelease,
      moduleName,
    );
  }
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
  if (resources.qemu !== null) {
    await attempt("QEMU process", async () => {
      const controller = resources.qemu;
      if (
        controller.child.exitCode === null &&
        controller.child.signalCode === null
      ) {
        controller.child.kill("SIGKILL");
      }
      // Cleanup owns retirement, not conformance. Observing close proves the
      // spawned child is terminal (or spawn failed) and all stdio is closed;
      // exit-shape and bounded-output assertions remain on the primary path.
      await observeQemuTerminal(controller, "cleanup QEMU");
      resources.qemu = null;
    });
  }
  if (resources.repairMounted) {
    await attempt("repair mount", async () => {
      await runCommand(COMMANDS.umount, ["--", resources.repairMount]);
      resources.repairMounted = false;
    });
  }
  if (resources.repairLoop !== null) {
    await attempt("repair loop", async () => {
      await detachLoop(resources.repairLoop);
      resources.repairLoop = null;
    });
  }
  const allResourcesRetired =
    resources.qemu === null &&
    !resources.repairMounted &&
    resources.repairLoop === null;
  if (resources.controlRootCreated && allResourcesRetired) {
    await attempt("control root", async () => {
      await rm(resources.controlRoot, { recursive: true });
      resources.controlRootCreated = false;
    });
  } else if (resources.controlRootCreated) {
    failures.push(new Error(
      "cleanup withheld control-root removal because QEMU authority remains",
    ));
  }
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
  const kernelPath = checkedAbsolutePath(
    process.env.LINUX_EXT4_SUDDEN_GUEST_POWER_LOSS_KERNEL,
    "guest kernel path",
  );
  const kernelRelease =
    process.env.LINUX_EXT4_SUDDEN_GUEST_POWER_LOSS_KERNEL_RELEASE;
  assert.match(kernelRelease, /^[A-Za-z0-9._+-]+$/u);
  await assertPrerequisites(kernelPath, kernelRelease);
  const suffix = randomBytes(4).toString("hex");
  const nonce = randomBytes(16).toString("hex");
  const root = checkedTestRoot(
    process.env.LINUX_EXT4_SUDDEN_GUEST_POWER_LOSS_TEST_ROOT,
    suffix,
  );
  const controlRoot = checkedControlRoot(
    process.env.LINUX_EXT4_SUDDEN_GUEST_POWER_LOSS_CONTROL_ROOT,
    suffix,
  );
  const marker = `pcrpl_${process.pid}_${suffix}`;
  const dataPath = join(root, "data.raw");
  const artifactPath = join(root, "artifact.raw");
  const initramfsPath = join(root, "pcr-initramfs.img");
  const guestAgentPath = join(root, "pcr-power-loss-guest");
  const rolloutRelativePath = join(
    "codex-home",
    "sessions",
    "2026",
    "08",
    "28",
    "sudden-power-loss.jsonl",
  );
  const resources = {
    controlRoot,
    controlRootCreated: false,
    qemu: null,
    repairLoop: null,
    repairMount: join(root, "repair-mount"),
    repairMounted: false,
    root,
    rootCreated: false,
  };
  let primaryFailure;
  try {
    await mkdir(root, { mode: 0o700 });
    resources.rootCreated = true;
    await mkdir(controlRoot, { mode: 0o700 });
    resources.controlRootCreated = true;
    await mkdir(resources.repairMount, { mode: 0o700 });
    await compileGuestAgent(guestAgentPath);
    await buildInitramfs(initramfsPath, guestAgentPath, kernelRelease);
    const { stdout: qemuVersionOutput } = await runCommand(
      COMMANDS.qemu,
      ["--version"],
    );
    const runtimeEvidence = Object.freeze({
      guestAgent: await digestPath(guestAgentPath),
      initramfs: await digestPath(initramfsPath),
      kernel: await digestPath(kernelPath),
      kernelRelease,
      qemuVersion: qemuVersionOutput.trim().split("\n")[0],
    });

    const data = await open(dataPath, "wx", 0o600);
    try {
      await data.truncate(DATA_IMAGE_SIZE);
      await data.sync();
    } finally {
      await data.close();
    }
    await syncDirectory(root);
    await runCommand(COMMANDS.mkfsExt4, [
      "-F",
      "-q",
      "-b",
      "4096",
      "-E",
      "lazy_itable_init=0,lazy_journal_init=0",
      dataPath,
    ]);
    const formatted = await open(dataPath, "r+");
    try {
      await formatted.sync();
    } finally {
      await formatted.close();
    }
    await syncDirectory(root);
    const dataMetadata = await lstat(dataPath, { bigint: true });
    const dataEvidence = regularFileEvidence(dataMetadata);
    assert.deepEqual(dataEvidence, {
      accessPolicy: { mode: 0o600n, nlink: 1n },
      contentBoundary: { size: BigInt(DATA_IMAGE_SIZE) },
      objectIdentity: { dev: dataMetadata.dev, ino: dataMetadata.ino },
    });
    const formattedFilesystem = await filesystemRecoveryEvidence(dataPath);
    assert.equal(formattedFilesystem.needsRecovery, false);

    const configuration = (mode) => ({
      dataPath,
      initramfsPath,
      kernelPath,
      marker,
      mode,
      nonce,
      pidfilePath: join(root, `qemu-${mode}.pid`),
      qmpPath: join(controlRoot, `${mode[0]}.sock`),
    });
    await runCleanBoot(resources, configuration("setup"));
    assert.deepEqual(
      regularFileEvidence(await lstat(dataPath, { bigint: true })),
      dataEvidence,
    );
    const setupFilesystem = await filesystemRecoveryEvidence(dataPath);
    assert.equal(setupFilesystem.needsRecovery, false);

    const armed = await runArmedBoot(resources, configuration("armed"));
    assert.deepEqual(
      regularFileEvidence(await lstat(dataPath, { bigint: true })),
      dataEvidence,
    );
    const crashedFilesystem = await filesystemRecoveryEvidence(dataPath);
    assert.equal(crashedFilesystem.needsRecovery, true);

    await fullCopy(dataPath, artifactPath);
    const artifactHandle = await open(
      artifactPath,
      fsConstants.O_RDWR | fsConstants.O_NOFOLLOW,
    );
    try {
      await artifactHandle.chmod(0o400);
      await artifactHandle.sync();
    } finally {
      await artifactHandle.close();
    }
    await syncDirectory(root);
    const artifactMetadata = await lstat(artifactPath, { bigint: true });
    const artifactEvidence = regularFileEvidence(artifactMetadata);
    assert.deepEqual(artifactEvidence, {
      accessPolicy: { mode: 0o400n, nlink: 1n },
      contentBoundary: { size: BigInt(DATA_IMAGE_SIZE) },
      objectIdentity: {
        dev: artifactMetadata.dev,
        ino: artifactMetadata.ino,
      },
    });
    assert.equal(artifactMetadata.dev, dataMetadata.dev);
    assert.notEqual(artifactMetadata.ino, dataMetadata.ino);
    const artifactBefore = await digestPath(artifactPath);
    assert.deepEqual(await digestPath(dataPath), artifactBefore);

    const recoverMarker = await runCleanBoot(
      resources,
      configuration("recover"),
    );
    const recoverMatch = recoverMarker.match(/ ([0-9]+)$/u);
    assert.notEqual(recoverMatch, null);
    const guestTailBytes = Number(recoverMatch[1]);
    assert.equal(Number.isSafeInteger(guestTailBytes), true);
    assert.equal(guestTailBytes >= 0, true);
    assert.equal(guestTailBytes <= PARTIAL_SUFFIX.length, true);
    assert.deepEqual(
      regularFileEvidence(await lstat(dataPath, { bigint: true })),
      dataEvidence,
    );
    const recoveredFilesystem = await filesystemRecoveryEvidence(dataPath);
    assert.equal(recoveredFilesystem.needsRecovery, false);

    resources.repairLoop = await attachLoop(dataPath);
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
    const crashTail = assertAdmittedRecoveredCrashPrefix(preRepairRollout);
    assert.equal(crashTail.length, guestTailBytes);
    const repairRequest = {
      codexHome: repairCodexHome,
      rootSessionId: ROOT_SESSION_ID,
      runtimeIdentity: RUNTIME_IDENTITY,
    };
    const proof = await repairStoppedRolloutTails(repairRequest);
    assert.deepEqual(proof, {
      compatibility: RUNTIME_IDENTITY,
      files: [{
        action: crashTail.length === 0 ? "unchanged" : "truncate_partial_tail",
        after: contentProof(FULL_PREFIX),
        before: contentProof(preRepairRollout),
        relativePath: "2026/08/28/sudden-power-loss.jsonl",
        removedBytes: crashTail.length,
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
        relativePath: "2026/08/28/sudden-power-loss.jsonl",
        removedBytes: 0,
      }],
      rootSessionId: ROOT_SESSION_ID,
    });
    await appendContinuation(repairRolloutPath);
    const continued = await readFile(repairRolloutPath);
    assertNoAbortMarker(continued);
    assert.deepEqual(parseCompleteJsonLines(continued), [
      sessionMeta(),
      firstEvent(),
      continuationEvent(),
    ]);
    await runCommand(COMMANDS.umount, ["--", resources.repairMount]);
    resources.repairMounted = false;
    await detachLoop(resources.repairLoop);
    resources.repairLoop = null;

    resources.repairLoop = await attachLoop(dataPath, true);
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
      firstEvent(),
      continuationEvent(),
    ]);
    assert.deepEqual(
      regularFileEvidence(await lstat(dataPath, { bigint: true })),
      dataEvidence,
    );

    const artifactAfter = await digestPath(artifactPath);
    const artifactMetadataAfter = await lstat(artifactPath, { bigint: true });
    assert.deepEqual(
      regularFileEvidence(artifactMetadataAfter),
      artifactEvidence,
    );
    assert.deepEqual(artifactAfter, artifactBefore);
    t.diagnostic(JSON.stringify({
      artifact: artifactAfter,
      artifactFilesystemReadbackClaimed: false,
      artifactMode: "0400",
      controllerCachePowerLossClaimed: false,
      crashedFilesystem,
      evidenceClass: "external-qemu-sigkill-sudden-guest-power-loss",
      filesystemFreezeVerified: false,
      fuaVerified: false,
      guestFsyncBoundary: "4096-byte-prefix-and-rollout-directory-entry",
      guestPowerLossClaimed: true,
      guestWriteCache: "write back",
      hostPowerLossClaimed: false,
      qemuBlockCache: armed.qmpCache,
      qemuExit: armed.exit,
      qemuPid: armed.qemuPid,
      recoveredFilesystem,
      recoveryReadbackBoundary: "same-data-raw-guest-restart",
      recoveredUnsyncedTailBytes: crashTail.length,
      repairAction: proof.files[0].action,
      repairReplayAction: replay.files[0].action,
      runtime: runtimeEvidence,
      sameMediumIdentity: {
        dev: String(dataEvidence.objectIdentity.dev),
        ino: String(dataEvidence.objectIdentity.ino),
        mode: Number(dataEvidence.accessPolicy.mode),
        nlink: Number(dataEvidence.accessPolicy.nlink),
        size: Number(dataEvidence.contentBoundary.size),
      },
      unsyncedTailDurabilityClaimed: false,
      wholeFilesystemDurabilityClaimed: false,
      writableContinuationRecords: 3,
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
  assert.equal(resources.controlRootCreated, false);
  assert.equal(resources.rootCreated, false);
}

test("sudden guest power-loss prefix occupies one complete ext4 block", () => {
  assert.equal(FULL_PREFIX.length, 4096);
  assert.deepEqual(parseCompleteJsonLines(FULL_PREFIX), [
    sessionMeta(),
    firstEvent(),
  ]);
  assert.equal(PARTIAL_SUFFIX.includes(0x0a), false);
  assert.equal(PARTIAL_SUFFIX.length > 0, true);
  assert.equal(assertAdmittedRecoveredCrashPrefix(FULL_PREFIX).length, 0);
  const admitted = Buffer.concat([FULL_PREFIX, PARTIAL_SUFFIX]);
  assert.deepEqual(
    assertAdmittedRecoveredCrashPrefix(admitted),
    PARTIAL_SUFFIX,
  );
  assert.throws(
    () => assertAdmittedRecoveredCrashPrefix(
      Buffer.concat([FULL_PREFIX, Buffer.from("{}", "utf8")]),
    ),
    /complete JSON value/u,
  );
});

test("sudden guest power-loss test root is narrowly scoped", () => {
  assert.equal(
    checkedTestRoot(
      "/var/tmp/portable-codex-runtime-linux-ext4-sudden-guest-power-loss-local",
      "00000000",
    ),
    "/var/tmp/portable-codex-runtime-linux-ext4-sudden-guest-power-loss-local",
  );
  assert.throws(
    () => checkedTestRoot("/var/tmp/not-scoped", "00000000"),
    /unsafe sudden guest power-loss test root/u,
  );
  assert.equal(
    checkedControlRoot("/var/tmp/pcrpl-qmp-local", "00000000"),
    "/var/tmp/pcrpl-qmp-local",
  );
  assert.throws(
    () => checkedControlRoot("/var/tmp/not-scoped", "00000000"),
    /unsafe sudden guest power-loss control root/u,
  );
  assert.throws(
    () => checkedControlRoot(`/var/tmp/pcrpl-qmp-${"q".repeat(108)}`, "00000000"),
    /unsafe sudden guest power-loss control root/u,
  );
});

test("ext4 journal recovery evidence uses the needs_recovery feature", () => {
  assert.deepEqual(parseFilesystemRecoveryEvidence(
    "Filesystem state:         clean\n" +
      "Filesystem features:      has_journal ext_attr needs_recovery extent\n",
  ), {
    needsRecovery: true,
    state: "clean",
  });
  assert.deepEqual(parseFilesystemRecoveryEvidence(
    "Filesystem state:         clean\n" +
      "Filesystem features:      has_journal ext_attr extent\n",
  ), {
    needsRecovery: false,
    state: "clean",
  });
  assert.throws(
    () => parseFilesystemRecoveryEvidence(
      "Filesystem state:         clean with errors\n" +
        "Filesystem features:      has_journal needs_recovery\n",
    ),
    /match/u,
  );
});

test("initramfs boot capabilities accept loadable and built-in modules", () => {
  assert.equal(
    assertInitramfsBootCapability(
      ["usr/lib/modules/test/kernel/fs/ext4/ext4.ko.zst"],
      "",
      "test",
      "ext4",
    ),
    "module",
  );
  assert.equal(
    assertInitramfsBootCapability(
      ["usr/lib/modules/test/modules.builtin"],
      "kernel/fs/ext4/ext4.ko\n",
      "test",
      "ext4",
    ),
    "builtin",
  );
  assert.throws(
    () => assertInitramfsBootCapability([], "", "test", "ext4"),
    /kernel\/initramfs is missing ext4/u,
  );
  assert.throws(
    () => assertInitramfsBootCapability(
      [],
      "kernel/fs/ext4/ext4.ko\n",
      "test",
      "ext4",
    ),
    /initramfs is missing modules\.builtin for ext4/u,
  );
});

function qemuTestController({ exitCode = null, signalCode = null } = {}) {
  const controller = {
    child: {
      exitCode,
      kill: (signal) => {
        controller.killSignals.push(signal);
        return true;
      },
      signalCode,
    },
    discardingLongLine: false,
    guestErrorCount: 0,
    guestErrors: [],
    killSignals: [],
    lines: [],
    outputState: {
      abortRequested: false,
      bytes: 0,
      overflow: false,
      protocolOverflow: false,
    },
    partialLine: "",
    protocolLineCount: 0,
    protocolLines: [],
    spawnError: null,
    stderr: "",
    stdout: "",
  };
  controller.exit = Promise.resolve({ code: exitCode, signal: signalCode });
  controller.closed = Promise.resolve({ code: exitCode, signal: signalCode });
  return controller;
}

test("QEMU marker waits reject post-success errors with bounded output state", async () => {
  const expected = `PCR_SUDDEN_GUEST_POWER_SETUP_OK_V1 ${"ab".repeat(16)}`;
  const closeController = qemuTestController({ exitCode: 0 });
  closeController.closed = Promise.resolve().then(() => {
    recordQemuChunk(closeController, "stdout", `${expected}\n`);
    finishQemuStdout(closeController);
    return { code: 0, signal: null };
  });
  assert.equal(
    await waitForQemuLine(
      closeController,
      (line) => line === expected,
      "setup boot",
    ),
    expected,
  );

  const guestError =
    `PCR_SUDDEN_GUEST_POWER_ERROR_V1 ${"cd".repeat(16)} mount_ext4 status=1`;
  const discardedPrefix = "discarded-prefix";
  const stdout =
    `${discardedPrefix}${"x".repeat(QEMU_DIAGNOSTIC_TAIL_LIMIT)}\n` +
    `${guestError}\n`;
  const errorController = qemuTestController();
  recordQemuChunk(errorController, "stdout", stdout);
  recordQemuChunk(errorController, "stderr", "guest stderr\n");
  await assert.rejects(
    waitForQemuLine(errorController, () => false, "recovery boot"),
    (error) => {
      assert.match(error.message, /reported guest failure/u);
      assert.match(error.message, /mount_ext4 status=1/u);
      assert.match(error.message, /stdoutTail=/u);
      assert.match(error.message, /stderrTail="guest stderr\\n"/u);
      assert.doesNotMatch(error.message, /discarded-prefix/u);
      return true;
    },
  );

  const postSuccessErrorController = qemuTestController({ exitCode: 0 });
  recordQemuChunk(postSuccessErrorController, "stdout", `${expected}\n`);
  assert.equal(
    await waitForQemuLine(
      postSuccessErrorController,
      (line) => line === expected,
      "setup boot",
    ),
    expected,
  );
  recordQemuChunk(
    postSuccessErrorController,
    "stdout",
    `${guestError}\n`,
  );
  await assert.rejects(
    waitForQemuExit(postSuccessErrorController, "setup boot"),
    /reported guest failure/u,
  );

  const lineController = qemuTestController();
  recordQemuChunk(
    lineController,
    "stdout",
    Array.from(
      { length: QEMU_LINE_HISTORY_LIMIT + 10 },
      (_, index) => `noise-${index}\n`,
    ).join(""),
  );
  assert.equal(lineController.lines.length, QEMU_LINE_HISTORY_LIMIT);
  assert.equal(lineController.lines[0], "noise-10");
  recordQemuChunk(
    lineController,
    "stdout",
    "x".repeat(QEMU_LINE_BYTE_LIMIT + 1),
  );
  assert.equal(lineController.discardingLongLine, true);
  assert.equal(lineController.partialLine, "");

  const overflowController = qemuTestController();
  recordQemuChunk(
    overflowController,
    "stdout",
    "x".repeat(QEMU_OUTPUT_LIMIT + 1),
  );
  assert.equal(overflowController.outputState.overflow, true);
  assert.deepEqual(overflowController.killSignals, ["SIGKILL"]);
  assert.equal(
    Buffer.byteLength(overflowController.stdout) <= QEMU_OUTPUT_LIMIT,
    true,
  );
});

test("QEMU arguments preserve flushes and forbid graceful crash injection", () => {
  const arguments_ = qemuArguments({
    dataPath: "/var/tmp/pcr/data.raw",
    initramfsPath: "/var/tmp/pcr/initramfs.img",
    kernelPath: "/var/tmp/pcr/vmlinuz-test",
    marker: "pcrpl_1234_abcdef12",
    mode: "armed",
    nonce: "ab".repeat(16),
    pidfilePath: "/var/tmp/pcr/qemu.pid",
    qmpPath: "/var/tmp/pcr/qmp.sock",
  });
  assert.equal(arguments_.includes("-daemonize"), false);
  assert.equal(arguments_.includes("-snapshot"), false);
  assert.equal(arguments_.includes("cache=unsafe"), false);
  assert.equal(arguments_.includes("cache.no-flush=on"), false);
  assert.equal(arguments_.includes("-nic"), true);
  assert.equal(arguments_.includes("none"), true);
  const drive = arguments_[arguments_.indexOf("-drive") + 1];
  assert.match(drive, /cache=none/u);
  assert.match(drive, /aio=threads/u);
  assert.match(drive, /snapshot=off/u);
  assert.equal(arguments_.some((value) => /system_powerdown|\bquit\b/u.test(value)), false);
  assert.throws(
    () => qemuArguments({
      dataPath: "/var/tmp/pcr/data.raw",
      initramfsPath: "/var/tmp/pcr/initramfs.img",
      kernelPath: "/var/tmp/pcr/vmlinuz-test",
      marker: "pcrpl_1234_abcdef12",
      mode: "armed",
      nonce: "ab".repeat(16),
      pidfilePath: "/var/tmp/pcr/qemu.pid",
      qmpPath: `/var/tmp/${"q".repeat(108)}`,
    }),
    /Unix-socket path limit/u,
  );
});

test(
  "external QEMU SIGKILL preserves the fsynced ext4 prefix across same-medium restart",
  { skip: !CONFORMANCE_ENABLED, timeout: 20 * 60 * 1000 },
  runConformance,
);
