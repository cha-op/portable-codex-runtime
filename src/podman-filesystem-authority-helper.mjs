import { Buffer } from "node:buffer";
import {
  closeSync,
  constants as fsConstants,
  fstatSync,
  openSync,
} from "node:fs";
import { isAbsolute, resolve } from "node:path";

const MAX_CONTROL_BYTES = 16;
const MAX_ACQUISITION_BYTES = 64 * 1024;
const MAX_RECEIPT_BYTES = 1024;
const MAX_HOST_PATH_BYTES = 4095;
const CLOSE_MESSAGE = "close\n";
const VERIFY_MESSAGE = "verify\n";

function failClosed() {
  process.exitCode = 70;
}

function openDirectory(path) {
  return openSync(
    path,
    fsConstants.O_RDONLY | fsConstants.O_DIRECTORY | fsConstants.O_NOFOLLOW,
  );
}

function heldDirectory(fd) {
  const stat = fstatSync(fd, { bigint: true });
  if ((stat.mode & BigInt(fsConstants.S_IFMT)) !== BigInt(fsConstants.S_IFDIR)) {
    throw new Error("not a directory");
  }
  return {
    dev: stat.dev.toString(10),
    fd,
    ino: stat.ino.toString(10),
  };
}

function validHostPathname(path) {
  if (
    typeof path !== "string" ||
    path.length <= 1 ||
    path.length > MAX_HOST_PATH_BYTES
  ) return false;
  const encoded = Buffer.from(path, "utf8");
  return encoded.length <= MAX_HOST_PATH_BYTES &&
    encoded.toString("utf8") === path &&
    isAbsolute(path) &&
    resolve(path) === path &&
    !/[\0\r\n]/u.test(path);
}

let configuredFd = null;
let attachmentFd = null;
let initialAttachment = null;
let initialConfigured = null;
let phase = "acquisition";
let input = Buffer.alloc(0);

function terminate() {
  failClosed();
  process.stdin.destroy();
}

function acquisitionFrame(message) {
  const value = JSON.parse(message);
  if (
    value === null ||
    typeof value !== "object" ||
    Array.isArray(value) ||
    Reflect.ownKeys(value).length !== 3 ||
    value.contractVersion !== 1 ||
    typeof value.configuredRoot !== "string" ||
    typeof value.attachmentRoot !== "string"
  ) throw new Error("invalid acquisition");
  for (const path of [value.configuredRoot, value.attachmentRoot]) {
    if (!validHostPathname(path)) throw new Error("invalid path");
  }
  configuredFd = openDirectory(value.configuredRoot);
  attachmentFd = openDirectory(value.attachmentRoot);
  initialAttachment = heldDirectory(attachmentFd);
  initialConfigured = heldDirectory(configuredFd);
  const receipt = JSON.stringify({
    attachment: initialAttachment,
    configured: initialConfigured,
    contractVersion: 1,
    pid: process.pid,
    status: "ready",
  });
  if (Buffer.byteLength(receipt, "utf8") > MAX_RECEIPT_BYTES) {
    throw new Error("oversized receipt");
  }
  phase = "ready";
  process.stdout.write(`${receipt}\n`);
}

function controlFrame(message) {
  if (message === VERIFY_MESSAGE) {
    const currentAttachment = heldDirectory(attachmentFd);
    const currentConfigured = heldDirectory(configuredFd);
    if (
      currentAttachment.dev !== initialAttachment.dev ||
      currentAttachment.ino !== initialAttachment.ino ||
      currentConfigured.dev !== initialConfigured.dev ||
      currentConfigured.ino !== initialConfigured.ino
    ) throw new Error("held directory changed");
    process.stdout.write("verified\n");
    return;
  }
  if (message === CLOSE_MESSAGE) {
    phase = "closing";
    process.stdin.destroy();
    return;
  }
  throw new Error("invalid control");
}

try {
  if (process.argv.length !== 2) throw new Error("invalid arguments");
  process.stdin.on("data", (chunk) => {
    if (phase === "closing") return terminate();
    input = Buffer.concat([input, chunk], input.length + chunk.length);
    const limit = phase === "acquisition"
      ? MAX_ACQUISITION_BYTES
      : MAX_CONTROL_BYTES;
    if (input.length > limit) return terminate();
    const newline = input.indexOf(0x0a);
    if (newline === -1) return;
    if (newline !== input.length - 1) return terminate();
    const message = input.toString("utf8");
    input = Buffer.alloc(0);
    try {
      if (phase === "acquisition") acquisitionFrame(message.slice(0, -1));
      else controlFrame(message);
    } catch {
      terminate();
    }
  });
  process.stdin.on("end", () => {
    if (input.length !== 0) failClosed();
  });
  process.stdin.resume();
} catch {
  failClosed();
} finally {
  process.on("exit", () => {
    if (attachmentFd !== null) {
      try {
        closeSync(attachmentFd);
      } catch {
        failClosed();
      }
    }
    if (configuredFd !== null) {
      try {
        closeSync(configuredFd);
      } catch {
        failClosed();
      }
    }
  });
}
