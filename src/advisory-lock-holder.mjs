import { constants } from "node:fs";
import { lstat, open, rename } from "node:fs/promises";
import { createInterface } from "node:readline";

import {
  assertInheritedLockPathCurrent,
  holderLockPath,
} from "./advisory-lock-holder-guard.mjs";

const input = createInterface({ input: process.stdin });
const lockPath = holderLockPath();
let operations = Promise.resolve();
let unavailable = false;

function uncertainCommitError() {
  const error = new Error("authority lock holder is unavailable");
  error.code = "lock_commit_uncertain";
  return error;
}

function destinationChangedError() {
  const error = new Error("rename destination changed");
  error.code = "destination_changed";
  return error;
}

function parseExpectedIdentity(value) {
  if (
    value === null ||
    typeof value !== "object" ||
    Array.isArray(value) ||
    !["absent", "present"].includes(value.kind)
  ) {
    throw new Error("invalid destination precondition");
  }
  const keys = Object.keys(value).sort();
  if (value.kind === "absent") {
    if (keys.length !== 1 || keys[0] !== "kind") {
      throw new Error("invalid destination precondition");
    }
    return value;
  }
  if (
    keys.join("\0") !== "dev\0ino\0kind" ||
    !/^(?:0|[1-9][0-9]*)$/u.test(value.dev) ||
    !/^(?:0|[1-9][0-9]*)$/u.test(value.ino)
  ) {
    throw new Error("invalid destination precondition");
  }
  return { dev: BigInt(value.dev), ino: BigInt(value.ino), kind: value.kind };
}

function sameIdentity(left, right) {
  return left.dev === right.dev && left.ino === right.ino;
}

async function assertDestinationPrecondition(path, expected) {
  if (expected === undefined) return;
  const parsed = parseExpectedIdentity(expected);
  if (parsed.kind === "absent") {
    try {
      await lstat(path, { bigint: true });
    } catch (error) {
      if (error?.code === "ENOENT") return;
      throw destinationChangedError();
    }
    throw destinationChangedError();
  }

  let handle;
  try {
    handle = await open(
      path,
      constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK,
    );
    const [held, visible] = await Promise.all([
      handle.stat({ bigint: true }),
      lstat(path, { bigint: true }),
    ]);
    if (
      !held.isFile() ||
      !visible.isFile() ||
      held.nlink !== 1n ||
      visible.nlink !== 1n ||
      !sameIdentity(held, parsed) ||
      !sameIdentity(visible, parsed)
    ) {
      throw destinationChangedError();
    }
  } catch (error) {
    if (error?.code === "destination_changed") throw error;
    throw destinationChangedError();
  } finally {
    await handle?.close().catch(() => {});
  }
}

async function handleCommand(line) {
  let command;
  try {
    command = JSON.parse(line);
    if (unavailable) throw uncertainCommitError();
    if (
      command?.action !== "rename" ||
      !Number.isSafeInteger(command.id) ||
      typeof command.source !== "string" ||
      typeof command.destination !== "string"
    ) {
      throw new Error("invalid lock holder command");
    }
    const expectedDestination = command.expectedDestination;
    try {
      await assertInheritedLockPathCurrent(lockPath);
    } catch (error) {
      if (error?.code === "lock_replaced") unavailable = true;
      throw error;
    }
    await assertDestinationPrecondition(command.destination, expectedDestination);
    await rename(command.source, command.destination);
    try {
      await assertInheritedLockPathCurrent(lockPath);
    } catch (error) {
      if (error?.code === "lock_replaced") {
        unavailable = true;
        throw uncertainCommitError();
      }
      throw error;
    }
    process.stdout.write(`${JSON.stringify({ id: command.id, ok: true })}\n`);
  } catch (error) {
    const id = Number.isSafeInteger(command?.id) ? command.id : null;
    const code = typeof error?.code === "string" ? error.code : "invalid_command";
    process.stdout.write(`${JSON.stringify({ id, ok: false, code })}\n`);
  }
}

input.on("line", (line) => {
  operations = operations.then(() => handleCommand(line));
});
input.on("close", async () => {
  await operations.catch(() => {});
  process.exit(0);
});
process.stdin.on("error", () => input.close());

process.stdout.write("locked\n");
