import { rename } from "node:fs/promises";
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
    try {
      await assertInheritedLockPathCurrent(lockPath);
    } catch (error) {
      if (error?.code === "lock_replaced") unavailable = true;
      throw error;
    }
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
