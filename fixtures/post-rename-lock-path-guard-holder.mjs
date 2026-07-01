import { writeFileSync } from "node:fs";
import { access, rename, writeFile } from "node:fs/promises";
import { createInterface } from "node:readline";
import { setTimeout as delay } from "node:timers/promises";

import {
  assertInheritedLockPathCurrent,
  holderLockPath,
} from "../src/advisory-lock-holder-guard.mjs";

const [readyMarker, continueMarker, queuedMarker, latchedMarker] = process.argv.slice(2);
const lockPath = holderLockPath();
const input = createInterface({ input: process.stdin });
let operations = Promise.resolve();
let unavailable = false;
let injectPostRenameReplacement = true;
let bufferedPostRenameResponse;
let receivedCommands = 0;

function writeResponse(response) {
  process.stdout.write(`${JSON.stringify(response)}\n`);
}

function lockReplacedError() {
  const error = new Error("synthetic post-rename lock replacement");
  error.code = "lock_replaced";
  return error;
}

async function assertPostRenameLockPathCurrent() {
  if (injectPostRenameReplacement) {
    injectPostRenameReplacement = false;
    throw lockReplacedError();
  }
  await assertInheritedLockPathCurrent(lockPath);
}

async function waitForContinue() {
  while (true) {
    try {
      await access(continueMarker);
      return;
    } catch (error) {
      if (error?.code !== "ENOENT") throw error;
      await delay(10);
    }
  }
}

async function handleCommand(line) {
  let command;
  try {
    command = JSON.parse(line);
    if (unavailable) {
      await writeFile(latchedMarker, "latched\n", { mode: 0o600 });
      if (bufferedPostRenameResponse) {
        writeResponse(bufferedPostRenameResponse);
        bufferedPostRenameResponse = undefined;
      }
      writeResponse({ id: command.id, ok: false, code: "lock_commit_uncertain" });
      return;
    }
    try {
      await assertInheritedLockPathCurrent(lockPath);
    } catch (error) {
      if (error?.code === "lock_replaced") unavailable = true;
      throw error;
    }
    await writeFile(readyMarker, "ready\n", { mode: 0o600 });
    await waitForContinue();
    await rename(command.source, command.destination);
    try {
      await assertPostRenameLockPathCurrent();
    } catch (error) {
      if (error?.code === "lock_replaced") {
        unavailable = true;
        // Hold the current response until the already queued command observes
        // the local fence. This makes the regression independent of how soon
        // the broker consumes output and starts quiescing the holder.
        bufferedPostRenameResponse = {
          id: command.id,
          ok: false,
          code: "lock_commit_uncertain",
        };
        return;
      }
      throw error;
    }
    writeResponse({ id: command.id, ok: true });
  } catch (error) {
    const id = Number.isSafeInteger(command?.id) ? command.id : null;
    writeResponse({ id, ok: false, code: error?.code ?? "unknown" });
  }
}

input.on("line", (line) => {
  receivedCommands += 1;
  if (receivedCommands === 2) {
    writeFileSync(queuedMarker, "queued\n", { mode: 0o600 });
  }
  operations = operations.then(() => handleCommand(line));
});
input.on("close", async () => {
  await operations.catch(() => {});
  process.exit(0);
});
process.stdin.on("error", () => input.close());

process.stdout.write("locked\n");
