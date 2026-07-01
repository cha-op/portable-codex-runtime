import { access, rename, writeFile } from "node:fs/promises";
import { createInterface } from "node:readline";
import { setTimeout as delay } from "node:timers/promises";

import {
  assertInheritedLockPathCurrent,
  holderLockPath,
} from "../src/advisory-lock-holder-guard.mjs";

const [readyMarker, continueMarker] = process.argv.slice(2);
const lockPath = holderLockPath();
const input = createInterface({ input: process.stdin });

input.on("line", async (line) => {
  const command = JSON.parse(line);
  try {
    await writeFile(readyMarker, "ready\n", { mode: 0o600 });
    while (true) {
      try {
        await access(continueMarker);
        break;
      } catch (error) {
        if (error?.code !== "ENOENT") throw error;
        await delay(10);
      }
    }
    await assertInheritedLockPathCurrent(lockPath);
    await rename(command.source, command.destination);
    process.stdout.write(`${JSON.stringify({ id: command.id, ok: true })}\n`);
  } catch (error) {
    process.stdout.write(
      `${JSON.stringify({ id: command.id, ok: false, code: error?.code ?? "unknown" })}\n`,
    );
  }
});

process.stdout.write("locked\n");
