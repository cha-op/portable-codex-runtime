import assert from "node:assert/strict";
import { spawn } from "node:child_process";
import { once } from "node:events";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";
import { join } from "node:path";
import test from "node:test";

import { AdvisoryLockError, acquireAdvisoryLock } from "../src/advisory-lock.mjs";

const HOLDER_FIXTURE = fileURLToPath(new URL("../fixtures/hold-advisory-lock.mjs", import.meta.url));

function waitForLine(child, expected) {
  return new Promise((resolve, reject) => {
    let output = "";
    const timer = setTimeout(() => finish(() => reject(new Error("lock owner timed out"))), 5_000);
    const finish = (callback) => {
      clearTimeout(timer);
      child.stdout.off("data", onData);
      child.off("exit", onExit);
      callback();
    };
    const onData = (chunk) => {
      output += chunk.toString("utf8");
      if (output.includes(expected)) finish(resolve);
    };
    const onExit = () => finish(() => reject(new Error("lock owner exited before ready")));
    child.stdout.on("data", onData);
    child.on("exit", onExit);
  });
}

test("advisory lock rejects concurrent holders and can be reacquired", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-advisory-lock-"));
  const path = join(root, "authority.lock");
  let first;
  let second;
  try {
    first = await acquireAdvisoryLock(path);
    await assert.rejects(acquireAdvisoryLock(path), (error) => {
      assert(error instanceof AdvisoryLockError);
      assert.equal(error.code, "lock_unavailable");
      return true;
    });
    await first.release();
    first = undefined;
    second = await acquireAdvisoryLock(path);
    second.assertHeld();
  } finally {
    await first?.release();
    await second?.release();
    await rm(root, { recursive: true, force: true });
  }
});

test("OS releases the advisory lock after the owning process is killed", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-advisory-crash-lock-"));
  const path = join(root, "authority.lock");
  const owner = spawn(process.execPath, [HOLDER_FIXTURE, path], {
    stdio: ["pipe", "pipe", "pipe"],
  });
  let recovered;
  try {
    await waitForLine(owner, "ready\n");
    owner.kill("SIGKILL");
    await once(owner, "exit");

    for (let attempt = 0; attempt < 50; attempt += 1) {
      try {
        recovered = await acquireAdvisoryLock(path);
        break;
      } catch (error) {
        if (!(error instanceof AdvisoryLockError) || error.code !== "lock_unavailable") throw error;
        await new Promise((resolve) => setTimeout(resolve, 20));
      }
    }
    assert(recovered, "lock was not released after the owner process died");
    recovered.assertHeld();
  } finally {
    if (owner.exitCode === null && owner.signalCode === null) owner.kill("SIGKILL");
    await recovered?.release();
    await rm(root, { recursive: true, force: true });
  }
});
