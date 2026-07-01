import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import { once } from "node:events";
import { access, link, mkdtemp, rename, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";
import { join } from "node:path";
import test from "node:test";

import {
  AdvisoryLockError,
  acquireAdvisoryLock,
  advisoryLockCommand,
} from "../src/advisory-lock.mjs";

const HOLDER_FIXTURE = fileURLToPath(new URL("../fixtures/hold-advisory-lock.mjs", import.meta.url));
const FAIL_HOLDER_FIXTURE = fileURLToPath(
  new URL("../fixtures/fail-advisory-lock-holder.mjs", import.meta.url),
);
const STUBBORN_HOLDER_FIXTURE = fileURLToPath(
  new URL("../fixtures/stubborn-advisory-lock-holder.mjs", import.meta.url),
);

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
    await second.assertHeld();
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
    await recovered.assertHeld();
  } finally {
    if (owner.exitCode === null && owner.signalCode === null) owner.kill("SIGKILL");
    await recovered?.release();
    await rm(root, { recursive: true, force: true });
  }
});

test("advisory lock backends use the inherited secure file descriptor", () => {
  assert.deepEqual(advisoryLockCommand("darwin"), {
    command: "/usr/bin/lockf",
    args: [
      "-k",
      "-s",
      "-t",
      "0",
      "-w",
      "/dev/fd/3",
      process.execPath,
      fileURLToPath(new URL("../src/advisory-lock-holder.mjs", import.meta.url)),
    ],
    conflictExitCode: 75,
  });
  assert.deepEqual(advisoryLockCommand("linux"), {
    command: "flock",
    args: [
      "--exclusive",
      "--nonblock",
      "--conflict-exit-code",
      "75",
      "--no-fork",
      "/proc/self/fd/3",
      process.execPath,
      fileURLToPath(new URL("../src/advisory-lock-holder.mjs", import.meta.url)),
    ],
    conflictExitCode: 75,
  });
});

test("holder exit one is a runtime failure rather than lock contention", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-advisory-holder-failure-"));
  const path = join(root, "authority.lock");
  try {
    await assert.rejects(acquireAdvisoryLock(path, { holderPath: FAIL_HOLDER_FIXTURE }), (error) => {
      return error instanceof AdvisoryLockError && error.code === "lock_runtime_failed";
    });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("startup timeout kills a stubborn lock process group", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-advisory-startup-timeout-"));
  const path = join(root, "authority.lock");
  let recovered;
  try {
    await assert.rejects(
      acquireAdvisoryLock(path, {
        holderArgs: ["timeout"],
        holderPath: STUBBORN_HOLDER_FIXTURE,
        signalGraceMs: 100,
        timeoutMs: 100,
      }),
      (error) => error instanceof AdvisoryLockError && error.code === "lock_timeout",
    );
    recovered = await acquireAdvisoryLock(path);
  } finally {
    await recovered?.release();
    await rm(root, { recursive: true, force: true });
  }
});

test("release kills a stubborn lock process group before returning", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-advisory-stubborn-release-"));
  const path = join(root, "authority.lock");
  let lock;
  let recovered;
  try {
    lock = await acquireAdvisoryLock(path, {
      holderArgs: ["release"],
      holderPath: STUBBORN_HOLDER_FIXTURE,
      releaseGraceMs: 50,
      signalGraceMs: 100,
    });
    await lock.release();
    lock = undefined;
    recovered = await acquireAdvisoryLock(path);
  } finally {
    await lock?.release();
    await recovered?.release();
    await rm(root, { recursive: true, force: true });
  }
});

test("unsupported lock platforms fail before creating the lock file", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-advisory-platform-"));
  const path = join(root, "authority.lock");
  try {
    await assert.rejects(acquireAdvisoryLock(path, { platform: "win32" }), (error) => {
      return error instanceof AdvisoryLockError && error.code === "unsupported_platform";
    });
    await assert.rejects(access(path), (error) => error.code === "ENOENT");
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("advisory lock rejects symlinks and hard links", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-advisory-unsafe-lock-"));
  const target = join(root, "target");
  const path = join(root, "authority.lock");
  try {
    await writeFile(target, "unrelated\n", { mode: 0o644 });
    await symlink(target, path);
    await assert.rejects(acquireAdvisoryLock(path), (error) => {
      return error instanceof AdvisoryLockError && error.code === "unsafe_lock_file";
    });
    await rm(path);
    await link(target, path);
    await assert.rejects(acquireAdvisoryLock(path), (error) => {
      return error instanceof AdvisoryLockError && error.code === "unsafe_lock_file";
    });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("advisory lock detects path replacement while held", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-advisory-replaced-lock-"));
  const path = join(root, "authority.lock");
  const displaced = join(root, "displaced.lock");
  let lock;
  try {
    lock = await acquireAdvisoryLock(path);
    await rename(path, displaced);
    await writeFile(path, "replacement\n", { mode: 0o600 });
    await assert.rejects(lock.assertHeld(), (error) => {
      return error instanceof AdvisoryLockError && error.code === "lock_replaced";
    });
  } finally {
    await lock?.release();
    await rm(root, { recursive: true, force: true });
  }
});

test("advisory lock rejects FIFO replacement without blocking", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-advisory-fifo-lock-"));
  const path = join(root, "authority.lock");
  const displaced = join(root, "displaced.lock");
  let lock;
  try {
    lock = await acquireAdvisoryLock(path);
    await rename(path, displaced);
    const created = spawnSync("mkfifo", [path], { encoding: "utf8" });
    assert.equal(created.status, 0, created.stderr);
    await assert.rejects(lock.assertHeld(), (error) => {
      return error instanceof AdvisoryLockError && error.code === "lock_replaced";
    });
  } finally {
    await lock?.release();
    await rm(root, { recursive: true, force: true });
  }
});
