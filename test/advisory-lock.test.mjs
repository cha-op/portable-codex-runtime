import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import { once } from "node:events";
import {
  access,
  chmod,
  link,
  lstat,
  mkdtemp,
  readFile,
  rename,
  rm,
  symlink,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { fileURLToPath } from "node:url";
import { join } from "node:path";
import test from "node:test";

import {
  AdvisoryLockError,
  acquireAdvisoryLock,
  advisoryLockCommand,
  sameFileIdentity,
} from "../src/advisory-lock.mjs";

const HOLDER_FIXTURE = fileURLToPath(new URL("../fixtures/hold-advisory-lock.mjs", import.meta.url));
const FAIL_HOLDER_FIXTURE = fileURLToPath(
  new URL("../fixtures/fail-advisory-lock-holder.mjs", import.meta.url),
);
const EXIT_AFTER_LOCK_FIXTURE = fileURLToPath(
  new URL("../fixtures/exit-after-lock-holder.mjs", import.meta.url),
);
const STUBBORN_HOLDER_FIXTURE = fileURLToPath(
  new URL("../fixtures/stubborn-advisory-lock-holder.mjs", import.meta.url),
);
const DELAYED_HOLDER_FIXTURE = fileURLToPath(
  new URL("../fixtures/delayed-advisory-lock-holder.mjs", import.meta.url),
);
const QUEUED_DELAYED_HOLDER_FIXTURE = fileURLToPath(
  new URL("../fixtures/queued-delayed-advisory-lock-holder.mjs", import.meta.url),
);
const CONTROLLED_LOCK_PATH_GUARD_FIXTURE = fileURLToPath(
  new URL("../fixtures/controlled-lock-path-guard-holder.mjs", import.meta.url),
);
const POST_RENAME_LOCK_PATH_GUARD_FIXTURE = fileURLToPath(
  new URL("../fixtures/post-rename-lock-path-guard-holder.mjs", import.meta.url),
);
const KILL_LOCKF_PARENT_FIXTURE = fileURLToPath(
  new URL("../fixtures/kill-lockf-parent-holder.mjs", import.meta.url),
);
const KILL_LOCKF_PARENT_DURING_RENAME_FIXTURE = fileURLToPath(
  new URL("../fixtures/kill-lockf-parent-during-rename-holder.mjs", import.meta.url),
);
const REPLACE_LOCK_BEFORE_READY_FIXTURE = fileURLToPath(
  new URL("../fixtures/replace-advisory-lock-before-ready-holder.mjs", import.meta.url),
);

test("file identity comparison preserves large inode precision", () => {
  const inode = 2n ** 54n;
  assert.equal(Number(inode), Number(inode + 1n));
  assert.equal(
    sameFileIdentity({ dev: 1n, ino: inode }, { dev: 1n, ino: inode + 1n }),
    false,
  );
  assert.equal(
    sameFileIdentity({ dev: 1n, ino: inode }, { dev: 1n, ino: inode }),
    true,
  );
  assert.equal(sameFileIdentity(undefined, undefined), false);
  assert.equal(
    sameFileIdentity({ dev: 1, ino: Number(inode) }, { dev: 1, ino: Number(inode) }),
    false,
  );
});

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

function failFirstStopProcess({ failFirstUnref = false } = {}) {
  let attempts = 0;
  let child;
  let unrefCalls = 0;
  const cleanupError = new AdvisoryLockError(
    "lock_cleanup_failed",
    "authority lock process group survived SIGKILL",
  );
  return {
    get attempts() {
      return attempts;
    },
    get child() {
      return child;
    },
    get cleanupError() {
      return cleanupError;
    },
    get unrefCalls() {
      return unrefCalls;
    },
    async stopProcess(currentChild, options, defaultStopProcess) {
      attempts += 1;
      child = currentChild;
      if (attempts === 1) {
        const unref = currentChild.unref.bind(currentChild);
        currentChild.unref = () => {
          unrefCalls += 1;
          if (failFirstUnref && unrefCalls === 1) {
            throw new Error("injected unref failure");
          }
          return unref();
        };
        throw cleanupError;
      }
      await defaultStopProcess(currentChild, options);
    },
  };
}

function assertProcessReferencesDisposed(probe) {
  assert(probe.child, "expected stopProcess to observe the lock child");
  assert.equal(probe.child.stdin.destroyed, true);
  assert.equal(probe.child.stdout.destroyed, true);
  assert.equal(probe.child.stderr.destroyed, true);
  assert.equal(probe.child.stdout.listenerCount("data"), 0);
  assert.equal(probe.unrefCalls, 1);
}

async function within(promise, timeoutMs) {
  let timer;
  try {
    return await Promise.race([
      promise,
      new Promise((_, reject) => {
        timer = setTimeout(() => reject(new Error("operation did not settle promptly")), timeoutMs);
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

async function waitForPath(path, timeoutMs = 2_000) {
  const deadline = Date.now() + timeoutMs;
  while (true) {
    try {
      await access(path);
      return;
    } catch (error) {
      if (error?.code !== "ENOENT") throw error;
    }
    assert(Date.now() < deadline, `timed out waiting for ${path}`);
    await new Promise((resolve) => setTimeout(resolve, 10));
  }
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

test("existing-only advisory locks neither create nor repair the lock file", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-advisory-existing-"));
  const path = join(root, "authority.lock");
  let lock;
  try {
    await assert.rejects(
      acquireAdvisoryLock(path, { requireExisting: true }),
      (error) =>
        error instanceof AdvisoryLockError && error.code === "unsafe_lock_file",
    );
    await assert.rejects(access(path), (error) => error.code === "ENOENT");

    await writeFile(path, "", { mode: 0o644 });
    await chmod(path, 0o644);
    await assert.rejects(
      acquireAdvisoryLock(path, { requireExisting: true }),
      (error) =>
        error instanceof AdvisoryLockError && error.code === "unsafe_lock_file",
    );
    assert.equal(Number((await lstat(path, { bigint: true })).mode & 0o777n), 0o644);

    await chmod(path, 0o600);
    await writeFile(path, "foreign\n", { mode: 0o600 });
    await assert.rejects(
      acquireAdvisoryLock(path, { requireExisting: true }),
      (error) =>
        error instanceof AdvisoryLockError && error.code === "unsafe_lock_file",
    );
    assert.equal(await readFile(path, "utf8"), "foreign\n");

    await writeFile(path, "", { mode: 0o600 });
    lock = await acquireAdvisoryLock(path, { requireExisting: true });
    await lock.assertHeld();
    assert.equal(Number((await lstat(path, { bigint: true })).mode & 0o777n), 0o600);
  } finally {
    await lock?.release();
    await rm(root, { recursive: true, force: true });
  }
});

test("existing-only advisory locks detect protection changes while held", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-advisory-protection-"));
  const path = join(root, "authority.lock");
  let lock;
  try {
    await writeFile(path, "", { mode: 0o600 });
    lock = await acquireAdvisoryLock(path, { requireExisting: true });
    await chmod(path, 0o644);
    await assert.rejects(
      lock.assertHeld(),
      (error) =>
        error instanceof AdvisoryLockError && error.code === "lock_replaced",
    );
    assert.equal(Number((await lstat(path, { bigint: true })).mode & 0o777n), 0o644);
  } finally {
    await lock?.release().catch(() => {});
    await rm(root, { recursive: true, force: true });
  }
});

test("uncertain rename quiesces the holder before it can mutate later", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-advisory-lock-delayed-"));
  const lockPath = join(root, "authority.lock");
  const source = join(root, "source");
  const destination = join(root, "destination");
  let lock;
  try {
    await writeFile(source, "candidate\n", { mode: 0o600 });
    lock = await acquireAdvisoryLock(lockPath, {
      holderPath: DELAYED_HOLDER_FIXTURE,
      signalGraceMs: 1_000,
      startupTimeoutMs: 5_000,
      timeoutMs: 50,
    });
    await assert.rejects(
      lock.renameWhileHeld(source, destination),
      (error) => error.code === "lock_commit_uncertain",
    );
    await new Promise((resolve) => setTimeout(resolve, 350));
    assert.equal(await readFile(source, "utf8"), "candidate\n");
    await assert.rejects(access(destination), (error) => error.code === "ENOENT");
  } finally {
    await lock?.release().catch(() => {});
    await rm(root, { recursive: true, force: true });
  }
});

test("advisory lock holder performs the final rename while holding the lock", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-advisory-commit-"));
  const path = join(root, "authority.lock");
  const source = join(root, "auth.next");
  const destination = join(root, "auth.json");
  let lock;
  try {
    await writeFile(source, "rotated\n", { mode: 0o600 });
    lock = await acquireAdvisoryLock(path);
    await lock.renameWhileHeld(source, destination);
    assert.equal(await readFile(destination, "utf8"), "rotated\n");
    await assert.rejects(access(source), (error) => error.code === "ENOENT");
    await lock.assertHeld();
  } finally {
    await lock?.release();
    await rm(root, { recursive: true, force: true });
  }
});

test("advisory lock holder rejects a changed rename destination before commit", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-advisory-destination-"));
  const path = join(root, "authority.lock");
  const source = join(root, "candidate.json");
  const destination = join(root, "current.json");
  let lock;
  try {
    await writeFile(source, "candidate\n", { mode: 0o600 });
    await writeFile(destination, "current\n", { mode: 0o600 });
    lock = await acquireAdvisoryLock(path);
    await assert.rejects(
      lock.renameWhileHeld(source, destination, { kind: "absent" }),
      (error) =>
        error instanceof AdvisoryLockError &&
        error.code === "destination_changed" &&
        error.renameOutcome === "not-committed",
    );
    assert.equal(await readFile(source, "utf8"), "candidate\n");
    assert.equal(await readFile(destination, "utf8"), "current\n");
    await lock.assertHeld();
  } finally {
    await lock?.release().catch(() => {});
    await rm(root, { recursive: true, force: true });
  }
});

test("advisory lock holder accepts the exact expected rename destination", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-advisory-expected-destination-"));
  const path = join(root, "authority.lock");
  const source = join(root, "candidate.json");
  const destination = join(root, "current.json");
  let lock;
  try {
    await writeFile(source, "candidate\n", { mode: 0o600 });
    await writeFile(destination, "current\n", { mode: 0o600 });
    const expected = await lstat(destination, { bigint: true });
    lock = await acquireAdvisoryLock(path);
    await lock.renameWhileHeld(source, destination, {
      dev: expected.dev.toString(),
      ino: expected.ino.toString(),
      kind: "present",
    });
    assert.equal(await readFile(destination, "utf8"), "candidate\n");
    await assert.rejects(access(source), (error) => error.code === "ENOENT");
    await lock.assertHeld();
  } finally {
    await lock?.release().catch(() => {});
    await rm(root, { recursive: true, force: true });
  }
});

test("advisory lock holder rejects replacement of an expected destination inode", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-advisory-replaced-destination-"));
  const path = join(root, "authority.lock");
  const source = join(root, "candidate.json");
  const destination = join(root, "current.json");
  const displaced = join(root, "displaced.json");
  let lock;
  try {
    await writeFile(source, "candidate\n", { mode: 0o600 });
    await writeFile(destination, "original\n", { mode: 0o600 });
    const expected = await lstat(destination, { bigint: true });
    lock = await acquireAdvisoryLock(path);
    await rename(destination, displaced);
    await writeFile(destination, "replacement\n", { mode: 0o600 });
    await assert.rejects(
      lock.renameWhileHeld(source, destination, {
        dev: expected.dev.toString(),
        ino: expected.ino.toString(),
        kind: "present",
      }),
      (error) =>
        error instanceof AdvisoryLockError &&
        error.code === "destination_changed" &&
        error.renameOutcome === "not-committed",
    );
    assert.equal(await readFile(source, "utf8"), "candidate\n");
    assert.equal(await readFile(destination, "utf8"), "replacement\n");
    assert.equal(await readFile(displaced, "utf8"), "original\n");
    await lock.assertHeld();
  } finally {
    await lock?.release().catch(() => {});
    await rm(root, { recursive: true, force: true });
  }
});

test("invalid destination preconditions do not strand a pending rename", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-advisory-invalid-destination-"));
  const path = join(root, "authority.lock");
  const source = join(root, "candidate.json");
  const destination = join(root, "current.json");
  let lock;
  try {
    await writeFile(source, "candidate\n", { mode: 0o600 });
    lock = await acquireAdvisoryLock(path, { timeoutMs: 500 });
    await assert.rejects(
      lock.renameWhileHeld(source, destination, {
        dev: 1n,
        ino: "2",
        kind: "present",
      }),
      (error) =>
        error instanceof AdvisoryLockError &&
        error.code === "invalid_rename_request" &&
        error.renameOutcome === "not-committed",
    );
    await new Promise((resolve) => setTimeout(resolve, 600));
    await lock.assertHeld();
    await lock.renameWhileHeld(source, destination, { kind: "absent" });
    assert.equal(await readFile(destination, "utf8"), "candidate\n");
  } finally {
    await lock?.release().catch(() => {});
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

test("advisory lock backends use independent lock descriptions", () => {
  const protectedLockPath = "/protected/authority.lock";
  assert.deepEqual(advisoryLockCommand("darwin", { lockPath: protectedLockPath }), {
    command: "/usr/bin/lockf",
    args: [
      "-k",
      "-s",
      "-t",
      "0",
      "-w",
      protectedLockPath,
      process.execPath,
      fileURLToPath(new URL("../src/advisory-lock-holder.mjs", import.meta.url)),
      "--portable-auth-lock-path",
      protectedLockPath,
    ],
    conflictExitCode: 75,
  });
  assert.throws(
    () => advisoryLockCommand("darwin"),
    (error) => error instanceof AdvisoryLockError && error.code === "unsafe_lock_file",
  );
  assert.deepEqual(advisoryLockCommand("linux", { lockPath: protectedLockPath }), {
    command: "/usr/bin/flock",
    args: [
      "--exclusive",
      "--nonblock",
      "--conflict-exit-code",
      "75",
      "--no-fork",
      "/proc/self/fd/3",
      process.execPath,
      fileURLToPath(new URL("../src/advisory-lock-holder.mjs", import.meta.url)),
      "--portable-auth-lock-path",
      protectedLockPath,
    ],
    conflictExitCode: 75,
  });
  assert.deepEqual(
    advisoryLockCommand("darwin", {
      lockPath: protectedLockPath,
      requireExisting: true,
    }).args.slice(0, 7),
    ["-k", "-s", "-n", "-t", "0", "-w", protectedLockPath],
  );
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

test("assertHeld reports loss after the owning holder exits", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-advisory-lost-holder-"));
  const path = join(root, "authority.lock");
  let lock;
  try {
    lock = await acquireAdvisoryLock(path, { holderPath: EXIT_AFTER_LOCK_FIXTURE });
    await lock.waitForLoss();
    await assert.rejects(lock.assertHeld(), (error) => {
      return error instanceof AdvisoryLockError && error.code === "lock_lost";
    });
  } finally {
    await lock?.release();
    await rm(root, { recursive: true, force: true });
  }
});

test(
  "holder exit releases the lock while the broker inode guard remains open",
  { skip: !["darwin", "linux"].includes(process.platform) },
  async () => {
    const root = await mkdtemp(join(tmpdir(), "portable-advisory-exited-holder-"));
    const path = join(root, "authority.lock");
    let lostLock;
    let recovered;
    try {
      lostLock = await acquireAdvisoryLock(path, { holderPath: EXIT_AFTER_LOCK_FIXTURE });
      await lostLock.waitForLoss();

      // Do not release lostLock yet. On macOS lockf path mode owns an independent
      // description; on Linux the holder owns the separate OFD reopened through
      // procfs. In both cases the broker descriptor remains open only as an
      // inode-identity guard and must not prevent reacquisition.
      recovered = await acquireAdvisoryLock(path);
      await recovered.assertHeld();
    } finally {
      await lostLock?.release();
      await recovered?.release();
      await rm(root, { recursive: true, force: true });
    }
  },
);

test(
  "macOS lockf executor SIGKILL releases the path-mode lock while the broker guard stays open",
  { skip: process.platform !== "darwin" },
  async () => {
    const root = await mkdtemp(join(tmpdir(), "portable-advisory-lockf-sigkill-"));
    const path = join(root, "authority.lock");
    let lostLock;
    let recovered;
    try {
      lostLock = await acquireAdvisoryLock(path, {
        holderPath: KILL_LOCKF_PARENT_FIXTURE,
      });
      await within(lostLock.waitForLoss(), 2_000);

      for (let attempt = 0; attempt < 50; attempt += 1) {
        try {
          recovered = await acquireAdvisoryLock(path);
          break;
        } catch (error) {
          if (!(error instanceof AdvisoryLockError) || error.code !== "lock_unavailable") {
            throw error;
          }
          await new Promise((resolve) => setTimeout(resolve, 20));
        }
      }
      assert(recovered, "path-mode lock survived after the lockf executor was killed");
      await recovered.assertHeld();
    } finally {
      await lostLock?.release().catch(() => {});
      await recovered?.release();
      await rm(root, { recursive: true, force: true });
    }
  },
);

test(
  "macOS lockf executor loss quiesces a pending rename before recovery",
  { skip: process.platform !== "darwin" },
  async () => {
    const root = await mkdtemp(join(tmpdir(), "portable-advisory-lockf-rename-loss-"));
    const lockPath = join(root, "authority.lock");
    const staleSource = join(root, "stale.next");
    const secondStaleSource = join(root, "second-stale.next");
    const lateSource = join(root, "late.next");
    const recoveredSource = join(root, "recovered.next");
    const destination = join(root, "auth.json");
    const secondDestination = join(root, "second-auth.json");
    const lateDestination = join(root, "late-auth.json");
    const cleanupError = new Error("synthetic quiescence cleanup failure");
    let groupCleanupFinished = false;
    let resolveStopStarted;
    const stopStarted = new Promise((resolve) => {
      resolveStopStarted = resolve;
    });
    let stopCalls = 0;
    let lostLock;
    let recovered;
    try {
      await writeFile(staleSource, "stale\n", { mode: 0o600 });
      await writeFile(secondStaleSource, "second stale\n", { mode: 0o600 });
      await writeFile(lateSource, "late\n", { mode: 0o600 });
      await writeFile(recoveredSource, "recovered\n", { mode: 0o600 });
      lostLock = await acquireAdvisoryLock(lockPath, {
        holderPath: KILL_LOCKF_PARENT_DURING_RENAME_FIXTURE,
        signalGraceMs: 150,
        stopProcess: async (child, options, defaultStopProcess) => {
          stopCalls += 1;
          resolveStopStarted();
          await defaultStopProcess(child, options);
          groupCleanupFinished = true;
          if (stopCalls === 1) throw cleanupError;
        },
        startupTimeoutMs: 5_000,
        timeoutMs: 10,
      });
      const assertSettledAfterCleanup = (promise) =>
        promise.then(
          () => assert.fail("rename unexpectedly succeeded"),
          (error) => {
            assert.equal(groupCleanupFinished, true);
            return error;
          },
        );
      const firstPending = assertSettledAfterCleanup(
        lostLock.renameWhileHeld(staleSource, destination),
      );
      const secondPending = assertSettledAfterCleanup(
        lostLock.renameWhileHeld(secondStaleSource, secondDestination),
      );
      const observedLoss = lostLock.waitForLoss().then((error) => {
        assert.equal(groupCleanupFinished, true);
        return error;
      });
      void firstPending.catch(() => {});
      void secondPending.catch(() => {});
      void observedLoss.catch(() => {});

      await within(stopStarted, 1_000);
      const lateRename = assertSettledAfterCleanup(
        lostLock.renameWhileHeld(lateSource, lateDestination),
      );
      void lateRename.catch(() => {});

      const [loss, firstError, secondError, lateError] = await Promise.all([
        within(observedLoss, 2_000),
        within(firstPending, 2_000),
        within(secondPending, 2_000),
        within(lateRename, 2_000),
      ]);
      assert.equal(loss.code, "lock_lost");
      assert.equal(loss.cause, cleanupError);
      for (const error of [firstError, secondError, lateError]) {
        assert(error instanceof AdvisoryLockError);
        assert.equal(error.code, "lock_commit_uncertain");
        assert.equal(error.cause, cleanupError);
      }
      assert.equal(stopCalls, 1);

      recovered = await acquireAdvisoryLock(lockPath);
      await recovered.renameWhileHeld(recoveredSource, destination);
      await new Promise((resolve) => setTimeout(resolve, 350));
      assert.equal(await readFile(destination, "utf8"), "recovered\n");
      assert.equal(await readFile(staleSource, "utf8"), "stale\n");
    } finally {
      await lostLock?.release().catch(() => {});
      await recovered?.release().catch(() => {});
      await rm(root, { recursive: true, force: true });
    }
  },
);

test(
  "acquire rejects a lock pathname replacement before returning the lock",
  { skip: !["darwin", "linux"].includes(process.platform) },
  async () => {
    const root = await mkdtemp(join(tmpdir(), "portable-advisory-acquire-replaced-"));
    const path = join(root, "authority.lock");
    const displaced = join(root, "displaced.lock");
    let recovered;
    try {
      await assert.rejects(
        acquireAdvisoryLock(path, {
          holderArgs: [path, displaced],
          holderPath: REPLACE_LOCK_BEFORE_READY_FIXTURE,
        }),
        (error) => error instanceof AdvisoryLockError && error.code === "lock_replaced",
      );

      recovered = await acquireAdvisoryLock(path);
      await recovered.assertHeld();
    } finally {
      await recovered?.release();
      await rm(root, { recursive: true, force: true });
    }
  },
);

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

test("acquire cleanup failure disposes child references and preserves the cleanup error", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-advisory-acquire-cleanup-"));
  const path = join(root, "authority.lock");
  const probe = failFirstStopProcess();
  let recovered;
  try {
    await assert.rejects(
      acquireAdvisoryLock(path, {
        holderPath: FAIL_HOLDER_FIXTURE,
        stopProcess: probe.stopProcess.bind(probe),
      }),
      (error) => {
        assert.equal(error, probe.cleanupError);
        assert.equal(error.code, "lock_cleanup_failed");
        assert.equal(error.cause?.code, "lock_runtime_failed");
        return true;
      },
    );
    assert.equal(probe.attempts, 1);
    assertProcessReferencesDisposed(probe);

    recovered = await acquireAdvisoryLock(path);
    await recovered.assertHeld();
  } finally {
    await recovered?.release();
    await rm(root, { recursive: true, force: true });
  }
});

test("acquire disposal-only failure preserves acquisition and resource errors", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-advisory-acquire-disposal-"));
  const path = join(root, "authority.lock");
  const disposalError = new Error("injected child unref failure");
  let recovered;
  try {
    await assert.rejects(
      acquireAdvisoryLock(path, {
        holderPath: FAIL_HOLDER_FIXTURE,
        stopProcess: async (child, options, defaultStopProcess) => {
          child.unref = () => {
            throw disposalError;
          };
          await defaultStopProcess(child, options);
        },
      }),
      (error) => {
        assert.equal(error.code, "lock_cleanup_failed");
        assert.equal(error.cause?.code, "lock_runtime_failed");
        assert.equal(error.cleanupError, disposalError);
        assert.equal(Object.prototype.propertyIsEnumerable.call(error, "cleanupError"), false);
        return true;
      },
    );

    recovered = await acquireAdvisoryLock(path);
  } finally {
    await recovered?.release();
    await rm(root, { recursive: true, force: true });
  }
});

test("acquire preserves an advisory resource error's existing cause", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-advisory-acquire-layered-disposal-"));
  const path = join(root, "authority.lock");
  const underlyingError = new Error("injected underlying disposal failure");
  const disposalError = new AdvisoryLockError(
    "lock_cleanup_failed",
    "injected advisory disposal failure",
  );
  disposalError.cause = underlyingError;
  let recovered;
  try {
    await assert.rejects(
      acquireAdvisoryLock(path, {
        holderPath: FAIL_HOLDER_FIXTURE,
        stopProcess: async (child, options, defaultStopProcess) => {
          child.unref = () => {
            throw disposalError;
          };
          await defaultStopProcess(child, options);
        },
      }),
      (error) => {
        assert.equal(error, disposalError);
        assert.equal(error.cause?.code, "lock_runtime_failed");
        assert.equal(error.cleanupError, underlyingError);
        assert.equal(Object.prototype.propertyIsEnumerable.call(error, "cleanupError"), false);
        return true;
      },
    );

    recovered = await acquireAdvisoryLock(path);
  } finally {
    await recovered?.release();
    await rm(root, { recursive: true, force: true });
  }
});

test("acquire close-only failure preserves acquisition and resource errors", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-advisory-acquire-close-"));
  const path = join(root, "authority.lock");
  const closeError = new Error("injected file handle close failure");
  let closeCalls = 0;
  let recovered;
  try {
    await assert.rejects(
      acquireAdvisoryLock(path, {
        closeFileHandle: async (handle) => {
          closeCalls += 1;
          await handle.close();
          throw closeError;
        },
        holderPath: FAIL_HOLDER_FIXTURE,
      }),
      (error) => {
        assert.equal(error.code, "lock_cleanup_failed");
        assert.equal(error.cause?.code, "lock_runtime_failed");
        assert.equal(error.cleanupError, closeError);
        assert.equal(Object.prototype.propertyIsEnumerable.call(error, "cleanupError"), false);
        return true;
      },
    );
    assert.equal(closeCalls, 1);

    recovered = await acquireAdvisoryLock(path);
  } finally {
    await recovered?.release();
    await rm(root, { recursive: true, force: true });
  }
});

test("failed release disposes handles and a later release retries process-group cleanup", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-advisory-release-retry-"));
  const path = join(root, "authority.lock");
  const probe = failFirstStopProcess({ failFirstUnref: true });
  let lock;
  let recovered;
  try {
    lock = await acquireAdvisoryLock(path, {
      holderArgs: ["release"],
      holderPath: STUBBORN_HOLDER_FIXTURE,
      releaseGraceMs: 20,
      signalGraceMs: 100,
      stopProcess: probe.stopProcess.bind(probe),
      timeoutMs: 5_000,
    });
    let pendingRejections = 0;
    const pendingRename = lock.renameWhileHeld(
      join(root, "ignored-source"),
      join(root, "ignored-destination"),
    );
    void pendingRename.catch(() => {
      pendingRejections += 1;
    });
    const observedLoss = lock.waitForLoss();

    const firstRelease = lock.release();
    const concurrentRelease = lock.release();
    assert.equal(concurrentRelease, firstRelease);
    await within(
      assert.rejects(firstRelease, (error) => error === probe.cleanupError),
      1_000,
    );
    await assert.rejects(
      pendingRename,
      (error) => error instanceof AdvisoryLockError && error.code === "lock_commit_uncertain",
    );
    await new Promise((resolve) => setTimeout(resolve, 50));
    assert.equal(pendingRejections, 1);
    assert.equal(probe.attempts, 1);
    assertProcessReferencesDisposed(probe);
    await assert.rejects(lock.assertHeld(), (error) => error.code === "lock_lost");
    assert.equal((await within(observedLoss, 100)).code, "lock_lost");
    assert.equal((await within(lock.waitForLoss(), 100)).code, "lock_lost");
    await assert.rejects(acquireAdvisoryLock(path), (error) => error.code === "lock_unavailable");

    await lock.release();
    assert.equal(probe.attempts, 2);
    assert.equal(probe.unrefCalls, 2);
    await lock.release();
    assert.equal(probe.attempts, 2);
    lock = undefined;

    recovered = await acquireAdvisoryLock(path);
    await recovered.assertHeld();
  } finally {
    await lock?.release().catch(() => {});
    await recovered?.release();
    await rm(root, { recursive: true, force: true });
  }
});

test("release preserves advisory resource cleanup errors without a self-referential cause", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-advisory-release-resource-retry-"));
  const path = join(root, "authority.lock");
  const underlyingError = new Error("injected underlying unref failure");
  const cleanupError = new AdvisoryLockError(
    "lock_cleanup_failed",
    "injected advisory unref failure",
  );
  cleanupError.cause = underlyingError;
  let unrefCalls = 0;
  let lock;
  let recovered;
  try {
    lock = await acquireAdvisoryLock(path, {
      stopProcess: async (child, options, defaultStopProcess) => {
        const unref = child.unref.bind(child);
        child.unref = () => {
          unrefCalls += 1;
          if (unrefCalls === 1) throw cleanupError;
          return unref();
        };
        await defaultStopProcess(child, options);
      },
    });

    await assert.rejects(lock.release(), (error) => {
      assert.equal(error, cleanupError);
      assert.equal(error.cause, underlyingError);
      assert.notEqual(error.cause, error);
      return true;
    });
    assert.equal(unrefCalls, 1);

    await lock.release();
    assert.equal(unrefCalls, 2);
    lock = undefined;
    recovered = await acquireAdvisoryLock(path);
  } finally {
    await lock?.release().catch(() => {});
    await recovered?.release();
    await rm(root, { recursive: true, force: true });
  }
});

test("uncertain rename cleanup failure disposes references and remains releasable", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-advisory-quiesce-retry-"));
  const path = join(root, "authority.lock");
  const probe = failFirstStopProcess();
  let lock;
  let recovered;
  try {
    lock = await acquireAdvisoryLock(path, {
      holderArgs: ["release"],
      holderPath: STUBBORN_HOLDER_FIXTURE,
      releaseGraceMs: 20,
      signalGraceMs: 100,
      startupTimeoutMs: 5_000,
      stopProcess: probe.stopProcess.bind(probe),
      timeoutMs: 20,
    });
    await within(
      assert.rejects(
        lock.renameWhileHeld(join(root, "source"), join(root, "destination")),
        (error) => {
          assert.equal(error.code, "lock_commit_uncertain");
          assert.equal(error.cause, probe.cleanupError);
          return true;
        },
      ),
      1_000,
    );
    assert.equal(probe.attempts, 1);
    assertProcessReferencesDisposed(probe);
    await assert.rejects(lock.assertHeld(), (error) => error.code === "lock_lost");

    await lock.release();
    assert.equal(probe.attempts, 2);
    lock = undefined;
    recovered = await acquireAdvisoryLock(path);
  } finally {
    await lock?.release().catch(() => {});
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

test("prequeue lock replacement quiesces existing holder commands before exact rejection", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-advisory-rename-replaced-lock-"));
  const path = join(root, "authority.lock");
  const displaced = join(root, "displaced.lock");
  const queuedMarker = join(root, "queued");
  const firstSource = join(root, "first.next");
  const firstDestination = join(root, "first-auth.json");
  const secondSource = join(root, "second.next");
  const secondDestination = join(root, "second-auth.json");
  let lock;
  let replacementLock;
  try {
    await writeFile(firstSource, "first candidate\n", { mode: 0o600 });
    await writeFile(firstDestination, "first canonical\n", { mode: 0o600 });
    await writeFile(secondSource, "second candidate\n", { mode: 0o600 });
    await writeFile(secondDestination, "second canonical\n", { mode: 0o600 });
    lock = await acquireAdvisoryLock(path, {
      holderArgs: [queuedMarker],
      holderPath: QUEUED_DELAYED_HOLDER_FIXTURE,
      signalGraceMs: 100,
      timeoutMs: 2_000,
    });
    let firstSettled = false;
    const first = lock.renameWhileHeld(firstSource, firstDestination).then(
      () => assert.fail("queued rename unexpectedly succeeded"),
      (error) => {
        firstSettled = true;
        return error;
      },
    );
    void first.catch(() => {});
    await waitForPath(queuedMarker);

    await rename(path, displaced);
    await writeFile(path, "replacement\n", { mode: 0o600 });
    replacementLock = await acquireAdvisoryLock(path);
    await replacementLock.assertHeld();

    let replacementError;
    try {
      await lock.renameWhileHeld(secondSource, secondDestination);
    } catch (error) {
      replacementError = error;
    }
    assert(replacementError instanceof AdvisoryLockError);
    assert.equal(replacementError.code, "lock_replaced");
    assert.equal(replacementError.renameOutcome, "not-committed");
    assert.equal(firstSettled, true);
    const firstError = await first;
    assert(firstError instanceof AdvisoryLockError);
    assert.equal(firstError.code, "lock_commit_uncertain");

    await new Promise((resolve) => setTimeout(resolve, 350));
    assert.equal(await readFile(firstSource, "utf8"), "first candidate\n");
    assert.equal(await readFile(firstDestination, "utf8"), "first canonical\n");
    assert.equal(await readFile(secondSource, "utf8"), "second candidate\n");
    assert.equal(await readFile(secondDestination, "utf8"), "second canonical\n");
  } finally {
    await lock?.release().catch(() => {});
    await replacementLock?.release().catch(() => {});
    await rm(root, { recursive: true, force: true });
  }
});

test("holder rejects a lock replacement in the final controlled rename window", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-advisory-holder-path-guard-"));
  const path = join(root, "authority.lock");
  const displaced = join(root, "displaced.lock");
  const readyMarker = join(root, "holder-ready");
  const continueMarker = join(root, "holder-continue");
  const source = join(root, "auth.next");
  const destination = join(root, "auth.json");
  let lock;
  let replacementLock;
  try {
    await writeFile(source, "candidate\n", { mode: 0o600 });
    await writeFile(destination, "canonical\n", { mode: 0o600 });
    lock = await acquireAdvisoryLock(path, {
      holderArgs: [readyMarker, continueMarker],
      holderPath: CONTROLLED_LOCK_PATH_GUARD_FIXTURE,
      signalGraceMs: 100,
      timeoutMs: 2_000,
    });
    const renameAttempt = lock.renameWhileHeld(source, destination);
    void renameAttempt.catch(() => {});
    await waitForPath(readyMarker);

    await rename(path, displaced);
    await writeFile(path, "replacement\n", { mode: 0o600 });
    replacementLock = await acquireAdvisoryLock(path);
    await replacementLock.assertHeld();
    await writeFile(continueMarker, "continue\n", { mode: 0o600 });

    await assert.rejects(renameAttempt, (error) => {
      return (
        error instanceof AdvisoryLockError &&
        error.code === "lock_replaced" &&
        error.renameOutcome === "not-committed"
      );
    });
    assert.equal(await readFile(source, "utf8"), "candidate\n");
    assert.equal(await readFile(destination, "utf8"), "canonical\n");
  } finally {
    await lock?.release().catch(() => {});
    await replacementLock?.release().catch(() => {});
    await rm(root, { recursive: true, force: true });
  }
});

test("holder-local post-rename fence blocks queued mutation before broker quiescence", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-advisory-post-rename-guard-"));
  const path = join(root, "authority.lock");
  const displaced = join(root, "displaced.lock");
  const readyMarker = join(root, "holder-ready");
  const continueMarker = join(root, "holder-continue");
  const queuedMarker = join(root, "holder-queued");
  const latchedMarker = join(root, "holder-latched");
  const source = join(root, "auth.next");
  const destination = join(root, "auth.json");
  const queuedSource = join(root, "queued.next");
  const queuedDestination = join(root, "queued-auth.json");
  const cleanupError = new Error("synthetic post-rename quiescence cleanup failure");
  let stopCalls = 0;
  let lock;
  let replacementLock;
  let recoveredLock;
  try {
    await writeFile(source, "candidate\n", { mode: 0o600 });
    await writeFile(destination, "canonical\n", { mode: 0o600 });
    await writeFile(queuedSource, "queued candidate\n", { mode: 0o600 });
    await writeFile(queuedDestination, "queued canonical\n", { mode: 0o600 });
    lock = await acquireAdvisoryLock(path, {
      holderArgs: [readyMarker, continueMarker, queuedMarker, latchedMarker],
      holderPath: POST_RENAME_LOCK_PATH_GUARD_FIXTURE,
      signalGraceMs: 100,
      stopProcess: async (child, options, defaultStopProcess) => {
        stopCalls += 1;
        await defaultStopProcess(child, options);
        if (stopCalls === 1) throw cleanupError;
      },
      timeoutMs: 2_000,
    });

    let currentRejections = 0;
    const current = lock.renameWhileHeld(source, destination).catch((error) => {
      currentRejections += 1;
      throw error;
    });
    void current.catch(() => {});
    await waitForPath(readyMarker);

    let queuedRejections = 0;
    const queued = lock.renameWhileHeld(queuedSource, queuedDestination).catch((error) => {
      queuedRejections += 1;
      throw error;
    });
    void queued.catch(() => {});
    const observedLoss = lock.waitForLoss();
    await waitForPath(queuedMarker);

    await rename(path, displaced);
    await writeFile(path, "replacement\n", { mode: 0o600 });
    replacementLock = await acquireAdvisoryLock(path);
    await replacementLock.assertHeld();
    await replacementLock.release();
    replacementLock = undefined;
    await rm(path);
    await rename(displaced, path);
    await writeFile(continueMarker, "continue\n", { mode: 0o600 });
    await waitForPath(latchedMarker);

    const [currentError, queuedError, lossError] = await Promise.all([
      current.then(
        () => assert.fail("post-rename command unexpectedly succeeded"),
        (error) => error,
      ),
      queued.then(
        () => assert.fail("queued command unexpectedly succeeded"),
        (error) => error,
      ),
      observedLoss,
    ]);
    for (const error of [currentError, queuedError]) {
      assert(error instanceof AdvisoryLockError);
      assert.equal(error.code, "lock_commit_uncertain");
      assert.equal(error.cause, cleanupError);
    }
    assert.equal(lossError.code, "lock_lost");
    assert.equal(lossError.cause, cleanupError);
    assert.equal(stopCalls, 1);
    await assert.rejects(lock.assertHeld(), (error) => error.code === "lock_lost");

    recoveredLock = await acquireAdvisoryLock(path);
    await recoveredLock.assertHeld();
    assert.equal(await readFile(destination, "utf8"), "candidate\n");
    await assert.rejects(access(source), (error) => error?.code === "ENOENT");
    assert.equal(await readFile(queuedSource, "utf8"), "queued candidate\n");
    assert.equal(await readFile(queuedDestination, "utf8"), "queued canonical\n");

    await new Promise((resolve) => setTimeout(resolve, 100));
    assert.equal(currentRejections, 1);
    assert.equal(queuedRejections, 1);
    assert.equal(await readFile(destination, "utf8"), "candidate\n");
    assert.equal(await readFile(queuedDestination, "utf8"), "queued canonical\n");
  } finally {
    await lock?.release().catch(() => {});
    await replacementLock?.release().catch(() => {});
    await recoveredLock?.release().catch(() => {});
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
