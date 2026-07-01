import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import { once } from "node:events";
import {
  access,
  link,
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

      // Do not release lostLock yet. On macOS the exited lockf process owned the
      // process-associated fcntl lock; on Linux the holder owned the separate
      // OFD reopened through procfs. In both cases the broker descriptor remains
      // open only as an inode-identity guard and must not prevent reacquisition.
      recovered = await acquireAdvisoryLock(path);
      await recovered.assertHeld();
    } finally {
      await lostLock?.release();
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
