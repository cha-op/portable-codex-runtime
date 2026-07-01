import { spawn } from "node:child_process";
import { constants } from "node:fs";
import { open } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { createInterface } from "node:readline";

const HOLDER_PATH = fileURLToPath(new URL("./advisory-lock-holder.mjs", import.meta.url));

export function advisoryLockCommand(
  platform = process.platform,
  { holderArgs = [], holderPath = HOLDER_PATH } = {},
) {
  if (platform === "darwin") {
    return {
      command: "/usr/bin/lockf",
      args: [
        "-k",
        "-s",
        "-t",
        "0",
        "-w",
        "/dev/fd/3",
        process.execPath,
        holderPath,
        ...holderArgs,
      ],
      conflictExitCode: 75,
    };
  }
  if (platform === "linux") {
    return {
      command: "/usr/bin/flock",
      // The command form treats a bare "3" as a path. On Linux, reopening the
      // inherited regular-file descriptor through procfs creates a separate
      // open file description for the same securely pre-opened inode. `flock`
      // locks that description and `--no-fork` carries it into the holder, so
      // holder exit releases the lock even while the broker keeps its original,
      // unlocked descriptor open as an inode-identity guard.
      args: [
        "--exclusive",
        "--nonblock",
        "--conflict-exit-code",
        "75",
        "--no-fork",
        "/proc/self/fd/3",
        process.execPath,
        holderPath,
        ...holderArgs,
      ],
      conflictExitCode: 75,
    };
  }
  throw new AdvisoryLockError("unsupported_platform", "advisory auth locks require macOS or Linux");
}

function processGroupExists(processGroupId) {
  try {
    process.kill(-processGroupId, 0);
    return true;
  } catch (error) {
    if (error?.code === "ESRCH") return false;
    return true;
  }
}

async function waitForProcessGroupExit(processGroupId, timeoutMs) {
  const deadline = Date.now() + timeoutMs;
  while (processGroupExists(processGroupId)) {
    if (Date.now() >= deadline) return false;
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  return true;
}

function signalProcessGroup(processGroupId, signal) {
  try {
    process.kill(-processGroupId, signal);
  } catch (error) {
    if (error?.code !== "ESRCH") throw error;
  }
}

async function stopLockProcess(child, { initialWaitMs, signalWaitMs }) {
  child.stdin.end();
  if (!child.pid || (await waitForProcessGroupExit(child.pid, initialWaitMs))) return;
  signalProcessGroup(child.pid, "SIGTERM");
  if (await waitForProcessGroupExit(child.pid, signalWaitMs)) return;
  signalProcessGroup(child.pid, "SIGKILL");
  if (!(await waitForProcessGroupExit(child.pid, signalWaitMs))) {
    throw new AdvisoryLockError("lock_cleanup_failed", "authority lock process group survived SIGKILL");
  }
}

export class AdvisoryLockError extends Error {
  constructor(code, message) {
    super(message);
    this.name = "AdvisoryLockError";
    this.code = code;
  }
}

export async function acquireAdvisoryLock(
  lockPath,
  {
    holderArgs = [],
    holderPath = HOLDER_PATH,
    platform = process.platform,
    releaseGraceMs = 2_000,
    signalGraceMs = 2_000,
    timeoutMs = 5_000,
  } = {},
) {
  const { command, args, conflictExitCode } = advisoryLockCommand(platform, {
    holderArgs,
    holderPath,
  });
  if (typeof constants.O_NOFOLLOW !== "number") {
    throw new AdvisoryLockError(
      "unsupported_platform",
      "safe authority locks require O_NOFOLLOW support",
    );
  }

  let handle;
  try {
    handle = await open(
      lockPath,
      constants.O_RDWR | constants.O_CREAT | constants.O_NOFOLLOW | constants.O_NONBLOCK,
      0o600,
    );
    const fileStat = await handle.stat();
    if (!fileStat.isFile() || fileStat.nlink !== 1) {
      throw new AdvisoryLockError(
        "unsafe_lock_file",
        "authority lock must be a regular single-link file",
      );
    }
    await handle.chmod(0o600);
  } catch (error) {
    await handle?.close().catch(() => {});
    if (error instanceof AdvisoryLockError) throw error;
    throw new AdvisoryLockError("unsafe_lock_file", "authority lock file could not be opened safely");
  }

  const child = spawn(command, args, {
    detached: true,
    env: { PATH: process.env.PATH ?? "/usr/bin:/bin" },
    stdio: ["pipe", "pipe", "pipe", handle.fd],
  });
  child.stdin.on("error", () => {});
  let stderrBytes = 0;
  child.stderr.on("data", (chunk) => {
    stderrBytes += chunk.length;
  });

  try {
    await new Promise((resolve, reject) => {
      let settled = false;
      let stdout = "";
      const onAcquireData = (chunk) => {
        stdout += chunk.toString("utf8");
        if (stdout.includes("locked\n")) finish(resolve);
      };
      const finish = (callback) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        child.stdout.off("data", onAcquireData);
        callback();
      };
      const timer = setTimeout(() => {
        finish(() =>
          reject(new AdvisoryLockError("lock_timeout", "timed out acquiring the authority lock")),
        );
      }, timeoutMs);
      child.once("error", () => {
        finish(() =>
          reject(new AdvisoryLockError("lock_runtime_unavailable", "authority lock runtime failed")),
        );
      });
      child.once("exit", (code) => {
        finish(() =>
          reject(
            new AdvisoryLockError(
              code === conflictExitCode ? "lock_unavailable" : "lock_runtime_failed",
              `authority lock was not acquired; stderr omitted (${stderrBytes} bytes)`,
            ),
          ),
        );
      });
      child.stdout.on("data", onAcquireData);
    });
  } catch (error) {
    let cleanupError;
    try {
      await stopLockProcess(child, { initialWaitMs: 0, signalWaitMs: signalGraceMs });
    } catch (failure) {
      cleanupError = failure;
    } finally {
      await handle.close().catch(() => {});
    }
    if (cleanupError) {
      cleanupError.cause = error;
      throw cleanupError;
    }
    throw error;
  }

  let released = false;
  let nextCommandId = 1;
  const pendingCommands = new Map();
  let resolveLoss;
  const loss = new Promise((resolve) => {
    resolveLoss = resolve;
  });
  const output = createInterface({ input: child.stdout });
  let commitQuiescence;
  const quiesceUncertainCommit = async (message) => {
    const uncertain = new AdvisoryLockError("lock_commit_uncertain", message);
    commitQuiescence ??= stopLockProcess(child, {
      initialWaitMs: 0,
      signalWaitMs: signalGraceMs,
    });
    try {
      await commitQuiescence;
    } catch (error) {
      uncertain.cause = error;
    }
    return uncertain;
  };
  output.on("line", (line) => {
    let response;
    try {
      response = JSON.parse(line);
    } catch {
      return;
    }
    const pending = pendingCommands.get(response?.id);
    if (!pending) return;
    pendingCommands.delete(response.id);
    clearTimeout(pending.timer);
    if (response.ok === true) pending.resolve();
    else {
      pending.reject(
        new AdvisoryLockError(
          "lock_commit_failed",
          `lock holder rename failed (${response.code ?? "unknown"})`,
        ),
      );
    }
  });
  child.once("exit", () => {
    resolveLoss(
      new AdvisoryLockError("lock_lost", "authority advisory lock process exited unexpectedly"),
    );
    for (const pending of pendingCommands.values()) {
      clearTimeout(pending.timer);
      pending.reject(
        new AdvisoryLockError(
          "lock_commit_uncertain",
          "lock holder exited before confirming the atomic rename",
        ),
      );
    }
    pendingCommands.clear();
  });

  return {
    async assertHeld() {
      if (released || child.exitCode !== null || child.signalCode !== null) {
        throw new AdvisoryLockError("lock_lost", "authority advisory lock was lost");
      }
      let handleStat;
      let pathHandle;
      try {
        handleStat = await handle.stat();
        pathHandle = await open(
          lockPath,
          constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK,
        );
      } catch {
        throw new AdvisoryLockError("lock_replaced", "authority lock path changed");
      }
      try {
        const pathStat = await pathHandle.stat();
        if (
          !handleStat.isFile() ||
          handleStat.nlink !== 1 ||
          !pathStat.isFile() ||
          pathStat.nlink !== 1 ||
          pathStat.dev !== handleStat.dev ||
          pathStat.ino !== handleStat.ino
        ) {
          throw new AdvisoryLockError("lock_replaced", "authority lock path changed");
        }
      } finally {
        await pathHandle.close().catch(() => {});
      }
    },
    renameWhileHeld(source, destination) {
      if (released || child.exitCode !== null || child.signalCode !== null) {
        return Promise.reject(
          new AdvisoryLockError("lock_commit_uncertain", "authority advisory lock was lost"),
        );
      }
      const id = nextCommandId++;
      return new Promise((resolve, reject) => {
        const timer = setTimeout(() => {
          pendingCommands.delete(id);
          void quiesceUncertainCommit("lock holder did not confirm the atomic rename").then(
            reject,
          );
        }, timeoutMs);
        pendingCommands.set(id, { reject, resolve, timer });
        child.stdin.write(
          `${JSON.stringify({ action: "rename", destination, id, source })}\n`,
          (error) => {
            if (!error) return;
            const pending = pendingCommands.get(id);
            if (!pending) return;
            pendingCommands.delete(id);
            clearTimeout(timer);
            void quiesceUncertainCommit(
              "lock holder command channel failed before confirming rename",
            ).then(reject);
          },
        );
      });
    },
    waitForLoss() {
      if (child.exitCode !== null || child.signalCode !== null) {
        return Promise.resolve(
          new AdvisoryLockError("lock_lost", "authority advisory lock process already exited"),
        );
      }
      return loss;
    },
    async release() {
      if (released) return;
      released = true;
      for (const pending of pendingCommands.values()) {
        clearTimeout(pending.timer);
        pending.reject(
          new AdvisoryLockError(
            "lock_commit_uncertain",
            "lock released before confirming the atomic rename",
          ),
        );
      }
      pendingCommands.clear();
      output.close();
      try {
        await stopLockProcess(child, {
          initialWaitMs: releaseGraceMs,
          signalWaitMs: signalGraceMs,
        });
      } finally {
        await handle.close();
      }
    },
  };
}
