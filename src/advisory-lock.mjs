import { spawn } from "node:child_process";
import { constants } from "node:fs";
import { open } from "node:fs/promises";
import { fileURLToPath } from "node:url";

const HOLDER_PATH = fileURLToPath(new URL("./advisory-lock-holder.mjs", import.meta.url));

export function advisoryLockCommand(platform = process.platform) {
  if (platform === "darwin") {
    return {
      command: "/usr/bin/lockf",
      args: ["-k", "-s", "-t", "0", "-w", "/dev/fd/3", process.execPath, HOLDER_PATH],
    };
  }
  if (platform === "linux") {
    return {
      command: "flock",
      // The command form treats a bare "3" as a path. Opening the inherited
      // descriptor through procfs keeps the secure pre-opened inode while the
      // flock wrapper owns the lock for the lifetime of the holder process.
      args: ["--exclusive", "--nonblock", "/proc/self/fd/3", process.execPath, HOLDER_PATH],
    };
  }
  throw new AdvisoryLockError("unsupported_platform", "advisory auth locks require macOS or Linux");
}

function waitForExit(child, timeoutMs) {
  if (child.exitCode !== null || child.signalCode !== null) return Promise.resolve(true);
  return new Promise((resolve) => {
    const timer = setTimeout(() => resolve(false), timeoutMs);
    child.once("exit", () => {
      clearTimeout(timer);
      resolve(true);
    });
  });
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
  { timeoutMs = 5_000, platform = process.platform } = {},
) {
  const { command, args } = advisoryLockCommand(platform);
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
      const finish = (callback) => {
        if (settled) return;
        settled = true;
        clearTimeout(timer);
        callback();
      };
      const timer = setTimeout(() => {
        child.kill("SIGTERM");
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
              code === 1 || code === 75 ? "lock_unavailable" : "lock_runtime_failed",
              `authority lock was not acquired; stderr omitted (${stderrBytes} bytes)`,
            ),
          ),
        );
      });
      child.stdout.on("data", (chunk) => {
        stdout += chunk.toString("utf8");
        if (stdout.includes("locked\n")) finish(resolve);
      });
    });
  } catch (error) {
    await handle.close().catch(() => {});
    throw error;
  }

  let released = false;
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
    async release() {
      if (released) return;
      released = true;
      child.stdin.end();
      if (!(await waitForExit(child, 2_000))) {
        child.kill("SIGTERM");
        if (!(await waitForExit(child, 2_000))) child.kill("SIGKILL");
      }
      await handle.close();
    },
  };
}
