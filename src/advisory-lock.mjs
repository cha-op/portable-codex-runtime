import { spawn } from "node:child_process";
import { fileURLToPath } from "node:url";

const HOLDER_PATH = fileURLToPath(new URL("./advisory-lock-holder.mjs", import.meta.url));

function lockCommand(lockPath) {
  if (process.platform === "darwin") {
    return {
      command: "/usr/bin/lockf",
      args: ["-k", "-s", "-t", "0", "-w", lockPath, process.execPath, HOLDER_PATH],
    };
  }
  if (process.platform === "linux") {
    return {
      command: "flock",
      args: ["--exclusive", "--nonblock", lockPath, process.execPath, HOLDER_PATH],
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

export async function acquireAdvisoryLock(lockPath, { timeoutMs = 5_000 } = {}) {
  const { command, args } = lockCommand(lockPath);
  const child = spawn(command, args, {
    env: { PATH: process.env.PATH ?? "/usr/bin:/bin" },
    stdio: ["pipe", "pipe", "pipe"],
  });
  child.stdin.on("error", () => {});
  let stderrBytes = 0;
  child.stderr.on("data", (chunk) => {
    stderrBytes += chunk.length;
  });

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

  let released = false;
  return {
    assertHeld() {
      if (released || child.exitCode !== null || child.signalCode !== null) {
        throw new AdvisoryLockError("lock_lost", "authority advisory lock was lost");
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
    },
  };
}
