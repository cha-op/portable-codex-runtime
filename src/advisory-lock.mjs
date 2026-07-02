import { spawn } from "node:child_process";
import { constants } from "node:fs";
import { open } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { createInterface } from "node:readline";

import { HOLDER_LOCK_PATH_FLAG } from "./advisory-lock-holder-guard.mjs";

const HOLDER_PATH = fileURLToPath(new URL("./advisory-lock-holder.mjs", import.meta.url));

export function advisoryLockCommand(
  platform = process.platform,
  { holderArgs = [], holderPath = HOLDER_PATH, lockPath } = {},
) {
  if (
    ["darwin", "linux"].includes(platform) &&
    (typeof lockPath !== "string" || lockPath.length === 0)
  ) {
    throw new AdvisoryLockError(
      "unsafe_lock_file",
      "advisory locks require a protected lock path",
    );
  }
  if (platform === "darwin") {
    return {
      command: "/usr/bin/lockf",
      // macOS lockf's fdlock mode would flock the broker-shared descriptor 3,
      // allowing an executor SIGKILL to strand the lock on the broker's open
      // file description. Path mode opens and locks an independent description;
      // the broker descriptor remains only an inode-identity guard.
      args: [
        "-k",
        "-s",
        "-t",
        "0",
        "-w",
        lockPath,
        process.execPath,
        holderPath,
        ...holderArgs,
        HOLDER_LOCK_PATH_FLAG,
        lockPath,
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
        HOLDER_LOCK_PATH_FLAG,
        lockPath,
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
  if (!child.stdin.destroyed) child.stdin.end();
  if (!child.pid || (await waitForProcessGroupExit(child.pid, initialWaitMs))) return;
  signalProcessGroup(child.pid, "SIGTERM");
  if (await waitForProcessGroupExit(child.pid, signalWaitMs)) return;
  signalProcessGroup(child.pid, "SIGKILL");
  if (!(await waitForProcessGroupExit(child.pid, signalWaitMs))) {
    throw new AdvisoryLockError("lock_cleanup_failed", "authority lock process group survived SIGKILL");
  }
}

function disposeLockProcessReferences(child, output) {
  let firstError;
  const attempt = (action) => {
    try {
      action();
    } catch (error) {
      firstError ??= error;
    }
  };

  if (output) attempt(() => output.close());
  for (const stream of [child.stdin, child.stdout, child.stderr]) {
    if (stream && !stream.destroyed) attempt(() => stream.destroy());
  }
  attempt(() => child.unref());
  return firstError;
}

function closeLockHandle(handle) {
  return handle.close();
}

function createHandleCloser(handle, closeFileHandle) {
  let closed = false;
  let closeInFlight;
  return async () => {
    if (closed) return;
    if (!closeInFlight) {
      const attempt = Promise.resolve().then(() => closeFileHandle(handle));
      closeInFlight = attempt;
      try {
        await attempt;
        closed = true;
      } finally {
        if (!closed && closeInFlight === attempt) closeInFlight = undefined;
      }
      return;
    }
    await closeInFlight;
  };
}

function withCause(error, cause) {
  if (cause !== undefined && !Object.hasOwn(error, "cause")) error.cause = cause;
  return error;
}

function withRenameOutcome(error, renameOutcome) {
  if (!Object.hasOwn(error, "renameOutcome")) {
    Object.defineProperty(error, "renameOutcome", {
      configurable: true,
      value: renameOutcome,
    });
  }
  return error;
}

function invalidRenameRequest() {
  return withRenameOutcome(
    new AdvisoryLockError(
      "invalid_rename_request",
      "rename destination precondition is invalid",
    ),
    "not-committed",
  );
}

function normalizeExpectedDestination(value) {
  if (value === undefined) return undefined;
  let keys;
  let prototype;
  try {
    keys = Reflect.ownKeys(value);
    prototype = Object.getPrototypeOf(value);
  } catch {
    throw invalidRenameRequest();
  }
  if (
    value === null ||
    typeof value !== "object" ||
    Array.isArray(value) ||
    ![Object.prototype, null].includes(prototype) ||
    keys.some((key) => typeof key !== "string")
  ) {
    throw invalidRenameRequest();
  }
  const normalized = Object.create(null);
  for (const key of keys) {
    let descriptor;
    try {
      descriptor = Object.getOwnPropertyDescriptor(value, key);
    } catch {
      throw invalidRenameRequest();
    }
    if (descriptor?.enumerable !== true || !Object.hasOwn(descriptor, "value")) {
      throw invalidRenameRequest();
    }
    normalized[key] = descriptor.value;
  }
  const sortedKeys = [...keys].sort();
  if (
    normalized.kind === "absent" &&
    sortedKeys.length === 1 &&
    sortedKeys[0] === "kind"
  ) {
    return Object.freeze({ kind: "absent" });
  }
  if (
    normalized.kind === "present" &&
    sortedKeys.join("\0") === "dev\0ino\0kind" &&
    typeof normalized.dev === "string" &&
    typeof normalized.ino === "string" &&
    /^(?:0|[1-9][0-9]*)$/u.test(normalized.dev) &&
    /^(?:0|[1-9][0-9]*)$/u.test(normalized.ino)
  ) {
    return Object.freeze({
      dev: normalized.dev,
      ino: normalized.ino,
      kind: "present",
    });
  }
  throw invalidRenameRequest();
}

function cleanupResourceError(error, operationError) {
  if (operationError === undefined && error instanceof AdvisoryLockError) return error;
  const failure =
    error instanceof AdvisoryLockError
      ? error
      : new AdvisoryLockError(
          "lock_cleanup_failed",
          "authority lock resources could not be closed",
        );
  if (operationError === undefined) return withCause(failure, error);

  const cleanupError =
    failure === error && Object.hasOwn(failure, "cause") ? failure.cause : error;
  if (cleanupError !== failure && !Object.hasOwn(failure, "cleanupError")) {
    Object.defineProperty(failure, "cleanupError", {
      configurable: true,
      value: cleanupError,
    });
  }
  Object.defineProperty(failure, "cause", {
    configurable: true,
    value: operationError,
    writable: true,
  });
  return failure;
}

function runStopProcess(stopProcess, child, options) {
  return stopProcess(child, options, stopLockProcess);
}

export class AdvisoryLockError extends Error {
  constructor(code, message) {
    super(message);
    this.name = "AdvisoryLockError";
    this.code = code;
  }
}

export function sameFileIdentity(left, right) {
  return (
    typeof left?.dev === "bigint" &&
    typeof left?.ino === "bigint" &&
    typeof right?.dev === "bigint" &&
    typeof right?.ino === "bigint" &&
    left.dev === right.dev &&
    left.ino === right.ino
  );
}

async function assertLockPathIdentity(lockPath, handle) {
  let handleStat;
  let pathHandle;
  try {
    handleStat = await handle.stat({ bigint: true });
    pathHandle = await open(
      lockPath,
      constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK,
    );
  } catch {
    throw new AdvisoryLockError("lock_replaced", "authority lock path changed");
  }
  try {
    const pathStat = await pathHandle.stat({ bigint: true });
    if (
      !handleStat.isFile() ||
      handleStat.nlink !== 1n ||
      !pathStat.isFile() ||
      pathStat.nlink !== 1n ||
      !sameFileIdentity(pathStat, handleStat)
    ) {
      throw new AdvisoryLockError("lock_replaced", "authority lock path changed");
    }
  } finally {
    await pathHandle.close().catch(() => {});
  }
}

export async function acquireAdvisoryLock(
  lockPath,
  {
    closeFileHandle = closeLockHandle,
    holderArgs = [],
    holderPath = HOLDER_PATH,
    platform = process.platform,
    releaseGraceMs = 2_000,
    signalGraceMs = 2_000,
    stopProcess = stopLockProcess,
    timeoutMs = 5_000,
    startupTimeoutMs = timeoutMs,
  } = {},
) {
  const { command, args, conflictExitCode } = advisoryLockCommand(platform, {
    holderArgs,
    holderPath,
    lockPath,
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
    const fileStat = await handle.stat({ bigint: true });
    if (!fileStat.isFile() || fileStat.nlink !== 1n) {
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
  const closeHandle = createHandleCloser(handle, closeFileHandle);

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
      }, startupTimeoutMs);
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
    // macOS path mode opens the lock independently after the secure broker
    // pre-open. Revalidate immediately after the holder proves lock acquisition
    // so any pathname swap fails closed before the lock object is returned.
    await assertLockPathIdentity(lockPath, handle);
  } catch (error) {
    let cleanupError;
    try {
      await runStopProcess(stopProcess, child, {
        initialWaitMs: 0,
        signalWaitMs: signalGraceMs,
      });
    } catch (failure) {
      cleanupError = failure;
    }
    const disposalError = disposeLockProcessReferences(child);
    let closeError;
    try {
      await closeHandle();
    } catch (failure) {
      closeError = failure;
    }
    if (cleanupError) {
      throw withCause(cleanupError, error);
    }
    if (disposalError) throw cleanupResourceError(disposalError, error);
    if (closeError) throw cleanupResourceError(closeError, error);
    throw error;
  }

  let unavailable = false;
  let released = false;
  let releaseInFlight;
  let nextCommandId = 1;
  const pendingCommands = new Map();
  let resolveLoss;
  const loss = new Promise((resolve) => {
    resolveLoss = resolve;
  });
  const output = createInterface({ input: child.stdout });
  let referencesDisposed = false;
  let stopInFlight;
  let stopSucceeded = false;
  const disposeReferences = () => {
    if (referencesDisposed) return undefined;
    const error = disposeLockProcessReferences(child, output);
    if (!error) referencesDisposed = true;
    return error;
  };
  const stopProcessGroup = async (options) => {
    if (stopSucceeded) return;
    if (stopInFlight) {
      await stopInFlight;
      return;
    }
    const attempt = (async () => {
      await runStopProcess(stopProcess, child, options);
      stopSucceeded = true;
    })();
    stopInFlight = attempt;
    try {
      await attempt;
    } finally {
      if (stopInFlight === attempt) stopInFlight = undefined;
    }
  };
  const cleanup = async (options) => {
    let cleanupError;
    try {
      await stopProcessGroup(options);
    } catch (error) {
      cleanupError = error;
    }
    const disposalError = disposeReferences();
    let closeError;
    try {
      await closeHandle();
    } catch (error) {
      closeError = error;
    }

    if (cleanupError) throw cleanupError;
    if (disposalError) throw cleanupResourceError(disposalError);
    if (closeError) throw cleanupResourceError(closeError);
  };
  const rejectPendingCommands = (message, cause) => {
    const pending = [...pendingCommands.values()];
    pendingCommands.clear();
    for (const command of pending) {
      clearTimeout(command.timer);
      command.reject(
        withCause(new AdvisoryLockError("lock_commit_uncertain", message), cause),
      );
    }
  };
  let lossSettled = false;
  let quiescenceAttempted = false;
  let quiescenceError;
  let quiescenceInFlight;
  let quiescenceSucceeded = false;
  const settleQuiescence = ({ cleanupError, lossMessage, pendingMessage }) => {
    if (lossSettled) return;
    lossSettled = true;
    resolveLoss(
      withCause(new AdvisoryLockError("lock_lost", lossMessage), cleanupError),
    );
    rejectPendingCommands(pendingMessage, cleanupError);
  };
  const beginQuiescence = ({
    initialWaitMs,
    lossMessage,
    pendingMessage,
    retry = false,
  }) => {
    unavailable = true;
    if (quiescenceSucceeded) return Promise.resolve({ cleanupError: undefined });
    if (quiescenceInFlight) return quiescenceInFlight;
    if (quiescenceAttempted && !retry) {
      return Promise.resolve({ cleanupError: quiescenceError });
    }
    quiescenceAttempted = true;
    const attempt = (async () => {
      let cleanupError;
      try {
        await cleanup({ initialWaitMs, signalWaitMs: signalGraceMs });
        quiescenceSucceeded = true;
      } catch (error) {
        cleanupError = error;
      }
      quiescenceError = cleanupError;
      settleQuiescence({ cleanupError, lossMessage, pendingMessage });
      return { cleanupError };
    })();
    quiescenceInFlight = attempt;
    void attempt
      .finally(() => {
        if (quiescenceInFlight === attempt) quiescenceInFlight = undefined;
      })
      .catch(() => {});
    return attempt;
  };
  const currentQuiescence = () => {
    if (quiescenceInFlight) return quiescenceInFlight;
    if (quiescenceAttempted) {
      return Promise.resolve({ cleanupError: quiescenceError });
    }
    return undefined;
  };
  const quiesceUncertainCommit = async (message) => {
    const uncertain = new AdvisoryLockError("lock_commit_uncertain", message);
    const { cleanupError } = await beginQuiescence({
      initialWaitMs: 0,
      lossMessage: "authority advisory lock is unavailable",
      pendingMessage: "lock holder quiesced before confirming the atomic rename",
    });
    return withCause(uncertain, cleanupError);
  };
  const beginUnexpectedLoss = () => {
    return beginQuiescence({
      initialWaitMs: 0,
      lossMessage: "authority advisory lock process exited unexpectedly",
      pendingMessage: "lock holder exited before confirming the atomic rename",
    });
  };
  const failLockReplacement = async (error) => {
    const { cleanupError } = await beginQuiescence({
      initialWaitMs: 0,
      lossMessage: "authority advisory lock path changed",
      pendingMessage: "authority lock path changed before confirming the atomic rename",
    });
    throw withCause(error, cleanupError);
  };
  output.on("line", (line) => {
    if (unavailable) return;
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
    else if (response.code === "lock_replaced") {
      void failLockReplacement(
        withRenameOutcome(
          new AdvisoryLockError("lock_replaced", "authority lock path changed"),
          "not-committed",
        ),
      ).then(pending.resolve, pending.reject);
    } else if (response.code === "lock_commit_uncertain") {
      void quiesceUncertainCommit(
        "lock holder detected a lock-path change after the atomic rename",
      ).then(pending.reject, pending.reject);
    } else if (response.code === "destination_changed") {
      pending.reject(
        withRenameOutcome(
          new AdvisoryLockError(
            "destination_changed",
            "rename destination changed before the atomic rename",
          ),
          "not-committed",
        ),
      );
    } else {
      pending.reject(
        new AdvisoryLockError(
          "lock_commit_failed",
          `lock holder rename failed (${response.code ?? "unknown"})`,
        ),
      );
    }
  });
  child.once("exit", () => {
    void beginUnexpectedLoss();
  });

  return {
    async assertHeld() {
      const processExited = child.exitCode !== null || child.signalCode !== null;
      const lossSettlement =
        !unavailable && processExited ? beginUnexpectedLoss() : currentQuiescence();
      if (unavailable || released || child.exitCode !== null || child.signalCode !== null) {
        const outcome = await lossSettlement;
        throw withCause(
          new AdvisoryLockError("lock_lost", "authority advisory lock was lost"),
          outcome?.cleanupError,
        );
      }
      try {
        await assertLockPathIdentity(lockPath, handle);
      } catch (error) {
        if (error instanceof AdvisoryLockError && error.code === "lock_replaced") {
          return failLockReplacement(error);
        }
        throw error;
      }
    },
    async renameWhileHeld(source, destination, expectedDestination) {
      const normalizedExpectedDestination = normalizeExpectedDestination(
        expectedDestination,
      );
      const processExited = child.exitCode !== null || child.signalCode !== null;
      const lossSettlement =
        !unavailable && processExited ? beginUnexpectedLoss() : currentQuiescence();
      if (unavailable || released || child.exitCode !== null || child.signalCode !== null) {
        if (lossSettlement) {
          const { cleanupError } = await lossSettlement;
          throw withCause(
            new AdvisoryLockError(
              "lock_commit_uncertain",
              "authority advisory lock was lost",
            ),
            cleanupError,
          );
        }
        throw new AdvisoryLockError(
          "lock_commit_uncertain",
          "authority advisory lock was lost",
        );
      }

      try {
        await assertLockPathIdentity(lockPath, handle);
      } catch (error) {
        if (error instanceof AdvisoryLockError && error.code === "lock_replaced") {
          return failLockReplacement(withRenameOutcome(error, "not-committed"));
        }
        throw error;
      }

      const processExitedAfterIdentityCheck =
        child.exitCode !== null || child.signalCode !== null;
      const lossSettlementAfterIdentityCheck =
        !unavailable && processExitedAfterIdentityCheck
          ? beginUnexpectedLoss()
          : currentQuiescence();
      if (
        unavailable ||
        released ||
        child.exitCode !== null ||
        child.signalCode !== null
      ) {
        const outcome = await lossSettlementAfterIdentityCheck;
        throw withCause(
          new AdvisoryLockError(
            "lock_commit_uncertain",
            "authority advisory lock was lost",
          ),
          outcome?.cleanupError,
        );
      }

      const id = nextCommandId++;
      const commandLine = `${JSON.stringify({
        action: "rename",
        destination,
        expectedDestination: normalizedExpectedDestination,
        id,
        source,
      })}\n`;
      return new Promise((resolve, reject) => {
        const timer = setTimeout(() => {
          pendingCommands.delete(id);
          void quiesceUncertainCommit("lock holder did not confirm the atomic rename").then(
            reject,
            reject,
          );
        }, timeoutMs);
        pendingCommands.set(id, { reject, resolve, timer });
        const rejectWrite = () => {
          const pending = pendingCommands.get(id);
          if (!pending) return;
          pendingCommands.delete(id);
          clearTimeout(timer);
          void quiesceUncertainCommit(
            "lock holder command channel failed before confirming rename",
          ).then(reject, reject);
        };
        try {
          child.stdin.write(commandLine, (error) => {
            if (error) rejectWrite();
          });
        } catch {
          rejectWrite();
        }
      });
    },
    waitForLoss() {
      if (!unavailable && (child.exitCode !== null || child.signalCode !== null)) {
        beginUnexpectedLoss();
      }
      return loss;
    },
    release() {
      if (released) return Promise.resolve();
      if (releaseInFlight) return releaseInFlight;
      const attempt = (async () => {
        const { cleanupError } = await beginQuiescence({
          initialWaitMs: releaseGraceMs,
          lossMessage: "authority advisory lock is unavailable",
          pendingMessage: "lock released before confirming the atomic rename",
          retry: true,
        });
        if (cleanupError) throw cleanupError;
        released = true;
      })();
      releaseInFlight = attempt;
      void attempt
        .finally(() => {
          if (releaseInFlight === attempt) releaseInFlight = undefined;
        })
        .catch(() => {});
      return attempt;
    },
  };
}
