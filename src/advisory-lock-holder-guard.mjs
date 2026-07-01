import { fstat } from "node:fs";
import { lstat } from "node:fs/promises";

export const HOLDER_LOCK_PATH_FLAG = "--portable-auth-lock-path";

function lockReplacedError() {
  const error = new Error("authority lock path changed");
  error.code = "lock_replaced";
  return error;
}

function statFileDescriptor(fd) {
  return new Promise((resolve, reject) => {
    fstat(fd, { bigint: true }, (error, value) => {
      if (error) reject(error);
      else resolve(value);
    });
  });
}

export function holderLockPath(argv = process.argv.slice(2)) {
  const flagIndex = argv.lastIndexOf(HOLDER_LOCK_PATH_FLAG);
  const lockPath = flagIndex >= 0 ? argv[flagIndex + 1] : undefined;
  if (typeof lockPath !== "string" || lockPath.length === 0) throw lockReplacedError();
  return lockPath;
}

export async function assertInheritedLockPathCurrent(
  lockPath,
  { inheritedFd = 3, statFd = statFileDescriptor, statPath = lstat } = {},
) {
  try {
    const [inheritedStat, pathStat] = await Promise.all([
      statFd(inheritedFd),
      statPath(lockPath, { bigint: true }),
    ]);
    if (
      !inheritedStat.isFile() ||
      inheritedStat.nlink !== 1n ||
      !pathStat.isFile() ||
      pathStat.nlink !== 1n ||
      inheritedStat.dev !== pathStat.dev ||
      inheritedStat.ino !== pathStat.ino
    ) {
      throw lockReplacedError();
    }
  } catch {
    throw lockReplacedError();
  }
}
