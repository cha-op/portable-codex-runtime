import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import { createHash } from "node:crypto";
import { constants as fsConstants, mkdirSync } from "node:fs";
import {
  access,
  chmod,
  lstat,
  open,
  readlink,
  readdir,
  readFile,
  realpath,
  rm,
  symlink,
} from "node:fs/promises";
import { platform } from "node:os";
import {
  basename,
  dirname,
  isAbsolute,
  join,
  parse,
  relative,
  resolve,
  sep,
} from "node:path";
import { promisify } from "node:util";

const MAX_SYMLINK_RESOLUTION_DEPTH = 32;
const execFileAsync = promisify(execFile);

async function runSequentialCleanup(cleanups, primaryFailure) {
  let firstCleanupFailure;
  for (const cleanup of cleanups) {
    try {
      await cleanup();
    } catch (error) {
      firstCleanupFailure ??= { error };
    }
  }
  if (primaryFailure) {
    const primaryError = primaryFailure.error;
    if (
      firstCleanupFailure &&
      primaryError !== null &&
      ["object", "function"].includes(typeof primaryError)
    ) {
      try {
        if (!Object.hasOwn(primaryError, "cleanupError")) {
          Object.defineProperty(primaryError, "cleanupError", {
            configurable: true,
            enumerable: false,
            value: firstCleanupFailure.error,
          });
        }
      } catch {
        // A frozen primary error still takes precedence over cleanup diagnostics.
      }
    }
    return;
  }
  if (firstCleanupFailure) throw firstCleanupFailure.error;
}

function integerAsBigInt(value) {
  if (typeof value === "bigint") return value;
  return Number.isSafeInteger(value) ? BigInt(value) : null;
}

function authorityDirectoryPermissionsAreSafe(
  { isDirectory, mode, uid },
  {
    allowRootOwner = false,
    allowStickyShared = false,
    brokerUid,
    childUid,
    disallowedModeBits,
    requiredModeBits = 0,
  },
) {
  const normalizedMode = integerAsBigInt(mode);
  const normalizedUid = integerAsBigInt(uid);
  const normalizedBrokerUid = integerAsBigInt(brokerUid);
  const normalizedChildUid = integerAsBigInt(childUid);
  const normalizedDisallowedModeBits = integerAsBigInt(disallowedModeBits);
  const normalizedRequiredModeBits = integerAsBigInt(requiredModeBits);
  if (
    normalizedMode === null ||
    normalizedUid === null ||
    (brokerUid !== null && normalizedBrokerUid === null) ||
    normalizedDisallowedModeBits === null ||
    normalizedRequiredModeBits === null
  ) {
    return false;
  }
  const ownerIsTrusted =
    brokerUid === null ||
    normalizedUid === normalizedBrokerUid ||
    (allowRootOwner && normalizedUid === 0n);
  const childOwnerIsTrusted =
    brokerUid === null ||
    normalizedChildUid === normalizedBrokerUid ||
    (allowRootOwner && normalizedChildUid === 0n);
  const hasDisallowedWrite = (normalizedMode & normalizedDisallowedModeBits) !== 0n;
  const stickyProtectsTrustedChild =
    allowStickyShared && (normalizedMode & 0o1000n) !== 0n && childOwnerIsTrusted;
  return (
    isDirectory === true &&
    ownerIsTrusted &&
    (normalizedMode & normalizedRequiredModeBits) === normalizedRequiredModeBits &&
    (!hasDisallowedWrite || stickyProtectsTrustedChild)
  );
}

function runAclListing(binary, args, options) {
  return new Promise((resolve, reject) => {
    execFile(binary, args, options, (error, stdout, stderr) => {
      if (error) reject(error);
      else resolve({ stderr, stdout });
    });
  });
}

function parseAclListingDisposition(stdout, currentPlatform = process.platform) {
  if (typeof stdout !== "string" || !["darwin", "linux"].includes(currentPlatform)) {
    throw new Error("extended ACL listing could not be parsed");
  }
  const lines = stdout.split(/\r?\n/u);
  if (lines.at(-1) === "") lines.pop();
  const mode = lines[0]?.match(/^(\S+)/u)?.[1];
  const match = mode?.match(
    /^[bcdlps-][r-][w-][xSs-][r-][w-][xSs-][r-][w-][xTt-]([+@.]?)$/u,
  );
  if (!match) throw new Error("extended ACL listing could not be parsed");
  const marker = match[1];
  const detailLines = lines.slice(1).filter((line) => line.length > 0);
  if (currentPlatform === "linux") {
    if (!["", ".", "+"].includes(marker) || detailLines.length > 0) {
      throw new Error("extended ACL listing could not be parsed");
    }
    return marker === "+" ? "allow-or-unknown" : "none";
  }
  if (!["", "@", "+"].includes(marker)) {
    throw new Error("extended ACL listing could not be parsed");
  }
  if (detailLines.length === 0) {
    return marker === "+" ? "allow-or-unknown" : "none";
  }
  const actions = detailLines.map(
    (line) => line.match(/^\s+\d+:\s+.+\s+(allow|deny)\s+\S.*$/u)?.[1],
  );
  if (actions.some((action) => action === undefined)) {
    throw new Error("extended ACL listing could not be parsed");
  }
  return actions.every((action) => action === "deny") ? "deny-only" : "allow-or-unknown";
}

async function inspectPathAclDisposition(
  path,
  { platform: currentPlatform = process.platform, runCommand = runAclListing } = {},
) {
  try {
    if (typeof path !== "string" || !isAbsolute(path)) throw new Error("invalid path");
    const flags =
      currentPlatform === "darwin" ? ["-l", "-e", "-d", "-b"] : ["-l", "-d", "-b"];
    if (!["darwin", "linux"].includes(currentPlatform)) {
      throw new Error("unsupported platform");
    }
    const { stderr, stdout } = await runCommand("/bin/ls", [...flags, path], {
      encoding: "utf8",
      env: { ...process.env, LANG: "C", LC_ALL: "C" },
      maxBuffer: 64 * 1024,
    });
    if (stderr !== "") throw new Error("unexpected ACL listing diagnostics");
    return parseAclListingDisposition(stdout, currentPlatform);
  } catch {
    throw new Error("extended ACL inspection failed");
  }
}

async function pathHasExtendedAcl(path, options) {
  return (await inspectPathAclDisposition(path, options)) !== "none";
}

async function pathHasUnsafeAncestorAcl(path, options) {
  return (await inspectPathAclDisposition(path, options)) === "allow-or-unknown";
}

async function assertDirectOwnedPath(ownedRootAuthority, candidate, label, { mustExist }) {
  await ownedRootAuthority.assertCurrent();
  const canonicalRoot = ownedRootAuthority.path;
  const canonicalParent = await realpath(dirname(candidate));
  assert.equal(canonicalParent, canonicalRoot, `${label} must be a direct owned child`);
  const candidateName = basename(candidate);
  assert(
    candidateName !== "." && candidateName !== "..",
    `${label} must be a direct owned child`,
  );
  const canonicalCandidate = join(canonicalParent, candidateName);
  assert.equal(
    dirname(canonicalCandidate),
    canonicalRoot,
    `${label} must be a direct owned child`,
  );
  try {
    const metadata = await lstat(canonicalCandidate);
    if (!mustExist) throw new Error(`${label} already exists`);
    assert(metadata.isDirectory(), `${label} must be a directory`);
    assert(!metadata.isSymbolicLink(), `${label} must not be a symlink`);
  } catch (error) {
    if (!mustExist && error?.code === "ENOENT") {
      await ownedRootAuthority.assertCurrent();
      return canonicalCandidate;
    }
    throw error;
  }
  await ownedRootAuthority.assertCurrent();
  return canonicalCandidate;
}

function stoppedTreeAncestorPermissionsAreSafe(metadata, childUid, currentUid) {
  return authorityDirectoryPermissionsAreSafe(
    {
      isDirectory: metadata.isDirectory(),
      mode: metadata.mode,
      uid: metadata.uid,
    },
    {
      allowRootOwner: true,
      allowStickyShared: true,
      brokerUid: currentUid,
      childUid,
      disallowedModeBits: 0o022,
    },
  );
}

async function inspectStoppedTreeAcl(inspector, path, invalidMessage, unsafeMessage) {
  let unsafe;
  try {
    unsafe = await inspector(path);
  } catch {
    throw new Error(invalidMessage);
  }
  assert.equal(unsafe, false, unsafeMessage);
}

async function openPrivateOwnedRootAuthority(
  ownedRoot,
  {
    inspectAncestorAcl = recoveryPathHasUnsafeAncestorAcl,
    inspectRootAcl = recoveryPathHasExtendedAcl,
  } = {},
) {
  const requestedRoot = resolve(ownedRoot);
  const metadata = await lstat(requestedRoot, { bigint: true });
  const currentUid = process.geteuid?.() ?? process.getuid?.();
  assert.notEqual(currentUid, undefined, "stopped-tree copy requires a POSIX owner identity");
  assert(
    metadata.isDirectory() && !metadata.isSymbolicLink(),
    "stopped-tree owned root must be a directory",
  );
  assert.equal(
    metadata.uid,
    BigInt(currentUid),
    "stopped-tree owned root must be owned by this user",
  );
  assert.equal(
    Number(metadata.mode & 0o777n),
    0o700,
    "stopped-tree owned root must have mode 0700",
  );
  const canonicalRoot = await realpath(requestedRoot);
  await assertPathIdentity(
    requestedRoot,
    metadata,
    "stopped-tree copy rejects owned-root identity changes",
  );
  const handle = await open(
    canonicalRoot,
    fsConstants.O_RDONLY | fsConstants.O_DIRECTORY | fsConstants.O_NOFOLLOW,
  );
  const ancestors = [];
  let primaryFailure;
  try {
    const heldIdentity = await handle.stat({ bigint: true });
    assert(
      heldIdentity.isDirectory() && sameFileIdentity(metadata, heldIdentity),
      "stopped-tree copy rejects owned-root identity changes",
    );
    await inspectStoppedTreeAcl(
      inspectRootAcl,
      canonicalRoot,
      "stopped-tree owned root ACL could not be validated",
      "stopped-tree owned root must not have extended access controls",
    );

    let childUid = metadata.uid;
    let ancestorPath = dirname(canonicalRoot);
    while (true) {
      const ancestorIdentity = await lstat(ancestorPath, { bigint: true });
      assert(
        stoppedTreeAncestorPermissionsAreSafe(ancestorIdentity, childUid, currentUid),
        "stopped-tree owned root ancestor chain is not trusted",
      );
      await inspectStoppedTreeAcl(
        inspectAncestorAcl,
        ancestorPath,
        "stopped-tree owned root ancestor ACL could not be validated",
        "stopped-tree owned root ancestor chain has unsafe access controls",
      );
      ancestors.push({ identity: ancestorIdentity, path: ancestorPath });
      const parent = dirname(ancestorPath);
      if (parent === ancestorPath) break;
      childUid = ancestorIdentity.uid;
      ancestorPath = parent;
    }

    const authority = {
      ancestors,
      currentUid,
      handle,
      identity: metadata,
      inspectAncestorAcl,
      inspectRootAcl,
      path: canonicalRoot,
    };
    authority.assertCurrent = async () => {
      const [current, held] = await Promise.all([
        lstat(authority.path, { bigint: true }),
        authority.handle.stat({ bigint: true }),
      ]);
      assert(
        current.isDirectory() &&
          sameFileIdentity(current, authority.identity) &&
          sameFileIdentity(held, authority.identity) &&
          current.uid === BigInt(authority.currentUid) &&
          Number(current.mode & 0o777n) === 0o700,
        "stopped-tree copy rejects owned-root identity or permission changes",
      );
      await inspectStoppedTreeAcl(
        authority.inspectRootAcl,
        authority.path,
        "stopped-tree owned root ACL could not be validated",
        "stopped-tree owned root must not have extended access controls",
      );
      let currentChildUid = current.uid;
      for (const ancestor of authority.ancestors) {
        const currentAncestor = await lstat(ancestor.path, { bigint: true });
        assert(
          sameFileIdentity(currentAncestor, ancestor.identity) &&
            stoppedTreeAncestorPermissionsAreSafe(
              currentAncestor,
              currentChildUid,
              authority.currentUid,
            ),
          "stopped-tree owned root ancestor identity or permissions changed",
        );
        await inspectStoppedTreeAcl(
          authority.inspectAncestorAcl,
          ancestor.path,
          "stopped-tree owned root ancestor ACL could not be validated",
          "stopped-tree owned root ancestor chain has unsafe access controls",
        );
        currentChildUid = currentAncestor.uid;
      }
    };
    await authority.assertCurrent();
    return authority;
  } catch (error) {
    primaryFailure = { error };
    throw error;
  } finally {
    if (primaryFailure) await runSequentialCleanup([() => handle.close()], primaryFailure);
  }
}

/**
 * Opens and pins a private stopped-tree root. The caller owns the returned
 * authority handle and must close `authority.handle`.
 */
export async function openStoppedTreeRootAuthority(
  ownedRoot,
  {
    inspectAncestorAcl,
    inspectOwnedRootAcl,
    inspectOwnedRootAncestorAcl,
    inspectRootAcl,
  } = {},
) {
  const authority = await openPrivateOwnedRootAuthority(ownedRoot, {
    inspectAncestorAcl:
      inspectOwnedRootAncestorAcl ??
      inspectAncestorAcl ??
      recoveryPathHasUnsafeAncestorAcl,
    inspectRootAcl:
      inspectOwnedRootAcl ?? inspectRootAcl ?? recoveryPathHasExtendedAcl,
  });
  return Object.freeze({
    assertCurrent: authority.assertCurrent,
    handle: authority.handle,
    identity: Object.freeze({
      dev: authority.identity.dev,
      ino: authority.identity.ino,
    }),
    path: authority.path,
  });
}

function pathIsInside(root, candidate) {
  const child = relative(root, candidate);
  return child === "" || (child !== ".." && !child.startsWith(`..${sep}`) && !isAbsolute(child));
}

function decodeMountPath(value) {
  return value.replace(/\\([0-7]{3})/g, (_match, digits) =>
    String.fromCharCode(Number.parseInt(digits, 8)),
  );
}

function decodeMountTableText(value, label) {
  assert(Buffer.isBuffer(value), `${label} must be bytes`);
  const decoded = value.toString("utf8");
  assert(
    Buffer.from(decoded, "utf8").equals(value),
    `${label} contains non-UTF-8 bytes`,
  );
  return decoded;
}

export function parseLinuxMountInfo(value) {
  const mountPoints = decodeMountTableText(value, "Linux mountinfo")
    .split("\n")
    .filter((line) => line !== "")
    .map((line) => {
      const fields = line.split(" ");
      assert(fields.length >= 10 && fields.includes("-"), "Linux mountinfo is malformed");
      const mountPoint = decodeMountPath(fields[4]);
      assert(isAbsolute(mountPoint), "Linux mountinfo contains a non-absolute path");
      return mountPoint;
    });
  assert(mountPoints.includes("/"), "Linux mountinfo omitted the root mount");
  return mountPoints;
}

export function parseDarwinMountTable(value) {
  const mountPoints = decodeMountTableText(value, "Darwin mount table")
    .split("\n")
    .filter((line) => line !== "")
    .map((line) => {
      const mountSeparator = " on ";
      const mountSeparatorIndex = line.indexOf(mountSeparator);
      assert(
        mountSeparatorIndex > 0 && mountSeparatorIndex === line.lastIndexOf(mountSeparator),
        "Darwin mount table contains an ambiguous mount path",
      );
      const mountPointStart = mountSeparatorIndex + mountSeparator.length;
      const optionsSeparator = " (";
      const optionsSeparatorIndex = line.indexOf(optionsSeparator, mountPointStart);
      assert(
        optionsSeparatorIndex > mountPointStart &&
          optionsSeparatorIndex === line.lastIndexOf(optionsSeparator) &&
          line.endsWith(")"),
        "Darwin mount table contains an ambiguous mount path",
      );
      // macOS mount(8) prints f_mntonname directly rather than applying fstab
      // octal escaping. Preserve the bytes represented by Node's UTF-8 string
      // and fail closed above when its textual separators are ambiguous.
      const mountPoint = line.slice(mountPointStart, optionsSeparatorIndex);
      assert(isAbsolute(mountPoint), "Darwin mount table contains a non-absolute path");
      return mountPoint;
    });
  assert(mountPoints.includes("/"), "Darwin mount table omitted the root mount");
  return mountPoints;
}

async function listCurrentMountPoints({
  currentPlatform = process.platform,
  readMountInfo = readFile,
  runMount = execFileAsync,
} = {}) {
  try {
    if (currentPlatform === "linux") {
      return parseLinuxMountInfo(await readMountInfo("/proc/self/mountinfo"));
    }
    if (currentPlatform === "darwin") {
      const { stderr, stdout } = await runMount("/sbin/mount", [], {
        encoding: "buffer",
        env: { ...process.env, LANG: "C", LC_ALL: "C" },
        maxBuffer: 1024 * 1024,
      });
      if (Buffer.isBuffer(stderr) ? stderr.length !== 0 : stderr !== "") {
        throw new Error("unexpected mount diagnostics");
      }
      return parseDarwinMountTable(stdout);
    }
    throw new Error("unsupported platform");
  } catch {
    throw new Error("portable tree mount boundary inspection failed");
  }
}

async function assertNoMountBoundary(
  path,
  listMountPoints,
  { allowRootMount = false } = {},
) {
  assert.equal(typeof allowRootMount, "boolean", "allowRootMount must be a boolean");
  const root = await realpath(path);
  let mountPoints;
  try {
    mountPoints = await listMountPoints();
  } catch {
    throw new Error("portable tree mount boundary inspection failed");
  }
  assert(Array.isArray(mountPoints), "portable tree mount boundary inspection failed");
  for (const mountPoint of mountPoints) {
    assert.equal(typeof mountPoint, "string", "portable tree mount boundary inspection failed");
    let candidate;
    try {
      candidate = await realpath(mountPoint);
    } catch {
      candidate = resolve(mountPoint);
    }
    if (
      (!allowRootMount && candidate === root) ||
      (candidate !== root && pathIsInside(root, candidate))
    ) {
      throw new Error("portable tree rejects nested mount points");
    }
  }
}

export function portableMode(metadata) {
  const mode = typeof metadata.mode === "bigint" ? Number(metadata.mode) : metadata.mode;
  if ((mode & 0o7000) !== 0) {
    throw new Error("portable tree rejects special permission bits");
  }
  return mode & 0o777;
}

export function sameFileIdentity(left, right) {
  return left.dev === right.dev && left.ino === right.ino;
}

function fileIdentityKey(metadata) {
  return `${metadata.dev}:${metadata.ino}`;
}

function assertStableFileMetadata(before, after, message) {
  assert(
    sameFileIdentity(before, after) &&
      before.size === after.size &&
      before.mtimeNs === after.mtimeNs &&
      before.ctimeNs === after.ctimeNs,
    message,
  );
}

async function assertPathIdentity(path, expected, message) {
  let current;
  try {
    current = await lstat(path, { bigint: true });
  } catch {
    throw new Error(message);
  }
  assert(sameFileIdentity(current, expected), message);
  return current;
}

async function collectTreeIdentities(path, identities = new Set()) {
  const metadata = await lstat(path, { bigint: true });
  identities.add(fileIdentityKey(metadata));
  if (!metadata.isDirectory()) {
    await assertPathIdentity(
      path,
      metadata,
      "stopped-tree copy rejects source entry identity changes",
    );
    return identities;
  }

  const handle = await open(
    path,
    fsConstants.O_RDONLY | fsConstants.O_DIRECTORY | fsConstants.O_NOFOLLOW,
  );
  let primaryFailure;
  try {
    const heldMetadata = await handle.stat({ bigint: true });
    assert(
      heldMetadata.isDirectory() && sameFileIdentity(metadata, heldMetadata),
      "stopped-tree copy rejects source directory identity changes",
    );
    const entries = await readPortableDirectory(path);
    await assertPathIdentity(
      path,
      heldMetadata,
      "stopped-tree copy rejects source directory identity changes",
    );
    for (const entry of entries) {
      await collectTreeIdentities(join(path, entry), identities);
    }
    assertStableFileMetadata(
      heldMetadata,
      await handle.stat({ bigint: true }),
      "stopped-tree copy rejects source directory metadata changes",
    );
    await assertPathIdentity(
      path,
      heldMetadata,
      "stopped-tree copy rejects source directory identity changes",
    );
    return identities;
  } catch (error) {
    primaryFailure = { error };
    throw error;
  } finally {
    await runSequentialCleanup([() => handle.close()], primaryFailure);
  }
}

async function copyFileContents(sourceHandle, destinationHandle) {
  const buffer = Buffer.allocUnsafe(1024 * 1024);
  let position = 0;
  while (true) {
    const { bytesRead } = await sourceHandle.read(buffer, 0, buffer.length, position);
    if (bytesRead === 0) return;
    let written = 0;
    while (written < bytesRead) {
      const result = await destinationHandle.write(
        buffer,
        written,
        bytesRead - written,
        position + written,
      );
      written += result.bytesWritten;
    }
    position += bytesRead;
  }
}

async function digestFileHandle(handle) {
  const hash = createHash("sha256");
  const buffer = Buffer.allocUnsafe(1024 * 1024);
  let position = 0;
  while (true) {
    const { bytesRead } = await handle.read(buffer, 0, buffer.length, position);
    if (bytesRead === 0) return hash.digest("hex");
    hash.update(buffer.subarray(0, bytesRead));
    position += bytesRead;
  }
}

function mkdirPrivate(path) {
  const previousUmask = process.umask(0);
  try {
    mkdirSync(path, { mode: 0o700 });
  } finally {
    process.umask(previousUmask);
  }
}

async function assertSnapshotUserAccess(path, mode, checkAccess = access) {
  try {
    await checkAccess(path, mode);
  } catch (error) {
    if (error?.code === "EACCES" || error?.code === "EPERM") {
      throw new Error("portable tree rejects entries inaccessible to the snapshot user");
    }
    throw error;
  }
}

async function readPortableSymlink(path) {
  const bytes = await readlink(path, { encoding: "buffer" });
  const target = bytes.toString("utf8");
  if (!Buffer.from(target, "utf8").equals(bytes)) {
    throw new Error("stopped-tree copy rejects non-UTF-8 symlink targets");
  }
  return { bytes, target };
}

export function decodePortablePathBytes(bytes) {
  assert(Buffer.isBuffer(bytes), "portable path bytes must be a Buffer");
  const value = bytes.toString("utf8");
  if (!Buffer.from(value, "utf8").equals(bytes)) {
    throw new Error("portable tree rejects non-UTF-8 directory entry names");
  }
  return value;
}

export function assertPortableDirectoryNames(entries) {
  const portableKeys = new Set();
  for (const entry of entries) {
    assert.equal(typeof entry, "string", "portable directory entries must be strings");
    const normalized = entry.normalize("NFC");
    if (entry !== normalized) {
      throw new Error("portable tree rejects non-NFC directory names");
    }
    for (const value of [entry, normalized]) {
      for (const character of value) {
        if (
          character.codePointAt(0) > 0x7f &&
          (character.toLowerCase() !== character || character.toUpperCase() !== character)
        ) {
          throw new Error("portable tree rejects non-ASCII cased directory names");
        }
      }
    }
    const portableKey = normalized.toLowerCase();
    if (portableKeys.has(portableKey)) {
      throw new Error(
        "portable tree rejects case or Unicode-normalization name collisions",
      );
    }
    portableKeys.add(portableKey);
  }
  return [...entries].sort();
}

async function readPortableDirectory(path) {
  const entries = await readdir(path, { encoding: "buffer" });
  return assertPortableDirectoryNames(entries.map(decodePortablePathBytes));
}

function rawChildPath(parent, entry) {
  const parentBytes = Buffer.isBuffer(parent) ? parent : Buffer.from(parent);
  return Buffer.concat([parentBytes, Buffer.from(sep), entry]);
}

function pathComponents(path) {
  return path.split(sep);
}

async function resolveSymlinkTargetWithoutHiddenTraversal({
  exactPortableNames,
  inspectMetadata = lstat,
  start,
  target,
  validateCandidate,
  validateMetadata = async () => {},
  danglingMessage,
  nonDirectoryMessage,
}) {
  let current = start;
  let currentIsDirectory = true;
  // The source entry being validated is itself the first symlink traversal.
  let followedSymlinks = 1;
  const pending = pathComponents(target);

  while (pending.length > 0) {
    const component = pending.shift();
    if (component === "" || component === ".") {
      if (!currentIsDirectory) {
        throw new Error(nonDirectoryMessage);
      }
      continue;
    }
    if (component === "..") {
      if (!currentIsDirectory) {
        throw new Error(nonDirectoryMessage);
      }
      current = dirname(current);
      validateCandidate(current);
      continue;
    }
    if (!currentIsDirectory) {
      throw new Error(nonDirectoryMessage);
    }

    if (exactPortableNames) {
      const entries = await readPortableDirectory(current);
      if (!entries.includes(component)) {
        const portableComponent = component.normalize("NFC").toLowerCase();
        if (
          entries.some((entry) => entry.normalize("NFC").toLowerCase() === portableComponent)
        ) {
          throw new Error(
            "stopped-tree copy rejects relative symlink case or normalization aliases",
          );
        }
        throw new Error(danglingMessage);
      }
    }

    const candidate = join(current, component);
    validateCandidate(candidate);
    let metadata;
    try {
      metadata = await inspectMetadata(candidate, { bigint: true });
    } catch (error) {
      if (error?.code === "ENOENT") throw new Error(danglingMessage);
      throw error;
    }
    await validateMetadata(candidate, metadata);
    if (!metadata.isSymbolicLink()) {
      current = candidate;
      currentIsDirectory = metadata.isDirectory();
      continue;
    }

    followedSymlinks += 1;
    if (followedSymlinks > MAX_SYMLINK_RESOLUTION_DEPTH) {
      throw new Error("stopped-tree copy rejects excessive symlink resolution depth");
    }
    const { target: nestedTarget } = await readPortableSymlink(candidate);
    await assertPathIdentity(
      candidate,
      metadata,
      "stopped-tree copy rejects symlink resolution identity changes",
    );
    if (isAbsolute(nestedTarget)) {
      const root = parse(nestedTarget).root;
      current = root;
      currentIsDirectory = true;
      validateCandidate(current);
      pending.unshift(...pathComponents(nestedTarget.slice(root.length)));
    } else {
      pending.unshift(...pathComponents(nestedTarget));
    }
  }
  return current;
}

async function assertPortableSymlink({
  allowAbsoluteSymlinks,
  destination,
  destinationRoots,
  destinationRootIdentity,
  forbiddenAbsoluteSymlinkAuthorities,
  inspectSymlinkPath,
  source,
  sourceIdentityKeys,
  sourceRootIdentity,
  sourceRoots,
  target,
}) {
  if (isAbsolute(target)) {
    if (!allowAbsoluteSymlinks) {
      throw new Error("stopped-tree copy rejects absolute symlinks by policy");
    }
    const root = parse(target).root;
    await resolveSymlinkTargetWithoutHiddenTraversal({
      exactPortableNames: false,
      inspectMetadata: inspectSymlinkPath,
      start: root,
      target: target.slice(root.length),
      danglingMessage: "stopped-tree copy rejects dangling absolute symlinks",
      nonDirectoryMessage: "stopped-tree copy rejects absolute symlinks through non-directories",
      validateCandidate: (candidate) => {
        if (sourceRoots.some((sourceRoot) => pathIsInside(sourceRoot, candidate))) {
          throw new Error("stopped-tree copy rejects absolute symlinks into the source tree");
        }
        if (
          destinationRoots.some((destinationRoot) => pathIsInside(destinationRoot, candidate))
        ) {
          throw new Error(
            "stopped-tree copy rejects absolute symlinks into the destination tree",
          );
        }
        if (
          forbiddenAbsoluteSymlinkAuthorities.some((authority) =>
            pathIsInside(authority.path, candidate),
          )
        ) {
          throw new Error(
            "stopped-tree copy rejects absolute symlinks into a forbidden authority",
          );
        }
      },
      validateMetadata: async (_candidate, metadata) => {
        if (
          sourceIdentityKeys.has(fileIdentityKey(metadata)) ||
          sameFileIdentity(metadata, sourceRootIdentity)
        ) {
          throw new Error("stopped-tree copy rejects absolute symlinks into the source tree");
        }
        if (sameFileIdentity(metadata, destinationRootIdentity)) {
          throw new Error(
            "stopped-tree copy rejects absolute symlinks into the destination tree",
          );
        }
        if (
          forbiddenAbsoluteSymlinkAuthorities.some((authority) =>
            sameFileIdentity(metadata, authority.identity),
          )
        ) {
          throw new Error(
            "stopped-tree copy rejects absolute symlinks into a forbidden authority",
          );
        }
      },
    });
    return;
  }

  const sourceTarget = resolve(dirname(source), target);
  const destinationTarget = resolve(dirname(destination), target);
  const lexicalSourceRoot = sourceRoots[0];
  const lexicalDestinationRoot = destinationRoots[0];
  if (
    !pathIsInside(lexicalSourceRoot, sourceTarget) ||
    !pathIsInside(lexicalDestinationRoot, destinationTarget) ||
    relative(lexicalSourceRoot, sourceTarget) !==
      relative(lexicalDestinationRoot, destinationTarget)
  ) {
    throw new Error("stopped-tree copy rejects non-relocatable relative symlinks");
  }

  const current = await realpath(dirname(source));
  const validateCandidate = (candidate) => {
    if (!pathIsInside(sourceRoots[1], candidate)) {
      throw new Error("stopped-tree copy rejects relative symlinks outside the source tree");
    }
  };
  validateCandidate(current);
  await resolveSymlinkTargetWithoutHiddenTraversal({
    exactPortableNames: true,
    start: current,
    target,
    danglingMessage: "stopped-tree copy rejects dangling relative symlinks",
    nonDirectoryMessage: "stopped-tree copy rejects relative symlinks through non-directories",
    validateCandidate,
  });
}

async function copyTreeEntry(
  context,
  source,
  destination,
  {
    assertDestinationParentCurrent = context.assertDestinationRootCurrent,
    destinationDirectoryCreated = false,
    destinationDirectoryHandle: providedDestinationDirectoryHandle,
    destinationDirectoryIdentity: providedDestinationDirectoryIdentity,
    expectedSourceIdentity,
  } = {},
) {
  const metadata = await lstat(source, { bigint: true });
  assert(
    context.sourceIdentityKeys.has(fileIdentityKey(metadata)),
    "stopped-tree copy rejects source entry identity changes",
  );
  if (expectedSourceIdentity) {
    assert(
      metadata.isDirectory() && sameFileIdentity(metadata, expectedSourceIdentity),
      "stopped-tree copy rejects source root identity changes",
    );
  }
  if (metadata.isSymbolicLink()) {
    if (metadata.nlink !== 1n) {
      throw new Error("stopped-tree copy rejects hard-linked symlinks");
    }
    const { target } = await readPortableSymlink(source);
    await assertPathIdentity(
      source,
      metadata,
      "stopped-tree copy rejects source symlink identity changes",
    );
    await assertPortableSymlink({ ...context, source, destination, target });
    await assertDestinationParentCurrent();
    await context.afterSourceSymlinkValidated?.(source);
    await assertPathIdentity(
      source,
      metadata,
      "stopped-tree copy rejects source symlink identity changes",
    );
    await symlink(target, destination);
    await assertPathIdentity(
      source,
      metadata,
      "stopped-tree copy rejects source symlink identity changes",
    );
    await assertDestinationParentCurrent();
    return;
  }
  if (metadata.isDirectory()) {
    await context.beforeSourceOpen?.(source);
    const sourceHandle = await open(
      source,
      fsConstants.O_RDONLY | fsConstants.O_DIRECTORY | fsConstants.O_NOFOLLOW,
    );
    let destinationDirectoryHandle = providedDestinationDirectoryHandle;
    let destinationDirectoryIdentity = providedDestinationDirectoryIdentity;
    let ownsDestinationDirectoryHandle = false;
    let primaryFailure;
    try {
      const heldMetadata = await sourceHandle.stat({ bigint: true });
      assert(
        sameFileIdentity(metadata, heldMetadata) && heldMetadata.isDirectory(),
        "stopped-tree copy rejects source directory identity changes",
      );
      await context.afterSourceDirectoryOpen?.(source);
      await assertSnapshotUserAccess(
        source,
        fsConstants.R_OK | fsConstants.X_OK,
        context.checkAccess,
      );
      const finalMode = portableMode(heldMetadata);
      if (!destinationDirectoryCreated) {
        await assertDestinationParentCurrent();
        mkdirPrivate(destination);
        destinationDirectoryIdentity = await lstat(destination, { bigint: true });
        await context.afterDestinationDirectoryCreated?.(destination);
        await assertPathIdentity(
          destination,
          destinationDirectoryIdentity,
          "stopped-tree copy rejects destination directory identity changes",
        );
        destinationDirectoryHandle = await open(
          destination,
          fsConstants.O_RDONLY | fsConstants.O_DIRECTORY | fsConstants.O_NOFOLLOW,
        );
        ownsDestinationDirectoryHandle = true;
      }
      assert(destinationDirectoryHandle, "stopped-tree copy requires a destination handle");
      assert(
        destinationDirectoryIdentity?.isDirectory(),
        "stopped-tree copy rejects destination directory identity changes",
      );
      const heldDestinationDirectoryIdentity = await destinationDirectoryHandle.stat({
        bigint: true,
      });
      assert(
        heldDestinationDirectoryIdentity.isDirectory() &&
          sameFileIdentity(
            destinationDirectoryIdentity,
            heldDestinationDirectoryIdentity,
          ),
        "stopped-tree copy rejects destination directory identity changes",
      );
      const assertDestinationDirectoryCurrent = async () => {
        await assertDestinationParentCurrent();
        const heldIdentity = await destinationDirectoryHandle.stat({ bigint: true });
        const currentIdentity = await assertPathIdentity(
          destination,
          destinationDirectoryIdentity,
          "stopped-tree copy rejects destination directory identity changes",
        );
        assert(
          sameFileIdentity(heldIdentity, currentIdentity),
          "stopped-tree copy rejects destination directory identity changes",
        );
      };
      await assertDestinationDirectoryCurrent();
      await destinationDirectoryHandle.chmod(0o700);
      const entries = await readPortableDirectory(source);
      await assertPathIdentity(
        source,
        heldMetadata,
        "stopped-tree copy rejects source directory identity changes",
      );
      for (const entry of entries) {
        await assertDestinationDirectoryCurrent();
        await copyTreeEntry(context, join(source, entry), join(destination, entry), {
          assertDestinationParentCurrent: assertDestinationDirectoryCurrent,
        });
      }
      const finalSourceMetadata = await sourceHandle.stat({ bigint: true });
      assertStableFileMetadata(
        heldMetadata,
        finalSourceMetadata,
        "stopped-tree copy rejects source directory metadata changes",
      );
      await assertPathIdentity(
        source,
        heldMetadata,
        "stopped-tree copy rejects source directory identity changes",
      );
      await assertDestinationDirectoryCurrent();
      await destinationDirectoryHandle.chmod(finalMode);
      await assertDestinationDirectoryCurrent();
      return;
    } catch (error) {
      primaryFailure = { error };
      throw error;
    } finally {
      await runSequentialCleanup(
        [
          () =>
            ownsDestinationDirectoryHandle
              ? destinationDirectoryHandle?.close()
              : undefined,
          () => sourceHandle.close(),
        ],
        primaryFailure,
      );
    }
  }
  if (!metadata.isFile()) {
    throw new Error("stopped-tree copy rejects sockets, devices, and FIFOs");
  }
  await assertSnapshotUserAccess(source, fsConstants.R_OK, context.checkAccess);
  if (metadata.nlink !== 1n) throw new Error("stopped-tree copy rejects hard-linked files");
  await context.beforeSourceOpen?.(source);
  const sourceHandle = await open(source, fsConstants.O_RDONLY | fsConstants.O_NOFOLLOW);
  let destinationHandle;
  let primaryFailure;
  try {
    const heldMetadata = await sourceHandle.stat({ bigint: true });
    assert(
      sameFileIdentity(metadata, heldMetadata) && heldMetadata.isFile(),
      "stopped-tree copy rejects source file identity changes",
    );
    const finalMode = portableMode(heldMetadata);
    await assertDestinationParentCurrent();
    destinationHandle = await open(
      destination,
      fsConstants.O_WRONLY |
        fsConstants.O_CREAT |
        fsConstants.O_EXCL |
        fsConstants.O_NOFOLLOW,
      0o600,
    );
    await copyFileContents(sourceHandle, destinationHandle);
    const finalSourceMetadata = await sourceHandle.stat({ bigint: true });
    assertStableFileMetadata(
      heldMetadata,
      finalSourceMetadata,
      "stopped-tree copy rejects source file metadata changes",
    );
    await assertPathIdentity(
      source,
      heldMetadata,
      "stopped-tree copy rejects source file identity changes",
    );
    await destinationHandle.chmod(finalMode);
    await assertDestinationParentCurrent();
  } catch (error) {
    primaryFailure = { error };
    throw error;
  } finally {
    await runSequentialCleanup(
      [() => destinationHandle?.close(), () => sourceHandle.close()],
      primaryFailure,
    );
  }
}

async function removeTreeEntryForCleanup(path) {
  let metadata;
  try {
    metadata = await lstat(path);
  } catch (error) {
    if (error?.code === "ENOENT") return;
    throw error;
  }
  if (metadata.isDirectory()) {
    await chmod(path, (metadata.mode & 0o777) | 0o700);
    const entries = await readdir(path, { encoding: "buffer" });
    for (const entry of entries) await removeTreeEntryForCleanup(rawChildPath(path, entry));
  }
  await rm(path, { recursive: metadata.isDirectory(), force: true });
}

export async function removeTreeForCleanup(
  path,
  { listMountPoints = listCurrentMountPoints } = {},
) {
  let metadata;
  try {
    metadata = await lstat(path);
  } catch (error) {
    if (error?.code === "ENOENT") return;
    throw error;
  }
  if (metadata.isDirectory()) await assertNoMountBoundary(path, listMountPoints);
  await removeTreeEntryForCleanup(path);
}

export async function copyStoppedTreeBetweenRoots({
  sourceOwnedRoot,
  source,
  destinationOwnedRoot,
  destination,
  allowAbsoluteSymlinks = true,
  allowSourceRootMount = false,
  afterDestinationValidation,
  afterDestinationRootCreated,
  afterDestinationDirectoryCreated,
  afterSourceSymlinkValidated,
  afterSourceDirectoryOpen,
  beforeSourceOpen,
  checkAccess = access,
  expectedSourceRootIdentity = null,
  forbiddenAbsoluteSymlinkAuthorities = [],
  inspectOwnedRootAncestorAcl = recoveryPathHasUnsafeAncestorAcl,
  inspectOwnedRootAcl = recoveryPathHasExtendedAcl,
  inspectSymlinkPath = lstat,
  listMountPoints = listCurrentMountPoints,
}) {
  assert.equal(
    typeof allowAbsoluteSymlinks,
    "boolean",
    "allowAbsoluteSymlinks must be a boolean",
  );
  if (expectedSourceRootIdentity !== null) {
    assert(
      expectedSourceRootIdentity !== undefined &&
        typeof expectedSourceRootIdentity === "object" &&
        integerAsBigInt(expectedSourceRootIdentity.dev) !== null &&
        integerAsBigInt(expectedSourceRootIdentity.ino) !== null,
      "expectedSourceRootIdentity must be a filesystem identity",
    );
  }
  assert.equal(
    typeof allowSourceRootMount,
    "boolean",
    "allowSourceRootMount must be a boolean",
  );
  assert(
    Array.isArray(forbiddenAbsoluteSymlinkAuthorities),
    "forbiddenAbsoluteSymlinkAuthorities must be an array",
  );
  const normalizedForbiddenAbsoluteSymlinkAuthorities =
    forbiddenAbsoluteSymlinkAuthorities.map((authority) => {
      assert(
        authority !== null &&
          typeof authority === "object" &&
          typeof authority.path === "string" &&
          isAbsolute(authority.path) &&
          resolve(authority.path) === authority.path,
        "forbidden absolute symlink authority must have an absolute normalized path",
      );
      const dev =
        typeof authority.device === "string" &&
        /^(?:0|[1-9][0-9]*)$/u.test(authority.device)
          ? BigInt(authority.device)
          : integerAsBigInt(authority.device);
      const ino =
        typeof authority.inode === "string" &&
        /^[1-9][0-9]*$/u.test(authority.inode)
          ? BigInt(authority.inode)
          : integerAsBigInt(authority.inode);
      assert(
        dev !== null && dev >= 0n && ino !== null && ino > 0n,
        "forbidden absolute symlink authority must have a valid identity",
      );
      return Object.freeze({
        identity: Object.freeze({ dev, ino }),
        path: authority.path,
      });
    });
  let sourceOwnedRootAuthority;
  let destinationOwnedRootAuthority;
  let destinationRootHandle;
  let destinationRootIdentity;
  let primaryFailure;
  try {
    sourceOwnedRootAuthority = await openPrivateOwnedRootAuthority(sourceOwnedRoot, {
      inspectAncestorAcl: inspectOwnedRootAncestorAcl,
      inspectRootAcl: inspectOwnedRootAcl,
    });
    destinationOwnedRootAuthority = await openPrivateOwnedRootAuthority(
      destinationOwnedRoot,
      {
        inspectAncestorAcl: inspectOwnedRootAncestorAcl,
        inspectRootAcl: inspectOwnedRootAcl,
      },
    );
    const rootsShareRequestedPath =
      resolve(sourceOwnedRoot) === resolve(destinationOwnedRoot);
    if (!rootsShareRequestedPath) {
      assert(
        !sameFileIdentity(
          sourceOwnedRootAuthority.identity,
          destinationOwnedRootAuthority.identity,
        ),
        "stopped-tree copy rejects distinct owned-root identity aliases",
      );
    }
    if (sourceOwnedRootAuthority.path !== destinationOwnedRootAuthority.path) {
      assert(
        !pathIsInside(
          sourceOwnedRootAuthority.path,
          destinationOwnedRootAuthority.path,
        ) &&
          !pathIsInside(
            destinationOwnedRootAuthority.path,
            sourceOwnedRootAuthority.path,
          ),
        "stopped-tree copy rejects nested owned roots",
      );
    }
    const canonicalSource = await assertDirectOwnedPath(
      sourceOwnedRootAuthority,
      source,
      "source",
      { mustExist: true },
    );
    const canonicalSourceRoot = await realpath(canonicalSource);
    const sourceRootIdentity = await lstat(canonicalSource, { bigint: true });
    assert(
      sourceRootIdentity.isDirectory() &&
        (expectedSourceRootIdentity === null ||
          sameFileIdentity(sourceRootIdentity, expectedSourceRootIdentity)),
      "stopped-tree copy rejects source root identity changes",
    );
    await assertNoMountBoundary(canonicalSource, listMountPoints, {
      allowRootMount: allowSourceRootMount,
    });
    const canonicalDestination = await assertDirectOwnedPath(
      destinationOwnedRootAuthority,
      destination,
      "destination",
      { mustExist: false },
    );
    await afterDestinationValidation?.();
    await sourceOwnedRootAuthority.assertCurrent();
    await destinationOwnedRootAuthority.assertCurrent();
    mkdirPrivate(canonicalDestination);
    destinationRootIdentity = await lstat(canonicalDestination, { bigint: true });
    await afterDestinationRootCreated?.();
    await assertPathIdentity(
      canonicalDestination,
      destinationRootIdentity,
      "stopped-tree copy rejects destination root identity changes",
    );
    destinationRootHandle = await open(
      canonicalDestination,
      fsConstants.O_RDONLY | fsConstants.O_DIRECTORY | fsConstants.O_NOFOLLOW,
    );
    const heldDestinationRootIdentity = await destinationRootHandle.stat({ bigint: true });
    assert(
      destinationRootIdentity.isDirectory() &&
        sameFileIdentity(destinationRootIdentity, heldDestinationRootIdentity),
      "stopped-tree copy rejects destination root identity changes",
    );
    await destinationRootHandle.chmod(0o700);
    const assertDestinationRootCurrent = async () => {
      await sourceOwnedRootAuthority.assertCurrent();
      await destinationOwnedRootAuthority.assertCurrent();
      const heldIdentity = await destinationRootHandle.stat({ bigint: true });
      const currentIdentity = await assertPathIdentity(
        canonicalDestination,
        destinationRootIdentity,
        "stopped-tree copy rejects destination root identity changes",
      );
      assert(
        sameFileIdentity(heldIdentity, currentIdentity),
        "stopped-tree copy rejects destination root identity changes",
      );
    };
    // Index before copying so an absolute link cannot reach a source subtree
    // through an external bind-mount or other filesystem identity alias.
    const sourceIdentityKeys = await collectTreeIdentities(canonicalSource);
    await assertDestinationRootCurrent();
    await copyTreeEntry(
      {
        allowAbsoluteSymlinks,
        assertDestinationRootCurrent,
        afterDestinationDirectoryCreated,
        afterSourceSymlinkValidated,
        afterSourceDirectoryOpen,
        beforeSourceOpen,
        checkAccess,
        destinationRoots: [
          resolve(canonicalDestination),
          join(
            await realpath(dirname(canonicalDestination)),
            basename(canonicalDestination),
          ),
        ],
        destinationRootIdentity,
        forbiddenAbsoluteSymlinkAuthorities:
          normalizedForbiddenAbsoluteSymlinkAuthorities,
        inspectSymlinkPath,
        sourceIdentityKeys,
        sourceRootIdentity,
        sourceRoots: [resolve(canonicalSource), canonicalSourceRoot],
      },
      canonicalSource,
      canonicalDestination,
      {
        assertDestinationParentCurrent: async () => {},
        destinationDirectoryCreated: true,
        destinationDirectoryHandle: destinationRootHandle,
        destinationDirectoryIdentity: destinationRootIdentity,
        expectedSourceIdentity: sourceRootIdentity,
      },
    );
    await assertDestinationRootCurrent();
  } catch (error) {
    primaryFailure = { error };
    throw error;
  } finally {
    await runSequentialCleanup(
      [
        () => destinationRootHandle?.close(),
        () => destinationOwnedRootAuthority?.handle.close(),
        () => sourceOwnedRootAuthority?.handle.close(),
      ],
      primaryFailure,
    );
  }
}

export async function copyStoppedTree({ ownedRoot, ...options }) {
  return copyStoppedTreeBetweenRoots({
    ...options,
    destinationOwnedRoot: ownedRoot,
    sourceOwnedRoot: ownedRoot,
  });
}

function updateHashFields(hash, fields) {
  for (const field of fields) {
    const bytes = Buffer.isBuffer(field) ? field : Buffer.from(String(field));
    hash.update(`${bytes.length}:`);
    hash.update(bytes);
  }
}

async function syncTreeEntry(
  path,
  {
    afterEntryOpen,
    beforeEntryOpen,
    checkAccess,
    syncDirectory,
    syncFile,
  },
) {
  const metadata = await lstat(path, { bigint: true });
  if (metadata.isSymbolicLink()) {
    if (metadata.nlink !== 1n) {
      throw new Error("stopped-tree sync rejects hard-linked symlinks");
    }
    await readPortableSymlink(path);
    const finalMetadata = await lstat(path, { bigint: true });
    assertStableFileMetadata(
      metadata,
      finalMetadata,
      "stopped-tree sync rejects symlink metadata changes",
    );
    await assertPathIdentity(
      path,
      metadata,
      "stopped-tree sync rejects symlink identity changes",
    );
    return;
  }

  if (metadata.isDirectory()) {
    await beforeEntryOpen?.(path, metadata);
    const handle = await open(
      path,
      fsConstants.O_RDONLY | fsConstants.O_DIRECTORY | fsConstants.O_NOFOLLOW,
    );
    let primaryFailure;
    try {
      const heldMetadata = await handle.stat({ bigint: true });
      assert(
        heldMetadata.isDirectory(),
        "stopped-tree sync rejects directory identity changes",
      );
      assertStableFileMetadata(
        metadata,
        heldMetadata,
        "stopped-tree sync rejects directory metadata changes",
      );
      await afterEntryOpen?.(path, heldMetadata);
      await assertSnapshotUserAccess(
        path,
        fsConstants.R_OK | fsConstants.X_OK,
        checkAccess,
      );
      portableMode(heldMetadata);
      const entries = await readPortableDirectory(path);
      await assertPathIdentity(
        path,
        heldMetadata,
        "stopped-tree sync rejects directory identity changes",
      );
      for (const entry of entries) {
        await syncTreeEntry(join(path, entry), {
          afterEntryOpen,
          beforeEntryOpen,
          checkAccess,
          syncDirectory,
          syncFile,
        });
      }
      await syncDirectory(handle, path);
      assertStableFileMetadata(
        heldMetadata,
        await handle.stat({ bigint: true }),
        "stopped-tree sync rejects directory metadata changes",
      );
      await assertPathIdentity(
        path,
        heldMetadata,
        "stopped-tree sync rejects directory identity changes",
      );
    } catch (error) {
      primaryFailure = { error };
      throw error;
    } finally {
      await runSequentialCleanup([() => handle.close()], primaryFailure);
    }
    return;
  }

  if (!metadata.isFile()) {
    throw new Error("stopped-tree sync rejects sockets, devices, and FIFOs");
  }
  await assertSnapshotUserAccess(path, fsConstants.R_OK, checkAccess);
  if (metadata.nlink !== 1n) {
    throw new Error("stopped-tree sync rejects hard-linked files");
  }
  await beforeEntryOpen?.(path, metadata);
  const handle = await open(path, fsConstants.O_RDONLY | fsConstants.O_NOFOLLOW);
  let primaryFailure;
  try {
    const heldMetadata = await handle.stat({ bigint: true });
    assert(
      heldMetadata.isFile(),
      "stopped-tree sync rejects file identity changes",
    );
    assertStableFileMetadata(
      metadata,
      heldMetadata,
      "stopped-tree sync rejects file metadata changes",
    );
    await afterEntryOpen?.(path, heldMetadata);
    portableMode(heldMetadata);
    await syncFile(handle, path);
    assertStableFileMetadata(
      heldMetadata,
      await handle.stat({ bigint: true }),
      "stopped-tree sync rejects file metadata changes",
    );
    await assertPathIdentity(
      path,
      heldMetadata,
      "stopped-tree sync rejects file identity changes",
    );
  } catch (error) {
    primaryFailure = { error };
    throw error;
  } finally {
    await runSequentialCleanup([() => handle.close()], primaryFailure);
  }
}

export async function syncStoppedTree(
  root,
  {
    afterEntryOpen,
    allowRootMount = false,
    beforeEntryOpen,
    checkAccess = access,
    listMountPoints = listCurrentMountPoints,
    syncDirectory = async (handle) => handle.sync(),
    syncFile = async (handle) => handle.sync(),
  } = {},
) {
  assert.equal(typeof allowRootMount, "boolean", "allowRootMount must be a boolean");
  assert.equal(typeof syncDirectory, "function", "syncDirectory must be a function");
  assert.equal(typeof syncFile, "function", "syncFile must be a function");
  const rootMetadata = await lstat(root, { bigint: true });
  assert(
    rootMetadata.isDirectory() && !rootMetadata.isSymbolicLink(),
    "stopped-tree sync root must be a directory",
  );
  await assertNoMountBoundary(root, listMountPoints, { allowRootMount });
  await syncTreeEntry(root, {
    afterEntryOpen,
    beforeEntryOpen,
    checkAccess,
    syncDirectory,
    syncFile,
  });
}

async function hashTreeEntry(hash, root, path, checkAccess) {
  const metadata = await lstat(path, { bigint: true });
  const entryPath = relative(root, path) || ".";
  if (metadata.isSymbolicLink()) {
    if (metadata.nlink !== 1n) throw new Error("tree digest rejects hard-linked symlinks");
    const { bytes } = await readPortableSymlink(path);
    await assertPathIdentity(path, metadata, "tree digest rejects source symlink identity changes");
    updateHashFields(hash, ["symlink", entryPath, bytes]);
    return;
  }
  if (metadata.isDirectory()) {
    const handle = await open(
      path,
      fsConstants.O_RDONLY | fsConstants.O_DIRECTORY | fsConstants.O_NOFOLLOW,
    );
    let primaryFailure;
    try {
      const heldMetadata = await handle.stat({ bigint: true });
      assert(
        sameFileIdentity(metadata, heldMetadata) && heldMetadata.isDirectory(),
        "tree digest rejects source directory identity changes",
      );
      await assertSnapshotUserAccess(path, fsConstants.R_OK | fsConstants.X_OK, checkAccess);
      updateHashFields(hash, ["directory", entryPath, portableMode(heldMetadata)]);
      const entries = await readPortableDirectory(path);
      await assertPathIdentity(
        path,
        heldMetadata,
        "tree digest rejects source directory identity changes",
      );
      for (const entry of entries) {
        await hashTreeEntry(hash, root, join(path, entry), checkAccess);
      }
      assertStableFileMetadata(
        heldMetadata,
        await handle.stat({ bigint: true }),
        "tree digest rejects source directory metadata changes",
      );
      await assertPathIdentity(
        path,
        heldMetadata,
        "tree digest rejects source directory identity changes",
      );
      return;
    } catch (error) {
      primaryFailure = { error };
      throw error;
    } finally {
      await runSequentialCleanup([() => handle.close()], primaryFailure);
    }
  }
  if (!metadata.isFile()) throw new Error("tree digest rejects non-file entries");
  await assertSnapshotUserAccess(path, fsConstants.R_OK, checkAccess);
  if (metadata.nlink !== 1n) throw new Error("tree digest rejects hard-linked files");
  const handle = await open(path, fsConstants.O_RDONLY | fsConstants.O_NOFOLLOW);
  let primaryFailure;
  try {
    const heldMetadata = await handle.stat({ bigint: true });
    assert(
      sameFileIdentity(metadata, heldMetadata) && heldMetadata.isFile(),
      "tree digest rejects source file identity changes",
    );
    const digest = await digestFileHandle(handle);
    assertStableFileMetadata(
      heldMetadata,
      await handle.stat({ bigint: true }),
      "tree digest rejects source file metadata changes",
    );
    await assertPathIdentity(path, heldMetadata, "tree digest rejects source file identity changes");
    updateHashFields(hash, [
      "file",
      entryPath,
      portableMode(heldMetadata),
      heldMetadata.size,
      digest,
    ]);
  } catch (error) {
    primaryFailure = { error };
    throw error;
  } finally {
    await runSequentialCleanup([() => handle.close()], primaryFailure);
  }
}

export async function digestTree(
  root,
  {
    allowRootMount = false,
    checkAccess = access,
    listMountPoints = listCurrentMountPoints,
  } = {},
) {
  assert.equal(typeof allowRootMount, "boolean", "allowRootMount must be a boolean");
  await assertNoMountBoundary(root, listMountPoints, { allowRootMount });
  const hash = createHash("sha256");
  await hashTreeEntry(hash, root, root, checkAccess);
  return hash.digest("hex");
}

export async function digestFile(path) {
  const handle = await open(path, fsConstants.O_RDONLY | fsConstants.O_NOFOLLOW);
  let primaryFailure;
  try {
    return await digestFileHandle(handle);
  } catch (error) {
    primaryFailure = { error };
    throw error;
  } finally {
    await runSequentialCleanup([() => handle.close()], primaryFailure);
  }
}

export function parseLinuxGetfacl(value) {
  assert.equal(typeof value, "string", "Linux getfacl output must be text");
  const entries = value
    .split("\n")
    .map((line) => line.trim())
    .filter((line) => line !== "" && !line.startsWith("#"));
  assert(entries.length >= 3, "Linux getfacl output is incomplete");
  let hasUser = false;
  let hasGroup = false;
  let hasOther = false;
  let extended = false;
  for (const entry of entries) {
    const match = entry.match(
      /^(default:)?(user|group|mask|other):([^:]*):[rwx-]{3}(?:\s+#effective:[rwx-]{3})?$/u,
    );
    assert(match, "Linux getfacl output is malformed");
    const [, defaultPrefix, kind, qualifier] = match;
    if (defaultPrefix || kind === "mask" || qualifier !== "") {
      extended = true;
      continue;
    }
    if (kind === "user") hasUser = true;
    if (kind === "group") hasGroup = true;
    if (kind === "other") hasOther = true;
  }
  assert(hasUser && hasGroup && hasOther, "Linux getfacl output omitted base entries");
  return extended;
}

export async function inspectLinuxRecoveryAcl(path, { runCommand = execFileAsync } = {}) {
  try {
    if (typeof path !== "string" || !isAbsolute(path)) throw new Error("invalid path");
    const { stderr, stdout } = await runCommand(
      "/usr/bin/getfacl",
      ["--absolute-names", "--omit-header", path],
      {
        encoding: "utf8",
        env: { ...process.env, LANG: "C", LC_ALL: "C" },
        maxBuffer: 64 * 1024,
      },
    );
    if (stderr !== "") throw new Error("unexpected getfacl diagnostics");
    return parseLinuxGetfacl(stdout);
  } catch {
    throw new Error("Linux ACL capability inspection failed");
  }
}

export async function recoveryPathHasExtendedAcl(path) {
  return platform() === "linux"
    ? inspectLinuxRecoveryAcl(path)
    : pathHasExtendedAcl(path);
}

export async function recoveryPathHasUnsafeAncestorAcl(path) {
  return platform() === "linux"
    ? inspectLinuxRecoveryAcl(path)
    : pathHasUnsafeAncestorAcl(path);
}
