import { createHash, randomUUID } from "node:crypto";
import { constants } from "node:fs";
import {
  chmod,
  lstat,
  mkdir,
  mkdtemp,
  open,
  readFile,
  readdir,
  realpath,
  rm,
  rmdir,
} from "node:fs/promises";
import { homedir } from "node:os";
import { dirname, isAbsolute, join, resolve } from "node:path";

import { AdvisoryLockError, acquireAdvisoryLock } from "./advisory-lock.mjs";
import { AppServerClient } from "./app-server-auth-probe.mjs";

const LOCK_FILE = ".portable-auth-refresh.lock";
const STAGING_DIRECTORY = ".portable-auth-refresh-staging";
const MIN_REFRESHED_TOKEN_VALIDITY_SECONDS = 120;
const EXPECTED_RPC_AUDIT = [
  { kind: "request", method: "initialize" },
  { kind: "notification", method: "initialized" },
  { kind: "request", method: "account/read" },
];
const STAGING_CLEANUP_RETRY_DELAYS_MS = [50, 100, 250, 500, 1_000];
const PRE_DISPATCH_REFRESH_ERRORS = new WeakSet();

function isObjectLike(value) {
  return value !== null && ["object", "function"].includes(typeof value);
}

function markPreDispatchRefreshError(error) {
  if (isObjectLike(error)) PRE_DISPATCH_REFRESH_ERRORS.add(error);
}

function isKnownPreDispatchRefreshError(error) {
  return isObjectLike(error) && PRE_DISPATCH_REFRESH_ERRORS.has(error);
}

function sha256(value) {
  return createHash("sha256").update(value).digest("hex");
}

function fingerprint(value) {
  return `sha256:${sha256(value).slice(0, 24)}`;
}

function fail(
  code,
  message,
  { recoveryPath, recoveryPaths, recoveryReason, retryable = false } = {},
) {
  throw new ManagedAuthRefreshError(code, message, {
    recoveryPath,
    recoveryPaths,
    recoveryReason,
    retryable,
  });
}

function ensure(condition, code, message, options) {
  if (!condition) fail(code, message, options);
}

function withSensitiveProperties(publicValue, sensitiveProperties) {
  for (const [name, value] of Object.entries(sensitiveProperties)) {
    Object.defineProperty(publicValue, name, { enumerable: false, value });
  }
  return publicValue;
}

function decodeJwtPayload(token, label) {
  const parts = token.split(".");
  ensure(parts.length >= 2, "invalid_auth_record", `${label} is not a JWT`);
  try {
    const payload = JSON.parse(Buffer.from(parts[1], "base64url").toString("utf8"));
    ensure(
      payload !== null && typeof payload === "object" && !Array.isArray(payload),
      "invalid_auth_record",
      `${label} JWT payload must be an object`,
    );
    return payload;
  } catch {
    fail("invalid_auth_record", `${label} has an invalid JWT payload`);
  }
}

function authClaims(payload) {
  const claims = payload?.["https://api.openai.com/auth"];
  return claims && typeof claims === "object" ? claims : {};
}

function collectRedactionValues(auth, accessPayload, idPayload) {
  const values = [
    auth.OPENAI_API_KEY,
    auth.tokens?.access_token,
    auth.tokens?.refresh_token,
    auth.tokens?.id_token,
    auth.tokens?.account_id,
    accessPayload?.email,
    idPayload?.email,
    authClaims(accessPayload).chatgpt_account_id,
    authClaims(accessPayload).chatgpt_user_id,
    authClaims(idPayload).chatgpt_account_id,
    authClaims(idPayload).chatgpt_user_id,
  ];
  return [...new Set(values.filter((value) => typeof value === "string" && value.length >= 8))];
}

function rpcAuditMatches(actual) {
  return (
    Array.isArray(actual) &&
    actual.length === EXPECTED_RPC_AUDIT.length &&
    actual.every(
      (entry, index) =>
        entry?.kind === EXPECTED_RPC_AUDIT[index].kind &&
        entry?.method === EXPECTED_RPC_AUDIT[index].method,
    )
  );
}

function sameFileIdentity(left, right) {
  return left.dev === right.dev && left.ino === right.ino;
}

export function authorityDirectoryPermissionsAreSafe(
  { isDirectory, mode, uid },
  { allowRootOwner = false, brokerUid, disallowedModeBits },
) {
  return (
    isDirectory === true &&
    (brokerUid === null || uid === brokerUid || (allowRootOwner && uid === 0)) &&
    (mode & disallowedModeBits) === 0
  );
}

function attachRecoveryPaths(target, paths) {
  if (!target || typeof target !== "object") return;
  const recoveryPaths = [
    ...(Array.isArray(target.recoveryPaths) ? target.recoveryPaths : []),
    ...paths,
  ].filter((value) => typeof value === "string" && value.length > 0);
  if (recoveryPaths.length === 0) return;
  target.recoveryPaths = [...new Set(recoveryPaths)];
  target.recoveryPath ??= target.recoveryPaths[0];
}

function lastRefreshAdvanced(before, after) {
  if (typeof after !== "string") return false;
  const afterTime = Date.parse(after);
  if (!Number.isFinite(afterTime)) return false;
  if (before === null || before === undefined) return true;
  const beforeTime = Date.parse(before);
  return Number.isFinite(beforeTime) && afterTime > beforeTime;
}

async function resolveAuthorityHome(authHome) {
  ensure(
    typeof constants.O_DIRECTORY === "number" && typeof constants.O_NOFOLLOW === "number",
    "unsupported_platform",
    "safe authority directory guards require O_DIRECTORY and O_NOFOLLOW support",
  );
  const authorityHome = await realpath(resolve(authHome));
  let handle;
  try {
    handle = await open(
      authorityHome,
      constants.O_RDONLY | constants.O_DIRECTORY | constants.O_NOFOLLOW,
    );
    const authorityHomeStat = await handle.stat();
    ensure(
      authorityHomeStat.isDirectory(),
      "unsafe_auth_home",
      "authority home must be a real directory",
    );
    const authorityParentStat = await lstat(dirname(authorityHome));
    const currentUid =
      typeof process.geteuid === "function"
        ? process.geteuid()
        : typeof process.getuid === "function"
          ? process.getuid()
          : null;
    ensure(
      authorityDirectoryPermissionsAreSafe(
        {
          isDirectory: authorityHomeStat.isDirectory(),
          mode: authorityHomeStat.mode,
          uid: authorityHomeStat.uid,
        },
        { brokerUid: currentUid, disallowedModeBits: 0o077 },
      ),
      "unsafe_auth_home",
      "authority home must be broker-owned and private",
    );
    ensure(
      authorityDirectoryPermissionsAreSafe(
        {
          isDirectory: authorityParentStat.isDirectory(),
          mode: authorityParentStat.mode,
          uid: authorityParentStat.uid,
        },
        {
          allowRootOwner: true,
          brokerUid: currentUid,
          disallowedModeBits: 0o022,
        },
      ),
      "unsafe_auth_home",
      "authority home parent must be broker/root-owned and not group/world writable",
    );
    const protectedHomes = await Promise.all(
      [join(homedir(), ".codex"), process.env.CODEX_HOME]
        .filter((value) => typeof value === "string" && value.length > 0)
        .map(async (value) => {
          try {
            const path = await realpath(resolve(value));
            return { path, stat: await lstat(path) };
          } catch {
            return null;
          }
        }),
    );
    ensure(
      !protectedHomes.some(
        (protectedHome) =>
          protectedHome !== null &&
          (protectedHome.path === authorityHome ||
            sameFileIdentity(protectedHome.stat, authorityHomeStat)),
      ),
      "unsafe_auth_home",
      "refusing to mutate the default or active Codex home; use a dedicated authority home",
    );
    return { handle, identity: authorityHomeStat, path: authorityHome };
  } catch (error) {
    await handle?.close().catch(() => {});
    throw error;
  }
}

async function assertAuthorityHomeCurrent(authority) {
  let current;
  try {
    current = await open(
      authority.path,
      constants.O_RDONLY | constants.O_DIRECTORY | constants.O_NOFOLLOW,
    );
    const currentStat = await current.stat();
    ensure(
      currentStat.isDirectory() && sameFileIdentity(currentStat, authority.identity),
      "authority_home_replaced",
      "authority home identity changed during refresh",
    );
  } catch (error) {
    if (error instanceof ManagedAuthRefreshError) throw error;
    fail("authority_home_replaced", "authority home identity changed during refresh");
  } finally {
    await current?.close().catch(() => {});
  }
}

async function acquireAuthorityLock(authority) {
  await assertAuthorityHomeCurrent(authority);
  const lockPath = join(authority.path, LOCK_FILE);
  let lock;
  try {
    lock = await acquireAdvisoryLock(lockPath);
    await assertAuthorityHomeCurrent(authority);
    return lock;
  } catch (error) {
    await lock?.release().catch(() => {});
    if (error instanceof AdvisoryLockError && error.code === "lock_unavailable") {
      fail("authority_locked", "another authority refresh holds the dedicated auth lock", {
        retryable: true,
      });
    }
    throw error;
  }
}

async function findRecoveryArtifacts(authority) {
  await assertAuthorityHomeCurrent(authority);
  const recoveryPaths = [];
  const entries = await readdir(authority.path, { withFileTypes: true });
  for (const entry of entries) {
    if (entry.name.startsWith(".auth.json.next-")) {
      recoveryPaths.push(join(authority.path, entry.name));
    }
  }

  const stagingRoot = join(authority.path, STAGING_DIRECTORY);
  try {
    const stagingRootStat = await lstat(stagingRoot);
    if (!stagingRootStat.isDirectory() || stagingRootStat.isSymbolicLink()) {
      recoveryPaths.push(stagingRoot);
    } else {
      const attempts = await readdir(stagingRoot, { withFileTypes: true });
      for (const attempt of attempts) {
        if (attempt.name.startsWith("attempt-")) {
          recoveryPaths.push(join(stagingRoot, attempt.name));
        }
      }
    }
  } catch (error) {
    if (error?.code !== "ENOENT") throw error;
  }
  await assertAuthorityHomeCurrent(authority);
  return recoveryPaths.sort();
}

async function releaseAuthorityLock(lock) {
  await lock.release();
}

async function writeFileDurably(path, contents, { flag = "wx", mode = 0o600 } = {}) {
  let handle;
  try {
    handle = await open(path, flag, mode);
    await handle.writeFile(contents);
    await handle.sync();
  } finally {
    await handle?.close().catch(() => {});
  }
}

async function syncDirectoryPath(path) {
  let handle;
  try {
    handle = await open(
      path,
      constants.O_RDONLY | constants.O_DIRECTORY | constants.O_NOFOLLOW,
    );
    await handle.sync();
    return true;
  } catch (error) {
    if (["EINVAL", "ENOTSUP", "EISDIR", "EPERM"].includes(error?.code)) return false;
    throw error;
  } finally {
    await handle?.close().catch(() => {});
  }
}

async function syncRefreshedStagingAuth(stagingHome, syncStagingDirectory) {
  let handle;
  try {
    handle = await open(
      join(stagingHome, "auth.json"),
      constants.O_RDWR | constants.O_NOFOLLOW | constants.O_NONBLOCK,
    );
    const fileStat = await handle.stat();
    ensure(
      fileStat.isFile() && fileStat.nlink === 1,
      "staging_recovery_not_durable",
      "refreshed staging auth must remain a regular single-link file",
      { recoveryPath: stagingHome, recoveryReason: "staging_sync_failed" },
    );
    await handle.sync();
    ensure(
      await syncStagingDirectory(stagingHome),
      "staging_recovery_not_durable",
      "refreshed staging auth directory could not be synchronized",
      { recoveryPath: stagingHome, recoveryReason: "staging_sync_failed" },
    );
  } catch (error) {
    if (error instanceof ManagedAuthRefreshError) throw error;
    const durabilityError = new ManagedAuthRefreshError(
      "staging_recovery_not_durable",
      "refreshed staging auth could not be synchronized",
      { recoveryPath: stagingHome, recoveryReason: "staging_sync_failed" },
    );
    durabilityError.cause = error;
    throw durabilityError;
  } finally {
    await handle?.close().catch(() => {});
  }
}

async function createStagingHome(
  authority,
  rawAuth,
  {
    syncStagingDirectory = syncDirectoryPath,
    writeStagingFile = writeFileDurably,
  } = {},
) {
  const authorityHome = authority.path;
  const stagingRoot = join(authorityHome, STAGING_DIRECTORY);
  let stagingRootCreated = false;
  let stagingHome;
  try {
    try {
      await mkdir(stagingRoot, { mode: 0o700 });
      stagingRootCreated = true;
    } catch (error) {
      if (error?.code !== "EEXIST") throw error;
    }
    ensure(
      await syncParentDirectory(authority),
      "staging_not_durable",
      "authority filesystem cannot durably record the staging root",
    );
    const stagingRootStat = await lstat(stagingRoot);
    ensure(
      stagingRootStat.isDirectory() && !stagingRootStat.isSymbolicLink(),
      "unsafe_staging_path",
      "authority staging root must be a real directory",
    );
    await chmod(stagingRoot, 0o700);
    stagingHome = await mkdtemp(join(stagingRoot, "attempt-"));
    await chmod(stagingHome, 0o700);
    ensure(
      await syncStagingDirectory(stagingRoot),
      "staging_not_durable",
      "authority filesystem cannot durably record the staging attempt",
    );
    await writeStagingFile(join(stagingHome, "auth.json"), rawAuth, {
      flag: "wx",
      mode: 0o600,
    });
    const config = `cli_auth_credentials_store = "file"

[features]
plugin_hooks = false
plugin_sharing = false
plugins = false
remote_plugin = false
`;
    await writeStagingFile(join(stagingHome, "config.toml"), config, {
      flag: "wx",
      mode: 0o600,
    });
    ensure(
      await syncStagingDirectory(stagingHome),
      "staging_not_durable",
      "authority filesystem cannot durably record staged credentials",
    );
    return { stagingHome, stagingRoot };
  } catch (error) {
    if (stagingHome) await rm(stagingHome, { recursive: true, force: true }).catch(() => {});
    if (stagingRootCreated) await rmdir(stagingRoot).catch(() => {});
    throw error;
  }
}

async function syncParentDirectory(authority) {
  try {
    await authority.handle.sync();
    return true;
  } catch (error) {
    if (["EINVAL", "ENOTSUP", "EISDIR", "EPERM"].includes(error?.code)) return false;
    throw error;
  }
}

async function atomicallyPromoteAuth(
  authority,
  rawAuth,
  expectedSource,
  assertLockHeld,
  commitRename,
  readCanonicalSource,
  syncDirectory,
) {
  const authorityHome = authority.path;
  const destination = join(authorityHome, "auth.json");
  const temporary = join(authorityHome, `.auth.json.next-${randomUUID()}`);
  let handle;
  let renamed = false;
  let retainTemporary = false;
  let operationError;
  try {
    await assertAuthorityHomeCurrent(authority);
    handle = await open(
      temporary,
      constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW,
      0o600,
    );
    await handle.writeFile(rawAuth);
    await handle.sync();
    await handle.close();
    handle = undefined;

    await assertAuthorityHomeCurrent(authority);
    let currentSource;
    try {
      currentSource = await readCanonicalSource(authorityHome);
    } catch (error) {
      // A failed pathname read may itself be evidence that the authority was
      // replaced. Revalidate before exposing the read error or recovery paths.
      await assertAuthorityHomeCurrent(authority);
      throw error;
    }
    await assertAuthorityHomeCurrent(authority);
    ensure(
      currentSource.authFileFingerprint === expectedSource.authFileFingerprint &&
        sameFileIdentity(currentSource.fileIdentity, expectedSource.fileIdentity),
      "authority_conflict_after_refresh",
      "canonical authority state changed while the staged refresh was running",
    );

    await assertLockHeld();
    await assertAuthorityHomeCurrent(authority);
    try {
      await commitRename(temporary, destination);
      renamed = true;
    } catch (error) {
      if (!(error instanceof AdvisoryLockError) || error.code !== "lock_commit_uncertain") {
        throw error;
      }
      retainTemporary = true;
      const recoveryPaths = [];
      try {
        await lstat(temporary);
        recoveryPaths.push(temporary);
      } catch (candidateError) {
        if (candidateError?.code !== "ENOENT") recoveryPaths.push(temporary);
      }
      const uncertain = new ManagedAuthRefreshError(
        "promotion_commit_uncertain",
        "lock holder did not confirm the atomic authority promotion",
        {
          recoveryPaths,
          recoveryReason: "holder_commit_ack_lost",
        },
      );
      uncertain.cause = error;
      throw uncertain;
    }
    try {
      const parentDirectorySynced = await syncDirectory(authority);
      return {
        parentDirectorySynced,
        warnings: parentDirectorySynced ? [] : ["parent_directory_sync_failed"],
      };
    } catch {
      return {
        parentDirectorySynced: false,
        warnings: ["parent_directory_sync_failed"],
      };
    }
  } catch (error) {
    operationError = error;
    throw error;
  } finally {
    await handle?.close().catch(() => {});
    if (!renamed && !retainTemporary) {
      let authorityCurrent = false;
      try {
        await assertAuthorityHomeCurrent(authority);
        authorityCurrent = true;
      } catch {
        // Never resolve cleanup through a replaced authority path. A retained
        // candidate must be inspected from the single-attached authority volume.
      }
      if (authorityCurrent) {
        try {
          await rm(temporary, { force: true });
        } catch {
          attachRecoveryPaths(operationError, [temporary]);
        }
      }
    }
  }
}

export class ManagedAuthRefreshError extends Error {
  constructor(
    code,
    message,
    { recoveryPath, recoveryPaths = [], recoveryReason, retryable = false } = {},
  ) {
    super(message);
    this.name = "ManagedAuthRefreshError";
    this.code = code;
    this.retryable = retryable;
    if (recoveryPath) this.recoveryPath = recoveryPath;
    attachRecoveryPaths(this, [...recoveryPaths, recoveryPath]);
    if (recoveryReason) this.recoveryReason = recoveryReason;
  }
}

export function managedAuthRefreshErrorMetadata(error) {
  if (!error || typeof error !== "object") return {};
  const metadata = {};
  if (typeof error.code === "string") metadata.code = error.code;
  if (typeof error.retryable === "boolean") metadata.retryable = error.retryable;
  if (typeof error.recoveryPath === "string") metadata.recoveryPath = error.recoveryPath;
  if (Array.isArray(error.recoveryPaths)) {
    metadata.recoveryPaths = error.recoveryPaths.filter((value) => typeof value === "string");
  }
  if (typeof error.recoveryReason === "string") metadata.recoveryReason = error.recoveryReason;
  if (Array.isArray(error.cleanupWarnings)) {
    metadata.cleanupWarnings = error.cleanupWarnings.filter((value) => typeof value === "string");
  }
  return metadata;
}

export function managedAuthRefreshFailureReport(error) {
  if (error instanceof ManagedAuthRefreshError) {
    return {
      error: {
        type: "managed_auth_refresh",
        ...managedAuthRefreshErrorMetadata(error),
      },
      result: "failed",
    };
  }
  return {
    error: {
      code: "live_probe_failed",
      retryable: false,
      type: "probe_failure",
    },
    result: "failed",
  };
}

export async function readManagedAuthSnapshot(authHome) {
  ensure(
    typeof constants.O_NOFOLLOW === "number",
    "unsupported_platform",
    "safe authority credential reads require O_NOFOLLOW support",
  );
  const authorityHome = await realpath(resolve(authHome));
  const authPath = join(authorityHome, "auth.json");
  let handle;
  let raw;
  let fileStat;
  try {
    handle = await open(
      authPath,
      constants.O_RDONLY | constants.O_NOFOLLOW | constants.O_NONBLOCK,
    );
    fileStat = await handle.stat();
    ensure(fileStat.isFile(), "invalid_auth_record", "authority auth.json must be a regular file");
    ensure(fileStat.nlink === 1, "invalid_auth_record", "authority auth.json must not be hard linked");
    ensure(
      (fileStat.mode & 0o077) === 0,
      "invalid_auth_record",
      "authority auth.json must not be group/world accessible",
    );
    raw = await handle.readFile("utf8");
  } finally {
    await handle?.close().catch(() => {});
  }

  let auth;
  try {
    auth = JSON.parse(raw);
  } catch {
    fail("invalid_auth_record", "authority auth.json is not valid JSON");
  }
  ensure(auth.auth_mode === "chatgpt", "invalid_auth_record", "authority must use ChatGPT auth");
  ensure(
    typeof auth.tokens?.access_token === "string" && auth.tokens.access_token.length > 0,
    "invalid_auth_record",
    "authority auth.json is missing access_token",
  );
  ensure(
    typeof auth.tokens?.id_token === "string" && auth.tokens.id_token.length > 0,
    "invalid_auth_record",
    "authority auth.json is missing id_token",
  );
  ensure(
    typeof auth.tokens?.refresh_token === "string" && auth.tokens.refresh_token.length > 0,
    "invalid_auth_record",
    "authority auth.json is missing refresh_token",
  );

  const accessPayload = decodeJwtPayload(auth.tokens.access_token, "access_token");
  const idPayload = decodeJwtPayload(auth.tokens.id_token, "id_token");
  const accessAuth = authClaims(accessPayload);
  const idAuth = authClaims(idPayload);
  ensure(
    typeof accessAuth.chatgpt_account_id === "string" &&
      accessAuth.chatgpt_account_id.length > 0 &&
      typeof accessAuth.chatgpt_user_id === "string" &&
      accessAuth.chatgpt_user_id.length > 0,
    "invalid_auth_record",
    "authority access_token is missing account or user identity claims",
  );
  const accountIds = [
    auth.tokens.account_id,
    accessAuth.chatgpt_account_id,
    idAuth.chatgpt_account_id,
  ].filter((value) => value !== null && value !== undefined);
  ensure(accountIds.length > 0, "invalid_auth_record", "authority auth.json is missing account id");
  ensure(
    accountIds.every(
      (value) => typeof value === "string" && value.length > 0 && value === accountIds[0],
    ),
    "invalid_auth_record",
    "authority auth.json account identities do not match",
  );
  const userIds = [accessAuth.chatgpt_user_id, idAuth.chatgpt_user_id].filter(
    (value) => value !== null && value !== undefined,
  );
  ensure(userIds.length > 0, "invalid_auth_record", "authority auth.json is missing user id");
  ensure(
    userIds.every(
      (value) => typeof value === "string" && value.length > 0 && value === userIds[0],
    ),
    "invalid_auth_record",
    "authority auth.json user identities do not match",
  );

  return withSensitiveProperties({
    authPath,
    expiresAt:
      typeof accessPayload.exp === "number" ? new Date(accessPayload.exp * 1000).toISOString() : null,
    expiresAtUnixSeconds:
      typeof accessPayload.exp === "number" ? accessPayload.exp : null,
    fileIdentity: { dev: fileStat.dev, ino: fileStat.ino },
    fileMode: (fileStat.mode & 0o777).toString(8).padStart(4, "0"),
    lastRefreshAt: auth.last_refresh ?? null,
    planType: accessAuth.chatgpt_plan_type ?? idAuth.chatgpt_plan_type ?? null,
  }, {
    accessToken: auth.tokens.access_token,
    accessTokenFingerprint: fingerprint(auth.tokens.access_token),
    accountId: accountIds[0],
    authFileFingerprint: fingerprint(raw),
    raw,
    redactionValues: collectRedactionValues(auth, accessPayload, idPayload),
    refreshTokenFingerprint: fingerprint(auth.tokens.refresh_token),
    userId: userIds[0],
  });
}

export async function runCodexManagedRefresh({
  assertLockHeldBeforeDispatch,
  codexBin = "codex",
  createClient = (options) => new AppServerClient(options),
  signal,
  stagingHome,
}) {
  if (!(typeof codexBin === "string" && isAbsolute(codexBin))) {
    const error = new ManagedAuthRefreshError(
      "unsafe_codex_binary",
      "managed refresh requires an absolute Codex binary path",
    );
    markPreDispatchRefreshError(error);
    throw error;
  }
  const client = createClient({ codexBin, codexHome: stagingHome, timeoutMs: 120_000 });
  let operationError;
  let refreshRequestDispatched = false;
  const abortClient = async (reason) => {
    if (typeof client.abort === "function") {
      await client.abort(reason);
      return;
    }
    await client.stop({ force: true });
  };
  const withAbort = async (operation) => {
    if (!signal) return operation();
    if (signal.aborted) throw signal.reason ?? new Error("managed refresh aborted");
    const operationPromise = Promise.resolve().then(operation);
    return new Promise((resolve, reject) => {
      const onAbort = () => {
        const reason = signal.reason ?? new Error("managed refresh aborted");
        void abortClient(reason).then(
          () => reject(reason),
          (error) => reject(error),
        );
      };
      signal.addEventListener("abort", onAbort, { once: true });
      operationPromise.then(
        (value) => {
          if (signal.aborted) return;
          signal.removeEventListener("abort", onAbort);
          resolve(value);
        },
        (error) => {
          if (signal.aborted) return;
          signal.removeEventListener("abort", onAbort);
          reject(error);
        },
      );
    });
  };
  try {
    await withAbort(() => client.start());
    const initializeResult = await withAbort(() => client.initialize(false));
    if (typeof assertLockHeldBeforeDispatch === "function") {
      await withAbort(() => assertLockHeldBeforeDispatch());
    }
    let response;
    try {
      refreshRequestDispatched = true;
      response = await withAbort(() =>
        client.request("account/read", { refreshToken: true }),
      );
    } catch (error) {
      if (signal?.aborted) throw error;
      const uncertain = new ManagedAuthRefreshError(
        "refresh_outcome_uncertain",
        "account/read failed after the token refresh request was dispatched",
        {
          recoveryPath: stagingHome,
          recoveryReason: "account_read_outcome_unknown",
        },
      );
      uncertain.cause = error;
      throw uncertain;
    }
    return {
      initializeResult,
      response,
      rpcAudit: client.rpcMethodAudit(),
    };
  } catch (error) {
    if (!refreshRequestDispatched) markPreDispatchRefreshError(error);
    operationError = error;
    throw error;
  } finally {
    try {
      await client.stop();
    } catch (stopError) {
      if (operationError) {
        if (!refreshRequestDispatched) {
          const uncertain = new ManagedAuthRefreshError(
            "refresh_outcome_uncertain",
            "app-server shutdown failed before refresh dispatch could be proven",
            {
              recoveryPath: stagingHome,
              recoveryReason: "app_server_shutdown_unknown",
            },
          );
          uncertain.cause = operationError;
          uncertain.cleanupWarnings = [
            ...(Array.isArray(operationError.cleanupWarnings)
              ? operationError.cleanupWarnings
              : []),
            "app_server_stop_failed",
          ];
          throw uncertain;
        }
        operationError.cleanupWarnings = [
          ...(Array.isArray(operationError.cleanupWarnings)
            ? operationError.cleanupWarnings
            : []),
          "app_server_stop_failed",
        ];
      } else if (refreshRequestDispatched) {
        const uncertain = new ManagedAuthRefreshError(
          "refresh_outcome_uncertain",
          "app-server cleanup failed after the token refresh request was dispatched",
          {
            recoveryPath: stagingHome,
            recoveryReason: "account_read_outcome_unknown",
          },
        );
        uncertain.cause = stopError;
        throw uncertain;
      } else {
        throw stopError;
      }
    }
  }
}

async function assertLockHeldBeforeRefresh(lock) {
  try {
    await lock.assertHeld();
  } catch (error) {
    const beforeRefresh = new ManagedAuthRefreshError(
      "lock_lost_before_refresh",
      "authority lock was lost before token refresh started",
      { retryable: true },
    );
    beforeRefresh.cause = error;
    markPreDispatchRefreshError(beforeRefresh);
    throw beforeRefresh;
  }
}

async function runRefreshWhileLockHeld(lock, operation, recoveryPath) {
  await assertLockHeldBeforeRefresh(lock);
  const controller = new AbortController();
  const refresh = Promise.resolve().then(() => operation(controller.signal));
  if (typeof lock.waitForLoss !== "function") return refresh;
  const first = await Promise.race([
    refresh.then(
      (value) => ({ kind: "completed", value }),
      (error) => ({ error, kind: "failed" }),
    ),
    lock.waitForLoss().then((error) => ({ error, kind: "lost" })),
  ]);
  if (first.kind === "completed") return first.value;
  if (first.kind === "failed") throw first.error;
  controller.abort(first.error);
  await refresh.catch(() => {});
  const uncertain = new ManagedAuthRefreshError(
    "refresh_outcome_uncertain",
    "authority lock was lost while the token refresh outcome was uncertain",
    {
      recoveryPath,
      recoveryReason: "lock_lost_during_refresh",
    },
  );
  uncertain.cause = first.error;
  throw uncertain;
}

async function cleanupManagedRefreshArtifacts({ preserveStaging, stagingHome, stagingRoot }) {
  if (preserveStaging) return;
  if (stagingHome) {
    for (let attempt = 0; ; attempt += 1) {
      try {
        await rm(stagingHome, { recursive: true, force: true });
        break;
      } catch (error) {
        const delay = STAGING_CLEANUP_RETRY_DELAYS_MS[attempt];
        if (!delay || !["EBUSY", "ENOTEMPTY"].includes(error?.code)) throw error;
        await new Promise((resolve) => setTimeout(resolve, delay));
      }
    }
  }
  if (stagingRoot) {
    await rmdir(stagingRoot).catch((error) => {
      if (!["ENOENT", "ENOTEMPTY"].includes(error?.code)) throw error;
    });
  }
}

/**
 * Runs one managed credential refresh transaction.
 *
 * `runRefresh` is a trusted/test-only injection seam because it receives the
 * credential-bearing `stagingHome`. A trusted custom adapter receives
 * `assertLockHeldBeforeDispatch` and MUST await it immediately before
 * dispatching `account/read(refreshToken=true)`. It must classify every
 * failure after that dispatch as a non-retryable `ManagedAuthRefreshError`
 * with code `refresh_outcome_uncertain`. The default adapter performs the
 * assertion and marks failures proven to occur before dispatch; every other
 * adapter failure is conservatively wrapped as post-dispatch uncertainty so
 * the durable staging sentinel cannot be reaped before operator recovery.
 */
export async function refreshManagedAuthRecord({
  acquireLock = acquireAuthorityLock,
  authHome,
  cleanupArtifacts = cleanupManagedRefreshArtifacts,
  codexBin = "codex",
  readCanonicalSource = readManagedAuthSnapshot,
  runRefresh = runCodexManagedRefresh,
  syncStagingDirectory = syncDirectoryPath,
  syncDirectory = syncParentDirectory,
  verifyPromoted = readManagedAuthSnapshot,
  writeStagingFile = writeFileDurably,
}) {
  const authority = await resolveAuthorityHome(authHome);
  let lock;
  try {
    lock = await acquireLock(authority);
    await assertAuthorityHomeCurrent(authority);
  } catch (error) {
    await lock?.release().catch(() => {});
    await authority.handle.close().catch(() => {});
    throw error;
  }
  const authorityHome = authority.path;
  let before;
  let stagingHome;
  let stagingRoot;
  let preserveStaging = false;
  let outcome;
  let primaryError;
  const committedWarnings = [];
  try {
    await assertAuthorityHomeCurrent(authority);
    const recoveryPaths = await findRecoveryArtifacts(authority);
    ensure(
      recoveryPaths.length === 0,
      "recovery_required",
      "managed authority contains unresolved refresh recovery artifacts",
      {
        recoveryPath: recoveryPaths[0],
        recoveryPaths,
        recoveryReason: "orphaned_refresh_artifacts",
      },
    );
    before = await readManagedAuthSnapshot(authorityHome);
    await assertAuthorityHomeCurrent(authority);
    ({ stagingHome, stagingRoot } = await createStagingHome(authority, before.raw, {
      syncStagingDirectory,
      writeStagingFile,
    }));
    await assertAuthorityHomeCurrent(authority);
    let refreshResult;
    let refreshError;
    try {
      refreshResult = await runRefreshWhileLockHeld(
        lock,
        (signal) =>
          runRefresh({
            assertLockHeldBeforeDispatch: () => assertLockHeldBeforeRefresh(lock),
            codexBin,
            signal,
            stagingHome,
          }),
        stagingHome,
      );
    } catch (error) {
      refreshError = error;
    }
    // Resolve the authority identity before following the staging pathname.
    // If the authority was replaced during refresh, that replacement is the
    // decisive failure and the displaced volume must be recovered directly.
    await assertAuthorityHomeCurrent(authority);
    if (refreshError && isKnownPreDispatchRefreshError(refreshError)) throw refreshError;
    if (refreshError?.code !== "refresh_outcome_uncertain" && refreshError) {
      const uncertain = new ManagedAuthRefreshError(
        "refresh_outcome_uncertain",
        "refresh adapter failed without proving that account/read was not dispatched",
        {
          recoveryPath: stagingHome,
          recoveryReason: "unclassified_adapter_failure",
        },
      );
      uncertain.cause = refreshError;
      refreshError = uncertain;
    }
    try {
      await syncRefreshedStagingAuth(stagingHome, syncStagingDirectory);
    } catch (error) {
      // Recheck before exposing stagingHome as a recovery path. A concurrent
      // authority replacement makes that pathname stale and takes precedence.
      await assertAuthorityHomeCurrent(authority);
      if (refreshError && !error.cause) error.cause = refreshError;
      throw error;
    }
    await assertAuthorityHomeCurrent(authority);
    if (refreshError) throw refreshError;
    // Once account/read returns, every subsequent failure is conservatively
    // treated as post-dispatch. The OAuth service may have consumed the old
    // refresh token even when local staged bytes still match the source.
    preserveStaging = true;
    await assertAuthorityHomeCurrent(authority);
    ensure(
      rpcAuditMatches(refreshResult?.rpcAudit),
      "unexpected_rpc_sequence",
      "managed authority used an unexpected app-server RPC sequence",
      {
        recoveryPath: stagingHome,
        recoveryReason: "post_dispatch_validation_failed",
      },
    );

    const staged = await readManagedAuthSnapshot(stagingHome);
    await lock.assertHeld();
    ensure(
      refreshResult?.response &&
        typeof refreshResult.response === "object" &&
        !Array.isArray(refreshResult.response),
      "invalid_refresh_response",
      "account/read returned an invalid response",
      {
        recoveryPath: stagingHome,
        recoveryReason: "post_dispatch_validation_failed",
      },
    );
    if (refreshResult.response.requiresOpenaiAuth === true && refreshResult.response.account === null) {
      fail("reauth_required", "managed authority credentials require interactive login");
    }
    ensure(
      staged.accountId === before.accountId,
      "account_identity_changed",
      "managed refresh changed the ChatGPT account identity",
      { recoveryPath: stagingHome },
    );
    ensure(
      staged.userId === before.userId,
      "user_identity_changed",
      "managed refresh changed the ChatGPT user identity",
      { recoveryPath: stagingHome },
    );
    ensure(
      lastRefreshAdvanced(before.lastRefreshAt, staged.lastRefreshAt),
      "refresh_not_observed",
      "account/read completed without an observable token refresh",
      {
        recoveryPath: stagingHome,
        recoveryReason: "post_dispatch_validation_failed",
      },
    );
    ensure(
      staged.accessTokenFingerprint !== before.accessTokenFingerprint,
      "access_token_unchanged",
      "managed refresh did not produce a new access token",
      {
        recoveryPath: stagingHome,
        recoveryReason: "post_dispatch_validation_failed",
      },
    );
    ensure(
      Number.isFinite(staged.expiresAtUnixSeconds) &&
        staged.expiresAtUnixSeconds - Math.floor(Date.now() / 1000) >=
          MIN_REFRESHED_TOKEN_VALIDITY_SECONDS,
      "refreshed_access_token_invalid",
      "managed refresh produced an access token without sufficient remaining validity",
      { recoveryPath: stagingHome },
    );

    const promotion = await atomicallyPromoteAuth(
      authority,
      staged.raw,
      before,
      () => lock.assertHeld(),
      (source, destination) => lock.renameWhileHeld(source, destination),
      readCanonicalSource,
      syncDirectory,
    );
    committedWarnings.push(...promotion.warnings);
    let promoted;
    try {
      await assertAuthorityHomeCurrent(authority);
      await lock.assertHeld();
      promoted = await verifyPromoted(authorityHome);
      await assertAuthorityHomeCurrent(authority);
      await lock.assertHeld();
      ensure(
        promoted.raw === staged.raw &&
          promoted.accountId === before.accountId &&
          promoted.userId === before.userId,
        "promotion_verification_failed",
        "promoted authority state failed verification",
        { recoveryPath: stagingHome },
      );
      preserveStaging = promotion.warnings.length > 0;
    } catch (error) {
      if (error?.code === "authority_home_replaced") throw error;
      // A verify read can fail because its authority pathname was replaced.
      // Revalidate before wrapping it with a stale staging recovery path.
      await assertAuthorityHomeCurrent(authority);
      if (error?.code === "promotion_verification_failed") throw error;
      const verificationError = new ManagedAuthRefreshError(
        "promotion_verification_failed",
        "promoted authority state could not be verified while the lock was held",
        { recoveryPath: stagingHome },
      );
      verificationError.cause = error;
      throw verificationError;
    }

    outcome = withSensitiveProperties({
      codexUserAgent: refreshResult.initializeResult?.userAgent ?? null,
      comparisons: {
        accessTokenChanged: true,
        accountContinuity: true,
        userContinuity: true,
        authFileChanged: promoted.authFileFingerprint !== before.authFileFingerprint,
        lastRefreshAdvanced: true,
        refreshTokenChanged:
          promoted.refreshTokenFingerprint !== before.refreshTokenFingerprint,
      },
      expiresAt: promoted.expiresAt,
      fileMode: promoted.fileMode,
      parentDirectorySynced: promotion.parentDirectorySynced,
      planType: promoted.planType,
      ...(preserveStaging ? { recoveryPath: stagingHome } : {}),
      rpcAudit: refreshResult.rpcAudit,
    }, {
      accessToken: promoted.accessToken,
      accountId: promoted.accountId,
      redactionValues: [...new Set([...before.redactionValues, ...promoted.redactionValues])],
    });
  } catch (error) {
    if (error?.code === "refresh_outcome_uncertain") preserveStaging = true;
    if (error?.code === "staging_recovery_not_durable") preserveStaging = true;
    if (
      stagingHome &&
      error instanceof AdvisoryLockError &&
      ["lock_lost", "lock_replaced"].includes(error.code)
    ) {
      const uncertain = new ManagedAuthRefreshError(
        "refresh_outcome_uncertain",
        "authority lock was lost after token refresh started",
        {
          recoveryPath: stagingHome,
          recoveryReason: "lock_lost_during_refresh",
        },
      );
      uncertain.cause = error;
      error = uncertain;
      preserveStaging = true;
    }
    const authorityReplaced = error?.code === "authority_home_replaced";
    if (stagingHome && before && !preserveStaging && !authorityReplaced) {
      try {
        const stagedAfterFailure = await readManagedAuthSnapshot(stagingHome);
        preserveStaging =
          stagedAfterFailure.authFileFingerprint !== before.authFileFingerprint;
      } catch {
        // A malformed or partially written staged record may be the only copy
        // left after a refresh-token rotation. Preserve it for manual recovery.
        preserveStaging = true;
      }
    }
    if (preserveStaging && stagingHome && !authorityReplaced) {
      if (!(error instanceof ManagedAuthRefreshError)) {
        const uncertain = new ManagedAuthRefreshError(
          "refresh_outcome_uncertain",
          "token refresh may have rotated credentials before the adapter failed",
          {
            recoveryPath: stagingHome,
            recoveryReason: "post_dispatch_outcome_uncertain",
          },
        );
        uncertain.cause = error;
        error = uncertain;
      } else {
        error.retryable = false;
        error.recoveryReason ??= "post_dispatch_outcome_uncertain";
        attachRecoveryPaths(error, [stagingHome]);
      }
    }
    primaryError = error;
  }

  const cleanupWarnings = [...committedWarnings];
  let authorityCurrent = true;
  try {
    await assertAuthorityHomeCurrent(authority);
  } catch (error) {
    authorityCurrent = false;
    if (primaryError && primaryError !== error && !error.cause) {
      error.cause = primaryError;
    }
    primaryError = error;
    cleanupWarnings.push("authority_home_replaced");
  }
  if (authorityCurrent) {
    try {
      await cleanupArtifacts({ preserveStaging, stagingHome, stagingRoot });
    } catch {
      cleanupWarnings.push("staging_cleanup_failed");
      if (stagingHome) {
        try {
          await lstat(stagingHome);
          if (primaryError && typeof primaryError === "object") {
            attachRecoveryPaths(primaryError, [stagingHome]);
          }
          if (outcome) outcome.recoveryPath ??= stagingHome;
        } catch {
          // The staging home was removed before a later cleanup step failed.
        }
      }
    }
  }
  try {
    await releaseAuthorityLock(lock);
  } catch {
    cleanupWarnings.push("lock_release_failed");
  }
  try {
    await authority.handle.close();
  } catch {
    cleanupWarnings.push("authority_guard_close_failed");
  }
  if (primaryError) {
    if (cleanupWarnings.length > 0 && typeof primaryError === "object") {
      primaryError.cleanupWarnings = [
        ...(Array.isArray(primaryError.cleanupWarnings) ? primaryError.cleanupWarnings : []),
        ...cleanupWarnings,
      ];
    }
    throw primaryError;
  }
  outcome.cleanupWarnings = cleanupWarnings;
  return outcome;
}

export class ManagedAuthRefreshAuthority {
  constructor({ authHome, codexBin, refreshRecord = refreshManagedAuthRecord }) {
    this.authHome = authHome;
    this.codexBin = codexBin;
    this.refreshRecord = refreshRecord;
    this.generation = 0;
    this.refreshExecutions = 0;
    this.inFlight = null;
  }

  refresh() {
    if (this.inFlight) return this.inFlight;
    const task = this.#refreshOnce();
    this.inFlight = task;
    task.then(
      () => {
        if (this.inFlight === task) this.inFlight = null;
      },
      () => {
        if (this.inFlight === task) this.inFlight = null;
      },
    );
    return task;
  }

  async #refreshOnce() {
    this.refreshExecutions += 1;
    const record = await this.refreshRecord({ authHome: this.authHome, codexBin: this.codexBin });
    this.generation += 1;
    return withSensitiveProperties({
      codexUserAgent: record.codexUserAgent,
      cleanupWarnings: record.cleanupWarnings,
      comparisons: record.comparisons,
      expiresAt: record.expiresAt,
      fileMode: record.fileMode,
      generation: this.generation,
      parentDirectorySynced: record.parentDirectorySynced,
      planType: record.planType,
      ...(record.recoveryPath ? { recoveryPath: record.recoveryPath } : {}),
      rpcAudit: record.rpcAudit,
    }, {
      accessToken: record.accessToken,
      accountId: record.accountId,
      redactionValues: record.redactionValues,
    });
  }
}

export function authorityStagingDirectory(authHome) {
  return join(resolve(authHome), STAGING_DIRECTORY);
}

export function authorityLockPath(authHome) {
  return join(resolve(authHome), LOCK_FILE);
}
