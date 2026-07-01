import { createHash, randomUUID } from "node:crypto";
import { constants } from "node:fs";
import {
  chmod,
  lstat,
  mkdir,
  mkdtemp,
  open,
  readFile,
  realpath,
  rename,
  rm,
  rmdir,
  writeFile,
} from "node:fs/promises";
import { homedir } from "node:os";
import { join, resolve } from "node:path";

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

function sha256(value) {
  return createHash("sha256").update(value).digest("hex");
}

function fingerprint(value) {
  return `sha256:${sha256(value).slice(0, 24)}`;
}

function fail(code, message, { retryable = false, recoveryPath } = {}) {
  throw new ManagedAuthRefreshError(code, message, { retryable, recoveryPath });
}

function ensure(condition, code, message, options) {
  if (!condition) fail(code, message, options);
}

function decodeJwtPayload(token, label) {
  const parts = token.split(".");
  ensure(parts.length >= 2, "invalid_auth_record", `${label} is not a JWT`);
  try {
    return JSON.parse(Buffer.from(parts[1], "base64url").toString("utf8"));
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

function lastRefreshAdvanced(before, after) {
  if (typeof after !== "string") return false;
  const afterTime = Date.parse(after);
  if (!Number.isFinite(afterTime)) return false;
  if (before === null || before === undefined) return true;
  const beforeTime = Date.parse(before);
  return Number.isFinite(beforeTime) && afterTime > beforeTime;
}

async function resolveAuthorityHome(authHome) {
  const authorityHome = await realpath(resolve(authHome));
  const defaultCodexHome = await realpath(join(homedir(), ".codex")).catch(() => null);
  ensure(
    defaultCodexHome === null || authorityHome !== defaultCodexHome,
    "unsafe_auth_home",
    "refusing to mutate the default Codex home; use a dedicated authority home",
  );
  return authorityHome;
}

async function acquireAuthorityLock(authorityHome) {
  const lockPath = join(authorityHome, LOCK_FILE);
  let lock;
  try {
    lock = await acquireAdvisoryLock(lockPath);
    await chmod(lockPath, 0o600);
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

async function releaseAuthorityLock(lock) {
  await lock.release();
}

async function createStagingHome(authorityHome, rawAuth) {
  const stagingRoot = join(authorityHome, STAGING_DIRECTORY);
  await mkdir(stagingRoot, { recursive: true, mode: 0o700 });
  const stagingRootStat = await lstat(stagingRoot);
  ensure(
    stagingRootStat.isDirectory() && !stagingRootStat.isSymbolicLink(),
    "unsafe_staging_path",
    "authority staging root must be a real directory",
  );
  await chmod(stagingRoot, 0o700);

  let stagingHome;
  try {
    stagingHome = await mkdtemp(join(stagingRoot, "attempt-"));
    await chmod(stagingHome, 0o700);
    await writeFile(join(stagingHome, "auth.json"), rawAuth, { flag: "wx", mode: 0o600 });
    await writeFile(join(stagingHome, "config.toml"), 'cli_auth_credentials_store = "file"\n', {
      flag: "wx",
      mode: 0o600,
    });
    return { stagingHome, stagingRoot };
  } catch (error) {
    if (stagingHome) await rm(stagingHome, { recursive: true, force: true }).catch(() => {});
    throw error;
  }
}

async function syncParentDirectory(path) {
  let handle;
  try {
    handle = await open(path, constants.O_RDONLY);
    await handle.sync();
    return true;
  } catch (error) {
    if (["EINVAL", "ENOTSUP", "EISDIR", "EPERM"].includes(error?.code)) return false;
    throw error;
  } finally {
    await handle?.close().catch(() => {});
  }
}

async function atomicallyPromoteAuth(authorityHome, rawAuth, expectedSource) {
  const destination = join(authorityHome, "auth.json");
  const temporary = join(authorityHome, `.auth.json.next-${randomUUID()}`);
  let handle;
  let renamed = false;
  try {
    handle = await open(
      temporary,
      constants.O_WRONLY | constants.O_CREAT | constants.O_EXCL | constants.O_NOFOLLOW,
      0o600,
    );
    await handle.writeFile(rawAuth);
    await handle.sync();
    await handle.close();
    handle = undefined;

    const currentSource = await readManagedAuthSnapshot(authorityHome);
    ensure(
      currentSource.authFileFingerprint === expectedSource.authFileFingerprint &&
        sameFileIdentity(currentSource.fileIdentity, expectedSource.fileIdentity),
      "authority_conflict_after_refresh",
      "canonical authority state changed while the staged refresh was running",
    );

    await rename(temporary, destination);
    renamed = true;
    return { parentDirectorySynced: await syncParentDirectory(authorityHome) };
  } finally {
    await handle?.close().catch(() => {});
    if (!renamed) await rm(temporary, { force: true }).catch(() => {});
  }
}

export class ManagedAuthRefreshError extends Error {
  constructor(code, message, { retryable = false, recoveryPath } = {}) {
    super(message);
    this.name = "ManagedAuthRefreshError";
    this.code = code;
    this.retryable = retryable;
    if (recoveryPath) this.recoveryPath = recoveryPath;
  }
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
    handle = await open(authPath, constants.O_RDONLY | constants.O_NOFOLLOW);
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

  return {
    accessToken: auth.tokens.access_token,
    accessTokenFingerprint: fingerprint(auth.tokens.access_token),
    accountId: accountIds[0],
    authFileFingerprint: fingerprint(raw),
    authPath,
    expiresAt:
      typeof accessPayload.exp === "number" ? new Date(accessPayload.exp * 1000).toISOString() : null,
    expiresAtUnixSeconds:
      typeof accessPayload.exp === "number" ? accessPayload.exp : null,
    fileIdentity: { dev: fileStat.dev, ino: fileStat.ino },
    fileMode: (fileStat.mode & 0o777).toString(8).padStart(4, "0"),
    lastRefreshAt: auth.last_refresh ?? null,
    planType: accessAuth.chatgpt_plan_type ?? idAuth.chatgpt_plan_type ?? null,
    raw,
    redactionValues: collectRedactionValues(auth, accessPayload, idPayload),
    refreshTokenFingerprint: fingerprint(auth.tokens.refresh_token),
  };
}

export async function runCodexManagedRefresh({ codexBin = "codex", stagingHome }) {
  const client = new AppServerClient({ codexBin, codexHome: stagingHome, timeoutMs: 120_000 });
  try {
    await client.start();
    const initializeResult = await client.initialize(false);
    const response = await client.request("account/read", { refreshToken: true });
    return {
      initializeResult,
      response,
      rpcAudit: client.rpcMethodAudit(),
    };
  } finally {
    await client.stop();
  }
}

export async function refreshManagedAuthRecord({
  authHome,
  codexBin = "codex",
  runRefresh = runCodexManagedRefresh,
}) {
  const authorityHome = await resolveAuthorityHome(authHome);
  const lock = await acquireAuthorityLock(authorityHome);
  let before;
  let stagingHome;
  let stagingRoot;
  let preserveStaging = false;
  try {
    before = await readManagedAuthSnapshot(authorityHome);
    ({ stagingHome, stagingRoot } = await createStagingHome(authorityHome, before.raw));
    const refreshResult = await runRefresh({ codexBin, stagingHome });
    ensure(
      rpcAuditMatches(refreshResult?.rpcAudit),
      "unexpected_rpc_sequence",
      "managed authority used an unexpected app-server RPC sequence",
    );

    const staged = await readManagedAuthSnapshot(stagingHome);
    lock.assertHeld();
    preserveStaging = staged.authFileFingerprint !== before.authFileFingerprint;
    ensure(
      refreshResult?.response &&
        typeof refreshResult.response === "object" &&
        !Array.isArray(refreshResult.response),
      "invalid_refresh_response",
      "account/read returned an invalid response",
      { retryable: true },
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
      lastRefreshAdvanced(before.lastRefreshAt, staged.lastRefreshAt),
      "refresh_not_observed",
      "account/read completed without an observable token refresh",
      { retryable: true, recoveryPath: preserveStaging ? stagingHome : undefined },
    );
    ensure(
      staged.accessTokenFingerprint !== before.accessTokenFingerprint,
      "access_token_unchanged",
      "managed refresh did not produce a new access token",
      { retryable: true, recoveryPath: stagingHome },
    );
    ensure(
      Number.isFinite(staged.expiresAtUnixSeconds) &&
        staged.expiresAtUnixSeconds - Math.floor(Date.now() / 1000) >=
          MIN_REFRESHED_TOKEN_VALIDITY_SECONDS,
      "refreshed_access_token_invalid",
      "managed refresh produced an access token without sufficient remaining validity",
      { recoveryPath: stagingHome },
    );

    const promotion = await atomicallyPromoteAuth(authorityHome, staged.raw, before);
    lock.assertHeld();
    const promoted = await readManagedAuthSnapshot(authorityHome);
    ensure(
      promoted.authFileFingerprint === staged.authFileFingerprint &&
        promoted.accountId === before.accountId,
      "promotion_verification_failed",
      "promoted authority state failed verification",
      { recoveryPath: stagingHome },
    );
    preserveStaging = false;

    return {
      accessToken: promoted.accessToken,
      accountId: promoted.accountId,
      codexUserAgent: refreshResult.initializeResult?.userAgent ?? null,
      comparisons: {
        accessTokenChanged: true,
        accountContinuity: true,
        authFileChanged: promoted.authFileFingerprint !== before.authFileFingerprint,
        lastRefreshAdvanced: true,
        refreshTokenChanged:
          promoted.refreshTokenFingerprint !== before.refreshTokenFingerprint,
      },
      expiresAt: promoted.expiresAt,
      fileMode: promoted.fileMode,
      parentDirectorySynced: promotion.parentDirectorySynced,
      planType: promoted.planType,
      redactionValues: [...new Set([...before.redactionValues, ...promoted.redactionValues])],
      rpcAudit: refreshResult.rpcAudit,
    };
  } catch (error) {
    if (stagingHome && before && !preserveStaging) {
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
    if (preserveStaging && stagingHome && error && typeof error === "object") {
      error.recoveryPath ??= stagingHome;
    }
    throw error;
  } finally {
    try {
      if (stagingHome && !preserveStaging) {
        await rm(stagingHome, { recursive: true, force: true });
      }
      if (stagingRoot && !preserveStaging) {
        await rmdir(stagingRoot).catch((error) => {
          if (!["ENOENT", "ENOTEMPTY"].includes(error?.code)) throw error;
        });
      }
    } finally {
      await releaseAuthorityLock(lock);
    }
  }
}

export class ManagedAuthRefreshAuthority {
  constructor({ authHome, codexBin = "codex", refreshRecord = refreshManagedAuthRecord }) {
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
    return {
      accessToken: record.accessToken,
      accountId: record.accountId,
      codexUserAgent: record.codexUserAgent,
      comparisons: record.comparisons,
      expiresAt: record.expiresAt,
      fileMode: record.fileMode,
      generation: this.generation,
      parentDirectorySynced: record.parentDirectorySynced,
      planType: record.planType,
      redactionValues: record.redactionValues,
      rpcAudit: record.rpcAudit,
    };
  }
}

export function authorityStagingDirectory(authHome) {
  return join(resolve(authHome), STAGING_DIRECTORY);
}

export function authorityLockPath(authHome) {
  return join(resolve(authHome), LOCK_FILE);
}
