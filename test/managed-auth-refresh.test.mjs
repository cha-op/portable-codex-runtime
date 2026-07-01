import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdirSync, renameSync } from "node:fs";
import {
  chmod,
  link,
  mkdtemp,
  mkdir,
  open,
  readFile,
  readdir,
  realpath,
  rename,
  rm,
  stat,
  symlink,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";

import { JsonRpcError } from "../src/app-server-auth-probe.mjs";
import {
  ManagedAuthRefreshAuthority,
  ManagedAuthRefreshError,
  authorityDirectoryPermissionsAreSafe,
  authorityLockPath,
  authorityStagingDirectory,
  managedAuthRefreshErrorMetadata,
  managedAuthRefreshFailureReport,
  readManagedAuthSnapshot,
  refreshManagedAuthRecord,
  runCodexManagedRefresh,
} from "../src/managed-auth-refresh.mjs";
import { AdvisoryLockError, acquireAdvisoryLock } from "../src/advisory-lock.mjs";

const ACCOUNT_ID = "123e4567-e89b-42d3-a456-426614174088";
const USER_ID = "user-123e4567-e89b-42d3-a456-426614174088";
const DEFAULT_TEST_TOKEN_EXPIRY_UNIX_SECONDS = Math.floor(Date.now() / 1000) + 3600;
const UNSAFE_HOME_FIXTURE = fileURLToPath(
  new URL("../fixtures/probe-unsafe-authority-home.mjs", import.meta.url),
);
const RPC_AUDIT = [
  { kind: "request", method: "initialize" },
  { kind: "notification", method: "initialized" },
  { kind: "request", method: "account/read" },
];

test("authority directory permission policy accepts only trusted owners and modes", () => {
  const options = { brokerUid: 501, disallowedModeBits: 0o022 };
  assert.equal(
    authorityDirectoryPermissionsAreSafe(
      { isDirectory: true, mode: 0o40755, uid: 501 },
      options,
    ),
    true,
  );
  assert.equal(
    authorityDirectoryPermissionsAreSafe(
      { isDirectory: true, mode: 0o40555, uid: 0 },
      { ...options, allowRootOwner: true },
    ),
    true,
  );
  assert.equal(
    authorityDirectoryPermissionsAreSafe(
      { isDirectory: true, mode: 0o40555, uid: 777 },
      { ...options, allowRootOwner: true },
    ),
    false,
  );
  assert.equal(
    authorityDirectoryPermissionsAreSafe(
      { isDirectory: true, mode: 0o40777, uid: 0 },
      { ...options, allowRootOwner: true },
    ),
    false,
  );
  assert.equal(
    authorityDirectoryPermissionsAreSafe(
      { isDirectory: true, mode: 0o41777, uid: 0 },
      {
        ...options,
        allowRootOwner: true,
        allowStickyShared: true,
        childUid: 501,
      },
    ),
    true,
  );
  assert.equal(
    authorityDirectoryPermissionsAreSafe(
      { isDirectory: true, mode: 0o41777, uid: 0 },
      {
        ...options,
        allowRootOwner: true,
        allowStickyShared: true,
        childUid: 777,
      },
    ),
    false,
  );
  assert.equal(
    authorityDirectoryPermissionsAreSafe(
      { isDirectory: true, mode: 0o40700, uid: 501 },
      { brokerUid: 501, disallowedModeBits: 0o077, requiredModeBits: 0o700 },
    ),
    true,
  );
  assert.equal(
    authorityDirectoryPermissionsAreSafe(
      { isDirectory: true, mode: 0o40500, uid: 501 },
      { brokerUid: 501, disallowedModeBits: 0o077, requiredModeBits: 0o700 },
    ),
    false,
  );
  assert.equal(
    authorityDirectoryPermissionsAreSafe(
      { isDirectory: true, mode: 0o40700n, uid: 501n },
      { brokerUid: 501, disallowedModeBits: 0o077, requiredModeBits: 0o700 },
    ),
    true,
  );
});

function encodeJwt(payload) {
  const encode = (value) => Buffer.from(JSON.stringify(value)).toString("base64url");
  return `${encode({ alg: "none", typ: "JWT" })}.${encode(payload)}.${encode("signature")}`;
}

function authDocument({
  accessMarker,
  accountId = ACCOUNT_ID,
  expiresAtUnixSeconds = DEFAULT_TEST_TOKEN_EXPIRY_UNIX_SECONDS,
  lastRefresh,
  refreshToken,
  userId = USER_ID,
}) {
  const claims = {
    chatgpt_account_id: accountId,
    chatgpt_plan_type: "enterprise",
    chatgpt_user_id: userId,
  };
  return {
    OPENAI_API_KEY: null,
    auth_mode: "chatgpt",
    last_refresh: lastRefresh,
    tokens: {
      access_token: encodeJwt({
        marker: accessMarker,
        exp: expiresAtUnixSeconds,
        "https://api.openai.com/auth": claims,
      }),
      account_id: accountId,
      id_token: encodeJwt({ "https://api.openai.com/auth": claims }),
      refresh_token: refreshToken,
    },
  };
}

async function createAuthorityHome() {
  const root = await mkdtemp(join(tmpdir(), "portable-managed-authority-"));
  const authHome = join(root, "authority-home");
  await mkdir(authHome, { mode: 0o700 });
  await writeFile(
    join(authHome, "auth.json"),
    `${JSON.stringify(
      authDocument({
        accessMarker: "before",
        lastRefresh: "2026-07-01T08:00:00.000Z",
        refreshToken: "refresh-before-sensitive",
      }),
    )}\n`,
    { mode: 0o600 },
  );
  return { authHome, root };
}

async function replaceStagedAuth(
  stagingHome,
  {
    accessMarker = "after",
    accountId = ACCOUNT_ID,
    expiresAtUnixSeconds,
    lastRefresh = "2026-07-01T08:01:00.000Z",
    refreshToken = "refresh-after-sensitive",
    userId,
  } = {},
) {
  await writeFile(
    join(stagingHome, "auth.json"),
    `${JSON.stringify(
      authDocument({
        accessMarker,
        accountId,
        expiresAtUnixSeconds,
        lastRefresh,
        refreshToken,
        userId,
      }),
    )}\n`,
    { mode: 0o600 },
  );
}

function failHandleCloseAfterClosing(handle, closeError) {
  const close = handle.close.bind(handle);
  handle.close = async () => {
    await close();
    throw closeError;
  };
  return handle;
}

function successResponse() {
  return {
    initializeResult: { userAgent: "codex-test" },
    response: {
      account: { type: "chatgpt", planType: "enterprise" },
      requiresOpenaiAuth: true,
    },
    rpcAudit: RPC_AUDIT,
  };
}

test("managed refresh stages Codex writes and atomically promotes verified auth", async () => {
  const { authHome, root } = await createAuthorityHome();
  try {
    const beforeStat = await stat(join(authHome, "auth.json"));
    const result = await refreshManagedAuthRecord({
      authHome,
      runRefresh: async ({ stagingHome }) => {
        await replaceStagedAuth(stagingHome);
        return successResponse();
      },
    });
    const after = await readManagedAuthSnapshot(authHome);
    const afterStat = await stat(join(authHome, "auth.json"));

    assert.deepEqual(result.comparisons, {
      accessTokenChanged: true,
      accountContinuity: true,
      authFileChanged: true,
      lastRefreshAdvanced: true,
      refreshTokenChanged: true,
      userContinuity: true,
    });
    assert.deepEqual(result.rpcAudit.map(({ method }) => method), [
      "initialize",
      "initialized",
      "account/read",
    ]);
    assert.equal(after.fileMode, "0600");
    assert.notEqual(afterStat.ino, beforeStat.ino);
    assert.equal(JSON.stringify(after).includes(after.accessToken), false);
    assert.equal(JSON.stringify(result).includes(result.accessToken), false);
    assert.equal((await stat(authorityLockPath(authHome))).isFile(), true);
    await assert.rejects(stat(authorityStagingDirectory(authHome)), /ENOENT/);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("managed refresh restores writable credential modes under a restrictive umask", async () => {
  const { authHome, root } = await createAuthorityHome();
  const previousUmask = process.umask(0o200);
  try {
    const result = await refreshManagedAuthRecord({
      authHome,
      runRefresh: async ({ stagingHome }) => {
        await replaceStagedAuth(stagingHome);
        const stagedStat = await stat(join(stagingHome, "auth.json"));
        assert.equal(stagedStat.mode & 0o777, 0o600);
        return successResponse();
      },
    });

    const promotedStat = await stat(join(authHome, "auth.json"));
    assert.equal(promotedStat.mode & 0o777, 0o600);
    assert.equal(result.fileMode, "0600");
  } finally {
    process.umask(previousUmask);
    await rm(root, { recursive: true, force: true });
  }
});

test("authority coalesces concurrent refresh callers into one generation", async () => {
  let release;
  const gate = new Promise((resolve) => {
    release = resolve;
  });
  let executions = 0;
  const authority = new ManagedAuthRefreshAuthority({
    authHome: "/dedicated-test-home",
    refreshRecord: async () => {
      executions += 1;
      await gate;
      return {
        accessToken: "access-sensitive",
        accountId: ACCOUNT_ID,
        codexUserAgent: "codex-test",
        comparisons: { accessTokenChanged: true },
        expiresAt: "2026-07-01T09:00:00.000Z",
        fileMode: "0600",
        parentDirectorySynced: true,
        planType: "enterprise",
        recoveryPath: "/dedicated-test-home/staging/attempt-1",
        redactionValues: ["access-sensitive", ACCOUNT_ID],
        rpcAudit: RPC_AUDIT,
      };
    },
  });

  const callers = Array.from({ length: 20 }, () => authority.refresh());
  release();
  const results = await Promise.all(callers);

  assert.equal(executions, 1);
  assert.equal(authority.refreshExecutions, 1);
  assert.deepEqual(
    results.map(({ generation }) => generation),
    Array(20).fill(1),
  );
  assert.equal(results.every((result) => result.accessToken === "access-sensitive"), true);
  assert.equal(results[0].recoveryPath, "/dedicated-test-home/staging/attempt-1");
  assert.equal(JSON.stringify(results[0]).includes("access-sensitive"), false);
  assert.equal(JSON.stringify(results[0]).includes(ACCOUNT_ID), false);
});

test("error metadata exposes recovery controls without serializing the error", () => {
  const error = new ManagedAuthRefreshError("refresh_failed", "secret-bearing message", {
    recoveryPath: "/dedicated/staging/attempt-1",
    retryable: false,
  });
  error.secret = "must-not-be-returned";
  assert.deepEqual(managedAuthRefreshErrorMetadata(error), {
    code: "refresh_failed",
    retryable: false,
    recoveryPath: "/dedicated/staging/attempt-1",
    recoveryPaths: ["/dedicated/staging/attempt-1"],
  });
});

test("runCodexManagedRefresh uses only the managed account refresh choreography", async () => {
  const calls = [];
  const result = await runCodexManagedRefresh({
    assertLockHeldBeforeDispatch: async () => calls.push(["assert-lock-before-dispatch"]),
    codexBin: "/pinned/codex",
    stagingHome: "/isolated/staging-home",
    createClient: (options) => {
      calls.push(["create", options]);
      return {
        initialize: async (experimentalApi) => {
          calls.push(["initialize", experimentalApi]);
          return { userAgent: "codex-test" };
        },
        request: async (method, params) => {
          calls.push(["request", method, params]);
          return { account: { type: "chatgpt" }, requiresOpenaiAuth: true };
        },
        rpcMethodAudit: () => RPC_AUDIT,
        start: async () => calls.push(["start"]),
        stop: async () => calls.push(["stop"]),
      };
    },
  });
  assert.deepEqual(calls, [
    [
      "create",
      {
        codexBin: "/pinned/codex",
        codexHome: "/isolated/staging-home",
        timeoutMs: 120_000,
      },
    ],
    ["start"],
    ["initialize", false],
    ["assert-lock-before-dispatch"],
    ["request", "account/read", { refreshToken: true }],
    ["stop"],
  ]);
  assert.deepEqual(result.rpcAudit, RPC_AUDIT);
});

test("managed refresh rechecks lock ownership after initialize and before account/read", async () => {
  const { authHome, root } = await createAuthorityHome();
  let lockHeld = true;
  let lockAssertions = 0;
  let released = false;
  const requestMethods = [];
  try {
    let refreshError;
    try {
      await refreshManagedAuthRecord({
        authHome,
        acquireLock: async () => ({
          assertHeld: async () => {
            lockAssertions += 1;
            if (!lockHeld) throw new AdvisoryLockError("lock_lost", "synthetic lock loss");
          },
          release: async () => {
            released = true;
          },
          waitForLoss: () => new Promise(() => {}),
        }),
        codexBin: "/pinned/codex",
        runRefresh: (options) =>
          runCodexManagedRefresh({
            ...options,
            createClient: () => ({
              initialize: async () => {
                lockHeld = false;
                return { userAgent: "codex-test" };
              },
              request: async (method) => {
                requestMethods.push(method);
                return { account: { type: "chatgpt" }, requiresOpenaiAuth: true };
              },
              start: async () => {},
              stop: async () => {},
            }),
          }),
      });
    } catch (error) {
      refreshError = error;
    }
    assert.equal(refreshError.code, "lock_lost_before_refresh");
    assert.equal(refreshError.retryable, true);
    assert.deepEqual(requestMethods, []);
    assert.equal(lockAssertions, 2);
    assert.equal(released, true);
    await assert.rejects(stat(authorityStagingDirectory(authHome)), /ENOENT/);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("runCodexManagedRefresh always stops the app-server after request failure", async () => {
  let stopped = 0;
  await assert.rejects(
    runCodexManagedRefresh({
      codexBin: "/pinned/codex",
      stagingHome: "/isolated/staging-home",
      createClient: () => ({
        initialize: async () => ({}),
        request: async () => {
          throw new Error("synthetic request failure");
        },
        start: async () => {},
        stop: async () => {
          stopped += 1;
        },
      }),
    }),
    (error) =>
      error.code === "refresh_outcome_uncertain" &&
      error.retryable === false &&
      error.recoveryReason === "account_read_outcome_unknown" &&
      error.recoveryPath === "/isolated/staging-home",
  );
  assert.equal(stopped, 1);
});

test("managed refresh errors do not serialize JSON-RPC credential payloads", async () => {
  const syntheticToken = "synthetic-managed-json-rpc-refresh-token";
  const rpcError = new JsonRpcError("account/read", {
    data: { refreshToken: syntheticToken },
    message: `request failed with ${syntheticToken}`,
  });

  await assert.rejects(
    runCodexManagedRefresh({
      codexBin: "/pinned/codex",
      stagingHome: "/isolated/staging-home",
      createClient: () => ({
        initialize: async () => ({}),
        request: async () => {
          throw rpcError;
        },
        start: async () => {},
        stop: async () => {},
      }),
    }),
    (error) => {
      assert(error instanceof ManagedAuthRefreshError);
      assert.equal(error.code, "refresh_outcome_uncertain");
      assert.equal(error.cause, rpcError);
      assert.equal(Object.prototype.propertyIsEnumerable.call(error, "cause"), false);
      assert.equal(error.cause.payload.data.refreshToken, syntheticToken);
      assert.equal(JSON.stringify(error).includes(syntheticToken), false);
      assert.equal(
        JSON.stringify(managedAuthRefreshFailureReport(error)).includes(syntheticToken),
        false,
      );
      return true;
    },
  );
});

test("app-server stop failure cannot mask an uncertain refresh outcome", async () => {
  await assert.rejects(
    runCodexManagedRefresh({
      codexBin: "/pinned/codex",
      stagingHome: "/isolated/staging-home",
      createClient: () => ({
        initialize: async () => ({}),
        request: async () => {
          throw new Error("synthetic request failure");
        },
        start: async () => {},
        stop: async () => {
          throw new Error("synthetic stop failure");
        },
      }),
    }),
    (error) => {
      assert.equal(error.code, "refresh_outcome_uncertain");
      assert.deepEqual(error.cleanupWarnings, ["app_server_stop_failed"]);
      assert.match(error.cause.cause.message, /synthetic request failure/);
      assert.match(error.cleanupError.message, /synthetic stop failure/);
      assert.equal(Object.prototype.propertyIsEnumerable.call(error, "cleanupError"), false);
      return true;
    },
  );
});

test("successful account refresh with unconfirmed shutdown retains an unsynced gate", async () => {
  const { authHome, root } = await createAuthorityHome();
  const canonicalBefore = await readFile(join(authHome, "auth.json"), "utf8");
  const stopSecret = "synthetic-survived-sigkill-refresh-token";
  let syncCount = 0;
  try {
    let refreshError;
    try {
      await refreshManagedAuthRecord({
        authHome,
        codexBin: "/pinned/codex",
        syncStagingDirectory: async () => {
          syncCount += 1;
          return true;
        },
        runRefresh: (options) =>
          runCodexManagedRefresh({
            ...options,
            createClient: ({ codexHome }) => ({
              initialize: async () => ({}),
              request: async () => {
                await replaceStagedAuth(codexHome);
                return { account: { type: "chatgpt" }, requiresOpenaiAuth: true };
              },
              rpcMethodAudit: () => RPC_AUDIT,
              start: async () => {},
              stop: async () => {
                throw new Error(`codex app-server process group survived SIGKILL: ${stopSecret}`);
              },
            }),
          }),
      });
    } catch (error) {
      refreshError = error;
    }

    assert(refreshError instanceof ManagedAuthRefreshError);
    assert.equal(refreshError.code, "refresh_outcome_uncertain");
    assert.equal(refreshError.retryable, false);
    assert.equal(refreshError.recoveryReason, "app_server_shutdown_unknown");
    assert.deepEqual(refreshError.cleanupWarnings, ["app_server_stop_failed"]);
    assert.match(refreshError.cause.message, /survived SIGKILL/);
    assert.equal(Object.prototype.propertyIsEnumerable.call(refreshError, "cause"), false);
    assert.equal(syncCount, 2, "unquiesced staging must not receive a post-refresh sync");
    assert.equal(await readFile(join(authHome, "auth.json"), "utf8"), canonicalBefore);
    assert.equal((await stat(join(refreshError.recoveryPath, "auth.json"))).isFile(), true);
    assert.equal(JSON.stringify(refreshError).includes(stopSecret), false);

    let secondRefreshCalled = false;
    await assert.rejects(
      refreshManagedAuthRecord({
        authHome,
        runRefresh: async () => {
          secondRefreshCalled = true;
          return successResponse();
        },
      }),
      (error) => error.code === "recovery_required",
    );
    assert.equal(secondRefreshCalled, false);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("frozen post-dispatch errors are wrapped before stop-failure metadata is added", async () => {
  const { authHome, root } = await createAuthorityHome();
  const warningSecret = "synthetic-frozen-warning-refresh-token";
  const stopSecret = "synthetic-frozen-stop-refresh-token";
  const frozenError = new ManagedAuthRefreshError(
    "refresh_outcome_uncertain",
    "frozen adapter request failure",
    { recoveryReason: "frozen_adapter_failure", retryable: true },
  );
  frozenError.cleanupWarnings = ["parent_directory_sync_failed", warningSecret];
  Object.freeze(frozenError);
  let released = false;
  let syncCount = 0;
  try {
    let refreshError;
    try {
      await refreshManagedAuthRecord({
        authHome,
        acquireLock: async () => ({
          assertHeld: async () => {},
          release: async () => {
            released = true;
          },
        }),
        codexBin: "/pinned/codex",
        syncStagingDirectory: async () => {
          syncCount += 1;
          return true;
        },
        runRefresh: (options) =>
          runCodexManagedRefresh({
            ...options,
            createClient: () => ({
              initialize: async () => ({}),
              request: async () => ({
                account: { type: "chatgpt" },
                requiresOpenaiAuth: false,
              }),
              rpcMethodAudit: () => {
                throw frozenError;
              },
              start: async () => {},
              stop: async () => {
                throw new Error(`process group survived SIGKILL: ${stopSecret}`);
              },
            }),
          }),
      });
    } catch (error) {
      refreshError = error;
    }

    assert(refreshError instanceof ManagedAuthRefreshError);
    assert.notEqual(refreshError, frozenError);
    assert.equal(refreshError.code, "refresh_outcome_uncertain");
    assert.equal(refreshError.recoveryReason, "app_server_shutdown_unknown");
    assert.equal(refreshError.retryable, false);
    assert.equal(refreshError.cause, frozenError);
    assert.deepEqual(refreshError.cleanupWarnings, [
      "parent_directory_sync_failed",
      "app_server_stop_failed",
    ]);
    assert.match(refreshError.cleanupError.message, /survived SIGKILL/);
    assert.equal(released, true);
    assert.equal(syncCount, 2, "unquiesced frozen failure must not receive a post-refresh sync");
    assert.equal(JSON.stringify(refreshError).includes(warningSecret), false);
    assert.equal(JSON.stringify(refreshError).includes(stopSecret), false);
    assert.equal((await stat(join(refreshError.recoveryPath, "auth.json"))).isFile(), true);

    await assert.rejects(
      refreshManagedAuthRecord({ authHome }),
      (error) => error.code === "recovery_required",
    );
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("sealed custom adapter errors are normalized before recovery and cleanup metadata", async () => {
  const { authHome, root } = await createAuthorityHome();
  const warningSecret = "synthetic-sealed-warning-refresh-token";
  const sealedError = new ManagedAuthRefreshError(
    "refresh_outcome_uncertain",
    "sealed custom adapter failure",
    { recoveryReason: "sealed_adapter_uncertain", retryable: true },
  );
  sealedError.cleanupWarnings = ["parent_directory_sync_failed", warningSecret];
  Object.seal(sealedError);
  let releaseAttempted = false;
  try {
    let refreshError;
    try {
      await refreshManagedAuthRecord({
        authHome,
        acquireLock: async () => ({
          assertHeld: async () => {},
          release: async () => {
            releaseAttempted = true;
            throw new Error("synthetic lock release failure");
          },
        }),
        runRefresh: async () => {
          throw sealedError;
        },
      });
    } catch (error) {
      refreshError = error;
    }

    assert(refreshError instanceof ManagedAuthRefreshError);
    assert.notEqual(refreshError, sealedError);
    assert.equal(refreshError.code, sealedError.code);
    assert.equal(refreshError.message, sealedError.message);
    assert.equal(refreshError.retryable, false);
    assert.equal(refreshError.recoveryReason, "sealed_adapter_uncertain");
    assert.equal(refreshError.cause, sealedError);
    assert.equal(Object.prototype.propertyIsEnumerable.call(refreshError, "cause"), false);
    assert.deepEqual(refreshError.cleanupWarnings, [
      "parent_directory_sync_failed",
      "lock_release_failed",
    ]);
    assert.equal(releaseAttempted, true);
    assert.equal(JSON.stringify(refreshError).includes(warningSecret), false);
    assert.equal((await stat(join(refreshError.recoveryPath, "auth.json"))).isFile(), true);

    await assert.rejects(
      refreshManagedAuthRecord({ authHome }),
      (error) => error.code === "recovery_required",
    );
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("startup failure plus stop failure becomes sanitized non-retryable uncertainty", async () => {
  for (const phase of ["start", "initialize"]) {
    const operationSecret = `${phase}-credential-secret`;
    const stopSecret = `${phase}-stop-credential-secret`;
    let refreshError;
    try {
      await runCodexManagedRefresh({
        codexBin: "/pinned/codex",
        stagingHome: `/isolated/${phase}-staging-home`,
        createClient: () => ({
          start: async () => {
            if (phase === "start") throw new Error(operationSecret);
          },
          initialize: async () => {
            if (phase === "initialize") throw new Error(operationSecret);
            return {};
          },
          request: async () => assert.fail("account/read must not be dispatched"),
          stop: async () => {
            throw new Error(stopSecret);
          },
        }),
      });
    } catch (error) {
      refreshError = error;
    }
    assert(refreshError instanceof ManagedAuthRefreshError);
    assert.equal(refreshError.code, "refresh_outcome_uncertain");
    assert.equal(refreshError.retryable, false);
    assert.equal(refreshError.recoveryReason, "app_server_shutdown_unknown");
    assert.deepEqual(refreshError.cleanupWarnings, ["app_server_stop_failed"]);
    assert.equal(refreshError.cause.message, operationSecret);
    const serialized = JSON.stringify(managedAuthRefreshFailureReport(refreshError));
    assert.equal(serialized.includes(operationSecret), false);
    assert.equal(serialized.includes(stopSecret), false);
  }
});

test("initialize plus stop failure retains staging and blocks the next rotation", async () => {
  const { authHome, root } = await createAuthorityHome();
  const operationSecret = "initialize-login-secret";
  const stopSecret = "stop-login-secret";
  try {
    const runRefresh = (options) =>
      runCodexManagedRefresh({
        ...options,
        createClient: () => ({
          start: async () => {},
          initialize: async () => {
            throw new Error(operationSecret);
          },
          request: async () => assert.fail("account/read must not be dispatched"),
          stop: async () => {
            throw new Error(stopSecret);
          },
        }),
      });
    let refreshError;
    try {
      await refreshManagedAuthRecord({
        authHome,
        codexBin: "/pinned/codex",
        runRefresh,
      });
    } catch (error) {
      refreshError = error;
    }
    assert(refreshError instanceof ManagedAuthRefreshError);
    assert.equal(refreshError.code, "refresh_outcome_uncertain");
    assert.equal(refreshError.retryable, false);
    assert.equal(refreshError.recoveryReason, "app_server_shutdown_unknown");
    assert.equal((await stat(join(refreshError.recoveryPath, "auth.json"))).isFile(), true);
    const serialized = JSON.stringify(managedAuthRefreshFailureReport(refreshError));
    assert.equal(serialized.includes(operationSecret), false);
    assert.equal(serialized.includes(stopSecret), false);

    let nextRefreshCalled = false;
    await assert.rejects(
      refreshManagedAuthRecord({
        authHome,
        runRefresh: async () => {
          nextRefreshCalled = true;
          return successResponse();
        },
      }),
      (error) => error.code === "recovery_required",
    );
    assert.equal(nextRefreshCalled, false);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("post-dispatch RPC failure preserves an unchanged staging sentinel", async () => {
  const { authHome, root } = await createAuthorityHome();
  try {
    const runRefresh = (options) =>
      runCodexManagedRefresh({
        ...options,
        createClient: () => ({
          initialize: async () => ({}),
          request: async () => {
            throw new Error("synthetic post-dispatch failure");
          },
          rpcMethodAudit: () => RPC_AUDIT,
          start: async () => {},
          stop: async () => {
            throw new Error("synthetic stop failure after request failure");
          },
        }),
      });
    let refreshError;
    try {
      await refreshManagedAuthRecord({
        authHome,
        codexBin: "/pinned/codex",
        runRefresh,
      });
    } catch (error) {
      refreshError = error;
    }
    assert.equal(refreshError.code, "refresh_outcome_uncertain");
    assert.equal(refreshError.retryable, false);
    assert.equal(refreshError.recoveryReason, "app_server_shutdown_unknown");
    assert.deepEqual(refreshError.cleanupWarnings, ["app_server_stop_failed"]);
    assert.equal((await stat(join(refreshError.recoveryPath, "auth.json"))).isFile(), true);
    let secondRefreshCalled = false;
    await assert.rejects(
      refreshManagedAuthRecord({
        authHome,
        runRefresh: async () => {
          secondRefreshCalled = true;
          return successResponse();
        },
      }),
      (error) => error.code === "recovery_required",
    );
    assert.equal(secondRefreshCalled, false);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("runCodexManagedRefresh aborts an in-flight request and stops the app-server", async () => {
  const controller = new AbortController();
  let requestStarted;
  const started = new Promise((resolve) => {
    requestStarted = resolve;
  });
  let finishAbort;
  const abortFinished = new Promise((resolve) => {
    finishAbort = resolve;
  });
  let aborted = 0;
  let stopped = 0;
  let settled = false;
  const refresh = runCodexManagedRefresh({
    codexBin: "/pinned/codex",
    signal: controller.signal,
    stagingHome: "/isolated/staging-home",
    createClient: () => ({
      initialize: async () => ({}),
      request: async () => {
        requestStarted();
        return new Promise(() => {});
      },
      start: async () => {},
      abort: async () => {
        aborted += 1;
        await abortFinished;
      },
      stop: async () => {
        stopped += 1;
      },
    }),
  });
  void refresh.finally(() => {
    settled = true;
  }).catch(() => {});
  await started;
  const lost = new AdvisoryLockError("lock_lost", "synthetic holder exit");
  controller.abort(lost);
  await new Promise((resolve) => setImmediate(resolve));
  assert.equal(settled, false);
  finishAbort();
  await assert.rejects(refresh, (error) => error === lost);
  assert.equal(aborted, 1);
  assert.equal(stopped, 1);
});

test("managed refresh rejects PATH-resolved Codex binaries before startup", async () => {
  let clientCreated = false;
  await assert.rejects(
    runCodexManagedRefresh({
      codexBin: "codex",
      stagingHome: "/isolated/staging-home",
      createClient: () => {
        clientCreated = true;
        return {};
      },
    }),
    (error) => error.code === "unsafe_codex_binary",
  );
  assert.equal(clientCreated, false);
});

test("client construction failures are confirmed pre-dispatch and leave no recovery gate", async () => {
  const { authHome, root } = await createAuthorityHome();
  const constructorError = new Error("synthetic client construction failure");
  let dispatchGuardCalled = false;
  try {
    await assert.rejects(
      refreshManagedAuthRecord({
        authHome,
        codexBin: "/pinned/codex",
        runRefresh: (options) =>
          runCodexManagedRefresh({
            ...options,
            assertLockHeldBeforeDispatch: async () => {
              dispatchGuardCalled = true;
            },
            createClient: () => {
              throw constructorError;
            },
          }),
      }),
      (error) => error === constructorError,
    );
    assert.equal(dispatchGuardCalled, false);
    await assert.rejects(stat(authorityStagingDirectory(authHome)), /ENOENT/);

    const result = await refreshManagedAuthRecord({
      authHome,
      runRefresh: async ({ stagingHome }) => {
        await replaceStagedAuth(stagingHome);
        return successResponse();
      },
    });
    assert.equal(result.comparisons.refreshTokenChanged, true);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("primitive client construction failures are wrapped without serialization leakage", async () => {
  const thrownValue = "primitive-client-construction-sensitive";
  await assert.rejects(
    runCodexManagedRefresh({
      codexBin: "/pinned/codex",
      stagingHome: "/isolated/staging-home",
      createClient: () => {
        throw thrownValue;
      },
    }),
    (error) => {
      assert.equal(error.code, "app_server_client_creation_failed");
      assert.equal(error.retryable, false);
      assert.equal(error.cause, thrownValue);
      assert.equal(Object.prototype.propertyIsEnumerable.call(error, "cause"), false);
      assert.equal(JSON.stringify(error).includes(thrownValue), false);
      return true;
    },
  );
});

test("unresolved refresh artifacts block token rotation and expose safe recovery paths", async () => {
  const { authHome, root } = await createAuthorityHome();
  const promotionCandidate = join(authHome, ".auth.json.next-recovery");
  const attempt = join(authorityStagingDirectory(authHome), "attempt-recovery");
  const rotated = `${JSON.stringify(
    authDocument({
      accessMarker: "orphaned",
      lastRefresh: "2026-07-01T08:01:00.000Z",
      refreshToken: "refresh-orphaned-sensitive",
    }),
  )}\n`;
  let refreshCalled = false;
  try {
    await writeFile(promotionCandidate, rotated, { mode: 0o600 });
    await mkdir(attempt, { recursive: true, mode: 0o700 });
    await writeFile(join(attempt, "auth.json"), rotated, { mode: 0o600 });
    const canonicalBefore = await readFile(join(authHome, "auth.json"), "utf8");
    let refreshError;
    try {
      await refreshManagedAuthRecord({
        authHome,
        runRefresh: async () => {
          refreshCalled = true;
          return successResponse();
        },
      });
    } catch (error) {
      refreshError = error;
    }
    assert.equal(refreshError.code, "recovery_required");
    assert.equal(refreshError.retryable, false);
    assert.equal(refreshError.recoveryReason, "orphaned_refresh_artifacts");
    const canonicalHome = await realpath(authHome);
    assert.deepEqual(
      refreshError.recoveryPaths,
      [
        join(canonicalHome, ".auth.json.next-recovery"),
        join(canonicalHome, ".portable-auth-refresh-staging", "attempt-recovery"),
      ].sort(),
    );
    assert.equal(refreshCalled, false);
    assert.equal(await readFile(join(authHome, "auth.json"), "utf8"), canonicalBefore);
    assert.equal((await stat(promotionCandidate)).isFile(), true);
    assert.equal((await stat(join(attempt, "auth.json"))).isFile(), true);
    const serialized = JSON.stringify(managedAuthRefreshErrorMetadata(refreshError));
    assert.equal(serialized.includes("refresh-orphaned-sensitive"), false);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("non-object JWT payloads are classified as invalid auth records", async () => {
  const { authHome, root } = await createAuthorityHome();
  try {
    const document = JSON.parse(await readFile(join(authHome, "auth.json"), "utf8"));
    document.tokens.access_token = encodeJwt(null);
    await writeFile(join(authHome, "auth.json"), `${JSON.stringify(document)}\n`, { mode: 0o600 });
    await assert.rejects(
      readManagedAuthSnapshot(authHome),
      (error) => error.code === "invalid_auth_record",
    );
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("access-token expirations outside the ECMAScript Date range are invalid auth records", async () => {
  const { authHome, root } = await createAuthorityHome();
  try {
    const document = authDocument({
      accessMarker: "invalid-expiration",
      expiresAtUnixSeconds: 1e20,
      lastRefresh: "2026-07-01T08:00:00.000Z",
      refreshToken: "refresh-invalid-expiration-sensitive",
    });
    await writeFile(join(authHome, "auth.json"), `${JSON.stringify(document)}\n`, {
      mode: 0o600,
    });

    await assert.rejects(readManagedAuthSnapshot(authHome), (error) => {
      assert(error instanceof ManagedAuthRefreshError);
      assert.equal(error.code, "invalid_auth_record");
      assert.doesNotMatch(error.message, /1e20|refresh-invalid/);
      return true;
    });
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("non-object auth documents are classified as invalid auth records", async () => {
  for (const document of [null, "scalar", []]) {
    const { authHome, root } = await createAuthorityHome();
    try {
      await writeFile(join(authHome, "auth.json"), `${JSON.stringify(document)}\n`, {
        mode: 0o600,
      });
      await assert.rejects(readManagedAuthSnapshot(authHome), (error) => {
        assert(error instanceof ManagedAuthRefreshError);
        assert.equal(error.code, "invalid_auth_record");
        return true;
      });
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  }
});

test("invalid refresh responses and unchanged access tokens fail closed", async () => {
  const invalid = await createAuthorityHome();
  try {
    await assert.rejects(
      refreshManagedAuthRecord({
        authHome: invalid.authHome,
        runRefresh: async ({ stagingHome }) => {
          await replaceStagedAuth(stagingHome);
          return { ...successResponse(), response: null };
        },
      }),
      (error) => error.code === "invalid_refresh_response" && error.retryable === false,
    );
  } finally {
    await rm(invalid.root, { recursive: true, force: true });
  }

  const unchanged = await createAuthorityHome();
  try {
    await assert.rejects(
      refreshManagedAuthRecord({
        authHome: unchanged.authHome,
        runRefresh: async ({ stagingHome }) => {
          await replaceStagedAuth(stagingHome, { accessMarker: "before" });
          return successResponse();
        },
      }),
      (error) => error.code === "access_token_unchanged" && error.retryable === false,
    );
  } finally {
    await rm(unchanged.root, { recursive: true, force: true });
  }
});

test("unchanged refresh tokens are never promoted after an observed access refresh", async () => {
  const { authHome, root } = await createAuthorityHome();
  const canonicalBefore = await readFile(join(authHome, "auth.json"), "utf8");
  try {
    let refreshError;
    try {
      await refreshManagedAuthRecord({
        authHome,
        runRefresh: async ({ stagingHome }) => {
          await replaceStagedAuth(stagingHome, {
            refreshToken: "refresh-before-sensitive",
          });
          return successResponse();
        },
      });
    } catch (error) {
      refreshError = error;
    }

    assert.equal(refreshError.code, "refresh_token_unchanged");
    assert.equal(refreshError.retryable, false);
    assert.equal(refreshError.recoveryReason, "refresh_token_not_rotated");
    assert.equal(await readFile(join(authHome, "auth.json"), "utf8"), canonicalBefore);
    assert.equal((await stat(join(refreshError.recoveryPath, "auth.json"))).isFile(), true);
    await assert.rejects(
      refreshManagedAuthRecord({ authHome }),
      (error) => error.code === "recovery_required",
    );
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("initial durable staging close failures prevent refresh dispatch", async () => {
  const { authHome, root } = await createAuthorityHome();
  const closeError = new Error("synthetic initial staging close failure");
  let closeInjected = false;
  let refreshCalled = false;
  try {
    await assert.rejects(
      refreshManagedAuthRecord({
        authHome,
        openFile: async (path, flags, mode) => {
          const handle = await open(path, flags, mode);
          if (!closeInjected && path.endsWith("auth.json") && flags === "wx") {
            closeInjected = true;
            return failHandleCloseAfterClosing(handle, closeError);
          }
          return handle;
        },
        runRefresh: async () => {
          refreshCalled = true;
          return successResponse();
        },
      }),
      (error) => error === closeError,
    );
    assert.equal(closeInjected, true);
    assert.equal(refreshCalled, false);
    await assert.rejects(stat(authorityStagingDirectory(authHome)), /ENOENT/);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("initial durable staging preserves write errors when close also fails", async () => {
  const { authHome, root } = await createAuthorityHome();
  const writeError = new Error("synthetic initial staging write failure");
  const closeError = new Error("synthetic close failure after write failure");
  let failureInjected = false;
  let refreshCalled = false;
  try {
    await assert.rejects(
      refreshManagedAuthRecord({
        authHome,
        openFile: async (path, flags, mode) => {
          const handle = await open(path, flags, mode);
          if (!failureInjected && path.endsWith("auth.json") && flags === "wx") {
            failureInjected = true;
            handle.writeFile = async () => {
              throw writeError;
            };
            return failHandleCloseAfterClosing(handle, closeError);
          }
          return handle;
        },
        runRefresh: async () => {
          refreshCalled = true;
          return successResponse();
        },
      }),
      (error) => {
        assert.equal(error, writeError);
        assert.equal(error.closeError, closeError);
        assert.equal(Object.prototype.propertyIsEnumerable.call(error, "closeError"), false);
        return true;
      },
    );
    assert.equal(failureInjected, true);
    assert.equal(refreshCalled, false);
    await assert.rejects(stat(authorityStagingDirectory(authHome)), /ENOENT/);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("staging durability is established before token-mutating refresh begins", async () => {
  const { authHome, root } = await createAuthorityHome();
  const events = [];
  try {
    await refreshManagedAuthRecord({
      authHome,
      syncStagingDirectory: async (path) => {
        events.push(["sync-directory", path]);
        return true;
      },
      writeStagingFile: async (path, contents, options) => {
        events.push(["write-file", path]);
        await writeFile(path, contents, options);
      },
      runRefresh: async ({ stagingHome }) => {
        events.push(["run-refresh", stagingHome]);
        await replaceStagedAuth(stagingHome);
        return successResponse();
      },
    });
    const runIndex = events.findIndex(([event]) => event === "run-refresh");
    const prior = events.slice(0, runIndex).map(([event]) => event);
    assert.deepEqual(prior, [
      "sync-directory",
      "write-file",
      "write-file",
      "sync-directory",
    ]);
    assert.deepEqual(events.slice(runIndex + 1).map(([event]) => event), ["sync-directory"]);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("post-refresh staging sync failure preserves a non-retryable recovery sentinel", async () => {
  const { authHome, root } = await createAuthorityHome();
  let syncCount = 0;
  try {
    let refreshError;
    try {
      await refreshManagedAuthRecord({
        authHome,
        syncStagingDirectory: async () => {
          syncCount += 1;
          return syncCount < 3;
        },
        runRefresh: async ({ stagingHome }) => {
          await replaceStagedAuth(stagingHome);
          return successResponse();
        },
      });
    } catch (error) {
      refreshError = error;
    }
    assert.equal(refreshError.code, "staging_recovery_not_durable");
    assert.equal(refreshError.retryable, false);
    assert.equal(refreshError.recoveryReason, "staging_sync_failed");
    assert.equal((await stat(join(refreshError.recoveryPath, "auth.json"))).isFile(), true);
    await assert.rejects(
      refreshManagedAuthRecord({ authHome }),
      (error) => error.code === "recovery_required",
    );
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("post-refresh staging close failures retain a non-retryable recovery sentinel", async () => {
  const { authHome, root } = await createAuthorityHome();
  const closeError = new Error("synthetic refreshed staging close failure");
  let closeInjected = false;
  try {
    let refreshError;
    try {
      await refreshManagedAuthRecord({
        authHome,
        openFile: async (path, flags, mode) => {
          const handle = await open(path, flags, mode);
          if (!closeInjected && path.endsWith("auth.json") && typeof flags === "number") {
            closeInjected = true;
            return failHandleCloseAfterClosing(handle, closeError);
          }
          return handle;
        },
        runRefresh: async ({ stagingHome }) => {
          await replaceStagedAuth(stagingHome);
          return successResponse();
        },
      });
    } catch (error) {
      refreshError = error;
    }

    assert.equal(closeInjected, true);
    assert.equal(refreshError.code, "staging_recovery_not_durable");
    assert.equal(refreshError.retryable, false);
    assert.equal(refreshError.recoveryReason, "staging_sync_failed");
    assert.equal(refreshError.cause, closeError);
    assert.equal((await stat(join(refreshError.recoveryPath, "auth.json"))).isFile(), true);
    await assert.rejects(
      refreshManagedAuthRecord({ authHome }),
      (error) => error.code === "recovery_required",
    );
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("authority replacement during post-refresh sync takes precedence over recovery path", async () => {
  const { authHome, root } = await createAuthorityHome();
  const displaced = join(root, "displaced-during-staging-sync");
  let syncCount = 0;
  try {
    let refreshError;
    try {
      await refreshManagedAuthRecord({
        authHome,
        syncStagingDirectory: async () => {
          syncCount += 1;
          if (syncCount === 3) {
            await rename(authHome, displaced);
            await mkdir(authHome, { mode: 0o700 });
            return false;
          }
          return true;
        },
        runRefresh: async ({ stagingHome }) => {
          await replaceStagedAuth(stagingHome);
          return successResponse();
        },
      });
    } catch (error) {
      refreshError = error;
    }
    assert.equal(refreshError.code, "authority_home_replaced");
    assert.equal(refreshError.recoveryPath, undefined);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("pre-dispatch refresh failure is not replaced by post-refresh durability failure", async () => {
  const { authHome, root } = await createAuthorityHome();
  let syncCount = 0;
  try {
    await assert.rejects(
      refreshManagedAuthRecord({
        authHome,
        codexBin: "/pinned/codex",
        syncStagingDirectory: async () => {
          syncCount += 1;
          return syncCount < 3;
        },
        runRefresh: (options) =>
          runCodexManagedRefresh({
            ...options,
            createClient: () => ({
              start: async () => {
                throw new Error("synthetic pre-dispatch startup failure");
              },
              stop: async () => {},
            }),
          }),
      }),
      /synthetic pre-dispatch startup failure/,
    );
    assert.equal(syncCount, 2);

    syncCount = 0;
    await assert.rejects(
      refreshManagedAuthRecord({
        authHome,
        codexBin: "codex",
        syncStagingDirectory: async () => {
          syncCount += 1;
          return syncCount < 3;
        },
      }),
      (error) => error.code === "unsafe_codex_binary" && error.retryable === false,
    );
    assert.equal(syncCount, 2);
    assert.equal(
      (await readdir(authHome)).some((name) => name === ".portable-auth-refresh-staging"),
      false,
    );
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("changed pre-dispatch staging is synchronized before a recovery sentinel is returned", async () => {
  const { authHome, root } = await createAuthorityHome();
  const events = [];
  try {
    let refreshError;
    try {
      await refreshManagedAuthRecord({
        authHome,
        codexBin: "/pinned/codex",
        syncStagingDirectory: async (path) => {
          events.push(["sync-directory", path]);
          return true;
        },
        runRefresh: (options) =>
          runCodexManagedRefresh({
            ...options,
            createClient: ({ codexHome }) => ({
              start: async () => {},
              initialize: async () => {
                await replaceStagedAuth(codexHome);
                throw new Error("synthetic pre-dispatch initialize failure");
              },
              stop: async () => {},
            }),
          }),
      });
    } catch (error) {
      refreshError = error;
    }

    assert.equal(refreshError.code, "refresh_outcome_uncertain");
    assert.equal(refreshError.retryable, false);
    assert.equal(refreshError.recoveryReason, "pre_dispatch_staging_changed");
    assert.match(refreshError.cause.message, /pre-dispatch initialize failure/);
    assert.equal(events.length, 3);
    assert.equal(events[2][1], refreshError.recoveryPath);
    assert.equal((await stat(join(refreshError.recoveryPath, "auth.json"))).isFile(), true);
    await assert.rejects(
      refreshManagedAuthRecord({ authHome }),
      (error) => error.code === "recovery_required",
    );
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("changed pre-dispatch staging sync failures retain the original cause", async () => {
  const { authHome, root } = await createAuthorityHome();
  const syncError = new Error("synthetic staging directory sync failure");
  let syncCount = 0;
  try {
    let refreshError;
    try {
      await refreshManagedAuthRecord({
        authHome,
        codexBin: "/pinned/codex",
        syncStagingDirectory: async () => {
          syncCount += 1;
          if (syncCount === 3) throw syncError;
          return true;
        },
        runRefresh: (options) =>
          runCodexManagedRefresh({
            ...options,
            createClient: ({ codexHome }) => ({
              start: async () => {
                await replaceStagedAuth(codexHome);
                throw new Error("synthetic pre-dispatch startup failure after staging change");
              },
              stop: async () => {},
            }),
          }),
      });
    } catch (error) {
      refreshError = error;
    }

    assert.equal(syncCount, 3);
    assert.equal(refreshError.code, "staging_recovery_not_durable");
    assert.equal(refreshError.retryable, false);
    assert.equal(refreshError.recoveryReason, "staging_sync_failed");
    assert.match(refreshError.cause.message, /startup failure after staging change/);
    assert.equal(refreshError.syncError.cause, syncError);
    assert.equal(Object.prototype.propertyIsEnumerable.call(refreshError, "syncError"), false);
    assert.equal((await stat(join(refreshError.recoveryPath, "auth.json"))).isFile(), true);
    await assert.rejects(
      refreshManagedAuthRecord({ authHome }),
      (error) => error.code === "recovery_required",
    );
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("authority replacement wins during changed pre-dispatch staging sync", async () => {
  const { authHome, root } = await createAuthorityHome();
  const displaced = join(root, "displaced-during-pre-dispatch-staging-sync");
  let stagingHome;
  let syncCount = 0;
  try {
    let refreshError;
    try {
      await refreshManagedAuthRecord({
        authHome,
        codexBin: "/pinned/codex",
        syncStagingDirectory: async () => {
          syncCount += 1;
          if (syncCount === 3) {
            await rename(authHome, displaced);
            await mkdir(authHome, { mode: 0o700 });
            return false;
          }
          return true;
        },
        runRefresh: (options) =>
          runCodexManagedRefresh({
            ...options,
            createClient: ({ codexHome }) => ({
              start: async () => {
                stagingHome = codexHome;
                await replaceStagedAuth(codexHome);
                throw new Error("synthetic pre-dispatch authority replacement failure");
              },
              stop: async () => {},
            }),
          }),
      });
    } catch (error) {
      refreshError = error;
    }

    assert.equal(syncCount, 3);
    assert.equal(refreshError.code, "authority_home_replaced");
    assert.equal(refreshError.recoveryPath, undefined);
    assert.equal(refreshError.recoveryPaths, undefined);
    const serialized = JSON.stringify(managedAuthRefreshErrorMetadata(refreshError));
    assert.equal(serialized.includes(displaced), false);
    assert.equal(serialized.includes(stagingHome), false);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("unparseable pre-dispatch staging is synchronized and retained for recovery", async () => {
  const { authHome, root } = await createAuthorityHome();
  let syncCount = 0;
  try {
    let refreshError;
    try {
      await refreshManagedAuthRecord({
        authHome,
        codexBin: "/pinned/codex",
        syncStagingDirectory: async () => {
          syncCount += 1;
          return true;
        },
        runRefresh: (options) =>
          runCodexManagedRefresh({
            ...options,
            createClient: ({ codexHome }) => ({
              start: async () => {
                await writeFile(join(codexHome, "auth.json"), "{invalid-json\n");
                throw new Error("synthetic pre-dispatch malformed staging failure");
              },
              stop: async () => {},
            }),
          }),
      });
    } catch (error) {
      refreshError = error;
    }

    assert.equal(syncCount, 3);
    assert.equal(refreshError.code, "refresh_outcome_uncertain");
    assert.equal(refreshError.recoveryReason, "pre_dispatch_staging_changed");
    assert.match(refreshError.cause.message, /malformed staging failure/);
    assert.equal(
      await readFile(join(refreshError.recoveryPath, "auth.json"), "utf8"),
      "{invalid-json\n",
    );
    await assert.rejects(
      refreshManagedAuthRecord({ authHome }),
      (error) => error.code === "recovery_required",
    );
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("authority replacement takes precedence over a marked pre-dispatch failure", async () => {
  const { authHome, root } = await createAuthorityHome();
  const displaced = join(root, "displaced-during-startup");
  try {
    let refreshError;
    try {
      await refreshManagedAuthRecord({
        authHome,
        codexBin: "/pinned/codex",
        runRefresh: (options) =>
          runCodexManagedRefresh({
            ...options,
            createClient: () => ({
              start: async () => {
                await rename(authHome, displaced);
                await mkdir(authHome, { mode: 0o700 });
                throw new Error("synthetic pre-dispatch startup failure");
              },
              stop: async () => {},
            }),
          }),
      });
    } catch (error) {
      refreshError = error;
    }
    assert.equal(refreshError.code, "authority_home_replaced");
    assert.equal(refreshError.recoveryPath, undefined);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("unsupported staging directory sync fails before refresh-token rotation", async () => {
  const { authHome, root } = await createAuthorityHome();
  let refreshCalled = false;
  try {
    await assert.rejects(
      refreshManagedAuthRecord({
        authHome,
        syncStagingDirectory: async () => false,
        runRefresh: async () => {
          refreshCalled = true;
          return successResponse();
        },
      }),
      (error) => error.code === "staging_not_durable",
    );
    assert.equal(refreshCalled, false);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("lost lock gates token-mutating refresh before the RPC starts", async () => {
  const { authHome, root } = await createAuthorityHome();
  let refreshCalled = false;
  let released = false;
  try {
    await assert.rejects(
      refreshManagedAuthRecord({
        authHome,
        acquireLock: async () => ({
          assertHeld: async () => {
            throw new AdvisoryLockError("lock_lost", "synthetic holder exit");
          },
          release: async () => {
            released = true;
          },
        }),
        runRefresh: async () => {
          refreshCalled = true;
          return successResponse();
        },
      }),
      (error) => error.code === "lock_lost_before_refresh" && error.retryable === true,
    );
    assert.equal(refreshCalled, false);
    assert.equal(released, true);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("holder exit aborts an in-flight token refresh before cleanup", async () => {
  const { authHome, root } = await createAuthorityHome();
  let reportLoss;
  const loss = new Promise((resolve) => {
    reportLoss = resolve;
  });
  let reportStarted;
  const started = new Promise((resolve) => {
    reportStarted = resolve;
  });
  try {
    const refresh = refreshManagedAuthRecord({
      authHome,
      acquireLock: async () => ({
        assertHeld: async () => {},
        release: async () => {},
        waitForLoss: () => loss,
      }),
      runRefresh: async ({ signal }) => {
        reportStarted();
        return new Promise((resolve, reject) => {
          signal.addEventListener("abort", () => reject(signal.reason), { once: true });
        });
      },
    });
    await started;
    const lost = new AdvisoryLockError("lock_lost", "synthetic holder exit");
    reportLoss(lost);
    let refreshError;
    try {
      await refresh;
    } catch (error) {
      refreshError = error;
    }
    assert.equal(refreshError.code, "refresh_outcome_uncertain");
    assert.equal(refreshError.retryable, false);
    assert.equal(refreshError.recoveryReason, "lock_lost_during_refresh");
    assert.equal((await stat(join(refreshError.recoveryPath, "auth.json"))).isFile(), true);
    let secondRefreshCalled = false;
    await assert.rejects(
      refreshManagedAuthRecord({
        authHome,
        runRefresh: async () => {
          secondRefreshCalled = true;
          return successResponse();
        },
      }),
      (error) => error.code === "recovery_required",
    );
    assert.equal(secondRefreshCalled, false);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("lock loss retains abort and shutdown failures as sanitized cleanup evidence", async () => {
  const { authHome, root } = await createAuthorityHome();
  const abortSecret = "synthetic-abort-refresh-token";
  const stopSecret = "synthetic-stop-refresh-token";
  let reportLoss;
  const loss = new Promise((resolve) => {
    reportLoss = resolve;
  });
  let reportStarted;
  const started = new Promise((resolve) => {
    reportStarted = resolve;
  });
  let syncCount = 0;
  try {
    const refresh = refreshManagedAuthRecord({
      authHome,
      acquireLock: async () => ({
        assertHeld: async () => {},
        release: async () => {},
        waitForLoss: () => loss,
      }),
      codexBin: "/pinned/codex",
      syncStagingDirectory: async () => {
        syncCount += 1;
        return true;
      },
      runRefresh: (options) =>
        runCodexManagedRefresh({
          ...options,
          createClient: () => ({
            abort: async () => {
              throw new JsonRpcError("account/read", {
                data: { refreshToken: abortSecret },
                message: `abort failed with ${abortSecret}`,
              });
            },
            initialize: async () => ({}),
            request: async () => {
              reportStarted();
              return new Promise(() => {});
            },
            start: async () => {},
            stop: async () => {
              throw new Error(`codex app-server process group survived SIGKILL: ${stopSecret}`);
            },
          }),
        }),
    });
    await started;
    const lost = new AdvisoryLockError("lock_lost", "synthetic holder exit");
    reportLoss(lost);

    let refreshError;
    try {
      await refresh;
    } catch (error) {
      refreshError = error;
    }

    assert(refreshError instanceof ManagedAuthRefreshError);
    assert.equal(refreshError.code, "refresh_outcome_uncertain");
    assert.equal(refreshError.recoveryReason, "lock_lost_during_refresh");
    assert.equal(refreshError.cause, lost);
    assert.equal(Object.prototype.propertyIsEnumerable.call(refreshError, "cause"), false);
    assert(refreshError.cleanupError instanceof ManagedAuthRefreshError);
    assert(refreshError.cleanupError.cause instanceof JsonRpcError);
    assert.equal(refreshError.cleanupError.cause.payload.data.refreshToken, abortSecret);
    assert.equal(Object.prototype.propertyIsEnumerable.call(refreshError, "cleanupError"), false);
    assert.match(refreshError.cleanupError.cleanupError.message, /survived SIGKILL/);
    assert.equal(
      Object.prototype.propertyIsEnumerable.call(refreshError.cleanupError, "cleanupError"),
      false,
    );
    assert.deepEqual(refreshError.cleanupWarnings, [
      "refresh_abort_cleanup_failed",
      "app_server_stop_failed",
    ]);
    assert.equal(syncCount, 2, "unquiesced staging must not receive a post-refresh sync");
    assert.equal(JSON.stringify(refreshError).includes(abortSecret), false);
    assert.equal(JSON.stringify(refreshError).includes(stopSecret), false);
    assert.equal((await stat(join(refreshError.recoveryPath, "auth.json"))).isFile(), true);

    let secondRefreshCalled = false;
    await assert.rejects(
      refreshManagedAuthRecord({
        authHome,
        runRefresh: async () => {
          secondRefreshCalled = true;
          return successResponse();
        },
      }),
      (error) => error.code === "recovery_required",
    );
    assert.equal(secondRefreshCalled, false);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("lock loss preserves stop failure metadata when abort rejects with the lock error", async () => {
  const { authHome, root } = await createAuthorityHome();
  const stopSecret = "synthetic-same-error-stop-refresh-token";
  let reportLoss;
  const loss = new Promise((resolve) => {
    reportLoss = resolve;
  });
  let reportStarted;
  const started = new Promise((resolve) => {
    reportStarted = resolve;
  });
  let syncCount = 0;
  try {
    const refresh = refreshManagedAuthRecord({
      authHome,
      acquireLock: async () => ({
        assertHeld: async () => {},
        release: async () => {},
        waitForLoss: () => loss,
      }),
      codexBin: "/pinned/codex",
      syncStagingDirectory: async () => {
        syncCount += 1;
        return true;
      },
      runRefresh: (options) =>
        runCodexManagedRefresh({
          ...options,
          createClient: () => ({
            abort: async () => {},
            initialize: async () => ({}),
            request: async () => {
              reportStarted();
              return new Promise(() => {});
            },
            start: async () => {},
            stop: async () => {
              throw new Error(`codex app-server process group survived SIGKILL: ${stopSecret}`);
            },
          }),
        }),
    });
    await started;
    const lost = new AdvisoryLockError("lock_lost", "synthetic holder exit");
    Object.defineProperty(lost, "cause", {
      configurable: true,
      enumerable: false,
      value: lost,
    });
    reportLoss(lost);

    let refreshError;
    try {
      await refresh;
    } catch (error) {
      refreshError = error;
    }

    assert(refreshError instanceof ManagedAuthRefreshError);
    assert.equal(refreshError.code, "refresh_outcome_uncertain");
    assert.equal(refreshError.recoveryReason, "lock_lost_during_refresh");
    assert.equal(refreshError.cause, lost);
    assert.deepEqual(refreshError.cleanupWarnings, [
      "refresh_abort_cleanup_failed",
      "app_server_stop_failed",
    ]);
    assert(refreshError.cleanupError instanceof ManagedAuthRefreshError);
    assert.equal(refreshError.cleanupError.cause, lost);
    assert.deepEqual(refreshError.cleanupError.cleanupWarnings, ["app_server_stop_failed"]);
    assert.match(refreshError.cleanupError.cleanupError.message, /survived SIGKILL/);
    assert.equal(
      Object.prototype.propertyIsEnumerable.call(refreshError.cleanupError, "cleanupError"),
      false,
    );
    assert.equal(syncCount, 2, "nested stop failure must suppress post-refresh sync");
    assert.equal(JSON.stringify(refreshError).includes(stopSecret), false);
    assert.equal((await stat(join(refreshError.recoveryPath, "auth.json"))).isFile(), true);

    let secondRefreshCalled = false;
    await assert.rejects(
      refreshManagedAuthRecord({
        authHome,
        runRefresh: async () => {
          secondRefreshCalled = true;
          return successResponse();
        },
      }),
      (error) => error.code === "recovery_required",
    );
    assert.equal(secondRefreshCalled, false);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("permanent account loss becomes reauth_required without changing canonical auth", async () => {
  const { authHome, root } = await createAuthorityHome();
  try {
    const before = await readFile(join(authHome, "auth.json"), "utf8");
    let refreshError;
    try {
      await refreshManagedAuthRecord({
        authHome,
        runRefresh: async () => ({
          ...successResponse(),
          response: { account: null, requiresOpenaiAuth: true },
        }),
      });
    } catch (error) {
      refreshError = error;
    }
    assert(refreshError instanceof ManagedAuthRefreshError);
    assert.equal(refreshError.code, "reauth_required");
    assert.equal(refreshError.retryable, false);
    assert.equal(refreshError.recoveryReason, "post_dispatch_outcome_uncertain");
    assert.equal((await stat(join(refreshError.recoveryPath, "auth.json"))).isFile(), true);
    assert.equal(await readFile(join(authHome, "auth.json"), "utf8"), before);
    let nextRefreshCalled = false;
    await assert.rejects(
      refreshManagedAuthRecord({
        authHome,
        runRefresh: async () => {
          nextRefreshCalled = true;
          return successResponse();
        },
      }),
      (error) => error.code === "recovery_required",
    );
    assert.equal(nextRefreshCalled, false);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("unobserved post-dispatch refresh fails closed and retains a sentinel", async () => {
  const { authHome, root } = await createAuthorityHome();
  try {
    const before = await readFile(join(authHome, "auth.json"), "utf8");
    let refreshError;
    try {
      await refreshManagedAuthRecord({ authHome, runRefresh: async () => successResponse() });
    } catch (error) {
      refreshError = error;
    }
    assert(refreshError instanceof ManagedAuthRefreshError);
    assert.equal(refreshError.code, "refresh_not_observed");
    assert.equal(refreshError.retryable, false);
    assert.equal(refreshError.recoveryReason, "post_dispatch_validation_failed");
    assert.equal((await stat(join(refreshError.recoveryPath, "auth.json"))).isFile(), true);
    assert.equal(await readFile(join(authHome, "auth.json"), "utf8"), before);
    await assert.rejects(
      refreshManagedAuthRecord({ authHome }),
      (error) => error.code === "recovery_required",
    );
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("canonical compare-and-swap conflict preserves the staged recovery record", async () => {
  const { authHome, root } = await createAuthorityHome();
  try {
    let refreshError;
    try {
      await refreshManagedAuthRecord({
        authHome,
        runRefresh: async ({ stagingHome }) => {
          await replaceStagedAuth(stagingHome);
          await writeFile(
            join(authHome, "auth.json"),
            `${JSON.stringify(
              authDocument({
                accessMarker: "concurrent",
                lastRefresh: "2026-07-01T08:02:00.000Z",
                refreshToken: "refresh-concurrent-sensitive",
              }),
            )}\n`,
            { mode: 0o600 },
          );
          return successResponse();
        },
      });
    } catch (error) {
      refreshError = error;
    }
    assert(refreshError instanceof ManagedAuthRefreshError);
    assert.equal(refreshError.code, "authority_conflict_after_refresh");
    assert.equal(typeof refreshError.recoveryPath, "string");
    assert.equal((await stat(join(refreshError.recoveryPath, "auth.json"))).isFile(), true);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("authority replacement wins when the canonical CAS reread fails", async () => {
  const { authHome, root } = await createAuthorityHome();
  const displaced = join(root, "displaced-during-cas-read");
  try {
    let refreshError;
    try {
      await refreshManagedAuthRecord({
        authHome,
        readCanonicalSource: async (path) => {
          await rename(authHome, displaced);
          await mkdir(authHome, { mode: 0o700 });
          return readManagedAuthSnapshot(path);
        },
        runRefresh: async ({ stagingHome }) => {
          await replaceStagedAuth(stagingHome);
          return successResponse();
        },
      });
    } catch (error) {
      refreshError = error;
    }
    assert.equal(refreshError.code, "authority_home_replaced");
    assert.equal(refreshError.recoveryPath, undefined);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("account identity changes fail without exposing either identity", async () => {
  const { authHome, root } = await createAuthorityHome();
  const changedAccount = "123e4567-e89b-42d3-a456-426614174099";
  try {
    await assert.rejects(
      refreshManagedAuthRecord({
        authHome,
        runRefresh: async ({ stagingHome }) => {
          await replaceStagedAuth(stagingHome, { accountId: changedAccount });
          return successResponse();
        },
      }),
      (error) => {
        assert.equal(error.code, "account_identity_changed");
        assert.equal(error.stack.includes(ACCOUNT_ID), false);
        assert.equal(error.stack.includes(changedAccount), false);
        return true;
      },
    );
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("user identity changes fail even inside the same workspace", async () => {
  const { authHome, root } = await createAuthorityHome();
  const changedUser = "user-123e4567-e89b-42d3-a456-426614174099";
  try {
    await assert.rejects(
      refreshManagedAuthRecord({
        authHome,
        runRefresh: async ({ stagingHome }) => {
          await replaceStagedAuth(stagingHome, { userId: changedUser });
          return successResponse();
        },
      }),
      (error) => {
        assert.equal(error.code, "user_identity_changed");
        assert.equal(error.stack.includes(USER_ID), false);
        assert.equal(error.stack.includes(changedUser), false);
        return true;
      },
    );
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("unexpected RPC activity is rejected before canonical promotion", async () => {
  const { authHome, root } = await createAuthorityHome();
  try {
    await assert.rejects(
      refreshManagedAuthRecord({
        authHome,
        runRefresh: async ({ stagingHome }) => {
          await replaceStagedAuth(stagingHome);
          return {
            ...successResponse(),
            rpcAudit: [...RPC_AUDIT, { kind: "request", method: "turn/start" }],
          };
        },
      }),
      (error) => error.code === "unexpected_rpc_sequence",
    );
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("unexpected post-dispatch RPC audit retains an unchanged staging sentinel", async () => {
  const { authHome, root } = await createAuthorityHome();
  try {
    let refreshError;
    try {
      await refreshManagedAuthRecord({
        authHome,
        runRefresh: async () => ({
          ...successResponse(),
          rpcAudit: [...RPC_AUDIT, { kind: "request", method: "turn/start" }],
        }),
      });
    } catch (error) {
      refreshError = error;
    }
    assert.equal(refreshError.code, "unexpected_rpc_sequence");
    assert.equal(refreshError.retryable, false);
    assert.equal(refreshError.recoveryReason, "post_dispatch_validation_failed");
    assert.equal((await stat(join(refreshError.recoveryPath, "auth.json"))).isFile(), true);
    await assert.rejects(
      refreshManagedAuthRecord({ authHome }),
      (error) => error.code === "recovery_required",
    );
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("adapter failure after rotation preserves the only staged recovery record", async () => {
  const { authHome, root } = await createAuthorityHome();
  try {
    let refreshError;
    try {
      await refreshManagedAuthRecord({
        authHome,
        runRefresh: async ({ stagingHome }) => {
          await replaceStagedAuth(stagingHome);
          throw new ManagedAuthRefreshError(
            "adapter_shutdown_failed",
            "adapter failed after the refresh request",
            { retryable: true },
          );
        },
      });
    } catch (error) {
      refreshError = error;
    }
    assert(refreshError instanceof ManagedAuthRefreshError);
    assert.equal(refreshError.code, "refresh_outcome_uncertain");
    assert.equal(refreshError.retryable, false);
    assert.equal(refreshError.recoveryReason, "unclassified_adapter_failure");
    assert.equal(refreshError.cause.code, "adapter_shutdown_failed");
    assert.equal(typeof refreshError.recoveryPath, "string");
    assert.equal((await stat(join(refreshError.recoveryPath, "auth.json"))).isFile(), true);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("unclassified adapter failure fails closed even when staged bytes are unchanged", async () => {
  const { authHome, root } = await createAuthorityHome();
  try {
    let refreshError;
    try {
      await refreshManagedAuthRecord({
        authHome,
        runRefresh: async () => {
          throw new Error("synthetic unclassified adapter failure");
        },
      });
    } catch (error) {
      refreshError = error;
    }
    assert(refreshError instanceof ManagedAuthRefreshError);
    assert.equal(refreshError.code, "refresh_outcome_uncertain");
    assert.equal(refreshError.retryable, false);
    assert.equal(refreshError.recoveryReason, "unclassified_adapter_failure");
    assert.match(refreshError.cause.message, /synthetic unclassified adapter failure/);
    assert.equal((await stat(join(refreshError.recoveryPath, "auth.json"))).isFile(), true);
    await assert.rejects(
      refreshManagedAuthRecord({ authHome }),
      (error) => error.code === "recovery_required",
    );
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("custom adapters cannot spoof internal pre-dispatch error codes", async () => {
  for (const code of ["unsafe_codex_binary", "lock_lost_before_refresh"]) {
    const { authHome, root } = await createAuthorityHome();
    try {
      let refreshError;
      try {
        await refreshManagedAuthRecord({
          authHome,
          runRefresh: async () => {
            throw new ManagedAuthRefreshError(code, "synthetic spoofed adapter failure", {
              retryable: true,
            });
          },
        });
      } catch (error) {
        refreshError = error;
      }
      assert.equal(refreshError.code, "refresh_outcome_uncertain");
      assert.equal(refreshError.retryable, false);
      assert.equal(refreshError.recoveryReason, "unclassified_adapter_failure");
      assert.equal(refreshError.cause.code, code);
      assert.equal((await stat(join(refreshError.recoveryPath, "auth.json"))).isFile(), true);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  }
});

test("expired refreshed access token is never promoted", async () => {
  const { authHome, root } = await createAuthorityHome();
  try {
    const canonicalBefore = await readFile(join(authHome, "auth.json"), "utf8");
    let refreshError;
    try {
      await refreshManagedAuthRecord({
        authHome,
        runRefresh: async ({ stagingHome }) => {
          await replaceStagedAuth(stagingHome, {
            expiresAtUnixSeconds: Math.floor(Date.now() / 1000) - 1,
          });
          return successResponse();
        },
      });
    } catch (error) {
      refreshError = error;
    }
    assert(refreshError instanceof ManagedAuthRefreshError);
    assert.equal(refreshError.code, "refreshed_access_token_invalid");
    assert.equal(await readFile(join(authHome, "auth.json"), "utf8"), canonicalBefore);
    assert.equal((await stat(join(refreshError.recoveryPath, "auth.json"))).isFile(), true);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("post-commit cleanup failure returns success with a warning", async () => {
  const { authHome, root } = await createAuthorityHome();
  try {
    const result = await refreshManagedAuthRecord({
      authHome,
      cleanupArtifacts: async () => {
        throw new Error("synthetic cleanup failure");
      },
      runRefresh: async ({ stagingHome }) => {
        await replaceStagedAuth(stagingHome);
        return successResponse();
      },
    });
    assert.deepEqual(result.cleanupWarnings, ["staging_cleanup_failed"]);
    assert.equal(typeof result.recoveryPath, "string");
    assert.equal((await stat(result.recoveryPath)).isDirectory(), true);
    const promoted = await readManagedAuthSnapshot(authHome);
    assert.equal(promoted.accessToken, result.accessToken);

    const lock = await acquireAdvisoryLock(authorityLockPath(authHome));
    await lock.release();
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("cleanup failure attaches a retained staging path to error metadata", async () => {
  const { authHome, root } = await createAuthorityHome();
  try {
    let refreshError;
    try {
      await refreshManagedAuthRecord({
        authHome,
        cleanupArtifacts: async () => {
          throw new Error("synthetic cleanup failure");
        },
        runRefresh: async () => successResponse(),
      });
    } catch (error) {
      refreshError = error;
    }
    assert.equal(refreshError.code, "refresh_not_observed");
    assert.deepEqual(refreshError.cleanupWarnings, ["staging_cleanup_failed"]);
    assert.equal(typeof refreshError.recoveryPath, "string");
    assert.equal((await stat(refreshError.recoveryPath)).isDirectory(), true);
    assert.equal(
      managedAuthRefreshErrorMetadata(refreshError).recoveryPath,
      refreshError.recoveryPath,
    );
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("post-commit verification failure blocks success and preserves recovery state", async () => {
  const { authHome, root } = await createAuthorityHome();
  try {
    let refreshError;
    try {
      await refreshManagedAuthRecord({
        authHome,
        runRefresh: async ({ stagingHome }) => {
          await replaceStagedAuth(stagingHome);
          return successResponse();
        },
        syncDirectory: async () => {
          throw new Error("synthetic directory sync failure");
        },
        verifyPromoted: async () => {
          throw new Error("synthetic post-commit verification failure");
        },
      });
    } catch (error) {
      refreshError = error;
    }
    assert.equal(refreshError.code, "promotion_verification_failed");
    assert.deepEqual(refreshError.cleanupWarnings, ["parent_directory_sync_failed"]);
    assert.equal(typeof refreshError.recoveryPath, "string");
    const promoted = await readManagedAuthSnapshot(authHome);
    assert.equal((await stat(refreshError.recoveryPath)).isDirectory(), true);
    const stagingRoot = await realpath(authorityStagingDirectory(authHome));
    assert.equal(refreshError.recoveryPath.startsWith(`${stagingRoot}/`), true);
    assert.notEqual(promoted.authFileFingerprint, undefined);

    const lock = await acquireAdvisoryLock(authorityLockPath(authHome));
    await lock.release();
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("authority replacement wins when the post-promotion reread fails", async () => {
  const { authHome, root } = await createAuthorityHome();
  const displaced = join(root, "displaced-during-promotion-verify");
  try {
    let refreshError;
    try {
      await refreshManagedAuthRecord({
        authHome,
        runRefresh: async ({ stagingHome }) => {
          await replaceStagedAuth(stagingHome);
          return successResponse();
        },
        verifyPromoted: async (path) => {
          await rename(authHome, displaced);
          await mkdir(authHome, { mode: 0o700 });
          return readManagedAuthSnapshot(path);
        },
      });
    } catch (error) {
      refreshError = error;
    }
    assert.equal(refreshError.code, "authority_home_replaced");
    assert.equal(refreshError.recoveryPath, undefined);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("final authority guard replaces an earlier error with a stale recovery path", async () => {
  const { authHome, root } = await createAuthorityHome();
  const displaced = join(root, "displaced-before-final-authority-check");
  let stagingHome;
  let replaced = false;
  try {
    let refreshError;
    try {
      await refreshManagedAuthRecord({
        authHome,
        runRefresh: async (options) => {
          stagingHome = options.stagingHome;
          await replaceStagedAuth(stagingHome);
          return successResponse();
        },
        verifyPromoted: async (path) => {
          const promoted = await readManagedAuthSnapshot(path);
          return new Proxy(promoted, {
            get(target, property, receiver) {
              if (property === "authFileFingerprint" && !replaced) {
                replaced = true;
                renameSync(authHome, displaced);
                mkdirSync(authHome, { mode: 0o700 });
                throw new ManagedAuthRefreshError(
                  "synthetic_post_verify_failure",
                  "synthetic failure after verified canonical reread",
                  { recoveryPath: stagingHome },
                );
              }
              return Reflect.get(target, property, receiver);
            },
          });
        },
      });
    } catch (error) {
      refreshError = error;
    }
    assert.equal(replaced, true);
    assert.equal(refreshError.code, "authority_home_replaced");
    assert.equal(refreshError.recoveryPath, undefined);
    assert.equal(refreshError.cause.code, "synthetic_post_verify_failure");
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("directory sync failure alone preserves the rotated staging credential", async () => {
  const { authHome, root } = await createAuthorityHome();
  try {
    const result = await refreshManagedAuthRecord({
      authHome,
      runRefresh: async ({ stagingHome }) => {
        await replaceStagedAuth(stagingHome);
        return successResponse();
      },
      syncDirectory: async () => false,
    });
    assert.deepEqual(result.cleanupWarnings, ["parent_directory_sync_failed"]);
    assert.equal(typeof result.recoveryPath, "string");
    assert.equal((await stat(join(result.recoveryPath, "auth.json"))).isFile(), true);
    await assert.rejects(
      refreshManagedAuthRecord({ authHome }),
      (error) => error.code === "recovery_required",
    );
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("uncertain holder commit fails closed and preserves every recovery copy", async () => {
  const { authHome, root } = await createAuthorityHome();
  try {
    const canonicalBefore = await readFile(join(authHome, "auth.json"), "utf8");
    let refreshError;
    try {
      await refreshManagedAuthRecord({
        authHome,
        acquireLock: async () => ({
          assertHeld: async () => {},
          release: async () => {},
          renameWhileHeld: async () => {
            throw new AdvisoryLockError(
              "lock_commit_uncertain",
              "synthetic lost commit acknowledgement",
            );
          },
        }),
        runRefresh: async ({ stagingHome }) => {
          await replaceStagedAuth(stagingHome);
          return successResponse();
        },
      });
    } catch (error) {
      refreshError = error;
    }
    assert.equal(refreshError.code, "promotion_commit_uncertain");
    assert.equal(refreshError.retryable, false);
    assert.equal(refreshError.recoveryReason, "holder_commit_ack_lost");
    const stagingRecoveryPath = refreshError.recoveryPaths.find((path) =>
      path.includes(".portable-auth-refresh-staging"),
    );
    const promotionRecoveryPath = refreshError.recoveryPaths.find((path) =>
      path.includes(".auth.json.next-"),
    );
    assert.equal((await stat(join(stagingRecoveryPath, "auth.json"))).isFile(), true);
    assert.equal((await stat(promotionRecoveryPath)).isFile(), true);
    assert.equal(await readFile(join(authHome, "auth.json"), "utf8"), canonicalBefore);
    assert.equal(
      (await readdir(authHome)).filter((name) => name.startsWith(".auth.json.next-")).length,
      1,
    );
    await assert.rejects(
      refreshManagedAuthRecord({ authHome }),
      (error) => error.code === "recovery_required",
    );
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("authority maps a held advisory lock to a retryable error", async () => {
  const { authHome, root } = await createAuthorityHome();
  let lock;
  try {
    lock = await acquireAdvisoryLock(authorityLockPath(authHome));
    await assert.rejects(refreshManagedAuthRecord({ authHome }), (error) => {
      assert.equal(error.code, "authority_locked");
      assert.equal(error.retryable, true);
      return true;
    });
  } finally {
    await lock?.release();
    await rm(root, { recursive: true, force: true });
  }
});

test("authority refuses the active CODEX_HOME", async () => {
  const { authHome, root } = await createAuthorityHome();
  try {
    const result = spawnSync(process.execPath, [UNSAFE_HOME_FIXTURE, authHome], {
      encoding: "utf8",
      env: { ...process.env, CODEX_HOME: authHome },
    });
    assert.equal(result.status, 0);
    assert.equal(result.stdout, "unsafe_auth_home\n");
    assert.equal(result.stderr, "");
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("authority directory replacement is rejected before protected auth is read", async () => {
  const { authHome, root } = await createAuthorityHome();
  const displaced = join(root, "displaced-authority");
  const protectedHome = join(root, "protected-home");
  const protectedAuth = `${JSON.stringify(
    authDocument({
      accessMarker: "protected",
      lastRefresh: "2026-07-01T08:00:00.000Z",
      refreshToken: "refresh-protected-sensitive",
    }),
  )}\n`;
  let released = false;
  try {
    await mkdir(protectedHome, { mode: 0o700 });
    await writeFile(join(protectedHome, "auth.json"), protectedAuth, { mode: 0o600 });
    await assert.rejects(
      refreshManagedAuthRecord({
        authHome,
        acquireLock: async () => {
          await rename(authHome, displaced);
          await symlink(protectedHome, authHome);
          return {
            assertHeld: async () => {},
            release: async () => {
              released = true;
            },
          };
        },
      }),
      (error) => error.code === "authority_home_replaced",
    );
    assert.equal(released, true);
    assert.equal(await readFile(join(protectedHome, "auth.json"), "utf8"), protectedAuth);
    await assert.rejects(stat(join(protectedHome, ".portable-auth-refresh.lock")), /ENOENT/);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("authority guard defeats directory replacement even when the lock inode is preserved", async () => {
  const { authHome, root } = await createAuthorityHome();
  const displaced = join(root, "displaced-authority");
  const replacementAuth = `${JSON.stringify(
    authDocument({
      accessMarker: "replacement",
      lastRefresh: "2026-07-01T08:00:00.000Z",
      refreshToken: "refresh-replacement-sensitive",
    }),
  )}\n`;
  try {
    let refreshError;
    try {
      await refreshManagedAuthRecord({
        authHome,
        runRefresh: async ({ stagingHome }) => {
          await replaceStagedAuth(stagingHome);
          await rename(authHome, displaced);
          await mkdir(authHome, { mode: 0o700 });
          await rename(
            join(displaced, ".portable-auth-refresh.lock"),
            join(authHome, ".portable-auth-refresh.lock"),
          );
          await writeFile(join(authHome, "auth.json"), replacementAuth, { mode: 0o600 });
          return successResponse();
        },
      });
    } catch (error) {
      refreshError = error;
    }
    assert.equal(refreshError.code, "authority_home_replaced");
    assert.equal(refreshError.recoveryPath, undefined);
    assert.equal(await readFile(join(authHome, "auth.json"), "utf8"), replacementAuth);
    assert.equal(
      (await readdir(authHome)).some((name) => name.startsWith(".auth.json.next-")),
      false,
    );
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("staging setup failure removes a staging root created by this attempt", async () => {
  const { authHome, root } = await createAuthorityHome();
  try {
    await assert.rejects(
      refreshManagedAuthRecord({
        authHome,
        writeStagingFile: async () => {
          throw new Error("synthetic staging write failure");
        },
      }),
      /synthetic staging write failure/,
    );
    await assert.rejects(stat(authorityStagingDirectory(authHome)), /ENOENT/);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("staging setup cleanup never follows paths through a replaced authority home", async () => {
  const { authHome, root } = await createAuthorityHome();
  const displaced = join(root, "displaced-during-staging-setup");
  const setupError = new Error("synthetic config staging failure after replacement");
  let displacedAttempt;
  let marker;
  let writeCount = 0;
  try {
    let refreshError;
    try {
      await refreshManagedAuthRecord({
        authHome,
        writeStagingFile: async (path, contents, options) => {
          writeCount += 1;
          if (writeCount === 1) {
            const attemptName = basename(dirname(path));
            displacedAttempt = join(authorityStagingDirectory(displaced), attemptName);
            await writeFile(path, contents, options);
            return;
          }

          const attemptName = basename(dirname(path));
          await rename(authHome, displaced);
          const replacementAttempt = join(authorityStagingDirectory(authHome), attemptName);
          await mkdir(replacementAttempt, { mode: 0o700, recursive: true });
          marker = join(replacementAttempt, "replacement-marker");
          await writeFile(marker, "must remain\n", { mode: 0o600 });
          throw setupError;
        },
      });
    } catch (error) {
      refreshError = error;
    }

    assert.equal(writeCount, 2);
    assert.equal(refreshError.code, "authority_home_replaced");
    assert.equal(refreshError.recoveryPath, undefined);
    assert.equal(refreshError.recoveryPaths, undefined);
    assert.equal(await readFile(marker, "utf8"), "must remain\n");
    assert.equal((await stat(displacedAttempt)).isDirectory(), true);
    assert.equal((await stat(join(displacedAttempt, "auth.json"))).isFile(), true);
    const metadata = JSON.stringify(managedAuthRefreshErrorMetadata(refreshError));
    assert.equal(metadata.includes(displacedAttempt), false);
    assert.equal(metadata.includes(dirname(marker)), false);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("staging setup cleanup failure reports the retained attempt and gates the next run", async () => {
  const { authHome, root } = await createAuthorityHome();
  const setupSecret = "synthetic staging config credential secret";
  const setupError = new Error(setupSecret);
  let writeCount = 0;
  try {
    let refreshError;
    try {
      await refreshManagedAuthRecord({
        authHome,
        cleanupStagingAttempt: async () => {
          throw new Error("synthetic attempt cleanup failure");
        },
        writeStagingFile: async (path, contents, options) => {
          writeCount += 1;
          if (writeCount === 2) throw setupError;
          await writeFile(path, contents, options);
        },
      });
    } catch (error) {
      refreshError = error;
    }
    assert(refreshError instanceof ManagedAuthRefreshError);
    assert.equal(refreshError.code, "staging_setup_failed");
    assert.equal(refreshError.cause, setupError);
    assert.deepEqual(refreshError.cleanupWarnings, ["staging_cleanup_failed"]);
    assert.equal((await stat(refreshError.recoveryPath)).isDirectory(), true);
    const safeReport = JSON.stringify(managedAuthRefreshFailureReport(refreshError));
    assert.equal(safeReport.includes(setupSecret), false);
    assert.equal(JSON.parse(safeReport).error.recoveryPath, refreshError.recoveryPath);
    assert.deepEqual(JSON.parse(safeReport).error.cleanupWarnings, ["staging_cleanup_failed"]);
    const retainedAttempt = refreshError.recoveryPath;

    let nextRefreshCalled = false;
    await assert.rejects(
      refreshManagedAuthRecord({
        authHome,
        runRefresh: async () => {
          nextRefreshCalled = true;
          return successResponse();
        },
      }),
      (error) =>
        error.code === "recovery_required" &&
        error.recoveryPaths.includes(retainedAttempt),
    );
    assert.equal(nextRefreshCalled, false);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("authority auth.json rejects symlinks and permissive modes", async () => {
  const { authHome, root } = await createAuthorityHome();
  const authPath = join(authHome, "auth.json");
  const target = join(authHome, "auth-target.json");
  try {
    await chmod(authPath, 0o644);
    await assert.rejects(readManagedAuthSnapshot(authHome), (error) => {
      return error.code === "invalid_auth_record" && /group\/world/.test(error.message);
    });
    await chmod(authPath, 0o600);
    await writeFile(target, await readFile(authPath), { mode: 0o600 });
    await rm(authPath);
    await symlink(target, authPath);
    await assert.rejects(readManagedAuthSnapshot(authHome));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("authority auth.json rejects hard links", async () => {
  const { authHome, root } = await createAuthorityHome();
  try {
    await link(join(authHome, "auth.json"), join(authHome, "auth-copy.json"));
    await assert.rejects(
      readManagedAuthSnapshot(authHome),
      (error) => error.code === "invalid_auth_record" && /hard linked/.test(error.message),
    );
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("authority rejects permissive homes and writable parent directories", async () => {
  const permissiveHome = await createAuthorityHome();
  try {
    await chmod(permissiveHome.authHome, 0o755);
    await assert.rejects(
      refreshManagedAuthRecord({ authHome: permissiveHome.authHome }),
      (error) => error.code === "unsafe_auth_home",
    );
  } finally {
    await rm(permissiveHome.root, { recursive: true, force: true });
  }

  const permissiveParent = await createAuthorityHome();
  try {
    await chmod(permissiveParent.root, 0o777);
    await assert.rejects(
      refreshManagedAuthRecord({ authHome: permissiveParent.authHome }),
      (error) => error.code === "unsafe_auth_home",
    );
  } finally {
    await rm(permissiveParent.root, { recursive: true, force: true });
  }
});

test("authority rejects a writable non-sticky grandparent without leaking its path", async () => {
  const { authHome, root } = await createAuthorityHome();
  const grandparent = join(root, "writable-grandparent");
  const parent = join(grandparent, "trusted-parent");
  const nestedAuthHome = join(parent, "authority-home");
  let refreshCalled = false;
  try {
    await mkdir(parent, { mode: 0o700, recursive: true });
    await rename(authHome, nestedAuthHome);
    await chmod(grandparent, 0o777);
    await assert.rejects(
      refreshManagedAuthRecord({
        authHome: nestedAuthHome,
        runRefresh: async () => {
          refreshCalled = true;
          return successResponse();
        },
      }),
      (error) => {
        assert.equal(error.code, "unsafe_auth_home");
        assert.equal(error.message.includes(root), false);
        return true;
      },
    );
    assert.equal(refreshCalled, false);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("authority accepts a sticky shared grandparent protecting a trusted child", async () => {
  const { authHome, root } = await createAuthorityHome();
  const grandparent = join(root, "sticky-grandparent");
  const parent = join(grandparent, "trusted-parent");
  const nestedAuthHome = join(parent, "authority-home");
  try {
    await mkdir(parent, { mode: 0o700, recursive: true });
    await rename(authHome, nestedAuthHome);
    await chmod(grandparent, 0o1777);
    const result = await refreshManagedAuthRecord({
      authHome: nestedAuthHome,
      runRefresh: async ({ stagingHome }) => {
        await replaceStagedAuth(stagingHome);
        return successResponse();
      },
    });
    assert.equal(result.comparisons.accessTokenChanged, true);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("authority requires identity claims in the access token itself", async () => {
  for (const accessClaims of [
    { chatgpt_account_id: ACCOUNT_ID },
    { chatgpt_user_id: USER_ID },
  ]) {
    const { authHome, root } = await createAuthorityHome();
    try {
      const document = JSON.parse(await readFile(join(authHome, "auth.json"), "utf8"));
      document.tokens.access_token = encodeJwt({
        exp: DEFAULT_TEST_TOKEN_EXPIRY_UNIX_SECONDS,
        "https://api.openai.com/auth": accessClaims,
      });
      await writeFile(join(authHome, "auth.json"), `${JSON.stringify(document)}\n`, {
        mode: 0o600,
      });
      await assert.rejects(
        readManagedAuthSnapshot(authHome),
        (error) =>
          error.code === "invalid_auth_record" && /identity claims/.test(error.message),
      );
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  }
});

test("authority auth.json rejects FIFOs without blocking", async () => {
  const { authHome, root } = await createAuthorityHome();
  const authPath = join(authHome, "auth.json");
  try {
    await rm(authPath);
    const mkfifo = spawnSync("mkfifo", [authPath], { encoding: "utf8" });
    assert.equal(mkfifo.status, 0, mkfifo.stderr);
    const probe = spawnSync(process.execPath, [UNSAFE_HOME_FIXTURE, authHome], {
      encoding: "utf8",
      timeout: 1_000,
    });
    assert.equal(probe.signal, null, probe.error?.message);
    assert.equal(probe.status, 0, probe.stderr);
    assert.equal(probe.stdout.trim(), "invalid_auth_record");
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});
