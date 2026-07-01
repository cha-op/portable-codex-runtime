import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import {
  chmod,
  link,
  mkdtemp,
  mkdir,
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
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import test from "node:test";

import {
  ManagedAuthRefreshAuthority,
  ManagedAuthRefreshError,
  authorityLockPath,
  authorityStagingDirectory,
  managedAuthRefreshErrorMetadata,
  readManagedAuthSnapshot,
  refreshManagedAuthRecord,
  runCodexManagedRefresh,
} from "../src/managed-auth-refresh.mjs";
import { AdvisoryLockError, acquireAdvisoryLock } from "../src/advisory-lock.mjs";

const ACCOUNT_ID = "123e4567-e89b-42d3-a456-426614174088";
const USER_ID = "user-123e4567-e89b-42d3-a456-426614174088";
const UNSAFE_HOME_FIXTURE = fileURLToPath(
  new URL("../fixtures/probe-unsafe-authority-home.mjs", import.meta.url),
);
const RPC_AUDIT = [
  { kind: "request", method: "initialize" },
  { kind: "notification", method: "initialized" },
  { kind: "request", method: "account/read" },
];

function encodeJwt(payload) {
  const encode = (value) => Buffer.from(JSON.stringify(value)).toString("base64url");
  return `${encode({ alg: "none", typ: "JWT" })}.${encode(payload)}.${encode("signature")}`;
}

function authDocument({
  accessMarker,
  accountId = ACCOUNT_ID,
  expiresAtUnixSeconds = Math.floor(Date.now() / 1000) + 3600,
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
    ["request", "account/read", { refreshToken: true }],
    ["stop"],
  ]);
  assert.deepEqual(result.rpcAudit, RPC_AUDIT);
});

test("runCodexManagedRefresh always stops the app-server after request failure", async () => {
  let stopped = 0;
  await assert.rejects(
    runCodexManagedRefresh({
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
    /synthetic request failure/,
  );
  assert.equal(stopped, 1);
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
      (error) => error.code === "invalid_refresh_response" && error.retryable === true,
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
      (error) => error.code === "access_token_unchanged" && error.retryable === true,
    );
  } finally {
    await rm(unchanged.root, { recursive: true, force: true });
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

test("permanent account loss becomes reauth_required without changing canonical auth", async () => {
  const { authHome, root } = await createAuthorityHome();
  try {
    const before = await readFile(join(authHome, "auth.json"), "utf8");
    await assert.rejects(
      refreshManagedAuthRecord({
        authHome,
        runRefresh: async () => ({
          ...successResponse(),
          response: { account: null, requiresOpenaiAuth: true },
        }),
      }),
      (error) => {
        assert(error instanceof ManagedAuthRefreshError);
        assert.equal(error.code, "reauth_required");
        assert.equal(error.retryable, false);
        return true;
      },
    );
    assert.equal(await readFile(join(authHome, "auth.json"), "utf8"), before);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("unobserved refresh is retryable and leaves canonical auth unchanged", async () => {
  const { authHome, root } = await createAuthorityHome();
  try {
    const before = await readFile(join(authHome, "auth.json"), "utf8");
    await assert.rejects(
      refreshManagedAuthRecord({ authHome, runRefresh: async () => successResponse() }),
      (error) => {
        assert(error instanceof ManagedAuthRefreshError);
        assert.equal(error.code, "refresh_not_observed");
        assert.equal(error.retryable, true);
        return true;
      },
    );
    assert.equal(await readFile(join(authHome, "auth.json"), "utf8"), before);
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
    assert.equal(refreshError.code, "adapter_shutdown_failed");
    assert.equal(typeof refreshError.recoveryPath, "string");
    assert.equal((await stat(join(refreshError.recoveryPath, "auth.json"))).isFile(), true);
  } finally {
    await rm(root, { recursive: true, force: true });
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
