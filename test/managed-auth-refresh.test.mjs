import assert from "node:assert/strict";
import { link, mkdtemp, mkdir, readFile, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import {
  ManagedAuthRefreshAuthority,
  ManagedAuthRefreshError,
  authorityLockPath,
  authorityStagingDirectory,
  readManagedAuthSnapshot,
  refreshManagedAuthRecord,
} from "../src/managed-auth-refresh.mjs";

const ACCOUNT_ID = "123e4567-e89b-42d3-a456-426614174088";
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
}) {
  const claims = {
    chatgpt_account_id: accountId,
    chatgpt_plan_type: "enterprise",
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
    });
    assert.deepEqual(result.rpcAudit.map(({ method }) => method), [
      "initialize",
      "initialized",
      "account/read",
    ]);
    assert.equal(after.fileMode, "0600");
    assert.notEqual(afterStat.ino, beforeStat.ino);
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
