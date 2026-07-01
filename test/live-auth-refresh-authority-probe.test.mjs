import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import test from "node:test";

import {
  assertAuthorityEvidenceSafe,
  assertAuthorityRefreshTokenChanged,
  codexSourceCommit,
  probeLiveAuthRefreshAuthority,
} from "../src/live-auth-refresh-authority-probe.mjs";
import {
  ManagedAuthRefreshError,
  managedAuthRefreshFailureReport,
} from "../src/managed-auth-refresh.mjs";

const LIVE_PROBE_SCRIPT = fileURLToPath(
  new URL("../scripts/probe-live-auth-refresh-authority.mjs", import.meta.url),
);

test("source provenance is omitted without an explicit source mirror", () => {
  let spawnCalls = 0;
  const commit = codexSourceCommit(undefined, () => {
    spawnCalls += 1;
    throw new Error("git must not run without an explicit source mirror");
  });

  assert.equal(commit, null);
  assert.equal(spawnCalls, 0);
});

test("source provenance resolves HEAD for an explicit source mirror", () => {
  const calls = [];
  const commit = codexSourceCommit("/explicit/codex", (...args) => {
    calls.push(args);
    return { status: 0, stdout: "0123456789abcdef\n" };
  });

  assert.equal(commit, "0123456789abcdef");
  assert.deepEqual(calls, [
    [
      "git",
      ["-C", "/explicit/codex", "rev-parse", "HEAD"],
      {
        encoding: "utf8",
        stdio: ["ignore", "pipe", "ignore"],
      },
    ],
  ]);
});

test("live authority probe rejects PATH-resolved Codex before reading auth", async () => {
  await assert.rejects(
    probeLiveAuthRefreshAuthority({
      allowAuthMutation: true,
      authHome: "/definitely/missing/auth-home",
      codexBin: "codex",
    }),
    /CODEX_BIN to be an absolute pinned-image path/,
  );
});

test("live authority probe rejects unsupported platforms before auth mutation", async () => {
  let authorityCreationCalls = 0;
  let snapshotReadCalls = 0;
  let temporaryDirectoryCalls = 0;
  let workerProbeCalls = 0;
  await assert.rejects(
    probeLiveAuthRefreshAuthority({
      allowAuthMutation: true,
      authHome: "/definitely/missing/auth-home",
      codexBin: "/pinned/codex",
      createAuthority: () => {
        authorityCreationCalls += 1;
        throw new Error("authority creation must not be reached");
      },
      makeTemporaryDirectory: async () => {
        temporaryDirectoryCalls += 1;
        throw new Error("worker home creation must not be reached");
      },
      platform: "win32",
      readSnapshot: async () => {
        snapshotReadCalls += 1;
        throw new Error("auth snapshot read must not be reached");
      },
      runWorkerProbe: async () => {
        workerProbeCalls += 1;
        throw new Error("worker probe must not be reached");
      },
    }),
    (error) => error?.code === "unsupported_platform",
  );
  assert.equal(authorityCreationCalls, 0);
  assert.equal(snapshotReadCalls, 0);
  assert.equal(temporaryDirectoryCalls, 0);
  assert.equal(workerProbeCalls, 0);
});

test("managed auth failure reports never serialize generic error details", () => {
  const secret = "refresh-token-secret-sentinel";
  const report = managedAuthRefreshFailureReport(new Error(secret));
  assert.deepEqual(report, {
    error: {
      code: "live_probe_failed",
      retryable: false,
      type: "probe_failure",
    },
    result: "failed",
  });
  assert.equal(JSON.stringify(report).includes(secret), false);

  const managed = new ManagedAuthRefreshError("recovery_required", secret, {
    recoveryPath: "/dedicated/recovery/attempt-1",
    recoveryReason: "orphaned_refresh_artifacts",
  });
  managed.cause = new Error(secret);
  const managedReport = managedAuthRefreshFailureReport(managed);
  assert.equal(managedReport.error.code, "recovery_required");
  assert.equal(managedReport.error.recoveryPath, "/dedicated/recovery/attempt-1");
  assert.equal(JSON.stringify(managedReport).includes(secret), false);
});

test("live authority probe script emits only structured sanitized failures", () => {
  const env = { ...process.env, CODEX_ALLOW_AUTH_MUTATION: "1" };
  delete env.CODEX_BIN;
  const result = spawnSync(process.execPath, [LIVE_PROBE_SCRIPT], {
    encoding: "utf8",
    env,
  });
  assert.equal(result.status, 1);
  assert.equal(result.stdout, "");
  assert.deepEqual(JSON.parse(result.stderr), {
    error: {
      code: "live_probe_failed",
      retryable: false,
      type: "probe_failure",
    },
    result: "failed",
  });
  assert.doesNotMatch(result.stderr, /AssertionError|\.mjs:\d+|at probeLive/);
});

test("authority evidence rejects raw current or rotated credentials", () => {
  const oldAccess = "old-access-token-sensitive";
  const newRefresh = "new-refresh-token-sensitive";
  assert.doesNotThrow(() =>
    assertAuthorityEvidenceSafe(
      JSON.stringify({ accessTokenChanged: true, refreshTokenChanged: true }),
      [oldAccess, newRefresh],
    ),
  );
  assert.throws(
    () => assertAuthorityEvidenceSafe(JSON.stringify({ token: oldAccess }), [oldAccess]),
    /credential or account identity material/,
  );
  assert.throws(
    () =>
      assertAuthorityEvidenceSafe(
        JSON.stringify({ token: "eyJhbGciOiJub25lIn0.eyJzdWIiOiJzZWNyZXQifQ.signature" }),
        [],
      ),
    /eyJ/,
  );
});

test("live authority evidence fails closed when refresh-token rotation is unproven", () => {
  assert.doesNotThrow(() =>
    assertAuthorityRefreshTokenChanged({ refreshTokenChanged: true }),
  );
  assert.throws(
    () => assertAuthorityRefreshTokenChanged({ refreshTokenChanged: false }),
    /refresh token did not rotate/,
  );
  assert.throws(
    () => assertAuthorityRefreshTokenChanged(undefined),
    /refresh token did not rotate/,
  );
});
