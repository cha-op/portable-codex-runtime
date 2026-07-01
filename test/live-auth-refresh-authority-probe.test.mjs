import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import test from "node:test";

import {
  assertAuthorityEvidenceSafe,
  probeLiveAuthRefreshAuthority,
} from "../src/live-auth-refresh-authority-probe.mjs";
import {
  ManagedAuthRefreshError,
  managedAuthRefreshFailureReport,
} from "../src/managed-auth-refresh.mjs";

const LIVE_PROBE_SCRIPT = fileURLToPath(
  new URL("../scripts/probe-live-auth-refresh-authority.mjs", import.meta.url),
);

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
