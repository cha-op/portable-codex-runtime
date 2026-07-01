import assert from "node:assert/strict";
import test from "node:test";

import {
  assertAuthorityEvidenceSafe,
  probeLiveAuthRefreshAuthority,
} from "../src/live-auth-refresh-authority-probe.mjs";

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
