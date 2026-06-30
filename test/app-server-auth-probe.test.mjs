import assert from "node:assert/strict";
import test from "node:test";

import {
  probeExperimentalGate,
  probeExternalAuthRefresh,
} from "../src/app-server-auth-probe.mjs";

test("chatgptAuthTokens is gated by experimentalApi", async () => {
  const result = await probeExperimentalGate();
  assert.equal(result.gated, true);
  assert.match(result.errorMessage, /experimentalApi capability/);
});

test("chatgptAuthTokens refreshes after 401 without writing auth.json", async () => {
  const result = await probeExternalAuthRefresh();
  assert.match(result.codexVersion, /^codex-cli /);
  assert.equal(result.loginType, "chatgptAuthTokens");
  assert.equal(result.refreshCount, 1);
  assert.deepEqual(result.requestAuthorizationSequence, ["initial", "refreshed"]);
  assert.equal(result.authJsonCreated, false);
  assert.equal(result.turnStatus, "completed");
});
