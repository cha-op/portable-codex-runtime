import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import test from "node:test";

import {
  probeExperimentalGate,
  probeExternalAuthRefresh,
} from "../src/app-server-auth-probe.mjs";

const codexBin = process.env.CODEX_BIN ?? "codex";
const codexUnavailable =
  spawnSync(codexBin, ["--version"], { stdio: "ignore" }).status === 0
    ? false
    : `Codex CLI is unavailable at ${codexBin}`;

test(
  "chatgptAuthTokens is gated by experimentalApi",
  { skip: codexUnavailable },
  async () => {
    const result = await probeExperimentalGate();
    assert.equal(result.gated, true);
    assert.match(result.errorMessage, /experimentalApi capability/);
  },
);

test(
  "chatgptAuthTokens refreshes after 401 without writing auth.json",
  { skip: codexUnavailable },
  async () => {
    const result = await probeExternalAuthRefresh();
    assert.match(result.codexVersion, /^codex-cli /);
    assert.equal(result.loginType, "chatgptAuthTokens");
    assert.equal(result.refreshCount, 1);
    assert.deepEqual(result.requestAuthorizationSequence, ["initial", "refreshed"]);
    assert.equal(result.authJsonCreated, false);
    assert.equal(result.turnStatus, "completed");
  },
);
