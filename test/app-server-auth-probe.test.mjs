import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import {
  probeExperimentalGate,
  probeExternalAuthRefresh,
  stopAndAssertNoWorkerAuth,
} from "../src/app-server-auth-probe.mjs";

const codexBin = process.env.CODEX_BIN ?? "codex";
const codexUnavailable =
  spawnSync(codexBin, ["--version"], { stdio: "ignore" }).status === 0
    ? false
    : `Codex CLI is unavailable at ${codexBin}`;

test("worker auth is checked after app-server shutdown", async () => {
  const codexHome = await mkdtemp(join(tmpdir(), "portable-codex-stop-fixture-"));
  const client = {
    stop: async () => writeFile(join(codexHome, "auth.json"), "{}\n", { mode: 0o600 }),
  };
  try {
    await assert.rejects(
      () => stopAndAssertNoWorkerAuth(client, codexHome),
      /wrote worker auth\.json during shutdown/,
    );
  } finally {
    await rm(codexHome, { recursive: true, force: true });
  }
});

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
