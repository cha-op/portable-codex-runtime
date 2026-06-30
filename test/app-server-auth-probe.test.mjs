import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import {
  AppServerClient,
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

test("app-server shutdown waits for EOF and ignores late refresh replies", async () => {
  const codexHome = await mkdtemp(join(tmpdir(), "portable-codex-stop-client-fixture-"));
  const fixturePath = join(codexHome, "fixture.mjs");
  await writeFile(
    fixturePath,
    `
process.stdout.write(
  JSON.stringify({ id: 1, method: "account/chatgptAuthTokens/refresh", params: {} }) + "\\n",
);
process.stdin.resume();
process.stdin.on("end", () => setTimeout(() => process.exit(0), 20));
`,
  );

  let releaseRefresh;
  let markRefreshStarted;
  const refreshStarted = new Promise((resolve) => {
    markRefreshStarted = resolve;
  });
  const refreshReleased = new Promise((resolve) => {
    releaseRefresh = resolve;
  });
  const client = new AppServerClient({
    codexBin: process.execPath,
    codexArgs: [fixturePath],
    codexHome,
    onRefresh: async () => {
      markRefreshStarted();
      await refreshReleased;
      return { accessToken: "replacement" };
    },
  });

  try {
    await client.start();
    await refreshStarted;
    await client.stop();
    assert.equal(client.child.exitCode, 0);
    assert.equal(client.child.signalCode, null);
    releaseRefresh();
    await new Promise((resolve) => setImmediate(resolve));
  } finally {
    releaseRefresh?.();
    await client.stop();
    await rm(codexHome, { recursive: true, force: true });
  }
});

test("stdin failures reject current and future requests", async () => {
  const codexHome = await mkdtemp(join(tmpdir(), "portable-codex-stdin-fixture-"));
  const fixturePath = join(codexHome, "fixture.mjs");
  await writeFile(
    fixturePath,
    `
process.stdin.destroy();
process.stdout.write(JSON.stringify({ method: "fixture/ready" }) + "\\n");
setTimeout(() => process.exit(0), 500);
`,
  );
  const client = new AppServerClient({
    codexBin: process.execPath,
    codexArgs: [fixturePath],
    codexHome,
    timeoutMs: 1_000,
  });

  try {
    await client.start();
    await client.waitForNotification("fixture/ready");
    await assert.rejects(
      client.request("fixture/write", { payload: "x".repeat(1_000_000) }),
      /EPIPE|exited unexpectedly/,
    );
    await assert.rejects(client.waitForNotification("fixture/never"), /EPIPE|exited unexpectedly/);
    await assert.rejects(client.request("fixture/again", {}), /EPIPE|exited unexpectedly/);
    assert.throws(() => client.notify("fixture/again", {}), /EPIPE|exited unexpectedly/);
  } finally {
    await client.stop();
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
