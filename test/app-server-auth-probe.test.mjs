import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdir, mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import test from "node:test";

import {
  AppServerClient,
  buildWorkerEnvironment,
  createWorkerAuthMonitor,
  fileExists,
  probeExperimentalGate,
  probeExternalAuthRefresh,
  stopAndAssertNoWorkerAuth,
} from "../src/app-server-auth-probe.mjs";

const codexBin = process.env.CODEX_BIN ?? "codex";
const codexUnavailable =
  spawnSync(codexBin, ["--version"], { stdio: "ignore" }).status === 0
    ? false
    : `Codex CLI is unavailable at ${codexBin}`;

test("worker environment excludes arbitrary host credentials", () => {
  assert.deepEqual(
    buildWorkerEnvironment("/isolated/codex-home", {
      AWS_SECRET_ACCESS_KEY: "aws-secret",
      CODEX_TEST_HOME: "/sensitive/auth-home",
      GH_TOKEN: "github-secret",
      HOME: "/host/home",
      PATH: "/usr/bin:/bin",
      TMPDIR: "/tmp/",
    }),
    {
      CODEX_HOME: "/isolated/codex-home",
      PATH: "/usr/bin:/bin",
      TMPDIR: "/tmp/",
    },
  );
});

test("worker environment preserves standard Windows process variables", () => {
  assert.deepEqual(
    buildWorkerEnvironment("C:\\isolated\\codex-home", {
      ComSpec: "C:\\Windows\\System32\\cmd.exe",
      PATHEXT: ".COM;.EXE;.BAT;.CMD",
      SystemRoot: "C:\\Windows",
      USERPROFILE: "C:\\Users\\sensitive",
      WINDIR: "C:\\Windows",
    }),
    {
      CODEX_HOME: "C:\\isolated\\codex-home",
      ComSpec: "C:\\Windows\\System32\\cmd.exe",
      PATHEXT: ".COM;.EXE;.BAT;.CMD",
      SystemRoot: "C:\\Windows",
      WINDIR: "C:\\Windows",
    },
  );
});

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

test("worker auth is checked before app-server shutdown", async () => {
  const codexHome = await mkdtemp(join(tmpdir(), "portable-codex-pre-stop-fixture-"));
  let stopCalled = false;
  const client = {
    stop: async () => {
      stopCalled = true;
      await rm(join(codexHome, "auth.json"));
    },
  };
  try {
    await writeFile(join(codexHome, "auth.json"), "{}\n", { mode: 0o600 });
    await assert.rejects(
      () => stopAndAssertNoWorkerAuth(client, codexHome),
      /wrote worker auth\.json before shutdown/,
    );
    assert.equal(stopCalled, true);
  } finally {
    await rm(codexHome, { recursive: true, force: true });
  }
});

test(
  "worker auth monitor detects a transient auth file",
  { timeout: 2_000 },
  async () => {
    const codexHome = await mkdtemp(join(tmpdir(), "portable-codex-auth-monitor-"));
    let signalChange;
    const monitor = createWorkerAuthMonitor(codexHome, (_path, _options, listener) => {
      signalChange = listener;
      return {
        close() {},
        on() {},
      };
    });
    try {
      await writeFile(join(codexHome, "auth.json"), "{}\n", { mode: 0o600 });
      signalChange("rename", "auth.json");
      await rm(join(codexHome, "auth.json"));
      await monitor.waitForObservation();
      await assert.rejects(
        () => monitor.assertNoAuthObserved(),
        /created or changed worker auth\.json/,
      );
    } finally {
      monitor.close();
      await rm(codexHome, { recursive: true, force: true });
    }
  },
);

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

test("spawn errors reject cleanly without an unhandled exit promise", async () => {
  const codexHome = await mkdtemp(join(tmpdir(), "portable-codex-spawn-fixture-"));
  const client = new AppServerClient({
    codexBin: join(codexHome, "missing-codex"),
    codexHome,
    timeoutMs: 1_000,
  });
  try {
    await client.start();
    await assert.rejects(
      client.initialize(false),
      (error) => error?.code === "ENOENT",
    );
  } finally {
    await client.stop();
    await rm(codexHome, { recursive: true, force: true });
  }
});

test("invalid app-server JSONL errors omit the raw line", async () => {
  const codexHome = await mkdtemp(join(tmpdir(), "portable-codex-jsonl-fixture-"));
  const fixturePath = join(codexHome, "fixture.mjs");
  const sensitiveLine = "not-json-GITHUB_SECRET_SENTINEL";
  await writeFile(
    fixturePath,
    `
process.stdout.write(${JSON.stringify(`${sensitiveLine}\n`)});
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
    await assert.rejects(client.waitForNotification("fixture/never"), (error) => {
      assert.match(error.message, /invalid app-server JSONL \(31 bytes\)/);
      assert.doesNotMatch(error.stack, /GITHUB_SECRET_SENTINEL/);
      return true;
    });
  } finally {
    await client.stop();
    await rm(codexHome, { recursive: true, force: true });
  }
});

test("non-object app-server JSONL is rejected cleanly", async () => {
  const codexHome = await mkdtemp(join(tmpdir(), "portable-codex-jsonl-shape-fixture-"));
  const fixturePath = join(codexHome, "fixture.mjs");
  await writeFile(
    fixturePath,
    `
process.stdout.write("null\\n");
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
    await assert.rejects(
      client.waitForNotification("fixture/never"),
      /invalid app-server message \(4 bytes\)/,
    );
  } finally {
    await client.stop();
    await rm(codexHome, { recursive: true, force: true });
  }
});

test("unexpected app-server stderr is summarized without exposing its contents", async () => {
  const codexHome = await mkdtemp(join(tmpdir(), "portable-codex-stderr-fixture-"));
  const fixturePath = join(codexHome, "fixture.mjs");
  const sensitiveStderr = "STDERR_SECRET_SENTINEL";
  await writeFile(
    fixturePath,
    `
process.stderr.write(${JSON.stringify(sensitiveStderr)});
process.exit(7);
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
    await assert.rejects(client.waitForNotification("fixture/never"), (error) => {
      assert.match(error.message, /stderr omitted \(22 bytes\)/);
      assert.doesNotMatch(error.stack, /STDERR_SECRET_SENTINEL/);
      return true;
    });
  } finally {
    await client.stop();
    await rm(codexHome, { recursive: true, force: true });
  }
});

test("external auth probe releases setup resources after startup failure", async () => {
  let temporaryHome;
  let mockClosed = false;
  await assert.rejects(
    () =>
      probeExternalAuthRefresh({
        codexBin: join(tmpdir(), "definitely-missing-portable-codex"),
        makeDirectory: async (workspace) => {
          temporaryHome = dirname(workspace);
          await mkdir(workspace);
        },
        startMock: async () => ({
          baseUrl: "http://127.0.0.1:9/v1",
          chatgptBaseUrl: "http://127.0.0.1:9/backend-api",
          close: async () => {
            mockClosed = true;
          },
        }),
      }),
    (error) => error?.code === "ENOENT",
  );
  assert.equal(mockClosed, true);
  assert.equal(await fileExists(temporaryHome), false);
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
