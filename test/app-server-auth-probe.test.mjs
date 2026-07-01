import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { access, mkdir, mkdtemp, realpath, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { delimiter, dirname, isAbsolute, join, relative, resolve } from "node:path";
import test from "node:test";

import {
  AppServerClient,
  JsonRpcError,
  buildWorkerEnvironment,
  codexVersion,
  createWorkerAuthMonitor,
  fileExists,
  inspectLinuxProcessGroup,
  probeExperimentalGate,
  probeExternalAuthRefresh,
  resolveAppServerExecutable,
  runSequentialCleanup,
  stopAndAssertNoWorkerAuth,
} from "../src/app-server-auth-probe.mjs";
import { readDedicatedChatgptCredential } from "../src/live-app-server-auth-probe.mjs";

const configuredCodexBin = process.env.CODEX_BIN ?? "codex";
const codexBin = resolveAppServerExecutable(configuredCodexBin);
const codexUnavailable =
  spawnSync(codexBin, ["--version"], { stdio: "ignore" }).status === 0
    ? false
    : `Codex CLI is unavailable at ${codexBin}`;

function relativeNodeExecutable() {
  const relativeNode = relative(process.cwd(), process.execPath);
  return relativeNode.includes("/") || relativeNode.includes("\\")
    ? relativeNode
    : `./${relativeNode}`;
}

function encodeJwt(payload) {
  const encode = (value) => Buffer.from(JSON.stringify(value)).toString("base64url");
  return `${encode({ alg: "none", typ: "JWT" })}.${encode(payload)}.${encode("signature")}`;
}

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

test("JSON-RPC errors retain payload access without serializing credential data", () => {
  const syntheticToken = "synthetic-json-rpc-refresh-token";
  const error = new JsonRpcError("account/read", {
    data: { refreshToken: syntheticToken },
    message: `request failed with ${syntheticToken}`,
  });

  assert.equal(error.payload.data.refreshToken, syntheticToken);
  assert.equal(Object.prototype.propertyIsEnumerable.call(error, "payload"), false);
  assert.doesNotMatch(error.message, new RegExp(syntheticToken));
  assert.doesNotMatch(error.stack, new RegExp(syntheticToken));
  assert.equal(JSON.stringify(error).includes(syntheticToken), false);
});

test("dedicated credentials reject access-token expirations outside the Date range", async () => {
  const authHome = await mkdtemp(join(tmpdir(), "portable-codex-invalid-live-exp-"));
  const accountId = "123e4567-e89b-42d3-a456-426614174099";
  const claims = {
    chatgpt_account_id: accountId,
    chatgpt_plan_type: "enterprise",
    chatgpt_user_id: "user-invalid-expiration",
  };
  const refreshToken = "refresh-invalid-expiration-sensitive";
  try {
    await writeFile(
      join(authHome, "auth.json"),
      JSON.stringify({
        auth_mode: "chatgpt",
        tokens: {
          access_token: encodeJwt({
            exp: 1e20,
            "https://api.openai.com/auth": claims,
          }),
          account_id: accountId,
          id_token: encodeJwt({ "https://api.openai.com/auth": claims }),
          refresh_token: refreshToken,
        },
      }),
      { mode: 0o600 },
    );

    await assert.rejects(readDedicatedChatgptCredential(authHome), (error) => {
      assert.equal(error.name, "AssertionError");
      assert.match(error.message, /finite and within the ECMAScript Date range/);
      assert.doesNotMatch(error.stack, /refresh-invalid-expiration-sensitive/);
      assert.notEqual(error.name, "RangeError");
      return true;
    });
  } finally {
    await rm(authHome, { recursive: true, force: true });
  }
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

test("worker environment anchors relative PATH entries to the launcher directory", () => {
  const launcherDirectory = resolve("launcher-root");
  const absoluteBin = resolve(launcherDirectory, "..", "absolute-bin");
  assert.equal(
    buildWorkerEnvironment(
      "/isolated/codex-home",
      { PATH: ["./bin", "", absoluteBin].join(delimiter) },
      launcherDirectory,
    ).PATH,
    [resolve(launcherDirectory, "bin"), launcherDirectory, absoluteBin].join(delimiter),
  );
});

test("app-server executable resolution preserves PATH commands and freezes relative paths", () => {
  assert.equal(resolveAppServerExecutable("codex", "/launcher"), "codex");
  assert.equal(
    resolveAppServerExecutable("./tools/codex", "/launcher"),
    "/launcher/tools/codex",
  );
  assert.equal(resolveAppServerExecutable("/pinned/codex", "/launcher"), "/pinned/codex");
  assert.equal(codexVersion(relativeNodeExecutable()), process.version);
});

test(
  "app-server resolves a bare executable from the launcher-relative PATH after changing cwd",
  { skip: process.platform === "win32" },
  async () => {
    const root = await mkdtemp(join(tmpdir(), "portable-codex-relative-path-"));
    const launcherDirectory = join(root, "launcher");
    const binDirectory = join(launcherDirectory, "bin");
    const codexHome = join(root, "isolated-home");
    const fixturePath = join(root, "fixture.mjs");
    let client;
    try {
      await mkdir(binDirectory, { recursive: true });
      await mkdir(codexHome);
      await symlink(process.execPath, join(binDirectory, "codex"));
      await writeFile(
        fixturePath,
        `
process.stdout.write(JSON.stringify({ method: "fixture/ready" }) + "\\n");
process.stdin.resume();
process.stdin.on("end", () => process.exit(0));
`,
      );
      client = new AppServerClient({
        codexArgs: [fixturePath],
        codexBin: "codex",
        codexHome,
        launcherDirectory,
        sourceEnv: { PATH: ["./bin", "/usr/bin", "/bin"].join(delimiter) },
        timeoutMs: 1_000,
      });
      await client.start();
      await client.waitForNotification("fixture/ready");
    } finally {
      await runSequentialCleanup(
        [
          () => client?.stop(),
          () => rm(root, { force: true, recursive: true }),
        ],
      );
    }
  },
);

test("sequential cleanup preserves a primary failure and runs every cleanup", async () => {
  const primaryError = new Error("primary operation failed");
  const firstCleanupError = new Error("first cleanup failed");
  const secondCleanupError = new Error("second cleanup failed");
  const events = [];
  await assert.rejects(
    async () => {
      let primaryFailure;
      try {
        throw primaryError;
      } catch (error) {
        primaryFailure = { error };
        throw error;
      } finally {
        await runSequentialCleanup(
          [
            async () => {
              events.push("first");
              throw firstCleanupError;
            },
            async () => {
              events.push("second");
              throw secondCleanupError;
            },
            async () => events.push("third"),
          ],
          primaryFailure,
        );
      }
    },
    (error) => error === primaryError,
  );
  assert.deepEqual(events, ["first", "second", "third"]);
  assert.equal(primaryError.cleanupError, firstCleanupError);
  assert.equal(Object.prototype.propertyIsEnumerable.call(primaryError, "cleanupError"), false);
});

test("sequential cleanup fails a successful operation with its first cleanup error", async () => {
  const firstCleanupError = new Error("first cleanup failed");
  const events = [];
  await assert.rejects(
    runSequentialCleanup([
      async () => {
        events.push("first");
        throw firstCleanupError;
      },
      async () => events.push("second"),
      async () => {
        events.push("third");
        throw new Error("later cleanup failed");
      },
    ]),
    (error) => error === firstCleanupError,
  );
  assert.deepEqual(events, ["first", "second", "third"]);
});

test("Linux process-group inspection distinguishes live and zombie-only members", async () => {
  const procRoot = await mkdtemp(join(tmpdir(), "portable-codex-proc-fixture-"));
  const processGroupId = 4242;
  const writeStat = async (pid, command, state, groupId) => {
    const processRoot = join(procRoot, String(pid));
    await mkdir(processRoot);
    await writeFile(
      join(processRoot, "stat"),
      `${pid} (${command}) ${state} 1 ${groupId} ${groupId} 0 0 0\n`,
    );
  };
  try {
    await writeStat(100, "zombie ) worker", "Z", processGroupId);
    await writeStat(101, "unrelated", "S", 9000);
    await writeStat(104, "kernel-style unrelated", "S", 0);
    assert.equal(await inspectLinuxProcessGroup(processGroupId, procRoot), "zombie-only");

    await writeStat(102, "live worker", "D", processGroupId);
    assert.equal(await inspectLinuxProcessGroup(processGroupId, procRoot), "live");

    await rm(join(procRoot, "102"), { recursive: true, force: true });
    await mkdir(join(procRoot, "103"));
    await writeFile(join(procRoot, "103", "stat"), "malformed\n");
    assert.equal(await inspectLinuxProcessGroup(processGroupId, procRoot), "unknown");
  } finally {
    await rm(procRoot, { recursive: true, force: true });
  }
});

function installSyntheticChild(client, { onStdinEnd = () => {} } = {}) {
  client.child = {
    pid: 4242,
    stdin: { destroy: () => {}, end: onStdinEnd },
    stdout: { destroy: () => {} },
    stderr: { destroy: () => {} },
    unref: () => {},
  };
  client.exitPromise = Promise.resolve();
  client.stdout = { close: () => {} };
}

test("successful stop is a permanent no-op for later callers", async () => {
  let existsCalls = 0;
  let waitCalls = 0;
  let stdinEnds = 0;
  const signals = [];
  const client = new AppServerClient({
    codexBin: process.execPath,
    codexHome: "/isolated/codex-home",
    processControl: {
      exists: () => {
        existsCalls += 1;
        return true;
      },
      signal: (_child, signal) => signals.push(signal),
      waitForExit: async () => {
        waitCalls += 1;
        return true;
      },
    },
  });
  installSyntheticChild(client, { onStdinEnd: () => { stdinEnds += 1; } });

  const first = client.stop();
  await first;
  const second = client.stop();
  assert.equal(second, first);
  await second;
  assert.equal(existsCalls, 1);
  assert.equal(waitCalls, 1);
  assert.equal(stdinEnds, 1);
  assert.deepEqual(signals, []);
});

test("concurrent stop callers share one in-flight shutdown", async () => {
  let finishExit;
  const exit = new Promise((resolve) => {
    finishExit = resolve;
  });
  let existsCalls = 0;
  let waitCalls = 0;
  let stdinEnds = 0;
  const client = new AppServerClient({
    codexBin: process.execPath,
    codexHome: "/isolated/codex-home",
    processControl: {
      exists: () => {
        existsCalls += 1;
        return true;
      },
      signal: () => {},
      waitForExit: async () => {
        waitCalls += 1;
        return exit;
      },
    },
  });
  installSyntheticChild(client, { onStdinEnd: () => { stdinEnds += 1; } });

  const first = client.stop();
  const second = client.stop();
  assert.equal(second, first);
  assert.equal(existsCalls, 1);
  assert.equal(waitCalls, 1);
  assert.equal(stdinEnds, 1);
  finishExit(true);
  await Promise.all([first, second]);
});

test("failed stop clears the shared state and allows a successful retry", async () => {
  let waitCalls = 0;
  let existsCalls = 0;
  let stdinEnds = 0;
  const signals = [];
  const client = new AppServerClient({
    codexBin: process.execPath,
    codexHome: "/isolated/codex-home",
    processControl: {
      exists: () => {
        existsCalls += 1;
        return true;
      },
      signal: (_child, signal) => signals.push(signal),
      waitForExit: async () => {
        waitCalls += 1;
        return waitCalls >= 4;
      },
    },
    shutdownGraceMs: 0,
  });
  installSyntheticChild(client, { onStdinEnd: () => { stdinEnds += 1; } });

  const first = client.stop();
  await assert.rejects(first, /survived SIGKILL/);
  const second = client.stop();
  assert.notEqual(second, first);
  await second;
  assert.equal(client.stop(), second);
  assert.equal(existsCalls, 2);
  assert.equal(waitCalls, 4);
  assert.equal(stdinEnds, 2);
  assert.deepEqual(signals, ["SIGTERM", "SIGKILL"]);
});

test("forced-kill settling uses an independent minimum after a short graceful timeout", async () => {
  const waits = [];
  const signals = [];
  const client = new AppServerClient({
    codexBin: process.execPath,
    codexHome: "/isolated/codex-home",
    forcedKillSettleMs: 20,
    processControl: {
      exists: () => true,
      signal: (_child, signal) => signals.push(signal),
      waitForExit: async (_child, timeoutMs, options = {}) => {
        waits.push({ options, timeoutMs });
        return waits.length === 3;
      },
    },
    shutdownGraceMs: 20,
  });
  installSyntheticChild(client);

  await client.stop();
  assert.deepEqual(signals, ["SIGTERM", "SIGKILL"]);
  assert.deepEqual(waits, [
    { options: {}, timeoutMs: 20 },
    { options: {}, timeoutMs: 20 },
    { options: { acceptZombieOnly: true }, timeoutMs: 250 },
  ]);
});

test("abort uses the independent forced-kill settle window", async () => {
  const waits = [];
  const client = new AppServerClient({
    codexBin: process.execPath,
    codexHome: "/isolated/codex-home",
    forcedKillSettleMs: 20,
    processControl: {
      exists: () => true,
      signal: () => {},
      waitForExit: async (_child, timeoutMs, options = {}) => {
        waits.push({ options, timeoutMs });
        return true;
      },
    },
    shutdownGraceMs: 20,
  });
  installSyntheticChild(client);

  await client.abort();
  assert.deepEqual(waits, [
    { options: { acceptZombieOnly: true }, timeoutMs: 250 },
  ]);
});

test("non-finite forced-kill settle values fall back to the bounded default", async () => {
  for (const { action, forcedKillSettleMs } of [
    { action: "stop", forcedKillSettleMs: Number.NaN },
    { action: "abort", forcedKillSettleMs: Number.POSITIVE_INFINITY },
  ]) {
    const waits = [];
    const client = new AppServerClient({
      codexBin: process.execPath,
      codexHome: "/isolated/codex-home",
      forcedKillSettleMs,
      processControl: {
        exists: () => true,
        signal: () => {},
        waitForExit: async (_child, timeoutMs, options = {}) => {
          waits.push({ options, timeoutMs });
          return action === "abort" || waits.length === 3;
        },
      },
      shutdownGraceMs: 20,
    });
    installSyntheticChild(client);

    await client[action]();
    const forcedWait = waits.at(-1);
    assert.deepEqual(forcedWait, {
      options: { acceptZombieOnly: true },
      timeoutMs: 2_000,
    });
    assert.equal(Number.isFinite(forcedWait.timeoutMs), true);
  }
});

test("cleanup-only stop retry does not probe or signal an already quiesced process tree", async () => {
  const closeError = new Error("synthetic output close failure");
  let closeCalls = 0;
  let existsCalls = 0;
  let waitCalls = 0;
  let stdinEnds = 0;
  const signals = [];
  const client = new AppServerClient({
    codexBin: process.execPath,
    codexHome: "/isolated/codex-home",
    processControl: {
      exists: () => {
        existsCalls += 1;
        return true;
      },
      signal: (_child, signal) => signals.push(signal),
      waitForExit: async () => {
        waitCalls += 1;
        return true;
      },
    },
  });
  installSyntheticChild(client, { onStdinEnd: () => { stdinEnds += 1; } });
  client.stdout = {
    close: () => {
      closeCalls += 1;
      if (closeCalls === 1) throw closeError;
    },
  };

  const first = client.stop();
  await assert.rejects(first, (error) => error === closeError);
  const second = client.stop();
  assert.notEqual(second, first);
  await second;
  assert.equal(client.stop(), second);
  assert.equal(closeCalls, 2);
  assert.equal(existsCalls, 1);
  assert.equal(waitCalls, 1);
  assert.equal(stdinEnds, 1);
  assert.deepEqual(signals, []);
});

test("successful abort completes shared shutdown and makes stop a no-op", async () => {
  let existsCalls = 0;
  let waitCalls = 0;
  const signals = [];
  const client = new AppServerClient({
    codexBin: process.execPath,
    codexHome: "/isolated/codex-home",
    processControl: {
      exists: () => {
        existsCalls += 1;
        return true;
      },
      signal: (_child, signal) => signals.push(signal),
      waitForExit: async () => {
        waitCalls += 1;
        return true;
      },
    },
  });
  installSyntheticChild(client);

  const abort = client.abort();
  await abort;
  const stop = client.stop();
  assert.equal(stop, abort);
  await stop;
  assert.equal(existsCalls, 1);
  assert.equal(waitCalls, 1);
  assert.deepEqual(signals, ["SIGKILL"]);
});

test("failed abort clears shared shutdown so stop can retry", async () => {
  let existsCalls = 0;
  let waitCalls = 0;
  const signals = [];
  const client = new AppServerClient({
    codexBin: process.execPath,
    codexHome: "/isolated/codex-home",
    processControl: {
      exists: () => {
        existsCalls += 1;
        return true;
      },
      signal: (_child, signal) => signals.push(signal),
      waitForExit: async () => {
        waitCalls += 1;
        return waitCalls >= 2;
      },
    },
    shutdownGraceMs: 0,
  });
  installSyntheticChild(client);

  const abort = client.abort();
  await assert.rejects(abort, /survived SIGKILL/);
  const stop = client.stop();
  assert.notEqual(stop, abort);
  await stop;
  assert.equal(existsCalls, 2);
  assert.equal(waitCalls, 2);
  assert.deepEqual(signals, ["SIGKILL"]);
});

test("stop rejects pending RPCs and closes output when a process group survives SIGKILL", async () => {
  const signals = [];
  const client = new AppServerClient({
    codexBin: process.execPath,
    codexHome: "/isolated/codex-home",
    processControl: {
      exists: () => true,
      signal: (_child, signal) => signals.push(signal),
      waitForExit: async () => false,
    },
    shutdownGraceMs: 0,
  });
  let stdinEnded = false;
  let outputClosed = false;
  let childUnrefed = false;
  const destroyedStreams = [];
  let pendingTimerFired = false;
  client.child = {
    pid: 4242,
    stdin: {
      destroy: () => destroyedStreams.push("stdin"),
      end: () => { stdinEnded = true; },
    },
    stdout: { destroy: () => destroyedStreams.push("stdout") },
    stderr: { destroy: () => destroyedStreams.push("stderr") },
    unref: () => { childUnrefed = true; },
  };
  client.exitPromise = new Promise(() => {});
  client.stdout = { close: () => { outputClosed = true; } };

  let rejectPending;
  const pendingResult = new Promise((_resolve, reject) => {
    rejectPending = reject;
  }).catch((error) => error);
  const pendingTimer = setTimeout(() => {
    pendingTimerFired = true;
  }, 25);
  client.pending.set(1, {
    method: "fixture/pending",
    reject: rejectPending,
    resolve: () => {},
    timer: pendingTimer,
  });

  let settleTimer;
  const stopOutcome = await Promise.race([
    client.stop().then(
      () => ({ kind: "resolved" }),
      (error) => ({ error, kind: "rejected" }),
    ),
    new Promise((resolve) => {
      settleTimer = setTimeout(() => resolve({ kind: "timed-out" }), 100);
    }),
  ]);
  clearTimeout(settleTimer);
  assert.equal(stopOutcome.kind, "rejected");
  assert.match(stopOutcome.error.message, /survived SIGKILL/);
  assert.equal(await pendingResult, stopOutcome.error);
  assert.equal(stdinEnded, true);
  assert.equal(outputClosed, true);
  assert.deepEqual(destroyedStreams, ["stdin", "stdout", "stderr"]);
  assert.equal(childUnrefed, true);
  assert.deepEqual(signals, ["SIGTERM", "SIGKILL"]);
  assert.equal(client.pending.size, 0);
  await new Promise((resolve) => setTimeout(resolve, 35));
  assert.equal(pendingTimerFired, false);
});

test("abort closes output and preserves process-group survival errors", async () => {
  const signals = [];
  const closeError = new Error("synthetic output close failure");
  const client = new AppServerClient({
    codexBin: process.execPath,
    codexHome: "/isolated/codex-home",
    processControl: {
      exists: () => true,
      signal: (_child, signal) => signals.push(signal),
      waitForExit: async () => false,
    },
    shutdownGraceMs: 0,
  });
  let stdoutCloseAttempted = false;
  let childUnrefed = false;
  const destroyedStreams = [];
  client.child = {
    pid: 4242,
    stdin: { destroy: () => destroyedStreams.push("stdin") },
    stdout: { destroy: () => destroyedStreams.push("stdout") },
    stderr: { destroy: () => destroyedStreams.push("stderr") },
    unref: () => { childUnrefed = true; },
  };
  client.exitPromise = new Promise(() => {});
  client.stdout = {
    close: () => {
      stdoutCloseAttempted = true;
      throw closeError;
    },
  };

  await assert.rejects(client.abort(), (error) => {
    assert.match(error.message, /survived SIGKILL/);
    assert.notEqual(error, closeError);
    return true;
  });
  assert.equal(stdoutCloseAttempted, true);
  assert.deepEqual(destroyedStreams, ["stdin", "stdout", "stderr"]);
  assert.equal(childUnrefed, true);
  assert.deepEqual(signals, ["SIGKILL"]);
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

test("app-server runs from the isolated Codex home", async () => {
  const codexHome = await mkdtemp(join(tmpdir(), "portable-codex-cwd-fixture-"));
  const fixturePath = join(codexHome, "fixture.mjs");
  await writeFile(
    fixturePath,
    `
process.stdout.write(JSON.stringify({ method: "fixture/cwd", params: { cwd: process.cwd() } }) + "\\n");
process.stdin.resume();
process.stdin.on("end", () => process.exit(0));
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
    const message = await client.waitForNotification("fixture/cwd");
    assert.equal(message.params.cwd, await realpath(codexHome));
  } finally {
    await client.stop();
    await rm(codexHome, { recursive: true, force: true });
  }
});

test("app-server resolves a path-containing relative executable before changing cwd", async () => {
  const codexHome = await mkdtemp(join(tmpdir(), "portable-codex-relative-bin-fixture-"));
  const fixturePath = join(codexHome, "fixture.mjs");
  await writeFile(
    fixturePath,
    `
process.stdout.write(JSON.stringify({ method: "fixture/ready" }) + "\\n");
process.stdin.resume();
process.stdin.on("end", () => process.exit(0));
`,
  );
  const relativeNode = relativeNodeExecutable();
  assert.equal(isAbsolute(relativeNode), false);
  const client = new AppServerClient({
    codexBin: relativeNode,
    codexArgs: [fixturePath],
    codexHome,
    timeoutMs: 1_000,
  });
  try {
    assert.equal(client.codexBin, process.execPath);
    await client.start();
    await client.waitForNotification("fixture/ready");
  } finally {
    await client.stop();
    await rm(codexHome, { recursive: true, force: true });
  }
});

test(
  "app-server abort kills an in-flight request before delayed local mutation",
  { skip: process.platform === "win32" },
  async () => {
    const codexHome = await mkdtemp(join(tmpdir(), "portable-codex-abort-fixture-"));
    const fixturePath = join(codexHome, "fixture.mjs");
    const markerPath = join(codexHome, "late-mutation");
    await writeFile(
      fixturePath,
      `
import { writeFileSync } from "node:fs";
import { createInterface } from "node:readline";
const marker = ${JSON.stringify(markerPath)};
const input = createInterface({ input: process.stdin });
input.on("line", (line) => {
  const message = JSON.parse(line);
  if (message.method !== "account/read") return;
  process.stdout.write(JSON.stringify({ method: "fixture/request-started" }) + "\\n");
  setTimeout(() => {
    writeFileSync(marker, "late mutation\\n");
    process.stdout.write(JSON.stringify({ id: message.id, result: {} }) + "\\n");
  }, 250);
});
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
      const request = client.request("account/read", { refreshToken: true });
      await client.waitForNotification("fixture/request-started");
      const lost = new Error("synthetic lock loss");
      const abort = client.abort(lost);
      await assert.rejects(request, (error) => error === lost);
      await abort;
      await new Promise((resolve) => setTimeout(resolve, 350));
      await assert.rejects(access(markerPath), (error) => error.code === "ENOENT");
      assert.equal(client.child.signalCode, "SIGKILL");
    } finally {
      await client.stop();
      await rm(codexHome, { recursive: true, force: true });
    }
  },
);

test(
  "app-server stop reaps descendants from the detached process group",
  { skip: process.platform === "win32" },
  async () => {
    const codexHome = await mkdtemp(join(tmpdir(), "portable-codex-stop-group-fixture-"));
    const fixturePath = join(codexHome, "fixture.mjs");
    const descendantPath = join(codexHome, "descendant.mjs");
    const markerPath = join(codexHome, "orphan-mutation");
    await writeFile(
      descendantPath,
      `
import { writeFileSync } from "node:fs";
process.on("SIGTERM", () => {});
setTimeout(() => writeFileSync(${JSON.stringify(markerPath)}, "orphan mutation\\n"), 250);
setTimeout(() => process.exit(0), 1_000);
`,
    );
    await writeFile(
      fixturePath,
      `
import { spawn } from "node:child_process";
spawn(process.execPath, [${JSON.stringify(descendantPath)}], { stdio: "ignore" });
process.stdout.write(JSON.stringify({ method: "fixture/ready" }) + "\\n");
process.stdin.resume();
process.stdin.on("end", () => process.exit(0));
`,
    );
    const client = new AppServerClient({
      codexBin: process.execPath,
      codexArgs: [fixturePath],
      codexHome,
      shutdownGraceMs: 20,
      timeoutMs: 1_000,
    });
    try {
      await client.start();
      await client.waitForNotification("fixture/ready");
      await client.stop();
      await new Promise((resolve) => setTimeout(resolve, 350));
      await assert.rejects(access(markerPath), (error) => error.code === "ENOENT");
    } finally {
      await client.abort().catch(() => {});
      await rm(codexHome, { recursive: true, force: true });
    }
  },
);

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
