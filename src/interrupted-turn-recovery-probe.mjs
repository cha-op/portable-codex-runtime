import assert from "node:assert/strict";
import { once } from "node:events";
import { constants as fsConstants } from "node:fs";
import {
  chmod,
  copyFile,
  lstat,
  mkdir,
  mkdtemp,
  open,
  readFile,
  realpath,
  rename,
  rm,
  rmdir,
  stat,
  writeFile,
} from "node:fs/promises";
import { createServer } from "node:http";
import { arch, platform, tmpdir } from "node:os";
import { basename, dirname, isAbsolute, join, resolve } from "node:path";

import {
  AppServerClient,
  buildWorkerEnvironment,
  codexVersion,
  resolveAppServerExecutable,
  runSequentialCleanup,
} from "./app-server-auth-probe.mjs";
import {
  authorityDirectoryPermissionsAreSafe,
} from "./managed-auth-refresh.mjs";

import {
  assertPortableDirectoryNames,
  copyStoppedTree,
  decodePortablePathBytes,
  digestFile,
  digestTree,
  inspectLinuxRecoveryAcl,
  parseDarwinMountTable,
  parseLinuxGetfacl,
  parseLinuxMountInfo,
  portableMode,
  recoveryPathHasExtendedAcl,
  recoveryPathHasUnsafeAncestorAcl,
  removeTreeForCleanup,
  sameFileIdentity,
} from "./stopped-tree.mjs";

export {
  assertPortableDirectoryNames,
  copyStoppedTree,
  decodePortablePathBytes,
  digestTree,
  inspectLinuxRecoveryAcl,
  parseDarwinMountTable,
  parseLinuxGetfacl,
  parseLinuxMountInfo,
  removeTreeForCleanup,
};

export const PINNED_SOURCE_ANALYSIS_COMMIT =
  "db887d03e1f907467e33271572dffb73bceecd6b";
export const RECOVERY_SCENARIOS = Object.freeze([
  "logical_interrupt",
  "sigterm",
  "sigkill",
  "snapshot_restore",
]);

const DEFAULT_TIMEOUT_MS = 20_000;
const SENSITIVE_EVIDENCE_PATTERNS = [
  /(?:^|["'\s])\/[^"'\s]/,
  /\b[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\b/i,
  /eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]*/,
  /\b(?:sk|sess|rk)-[A-Za-z0-9_-]{8,}\b/,
  /<turn_aborted>/,
  /portable recovery probe/i,
];
const JSON_STRING_TOKEN_PATTERN =
  /"(?:\\(?:["\\/bfnrt]|u[0-9a-fA-F]{4})|[^"\\\u0000-\u001F])*"/g;
const JSON_STRING_AT_PATTERN =
  /"(?:\\(?:["\\/bfnrt]|u[0-9a-fA-F]{4})|[^"\\\u0000-\u001F])*"/y;
const JSON_PRIMITIVE_AT_PATTERN =
  /(?:-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?|true|false|null)/y;
const MAX_EVIDENCE_STRING_SURFACES = 1_024;
const NODE_ARCHITECTURES = new Set([
  "arm",
  "arm64",
  "ia32",
  "loong64",
  "mips",
  "mipsel",
  "ppc",
  "ppc64",
  "riscv64",
  "s390",
  "s390x",
  "x64",
]);

async function withTimeout(promise, timeoutMs, label) {
  let timer;
  try {
    return await Promise.race([
      promise,
      new Promise((_, reject) => {
        timer = setTimeout(
          () => reject(new Error(`${label} timed out after ${timeoutMs} ms`)),
          timeoutMs,
        );
      }),
    ]);
  } finally {
    clearTimeout(timer);
  }
}

function completedSseBody() {
  const events = [
    { type: "response.created", response: { id: "resp-recovery-probe" } },
    {
      type: "response.output_item.done",
      item: {
        type: "message",
        role: "assistant",
        id: "msg-recovery-probe",
        content: [{ type: "output_text", text: "recovery probe ok" }],
      },
    },
    {
      type: "response.completed",
      response: {
        id: "resp-recovery-probe",
        usage: {
          input_tokens: 0,
          input_tokens_details: null,
          output_tokens: 0,
          output_tokens_details: null,
          total_tokens: 0,
        },
      },
    },
  ];
  return events
    .map((event) => `event: ${event.type}\ndata: ${JSON.stringify(event)}\n\n`)
    .join("");
}

export async function startHeldResponsesMock({ timeoutMs = DEFAULT_TIMEOUT_MS } = {}) {
  const requests = [];
  const responses = new Set();
  const sockets = new Set();
  const waiters = [];
  const server = createServer(async (request, response) => {
    try {
      const chunks = [];
      for await (const chunk of request) chunks.push(chunk);
      const body = Buffer.concat(chunks).toString("utf8");

      if (request.method !== "POST" || request.url !== "/v1/responses") {
        response.writeHead(404, { "content-type": "application/json" });
        response.end(JSON.stringify({ error: { message: "not found" } }));
        return;
      }

      requests.push(body);
      while (waiters.length > 0) waiters.shift()();
      response.writeHead(200, {
        "cache-control": "no-cache",
        "content-type": "text/event-stream",
      });
      if (requests.length === 1) {
        responses.add(response);
        response.once("close", () => responses.delete(response));
        response.write(
          `event: response.created\ndata: ${JSON.stringify({
            type: "response.created",
            response: { id: "resp-held-recovery-probe" },
          })}\n\n`,
        );
        return;
      }
      response.end(completedSseBody());
    } catch {
      response.destroy();
    }
  });
  server.on("connection", (socket) => {
    sockets.add(socket);
    socket.once("close", () => sockets.delete(socket));
  });

  const close = async () => {
    for (const response of responses) response.destroy();
    for (const socket of sockets) socket.destroy();
    if (!server.listening) return;
    server.close();
    await once(server, "close");
  };

  try {
    server.listen(0, "127.0.0.1");
    await once(server, "listening");
    const address = server.address();
    assert(address && typeof address !== "string");
    return {
      baseUrl: `http://127.0.0.1:${address.port}/v1`,
      close,
      requestBody(index) {
        return requests[index];
      },
      requestCount() {
        return requests.length;
      },
      async waitForRequest(count) {
        const deadline = Date.now() + timeoutMs;
        while (requests.length < count) {
          const remaining = deadline - Date.now();
          assert(remaining > 0, `timed out waiting for mock request ${count}`);
          await withTimeout(
            new Promise((resolveWaiter) => waiters.push(resolveWaiter)),
            remaining,
            `mock request ${count}`,
          );
        }
      },
    };
  } catch (error) {
    await close().catch(() => {});
    throw error;
  }
}

async function writeRecoveryConfig(codexHome, baseUrl) {
  const config = `
model = "mock-model"
model_provider = "recovery_probe"
approval_policy = "never"
sandbox_mode = "read-only"
disable_response_storage = true

[agents]
interrupt_message = true

[features]
shell_snapshot = false

[model_providers.recovery_probe]
name = "Recovery probe"
base_url = "${baseUrl}"
wire_api = "responses"
requires_openai_auth = false
request_max_retries = 0
stream_max_retries = 0
supports_websockets = false
`;
  const configPath = join(codexHome, "config.toml");
  await writeFile(configPath, config, { mode: 0o600 });
  await chmod(configPath, 0o600);
}

export async function createRecoveryLayout(ownedRoot) {
  const sessionRoot = join(ownedRoot, "session");
  const codexHome = join(sessionRoot, "codex-home");
  const workspace = join(sessionRoot, "workspace");
  for (const path of [sessionRoot, codexHome, workspace]) {
    await mkdir(path, { mode: 0o700 });
    await chmod(path, 0o700);
  }
  return { codexHome, sessionRoot, workspace };
}

export function assertProcessGroupTarget(pid, currentPid = process.pid) {
  if (!Number.isSafeInteger(pid) || pid <= 1 || pid === currentPid) {
    throw new Error("refusing unsafe app-server process-group target");
  }
  return pid;
}

/**
 * Terminate a probe-owned AppServerClient child. AppServerClient.start() always
 * spawns detached, so its child PID is the process-group ID. Signal observation
 * is an intentional pinned-runtime contract; a trapped signal and clean exit
 * must fail the compatibility probe until that contract is reviewed.
 */
export async function terminateAppServer(
  client,
  signal,
  {
    abortClient = () => client.abort(),
    killProcess = process.kill,
    timeoutMs = 5_000,
  } = {},
) {
  const pid = assertProcessGroupTarget(client?.child?.pid);
  if (signal !== "SIGTERM" && signal !== "SIGKILL") {
    throw new Error("recovery probe permits only SIGTERM or SIGKILL");
  }

  let primaryFailure;
  let exitResult;
  client.stopping = true;
  try {
    // AppServerClient starts the child detached, so its PID is also the process-group ID.
    try {
      killProcess(-pid, signal);
    } catch (error) {
      if (error?.code !== "ESRCH") throw error;
    }
    exitResult = await withTimeout(client.exitPromise, timeoutMs, `${signal} app-server exit`);
    // The pinned compatibility scenario requires signal termination, not merely recovery
    // after an implementation-defined clean exit.
    assert.equal(exitResult[0], null, `app-server exited with code ${exitResult[0]}`);
    assert.equal(exitResult[1], signal, `app-server observed ${exitResult[1]} instead of ${signal}`);
    await withTimeout(
      Promise.all([
        client.childClosePromise ?? client.exitPromise,
        client.stdoutClosePromise ?? Promise.resolve(),
      ]),
      timeoutMs,
      `${signal} app-server stdio close`,
    );
  } catch (error) {
    primaryFailure = { error };
    throw error;
  } finally {
    try {
      await abortClient();
    } catch (cleanupError) {
      if (!primaryFailure) throw cleanupError;
      if (!Object.hasOwn(primaryFailure.error, "cleanupError")) {
        Object.defineProperty(primaryFailure.error, "cleanupError", {
          configurable: true,
          enumerable: false,
          value: cleanupError,
        });
      }
    }
  }
  return { signal: exitResult[1] };
}

function findTailTurn(thread, turnId) {
  assert(Array.isArray(thread?.turns) && thread.turns.length > 0, "thread response omitted turns");
  const turn = thread.turns.at(-1);
  assert.equal(turn.id, turnId, "recovered tail did not match the interrupted turn");
  return turn;
}

export function assertNewTurnId(turnId, interruptedTurnId) {
  assert(typeof turnId === "string" && turnId.length > 0, "follow-up turn omitted its ID");
  assert.notEqual(turnId, interruptedTurnId, "follow-up turn reused the interrupted turn ID");
  return turnId;
}

export async function verifyModelWorkspaceContext(
  requestBody,
  {
    canonicalizePath = realpath,
    previousWorkspace,
    previousWorkspaceCanonical = previousWorkspace,
    workspace,
  },
) {
  let request;
  try {
    request = JSON.parse(requestBody);
  } catch {
    throw new Error("follow-up model request was not valid JSON");
  }
  assert(Array.isArray(request?.input), "follow-up model request omitted input messages");
  const contextCwds = [];
  for (const item of request.input) {
    if (item?.type !== "message" || item.role !== "user" || !Array.isArray(item.content)) continue;
    for (const content of item.content) {
      if (content?.type !== "input_text" || typeof content.text !== "string") continue;
      if (!content.text.includes("<environment_context>")) continue;
      for (const match of content.text.matchAll(/<cwd>([^<\r\n]+)<\/cwd>/g)) {
        contextCwds.push(match[1]);
      }
    }
  }
  assert(contextCwds.length > 0, "follow-up model request omitted workspace context");
  const activeWorkspace = contextCwds.at(-1);
  const expectedWorkspaceCanonical = await canonicalizePath(workspace);
  let activeWorkspaceMatched =
    activeWorkspace === workspace || activeWorkspace === expectedWorkspaceCanonical;
  if (!activeWorkspaceMatched) {
    try {
      activeWorkspaceMatched =
        (await canonicalizePath(activeWorkspace)) === expectedWorkspaceCanonical;
    } catch (error) {
      if (error?.code !== "ENOENT") throw error;
    }
  }
  assert(
    activeWorkspaceMatched,
    "latest model workspace context did not match the resumed workspace",
  );
  const relocated = previousWorkspace !== undefined && previousWorkspace !== workspace;
  const previousWorkspaceAliases = new Set([
    previousWorkspace,
    previousWorkspaceCanonical,
  ]);
  const historicalWorkspaceRetained =
    relocated && contextCwds.slice(0, -1).some((cwd) => previousWorkspaceAliases.has(cwd));
  if (relocated) {
    assert(
      historicalWorkspaceRetained,
      "relocated model context omitted the immutable historical workspace",
    );
  }
  return { activeWorkspaceMatched: true, historicalWorkspaceRetained };
}

export async function startRecoveryClient({
  codexBin,
  codexHome,
  createClient = (options) => new AppServerClient(options),
  timeoutMs,
}) {
  const client = createClient({ codexBin, codexHome, timeoutMs });
  try {
    await client.start();
    await client.initialize(false);
    return client;
  } catch (error) {
    await runSequentialCleanup([() => client.abort()], { error });
    throw error;
  }
}

async function recoverAndInspect({
  client,
  coldReadTurnStatus,
  mock,
  previousWorkspace,
  previousWorkspaceCanonical,
  threadId,
  turnId,
  workspace,
  expectAbortMarker,
  modelRequestBaseline,
}) {
  const resumed = await client.request("thread/resume", { threadId, cwd: workspace });
  assert.equal(resumed.thread.id, threadId);
  assert.equal(await realpath(resumed.cwd), await realpath(workspace));
  const resumedTurn = findTailTurn(resumed.thread, turnId);
  assert.equal(resumedTurn.status, "interrupted");

  assert.equal(
    mock.requestCount(),
    modelRequestBaseline,
    "cold read or resume unexpectedly issued a model request",
  );
  const requestIndex = modelRequestBaseline;
  const followUpTurn = await client.request("turn/start", {
    threadId,
    input: [{ type: "text", text: "Complete the portable recovery probe.", textElements: [] }],
  });
  const followUpTurnId = assertNewTurnId(followUpTurn?.turn?.id, turnId);
  const completed = await client.waitForNotification("turn/completed");
  assert.equal(completed.params.turn.id, followUpTurnId);
  assert.equal(completed.params.turn.status, "completed");
  await mock.waitForRequest(requestIndex + 1);
  assert.equal(mock.requestCount(), requestIndex + 1, "follow-up turn issued unexpected model requests");
  const followUpRequest = mock.requestBody(requestIndex);
  const markerPresent = followUpRequest.includes("<turn_aborted>");
  assert.equal(markerPresent, expectAbortMarker);
  const workspaceContext = await verifyModelWorkspaceContext(followUpRequest, {
    previousWorkspace,
    previousWorkspaceCanonical,
    workspace,
  });

  return {
    resumeSucceeded: true,
    sameThreadId: true,
    tailTurnStatus: resumedTurn.status,
    threadReadAgrees: coldReadTurnStatus === resumedTurn.status,
    modelAbortMarker: markerPresent ? "present" : "absent",
    modelWorkspaceContextMatched: workspaceContext.activeWorkspaceMatched,
    historicalWorkspaceRetained: workspaceContext.historicalWorkspaceRetained,
  };
}

export async function runRecoveryScenario({
  codexBin,
  kind,
  makeTemporaryDirectory = mkdtemp,
  startMock = startHeldResponsesMock,
  temporaryRoot,
  timeoutMs = DEFAULT_TIMEOUT_MS,
}) {
  if (!RECOVERY_SCENARIOS.includes(kind)) throw new Error(`unknown recovery scenario: ${kind}`);
  assert(
    typeof temporaryRoot === "string" && isAbsolute(temporaryRoot),
    "recovery scenario requires a validated absolute temporary root",
  );
  const ownedRoot = await makeTemporaryDirectory(
    join(temporaryRoot, "portable-codex-recovery-"),
  );
  let client;
  let readClient;
  let recoveredClient;
  let mock;
  let primaryFailure;
  try {
    await chmod(ownedRoot, 0o700);
    let { codexHome, sessionRoot, workspace } = await createRecoveryLayout(ownedRoot);
    const originalWorkspace = workspace;
    const originalWorkspaceCanonical = await realpath(workspace);
    const sentinelPath = join(workspace, "sentinel.txt");
    await writeFile(sentinelPath, "portable-recovery-sentinel\n", {
      mode: 0o600,
    });
    await chmod(sentinelPath, 0o600);
    mock = await startMock({ timeoutMs });
    await writeRecoveryConfig(codexHome, mock.baseUrl);

    client = await startRecoveryClient({ codexBin, codexHome, timeoutMs });
    const started = await client.request("thread/start", { cwd: workspace, model: "mock-model" });
    const threadId = started.thread.id;
    const turn = await client.request("turn/start", {
      threadId,
      input: [{ type: "text", text: "Run the portable recovery probe.", textElements: [] }],
    });
    const turnId = turn.turn.id;
    await mock.waitForRequest(1);

    let terminationObserved;
    let originalCompletionObserved = false;
    let expectAbortMarker = false;
    let snapshot;
    if (kind === "logical_interrupt") {
      const completedPromise = client.waitForNotification("turn/completed");
      // If the interrupt RPC fails, outer cleanup rejects this waiter later.
      // Observe that rejection immediately while retaining the original promise
      // for the successful path below.
      void completedPromise.catch(() => {});
      await client.request("turn/interrupt", { threadId, turnId });
      const completed = await completedPromise;
      assert.equal(completed.params.turn.id, turnId);
      assert.equal(completed.params.turn.status, "interrupted");
      originalCompletionObserved = true;
      expectAbortMarker = true;
      terminationObserved = "rpc-interrupt";
      await client.stop();
    } else {
      const signal = kind === "sigterm" ? "SIGTERM" : "SIGKILL";
      const completedPromise = client.waitForNotification("turn/completed");
      void completedPromise.catch(() => {});
      const termination = await terminateAppServer(client, signal, { timeoutMs: 5_000 });
      terminationObserved = termination.signal;
      let completed;
      try {
        completed = await completedPromise;
      } catch {
        // Client shutdown rejects the pre-registered waiter when no completion arrived.
      }
      if (completed) {
        assert.equal(completed.params.turn.id, turnId);
        originalCompletionObserved = true;
      }
      assert.equal(
        originalCompletionObserved,
        false,
        `${signal} scenario unexpectedly observed turn/completed`,
      );
    }

    if (kind === "snapshot_restore") {
      const sourceDigest = await digestTree(sessionRoot);
      const sourceWorkspaceDigest = await digestTree(workspace);
      const backupRoot = join(ownedRoot, "stopped-tree-copy");
      await copyStoppedTree({ ownedRoot, source: sessionRoot, destination: backupRoot });
      const backupDigest = await digestTree(backupRoot);
      assert.equal(backupDigest, sourceDigest);
      assert.equal(await digestTree(join(backupRoot, "workspace")), sourceWorkspaceDigest);
      await removeTreeForCleanup(sessionRoot);
      const restoredRoot = join(ownedRoot, "restored-session");
      await copyStoppedTree({ ownedRoot, source: backupRoot, destination: restoredRoot });
      const restoredDigest = await digestTree(restoredRoot);
      assert.equal(restoredDigest, sourceDigest);
      sessionRoot = restoredRoot;
      codexHome = join(sessionRoot, "codex-home");
      workspace = join(sessionRoot, "workspace");
      assert.equal(
        await readFile(join(workspace, "sentinel.txt"), "utf8"),
        "portable-recovery-sentinel\n",
      );
      assert.equal(await digestTree(workspace), sourceWorkspaceDigest);
      snapshot = {
        kind: "stopped-tree-copy",
        sourceQuiesced: true,
        modeledTreeDigestMatched: true,
        workspaceModeledDigestMatched: true,
      };
    }

    assert.equal(
      mock.requestCount(),
      1,
      "interrupted turn issued unexpected model requests",
    );
    const modelRequestBaseline = mock.requestCount();
    const coldReadRoot = join(ownedRoot, "cold-read-session");
    const recoveryStateDigest = await digestTree(sessionRoot);
    await copyStoppedTree({
      ownedRoot,
      source: sessionRoot,
      destination: coldReadRoot,
    });
    assert.equal(await digestTree(coldReadRoot), recoveryStateDigest);
    const heldRecoveryRoot = join(ownedRoot, "resume-held-session");
    const recoveryRootMode = portableMode(await lstat(sessionRoot));
    await rename(sessionRoot, heldRecoveryRoot);
    await chmod(heldRecoveryRoot, 0o000);
    let coldReadTurn;
    let coldReadFailure;
    try {
      try {
        await lstat(sessionRoot);
        throw new Error("cold read left the original recovery path reachable");
      } catch (error) {
        if (error?.code !== "ENOENT") throw error;
      }
      readClient = await startRecoveryClient({
        codexBin,
        codexHome: join(coldReadRoot, "codex-home"),
        timeoutMs,
      });
      const coldRead = await readClient.request("thread/read", {
        threadId,
        includeTurns: true,
      });
      assert.equal(coldRead.thread.id, threadId);
      coldReadTurn = findTailTurn(coldRead.thread, turnId);
      assert.equal(coldReadTurn.status, "interrupted");
      await readClient.stop();
    } catch (error) {
      coldReadFailure = { error };
      throw error;
    } finally {
      await runSequentialCleanup(
        [
          () => readClient?.abort(),
          () => chmod(heldRecoveryRoot, recoveryRootMode),
          () => rename(heldRecoveryRoot, sessionRoot),
        ],
        coldReadFailure,
      );
      readClient = undefined;
    }

    recoveredClient = await startRecoveryClient({ codexBin, codexHome, timeoutMs });
    const recovery = await recoverAndInspect({
      client: recoveredClient,
      coldReadTurnStatus: coldReadTurn.status,
      mock,
      previousWorkspace: originalWorkspace,
      previousWorkspaceCanonical: originalWorkspaceCanonical,
      threadId,
      turnId,
      workspace,
      expectAbortMarker,
      modelRequestBaseline,
    });
    const {
      historicalWorkspaceRetained,
      modelWorkspaceContextMatched,
      ...recoveryReport
    } = recovery;
    if (snapshot) {
      snapshot.appServerWorkspaceMatched = modelWorkspaceContextMatched;
      snapshot.historicalWorkspaceRetained = historicalWorkspaceRetained;
    }
    await recoveredClient.stop();

    return {
      kind,
      turnMaterialized: true,
      terminationObserved,
      originalCompletionObserved,
      threadReadIsolation: "copy-original-path-absent-held-tree-000",
      ...recoveryReport,
      ...(snapshot ? { snapshot } : {}),
    };
  } catch (error) {
    primaryFailure = { error };
    throw error;
  } finally {
    await runSequentialCleanup(
      [
        () => recoveredClient?.abort(),
        () => readClient?.abort(),
        () => client?.abort(),
        () => mock?.close(),
        () => removeTreeForCleanup(ownedRoot),
      ],
      primaryFailure,
    );
  }
}

function assertExactObject(value, keys, label) {
  assert(value !== null && typeof value === "object" && !Array.isArray(value), `${label} must be an object`);
  assert.deepEqual(Object.keys(value).sort(), [...keys].sort(), `${label} contains unexpected fields`);
}

function assertRecoveryEvidenceSchema(report) {
  assertExactObject(
    report,
    ["schemaVersion", "probe", "runtime", "backend", "snapshot", "scenarios", "result"],
    "recovery evidence",
  );
  assert.equal(report.schemaVersion, 5);
  assert.equal(report.probe, "interrupted-turn-recovery");
  assert.equal(report.result, "passed");

  assertExactObject(
    report.runtime,
    [
      "codexVersion",
      "codexBinarySha256",
      "binaryExecution",
      "sourceAnalysisCommit",
      "platform",
      "launcherArch",
    ],
    "runtime evidence",
  );
  assert.match(
    report.runtime.codexVersion,
    /^codex-cli [0-9]+\.[0-9]+\.[0-9]+$/,
  );
  assert.match(report.runtime.codexBinarySha256, /^[0-9a-f]{64}$/);
  assert.equal(report.runtime.binaryExecution, "private-read-only-copy");
  assert.match(report.runtime.sourceAnalysisCommit, /^[0-9a-f]{40}$/);
  assert(["darwin", "linux"].includes(report.runtime.platform), "unsupported evidence platform");
  assert(
    NODE_ARCHITECTURES.has(report.runtime.launcherArch),
    "runtime launcherArch is not a recognized Node architecture",
  );

  assertExactObject(
    report.backend,
    [
      "type",
      "realModelTurnConfigured",
      "credentialInputProvisioned",
      "outboundNetworkIsolated",
    ],
    "backend evidence",
  );
  assert.equal(report.backend.type, "loopback-held-responses-mock");
  assert.equal(report.backend.realModelTurnConfigured, false);
  assert.equal(report.backend.credentialInputProvisioned, false);
  assert.equal(report.backend.outboundNetworkIsolated, false);

  assertExactObject(
    report.snapshot,
    [
      "kind",
      "appServerWorkspaceMatched",
      "historicalWorkspaceRetained",
      "modeledTreeDigestMatched",
      "sourceQuiesced",
      "workspaceModeledDigestMatched",
    ],
    "snapshot evidence",
  );
  assert.deepEqual(report.snapshot, {
    kind: "stopped-tree-copy",
    appServerWorkspaceMatched: true,
    historicalWorkspaceRetained: true,
    modeledTreeDigestMatched: true,
    sourceQuiesced: true,
    workspaceModeledDigestMatched: true,
  });

  assert(Array.isArray(report.scenarios), "scenario evidence must be an array");
  assert.deepEqual(
    report.scenarios.map((scenario) => scenario?.kind),
    RECOVERY_SCENARIOS,
  );
  for (const scenario of report.scenarios) {
    assertExactObject(
      scenario,
      [
        "kind",
        "turnMaterialized",
        "terminationObserved",
        "originalCompletionObserved",
        "resumeSucceeded",
        "sameThreadId",
        "tailTurnStatus",
        "threadReadAgrees",
        "threadReadIsolation",
        "modelAbortMarker",
      ],
      `${scenario.kind} scenario evidence`,
    );
    const explicit = scenario.kind === "logical_interrupt";
    const expectedTermination = explicit
      ? "rpc-interrupt"
      : scenario.kind === "sigterm"
        ? "SIGTERM"
        : "SIGKILL";
    assert.equal(scenario.turnMaterialized, true);
    assert.equal(scenario.terminationObserved, expectedTermination);
    assert.equal(scenario.originalCompletionObserved, explicit);
    assert.equal(scenario.resumeSucceeded, true);
    assert.equal(scenario.sameThreadId, true);
    assert.equal(scenario.tailTurnStatus, "interrupted");
    assert.equal(scenario.threadReadAgrees, true);
    assert.equal(
      scenario.threadReadIsolation,
      "copy-original-path-absent-held-tree-000",
    );
    assert.equal(scenario.modelAbortMarker, explicit ? "present" : "absent");
  }
}

function collectEvidenceStringSurfaces(...roots) {
  const pending = roots.filter((value) => typeof value === "string");
  const surfaces = [];
  const seen = new Set();
  while (pending.length > 0) {
    const surface = pending.shift();
    if (seen.has(surface)) continue;
    assert(
      surfaces.length < MAX_EVIDENCE_STRING_SURFACES,
      "recovery evidence contains excessive nested string data",
    );
    seen.add(surface);
    surfaces.push(surface);
    for (const token of surface.matchAll(JSON_STRING_TOKEN_PATTERN)) {
      pending.push(JSON.parse(token[0]));
    }
  }
  return surfaces;
}

function assertNoDuplicateJsonObjectKeys(serialized) {
  let index = 0;
  const skipWhitespace = () => {
    while (/\s/.test(serialized[index] ?? "")) index += 1;
  };
  const parseString = () => {
    JSON_STRING_AT_PATTERN.lastIndex = index;
    const match = JSON_STRING_AT_PATTERN.exec(serialized);
    assert(match, "recovery evidence contains invalid JSON string syntax");
    index = JSON_STRING_AT_PATTERN.lastIndex;
    return JSON.parse(match[0]);
  };
  const parseValue = () => {
    skipWhitespace();
    if (serialized[index] === "{") {
      index += 1;
      skipWhitespace();
      const keys = new Set();
      if (serialized[index] === "}") {
        index += 1;
        return;
      }
      while (true) {
        skipWhitespace();
        const key = parseString();
        assert(!keys.has(key), "recovery evidence contains duplicate object keys");
        keys.add(key);
        skipWhitespace();
        assert.equal(
          serialized[index],
          ":",
          "recovery evidence contains invalid JSON object syntax",
        );
        index += 1;
        parseValue();
        skipWhitespace();
        if (serialized[index] === "}") {
          index += 1;
          return;
        }
        assert.equal(
          serialized[index],
          ",",
          "recovery evidence contains invalid JSON object syntax",
        );
        index += 1;
      }
    }
    if (serialized[index] === "[") {
      index += 1;
      skipWhitespace();
      if (serialized[index] === "]") {
        index += 1;
        return;
      }
      while (true) {
        parseValue();
        skipWhitespace();
        if (serialized[index] === "]") {
          index += 1;
          return;
        }
        assert.equal(serialized[index], ",", "recovery evidence contains invalid JSON array syntax");
        index += 1;
      }
    }
    if (serialized[index] === '"') {
      parseString();
      return;
    }
    JSON_PRIMITIVE_AT_PATTERN.lastIndex = index;
    const match = JSON_PRIMITIVE_AT_PATTERN.exec(serialized);
    assert(match, "recovery evidence contains invalid JSON value syntax");
    index = JSON_PRIMITIVE_AT_PATTERN.lastIndex;
  };

  parseValue();
  skipWhitespace();
  assert.equal(index, serialized.length, "recovery evidence contains trailing JSON data");
}

export function assertRecoveryEvidenceSafe(report) {
  const serialized = typeof report === "string" ? report : JSON.stringify(report);
  if (typeof report === "string") assertNoDuplicateJsonObjectKeys(serialized);
  const structured = typeof report === "string" ? JSON.parse(report) : report;
  const normalized = JSON.stringify(structured);
  const surfaces = collectEvidenceStringSurfaces(serialized, normalized);
  for (const pattern of SENSITIVE_EVIDENCE_PATTERNS) {
    assert(
      surfaces.every((surface) => !pattern.test(surface)),
      "recovery evidence contains disallowed runtime data",
    );
  }
  assertRecoveryEvidenceSchema(structured);
  return serialized;
}

function evidenceDirectoryPermissionsAreSafe(metadata, currentUid) {
  return authorityDirectoryPermissionsAreSafe(
    {
      isDirectory: metadata.isDirectory(),
      mode: metadata.mode,
      uid: metadata.uid,
    },
    {
      brokerUid: currentUid,
      disallowedModeBits: 0o022,
      requiredModeBits: 0o700,
    },
  );
}

function evidenceAncestorPermissionsAreSafe(metadata, childUid, currentUid) {
  return authorityDirectoryPermissionsAreSafe(
    {
      isDirectory: metadata.isDirectory(),
      mode: metadata.mode,
      uid: metadata.uid,
    },
    {
      allowRootOwner: true,
      allowStickyShared: true,
      brokerUid: currentUid,
      childUid,
      disallowedModeBits: 0o022,
    },
  );
}

async function inspectEvidenceAcl(inspector, path, message) {
  let unsafe;
  try {
    unsafe = await inspector(path);
  } catch {
    throw new Error(message);
  }
  assert.equal(unsafe, false, message);
}

async function openEvidenceDirectoryAuthority(
  path,
  {
    inspectAncestorAcl = recoveryPathHasUnsafeAncestorAcl,
    inspectDirectoryAcl = recoveryPathHasExtendedAcl,
  } = {},
) {
  const currentUid = process.geteuid?.() ?? process.getuid?.();
  assert.notEqual(currentUid, undefined, "evidence writing requires a POSIX owner identity");
  const requestedPath = resolve(path);
  const requestedMetadata = await lstat(requestedPath, { bigint: true });
  assert(
    requestedMetadata.isDirectory() && !requestedMetadata.isSymbolicLink(),
    "evidence directory must be a pre-existing real directory",
  );
  const canonicalPath = await realpath(requestedPath);
  const identity = await lstat(canonicalPath, { bigint: true });
  assert(
    sameFileIdentity(requestedMetadata, identity),
    "evidence directory identity changed during validation",
  );
  assert(
    evidenceDirectoryPermissionsAreSafe(identity, currentUid),
    "evidence directory must be current-user-owned and not writable by other users",
  );
  const handle = await open(
    canonicalPath,
    fsConstants.O_RDONLY | fsConstants.O_DIRECTORY | fsConstants.O_NOFOLLOW,
  );
  const ancestors = [];
  let primaryFailure;
  try {
    const heldIdentity = await handle.stat({ bigint: true });
    assert(
      heldIdentity.isDirectory() && sameFileIdentity(identity, heldIdentity),
      "evidence directory identity changed during validation",
    );
    await inspectEvidenceAcl(
      inspectDirectoryAcl,
      canonicalPath,
      "evidence directory ACL could not be trusted",
    );

    let childUid = identity.uid;
    let ancestorPath = dirname(canonicalPath);
    while (true) {
      const ancestorIdentity = await lstat(ancestorPath, { bigint: true });
      assert(
        evidenceAncestorPermissionsAreSafe(ancestorIdentity, childUid, currentUid),
        "evidence directory ancestor chain is not trusted",
      );
      await inspectEvidenceAcl(
        inspectAncestorAcl,
        ancestorPath,
        "evidence directory ancestor ACL could not be trusted",
      );
      ancestors.push({ identity: ancestorIdentity, path: ancestorPath });
      const parent = dirname(ancestorPath);
      if (parent === ancestorPath) break;
      childUid = ancestorIdentity.uid;
      ancestorPath = parent;
    }

    const authority = {
      ancestors,
      currentUid,
      handle,
      identity,
      inspectAncestorAcl,
      inspectDirectoryAcl,
      path: canonicalPath,
    };
    authority.assertCurrent = async () => {
      const [current, held] = await Promise.all([
        lstat(authority.path, { bigint: true }),
        authority.handle.stat({ bigint: true }),
      ]);
      assert(
        current.isDirectory() &&
          sameFileIdentity(current, authority.identity) &&
          sameFileIdentity(held, authority.identity) &&
          evidenceDirectoryPermissionsAreSafe(current, authority.currentUid),
        "evidence directory identity or permissions changed",
      );
      await inspectEvidenceAcl(
        authority.inspectDirectoryAcl,
        authority.path,
        "evidence directory ACL could not be trusted",
      );
      let currentChildUid = current.uid;
      for (const ancestor of authority.ancestors) {
        const currentAncestor = await lstat(ancestor.path, { bigint: true });
        assert(
          sameFileIdentity(currentAncestor, ancestor.identity) &&
            evidenceAncestorPermissionsAreSafe(
              currentAncestor,
              currentChildUid,
              authority.currentUid,
            ),
          "evidence directory ancestor identity or permissions changed",
        );
        await inspectEvidenceAcl(
          authority.inspectAncestorAcl,
          ancestor.path,
          "evidence directory ancestor ACL could not be trusted",
        );
        currentChildUid = currentAncestor.uid;
      }
    };
    await authority.assertCurrent();
    return authority;
  } catch (error) {
    primaryFailure = { error };
    throw error;
  } finally {
    if (primaryFailure) await runSequentialCleanup([() => handle.close()], primaryFailure);
  }
}

async function openTemporaryEvidenceDirectory(path, currentUid) {
  const identity = await lstat(path, { bigint: true });
  assert(
    identity.isDirectory() &&
      identity.uid === BigInt(currentUid) &&
      Number(identity.mode & 0o777n) === 0o700,
    "temporary evidence directory is not private",
  );
  const handle = await open(
    path,
    fsConstants.O_RDONLY | fsConstants.O_DIRECTORY | fsConstants.O_NOFOLLOW,
  );
  let primaryFailure;
  try {
    const heldIdentity = await handle.stat({ bigint: true });
    assert(
      heldIdentity.isDirectory() && sameFileIdentity(identity, heldIdentity),
      "temporary evidence directory identity changed",
    );
    return {
      assertCurrent: async () => {
        const [current, held] = await Promise.all([
          lstat(path, { bigint: true }),
          handle.stat({ bigint: true }),
        ]);
        assert(
          current.isDirectory() &&
            sameFileIdentity(current, identity) &&
            sameFileIdentity(held, identity) &&
            current.uid === BigInt(currentUid) &&
            Number(current.mode & 0o777n) === 0o700,
          "temporary evidence directory identity or permissions changed",
        );
      },
      handle,
      identity,
      path,
    };
  } catch (error) {
    primaryFailure = { error };
    throw error;
  } finally {
    if (primaryFailure) await runSequentialCleanup([() => handle.close()], primaryFailure);
  }
}

function assertPrivateEvidenceFile(metadata, currentUid, message) {
  assert(
    metadata.isFile() &&
      metadata.nlink === 1n &&
      metadata.uid === BigInt(currentUid) &&
      Number(metadata.mode & 0o777n) === 0o600,
    message,
  );
}

async function assertEvidenceFileCurrent(path, identity, currentUid, message) {
  const current = await lstat(path, { bigint: true });
  assert(sameFileIdentity(current, identity), message);
  assertPrivateEvidenceFile(current, currentUid, message);
}

export async function writeRecoveryEvidence(
  path,
  report,
  {
    afterEvidenceDirectoryOpened = async () => {},
    afterEvidenceRename = async () => {},
    afterTemporaryDirectoryOpened = async () => {},
    beforeEvidenceRename = async () => {},
    inspectEvidenceAncestorAcl = recoveryPathHasUnsafeAncestorAcl,
    inspectEvidenceDirectoryAcl = recoveryPathHasExtendedAcl,
    openEvidenceFile = open,
    syncDirectory = async (handle) => handle.sync(),
  } = {},
) {
  const serialized = `${JSON.stringify(report, null, 2)}\n`;
  assertRecoveryEvidenceSafe(serialized);
  const evidenceName = basename(path);
  assert(evidenceName !== "." && evidenceName !== "..", "invalid evidence filename");
  let authority;
  let durabilityConfirmed = false;
  let renamed = false;
  let temporaryDirectory;
  let temporaryDirectoryAuthority;
  let primaryFailure;
  try {
    authority = await openEvidenceDirectoryAuthority(dirname(path), {
      inspectAncestorAcl: inspectEvidenceAncestorAcl,
      inspectDirectoryAcl: inspectEvidenceDirectoryAcl,
    });
    const evidencePath = join(authority.path, evidenceName);
    await afterEvidenceDirectoryOpened({ authority, evidencePath });
    await authority.assertCurrent();
    temporaryDirectory = await mkdtemp(
      join(authority.path, `.${evidenceName}.tmp-${process.pid}-`),
    );
    await chmod(temporaryDirectory, 0o700);
    temporaryDirectoryAuthority = await openTemporaryEvidenceDirectory(
      temporaryDirectory,
      authority.currentUid,
    );
    await afterTemporaryDirectoryOpened({ authority, temporaryDirectoryAuthority });
    await temporaryDirectoryAuthority.assertCurrent();
    await authority.assertCurrent();

    const temporaryPath = join(temporaryDirectory, "evidence.json");
    const file = await openEvidenceFile(
      temporaryPath,
      fsConstants.O_WRONLY |
        fsConstants.O_CREAT |
        fsConstants.O_EXCL |
        fsConstants.O_NOFOLLOW,
      0o600,
    );
    let fileIdentity;
    let fileFailure;
    try {
      await file.chmod(0o600);
      await file.writeFile(serialized);
      await file.sync();
      fileIdentity = await file.stat({ bigint: true });
      assertPrivateEvidenceFile(
        fileIdentity,
        authority.currentUid,
        "temporary evidence file is not private",
      );
    } catch (error) {
      fileFailure = { error };
      throw error;
    } finally {
      await runSequentialCleanup([() => file.close()], fileFailure);
    }

    await temporaryDirectoryAuthority.assertCurrent();
    await authority.assertCurrent();
    await assertEvidenceFileCurrent(
      temporaryPath,
      fileIdentity,
      authority.currentUid,
      "temporary evidence file identity or permissions changed",
    );
    await beforeEvidenceRename({ authority, evidencePath, temporaryPath });
    await authority.assertCurrent();
    await temporaryDirectoryAuthority.assertCurrent();
    await assertEvidenceFileCurrent(
      temporaryPath,
      fileIdentity,
      authority.currentUid,
      "temporary evidence file identity or permissions changed",
    );
    await rename(temporaryPath, evidencePath);
    renamed = true;
    await afterEvidenceRename({ authority, evidencePath, fileIdentity });
    await authority.assertCurrent();
    await assertEvidenceFileCurrent(
      evidencePath,
      fileIdentity,
      authority.currentUid,
      "published evidence file identity or permissions changed",
    );
    await authority.assertCurrent();
    await temporaryDirectoryAuthority.assertCurrent();
    await rmdir(temporaryDirectory);
    temporaryDirectory = undefined;
    await syncDirectory(authority.handle, authority.path);
    durabilityConfirmed = true;
    await authority.assertCurrent();
  } catch (error) {
    let failure = error;
    if (renamed && !durabilityConfirmed && error?.code !== "evidence_durability_uncertain") {
      failure = new Error("evidence publication durability is uncertain", { cause: error });
      failure.code = "evidence_durability_uncertain";
    }
    primaryFailure = { error: failure };
    throw failure;
  } finally {
    await runSequentialCleanup(
      [
        () => temporaryDirectoryAuthority?.handle.close(),
        () => authority?.handle.close(),
      ],
      primaryFailure,
    );
  }
}

async function assertPrivateBinaryIntegrity(path, expectedDigest) {
  const metadata = await lstat(path);
  assert(metadata.isFile(), "private CODEX_BIN copy must be a regular file");
  assert.equal(metadata.nlink, 1, "private CODEX_BIN copy must not be hard linked");
  assert.equal(metadata.mode & 0o777, 0o500, "private CODEX_BIN copy must remain mode 0500");
  assert.equal(
    await digestFile(path),
    expectedDigest,
    "private CODEX_BIN changed during the recovery probe",
  );
}

async function assertTrustedExecutableRoot(
  path,
  identity,
  {
    currentUid,
    inspectAncestorAcl = recoveryPathHasUnsafeAncestorAcl,
    inspectRootAcl = recoveryPathHasExtendedAcl,
  },
) {
  const safeDirectory = (metadata, childUid) =>
    authorityDirectoryPermissionsAreSafe(
      {
        isDirectory: metadata.isDirectory(),
        mode: metadata.mode,
        uid: metadata.uid,
      },
      {
        allowRootOwner: true,
        allowStickyShared: true,
        brokerUid: currentUid,
        childUid,
        disallowedModeBits: 0o022,
      },
    );

  assert(
    safeDirectory(identity, currentUid),
    "recovery executable root must have trusted ownership and permissions",
  );
  let rootHasExtendedAcl;
  try {
    rootHasExtendedAcl = await inspectRootAcl(path);
  } catch {
    throw new Error("recovery executable root ACL could not be validated");
  }
  assert.equal(
    rootHasExtendedAcl,
    false,
    "recovery executable root must not have extended access controls",
  );

  let childUid = identity.uid;
  let ancestor = dirname(path);
  while (true) {
    let metadata;
    try {
      metadata = await lstat(ancestor);
    } catch {
      throw new Error("recovery executable root ancestor chain could not be validated");
    }
    assert(
      safeDirectory(metadata, childUid),
      "recovery executable root ancestor chain is not trusted",
    );
    let ancestorHasUnsafeAcl;
    try {
      ancestorHasUnsafeAcl = await inspectAncestorAcl(ancestor);
    } catch {
      throw new Error("recovery executable root ancestor ACL could not be validated");
    }
    assert.equal(
      ancestorHasUnsafeAcl,
      false,
      "recovery executable root ancestor chain has unsafe access controls",
    );
    const parent = dirname(ancestor);
    if (parent === ancestor) break;
    childUid = metadata.uid;
    ancestor = parent;
  }
}

export async function probeInterruptedTurnRecovery({
  codexBin = process.env.CODEX_BIN,
  evidencePath,
  executableRoot = process.env.CODEX_RECOVERY_EXEC_ROOT ?? tmpdir(),
  inspectExecutableAncestorAcl = recoveryPathHasUnsafeAncestorAcl,
  inspectExecutableRootAcl = recoveryPathHasExtendedAcl,
  readCodexVersion = codexVersion,
  runScenario = runRecoveryScenario,
  scenarios = RECOVERY_SCENARIOS,
  sourceAnalysisCommit = PINNED_SOURCE_ANALYSIS_COMMIT,
  writeEvidence = false,
} = {}) {
  if (typeof codexBin !== "string" || !isAbsolute(codexBin)) {
    throw new Error("CODEX_BIN must be an absolute pinned-image path");
  }
  if (typeof executableRoot !== "string" || !isAbsolute(executableRoot)) {
    throw new Error("recovery executable root must be an absolute path");
  }
  const executable = resolveAppServerExecutable(codexBin);
  const binary = await realpath(executable);
  const binaryMetadata = await stat(binary);
  assert(binaryMetadata.isFile(), "CODEX_BIN must resolve to a regular file");
  const canonicalExecutableRoot = await realpath(executableRoot);
  const executableRootMetadata = await stat(canonicalExecutableRoot);
  assert(executableRootMetadata.isDirectory(), "recovery executable root must be a directory");
  const currentUid = process.geteuid?.() ?? process.getuid?.();
  assert.notEqual(currentUid, undefined, "recovery executable root requires a POSIX host");
  await assertTrustedExecutableRoot(canonicalExecutableRoot, executableRootMetadata, {
    currentUid,
    inspectAncestorAcl: inspectExecutableAncestorAcl,
    inspectRootAcl: inspectExecutableRootAcl,
  });
  assert.deepEqual([...scenarios], RECOVERY_SCENARIOS, "the evidence probe requires all scenarios");
  let binaryRoot;
  let primaryFailure;
  try {
    binaryRoot = await mkdtemp(join(canonicalExecutableRoot, "portable-codex-binary-"));
    await chmod(binaryRoot, 0o700);
    await assertTrustedExecutableRoot(binaryRoot, await stat(binaryRoot), {
      currentUid,
      inspectAncestorAcl: inspectExecutableAncestorAcl,
      inspectRootAcl: inspectExecutableRootAcl,
    });
    const privateBinary = join(binaryRoot, "codex");
    await copyFile(binary, privateBinary);
    await chmod(privateBinary, 0o500);
    const binaryDigest = await digestFile(privateBinary);
    await assertPrivateBinaryIntegrity(privateBinary, binaryDigest);
    const versionHome = join(binaryRoot, "version-home");
    await mkdir(versionHome, { mode: 0o700 });
    await chmod(versionHome, 0o700);
    const binaryVersion = readCodexVersion(privateBinary, {
      cwd: versionHome,
      env: buildWorkerEnvironment(versionHome, process.env, versionHome),
    });

    const scenarioReports = [];
    for (const kind of scenarios) {
      await assertPrivateBinaryIntegrity(privateBinary, binaryDigest);
      scenarioReports.push(
        await runScenario({ codexBin: privateBinary, kind, temporaryRoot: binaryRoot }),
      );
    }
    await assertPrivateBinaryIntegrity(privateBinary, binaryDigest);
    const snapshotScenario = scenarioReports.find(
      (scenario) => scenario.kind === "snapshot_restore",
    );
    const report = {
      schemaVersion: 5,
      probe: "interrupted-turn-recovery",
      runtime: {
        codexVersion: binaryVersion,
        codexBinarySha256: binaryDigest,
        binaryExecution: "private-read-only-copy",
        sourceAnalysisCommit,
        platform: platform(),
        launcherArch: arch(),
      },
      backend: {
        type: "loopback-held-responses-mock",
        realModelTurnConfigured: false,
        credentialInputProvisioned: false,
        outboundNetworkIsolated: false,
      },
      snapshot: snapshotScenario.snapshot,
      scenarios: scenarioReports.map(({ snapshot: _snapshot, ...scenario }) => scenario),
      result: "passed",
    };
    assertRecoveryEvidenceSafe(report);
    if (writeEvidence) {
      assert(evidencePath, "evidencePath is required when writeEvidence is enabled");
      await writeRecoveryEvidence(evidencePath, report);
    }
    return report;
  } catch (error) {
    primaryFailure = { error };
    throw error;
  } finally {
    if (binaryRoot) {
      await runSequentialCleanup(
        [() => removeTreeForCleanup(binaryRoot)],
        primaryFailure,
      );
    }
  }
}

export function interruptedTurnRecoveryFailureReport(error) {
  let code = "recovery_probe_failed";
  try {
    const codeDescriptor =
      error && (typeof error === "object" || typeof error === "function")
        ? Object.getOwnPropertyDescriptor(error, "code")
        : undefined;
    if (codeDescriptor?.value === "evidence_durability_uncertain") {
      code = "evidence_durability_uncertain";
    }
  } catch {
    // A hostile error object must not escape the structured redaction boundary.
  }
  return {
    error: { code, retryable: false, type: "probe_failure" },
    result: "failed",
  };
}

export async function runInterruptedTurnRecoveryCli({
  args = process.argv.slice(2),
  cwd = process.cwd(),
  env = process.env,
  probe = probeInterruptedTurnRecovery,
  stderr = process.stderr,
  stdout = process.stdout,
} = {}) {
  const writeEvidence = args.includes("--write-evidence");
  const evidencePath = resolve(
    cwd,
    env.CODEX_RECOVERY_EVIDENCE ?? "evidence/interrupted-turn-recovery.json",
  );
  try {
    const report = await probe({
      codexBin: env.CODEX_BIN,
      evidencePath,
      writeEvidence,
    });
    stdout.write(`${JSON.stringify(report, null, 2)}\n`);
    return 0;
  } catch (error) {
    stderr.write(`${JSON.stringify(interruptedTurnRecoveryFailureReport(error))}\n`);
    return 1;
  }
}
