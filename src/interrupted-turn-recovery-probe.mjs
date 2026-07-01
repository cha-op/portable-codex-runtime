import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import { once } from "node:events";
import { createReadStream } from "node:fs";
import {
  chmod,
  copyFile,
  lstat,
  mkdir,
  mkdtemp,
  open,
  readlink,
  readdir,
  readFile,
  realpath,
  rename,
  rm,
  stat,
  symlink,
  writeFile,
} from "node:fs/promises";
import { createServer } from "node:http";
import { arch, platform, tmpdir } from "node:os";
import { dirname, isAbsolute, join, relative, resolve } from "node:path";

import {
  AppServerClient,
  codexVersion,
  resolveAppServerExecutable,
  runSequentialCleanup,
} from "./app-server-auth-probe.mjs";

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
  /(?:^|["'\s])\/(?:Users|home|private|tmp|var\/folders)\//,
  /\b[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\b/i,
  /eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]*/,
  /\b(?:sk|sess|rk)-[A-Za-z0-9_-]{8,}\b/,
  /<turn_aborted>/,
  /portable recovery probe/i,
];

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
  await writeFile(join(codexHome, "config.toml"), config, { mode: 0o600 });
}

export function assertProcessGroupTarget(pid, currentPid = process.pid) {
  if (!Number.isSafeInteger(pid) || pid <= 1 || pid === currentPid) {
    throw new Error("refusing unsafe app-server process-group target");
  }
  return pid;
}

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
    killProcess(-pid, signal);
    exitResult = await withTimeout(client.exitPromise, timeoutMs, `${signal} app-server exit`);
    assert.equal(exitResult[0], null, `app-server exited with code ${exitResult[0]}`);
    assert.equal(exitResult[1], signal, `app-server observed ${exitResult[1]} instead of ${signal}`);
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

async function assertDirectOwnedPath(ownedRoot, candidate, label, { mustExist }) {
  const canonicalRoot = await realpath(ownedRoot);
  const canonicalParent = await realpath(dirname(resolve(candidate)));
  assert.equal(canonicalParent, canonicalRoot, `${label} must be a direct owned child`);
  try {
    const metadata = await lstat(candidate);
    if (!mustExist) throw new Error(`${label} already exists`);
    assert(metadata.isDirectory(), `${label} must be a directory`);
    assert(!metadata.isSymbolicLink(), `${label} must not be a symlink`);
  } catch (error) {
    if (!mustExist && error?.code === "ENOENT") return;
    throw error;
  }
}

function pathIsInside(root, candidate) {
  const child = relative(root, candidate);
  return child === "" || (!child.startsWith("..") && !isAbsolute(child));
}

async function copyTreeEntry(sourceRoot, source, destination) {
  const metadata = await lstat(source);
  if (metadata.isSymbolicLink()) {
    const target = await readlink(source);
    if (isAbsolute(target) && pathIsInside(sourceRoot, resolve(target))) {
      throw new Error("stopped-tree copy rejects absolute symlinks into the source tree");
    }
    await symlink(target, destination);
    return;
  }
  if (metadata.isDirectory()) {
    await mkdir(destination, { mode: metadata.mode & 0o777 });
    const entries = await readdir(source);
    entries.sort();
    for (const entry of entries) {
      await copyTreeEntry(sourceRoot, join(source, entry), join(destination, entry));
    }
    await chmod(destination, metadata.mode & 0o777);
    return;
  }
  if (!metadata.isFile()) {
    throw new Error("stopped-tree copy rejects sockets, devices, and FIFOs");
  }
  await copyFile(source, destination);
  await chmod(destination, metadata.mode & 0o777);
}

export async function copyStoppedTree({ ownedRoot, source, destination }) {
  await assertDirectOwnedPath(ownedRoot, source, "source", { mustExist: true });
  await assertDirectOwnedPath(ownedRoot, destination, "destination", { mustExist: false });
  try {
    await copyTreeEntry(resolve(source), source, destination);
  } catch (error) {
    await rm(destination, { recursive: true, force: true }).catch(() => {});
    throw error;
  }
}

async function updateHashFromFile(hash, path) {
  const input = createReadStream(path);
  input.on("data", (chunk) => hash.update(chunk));
  // events.once rejects if the stream emits "error" before "end".
  await once(input, "end");
}

async function hashTreeEntry(hash, root, path) {
  const metadata = await lstat(path);
  const entryPath = relative(root, path) || ".";
  if (metadata.isSymbolicLink()) {
    hash.update(`symlink\0${entryPath}\0${await readlink(path)}\0`);
    return;
  }
  if (metadata.isDirectory()) {
    hash.update(`directory\0${entryPath}\0${metadata.mode & 0o777}\0`);
    const entries = await readdir(path);
    entries.sort();
    for (const entry of entries) await hashTreeEntry(hash, root, join(path, entry));
    return;
  }
  if (!metadata.isFile()) throw new Error("tree digest rejects non-file entries");
  hash.update(`file\0${entryPath}\0${metadata.mode & 0o777}\0${metadata.size}\0`);
  await updateHashFromFile(hash, path);
  hash.update("\0");
}

export async function digestTree(root) {
  const hash = createHash("sha256");
  await hashTreeEntry(hash, root, root);
  return hash.digest("hex");
}

async function digestFile(path) {
  const hash = createHash("sha256");
  await updateHashFromFile(hash, path);
  return hash.digest("hex");
}

function findTurn(thread, turnId) {
  assert(Array.isArray(thread?.turns), "thread response did not include turns");
  const turn = thread.turns.find((candidate) => candidate.id === turnId);
  assert(turn, "recovered thread omitted the interrupted turn");
  return turn;
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
  mock,
  threadId,
  turnId,
  workspace,
  expectAbortMarker,
}) {
  const resumed = await client.request("thread/resume", { threadId, cwd: workspace });
  assert.equal(resumed.thread.id, threadId);
  const resumedTurn = findTurn(resumed.thread, turnId);
  assert.equal(resumedTurn.status, "interrupted");

  const read = await client.request("thread/read", { threadId, includeTurns: true });
  assert.equal(read.thread.id, threadId);
  const readTurn = findTurn(read.thread, turnId);
  assert.equal(readTurn.status, "interrupted");

  await client.request("turn/start", {
    threadId,
    input: [{ type: "text", text: "Complete the portable recovery probe.", textElements: [] }],
  });
  const completed = await client.waitForNotification("turn/completed");
  assert.equal(completed.params.turn.status, "completed");
  await mock.waitForRequest(2);
  const markerPresent = mock.requestBody(1).includes("<turn_aborted>");
  assert.equal(markerPresent, expectAbortMarker);

  return {
    resumeSucceeded: true,
    sameThreadId: true,
    tailTurnStatus: resumedTurn.status,
    threadReadAgrees: readTurn.status === resumedTurn.status,
    modelAbortMarker: markerPresent ? "present" : "absent",
  };
}

export async function runRecoveryScenario({
  codexBin,
  kind,
  makeTemporaryDirectory = mkdtemp,
  startMock = startHeldResponsesMock,
  timeoutMs = DEFAULT_TIMEOUT_MS,
}) {
  if (!RECOVERY_SCENARIOS.includes(kind)) throw new Error(`unknown recovery scenario: ${kind}`);
  const ownedRoot = await makeTemporaryDirectory(join(tmpdir(), "portable-codex-recovery-"));
  await chmod(ownedRoot, 0o700);
  let client;
  let recoveredClient;
  let mock;
  let primaryFailure;
  try {
    let sessionRoot = join(ownedRoot, "session");
    let codexHome = join(sessionRoot, "codex-home");
    let workspace = join(sessionRoot, "workspace");
    await mkdir(codexHome, { recursive: true, mode: 0o700 });
    await mkdir(workspace, { mode: 0o700 });
    await writeFile(join(workspace, "sentinel.txt"), "portable-recovery-sentinel\n", {
      mode: 0o600,
    });
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
      const termination = await terminateAppServer(client, signal, { timeoutMs: 5_000 });
      terminationObserved = termination.signal;
    }

    if (kind === "snapshot_restore") {
      const sourceDigest = await digestTree(sessionRoot);
      const sourceWorkspaceDigest = await digestTree(workspace);
      const backupRoot = join(ownedRoot, "stopped-tree-copy");
      await copyStoppedTree({ ownedRoot, source: sessionRoot, destination: backupRoot });
      const backupDigest = await digestTree(backupRoot);
      assert.equal(backupDigest, sourceDigest);
      assert.equal(await digestTree(join(backupRoot, "workspace")), sourceWorkspaceDigest);
      await rm(sessionRoot, { recursive: true });
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
        treeDigestMatched: true,
        workspaceDigestMatched: true,
      };
    }

    recoveredClient = await startRecoveryClient({ codexBin, codexHome, timeoutMs });
    const recovery = await recoverAndInspect({
      client: recoveredClient,
      mock,
      threadId,
      turnId,
      workspace,
      expectAbortMarker,
    });
    await recoveredClient.stop();

    return {
      kind,
      turnMaterialized: true,
      terminationObserved,
      originalCompletionObserved,
      ...recovery,
      ...(snapshot ? { snapshot } : {}),
    };
  } catch (error) {
    primaryFailure = { error };
    throw error;
  } finally {
    await runSequentialCleanup(
      [
        () => recoveredClient?.abort(),
        () => client?.abort(),
        () => mock?.close(),
        () => rm(ownedRoot, { recursive: true, force: true }),
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
  assert.equal(report.schemaVersion, 1);
  assert.equal(report.probe, "interrupted-turn-recovery");
  assert.equal(report.result, "passed");

  assertExactObject(
    report.runtime,
    ["codexVersion", "codexBinarySha256", "sourceAnalysisCommit", "platform", "arch"],
    "runtime evidence",
  );
  assert.match(
    report.runtime.codexVersion,
    /^codex-cli [0-9]+\.[0-9]+\.[0-9]+(?:[-+][0-9A-Za-z.-]+)?$/,
  );
  assert.match(report.runtime.codexBinarySha256, /^[0-9a-f]{64}$/);
  assert.match(report.runtime.sourceAnalysisCommit, /^[0-9a-f]{40}$/);
  assert(["darwin", "linux"].includes(report.runtime.platform), "unsupported evidence platform");
  assert.match(report.runtime.arch, /^[0-9A-Za-z_-]+$/);

  assertExactObject(
    report.backend,
    ["type", "realModelTurn", "authMaterialUsed"],
    "backend evidence",
  );
  assert.equal(report.backend.type, "loopback-held-responses-mock");
  assert.equal(report.backend.realModelTurn, false);
  assert.equal(report.backend.authMaterialUsed, false);

  assertExactObject(
    report.snapshot,
    ["kind", "sourceQuiesced", "treeDigestMatched", "workspaceDigestMatched"],
    "snapshot evidence",
  );
  assert.deepEqual(report.snapshot, {
    kind: "stopped-tree-copy",
    sourceQuiesced: true,
    treeDigestMatched: true,
    workspaceDigestMatched: true,
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
    assert.equal(scenario.modelAbortMarker, explicit ? "present" : "absent");
  }
}

export function assertRecoveryEvidenceSafe(report) {
  const serialized = typeof report === "string" ? report : JSON.stringify(report);
  for (const pattern of SENSITIVE_EVIDENCE_PATTERNS) {
    assert(!pattern.test(serialized), "recovery evidence contains disallowed runtime data");
  }
  const structured = typeof report === "string" ? JSON.parse(report) : report;
  assertRecoveryEvidenceSchema(structured);
  return serialized;
}

export async function writeRecoveryEvidence(path, report) {
  const serialized = `${JSON.stringify(report, null, 2)}\n`;
  assertRecoveryEvidenceSafe(serialized);
  await mkdir(dirname(path), { recursive: true });
  const temporaryPath = `${path}.next-${process.pid}`;
  let promoted = false;
  try {
    const file = await open(temporaryPath, "wx", 0o600);
    try {
      await file.writeFile(serialized);
      await file.sync();
    } finally {
      await file.close();
    }
    await rename(temporaryPath, path);
    promoted = true;
  } finally {
    if (!promoted) await rm(temporaryPath, { force: true }).catch(() => {});
  }
}

export async function probeInterruptedTurnRecovery({
  codexBin = process.env.CODEX_BIN,
  evidencePath,
  readCodexVersion = codexVersion,
  runScenario = runRecoveryScenario,
  scenarios = RECOVERY_SCENARIOS,
  sourceAnalysisCommit = PINNED_SOURCE_ANALYSIS_COMMIT,
  writeEvidence = false,
} = {}) {
  if (typeof codexBin !== "string" || !isAbsolute(codexBin)) {
    throw new Error("CODEX_BIN must be an absolute pinned-image path");
  }
  const executable = resolveAppServerExecutable(codexBin);
  const binary = await realpath(executable);
  const binaryMetadata = await stat(binary);
  assert(binaryMetadata.isFile(), "CODEX_BIN must resolve to a regular file");
  assert.deepEqual([...scenarios], RECOVERY_SCENARIOS, "the evidence probe requires all scenarios");

  const scenarioReports = [];
  for (const kind of scenarios) {
    scenarioReports.push(await runScenario({ codexBin: binary, kind }));
  }
  const snapshotScenario = scenarioReports.find((scenario) => scenario.kind === "snapshot_restore");
  const report = {
    schemaVersion: 1,
    probe: "interrupted-turn-recovery",
    runtime: {
      codexVersion: readCodexVersion(binary),
      codexBinarySha256: await digestFile(binary),
      sourceAnalysisCommit,
      platform: platform(),
      arch: arch(),
    },
    backend: {
      type: "loopback-held-responses-mock",
      realModelTurn: false,
      authMaterialUsed: false,
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
}

export function interruptedTurnRecoveryFailureReport() {
  return {
    error: { code: "recovery_probe_failed", retryable: false, type: "probe_failure" },
    result: "failed",
  };
}
