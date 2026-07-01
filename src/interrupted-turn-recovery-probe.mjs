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
import { basename, dirname, isAbsolute, join, relative, resolve, sep } from "node:path";

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
  /(?:^|["'\s])\/[^"'\s]/,
  /\b[0-9a-f]{8}-[0-9a-f]{4}-[1-5][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}\b/i,
  /eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]*/,
  /\b(?:sk|sess|rk)-[A-Za-z0-9_-]{8,}\b/,
  /<turn_aborted>/,
  /portable recovery probe/i,
];
const JSON_STRING_TOKEN_PATTERN =
  /"(?:\\(?:["\\/bfnrt]|u[0-9a-fA-F]{4})|[^"\\\u0000-\u001F])*"/g;
const MAX_EVIDENCE_STRING_SURFACES = 1_024;

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
  const canonicalParent = await realpath(dirname(candidate));
  assert.equal(canonicalParent, canonicalRoot, `${label} must be a direct owned child`);
  const candidateName = basename(candidate);
  assert(
    candidateName !== "." && candidateName !== "..",
    `${label} must be a direct owned child`,
  );
  const canonicalCandidate = join(canonicalParent, candidateName);
  assert.equal(
    dirname(canonicalCandidate),
    canonicalRoot,
    `${label} must be a direct owned child`,
  );
  try {
    const metadata = await lstat(canonicalCandidate);
    if (!mustExist) throw new Error(`${label} already exists`);
    assert(metadata.isDirectory(), `${label} must be a directory`);
    assert(!metadata.isSymbolicLink(), `${label} must not be a symlink`);
  } catch (error) {
    if (!mustExist && error?.code === "ENOENT") return canonicalCandidate;
    throw error;
  }
  return canonicalCandidate;
}

function pathIsInside(root, candidate) {
  const child = relative(root, candidate);
  return child === "" || (child !== ".." && !child.startsWith(`..${sep}`) && !isAbsolute(child));
}

function portableMode(metadata) {
  if ((metadata.mode & 0o7000) !== 0) {
    throw new Error("portable tree rejects special permission bits");
  }
  return metadata.mode & 0o777;
}

async function readPortableSymlink(path) {
  const bytes = await readlink(path, { encoding: "buffer" });
  const target = bytes.toString("utf8");
  if (!Buffer.from(target, "utf8").equals(bytes)) {
    throw new Error("stopped-tree copy rejects non-UTF-8 symlink targets");
  }
  return { bytes, target };
}

export function decodePortablePathBytes(bytes) {
  assert(Buffer.isBuffer(bytes), "portable path bytes must be a Buffer");
  const value = bytes.toString("utf8");
  if (!Buffer.from(value, "utf8").equals(bytes)) {
    throw new Error("portable tree rejects non-UTF-8 directory entry names");
  }
  return value;
}

export function assertPortableDirectoryNames(entries) {
  const portableKeys = new Set();
  for (const entry of entries) {
    assert.equal(typeof entry, "string", "portable directory entries must be strings");
    const normalized = entry.normalize("NFC");
    for (const character of normalized) {
      if (
        character.codePointAt(0) > 0x7f &&
        (character.toLowerCase() !== character || character.toUpperCase() !== character)
      ) {
        throw new Error("portable tree rejects non-ASCII cased directory names");
      }
    }
    const portableKey = normalized.toLowerCase();
    if (portableKeys.has(portableKey)) {
      throw new Error(
        "portable tree rejects case or Unicode-normalization name collisions",
      );
    }
    portableKeys.add(portableKey);
  }
  return [...entries].sort();
}

async function readPortableDirectory(path) {
  const entries = await readdir(path, { encoding: "buffer" });
  return assertPortableDirectoryNames(entries.map(decodePortablePathBytes));
}

function rawChildPath(parent, entry) {
  const parentBytes = Buffer.isBuffer(parent) ? parent : Buffer.from(parent);
  return Buffer.concat([parentBytes, Buffer.from(sep), entry]);
}

async function assertPortableSymlink({
  destination,
  destinationRoots,
  source,
  sourceRoots,
  target,
}) {
  if (isAbsolute(target)) {
    let canonicalTarget;
    try {
      canonicalTarget = await realpath(target);
    } catch (error) {
      if (error?.code === "ENOENT") {
        throw new Error("stopped-tree copy rejects dangling absolute symlinks");
      }
      throw error;
    }
    const targetCandidates = [resolve(target), canonicalTarget];
    if (targetCandidates.some((candidate) =>
      sourceRoots.some((sourceRoot) => pathIsInside(sourceRoot, candidate)))) {
      throw new Error("stopped-tree copy rejects absolute symlinks into the source tree");
    }
    if (targetCandidates.some((candidate) =>
      destinationRoots.some((destinationRoot) => pathIsInside(destinationRoot, candidate)))) {
      throw new Error("stopped-tree copy rejects absolute symlinks into the destination tree");
    }
    return;
  }

  const sourceTarget = resolve(dirname(source), target);
  const destinationTarget = resolve(dirname(destination), target);
  const lexicalSourceRoot = sourceRoots[0];
  const lexicalDestinationRoot = destinationRoots[0];
  if (
    !pathIsInside(lexicalSourceRoot, sourceTarget) ||
    !pathIsInside(lexicalDestinationRoot, destinationTarget) ||
    relative(lexicalSourceRoot, sourceTarget) !==
      relative(lexicalDestinationRoot, destinationTarget)
  ) {
    throw new Error("stopped-tree copy rejects non-relocatable relative symlinks");
  }
}

async function copyTreeEntry(context, source, destination) {
  const metadata = await lstat(source);
  if (metadata.isSymbolicLink()) {
    const { target } = await readPortableSymlink(source);
    await assertPortableSymlink({ ...context, source, destination, target });
    await symlink(target, destination);
    return;
  }
  if (metadata.isDirectory()) {
    const finalMode = portableMode(metadata);
    await mkdir(destination, { mode: 0o700 });
    await chmod(destination, 0o700);
    const entries = await readPortableDirectory(source);
    for (const entry of entries) {
      await copyTreeEntry(context, join(source, entry), join(destination, entry));
    }
    await chmod(destination, finalMode);
    return;
  }
  if (!metadata.isFile()) {
    throw new Error("stopped-tree copy rejects sockets, devices, and FIFOs");
  }
  if (metadata.nlink !== 1) throw new Error("stopped-tree copy rejects hard-linked files");
  const finalMode = portableMode(metadata);
  await copyFile(source, destination);
  await chmod(destination, finalMode);
}

export async function removeTreeForCleanup(path) {
  let metadata;
  try {
    metadata = await lstat(path);
  } catch (error) {
    if (error?.code === "ENOENT") return;
    throw error;
  }
  if (metadata.isDirectory()) {
    await chmod(path, (metadata.mode & 0o777) | 0o700);
    const entries = await readdir(path, { encoding: "buffer" });
    for (const entry of entries) await removeTreeForCleanup(rawChildPath(path, entry));
  }
  await rm(path, { recursive: metadata.isDirectory(), force: true });
}

export async function copyStoppedTree({ ownedRoot, source, destination }) {
  const canonicalSource = await assertDirectOwnedPath(ownedRoot, source, "source", {
    mustExist: true,
  });
  const canonicalDestination = await assertDirectOwnedPath(
    ownedRoot,
    destination,
    "destination",
    { mustExist: false },
  );
  try {
    await copyTreeEntry(
      {
        destinationRoots: [
          resolve(canonicalDestination),
          join(
            await realpath(dirname(canonicalDestination)),
            basename(canonicalDestination),
          ),
        ],
        sourceRoots: [resolve(canonicalSource), await realpath(canonicalSource)],
      },
      canonicalSource,
      canonicalDestination,
    );
  } catch (error) {
    await removeTreeForCleanup(canonicalDestination).catch(() => {});
    throw error;
  }
}

async function updateHashFromFile(hash, path) {
  const input = createReadStream(path);
  input.on("data", (chunk) => hash.update(chunk));
  // events.once rejects if the stream emits "error" before "end".
  await once(input, "end");
}

function updateHashFields(hash, fields) {
  for (const field of fields) {
    const bytes = Buffer.isBuffer(field) ? field : Buffer.from(String(field));
    hash.update(`${bytes.length}:`);
    hash.update(bytes);
  }
}

async function hashTreeEntry(hash, root, path) {
  const metadata = await lstat(path);
  const entryPath = relative(root, path) || ".";
  if (metadata.isSymbolicLink()) {
    const { bytes } = await readPortableSymlink(path);
    updateHashFields(hash, ["symlink", entryPath, bytes]);
    return;
  }
  if (metadata.isDirectory()) {
    updateHashFields(hash, ["directory", entryPath, portableMode(metadata)]);
    const entries = await readPortableDirectory(path);
    for (const entry of entries) await hashTreeEntry(hash, root, join(path, entry));
    return;
  }
  if (!metadata.isFile()) throw new Error("tree digest rejects non-file entries");
  if (metadata.nlink !== 1) throw new Error("tree digest rejects hard-linked files");
  updateHashFields(hash, [
    "file",
    entryPath,
    portableMode(metadata),
    metadata.size,
    await digestFile(path),
  ]);
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
  timeoutMs = DEFAULT_TIMEOUT_MS,
}) {
  if (!RECOVERY_SCENARIOS.includes(kind)) throw new Error(`unknown recovery scenario: ${kind}`);
  const ownedRoot = await makeTemporaryDirectory(join(tmpdir(), "portable-codex-recovery-"));
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
        treeDigestMatched: true,
        workspaceDigestMatched: true,
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
  assert.equal(report.schemaVersion, 2);
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
      "arch",
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
    [
      "kind",
      "appServerWorkspaceMatched",
      "historicalWorkspaceRetained",
      "sourceQuiesced",
      "treeDigestMatched",
      "workspaceDigestMatched",
    ],
    "snapshot evidence",
  );
  assert.deepEqual(report.snapshot, {
    kind: "stopped-tree-copy",
    appServerWorkspaceMatched: true,
    historicalWorkspaceRetained: true,
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

export function assertRecoveryEvidenceSafe(report) {
  const serialized = typeof report === "string" ? report : JSON.stringify(report);
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

async function ensurePrivateEvidenceDirectory(path) {
  const missing = [];
  let cursor = resolve(path);
  while (true) {
    try {
      const metadata = await lstat(cursor);
      assert(metadata.isDirectory(), "evidence parent must be a directory");
      assert(!metadata.isSymbolicLink(), "evidence parent must not be a symlink");
      break;
    } catch (error) {
      if (error?.code !== "ENOENT") throw error;
      missing.push(cursor);
      const parent = dirname(cursor);
      assert.notEqual(parent, cursor, "evidence path has no existing directory ancestor");
      cursor = parent;
    }
  }

  let current = await realpath(cursor);
  for (const directory of missing.reverse()) {
    current = join(current, basename(directory));
    try {
      await mkdir(current, { mode: 0o700 });
    } catch (error) {
      if (error?.code !== "EEXIST") throw error;
      const metadata = await lstat(current);
      assert(metadata.isDirectory(), "evidence parent must be a directory");
      assert(!metadata.isSymbolicLink(), "evidence parent must not be a symlink");
    }
    await chmod(current, 0o700);
  }
  return current;
}

export async function writeRecoveryEvidence(path, report) {
  const serialized = `${JSON.stringify(report, null, 2)}\n`;
  assertRecoveryEvidenceSafe(serialized);
  const evidenceName = basename(path);
  assert(evidenceName !== "." && evidenceName !== "..", "invalid evidence filename");
  const evidenceDirectory = await ensurePrivateEvidenceDirectory(dirname(path));
  const evidencePath = join(evidenceDirectory, evidenceName);
  const temporaryDirectory = await mkdtemp(
    join(evidenceDirectory, `.${basename(path)}.tmp-${process.pid}-`),
  );
  const temporaryPath = join(temporaryDirectory, "evidence.json");
  try {
    await chmod(temporaryDirectory, 0o700);
    const file = await open(temporaryPath, "wx", 0o600);
    try {
      await file.chmod(0o600);
      await file.writeFile(serialized);
      await file.sync();
    } finally {
      await file.close();
    }
    await rename(temporaryPath, evidencePath);
  } finally {
    await rm(temporaryDirectory, { recursive: true, force: true }).catch(() => {});
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
  let binaryRoot;
  let primaryFailure;
  try {
    binaryRoot = await mkdtemp(join(tmpdir(), "portable-codex-binary-"));
    await chmod(binaryRoot, 0o700);
    const privateBinary = join(binaryRoot, "codex");
    await copyFile(binary, privateBinary);
    await chmod(privateBinary, 0o500);
    const privateMetadata = await lstat(privateBinary);
    assert(privateMetadata.isFile(), "private CODEX_BIN copy must be a regular file");
    assert.equal(privateMetadata.nlink, 1, "private CODEX_BIN copy must not be hard linked");
    const binaryDigest = await digestFile(privateBinary);
    const binaryVersion = readCodexVersion(privateBinary);

    const scenarioReports = [];
    for (const kind of scenarios) {
      scenarioReports.push(await runScenario({ codexBin: privateBinary, kind }));
    }
    assert.equal(
      await digestFile(privateBinary),
      binaryDigest,
      "private CODEX_BIN changed during the recovery probe",
    );
    const snapshotScenario = scenarioReports.find(
      (scenario) => scenario.kind === "snapshot_restore",
    );
    const report = {
      schemaVersion: 2,
      probe: "interrupted-turn-recovery",
      runtime: {
        codexVersion: binaryVersion,
        codexBinarySha256: binaryDigest,
        binaryExecution: "private-read-only-copy",
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

export function interruptedTurnRecoveryFailureReport() {
  return {
    error: { code: "recovery_probe_failed", retryable: false, type: "probe_failure" },
    result: "failed",
  };
}
