import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import { once } from "node:events";
import { watch } from "node:fs";
import { createServer } from "node:http";
import { mkdtemp, mkdir, readFile, readdir, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { delimiter, isAbsolute, join, resolve, sep } from "node:path";
import { createInterface } from "node:readline";

const DEFAULT_TIMEOUT_MS = 20_000;
const DEFAULT_SHUTDOWN_GRACE_MS = 2_000;
const INITIAL_ACCOUNT_ID = "123e4567-e89b-42d3-a456-426614174011";
const WORKER_ENV_KEYS = [
  "PATH",
  "TMPDIR",
  "TMP",
  "TEMP",
  "LANG",
  "LC_ALL",
  "LC_CTYPE",
  "SSL_CERT_FILE",
  "SSL_CERT_DIR",
  "PATHEXT",
  "SystemRoot",
  "WINDIR",
  "ComSpec",
];

function processTreeExists(child) {
  if (!child?.pid) return false;
  if (process.platform === "win32") {
    return child.exitCode === null && child.signalCode === null;
  }
  try {
    process.kill(-child.pid, 0);
    return true;
  } catch (error) {
    return error?.code !== "ESRCH";
  }
}

function parseLinuxProcessStat(raw) {
  const commandEnd = raw.lastIndexOf(")");
  if (commandEnd < 0) return null;
  const fields = raw.slice(commandEnd + 1).trim().split(/\s+/);
  if (fields.length < 3) return null;
  const processGroupId = Number(fields[2]);
  if (!Number.isSafeInteger(processGroupId) || processGroupId <= 0) return null;
  return { processGroupId, state: fields[0] };
}

export async function inspectLinuxProcessGroup(processGroupId, procRoot = "/proc") {
  let entries;
  try {
    entries = await readdir(procRoot, { withFileTypes: true });
  } catch {
    return "unknown";
  }

  let foundMember = false;
  let inspectionUncertain = false;
  for (const entry of entries) {
    if (!/^\d+$/.test(entry.name)) continue;
    let parsed;
    try {
      parsed = parseLinuxProcessStat(
        await readFile(join(procRoot, entry.name, "stat"), "utf8"),
      );
    } catch (error) {
      if (error?.code !== "ENOENT") inspectionUncertain = true;
      continue;
    }
    if (!parsed) {
      inspectionUncertain = true;
      continue;
    }
    if (parsed.processGroupId !== processGroupId) continue;
    foundMember = true;
    if (!["Z", "X", "x"].includes(parsed.state)) return "live";
  }

  if (inspectionUncertain) return "unknown";
  return foundMember ? "zombie-only" : "empty";
}

function signalProcessTree(child, signal) {
  if (!child?.pid) return;
  try {
    if (process.platform === "win32") child.kill(signal);
    else process.kill(-child.pid, signal);
  } catch (error) {
    if (error?.code !== "ESRCH") throw error;
  }
}

async function waitForProcessTreeExit(child, timeoutMs, { acceptZombieOnly = false } = {}) {
  const deadline = Date.now() + timeoutMs;
  while (processTreeExists(child)) {
    if (acceptZombieOnly && process.platform === "linux") {
      const state = await inspectLinuxProcessGroup(child.pid);
      if (state === "empty" || state === "zombie-only") return true;
    }
    if (Date.now() >= deadline) return false;
    await new Promise((resolve) => setTimeout(resolve, 20));
  }
  return true;
}

const DEFAULT_PROCESS_CONTROL = Object.freeze({
  exists: processTreeExists,
  signal: signalProcessTree,
  waitForExit: waitForProcessTreeExit,
});

function releaseAppServerHandles(child, output) {
  let cleanupError;
  const attempt = (operation) => {
    try {
      operation();
    } catch (error) {
      cleanupError ??= error;
    }
  };
  attempt(() => output?.close());
  attempt(() => child?.stdin?.destroy());
  attempt(() => child?.stdout?.destroy());
  attempt(() => child?.stderr?.destroy());
  attempt(() => child?.unref?.());
  return cleanupError;
}

export function buildWorkerEnvironment(
  codexHome,
  sourceEnv = process.env,
  launcherDirectory = process.cwd(),
) {
  const env = { CODEX_HOME: codexHome };
  for (const key of WORKER_ENV_KEYS) {
    if (typeof sourceEnv[key] !== "string") continue;
    env[key] =
      key === "PATH"
        ? sourceEnv[key]
            .split(delimiter)
            .map((entry) => (isAbsolute(entry) ? entry : resolve(launcherDirectory, entry || ".")))
            .join(delimiter)
        : sourceEnv[key];
  }
  return env;
}

export function resolveAppServerExecutable(codexBin, baseDirectory = process.cwd()) {
  if (typeof codexBin !== "string" || codexBin.length === 0 || isAbsolute(codexBin)) {
    return codexBin;
  }
  const containsPathSeparator =
    codexBin.includes("/") || (sep === "\\" && codexBin.includes("\\"));
  return containsPathSeparator ? resolve(baseDirectory, codexBin) : codexBin;
}

export async function runSequentialCleanup(cleanups, primaryFailure) {
  let firstCleanupFailure;
  for (const cleanup of cleanups) {
    try {
      await cleanup();
    } catch (error) {
      firstCleanupFailure ??= { error };
    }
  }

  if (primaryFailure) {
    const primaryError = primaryFailure.error;
    if (
      firstCleanupFailure &&
      primaryError !== null &&
      ["object", "function"].includes(typeof primaryError)
    ) {
      try {
        if (!Object.hasOwn(primaryError, "cleanupError")) {
          Object.defineProperty(primaryError, "cleanupError", {
            configurable: true,
            enumerable: false,
            value: firstCleanupFailure.error,
          });
        }
      } catch {
        // A frozen primary error still takes precedence over cleanup diagnostics.
      }
    }
    return;
  }
  if (firstCleanupFailure) throw firstCleanupFailure.error;
}

export class JsonRpcError extends Error {
  constructor(method, payload) {
    super(`${method} failed`);
    this.name = "JsonRpcError";
    this.method = method;
    Object.defineProperty(this, "payload", {
      configurable: true,
      enumerable: false,
      value: payload,
    });
  }
}

export class AppServerClient {
  constructor({
    codexBin,
    codexArgs = ["app-server", "--stdio"],
    codexHome,
    launcherDirectory = process.cwd(),
    onRefresh,
    processControl = DEFAULT_PROCESS_CONTROL,
    shutdownGraceMs = DEFAULT_SHUTDOWN_GRACE_MS,
    sourceEnv = process.env,
    timeoutMs = DEFAULT_TIMEOUT_MS,
  }) {
    this.codexBin = resolveAppServerExecutable(codexBin, launcherDirectory);
    this.codexArgs = codexArgs;
    this.codexHome = codexHome;
    this.environment = buildWorkerEnvironment(codexHome, sourceEnv, launcherDirectory);
    this.onRefresh = onRefresh;
    this.processControl = processControl;
    this.shutdownGraceMs = shutdownGraceMs;
    this.timeoutMs = timeoutMs;
    this.nextRequestId = 1;
    this.pending = new Map();
    this.messages = [];
    this.waiters = [];
    this.sentRpcMethods = [];
    this.stderrBytes = 0;
    this.stopping = false;
    this.terminalError = null;
    this.processTreeQuiesced = false;
    this.shutdownCompleted = false;
    this.shutdownPromise = undefined;
  }

  async start() {
    this.child = spawn(this.codexBin, this.codexArgs, {
      cwd: this.codexHome,
      detached: process.platform !== "win32",
      env: this.environment,
      stdio: ["pipe", "pipe", "pipe"],
    });
    this.exitPromise = once(this.child, "exit");
    void this.exitPromise.catch(() => {});

    this.child.once("error", (error) => this.#failAll(error));
    this.child.stdin.on("error", (error) => {
      if (!this.stopping) this.#failAll(error);
    });
    this.child.once("exit", (code, signal) => {
      if (!this.stopping) {
        this.#failAll(
          new Error(
            `codex app-server exited unexpectedly (${code ?? signal}); ` +
              `stderr omitted (${this.stderrBytes} bytes)`,
          ),
        );
      }
    });

    this.stdout = createInterface({ input: this.child.stdout });
    this.stdout.on("line", (line) => {
      if (line.trim() === "") return;
      let message;
      try {
        message = JSON.parse(line);
      } catch {
        this.#failAll(
          new Error(`invalid app-server JSONL (${Buffer.byteLength(line, "utf8")} bytes)`),
        );
        return;
      }
      if (message === null || typeof message !== "object" || Array.isArray(message)) {
        this.#failAll(
          new Error(`invalid app-server message (${Buffer.byteLength(line, "utf8")} bytes)`),
        );
        return;
      }
      this.#handleMessage(message);
    });
    this.child.stderr.on("data", (chunk) => {
      this.stderrBytes += chunk.length;
    });
  }

  async initialize(experimentalApi) {
    const result = await this.request("initialize", {
      clientInfo: {
        name: "portable_codex_runtime_probe",
        title: "Portable Codex Runtime Probe",
        version: "0.1.0",
      },
      capabilities: { experimentalApi },
    });
    this.notify("initialized", {});
    return result;
  }

  request(method, params) {
    if (this.terminalError) return Promise.reject(this.terminalError);
    const id = this.nextRequestId++;
    this.sentRpcMethods.push({ kind: "request", method });
    const promise = new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new Error(`${method} timed out after ${this.timeoutMs} ms`));
      }, this.timeoutMs);
      this.pending.set(id, { method, resolve, reject, timer });
    });
    try {
      this.#send({ method, id, params });
    } catch (error) {
      this.#failAll(error);
    }
    return promise;
  }

  notify(method, params) {
    this.sentRpcMethods.push({ kind: "notification", method });
    this.#send({ method, params });
  }

  rpcMethodAudit() {
    return this.sentRpcMethods.map((entry) => ({ ...entry }));
  }

  waitForNotification(method) {
    return this.#waitFor(
      (message) => message.method === method && message.id === undefined,
      `notification ${method}`,
    );
  }

  stop() {
    if (!this.child) return Promise.resolve();
    if (this.shutdownPromise) return this.shutdownPromise;
    const attempt = this.#stopOnce();
    this.shutdownPromise = attempt;
    void attempt.then(
      () => {
        this.shutdownCompleted = true;
      },
      () => {
        if (this.shutdownPromise === attempt) this.shutdownPromise = undefined;
      },
    );
    return attempt;
  }

  async #stopOnce() {
    this.stopping = true;
    let shutdownError;
    try {
      if (!this.processTreeQuiesced) {
        if (this.processControl.exists(this.child)) {
          this.child.stdin.end();
          if (!(await this.processControl.waitForExit(this.child, this.shutdownGraceMs))) {
            this.processControl.signal(this.child, "SIGTERM");
            if (!(await this.processControl.waitForExit(this.child, this.shutdownGraceMs))) {
              this.processControl.signal(this.child, "SIGKILL");
              if (!(
                await this.processControl.waitForExit(this.child, this.shutdownGraceMs, {
                  acceptZombieOnly: true,
                })
              )) {
                throw new Error("codex app-server process group survived SIGKILL");
              }
            }
          }
        }
        this.processTreeQuiesced = true;
      }
      await this.exitPromise?.catch(() => {});
    } catch (error) {
      shutdownError = error;
    } finally {
      const handleCleanupError = releaseAppServerHandles(this.child, this.stdout);
      shutdownError ??= handleCleanupError;
      this.#failAll(
        this.terminalError ?? shutdownError ?? new Error("codex app-server stopped"),
      );
    }
    if (shutdownError) throw shutdownError;
  }

  abort(error = new Error("codex app-server aborted")) {
    if (this.shutdownPromise) return this.shutdownPromise;
    this.stopping = true;
    this.#failAll(error);
    const attempt = (async () => {
      let shutdownError;
      try {
        if (!this.processTreeQuiesced) {
          if (
            this.child &&
            this.processControl.exists(this.child)
          ) {
            this.processControl.signal(this.child, "SIGKILL");
            if (!(
              await this.processControl.waitForExit(this.child, this.shutdownGraceMs, {
                acceptZombieOnly: true,
              })
            )) {
              throw new Error("codex app-server process group survived SIGKILL");
            }
          }
          this.processTreeQuiesced = true;
        }
        await this.exitPromise?.catch(() => {});
      } catch (error) {
        shutdownError = error;
      } finally {
        const handleCleanupError = releaseAppServerHandles(this.child, this.stdout);
        shutdownError ??= handleCleanupError;
      }
      if (shutdownError) throw shutdownError;
    })();
    this.abortPromise = attempt;
    this.shutdownPromise = attempt;
    void attempt.then(
      () => {
        this.shutdownCompleted = true;
      },
      () => {
        if (this.abortPromise === attempt) this.abortPromise = undefined;
        if (this.shutdownPromise === attempt) this.shutdownPromise = undefined;
      },
    );
    return attempt;
  }

  #send(message) {
    if (this.terminalError) throw this.terminalError;
    if (!this.child?.stdin.writable) {
      throw new Error("codex app-server stdin is not writable");
    }
    this.child.stdin.write(`${JSON.stringify(message)}\n`);
  }

  #handleMessage(message) {
    if (message.method && message.id !== undefined) {
      void this.#handleServerRequest(message).catch((error) => {
        if (!this.stopping) this.#failAll(error);
      });
      return;
    }

    if (message.id !== undefined) {
      const pending = this.pending.get(message.id);
      if (!pending) return;
      this.pending.delete(message.id);
      clearTimeout(pending.timer);
      if (message.error) {
        pending.reject(new JsonRpcError(pending.method, message.error));
      } else {
        pending.resolve(message.result);
      }
      return;
    }

    const waiterIndex = this.waiters.findIndex((waiter) => waiter.predicate(message));
    if (waiterIndex >= 0) {
      const [waiter] = this.waiters.splice(waiterIndex, 1);
      clearTimeout(waiter.timer);
      waiter.resolve(message);
    } else {
      this.messages.push(message);
    }
  }

  async #handleServerRequest(message) {
    if (message.method !== "account/chatgptAuthTokens/refresh" || !this.onRefresh) {
      this.#sendServerResponse({
        id: message.id,
        error: { code: -32601, message: `unsupported server request: ${message.method}` },
      });
      return;
    }

    try {
      const result = await this.onRefresh(message.params);
      this.#sendServerResponse({ id: message.id, result });
    } catch (error) {
      this.#sendServerResponse({
        id: message.id,
        error: { code: -32000, message: error instanceof Error ? error.message : String(error) },
      });
    }
  }

  #sendServerResponse(message) {
    if (this.stopping || this.terminalError || !this.child?.stdin.writable) return;
    this.#send(message);
  }

  #waitFor(predicate, label) {
    if (this.terminalError) return Promise.reject(this.terminalError);
    const existingIndex = this.messages.findIndex(predicate);
    if (existingIndex >= 0) {
      return Promise.resolve(this.messages.splice(existingIndex, 1)[0]);
    }

    return new Promise((resolve, reject) => {
      const waiter = { predicate, resolve, reject, timer: undefined };
      waiter.timer = setTimeout(() => {
        const index = this.waiters.indexOf(waiter);
        if (index >= 0) this.waiters.splice(index, 1);
        reject(new Error(`${label} timed out after ${this.timeoutMs} ms`));
      }, this.timeoutMs);
      this.waiters.push(waiter);
    });
  }

  #failAll(error) {
    this.terminalError ??= error;
    const failure = this.terminalError;
    for (const pending of this.pending.values()) {
      clearTimeout(pending.timer);
      pending.reject(failure);
    }
    this.pending.clear();
    for (const waiter of this.waiters) {
      clearTimeout(waiter.timer);
      waiter.reject(failure);
    }
    this.waiters = [];
  }
}

function encodeJwt({ email, accountId, planType }) {
  const header = { alg: "none", typ: "JWT" };
  const payload = {
    email,
    "https://api.openai.com/auth": {
      chatgpt_account_id: accountId,
      chatgpt_plan_type: planType,
    },
  };
  const encode = (value) => Buffer.from(JSON.stringify(value)).toString("base64url");
  return `${encode(header)}.${encode(payload)}.${Buffer.from("signature").toString("base64url")}`;
}

function sseBody() {
  const events = [
    { type: "response.created", response: { id: "resp-probe" } },
    {
      type: "response.output_item.done",
      item: {
        type: "message",
        role: "assistant",
        id: "msg-probe",
        content: [{ type: "output_text", text: "probe ok" }],
      },
    },
    {
      type: "response.completed",
      response: {
        id: "resp-probe",
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

async function startResponsesMock() {
  const requests = [];
  const cloudConfigRequests = [];
  let responseCalls = 0;
  const server = createServer(async (request, response) => {
    const chunks = [];
    for await (const chunk of request) chunks.push(chunk);
    const body = Buffer.concat(chunks).toString();

    if (request.method === "GET" && request.url === "/backend-api/wham/config/bundle") {
      cloudConfigRequests.push({ authorization: request.headers.authorization ?? null });
      response.writeHead(200, { "content-type": "application/json" });
      response.end("{}");
      return;
    }

    if (request.method !== "POST" || request.url !== "/v1/responses") {
      response.writeHead(404, { "content-type": "application/json" });
      response.end(JSON.stringify({ error: { message: "not found" } }));
      return;
    }

    responseCalls += 1;
    requests.push({
      authorization: request.headers.authorization ?? null,
      body,
    });

    if (responseCalls === 1) {
      response.writeHead(401, { "content-type": "application/json" });
      response.end(JSON.stringify({ error: { message: "probe unauthorized" } }));
      return;
    }

    response.writeHead(200, {
      "cache-control": "no-cache",
      "content-type": "text/event-stream",
    });
    response.end(sseBody());
  });
  const close = async () => {
    if (server.listening) {
      server.close();
      await once(server, "close");
    }
  };
  try {
    server.listen(0, "127.0.0.1");
    await once(server, "listening");
    const address = server.address();
    assert(address && typeof address !== "string");
    const origin = `http://127.0.0.1:${address.port}`;
    return {
      baseUrl: `${origin}/v1`,
      chatgptBaseUrl: `${origin}/backend-api`,
      cloudConfigRequests,
      requests,
      close,
    };
  } catch (error) {
    await close().catch(() => {});
    throw error;
  }
}

async function writeProbeConfig(codexHome, baseUrl, chatgptBaseUrl) {
  const config = `
model = "gpt-5.4"
model_provider = "probe"
approval_policy = "never"
sandbox_mode = "read-only"
disable_response_storage = true
chatgpt_base_url = "${chatgptBaseUrl}"

[features]
shell_snapshot = false

[model_providers.probe]
name = "OpenAI"
base_url = "${baseUrl}"
wire_api = "responses"
requires_openai_auth = true
request_max_retries = 0
stream_max_retries = 0
supports_websockets = false
`;
  await writeFile(join(codexHome, "config.toml"), config);
}

export async function fileExists(path) {
  try {
    await stat(path);
    return true;
  } catch (error) {
    if (error?.code === "ENOENT") return false;
    throw error;
  }
}

export async function assertNoWorkerAuth(codexHome, phase) {
  assert.equal(
    await fileExists(join(codexHome, "auth.json")),
    false,
    `app-server wrote worker auth.json ${phase}`,
  );
}

export function createWorkerAuthMonitor(codexHome, watchDirectory = watch) {
  let monitorError;
  let observed = false;
  let resolveObservation;
  const observation = new Promise((resolve) => {
    resolveObservation = resolve;
  });
  const watcher = watchDirectory(codexHome, { persistent: false }, (_eventType, filename) => {
    if (filename === null) {
      monitorError ??= new Error("worker auth monitor received an event without a filename");
      resolveObservation();
      return;
    }
    if (filename.toString().toLowerCase() === "auth.json") {
      observed = true;
      resolveObservation();
    }
  });
  watcher.on("error", (error) => {
    monitorError ??= error;
    resolveObservation();
  });

  return {
    async assertNoAuthObserved() {
      await new Promise((resolve) => setImmediate(resolve));
      if (monitorError) {
        throw new Error("worker auth monitor failed", { cause: monitorError });
      }
      assert.equal(observed, false, "app-server created or changed worker auth.json");
    },
    close() {
      watcher.close();
    },
    waitForObservation() {
      return observation;
    },
  };
}

export async function stopAndAssertNoWorkerAuth(client, codexHome) {
  const existedBeforeStop = await fileExists(join(codexHome, "auth.json"));
  await client.stop();
  assert.equal(
    existedBeforeStop,
    false,
    "app-server wrote worker auth.json before shutdown",
  );
  await assertNoWorkerAuth(codexHome, "during shutdown");
}

export function codexVersion(codexBin) {
  const executable = resolveAppServerExecutable(codexBin);
  const result = spawnSync(executable, ["--version"], { encoding: "utf8" });
  if (result.error) throw result.error;
  if (result.status !== 0) {
    throw new Error(`failed to run ${executable} --version: ${result.stderr}`);
  }
  return result.stdout.trim();
}

export async function probeExperimentalGate({ codexBin = process.env.CODEX_BIN ?? "codex" } = {}) {
  const executable = resolveAppServerExecutable(codexBin);
  const codexHome = await mkdtemp(join(tmpdir(), "portable-codex-gate-"));
  const client = new AppServerClient({ codexBin: executable, codexHome });
  let primaryFailure;
  try {
    await writeProbeConfig(
      codexHome,
      "http://127.0.0.1:9/v1",
      "http://127.0.0.1:9/backend-api",
    );
    await client.start();
    await client.initialize(false);

    const token = encodeJwt({
      email: "gate@example.com",
      accountId: INITIAL_ACCOUNT_ID,
      planType: "enterprise",
    });
    let rejection;
    try {
      await client.request("account/login/start", {
        type: "chatgptAuthTokens",
        accessToken: token,
        chatgptAccountId: INITIAL_ACCOUNT_ID,
        chatgptPlanType: "enterprise",
      });
    } catch (error) {
      rejection = error;
    }

    assert(rejection instanceof JsonRpcError, "experimental login should be rejected without opt-in");
    assert.match(rejection.payload.message, /experimentalApi capability/);
    await stopAndAssertNoWorkerAuth(client, codexHome);
    return { gated: true, errorMessage: rejection.payload.message };
  } catch (error) {
    primaryFailure = { error };
    throw error;
  } finally {
    await runSequentialCleanup(
      [
        () => client.stop(),
        () => rm(codexHome, { recursive: true, force: true }),
      ],
      primaryFailure,
    );
  }
}

export async function probeExternalAuthRefresh({
  codexBin = process.env.CODEX_BIN ?? "codex",
  makeDirectory = mkdir,
  startMock = startResponsesMock,
} = {}) {
  const executable = resolveAppServerExecutable(codexBin);
  const codexHome = await mkdtemp(join(tmpdir(), "portable-codex-auth-"));
  let authMonitor;
  let client;
  let mock;
  let primaryFailure;
  try {
    const workspace = join(codexHome, "workspace");
    await makeDirectory(workspace);
    mock = await startMock();

    const initialToken = encodeJwt({
      email: "initial@example.com",
      accountId: INITIAL_ACCOUNT_ID,
      planType: "enterprise",
    });
    const refreshedToken = encodeJwt({
      email: "refreshed@example.com",
      accountId: INITIAL_ACCOUNT_ID,
      planType: "enterprise",
    });
    let refreshParams;
    let refreshCount = 0;

    client = new AppServerClient({
      codexBin: executable,
      codexHome,
      onRefresh: async (params) => {
        refreshCount += 1;
        refreshParams = params;
        return {
          accessToken: refreshedToken,
          chatgptAccountId: INITIAL_ACCOUNT_ID,
          chatgptPlanType: "enterprise",
        };
      },
    });

    await writeProbeConfig(codexHome, mock.baseUrl, mock.chatgptBaseUrl);
    authMonitor = createWorkerAuthMonitor(codexHome);
    await client.start();
    const initializeResult = await client.initialize(true);
    const loginResult = await client.request("account/login/start", {
      type: "chatgptAuthTokens",
      accessToken: initialToken,
      chatgptAccountId: INITIAL_ACCOUNT_ID,
      chatgptPlanType: "enterprise",
    });
    assert.equal(loginResult.type, "chatgptAuthTokens");
    await assertNoWorkerAuth(codexHome, "after external-auth login");
    await authMonitor.assertNoAuthObserved();

    const threadResult = await client.request("thread/start", {
      cwd: workspace,
      model: "gpt-5.4",
    });
    const turnResult = await client.request("turn/start", {
      threadId: threadResult.thread.id,
      input: [{ type: "text", text: "Reply with probe ok.", textElements: [] }],
    });
    const completed = await client.waitForNotification("turn/completed");

    assert.equal(refreshCount, 1);
    assert.deepEqual(refreshParams, {
      reason: "unauthorized",
      previousAccountId: INITIAL_ACCOUNT_ID,
    });
    assert.equal(mock.requests.length, 2);
    assert.equal(mock.requests[0].authorization, `Bearer ${initialToken}`);
    assert.equal(mock.requests[1].authorization, `Bearer ${refreshedToken}`);
    assert.equal(completed.params.turn.status, "completed");
    await assertNoWorkerAuth(codexHome, "after turn completion");
    await authMonitor.assertNoAuthObserved();
    await stopAndAssertNoWorkerAuth(client, codexHome);
    await authMonitor.assertNoAuthObserved();

    return {
      codexVersion: codexVersion(executable),
      userAgent: initializeResult.userAgent,
      loginType: loginResult.type,
      threadId: threadResult.thread.id,
      turnId: turnResult.turn.id,
      refreshCount,
      cloudConfigRequestCount: mock.cloudConfigRequests.length,
      requestAuthorizationSequence: ["initial", "refreshed"],
      authJsonCreated: false,
      turnStatus: completed.params.turn.status,
    };
  } catch (error) {
    primaryFailure = { error };
    throw error;
  } finally {
    await runSequentialCleanup(
      [
        () => client?.stop(),
        () => mock?.close(),
        () => authMonitor?.close(),
        () => rm(codexHome, { recursive: true, force: true }),
      ],
      primaryFailure,
    );
  }
}

export async function runAppServerAuthProbe(options = {}) {
  const gate = await probeExperimentalGate(options);
  const refresh = await probeExternalAuthRefresh(options);
  return { gate, refresh };
}
