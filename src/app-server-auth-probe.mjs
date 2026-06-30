import assert from "node:assert/strict";
import { spawn, spawnSync } from "node:child_process";
import { once } from "node:events";
import { createServer } from "node:http";
import { mkdtemp, mkdir, rm, stat, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createInterface } from "node:readline";

const DEFAULT_TIMEOUT_MS = 20_000;
const INITIAL_ACCOUNT_ID = "123e4567-e89b-42d3-a456-426614174011";

export class JsonRpcError extends Error {
  constructor(method, payload) {
    super(`${method} failed: ${payload.message ?? JSON.stringify(payload)}`);
    this.name = "JsonRpcError";
    this.method = method;
    this.payload = payload;
  }
}

export class AppServerClient {
  constructor({ codexBin, codexHome, onRefresh, timeoutMs = DEFAULT_TIMEOUT_MS }) {
    this.codexBin = codexBin;
    this.codexHome = codexHome;
    this.onRefresh = onRefresh;
    this.timeoutMs = timeoutMs;
    this.nextRequestId = 1;
    this.pending = new Map();
    this.messages = [];
    this.waiters = [];
    this.stderr = [];
    this.stopping = false;
  }

  async start() {
    const env = { ...process.env, CODEX_HOME: this.codexHome };
    delete env.CODEX_ACCESS_TOKEN;
    delete env.OPENAI_API_KEY;

    this.child = spawn(this.codexBin, ["app-server", "--stdio"], {
      env,
      stdio: ["pipe", "pipe", "pipe"],
    });
    this.exitPromise = once(this.child, "exit");

    this.child.once("error", (error) => this.#failAll(error));
    this.child.once("exit", (code, signal) => {
      if (!this.stopping) {
        this.#failAll(
          new Error(
            `codex app-server exited unexpectedly (${code ?? signal})\n${this.stderr.join("")}`,
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
      } catch (error) {
        this.#failAll(new Error(`invalid app-server JSONL: ${line}`, { cause: error }));
        return;
      }
      this.#handleMessage(message);
    });
    this.child.stderr.on("data", (chunk) => this.stderr.push(chunk.toString()));
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
    const id = this.nextRequestId++;
    const promise = new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this.pending.delete(id);
        reject(new Error(`${method} timed out after ${this.timeoutMs} ms`));
      }, this.timeoutMs);
      this.pending.set(id, { method, resolve, reject, timer });
    });
    this.#send({ method, id, params });
    return promise;
  }

  notify(method, params) {
    this.#send({ method, params });
  }

  waitForNotification(method) {
    return this.#waitFor(
      (message) => message.method === method && message.id === undefined,
      `notification ${method}`,
    );
  }

  async stop() {
    if (!this.child || this.child.exitCode !== null) return;
    this.stopping = true;
    this.child.stdin.end();
    this.child.kill("SIGTERM");
    const forceKill = setTimeout(() => this.child.kill("SIGKILL"), 2_000);
    await this.exitPromise.catch(() => {});
    clearTimeout(forceKill);
    this.stdout.close();
  }

  #send(message) {
    if (!this.child?.stdin.writable) {
      throw new Error("codex app-server stdin is not writable");
    }
    this.child.stdin.write(`${JSON.stringify(message)}\n`);
  }

  #handleMessage(message) {
    if (message.method && message.id !== undefined) {
      void this.#handleServerRequest(message);
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
      this.#send({
        id: message.id,
        error: { code: -32601, message: `unsupported server request: ${message.method}` },
      });
      return;
    }

    try {
      const result = await this.onRefresh(message.params);
      this.#send({ id: message.id, result });
    } catch (error) {
      this.#send({
        id: message.id,
        error: { code: -32000, message: error instanceof Error ? error.message : String(error) },
      });
    }
  }

  #waitFor(predicate, label) {
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
    for (const pending of this.pending.values()) {
      clearTimeout(pending.timer);
      pending.reject(error);
    }
    this.pending.clear();
    for (const waiter of this.waiters) {
      clearTimeout(waiter.timer);
      waiter.reject(error);
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
    close: async () => {
      server.close();
      await once(server, "close");
    },
  };
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

export function codexVersion(codexBin) {
  const result = spawnSync(codexBin, ["--version"], { encoding: "utf8" });
  if (result.error) throw result.error;
  if (result.status !== 0) {
    throw new Error(`failed to run ${codexBin} --version: ${result.stderr}`);
  }
  return result.stdout.trim();
}

export async function probeExperimentalGate({ codexBin = process.env.CODEX_BIN ?? "codex" } = {}) {
  const codexHome = await mkdtemp(join(tmpdir(), "portable-codex-gate-"));
  const client = new AppServerClient({ codexBin, codexHome });
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
    assert.match(rejection.message, /experimentalApi capability/);
    assert.equal(await fileExists(join(codexHome, "auth.json")), false);
    return { gated: true, errorMessage: rejection.payload.message };
  } finally {
    await client.stop();
    await rm(codexHome, { recursive: true, force: true });
  }
}

export async function probeExternalAuthRefresh({
  codexBin = process.env.CODEX_BIN ?? "codex",
} = {}) {
  const codexHome = await mkdtemp(join(tmpdir(), "portable-codex-auth-"));
  const workspace = join(codexHome, "workspace");
  await mkdir(workspace);
  const mock = await startResponsesMock();

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

  const client = new AppServerClient({
    codexBin,
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

  try {
    await writeProbeConfig(codexHome, mock.baseUrl, mock.chatgptBaseUrl);
    await client.start();
    const initializeResult = await client.initialize(true);
    const loginResult = await client.request("account/login/start", {
      type: "chatgptAuthTokens",
      accessToken: initialToken,
      chatgptAccountId: INITIAL_ACCOUNT_ID,
      chatgptPlanType: "enterprise",
    });
    assert.equal(loginResult.type, "chatgptAuthTokens");

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
    assert.equal(await fileExists(join(codexHome, "auth.json")), false);

    return {
      codexVersion: codexVersion(codexBin),
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
  } finally {
    await client.stop();
    await mock.close();
    await rm(codexHome, { recursive: true, force: true });
  }
}

export async function runAppServerAuthProbe(options = {}) {
  const gate = await probeExperimentalGate(options);
  const refresh = await probeExternalAuthRefresh(options);
  return { gate, refresh };
}
