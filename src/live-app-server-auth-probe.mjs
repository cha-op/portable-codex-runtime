import assert from "node:assert/strict";
import { createHash, randomUUID } from "node:crypto";
import {
  mkdir,
  mkdtemp,
  open,
  readFile,
  realpath,
  rename,
  rm,
  stat,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { basename, dirname, isAbsolute, join, relative, resolve, sep } from "node:path";

import {
  AppServerClient,
  codexVersion,
  fileExists,
} from "./app-server-auth-probe.mjs";

const DEFAULT_AUTH_HOME = ".test-codex-home";
const DEFAULT_EVIDENCE_PATH = "evidence/live-external-auth.json";
const DEFAULT_MODEL = "gpt-5.4";
const MIN_TOKEN_VALIDITY_SECONDS = 120;

function sha256(value) {
  return createHash("sha256").update(value).digest("hex");
}

function fingerprint(value) {
  return `sha256:${sha256(value).slice(0, 24)}`;
}

function decodeJwtPayload(token, label) {
  const parts = token.split(".");
  assert(parts.length >= 2, `${label} is not a JWT`);
  try {
    return JSON.parse(Buffer.from(parts[1], "base64url").toString("utf8"));
  } catch (error) {
    throw new Error(`${label} has an invalid JWT payload`, { cause: error });
  }
}

function authClaims(payload) {
  const claims = payload?.["https://api.openai.com/auth"];
  return claims && typeof claims === "object" ? claims : {};
}

function collectRedactionValues(auth, accessPayload, idPayload) {
  const candidates = [
    auth.OPENAI_API_KEY,
    auth.tokens?.access_token,
    auth.tokens?.refresh_token,
    auth.tokens?.id_token,
    auth.tokens?.account_id,
    accessPayload?.email,
    idPayload?.email,
    authClaims(accessPayload).chatgpt_account_id,
    authClaims(accessPayload).chatgpt_user_id,
    authClaims(idPayload).chatgpt_account_id,
    authClaims(idPayload).chatgpt_user_id,
  ];
  return [...new Set(candidates.filter((value) => typeof value === "string" && value.length >= 8))];
}

function displayAuthPath(authPath) {
  const workspaceRelative = relative(process.cwd(), authPath);
  if (!workspaceRelative.startsWith("..") && !isAbsolute(workspaceRelative)) {
    return workspaceRelative;
  }
  return join(basename(dirname(authPath)), basename(authPath));
}

export async function readDedicatedChatgptCredential(authHome = DEFAULT_AUTH_HOME) {
  const authPath = join(resolve(authHome), "auth.json");
  const [raw, fileStat] = await Promise.all([readFile(authPath, "utf8"), stat(authPath)]);
  assert.equal(fileStat.mode & 0o077, 0, "dedicated auth.json must not be group/world accessible");

  const auth = JSON.parse(raw);
  assert.equal(auth.auth_mode, "chatgpt", "dedicated auth.json must use ChatGPT auth");
  assert.equal(typeof auth.tokens?.access_token, "string", "auth.json is missing access_token");
  assert.equal(typeof auth.tokens?.id_token, "string", "auth.json is missing id_token");
  assert.equal(typeof auth.tokens?.refresh_token, "string", "auth.json is missing refresh_token");

  const accessPayload = decodeJwtPayload(auth.tokens.access_token, "access_token");
  const idPayload = decodeJwtPayload(auth.tokens.id_token, "id_token");
  const accessAuth = authClaims(accessPayload);
  const idAuth = authClaims(idPayload);
  const accountId =
    auth.tokens.account_id ?? accessAuth.chatgpt_account_id ?? idAuth.chatgpt_account_id;
  assert.equal(typeof accountId, "string", "auth.json is missing a ChatGPT account/workspace id");
  const planType = accessAuth.chatgpt_plan_type ?? idAuth.chatgpt_plan_type ?? null;
  const expiresAt =
    typeof accessPayload.exp === "number" ? new Date(accessPayload.exp * 1000).toISOString() : null;
  if (typeof accessPayload.exp === "number") {
    const remainingSeconds = accessPayload.exp - Math.floor(Date.now() / 1000);
    assert(
      remainingSeconds >= MIN_TOKEN_VALIDITY_SECONDS,
      `access token expires too soon (${remainingSeconds}s); refresh the dedicated login first`,
    );
  }

  return {
    accessToken: auth.tokens.access_token,
    accountId,
    authFileFingerprint: fingerprint(raw),
    authMode: auth.auth_mode,
    authPath,
    authPathForEvidence: displayAuthPath(authPath),
    credentialFingerprint: fingerprint(auth.tokens.access_token),
    expiresAt,
    fileMode: (fileStat.mode & 0o777).toString(8).padStart(4, "0"),
    lastRefreshAt: auth.last_refresh ?? null,
    planType,
    redactionValues: collectRedactionValues(auth, accessPayload, idPayload),
  };
}

export function assertNoCredentialMaterial(serializedEvidence, credential) {
  for (const secret of credential.redactionValues) {
    assert.equal(
      serializedEvidence.includes(secret),
      false,
      "evidence contains raw credential or account identity material",
    );
  }
}

function isSameOrDescendant(candidate, parent) {
  return candidate === parent || candidate.startsWith(`${parent}${sep}`);
}

async function resolveThroughExistingAncestor(path) {
  const missingSegments = [];
  let cursor = resolve(path);
  while (true) {
    try {
      const canonicalAncestor = await realpath(cursor);
      return join(canonicalAncestor, ...missingSegments.reverse());
    } catch (error) {
      if (error?.code !== "ENOENT") throw error;
      const parent = dirname(cursor);
      assert.notEqual(parent, cursor, "could not resolve an existing evidence path ancestor");
      missingSegments.push(basename(cursor));
      cursor = parent;
    }
  }
}

export async function validateEvidenceDestination(path, sourceAuthPath) {
  const evidencePath = resolve(path);
  const lexicalAuthHome = resolve(dirname(sourceAuthPath));
  assert.equal(
    isSameOrDescendant(evidencePath, lexicalAuthHome),
    false,
    "evidence destination must not overlap the dedicated auth home",
  );

  const [canonicalDestination, canonicalSource, sourceStat] = await Promise.all([
    resolveThroughExistingAncestor(evidencePath),
    realpath(sourceAuthPath),
    stat(sourceAuthPath),
  ]);
  const canonicalAuthHome = dirname(canonicalSource);
  assert.equal(
    isSameOrDescendant(canonicalDestination, canonicalAuthHome),
    false,
    "evidence destination must not resolve inside the dedicated auth home",
  );

  const canonicalParent = dirname(canonicalDestination);
  await mkdir(canonicalParent, { recursive: true });
  assert.equal(
    await realpath(canonicalParent),
    canonicalParent,
    "evidence destination parent changed while it was being prepared",
  );

  try {
    const destinationStat = await stat(canonicalDestination);
    assert.equal(
      destinationStat.dev === sourceStat.dev && destinationStat.ino === sourceStat.ino,
      false,
      "evidence destination must not reference the source auth file",
    );
  } catch (error) {
    if (error?.code !== "ENOENT") throw error;
  }
  return canonicalDestination;
}

async function writeLiveConfig(codexHome, model) {
  const config = `
model = ${JSON.stringify(model)}
approval_policy = "never"
sandbox_mode = "read-only"
disable_response_storage = true

[features]
shell_snapshot = false
`;
  await writeFile(join(codexHome, "config.toml"), config);
}

export async function writeEvidenceAtomically(
  evidencePath,
  serialized,
  { nonce = randomUUID() } = {},
) {
  const temporaryPath = `${evidencePath}.${process.pid}.${nonce}.tmp`;
  let created = false;
  let handle;
  try {
    handle = await open(temporaryPath, "wx", 0o600);
    created = true;
    await handle.writeFile(serialized, "utf8");
    await handle.sync();
    await handle.close();
    handle = undefined;
    await rename(temporaryPath, evidencePath);
    created = false;
  } finally {
    if (handle) await handle.close().catch(() => {});
    if (created) await rm(temporaryPath, { force: true });
  }
}

async function writeEvidence(path, report, credential) {
  const evidencePath = await validateEvidenceDestination(path, credential.authPath);
  const serialized = `${JSON.stringify(report, null, 2)}\n`;
  assertNoCredentialMaterial(serialized, credential);
  await writeEvidenceAtomically(evidencePath, serialized);
  return evidencePath;
}

export async function probeLiveExternalAuth({
  authHome = process.env.CODEX_TEST_HOME ?? DEFAULT_AUTH_HOME,
  codexBin = process.env.CODEX_BIN ?? "codex",
  evidencePath = process.env.CODEX_LIVE_EVIDENCE ?? DEFAULT_EVIDENCE_PATH,
  model = process.env.CODEX_LIVE_PROBE_MODEL ?? DEFAULT_MODEL,
} = {}) {
  const startedAt = new Date().toISOString();
  const sourceBefore = await readDedicatedChatgptCredential(authHome);
  const workerHome = await mkdtemp(join(tmpdir(), "portable-codex-live-auth-"));
  const workspace = join(workerHome, "workspace");
  await mkdir(workspace);
  let refreshCallbackCount = 0;

  const client = new AppServerClient({
    codexBin,
    codexHome: workerHome,
    timeoutMs: 120_000,
    onRefresh: async () => {
      refreshCallbackCount += 1;
      const latest = await readDedicatedChatgptCredential(authHome);
      return {
        accessToken: latest.accessToken,
        chatgptAccountId: latest.accountId,
        chatgptPlanType: latest.planType,
      };
    },
  });

  try {
    await writeLiveConfig(workerHome, model);
    await client.start();
    const initializeResult = await client.initialize(true);
    const loginResult = await client.request("account/login/start", {
      type: "chatgptAuthTokens",
      accessToken: sourceBefore.accessToken,
      chatgptAccountId: sourceBefore.accountId,
      chatgptPlanType: sourceBefore.planType,
    });
    assert.equal(loginResult.type, "chatgptAuthTokens");

    const threadResult = await client.request("thread/start", {
      cwd: workspace,
      model,
    });
    const turnResult = await client.request("turn/start", {
      threadId: threadResult.thread.id,
      input: [
        {
          type: "text",
          text: "Reply with exactly LIVE_EXTERNAL_AUTH_OK. Do not call tools.",
          textElements: [],
        },
      ],
    });
    const completed = await client.waitForNotification("turn/completed");
    assert.equal(completed.params.turn.status, "completed");
    assert.equal(await fileExists(join(workerHome, "auth.json")), false);

    const sourceAfter = await readDedicatedChatgptCredential(authHome);
    assert.equal(
      sourceAfter.authFileFingerprint,
      sourceBefore.authFileFingerprint,
      "source auth.json changed during the read-only live probe",
    );

    const report = {
      schemaVersion: 1,
      probe: "app-server-chatgpt-auth-tokens-live",
      startedAt,
      completedAt: new Date().toISOString(),
      codexVersion: codexVersion(codexBin),
      userAgent: initializeResult.userAgent,
      experimentalApi: true,
      sourceAuth: {
        path: sourceBefore.authPathForEvidence,
        mode: sourceBefore.fileMode,
        authMode: sourceBefore.authMode,
        planType: sourceBefore.planType,
        unchangedDuringProbe: true,
      },
      worker: {
        loginType: loginResult.type,
        model: threadResult.model,
        modelProvider: threadResult.modelProvider,
        turnStatus: completed.params.turn.status,
        refreshCallbackCount,
        authJsonCreated: false,
      },
      result: "passed",
    };
    const serialized = JSON.stringify(report);
    assertNoCredentialMaterial(serialized, sourceBefore);
    const writtenEvidencePath = await writeEvidence(evidencePath, report, sourceBefore);
    return {
      ...report,
      evidencePath: relative(process.cwd(), writtenEvidencePath),
    };
  } finally {
    await client.stop();
    await rm(workerHome, { recursive: true, force: true });
  }
}
