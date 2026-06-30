import assert from "node:assert/strict";
import { mkdtemp, mkdir, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import {
  assertNoCredentialMaterial,
  readDedicatedChatgptCredential,
} from "../src/live-app-server-auth-probe.mjs";

function encodeJwt(payload) {
  const header = Buffer.from(JSON.stringify({ alg: "none", typ: "JWT" })).toString("base64url");
  const body = Buffer.from(JSON.stringify(payload)).toString("base64url");
  const signature = Buffer.from("signature").toString("base64url");
  return `${header}.${body}.${signature}`;
}

test("dedicated credential metadata is redacted before evidence is written", async () => {
  const authHome = await mkdtemp(join(tmpdir(), "portable-codex-live-fixture-"));
  const accountId = "123e4567-e89b-42d3-a456-426614174099";
  const email = "live-probe@example.com";
  const authClaims = {
    chatgpt_account_id: accountId,
    chatgpt_plan_type: "enterprise",
    chatgpt_user_id: "user-sensitive-123",
  };
  const accessToken = encodeJwt({
    email,
    exp: Math.floor(Date.now() / 1000) + 3600,
    "https://api.openai.com/auth": authClaims,
  });
  const idToken = encodeJwt({
    email,
    "https://api.openai.com/auth": authClaims,
  });
  const refreshToken = "refresh-sensitive-1234567890";

  try {
    await mkdir(authHome, { recursive: true });
    await writeFile(
      join(authHome, "auth.json"),
      JSON.stringify({
        OPENAI_API_KEY: null,
        auth_mode: "chatgpt",
        last_refresh: "2026-06-30T00:00:00Z",
        tokens: {
          access_token: accessToken,
          account_id: accountId,
          id_token: idToken,
          refresh_token: refreshToken,
        },
      }),
      { mode: 0o600 },
    );

    const credential = await readDedicatedChatgptCredential(authHome);
    assert.equal(credential.planType, "enterprise");
    assert.equal(credential.fileMode, "0600");

    const safeEvidence = JSON.stringify({
      accountFingerprint: `sha256:${"a".repeat(24)}`,
      credentialFingerprint: credential.credentialFingerprint,
      planType: credential.planType,
    });
    assert.doesNotThrow(() => assertNoCredentialMaterial(safeEvidence, credential));
    assert.throws(
      () => assertNoCredentialMaterial(JSON.stringify({ accessToken }), credential),
      /credential or account identity material/,
    );
  } finally {
    await rm(authHome, { recursive: true, force: true });
  }
});
