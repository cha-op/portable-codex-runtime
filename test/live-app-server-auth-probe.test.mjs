import assert from "node:assert/strict";
import { link, mkdtemp, mkdir, readFile, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import {
  assertNoCredentialMaterial,
  readDedicatedChatgptCredential,
  validateEvidenceDestination,
  writeEvidenceAtomically,
} from "../src/live-app-server-auth-probe.mjs";

function encodeJwt(payload) {
  const header = Buffer.from(JSON.stringify({ alg: "none", typ: "JWT" })).toString("base64url");
  const body = Buffer.from(JSON.stringify(payload)).toString("base64url");
  const signature = Buffer.from("signature").toString("base64url");
  return `${header}.${body}.${signature}`;
}

test("dedicated credential metadata is redacted before evidence is written", async () => {
  const authHome = await mkdtemp(join(tmpdir(), "portable-codex-live-fixture-"));
  const evidenceHome = await mkdtemp(join(tmpdir(), "portable-codex-evidence-fixture-"));
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
    await assert.rejects(
      () => validateEvidenceDestination(join(authHome, "auth.json"), credential.authPath),
      /must not overlap the dedicated auth home/,
    );

    const hardLinkPath = join(evidenceHome, "hard-linked-auth.json");
    await link(credential.authPath, hardLinkPath);
    await assert.rejects(
      () => validateEvidenceDestination(hardLinkPath, credential.authPath),
      /must not reference the source auth file/,
    );

    const linkedAuthHome = join(evidenceHome, "linked-auth-home");
    await symlink(authHome, linkedAuthHome, "dir");
    await assert.rejects(
      () => validateEvidenceDestination(join(linkedAuthHome, "evidence.json"), credential.authPath),
      /must not resolve inside the dedicated auth home/,
    );
    const nestedAuthPath = join(linkedAuthHome, "new", "evidence.json");
    await assert.rejects(
      () => validateEvidenceDestination(nestedAuthPath, credential.authPath),
      /must not resolve inside the dedicated auth home/,
    );
    await assert.rejects(() => readFile(join(authHome, "new")), /ENOENT/);

    const unrelatedTargetPath = join(evidenceHome, "unrelated-target.json");
    const linkedEvidencePath = join(evidenceHome, "linked-evidence.json");
    await writeFile(unrelatedTargetPath, "leave unchanged\n");
    await symlink(unrelatedTargetPath, linkedEvidencePath);
    await assert.rejects(
      () => validateEvidenceDestination(linkedEvidencePath, credential.authPath),
      /must not be a symbolic link/,
    );
    assert.equal(await readFile(unrelatedTargetPath, "utf8"), "leave unchanged\n");

    const collisionEvidencePath = join(evidenceHome, "collision-evidence.json");
    const collisionNonce = "known-collision";
    const collisionTemporaryPath = `${collisionEvidencePath}.${process.pid}.${collisionNonce}.tmp`;
    const authBeforeCollision = await readFile(credential.authPath, "utf8");
    await symlink(credential.authPath, collisionTemporaryPath);
    await assert.rejects(
      () =>
        writeEvidenceAtomically(collisionEvidencePath, "{}\n", {
          nonce: collisionNonce,
        }),
      (error) => error?.code === "EEXIST",
    );
    assert.equal(await readFile(credential.authPath, "utf8"), authBeforeCollision);
  } finally {
    await rm(authHome, { recursive: true, force: true });
    await rm(evidenceHome, { recursive: true, force: true });
  }
});
