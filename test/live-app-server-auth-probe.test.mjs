import assert from "node:assert/strict";
import {
  link,
  mkdtemp,
  mkdir,
  readFile,
  realpath,
  rename,
  rm,
  stat,
  symlink,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { dirname, join } from "node:path";
import test from "node:test";

import {
  assertRefreshAccountContinuity,
  assertNoCredentialMaterial,
  probeLiveExternalAuth,
  readDedicatedChatgptCredential,
  validateEvidenceDestination,
  writeEvidenceSafely,
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
    assert.equal(credential.authPathForEvidence, "dedicated-auth-home/auth.json");

    assert.doesNotThrow(() =>
      assertRefreshAccountContinuity(
        { previousAccountId: credential.accountId },
        credential,
        credential,
      ),
    );
    assert.throws(
      () =>
        assertRefreshAccountContinuity(
          { previousAccountId: "different-account" },
          credential,
          credential,
        ),
      /refresh request account does not match/,
    );
    assert.throws(
      () =>
        assertRefreshAccountContinuity(
          { previousAccountId: credential.accountId },
          credential,
          { ...credential, accountId: "different-account" },
        ),
      /refreshed credential account does not match/,
    );

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

    const unrelatedHardLinkSource = join(evidenceHome, "unrelated-hardlink-source.json");
    const unrelatedHardLinkPath = join(evidenceHome, "unrelated-hardlink-evidence.json");
    await writeFile(unrelatedHardLinkSource, "leave hardlink unchanged\n");
    await link(unrelatedHardLinkSource, unrelatedHardLinkPath);
    await assert.rejects(
      () => writeEvidenceSafely(unrelatedHardLinkPath, "must not be written\n", credential.authPath),
      /must not be hard linked/,
    );
    assert.equal(await readFile(unrelatedHardLinkSource, "utf8"), "leave hardlink unchanged\n");

    const safeEvidencePath = join(evidenceHome, "safe-evidence.json");
    await writeFile(safeEvidencePath, "stale evidence that is longer\n");
    await writeEvidenceSafely(safeEvidencePath, "{}\n", credential.authPath);
    assert.equal(await readFile(safeEvidencePath, "utf8"), "{}\n");

    const nestedEvidencePath = join(evidenceHome, "nested", "fresh-evidence.json");
    const writtenNestedPath = await writeEvidenceSafely(
      nestedEvidencePath,
      '{"result":"passed"}\n',
      credential.authPath,
    );
    assert.equal(writtenNestedPath, await realpath(nestedEvidencePath));
    assert.equal(await readFile(nestedEvidencePath, "utf8"), '{"result":"passed"}\n');
    assert.equal((await stat(nestedEvidencePath)).mode & 0o777, 0o600);

    const exchangeParent = join(evidenceHome, "exchange-parent");
    const displacedParent = join(evidenceHome, "displaced-parent");
    const exchangeEvidencePath = join(exchangeParent, "auth.json");
    await mkdir(exchangeParent);
    const authBeforeExchange = await readFile(credential.authPath, "utf8");
    await assert.rejects(
      () =>
        writeEvidenceSafely(exchangeEvidencePath, "must not be written\n", credential.authPath, {
          afterOpen: async () => {
            await rename(exchangeParent, displacedParent);
            await symlink(authHome, exchangeParent, "dir");
          },
        }),
      /parent changed before the write/,
    );
    assert.equal(await readFile(credential.authPath, "utf8"), authBeforeExchange);
  } finally {
    await rm(authHome, { recursive: true, force: true });
    await rm(evidenceHome, { recursive: true, force: true });
  }
});

test("malformed dedicated auth JSON errors omit credential fragments", async () => {
  const authHome = await mkdtemp(join(tmpdir(), "portable-codex-malformed-auth-"));
  const sensitiveFragment = "REFRESH_TOKEN_SECRET_SENTINEL";
  try {
    await writeFile(join(authHome, "auth.json"), `{"refresh_token":"${sensitiveFragment}"`, {
      mode: 0o600,
    });
    await assert.rejects(readDedicatedChatgptCredential(authHome), (error) => {
      assert.equal(error.message, "dedicated auth.json is not valid JSON");
      assert.doesNotMatch(error.stack, /REFRESH_TOKEN_SECRET_SENTINEL/);
      return true;
    });
  } finally {
    await rm(authHome, { recursive: true, force: true });
  }
});

test("dedicated credential rejects symlinks and mismatched account claims", async () => {
  const fixtureHome = await mkdtemp(join(tmpdir(), "portable-codex-credential-checks-"));
  const matchingAccountId = "123e4567-e89b-42d3-a456-426614174099";
  const mismatchedAccountId = "123e4567-e89b-42d3-a456-426614174100";
  const claims = {
    chatgpt_account_id: matchingAccountId,
    chatgpt_plan_type: "enterprise",
  };
  const auth = {
    auth_mode: "chatgpt",
    tokens: {
      access_token: encodeJwt({
        exp: Math.floor(Date.now() / 1000) + 3600,
        "https://api.openai.com/auth": claims,
      }),
      account_id: mismatchedAccountId,
      id_token: encodeJwt({ "https://api.openai.com/auth": claims }),
      refresh_token: "refresh-sensitive-1234567890",
    },
  };
  const realAuthPath = join(fixtureHome, "real-auth.json");
  const mismatchedHome = join(fixtureHome, "mismatched");
  const symlinkHome = join(fixtureHome, "symlinked");
  try {
    await mkdir(mismatchedHome);
    await writeFile(join(mismatchedHome, "auth.json"), JSON.stringify(auth), { mode: 0o600 });
    await assert.rejects(
      readDedicatedChatgptCredential(mismatchedHome),
      /auth\.json account IDs do not match/,
    );

    await writeFile(
      realAuthPath,
      JSON.stringify({
        ...auth,
        tokens: { ...auth.tokens, account_id: matchingAccountId },
      }),
      { mode: 0o600 },
    );
    await mkdir(symlinkHome);
    await symlink(realAuthPath, join(symlinkHome, "auth.json"));
    await assert.rejects(
      readDedicatedChatgptCredential(symlinkHome),
      (error) => error?.code === "ELOOP",
    );
  } finally {
    await rm(fixtureHome, { recursive: true, force: true });
  }
});

test("live auth probe removes its worker home after setup failure", async () => {
  const authHome = await mkdtemp(join(tmpdir(), "portable-codex-live-cleanup-auth-"));
  const accountId = "123e4567-e89b-42d3-a456-426614174099";
  const claims = {
    chatgpt_account_id: accountId,
    chatgpt_plan_type: "enterprise",
  };
  let workerHome;
  try {
    await writeFile(
      join(authHome, "auth.json"),
      JSON.stringify({
        auth_mode: "chatgpt",
        tokens: {
          access_token: encodeJwt({
            exp: Math.floor(Date.now() / 1000) + 3600,
            "https://api.openai.com/auth": claims,
          }),
          account_id: accountId,
          id_token: encodeJwt({ "https://api.openai.com/auth": claims }),
          refresh_token: "refresh-sensitive-1234567890",
        },
      }),
      { mode: 0o600 },
    );
    await assert.rejects(
      () =>
        probeLiveExternalAuth({
          authHome,
          makeDirectory: async (workspace) => {
            workerHome = dirname(workspace);
            throw new Error("live setup failed");
          },
        }),
      /live setup failed/,
    );
    await assert.rejects(stat(workerHome), /ENOENT/);
  } finally {
    await rm(authHome, { recursive: true, force: true });
  }
});
