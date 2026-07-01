import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { mkdir, mkdtemp, rm } from "node:fs/promises";
import { homedir, tmpdir } from "node:os";
import { isAbsolute, join } from "node:path";

import { codexVersion } from "./app-server-auth-probe.mjs";
import {
  assertNoCredentialMaterial,
  probeLiveExternalAuth,
  writeEvidenceSafely,
} from "./live-app-server-auth-probe.mjs";
import {
  ManagedAuthRefreshAuthority,
  readManagedAuthSnapshot,
} from "./managed-auth-refresh.mjs";

const DEFAULT_AUTH_HOME = ".test-codex-home";
const DEFAULT_EVIDENCE_PATH = "evidence/live-auth-refresh-authority.json";
const DEFAULT_MODEL = "gpt-5.4";

function codexSourceCommit(sourceMirror = join(homedir(), "codex")) {
  const result = spawnSync("git", ["-C", sourceMirror, "rev-parse", "HEAD"], {
    encoding: "utf8",
    stdio: ["ignore", "pipe", "ignore"],
  });
  return result.status === 0 ? result.stdout.trim() : null;
}

export function assertAuthorityEvidenceSafe(serialized, redactionValues) {
  assertNoCredentialMaterial(serialized, { redactionValues });
  assert.doesNotMatch(serialized, /eyJ[A-Za-z0-9_-]+\.[A-Za-z0-9_-]+\./);
}

export function assertAuthorityRefreshTokenChanged(comparisons) {
  assert.equal(
    comparisons?.refreshTokenChanged,
    true,
    "managed authority refresh token did not rotate",
  );
}

export async function probeLiveAuthRefreshAuthority({
  allowAuthMutation = process.env.CODEX_ALLOW_AUTH_MUTATION === "1",
  authHome = process.env.CODEX_TEST_HOME ?? DEFAULT_AUTH_HOME,
  codexBin = process.env.CODEX_BIN,
  evidencePath = process.env.CODEX_AUTH_REFRESH_EVIDENCE ?? DEFAULT_EVIDENCE_PATH,
  makeDirectory = mkdir,
  model = process.env.CODEX_LIVE_PROBE_MODEL ?? DEFAULT_MODEL,
  sourceMirror = process.env.CODEX_SOURCE_MIRROR,
} = {}) {
  assert.equal(
    allowAuthMutation,
    true,
    "live authority refresh mutates the dedicated login; set CODEX_ALLOW_AUTH_MUTATION=1",
  );
  assert.equal(
    typeof codexBin === "string" && isAbsolute(codexBin),
    true,
    "live authority refresh requires CODEX_BIN to be an absolute pinned-image path",
  );

  const startedAt = new Date().toISOString();
  const before = await readManagedAuthSnapshot(authHome);
  const authority = new ManagedAuthRefreshAuthority({ authHome, codexBin });
  const concurrentCallers = 2;
  const [first, second] = await Promise.all([authority.refresh(), authority.refresh()]);
  assert.equal(authority.refreshExecutions, 1, "concurrent callers were not coalesced");
  assert.equal(first.generation, 1, "first refresh must create generation 1");
  assert.equal(second.generation, first.generation, "coalesced callers saw different generations");
  assertAuthorityRefreshTokenChanged(first.comparisons);
  assert.equal(
    second.accessToken === first.accessToken && second.accountId === first.accountId,
    true,
    "coalesced callers saw different credentials",
  );

  const after = await readManagedAuthSnapshot(authHome);
  assert.equal(after.accountId === before.accountId, true, "authority account identity changed");
  assert.equal(
    after.authFileFingerprint !== before.authFileFingerprint,
    true,
    "canonical authority auth did not change",
  );
  const rpcMethods = first.rpcAudit.map(({ method }) => method);
  assert.deepEqual(rpcMethods, ["initialize", "initialized", "account/read"]);
  assert.equal(
    rpcMethods.some((method) => method === "thread/start" || method === "turn/start"),
    false,
    "authority refresh unexpectedly started a model turn",
  );

  const workerEvidenceHome = await mkdtemp(join(tmpdir(), "portable-auth-refresh-worker-"));
  let workerReport;
  try {
    await makeDirectory(workerEvidenceHome, { recursive: true });
    workerReport = await probeLiveExternalAuth({
      authHome,
      codexBin,
      evidencePath: join(workerEvidenceHome, "worker-validation.json"),
      model,
    });
  } finally {
    await rm(workerEvidenceHome, { recursive: true, force: true });
  }

  const report = {
    schemaVersion: 1,
    probe: "managed-auth-refresh-authority-live",
    startedAt,
    completedAt: new Date().toISOString(),
    codexVersion: codexVersion(codexBin),
    sourceMirrorCommit: codexSourceCommit(sourceMirror),
    refreshMethod: "account/read",
    authority: {
      managedAuth: true,
      rpcMethods,
      modelTurnCount: 0,
      concurrentCallers,
      refreshExecutions: authority.refreshExecutions,
      generationBefore: 0,
      generationAfter: first.generation,
      accountContinuity: first.comparisons.accountContinuity,
      userContinuity: first.comparisons.userContinuity,
      lastRefreshAdvanced: first.comparisons.lastRefreshAdvanced,
      accessTokenChanged: first.comparisons.accessTokenChanged,
      refreshTokenChanged: first.comparisons.refreshTokenChanged,
      authFileChanged: first.comparisons.authFileChanged,
      canonicalPromotion: "atomic-rename",
      parentDirectorySynced: first.parentDirectorySynced,
      fileMode: first.fileMode,
      cleanupWarnings: first.cleanupWarnings,
    },
    workerValidation: {
      performedAfterAuthorityRefresh: true,
      loginType: workerReport.worker.loginType,
      model: workerReport.worker.model,
      turnStatus: workerReport.worker.turnStatus,
      authJsonCreated: workerReport.worker.authJsonCreated,
      sourceAuthUnchangedDuringWorkerValidation:
        workerReport.sourceAuth.unchangedDuringProbe,
    },
    result: "passed",
  };
  const serialized = `${JSON.stringify(report, null, 2)}\n`;
  assertAuthorityEvidenceSafe(serialized, first.redactionValues);
  await writeEvidenceSafely(evidencePath, serialized, before.authPath);
  return { ...report, evidenceWritten: true };
}
