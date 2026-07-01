import assert from "node:assert/strict";
import { lstat, mkdtemp, mkdir, readFile, readlink, rm, symlink, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { isAbsolute, join } from "node:path";
import test from "node:test";

import {
  PINNED_SOURCE_ANALYSIS_COMMIT,
  RECOVERY_SCENARIOS,
  assertProcessGroupTarget,
  assertRecoveryEvidenceSafe,
  copyStoppedTree,
  digestTree,
  interruptedTurnRecoveryFailureReport,
  probeInterruptedTurnRecovery,
  terminateAppServer,
  writeRecoveryEvidence,
} from "../src/interrupted-turn-recovery-probe.mjs";

function scenarioReport(kind) {
  return {
    kind,
    turnMaterialized: true,
    terminationObserved:
      kind === "logical_interrupt" ? "rpc-interrupt" : kind === "sigterm" ? "SIGTERM" : "SIGKILL",
    originalCompletionObserved: kind === "logical_interrupt",
    resumeSucceeded: true,
    sameThreadId: true,
    tailTurnStatus: "interrupted",
    threadReadAgrees: true,
    modelAbortMarker: kind === "logical_interrupt" ? "present" : "absent",
    ...(kind === "snapshot_restore"
      ? {
          snapshot: {
            kind: "stopped-tree-copy",
            sourceQuiesced: true,
            treeDigestMatched: true,
            workspaceDigestMatched: true,
          },
        }
      : {}),
  };
}

function completeEvidenceReport() {
  const scenarios = RECOVERY_SCENARIOS.map((kind) => {
    const { snapshot: _snapshot, ...scenario } = scenarioReport(kind);
    return scenario;
  });
  return {
    schemaVersion: 1,
    probe: "interrupted-turn-recovery",
    runtime: {
      codexVersion: "codex-cli 0.142.4",
      codexBinarySha256: "a".repeat(64),
      sourceAnalysisCommit: "b".repeat(40),
      platform: "darwin",
      arch: "arm64",
    },
    backend: {
      type: "loopback-held-responses-mock",
      realModelTurn: false,
      authMaterialUsed: false,
    },
    snapshot: scenarioReport("snapshot_restore").snapshot,
    scenarios,
    result: "passed",
  };
}

test("process-group target validation rejects unsafe identifiers", () => {
  assert.equal(assertProcessGroupTarget(4242, 7), 4242);
  for (const value of [undefined, 0, 1, -2, 1.5, 7]) {
    assert.throws(() => assertProcessGroupTarget(value, 7), /unsafe app-server process-group/);
  }
});

test("signal termination observes the requested signal and always cleans up", async () => {
  const signals = [];
  let cleanupCalls = 0;
  const client = {
    child: { pid: 4242 },
    exitPromise: Promise.resolve([null, "SIGTERM"]),
  };
  const result = await terminateAppServer(client, "SIGTERM", {
    abortClient: async () => {
      cleanupCalls += 1;
    },
    killProcess: (pid, signal) => signals.push([pid, signal]),
  });
  assert.deepEqual(result, { signal: "SIGTERM" });
  assert.deepEqual(signals, [[-4242, "SIGTERM"]]);
  assert.equal(cleanupCalls, 1);
  assert.equal(client.stopping, true);
});

test("signal termination preserves the primary failure over cleanup failure", async () => {
  let primary;
  try {
    await terminateAppServer(
      { child: { pid: 4242 }, exitPromise: Promise.resolve([9, null]) },
      "SIGKILL",
      {
        abortClient: async () => {
          throw new Error("cleanup sentinel");
        },
        killProcess: () => {},
      },
    );
  } catch (error) {
    primary = error;
  }
  assert(primary);
  assert.match(primary.message, /exited with code 9/);
  assert.match(primary.cleanupError.message, /cleanup sentinel/);
});

test("stopped-tree copy preserves a deterministic tree digest", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-copy-test-"));
  try {
    const source = join(root, "source");
    const destination = join(root, "destination");
    await mkdir(join(source, "nested"), { recursive: true, mode: 0o700 });
    await writeFile(join(source, "nested", "state.jsonl"), "{\"ok\":true}\n", {
      mode: 0o600,
    });
    const before = await digestTree(source);
    await copyStoppedTree({ ownedRoot: root, source, destination });
    assert.equal(await digestTree(destination), before);
    assert.equal(await readFile(join(destination, "nested", "state.jsonl"), "utf8"), "{\"ok\":true}\n");
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("stopped-tree copy preserves symlinks without following their targets", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-copy-link-test-"));
  try {
    const source = join(root, "source");
    const destination = join(root, "destination");
    await mkdir(source);
    await writeFile(join(root, "outside"), "sentinel");
    await symlink(join(root, "outside"), join(source, "link"));
    await copyStoppedTree({ ownedRoot: root, source, destination });
    assert.equal((await lstat(join(destination, "link"))).isSymbolicLink(), true);
    assert.equal(await readlink(join(destination, "link")), join(root, "outside"));
    assert.equal(await readFile(join(destination, "link"), "utf8"), "sentinel");
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("recovery evidence is allowlisted and rejects identifiers, paths, and prompts", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-evidence-test-"));
  try {
    const report = completeEvidenceReport();
    assert.doesNotThrow(() => assertRecoveryEvidenceSafe(report));
    for (const unsafe of [
      { ...report, cwd: "/Users/example/private" },
      { ...report, value: "123e4567-e89b-42d3-a456-426614174000" },
      { ...report, prompt: "portable recovery probe" },
      { ...report, marker: "<turn_aborted>" },
      { ...report, apiKey: "sk-secret-sentinel" },
      { ...report, path: "/var/folders/private-state" },
    ]) {
      assert.throws(() => assertRecoveryEvidenceSafe(unsafe), /unexpected fields|disallowed runtime data/);
    }
    const path = join(root, "evidence.json");
    await writeRecoveryEvidence(path, report);
    assert.deepEqual(JSON.parse(await readFile(path, "utf8")), report);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("probe report contains all four recovery scenarios without runtime identifiers", async () => {
  const calls = [];
  const report = await probeInterruptedTurnRecovery({
    codexBin: process.execPath,
    runScenario: async ({ kind }) => {
      calls.push(kind);
      return scenarioReport(kind);
    },
  });
  assert.deepEqual(calls, RECOVERY_SCENARIOS);
  assert.equal(report.schemaVersion, 1);
  assert.equal(report.runtime.sourceAnalysisCommit, PINNED_SOURCE_ANALYSIS_COMMIT);
  assert.equal(report.backend.realModelTurn, false);
  assert.equal(report.backend.authMaterialUsed, false);
  assert.equal(report.snapshot.kind, "stopped-tree-copy");
  assert.equal(report.scenarios.length, 4);
  assert.doesNotThrow(() => assertRecoveryEvidenceSafe(report));
});

test("probe requires an absolute binary and the complete scenario matrix", async () => {
  await assert.rejects(
    probeInterruptedTurnRecovery({ codexBin: "codex", runScenario: async () => ({}) }),
    /absolute pinned-image path/,
  );
  await assert.rejects(
    probeInterruptedTurnRecovery({
      codexBin: process.execPath,
      scenarios: ["sigkill"],
      runScenario: async () => ({}),
    }),
    /requires all scenarios/,
  );
});

test("failure report never serializes exception details", () => {
  assert.deepEqual(interruptedTurnRecoveryFailureReport(new Error("secret sentinel")), {
    error: { code: "recovery_probe_failed", retryable: false, type: "probe_failure" },
    result: "failed",
  });
});

const liveCodexBin = process.env.CODEX_BIN;
test(
  "installed Codex recovers all interrupted-turn scenarios through app-server",
  { skip: !liveCodexBin || !isAbsolute(liveCodexBin) ? "set absolute CODEX_BIN to run" : false },
  async () => {
    const report = await probeInterruptedTurnRecovery({ codexBin: liveCodexBin });
    assert.equal(report.result, "passed");
    assert.deepEqual(
      report.scenarios.map((scenario) => scenario.kind),
      RECOVERY_SCENARIOS,
    );
  },
);
