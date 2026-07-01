import assert from "node:assert/strict";
import {
  chmod,
  link,
  lstat,
  mkdtemp,
  mkdir,
  readFile,
  readlink,
  readdir,
  realpath,
  rm,
  stat,
  symlink,
  writeFile,
} from "node:fs/promises";
import { platform, tmpdir } from "node:os";
import { dirname, isAbsolute, join } from "node:path";
import test from "node:test";

import {
  PINNED_SOURCE_ANALYSIS_COMMIT,
  RECOVERY_SCENARIOS,
  assertNewTurnId,
  assertPortableDirectoryNames,
  assertProcessGroupTarget,
  assertRecoveryEvidenceSafe,
  copyStoppedTree,
  createRecoveryLayout,
  decodePortablePathBytes,
  digestTree,
  interruptedTurnRecoveryFailureReport,
  probeInterruptedTurnRecovery,
  removeTreeForCleanup,
  startRecoveryClient,
  terminateAppServer,
  verifyModelWorkspaceContext,
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
    threadReadIsolation: "copy-original-path-absent-held-tree-000",
    modelAbortMarker: kind === "logical_interrupt" ? "present" : "absent",
    ...(kind === "snapshot_restore"
      ? {
          snapshot: {
            kind: "stopped-tree-copy",
            appServerWorkspaceMatched: true,
            historicalWorkspaceRetained: true,
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
    schemaVersion: 2,
    probe: "interrupted-turn-recovery",
    runtime: {
      codexVersion: "codex-cli 0.142.4",
      codexBinarySha256: "a".repeat(64),
      binaryExecution: "private-read-only-copy",
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

test("recovery layout restores private directory modes under a restrictive umask", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-recovery-layout-test-"));
  try {
    const previousUmask = process.umask(0o777);
    let layout;
    try {
      layout = await createRecoveryLayout(root);
    } finally {
      process.umask(previousUmask);
    }
    for (const path of [layout.sessionRoot, layout.codexHome, layout.workspace]) {
      assert.equal((await stat(path)).mode & 0o777, 0o700);
    }
  } finally {
    await rm(root, { recursive: true, force: true });
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
  await assert.rejects(
    terminateAppServer(
      { child: { pid: 4242 }, exitPromise: Promise.resolve([null, "SIGINT"]) },
      "SIGINT",
      { abortClient: async () => {}, killProcess: () => {} },
    ),
    /permits only SIGTERM or SIGKILL/,
  );
});

test("signal termination tolerates an already absent process group", async () => {
  const missingProcessGroup = new Error("missing process group");
  missingProcessGroup.code = "ESRCH";
  const result = await terminateAppServer(
    { child: { pid: 4242 }, exitPromise: Promise.resolve([null, "SIGKILL"]) },
    "SIGKILL",
    {
      abortClient: async () => {},
      killProcess: () => {
        throw missingProcessGroup;
      },
    },
  );
  assert.deepEqual(result, { signal: "SIGKILL" });
});

test("signal termination rejects a different observed signal", async () => {
  await assert.rejects(
    terminateAppServer(
      { child: { pid: 4242 }, exitPromise: Promise.resolve([null, "SIGKILL"]) },
      "SIGTERM",
      { abortClient: async () => {}, killProcess: () => {} },
    ),
    /observed SIGKILL instead of SIGTERM/,
  );
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

test("client initialization failure aborts the detached app-server before rejection", async () => {
  const initializationFailure = new Error("initialize sentinel");
  const calls = [];
  const client = {
    async abort() {
      calls.push("abort");
    },
    async initialize() {
      calls.push("initialize");
      throw initializationFailure;
    },
    async start() {
      calls.push("start");
    },
  };
  await assert.rejects(
    startRecoveryClient({
      codexBin: "/pinned/codex",
      codexHome: "/owned/home",
      createClient: () => client,
      timeoutMs: 100,
    }),
    (error) => error === initializationFailure,
  );
  assert.deepEqual(calls, ["start", "initialize", "abort"]);
});

test("follow-up turn identity is present and distinct from the interrupted turn", () => {
  assert.equal(assertNewTurnId("turn-follow-up", "turn-interrupted"), "turn-follow-up");
  for (const turnId of [undefined, "", "turn-interrupted"]) {
    assert.throws(
      () => assertNewTurnId(turnId, "turn-interrupted"),
      /omitted its ID|reused the interrupted turn ID/,
    );
  }
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

test("stopped-tree copy populates read-only directories before restoring their mode", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-copy-read-only-test-"));
  try {
    const source = join(root, "source");
    const destination = join(root, "destination");
    await mkdir(join(source, "read-only"), { recursive: true });
    await writeFile(join(source, "read-only", "state"), "sentinel");
    await chmod(join(source, "read-only"), 0o500);
    await copyStoppedTree({ ownedRoot: root, source, destination });
    assert.equal((await lstat(join(destination, "read-only"))).mode & 0o777, 0o500);
    assert.equal(await readFile(join(destination, "read-only", "state"), "utf8"), "sentinel");
  } finally {
    await chmod(join(root, "source", "read-only"), 0o700).catch(() => {});
    await chmod(join(root, "destination", "read-only"), 0o700).catch(() => {});
    await rm(root, { recursive: true, force: true });
  }
});

test("stopped-tree copy overrides a restrictive umask while populating directories", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-copy-umask-test-"));
  const source = join(root, "source");
  const destination = join(root, "destination");
  try {
    await mkdir(join(source, "nested"), { recursive: true, mode: 0o700 });
    await writeFile(join(source, "nested", "state.jsonl"), "{\"ok\":true}\n", {
      mode: 0o600,
    });
    const previousUmask = process.umask(0o777);
    try {
      await copyStoppedTree({ ownedRoot: root, source, destination });
    } finally {
      process.umask(previousUmask);
    }
    assert.equal(
      await readFile(join(destination, "nested", "state.jsonl"), "utf8"),
      "{\"ok\":true}\n",
    );
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("stopped-tree copy rejects symlink-parent dot-dot paths outside the owned root", async () => {
  const container = await mkdtemp(join(tmpdir(), "portable-copy-parent-alias-test-"));
  try {
    const ownedRoot = join(container, "owned");
    const outsideRoot = join(container, "outside");
    await mkdir(ownedRoot);
    await mkdir(join(outsideRoot, "child"), { recursive: true });
    await mkdir(join(outsideRoot, "source"));
    await writeFile(join(outsideRoot, "source", "sentinel"), "outside");
    await symlink(join(outsideRoot, "child"), join(ownedRoot, "link"));
    const source = `${ownedRoot}/link/../source`;
    const destination = `${ownedRoot}/link/../destination`;
    await assert.rejects(
      copyStoppedTree({ ownedRoot, source, destination }),
      /source must be a direct owned child/,
    );
    await mkdir(join(ownedRoot, "source"));
    await writeFile(join(ownedRoot, "source", "sentinel"), "inside");
    await assert.rejects(
      copyStoppedTree({
        ownedRoot,
        source: join(ownedRoot, "source"),
        destination,
      }),
      /destination must be a direct owned child/,
    );
    await assert.rejects(lstat(join(outsideRoot, "destination")), /ENOENT/);
  } finally {
    await rm(container, { recursive: true, force: true });
  }
});

test("stopped-tree copy rejects terminal dot segments as owned children", async () => {
  const container = await mkdtemp(join(tmpdir(), "portable-copy-dot-segment-test-"));
  try {
    const ownedRoot = join(container, "owned");
    const source = join(ownedRoot, "source");
    await mkdir(source, { recursive: true });
    await writeFile(join(source, "sentinel"), "inside");
    for (const segment of [".", ".."]) {
      await assert.rejects(
        copyStoppedTree({
          ownedRoot,
          source: `${ownedRoot}/${segment}`,
          destination: join(ownedRoot, `source-${segment.length}`),
        }),
        /source must be a direct owned child/,
      );
      await assert.rejects(
        copyStoppedTree({
          ownedRoot,
          source,
          destination: `${ownedRoot}/${segment}`,
        }),
        /destination must be a direct owned child/,
      );
    }
  } finally {
    await rm(container, { recursive: true, force: true });
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

test("stopped-tree copy preserves relocatable relative symlinks", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-copy-relative-ok-test-"));
  try {
    const source = join(root, "source");
    const destination = join(root, "destination");
    await mkdir(join(source, "nested"), { recursive: true });
    await writeFile(join(source, "target"), "sentinel");
    await symlink("../target", join(source, "nested", "link"));
    await copyStoppedTree({ ownedRoot: root, source, destination });
    assert.equal(await readlink(join(destination, "nested", "link")), "../target");
    assert.equal(await readFile(join(destination, "nested", "link"), "utf8"), "sentinel");
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("stopped-tree copy rejects non-UTF-8 symlink targets", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-copy-non-utf8-link-test-"));
  try {
    const source = join(root, "source");
    const destination = join(root, "destination");
    await mkdir(source);
    await symlink(Buffer.from([0x2e, 0x2f, 0x80]), join(source, "non-utf8-link"));
    await assert.rejects(
      copyStoppedTree({ ownedRoot: root, source, destination }),
      /rejects non-UTF-8 symlink targets/,
    );
    await assert.rejects(lstat(destination), /ENOENT/);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("portable directory entry decoding rejects lossy UTF-8", () => {
  assert.equal(decodePortablePathBytes(Buffer.from("portable-name")), "portable-name");
  assert.throws(
    () => decodePortablePathBytes(Buffer.from([0x66, 0x80])),
    /rejects non-UTF-8 directory entry names/,
  );
});

test("portable directory names reject case and Unicode-normalization collisions", () => {
  assert.deepEqual(assertPortableDirectoryNames(["zeta", "Alpha"]), ["Alpha", "zeta"]);
  for (const entries of [
    ["README", "readme"],
    ["\uac00", "\u1100\u1161"],
  ]) {
    assert.throws(
      () => assertPortableDirectoryNames(entries),
      /case or Unicode-normalization name collisions/,
    );
  }
  for (const entry of ["\u03a3", "\u03c2", "Stra\u00dfe", "caf\u00e9"]) {
    assert.throws(
      () => assertPortableDirectoryNames([entry]),
      /non-ASCII cased directory names/,
    );
  }
});

test(
  "portable tree operations reject and clean up non-UTF-8 directory entries",
  { skip: platform() !== "linux" },
  async () => {
    const root = await mkdtemp(join(tmpdir(), "portable-copy-non-utf8-name-test-"));
    const source = join(root, "source");
    const destination = join(root, "destination");
    try {
      await mkdir(source);
      const rawPath = Buffer.concat([
        Buffer.from(source),
        Buffer.from("/"),
        Buffer.from([0x66, 0x80]),
      ]);
      await writeFile(rawPath, "sentinel");
      await assert.rejects(digestTree(source), /rejects non-UTF-8 directory entry names/);
      await assert.rejects(
        copyStoppedTree({ ownedRoot: root, source, destination }),
        /rejects non-UTF-8 directory entry names/,
      );
      await assert.rejects(lstat(destination), /ENOENT/);
      await removeTreeForCleanup(source);
      await assert.rejects(lstat(source), /ENOENT/);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  },
);

test("stopped-tree copy rejects absolute symlinks into the relocated source tree", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-copy-internal-link-test-"));
  try {
    const source = join(root, "source");
    const destination = join(root, "destination");
    await mkdir(source);
    await writeFile(join(source, "target"), "sentinel");
    await symlink(join(source, "target"), join(source, "absolute-internal-link"));
    await assert.rejects(
      copyStoppedTree({ ownedRoot: root, source, destination }),
      /rejects absolute symlinks into the source tree/,
    );
    await assert.rejects(lstat(destination), /ENOENT/);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("stopped-tree copy rejects canonical aliases of absolute internal symlinks", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-copy-canonical-link-test-"));
  try {
    const source = join(root, "source");
    const destination = join(root, "destination");
    await mkdir(source);
    await writeFile(join(source, "target"), "sentinel");
    const canonicalTarget = join(await realpath(source), "target");
    await symlink(canonicalTarget, join(source, "canonical-internal-link"));
    await assert.rejects(
      copyStoppedTree({ ownedRoot: root, source, destination }),
      /rejects absolute symlinks into the source tree/,
    );
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("stopped-tree copy rejects dangling absolute symlinks through source aliases", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-copy-dangling-link-test-"));
  try {
    const source = join(root, "source");
    const destination = join(root, "destination");
    await mkdir(source);
    await symlink(source, join(root, "source-alias"));
    await symlink(
      join(root, "source-alias", "missing"),
      join(source, "dangling-internal-link"),
    );
    await assert.rejects(
      copyStoppedTree({ ownedRoot: root, source, destination }),
      /rejects dangling absolute symlinks/,
    );
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("stopped-tree copy rejects absolute links that would become valid in the destination", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-copy-destination-link-test-"));
  try {
    const source = join(root, "source");
    const destination = join(root, "destination");
    await mkdir(source);
    await writeFile(join(source, "a-target"), "sentinel");
    await symlink(join(destination, "a-target"), join(source, "z-destination-link"));
    await assert.rejects(
      copyStoppedTree({ ownedRoot: root, source, destination }),
      /rejects absolute symlinks into the destination tree/,
    );
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("stopped-tree copy rejects relative symlinks whose meaning changes after relocation", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-copy-relative-link-test-"));
  try {
    const source = join(root, "session");
    const destination = join(root, "stopped-tree-copy");
    await mkdir(join(source, "workspace"), { recursive: true });
    await writeFile(join(source, "workspace", "target"), "sentinel");
    await symlink("../session/workspace/target", join(source, "non-relocatable-link"));
    await assert.rejects(
      copyStoppedTree({ ownedRoot: root, source, destination }),
      /rejects non-relocatable relative symlinks/,
    );
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("stopped-tree copy rejects special permission bits and hard-link topology", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-copy-metadata-test-"));
  try {
    const specialSource = join(root, "special-source");
    await mkdir(specialSource, { mode: 0o700 });
    await chmod(specialSource, 0o1700);
    await assert.rejects(
      copyStoppedTree({
        ownedRoot: root,
        source: specialSource,
        destination: join(root, "special-destination"),
      }),
      /rejects special permission bits/,
    );

    const hardLinkSource = join(root, "hard-link-source");
    await mkdir(hardLinkSource);
    await writeFile(join(hardLinkSource, "first"), "sentinel");
    await link(join(hardLinkSource, "first"), join(hardLinkSource, "second"));
    await assert.rejects(
      copyStoppedTree({
        ownedRoot: root,
        source: hardLinkSource,
        destination: join(root, "hard-link-destination"),
      }),
      /rejects hard-linked files/,
    );
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("recovery evidence is allowlisted and rejects identifiers, paths, and prompts", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-evidence-test-"));
  try {
    const report = completeEvidenceReport();
    assert.doesNotThrow(() => assertRecoveryEvidenceSafe(report));
    assert.throws(
      () => assertRecoveryEvidenceSafe({ ...report, schemaVersion: 1 }),
      /1 !== 2/,
    );
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
    for (const codexVersion of [
      "sk-secret-sentinel",
      "123e4567-e89b-42d3-a456-426614174000",
      "/var/folders/private-state",
      "/srv/portable/private-state",
      "codex-cli 0.142.4+builder01.corp.internal",
    ]) {
      assert.throws(
        () => assertRecoveryEvidenceSafe({
          ...report,
          runtime: { ...report.runtime, codexVersion },
        }),
        /disallowed runtime data|did not match/,
      );
    }
    const escapedSecret = JSON.stringify(report).replace(
      "codex-cli 0.142.4",
      "codex-cli 0.142.4-s\\u006b-secret-sentinel",
    );
    assert.throws(
      () => assertRecoveryEvidenceSafe(escapedSecret),
      /disallowed runtime data/,
    );
    const duplicateEscapedSecret = JSON.stringify(report).replace(
      '"codexVersion":"codex-cli 0.142.4"',
      '"codexVersion":"s\\u006b-secret-sentinel","codexVersion":"codex-cli 0.142.4"',
    );
    assert.throws(
      () => assertRecoveryEvidenceSafe(duplicateEscapedSecret),
      /disallowed runtime data/,
    );
    const nestedSecret = String.raw`{"token":"s\u006b-secret-sentinel"}`;
    const duplicateNestedSecret = JSON.stringify(report).replace(
      '"codexVersion":"codex-cli 0.142.4"',
      `"codexVersion":${JSON.stringify(nestedSecret)},"codexVersion":"codex-cli 0.142.4"`,
    );
    assert.throws(
      () => assertRecoveryEvidenceSafe(duplicateNestedSecret),
      /disallowed runtime data/,
    );
    const path = join(root, "evidence.json");
    await Promise.all([
      writeRecoveryEvidence(path, report),
      writeRecoveryEvidence(path, report),
    ]);
    assert.deepEqual(JSON.parse(await readFile(path, "utf8")), report);
    assert.deepEqual(await readdir(root), ["evidence.json"]);

    const nestedPath = join(root, "new-private", "nested", "evidence.json");
    const previousUmask = process.umask(0o777);
    try {
      await writeRecoveryEvidence(nestedPath, report);
    } finally {
      process.umask(previousUmask);
    }
    for (const directory of [dirname(nestedPath), dirname(dirname(nestedPath))]) {
      assert.equal((await stat(directory)).mode & 0o777, 0o700);
    }
    assert.equal((await stat(nestedPath)).mode & 0o777, 0o600);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("model workspace evidence distinguishes canonical history from active context", async () => {
  const previousWorkspace = "/var/session/workspace";
  const previousWorkspaceCanonical = "/private/var/session/workspace";
  const workspace = "/var/restored-session/workspace";
  const workspaceCanonical = "/private/var/restored-session/workspace";
  const canonicalizePath = async (path) =>
    path === workspace || path === workspaceCanonical ? workspaceCanonical : path;
  const environmentMessage = (cwd) => ({
    type: "message",
    role: "user",
    content: [
      {
        type: "input_text",
        text: `<environment_context>\n<cwd>${cwd}</cwd>\n</environment_context>`,
      },
    ],
  });
  const requestBody = JSON.stringify({
    input: [
      environmentMessage(previousWorkspaceCanonical),
      { type: "message", role: "user", content: [{ type: "input_text", text: "turn" }] },
      environmentMessage(workspaceCanonical),
    ],
  });
  assert.deepEqual(
    await verifyModelWorkspaceContext(requestBody, {
      canonicalizePath,
      previousWorkspace,
      previousWorkspaceCanonical,
      workspace,
    }),
    { activeWorkspaceMatched: true, historicalWorkspaceRetained: true },
  );
  await assert.rejects(
    verifyModelWorkspaceContext(requestBody, {
      canonicalizePath,
      previousWorkspace,
      previousWorkspaceCanonical,
      workspace: "/unexpected/workspace",
    }),
    /latest model workspace context did not match/,
  );
  const missingPath = new Error("missing workspace");
  missingPath.code = "ENOENT";
  await assert.rejects(
    verifyModelWorkspaceContext(
      JSON.stringify({ input: [environmentMessage("/deleted/workspace")] }),
      {
        canonicalizePath: async (path) => {
          if (path === "/deleted/workspace") throw missingPath;
          return canonicalizePath(path);
        },
        previousWorkspace,
        previousWorkspaceCanonical,
        workspace,
      },
    ),
    /latest model workspace context did not match/,
  );
  const activeOnlyRequest = JSON.stringify({
    input: [environmentMessage(workspaceCanonical)],
  });
  await assert.rejects(
    verifyModelWorkspaceContext(activeOnlyRequest, {
      canonicalizePath,
      previousWorkspace,
      previousWorkspaceCanonical,
      workspace,
    }),
    /omitted the immutable historical workspace/,
  );
});

test("probe report contains all four recovery scenarios without runtime identifiers", async () => {
  const calls = [];
  const scenarioBinaries = [];
  let versionBinary;
  const report = await probeInterruptedTurnRecovery({
    codexBin: process.execPath,
    readCodexVersion: (codexBin) => {
      versionBinary = codexBin;
      return "codex-cli 0.142.4";
    },
    runScenario: async ({ codexBin, kind }) => {
      calls.push(kind);
      scenarioBinaries.push(codexBin);
      return scenarioReport(kind);
    },
  });
  assert.deepEqual(calls, RECOVERY_SCENARIOS);
  assert.equal(report.schemaVersion, 2);
  assert.equal(report.runtime.binaryExecution, "private-read-only-copy");
  assert(scenarioBinaries.every((binary) => binary === versionBinary));
  assert.notEqual(versionBinary, process.execPath);
  await assert.rejects(lstat(versionBinary), /ENOENT/);
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
