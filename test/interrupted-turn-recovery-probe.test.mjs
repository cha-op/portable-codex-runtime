import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
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
  rename,
  rm,
  stat,
  symlink,
  writeFile,
} from "node:fs/promises";
import { platform, tmpdir } from "node:os";
import { basename, dirname, isAbsolute, join } from "node:path";
import test from "node:test";

import {
  PINNED_SOURCE_ANALYSIS_COMMIT,
  RECOVERY_SCENARIOS,
  assertNewTurnId,
  assertPortableDirectoryNames as recoveryProbeAssertPortableDirectoryNames,
  assertProcessGroupTarget,
  assertRecoveryEvidenceSafe,
  copyStoppedTree as recoveryProbeCopyStoppedTree,
  createRecoveryLayout,
  decodePortablePathBytes as recoveryProbeDecodePortablePathBytes,
  digestTree as recoveryProbeDigestTree,
  interruptedTurnRecoveryFailureReport,
  inspectLinuxRecoveryAcl as recoveryProbeInspectLinuxRecoveryAcl,
  parseDarwinMountTable as recoveryProbeParseDarwinMountTable,
  parseLinuxGetfacl as recoveryProbeParseLinuxGetfacl,
  parseLinuxMountInfo as recoveryProbeParseLinuxMountInfo,
  probeInterruptedTurnRecovery,
  removeTreeForCleanup as recoveryProbeRemoveTreeForCleanup,
  runInterruptedTurnRecoveryCli,
  startRecoveryClient,
  terminateAppServer,
  verifyModelWorkspaceContext,
  writeRecoveryEvidence as writeRecoveryEvidenceWithAcl,
} from "../src/interrupted-turn-recovery-probe.mjs";
import {
  assertPortableDirectoryNames,
  copyStoppedTree as copyStoppedTreeWithAcl,
  copyStoppedTreeBetweenRoots as copyStoppedTreeBetweenRootsWithAcl,
  decodePortablePathBytes,
  digestStoppedTreeIdentities,
  digestTree,
  inspectLinuxRecoveryAcl,
  openStoppedTreeRootAuthority,
  parseDarwinMountTable,
  parseLinuxGetfacl,
  parseLinuxMountInfo,
  removeTreeForCleanup,
  stoppedTreeContainsAnyIdentity,
  stoppedTreesShareAnyIdentity,
  syncStoppedTree,
} from "../src/stopped-tree.mjs";

const TEST_OBJECT_IDENTITY_SCHEME = "test-object-generation-v1";

async function inspectTestPersistentObjectIdentity(path) {
  const metadata = await lstat(path, { bigint: true });
  return {
    device: metadata.dev.toString(),
    inode: metadata.ino.toString(),
    objectId: `test-object-${metadata.dev}-${metadata.ino}-${metadata.birthtimeNs}`,
  };
}

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
            modeledTreeDigestMatched: true,
            sourceQuiesced: true,
            workspaceModeledDigestMatched: true,
          },
        }
      : {}),
  };
}

async function assertRetainedFailedDestination(destination) {
  assert.equal(
    (await lstat(destination)).isDirectory(),
    true,
    "a failed copy must retain its partial destination for trusted-owner cleanup",
  );
}

const TRUSTED_RECOVERY_ACL = {
  inspectExecutableAncestorAcl: async () => false,
  inspectExecutableRootAcl: async () => false,
};

const copyStoppedTree = (options) =>
  copyStoppedTreeWithAcl({
    inspectOwnedRootAcl: async () => false,
    inspectOwnedRootAncestorAcl: async () => false,
    ...options,
  });

const copyStoppedTreeBetweenRoots = (options) =>
  copyStoppedTreeBetweenRootsWithAcl({
    inspectOwnedRootAcl: async () => false,
    inspectOwnedRootAncestorAcl: async () => false,
    ...options,
  });

const writeRecoveryEvidence = (path, report, options = {}) =>
  writeRecoveryEvidenceWithAcl(path, report, {
    inspectEvidenceAncestorAcl: async () => false,
    inspectEvidenceDirectoryAcl: async () => false,
    ...options,
  });

function completeEvidenceReport() {
  const scenarios = RECOVERY_SCENARIOS.map((kind) => {
    const { snapshot: _snapshot, ...scenario } = scenarioReport(kind);
    return scenario;
  });
  return {
    schemaVersion: 5,
    probe: "interrupted-turn-recovery",
    runtime: {
      codexVersion: "codex-cli 0.142.4",
      codexBinarySha256: "a".repeat(64),
      binaryExecution: "private-read-only-copy",
      sourceAnalysisCommit: "b".repeat(40),
      platform: "darwin",
      launcherArch: "arm64",
    },
    backend: {
      type: "loopback-held-responses-mock",
      realModelTurnConfigured: false,
      credentialInputProvisioned: false,
      outboundNetworkIsolated: false,
    },
    snapshot: scenarioReport("snapshot_restore").snapshot,
    scenarios,
    result: "passed",
  };
}

function privateEvidenceFileMetadata() {
  const currentUid = process.geteuid?.() ?? process.getuid?.();
  assert.notEqual(currentUid, undefined);
  return {
    dev: 1n,
    ino: 1n,
    isFile: () => true,
    mode: 0o600n,
    nlink: 1n,
    uid: BigInt(currentUid),
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

test("signal termination drains stdout and child close before abort cleanup", async () => {
  let abortCalls = 0;
  let notificationObserved = false;
  let resolveChildClose;
  let resolveStdoutClose;
  const client = {
    child: { pid: 4242 },
    childClosePromise: new Promise((resolveClose) => {
      resolveChildClose = resolveClose;
    }),
    exitPromise: Promise.resolve([null, "SIGTERM"]),
    stdoutClosePromise: new Promise((resolveClose) => {
      resolveStdoutClose = resolveClose;
    }),
  };
  const termination = terminateAppServer(client, "SIGTERM", {
    abortClient: async () => {
      abortCalls += 1;
      assert.equal(notificationObserved, true);
    },
    killProcess: () => {},
  });
  await Promise.resolve();
  assert.equal(abortCalls, 0);
  notificationObserved = true;
  resolveStdoutClose();
  await Promise.resolve();
  assert.equal(abortCalls, 0);
  resolveChildClose([null, "SIGTERM"]);
  assert.deepEqual(await termination, { signal: "SIGTERM" });
  assert.equal(abortCalls, 1);
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

test("recovery probe preserves stopped-tree export compatibility", () => {
  for (const [probeExport, stoppedTreeExport] of [
    [recoveryProbeAssertPortableDirectoryNames, assertPortableDirectoryNames],
    [recoveryProbeCopyStoppedTree, copyStoppedTreeWithAcl],
    [recoveryProbeDecodePortablePathBytes, decodePortablePathBytes],
    [recoveryProbeDigestTree, digestTree],
    [recoveryProbeInspectLinuxRecoveryAcl, inspectLinuxRecoveryAcl],
    [recoveryProbeParseDarwinMountTable, parseDarwinMountTable],
    [recoveryProbeParseLinuxGetfacl, parseLinuxGetfacl],
    [recoveryProbeParseLinuxMountInfo, parseLinuxMountInfo],
    [recoveryProbeRemoveTreeForCleanup, removeTreeForCleanup],
  ]) {
    assert.equal(probeExport, stoppedTreeExport);
  }
});

test("stopped-tree root authority exposes the existing pinned private-root contract", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-root-authority-test-"));
  let authority;
  try {
    authority = await openStoppedTreeRootAuthority(root, {
      inspectOwnedRootAcl: async () => false,
      inspectOwnedRootAncestorAcl: async () => false,
    });
    assert.equal(authority.path, await realpath(root));
    assert.equal((await authority.handle.stat()).isDirectory(), true);
    await authority.assertCurrent();
  } finally {
    await authority?.handle.close();
    await rm(root, { recursive: true, force: true });
  }
});

test("stopped-tree copy supports distinct private source and destination roots", async () => {
  const container = await mkdtemp(join(tmpdir(), "portable-cross-root-copy-test-"));
  try {
    const sourceOwnedRoot = join(container, "source-root");
    const destinationOwnedRoot = join(container, "destination-root");
    const source = join(sourceOwnedRoot, "source");
    const destination = join(destinationOwnedRoot, "destination");
    await mkdir(join(source, "nested"), { recursive: true, mode: 0o700 });
    await mkdir(destinationOwnedRoot, { mode: 0o700 });
    await writeFile(join(source, "nested", "state"), "portable", { mode: 0o600 });

    await copyStoppedTreeBetweenRoots({
      destination,
      destinationOwnedRoot,
      source,
      sourceOwnedRoot,
    });

    assert.equal(await digestTree(destination), await digestTree(source));
    assert.equal(await readFile(join(destination, "nested", "state"), "utf8"), "portable");
  } finally {
    await rm(container, { recursive: true, force: true });
  }
});

test("stopped-tree cross-root copy rejects nested owned roots", async () => {
  const sourceOwnedRoot = await mkdtemp(
    join(tmpdir(), "portable-nested-root-copy-test-"),
  );
  try {
    const source = join(sourceOwnedRoot, "source");
    const destinationOwnedRoot = join(sourceOwnedRoot, "destination-root");
    const destination = join(destinationOwnedRoot, "destination");
    await mkdir(source, { mode: 0o700 });
    await mkdir(destinationOwnedRoot, { mode: 0o700 });
    await assert.rejects(
      copyStoppedTreeBetweenRoots({
        destination,
        destinationOwnedRoot,
        source,
        sourceOwnedRoot,
      }),
      /rejects nested owned roots/,
    );
    await assert.rejects(lstat(destination), /ENOENT/);
  } finally {
    await rm(sourceOwnedRoot, { recursive: true, force: true });
  }
});

test("stopped-tree cross-root copy rejects distinct paths to one root identity", async () => {
  const container = await mkdtemp(join(tmpdir(), "portable-root-alias-copy-test-"));
  try {
    const realParent = join(container, "real-parent");
    const aliasParent = join(container, "alias-parent");
    const sourceOwnedRoot = join(realParent, "owned-root");
    const destinationOwnedRoot = join(aliasParent, "owned-root");
    await mkdir(sourceOwnedRoot, { recursive: true, mode: 0o700 });
    await symlink("real-parent", aliasParent);
    const source = join(sourceOwnedRoot, "source");
    const destination = join(destinationOwnedRoot, "destination");
    await mkdir(source, { mode: 0o700 });

    await assert.rejects(
      copyStoppedTreeBetweenRoots({
        destination,
        destinationOwnedRoot,
        source,
        sourceOwnedRoot,
      }),
      /rejects distinct owned-root identity aliases/,
    );
    await assert.rejects(lstat(destination), /ENOENT/);
  } finally {
    await rm(container, { recursive: true, force: true });
  }
});

test("legacy stopped-tree copy remains a same-root compatibility wrapper", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-copy-wrapper-test-"));
  try {
    const source = join(root, "source");
    const destination = join(root, "destination");
    const directDestination = join(root, "direct-destination");
    await mkdir(source, { mode: 0o700 });
    await writeFile(join(source, "state"), "wrapper", { mode: 0o600 });
    await copyStoppedTreeBetweenRoots({
      destination: directDestination,
      destinationOwnedRoot: root,
      source,
      sourceOwnedRoot: root,
    });
    await copyStoppedTree({ ownedRoot: root, source, destination });
    assert.equal(
      await readFile(join(directDestination, "state"), "utf8"),
      "wrapper",
    );
    assert.equal(await readFile(join(destination, "state"), "utf8"), "wrapper");
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("stopped-tree sync fsyncs files and directories post-order without opening symlinks", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-tree-sync-order-test-"));
  try {
    const nested = join(root, "nested");
    const file = join(nested, "state");
    const linkPath = join(root, "state-link");
    await mkdir(nested, { mode: 0o700 });
    await writeFile(file, "state", { mode: 0o600 });
    await symlink("nested/state", linkPath);
    const opened = [];
    const synced = [];

    await syncStoppedTree(root, {
      beforeEntryOpen: async (path) => opened.push(path),
      listMountPoints: async () => [],
      syncDirectory: async (_handle, path) => synced.push(["directory", path]),
      syncFile: async (_handle, path) => synced.push(["file", path]),
    });

    assert.deepEqual(opened, [root, nested, file]);
    assert.deepEqual(synced, [
      ["file", file],
      ["directory", nested],
      ["directory", root],
    ]);
    assert.equal(await readlink(linkPath), "nested/state");
    await assert.rejects(
      syncStoppedTree(linkPath, { listMountPoints: async () => [] }),
      /sync root must be a directory/,
    );
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("stopped-tree sync rejects file replacement races and propagates fsync failure", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-tree-sync-race-test-"));
  try {
    const file = join(root, "state");
    const displaced = join(root, "displaced");
    await writeFile(file, "original", { mode: 0o600 });
    await assert.rejects(
      syncStoppedTree(root, {
        afterEntryOpen: async (path) => {
          if (path !== file) return;
          await rename(file, displaced);
          await writeFile(file, "replacement", { mode: 0o600 });
        },
        listMountPoints: async () => [],
        syncDirectory: async () => {},
        syncFile: async () => {},
      }),
      /rejects file (?:identity|metadata) changes/,
    );

    await rm(file);
    await rename(displaced, file);
    await assert.rejects(
      syncStoppedTree(root, {
        listMountPoints: async () => [],
        syncDirectory: async () => {},
        syncFile: async () => {
          throw new Error("injected fsync failure");
        },
      }),
      /injected fsync failure/,
    );
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("portable tree root mounts require explicit opt-in while nested mounts remain rejected", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-root-mount-opt-in-test-"));
  try {
    const source = join(root, "source");
    const nested = join(source, "nested");
    await mkdir(nested, { recursive: true, mode: 0o700 });
    await writeFile(join(nested, "state"), "state", { mode: 0o600 });
    const rootMount = async () => [source];
    const nestedMount = async () => [source, nested];

    await assert.rejects(
      digestTree(source, { listMountPoints: rootMount }),
      /rejects nested mount points/,
    );
    await digestTree(source, {
      allowRootMount: true,
      listMountPoints: rootMount,
    });
    await assert.rejects(
      syncStoppedTree(source, { listMountPoints: rootMount }),
      /rejects nested mount points/,
    );
    await syncStoppedTree(source, {
      allowRootMount: true,
      listMountPoints: rootMount,
    });
    await assert.rejects(
      syncStoppedTree(source, {
        allowRootMount: true,
        listMountPoints: nestedMount,
      }),
      /rejects nested mount points/,
    );

    const rejectedDestination = join(root, "rejected-destination");
    await assert.rejects(
      copyStoppedTree({
        destination: rejectedDestination,
        listMountPoints: rootMount,
        ownedRoot: root,
        source,
      }),
      /rejects nested mount points/,
    );
    await copyStoppedTree({
      allowSourceRootMount: true,
      destination: join(root, "destination"),
      listMountPoints: rootMount,
      ownedRoot: root,
      source,
    });
  } finally {
    await rm(root, { recursive: true, force: true });
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
    await mkdir(ownedRoot, { mode: 0o700 });
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

test("stopped-tree copy requires a private coordinator-owned root", async () => {
  const container = await mkdtemp(join(tmpdir(), "portable-copy-owned-root-test-"));
  try {
    const ownedRoot = join(container, "owned");
    const source = join(ownedRoot, "source");
    await mkdir(source, { recursive: true });
    await chmod(ownedRoot, 0o750);
    await assert.rejects(
      copyStoppedTree({
        ownedRoot,
        source,
        destination: join(ownedRoot, "destination"),
      }),
      /owned root must have mode 0700/,
    );
  } finally {
    await rm(container, { recursive: true, force: true });
  }
});

test("stopped-tree copy rejects an owned root below an unsafe ancestor", async () => {
  const container = await mkdtemp(join(tmpdir(), "portable-copy-owned-root-parent-test-"));
  const unsafeParent = join(container, "unsafe-parent");
  const ownedRoot = join(unsafeParent, "owned");
  const source = join(ownedRoot, "source");
  try {
    await mkdir(source, { recursive: true });
    await chmod(unsafeParent, 0o777);
    await chmod(ownedRoot, 0o700);
    await assert.rejects(
      copyStoppedTree({
        ownedRoot,
        source,
        destination: join(ownedRoot, "destination"),
      }),
      /owned root ancestor chain is not trusted/,
    );
    await assert.rejects(lstat(join(ownedRoot, "destination")), /ENOENT/);
  } finally {
    await chmod(unsafeParent, 0o700).catch(() => {});
    await rm(container, { recursive: true, force: true });
  }
});

test("stopped-tree copy rejects replacement of its held owned root", async () => {
  const container = await mkdtemp(join(tmpdir(), "portable-copy-owned-root-race-test-"));
  const ownedRoot = join(container, "owned");
  const displacedRoot = join(container, "displaced-owned");
  const source = join(ownedRoot, "source");
  const destination = join(ownedRoot, "destination");
  try {
    await mkdir(source, { recursive: true });
    await chmod(ownedRoot, 0o700);
    await writeFile(join(source, "sentinel"), "source");
    await assert.rejects(
      copyStoppedTree({
        ownedRoot,
        source,
        destination,
        afterDestinationValidation: async () => {
          await rename(ownedRoot, displacedRoot);
          await mkdir(ownedRoot, { mode: 0o700 });
          await writeFile(join(ownedRoot, "replacement"), "preserve");
        },
      }),
      /owned-root identity or permission changes/,
    );
    assert.equal(await readFile(join(ownedRoot, "replacement"), "utf8"), "preserve");
    await assert.rejects(lstat(destination), /ENOENT/);
  } finally {
    await rm(container, { recursive: true, force: true });
  }
});

test("stopped-tree copy fails closed on owned-root ACL inspection", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-copy-owned-root-acl-test-"));
  try {
    const source = join(root, "source");
    await mkdir(source);
    await writeFile(join(source, "sentinel"), "inside");
    const cases = [
      {
        inspectOwnedRootAcl: async () => true,
        message: /owned root must not have extended access controls/,
      },
      {
        inspectOwnedRootAcl: async () => {
          throw new Error("sensitive ACL principal");
        },
        message: /owned root ACL could not be validated/,
      },
    ];
    for (const [index, aclCase] of cases.entries()) {
      const destination = join(root, `destination-${index}`);
      await assert.rejects(
        copyStoppedTree({
          ownedRoot: root,
          source,
          destination,
          inspectOwnedRootAcl: aclCase.inspectOwnedRootAcl,
        }),
        (error) => aclCase.message.test(error.message) && !error.message.includes("sensitive"),
      );
      await assert.rejects(lstat(destination), /ENOENT/);
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("stopped-tree copy fails closed on owned-root ancestor ACL inspection", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-copy-owned-root-parent-acl-test-"));
  try {
    const source = join(root, "source");
    await mkdir(source);
    await writeFile(join(source, "sentinel"), "inside");
    const cases = [
      {
        inspectOwnedRootAncestorAcl: async () => true,
        message: /owned root ancestor chain has unsafe access controls/,
      },
      {
        inspectOwnedRootAncestorAcl: async () => {
          throw new Error("sensitive ancestor ACL principal");
        },
        message: /owned root ancestor ACL could not be validated/,
      },
    ];
    for (const [index, aclCase] of cases.entries()) {
      const destination = join(root, `destination-ancestor-${index}`);
      await assert.rejects(
        copyStoppedTree({
          ownedRoot: root,
          source,
          destination,
          inspectOwnedRootAncestorAcl: aclCase.inspectOwnedRootAncestorAcl,
        }),
        (error) => aclCase.message.test(error.message) && !error.message.includes("sensitive"),
      );
      await assert.rejects(lstat(destination), /ENOENT/);
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test(
  "stopped-tree copy rejects a real macOS allow ACL on its owned root",
  { skip: platform() !== "darwin" },
  async (t) => {
    const root = await mkdtemp(join(tmpdir(), "portable-copy-owned-root-real-acl-test-"));
    try {
      const source = join(root, "source");
      const destination = join(root, "destination");
      await mkdir(source);
      await writeFile(join(source, "sentinel"), "inside");
      const result = spawnSync(
        "/bin/chmod",
        ["+a", "everyone allow add_file,add_subdirectory,delete_child", root],
        { encoding: "utf8" },
      );
      if (result.error?.code === "ENOENT") {
        t.skip("/bin/chmod is unavailable for ACL setup");
        return;
      }
      if (result.status !== 0) {
        t.skip("the test filesystem does not support a macOS allow ACL");
        return;
      }
      assert.equal((await stat(root)).mode & 0o777, 0o700);
      await assert.rejects(
        copyStoppedTreeWithAcl({ ownedRoot: root, source, destination }),
        /owned root must not have extended access controls/,
      );
      await assert.rejects(lstat(destination), /ENOENT/);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  },
);

test("portable tree operations reject declared nested mount boundaries", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-copy-mount-boundary-test-"));
  try {
    const source = join(root, "source");
    const mounted = join(source, "mounted");
    const destination = join(root, "destination");
    await mkdir(mounted, { recursive: true });
    await writeFile(join(mounted, "sentinel"), "preserve");
    const listMountPoints = async () => [mounted];
    await assert.rejects(
      digestTree(source, { listMountPoints }),
      /rejects nested mount points/,
    );
    await assert.rejects(
      copyStoppedTree({ ownedRoot: root, source, destination, listMountPoints }),
      /rejects nested mount points/,
    );
    await assert.rejects(lstat(destination), /ENOENT/);
    await assert.rejects(
      removeTreeForCleanup(source, { listMountPoints }),
      /rejects nested mount points/,
    );
    assert.equal(await readFile(join(mounted, "sentinel"), "utf8"), "preserve");
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("stopped-tree copy rejects terminal dot segments as owned children", async () => {
  const container = await mkdtemp(join(tmpdir(), "portable-copy-dot-segment-test-"));
  try {
    const ownedRoot = join(container, "owned");
    const source = join(ownedRoot, "source");
    await mkdir(ownedRoot, { mode: 0o700 });
    await mkdir(source);
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

test("stopped-tree copy rejects external identity aliases into the source tree", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-copy-source-alias-test-"));
  try {
    const source = join(root, "source");
    const sourceSubtree = join(source, "subtree");
    const externalAlias = join(root, "external-alias");
    const destination = join(root, "destination");
    await mkdir(sourceSubtree, { recursive: true });
    await mkdir(externalAlias);
    await writeFile(join(sourceSubtree, "source-sentinel"), "source");
    await writeFile(join(externalAlias, "external-sentinel"), "external");
    await symlink(join(externalAlias, "external-sentinel"), join(source, "alias-link"));
    const sourceSubtreeIdentity = await lstat(sourceSubtree, { bigint: true });
    const canonicalExternalAlias = await realpath(externalAlias);

    await assert.rejects(
      copyStoppedTree({
        ownedRoot: root,
        source,
        destination,
        inspectSymlinkPath: async (candidate, options) =>
          (await realpath(candidate)) === canonicalExternalAlias
            ? sourceSubtreeIdentity
            : lstat(candidate, options),
      }),
      /rejects absolute symlinks into the source tree/,
    );
    await assertRetainedFailedDestination(destination);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("stopped-tree copy rejects external identity aliases to source files", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-copy-source-file-alias-test-"));
  try {
    const source = join(root, "source");
    const sourceFile = join(source, "source-file");
    const external = join(root, "external");
    const externalAlias = join(external, "external-alias");
    const destination = join(root, "destination");
    await mkdir(source);
    await mkdir(external);
    await writeFile(sourceFile, "source");
    await writeFile(externalAlias, "external");
    await symlink(externalAlias, join(source, "alias-link"));
    const sourceFileIdentity = await lstat(sourceFile, { bigint: true });
    const canonicalExternalAlias = await realpath(externalAlias);

    await assert.rejects(
      copyStoppedTree({
        ownedRoot: root,
        source,
        destination,
        inspectSymlinkPath: async (candidate, options) =>
          (await realpath(candidate)) === canonicalExternalAlias
            ? sourceFileIdentity
            : lstat(candidate, options),
      }),
      /rejects absolute symlinks into the source tree/,
    );
    await assertRetainedFailedDestination(destination);
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
    await assertRetainedFailedDestination(destination);
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

test("mount table parsers preserve their platform-specific absolute path encoding", () => {
  assert.deepEqual(
    parseLinuxMountInfo(
      Buffer.from(
        "36 29 0:32 / / rw,relatime - overlay overlay rw\n" +
          "37 36 0:33 / /private/session/mounted\\040tree rw - tmpfs tmpfs rw\n",
      ),
    ),
    ["/", "/private/session/mounted tree"],
  );
  assert.deepEqual(
    parseDarwinMountTable(
      Buffer.from(
        "/dev/disk3s1s1 on / (apfs, local)\n" +
          "map on /private/session/mounted tree (autofs, local)\n" +
          "literal on /private/session/backslash\\040tree (apfs, local)\n",
      ),
    ),
    ["/", "/private/session/mounted tree", "/private/session/backslash\\040tree"],
  );
});

test("Darwin mount table parsing fails closed on unescaped separator text in paths", () => {
  for (const ambiguous of [
    "map on /private/session/dir on child (autofs, local)\n",
    "map on /private/session/dir ( child (autofs, local)\n",
  ]) {
    assert.throws(
      () => parseDarwinMountTable(Buffer.from(ambiguous)),
      /ambiguous mount path/,
    );
  }
});

test("mount table parsing fails closed before lossy UTF-8 decoding", () => {
  assert.throws(() => parseLinuxMountInfo("text"), /Linux mountinfo must be bytes/);
  assert.throws(() => parseDarwinMountTable("text"), /Darwin mount table must be bytes/);
  const invalidUtf8 = Buffer.from([0x2f, 0x80, 0x0a]);
  assert.throws(
    () => parseLinuxMountInfo(invalidUtf8),
    /Linux mountinfo contains non-UTF-8 bytes/,
  );
  assert.throws(
    () => parseDarwinMountTable(invalidUtf8),
    /Darwin mount table contains non-UTF-8 bytes/,
  );
});

test("mount table parsing fails closed on empty or incomplete output", () => {
  for (const value of [Buffer.alloc(0), Buffer.from("\n")]) {
    assert.throws(() => parseLinuxMountInfo(value), /Linux mountinfo omitted the root mount/);
    assert.throws(
      () => parseDarwinMountTable(value),
      /Darwin mount table omitted the root mount/,
    );
  }
});

test("Linux getfacl parsing distinguishes base and extended ACL entries", () => {
  assert.equal(parseLinuxGetfacl("user::rwx\ngroup::---\nother::---\n"), false);
  assert.equal(
    parseLinuxGetfacl(
      "user::rwx\nuser:1234:r-x\ngroup::---\nmask::r-x\nother::---\n",
    ),
    true,
  );
  assert.equal(
    parseLinuxGetfacl(
      "user::rwx\ngroup::---\nother::---\ndefault:user::rwx\n" +
        "default:group::---\ndefault:other::---\n",
    ),
    true,
  );
  assert.throws(() => parseLinuxGetfacl("user::rwx\ngroup::---\n"), /incomplete|omitted/);
});

test("Linux recovery ACL inspection requires the fixed getfacl capability", async () => {
  const calls = [];
  assert.equal(
    await inspectLinuxRecoveryAcl("/private/session", {
      runCommand: async (...arguments_) => {
        calls.push(arguments_);
        return { stderr: "", stdout: "user::rwx\ngroup::---\nother::---\n" };
      },
    }),
    false,
  );
  assert.deepEqual(calls[0][0], "/usr/bin/getfacl");
  assert.deepEqual(calls[0][1], [
    "--absolute-names",
    "--omit-header",
    "/private/session",
  ]);
  await assert.rejects(
    inspectLinuxRecoveryAcl("/private/session", {
      runCommand: async () => {
        const error = new Error("missing capability");
        error.code = "ENOENT";
        throw error;
      },
    }),
    /Linux ACL capability inspection failed/,
  );
});

test("portable directory names require NFC and reject portable collisions", () => {
  assert.deepEqual(assertPortableDirectoryNames(["zeta", "Alpha"]), ["Alpha", "zeta"]);
  assert.throws(
    () => assertPortableDirectoryNames(["README", "readme"]),
    /case or Unicode-normalization name collisions/,
  );
  assert.throws(
    () => assertPortableDirectoryNames(["e\u0301"]),
    /rejects non-NFC directory names/,
  );
  for (const entry of ["\u03a3", "\u03c2", "Stra\u00dfe", "caf\u00e9"]) {
    assert.throws(
      () => assertPortableDirectoryNames([entry]),
      /non-ASCII cased directory names/,
    );
  }
});

test("source identity pre-scan failures occur before destination creation", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-copy-prescan-failure-test-"));
  try {
    const source = join(root, "source");
    const destination = join(root, "destination");
    await mkdir(source);
    await writeFile(join(source, "\u00c4"), "non-portable cased name");

    await assert.rejects(
      copyStoppedTree({ ownedRoot: root, source, destination }),
      /rejects non-ASCII cased directory names/,
    );
    await assert.rejects(lstat(destination), /ENOENT/);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("stopped-tree copy can require the caller-observed source identity", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-copy-expected-source-test-"));
  try {
    const source = join(root, "source");
    const displaced = join(root, "displaced");
    const destination = join(root, "destination");
    await mkdir(source);
    const expectedSourceRootIdentity = await lstat(source, { bigint: true });
    await rename(source, displaced);
    await mkdir(source);

    await assert.rejects(
      copyStoppedTree({
        destination,
        expectedSourceRootIdentity,
        ownedRoot: root,
        source,
      }),
      /rejects source root identity changes/,
    );
    await assert.rejects(lstat(destination), /ENOENT/);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("stopped-tree identity scans include nested directory authorities", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-tree-identity-scan-test-"));
  try {
    const source = join(root, "source");
    const nested = join(source, "nested");
    const outside = join(root, "outside");
    await mkdir(nested, { recursive: true });
    await mkdir(outside);
    const nestedIdentity = await lstat(nested, { bigint: true });
    const outsideIdentity = await lstat(outside, { bigint: true });

    assert.equal(
      await stoppedTreeContainsAnyIdentity(source, [nestedIdentity]),
      true,
    );
    assert.equal(
      await stoppedTreeContainsAnyIdentity(source, [outsideIdentity]),
      false,
    );
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("stopped-tree identity proofs detect subtree aliases and inode replacement", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-tree-identity-proof-test-"));
  try {
    const left = join(root, "left");
    const renamedLeft = join(root, "renamed-left");
    const right = join(root, "right");
    await mkdir(left);
    await mkdir(right);
    const leftFile = join(left, "data");
    await writeFile(leftFile, "same bytes\n");
    await writeFile(join(right, "copy"), "same bytes\n");

    assert.equal(await stoppedTreesShareAnyIdentity(left, right), false);
    const originalDigest = await digestStoppedTreeIdentities(
      left,
      "test-filesystem-001",
      TEST_OBJECT_IDENTITY_SCHEME,
      inspectTestPersistentObjectIdentity,
    );
    await rename(left, renamedLeft);
    assert.equal(
      await digestStoppedTreeIdentities(
        renamedLeft,
        "test-filesystem-001",
        TEST_OBJECT_IDENTITY_SCHEME,
        inspectTestPersistentObjectIdentity,
      ),
      originalDigest,
    );
    assert.notEqual(
      await digestStoppedTreeIdentities(
        renamedLeft,
        "test-filesystem-002",
        TEST_OBJECT_IDENTITY_SCHEME,
        inspectTestPersistentObjectIdentity,
      ),
      originalDigest,
    );
    await link(join(renamedLeft, "data"), join(right, "shared"));
    assert.equal(await stoppedTreesShareAnyIdentity(renamedLeft, right), true);

    await rm(join(renamedLeft, "data"));
    await writeFile(join(renamedLeft, "data"), "same bytes\n");
    assert.notEqual(
      await digestStoppedTreeIdentities(
        renamedLeft,
        "test-filesystem-001",
        TEST_OBJECT_IDENTITY_SCHEME,
        inspectTestPersistentObjectIdentity,
      ),
      originalDigest,
    );
  } finally {
    await rm(root, { recursive: true, force: true });
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
    await assertRetainedFailedDestination(destination);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("stopped-tree copy can reject every absolute symlink by policy", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-copy-absolute-policy-test-"));
  try {
    const source = join(root, "source");
    const destination = join(root, "destination");
    const external = join(root, "external");
    await mkdir(source);
    await writeFile(external, "sentinel");
    await symlink(external, join(source, "absolute-link"));

    await assert.rejects(
      copyStoppedTree({
        allowAbsoluteSymlinks: false,
        destination,
        ownedRoot: root,
        source,
      }),
      /rejects absolute symlinks by policy/,
    );
    await assertRetainedFailedDestination(destination);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("stopped-tree copy rejects absolute symlinks into a forbidden authority", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-copy-forbidden-link-test-"));
  try {
    const source = join(root, "source");
    const destination = join(root, "destination");
    const forbidden = join(root, "forbidden");
    await mkdir(source);
    await mkdir(forbidden);
    await writeFile(join(forbidden, "record"), "sentinel");
    await symlink(join(forbidden, "record"), join(source, "authority-record"));
    const identity = await lstat(forbidden, { bigint: true });

    await assert.rejects(
      copyStoppedTree({
        destination,
        forbiddenAbsoluteSymlinkAuthorities: [{
          device: identity.dev.toString(),
          inode: identity.ino.toString(),
          path: await realpath(forbidden),
        }],
        ownedRoot: root,
        source,
      }),
      /rejects absolute symlinks into a forbidden authority/,
    );
    await assertRetainedFailedDestination(destination);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("stopped-tree copy rejects identity aliases of forbidden authorities", async () => {
  const root = await mkdtemp(
    join(tmpdir(), "portable-copy-forbidden-alias-test-"),
  );
  try {
    const source = join(root, "source");
    const destination = join(root, "destination");
    const forbidden = join(root, "forbidden");
    const alias = join(root, "alias");
    await mkdir(source);
    await mkdir(forbidden);
    await writeFile(join(forbidden, "record"), "sentinel");
    await symlink(forbidden, alias);
    await symlink(join(alias, "record"), join(source, "authority-record"));
    const identity = await lstat(forbidden, { bigint: true });

    await assert.rejects(
      copyStoppedTree({
        destination,
        forbiddenAbsoluteSymlinkAuthorities: [{
          device: identity.dev.toString(),
          inode: identity.ino.toString(),
          path: await realpath(forbidden),
        }],
        inspectSymlinkPath: async (path, options) =>
          path === alias ? identity : lstat(path, options),
        ownedRoot: root,
        source,
      }),
      /rejects absolute symlinks into a forbidden authority/,
    );
    await assertRetainedFailedDestination(destination);
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

test("stopped-tree copy rejects case aliases of absolute internal symlinks", async (context) => {
  const root = await mkdtemp(join(tmpdir(), "portable-copy-case-alias-link-test-"));
  try {
    const source = join(root, "source");
    const destination = join(root, "destination");
    await mkdir(source);
    await writeFile(join(source, "target"), "sentinel");
    const sourceAlias = join(root, "SOURCE");
    try {
      await lstat(sourceAlias);
    } catch (error) {
      if (error?.code === "ENOENT") {
        context.skip("filesystem is case-sensitive");
        return;
      }
      throw error;
    }
    await symlink(join(sourceAlias, "target"), join(source, "case-alias"));
    await assert.rejects(
      copyStoppedTree({ ownedRoot: root, source, destination }),
      /rejects absolute symlinks into the source tree/,
    );
    await assertRetainedFailedDestination(destination);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("stopped-tree copy rejects absolute symlink chains that enter and leave the source", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-copy-absolute-chain-test-"));
  try {
    const source = join(root, "source");
    const external = join(root, "external");
    const outsideTarget = join(root, "outside-target");
    const destination = join(root, "destination");
    await mkdir(source);
    await mkdir(external);
    await writeFile(outsideTarget, "sentinel");
    await symlink(outsideTarget, join(source, "hop"));
    await symlink(join(source, "hop"), join(external, "bridge"));
    await symlink(join(external, "bridge"), join(source, "alias"));
    await assert.rejects(
      copyStoppedTree({ ownedRoot: root, source, destination }),
      /rejects absolute symlinks into the source tree/,
    );
    await assertRetainedFailedDestination(destination);
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
      /rejects absolute symlinks into the source tree/,
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

test("stopped-tree copy rejects relative symlink chains that leave and reenter the source", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-copy-relative-chain-test-"));
  try {
    const source = join(root, "source");
    const external = join(root, "external");
    const destination = join(root, "destination");
    await mkdir(source);
    await mkdir(external);
    await writeFile(join(source, "target"), "sentinel");
    await symlink(external, join(source, "link"));
    await symlink(join(source, "target"), join(external, "back"));
    await symlink("link/back", join(source, "alias"));
    await assert.rejects(
      copyStoppedTree({ ownedRoot: root, source, destination }),
      /relative symlinks outside the source tree/,
    );
    await assertRetainedFailedDestination(destination);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("stopped-tree copy preserves non-directory path component semantics", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-copy-relative-nondir-test-"));
  try {
    const source = join(root, "source");
    const destination = join(root, "destination");
    const linkPath = join(source, "link");
    await mkdir(source);
    await writeFile(join(source, "file"), "not a directory");
    await writeFile(join(source, "target"), "sentinel");
    for (const target of ["file/../target", "file/.", "file/"]) {
      await symlink(target, linkPath);
      await assert.rejects(
        copyStoppedTree({ ownedRoot: root, source, destination }),
        /relative symlinks through non-directories/,
      );
      await assertRetainedFailedDestination(destination);
      await removeTreeForCleanup(destination);
      await rm(linkPath);
    }
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("stopped-tree copy rejects relative symlink case aliases", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-copy-relative-case-test-"));
  try {
    const source = join(root, "source");
    const destination = join(root, "destination");
    await mkdir(source);
    await writeFile(join(source, "target"), "sentinel");
    await symlink("TARGET", join(source, "link"));
    await assert.rejects(
      copyStoppedTree({ ownedRoot: root, source, destination }),
      /relative symlink case or normalization aliases/,
    );
    await assertRetainedFailedDestination(destination);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("stopped-tree copy rejects relative symlink normalization aliases", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-copy-relative-normalization-test-"));
  try {
    const source = join(root, "source");
    const destination = join(root, "destination");
    await mkdir(source);
    await writeFile(join(source, "\uac00"), "sentinel");
    await symlink("\u1100\u1161", join(source, "link"));
    await assert.rejects(
      copyStoppedTree({ ownedRoot: root, source, destination }),
      /relative symlink case or normalization aliases/,
    );
    await assertRetainedFailedDestination(destination);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("stopped-tree copy rejects dangling relative symlinks", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-copy-relative-dangling-test-"));
  try {
    const source = join(root, "source");
    const destination = join(root, "destination");
    await mkdir(source);
    await symlink("missing", join(source, "link"));
    await assert.rejects(
      copyStoppedTree({ ownedRoot: root, source, destination }),
      /rejects dangling relative symlinks/,
    );
    await assertRetainedFailedDestination(destination);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("stopped-tree copy rejects symlink chains beyond the Darwin limit", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-copy-symlink-depth-test-"));
  try {
    const source = join(root, "source");
    const destination = join(root, "destination");
    await mkdir(source);
    await writeFile(join(source, "target"), "sentinel");
    let target = "target";
    for (let index = 32; index >= 0; index -= 1) {
      const name = `hop-${String(index).padStart(2, "0")}`;
      await symlink(target, join(source, name));
      target = name;
    }
    await assert.rejects(
      copyStoppedTree({ ownedRoot: root, source, destination }),
      /rejects excessive symlink resolution depth/,
    );
    await assertRetainedFailedDestination(destination);
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

test("portable tree operations reject entries inaccessible to the snapshot user", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-copy-inaccessible-test-"));
  const source = join(root, "source");
  const inaccessibleFile = join(source, "inaccessible");
  const inaccessiblePaths = new Set([inaccessibleFile]);
  const checkAccess = async (path) => {
    if (!inaccessiblePaths.has(path)) return;
    const error = new Error("injected access denial");
    error.code = "EACCES";
    throw error;
  };
  try {
    await mkdir(source);
    await writeFile(inaccessibleFile, "sentinel");
    inaccessiblePaths.add(await realpath(inaccessibleFile));
    await assert.rejects(
      digestTree(source, { checkAccess }),
      /entries inaccessible to the snapshot user/,
    );
    await assert.rejects(
      copyStoppedTree({
        ownedRoot: root,
        source,
        destination: join(root, "destination"),
        checkAccess,
      }),
      /entries inaccessible to the snapshot user/,
    );
    await assertRetainedFailedDestination(join(root, "destination"));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("stopped-tree copy never removes a destination created by a racing writer", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-copy-destination-race-test-"));
  try {
    const source = join(root, "source");
    const destination = join(root, "destination");
    const sentinel = join(destination, "racing-writer");
    await mkdir(source);
    await writeFile(join(source, "source-file"), "source");
    await assert.rejects(
      copyStoppedTree({
        ownedRoot: root,
        source,
        destination,
        afterDestinationValidation: async () => {
          await mkdir(destination);
          await writeFile(sentinel, "preserve");
        },
      }),
      /EEXIST/,
    );
    assert.equal(await readFile(sentinel, "utf8"), "preserve");
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("stopped-tree copy preserves a replacement after destination-root identity changes", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-copy-destination-identity-test-"));
  try {
    const source = join(root, "source");
    const destination = join(root, "destination");
    const displaced = join(root, "displaced-destination");
    const sentinel = join(destination, "replacement-writer");
    await mkdir(source);
    await writeFile(join(source, "source-file"), "source");
    await assert.rejects(
      copyStoppedTree({
        ownedRoot: root,
        source,
        destination,
        afterDestinationRootCreated: async () => {
          await rename(destination, displaced);
          await mkdir(destination);
          await writeFile(sentinel, "preserve");
        },
      }),
      /destination root identity changes/,
    );
    assert.equal(await readFile(sentinel, "utf8"), "preserve");
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("stopped-tree copy rejects replaced destination subdirectories before writes", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-copy-destination-child-race-test-"));
  try {
    const source = join(root, "source");
    const destination = join(root, "destination");
    const destinationChild = join(destination, "nested");
    const displaced = join(root, "displaced-destination-child");
    const outside = join(root, "outside");
    let replaced = false;
    await mkdir(join(source, "nested"), { recursive: true });
    await writeFile(join(source, "nested", "source-file"), "source");
    await mkdir(outside, { mode: 0o750 });
    await writeFile(join(outside, "sentinel"), "preserve");
    const outsideMode = (await stat(outside)).mode & 0o777;
    await assert.rejects(
      copyStoppedTree({
        ownedRoot: root,
        source,
        destination,
        afterDestinationDirectoryCreated: async (path) => {
          if (replaced || basename(path) !== "nested") return;
          replaced = true;
          await rename(destinationChild, displaced);
          await symlink(outside, destinationChild);
        },
      }),
      /ELOOP|destination directory identity changes/,
    );
    assert.equal(await readFile(join(outside, "sentinel"), "utf8"), "preserve");
    assert.equal((await stat(outside)).mode & 0o777, outsideMode);
    await assert.rejects(lstat(join(outside, "source-file")), /ENOENT/);
    await assertRetainedFailedDestination(destination);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("stopped-tree copy rejects source symlinks replaced after target validation", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-copy-source-symlink-race-test-"));
  try {
    const source = join(root, "source");
    const destination = join(root, "destination");
    const sourceLink = join(source, "victim");
    const displaced = join(root, "displaced-source-link");
    let replaced = false;
    await mkdir(source);
    await writeFile(join(source, "target"), "source");
    await symlink("target", sourceLink);
    await assert.rejects(
      copyStoppedTree({
        ownedRoot: root,
        source,
        destination,
        afterSourceSymlinkValidated: async (path) => {
          if (replaced || basename(path) !== "victim") return;
          replaced = true;
          await rename(sourceLink, displaced);
          await symlink("target", sourceLink);
        },
      }),
      /source symlink identity changes/,
    );
    assert.equal((await lstat(sourceLink)).isSymbolicLink(), true);
    assert.equal(await readlink(sourceLink), "target");
    await assertRetainedFailedDestination(destination);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("stopped-tree copy rejects source files replaced by symlinks before open", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-copy-source-race-test-"));
  try {
    const source = join(root, "source");
    const destination = join(root, "destination");
    const sourceFile = join(source, "victim");
    const displaced = join(root, "displaced-source-file");
    const outside = join(root, "outside");
    let replaced = false;
    await mkdir(source);
    await writeFile(sourceFile, "source");
    await writeFile(outside, "outside");
    await assert.rejects(
      copyStoppedTree({
        ownedRoot: root,
        source,
        destination,
        beforeSourceOpen: async (path) => {
          if (replaced || basename(path) !== "victim") return;
          replaced = true;
          await rename(sourceFile, displaced);
          await symlink(outside, sourceFile);
        },
      }),
      /ELOOP|source file identity changes/,
    );
    assert.equal((await lstat(sourceFile)).isSymbolicLink(), true);
    assert.equal(await readFile(outside, "utf8"), "outside");
    await assertRetainedFailedDestination(destination);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("stopped-tree copy rejects source directories replaced after open", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-copy-source-directory-race-test-"));
  try {
    const source = join(root, "source");
    const destination = join(root, "destination");
    const displaced = join(root, "displaced-source");
    const outside = join(root, "outside-directory");
    let replaced = false;
    await mkdir(source);
    await writeFile(join(source, "source-file"), "source");
    await mkdir(outside);
    await writeFile(join(outside, "outside-file"), "outside");
    await assert.rejects(
      copyStoppedTree({
        ownedRoot: root,
        source,
        destination,
        afterSourceDirectoryOpen: async (path) => {
          if (replaced || basename(path) !== "source") return;
          replaced = true;
          await rename(source, displaced);
          await symlink(outside, source);
        },
      }),
      /source directory identity changes/,
    );
    await assertRetainedFailedDestination(destination);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test(
  "portable tree operations reject hard-linked symlinks",
  { skip: platform() !== "linux" },
  async () => {
    const root = await mkdtemp(join(tmpdir(), "portable-copy-hard-link-symlink-test-"));
    try {
      const source = join(root, "source");
      const destination = join(root, "destination");
      await mkdir(source);
      await writeFile(join(source, "target"), "sentinel");
      await symlink("target", join(source, "first"));
      await link(join(source, "first"), join(source, "second"));
      await assert.rejects(digestTree(source), /rejects hard-linked symlinks/);
      await assert.rejects(
        copyStoppedTree({ ownedRoot: root, source, destination }),
        /rejects hard-linked symlinks/,
      );
      await assertRetainedFailedDestination(destination);
    } finally {
      await rm(root, { recursive: true, force: true });
    }
  },
);

test("recovery evidence is allowlisted and rejects identifiers, paths, and prompts", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-evidence-test-"));
  try {
    const report = completeEvidenceReport();
    assert.doesNotThrow(() => assertRecoveryEvidenceSafe(report));
    assert.throws(
      () => assertRecoveryEvidenceSafe({ ...report, schemaVersion: 1 }),
      /1 !== 5/,
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
    assert.throws(
      () =>
        assertRecoveryEvidenceSafe({
          ...report,
          runtime: { ...report.runtime, launcherArch: "build-host-01" },
        }),
      /launcherArch is not a recognized Node architecture/,
    );
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
      /duplicate object keys/,
    );
    const nestedSecret = String.raw`{"token":"s\u006b-secret-sentinel"}`;
    const duplicateNestedSecret = JSON.stringify(report).replace(
      '"codexVersion":"codex-cli 0.142.4"',
      `"codexVersion":${JSON.stringify(nestedSecret)},"codexVersion":"codex-cli 0.142.4"`,
    );
    assert.throws(
      () => assertRecoveryEvidenceSafe(duplicateNestedSecret),
      /duplicate object keys/,
    );
    const serialized = JSON.stringify(report);
    for (const duplicate of [
      serialized.replace('"schemaVersion":5', '"schemaVersion":1,"schemaVersion":5'),
      serialized.replace('"result":"passed"', '"result":"failed","result":"passed"'),
      serialized.replace(
        '"schemaVersion":5',
        '"\\u0073chemaVersion":1,"schemaVersion":5',
      ),
    ]) {
      assert.throws(
        () => assertRecoveryEvidenceSafe(duplicate),
        /duplicate object keys/,
      );
    }
    const path = join(root, "evidence.json");
    const synchronizedDirectories = [];
    for (let index = 0; index < 2; index += 1) {
      await writeRecoveryEvidence(path, report, {
        syncDirectory: async (_handle, directory) =>
          synchronizedDirectories.push(directory),
      });
    }
    assert.deepEqual(JSON.parse(await readFile(path, "utf8")), report);
    assert.deepEqual(await readdir(root), ["evidence.json"]);
    assert.deepEqual(synchronizedDirectories, [await realpath(root), await realpath(root)]);

    const unsynchronizedPath = join(root, "unsynchronized-evidence.json");
    await assert.rejects(
      writeRecoveryEvidence(unsynchronizedPath, report, {
        syncDirectory: async () => {
          throw new Error("synthetic evidence directory sync failure");
        },
      }),
      (error) =>
        error.code === "evidence_durability_uncertain" &&
        error.message === "evidence publication durability is uncertain" &&
        error.cause?.message === "synthetic evidence directory sync failure",
    );
    assert.deepEqual(JSON.parse(await readFile(unsynchronizedPath, "utf8")), report);
    assert.deepEqual(
      (await readdir(root)).filter((entry) => entry.startsWith(".unsynchronized-evidence")),
      [],
    );

    const writeFailure = new Error("synthetic evidence write failure");
    const writeCloseFailure = new Error("synthetic evidence write close failure");
    await assert.rejects(
      writeRecoveryEvidence(join(root, "write-failure.json"), report, {
        openEvidenceFile: async () => ({
          chmod: async () => {},
          close: async () => {
            throw writeCloseFailure;
          },
          sync: async () => assert.fail("sync must not run after write failure"),
          writeFile: async () => {
            throw writeFailure;
          },
        }),
      }),
      (error) => error === writeFailure && error.cleanupError === writeCloseFailure,
    );

    const closeOnlyFailure = new Error("synthetic evidence close-only failure");
    await assert.rejects(
      writeRecoveryEvidence(join(root, "close-failure.json"), report, {
        openEvidenceFile: async () => ({
          chmod: async () => {},
          close: async () => {
            throw closeOnlyFailure;
          },
          stat: async () => privateEvidenceFileMetadata(),
          sync: async () => {},
          writeFile: async () => {},
        }),
      }),
      (error) => error === closeOnlyFailure,
    );

    const missingParent = join(root, "missing-parent");
    await assert.rejects(
      writeRecoveryEvidence(join(missingParent, "evidence.json"), report),
      /ENOENT/,
    );
    await assert.rejects(lstat(missingParent), /ENOENT/);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("tracked interrupted-turn recovery evidence matches the pinned run", async () => {
  const raw = await readFile(
    new URL("../evidence/interrupted-turn-recovery.json", import.meta.url),
    "utf8",
  );
  assert.equal(assertRecoveryEvidenceSafe(raw), raw);
  const report = JSON.parse(raw);

  assert.equal(report.schemaVersion, 5);
  assert.equal(report.probe, "interrupted-turn-recovery");
  assert.equal(report.runtime.codexVersion, "codex-cli 0.142.4");
  assert.equal(
    report.runtime.codexBinarySha256,
    "32b3b3a3e8e19b09f2b74979ca2a7f6890dc88b8335bb0e1913a0ad68a6505b5",
  );
  assert.equal(report.runtime.sourceAnalysisCommit, PINNED_SOURCE_ANALYSIS_COMMIT);
  assert.equal(report.runtime.platform, "darwin");
  assert.equal(report.runtime.launcherArch, "arm64");
});

test("recovery evidence rejects unsafe directory permissions and ACLs before writing", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-evidence-trust-test-"));
  try {
    let openCalls = 0;
    const openEvidenceFile = async () => {
      openCalls += 1;
      assert.fail("unsafe evidence directories must fail before opening a file");
    };
    await chmod(root, 0o777);
    await assert.rejects(
      writeRecoveryEvidence(join(root, "mode.json"), completeEvidenceReport(), {
        openEvidenceFile,
      }),
      /not writable by other users/,
    );
    await chmod(root, 0o700);
    await assert.rejects(
      writeRecoveryEvidence(join(root, "directory-acl.json"), completeEvidenceReport(), {
        inspectEvidenceDirectoryAcl: async () => true,
        openEvidenceFile,
      }),
      /evidence directory ACL could not be trusted/,
    );
    const nested = join(root, "nested");
    await mkdir(nested, { mode: 0o700 });
    await assert.rejects(
      writeRecoveryEvidence(join(nested, "ancestor-acl.json"), completeEvidenceReport(), {
        inspectEvidenceAncestorAcl: async () => true,
        openEvidenceFile,
      }),
      /evidence directory ancestor ACL could not be trusted/,
    );
    assert.equal(openCalls, 0);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("recovery evidence rejects a directory replacement before publication", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-evidence-replacement-test-"));
  const evidenceDirectory = join(root, "evidence");
  const displacedDirectory = join(root, "displaced");
  try {
    await mkdir(evidenceDirectory, { mode: 0o700 });
    await assert.rejects(
      writeRecoveryEvidence(
        join(evidenceDirectory, "result.json"),
        completeEvidenceReport(),
        {
          beforeEvidenceRename: async () => {
            await rename(evidenceDirectory, displacedDirectory);
            await mkdir(evidenceDirectory, { mode: 0o700 });
            await writeFile(join(evidenceDirectory, "sentinel"), "replacement");
          },
        },
      ),
      /evidence directory identity|temporary evidence directory identity/,
    );
    assert.equal(await readFile(join(evidenceDirectory, "sentinel"), "utf8"), "replacement");
    await assert.rejects(lstat(join(evidenceDirectory, "result.json")), /ENOENT/);
    assert((await readdir(displacedDirectory)).some((entry) => entry.startsWith(".result.json.tmp-")));
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("recovery evidence rejects a temporary-directory replacement before writing", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-evidence-temp-replacement-test-"));
  try {
    let displacedDirectory;
    let replacementDirectory;
    await assert.rejects(
      writeRecoveryEvidence(join(root, "result.json"), completeEvidenceReport(), {
        afterTemporaryDirectoryOpened: async ({ temporaryDirectoryAuthority }) => {
          replacementDirectory = temporaryDirectoryAuthority.path;
          displacedDirectory = `${temporaryDirectoryAuthority.path}.displaced`;
          await rename(temporaryDirectoryAuthority.path, displacedDirectory);
          await mkdir(temporaryDirectoryAuthority.path, { mode: 0o700 });
          await writeFile(join(temporaryDirectoryAuthority.path, "sentinel"), "replacement");
        },
      }),
      /temporary evidence directory identity or permissions changed/,
    );
    assert.equal(
      await readFile(join(replacementDirectory, "sentinel"), "utf8"),
      "replacement",
    );
    await assert.rejects(lstat(join(root, "result.json")), /ENOENT/);
    assert.equal((await stat(displacedDirectory)).isDirectory(), true);
  } finally {
    await rm(root, { recursive: true, force: true });
  }
});

test("recovery evidence never removes a replaced temp directory after rename", async () => {
  const root = await mkdtemp(join(tmpdir(), "portable-evidence-post-rename-temp-test-"));
  try {
    let displacedDirectory;
    let temporaryDirectory;
    let syncCalls = 0;
    await assert.rejects(
      writeRecoveryEvidence(join(root, "result.json"), completeEvidenceReport(), {
        afterTemporaryDirectoryOpened: async ({ temporaryDirectoryAuthority }) => {
          temporaryDirectory = temporaryDirectoryAuthority.path;
          displacedDirectory = `${temporaryDirectoryAuthority.path}.displaced`;
        },
        afterEvidenceRename: async () => {
          await rename(temporaryDirectory, displacedDirectory);
          await mkdir(temporaryDirectory, { mode: 0o700 });
        },
        syncDirectory: async () => {
          syncCalls += 1;
        },
      }),
      (error) =>
        error.code === "evidence_durability_uncertain" &&
        /temporary evidence directory identity or permissions changed/.test(
          error.cause?.message ?? "",
        ),
    );
    assert.equal((await stat(temporaryDirectory)).isDirectory(), true);
    assert.equal((await stat(displacedDirectory)).isDirectory(), true);
    assert.deepEqual(
      JSON.parse(await readFile(join(root, "result.json"), "utf8")),
      completeEvidenceReport(),
    );
    assert.equal(syncCalls, 0);
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
  const scenarioRoots = [];
  let versionBinary;
  let versionContext;
  const report = await probeInterruptedTurnRecovery({
    ...TRUSTED_RECOVERY_ACL,
    codexBin: process.execPath,
    readCodexVersion: (codexBin, context) => {
      versionBinary = codexBin;
      versionContext = context;
      return "codex-cli 0.142.4";
    },
    runScenario: async ({ codexBin, kind, temporaryRoot }) => {
      calls.push(kind);
      scenarioBinaries.push(codexBin);
      scenarioRoots.push(temporaryRoot);
      return scenarioReport(kind);
    },
  });
  assert.deepEqual(calls, RECOVERY_SCENARIOS);
  assert.equal(report.schemaVersion, 5);
  assert.equal(report.runtime.binaryExecution, "private-read-only-copy");
  assert(scenarioBinaries.every((binary) => binary === versionBinary));
  assert(scenarioRoots.every((root) => root === dirname(versionBinary)));
  assert.notEqual(versionBinary, process.execPath);
  assert.equal(versionContext.cwd, versionContext.env.CODEX_HOME);
  assert.equal(versionContext.env.OPENAI_API_KEY, undefined);
  assert.equal(versionContext.env.AWS_SECRET_ACCESS_KEY, undefined);
  await assert.rejects(lstat(versionBinary), /ENOENT/);
  assert.equal(report.runtime.sourceAnalysisCommit, PINNED_SOURCE_ANALYSIS_COMMIT);
  assert.equal(report.backend.realModelTurnConfigured, false);
  assert.equal(report.backend.credentialInputProvisioned, false);
  assert.equal(report.backend.outboundNetworkIsolated, false);
  assert.equal(report.snapshot.kind, "stopped-tree-copy");
  assert.equal(report.scenarios.length, 4);
  assert.doesNotThrow(() => assertRecoveryEvidenceSafe(report));
});

test("probe rejects private binary mode changes between scenarios", async () => {
  let privateBinary;
  let calls = 0;
  await assert.rejects(
    probeInterruptedTurnRecovery({
      ...TRUSTED_RECOVERY_ACL,
      codexBin: process.execPath,
      readCodexVersion: () => "codex-cli 0.142.4",
      runScenario: async ({ codexBin, kind }) => {
        privateBinary = codexBin;
        calls += 1;
        await chmod(codexBin, 0o700);
        return scenarioReport(kind);
      },
    }),
    /private CODEX_BIN copy must remain mode 0500/,
  );
  assert.equal(calls, 1);
  await assert.rejects(lstat(privateBinary), /ENOENT/);
});

test("probe stages its private binary under an explicit executable root", async () => {
  const executableRoot = await mkdtemp(join(tmpdir(), "portable-executable-root-test-"));
  let privateBinary;
  try {
    await probeInterruptedTurnRecovery({
      ...TRUSTED_RECOVERY_ACL,
      codexBin: process.execPath,
      executableRoot,
      readCodexVersion: (codexBin) => {
        privateBinary = codexBin;
        return "codex-cli 0.142.4";
      },
      runScenario: async ({ kind }) => scenarioReport(kind),
    });
    assert.equal(dirname(dirname(privateBinary)), await realpath(executableRoot));
    await assert.rejects(lstat(privateBinary), /ENOENT/);
  } finally {
    await rm(executableRoot, { recursive: true, force: true });
  }
});

test("probe rejects an untrusted writable executable root", async () => {
  const executableRoot = await mkdtemp(join(tmpdir(), "portable-unsafe-exec-root-test-"));
  try {
    await chmod(executableRoot, 0o777);
    await assert.rejects(
      probeInterruptedTurnRecovery({
        ...TRUSTED_RECOVERY_ACL,
        codexBin: process.execPath,
        executableRoot,
        readCodexVersion: () => "codex-cli 0.142.4",
        runScenario: async ({ kind }) => scenarioReport(kind),
      }),
      /recovery executable root must have trusted ownership and permissions/,
    );
  } finally {
    await chmod(executableRoot, 0o700).catch(() => {});
    await rm(executableRoot, { recursive: true, force: true });
  }
});

test("probe rejects an executable root below an unsafe ancestor", async () => {
  const container = await mkdtemp(join(tmpdir(), "portable-unsafe-exec-parent-test-"));
  const unsafeParent = join(container, "unsafe-parent");
  const executableRoot = join(unsafeParent, "safe-root");
  try {
    await mkdir(executableRoot, { recursive: true });
    await chmod(unsafeParent, 0o777);
    await chmod(executableRoot, 0o700);
    await assert.rejects(
      probeInterruptedTurnRecovery({
        ...TRUSTED_RECOVERY_ACL,
        codexBin: process.execPath,
        executableRoot,
        readCodexVersion: () => "codex-cli 0.142.4",
        runScenario: async ({ kind }) => scenarioReport(kind),
      }),
      /recovery executable root ancestor chain is not trusted/,
    );
  } finally {
    await chmod(unsafeParent, 0o700).catch(() => {});
    await rm(container, { recursive: true, force: true });
  }
});

test("probe rejects executable-root ACLs through the inspection seam", async () => {
  const executableRoot = await mkdtemp(join(tmpdir(), "portable-exec-root-acl-test-"));
  try {
    await assert.rejects(
      probeInterruptedTurnRecovery({
        codexBin: process.execPath,
        executableRoot,
        inspectExecutableAncestorAcl: async () => false,
        inspectExecutableRootAcl: async () => true,
        readCodexVersion: () => "codex-cli 0.142.4",
        runScenario: async ({ kind }) => scenarioReport(kind),
      }),
      /recovery executable root must not have extended access controls/,
    );
  } finally {
    await rm(executableRoot, { recursive: true, force: true });
  }
});

test("probe requires an absolute binary and the complete scenario matrix", async () => {
  await assert.rejects(
    probeInterruptedTurnRecovery({ codexBin: "codex", runScenario: async () => ({}) }),
    /absolute pinned-image path/,
  );
  await assert.rejects(
    probeInterruptedTurnRecovery({
      ...TRUSTED_RECOVERY_ACL,
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
  const hostileError = new Error("secret hostile diagnostics");
  Object.defineProperty(hostileError, "code", {
    get() {
      throw new Error("secret code getter diagnostics");
    },
  });
  assert.deepEqual(interruptedTurnRecoveryFailureReport(hostileError), {
    error: { code: "recovery_probe_failed", retryable: false, type: "probe_failure" },
    result: "failed",
  });
});

test("CLI preserves only the allowlisted evidence durability error code", async () => {
  let stdout = "";
  let stderr = "";
  const failure = new Error("secret evidence sync diagnostics");
  failure.code = "evidence_durability_uncertain";
  const status = await runInterruptedTurnRecoveryCli({
    args: ["--write-evidence"],
    cwd: "/safe-working-directory",
    env: {
      CODEX_BIN: "/pinned/codex",
      CODEX_RECOVERY_EVIDENCE: "evidence/result.json",
    },
    probe: async () => {
      throw failure;
    },
    stderr: { write: (value) => (stderr += value) },
    stdout: { write: (value) => (stdout += value) },
  });
  assert.equal(status, 1);
  assert.equal(stdout, "");
  assert.deepEqual(JSON.parse(stderr), {
    error: {
      code: "evidence_durability_uncertain",
      retryable: false,
      type: "probe_failure",
    },
    result: "failed",
  });
  assert(!stderr.includes("secret evidence sync diagnostics"));
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
