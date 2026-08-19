import assert from "node:assert/strict";
import crypto, { createHash } from "node:crypto";
import {
  chmod,
  link,
  lstat,
  mkdir,
  mkdtemp,
  open,
  readFile,
  readdir,
  realpath,
  rename,
  rm,
  symlink,
  writeFile,
} from "node:fs/promises";
import { syncBuiltinESMExports } from "node:module";
import { tmpdir } from "node:os";
import path, { join } from "node:path";
import test from "node:test";

import {
  PODMAN_WRITER_SUPERVISOR_STATE_COLLECTION_CONTRACT_VERSION,
  PODMAN_WRITER_SUPERVISOR_STATE_CONTRACT_VERSION,
  PodmanWriterSupervisorStateError,
  assertPodmanWriterSupervisorStateRecord,
  createPodmanWriterSupervisorState,
  createPodmanWriterSupervisorStateBundle,
  isPodmanWriterSupervisorState,
  isPodmanWriterSupervisorStateBundle,
  isPodmanWriterSupervisorStateTerminalCollector,
} from "../src/podman-writer-supervisor-state.mjs";

const REQUEST_SHA256 = "a".repeat(64);
const CONTAINER_ID = "b".repeat(64);

function exact(value) {
  return Object.freeze(Object.assign(Object.create(null), value));
}

function record(status, override = {}) {
  const revision = {
    preparing: 0,
    created: 1,
    started: 2,
    stopping: 3,
    stopped: 4,
  }[status];
  return exact({
    containerId: status === "preparing" ? null : CONTAINER_ID,
    containerName: "codex-writer-0123456789abcdef",
    contractVersion: PODMAN_WRITER_SUPERVISOR_STATE_CONTRACT_VERSION,
    launchAttemptId: "launch-attempt-001",
    processIncarnationId:
      status === "preparing" ? null : `podman-process:${CONTAINER_ID}`,
    proofId: ["preparing", "created"].includes(status)
      ? null
      : `podman-start:${"c".repeat(64)}`,
    requestSha256: REQUEST_SHA256,
    revision,
    status,
    stopOperationId: ["stopping", "stopped"].includes(status)
      ? "stop-operation-001"
      : null,
    stopProofId: status === "stopped"
      ? `podman-stopped:${"d".repeat(64)}`
      : null,
    writerIncarnationId:
      status === "preparing" ? null : `podman-writer:${"e".repeat(64)}`,
    ...override,
  });
}

async function fixture(t) {
  const parent = await mkdtemp(
    join(await realpath(tmpdir()), "podman-writer-state-test-"),
  );
  const root = join(parent, "state");
  t.after(() => rm(parent, { force: true, recursive: true }));
  return { root, state: createPodmanWriterSupervisorState(exact({ root })) };
}

function faultingState(root, hookName) {
  const faultHooks = exact({
    [hookName]() {
      throw new Error(`simulated crash after ${hookName}`);
    },
  });
  return stateWithFaultHooks(root, faultHooks);
}

function stateWithFaultHooks(root, faultHooks) {
  return createPodmanWriterSupervisorState(exact({ faultHooks, root }));
}

function stateIoError(error) {
  return (
    error instanceof PodmanWriterSupervisorStateError &&
    error.code === "podman_writer_state_io_failed"
  );
}

function stateConflict(error) {
  return (
    error instanceof PodmanWriterSupervisorStateError &&
    error.code === "podman_writer_state_conflict"
  );
}

function transition(state, before, after) {
  return state.transition(
    exact({
      expectedRevision: before.revision,
      expectedStatus: before.status,
      record: after,
    }),
  );
}

async function createStoppedChain(state, override = {}) {
  const records = [
    record("preparing", override),
    record("created", override),
    record("started", override),
    record("stopping", override),
    record("stopped", override),
  ];
  await state.claim(exact({ record: records[0] }));
  for (let index = 1; index < records.length; index += 1) {
    await transition(state, records[index - 1], records[index]);
  }
  return records;
}

function collectionUncertain(error) {
  return (
    error instanceof PodmanWriterSupervisorStateError &&
    error.code === "podman_writer_state_collection_outcome_uncertain"
  );
}

test("creates an exact owner-private append-only state surface", async (t) => {
  const { root, state } = await fixture(t);
  assert.equal(isPodmanWriterSupervisorState(state), true);
  assert.equal(Object.getPrototypeOf(state), null);
  assert.equal(Object.isFrozen(state), true);
  assert.deepEqual(Reflect.ownKeys(state), [
    "claim",
    "contractVersion",
    "read",
    "transition",
  ]);
  assert.equal(state.claim.length, 1);
  assert.equal(state.read.length, 1);
  assert.equal(state.transition.length, 1);

  const pending = state.claim(exact({ record: record("preparing") }));
  assert.equal(Object.getPrototypeOf(pending), Promise.prototype);
  const claim = await pending;
  assert.equal(claim.created, true);
  assert.equal(claim.record.status, "preparing");
  assert.equal(Object.isFrozen(claim.record), true);
  assert.equal(Object.getPrototypeOf(claim.record), null);

  const rootStat = await lstat(root, { bigint: true });
  assert.equal(Number(rootStat.mode & 0o777n), 0o700);
  const entries = await readdir(root);
  assert.equal(entries.length, 1);
  assert.match(entries[0], /^[0-9a-f]{64}\.0\.json$/u);
  for (const entry of entries) {
    const fileStat = await lstat(join(root, entry), { bigint: true });
    assert.equal(Number(fileStat.mode & 0o777n), 0o600);
    assert.equal(fileStat.nlink, 1n);
  }
});

test("creates a separate exact terminal collection capability without changing the state ABI", async (t) => {
  const parent = await mkdtemp(
    join(await realpath(tmpdir()), "podman-writer-state-bundle-test-"),
  );
  const root = join(parent, "state");
  t.after(() => rm(parent, { force: true, recursive: true }));
  const bundle = createPodmanWriterSupervisorStateBundle(exact({ root }));

  assert.equal(isPodmanWriterSupervisorStateBundle(bundle), true);
  assert.equal(Object.getPrototypeOf(bundle), null);
  assert.equal(Object.isFrozen(bundle), true);
  assert.deepEqual(Reflect.ownKeys(bundle), ["state", "terminalCollector"]);
  assert.equal(isPodmanWriterSupervisorState(bundle.state), true);
  assert.deepEqual(Reflect.ownKeys(bundle.state), [
    "claim",
    "contractVersion",
    "read",
    "transition",
  ]);
  assert.equal(
    isPodmanWriterSupervisorStateTerminalCollector(bundle.terminalCollector),
    true,
  );
  assert.deepEqual(Reflect.ownKeys(bundle.terminalCollector), [
    "collect",
    "contractVersion",
  ]);
  assert.equal(
    bundle.terminalCollector.contractVersion,
    PODMAN_WRITER_SUPERVISOR_STATE_COLLECTION_CONTRACT_VERSION,
  );
  assert.equal(bundle.terminalCollector.collect.length, 1);
  assert.equal(Object.isFrozen(bundle.terminalCollector.collect), true);
  assert.equal(isPodmanWriterSupervisorStateBundle({ ...bundle }), false);
  assert.equal(
    isPodmanWriterSupervisorStateTerminalCollector({
      ...bundle.terminalCollector,
    }),
    false,
  );
});

test("collects an exact stopped chain in two durable phases and replays absence", async (t) => {
  const parent = await mkdtemp(
    join(await realpath(tmpdir()), "podman-writer-state-collect-test-"),
  );
  const root = join(parent, "state");
  t.after(() => rm(parent, { force: true, recursive: true }));
  const bundle = createPodmanWriterSupervisorStateBundle(exact({ root }));
  const records = await createStoppedChain(bundle.state);
  const other = record("preparing", {
    launchAttemptId: "launch-attempt-other",
    requestSha256: "f".repeat(64),
  });
  await bundle.state.claim(exact({ record: other }));

  const receipt = await bundle.terminalCollector.collect(
    exact({ terminalRecord: records[4] }),
  );
  assert.equal(Object.getPrototypeOf(receipt), null);
  assert.equal(Object.isFrozen(receipt), true);
  assert.deepEqual(Reflect.ownKeys(receipt), [
    "contractVersion",
    "launchAttemptId",
    "status",
    "terminalRecordSha256",
  ]);
  assert.equal(receipt.status, "collected");
  assert.match(receipt.terminalRecordSha256, /^[0-9a-f]{64}$/u);
  assert.equal(
    await bundle.state.read(
      exact({ launchAttemptId: records[4].launchAttemptId }),
    ),
    null,
  );
  assert.deepEqual(
    await bundle.state.read(exact({ launchAttemptId: other.launchAttemptId })),
    other,
  );
  const remaining = await readdir(root);
  assert.equal(remaining.length, 1);
  assert.match(remaining[0], /^[0-9a-f]{64}\.0\.json$/u);

  const replay = await bundle.terminalCollector.collect(
    exact({ terminalRecord: records[4] }),
  );
  assert.deepEqual(replay, exact({ ...receipt, status: "absent" }));
});

test("collection preflights the full chain and recognized aliases before unlink", async (t) => {
  const parent = await mkdtemp(
    join(await realpath(tmpdir()), "podman-writer-state-alias-test-"),
  );
  const root = join(parent, "state");
  t.after(() => rm(parent, { force: true, recursive: true }));
  const bundle = createPodmanWriterSupervisorStateBundle(exact({ root }));
  const records = await createStoppedChain(bundle.state);
  const entries = (await readdir(root)).sort();
  for (let index = 0; index < entries.length; index += 1) {
    const file = join(root, entries[index]);
    const bytes = await readFile(file);
    await writeFile(
      `${file}.ready`,
      `${createHash("sha256").update(bytes).digest("hex")}\n`,
      { flag: "wx", mode: 0o600 },
    );
  }
  const terminalEntry = entries.find((entry) => entry.endsWith(".4.json"));
  assert.equal(typeof terminalEntry, "string");
  await link(
    join(root, terminalEntry),
    `${join(root, terminalEntry)}.pending`,
  );

  const receipt = await bundle.terminalCollector.collect(
    exact({ terminalRecord: records[4] }),
  );
  assert.equal(receipt.status, "collected");
  assert.deepEqual(await readdir(root), []);
});

test("collection rejects wrong authority and sidecar tamper before deleting anything", async (t) => {
  const parent = await mkdtemp(
    join(await realpath(tmpdir()), "podman-writer-state-preflight-test-"),
  );
  const root = join(parent, "state");
  t.after(() => rm(parent, { force: true, recursive: true }));
  const bundle = createPodmanWriterSupervisorStateBundle(exact({ root }));
  const records = await createStoppedChain(bundle.state);
  const before = (await readdir(root)).sort();

  await assert.rejects(
    bundle.terminalCollector.collect(
      exact({
        terminalRecord: record("stopped", {
          requestSha256: "f".repeat(64),
        }),
      }),
    ),
    stateConflict,
  );
  assert.deepEqual((await readdir(root)).sort(), before);

  const revisionTwo = before.find((entry) => entry.endsWith(".2.json"));
  assert.equal(typeof revisionTwo, "string");
  await writeFile(`${join(root, revisionTwo)}.ready`, `${"0".repeat(64)}\n`, {
    flag: "wx",
    mode: 0o600,
  });
  const tampered = (await readdir(root)).sort();
  await assert.rejects(
    bundle.terminalCollector.collect(
      exact({ terminalRecord: records[4] }),
    ),
    stateConflict,
  );
  assert.deepEqual((await readdir(root)).sort(), tampered);
});

test("collection classifies readable non-canonical durable bytes as conflict", async (t) => {
  const parent = await mkdtemp(
    join(await realpath(tmpdir()), "podman-writer-state-malformed-test-"),
  );
  const root = join(parent, "state");
  t.after(() => rm(parent, { force: true, recursive: true }));
  const bundle = createPodmanWriterSupervisorStateBundle(exact({ root }));
  const records = await createStoppedChain(bundle.state);
  const revisionTwo = (await readdir(root)).find((entry) =>
    entry.endsWith(".2.json"),
  );
  assert.equal(typeof revisionTwo, "string");
  await writeFile(join(root, revisionTwo), "{\"malformed\":true}\n");
  const before = (await readdir(root)).sort();

  await assert.rejects(
    bundle.terminalCollector.collect(
      exact({ terminalRecord: records[4] }),
    ),
    stateConflict,
  );
  assert.deepEqual((await readdir(root)).sort(), before);
});

test("collection rejects future revisions and broken terminal chains before deletion", async (t) => {
  for (const scenario of [
    "foreign-pending",
    "future-record",
    "future-sidecar",
    "missing-anchor",
  ]) {
    await t.test(scenario, async (subtest) => {
      const parent = await mkdtemp(
        join(await realpath(tmpdir()), "podman-writer-state-future-test-"),
      );
      const root = join(parent, "state");
      subtest.after(() => rm(parent, { force: true, recursive: true }));
      const bundle = createPodmanWriterSupervisorStateBundle(exact({ root }));
      const records = await createStoppedChain(bundle.state);
      const entries = (await readdir(root)).sort();
      const terminalEntry = entries.find((entry) => entry.endsWith(".4.json"));
      assert.equal(typeof terminalEntry, "string");
      if (scenario === "foreign-pending") {
        await writeFile(
          `${join(root, terminalEntry)}.pending`,
          `${JSON.stringify(records[4])}\n`,
          { flag: "wx", mode: 0o600 },
        );
      } else if (scenario === "future-record") {
        await writeFile(
          join(root, terminalEntry.replace(".4.json", ".5.json")),
          `${JSON.stringify(records[4])}\n`,
          { flag: "wx", mode: 0o600 },
        );
      } else if (scenario === "future-sidecar") {
        await writeFile(
          `${join(root, terminalEntry.replace(".4.json", ".9.json"))}.ready`,
          `${"0".repeat(64)}\n`,
          { flag: "wx", mode: 0o600 },
        );
      } else {
        await rm(join(root, terminalEntry));
      }
      const before = (await readdir(root)).sort();
      await assert.rejects(
        bundle.terminalCollector.collect(
          exact({ terminalRecord: records[4] }),
        ),
        stateConflict,
      );
      assert.deepEqual((await readdir(root)).sort(), before);
    });
  }
});

test("collection revalidates terminal content through its held file after unlink", async (t) => {
  const parent = await mkdtemp(
    join(await realpath(tmpdir()), "podman-writer-state-content-race-test-"),
  );
  const root = join(parent, "state");
  t.after(() => rm(parent, { force: true, recursive: true }));
  const writer = createPodmanWriterSupervisorStateBundle(exact({ root }));
  const records = await createStoppedChain(writer.state);
  const terminalEntry = (await readdir(root)).find((entry) =>
    entry.endsWith(".4.json"),
  );
  assert.equal(typeof terminalEntry, "string");
  const terminalPath = join(root, terminalEntry);
  const mutated = record("stopped", {
    stopProofId: `podman-stopped:${"f".repeat(64)}`,
  });
  const mutatedBytes = Buffer.from(`${JSON.stringify(mutated)}\n`, "utf8");
  const attackerHandle = await open(terminalPath, "r+");
  t.after(() => attackerHandle.close());
  let unlinks = 0;
  const collector = createPodmanWriterSupervisorStateBundle(
    exact({
      faultHooks: exact({
        async afterCollectionArtifactUnlink() {
          unlinks += 1;
          if (unlinks === 5) {
            await attackerHandle.writeFile(mutatedBytes);
            await attackerHandle.sync();
          }
        },
      }),
      root,
    }),
  );

  await assert.rejects(
    collector.terminalCollector.collect(
      exact({ terminalRecord: records[4] }),
    ),
    collectionUncertain,
  );
  await assert.rejects(lstat(terminalPath), (error) => error?.code === "ENOENT");
  const heldStat = await attackerHandle.stat({ bigint: true });
  assert.equal(heldStat.nlink, 0n);
  const observedBytes = Buffer.alloc(mutatedBytes.length);
  const observed = await attackerHandle.read(
    observedBytes,
    0,
    observedBytes.length,
    0,
  );
  assert.equal(observed.bytesRead, mutatedBytes.length);
  assert.equal(observedBytes.equals(mutatedBytes), true);
});

test("partial collection retries retain the uncertain-outcome fence", async (t) => {
  const parent = await mkdtemp(
    join(await realpath(tmpdir()), "podman-writer-state-partial-retry-test-"),
  );
  const root = join(parent, "state");
  t.after(() => rm(parent, { force: true, recursive: true }));
  const initial = createPodmanWriterSupervisorStateBundle(exact({ root }));
  const records = await createStoppedChain(initial.state);
  const partial = createPodmanWriterSupervisorStateBundle(
    exact({
      faultHooks: exact({
        afterCollectionFirstDirectorySync() {
          throw new Error("simulated first-phase crash");
        },
      }),
      root,
    }),
  );
  await assert.rejects(
    partial.terminalCollector.collect(exact({ terminalRecord: records[4] })),
    collectionUncertain,
  );

  const retry = createPodmanWriterSupervisorStateBundle(
    exact({
      faultHooks: exact({
        afterCollectionTerminalRevalidation() {
          throw new Error("simulated retry crash");
        },
      }),
      root,
    }),
  );
  await assert.rejects(
    retry.terminalCollector.collect(exact({ terminalRecord: records[4] })),
    collectionUncertain,
  );
});

test("an absent retry fsyncs the state root before completing", async (t) => {
  const parent = await mkdtemp(
    join(await realpath(tmpdir()), "podman-writer-state-absent-sync-test-"),
  );
  const root = join(parent, "state");
  t.after(() => rm(parent, { force: true, recursive: true }));
  const initial = createPodmanWriterSupervisorStateBundle(exact({ root }));
  const records = await createStoppedChain(initial.state);
  const interrupted = createPodmanWriterSupervisorStateBundle(
    exact({
      faultHooks: exact({
        afterCollectionTerminalUnlink() {
          throw new Error("simulated crash before final directory sync");
        },
      }),
      root,
    }),
  );
  await assert.rejects(
    interrupted.terminalCollector.collect(
      exact({ terminalRecord: records[4] }),
    ),
    collectionUncertain,
  );
  assert.deepEqual(await readdir(root), []);

  let finalSyncs = 0;
  const recovery = createPodmanWriterSupervisorStateBundle(
    exact({
      faultHooks: exact({
        afterCollectionFinalDirectorySync() {
          finalSyncs += 1;
        },
      }),
      root,
    }),
  );
  const receipt = await recovery.terminalCollector.collect(
    exact({ terminalRecord: records[4] }),
  );
  assert.equal(receipt.status, "absent");
  assert.equal(finalSyncs, 1);
  assert.deepEqual(await readdir(root), []);
});

test("a missing-root collection rejects parent replacement before reporting absence", async (t) => {
  const sandbox = await mkdtemp(
    join(await realpath(tmpdir()), "podman-writer-state-missing-root-race-test-"),
  );
  const parent = join(sandbox, "private-parent");
  const displacedParent = join(sandbox, "displaced-private-parent");
  const root = join(parent, "state");
  await mkdir(parent, { mode: 0o700 });
  t.after(() => rm(sandbox, { force: true, recursive: true }));
  const writer = createPodmanWriterSupervisorStateBundle(exact({ root }));
  const records = await createStoppedChain(writer.state);
  const before = (await readdir(root)).sort();

  let replacements = 0;
  const collector = createPodmanWriterSupervisorStateBundle(
    exact({
      faultHooks: exact({
        async afterPrivateParentHold() {
          await rename(parent, displacedParent);
          await mkdir(parent, { mode: 0o700 });
          replacements += 1;
        },
      }),
      root,
    }),
  );

  await assert.rejects(
    collector.terminalCollector.collect(
      exact({ terminalRecord: records[4] }),
    ),
    stateIoError,
  );
  assert.equal(replacements, 1);
  const heldParentStat = await lstat(displacedParent, { bigint: true });
  const namedParentStat = await lstat(parent, { bigint: true });
  assert.equal(
    heldParentStat.dev === namedParentStat.dev &&
      heldParentStat.ino === namedParentStat.ino,
    false,
  );
  await assert.rejects(lstat(root), (error) => error?.code === "ENOENT");
  assert.deepEqual(
    (await readdir(join(displacedParent, "state"))).sort(),
    before,
  );
  const retained = createPodmanWriterSupervisorStateBundle(
    exact({ root: join(displacedParent, "state") }),
  );
  assert.deepEqual(
    await retained.state.read(
      exact({ launchAttemptId: records[4].launchAttemptId }),
    ),
    records[4],
  );
});

test("a missing-root collection requires parent fsync acknowledgement before absence", async (t) => {
  const parent = await mkdtemp(
    join(await realpath(tmpdir()), "podman-writer-state-missing-root-sync-test-"),
  );
  const root = join(parent, "state");
  t.after(() => rm(parent, { force: true, recursive: true }));
  const terminalRecord = record("stopped");
  const interrupted = createPodmanWriterSupervisorStateBundle(
    exact({
      faultHooks: exact({
        afterParentDirectorySync() {
          throw new Error("simulated missing-root parent fsync acknowledgement loss");
        },
      }),
      root,
    }),
  );

  await assert.rejects(
    interrupted.terminalCollector.collect(exact({ terminalRecord })),
    stateIoError,
  );
  await assert.rejects(lstat(root), (error) => error?.code === "ENOENT");

  const cold = createPodmanWriterSupervisorStateBundle(exact({ root }));
  assert.equal(
    (await cold.terminalCollector.collect(exact({ terminalRecord }))).status,
    "absent",
  );
});

test("every collection unlink and fsync crash prefix is recoverable without state rollback", async (t) => {
  const scenarios = [];
  for (let ordinal = 1; ordinal <= 5; ordinal += 1) {
    scenarios.push({ hook: "afterCollectionArtifactUnlink", ordinal });
  }
  for (const hook of [
    "afterCollectionFirstDirectorySync",
    "afterCollectionTerminalRevalidation",
    "afterCollectionTerminalUnlink",
    "afterCollectionFinalDirectorySync",
  ]) {
    scenarios.push({ hook, ordinal: 1 });
  }

  for (const scenario of scenarios) {
    await t.test(`${scenario.hook}-${scenario.ordinal}`, async (subtest) => {
      const parent = await mkdtemp(
        join(await realpath(tmpdir()), "podman-writer-state-crash-test-"),
      );
      const root = join(parent, "state");
      subtest.after(() => rm(parent, { force: true, recursive: true }));
      const initial = createPodmanWriterSupervisorStateBundle(exact({ root }));
      const records = await createStoppedChain(initial.state);
      let calls = 0;
      const crashing = createPodmanWriterSupervisorStateBundle(
        exact({
          faultHooks: exact({
            [scenario.hook]() {
              calls += 1;
              if (calls === scenario.ordinal) {
                throw new Error("simulated collection crash");
              }
            },
          }),
          root,
        }),
      );
      await assert.rejects(
        crashing.terminalCollector.collect(
          exact({ terminalRecord: records[4] }),
        ),
        collectionUncertain,
      );

      const recovery = createPodmanWriterSupervisorStateBundle(exact({ root }));
      try {
        const observed = await recovery.state.read(
          exact({ launchAttemptId: records[4].launchAttemptId }),
        );
        assert.equal(observed, null);
      } catch (error) {
        assert.equal(
          error instanceof PodmanWriterSupervisorStateError &&
            error.code === "podman_writer_state_invalid",
          true,
        );
      }
      const receipt = await recovery.terminalCollector.collect(
        exact({ terminalRecord: records[4] }),
      );
      assert.equal(
        receipt.status === "collected" || receipt.status === "absent",
        true,
      );
      assert.deepEqual(await readdir(root), []);
      assert.equal(
        await recovery.state.read(
          exact({ launchAttemptId: records[4].launchAttemptId }),
        ),
        null,
      );
    });
  }
});

test("collection allows benign directory churn and serializes same-instance reads", async (t) => {
  const parent = await mkdtemp(
    join(await realpath(tmpdir()), "podman-writer-state-churn-test-"),
  );
  const root = join(parent, "state");
  t.after(() => rm(parent, { force: true, recursive: true }));
  const writer = createPodmanWriterSupervisorStateBundle(exact({ root }));
  const records = await createStoppedChain(writer.state);
  let observedTerminal;
  const terminalObserved = new Promise((resolve) => {
    observedTerminal = resolve;
  });
  let releaseTerminal;
  const terminalGate = new Promise((resolve) => {
    releaseTerminal = resolve;
  });
  const collecting = createPodmanWriterSupervisorStateBundle(
    exact({
      faultHooks: exact({
        async afterCollectionTerminalRevalidation() {
          const churn = join(root, "benign-child-churn");
          await writeFile(churn, "benign\n", { flag: "wx", mode: 0o600 });
          await rm(churn);
          observedTerminal();
          await terminalGate;
        },
      }),
      root,
    }),
  );
  const pendingCollection = collecting.terminalCollector.collect(
    exact({ terminalRecord: records[4] }),
  );
  await terminalObserved;
  let readSettled = false;
  const pendingRead = collecting.state
    .read(exact({ launchAttemptId: records[4].launchAttemptId }))
    .finally(() => {
      readSettled = true;
    });
  await new Promise((resolve) => setTimeout(resolve, 0));
  assert.equal(readSettled, false);
  releaseTerminal();
  assert.equal((await pendingCollection).status, "collected");
  assert.equal(await pendingRead, null);
});

test("collection revalidates benign file link churn without treating ctime as content", async (t) => {
  const parent = await mkdtemp(
    join(await realpath(tmpdir()), "podman-writer-state-file-churn-test-"),
  );
  const root = join(parent, "state");
  t.after(() => rm(parent, { force: true, recursive: true }));
  const writer = createPodmanWriterSupervisorStateBundle(exact({ root }));
  const records = await createStoppedChain(writer.state);
  const revisionZero = (await readdir(root)).find((entry) =>
    entry.endsWith(".0.json"),
  );
  assert.equal(typeof revisionZero, "string");
  const revisionZeroPath = join(root, revisionZero);
  const pendingPath = `${revisionZeroPath}.pending`;
  await link(revisionZeroPath, pendingPath);

  let reads = 0;
  const collector = createPodmanWriterSupervisorStateBundle(
    exact({
      faultHooks: exact({
        async afterCollectionFileFirstRead() {
          reads += 1;
          if (reads === 1) await rm(pendingPath);
        },
      }),
      root,
    }),
  );
  const receipt = await collector.terminalCollector.collect(
    exact({ terminalRecord: records[4] }),
  );
  assert.equal(receipt.status, "collected");
  assert.equal(reads >= 5, true);
  assert.deepEqual(await readdir(root), []);
});

test("collection rejects file content mutation during held-file revalidation", async (t) => {
  const parent = await mkdtemp(
    join(await realpath(tmpdir()), "podman-writer-state-file-content-test-"),
  );
  const root = join(parent, "state");
  t.after(() => rm(parent, { force: true, recursive: true }));
  const writer = createPodmanWriterSupervisorStateBundle(exact({ root }));
  const records = await createStoppedChain(writer.state);
  const revisionZero = (await readdir(root)).find((entry) =>
    entry.endsWith(".0.json"),
  );
  assert.equal(typeof revisionZero, "string");
  const revisionZeroPath = join(root, revisionZero);
  const before = (await readdir(root)).sort();

  let reads = 0;
  const collector = createPodmanWriterSupervisorStateBundle(
    exact({
      faultHooks: exact({
        async afterCollectionFileFirstRead() {
          reads += 1;
          if (reads === 1) {
            await writeFile(revisionZeroPath, "mutated\n");
          }
        },
      }),
      root,
    }),
  );
  await assert.rejects(
    collector.terminalCollector.collect(
      exact({ terminalRecord: records[4] }),
    ),
    stateIoError,
  );
  assert.equal(reads, 1);
  assert.deepEqual((await readdir(root)).sort(), before);
});

test("concurrent cold collectors cannot expose an earlier valid prefix", async (t) => {
  const parent = await mkdtemp(
    join(await realpath(tmpdir()), "podman-writer-state-concurrent-gc-test-"),
  );
  const root = join(parent, "state");
  t.after(() => rm(parent, { force: true, recursive: true }));
  const writer = createPodmanWriterSupervisorStateBundle(exact({ root }));
  const records = await createStoppedChain(writer.state);
  const left = createPodmanWriterSupervisorStateBundle(exact({ root }));
  const right = createPodmanWriterSupervisorStateBundle(exact({ root }));
  const results = await Promise.allSettled([
    left.terminalCollector.collect(exact({ terminalRecord: records[4] })),
    right.terminalCollector.collect(exact({ terminalRecord: records[4] })),
  ]);
  assert.equal(results.some((result) => result.status === "fulfilled"), true);
  const recovery = createPodmanWriterSupervisorStateBundle(exact({ root }));
  await recovery.terminalCollector.collect(
    exact({ terminalRecord: records[4] }),
  );
  assert.deepEqual(await readdir(root), []);
  assert.equal(
    await recovery.state.read(
      exact({ launchAttemptId: records[4].launchAttemptId }),
    ),
    null,
  );
});

test("concurrent cold collectors accept only monotonic deletion of recognized aliases", async (t) => {
  const parent = await mkdtemp(
    join(await realpath(tmpdir()), "podman-writer-state-concurrent-alias-gc-test-"),
  );
  const root = join(parent, "state");
  t.after(() => rm(parent, { force: true, recursive: true }));
  const writer = createPodmanWriterSupervisorStateBundle(exact({ root }));
  const records = await createStoppedChain(writer.state);
  const terminalEntry = (await readdir(root)).find((entry) =>
    entry.endsWith(".4.json"),
  );
  assert.equal(typeof terminalEntry, "string");
  const terminalPath = join(root, terminalEntry);
  const terminalBytes = await readFile(terminalPath);
  await writeFile(
    `${terminalPath}.ready`,
    `${createHash("sha256").update(terminalBytes).digest("hex")}\n`,
    { flag: "wx", mode: 0o600 },
  );
  await link(terminalPath, `${terminalPath}.pending`);

  let terminalPendingRemoved;
  const terminalPendingRemoval = new Promise((resolve) => {
    terminalPendingRemoved = resolve;
  });
  let releaseFirstCollector;
  const firstCollectorGate = new Promise((resolve) => {
    releaseFirstCollector = resolve;
  });
  let unlinks = 0;
  const first = createPodmanWriterSupervisorStateBundle(exact({
    faultHooks: exact({
      async afterCollectionArtifactUnlink() {
        unlinks += 1;
        if (unlinks === 6) {
          terminalPendingRemoved();
          await firstCollectorGate;
        }
      },
    }),
    root,
  }));
  const firstResult = first.terminalCollector.collect(
    exact({ terminalRecord: records[4] }),
  );
  await terminalPendingRemoval;

  const second = createPodmanWriterSupervisorStateBundle(exact({ root }));
  assert.equal(
    (await second.terminalCollector.collect(
      exact({ terminalRecord: records[4] }),
    )).status,
    "collected",
  );
  releaseFirstCollector();
  assert.equal((await firstResult).status, "collected");
  assert.deepEqual(await readdir(root), []);

  const replay = createPodmanWriterSupervisorStateBundle(exact({ root }));
  assert.equal(
    (await replay.terminalCollector.collect(
      exact({ terminalRecord: records[4] }),
    )).status,
    "absent",
  );
});

test("read-only absence does not create or chmod a state root", async (t) => {
  const { root, state } = await fixture(t);
  assert.equal(
    await state.read(exact({ launchAttemptId: "launch-attempt-absent" })),
    null,
  );
  await assert.rejects(lstat(root), (error) => error?.code === "ENOENT");
});

test("cold retry closes root and parent directory fsync acknowledgement loss", async (t) => {
  for (const hookName of [
    "afterRootDirectorySync",
    "afterParentDirectorySync",
  ]) {
    await t.test(hookName, async (subtest) => {
      const { root } = await fixture(subtest);
      const initial = record("preparing");
      await assert.rejects(
        faultingState(root, hookName).claim(exact({ record: initial })),
        stateIoError,
      );
      assert.deepEqual(await readdir(root), []);

      const cold = createPodmanWriterSupervisorState(exact({ root }));
      assert.equal(
        await cold.read(exact({ launchAttemptId: initial.launchAttemptId })),
        null,
      );
      const replay = await cold.claim(exact({ record: initial }));
      assert.equal(replay.created, true);
      assert.deepEqual(replay.record, initial);
      assert.equal((await readdir(root)).length, 1);
    });
  }
});

test("requires an owner-private parent and never traverses symlink ancestors", async (t) => {
  const sandbox = await mkdtemp(
    join(await realpath(tmpdir()), "podman-writer-state-policy-test-"),
  );
  t.after(() => rm(sandbox, { force: true, recursive: true }));
  const stateIoError = (error) =>
    error instanceof PodmanWriterSupervisorStateError &&
    error.code === "podman_writer_state_io_failed";

  const target = join(sandbox, "target");
  const targetParent = join(target, "private-parent");
  await mkdir(target, { mode: 0o700 });
  await mkdir(targetParent, { mode: 0o700 });
  const link = join(sandbox, "link");
  await symlink(target, link);
  const linkedRoot = join(link, "private-parent", "state");
  const linkedState = createPodmanWriterSupervisorState(
    exact({ root: linkedRoot }),
  );
  await assert.rejects(
    linkedState.claim(exact({ record: record("preparing") })),
    stateIoError,
  );
  await assert.rejects(
    lstat(join(targetParent, "state")),
    (error) => error?.code === "ENOENT",
  );

  const publicParent = join(sandbox, "public-parent");
  await mkdir(publicParent, { mode: 0o700 });
  await chmod(publicParent, 0o755);
  const publicRoot = join(publicParent, "state");
  const publicState = createPodmanWriterSupervisorState(
    exact({ root: publicRoot }),
  );
  await assert.rejects(
    publicState.claim(exact({ record: record("preparing") })),
    stateIoError,
  );
  await assert.rejects(lstat(publicRoot), (error) => error?.code === "ENOENT");

  const privateParent = join(sandbox, "private-parent");
  const publicExistingRoot = join(privateParent, "state");
  await mkdir(privateParent, { mode: 0o700 });
  await mkdir(publicExistingRoot, { mode: 0o755 });
  const publicExistingState = createPodmanWriterSupervisorState(
    exact({ root: publicExistingRoot }),
  );
  await assert.rejects(
    publicExistingState.claim(exact({ record: record("preparing") })),
    stateIoError,
  );
  const publicExistingStat = await lstat(publicExistingRoot, { bigint: true });
  assert.equal(Number(publicExistingStat.mode & 0o777n), 0o755);

  const missingRoot = join(sandbox, "missing-parent", "state");
  const missingState = createPodmanWriterSupervisorState(
    exact({ root: missingRoot }),
  );
  await assert.rejects(
    missingState.claim(exact({ record: record("preparing") })),
    stateIoError,
  );
  await assert.rejects(
    lstat(join(sandbox, "missing-parent")),
    (error) => error?.code === "ENOENT",
  );
});

test("held state roots reject every special mode bit", async (t) => {
  for (const [name, specialBit] of [
    ["sticky", 0o1000],
    ["setgid", 0o2000],
    ["setuid", 0o4000],
  ]) {
    await t.test(name, async (subtest) => {
      const { root } = await fixture(subtest);
      const unsafeMode = 0o700 | specialBit;
      let mutated = false;
      let modeSupported = true;
      const state = stateWithFaultHooks(
        root,
        exact({
          async afterRootDirectorySync() {
            mutated = true;
            await chmod(root, unsafeMode);
            modeSupported =
              Number((await lstat(root, { bigint: true })).mode & 0o7777n) ===
              unsafeMode;
          },
        }),
      );

      let captured;
      try {
        await state.claim(exact({ record: record("preparing") }));
      } catch (error) {
        captured = error;
      }
      assert.equal(mutated, true);
      if (!modeSupported) {
        subtest.skip("host filesystem does not retain this state-root mode bit");
        return;
      }
      assert.notEqual(captured, undefined);
      assert.equal(stateIoError(captured), true);
      assert.deepEqual(await readdir(root), []);
    });
  }
});

test("pending state files reject every special mode bit before publication", async (t) => {
  for (const [name, specialBit] of [
    ["sticky", 0o1000],
    ["setgid", 0o2000],
    ["setuid", 0o4000],
  ]) {
    await t.test(name, async (subtest) => {
      const { root } = await fixture(subtest);
      const unsafeMode = 0o600 | specialBit;
      let pendingPath;
      let modeSupported = true;
      const state = stateWithFaultHooks(
        root,
        exact({
          async afterTemporarySync() {
            const entries = await readdir(root);
            assert.equal(entries.length, 1);
            assert.match(entries[0], /\.json\.pending$/u);
            pendingPath = join(root, entries[0]);
            await chmod(pendingPath, unsafeMode);
            modeSupported =
              Number(
                (await lstat(pendingPath, { bigint: true })).mode & 0o7777n,
              ) === unsafeMode;
          },
        }),
      );

      let captured;
      try {
        await state.claim(exact({ record: record("preparing") }));
      } catch (error) {
        captured = error;
      }
      assert.notEqual(pendingPath, undefined);
      if (!modeSupported) {
        subtest.skip("host filesystem does not retain this pending-file mode bit");
        return;
      }
      assert.notEqual(captured, undefined);
      assert.equal(stateIoError(captured), true);
      assert.deepEqual(await readdir(root), [pendingPath.slice(root.length + 1)]);
    });
  }
});

test("published state files with special mode bits are neither read nor cleaned", async (t) => {
  for (const [name, specialBit] of [
    ["sticky", 0o1000],
    ["setgid", 0o2000],
    ["setuid", 0o4000],
  ]) {
    await t.test(name, async (subtest) => {
      const { root, state } = await fixture(subtest);
      const initial = record("preparing");
      await state.claim(exact({ record: initial }));
      const entries = await readdir(root);
      assert.equal(entries.length, 1);
      const path = join(root, entries[0]);
      const unsafeMode = 0o600 | specialBit;
      await chmod(path, unsafeMode);
      const observedMode = Number(
        (await lstat(path, { bigint: true })).mode & 0o7777n,
      );
      if (observedMode !== unsafeMode) {
        subtest.skip("host filesystem does not retain this published-file mode bit");
        return;
      }

      await assert.rejects(
        state.read(exact({ launchAttemptId: initial.launchAttemptId })),
        stateIoError,
      );
      assert.deepEqual(await readdir(root), entries);
    });
  }
});

test("claims one launcher, replays the winner, and rejects conflicting transitions", async (t) => {
  const { state } = await fixture(t);
  const initial = record("preparing");
  const [left, right] = await Promise.all([
    state.claim(exact({ record: initial })),
    state.claim(exact({ record: initial })),
  ]);
  assert.deepEqual([left.created, right.created].sort(), [false, true]);
  assert.deepEqual(left.record, right.record);

  const created = record("created");
  assert.deepEqual(await transition(state, initial, created), created);
  assert.deepEqual(await transition(state, initial, created), created);
  await assert.rejects(
    transition(
      state,
      initial,
      record("created", { containerId: "f".repeat(64) }),
    ),
    (error) =>
      error instanceof PodmanWriterSupervisorStateError &&
      error.code === "podman_writer_state_conflict",
  );
});

test("cross-instance recovery cleanup preserves the publishing claim winner", async (t) => {
  const { root } = await fixture(t);
  const initial = record("preparing");
  let observePublish;
  const publishObserved = new Promise((resolve) => {
    observePublish = resolve;
  });
  let releasePublisher;
  const publisherGate = new Promise((resolve) => {
    releasePublisher = resolve;
  });
  const publisher = stateWithFaultHooks(
    root,
    exact({
      afterPublish() {
        assert.equal(this, undefined);
        observePublish();
        return publisherGate;
      },
    }),
  );
  const recovery = createPodmanWriterSupervisorState(exact({ root }));

  const published = publisher.claim(exact({ record: initial }));
  const recovered = (async () => {
    await publishObserved;
    try {
      return await recovery.claim(exact({ record: initial }));
    } finally {
      releasePublisher();
    }
  })();
  const [publisherReceipt, recoveryReceipt] = await Promise.all([
    published,
    recovered,
  ]);

  assert.equal(publisherReceipt.created, true);
  assert.equal(recoveryReceipt.created, false);
  assert.deepEqual(publisherReceipt.record, initial);
  assert.deepEqual(recoveryReceipt.record, initial);
  const entries = await readdir(root);
  assert.equal(entries.length, 1);
  const [entry] = entries;
  assert.match(entry, /^[0-9a-f]{64}\.0\.json$/u);
  const stat = await lstat(join(root, entry), { bigint: true });
  assert.equal(stat.nlink, 1n);
});

test("every publication crash prefix exposes only the old or complete new revision", async (t) => {
  const crashPrefixes = [
    ["afterTemporaryWrite", false],
    ["afterTemporarySync", false],
    ["afterPublish", true],
    ["afterPublishDirectorySync", true],
    ["afterCleanup", true],
    ["afterCleanupDirectorySync", true],
  ];

  for (const [hookName, published] of crashPrefixes) {
    await t.test(hookName, async (subtest) => {
      const { root } = await fixture(subtest);
      const initial = record("preparing");
      await assert.rejects(
        faultingState(root, hookName).claim(exact({ record: initial })),
        stateIoError,
      );

      const recovered = createPodmanWriterSupervisorState(exact({ root }));
      assert.deepEqual(
        await recovered.read(exact({ launchAttemptId: initial.launchAttemptId })),
        published ? initial : null,
      );
      const replay = await recovered.claim(exact({ record: initial }));
      assert.equal(replay.created, !published);
      assert.deepEqual(replay.record, initial);

      const entries = await readdir(root);
      assert.equal(entries.length, 1);
      assert.match(entries[0], /^[0-9a-f]{64}\.0\.json$/u);
    });
  }
});

test("partial or mismatched unpublished precursors remain invisible and fail closed", async (t) => {
  const { root } = await fixture(t);
  const initial = record("preparing");
  await assert.rejects(
    faultingState(root, "afterTemporaryWrite").claim(exact({ record: initial })),
    stateIoError,
  );
  const [pendingEntry] = await readdir(root);
  assert.match(pendingEntry, /^[0-9a-f]{64}\.0\.json\.pending$/u);
  const pendingPath = join(root, pendingEntry);
  const recovered = createPodmanWriterSupervisorState(exact({ root }));

  await writeFile(pendingPath, "{\n");
  assert.equal(
    await recovered.read(exact({ launchAttemptId: initial.launchAttemptId })),
    null,
  );
  await assert.rejects(
    recovered.claim(exact({ record: initial })),
    stateIoError,
  );

  const mismatched = record("preparing", { requestSha256: "f".repeat(64) });
  await writeFile(pendingPath, `${JSON.stringify(mismatched)}\n`);
  assert.equal(
    await recovered.read(exact({ launchAttemptId: initial.launchAttemptId })),
    null,
  );
  await assert.rejects(
    recovered.claim(exact({ record: initial })),
    stateIoError,
  );
});

test("transition retry adopts only complete precursors and replays published effects", async (t) => {
  const crashPrefixes = [
    ["afterTemporaryWrite", false],
    ["afterTemporarySync", false],
    ["afterPublish", true],
    ["afterPublishDirectorySync", true],
    ["afterCleanup", true],
    ["afterCleanupDirectorySync", true],
  ];

  for (const [hookName, published] of crashPrefixes) {
    await t.test(hookName, async (subtest) => {
      const { root, state } = await fixture(subtest);
      const before = record("preparing");
      const after = record("created");
      await state.claim(exact({ record: before }));

      await assert.rejects(
        transition(faultingState(root, hookName), before, after),
        stateIoError,
      );
      const recovered = createPodmanWriterSupervisorState(exact({ root }));
      assert.deepEqual(
        await recovered.read(exact({ launchAttemptId: before.launchAttemptId })),
        published ? after : before,
      );
      assert.deepEqual(await transition(recovered, before, after), after);
      assert.deepEqual(
        await recovered.read(exact({ launchAttemptId: before.launchAttemptId })),
        after,
      );
      assert.equal((await readdir(root)).length, 2);
    });
  }
});

test("recovers the complete durable chain from a new state instance", async (t) => {
  const { root, state } = await fixture(t);
  const states = [
    record("preparing"),
    record("created"),
    record("started"),
    record("stopping"),
    record("stopped"),
  ];
  await state.claim(exact({ record: states[0] }));
  for (let index = 1; index < states.length; index += 1) {
    await transition(state, states[index - 1], states[index]);
  }

  const recovered = createPodmanWriterSupervisorState(exact({ root }));
  assert.deepEqual(
    await recovered.read(exact({ launchAttemptId: "launch-attempt-001" })),
    states.at(-1),
  );
  assert.equal(
    await recovered.read(exact({ launchAttemptId: "launch-attempt-missing" })),
    null,
  );
});

test("adopts canonical data-only precursors and validates legacy ready markers", async (t) => {
  const { root, state } = await fixture(t);
  const initial = record("preparing");
  await state.claim(exact({ record: initial }));
  const [entry] = await readdir(root);
  const path = join(root, entry);
  const bytes = await readFile(path);
  const readyPath = `${path}.ready`;
  await writeFile(
    readyPath,
    `${createHash("sha256").update(bytes).digest("hex")}\n`,
    { flag: "wx", mode: 0o600 },
  );

  const recovered = createPodmanWriterSupervisorState(exact({ root }));
  assert.deepEqual(
    await recovered.read(exact({ launchAttemptId: initial.launchAttemptId })),
    initial,
  );

  await writeFile(readyPath, `${"0".repeat(64)}\n`);
  await assert.rejects(
    recovered.read(exact({ launchAttemptId: initial.launchAttemptId })),
    stateIoError,
  );

  await rm(path);
  await assert.rejects(
    recovered.read(exact({ launchAttemptId: initial.launchAttemptId })),
    stateIoError,
  );
});

test("fails closed on non-canonical or partial published precursors", async (t) => {
  const { root, state } = await fixture(t);
  const initial = record("preparing");
  await state.claim(exact({ record: initial }));
  const [entry] = await readdir(root);
  const path = join(root, entry);
  const canonical = await readFile(path, "utf8");
  const stateInvalid = (error) =>
    error instanceof PodmanWriterSupervisorStateError &&
    error.code === "podman_writer_state_invalid";

  await writeFile(path, `${canonical.slice(0, -1)} \n`);
  const recovered = createPodmanWriterSupervisorState(exact({ root }));
  await assert.rejects(
    recovered.read(exact({ launchAttemptId: initial.launchAttemptId })),
    stateInvalid,
  );

  await writeFile(path, "{\n");
  await assert.rejects(
    recovered.read(exact({ launchAttemptId: initial.launchAttemptId })),
    stateInvalid,
  );
});

test("rejects an unrecognized hard-link access path", async (t) => {
  const { root, state } = await fixture(t);
  const initial = record("preparing");
  await state.claim(exact({ record: initial }));
  const [entry] = await readdir(root);
  await link(join(root, entry), join(root, "unexpected-alias"));

  const recovered = createPodmanWriterSupervisorState(exact({ root }));
  await assert.rejects(
    recovered.read(exact({ launchAttemptId: initial.launchAttemptId })),
    stateIoError,
  );
});

test("state bytes contain no raw attachment, image, device, or provider authority", async (t) => {
  const { root, state } = await fixture(t);
  await state.claim(exact({ record: record("preparing") }));
  await transition(state, record("preparing"), record("created"));
  const entries = await readdir(root);
  const bytes = (
    await Promise.all(entries.map((entry) => readFile(join(root, entry), "utf8")))
  ).join("\n");
  for (const authority of [
    "/private/attachment",
    "/provider/image",
    "/dev/mapper/session",
    "registry.example.invalid",
  ]) {
    assert.equal(bytes.includes(authority), false);
  }
});

test("reserves the native pathname budget for pending state records", () => {
  const asciiSegments = [
    ...new Array(19).fill("a".repeat(199)),
    "a".repeat(214),
  ];
  const maximumAsciiRoot = `/${asciiSegments.join("/")}`;
  const overlongAsciiRoot = `${maximumAsciiRoot}a`;
  const utf8Segments = [
    ...new Array(19).fill("é".repeat(100)),
    `${"é".repeat(97)}a`,
  ];
  const maximumUtf8Root = `/${utf8Segments.join("/")}`;
  const overlongUtf8Root = `${maximumUtf8Root}a`;
  const pendingSuffix = `/${"f".repeat(64)}.4.json.pending`;

  assert.equal(Buffer.byteLength(pendingSuffix, "utf8"), 80);
  for (const root of [maximumAsciiRoot, maximumUtf8Root]) {
    assert.equal(Buffer.byteLength(root, "utf8"), 4_015);
    assert.equal(Buffer.byteLength(`${root}${pendingSuffix}`, "utf8"), 4_095);
    assert.equal(
      isPodmanWriterSupervisorState(
        createPodmanWriterSupervisorState(exact({ root })),
      ),
      true,
    );
  }
  for (const root of [
    overlongAsciiRoot,
    overlongUtf8Root,
    "/state/\ud800",
  ]) {
    assert.throws(
      () => createPodmanWriterSupervisorState(exact({ root })),
      (error) =>
        error instanceof PodmanWriterSupervisorStateError &&
        error.code === "podman_writer_state_invalid",
    );
  }
});

test(
  "state paths survive post-import crypto and path intrinsic poisoning",
  { concurrency: false },
  async (t) => {
    const { root } = await fixture(t);
    const initial = record("preparing");
    const expectedKey = createHash("sha256")
      .update("portable-codex-runtime:podman-writer-state:v1\0", "utf8")
      .update(initial.launchAttemptId, "utf8")
      .digest("hex");
    const specifications = [
      [crypto, "createHash"],
      [path, "basename"],
      [path, "dirname"],
      [path, "isAbsolute"],
      [path, "resolve"],
    ];
    const originals = specifications.map(([target, key]) =>
      Object.getOwnPropertyDescriptor(target, key)
    );
    const poisonCalls = new Map(specifications.map(([, key]) => [key, 0]));
    try {
      for (let index = 0; index < specifications.length; index += 1) {
        const [target, key] = specifications[index];
        const descriptor = originals[index];
        assert.equal(typeof descriptor?.value, "function");
        Object.defineProperty(target, key, {
          ...descriptor,
          value() {
            poisonCalls.set(key, poisonCalls.get(key) + 1);
            throw new Error(`poisoned node intrinsic: ${key}`);
          },
        });
      }
      syncBuiltinESMExports();
      const state = createPodmanWriterSupervisorState(exact({ root }));
      assert.equal(
        await state.read(exact({ launchAttemptId: "launch-attempt-absent" })),
        null,
      );
      const claim = await state.claim(exact({ record: initial }));
      assert.equal(claim.created, true);
      assert.deepEqual(await readdir(root), [`${expectedKey}.0.json`]);
      assert.deepEqual(
        await state.read(exact({ launchAttemptId: initial.launchAttemptId })),
        initial,
      );
    } finally {
      for (let index = 0; index < specifications.length; index += 1) {
        Object.defineProperty(
          specifications[index][0],
          specifications[index][1],
          originals[index],
        );
      }
      syncBuiltinESMExports();
    }
    assert.deepEqual([...poisonCalls.values()], [0, 0, 0, 0, 0]);
    const restarted = createPodmanWriterSupervisorState(exact({ root }));
    assert.deepEqual(
      await restarted.read(exact({ launchAttemptId: initial.launchAttemptId })),
      initial,
    );
  },
);

test("rejects malformed records, paths, and state transitions", async (t) => {
  const { root, state } = await fixture(t);
  assert.throws(
    () => createPodmanWriterSupervisorState(exact({ root: "relative/state" })),
    PodmanWriterSupervisorStateError,
  );
  assert.throws(
    () => assertPodmanWriterSupervisorStateRecord({ ...record("preparing"), extra: true }),
    PodmanWriterSupervisorStateError,
  );
  await state.claim(exact({ record: record("preparing") }));
  await assert.rejects(
    transition(state, record("preparing"), record("started")),
    PodmanWriterSupervisorStateError,
  );

  const thenableState = stateWithFaultHooks(
    root,
    exact({
      afterRootDirectorySync() {
        return exact({ then() {} });
      },
    }),
  );
  await assert.rejects(
    thenableState.claim(exact({ record: record("preparing") })),
    stateIoError,
  );

  const promiseWithOwnThen = Promise.resolve();
  Object.defineProperty(promiseWithOwnThen, "then", {
    value: Promise.prototype.then,
  });
  const hookedPromiseState = stateWithFaultHooks(
    root,
    exact({
      afterRootDirectorySync() {
        return promiseWithOwnThen;
      },
    }),
  );
  await assert.rejects(
    hookedPromiseState.claim(exact({ record: record("preparing") })),
    stateIoError,
  );
});
