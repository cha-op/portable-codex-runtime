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
  PODMAN_WRITER_SUPERVISOR_STATE_OWNER_CONTRACT_VERSION,
  PodmanWriterSupervisorStateError,
  assertPodmanWriterSupervisorStateRecord,
  createPodmanWriterSupervisorState,
  createPodmanWriterSupervisorStateBundle,
  isPodmanWriterSupervisorState,
  isPodmanWriterSupervisorStateBundle,
  isPodmanWriterSupervisorStateOwner,
  isPodmanWriterSupervisorStateTerminalCollector,
  preparePodmanWriterSupervisorStateOwner,
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

async function prepareOwner(root, expectedStateOwnerId = null) {
  return preparePodmanWriterSupervisorStateOwner(
    exact({ expectedStateOwnerId, root }),
  );
}

async function stateBundle(root, faultHooks = undefined) {
  const owner = await prepareOwner(root);
  return createPodmanWriterSupervisorStateBundle(
    faultHooks === undefined
      ? exact({ owner })
      : exact({ faultHooks, owner }),
  );
}

function collect(bundle, terminalRecord) {
  return bundle.terminalCollector.collect(
    exact({ stateOwnerId: bundle.stateOwnerId, terminalRecord }),
  );
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

function stateInvalid(error) {
  return (
    error instanceof PodmanWriterSupervisorStateError &&
    error.code === "podman_writer_state_invalid"
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

test("prepares a durable branded state owner and adopts it after restart or lost acknowledgement", async (t) => {
  const parent = await mkdtemp(
    join(await realpath(tmpdir()), "podman-writer-state-owner-test-"),
  );
  const root = join(parent, "state");
  t.after(() => rm(parent, { force: true, recursive: true }));

  const owner = await prepareOwner(root);
  assert.equal(isPodmanWriterSupervisorStateOwner(owner), true);
  assert.equal(Object.getPrototypeOf(owner), null);
  assert.equal(Object.isFrozen(owner), true);
  assert.deepEqual(Reflect.ownKeys(owner), ["contractVersion", "stateOwnerId"]);
  assert.equal(
    owner.contractVersion,
    PODMAN_WRITER_SUPERVISOR_STATE_OWNER_CONTRACT_VERSION,
  );
  assert.match(owner.stateOwnerId, /^state-owner:[0-9a-f]{64}$/u);

  const markerPath = join(root, ".state-owner-v1.json");
  const markerBytes = await readFile(markerPath, "utf8");
  assert.equal(
    markerBytes,
    `${JSON.stringify({
      contractVersion: PODMAN_WRITER_SUPERVISOR_STATE_OWNER_CONTRACT_VERSION,
      stateOwnerId: owner.stateOwnerId,
    })}\n`,
  );
  const markerStat = await lstat(markerPath, { bigint: true });
  assert.equal(Number(markerStat.mode & 0o7777n), 0o600);
  assert.equal(markerStat.nlink, 1n);
  assert.equal(markerStat.uid, BigInt(process.getuid()));
  const rootStat = await lstat(root, { bigint: true });
  assert.equal(Number(rootStat.mode & 0o7777n), 0o700);
  assert.equal(rootStat.uid, BigInt(process.getuid()));

  // Both calls model a retry after the initializer completed but its final
  // acknowledgement was lost. Neither retry rewrites the immutable marker.
  const restarted = await prepareOwner(root, owner.stateOwnerId);
  const ackLossRetry = await prepareOwner(root);
  assert.equal(restarted.stateOwnerId, owner.stateOwnerId);
  assert.equal(ackLossRetry.stateOwnerId, owner.stateOwnerId);
  assert.equal(await readFile(markerPath, "utf8"), markerBytes);
  assert.equal(isPodmanWriterSupervisorStateOwner({ ...owner }), false);
});

test("state owner initialization is root-local and never creates for an expected owner", async (t) => {
  const parent = await mkdtemp(
    join(await realpath(tmpdir()), "podman-writer-state-owner-scope-test-"),
  );
  const leftRoot = join(parent, "left");
  const rightRoot = join(parent, "right");
  const missingRoot = join(parent, "missing");
  const unmarkedRoot = join(parent, "unmarked");
  t.after(() => rm(parent, { force: true, recursive: true }));

  const left = await prepareOwner(leftRoot);
  const right = await prepareOwner(rightRoot);
  assert.notEqual(left.stateOwnerId, right.stateOwnerId);
  await assert.rejects(
    prepareOwner(leftRoot, right.stateOwnerId),
    stateIoError,
  );
  await assert.rejects(
    prepareOwner(missingRoot, left.stateOwnerId),
    stateIoError,
  );
  await assert.rejects(lstat(missingRoot), (error) => error?.code === "ENOENT");
  await mkdir(unmarkedRoot, { mode: 0o700 });
  await assert.rejects(prepareOwner(unmarkedRoot), stateIoError);
  assert.deepEqual(await readdir(unmarkedRoot), []);
});

test("owned operations reject root or marker identity, content, and access-policy drift", async (t) => {
  for (const scenario of [
    "missing",
    "replaced",
    "mismatched",
    "mode",
    "hardlink",
    "root-mode",
    "parent-mode",
    "root-replaced",
  ]) {
    await t.test(scenario, async (subtest) => {
      const parent = await mkdtemp(
        join(await realpath(tmpdir()), "podman-writer-state-owner-tamper-test-"),
      );
      const root = join(parent, "state");
      const markerPath = join(root, ".state-owner-v1.json");
      subtest.after(() => rm(parent, { force: true, recursive: true }));
      const owner = await prepareOwner(root);
      const bundle = createPodmanWriterSupervisorStateBundle(exact({ owner }));
      const original = await readFile(markerPath);
      if (scenario === "missing") {
        await rm(markerPath);
      } else if (scenario === "replaced") {
        await rename(markerPath, `${markerPath}.displaced`);
        await writeFile(markerPath, original, { flag: "wx", mode: 0o600 });
      } else if (scenario === "mismatched") {
        const replacementDigit = owner.stateOwnerId.endsWith("f".repeat(64))
          ? "e"
          : "f";
        await writeFile(
          markerPath,
          `{"contractVersion":1,"stateOwnerId":"state-owner:${replacementDigit.repeat(64)}"}\n`,
        );
      } else if (scenario === "mode") {
        await chmod(markerPath, 0o400);
      } else if (scenario === "hardlink") {
        await link(markerPath, `${markerPath}.alias`);
      } else if (scenario === "root-mode") {
        await chmod(root, 0o750);
      } else if (scenario === "parent-mode") {
        await chmod(parent, 0o750);
      } else {
        await rename(root, `${root}.displaced`);
        await mkdir(root, { mode: 0o700 });
        await writeFile(markerPath, original, { flag: "wx", mode: 0o600 });
      }
      await assert.rejects(
        bundle.state.read(exact({ launchAttemptId: "launch-attempt-001" })),
        stateIoError,
      );
    });
  }
});

test("the durable owner marker outlives the in-memory capability", async (t) => {
  const parent = await mkdtemp(
    join(await realpath(tmpdir()), "podman-writer-state-owner-lifetime-test-"),
  );
  const root = join(parent, "state");
  t.after(() => rm(parent, { force: true, recursive: true }));
  let stateOwnerId;
  {
    const owner = await prepareOwner(root);
    stateOwnerId = owner.stateOwnerId;
  }
  if (typeof globalThis.gc === "function") globalThis.gc();
  assert.equal(typeof (await lstat(join(root, ".state-owner-v1.json"))).ino, "number");
  assert.equal((await prepareOwner(root)).stateOwnerId, stateOwnerId);
});

test("owner preparation survives post-import crypto and path intrinsic poisoning", { concurrency: false }, async (t) => {
  const parent = await mkdtemp(
    join(await realpath(tmpdir()), "podman-writer-state-owner-poison-test-"),
  );
  const root = join(parent, "state");
  t.after(() => rm(parent, { force: true, recursive: true }));
  const specifications = [
    [crypto, "randomBytes"],
    [path, "basename"],
    [path, "dirname"],
    [path, "isAbsolute"],
    [path, "resolve"],
  ];
  const descriptors = specifications.map(([target, key]) =>
    Object.getOwnPropertyDescriptor(target, key)
  );
  const poisonCalls = new Map(specifications.map(([, key]) => [key, 0]));
  try {
    for (let index = 0; index < specifications.length; index += 1) {
      const [target, key] = specifications[index];
      const descriptor = descriptors[index];
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
    assert.match(
      (await prepareOwner(root)).stateOwnerId,
      /^state-owner:[0-9a-f]{64}$/u,
    );
  } finally {
    for (let index = 0; index < specifications.length; index += 1) {
      Object.defineProperty(
        specifications[index][0],
        specifications[index][1],
        descriptors[index],
      );
    }
    syncBuiltinESMExports();
  }
  assert.deepEqual([...poisonCalls.values()], [0, 0, 0, 0, 0]);
});

test("creates a separate exact terminal collection capability without changing the state ABI", async (t) => {
  const parent = await mkdtemp(
    join(await realpath(tmpdir()), "podman-writer-state-bundle-test-"),
  );
  const root = join(parent, "state");
  t.after(() => rm(parent, { force: true, recursive: true }));
  const bundle = await stateBundle(root);

  assert.equal(isPodmanWriterSupervisorStateBundle(bundle), true);
  assert.equal(Object.getPrototypeOf(bundle), null);
  assert.equal(Object.isFrozen(bundle), true);
  assert.deepEqual(Reflect.ownKeys(bundle), [
    "state",
    "stateOwnerId",
    "terminalCollector",
  ]);
  assert.match(bundle.stateOwnerId, /^state-owner:[0-9a-f]{64}$/u);
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
    "stateOwnerId",
  ]);
  assert.equal(bundle.terminalCollector.stateOwnerId, bundle.stateOwnerId);
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
  assert.throws(
    () => createPodmanWriterSupervisorStateBundle(exact({ root })),
    stateInvalid,
  );
  assert.throws(
    () => createPodmanWriterSupervisorStateBundle(exact({
      owner: exact({
        contractVersion: PODMAN_WRITER_SUPERVISOR_STATE_OWNER_CONTRACT_VERSION,
        stateOwnerId: bundle.stateOwnerId,
      }),
    })),
    stateInvalid,
  );
});

test("collects an exact stopped chain in two durable phases and replays absence", async (t) => {
  const parent = await mkdtemp(
    join(await realpath(tmpdir()), "podman-writer-state-collect-test-"),
  );
  const root = join(parent, "state");
  t.after(() => rm(parent, { force: true, recursive: true }));
  const bundle = await stateBundle(root);
  const records = await createStoppedChain(bundle.state);
  const other = record("preparing", {
    launchAttemptId: "launch-attempt-other",
    requestSha256: "f".repeat(64),
  });
  await bundle.state.claim(exact({ record: other }));

  const receipt = await collect(bundle, records[4]);
  assert.equal(Object.getPrototypeOf(receipt), null);
  assert.equal(Object.isFrozen(receipt), true);
  assert.deepEqual(Reflect.ownKeys(receipt), [
    "contractVersion",
    "launchAttemptId",
    "stateOwnerId",
    "status",
    "terminalRecordSha256",
  ]);
  assert.equal(receipt.status, "collected");
  assert.equal(
    receipt.terminalRecordSha256,
    createHash("sha256")
      .update(
        "portable-codex-runtime:podman-writer-state-collection:v2\0",
        "utf8",
      )
      .update(bundle.stateOwnerId, "utf8")
      .update("\0", "utf8")
      .update(JSON.stringify(records[4]), "utf8")
      .digest("hex"),
  );
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
  assert.equal(remaining.length, 2);
  assert.equal(remaining.includes(".state-owner-v1.json"), true);
  assert.equal(
    remaining.some((entry) => /^[0-9a-f]{64}\.0\.json$/u.test(entry)),
    true,
  );

  const replay = await collect(bundle, records[4]);
  assert.deepEqual(replay, exact({ ...receipt, status: "absent" }));
});

test("collection preflights the full chain and recognized aliases before unlink", async (t) => {
  const parent = await mkdtemp(
    join(await realpath(tmpdir()), "podman-writer-state-alias-test-"),
  );
  const root = join(parent, "state");
  t.after(() => rm(parent, { force: true, recursive: true }));
  const bundle = await stateBundle(root);
  const records = await createStoppedChain(bundle.state);
  const entries = (await readdir(root))
    .filter((entry) => /^[0-9a-f]{64}\.[0-4]\.json$/u.test(entry))
    .sort();
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

  const receipt = await collect(bundle, records[4]);
  assert.equal(receipt.status, "collected");
  assert.deepEqual(await readdir(root), [".state-owner-v1.json"]);
});

test("collection rejects wrong authority and sidecar tamper before deleting anything", async (t) => {
  const parent = await mkdtemp(
    join(await realpath(tmpdir()), "podman-writer-state-preflight-test-"),
  );
  const root = join(parent, "state");
  t.after(() => rm(parent, { force: true, recursive: true }));
  const bundle = await stateBundle(root);
  const records = await createStoppedChain(bundle.state);
  const before = (await readdir(root)).sort();

  const wrongStateOwnerId = bundle.stateOwnerId === `state-owner:${"0".repeat(64)}`
    ? `state-owner:${"1".repeat(64)}`
    : `state-owner:${"0".repeat(64)}`;
  await assert.rejects(
    bundle.terminalCollector.collect(exact({
      stateOwnerId: wrongStateOwnerId,
      terminalRecord: records[4],
    })),
    stateInvalid,
  );
  await assert.rejects(
    collect(
      bundle,
      record("stopped", {
          requestSha256: "f".repeat(64),
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
    collect(bundle, records[4]),
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
  const bundle = await stateBundle(root);
  const records = await createStoppedChain(bundle.state);
  const revisionTwo = (await readdir(root)).find((entry) =>
    entry.endsWith(".2.json"),
  );
  assert.equal(typeof revisionTwo, "string");
  await writeFile(join(root, revisionTwo), "{\"malformed\":true}\n");
  const before = (await readdir(root)).sort();

  await assert.rejects(
    collect(bundle, records[4]),
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
      const bundle = await stateBundle(root);
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
        collect(bundle, records[4]),
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
  const writer = await stateBundle(root);
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
  const collector = await stateBundle(
    root,
    exact({
        async afterCollectionArtifactUnlink() {
          unlinks += 1;
          if (unlinks === 5) {
            await attackerHandle.writeFile(mutatedBytes);
            await attackerHandle.sync();
          }
        },
    }),
  );

  await assert.rejects(
    collect(collector, records[4]),
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
  const initial = await stateBundle(root);
  const records = await createStoppedChain(initial.state);
  const partial = await stateBundle(
    root,
    exact({
        afterCollectionFirstDirectorySync() {
          throw new Error("simulated first-phase crash");
        },
    }),
  );
  await assert.rejects(
    collect(partial, records[4]),
    collectionUncertain,
  );

  const retry = await stateBundle(
    root,
    exact({
        afterCollectionTerminalRevalidation() {
          throw new Error("simulated retry crash");
        },
    }),
  );
  await assert.rejects(
    collect(retry, records[4]),
    collectionUncertain,
  );
});

test("an absent retry fsyncs the state root before completing", async (t) => {
  const parent = await mkdtemp(
    join(await realpath(tmpdir()), "podman-writer-state-absent-sync-test-"),
  );
  const root = join(parent, "state");
  t.after(() => rm(parent, { force: true, recursive: true }));
  const initial = await stateBundle(root);
  const records = await createStoppedChain(initial.state);
  const interrupted = await stateBundle(
    root,
    exact({
        afterCollectionTerminalUnlink() {
          throw new Error("simulated crash before final directory sync");
        },
    }),
  );
  await assert.rejects(
    collect(interrupted, records[4]),
    collectionUncertain,
  );
  assert.deepEqual(await readdir(root), [".state-owner-v1.json"]);

  let finalSyncs = 0;
  const recovery = await stateBundle(
    root,
    exact({
        afterCollectionFinalDirectorySync() {
          finalSyncs += 1;
        },
    }),
  );
  const receipt = await collect(recovery, records[4]);
  assert.equal(receipt.status, "absent");
  assert.equal(finalSyncs, 1);
  assert.deepEqual(await readdir(root), [".state-owner-v1.json"]);
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
  const writer = await stateBundle(root);
  const records = await createStoppedChain(writer.state);
  const before = (await readdir(root)).sort();

  let replacements = 0;
  const collector = await stateBundle(
    root,
    exact({
        async afterPrivateParentHold() {
          await rename(parent, displacedParent);
          await mkdir(parent, { mode: 0o700 });
          replacements += 1;
        },
    }),
  );

  await assert.rejects(
    collect(collector, records[4]),
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
  const retained = await stateBundle(join(displacedParent, "state"));
  assert.deepEqual(
    await retained.state.read(
      exact({ launchAttemptId: records[4].launchAttemptId }),
    ),
    records[4],
  );
});

test("an owned collector never reports absence for a missing state root", async (t) => {
  const parent = await mkdtemp(
    join(await realpath(tmpdir()), "podman-writer-state-missing-root-sync-test-"),
  );
  const root = join(parent, "state");
  t.after(() => rm(parent, { force: true, recursive: true }));
  const terminalRecord = record("stopped");
  const owner = await prepareOwner(root);
  const bundle = createPodmanWriterSupervisorStateBundle(exact({ owner }));
  await rm(root, { recursive: true });

  await assert.rejects(
    collect(bundle, terminalRecord),
    stateIoError,
  );
  await assert.rejects(lstat(root), (error) => error?.code === "ENOENT");
  await assert.rejects(
    prepareOwner(root, owner.stateOwnerId),
    stateIoError,
  );
  await assert.rejects(lstat(root), (error) => error?.code === "ENOENT");
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
      const initial = await stateBundle(root);
      const records = await createStoppedChain(initial.state);
      let calls = 0;
      const crashing = await stateBundle(
        root,
        exact({
            [scenario.hook]() {
              calls += 1;
              if (calls === scenario.ordinal) {
                throw new Error("simulated collection crash");
              }
            },
        }),
      );
      await assert.rejects(
        collect(crashing, records[4]),
        collectionUncertain,
      );

      const recovery = await stateBundle(root);
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
      const receipt = await collect(recovery, records[4]);
      assert.equal(
        receipt.status === "collected" || receipt.status === "absent",
        true,
      );
      assert.deepEqual(await readdir(root), [".state-owner-v1.json"]);
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
  const writer = await stateBundle(root);
  const records = await createStoppedChain(writer.state);
  let observedTerminal;
  const terminalObserved = new Promise((resolve) => {
    observedTerminal = resolve;
  });
  let releaseTerminal;
  const terminalGate = new Promise((resolve) => {
    releaseTerminal = resolve;
  });
  const collecting = await stateBundle(
    root,
    exact({
      async afterCollectionTerminalRevalidation() {
        const churn = join(root, "benign-child-churn");
        await writeFile(churn, "benign\n", { flag: "wx", mode: 0o600 });
        await rm(churn);
        observedTerminal();
        await terminalGate;
      },
    }),
  );
  const pendingCollection = collect(collecting, records[4]);
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

test("collection fails closed when marker content changes during the operation", async (t) => {
  const parent = await mkdtemp(
    join(await realpath(tmpdir()), "podman-writer-state-marker-race-test-"),
  );
  const root = join(parent, "state");
  const markerPath = join(root, ".state-owner-v1.json");
  t.after(() => rm(parent, { force: true, recursive: true }));
  const writer = await stateBundle(root);
  const records = await createStoppedChain(writer.state);
  const replacementDigit = writer.stateOwnerId.endsWith("f".repeat(64))
    ? "e"
    : "f";
  const collector = await stateBundle(root, exact({
    async afterCollectionTerminalRevalidation() {
      await writeFile(
        markerPath,
        `{"contractVersion":1,"stateOwnerId":"state-owner:${replacementDigit.repeat(64)}"}\n`,
      );
    },
  }));

  await assert.rejects(collect(collector, records[4]), collectionUncertain);
});

test("collection revalidates benign file link churn without treating ctime as content", async (t) => {
  const parent = await mkdtemp(
    join(await realpath(tmpdir()), "podman-writer-state-file-churn-test-"),
  );
  const root = join(parent, "state");
  t.after(() => rm(parent, { force: true, recursive: true }));
  const writer = await stateBundle(root);
  const records = await createStoppedChain(writer.state);
  const revisionZero = (await readdir(root)).find((entry) =>
    entry.endsWith(".0.json"),
  );
  assert.equal(typeof revisionZero, "string");
  const revisionZeroPath = join(root, revisionZero);
  const pendingPath = `${revisionZeroPath}.pending`;
  await link(revisionZeroPath, pendingPath);

  let reads = 0;
  const collector = await stateBundle(
    root,
    exact({
      async afterCollectionFileFirstRead() {
        reads += 1;
        if (reads === 1) await rm(pendingPath);
      },
    }),
  );
  const receipt = await collect(collector, records[4]);
  assert.equal(receipt.status, "collected");
  assert.equal(reads >= 5, true);
  assert.deepEqual(await readdir(root), [".state-owner-v1.json"]);
});

test("collection rejects file content mutation during held-file revalidation", async (t) => {
  const parent = await mkdtemp(
    join(await realpath(tmpdir()), "podman-writer-state-file-content-test-"),
  );
  const root = join(parent, "state");
  t.after(() => rm(parent, { force: true, recursive: true }));
  const writer = await stateBundle(root);
  const records = await createStoppedChain(writer.state);
  const revisionZero = (await readdir(root)).find((entry) =>
    entry.endsWith(".0.json"),
  );
  assert.equal(typeof revisionZero, "string");
  const revisionZeroPath = join(root, revisionZero);
  const before = (await readdir(root)).sort();

  let reads = 0;
  const collector = await stateBundle(
    root,
    exact({
        async afterCollectionFileFirstRead() {
          reads += 1;
          if (reads === 1) {
            await writeFile(revisionZeroPath, "mutated\n");
          }
        },
    }),
  );
  await assert.rejects(
    collect(collector, records[4]),
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
  const writer = await stateBundle(root);
  const records = await createStoppedChain(writer.state);
  const left = await stateBundle(root);
  const right = await stateBundle(root);
  const results = await Promise.allSettled([
    collect(left, records[4]),
    collect(right, records[4]),
  ]);
  assert.equal(results.some((result) => result.status === "fulfilled"), true);
  const recovery = await stateBundle(root);
  await collect(recovery, records[4]);
  assert.deepEqual(await readdir(root), [".state-owner-v1.json"]);
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
  const writer = await stateBundle(root);
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
  const first = await stateBundle(root, exact({
      async afterCollectionArtifactUnlink() {
        unlinks += 1;
        if (unlinks === 6) {
          terminalPendingRemoved();
          await firstCollectorGate;
        }
      },
  }));
  const firstResult = collect(first, records[4]);
  await terminalPendingRemoval;

  const second = await stateBundle(root);
  assert.equal(
    (await collect(second, records[4])).status,
    "collected",
  );
  releaseFirstCollector();
  assert.equal((await firstResult).status, "collected");
  assert.deepEqual(await readdir(root), [".state-owner-v1.json"]);

  const replay = await stateBundle(root);
  assert.equal(
    (await collect(replay, records[4])).status,
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
