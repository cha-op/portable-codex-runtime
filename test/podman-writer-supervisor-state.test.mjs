import assert from "node:assert/strict";
import crypto, { createHash } from "node:crypto";
import {
  chmod,
  link,
  lstat,
  mkdir,
  mkdtemp,
  readFile,
  readdir,
  realpath,
  rm,
  symlink,
  writeFile,
} from "node:fs/promises";
import { syncBuiltinESMExports } from "node:module";
import { tmpdir } from "node:os";
import path, { join } from "node:path";
import test from "node:test";

import {
  PODMAN_WRITER_SUPERVISOR_STATE_CONTRACT_VERSION,
  PodmanWriterSupervisorStateError,
  assertPodmanWriterSupervisorStateRecord,
  createPodmanWriterSupervisorState,
  isPodmanWriterSupervisorState,
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

function transition(state, before, after) {
  return state.transition(
    exact({
      expectedRevision: before.revision,
      expectedStatus: before.status,
      record: after,
    }),
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
    assert.deepEqual([...poisonCalls.values()], [0, 0, 0, 0]);
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
