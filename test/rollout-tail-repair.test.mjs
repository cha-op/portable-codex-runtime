import assert from "node:assert/strict";
import { execFile } from "node:child_process";
import {
  chmod,
  link,
  mkdir,
  mkdtemp,
  readFile,
  readdir,
  rename,
  rm,
  symlink,
  truncate,
  writeFile,
} from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { promisify } from "node:util";

import {
  ROLLOUT_TAIL_REPAIR_COMPATIBILITY,
  RolloutTailRepairError,
  __testing,
  repairStoppedRolloutTails,
} from "../src/rollout-tail-repair.mjs";

const execFileAsync = promisify(execFile);
const ROOT_ID = "019f2600-0000-7000-8000-000000000001";
const SUBAGENT_ID = "019f2600-0000-7000-8000-000000000002";
const OTHER_ID = "019f2600-0000-7000-8000-000000000099";
const BINARY_SHA = "ab".repeat(32);
const RUNTIME_IDENTITY = Object.freeze({
  codexBinarySha256: BINARY_SHA,
  codexVersion: "codex-cli 0.144.1",
  sourceAnalysisCommit: "db887d03e1f907467e33271572dffb73bceecd6b",
});

function sessionMeta(id = ROOT_ID, sessionId = ROOT_ID) {
  return {
    timestamp: "2026-07-15T00:00:00.000Z",
    type: "session_meta",
    payload: {
      cli_version: "0.144.1",
      cwd: "/workspace",
      id,
      originator: "test",
      session_id: sessionId,
      timestamp: "2026-07-15T00:00:00.000Z",
    },
  };
}

function event(sequence = 1) {
  return { type: "event_msg", payload: { sequence } };
}

function lines(...values) {
  return Buffer.from(`${values.map((value) => JSON.stringify(value)).join("\n")}\n`);
}

async function fixture(t) {
  const root = await mkdtemp(join(tmpdir(), "rollout-tail-repair-"));
  const codexHome = join(root, "codex-home");
  const sessions = join(codexHome, "sessions");
  const day = join(sessions, "2026", "07", "15");
  await mkdir(day, { mode: 0o700, recursive: true });
  for (const path of [codexHome, sessions, join(sessions, "2026"), join(sessions, "2026", "07"), day]) {
    await chmod(path, 0o700);
  }
  t.after(() => rm(root, { force: true, recursive: true }));
  return { codexHome, day, root, sessions };
}

async function putRollout(directory, name, bytes, mode = 0o600) {
  const path = join(directory, name);
  await writeFile(path, bytes, { mode });
  await chmod(path, mode);
  return path;
}

function request(codexHome, overrides = {}) {
  return {
    codexHome,
    rootSessionId: ROOT_ID,
    runtimeIdentity: RUNTIME_IDENTITY,
    ...overrides,
  };
}

function assertDeepFrozen(value, visited = new Set()) {
  if (value === null || typeof value !== "object" || visited.has(value)) return;
  visited.add(value);
  assert.equal(Object.isFrozen(value), true);
  for (const key of Reflect.ownKeys(value)) assertDeepFrozen(value[key], visited);
}

function assertRepairError(error, code, forbidden = []) {
  assert(error instanceof RolloutTailRepairError);
  assert.equal(error.code, code);
  assert.equal(error.message.length > 0, true);
  assert.equal(error.retryable, false);
  assert.equal(Object.isFrozen(error), true);
  assert.deepEqual(Reflect.ownKeys(error).sort(), [
    "code",
    "message",
    "name",
    "retryable",
    "stack",
  ]);
  const surface = Reflect.ownKeys(error).map((key) => String(error[key])).join("\n");
  for (const secret of forbidden) assert.equal(surface.includes(secret), false);
  return true;
}

test("publishes the exact pinned compatibility surface", () => {
  assert.deepEqual(ROLLOUT_TAIL_REPAIR_COMPATIBILITY, {
    codexVersion: "codex-cli 0.144.1",
    rolloutCliVersion: "0.144.1",
    sourceAnalysisCommit: "db887d03e1f907467e33271572dffb73bceecd6b",
  });
  assertDeepFrozen(ROLLOUT_TAIL_REPAIR_COMPATIBILITY);
});

test("leaves a valid LF-terminated rollout unchanged and is idempotent", async (t) => {
  const { codexHome, day } = await fixture(t);
  const contentSentinel = "rollout-content-must-not-appear-in-proof";
  const original = lines(sessionMeta(), { ...event(), contentSentinel });
  const path = await putRollout(day, "not-derived-from-the-thread-id.jsonl", original);

  const first = await repairStoppedRolloutTails(request(codexHome));
  const second = await repairStoppedRolloutTails(request(codexHome));

  assert.deepEqual(await readFile(path), original);
  assert.equal(first.files.length, 1);
  assert.equal(first.files[0].action, "unchanged");
  assert.equal(first.files[0].removedBytes, 0);
  assert.deepEqual(first.files[0].before, first.files[0].after);
  assert.deepEqual(second, first);
  assert.equal(first.compatibility.codexBinarySha256, BINARY_SHA);
  assert.equal(JSON.stringify(first).includes(contentSentinel), false);
  assertDeepFrozen(first);
});

test("appends LF to a syntactically complete final record", async (t) => {
  const { codexHome, day } = await fixture(t);
  const original = Buffer.from(
    `${JSON.stringify(sessionMeta())}\n${JSON.stringify(event())}`,
  );
  const path = await putRollout(day, "complete-tail.jsonl", original);

  const proof = await repairStoppedRolloutTails(request(codexHome));

  assert.deepEqual(await readFile(path), Buffer.concat([original, Buffer.from("\n")]));
  assert.equal(proof.files[0].action, "append_lf");
  assert.equal(proof.files[0].after.size, proof.files[0].before.size + 1);
  assert.equal(proof.files[0].removedBytes, 0);
  assert.equal((await readdir(day)).some((name) => name.includes("rollout-tail-repair")), false);
  const replay = await repairStoppedRolloutTails(request(codexHome));
  assert.equal(replay.files[0].action, "unchanged");
});

test("truncates only an invalid unterminated final suffix", async (t) => {
  const { codexHome, day } = await fixture(t);
  const prefix = lines(sessionMeta(), event(1));
  const torn = Buffer.from('{"type":"event_msg","payload":');
  const path = await putRollout(day, "torn-tail.jsonl", Buffer.concat([prefix, torn]));

  const proof = await repairStoppedRolloutTails(request(codexHome));

  assert.deepEqual(await readFile(path), prefix);
  assert.equal(proof.files[0].action, "truncate_partial_tail");
  assert.equal(proof.files[0].removedBytes, torn.length);
  assert.equal(proof.files[0].after.size, prefix.length);
  assert.equal(proof.files[0].before.size, prefix.length + torn.length);
  const replay = await repairStoppedRolloutTails(request(codexHome));
  assert.equal(replay.files[0].action, "unchanged");
});

test("discovers one root and same-session subagents without trusting filenames", async (t) => {
  const { codexHome, day } = await fixture(t);
  const nested = join(day, "agents");
  await mkdir(nested, { mode: 0o700 });
  await putRollout(day, "alpha.jsonl", lines(sessionMeta(), event(1)));
  await putRollout(
    nested,
    "beta.jsonl",
    lines(sessionMeta(SUBAGENT_ID, ROOT_ID), event(2)),
  );

  const proof = await repairStoppedRolloutTails(request(codexHome));

  assert.deepEqual(
    proof.files.map(({ relativePath }) => relativePath),
    [
      "2026/07/15/agents/beta.jsonl",
      "2026/07/15/alpha.jsonl",
    ],
  );
});

test("repairs every validated member of one session tree and then converges", async (t) => {
  const { codexHome, day } = await fixture(t);
  const rootPrefix = lines(sessionMeta());
  const subagentComplete = Buffer.from(
    JSON.stringify(sessionMeta(SUBAGENT_ID, ROOT_ID)),
  );
  await putRollout(
    day,
    "root-arbitrary.jsonl",
    Buffer.concat([rootPrefix, Buffer.from("{")]),
  );
  await putRollout(day, "subagent-arbitrary.jsonl", subagentComplete);

  const proof = await repairStoppedRolloutTails(request(codexHome));
  assert.deepEqual(
    proof.files.map(({ action }) => action).sort(),
    ["append_lf", "truncate_partial_tail"],
  );
  const replay = await repairStoppedRolloutTails(request(codexHome));
  assert.deepEqual(replay.files.map(({ action }) => action), [
    "unchanged",
    "unchanged",
  ]);
});

test("rejects a foreign session and duplicate thread identity without mutation", async (t) => {
  await t.test("foreign session", async (t) => {
    const { codexHome, day } = await fixture(t);
    const root = await putRollout(day, "root.jsonl", lines(sessionMeta()));
    const foreign = await putRollout(
      day,
      "foreign.jsonl",
      lines(sessionMeta(OTHER_ID, OTHER_ID)),
    );
    const before = await readFile(root);
    await assert.rejects(
      repairStoppedRolloutTails(request(codexHome)),
      (error) => assertRepairError(error, "rollout_set_invalid"),
    );
    assert.deepEqual(await readFile(root), before);
    assert.equal((await readFile(foreign)).at(-1), 0x0a);
  });

  await t.test("duplicate thread identity", async (t) => {
    const { codexHome, day } = await fixture(t);
    await putRollout(day, "root.jsonl", lines(sessionMeta()));
    await putRollout(day, "duplicate.jsonl", lines(sessionMeta()));
    await assert.rejects(
      repairStoppedRolloutTails(request(codexHome)),
      (error) => assertRepairError(error, "rollout_set_invalid"),
    );
  });
});

test("rejects missing or malformed SessionMeta and non-tail corruption", async (t) => {
  const cases = [
    ["missing-meta.jsonl", lines(event())],
    ["missing-session-id.jsonl", lines({ ...sessionMeta(), payload: { id: ROOT_ID } })],
    [
      "missing-cli-version.jsonl",
      lines({
        ...sessionMeta(),
        payload: { ...sessionMeta().payload, cli_version: undefined },
      }),
    ],
    [
      "mismatched-cli-version.jsonl",
      lines({
        ...sessionMeta(),
        payload: { ...sessionMeta().payload, cli_version: "0.143.0" },
      }),
    ],
    ["invalid-first-tail.jsonl", Buffer.from('{"type":"session_meta"')],
    [
      "invalid-middle.jsonl",
      Buffer.from(`${JSON.stringify(sessionMeta())}\nnot-json\n${JSON.stringify(event())}`),
    ],
    [
      "empty-middle.jsonl",
      Buffer.from(`${JSON.stringify(sessionMeta())}\n\n${JSON.stringify(event())}\n`),
    ],
  ];
  for (const [name, bytes] of cases) {
    await t.test(name, async (t) => {
      const { codexHome, day } = await fixture(t);
      const path = await putRollout(day, name, bytes);
      const before = await readFile(path);
      await assert.rejects(
        repairStoppedRolloutTails(request(codexHome)),
        (error) => assertRepairError(error, "rollout_content_invalid"),
      );
      assert.deepEqual(await readFile(path), before);
    });
  }
});

test("truncates invalid UTF-8 only when confined to the unterminated final suffix", async (t) => {
  const { codexHome, day } = await fixture(t);
  const prefix = lines(sessionMeta());
  const path = await putRollout(
    day,
    "invalid-utf8.jsonl",
    Buffer.concat([prefix, Buffer.from([0xc3])]),
  );
  const proof = await repairStoppedRolloutTails(request(codexHome));
  assert.deepEqual(await readFile(path), prefix);
  assert.equal(proof.files[0].action, "truncate_partial_tail");
  assert.equal(proof.files[0].removedBytes, 1);
});

test("rejects invalid UTF-8 in a newline-terminated record", async (t) => {
  const { codexHome, day } = await fixture(t);
  const bytes = Buffer.concat([lines(sessionMeta()), Buffer.from([0xc3, 0x0a])]);
  const path = await putRollout(day, "invalid-utf8-line.jsonl", bytes);
  await assert.rejects(
    repairStoppedRolloutTails(request(codexHome)),
    (error) => assertRepairError(error, "rollout_content_invalid"),
  );
  assert.deepEqual(await readFile(path), bytes);
});

test("rejects compressed, unknown, symlink, hard-linked, and non-regular objects", async (t) => {
  await t.test("compressed rollout", async (t) => {
    const { codexHome, day } = await fixture(t);
    await putRollout(day, "root.jsonl", lines(sessionMeta()));
    await putRollout(day, "root.jsonl.zst", Buffer.from("compressed"));
    await assert.rejects(
      repairStoppedRolloutTails(request(codexHome)),
      (error) => assertRepairError(error, "unsupported_rollout_object"),
    );
  });

  await t.test("unknown regular object", async (t) => {
    const { codexHome, day } = await fixture(t);
    await putRollout(day, "root.jsonl", lines(sessionMeta()));
    await putRollout(day, "notes.txt", Buffer.from("private"));
    await assert.rejects(
      repairStoppedRolloutTails(request(codexHome)),
      (error) => assertRepairError(error, "unsupported_rollout_object", ["private"]),
    );
  });

  await t.test("symlink", async (t) => {
    const { codexHome, day, root } = await fixture(t);
    const outside = join(root, "outside.jsonl");
    await writeFile(outside, lines(sessionMeta()), { mode: 0o600 });
    await symlink(outside, join(day, "root.jsonl"));
    await assert.rejects(
      repairStoppedRolloutTails(request(codexHome)),
      (error) => assertRepairError(error, "unsafe_filesystem", [outside]),
    );
  });

  await t.test("symlinked Codex home", async (t) => {
    const { codexHome, day, root } = await fixture(t);
    await putRollout(day, "root.jsonl", lines(sessionMeta()));
    const alias = join(root, "codex-home-alias");
    await symlink(codexHome, alias);
    await assert.rejects(
      repairStoppedRolloutTails(request(alias)),
      (error) => assertRepairError(error, "unsafe_filesystem", [codexHome]),
    );
  });

  await t.test("symlinked sessions descendant", async (t) => {
    const { codexHome, day, root } = await fixture(t);
    const outside = join(root, "outside-directory");
    await mkdir(outside, { mode: 0o700 });
    await putRollout(day, "root.jsonl", lines(sessionMeta()));
    await symlink(outside, join(day, "linked-directory"));
    await assert.rejects(
      repairStoppedRolloutTails(request(codexHome)),
      (error) => assertRepairError(error, "unsafe_filesystem", [outside]),
    );
  });

  await t.test("hard link", async (t) => {
    const { codexHome, day, root } = await fixture(t);
    const path = await putRollout(day, "root.jsonl", lines(sessionMeta()));
    await link(path, join(root, "alias.jsonl"));
    await assert.rejects(
      repairStoppedRolloutTails(request(codexHome)),
      (error) => assertRepairError(error, "unsafe_filesystem"),
    );
  });

  await t.test("FIFO", async (t) => {
    const { codexHome, day } = await fixture(t);
    await putRollout(day, "root.jsonl", lines(sessionMeta()));
    await execFileAsync("/usr/bin/mkfifo", [join(day, "pipe.jsonl")]);
    await assert.rejects(
      repairStoppedRolloutTails(request(codexHome)),
      (error) => assertRepairError(error, "unsupported_rollout_object"),
    );
  });
});

test("rejects unsafe file and directory modes", async (t) => {
  await t.test("readable-by-group rollout", async (t) => {
    const { codexHome, day } = await fixture(t);
    await putRollout(day, "root.jsonl", lines(sessionMeta()), 0o640);
    await assert.rejects(
      repairStoppedRolloutTails(request(codexHome)),
      (error) => assertRepairError(error, "unsafe_filesystem"),
    );
  });

  await t.test("read-only owner rollout", async (t) => {
    const { codexHome, day } = await fixture(t);
    await putRollout(day, "root.jsonl", lines(sessionMeta()), 0o400);
    await assert.rejects(
      repairStoppedRolloutTails(request(codexHome)),
      (error) => assertRepairError(error, "unsafe_filesystem"),
    );
  });

  await t.test("owner-executable rollout", async (t) => {
    const { codexHome, day } = await fixture(t);
    await putRollout(day, "root.jsonl", lines(sessionMeta()), 0o700);
    await assert.rejects(
      repairStoppedRolloutTails(request(codexHome)),
      (error) => assertRepairError(error, "unsafe_filesystem"),
    );
  });

  await t.test("group-writable sessions directory", async (t) => {
    const { codexHome, day, sessions } = await fixture(t);
    await putRollout(day, "root.jsonl", lines(sessionMeta()));
    await chmod(sessions, 0o720);
    await assert.rejects(
      repairStoppedRolloutTails(request(codexHome)),
      (error) => assertRepairError(error, "unsafe_filesystem"),
    );
  });

  await t.test("world-readable nested directory", async (t) => {
    const { codexHome, day } = await fixture(t);
    await putRollout(day, "root.jsonl", lines(sessionMeta()));
    await chmod(day, 0o755);
    await assert.rejects(
      repairStoppedRolloutTails(request(codexHome)),
      (error) => assertRepairError(error, "unsafe_filesystem"),
    );
  });
});

test("enforces the per-file size bound before reading contents", async (t) => {
  const { codexHome, day } = await fixture(t);
  const path = await putRollout(day, "root.jsonl", lines(sessionMeta()));
  await truncate(path, 64 * 1024 * 1024 + 1);
  await assert.rejects(
    repairStoppedRolloutTails(request(codexHome)),
    (error) => assertRepairError(error, "rollout_content_invalid"),
  );
});

test("requires the exact trusted runtime binding shape and pinned version/source", async (t) => {
  const { codexHome, day } = await fixture(t);
  await putRollout(day, "root.jsonl", lines(sessionMeta()));
  for (const runtimeIdentity of [
    { ...RUNTIME_IDENTITY, codexVersion: "0.144.2" },
    { ...RUNTIME_IDENTITY, sourceAnalysisCommit: "0".repeat(40) },
    { ...RUNTIME_IDENTITY, codexBinarySha256: "AB".repeat(32) },
    { ...RUNTIME_IDENTITY, extra: true },
  ]) {
    await assert.rejects(
      repairStoppedRolloutTails(request(codexHome, { runtimeIdentity })),
      (error) =>
        assertRepairError(
          error,
          objectHasExtra(runtimeIdentity) ? "invalid_request" : "runtime_identity_mismatch",
        ),
    );
  }
});

function objectHasExtra(value) {
  return Object.hasOwn(value, "extra");
}

test("detects a before-rename identity race without publishing repair", async (t) => {
  const { codexHome, day } = await fixture(t);
  const prefix = lines(sessionMeta());
  const original = Buffer.concat([prefix, Buffer.from("{")]);
  const path = await putRollout(day, "root.jsonl", original);
  const displaced = join(day, "displaced.jsonl");
  const repair = __testing.createRepair({
    afterRename: async () => {},
    beforeRename: async () => {
      await rename(path, displaced);
      await putRollout(day, "root.jsonl", original);
    },
    syncDirectory: async (handle) => handle.sync(),
  });

  await assert.rejects(
    repair(request(codexHome)),
    (error) => assertRepairError(error, "repair_failed", [path]),
  );
  assert.deepEqual(await readFile(path), original);
  assert.deepEqual(await readFile(displaced), original);
});

test("reports uncertain outcome when parent-directory sync fails after rename", async (t) => {
  const { codexHome, day } = await fixture(t);
  const original = Buffer.from(JSON.stringify(sessionMeta()));
  const path = await putRollout(day, "root.jsonl", original);
  const secret = "post-rename-secret";
  const repair = __testing.createRepair({
    afterRename: async () => {},
    beforeRename: async () => {},
    syncDirectory: async () => {
      throw new Error(secret);
    },
  });

  await assert.rejects(
    repair(request(codexHome)),
    (error) => assertRepairError(error, "repair_outcome_uncertain", [secret, path]),
  );
  assert.deepEqual(await readFile(path), Buffer.concat([original, Buffer.from("\n")]));
});
