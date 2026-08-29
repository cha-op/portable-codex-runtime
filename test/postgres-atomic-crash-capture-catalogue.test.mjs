import assert from "node:assert/strict";
import { Buffer } from "node:buffer";
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import test from "node:test";

import {
  PostgresSerializableStore,
  PostgresSerializableStoreError,
} from "../src/postgres-serializable-store.mjs";
import {
  assertAtomicCrashCaptureRequest,
} from "../src/session-storage-contracts.mjs";

const NOW = "2026-08-29T00:00:00.000Z";
const SESSION_ID = "019f8d00-0000-7000-8000-000000000001";
const CODEX_SESSION_ID = "019f8d00-0000-7000-8000-000000000002";
const CODEX_THREAD_ID = CODEX_SESSION_ID;
const IMAGE_DIGEST = `sha256:${"a".repeat(64)}`;

const harnesses = new WeakMap();
const runSerializableDescriptor = Object.getOwnPropertyDescriptor(
  PostgresSerializableStore.prototype,
  "runSerializable",
);

function replaceRows(target, source) {
  target.clear();
  for (const [key, value] of source) target.set(key, structuredClone(value));
}

function fakeRunSerializable(callback) {
  const harness = harnesses.get(this);
  assert.notEqual(harness, undefined);
  const execute = async () => {
    const script = harness.scripts.shift() ?? {};
    const state = new Map(structuredClone([...harness.rows]));
    const transaction = Object.freeze({
      now: NOW,
      query(text, values) {
        return Promise.resolve(harness.query({ script, state, text, values }));
      },
    });
    let value;
    try {
      value = await callback(transaction);
    } catch (error) {
      harness.transactions.push("rolled-back");
      throw error;
    }
    const outcome = script.commitOutcome ?? "committed";
    if (outcome === "committed" || outcome === "ack-loss") {
      replaceRows(harness.rows, state);
    }
    harness.transactions.push(outcome);
    if (outcome === "ack-loss" || outcome === "uncertain-missing") {
      throw new PostgresSerializableStoreError(
        "transaction_commit_outcome_uncertain",
        "uncertain",
      );
    }
    if (outcome === "not-committed") {
      throw new PostgresSerializableStoreError(
        "transaction_commit_outcome_uncertain",
        "not-committed",
      );
    }
    assert.equal(outcome, "committed");
    return value;
  };
  const promise = harness.tail.then(execute, execute);
  harness.tail = promise.catch(() => undefined);
  return promise;
}

Object.defineProperty(PostgresSerializableStore.prototype, "runSerializable", {
  ...runSerializableDescriptor,
  value: fakeRunSerializable,
});
const catalogueModule = await import(
  "../src/postgres-atomic-crash-capture-catalogue.mjs?unit-test"
);
Object.defineProperty(
  PostgresSerializableStore.prototype,
  "runSerializable",
  runSerializableDescriptor,
);

const {
  POSTGRES_ATOMIC_CRASH_CAPTURE_CATALOGUE_CONTRACT_VERSION,
  PostgresAtomicCrashCaptureCatalogueError,
  createPostgresAtomicCrashCaptureCatalogue,
  isPostgresAtomicCrashCaptureCatalogue,
} = catalogueModule;

function captureRequest({
  artifactId = "artifact-atomic-001",
  captureAttemptId = "capture-attempt-atomic-001",
  checkpointId = "checkpoint-atomic-001",
  operationId = "operation-atomic-001",
  rootPath = "/var/lib/portable-codex/session-atomic-001",
} = {}) {
  return {
    captureAttemptId,
    checkpoint: {
      artifactId,
      backendId: "lvm-atomic-test",
      checkpointClass: "crash-prefix",
      checkpointId,
      codexSessionId: CODEX_SESSION_ID,
      codexThreadId: CODEX_THREAD_ID,
      contractVersion: 1,
      createdAt: NOW,
      imageDigest: IMAGE_DIGEST,
      sessionId: SESSION_ID,
      sourceFencingEpoch: "18446744073709551615",
      storageId: "storage-atomic-001",
    },
    contractVersion: 1,
    mutationRequest: {
      backendId: "lvm-atomic-test",
      contractVersion: 1,
      fencingEpoch: "18446744073709551615",
      holderId: "holder-atomic-001",
      leaseId: "lease-atomic-001",
      operation: "checkpoint",
      operationId,
      sessionId: SESSION_ID,
      storageId: "storage-atomic-001",
      target: {
        artifactId,
        checkpointId,
        kind: "checkpoint",
      },
    },
    sourceAttachment: {
      attachmentId: "attachment-atomic-001",
      backendId: "lvm-atomic-test",
      contractVersion: 1,
      fencingEpoch: "18446744073709551615",
      holderId: "holder-atomic-001",
      kind: "directory",
      leaseId: "lease-atomic-001",
      mode: "read-write",
      operationId: "operation-attach-atomic-001",
      proofId: "proof-attach-atomic-001",
      rootPath,
      sessionId: SESSION_ID,
      storageId: "storage-atomic-001",
    },
    storageRef: {
      backendId: "lvm-atomic-test",
      contractVersion: 1,
      sessionId: SESSION_ID,
      storageId: "storage-atomic-001",
    },
  };
}

function captureResult(request, { proofId = "proof-atomic-001" } = {}) {
  return {
    artifact: {
      byteLength: "9007199254740993",
      contentSha256: "f".repeat(64),
      objectId: "lvm-snapshot-object-001",
      objectIdentityScheme: "lvm-lv-uuid-v1",
      readOnly: true,
    },
    artifactId: request.checkpoint.artifactId,
    backendId: request.storageRef.backendId,
    captureAttemptId: request.captureAttemptId,
    checkpointId: request.checkpoint.checkpointId,
    contractVersion: 1,
    operationId: request.mutationRequest.operationId,
    proofId,
    sessionId: request.storageRef.sessionId,
    sourceFencingEpoch: request.checkpoint.sourceFencingEpoch,
    status: "committed",
    storageId: request.storageRef.storageId,
  };
}

function providerBinding(overrides = {}) {
  return {
    bindingKind: "lvm-classic-snapshot-v1",
    contractVersion: 1,
    originLvUuid: "origin-lv-uuid-001",
    snapshotName: "codex-atomic-001",
    snapshotSizeBytes: "1073741824",
    snapshotTag: "portable-codex.atomic-capture-001",
    ...overrides,
  };
}

function canonicalJson(value) {
  if (Array.isArray(value)) return `[${value.map(canonicalJson).join(",")}]`;
  if (value !== null && typeof value === "object") {
    return `{${Object.keys(value)
      .sort()
      .map((key) => `${JSON.stringify(key)}:${canonicalJson(value[key])}`)
      .join(",")}}`;
  }
  return JSON.stringify(value);
}

function canonicalSha256(value) {
  return createHash("sha256").update(canonicalJson(value), "utf8").digest("hex");
}

function assertCanonicalEqual(actual, expected) {
  assert.equal(canonicalJson(actual), canonicalJson(expected));
}

function createHarness() {
  const harness = {
    rows: new Map(),
    scripts: [],
    tail: Promise.resolve(),
    trace: [],
    transactions: [],
    query({ script, state, text, values }) {
      harness.trace.push({ text, values: structuredClone(values) });
      if (text.startsWith("INSERT INTO session_authority.atomic_crash_captures")) {
        const collision = [...state.values()].some(
          (row) =>
            row.capture_attempt_id === values[0] ||
            row.operation_id === values[1] ||
            row.checkpoint_id === values[2] ||
            row.artifact_id === values[3],
        );
        if (collision) return { rows: [] };
        state.set(values[0], {
          artifact_id: values[3],
          backend_id: values[5],
          capture_attempt_id: values[0],
          checkpoint_id: values[2],
          claimed_at: new Date(NOW),
          committed_at: null,
          contract_version: values[4],
          operation_id: values[1],
          provider_binding: JSON.parse(values[11]),
          provider_binding_json: values[11],
          provider_binding_sha256: values[12],
          request_json: JSON.parse(values[9]),
          request_sha256: values[10],
          result_json: null,
          result_sha256: null,
          session_id: values[6],
          source_fencing_epoch: values[8],
          state: "starting",
          storage_id: values[7],
          uncertain_at: null,
        });
        return { rows: [{ capture_attempt_id: values[0] }] };
      }
      if (text.startsWith("SELECT ")) {
        let rows = [...state.values()].filter(
          (row) =>
            row.capture_attempt_id === values[0] ||
            row.operation_id === values[1] ||
            row.checkpoint_id === values[2] ||
            row.artifact_id === values[3],
        );
        rows = structuredClone(rows);
        if (typeof script.rowMutator === "function") {
          rows = rows.map((row) => script.rowMutator(row));
        }
        return { rows };
      }
      if (text.includes("SET state = 'uncertain'")) {
        const row = state.get(values[0]);
        if (
          row === undefined ||
          row.operation_id !== values[1] ||
          row.checkpoint_id !== values[2] ||
          row.artifact_id !== values[3] ||
          row.request_sha256 !== values[4] ||
          row.provider_binding_sha256 !== values[5] ||
          row.state !== "starting"
        ) {
          return { rows: [] };
        }
        row.state = "uncertain";
        row.uncertain_at = new Date(NOW);
        return { rows: [structuredClone(row)] };
      }
      if (text.includes("SET state = 'committed'")) {
        const row = state.get(values[0]);
        if (
          row === undefined ||
          row.operation_id !== values[1] ||
          row.checkpoint_id !== values[2] ||
          row.artifact_id !== values[3] ||
          row.request_sha256 !== values[4] ||
          row.provider_binding_sha256 !== values[5] ||
          !["starting", "uncertain"].includes(row.state)
        ) {
          return { rows: [] };
        }
        row.state = "committed";
        row.result_json = JSON.parse(values[6]);
        row.result_sha256 = values[7];
        row.committed_at = new Date(NOW);
        return { rows: [structuredClone(row)] };
      }
      assert.fail(`unexpected catalogue query: ${text}`);
    },
  };
  return harness;
}

function createFixture() {
  const harness = createHarness();
  const store = new PostgresSerializableStore({
    dedicatedPool: { connect() {} },
  });
  harnesses.set(store, harness);
  return {
    catalogue: createPostgresAtomicCrashCaptureCatalogue({ store }),
    harness,
    store,
  };
}

function catalogueError(code) {
  return (error) => {
    assert.ok(error instanceof PostgresAtomicCrashCaptureCatalogueError);
    assert.equal(error.code, code);
    assert.equal(error.retryable, false);
    assert.equal(Object.isFrozen(error), true);
    return true;
  };
}

async function claim(fixture, request = captureRequest(), binding = providerBinding()) {
  return fixture.catalogue.claimStarting({
    providerBinding: binding,
    request,
  });
}

test("catalogue surface is branded and deeply frozen", () => {
  const fixture = createFixture();
  assert.equal(
    POSTGRES_ATOMIC_CRASH_CAPTURE_CATALOGUE_CONTRACT_VERSION,
    1,
  );
  assert.equal(fixture.catalogue.contractVersion, 1);
  assert.equal(isPostgresAtomicCrashCaptureCatalogue(fixture.catalogue), true);
  assert.equal(isPostgresAtomicCrashCaptureCatalogue({}), false);
  assert.equal(Object.isFrozen(fixture.catalogue), true);
  assert.equal(Object.getPrototypeOf(fixture.catalogue), null);
  for (const method of [
    "claimStarting",
    "markUncertain",
    "commitResult",
    "readCommitted",
  ]) {
    assert.equal(Object.isFrozen(fixture.catalogue[method]), true);
  }
});

test("catalogue fixture is a contract-valid atomic capture request", () => {
  assert.doesNotThrow(() => assertAtomicCrashCaptureRequest(captureRequest()));
});

test("max-length non-UUID identities dispatch once and commit exact frozen data", async () => {
  const fixture = createFixture();
  const request = captureRequest({
    artifactId: `a${"-".repeat(127)}`,
    captureAttemptId: `c${".".repeat(127)}`,
    checkpointId: `k${"_".repeat(127)}`,
    operationId: `o${":".repeat(127)}`,
  });
  const first = await claim(fixture, request);
  assert.equal(first.outcome, "dispatch");
  assert.equal(Object.isFrozen(first), true);
  assert.equal(Object.isFrozen(first.dispatchClaim), true);
  assert.equal(Object.getPrototypeOf(first.dispatchClaim), null);

  const committed = await fixture.catalogue.commitResult({
    dispatchClaim: first.dispatchClaim,
    result: captureResult(request),
  });
  assert.equal(committed.outcome, "committed");
  assertCanonicalEqual(committed.providerBinding, providerBinding());
  assertCanonicalEqual(committed.result, captureResult(request));
  assert.equal(Object.isFrozen(committed), true);
  assert.equal(Object.isFrozen(committed.providerBinding), true);
  assert.equal(Object.isFrozen(committed.result.artifact), true);

  const replay = await claim(fixture, request);
  assert.deepEqual(replay, committed);
  assert.equal(Object.hasOwn(replay, "dispatchClaim"), false);
  const restarted = createPostgresAtomicCrashCaptureCatalogue({
    store: fixture.store,
  });
  assert.deepEqual(await restarted.readCommitted({ request }), committed);
});

test("concurrent claims yield one dispatch grant and one unknown", async () => {
  const fixture = createFixture();
  const outcomes = await Promise.all([claim(fixture), claim(fixture)]);
  assert.deepEqual(
    outcomes.map(({ outcome }) => outcome).sort(),
    ["dispatch", "unknown"],
  );
  const unknown = outcomes.find(({ outcome }) => outcome === "unknown");
  assert.equal(Object.hasOwn(unknown, "dispatchClaim"), false);
});

test("insert commit acknowledgement loss never returns a dispatch grant", async () => {
  const fixture = createFixture();
  fixture.harness.scripts.push({ commitOutcome: "ack-loss" });
  const outcome = await claim(fixture);
  assertCanonicalEqual(outcome, { outcome: "unknown" });
  assert.equal(Object.hasOwn(outcome, "dispatchClaim"), false);
  assert.equal(fixture.harness.rows.values().next().value.state, "starting");
});

test("a truly missing uncertain insert fails closed without a grant", async () => {
  const fixture = createFixture();
  fixture.harness.scripts.push({ commitOutcome: "uncertain-missing" });
  await assert.rejects(
    claim(fixture),
    catalogueError(
      "postgres_atomic_crash_capture_catalogue_outcome_uncertain",
    ),
  );
  assert.equal(fixture.harness.rows.size, 0);
});

test("markUncertain consumes its claim and durable uncertain never grants again", async () => {
  const fixture = createFixture();
  const first = await claim(fixture);
  assertCanonicalEqual(
    await fixture.catalogue.markUncertain({
      dispatchClaim: first.dispatchClaim,
    }),
    { outcome: "uncertain" },
  );
  assertCanonicalEqual(await claim(fixture), { outcome: "unknown" });
  await assert.rejects(
    fixture.catalogue.markUncertain({ dispatchClaim: first.dispatchClaim }),
    catalogueError("invalid_postgres_atomic_crash_capture_dispatch_claim"),
  );
});

test("a failed markUncertain burns the claim while starting stays non-retryable", async () => {
  const fixture = createFixture();
  const first = await claim(fixture);
  fixture.harness.scripts.push(
    { commitOutcome: "not-committed" },
    {},
    { commitOutcome: "uncertain-missing" },
    {},
  );
  await assert.rejects(
    fixture.catalogue.markUncertain({
      dispatchClaim: first.dispatchClaim,
    }),
    catalogueError(
      "postgres_atomic_crash_capture_catalogue_outcome_uncertain",
    ),
  );
  assert.equal(fixture.harness.rows.values().next().value.state, "starting");
  await assert.rejects(
    fixture.catalogue.markUncertain({
      dispatchClaim: first.dispatchClaim,
    }),
    catalogueError("invalid_postgres_atomic_crash_capture_dispatch_claim"),
  );
});

test("commit acknowledgement loss rehydrates the exact committed replay", async () => {
  const fixture = createFixture();
  const request = captureRequest();
  const first = await claim(fixture, request);
  fixture.harness.scripts.push({ commitOutcome: "ack-loss" });
  const outcome = await fixture.catalogue.commitResult({
    dispatchClaim: first.dispatchClaim,
    result: captureResult(request),
  });
  assert.equal(outcome.outcome, "committed");
  assertCanonicalEqual(outcome.result, captureResult(request));
  assert.deepEqual(await fixture.catalogue.readCommitted({ request }), outcome);
});

test("all four independently unique identities reject conflicting requests", async () => {
  const dimensions = [
    "captureAttemptId",
    "operationId",
    "checkpointId",
    "artifactId",
  ];
  for (const preserved of dimensions) {
    const fixture = createFixture();
    const original = captureRequest();
    await claim(fixture, original);
    const ids = {
      artifactId: "artifact-atomic-other",
      captureAttemptId: "capture-attempt-atomic-other",
      checkpointId: "checkpoint-atomic-other",
      operationId: "operation-atomic-other",
      [preserved]:
        preserved === "captureAttemptId"
          ? original.captureAttemptId
          : preserved === "operationId"
            ? original.mutationRequest.operationId
            : preserved === "checkpointId"
              ? original.checkpoint.checkpointId
              : original.checkpoint.artifactId,
    };
    await assert.rejects(
      claim(fixture, captureRequest(ids)),
      catalogueError("postgres_atomic_crash_capture_catalogue_conflict"),
      preserved,
    );
  }
});

test("request and provider-binding conflicts fail closed", async () => {
  const fixture = createFixture();
  const request = captureRequest();
  await claim(fixture, request);
  await assert.rejects(
    claim(fixture, captureRequest({ rootPath: "/different/source/path" })),
    catalogueError("postgres_atomic_crash_capture_catalogue_conflict"),
  );
  await assert.rejects(
    claim(fixture, request, providerBinding({ snapshotName: "other" })),
    catalogueError("postgres_atomic_crash_capture_catalogue_conflict"),
  );
});

test("mismatched committed result is rejected instead of replayed", async () => {
  const fixture = createFixture();
  const request = captureRequest();
  const first = await claim(fixture, request);
  const row = fixture.harness.rows.get(request.captureAttemptId);
  const exact = captureResult(request);
  row.state = "committed";
  row.result_json = structuredClone(exact);
  row.result_sha256 = canonicalSha256(exact);
  row.committed_at = new Date(NOW);
  await assert.rejects(
    fixture.catalogue.commitResult({
      dispatchClaim: first.dispatchClaim,
      result: captureResult(request, { proofId: "different-proof" }),
    }),
    catalogueError("postgres_atomic_crash_capture_catalogue_conflict"),
  );
});

test("provider bindings reject unsafe shapes and incompatible JSON", async () => {
  const cases = [
    new Proxy(providerBinding(), {}),
    Object.defineProperty(providerBinding(), "snapshotName", {
      enumerable: true,
      get() {
        throw new Error("must not execute");
      },
    }),
    (() => {
      const value = providerBinding();
      value.self = value;
      return value;
    })(),
    providerBinding({ snapshotTag: "x".repeat(70_000) }),
    providerBinding({ snapshotTag: "\u0000" }),
    providerBinding({ snapshotTag: "\ud800" }),
    { ["\udc00"]: "invalid-key" },
  ];
  for (const binding of cases) {
    const fixture = createFixture();
    await assert.rejects(
      claim(fixture, captureRequest(), binding),
      catalogueError(
        "invalid_postgres_atomic_crash_capture_catalogue_request",
      ),
    );
    assert.equal(fixture.harness.trace.length, 0);
  }
});

test("provider-binding admission uses the exact canonical UTF-8 boundary", async () => {
  const highOverhead = {
    entries: new Array(8_189).fill(0),
    padding: "",
  };
  highOverhead.padding = "x".repeat(
    65_536 - Buffer.byteLength(canonicalJson(highOverhead), "utf8"),
  );
  for (const binding of [
    { payload: "x".repeat(65_522) },
    { payload: "é".repeat(32_761) },
    highOverhead,
  ]) {
    const fixture = createFixture();
    assert.equal((await claim(fixture, captureRequest(), binding)).outcome, "dispatch");
    assert.equal(
      Buffer.byteLength(fixture.harness.trace[0].values[11], "utf8"),
      65_536,
    );
  }

  const fixture = createFixture();
  await assert.rejects(
    claim(fixture, captureRequest(), { payload: "x".repeat(65_523) }),
    catalogueError(
      "invalid_postgres_atomic_crash_capture_catalogue_request",
    ),
  );
  assert.equal(fixture.harness.trace.length, 0);
});

test("dispatch claims reject proxies and forgeries before PostgreSQL", async () => {
  const fixture = createFixture();
  const before = fixture.harness.trace.length;
  for (const dispatchClaim of [
    Object.freeze(Object.create(null)),
    new Proxy(Object.freeze(Object.create(null)), {}),
  ]) {
    await assert.rejects(
      fixture.catalogue.markUncertain({ dispatchClaim }),
      catalogueError("invalid_postgres_atomic_crash_capture_dispatch_claim"),
    );
  }
  assert.equal(fixture.harness.trace.length, before);
});

test("catalogue persists exact canonical provider-binding JSON", async () => {
  const fixture = createFixture();
  const request = captureRequest();
  const binding = { z: 0, a: ["é", true] };

  assert.equal((await claim(fixture, request, binding)).outcome, "dispatch");
  const canonical = '{"a":["é",true],"z":0}';
  assert.equal(fixture.harness.trace[0].values[11], canonical);
  assert.equal(
    fixture.harness.rows.get(request.captureAttemptId).provider_binding_json,
    canonical,
  );
  fixture.harness.rows.get(request.captureAttemptId).provider_binding_json =
    `{ ${canonical.slice(1)}`;
  await assert.rejects(
    claim(fixture, request, binding),
    catalogueError(
      "postgres_atomic_crash_capture_catalogue_state_invalid",
    ),
  );
});

test("tampered database rows are rejected as invalid durable state", async () => {
  const fixture = createFixture();
  const request = captureRequest();
  const first = await claim(fixture, request);
  await fixture.catalogue.commitResult({
    dispatchClaim: first.dispatchClaim,
    result: captureResult(request),
  });
  fixture.harness.rows.get(request.captureAttemptId).provider_binding.snapshotName =
    "tampered";
  await assert.rejects(
    fixture.catalogue.readCommitted({ request }),
    catalogueError(
      "postgres_atomic_crash_capture_catalogue_state_invalid",
    ),
  );
});

test("migration defines immutable transition audit and permanent rows", async () => {
  const sql = await readFile(
    new URL(
      "../migrations/authority/012-atomic-crash-capture-catalogue.sql",
      import.meta.url,
    ),
    "utf8",
  );
  for (const column of ["claimed_at", "uncertain_at", "committed_at"]) {
    assert.match(sql, new RegExp(`\\b${column}\\b`, "u"));
  }
  assert.match(
    sql,
    /OLD\.state IN \('starting', 'uncertain'\)[\s\S]+NEW\.state = 'committed'/u,
  );
  assert.match(sql, /atomic_crash_captures_insert_guard/u);
  assert.match(sql, /atomic_crash_captures_update_guard/u);
  assert.match(sql, /atomic_crash_captures_delete_guard/u);
  assert.match(sql, /atomic_crash_captures_truncate_guard/u);
  assert.match(
    sql,
    /provider_binding_json text COLLATE pg_catalog\."C" NOT NULL/u,
  );
  assert.match(
    sql,
    /octet_length\(\s*pg_catalog\.convert_to\(provider_binding_json, 'UTF8'\)\s*\) BETWEEN 2 AND 65536/u,
  );
  assert.match(
    sql,
    /provider_binding_json::pg_catalog\.jsonb = provider_binding/u,
  );
  assert.doesNotMatch(sql, /pg_column_size\(provider_binding\)/u);
  assert.match(sql, /NEW\.claimed_at := pg_catalog\.transaction_timestamp\(\)/u);
  assert.match(sql, /NEW\.uncertain_at := pg_catalog\.transaction_timestamp\(\)/u);
  assert.match(sql, /NEW\.committed_at := pg_catalog\.transaction_timestamp\(\)/u);
});
