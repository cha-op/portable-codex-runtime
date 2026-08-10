import assert from "node:assert/strict";
import test from "node:test";

import {
  PostgresRestoreRecoveryRunnerError,
  createPostgresRestoreRecoveryRunner,
} from "../src/postgres-restore-recovery-runner.mjs";
import {
  createPostgresRestoreActivationRecoveryService,
  isPostgresRestoreActivationRecoveryService,
} from "../src/postgres-restore-activation-recovery-service.mjs";

const RECOVERY_SCOPE_ID = "restore-recovery-scope-001";
const SESSION_ID_1 = "019f2600-0000-7000-8000-000000000001";
const SESSION_ID_2 = "019f2600-0000-7000-8000-000000000002";
const CODEX_ID = "019f2600-0000-7000-8000-000000000003";
const IMAGE_DIGEST = `sha256:${"a".repeat(64)}`;
const UPDATED_AT = "2026-08-10T00:00:00.000Z";
const LANE_SPECS = [
  { field: "generation", lane: "generation" },
  { field: "activation", lane: "activation" },
  { field: "launchAttempt", lane: "launch-attempt" },
  { field: "currentLaunch", lane: "current-launch" },
];

function freezeRecord(value) {
  return Object.freeze(Object.assign(Object.create(null), value));
}

function checkpoint(sessionId = SESSION_ID_1) {
  return {
    contractVersion: 1,
    checkpointId: `checkpoint-${sessionId}`,
    artifactId: `artifact-${sessionId}`,
    backendId: "backend-001",
    storageId: "storage-001",
    sessionId,
    codexThreadId: CODEX_ID,
    codexSessionId: CODEX_ID,
    imageDigest: IMAGE_DIGEST,
    sourceFencingEpoch: "1",
    checkpointClass: "clean",
    createdAt: "2026-08-10T00:00:00.000Z",
  };
}

function restoreRequest(sessionId = SESSION_ID_1) {
  const value = checkpoint(sessionId);
  return {
    contractVersion: 1,
    backendId: value.backendId,
    storageId: value.storageId,
    sessionId,
    leaseId: `lease-${sessionId}`,
    holderId: "host-001",
    fencingEpoch: "2",
    operation: "restore",
    operationId: `restore-${sessionId}`,
    target: {
      artifactId: value.artifactId,
      checkpointId: value.checkpointId,
      kind: "checkpoint",
    },
  };
}

function generationCandidate(sessionId = SESSION_ID_1) {
  return {
    checkpoint: checkpoint(sessionId),
    generationId: `generation-${sessionId}`,
    request: restoreRequest(sessionId),
  };
}

function page(candidates = [], nextAfterSessionId = null) {
  return { candidates, nextAfterSessionId };
}

function cursor(
  lane,
  {
    afterSessionId = null,
    cycle = "0",
    lastRequestSha256 = null,
    lastTransitionId = null,
    revision = "0",
  } = {},
) {
  return freezeRecord({
    recoveryScopeId: RECOVERY_SCOPE_ID,
    lane,
    afterSessionId,
    cycle,
    revision,
    lastTransitionId,
    lastRequestSha256,
    updatedAt: UPDATED_AT,
  });
}

function increment(value) {
  return `${BigInt(value) + 1n}`;
}

function createCursorStore({
  advanceResult = null,
  cursors = {},
  onAdvance = null,
  onRead = null,
  recordCalls = true,
} = {}) {
  const calls = [];
  const state = new Map();
  for (const { field, lane } of LANE_SPECS) {
    state.set(lane, cursors[field] ?? cursor(lane));
  }

  const store = freezeRecord({
    async readLane(input) {
      if (recordCalls) calls.push(["read", input]);
      onRead?.(input);
      return state.get(input.lane);
    },
    async advanceLane(input) {
      if (recordCalls) calls.push(["advance", input]);
      onAdvance?.(input);
      const before = state.get(input.lane);
      const advancedCursor = cursor(input.lane, {
        afterSessionId: input.nextAfterSessionId,
        cycle:
          input.nextAfterSessionId === null
            ? increment(before.cycle)
            : before.cycle,
        lastRequestSha256: input.requestSha256,
        lastTransitionId: input.transitionId,
        revision: increment(before.revision),
      });
      state.set(input.lane, advancedCursor);
      return advanceResult?.(input, advancedCursor) ??
        freezeRecord({ advanced: true, cursor: advancedCursor });
    },
  });
  return { calls, state, store };
}

function createRecoveryService({
  listOverrides = {},
  onList = null,
  onReconcileGeneration = null,
  pages = {},
  recordCalls = true,
} = {}) {
  const calls = [];
  const reconcileCalls = [];

  function list(field, input) {
    if (recordCalls) calls.push([field, input]);
    onList?.(field, input);
    if (listOverrides[field]) return listOverrides[field](input);
    return pages[field] ?? page();
  }

  const service = createPostgresRestoreActivationRecoveryService({
    listCurrentWriterLaunchCandidates(input) {
      return list("currentLaunch", input);
    },
    listRestoreAttachmentActivationCandidates(input) {
      return list("activation", input);
    },
    listRestoreGenerationCandidates(input) {
      return list("generation", input);
    },
    listWriterLaunchAttemptCandidates(input) {
      return list("launchAttempt", input);
    },
    reconcileRestoreAttachmentActivation(candidate) {
      if (recordCalls) reconcileCalls.push(["activation", candidate]);
    },
    reconcileRestoreGeneration(candidate) {
      if (recordCalls) reconcileCalls.push(["generation", candidate]);
      return onReconcileGeneration?.(candidate);
    },
    reconcileWriterLaunchAttempt(candidate) {
      if (recordCalls) reconcileCalls.push(["launchAttempt", candidate]);
    },
  });
  assert.equal(isPostgresRestoreActivationRecoveryService(service), true);
  return { calls, reconcileCalls, service };
}

function limits(overrides = {}) {
  return {
    generation: 2,
    activation: 3,
    launchAttempt: 4,
    currentLaunch: 5,
    ...overrides,
  };
}

function createRunner({ cursorFixture, limitValues, serviceFixture } = {}) {
  const cursorValue = cursorFixture ?? createCursorStore();
  const serviceValue = serviceFixture ?? createRecoveryService();
  const runner = createPostgresRestoreRecoveryRunner({
    cursorStore: cursorValue.store,
    recoveryService: serviceValue.service,
    recoveryScopeId: RECOVERY_SCOPE_ID,
    limits: limitValues ?? limits(),
  });
  return { cursorFixture: cursorValue, runner, serviceFixture: serviceValue };
}

async function runGenerationDigest(status, { onReconcile = null } = {}) {
  const serviceFixture = createRecoveryService({
    pages: {
      generation: page([generationCandidate()], SESSION_ID_1),
    },
    onReconcileGeneration(candidate) {
      onReconcile?.(candidate);
      if (status === "pending") throw new Error("remains pending");
    },
    recordCalls: false,
  });
  const cursorFixture = createCursorStore({ recordCalls: false });
  const { runner } = createRunner({
    cursorFixture,
    limitValues: limits({ generation: 1 }),
    serviceFixture,
  });
  return runner.runOnce({ signal: null });
}

function assertCode(code) {
  return (error) =>
    error instanceof PostgresRestoreRecoveryRunnerError && error.code === code;
}

function restoreOwnProperty(target, key, descriptor) {
  if (descriptor === undefined) {
    delete target[key];
  } else {
    Object.defineProperty(target, key, descriptor);
  }
}

function assertDeepFrozen(value, seen = new Set()) {
  if (value === null || typeof value !== "object" || seen.has(value)) return;
  seen.add(value);
  assert.equal(Object.isFrozen(value), true);
  for (const child of Object.values(value)) assertDeepFrozen(child, seen);
}

test("runs and durably advances four recovery lanes in fixed order", async () => {
  const events = [];
  const cursorFixture = createCursorStore({
    onAdvance(input) {
      events.push(`advance:${input.lane}`);
    },
    onRead(input) {
      events.push(`read:${input.lane}`);
    },
  });
  const serviceFixture = createRecoveryService({
    onList(field) {
      events.push(`batch:${field}`);
    },
  });
  const { runner } = createRunner({ cursorFixture, serviceFixture });

  const result = await runner.runOnce({ signal: null });

  assert.deepEqual(events, [
    "read:generation",
    "batch:generation",
    "advance:generation",
    "read:activation",
    "batch:activation",
    "advance:activation",
    "read:launch-attempt",
    "batch:launchAttempt",
    "advance:launch-attempt",
    "read:current-launch",
    "batch:currentLaunch",
    "advance:current-launch",
  ]);
  assert.equal(result.status, "sweep-complete");
  assert.equal(result.recoveryScopeId, RECOVERY_SCOPE_ID);
  for (const { field, lane } of LANE_SPECS) {
    const receipt = result[field];
    assert.equal(receipt.cursorBefore.lane, lane);
    assert.equal(receipt.batch.status, "sweep-complete");
    assert.match(receipt.transitionId, /^[0-9a-f-]{36}$/u);
    assert.match(receipt.requestSha256, /^[0-9a-f]{64}$/u);
    assert.equal(receipt.advance.advanced, true);
    assert.equal(receipt.advance.cursor.revision, "1");
    assert.equal(receipt.advance.cursor.cycle, "1");
    assert.equal(receipt.advance.cursor.lastTransitionId, receipt.transitionId);
    assert.equal(
      receipt.advance.cursor.lastRequestSha256,
      receipt.requestSha256,
    );
    assert.deepEqual(Reflect.ownKeys(receipt), [
      "cursorBefore",
      "batch",
      "transitionId",
      "requestSha256",
      "advance",
    ]);
  }
  assertDeepFrozen(result);
  assertDeepFrozen(runner);
  assert.deepEqual(Reflect.ownKeys(runner), ["runOnce"]);
});

test(
  "callback-time iterator pollution cannot reroute durable lanes",
  { concurrency: false },
  async () => {
    const iteratorDescriptor = Object.getOwnPropertyDescriptor(
      Array.prototype,
      Symbol.iterator,
    );
    const originalIterator = Array.prototype[Symbol.iterator];
    const forgedActivationLane = [
      "activation",
      "generation",
      "runActivationBatch",
    ];
    let poisonedLaneReads = 0;
    let pollutionInstalled = false;
    const serviceFixture = createRecoveryService({
      pages: {
        generation: page([generationCandidate()]),
      },
      onReconcileGeneration() {
        pollutionInstalled = true;
        Object.defineProperty(Array.prototype, Symbol.iterator, {
          configurable: true,
          value() {
            if (
              this.length === 3 &&
              this[0] === "activation" &&
              this[1] === "activation" &&
              this[2] === "runActivationBatch"
            ) {
              poisonedLaneReads += 1;
              return Reflect.apply(
                originalIterator,
                forgedActivationLane,
                [],
              );
            }
            return Reflect.apply(originalIterator, this, []);
          },
          writable: true,
        });
      },
    });
    const { cursorFixture, runner } = createRunner({ serviceFixture });
    let observedError = null;
    let result;

    try {
      result = await runner.runOnce({ signal: null });
    } catch (error) {
      observedError = error;
    } finally {
      restoreOwnProperty(
        Array.prototype,
        Symbol.iterator,
        iteratorDescriptor,
      );
    }

    assert.equal(observedError, null);
    assert.equal(pollutionInstalled, true);
    assert.equal(poisonedLaneReads, 0);
    assert.equal(result.activation.cursorBefore.lane, "activation");
    assert.equal(cursorFixture.state.get("generation").revision, "1");
    assert.equal(cursorFixture.state.get("activation").revision, "1");
    assert.equal(cursorFixture.state.get("launch-attempt").revision, "1");
    assert.equal(cursorFixture.state.get("current-launch").revision, "1");
  },
);

test(
  "callback-time inherited frozen options cannot block a later run",
  { concurrency: false },
  async () => {
    const frozenDescriptor = Object.getOwnPropertyDescriptor(
      Object.prototype,
      "frozen",
    );
    let getterCalls = 0;
    let pollutionInstalled = false;
    const serviceFixture = createRecoveryService({
      pages: {
        generation: page([generationCandidate()]),
      },
      onReconcileGeneration() {
        if (pollutionInstalled) return;
        pollutionInstalled = true;
        Object.defineProperty(Object.prototype, "frozen", {
          configurable: true,
          get() {
            getterCalls += 1;
            throw new Error("inherited frozen getter must not run");
          },
        });
      },
      recordCalls: false,
    });
    const cursorFixture = createCursorStore({ recordCalls: false });
    const { runner } = createRunner({ cursorFixture, serviceFixture });
    let first;
    let observedError = null;
    let second;

    try {
      first = await runner.runOnce({ signal: null });
      second = await runner.runOnce({ signal: null });
    } catch (error) {
      observedError = error;
    } finally {
      restoreOwnProperty(Object.prototype, "frozen", frozenDescriptor);
    }

    assert.equal(observedError, null);
    assert.equal(pollutionInstalled, true);
    assert.equal(getterCalls, 0);
    assert.equal(first.generation.advance.cursor.revision, "1");
    assert.equal(second.generation.cursorBefore.revision, "1");
    assert.equal(second.generation.advance.cursor.revision, "2");
    assert.equal(cursorFixture.state.get("generation").revision, "2");
  },
);

test(
  "invalid pages cannot inject candidates through inherited numeric setters",
  { concurrency: false },
  async () => {
    async function runWithPrototype(prototype) {
      const numericDescriptor = Object.getOwnPropertyDescriptor(prototype, "0");
      const injectedCandidate = generationCandidate();
      let setterCalls = 0;
      let advanceCalls = 0;
      const cursorFixture = createCursorStore({
        onAdvance() {
          advanceCalls += 1;
        },
        recordCalls: false,
      });
      const serviceFixture = createRecoveryService({
        listOverrides: {
          generation() {
            Object.defineProperty(prototype, "0", {
              configurable: true,
              set(value) {
                if (Array.isArray(this) && value === null) {
                  setterCalls += 1;
                  restoreOwnProperty(prototype, "0", numericDescriptor);
                  Object.defineProperty(this, "0", {
                    configurable: true,
                    enumerable: true,
                    value: injectedCandidate,
                    writable: true,
                  });
                  return;
                }
                Object.defineProperty(this, "0", {
                  configurable: true,
                  enumerable: true,
                  value,
                  writable: true,
                });
              },
            });
            return page([null]);
          },
        },
        recordCalls: false,
      });
      const { runner } = createRunner({ cursorFixture, serviceFixture });
      let observedError = null;

      try {
        await runner.runOnce({ signal: null });
      } catch (error) {
        observedError = error;
      } finally {
        restoreOwnProperty(prototype, "0", numericDescriptor);
      }

      return { advanceCalls, cursorFixture, observedError, setterCalls };
    }

    for (const prototype of [Array.prototype, Object.prototype]) {
      const observed = await runWithPrototype(prototype);
      assert.equal(
        assertCode("postgres_restore_recovery_runner_outcome_uncertain")(
          observed.observedError,
        ),
        true,
      );
      assert.equal(observed.setterCalls, 0);
      assert.equal(observed.advanceCalls, 0);
      assert.equal(observed.cursorFixture.state.get("generation").revision, "0");
      assert.equal(observed.cursorFixture.state.get("generation").cycle, "0");
      assert.equal(
        observed.cursorFixture.state.get("generation").afterSessionId,
        null,
      );
    }
  },
);

test(
  "inherited non-writable numeric properties cannot block settled batches",
  { concurrency: false },
  async () => {
    const protectedIndex = "99";
    const numericDescriptor = Object.getOwnPropertyDescriptor(
      Array.prototype,
      protectedIndex,
    );
    const candidates = Array.from({ length: 100 }, (_, index) =>
      generationCandidate(
        `019f2600-0000-7000-8000-${`${index + 1}`.padStart(12, "0")}`,
      ),
    );
    let pollutionInstalled = false;
    const cursorFixture = createCursorStore({ recordCalls: false });
    const serviceFixture = createRecoveryService({
      listOverrides: {
        generation() {
          pollutionInstalled = true;
          Object.defineProperty(Array.prototype, protectedIndex, {
            configurable: true,
            value: null,
            writable: false,
          });
          return page(candidates);
        },
      },
      recordCalls: false,
    });
    const { runner } = createRunner({
      cursorFixture,
      limitValues: limits({ generation: 100 }),
      serviceFixture,
    });
    let observedError = null;
    let result;

    try {
      result = await runner.runOnce({ signal: null });
    } catch (error) {
      observedError = error;
    } finally {
      restoreOwnProperty(Array.prototype, protectedIndex, numericDescriptor);
    }

    assert.equal(observedError, null);
    assert.equal(pollutionInstalled, true);
    assert.equal(result.generation.batch.results.length, 100);
    assert.equal(
      result.generation.batch.results[99].sessionId,
      "019f2600-0000-7000-8000-000000000100",
    );
    assert.equal(
      Object.getPrototypeOf(result.generation.batch.results),
      Array.prototype,
    );
    assert.equal(cursorFixture.state.get("generation").revision, "1");
  },
);

test("passes startup-fixed lane limits and persists each settled continuation", async () => {
  const serviceFixture = createRecoveryService({
    pages: {
      generation: page([generationCandidate()], SESSION_ID_1),
      currentLaunch: page([], SESSION_ID_2),
    },
  });
  const { cursorFixture, runner } = createRunner({
    limitValues: limits({ generation: 1 }),
    serviceFixture,
  });

  const result = await runner.runOnce({ signal: null });

  assert.equal(result.status, "limit-reached");
  assert.deepEqual(
    serviceFixture.calls.map(([lane, input]) => [lane, input.limit]),
    [
      ["generation", 1],
      ["activation", 3],
      ["launchAttempt", 4],
      ["currentLaunch", 5],
    ],
  );
  for (const [, input] of serviceFixture.calls) {
    assert.equal(Object.isFrozen(input), true);
  }
  assert.equal(cursorFixture.state.get("generation").afterSessionId, SESSION_ID_1);
  assert.equal(cursorFixture.state.get("generation").cycle, "0");
  assert.equal(
    cursorFixture.state.get("current-launch").afterSessionId,
    SESSION_ID_2,
  );
  const generationAdvance = cursorFixture.calls.find(
    ([kind, input]) => kind === "advance" && input.lane === "generation",
  )[1];
  assert.deepEqual(Reflect.ownKeys(generationAdvance), [
    "recoveryScopeId",
    "lane",
    "transitionId",
    "expectedRevision",
    "expectedCycle",
    "expectedAfterSessionId",
    "nextAfterSessionId",
    "requestSha256",
  ]);
  assert.equal(Object.isFrozen(generationAdvance), true);
});

test("request hashes are deterministic over cursor state, limit, and full batch", async () => {
  const first = await runGenerationDigest("pending");
  const replay = await runGenerationDigest("pending");
  const changed = await runGenerationDigest("reconciled");

  assert.equal(first.generation.requestSha256, replay.generation.requestSha256);
  assert.notEqual(first.generation.transitionId, replay.generation.transitionId);
  assert.notEqual(first.generation.requestSha256, changed.generation.requestSha256);

  const narrow = await createRunner({
    limitValues: limits({ generation: 2 }),
  }).runner.runOnce({ signal: null });
  const wide = await createRunner({
    limitValues: limits({ generation: 3 }),
  }).runner.runOnce({ signal: null });
  assert.notEqual(
    narrow.generation.requestSha256,
    wide.generation.requestSha256,
  );
});

test(
  "request hashes ignore post-import prototype toJSON pollution",
  { concurrency: false },
  async () => {
    const baselinePending = await runGenerationDigest("pending");
    const baselineReconciled = await runGenerationDigest("reconciled");
    assert.equal(
      baselinePending.generation.requestSha256,
      "e9ae895a920e0540006f2e33e935e1ec146870504412d9378d225ed39ef4b0e1",
    );
    assert.equal(
      baselineReconciled.generation.requestSha256,
      "b2fcc93c8f30a210bfb83386eb8bf70324b2abad3c2ecea56a791f2c66de3233",
    );
    async function runWithPollution(prototype, label) {
      const descriptor = Object.getOwnPropertyDescriptor(prototype, "toJSON");
      let calls = 0;
      let pending;
      let reconciled;
      try {
        Object.defineProperty(prototype, "toJSON", {
          configurable: true,
          value() {
            calls += 1;
            return `polluted-${label}`;
          },
        });
        pending = await runGenerationDigest("pending");
        reconciled = await runGenerationDigest("reconciled");
      } finally {
        if (descriptor === undefined) {
          delete prototype.toJSON;
        } else {
          Object.defineProperty(prototype, "toJSON", descriptor);
        }
      }
      return { calls, pending, reconciled };
    }

    const arrayPollution = await runWithPollution(
      Array.prototype,
      "array",
    );
    const objectPollution = await runWithPollution(
      Object.prototype,
      "object",
    );

    for (const pollution of [arrayPollution, objectPollution]) {
      assert.equal(pollution.calls, 0);
      assert.equal(
        pollution.pending.generation.requestSha256,
        baselinePending.generation.requestSha256,
      );
      assert.equal(
        pollution.reconciled.generation.requestSha256,
        baselineReconciled.generation.requestSha256,
      );
      assert.notEqual(
        pollution.pending.generation.requestSha256,
        pollution.reconciled.generation.requestSha256,
      );
    }
  },
);

test(
  "request hashes ignore callback-time inherited numeric setters",
  { concurrency: false },
  async () => {
    async function runWithPollution(prototype, status) {
      const descriptor = Object.getOwnPropertyDescriptor(prototype, "0");
      let pollutionInstalled = false;
      let setterCalls = 0;
      let observedError = null;
      let result;

      try {
        result = await runGenerationDigest(status, {
          onReconcile() {
            pollutionInstalled = true;
            Object.defineProperty(prototype, "0", {
              configurable: true,
              set(value) {
                if (
                  value !== null &&
                  typeof value === "object" &&
                  Object.hasOwn(value, "operationId") &&
                  Object.hasOwn(value, "sessionId") &&
                  Object.hasOwn(value, "status")
                ) {
                  setterCalls += 1;
                }
                Object.defineProperty(this, "0", {
                  configurable: true,
                  enumerable: true,
                  value,
                  writable: true,
                });
              },
            });
          },
        });
      } catch (error) {
        observedError = error;
      } finally {
        restoreOwnProperty(prototype, "0", descriptor);
      }

      assert.equal(observedError, null);
      assert.equal(pollutionInstalled, true);
      assert.equal(setterCalls, 0);
      assert.equal(
        Object.getPrototypeOf(result.generation.batch.results),
        Array.prototype,
      );
      return result.generation.requestSha256;
    }

    for (const prototype of [Array.prototype, Object.prototype]) {
      assert.equal(
        await runWithPollution(prototype, "pending"),
        "e9ae895a920e0540006f2e33e935e1ec146870504412d9378d225ed39ef4b0e1",
      );
      assert.equal(
        await runWithPollution(prototype, "reconciled"),
        "b2fcc93c8f30a210bfb83386eb8bf70324b2abad3c2ecea56a791f2c66de3233",
      );
    }
  },
);

test("abort before the first read performs no cursor initialization or recovery work", async () => {
  const controller = new AbortController();
  controller.abort();
  const { cursorFixture, runner, serviceFixture } = createRunner();

  const result = await runner.runOnce({ signal: controller.signal });

  assert.deepEqual({ ...result }, {
    recoveryScopeId: RECOVERY_SCOPE_ID,
    generation: null,
    activation: null,
    launchAttempt: null,
    currentLaunch: null,
    status: "aborted",
  });
  assert.deepEqual(cursorFixture.calls, []);
  assert.deepEqual(serviceFixture.calls, []);
  assertDeepFrozen(result);
});

test("persists an aborted lane's settled cursor before stopping later lanes", async () => {
  const controller = new AbortController();
  const serviceFixture = createRecoveryService({
    pages: {
      generation: page([generationCandidate()]),
    },
    onReconcileGeneration() {
      controller.abort();
    },
  });
  const { cursorFixture, runner } = createRunner({ serviceFixture });

  const result = await runner.runOnce({ signal: controller.signal });

  assert.equal(result.status, "aborted");
  assert.equal(result.generation.batch.nextAfterSessionId, SESSION_ID_1);
  assert.equal(result.generation.advance.cursor.afterSessionId, SESSION_ID_1);
  assert.equal(result.activation, null);
  assert.equal(result.launchAttempt, null);
  assert.equal(result.currentLaunch, null);
  assert.deepEqual(
    cursorFixture.calls.map(([kind, input]) => `${kind}:${input.lane}`),
    ["read:generation", "advance:generation"],
  );
});

test("does not advance an aborted lane without settled cursor progress", async () => {
  const controller = new AbortController();
  const serviceFixture = createRecoveryService({
    listOverrides: {
      generation(input) {
        controller.abort();
        assert.equal(input.afterSessionId, null);
        return page();
      },
    },
  });
  const { cursorFixture, runner } = createRunner({ serviceFixture });

  const result = await runner.runOnce({ signal: controller.signal });

  assert.equal(result.status, "aborted");
  assert.equal(result.generation.cursorBefore.revision, "0");
  assert.equal(result.generation.batch.status, "aborted");
  assert.equal(result.generation.transitionId, null);
  assert.equal(result.generation.requestSha256, null);
  assert.equal(result.generation.advance, null);
  assert.equal(result.activation, null);
  assert.equal(cursorFixture.state.get("generation").revision, "0");
  assert.deepEqual(
    cursorFixture.calls.map(([kind, input]) => `${kind}:${input.lane}`),
    ["read:generation"],
  );
  assertDeepFrozen(result);
});

test("a later failure leaves every earlier lane durably advanced", async () => {
  const serviceFixture = createRecoveryService({
    listOverrides: {
      activation() {
        throw new Error("private database failure");
      },
    },
  });
  const { cursorFixture, runner } = createRunner({ serviceFixture });

  let observed;
  await assert.rejects(runner.runOnce({ signal: null }), (error) => {
    observed = error;
    return assertCode("postgres_restore_recovery_runner_outcome_uncertain")(
      error,
    );
  });

  assert.equal(observed.message.includes("private database failure"), false);
  assert.equal(cursorFixture.state.get("generation").revision, "1");
  assert.equal(cursorFixture.state.get("activation").revision, "0");
  assert.deepEqual(
    cursorFixture.calls.map(([kind, input]) => `${kind}:${input.lane}`),
    ["read:generation", "advance:generation", "read:activation"],
  );
});

test("collaborators cannot forge a runner request classification", async () => {
  const serviceFixture = createRecoveryService({
    listOverrides: {
      generation() {
        throw new PostgresRestoreRecoveryRunnerError(
          "invalid_postgres_restore_recovery_runner_request",
        );
      },
    },
  });
  const { runner } = createRunner({ serviceFixture });

  await assert.rejects(
    runner.runOnce({ signal: null }),
    assertCode("postgres_restore_recovery_runner_outcome_uncertain"),
  );
});

test("one runner admits only one run at a time", async () => {
  let release;
  const waiting = new Promise((resolve) => {
    release = resolve;
  });
  const cursorFixture = createCursorStore({
    async onRead() {
      await waiting;
    },
  });
  const original = cursorFixture.store;
  const blockingStore = freezeRecord({
    async readLane(input) {
      await waiting;
      return original.readLane(input);
    },
    advanceLane: original.advanceLane,
  });
  const { runner } = createRunner({
    cursorFixture: { ...cursorFixture, store: blockingStore },
  });

  const first = runner.runOnce({ signal: null });
  await assert.rejects(
    runner.runOnce({ signal: null }),
    assertCode("postgres_restore_recovery_runner_outcome_uncertain"),
  );
  release();
  await first;
});

test("accepts a validated idempotent advance replay receipt", async () => {
  const cursorFixture = createCursorStore({
    advanceResult(_input, advancedCursor) {
      return freezeRecord({ advanced: false, cursor: advancedCursor });
    },
  });
  const { runner } = createRunner({ cursorFixture });

  const result = await runner.runOnce({ signal: null });

  for (const { field } of LANE_SPECS) {
    assert.equal(result[field].advance.advanced, false);
  }
});

test("rejects malformed cursor and advance receipts", async () => {
  const malformedCursorStore = createCursorStore();
  const badCursorStore = freezeRecord({
    ...malformedCursorStore.store,
    async readLane(input) {
      return { ...cursor(input.lane), revision: "01" };
    },
  });
  const badCursor = createRunner({
    cursorFixture: { ...malformedCursorStore, store: badCursorStore },
  });
  await assert.rejects(
    badCursor.runner.runOnce({ signal: null }),
    assertCode("postgres_restore_recovery_runner_outcome_uncertain"),
  );

  const badAdvanceFixture = createCursorStore({
    advanceResult(_input, advancedCursor) {
      return freezeRecord({
        advanced: true,
        cursor: freezeRecord({ ...advancedCursor, cycle: "99" }),
      });
    },
  });
  const badAdvance = createRunner({ cursorFixture: badAdvanceFixture });
  await assert.rejects(
    badAdvance.runner.runOnce({ signal: null }),
    assertCode("postgres_restore_recovery_runner_outcome_uncertain"),
  );
});

test("rejects a frozen recovery-service lookalike before any method call", () => {
  let methodCalls = 0;
  function forgedMethod() {
    methodCalls += 1;
    return freezeRecord({});
  }
  const forgedService = freezeRecord({
    runActivationBatch: forgedMethod,
    runGenerationBatch: forgedMethod,
    runLaunchAttemptBatch: forgedMethod,
    runSweep: forgedMethod,
    scanCurrentLaunchBatch: forgedMethod,
  });

  assert.throws(
    () =>
      createPostgresRestoreRecoveryRunner({
        cursorStore: createCursorStore().store,
        recoveryService: forgedService,
        recoveryScopeId: RECOVERY_SCOPE_ID,
        limits: limits(),
      }),
    assertCode("invalid_postgres_restore_recovery_runner_options"),
  );
  assert.equal(methodCalls, 0);
});

test("rejects proxy, accessor, generator, and thenable expansion without traps", async () => {
  let proxyTrapCalls = 0;
  const proxiedOptions = new Proxy(
    {
      cursorStore: createCursorStore().store,
      recoveryService: createRecoveryService().service,
      recoveryScopeId: RECOVERY_SCOPE_ID,
      limits: limits(),
    },
    {
      get() {
        proxyTrapCalls += 1;
        throw new Error("proxy trap must not run");
      },
      getOwnPropertyDescriptor() {
        proxyTrapCalls += 1;
        throw new Error("proxy trap must not run");
      },
      getPrototypeOf() {
        proxyTrapCalls += 1;
        throw new Error("proxy trap must not run");
      },
      ownKeys() {
        proxyTrapCalls += 1;
        throw new Error("proxy trap must not run");
      },
    },
  );
  assert.throws(
    () => createPostgresRestoreRecoveryRunner(proxiedOptions),
    assertCode("invalid_postgres_restore_recovery_runner_options"),
  );
  assert.equal(proxyTrapCalls, 0);

  let getterCalls = 0;
  const accessorLimits = limits();
  Object.defineProperty(accessorLimits, "generation", {
    enumerable: true,
    get() {
      getterCalls += 1;
      throw new Error("accessor must not run");
    },
  });
  assert.throws(
    () =>
      createPostgresRestoreRecoveryRunner({
        cursorStore: createCursorStore().store,
        recoveryService: createRecoveryService().service,
        recoveryScopeId: RECOVERY_SCOPE_ID,
        limits: accessorLimits,
      }),
    assertCode("invalid_postgres_restore_recovery_runner_options"),
  );
  assert.equal(getterCalls, 0);

  const generatorStore = freezeRecord({
    readLane: function* invalidRead() {
      yield "cursor";
    },
    async advanceLane() {},
  });
  assert.throws(
    () =>
      createPostgresRestoreRecoveryRunner({
        cursorStore: generatorStore,
        recoveryService: createRecoveryService().service,
        recoveryScopeId: RECOVERY_SCOPE_ID,
        limits: limits(),
      }),
    assertCode("invalid_postgres_restore_recovery_runner_options"),
  );

  let thenCalls = 0;
  const thenableStore = freezeRecord({
    readLane() {
      return {
        then() {
          thenCalls += 1;
        },
      };
    },
    async advanceLane() {},
  });
  const thenableRunner = createPostgresRestoreRecoveryRunner({
    cursorStore: thenableStore,
    recoveryService: createRecoveryService().service,
    recoveryScopeId: RECOVERY_SCOPE_ID,
    limits: limits(),
  });
  await assert.rejects(
    thenableRunner.runOnce({ signal: null }),
    assertCode("postgres_restore_recovery_runner_outcome_uncertain"),
  );
  assert.equal(thenCalls, 0);
});

test("rejects Promise subclasses without invoking an overridden then", async () => {
  let thenCalls = 0;
  class SpoofedPromise extends Promise {
    then(onFulfilled, onRejected) {
      thenCalls += 1;
      return Promise.resolve(cursor("generation")).then(
        onFulfilled,
        onRejected,
      );
    }
  }
  const spoofed = new SpoofedPromise((resolve) => resolve(cursor("generation")));
  const store = freezeRecord({
    readLane() {
      return spoofed;
    },
    async advanceLane() {},
  });
  const runner = createPostgresRestoreRecoveryRunner({
    cursorStore: store,
    recoveryService: createRecoveryService().service,
    recoveryScopeId: RECOVERY_SCOPE_ID,
    limits: limits(),
  });

  await assert.rejects(
    runner.runOnce({ signal: null }),
    assertCode("postgres_restore_recovery_runner_outcome_uncertain"),
  );
  assert.equal(thenCalls, 0);
});

test("rejects proxied and accessor collaborator records without traps", async () => {
  let proxyTrapCalls = 0;
  const proxiedCursor = new Proxy(cursor("generation"), {
    get() {
      proxyTrapCalls += 1;
      throw new Error("proxy get must not run");
    },
    getOwnPropertyDescriptor() {
      proxyTrapCalls += 1;
      throw new Error("proxy descriptor must not run");
    },
    getPrototypeOf() {
      proxyTrapCalls += 1;
      throw new Error("proxy prototype must not run");
    },
    ownKeys() {
      proxyTrapCalls += 1;
      throw new Error("proxy ownKeys must not run");
    },
  });
  const proxyStore = freezeRecord({
    readLane() {
      return proxiedCursor;
    },
    async advanceLane() {},
  });
  const proxyRunner = createPostgresRestoreRecoveryRunner({
    cursorStore: proxyStore,
    recoveryService: createRecoveryService().service,
    recoveryScopeId: RECOVERY_SCOPE_ID,
    limits: limits(),
  });
  await assert.rejects(
    proxyRunner.runOnce({ signal: null }),
    assertCode("postgres_restore_recovery_runner_outcome_uncertain"),
  );
  assert.equal(proxyTrapCalls, 0);

  let getterCalls = 0;
  const accessorCursor = { ...cursor("generation") };
  Object.defineProperty(accessorCursor, "revision", {
    enumerable: true,
    get() {
      getterCalls += 1;
      throw new Error("cursor accessor must not run");
    },
  });
  Object.freeze(accessorCursor);
  const accessorStore = freezeRecord({
    readLane() {
      return accessorCursor;
    },
    async advanceLane() {},
  });
  const accessorRunner = createPostgresRestoreRecoveryRunner({
    cursorStore: accessorStore,
    recoveryService: createRecoveryService().service,
    recoveryScopeId: RECOVERY_SCOPE_ID,
    limits: limits(),
  });
  await assert.rejects(
    accessorRunner.runOnce({ signal: null }),
    assertCode("postgres_restore_recovery_runner_outcome_uncertain"),
  );
  assert.equal(getterCalls, 0);
});

test("constructor and run request require exact plain data", async () => {
  const valid = createRunner();
  for (const optionValue of [
    undefined,
    {
      cursorStore: valid.cursorFixture.store,
      recoveryService: valid.serviceFixture.service,
      recoveryScopeId: RECOVERY_SCOPE_ID,
      limits: limits(),
      extra: true,
    },
    {
      cursorStore: valid.cursorFixture.store,
      recoveryService: valid.serviceFixture.service,
      recoveryScopeId: "invalid scope",
      limits: limits(),
    },
    {
      cursorStore: valid.cursorFixture.store,
      recoveryService: valid.serviceFixture.service,
      recoveryScopeId: RECOVERY_SCOPE_ID,
      limits: limits({ generation: 0 }),
    },
  ]) {
    assert.throws(
      () => createPostgresRestoreRecoveryRunner(optionValue),
      assertCode("invalid_postgres_restore_recovery_runner_options"),
    );
  }

  for (const request of [undefined, {}, { signal: null, extra: true }, { signal: {} }]) {
    await assert.rejects(
      valid.runner.runOnce(request),
      assertCode("invalid_postgres_restore_recovery_runner_request"),
    );
  }

  let signalPrototypeTrapCalls = 0;
  const hostileSignal = Object.create(
    new Proxy(Object.create(null), {
      getPrototypeOf() {
        signalPrototypeTrapCalls += 1;
        throw new Error("signal prototype trap must not run");
      },
    }),
  );
  await assert.rejects(
    valid.runner.runOnce({ signal: hostileSignal }),
    assertCode("invalid_postgres_restore_recovery_runner_request"),
  );
  assert.equal(signalPrototypeTrapCalls, 0);
});
