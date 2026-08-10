import assert from "node:assert/strict";
import test from "node:test";

import {
  createPostgresCheckpointRecoveryService,
  PostgresCheckpointRecoveryServiceError,
} from "../src/postgres-checkpoint-recovery-service.mjs";

const SESSION_ID_1 = "019f2200-0000-7000-8000-000000000001";
const SESSION_ID_2 = "019f2200-0000-7000-8000-000000000002";
const SESSION_ID_3 = "019f2200-0000-7000-8000-000000000003";
const NOW = "2026-08-02T12:00:00.000Z";
const IMAGE_DIGEST = `sha256:${"a".repeat(64)}`;

function deepFreeze(value) {
  if (value !== null && typeof value === "object" && !Object.isFrozen(value)) {
    const keys = Reflect.ownKeys(value);
    for (let index = 0; index < keys.length; index += 1) {
      const descriptor = Object.getOwnPropertyDescriptor(value, keys[index]);
      if (descriptor !== undefined && "value" in descriptor) {
        deepFreeze(descriptor.value);
      }
    }
    Object.freeze(value);
  }
  return value;
}

function candidate(
  sessionId,
  suffix = sessionId.slice(-3),
  state = "starting",
) {
  return deepFreeze({
    checkpoint: {
      artifactId: `artifact-${suffix}`,
      backendId: "stopped-directory",
      checkpointClass: "clean",
      checkpointId: `checkpoint-${suffix}`,
      codexSessionId: sessionId,
      codexThreadId: sessionId,
      contractVersion: 1,
      createdAt: NOW,
      imageDigest: IMAGE_DIGEST,
      sessionId,
      sourceFencingEpoch: "4",
      storageId: `storage-${suffix}`,
    },
    request: {
      backendId: "stopped-directory",
      contractVersion: 1,
      fencingEpoch: "4",
      holderId: "holder-001",
      leaseId: "lease-001",
      operation: "checkpoint",
      operationId: `checkpoint-capture-${suffix}`,
      sessionId,
      storageId: `storage-${suffix}`,
      target: {
        artifactId: `artifact-${suffix}`,
        checkpointId: `checkpoint-${suffix}`,
        kind: "checkpoint",
      },
    },
    state,
  });
}

function page(candidates, nextAfterSessionId = null) {
  return deepFreeze({
    candidates: [...candidates],
    nextAfterSessionId,
  });
}

function request(overrides = {}) {
  return {
    afterSessionId: null,
    limit: 100,
    signal: null,
    ...overrides,
  };
}

function exactKeys(value, keys) {
  assert.deepEqual(Reflect.ownKeys(value).sort(), [...keys].sort());
}

function assertFrozenRecord(value, keys) {
  assert.equal(Object.getPrototypeOf(value), null);
  assert.equal(Object.isFrozen(value), true);
  exactKeys(value, keys);
}

function deferred() {
  let resolve;
  let reject;
  const promise = new Promise((resolvePromise, rejectPromise) => {
    resolve = resolvePromise;
    reject = rejectPromise;
  });
  return { promise, reject, resolve };
}

function assertSynchronousServiceError(callback, code) {
  assert.throws(callback, (error) => {
    assert.ok(error instanceof PostgresCheckpointRecoveryServiceError);
    assert.equal(error.name, "PostgresCheckpointRecoveryServiceError");
    assert.equal(error.code, code);
    assert.equal(error.retryable, false);
    assert.equal(Object.isFrozen(error), true);
    assert.equal("cause" in error, false);
    return true;
  });
}

async function assertServiceError(promise, code) {
  await assert.rejects(promise, (error) => {
    assert.ok(error instanceof PostgresCheckpointRecoveryServiceError);
    assert.equal(error.name, "PostgresCheckpointRecoveryServiceError");
    assert.equal(error.code, code);
    assert.equal(error.retryable, false);
    assert.equal(Object.isFrozen(error), true);
    assert.equal("cause" in error, false);
    assert.equal("candidate" in error, false);
    assert.equal("callback" in error, false);
    return true;
  });
}

test("captures exact callbacks once and returns exact frozen receipts", async () => {
  const first = candidate(SESSION_ID_1);
  const second = candidate(SESSION_ID_2);
  const listInputs = [];
  const reconciliationInputs = [];
  let listThis = "unobserved";
  let reconcileThis = "unobserved";

  const options = {
    listCandidates(input) {
      listThis = this;
      listInputs.push(input);
      return page([first, second], SESSION_ID_2);
    },
    reconcileCheckpointCapture(input) {
      reconcileThis = this;
      reconciliationInputs.push(input);
      return undefined;
    },
  };
  const service = createPostgresCheckpointRecoveryService(options);
  options.listCandidates = () => {
    throw new Error("replacement callback must not be read");
  };
  options.reconcileCheckpointCapture = () => {
    throw new Error("replacement callback must not be read");
  };

  assertFrozenRecord(service, ["runBatch"]);
  const result = await service.runBatch(
    request({ afterSessionId: null, limit: 2 }),
  );

  assert.equal(listThis, undefined);
  assert.equal(reconcileThis, undefined);
  assert.equal(listInputs.length, 1);
  assertFrozenRecord(listInputs[0], ["afterSessionId", "limit"]);
  assert.equal(listInputs[0].afterSessionId, null);
  assert.equal(listInputs[0].limit, 2);
  assert.equal(reconciliationInputs.length, 2);
  for (let index = 0; index < reconciliationInputs.length; index += 1) {
    assertFrozenRecord(reconciliationInputs[index], ["checkpoint", "request"]);
    const source = index === 0 ? first : second;
    assert.strictEqual(reconciliationInputs[index].checkpoint, source.checkpoint);
    assert.strictEqual(reconciliationInputs[index].request, source.request);
  }

  assertFrozenRecord(result, ["nextAfterSessionId", "results", "status"]);
  assert.equal(result.nextAfterSessionId, SESSION_ID_2);
  assert.equal(result.status, "limit-reached");
  assert.equal(Object.isFrozen(result.results), true);
  assert.equal(result.results.length, 2);
  for (let index = 0; index < result.results.length; index += 1) {
    assertFrozenRecord(result.results[index], [
      "operationId",
      "sessionId",
      "status",
    ]);
    assert.equal(result.results[index].status, "reconciled");
  }
});

test("routes prepared candidates to fresh resume and active candidates to committed verification", async () => {
  const prepared = candidate(SESSION_ID_1, "001", "prepared");
  const starting = candidate(SESSION_ID_2, "002", "starting");
  const uncertain = candidate(SESSION_ID_3, "003", "uncertain");
  const preparedCalls = [];
  const reconciliationCalls = [];
  const service = createPostgresCheckpointRecoveryService({
    async listCandidates() {
      return page([prepared, starting, uncertain]);
    },
    async reconcileCheckpointCapture(input) {
      reconciliationCalls.push(input);
    },
    async resumePreparedCheckpointCapture(input) {
      preparedCalls.push(input);
    },
  });

  const result = await service.runBatch(request({ limit: 3 }));

  assert.deepEqual(
    preparedCalls.map((input) => input.request.operationId),
    [prepared.request.operationId],
  );
  assert.deepEqual(
    reconciliationCalls.map((input) => input.request.operationId),
    [starting.request.operationId, uncertain.request.operationId],
  );
  for (const input of [...preparedCalls, ...reconciliationCalls]) {
    assertFrozenRecord(input, ["checkpoint", "request"]);
    assert.equal(Object.hasOwn(input, "state"), false);
  }
  assert.deepEqual(
    result.results.map((item) => item.status),
    ["reconciled", "reconciled", "reconciled"],
  );
});

test("leaves a prepared candidate pending when the optional resume extension is absent", async () => {
  const prepared = candidate(SESSION_ID_1, "001", "prepared");
  let reconciliationCalls = 0;
  const service = createPostgresCheckpointRecoveryService({
    async listCandidates() {
      return page([prepared]);
    },
    async reconcileCheckpointCapture() {
      reconciliationCalls += 1;
    },
  });

  const result = await service.runBatch(request({ limit: 1 }));

  assert.equal(reconciliationCalls, 0);
  assert.equal(result.results[0].status, "pending");
});

test("uses a settled session cursor across two bounded pages", async () => {
  const first = candidate(SESSION_ID_1);
  const second = candidate(SESSION_ID_2);
  const third = candidate(SESSION_ID_3);
  const observedCursors = [];
  const service = createPostgresCheckpointRecoveryService({
    async listCandidates(input) {
      observedCursors.push(input.afterSessionId);
      if (input.afterSessionId === null) {
        return page([first, second], SESSION_ID_2);
      }
      assert.equal(input.afterSessionId, SESSION_ID_2);
      return page([third]);
    },
    async reconcileCheckpointCapture() {},
  });

  const firstBatch = await service.runBatch(
    request({ afterSessionId: null, limit: 2 }),
  );
  const secondBatch = await service.runBatch(
    request({
      afterSessionId: firstBatch.nextAfterSessionId,
      limit: 2,
    }),
  );

  assert.deepEqual(observedCursors, [null, SESSION_ID_2]);
  assert.equal(firstBatch.status, "limit-reached");
  assert.equal(firstBatch.nextAfterSessionId, SESSION_ID_2);
  assert.equal(secondBatch.status, "sweep-complete");
  assert.equal(secondBatch.nextAfterSessionId, null);
  assert.deepEqual(
    secondBatch.results.map((item) => item.sessionId),
    [SESSION_ID_3],
  );
});

test("accepts an exactly full terminal page without an extra row", async () => {
  const first = candidate(SESSION_ID_1);
  const second = candidate(SESSION_ID_2);
  const service = createPostgresCheckpointRecoveryService({
    async listCandidates() {
      return page([first, second]);
    },
    async reconcileCheckpointCapture() {},
  });

  const result = await service.runBatch(request({ limit: 2 }));

  assert.equal(result.status, "sweep-complete");
  assert.equal(result.nextAfterSessionId, null);
  assert.equal(result.results.length, 2);
});

test("marks rejected candidates pending and continues sequentially", async () => {
  const candidates = [
    candidate(SESSION_ID_1),
    candidate(SESSION_ID_2),
    candidate(SESSION_ID_3),
  ];
  const calls = [];
  const service = createPostgresCheckpointRecoveryService({
    async listCandidates() {
      return page(candidates);
    },
    async reconcileCheckpointCapture(value) {
      calls.push(value.request.sessionId);
      if (value.request.sessionId === SESSION_ID_2) {
        throw new Error("private backend uncertainty");
      }
    },
  });

  const result = await service.runBatch(request({ limit: 3 }));

  assert.deepEqual(calls, [SESSION_ID_1, SESSION_ID_2, SESSION_ID_3]);
  assert.deepEqual(
    result.results.map((item) => item.status),
    ["reconciled", "pending", "reconciled"],
  );
  assert.equal(result.status, "sweep-complete");
  assert.equal(JSON.stringify(result).includes("private"), false);
});

test("maps bound generator callback results to pending without executing bodies", async () => {
  const value = candidate(SESSION_ID_1);
  let generatorBodyCalls = 0;
  let asyncGeneratorBodyCalls = 0;
  let thenCalls = 0;
  function* reconcileGenerator() {
    generatorBodyCalls += 1;
    yield "must not run";
  }
  async function* reconcileAsyncGenerator() {
    asyncGeneratorBodyCalls += 1;
    yield "must not run";
  }
  const callbacks = [
    reconcileGenerator.bind(undefined),
    reconcileAsyncGenerator.bind(undefined),
  ];
  const generatorPrototypes = [
    reconcileGenerator.prototype,
    reconcileAsyncGenerator.prototype,
  ];

  try {
    for (let index = 0; index < generatorPrototypes.length; index += 1) {
      Object.defineProperty(generatorPrototypes[index], "then", {
        configurable: true,
        value(resolve) {
          thenCalls += 1;
          resolve("must not be assimilated");
        },
      });
    }
    for (let index = 0; index < callbacks.length; index += 1) {
      const service = createPostgresCheckpointRecoveryService({
        async listCandidates() {
          return page([value]);
        },
        reconcileCheckpointCapture: callbacks[index],
      });
      const result = await service.runBatch(request({ limit: 1 }));
      assert.equal(result.results[0].status, "pending");
    }
  } finally {
    for (let index = 0; index < generatorPrototypes.length; index += 1) {
      delete generatorPrototypes[index].then;
    }
  }

  assert.equal(thenCalls, 0);
  assert.equal(generatorBodyCalls, 0);
  assert.equal(asyncGeneratorBodyCalls, 0);
});

test("never starts a second candidate while the first is in flight", async () => {
  const first = candidate(SESSION_ID_1);
  const second = candidate(SESSION_ID_2);
  const firstStarted = deferred();
  const firstSettles = deferred();
  const calls = [];
  let active = 0;
  let maximumActive = 0;
  const service = createPostgresCheckpointRecoveryService({
    async listCandidates() {
      return page([first, second]);
    },
    async reconcileCheckpointCapture(value) {
      calls.push(value.request.sessionId);
      active += 1;
      maximumActive = Math.max(maximumActive, active);
      if (value.request.sessionId === SESSION_ID_1) {
        firstStarted.resolve();
        await firstSettles.promise;
      }
      active -= 1;
    },
  });

  const running = service.runBatch(request({ limit: 2 }));
  await firstStarted.promise;
  assert.deepEqual(calls, [SESSION_ID_1]);
  assert.equal(active, 1);
  firstSettles.resolve();
  const result = await running;

  assert.deepEqual(calls, [SESSION_ID_1, SESSION_ID_2]);
  assert.equal(maximumActive, 1);
  assert.equal(result.results.length, 2);
});

test("allows only one service batch in flight and releases the guard", async () => {
  const value = candidate(SESSION_ID_1);
  const enumerationStarted = deferred();
  const firstEnumerationSettles = deferred();
  let listCalls = 0;
  let reconcileCalls = 0;
  const service = createPostgresCheckpointRecoveryService({
    async listCandidates() {
      listCalls += 1;
      if (listCalls === 1) {
        enumerationStarted.resolve();
        await firstEnumerationSettles.promise;
      }
      return page([value]);
    },
    async reconcileCheckpointCapture() {
      reconcileCalls += 1;
    },
  });

  const first = service.runBatch(request({ limit: 1 }));
  await enumerationStarted.promise;
  await assertServiceError(
    service.runBatch(request({ limit: 1 })),
    "postgres_checkpoint_recovery_service_outcome_uncertain",
  );
  await assertServiceError(
    service.runBatch(request({ limit: 0 })),
    "invalid_postgres_checkpoint_recovery_service_request",
  );
  assert.equal(listCalls, 1);
  assert.equal(reconcileCalls, 0);

  firstEnumerationSettles.resolve();
  const firstResult = await first;
  const laterResult = await service.runBatch(request({ limit: 1 }));

  assert.equal(firstResult.results[0].status, "reconciled");
  assert.equal(laterResult.results[0].status, "reconciled");
  assert.equal(listCalls, 2);
  assert.equal(reconcileCalls, 2);
});

test("returns aborted before enumeration without calling collaborators", async () => {
  const controller = new AbortController();
  controller.abort();
  let listCalls = 0;
  let reconcileCalls = 0;
  const service = createPostgresCheckpointRecoveryService({
    async listCandidates() {
      listCalls += 1;
      return page([]);
    },
    async reconcileCheckpointCapture() {
      reconcileCalls += 1;
    },
  });

  const result = await service.runBatch(
    request({
      afterSessionId: SESSION_ID_1,
      limit: 2,
      signal: controller.signal,
    }),
  );

  assert.equal(listCalls, 0);
  assert.equal(reconcileCalls, 0);
  assert.equal(result.status, "aborted");
  assert.equal(result.nextAfterSessionId, SESSION_ID_1);
  assert.deepEqual(result.results, []);
});

test("observes abort after enumeration and validates the page first", async () => {
  const controller = new AbortController();
  const enumerationStarted = deferred();
  const enumerationSettles = deferred();
  let reconcileCalls = 0;
  const service = createPostgresCheckpointRecoveryService({
    async listCandidates() {
      enumerationStarted.resolve();
      return enumerationSettles.promise;
    },
    async reconcileCheckpointCapture() {
      reconcileCalls += 1;
    },
  });

  const running = service.runBatch(
    request({ limit: 1, signal: controller.signal }),
  );
  await enumerationStarted.promise;
  controller.abort();
  enumerationSettles.resolve(page([candidate(SESSION_ID_1)]));
  const result = await running;

  assert.equal(reconcileCalls, 0);
  assert.equal(result.status, "aborted");
  assert.equal(result.nextAfterSessionId, null);
  assert.deepEqual(result.results, []);
});

test("malformed enumeration remains fail-closed when aborted in flight", async () => {
  const controller = new AbortController();
  const enumerationStarted = deferred();
  const enumerationSettles = deferred();
  let reconcileCalls = 0;
  const service = createPostgresCheckpointRecoveryService({
    async listCandidates() {
      enumerationStarted.resolve();
      return enumerationSettles.promise;
    },
    async reconcileCheckpointCapture() {
      reconcileCalls += 1;
    },
  });

  const running = service.runBatch(
    request({ limit: 1, signal: controller.signal }),
  );
  await enumerationStarted.promise;
  controller.abort();
  enumerationSettles.resolve({
    candidates: Object.freeze([]),
    nextAfterSessionId: null,
  });

  await assertServiceError(
    running,
    "postgres_checkpoint_recovery_service_outcome_uncertain",
  );
  assert.equal(reconcileCalls, 0);
});

test("waits for an in-flight candidate before returning an abort", async () => {
  const controller = new AbortController();
  const first = candidate(SESSION_ID_1);
  const second = candidate(SESSION_ID_2);
  const reconciliationStarted = deferred();
  const reconciliationSettles = deferred();
  const calls = [];
  const service = createPostgresCheckpointRecoveryService({
    async listCandidates() {
      return page([first, second]);
    },
    async reconcileCheckpointCapture(value) {
      calls.push(value);
      if (value.request.sessionId === SESSION_ID_1) {
        reconciliationStarted.resolve();
        await reconciliationSettles.promise;
      }
    },
  });

  const running = service.runBatch(
    request({ limit: 2, signal: controller.signal }),
  );
  await reconciliationStarted.promise;
  controller.abort();
  let settled = false;
  running.then(() => {
    settled = true;
  });
  await Promise.resolve();
  assert.equal(settled, false);
  reconciliationSettles.resolve();
  const result = await running;

  assert.deepEqual(
    calls.map((input) => input.request.sessionId),
    [SESSION_ID_1],
  );
  assert.equal(result.status, "aborted");
  assert.equal(result.nextAfterSessionId, SESSION_ID_1);
  assert.equal(result.results.length, 1);
  assert.equal(result.results[0].status, "reconciled");
});

test("rejects invalid requests before either collaborator", async () => {
  let listCalls = 0;
  let reconcileCalls = 0;
  const service = createPostgresCheckpointRecoveryService({
    async listCandidates() {
      listCalls += 1;
      return page([]);
    },
    async reconcileCheckpointCapture() {
      reconcileCalls += 1;
    },
  });
  const invalidRequests = [
    undefined,
    {},
    request({ afterSessionId: "019F2200-0000-7000-8000-000000000001" }),
    request({ limit: 0 }),
    request({ limit: 101 }),
    request({ limit: 1.5 }),
    request({ signal: { aborted: false } }),
    { ...request(), extra: true },
  ];

  for (let index = 0; index < invalidRequests.length; index += 1) {
    await assertServiceError(
      service.runBatch(invalidRequests[index]),
      "invalid_postgres_checkpoint_recovery_service_request",
    );
  }
  await assertServiceError(
    service.runBatch(),
    "invalid_postgres_checkpoint_recovery_service_request",
  );
  assert.equal(listCalls, 0);
  assert.equal(reconcileCalls, 0);
});

test("rejects untrusted option shapes and generator callbacks", () => {
  const valid = {
    async listCandidates() {
      return page([]);
    },
    async reconcileCheckpointCapture() {},
  };
  const code = "invalid_postgres_checkpoint_recovery_service_options";

  assertSynchronousServiceError(
    () => createPostgresCheckpointRecoveryService(),
    code,
  );
  assertSynchronousServiceError(
    () =>
      createPostgresCheckpointRecoveryService({ ...valid, extra: true }),
    code,
  );
  assertSynchronousServiceError(
    () =>
      createPostgresCheckpointRecoveryService({
        ...valid,
        listCandidates: new Proxy(valid.listCandidates, {}),
      }),
    code,
  );
  assertSynchronousServiceError(
    () =>
      createPostgresCheckpointRecoveryService({
        ...valid,
        reconcileCheckpointCapture: function* reconcile() {},
      }),
    code,
  );
  const accessor = {
    reconcileCheckpointCapture: valid.reconcileCheckpointCapture,
  };
  Object.defineProperty(accessor, "listCandidates", {
    enumerable: true,
    get() {
      return valid.listCandidates;
    },
  });
  assertSynchronousServiceError(
    () => createPostgresCheckpointRecoveryService(accessor),
    code,
  );
});

test("sanitizes revoked proxies at options, request, and page boundaries", async () => {
  const optionRevocable = Proxy.revocable({}, {});
  optionRevocable.revoke();
  assertSynchronousServiceError(
    () =>
      createPostgresCheckpointRecoveryService(optionRevocable.proxy),
    "invalid_postgres_checkpoint_recovery_service_options",
  );

  let listCalls = 0;
  let reconcileCalls = 0;
  const validService = createPostgresCheckpointRecoveryService({
    async listCandidates() {
      listCalls += 1;
      return page([]);
    },
    async reconcileCheckpointCapture() {
      reconcileCalls += 1;
    },
  });
  const requestRevocable = Proxy.revocable(request(), {});
  requestRevocable.revoke();
  await assertServiceError(
    validService.runBatch(requestRevocable.proxy),
    "invalid_postgres_checkpoint_recovery_service_request",
  );
  assert.equal(listCalls, 0);

  const topPageRevocable = Proxy.revocable(page([]), {});
  topPageRevocable.revoke();
  const arrayRevocable = Proxy.revocable([], {});
  const arrayPage = Object.freeze({
    candidates: arrayRevocable.proxy,
    nextAfterSessionId: null,
  });
  arrayRevocable.revoke();
  const candidateRevocable = Proxy.revocable(candidate(SESSION_ID_1), {});
  const candidatePage = Object.freeze({
    candidates: Object.freeze([candidateRevocable.proxy]),
    nextAfterSessionId: null,
  });
  candidateRevocable.revoke();
  const hostilePages = [
    topPageRevocable.proxy,
    arrayPage,
    candidatePage,
  ];

  for (let index = 0; index < hostilePages.length; index += 1) {
    const service = createPostgresCheckpointRecoveryService({
      listCandidates() {
        return hostilePages[index];
      },
      async reconcileCheckpointCapture() {
        reconcileCalls += 1;
      },
    });
    await assertServiceError(
      service.runBatch(request({ limit: 1 })),
      "postgres_checkpoint_recovery_service_outcome_uncertain",
    );
  }
  assert.equal(reconcileCalls, 0);
});

test("sanitizes enumeration rejection without exposing its cause", async () => {
  const secret = new Error("postgresql://private-user:secret@host/db");
  let listCalls = 0;
  const service = createPostgresCheckpointRecoveryService({
    async listCandidates() {
      listCalls += 1;
      throw secret;
    },
    async reconcileCheckpointCapture() {
      throw new Error("must not run");
    },
  });

  let observed;
  try {
    await service.runBatch(request({ limit: 1 }));
  } catch (error) {
    observed = error;
  }
  assert.ok(observed instanceof PostgresCheckpointRecoveryServiceError);
  assert.equal(
    observed.code,
    "postgres_checkpoint_recovery_service_outcome_uncertain",
  );
  assert.equal(Object.isFrozen(observed), true);
  assert.equal("cause" in observed, false);
  assert.equal(observed.stack.includes("private-user"), false);
  assert.equal(observed.message.includes("secret"), false);
  assert.equal(listCalls, 1);
});

test("validates the complete page before reconciling any candidate", async () => {
  const valid = candidate(SESSION_ID_1);
  const malformed = deepFreeze({
    checkpoint: candidate(SESSION_ID_2).checkpoint,
  });
  let reconcileCalls = 0;
  const service = createPostgresCheckpointRecoveryService({
    async listCandidates() {
      return page([valid, malformed]);
    },
    async reconcileCheckpointCapture() {
      reconcileCalls += 1;
    },
  });

  await assertServiceError(
    service.runBatch(request({ limit: 2 })),
    "postgres_checkpoint_recovery_service_outcome_uncertain",
  );
  assert.equal(reconcileCalls, 0);
});

test("fails closed on hostile, unordered, and inconsistent pages", async () => {
  const first = candidate(SESSION_ID_1);
  const second = candidate(SESSION_ID_2);
  const sparse = new Array(2);
  sparse[0] = first;
  Object.freeze(sparse);
  const accessorCandidate = { checkpoint: first.checkpoint };
  Object.defineProperty(accessorCandidate, "request", {
    enumerable: true,
    get() {
      throw new Error("must not be read");
    },
  });
  Object.freeze(accessorCandidate);
  const mutablePage = {
    candidates: Object.freeze([]),
    nextAfterSessionId: null,
  };
  const hostilePages = [
    mutablePage,
    Object.freeze({ candidates: [], nextAfterSessionId: null }),
    page([first], SESSION_ID_1),
    page([first, second], SESSION_ID_1),
    page([second, first]),
    deepFreeze({ candidates: sparse, nextAfterSessionId: null }),
    page([accessorCandidate]),
    new Proxy(page([first]), {}),
  ];

  for (let index = 0; index < hostilePages.length; index += 1) {
    let reconcileCalls = 0;
    const service = createPostgresCheckpointRecoveryService({
      async listCandidates() {
        return hostilePages[index];
      },
      async reconcileCheckpointCapture() {
        reconcileCalls += 1;
      },
    });
    await assertServiceError(
      service.runBatch(request({ limit: 2 })),
      "postgres_checkpoint_recovery_service_outcome_uncertain",
    );
    assert.equal(reconcileCalls, 0);
  }
});

test("enforces the hard one-hundred-candidate page limit", async () => {
  const candidates = [];
  for (let index = 1; index <= 101; index += 1) {
    const suffix = index.toString(16).padStart(12, "0");
    candidates.push(
      candidate(`019f2200-0000-7000-8000-${suffix}`, suffix),
    );
  }
  let reconcileCalls = 0;
  const service = createPostgresCheckpointRecoveryService({
    async listCandidates() {
      return page(candidates);
    },
    async reconcileCheckpointCapture() {
      reconcileCalls += 1;
    },
  });

  await assertServiceError(
    service.runBatch(request({ limit: 100 })),
    "postgres_checkpoint_recovery_service_outcome_uncertain",
  );
  assert.equal(reconcileCalls, 0);
});

test("uses captured intrinsics instead of mutable iterator and promise helpers", async () => {
  const value = candidate(SESSION_ID_1);
  const expectedPage = page([value]);
  const controller = new AbortController();
  const service = createPostgresCheckpointRecoveryService({
    async listCandidates() {
      return expectedPage;
    },
    async reconcileCheckpointCapture(input) {
      assert.equal(Object.getPrototypeOf(input), null);
      assert.equal(Object.isFrozen(input), true);
      const keys = Reflect.ownKeys(input);
      assert.equal(keys.length, 2);
      assert.equal(keys[0], "checkpoint");
      assert.equal(keys[1], "request");
      assert.strictEqual(input.checkpoint, value.checkpoint);
      assert.strictEqual(input.request, value.request);
    },
  });
  const arrayIteratorDescriptor = Object.getOwnPropertyDescriptor(
    Array.prototype,
    Symbol.iterator,
  );
  const arrayPushDescriptor = Object.getOwnPropertyDescriptor(
    Array.prototype,
    "push",
  );
  const promiseResolveDescriptor = Object.getOwnPropertyDescriptor(
    Promise,
    "resolve",
  );
  const regexpExecDescriptor = Object.getOwnPropertyDescriptor(
    RegExp.prototype,
    "exec",
  );
  const abortedDescriptor = Object.getOwnPropertyDescriptor(
    AbortSignal.prototype,
    "aborted",
  );
  let result;
  let observedError;

  try {
    Object.defineProperty(Array.prototype, Symbol.iterator, {
      ...arrayIteratorDescriptor,
      value: function* emptyIterator() {},
    });
    Object.defineProperty(Array.prototype, "push", {
      ...arrayPushDescriptor,
      value() {
        throw new Error("poisoned Array.prototype.push");
      },
    });
    Object.defineProperty(Promise, "resolve", {
      ...promiseResolveDescriptor,
      value() {
        throw new Error("poisoned Promise.resolve");
      },
    });
    Object.defineProperty(RegExp.prototype, "exec", {
      ...regexpExecDescriptor,
      value() {
        throw new Error("poisoned RegExp.prototype.exec");
      },
    });
    Object.defineProperty(AbortSignal.prototype, "aborted", {
      ...abortedDescriptor,
      get() {
        throw new Error("poisoned AbortSignal.prototype.aborted");
      },
    });
    try {
      result = await service.runBatch(
        request({ limit: 1, signal: controller.signal }),
      );
    } catch (error) {
      observedError = error;
    }
  } finally {
    Object.defineProperty(
      Array.prototype,
      Symbol.iterator,
      arrayIteratorDescriptor,
    );
    Object.defineProperty(Array.prototype, "push", arrayPushDescriptor);
    Object.defineProperty(Promise, "resolve", promiseResolveDescriptor);
    Object.defineProperty(RegExp.prototype, "exec", regexpExecDescriptor);
    Object.defineProperty(
      AbortSignal.prototype,
      "aborted",
      abortedDescriptor,
    );
  }

  assert.equal(observedError, undefined);
  assert.equal(result.status, "sweep-complete");
  assert.equal(result.results[0].status, "reconciled");
});
