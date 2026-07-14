import assert from "node:assert/strict";
import { syncBuiltinESMExports } from "node:module";
import path from "node:path";
import test from "node:test";
import { types as utilTypes } from "node:util";
import { runInNewContext } from "node:vm";

import {
  STOPPED_WRITER_STOP_CONFIRMED,
  StoppedWriterCapabilityCoordinator,
  StoppedWriterCapabilityError,
} from "../src/stopped-writer-capability.mjs";

const SESSION_ID = "019f2100-0000-7000-8000-000000000001";
const OTHER_SESSION_ID = "019f2100-0000-7000-8000-000000000099";
const BACKEND_ID = "single-attach-test";
const STORAGE_ID = "volume-001";
const PROCESS_INCARNATION_ID = "process-incarnation-001";
const WRITER_INCARNATION_ID = "writer-incarnation-001";
const STOP_OPERATION_ID = "stop-operation-001";
const LARGE_EPOCH = "9007199254740993";

const ERROR_CODES = new Set([
  "invalid_stopped_writer_request",
  "snapshot_outcome_uncertain",
  "stopped_writer_capability_rejected",
  "writer_state_conflict",
  "writer_stop_outcome_uncertain",
]);

function lease(overrides = {}) {
  return {
    contractVersion: 1,
    sessionId: SESSION_ID,
    leaseId: "lease-001",
    holderId: "host-001",
    fencingEpoch: LARGE_EPOCH,
    expiresAt: "2026-07-14T12:01:00.000Z",
    ...overrides,
  };
}

function attachment(writerLease = lease(), overrides = {}) {
  return {
    contractVersion: 1,
    backendId: BACKEND_ID,
    storageId: STORAGE_ID,
    sessionId: writerLease.sessionId,
    attachmentId: "attachment-001",
    leaseId: writerLease.leaseId,
    holderId: writerLease.holderId,
    fencingEpoch: writerLease.fencingEpoch,
    operationId: "operation-attach-001",
    proofId: "proof-attachment-001",
    kind: "directory",
    rootPath: "/var/lib/portable-codex/session-001",
    mode: "read-write",
    ...overrides,
  };
}

function registerOptions({
  attachment: mounted = attachment(),
  canonicalLease = lease(),
  processIncarnationId = PROCESS_INCARNATION_ID,
  stopWriter = async () => STOPPED_WRITER_STOP_CONFIRMED,
  writerIncarnationId = WRITER_INCARNATION_ID,
} = {}) {
  return {
    attachment: mounted,
    canonicalLease,
    processIncarnationId,
    stopWriter,
    writerIncarnationId,
  };
}

function stopOptions(writer, overrides = {}) {
  return {
    processIncarnationId: PROCESS_INCARNATION_ID,
    stopOperationId: STOP_OPERATION_ID,
    writer,
    writerIncarnationId: WRITER_INCARNATION_ID,
    ...overrides,
  };
}

function consumeOptions(writer, capability, overrides = {}) {
  return {
    attachment: attachment(),
    canonicalLease: lease(),
    capability,
    processIncarnationId: PROCESS_INCARNATION_ID,
    runSnapshot: async () => undefined,
    stopOperationId: STOP_OPERATION_ID,
    writer,
    writerIncarnationId: WRITER_INCARNATION_ID,
    ...overrides,
  };
}

function writerOptions(writer, overrides = {}) {
  return {
    processIncarnationId: PROCESS_INCARNATION_ID,
    writer,
    writerIncarnationId: WRITER_INCARNATION_ID,
    ...overrides,
  };
}

function deferred() {
  let reject;
  let resolve;
  const promise = new Promise((resolvePromise, rejectPromise) => {
    reject = rejectPromise;
    resolve = resolvePromise;
  });
  return { promise, reject, resolve };
}

function assertCapabilityError(error, expectedCode, forbidden = []) {
  assert(error instanceof StoppedWriterCapabilityError);
  assert.equal(error.name, "StoppedWriterCapabilityError");
  assert.equal(error.code, expectedCode);
  assert(ERROR_CODES.has(error.code));
  assert.equal(error.retryable, false);
  assert.equal(Object.isFrozen(error), true);
  assert.equal(Object.hasOwn(error, "cause"), false);
  assert.equal(Object.hasOwn(error, "details"), false);
  assert.deepEqual(Reflect.ownKeys(error).sort(), [
    "code",
    "message",
    "name",
    "retryable",
    "stack",
  ]);
  assert.equal(error.stack, `${error.name}: ${error.message}`);
  const publicSurface = Reflect.ownKeys(error)
    .map((key) => String(error[key]))
    .join("\n");
  for (const value of forbidden) {
    assert.equal(
      publicSurface.includes(value),
      false,
      `public error leaked ${value}`,
    );
  }
  return true;
}

function syncCapabilityError(operation, expectedCode, forbidden = []) {
  assert.throws(
    operation,
    (error) => assertCapabilityError(error, expectedCode, forbidden),
  );
}

async function rejectedCapabilityError(operation, expectedCode, forbidden = []) {
  await assert.rejects(
    operation,
    (error) => assertCapabilityError(error, expectedCode, forbidden),
  );
}

function assertOpaqueHandle(value) {
  assert.notEqual(value, null);
  assert.equal(typeof value, "object");
  assert.equal(Object.isFrozen(value), true);
  assert.deepEqual(Reflect.ownKeys(value), []);
  assert.equal(JSON.stringify(value), "{}");
}

async function readyCapability({
  coordinator = new StoppedWriterCapabilityCoordinator(),
  mounted = attachment(),
  processIncarnationId = PROCESS_INCARNATION_ID,
  stopOperationId = STOP_OPERATION_ID,
  stopWriter = async () => STOPPED_WRITER_STOP_CONFIRMED,
  writerIncarnationId = WRITER_INCARNATION_ID,
  writerLease = lease(),
} = {}) {
  const writer = coordinator.registerWriter({
    attachment: mounted,
    canonicalLease: writerLease,
    processIncarnationId,
    stopWriter,
    writerIncarnationId,
  });
  const capability = await coordinator.stopAndIssueCapability({
    processIncarnationId,
    stopOperationId,
    writer,
    writerIncarnationId,
  });
  return {
    attachment: mounted,
    canonicalLease: writerLease,
    capability,
    coordinator,
    processIncarnationId,
    stopOperationId,
    writer,
    writerIncarnationId,
  };
}

test("error constructor rejects hostile codes without coercion", () => {
  const secret = "hostile-code-secret";
  let traps = 0;
  const proxy = new Proxy(
    {},
    {
      get() {
        traps += 1;
        throw new Error(secret);
      },
    },
  );
  const revoked = Proxy.revocable({}, {});
  revoked.revoke();

  for (const code of [proxy, revoked.proxy, {}, Symbol("invalid-code")]) {
    assert.throws(
      () => new StoppedWriterCapabilityError(code),
      (error) => {
        assert(error instanceof TypeError);
        assert.equal(
          error.message,
          "unsupported stopped-writer capability error code",
        );
        assert.equal(String(error).includes(secret), false);
        return true;
      },
    );
  }
  assert.equal(traps, 0);
});

test("stops one exact writer and returns a one-use opaque capability", async () => {
  const coordinator = new StoppedWriterCapabilityCoordinator();
  let stopCalls = 0;
  let snapshotCalls = 0;
  const writer = coordinator.registerWriter(
    registerOptions({
      stopWriter: async () => {
        stopCalls += 1;
        return STOPPED_WRITER_STOP_CONFIRMED;
      },
    }),
  );
  assertOpaqueHandle(writer);

  const capability = await coordinator.stopAndIssueCapability(stopOptions(writer));
  assertOpaqueHandle(capability);
  assert.notStrictEqual(capability, writer);
  assert.equal(stopCalls, 1);

  const expectedResult = Object.freeze({ status: "checkpoint-created" });
  const result = await coordinator.consumeCapability(
    consumeOptions(writer, capability, {
      canonicalLease: lease({ expiresAt: "2026-07-14T12:02:00.000Z" }),
      runSnapshot: async () => {
        snapshotCalls += 1;
        return expectedResult;
      },
    }),
  );
  assert.strictEqual(result, expectedResult);
  assert.equal(snapshotCalls, 1);

  await rejectedCapabilityError(
    () => coordinator.consumeCapability(consumeOptions(writer, capability)),
    "stopped_writer_capability_rejected",
  );
  await rejectedCapabilityError(
    () => coordinator.stopAndIssueCapability(stopOptions(writer)),
    "writer_state_conflict",
  );
  assert.equal(stopCalls, 1);
});

test("lease renewal may change only expiresAt without invalidating the binding", async () => {
  const originalLease = lease();
  const mounted = attachment(originalLease);
  const fixture = await readyCapability({ mounted, writerLease: originalLease });
  let snapshotCalls = 0;

  await fixture.coordinator.consumeCapability({
    attachment: mounted,
    canonicalLease: lease({ expiresAt: "2026-07-14T12:05:00.000Z" }),
    capability: fixture.capability,
    processIncarnationId: fixture.processIncarnationId,
    runSnapshot: async () => {
      snapshotCalls += 1;
    },
    stopOperationId: fixture.stopOperationId,
    writer: fixture.writer,
    writerIncarnationId: fixture.writerIncarnationId,
  });
  assert.equal(snapshotCalls, 1);
});

test("concurrent stop attempts run the stop callback once and issue one capability", async () => {
  const coordinator = new StoppedWriterCapabilityCoordinator();
  const releaseStop = deferred();
  const stopStarted = deferred();
  let stopCalls = 0;
  const writer = coordinator.registerWriter(
    registerOptions({
      stopWriter: async () => {
        stopCalls += 1;
        stopStarted.resolve();
        await releaseStop.promise;
        return STOPPED_WRITER_STOP_CONFIRMED;
      },
    }),
  );

  const first = coordinator.stopAndIssueCapability(stopOptions(writer));
  await stopStarted.promise;
  const second = coordinator.stopAndIssueCapability(stopOptions(writer));
  releaseStop.resolve();
  const outcomes = await Promise.allSettled([first, second]);

  assert.equal(stopCalls, 1);
  assert.equal(outcomes.filter(({ status }) => status === "fulfilled").length, 1);
  const rejected = outcomes.find(({ status }) => status === "rejected");
  assertCapabilityError(rejected.reason, "writer_state_conflict");
  assertOpaqueHandle(outcomes.find(({ status }) => status === "fulfilled").value);
});

test("concurrent capability consumption runs one snapshot callback", async () => {
  const fixture = await readyCapability();
  const releaseSnapshot = deferred();
  const snapshotStarted = deferred();
  let snapshotCalls = 0;
  const options = consumeOptions(fixture.writer, fixture.capability, {
    runSnapshot: async () => {
      snapshotCalls += 1;
      snapshotStarted.resolve();
      await releaseSnapshot.promise;
      return "captured";
    },
  });

  const first = fixture.coordinator.consumeCapability(options);
  await snapshotStarted.promise;
  const second = fixture.coordinator.consumeCapability(options);
  releaseSnapshot.resolve();
  const outcomes = await Promise.allSettled([first, second]);

  assert.equal(snapshotCalls, 1);
  assert.deepEqual(
    outcomes.map(({ status }) => status).sort(),
    ["fulfilled", "rejected"],
  );
  assert.equal(outcomes.find(({ status }) => status === "fulfilled").value, "captured");
  assertCapabilityError(
    outcomes.find(({ status }) => status === "rejected").reason,
    "stopped_writer_capability_rejected",
  );
});

test("stop callback failure is sanitized and terminal", async () => {
  const coordinator = new StoppedWriterCapabilityCoordinator();
  const secret = "stop-secret-do-not-leak";
  const privatePath = "/company/private/session";
  let stopCalls = 0;
  const writer = coordinator.registerWriter(
    registerOptions({
      stopWriter: async () => {
        stopCalls += 1;
        throw new Error(`${secret} at ${privatePath}`);
      },
    }),
  );

  await rejectedCapabilityError(
    () => coordinator.stopAndIssueCapability(stopOptions(writer)),
    "writer_stop_outcome_uncertain",
    [secret, privatePath, PROCESS_INCARNATION_ID, WRITER_INCARNATION_ID],
  );
  await rejectedCapabilityError(
    () => coordinator.stopAndIssueCapability(stopOptions(writer)),
    "writer_state_conflict",
    [secret, privatePath],
  );
  assert.equal(stopCalls, 1);
});

test("stop callback requires the exact confirmation sentinel", async (t) => {
  let generatorBodyCalls = 0;
  for (const [name, stopWriter] of [
    ["implicit return", async () => undefined],
    ["foreign object", async () => Object.freeze(Object.create(null))],
    [
      "bound generator",
      (function* stopWriter() {
        generatorBodyCalls += 1;
        return STOPPED_WRITER_STOP_CONFIRMED;
      }).bind(null),
    ],
    [
      "bound async generator",
      (async function* stopWriter() {
        generatorBodyCalls += 1;
        return STOPPED_WRITER_STOP_CONFIRMED;
      }).bind(null),
    ],
  ]) {
    await t.test(name, async () => {
      const coordinator = new StoppedWriterCapabilityCoordinator();
      const writer = coordinator.registerWriter(registerOptions({ stopWriter }));
      await rejectedCapabilityError(
        () => coordinator.stopAndIssueCapability(stopOptions(writer)),
        "writer_stop_outcome_uncertain",
      );
      await rejectedCapabilityError(
        () => coordinator.stopAndIssueCapability(stopOptions(writer)),
        "writer_state_conflict",
      );
    });
  }
  assert.equal(generatorBodyCalls, 0);
});

test("stop callback direct thenables are rejected before coordinator assimilation", async (t) => {
  const scenarios = [
    {
      name: "own callable then",
      create(hooks) {
        return {
          then(resolve) {
            hooks.thenCalls += 1;
            resolve(STOPPED_WRITER_STOP_CONFIRMED);
          },
        };
      },
    },
    {
      name: "inherited callable then",
      create(hooks) {
        const owner = Object.defineProperty({}, "then", {
          value(resolve) {
            hooks.thenCalls += 1;
            resolve(STOPPED_WRITER_STOP_CONFIRMED);
          },
        });
        return Object.create(owner);
      },
    },
    {
      name: "own then accessor",
      create(hooks) {
        return Object.defineProperty({}, "then", {
          get() {
            hooks.thenReads += 1;
            return (resolve) => {
              hooks.thenCalls += 1;
              resolve(STOPPED_WRITER_STOP_CONFIRMED);
            };
          },
        });
      },
    },
    {
      name: "inherited then accessor",
      create(hooks) {
        const owner = Object.defineProperty({}, "then", {
          get() {
            hooks.thenReads += 1;
            return (resolve) => {
              hooks.thenCalls += 1;
              resolve(STOPPED_WRITER_STOP_CONFIRMED);
            };
          },
        });
        return Object.create(owner);
      },
    },
  ];

  for (const scenario of scenarios) {
    await t.test(scenario.name, async () => {
      const coordinator = new StoppedWriterCapabilityCoordinator();
      const hooks = { thenCalls: 0, thenReads: 0 };
      let stopCalls = 0;
      const writer = coordinator.registerWriter(
        registerOptions({
          stopWriter: () => {
            stopCalls += 1;
            return scenario.create(hooks);
          },
        }),
      );

      await rejectedCapabilityError(
        () => coordinator.stopAndIssueCapability(stopOptions(writer)),
        "writer_stop_outcome_uncertain",
      );
      assert.equal(stopCalls, 1);
      assert.equal(hooks.thenReads, 0);
      assert.equal(hooks.thenCalls, 0);
      await rejectedCapabilityError(
        () => coordinator.stopAndIssueCapability(stopOptions(writer)),
        "writer_state_conflict",
      );
      assert.equal(stopCalls, 1);
      syncCapabilityError(
        () => coordinator.retireWriter(writerOptions(writer)),
        "writer_state_conflict",
      );
      syncCapabilityError(
        () => coordinator.dispose(),
        "writer_state_conflict",
      );
    });
  }
});

test("generator-shaped stop results reject inherited then without assimilation", async (t) => {
  const scenarios = [
    {
      name: "generator result",
      createStopWriter(hooks) {
        function* stopResult() {
          hooks.bodyCalls += 1;
          return STOPPED_WRITER_STOP_CONFIRMED;
        }
        Object.defineProperty(stopResult.prototype, "then", {
          value(resolve) {
            hooks.thenCalls += 1;
            resolve(STOPPED_WRITER_STOP_CONFIRMED);
          },
        });
        return () => stopResult();
      },
    },
    {
      name: "bound generator callback",
      createStopWriter(hooks) {
        function* stopWriter() {
          hooks.bodyCalls += 1;
          return STOPPED_WRITER_STOP_CONFIRMED;
        }
        Object.defineProperty(stopWriter.prototype, "then", {
          value(resolve) {
            hooks.thenCalls += 1;
            resolve(STOPPED_WRITER_STOP_CONFIRMED);
          },
        });
        return stopWriter.bind(null);
      },
    },
    {
      name: "bound async-generator callback",
      createStopWriter(hooks) {
        async function* stopWriter() {
          hooks.bodyCalls += 1;
          return STOPPED_WRITER_STOP_CONFIRMED;
        }
        Object.defineProperty(stopWriter.prototype, "then", {
          value(resolve) {
            hooks.thenCalls += 1;
            resolve(STOPPED_WRITER_STOP_CONFIRMED);
          },
        });
        return stopWriter.bind(null);
      },
    },
  ];

  for (const scenario of scenarios) {
    await t.test(scenario.name, async () => {
      const coordinator = new StoppedWriterCapabilityCoordinator();
      const hooks = { bodyCalls: 0, thenCalls: 0 };
      const writer = coordinator.registerWriter(
        registerOptions({ stopWriter: scenario.createStopWriter(hooks) }),
      );

      await rejectedCapabilityError(
        () => coordinator.stopAndIssueCapability(stopOptions(writer)),
        "writer_stop_outcome_uncertain",
      );
      assert.equal(hooks.bodyCalls, 0);
      assert.equal(hooks.thenCalls, 0);
      await rejectedCapabilityError(
        () => coordinator.stopAndIssueCapability(stopOptions(writer)),
        "writer_state_conflict",
      );
    });
  }
});

test("stop callback accepts a native Promise resolving the exact sentinel", async () => {
  const coordinator = new StoppedWriterCapabilityCoordinator();
  let stopCalls = 0;
  const writer = coordinator.registerWriter(
    registerOptions({
      stopWriter: () => {
        stopCalls += 1;
        return Promise.resolve(STOPPED_WRITER_STOP_CONFIRMED);
      },
    }),
  );

  const capability = await coordinator.stopAndIssueCapability(stopOptions(writer));
  assertOpaqueHandle(capability);
  assert.equal(stopCalls, 1);
});

test("snapshot callback failure is sanitized, consumes authority, and is terminal", async (t) => {
  for (const scenario of [
    {
      name: "synchronous throw",
      runSnapshot() {
        throw new Error("snapshot-secret-sync /private/source");
      },
    },
    {
      name: "asynchronous rejection",
      async runSnapshot() {
        throw new Error("snapshot-secret-async /private/source");
      },
    },
    {
      name: "throwing then getter",
      runSnapshot() {
        return Object.defineProperty({}, "then", {
          get() {
            throw new Error("snapshot-secret-thenable /private/source");
          },
        });
      },
    },
  ]) {
    await t.test(scenario.name, async () => {
      const fixture = await readyCapability();
      let snapshotCalls = 0;
      await rejectedCapabilityError(
        () =>
          fixture.coordinator.consumeCapability(
            consumeOptions(fixture.writer, fixture.capability, {
              runSnapshot: (...args) => {
                snapshotCalls += 1;
                return scenario.runSnapshot(...args);
              },
            }),
          ),
        "snapshot_outcome_uncertain",
        ["snapshot-secret", "/private/source", STORAGE_ID],
      );
      await rejectedCapabilityError(
        () =>
          fixture.coordinator.consumeCapability(
            consumeOptions(fixture.writer, fixture.capability, {
              runSnapshot: async () => {
                snapshotCalls += 1;
              },
            }),
          ),
        "stopped_writer_capability_rejected",
      );
      assert.equal(snapshotCalls, 1);
      await rejectedCapabilityError(
        () => fixture.coordinator.stopAndIssueCapability(stopOptions(fixture.writer)),
        "writer_state_conflict",
      );
    });
  }
});

test("direct callable thenables are rejected before coordinator assimilation", async () => {
  const fixture = await readyCapability();
  let thenCalls = 0;
  const result = {
    then(resolve) {
      thenCalls += 1;
      resolve("false-success");
    },
  };

  await rejectedCapabilityError(
    () =>
      fixture.coordinator.consumeCapability(
        consumeOptions(fixture.writer, fixture.capability, {
          runSnapshot: () => result,
        }),
      ),
    "snapshot_outcome_uncertain",
  );
  assert.equal(thenCalls, 0);
  await rejectedCapabilityError(
    () =>
      fixture.coordinator.consumeCapability(
        consumeOptions(fixture.writer, fixture.capability),
      ),
    "stopped_writer_capability_rejected",
  );
  syncCapabilityError(
    () => fixture.coordinator.retireWriter(writerOptions(fixture.writer)),
    "writer_state_conflict",
  );
});

test("coordinator awaits only Promise values with a safe constructor path", async (t) => {
  await t.test("captured Promise constructor", async () => {
    const fixture = await readyCapability();
    const expected = Object.freeze({ status: "checkpoint-created" });
    const returned = await fixture.coordinator.consumeCapability(
      consumeOptions(fixture.writer, fixture.capability, {
        runSnapshot: () => Promise.resolve(expected),
      }),
    );
    assert.strictEqual(returned, expected);
  });

  let constructorReads = 0;
  let prototypeTraps = 0;
  class ForeignPromise extends Promise {}
  const scenarios = [
    {
      name: "constructor accessor",
      create() {
        return Object.defineProperty(Promise.resolve("unsafe"), "constructor", {
          configurable: true,
          get() {
            constructorReads += 1;
            return Promise;
          },
        });
      },
    },
    {
      name: "handled rejection with a constructor accessor",
      create() {
        const value = Promise.reject(
          new Error("snapshot-secret-owned-rejection"),
        );
        value.catch(() => undefined);
        return Object.defineProperty(value, "constructor", {
          configurable: true,
          get() {
            constructorReads += 1;
            throw new Error("snapshot-secret-constructor-accessor");
          },
        });
      },
    },
    {
      name: "foreign constructor data",
      create() {
        return Object.defineProperty(Promise.resolve("unsafe"), "constructor", {
          configurable: true,
          value: ForeignPromise,
        });
      },
    },
    {
      name: "Promise subclass",
      create: () => ForeignPromise.resolve("unsafe"),
    },
    {
      name: "cross-realm Promise",
      create: () => runInNewContext('Promise.resolve("unsafe")'),
    },
    {
      name: "proxy constructor chain",
      create() {
        const value = Promise.resolve("unsafe");
        const prototype = new Proxy(Object.getPrototypeOf(value), {
          getOwnPropertyDescriptor() {
            prototypeTraps += 1;
            throw new Error("constructor proxy must remain opaque");
          },
          getPrototypeOf() {
            prototypeTraps += 1;
            throw new Error("constructor proxy must remain opaque");
          },
        });
        Object.setPrototypeOf(value, prototype);
        return value;
      },
    },
  ];

  for (const scenario of scenarios) {
    await t.test(scenario.name, async () => {
      const fixture = await readyCapability();
      await rejectedCapabilityError(
        () =>
          fixture.coordinator.consumeCapability(
            consumeOptions(fixture.writer, fixture.capability, {
              runSnapshot: scenario.create,
            }),
          ),
        "snapshot_outcome_uncertain",
        ["snapshot-secret", STORAGE_ID],
      );
      await new Promise((resolve) => setImmediate(resolve));
    });
  }
  assert.equal(constructorReads, 0);
  assert.equal(prototypeTraps, 0);
});

test("direct stateful then access is rejected before assimilation", async (t) => {
  for (const inherited of [false, true]) {
    await t.test(inherited ? "inherited then accessor" : "own then accessor", async () => {
      const fixture = await readyCapability();
      let thenReads = 0;
      const owner = {};
      Object.defineProperty(owner, "then", {
        configurable: true,
        get() {
          thenReads += 1;
          if (thenReads > 1) {
            throw new Error("snapshot-secret-second-then /private/source");
          }
          return undefined;
        },
      });
      const result = inherited ? Object.create(owner) : owner;

      await rejectedCapabilityError(
        () =>
          fixture.coordinator.consumeCapability(
            consumeOptions(fixture.writer, fixture.capability, {
              runSnapshot: () => result,
            }),
          ),
        "snapshot_outcome_uncertain",
        ["snapshot-secret", "/private/source", STORAGE_ID],
      );
      assert.equal(thenReads, 0);
      await rejectedCapabilityError(
        () =>
          fixture.coordinator.consumeCapability(
            consumeOptions(fixture.writer, fixture.capability),
        ),
        "stopped_writer_capability_rejected",
      );
      syncCapabilityError(
        () => fixture.coordinator.retireWriter(writerOptions(fixture.writer)),
        "writer_state_conflict",
      );
      const newerLease = lease({ fencingEpoch: "9007199254740994" });
      syncCapabilityError(
        () =>
          fixture.coordinator.registerWriter(
            registerOptions({
              attachment: attachment(newerLease),
              canonicalLease: newerLease,
            }),
          ),
        "writer_state_conflict",
      );
    });
  }
});

test("then access cannot install a callable data descriptor for async return", async () => {
  const fixture = await readyCapability();
  let callableThenCalls = 0;
  let thenReads = 0;
  const result = {};
  Object.defineProperty(result, "then", {
    configurable: true,
    get() {
      thenReads += 1;
      Object.defineProperty(result, "then", {
        configurable: true,
        value() {
          callableThenCalls += 1;
          throw new Error("snapshot-secret-callable-then /private/source");
        },
      });
      return undefined;
    },
  });

  await rejectedCapabilityError(
    () =>
      fixture.coordinator.consumeCapability(
        consumeOptions(fixture.writer, fixture.capability, {
          runSnapshot: () => Promise.resolve(result),
        }),
      ),
    "snapshot_outcome_uncertain",
    ["snapshot-secret", "/private/source", STORAGE_ID],
  );
  assert.equal(thenReads, 1);
  assert.equal(callableThenCalls, 0);
  syncCapabilityError(
    () => fixture.coordinator.retireWriter(writerOptions(fixture.writer)),
    "writer_state_conflict",
  );
});

test("non-callable data then descriptors preserve callback result identity", async (t) => {
  for (const inherited of [false, true]) {
    await t.test(inherited ? "inherited data descriptor" : "own data descriptor", async () => {
      const fixture = await readyCapability();
      const owner = Object.defineProperty({}, "then", { value: null });
      const result = inherited
        ? Object.assign(Object.create(Object.freeze(owner)), {
            status: "checkpoint-created",
          })
        : Object.assign(owner, { status: "checkpoint-created" });
      Object.freeze(result);

      const returned = await fixture.coordinator.consumeCapability(
        consumeOptions(fixture.writer, fixture.capability, {
          runSnapshot: () => result,
        }),
      );
      assert.strictEqual(returned, result);
    });
  }
});

test("callback intrinsic poisoning cannot bypass result assimilation checks", async () => {
  const originals = {
    defineProperty: Object.defineProperty,
    errorNameDescriptor: Object.getOwnPropertyDescriptor(Error.prototype, "name"),
    freeze: Object.freeze,
    getOwnPropertyDescriptor: Object.getOwnPropertyDescriptor,
    getPrototypeOf: Object.getPrototypeOf,
    hasOwn: Object.hasOwn,
    isPromise: utilTypes.isPromise,
    isProxy: utilTypes.isProxy,
    reflectApply: Reflect.apply,
    weakSetAdd: WeakSet.prototype.add,
    weakSetHas: WeakSet.prototype.has,
  };
  const restore = () => {
    Object.defineProperty = originals.defineProperty;
    if (originals.errorNameDescriptor === undefined) {
      delete Error.prototype.name;
    } else {
      originals.defineProperty(
        Error.prototype,
        "name",
        originals.errorNameDescriptor,
      );
    }
    Object.freeze = originals.freeze;
    Object.getOwnPropertyDescriptor = originals.getOwnPropertyDescriptor;
    Object.getPrototypeOf = originals.getPrototypeOf;
    Object.hasOwn = originals.hasOwn;
    utilTypes.isPromise = originals.isPromise;
    utilTypes.isProxy = originals.isProxy;
    Reflect.apply = originals.reflectApply;
    WeakSet.prototype.add = originals.weakSetAdd;
    WeakSet.prototype.has = originals.weakSetHas;
  };
  const fixture = await readyCapability();
  let prototypeInspectionTraps = 0;
  let thenReads = 0;
  const poisonedPrototype = new Proxy(
    {},
    {
      get(target, key, receiver) {
        if (key !== "then") return Reflect.get(target, key, receiver);
        thenReads += 1;
        return undefined;
      },
      getOwnPropertyDescriptor() {
        prototypeInspectionTraps += 1;
        return undefined;
      },
      getPrototypeOf() {
        prototypeInspectionTraps += 1;
        return null;
      },
    },
  );
  const result = Object.create(poisonedPrototype);
  let caught;
  try {
    await fixture.coordinator.consumeCapability(
      consumeOptions(fixture.writer, fixture.capability, {
        runSnapshot: () => {
          originals.defineProperty(Error.prototype, "name", {
            configurable: true,
            set() {
              throw new Error("snapshot-secret-error-setter /private/source");
            },
          });
          Object.defineProperty = () => {
            throw new Error("snapshot-secret-define-property /private/source");
          };
          Object.freeze = () => {
            throw new Error("snapshot-secret-freeze /private/source");
          };
          Object.getOwnPropertyDescriptor = () => undefined;
          Object.getPrototypeOf = () => null;
          Object.hasOwn = () => {
            throw new Error("snapshot-secret-has-own /private/source");
          };
          utilTypes.isPromise = () => false;
          utilTypes.isProxy = () => false;
          Reflect.apply = () => {
            throw new Error("snapshot-secret-reflect-apply /private/source");
          };
          WeakSet.prototype.add = () => {
            throw new Error("snapshot-secret-weak-set-add /private/source");
          };
          WeakSet.prototype.has = () => {
            throw new Error("snapshot-secret-weak-set-has /private/source");
          };
          return result;
        },
      }),
    );
  } catch (error) {
    caught = error;
  } finally {
    restore();
  }

  assertCapabilityError(caught, "snapshot_outcome_uncertain", [
    "snapshot-secret",
    "/private/source",
    STORAGE_ID,
  ]);
  assert.equal(thenReads, 0);
  assert.equal(prototypeInspectionTraps, 0);
  await rejectedCapabilityError(
    () =>
      fixture.coordinator.consumeCapability(
        consumeOptions(fixture.writer, fixture.capability),
      ),
    "stopped_writer_capability_rejected",
  );
  syncCapabilityError(
    () => fixture.coordinator.retireWriter(writerOptions(fixture.writer)),
    "writer_state_conflict",
  );
});

test("generator-shaped snapshot results are terminal uncertainty", async (t) => {
  for (const [name, createCallback] of [
    [
      "bound generator",
      (onBody) =>
        (function* runSnapshot() {
          onBody();
          return "captured";
        }).bind(null),
    ],
    [
      "bound async generator",
      (onBody) =>
        (async function* runSnapshot() {
          onBody();
          return "captured";
        }).bind(null),
    ],
    [
      "proxy-wrapped generator",
      (onBody) => () =>
        new Proxy(
          (function* snapshotResult() {
            onBody();
            return "captured";
          })(),
          {},
        ),
    ],
    [
      "proxy-wrapped async generator",
      (onBody) => () =>
        new Proxy(
          (async function* snapshotResult() {
            onBody();
            return "captured";
          })(),
          {},
        ),
    ],
  ]) {
    await t.test(name, async () => {
      const fixture = await readyCapability();
      let bodyCalls = 0;
      const runSnapshot = createCallback(() => {
        bodyCalls += 1;
      });
      await rejectedCapabilityError(
        () =>
          fixture.coordinator.consumeCapability(
            consumeOptions(fixture.writer, fixture.capability, { runSnapshot }),
          ),
        "snapshot_outcome_uncertain",
      );
      assert.equal(bodyCalls, 0);
      await rejectedCapabilityError(
        () =>
          fixture.coordinator.consumeCapability(
            consumeOptions(fixture.writer, fixture.capability),
          ),
        "stopped_writer_capability_rejected",
      );
    });
  }
});

test("revocation invalidates running and stopped writers without an oracle", async () => {
  const runningCoordinator = new StoppedWriterCapabilityCoordinator();
  let runningStopCalls = 0;
  const runningWriter = runningCoordinator.registerWriter(
    registerOptions({
      stopWriter: async () => {
        runningStopCalls += 1;
        return STOPPED_WRITER_STOP_CONFIRMED;
      },
    }),
  );
  runningCoordinator.revokeWriter(writerOptions(runningWriter));
  await rejectedCapabilityError(
    () => runningCoordinator.stopAndIssueCapability(stopOptions(runningWriter)),
    "writer_state_conflict",
  );
  assert.equal(runningStopCalls, 0);

  const fixture = await readyCapability();
  let snapshotCalls = 0;
  fixture.coordinator.revokeWriter(writerOptions(fixture.writer));
  await rejectedCapabilityError(
    () =>
      fixture.coordinator.consumeCapability(
        consumeOptions(fixture.writer, fixture.capability, {
          runSnapshot: async () => {
            snapshotCalls += 1;
          },
        }),
      ),
    "stopped_writer_capability_rejected",
  );
  assert.equal(snapshotCalls, 0);
});

test("retirement releases a storage slot only for a strictly newer fence", async () => {
  const coordinator = new StoppedWriterCapabilityCoordinator();
  const initialLease = lease();
  const first = await readyCapability({
    coordinator,
    mounted: attachment(initialLease),
    writerLease: initialLease,
  });
  await coordinator.consumeCapability(
    consumeOptions(first.writer, first.capability, {
      runSnapshot: async () => "done",
    }),
  );
  coordinator.retireWriter(writerOptions(first.writer));

  for (const staleEpoch of [LARGE_EPOCH, "9007199254740992"]) {
    const staleLease = lease({
      fencingEpoch: staleEpoch,
      leaseId: `lease-${staleEpoch}`,
    });
    syncCapabilityError(
      () =>
        coordinator.registerWriter(
          registerOptions({
            attachment: attachment(staleLease, { attachmentId: `attachment-${staleEpoch}` }),
            canonicalLease: staleLease,
            processIncarnationId: `process-${staleEpoch}`,
            writerIncarnationId: `writer-${staleEpoch}`,
          }),
        ),
      "writer_state_conflict",
    );
  }

  const newerLease = lease({
    fencingEpoch: "9007199254740994",
    leaseId: "lease-002",
    holderId: "host-002",
  });
  const replacement = coordinator.registerWriter(
    registerOptions({
      attachment: attachment(newerLease, { attachmentId: "attachment-002" }),
      canonicalLease: newerLease,
      processIncarnationId: "process-incarnation-002",
      writerIncarnationId: "writer-incarnation-002",
    }),
  );
  assertOpaqueHandle(replacement);
});

test("reentrant higher-fence registration cannot replace the only live slot writer", async () => {
  const coordinator = new StoppedWriterCapabilityCoordinator();
  const first = await readyCapability({ coordinator });
  await coordinator.consumeCapability(
    consumeOptions(first.writer, first.capability, {
      runSnapshot: () => "initial-captured",
    }),
  );
  coordinator.retireWriter(writerOptions(first.writer));

  const outerLease = lease({
    fencingEpoch: "9007199254740994",
    holderId: "host-outer",
    leaseId: "lease-outer",
  });
  const outerAttachment = attachment(outerLease, {
    attachmentId: "attachment-outer",
    operationId: "operation-outer",
    proofId: "proof-outer",
  });
  const innerLease = lease({
    fencingEpoch: "9007199254740995",
    holderId: "host-inner",
    leaseId: "lease-inner",
  });
  const innerAttachment = attachment(innerLease, {
    attachmentId: "attachment-inner",
    operationId: "operation-inner",
    proofId: "proof-inner",
  });
  const originalBigInt = globalThis.BigInt;
  let bigIntCalls = 0;
  let innerStopCalls = 0;
  let innerWriter;
  let outerStopCalls = 0;
  let outerWriter;

  try {
    globalThis.BigInt = (value) => {
      bigIntCalls += 1;
      if (bigIntCalls === 3) {
        globalThis.BigInt = originalBigInt;
        innerWriter = coordinator.registerWriter(
          registerOptions({
            attachment: innerAttachment,
            canonicalLease: innerLease,
            processIncarnationId: "process-incarnation-inner",
            stopWriter: () => {
              innerStopCalls += 1;
              return STOPPED_WRITER_STOP_CONFIRMED;
            },
            writerIncarnationId: "writer-incarnation-inner",
          }),
        );
      }
      return originalBigInt(value);
    };
    outerWriter = coordinator.registerWriter(
      registerOptions({
        attachment: outerAttachment,
        canonicalLease: outerLease,
        processIncarnationId: "process-incarnation-outer",
        stopWriter: () => {
          outerStopCalls += 1;
          return STOPPED_WRITER_STOP_CONFIRMED;
        },
        writerIncarnationId: "writer-incarnation-outer",
      }),
    );
  } finally {
    globalThis.BigInt = originalBigInt;
  }

  assert.equal(bigIntCalls, 2);
  assert.equal(innerWriter, undefined);
  assertOpaqueHandle(outerWriter);
  assert.equal(innerStopCalls, 0);
  const outerStopOperationId = "stop-operation-outer";
  const outerCapability = await coordinator.stopAndIssueCapability(
    stopOptions(outerWriter, {
      processIncarnationId: "process-incarnation-outer",
      stopOperationId: outerStopOperationId,
      writerIncarnationId: "writer-incarnation-outer",
    }),
  );
  assert.equal(outerStopCalls, 1);
  let snapshotCalls = 0;
  const outerConsumeOptions = consumeOptions(outerWriter, outerCapability, {
    attachment: outerAttachment,
    canonicalLease: outerLease,
    processIncarnationId: "process-incarnation-outer",
    runSnapshot: () => {
      snapshotCalls += 1;
      return "outer-captured";
    },
    stopOperationId: outerStopOperationId,
    writerIncarnationId: "writer-incarnation-outer",
  });
  assert.equal(
    await coordinator.consumeCapability(outerConsumeOptions),
    "outer-captured",
  );
  await rejectedCapabilityError(
    () => coordinator.consumeCapability(outerConsumeOptions),
    "stopped_writer_capability_rejected",
  );
  assert.equal(snapshotCalls, 1);
  coordinator.retireWriter(
    writerOptions(outerWriter, {
      processIncarnationId: "process-incarnation-outer",
      writerIncarnationId: "writer-incarnation-outer",
    }),
  );
  assert.equal(coordinator.dispose(), undefined);
});

test("fresh-slot registration validates fencing epochs with captured intrinsics", () => {
  const coordinator = new StoppedWriterCapabilityCoordinator();
  const invalidLease = lease({
    fencingEpoch: "18446744073709551616",
  });
  const invalidAttachment = attachment(invalidLease);
  const originalBigInt = globalThis.BigInt;
  let bigIntCalls = 0;
  let stopCalls = 0;

  try {
    globalThis.BigInt = () => {
      bigIntCalls += 1;
      return 1n;
    };
    syncCapabilityError(
      () =>
        coordinator.registerWriter(
          registerOptions({
            attachment: invalidAttachment,
            canonicalLease: invalidLease,
            stopWriter: () => {
              stopCalls += 1;
              return STOPPED_WRITER_STOP_CONFIRMED;
            },
          }),
        ),
      "invalid_stopped_writer_request",
    );
  } finally {
    globalThis.BigInt = originalBigInt;
  }

  assert.equal(bigIntCalls, 2);
  assert.equal(stopCalls, 0);
  assert.equal(coordinator.dispose(), undefined);
});

test("registration validation ignores RegExp exec prototype poisoning", () => {
  const originalExec = RegExp.prototype.exec;
  let poisonedExecCalls = 0;

  try {
    RegExp.prototype.exec = () => {
      poisonedExecCalls += 1;
      return ["poisoned-match"];
    };

    const identityCoordinator = new StoppedWriterCapabilityCoordinator();
    syncCapabilityError(
      () =>
        identityCoordinator.registerWriter(
          registerOptions({ processIncarnationId: "" }),
        ),
      "invalid_stopped_writer_request",
    );
    assert.equal(poisonedExecCalls, 0);
    assert.equal(identityCoordinator.dispose(), undefined);

    const fenceCoordinator = new StoppedWriterCapabilityCoordinator();
    const invalidLease = lease({ fencingEpoch: "-1" });
    syncCapabilityError(
      () =>
        fenceCoordinator.registerWriter(
          registerOptions({
            attachment: attachment(invalidLease),
            canonicalLease: invalidLease,
          }),
        ),
      "invalid_stopped_writer_request",
    );
    assert(poisonedExecCalls > 0);
    assert.equal(fenceCoordinator.dispose(), undefined);

    const bindingScenarios = [
      [{ attachmentId: "" }, {}],
      [{ backendId: "" }, {}],
      [{ operationId: "" }, {}],
      [{ proofId: "" }, {}],
      [{ storageId: "" }, {}],
      [{}, { holderId: "" }],
      [{}, { leaseId: "" }],
      [{}, { sessionId: "" }],
    ];
    for (const [attachmentOverrides, leaseOverrides] of bindingScenarios) {
      const invalidLease = lease(leaseOverrides);
      const coordinator = new StoppedWriterCapabilityCoordinator();
      const before = poisonedExecCalls;
      syncCapabilityError(
        () =>
          coordinator.registerWriter(
            registerOptions({
              attachment: attachment(invalidLease, attachmentOverrides),
              canonicalLease: invalidLease,
            }),
          ),
        "invalid_stopped_writer_request",
      );
      assert(poisonedExecCalls > before);
      assert.equal(coordinator.dispose(), undefined);
    }
  } finally {
    RegExp.prototype.exec = originalExec;
  }
});

test("registration independently validates timestamps", () => {
  const originalDateParse = Date.parse;
  const originalDateToISOString = Date.prototype.toISOString;
  let poisonedDateCalls = 0;

  try {
    Date.parse = () => {
      poisonedDateCalls += 1;
      return 0;
    };
    Date.prototype.toISOString = () => "not-a-timestamp";

    const timestampLease = lease({ expiresAt: "not-a-timestamp" });
    const timestampCoordinator = new StoppedWriterCapabilityCoordinator();
    syncCapabilityError(
      () =>
        timestampCoordinator.registerWriter(
          registerOptions({
            attachment: attachment(timestampLease),
            canonicalLease: timestampLease,
          }),
        ),
      "invalid_stopped_writer_request",
    );
    assert(poisonedDateCalls > 0);
    assert.equal(timestampCoordinator.dispose(), undefined);
  } finally {
    Date.parse = originalDateParse;
    Date.prototype.toISOString = originalDateToISOString;
  }
});

test("registration independently rejects NUL root paths", () => {
  const originalIncludes = String.prototype.includes;
  let poisonedIncludesCalls = 0;

  try {
    String.prototype.includes = () => {
      poisonedIncludesCalls += 1;
      return false;
    };
    const coordinator = new StoppedWriterCapabilityCoordinator();
    syncCapabilityError(
      () =>
        coordinator.registerWriter(
          registerOptions({
            attachment: attachment(lease(), {
              rootPath: "/var/lib/portable-codex/\0session-001",
            }),
          }),
        ),
      "invalid_stopped_writer_request",
    );
    assert(poisonedIncludesCalls > 0);
    assert.equal(coordinator.dispose(), undefined);
  } finally {
    String.prototype.includes = originalIncludes;
  }
});

test("registration captures path validators before builtin ESM synchronization", () => {
  const originalResolve = path.resolve;
  let poisonedResolveCalls = 0;

  try {
    path.resolve = (value) => {
      poisonedResolveCalls += 1;
      return value;
    };
    syncBuiltinESMExports();

    const coordinator = new StoppedWriterCapabilityCoordinator();
    syncCapabilityError(
      () =>
        coordinator.registerWriter(
          registerOptions({
            attachment: attachment(lease(), {
              rootPath: "/var/lib/../etc",
            }),
          }),
        ),
      "invalid_stopped_writer_request",
    );
    assert(poisonedResolveCalls > 0);
    assert.equal(coordinator.dispose(), undefined);
  } finally {
    path.resolve = originalResolve;
    syncBuiltinESMExports();
  }
});

test("dispose requires safe retirement and permanently closes the issuer", async () => {
  const coordinator = new StoppedWriterCapabilityCoordinator();
  let snapshotCalls = 0;
  let stopCalls = 0;
  const fixture = await readyCapability({
    coordinator,
    stopWriter: async () => {
      stopCalls += 1;
      return STOPPED_WRITER_STOP_CONFIRMED;
    },
  });

  syncCapabilityError(
    () => coordinator.dispose(),
    "writer_state_conflict",
  );
  await coordinator.consumeCapability(
    consumeOptions(fixture.writer, fixture.capability, {
      runSnapshot: async () => {
        snapshotCalls += 1;
        return "captured";
      },
    }),
  );
  coordinator.retireWriter(writerOptions(fixture.writer));
  syncCapabilityError(
    () => coordinator.dispose("unexpected"),
    "invalid_stopped_writer_request",
  );
  assert.equal(coordinator.dispose(), undefined);

  let inputTraps = 0;
  const hostileOptions = new Proxy(
    {},
    {
      get() {
        inputTraps += 1;
        throw new Error("disposed coordinator must not inspect input");
      },
      getPrototypeOf() {
        inputTraps += 1;
        throw new Error("disposed coordinator must not inspect input");
      },
      ownKeys() {
        inputTraps += 1;
        throw new Error("disposed coordinator must not inspect input");
      },
    },
  );
  syncCapabilityError(
    () => coordinator.registerWriter(hostileOptions),
    "writer_state_conflict",
  );
  await rejectedCapabilityError(
    () => coordinator.stopAndIssueCapability(hostileOptions),
    "writer_state_conflict",
  );
  await rejectedCapabilityError(
    () => coordinator.consumeCapability(hostileOptions),
    "stopped_writer_capability_rejected",
  );
  syncCapabilityError(
    () => coordinator.revokeWriter(hostileOptions),
    "writer_state_conflict",
  );
  syncCapabilityError(
    () => coordinator.retireWriter(hostileOptions),
    "writer_state_conflict",
  );
  syncCapabilityError(
    () => coordinator.dispose(),
    "writer_state_conflict",
  );
  assert.equal(inputTraps, 0);
  assert.equal(stopCalls, 1);
  assert.equal(snapshotCalls, 1);
});

test("dispose refuses running and uncertain writer scopes", async (t) => {
  await t.test("running writer", () => {
    const coordinator = new StoppedWriterCapabilityCoordinator();
    const writer = coordinator.registerWriter(registerOptions());
    syncCapabilityError(
      () => coordinator.dispose(),
      "writer_state_conflict",
    );
    coordinator.revokeWriter(writerOptions(writer));
    syncCapabilityError(
      () => coordinator.dispose(),
      "writer_state_conflict",
    );
  });

  await t.test("stop-uncertain writer", async () => {
    const coordinator = new StoppedWriterCapabilityCoordinator();
    const writer = coordinator.registerWriter(
      registerOptions({
        stopWriter: async () => {
          throw new Error("uncertain stop");
        },
      }),
    );
    await rejectedCapabilityError(
      () => coordinator.stopAndIssueCapability(stopOptions(writer)),
      "writer_stop_outcome_uncertain",
    );
    syncCapabilityError(
      () => coordinator.dispose(),
      "writer_state_conflict",
    );
  });

  await t.test("snapshot-uncertain writer", async () => {
    const fixture = await readyCapability();
    await rejectedCapabilityError(
      () =>
        fixture.coordinator.consumeCapability(
          consumeOptions(fixture.writer, fixture.capability, {
            runSnapshot: async () => {
              throw new Error("uncertain snapshot");
            },
          }),
        ),
      "snapshot_outcome_uncertain",
    );
    syncCapabilityError(
      () => fixture.coordinator.dispose(),
      "writer_state_conflict",
    );
  });
});

test("dispose releases a high-churn finite issuer scope", async () => {
  const coordinator = new StoppedWriterCapabilityCoordinator();
  const entries = [];
  for (let index = 0; index < 128; index += 1) {
    const id = index.toString(16).padStart(4, "0");
    const sessionId = `019f2100-0000-7000-8000-${index
      .toString(16)
      .padStart(12, "0")}`;
    const writerLease = lease({
      holderId: `host-${id}`,
      leaseId: `lease-${id}`,
      sessionId,
    });
    const mounted = attachment(writerLease, {
      attachmentId: `attachment-${id}`,
      operationId: `operation-attach-${id}`,
      proofId: `proof-attachment-${id}`,
      rootPath: `/var/lib/portable-codex/churn-${id}`,
      storageId: `volume-${id}`,
    });
    const processIncarnationId = `process-${id}`;
    const stopOperationId = `stop-${id}`;
    const writerIncarnationId = `writer-${id}`;
    const fixture = await readyCapability({
      coordinator,
      mounted,
      processIncarnationId,
      stopOperationId,
      writerIncarnationId,
      writerLease,
    });
    entries.push({
      fixture,
      id,
      mounted,
      processIncarnationId,
      stopOperationId,
      writerIncarnationId,
      writerLease,
    });
  }

  syncCapabilityError(
    () => coordinator.dispose(),
    "writer_state_conflict",
  );
  for (const [index, entry] of entries.entries()) {
    const result = await coordinator.consumeCapability({
      attachment: entry.mounted,
      canonicalLease: entry.writerLease,
      capability: entry.fixture.capability,
      processIncarnationId: entry.processIncarnationId,
      runSnapshot: async () => entry.id,
      stopOperationId: entry.stopOperationId,
      writer: entry.fixture.writer,
      writerIncarnationId: entry.writerIncarnationId,
    });
    assert.equal(result, entry.id);
    coordinator.retireWriter({
      processIncarnationId: entry.processIncarnationId,
      writer: entry.fixture.writer,
      writerIncarnationId: entry.writerIncarnationId,
    });
    if (index === 63) {
      syncCapabilityError(
        () => coordinator.dispose(),
        "writer_state_conflict",
      );
    }
  }

  assert.equal(coordinator.dispose(), undefined);
  syncCapabilityError(
    () => coordinator.registerWriter(registerOptions()),
    "writer_state_conflict",
  );
});

test("slot identity is session, backend, and storage rather than attachment ID", () => {
  const coordinator = new StoppedWriterCapabilityCoordinator();
  const first = coordinator.registerWriter(registerOptions());
  assertOpaqueHandle(first);

  syncCapabilityError(
    () =>
      coordinator.registerWriter(
        registerOptions({
          attachment: attachment(lease(), {
            attachmentId: "attachment-002",
            operationId: "operation-attach-002",
            proofId: "proof-attachment-002",
            rootPath: "/var/lib/portable-codex/session-002",
          }),
          processIncarnationId: "process-incarnation-002",
          writerIncarnationId: "writer-incarnation-002",
        }),
      ),
    "writer_state_conflict",
  );

  const otherStorageLease = lease({ leaseId: "lease-other-storage" });
  const otherStorage = coordinator.registerWriter(
    registerOptions({
      attachment: attachment(otherStorageLease, {
        attachmentId: "attachment-other-storage",
        operationId: "operation-attach-other-storage",
        proofId: "proof-attachment-other-storage",
        rootPath: "/var/lib/portable-codex/other-storage",
        storageId: "volume-002",
      }),
      canonicalLease: otherStorageLease,
      processIncarnationId: "process-incarnation-other-storage",
      writerIncarnationId: "writer-incarnation-other-storage",
    }),
  );
  assertOpaqueHandle(otherStorage);
});

test("slot lookup is immune to inherited toJSON poisoning", async (t) => {
  for (const [name, prototype] of [
    ["Array.prototype", Array.prototype],
    ["Object.prototype", Object.prototype],
  ]) {
    await t.test(name, () => {
      const original = Object.getOwnPropertyDescriptor(prototype, "toJSON");
      const coordinator = new StoppedWriterCapabilityCoordinator();
      coordinator.registerWriter(registerOptions());
      let toJSONCalls = 0;
      Object.defineProperty(prototype, "toJSON", {
        configurable: true,
        value() {
          toJSONCalls += 1;
          return [`poisoned-${name}-${toJSONCalls}`];
        },
        writable: true,
      });
      try {
        const newerLease = lease({
          fencingEpoch: "9007199254740994",
          holderId: "host-002",
          leaseId: "lease-002",
        });
        syncCapabilityError(
          () =>
            coordinator.registerWriter(
              registerOptions({
                attachment: attachment(newerLease, {
                  attachmentId: "attachment-002",
                }),
                canonicalLease: newerLease,
                processIncarnationId: "process-incarnation-002",
                writerIncarnationId: "writer-incarnation-002",
              }),
            ),
          "writer_state_conflict",
        );
        assert.equal(toJSONCalls, 0);
      } finally {
        if (original === undefined) {
          delete prototype.toJSON;
        } else {
          Object.defineProperty(prototype, "toJSON", original);
        }
      }
    });
  }
});

test("serialized, cloned, wrapped, and foreign capabilities are rejected", async (t) => {
  await t.test("structured clone", async () => {
    const fixture = await readyCapability();
    const clone = structuredClone(fixture.capability);
    let snapshotCalls = 0;
    await rejectedCapabilityError(
      () =>
        fixture.coordinator.consumeCapability(
          consumeOptions(fixture.writer, clone, {
            runSnapshot: async () => {
              snapshotCalls += 1;
            },
          }),
        ),
      "stopped_writer_capability_rejected",
    );
    assert.equal(snapshotCalls, 0);
    await fixture.coordinator.consumeCapability(
      consumeOptions(fixture.writer, fixture.capability),
    );
  });

  await t.test("JSON round trip", async () => {
    const fixture = await readyCapability();
    const clone = JSON.parse(JSON.stringify(fixture.capability));
    await rejectedCapabilityError(
      () =>
        fixture.coordinator.consumeCapability(
          consumeOptions(fixture.writer, clone),
        ),
      "stopped_writer_capability_rejected",
    );
    await fixture.coordinator.consumeCapability(
      consumeOptions(fixture.writer, fixture.capability),
    );
  });

  await t.test("proxy wrapper without traps", async () => {
    const fixture = await readyCapability();
    let traps = 0;
    const wrapped = new Proxy(fixture.capability, {
      get() {
        traps += 1;
        throw new Error("capability proxy must stay opaque");
      },
      getPrototypeOf() {
        traps += 1;
        throw new Error("capability proxy must stay opaque");
      },
      ownKeys() {
        traps += 1;
        throw new Error("capability proxy must stay opaque");
      },
    });
    await rejectedCapabilityError(
      () =>
        fixture.coordinator.consumeCapability(
          consumeOptions(fixture.writer, wrapped),
        ),
      "stopped_writer_capability_rejected",
    );
    assert.equal(traps, 0);
    await fixture.coordinator.consumeCapability(
      consumeOptions(fixture.writer, fixture.capability),
    );
  });

  await t.test("foreign coordinator", async () => {
    const first = await readyCapability();
    const second = await readyCapability({
      coordinator: new StoppedWriterCapabilityCoordinator(),
    });
    let secondCalls = 0;
    await rejectedCapabilityError(
      () =>
        second.coordinator.consumeCapability(
          consumeOptions(second.writer, first.capability, {
            runSnapshot: async () => {
              secondCalls += 1;
            },
          }),
        ),
      "stopped_writer_capability_rejected",
    );
    assert.equal(secondCalls, 0);
    await second.coordinator.consumeCapability(
      consumeOptions(second.writer, second.capability),
    );
    await first.coordinator.consumeCapability(
      consumeOptions(first.writer, first.capability),
    );
  });
});

test("exact process, writer, stop-operation, attachment, lease, and handle bindings are enforced", async (t) => {
  const scenarios = [
    {
      name: "process incarnation",
      change: (options) => ({ ...options, processIncarnationId: "process-incarnation-002" }),
    },
    {
      name: "writer incarnation",
      change: (options) => ({ ...options, writerIncarnationId: "writer-incarnation-002" }),
    },
    {
      name: "stop operation",
      change: (options) => ({ ...options, stopOperationId: "stop-operation-002" }),
    },
    {
      name: "writer handle clone",
      change: (options) => ({ ...options, writer: structuredClone(options.writer) }),
    },
    {
      name: "backend",
      change: (options) => ({
        ...options,
        attachment: { ...options.attachment, backendId: "other-backend" },
      }),
    },
    {
      name: "storage",
      change: (options) => ({
        ...options,
        attachment: { ...options.attachment, storageId: "volume-002" },
      }),
    },
    {
      name: "attachment",
      change: (options) => ({
        ...options,
        attachment: { ...options.attachment, attachmentId: "attachment-002" },
      }),
    },
    {
      name: "attachment proof",
      change: (options) => ({
        ...options,
        attachment: { ...options.attachment, proofId: "proof-attachment-002" },
      }),
    },
    {
      name: "attachment operation",
      change: (options) => ({
        ...options,
        attachment: { ...options.attachment, operationId: "operation-attach-002" },
      }),
    },
    {
      name: "root path",
      change: (options) => ({
        ...options,
        attachment: {
          ...options.attachment,
          rootPath: "/var/lib/portable-codex/session-002",
        },
      }),
    },
    {
      name: "lease ID",
      change: (options) => ({
        ...options,
        canonicalLease: { ...options.canonicalLease, leaseId: "lease-002" },
      }),
    },
    {
      name: "holder ID",
      change: (options) => ({
        ...options,
        canonicalLease: { ...options.canonicalLease, holderId: "host-002" },
      }),
    },
    {
      name: "fencing epoch",
      change: (options) => ({
        ...options,
        canonicalLease: {
          ...options.canonicalLease,
          fencingEpoch: "9007199254740994",
        },
      }),
    },
    {
      name: "session ID",
      change: (options) => ({
        ...options,
        canonicalLease: { ...options.canonicalLease, sessionId: OTHER_SESSION_ID },
      }),
    },
  ];

  for (const scenario of scenarios) {
    await t.test(scenario.name, async () => {
      const fixture = await readyCapability();
      let snapshotCalls = 0;
      await rejectedCapabilityError(
        () =>
          fixture.coordinator.consumeCapability(
            scenario.change(
              consumeOptions(fixture.writer, fixture.capability, {
                runSnapshot: async () => {
                  snapshotCalls += 1;
                },
              }),
            ),
          ),
        "stopped_writer_capability_rejected",
      );
      assert.equal(snapshotCalls, 0);
      await rejectedCapabilityError(
        () =>
          fixture.coordinator.consumeCapability(
            consumeOptions(fixture.writer, fixture.capability),
          ),
        "stopped_writer_capability_rejected",
      );
    });
  }
});

test("invalid stop binding does not dispatch stop or mint a capability", async (t) => {
  for (const [name, overrides] of [
    ["process incarnation", { processIncarnationId: "process-incarnation-002" }],
    ["writer incarnation", { writerIncarnationId: "writer-incarnation-002" }],
    ["stop operation", { stopOperationId: "" }],
    ["writer lookalike", { writer: Object.freeze(Object.create(null)) }],
  ]) {
    await t.test(name, async () => {
      const coordinator = new StoppedWriterCapabilityCoordinator();
      let stopCalls = 0;
      const writer = coordinator.registerWriter(
        registerOptions({
          stopWriter: async () => {
            stopCalls += 1;
            return STOPPED_WRITER_STOP_CONFIRMED;
          },
        }),
      );
      await rejectedCapabilityError(
        () =>
          coordinator.stopAndIssueCapability(
            stopOptions(writer, overrides),
          ),
        overrides.stopOperationId === ""
          ? "invalid_stopped_writer_request"
          : "writer_state_conflict",
      );
      assert.equal(stopCalls, 0);
      const capability = await coordinator.stopAndIssueCapability(stopOptions(writer));
      assertOpaqueHandle(capability);
    });
  }
});

test("registration snapshots caller-owned attachment and lease records", async () => {
  const coordinator = new StoppedWriterCapabilityCoordinator();
  const originalLease = lease();
  const originalAttachment = attachment(originalLease);
  const expectedLease = structuredClone(originalLease);
  const expectedAttachment = structuredClone(originalAttachment);
  const writer = coordinator.registerWriter(
    registerOptions({
      attachment: originalAttachment,
      canonicalLease: originalLease,
    }),
  );
  originalLease.fencingEpoch = "9007199254740994";
  originalLease.leaseId = "mutated-lease";
  originalAttachment.rootPath = "/mutated/path";
  originalAttachment.proofId = "mutated-proof";

  const capability = await coordinator.stopAndIssueCapability(stopOptions(writer));
  let snapshotCalls = 0;
  await coordinator.consumeCapability(
    consumeOptions(writer, capability, {
      attachment: expectedAttachment,
      canonicalLease: expectedLease,
      runSnapshot: async () => {
        snapshotCalls += 1;
      },
    }),
  );
  assert.equal(snapshotCalls, 1);
});

test("registration owns a frozen binding snapshot despite poisoned clone and freeze globals", async () => {
  const originalStructuredClone = globalThis.structuredClone;
  const originalFreeze = Object.freeze;

  const registerFixture = () => {
    const coordinator = new StoppedWriterCapabilityCoordinator();
    const callerLease = lease();
    const callerAttachment = attachment(callerLease);
    const expectedLease = originalStructuredClone(callerLease);
    const expectedAttachment = originalStructuredClone(callerAttachment);
    const observed = { snapshotBinding: null, stopBinding: null };
    let writer;

    try {
      globalThis.structuredClone = (value) => value;
      Object.freeze = (value) => value;
      writer = coordinator.registerWriter(
        registerOptions({
          attachment: callerAttachment,
          canonicalLease: callerLease,
          stopWriter(binding) {
            observed.stopBinding = binding;
            return Promise.resolve(STOPPED_WRITER_STOP_CONFIRMED);
          },
        }),
      );
    } finally {
      globalThis.structuredClone = originalStructuredClone;
      Object.freeze = originalFreeze;
    }

    return {
      callerAttachment,
      callerLease,
      coordinator,
      expectedAttachment,
      expectedLease,
      observed,
      writer,
    };
  };

  const mutateCallerBinding = (fixture) => {
    fixture.callerLease.fencingEpoch = "9007199254740994";
    fixture.callerLease.holderId = "host-mutated";
    fixture.callerLease.leaseId = "lease-mutated";
    fixture.callerAttachment.attachmentId = "attachment-mutated";
    fixture.callerAttachment.backendId = "backend-mutated";
    fixture.callerAttachment.fencingEpoch = "9007199254740994";
    fixture.callerAttachment.holderId = "host-mutated";
    fixture.callerAttachment.leaseId = "lease-mutated";
    fixture.callerAttachment.operationId = "operation-mutated";
    fixture.callerAttachment.proofId = "proof-mutated";
    fixture.callerAttachment.rootPath = "/var/lib/portable-codex/mutated";
    fixture.callerAttachment.storageId = "storage-mutated";
  };

  const assertOriginalFrozenBinding = (binding, fixture) => {
    assert.notEqual(binding, null);
    assert.equal(Object.isFrozen(binding), true);
    assert.equal(Object.isFrozen(binding.attachment), true);
    assert.equal(Object.isFrozen(binding.writerFence), true);
    assert.equal(Object.getPrototypeOf(binding.attachment), null);
    assert.notStrictEqual(binding.attachment, fixture.callerAttachment);
    assert.deepEqual({ ...binding.attachment }, fixture.expectedAttachment);
    assert.deepEqual(
      { ...binding.writerFence },
      {
        contractVersion: fixture.expectedLease.contractVersion,
        fencingEpoch: fixture.expectedLease.fencingEpoch,
        holderId: fixture.expectedLease.holderId,
        leaseId: fixture.expectedLease.leaseId,
        sessionId: fixture.expectedLease.sessionId,
      },
    );
    assert.throws(() => {
      binding.attachment.rootPath = "/binding-mutation-must-fail";
    }, TypeError);
  };

  const accepted = registerFixture();
  mutateCallerBinding(accepted);
  const acceptedCapability = await accepted.coordinator.stopAndIssueCapability(
    stopOptions(accepted.writer),
  );
  assertOriginalFrozenBinding(accepted.observed.stopBinding, accepted);
  const result = await accepted.coordinator.consumeCapability(
    consumeOptions(accepted.writer, acceptedCapability, {
      attachment: accepted.expectedAttachment,
      canonicalLease: accepted.expectedLease,
      runSnapshot(binding) {
        accepted.observed.snapshotBinding = binding;
        return "captured";
      },
    }),
  );
  assert.equal(result, "captured");
  assertOriginalFrozenBinding(accepted.observed.snapshotBinding, accepted);

  const rejected = registerFixture();
  mutateCallerBinding(rejected);
  const rejectedCapability = await rejected.coordinator.stopAndIssueCapability(
    stopOptions(rejected.writer),
  );
  assertOriginalFrozenBinding(rejected.observed.stopBinding, rejected);
  let snapshotCalls = 0;
  await rejectedCapabilityError(
    () =>
      rejected.coordinator.consumeCapability(
        consumeOptions(rejected.writer, rejectedCapability, {
          attachment: rejected.callerAttachment,
          canonicalLease: rejected.callerLease,
          runSnapshot() {
            snapshotCalls += 1;
          },
        }),
      ),
    "stopped_writer_capability_rejected",
  );
  assert.equal(snapshotCalls, 0);
});

test("disposal reentrancy during registration validation remains terminal", async (t) => {
  const originalStructuredClone = globalThis.structuredClone;

  for (const [name, disposeOnCloneCall] of [
    ["attachment validation", 1],
    ["lease validation", 2],
  ]) {
    await t.test(name, () => {
      const coordinator = new StoppedWriterCapabilityCoordinator();
      let disposeCalls = 0;
      let outerWriter;
      let poisonCalls = 0;
      let stopCalls = 0;

      try {
        globalThis.structuredClone = (value) => {
          poisonCalls += 1;
          if (poisonCalls === disposeOnCloneCall) {
            assert.equal(coordinator.dispose(), undefined);
            disposeCalls += 1;
            globalThis.structuredClone = originalStructuredClone;
          }
          return originalStructuredClone(value);
        };
        syncCapabilityError(
          () => {
            outerWriter = coordinator.registerWriter(
              registerOptions({
                stopWriter: () => {
                  stopCalls += 1;
                  return STOPPED_WRITER_STOP_CONFIRMED;
                },
              }),
            );
          },
          "writer_state_conflict",
        );
      } finally {
        globalThis.structuredClone = originalStructuredClone;
      }

      assert.equal(poisonCalls, disposeOnCloneCall);
      assert.equal(disposeCalls, 1);
      assert.equal(outerWriter, undefined);
      assert.equal(stopCalls, 0);
      syncCapabilityError(
        () => coordinator.registerWriter(registerOptions()),
        "writer_state_conflict",
      );
      syncCapabilityError(
        () => coordinator.dispose(),
        "writer_state_conflict",
      );
      assert.equal(stopCalls, 0);
    });
  }
});

test("public option envelopes reject hostile values without invoking traps", async (t) => {
  let traps = 0;
  const accessor = Object.defineProperty({}, "attachment", {
    enumerable: true,
    get() {
      traps += 1;
      throw new Error("accessor must not run");
    },
  });
  const proxy = new Proxy(
    {},
    {
      get() {
        traps += 1;
        throw new Error("proxy must not run");
      },
      getOwnPropertyDescriptor() {
        traps += 1;
        throw new Error("proxy must not run");
      },
      getPrototypeOf() {
        traps += 1;
        throw new Error("proxy must not run");
      },
      ownKeys() {
        traps += 1;
        throw new Error("proxy must not run");
      },
    },
  );
  const revoked = Proxy.revocable({}, {});
  revoked.revoke();

  const invalidEnvelopes = [
    null,
    undefined,
    [],
    "invalid",
    accessor,
    proxy,
    revoked.proxy,
    { ...registerOptions(), unexpected: true },
    { ...registerOptions(), [Symbol("authority")]: true },
    Object.create(registerOptions()),
  ];

  for (const [index, value] of invalidEnvelopes.entries()) {
    await t.test(`register ${index}`, () => {
      const coordinator = new StoppedWriterCapabilityCoordinator();
      syncCapabilityError(
        () => coordinator.registerWriter(value),
        "invalid_stopped_writer_request",
      );
    });
  }
  assert.equal(traps, 0);

  await t.test("async entry points", async () => {
    const coordinator = new StoppedWriterCapabilityCoordinator();
    let stopCalls = 0;
    const writer = coordinator.registerWriter(
      registerOptions({
        stopWriter: async () => {
          stopCalls += 1;
          return STOPPED_WRITER_STOP_CONFIRMED;
        },
      }),
    );
    for (const value of [null, [], accessor, proxy, revoked.proxy]) {
      await rejectedCapabilityError(
        () => coordinator.stopAndIssueCapability(value),
        "invalid_stopped_writer_request",
      );
    }
    assert.equal(stopCalls, 0);
    const capability = await coordinator.stopAndIssueCapability(stopOptions(writer));
    for (const value of [null, [], accessor, proxy, revoked.proxy]) {
      await rejectedCapabilityError(
        () => coordinator.consumeCapability(value),
        "invalid_stopped_writer_request",
      );
    }
    await coordinator.consumeCapability(consumeOptions(writer, capability));
  });
  assert.equal(traps, 0);
});

test("revoked writer proxies remain inside the sanitized public error boundary", async () => {
  const coordinator = new StoppedWriterCapabilityCoordinator();
  const revoked = Proxy.revocable({}, {});
  revoked.revoke();

  await rejectedCapabilityError(
    () =>
      coordinator.stopAndIssueCapability(
        stopOptions(revoked.proxy),
      ),
    "writer_state_conflict",
  );
  syncCapabilityError(
    () => coordinator.revokeWriter(writerOptions(revoked.proxy)),
    "writer_state_conflict",
  );
  syncCapabilityError(
    () => coordinator.retireWriter(writerOptions(revoked.proxy)),
    "writer_state_conflict",
  );
  assert.equal(coordinator.dispose(), undefined);
});

test("registration rejects malformed identities, callbacks, and mismatched attachment fences", async (t) => {
  const invalidRegistrations = [
    registerOptions({ processIncarnationId: "" }),
    registerOptions({ writerIncarnationId: "" }),
    registerOptions({ processIncarnationId: null }),
    registerOptions({ writerIncarnationId: {} }),
    registerOptions({ stopWriter: null }),
    registerOptions({ stopWriter: {} }),
    registerOptions({ stopWriter: function* stopWriter() {} }),
    registerOptions({ stopWriter: async function* stopWriter() {} }),
    registerOptions({ canonicalLease: lease({ leaseId: "lease-002" }) }),
    registerOptions({ canonicalLease: lease({ holderId: "host-002" }) }),
    registerOptions({ canonicalLease: lease({ fencingEpoch: "9007199254740994" }) }),
    registerOptions({ canonicalLease: lease({ sessionId: OTHER_SESSION_ID }) }),
  ];
  for (const [index, options] of invalidRegistrations.entries()) {
    await t.test(String(index), () => {
      syncCapabilityError(
        () => new StoppedWriterCapabilityCoordinator().registerWriter(options),
        "invalid_stopped_writer_request",
      );
    });
  }
});

test("invalid consume inputs never dispatch the snapshot callback", async (t) => {
  const scenarios = [
    ["missing runSnapshot", (options) => {
      const changed = { ...options };
      delete changed.runSnapshot;
      return changed;
    }],
    ["non-function runSnapshot", (options) => ({ ...options, runSnapshot: {} })],
    ["extra field", (options) => ({ ...options, unexpected: true })],
    ["symbol field", (options) => ({ ...options, [Symbol("authority")]: true })],
    ["invalid process ID", (options) => ({ ...options, processIncarnationId: "" })],
    ["invalid stop operation", (options) => ({ ...options, stopOperationId: "" })],
  ];
  for (const [name, change] of scenarios) {
    await t.test(name, async () => {
      const fixture = await readyCapability();
      let snapshotCalls = 0;
      await rejectedCapabilityError(
        () =>
          fixture.coordinator.consumeCapability(
            change(
              consumeOptions(fixture.writer, fixture.capability, {
                runSnapshot: async () => {
                  snapshotCalls += 1;
                },
              }),
            ),
          ),
        ["missing runSnapshot", "extra field", "symbol field"].includes(name)
          ? "invalid_stopped_writer_request"
          : "stopped_writer_capability_rejected",
      );
      assert.equal(snapshotCalls, 0);
    });
  }
});

test("callback reentrancy cannot publish authority after revocation", async () => {
  const stopCoordinator = new StoppedWriterCapabilityCoordinator();
  let stopWriter;
  let stopRetireErrors = 0;
  stopWriter = stopCoordinator.registerWriter(
    registerOptions({
      stopWriter: async () => {
        stopCoordinator.revokeWriter(writerOptions(stopWriter));
        syncCapabilityError(
          () => stopCoordinator.retireWriter(writerOptions(stopWriter)),
          "writer_state_conflict",
        );
        stopRetireErrors += 1;
        return STOPPED_WRITER_STOP_CONFIRMED;
      },
    }),
  );
  await rejectedCapabilityError(
    () => stopCoordinator.stopAndIssueCapability(stopOptions(stopWriter)),
    "writer_state_conflict",
  );
  assert.equal(stopRetireErrors, 1);

  const snapshotFixture = await readyCapability();
  let snapshotRetireErrors = 0;
  await rejectedCapabilityError(
    () =>
      snapshotFixture.coordinator.consumeCapability(
        consumeOptions(snapshotFixture.writer, snapshotFixture.capability, {
          runSnapshot: async () => {
            snapshotFixture.coordinator.revokeWriter(
              writerOptions(snapshotFixture.writer),
            );
            syncCapabilityError(
              () =>
                snapshotFixture.coordinator.retireWriter(
                  writerOptions(snapshotFixture.writer),
                ),
              "writer_state_conflict",
            );
            snapshotRetireErrors += 1;
            return "captured";
          },
        }),
      ),
    "snapshot_outcome_uncertain",
  );
  assert.equal(snapshotRetireErrors, 1);
  await rejectedCapabilityError(
    () =>
      snapshotFixture.coordinator.consumeCapability(
        consumeOptions(snapshotFixture.writer, snapshotFixture.capability),
      ),
    "stopped_writer_capability_rejected",
  );
});

test("mismatched revoke and retire attempts cannot affect the current writer", async () => {
  const coordinator = new StoppedWriterCapabilityCoordinator();
  let stopCalls = 0;
  const writer = coordinator.registerWriter(
    registerOptions({
      stopWriter: async () => {
        stopCalls += 1;
        return STOPPED_WRITER_STOP_CONFIRMED;
      },
    }),
  );

  for (const operation of [
    () =>
      coordinator.revokeWriter(
        writerOptions(writer, { processIncarnationId: "process-incarnation-002" }),
      ),
    () =>
      coordinator.revokeWriter(
        writerOptions(writer, { writerIncarnationId: "writer-incarnation-002" }),
      ),
    () =>
      coordinator.retireWriter(
        writerOptions(structuredClone(writer)),
      ),
  ]) {
    syncCapabilityError(operation, "writer_state_conflict");
  }

  const capability = await coordinator.stopAndIssueCapability(stopOptions(writer));
  assert.equal(stopCalls, 1);
  await coordinator.consumeCapability(consumeOptions(writer, capability));
});

test("retire cannot release an active writer slot before stop authority settles", async () => {
  const coordinator = new StoppedWriterCapabilityCoordinator();
  const writer = coordinator.registerWriter(registerOptions());
  syncCapabilityError(
    () => coordinator.retireWriter(writerOptions(writer)),
    "writer_state_conflict",
  );

  const capability = await coordinator.stopAndIssueCapability(stopOptions(writer));
  syncCapabilityError(
    () => coordinator.retireWriter(writerOptions(writer)),
    "writer_state_conflict",
  );

  coordinator.revokeWriter(writerOptions(writer));
  coordinator.retireWriter(writerOptions(writer));
  await rejectedCapabilityError(
    () => coordinator.consumeCapability(consumeOptions(writer, capability)),
    "stopped_writer_capability_rejected",
  );
});
