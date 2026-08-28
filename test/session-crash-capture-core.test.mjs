import assert from "node:assert/strict";
import test from "node:test";

import {
  SessionCrashCaptureCoreError,
  capturePreparedAtomicCrashCheckpoint,
  prepareAtomicCrashCapture,
  verifyCommittedAtomicCrashCapture,
} from "../src/session-crash-capture-core.mjs";
import {
  ATOMIC_CRASH_CAPTURE_CONTRACT_VERSION,
  SessionStorageContractError,
} from "../src/session-storage-contracts.mjs";

const SESSION_ID = "019f2100-0000-7000-8000-000000000001";
const THREAD_ID = "019f2100-0000-7000-8000-000000000002";
const IMAGE_DIGEST = `sha256:${"a".repeat(64)}`;
const CONTENT_SHA256 = "b".repeat(64);

function storageRef(overrides = {}) {
  return {
    contractVersion: 1,
    backendId: "atomic-crash-test",
    storageId: "volume-001",
    sessionId: SESSION_ID,
    ...overrides,
  };
}

function sourceAttachment(overrides = {}) {
  return {
    contractVersion: 1,
    backendId: "atomic-crash-test",
    storageId: "volume-001",
    sessionId: SESSION_ID,
    attachmentId: "attachment-001",
    leaseId: "lease-001",
    holderId: "host-001",
    fencingEpoch: "11",
    operationId: "operation-attach-001",
    proofId: "proof-attachment-001",
    kind: "directory",
    rootPath: "/var/lib/portable-codex/session-001",
    mode: "read-write",
    ...overrides,
  };
}

function checkpoint(overrides = {}) {
  return {
    contractVersion: 1,
    checkpointId: "checkpoint-001",
    artifactId: "artifact-001",
    backendId: "atomic-crash-test",
    storageId: "volume-001",
    sessionId: SESSION_ID,
    codexThreadId: THREAD_ID,
    codexSessionId: THREAD_ID,
    imageDigest: IMAGE_DIGEST,
    sourceFencingEpoch: "11",
    checkpointClass: "crash-prefix",
    createdAt: "2026-08-28T12:00:00.000Z",
    ...overrides,
  };
}

function mutationRequest(overrides = {}) {
  return {
    contractVersion: 1,
    backendId: "atomic-crash-test",
    storageId: "volume-001",
    sessionId: SESSION_ID,
    leaseId: "lease-001",
    holderId: "host-001",
    fencingEpoch: "11",
    operation: "checkpoint",
    operationId: "operation-checkpoint-001",
    target: {
      artifactId: "artifact-001",
      checkpointId: "checkpoint-001",
      kind: "checkpoint",
    },
    ...overrides,
  };
}

function captureRequest(overrides = {}) {
  return {
    captureAttemptId: "capture-attempt-001",
    checkpoint: checkpoint(),
    contractVersion: ATOMIC_CRASH_CAPTURE_CONTRACT_VERSION,
    mutationRequest: mutationRequest(),
    sourceAttachment: sourceAttachment(),
    storageRef: storageRef(),
    ...overrides,
  };
}

function committedResult(request, overrides = {}) {
  const { artifact: artifactOverrides = {}, ...resultOverrides } = overrides;
  const artifact = {
    byteLength: "4096",
    contentSha256: CONTENT_SHA256,
    objectId: "snapshot-object-001",
    objectIdentityScheme: "test-atomic-snapshot-v1",
    readOnly: true,
    ...artifactOverrides,
  };
  return {
    artifact,
    artifactId: request.checkpoint.artifactId,
    backendId: request.storageRef.backendId,
    captureAttemptId: request.captureAttemptId,
    checkpointId: request.checkpoint.checkpointId,
    contractVersion: ATOMIC_CRASH_CAPTURE_CONTRACT_VERSION,
    operationId: request.mutationRequest.operationId,
    proofId: "proof-atomic-capture-001",
    sessionId: request.storageRef.sessionId,
    sourceFencingEpoch: request.checkpoint.sourceFencingEpoch,
    status: "committed",
    storageId: request.storageRef.storageId,
    ...resultOverrides,
    artifact,
  };
}

function committedVerification(request, result = committedResult(request)) {
  return {
    contractVersion: ATOMIC_CRASH_CAPTURE_CONTRACT_VERSION,
    outcome: "committed",
    result,
  };
}

function unknownVerification() {
  return {
    contractVersion: ATOMIC_CRASH_CAPTURE_CONTRACT_VERSION,
    outcome: "unknown",
    result: null,
  };
}

function createBackend({ capture, verify } = {}) {
  const calls = {
    capture: [],
    captureReceivers: [],
    verify: [],
    verifyReceivers: [],
  };
  const operation = async () => {};
  const backend = {
    contractVersion: 1,
    backendId: "atomic-crash-test",
    capabilities: {
      atomicPointInTimeCheckpoint: true,
      exclusiveWriterAttachment: true,
      fencing: "epoch-enforced",
      normalDirectoryAttachment: true,
    },
    atomicCrashCaptureContractVersion:
      ATOMIC_CRASH_CAPTURE_CONTRACT_VERSION,
    async captureAtomicCrashCheckpoint(input) {
      calls.capture.push(input);
      calls.captureReceivers.push(this);
      if (capture) return capture.call(this, input);
      return committedResult(input.request);
    },
    async verifyCommittedAtomicCrashCheckpoint(input) {
      calls.verify.push(input);
      calls.verifyReceivers.push(this);
      if (verify) return verify.call(this, input);
      return committedVerification(input);
    },
    captureCheckpoint: operation,
    destroySession: operation,
    detachAttachment: operation,
    forceFence: operation,
    prepareWritableAttachment: operation,
    provisionSession: operation,
    restoreCheckpoint: operation,
  };
  return { backend, calls };
}

function assertCoreCode(code) {
  return (error) =>
    error instanceof SessionCrashCaptureCoreError &&
    error.code === code &&
    error.retryable === false &&
    Object.isFrozen(error) &&
    !Object.hasOwn(error, "cause") &&
    !Object.hasOwn(error, "details");
}

function assertContractFailure(error) {
  return (
    error instanceof SessionStorageContractError &&
    !error.message.includes("secret") &&
    !error.message.includes("forged")
  );
}

test("prepared atomic capture freezes one request identity and dispatches exact authority", async () => {
  let backendResponse;
  const instance = createBackend({
    capture(input) {
      assert.strictEqual(this, instance.backend);
      assert(Object.isFrozen(input));
      assert.deepEqual(Object.keys(input).sort(), [
        "captureAuthority",
        "request",
      ]);
      backendResponse = committedResult(input.request);
      return backendResponse;
    },
  });
  const callerRequest = captureRequest();
  const preparedCapture = prepareAtomicCrashCapture({
    backend: instance.backend,
    request: callerRequest,
  });
  let authorityAccessorReads = 0;
  const captureAuthority = {};
  Object.defineProperty(captureAuthority, "providerSecret", {
    enumerable: true,
    get() {
      authorityAccessorReads += 1;
      throw new Error("opaque capture authority must not be inspected");
    },
  });
  Object.freeze(captureAuthority);

  assert(Object.isFrozen(preparedCapture));
  assert.deepEqual(Object.keys(preparedCapture).sort(), ["backendId", "request"]);
  assert.equal(preparedCapture.backendId, instance.backend.backendId);
  assert.notStrictEqual(preparedCapture.request, callerRequest);
  assert.deepEqual(preparedCapture.request, callerRequest);
  assert(Object.isFrozen(preparedCapture.request));
  assert(Object.isFrozen(preparedCapture.request.checkpoint));
  assert(Object.isFrozen(preparedCapture.request.mutationRequest));
  assert(Object.isFrozen(preparedCapture.request.mutationRequest.target));
  assert(Object.isFrozen(preparedCapture.request.sourceAttachment));
  assert(Object.isFrozen(preparedCapture.request.storageRef));

  const result = await capturePreparedAtomicCrashCheckpoint({
    captureAuthority,
    preparedCapture,
  });

  assert.equal(instance.calls.capture.length, 1);
  assert.strictEqual(instance.calls.captureReceivers[0], instance.backend);
  assert.strictEqual(instance.calls.capture[0].captureAuthority, captureAuthority);
  assert.strictEqual(instance.calls.capture[0].request, preparedCapture.request);
  assert.equal(authorityAccessorReads, 0);
  assert.deepEqual(result, committedResult(preparedCapture.request));
  assert(Object.isFrozen(result));
  assert(Object.isFrozen(result.artifact));
  assert.notStrictEqual(result, backendResponse);
  assert.notStrictEqual(result.artifact, backendResponse.artifact);
});

test("prepared token is object-authenticated and synchronously consumed before dispatch", async () => {
  let preparedCapture;
  let reentrantFailure;
  const captureAuthority = Object.freeze({ authority: "opaque" });
  const instance = createBackend({
    async capture(input) {
      try {
        await capturePreparedAtomicCrashCheckpoint({
          captureAuthority,
          preparedCapture,
        });
      } catch (error) {
        reentrantFailure = error;
      }
      return committedResult(input.request);
    },
  });
  preparedCapture = prepareAtomicCrashCapture({
    backend: instance.backend,
    request: captureRequest(),
  });

  const forgedToken = Object.freeze({ ...preparedCapture });
  await assert.rejects(
    () =>
      capturePreparedAtomicCrashCheckpoint({
        captureAuthority,
        preparedCapture: forgedToken,
      }),
    assertContractFailure,
  );
  assert.equal(instance.calls.capture.length, 0);

  let proxyTraps = 0;
  const proxyToken = new Proxy(forgedToken, {
    getPrototypeOf() {
      proxyTraps += 1;
      throw new Error("secret prepared token proxy detail");
    },
    ownKeys() {
      proxyTraps += 1;
      throw new Error("secret prepared token proxy detail");
    },
  });
  const revokedToken = Proxy.revocable({}, {});
  revokedToken.revoke();
  for (const candidate of [proxyToken, revokedToken.proxy]) {
    await assert.rejects(
      () =>
        capturePreparedAtomicCrashCheckpoint({
          captureAuthority,
          preparedCapture: candidate,
        }),
      assertContractFailure,
    );
  }
  assert.equal(proxyTraps, 0);
  assert.equal(instance.calls.capture.length, 0);

  await capturePreparedAtomicCrashCheckpoint({
    captureAuthority,
    preparedCapture,
  });
  assert.equal(instance.calls.capture.length, 1);
  assert(assertContractFailure(reentrantFailure));

  await assert.rejects(
    () =>
      capturePreparedAtomicCrashCheckpoint({
        captureAuthority,
        preparedCapture,
      }),
    assertContractFailure,
  );
  assert.equal(instance.calls.capture.length, 1);
});

test("capture authority must be an opaque non-proxy object before token consumption", async (t) => {
  const revoked = Proxy.revocable({}, {});
  revoked.revoke();
  let proxyTraps = 0;
  const hostileProxy = new Proxy(
    {},
    {
      getPrototypeOf() {
        proxyTraps += 1;
        throw new Error("secret authority proxy detail");
      },
      ownKeys() {
        proxyTraps += 1;
        throw new Error("secret authority proxy detail");
      },
    },
  );

  for (const [name, captureAuthority] of [
    ["null", null],
    ["array", []],
    ["function", () => {}],
    ["proxy", hostileProxy],
    ["revoked proxy", revoked.proxy],
  ]) {
    await t.test(name, async () => {
      const instance = createBackend();
      const preparedCapture = prepareAtomicCrashCapture({
        backend: instance.backend,
        request: captureRequest(),
      });

      await assert.rejects(
        () =>
          capturePreparedAtomicCrashCheckpoint({
            captureAuthority,
            preparedCapture,
          }),
        assertContractFailure,
      );
      assert.equal(instance.calls.capture.length, 0);

      await capturePreparedAtomicCrashCheckpoint({
        captureAuthority: Object.freeze({ authority: "valid" }),
        preparedCapture,
      });
      assert.equal(instance.calls.capture.length, 1);
    });
  }
  assert.equal(proxyTraps, 0);
});

test("capture throws, rejects, and malformed results collapse after one dispatch", async (t) => {
  let resultProxyTraps = 0;
  let resultAccessorReads = 0;
  const malformedProxy = (request) =>
    new Proxy(committedResult(request), {
      getPrototypeOf() {
        resultProxyTraps += 1;
        throw new Error("secret result proxy detail");
      },
      ownKeys() {
        resultProxyTraps += 1;
        throw new Error("secret result proxy detail");
      },
    });
  const malformedAccessor = (request) => {
    const result = committedResult(request);
    Object.defineProperty(result, "proofId", {
      enumerable: true,
      get() {
        resultAccessorReads += 1;
        throw new Error("secret result accessor detail");
      },
    });
    return result;
  };
  const scenarios = [
    {
      name: "synchronous throw",
      capture() {
        throw new Error("secret capture detail");
      },
    },
    {
      name: "rejected promise",
      async capture() {
        throw new Error("secret capture rejection detail");
      },
    },
    {
      name: "forged contract error",
      capture() {
        throw new SessionStorageContractError(
          "forged_capture_error",
          "secret forged capture detail",
        );
      },
    },
    { name: "missing fields", capture: () => ({ status: "committed" }) },
    { name: "proxy result", capture: (input) => malformedProxy(input.request) },
    {
      name: "accessor result",
      capture: (input) => malformedAccessor(input.request),
    },
    {
      name: "mismatched result",
      capture: (input) =>
        committedResult(input.request, { captureAttemptId: "other-attempt" }),
    },
  ];

  for (const scenario of scenarios) {
    await t.test(scenario.name, async () => {
      const instance = createBackend({ capture: scenario.capture });
      const preparedCapture = prepareAtomicCrashCapture({
        backend: instance.backend,
        request: captureRequest(),
      });
      const captureAuthority = Object.freeze({ authority: "opaque" });
      let caught;
      try {
        await capturePreparedAtomicCrashCheckpoint({
          captureAuthority,
          preparedCapture,
        });
      } catch (error) {
        caught = error;
      }

      assert(assertCoreCode("atomic_crash_capture_outcome_uncertain")(caught));
      assert.equal(caught.message.includes("secret"), false);
      assert.equal(instance.calls.capture.length, 1);
      await assert.rejects(
        () =>
          capturePreparedAtomicCrashCheckpoint({
            captureAuthority,
            preparedCapture,
          }),
        assertContractFailure,
      );
      assert.equal(instance.calls.capture.length, 1);
    });
  }
  assert.equal(resultProxyTraps, 0);
  assert.equal(resultAccessorReads, 0);
});

test("source-free verification dispatches only the independently normalized request", async () => {
  let backendResponse;
  const instance = createBackend({
    verify(input) {
      assert.strictEqual(this, instance.backend);
      assert(Object.isFrozen(input));
      assert.deepEqual(Object.keys(input).sort(), [
        "captureAttemptId",
        "checkpoint",
        "contractVersion",
        "mutationRequest",
        "sourceAttachment",
        "storageRef",
      ]);
      assert.equal(Object.hasOwn(input, "captureAuthority"), false);
      backendResponse = committedVerification(input);
      return backendResponse;
    },
  });
  const callerRequest = captureRequest();
  const result = await verifyCommittedAtomicCrashCapture({
    backend: instance.backend,
    request: callerRequest,
  });

  assert.equal(instance.calls.verify.length, 1);
  assert.strictEqual(instance.calls.verifyReceivers[0], instance.backend);
  assert.notStrictEqual(instance.calls.verify[0], callerRequest);
  assert.deepEqual(instance.calls.verify[0], callerRequest);
  assert(Object.isFrozen(instance.calls.verify[0]));
  assert.deepEqual(result, committedVerification(callerRequest));
  assert(Object.isFrozen(result));
  assert(Object.isFrozen(result.result));
  assert(Object.isFrozen(result.result.artifact));
  assert.notStrictEqual(result, backendResponse);
  assert.notStrictEqual(result.result, backendResponse.result);
});

test("source-free verification accepts only the exact unknown result without capture authority", async () => {
  const instance = createBackend({
    verify(input) {
      assert.equal(input.contractVersion, ATOMIC_CRASH_CAPTURE_CONTRACT_VERSION);
      assert.equal(Object.hasOwn(input, "captureAuthority"), false);
      return unknownVerification();
    },
  });
  const result = await verifyCommittedAtomicCrashCapture({
    backend: instance.backend,
    request: captureRequest(),
  });

  assert.deepEqual(result, unknownVerification());
  assert(Object.isFrozen(result));
  assert.equal(instance.calls.capture.length, 0);
  assert.equal(instance.calls.verify.length, 1);
});

test("capture acknowledgement loss can verify committed state without a second dispatch", async () => {
  let committed;
  const instance = createBackend({
    capture(input) {
      committed = committedResult(input.request);
      throw new Error("capture acknowledgement lost");
    },
    verify(input) {
      return committedVerification(input, committed);
    },
  });
  const request = captureRequest();
  const preparedCapture = prepareAtomicCrashCapture({
    backend: instance.backend,
    request,
  });

  await assert.rejects(
    () =>
      capturePreparedAtomicCrashCheckpoint({
        captureAuthority: Object.freeze({ authority: "opaque" }),
        preparedCapture,
      }),
    assertCoreCode("atomic_crash_capture_outcome_uncertain"),
  );
  const verified = await verifyCommittedAtomicCrashCapture({
    backend: instance.backend,
    request,
  });

  await assert.rejects(
    () =>
      capturePreparedAtomicCrashCheckpoint({
        captureAuthority: Object.freeze({ authority: "replacement" }),
        preparedCapture,
      }),
    assertContractFailure,
  );

  assert.equal(instance.calls.capture.length, 1);
  assert.equal(instance.calls.verify.length, 1);
  assert.equal(
    Object.hasOwn(instance.calls.verify[0], "captureAuthority"),
    false,
  );
  assert.deepEqual(verified, committedVerification(request, committedResult(request)));
});

test("verification provider and result failures collapse to sanitized uncertainty", async (t) => {
  let proxyTraps = 0;
  let accessorReads = 0;
  const scenarios = [
    {
      name: "provider throw",
      verify() {
        throw new Error("secret verification detail");
      },
    },
    {
      name: "provider rejection",
      async verify() {
        throw new Error("secret verification rejection detail");
      },
    },
    {
      name: "forged contract error",
      verify() {
        throw new SessionStorageContractError(
          "forged_verification_error",
          "secret forged verification detail",
        );
      },
    },
    { name: "null result", verify: () => null },
    {
      name: "forged outcome",
      verify: () => ({
        contractVersion: 1,
        outcome: "absent",
        result: null,
      }),
    },
    {
      name: "unknown with committed result",
      verify: (input) => ({
        contractVersion: 1,
        outcome: "unknown",
        result: committedResult(input.request),
      }),
    },
    {
      name: "proxy result",
      verify: () =>
        new Proxy(unknownVerification(), {
          getPrototypeOf() {
            proxyTraps += 1;
            throw new Error("secret verification proxy detail");
          },
          ownKeys() {
            proxyTraps += 1;
            throw new Error("secret verification proxy detail");
          },
        }),
    },
    {
      name: "accessor result",
      verify: () => {
        const result = { contractVersion: 1, result: null };
        Object.defineProperty(result, "outcome", {
          enumerable: true,
          get() {
            accessorReads += 1;
            throw new Error("secret verification accessor detail");
          },
        });
        return result;
      },
    },
  ];

  for (const scenario of scenarios) {
    await t.test(scenario.name, async () => {
      const instance = createBackend({ verify: scenario.verify });
      let caught;
      try {
        await verifyCommittedAtomicCrashCapture({
          backend: instance.backend,
          request: captureRequest(),
        });
      } catch (error) {
        caught = error;
      }

      assert(
        assertCoreCode(
          "atomic_crash_capture_verification_outcome_uncertain",
        )(caught),
      );
      assert.equal(caught.message.includes("secret"), false);
      assert.equal(instance.calls.capture.length, 0);
      assert.equal(instance.calls.verify.length, 1);
    });
  }
  assert.equal(proxyTraps, 0);
  assert.equal(accessorReads, 0);
});

test("accessor and proxy option envelopes fail closed before backend dispatch", async (t) => {
  for (const operation of ["prepare", "capture", "verify"]) {
    await t.test(operation, async () => {
      const instance = createBackend();
      const preparedCapture = prepareAtomicCrashCapture({
        backend: instance.backend,
        request: captureRequest(),
      });
      const validOptions =
        operation === "prepare"
          ? { backend: createBackend().backend, request: captureRequest() }
          : operation === "capture"
            ? {
                captureAuthority: Object.freeze({ authority: "opaque" }),
                preparedCapture,
              }
            : { backend: createBackend().backend, request: captureRequest() };
      const firstKey = Object.keys(validOptions)[0];
      let accessorReads = 0;
      const accessorOptions = { ...validOptions };
      Object.defineProperty(accessorOptions, firstKey, {
        enumerable: true,
        get() {
          accessorReads += 1;
          return validOptions[firstKey];
        },
      });
      const invoke =
        operation === "prepare"
          ? prepareAtomicCrashCapture
          : operation === "capture"
            ? capturePreparedAtomicCrashCheckpoint
            : verifyCommittedAtomicCrashCapture;

      await assert.rejects(
        async () => invoke(accessorOptions),
        assertContractFailure,
      );
      assert.equal(accessorReads, 0);

      let proxyTraps = 0;
      const proxyOptions = new Proxy(validOptions, {
        getPrototypeOf() {
          proxyTraps += 1;
          throw new Error("secret option proxy detail");
        },
        ownKeys() {
          proxyTraps += 1;
          throw new Error("secret option proxy detail");
        },
      });
      await assert.rejects(async () => invoke(proxyOptions), assertContractFailure);
      assert.equal(proxyTraps, 0);
      assert.equal(instance.calls.capture.length, 0);
      assert.equal(instance.calls.verify.length, 0);
    });
  }
});

test("preparation resists targeted WeakMap.set poisoning", { concurrency: false }, async () => {
  const instance = createBackend();
  const defineProperty = Object.defineProperty;
  const reflectApply = Reflect.apply;
  const setDescriptor = Object.getOwnPropertyDescriptor(
    WeakMap.prototype,
    "set",
  );
  const originalSet = setDescriptor.value;
  let poisonCalls = 0;
  let preparedCapture;

  defineProperty(WeakMap.prototype, "set", {
    ...setDescriptor,
    value(key, value) {
      if (
        value !== null &&
        typeof value === "object" &&
        value.state === "prepared" &&
        typeof value.capture === "function"
      ) {
        poisonCalls += 1;
        throw new Error("prepared atomic capture state intercepted");
      }
      return reflectApply(originalSet, this, [key, value]);
    },
  });
  try {
    preparedCapture = prepareAtomicCrashCapture({
      backend: instance.backend,
      request: captureRequest(),
    });
  } finally {
    defineProperty(WeakMap.prototype, "set", setDescriptor);
  }

  assert.equal(poisonCalls, 0);
  const result = await capturePreparedAtomicCrashCheckpoint({
    captureAuthority: Object.freeze({ authority: "opaque" }),
    preparedCapture,
  });
  assert.equal(instance.calls.capture.length, 1);
  assert.deepEqual(result, committedResult(preparedCapture.request));
});

test("prepared dispatch resists post-preparation intrinsic and method poisoning", { concurrency: false }, async () => {
  const instance = createBackend();
  const preparedCapture = prepareAtomicCrashCapture({
    backend: instance.backend,
    request: captureRequest(),
  });
  const captureAuthority = Object.freeze({ authority: "opaque" });
  const dispatchOptions = { captureAuthority, preparedCapture };
  const defineProperty = Object.defineProperty;
  const descriptors = {
    captureCall: Object.getOwnPropertyDescriptor(
      instance.backend.captureAtomicCrashCheckpoint,
      "call",
    ),
    getOwnPropertyDescriptor: Object.getOwnPropertyDescriptor(
      Object,
      "getOwnPropertyDescriptor",
    ),
    objectFreeze: Object.getOwnPropertyDescriptor(Object, "freeze"),
    reflectApply: Object.getOwnPropertyDescriptor(Reflect, "apply"),
    weakMapGet: Object.getOwnPropertyDescriptor(WeakMap.prototype, "get"),
    weakMapHas: Object.getOwnPropertyDescriptor(WeakMap.prototype, "has"),
    weakMapSet: Object.getOwnPropertyDescriptor(WeakMap.prototype, "set"),
  };
  const originalFreeze = descriptors.objectFreeze.value;
  const originalGetOwnPropertyDescriptor =
    descriptors.getOwnPropertyDescriptor.value;
  const poisonCalls = [];
  const poison = (name) =>
    function poisonedIntrinsic() {
      poisonCalls.push(name);
      throw new Error(`mutable intrinsic used after preparation: ${name}`);
    };
  const replace = (target, key, descriptor, value) => {
    defineProperty(target, key, { ...descriptor, value });
  };

  replace(
    Object,
    "getOwnPropertyDescriptor",
    descriptors.getOwnPropertyDescriptor,
    function poisonedGetOwnPropertyDescriptor(value, key) {
      if (value === dispatchOptions) {
        poisonCalls.push(`Object.getOwnPropertyDescriptor:${String(key)}`);
      }
      return originalGetOwnPropertyDescriptor(value, key);
    },
  );
  replace(
    Object,
    "freeze",
    descriptors.objectFreeze,
    function poisonedFreeze(value) {
      if (
        value !== null &&
        typeof value === "object" &&
        Object.hasOwn(value, "captureAuthority")
      ) {
        poisonCalls.push("Object.freeze");
      }
      return originalFreeze(value);
    },
  );
  replace(Reflect, "apply", descriptors.reflectApply, poison("Reflect.apply"));
  replace(
    WeakMap.prototype,
    "get",
    descriptors.weakMapGet,
    poison("WeakMap.prototype.get"),
  );
  replace(
    WeakMap.prototype,
    "has",
    descriptors.weakMapHas,
    poison("WeakMap.prototype.has"),
  );
  replace(
    WeakMap.prototype,
    "set",
    descriptors.weakMapSet,
    poison("WeakMap.prototype.set"),
  );
  defineProperty(instance.backend.captureAtomicCrashCheckpoint, "call", {
    configurable: true,
    enumerable: false,
    value: poison("captureAtomicCrashCheckpoint.call"),
    writable: true,
  });

  let failure;
  let replayFailure;
  let result;
  try {
    result = await capturePreparedAtomicCrashCheckpoint(dispatchOptions);
    try {
      await capturePreparedAtomicCrashCheckpoint(dispatchOptions);
    } catch (error) {
      replayFailure = error;
    }
  } catch (error) {
    failure = error;
  } finally {
    defineProperty(
      Object,
      "getOwnPropertyDescriptor",
      descriptors.getOwnPropertyDescriptor,
    );
    defineProperty(Object, "freeze", descriptors.objectFreeze);
    defineProperty(Reflect, "apply", descriptors.reflectApply);
    defineProperty(WeakMap.prototype, "get", descriptors.weakMapGet);
    defineProperty(WeakMap.prototype, "has", descriptors.weakMapHas);
    defineProperty(WeakMap.prototype, "set", descriptors.weakMapSet);
    if (descriptors.captureCall === undefined) {
      delete instance.backend.captureAtomicCrashCheckpoint.call;
    } else {
      defineProperty(
        instance.backend.captureAtomicCrashCheckpoint,
        "call",
        descriptors.captureCall,
      );
    }
  }

  assert.deepEqual(poisonCalls, []);
  if (failure !== undefined) throw failure;
  assert(assertContractFailure(replayFailure));
  assert.equal(instance.calls.capture.length, 1);
  assert.strictEqual(instance.calls.capture[0].captureAuthority, captureAuthority);
  assert.strictEqual(instance.calls.capture[0].request, preparedCapture.request);
  assert.deepEqual(result, committedResult(preparedCapture.request));
});
