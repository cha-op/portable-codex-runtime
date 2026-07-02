import assert from "node:assert/strict";
import test from "node:test";

import {
  SessionSnapshotCoreError,
  captureCleanCheckpoint,
  restoreCleanCheckpoint,
} from "../src/session-snapshot-core.mjs";
import {
  SessionStorageContractError,
  createSessionManifest,
} from "../src/session-storage-contracts.mjs";

const SESSION_ID = "019f2100-0000-7000-8000-000000000001";
const THREAD_ID = "019f2100-0000-7000-8000-000000000002";
const OTHER_SESSION_ID = "019f2100-0000-7000-8000-000000000099";
const IMAGE_DIGEST = `sha256:${"a".repeat(64)}`;
const NOW = Date.parse("2026-07-02T12:00:00.000Z");
const CREATED_AT = "2026-07-02T12:00:00.000Z";

function manifest(overrides = {}) {
  return createSessionManifest({
    sessionId: overrides.sessionId ?? SESSION_ID,
    codex: {
      rootThreadId: THREAD_ID,
      sessionId: THREAD_ID,
      ephemeral: false,
      historyMode: "paginated",
    },
    runtime: {
      imageDigest: IMAGE_DIGEST,
      imageMediaType: "application/vnd.oci.image.manifest.v1+json",
      platform: "linux/arm64",
      codexVersion: "codex-cli 0.142.4",
      codexSandbox: "danger-full-access",
    },
  });
}

function storageRef(overrides = {}) {
  return {
    contractVersion: 1,
    backendId: "single-attach-test",
    storageId: "volume-001",
    sessionId: SESSION_ID,
    ...overrides,
  };
}

function lease(overrides = {}) {
  return {
    contractVersion: 1,
    sessionId: SESSION_ID,
    leaseId: "lease-001",
    holderId: "host-001",
    fencingEpoch: "11",
    expiresAt: "2026-07-02T12:01:00.000Z",
    ...overrides,
  };
}

function attachment(writerLease = lease(), overrides = {}) {
  return {
    contractVersion: 1,
    backendId: "single-attach-test",
    storageId: "volume-001",
    sessionId: SESSION_ID,
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

function mutationRequest(operation, writerLease = lease(), overrides = {}) {
  return {
    contractVersion: 1,
    backendId: "single-attach-test",
    storageId: "volume-001",
    sessionId: SESSION_ID,
    leaseId: writerLease.leaseId,
    holderId: writerLease.holderId,
    fencingEpoch: writerLease.fencingEpoch,
    operation,
    operationId: `operation-${operation}-001`,
    target: {
      artifactId: "artifact-001",
      checkpointId: "checkpoint-001",
      kind: "checkpoint",
    },
    ...overrides,
  };
}

function checkpoint(overrides = {}) {
  return {
    contractVersion: 1,
    checkpointId: "checkpoint-001",
    artifactId: "artifact-001",
    backendId: "single-attach-test",
    storageId: "volume-001",
    sessionId: SESSION_ID,
    codexThreadId: THREAD_ID,
    codexSessionId: THREAD_ID,
    imageDigest: IMAGE_DIGEST,
    sourceFencingEpoch: "10",
    checkpointClass: "clean",
    createdAt: CREATED_AT,
    ...overrides,
  };
}

function mutationResult(request) {
  return {
    ...request,
    proofId: `proof-${request.operation}-001`,
    status: request.operation === "checkpoint" ? "checkpoint-created" : "restored",
  };
}

function backendCheckpointResult(input) {
  return {
    checkpoint: input.checkpoint,
    mutation: mutationResult(input.request),
  };
}

function createBackend({
  backendId = "single-attach-test",
  capture,
  restore,
} = {}) {
  const calls = { capture: [], restore: [] };
  const operation = async () => {};
  const backend = {
    contractVersion: 1,
    backendId,
    capabilities: {
      atomicPointInTimeCheckpoint: true,
      exclusiveWriterAttachment: true,
      fencing: "epoch-enforced",
      normalDirectoryAttachment: true,
    },
    async captureCheckpoint(input) {
      calls.capture.push(input);
      if (capture) return capture.call(this, input);
      return backendCheckpointResult(input);
    },
    destroySession: operation,
    detachAttachment: operation,
    forceFence: operation,
    prepareWritableAttachment: operation,
    provisionSession: operation,
    async restoreCheckpoint(input) {
      calls.restore.push(input);
      if (restore) return restore.call(this, input);
      return backendCheckpointResult(input);
    },
  };
  return { backend, calls };
}

function captureOptions(backend, overrides = {}) {
  const canonicalLease = overrides.canonicalLease ?? lease();
  return {
    backend,
    manifest: manifest(),
    storageRef: storageRef(),
    attachment: attachment(canonicalLease),
    canonicalLease,
    request: mutationRequest("checkpoint", canonicalLease),
    checkpointClass: "clean",
    createdAt: CREATED_AT,
    now: NOW,
    stoppedWriterEvidence: { handle: "trusted-stop-proof-001" },
    ...overrides,
  };
}

function restoreOptions(backend, overrides = {}) {
  const canonicalLease = overrides.canonicalLease ?? lease();
  return {
    backend,
    manifest: manifest(),
    storageRef: storageRef(),
    canonicalLease,
    request: mutationRequest("restore", canonicalLease),
    checkpoint: checkpoint(),
    now: NOW,
    ...overrides,
  };
}

function assertContractCode(code) {
  return (error) => error instanceof SessionStorageContractError && error.code === code;
}

function assertCoreCode(code) {
  return (error) =>
    error instanceof SessionSnapshotCoreError &&
    error.code === code &&
    error.retryable === false &&
    !Object.hasOwn(error, "cause") &&
    !Object.hasOwn(error, "details");
}

test("clean checkpoint capture dispatches exact frozen portable data", async () => {
  let backendResponse;
  const { backend, calls } = createBackend({
    capture(input) {
      assert.equal(this, backend);
      assert(Object.isFrozen(input));
      backendResponse = backendCheckpointResult(input);
      return backendResponse;
    },
  });
  const options = captureOptions(backend);
  const result = await captureCleanCheckpoint(options);

  assert.equal(calls.capture.length, 1);
  assert.deepEqual(calls.capture[0], {
    attachment: options.attachment,
    checkpoint: checkpoint({ sourceFencingEpoch: "11" }),
    request: options.request,
    stoppedWriterEvidence: options.stoppedWriterEvidence,
  });
  assert.strictEqual(
    calls.capture[0].stoppedWriterEvidence,
    options.stoppedWriterEvidence,
  );
  assert.equal(Object.isFrozen(options.stoppedWriterEvidence), false);
  assert.deepEqual(result, {
    checkpoint: checkpoint({ sourceFencingEpoch: "11" }),
    mutation: mutationResult(options.request),
  });
  assert(Object.isFrozen(result));
  assert(Object.isFrozen(result.checkpoint));
  assert(Object.isFrozen(result.mutation));
  assert(Object.isFrozen(result.mutation.target));
  assert.notStrictEqual(result.checkpoint, backendResponse.checkpoint);
  assert.notStrictEqual(result.mutation, backendResponse.mutation);

  const serialized = JSON.stringify(result.checkpoint);
  for (const forbidden of [
    options.attachment.rootPath,
    "rootPath",
    "authJson",
    "accessToken",
    "refreshToken",
    "gitSummary",
    "branch",
  ]) {
    assert.equal(serialized.includes(forbidden), false);
  }
});

test("clean checkpoint restore requires a newer lease and dispatches exact portable data", async () => {
  let backendResponse;
  const { backend, calls } = createBackend({
    restore(input) {
      assert.equal(this, backend);
      assert(Object.isFrozen(input));
      backendResponse = backendCheckpointResult(input);
      return backendResponse;
    },
  });
  const options = restoreOptions(backend);
  const result = await restoreCleanCheckpoint(options);

  assert.equal(calls.restore.length, 1);
  assert.deepEqual(calls.restore[0], {
    checkpoint: options.checkpoint,
    request: options.request,
  });
  assert.deepEqual(result, {
    checkpoint: options.checkpoint,
    mutation: mutationResult(options.request),
  });
  assert(Object.isFrozen(result));
  assert(Object.isFrozen(result.checkpoint));
  assert(Object.isFrozen(result.mutation));
  assert(Object.isFrozen(result.mutation.target));
  assert.notStrictEqual(result.checkpoint, backendResponse.checkpoint);
  assert.notStrictEqual(result.mutation, backendResponse.mutation);
});

test("clean checkpoint restore supports a replacement volume on the same backend", async () => {
  const { backend, calls } = createBackend();
  const destination = storageRef({ storageId: "volume-002" });
  const options = restoreOptions(backend, {
    storageRef: destination,
    request: mutationRequest("restore", lease(), { storageId: destination.storageId }),
  });
  const result = await restoreCleanCheckpoint(options);

  assert.equal(calls.restore.length, 1);
  assert.equal(calls.restore[0].checkpoint.storageId, "volume-001");
  assert.equal(calls.restore[0].request.storageId, "volume-002");
  assert.equal(result.checkpoint.storageId, "volume-001");
  assert.equal(result.mutation.storageId, "volume-002");
});

test("explicit retries preserve operation IDs and leave replay to the backend", async () => {
  const captureBackend = createBackend();
  const capture = captureOptions(captureBackend.backend);
  const firstCapture = await captureCleanCheckpoint(capture);
  const secondCapture = await captureCleanCheckpoint(capture);
  assert.deepEqual(secondCapture, firstCapture);
  assert.equal(captureBackend.calls.capture.length, 2);
  assert.equal(captureBackend.calls.capture[0].request.operationId, capture.request.operationId);
  assert.equal(captureBackend.calls.capture[1].request.operationId, capture.request.operationId);

  const restoreBackend = createBackend();
  const restore = restoreOptions(restoreBackend.backend);
  const firstRestore = await restoreCleanCheckpoint(restore);
  const secondRestore = await restoreCleanCheckpoint(restore);
  assert.deepEqual(secondRestore, firstRestore);
  assert.equal(restoreBackend.calls.restore.length, 2);
  assert.equal(restoreBackend.calls.restore[0].request.operationId, restore.request.operationId);
  assert.equal(restoreBackend.calls.restore[1].request.operationId, restore.request.operationId);
});

test("capture rejects an idempotent replay whose echoed descriptor belongs to an earlier attempt", async () => {
  let replayedResult;
  const { backend, calls } = createBackend({
    capture(input) {
      replayedResult ??= backendCheckpointResult(input);
      return replayedResult;
    },
  });
  const first = captureOptions(backend);
  const firstResult = await captureCleanCheckpoint(first);
  assert.equal(firstResult.checkpoint.createdAt, CREATED_AT);

  const second = captureOptions(backend, {
    createdAt: "2026-07-02T12:00:01.000Z",
  });
  assert.equal(second.request.operationId, first.request.operationId);
  await assert.rejects(
    () => captureCleanCheckpoint(second),
    assertCoreCode("checkpoint_outcome_uncertain"),
  );
  assert.equal(calls.capture.length, 2);
  assert.equal(calls.capture[1].checkpoint.createdAt, second.createdAt);
  assert.equal(replayedResult.checkpoint.createdAt, first.createdAt);
});

test("restore rejects a backend echo of a different valid source descriptor", async () => {
  const { backend, calls } = createBackend({
    restore(input) {
      return {
        checkpoint: {
          ...input.checkpoint,
          createdAt: "2026-07-02T12:00:01.000Z",
        },
        mutation: mutationResult(input.request),
      };
    },
  });
  await assert.rejects(
    () => restoreCleanCheckpoint(restoreOptions(backend)),
    assertCoreCode("restore_outcome_uncertain"),
  );
  assert.equal(calls.restore.length, 1);
});

test("capture rejects identity, fence, backend, operation, time, and class mismatches before dispatch", async (t) => {
  const scenarios = [
    {
      name: "manifest identity",
      expected: "stale_fence",
      change: (options) => ({ ...options, manifest: manifest({ sessionId: OTHER_SESSION_ID }) }),
    },
    {
      name: "attachment writer",
      expected: "stale_fence",
      change: (options) => ({
        ...options,
        attachment: attachment(options.canonicalLease, { holderId: "host-002" }),
      }),
    },
    {
      name: "attachment storage",
      expected: "stale_fence",
      change: (options) => ({
        ...options,
        attachment: attachment(options.canonicalLease, { storageId: "volume-002" }),
      }),
    },
    {
      name: "request fence",
      expected: "stale_fence",
      change: (options) => ({
        ...options,
        request: mutationRequest("checkpoint", lease({ fencingEpoch: "12" })),
      }),
    },
    {
      name: "backend identity",
      expected: "invalid_storage_backend",
      backendId: "other-backend",
      change: (options) => options,
    },
    {
      name: "wrong operation",
      expected: "invalid_storage_mutation",
      change: (options) => ({
        ...options,
        request: mutationRequest("restore", options.canonicalLease),
      }),
    },
    {
      name: "expired lease",
      expected: "lease_expired",
      change: (options) => ({ ...options, now: Date.parse(options.canonicalLease.expiresAt) }),
    },
    {
      name: "invalid creation time",
      expected: "invalid_checkpoint",
      change: (options) => ({ ...options, createdAt: "2026-07-02 12:00:00Z" }),
    },
  ];

  for (const scenario of scenarios) {
    await t.test(scenario.name, async () => {
      const { backend, calls } = createBackend({ backendId: scenario.backendId });
      await assert.rejects(
        () => captureCleanCheckpoint(scenario.change(captureOptions(backend))),
        assertContractCode(scenario.expected),
      );
      assert.equal(calls.capture.length, 0);
    });
  }

  for (const checkpointClass of ["graceful-abort", "crash-prefix"]) {
    await t.test(checkpointClass, async () => {
      const { backend, calls } = createBackend();
      await assert.rejects(
        () => captureCleanCheckpoint(captureOptions(backend, { checkpointClass })),
        assertCoreCode("unsupported_checkpoint_class"),
      );
      assert.equal(calls.capture.length, 0);
    });
  }
});

test("capture requires an opaque stopped-writer evidence handle before dispatch", async (t) => {
  let evidenceProxyTraps = 0;
  const revokedEvidence = Proxy.revocable({}, {});
  revokedEvidence.revoke();
  const invalidEvidence = [
    { name: "undefined", value: undefined },
    { name: "null", value: null },
    { name: "primitive", value: "writer-stopped" },
    { name: "array", value: [] },
    { name: "revoked proxy", value: revokedEvidence.proxy },
    {
      name: "proxy",
      value: new Proxy(
        {},
        {
          get() {
            evidenceProxyTraps += 1;
            throw new Error("evidence must remain opaque");
          },
          getPrototypeOf() {
            evidenceProxyTraps += 1;
            throw new Error("evidence must remain opaque");
          },
          ownKeys() {
            evidenceProxyTraps += 1;
            throw new Error("evidence must remain opaque");
          },
        },
      ),
    },
  ];

  for (const scenario of invalidEvidence) {
    await t.test(scenario.name, async () => {
      const { backend, calls } = createBackend();
      await assert.rejects(
        () =>
          captureCleanCheckpoint(
            captureOptions(backend, { stoppedWriterEvidence: scenario.value }),
          ),
        assertContractCode("invalid_checkpoint"),
      );
      assert.equal(calls.capture.length, 0);
    });
  }
  assert.equal(evidenceProxyTraps, 0);

  await t.test("missing", async () => {
    const { backend, calls } = createBackend();
    const options = captureOptions(backend);
    delete options.stoppedWriterEvidence;
    await assert.rejects(
      () => captureCleanCheckpoint(options),
      assertContractCode("invalid_checkpoint"),
    );
    assert.equal(calls.capture.length, 0);
  });
});

test("restore rejects identity, fence, target, backend, class, expiration, and epoch mismatches before dispatch", async (t) => {
  const scenarios = [
    {
      name: "manifest identity",
      expected: "invalid_checkpoint",
      change: (options) => ({
        ...options,
        checkpoint: checkpoint({ sessionId: OTHER_SESSION_ID }),
      }),
    },
    {
      name: "source backend",
      expected: "invalid_checkpoint",
      change: (options) => ({
        ...options,
        checkpoint: checkpoint({ backendId: "other-backend" }),
      }),
    },
    {
      name: "request fence",
      expected: "stale_fence",
      change: (options) => ({
        ...options,
        request: mutationRequest("restore", lease({ fencingEpoch: "12" })),
      }),
    },
    {
      name: "target checkpoint",
      expected: "invalid_storage_mutation",
      change: (options) => ({
        ...options,
        request: mutationRequest("restore", options.canonicalLease, {
          target: {
            artifactId: "artifact-001",
            checkpointId: "checkpoint-002",
            kind: "checkpoint",
          },
        }),
      }),
    },
    {
      name: "target artifact",
      expected: "invalid_storage_mutation",
      change: (options) => ({
        ...options,
        request: mutationRequest("restore", options.canonicalLease, {
          target: {
            artifactId: "artifact-002",
            checkpointId: "checkpoint-001",
            kind: "checkpoint",
          },
        }),
      }),
    },
    {
      name: "backend identity",
      expected: "invalid_storage_backend",
      backendId: "other-backend",
      change: (options) => options,
    },
    {
      name: "wrong operation",
      expected: "invalid_storage_mutation",
      change: (options) => ({
        ...options,
        request: mutationRequest("checkpoint", options.canonicalLease),
      }),
    },
    {
      name: "expired lease",
      expected: "lease_expired",
      change: (options) => ({ ...options, now: Date.parse(options.canonicalLease.expiresAt) }),
    },
    {
      name: "equal source epoch",
      expected: "stale_fence",
      change: (options) => ({
        ...options,
        checkpoint: checkpoint({ sourceFencingEpoch: options.canonicalLease.fencingEpoch }),
      }),
    },
    {
      name: "older current epoch",
      expected: "stale_fence",
      change: (options) => ({
        ...options,
        checkpoint: checkpoint({ sourceFencingEpoch: "12" }),
      }),
    },
  ];

  for (const scenario of scenarios) {
    await t.test(scenario.name, async () => {
      const { backend, calls } = createBackend({ backendId: scenario.backendId });
      await assert.rejects(
        () => restoreCleanCheckpoint(scenario.change(restoreOptions(backend))),
        assertContractCode(scenario.expected),
      );
      assert.equal(calls.restore.length, 0);
    });
  }

  for (const checkpointClass of ["graceful-abort", "crash-prefix"]) {
    await t.test(checkpointClass, async () => {
      const { backend, calls } = createBackend();
      await assert.rejects(
        () =>
          restoreCleanCheckpoint(
            restoreOptions(backend, { checkpoint: checkpoint({ checkpointClass }) }),
          ),
        assertCoreCode("unsupported_checkpoint_class"),
      );
      assert.equal(calls.restore.length, 0);
    });
  }
});

test("backend throws and malformed results become sanitized non-retryable uncertainty without retry", async (t) => {
  for (const scenario of [
    {
      name: "capture throw",
      operation: "capture",
      code: "checkpoint_outcome_uncertain",
      backend: () => createBackend({ capture: () => { throw new Error("secret backend detail"); } }),
    },
    {
      name: "capture malformed result",
      operation: "capture",
      code: "checkpoint_outcome_uncertain",
      backend: () => createBackend({ capture: () => ({ status: "checkpoint-created" }) }),
    },
    {
      name: "capture mismatched writer tuple",
      operation: "capture",
      code: "checkpoint_outcome_uncertain",
      backend: () =>
        createBackend({
          capture: (input) => ({
            checkpoint: input.checkpoint,
            mutation: mutationResult({
              ...input.request,
              holderId: "host-returned-by-stale-writer",
            }),
          }),
        }),
    },
    {
      name: "restore throw",
      operation: "restore",
      code: "restore_outcome_uncertain",
      backend: () => createBackend({ restore: () => { throw new Error("secret backend detail"); } }),
    },
    {
      name: "restore malformed result",
      operation: "restore",
      code: "restore_outcome_uncertain",
      backend: () => createBackend({ restore: () => ({ status: "restored" }) }),
    },
    {
      name: "restore mismatched writer tuple",
      operation: "restore",
      code: "restore_outcome_uncertain",
      backend: () =>
        createBackend({
          restore: (input) => ({
            checkpoint: input.checkpoint,
            mutation: mutationResult({ ...input.request, fencingEpoch: "12" }),
          }),
        }),
    },
  ]) {
    await t.test(scenario.name, async () => {
      const instance = scenario.backend();
      const invoke =
        scenario.operation === "capture"
          ? () => captureCleanCheckpoint(captureOptions(instance.backend))
          : () => restoreCleanCheckpoint(restoreOptions(instance.backend));
      let caught;
      try {
        await invoke();
      } catch (error) {
        caught = error;
      }
      assert(assertCoreCode(scenario.code)(caught));
      assert.equal(caught.message.includes("secret"), false);
      assert(Object.isFrozen(caught));
      assert.equal(instance.calls[scenario.operation].length, 1);
    });
  }
});

test("backend checkpoint result envelopes reject proxies, accessors, extra, and missing fields", async (t) => {
  for (const operation of ["capture", "restore"]) {
    for (const shape of ["proxy", "accessor", "extra", "missing"]) {
      await t.test(`${operation} ${shape}`, async () => {
        let envelopeTraps = 0;
        const malformed = (input) => {
          const valid = backendCheckpointResult(input);
          if (shape === "proxy") {
            return new Proxy(valid, {
              getPrototypeOf() {
                envelopeTraps += 1;
                throw new Error("secret result proxy detail");
              },
              ownKeys() {
                envelopeTraps += 1;
                throw new Error("secret result proxy detail");
              },
            });
          }
          if (shape === "accessor") {
            const result = { mutation: valid.mutation };
            Object.defineProperty(result, "checkpoint", {
              enumerable: true,
              get() {
                envelopeTraps += 1;
                throw new Error("secret result accessor detail");
              },
            });
            return result;
          }
          if (shape === "extra") return { ...valid, extra: "unexpected" };
          return { checkpoint: valid.checkpoint };
        };
        const instance =
          operation === "capture"
            ? createBackend({ capture: malformed })
            : createBackend({ restore: malformed });
        const invoke =
          operation === "capture"
            ? () => captureCleanCheckpoint(captureOptions(instance.backend))
            : () => restoreCleanCheckpoint(restoreOptions(instance.backend));
        await assert.rejects(
          invoke,
          assertCoreCode(
            operation === "capture"
              ? "checkpoint_outcome_uncertain"
              : "restore_outcome_uncertain",
          ),
        );
        assert.equal(envelopeTraps, 0);
        assert.equal(instance.calls[operation].length, 1);
      });
    }
  }
});

test("backend identity getter failures on the comparison read stay pre-dispatch and sanitized", async (t) => {
  for (const operation of ["capture", "restore"]) {
    await t.test(operation, async () => {
      const { backend, calls } = createBackend();
      let getterReads = 0;
      Object.defineProperty(backend, "backendId", {
        configurable: true,
        enumerable: true,
        get() {
          getterReads += 1;
          if (getterReads === 1) return "single-attach-test";
          throw new Error("secret backend identity detail");
        },
      });

      const invoke =
        operation === "capture"
          ? () => captureCleanCheckpoint(captureOptions(backend))
          : () => restoreCleanCheckpoint(restoreOptions(backend));
      await assert.rejects(
        invoke,
        (error) =>
          assertContractCode("invalid_storage_backend")(error) &&
          !error.message.includes("secret"),
      );
      assert.equal(getterReads, 2);
      assert.equal(calls[operation].length, 0);
    });
  }
});

test("backend operation getter failures on the dispatch read stay pre-dispatch and sanitized", async (t) => {
  for (const operation of ["capture", "restore"]) {
    await t.test(operation, async () => {
      const { backend, calls } = createBackend();
      const method = operation === "capture" ? "captureCheckpoint" : "restoreCheckpoint";
      let getterReads = 0;
      let invocations = 0;
      Object.defineProperty(backend, method, {
        configurable: true,
        enumerable: true,
        get() {
          getterReads += 1;
          if (getterReads === 1) {
            return async () => {
              invocations += 1;
            };
          }
          throw new Error("secret backend getter detail");
        },
      });

      const invoke =
        operation === "capture"
          ? () => captureCleanCheckpoint(captureOptions(backend))
          : () => restoreCleanCheckpoint(restoreOptions(backend));
      await assert.rejects(
        invoke,
        (error) =>
          assertContractCode("invalid_storage_backend")(error) &&
          !error.message.includes("secret"),
      );
      assert.equal(getterReads, 2);
      assert.equal(invocations, 0);
      assert.equal(calls[operation].length, 0);
    });
  }
});

test("accessor and proxy option envelopes fail before backend property access or dispatch", async (t) => {
  for (const operation of ["capture", "restore"]) {
    await t.test(operation, async () => {
      let backendReads = 0;
      let proxyTraps = 0;
      const { backend, calls } = createBackend();
      const accessorOptions =
        operation === "capture" ? captureOptions(backend) : restoreOptions(backend);
      Object.defineProperty(accessorOptions, "backend", {
        enumerable: true,
        get() {
          backendReads += 1;
          return backend;
        },
      });
      const invoke = operation === "capture" ? captureCleanCheckpoint : restoreCleanCheckpoint;
      await assert.rejects(() => invoke(accessorOptions), assertContractCode("invalid_checkpoint"));
      assert.equal(backendReads, 0);
      assert.equal(calls[operation].length, 0);

      const proxyOptions = new Proxy(
        operation === "capture" ? captureOptions(backend) : restoreOptions(backend),
        {
          getPrototypeOf() {
            proxyTraps += 1;
            throw new Error("proxy trap must not run");
          },
          ownKeys() {
            proxyTraps += 1;
            throw new Error("proxy trap must not run");
          },
        },
      );
      await assert.rejects(() => invoke(proxyOptions), assertContractCode("invalid_checkpoint"));
      assert.equal(proxyTraps, 0);
      assert.equal(calls[operation].length, 0);

      const extraOptions = {
        ...(operation === "capture" ? captureOptions(backend) : restoreOptions(backend)),
        stopProof: "not-authority",
      };
      await assert.rejects(() => invoke(extraOptions), assertContractCode("invalid_checkpoint"));
      assert.equal(calls[operation].length, 0);
    });
  }
});
