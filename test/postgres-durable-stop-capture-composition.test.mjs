import assert from "node:assert/strict";
import test from "node:test";

import {
  PostgresDurableStopCaptureCompositionError,
  createPostgresDurableStopCaptureComposition,
} from "../src/postgres-durable-stop-capture-composition.mjs";
import {
  derivePostgresLogicalWriterStopOperationId,
} from "../src/postgres-logical-writer-launcher.mjs";
import { createSessionManifest } from "../src/session-storage-contracts.mjs";

const SESSION_ID = "019f2100-0000-7000-8000-000000000001";
const THREAD_ID = "019f2100-0000-7000-8000-000000000002";
const IMAGE_DIGEST = `sha256:${"a".repeat(64)}`;
const NOW = Date.parse("2026-08-05T12:00:00.000Z");
const CREATED_AT = "2026-08-05T12:00:00.000Z";
const LAUNCH_ATTEMPT_ID = "launch-attempt-001";
const PROCESS_INCARNATION_ID = "process-incarnation-001";
const WRITER_INCARNATION_ID = "writer-incarnation-001";
const SUPERVISOR_ID = "supervisor-001";
const STOP_DISPATCH_CLAIM_SHA256 = "d".repeat(64);

function deepFreeze(value) {
  if (value === null || typeof value !== "object" || Object.isFrozen(value)) {
    return value;
  }
  for (const key of Reflect.ownKeys(value)) deepFreeze(value[key]);
  return Object.freeze(value);
}

function manifest() {
  return createSessionManifest({
    sessionId: SESSION_ID,
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

function storageRef() {
  return {
    contractVersion: 1,
    backendId: "single-attach-test",
    storageId: "volume-001",
    sessionId: SESSION_ID,
  };
}

function lease() {
  return {
    contractVersion: 1,
    sessionId: SESSION_ID,
    leaseId: "lease-001",
    holderId: "host-001",
    fencingEpoch: "11",
    expiresAt: "2026-08-05T13:00:00.000Z",
  };
}

function attachment() {
  const writerLease = lease();
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
  };
}

function mutationRequest() {
  const writerLease = lease();
  return {
    contractVersion: 1,
    backendId: "single-attach-test",
    storageId: "volume-001",
    sessionId: SESSION_ID,
    leaseId: writerLease.leaseId,
    holderId: writerLease.holderId,
    fencingEpoch: writerLease.fencingEpoch,
    operation: "checkpoint",
    operationId: "operation-checkpoint-001",
    target: {
      artifactId: "artifact-001",
      checkpointId: "checkpoint-001",
      kind: "checkpoint",
    },
  };
}

function mutationResult(request) {
  return {
    ...request,
    proofId: "proof-checkpoint-001",
    status: "checkpoint-created",
  };
}

function createBackend({ capture } = {}) {
  const calls = [];
  const operation = async () => {};
  const backend = {
    contractVersion: 1,
    backendId: "single-attach-test",
    capabilities: {
      atomicPointInTimeCheckpoint: true,
      exclusiveWriterAttachment: true,
      fencing: "epoch-enforced",
      normalDirectoryAttachment: true,
    },
    async captureCheckpoint(input) {
      calls.push(input);
      if (capture) return capture.call(this, input);
      return {
        checkpoint: input.checkpoint,
        mutation: mutationResult(input.request),
      };
    },
    destroySession: operation,
    detachAttachment: operation,
    forceFence: operation,
    prepareWritableAttachment: operation,
    provisionSession: operation,
    restoreCheckpoint: operation,
  };
  return { backend, calls };
}

function captureOptions(backend) {
  return {
    attachment: attachment(),
    backend,
    canonicalLease: lease(),
    checkpointClass: "clean",
    createdAt: CREATED_AT,
    manifest: manifest(),
    now: NOW,
    request: mutationRequest(),
    storageRef: storageRef(),
  };
}

function opaqueHandle() {
  return Object.freeze(Object.create(null));
}

function writerLaunchStopRequest(contractVersion) {
  const request = { contractVersion };
  if (contractVersion === 2) {
    request.dispatchClaimSha256 = STOP_DISPATCH_CLAIM_SHA256;
  }
  request.launch = {
    attachmentId: "attachment-001",
    contractVersion: 1,
    fencingEpoch: "11",
    launchAttemptId: LAUNCH_ATTEMPT_ID,
    leaseId: "lease-001",
    processIncarnationId: PROCESS_INCARNATION_ID,
    supervisorId: SUPERVISOR_ID,
    writerIncarnationId: WRITER_INCARNATION_ID,
  };
  return request;
}

function stoppedCaptureResult(capture, { stopRequestVersion = 2 } = {}) {
  const stopOperationId = derivePostgresLogicalWriterStopOperationId({
    ...capture,
    launchAttemptId: LAUNCH_ATTEMPT_ID,
  });
  const capability = opaqueHandle();
  const writer = opaqueHandle();
  const evidence = deepFreeze({
    contractVersion: 1,
    launchAttemptId: LAUNCH_ATTEMPT_ID,
    processIncarnationId: PROCESS_INCARNATION_ID,
    proofId: stopOperationId,
    status: "complete-stopped",
    supervisorId: SUPERVISOR_ID,
    writerIncarnationId: WRITER_INCARNATION_ID,
  });
  const resolution = deepFreeze({
    canonicalLeaseAtRegistration: lease(),
    processIncarnationId: PROCESS_INCARNATION_ID,
    stopOperationId,
    writer,
    writerIncarnationId: WRITER_INCARNATION_ID,
  });
  const terminal = deepFreeze({
    evidence: { ...evidence },
    outcome: "writer-launch-stopped",
    resultVersion: 1,
  });
  const stop = deepFreeze({
    status: "committed",
    session: {
      sessionId: SESSION_ID,
      document: { launch: null },
    },
    operation: {
      operationId: stopOperationId,
      request: writerLaunchStopRequest(stopRequestVersion),
      result: terminal,
      sessionId: SESSION_ID,
      state: "committed",
    },
    reservation: {
      operationId: stopOperationId,
      sessionId: SESSION_ID,
      state: "released",
    },
    finalized: true,
    launch: null,
    stop: {
      contractVersion: stopRequestVersion,
      launchAttemptId: LAUNCH_ATTEMPT_ID,
      request: writerLaunchStopRequest(stopRequestVersion),
      result: terminal,
      state: "committed",
      stopOperationId,
    },
  });
  return deepFreeze({ capability, evidence, resolution, stop });
}

function createLauncher({ retire, stop, stopRequestVersion = 2 } = {}) {
  const calls = { retire: [], stop: [] };
  const fixture = { calls, defaultStopped: null, launcher: null };
  const stopWriterForCapture = async function (input) {
    calls.stop.push(input);
    const defaultStopped = stoppedCaptureResult(input, { stopRequestVersion });
    fixture.defaultStopped = defaultStopped;
    if (stop) return stop(input, defaultStopped);
    return defaultStopped;
  };
  const retireStoppedWriter = function (input) {
    calls.retire.push(input);
    if (retire) return retire(input);
    return undefined;
  };
  Object.freeze(stopWriterForCapture);
  Object.freeze(retireStoppedWriter);
  fixture.launcher = Object.freeze({
    retireStoppedWriter,
    stopWriterForCapture,
  });
  return fixture;
}

function assertCompositionError(code) {
  return (error) =>
    error instanceof PostgresDurableStopCaptureCompositionError &&
    error.code === code &&
    error.retryable === false;
}

async function assertStopReceiptRejected(
  mutate,
  { stopRequestVersion = 2 } = {},
) {
  const { backend, calls: backendCalls } = createBackend();
  const launcherFixture = createLauncher({
    stop(_input, result) {
      return deepFreeze(mutate(result));
    },
    stopRequestVersion,
  });
  const composition = createPostgresDurableStopCaptureComposition({
    launcher: launcherFixture.launcher,
  });

  await assert.rejects(
    () => composition.runCapture(captureOptions(backend)),
    assertCompositionError(
      "postgres_durable_stop_capture_composition_outcome_uncertain",
    ),
  );
  assert.equal(launcherFixture.calls.stop.length, 1);
  assert.equal(backendCalls.length, 0);
  assert.equal(launcherFixture.calls.retire.length, 0);
}

test("durable stop composition dispatches one exact tuple and retires after capture", async () => {
  const order = [];
  let stopInput;
  const { backend, calls: backendCalls } = createBackend({
    capture(input) {
      order.push("capture");
      assert.strictEqual(input.attachment, stopInput.attachment);
      assert.strictEqual(input.checkpoint, stopInput.checkpoint);
      assert.strictEqual(input.request, stopInput.request);
      assert.strictEqual(
        input.stoppedWriterEvidence,
        launcherFixture.defaultStopped.capability,
      );
      return {
        checkpoint: input.checkpoint,
        mutation: mutationResult(input.request),
      };
    },
  });
  const launcherFixture = createLauncher({
    stop(input, result) {
      order.push("stop");
      stopInput = input;
      assert(Object.isFrozen(input));
      assert.deepEqual(Object.keys(input).sort(), [
        "attachment",
        "checkpoint",
        "request",
      ]);
      assert.equal(result.stop.stop.contractVersion, 2);
      assert.deepEqual(Object.keys(result.stop.stop.request).sort(), [
        "contractVersion",
        "dispatchClaimSha256",
        "launch",
      ]);
      assert.deepEqual(
        Object.keys(result.stop.operation.request).sort(),
        Object.keys(result.stop.stop.request).sort(),
      );
      assert.notStrictEqual(
        result.stop.operation.request,
        result.stop.stop.request,
      );
      assert.match(
        result.stop.stop.request.dispatchClaimSha256,
        /^[0-9a-f]{64}$/u,
      );
      return result;
    },
    retire(input) {
      order.push("retire");
      assert.strictEqual(input, launcherFixture.defaultStopped.resolution);
    },
  });
  const composition = createPostgresDurableStopCaptureComposition({
    launcher: launcherFixture.launcher,
  });

  assert(Object.isFrozen(composition));
  assert(Object.isFrozen(composition.runCapture));
  const result = await composition.runCapture(captureOptions(backend));

  assert.deepEqual(order, ["stop", "capture", "retire"]);
  assert.equal(launcherFixture.calls.stop.length, 1);
  assert.equal(backendCalls.length, 1);
  assert.equal(launcherFixture.calls.retire.length, 1);
  assert.deepEqual(result, {
    checkpoint: stopInput.checkpoint,
    mutation: mutationResult(stopInput.request),
  });
});

test("durable stop composition retains v1 receipt compatibility", async () => {
  const { backend, calls: backendCalls } = createBackend();
  const launcherFixture = createLauncher({ stopRequestVersion: 1 });
  const composition = createPostgresDurableStopCaptureComposition({
    launcher: launcherFixture.launcher,
  });

  await composition.runCapture(captureOptions(backend));

  assert.equal(launcherFixture.defaultStopped.stop.stop.contractVersion, 1);
  assert.deepEqual(
    Object.keys(launcherFixture.defaultStopped.stop.stop.request).sort(),
    ["contractVersion", "launch"],
  );
  assert.equal(launcherFixture.calls.stop.length, 1);
  assert.equal(backendCalls.length, 1);
  assert.equal(launcherFixture.calls.retire.length, 1);
});

test("Promise subclasses are rejected before an overridden then can run", async () => {
  const { backend, calls: backendCalls } = createBackend();
  let stopCalls = 0;
  let retireCalls = 0;
  let thenCalls = 0;
  class SpoofedStopPromise extends Promise {
    then(onFulfilled, onRejected) {
      thenCalls += 1;
      return Promise.resolve(Object.freeze({ forged: true })).then(
        onFulfilled,
        onRejected,
      );
    }
  }
  const speciesHolder = Object.freeze(
    Object.create(null, {
      [Symbol.species]: {
        configurable: false,
        enumerable: false,
        value: Promise,
        writable: false,
      },
    }),
  );
  const stopWriterForCapture = function (input) {
    stopCalls += 1;
    const pending = new SpoofedStopPromise((resolve) => {
      resolve(stoppedCaptureResult(input));
    });
    Object.defineProperty(pending, "constructor", {
      configurable: false,
      enumerable: false,
      value: speciesHolder,
      writable: false,
    });
    return pending;
  };
  const retireStoppedWriter = function () {
    retireCalls += 1;
  };
  Object.freeze(stopWriterForCapture);
  Object.freeze(retireStoppedWriter);
  const launcher = Object.freeze({
    retireStoppedWriter,
    stopWriterForCapture,
  });
  const composition = createPostgresDurableStopCaptureComposition({ launcher });

  await assert.rejects(
    () => composition.runCapture(captureOptions(backend)),
    assertCompositionError(
      "postgres_durable_stop_capture_composition_outcome_uncertain",
    ),
  );
  assert.equal(stopCalls, 1);
  assert.equal(thenCalls, 0);
  assert.equal(backendCalls.length, 0);
  assert.equal(retireCalls, 0);
});

test("stop callback cannot poison exact receipt binding", { concurrency: false }, async () => {
  const { backend, calls: backendCalls } = createBackend();
  const defineProperty = Object.defineProperty;
  const objectIsDescriptor = Object.getOwnPropertyDescriptor(Object, "is");
  const launcherFixture = createLauncher({
    stop(_input, result) {
      defineProperty(Object, "is", {
        ...objectIsDescriptor,
        value: () => true,
      });
      return deepFreeze({
        ...result,
        stop: {
          ...result.stop,
          operation: {
            ...result.stop.operation,
            request: {
              ...result.stop.operation.request,
              dispatchClaimSha256: "e".repeat(64),
            },
          },
        },
      });
    },
  });
  const composition = createPostgresDurableStopCaptureComposition({
    launcher: launcherFixture.launcher,
  });

  let failure = null;
  try {
    await composition.runCapture(captureOptions(backend));
  } catch (error) {
    failure = error;
  } finally {
    defineProperty(Object, "is", objectIsDescriptor);
  }

  assert(
    assertCompositionError(
      "postgres_durable_stop_capture_composition_outcome_uncertain",
    )(failure),
  );
  assert.equal(launcherFixture.calls.stop.length, 1);
  assert.equal(backendCalls.length, 0);
  assert.equal(launcherFixture.calls.retire.length, 0);
});

test("stop callback cannot replace validated retirement resolution", { concurrency: false }, async () => {
  const { backend, calls: backendCalls } = createBackend();
  const defineProperty = Object.defineProperty;
  const freezeDescriptor = Object.getOwnPropertyDescriptor(Object, "freeze");
  const originalFreeze = freezeDescriptor.value;
  const objectHasOwn = Object.hasOwn;
  const foreignResolution = originalFreeze({ foreign: true });
  let replacementCalls = 0;
  const launcherFixture = createLauncher({
    stop(_input, result) {
      defineProperty(Object, "freeze", {
        ...freezeDescriptor,
        value(value) {
          if (
            value !== null &&
            typeof value === "object" &&
            objectHasOwn(value, "capability") &&
            objectHasOwn(value, "evidence") &&
            objectHasOwn(value, "resolution") &&
            objectHasOwn(value, "stop")
          ) {
            replacementCalls += 1;
            return { ...value, resolution: foreignResolution };
          }
          return originalFreeze(value);
        },
      });
      return result;
    },
  });
  const composition = createPostgresDurableStopCaptureComposition({
    launcher: launcherFixture.launcher,
  });

  let result;
  let failure = null;
  try {
    result = await composition.runCapture(captureOptions(backend));
  } catch (error) {
    failure = error;
  } finally {
    defineProperty(Object, "freeze", freezeDescriptor);
  }

  if (failure !== null) throw failure;
  assert.equal(replacementCalls, 0);
  assert.equal(backendCalls.length, 1);
  assert.equal(launcherFixture.calls.retire.length, 1);
  assert.strictEqual(
    launcherFixture.calls.retire[0],
    launcherFixture.defaultStopped.resolution,
  );
  assert.notStrictEqual(launcherFixture.calls.retire[0], foreignResolution);
  assert.deepEqual(result, {
    checkpoint: backendCalls[0].checkpoint,
    mutation: mutationResult(backendCalls[0].request),
  });
});

test("durable stop composition rejects caller-supplied stopped-writer evidence", async () => {
  const { backend, calls: backendCalls } = createBackend();
  const launcherFixture = createLauncher();
  const composition = createPostgresDurableStopCaptureComposition({
    launcher: launcherFixture.launcher,
  });

  await assert.rejects(
    () =>
      composition.runCapture({
        ...captureOptions(backend),
        stoppedWriterEvidence: opaqueHandle(),
      }),
    assertCompositionError(
      "invalid_postgres_durable_stop_capture_composition_request",
    ),
  );
  assert.equal(launcherFixture.calls.stop.length, 0);
  assert.equal(backendCalls.length, 0);
  assert.equal(launcherFixture.calls.retire.length, 0);
});

test("capture preparation failures are request errors before durable stop", async () => {
  const { backend, calls: backendCalls } = createBackend();
  const launcherFixture = createLauncher();
  const composition = createPostgresDurableStopCaptureComposition({
    launcher: launcherFixture.launcher,
  });

  await assert.rejects(
    () =>
      composition.runCapture({
        ...captureOptions(backend),
        checkpointClass: "graceful-abort",
      }),
    assertCompositionError(
      "invalid_postgres_durable_stop_capture_composition_request",
    ),
  );
  assert.equal(launcherFixture.calls.stop.length, 0);
  assert.equal(backendCalls.length, 0);
  assert.equal(launcherFixture.calls.retire.length, 0);
});

test("revoked options and capture requests have fixed public error classes", async () => {
  const launcherFixture = createLauncher();
  const revokedOptions = Proxy.revocable(
    { launcher: launcherFixture.launcher },
    {},
  );
  revokedOptions.revoke();
  assert.throws(
    () => createPostgresDurableStopCaptureComposition(revokedOptions.proxy),
    assertCompositionError(
      "invalid_postgres_durable_stop_capture_composition_options",
    ),
  );

  const { backend, calls: backendCalls } = createBackend();
  const composition = createPostgresDurableStopCaptureComposition({
    launcher: launcherFixture.launcher,
  });
  const revokedRequest = Proxy.revocable(captureOptions(backend), {});
  revokedRequest.revoke();
  await assert.rejects(
    () => composition.runCapture(revokedRequest.proxy),
    assertCompositionError(
      "invalid_postgres_durable_stop_capture_composition_request",
    ),
  );
  assert.equal(launcherFixture.calls.stop.length, 0);
  assert.equal(backendCalls.length, 0);
});

test("stop failure cannot dispatch capture or retirement", async () => {
  const { backend, calls: backendCalls } = createBackend();
  const launcherFixture = createLauncher({
    stop() {
      throw new Error("stop acknowledgement lost");
    },
  });
  const composition = createPostgresDurableStopCaptureComposition({
    launcher: launcherFixture.launcher,
  });

  await assert.rejects(
    () => composition.runCapture(captureOptions(backend)),
    assertCompositionError(
      "postgres_durable_stop_capture_composition_outcome_uncertain",
    ),
  );
  assert.equal(launcherFixture.calls.stop.length, 1);
  assert.equal(backendCalls.length, 0);
  assert.equal(launcherFixture.calls.retire.length, 0);
});

test("malformed durable stop receipt fails before capture or retirement", async () => {
  const { backend, calls: backendCalls } = createBackend();
  const launcherFixture = createLauncher({
    stop(_input, result) {
      return deepFreeze({
        ...result,
        stop: {
          ...result.stop,
          launch: { launchAttemptId: LAUNCH_ATTEMPT_ID },
        },
      });
    },
  });
  const composition = createPostgresDurableStopCaptureComposition({
    launcher: launcherFixture.launcher,
  });

  await assert.rejects(
    () => composition.runCapture(captureOptions(backend)),
    assertCompositionError(
      "postgres_durable_stop_capture_composition_outcome_uncertain",
    ),
  );
  assert.equal(backendCalls.length, 0);
  assert.equal(launcherFixture.calls.retire.length, 0);
});

test("unsupported durable stop receipt versions fail before capture", async () => {
  const { backend, calls: backendCalls } = createBackend();
  const launcherFixture = createLauncher({
    stop(_input, result) {
      return deepFreeze({
        ...result,
        stop: {
          ...result.stop,
          stop: {
            ...result.stop.stop,
            contractVersion: 3,
          },
        },
      });
    },
  });
  const composition = createPostgresDurableStopCaptureComposition({
    launcher: launcherFixture.launcher,
  });

  await assert.rejects(
    () => composition.runCapture(captureOptions(backend)),
    assertCompositionError(
      "postgres_durable_stop_capture_composition_outcome_uncertain",
    ),
  );
  assert.equal(backendCalls.length, 0);
  assert.equal(launcherFixture.calls.retire.length, 0);
});

test("malformed v2 durable stop requests fail before capture", async () => {
  const scenarios = [
    {
      name: "missing digest",
      mutate(result) {
        return {
          ...result,
          stop: {
            ...result.stop,
            operation: {
              ...result.stop.operation,
              request: {
                contractVersion: 2,
                launch: result.stop.operation.request.launch,
              },
            },
            stop: {
              ...result.stop.stop,
              request: {
                contractVersion: 2,
                launch: result.stop.stop.request.launch,
              },
            },
          },
        };
      },
    },
    {
      name: "invalid digest",
      mutate(result) {
        const request = {
          ...result.stop.stop.request,
          dispatchClaimSha256: "D".repeat(64),
        };
        const operationRequest = {
          ...result.stop.operation.request,
          dispatchClaimSha256: "D".repeat(64),
        };
        return {
          ...result,
          stop: {
            ...result.stop,
            operation: {
              ...result.stop.operation,
              request: operationRequest,
            },
            stop: {
              ...result.stop.stop,
              request,
            },
          },
        };
      },
    },
    {
      name: "contradictory digest",
      mutate(result) {
        return {
          ...result,
          stop: {
            ...result.stop,
            operation: {
              ...result.stop.operation,
              request: {
                ...result.stop.operation.request,
                dispatchClaimSha256: "e".repeat(64),
              },
            },
          },
        };
      },
    },
    {
      name: "outer version mismatch",
      mutate(result) {
        return {
          ...result,
          stop: {
            ...result.stop,
            stop: {
              ...result.stop.stop,
              contractVersion: 1,
            },
          },
        };
      },
    },
  ];

  for (const scenario of scenarios) {
    await assertStopReceiptRejected(scenario.mutate);
  }
});

test("v1 durable stop requests reject v2-only digest fields", async () => {
  await assertStopReceiptRejected(
    (result) => ({
      ...result,
      stop: {
        ...result.stop,
        operation: {
          ...result.stop.operation,
          request: {
            ...result.stop.operation.request,
            dispatchClaimSha256: STOP_DISPATCH_CLAIM_SHA256,
          },
        },
        stop: {
          ...result.stop.stop,
          request: {
            ...result.stop.stop.request,
            dispatchClaimSha256: STOP_DISPATCH_CLAIM_SHA256,
          },
        },
      },
    }),
    { stopRequestVersion: 1 },
  );
});

test("contradictory nested durable stop identity fails before capture", async () => {
  const { backend, calls: backendCalls } = createBackend();
  const launcherFixture = createLauncher({
    stop(_input, result) {
      return deepFreeze({
        ...result,
        stop: {
          ...result.stop,
          operation: {
            ...result.stop.operation,
            request: {
              ...result.stop.operation.request,
              launch: {
                ...result.stop.operation.request.launch,
                writerIncarnationId: "writer-incarnation-contradiction",
              },
            },
          },
        },
      });
    },
  });
  const composition = createPostgresDurableStopCaptureComposition({
    launcher: launcherFixture.launcher,
  });

  await assert.rejects(
    () => composition.runCapture(captureOptions(backend)),
    assertCompositionError(
      "postgres_durable_stop_capture_composition_outcome_uncertain",
    ),
  );
  assert.equal(backendCalls.length, 0);
  assert.equal(launcherFixture.calls.retire.length, 0);
});

test("a real receipt for a different complete capture tuple is rejected", async () => {
  const scenarios = [
    {
      name: "request",
      mutate(input) {
        return deepFreeze({
          ...input,
          request: {
            ...input.request,
            operationId: "operation-checkpoint-wrong",
          },
        });
      },
    },
    {
      name: "checkpoint",
      mutate(input) {
        return deepFreeze({
          ...input,
          checkpoint: {
            ...input.checkpoint,
            createdAt: "2026-08-05T12:00:01.000Z",
          },
        });
      },
    },
  ];

  for (const scenario of scenarios) {
    const { backend, calls: backendCalls } = createBackend();
    const launcherFixture = createLauncher({
      stop(input) {
        return stoppedCaptureResult(scenario.mutate(input));
      },
    });
    const composition = createPostgresDurableStopCaptureComposition({
      launcher: launcherFixture.launcher,
    });

    await assert.rejects(
      () => composition.runCapture(captureOptions(backend)),
      assertCompositionError(
        "postgres_durable_stop_capture_composition_outcome_uncertain",
      ),
      scenario.name,
    );
    assert.equal(backendCalls.length, 0, scenario.name);
    assert.equal(launcherFixture.calls.retire.length, 0, scenario.name);
  }
});

test("capture uncertainty does not retire or retry", async () => {
  const { backend, calls: backendCalls } = createBackend({
    capture() {
      throw new Error("capture acknowledgement lost");
    },
  });
  const launcherFixture = createLauncher();
  const composition = createPostgresDurableStopCaptureComposition({
    launcher: launcherFixture.launcher,
  });

  await assert.rejects(
    () => composition.runCapture(captureOptions(backend)),
    assertCompositionError(
      "postgres_durable_stop_capture_composition_outcome_uncertain",
    ),
  );
  assert.equal(launcherFixture.calls.stop.length, 1);
  assert.equal(backendCalls.length, 1);
  assert.equal(launcherFixture.calls.retire.length, 0);
});

test("retirement failure is classified after one definite capture", async () => {
  const scenarios = [
    {
      name: "rejection",
      retire() {
        throw new Error("retirement acknowledgement lost");
      },
    },
    {
      name: "unexpected result",
      retire() {
        return Object.freeze({ retired: true });
      },
    },
    {
      name: "rejecting native promise",
      retire() {
        return Promise.reject(new Error("retirement acknowledgement lost"));
      },
    },
  ];

  for (const scenario of scenarios) {
    const { backend, calls: backendCalls } = createBackend();
    const launcherFixture = createLauncher({ retire: scenario.retire });
    const composition = createPostgresDurableStopCaptureComposition({
      launcher: launcherFixture.launcher,
    });

    await assert.rejects(
      () => composition.runCapture(captureOptions(backend)),
      assertCompositionError(
        "postgres_durable_stop_capture_retirement_outcome_uncertain",
      ),
      scenario.name,
    );
    assert.equal(launcherFixture.calls.stop.length, 1, scenario.name);
    assert.equal(backendCalls.length, 1, scenario.name);
    assert.equal(launcherFixture.calls.retire.length, 1, scenario.name);
  }
});
