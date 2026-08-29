import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import test from "node:test";

import {
  SESSION_AUTHORITY_DOCUMENT_VERSION,
  SESSION_OPERATION_CONFLICT_CLASS,
  WRITER_FORCE_FENCE_OPERATION_KIND,
  WRITER_LAUNCH_ATTEMPT_OPERATION_KIND,
  assertSessionOperationBinding,
  createWriterLaunchAttemptOperationRequest,
} from "../src/postgres-session-authority.mjs";
import {
  POSTGRES_PODMAN_VERIFIED_STOP_FENCE_BINDING_CONTRACT_VERSION,
  PostgresPodmanVerifiedStopFenceBindingError,
  createPostgresPodmanVerifiedStopFenceBindingResolver,
} from "../src/postgres-podman-verified-stop-fence-binding.mjs";
import {
  createSessionManifest,
} from "../src/session-storage-contracts.mjs";

const SESSION_ID = "018f4d31-a8ab-7dd2-b225-000000000001";
const STATE_OWNER_ID = `state-owner:${"1".repeat(64)}`;
const LAUNCH_ATTEMPT_ID = "writer-launch-attempt-001";
const FENCE_OPERATION_ID = "writer-force-fence-001";
const SUPERVISOR_ID = "podman-supervisor-001";
const PROCESS_INCARNATION_ID = `podman-process:${"a".repeat(64)}`;
const WRITER_INCARNATION_ID = `podman-writer:${"b".repeat(64)}`;
const START_PROOF_ID = `podman-start:${"c".repeat(64)}`;
const CREATED_AT = "2026-08-29T08:00:00.000Z";
const LAUNCH_PREPARED_AT = "2026-08-29T08:00:01.000Z";
const LAUNCH_COMMITTED_AT = "2026-08-29T08:00:02.000Z";
const FENCE_STARTED_AT = "2026-08-29T08:00:03.000Z";

function sha256(value) {
  return createHash("sha256").update(value, "utf8").digest("hex");
}

function canonicalPayload(value) {
  if (Array.isArray(value)) {
    const result = value.map(canonicalPayload);
    Object.setPrototypeOf(result, null);
    return result;
  }
  if (value === null || typeof value !== "object") return value;
  const result = Object.create(null);
  for (const key of Object.keys(value).sort()) {
    result[key] = canonicalPayload(value[key]);
  }
  return result;
}

function canonicalSha256(value) {
  return sha256(JSON.stringify(canonicalPayload(value)));
}

function deepFreeze(value) {
  if (value !== null && typeof value === "object" && !Object.isFrozen(value)) {
    for (const key of Reflect.ownKeys(value)) {
      const descriptor = Object.getOwnPropertyDescriptor(value, key);
      if (descriptor && Object.hasOwn(descriptor, "value")) {
        deepFreeze(descriptor.value);
      }
    }
    Object.freeze(value);
  }
  return value;
}

function exact(values) {
  return Object.freeze(Object.assign(Object.create(null), values));
}

function terminalPointer({ binding, operationRevision, result }) {
  return {
    conflictClass: SESSION_OPERATION_CONFLICT_CLASS,
    expectedSessionRevision: binding.expectedSession.revision,
    kind: WRITER_LAUNCH_ATTEMPT_OPERATION_KIND,
    operationId: LAUNCH_ATTEMPT_ID,
    operationRevision,
    requestSha256: binding.requestSha256,
    reservationId: binding.reservationId,
    resultSha256: canonicalSha256(result),
    state: "committed",
  };
}

function fixture() {
  const manifest = createSessionManifest({
    sessionId: SESSION_ID,
    codex: {
      rootThreadId: SESSION_ID,
      sessionId: SESSION_ID,
      ephemeral: false,
      historyMode: "paginated",
    },
    runtime: {
      imageDigest: `sha256:${"d".repeat(64)}`,
      imageMediaType: "application/vnd.oci.image.manifest.v1+json",
      platform: "linux/arm64",
      codexVersion: "codex-cli 0.142.4",
      codexSandbox: "danger-full-access",
    },
  });
  const lease = {
    contractVersion: 1,
    expiresAt: "2026-08-29T09:00:00.000Z",
    fencingEpoch: "7",
    holderId: "host-001",
    leaseId: "lease-001",
    sessionId: SESSION_ID,
  };
  const attachment = {
    attachmentId: "attachment-001",
    backendId: "filesystem-backend",
    contractVersion: 1,
    fencingEpoch: lease.fencingEpoch,
    holderId: lease.holderId,
    kind: "directory",
    leaseId: lease.leaseId,
    mode: "read-write",
    operationId: "writer-attachment-acquire-001",
    proofId: "attachment-proof-001",
    rootPath: "/var/lib/portable-codex/session-001",
    sessionId: SESSION_ID,
    storageId: "volume-001",
  };
  const backendCapabilities = {
    atomicPointInTimeCheckpoint: false,
    exclusiveWriterAttachment: true,
    fencing: "verified-detach",
    normalDirectoryAttachment: true,
  };
  const previous = {
    conflictClass: SESSION_OPERATION_CONFLICT_CLASS,
    expectedSessionRevision: "2",
    kind: "writer-attachment-acquire-v1",
    operationId: attachment.operationId,
    operationRevision: "2",
    requestSha256: "2".repeat(64),
    reservationId: "reservation-writer-attachment-acquire-001",
    resultSha256: "3".repeat(64),
    state: "committed",
  };
  const preLaunchSession = {
    createdAt: CREATED_AT,
    document: {
      activeOperation: null,
      attachment,
      backendCapabilities,
      documentVersion: SESSION_AUTHORITY_DOCUMENT_VERSION,
      lastOperation: previous,
      launch: null,
      lease,
      lifecycle: "ATTACHED",
      manifest,
      recovery: null,
      storageRef: {
        backendId: attachment.backendId,
        contractVersion: 1,
        sessionId: SESSION_ID,
        storageId: attachment.storageId,
      },
      writerEpoch: lease.fencingEpoch,
    },
    revision: "5",
    sessionId: SESSION_ID,
    updatedAt: CREATED_AT,
  };
  const generation = {
    binding: { source: "test-binding" },
    checkpointId: "checkpoint-001",
    claimedAt: "2026-08-29T07:59:58.000Z",
    committedAt: "2026-08-29T07:59:59.000Z",
    document: { source: "test-document" },
    generationId: "generation-001",
    operationId: "restore-generation-001",
    sessionId: SESSION_ID,
    state: "committed",
  };
  const measuredImage = {
    projection: {
      codexSandbox: manifest.runtime.codexSandbox,
      codexVersion: manifest.runtime.codexVersion,
      platformImage: {
        architecture: "arm64",
        config: {
          digest: `sha256:${"e".repeat(64)}`,
          mediaType: "application/vnd.oci.image.config.v1+json",
          size: 512,
        },
        digest: manifest.runtime.imageDigest,
        mediaType: manifest.runtime.imageMediaType,
        os: "linux",
        size: 1024,
      },
    },
    runtimeIdentity: {
      codexBinaryPath: "/opt/portable-codex/bin/codex",
      codexBinarySha256: "f".repeat(64),
      codexVersion: manifest.runtime.codexVersion,
      platformImageDigest: manifest.runtime.imageDigest,
    },
  };
  const request = createWriterLaunchAttemptOperationRequest({
    expectedSession: preLaunchSession,
    generation,
    measuredImage,
    supervisor: { contractVersion: 1, supervisorId: SUPERVISOR_ID },
  });
  const operationBinding = assertSessionOperationBinding({
    expectedSession: preLaunchSession,
    kind: WRITER_LAUNCH_ATTEMPT_OPERATION_KIND,
    operationId: LAUNCH_ATTEMPT_ID,
    request,
  });
  const result = {
    evidence: {
      contractVersion: 1,
      launchAttemptId: LAUNCH_ATTEMPT_ID,
      processIncarnationId: PROCESS_INCARNATION_ID,
      proofId: START_PROOF_ID,
      status: "started",
      supervisorId: SUPERVISOR_ID,
      writerIncarnationId: WRITER_INCARNATION_ID,
    },
    outcome: "writer-launch-started",
    resultVersion: 1,
  };
  const launch = {
    attachmentId: attachment.attachmentId,
    attachmentSha256: canonicalSha256(request.attachment),
    contractVersion: 1,
    fencingEpoch: lease.fencingEpoch,
    generation: request.generation,
    launchAttemptId: LAUNCH_ATTEMPT_ID,
    launchResultSha256: canonicalSha256(result),
    leaseId: lease.leaseId,
    leaseSha256: canonicalSha256(request.lease),
    measuredImageSha256: canonicalSha256(request.measuredImage),
    processIncarnationId: PROCESS_INCARNATION_ID,
    startedAt: LAUNCH_COMMITTED_AT,
    supervisorId: SUPERVISOR_ID,
    supervisorProofId: START_PROOF_ID,
    writerIncarnationId: WRITER_INCARNATION_ID,
  };
  const expectedSession = {
    createdAt: CREATED_AT,
    document: {
      ...structuredClone(preLaunchSession.document),
      activeOperation: null,
      lastOperation: terminalPointer({
        binding: operationBinding,
        operationRevision: "2",
        result,
      }),
      launch,
    },
    revision: "8",
    sessionId: SESSION_ID,
    updatedAt: LAUNCH_COMMITTED_AT,
  };
  const fenceRequest = {
    backendId: attachment.backendId,
    contractVersion: 1,
    fencingEpoch: "8",
    operationId: FENCE_OPERATION_ID,
    revokedFence: {
      fencingEpoch: lease.fencingEpoch,
      holderId: lease.holderId,
      leaseId: lease.leaseId,
    },
    sessionId: SESSION_ID,
    storageId: attachment.storageId,
    target: { attachmentId: attachment.attachmentId, kind: "attachment" },
  };
  const activeOperation = {
    conflictClass: SESSION_OPERATION_CONFLICT_CLASS,
    expectedSessionRevision: expectedSession.revision,
    kind: WRITER_FORCE_FENCE_OPERATION_KIND,
    operationId: FENCE_OPERATION_ID,
    operationRevision: "1",
    requestSha256: "4".repeat(64),
    reservationId: "reservation-writer-force-fence-001",
    state: "starting",
  };
  const currentSession = {
    createdAt: CREATED_AT,
    document: {
      ...structuredClone(expectedSession.document),
      activeOperation,
      lifecycle: "FENCING",
      writerEpoch: fenceRequest.fencingEpoch,
    },
    revision: "10",
    sessionId: SESSION_ID,
    updatedAt: FENCE_STARTED_AT,
  };
  const operation = {
    conflictClass: SESSION_OPERATION_CONFLICT_CLASS,
    createdAt: LAUNCH_PREPARED_AT,
    expectedSession: preLaunchSession,
    kind: WRITER_LAUNCH_ATTEMPT_OPERATION_KIND,
    operationId: LAUNCH_ATTEMPT_ID,
    request,
    requestSha256: operationBinding.requestSha256,
    result,
    retiredAt: LAUNCH_COMMITTED_AT,
    revision: "2",
    sessionId: SESSION_ID,
    state: "committed",
    updatedAt: LAUNCH_COMMITTED_AT,
  };
  const reservation = {
    conflictClass: SESSION_OPERATION_CONFLICT_CLASS,
    createdAt: LAUNCH_PREPARED_AT,
    expectedSessionRevision: preLaunchSession.revision,
    expiresAt: null,
    kind: WRITER_LAUNCH_ATTEMPT_OPERATION_KIND,
    operationId: LAUNCH_ATTEMPT_ID,
    releasedAt: LAUNCH_COMMITTED_AT,
    requestSha256: operationBinding.requestSha256,
    reservationId: operationBinding.reservationId,
    sessionId: SESSION_ID,
    state: "released",
    updatedAt: LAUNCH_COMMITTED_AT,
  };
  const forceReceipt = exact({
    expectedSession: deepFreeze(expectedSession),
    fenceRequest: deepFreeze(fenceRequest),
    operationId: FENCE_OPERATION_ID,
    writerEpoch: fenceRequest.fencingEpoch,
  });
  const launchReceipt = deepFreeze({
    status: "committed",
    session: currentSession,
    operation,
    reservation,
    attempt: {
      contractVersion: 1,
      launchAttemptId: LAUNCH_ATTEMPT_ID,
      request,
      result,
      state: "committed",
    },
    launch,
  });
  const calls = [];
  const authority = Object.freeze({
    async readWriterForceFenceProviderBinding(input) {
      calls.push(["readWriterForceFenceProviderBinding", input]);
      return forceReceipt;
    },
    async readWriterLaunchAttempt(input) {
      calls.push(["readWriterLaunchAttempt", input]);
      return launchReceipt;
    },
  });
  return {
    authority,
    calls,
    currentSession,
    expectedSession,
    fenceRequest: deepFreeze(fenceRequest),
    forceReceipt,
    launch,
    launchReceipt,
    operation,
    request,
    reservation,
    result,
  };
}

function resolver(fixed, options = {}) {
  return createPostgresPodmanVerifiedStopFenceBindingResolver({
    authority: fixed.authority,
    stateOwnerId: STATE_OWNER_ID,
    ...options,
  });
}

function assertBindingError(code) {
  return (error) =>
    error instanceof PostgresPodmanVerifiedStopFenceBindingError &&
    error.code === code &&
    error.retryable === false &&
    Object.isFrozen(error);
}

test("resolves only the exact durable V2 fence-to-launch relation", async () => {
  const fixed = fixture();
  const resolveFenceBinding = resolver(fixed);
  const first = await resolveFenceBinding(fixed.fenceRequest);
  const second = await resolveFenceBinding(fixed.fenceRequest);

  assert.equal(
    POSTGRES_PODMAN_VERIFIED_STOP_FENCE_BINDING_CONTRACT_VERSION,
    1,
  );
  assert.deepEqual(Reflect.ownKeys(first), ["binding", "signal"]);
  assert.deepEqual(Reflect.ownKeys(first.binding), [
    "contractVersion",
    "launch",
    "request",
    "result",
    "stateOwnerId",
  ]);
  assert.equal(first.binding.contractVersion, 1);
  assert.deepEqual(
    canonicalPayload(first.binding.launch),
    canonicalPayload(fixed.launch),
  );
  assert.deepEqual(
    canonicalPayload(first.binding.request),
    canonicalPayload(fixed.request),
  );
  assert.deepEqual(
    canonicalPayload(first.binding.result),
    canonicalPayload(fixed.result),
  );
  assert.equal(first.binding.stateOwnerId, STATE_OWNER_ID);
  assert.equal(first.signal instanceof AbortSignal, true);
  assert.notStrictEqual(first.signal, second.signal);
  assert.equal(Object.getPrototypeOf(first), null);
  assert.equal(Object.isFrozen(first), true);
  assert.equal(Object.isFrozen(first.binding), true);
  assert.deepEqual(fixed.calls, [
    [
      "readWriterForceFenceProviderBinding",
      exact({ operationId: FENCE_OPERATION_ID }),
    ],
    [
      "readWriterLaunchAttempt",
      exact({ operationId: LAUNCH_ATTEMPT_ID, stateOwnerId: STATE_OWNER_ID }),
    ],
    [
      "readWriterForceFenceProviderBinding",
      exact({ operationId: FENCE_OPERATION_ID }),
    ],
    [
      "readWriterLaunchAttempt",
      exact({ operationId: LAUNCH_ATTEMPT_ID, stateOwnerId: STATE_OWNER_ID }),
    ],
  ]);
});

test("rejects unsafe factory inputs without executing accessors, proxies, or thenables", () => {
  const fixed = fixture();
  const code =
    "invalid_postgres_podman_verified_stop_fence_binding_options";
  assert.throws(
    () =>
      createPostgresPodmanVerifiedStopFenceBindingResolver({
        authority: fixed.authority,
        extra: true,
        stateOwnerId: STATE_OWNER_ID,
      }),
    assertBindingError(code),
  );
  assert.throws(
    () =>
      createPostgresPodmanVerifiedStopFenceBindingResolver({
        authority: fixed.authority,
        stateOwnerId: "state-owner:wrong",
      }),
    assertBindingError(code),
  );

  let accessorReads = 0;
  const accessorOptions = { authority: fixed.authority, stateOwnerId: STATE_OWNER_ID };
  Object.defineProperty(accessorOptions, "authority", {
    enumerable: true,
    get() {
      accessorReads += 1;
      return fixed.authority;
    },
  });
  assert.throws(
    () => createPostgresPodmanVerifiedStopFenceBindingResolver(accessorOptions),
    assertBindingError(code),
  );
  assert.equal(accessorReads, 0);

  let proxyTraps = 0;
  const proxy = new Proxy({}, {
    getPrototypeOf() {
      proxyTraps += 1;
      throw new Error("proxy trap must not execute");
    },
    ownKeys() {
      proxyTraps += 1;
      throw new Error("proxy trap must not execute");
    },
  });
  assert.throws(
    () =>
      createPostgresPodmanVerifiedStopFenceBindingResolver({
        authority: proxy,
        stateOwnerId: STATE_OWNER_ID,
      }),
    assertBindingError(code),
  );
  assert.equal(proxyTraps, 0);

  let thenReads = 0;
  const thenableAuthority = {
    readWriterForceFenceProviderBinding:
      fixed.authority.readWriterForceFenceProviderBinding,
    readWriterLaunchAttempt: fixed.authority.readWriterLaunchAttempt,
  };
  Object.defineProperty(thenableAuthority, "then", {
    get() {
      thenReads += 1;
      throw new Error("then getter must not execute");
    },
  });
  assert.throws(
    () =>
      createPostgresPodmanVerifiedStopFenceBindingResolver({
        authority: thenableAuthority,
        stateOwnerId: STATE_OWNER_ID,
      }),
    assertBindingError(code),
  );
  assert.equal(thenReads, 0);
});

test("rejects malformed force-fence requests before any durable read", async () => {
  const fixed = fixture();
  await assert.rejects(
    resolver(fixed)({ ...fixed.fenceRequest, extra: true }),
    assertBindingError(
      "invalid_postgres_podman_verified_stop_fence_binding_request",
    ),
  );
  assert.deepEqual(fixed.calls, []);
});

test("fails closed on crossed, reordered, or unknown durable evidence", async (t) => {
  const cases = [
    ["crossed force operation", (fixed) => exact({
      ...fixed.forceReceipt,
      operationId: "writer-force-fence-crossed",
    })],
    ["advanced fence epoch", (fixed) => exact({
      ...fixed.forceReceipt,
      writerEpoch: "9",
    })],
    ["crossed launch pointer", (fixed) => deepFreeze({
      ...structuredClone(fixed.launchReceipt),
      launch: { ...structuredClone(fixed.launch), launchAttemptId: "crossed" },
    })],
    ["non-terminal attempt", (fixed) => deepFreeze({
      ...structuredClone(fixed.launchReceipt),
      attempt: { ...structuredClone(fixed.launchReceipt.attempt), state: "starting" },
    })],
    ["unknown launch outcome", (fixed) => deepFreeze({
      ...structuredClone(fixed.launchReceipt),
      attempt: {
        ...structuredClone(fixed.launchReceipt.attempt),
        result: {
          ...structuredClone(fixed.result),
          outcome: "writer-launch-not-started",
        },
      },
    })],
    ["unreleased reservation", (fixed) => deepFreeze({
      ...structuredClone(fixed.launchReceipt),
      reservation: {
        ...structuredClone(fixed.reservation),
        releasedAt: null,
        state: "starting",
      },
    })],
    ["crossed current force", (fixed) => deepFreeze({
      ...structuredClone(fixed.launchReceipt),
      session: {
        ...structuredClone(fixed.currentSession),
        document: {
          ...structuredClone(fixed.currentSession.document),
          activeOperation: {
            ...structuredClone(fixed.currentSession.document.activeOperation),
            operationId: "writer-force-fence-crossed",
          },
        },
      },
    })],
  ];
  for (const [name, mutate] of cases) {
    await t.test(name, async () => {
      const fixed = fixture();
      const candidate = mutate(fixed);
      const authority = Object.freeze({
        async readWriterForceFenceProviderBinding() {
          return name.includes("force operation") || name.includes("epoch")
            ? candidate
            : fixed.forceReceipt;
        },
        async readWriterLaunchAttempt() {
          return candidate;
        },
      });
      await assert.rejects(
        resolver({ ...fixed, authority })(fixed.fenceRequest),
        assertBindingError(
          "postgres_podman_verified_stop_fence_binding_outcome_uncertain",
        ),
      );
    });
  }
});

test("rejects non-native collaborator settlements without invoking then", async (t) => {
  await t.test("ordinary thenable", async () => {
    const fixed = fixture();
    let thenReads = 0;
    const thenable = {};
    Object.defineProperty(thenable, "then", {
      get() {
        thenReads += 1;
        throw new Error("then getter must not execute");
      },
    });
    const authority = Object.freeze({
      readWriterForceFenceProviderBinding() {
        return thenable;
      },
      readWriterLaunchAttempt: fixed.authority.readWriterLaunchAttempt,
    });
    await assert.rejects(
      resolver({ ...fixed, authority })(fixed.fenceRequest),
      assertBindingError(
        "postgres_podman_verified_stop_fence_binding_outcome_uncertain",
      ),
    );
    assert.equal(thenReads, 0);
  });

  await t.test("Promise subclass", async () => {
    const fixed = fixture();
    let thenCalls = 0;
    class HostilePromise extends Promise {
      then(...args) {
        thenCalls += 1;
        return super.then(...args);
      }
    }
    const authority = Object.freeze({
      readWriterForceFenceProviderBinding() {
        return HostilePromise.resolve(fixed.forceReceipt);
      },
      readWriterLaunchAttempt: fixed.authority.readWriterLaunchAttempt,
    });
    await assert.rejects(
      resolver({ ...fixed, authority })(fixed.fenceRequest),
      assertBindingError(
        "postgres_podman_verified_stop_fence_binding_outcome_uncertain",
      ),
    );
    assert.equal(thenCalls, 0);
  });
});

test("requires one genuine, fresh AbortSignal per successful resolution", async () => {
  const fixed = fixture();
  const signal = new AbortController().signal;
  const resolveFenceBinding = resolver(fixed, {
    signalFactory: () => signal,
  });
  const first = await resolveFenceBinding(fixed.fenceRequest);
  assert.strictEqual(first.signal, signal);
  await assert.rejects(
    resolveFenceBinding(fixed.fenceRequest),
    assertBindingError(
      "postgres_podman_verified_stop_fence_binding_outcome_uncertain",
    ),
  );

  const invalid = resolver(fixture(), { signalFactory: () => ({ aborted: false }) });
  await assert.rejects(
    invalid(fixture().fenceRequest),
    assertBindingError(
      "postgres_podman_verified_stop_fence_binding_outcome_uncertain",
    ),
  );
});
