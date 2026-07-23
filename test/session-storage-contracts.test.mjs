import assert from "node:assert/strict";
import { types as utilTypes } from "node:util";
import test from "node:test";

import {
  CHECKPOINT_CAPTURE_RECONCILIATION_CONTRACT_VERSION,
  CHECKPOINT_CLASS_POLICIES,
  DEFAULT_AGENT_POLICY,
  DEFAULT_MAX_SUBAGENTS,
  MAX_AGENT_DEPTH,
  MAX_SUBAGENTS,
  PLATFORM_IMAGE_MEDIA_TYPES,
  SESSION_AUTH_MODE,
  SESSION_WORKER_LAYOUT,
  SESSION_WORKER_ROOT,
  SessionStorageContractError,
  assertCanonicalFenceMatch,
  assertCheckpointCaptureReconciliationBackend,
  assertCheckpointClass,
  assertCheckpointDescriptor,
  assertLeaseGrant,
  assertLeaseRenewal,
  assertResolvedPlatformImageMatchesManifest,
  assertSessionAttachment,
  assertSessionAttachmentMatches,
  assertSessionManifest,
  assertSessionProvisionRequest,
  assertSessionProvisionResult,
  assertSessionStorageRef,
  assertStorageBackend,
  assertStorageMutationMatchesLeaseSnapshot,
  assertStorageMutationRequest,
  assertStorageMutationResult,
  checkpointClassPolicy,
  compareFencingEpochs,
  createRootlessWorkerTemplate,
  createSessionManifest,
  parseFencingEpoch,
  parseSessionManifest,
  serializeSessionManifest,
} from "../src/session-storage-contracts.mjs";

const RUNTIME_SESSION_ID = "019f2100-0000-7000-8000-000000000001";
const CODEX_THREAD_ID = "019f2100-0000-7000-8000-000000000002";
const CODEX_SESSION_ID = CODEX_THREAD_ID;
const IMAGE_DIGEST = `sha256:${"a".repeat(64)}`;

function manifestInput() {
  return {
    sessionId: RUNTIME_SESSION_ID,
    codex: {
      rootThreadId: CODEX_THREAD_ID,
      sessionId: CODEX_SESSION_ID,
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
  };
}

function sessionManifest() {
  return createSessionManifest(manifestInput());
}

function storageRef() {
  return {
    contractVersion: 1,
    backendId: "single-attach-test",
    storageId: "volume-001",
    sessionId: RUNTIME_SESSION_ID,
  };
}

function lease(overrides = {}) {
  return {
    contractVersion: 1,
    sessionId: RUNTIME_SESSION_ID,
    leaseId: "lease-001",
    holderId: "host-001",
    fencingEpoch: "9007199254740993",
    expiresAt: "2026-07-02T12:00:30.000Z",
    ...overrides,
  };
}

function attachment(overrides = {}) {
  return {
    contractVersion: 1,
    backendId: "single-attach-test",
    storageId: "volume-001",
    sessionId: RUNTIME_SESSION_ID,
    attachmentId: "attachment-001",
    leaseId: "lease-001",
    holderId: "host-001",
    fencingEpoch: "9007199254740993",
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
    backendId: "single-attach-test",
    storageId: "volume-001",
    sessionId: RUNTIME_SESSION_ID,
    codexThreadId: CODEX_THREAD_ID,
    codexSessionId: CODEX_SESSION_ID,
    imageDigest: IMAGE_DIGEST,
    sourceFencingEpoch: "9007199254740993",
    checkpointClass: "crash-prefix",
    createdAt: "2026-07-02T12:00:00.000Z",
    ...overrides,
  };
}

function storageBackend({ atomicPointInTimeCheckpoint = true } = {}) {
  const operation = async () => {};
  return {
    contractVersion: 1,
    backendId: "single-attach-test",
    capabilities: {
      atomicPointInTimeCheckpoint,
      exclusiveWriterAttachment: true,
      fencing: "epoch-enforced",
      normalDirectoryAttachment: true,
    },
    captureCheckpoint: operation,
    destroySession: operation,
    detachAttachment: operation,
    forceFence: operation,
    prepareWritableAttachment: operation,
    provisionSession: operation,
    restoreCheckpoint: operation,
  };
}

function provisionRequest(overrides = {}) {
  return {
    contractVersion: 1,
    backendId: "single-attach-test",
    sessionId: RUNTIME_SESSION_ID,
    operationId: "operation-provision-001",
    ...overrides,
  };
}

function mutationRequest(overrides = {}) {
  const operation = overrides.operation ?? "checkpoint";
  const targets = {
    attach: { attachmentId: "attachment-001", kind: "attachment" },
    checkpoint: {
      artifactId: "artifact-001",
      checkpointId: "checkpoint-001",
      kind: "checkpoint",
    },
    destroy: { kind: "storage", storageId: "volume-001" },
    detach: { attachmentId: "attachment-001", kind: "attachment" },
    restore: {
      artifactId: "artifact-001",
      checkpointId: "checkpoint-001",
      kind: "checkpoint",
    },
  };
  return {
    contractVersion: 1,
    backendId: "single-attach-test",
    storageId: "volume-001",
    sessionId: RUNTIME_SESSION_ID,
    leaseId: "lease-001",
    holderId: "host-001",
    fencingEpoch: "9007199254740993",
    operation,
    operationId: `operation-${operation}-001`,
    target: targets[operation],
    ...overrides,
  };
}

function assertCode(code) {
  return (error) => error instanceof SessionStorageContractError && error.code === code;
}

test("session manifest captures immutable Codex and runtime identity with fixed defaults", () => {
  const input = manifestInput();
  const manifest = createSessionManifest(input);
  assert.equal(manifest.sessionId, RUNTIME_SESSION_ID);
  assert.deepEqual(manifest.codex, input.codex);
  assert.deepEqual(manifest.agents, {
    defaultMaxSubagents: 6,
    maxDepth: 2,
    maxSubagents: 10,
  });
  assert.equal(manifest.authMode, SESSION_AUTH_MODE);
  assert.equal(manifest.layoutVersion, 1);
  assert(Object.isFrozen(manifest));
  assert(Object.isFrozen(manifest.codex));
  input.codex.rootThreadId = "019f2100-0000-7000-8000-000000000099";
  assert.equal(manifest.codex.rootThreadId, CODEX_THREAD_ID);

  assert.equal(DEFAULT_MAX_SUBAGENTS, 6);
  assert.equal(MAX_SUBAGENTS, 10);
  assert.equal(MAX_AGENT_DEPTH, 2);
  assert.deepEqual(DEFAULT_AGENT_POLICY, manifest.agents);
  assert.deepEqual(PLATFORM_IMAGE_MEDIA_TYPES, [
    "application/vnd.oci.image.manifest.v1+json",
    "application/vnd.docker.distribution.manifest.v2+json",
  ]);
});

test("session manifest round-trips canonically and rejects duplicate JSON keys", () => {
  const manifest = sessionManifest();
  const serialized = serializeSessionManifest(manifest);
  assert(serialized.endsWith("\n"));
  assert.deepEqual(parseSessionManifest(serialized), manifest);
  const reordered = {
    agents: {
      maxDepth: manifest.agents.maxDepth,
      maxSubagents: manifest.agents.maxSubagents,
      defaultMaxSubagents: manifest.agents.defaultMaxSubagents,
    },
    authMode: manifest.authMode,
    codex: {
      historyMode: manifest.codex.historyMode,
      ephemeral: manifest.codex.ephemeral,
      sessionId: manifest.codex.sessionId,
      rootThreadId: manifest.codex.rootThreadId,
    },
    layoutVersion: manifest.layoutVersion,
    runtime: {
      codexSandbox: manifest.runtime.codexSandbox,
      codexVersion: manifest.runtime.codexVersion,
      platform: manifest.runtime.platform,
      imageMediaType: manifest.runtime.imageMediaType,
      imageDigest: manifest.runtime.imageDigest,
    },
    schemaVersion: manifest.schemaVersion,
    sessionId: manifest.sessionId,
  };
  assert.equal(serializeSessionManifest(reordered), serialized);
  const duplicate = serialized.replace(
    '"schemaVersion": 1',
    '"schemaVersion": 9, "schemaVersion": 1',
  );
  assert.throws(
    () => parseSessionManifest(duplicate),
    (error) => assertCode("invalid_session_manifest")(error) && /duplicate/.test(error.message),
  );
  const deeplyNested = `${"[".repeat(32)}0${"]".repeat(32)}`;
  assert.throws(
    () => parseSessionManifest(deeplyNested),
    (error) =>
      assertCode("invalid_session_manifest")(error) && /nesting depth/.test(error.message),
  );
});

test("session manifest rejects mutable identity, credentials, tags, and unsupported layouts", () => {
  const manifest = sessionManifest();
  const coercingLimit = {
    [Symbol.toPrimitive]() {
      throw new Error("agent limit coercion must not execute");
    },
  };
  for (const invalid of [
    { ...manifest, authJson: "/session/codex-home/auth.json" },
    { ...manifest, refreshToken: "secret" },
    { ...manifest, codex: { ...manifest.codex, ephemeral: true } },
    {
      ...manifest,
      codex: {
        ...manifest.codex,
        sessionId: "019f2100-0000-7000-8000-000000000099",
      },
    },
    { ...manifest, runtime: { ...manifest.runtime, imageDigest: "runtime:latest" } },
    {
      ...manifest,
      runtime: {
        ...manifest.runtime,
        imageMediaType: "application/vnd.oci.image.index.v1+json",
      },
    },
    { ...manifest, runtime: { ...manifest.runtime, platform: "darwin/arm64" } },
    {
      ...manifest,
      runtime: {
        ...manifest.runtime,
        codexVersion: `codex-cli 1.2.3-${"a".repeat(128)}`,
      },
    },
    {
      ...manifest,
      runtime: {
        ...manifest.runtime,
        codexVersion: "codex-cli 0.142.4+builder01.corp.internal",
      },
    },
    {
      ...manifest,
      runtime: {
        ...manifest.runtime,
        codexVersion: "codex-cli 0.142.4-sk-secret-sentinel",
      },
    },
    { ...manifest, layoutVersion: 2 },
    { ...manifest, agents: { ...manifest.agents, defaultMaxSubagents: 11 } },
    { ...manifest, agents: { ...manifest.agents, maxSubagents: 11 } },
    { ...manifest, agents: { ...manifest.agents, maxSubagents: coercingLimit } },
    { ...manifest, agents: { ...manifest.agents, maxDepth: 3 } },
  ]) {
    assert.throws(() => assertSessionManifest(invalid), assertCode("invalid_session_manifest"));
  }
});

test("session manifest UUID validation ignores post-import RegExp poisoning", () => {
  const invalid = {
    ...sessionManifest(),
    sessionId: "not-a-uuid",
  };
  const execDescriptor = Object.getOwnPropertyDescriptor(
    RegExp.prototype,
    "exec",
  );
  const testDescriptor = Object.getOwnPropertyDescriptor(
    RegExp.prototype,
    "test",
  );
  let poisonedCalls = 0;
  let validationError;
  try {
    Object.defineProperty(RegExp.prototype, "exec", {
      ...execDescriptor,
      value() {
        poisonedCalls += 1;
        return ["forged UUID match"];
      },
    });
    Object.defineProperty(RegExp.prototype, "test", {
      ...testDescriptor,
      value() {
        poisonedCalls += 1;
        return true;
      },
    });
    try {
      assertSessionManifest(invalid);
    } catch (error) {
      validationError = error;
    }
  } finally {
    Object.defineProperty(RegExp.prototype, "exec", execDescriptor);
    Object.defineProperty(RegExp.prototype, "test", testDescriptor);
  }

  assert.equal(poisonedCalls, 0);
  assert.ok(assertCode("invalid_session_manifest")(validationError));
});

test("session history validation ignores post-import Array prototype poisoning", () => {
  const invalid = {
    ...sessionManifest(),
    codex: {
      ...sessionManifest().codex,
      historyMode: "future-history",
    },
  };
  const everyDescriptor = Object.getOwnPropertyDescriptor(
    Array.prototype,
    "every",
  );
  const includesDescriptor = Object.getOwnPropertyDescriptor(
    Array.prototype,
    "includes",
  );
  let poisonedCalls = 0;
  let validationError;
  try {
    Object.defineProperty(Array.prototype, "every", {
      ...everyDescriptor,
      value() {
        poisonedCalls += 1;
        return true;
      },
    });
    Object.defineProperty(Array.prototype, "includes", {
      ...includesDescriptor,
      value() {
        poisonedCalls += 1;
        return true;
      },
    });
    try {
      assertSessionManifest(invalid);
    } catch (error) {
      validationError = error;
    }
  } finally {
    Object.defineProperty(Array.prototype, "every", everyDescriptor);
    Object.defineProperty(Array.prototype, "includes", includesDescriptor);
  }

  assert.equal(poisonedCalls, 0);
  assert.ok(assertCode("invalid_session_manifest")(validationError));
});

test("session manifest validation uses captured static intrinsics", () => {
  const manifest = sessionManifest();
  const targets = [
    [Array, "isArray"],
    [Number, "isSafeInteger"],
    [Object, "freeze"],
    [Object, "getOwnPropertyDescriptor"],
    [Object, "getPrototypeOf"],
    [Object, "hasOwn"],
    [Object, "isFrozen"],
    [Object, "values"],
    [Reflect, "apply"],
    [Reflect, "ownKeys"],
    [utilTypes, "isProxy"],
  ].map(([owner, key]) => ({
    descriptor: Object.getOwnPropertyDescriptor(owner, key),
    key,
    owner,
  }));
  let poisonedCalls = 0;
  let validated;
  let validationError;
  try {
    for (const target of targets) {
      Object.defineProperty(target.owner, target.key, {
        ...target.descriptor,
        value() {
          poisonedCalls += 1;
          throw new Error(`poisoned ${target.key}`);
        },
      });
    }
    try {
      validated = assertSessionManifest(manifest);
    } catch (error) {
      validationError = error;
    }
  } finally {
    for (const target of targets) {
      Object.defineProperty(
        target.owner,
        target.key,
        target.descriptor,
      );
    }
  }

  assert.equal(validationError, undefined);
  assert.equal(poisonedCalls, 0);
  assert.deepEqual(validated, manifest);
  assert.equal(Object.isFrozen(validated), true);
});

test("trusted OCI resolution must match the recorded platform manifest", () => {
  const resolution = {
    codexVersion: "codex-cli 0.142.4",
    digest: IMAGE_DIGEST,
    mediaType: "application/vnd.oci.image.manifest.v1+json",
    platform: "linux/arm64",
  };
  assert.deepEqual(
    assertResolvedPlatformImageMatchesManifest({
      manifest: sessionManifest(),
      resolution,
    }),
    resolution,
  );
  assert.throws(
    () => assertResolvedPlatformImageMatchesManifest(),
    assertCode("invalid_image_resolution"),
  );
  assert.throws(
    () =>
      assertResolvedPlatformImageMatchesManifest({
        manifest: sessionManifest(),
        resolution,
        trusted: true,
      }),
    assertCode("invalid_image_resolution"),
  );
  for (const invalid of [
    { ...resolution, digest: `sha256:${"b".repeat(64)}` },
    { ...resolution, mediaType: "application/vnd.oci.image.index.v1+json" },
    { ...resolution, platform: "linux/amd64" },
    { ...resolution, codexVersion: "codex-cli 9.9.9" },
  ]) {
    assert.throws(
      () =>
        assertResolvedPlatformImageMatchesManifest({
          manifest: sessionManifest(),
          resolution: invalid,
        }),
      assertCode("invalid_image_resolution"),
    );
  }
});

test("fencing epochs use canonical uint64 decimal strings without Number precision loss", () => {
  assert.equal(parseFencingEpoch("9007199254740993"), 9_007_199_254_740_993n);
  assert.equal(compareFencingEpochs("9007199254740993", "9007199254740994"), -1);
  assert.equal(compareFencingEpochs("18446744073709551615", "18446744073709551615"), 0);
  for (const invalid of [
    0,
    "0",
    "01",
    "-1",
    "1e3",
    "18446744073709551616",
    "9".repeat(1_000),
  ]) {
    assert.throws(() => parseFencingEpoch(invalid), assertCode("invalid_fence"));
  }
});

test("lease renewal preserves the writer fence and extends authority time", () => {
  const before = lease();
  const after = lease({ expiresAt: "2026-07-02T12:01:00.000Z" });
  const renewalOptions = {
    canonical: before,
    now: Date.parse("2026-07-02T12:00:00.000Z"),
  };
  assert.deepEqual(assertLeaseRenewal(before, after, renewalOptions), after);
  assert.throws(
    () => assertLeaseRenewal(before, after),
    assertCode("invalid_fence"),
  );
  assert.throws(
    () =>
      assertLeaseRenewal(before, after, {
        ...renewalOptions,
        [Symbol("stopProof")]: "not-authority",
      }),
    assertCode("invalid_fence"),
  );
  for (const invalid of [
    lease({ leaseId: "lease-002", expiresAt: "2026-07-02T12:01:00.000Z" }),
    lease({ fencingEpoch: "9007199254740994", expiresAt: "2026-07-02T12:01:00.000Z" }),
  ]) {
    assert.throws(
      () => assertLeaseRenewal(before, invalid, renewalOptions),
      assertCode("stale_fence"),
    );
  }
  assert.throws(
    () =>
      assertLeaseRenewal(before, lease({ expiresAt: before.expiresAt }), renewalOptions),
    assertCode("invalid_fence"),
  );
  assert.throws(
    () =>
      assertLeaseRenewal(before, after, {
        canonical: before,
        now: Date.parse(before.expiresAt),
      }),
    assertCode("lease_expired"),
  );
  assert.throws(
    () =>
      assertLeaseRenewal(before, after, {
        canonical: lease({ fencingEpoch: "9007199254740994" }),
        now: renewalOptions.now,
      }),
    assertCode("stale_fence"),
  );
});

test("canonical fence matching rejects stale writers and expired authority", () => {
  const canonical = lease();
  assert.deepEqual(
    assertCanonicalFenceMatch({
      canonical,
      presented: lease({ expiresAt: "2026-07-02T12:00:20.000Z" }),
      now: Date.parse("2026-07-02T12:00:00.000Z"),
    }),
    canonical,
  );
  assert.throws(
    () => assertCanonicalFenceMatch({ canonical, presented: canonical }),
    assertCode("invalid_fence"),
  );
  assert.throws(
    () =>
      assertCanonicalFenceMatch({
        canonical,
        now: Date.parse("2026-07-02T12:00:00.000Z"),
        presented: canonical,
        stopProof: "not-authority",
      }),
    assertCode("invalid_fence"),
  );
  assert.throws(
    () =>
      assertCanonicalFenceMatch({
        canonical,
        presented: lease({ fencingEpoch: "9007199254740992" }),
        now: Date.parse("2026-07-02T12:00:00.000Z"),
      }),
    assertCode("stale_fence"),
  );
  assert.throws(
    () =>
      assertCanonicalFenceMatch({
        canonical,
        presented: canonical,
        now: Date.parse(canonical.expiresAt),
      }),
    assertCode("lease_expired"),
  );
});

test("storage references and attachments contain no host path in portable state", () => {
  assert.deepEqual(assertSessionStorageRef(storageRef()), storageRef());
  assert.deepEqual(assertSessionAttachment(attachment()), attachment());
  assert.equal(Object.hasOwn(storageRef(), "rootPath"), false);
  assert.throws(
    () => assertSessionStorageRef({ ...storageRef(), rootPath: "/host/path" }),
    assertCode("invalid_storage_ref"),
  );
  for (const invalid of [
    attachment({ rootPath: "relative/session" }),
    attachment({ kind: "block-device" }),
    attachment({ mode: "read-only" }),
    attachment({ rootPath: "/" }),
    attachment({ rootPath: "/var/lib/../etc" }),
    attachment({ rootPath: "/var/lib/portable-codex/\0session" }),
    attachment({ fencingEpoch: "0" }),
    attachment({ fencingEpoch: "18446744073709551616" }),
    { ...attachment(), rawDevice: "/dev/disk9" },
  ]) {
    assert.throws(() => assertSessionAttachment(invalid), assertCode("invalid_storage_attachment"));
  }
});

test("rootless worker template is structural and fixed-layout", () => {
  const currentLease = lease();
  const matched = assertSessionAttachmentMatches({
    attachment: attachment(),
    lease: currentLease,
    manifest: sessionManifest(),
    storageRef: storageRef(),
  });
  assert.equal(matched.attachment.rootPath, attachment().rootPath);
  assert.equal(matched.lease.fencingEpoch, currentLease.fencingEpoch);
  assert(Object.isFrozen(matched));
  const template = createRootlessWorkerTemplate({
    attachment: attachment(),
    lease: currentLease,
    manifest: sessionManifest(),
    storageRef: storageRef(),
  });
  assert.deepEqual(template.mount, {
    propagation: "rprivate",
    readOnly: false,
    source: attachment().rootPath,
    target: SESSION_WORKER_ROOT,
    type: "bind",
  });
  assert.deepEqual(template.env, {
    CODEX_HOME: SESSION_WORKER_LAYOUT.codexHome,
    CODEX_SQLITE_HOME: SESSION_WORKER_LAYOUT.codexHome,
  });
  assert.deepEqual(template.codexConfig, {
    cliOverrides: {
      sqlite_home: SESSION_WORKER_LAYOUT.codexHome,
    },
    deniedRequestOverrideKeys: ["sqlite_home"],
    requiredEffectiveValues: {
      sqlite_home: SESSION_WORKER_LAYOUT.codexHome,
    },
  });
  assert(Object.isFrozen(template.codexConfig));
  assert(Object.isFrozen(template.codexConfig.cliOverrides));
  assert(Object.isFrozen(template.codexConfig.deniedRequestOverrideKeys));
  assert(Object.isFrozen(template.codexConfig.requiredEffectiveValues));
  assert.equal(template.cwd, SESSION_WORKER_LAYOUT.workspace);
  assert.equal(template.rootless, true);
  assert.equal(template.codexSandbox, "danger-full-access");
  assert.deepEqual(template.auth, {
    authJsonPolicy: "forbidden",
    mode: SESSION_AUTH_MODE,
  });
  assert.equal(Object.hasOwn(template.auth, "accessToken"), false);
  assert.equal(Object.hasOwn(template.auth, "refreshToken"), false);
  assert.equal(Object.hasOwn(template, "authority"), false);
  assert.equal(Object.hasOwn(template, "launch"), false);

  assert.throws(
    () => createRootlessWorkerTemplate(),
    assertCode("invalid_worker_template"),
  );
  assert.throws(
    () =>
      createRootlessWorkerTemplate({
        attachment: attachment(),
        lease: currentLease,
        manifest: sessionManifest(),
        storageRef: storageRef(),
        stopProof: "not-authority",
      }),
    assertCode("invalid_worker_template"),
  );

  assert.throws(
    () => createRootlessWorkerTemplate({
      attachment: attachment({ fencingEpoch: "9007199254740992" }),
      lease: currentLease,
      manifest: sessionManifest(),
      storageRef: storageRef(),
    }),
    assertCode("stale_fence"),
  );
});

test("storage backend contract requires directory, exclusivity, fencing, and all operations", () => {
  const backend = storageBackend();
  assert.equal(assertStorageBackend(backend), backend);
  assert.throws(
    () => assertStorageBackend({ ...backend, forceFence: undefined }),
    assertCode("invalid_storage_backend"),
  );
  assert.throws(
    () =>
      assertStorageBackend({
        ...backend,
        capabilities: { ...backend.capabilities, exclusiveWriterAttachment: false },
      }),
    assertCode("invalid_storage_backend"),
  );
});

test("checkpoint capture reconciliation is an optional versioned backend extension", () => {
  const base = storageBackend();
  assert.equal(CHECKPOINT_CAPTURE_RECONCILIATION_CONTRACT_VERSION, 1);
  assert.equal(assertStorageBackend(base), base);
  assert.throws(
    () => assertCheckpointCaptureReconciliationBackend(base),
    assertCode("invalid_storage_backend"),
  );

  const reconcileCheckpointCapture = async () => {};
  const extended = {
    ...base,
    captureReconciliationContractVersion:
      CHECKPOINT_CAPTURE_RECONCILIATION_CONTRACT_VERSION,
    reconcileCheckpointCapture,
  };
  assert.equal(assertStorageBackend(extended), extended);
  assert.equal(
    assertCheckpointCaptureReconciliationBackend(extended),
    extended,
  );

  for (const invalid of [
    { ...extended, captureReconciliationContractVersion: 2 },
    { ...extended, reconcileCheckpointCapture: undefined },
  ]) {
    assert.throws(
      () => assertCheckpointCaptureReconciliationBackend(invalid),
      assertCode("invalid_storage_backend"),
    );
  }
});

test("storage provisioning is an idempotent control-plane mutation without writer authority", () => {
  const request = provisionRequest();
  assert.deepEqual(assertSessionProvisionRequest(request), request);
  assert.equal(Object.hasOwn(request, "leaseId"), false);
  assert.equal(Object.hasOwn(request, "fencingEpoch"), false);
  const result = {
    ...request,
    proofId: "proof-provision-001",
    status: "provisioned",
    storageId: "volume-001",
  };
  assert.deepEqual(assertSessionProvisionResult(result, { request }), result);
  assert.deepEqual(
    assertSessionProvisionResult(result, { previousResult: result, request }),
    result,
  );
  assert.throws(
    () => assertSessionProvisionResult(result),
    assertCode("invalid_storage_provision"),
  );
  assert.throws(
    () =>
      assertSessionProvisionResult(
        { ...result, operationId: "operation-provision-002" },
        { request },
      ),
    assertCode("invalid_storage_provision"),
  );
  assert.throws(
    () =>
      assertSessionProvisionResult(
        { ...result, storageId: "volume-002" },
        { previousResult: result, request },
      ),
    assertCode("invalid_storage_provision"),
  );
  assert.throws(
    () => assertSessionProvisionRequest({ ...request, leaseId: "lease-001" }),
    assertCode("invalid_storage_provision"),
  );
  assert.throws(
    () => assertStorageMutationRequest({ ...mutationRequest(), operation: "provision" }),
    assertCode("invalid_storage_mutation"),
  );
});

test("storage mutation envelopes bind operation IDs to the complete writer fence", () => {
  const request = mutationRequest();
  assert.deepEqual(assertStorageMutationRequest(request), request);
  for (const fencingEpoch of ["0", "18446744073709551616"]) {
    assert.throws(
      () => assertStorageMutationRequest({ ...request, fencingEpoch }),
      assertCode("invalid_storage_mutation"),
    );
  }
  assert.deepEqual(
    assertStorageMutationMatchesLeaseSnapshot({
      canonicalLease: lease(),
      now: Date.parse("2026-07-02T12:00:00.000Z"),
      request,
      storageRef: storageRef(),
    }),
    request,
  );
  assert.throws(
    () => assertStorageMutationMatchesLeaseSnapshot(null),
    assertCode("invalid_storage_mutation"),
  );
  assert.throws(
    () =>
      assertStorageMutationMatchesLeaseSnapshot({
        canonicalLease: lease(),
        now: Date.parse("2026-07-02T12:00:00.000Z"),
        request,
        storageRef: storageRef(),
        stopProof: "not-authority",
      }),
    assertCode("invalid_storage_mutation"),
  );
  const result = {
    ...request,
    proofId: "proof-checkpoint-001",
    status: "checkpoint-created",
  };
  assert.deepEqual(assertStorageMutationResult(result, { request }), result);
  assert.throws(
    () => assertStorageMutationResult(result),
    assertCode("invalid_storage_mutation"),
  );
  const hiddenResultOptions = { request };
  Object.defineProperty(hiddenResultOptions, "stopProof", {
    enumerable: false,
    value: "not-authority",
  });
  assert.throws(
    () => assertStorageMutationResult(result, hiddenResultOptions),
    assertCode("invalid_storage_mutation"),
  );
  assert.throws(
    () =>
      assertStorageMutationResult({ ...result, status: "detached" }, { request }),
    assertCode("invalid_storage_mutation"),
  );
  assert.throws(
    () =>
      assertStorageMutationResult(
        {
          ...result,
          target: {
            artifactId: "artifact-001",
            checkpointId: "checkpoint-002",
            kind: "checkpoint",
          },
        },
        { request },
      ),
    assertCode("invalid_storage_mutation"),
  );
  assert.throws(
    () =>
      assertStorageMutationResult(
        { ...result, operationId: "operation-checkpoint-002" },
        { request },
      ),
    assertCode("invalid_storage_mutation"),
  );
  assert.throws(
    () =>
      assertStorageMutationResult(
        { ...result, fencingEpoch: "9007199254740994" },
        { request },
      ),
    assertCode("stale_fence"),
  );
  for (const [operation, target] of [
    ["attach", { attachmentId: "attachment-001", kind: "attachment" }],
    [
      "checkpoint",
      {
        artifactId: "artifact-001",
        checkpointId: "checkpoint-001",
        kind: "checkpoint",
      },
    ],
    ["destroy", { kind: "storage", storageId: "volume-001" }],
    ["detach", { attachmentId: "attachment-001", kind: "attachment" }],
    [
      "restore",
      {
        artifactId: "artifact-001",
        checkpointId: "checkpoint-001",
        kind: "checkpoint",
      },
    ],
  ]) {
    assert.deepEqual(assertStorageMutationRequest(mutationRequest({ operation })).target, target);
  }
  for (const [operation, field] of [
    ["attach", "attachmentId"],
    ["checkpoint", "artifactId"],
    ["checkpoint", "checkpointId"],
    ["destroy", "storageId"],
    ["detach", "attachmentId"],
    ["restore", "artifactId"],
    ["restore", "checkpointId"],
  ]) {
    const invalid = mutationRequest({ operation });
    assert.throws(
      () =>
        assertStorageMutationRequest({
          ...invalid,
          target: { ...invalid.target, [field]: undefined },
        }),
      assertCode("invalid_storage_mutation"),
    );
  }
  assert.throws(
    () =>
      assertStorageMutationRequest(
        mutationRequest({
          operation: "detach",
          target: { attachmentId: "attachment-001", kind: "checkpoint" },
        }),
      ),
    assertCode("invalid_storage_mutation"),
  );
  const takeover = lease({
    leaseId: "lease-002",
    fencingEpoch: "9007199254740994",
  });
  for (const operation of ["checkpoint", "destroy", "detach"]) {
    assert.throws(
      () =>
        assertStorageMutationMatchesLeaseSnapshot({
          canonicalLease: takeover,
          now: Date.parse("2026-07-02T12:00:00.000Z"),
          request: mutationRequest({ operation }),
          storageRef: storageRef(),
        }),
      assertCode("stale_fence"),
    );
  }
  assert.deepEqual(
    assertStorageMutationMatchesLeaseSnapshot({
      allowExpired: true,
      canonicalLease: lease(),
      now: Date.parse(lease().expiresAt),
      request: mutationRequest({ operation: "detach" }),
      storageRef: storageRef(),
    }),
    mutationRequest({ operation: "detach" }),
  );
  for (const allowExpired of ["false", 1]) {
    assert.throws(
      () =>
        assertStorageMutationMatchesLeaseSnapshot({
          allowExpired,
          canonicalLease: lease(),
          now: Date.parse(lease().expiresAt),
          request: mutationRequest({ operation: "detach" }),
          storageRef: storageRef(),
        }),
      assertCode("invalid_storage_mutation"),
    );
  }
  assert.throws(
    () =>
      assertStorageMutationMatchesLeaseSnapshot({
        allowExpired: true,
        canonicalLease: lease(),
        now: Date.parse(lease().expiresAt),
        request,
        storageRef: storageRef(),
      }),
    assertCode("invalid_storage_mutation"),
  );
  assert.throws(
    () =>
      assertStorageMutationMatchesLeaseSnapshot({
        canonicalLease: lease(),
        now: Date.parse("2026-07-02T12:00:00.000Z"),
        request,
        storageRef: { ...storageRef(), backendId: "different-backend" },
      }),
    assertCode("invalid_storage_mutation"),
  );
});

test("checkpoint classes preserve graceful versus crash recovery semantics", () => {
  assert.deepEqual(assertCheckpointClass("clean"), "clean");
  assert.equal(checkpointClassPolicy("graceful-abort").explicitAbortMarker, "required");
  assert.deepEqual(checkpointClassPolicy("crash-prefix"), {
    captureBoundary: "atomic-crash-capture",
    explicitAbortMarker: "must-not-infer",
    requiresTailRepair: true,
    writerBoundary: "stopped-or-fenced",
    writableResume: "after-tail-repair-and-new-lease",
  });
  assert.equal(CHECKPOINT_CLASS_POLICIES.clean.requiresTailRepair, false);
  for (const invalid of ["git-wip", "graceful-interrupt", "crash-consistent"]) {
    assert.throws(() => checkpointClassPolicy(invalid), assertCode("invalid_checkpoint"));
  }
});

test("checkpoint descriptor binds immutable session identity but never restores authority", () => {
  const descriptor = checkpoint();
  assert.deepEqual(
    assertCheckpointDescriptor(descriptor, {
      manifest: sessionManifest(),
      storageRef: storageRef(),
    }),
    descriptor,
  );
  assert.equal(Object.hasOwn(descriptor, "leaseId"), false);
  assert.equal(Object.hasOwn(descriptor, "expiresAt"), false);
  assert.equal(Object.hasOwn(descriptor, "proofId"), false);
  assert.equal(Object.hasOwn(descriptor, "stopProof"), false);
  assert.throws(
    () => assertCheckpointDescriptor(descriptor, null),
    assertCode("invalid_checkpoint"),
  );
  assert.throws(
    () =>
      assertCheckpointDescriptor(descriptor, {
        manifest: sessionManifest(),
        storageRef: storageRef(),
        stopProof: "not-authority",
      }),
    assertCode("invalid_checkpoint"),
  );
  assert.throws(
    () =>
      assertCheckpointDescriptor(
        checkpoint({ codexThreadId: "019f2100-0000-7000-8000-000000000099" }),
      ),
    assertCode("invalid_checkpoint"),
  );
  for (const invalid of [
    checkpoint({ codexThreadId: "019f2100-0000-7000-8000-000000000099" }),
    checkpoint({ imageDigest: `sha256:${"b".repeat(64)}` }),
    checkpoint({ storageId: "volume-002" }),
    checkpoint({ sourceFencingEpoch: "0" }),
    checkpoint({ sourceFencingEpoch: "18446744073709551616" }),
    { ...checkpoint(), leaseId: "lease-001" },
    { ...checkpoint(), authJson: "forbidden" },
    { ...checkpoint(), gitSummary: { branch: "main" } },
  ]) {
    assert.throws(
      () =>
        assertCheckpointDescriptor(invalid, {
          manifest: sessionManifest(),
          storageRef: storageRef(),
        }),
      assertCode("invalid_checkpoint"),
    );
  }
});

test("public validators return frozen defensive copies for portable records", () => {
  const mutableLease = lease();
  const checkedLease = assertLeaseGrant(mutableLease);
  mutableLease.leaseId = "lease-mutated";
  assert.equal(checkedLease.leaseId, "lease-001");
  assert(Object.isFrozen(checkedLease));
  assert(Object.isFrozen(assertSessionStorageRef(storageRef())));
  assert(Object.isFrozen(assertSessionAttachment(attachment())));
});

test("portable record validators reject accessor fields before validation or cloning", () => {
  const accessorLease = lease();
  let leaseReads = 0;
  Object.defineProperty(accessorLease, "fencingEpoch", {
    enumerable: true,
    get() {
      leaseReads += 1;
      return leaseReads === 1 ? "1" : "0";
    },
  });
  assert.throws(() => assertLeaseGrant(accessorLease), assertCode("invalid_fence"));
  assert.equal(leaseReads, 0);

  const accessorAttachment = attachment();
  let pathReads = 0;
  Object.defineProperty(accessorAttachment, "rootPath", {
    enumerable: true,
    get() {
      pathReads += 1;
      return pathReads === 1 ? "/safe/path" : "/";
    },
  });
  assert.throws(
    () => assertSessionAttachment(accessorAttachment),
    assertCode("invalid_storage_attachment"),
  );
  assert.equal(pathReads, 0);

  const hiddenLease = lease();
  Object.defineProperty(hiddenLease, "stopProof", {
    enumerable: false,
    value: "not-authority",
  });
  assert.throws(() => assertLeaseGrant(hiddenLease), assertCode("invalid_fence"));
  assert.throws(
    () => assertLeaseGrant({ ...lease(), [Symbol("authority")]: "not-authority" }),
    assertCode("invalid_fence"),
  );
});

test("portable record validators reject hostile proxies without invoking traps", () => {
  let traps = 0;
  const forged = new SessionStorageContractError(
    "forged_contract_error",
    "secret forged contract detail",
  );
  const hostile = new Proxy(
    {},
    {
      getPrototypeOf() {
        traps += 1;
        throw forged;
      },
      ownKeys() {
        traps += 1;
        throw forged;
      },
    },
  );
  const revoked = Proxy.revocable({}, {});
  revoked.revoke();

  assert.throws(() => assertSessionManifest(hostile), assertCode("invalid_session_manifest"));
  assert.throws(
    () => assertSessionManifest(revoked.proxy),
    assertCode("invalid_session_manifest"),
  );
  assert.equal(traps, 0);
});

test("public option envelopes reject accessors before destructuring", () => {
  const options = { manifest: sessionManifest() };
  let reads = 0;
  Object.defineProperty(options, "resolution", {
    enumerable: true,
    get() {
      reads += 1;
      return {
        digest: IMAGE_DIGEST,
        mediaType: "application/vnd.oci.image.manifest.v1+json",
        platform: "linux/arm64",
      };
    },
  });
  assert.throws(
    () => assertResolvedPlatformImageMatchesManifest(options),
    assertCode("invalid_image_resolution"),
  );
  assert.equal(reads, 0);
});

test("public option envelopes ignore inherited optional fields", () => {
  const descriptor = checkpoint();
  const canonicalLease = lease();
  const detach = mutationRequest({ operation: "detach" });
  const previousAllowExpired = Object.getOwnPropertyDescriptor(
    Object.prototype,
    "allowExpired",
  );
  const previousManifest = Object.getOwnPropertyDescriptor(Object.prototype, "manifest");
  try {
    Object.defineProperty(Object.prototype, "allowExpired", {
      configurable: true,
      value: true,
    });
    Object.defineProperty(Object.prototype, "manifest", {
      configurable: true,
      value: "inherited manifest must be ignored",
    });
    assert.throws(
      () =>
        assertStorageMutationMatchesLeaseSnapshot({
          canonicalLease,
          now: Date.parse(canonicalLease.expiresAt),
          request: detach,
          storageRef: storageRef(),
        }),
      assertCode("lease_expired"),
    );
    assert.deepEqual(assertCheckpointDescriptor(descriptor), descriptor);
  } finally {
    if (previousAllowExpired === undefined) delete Object.prototype.allowExpired;
    else Object.defineProperty(Object.prototype, "allowExpired", previousAllowExpired);
    if (previousManifest === undefined) delete Object.prototype.manifest;
    else Object.defineProperty(Object.prototype, "manifest", previousManifest);
  }
});
