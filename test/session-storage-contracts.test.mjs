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
  PREPARED_CHECKPOINT_CAPTURE_CONTRACT_VERSION,
  RESTORE_ATTACHMENT_ACTIVATION_CONTRACT_VERSION,
  RESTORE_ATTACHMENT_RECONCILIATION_CONTRACT_VERSION,
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
  assertPreparedCheckpointCaptureBackend,
  assertResolvedPlatformImageMatchesManifest,
  assertRestoreCheckpointAdmission,
  assertRestoreAttachmentActivationBackend,
  assertRestoreAttachmentActivationRequest,
  assertRestoreAttachmentActivationResult,
  assertRestoreAttachmentReconciliationBackend,
  assertRestoreAttachmentReconciliationResult,
  assertSessionAttachment,
  assertSessionAttachmentMatches,
  assertSessionManifest,
  assertSessionProvisionRequest,
  assertSessionProvisionResult,
  assertSessionStorageRef,
  assertStorageBackend,
  assertStorageBackendCapabilities,
  assertStorageForceFenceRequest,
  assertStorageForceFenceResult,
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
const OTHER_RUNTIME_SESSION_ID = "019f2100-0000-7000-8000-000000000003";
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

function forceFenceRequest(overrides = {}) {
  return {
    contractVersion: 1,
    backendId: "single-attach-test",
    storageId: "volume-001",
    sessionId: RUNTIME_SESSION_ID,
    fencingEpoch: "9007199254740994",
    operationId: "operation-force-fence-001",
    revokedFence: {
      fencingEpoch: "9007199254740993",
      holderId: "host-001",
      leaseId: "lease-001",
    },
    target: {
      attachmentId: "attachment-001",
      kind: "attachment",
    },
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

function restoreAdmission({ checkpointOverrides = {}, requestOverrides = {} } = {}) {
  return {
    checkpoint: checkpoint({
      checkpointClass: "clean",
      ...checkpointOverrides,
    }),
    request: mutationRequest({
      fencingEpoch: "9007199254740994",
      operation: "restore",
      storageId: "destination-volume-001",
      ...requestOverrides,
    }),
  };
}

function restoreAttachmentPublication(overrides = {}) {
  const root = {
    filesystemId: "test-filesystem-001",
    objectIdentityScheme: "test-persistent-object-v1",
    objectId: "restore-object-001",
    rootPath: "/var/lib/portable-codex/restored-session-001",
    ...(overrides.root ?? {}),
  };
  return {
    artifactManifestDigest: "b".repeat(64),
    coordinatorBindingSha256: "c".repeat(64),
    modeledDigest: "d".repeat(64),
    publicationId: "publication-restore-001",
    publicationKind: "restore-destination",
    root,
    treeIdentityDigest: "e".repeat(64),
    ...overrides,
    root,
  };
}

function restoreAttachmentActivationRequest(overrides = {}) {
  return {
    contractVersion: RESTORE_ATTACHMENT_ACTIVATION_CONTRACT_VERSION,
    lease: lease(),
    manifest: sessionManifest(),
    mutationRequest: mutationRequest({ operation: "attach" }),
    publication: restoreAttachmentPublication(),
    storageRef: storageRef(),
    ...overrides,
  };
}

function restoreAttachmentActivationResult(request, overrides = {}) {
  const {
    attachment: attachmentOverrides = {},
    mutationResult: mutationResultOverrides = {},
    ...resultOverrides
  } = overrides;
  const mutationResult = {
    ...request.mutationRequest,
    proofId: "proof-attachment-001",
    status: "attached",
    ...mutationResultOverrides,
  };
  return {
    attachment: attachment({
      attachmentId: request.mutationRequest.target.attachmentId,
      operationId: request.mutationRequest.operationId,
      proofId: mutationResult.proofId,
      rootPath: request.publication.root.rootPath,
      ...attachmentOverrides,
    }),
    contractVersion: RESTORE_ATTACHMENT_ACTIVATION_CONTRACT_VERSION,
    mutationResult,
    publication: structuredClone(request.publication),
    ...resultOverrides,
    mutationResult,
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

test("attachment matching rejects cross-session authority despite Array every poisoning", () => {
  const otherManifest = createSessionManifest({
    ...manifestInput(),
    sessionId: OTHER_RUNTIME_SESSION_ID,
  });
  const everyDescriptor = Object.getOwnPropertyDescriptor(
    Array.prototype,
    "every",
  );
  let poisonedCalls = 0;
  let poisonedEveryResult;
  let matchError;
  let templateError;
  let template;
  try {
    Object.defineProperty(Array.prototype, "every", {
      ...everyDescriptor,
      value() {
        poisonedCalls += 1;
        return true;
      },
    });
    poisonedEveryResult = [].every(() => false);
    try {
      assertSessionAttachmentMatches({
        attachment: attachment(),
        lease: lease(),
        manifest: otherManifest,
        storageRef: storageRef(),
      });
    } catch (error) {
      matchError = error;
    }
    try {
      template = createRootlessWorkerTemplate({
        attachment: attachment(),
        lease: lease(),
        manifest: otherManifest,
        storageRef: storageRef(),
      });
    } catch (error) {
      templateError = error;
    }
  } finally {
    Object.defineProperty(Array.prototype, "every", everyDescriptor);
  }

  assert(poisonedCalls > 0);
  assert.equal(poisonedEveryResult, true);
  assert.ok(assertCode("stale_fence")(matchError));
  assert.ok(assertCode("stale_fence")(templateError));
  assert.equal(template, undefined);
});

test("attachment paths reject NUL despite String prototype poisoning", () => {
  const charCodeAtDescriptor = Object.getOwnPropertyDescriptor(
    String.prototype,
    "charCodeAt",
  );
  const includesDescriptor = Object.getOwnPropertyDescriptor(
    String.prototype,
    "includes",
  );
  const invalidRootPath = "/var/lib/portable-codex/session-001\0substituted";
  let attachmentError;
  let poisonedCharCodeAtCalls = 0;
  let poisonedCalls = 0;
  let charCodeAtCallsBeforeValidation;
  let callsBeforeValidation;
  try {
    Object.defineProperty(String.prototype, "charCodeAt", {
      ...charCodeAtDescriptor,
      value() {
        poisonedCharCodeAtCalls += 1;
        return 47;
      },
    });
    Object.defineProperty(String.prototype, "includes", {
      ...includesDescriptor,
      value() {
        poisonedCalls += 1;
        return false;
      },
    });
    assert.equal("x".charCodeAt(0), 47);
    assert.equal(invalidRootPath.includes("\0"), false);
    charCodeAtCallsBeforeValidation = poisonedCharCodeAtCalls;
    callsBeforeValidation = poisonedCalls;
    try {
      assertSessionAttachment(attachment({ rootPath: invalidRootPath }));
    } catch (error) {
      attachmentError = error;
    }
  } finally {
    Object.defineProperty(
      String.prototype,
      "charCodeAt",
      charCodeAtDescriptor,
    );
    Object.defineProperty(
      String.prototype,
      "includes",
      includesDescriptor,
    );
  }

  assert.equal(
    poisonedCharCodeAtCalls,
    charCodeAtCallsBeforeValidation,
  );
  assert.equal(poisonedCalls, callsBeforeValidation);
  assert.ok(assertCode("invalid_storage_attachment")(attachmentError));
});

test("worker template ignores post-import clone and freeze poisoning across session bindings", () => {
  const manifest = sessionManifest();
  const storage = storageRef();
  const writerLease = lease();
  const mounted = attachment();
  const originalStructuredClone = globalThis.structuredClone;
  const objectTargets = ["freeze", "isFrozen", "values"].map((key) => ({
    descriptor: Object.getOwnPropertyDescriptor(Object, key),
    key,
  }));
  const alternateRootPath = "/var/lib/portable-codex/other-session";
  let poisonedObjectCalls = 0;
  let poisonedCalls = 0;
  let template;
  try {
    for (const target of objectTargets) {
      Object.defineProperty(Object, target.key, {
        ...target.descriptor,
        value() {
          poisonedObjectCalls += 1;
          throw new Error(`poisoned Object.${target.key}`);
        },
      });
    }
    globalThis.structuredClone = (value) => {
      poisonedCalls += 1;
      const clone = Reflect.apply(originalStructuredClone, globalThis, [value]);
      if (clone && typeof clone === "object" && "sessionId" in clone) {
        clone.sessionId = OTHER_RUNTIME_SESSION_ID;
      }
      if (clone && typeof clone === "object" && "rootPath" in clone) {
        clone.rootPath = alternateRootPath;
      }
      return clone;
    };
    template = createRootlessWorkerTemplate({
      attachment: mounted,
      lease: writerLease,
      manifest,
      storageRef: storage,
    });
  } finally {
    globalThis.structuredClone = originalStructuredClone;
    for (const target of objectTargets) {
      Object.defineProperty(Object, target.key, target.descriptor);
    }
  }

  assert.equal(poisonedCalls, 0);
  assert.equal(poisonedObjectCalls, 0);
  assert.equal(template.mount.source, mounted.rootPath);
  assert.notEqual(template.mount.source, alternateRootPath);
});

test("storage backend contract requires directory, exclusivity, fencing, and all operations", () => {
  const backend = storageBackend();
  const mutableCapabilities = structuredClone(backend.capabilities);
  const checkedCapabilities =
    assertStorageBackendCapabilities(mutableCapabilities);
  mutableCapabilities.fencing = "manual";
  assert.deepEqual(checkedCapabilities, backend.capabilities);
  assert.equal(Object.isFrozen(checkedCapabilities), true);
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

test("prepared checkpoint capture is an optional versioned backend extension", () => {
  const base = storageBackend();
  assert.equal(PREPARED_CHECKPOINT_CAPTURE_CONTRACT_VERSION, 1);
  assert.equal(assertStorageBackend(base), base);
  assert.throws(
    () => assertPreparedCheckpointCaptureBackend(base),
    assertCode("invalid_storage_backend"),
  );

  const extended = {
    ...base,
    preparedCheckpointCaptureContractVersion:
      PREPARED_CHECKPOINT_CAPTURE_CONTRACT_VERSION,
    resumePreparedCheckpointCapture: async () => {},
  };
  assert.equal(assertStorageBackend(extended), extended);
  assert.equal(assertPreparedCheckpointCaptureBackend(extended), extended);

  for (const invalid of [
    { ...extended, preparedCheckpointCaptureContractVersion: 2 },
    { ...extended, resumePreparedCheckpointCapture: undefined },
  ]) {
    assert.throws(
      () => assertPreparedCheckpointCaptureBackend(invalid),
      assertCode("invalid_storage_backend"),
    );
  }

  let reads = 0;
  const accessor = { ...extended };
  Object.defineProperty(accessor, "resumePreparedCheckpointCapture", {
    enumerable: true,
    get() {
      reads += 1;
      return async () => {};
    },
  });
  assert.throws(
    () => assertPreparedCheckpointCaptureBackend(accessor),
    assertCode("invalid_storage_backend"),
  );
  assert.equal(reads, 0);

  let traps = 0;
  const hostile = new Proxy(extended, {
    get() {
      traps += 1;
      throw new Error("proxy trap must not run");
    },
  });
  assert.throws(
    () => assertPreparedCheckpointCaptureBackend(hostile),
    assertCode("invalid_storage_backend"),
  );
  assert.equal(traps, 0);
});

test("restore attachment activation is an optional versioned backend extension", () => {
  const base = storageBackend();
  assert.equal(RESTORE_ATTACHMENT_ACTIVATION_CONTRACT_VERSION, 1);
  assert.equal(assertStorageBackend(base), base);
  assert.throws(
    () => assertRestoreAttachmentActivationBackend(base),
    assertCode("invalid_storage_backend"),
  );

  const extended = {
    ...base,
    restoreAttachmentActivationContractVersion:
      RESTORE_ATTACHMENT_ACTIVATION_CONTRACT_VERSION,
    prepareRestoreAttachment: async () => {},
  };
  assert.equal(assertStorageBackend(extended), extended);
  assert.equal(assertRestoreAttachmentActivationBackend(extended), extended);

  for (const invalid of [
    { ...extended, restoreAttachmentActivationContractVersion: 2 },
    { ...extended, prepareRestoreAttachment: undefined },
  ]) {
    assert.throws(
      () => assertRestoreAttachmentActivationBackend(invalid),
      assertCode("invalid_storage_backend"),
    );
  }

  let reads = 0;
  const accessor = { ...extended };
  Object.defineProperty(accessor, "prepareRestoreAttachment", {
    enumerable: true,
    get() {
      reads += 1;
      return async () => {};
    },
  });
  assert.throws(
    () => assertRestoreAttachmentActivationBackend(accessor),
    assertCode("invalid_storage_backend"),
  );
  assert.equal(reads, 0);

  let traps = 0;
  const hostile = new Proxy(extended, {
    get() {
      traps += 1;
      throw new Error("proxy trap must not run");
    },
  });
  assert.throws(
    () => assertRestoreAttachmentActivationBackend(hostile),
    assertCode("invalid_storage_backend"),
  );
  assert.equal(traps, 0);
});

test("restore attachment reconciliation requires the complete activation extension", () => {
  const base = storageBackend();
  const activation = {
    ...base,
    restoreAttachmentActivationContractVersion:
      RESTORE_ATTACHMENT_ACTIVATION_CONTRACT_VERSION,
    prepareRestoreAttachment: async () => {},
  };
  assert.equal(RESTORE_ATTACHMENT_RECONCILIATION_CONTRACT_VERSION, 1);
  assert.throws(
    () => assertRestoreAttachmentReconciliationBackend(base),
    assertCode("invalid_storage_backend"),
  );
  assert.throws(
    () => assertRestoreAttachmentReconciliationBackend(activation),
    assertCode("invalid_storage_backend"),
  );

  const extended = {
    ...activation,
    reconcileRestoreAttachment: async () => {},
    restoreAttachmentReconciliationContractVersion:
      RESTORE_ATTACHMENT_RECONCILIATION_CONTRACT_VERSION,
  };
  assert.equal(
    assertRestoreAttachmentReconciliationBackend(extended),
    extended,
  );

  for (const invalid of [
    { ...extended, restoreAttachmentActivationContractVersion: 2 },
    { ...extended, prepareRestoreAttachment: undefined },
    { ...extended, restoreAttachmentReconciliationContractVersion: 2 },
    { ...extended, reconcileRestoreAttachment: undefined },
  ]) {
    assert.throws(
      () => assertRestoreAttachmentReconciliationBackend(invalid),
      assertCode("invalid_storage_backend"),
    );
  }

  let reads = 0;
  const accessor = { ...extended };
  Object.defineProperty(accessor, "reconcileRestoreAttachment", {
    enumerable: true,
    get() {
      reads += 1;
      return async () => {};
    },
  });
  assert.throws(
    () => assertRestoreAttachmentReconciliationBackend(accessor),
    assertCode("invalid_storage_backend"),
  );
  assert.equal(reads, 0);

  const inherited = Object.create({
    reconcileRestoreAttachment: async () => {},
    restoreAttachmentReconciliationContractVersion:
      RESTORE_ATTACHMENT_RECONCILIATION_CONTRACT_VERSION,
  });
  Object.assign(inherited, activation);
  assert.throws(
    () => assertRestoreAttachmentReconciliationBackend(inherited),
    assertCode("invalid_storage_backend"),
  );

  let traps = 0;
  const hostile = new Proxy(extended, {
    get() {
      traps += 1;
      throw new Error("proxy trap must not run");
    },
  });
  assert.throws(
    () => assertRestoreAttachmentReconciliationBackend(hostile),
    assertCode("invalid_storage_backend"),
  );
  assert.equal(traps, 0);
});

test("restore attachment activation binds publication, attach mutation, and writer fence", () => {
  const request = restoreAttachmentActivationRequest();
  const result = restoreAttachmentActivationResult(request);
  const checkedRequest = assertRestoreAttachmentActivationRequest(request);
  const checkedResult = assertRestoreAttachmentActivationResult(result, {
    request,
  });

  assert.deepEqual(checkedRequest, request);
  assert.deepEqual(checkedResult, result);
  for (const value of [
    checkedRequest,
    checkedRequest.lease,
    checkedRequest.manifest,
    checkedRequest.mutationRequest,
    checkedRequest.publication,
    checkedRequest.publication.root,
    checkedRequest.storageRef,
    checkedResult,
    checkedResult.attachment,
    checkedResult.mutationResult,
    checkedResult.publication,
    checkedResult.publication.root,
  ]) {
    assert.equal(Object.isFrozen(value), true);
  }

  assert.deepEqual(
    assertRestoreAttachmentActivationRequest(checkedRequest),
    checkedRequest,
  );
  assert.deepEqual(
    assertRestoreAttachmentActivationResult(checkedResult, {
      request: checkedRequest,
    }),
    checkedResult,
  );

  request.publication.root.objectId = "mutated-object";
  request.mutationRequest.operationId = "operation-mutated";
  result.attachment.rootPath = "/var/lib/portable-codex/mutated";
  assert.equal(checkedRequest.publication.root.objectId, "restore-object-001");
  assert.equal(checkedRequest.mutationRequest.operationId, "operation-attach-001");
  assert.equal(
    checkedResult.attachment.rootPath,
    "/var/lib/portable-codex/restored-session-001",
  );
});

test("restore attachment activation accepts provider-declared aliases without treating path as identity", () => {
  const first = restoreAttachmentActivationRequest();
  const alias = restoreAttachmentActivationRequest({
    publication: restoreAttachmentPublication({
      root: {
        objectId: first.publication.root.objectId,
        rootPath: "/srv/portable-codex/restore-alias-001",
      },
    }),
  });

  assert.notEqual(first.publication.root.rootPath, alias.publication.root.rootPath);
  assert.equal(first.publication.root.objectId, alias.publication.root.objectId);
  assert.doesNotThrow(() =>
    assertRestoreAttachmentActivationResult(
      restoreAttachmentActivationResult(first),
      { request: first },
    ),
  );
  assert.doesNotThrow(() =>
    assertRestoreAttachmentActivationResult(
      restoreAttachmentActivationResult(alias),
      { request: alias },
    ),
  );
  assert.throws(
    () =>
      assertRestoreAttachmentActivationResult(
        restoreAttachmentActivationResult(alias),
        { request: first },
      ),
    assertCode("invalid_restore_attachment_activation"),
  );
});

test("restore attachment activation rejects non-exact and mismatched requests", () => {
  const request = restoreAttachmentActivationRequest();
  const invalidRequests = [
    { ...request, extra: true },
    { ...request, contractVersion: 2 },
    {
      ...request,
      mutationRequest: mutationRequest({ operation: "restore" }),
    },
    {
      ...request,
      mutationRequest: mutationRequest({
        operation: "attach",
        backendId: "other-backend",
      }),
    },
    {
      ...request,
      mutationRequest: mutationRequest({
        operation: "attach",
        leaseId: "lease-002",
      }),
    },
    {
      ...request,
      publication: { ...request.publication, extra: true },
    },
    {
      ...request,
      publication: {
        ...request.publication,
        root: { ...request.publication.root, extra: true },
      },
    },
    {
      ...request,
      publication: {
        ...request.publication,
        artifactManifestDigest: `sha256:${"b".repeat(64)}`,
      },
    },
    {
      ...request,
      publication: {
        ...request.publication,
        publicationKind: "checkpoint-artifact",
      },
    },
    {
      ...request,
      publication: {
        ...request.publication,
        root: { ...request.publication.root, rootPath: "relative/restore" },
      },
    },
    {
      ...request,
      publication: {
        ...request.publication,
        root: { ...request.publication.root, objectId: `a${"b".repeat(256)}` },
      },
    },
  ];

  for (const invalid of invalidRequests) {
    assert.throws(
      () => assertRestoreAttachmentActivationRequest(invalid),
      (error) =>
        error instanceof SessionStorageContractError &&
        [
          "invalid_restore_attachment_activation",
          "invalid_storage_mutation",
          "stale_fence",
        ].includes(error.code),
    );
  }
});

test("restore attachment activation result requires exact provider proof echoes", () => {
  const request = restoreAttachmentActivationRequest();
  const result = restoreAttachmentActivationResult(request);
  const otherPublication = restoreAttachmentPublication({
    root: { objectId: "restore-object-002" },
  });
  const invalidResults = [
    { ...result, extra: true },
    { ...result, contractVersion: 2 },
    { ...result, publication: otherPublication },
    restoreAttachmentActivationResult(request, {
      attachment: { rootPath: "/var/lib/portable-codex/other-root" },
    }),
    restoreAttachmentActivationResult(request, {
      attachment: { operationId: "operation-attach-002" },
    }),
    restoreAttachmentActivationResult(request, {
      attachment: { attachmentId: "attachment-002" },
    }),
    restoreAttachmentActivationResult(request, {
      attachment: { proofId: "proof-attachment-002" },
    }),
    {
      ...result,
      mutationResult: {
        ...result.mutationResult,
        proofId: "proof-attachment-002",
      },
    },
  ];

  for (const invalid of invalidResults) {
    assert.throws(
      () => assertRestoreAttachmentActivationResult(invalid, { request }),
      assertCode("invalid_restore_attachment_activation"),
    );
  }
  assert.throws(
    () =>
      assertRestoreAttachmentActivationResult(
        restoreAttachmentActivationResult(request, {
          mutationResult: { operationId: "operation-attach-002" },
        }),
        { request },
      ),
    assertCode("invalid_storage_mutation"),
  );
});

test("restore attachment reconciliation has three exact read-only outcomes", () => {
  const request = restoreAttachmentActivationRequest();
  const result = restoreAttachmentActivationResult(request);
  const appliedInput = {
    contractVersion: RESTORE_ATTACHMENT_RECONCILIATION_CONTRACT_VERSION,
    outcome: "applied",
    result,
  };
  const absentInput = {
    contractVersion: RESTORE_ATTACHMENT_RECONCILIATION_CONTRACT_VERSION,
    outcome: "absent-and-quiescent",
  };
  const unknownInput = {
    contractVersion: RESTORE_ATTACHMENT_RECONCILIATION_CONTRACT_VERSION,
    outcome: "unknown",
  };

  const applied = assertRestoreAttachmentReconciliationResult(appliedInput, {
    request,
  });
  const absent = assertRestoreAttachmentReconciliationResult(absentInput, {
    request,
  });
  const unknown = assertRestoreAttachmentReconciliationResult(unknownInput, {
    request,
  });

  assert.deepEqual(applied, appliedInput);
  assert.deepEqual(absent, absentInput);
  assert.deepEqual(unknown, unknownInput);
  assert.equal(Object.isFrozen(applied), true);
  assert.equal(Object.isFrozen(applied.result), true);
  assert.equal(Object.isFrozen(applied.result.attachment), true);
  assert.equal(Object.isFrozen(absent), true);
  assert.equal(Object.isFrozen(unknown), true);

  result.attachment.proofId = "mutated-proof";
  result.mutationResult.proofId = "mutated-proof";
  assert.equal(applied.result.attachment.proofId, "proof-attachment-001");
  assert.equal(applied.result.mutationResult.proofId, "proof-attachment-001");

  const invalidAppliedResult = restoreAttachmentActivationResult(request, {
    attachment: { proofId: "crossed-proof" },
  });
  for (const invalid of [
    { ...appliedInput, contractVersion: 2 },
    { ...appliedInput, outcome: "unknown" },
    { ...appliedInput, extra: true },
    {
      contractVersion: RESTORE_ATTACHMENT_RECONCILIATION_CONTRACT_VERSION,
      outcome: "applied",
    },
    { ...absentInput, result: restoreAttachmentActivationResult(request) },
    { ...unknownInput, result: restoreAttachmentActivationResult(request) },
    {
      contractVersion: RESTORE_ATTACHMENT_RECONCILIATION_CONTRACT_VERSION,
      outcome: "applied",
      result: invalidAppliedResult,
    },
    {
      contractVersion: RESTORE_ATTACHMENT_RECONCILIATION_CONTRACT_VERSION,
      outcome: "unsupported",
    },
  ]) {
    assert.throws(
      () =>
        assertRestoreAttachmentReconciliationResult(invalid, { request }),
      assertCode("invalid_restore_attachment_reconciliation"),
    );
  }
});

test("restore attachment reconciliation rejects executable shapes without observation", () => {
  const request = restoreAttachmentActivationRequest();
  let reads = 0;
  const accessor = {
    contractVersion: RESTORE_ATTACHMENT_RECONCILIATION_CONTRACT_VERSION,
    outcome: "unknown",
  };
  Object.defineProperty(accessor, "outcome", {
    enumerable: true,
    get() {
      reads += 1;
      return "unknown";
    },
  });
  assert.throws(
    () =>
      assertRestoreAttachmentReconciliationResult(accessor, { request }),
    assertCode("invalid_restore_attachment_reconciliation"),
  );
  assert.equal(reads, 0);

  const options = { request };
  Object.defineProperty(options, "request", {
    enumerable: true,
    get() {
      reads += 1;
      return request;
    },
  });
  assert.throws(
    () =>
      assertRestoreAttachmentReconciliationResult(
        {
          contractVersion:
            RESTORE_ATTACHMENT_RECONCILIATION_CONTRACT_VERSION,
          outcome: "unknown",
        },
        options,
      ),
    assertCode("invalid_restore_attachment_reconciliation"),
  );
  assert.equal(reads, 0);

  let inheritedReads = 0;
  Object.defineProperty(Object.prototype, "outcome", {
    configurable: true,
    get() {
      inheritedReads += 1;
      return "unknown";
    },
  });
  try {
    assert.throws(
      () =>
        assertRestoreAttachmentReconciliationResult(
          {
            contractVersion:
              RESTORE_ATTACHMENT_RECONCILIATION_CONTRACT_VERSION,
          },
          { request },
        ),
      assertCode("invalid_restore_attachment_reconciliation"),
    );
  } finally {
    delete Object.prototype.outcome;
  }
  assert.equal(inheritedReads, 0);

  let traps = 0;
  const hostile = new Proxy(
    {
      contractVersion: RESTORE_ATTACHMENT_RECONCILIATION_CONTRACT_VERSION,
      outcome: "unknown",
    },
    {
      get() {
        traps += 1;
        throw new Error("proxy trap must not run");
      },
      getPrototypeOf() {
        traps += 1;
        throw new Error("proxy trap must not run");
      },
      ownKeys() {
        traps += 1;
        throw new Error("proxy trap must not run");
      },
    },
  );
  assert.throws(
    () =>
      assertRestoreAttachmentReconciliationResult(hostile, { request }),
    assertCode("invalid_restore_attachment_reconciliation"),
  );
  assert.equal(traps, 0);
});

test("restore attachment activation rejects accessors and proxies before observation", () => {
  let reads = 0;
  const accessorRequest = restoreAttachmentActivationRequest();
  Object.defineProperty(accessorRequest, "publication", {
    enumerable: true,
    get() {
      reads += 1;
      return restoreAttachmentPublication();
    },
  });
  assert.throws(
    () => assertRestoreAttachmentActivationRequest(accessorRequest),
    assertCode("invalid_restore_attachment_activation"),
  );
  assert.equal(reads, 0);

  const nestedAccessorRequest = restoreAttachmentActivationRequest();
  Object.defineProperty(nestedAccessorRequest.publication.root, "objectId", {
    enumerable: true,
    get() {
      reads += 1;
      return "restore-object-001";
    },
  });
  assert.throws(
    () => assertRestoreAttachmentActivationRequest(nestedAccessorRequest),
    assertCode("invalid_restore_attachment_activation"),
  );
  assert.equal(reads, 0);

  const request = restoreAttachmentActivationRequest();
  const result = restoreAttachmentActivationResult(request);
  const options = { request };
  Object.defineProperty(options, "request", {
    enumerable: true,
    get() {
      reads += 1;
      return request;
    },
  });
  assert.throws(
    () => assertRestoreAttachmentActivationResult(result, options),
    assertCode("invalid_restore_attachment_activation"),
  );
  assert.equal(reads, 0);

  let traps = 0;
  const hostile = new Proxy(request, {
    ownKeys() {
      traps += 1;
      throw new Error("proxy trap must not run");
    },
  });
  assert.throws(
    () => assertRestoreAttachmentActivationRequest(hostile),
    assertCode("invalid_restore_attachment_activation"),
  );
  assert.equal(traps, 0);
});

test("restore attachment activation validation resists intrinsic poisoning", () => {
  const request = restoreAttachmentActivationRequest();
  const result = restoreAttachmentActivationResult(request);
  const originalIncludes = Array.prototype.includes;
  const originalEvery = Array.prototype.every;
  const originalKeys = Object.keys;
  const originalStructuredClone = globalThis.structuredClone;
  let poisonedCalls = 0;
  let checkedRequest;
  let checkedResult;
  try {
    Array.prototype.includes = () => {
      poisonedCalls += 1;
      return false;
    };
    Array.prototype.every = () => {
      poisonedCalls += 1;
      return false;
    };
    Object.keys = () => {
      poisonedCalls += 1;
      return [];
    };
    globalThis.structuredClone = () => {
      poisonedCalls += 1;
      throw new Error("poisoned structuredClone");
    };
    checkedRequest = assertRestoreAttachmentActivationRequest(request);
    checkedResult = assertRestoreAttachmentActivationResult(result, { request });
  } finally {
    Array.prototype.includes = originalIncludes;
    Array.prototype.every = originalEvery;
    Object.keys = originalKeys;
    globalThis.structuredClone = originalStructuredClone;
  }

  assert.equal(poisonedCalls, 0);
  assert.deepEqual(checkedRequest, request);
  assert.deepEqual(checkedResult, result);
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

test("storage force-fence envelopes are exact frozen defensive proofs", () => {
  const request = forceFenceRequest();
  const checkedRequest = assertStorageForceFenceRequest(request);
  assert.deepEqual(checkedRequest, request);
  assert(Object.isFrozen(checkedRequest));
  assert(Object.isFrozen(checkedRequest.revokedFence));
  assert(Object.isFrozen(checkedRequest.target));

  request.revokedFence.holderId = "host-mutated";
  request.target.attachmentId = "attachment-mutated";
  assert.equal(checkedRequest.revokedFence.holderId, "host-001");
  assert.equal(checkedRequest.target.attachmentId, "attachment-001");

  const canonicalRequest = forceFenceRequest();
  const result = {
    ...canonicalRequest,
    proofId: "proof-force-fence-001",
    status: "fenced",
  };
  const checkedResult = assertStorageForceFenceResult(result, {
    request: canonicalRequest,
  });
  assert.deepEqual(checkedResult, result);
  assert(Object.isFrozen(checkedResult));
  assert(Object.isFrozen(checkedResult.revokedFence));
  assert(Object.isFrozen(checkedResult.target));

  result.revokedFence.leaseId = "lease-mutated";
  result.target.attachmentId = "attachment-mutated";
  assert.equal(checkedResult.revokedFence.leaseId, "lease-001");
  assert.equal(checkedResult.target.attachmentId, "attachment-001");

  assert.throws(
    () => assertStorageMutationRequest(forceFenceRequest()),
    assertCode("invalid_storage_mutation"),
  );
});

test("storage force-fence requests reject invalid exact fields and non-advancing epochs", () => {
  const request = forceFenceRequest();
  const { target: omittedTarget, ...missingTarget } = request;
  assert.equal(omittedTarget, request.target);
  for (const invalid of [
    { ...request, contractVersion: 2 },
    { ...request, unexpected: true },
    missingTarget,
    { ...request, backendId: "" },
    { ...request, storageId: "volume/001" },
    { ...request, sessionId: "not-a-uuid" },
    { ...request, operationId: "operation force fence" },
    {
      ...request,
      revokedFence: { ...request.revokedFence, holderId: "" },
    },
    {
      ...request,
      revokedFence: { ...request.revokedFence, leaseId: "lease/001" },
    },
    {
      ...request,
      revokedFence: { ...request.revokedFence, unexpected: true },
    },
    {
      ...request,
      target: { ...request.target, attachmentId: "" },
    },
    {
      ...request,
      target: { ...request.target, kind: "checkpoint" },
    },
    {
      ...request,
      target: { ...request.target, unexpected: true },
    },
  ]) {
    assert.throws(
      () => assertStorageForceFenceRequest(invalid),
      assertCode("invalid_storage_force_fence"),
    );
  }

  for (const fencingEpoch of [
    2,
    "",
    "0",
    "01",
    "1e3",
    "18446744073709551616",
  ]) {
    assert.throws(
      () =>
        assertStorageForceFenceRequest({
          ...request,
          fencingEpoch,
        }),
      assertCode("invalid_storage_force_fence"),
    );
    assert.throws(
      () =>
        assertStorageForceFenceRequest({
          ...request,
          revokedFence: {
            ...request.revokedFence,
            fencingEpoch,
          },
        }),
      assertCode("invalid_storage_force_fence"),
    );
  }

  for (const fencingEpoch of [
    request.revokedFence.fencingEpoch,
    "9007199254740992",
  ]) {
    assert.throws(
      () =>
        assertStorageForceFenceRequest({
          ...request,
          fencingEpoch,
        }),
      assertCode("invalid_storage_force_fence"),
    );
  }

  assert.deepEqual(
    assertStorageForceFenceRequest(
      forceFenceRequest({
        fencingEpoch: "18446744073709551615",
        revokedFence: {
          fencingEpoch: "18446744073709551614",
          holderId: "host-001",
          leaseId: "lease-001",
        },
      }),
    ).fencingEpoch,
    "18446744073709551615",
  );
});

test("storage force-fence results must project the complete request", () => {
  const request = forceFenceRequest();
  const result = {
    ...request,
    proofId: "proof-force-fence-001",
    status: "fenced",
  };
  const { status: omittedStatus, ...missingStatus } = result;
  assert.equal(omittedStatus, "fenced");
  for (const invalid of [
    { ...result, proofId: "" },
    { ...result, status: "detached" },
    { ...result, unexpected: true },
    missingStatus,
    { ...result, backendId: "other-backend" },
    { ...result, fencingEpoch: "9007199254740995" },
    { ...result, operationId: "operation-force-fence-002" },
    { ...result, sessionId: OTHER_RUNTIME_SESSION_ID },
    { ...result, storageId: "volume-002" },
    {
      ...result,
      revokedFence: {
        ...result.revokedFence,
        fencingEpoch: "9007199254740992",
      },
    },
    {
      ...result,
      revokedFence: {
        ...result.revokedFence,
        holderId: "host-002",
      },
    },
    {
      ...result,
      revokedFence: {
        ...result.revokedFence,
        leaseId: "lease-002",
      },
    },
    {
      ...result,
      target: {
        ...result.target,
        attachmentId: "attachment-002",
      },
    },
  ]) {
    assert.throws(
      () => assertStorageForceFenceResult(invalid, { request }),
      assertCode("invalid_storage_force_fence"),
    );
  }

  assert.throws(
    () => assertStorageForceFenceResult(result),
    assertCode("invalid_storage_force_fence"),
  );
  assert.throws(
    () =>
      assertStorageForceFenceResult(result, {
        request,
        previousResult: result,
      }),
    assertCode("invalid_storage_force_fence"),
  );
});

test("storage force-fence validators reject proxies and accessors without invoking them", () => {
  let traps = 0;
  const hostile = new Proxy(
    {},
    {
      getPrototypeOf() {
        traps += 1;
        throw new Error("hostile getPrototypeOf trap");
      },
      ownKeys() {
        traps += 1;
        throw new Error("hostile ownKeys trap");
      },
    },
  );
  assert.throws(
    () => assertStorageForceFenceRequest(hostile),
    assertCode("invalid_storage_force_fence"),
  );
  assert.throws(
    () =>
      assertStorageForceFenceRequest(
        forceFenceRequest({ revokedFence: hostile }),
      ),
    assertCode("invalid_storage_force_fence"),
  );
  assert.throws(
    () =>
      assertStorageForceFenceRequest(
        forceFenceRequest({ target: hostile }),
      ),
    assertCode("invalid_storage_force_fence"),
  );
  assert.equal(traps, 0);

  const accessorRequest = forceFenceRequest();
  let reads = 0;
  Object.defineProperty(accessorRequest, "fencingEpoch", {
    enumerable: true,
    get() {
      reads += 1;
      return "9007199254740994";
    },
  });
  assert.throws(
    () => assertStorageForceFenceRequest(accessorRequest),
    assertCode("invalid_storage_force_fence"),
  );

  const nestedAccessorRequest = forceFenceRequest();
  Object.defineProperty(nestedAccessorRequest.revokedFence, "leaseId", {
    enumerable: true,
    get() {
      reads += 1;
      return "lease-001";
    },
  });
  assert.throws(
    () => assertStorageForceFenceRequest(nestedAccessorRequest),
    assertCode("invalid_storage_force_fence"),
  );

  const request = forceFenceRequest();
  const result = {
    ...request,
    proofId: "proof-force-fence-001",
    status: "fenced",
  };
  const options = { request };
  Object.defineProperty(options, "request", {
    enumerable: true,
    get() {
      reads += 1;
      return request;
    },
  });
  assert.throws(
    () => assertStorageForceFenceResult(result, options),
    assertCode("invalid_storage_force_fence"),
  );
  assert.equal(reads, 0);
});

test("storage force-fence validation uses captured intrinsics", () => {
  const request = forceFenceRequest();
  const result = {
    ...request,
    proofId: "proof-force-fence-001",
    status: "fenced",
  };
  const targets = [
    [Array.prototype, "every"],
    [Array.prototype, "includes"],
    [Object, "freeze"],
    [Object, "getOwnPropertyDescriptor"],
    [Object, "getPrototypeOf"],
    [Object, "hasOwn"],
    [Object, "isFrozen"],
    [Object, "keys"],
    [Object, "values"],
    [Reflect, "apply"],
    [Reflect, "ownKeys"],
    [RegExp.prototype, "exec"],
    [RegExp.prototype, "test"],
    [String.prototype, "charCodeAt"],
    [globalThis, "BigInt"],
    [globalThis, "structuredClone"],
    [utilTypes, "isProxy"],
  ].map(([owner, key]) => ({
    descriptor: Object.getOwnPropertyDescriptor(owner, key),
    key,
    owner,
  }));
  let checkedRequest;
  let checkedResult;
  let poisonedCalls = 0;
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
      checkedRequest = assertStorageForceFenceRequest(request);
      checkedResult = assertStorageForceFenceResult(result, { request });
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
  assert.deepEqual(checkedRequest, request);
  assert.deepEqual(checkedResult, result);
  assert(Object.isFrozen(checkedResult));
  assert(Object.isFrozen(checkedResult.revokedFence));
  assert(Object.isFrozen(checkedResult.target));
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
  const attachRequest = mutationRequest({ operation: "attach" });
  const legacyAttachResult = {
    ...attachRequest,
    proofId: "proof-attachment-001",
    status: "attached",
  };
  assert.deepEqual(
    assertStorageMutationResult(legacyAttachResult, {
      request: attachRequest,
    }),
    legacyAttachResult,
  );
  assert.throws(
    () =>
      assertStorageMutationResult(
        {
          ...legacyAttachResult,
          rootPath: "/var/lib/portable-codex/session-001",
        },
        { request: attachRequest },
      ),
    assertCode("invalid_storage_mutation"),
  );
  assert.throws(
    () =>
      assertStorageMutationResult(
        {
          ...result,
          rootPath: "/var/lib/portable-codex/session-001",
        },
        { request },
      ),
    assertCode("invalid_storage_mutation"),
  );
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

test("restore checkpoint admission binds the complete source descriptor to the restore request", () => {
  const value = restoreAdmission();
  const admitted = assertRestoreCheckpointAdmission(value);

  assert.deepEqual(admitted, value);
  assert.equal(admitted.checkpoint.storageId, "volume-001");
  assert.equal(admitted.request.storageId, "destination-volume-001");
  assert.equal(Object.isFrozen(admitted), true);
  assert.equal(Object.isFrozen(admitted.checkpoint), true);
  assert.equal(Object.isFrozen(admitted.request), true);

  for (const candidate of [
    restoreAdmission({ checkpointOverrides: { checkpointClass: "crash-prefix" } }),
    restoreAdmission({ checkpointOverrides: { backendId: "other-backend" } }),
    restoreAdmission({ checkpointOverrides: { sessionId: OTHER_RUNTIME_SESSION_ID } }),
    restoreAdmission({ checkpointOverrides: { artifactId: "other-artifact" } }),
    restoreAdmission({ checkpointOverrides: { checkpointId: "other-checkpoint" } }),
    restoreAdmission({ requestOverrides: { operation: "checkpoint" } }),
    restoreAdmission({ requestOverrides: { fencingEpoch: "9007199254740993" } }),
    restoreAdmission({ requestOverrides: { fencingEpoch: "9007199254740992" } }),
  ]) {
    assert.throws(
      () => assertRestoreCheckpointAdmission(candidate),
      assertCode("invalid_restore_checkpoint_admission"),
    );
  }
});

test("restore checkpoint admission rejects hostile and inexact data shapes", () => {
  assert.throws(
    () => assertRestoreCheckpointAdmission({ ...restoreAdmission(), extra: true }),
    assertCode("invalid_restore_checkpoint_admission"),
  );
  assert.throws(
    () => assertRestoreCheckpointAdmission(new Proxy(restoreAdmission(), {})),
    assertCode("invalid_restore_checkpoint_admission"),
  );

  const value = restoreAdmission();
  let getterCalls = 0;
  Object.defineProperty(value, "request", {
    enumerable: true,
    get() {
      getterCalls += 1;
      throw new Error("restore admission getter must not run");
    },
  });
  assert.throws(
    () => assertRestoreCheckpointAdmission(value),
    assertCode("invalid_restore_checkpoint_admission"),
  );
  assert.equal(getterCalls, 0);
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
