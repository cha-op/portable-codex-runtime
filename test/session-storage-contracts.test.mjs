import assert from "node:assert/strict";
import test from "node:test";

import {
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
  assertCheckpointClass,
  assertCheckpointDescriptor,
  assertLeaseGrant,
  assertLeaseRenewal,
  assertResolvedPlatformImageMatchesManifest,
  assertSessionAttachment,
  assertSessionManifest,
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

function mutationRequest(overrides = {}) {
  return {
    contractVersion: 1,
    backendId: "single-attach-test",
    storageId: "volume-001",
    sessionId: RUNTIME_SESSION_ID,
    leaseId: "lease-001",
    holderId: "host-001",
    fencingEpoch: "9007199254740993",
    operation: "checkpoint",
    operationId: "operation-checkpoint-001",
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
    { ...manifest, layoutVersion: 2 },
    { ...manifest, agents: { ...manifest.agents, defaultMaxSubagents: 11 } },
    { ...manifest, agents: { ...manifest.agents, maxSubagents: 11 } },
    { ...manifest, agents: { ...manifest.agents, maxDepth: 3 } },
  ]) {
    assert.throws(() => assertSessionManifest(invalid), assertCode("invalid_session_manifest"));
  }
});

test("trusted OCI resolution must match the recorded platform manifest", () => {
  const resolution = {
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
  for (const invalid of [
    { ...resolution, digest: `sha256:${"b".repeat(64)}` },
    { ...resolution, mediaType: "application/vnd.oci.image.index.v1+json" },
    { ...resolution, platform: "linux/amd64" },
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
    { ...attachment(), rawDevice: "/dev/disk9" },
  ]) {
    assert.throws(() => assertSessionAttachment(invalid), assertCode("invalid_storage_attachment"));
  }
});

test("rootless worker template is structural and fixed-layout", () => {
  const currentLease = lease();
  const template = createRootlessWorkerTemplate({
    attachment: attachment(),
    lease: currentLease,
    manifest: sessionManifest(),
    storageRef: storageRef(),
  });
  assert.deepEqual(template.mount, {
    propagation: "private",
    readOnly: false,
    source: attachment().rootPath,
    target: SESSION_WORKER_ROOT,
    type: "bind",
  });
  assert.deepEqual(template.env, {
    CODEX_HOME: SESSION_WORKER_LAYOUT.codexHome,
    CODEX_SQLITE_HOME: SESSION_WORKER_LAYOUT.codexHome,
  });
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

test("storage mutation envelopes bind operation IDs to the complete writer fence", () => {
  const request = mutationRequest();
  assert.deepEqual(assertStorageMutationRequest(request), request);
  assert.deepEqual(
    assertStorageMutationMatchesLeaseSnapshot({
      canonicalLease: lease(),
      now: Date.parse("2026-07-02T12:00:00.000Z"),
      request,
      storageRef: storageRef(),
    }),
    request,
  );
  const result = {
    ...request,
    proofId: "proof-checkpoint-001",
    status: "checkpoint-created",
  };
  assert.deepEqual(assertStorageMutationResult(result, { request }), result);
  assert.throws(
    () =>
      assertStorageMutationResult({ ...result, status: "detached" }, { request }),
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
    explicitAbortMarker: "must-not-infer",
    requiresStorageBarrier: true,
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
  for (const invalid of [
    checkpoint({ codexThreadId: "019f2100-0000-7000-8000-000000000099" }),
    checkpoint({ imageDigest: `sha256:${"b".repeat(64)}` }),
    checkpoint({ storageId: "volume-002" }),
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
});
