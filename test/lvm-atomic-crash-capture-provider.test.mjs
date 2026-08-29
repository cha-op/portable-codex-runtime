import assert from "node:assert/strict";
import { Readable } from "node:stream";
import test from "node:test";

import {
  LVM_ATOMIC_CRASH_CAPTURE_BINDING_KIND,
  LVM_ATOMIC_CRASH_CAPTURE_DRIVER_CONTRACT_VERSION,
  LVM_ATOMIC_CRASH_CAPTURE_PROVIDER_CONTRACT_VERSION,
  LvmAtomicCrashCaptureProviderError,
  assertLvmAtomicCrashCaptureProviderBinding,
  createLvmAtomicCrashCaptureDriver,
  createLvmAtomicCrashCaptureProvider,
} from "../src/lvm-atomic-crash-capture-provider.mjs";
import {
  ATOMIC_CRASH_CAPTURE_CONTRACT_VERSION,
  assertAtomicCrashCaptureBackend,
} from "../src/session-storage-contracts.mjs";

const BACKEND_ID = "lvm-atomic-test";
const SESSION_ID = "019f2100-0000-7000-8000-000000000001";
const THREAD_ID = "019f2100-0000-7000-8000-000000000002";
const CONTENT_SHA256 = "b".repeat(64);
const ORIGIN_LV_UUID = "ORIGIN-1234567890";
const SNAPSHOT_LV_UUID = "SNAPSHOT-1234567890";

function exact(values) {
  return Object.freeze(Object.assign(Object.create(null), values));
}

function storageRef(overrides = {}) {
  return {
    backendId: BACKEND_ID,
    contractVersion: 1,
    sessionId: SESSION_ID,
    storageId: "storage-001",
    ...overrides,
  };
}

function sourceAttachment(overrides = {}) {
  return {
    attachmentId: "attachment-001",
    backendId: BACKEND_ID,
    contractVersion: 1,
    fencingEpoch: "11",
    holderId: "host-001",
    kind: "directory",
    leaseId: "lease-001",
    mode: "read-write",
    operationId: "attach-operation-001",
    proofId: "attach-proof-001",
    rootPath: "/private/source/must-not-be-opened",
    sessionId: SESSION_ID,
    storageId: "storage-001",
    ...overrides,
  };
}

function checkpoint(overrides = {}) {
  return {
    artifactId: "artifact-001",
    backendId: BACKEND_ID,
    checkpointClass: "crash-prefix",
    checkpointId: "checkpoint-001",
    codexSessionId: THREAD_ID,
    codexThreadId: THREAD_ID,
    contractVersion: 1,
    createdAt: "2026-08-29T00:00:00.000Z",
    imageDigest: `sha256:${"a".repeat(64)}`,
    sessionId: SESSION_ID,
    sourceFencingEpoch: "11",
    storageId: "storage-001",
    ...overrides,
  };
}

function mutationRequest(overrides = {}) {
  return {
    backendId: BACKEND_ID,
    contractVersion: 1,
    fencingEpoch: "11",
    holderId: "host-001",
    leaseId: "lease-001",
    operation: "checkpoint",
    operationId: "checkpoint-operation-001",
    sessionId: SESSION_ID,
    storageId: "storage-001",
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

function providerBinding(overrides = {}) {
  return {
    bindingKind: LVM_ATOMIC_CRASH_CAPTURE_BINDING_KIND,
    contractVersion: LVM_ATOMIC_CRASH_CAPTURE_PROVIDER_CONTRACT_VERSION,
    originLvUuid: ORIGIN_LV_UUID,
    snapshotName: "pcr-snapshot-001",
    snapshotSizeBytes: "4",
    snapshotTag: "pcr.atomic.snapshot-001",
    ...overrides,
  };
}

function committedResult(request, overrides = {}) {
  const { artifact: artifactOverrides = {}, ...outerOverrides } = overrides;
  return {
    artifact: {
      byteLength: "8",
      contentSha256: CONTENT_SHA256,
      objectId: SNAPSHOT_LV_UUID,
      objectIdentityScheme: "lvm-lv-uuid-v1",
      readOnly: true,
      ...artifactOverrides,
    },
    artifactId: request.checkpoint.artifactId,
    backendId: request.storageRef.backendId,
    captureAttemptId: request.captureAttemptId,
    checkpointId: request.checkpoint.checkpointId,
    contractVersion: ATOMIC_CRASH_CAPTURE_CONTRACT_VERSION,
    operationId: request.mutationRequest.operationId,
    proofId: "lvm-proof-001",
    sessionId: request.storageRef.sessionId,
    sourceFencingEpoch: request.checkpoint.sourceFencingEpoch,
    status: "committed",
    storageId: request.storageRef.storageId,
    ...outerOverrides,
  };
}

function createBaseBackend() {
  const calls = [];
  const backend = {
    backendId: BACKEND_ID,
    capabilities: {
      atomicPointInTimeCheckpoint: false,
      exclusiveWriterAttachment: true,
      fencing: "manual",
      normalDirectoryAttachment: true,
    },
    contractVersion: 1,
  };
  for (const name of [
    "captureCheckpoint",
    "destroySession",
    "detachAttachment",
    "forceFence",
    "prepareWritableAttachment",
    "provisionSession",
    "restoreCheckpoint",
  ]) {
    backend[name] = function delegatedBaseMethod(...args) {
      calls.push({ args, name, receiver: this });
      return `${name}-result`;
    };
  }
  return { backend, calls };
}

function createCatalogue(options = {}) {
  const calls = {
    claim: [],
    commit: [],
    mark: [],
    read: [],
  };
  let state = options.initialState ?? "empty";
  let storedBinding = options.providerBinding ?? null;
  let storedResult = options.result ?? null;
  const dispatchClaim = exact({ claim: "opaque-claim" });
  const catalogue = exact({
    async claimStarting(input) {
      calls.claim.push(input);
      options.events?.push("claim");
      if (options.claimResult) return options.claimResult(input);
      if (state === "empty") {
        state = "starting";
        storedBinding = input.providerBinding;
        return exact({ dispatchClaim, outcome: "dispatch" });
      }
      if (state === "committed") {
        return exact({
          outcome: "committed",
          providerBinding: storedBinding,
          result: storedResult,
        });
      }
      return exact({ outcome: "unknown" });
    },
    async commitResult(input) {
      calls.commit.push(input);
      options.events?.push("commit");
      assert.strictEqual(input.dispatchClaim, dispatchClaim);
      state = "committed";
      storedResult = input.result;
      if (options.commitAckLoss) throw new Error("commit ack lost");
      return exact({
        outcome: "committed",
        providerBinding: storedBinding,
        result: storedResult,
      });
    },
    contractVersion: LVM_ATOMIC_CRASH_CAPTURE_PROVIDER_CONTRACT_VERSION,
    async markUncertain(input) {
      calls.mark.push(input);
      options.events?.push("mark");
      assert.strictEqual(input.dispatchClaim, dispatchClaim);
      state = "uncertain";
      return exact({ outcome: "uncertain" });
    },
    async readCommitted(input) {
      calls.read.push(input);
      options.events?.push("read");
      if (state !== "committed") return exact({ outcome: "unknown" });
      return exact({
        outcome: "committed",
        providerBinding: storedBinding,
        result: storedResult,
      });
    },
  });
  return {
    calls,
    catalogue,
    get state() {
      return state;
    },
  };
}

function createFakeDriver(options = {}) {
  const calls = { capture: [], resolve: [], verify: [] };
  const binding = assertLvmAtomicCrashCaptureProviderBinding(
    options.providerBinding ?? providerBinding(),
  );
  const driver = exact({
    async captureSnapshot(input) {
      calls.capture.push(input);
      options.events?.push("driver-capture");
      if (options.capture) return options.capture(input);
      return committedResult(input.request);
    },
    contractVersion: LVM_ATOMIC_CRASH_CAPTURE_DRIVER_CONTRACT_VERSION,
    async resolveProviderBinding(input) {
      calls.resolve.push(input);
      options.events?.push("resolve");
      if (options.resolve) return options.resolve(input);
      return binding;
    },
    async verifySnapshot(input) {
      calls.verify.push(input);
      options.events?.push("verify");
      if (options.verify) return options.verify(input);
      return true;
    },
  });
  return { binding, calls, driver };
}

function createAuthorityConsumer(options = {}) {
  const calls = [];
  const authorityConsumer = options.consumer ?? (async (admission, run) => {
    calls.push({ admission, run });
    options.events?.push("authority");
    return run();
  });
  return { authorityConsumer, calls };
}

function createFixture(options = {}) {
  const base = createBaseBackend();
  const catalogue = options.catalogue ?? createCatalogue(options.catalogueOptions);
  const driver = options.driver ?? createFakeDriver(options.driverOptions);
  const authority = options.authority ?? createAuthorityConsumer(options.authorityOptions);
  const provider = createLvmAtomicCrashCaptureProvider({
    authorityConsumer: authority.authorityConsumer,
    baseBackend: base.backend,
    catalogue: catalogue.catalogue,
    driver: driver.driver,
  });
  return { authority, base, catalogue, driver, provider };
}

function captureInput(request = captureRequest(), captureAuthority = exact({})) {
  return exact({ captureAuthority, request });
}

function assertUncertain(error) {
  return (
    error instanceof LvmAtomicCrashCaptureProviderError &&
    error.code === "lvm_atomic_crash_capture_outcome_uncertain" &&
    error.retryable === false &&
    Object.isFrozen(error)
  );
}

test("provider delegates lifecycle and publishes a private atomic capability tuple", () => {
  const fixture = createFixture();

  assert.strictEqual(assertAtomicCrashCaptureBackend(fixture.provider), fixture.provider);
  assert.equal(fixture.provider.backendId, BACKEND_ID);
  assert.equal(fixture.provider.contractVersion, 1);
  assert.equal(fixture.provider.atomicCrashCaptureContractVersion, 1);
  assert.equal(fixture.provider.capabilities.atomicPointInTimeCheckpoint, true);
  assert.equal(fixture.provider.capabilities.exclusiveWriterAttachment, true);
  assert.equal(fixture.provider.capabilities.fencing, "manual");
  assert.equal(fixture.provider.capabilities.normalDirectoryAttachment, true);
  assert.notStrictEqual(
    fixture.provider.capabilities,
    fixture.base.backend.capabilities,
  );
  assert(Object.isFrozen(fixture.provider));
  assert(Object.isFrozen(fixture.provider.capabilities));

  for (const name of [
    "captureCheckpoint",
    "destroySession",
    "detachAttachment",
    "forceFence",
    "prepareWritableAttachment",
    "provisionSession",
    "restoreCheckpoint",
  ]) {
    assert.equal(fixture.provider[name](name), `${name}-result`);
  }
  assert.equal(fixture.base.calls.length, 7);
  for (const call of fixture.base.calls) {
    assert.strictEqual(call.receiver, fixture.base.backend);
  }
  assert.throws(
    () => fixture.provider.captureCheckpoint.call({}, "wrong receiver"),
    /Invalid LVM atomic crash-capture provider receiver/u,
  );
});

test("fresh capture claims durably before consuming authority and commits once", async () => {
  const events = [];
  const fixture = createFixture({
    authorityOptions: { events },
    catalogueOptions: { events },
    driverOptions: { events },
  });
  let authorityReads = 0;
  const captureAuthority = {};
  Object.defineProperty(captureAuthority, "secret", {
    get() {
      authorityReads += 1;
      throw new Error("must remain opaque");
    },
  });
  Object.freeze(captureAuthority);
  const request = captureRequest();

  const result = await fixture.provider.captureAtomicCrashCheckpoint(
    captureInput(request, captureAuthority),
  );

  assert.deepEqual(events, [
    "resolve",
    "claim",
    "authority",
    "driver-capture",
    "commit",
  ]);
  assert.equal(authorityReads, 0);
  assert.equal(fixture.catalogue.calls.claim.length, 1);
  assert.equal(fixture.authority.calls.length, 1);
  assert.strictEqual(
    fixture.authority.calls[0].admission.captureAuthority,
    captureAuthority,
  );
  assert.deepEqual(result, committedResult(request));
  assert.equal(fixture.catalogue.state, "committed");
  assert.equal(fixture.catalogue.calls.mark.length, 0);
});

test("concurrent and existing starting or uncertain claims never redispatch", async () => {
  const fixture = createFixture();
  const input = captureInput();
  const first = fixture.provider.captureAtomicCrashCheckpoint(input);
  const second = fixture.provider.captureAtomicCrashCheckpoint(input);
  const settled = await Promise.allSettled([first, second]);

  assert.equal(settled.filter(({ status }) => status === "fulfilled").length, 1);
  assert.equal(settled.filter(({ status }) => status === "rejected").length, 1);
  assert.equal(fixture.driver.calls.capture.length, 1);
  assert.equal(fixture.authority.calls.length, 1);

  for (const initialState of ["starting", "uncertain"]) {
    const blocked = createFixture({
      catalogue: createCatalogue({ initialState }),
    });
    await assert.rejects(
      blocked.provider.captureAtomicCrashCheckpoint(captureInput()),
      assertUncertain,
    );
    assert.equal(blocked.driver.calls.capture.length, 0);
    assert.equal(blocked.authority.calls.length, 0);
  }
});

test("authority cardinality and awaited identity failures mark dispatch uncertain", async (t) => {
  const cases = [
    {
      consumer: async () => null,
      expectedCaptureCalls: 0,
      name: "no callback",
    },
    {
      consumer: async (_admission, run) => {
        await run();
        return exact({ substituted: true });
      },
      expectedCaptureCalls: 1,
      name: "substituted completion",
    },
    {
      consumer: async (_admission, run) => {
        await run();
        return run();
      },
      expectedCaptureCalls: 1,
      name: "second callback",
    },
  ];
  for (const entry of cases) {
    await t.test(entry.name, async () => {
      const authority = createAuthorityConsumer({ consumer: entry.consumer });
      const fixture = createFixture({ authority });
      await assert.rejects(
        fixture.provider.captureAtomicCrashCheckpoint(captureInput()),
        assertUncertain,
      );
      assert.equal(fixture.driver.calls.capture.length, entry.expectedCaptureCalls);
      assert.equal(fixture.catalogue.calls.mark.length, 1);
      assert.equal(fixture.catalogue.state, "uncertain");
    });
  }
});

test("driver rejection and malformed result become durable uncertainty", async (t) => {
  for (const [name, capture] of [
    ["driver rejection", async () => { throw new Error("private driver failure"); }],
    ["malformed result", async () => exact({ status: "committed" })],
  ]) {
    await t.test(name, async () => {
      const fixture = createFixture({
        driver: createFakeDriver({ capture }),
      });
      await assert.rejects(
        fixture.provider.captureAtomicCrashCheckpoint(captureInput()),
        assertUncertain,
      );
      assert.equal(fixture.catalogue.calls.mark.length, 1);
      assert.equal(fixture.catalogue.state, "uncertain");
    });
  }
});

test("ambiguous commit acknowledgement is not rewritten and committed replay is reverified", async () => {
  const catalogue = createCatalogue({ commitAckLoss: true });
  const fixture = createFixture({ catalogue });
  const input = captureInput();

  await assert.rejects(
    fixture.provider.captureAtomicCrashCheckpoint(input),
    assertUncertain,
  );
  assert.equal(catalogue.state, "committed");
  assert.equal(catalogue.calls.mark.length, 0);
  assert.equal(fixture.driver.calls.capture.length, 1);

  const replay = await fixture.provider.captureAtomicCrashCheckpoint(input);
  assert.deepEqual(replay, committedResult(input.request));
  assert.equal(fixture.driver.calls.capture.length, 1);
  assert.equal(fixture.driver.calls.verify.length, 1);
  assert.equal(fixture.authority.calls.length, 1);
});

test("source-free verification reads only catalogue and retained snapshot evidence", async () => {
  const request = captureRequest({
    sourceAttachment: sourceAttachment({
      rootPath: "/source/has/disappeared-after-restart",
    }),
  });
  const result = committedResult(request);
  const catalogue = createCatalogue({
    initialState: "committed",
    providerBinding: providerBinding(),
    result,
  });
  const driver = createFakeDriver({
    resolve() {
      throw new Error("verification must not resolve source binding");
    },
    verify(input) {
      assert.deepEqual(Object.keys(input).sort(), ["providerBinding", "result"]);
      assert.equal(Object.hasOwn(input, "request"), false);
      assert.equal(
        JSON.stringify(input).includes(request.sourceAttachment.rootPath),
        false,
      );
      return true;
    },
  });
  const fixture = createFixture({ catalogue, driver });

  const verification =
    await fixture.provider.verifyCommittedAtomicCrashCheckpoint(request);

  assert.equal(verification.outcome, "committed");
  assert.deepEqual(verification.result, result);
  assert.equal(driver.calls.resolve.length, 0);
  assert.equal(driver.calls.verify.length, 1);
  assert.equal(fixture.authority.calls.length, 0);
});

test("every unreadable or mismatched retained snapshot observation stays unknown", async (t) => {
  for (const reason of [
    "uuid",
    "origin",
    "digest",
    "size",
    "read-only",
    "cow-full",
    "unreadable",
  ]) {
    await t.test(reason, async () => {
      const request = captureRequest();
      const catalogue = createCatalogue({
        initialState: "committed",
        providerBinding: providerBinding(),
        result: committedResult(request),
      });
      const driver = createFakeDriver({
        async verify() {
          if (reason === "unreadable") throw new Error("unreadable snapshot");
          return false;
        },
      });
      const fixture = createFixture({ catalogue, driver });
      const verification =
        await fixture.provider.verifyCommittedAtomicCrashCheckpoint(request);
      assert.equal(verification.contractVersion, 1);
      assert.equal(verification.outcome, "unknown");
      assert.equal(verification.result, null);
      assert.equal(fixture.authority.calls.length, 0);
    });
  }
});

test("factory rejects proxy and accessor surfaces without executing traps", () => {
  let traps = 0;
  const proxy = new Proxy({}, {
    get() {
      traps += 1;
      throw new Error("secret proxy trap");
    },
    getOwnPropertyDescriptor() {
      traps += 1;
      throw new Error("secret proxy trap");
    },
    getPrototypeOf() {
      traps += 1;
      throw new Error("secret proxy trap");
    },
    ownKeys() {
      traps += 1;
      throw new Error("secret proxy trap");
    },
  });
  assert.throws(
    () => createLvmAtomicCrashCaptureProvider(proxy),
    LvmAtomicCrashCaptureProviderError,
  );
  assert.equal(traps, 0);

  const normal = createFixture();
  let accessorReads = 0;
  const hostileCatalogue = {
    claimStarting: normal.catalogue.catalogue.claimStarting,
    commitResult: normal.catalogue.catalogue.commitResult,
    contractVersion: 1,
    markUncertain: normal.catalogue.catalogue.markUncertain,
    readCommitted: normal.catalogue.catalogue.readCommitted,
  };
  Object.defineProperty(hostileCatalogue, "claimStarting", {
    enumerable: true,
    get() {
      accessorReads += 1;
      throw new Error("secret accessor");
    },
  });
  assert.throws(
    () =>
      createLvmAtomicCrashCaptureProvider({
        authorityConsumer: normal.authority.authorityConsumer,
        baseBackend: normal.base.backend,
        catalogue: hostileCatalogue,
        driver: normal.driver.driver,
      }),
    LvmAtomicCrashCaptureProviderError,
  );
  assert.equal(accessorReads, 0);
});

test("unsafe thenables are rejected without reading or invoking then", async (t) => {
  const hostileThenable = () => {
    let reads = 0;
    const value = {};
    Object.defineProperty(value, "then", {
      get() {
        reads += 1;
        throw new Error("hostile then getter");
      },
    });
    return { get reads() { return reads; }, value };
  };

  await t.test("catalogue", async () => {
    const hostile = hostileThenable();
    const catalogue = createCatalogue({
      claimResult() {
        return hostile.value;
      },
    });
    // Replace the async test catalogue method so it does not assimilate the
    // thenable before the provider can reject it.
    const surface = exact({
      claimStarting() {
        return hostile.value;
      },
      commitResult: catalogue.catalogue.commitResult,
      contractVersion: 1,
      markUncertain: catalogue.catalogue.markUncertain,
      readCommitted: catalogue.catalogue.readCommitted,
    });
    const fixture = createFixture({ catalogue: { ...catalogue, catalogue: surface } });
    await assert.rejects(
      fixture.provider.captureAtomicCrashCheckpoint(captureInput()),
      assertUncertain,
    );
    assert.equal(hostile.reads, 0);
  });

  await t.test("authority", async () => {
    const hostile = hostileThenable();
    const authority = createAuthorityConsumer({
      consumer() {
        return hostile.value;
      },
    });
    const fixture = createFixture({ authority });
    await assert.rejects(
      fixture.provider.captureAtomicCrashCheckpoint(captureInput()),
      assertUncertain,
    );
    assert.equal(hostile.reads, 0);
    assert.equal(fixture.catalogue.calls.mark.length, 1);
  });

  await t.test("driver", async () => {
    const hostile = hostileThenable();
    const normal = createFakeDriver();
    const driver = exact({
      captureSnapshot() {
        return hostile.value;
      },
      contractVersion: 1,
      resolveProviderBinding: normal.driver.resolveProviderBinding,
      verifySnapshot: normal.driver.verifySnapshot,
    });
    const fixture = createFixture({
      driver: { ...normal, driver },
    });
    await assert.rejects(
      fixture.provider.captureAtomicCrashCheckpoint(captureInput()),
      assertUncertain,
    );
    assert.equal(hostile.reads, 0);
    assert.equal(fixture.catalogue.calls.mark.length, 1);
  });
});

function lvsJson(rows) {
  return JSON.stringify({
    report: [{
      lv: rows.map((row) => Object.fromEntries(
        Object.entries(row).map(([key, value]) => [key, `  ${value}  `]),
      )),
    }],
  });
}

function createCommandDriverFixture() {
  const state = {
    calls: [],
    mode: "capture",
    snapshotCreated: false,
    snapshotName: null,
    snapshotTag: null,
    snapshotObservations: 0,
  };
  const originRow = () => ({
    lv_attr: "-wi-a-----",
    lv_dm_path: "/dev/mapper/pcrvg-origin",
    lv_name: "origin",
    lv_path: "/dev/pcrvg/origin",
    lv_size: "8",
    lv_tags: "",
    lv_uuid: ORIGIN_LV_UUID,
    origin_uuid: "",
    snap_percent: "",
  });
  const snapshotRow = () => {
    state.snapshotObservations += 1;
    const mode = state.mode;
    const benign = mode === "benign";
    return {
      lv_attr:
        mode === "read-only" ? "swi-a-s---" :
          benign && state.snapshotObservations % 2 === 0 ? "sri-a-s--p" :
            "sri-a-s---",
      lv_dm_path: "/dev/mapper/pcrvg-snapshot",
      lv_name: state.snapshotName,
      lv_path: "/dev/pcrvg/snapshot",
      lv_size: mode === "cow-allocation" ? "5" : "4",
      lv_tags: `unrelated,${state.snapshotTag},metadata-churn`,
      lv_uuid: mode === "uuid" ? "SNAPSHOT-9999999999" : SNAPSHOT_LV_UUID,
      origin_uuid: mode === "origin" ? "ORIGIN-9999999999" : ORIGIN_LV_UUID,
      snap_percent:
        mode === "cow-full" ? "100.00" :
          benign && state.snapshotObservations % 2 === 0 ? "9.75" : "1.25",
    };
  };
  const commandRunner = async (executable, args, options) => {
    state.calls.push({ args, executable, options });
    assert.equal(Object.isFrozen(args), true);
    assert.equal(options.maxBuffer, 1024 * 1024);
    if (executable === "/fixed/lvs") {
      const selector = args.at(-1);
      if (selector === `lv_uuid=${ORIGIN_LV_UUID}`) {
        return { stderr: "", stdout: lvsJson([originRow()]) };
      }
      assert.equal(state.snapshotCreated, true);
      return { stderr: "", stdout: lvsJson([snapshotRow()]) };
    }
    if (executable === "/fixed/lvcreate") {
      assert.equal(state.snapshotCreated, false);
      state.snapshotName = args[args.indexOf("--name") + 1];
      state.snapshotTag = args[args.indexOf("--addtag") + 1];
      state.snapshotCreated = true;
      return { stderr: "", stdout: "created\n" };
    }
    if (executable === "/fixed/dmsetup") {
      const uuid =
        state.mode === "dm-uuid"
          ? "LVM-WRONG"
          : "LVM-VG0000SNAPSHOT1234567890";
      return { stderr: "", stdout: `  ${uuid}  :  253  :  7  \n` };
    }
    if (executable === "/fixed/blockdev") {
      if (args[0] === "--getro") {
        return { stderr: "", stdout: state.mode === "block-read-only" ? "0\n" : "1\n" };
      }
      return {
        stderr: "",
        stdout: state.mode === "visible-size" ? "9\n" : "8\n",
      };
    }
    throw new Error(`unexpected executable: ${executable}`);
  };
  const driver = createLvmAtomicCrashCaptureDriver({
    blockdevExecutable: "/fixed/blockdev",
    commandRunner,
    createSnapshotReadStream() {
      const fill = state.mode === "digest" ? 2 : 1;
      return Readable.from([
        Buffer.alloc(state.mode === "visible-size" ? 9 : 8, fill),
      ]);
    },
    dmsetupExecutable: "/fixed/dmsetup",
    lvcreateExecutable: "/fixed/lvcreate",
    lvsExecutable: "/fixed/lvs",
    resolveOrigin() {
      return exact({
        originLvUuid: ORIGIN_LV_UUID,
        snapshotSizeBytes: "4",
      });
    },
  });
  return { driver, state };
}

test("fixed-command driver captures a stable read-only classic snapshot", async () => {
  const fixture = createCommandDriverFixture();
  const request = captureRequest();
  const binding = await fixture.driver.resolveProviderBinding(exact({ request }));
  const result = await fixture.driver.captureSnapshot(
    exact({ providerBinding: binding, request }),
  );

  assert.equal(binding.snapshotSizeBytes, "4");
  assert.equal(result.artifact.byteLength, "8");
  assert.equal(result.artifact.objectId, SNAPSHOT_LV_UUID);
  assert.equal(result.artifact.objectIdentityScheme, "lvm-lv-uuid-v1");
  assert.equal(result.artifact.readOnly, true);
  assert.equal(result.artifact.contentSha256.length, 64);
  assert.equal(
    await fixture.driver.verifySnapshot(exact({ providerBinding: binding, result })),
    true,
  );

  const lvcreate = fixture.state.calls.find(
    ({ executable }) => executable === "/fixed/lvcreate",
  );
  assert.deepEqual(lvcreate.args, [
    "--snapshot",
    "--name",
    binding.snapshotName,
    "--size",
    "4B",
    "--addtag",
    binding.snapshotTag,
    "--permission",
    "r",
    "/dev/pcrvg/origin",
  ]);
  assert.equal(
    fixture.state.calls.some(({ executable }) => !executable.startsWith("/")),
    false,
  );
});

test("driver verification separates persistent identity, content, and access policy", async (t) => {
  for (const mode of [
    "uuid",
    "origin",
    "digest",
    "visible-size",
    "cow-allocation",
    "read-only",
    "block-read-only",
    "cow-full",
    "dm-uuid",
  ]) {
    await t.test(mode, async () => {
      const fixture = createCommandDriverFixture();
      const request = captureRequest();
      const binding = await fixture.driver.resolveProviderBinding(exact({ request }));
      const result = await fixture.driver.captureSnapshot(
        exact({ providerBinding: binding, request }),
      );
      fixture.state.mode = mode;
      fixture.state.snapshotObservations = 0;
      assert.equal(
        await fixture.driver.verifySnapshot(
          exact({ providerBinding: binding, result }),
        ),
        false,
      );
    });
  }
});

test("benign COW percentage and unrelated LV metadata churn remain verifiable", async () => {
  const fixture = createCommandDriverFixture();
  const request = captureRequest();
  const binding = await fixture.driver.resolveProviderBinding(exact({ request }));
  const result = await fixture.driver.captureSnapshot(
    exact({ providerBinding: binding, request }),
  );
  fixture.state.mode = "benign";
  fixture.state.snapshotObservations = 0;

  assert.equal(
    await fixture.driver.verifySnapshot(exact({ providerBinding: binding, result })),
    true,
  );
});

test("binding and driver constructors reject noncanonical or executable shapes", () => {
  for (const value of [
    providerBinding({ snapshotSizeBytes: "0" }),
    providerBinding({ snapshotSizeBytes: "04" }),
    providerBinding({ snapshotName: "bad/name" }),
    providerBinding({ snapshotTag: "bad/tag" }),
    providerBinding({ originLvUuid: "x" }),
  ]) {
    assert.throws(
      () => assertLvmAtomicCrashCaptureProviderBinding(value),
      LvmAtomicCrashCaptureProviderError,
    );
  }
  assert.throws(
    () =>
      createLvmAtomicCrashCaptureDriver({
        blockdevExecutable: "blockdev",
        commandRunner: async () => ({ stderr: "", stdout: "" }),
        resolveOrigin: () => exact({
          originLvUuid: ORIGIN_LV_UUID,
          snapshotSizeBytes: "4",
        }),
      }),
    LvmAtomicCrashCaptureProviderError,
  );
});
