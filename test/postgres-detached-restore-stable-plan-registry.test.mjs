import assert from "node:assert/strict";
import test from "node:test";

import {
  createPostgresDetachedRestorePlan,
  isPostgresDetachedRestorePlan,
} from "../src/postgres-detached-restore-plan.mjs";
import {
  createPostgresDetachedRestoreOperationalLeaseBudget,
} from "../src/postgres-detached-restore-operational-lease-budget.mjs";
import {
  PostgresSerializableStore,
  PostgresSerializableStoreError,
} from "../src/postgres-serializable-store.mjs";
import {
  SESSION_AUTHORITY_DOCUMENT_VERSION,
  assertSessionAuthoritySnapshot,
} from "../src/postgres-session-authority.mjs";
import { createSessionManifest } from "../src/session-storage-contracts.mjs";

const SESSION_ID = "019f8475-7c55-7a11-8a88-001122334455";
const THREAD_ID = "019f8475-7c55-7a11-8a88-001122334456";
const BACKEND_ID = "stable-plan-backend";
const SOURCE_STORAGE_ID = "source-storage-001";
const DESTINATION_STORAGE_ID = "destination-storage-001";
const IMAGE_DIGEST = `sha256:${"a".repeat(64)}`;
const NOW = "2026-08-11T12:00:00.000Z";
const PHYSICAL_LIFECYCLE_METHODS = Object.freeze([
  "captureCheckpoint",
  "destroySession",
  "detachAttachment",
  "forceFence",
  "prepareRestoreAttachment",
  "prepareWritableAttachment",
  "provisionSession",
  "reconcileRestoreAttachment",
  "restoreCheckpoint",
]);
const PHYSICAL_PUBLICATION_METHODS = Object.freeze([
  "publishFreshCheckpointArtifact",
  "publishRestoreDestination",
  "verifyCommittedCheckpointArtifact",
  "verifyCommittedRestoreDestination",
]);
const PHYSICAL_SUPERVISOR_METHODS = Object.freeze([
  "launchWriter",
  "reconcileWriterLaunch",
  "stopWriter",
]);

const harnesses = new WeakMap();
const runSerializableDescriptor = Object.getOwnPropertyDescriptor(
  PostgresSerializableStore.prototype,
  "runSerializable",
);

function pinStorePromise(promise) {
  Object.defineProperty(promise, "constructor", {
    configurable: false,
    enumerable: false,
    value: Promise,
    writable: false,
  });
  return promise;
}

function replaceMap(target, source) {
  target.clear();
  for (const [key, value] of source) target.set(key, structuredClone(value));
}

function cloneState(harness) {
  return {
    claims: new Map(structuredClone([...harness.claims])),
    plans: new Map(structuredClone([...harness.plans])),
  };
}

function fakeRunSerializable(callback) {
  const harness = harnesses.get(this);
  assert.notEqual(harness, undefined);
  return pinStorePromise(
    (async () => {
      const transactionId = ++harness.transactionCount;
      const script = harness.transactionScripts.shift() ?? {};
      const state = cloneState(harness);
      const transaction = Object.freeze({
        now: NOW,
        query(text, values) {
          return harness.query({ script, state, text, transactionId, values });
        },
      });
      let value;
      try {
        value = await callback(transaction);
      } catch (error) {
        harness.transactions.push({ outcome: "rolled-back", transactionId });
        throw error;
      }

      const outcome = script.commitOutcome ?? "committed";
      if (outcome === "committed" || outcome === "ack-loss") {
        replaceMap(harness.claims, state.claims);
        replaceMap(harness.plans, state.plans);
      }
      harness.transactions.push({ outcome, transactionId });
      if (outcome === "ack-loss" || outcome === "uncertain-missing") {
        throw new PostgresSerializableStoreError(
          "transaction_commit_outcome_uncertain",
          "uncertain",
        );
      }
      if (outcome === "not-committed") {
        throw new PostgresSerializableStoreError(
          "transaction_commit_outcome_uncertain",
          "not-committed",
        );
      }
      assert.equal(outcome, "committed");
      return value;
    })(),
  );
}

Object.defineProperty(PostgresSerializableStore.prototype, "runSerializable", {
  ...runSerializableDescriptor,
  value: fakeRunSerializable,
});
const registryModule = await import(
  "../src/postgres-detached-restore-stable-plan-registry.mjs?unit-test"
);
Object.defineProperty(
  PostgresSerializableStore.prototype,
  "runSerializable",
  runSerializableDescriptor,
);

const {
  POSTGRES_DETACHED_RESTORE_STABLE_PLAN_PROVISIONING_CONFIRMED,
  PostgresDetachedRestoreStablePlanRegistryError,
  createPostgresDetachedRestoreStablePlanRegistry,
  isPostgresDetachedRestoreStablePlanRegistry,
} = registryModule;

function manifest({ imageDigest = IMAGE_DIGEST } = {}) {
  return createSessionManifest({
    sessionId: SESSION_ID,
    codex: {
      ephemeral: false,
      historyMode: "paginated",
      rootThreadId: THREAD_ID,
      sessionId: THREAD_ID,
    },
    runtime: {
      codexSandbox: "danger-full-access",
      codexVersion: "codex-cli 0.142.4",
      imageDigest,
      imageMediaType: "application/vnd.oci.image.manifest.v1+json",
      platform: "linux/arm64",
    },
  });
}

function expectedSession({
  backendId = BACKEND_ID,
  imageDigest = IMAGE_DIGEST,
  storageId = DESTINATION_STORAGE_ID,
} = {}) {
  return assertSessionAuthoritySnapshot({
    createdAt: NOW,
    document: {
      activeOperation: null,
      attachment: null,
      backendCapabilities: {
        atomicPointInTimeCheckpoint: true,
        exclusiveWriterAttachment: true,
        fencing: "epoch-enforced",
        normalDirectoryAttachment: true,
      },
      documentVersion: SESSION_AUTHORITY_DOCUMENT_VERSION,
      lastOperation: null,
      launch: null,
      lease: null,
      lifecycle: "DETACHED",
      manifest: manifest({ imageDigest }),
      recovery: null,
      storageRef: {
        backendId,
        contractVersion: 1,
        sessionId: SESSION_ID,
        storageId,
      },
      writerEpoch: "0",
    },
    revision: "0",
    sessionId: SESSION_ID,
    updatedAt: NOW,
  });
}

function admission(operationId = "restore-operation-001") {
  return {
    checkpoint: {
      artifactId: "source-artifact-001",
      backendId: BACKEND_ID,
      checkpointClass: "clean",
      checkpointId: "source-checkpoint-001",
      codexSessionId: THREAD_ID,
      codexThreadId: THREAD_ID,
      contractVersion: 1,
      createdAt: "2026-08-11T11:00:00.000Z",
      imageDigest: IMAGE_DIGEST,
      sessionId: SESSION_ID,
      sourceFencingEpoch: "41",
      storageId: SOURCE_STORAGE_ID,
    },
    request: {
      backendId: BACKEND_ID,
      contractVersion: 1,
      fencingEpoch: "42",
      holderId: "restore-holder-001",
      leaseId: "restore-lease-001",
      operation: "restore",
      operationId,
      sessionId: SESSION_ID,
      storageId: DESTINATION_STORAGE_ID,
      target: {
        artifactId: "source-artifact-001",
        checkpointId: "source-checkpoint-001",
        kind: "checkpoint",
      },
    },
  };
}

function stablePlan(value = admission(), overrides = {}) {
  return createPostgresDetachedRestorePlan({
    request: value.request,
    plan: {
      captureCreatedAt: "2026-08-11T12:00:00.000Z",
      destinationDirectory: "/var/lib/portable-codex/restores/restore-001",
      destinationOwnedRoot: "/var/lib/portable-codex/restores",
      detachMode: "release",
      holderId: "restored-writer-001",
      imagePlanId: "image-plan-001",
      leaseDurationMilliseconds: 600_000,
      sourceArtifactDirectory:
        "/var/lib/portable-codex/artifacts/source-001",
      sourceArtifactOwnedRoot: "/var/lib/portable-codex/artifacts",
      ...overrides,
    },
  });
}

function physicalPolicies(methods) {
  return Object.fromEntries(
    methods.map((method) => [
      method,
      Object.freeze({
        deadlineMilliseconds: 1,
        settlementGraceMilliseconds: 1,
      }),
    ]),
  );
}

function operationalLeaseBudget(leaseDurationMilliseconds = 600_000) {
  return createPostgresDetachedRestoreOperationalLeaseBudget({
    databaseRequestMilliseconds: 1,
    imagePlanProviderSettlement: physicalPolicies([
      "inspectCodex",
      "resolveImagePlan",
    ]),
    leaseDurationMilliseconds,
    lifecycleBackendSettlement: physicalPolicies(
      PHYSICAL_LIFECYCLE_METHODS,
    ),
    publicationSettlement: physicalPolicies(PHYSICAL_PUBLICATION_METHODS),
    resolveRestoreDestinationSettlement: Object.freeze({
      deadlineMilliseconds: 1,
      settlementGraceMilliseconds: 1,
    }),
    safetyMarginMilliseconds: 1,
    supervisorSettlement: physicalPolicies(PHYSICAL_SUPERVISOR_METHODS),
  });
}

function createHarness() {
  const harness = {
    claims: new Map(),
    plans: new Map(),
    registryRowMutator: null,
    trace: [],
    transactionCount: 0,
    transactionScripts: [],
    transactions: [],
    query(context) {
      return pinStorePromise(
        new Promise((resolve, reject) => {
          try {
            resolve(querySync(context));
          } catch (error) {
            reject(error);
          }
        }),
      );
    },
  };

  function querySync({ script, state, text, transactionId, values }) {
      let kind;
      if (text.includes("FROM session_authority.sessions")) {
        kind = "session-lock";
        harness.trace.push({ kind, text, transactionId, values });
        return { rows: [{ session_id: values[0] }] };
      }
      if (text.startsWith("INSERT INTO session_authority.operation_id_registry")) {
        kind = "insert-claim";
        harness.trace.push({ kind, text, transactionId, values });
        if (state.claims.has(values[0])) return { rows: [] };
        state.claims.set(values[0], {
          binding: JSON.parse(values[2]),
          claim_type: "detached-restore-stable-plan-v1",
          claimed_at: values[3],
          claimant_operation_id: null,
          materialized_at: null,
          operation_id: values[0],
          session_id: values[1],
        });
        return { rows: [{ operation_id: values[0] }] };
      }
      if (
        text.startsWith(
          "INSERT INTO session_authority.detached_restore_stable_plans",
        )
      ) {
        kind = "insert-plan";
        harness.trace.push({ kind, text, transactionId, values });
        if (state.plans.has(values[0])) return { rows: [] };
        state.plans.set(values[0], {
          admission: JSON.parse(values[5]),
          backend_id: values[2],
          binding_sha256: values[8],
          operation_id: values[0],
          plan_contract_version: values[4],
          plan_input: JSON.parse(values[6]),
          plan_sha256: values[7],
          provisioned_at: values[9],
          session_id: values[1],
          storage_id: values[3],
        });
        return { rows: [{ operation_id: values[0] }] };
      }
      if (text.includes("FROM session_authority.operation_id_registry")) {
        kind = text.includes("FOR UPDATE OF registry")
          ? "read-for-update"
          : "read";
        harness.trace.push({ kind, text, transactionId, values });
        if (script.hideRegistryReads === true) return { rows: [] };
        const claim = state.claims.get(values[0]);
        if (claim === undefined) return { rows: [] };
        const plan = state.plans.get(values[0]);
        let row = structuredClone({
              admission: plan?.admission ?? null,
              backend_id: plan?.backend_id ?? null,
              binding_sha256: plan?.binding_sha256 ?? null,
              claim_binding: claim.binding,
              claim_type: claim.claim_type,
              claimed_at: claim.claimed_at,
              claimant_operation_id: claim.claimant_operation_id,
              materialized_at: claim.materialized_at,
              operation_id: claim.operation_id,
              plan_contract_version: plan?.plan_contract_version ?? null,
              plan_input: plan?.plan_input ?? null,
              plan_sha256: plan?.plan_sha256 ?? null,
              provisioned_at: plan?.provisioned_at ?? null,
              session_id: claim.session_id,
              stable_operation_id: plan?.operation_id ?? null,
              stable_session_id: plan?.session_id ?? null,
              storage_id: plan?.storage_id ?? null,
            });
        if (harness.registryRowMutator !== null) {
          row = harness.registryRowMutator(row);
        }
        return { rows: [row] };
      }
      assert.fail(`unexpected registry query: ${text}`);
  }

  return harness;
}

function createFixture({
  gateResult = POSTGRES_DETACHED_RESTORE_STABLE_PLAN_PROVISIONING_CONFIRMED,
  leaseBudget = operationalLeaseBudget(),
} = {}) {
  const harness = createHarness();
  const store = new PostgresSerializableStore({
    dedicatedPool: { connect() {} },
  });
  harnesses.set(store, harness);
  let gateCalls = 0;
  let lastGateInput;
  const registry = createPostgresDetachedRestoreStablePlanRegistry({
    operationalLeaseBudget: leaseBudget,
    provisioningFleetCapabilityGate(input) {
      gateCalls += 1;
      lastGateInput = input;
      return typeof gateResult === "function" ? gateResult(input) : gateResult;
    },
    store,
  });
  return {
    get gateCalls() {
      return gateCalls;
    },
    get lastGateInput() {
      return lastGateInput;
    },
    harness,
    leaseBudget,
    registry,
    store,
  };
}

function traceCount(harness, kind) {
  return harness.trace.filter((entry) => entry.kind === kind).length;
}

function assertProtectedPublicPromise(value) {
  assert.equal(value instanceof Promise, true);
  assert.equal(Object.getPrototypeOf(value), Promise.prototype);
  const constructorDescriptor = Object.getOwnPropertyDescriptor(
    value,
    "constructor",
  );
  assert.deepEqual(
    {
      configurable: constructorDescriptor.configurable,
      enumerable: constructorDescriptor.enumerable,
      writable: constructorDescriptor.writable,
    },
    { configurable: false, enumerable: false, writable: false },
  );
  const speciesHolder = constructorDescriptor.value;
  assert.equal(Object.getPrototypeOf(speciesHolder), null);
  assert.equal(Object.isFrozen(speciesHolder), true);
  assert.deepEqual(Reflect.ownKeys(speciesHolder), [Symbol.species]);
  assert.equal(speciesHolder[Symbol.species], Promise);
  for (const key of ["then", "catch", "finally"]) {
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    assert.equal(descriptor.configurable, false);
    assert.equal(descriptor.enumerable, false);
    assert.equal(descriptor.writable, false);
    assert.equal(typeof descriptor.value, "function");
  }
}

function registryError(code) {
  return (error) => {
    assert(error instanceof PostgresDetachedRestoreStablePlanRegistryError);
    assert.equal(error.code, code);
    assert.equal(error.retryable, false);
    assert.equal(Object.isFrozen(error), true);
    return true;
  };
}

test("stable plan registry exposes one exact frozen branded facade", () => {
  const fixture = createFixture();
  assert.deepEqual(Reflect.ownKeys(fixture.registry), [
    "provisionStablePlan",
    "resolveStablePlan",
  ]);
  assert.equal(Object.getPrototypeOf(fixture.registry), null);
  assert.equal(Object.isFrozen(fixture.registry), true);
  assert.equal(isPostgresDetachedRestoreStablePlanRegistry(fixture.registry), true);
  assert.equal(
    isPostgresDetachedRestoreStablePlanRegistry(
      Object.freeze({ ...fixture.registry }),
    ),
    false,
  );
  assert.deepEqual(fixture.harness.trace, []);
});

test("stable plan registry rejects a forged operational lease budget", () => {
  const fixture = createFixture();
  assert.throws(
    () =>
      createPostgresDetachedRestoreStablePlanRegistry({
        operationalLeaseBudget: Object.freeze({}),
        provisioningFleetCapabilityGate() {
          assert.fail("forged budget must fail before the fleet gate");
        },
        store: fixture.store,
      }),
    registryError(
      "invalid_postgres_detached_restore_stable_plan_registry_options",
    ),
  );
  assert.equal(fixture.gateCalls, 0);
  assert.deepEqual(fixture.harness.trace, []);
});

test("a short plan lease fails before the provisioning gate or PostgreSQL", async () => {
  const fixture = createFixture();
  const value = admission("restore-operation-short-lease-001");
  await assert.rejects(
    fixture.registry.provisionStablePlan({
      admission: value,
      plan: stablePlan(value, { leaseDurationMilliseconds: 599_999 }),
    }),
    registryError(
      "postgres_detached_restore_stable_plan_registry_operational_lease_required",
    ),
  );
  assert.equal(fixture.gateCalls, 0);
  assert.equal(fixture.harness.transactionCount, 0);
  assert.deepEqual(fixture.harness.trace, []);
});

test("provisioning gate denies before the first PostgreSQL command", async () => {
  const fixture = createFixture({ gateResult: null });
  const value = admission();
  await assert.rejects(
    fixture.registry.provisionStablePlan({
      admission: value,
      plan: stablePlan(value),
    }),
    registryError(
      "postgres_detached_restore_stable_plan_provisioning_capability_required",
    ),
  );
  assert.equal(fixture.gateCalls, 1);
  assert.deepEqual(fixture.harness.trace, []);
});

test("fresh provision and exact replay return a durable rehydrated plan", async () => {
  const fixture = createFixture();
  const value = admission();
  const candidate = stablePlan(value);
  const first = await fixture.registry.provisionStablePlan({
    admission: value,
    plan: candidate,
  });
  assert.equal(isPostgresDetachedRestorePlan(first), true);
  assert.notStrictEqual(first, candidate);
  assert.equal(first.planSha256, candidate.planSha256);
  const writesAfterFirst = fixture.harness.trace.filter((entry) =>
    entry.text.startsWith("INSERT INTO"),
  ).length;

  const replay = await fixture.registry.provisionStablePlan({
    admission: value,
    plan: candidate,
  });
  assert.equal(isPostgresDetachedRestorePlan(replay), true);
  assert.equal(replay.planSha256, candidate.planSha256);
  assert.equal(
    traceCount(fixture.harness, "insert-plan"),
    1,
  );
  assert.equal(writesAfterFirst, 2);
});

test("a plan lease exactly equal to the configured duration is admitted", async () => {
  const leaseDurationMilliseconds = 600_000;
  const fixture = createFixture({
    leaseBudget: operationalLeaseBudget(leaseDurationMilliseconds),
  });
  const value = admission("restore-operation-exact-lease-001");
  const result = await fixture.registry.provisionStablePlan({
    admission: value,
    plan: stablePlan(value, { leaseDurationMilliseconds }),
  });
  assert.equal(isPostgresDetachedRestorePlan(result), true);
  assert.equal(result.leaseDurationMilliseconds, leaseDurationMilliseconds);
  assert.equal(fixture.gateCalls, 1);
  assert.equal(traceCount(fixture.harness, "insert-plan"), 1);
});

test("resolver performs only read commands and reports a missing plan", async () => {
  const fixture = createFixture();
  const value = admission();
  await assert.rejects(
    fixture.registry.resolveStablePlan({
      admission: value,
      expectedSession: expectedSession(),
    }),
    registryError("postgres_detached_restore_stable_plan_registry_not_found"),
  );
  assert.equal(fixture.gateCalls, 0);
  assert.equal(fixture.harness.trace.length, 1);
  assert.equal(
    fixture.harness.trace.every((entry) => entry.text.startsWith("SELECT ")),
    true,
  );
});

test("resolver rehydrates the exact persisted plan without writes", async () => {
  const fixture = createFixture();
  const value = admission();
  const candidate = stablePlan(value);
  await fixture.registry.provisionStablePlan({ admission: value, plan: candidate });
  fixture.harness.trace.length = 0;

  const resolved = await fixture.registry.resolveStablePlan({
    admission: value,
    expectedSession: expectedSession(),
  });
  assert.equal(isPostgresDetachedRestorePlan(resolved), true);
  assert.equal(resolved.planSha256, candidate.planSha256);
  assert.equal(
    fixture.harness.trace.every((entry) => entry.text.startsWith("SELECT ")),
    true,
  );
});

test("every resolution rejects a durable plan after operational lease drift", async () => {
  const fixture = createFixture();
  const value = admission("restore-operation-lease-drift-001");
  await fixture.registry.provisionStablePlan({
    admission: value,
    plan: stablePlan(value),
  });
  fixture.harness.trace.length = 0;
  const driftedRegistry = createPostgresDetachedRestoreStablePlanRegistry({
    operationalLeaseBudget: operationalLeaseBudget(600_001),
    provisioningFleetCapabilityGate() {
      assert.fail("resolution must not invoke the provisioning gate");
    },
    store: fixture.store,
  });
  for (let attempt = 0; attempt < 2; attempt += 1) {
    await assert.rejects(
      driftedRegistry.resolveStablePlan({
        admission: value,
        expectedSession: expectedSession(),
      }),
      registryError(
        "postgres_detached_restore_stable_plan_registry_operational_lease_required",
      ),
    );
  }
  assert.equal(traceCount(fixture.harness, "read"), 2);
  assert.equal(traceCount(fixture.harness, "insert-claim"), 0);
  assert.equal(traceCount(fixture.harness, "insert-plan"), 0);
});

test("commit acknowledgement loss succeeds only through exact authoritative readback", async () => {
  const fixture = createFixture();
  const value = admission("restore-operation-ack-loss-001");
  const candidate = stablePlan(value);
  fixture.harness.transactionScripts.push({ commitOutcome: "ack-loss" });

  const result = await fixture.registry.provisionStablePlan({
    admission: value,
    plan: candidate,
  });
  assert.equal(isPostgresDetachedRestorePlan(result), true);
  assert.equal(result.planSha256, candidate.planSha256);
  assert.equal(fixture.harness.claims.size, 1);
  assert.equal(fixture.harness.plans.size, 1);
  assert.equal(fixture.harness.transactionCount, 2);
  assert.deepEqual(fixture.harness.transactions, [
    { outcome: "ack-loss", transactionId: 1 },
    { outcome: "committed", transactionId: 2 },
  ]);
  assert.equal(traceCount(fixture.harness, "insert-claim"), 1);
  assert.equal(traceCount(fixture.harness, "insert-plan"), 1);
  assert.equal(traceCount(fixture.harness, "read-for-update"), 1);
  assert.equal(traceCount(fixture.harness, "read"), 1);
});

test("public methods protect their native Promise and every derived reaction", async () => {
  const fixture = createFixture();
  const value = admission("restore-operation-protected-promise-001");
  const promise = fixture.registry.provisionStablePlan({
    admission: value,
    plan: stablePlan(value),
  });
  assertProtectedPublicPromise(promise);
  const child = promise.then((result) => result);
  assertProtectedPublicPromise(child);
  assert.equal(isPostgresDetachedRestorePlan(await child), true);
});

test("a native async provisioning gate is accepted with an exact frozen input", async () => {
  let observedInput;
  const fixture = createFixture({
    gateResult(input) {
      observedInput = input;
      return Promise.resolve(
        POSTGRES_DETACHED_RESTORE_STABLE_PLAN_PROVISIONING_CONFIRMED,
      );
    },
  });
  const value = admission("restore-operation-async-gate-001");
  await fixture.registry.provisionStablePlan({
    admission: value,
    plan: stablePlan(value),
  });
  assert.equal(Object.getPrototypeOf(observedInput), null);
  assert.equal(Object.isFrozen(observedInput), true);
  assert.deepEqual(Reflect.ownKeys(observedInput), ["admission", "plan"]);
});

test("hostile provisioning gate results fail closed before PostgreSQL", async (t) => {
  class GatePromise extends Promise {}
  const cases = [
    ["thenable", () => ({ then() {} })],
    [
      "Promise subclass",
      () =>
        GatePromise.resolve(
          POSTGRES_DETACHED_RESTORE_STABLE_PLAN_PROVISIONING_CONFIRMED,
        ),
    ],
    [
      "proxied Promise",
      () =>
        new Proxy(
          Promise.resolve(
            POSTGRES_DETACHED_RESTORE_STABLE_PLAN_PROVISIONING_CONFIRMED,
          ),
          {},
        ),
    ],
    ["rejected native Promise", () => Promise.reject(new Error("denied"))],
  ];

  for (const [name, result] of cases) {
    await t.test(name, async () => {
      const fixture = createFixture({ gateResult: () => result() });
      const value = admission(`restore-operation-hostile-gate-${name.length}`);
      await assert.rejects(
        fixture.registry.provisionStablePlan({
          admission: value,
          plan: stablePlan(value),
        }),
        registryError(
          "postgres_detached_restore_stable_plan_provisioning_capability_required",
        ),
      );
      assert.equal(fixture.gateCalls, 1);
      assert.deepEqual(fixture.harness.trace, []);
      assert.equal(fixture.harness.transactionCount, 0);
    });
  }
});

test("accessor and Proxy public requests fail before gate or PostgreSQL", async (t) => {
  const value = admission("restore-operation-hostile-request-001");
  const candidate = stablePlan(value);
  const accessorRequest = { plan: candidate };
  Object.defineProperty(accessorRequest, "admission", {
    enumerable: true,
    get() {
      throw new Error("must not execute");
    },
  });
  const nestedAccessorAdmission = structuredClone(value);
  Object.defineProperty(nestedAccessorAdmission, "request", {
    enumerable: true,
    get() {
      throw new Error("must not execute");
    },
  });
  const cases = [
    ["accessor", accessorRequest],
    ["Proxy", new Proxy({ admission: value, plan: candidate }, {})],
    [
      "nested admission accessor",
      { admission: nestedAccessorAdmission, plan: candidate },
    ],
  ];

  for (const [name, request] of cases) {
    await t.test(name, async () => {
      const fixture = createFixture();
      await assert.rejects(
        fixture.registry.provisionStablePlan(request),
        registryError(
          "invalid_postgres_detached_restore_stable_plan_registry_request",
        ),
      );
      assert.equal(fixture.gateCalls, 0);
      assert.deepEqual(fixture.harness.trace, []);
    });
  }
});

test("equal and lower restore fencing epochs fail before gate or PostgreSQL", async (t) => {
  for (const fencingEpoch of ["41", "40"]) {
    await t.test(`fencing epoch ${fencingEpoch}`, async () => {
      const value = admission(`restore-operation-fence-${fencingEpoch}`);
      value.request.fencingEpoch = fencingEpoch;
      const fixture = createFixture();
      await assert.rejects(
        fixture.registry.provisionStablePlan({
          admission: value,
          plan: stablePlan(value),
        }),
        registryError(
          "invalid_postgres_detached_restore_stable_plan_registry_request",
        ),
      );
      assert.equal(fixture.gateCalls, 0);
      assert.deepEqual(fixture.harness.trace, []);
    });
  }
});

test("crossed candidate plan and admission fail before gate or PostgreSQL", async () => {
  const value = admission("restore-operation-crossed-local-001");
  const crossed = structuredClone(value);
  crossed.request.fencingEpoch = "43";
  const fixture = createFixture();
  await assert.rejects(
    fixture.registry.provisionStablePlan({
      admission: value,
      plan: stablePlan(crossed),
    }),
    registryError(
      "invalid_postgres_detached_restore_stable_plan_registry_request",
    ),
  );
  assert.equal(fixture.gateCalls, 0);
  assert.deepEqual(fixture.harness.trace, []);
});

test("same operation with a crossed durable admission and plan is an identity conflict", async () => {
  const fixture = createFixture();
  const firstAdmission = admission("restore-operation-crossed-replay-001");
  await fixture.registry.provisionStablePlan({
    admission: firstAdmission,
    plan: stablePlan(firstAdmission),
  });
  const crossedAdmission = structuredClone(firstAdmission);
  crossedAdmission.request.fencingEpoch = "43";
  await assert.rejects(
    fixture.registry.provisionStablePlan({
      admission: crossedAdmission,
      plan: stablePlan(crossedAdmission, {
        holderId: "crossed-restored-writer-001",
      }),
    }),
    registryError(
      "postgres_detached_restore_stable_plan_registry_identity_conflict",
    ),
  );
  assert.equal(fixture.harness.claims.size, 1);
  assert.equal(fixture.harness.plans.size, 1);
  assert.equal(fixture.harness.transactionCount, 2);
});

test("an operation ID owned by a different claim type is an identity conflict", async () => {
  const fixture = createFixture();
  const value = admission("restore-operation-crossed-claim-001");
  await fixture.registry.provisionStablePlan({
    admission: value,
    plan: stablePlan(value),
  });
  fixture.harness.claims.get(value.request.operationId).claim_type =
    "restore-generation-v1";
  fixture.harness.trace.length = 0;
  await assert.rejects(
    fixture.registry.resolveStablePlan({
      admission: value,
      expectedSession: expectedSession(),
    }),
    registryError(
      "postgres_detached_restore_stable_plan_registry_identity_conflict",
    ),
  );
  assert.equal(traceCount(fixture.harness, "read"), 1);
});

test("resolver rejects manifest and storage authority mismatches without SQL", async (t) => {
  const value = admission("restore-operation-resolver-authority-001");
  const cases = [
    [
      "manifest image",
      expectedSession({ imageDigest: `sha256:${"b".repeat(64)}` }),
    ],
    ["storage", expectedSession({ storageId: "other-destination-storage" })],
    ["backend", expectedSession({ backendId: "other-stable-plan-backend" })],
  ];
  for (const [name, session] of cases) {
    await t.test(name, async () => {
      const fixture = createFixture();
      await assert.rejects(
        fixture.registry.resolveStablePlan({
          admission: value,
          expectedSession: session,
        }),
        registryError(
          "invalid_postgres_detached_restore_stable_plan_registry_request",
        ),
      );
      assert.equal(fixture.gateCalls, 0);
      assert.deepEqual(fixture.harness.trace, []);
      assert.equal(fixture.harness.transactionCount, 0);
    });
  }
});

test("corrupt durable rows fail closed as invalid state", async (t) => {
  const cases = [
    ["unexpected row field", (_claim, _plan, harness) => {
      harness.registryRowMutator = (row) => ({ ...row, unexpected: true });
    }],
    ["missing stable row", (_claim, _plan, harness, operationId) => {
      harness.plans.delete(operationId);
    }],
    ["claimant relation", (claim) => {
      claim.claimant_operation_id = "other-operation";
    }],
    ["stable operation relation", (_claim, plan) => {
      plan.operation_id = "other-operation";
    }],
    ["stable session relation", (_claim, plan) => {
      plan.session_id = "019f8475-7c55-7a11-8a88-001122334499";
    }],
    ["backend relation", (_claim, plan) => {
      plan.backend_id = "other-backend";
    }],
    ["storage relation", (_claim, plan) => {
      plan.storage_id = "other-storage";
    }],
    ["claimed/provisioned time relation", (claim) => {
      claim.claimed_at = "2026-08-11T11:59:59.000Z";
    }],
    ["materialized before claim", (claim) => {
      claim.materialized_at = "2026-08-11T11:59:59.000Z";
    }],
    ["plan digest", (_claim, plan) => {
      plan.plan_sha256 = "0".repeat(64);
    }],
    ["binding digest", (_claim, plan) => {
      plan.binding_sha256 = "0".repeat(64);
    }],
    ["claim binding plan digest", (claim) => {
      claim.binding.planSha256 = "0".repeat(64);
    }],
    ["claim binding digest", (claim) => {
      claim.binding.bindingSha256 = "0".repeat(64);
    }],
    ["crossed claim request", (claim) => {
      claim.binding.request = admission("other-restore-operation").request;
    }],
    ["plan input", (_claim, plan) => {
      plan.plan_input.leaseDurationMilliseconds += 1;
    }],
    ["crossed durable admission", (_claim, plan) => {
      plan.admission.request.fencingEpoch = "43";
    }],
  ];

  for (const [name, corrupt] of cases) {
    await t.test(name, async () => {
      const fixture = createFixture();
      const value = admission(
        `restore-operation-corrupt-${String(name).replace(/[^A-Za-z0-9]/gu, "-")}`,
      );
      await fixture.registry.provisionStablePlan({
        admission: value,
        plan: stablePlan(value),
      });
      const operationId = value.request.operationId;
      corrupt(
        fixture.harness.claims.get(operationId),
        fixture.harness.plans.get(operationId),
        fixture.harness,
        operationId,
      );
      fixture.harness.trace.length = 0;
      await assert.rejects(
        fixture.registry.resolveStablePlan({
          admission: value,
          expectedSession: expectedSession(),
        }),
        registryError(
          "postgres_detached_restore_stable_plan_registry_state_invalid",
        ),
      );
      assert.equal(traceCount(fixture.harness, "read"), 1);
      assert.equal(traceCount(fixture.harness, "insert-claim"), 0);
      assert.equal(traceCount(fixture.harness, "insert-plan"), 0);
    });
  }
});

test("flat hostile claim rows fail before bounded key sorting or a next effect", async (t) => {
  const canonicalObjectKeyCap = 24;
  const cases = [
    ["request at key cap", canonicalObjectKeyCap, "request"],
    ["request over key cap", canonicalObjectKeyCap + 1, "request"],
    ["large reverse-ordered request", 8_192, "request"],
    ["large reverse-ordered claim binding", 8_192, "binding"],
  ];

  for (const [name, keyCount, target] of cases) {
    await t.test(name, async () => {
      const fixture = createFixture();
      const value = admission(
        `restore-operation-hostile-flat-${String(keyCount)}`,
      );
      await fixture.registry.provisionStablePlan({
        admission: value,
        plan: stablePlan(value),
      });

      let nextEffectCount = 0;
      const hostileObject = Object.create(null);
      Object.defineProperty(hostileObject, "contractVersion", {
        enumerable: true,
        get() {
          nextEffectCount += 1;
          return 1;
        },
      });
      for (let index = keyCount - 1; index > 0; index -= 1) {
        Object.defineProperty(
          hostileObject,
          `field_${String(index).padStart(5, "0")}`,
          { enumerable: true, value: index },
        );
      }
      assert.equal(Reflect.ownKeys(hostileObject).length, keyCount);
      fixture.harness.registryRowMutator = (row) => {
        if (target === "request") {
          row.claim_binding.request = hostileObject;
        } else {
          row.claim_binding = hostileObject;
        }
        return row;
      };
      fixture.harness.trace.length = 0;
      fixture.harness.transactions.length = 0;

      await assert.rejects(
        fixture.registry.resolveStablePlan({
          admission: value,
          expectedSession: expectedSession(),
        }),
        registryError(
          "postgres_detached_restore_stable_plan_registry_state_invalid",
        ),
      );
      assert.equal(nextEffectCount, 0);
      assert.equal(fixture.harness.transactions.length, 1);
      assert.equal(fixture.harness.transactions[0].outcome, "rolled-back");
      assert.equal(traceCount(fixture.harness, "read"), 1);
      assert.equal(traceCount(fixture.harness, "read-for-update"), 0);
      assert.equal(traceCount(fixture.harness, "insert-claim"), 0);
      assert.equal(traceCount(fixture.harness, "insert-plan"), 0);
    });
  }
});

test("one invisible in-transaction read rolls back and retries provisioning once", async () => {
  const fixture = createFixture();
  const value = admission("restore-operation-visibility-retry-001");
  fixture.harness.transactionScripts.push({ hideRegistryReads: true });
  const result = await fixture.registry.provisionStablePlan({
    admission: value,
    plan: stablePlan(value),
  });
  assert.equal(isPostgresDetachedRestorePlan(result), true);
  assert.deepEqual(fixture.harness.transactions, [
    { outcome: "rolled-back", transactionId: 1 },
    { outcome: "committed", transactionId: 2 },
  ]);
  assert.equal(traceCount(fixture.harness, "session-lock"), 2);
  assert.equal(traceCount(fixture.harness, "insert-claim"), 2);
  assert.equal(traceCount(fixture.harness, "insert-plan"), 2);
  assert.equal(traceCount(fixture.harness, "read-for-update"), 2);
  assert.equal(traceCount(fixture.harness, "read"), 0);
  assert.equal(fixture.harness.claims.size, 1);
  assert.equal(fixture.harness.plans.size, 1);
});

test("a proved not-committed miss reads back, then retries once", async () => {
  const fixture = createFixture();
  const value = admission("restore-operation-not-committed-retry-001");
  fixture.harness.transactionScripts.push({ commitOutcome: "not-committed" });
  const result = await fixture.registry.provisionStablePlan({
    admission: value,
    plan: stablePlan(value),
  });
  assert.equal(isPostgresDetachedRestorePlan(result), true);
  assert.deepEqual(fixture.harness.transactions, [
    { outcome: "not-committed", transactionId: 1 },
    { outcome: "committed", transactionId: 2 },
    { outcome: "committed", transactionId: 3 },
  ]);
  assert.equal(traceCount(fixture.harness, "insert-claim"), 2);
  assert.equal(traceCount(fixture.harness, "insert-plan"), 2);
  assert.equal(traceCount(fixture.harness, "read"), 1);
  assert.equal(fixture.harness.claims.size, 1);
  assert.equal(fixture.harness.plans.size, 1);
});

test("an uncertain commit with an authoritative miss is never retried", async () => {
  const fixture = createFixture();
  const value = admission("restore-operation-uncertain-missing-001");
  fixture.harness.transactionScripts.push({
    commitOutcome: "uncertain-missing",
  });
  await assert.rejects(
    fixture.registry.provisionStablePlan({
      admission: value,
      plan: stablePlan(value),
    }),
    registryError(
      "postgres_detached_restore_stable_plan_registry_outcome_uncertain",
    ),
  );
  assert.deepEqual(fixture.harness.transactions, [
    { outcome: "uncertain-missing", transactionId: 1 },
    { outcome: "committed", transactionId: 2 },
  ]);
  assert.equal(traceCount(fixture.harness, "insert-claim"), 1);
  assert.equal(traceCount(fixture.harness, "insert-plan"), 1);
  assert.equal(traceCount(fixture.harness, "read"), 1);
  assert.equal(fixture.harness.claims.size, 0);
  assert.equal(fixture.harness.plans.size, 0);
});

test("stable-plan binding SHA-256 remains on its fixed canonical vector", async () => {
  const fixture = createFixture();
  const value = admission("restore-operation-binding-vector-001");
  await fixture.registry.provisionStablePlan({
    admission: value,
    plan: stablePlan(value),
  });
  const claim = fixture.harness.claims.get(value.request.operationId);
  const plan = fixture.harness.plans.get(value.request.operationId);
  assert.equal(
    claim.binding.bindingSha256,
    "d2d77ece3a7f3ed1f8427322137e96be40e33f4b3084c1041e2c2674f04c90d5",
  );
  assert.equal(plan.binding_sha256, claim.binding.bindingSha256);
  assert.equal(claim.binding.planSha256, plan.plan_sha256);
});
