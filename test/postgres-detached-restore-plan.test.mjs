import assert from "node:assert/strict";
import { Hash } from "node:crypto";
import test from "node:test";

import {
  POSTGRES_DETACHED_RESTORE_PLAN_CONTRACT_VERSION,
  PostgresDetachedRestorePlanError,
  createPostgresDetachedRestorePlan,
  isPostgresDetachedRestorePlan,
  rehydratePostgresDetachedRestorePlan,
} from "../src/postgres-detached-restore-plan.mjs";

const SESSION_ID = "018f8475-7c55-7a11-8a88-001122334455";
const DERIVED_ID_KEYS = Object.freeze([
  "renewalOperationId",
  "captureOperationId",
  "captureArtifactId",
  "captureCheckpointId",
  "generationId",
  "destinationIsolationProofId",
  "detachOperationId",
  "activationOperationId",
  "launchAttemptId",
]);

function restoreRequest() {
  return {
    backendId: "storage-backend-001",
    contractVersion: 1,
    fencingEpoch: "42",
    holderId: "restore-holder-001",
    leaseId: "restore-lease-001",
    operation: "restore",
    operationId: "restore-root-001",
    sessionId: SESSION_ID,
    storageId: "storage-001",
    target: {
      artifactId: "artifact-source-001",
      checkpointId: "checkpoint-source-001",
      kind: "checkpoint",
    },
  };
}

function stablePlan() {
  return {
    captureCreatedAt: "2026-08-11T09:00:00.000Z",
    destinationDirectory:
      "/var/lib/portable-codex/restores/destination-001",
    destinationOwnedRoot: "/var/lib/portable-codex/restores",
    detachMode: "release",
    holderId: "writer-holder-002",
    imagePlanId: "image-plan-001",
    leaseDurationMilliseconds: 600_000,
    sourceArtifactDirectory:
      "/var/lib/portable-codex/artifacts/source-001",
    sourceArtifactOwnedRoot: "/var/lib/portable-codex/artifacts",
  };
}

function fixture() {
  return { plan: stablePlan(), request: restoreRequest() };
}

function reverseRecord(value) {
  const result = Object.create(null);
  const keys = Reflect.ownKeys(value).reverse();
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
    const child = value[key];
    result[key] =
      child !== null && typeof child === "object"
        ? reverseRecord(child)
        : child;
  }
  return result;
}

function assertPlanError(operation) {
  assert.throws(operation, (error) => {
    assert.equal(error instanceof PostgresDetachedRestorePlanError, true);
    assert.equal(error.code, "invalid_postgres_detached_restore_plan");
    assert.equal(error.retryable, false);
    assert.equal(Object.isFrozen(error), true);
    return true;
  });
}

function cloneWithPlan(overrides) {
  const value = fixture();
  Object.assign(value.plan, overrides);
  return value;
}

function cloneWithRequest(overrides) {
  const value = fixture();
  Object.assign(value.request, overrides);
  return value;
}

test("detached restore plan binds one exact request and stable root plan", () => {
  const value = fixture();
  const plan = createPostgresDetachedRestorePlan(value);

  assert.equal(
    plan.contractVersion,
    POSTGRES_DETACHED_RESTORE_PLAN_CONTRACT_VERSION,
  );
  assert.deepEqual(Reflect.ownKeys(plan), [
    "contractVersion",
    "request",
    "captureCreatedAt",
    "destinationDirectory",
    "destinationOwnedRoot",
    "detachMode",
    "holderId",
    "imagePlanId",
    "leaseDurationMilliseconds",
    "sourceArtifactDirectory",
    "sourceArtifactOwnedRoot",
    "renewalOperationId",
    "captureOperationId",
    "captureArtifactId",
    "captureCheckpointId",
    "generationId",
    "destinationIsolationProofId",
    "detachOperationId",
    "activationOperationId",
    "launchAttemptId",
    "planSha256",
  ]);
  assert.deepEqual(Reflect.ownKeys(plan.request), [
    "backendId",
    "contractVersion",
    "fencingEpoch",
    "holderId",
    "leaseId",
    "operation",
    "operationId",
    "sessionId",
    "storageId",
    "target",
  ]);
  assert.equal(Object.getPrototypeOf(plan), null);
  assert.equal(Object.getPrototypeOf(plan.request), null);
  assert.equal(Object.getPrototypeOf(plan.request.target), null);
  assert.equal(Object.isFrozen(plan), true);
  assert.equal(Object.isFrozen(plan.request), true);
  assert.equal(Object.isFrozen(plan.request.target), true);
  assert.equal(isPostgresDetachedRestorePlan(plan), true);
  assert.equal(Object.hasOwn(plan, "stopOperationId"), false);
  assert.equal(Object.hasOwn(plan, "captureAttemptId"), false);
  assert.match(plan.planSha256, /^[0-9a-f]{64}$/u);
  for (const key of DERIVED_ID_KEYS) {
    assert.match(plan[key], /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u);
  }

  value.request.target.artifactId = "mutated-artifact";
  value.plan.captureCreatedAt = "2026-08-11T10:00:00.000Z";
  assert.equal(plan.request.target.artifactId, "artifact-source-001");
  assert.equal(plan.captureCreatedAt, "2026-08-11T09:00:00.000Z");
});

test("detached restore plan identity has a locked domain-separated encoding", () => {
  const plan = createPostgresDetachedRestorePlan(fixture());

  assert.deepEqual(
    Object.fromEntries(
      [...DERIVED_ID_KEYS, "planSha256"].map((key) => [key, plan[key]]),
    ),
    {
      activationOperationId:
        "restore-activation:08fc55113e34874141501006124851703e993444fbde7ec8601a7d6b70959232",
      captureArtifactId:
        "restore-artifact:c7d4ea9c47fce8f98519118507335fadc87b2e50d3597439c996508d6d5700f4",
      captureCheckpointId:
        "restore-checkpoint:0fec9a60be0ebda0f0576c861fff2e6abc45e6754169509c6b7df6ca31e66ae8",
      captureOperationId:
        "restore-capture:1ce76f4fda397155e5db9884de50b0220275eaa932a15b4a3a7b492a013fec10",
      destinationIsolationProofId:
        "restore-destination-proof:a590a6ac1387997356290843280c86172a512b09f1d65bbe78cc161e633680ac",
      detachOperationId:
        "restore-detach:2ee709dbb57391a692bde065df3320cd78be6271cfac5b4c9077fe3e1519af52",
      generationId:
        "restore-generation:68474d3614bfc17e95beaed80330e121141462bc7092f912ad2c47e33fae099c",
      launchAttemptId:
        "restore-launch:09fe9abd045e5bbcd9bad093a54ac7dea0467eb74feb21e42aa3e05f9d2ee971",
      planSha256:
        "9deadf0831ed48abba142646548b62f9bdfc394f2209f1dd5ae7c18bf209e89a",
      renewalOperationId:
        "restore-renewal:6792fd12977c3b87e18b4e55b372f3ccd316be849ceb1588a80cc45a336345b1",
    },
  );
});

test("equivalent property order produces one stable branded plan", () => {
  const canonical = createPostgresDetachedRestorePlan(fixture());
  const reordered = createPostgresDetachedRestorePlan(
    reverseRecord(fixture()),
  );

  assert.equal(JSON.stringify(reordered), JSON.stringify(canonical));
  assert.equal(isPostgresDetachedRestorePlan(reordered), true);
  assert.equal(
    isPostgresDetachedRestorePlan(
      Object.freeze(JSON.parse(JSON.stringify(canonical))),
    ),
    false,
  );
  assert.equal(isPostgresDetachedRestorePlan(canonical, canonical), false);
  assert.equal(isPostgresDetachedRestorePlan(), false);
});

test("durable detached restore plan documents rehydrate only by recomputing the full contract", () => {
  const original = createPostgresDetachedRestorePlan(fixture());
  const durable = JSON.parse(JSON.stringify(original));
  const rehydrated = rehydratePostgresDetachedRestorePlan(
    reverseRecord(durable),
  );

  assert.notStrictEqual(rehydrated, original);
  assert.equal(isPostgresDetachedRestorePlan(durable), false);
  assert.equal(isPostgresDetachedRestorePlan(rehydrated), true);
  assert.equal(JSON.stringify(rehydrated), JSON.stringify(original));
  assert.equal(Object.getPrototypeOf(rehydrated), null);
  assert.equal(Object.isFrozen(rehydrated), true);
  assert.equal(Object.isFrozen(rehydrated.request), true);
  assert.equal(Object.isFrozen(rehydrated.request.target), true);
});

test("detached restore plan rehydration rejects any persisted identity drift", () => {
  const original = createPostgresDetachedRestorePlan(fixture());
  const mutations = [
    (value) => {
      value.contractVersion = 2;
    },
    (value) => {
      value.request.leaseId = "restore-lease-002";
    },
    (value) => {
      value.captureCreatedAt = "2026-08-11T09:00:01.000Z";
    },
    (value) => {
      value.captureOperationId = `restore-capture:${"0".repeat(64)}`;
    },
    (value) => {
      value.planSha256 = "0".repeat(64);
    },
  ];

  for (const mutate of mutations) {
    const durable = JSON.parse(JSON.stringify(original));
    mutate(durable);
    assertPlanError(() => rehydratePostgresDetachedRestorePlan(durable));
  }
});

test("detached restore plan rehydration rejects hostile and inexact document shapes", () => {
  const original = JSON.parse(
    JSON.stringify(createPostgresDetachedRestorePlan(fixture())),
  );
  assertPlanError(() => rehydratePostgresDetachedRestorePlan());
  assertPlanError(() =>
    rehydratePostgresDetachedRestorePlan(original, original),
  );
  assertPlanError(() =>
    rehydratePostgresDetachedRestorePlan(new Proxy(original, {})),
  );
  assertPlanError(() =>
    rehydratePostgresDetachedRestorePlan({ ...original, extra: true }),
  );
  const missing = { ...original };
  delete missing.planSha256;
  assertPlanError(() => rehydratePostgresDetachedRestorePlan(missing));

  let getterCalls = 0;
  const accessor = { ...original };
  Object.defineProperty(accessor, "planSha256", {
    enumerable: true,
    get() {
      getterCalls += 1;
      throw new Error("persisted plan accessor must not run");
    },
  });
  assertPlanError(() => rehydratePostgresDetachedRestorePlan(accessor));
  assert.equal(getterCalls, 0);
});

test("detached restore plan rehydration never consults inherited toJSON", () => {
  const descriptor = Object.getOwnPropertyDescriptor(
    Object.prototype,
    "toJSON",
  );
  const durable = JSON.parse(
    JSON.stringify(createPostgresDetachedRestorePlan(fixture())),
  );
  let calls = 0;
  try {
    Object.defineProperty(Object.prototype, "toJSON", {
      configurable: true,
      value() {
        calls += 1;
        throw new Error("inherited toJSON must not run during rehydration");
      },
    });
    const rehydrated = rehydratePostgresDetachedRestorePlan(durable);
    assert.equal(isPostgresDetachedRestorePlan(rehydrated), true);
  } finally {
    if (descriptor === undefined) delete Object.prototype.toJSON;
    else Object.defineProperty(Object.prototype, "toJSON", descriptor);
  }
  assert.equal(calls, 0);
});

test("request drift changes every subordinate identity despite a stable root operation ID", () => {
  const original = createPostgresDetachedRestorePlan(fixture());
  const driftedInput = cloneWithRequest({ leaseId: "restore-lease-002" });
  assert.equal(
    driftedInput.request.operationId,
    fixture().request.operationId,
  );
  const drifted = createPostgresDetachedRestorePlan(driftedInput);

  for (const key of DERIVED_ID_KEYS) assert.notEqual(drifted[key], original[key]);
  assert.notEqual(drifted.planSha256, original.planSha256);
  assert.equal(drifted.request.leaseId, "restore-lease-002");
});

test("capture timestamp and every stable plan field participate in identity", () => {
  const original = createPostgresDetachedRestorePlan(fixture());
  const changes = {
    captureCreatedAt: "2026-08-11T09:00:01.000Z",
    destinationDirectory:
      "/var/lib/portable-codex/restores/destination-002",
    destinationOwnedRoot: "/var/lib/portable-codex/restores-next",
    detachMode: "force-fence",
    holderId: "writer-holder-003",
    imagePlanId: "image-plan-002",
    leaseDurationMilliseconds: 600_001,
    sourceArtifactDirectory:
      "/var/lib/portable-codex/artifacts/source-002",
    sourceArtifactOwnedRoot: "/var/lib/portable-codex/artifacts-next",
  };

  for (const [key, replacement] of Object.entries(changes)) {
    const candidate = fixture();
    if (key === "sourceArtifactOwnedRoot") {
      candidate.plan.sourceArtifactOwnedRoot = replacement;
      candidate.plan.sourceArtifactDirectory = `${replacement}/source-001`;
    } else if (key === "destinationOwnedRoot") {
      candidate.plan.destinationOwnedRoot = replacement;
      candidate.plan.destinationDirectory = `${replacement}/destination-001`;
    } else {
      candidate.plan[key] = replacement;
    }
    const changed = createPostgresDetachedRestorePlan(candidate);
    assert.notEqual(changed.planSha256, original.planSha256, key);
    for (const identityKey of DERIVED_ID_KEYS) {
      assert.notEqual(changed[identityKey], original[identityKey], key);
    }
  }
});

test("options, request, target, and plan require exact plain data shapes", () => {
  assertPlanError(() => createPostgresDetachedRestorePlan());
  assertPlanError(() => createPostgresDetachedRestorePlan(fixture(), fixture()));
  assertPlanError(() => createPostgresDetachedRestorePlan(null));
  assertPlanError(() => createPostgresDetachedRestorePlan([]));
  assertPlanError(() => createPostgresDetachedRestorePlan(new Proxy(fixture(), {})));
  assertPlanError(() =>
    createPostgresDetachedRestorePlan(
      Object.assign(Object.create({ inherited: true }), fixture()),
    ),
  );

  const extraOption = fixture();
  extraOption.extra = true;
  assertPlanError(() => createPostgresDetachedRestorePlan(extraOption));
  const symbolOption = fixture();
  symbolOption[Symbol("extra")] = true;
  assertPlanError(() => createPostgresDetachedRestorePlan(symbolOption));

  const proxyRequest = fixture();
  proxyRequest.request = new Proxy(proxyRequest.request, {});
  assertPlanError(() => createPostgresDetachedRestorePlan(proxyRequest));
  const proxyTarget = fixture();
  proxyTarget.request.target = new Proxy(proxyTarget.request.target, {});
  assertPlanError(() => createPostgresDetachedRestorePlan(proxyTarget));
  const proxyPlan = fixture();
  proxyPlan.plan = new Proxy(proxyPlan.plan, {});
  assertPlanError(() => createPostgresDetachedRestorePlan(proxyPlan));

  for (const location of ["options", "request", "target", "plan"]) {
    const value = fixture();
    const parent =
      location === "options"
        ? value
        : location === "target"
          ? value.request.target
          : value[location];
    const key = Reflect.ownKeys(parent)[0];
    const current = parent[key];
    Object.defineProperty(parent, key, {
      enumerable: true,
      get() {
        assert.fail(`${location} accessor must not run`);
      },
    });
    assertPlanError(() => createPostgresDetachedRestorePlan(value));
    Object.defineProperty(parent, key, {
      configurable: true,
      enumerable: true,
      value: current,
    });
  }

  for (const location of ["request", "target", "plan"]) {
    const value = fixture();
    const parent =
      location === "target" ? value.request.target : value[location];
    parent.extra = true;
    assertPlanError(() => createPostgresDetachedRestorePlan(value));
  }
});

test("restore request enforces the exact storage mutation subset", () => {
  const invalidRequests = [
    { contractVersion: 2 },
    { operation: "checkpoint" },
    { sessionId: "not-a-uuid" },
    { sessionId: "018f8475-7c55-0a11-8a88-001122334455" },
    { sessionId: "018f8475-7c55-7a11-7a88-001122334455" },
    { fencingEpoch: "0" },
    { fencingEpoch: "01" },
    { fencingEpoch: "18446744073709551616" },
    { operationId: "contains/slash" },
    { holderId: "" },
    { storageId: "x".repeat(129) },
  ];
  for (const overrides of invalidRequests) {
    assertPlanError(() =>
      createPostgresDetachedRestorePlan(cloneWithRequest(overrides)),
    );
  }

  for (const overrides of [
    { kind: "attachment" },
    { artifactId: "" },
    { checkpointId: "contains/slash" },
  ]) {
    const value = fixture();
    Object.assign(value.request.target, overrides);
    assertPlanError(() => createPostgresDetachedRestorePlan(value));
  }
});

test("stable plan rejects invalid scalars and noncanonical capture timestamps", () => {
  const invalidPlans = [
    { detachMode: "automatic" },
    { holderId: "" },
    { imagePlanId: "contains/slash" },
    { leaseDurationMilliseconds: 0 },
    { leaseDurationMilliseconds: 1.5 },
    { leaseDurationMilliseconds: 86_400_001 },
    { leaseDurationMilliseconds: Number.POSITIVE_INFINITY },
    { captureCreatedAt: "2026-08-11T09:00:00Z" },
    { captureCreatedAt: "not-a-timestamp" },
    { captureCreatedAt: 0 },
  ];
  for (const overrides of invalidPlans) {
    assertPlanError(() =>
      createPostgresDetachedRestorePlan(cloneWithPlan(overrides)),
    );
  }
});

test("source artifact and destination plans require disjoint direct owned children", () => {
  const invalidPlans = [
    { sourceArtifactDirectory: "relative/source-001" },
    { sourceArtifactOwnedRoot: "/" },
    {
      sourceArtifactDirectory:
        "/var/lib/portable-codex/artifacts/nested/source-001",
    },
    {
      sourceArtifactDirectory:
        "/var/lib/portable-codex/artifacts/../artifacts/source-001",
    },
    { destinationDirectory: "relative/destination-001" },
    { destinationOwnedRoot: "/" },
    {
      destinationDirectory:
        "/var/lib/portable-codex/restores/nested/destination-001",
    },
    {
      destinationOwnedRoot: "/var/lib/portable-codex/artifacts",
      destinationDirectory:
        "/var/lib/portable-codex/artifacts/destination-001",
    },
    {
      destinationOwnedRoot:
        "/var/lib/portable-codex/artifacts/nested",
      destinationDirectory:
        "/var/lib/portable-codex/artifacts/nested/destination-001",
    },
    {
      sourceArtifactOwnedRoot: "/var/lib/portable-codex/restores/nested",
      sourceArtifactDirectory:
        "/var/lib/portable-codex/restores/nested/source-001",
    },
  ];
  for (const overrides of invalidPlans) {
    assertPlanError(() =>
      createPostgresDetachedRestorePlan(cloneWithPlan(overrides)),
    );
  }

  const siblingPrefix = cloneWithPlan({
    sourceArtifactDirectory: "/var/lib/portable-codex/art/source-001",
    sourceArtifactOwnedRoot: "/var/lib/portable-codex/art",
    destinationDirectory:
      "/var/lib/portable-codex/artifacts/destination-001",
    destinationOwnedRoot: "/var/lib/portable-codex/artifacts",
  });
  assert.equal(
    isPostgresDetachedRestorePlan(
      createPostgresDetachedRestorePlan(siblingPrefix),
    ),
    true,
  );
});

test("captured intrinsics preserve canonicalization after prototype poisoning", () => {
  const expected = JSON.stringify(createPostgresDetachedRestorePlan(fixture()));
  const descriptors = {
    arrayIncludes: Object.getOwnPropertyDescriptor(Array.prototype, "includes"),
    date: Object.getOwnPropertyDescriptor(globalThis, "Date"),
    dateParse: Object.getOwnPropertyDescriptor(Date, "parse"),
    dateToISOString: Object.getOwnPropertyDescriptor(
      Date.prototype,
      "toISOString",
    ),
    hashDigest: Object.getOwnPropertyDescriptor(Hash.prototype, "digest"),
    hashUpdate: Object.getOwnPropertyDescriptor(Hash.prototype, "update"),
    json: Object.getOwnPropertyDescriptor(globalThis, "JSON"),
    numberIsFinite: Object.getOwnPropertyDescriptor(Number, "isFinite"),
    numberIsSafeInteger: Object.getOwnPropertyDescriptor(
      Number,
      "isSafeInteger",
    ),
    objectGetOwnPropertyDescriptor: Object.getOwnPropertyDescriptor(
      Object,
      "getOwnPropertyDescriptor",
    ),
    objectGetPrototypeOf: Object.getOwnPropertyDescriptor(
      Object,
      "getPrototypeOf",
    ),
    objectHasOwn: Object.getOwnPropertyDescriptor(Object, "hasOwn"),
    reflectApply: Object.getOwnPropertyDescriptor(Reflect, "apply"),
    reflectOwnKeys: Object.getOwnPropertyDescriptor(Reflect, "ownKeys"),
    regexpExec: Object.getOwnPropertyDescriptor(RegExp.prototype, "exec"),
    stringCharCodeAt: Object.getOwnPropertyDescriptor(
      String.prototype,
      "charCodeAt",
    ),
    stringStartsWith: Object.getOwnPropertyDescriptor(
      String.prototype,
      "startsWith",
    ),
    weakSetAdd: Object.getOwnPropertyDescriptor(WeakSet.prototype, "add"),
    weakSetHas: Object.getOwnPropertyDescriptor(WeakSet.prototype, "has"),
  };
  const poison = function poison() {
    throw new Error("poisoned intrinsic");
  };
  let actual;

  try {
    Object.defineProperty(Array.prototype, "includes", {
      ...descriptors.arrayIncludes,
      value: poison,
    });
    Object.defineProperty(globalThis, "Date", {
      ...descriptors.date,
      value: poison,
    });
    Object.defineProperty(Date, "parse", {
      ...descriptors.dateParse,
      value: poison,
    });
    Object.defineProperty(Date.prototype, "toISOString", {
      ...descriptors.dateToISOString,
      value: poison,
    });
    Object.defineProperty(Hash.prototype, "digest", {
      ...descriptors.hashDigest,
      value: poison,
    });
    Object.defineProperty(Hash.prototype, "update", {
      ...descriptors.hashUpdate,
      value: poison,
    });
    Object.defineProperty(globalThis, "JSON", {
      ...descriptors.json,
      value: Object.freeze({ stringify: poison }),
    });
    Object.defineProperty(Number, "isFinite", {
      ...descriptors.numberIsFinite,
      value: poison,
    });
    Object.defineProperty(Number, "isSafeInteger", {
      ...descriptors.numberIsSafeInteger,
      value: poison,
    });
    Object.defineProperty(Object, "getOwnPropertyDescriptor", {
      ...descriptors.objectGetOwnPropertyDescriptor,
      value: poison,
    });
    Object.defineProperty(Object, "getPrototypeOf", {
      ...descriptors.objectGetPrototypeOf,
      value: poison,
    });
    Object.defineProperty(Object, "hasOwn", {
      ...descriptors.objectHasOwn,
      value: poison,
    });
    Object.defineProperty(Reflect, "apply", {
      ...descriptors.reflectApply,
      value: poison,
    });
    Object.defineProperty(Reflect, "ownKeys", {
      ...descriptors.reflectOwnKeys,
      value: poison,
    });
    Object.defineProperty(RegExp.prototype, "exec", {
      ...descriptors.regexpExec,
      value: poison,
    });
    Object.defineProperty(String.prototype, "charCodeAt", {
      ...descriptors.stringCharCodeAt,
      value: poison,
    });
    Object.defineProperty(String.prototype, "startsWith", {
      ...descriptors.stringStartsWith,
      value: poison,
    });
    Object.defineProperty(WeakSet.prototype, "add", {
      ...descriptors.weakSetAdd,
      value: poison,
    });
    Object.defineProperty(WeakSet.prototype, "has", {
      ...descriptors.weakSetHas,
      value: poison,
    });

    actual = createPostgresDetachedRestorePlan(fixture());
  } finally {
    Object.defineProperty(Array.prototype, "includes", descriptors.arrayIncludes);
    Object.defineProperty(globalThis, "Date", descriptors.date);
    Object.defineProperty(Date, "parse", descriptors.dateParse);
    Object.defineProperty(
      Date.prototype,
      "toISOString",
      descriptors.dateToISOString,
    );
    Object.defineProperty(Hash.prototype, "digest", descriptors.hashDigest);
    Object.defineProperty(Hash.prototype, "update", descriptors.hashUpdate);
    Object.defineProperty(globalThis, "JSON", descriptors.json);
    Object.defineProperty(Number, "isFinite", descriptors.numberIsFinite);
    Object.defineProperty(
      Number,
      "isSafeInteger",
      descriptors.numberIsSafeInteger,
    );
    Object.defineProperty(
      Object,
      "getOwnPropertyDescriptor",
      descriptors.objectGetOwnPropertyDescriptor,
    );
    Object.defineProperty(
      Object,
      "getPrototypeOf",
      descriptors.objectGetPrototypeOf,
    );
    Object.defineProperty(Object, "hasOwn", descriptors.objectHasOwn);
    Object.defineProperty(Reflect, "apply", descriptors.reflectApply);
    Object.defineProperty(Reflect, "ownKeys", descriptors.reflectOwnKeys);
    Object.defineProperty(RegExp.prototype, "exec", descriptors.regexpExec);
    Object.defineProperty(
      String.prototype,
      "charCodeAt",
      descriptors.stringCharCodeAt,
    );
    Object.defineProperty(
      String.prototype,
      "startsWith",
      descriptors.stringStartsWith,
    );
    Object.defineProperty(WeakSet.prototype, "add", descriptors.weakSetAdd);
    Object.defineProperty(WeakSet.prototype, "has", descriptors.weakSetHas);
  }

  assert.equal(JSON.stringify(actual), expected);
  assert.equal(isPostgresDetachedRestorePlan(actual), true);
});

test("error mapping does not expose a mutable has-instance hook", () => {
  assert.equal(Object.isFrozen(PostgresDetachedRestorePlanError), true);
  assert.throws(() =>
    Object.defineProperty(PostgresDetachedRestorePlanError, Symbol.hasInstance, {
      value() {
        throw new Error("escaped has-instance hook");
      },
    }),
  );
  assertPlanError(() => createPostgresDetachedRestorePlan(null));
});
