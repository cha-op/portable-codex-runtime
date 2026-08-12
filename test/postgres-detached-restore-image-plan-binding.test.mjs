import assert from "node:assert/strict";
import { createHash } from "node:crypto";
import test from "node:test";

import {
  POSTGRES_DETACHED_RESTORE_IMAGE_PLAN_BINDING_CONTRACT_VERSION,
  POSTGRES_DETACHED_RESTORE_IMAGE_PLAN_PROVIDER_CONTRACT_VERSION,
  PostgresDetachedRestoreImagePlanBindingError,
  createPostgresDetachedRestoreImagePlanBinding,
  isPostgresDetachedRestoreImagePlanBinding,
  isPostgresDetachedRestoreImagePlanReservation,
} from "../src/postgres-detached-restore-image-plan-binding.mjs";
import {
  createPostgresDetachedRestorePlan,
} from "../src/postgres-detached-restore-plan.mjs";
import { createSessionManifest } from "../src/session-storage-contracts.mjs";

const SESSION_ID = "019f8a00-0000-7000-8000-000000000001";
const OTHER_SESSION_ID = "019f8a00-0000-7000-8000-000000000002";
const THREAD_ID = "019f8a00-0000-7000-8000-000000000003";
const CODEX_VERSION = "codex-cli 0.144.1";
const OCI_MANIFEST_MEDIA_TYPE =
  "application/vnd.oci.image.manifest.v1+json";
const OCI_CONFIG_MEDIA_TYPE =
  "application/vnd.oci.image.config.v1+json";
const OCI_LAYER_MEDIA_TYPE =
  "application/vnd.oci.image.layer.v1.tar+gzip";

function digest(bytes) {
  return `sha256:${createHash("sha256").update(bytes).digest("hex")}`;
}

function jsonBytes(value) {
  return Buffer.from(JSON.stringify(value), "utf8");
}

function imageFixture() {
  const configBytes = jsonBytes({
    architecture: "arm64",
    config: { Env: ["PATH=/usr/local/bin:/usr/bin:/bin"] },
    os: "linux",
    rootfs: {
      diff_ids: [`sha256:${"d".repeat(64)}`],
      type: "layers",
    },
  });
  const descriptorBytes = jsonBytes({
    config: {
      digest: digest(configBytes),
      mediaType: OCI_CONFIG_MEDIA_TYPE,
      size: configBytes.byteLength,
    },
    layers: [
      {
        digest: `sha256:${"c".repeat(64)}`,
        mediaType: OCI_LAYER_MEDIA_TYPE,
        size: 1024,
      },
    ],
    mediaType: OCI_MANIFEST_MEDIA_TYPE,
    schemaVersion: 2,
  });
  return {
    configBytes,
    descriptor: Object.freeze({
      bytes: descriptorBytes,
      digest: digest(descriptorBytes),
      mediaType: OCI_MANIFEST_MEDIA_TYPE,
      size: descriptorBytes.byteLength,
    }),
  };
}

function sessionManifest(
  image = imageFixture(),
  { sessionId = SESSION_ID, imageDigest = image.descriptor.digest } = {},
) {
  return createSessionManifest({
    sessionId,
    codex: {
      ephemeral: false,
      historyMode: "paginated",
      rootThreadId: THREAD_ID,
      sessionId: THREAD_ID,
    },
    runtime: {
      codexSandbox: "danger-full-access",
      codexVersion: CODEX_VERSION,
      imageDigest,
      imageMediaType: OCI_MANIFEST_MEDIA_TYPE,
      platform: "linux/arm64",
    },
  });
}

function restorePlan({
  imagePlanId = "image-plan-binding-001",
  sessionId = SESSION_ID,
} = {}) {
  return createPostgresDetachedRestorePlan({
    request: {
      backendId: "binding-backend-001",
      contractVersion: 1,
      fencingEpoch: "42",
      holderId: "binding-holder-001",
      leaseId: "binding-lease-001",
      operation: "restore",
      operationId: "binding-restore-operation-001",
      sessionId,
      storageId: "binding-storage-001",
      target: {
        artifactId: "binding-artifact-001",
        checkpointId: "binding-checkpoint-001",
        kind: "checkpoint",
      },
    },
    plan: {
      captureCreatedAt: "2026-08-12T01:00:00.000Z",
      destinationDirectory:
        "/var/lib/portable-codex/restores/image-binding-001",
      destinationOwnedRoot: "/var/lib/portable-codex/restores",
      detachMode: "release",
      holderId: "binding-restored-writer-001",
      imagePlanId,
      leaseDurationMilliseconds: 600_000,
      sourceArtifactDirectory:
        "/var/lib/portable-codex/artifacts/image-binding-001",
      sourceArtifactOwnedRoot: "/var/lib/portable-codex/artifacts",
    },
  });
}

function runtimeMeasurement(overrides = {}) {
  return Object.freeze({
    codexBinaryPath: "/opt/portable-codex/bin/codex",
    codexBinarySha256: "b".repeat(64),
    codexVersion: CODEX_VERSION,
    ...overrides,
  });
}

function resolvedImage(image) {
  return Object.freeze({
    configBytes: image.configBytes,
    descriptor: image.descriptor,
  });
}

function provider({
  image = imageFixture(),
  imagePlanProviderId = "image-provider-binding-001",
  inspectCodex = async () => runtimeMeasurement(),
  resolveImagePlan = async () => resolvedImage(image),
} = {}) {
  return Object.freeze({
    contractVersion:
      POSTGRES_DETACHED_RESTORE_IMAGE_PLAN_PROVIDER_CONTRACT_VERSION,
    imagePlanProviderId,
    inspectCodex,
    resolveImagePlan,
  });
}

function prepareInput(image = imageFixture(), overrides = {}) {
  return {
    plan: restorePlan(),
    sessionManifest: sessionManifest(image),
    ...overrides,
  };
}

function assertBindingError(code) {
  return (error) => {
    assert(error instanceof PostgresDetachedRestoreImagePlanBindingError);
    assert.equal(error.code, code);
    assert.equal(error.retryable, false);
    assert.equal(Object.isFrozen(error), true);
    assert.equal(Object.hasOwn(error, "cause"), false);
    assert.equal(error.message.length < 160, true);
    return true;
  };
}

function assertProtectedPromise(value) {
  assert.equal(value instanceof Promise, true);
  for (const key of ["catch", "constructor", "finally", "then"]) {
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    assert.equal(descriptor?.configurable, false);
    assert.equal(descriptor?.writable, false);
  }
  return value;
}

test("constructs one exact frozen branded binding without provider I/O", () => {
  const calls = [];
  const binding = createPostgresDetachedRestoreImagePlanBinding(
    provider({
      inspectCodex: async () => {
        calls.push("inspect");
        throw new Error("must not inspect during construction");
      },
      resolveImagePlan: async () => {
        calls.push("resolve");
        throw new Error("must not resolve during construction");
      },
    }),
  );

  assert.equal(
    POSTGRES_DETACHED_RESTORE_IMAGE_PLAN_BINDING_CONTRACT_VERSION,
    1,
  );
  assert.equal(
    POSTGRES_DETACHED_RESTORE_IMAGE_PLAN_PROVIDER_CONTRACT_VERSION,
    1,
  );
  assert.deepEqual(Reflect.ownKeys(binding), [
    "consumeImageReservation",
    "contractVersion",
    "imagePlanProviderId",
    "prepareImageReservation",
    "revalidateImageReservation",
  ]);
  assert.equal(Object.getPrototypeOf(binding), null);
  assert.equal(Object.isFrozen(binding), true);
  assert.equal(
    binding.contractVersion,
    POSTGRES_DETACHED_RESTORE_IMAGE_PLAN_BINDING_CONTRACT_VERSION,
  );
  assert.equal(binding.imagePlanProviderId, "image-provider-binding-001");
  for (const key of [
    "consumeImageReservation",
    "prepareImageReservation",
    "revalidateImageReservation",
  ]) {
    assert.equal(typeof binding[key], "function");
    assert.equal(Object.isFrozen(binding[key]), true);
  }
  for (const hidden of [
    "configBytes",
    "coordinator",
    "descriptor",
    "inspectCodex",
    "provider",
    "reservations",
    "resolveImagePlan",
  ]) {
    assert.equal(hidden in binding, false);
  }
  assert.equal(isPostgresDetachedRestoreImagePlanBinding(binding), true);
  assert.equal(
    isPostgresDetachedRestoreImagePlanBinding(Object.freeze({ ...binding })),
    false,
  );
  assert.equal(isPostgresDetachedRestoreImagePlanBinding(new Proxy(binding, {})), false);
  assert.deepEqual(calls, []);
});

test("binds one authentic plan and manifest to exact resolver and inspector requests", async () => {
  const image = imageFixture();
  const plan = restorePlan({ imagePlanId: "image-plan-exact-001" });
  const manifest = sessionManifest(image);
  const resolverRequests = [];
  const inspectorRequests = [];
  const binding = createPostgresDetachedRestoreImagePlanBinding(
    provider({
      image,
      imagePlanProviderId: "image-provider-exact-001",
      async inspectCodex(request) {
        inspectorRequests.push(request);
        assert.deepEqual(Reflect.ownKeys(request), [
          "imagePlanId",
          "imagePlanProviderId",
          "inspection",
        ]);
        assert.equal(Object.getPrototypeOf(request), null);
        assert.equal(Object.isFrozen(request), true);
        assert.equal(request.imagePlanId, plan.imagePlanId);
        assert.equal(
          request.imagePlanProviderId,
          "image-provider-exact-001",
        );
        assert.deepEqual(
          [...Reflect.ownKeys(request.inspection)].sort(),
          ["platformImage", "codexSandbox", "codexVersion"].sort(),
        );
        assert.equal(Object.isFrozen(request.inspection), true);
        assert.equal(
          request.inspection.platformImage.digest,
          image.descriptor.digest,
        );
        return runtimeMeasurement();
      },
      async resolveImagePlan(request) {
        resolverRequests.push(request);
        assert.deepEqual(Reflect.ownKeys(request), [
          "imagePlanId",
          "imagePlanProviderId",
          "sessionManifest",
        ]);
        assert.equal(Object.getPrototypeOf(request), null);
        assert.equal(Object.isFrozen(request), true);
        assert.equal(request.imagePlanId, plan.imagePlanId);
        assert.equal(
          request.imagePlanProviderId,
          "image-provider-exact-001",
        );
        assert.deepEqual(request.sessionManifest, manifest);
        assert.notStrictEqual(request.sessionManifest, manifest);
        assert.equal("launchIntent" in request, false);
        assert.equal("plan" in request, false);
        return resolvedImage(image);
      },
    }),
  );

  const pending = binding.prepareImageReservation({
    plan,
    sessionManifest: manifest,
  });
  assertProtectedPromise(pending);
  const reservation = await pending;

  assert.equal(isPostgresDetachedRestoreImagePlanReservation(reservation), true);
  assert.equal(Object.getPrototypeOf(reservation), null);
  assert.equal(Object.isFrozen(reservation), true);
  assert.deepEqual(Reflect.ownKeys(reservation), []);
  assert.equal(JSON.stringify(reservation), "{}");
  for (const hidden of [
    "binding",
    "configBytes",
    "coordinator",
    "descriptor",
    "imagePlanId",
    "inspectCodex",
    "reservation",
    "runtimeIdentity",
    "sessionManifest",
  ]) {
    assert.equal(hidden in reservation, false);
  }
  assert.equal(resolverRequests.length, 1);
  assert.equal(inspectorRequests.length, 1);

  const revalidated = await binding.revalidateImageReservation(reservation);
  assert.equal(Object.getPrototypeOf(revalidated), null);
  assert.equal(
    Object.getPrototypeOf(revalidated.projection),
    Object.prototype,
  );
  assert.equal(
    Object.getPrototypeOf(revalidated.projection.platformImage),
    Object.prototype,
  );
  assert.equal(
    Object.getPrototypeOf(revalidated.projection.platformImage.config),
    Object.prototype,
  );
  assert.deepEqual(JSON.parse(JSON.stringify(revalidated.projection)), {
    codexSandbox: "danger-full-access",
    codexVersion: CODEX_VERSION,
    platformImage: {
      architecture: "arm64",
      config: {
        digest: digest(image.configBytes),
        mediaType: OCI_CONFIG_MEDIA_TYPE,
        size: image.configBytes.byteLength,
      },
      digest: image.descriptor.digest,
      mediaType: OCI_MANIFEST_MEDIA_TYPE,
      os: "linux",
      size: image.descriptor.size,
    },
  });
  assert.equal(
    Object.getPrototypeOf(revalidated.runtimeIdentity),
    Object.prototype,
  );
  assert.deepEqual(JSON.parse(JSON.stringify(revalidated.runtimeIdentity)), {
    codexBinaryPath: "/opt/portable-codex/bin/codex",
    codexBinarySha256: "b".repeat(64),
    codexVersion: CODEX_VERSION,
    platformImageDigest: image.descriptor.digest,
  });
  assert.equal(Object.isFrozen(revalidated), true);
  assert.equal(inspectorRequests.length, 2);

  const consumed = await binding.consumeImageReservation(reservation);
  assert.strictEqual(consumed.projection, revalidated.projection);
  assert.strictEqual(consumed.runtimeIdentity, revalidated.runtimeIdentity);
  assert.equal(inspectorRequests.length, 3);
  await assert.rejects(
    binding.consumeImageReservation(reservation),
    assertBindingError(
      "postgres_detached_restore_image_plan_reservation_rejected",
    ),
  );
  await assert.rejects(
    binding.revalidateImageReservation(reservation),
    assertBindingError(
      "postgres_detached_restore_image_plan_reservation_rejected",
    ),
  );
  assert.equal(inspectorRequests.length, 3);
});

test("copies resolver evidence and keeps later caller mutation outside the binding", async () => {
  const image = imageFixture();
  const originalConfig = Buffer.from(image.configBytes);
  const originalDescriptor = Buffer.from(image.descriptor.bytes);
  const binding = createPostgresDetachedRestoreImagePlanBinding(
    provider({ image }),
  );
  const reservation = await binding.prepareImageReservation(
    prepareInput(image),
  );
  image.configBytes.fill(0);
  image.descriptor.bytes.fill(0);

  const measured = await binding.consumeImageReservation(reservation);
  assert.equal(measured.projection.platformImage.config.digest, digest(originalConfig));
  assert.equal(measured.projection.platformImage.digest, digest(originalDescriptor));
});

test("accepts and snapshots ordinary exact inspector measurements", async () => {
  const image = imageFixture();
  const expectedMeasurement = {
    codexBinaryPath: "/opt/portable-codex/bin/codex",
    codexBinarySha256: "b".repeat(64),
    codexVersion: CODEX_VERSION,
  };
  const firstMeasurement = { ...expectedMeasurement };
  let inspections = 0;
  const binding = createPostgresDetachedRestoreImagePlanBinding(
    provider({
      image,
      async inspectCodex() {
        inspections += 1;
        return inspections === 1
          ? firstMeasurement
          : { ...expectedMeasurement };
      },
    }),
  );
  const reservation = await binding.prepareImageReservation(
    prepareInput(image),
  );

  firstMeasurement.codexVersion = "codex-cli caller-mutated";
  const revalidated = await binding.revalidateImageReservation(reservation);
  assert.equal(revalidated.runtimeIdentity.codexVersion, CODEX_VERSION);
  const consumed = await binding.consumeImageReservation(reservation);
  assert.equal(consumed.runtimeIdentity.codexVersion, CODEX_VERSION);
  assert.equal(inspections, 3);
});

test("rejects structural plans, crossed sessions, inexact requests, and hostile input", async (t) => {
  const image = imageFixture();
  const binding = createPostgresDetachedRestoreImagePlanBinding(
    provider({ image }),
  );
  const code = "invalid_postgres_detached_restore_image_plan_binding_request";
  const valid = prepareInput(image);
  const cases = [
    undefined,
    null,
    { ...valid, extra: true },
    { plan: JSON.parse(JSON.stringify(valid.plan)), sessionManifest: valid.sessionManifest },
    {
      plan: valid.plan,
      sessionManifest: sessionManifest(image, { sessionId: OTHER_SESSION_ID }),
    },
  ];
  for (let index = 0; index < cases.length; index += 1) {
    await t.test(`invalid case ${index}`, async () => {
      await assert.rejects(
        binding.prepareImageReservation(cases[index]),
        assertBindingError(code),
      );
    });
  }

  let getterCalls = 0;
  const accessor = { plan: valid.plan };
  Object.defineProperty(accessor, "sessionManifest", {
    enumerable: true,
    get() {
      getterCalls += 1;
      throw new Error("must not invoke request accessor");
    },
  });
  await assert.rejects(
    binding.prepareImageReservation(accessor),
    assertBindingError(code),
  );
  assert.equal(getterCalls, 0);

  let proxyTraps = 0;
  const proxy = new Proxy(valid, {
    getPrototypeOf() {
      proxyTraps += 1;
      throw new Error("must not inspect request proxy");
    },
    ownKeys() {
      proxyTraps += 1;
      throw new Error("must not enumerate request proxy");
    },
  });
  await assert.rejects(
    binding.prepareImageReservation(proxy),
    assertBindingError(code),
  );
  assert.equal(proxyTraps, 0);

  const hostilePrototype = new Proxy(Object.prototype, {
    get() {
      proxyTraps += 1;
      throw new Error("must not read hostile prototype");
    },
  });
  const inherited = Object.create(hostilePrototype);
  inherited.plan = valid.plan;
  inherited.sessionManifest = valid.sessionManifest;
  await assert.rejects(
    binding.prepareImageReservation(inherited),
    assertBindingError(code),
  );
  assert.equal(proxyTraps, 0);
});

test("opaque reservations are binding-local and structural or proxied values cannot forge them", async () => {
  const image = imageFixture();
  const sharedProvider = provider({ image });
  const first = createPostgresDetachedRestoreImagePlanBinding(sharedProvider);
  const second = createPostgresDetachedRestoreImagePlanBinding(sharedProvider);
  const reservation = await first.prepareImageReservation(prepareInput(image));

  assert.equal(isPostgresDetachedRestoreImagePlanReservation(reservation), true);
  await assert.rejects(
    second.revalidateImageReservation(reservation),
    assertBindingError(
      "postgres_detached_restore_image_plan_reservation_rejected",
    ),
  );
  await assert.rejects(
    first.revalidateImageReservation(Object.freeze(Object.create(null))),
    assertBindingError(
      "invalid_postgres_detached_restore_image_plan_binding_request",
    ),
  );
  await assert.rejects(
    first.revalidateImageReservation(new Proxy(reservation, {})),
    assertBindingError(
      "invalid_postgres_detached_restore_image_plan_binding_request",
    ),
  );
  assert.equal(
    isPostgresDetachedRestoreImagePlanReservation(
      Object.freeze(Object.create(null)),
    ),
    false,
  );
  assert.equal(
    isPostgresDetachedRestoreImagePlanReservation(new Proxy(reservation, {})),
    false,
  );
  await first.consumeImageReservation(reservation);
});

test("translates image mismatch and inspection uncertainty without leaking provider errors", async () => {
  const image = imageFixture();
  const mismatchedManifest = sessionManifest(image, {
    imageDigest: `sha256:${"e".repeat(64)}`,
  });
  const resolutionBinding = createPostgresDetachedRestoreImagePlanBinding(
    provider({ image }),
  );
  await assert.rejects(
    resolutionBinding.prepareImageReservation({
      plan: restorePlan(),
      sessionManifest: mismatchedManifest,
    }),
    assertBindingError(
      "postgres_detached_restore_image_plan_resolution_uncertain",
    ),
  );

  const privateProviderError = new Error("private inspector detail");
  const inspectionBinding = createPostgresDetachedRestoreImagePlanBinding(
    provider({
      image,
      inspectCodex: async () => {
        throw privateProviderError;
      },
    }),
  );
  let observed;
  try {
    await inspectionBinding.prepareImageReservation(prepareInput(image));
  } catch (error) {
    observed = error;
  }
  assertBindingError(
    "postgres_detached_restore_image_plan_inspection_uncertain",
  )(observed);
  assert.notEqual(observed, privateProviderError);
  assert.equal(observed.message.includes("private"), false);
});

test("measurement drift rejects and inspection uncertainty revokes an issued reservation", async () => {
  const image = imageFixture();
  let inspections = 0;
  const driftBinding = createPostgresDetachedRestoreImagePlanBinding(
    provider({
      image,
      inspectCodex: async () => {
        inspections += 1;
        return runtimeMeasurement({
          codexBinarySha256: (inspections === 1 ? "b" : "c").repeat(64),
        });
      },
    }),
  );
  const drifted = await driftBinding.prepareImageReservation(
    prepareInput(image),
  );
  await assert.rejects(
    driftBinding.revalidateImageReservation(drifted),
    assertBindingError(
      "postgres_detached_restore_image_plan_reservation_rejected",
    ),
  );
  assert.equal(inspections, 2);
  await assert.rejects(
    driftBinding.consumeImageReservation(drifted),
    assertBindingError(
      "postgres_detached_restore_image_plan_reservation_rejected",
    ),
  );
  assert.equal(inspections, 2);

  inspections = 0;
  const uncertainBinding = createPostgresDetachedRestoreImagePlanBinding(
    provider({
      image,
      inspectCodex: async () => {
        inspections += 1;
        if (inspections === 2) throw new Error("inspection unavailable");
        return runtimeMeasurement();
      },
    }),
  );
  const uncertain = await uncertainBinding.prepareImageReservation(
    prepareInput(image),
  );
  await assert.rejects(
    uncertainBinding.revalidateImageReservation(uncertain),
    assertBindingError(
      "postgres_detached_restore_image_plan_inspection_uncertain",
    ),
  );
  await assert.rejects(
    uncertainBinding.consumeImageReservation(uncertain),
    assertBindingError(
      "postgres_detached_restore_image_plan_reservation_rejected",
    ),
  );
  assert.equal(inspections, 2);
});

test("concurrent reservation use permanently revokes rather than resurrecting the capability", async (t) => {
  for (const secondMethod of [
    "revalidateImageReservation",
    "consumeImageReservation",
  ]) {
    await t.test(`revalidate plus ${secondMethod}`, async () => {
      const image = imageFixture();
      let inspections = 0;
      let inspectionEntered;
      let releaseInspection;
      const entered = new Promise((resolve) => {
        inspectionEntered = resolve;
      });
      const blocked = new Promise((resolve) => {
        releaseInspection = resolve;
      });
      const binding = createPostgresDetachedRestoreImagePlanBinding(
        provider({
          image,
          inspectCodex() {
            inspections += 1;
            if (inspections === 1) {
              return Promise.resolve(runtimeMeasurement());
            }
            inspectionEntered();
            return blocked;
          },
        }),
      );
      const reservation = await binding.prepareImageReservation(
        prepareInput(image),
      );
      const first = binding.revalidateImageReservation(reservation);
      await entered;
      await assert.rejects(
        binding[secondMethod](reservation),
        assertBindingError(
          "postgres_detached_restore_image_plan_reservation_rejected",
        ),
      );
      releaseInspection(runtimeMeasurement());
      await assert.rejects(
        first,
        assertBindingError(
          "postgres_detached_restore_image_plan_reservation_rejected",
        ),
      );
      for (const method of [
        "revalidateImageReservation",
        "consumeImageReservation",
      ]) {
        await assert.rejects(
          binding[method](reservation),
          assertBindingError(
            "postgres_detached_restore_image_plan_reservation_rejected",
          ),
        );
      }
      assert.equal(inspections, 2);
    });
  }
});

test("rejects unsafe provider shapes, accessors, proxies, generators, and hostile prototypes", () => {
  const image = imageFixture();
  const optionCode =
    "invalid_postgres_detached_restore_image_plan_binding_options";
  const valid = provider({ image });
  const cases = [
    undefined,
    { ...valid },
    Object.freeze({ ...valid, extra: true }),
    Object.freeze({ ...valid, contractVersion: 2 }),
    Object.freeze({ ...valid, imagePlanProviderId: "bad provider id" }),
    Object.freeze({
      ...valid,
      resolveImagePlan: new Proxy(valid.resolveImagePlan, {}),
    }),
    Object.freeze({
      ...valid,
      inspectCodex: function* inspectCodex() {
        yield runtimeMeasurement();
      },
    }),
  ];
  for (const value of cases) {
    assert.throws(
      () => createPostgresDetachedRestoreImagePlanBinding(value),
      assertBindingError(optionCode),
    );
  }

  let traps = 0;
  const proxied = new Proxy(valid, {
    ownKeys() {
      traps += 1;
      throw new Error("must not enumerate provider proxy");
    },
  });
  assert.throws(
    () => createPostgresDetachedRestoreImagePlanBinding(proxied),
    assertBindingError(optionCode),
  );
  assert.equal(traps, 0);

  let getterCalls = 0;
  const accessor = { ...valid };
  Object.defineProperty(accessor, "resolveImagePlan", {
    enumerable: true,
    get() {
      getterCalls += 1;
      throw new Error("must not invoke provider accessor");
    },
  });
  Object.freeze(accessor);
  assert.throws(
    () => createPostgresDetachedRestoreImagePlanBinding(accessor),
    assertBindingError(optionCode),
  );
  assert.equal(getterCalls, 0);

  const hostilePrototype = new Proxy(Object.prototype, {
    get() {
      traps += 1;
      throw new Error("must not read provider prototype");
    },
  });
  const inherited = Object.create(hostilePrototype);
  Object.assign(inherited, valid);
  Object.freeze(inherited);
  assert.throws(
    () => createPostgresDetachedRestoreImagePlanBinding(inherited),
    assertBindingError(optionCode),
  );
  assert.equal(traps, 0);
});

test("protects raw provider promises before synchronously polluted Promise prototypes can react", async (t) => {
  const constructorDescriptor = Object.getOwnPropertyDescriptor(
    Promise.prototype,
    "constructor",
  );
  const thenDescriptor = Object.getOwnPropertyDescriptor(
    Promise.prototype,
    "then",
  );
  for (const providerMethod of ["resolveImagePlan", "inspectCodex"]) {
    await t.test(providerMethod, async () => {
      const image = imageFixture();
      let poisonReads = 0;
      let restoreQueued = false;
      const restore = () => {
        Object.defineProperty(
          Promise.prototype,
          "constructor",
          constructorDescriptor,
        );
        Object.defineProperty(Promise.prototype, "then", thenDescriptor);
      };
      const poison = () => {
        Object.defineProperty(Promise.prototype, "constructor", {
          configurable: true,
          enumerable: false,
          get() {
            poisonReads += 1;
            throw new Error("polluted Promise constructor must not run");
          },
        });
        Object.defineProperty(Promise.prototype, "then", {
          configurable: true,
          enumerable: false,
          get() {
            poisonReads += 1;
            throw new Error("polluted Promise then must not run");
          },
        });
      };
      const overrides =
        providerMethod === "resolveImagePlan"
          ? {
              resolveImagePlan() {
                const pending = Promise.resolve(resolvedImage(image));
                poison();
                return pending;
              },
            }
          : {
              inspectCodex() {
                const pending = Promise.resolve(runtimeMeasurement());
                poison();
                restoreQueued = true;
                queueMicrotask(restore);
                return pending;
              },
            };
      const binding = createPostgresDetachedRestoreImagePlanBinding(
        provider({ image, ...overrides }),
      );
      let pending;
      try {
        pending = binding.prepareImageReservation(prepareInput(image));
        if (providerMethod === "resolveImagePlan") restore();
        const reservation = await pending;
        assert.equal(
          isPostgresDetachedRestoreImagePlanReservation(reservation),
          true,
        );
        await binding.consumeImageReservation(reservation);
      } finally {
        restore();
      }
      assert.equal(poisonReads, 0);
      assert.equal(
        restoreQueued,
        providerMethod === "inspectCodex",
      );
    });
  }
});

test("callback-time Object.prototype.then cannot forge prepare, revalidate, or consume results", async () => {
  const image = imageFixture();
  const thenDescriptor = Object.getOwnPropertyDescriptor(
    Object.prototype,
    "then",
  );
  let poisonEnabled = false;
  let poisonCalls = 0;
  let resolverCalls = 0;
  const forgedThen = Object.freeze(function forgedThen(resolve) {
    poisonCalls += 1;
    resolve("forged-image-plan-result");
  });
  const restore = () => {
    poisonEnabled = false;
    if (thenDescriptor === undefined) delete Object.prototype.then;
    else Object.defineProperty(Object.prototype, "then", thenDescriptor);
  };
  const poison = () => {
    poisonEnabled = true;
    Object.defineProperty(Object.prototype, "then", {
      configurable: true,
      enumerable: false,
      get() {
        if (!poisonEnabled || this === null || typeof this !== "object") {
          return undefined;
        }
        const keys = Reflect.ownKeys(this);
        const target =
          (keys.length === 2 &&
            keys.includes("configBytes") &&
            keys.includes("descriptor")) ||
          (keys.length === 3 &&
            keys.includes("codexBinaryPath") &&
            keys.includes("codexBinarySha256") &&
            keys.includes("codexVersion")) ||
          (keys.length === 3 &&
            keys.includes("projection") &&
            keys.includes("reservation") &&
            keys.includes("runtimeIdentity")) ||
          (keys.length === 2 &&
            keys.includes("projection") &&
            keys.includes("runtimeIdentity"));
        return target ? forgedThen : undefined;
      },
    });
  };
  const binding = createPostgresDetachedRestoreImagePlanBinding(
    provider({
      image,
      resolveImagePlan() {
        resolverCalls += 1;
        const pending = Promise.resolve(resolvedImage(image));
        poison();
        return pending;
      },
      inspectCodex() {
        // Isolate each provider callback boundary: a provider cannot safely
        // construct its own Promise while poison deliberately left by the
        // preceding resolver callback remains active.
        restore();
        const pending = Promise.resolve(runtimeMeasurement());
        poison();
        return pending;
      },
    }),
  );

  let reservation;
  try {
    reservation = await binding.prepareImageReservation(prepareInput(image));
  } finally {
    restore();
  }
  assert.equal(isPostgresDetachedRestoreImagePlanReservation(reservation), true);
  assert.equal(resolverCalls, 1);
  assert.equal(poisonCalls, 0);

  let revalidated;
  try {
    revalidated = await binding.revalidateImageReservation(reservation);
  } finally {
    restore();
  }
  assert.equal(Object.isFrozen(revalidated), true);
  assert.equal(revalidated.runtimeIdentity.codexVersion, CODEX_VERSION);
  assert.equal(resolverCalls, 1);
  assert.equal(poisonCalls, 0);

  let consumed;
  try {
    consumed = await binding.consumeImageReservation(reservation);
  } finally {
    restore();
  }
  assert.equal(Object.isFrozen(consumed), true);
  assert.equal(consumed.projection.platformImage.digest, image.descriptor.digest);
  assert.equal(resolverCalls, 1);
  assert.equal(poisonCalls, 0);
});

test("requires direct plain native Promise provider results", async (t) => {
  const image = imageFixture();
  const planInput = prepareInput(image);
  class PromiseSubclass extends Promise {}
  const unsafeResolverCases = [
    {
      name: "thenable",
      resolveImagePlan() {
        return Object.freeze({
          then() {
            throw new Error("thenable must not be assimilated");
          },
        });
      },
    },
    {
      name: "Promise subclass",
      resolveImagePlan() {
        return new PromiseSubclass((resolve) => resolve(resolvedImage(image)));
      },
    },
    {
      name: "own then",
      resolveImagePlan() {
        const pending = Promise.resolve(resolvedImage(image));
        Object.defineProperty(pending, "then", {
          configurable: true,
          value: Promise.prototype.then,
        });
        return pending;
      },
    },
    {
      name: "own then accessor",
      resolveImagePlan() {
        const pending = Promise.resolve(resolvedImage(image));
        Object.defineProperty(pending, "then", {
          configurable: true,
          get() {
            throw new Error("own then accessor must not run");
          },
        });
        return pending;
      },
    },
  ];
  for (const unsafe of unsafeResolverCases) {
    await t.test(`resolver ${unsafe.name}`, async () => {
      const binding = createPostgresDetachedRestoreImagePlanBinding(
        provider({ image, resolveImagePlan: unsafe.resolveImagePlan }),
      );
      await assert.rejects(
        binding.prepareImageReservation(planInput),
        assertBindingError(
          "postgres_detached_restore_image_plan_resolution_uncertain",
        ),
      );
    });
  }

  const unsafeInspectorCases = [
    {
      name: "thenable",
      inspectCodex() {
        return Object.freeze({ then: Object.freeze(() => undefined) });
      },
    },
    {
      name: "Promise subclass",
      inspectCodex() {
        return new PromiseSubclass((resolve) => resolve(runtimeMeasurement()));
      },
    },
  ];
  for (const unsafe of unsafeInspectorCases) {
    await t.test(`inspector ${unsafe.name}`, async () => {
      const binding = createPostgresDetachedRestoreImagePlanBinding(
        provider({ image, inspectCodex: unsafe.inspectCodex }),
      );
      await assert.rejects(
        binding.prepareImageReservation(planInput),
        assertBindingError(
          "postgres_detached_restore_image_plan_inspection_uncertain",
        ),
      );
    });
  }
});

test("rejects hostile resolver results without invoking accessors or proxy traps", async () => {
  const image = imageFixture();
  let traps = 0;
  const proxyResult = new Proxy(resolvedImage(image), {
    ownKeys() {
      traps += 1;
      throw new Error("must not enumerate resolver proxy");
    },
  });
  const proxyBinding = createPostgresDetachedRestoreImagePlanBinding(
    provider({
      image,
      resolveImagePlan() {
        return Promise.resolve(proxyResult);
      },
    }),
  );
  await assert.rejects(
    proxyBinding.prepareImageReservation(prepareInput(image)),
    assertBindingError(
      "postgres_detached_restore_image_plan_resolution_uncertain",
    ),
  );
  assert.equal(traps, 0);

  let getterCalls = 0;
  const accessorResult = { configBytes: image.configBytes };
  Object.defineProperty(accessorResult, "descriptor", {
    enumerable: true,
    get() {
      getterCalls += 1;
      throw new Error("must not invoke resolver-result accessor");
    },
  });
  Object.freeze(accessorResult);
  const accessorBinding = createPostgresDetachedRestoreImagePlanBinding(
    provider({
      image,
      resolveImagePlan() {
        return Promise.resolve(accessorResult);
      },
    }),
  );
  await assert.rejects(
    accessorBinding.prepareImageReservation(prepareInput(image)),
    assertBindingError(
      "postgres_detached_restore_image_plan_resolution_uncertain",
    ),
  );
  assert.equal(getterCalls, 0);

  const unfrozenBinding = createPostgresDetachedRestoreImagePlanBinding(
    provider({
      image,
      resolveImagePlan() {
        return Promise.resolve({
          configBytes: image.configBytes,
          descriptor: image.descriptor,
        });
      },
    }),
  );
  await assert.rejects(
    unfrozenBinding.prepareImageReservation(prepareInput(image)),
    assertBindingError(
      "postgres_detached_restore_image_plan_resolution_uncertain",
    ),
  );
});
