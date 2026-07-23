import assert from "node:assert/strict";
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import { fileURLToPath } from "node:url";
import test from "node:test";

import {
  MAX_IMAGE_CONFIG_BYTES,
  MAX_IMAGE_HISTORY_ENTRIES,
  MAX_IMAGE_JSON_ARRAY_ELEMENTS,
  MAX_IMAGE_JSON_CONTAINER_ENTRIES,
  MAX_IMAGE_JSON_OBJECT_MEMBERS,
  MAX_IMAGE_LAYER_COUNT,
  MAX_PLATFORM_MANIFEST_BYTES,
  PlatformImageReservationCoordinator,
  PlatformImageReservationError,
} from "../src/platform-image-reservation.mjs";
import { createSessionManifest } from "../src/session-storage-contracts.mjs";

const SESSION_ID = "019f3d80-0000-7000-8000-000000000001";
const THREAD_ID = "019f3d80-0000-7000-8000-000000000002";
const OCI_MANIFEST_MEDIA_TYPE =
  "application/vnd.oci.image.manifest.v1+json";
const OCI_CONFIG_MEDIA_TYPE =
  "application/vnd.oci.image.config.v1+json";
const DOCKER_MANIFEST_MEDIA_TYPE =
  "application/vnd.docker.distribution.manifest.v2+json";
const DOCKER_CONFIG_MEDIA_TYPE =
  "application/vnd.docker.container.image.v1+json";
const OCI_LAYER_MEDIA_TYPE =
  "application/vnd.oci.image.layer.v1.tar+gzip";
const DOCKER_LAYER_MEDIA_TYPE =
  "application/vnd.docker.image.rootfs.diff.tar.gzip";
const INDEX_MEDIA_TYPE = "application/vnd.oci.image.index.v1+json";
const CODEX_VERSION = "codex-cli 0.144.1";
const CODEX_BINARY_SHA256 = "b".repeat(64);
const LAYER_DIGEST = `sha256:${"c".repeat(64)}`;
const DIFF_ID = `sha256:${"d".repeat(64)}`;
const INTRINSIC_POISONING_FIXTURE = fileURLToPath(
  new URL(
    "./fixtures/platform-image-reservation-intrinsic-poisoning.mjs",
    import.meta.url,
  ),
);

function digest(bytes) {
  return `sha256:${createHash("sha256").update(bytes).digest("hex")}`;
}

function jsonBytes(value) {
  return Buffer.from(JSON.stringify(value), "utf8");
}

function createManifest(imageDigest, mediaType, platform, codexVersion = CODEX_VERSION) {
  return createSessionManifest({
    sessionId: SESSION_ID,
    codex: {
      rootThreadId: THREAD_ID,
      sessionId: THREAD_ID,
      ephemeral: false,
      historyMode: "paginated",
    },
    runtime: {
      imageDigest,
      imageMediaType: mediaType,
      platform,
      codexVersion,
      codexSandbox: "danger-full-access",
    },
  });
}

function imageFixture({
  architecture = "arm64",
  configDocument = undefined,
  configMediaType = OCI_CONFIG_MEDIA_TYPE,
  manifestMediaType = OCI_MANIFEST_MEDIA_TYPE,
  manifestMutator = undefined,
  os = "linux",
} = {}) {
  const configBytes = jsonBytes(
    configDocument ?? {
      architecture,
      config: {
        Env: ["PATH=/usr/local/bin:/usr/bin:/bin"],
      },
      os,
      rootfs: {
        type: "layers",
        diff_ids: [DIFF_ID],
      },
    },
  );
  const manifestDocument = {
    schemaVersion: 2,
    mediaType: manifestMediaType,
    config: {
      mediaType: configMediaType,
      digest: digest(configBytes),
      size: configBytes.byteLength,
    },
    layers: [
      {
        mediaType:
          manifestMediaType === OCI_MANIFEST_MEDIA_TYPE
            ? OCI_LAYER_MEDIA_TYPE
            : DOCKER_LAYER_MEDIA_TYPE,
        digest: LAYER_DIGEST,
        size: 1024,
      },
    ],
  };
  manifestMutator?.(manifestDocument, configBytes);
  const descriptorBytes = jsonBytes(manifestDocument);
  const descriptor = {
    bytes: descriptorBytes,
    digest: digest(descriptorBytes),
    mediaType: manifestMediaType,
    size: descriptorBytes.byteLength,
  };
  return {
    configBytes,
    descriptor,
    manifest: createManifest(
      descriptor.digest,
      descriptor.mediaType,
      `${os}/${architecture}`,
    ),
  };
}

function measurement(overrides = {}) {
  return {
    codexBinaryPath: "/opt/portable-codex/bin/codex",
    codexBinarySha256: CODEX_BINARY_SHA256,
    codexVersion: CODEX_VERSION,
    ...overrides,
  };
}

function inspector({ result = measurement(), throws = undefined } = {}) {
  const requests = [];
  const inspectCodex = async (request) => {
    requests.push(request);
    if (throws !== undefined) throw throws;
    return typeof result === "function" ? result(request) : result;
  };
  return { inspectCodex, requests };
}

function reserveOptions(fixture, inspectCodex, overrides = {}) {
  return {
    configBytes: fixture.configBytes,
    descriptor: fixture.descriptor,
    inspectCodex,
    sessionManifest: fixture.manifest,
    ...overrides,
  };
}

function useOptions(fixture, inspectCodex, reservation, overrides = {}) {
  return {
    configBytes: fixture.configBytes,
    descriptor: fixture.descriptor,
    inspectCodex,
    reservation,
    ...overrides,
  };
}

function assertCode(code) {
  return (error) =>
    error instanceof PlatformImageReservationError &&
    error.code === code &&
    error.message.length < 128 &&
    !Object.hasOwn(error, "cause");
}

test("reserves, revalidates, and consumes one exact OCI platform image", async () => {
  const fixture = imageFixture();
  const inspected = inspector();
  const coordinator = new PlatformImageReservationCoordinator();

  const reserved = await coordinator.reservePlatformImage(
    reserveOptions(fixture, inspected.inspectCodex),
  );
  assert.deepEqual(reserved.projection, {
    platformImage: {
      architecture: "arm64",
      config: {
        digest: digest(fixture.configBytes),
        mediaType: OCI_CONFIG_MEDIA_TYPE,
        size: fixture.configBytes.byteLength,
      },
      digest: fixture.descriptor.digest,
      mediaType: OCI_MANIFEST_MEDIA_TYPE,
      os: "linux",
      size: fixture.descriptor.size,
    },
    codexSandbox: "danger-full-access",
    codexVersion: CODEX_VERSION,
  });
  assert.deepEqual(reserved.runtimeIdentity, {
    codexBinaryPath: "/opt/portable-codex/bin/codex",
    codexBinarySha256: CODEX_BINARY_SHA256,
    codexVersion: CODEX_VERSION,
    platformImageDigest: fixture.descriptor.digest,
  });
  assert(Object.isFrozen(reserved));
  assert(Object.isFrozen(reserved.projection));
  assert(Object.isFrozen(reserved.projection.platformImage.config));
  assert(Object.isFrozen(reserved.runtimeIdentity));
  assert.equal(Object.getPrototypeOf(reserved.reservation), null);
  assert.deepEqual(Reflect.ownKeys(reserved.reservation), []);
  assert(Object.isFrozen(reserved.reservation));
  assert.equal(inspected.requests.length, 1);
  assert(Object.isFrozen(inspected.requests[0]));
  assert.strictEqual(
    inspected.requests[0].platformImage,
    reserved.projection.platformImage,
  );

  const revalidated = await coordinator.revalidateReservation(
    useOptions(fixture, inspected.inspectCodex, reserved.reservation),
  );
  assert.strictEqual(revalidated.projection, reserved.projection);
  assert.strictEqual(revalidated.runtimeIdentity, reserved.runtimeIdentity);
  assert.equal(inspected.requests.length, 2);

  const consumed = await coordinator.consumeReservation(
    useOptions(fixture, inspected.inspectCodex, reserved.reservation),
  );
  assert.strictEqual(consumed.projection, reserved.projection);
  assert.strictEqual(consumed.runtimeIdentity, reserved.runtimeIdentity);
  assert.equal(inspected.requests.length, 3);

  await assert.rejects(
    coordinator.consumeReservation(
      useOptions(fixture, inspected.inspectCodex, reserved.reservation),
    ),
    assertCode("platform_image_reservation_rejected"),
  );
  await assert.rejects(
    coordinator.revalidateReservation(
      useOptions(fixture, inspected.inspectCodex, reserved.reservation),
    ),
    assertCode("platform_image_reservation_rejected"),
  );
  assert.equal(inspected.requests.length, 3);
});

test("accepts the matching Docker schema-2 platform-manifest pair", async () => {
  const fixture = imageFixture({
    configMediaType: DOCKER_CONFIG_MEDIA_TYPE,
    manifestMediaType: DOCKER_MANIFEST_MEDIA_TYPE,
  });
  const inspected = inspector();
  const reserved = await new PlatformImageReservationCoordinator()
    .reservePlatformImage(reserveOptions(fixture, inspected.inspectCodex));
  assert.equal(
    reserved.projection.platformImage.mediaType,
    DOCKER_MANIFEST_MEDIA_TYPE,
  );
  assert.equal(
    reserved.projection.platformImage.config.mediaType,
    DOCKER_CONFIG_MEDIA_TYPE,
  );
});

test("accepts standard OCI optional manifest and descriptor metadata", async () => {
  const fixture = imageFixture({
    manifestMutator(document, configBytes) {
      delete document.mediaType;
      document.annotations = {
        "org.opencontainers.image.source": "https://example.invalid/source",
      };
      document.config.urls = ["https://example.invalid/config"];
      document.config.annotations = {
        "org.example.fixture": "config",
      };
      document.config.data = configBytes.toString("base64");
    },
  });
  const inspected = inspector();
  const reserved =
    await new PlatformImageReservationCoordinator().reservePlatformImage(
      reserveOptions(fixture, inspected.inspectCodex),
    );
  assert.equal(
    reserved.projection.platformImage.digest,
    fixture.descriptor.digest,
  );
  assert.equal(inspected.requests.length, 1);
});

test("rejects descriptor metadata outside the bounded profile", async (t) => {
  for (const [name, mutate] of [
    [
      "invalid artifact media type",
      (document) => {
        document.config.artifactType = "application/x-";
      },
    ],
    [
      "overlong artifact media subtype",
      (document) => {
        document.config.artifactType = `application/${"x".repeat(128)}`;
      },
    ],
    [
      "malformed absolute URL",
      (document) => {
        document.config.urls = ["https://[::1"];
      },
    ],
    [
      "credential-bearing URL",
      (document) => {
        document.config.urls = [
          "https://user:password@example.invalid/config",
        ];
      },
    ],
    [
      "empty URL fragment",
      (document) => {
        document.config.urls = ["https://example.invalid/config#"];
      },
    ],
    [
      "non-HTTPS URL",
      (document) => {
        document.config.urls = ["http://example.invalid/config"];
      },
    ],
  ]) {
    await t.test(name, async () => {
      const fixture = imageFixture({ manifestMutator: mutate });
      const inspected = inspector();
      await assert.rejects(
        new PlatformImageReservationCoordinator().reservePlatformImage(
          reserveOptions(fixture, inspected.inspectCodex),
        ),
        assertCode("platform_image_identity_mismatch"),
      );
      assert.equal(inspected.requests.length, 0);
    });
  }
});

test("rejects malformed runnable-image descriptors and config rootfs", async (t) => {
  const scenarios = [
    {
      name: "empty layer list",
      fixture: imageFixture({
        manifestMutator(document) {
          document.layers = [];
        },
      }),
    },
    {
      name: "null layer descriptor",
      fixture: imageFixture({
        manifestMutator(document) {
          document.layers[0] = null;
        },
      }),
    },
    {
      name: "layer descriptor without digest",
      fixture: imageFixture({
        manifestMutator(document) {
          delete document.layers[0].digest;
        },
      }),
    },
    {
      name: "missing rootfs",
      fixture: imageFixture({
        configDocument: {
          architecture: "arm64",
          os: "linux",
        },
      }),
    },
    {
      name: "layer and diff-id count mismatch",
      fixture: imageFixture({
        configDocument: {
          architecture: "arm64",
          os: "linux",
          rootfs: {
            type: "layers",
            diff_ids: [],
          },
        },
      }),
    },
  ];
  for (const scenario of scenarios) {
    await t.test(scenario.name, async () => {
      const inspected = inspector();
      await assert.rejects(
        new PlatformImageReservationCoordinator().reservePlatformImage(
          reserveOptions(scenario.fixture, inspected.inspectCodex),
        ),
        assertCode("platform_image_identity_mismatch"),
      );
      assert.equal(inspected.requests.length, 0);
    });
  }
});

test("rejects tags, descriptor digest drift, size drift, MIME drift, and indexes", async (t) => {
  const fixture = imageFixture();
  const cases = [
    {
      name: "tag in place of digest",
      code: "invalid_platform_image_request",
      descriptor: { ...fixture.descriptor, digest: "registry.example/image:latest" },
    },
    {
      name: "descriptor digest drift",
      code: "platform_image_identity_mismatch",
      descriptor: {
        ...fixture.descriptor,
        digest: `sha256:${"c".repeat(64)}`,
      },
    },
    {
      name: "descriptor size drift",
      code: "platform_image_identity_mismatch",
      descriptor: {
        ...fixture.descriptor,
        size: fixture.descriptor.size + 1,
      },
    },
    {
      name: "descriptor MIME drift",
      code: "platform_image_identity_mismatch",
      descriptor: {
        ...fixture.descriptor,
        mediaType: DOCKER_MANIFEST_MEDIA_TYPE,
      },
    },
    {
      name: "OCI index",
      code: "platform_image_identity_mismatch",
      descriptor: {
        ...fixture.descriptor,
        mediaType: INDEX_MEDIA_TYPE,
      },
    },
  ];
  for (const scenario of cases) {
    await t.test(scenario.name, async () => {
      const inspected = inspector();
      await assert.rejects(
        new PlatformImageReservationCoordinator().reservePlatformImage(
          reserveOptions(fixture, inspected.inspectCodex, {
            descriptor: scenario.descriptor,
          }),
        ),
        assertCode(scenario.code),
      );
      assert.equal(inspected.requests.length, 0);
    });
  }
});

test("rejects oversized byte views before allocating or copying", async () => {
  const nativeBufferAllocUnsafe = Buffer.allocUnsafe;
  const nativeBufferFrom = Buffer.from;
  const allocations = [];
  let copies = 0;
  Buffer.allocUnsafe = function monitoredBufferAllocUnsafe(size) {
    allocations.push(size);
    return Reflect.apply(nativeBufferAllocUnsafe, Buffer, [size]);
  };
  Buffer.from = function monitoredBufferFrom(...args) {
    copies += 1;
    return Reflect.apply(nativeBufferFrom, Buffer, args);
  };

  let FreshCoordinator;
  try {
    ({ PlatformImageReservationCoordinator: FreshCoordinator } =
      await import(
        "../src/platform-image-reservation.mjs?bounded-copy-preflight"
      ));
  } finally {
    Buffer.allocUnsafe = nativeBufferAllocUnsafe;
    Buffer.from = nativeBufferFrom;
  }
  allocations.length = 0;
  copies = 0;

  const fixture = imageFixture();
  const inspected = inspector();
  const oversizedManifest = new Uint8Array(
    MAX_PLATFORM_MANIFEST_BYTES + 1,
  );
  await assert.rejects(
    new FreshCoordinator().reservePlatformImage(
      reserveOptions(fixture, inspected.inspectCodex, {
        descriptor: {
          ...fixture.descriptor,
          bytes: oversizedManifest,
          size: 1,
        },
      }),
    ),
    (error) =>
      error?.name === "PlatformImageReservationError" &&
      error.code === "invalid_platform_image_request",
  );
  assert.deepEqual(allocations, []);
  assert.equal(copies, 0);

  const oversizedConfig = Buffer.alloc(MAX_IMAGE_CONFIG_BYTES + 1);
  await assert.rejects(
    new FreshCoordinator().reservePlatformImage(
      reserveOptions(fixture, inspected.inspectCodex, {
        configBytes: oversizedConfig,
      }),
    ),
    (error) =>
      error?.name === "PlatformImageReservationError" &&
      error.code === "invalid_platform_image_request",
  );
  assert.deepEqual(allocations, [fixture.descriptor.bytes.byteLength]);
  assert.equal(copies, 0);
  assert.equal(inspected.requests.length, 0);
});

test("rejects structurally expansive image JSON within byte limits", async (t) => {
  const rejectFixture = async (fixture) => {
    const inspected = inspector();
    assert.ok(
      fixture.descriptor.bytes.byteLength <=
        MAX_PLATFORM_MANIFEST_BYTES,
    );
    assert.ok(
      fixture.configBytes.byteLength <= MAX_IMAGE_CONFIG_BYTES,
    );
    await assert.rejects(
      new PlatformImageReservationCoordinator().reservePlatformImage(
        reserveOptions(fixture, inspected.inspectCodex),
      ),
      assertCode("platform_image_identity_mismatch"),
    );
    assert.equal(inspected.requests.length, 0);
  };

  await t.test("one object exceeds its member budget", async () => {
    const extra = Object.fromEntries(
      Array.from(
        { length: MAX_IMAGE_JSON_CONTAINER_ENTRIES + 1 },
        (_, index) => [`key-${index}`, null],
      ),
    );
    await rejectFixture(
      imageFixture({
        configDocument: {
          architecture: "arm64",
          extra,
          os: "linux",
          rootfs: { diff_ids: [DIFF_ID], type: "layers" },
        },
      }),
    );
  });

  await t.test("aggregate object members exceed their budget", async () => {
    const groups = Math.floor(
      MAX_IMAGE_JSON_OBJECT_MEMBERS /
        MAX_IMAGE_JSON_CONTAINER_ENTRIES,
    ) + 1;
    const extra = Array.from({ length: groups }, (_, group) =>
      Object.fromEntries(
        Array.from(
          { length: MAX_IMAGE_JSON_CONTAINER_ENTRIES },
          (_, index) => [`key-${group}-${index}`, null],
        ),
      ),
    );
    await rejectFixture(
      imageFixture({
        configDocument: {
          architecture: "arm64",
          extra,
          os: "linux",
          rootfs: { diff_ids: [DIFF_ID], type: "layers" },
        },
      }),
    );
  });

  await t.test("aggregate array elements exceed their budget", async () => {
    const groups = Math.floor(
      MAX_IMAGE_JSON_ARRAY_ELEMENTS /
        MAX_IMAGE_JSON_CONTAINER_ENTRIES,
    ) + 1;
    const extra = Array.from({ length: groups }, () =>
      Array(MAX_IMAGE_JSON_CONTAINER_ENTRIES).fill(null),
    );
    await rejectFixture(
      imageFixture({
        configDocument: {
          architecture: "arm64",
          extra,
          os: "linux",
          rootfs: { diff_ids: [DIFF_ID], type: "layers" },
        },
      }),
    );
  });

  await t.test("aggregate nodes exceed their budget", async () => {
    const groupSize = MAX_IMAGE_JSON_CONTAINER_ENTRIES - 1;
    const groups = Math.floor(
      MAX_IMAGE_JSON_ARRAY_ELEMENTS /
        MAX_IMAGE_JSON_CONTAINER_ENTRIES,
    );
    const extra = Array.from({ length: groups }, (_, index) =>
      Array(index === groups - 1 ? groupSize - 1 : groupSize).fill({
        value: null,
      }),
    );
    await rejectFixture(
      imageFixture({
        configDocument: {
          architecture: "arm64",
          extra,
          marker: null,
          markerTwo: null,
          markerThree: null,
          os: "linux",
          rootfs: { diff_ids: [DIFF_ID], type: "layers" },
        },
      }),
    );
  });

  await t.test("layer count exceeds its pre-parse budget", async () => {
    await rejectFixture(
      imageFixture({
        manifestMutator(document) {
          document.layers = Array(MAX_IMAGE_LAYER_COUNT + 1).fill(
            document.layers[0],
          );
        },
      }),
    );
  });

  await t.test("history count exceeds its pre-parse budget", async () => {
    await rejectFixture(
      imageFixture({
        configDocument: {
          architecture: "arm64",
          history: Array(MAX_IMAGE_HISTORY_ENTRIES + 1).fill({
            created: "2026-07-23T00:00:00Z",
          }),
          os: "linux",
          rootfs: { diff_ids: [DIFF_ID], type: "layers" },
        },
      }),
    );
  });
});

test("applies image array budgets only to authoritative paths", async () => {
  const extensionEntries = MAX_IMAGE_LAYER_COUNT + 1;
  const fixture = imageFixture({
    configDocument: {
      architecture: "arm64",
      extension: {
        diff_ids: Array(extensionEntries).fill(null),
        history: Array(extensionEntries).fill(null),
      },
      layers: Array(extensionEntries).fill(null),
      os: "linux",
      rootfs: { diff_ids: [DIFF_ID], type: "layers" },
    },
  });
  const inspected = inspector();

  await new PlatformImageReservationCoordinator().reservePlatformImage(
    reserveOptions(fixture, inspected.inspectCodex),
  );

  assert.equal(inspected.requests.length, 1);
});

test("copies genuine byte views without invoking shadowable properties", async () => {
  const fixture = imageFixture();
  const inspected = inspector();
  let trapCalls = 0;
  const trap = () => {
    trapCalls += 1;
    throw new Error("byte-view property trap must not run");
  };
  const shadowProperties = (view) => {
    for (const key of [
      "buffer",
      "byteLength",
      "byteOffset",
      "constructor",
      "length",
      "valueOf",
    ]) {
      Object.defineProperty(view, key, {
        configurable: true,
        get: trap,
      });
    }
    return view;
  };
  const descriptorBytes = shadowProperties(
    new Uint8Array(fixture.descriptor.bytes),
  );
  const configBytes = shadowProperties(
    new Uint8Array(fixture.configBytes),
  );

  await new PlatformImageReservationCoordinator().reservePlatformImage(
    reserveOptions(fixture, inspected.inspectCodex, {
      configBytes,
      descriptor: {
        ...fixture.descriptor,
        bytes: descriptorBytes,
      },
    }),
  );

  assert.equal(trapCalls, 0);
  assert.equal(inspected.requests.length, 1);
});

test("rejects a manifest document that claims an index or a different MIME", async (t) => {
  for (const [name, mutate] of [
    [
      "index shape",
      (document) => {
        document.manifests = [];
      },
    ],
    [
      "index media type",
      (document) => {
        document.mediaType = INDEX_MEDIA_TYPE;
      },
    ],
    [
      "different platform-manifest MIME",
      (document) => {
        document.mediaType = DOCKER_MANIFEST_MEDIA_TYPE;
      },
    ],
  ]) {
    await t.test(name, async () => {
      const fixture = imageFixture({ manifestMutator: mutate });
      const inspected = inspector();
      await assert.rejects(
        new PlatformImageReservationCoordinator().reservePlatformImage(
          reserveOptions(fixture, inspected.inspectCodex),
        ),
        assertCode("platform_image_identity_mismatch"),
      );
      assert.equal(inspected.requests.length, 0);
    });
  }
});

test("authenticates config descriptor digest, size, media type, bytes, and platform", async (t) => {
  const mutations = [
    {
      name: "config digest",
      mutate(document) {
        document.config.digest = `sha256:${"d".repeat(64)}`;
      },
    },
    {
      name: "config size",
      mutate(document) {
        document.config.size += 1;
      },
    },
    {
      name: "config media type",
      mutate(document) {
        document.config.mediaType = DOCKER_CONFIG_MEDIA_TYPE;
      },
    },
  ];
  for (const scenario of mutations) {
    await t.test(scenario.name, async () => {
      const fixture = imageFixture({ manifestMutator: scenario.mutate });
      const inspected = inspector();
      await assert.rejects(
        new PlatformImageReservationCoordinator().reservePlatformImage(
          reserveOptions(fixture, inspected.inspectCodex),
        ),
        assertCode("platform_image_identity_mismatch"),
      );
      assert.equal(inspected.requests.length, 0);
    });
  }

  await t.test("different config bytes", async () => {
    const fixture = imageFixture();
    const inspected = inspector();
    await assert.rejects(
      new PlatformImageReservationCoordinator().reservePlatformImage(
        reserveOptions(fixture, inspected.inspectCodex, {
          configBytes: jsonBytes({ architecture: "arm64", os: "linux" }),
        }),
      ),
      assertCode("platform_image_identity_mismatch"),
    );
    assert.equal(inspected.requests.length, 0);
  });

  for (const [name, configDocument] of [
    ["operating system", { architecture: "arm64", os: "windows" }],
    ["architecture", { architecture: "amd64", os: "linux" }],
  ]) {
    await t.test(name, async () => {
      const fixture = imageFixture({ configDocument });
      const inspected = inspector();
      const manifest = createManifest(
        fixture.descriptor.digest,
        fixture.descriptor.mediaType,
        "linux/arm64",
      );
      await assert.rejects(
        new PlatformImageReservationCoordinator().reservePlatformImage(
          reserveOptions(fixture, inspected.inspectCodex, {
            sessionManifest: manifest,
          }),
        ),
        assertCode("platform_image_identity_mismatch"),
      );
      assert.equal(inspected.requests.length, 0);
    });
  }
});

test("requires exact session manifest image, Codex version, and sandbox", async (t) => {
  const fixture = imageFixture();
  const inspected = inspector();
  for (const [name, runtime] of [
    [
      "image digest",
      {
        ...fixture.manifest.runtime,
        imageDigest: `sha256:${"e".repeat(64)}`,
      },
    ],
    [
      "image media type",
      {
        ...fixture.manifest.runtime,
        imageMediaType: DOCKER_MANIFEST_MEDIA_TYPE,
      },
    ],
    [
      "platform",
      {
        ...fixture.manifest.runtime,
        platform: "linux/amd64",
      },
    ],
  ]) {
    await t.test(name, async () => {
      await assert.rejects(
        new PlatformImageReservationCoordinator().reservePlatformImage(
          reserveOptions(fixture, inspected.inspectCodex, {
            sessionManifest: {
              ...fixture.manifest,
              runtime,
            },
          }),
        ),
        assertCode("platform_image_identity_mismatch"),
      );
    });
  }

  await t.test("Codex version", async () => {
    const wrongVersion = inspector({
      result: measurement({ codexVersion: "codex-cli 9.9.9" }),
    });
    await assert.rejects(
      new PlatformImageReservationCoordinator().reservePlatformImage(
        reserveOptions(fixture, wrongVersion.inspectCodex),
      ),
      assertCode("platform_image_identity_mismatch"),
    );
  });

  await t.test("Codex sandbox", async () => {
    await assert.rejects(
      new PlatformImageReservationCoordinator().reservePlatformImage(
        reserveOptions(fixture, inspected.inspectCodex, {
          sessionManifest: {
            ...fixture.manifest,
            runtime: {
              ...fixture.manifest.runtime,
              codexSandbox: "workspace-write",
            },
          },
        }),
      ),
      assertCode("invalid_platform_image_request"),
    );
  });
});

test("rejects malformed or uncertain executable inspection without leaking details", async (t) => {
  const fixture = imageFixture();
  const secretPath = "/private/company/secret/codex";
  const scenarios = [
    {
      name: "inspector throws",
      inspected: inspector({ throws: new Error(`failed at ${secretPath}`) }),
    },
    {
      name: "invalid binary hash",
      inspected: inspector({
        result: measurement({ codexBinarySha256: "not-a-sha256" }),
      }),
    },
    {
      name: "relative binary path",
      inspected: inspector({
        result: measurement({ codexBinaryPath: "bin/codex" }),
      }),
    },
    {
      name: "noncanonical binary path",
      inspected: inspector({
        result: measurement({ codexBinaryPath: "/opt/../bin/codex" }),
      }),
    },
    {
      name: "extra measurement field",
      inspected: inspector({
        result: { ...measurement(), trusted: true },
      }),
    },
  ];
  for (const scenario of scenarios) {
    await t.test(scenario.name, async () => {
      let captured;
      try {
        await new PlatformImageReservationCoordinator().reservePlatformImage(
          reserveOptions(fixture, scenario.inspected.inspectCodex),
        );
        assert.fail("inspection must fail");
      } catch (error) {
        captured = error;
      }
      assertCode("platform_image_inspection_uncertain")(captured);
      assert.equal(String(captured).includes(secretPath), false);
      assert.equal(captured.stack.includes(secretPath), false);
    });
  }
});

test("structural copies, wrappers, foreign coordinators, and replacement inspectors fail", async (t) => {
  await t.test("structured clone", async () => {
    const fixture = imageFixture();
    const inspected = inspector();
    const coordinator = new PlatformImageReservationCoordinator();
    const reserved = await coordinator.reservePlatformImage(
      reserveOptions(fixture, inspected.inspectCodex),
    );
    const copied = structuredClone(reserved.reservation);
    await assert.rejects(
      coordinator.consumeReservation(
        useOptions(fixture, inspected.inspectCodex, copied),
      ),
      assertCode("platform_image_reservation_rejected"),
    );
    await coordinator.consumeReservation(
      useOptions(fixture, inspected.inspectCodex, reserved.reservation),
    );
  });

  await t.test("JSON copy", async () => {
    const fixture = imageFixture();
    const inspected = inspector();
    const coordinator = new PlatformImageReservationCoordinator();
    const reserved = await coordinator.reservePlatformImage(
      reserveOptions(fixture, inspected.inspectCodex),
    );
    const copied = JSON.parse(JSON.stringify(reserved.reservation));
    await assert.rejects(
      coordinator.consumeReservation(
        useOptions(fixture, inspected.inspectCodex, copied),
      ),
      assertCode("platform_image_reservation_rejected"),
    );
  });

  await t.test("proxy wrapper does not execute traps", async () => {
    const fixture = imageFixture();
    const inspected = inspector();
    const coordinator = new PlatformImageReservationCoordinator();
    const reserved = await coordinator.reservePlatformImage(
      reserveOptions(fixture, inspected.inspectCodex),
    );
    let traps = 0;
    const wrapped = new Proxy(reserved.reservation, {
      get() {
        traps += 1;
        throw new Error("opaque capability trap");
      },
      getPrototypeOf() {
        traps += 1;
        throw new Error("opaque capability trap");
      },
      ownKeys() {
        traps += 1;
        throw new Error("opaque capability trap");
      },
    });
    await assert.rejects(
      coordinator.consumeReservation(
        useOptions(fixture, inspected.inspectCodex, wrapped),
      ),
      assertCode("platform_image_reservation_rejected"),
    );
    assert.equal(traps, 0);
  });

  await t.test("foreign coordinator", async () => {
    const fixture = imageFixture();
    const inspected = inspector();
    const first = new PlatformImageReservationCoordinator();
    const second = new PlatformImageReservationCoordinator();
    const reserved = await first.reservePlatformImage(
      reserveOptions(fixture, inspected.inspectCodex),
    );
    await assert.rejects(
      second.consumeReservation(
        useOptions(fixture, inspected.inspectCodex, reserved.reservation),
      ),
      assertCode("platform_image_reservation_rejected"),
    );
  });

  await t.test("replacement inspector revokes the reservation", async () => {
    const fixture = imageFixture();
    const original = inspector();
    const replacement = inspector();
    const coordinator = new PlatformImageReservationCoordinator();
    const reserved = await coordinator.reservePlatformImage(
      reserveOptions(fixture, original.inspectCodex),
    );
    await assert.rejects(
      coordinator.consumeReservation(
        useOptions(fixture, replacement.inspectCodex, reserved.reservation),
      ),
      assertCode("platform_image_reservation_rejected"),
    );
    assert.equal(replacement.requests.length, 0);
    await assert.rejects(
      coordinator.consumeReservation(
        useOptions(fixture, original.inspectCodex, reserved.reservation),
      ),
      assertCode("platform_image_reservation_rejected"),
    );
  });
});

test("revalidation rejects image, config, binary, and path replacement", async (t) => {
  await t.test("platform image swap", async () => {
    const original = imageFixture();
    const replacement = imageFixture({
      configDocument: {
        architecture: "arm64",
        config: { Labels: { replacement: "true" } },
        os: "linux",
      },
    });
    const inspected = inspector();
    const coordinator = new PlatformImageReservationCoordinator();
    const reserved = await coordinator.reservePlatformImage(
      reserveOptions(original, inspected.inspectCodex),
    );
    await assert.rejects(
      coordinator.consumeReservation(
        useOptions(replacement, inspected.inspectCodex, reserved.reservation),
      ),
      assertCode("platform_image_reservation_rejected"),
    );
    await assert.rejects(
      coordinator.consumeReservation(
        useOptions(original, inspected.inspectCodex, reserved.reservation),
      ),
      assertCode("platform_image_reservation_rejected"),
    );
  });

  for (const [name, changed] of [
    [
      "binary hash",
      measurement({ codexBinarySha256: "f".repeat(64) }),
    ],
    [
      "binary path",
      measurement({ codexBinaryPath: "/usr/local/bin/codex" }),
    ],
  ]) {
    await t.test(name, async () => {
      const fixture = imageFixture();
      let current = measurement();
      const inspected = inspector({ result: () => current });
      const coordinator = new PlatformImageReservationCoordinator();
      const reserved = await coordinator.reservePlatformImage(
        reserveOptions(fixture, inspected.inspectCodex),
      );
      current = changed;
      await assert.rejects(
        coordinator.revalidateReservation(
          useOptions(fixture, inspected.inspectCodex, reserved.reservation),
        ),
        assertCode("platform_image_reservation_rejected"),
      );
      await assert.rejects(
        coordinator.consumeReservation(
          useOptions(fixture, inspected.inspectCodex, reserved.reservation),
        ),
        assertCode("platform_image_reservation_rejected"),
      );
    });
  }
});

test("inspection uncertainty during revalidation is terminal", async () => {
  const fixture = imageFixture();
  let uncertain = false;
  const inspected = inspector({
    result: () => {
      if (uncertain) throw new Error("mount state became uncertain");
      return measurement();
    },
  });
  const coordinator = new PlatformImageReservationCoordinator();
  const reserved = await coordinator.reservePlatformImage(
    reserveOptions(fixture, inspected.inspectCodex),
  );
  uncertain = true;
  await assert.rejects(
    coordinator.revalidateReservation(
      useOptions(fixture, inspected.inspectCodex, reserved.reservation),
    ),
    assertCode("platform_image_inspection_uncertain"),
  );
  uncertain = false;
  await assert.rejects(
    coordinator.consumeReservation(
      useOptions(fixture, inspected.inspectCodex, reserved.reservation),
    ),
    assertCode("platform_image_reservation_rejected"),
  );
});

test("any concurrent reservation use poisons the capability", async (t) => {
  for (const secondMethod of [
    "revalidateReservation",
    "consumeReservation",
  ]) {
    await t.test(`revalidate plus ${secondMethod}`, async () => {
      const fixture = imageFixture();
      let releaseInspection;
      const inspectionGate = new Promise((resolve) => {
        releaseInspection = resolve;
      });
      let announceInspection;
      const inspectionEntered = new Promise((resolve) => {
        announceInspection = resolve;
      });
      let inspectionCalls = 0;
      const inspectCodex = async () => {
        inspectionCalls += 1;
        if (inspectionCalls === 2) {
          announceInspection();
          await inspectionGate;
        }
        return measurement();
      };
      const coordinator = new PlatformImageReservationCoordinator();
      const reserved = await coordinator.reservePlatformImage(
        reserveOptions(fixture, inspectCodex),
      );
      assert.equal(inspectionCalls, 1);

      const first = coordinator.revalidateReservation(
        useOptions(fixture, inspectCodex, reserved.reservation),
      );
      await inspectionEntered;
      assert.equal(inspectionCalls, 2);

      await assert.rejects(
        coordinator[secondMethod](
          useOptions(fixture, inspectCodex, reserved.reservation),
        ),
        assertCode("platform_image_reservation_rejected"),
      );
      releaseInspection();
      await assert.rejects(
        first,
        assertCode("platform_image_reservation_rejected"),
      );
      assert.equal(inspectionCalls, 2);

      await assert.rejects(
        coordinator.consumeReservation(
          useOptions(fixture, inspectCodex, reserved.reservation),
        ),
        assertCode("platform_image_reservation_rejected"),
      );
      assert.equal(inspectionCalls, 2);
    });
  }
});

test("prototype poisoning cannot forge image authority", async (t) => {
  for (const scenario of [
    "array-iterator-freeze",
    "array-iterator-platform",
    "reserve",
    "revalidate",
    "consume",
    "hash-prototype",
    "promise-rejection",
    "regexp-prototype",
    "set-constructor",
    "typed-array-byte-length",
    "weakmap-constructor",
  ]) {
    await t.test(scenario, () => {
      const result = spawnSync(
        process.execPath,
        [
          "--unhandled-rejections=strict",
          INTRINSIC_POISONING_FIXTURE,
          scenario,
        ],
        {
          encoding: "utf8",
          timeout: 10_000,
        },
      );
      assert.equal(result.error, undefined);
      assert.equal(result.signal, null);
      assert.equal(
        result.status,
        0,
        `intrinsic-poisoning child failed\nstdout:\n${result.stdout}\nstderr:\n${result.stderr}`,
      );
    });
  }
});
