import assert from "node:assert/strict";
import { createHash } from "node:crypto";

import {
  PlatformImageReservationCoordinator,
} from "../../src/platform-image-reservation.mjs";
import { createSessionManifest } from "../../src/session-storage-contracts.mjs";

const scenario = process.argv[2];
const scenarios = new Set(["consume", "reserve", "revalidate"]);
if (scenario !== undefined && !scenarios.has(scenario)) {
  throw new Error("unsupported intrinsic-poisoning scenario");
}

const PromiseConstructor = Promise;
const objectGetOwnPropertyDescriptor = Object.getOwnPropertyDescriptor;
const promiseConstructorDescriptor = objectGetOwnPropertyDescriptor(
  PromiseConstructor.prototype,
  "constructor",
);
const promiseThenDescriptor = objectGetOwnPropertyDescriptor(
  PromiseConstructor.prototype,
  "then",
);
const CODEX_BINARY_SHA256 = "b".repeat(64);
const CODEX_VERSION = "codex-cli 0.144.1";
const CONFIG_MEDIA_TYPE = "application/vnd.oci.image.config.v1+json";
const DIFF_ID = `sha256:${"d".repeat(64)}`;
const LAYER_DIGEST = `sha256:${"c".repeat(64)}`;
const LAYER_MEDIA_TYPE =
  "application/vnd.oci.image.layer.v1.tar+gzip";
const MANIFEST_MEDIA_TYPE =
  "application/vnd.oci.image.manifest.v1+json";

function digest(bytes) {
  return `sha256:${createHash("sha256").update(bytes).digest("hex")}`;
}

function fixture() {
  const configBytes = Buffer.from(
    JSON.stringify({
      architecture: "arm64",
      config: {},
      os: "linux",
      rootfs: {
        type: "layers",
        diff_ids: [DIFF_ID],
      },
    }),
    "utf8",
  );
  const manifestBytes = Buffer.from(
    JSON.stringify({
      schemaVersion: 2,
      mediaType: MANIFEST_MEDIA_TYPE,
      config: {
        mediaType: CONFIG_MEDIA_TYPE,
        digest: digest(configBytes),
        size: configBytes.byteLength,
      },
      layers: [
        {
          mediaType: LAYER_MEDIA_TYPE,
          digest: LAYER_DIGEST,
          size: 1024,
        },
      ],
    }),
    "utf8",
  );
  const descriptor = {
    bytes: manifestBytes,
    digest: digest(manifestBytes),
    mediaType: MANIFEST_MEDIA_TYPE,
    size: manifestBytes.byteLength,
  };
  return {
    configBytes,
    descriptor,
    sessionManifest: createSessionManifest({
      sessionId: "019f3d80-0000-7000-8000-000000000001",
      codex: {
        rootThreadId: "019f3d80-0000-7000-8000-000000000002",
        sessionId: "019f3d80-0000-7000-8000-000000000002",
        ephemeral: false,
        historyMode: "paginated",
      },
      runtime: {
        imageDigest: descriptor.digest,
        imageMediaType: descriptor.mediaType,
        platform: "linux/arm64",
        codexVersion: CODEX_VERSION,
        codexSandbox: "danger-full-access",
      },
    }),
  };
}

function measurement() {
  return {
    codexBinaryPath: "/opt/portable-codex/bin/codex",
    codexBinarySha256: CODEX_BINARY_SHA256,
    codexVersion: CODEX_VERSION,
  };
}

let inspectionPromise;
let poisonNext = scenario === "reserve";

function restorePromisePrototype() {
  Object.defineProperty(
    PromiseConstructor.prototype,
    "constructor",
    promiseConstructorDescriptor,
  );
  Object.defineProperty(
    PromiseConstructor.prototype,
    "then",
    promiseThenDescriptor,
  );
}

function poisonPromisePrototype() {
  Object.defineProperty(PromiseConstructor.prototype, "constructor", {
    configurable: promiseConstructorDescriptor.configurable,
    enumerable: promiseConstructorDescriptor.enumerable,
    get() {
      throw new Error("poisoned Promise constructor getter");
    },
  });
  Object.defineProperty(PromiseConstructor.prototype, "then", {
    ...promiseThenDescriptor,
    value() {
      throw new Error("poisoned Promise then");
    },
  });
}

function assertProtectedPromise(value) {
  assert.deepEqual(objectGetOwnPropertyDescriptor(value, "constructor"), {
    configurable: false,
    enumerable: false,
    value: PromiseConstructor,
    writable: false,
  });
}

function safeThen(value, onFulfilled, onRejected) {
  return Reflect.apply(promiseThenDescriptor.value, value, [
    onFulfilled,
    onRejected,
  ]);
}

function inspectCodex() {
  inspectionPromise = PromiseConstructor.resolve(measurement());
  if (poisonNext) {
    poisonNext = false;
    poisonPromisePrototype();
  }
  return inspectionPromise;
}

const image = fixture();
const coordinator = new PlatformImageReservationCoordinator();

function assertRuntimeIdentity(result) {
  assert.equal(
    result.runtimeIdentity.codexBinarySha256,
    CODEX_BINARY_SHA256,
  );
}

function failAfterRestore(error) {
  restorePromisePrototype();
  throw error;
}

function runScenario() {
  if (scenario === "reserve") {
    const operation = coordinator.reservePlatformImage({
      ...image,
      inspectCodex,
    });
    assertProtectedPromise(inspectionPromise);
    assertProtectedPromise(operation);
    safeThen(
      operation,
      (reserved) => {
        restorePromisePrototype();
        assertRuntimeIdentity(reserved);
        process.exitCode = 0;
      },
      failAfterRestore,
    );
  } else {
    const reserveOperation = coordinator.reservePlatformImage({
      ...image,
      inspectCodex,
    });
    assertProtectedPromise(reserveOperation);
    safeThen(
      reserveOperation,
      (reserved) => {
        poisonNext = true;
        const operation = coordinator[
          scenario === "consume"
            ? "consumeReservation"
            : "revalidateReservation"
        ]({
          configBytes: image.configBytes,
          descriptor: image.descriptor,
          inspectCodex,
          reservation: reserved.reservation,
        });
        assertProtectedPromise(inspectionPromise);
        assertProtectedPromise(operation);
        safeThen(
          operation,
          (result) => {
            restorePromisePrototype();
            assertRuntimeIdentity(result);
            assertRuntimeIdentity(reserved);
            process.exitCode = 0;
          },
          failAfterRestore,
        );
      },
      (error) => {
        throw error;
      },
    );
  }
}

if (scenario !== undefined) {
  process.exitCode = 1;
  setImmediate(runScenario);
}
