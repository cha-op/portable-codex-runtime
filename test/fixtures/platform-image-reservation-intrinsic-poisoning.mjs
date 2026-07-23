import assert from "node:assert/strict";
import { createHash } from "node:crypto";

import {
  PlatformImageReservationCoordinator,
  PlatformImageReservationError,
} from "../../src/platform-image-reservation.mjs";
import { createSessionManifest } from "../../src/session-storage-contracts.mjs";

const scenario = process.argv[2];
const scenarios = new Set([
  "consume",
  "promise-rejection",
  "regexp-prototype",
  "reserve",
  "revalidate",
  "set-constructor",
  "typed-array-byte-length",
]);
if (scenario !== undefined && !scenarios.has(scenario)) {
  throw new Error("unsupported intrinsic-poisoning scenario");
}

const PromiseConstructor = Promise;
const objectGetOwnPropertyDescriptor = Object.getOwnPropertyDescriptor;
const promiseSpeciesSymbol = Symbol.species;
const promiseSpeciesDescriptor = objectGetOwnPropertyDescriptor(
  PromiseConstructor,
  promiseSpeciesSymbol,
);
const promiseConstructorDescriptor = objectGetOwnPropertyDescriptor(
  PromiseConstructor.prototype,
  "constructor",
);
const promiseCatchDescriptor = objectGetOwnPropertyDescriptor(
  PromiseConstructor.prototype,
  "catch",
);
const promiseFinallyDescriptor = objectGetOwnPropertyDescriptor(
  PromiseConstructor.prototype,
  "finally",
);
const promiseThenDescriptor = objectGetOwnPropertyDescriptor(
  PromiseConstructor.prototype,
  "then",
);
const regexpExecDescriptor = objectGetOwnPropertyDescriptor(
  RegExp.prototype,
  "exec",
);
const regexpTestDescriptor = objectGetOwnPropertyDescriptor(
  RegExp.prototype,
  "test",
);
const setAddDescriptor = objectGetOwnPropertyDescriptor(
  Set.prototype,
  "add",
);
const setHasDescriptor = objectGetOwnPropertyDescriptor(
  Set.prototype,
  "has",
);
const setConstructorDescriptor = objectGetOwnPropertyDescriptor(
  globalThis,
  "Set",
);
const typedArrayPrototype = Object.getPrototypeOf(
  Uint8Array.prototype,
);
const typedArrayByteLengthDescriptor = objectGetOwnPropertyDescriptor(
  typedArrayPrototype,
  "byteLength",
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

function fixture({ embedded = false } = {}) {
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
  const configDescriptor = {
    mediaType: CONFIG_MEDIA_TYPE,
    digest: digest(configBytes),
    size: configBytes.byteLength,
  };
  if (embedded) {
    configDescriptor.data = configBytes.toString("base64");
  }
  const manifestBytes = Buffer.from(
    JSON.stringify({
      schemaVersion: 2,
      mediaType: MANIFEST_MEDIA_TYPE,
      config: configDescriptor,
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
let poisonNext =
  scenario === "reserve" || scenario === "promise-rejection";

function restorePromisePrototype() {
  Object.defineProperty(
    PromiseConstructor.prototype,
    "catch",
    promiseCatchDescriptor,
  );
  Object.defineProperty(
    PromiseConstructor.prototype,
    "constructor",
    promiseConstructorDescriptor,
  );
  Object.defineProperty(
    PromiseConstructor.prototype,
    "finally",
    promiseFinallyDescriptor,
  );
  Object.defineProperty(
    PromiseConstructor.prototype,
    "then",
    promiseThenDescriptor,
  );
  Object.defineProperty(
    PromiseConstructor,
    promiseSpeciesSymbol,
    promiseSpeciesDescriptor,
  );
}

function restoreSetIntrinsics() {
  Object.defineProperty(
    globalThis,
    "Set",
    setConstructorDescriptor,
  );
  Object.defineProperty(
    Set.prototype,
    "add",
    setAddDescriptor,
  );
  Object.defineProperty(
    Set.prototype,
    "has",
    setHasDescriptor,
  );
}

function poisonSetIntrinsics() {
  const OriginalSet = Set;
  Object.defineProperty(OriginalSet.prototype, "add", {
    ...setAddDescriptor,
    value() {
      throw new Error("poisoned Set add");
    },
  });
  Object.defineProperty(OriginalSet.prototype, "has", {
    ...setHasDescriptor,
    value() {
      throw new Error("poisoned Set has");
    },
  });
  Object.defineProperty(globalThis, "Set", {
    ...setConstructorDescriptor,
    value: class PoisonedSet {
      constructor() {
        throw new Error("poisoned Set constructor");
      }
    },
  });
}

function poisonPromisePrototype() {
  function ForgedPromise(executor) {
    Reflect.apply(executor, undefined, [
      () => {},
      () => {},
    ]);
    return { forged: true };
  }
  Object.defineProperty(
    PromiseConstructor,
    promiseSpeciesSymbol,
    {
      configurable: promiseSpeciesDescriptor.configurable,
      enumerable: promiseSpeciesDescriptor.enumerable,
      value: ForgedPromise,
      writable: true,
    },
  );
  Object.defineProperty(PromiseConstructor.prototype, "catch", {
    ...promiseCatchDescriptor,
    value() {
      throw new Error("poisoned Promise catch");
    },
  });
  Object.defineProperty(PromiseConstructor.prototype, "constructor", {
    configurable: promiseConstructorDescriptor.configurable,
    enumerable: promiseConstructorDescriptor.enumerable,
    get() {
      throw new Error("poisoned Promise constructor getter");
    },
  });
  Object.defineProperty(PromiseConstructor.prototype, "finally", {
    ...promiseFinallyDescriptor,
    value() {
      throw new Error("poisoned Promise finally");
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
  const constructorDescriptor = objectGetOwnPropertyDescriptor(
    value,
    "constructor",
  );
  assert.equal(constructorDescriptor.configurable, false);
  assert.equal(constructorDescriptor.enumerable, false);
  assert.equal(constructorDescriptor.writable, false);
  assert.notEqual(constructorDescriptor.value, PromiseConstructor);
  assert.equal(
    Object.getPrototypeOf(constructorDescriptor.value),
    null,
  );
  assert.equal(Object.isFrozen(constructorDescriptor.value), true);
  assert.equal(
    constructorDescriptor.value[promiseSpeciesSymbol],
    PromiseConstructor,
  );
  for (const key of ["catch", "finally", "then"]) {
    const descriptor = objectGetOwnPropertyDescriptor(value, key);
    assert.equal(descriptor.configurable, false);
    assert.equal(descriptor.enumerable, false);
    assert.equal(descriptor.writable, false);
    assert.equal(typeof descriptor.value, "function");
    assert.notEqual(
      descriptor.value,
      objectGetOwnPropertyDescriptor(
        PromiseConstructor.prototype,
        key,
      ).value,
    );
  }
}

function safeThen(value, onFulfilled, onRejected) {
  return Reflect.apply(promiseThenDescriptor.value, value, [
    onFulfilled,
    onRejected,
  ]);
}

function inspectCodex() {
  inspectionPromise =
    scenario === "promise-rejection"
      ? PromiseConstructor.reject(new Error("inspector rejected"))
      : PromiseConstructor.resolve(measurement());
  if (poisonNext) {
    poisonNext = false;
    poisonPromisePrototype();
  }
  return inspectionPromise;
}

const image = fixture({
  embedded: scenario === "typed-array-byte-length",
});
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

function observeProtectedOperation(operation, validate) {
  assertProtectedPromise(operation);
  let catchCalls = 0;
  let completions = 0;
  let finallyCalls = 0;
  const complete = (assertion) => {
    try {
      assertion();
      completions += 1;
      if (completions === 3) {
        assert.equal(catchCalls, 0);
        assert.equal(finallyCalls, 1);
        restorePromisePrototype();
        process.exitCode = 0;
      }
    } catch (error) {
      failAfterRestore(error);
    }
  };

  const thenChain = operation.then((result) => {
    validate(result);
    return "then-completed";
  });
  const catchChain = operation.catch(() => {
    catchCalls += 1;
    throw new Error("successful operation reached catch");
  });
  const finallyChain = operation.finally(() => {
    finallyCalls += 1;
  });
  assertProtectedPromise(thenChain);
  assertProtectedPromise(catchChain);
  assertProtectedPromise(finallyChain);

  thenChain.then(
    (value) => complete(() => assert.equal(value, "then-completed")),
    failAfterRestore,
  );
  catchChain.then(
    (result) => complete(() => validate(result)),
    failAfterRestore,
  );
  finallyChain.then(
    (result) => complete(() => validate(result)),
    failAfterRestore,
  );
}

function observeRejectedOperation(operation) {
  assertProtectedPromise(operation);
  let completions = 0;
  let finallyCalls = 0;
  const validateError = (error) => {
    assert.ok(error instanceof PlatformImageReservationError);
    assert.equal(error.code, "platform_image_inspection_uncertain");
  };
  const complete = (assertion) => {
    try {
      assertion();
      completions += 1;
      if (completions === 3) {
        assert.equal(finallyCalls, 1);
        restorePromisePrototype();
        process.exitCode = 0;
      }
    } catch (error) {
      failAfterRestore(error);
    }
  };

  const thenChain = operation.then(
    () => {
      throw new Error("rejected operation fulfilled");
    },
    (error) => {
      validateError(error);
      return "then-rejected";
    },
  );
  const catchChain = operation.catch((error) => {
    validateError(error);
    return "catch-rejected";
  });
  const finallyChain = operation.finally(() => {
    finallyCalls += 1;
  });
  assertProtectedPromise(thenChain);
  assertProtectedPromise(catchChain);
  assertProtectedPromise(finallyChain);

  thenChain.then(
    (value) => complete(() => assert.equal(value, "then-rejected")),
    failAfterRestore,
  );
  catchChain.then(
    (value) => complete(() => assert.equal(value, "catch-rejected")),
    failAfterRestore,
  );
  finallyChain.then(
    () => complete(() => assert.fail("finally changed rejection")),
    (error) => complete(() => validateError(error)),
  );
}

function runScenario() {
  if (scenario === "reserve" || scenario === "promise-rejection") {
    const operation = coordinator.reservePlatformImage({
      ...image,
      inspectCodex,
    });
    assertProtectedPromise(inspectionPromise);
    if (scenario === "promise-rejection") {
      observeRejectedOperation(operation);
    } else {
      observeProtectedOperation(
        operation,
        assertRuntimeIdentity,
      );
    }
  } else if (scenario === "revalidate" || scenario === "consume") {
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
        observeProtectedOperation(
          operation,
          (result) => {
            assertRuntimeIdentity(result);
            assertRuntimeIdentity(reserved);
          },
        );
      },
      (error) => {
        throw error;
      },
    );
  } else if (scenario === "set-constructor") {
    const reserveOperation = coordinator.reservePlatformImage({
      ...image,
      inspectCodex,
    });
    safeThen(
      reserveOperation,
      (reserved) => {
        poisonSetIntrinsics();
        const operation = coordinator.revalidateReservation({
          configBytes: image.configBytes,
          descriptor: image.descriptor,
          inspectCodex,
          reservation: reserved.reservation,
        });
        safeThen(
          operation,
          (result) => {
            restoreSetIntrinsics();
            assertRuntimeIdentity(result);
            process.exitCode = 0;
          },
          (error) => {
            restoreSetIntrinsics();
            throw error;
          },
        );
      },
      (error) => {
        throw error;
      },
    );
  } else if (scenario === "regexp-prototype") {
    Object.defineProperty(RegExp.prototype, "exec", {
      ...regexpExecDescriptor,
      value(input) {
        if (this.sticky) {
          throw new Error("poisoned sticky RegExp exec");
        }
        return Reflect.apply(regexpExecDescriptor.value, this, [input]);
      },
    });
    Object.defineProperty(RegExp.prototype, "test", {
      ...regexpTestDescriptor,
      value(input) {
        if (this.source === "\\s") {
          throw new Error("poisoned whitespace RegExp test");
        }
        return Reflect.apply(regexpTestDescriptor.value, this, [input]);
      },
    });
    const restore = () => {
      Object.defineProperty(
        RegExp.prototype,
        "exec",
        regexpExecDescriptor,
      );
      Object.defineProperty(
        RegExp.prototype,
        "test",
        regexpTestDescriptor,
      );
    };
    safeThen(
      coordinator.reservePlatformImage({
        ...image,
        inspectCodex,
      }),
      (reserved) => {
        restore();
        assertRuntimeIdentity(reserved);
        process.exitCode = 0;
      },
      (error) => {
        restore();
        throw error;
      },
    );
  } else {
    Object.defineProperty(typedArrayPrototype, "byteLength", {
      configurable: typedArrayByteLengthDescriptor.configurable,
      enumerable: typedArrayByteLengthDescriptor.enumerable,
      get() {
        return (
          Reflect.apply(
            typedArrayByteLengthDescriptor.get,
            this,
            [],
          ) + 1
        );
      },
    });
    const restore = () => {
      Object.defineProperty(
        typedArrayPrototype,
        "byteLength",
        typedArrayByteLengthDescriptor,
      );
    };
    safeThen(
      coordinator.reservePlatformImage({
        ...image,
        inspectCodex,
      }),
      (reserved) => {
        restore();
        assertRuntimeIdentity(reserved);
        process.exitCode = 0;
      },
      (error) => {
        restore();
        throw error;
      },
    );
  }
}

if (scenario !== undefined) {
  process.exitCode = 1;
  setImmediate(runScenario);
}
