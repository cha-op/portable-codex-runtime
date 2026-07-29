import assert from "node:assert/strict";
import { Hash, createHash } from "node:crypto";
import { URL } from "node:url";

import {
  PlatformImageReservationCoordinator,
  PlatformImageReservationError,
} from "../../src/platform-image-reservation.mjs";
import { createSessionManifest } from "../../src/session-storage-contracts.mjs";

const scenario = process.argv[2];
const scenarios = new Set([
  "array-iterator-freeze",
  "array-iterator-platform",
  "consume",
  "hash-prototype",
  "manifest-validation",
  "promise-rejection",
  "regexp-prototype",
  "reserve",
  "revalidate",
  "set-constructor",
  "structured-clone-capture",
  "typed-array-byte-length",
  "url-accessors",
  "weakmap-constructor",
]);
if (scenario !== undefined && !scenarios.has(scenario)) {
  throw new Error("unsupported intrinsic-poisoning scenario");
}

const PromiseConstructor = Promise;
const WeakMapConstructor = WeakMap;
const objectGetOwnPropertyDescriptor = Object.getOwnPropertyDescriptor;
const arrayIteratorSymbol = Symbol.iterator;
const arrayIteratorDescriptor = objectGetOwnPropertyDescriptor(
  Array.prototype,
  arrayIteratorSymbol,
);
const arrayEveryDescriptor = objectGetOwnPropertyDescriptor(
  Array.prototype,
  "every",
);
const arrayIncludesDescriptor = objectGetOwnPropertyDescriptor(
  Array.prototype,
  "includes",
);
const globalWeakMapDescriptor = objectGetOwnPropertyDescriptor(
  globalThis,
  "WeakMap",
);
const objectGetOwnPropertyDescriptorDescriptor =
  objectGetOwnPropertyDescriptor(
    Object,
    "getOwnPropertyDescriptor",
  );
const structuredCloneDescriptor = objectGetOwnPropertyDescriptor(
  globalThis,
  "structuredClone",
);
const structuredCloneIntrinsic = structuredCloneDescriptor.value;
const hashDigestDescriptor = objectGetOwnPropertyDescriptor(
  Hash.prototype,
  "digest",
);
const hashUpdateDescriptor = objectGetOwnPropertyDescriptor(
  Hash.prototype,
  "update",
);
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
const urlHostnameDescriptor = objectGetOwnPropertyDescriptor(
  URL.prototype,
  "hostname",
);
const urlPasswordDescriptor = objectGetOwnPropertyDescriptor(
  URL.prototype,
  "password",
);
const urlProtocolDescriptor = objectGetOwnPropertyDescriptor(
  URL.prototype,
  "protocol",
);
const urlUsernameDescriptor = objectGetOwnPropertyDescriptor(
  URL.prototype,
  "username",
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

function fixture({
  architecture = "arm64",
  descriptorUrl = undefined,
  embedded = false,
} = {}) {
  const configBytes = Buffer.from(
    JSON.stringify({
      architecture,
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
  if (descriptorUrl !== undefined) {
    configDescriptor.urls = [descriptorUrl];
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

function tamperLayerDigest(descriptor) {
  const document = JSON.parse(descriptor.bytes.toString("utf8"));
  document.layers[0].digest = `sha256:${"e".repeat(64)}`;
  const bytes = Buffer.from(JSON.stringify(document), "utf8");
  assert.equal(bytes.byteLength, descriptor.bytes.byteLength);
  return {
    ...descriptor,
    bytes,
  };
}

function replaceLayerDigest(imageFixture) {
  const document = JSON.parse(
    imageFixture.descriptor.bytes.toString("utf8"),
  );
  document.layers[0].digest = `sha256:${"d".repeat(64)}`;
  const bytes = Buffer.from(JSON.stringify(document), "utf8");
  return {
    configBytes: imageFixture.configBytes,
    descriptor: {
      ...imageFixture.descriptor,
      bytes,
      digest: digest(bytes),
      size: bytes.byteLength,
    },
    sessionManifest: imageFixture.sessionManifest,
  };
}

let inspectionPromise;
let poisonNext =
  scenario === "reserve" ||
  scenario === "promise-rejection" ||
  scenario === "hash-prototype";
let leakedReservationLedger;
let poisonedHashDigestCalls = 0;
let poisonedHashUpdateCalls = 0;
let poisonedManifestValidationCalls = 0;
let poisonedStructuredCloneCalls = 0;

function restoreArrayIterator() {
  Object.defineProperty(
    Array.prototype,
    arrayIteratorSymbol,
    arrayIteratorDescriptor,
  );
}

function poisonArrayIterator(mode) {
  Object.defineProperty(Array.prototype, arrayIteratorSymbol, {
    ...arrayIteratorDescriptor,
    value() {
      if (
        mode === "platform" &&
        this.length === 4 &&
        this[0] === "configBytes" &&
        this[1] === "descriptor" &&
        this[2] === "sessionManifest" &&
        this[3] === "inspectCodex"
      ) {
        throw new Error("poisoned options key iterator");
      }
      if (
        mode === "platform" &&
        this.length === 2 &&
        this[0] === "linux" &&
        this[1] === "arm64"
      ) {
        return Reflect.apply(
          arrayIteratorDescriptor.value,
          ["linux", "amd64"],
          [],
        );
      }
      if (
        mode === "freeze" &&
        this.length === 3 &&
        ((this[0] === "platformImage" &&
          this[1] === "codexSandbox" &&
          this[2] === "codexVersion") ||
          (this[0] === "codexSandbox" &&
            this[1] === "codexVersion" &&
            this[2] === "platformImage"))
      ) {
        return Reflect.apply(
          arrayIteratorDescriptor.value,
          ["codexSandbox", "codexVersion"],
          [],
        );
      }
      return Reflect.apply(arrayIteratorDescriptor.value, this, []);
    },
  });
}

function restoreManifestValidationIntrinsics() {
  Object.defineProperty(
    Array.prototype,
    "every",
    arrayEveryDescriptor,
  );
  Object.defineProperty(
    Array.prototype,
    "includes",
    arrayIncludesDescriptor,
  );
  Object.defineProperty(
    Object,
    "getOwnPropertyDescriptor",
    objectGetOwnPropertyDescriptorDescriptor,
  );
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
}

function poisonManifestValidationIntrinsics() {
  Object.defineProperty(Array.prototype, "every", {
    ...arrayEveryDescriptor,
    value() {
      poisonedManifestValidationCalls += 1;
      return true;
    },
  });
  Object.defineProperty(Array.prototype, "includes", {
    ...arrayIncludesDescriptor,
    value() {
      poisonedManifestValidationCalls += 1;
      return true;
    },
  });
  Object.defineProperty(Object, "getOwnPropertyDescriptor", {
    ...objectGetOwnPropertyDescriptorDescriptor,
    value() {
      poisonedManifestValidationCalls += 1;
      throw new Error("poisoned Object.getOwnPropertyDescriptor");
    },
  });
  Object.defineProperty(RegExp.prototype, "exec", {
    ...regexpExecDescriptor,
    value() {
      poisonedManifestValidationCalls += 1;
      return ["forged manifest match"];
    },
  });
  Object.defineProperty(RegExp.prototype, "test", {
    ...regexpTestDescriptor,
    value() {
      poisonedManifestValidationCalls += 1;
      return true;
    },
  });
}

function restoreHashPrototype() {
  Object.defineProperty(
    Hash.prototype,
    "digest",
    hashDigestDescriptor,
  );
  Object.defineProperty(
    Hash.prototype,
    "update",
    hashUpdateDescriptor,
  );
}

function poisonHashPrototype() {
  const forgedHashes = new WeakSet();
  Object.defineProperty(Hash.prototype, "update", {
    ...hashUpdateDescriptor,
    value(input, encoding) {
      poisonedHashUpdateCalls += 1;
      if (
        Buffer.isBuffer(input) &&
        input.byteLength === tamperedDescriptor.bytes.byteLength
      ) {
        forgedHashes.add(this);
        return this;
      }
      return Reflect.apply(hashUpdateDescriptor.value, this, [
        input,
        encoding,
      ]);
    },
  });
  Object.defineProperty(Hash.prototype, "digest", {
    ...hashDigestDescriptor,
    value(encoding) {
      poisonedHashDigestCalls += 1;
      if (forgedHashes.has(this)) {
        assert.equal(encoding, "hex");
        return image.descriptor.digest.slice("sha256:".length);
      }
      return Reflect.apply(hashDigestDescriptor.value, this, [
        encoding,
      ]);
    },
  });
}

function restoreUrlAccessors() {
  Object.defineProperty(
    URL.prototype,
    "hostname",
    urlHostnameDescriptor,
  );
  Object.defineProperty(
    URL.prototype,
    "password",
    urlPasswordDescriptor,
  );
  Object.defineProperty(
    URL.prototype,
    "protocol",
    urlProtocolDescriptor,
  );
  Object.defineProperty(
    URL.prototype,
    "username",
    urlUsernameDescriptor,
  );
}

function poisonUrlAccessors() {
  Object.defineProperty(URL.prototype, "hostname", {
    ...urlHostnameDescriptor,
    get() {
      return "example.invalid";
    },
  });
  Object.defineProperty(URL.prototype, "password", {
    ...urlPasswordDescriptor,
    get() {
      return "";
    },
  });
  Object.defineProperty(URL.prototype, "protocol", {
    ...urlProtocolDescriptor,
    get() {
      return "https:";
    },
  });
  Object.defineProperty(URL.prototype, "username", {
    ...urlUsernameDescriptor,
    get() {
      return "";
    },
  });
}

function constructCoordinator() {
  if (scenario !== "weakmap-constructor") {
    return new PlatformImageReservationCoordinator();
  }
  class LeakingWeakMap extends WeakMapConstructor {
    constructor(...args) {
      super(...args);
      leakedReservationLedger = this;
    }
  }
  Object.defineProperty(globalThis, "WeakMap", {
    ...globalWeakMapDescriptor,
    value: LeakingWeakMap,
  });
  try {
    const coordinator = new PlatformImageReservationCoordinator();
    assert.equal(leakedReservationLedger, undefined);
    return coordinator;
  } finally {
    Object.defineProperty(
      globalThis,
      "WeakMap",
      globalWeakMapDescriptor,
    );
  }
}

function restoreStructuredClone() {
  Object.defineProperty(
    globalThis,
    "structuredClone",
    structuredCloneDescriptor,
  );
}

async function inspectWithStructuredClonePoisoning() {
  let nestedInspectorCalls = 0;
  Object.defineProperty(globalThis, "structuredClone", {
    ...structuredCloneDescriptor,
    value(value, options) {
      poisonedStructuredCloneCalls += 1;
      const clone =
        options === undefined
          ? Reflect.apply(structuredCloneIntrinsic, globalThis, [value])
          : Reflect.apply(structuredCloneIntrinsic, globalThis, [
              value,
              options,
            ]);
      if (value === image.sessionManifest) {
        clone.runtime.imageDigest = alternateImage.descriptor.digest;
      }
      return clone;
    },
  });
  try {
    let nestedError;
    try {
      await coordinator.reservePlatformImage({
        ...alternateImage,
        inspectCodex() {
          nestedInspectorCalls += 1;
          return PromiseConstructor.resolve(measurement());
        },
      });
    } catch (error) {
      nestedError = error;
    }
    assert.equal(poisonedStructuredCloneCalls, 0);
    assert.equal(nestedInspectorCalls, 0);
    assert.ok(nestedError instanceof PlatformImageReservationError);
    assert.equal(
      nestedError.code,
      "platform_image_identity_mismatch",
    );
    return measurement();
  } finally {
    restoreStructuredClone();
  }
}

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
  if (scenario === "structured-clone-capture") {
    inspectionPromise = inspectWithStructuredClonePoisoning();
    return inspectionPromise;
  }
  inspectionPromise =
    scenario === "promise-rejection"
      ? PromiseConstructor.reject(new Error("inspector rejected"))
      : PromiseConstructor.resolve(measurement());
  if (poisonNext) {
    poisonNext = false;
    if (scenario === "hash-prototype") {
      poisonHashPrototype();
    } else {
      poisonPromisePrototype();
    }
  }
  return inspectionPromise;
}

const image = fixture({
  architecture:
    scenario === "array-iterator-platform" ? "amd64" : "arm64",
  descriptorUrl:
    scenario === "url-accessors"
      ? "http://user:password@example.invalid/config"
      : undefined,
  embedded: scenario === "typed-array-byte-length",
});
const tamperedDescriptor = tamperLayerDigest(image.descriptor);
const alternateImage = replaceLayerDigest(image);
let coordinator;

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
  coordinator = constructCoordinator();
  if (
    scenario === "reserve" ||
    scenario === "promise-rejection" ||
    scenario === "structured-clone-capture" ||
    scenario === "weakmap-constructor"
  ) {
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
  } else if (scenario === "array-iterator-platform") {
    poisonArrayIterator("platform");
    safeThen(
      coordinator.reservePlatformImage({
        ...image,
        inspectCodex,
      }),
      () => {
        restoreArrayIterator();
        throw new Error("poisoned platform iterator forged image authority");
      },
      (error) => {
        restoreArrayIterator();
        assert.ok(error instanceof PlatformImageReservationError);
        assert.equal(error.code, "platform_image_identity_mismatch");
        process.exitCode = 0;
      },
    );
  } else if (scenario === "array-iterator-freeze") {
    poisonArrayIterator("freeze");
    safeThen(
      coordinator.reservePlatformImage({
        ...image,
        inspectCodex,
      }),
      (reserved) => {
        restoreArrayIterator();
        assert.equal(Object.isFrozen(reserved.projection), true);
        assert.equal(
          Object.isFrozen(reserved.projection.platformImage),
          true,
        );
        assert.equal(
          Object.isFrozen(reserved.projection.platformImage.config),
          true,
        );
        assert.equal(Object.isFrozen(reserved.runtimeIdentity), true);
        process.exitCode = 0;
      },
      (error) => {
        restoreArrayIterator();
        throw error;
      },
    );
  } else if (scenario === "hash-prototype") {
    safeThen(
      coordinator.reservePlatformImage({
        ...image,
        inspectCodex,
      }),
      (reserved) => {
        safeThen(
          coordinator.revalidateReservation({
            configBytes: image.configBytes,
            descriptor: tamperedDescriptor,
            inspectCodex,
            reservation: reserved.reservation,
          }),
          () => {
            restoreHashPrototype();
            throw new Error("poisoned Hash prototype forged image authority");
          },
          (error) => {
            restoreHashPrototype();
            assert.equal(poisonedHashDigestCalls, 0);
            assert.equal(poisonedHashUpdateCalls, 0);
            assert.ok(error instanceof PlatformImageReservationError);
            assert.equal(
              error.code,
              "platform_image_reservation_rejected",
            );
            process.exitCode = 0;
          },
        );
      },
      (error) => {
        restoreHashPrototype();
        throw error;
      },
    );
  } else if (scenario === "manifest-validation") {
    let inspectorCalls = 0;
    const invalidSessionManifest = {
      ...image.sessionManifest,
      codex: {
        ...image.sessionManifest.codex,
        historyMode: "future-history",
      },
      sessionId: "not-a-uuid",
    };
    poisonManifestValidationIntrinsics();
    safeThen(
      coordinator.reservePlatformImage({
        ...image,
        sessionManifest: invalidSessionManifest,
        inspectCodex() {
          inspectorCalls += 1;
          return PromiseConstructor.resolve(measurement());
        },
      }),
      () => {
        restoreManifestValidationIntrinsics();
        throw new Error(
          "poisoned manifest intrinsics forged image authority",
        );
      },
      (error) => {
        restoreManifestValidationIntrinsics();
        assert.equal(poisonedManifestValidationCalls, 0);
        assert.equal(inspectorCalls, 0);
        assert.ok(error instanceof PlatformImageReservationError);
        assert.equal(error.code, "invalid_platform_image_request");
        process.exitCode = 0;
      },
    );
  } else if (scenario === "url-accessors") {
    let inspectorCalls = 0;
    poisonUrlAccessors();
    safeThen(
      coordinator.reservePlatformImage({
        ...image,
        inspectCodex() {
          inspectorCalls += 1;
          return PromiseConstructor.resolve(measurement());
        },
      }),
      () => {
        restoreUrlAccessors();
        throw new Error("poisoned URL accessors forged image authority");
      },
      (error) => {
        restoreUrlAccessors();
        assert.equal(inspectorCalls, 0);
        assert.ok(error instanceof PlatformImageReservationError);
        assert.equal(
          error.code,
          "platform_image_identity_mismatch",
        );
        process.exitCode = 0;
      },
    );
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
