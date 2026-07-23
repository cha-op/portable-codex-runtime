import { createHash } from "node:crypto";
import { posix as posixPath } from "node:path";
import { URL } from "node:url";
import { TextDecoder, types as utilTypes } from "node:util";

import {
  PLATFORM_IMAGE_MEDIA_TYPES,
  assertSessionManifest,
} from "./session-storage-contracts.mjs";

const arrayEveryIntrinsic = Array.prototype.every;
const arrayIncludesIntrinsic = Array.prototype.includes;
const arrayIsArray = Array.isArray;
const arrayMapIntrinsic = Array.prototype.map;
const bufferAllocUnsafe = Buffer.allocUnsafe;
const bufferCompare = Buffer.compare;
const bufferFrom = Buffer.from;
const bufferIsBuffer = Buffer.isBuffer;
const jsonParse = JSON.parse;
const numberIsSafeInteger = Number.isSafeInteger;
const objectCreate = Object.create;
const objectDefineProperties = Object.defineProperties;
const objectDefineProperty = Object.defineProperty;
const objectFreeze = Object.freeze;
const objectGetOwnPropertyDescriptor = Object.getOwnPropertyDescriptor;
const objectGetPrototypeOf = Object.getPrototypeOf;
const objectHasOwn = Object.hasOwn;
const objectIsFrozen = Object.isFrozen;
const objectPrototype = Object.prototype;
const posixIsAbsolute = posixPath.isAbsolute;
const posixNormalize = posixPath.normalize;
const PromiseConstructor = Promise;
const reflectApply = Reflect.apply;
const reflectOwnKeys = Reflect.ownKeys;
const regexpExecIntrinsic = RegExp.prototype.exec;
const setAddIntrinsic = Set.prototype.add;
const setHasIntrinsic = Set.prototype.has;
const stringIncludesIntrinsic = String.prototype.includes;
const stringSplitIntrinsic = String.prototype.split;
const textDecoderDecodeIntrinsic = TextDecoder.prototype.decode;
const typedArrayPrototype = objectGetPrototypeOf(Uint8Array.prototype);
const typedArrayByteLengthGetter = objectGetOwnPropertyDescriptor(
  typedArrayPrototype,
  "byteLength",
).get;
const typedArraySetIntrinsic = objectGetOwnPropertyDescriptor(
  typedArrayPrototype,
  "set",
).value;
const {
  isPromise: isPromiseValue,
  isProxy: isProxyValue,
  isUint8Array: isUint8ArrayValue,
} = utilTypes;
const weakMapGetIntrinsic = WeakMap.prototype.get;
const weakMapSetIntrinsic = WeakMap.prototype.set;
const weakSetAddIntrinsic = WeakSet.prototype.add;
const weakSetHasIntrinsic = WeakSet.prototype.has;

function callIntrinsic(intrinsic, receiver, args) {
  return reflectApply(intrinsic, receiver, args);
}

function arrayEvery(value, callback) {
  return callIntrinsic(arrayEveryIntrinsic, value, [callback]);
}

function arrayIncludes(value, candidate) {
  return callIntrinsic(arrayIncludesIntrinsic, value, [candidate]);
}

function arrayMap(value, callback) {
  return callIntrinsic(arrayMapIntrinsic, value, [callback]);
}

function regexpTest(pattern, value) {
  return callIntrinsic(regexpExecIntrinsic, pattern, [value]) !== null;
}

function setAdd(value, entry) {
  return callIntrinsic(setAddIntrinsic, value, [entry]);
}

function setHas(value, entry) {
  return callIntrinsic(setHasIntrinsic, value, [entry]);
}

function stringIncludes(value, candidate) {
  return callIntrinsic(stringIncludesIntrinsic, value, [candidate]);
}

function stringSplit(value, separator) {
  return callIntrinsic(stringSplitIntrinsic, value, [separator]);
}

function typedArrayByteLength(value) {
  return callIntrinsic(typedArrayByteLengthGetter, value, []);
}

function typedArraySet(value, source) {
  return callIntrinsic(typedArraySetIntrinsic, value, [source]);
}

function weakMapGet(value, key) {
  return callIntrinsic(weakMapGetIntrinsic, value, [key]);
}

function weakMapSet(value, key, entry) {
  return callIntrinsic(weakMapSetIntrinsic, value, [key, entry]);
}

function weakSetAdd(value, entry) {
  return callIntrinsic(weakSetAddIntrinsic, value, [entry]);
}

function weakSetHas(value, entry) {
  return callIntrinsic(weakSetHasIntrinsic, value, [entry]);
}

function protectPromise(value) {
  if (!isPromiseValue(value)) return value;
  objectDefineProperty(value, "constructor", {
    configurable: false,
    enumerable: false,
    value: PromiseConstructor,
    writable: false,
  });
  return value;
}

export const PLATFORM_IMAGE_RESERVATION_CONTRACT_VERSION = 1;
export const MAX_PLATFORM_MANIFEST_BYTES = 4 * 1024 * 1024;
export const MAX_IMAGE_CONFIG_BYTES = 16 * 1024 * 1024;
export const MAX_IMAGE_JSON_NODES = 65_536;
export const MAX_IMAGE_JSON_OBJECT_MEMBERS = 32_768;
export const MAX_IMAGE_JSON_ARRAY_ELEMENTS = 32_768;
export const MAX_IMAGE_JSON_CONTAINER_ENTRIES = 4_096;
export const MAX_IMAGE_LAYER_COUNT = 2_048;
export const MAX_IMAGE_HISTORY_ENTRIES = 2_048;

const OCI_IMAGE_CONFIG_MEDIA_TYPE =
  "application/vnd.oci.image.config.v1+json";
const DOCKER_IMAGE_CONFIG_MEDIA_TYPE =
  "application/vnd.docker.container.image.v1+json";
const CONFIG_MEDIA_TYPE_BY_MANIFEST = Object.freeze({
  "application/vnd.docker.distribution.manifest.v2+json":
    DOCKER_IMAGE_CONFIG_MEDIA_TYPE,
  "application/vnd.oci.image.manifest.v1+json":
    OCI_IMAGE_CONFIG_MEDIA_TYPE,
});
const LAYER_MEDIA_TYPES_BY_MANIFEST = Object.freeze({
  "application/vnd.docker.distribution.manifest.v2+json": Object.freeze([
    "application/vnd.docker.image.rootfs.diff.tar.gzip",
    "application/vnd.docker.image.rootfs.foreign.diff.tar.gzip",
  ]),
  "application/vnd.oci.image.manifest.v1+json": Object.freeze([
    "application/vnd.oci.image.layer.v1.tar",
    "application/vnd.oci.image.layer.v1.tar+gzip",
    "application/vnd.oci.image.layer.v1.tar+zstd",
    "application/vnd.oci.image.layer.nondistributable.v1.tar",
    "application/vnd.oci.image.layer.nondistributable.v1.tar+gzip",
    "application/vnd.oci.image.layer.nondistributable.v1.tar+zstd",
  ]),
});
const OCI_DIGEST_PATTERN = /^sha256:[0-9a-f]{64}$/u;
const SHA256_HEX_PATTERN = /^[0-9a-f]{64}$/u;
const CODEX_VERSION_PATTERN = /^codex-cli [0-9]+\.[0-9]+\.[0-9]+$/u;
const BASE64_PATTERN =
  /^(?:[A-Za-z0-9+/]{4})*(?:[A-Za-z0-9+/]{2}==|[A-Za-z0-9+/]{3}=)?$/u;
const MEDIA_TYPE_PATTERN =
  /^[A-Za-z0-9](?:[A-Za-z0-9!#$&^_.+-]{0,125}[A-Za-z0-9])?\/[A-Za-z0-9](?:[A-Za-z0-9!#$&^_.+-]{0,125}[A-Za-z0-9])?$/u;
const URL_DISALLOWED_CHARACTER_PATTERN = /[\u0000-\u0020\u007F]/u;
const JSON_STRING_AT_PATTERN =
  /"(?:\\(?:["\\/bfnrt]|u[0-9a-fA-F]{4})|[^"\\\u0000-\u001F])*"/y;
const JSON_PRIMITIVE_AT_PATTERN =
  /(?:-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?|true|false|null)/y;
const MAX_JSON_NESTING_DEPTH = 32;

const ERROR_MESSAGES = Object.freeze({
  invalid_platform_image_request: "Platform image request is invalid",
  platform_image_identity_mismatch: "Platform image identity does not match",
  platform_image_inspection_uncertain: "Platform image inspection is uncertain",
  platform_image_reservation_rejected: "Platform image reservation was rejected",
});
const INTERNAL_ERRORS = new WeakSet();
const UTF8_DECODER = new TextDecoder("utf-8", { fatal: true });

export class PlatformImageReservationError extends Error {
  constructor(code) {
    if (typeof code !== "string" || !objectHasOwn(ERROR_MESSAGES, code)) {
      throw new TypeError("unsupported platform image reservation error code");
    }
    const message = ERROR_MESSAGES[code];
    super(message);
    objectDefineProperties(this, {
      name: {
        configurable: true,
        enumerable: true,
        value: "PlatformImageReservationError",
        writable: true,
      },
      code: {
        configurable: true,
        enumerable: true,
        value: code,
        writable: true,
      },
      retryable: {
        configurable: true,
        enumerable: true,
        value: false,
        writable: true,
      },
      stack: {
        configurable: false,
        enumerable: false,
        value: `PlatformImageReservationError: ${message}`,
        writable: false,
      },
    });
    objectFreeze(this);
  }
}

function makeError(code) {
  const error = new PlatformImageReservationError(code);
  weakSetAdd(INTERNAL_ERRORS, error);
  return error;
}

function fail(code) {
  throw makeError(code);
}

function ensure(condition, code) {
  if (!condition) fail(code);
}

function isInternalError(error, code) {
  return (
    error !== null &&
    typeof error === "object" &&
    weakSetHas(INTERNAL_ERRORS, error) &&
    (code === undefined || error.code === code)
  );
}

function assertExactPlainObject(value, keys, code) {
  if (
    isProxyValue(value) ||
    value === null ||
    typeof value !== "object" ||
    arrayIsArray(value)
  ) {
    fail(code);
  }

  let prototype;
  let actual;
  try {
    prototype = objectGetPrototypeOf(value);
    actual = reflectOwnKeys(value);
  } catch {
    fail(code);
  }
  ensure(
    prototype === objectPrototype || prototype === null,
    code,
  );
  ensure(
    actual.length === keys.length &&
      arrayEvery(
        actual,
        (key) => typeof key === "string" && arrayIncludes(keys, key),
      ),
    code,
  );

  const normalized = objectCreate(null);
  for (const key of actual) {
    let descriptor;
    try {
      descriptor = objectGetOwnPropertyDescriptor(value, key);
    } catch {
      fail(code);
    }
    ensure(
      descriptor?.enumerable === true &&
        objectHasOwn(descriptor, "value"),
      code,
    );
    normalized[key] = descriptor.value;
  }
  return normalized;
}

function assertPlainJsonObject(value, code) {
  if (
    value === null ||
    typeof value !== "object" ||
    arrayIsArray(value) ||
    objectGetPrototypeOf(value) !== objectPrototype
  ) {
    fail(code);
  }
  return value;
}

function assertAllowedJsonObject(value, allowedKeys, code) {
  const normalized = assertPlainJsonObject(value, code);
  ensure(
    arrayEvery(
      reflectOwnKeys(normalized),
      (key) => typeof key === "string" && arrayIncludes(allowedKeys, key),
    ),
    code,
  );
  return normalized;
}

function assertAnnotations(value, code) {
  const annotations = assertPlainJsonObject(value, code);
  ensure(
    arrayEvery(
      reflectOwnKeys(annotations),
      (key) =>
        typeof key === "string" &&
        key.length > 0 &&
        typeof annotations[key] === "string",
    ),
    code,
  );
}

function isAllowedDescriptorUrl(value) {
  if (
    typeof value !== "string" ||
    value.length === 0 ||
    value.length > 4096 ||
    regexpTest(URL_DISALLOWED_CHARACTER_PATTERN, value) ||
    stringIncludes(value, "#")
  ) {
    return false;
  }
  try {
    const parsed = new URL(value);
    return (
      parsed.protocol === "https:" &&
      parsed.hostname.length > 0 &&
      parsed.username === "" &&
      parsed.password === ""
    );
  } catch {
    return false;
  }
}

function assertOptionalDescriptorMetadata(descriptor, code) {
  if (objectHasOwn(descriptor, "urls")) {
    ensure(
      arrayIsArray(descriptor.urls) &&
        arrayEvery(descriptor.urls, isAllowedDescriptorUrl),
      code,
    );
  }
  if (objectHasOwn(descriptor, "annotations")) {
    assertAnnotations(descriptor.annotations, code);
  }
  if (objectHasOwn(descriptor, "artifactType")) {
    ensure(
      typeof descriptor.artifactType === "string" &&
        regexpTest(MEDIA_TYPE_PATTERN, descriptor.artifactType),
      code,
    );
  }
}

function decodeEmbeddedDescriptorData(descriptor, code) {
  if (!objectHasOwn(descriptor, "data")) return undefined;
  ensure(
    typeof descriptor.data === "string" &&
      descriptor.data.length > 0 &&
      regexpTest(BASE64_PATTERN, descriptor.data),
    code,
  );
  let decoded;
  try {
    decoded = bufferFrom(descriptor.data, "base64");
  } catch {
    fail(code);
  }
  ensure(
    decoded.byteLength === descriptor.size &&
      sha256Digest(decoded) === descriptor.digest,
    code,
  );
  return decoded;
}

function normalizeContentDescriptor(
  value,
  {
    allowedMediaTypes,
    expectedBytes = undefined,
    maximumSize = Number.MAX_SAFE_INTEGER,
  },
  code,
) {
  const descriptor = assertAllowedJsonObject(
    value,
    [
      "annotations",
      "artifactType",
      "data",
      "digest",
      "mediaType",
      "size",
      "urls",
    ],
    code,
  );
  ensure(
    typeof descriptor.mediaType === "string" &&
      arrayIncludes(allowedMediaTypes, descriptor.mediaType) &&
      typeof descriptor.digest === "string" &&
      regexpTest(OCI_DIGEST_PATTERN, descriptor.digest) &&
      numberIsSafeInteger(descriptor.size) &&
      descriptor.size > 0 &&
      descriptor.size <= maximumSize,
    code,
  );
  assertOptionalDescriptorMetadata(descriptor, code);
  const embedded = decodeEmbeddedDescriptorData(descriptor, code);
  if (expectedBytes !== undefined && embedded !== undefined) {
    ensure(bufferCompare(embedded, expectedBytes) === 0, code);
  }
  return {
    digest: descriptor.digest,
    mediaType: descriptor.mediaType,
    size: descriptor.size,
  };
}

function deepFreeze(value) {
  if (value !== null && typeof value === "object" && !objectIsFrozen(value)) {
    for (const key of reflectOwnKeys(value)) {
      const descriptor = objectGetOwnPropertyDescriptor(value, key);
      if (descriptor && objectHasOwn(descriptor, "value")) {
        deepFreeze(descriptor.value);
      }
    }
    objectFreeze(value);
  }
  return value;
}

function copyBoundedBytes(value, maximum, code) {
  if (
    isProxyValue(value) ||
    (!bufferIsBuffer(value) && !isUint8ArrayValue(value))
  ) {
    fail(code);
  }
  let sourceByteLength;
  try {
    sourceByteLength = typedArrayByteLength(value);
  } catch {
    fail(code);
  }
  ensure(
    sourceByteLength > 0 && sourceByteLength <= maximum,
    code,
  );

  let copy;
  try {
    copy = bufferAllocUnsafe(sourceByteLength);
    typedArraySet(copy, value);
  } catch {
    fail(code);
  }
  let copiedByteLength;
  let finalSourceByteLength;
  try {
    copiedByteLength = typedArrayByteLength(copy);
    finalSourceByteLength = typedArrayByteLength(value);
  } catch {
    fail(code);
  }
  ensure(
    copiedByteLength === sourceByteLength &&
      finalSourceByteLength === sourceByteLength &&
      copiedByteLength <= maximum,
    code,
  );
  return copy;
}

function sha256Digest(bytes) {
  return `sha256:${createHash("sha256").update(bytes).digest("hex")}`;
}

function assertNoDuplicateJsonObjectKeys(
  serialized,
  code,
  documentKind,
) {
  let index = 0;
  let arrayElements = 0;
  let nodes = 0;
  let objectMembers = 0;
  const skipWhitespace = () => {
    while (/\s/u.test(serialized[index] ?? "")) index += 1;
  };
  const parseString = () => {
    JSON_STRING_AT_PATTERN.lastIndex = index;
    const match = JSON_STRING_AT_PATTERN.exec(serialized);
    ensure(match !== null, code);
    index = JSON_STRING_AT_PATTERN.lastIndex;
    try {
      return jsonParse(match[0]);
    } catch {
      fail(code);
    }
  };
  const parseValue = (
    depth = 0,
    context = "root",
    maximumArrayEntries = MAX_IMAGE_JSON_CONTAINER_ENTRIES,
  ) => {
    nodes += 1;
    ensure(nodes <= MAX_IMAGE_JSON_NODES, code);
    skipWhitespace();
    if (serialized[index] === "{") {
      ensure(depth < MAX_JSON_NESTING_DEPTH, code);
      index += 1;
      skipWhitespace();
      const keys = new Set();
      let entries = 0;
      if (serialized[index] === "}") {
        index += 1;
        return;
      }
      while (true) {
        entries += 1;
        objectMembers += 1;
        ensure(
          entries <= MAX_IMAGE_JSON_CONTAINER_ENTRIES &&
            objectMembers <= MAX_IMAGE_JSON_OBJECT_MEMBERS,
          code,
        );
        skipWhitespace();
        const key = parseString();
        ensure(!setHas(keys, key), code);
        setAdd(keys, key);
        skipWhitespace();
        ensure(serialized[index] === ":", code);
        index += 1;
        let childContext = "other";
        let childMaximumArrayEntries =
          MAX_IMAGE_JSON_CONTAINER_ENTRIES;
        if (
          context === "root" &&
          documentKind === "manifest" &&
          key === "layers"
        ) {
          childMaximumArrayEntries = MAX_IMAGE_LAYER_COUNT;
        } else if (
          context === "root" &&
          documentKind === "config" &&
          key === "history"
        ) {
          childMaximumArrayEntries = MAX_IMAGE_HISTORY_ENTRIES;
        } else if (
          context === "root" &&
          documentKind === "config" &&
          key === "rootfs"
        ) {
          childContext = "config-rootfs";
        } else if (
          context === "config-rootfs" &&
          key === "diff_ids"
        ) {
          childMaximumArrayEntries = MAX_IMAGE_LAYER_COUNT;
        }
        parseValue(
          depth + 1,
          childContext,
          childMaximumArrayEntries,
        );
        skipWhitespace();
        if (serialized[index] === "}") {
          index += 1;
          return;
        }
        ensure(serialized[index] === ",", code);
        index += 1;
      }
    }
    if (serialized[index] === "[") {
      ensure(depth < MAX_JSON_NESTING_DEPTH, code);
      index += 1;
      skipWhitespace();
      let entries = 0;
      if (serialized[index] === "]") {
        index += 1;
        return;
      }
      while (true) {
        entries += 1;
        arrayElements += 1;
        ensure(
          entries <= maximumArrayEntries &&
            arrayElements <= MAX_IMAGE_JSON_ARRAY_ELEMENTS,
          code,
        );
        parseValue(depth + 1, "other");
        skipWhitespace();
        if (serialized[index] === "]") {
          index += 1;
          return;
        }
        ensure(serialized[index] === ",", code);
        index += 1;
      }
    }
    if (serialized[index] === '"') {
      parseString();
      return;
    }
    JSON_PRIMITIVE_AT_PATTERN.lastIndex = index;
    const match = JSON_PRIMITIVE_AT_PATTERN.exec(serialized);
    ensure(match !== null, code);
    index = JSON_PRIMITIVE_AT_PATTERN.lastIndex;
  };

  parseValue();
  skipWhitespace();
  ensure(index === serialized.length, code);
}

function parseBoundedJson(bytes, code, documentKind) {
  let serialized;
  try {
    serialized = callIntrinsic(textDecoderDecodeIntrinsic, UTF8_DECODER, [
      bytes,
    ]);
  } catch {
    fail(code);
  }
  assertNoDuplicateJsonObjectKeys(serialized, code, documentKind);
  try {
    return jsonParse(serialized);
  } catch {
    fail(code);
  }
}

function normalizeSessionRuntime(sessionManifest) {
  let manifest;
  try {
    manifest = assertSessionManifest(sessionManifest);
  } catch {
    fail("invalid_platform_image_request");
  }
  const [os, architecture, ...rest] = stringSplit(
    manifest.runtime.platform,
    "/",
  );
  ensure(
    rest.length === 0 &&
      os === "linux" &&
      arrayIncludes(["amd64", "arm64"], architecture),
    "invalid_platform_image_request",
  );
  return deepFreeze({
    architecture,
    codexSandbox: manifest.runtime.codexSandbox,
    codexVersion: manifest.runtime.codexVersion,
    digest: manifest.runtime.imageDigest,
    mediaType: manifest.runtime.imageMediaType,
    os,
    platform: manifest.runtime.platform,
  });
}

function normalizePlatformDescriptor(value) {
  const descriptor = assertExactPlainObject(
    value,
    ["bytes", "digest", "mediaType", "size"],
    "invalid_platform_image_request",
  );
  ensure(
    typeof descriptor.digest === "string" &&
      regexpTest(OCI_DIGEST_PATTERN, descriptor.digest),
    "invalid_platform_image_request",
  );
  ensure(
    typeof descriptor.mediaType === "string" &&
      arrayIncludes(PLATFORM_IMAGE_MEDIA_TYPES, descriptor.mediaType),
    "platform_image_identity_mismatch",
  );
  ensure(
    numberIsSafeInteger(descriptor.size) &&
      descriptor.size > 0 &&
      descriptor.size <= MAX_PLATFORM_MANIFEST_BYTES,
    "invalid_platform_image_request",
  );
  const bytes = copyBoundedBytes(
    descriptor.bytes,
    MAX_PLATFORM_MANIFEST_BYTES,
    "invalid_platform_image_request",
  );
  ensure(
    descriptor.size === bytes.byteLength &&
      descriptor.digest === sha256Digest(bytes),
    "platform_image_identity_mismatch",
  );
  return {
    bytes,
    digest: descriptor.digest,
    mediaType: descriptor.mediaType,
    size: descriptor.size,
  };
}

function normalizeManifestDocument(descriptor, configBytes) {
  const manifest = assertAllowedJsonObject(
    parseBoundedJson(
      descriptor.bytes,
      "platform_image_identity_mismatch",
      "manifest",
    ),
    ["annotations", "config", "layers", "mediaType", "schemaVersion"],
    "platform_image_identity_mismatch",
  );
  const isOci =
    descriptor.mediaType ===
    "application/vnd.oci.image.manifest.v1+json";
  ensure(
    manifest.schemaVersion === 2 &&
      arrayIsArray(manifest.layers) &&
      manifest.layers.length > 0 &&
      manifest.layers.length <= MAX_IMAGE_LAYER_COUNT &&
      !objectHasOwn(manifest, "manifests"),
    "platform_image_identity_mismatch",
  );
  ensure(
    isOci
      ? !objectHasOwn(manifest, "mediaType") ||
          manifest.mediaType === descriptor.mediaType
      : manifest.mediaType === descriptor.mediaType,
    "platform_image_identity_mismatch",
  );
  if (objectHasOwn(manifest, "annotations")) {
    assertAnnotations(
      manifest.annotations,
      "platform_image_identity_mismatch",
    );
  }
  const configCopy = copyBoundedBytes(
    configBytes,
    MAX_IMAGE_CONFIG_BYTES,
    "invalid_platform_image_request",
  );
  const config = normalizeContentDescriptor(
    manifest.config,
    {
      allowedMediaTypes: [
        CONFIG_MEDIA_TYPE_BY_MANIFEST[descriptor.mediaType],
      ],
      expectedBytes: configCopy,
      maximumSize: MAX_IMAGE_CONFIG_BYTES,
    },
    "platform_image_identity_mismatch",
  );
  const layers = arrayMap(manifest.layers, (layer) =>
    normalizeContentDescriptor(
      layer,
      {
        allowedMediaTypes:
          LAYER_MEDIA_TYPES_BY_MANIFEST[descriptor.mediaType],
      },
      "platform_image_identity_mismatch",
    ),
  );
  ensure(
    config.size === configCopy.byteLength &&
      config.digest === sha256Digest(configCopy),
    "platform_image_identity_mismatch",
  );
  const configDocument = assertPlainJsonObject(
    parseBoundedJson(
      configCopy,
      "platform_image_identity_mismatch",
      "config",
    ),
    "platform_image_identity_mismatch",
  );
  if (objectHasOwn(configDocument, "history")) {
    ensure(
      arrayIsArray(configDocument.history) &&
        configDocument.history.length <=
          MAX_IMAGE_HISTORY_ENTRIES,
      "platform_image_identity_mismatch",
    );
  }
  const rootfs = assertAllowedJsonObject(
    configDocument.rootfs,
    ["diff_ids", "type"],
    "platform_image_identity_mismatch",
  );
  ensure(
    typeof configDocument.os === "string" &&
      typeof configDocument.architecture === "string" &&
      rootfs.type === "layers" &&
      arrayIsArray(rootfs.diff_ids) &&
      rootfs.diff_ids.length <= MAX_IMAGE_LAYER_COUNT &&
      rootfs.diff_ids.length === layers.length &&
      arrayEvery(
        rootfs.diff_ids,
        (diffId) =>
          typeof diffId === "string" &&
          regexpTest(OCI_DIGEST_PATTERN, diffId),
      ),
    "platform_image_identity_mismatch",
  );
  return {
    architecture: configDocument.architecture,
    config: {
      digest: config.digest,
      mediaType: config.mediaType,
      size: config.size,
    },
    os: configDocument.os,
  };
}

function projectionFromEvidence(descriptor, document, expectedRuntime) {
  ensure(
    descriptor.digest === expectedRuntime.digest &&
      descriptor.mediaType === expectedRuntime.mediaType &&
      document.os === expectedRuntime.os &&
      document.architecture === expectedRuntime.architecture,
    "platform_image_identity_mismatch",
  );
  return deepFreeze({
    platformImage: {
      architecture: document.architecture,
      config: {
        digest: document.config.digest,
        mediaType: document.config.mediaType,
        size: document.config.size,
      },
      digest: descriptor.digest,
      mediaType: descriptor.mediaType,
      os: document.os,
      size: descriptor.size,
    },
    codexSandbox: expectedRuntime.codexSandbox,
    codexVersion: expectedRuntime.codexVersion,
  });
}

function normalizeInspector(inspectCodex) {
  ensure(
    typeof inspectCodex === "function" && !isProxyValue(inspectCodex),
    "invalid_platform_image_request",
  );
  return inspectCodex;
}

function normalizeMeasurement(value) {
  const measurement = assertExactPlainObject(
    value,
    ["codexBinaryPath", "codexBinarySha256", "codexVersion"],
    "platform_image_inspection_uncertain",
  );
  ensure(
    typeof measurement.codexBinarySha256 === "string" &&
      regexpTest(SHA256_HEX_PATTERN, measurement.codexBinarySha256) &&
      typeof measurement.codexVersion === "string" &&
      measurement.codexVersion.length <= 128 &&
      regexpTest(CODEX_VERSION_PATTERN, measurement.codexVersion) &&
      typeof measurement.codexBinaryPath === "string" &&
      measurement.codexBinaryPath.length > 1 &&
      measurement.codexBinaryPath.length <= 4096 &&
      !stringIncludes(measurement.codexBinaryPath, "\0") &&
      callIntrinsic(posixIsAbsolute, posixPath, [
        measurement.codexBinaryPath,
      ]) &&
      callIntrinsic(posixNormalize, posixPath, [
        measurement.codexBinaryPath,
      ]) ===
        measurement.codexBinaryPath,
    "platform_image_inspection_uncertain",
  );
  return measurement;
}

async function inspectRuntime(inspectCodex, projection) {
  const inspectionRequest = deepFreeze({
    codexSandbox: projection.codexSandbox,
    codexVersion: projection.codexVersion,
    platformImage: projection.platformImage,
  });
  let rawMeasurement;
  try {
    rawMeasurement = await protectPromise(
      reflectApply(inspectCodex, undefined, [inspectionRequest]),
    );
  } catch {
    fail("platform_image_inspection_uncertain");
  }
  let measurement;
  try {
    measurement = normalizeMeasurement(rawMeasurement);
  } catch (error) {
    if (isInternalError(error, "platform_image_inspection_uncertain")) {
      throw error;
    }
    fail("platform_image_inspection_uncertain");
  }
  ensure(
    measurement.codexVersion === projection.codexVersion,
    "platform_image_identity_mismatch",
  );
  return deepFreeze({
    codexBinaryPath: measurement.codexBinaryPath,
    codexBinarySha256: measurement.codexBinarySha256,
    codexVersion: measurement.codexVersion,
    platformImageDigest: projection.platformImage.digest,
  });
}

function sameProjection(left, right) {
  return (
    left.codexSandbox === right.codexSandbox &&
    left.codexVersion === right.codexVersion &&
    left.platformImage.architecture === right.platformImage.architecture &&
    left.platformImage.digest === right.platformImage.digest &&
    left.platformImage.mediaType === right.platformImage.mediaType &&
    left.platformImage.os === right.platformImage.os &&
    left.platformImage.size === right.platformImage.size &&
    left.platformImage.config.digest === right.platformImage.config.digest &&
    left.platformImage.config.mediaType ===
      right.platformImage.config.mediaType &&
    left.platformImage.config.size === right.platformImage.config.size
  );
}

function sameRuntimeIdentity(left, right) {
  return (
    left.codexBinaryPath === right.codexBinaryPath &&
    left.codexBinarySha256 === right.codexBinarySha256 &&
    left.codexVersion === right.codexVersion &&
    left.platformImageDigest === right.platformImageDigest
  );
}

async function verifyEvidence({
  configBytes,
  descriptor: descriptorValue,
  expectedRuntime,
  inspectCodex,
}) {
  const descriptor = normalizePlatformDescriptor(descriptorValue);
  const document = normalizeManifestDocument(descriptor, configBytes);
  const projection = projectionFromEvidence(
    descriptor,
    document,
    expectedRuntime,
  );
  const runtimeIdentity = await protectPromise(
    inspectRuntime(inspectCodex, projection),
  );
  return { projection, runtimeIdentity };
}

function makeOpaqueReservation() {
  return objectFreeze(objectCreate(null));
}

function consumptionProjection(record) {
  return deepFreeze({
    projection: record.projection,
    runtimeIdentity: record.runtimeIdentity,
  });
}

/**
 * Verifies caller-supplied OCI bytes and a trusted same-image inspection.
 *
 * This coordinator does not fetch registry content, verify image signatures or
 * publisher trust, pin a container-runtime object, or launch a container.
 */
export class PlatformImageReservationCoordinator {
  #reservations = new WeakMap();

  reservePlatformImage(options) {
    return protectPromise(this.#reservePlatformImage(options));
  }

  async #reservePlatformImage(options) {
    const normalized = assertExactPlainObject(
      options,
      ["configBytes", "descriptor", "inspectCodex", "sessionManifest"],
      "invalid_platform_image_request",
    );
    const expectedRuntime = normalizeSessionRuntime(
      normalized.sessionManifest,
    );
    const inspectCodex = normalizeInspector(normalized.inspectCodex);
    const verified = await protectPromise(
      verifyEvidence({
        configBytes: normalized.configBytes,
        descriptor: normalized.descriptor,
        expectedRuntime,
        inspectCodex,
      }),
    );
    const reservation = makeOpaqueReservation();
    const record = {
      expectedRuntime,
      inspectCodex,
      projection: verified.projection,
      reservation,
      runtimeIdentity: verified.runtimeIdentity,
      state: "issued",
    };
    weakMapSet(this.#reservations, reservation, record);
    return deepFreeze({
      projection: record.projection,
      reservation,
      runtimeIdentity: record.runtimeIdentity,
    });
  }

  async #revalidate(normalized, consume) {
    const record = weakMapGet(this.#reservations, normalized.reservation);
    if (record?.state === "revalidating") {
      record.state = "revoked";
      fail("platform_image_reservation_rejected");
    }
    if (record === undefined || record.state !== "issued") {
      fail("platform_image_reservation_rejected");
    }
    if (normalized.inspectCodex !== record.inspectCodex) {
      record.state = "revoked";
      fail("platform_image_reservation_rejected");
    }

    record.state = "revalidating";
    try {
      const verified = await protectPromise(
        verifyEvidence({
          configBytes: normalized.configBytes,
          descriptor: normalized.descriptor,
          expectedRuntime: record.expectedRuntime,
          inspectCodex: record.inspectCodex,
        }),
      );
      ensure(
        record.state === "revalidating" &&
          sameProjection(verified.projection, record.projection) &&
          sameRuntimeIdentity(
            verified.runtimeIdentity,
            record.runtimeIdentity,
          ),
        "platform_image_reservation_rejected",
      );
      record.state = consume ? "consumed" : "issued";
      return consumptionProjection(record);
    } catch (error) {
      record.state = "revoked";
      if (
        isInternalError(
          error,
          "platform_image_inspection_uncertain",
        )
      ) {
        throw error;
      }
      throw makeError("platform_image_reservation_rejected");
    }
  }

  revalidateReservation(options) {
    return protectPromise(this.#useReservation(options, false));
  }

  consumeReservation(options) {
    return protectPromise(this.#useReservation(options, true));
  }

  async #useReservation(options, consume) {
    const normalized = assertExactPlainObject(
      options,
      ["configBytes", "descriptor", "inspectCodex", "reservation"],
      "invalid_platform_image_request",
    );
    return await protectPromise(
      this.#revalidate(normalized, consume),
    );
  }
}
