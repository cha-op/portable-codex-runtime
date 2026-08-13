import { isAbsolute, parse, resolve } from "node:path";
import { types as utilTypes } from "node:util";

const arrayIncludesIntrinsic = Array.prototype.includes;
const arrayIsArray = Array.isArray;
const bigIntIntrinsic = BigInt;
const isProxyValue = utilTypes.isProxy;
const numberIsSafeInteger = Number.isSafeInteger;
const objectCreate = Object.create;
const objectFreeze = Object.freeze;
const objectGetOwnPropertyDescriptor = Object.getOwnPropertyDescriptor;
const objectGetPrototypeOf = Object.getPrototypeOf;
const objectHasOwn = Object.hasOwn;
const objectIsFrozen = Object.isFrozen;
const objectPrototype = Object.prototype;
const objectValues = Object.values;
const reflectApply = Reflect.apply;
const reflectOwnKeys = Reflect.ownKeys;
const regexpExecIntrinsic = RegExp.prototype.exec;
const stringCharCodeAtIntrinsic = String.prototype.charCodeAt;
const structuredCloneIntrinsic = globalThis.structuredClone;

export const SESSION_MANIFEST_SCHEMA_VERSION = 1;
export const SESSION_LAYOUT_VERSION = 1;
export const STORAGE_CONTRACT_VERSION = 1;
export const CHECKPOINT_CAPTURE_RECONCILIATION_CONTRACT_VERSION = 1;
export const PREPARED_CHECKPOINT_CAPTURE_CONTRACT_VERSION = 1;
export const RESTORE_ATTACHMENT_ACTIVATION_CONTRACT_VERSION = 1;
export const RESTORE_ATTACHMENT_RECONCILIATION_CONTRACT_VERSION = 1;
export const SESSION_WORKER_ROOT = "/session";
export const SESSION_WORKER_LAYOUT = deepFreeze({
  codexHome: "/session/codex-home",
  runtimeState: "/session/.portable-runtime",
  workspace: "/session/workspace",
});
export const SESSION_AUTH_MODE = "external-chatgpt-access-token";
export const PLATFORM_IMAGE_MEDIA_TYPES = Object.freeze([
  "application/vnd.oci.image.manifest.v1+json",
  "application/vnd.docker.distribution.manifest.v2+json",
]);
export const DEFAULT_MAX_SUBAGENTS = 6;
export const MAX_SUBAGENTS = 10;
export const MAX_AGENT_DEPTH = 2;
export const DEFAULT_AGENT_POLICY = deepFreeze({
  defaultMaxSubagents: DEFAULT_MAX_SUBAGENTS,
  maxDepth: MAX_AGENT_DEPTH,
  maxSubagents: MAX_SUBAGENTS,
});
export const CHECKPOINT_CLASSES = Object.freeze([
  "clean",
  "graceful-abort",
  "crash-prefix",
]);
export const CHECKPOINT_CLASS_POLICIES = deepFreeze({
  clean: {
    captureBoundary: "storage-barrier",
    explicitAbortMarker: "not-required",
    requiresTailRepair: false,
    writerBoundary: "stopped",
    writableResume: "after-new-lease",
  },
  "graceful-abort": {
    captureBoundary: "storage-barrier",
    explicitAbortMarker: "required",
    requiresTailRepair: false,
    writerBoundary: "stopped",
    writableResume: "after-new-lease",
  },
  "crash-prefix": {
    captureBoundary: "atomic-crash-capture",
    explicitAbortMarker: "must-not-infer",
    requiresTailRepair: true,
    writerBoundary: "stopped-or-fenced",
    writableResume: "after-tail-repair-and-new-lease",
  },
});

const UINT64_MAX = 18_446_744_073_709_551_615n;
const MAX_JSON_NESTING_DEPTH = 16;
const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u;
const OCI_DIGEST_PATTERN = /^sha256:[0-9a-f]{64}$/u;
const CODEX_VERSION_PATTERN = /^codex-cli [0-9]+\.[0-9]+\.[0-9]+$/u;
const JSON_STRING_AT_PATTERN =
  /"(?:\\(?:["\\/bfnrt]|u[0-9a-fA-F]{4})|[^"\\\u0000-\u001F])*"/y;
const JSON_PRIMITIVE_AT_PATTERN =
  /(?:-?(?:0|[1-9][0-9]*)(?:\.[0-9]+)?(?:[eE][+-]?[0-9]+)?|true|false|null)/y;
const STORAGE_BACKEND_METHODS = Object.freeze([
  "captureCheckpoint",
  "destroySession",
  "detachAttachment",
  "forceFence",
  "prepareWritableAttachment",
  "provisionSession",
  "restoreCheckpoint",
]);
const CHECKPOINT_BACKEND_METHODS = Object.freeze([
  "captureCheckpoint",
  "restoreCheckpoint",
]);
const MAX_BACKEND_PROTOTYPE_DEPTH = 64;

export class SessionStorageContractError extends Error {
  constructor(code, message) {
    super(message);
    this.name = "SessionStorageContractError";
    this.code = code;
  }
}

function fail(code, message) {
  throw new SessionStorageContractError(code, message);
}

function ensure(condition, code, message) {
  if (!condition) fail(code, message);
}

function deepFreeze(value) {
  if (value && typeof value === "object" && !objectIsFrozen(value)) {
    objectFreeze(value);
    const children = objectValues(value);
    for (let index = 0; index < children.length; index += 1) {
      deepFreeze(children[index]);
    }
  }
  return value;
}

function deepFreezeManifest(value) {
  return deepFreeze(value);
}

function defensiveClone(value, code, label) {
  try {
    return reflectApply(structuredCloneIntrinsic, globalThis, [value]);
  } catch {
    fail(code, `${label} must contain cloneable data`);
  }
}

function inspectPlainDataObject(value, code, label) {
  if (
    isProxyValue(value) ||
    value === null ||
    typeof value !== "object" ||
    arrayIsArray(value)
  ) {
    fail(code, `${label} must be a plain object`);
  }
  let prototype;
  let actual;
  try {
    prototype = objectGetPrototypeOf(value);
    actual = reflectOwnKeys(value);
  } catch {
    fail(code, `${label} must be a plain object`);
  }
  ensure(
    prototype === objectPrototype || prototype === null,
    code,
    `${label} must be a plain object`,
  );
  return actual;
}

function plainDataDescriptor(value, key, code, label) {
  let descriptor;
  try {
    descriptor = objectGetOwnPropertyDescriptor(value, key);
  } catch {
    fail(code, `${label} fields must be enumerable plain data properties`);
  }
  ensure(
    descriptor?.enumerable === true && objectHasOwn(descriptor, "value"),
    code,
    `${label} fields must be enumerable plain data properties`,
  );
  return descriptor;
}

function assertExactObject(value, keys, code, label) {
  const actual = inspectPlainDataObject(value, code, label);
  let exact = actual.length === keys.length;
  for (let index = 0; exact && index < actual.length; index += 1) {
    const key = actual[index];
    exact =
      typeof key === "string" &&
      reflectApply(arrayIncludesIntrinsic, keys, [key]);
  }
  ensure(exact, code, `${label} contains unexpected or missing fields`);
  for (let index = 0; index < actual.length; index += 1) {
    plainDataDescriptor(value, actual[index], code, label);
  }
}

function assertOptionsObject(value, allowedKeys, requiredKeys, code, label) {
  const actual = inspectPlainDataObject(value, code, label);
  let exact = true;
  for (let index = 0; exact && index < actual.length; index += 1) {
    const key = actual[index];
    exact =
      typeof key === "string" &&
      reflectApply(arrayIncludesIntrinsic, allowedKeys, [key]);
  }
  for (let index = 0; exact && index < requiredKeys.length; index += 1) {
    exact = reflectApply(arrayIncludesIntrinsic, actual, [
      requiredKeys[index],
    ]);
  }
  ensure(exact, code, `${label} contains unexpected or missing fields`);
  const normalized = objectCreate(null);
  for (let index = 0; index < actual.length; index += 1) {
    const key = actual[index];
    const descriptor = plainDataDescriptor(value, key, code, label);
    normalized[key] = descriptor.value;
  }
  return normalized;
}

function assertUuid(value, code, label) {
  ensure(
    isCanonicalUuid(value),
    code,
    `${label} must be a UUID`,
  );
}

function assertAttachmentRootPath(value, code, label) {
  ensure(
    typeof value === "string" &&
      !containsNullCharacter(value) &&
      isAbsolute(value) &&
      resolve(value) === value &&
      value !== parse(value).root,
    code,
    `${label} must be an absolute host-local directory path`,
  );
}

function assertOpaqueId(value, code, label) {
  ensure(
    isOpaqueId(value),
    code,
    `${label} must be an opaque identifier`,
  );
}

function stringCharCodeAt(value, index) {
  return reflectApply(stringCharCodeAtIntrinsic, value, [index]);
}

function containsNullCharacter(value) {
  for (let index = 0; index < value.length; index += 1) {
    if (stringCharCodeAt(value, index) === 0) return true;
  }
  return false;
}

function isAsciiAlphaNumeric(code) {
  return (
    (code >= 48 && code <= 57) ||
    (code >= 65 && code <= 90) ||
    (code >= 97 && code <= 122)
  );
}

function isLowerHex(code) {
  return (
    (code >= 48 && code <= 57) ||
    (code >= 97 && code <= 102)
  );
}

function isCanonicalUuid(value) {
  if (typeof value !== "string" || value.length !== 36) return false;
  for (let index = 0; index < value.length; index += 1) {
    const code = stringCharCodeAt(value, index);
    if (index === 8 || index === 13 || index === 18 || index === 23) {
      if (code !== 45) return false;
    } else if (!isLowerHex(code)) {
      return false;
    }
  }
  const version = stringCharCodeAt(value, 14);
  const variant = stringCharCodeAt(value, 19);
  return (
    version >= 49 &&
    version <= 56 &&
    (variant === 56 ||
      variant === 57 ||
      variant === 97 ||
      variant === 98)
  );
}

function isOpaqueId(value) {
  if (
    typeof value !== "string" ||
    value.length < 1 ||
    value.length > 128 ||
    !isAsciiAlphaNumeric(stringCharCodeAt(value, 0))
  ) {
    return false;
  }
  for (let index = 1; index < value.length; index += 1) {
    const code = stringCharCodeAt(value, index);
    if (
      !isAsciiAlphaNumeric(code) &&
      code !== 45 &&
      code !== 46 &&
      code !== 58 &&
      code !== 95
    ) {
      return false;
    }
  }
  return true;
}

function assertIsoTimestamp(value, code, label) {
  ensure(typeof value === "string", code, `${label} must be an ISO timestamp`);
  const timestamp = Date.parse(value);
  ensure(
    Number.isFinite(timestamp) && new Date(timestamp).toISOString() === value,
    code,
    `${label} must be a canonical ISO timestamp`,
  );
  return timestamp;
}

function assertNoDuplicateJsonObjectKeys(serialized) {
  let index = 0;
  const skipWhitespace = () => {
    while (/\s/u.test(serialized[index] ?? "")) index += 1;
  };
  const parseString = () => {
    JSON_STRING_AT_PATTERN.lastIndex = index;
    const match = JSON_STRING_AT_PATTERN.exec(serialized);
    ensure(match, "invalid_session_manifest", "session manifest contains invalid JSON");
    index = JSON_STRING_AT_PATTERN.lastIndex;
    return JSON.parse(match[0]);
  };
  const parseValue = (depth = 0) => {
    skipWhitespace();
    if (serialized[index] === "{") {
      ensure(
        depth < MAX_JSON_NESTING_DEPTH,
        "invalid_session_manifest",
        "session manifest exceeds the maximum JSON nesting depth",
      );
      index += 1;
      skipWhitespace();
      const keys = new Set();
      if (serialized[index] === "}") {
        index += 1;
        return;
      }
      while (true) {
        skipWhitespace();
        const key = parseString();
        ensure(
          !keys.has(key),
          "invalid_session_manifest",
          "session manifest contains duplicate object keys",
        );
        keys.add(key);
        skipWhitespace();
        ensure(
          serialized[index] === ":",
          "invalid_session_manifest",
          "session manifest contains invalid JSON",
        );
        index += 1;
        parseValue(depth + 1);
        skipWhitespace();
        if (serialized[index] === "}") {
          index += 1;
          return;
        }
        ensure(
          serialized[index] === ",",
          "invalid_session_manifest",
          "session manifest contains invalid JSON",
        );
        index += 1;
      }
    }
    if (serialized[index] === "[") {
      ensure(
        depth < MAX_JSON_NESTING_DEPTH,
        "invalid_session_manifest",
        "session manifest exceeds the maximum JSON nesting depth",
      );
      index += 1;
      skipWhitespace();
      if (serialized[index] === "]") {
        index += 1;
        return;
      }
      while (true) {
        parseValue(depth + 1);
        skipWhitespace();
        if (serialized[index] === "]") {
          index += 1;
          return;
        }
        ensure(
          serialized[index] === ",",
          "invalid_session_manifest",
          "session manifest contains invalid JSON",
        );
        index += 1;
      }
    }
    if (serialized[index] === '"') {
      parseString();
      return;
    }
    JSON_PRIMITIVE_AT_PATTERN.lastIndex = index;
    const match = JSON_PRIMITIVE_AT_PATTERN.exec(serialized);
    ensure(match, "invalid_session_manifest", "session manifest contains invalid JSON");
    index = JSON_PRIMITIVE_AT_PATTERN.lastIndex;
  };

  parseValue();
  skipWhitespace();
  ensure(
    index === serialized.length,
    "invalid_session_manifest",
    "session manifest contains trailing JSON data",
  );
}

function inspectManifestPlainDataObject(value, code, label) {
  if (
    isProxyValue(value) ||
    value === null ||
    typeof value !== "object" ||
    arrayIsArray(value)
  ) {
    fail(code, `${label} must be a plain object`);
  }
  let prototype;
  let actual;
  try {
    prototype = objectGetPrototypeOf(value);
    actual = reflectOwnKeys(value);
  } catch {
    fail(code, `${label} must be a plain object`);
  }
  ensure(
    prototype === objectPrototype || prototype === null,
    code,
    `${label} must be a plain object`,
  );
  return actual;
}

function manifestPlainDataDescriptor(value, key, code, label) {
  let descriptor;
  try {
    descriptor = objectGetOwnPropertyDescriptor(value, key);
  } catch {
    fail(code, `${label} fields must be enumerable plain data properties`);
  }
  ensure(
    descriptor?.enumerable === true && objectHasOwn(descriptor, "value"),
    code,
    `${label} fields must be enumerable plain data properties`,
  );
  return descriptor;
}

function assertManifestExactObject(value, keys, code, label) {
  const actual = inspectManifestPlainDataObject(value, code, label);
  let exact = actual.length === keys.length;
  for (let index = 0; exact && index < actual.length; index += 1) {
    const key = actual[index];
    exact =
      typeof key === "string" &&
      reflectApply(arrayIncludesIntrinsic, keys, [key]);
  }
  ensure(exact, code, `${label} contains unexpected or missing fields`);
  for (let index = 0; index < actual.length; index += 1) {
    manifestPlainDataDescriptor(value, actual[index], code, label);
  }
}

function assertManifestUuid(value, label) {
  ensure(
    typeof value === "string" &&
      reflectApply(regexpExecIntrinsic, UUID_PATTERN, [value]) !== null,
    "invalid_session_manifest",
    `${label} must be a UUID`,
  );
}

function assertAgentPolicy(value) {
  assertManifestExactObject(
    value,
    ["defaultMaxSubagents", "maxDepth", "maxSubagents"],
    "invalid_session_manifest",
    "session agent policy",
  );
  ensure(
    numberIsSafeInteger(value.maxSubagents) &&
      value.maxSubagents === MAX_SUBAGENTS,
    "invalid_session_manifest",
    "session subagent hard limit is unsupported",
  );
  ensure(
    numberIsSafeInteger(value.maxDepth) &&
      value.maxDepth === MAX_AGENT_DEPTH,
    "invalid_session_manifest",
    "session agent depth limit is unsupported",
  );
  ensure(
    numberIsSafeInteger(value.defaultMaxSubagents) &&
      value.defaultMaxSubagents >= 1 &&
      value.defaultMaxSubagents <= value.maxSubagents,
    "invalid_session_manifest",
    "default subagent limit is invalid",
  );
}

export function assertSessionManifest(value) {
  assertManifestExactObject(
    value,
    ["agents", "authMode", "codex", "layoutVersion", "runtime", "schemaVersion", "sessionId"],
    "invalid_session_manifest",
    "session manifest",
  );
  ensure(
    value.schemaVersion === SESSION_MANIFEST_SCHEMA_VERSION,
    "unsupported_manifest_version",
    "session manifest schema version is unsupported",
  );
  assertManifestUuid(value.sessionId, "runtime session ID");
  assertManifestExactObject(
    value.codex,
    ["ephemeral", "historyMode", "rootThreadId", "sessionId"],
    "invalid_session_manifest",
    "Codex session binding",
  );
  assertManifestUuid(value.codex.rootThreadId, "Codex root thread ID");
  assertManifestUuid(value.codex.sessionId, "Codex session-tree ID");
  ensure(
    value.codex.sessionId === value.codex.rootThreadId,
    "invalid_session_manifest",
    "root Codex session-tree ID must equal its thread ID",
  );
  ensure(value.codex.ephemeral === false, "invalid_session_manifest", "Codex thread must persist");
  ensure(
    reflectApply(arrayIncludesIntrinsic, ["legacy", "paginated"], [
      value.codex.historyMode,
    ]),
    "invalid_session_manifest",
    "Codex history mode is unsupported",
  );
  assertManifestExactObject(
    value.runtime,
    ["codexSandbox", "codexVersion", "imageDigest", "imageMediaType", "platform"],
    "invalid_session_manifest",
    "session runtime",
  );
  ensure(
    typeof value.runtime.imageDigest === "string" &&
      reflectApply(regexpExecIntrinsic, OCI_DIGEST_PATTERN, [
        value.runtime.imageDigest,
      ]) !== null,
    "invalid_session_manifest",
    "runtime image must use a concrete lowercase sha256 digest",
  );
  ensure(
    reflectApply(arrayIncludesIntrinsic, PLATFORM_IMAGE_MEDIA_TYPES, [
      value.runtime.imageMediaType,
    ]),
    "invalid_session_manifest",
    "runtime image media type must describe a platform manifest",
  );
  ensure(
    reflectApply(arrayIncludesIntrinsic, ["linux/amd64", "linux/arm64"], [
      value.runtime.platform,
    ]),
    "invalid_session_manifest",
    "runtime platform is unsupported",
  );
  ensure(
    typeof value.runtime.codexVersion === "string" &&
      value.runtime.codexVersion.length <= 128 &&
      reflectApply(regexpExecIntrinsic, CODEX_VERSION_PATTERN, [
        value.runtime.codexVersion,
      ]) !== null,
    "invalid_session_manifest",
    "Codex version is invalid",
  );
  ensure(
    value.runtime.codexSandbox === "danger-full-access",
    "invalid_session_manifest",
    "Codex sandbox contract is unsupported",
  );
  ensure(
    value.layoutVersion === SESSION_LAYOUT_VERSION,
    "invalid_session_manifest",
    "session layout version is unsupported",
  );
  ensure(
    value.authMode === SESSION_AUTH_MODE,
    "invalid_session_manifest",
    "session auth mode is unsupported",
  );
  assertAgentPolicy(value.agents);
  return deepFreezeManifest(
    defensiveClone(value, "invalid_session_manifest", "session manifest"),
  );
}

export function createSessionManifest(input) {
  assertExactObject(
    input,
    ["codex", "runtime", "sessionId"],
    "invalid_session_manifest",
    "session manifest input",
  );
  return assertSessionManifest({
    agents: DEFAULT_AGENT_POLICY,
    authMode: SESSION_AUTH_MODE,
    codex: input.codex,
    layoutVersion: SESSION_LAYOUT_VERSION,
    runtime: input.runtime,
    schemaVersion: SESSION_MANIFEST_SCHEMA_VERSION,
    sessionId: input.sessionId,
  });
}

export function parseSessionManifest(serialized) {
  ensure(
    typeof serialized === "string" && Buffer.byteLength(serialized, "utf8") <= 64 * 1024,
    "invalid_session_manifest",
    "session manifest must be bounded JSON text",
  );
  assertNoDuplicateJsonObjectKeys(serialized);
  let parsed;
  try {
    parsed = JSON.parse(serialized);
  } catch {
    fail("invalid_session_manifest", "session manifest contains invalid JSON");
  }
  return assertSessionManifest(parsed);
}

export function serializeSessionManifest(manifest) {
  const value = assertSessionManifest(manifest);
  const canonical = {
    schemaVersion: value.schemaVersion,
    sessionId: value.sessionId,
    codex: {
      rootThreadId: value.codex.rootThreadId,
      sessionId: value.codex.sessionId,
      ephemeral: value.codex.ephemeral,
      historyMode: value.codex.historyMode,
    },
    runtime: {
      imageDigest: value.runtime.imageDigest,
      imageMediaType: value.runtime.imageMediaType,
      platform: value.runtime.platform,
      codexVersion: value.runtime.codexVersion,
      codexSandbox: value.runtime.codexSandbox,
    },
    layoutVersion: value.layoutVersion,
    authMode: value.authMode,
    agents: {
      defaultMaxSubagents: value.agents.defaultMaxSubagents,
      maxSubagents: value.agents.maxSubagents,
      maxDepth: value.agents.maxDepth,
    },
  };
  return `${JSON.stringify(canonical, null, 2)}\n`;
}

/**
 * Structural comparison only. The caller must obtain `resolution` from a
 * trusted runtime probe that inspected the descriptor and image configuration
 * and measured the Codex version from the exact image.
 */
export function assertResolvedPlatformImageMatchesManifest(options) {
  const { manifest, resolution } = assertOptionsObject(
    options,
    ["manifest", "resolution"],
    ["manifest", "resolution"],
    "invalid_image_resolution",
    "platform image resolution options",
  );
  const sessionManifest = assertSessionManifest(manifest);
  assertExactObject(
    resolution,
    ["codexVersion", "digest", "mediaType", "platform"],
    "invalid_image_resolution",
    "resolved platform image",
  );
  ensure(
    typeof resolution.digest === "string" && OCI_DIGEST_PATTERN.test(resolution.digest),
    "invalid_image_resolution",
    "resolved image digest is invalid",
  );
  ensure(
    PLATFORM_IMAGE_MEDIA_TYPES.includes(resolution.mediaType),
    "invalid_image_resolution",
    "resolved image is not a platform manifest",
  );
  ensure(
    ["linux/amd64", "linux/arm64"].includes(resolution.platform),
    "invalid_image_resolution",
    "resolved image platform is unsupported",
  );
  ensure(
    typeof resolution.codexVersion === "string" &&
      resolution.codexVersion.length <= 128 &&
      CODEX_VERSION_PATTERN.test(resolution.codexVersion),
    "invalid_image_resolution",
    "resolved Codex version is invalid",
  );
  ensure(
    resolution.digest === sessionManifest.runtime.imageDigest &&
      resolution.mediaType === sessionManifest.runtime.imageMediaType &&
      resolution.platform === sessionManifest.runtime.platform &&
      resolution.codexVersion === sessionManifest.runtime.codexVersion,
    "invalid_image_resolution",
    "resolved platform image does not match the session manifest",
  );
  return deepFreeze(
    defensiveClone(resolution, "invalid_image_resolution", "resolved platform image"),
  );
}

function parseFencingEpochForRecord(value, code) {
  ensure(
    typeof value === "string" &&
      value.length >= 1 &&
      value.length <= 20 &&
      stringCharCodeAt(value, 0) >= 49 &&
      stringCharCodeAt(value, 0) <= 57,
    code,
    "fencing epoch must be a canonical positive decimal string",
  );
  for (let index = 1; index < value.length; index += 1) {
    const codePoint = stringCharCodeAt(value, index);
    ensure(
      codePoint >= 48 && codePoint <= 57,
      code,
      "fencing epoch must be a canonical positive decimal string",
    );
  }
  const epoch = bigIntIntrinsic(value);
  ensure(epoch <= UINT64_MAX, code, "fencing epoch exceeds uint64");
  return epoch;
}

export function parseFencingEpoch(value) {
  return parseFencingEpochForRecord(value, "invalid_fence");
}

export function compareFencingEpochs(left, right) {
  const leftEpoch = parseFencingEpoch(left);
  const rightEpoch = parseFencingEpoch(right);
  return leftEpoch < rightEpoch ? -1 : leftEpoch > rightEpoch ? 1 : 0;
}

export function assertLeaseGrant(value) {
  assertExactObject(
    value,
    ["contractVersion", "expiresAt", "fencingEpoch", "holderId", "leaseId", "sessionId"],
    "invalid_fence",
    "lease grant",
  );
  ensure(
    value.contractVersion === STORAGE_CONTRACT_VERSION,
    "invalid_fence",
    "lease contract version is unsupported",
  );
  assertUuid(value.sessionId, "invalid_fence", "lease session ID");
  assertOpaqueId(value.leaseId, "invalid_fence", "lease ID");
  assertOpaqueId(value.holderId, "invalid_fence", "lease holder ID");
  parseFencingEpochForRecord(value.fencingEpoch, "invalid_fence");
  assertIsoTimestamp(value.expiresAt, "invalid_fence", "lease expiration");
  return deepFreeze(defensiveClone(value, "invalid_fence", "lease grant"));
}

export function assertLeaseRenewal(previous, next, options) {
  const { canonical, now } = assertOptionsObject(
    options,
    ["canonical", "now"],
    ["canonical", "now"],
    "invalid_fence",
    "lease renewal options",
  );
  const before = assertLeaseGrant(previous);
  const after = assertLeaseGrant(next);
  const current = assertLeaseGrant(canonical);
  ensure(Number.isFinite(now), "invalid_fence", "authority time is invalid");
  ensure(
    before.sessionId === current.sessionId &&
      before.leaseId === current.leaseId &&
      before.holderId === current.holderId &&
      before.fencingEpoch === current.fencingEpoch &&
      before.expiresAt === current.expiresAt,
    "stale_fence",
    "lease renewal is not based on canonical authority",
  );
  ensure(Date.parse(before.expiresAt) > now, "lease_expired", "expired lease cannot be renewed");
  ensure(
    before.sessionId === after.sessionId &&
      before.leaseId === after.leaseId &&
      before.holderId === after.holderId &&
      before.fencingEpoch === after.fencingEpoch,
    "stale_fence",
    "lease renewal changed the writer fence",
  );
  ensure(
    Date.parse(after.expiresAt) > Date.parse(before.expiresAt),
    "invalid_fence",
    "lease renewal did not extend expiration",
  );
  return after;
}

export function assertCanonicalFenceMatch(options) {
  const { canonical, now, presented } = assertOptionsObject(
    options,
    ["canonical", "now", "presented"],
    ["canonical", "now", "presented"],
    "invalid_fence",
    "canonical fence match options",
  );
  const expected = assertLeaseGrant(canonical);
  const actual = assertLeaseGrant(presented);
  ensure(Number.isFinite(now), "invalid_fence", "authority time is invalid");
  ensure(
    expected.sessionId === actual.sessionId &&
      expected.leaseId === actual.leaseId &&
      expected.holderId === actual.holderId &&
      expected.fencingEpoch === actual.fencingEpoch,
    "stale_fence",
    "writer fence is stale",
  );
  ensure(Date.parse(expected.expiresAt) > now, "lease_expired", "writer lease has expired");
  return expected;
}

export function assertSessionStorageRef(value) {
  assertExactObject(
    value,
    ["backendId", "contractVersion", "sessionId", "storageId"],
    "invalid_storage_ref",
    "session storage reference",
  );
  ensure(
    value.contractVersion === STORAGE_CONTRACT_VERSION,
    "invalid_storage_ref",
    "storage reference contract version is unsupported",
  );
  assertOpaqueId(value.backendId, "invalid_storage_ref", "storage backend ID");
  assertOpaqueId(value.storageId, "invalid_storage_ref", "storage ID");
  assertUuid(value.sessionId, "invalid_storage_ref", "storage session ID");
  return deepFreeze(defensiveClone(value, "invalid_storage_ref", "session storage reference"));
}

export function assertSessionProvisionRequest(value) {
  assertExactObject(
    value,
    ["backendId", "contractVersion", "operationId", "sessionId"],
    "invalid_storage_provision",
    "session provision request",
  );
  ensure(
    value.contractVersion === STORAGE_CONTRACT_VERSION,
    "invalid_storage_provision",
    "storage provision contract version is unsupported",
  );
  assertOpaqueId(value.backendId, "invalid_storage_provision", "provision backend ID");
  assertUuid(value.sessionId, "invalid_storage_provision", "provision session ID");
  assertOpaqueId(value.operationId, "invalid_storage_provision", "provision operation ID");
  return deepFreeze(
    defensiveClone(value, "invalid_storage_provision", "session provision request"),
  );
}

export function assertSessionProvisionResult(value, options) {
  const { previousResult, request } = assertOptionsObject(
    options,
    ["previousResult", "request"],
    ["request"],
    "invalid_storage_provision",
    "session provision result options",
  );
  assertExactObject(
    value,
    [
      "backendId",
      "contractVersion",
      "operationId",
      "proofId",
      "sessionId",
      "status",
      "storageId",
    ],
    "invalid_storage_provision",
    "session provision result",
  );
  const expected = assertSessionProvisionRequest(request);
  const actualRequest = assertSessionProvisionRequest({
    backendId: value.backendId,
    contractVersion: value.contractVersion,
    operationId: value.operationId,
    sessionId: value.sessionId,
  });
  assertOpaqueId(value.storageId, "invalid_storage_provision", "provisioned storage ID");
  assertOpaqueId(value.proofId, "invalid_storage_provision", "provision proof ID");
  ensure(
    value.status === "provisioned",
    "invalid_storage_provision",
    "storage provision result status is unsupported",
  );
  ensure(
    Object.keys(expected).every((key) => expected[key] === actualRequest[key]),
    "invalid_storage_provision",
    "storage provision result does not match its request",
  );
  if (previousResult !== undefined) {
    const previous = assertSessionProvisionResult(previousResult, { request });
    ensure(
      Object.keys(previous).every((key) => previous[key] === value[key]),
      "invalid_storage_provision",
      "storage provision retry does not replay its original result",
    );
  }
  return deepFreeze(
    defensiveClone(value, "invalid_storage_provision", "session provision result"),
  );
}

export function assertSessionAttachment(value) {
  assertExactObject(
    value,
    [
      "attachmentId",
      "backendId",
      "contractVersion",
      "fencingEpoch",
      "holderId",
      "kind",
      "leaseId",
      "mode",
      "operationId",
      "proofId",
      "rootPath",
      "sessionId",
      "storageId",
    ],
    "invalid_storage_attachment",
    "session storage attachment",
  );
  ensure(
    value.contractVersion === STORAGE_CONTRACT_VERSION,
    "invalid_storage_attachment",
    "storage attachment contract version is unsupported",
  );
  assertOpaqueId(value.backendId, "invalid_storage_attachment", "attachment backend ID");
  assertOpaqueId(value.storageId, "invalid_storage_attachment", "attachment storage ID");
  assertUuid(value.sessionId, "invalid_storage_attachment", "attachment session ID");
  assertOpaqueId(value.attachmentId, "invalid_storage_attachment", "attachment ID");
  assertOpaqueId(value.leaseId, "invalid_storage_attachment", "attachment lease ID");
  assertOpaqueId(value.holderId, "invalid_storage_attachment", "attachment holder ID");
  assertOpaqueId(value.operationId, "invalid_storage_attachment", "attachment operation ID");
  assertOpaqueId(value.proofId, "invalid_storage_attachment", "attachment proof ID");
  parseFencingEpochForRecord(value.fencingEpoch, "invalid_storage_attachment");
  ensure(
    value.kind === "directory",
    "invalid_storage_attachment",
    "attachment must expose a normal directory",
  );
  assertAttachmentRootPath(
    value.rootPath,
    "invalid_storage_attachment",
    "attachment root",
  );
  ensure(
    value.mode === "read-write",
    "invalid_storage_attachment",
    "attachment mode is unsupported",
  );
  return deepFreeze(
    defensiveClone(value, "invalid_storage_attachment", "session storage attachment"),
  );
}

export function assertSessionAttachmentMatches(options) {
  const { attachment, lease, manifest, storageRef } = assertOptionsObject(
    options,
    ["attachment", "lease", "manifest", "storageRef"],
    ["attachment", "lease", "manifest", "storageRef"],
    "invalid_storage_attachment",
    "session attachment match options",
  );
  const sessionManifest = assertSessionManifest(manifest);
  const storage = assertSessionStorageRef(storageRef);
  const writerLease = assertLeaseGrant(lease);
  const mounted = assertSessionAttachment(attachment);
  ensure(
    storage.sessionId === sessionManifest.sessionId &&
      writerLease.sessionId === sessionManifest.sessionId &&
      mounted.sessionId === sessionManifest.sessionId &&
      mounted.backendId === storage.backendId &&
      mounted.storageId === storage.storageId &&
      mounted.leaseId === writerLease.leaseId &&
      mounted.holderId === writerLease.holderId &&
      mounted.fencingEpoch === writerLease.fencingEpoch,
    "stale_fence",
    "attachment does not match the current session writer fence",
  );
  return deepFreeze({
    attachment: mounted,
    lease: writerLease,
    manifest: sessionManifest,
    storageRef: storage,
  });
}

export function assertStorageBackend(value) {
  ensure(
    value !== null && typeof value === "object" && !arrayIsArray(value),
    "invalid_storage_backend",
    "storage backend must be an object",
  );
  ensure(
    value.contractVersion === STORAGE_CONTRACT_VERSION,
    "invalid_storage_backend",
    "storage backend contract version is unsupported",
  );
  assertOpaqueId(value.backendId, "invalid_storage_backend", "storage backend ID");
  assertStorageBackendCapabilities(value.capabilities);
  for (let index = 0; index < STORAGE_BACKEND_METHODS.length; index += 1) {
    const method = STORAGE_BACKEND_METHODS[index];
    ensure(
      typeof value[method] === "function",
      "invalid_storage_backend",
      "storage backend is missing a required operation",
    );
  }
  return value;
}

export function assertCheckpointBackend(value) {
  ensure(
    value !== null &&
      typeof value === "object" &&
      !arrayIsArray(value) &&
      !isProxyValue(value),
    "invalid_storage_backend",
    "checkpoint backend must be an object",
  );
  const dataValue = (key) => {
    let cursor = value;
    for (
      let depth = 0;
      cursor !== null && depth < MAX_BACKEND_PROTOTYPE_DEPTH;
      depth += 1
    ) {
      ensure(
        !isProxyValue(cursor),
        "invalid_storage_backend",
        "checkpoint backend prototype chain must not contain a proxy",
      );
      let nextPrototype;
      try {
        nextPrototype = objectGetPrototypeOf(cursor);
      } catch {
        fail(
          "invalid_storage_backend",
          "checkpoint backend prototype chain is invalid",
        );
      }
      ensure(
        cursor !== objectPrototype && !(depth > 0 && nextPrototype === null),
        "invalid_storage_backend",
        "checkpoint backend fields must not come from the shared object prototype",
      );
      let descriptor;
      try {
        descriptor = objectGetOwnPropertyDescriptor(cursor, key);
      } catch {
        fail(
          "invalid_storage_backend",
          "checkpoint backend fields must be data properties",
        );
      }
      if (descriptor !== undefined) {
        ensure(
          objectHasOwn(descriptor, "value"),
          "invalid_storage_backend",
          "checkpoint backend fields must be data properties",
        );
        return descriptor.value;
      }
      cursor = nextPrototype;
    }
    ensure(
      cursor === null,
      "invalid_storage_backend",
      "checkpoint backend prototype chain is too deep",
    );
    fail(
      "invalid_storage_backend",
      "checkpoint backend is missing a required field",
    );
  };
  const contractVersion = dataValue("contractVersion");
  ensure(
    contractVersion === STORAGE_CONTRACT_VERSION,
    "invalid_storage_backend",
    "checkpoint backend contract version is unsupported",
  );
  const backendId = dataValue("backendId");
  assertOpaqueId(
    backendId,
    "invalid_storage_backend",
    "checkpoint backend ID",
  );
  const capabilities = assertStorageBackendCapabilities(
    dataValue("capabilities"),
  );
  const projection = objectCreate(null);
  projection.backendId = backendId;
  projection.capabilities = capabilities;
  projection.contractVersion = contractVersion;
  for (let index = 0; index < CHECKPOINT_BACKEND_METHODS.length; index += 1) {
    const method = CHECKPOINT_BACKEND_METHODS[index];
    const operation = dataValue(method);
    ensure(
      typeof operation === "function" && !isProxyValue(operation),
      "invalid_storage_backend",
      "checkpoint backend is missing a required operation",
    );
    const captured = function checkpointBackendOperation(...args) {
      return reflectApply(operation, value, args);
    };
    objectFreeze(captured);
    projection[method] = captured;
  }
  return objectFreeze(projection);
}

export function assertStorageBackendCapabilities(value) {
  assertExactObject(
    value,
    [
      "atomicPointInTimeCheckpoint",
      "exclusiveWriterAttachment",
      "fencing",
      "normalDirectoryAttachment",
    ],
    "invalid_storage_backend",
    "storage backend capabilities",
  );
  ensure(
    value.normalDirectoryAttachment === true &&
      value.exclusiveWriterAttachment === true &&
      typeof value.atomicPointInTimeCheckpoint === "boolean" &&
      reflectApply(
        arrayIncludesIntrinsic,
        ["epoch-enforced", "verified-detach", "manual"],
        [value.fencing],
      ),
    "invalid_storage_backend",
    "storage backend capabilities are unsupported",
  );
  return deepFreeze(
    defensiveClone(
      value,
      "invalid_storage_backend",
      "storage backend capabilities",
    ),
  );
}

/**
 * Optional operator-plane extension for reconciling one exact checkpoint
 * capture attempt. This is not part of the base storage backend method set.
 */
export function assertCheckpointCaptureReconciliationBackend(value) {
  const backend = assertStorageBackend(value);
  ensure(
    backend.captureReconciliationContractVersion ===
      CHECKPOINT_CAPTURE_RECONCILIATION_CONTRACT_VERSION &&
      typeof backend.reconcileCheckpointCapture === "function",
    "invalid_storage_backend",
    "storage backend does not support checkpoint capture reconciliation",
  );
  return backend;
}

/**
 * Optional operator-plane extension for resuming one exact checkpoint capture
 * that was durably prepared by the stop-to-capture handoff. This is not part
 * of the base storage backend method set.
 */
export function assertPreparedCheckpointCaptureBackend(value) {
  ensure(
    !isProxyValue(value),
    "invalid_storage_backend",
    "storage backend must not be a proxy",
  );
  const backend = assertStorageBackend(value);
  const version = plainDataDescriptor(
    backend,
    "preparedCheckpointCaptureContractVersion",
    "invalid_storage_backend",
    "prepared checkpoint capture backend",
  ).value;
  const resume = plainDataDescriptor(
    backend,
    "resumePreparedCheckpointCapture",
    "invalid_storage_backend",
    "prepared checkpoint capture backend",
  ).value;
  ensure(
    version === PREPARED_CHECKPOINT_CAPTURE_CONTRACT_VERSION &&
      typeof resume === "function",
    "invalid_storage_backend",
    "storage backend does not support prepared checkpoint capture",
  );
  return backend;
}

/**
 * Optional provider extension for attaching one already-published restore
 * destination. The base storage contract remains version 1 and does not
 * require this operation.
 */
export function assertRestoreAttachmentActivationBackend(value) {
  ensure(
    !isProxyValue(value),
    "invalid_storage_backend",
    "storage backend must not be a proxy",
  );
  const backend = assertStorageBackend(value);
  const version = plainDataDescriptor(
    backend,
    "restoreAttachmentActivationContractVersion",
    "invalid_storage_backend",
    "restore attachment activation backend",
  ).value;
  const prepare = plainDataDescriptor(
    backend,
    "prepareRestoreAttachment",
    "invalid_storage_backend",
    "restore attachment activation backend",
  ).value;
  ensure(
    version === RESTORE_ATTACHMENT_ACTIVATION_CONTRACT_VERSION &&
      typeof prepare === "function",
    "invalid_storage_backend",
    "storage backend does not support restore attachment activation",
  );
  return backend;
}

/**
 * Optional operator-plane extension for reconciling one exact restore
 * attachment activation. Reconciliation is read-only physical observation;
 * it does not replace the activation extension or grant a fresh attach.
 */
export function assertRestoreAttachmentReconciliationBackend(value) {
  const backend = assertRestoreAttachmentActivationBackend(value);
  const version = plainDataDescriptor(
    backend,
    "restoreAttachmentReconciliationContractVersion",
    "invalid_storage_backend",
    "restore attachment reconciliation backend",
  ).value;
  const reconcile = plainDataDescriptor(
    backend,
    "reconcileRestoreAttachment",
    "invalid_storage_backend",
    "restore attachment reconciliation backend",
  ).value;
  ensure(
    version === RESTORE_ATTACHMENT_RECONCILIATION_CONTRACT_VERSION &&
      typeof reconcile === "function",
    "invalid_storage_backend",
    "storage backend does not support restore attachment reconciliation",
  );
  return backend;
}

function assertStorageForceFenceRevokedFence(value) {
  assertExactObject(
    value,
    ["fencingEpoch", "holderId", "leaseId"],
    "invalid_storage_force_fence",
    "revoked storage fence",
  );
  assertOpaqueId(
    value.holderId,
    "invalid_storage_force_fence",
    "revoked fence holder ID",
  );
  assertOpaqueId(
    value.leaseId,
    "invalid_storage_force_fence",
    "revoked fence lease ID",
  );
  return parseFencingEpochForRecord(
    value.fencingEpoch,
    "invalid_storage_force_fence",
  );
}

function assertStorageForceFenceTarget(value) {
  assertExactObject(
    value,
    ["attachmentId", "kind"],
    "invalid_storage_force_fence",
    "storage force-fence target",
  );
  assertOpaqueId(
    value.attachmentId,
    "invalid_storage_force_fence",
    "force-fence target attachment ID",
  );
  ensure(
    value.kind === "attachment",
    "invalid_storage_force_fence",
    "storage force-fence target kind is unsupported",
  );
}

export function assertStorageForceFenceRequest(value) {
  assertExactObject(
    value,
    [
      "backendId",
      "contractVersion",
      "fencingEpoch",
      "operationId",
      "revokedFence",
      "sessionId",
      "storageId",
      "target",
    ],
    "invalid_storage_force_fence",
    "storage force-fence request",
  );
  ensure(
    value.contractVersion === STORAGE_CONTRACT_VERSION,
    "invalid_storage_force_fence",
    "storage force-fence contract version is unsupported",
  );
  assertOpaqueId(
    value.backendId,
    "invalid_storage_force_fence",
    "force-fence backend ID",
  );
  assertOpaqueId(
    value.storageId,
    "invalid_storage_force_fence",
    "force-fence storage ID",
  );
  assertUuid(
    value.sessionId,
    "invalid_storage_force_fence",
    "force-fence session ID",
  );
  assertOpaqueId(
    value.operationId,
    "invalid_storage_force_fence",
    "force-fence operation ID",
  );
  const fencingEpoch = parseFencingEpochForRecord(
    value.fencingEpoch,
    "invalid_storage_force_fence",
  );
  const revokedFencingEpoch = assertStorageForceFenceRevokedFence(
    value.revokedFence,
  );
  ensure(
    fencingEpoch > revokedFencingEpoch,
    "invalid_storage_force_fence",
    "storage force-fence epoch must advance the revoked fence",
  );
  assertStorageForceFenceTarget(value.target);
  return deepFreeze(
    defensiveClone(
      value,
      "invalid_storage_force_fence",
      "storage force-fence request",
    ),
  );
}

export function assertStorageForceFenceResult(value, options) {
  const { request } = assertOptionsObject(
    options,
    ["request"],
    ["request"],
    "invalid_storage_force_fence",
    "storage force-fence result options",
  );
  assertExactObject(
    value,
    [
      "backendId",
      "contractVersion",
      "fencingEpoch",
      "operationId",
      "proofId",
      "revokedFence",
      "sessionId",
      "status",
      "storageId",
      "target",
    ],
    "invalid_storage_force_fence",
    "storage force-fence result",
  );
  const expected = assertStorageForceFenceRequest(request);
  const actual = assertStorageForceFenceRequest({
    backendId: value.backendId,
    contractVersion: value.contractVersion,
    fencingEpoch: value.fencingEpoch,
    operationId: value.operationId,
    revokedFence: value.revokedFence,
    sessionId: value.sessionId,
    storageId: value.storageId,
    target: value.target,
  });
  assertOpaqueId(
    value.proofId,
    "invalid_storage_force_fence",
    "force-fence proof ID",
  );
  ensure(
    value.status === "fenced",
    "invalid_storage_force_fence",
    "storage force-fence result status is unsupported",
  );
  ensure(
    expected.backendId === actual.backendId &&
      expected.contractVersion === actual.contractVersion &&
      expected.fencingEpoch === actual.fencingEpoch &&
      expected.operationId === actual.operationId &&
      expected.sessionId === actual.sessionId &&
      expected.storageId === actual.storageId &&
      expected.revokedFence.fencingEpoch ===
        actual.revokedFence.fencingEpoch &&
      expected.revokedFence.holderId === actual.revokedFence.holderId &&
      expected.revokedFence.leaseId === actual.revokedFence.leaseId &&
      expected.target.attachmentId === actual.target.attachmentId &&
      expected.target.kind === actual.target.kind,
    "invalid_storage_force_fence",
    "storage force-fence result does not match its request",
  );
  return deepFreeze(
    defensiveClone(
      value,
      "invalid_storage_force_fence",
      "storage force-fence result",
    ),
  );
}

function assertStorageMutationTarget(value, { operation, storageId }) {
  const schemas = {
    attach: ["attachmentId", "kind"],
    checkpoint: ["artifactId", "checkpointId", "kind"],
    destroy: ["kind", "storageId"],
    detach: ["attachmentId", "kind"],
    restore: ["artifactId", "checkpointId", "kind"],
  };
  assertExactObject(
    value,
    schemas[operation],
    "invalid_storage_mutation",
    "storage mutation target",
  );
  const expectedKind = {
    attach: "attachment",
    checkpoint: "checkpoint",
    destroy: "storage",
    detach: "attachment",
    restore: "checkpoint",
  }[operation];
  ensure(
    value.kind === expectedKind,
    "invalid_storage_mutation",
    "storage mutation target kind is unsupported",
  );
  const requiredIds = {
    attach: [["attachmentId", "target attachment ID"]],
    checkpoint: [
      ["artifactId", "target artifact ID"],
      ["checkpointId", "target checkpoint ID"],
    ],
    destroy: [["storageId", "target storage ID"]],
    detach: [["attachmentId", "target attachment ID"]],
    restore: [
      ["artifactId", "target artifact ID"],
      ["checkpointId", "target checkpoint ID"],
    ],
  }[operation];
  for (const [field, label] of requiredIds) {
    assertOpaqueId(value[field], "invalid_storage_mutation", label);
  }
  if (operation === "destroy") {
    ensure(
      value.storageId === storageId,
      "invalid_storage_mutation",
      "storage mutation target does not match storage ID",
    );
  }
}

export function assertStorageMutationRequest(value) {
  assertExactObject(
    value,
    [
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
    ],
    "invalid_storage_mutation",
    "storage mutation request",
  );
  ensure(
    value.contractVersion === STORAGE_CONTRACT_VERSION,
    "invalid_storage_mutation",
    "storage mutation contract version is unsupported",
  );
  assertOpaqueId(value.backendId, "invalid_storage_mutation", "mutation backend ID");
  assertOpaqueId(value.storageId, "invalid_storage_mutation", "mutation storage ID");
  assertUuid(value.sessionId, "invalid_storage_mutation", "mutation session ID");
  assertOpaqueId(value.leaseId, "invalid_storage_mutation", "mutation lease ID");
  assertOpaqueId(value.holderId, "invalid_storage_mutation", "mutation holder ID");
  assertOpaqueId(value.operationId, "invalid_storage_mutation", "mutation operation ID");
  parseFencingEpochForRecord(value.fencingEpoch, "invalid_storage_mutation");
  ensure(
    reflectApply(
      arrayIncludesIntrinsic,
      ["attach", "checkpoint", "destroy", "detach", "restore"],
      [value.operation],
    ),
    "invalid_storage_mutation",
    "storage mutation operation is unsupported",
  );
  assertStorageMutationTarget(value.target, {
    operation: value.operation,
    storageId: value.storageId,
  });
  return deepFreeze(
    defensiveClone(value, "invalid_storage_mutation", "storage mutation request"),
  );
}

/**
 * Canonicalizes the exact clean-checkpoint admission shared by detached
 * restore planning and foreground execution. The checkpoint may name source
 * storage distinct from the request's destination storage, but both records
 * must bind the same backend, session, artifact, and checkpoint identities.
 */
export function assertRestoreCheckpointAdmission(value) {
  assertExactObject(
    value,
    ["checkpoint", "request"],
    "invalid_restore_checkpoint_admission",
    "restore checkpoint admission",
  );
  let checkpoint;
  let request;
  try {
    checkpoint = assertCheckpointDescriptor(value.checkpoint);
    request = assertStorageMutationRequest(value.request);
  } catch {
    fail(
      "invalid_restore_checkpoint_admission",
      "restore checkpoint admission is invalid",
    );
  }
  ensure(
    checkpoint.checkpointClass === "clean" &&
      request.operation === "restore" &&
      request.backendId === checkpoint.backendId &&
      request.sessionId === checkpoint.sessionId &&
      request.target.kind === "checkpoint" &&
      request.target.artifactId === checkpoint.artifactId &&
      request.target.checkpointId === checkpoint.checkpointId &&
      parseFencingEpochForRecord(
        request.fencingEpoch,
        "invalid_restore_checkpoint_admission",
      ) >
        parseFencingEpochForRecord(
          checkpoint.sourceFencingEpoch,
          "invalid_restore_checkpoint_admission",
        ),
    "invalid_restore_checkpoint_admission",
    "restore checkpoint admission identity does not match",
  );
  return deepFreeze({ checkpoint, request });
}

/**
 * Structural snapshot comparison only. A backend must repeat this comparison
 * atomically with the mutation against its authoritative state.
 */
export function assertStorageMutationMatchesLeaseSnapshot(options) {
  const { allowExpired = false, canonicalLease, now, request, storageRef } =
    assertOptionsObject(
      options,
      ["allowExpired", "canonicalLease", "now", "request", "storageRef"],
      ["canonicalLease", "now", "request", "storageRef"],
      "invalid_storage_mutation",
      "storage mutation snapshot options",
    );
  ensure(
    typeof allowExpired === "boolean",
    "invalid_storage_mutation",
    "allowExpired must be a boolean",
  );
  const mutation = assertStorageMutationRequest(request);
  const canonical = assertLeaseGrant(canonicalLease);
  const storage = assertSessionStorageRef(storageRef);
  ensure(Number.isFinite(now), "invalid_fence", "authority time is invalid");
  ensure(
    mutation.sessionId === canonical.sessionId &&
      mutation.leaseId === canonical.leaseId &&
      mutation.holderId === canonical.holderId &&
      mutation.fencingEpoch === canonical.fencingEpoch,
    "stale_fence",
    "storage mutation fence is stale",
  );
  ensure(
    mutation.sessionId === storage.sessionId &&
      mutation.backendId === storage.backendId &&
      mutation.storageId === storage.storageId,
    "invalid_storage_mutation",
    "storage mutation does not match canonical storage",
  );
  if (allowExpired) {
    ensure(
      mutation.operation === "detach",
      "invalid_storage_mutation",
      "only exact-owner detach may proceed after lease expiration",
    );
  }
  if (!allowExpired) {
    ensure(Date.parse(canonical.expiresAt) > now, "lease_expired", "writer lease has expired");
  }
  return mutation;
}

export function assertStorageMutationResult(value, options) {
  const { request } = assertOptionsObject(
    options,
    ["request"],
    ["request"],
    "invalid_storage_mutation",
    "storage mutation result options",
  );
  assertExactObject(
    value,
    [
      "backendId",
      "contractVersion",
      "fencingEpoch",
      "holderId",
      "leaseId",
      "operation",
      "operationId",
      "proofId",
      "sessionId",
      "status",
      "storageId",
      "target",
    ],
    "invalid_storage_mutation",
    "storage mutation result",
  );
  const expected = assertStorageMutationRequest(request);
  const actualRequest = assertStorageMutationRequest({
    backendId: value.backendId,
    contractVersion: value.contractVersion,
    fencingEpoch: value.fencingEpoch,
    holderId: value.holderId,
    leaseId: value.leaseId,
    operation: value.operation,
    operationId: value.operationId,
    sessionId: value.sessionId,
    storageId: value.storageId,
    target: value.target,
  });
  assertOpaqueId(value.proofId, "invalid_storage_mutation", "mutation proof ID");
  ensure(
    {
      attach: "attached",
      checkpoint: "checkpoint-created",
      destroy: "destroyed",
      detach: "detached",
      restore: "restored",
    }[value.operation] === value.status,
    "invalid_storage_mutation",
    "storage mutation result status is unsupported",
  );
  const targetMatches =
    expected.target.kind === actualRequest.target.kind &&
    (expected.operation === "attach" || expected.operation === "detach"
      ? expected.target.attachmentId === actualRequest.target.attachmentId
      : expected.operation === "destroy"
        ? expected.target.storageId === actualRequest.target.storageId
        : expected.target.artifactId === actualRequest.target.artifactId &&
          expected.target.checkpointId === actualRequest.target.checkpointId);
  ensure(
    expected.backendId === actualRequest.backendId &&
      expected.contractVersion === actualRequest.contractVersion &&
      expected.operation === actualRequest.operation &&
      expected.operationId === actualRequest.operationId &&
      expected.sessionId === actualRequest.sessionId &&
      expected.storageId === actualRequest.storageId &&
      targetMatches,
    "invalid_storage_mutation",
    "storage mutation result does not match its request",
  );
  ensure(
    expected.fencingEpoch === actualRequest.fencingEpoch &&
      expected.holderId === actualRequest.holderId &&
      expected.leaseId === actualRequest.leaseId,
    "stale_fence",
    "storage mutation result fence does not match its request",
  );
  return deepFreeze(
    defensiveClone(value, "invalid_storage_mutation", "storage mutation result"),
  );
}

function isSha256Hex(value) {
  if (typeof value !== "string" || value.length !== 64) return false;
  for (let index = 0; index < value.length; index += 1) {
    if (!isLowerHex(stringCharCodeAt(value, index))) return false;
  }
  return true;
}

function assertPersistentObjectId(value, code, label) {
  ensure(
    typeof value === "string" &&
      value.length >= 1 &&
      value.length <= 256 &&
      isAsciiAlphaNumeric(stringCharCodeAt(value, 0)),
    code,
    `${label} must be a bounded persistent object identifier`,
  );
  for (let index = 1; index < value.length; index += 1) {
    const character = stringCharCodeAt(value, index);
    ensure(
      isAsciiAlphaNumeric(character) ||
        character === 45 ||
        character === 46 ||
        character === 58 ||
        character === 95,
      code,
      `${label} must be a bounded persistent object identifier`,
    );
  }
}

function assertRestoreAttachmentPublication(value) {
  const code = "invalid_restore_attachment_activation";
  assertExactObject(
    value,
    [
      "artifactManifestDigest",
      "coordinatorBindingSha256",
      "modeledDigest",
      "publicationId",
      "publicationKind",
      "root",
      "treeIdentityDigest",
    ],
    code,
    "restore attachment publication",
  );
  assertExactObject(
    value.root,
    ["filesystemId", "objectIdentityScheme", "objectId", "rootPath"],
    code,
    "restore attachment publication root",
  );
  ensure(
    isSha256Hex(value.artifactManifestDigest) &&
      isSha256Hex(value.coordinatorBindingSha256) &&
      isSha256Hex(value.modeledDigest) &&
      isSha256Hex(value.treeIdentityDigest),
    code,
    "restore attachment publication digests must be lowercase sha256 values",
  );
  assertOpaqueId(value.publicationId, code, "restore attachment publication ID");
  ensure(
    value.publicationKind === "restore-destination",
    code,
    "restore attachment publication kind is unsupported",
  );
  assertOpaqueId(value.root.filesystemId, code, "restore attachment filesystem ID");
  assertOpaqueId(
    value.root.objectIdentityScheme,
    code,
    "restore attachment object identity scheme",
  );
  assertPersistentObjectId(
    value.root.objectId,
    code,
    "restore attachment object ID",
  );
  assertAttachmentRootPath(
    value.root.rootPath,
    code,
    "restore attachment publication root",
  );
  return deepFreeze(
    defensiveClone(value, code, "restore attachment publication"),
  );
}

function restoreAttachmentPublicationsMatch(expected, actual) {
  return (
    expected.artifactManifestDigest === actual.artifactManifestDigest &&
    expected.coordinatorBindingSha256 === actual.coordinatorBindingSha256 &&
    expected.modeledDigest === actual.modeledDigest &&
    expected.publicationId === actual.publicationId &&
    expected.publicationKind === actual.publicationKind &&
    expected.treeIdentityDigest === actual.treeIdentityDigest &&
    expected.root.filesystemId === actual.root.filesystemId &&
    expected.root.objectIdentityScheme === actual.root.objectIdentityScheme &&
    expected.root.objectId === actual.root.objectId &&
    expected.root.rootPath === actual.root.rootPath
  );
}

export function assertRestoreAttachmentActivationRequest(value) {
  const code = "invalid_restore_attachment_activation";
  assertExactObject(
    value,
    ["contractVersion", "lease", "manifest", "mutationRequest", "publication", "storageRef"],
    code,
    "restore attachment activation request",
  );
  ensure(
    value.contractVersion === RESTORE_ATTACHMENT_ACTIVATION_CONTRACT_VERSION,
    code,
    "restore attachment activation contract version is unsupported",
  );
  const writerLease = assertLeaseGrant(value.lease);
  const sessionManifest = assertSessionManifest(value.manifest);
  const mutation = assertStorageMutationRequest(value.mutationRequest);
  const publication = assertRestoreAttachmentPublication(value.publication);
  const storage = assertSessionStorageRef(value.storageRef);
  ensure(
    mutation.operation === "attach",
    code,
    "restore attachment activation requires an attach mutation",
  );
  ensure(
    sessionManifest.sessionId === writerLease.sessionId &&
      storage.sessionId === sessionManifest.sessionId &&
      mutation.sessionId === sessionManifest.sessionId &&
      mutation.backendId === storage.backendId &&
      mutation.storageId === storage.storageId,
    code,
    "restore attachment activation request does not match canonical session storage",
  );
  ensure(
    mutation.leaseId === writerLease.leaseId &&
      mutation.holderId === writerLease.holderId &&
      mutation.fencingEpoch === writerLease.fencingEpoch,
    "stale_fence",
    "restore attachment activation request fence is stale",
  );
  return deepFreeze({
    contractVersion: RESTORE_ATTACHMENT_ACTIVATION_CONTRACT_VERSION,
    lease: writerLease,
    manifest: sessionManifest,
    mutationRequest: mutation,
    publication,
    storageRef: storage,
  });
}

export function assertRestoreAttachmentActivationResult(value, options) {
  const code = "invalid_restore_attachment_activation";
  const { request } = assertOptionsObject(
    options,
    ["request"],
    ["request"],
    code,
    "restore attachment activation result options",
  );
  assertExactObject(
    value,
    ["attachment", "contractVersion", "mutationResult", "publication"],
    code,
    "restore attachment activation result",
  );
  ensure(
    value.contractVersion === RESTORE_ATTACHMENT_ACTIVATION_CONTRACT_VERSION,
    code,
    "restore attachment activation contract version is unsupported",
  );
  const expected = assertRestoreAttachmentActivationRequest(request);
  const publication = assertRestoreAttachmentPublication(value.publication);
  ensure(
    restoreAttachmentPublicationsMatch(expected.publication, publication),
    code,
    "restore attachment activation result does not echo its publication",
  );
  const mutationResult = assertStorageMutationResult(value.mutationResult, {
    request: expected.mutationRequest,
  });
  const matched = assertSessionAttachmentMatches({
    attachment: value.attachment,
    lease: expected.lease,
    manifest: expected.manifest,
    storageRef: expected.storageRef,
  });
  ensure(
    matched.attachment.operationId === expected.mutationRequest.operationId &&
      matched.attachment.operationId === mutationResult.operationId &&
      matched.attachment.attachmentId ===
        expected.mutationRequest.target.attachmentId &&
      matched.attachment.attachmentId === mutationResult.target.attachmentId &&
      matched.attachment.proofId === mutationResult.proofId,
    code,
    "restore attachment does not match its attach request and result",
  );
  // Persistent object identity is established by the exact publication echo
  // above. Path equality only correlates that object with the prepared mount.
  ensure(
    matched.attachment.rootPath === publication.root.rootPath,
    code,
    "restore attachment path does not match the published destination",
  );
  return deepFreeze({
    attachment: matched.attachment,
    contractVersion: RESTORE_ATTACHMENT_ACTIVATION_CONTRACT_VERSION,
    mutationResult,
    publication,
  });
}

export function assertRestoreAttachmentReconciliationResult(value, options) {
  const code = "invalid_restore_attachment_reconciliation";
  const { request } = assertOptionsObject(
    options,
    ["request"],
    ["request"],
    code,
    "restore attachment reconciliation result options",
  );
  let expected;
  try {
    expected = assertRestoreAttachmentActivationRequest(request);
  } catch {
    fail(code, "restore attachment reconciliation request is invalid");
  }
  inspectPlainDataObject(
    value,
    code,
    "restore attachment reconciliation result",
  );
  const contractVersion = plainDataDescriptor(
    value,
    "contractVersion",
    code,
    "restore attachment reconciliation result",
  ).value;
  const outcome = plainDataDescriptor(
    value,
    "outcome",
    code,
    "restore attachment reconciliation result",
  ).value;
  ensure(
    contractVersion ===
      RESTORE_ATTACHMENT_RECONCILIATION_CONTRACT_VERSION,
    code,
    "restore attachment reconciliation contract version is unsupported",
  );
  if (outcome === "applied") {
    assertExactObject(
      value,
      ["contractVersion", "outcome", "result"],
      code,
      "restore attachment reconciliation result",
    );
    let result;
    try {
      result = assertRestoreAttachmentActivationResult(value.result, {
        request: expected,
      });
    } catch {
      fail(code, "restore attachment reconciliation result is invalid");
    }
    return deepFreeze({
      contractVersion: RESTORE_ATTACHMENT_RECONCILIATION_CONTRACT_VERSION,
      outcome: "applied",
      result,
    });
  }
  ensure(
    outcome === "absent-and-quiescent" || outcome === "unknown",
    code,
    "restore attachment reconciliation outcome is unsupported",
  );
  assertExactObject(
    value,
    ["contractVersion", "outcome"],
    code,
    "restore attachment reconciliation result",
  );
  return deepFreeze({
    contractVersion: RESTORE_ATTACHMENT_RECONCILIATION_CONTRACT_VERSION,
    outcome,
  });
}

/**
 * Builds runner-neutral data only. This function does not authorize launch or
 * prove that the pathname is still pinned; a trusted launcher must perform the
 * bind while holding its backend directory authority and canonical fence.
 */
export function createRootlessWorkerTemplate(options) {
  const { attachment, lease, manifest, storageRef } = assertOptionsObject(
    options,
    ["attachment", "lease", "manifest", "storageRef"],
    ["attachment", "lease", "manifest", "storageRef"],
    "invalid_worker_template",
    "rootless worker template options",
  );
  const matched = assertSessionAttachmentMatches({ attachment, lease, manifest, storageRef });
  return deepFreeze({
    agentPolicy: matched.manifest.agents,
    auth: {
      authJsonPolicy: "forbidden",
      mode: matched.manifest.authMode,
    },
    codexConfig: {
      cliOverrides: {
        sqlite_home: SESSION_WORKER_LAYOUT.codexHome,
      },
      deniedRequestOverrideKeys: ["sqlite_home"],
      requiredEffectiveValues: {
        sqlite_home: SESSION_WORKER_LAYOUT.codexHome,
      },
    },
    codexSandbox: matched.manifest.runtime.codexSandbox,
    cwd: SESSION_WORKER_LAYOUT.workspace,
    env: {
      CODEX_HOME: SESSION_WORKER_LAYOUT.codexHome,
      CODEX_SQLITE_HOME: SESSION_WORKER_LAYOUT.codexHome,
    },
    mount: {
      propagation: "rprivate",
      readOnly: false,
      source: matched.attachment.rootPath,
      target: SESSION_WORKER_ROOT,
      type: "bind",
    },
    rootless: true,
  });
}

export function assertCheckpointClass(value) {
  ensure(
    typeof value === "string" && CHECKPOINT_CLASSES.includes(value),
    "invalid_checkpoint",
    "checkpoint class is unsupported",
  );
  return value;
}

export function checkpointClassPolicy(value) {
  return CHECKPOINT_CLASS_POLICIES[assertCheckpointClass(value)];
}

export function assertCheckpointDescriptor(value, options = {}) {
  const { manifest, storageRef } = assertOptionsObject(
    options,
    ["manifest", "storageRef"],
    [],
    "invalid_checkpoint",
    "checkpoint descriptor options",
  );
  assertExactObject(
    value,
    [
      "artifactId",
      "backendId",
      "checkpointClass",
      "checkpointId",
      "codexSessionId",
      "codexThreadId",
      "contractVersion",
      "createdAt",
      "imageDigest",
      "sessionId",
      "sourceFencingEpoch",
      "storageId",
    ],
    "invalid_checkpoint",
    "checkpoint descriptor",
  );
  ensure(
    value.contractVersion === STORAGE_CONTRACT_VERSION,
    "invalid_checkpoint",
    "checkpoint contract version is unsupported",
  );
  assertOpaqueId(value.checkpointId, "invalid_checkpoint", "checkpoint ID");
  assertOpaqueId(value.artifactId, "invalid_checkpoint", "checkpoint artifact ID");
  assertOpaqueId(value.backendId, "invalid_checkpoint", "checkpoint backend ID");
  assertOpaqueId(value.storageId, "invalid_checkpoint", "checkpoint storage ID");
  assertUuid(value.sessionId, "invalid_checkpoint", "checkpoint session ID");
  assertUuid(value.codexThreadId, "invalid_checkpoint", "checkpoint Codex thread ID");
  assertUuid(value.codexSessionId, "invalid_checkpoint", "checkpoint Codex session-tree ID");
  ensure(
    value.codexThreadId === value.codexSessionId,
    "invalid_checkpoint",
    "checkpoint must identify the root Codex thread",
  );
  ensure(
    typeof value.imageDigest === "string" && OCI_DIGEST_PATTERN.test(value.imageDigest),
    "invalid_checkpoint",
    "checkpoint image digest is invalid",
  );
  parseFencingEpochForRecord(value.sourceFencingEpoch, "invalid_checkpoint");
  assertCheckpointClass(value.checkpointClass);
  assertIsoTimestamp(value.createdAt, "invalid_checkpoint", "checkpoint creation time");

  if (manifest !== undefined) {
    const expectedManifest = assertSessionManifest(manifest);
    ensure(
      value.sessionId === expectedManifest.sessionId &&
        value.codexThreadId === expectedManifest.codex.rootThreadId &&
        value.codexSessionId === expectedManifest.codex.sessionId &&
        value.imageDigest === expectedManifest.runtime.imageDigest,
      "invalid_checkpoint",
      "checkpoint does not match the immutable session manifest",
    );
  }
  if (storageRef !== undefined) {
    const expectedStorage = assertSessionStorageRef(storageRef);
    ensure(
      value.sessionId === expectedStorage.sessionId &&
        value.backendId === expectedStorage.backendId &&
        value.storageId === expectedStorage.storageId,
      "invalid_checkpoint",
      "checkpoint does not match session storage",
    );
  }
  return deepFreeze(defensiveClone(value, "invalid_checkpoint", "checkpoint descriptor"));
}
