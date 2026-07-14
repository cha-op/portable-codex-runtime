import { isAbsolute, parse, resolve } from "node:path";
import { types as utilTypes } from "node:util";

export const SESSION_MANIFEST_SCHEMA_VERSION = 1;
export const SESSION_LAYOUT_VERSION = 1;
export const STORAGE_CONTRACT_VERSION = 1;
export const CHECKPOINT_CAPTURE_RECONCILIATION_CONTRACT_VERSION = 1;
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
const OPAQUE_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u;
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
  if (value && typeof value === "object" && !Object.isFrozen(value)) {
    Object.freeze(value);
    for (const child of Object.values(value)) deepFreeze(child);
  }
  return value;
}

function defensiveClone(value, code, label) {
  try {
    return structuredClone(value);
  } catch {
    fail(code, `${label} must contain cloneable data`);
  }
}

function inspectPlainDataObject(value, code, label) {
  if (
    utilTypes.isProxy(value) ||
    value === null ||
    typeof value !== "object" ||
    Array.isArray(value)
  ) {
    fail(code, `${label} must be a plain object`);
  }
  let prototype;
  let actual;
  try {
    prototype = Object.getPrototypeOf(value);
    actual = Reflect.ownKeys(value);
  } catch {
    fail(code, `${label} must be a plain object`);
  }
  ensure(
    [Object.prototype, null].includes(prototype),
    code,
    `${label} must be a plain object`,
  );
  return actual;
}

function plainDataDescriptor(value, key, code, label) {
  let descriptor;
  try {
    descriptor = Object.getOwnPropertyDescriptor(value, key);
  } catch {
    fail(code, `${label} fields must be enumerable plain data properties`);
  }
  ensure(
    descriptor?.enumerable === true && Object.hasOwn(descriptor, "value"),
    code,
    `${label} fields must be enumerable plain data properties`,
  );
  return descriptor;
}

function assertExactObject(value, keys, code, label) {
  const actual = inspectPlainDataObject(value, code, label);
  ensure(
    actual.length === keys.length &&
      actual.every((key) => typeof key === "string" && keys.includes(key)),
    code,
    `${label} contains unexpected or missing fields`,
  );
  for (const key of actual) plainDataDescriptor(value, key, code, label);
}

function assertOptionsObject(value, allowedKeys, requiredKeys, code, label) {
  const actual = inspectPlainDataObject(value, code, label);
  ensure(
    actual.every((key) => typeof key === "string" && allowedKeys.includes(key)) &&
      requiredKeys.every((key) => actual.includes(key)),
    code,
    `${label} contains unexpected or missing fields`,
  );
  const normalized = Object.create(null);
  for (const key of actual) {
    const descriptor = plainDataDescriptor(value, key, code, label);
    normalized[key] = descriptor.value;
  }
  return normalized;
}

function assertUuid(value, code, label) {
  ensure(typeof value === "string" && UUID_PATTERN.test(value), code, `${label} must be a UUID`);
}

function assertOpaqueId(value, code, label) {
  ensure(
    typeof value === "string" && OPAQUE_ID_PATTERN.test(value),
    code,
    `${label} must be an opaque identifier`,
  );
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

function assertAgentPolicy(value) {
  assertExactObject(
    value,
    ["defaultMaxSubagents", "maxDepth", "maxSubagents"],
    "invalid_session_manifest",
    "session agent policy",
  );
  ensure(
    Number.isSafeInteger(value.maxSubagents) && value.maxSubagents === MAX_SUBAGENTS,
    "invalid_session_manifest",
    "session subagent hard limit is unsupported",
  );
  ensure(
    Number.isSafeInteger(value.maxDepth) && value.maxDepth === MAX_AGENT_DEPTH,
    "invalid_session_manifest",
    "session agent depth limit is unsupported",
  );
  ensure(
    Number.isSafeInteger(value.defaultMaxSubagents) &&
      value.defaultMaxSubagents >= 1 &&
      value.defaultMaxSubagents <= value.maxSubagents,
    "invalid_session_manifest",
    "default subagent limit is invalid",
  );
}

export function assertSessionManifest(value) {
  assertExactObject(
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
  assertUuid(value.sessionId, "invalid_session_manifest", "runtime session ID");
  assertExactObject(
    value.codex,
    ["ephemeral", "historyMode", "rootThreadId", "sessionId"],
    "invalid_session_manifest",
    "Codex session binding",
  );
  assertUuid(value.codex.rootThreadId, "invalid_session_manifest", "Codex root thread ID");
  assertUuid(value.codex.sessionId, "invalid_session_manifest", "Codex session-tree ID");
  ensure(
    value.codex.sessionId === value.codex.rootThreadId,
    "invalid_session_manifest",
    "root Codex session-tree ID must equal its thread ID",
  );
  ensure(value.codex.ephemeral === false, "invalid_session_manifest", "Codex thread must persist");
  ensure(
    ["legacy", "paginated"].includes(value.codex.historyMode),
    "invalid_session_manifest",
    "Codex history mode is unsupported",
  );
  assertExactObject(
    value.runtime,
    ["codexSandbox", "codexVersion", "imageDigest", "imageMediaType", "platform"],
    "invalid_session_manifest",
    "session runtime",
  );
  ensure(
    typeof value.runtime.imageDigest === "string" &&
      OCI_DIGEST_PATTERN.test(value.runtime.imageDigest),
    "invalid_session_manifest",
    "runtime image must use a concrete lowercase sha256 digest",
  );
  ensure(
    PLATFORM_IMAGE_MEDIA_TYPES.includes(value.runtime.imageMediaType),
    "invalid_session_manifest",
    "runtime image media type must describe a platform manifest",
  );
  ensure(
    ["linux/amd64", "linux/arm64"].includes(value.runtime.platform),
    "invalid_session_manifest",
    "runtime platform is unsupported",
  );
  ensure(
    typeof value.runtime.codexVersion === "string" &&
      value.runtime.codexVersion.length <= 128 &&
      CODEX_VERSION_PATTERN.test(value.runtime.codexVersion),
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
  return deepFreeze(defensiveClone(value, "invalid_session_manifest", "session manifest"));
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
    typeof value === "string" && /^[1-9][0-9]{0,19}$/u.test(value),
    code,
    "fencing epoch must be a canonical positive decimal string",
  );
  const epoch = BigInt(value);
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
  ensure(
    typeof value.rootPath === "string" &&
      !value.rootPath.includes("\0") &&
      isAbsolute(value.rootPath) &&
      resolve(value.rootPath) === value.rootPath &&
      value.rootPath !== parse(value.rootPath).root,
    "invalid_storage_attachment",
    "attachment root must be an absolute host-local directory path",
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
    [storage.sessionId, writerLease.sessionId, mounted.sessionId].every(
      (sessionId) => sessionId === sessionManifest.sessionId,
    ) &&
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
    value !== null && typeof value === "object" && !Array.isArray(value),
    "invalid_storage_backend",
    "storage backend must be an object",
  );
  ensure(
    value.contractVersion === STORAGE_CONTRACT_VERSION,
    "invalid_storage_backend",
    "storage backend contract version is unsupported",
  );
  assertOpaqueId(value.backendId, "invalid_storage_backend", "storage backend ID");
  assertExactObject(
    value.capabilities,
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
    value.capabilities.normalDirectoryAttachment === true &&
      value.capabilities.exclusiveWriterAttachment === true &&
      typeof value.capabilities.atomicPointInTimeCheckpoint === "boolean" &&
      ["epoch-enforced", "verified-detach", "manual"].includes(value.capabilities.fencing),
    "invalid_storage_backend",
    "storage backend capabilities are unsupported",
  );
  for (const method of STORAGE_BACKEND_METHODS) {
    ensure(
      typeof value[method] === "function",
      "invalid_storage_backend",
      "storage backend is missing a required operation",
    );
  }
  return value;
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
    ["attach", "checkpoint", "destroy", "detach", "restore"].includes(
      value.operation,
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
    Object.keys(expected.target).length === Object.keys(actualRequest.target).length &&
    Object.keys(expected.target).every(
      (targetKey) => expected.target[targetKey] === actualRequest.target[targetKey],
    );
  ensure(
    ["backendId", "contractVersion", "operation", "operationId", "sessionId", "storageId"].every(
      (key) => expected[key] === actualRequest[key],
    ) && targetMatches,
    "invalid_storage_mutation",
    "storage mutation result does not match its request",
  );
  ensure(
    ["fencingEpoch", "holderId", "leaseId"].every(
      (key) => expected[key] === actualRequest[key],
    ),
    "stale_fence",
    "storage mutation result fence does not match its request",
  );
  return deepFreeze(
    defensiveClone(value, "invalid_storage_mutation", "storage mutation result"),
  );
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
