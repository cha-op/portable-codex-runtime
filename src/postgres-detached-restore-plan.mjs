import { Hash, createHash as createHashExport } from "node:crypto";
import {
  basename as pathBasenameExport,
  dirname as pathDirnameExport,
  isAbsolute as pathIsAbsoluteExport,
  parse as pathParseExport,
  resolve as pathResolveExport,
} from "node:path";
import { types as utilTypes } from "node:util";

const arrayIncludesIntrinsic = Array.prototype.includes;
const arrayIsArray = Array.isArray;
const BigIntConstructor = BigInt;
const createHashIntrinsic = createHashExport;
const DateConstructor = Date;
const dateParseIntrinsic = Date.parse;
const dateToISOStringIntrinsic = Date.prototype.toISOString;
const ErrorConstructor = Error;
const hashDigestIntrinsic = Hash.prototype.digest;
const hashUpdateIntrinsic = Hash.prototype.update;
const isProxyValue = utilTypes.isProxy;
const JsonObject = JSON;
const jsonStringifyIntrinsic = JSON.stringify;
const numberIsFinite = Number.isFinite;
const numberIsSafeInteger = Number.isSafeInteger;
const objectCreate = Object.create;
const objectDefineProperty = Object.defineProperty;
const objectFreeze = Object.freeze;
const objectGetOwnPropertyDescriptor = Object.getOwnPropertyDescriptor;
const objectGetPrototypeOf = Object.getPrototypeOf;
const objectHasOwn = Object.hasOwn;
const objectPrototype = Object.prototype;
const pathBasename = pathBasenameExport;
const pathDirname = pathDirnameExport;
const pathIsAbsolute = pathIsAbsoluteExport;
const pathParse = pathParseExport;
const pathResolve = pathResolveExport;
const reflectApply = Reflect.apply;
const reflectOwnKeys = Reflect.ownKeys;
const regexpExecIntrinsic = RegExp.prototype.exec;
const stringCharCodeAtIntrinsic = String.prototype.charCodeAt;
const stringStartsWithIntrinsic = String.prototype.startsWith;
const TypeErrorConstructor = TypeError;
const WeakSetConstructor = WeakSet;
const weakSetAddIntrinsic = WeakSet.prototype.add;
const weakSetHasIntrinsic = WeakSet.prototype.has;

export const POSTGRES_DETACHED_RESTORE_PLAN_CONTRACT_VERSION = 1;

const MAX_WRITER_LEASE_DURATION_MILLISECONDS = 86_400_000;
const MAX_UINT64 = 18_446_744_073_709_551_615n;
const PLAN_INPUT_DOMAIN =
  "portable-codex-runtime:postgres-detached-restore-plan-input:v1";
const PLAN_ID_DOMAIN =
  "portable-codex-runtime:postgres-detached-restore-plan-id:v1";
const PLAN_SHA256_DOMAIN =
  "portable-codex-runtime:postgres-detached-restore-plan:v1";
const NUL_PATTERN = /\0/u;
const OPAQUE_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u;
const SHA256_PATTERN = /^[0-9a-f]{64}$/u;
const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u;

const OPTION_KEYS = objectFreeze(["plan", "request"]);
const PLAN_KEYS = objectFreeze([
  "captureCreatedAt",
  "destinationDirectory",
  "destinationOwnedRoot",
  "detachMode",
  "holderId",
  "imagePlanId",
  "leaseDurationMilliseconds",
  "sourceArtifactDirectory",
  "sourceArtifactOwnedRoot",
]);
const REQUEST_KEYS = objectFreeze([
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
const TARGET_KEYS = objectFreeze([
  "artifactId",
  "checkpointId",
  "kind",
]);

const ERROR_CODE = "invalid_postgres_detached_restore_plan";
const ERROR_MESSAGE = "PostgreSQL detached restore plan is invalid";
const errorBrands = new WeakSetConstructor();
const planBrands = new WeakSetConstructor();

export class PostgresDetachedRestorePlanError extends ErrorConstructor {
  constructor(code) {
    if (code !== ERROR_CODE) {
      throw new TypeErrorConstructor(
        "unsupported PostgreSQL detached restore plan error",
      );
    }
    super(ERROR_MESSAGE);
    this.name = "PostgresDetachedRestorePlanError";
    this.code = code;
    this.retryable = false;
    callIntrinsic(weakSetAddIntrinsic, errorBrands, [this]);
    objectFreeze(this);
  }
}

function fail() {
  throw new PostgresDetachedRestorePlanError(ERROR_CODE);
}

function ensure(condition) {
  if (!condition) fail();
}

function callIntrinsic(intrinsic, receiver, args) {
  return reflectApply(intrinsic, receiver, args);
}

function arrayIncludes(value, candidate) {
  return callIntrinsic(arrayIncludesIntrinsic, value, [candidate]);
}

function regexpTest(pattern, value) {
  return callIntrinsic(regexpExecIntrinsic, pattern, [value]) !== null;
}

function stringStartsWith(value, candidate) {
  return callIntrinsic(stringStartsWithIntrinsic, value, [candidate]);
}

function exactDataObject(value, expectedKeys) {
  ensure(
    value !== null &&
      typeof value === "object" &&
      !isProxyValue(value) &&
      !arrayIsArray(value),
  );
  let prototype;
  let keys;
  try {
    prototype = objectGetPrototypeOf(value);
    keys = reflectOwnKeys(value);
  } catch {
    fail();
  }
  ensure(
    (prototype === objectPrototype || prototype === null) &&
      keys.length === expectedKeys.length,
  );
  const normalized = objectCreate(null);
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
    ensure(typeof key === "string" && arrayIncludes(expectedKeys, key));
    let descriptor;
    try {
      descriptor = objectGetOwnPropertyDescriptor(value, key);
    } catch {
      fail();
    }
    ensure(
      descriptor?.enumerable === true && objectHasOwn(descriptor, "value"),
    );
    normalized[key] = descriptor.value;
  }
  return normalized;
}

function exactFrozenRecord(value) {
  const result = objectCreate(null);
  const keys = reflectOwnKeys(value);
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
    objectDefineProperty(result, key, {
      enumerable: true,
      value: value[key],
    });
  }
  return objectFreeze(result);
}

function opaqueId(value) {
  ensure(typeof value === "string" && regexpTest(OPAQUE_ID_PATTERN, value));
  return value;
}

function canonicalUuid(value) {
  ensure(typeof value === "string" && regexpTest(UUID_PATTERN, value));
  return value;
}

function canonicalFencingEpoch(value) {
  ensure(
    typeof value === "string" &&
      value.length >= 1 &&
      value.length <= 20 &&
      callIntrinsic(stringCharCodeAtIntrinsic, value, [0]) >= 49 &&
      callIntrinsic(stringCharCodeAtIntrinsic, value, [0]) <= 57,
  );
  for (let index = 1; index < value.length; index += 1) {
    const codePoint = callIntrinsic(stringCharCodeAtIntrinsic, value, [index]);
    ensure(codePoint >= 48 && codePoint <= 57);
  }
  let epoch;
  try {
    epoch = BigIntConstructor(value);
  } catch {
    fail();
  }
  ensure(epoch <= MAX_UINT64);
  return value;
}

function canonicalTimestamp(value) {
  ensure(typeof value === "string");
  let milliseconds;
  let canonical;
  try {
    milliseconds = callIntrinsic(dateParseIntrinsic, DateConstructor, [value]);
    canonical = callIntrinsic(
      dateToISOStringIntrinsic,
      new DateConstructor(milliseconds),
      [],
    );
  } catch {
    fail();
  }
  ensure(numberIsFinite(milliseconds) && canonical === value);
  return value;
}

function canonicalRequest(value) {
  const request = exactDataObject(value, REQUEST_KEYS);
  const target = exactDataObject(request.target, TARGET_KEYS);
  ensure(
    request.contractVersion === 1 &&
      request.operation === "restore" &&
      target.kind === "checkpoint",
  );
  const canonicalTarget = exactFrozenRecord({
    artifactId: opaqueId(target.artifactId),
    checkpointId: opaqueId(target.checkpointId),
    kind: "checkpoint",
  });
  return exactFrozenRecord({
    backendId: opaqueId(request.backendId),
    contractVersion: 1,
    fencingEpoch: canonicalFencingEpoch(request.fencingEpoch),
    holderId: opaqueId(request.holderId),
    leaseId: opaqueId(request.leaseId),
    operation: "restore",
    operationId: opaqueId(request.operationId),
    sessionId: canonicalUuid(request.sessionId),
    storageId: opaqueId(request.storageId),
    target: canonicalTarget,
  });
}

function canonicalDirectory(value) {
  ensure(
    typeof value === "string" &&
      !regexpTest(NUL_PATTERN, value) &&
      pathIsAbsolute(value) &&
      pathResolve(value) === value &&
      value !== pathParse(value).root,
  );
  return value;
}

function directPathPlan(directoryValue, ownedRootValue) {
  const directory = canonicalDirectory(directoryValue);
  const ownedRoot = canonicalDirectory(ownedRootValue);
  const name = pathBasename(directory);
  ensure(
    pathDirname(directory) === ownedRoot &&
      name.length > 0 &&
      name !== "." &&
      name !== ".." &&
      pathBasename(name) === name,
  );
  return exactFrozenRecord({ directory, ownedRoot });
}

function pathsAreDisjoint(left, right) {
  return (
    left !== right &&
    !stringStartsWith(left, `${right}/`) &&
    !stringStartsWith(right, `${left}/`)
  );
}

function canonicalPlanInput(value) {
  const plan = exactDataObject(value, PLAN_KEYS);
  const sourceArtifact = directPathPlan(
    plan.sourceArtifactDirectory,
    plan.sourceArtifactOwnedRoot,
  );
  const destination = directPathPlan(
    plan.destinationDirectory,
    plan.destinationOwnedRoot,
  );
  ensure(pathsAreDisjoint(sourceArtifact.ownedRoot, destination.ownedRoot));
  ensure(plan.detachMode === "release" || plan.detachMode === "force-fence");
  ensure(
    numberIsSafeInteger(plan.leaseDurationMilliseconds) &&
      plan.leaseDurationMilliseconds > 0 &&
      plan.leaseDurationMilliseconds <=
        MAX_WRITER_LEASE_DURATION_MILLISECONDS,
  );
  return exactFrozenRecord({
    captureCreatedAt: canonicalTimestamp(plan.captureCreatedAt),
    destinationDirectory: destination.directory,
    destinationOwnedRoot: destination.ownedRoot,
    detachMode: plan.detachMode,
    holderId: opaqueId(plan.holderId),
    imagePlanId: opaqueId(plan.imagePlanId),
    leaseDurationMilliseconds: plan.leaseDurationMilliseconds,
    sourceArtifactDirectory: sourceArtifact.directory,
    sourceArtifactOwnedRoot: sourceArtifact.ownedRoot,
  });
}

function canonicalSerialize(value) {
  let serialized;
  try {
    serialized = callIntrinsic(jsonStringifyIntrinsic, JsonObject, [value]);
  } catch {
    fail();
  }
  ensure(typeof serialized === "string");
  return serialized;
}

function sha256Parts(parts) {
  let hash;
  let digest;
  try {
    hash = callIntrinsic(createHashIntrinsic, undefined, ["sha256"]);
    for (let index = 0; index < parts.length; index += 1) {
      callIntrinsic(hashUpdateIntrinsic, hash, [parts[index], "utf8"]);
    }
    digest = callIntrinsic(hashDigestIntrinsic, hash, ["hex"]);
  } catch {
    fail();
  }
  ensure(typeof digest === "string" && regexpTest(SHA256_PATTERN, digest));
  return digest;
}

function planInputSha256(input) {
  return sha256Parts([
    PLAN_INPUT_DOMAIN,
    "\0",
    canonicalSerialize(input),
    "\n",
  ]);
}

function derivedDigest(seedSha256, role) {
  return sha256Parts([
    PLAN_ID_DOMAIN,
    "\0",
    role,
    "\0",
    seedSha256,
    "\n",
  ]);
}

function deterministicOpaqueId(seedSha256, role, prefix) {
  return `${prefix}${derivedDigest(seedSha256, role)}`;
}

function rootPlanSha256(value) {
  return sha256Parts([
    PLAN_SHA256_DOMAIN,
    "\0",
    canonicalSerialize(value),
    "\n",
  ]);
}

export function isPostgresDetachedRestorePlan(value) {
  if (arguments.length !== 1) return false;
  try {
    return callIntrinsic(weakSetHasIntrinsic, planBrands, [value]);
  } catch {
    return false;
  }
}

/**
 * Canonicalizes one caller-persisted detached-restore root plan and derives
 * every durable subordinate identity from its complete request binding.
 */
export function createPostgresDetachedRestorePlan(options) {
  if (arguments.length !== 1) fail();
  try {
    const normalized = exactDataObject(options, OPTION_KEYS);
    const request = canonicalRequest(normalized.request);
    const plan = canonicalPlanInput(normalized.plan);
    const input = exactFrozenRecord({
      contractVersion: POSTGRES_DETACHED_RESTORE_PLAN_CONTRACT_VERSION,
      request,
      captureCreatedAt: plan.captureCreatedAt,
      destinationDirectory: plan.destinationDirectory,
      destinationOwnedRoot: plan.destinationOwnedRoot,
      detachMode: plan.detachMode,
      holderId: plan.holderId,
      imagePlanId: plan.imagePlanId,
      leaseDurationMilliseconds: plan.leaseDurationMilliseconds,
      sourceArtifactDirectory: plan.sourceArtifactDirectory,
      sourceArtifactOwnedRoot: plan.sourceArtifactOwnedRoot,
    });
    const seedSha256 = planInputSha256(input);
    // The logical-writer launcher owns the formal stop operation ID and the
    // authoritative capture-attempt UUID. Neither has an injection seam, so
    // this root plan deliberately derives only identities its caller can use.
    const identities = exactFrozenRecord({
      renewalOperationId: deterministicOpaqueId(
        seedSha256,
        "lease-renewal-operation",
        "restore-renewal:",
      ),
      captureOperationId: deterministicOpaqueId(
        seedSha256,
        "checkpoint-capture-operation",
        "restore-capture:",
      ),
      captureArtifactId: deterministicOpaqueId(
        seedSha256,
        "checkpoint-capture-artifact",
        "restore-artifact:",
      ),
      captureCheckpointId: deterministicOpaqueId(
        seedSha256,
        "checkpoint-capture-checkpoint",
        "restore-checkpoint:",
      ),
      generationId: deterministicOpaqueId(
        seedSha256,
        "restore-generation",
        "restore-generation:",
      ),
      destinationIsolationProofId: deterministicOpaqueId(
        seedSha256,
        "destination-isolation-proof",
        "restore-destination-proof:",
      ),
      detachOperationId: deterministicOpaqueId(
        seedSha256,
        "writer-detach-operation",
        "restore-detach:",
      ),
      activationOperationId: deterministicOpaqueId(
        seedSha256,
        "attachment-activation-operation",
        "restore-activation:",
      ),
      launchAttemptId: deterministicOpaqueId(
        seedSha256,
        "writer-launch-attempt",
        "restore-launch:",
      ),
    });
    const hashedPlan = exactFrozenRecord({
      contractVersion: input.contractVersion,
      request,
      captureCreatedAt: input.captureCreatedAt,
      destinationDirectory: input.destinationDirectory,
      destinationOwnedRoot: input.destinationOwnedRoot,
      detachMode: input.detachMode,
      holderId: input.holderId,
      imagePlanId: input.imagePlanId,
      leaseDurationMilliseconds: input.leaseDurationMilliseconds,
      sourceArtifactDirectory: input.sourceArtifactDirectory,
      sourceArtifactOwnedRoot: input.sourceArtifactOwnedRoot,
      renewalOperationId: identities.renewalOperationId,
      captureOperationId: identities.captureOperationId,
      captureArtifactId: identities.captureArtifactId,
      captureCheckpointId: identities.captureCheckpointId,
      generationId: identities.generationId,
      destinationIsolationProofId: identities.destinationIsolationProofId,
      detachOperationId: identities.detachOperationId,
      activationOperationId: identities.activationOperationId,
      launchAttemptId: identities.launchAttemptId,
    });
    const result = exactFrozenRecord({
      contractVersion: hashedPlan.contractVersion,
      request: hashedPlan.request,
      captureCreatedAt: hashedPlan.captureCreatedAt,
      destinationDirectory: hashedPlan.destinationDirectory,
      destinationOwnedRoot: hashedPlan.destinationOwnedRoot,
      detachMode: hashedPlan.detachMode,
      holderId: hashedPlan.holderId,
      imagePlanId: hashedPlan.imagePlanId,
      leaseDurationMilliseconds: hashedPlan.leaseDurationMilliseconds,
      sourceArtifactDirectory: hashedPlan.sourceArtifactDirectory,
      sourceArtifactOwnedRoot: hashedPlan.sourceArtifactOwnedRoot,
      renewalOperationId: hashedPlan.renewalOperationId,
      captureOperationId: hashedPlan.captureOperationId,
      captureArtifactId: hashedPlan.captureArtifactId,
      captureCheckpointId: hashedPlan.captureCheckpointId,
      generationId: hashedPlan.generationId,
      destinationIsolationProofId:
        hashedPlan.destinationIsolationProofId,
      detachOperationId: hashedPlan.detachOperationId,
      activationOperationId: hashedPlan.activationOperationId,
      launchAttemptId: hashedPlan.launchAttemptId,
      planSha256: rootPlanSha256(hashedPlan),
    });
    callIntrinsic(weakSetAddIntrinsic, planBrands, [result]);
    return result;
  } catch (error) {
    if (
      error !== null &&
      (typeof error === "object" || typeof error === "function") &&
      !isProxyValue(error) &&
      callIntrinsic(weakSetHasIntrinsic, errorBrands, [error])
    ) {
      throw error;
    }
    fail();
  }
}

objectFreeze(PostgresDetachedRestorePlanError.prototype);
objectFreeze(PostgresDetachedRestorePlanError);
