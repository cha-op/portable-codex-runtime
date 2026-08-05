import { types as utilTypes } from "node:util";
import {
  basename as pathBasename,
  dirname as pathDirname,
  isAbsolute as pathIsAbsolute,
  parse as pathParse,
  resolve as pathResolve,
} from "node:path";

import { operationJournalBindingSha256 } from "./filesystem-operation-journal.mjs";
import {
  RESTORE_DESTINATION_GENERATION_OPERATION_KIND,
  SESSION_OPERATION_CONFLICT_CLASS,
  assertRestoreGenerationLaunchHandoffReceipt,
  createRestoreDestinationGenerationOperationRequestV2,
} from "./postgres-session-authority.mjs";
import {
  assertLogicalWriterLaunchStartedResult,
} from "./postgres-logical-writer-launcher.mjs";
import { StoppedDirectoryPublication } from "./stopped-directory-publication.mjs";
import {
  assertCheckpointDescriptor,
  assertLeaseGrant,
  assertSessionAttachment,
  assertSessionStorageRef,
  assertStorageMutationRequest,
  compareFencingEpochs,
} from "./session-storage-contracts.mjs";

const arrayIsArray = Array.isArray;
const arrayIncludesIntrinsic = Array.prototype.includes;
const arraySortIntrinsic = Array.prototype.sort;
const ArrayConstructor = Array;
const BigIntConstructor = BigInt;
const DateConstructor = Date;
const dateParseIntrinsic = Date.parse;
const dateToISOStringIntrinsic = Date.prototype.toISOString;
const functionToStringIntrinsic = Function.prototype.toString;
const jsonStringifyIntrinsic = JSON.stringify;
const objectCreate = Object.create;
const objectDefineProperty = Object.defineProperty;
const objectFreeze = Object.freeze;
const objectGetOwnPropertyDescriptor = Object.getOwnPropertyDescriptor;
const objectGetPrototypeOf = Object.getPrototypeOf;
const objectHasOwn = Object.hasOwn;
const objectIsFrozen = Object.isFrozen;
const objectPrototype = Object.prototype;
const numberIsFinite = Number.isFinite;
const PromiseConstructor = Promise;
const reflectApply = Reflect.apply;
const reflectOwnKeys = Reflect.ownKeys;
const regexpExecIntrinsic = RegExp.prototype.exec;
const stringStartsWithIntrinsic = String.prototype.startsWith;
const TypeErrorConstructor = TypeError;
const WeakSetConstructor = WeakSet;
const weakSetAddIntrinsic = WeakSet.prototype.add;
const weakSetHasIntrinsic = WeakSet.prototype.has;
const verifyCommittedRestoreDestinationIntrinsic =
  StoppedDirectoryPublication.prototype.verifyCommittedRestoreDestination;
const assertRestoreGenerationLaunchHandoffReceiptIntrinsic =
  assertRestoreGenerationLaunchHandoffReceipt;
const assertLogicalWriterLaunchStartedResultIntrinsic =
  assertLogicalWriterLaunchStartedResult;
const {
  isGeneratorFunction: isGeneratorFunctionValue,
  isGeneratorObject: isGeneratorObjectValue,
  isPromise: isPromiseValue,
  isProxy: isProxyValue,
} = utilTypes;

const NATIVE_FUNCTION_SOURCE_PATTERN =
  /\{\s*\[\s*native\s+code\s*\]\s*\}\s*$/u;
const OPAQUE_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u;
const REVISION_PATTERN = /^(?:0|[1-9][0-9]{0,18})$/u;
const SHA256_PATTERN = /^[0-9a-f]{64}$/u;
const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u;
const NUL_PATTERN = /\0/u;
const MAX_DATA_TREE_DEPTH = 24;
const MAX_DATA_TREE_NODES = 16_384;

const OPTION_KEYS = objectFreeze([
  "authority",
  "fleetCapabilityGate",
  "launcher",
  "operationGuard",
  "prepareRestore",
  "publication",
]);
const AUTHORITY_METHODS = objectFreeze([
  "cancelPreparedOperation",
  "claimRestoreDestinationGenerationDispatch",
  "finalizeRestoreDestinationGenerationAndReserveWriterLaunchAttempt",
  "markOperationUncertain",
  "readRestoreDestinationGenerationOperation",
  "reconcileOperation",
  "reserveOperation",
]);
const LAUNCHER_METHODS = objectFreeze([
  "prepareLaunchIntent",
  "runPreparedLaunch",
]);
const OPERATION_GUARD_METHODS = objectFreeze(["runExclusive"]);
const ADMISSION_KEYS = objectFreeze(["checkpoint", "request"]);
const PREPARATION_KEYS = objectFreeze([
  "artifactDirectory",
  "artifactOwnedRoot",
  "destinationDirectory",
  "destinationIsolationProofId",
  "destinationOwnedRoot",
  "generationId",
  "imageReservation",
  "launchAttemptId",
]);
const IMAGE_RESERVATION_KEYS = objectFreeze([
  "configBytes",
  "descriptor",
  "inspectCodex",
  "reservation",
]);
const SESSION_KEYS = objectFreeze([
  "createdAt",
  "document",
  "revision",
  "sessionId",
  "updatedAt",
]);
const SESSION_DOCUMENT_KEYS = objectFreeze([
  "activeOperation",
  "attachment",
  "backendCapabilities",
  "documentVersion",
  "lastOperation",
  "launch",
  "lease",
  "lifecycle",
  "manifest",
  "recovery",
  "storageRef",
  "writerEpoch",
]);
const ACTIVE_SESSION_STABLE_DOCUMENT_KEYS = objectFreeze([
  "attachment",
  "backendCapabilities",
  "documentVersion",
  "lastOperation",
  "launch",
  "lease",
  "lifecycle",
  "manifest",
  "recovery",
  "storageRef",
  "writerEpoch",
]);
const OPERATION_KEYS = objectFreeze([
  "conflictClass",
  "createdAt",
  "expectedSession",
  "kind",
  "operationId",
  "request",
  "requestSha256",
  "result",
  "retiredAt",
  "revision",
  "sessionId",
  "state",
  "updatedAt",
]);
const RESERVATION_KEYS = objectFreeze([
  "conflictClass",
  "createdAt",
  "expectedSessionRevision",
  "expiresAt",
  "kind",
  "operationId",
  "releasedAt",
  "requestSha256",
  "reservationId",
  "sessionId",
  "state",
  "updatedAt",
]);
const RESERVE_RECEIPT_KEYS = objectFreeze([
  "acquired",
  "operation",
  "reservation",
  "session",
  "status",
]);
const CLAIM_RECEIPT_KEYS = objectFreeze([
  "authorityNow",
  "catalogue",
  "dispatchGranted",
  "generation",
  "operation",
  "reservation",
  "session",
  "status",
]);
const RESTORE_READ_RECEIPT_KEYS = objectFreeze([
  "catalogue",
  "generation",
  "operation",
  "reservation",
  "session",
  "status",
]);
const GENERATION_KEYS = objectFreeze([
  "binding",
  "checkpointId",
  "claimedAt",
  "committedAt",
  "document",
  "generationId",
  "operationId",
  "sessionId",
  "state",
]);
const GENERATION_BINDING_KEYS = objectFreeze([
  "attachment",
  "captureAttemptId",
  "captureOperationId",
  "catalogueSha256",
  "checkpoint",
  "contractVersion",
  "destinationIsolationProofId",
  "destinationState",
  "generationId",
  "request",
  "reservationId",
]);
const CATALOGUE_KEYS = objectFreeze([
  "captureAttemptId",
  "checkpointId",
  "committedAt",
  "document",
  "sessionId",
]);
const CATALOGUE_DOCUMENT_KEYS = objectFreeze([
  "artifactProof",
  "contractVersion",
  "materialization",
  "result",
]);
const ARTIFACT_PROOF_KEYS = objectFreeze([
  "artifactManifestDigest",
  "captureOperationId",
  "modeledDigest",
]);
const COMPLETION_KEYS = objectFreeze([
  "materialization",
  "replayed",
  "result",
]);
const MATERIALIZATION_KEYS = objectFreeze([
  "artifactManifestDigest",
  "contractVersion",
  "coordinatorBindingSha256",
  "modeledDigest",
  "publicationId",
  "publicationKind",
  "stagedRoot",
  "treeIdentityDigest",
]);
const STAGED_ROOT_KEYS = objectFreeze([
  "filesystemId",
  "objectIdentityScheme",
  "objectId",
]);
const HANDOFF_RECEIPT_KEYS = objectFreeze([
  "generation",
  "launch",
  "restore",
  "session",
  "status",
]);
const HANDOFF_LAUNCH_KEYS = objectFreeze([
  "attempt",
  "operation",
  "reservation",
]);
const HANDOFF_RESTORE_KEYS = objectFreeze([
  "catalogue",
  "finalized",
  "operation",
  "reservation",
]);
const PROBE_KEYS = objectFreeze(["assertHeld"]);

const ERROR_MESSAGES = objectFreeze({
  invalid_postgres_restore_publication_launch_composition_options:
    "PostgreSQL restore publication and launch composition options are invalid",
  invalid_postgres_restore_publication_launch_composition_request:
    "PostgreSQL restore publication and launch composition request is invalid",
  postgres_restore_publication_launch_composition_outcome_uncertain:
    "PostgreSQL restore publication and launch composition outcome is uncertain",
  restore_launch_v2_fleet_capability_required:
    "Restore-to-launch version 2 requires confirmed fleet compatibility",
});
const INTERNAL_ERRORS = new WeakSetConstructor();

export const RESTORE_LAUNCH_V2_FLEET_CONFIRMED = objectFreeze(
  objectCreate(null),
);

function callIntrinsic(intrinsic, receiver, args) {
  return reflectApply(intrinsic, receiver, args);
}

function regexpTest(pattern, value) {
  return callIntrinsic(regexpExecIntrinsic, pattern, [value]) !== null;
}

function arrayIncludes(value, candidate) {
  return callIntrinsic(arrayIncludesIntrinsic, value, [candidate]);
}

function stringStartsWith(value, prefix) {
  return callIntrinsic(stringStartsWithIntrinsic, value, [prefix]);
}

function makeError(code) {
  const error = new PostgresRestorePublicationLaunchCompositionError(code);
  callIntrinsic(weakSetAddIntrinsic, INTERNAL_ERRORS, [error]);
  return error;
}

function fail(code) {
  throw makeError(code);
}

function ensure(condition, code) {
  if (!condition) fail(code);
}

function exactObject(value, expectedKeys, code, frozen = false) {
  ensure(
    value !== null &&
      typeof value === "object" &&
      !arrayIsArray(value) &&
      !isProxyValue(value),
    code,
  );
  let prototype;
  let keys;
  try {
    prototype = objectGetPrototypeOf(value);
    keys = reflectOwnKeys(value);
  } catch {
    fail(code);
  }
  ensure(
    (prototype === objectPrototype || prototype === null) &&
      keys.length === expectedKeys.length &&
      (!frozen || objectIsFrozen(value)),
    code,
  );
  const normalized = objectCreate(null);
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
    ensure(
      typeof key === "string" && arrayIncludes(expectedKeys, key),
      code,
    );
    let descriptor;
    try {
      descriptor = objectGetOwnPropertyDescriptor(value, key);
    } catch {
      fail(code);
    }
    ensure(
      descriptor?.enumerable === true && objectHasOwn(descriptor, "value"),
      code,
    );
    normalized[key] = descriptor.value;
  }
  for (let index = 0; index < expectedKeys.length; index += 1) {
    ensure(objectHasOwn(normalized, expectedKeys[index]), code);
  }
  return normalized;
}

function exactFrozenRecord(value) {
  const record = objectCreate(null);
  const keys = reflectOwnKeys(value);
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
    objectDefineProperty(record, key, {
      enumerable: true,
      value: value[key],
    });
  }
  return objectFreeze(record);
}

function snapshotData(value, state, code) {
  state.nodes += 1;
  ensure(
    state.depth <= MAX_DATA_TREE_DEPTH &&
      state.nodes <= MAX_DATA_TREE_NODES,
    code,
  );
  if (
    value === null ||
    typeof value === "string" ||
    typeof value === "boolean"
  ) {
    return value;
  }
  if (typeof value === "number") {
    ensure(numberIsFinite(value), code);
    return value;
  }
  ensure(
    typeof value === "object" &&
      !isProxyValue(value) &&
      !callIntrinsic(weakSetHasIntrinsic, state.seen, [value]),
    code,
  );
  callIntrinsic(weakSetAddIntrinsic, state.seen, [value]);
  ensure(objectIsFrozen(value), code);
  let keys;
  let prototype;
  try {
    keys = reflectOwnKeys(value);
    prototype = objectGetPrototypeOf(value);
  } catch {
    fail(code);
  }
  state.depth += 1;
  if (arrayIsArray(value)) {
    ensure(prototype === Array.prototype && keys.length === value.length + 1, code);
    const copy = new ArrayConstructor(value.length);
    for (let index = 0; index < value.length; index += 1) {
      const key = String(index);
      const descriptor = objectGetOwnPropertyDescriptor(value, key);
      ensure(
        descriptor?.enumerable === true && objectHasOwn(descriptor, "value"),
        code,
      );
      copy[index] = snapshotData(descriptor.value, state, code);
    }
    state.depth -= 1;
    return objectFreeze(copy);
  }
  ensure(prototype === objectPrototype || prototype === null, code);
  const stringKeys = new ArrayConstructor(keys.length);
  for (let index = 0; index < keys.length; index += 1) {
    ensure(typeof keys[index] === "string", code);
    stringKeys[index] = keys[index];
  }
  callIntrinsic(arraySortIntrinsic, stringKeys, []);
  const copy = objectCreate(null);
  for (let index = 0; index < stringKeys.length; index += 1) {
    const key = stringKeys[index];
    const descriptor = objectGetOwnPropertyDescriptor(value, key);
    ensure(
      descriptor?.enumerable === true && objectHasOwn(descriptor, "value"),
      code,
    );
    objectDefineProperty(copy, key, {
      enumerable: true,
      value: snapshotData(descriptor.value, state, code),
    });
  }
  state.depth -= 1;
  return objectFreeze(copy);
}

function canonicalData(value, code) {
  const snapshot = snapshotData(
    value,
    { depth: 0, nodes: 0, seen: new WeakSetConstructor() },
    code,
  );
  let serialized;
  try {
    serialized = callIntrinsic(jsonStringifyIntrinsic, JSON, [snapshot]);
  } catch {
    fail(code);
  }
  ensure(typeof serialized === "string", code);
  return serialized;
}

function sameData(left, right, code) {
  return canonicalData(left, code) === canonicalData(right, code);
}

function assertOpaqueId(value, code) {
  ensure(
    typeof value === "string" && regexpTest(OPAQUE_ID_PATTERN, value),
    code,
  );
  return value;
}

function timestampMilliseconds(value, code) {
  ensure(typeof value === "string", code);
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
    fail(code);
  }
  ensure(numberIsFinite(milliseconds) && canonical === value, code);
  return milliseconds;
}

function revision(value, code) {
  ensure(
    typeof value === "string" && regexpTest(REVISION_PATTERN, value),
    code,
  );
  try {
    return BigIntConstructor(value);
  } catch {
    fail(code);
  }
}

function assertCallable(value, code) {
  ensure(
    typeof value === "function" &&
      !isProxyValue(value) &&
      !isGeneratorFunctionValue(value),
    code,
  );
  let source;
  try {
    source = callIntrinsic(functionToStringIntrinsic, value, []);
  } catch {
    fail(code);
  }
  ensure(!regexpTest(NATIVE_FUNCTION_SOURCE_PATTERN, source), code);
  return value;
}

function lookupMethod(receiver, name, code) {
  ensure(
    receiver !== null &&
      (typeof receiver === "object" || typeof receiver === "function") &&
      !isProxyValue(receiver),
    code,
  );
  let current = receiver;
  while (current !== null) {
    let descriptor;
    try {
      descriptor = objectGetOwnPropertyDescriptor(current, name);
    } catch {
      fail(code);
    }
    if (descriptor !== undefined) {
      ensure(objectHasOwn(descriptor, "value"), code);
      return assertCallable(descriptor.value, code);
    }
    try {
      current = objectGetPrototypeOf(current);
    } catch {
      fail(code);
    }
  }
  fail(code);
}

function collaborator(value, methods, code) {
  const bound = objectCreate(null);
  for (let index = 0; index < methods.length; index += 1) {
    bound[methods[index]] = lookupMethod(value, methods[index], code);
  }
  return exactFrozenRecord({ methods: exactFrozenRecord(bound), receiver: value });
}

function isSafeNativePromise(value) {
  if (
    !isPromiseValue(value) ||
    isProxyValue(value) ||
    isGeneratorObjectValue(value)
  ) {
    return false;
  }
  let current = value;
  while (current !== null) {
    if (isProxyValue(current)) return false;
    const descriptor = objectGetOwnPropertyDescriptor(current, "constructor");
    if (descriptor !== undefined) {
      return (
        objectHasOwn(descriptor, "value") &&
        descriptor.value === PromiseConstructor
      );
    }
    current = objectGetPrototypeOf(current);
  }
  return false;
}

async function invokePromise(receiver, method, args, code) {
  let pending;
  try {
    pending = callIntrinsic(method, receiver, args);
  } catch {
    fail(code);
  }
  ensure(isSafeNativePromise(pending), code);
  try {
    return await pending;
  } catch {
    fail(code);
  }
}

async function invokeCollaborator(binding, name, args, code) {
  return invokePromise(binding.receiver, binding.methods[name], args, code);
}

function normalizeAdmission(value, code) {
  const input = exactObject(value, ADMISSION_KEYS, code);
  let checkpoint;
  let request;
  try {
    checkpoint = assertCheckpointDescriptor(input.checkpoint);
    request = assertStorageMutationRequest(input.request);
  } catch {
    fail(code);
  }
  ensure(
    checkpoint.checkpointClass === "clean" &&
      request.operation === "restore" &&
      checkpoint.sessionId === request.sessionId &&
      checkpoint.backendId === request.backendId &&
      request.target.kind === "checkpoint" &&
      request.target.checkpointId === checkpoint.checkpointId &&
      request.target.artifactId === checkpoint.artifactId,
    code,
  );
  return exactFrozenRecord({ checkpoint, request });
}

function normalizeSession(value, admission, code) {
  const session = exactObject(value, SESSION_KEYS, code, true);
  const document = exactObject(
    session.document,
    SESSION_DOCUMENT_KEYS,
    code,
    true,
  );
  let attachment;
  let lease;
  let storageRef;
  try {
    attachment = assertSessionAttachment(document.attachment);
    lease = assertLeaseGrant(document.lease);
    storageRef = assertSessionStorageRef(document.storageRef);
  } catch {
    fail(code);
  }
  ensure(
    session.sessionId === admission.checkpoint.sessionId &&
      document.lifecycle === "ATTACHED" &&
      document.activeOperation === null &&
      document.launch === null &&
      attachment.sessionId === admission.request.sessionId &&
      attachment.backendId === admission.request.backendId &&
      attachment.storageId === admission.request.storageId &&
      lease.sessionId === admission.request.sessionId &&
      lease.leaseId === admission.request.leaseId &&
      lease.holderId === admission.request.holderId &&
      lease.fencingEpoch === admission.request.fencingEpoch &&
      storageRef.sessionId === admission.request.sessionId &&
      storageRef.backendId === admission.request.backendId &&
      storageRef.storageId === admission.request.storageId &&
      compareFencingEpochs(
        admission.request.fencingEpoch,
        admission.checkpoint.sourceFencingEpoch,
      ) > 0,
    code,
  );
  canonicalData(value, code);
  return exactFrozenRecord({ attachment, lease, session: value, storageRef });
}

function normalizeRestoreReadReceipt(value, admission, code) {
  const receipt = exactObject(
    value,
    RESTORE_READ_RECEIPT_KEYS,
    code,
    true,
  );
  ensure(
    arrayIncludes(
      ["absent", "prepared", "starting", "uncertain", "committed"],
      receipt.status,
    ),
    code,
  );
  if (receipt.status === "absent") {
    ensure(
      receipt.operation === null &&
        receipt.reservation === null &&
        receipt.generation === null &&
        receipt.catalogue === null,
      code,
    );
    const fresh = normalizeSession(receipt.session, admission, code);
    return exactFrozenRecord({
      ...fresh,
      catalogue: null,
      generation: null,
      operation: null,
      reservation: null,
      status: "absent",
    });
  }

  const operation = exactObject(
    receipt.operation,
    OPERATION_KEYS,
    code,
    true,
  );
  const expected = normalizeSession(
    operation.expectedSession,
    admission,
    code,
  );
  ensure(
    operation.conflictClass === SESSION_OPERATION_CONFLICT_CLASS &&
      operation.kind === RESTORE_DESTINATION_GENERATION_OPERATION_KIND &&
      operation.operationId === admission.request.operationId &&
      operation.sessionId === admission.request.sessionId &&
      operation.state === receipt.status &&
      (receipt.status === "committed"
        ? operation.retiredAt === operation.updatedAt
        : operation.retiredAt === null) &&
      operation.request?.contractVersion === 2 &&
      sameData(operation.request.admission, admission, code) &&
      typeof operation.requestSha256 === "string" &&
      regexpTest(SHA256_PATTERN, operation.requestSha256),
    code,
  );
  timestampMilliseconds(operation.createdAt, code);
  timestampMilliseconds(operation.updatedAt, code);
  const operationRevision = revision(operation.revision, code);
  ensure(
    (receipt.status === "prepared" && operationRevision === 0n) ||
      (receipt.status === "starting" && operationRevision === 1n) ||
      (receipt.status === "uncertain" && operationRevision === 2n) ||
      (receipt.status === "committed" &&
        (operationRevision === 2n || operationRevision === 3n)),
    code,
  );
  const reservation = exactObject(
    receipt.reservation,
    RESERVATION_KEYS,
    code,
    true,
  );
  const active = receipt.status !== "committed";
  ensure(
    reservation.conflictClass === operation.conflictClass &&
      reservation.operationId === operation.operationId &&
      reservation.sessionId === operation.sessionId &&
      reservation.kind === operation.kind &&
      reservation.expectedSessionRevision === expected.session.revision &&
      reservation.requestSha256 === operation.requestSha256 &&
      reservation.createdAt === operation.createdAt &&
      reservation.updatedAt === operation.updatedAt &&
      reservation.state === (active ? receipt.status : "released") &&
      reservation.expiresAt === null &&
      (active
        ? reservation.releasedAt === null
        : reservation.releasedAt === operation.updatedAt),
    code,
  );
  if (active) {
    ensure(operation.result === null, code);
    validateActiveSession(
      receipt.session,
      expected.session,
      receipt.operation,
      receipt.reservation,
      code,
    );
  } else {
    ensure(operation.result !== null, code);
    const currentSession = exactObject(
      receipt.session,
      SESSION_KEYS,
      code,
      true,
    );
    ensure(currentSession.sessionId === expected.session.sessionId, code);
    canonicalData(receipt.session, code);
  }

  if (receipt.status === "prepared") {
    ensure(receipt.generation === null && receipt.catalogue === null, code);
    return exactFrozenRecord({
      ...expected,
      catalogue: null,
      generation: null,
      operation: receipt.operation,
      reservation: receipt.reservation,
      status: receipt.status,
    });
  }

  const generation = exactObject(
    receipt.generation,
    GENERATION_KEYS,
    code,
    true,
  );
  const generationClaimedAt = timestampMilliseconds(
    generation.claimedAt,
    code,
  );
  const operationCreatedAt = timestampMilliseconds(
    operation.createdAt,
    code,
  );
  const operationUpdatedAt = timestampMilliseconds(
    operation.updatedAt,
    code,
  );
  ensure(
    generation.operationId === operation.operationId &&
      generation.sessionId === operation.sessionId &&
      generation.checkpointId === admission.checkpoint.checkpointId &&
      generationClaimedAt >= operationCreatedAt &&
      generationClaimedAt <= operationUpdatedAt &&
      (receipt.status !== "starting" ||
        generation.claimedAt === operation.updatedAt) &&
      (receipt.status === "committed"
        ? generation.state === "committed" &&
          generation.committedAt === operation.updatedAt &&
          generation.document !== null
        : generation.state === "authorized" &&
          generation.committedAt === null &&
          generation.document === null),
    code,
  );
  const bindingInput = exactObject(
    generation.binding,
    GENERATION_BINDING_KEYS,
    code,
    true,
  );
  const binding = normalizeGenerationBinding(
    generation.binding,
    {
      admission,
      attachment: expected.attachment,
      destinationIsolationProofId:
        bindingInput.destinationIsolationProofId,
      generationId: generation.generationId,
      reservationId: reservation.reservationId,
    },
    code,
  );
  const catalogue = normalizeCatalogue(receipt.catalogue, admission, code);
  ensure(
    binding.captureAttemptId === catalogue.catalogue.captureAttemptId &&
      binding.captureOperationId === catalogue.artifactProof.captureOperationId,
    code,
  );
  return exactFrozenRecord({
    ...expected,
    artifactProof: catalogue.artifactProof,
    binding,
    catalogue: catalogue.catalogue,
    generation: receipt.generation,
    operation: receipt.operation,
    reservation: receipt.reservation,
    status: receipt.status,
  });
}

function canonicalDirectory(value, code) {
  ensure(
    typeof value === "string" &&
      !regexpTest(NUL_PATTERN, value) &&
      pathIsAbsolute(value) &&
      pathResolve(value) === value &&
      value !== pathParse(value).root,
    code,
  );
  return value;
}

function directPathPlan(directoryValue, ownedRootValue, code) {
  const directory = canonicalDirectory(directoryValue, code);
  const ownedRoot = canonicalDirectory(ownedRootValue, code);
  const name = pathBasename(directory);
  ensure(
    pathDirname(directory) === ownedRoot &&
      name.length > 0 &&
      name !== "." &&
      name !== ".." &&
      pathBasename(name) === name,
    code,
  );
  return exactFrozenRecord({ directory, ownedRoot });
}

function normalizeImageReservation(value, code) {
  const image = exactObject(value, IMAGE_RESERVATION_KEYS, code);
  ensure(
    typeof image.inspectCodex === "function" &&
      !isProxyValue(image.inspectCodex) &&
      image.reservation !== null &&
      typeof image.reservation === "object" &&
      !isProxyValue(image.reservation),
    code,
  );
  return exactFrozenRecord({
    configBytes: image.configBytes,
    descriptor: image.descriptor,
    inspectCodex: image.inspectCodex,
    reservation: image.reservation,
  });
}

function normalizePreparation(value, admission, code) {
  const input = exactObject(value, PREPARATION_KEYS, code);
  const destinationIsolationProofId = assertOpaqueId(
    input.destinationIsolationProofId,
    code,
  );
  const generationId = assertOpaqueId(input.generationId, code);
  const launchAttemptId = assertOpaqueId(input.launchAttemptId, code);
  const imageReservation = normalizeImageReservation(
    input.imageReservation,
    code,
  );
  ensure(launchAttemptId !== admission.request.operationId, code);
  const artifact = directPathPlan(
    input.artifactDirectory,
    input.artifactOwnedRoot,
    code,
  );
  const destination = directPathPlan(
    input.destinationDirectory,
    input.destinationOwnedRoot,
    code,
  );
  ensure(
    artifact.ownedRoot !== destination.ownedRoot &&
      !stringStartsWith(artifact.ownedRoot, `${destination.ownedRoot}/`) &&
      !stringStartsWith(destination.ownedRoot, `${artifact.ownedRoot}/`),
    code,
  );
  return exactFrozenRecord({
    artifact,
    admission,
    destination,
    destinationIsolationProofId,
    generationId,
    imageReservation,
    launchAttemptId,
  });
}

function validateOperation(
  operationValue,
  expected,
  state,
  expectedRevision,
  code,
) {
  const operation = exactObject(operationValue, OPERATION_KEYS, code, true);
  ensure(
    operation.conflictClass === SESSION_OPERATION_CONFLICT_CLASS &&
      operation.kind === RESTORE_DESTINATION_GENERATION_OPERATION_KIND &&
      operation.operationId === expected.operationId &&
      operation.sessionId === expected.expectedSession.sessionId &&
      operation.state === state &&
      operation.revision === expectedRevision &&
      operation.retiredAt === null &&
      operation.result === null &&
      sameData(operation.expectedSession, expected.expectedSession, code) &&
      sameData(operation.request, expected.request, code) &&
      typeof operation.requestSha256 === "string" &&
      regexpTest(SHA256_PATTERN, operation.requestSha256),
    code,
  );
  timestampMilliseconds(operation.createdAt, code);
  timestampMilliseconds(operation.updatedAt, code);
  return operation;
}

function validateReservation(
  reservationValue,
  operation,
  expectedSession,
  state,
  code,
) {
  const reservation = exactObject(
    reservationValue,
    RESERVATION_KEYS,
    code,
    true,
  );
  ensure(
    reservation.conflictClass === SESSION_OPERATION_CONFLICT_CLASS &&
      reservation.operationId === operation.operationId &&
      reservation.sessionId === operation.sessionId &&
      reservation.kind === operation.kind &&
      reservation.expectedSessionRevision === expectedSession.revision &&
      reservation.requestSha256 === operation.requestSha256 &&
      reservation.createdAt === operation.createdAt &&
      reservation.updatedAt === operation.updatedAt &&
      reservation.state === state &&
      reservation.releasedAt === null &&
      reservation.expiresAt === null,
    code,
  );
  return reservation;
}

function validateActiveSession(
  sessionValue,
  expectedSession,
  operation,
  reservation,
  code,
) {
  const session = exactObject(sessionValue, SESSION_KEYS, code, true);
  const document = exactObject(
    session.document,
    SESSION_DOCUMENT_KEYS,
    code,
    true,
  );
  const active = exactObject(
    document.activeOperation,
    [
      "conflictClass",
      "expectedSessionRevision",
      "kind",
      "operationId",
      "operationRevision",
      "requestSha256",
      "reservationId",
      "state",
    ],
    code,
    true,
  );
  ensure(
    session.sessionId === expectedSession.sessionId &&
      session.createdAt === expectedSession.createdAt &&
      session.updatedAt === operation.updatedAt &&
      revision(session.revision, code) ===
        revision(expectedSession.revision, code) +
          revision(operation.revision, code) +
          1n &&
      active.operationId === operation.operationId &&
      active.operationRevision === operation.revision &&
      active.reservationId === reservation.reservationId &&
      active.state === operation.state &&
      active.requestSha256 === operation.requestSha256,
    code,
  );
  const expectedDocument = exactObject(
    expectedSession.document,
    SESSION_DOCUMENT_KEYS,
    code,
    true,
  );
  for (
    let index = 0;
    index < ACTIVE_SESSION_STABLE_DOCUMENT_KEYS.length;
    index += 1
  ) {
    const key = ACTIVE_SESSION_STABLE_DOCUMENT_KEYS[index];
    ensure(sameData(document[key], expectedDocument[key], code), code);
  }
  canonicalData(sessionValue, code);
  return sessionValue;
}

function normalizeReserveReceipt(value, expected, code) {
  const receipt = exactObject(value, RESERVE_RECEIPT_KEYS, code, true);
  ensure(typeof receipt.acquired === "boolean" && receipt.status === "prepared", code);
  const operation = validateOperation(
    receipt.operation,
    expected,
    "prepared",
    "0",
    code,
  );
  const reservation = validateReservation(
    receipt.reservation,
    operation,
    expected.expectedSession,
    "prepared",
    code,
  );
  validateActiveSession(
    receipt.session,
    expected.expectedSession,
    operation,
    reservation,
    code,
  );
  return exactFrozenRecord({
    acquired: receipt.acquired,
    operation,
    reservation,
  });
}

function normalizeGenerationBinding(
  value,
  {
    admission,
    attachment: expectedAttachment,
    destinationIsolationProofId,
    generationId,
    reservationId,
  },
  code,
) {
  const binding = exactObject(value, GENERATION_BINDING_KEYS, code, true);
  ensure(
    binding.contractVersion === 1 &&
      binding.destinationState === "detached" &&
      binding.destinationIsolationProofId === destinationIsolationProofId &&
      binding.generationId === generationId &&
      binding.reservationId === reservationId &&
      typeof binding.captureAttemptId === "string" &&
      regexpTest(UUID_PATTERN, binding.captureAttemptId) &&
      typeof binding.captureOperationId === "string" &&
      regexpTest(OPAQUE_ID_PATTERN, binding.captureOperationId) &&
      sameData(binding.attachment, expectedAttachment, code) &&
      sameData(binding.checkpoint, admission.checkpoint, code) &&
      sameData(binding.request, admission.request, code) &&
      typeof binding.catalogueSha256 === "string" &&
      regexpTest(SHA256_PATTERN, binding.catalogueSha256),
    code,
  );
  return value;
}

function normalizeCatalogue(value, admission, code) {
  const catalogue = exactObject(value, CATALOGUE_KEYS, code, true);
  const document = exactObject(
    catalogue.document,
    CATALOGUE_DOCUMENT_KEYS,
    code,
    true,
  );
  const artifactProof = exactObject(
    document.artifactProof,
    ARTIFACT_PROOF_KEYS,
    code,
    true,
  );
  ensure(
    catalogue.checkpointId === admission.checkpoint.checkpointId &&
      catalogue.sessionId === admission.checkpoint.sessionId &&
      typeof artifactProof.artifactManifestDigest === "string" &&
      regexpTest(SHA256_PATTERN, artifactProof.artifactManifestDigest) &&
      typeof artifactProof.modeledDigest === "string" &&
      regexpTest(SHA256_PATTERN, artifactProof.modeledDigest) &&
      typeof artifactProof.captureOperationId === "string" &&
      regexpTest(OPAQUE_ID_PATTERN, artifactProof.captureOperationId),
    code,
  );
  canonicalData(value, code);
  return exactFrozenRecord({ artifactProof: document.artifactProof, catalogue: value });
}

function normalizeClaimReceipt(value, expected, preparation, code) {
  const receipt = exactObject(value, CLAIM_RECEIPT_KEYS, code, true);
  ensure(typeof receipt.dispatchGranted === "boolean", code);
  if (!receipt.dispatchGranted) {
    const replay = normalizeRestoreReadReceipt(
      exactFrozenRecord({
        catalogue: receipt.catalogue,
        generation: receipt.generation,
        operation: receipt.operation,
        reservation: receipt.reservation,
        session: receipt.session,
        status: receipt.status,
      }),
      preparation.admission,
      code,
    );
    ensure(
      replay.status !== "absent" &&
        replay.status !== "prepared" &&
        replay.generation.generationId === preparation.generationId &&
        replay.binding.destinationIsolationProofId ===
          preparation.destinationIsolationProofId &&
        sameData(replay.operation.expectedSession, expected.expectedSession, code) &&
        sameData(replay.operation.request, expected.request, code),
      code,
    );
    timestampMilliseconds(receipt.authorityNow, code);
    return exactFrozenRecord({
      ...replay,
      authorityNow: receipt.authorityNow,
      dispatchGranted: false,
    });
  }
  ensure(receipt.status === "starting", code);
  const operation = validateOperation(
    receipt.operation,
    expected,
    "starting",
    "1",
    code,
  );
  const reservation = validateReservation(
    receipt.reservation,
    operation,
    expected.expectedSession,
    "starting",
    code,
  );
  const session = validateActiveSession(
    receipt.session,
    expected.expectedSession,
    operation,
    reservation,
    code,
  );
  const generation = exactObject(
    receipt.generation,
    GENERATION_KEYS,
    code,
    true,
  );
  ensure(
    generation.state === "authorized" &&
      generation.generationId === preparation.generationId &&
      generation.operationId === expected.operationId &&
      generation.sessionId === expected.expectedSession.sessionId &&
      generation.checkpointId === preparation.admission.checkpoint.checkpointId &&
      generation.committedAt === null &&
      generation.document === null &&
      generation.claimedAt === operation.updatedAt &&
      timestampMilliseconds(receipt.authorityNow, code) >=
        timestampMilliseconds(generation.claimedAt, code),
    code,
  );
  const binding = normalizeGenerationBinding(
    generation.binding,
    {
      admission: preparation.admission,
      attachment: expected.expectedSession.document.attachment,
      destinationIsolationProofId: preparation.destinationIsolationProofId,
      generationId: preparation.generationId,
      reservationId: reservation.reservationId,
    },
    code,
  );
  const catalogue = normalizeCatalogue(
    receipt.catalogue,
    preparation.admission,
    code,
  );
  ensure(
    binding.captureAttemptId === catalogue.catalogue.captureAttemptId &&
      binding.captureOperationId ===
        catalogue.artifactProof.captureOperationId,
    code,
  );
  return exactFrozenRecord({
    artifactProof: catalogue.artifactProof,
    authorityNow: receipt.authorityNow,
    binding,
    catalogue: catalogue.catalogue,
    dispatchGranted: true,
    generation: receipt.generation,
    operation,
    reservation,
    session,
  });
}

function normalizeCompletion(value, claim, typedRequest, code) {
  const completion = exactObject(value, COMPLETION_KEYS, code, true);
  const materialization = exactObject(
    completion.materialization,
    MATERIALIZATION_KEYS,
    code,
    true,
  );
  const stagedRoot = exactObject(
    materialization.stagedRoot,
    STAGED_ROOT_KEYS,
    code,
    true,
  );
  ensure(
    typeof completion.replayed === "boolean" &&
      sameData(completion.result, typedRequest.predeterminedResult, code) &&
      materialization.contractVersion === 3 &&
      materialization.publicationKind === "restore-destination" &&
      materialization.artifactManifestDigest ===
        claim.artifactProof.artifactManifestDigest &&
      materialization.modeledDigest === claim.artifactProof.modeledDigest &&
      materialization.coordinatorBindingSha256 ===
        operationJournalBindingSha256(claim.binding) &&
      typeof materialization.publicationId === "string" &&
      regexpTest(OPAQUE_ID_PATTERN, materialization.publicationId) &&
      typeof materialization.treeIdentityDigest === "string" &&
      regexpTest(SHA256_PATTERN, materialization.treeIdentityDigest) &&
      typeof stagedRoot.filesystemId === "string" &&
      regexpTest(OPAQUE_ID_PATTERN, stagedRoot.filesystemId) &&
      typeof stagedRoot.objectIdentityScheme === "string" &&
      regexpTest(OPAQUE_ID_PATTERN, stagedRoot.objectIdentityScheme) &&
      typeof stagedRoot.objectId === "string" &&
      stagedRoot.objectId.length >= 1,
    code,
  );
  canonicalData(value, code);
  return value;
}

function normalizeHandoffReceipt(
  value,
  handoffInput,
  claim,
  launchIntent,
  completion,
  code,
) {
  let validated;
  try {
    validated = callIntrinsic(
      assertRestoreGenerationLaunchHandoffReceiptIntrinsic,
      undefined,
      [exactFrozenRecord({ input: handoffInput, receipt: value })],
    );
  } catch {
    fail(code);
  }
  const receipt = exactObject(validated, HANDOFF_RECEIPT_KEYS, code, true);
  // A committed receipt intentionally reuses the terminal operation result in
  // its attempt record. The authority validator already proves and freezes the
  // complete graph, so a second alias-rejecting whole-tree snapshot would
  // incorrectly reject that canonical replay shape.
  const launch = exactObject(receipt.launch, HANDOFF_LAUNCH_KEYS, code, true);
  const restore = exactObject(receipt.restore, HANDOFF_RESTORE_KEYS, code, true);
  const generation = exactObject(receipt.generation, GENERATION_KEYS, code, true);
  const expectedGenerationDocument = exactFrozenRecord({
    artifactProof: claim.artifactProof,
    contractVersion: 2,
    materialization: completion.materialization,
    result: completion.result,
  });
  ensure(
    arrayIncludes(
      ["prepared", "starting", "uncertain", "committed"],
      receipt.status,
    ) &&
      generation.state === "committed" &&
      generation.generationId === claim.generation.generationId &&
      generation.operationId === claim.generation.operationId &&
      generation.sessionId === claim.generation.sessionId &&
      generation.checkpointId === claim.generation.checkpointId &&
      generation.claimedAt === claim.generation.claimedAt &&
      sameData(generation.binding, claim.binding, code) &&
      sameData(generation.document, expectedGenerationDocument, code) &&
      sameData(restore.catalogue, claim.catalogue, code) &&
      restore.operation.operationId === claim.operation.operationId &&
      restore.operation.state === "committed" &&
      launch.attempt.launchAttemptId === launchIntent.launchAttemptId &&
      launch.operation.operationId === launchIntent.launchAttemptId &&
      launch.operation.state === receipt.status,
    code,
  );
  return validated;
}

function normalizeProbe(value, code) {
  const probe = exactObject(value, PROBE_KEYS, code, true);
  return exactFrozenRecord({ assertHeld: assertCallable(probe.assertHeld, code) });
}

async function assertGuardHeld(probe, code) {
  await invokePromise(undefined, probe.assertHeld, [], code);
}

async function bestEffortCancelPrepared(authority, baseInput, code) {
  try {
    const receipt = await invokeCollaborator(
      authority,
      "reconcileOperation",
      [baseInput],
      code,
    );
    const normalized = exactObject(
      receipt,
      ["operation", "reservation", "session", "status"],
      code,
      true,
    );
    if (normalized.status !== "prepared") return;
    await invokeCollaborator(
      authority,
      "cancelPreparedOperation",
      [
        exactFrozenRecord({
          ...baseInput,
          expectedOperationRevision: "0",
          reason: "restore-publication-not-started",
        }),
      ],
      code,
    );
  } catch {
    // The durable operation remains the recovery authority.
  }
}

async function bestEffortMarkUncertain(
  authority,
  baseInput,
  expectedOperationRevision,
  code,
) {
  try {
    await invokeCollaborator(
      authority,
      "markOperationUncertain",
      [
        exactFrozenRecord({
          ...baseInput,
          expectedOperationRevision,
        }),
      ],
      code,
    );
  } catch {
    // The caller still receives one fixed fail-closed outcome.
  }
}

export class PostgresRestorePublicationLaunchCompositionError extends Error {
  constructor(code) {
    if (typeof code !== "string" || !objectHasOwn(ERROR_MESSAGES, code)) {
      throw new TypeErrorConstructor(
        "Unsupported PostgreSQL restore publication and launch composition error",
      );
    }
    super(ERROR_MESSAGES[code]);
    objectDefineProperty(this, "name", {
      value: "PostgresRestorePublicationLaunchCompositionError",
    });
    objectDefineProperty(this, "code", { enumerable: true, value: code });
    objectDefineProperty(this, "retryable", { enumerable: true, value: false });
    objectDefineProperty(this, "stack", {
      value:
        `PostgresRestorePublicationLaunchCompositionError: ` +
        ERROR_MESSAGES[code],
    });
    objectFreeze(this);
  }
}

export function createPostgresRestorePublicationLaunchComposition(...args) {
  const optionCode =
    "invalid_postgres_restore_publication_launch_composition_options";
  const requestCode =
    "invalid_postgres_restore_publication_launch_composition_request";
  const outcomeCode =
    "postgres_restore_publication_launch_composition_outcome_uncertain";
  const fleetCode = "restore_launch_v2_fleet_capability_required";
  ensure(args.length === 1, optionCode);
  const options = exactObject(args[0], OPTION_KEYS, optionCode);
  const authority = collaborator(options.authority, AUTHORITY_METHODS, optionCode);
  const launcher = collaborator(options.launcher, LAUNCHER_METHODS, optionCode);
  const operationGuard = collaborator(
    options.operationGuard,
    OPERATION_GUARD_METHODS,
    optionCode,
  );
  ensure(
    options.publication !== null &&
      typeof options.publication === "object" &&
      !isProxyValue(options.publication) &&
      options.publication instanceof StoppedDirectoryPublication &&
      objectIsFrozen(options.publication),
    optionCode,
  );
  const publication = options.publication;
  const fleetCapabilityGate = assertCallable(
    options.fleetCapabilityGate,
    optionCode,
  );
  const prepareRestore = assertCallable(options.prepareRestore, optionCode);

  const readRestore = async (admission) =>
    normalizeRestoreReadReceipt(
      await invokeCollaborator(
        authority,
        "readRestoreDestinationGenerationOperation",
        [
          exactFrozenRecord({
            checkpoint: admission.checkpoint,
            request: admission.request,
          }),
        ],
        outcomeCode,
      ),
      admission,
      outcomeCode,
    );

  const runRestore = async function runRestore(admissionValue, publishValue) {
    const admission = normalizeAdmission(admissionValue, requestCode);
    const publish = assertCallable(publishValue, requestCode);
    let baseInput = null;
    let createdReservation = false;
    let dispatchDefinitelyBegan = false;
    let handoffConfirmed = false;
    let uncertaintyRevision = null;
    let publicationCompletion = null;
    let confirmedHandoff = null;
    let preparedImageReservation = null;
    let preparedLaunchAttemptId = null;
    try {
      const initial = await readRestore(admission);
      if (initial.status === "absent") {
        const gateContext = exactFrozenRecord({
          capability: "restore-generation-v2-launch-handoff-v1",
          contractVersion: 1,
        });
        const gateResult = await invokePromise(
          undefined,
          fleetCapabilityGate,
          [gateContext],
          fleetCode,
        );
        ensure(
          gateResult === RESTORE_LAUNCH_V2_FLEET_CONFIRMED,
          fleetCode,
        );
      }

      const durableRestore =
        initial.status === "absent"
          ? null
          : exactFrozenRecord({
              catalogue: initial.catalogue,
              generation: initial.generation,
              operation: initial.operation,
              reservation: initial.reservation,
              status: initial.status,
            });
      const preparation = normalizePreparation(
        await invokePromise(
          undefined,
          prepareRestore,
          [
            exactFrozenRecord({
              admission,
              durableRestore,
              expectedSession: initial.session,
            }),
          ],
          outcomeCode,
        ),
        admission,
        outcomeCode,
      );
      const launchIntent = await invokeCollaborator(
        launcher,
        "prepareLaunchIntent",
        [
          exactFrozenRecord({
            expectedSession: initial.session,
            imageReservation: preparation.imageReservation,
            launchAttemptId: preparation.launchAttemptId,
          }),
        ],
        outcomeCode,
      );
      ensure(
        objectIsFrozen(launchIntent) &&
          launchIntent.launchAttemptId === preparation.launchAttemptId,
        outcomeCode,
      );
      canonicalData(launchIntent, outcomeCode);
      if (initial.operation !== null) {
        ensure(
          sameData(
            initial.operation.request.launchIntent,
            launchIntent,
            outcomeCode,
          ) &&
            (initial.generation === null ||
              (initial.generation.generationId === preparation.generationId &&
                initial.binding.destinationIsolationProofId ===
                  preparation.destinationIsolationProofId)),
          outcomeCode,
        );
      }
      const typedRequest =
        createRestoreDestinationGenerationOperationRequestV2({
          admission,
          expectedSession: initial.session,
          launchIntent,
        });
      baseInput = exactFrozenRecord({
        expectedSession: initial.session,
        kind: RESTORE_DESTINATION_GENERATION_OPERATION_KIND,
        operationId: admission.request.operationId,
        request: typedRequest,
      });
      if (initial.operation !== null) {
        ensure(
          sameData(
            initial.operation.expectedSession,
            baseInput.expectedSession,
            outcomeCode,
          ) &&
            sameData(initial.operation.request, baseInput.request, outcomeCode),
          outcomeCode,
        );
      }

      const guarded = await invokeCollaborator(
        operationGuard,
        "runExclusive",
        [
          admission.request.operationId,
          async (probeValue) => {
            const probe = normalizeProbe(probeValue, outcomeCode);
            let durable = await readRestore(admission);
            await assertGuardHeld(probe, outcomeCode);
            if (durable.operation !== null) {
              ensure(
                sameData(
                  durable.operation.expectedSession,
                  baseInput.expectedSession,
                  outcomeCode,
                ) &&
                  sameData(
                    durable.operation.request,
                    baseInput.request,
                    outcomeCode,
                  ),
                outcomeCode,
              );
            } else {
              ensure(initial.status === "absent", outcomeCode);
              let reserveValue;
              try {
                reserveValue = await invokeCollaborator(
                  authority,
                  "reserveOperation",
                  [baseInput],
                  outcomeCode,
                );
              } catch {
                durable = await readRestore(admission);
                ensure(durable.status === "prepared", outcomeCode);
              }
              if (reserveValue !== undefined) {
                const reserve = normalizeReserveReceipt(
                  reserveValue,
                  baseInput,
                  outcomeCode,
                );
                createdReservation = reserve.acquired;
                durable = exactFrozenRecord({
                  catalogue: null,
                  generation: null,
                  operation: reserve.operation,
                  reservation: reserve.reservation,
                  session: baseInput.expectedSession,
                  status: "prepared",
                });
              }
            }

            let claim;
            if (durable.status === "prepared") {
              try {
                claim = normalizeClaimReceipt(
                  await invokeCollaborator(
                    authority,
                    "claimRestoreDestinationGenerationDispatch",
                    [
                      exactFrozenRecord({
                        ...baseInput,
                        destinationIsolationProofId:
                          preparation.destinationIsolationProofId,
                        expectedOperationRevision: "0",
                        generationId: preparation.generationId,
                      }),
                    ],
                    outcomeCode,
                  ),
                  baseInput,
                  preparation,
                  outcomeCode,
                );
              } catch {
                const reconciled = await readRestore(admission);
                ensure(
                  reconciled.status !== "absent" &&
                    reconciled.status !== "prepared" &&
                    reconciled.generation.generationId ===
                      preparation.generationId &&
                    reconciled.binding.destinationIsolationProofId ===
                      preparation.destinationIsolationProofId &&
                    sameData(
                      reconciled.operation.request,
                      baseInput.request,
                      outcomeCode,
                    ),
                  outcomeCode,
                );
                claim = exactFrozenRecord({
                  ...reconciled,
                  authorityNow: reconciled.generation.claimedAt,
                  dispatchGranted: false,
                });
              }
            } else {
              ensure(
                durable.generation !== null &&
                  durable.generation.generationId === preparation.generationId &&
                  durable.binding.destinationIsolationProofId ===
                    preparation.destinationIsolationProofId,
                outcomeCode,
              );
              claim = exactFrozenRecord({
                ...durable,
                authorityNow: durable.generation.claimedAt,
                dispatchGranted: false,
              });
            }
            dispatchDefinitelyBegan = true;
            uncertaintyRevision =
              claim.operation.state === "starting"
                ? claim.operation.revision
                : null;
            await assertGuardHeld(probe, outcomeCode);
            const publicationMode = claim.dispatchGranted
              ? "fresh-or-exact-replay"
              : "committed-only";
            const publicationContext = exactFrozenRecord({
              artifactDirectory: preparation.artifact.directory,
              artifactOwnedRoot: preparation.artifact.ownedRoot,
              artifactProof: claim.artifactProof,
              canonicalLease: initial.lease,
              destinationDirectory: preparation.destination.directory,
              destinationIsolationProofId:
                preparation.destinationIsolationProofId,
              destinationOwnedRoot: preparation.destination.ownedRoot,
              destinationState: "detached",
              generationBinding: claim.binding,
              now: callIntrinsic(dateParseIntrinsic, DateConstructor, [
                claim.authorityNow,
              ]),
              publicationMode,
              reservationId: claim.reservation.reservationId,
              result: typedRequest.predeterminedResult,
              storageRef: initial.storageRef,
            });
            const completion = normalizeCompletion(
              await invokePromise(
                undefined,
                publish,
                [publicationContext],
                outcomeCode,
              ),
              claim,
              typedRequest,
              outcomeCode,
            );
            publicationCompletion = completion;
            const verified = normalizeCompletion(
              await invokePromise(
                publication,
                verifyCommittedRestoreDestinationIntrinsic,
                [
                  exactFrozenRecord({
                    artifactDirectory: preparation.artifact.directory,
                    artifactOwnedRoot: preparation.artifact.ownedRoot,
                    artifactProof: claim.artifactProof,
                    binding: claim.binding,
                    destinationDirectory: preparation.destination.directory,
                    destinationOwnedRoot: preparation.destination.ownedRoot,
                    operationId: admission.request.operationId,
                    request: admission.request,
                    result: typedRequest.predeterminedResult,
                  }),
                ],
                outcomeCode,
              ),
              claim,
              typedRequest,
              outcomeCode,
            );
            ensure(
              sameData(
                verified.materialization,
                completion.materialization,
                outcomeCode,
              ) && sameData(verified.result, completion.result, outcomeCode),
              outcomeCode,
            );
            await assertGuardHeld(probe, outcomeCode);
            const expectedOperationRevision =
              claim.operation.state === "committed"
                ? claim.operation.revision === "2"
                  ? "1"
                  : "2"
                : claim.operation.revision;
            const handoffInput = exactFrozenRecord({
              launch: launchIntent,
              restore: exactFrozenRecord({
                ...baseInput,
                completion,
                expectedOperationRevision,
              }),
            });
            let rawHandoff;
            try {
              rawHandoff = await invokeCollaborator(
                authority,
                "finalizeRestoreDestinationGenerationAndReserveWriterLaunchAttempt",
                [handoffInput],
                outcomeCode,
              );
            } catch {
              await assertGuardHeld(probe, outcomeCode);
              rawHandoff = await invokeCollaborator(
                authority,
                "finalizeRestoreDestinationGenerationAndReserveWriterLaunchAttempt",
                [handoffInput],
                outcomeCode,
              );
            }
            const handoff = normalizeHandoffReceipt(
              rawHandoff,
              handoffInput,
              claim,
              launchIntent,
              completion,
              outcomeCode,
            );
            confirmedHandoff = handoff;
            preparedImageReservation = preparation.imageReservation;
            preparedLaunchAttemptId = preparation.launchAttemptId;
            handoffConfirmed = true;
            return exactFrozenRecord({
              completion,
              handoff,
              imageReservation: preparation.imageReservation,
              launchAttemptId: preparation.launchAttemptId,
            });
          },
        ],
        outcomeCode,
      );
      const guardedResult = exactObject(
        guarded,
        ["completion", "handoff", "imageReservation", "launchAttemptId"],
        outcomeCode,
        true,
      );
      ensure(
        handoffConfirmed &&
          guardedResult.completion === publicationCompletion &&
          guardedResult.handoff === confirmedHandoff &&
          guardedResult.imageReservation === preparedImageReservation &&
          guardedResult.launchAttemptId === preparedLaunchAttemptId,
        outcomeCode,
      );
      const rawLaunchResult = await invokeCollaborator(
        launcher,
        "runPreparedLaunch",
        [
          exactFrozenRecord({
            imageReservation: guardedResult.imageReservation,
            launchAttemptId: guardedResult.launchAttemptId,
          }),
        ],
        outcomeCode,
      );
      let launchResult;
      try {
        launchResult = callIntrinsic(
          assertLogicalWriterLaunchStartedResultIntrinsic,
          undefined,
          [
            exactFrozenRecord({
              handoff: exactFrozenRecord({
                attempt: guardedResult.handoff.launch.attempt,
                operation: guardedResult.handoff.launch.operation,
                reservation: guardedResult.handoff.launch.reservation,
                session: guardedResult.handoff.session,
                status: guardedResult.handoff.status,
              }),
              result: rawLaunchResult,
            }),
          ],
        );
      } catch {
        fail(outcomeCode);
      }
      ensure(launchResult.status === "started", outcomeCode);
      return guardedResult.completion;
    } catch (error) {
      if (callIntrinsic(weakSetHasIntrinsic, INTERNAL_ERRORS, [error])) {
        if (error.code === requestCode || error.code === fleetCode) throw error;
      }
      if (
        dispatchDefinitelyBegan &&
        !handoffConfirmed &&
        baseInput !== null &&
        uncertaintyRevision !== null
      ) {
        await bestEffortMarkUncertain(
          authority,
          baseInput,
          uncertaintyRevision,
          outcomeCode,
        );
      } else if (
        createdReservation &&
        !dispatchDefinitelyBegan &&
        baseInput !== null
      ) {
        await bestEffortCancelPrepared(authority, baseInput, outcomeCode);
      }
      fail(outcomeCode);
    }
  };

  return exactFrozenRecord({
    restoreContextContractVersion: 3,
    runRestore,
  });
}

objectFreeze(PostgresRestorePublicationLaunchCompositionError.prototype);
objectFreeze(PostgresRestorePublicationLaunchCompositionError);
