import { createHash as createHashExport } from "node:crypto";
import {
  basename as pathBasenameExport,
  dirname as pathDirnameExport,
  isAbsolute as pathIsAbsoluteExport,
  parse as pathParseExport,
  resolve as pathResolveExport,
} from "node:path";
import { types as utilTypes } from "node:util";

import {
  CHECKPOINT_CAPTURE_OPERATION_KIND,
  SESSION_OPERATION_CONFLICT_CLASS,
  createCheckpointCaptureOperationRequest,
} from "./postgres-session-authority.mjs";

const arrayIncludesIntrinsic = Array.prototype.includes;
const arrayIsArray = Array.isArray;
const arrayJoinIntrinsic = Array.prototype.join;
const arrayPushIntrinsic = Array.prototype.push;
const arraySortIntrinsic = Array.prototype.sort;
const BigIntConstructor = BigInt;
const bigIntToStringIntrinsic = BigInt.prototype.toString;
const DateConstructor = Date;
const dateParseIntrinsic = Date.parse;
const dateToISOStringIntrinsic = Date.prototype.toISOString;
const functionToStringIntrinsic = Function.prototype.toString;
const createHashIntrinsic = createHashExport;
const ErrorConstructor = Error;
const isAsyncFunctionValue = utilTypes.isAsyncFunction;
const isGeneratorFunctionValue = utilTypes.isGeneratorFunction;
const isGeneratorObjectValue = utilTypes.isGeneratorObject;
const isPromiseValue = utilTypes.isPromise;
const isProxyValue = utilTypes.isProxy;
const numberIsFinite = Number.isFinite;
const numberIsSafeInteger = Number.isSafeInteger;
const objectCreate = Object.create;
const objectDefineProperty = Object.defineProperty;
const objectFreeze = Object.freeze;
const objectGetOwnPropertyDescriptor = Object.getOwnPropertyDescriptor;
const objectGetPrototypeOf = Object.getPrototypeOf;
const objectHasOwn = Object.hasOwn;
const objectIs = Object.is;
const objectIsFrozen = Object.isFrozen;
const objectPrototype = Object.prototype;
const objectSetPrototypeOf = Object.setPrototypeOf;
const hashPrototype = objectGetPrototypeOf(createHashExport("sha256"));
const hashDigestIntrinsic = hashPrototype.digest;
const hashUpdateIntrinsic = hashPrototype.update;
const jsonReceiver = JSON;
const jsonStringifyIntrinsic = jsonReceiver.stringify;
const pathBasename = pathBasenameExport;
const pathDirname = pathDirnameExport;
const pathIsAbsolute = pathIsAbsoluteExport;
const pathParse = pathParseExport;
const pathResolve = pathResolveExport;
const PromiseConstructor = Promise;
const reflectApply = Reflect.apply;
const reflectOwnKeys = Reflect.ownKeys;
const regexpExecIntrinsic = RegExp.prototype.exec;
const stringStartsWithIntrinsic = String.prototype.startsWith;
const TypeErrorConstructor = TypeError;
const WeakSetConstructor = WeakSet;
const weakSetAddIntrinsic = WeakSet.prototype.add;
const weakSetDeleteIntrinsic = WeakSet.prototype.delete;
const weakSetHasIntrinsic = WeakSet.prototype.has;

const NATIVE_FUNCTION_SOURCE_PATTERN =
  /\{\s*\[\s*native\s+code\s*\]\s*\}\s*$/u;
const NUL_PATTERN = /\0/u;
const OPAQUE_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u;
const PERSISTENT_OBJECT_ID_PATTERN =
  /^[A-Za-z0-9][A-Za-z0-9._:-]{0,255}$/u;
const SHA256_PATTERN = /^[0-9a-f]{64}$/u;
const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u;
const MAX_DATA_TREE_DEPTH = 16;
const MAX_DATA_TREE_NODES = 4_096;
const CHECKPOINT_CAPTURE_ATTEMPT_CONTRACT_VERSION = 1;
const CHECKPOINT_CAPTURE_BINDING_CONTRACT_VERSION = 2;
const CHECKPOINT_CAPTURE_OPERATION_CONTRACT_VERSION = 1;
const CHECKPOINT_CATALOGUE_CONTRACT_VERSION = 1;
const OPERATION_REQUEST_VERSION = 1;
const CHECKPOINT_CAPTURE_DIAGNOSTIC =
  process.env.PORTABLE_CODEX_RUNTIME_CHECKPOINT_DIAGNOSTIC === "1";

const OPTION_KEYS = objectFreeze([
  "authority",
  "operationGuard",
  "resolveArtifactPaths",
  "resolveSourceOwnedRoot",
]);
const AUTHORITY_METHODS = objectFreeze([
  "cancelPreparedOperation",
  "claimCheckpointCaptureDispatch",
  "finalizeCheckpointCapture",
  "markOperationUncertain",
  "readCheckpointCaptureAttempt",
  "readSession",
  "reconcileOperation",
  "reserveOperation",
]);
const CAPTURE_ADMISSION_KEYS = objectFreeze([
  "attachment",
  "captureAttemptId",
  "checkpoint",
  "processIncarnationId",
  "request",
  "stopOperationId",
  "writerIncarnationId",
]);
const RECONCILIATION_ADMISSION_KEYS = objectFreeze([
  "checkpoint",
  "request",
]);
const ARTIFACT_PLAN_KEYS = objectFreeze([
  "artifactDirectory",
  "artifactOwnedRoot",
]);
const SOURCE_PLAN_KEYS = objectFreeze([
  "sourceDirectory",
  "sourceOwnedRoot",
]);
const COMPLETION_KEYS = objectFreeze([
  "artifactProof",
  "materialization",
  "replayed",
  "result",
]);
const CAPTURE_ATTEMPT_KEYS = objectFreeze([
  "binding",
  "captureAttemptId",
  "contractVersion",
  "operationId",
  "request",
  "result",
  "state",
]);
const CAPTURE_BINDING_KEYS = objectFreeze([
  "attachmentId",
  "attachmentOperationId",
  "attachmentProofId",
  "captureAttemptId",
  "checkpoint",
  "contractVersion",
  "processIncarnationId",
  "reservationId",
  "stopOperationId",
  "writerIncarnationId",
]);
const TYPED_CAPTURE_REQUEST_KEYS = objectFreeze([
  "admission",
  "contractVersion",
  "predeterminedResult",
]);
const CAPTURE_RESULT_KEYS = objectFreeze(["checkpoint", "mutation"]);
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
const MATERIALIZATION_KEYS = objectFreeze([
  "artifactManifestDigest",
  "contractVersion",
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
const HISTORICAL_SESSION_IDENTITY_DOCUMENT_KEYS = objectFreeze([
  "backendCapabilities",
  "documentVersion",
  "manifest",
  "storageRef",
]);
const ACTIVE_OPERATION_KEYS = objectFreeze([
  "conflictClass",
  "expectedSessionRevision",
  "kind",
  "operationId",
  "operationRevision",
  "requestSha256",
  "reservationId",
  "state",
]);
const LAST_OPERATION_KEYS = objectFreeze([
  ...ACTIVE_OPERATION_KEYS,
  "resultSha256",
]);
const TERMINAL_RESULT_KEYS = objectFreeze([
  "captureAttemptId",
  "catalogueSha256",
  "checkpointId",
  "outcome",
  "resultVersion",
]);
const RESERVE_RECEIPT_KEYS = objectFreeze([
  "acquired",
  "operation",
  "reservation",
  "session",
  "status",
]);
const CLAIM_RECEIPT_KEYS = objectFreeze([
  "attempt",
  "authorityNow",
  "dispatchGranted",
  "operation",
  "reservation",
  "session",
  "status",
]);
const ATTEMPT_READ_RECEIPT_KEYS = objectFreeze([
  "attempt",
  "catalogue",
  "operation",
  "reservation",
  "session",
  "status",
]);
const FINALIZE_RECEIPT_KEYS = objectFreeze([
  "attempt",
  "catalogue",
  "finalized",
  "operation",
  "reservation",
  "session",
  "status",
]);
const RECONCILE_RECEIPT_KEYS = objectFreeze([
  "operation",
  "reservation",
  "session",
  "status",
]);
const PROBE_KEYS = objectFreeze(["assertHeld"]);

const ERROR_MESSAGES = objectCreate(null);
objectDefineProperty(
  ERROR_MESSAGES,
  "invalid_postgres_checkpoint_mutation_authority_options",
  {
    enumerable: true,
    value: "PostgreSQL checkpoint mutation authority options are invalid",
  },
);
objectDefineProperty(
  ERROR_MESSAGES,
  "invalid_postgres_checkpoint_mutation_authority_request",
  {
    enumerable: true,
    value: "PostgreSQL checkpoint mutation authority request is invalid",
  },
);
objectDefineProperty(
  ERROR_MESSAGES,
  "postgres_checkpoint_mutation_authority_outcome_uncertain",
  {
    enumerable: true,
    value: "PostgreSQL checkpoint mutation authority outcome is uncertain",
  },
);
objectDefineProperty(
  ERROR_MESSAGES,
  "postgres_checkpoint_restore_unavailable",
  {
    enumerable: true,
    value: "PostgreSQL checkpoint restore is unavailable",
  },
);
objectFreeze(ERROR_MESSAGES);

const CANCEL_REASON = "capture-dispatch-not-started";

function callIntrinsic(intrinsic, receiver, args) {
  return reflectApply(intrinsic, receiver, args);
}

function arrayIncludes(value, candidate) {
  return callIntrinsic(arrayIncludesIntrinsic, value, [candidate]);
}

function regexpTest(pattern, value) {
  return callIntrinsic(regexpExecIntrinsic, pattern, [value]) !== null;
}

function stringStartsWith(value, prefix) {
  return callIntrinsic(stringStartsWithIntrinsic, value, [prefix]);
}

function makeError(code) {
  return new PostgresCheckpointMutationAuthorityError(code);
}

function fail(code) {
  throw makeError(code);
}

function ensure(condition, code) {
  if (!condition) fail(code);
}

function exactDataObject(value, expectedKeys, code) {
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
    arrayIncludes([objectPrototype, null], prototype) &&
      keys.length === expectedKeys.length,
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
      descriptor?.enumerable === true &&
        objectHasOwn(descriptor, "value"),
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

function consumeDataTreeNode(state, code) {
  state.nodes += 1;
  ensure(
    state.depth <= MAX_DATA_TREE_DEPTH &&
      state.nodes <= MAX_DATA_TREE_NODES,
    code,
  );
}

function weakSetHas(set, value) {
  return callIntrinsic(weakSetHasIntrinsic, set, [value]);
}

function weakSetAdd(set, value) {
  callIntrinsic(weakSetAddIntrinsic, set, [value]);
}

function weakSetDelete(set, value) {
  callIntrinsic(weakSetDeleteIntrinsic, set, [value]);
}

function deepFrozenDataSnapshot(value, state, code) {
  consumeDataTreeNode(state, code);
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
      !arrayIsArray(value) &&
      !isProxyValue(value) &&
      !weakSetHas(state.seen, value),
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
  ensure(arrayIncludes([objectPrototype, null], prototype), code);
  weakSetAdd(state.seen, value);
  const snapshot = objectCreate(null);
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
    ensure(typeof key === "string", code);
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
    const childState = {
      depth: state.depth + 1,
      nodes: state.nodes,
      seen: state.seen,
    };
    objectDefineProperty(snapshot, key, {
      enumerable: true,
      value: deepFrozenDataSnapshot(descriptor.value, childState, code),
    });
    state.nodes = childState.nodes;
  }
  weakSetDelete(state.seen, value);
  return objectFreeze(snapshot);
}

function assertDeepFrozenDataTree(value, state, code) {
  consumeDataTreeNode(state, code);
  if (
    value === null ||
    typeof value === "string" ||
    typeof value === "boolean"
  ) {
    return;
  }
  if (typeof value === "number") {
    ensure(numberIsFinite(value), code);
    return;
  }
  ensure(
    typeof value === "object" &&
      !arrayIsArray(value) &&
      !isProxyValue(value) &&
      objectIsFrozen(value) &&
      !weakSetHas(state.seen, value),
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
  ensure(arrayIncludes([objectPrototype, null], prototype), code);
  weakSetAdd(state.seen, value);
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
    ensure(typeof key === "string", code);
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
    const childState = {
      depth: state.depth + 1,
      nodes: state.nodes,
      seen: state.seen,
    };
    assertDeepFrozenDataTree(descriptor.value, childState, code);
    state.nodes = childState.nodes;
  }
  weakSetDelete(state.seen, value);
}

function sameDataTree(left, right, state, code) {
  consumeDataTreeNode(state, code);
  if (objectIs(left, right)) return true;
  if (
    left === null ||
    right === null ||
    typeof left !== "object" ||
    typeof right !== "object"
  ) {
    return objectIs(left, right);
  }
  ensure(
    !arrayIsArray(left) &&
      !arrayIsArray(right) &&
      !isProxyValue(left) &&
      !isProxyValue(right) &&
      !weakSetHas(state.leftSeen, left) &&
      !weakSetHas(state.rightSeen, right),
    code,
  );
  let leftPrototype;
  let rightPrototype;
  let leftKeys;
  let rightKeys;
  try {
    leftPrototype = objectGetPrototypeOf(left);
    rightPrototype = objectGetPrototypeOf(right);
    leftKeys = reflectOwnKeys(left);
    rightKeys = reflectOwnKeys(right);
  } catch {
    fail(code);
  }
  ensure(
    arrayIncludes([objectPrototype, null], leftPrototype) &&
      arrayIncludes([objectPrototype, null], rightPrototype),
    code,
  );
  if (leftKeys.length !== rightKeys.length) return false;
  weakSetAdd(state.leftSeen, left);
  weakSetAdd(state.rightSeen, right);
  for (let index = 0; index < leftKeys.length; index += 1) {
    const key = leftKeys[index];
    if (typeof key !== "string" || !objectHasOwn(right, key)) return false;
    let leftDescriptor;
    let rightDescriptor;
    try {
      leftDescriptor = objectGetOwnPropertyDescriptor(left, key);
      rightDescriptor = objectGetOwnPropertyDescriptor(right, key);
    } catch {
      fail(code);
    }
    ensure(
      leftDescriptor?.enumerable === true &&
        objectHasOwn(leftDescriptor, "value") &&
        rightDescriptor?.enumerable === true &&
        objectHasOwn(rightDescriptor, "value"),
      code,
    );
    const childState = {
      depth: state.depth + 1,
      leftSeen: state.leftSeen,
      nodes: state.nodes,
      rightSeen: state.rightSeen,
    };
    if (
      !sameDataTree(
        leftDescriptor.value,
        rightDescriptor.value,
        childState,
        code,
      )
    ) {
      return false;
    }
    state.nodes = childState.nodes;
  }
  weakSetDelete(state.leftSeen, left);
  weakSetDelete(state.rightSeen, right);
  return true;
}

function sameContent(left, right, code) {
  return sameDataTree(
    left,
    right,
    {
      depth: 0,
      leftSeen: new WeakSetConstructor(),
      nodes: 0,
      rightSeen: new WeakSetConstructor(),
    },
    code,
  );
}

function canonicalDataJson(value, state, code) {
  consumeDataTreeNode(state, code);
  if (
    value === null ||
    typeof value === "string" ||
    typeof value === "boolean"
  ) {
    const serialized = callIntrinsic(jsonStringifyIntrinsic, jsonReceiver, [
      value,
    ]);
    ensure(typeof serialized === "string", code);
    return serialized;
  }
  if (typeof value === "number") {
    ensure(numberIsFinite(value), code);
    return callIntrinsic(jsonStringifyIntrinsic, jsonReceiver, [value]);
  }
  ensure(
    typeof value === "object" &&
      !arrayIsArray(value) &&
      !isProxyValue(value) &&
      !weakSetHas(state.seen, value),
    code,
  );
  let prototype;
  let ownKeys;
  try {
    prototype = objectGetPrototypeOf(value);
    ownKeys = reflectOwnKeys(value);
  } catch {
    fail(code);
  }
  ensure(arrayIncludes([objectPrototype, null], prototype), code);
  const keys = [];
  callIntrinsic(objectSetPrototypeOf, undefined, [keys, null]);
  for (let index = 0; index < ownKeys.length; index += 1) {
    ensure(typeof ownKeys[index] === "string", code);
    callIntrinsic(arrayPushIntrinsic, keys, [ownKeys[index]]);
  }
  callIntrinsic(arraySortIntrinsic, keys, []);
  weakSetAdd(state.seen, value);
  const fields = [];
  callIntrinsic(objectSetPrototypeOf, undefined, [fields, null]);
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
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
    const childState = {
      depth: state.depth + 1,
      nodes: state.nodes,
      seen: state.seen,
    };
    const serializedKey = callIntrinsic(
      jsonStringifyIntrinsic,
      jsonReceiver,
      [key],
    );
    const serializedValue = canonicalDataJson(
      descriptor.value,
      childState,
      code,
    );
    state.nodes = childState.nodes;
    callIntrinsic(arrayPushIntrinsic, fields, [
      `${serializedKey}:${serializedValue}`,
    ]);
  }
  weakSetDelete(state.seen, value);
  return `{${callIntrinsic(arrayJoinIntrinsic, fields, [","])}}`;
}

function canonicalSha256(value, code) {
  const serialized = canonicalDataJson(
    value,
    {
      depth: 0,
      nodes: 0,
      seen: new WeakSetConstructor(),
    },
    code,
  );
  return stringSha256(serialized, code);
}

function stringSha256(value, code) {
  ensure(typeof value === "string", code);
  let hash;
  let digest;
  try {
    hash = callIntrinsic(createHashIntrinsic, undefined, ["sha256"]);
    callIntrinsic(hashUpdateIntrinsic, hash, [value, "utf8"]);
    digest = callIntrinsic(hashDigestIntrinsic, hash, ["hex"]);
  } catch {
    fail(code);
  }
  ensure(typeof digest === "string" && regexpTest(SHA256_PATTERN, digest), code);
  return digest;
}

function serializedDataSha256(value, code) {
  const snapshot = deepFrozenDataSnapshot(
    value,
    {
      depth: 0,
      nodes: 0,
      seen: new WeakSetConstructor(),
    },
    code,
  );
  let serialized;
  try {
    serialized = callIntrinsic(jsonStringifyIntrinsic, jsonReceiver, [
      snapshot,
    ]);
  } catch {
    fail(code);
  }
  ensure(typeof serialized === "string", code);
  return stringSha256(serialized, code);
}

function operationRequestSha256(expectedSession, typedRequest, code) {
  return serializedDataSha256(
    exactFrozenRecord({
      requestVersion: OPERATION_REQUEST_VERSION,
      conflictClass: SESSION_OPERATION_CONFLICT_CLASS,
      expectedSession,
      payload: typedRequest,
    }),
    code,
  );
}

function revisionNumber(value, code) {
  ensure(
    typeof value === "string" &&
      regexpTest(/^(?:0|[1-9][0-9]{0,18})$/u, value),
    code,
  );
  try {
    return BigIntConstructor(value);
  } catch {
    fail(code);
  }
}

function assertSourceBackedFunction(
  value,
  {
    asynchronous,
    code,
  },
) {
  ensure(
    typeof value === "function" &&
      !isProxyValue(value) &&
      !isGeneratorFunctionValue(value) &&
      isAsyncFunctionValue(value) === asynchronous,
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

function assertCallback(value, code) {
  ensure(
    typeof value === "function" &&
      !isProxyValue(value) &&
      !isGeneratorFunctionValue(value),
    code,
  );
  return value;
}

function lookupAsyncMethod(receiver, name, code) {
  ensure(
    receiver !== null &&
      arrayIncludes(["object", "function"], typeof receiver) &&
      !isProxyValue(receiver),
    code,
  );
  let current = receiver;
  while (current !== null) {
    ensure(!isProxyValue(current), code);
    let descriptor;
    try {
      descriptor = objectGetOwnPropertyDescriptor(current, name);
    } catch {
      fail(code);
    }
    if (descriptor !== undefined) {
      ensure(objectHasOwn(descriptor, "value"), code);
      return assertSourceBackedFunction(descriptor.value, {
        asynchronous: true,
        code,
      });
    }
    try {
      current = objectGetPrototypeOf(current);
    } catch {
      fail(code);
    }
  }
  fail(code);
}

function rejectThenableObject(value, code) {
  let current = value;
  while (current !== null) {
    ensure(!isProxyValue(current), code);
    let descriptor;
    try {
      descriptor = objectGetOwnPropertyDescriptor(current, "then");
    } catch {
      fail(code);
    }
    ensure(descriptor === undefined, code);
    try {
      current = objectGetPrototypeOf(current);
    } catch {
      fail(code);
    }
  }
}

function collaboratorBinding(value, methodNames, code) {
  rejectThenableObject(value, code);
  const methods = objectCreate(null);
  for (let index = 0; index < methodNames.length; index += 1) {
    const name = methodNames[index];
    methods[name] = lookupAsyncMethod(value, name, code);
  }
  return exactFrozenRecord({ methods: exactFrozenRecord(methods), receiver: value });
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
    let descriptor;
    try {
      descriptor = objectGetOwnPropertyDescriptor(current, "constructor");
    } catch {
      return false;
    }
    if (descriptor !== undefined) {
      return (
        objectHasOwn(descriptor, "value") &&
        descriptor.value === PromiseConstructor
      );
    }
    try {
      current = objectGetPrototypeOf(current);
    } catch {
      return false;
    }
  }
  return false;
}

async function invokeAsync(binding, name, args, code) {
  let pending;
  try {
    pending = callIntrinsic(
      binding.methods[name],
      binding.receiver,
      args,
    );
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

async function invokeCallback(callback, context, code) {
  let pending;
  try {
    pending = callIntrinsic(callback, undefined, [context]);
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

function canonicalTimestampMilliseconds(value, code) {
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

function pathsAreDisjoint(left, right) {
  return (
    left !== right &&
    !stringStartsWith(left, `${right}/`) &&
    !stringStartsWith(right, `${left}/`)
  );
}

function runPlanner(planner, input, keys, code) {
  let value;
  try {
    value = callIntrinsic(planner, undefined, [input]);
  } catch {
    fail(code);
  }
  ensure(
    !isPromiseValue(value) &&
      !isGeneratorObjectValue(value) &&
      !isProxyValue(value),
    code,
  );
  return exactDataObject(value, keys, code);
}

function planArtifact(planner, admission, code) {
  const plan = runPlanner(
    planner,
    exactFrozenRecord({
      checkpoint: admission.checkpoint,
      request: admission.request,
    }),
    ARTIFACT_PLAN_KEYS,
    code,
  );
  return directPathPlan(
    plan.artifactDirectory,
    plan.artifactOwnedRoot,
    code,
  );
}

function planSource(planner, admission, canonicalAttachment, code) {
  const plan = runPlanner(
    planner,
    exactFrozenRecord({
      canonicalAttachment,
      checkpoint: admission.checkpoint,
      request: admission.request,
    }),
    SOURCE_PLAN_KEYS,
    code,
  );
  const source = directPathPlan(
    plan.sourceDirectory,
    plan.sourceOwnedRoot,
    code,
  );
  ensure(source.directory === canonicalAttachment.rootPath, code);
  return source;
}

function normalizeAdmission(value, keys, code) {
  const admission = exactDataObject(value, keys, code);
  const request = exactDataObject(
    admission.request,
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
    code,
  );
  ensure(
    typeof request.operationId === "string" &&
      regexpTest(OPAQUE_ID_PATTERN, request.operationId),
    code,
  );
  const checkpoint = exactDataObject(
    admission.checkpoint,
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
    code,
  );
  ensure(
    typeof checkpoint.sessionId === "string" &&
      regexpTest(UUID_PATTERN, checkpoint.sessionId),
    code,
  );
  return deepFrozenDataSnapshot(
    admission,
    {
      depth: 0,
      nodes: 0,
      seen: new WeakSetConstructor(),
    },
    code,
  );
}

function sessionDocument(session, code) {
  const snapshot = exactDataObject(session, SESSION_KEYS, code);
  return exactDataObject(
    snapshot.document,
    SESSION_DOCUMENT_KEYS,
    code,
  );
}

function sessionAttachment(session, code) {
  const document = sessionDocument(session, code);
  ensure(document.attachment !== null, code);
  return document.attachment;
}

function operationInput(expectedSession, operationId, request) {
  return exactFrozenRecord({
    expectedSession,
    kind: CHECKPOINT_CAPTURE_OPERATION_KIND,
    operationId,
    request,
  });
}

function transitionInput(expectedSession, operationId, request, revision) {
  return exactFrozenRecord({
    expectedOperationRevision: revision,
    expectedSession,
    kind: CHECKPOINT_CAPTURE_OPERATION_KIND,
    operationId,
    request,
  });
}

function normalizeCompletion(value, replayed, code) {
  const completion = exactDataObject(value, COMPLETION_KEYS, code);
  ensure(
    completion.replayed === replayed,
    code,
  );
  assertDeepFrozenDataTree(
    value,
    {
      depth: 0,
      nodes: 0,
      seen: new WeakSetConstructor(),
    },
    code,
  );
  return value;
}

function normalizeProbe(value, code) {
  const probe = exactDataObject(value, PROBE_KEYS, code);
  ensure(objectIsFrozen(value), code);
  return exactFrozenRecord({
    assertHeld: assertCallback(probe.assertHeld, code),
  });
}

async function assertGuardHeld(probe, code) {
  let pending;
  try {
    pending = callIntrinsic(probe.assertHeld, undefined, []);
  } catch {
    fail(code);
  }
  ensure(isSafeNativePromise(pending), code);
  try {
    await pending;
  } catch {
    fail(code);
  }
}

function normalizeOperation(operation, code) {
  return exactDataObject(operation, OPERATION_KEYS, code);
}

function canonicalTypedCaptureRequest(
  value,
  expectedSession,
  callerAdmission,
  code,
) {
  const request = exactDataObject(value, TYPED_CAPTURE_REQUEST_KEYS, code);
  ensure(
    request.contractVersion ===
      CHECKPOINT_CAPTURE_OPERATION_CONTRACT_VERSION,
    code,
  );
  const admission = normalizeAdmission(
    request.admission,
    CAPTURE_ADMISSION_KEYS,
    code,
  );
  let rebuilt;
  try {
    rebuilt = createCheckpointCaptureOperationRequest({
      admission,
      expectedSession,
    });
  } catch {
    fail(code);
  }
  ensure(sameContent(value, rebuilt, code), code);
  if (callerAdmission !== null) {
    ensure(
      sameContent(
        rebuilt.admission.checkpoint,
        callerAdmission.checkpoint,
        code,
      ) &&
        sameContent(
          rebuilt.admission.request,
          callerAdmission.request,
          code,
        ),
      code,
    );
  }
  return rebuilt;
}

function normalizeCatalogueDocument(
  value,
  {
    completion,
    operationId,
    typedRequest,
  },
  code,
) {
  const document = exactDataObject(value, CATALOGUE_DOCUMENT_KEYS, code);
  ensure(
    document.contractVersion === CHECKPOINT_CATALOGUE_CONTRACT_VERSION,
    code,
  );
  const artifactProof = exactDataObject(
    document.artifactProof,
    ARTIFACT_PROOF_KEYS,
    code,
  );
  ensure(
    typeof artifactProof.artifactManifestDigest === "string" &&
      regexpTest(SHA256_PATTERN, artifactProof.artifactManifestDigest) &&
      artifactProof.captureOperationId === operationId &&
      typeof artifactProof.modeledDigest === "string" &&
      regexpTest(SHA256_PATTERN, artifactProof.modeledDigest),
    code,
  );
  const materialization = exactDataObject(
    document.materialization,
    MATERIALIZATION_KEYS,
    code,
  );
  const stagedRoot = exactDataObject(
    materialization.stagedRoot,
    STAGED_ROOT_KEYS,
    code,
  );
  ensure(
    materialization.contractVersion === 2 &&
      materialization.artifactManifestDigest ===
        artifactProof.artifactManifestDigest &&
      materialization.modeledDigest === artifactProof.modeledDigest &&
      typeof materialization.publicationId === "string" &&
      regexpTest(OPAQUE_ID_PATTERN, materialization.publicationId) &&
      materialization.publicationKind === "checkpoint-artifact" &&
      typeof materialization.treeIdentityDigest === "string" &&
      regexpTest(SHA256_PATTERN, materialization.treeIdentityDigest) &&
      typeof stagedRoot.filesystemId === "string" &&
      regexpTest(OPAQUE_ID_PATTERN, stagedRoot.filesystemId) &&
      typeof stagedRoot.objectIdentityScheme === "string" &&
      regexpTest(OPAQUE_ID_PATTERN, stagedRoot.objectIdentityScheme) &&
      typeof stagedRoot.objectId === "string" &&
      regexpTest(PERSISTENT_OBJECT_ID_PATTERN, stagedRoot.objectId),
    code,
  );
  exactDataObject(document.result, CAPTURE_RESULT_KEYS, code);
  ensure(
    sameContent(document.result, typedRequest.predeterminedResult, code),
    code,
  );
  if (completion !== null) {
    ensure(
      sameContent(document.artifactProof, completion.artifactProof, code) &&
        sameContent(
          document.materialization,
          completion.materialization,
          code,
        ) &&
        sameContent(document.result, completion.result, code),
      code,
    );
  }
  return exactFrozenRecord({
    artifactProof: exactFrozenRecord({
      artifactManifestDigest: artifactProof.artifactManifestDigest,
      captureOperationId: artifactProof.captureOperationId,
      modeledDigest: artifactProof.modeledDigest,
    }),
    contractVersion: CHECKPOINT_CATALOGUE_CONTRACT_VERSION,
    materialization: exactFrozenRecord({
      artifactManifestDigest: materialization.artifactManifestDigest,
      contractVersion: materialization.contractVersion,
      modeledDigest: materialization.modeledDigest,
      publicationId: materialization.publicationId,
      publicationKind: materialization.publicationKind,
      stagedRoot: exactFrozenRecord({
        filesystemId: stagedRoot.filesystemId,
        objectIdentityScheme: stagedRoot.objectIdentityScheme,
        objectId: stagedRoot.objectId,
      }),
      treeIdentityDigest: materialization.treeIdentityDigest,
    }),
    result: typedRequest.predeterminedResult,
  });
}

function normalizeReservation(
  value,
  {
    expectedReservation,
    expectedSession,
    operation,
    state,
  },
  code,
) {
  const reservation = exactDataObject(value, RESERVATION_KEYS, code);
  ensure(
    reservation.conflictClass === SESSION_OPERATION_CONFLICT_CLASS &&
      reservation.reservationId ===
        `reservation-${stringSha256(operation.operationId, code)}` &&
      reservation.operationId === operation.operationId &&
      reservation.sessionId === operation.sessionId &&
      reservation.kind === operation.kind &&
      reservation.expectedSessionRevision === expectedSession.revision &&
      reservation.requestSha256 === operation.requestSha256 &&
      reservation.createdAt === operation.createdAt &&
      reservation.updatedAt === operation.updatedAt &&
      reservation.expiresAt === null &&
      reservation.state === state,
    code,
  );
  canonicalTimestampMilliseconds(reservation.createdAt, code);
  canonicalTimestampMilliseconds(reservation.updatedAt, code);
  if (state === "released") {
    ensure(
      reservation.releasedAt === reservation.updatedAt &&
        operation.retiredAt === reservation.releasedAt,
      code,
    );
  } else {
    ensure(reservation.releasedAt === null, code);
  }
  if (expectedReservation !== null) {
    const prior = exactDataObject(
      expectedReservation,
      RESERVATION_KEYS,
      code,
    );
    ensure(
      reservation.reservationId === prior.reservationId &&
        reservation.operationId === prior.operationId &&
        reservation.sessionId === prior.sessionId &&
        reservation.kind === prior.kind &&
        reservation.expectedSessionRevision ===
          prior.expectedSessionRevision &&
        reservation.requestSha256 === prior.requestSha256 &&
        reservation.createdAt === prior.createdAt,
      code,
    );
  }
  return reservation;
}

function normalizeCaptureAttempt(
  value,
  {
    expectedAttempt,
    operation,
    reservation,
    state,
    typedRequest,
  },
  code,
) {
  const attempt = exactDataObject(value, CAPTURE_ATTEMPT_KEYS, code);
  ensure(
    attempt.contractVersion ===
      CHECKPOINT_CAPTURE_ATTEMPT_CONTRACT_VERSION &&
      typeof attempt.captureAttemptId === "string" &&
      regexpTest(UUID_PATTERN, attempt.captureAttemptId) &&
      attempt.captureAttemptId ===
        typedRequest.admission.captureAttemptId &&
      attempt.operationId === operation.operationId &&
      attempt.state === state &&
      sameContent(
        attempt.request,
        typedRequest.admission.request,
        code,
      ) &&
      sameContent(
        attempt.result,
        typedRequest.predeterminedResult,
        code,
      ),
    code,
  );
  const binding = exactDataObject(
    attempt.binding,
    CAPTURE_BINDING_KEYS,
    code,
  );
  const admission = typedRequest.admission;
  ensure(
    binding.contractVersion ===
      CHECKPOINT_CAPTURE_BINDING_CONTRACT_VERSION &&
      binding.captureAttemptId === attempt.captureAttemptId &&
      binding.reservationId === reservation.reservationId &&
      binding.attachmentId === admission.attachment.attachmentId &&
      binding.attachmentOperationId ===
        admission.attachment.operationId &&
      binding.attachmentProofId === admission.attachment.proofId &&
      binding.processIncarnationId === admission.processIncarnationId &&
      binding.stopOperationId === admission.stopOperationId &&
      binding.writerIncarnationId === admission.writerIncarnationId &&
      sameContent(binding.checkpoint, admission.checkpoint, code),
    code,
  );
  if (expectedAttempt !== null) {
    const prior = exactDataObject(
      expectedAttempt,
      CAPTURE_ATTEMPT_KEYS,
      code,
    );
    const expectedCommitted = exactFrozenRecord({
      binding: prior.binding,
      captureAttemptId: prior.captureAttemptId,
      contractVersion: prior.contractVersion,
      operationId: prior.operationId,
      request: prior.request,
      result: prior.result,
      state,
    });
    ensure(sameContent(value, expectedCommitted, code), code);
  }
  return attempt;
}

function normalizeTerminalResult(
  value,
  {
    attempt,
    catalogue,
    typedRequest,
  },
  code,
) {
  const result = exactDataObject(value, TERMINAL_RESULT_KEYS, code);
  ensure(
    result.resultVersion === 1 &&
      result.outcome === "checkpoint-captured" &&
      result.captureAttemptId === attempt.captureAttemptId &&
      result.checkpointId ===
        typedRequest.admission.checkpoint.checkpointId &&
      typeof result.catalogueSha256 === "string" &&
      regexpTest(SHA256_PATTERN, result.catalogueSha256) &&
      result.catalogueSha256 ===
        serializedDataSha256(catalogue.document, code),
    code,
  );
  return result;
}

function normalizeCatalogue(
  value,
  {
    attempt,
    completion,
    operation,
    typedRequest,
  },
  code,
) {
  const catalogue = exactDataObject(value, CATALOGUE_KEYS, code);
  ensure(
    catalogue.captureAttemptId === attempt.captureAttemptId &&
      catalogue.checkpointId ===
        typedRequest.admission.checkpoint.checkpointId &&
      catalogue.sessionId === operation.sessionId &&
      catalogue.committedAt === operation.updatedAt,
    code,
  );
  canonicalTimestampMilliseconds(catalogue.committedAt, code);
  const document = normalizeCatalogueDocument(
    catalogue.document,
    {
      completion,
      operationId: operation.operationId,
      typedRequest,
    },
    code,
  );
  return exactFrozenRecord({
    captureAttemptId: catalogue.captureAttemptId,
    checkpointId: catalogue.checkpointId,
    committedAt: catalogue.committedAt,
    document,
    sessionId: catalogue.sessionId,
  });
}

function validateOperationCommon(
  operation,
  {
    expectedSession,
    operationId,
    typedRequest,
  },
  code,
) {
  ensure(
    operation.conflictClass === SESSION_OPERATION_CONFLICT_CLASS &&
      operation.operationId === operationId &&
      operation.kind === CHECKPOINT_CAPTURE_OPERATION_KIND &&
      operation.sessionId === expectedSession.sessionId &&
      operation.requestSha256 ===
        operationRequestSha256(
          operation.expectedSession,
          operation.request,
          code,
        ) &&
      sameContent(operation.expectedSession, expectedSession, code) &&
      sameContent(operation.request, typedRequest, code),
    code,
  );
  canonicalTimestampMilliseconds(operation.createdAt, code);
  canonicalTimestampMilliseconds(operation.updatedAt, code);
}

function validateSessionTuple(
  value,
  {
    expectedSession,
    operation,
    reservation,
    terminal,
  },
  code,
) {
  const session = exactDataObject(value, SESSION_KEYS, code);
  const expected = exactDataObject(expectedSession, SESSION_KEYS, code);
  const document = sessionDocument(value, code);
  const expectedDocument = sessionDocument(expectedSession, code);
  ensure(
    session.sessionId === operation.sessionId &&
      session.sessionId === expected.sessionId &&
      session.createdAt === expected.createdAt &&
      session.updatedAt === operation.updatedAt &&
      revisionNumber(session.revision, code) ===
        revisionNumber(expected.revision, code) +
          revisionNumber(operation.revision, code) +
          1n,
    code,
  );
  const stableDocumentKeys = [
    "attachment",
    "backendCapabilities",
    "documentVersion",
    "launch",
    "lease",
    "lifecycle",
    "manifest",
    "recovery",
    "storageRef",
    "writerEpoch",
  ];
  for (let index = 0; index < stableDocumentKeys.length; index += 1) {
    const key = stableDocumentKeys[index];
    ensure(
      sameContent(document[key], expectedDocument[key], code),
      code,
    );
  }
  if (terminal) {
    const expectedLast = exactFrozenRecord({
      conflictClass: operation.conflictClass,
      expectedSessionRevision: expected.revision,
      kind: operation.kind,
      operationId: operation.operationId,
      operationRevision: operation.revision,
      requestSha256: operation.requestSha256,
      reservationId: reservation.reservationId,
      resultSha256: canonicalSha256(operation.result, code),
      state: "committed",
    });
    ensure(
      document.activeOperation === null &&
        sameContent(document.lastOperation, expectedLast, code),
      code,
    );
  } else {
    const expectedActive = exactFrozenRecord({
      conflictClass: operation.conflictClass,
      expectedSessionRevision: expected.revision,
      kind: operation.kind,
      operationId: operation.operationId,
      operationRevision: operation.revision,
      requestSha256: operation.requestSha256,
      reservationId: reservation.reservationId,
      state: operation.state,
    });
    ensure(
      sameContent(document.activeOperation, expectedActive, code) &&
        sameContent(
          document.lastOperation,
          expectedDocument.lastOperation,
          code,
        ),
      code,
    );
  }
  return session;
}

function validateHistoricalSessionIdentity(
  value,
  {
    expectedSession,
    operation,
  },
  code,
) {
  const session = exactDataObject(value, SESSION_KEYS, code);
  const expected = exactDataObject(expectedSession, SESSION_KEYS, code);
  const document = sessionDocument(value, code);
  const expectedDocument = sessionDocument(expectedSession, code);
  const createdAt = canonicalTimestampMilliseconds(
    session.createdAt,
    code,
  );
  const updatedAt = canonicalTimestampMilliseconds(
    session.updatedAt,
    code,
  );
  revisionNumber(session.revision, code);
  ensure(
    session.sessionId === operation.sessionId &&
      session.sessionId === expected.sessionId &&
      session.createdAt === expected.createdAt &&
      updatedAt >= createdAt,
    code,
  );
  for (
    let index = 0;
    index < HISTORICAL_SESSION_IDENTITY_DOCUMENT_KEYS.length;
    index += 1
  ) {
    const key = HISTORICAL_SESSION_IDENTITY_DOCUMENT_KEYS[index];
    ensure(
      sameContent(document[key], expectedDocument[key], code),
      code,
    );
  }
  if (document.activeOperation !== null) {
    const active = exactDataObject(
      document.activeOperation,
      ACTIVE_OPERATION_KEYS,
      code,
    );
    revisionNumber(active.expectedSessionRevision, code);
    revisionNumber(active.operationRevision, code);
  }
  if (document.lastOperation !== null) {
    const last = exactDataObject(
      document.lastOperation,
      LAST_OPERATION_KEYS,
      code,
    );
    revisionNumber(last.expectedSessionRevision, code);
    revisionNumber(last.operationRevision, code);
  }
  return session;
}

function normalizeReconciliationReadReceipt(value, admission, code) {
  const receipt = exactDataObject(
    value,
    ATTEMPT_READ_RECEIPT_KEYS,
    code,
  );
  ensure(
    receipt.status === "authorized" || receipt.status === "committed",
    code,
  );
  const operation = normalizeOperation(receipt.operation, code);
  const typedRequest = canonicalTypedCaptureRequest(
    operation.request,
    operation.expectedSession,
    admission,
    code,
  );
  validateOperationCommon(
    operation,
    {
      expectedSession: operation.expectedSession,
      operationId: admission.request.operationId,
      typedRequest,
    },
    code,
  );
  const expectedOperationRevision =
    receipt.status === "authorized"
      ? operation.revision
      : revisionNumber(operation.revision, code) - 1n;
  ensure(
    (receipt.status === "authorized" &&
      ((operation.state === "starting" && operation.revision === "1") ||
        (operation.state === "uncertain" &&
          operation.revision === "2")) &&
      operation.result === null &&
      operation.retiredAt === null) ||
      (receipt.status === "committed" &&
        operation.state === "committed" &&
        (operation.revision === "2" || operation.revision === "3") &&
        operation.result !== null &&
        operation.retiredAt === operation.updatedAt),
    code,
  );
  const reservation = normalizeReservation(
    receipt.reservation,
    {
      expectedReservation: null,
      expectedSession: operation.expectedSession,
      operation,
      state:
        receipt.status === "committed" ? "released" : operation.state,
    },
    code,
  );
  const attempt = normalizeCaptureAttempt(
    receipt.attempt,
    {
      expectedAttempt: null,
      operation,
      reservation,
      state: receipt.status,
      typedRequest,
    },
    code,
  );
  let catalogue = null;
  if (receipt.status === "authorized") {
    ensure(receipt.catalogue === null, code);
  } else {
    ensure(receipt.catalogue !== null, code);
    catalogue = normalizeCatalogue(
      receipt.catalogue,
      {
        attempt,
        completion: null,
        operation,
        typedRequest,
      },
      code,
    );
    normalizeTerminalResult(
      operation.result,
      { attempt, catalogue, typedRequest },
      code,
    );
  }
  if (receipt.status === "committed") {
    validateHistoricalSessionIdentity(
      receipt.session,
      {
        expectedSession: operation.expectedSession,
        operation,
      },
      code,
    );
  } else {
    validateSessionTuple(
      receipt.session,
      {
        expectedSession: operation.expectedSession,
        operation,
        reservation,
        terminal: false,
      },
      code,
    );
  }
  return exactFrozenRecord({
    attempt: receipt.attempt,
    catalogue,
    expectedOperationRevision:
      typeof expectedOperationRevision === "bigint"
        ? reflectApply(
            bigIntToStringIntrinsic,
            expectedOperationRevision,
            [],
          )
        : expectedOperationRevision,
    operation,
    reservation: receipt.reservation,
    session: receipt.session,
    typedRequest,
  });
}

function normalizeFinalizationReceipt(
  value,
  {
    completion,
    expectedAttempt,
    expectedOperationRevision,
    expectedReservation,
    expectedSession,
    operationId,
    typedRequest: typedRequestValue,
  },
  code,
) {
  const receipt = exactDataObject(value, FINALIZE_RECEIPT_KEYS, code);
  ensure(
    receipt.status === "committed" &&
      typeof receipt.finalized === "boolean",
    code,
  );
  const operation = normalizeOperation(receipt.operation, code);
  const typedRequest = canonicalTypedCaptureRequest(
    typedRequestValue,
    expectedSession,
    null,
    code,
  );
  validateOperationCommon(
    operation,
    { expectedSession, operationId, typedRequest },
    code,
  );
  ensure(
    operation.state === "committed" &&
      revisionNumber(operation.revision, code) ===
        revisionNumber(expectedOperationRevision, code) + 1n &&
      operation.result !== null &&
      operation.retiredAt === operation.updatedAt,
    code,
  );
  const reservation = normalizeReservation(
    receipt.reservation,
    {
      expectedReservation,
      expectedSession,
      operation,
      state: "released",
    },
    code,
  );
  const expectedAttemptRecord = exactDataObject(
    expectedAttempt,
    CAPTURE_ATTEMPT_KEYS,
    code,
  );
  ensure(
    receipt.finalized === (expectedAttemptRecord.state === "authorized"),
    code,
  );
  const attempt = normalizeCaptureAttempt(
    receipt.attempt,
    {
      expectedAttempt,
      operation,
      reservation,
      state: "committed",
      typedRequest,
    },
    code,
  );
  const catalogue = normalizeCatalogue(
    receipt.catalogue,
    {
      attempt,
      completion,
      operation,
      typedRequest,
    },
    code,
  );
  normalizeTerminalResult(
    operation.result,
    { attempt, catalogue, typedRequest },
    code,
  );
  if (expectedAttemptRecord.state === "committed") {
    validateHistoricalSessionIdentity(
      receipt.session,
      {
        expectedSession,
        operation,
      },
      code,
    );
  } else {
    validateSessionTuple(
      receipt.session,
      {
        expectedSession,
        operation,
        reservation,
        terminal: true,
      },
      code,
    );
  }
  return receipt;
}

async function bestEffortCancelPrepared(
  authority,
  baseInput,
  code,
) {
  let receipt;
  try {
    receipt = await invokeAsync(
      authority,
      "reconcileOperation",
      [baseInput],
      code,
    );
  } catch {
    return;
  }
  let normalized;
  try {
    normalized = exactDataObject(
      receipt,
      RECONCILE_RECEIPT_KEYS,
      code,
    );
  } catch {
    return;
  }
  if (
    normalized.status !== "prepared" ||
    normalized.operation === null
  ) {
    return;
  }
  let operation;
  try {
    operation = normalizeOperation(normalized.operation, code);
  } catch {
    return;
  }
  if (
    operation.state !== "prepared" ||
    operation.revision !== "0" ||
    operation.operationId !== baseInput.operationId
  ) {
    return;
  }
  try {
    await invokeAsync(
      authority,
      "cancelPreparedOperation",
      [
        exactFrozenRecord({
          ...baseInput,
          expectedOperationRevision: "0",
          reason: CANCEL_REASON,
        }),
      ],
      code,
    );
  } catch {
    // Recovery is best effort. The durable reservation remains authoritative.
  }
}

async function bestEffortMarkUncertain(authority, input, code) {
  try {
    await invokeAsync(
      authority,
      "markOperationUncertain",
      [
        exactFrozenRecord({
          ...input,
          expectedOperationRevision: "1",
        }),
      ],
      code,
    );
  } catch {
    // The caller still receives one fixed uncertain outcome.
  }
}

export class PostgresCheckpointMutationAuthorityError extends Error {
  constructor(code) {
    if (typeof code !== "string" || !objectHasOwn(ERROR_MESSAGES, code)) {
      throw new TypeErrorConstructor(
        "Unsupported PostgreSQL checkpoint authority error",
      );
    }
    const message = ERROR_MESSAGES[code];
    super(message);
    objectDefineProperty(this, "name", {
      enumerable: false,
      value: "PostgresCheckpointMutationAuthorityError",
    });
    objectDefineProperty(this, "code", {
      enumerable: true,
      value: code,
    });
    objectDefineProperty(this, "retryable", {
      enumerable: true,
      value: false,
    });
    objectDefineProperty(this, "stack", {
      enumerable: false,
      value: `PostgresCheckpointMutationAuthorityError: ${message}`,
    });
    objectFreeze(this);
  }
}

export function createPostgresCheckpointMutationAuthority(...args) {
  const optionCode =
    "invalid_postgres_checkpoint_mutation_authority_options";
  ensure(args.length === 1, optionCode);
  const options = exactDataObject(args[0], OPTION_KEYS, optionCode);
  const authority = collaboratorBinding(
    options.authority,
    AUTHORITY_METHODS,
    optionCode,
  );
  const operationGuard = collaboratorBinding(
    options.operationGuard,
    ["runExclusive"],
    optionCode,
  );
  const resolveArtifactPaths = assertSourceBackedFunction(
    options.resolveArtifactPaths,
    { asynchronous: false, code: optionCode },
  );
  const resolveSourceOwnedRoot = assertSourceBackedFunction(
    options.resolveSourceOwnedRoot,
    { asynchronous: false, code: optionCode },
  );

  const requestCode =
    "invalid_postgres_checkpoint_mutation_authority_request";
  const outcomeCode =
    "postgres_checkpoint_mutation_authority_outcome_uncertain";

  const runCapture = async function runCapture(admissionValue, publishValue) {
    const admission = normalizeAdmission(
      admissionValue,
      CAPTURE_ADMISSION_KEYS,
      requestCode,
    );
    const publish = assertCallback(publishValue, requestCode);
    const operationId = admission.request.operationId;
    let diagnosticStage = "guard";
    let dispatchDefinitelyBegan = false;
    let uncertaintyInput = null;
    let uncertaintyAttempted = false;
    let preparedRecoveryAttempted = false;

    const markUncertain = async () => {
      if (
        !dispatchDefinitelyBegan ||
        uncertaintyInput === null ||
        uncertaintyAttempted
      ) {
        return;
      }
      uncertaintyAttempted = true;
      await bestEffortMarkUncertain(authority, uncertaintyInput, outcomeCode);
    };
    const recoverPrepared = async (baseInput) => {
      if (preparedRecoveryAttempted) return;
      preparedRecoveryAttempted = true;
      await bestEffortCancelPrepared(authority, baseInput, outcomeCode);
    };

    try {
      return await invokeAsync(
        operationGuard,
        "runExclusive",
        [
          operationId,
          async (probeValue) => {
            const probe = normalizeProbe(probeValue, outcomeCode);
            let expectedSession;
            let typedRequest;
            let baseInput;
            let preparedRecoveryEligible = false;
            try {
              diagnosticStage = "read-session";
              expectedSession = await invokeAsync(
                authority,
                "readSession",
                [
                  exactFrozenRecord({
                    sessionId: admission.checkpoint.sessionId,
                  }),
                ],
                outcomeCode,
              );
              typedRequest = createCheckpointCaptureOperationRequest({
                admission,
                expectedSession,
              });
              const canonicalAdmission = typedRequest.admission;
              const canonicalAttachment = sessionAttachment(
                expectedSession,
                outcomeCode,
              );
              const artifact = planArtifact(
                resolveArtifactPaths,
                canonicalAdmission,
                outcomeCode,
              );
              const source = planSource(
                resolveSourceOwnedRoot,
                canonicalAdmission,
                canonicalAttachment,
                outcomeCode,
              );
              ensure(
                pathsAreDisjoint(source.ownedRoot, artifact.ownedRoot),
                outcomeCode,
              );
              baseInput = operationInput(
                expectedSession,
                operationId,
                typedRequest,
              );

              let reserveReceipt;
              preparedRecoveryEligible = true;
              diagnosticStage = "reserve";
              try {
                reserveReceipt = await invokeAsync(
                  authority,
                  "reserveOperation",
                  [baseInput],
                  outcomeCode,
                );
              } catch {
                await recoverPrepared(baseInput);
                fail(outcomeCode);
              }
              diagnosticStage = "reserve-receipt";
              const reserve = exactDataObject(
                reserveReceipt,
                RESERVE_RECEIPT_KEYS,
                outcomeCode,
              );
              if (reserve.acquired === false) {
                preparedRecoveryEligible = false;
                fail(outcomeCode);
              }
              ensure(
                reserve.acquired === true &&
                  reserve.status === "prepared",
                outcomeCode,
              );
              const reserveOperation = normalizeOperation(
                reserve.operation,
                outcomeCode,
              );
              const canonicalReserveRequest =
                canonicalTypedCaptureRequest(
                  reserveOperation.request,
                  reserveOperation.expectedSession,
                  canonicalAdmission,
                  outcomeCode,
                );
              validateOperationCommon(
                reserveOperation,
                {
                  expectedSession,
                  operationId,
                  typedRequest,
                },
                outcomeCode,
              );
              ensure(
                reserveOperation.state === "prepared" &&
                  reserveOperation.revision === "0" &&
                  reserveOperation.result === null &&
                  reserveOperation.retiredAt === null &&
                  reserveOperation.createdAt ===
                    reserveOperation.updatedAt &&
                  sameContent(
                    canonicalReserveRequest,
                    typedRequest,
                    outcomeCode,
                  ),
                outcomeCode,
              );
              const reservedReservation = normalizeReservation(
                reserve.reservation,
                {
                  expectedReservation: null,
                  expectedSession,
                  operation: reserveOperation,
                  state: "prepared",
                },
                outcomeCode,
              );
              validateSessionTuple(
                reserve.session,
                {
                  expectedSession,
                  operation: reserveOperation,
                  reservation: reservedReservation,
                  terminal: false,
                },
                outcomeCode,
              );

              let claimReceipt;
              diagnosticStage = "claim";
              try {
                claimReceipt = await invokeAsync(
                  authority,
                  "claimCheckpointCaptureDispatch",
                  [
                    transitionInput(
                      expectedSession,
                      operationId,
                      typedRequest,
                      "0",
                    ),
                  ],
                  outcomeCode,
                );
              } catch {
                await recoverPrepared(baseInput);
                fail(outcomeCode);
              }
              diagnosticStage = "claim-receipt";
              const claim = exactDataObject(
                claimReceipt,
                CLAIM_RECEIPT_KEYS,
                outcomeCode,
              );
              ensure(
                claim.dispatchGranted === true &&
                  claim.status === "starting",
                outcomeCode,
              );
              const claimOperation = normalizeOperation(
                claim.operation,
                outcomeCode,
              );
              const canonicalClaimRequest = canonicalTypedCaptureRequest(
                claimOperation.request,
                claimOperation.expectedSession,
                canonicalAdmission,
                outcomeCode,
              );
              validateOperationCommon(
                claimOperation,
                {
                  expectedSession,
                  operationId,
                  typedRequest,
                },
                outcomeCode,
              );
              ensure(
                claimOperation.state === "starting" &&
                  claimOperation.revision === "1" &&
                  claimOperation.result === null &&
                  claimOperation.retiredAt === null &&
                  sameContent(canonicalClaimRequest, typedRequest, outcomeCode),
                outcomeCode,
              );
              const reservation = normalizeReservation(
                claim.reservation,
                {
                  expectedReservation: reservedReservation,
                  expectedSession,
                  operation: claimOperation,
                  state: "starting",
                },
                outcomeCode,
              );
              const attemptRecord = normalizeCaptureAttempt(
                claim.attempt,
                {
                  expectedAttempt: null,
                  operation: claimOperation,
                  reservation,
                  state: "authorized",
                  typedRequest,
                },
                outcomeCode,
              );
              validateSessionTuple(
                claim.session,
                {
                  expectedSession,
                  operation: claimOperation,
                  reservation,
                  terminal: false,
                },
                outcomeCode,
              );
              const claimDocument = sessionDocument(
                claim.session,
                outcomeCode,
              );
              const claimAttachment = sessionAttachment(
                claim.session,
                outcomeCode,
              );
              ensure(
                claimAttachment.rootPath === source.directory,
                outcomeCode,
              );
              dispatchDefinitelyBegan = true;
              preparedRecoveryEligible = false;
              uncertaintyInput = operationInput(
                claimOperation.expectedSession,
                operationId,
                claimOperation.request,
              );

              diagnosticStage = "prepublish-probe";
              await assertGuardHeld(probe, outcomeCode);
              const context = exactFrozenRecord({
                artifactDirectory: artifact.directory,
                artifactOwnedRoot: artifact.ownedRoot,
                canonicalAttachment: claimAttachment,
                canonicalLease: claimDocument.lease,
                captureAttemptId: attemptRecord.captureAttemptId,
                now: canonicalTimestampMilliseconds(
                  claim.authorityNow,
                  outcomeCode,
                ),
                reservationId: reservation.reservationId,
                result: attemptRecord.result,
                sourceDirectory: source.directory,
                sourceOwnedRoot: source.ownedRoot,
                storageRef: claimDocument.storageRef,
              });
              diagnosticStage = "publish";
              const completion = normalizeCompletion(
                await invokeCallback(
                  publish,
                  context,
                  outcomeCode,
                ),
                false,
                outcomeCode,
              );
              diagnosticStage = "postpublish-probe";
              await assertGuardHeld(probe, outcomeCode);
              diagnosticStage = "finalize";
              normalizeFinalizationReceipt(
                await invokeAsync(
                  authority,
                  "finalizeCheckpointCapture",
                  [
                    exactFrozenRecord({
                      completion,
                      expectedOperationRevision: "1",
                      expectedSession: claimOperation.expectedSession,
                      kind: CHECKPOINT_CAPTURE_OPERATION_KIND,
                      operationId,
                      request: claimOperation.request,
                    }),
                  ],
                  outcomeCode,
                ),
                {
                  completion,
                  expectedAttempt: claim.attempt,
                  expectedOperationRevision: "1",
                  expectedReservation: claim.reservation,
                  expectedSession,
                  operationId,
                  typedRequest,
                },
                outcomeCode,
              );
              diagnosticStage = "complete";
              return completion;
            } catch {
              if (dispatchDefinitelyBegan) {
                await markUncertain();
              } else if (
                preparedRecoveryEligible &&
                baseInput !== undefined
              ) {
                await recoverPrepared(baseInput);
              }
              fail(outcomeCode);
            }
          },
        ],
        outcomeCode,
      );
    } catch {
      await markUncertain();
      if (CHECKPOINT_CAPTURE_DIAGNOSTIC) {
        throw new ErrorConstructor(
          `checkpoint capture diagnostic stage: ${diagnosticStage}`,
        );
      }
      fail(outcomeCode);
    }
  };

  const runCaptureReconciliation =
    async function runCaptureReconciliation(admissionValue, verifyValue) {
      const admission = normalizeAdmission(
        admissionValue,
        RECONCILIATION_ADMISSION_KEYS,
        requestCode,
      );
      const verify = assertCallback(verifyValue, requestCode);
      const operationId = admission.request.operationId;
      let dispatchDefinitelyBegan = false;
      let uncertaintyInput = null;
      let uncertaintyAttempted = false;

      const markUncertain = async () => {
        if (
          !dispatchDefinitelyBegan ||
          uncertaintyInput === null ||
          uncertaintyAttempted
        ) {
          return;
        }
        uncertaintyAttempted = true;
        await bestEffortMarkUncertain(
          authority,
          uncertaintyInput,
          outcomeCode,
        );
      };

      try {
        return await invokeAsync(
          operationGuard,
          "runExclusive",
          [
            operationId,
            async (probeValue) => {
              const probe = normalizeProbe(probeValue, outcomeCode);
              try {
                const read = normalizeReconciliationReadReceipt(
                  await invokeAsync(
                    authority,
                    "readCheckpointCaptureAttempt",
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
                const attempt = read.attempt;
                const expectedOperationRevision =
                  read.expectedOperationRevision;
                const operation = read.operation;
                const canonicalAdmission = read.typedRequest.admission;
                const baseInput = operationInput(
                  operation.expectedSession,
                  operationId,
                  operation.request,
                );
                if (
                  attempt.state === "authorized" &&
                  operation.state === "starting"
                ) {
                  dispatchDefinitelyBegan = true;
                  uncertaintyInput = baseInput;
                }
                const artifact = planArtifact(
                  resolveArtifactPaths,
                  canonicalAdmission,
                  outcomeCode,
                );
                await assertGuardHeld(probe, outcomeCode);
                const completion = normalizeCompletion(
                  await invokeCallback(
                    verify,
                    exactFrozenRecord({
                      artifactDirectory: artifact.directory,
                      artifactOwnedRoot: artifact.ownedRoot,
                      captureAttempt: attempt,
                    }),
                    outcomeCode,
                  ),
                  true,
                  outcomeCode,
                );
                await assertGuardHeld(probe, outcomeCode);
                normalizeFinalizationReceipt(
                  await invokeAsync(
                    authority,
                    "finalizeCheckpointCapture",
                    [
                      exactFrozenRecord({
                        completion,
                        expectedOperationRevision,
                        expectedSession: operation.expectedSession,
                        kind: CHECKPOINT_CAPTURE_OPERATION_KIND,
                        operationId,
                        request: operation.request,
                      }),
                    ],
                    outcomeCode,
                  ),
                  {
                    completion,
                    expectedAttempt: attempt,
                    expectedOperationRevision,
                    expectedReservation: read.reservation,
                    expectedSession: operation.expectedSession,
                    operationId,
                    typedRequest: read.typedRequest,
                  },
                  outcomeCode,
                );
                return completion;
              } catch {
                await markUncertain();
                fail(outcomeCode);
              }
            },
          ],
          outcomeCode,
        );
      } catch {
        await markUncertain();
        fail(outcomeCode);
      }
    };

  const runRestore = async function runRestore(admissionValue, publishValue) {
    normalizeAdmission(
      admissionValue,
      RECONCILIATION_ADMISSION_KEYS,
      requestCode,
    );
    assertCallback(publishValue, requestCode);
    fail("postgres_checkpoint_restore_unavailable");
  };

  return exactFrozenRecord({
    runCapture,
    runCaptureReconciliation,
    runRestore,
  });
}

objectFreeze(PostgresCheckpointMutationAuthorityError.prototype);
