import { createHash } from "node:crypto";
import { constants as fsConstants } from "node:fs";
import { lstat, open, realpath } from "node:fs/promises";
import { dirname, isAbsolute, join, parse, resolve } from "node:path";
import { types as utilTypes } from "node:util";

import {
  acquireAdvisoryLock,
  sameFileIdentity,
} from "./advisory-lock.mjs";
import {
  assertCheckpointDescriptor,
  assertStorageMutationRequest,
  assertStorageMutationResult,
  compareFencingEpochs,
} from "./session-storage-contracts.mjs";
import {
  recoveryPathHasExtendedAcl,
  recoveryPathHasUnsafeAncestorAcl,
} from "./stopped-tree.mjs";

export const OPERATION_JOURNAL_RECORD_VERSION = 1;
export const OPERATION_JOURNAL_LOCK_NAME = ".operation-journal.lock";

const MAX_CANONICAL_BYTES = 512 * 1024;
const MAX_CANONICAL_DEPTH = 24;
const MAX_CANONICAL_NODES = 8_192;
const OPAQUE_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u;
const SENSITIVE_KEY_PATTERN =
  /(?:api[_-]?key|auth(?:json)?|credential|password|private[_-]?key|secret|token)/iu;
const SENSITIVE_VALUE_PATTERNS = Object.freeze([
  /\b(?:sk|sess|rk)-[A-Za-z0-9_-]{8,}\b/u,
  /eyJ[A-Za-z0-9_-]{8,}\.[A-Za-z0-9_-]+\.[A-Za-z0-9_-]*/u,
  /\b(?:AKIA[0-9A-Z]{16}|AIza[0-9A-Za-z_-]{20,}|gh[pousr]_[0-9A-Za-z]{20,}|github_pat_[0-9A-Za-z_]{20,})\b/u,
  /\bxox[baprs]-[0-9A-Za-z-]{10,}\b/u,
  /\bBearer[ \t]+[0-9A-Za-z._~+/-]{8,}={0,2}\b/iu,
  /-----BEGIN (?:[A-Z0-9]+ )*PRIVATE KEY-----/u,
]);
const RECORD_KEYS = Object.freeze([
  "recordVersion",
  "operationId",
  "revision",
  "state",
  "binding",
  "request",
  "result",
  "materialization",
]);
const STATE_REVISIONS = Object.freeze({
  committed: "3",
  materialized: "2",
  prepared: "1",
});
const FAULT_NAMES = Object.freeze([
  "afterRecordRead",
  "afterTempSync",
  "beforeRename",
  "afterRename",
  "afterDirectorySync",
  "beforeReadback",
  "afterReadback",
  "beforeLockRelease",
]);
const ERROR_MESSAGES = Object.freeze({
  invalid_journal_request: "Operation journal request is invalid",
  invalid_journal_directory: "Operation journal directory is invalid",
  invalid_journal_record: "Operation journal record is invalid",
  unsupported_journal_record: "Operation journal record version is unsupported",
  operation_conflict: "Operation ID is already bound to a different operation",
  operation_already_started: "Operation ID already has durable state",
  invalid_state_transition: "Operation journal state transition is invalid",
  journal_io_failed: "Operation journal I/O failed before commit",
  journal_commit_outcome_uncertain: "Operation journal commit outcome is uncertain",
  journal_recovery_required: "Operation journal requires recovery",
  journal_lock_release_failed: "Operation journal lock release failed",
});
const COMMIT_STATES = new Set(["committed", "not-committed", "uncertain"]);
const operationQueues = new Map();
const internalJournalErrors = new WeakSet();

export class OperationJournalError extends Error {
  constructor(code, commitState = "not-committed") {
    if (!Object.hasOwn(ERROR_MESSAGES, code) || !COMMIT_STATES.has(commitState)) {
      throw new TypeError("unsupported operation journal error");
    }
    super(ERROR_MESSAGES[code]);
    this.name = "OperationJournalError";
    this.code = code;
    this.commitState = commitState;
    this.retryable = false;
    Object.freeze(this);
  }
}

function fail(code, commitState = "not-committed") {
  throw createJournalError(code, commitState);
}

function createJournalError(code, commitState = "not-committed") {
  const error = new OperationJournalError(code, commitState);
  internalJournalErrors.add(error);
  return error;
}

function isInternalJournalError(error) {
  return (
    error !== null &&
    ["object", "function"].includes(typeof error) &&
    internalJournalErrors.has(error)
  );
}

function ensure(condition, code, commitState = "not-committed") {
  if (!condition) fail(code, commitState);
}

function safeErrorCode(error) {
  try {
    return error?.code;
  } catch {
    return undefined;
  }
}

function safeRenameOutcome(error) {
  try {
    const descriptor = Object.getOwnPropertyDescriptor(error, "renameOutcome");
    return descriptor && Object.hasOwn(descriptor, "value")
      ? descriptor.value
      : undefined;
  } catch {
    return undefined;
  }
}

async function runUntrustedIo(operation) {
  try {
    return await operation();
  } catch {
    fail("journal_io_failed");
  }
}

function inspectPlainObject(value, code) {
  if (
    utilTypes.isProxy(value) ||
    value === null ||
    typeof value !== "object" ||
    Array.isArray(value)
  ) {
    fail(code);
  }
  let prototype;
  let keys;
  try {
    prototype = Object.getPrototypeOf(value);
    keys = Reflect.ownKeys(value);
  } catch {
    fail(code);
  }
  ensure([Object.prototype, null].includes(prototype), code);
  return keys;
}

function exactOptions(value, allowedKeys, requiredKeys, code = "invalid_journal_request") {
  const keys = inspectPlainObject(value, code);
  ensure(
    keys.every((key) => typeof key === "string" && allowedKeys.includes(key)) &&
      requiredKeys.every((key) => keys.includes(key)),
    code,
  );
  const normalized = Object.create(null);
  for (const key of keys) {
    let descriptor;
    try {
      descriptor = Object.getOwnPropertyDescriptor(value, key);
    } catch {
      fail(code);
    }
    ensure(
      descriptor?.enumerable === true && Object.hasOwn(descriptor, "value"),
      code,
    );
    normalized[key] = descriptor.value;
  }
  return normalized;
}

function assertOperationId(value, code = "invalid_journal_request") {
  ensure(typeof value === "string" && OPAQUE_ID_PATTERN.test(value), code);
  return value;
}

function assertLosslessString(value, code) {
  ensure(
    typeof value === "string" &&
      value.length <= MAX_CANONICAL_BYTES &&
      Buffer.from(value, "utf8").toString("utf8") === value &&
      SENSITIVE_VALUE_PATTERNS.every((pattern) => !pattern.test(value)),
    code,
  );
  return value;
}

function jsonStringByteLength(value) {
  let bytes = 2;
  for (let index = 0; index < value.length; index += 1) {
    const unit = value.charCodeAt(index);
    if (unit === 0x22 || unit === 0x5c) bytes += 2;
    else if (unit <= 0x1f) {
      bytes +=
        unit === 0x08 ||
        unit === 0x09 ||
        unit === 0x0a ||
        unit === 0x0c ||
        unit === 0x0d
          ? 2
          : 6;
    } else if (unit >= 0xd800 && unit <= 0xdbff) {
      bytes += 4;
      index += 1;
    } else if (unit <= 0x7f) bytes += 1;
    else if (unit <= 0x7ff) bytes += 2;
    else bytes += 3;
  }
  return bytes;
}

function consumeCanonicalBytes(state, bytes, code) {
  ensure(
    Number.isSafeInteger(bytes) &&
      bytes >= 0 &&
      state.budget.bytes <= MAX_CANONICAL_BYTES - bytes,
    code,
  );
  state.budget.bytes += bytes;
}

function canonicalTraversalState(budget) {
  return { budget, depth: 0, seen: new Set() };
}

function canonicalize(
  value,
  code,
  state = canonicalTraversalState({ bytes: 0, nodes: 0 }),
) {
  state.budget.nodes += 1;
  ensure(
    state.budget.nodes <= MAX_CANONICAL_NODES && state.depth <= MAX_CANONICAL_DEPTH,
    code,
  );
  if (value === null) {
    consumeCanonicalBytes(state, 4, code);
    return value;
  }
  if (typeof value === "boolean") {
    consumeCanonicalBytes(state, value ? 4 : 5, code);
    return value;
  }
  if (typeof value === "string") {
    assertLosslessString(value, code);
    consumeCanonicalBytes(state, jsonStringByteLength(value), code);
    return value;
  }
  if (typeof value === "number") {
    ensure(Number.isFinite(value), code);
    consumeCanonicalBytes(state, Buffer.byteLength(JSON.stringify(value), "utf8"), code);
    return Object.is(value, -0) ? 0 : value;
  }
  ensure(typeof value === "object" && !utilTypes.isProxy(value), code);
  ensure(!state.seen.has(value), code);
  state.seen.add(value);
  const childState = () => ({ ...state, depth: state.depth + 1 });
  let result;
  if (Array.isArray(value)) {
    ensure(
      Number.isSafeInteger(value.length) &&
        value.length <= MAX_CANONICAL_NODES - state.budget.nodes,
      code,
    );
    consumeCanonicalBytes(state, 2 + Math.max(0, value.length - 1), code);
    let keys;
    try {
      keys = Reflect.ownKeys(value);
    } catch {
      fail(code);
    }
    const keySet = new Set(keys);
    ensure(
      keys.length === value.length + 1 &&
        keySet.has("length") &&
        Array.from({ length: value.length }, (_, index) => String(index)).every((key) =>
          keySet.has(key),
        ),
      code,
    );
    result = [];
    for (let index = 0; index < value.length; index += 1) {
      let descriptor;
      try {
        descriptor = Object.getOwnPropertyDescriptor(value, String(index));
      } catch {
        fail(code);
      }
      ensure(
        descriptor?.enumerable === true && Object.hasOwn(descriptor, "value"),
        code,
      );
      const nestedState = childState();
      result.push(canonicalize(descriptor.value, code, nestedState));
    }
  } else {
    const keys = inspectPlainObject(value, code);
    ensure(
      keys.length <= MAX_CANONICAL_NODES - state.budget.nodes &&
        keys.every((key) => typeof key === "string"),
      code,
    );
    consumeCanonicalBytes(state, 2 + Math.max(0, keys.length - 1), code);
    result = {};
    for (const key of [...keys].sort()) {
      assertLosslessString(key, code);
      ensure(!SENSITIVE_KEY_PATTERN.test(key), code);
      consumeCanonicalBytes(state, jsonStringByteLength(key) + 1, code);
      let descriptor;
      try {
        descriptor = Object.getOwnPropertyDescriptor(value, key);
      } catch {
        fail(code);
      }
      ensure(
        descriptor?.enumerable === true && Object.hasOwn(descriptor, "value"),
        code,
      );
      const nestedState = childState();
      Object.defineProperty(result, key, {
        enumerable: true,
        value: canonicalize(descriptor.value, code, nestedState),
      });
    }
  }
  state.seen.delete(value);
  return result;
}

function deepFreeze(value) {
  if (value && typeof value === "object" && !Object.isFrozen(value)) {
    for (const child of Object.values(value)) deepFreeze(child);
    Object.freeze(value);
  }
  return value;
}

function canonicalBytes(value, code = "invalid_journal_request") {
  let serialized;
  try {
    serialized = `${JSON.stringify(value)}\n`;
  } catch {
    fail(code);
  }
  ensure(Buffer.byteLength(serialized, "utf8") <= MAX_CANONICAL_BYTES, code);
  return serialized;
}

function canonicalEqual(left, right) {
  return canonicalBytes(left) === canonicalBytes(right);
}

export function snapshotOperationJournalBinding(value) {
  const binding = canonicalize(
    value,
    "invalid_journal_request",
    canonicalTraversalState({ bytes: 0, nodes: 0 }),
  );
  ensure(
    binding !== null && typeof binding === "object" && !Array.isArray(binding),
    "invalid_journal_request",
  );
  canonicalBytes(binding);
  return deepFreeze(binding);
}

function normalizeResult(value, request, code, budget) {
  const envelope = exactOptions(value, ["checkpoint", "mutation"], ["checkpoint", "mutation"], code);
  let checkpoint;
  let mutation;
  try {
    checkpoint = assertCheckpointDescriptor(envelope.checkpoint);
    mutation = assertStorageMutationResult(envelope.mutation, { request });
  } catch {
    fail(code);
  }
  ensure(
    checkpoint.checkpointId === request.target.checkpointId &&
      checkpoint.artifactId === request.target.artifactId &&
      checkpoint.sessionId === request.sessionId &&
      checkpoint.backendId === request.backendId,
    code,
  );
  if (request.operation === "checkpoint") {
    ensure(
      checkpoint.storageId === request.storageId &&
        checkpoint.sourceFencingEpoch === request.fencingEpoch,
      code,
    );
  } else {
    ensure(
      compareFencingEpochs(request.fencingEpoch, checkpoint.sourceFencingEpoch) > 0,
      code,
    );
  }
  return canonicalize(
    { checkpoint, mutation },
    code,
    canonicalTraversalState(budget),
  );
}

function normalizeOperationInput(
  value,
  {
    code = "invalid_journal_request",
    materializationRequired = false,
  } = {},
) {
  const allowed = materializationRequired
    ? ["binding", "materialization", "operationId", "request", "result"]
    : ["binding", "operationId", "request", "result"];
  const required = ["binding", "operationId", "request", "result"];
  if (materializationRequired) required.push("materialization");
  const options = exactOptions(value, allowed, required, code);
  const canonicalBudget = { bytes: 0, nodes: 0 };
  const operationId = assertOperationId(options.operationId, code);
  let request;
  try {
    request = canonicalize(
      assertStorageMutationRequest(options.request),
      code,
      canonicalTraversalState(canonicalBudget),
    );
  } catch {
    fail(code);
  }
  ensure(
    request.operationId === operationId && ["checkpoint", "restore"].includes(request.operation),
    code,
  );
  const binding = canonicalize(
    options.binding,
    code,
    canonicalTraversalState(canonicalBudget),
  );
  ensure(
    binding !== null && typeof binding === "object" && !Array.isArray(binding),
    code,
  );
  const result = normalizeResult(options.result, request, code, canonicalBudget);
  const materialization = materializationRequired
    ? canonicalize(
        options.materialization,
        code,
        canonicalTraversalState(canonicalBudget),
      )
    : null;
  if (materializationRequired) {
    ensure(
      materialization !== null &&
        typeof materialization === "object" &&
        !Array.isArray(materialization) &&
        Object.keys(materialization).length > 0,
      code,
    );
  }
  canonicalBytes({ binding, materialization, operationId, request, result });
  return deepFreeze({ binding, materialization, operationId, request, result });
}

function makeRecord({ binding, materialization, operationId, request, result }, state) {
  return deepFreeze({
    recordVersion: OPERATION_JOURNAL_RECORD_VERSION,
    operationId,
    revision: STATE_REVISIONS[state],
    state,
    binding,
    request,
    result,
    materialization,
  });
}

function frozenOutcome(record, replayed) {
  return Object.freeze({ record, replayed });
}

export function operationJournalRecordFilename(operationId) {
  const value = assertOperationId(operationId);
  const digest = createHash("sha256")
    .update("portable-codex-operation-journal\0", "utf8")
    .update(value, "utf8")
    .digest("hex");
  return `operation-sha256-${digest}.json`;
}

function parseRecord(bytes) {
  ensure(Buffer.isBuffer(bytes) && bytes.length <= MAX_CANONICAL_BYTES, "invalid_journal_record");
  const serialized = bytes.toString("utf8");
  ensure(Buffer.from(serialized, "utf8").equals(bytes), "invalid_journal_record");
  let value;
  try {
    value = JSON.parse(serialized);
  } catch {
    fail("invalid_journal_record");
  }
  const parsedKeys = inspectPlainObject(value, "invalid_journal_record");
  let versionDescriptor;
  try {
    versionDescriptor = Object.getOwnPropertyDescriptor(value, "recordVersion");
  } catch {
    fail("invalid_journal_record");
  }
  ensure(
    parsedKeys.includes("recordVersion") &&
      versionDescriptor?.enumerable === true &&
      Object.hasOwn(versionDescriptor, "value"),
    "invalid_journal_record",
  );
  ensure(
    Number.isSafeInteger(versionDescriptor.value) && versionDescriptor.value > 0,
    "invalid_journal_record",
  );
  ensure(
    versionDescriptor.value === OPERATION_JOURNAL_RECORD_VERSION,
    "unsupported_journal_record",
  );
  const options = exactOptions(value, RECORD_KEYS, RECORD_KEYS, "invalid_journal_record");
  ensure(Object.hasOwn(STATE_REVISIONS, options.state), "invalid_journal_record");
  ensure(options.revision === STATE_REVISIONS[options.state], "invalid_journal_record");
  const normalized = normalizeOperationInput(
    {
      binding: options.binding,
      operationId: options.operationId,
      request: options.request,
      result: options.result,
      ...(options.state === "prepared"
        ? {}
        : { materialization: options.materialization }),
    },
    {
      code: "invalid_journal_record",
      materializationRequired: options.state !== "prepared",
    },
  );
  ensure(
    options.state === "prepared" ? options.materialization === null : true,
    "invalid_journal_record",
  );
  const record = makeRecord(
    {
      ...normalized,
      materialization: options.state === "prepared" ? null : normalized.materialization,
    },
    options.state,
  );
  ensure(canonicalBytes(record, "invalid_journal_record") === serialized, "invalid_journal_record");
  return record;
}

function sameOperation(record, input) {
  return (
    record.operationId === input.operationId &&
    canonicalEqual(record.binding, input.binding) &&
    canonicalEqual(record.request, input.request) &&
    canonicalEqual(record.result, input.result)
  );
}

function integerAsBigInt(value) {
  if (typeof value === "bigint") return value;
  return Number.isSafeInteger(value) ? BigInt(value) : null;
}

function ancestorPermissionsAreSafe(metadata, childUid, currentUid) {
  const mode = integerAsBigInt(metadata.mode);
  const uid = integerAsBigInt(metadata.uid);
  const child = integerAsBigInt(childUid);
  const owner = integerAsBigInt(currentUid);
  if ([mode, uid, child, owner].includes(null) || !metadata.isDirectory()) return false;
  const trustedOwner = uid === owner || uid === 0n;
  const trustedChild = child === owner || child === 0n;
  const writable = (mode & 0o022n) !== 0n;
  const stickyProtectsChild = (mode & 0o1000n) !== 0n && trustedChild;
  return trustedOwner && (!writable || stickyProtectsChild);
}

async function inspectAcl(inspector, path, code) {
  let unsafe;
  try {
    unsafe = await inspector(path);
  } catch {
    fail(code);
  }
  ensure(unsafe === false, code);
}

async function openDirectoryAuthority(
  directory,
  { expectedPin, inspectAncestorAcl, inspectDirectoryAcl },
) {
  ensure(
    typeof directory === "string" &&
      isAbsolute(directory) &&
      resolve(directory) === directory &&
      directory !== parse(directory).root,
    "invalid_journal_directory",
  );
  const currentUid = process.geteuid?.() ?? process.getuid?.();
  ensure(currentUid !== undefined, "invalid_journal_directory");
  let metadata;
  let canonical;
  try {
    metadata = await lstat(directory, { bigint: true });
    canonical = await realpath(directory);
  } catch {
    fail("invalid_journal_directory");
  }
  ensure(
    metadata.isDirectory() &&
      !metadata.isSymbolicLink() &&
      metadata.uid === BigInt(currentUid) &&
      Number(metadata.mode & 0o777n) === 0o700,
    "invalid_journal_directory",
  );
  let handle;
  try {
    handle = await open(
      canonical,
      fsConstants.O_RDONLY | fsConstants.O_DIRECTORY | fsConstants.O_NOFOLLOW,
    );
    const held = await handle.stat({ bigint: true });
    ensure(
      held.isDirectory() && sameFileIdentity(held, metadata),
      "invalid_journal_directory",
    );
    if (expectedPin !== undefined) {
      ensure(
        canonical === expectedPin.path &&
          sameFileIdentity(metadata, expectedPin.identity) &&
          sameFileIdentity(held, expectedPin.identity),
        "invalid_journal_directory",
      );
    }
    await inspectAcl(inspectDirectoryAcl, canonical, "invalid_journal_directory");
    const ancestors = [];
    let childUid = metadata.uid;
    let current = dirname(canonical);
    while (true) {
      const ancestor = await lstat(current, { bigint: true });
      ensure(
        ancestorPermissionsAreSafe(ancestor, childUid, currentUid),
        "invalid_journal_directory",
      );
      await inspectAcl(inspectAncestorAcl, current, "invalid_journal_directory");
      ancestors.push({ identity: ancestor, path: current });
      const parent = dirname(current);
      if (parent === current) break;
      childUid = ancestor.uid;
      current = parent;
    }
    const authority = {
      ancestors,
      currentUid,
      handle,
      identity: metadata,
      inspectAncestorAcl,
      inspectDirectoryAcl,
      path: canonical,
    };
    authority.assertIdentityCurrent = async () => {
      try {
        const [pathMetadata, held] = await Promise.all([
          lstat(authority.path, { bigint: true }),
          authority.handle.stat({ bigint: true }),
        ]);
        ensure(
          pathMetadata.isDirectory() &&
            sameFileIdentity(pathMetadata, authority.identity) &&
            sameFileIdentity(held, authority.identity) &&
            pathMetadata.uid === BigInt(authority.currentUid) &&
            Number(pathMetadata.mode & 0o777n) === 0o700,
          "invalid_journal_directory",
        );
        let currentChildUid = pathMetadata.uid;
        for (const ancestor of authority.ancestors) {
          const currentAncestor = await lstat(ancestor.path, { bigint: true });
          ensure(
            sameFileIdentity(currentAncestor, ancestor.identity) &&
              ancestorPermissionsAreSafe(
                currentAncestor,
                currentChildUid,
                authority.currentUid,
              ),
            "invalid_journal_directory",
          );
          currentChildUid = currentAncestor.uid;
        }
      } catch (error) {
        if (isInternalJournalError(error)) throw error;
        fail("invalid_journal_directory");
      }
    };
    authority.assertCurrent = async () => {
      await authority.assertIdentityCurrent();
      await inspectAcl(
        authority.inspectDirectoryAcl,
        authority.path,
        "invalid_journal_directory",
      );
      for (const ancestor of authority.ancestors) {
        await inspectAcl(
          authority.inspectAncestorAcl,
          ancestor.path,
          "invalid_journal_directory",
        );
      }
      await authority.assertIdentityCurrent();
    };
    await authority.assertCurrent();
    return authority;
  } catch (error) {
    await handle?.close().catch(() => {});
    if (isInternalJournalError(error)) throw error;
    fail("invalid_journal_directory");
  }
}

function tempPrefix(operationId) {
  return `.${operationJournalRecordFilename(operationId).slice(0, -5)}.tmp-`;
}

function temporaryRecordFilename(operationId) {
  return `${tempPrefix(operationId)}current`;
}

async function inspectTemporaryRecord(path) {
  try {
    await lstat(path, { bigint: true });
    return true;
  } catch (error) {
    if (safeErrorCode(error) === "ENOENT") return false;
    throw error;
  }
}

async function assertTemporaryRecordAbsent(
  authority,
  operationId,
  inspectTemporaryRecordPath,
) {
  let exists;
  try {
    exists = await inspectTemporaryRecordPath(
      join(authority.path, temporaryRecordFilename(operationId)),
    );
  } catch {
    fail("journal_io_failed");
  }
  ensure(typeof exists === "boolean", "journal_io_failed");
  ensure(!exists, "journal_recovery_required");
}

async function readCanonicalRecord(
  authority,
  operationId,
  faults,
  { expectedIdentity, inspectTemporaryRecordPath },
) {
  const filename = operationJournalRecordFilename(operationId);
  await assertTemporaryRecordAbsent(
    authority,
    operationId,
    inspectTemporaryRecordPath,
  );
  const path = join(authority.path, filename);
  let handle;
  try {
    handle = await open(
      path,
      fsConstants.O_RDONLY | fsConstants.O_NOFOLLOW | fsConstants.O_NONBLOCK,
    );
  } catch (error) {
    if (safeErrorCode(error) === "ENOENT") {
      await runUntrustedIo(() => faults.afterRecordRead({ record: null }));
      await authority.assertCurrent();
      return { identity: null, record: null };
    }
    fail("invalid_journal_record");
  }
  try {
    const metadata = await handle.stat({ bigint: true });
    ensure(
      metadata.isFile() &&
        metadata.nlink === 1n &&
        metadata.uid === BigInt(authority.currentUid) &&
        Number(metadata.mode & 0o777n) === 0o600 &&
        metadata.size <= BigInt(MAX_CANONICAL_BYTES) &&
        (expectedIdentity === undefined ||
          sameFileIdentity(metadata, expectedIdentity)),
      "invalid_journal_record",
    );
    const bytes = await handle.readFile();
    const afterRead = await handle.stat({ bigint: true });
    const pathMetadata = await lstat(path, { bigint: true });
    ensure(
      sameFileIdentity(metadata, afterRead) &&
        sameFileIdentity(metadata, pathMetadata) &&
        afterRead.size === metadata.size &&
        afterRead.mtimeNs === metadata.mtimeNs &&
        afterRead.ctimeNs === metadata.ctimeNs,
      "invalid_journal_record",
    );
    const record = parseRecord(bytes);
    ensure(record.operationId === operationId, "invalid_journal_record");
    await runUntrustedIo(() => faults.afterRecordRead({ record }));
    await authority.assertCurrent();
    return {
      identity: Object.freeze({ dev: metadata.dev, ino: metadata.ino }),
      record,
    };
  } catch (error) {
    if (isInternalJournalError(error)) throw error;
    fail("journal_io_failed");
  } finally {
    await handle.close().catch(() => {});
  }
}

async function assertRecordPathIdentity({
  authority,
  expectedIdentity,
  expectedSize,
  handle,
  path,
}) {
  let held;
  let pathMetadata;
  try {
    [held, pathMetadata] = await Promise.all([
      handle.stat({ bigint: true }),
      lstat(path, { bigint: true }),
    ]);
  } catch {
    fail("journal_io_failed");
  }
  ensure(
    held.isFile() &&
      pathMetadata.isFile() &&
      held.nlink === 1n &&
      pathMetadata.nlink === 1n &&
      held.uid === BigInt(authority.currentUid) &&
      pathMetadata.uid === BigInt(authority.currentUid) &&
      Number(held.mode & 0o777n) === 0o600 &&
      Number(pathMetadata.mode & 0o777n) === 0o600 &&
      held.size === expectedSize &&
      pathMetadata.size === expectedSize &&
      sameFileIdentity(held, expectedIdentity) &&
      sameFileIdentity(pathMetadata, expectedIdentity),
    "journal_io_failed",
  );
}

async function assertPathMissing(path) {
  try {
    await lstat(path, { bigint: true });
  } catch (error) {
    if (safeErrorCode(error) === "ENOENT") return;
    fail("journal_io_failed");
  }
  fail("journal_io_failed");
}

async function assertCanonicalRecordAbsent(
  authority,
  operationId,
  inspectTemporaryRecordPath,
) {
  await assertTemporaryRecordAbsent(
    authority,
    operationId,
    inspectTemporaryRecordPath,
  );
  try {
    await lstat(
      join(authority.path, operationJournalRecordFilename(operationId)),
      { bigint: true },
    );
  } catch (error) {
    if (safeErrorCode(error) === "ENOENT") {
      await authority.assertIdentityCurrent();
      return;
    }
    fail("journal_io_failed");
  }
  fail("invalid_journal_record");
}

async function assertCanonicalPrecondition({
  authority,
  expectedIdentity,
  expectedRecord,
  operationId,
  temporaryPath,
}) {
  await authority.assertIdentityCurrent();
  ensure(
    temporaryPath === join(authority.path, temporaryRecordFilename(operationId)),
    "journal_io_failed",
  );

  const canonicalPath = join(
    authority.path,
    operationJournalRecordFilename(operationId),
  );
  if (expectedRecord === null) {
    ensure(expectedIdentity === null, "journal_io_failed");
    try {
      await lstat(canonicalPath, { bigint: true });
    } catch (error) {
      if (safeErrorCode(error) === "ENOENT") {
        await authority.assertIdentityCurrent();
        return;
      }
      fail("journal_io_failed");
    }
    fail("journal_io_failed");
  }

  ensure(expectedIdentity !== null, "journal_io_failed");
  const expectedBytes = Buffer.from(canonicalBytes(expectedRecord), "utf8");
  const expectedSize = BigInt(expectedBytes.length);
  let handle;
  try {
    handle = await open(
      canonicalPath,
      fsConstants.O_RDONLY | fsConstants.O_NOFOLLOW | fsConstants.O_NONBLOCK,
    );
    await assertRecordPathIdentity({
      authority,
      expectedIdentity,
      expectedSize,
      handle,
      path: canonicalPath,
    });
    const bytes = await handle.readFile();
    ensure(bytes.equals(expectedBytes), "journal_io_failed");
    await assertRecordPathIdentity({
      authority,
      expectedIdentity,
      expectedSize,
      handle,
      path: canonicalPath,
    });
    await authority.assertIdentityCurrent();
    await handle.close();
    handle = undefined;
  } catch (error) {
    if (isInternalJournalError(error)) throw error;
    fail("journal_io_failed");
  } finally {
    await handle?.close().catch(() => {});
  }
}

async function writeCanonicalRecord({
  authority,
  expectedIdentity,
  expectedRecord,
  faults,
  inspectTemporaryRecordPath,
  lock,
  record,
  syncDirectory,
  trustRenameOutcome,
}) {
  const filename = operationJournalRecordFilename(record.operationId);
  const temporaryPath = join(
    authority.path,
    temporaryRecordFilename(record.operationId),
  );
  const canonicalPath = join(authority.path, filename);
  const serialized = canonicalBytes(record);
  const serializedSize = BigInt(Buffer.byteLength(serialized, "utf8"));
  let temporaryHandle;
  let renameAttempted = false;
  try {
    await authority.assertCurrent();
    await runUntrustedIo(() => lock.assertHeld());
    temporaryHandle = await open(
      temporaryPath,
      fsConstants.O_WRONLY |
        fsConstants.O_CREAT |
        fsConstants.O_EXCL |
        fsConstants.O_NOFOLLOW,
      0o600,
    );
    await temporaryHandle.chmod(0o600);
    await temporaryHandle.writeFile(serialized);
    await temporaryHandle.sync();
    const temporaryMetadata = await temporaryHandle.stat({ bigint: true });
    ensure(
      temporaryMetadata.isFile() &&
        temporaryMetadata.nlink === 1n &&
        temporaryMetadata.uid === BigInt(authority.currentUid) &&
        Number(temporaryMetadata.mode & 0o777n) === 0o600 &&
        temporaryMetadata.size === serializedSize,
      "journal_io_failed",
    );
    const temporaryIdentity = Object.freeze({
      dev: temporaryMetadata.dev,
      ino: temporaryMetadata.ino,
    });
    await runUntrustedIo(() => faults.afterTempSync({ record }));
    await assertRecordPathIdentity({
      authority,
      expectedIdentity: temporaryIdentity,
      expectedSize: serializedSize,
      handle: temporaryHandle,
      path: temporaryPath,
    });
    await authority.assertCurrent();
    await runUntrustedIo(() => lock.assertHeld());
    await runUntrustedIo(() => faults.beforeRename({ record }));
    await assertRecordPathIdentity({
      authority,
      expectedIdentity: temporaryIdentity,
      expectedSize: serializedSize,
      handle: temporaryHandle,
      path: temporaryPath,
    });
    await authority.assertCurrent();
    await runUntrustedIo(() => lock.assertHeld());
    await assertRecordPathIdentity({
      authority,
      expectedIdentity: temporaryIdentity,
      expectedSize: serializedSize,
      handle: temporaryHandle,
      path: temporaryPath,
    });
    await assertCanonicalPrecondition({
      authority,
      expectedIdentity,
      expectedRecord,
      operationId: record.operationId,
      temporaryPath,
    });
    renameAttempted = true;
    try {
      await lock.renameWhileHeld(temporaryPath, canonicalPath, {
        kind: expectedIdentity === null ? "absent" : "present",
        ...(expectedIdentity === null
          ? {}
          : {
              dev: expectedIdentity.dev.toString(),
              ino: expectedIdentity.ino.toString(),
            }),
      });
    } catch (error) {
      if (trustRenameOutcome && safeRenameOutcome(error) === "not-committed") {
        renameAttempted = false;
        fail("journal_io_failed", "not-committed");
      }
      throw error;
    }
    await assertPathMissing(temporaryPath);
    await assertRecordPathIdentity({
      authority,
      expectedIdentity: temporaryIdentity,
      expectedSize: serializedSize,
      handle: temporaryHandle,
      path: canonicalPath,
    });
    await runUntrustedIo(() => faults.afterRename({ record }));
    await assertRecordPathIdentity({
      authority,
      expectedIdentity: temporaryIdentity,
      expectedSize: serializedSize,
      handle: temporaryHandle,
      path: canonicalPath,
    });
    await runUntrustedIo(() => syncDirectory(authority.handle, authority.path));
    await runUntrustedIo(() => faults.afterDirectorySync({ record }));
    await authority.assertCurrent();
    await runUntrustedIo(() => lock.assertHeld());
    await runUntrustedIo(() => faults.beforeReadback({ record }));
    const readback = await readCanonicalRecord(
      authority,
      record.operationId,
      {
        ...faults,
        afterRecordRead: async () => {},
      },
      {
        expectedIdentity: temporaryIdentity,
        inspectTemporaryRecordPath,
      },
    );
    ensure(
      readback.record !== null && canonicalEqual(readback.record, record),
      "invalid_journal_record",
    );
    await runUntrustedIo(() => faults.afterReadback({ record: readback.record }));
    await assertRecordPathIdentity({
      authority,
      expectedIdentity: temporaryIdentity,
      expectedSize: serializedSize,
      handle: temporaryHandle,
      path: canonicalPath,
    });
    await authority.assertCurrent();
    await runUntrustedIo(() => lock.assertHeld());
    await assertRecordPathIdentity({
      authority,
      expectedIdentity: temporaryIdentity,
      expectedSize: serializedSize,
      handle: temporaryHandle,
      path: canonicalPath,
    });
    await temporaryHandle.close();
    temporaryHandle = undefined;
    return {
      identity: temporaryIdentity,
      record: readback.record,
    };
  } catch (error) {
    if (!renameAttempted) {
      if (isInternalJournalError(error)) throw error;
      fail("journal_io_failed", "not-committed");
    }
    fail("journal_commit_outcome_uncertain", "uncertain");
  } finally {
    await temporaryHandle?.close().catch(() => {});
    // Never remove a temporary or canonical pathname here. Before-rename
    // leftovers are explicit recovery evidence; after rename the commit may be
    // visible even when acknowledgement or directory durability was lost.
  }
}

async function confirmVisibleRecord({
  authority,
  identity,
  inspectTemporaryRecordPath,
  lock,
  operationId,
  record,
  syncDirectory,
}) {
  if (record === null) return null;
  const canonicalPath = join(
    authority.path,
    operationJournalRecordFilename(operationId),
  );
  const expectedSize = BigInt(Buffer.byteLength(canonicalBytes(record), "utf8"));
  let handle;
  try {
    await authority.assertCurrent();
    await runUntrustedIo(() => lock.assertHeld());
    handle = await open(
      canonicalPath,
      fsConstants.O_RDONLY | fsConstants.O_NOFOLLOW | fsConstants.O_NONBLOCK,
    );
    await assertRecordPathIdentity({
      authority,
      expectedIdentity: identity,
      expectedSize,
      handle,
      path: canonicalPath,
    });
    const beforeSync = await readCanonicalRecord(
      authority,
      operationId,
      { afterRecordRead: async () => {} },
      { expectedIdentity: identity, inspectTemporaryRecordPath },
    );
    ensure(
      beforeSync.record !== null && canonicalEqual(beforeSync.record, record),
      "invalid_journal_record",
    );
    await handle.sync();
    await assertRecordPathIdentity({
      authority,
      expectedIdentity: identity,
      expectedSize,
      handle,
      path: canonicalPath,
    });
    await runUntrustedIo(() => syncDirectory(authority.handle, authority.path));
    await authority.assertCurrent();
    await runUntrustedIo(() => lock.assertHeld());
    const confirmed = await readCanonicalRecord(
      authority,
      operationId,
      { afterRecordRead: async () => {} },
      { expectedIdentity: identity, inspectTemporaryRecordPath },
    );
    ensure(
      confirmed.record !== null && canonicalEqual(confirmed.record, record),
      "invalid_journal_record",
    );
    await assertRecordPathIdentity({
      authority,
      expectedIdentity: identity,
      expectedSize,
      handle,
      path: canonicalPath,
    });
    await handle.close();
    handle = undefined;
    return confirmed.record;
  } catch {
    fail("journal_commit_outcome_uncertain", "uncertain");
  } finally {
    await handle?.close().catch(() => {});
  }
}

async function verifyVisibleRecord({
  authority,
  identity,
  inspectTemporaryRecordPath,
  lock,
  operationId,
  record,
  syncDirectory,
}) {
  if (record !== null) {
    await confirmVisibleRecord({
      authority,
      identity,
      inspectTemporaryRecordPath,
      lock,
      operationId,
      record,
      syncDirectory,
    });
    return;
  }
  try {
    await authority.assertCurrent();
    await runUntrustedIo(() => lock.assertHeld());
    const confirmed = await readCanonicalRecord(
      authority,
      operationId,
      { afterRecordRead: async () => {} },
      { inspectTemporaryRecordPath },
    );
    ensure(
      confirmed.identity === null && confirmed.record === null,
      "invalid_journal_record",
    );
    await authority.assertCurrent();
    await runUntrustedIo(() => lock.assertHeld());
    await assertCanonicalRecordAbsent(
      authority,
      operationId,
      inspectTemporaryRecordPath,
    );
  } catch {
    fail("journal_commit_outcome_uncertain", "uncertain");
  }
}

function normalizeFaults(value) {
  const options = value === undefined
    ? Object.create(null)
    : exactOptions(value, FAULT_NAMES, [], "invalid_journal_request");
  const faults = {};
  for (const name of FAULT_NAMES) {
    const operation = options[name] ?? (async () => {});
    ensure(typeof operation === "function", "invalid_journal_request");
    faults[name] = operation;
  }
  return Object.freeze(faults);
}

function runQueued(key, operation) {
  const previous = operationQueues.get(key) ?? Promise.resolve();
  const current = previous.catch(() => {}).then(operation);
  operationQueues.set(key, current);
  return current.finally(() => {
    if (operationQueues.get(key) === current) operationQueues.delete(key);
  });
}

function normalizeRuntimeError(error, commitState = "not-committed") {
  if (isInternalJournalError(error)) return error;
  return createJournalError(
    commitState === "uncertain"
      ? "journal_commit_outcome_uncertain"
      : "journal_io_failed",
    commitState,
  );
}

export class FilesystemOperationJournal {
  #acquireLock;
  #directory;
  #directoryPinPromise;
  #faults;
  #inspectAncestorAcl;
  #inspectDirectoryAcl;
  #inspectTemporaryRecord;
  #syncDirectory;
  #trustRenameOutcome;

  constructor(options) {
    const normalized = exactOptions(
      options,
      [
        "acquireLock",
        "directory",
        "faults",
        "inspectAncestorAcl",
        "inspectDirectoryAcl",
        "inspectTemporaryRecord",
        "syncDirectory",
      ],
      ["directory"],
    );
    ensure(
      typeof normalized.directory === "string" &&
        isAbsolute(normalized.directory) &&
        resolve(normalized.directory) === normalized.directory,
      "invalid_journal_request",
    );
    this.#directory = normalized.directory;
    this.#acquireLock = normalized.acquireLock ?? acquireAdvisoryLock;
    this.#trustRenameOutcome = this.#acquireLock === acquireAdvisoryLock;
    this.#inspectAncestorAcl =
      normalized.inspectAncestorAcl ?? recoveryPathHasUnsafeAncestorAcl;
    this.#inspectDirectoryAcl =
      normalized.inspectDirectoryAcl ?? recoveryPathHasExtendedAcl;
    this.#inspectTemporaryRecord =
      normalized.inspectTemporaryRecord ?? inspectTemporaryRecord;
    this.#syncDirectory = normalized.syncDirectory ?? (async (handle) => handle.sync());
    this.#faults = normalizeFaults(normalized.faults);
    for (const operation of [
      this.#acquireLock,
      this.#inspectAncestorAcl,
      this.#inspectDirectoryAcl,
      this.#inspectTemporaryRecord,
      this.#syncDirectory,
    ]) {
      ensure(typeof operation === "function", "invalid_journal_request");
    }
    Object.freeze(this);
  }

  #getDirectoryPin() {
    if (this.#directoryPinPromise !== undefined) return this.#directoryPinPromise;
    const attempt = (async () => {
      const authority = await openDirectoryAuthority(this.#directory, {
        inspectAncestorAcl: this.#inspectAncestorAcl,
        inspectDirectoryAcl: this.#inspectDirectoryAcl,
      });
      const pin = Object.freeze({
        identity: Object.freeze({
          dev: authority.identity.dev,
          ino: authority.identity.ino,
        }),
        path: authority.path,
      });
      try {
        await authority.handle.close();
      } catch {
        fail("invalid_journal_directory");
      }
      return pin;
    })();
    this.#directoryPinPromise = attempt;
    void attempt.catch(() => {
      if (this.#directoryPinPromise === attempt) this.#directoryPinPromise = undefined;
    });
    return attempt;
  }

  async #run(operationId, operation) {
    const pin = await this.#getDirectoryPin();
    const queueKey = `${pin.identity.dev.toString()}\0${pin.identity.ino.toString()}`;
    return runQueued(queueKey, async () => {
      const authority = await openDirectoryAuthority(pin.path, {
        expectedPin: pin,
        inspectAncestorAcl: this.#inspectAncestorAcl,
        inspectDirectoryAcl: this.#inspectDirectoryAcl,
      });
      let lock;
      let primaryError;
      let result;
      let operationCommitState = "not-committed";
      try {
        try {
          lock = await this.#acquireLock(
            join(authority.path, OPERATION_JOURNAL_LOCK_NAME),
            { requireExisting: true },
          );
        } catch {
          fail("journal_io_failed");
        }
        await authority.assertCurrent();
        const completed = await operation({ authority, lock });
        result = completed.value;
        operationCommitState = completed.commitState;
        try {
          await this.#faults.beforeLockRelease({ operationId });
        } catch {
          throw createJournalError(
            "journal_lock_release_failed",
            operationCommitState,
          );
        }
        await verifyVisibleRecord({
          authority,
          inspectTemporaryRecordPath: this.#inspectTemporaryRecord,
          lock,
          syncDirectory: this.#syncDirectory,
          ...completed.verification,
        });
      } catch (error) {
        primaryError = normalizeRuntimeError(error, operationCommitState);
        operationCommitState = primaryError.commitState;
      }

      let releaseFailed = false;
      if (lock) {
        try {
          await lock.release();
        } catch {
          releaseFailed = true;
        }
      }
      let closeFailed = false;
      try {
        await authority.handle.close();
      } catch {
        closeFailed = true;
      }
      if (releaseFailed || closeFailed) {
        primaryError = createJournalError(
          "journal_lock_release_failed",
          operationCommitState,
        );
      }
      if (primaryError) throw primaryError;
      return result;
    });
  }

  async read(options) {
    const { operationId } = exactOptions(
      options,
      ["operationId"],
      ["operationId"],
    );
    const normalizedId = assertOperationId(operationId);
    return this.#run(normalizedId, async ({ authority, lock }) => {
      await runUntrustedIo(() => lock.assertHeld());
      const visible = await readCanonicalRecord(
        authority,
        normalizedId,
        this.#faults,
        { inspectTemporaryRecordPath: this.#inspectTemporaryRecord },
      );
      const record = await confirmVisibleRecord({
        authority,
        identity: visible.identity,
        inspectTemporaryRecordPath: this.#inspectTemporaryRecord,
        lock,
        operationId: normalizedId,
        record: visible.record,
        syncDirectory: this.#syncDirectory,
      });
      return {
        commitState: "not-committed",
        value: frozenOutcome(record, false),
        verification: {
          identity: visible.identity,
          operationId: normalizedId,
          record,
        },
      };
    });
  }

  async readStateHint(options) {
    const { operationId } = exactOptions(
      options,
      ["operationId"],
      ["operationId"],
    );
    const normalizedId = assertOperationId(operationId);
    const pin = await this.#getDirectoryPin();
    const queueKey = `${pin.identity.dev.toString()}\0${pin.identity.ino.toString()}`;
    return runQueued(queueKey, async () => {
      let authority;
      let outcome;
      let primaryError;
      try {
        authority = await openDirectoryAuthority(pin.path, {
          expectedPin: pin,
          inspectAncestorAcl: this.#inspectAncestorAcl,
          inspectDirectoryAcl: this.#inspectDirectoryAcl,
        });
        const visible = await readCanonicalRecord(
          authority,
          normalizedId,
          { afterRecordRead: async () => {} },
          { inspectTemporaryRecordPath: this.#inspectTemporaryRecord },
        );
        await authority.assertCurrent();
        outcome = frozenOutcome(visible.record, false);
      } catch (error) {
        primaryError = normalizeRuntimeError(error);
      }
      try {
        await authority?.handle.close();
      } catch {
        primaryError = createJournalError("journal_io_failed", "not-committed");
      }
      if (primaryError !== undefined) throw primaryError;
      return outcome;
    });
  }

  async describeAuthority() {
    const pin = await this.#getDirectoryPin();
    return Object.freeze({
      device: pin.identity.dev.toString(),
      inode: pin.identity.ino.toString(),
      path: pin.path,
    });
  }

  async prepare(options) {
    const input = normalizeOperationInput(options);
    return this.#transition(input, "prepared");
  }

  async prepareFresh(options) {
    const input = normalizeOperationInput(options);
    return this.#transition(input, "prepared", { requireAbsent: true });
  }

  async markMaterialized(options) {
    const input = normalizeOperationInput(options, { materializationRequired: true });
    return this.#transition(input, "materialized");
  }

  async commit(options) {
    const input = normalizeOperationInput(options, { materializationRequired: true });
    return this.#transition(input, "committed");
  }

  async #transition(input, requestedState, { requireAbsent = false } = {}) {
    return this.#run(input.operationId, async ({ authority, lock }) => {
      await runUntrustedIo(() => lock.assertHeld());
      const visible = await readCanonicalRecord(
        authority,
        input.operationId,
        this.#faults,
        { inspectTemporaryRecordPath: this.#inspectTemporaryRecord },
      );
      const existing = await confirmVisibleRecord({
        authority,
        identity: visible.identity,
        inspectTemporaryRecordPath: this.#inspectTemporaryRecord,
        lock,
        operationId: input.operationId,
        record: visible.record,
        syncDirectory: this.#syncDirectory,
      });
      const existingVerification = {
        identity: visible.identity,
        operationId: input.operationId,
        record: existing,
      };
      if (requireAbsent && existing !== null) {
        fail("operation_already_started");
      }
      if (existing !== null) {
        ensure(sameOperation(existing, input), "operation_conflict");
      }

      if (requestedState === "prepared" && existing !== null) {
        return {
          commitState: "not-committed",
          value: frozenOutcome(existing, true),
          verification: existingVerification,
        };
      }
      if (requestedState === "materialized" && existing !== null) {
        if (["materialized", "committed"].includes(existing.state)) {
          ensure(
            canonicalEqual(existing.materialization, input.materialization),
            "operation_conflict",
          );
          return {
            commitState: "not-committed",
            value: frozenOutcome(existing, true),
            verification: existingVerification,
          };
        }
        ensure(existing.state === "prepared", "invalid_state_transition");
      }
      if (requestedState === "committed") {
        ensure(existing !== null, "invalid_state_transition");
        if (existing.state === "committed") {
          ensure(
            canonicalEqual(existing.materialization, input.materialization),
            "operation_conflict",
          );
          return {
            commitState: "not-committed",
            value: frozenOutcome(existing, true),
            verification: existingVerification,
          };
        }
        ensure(existing.state === "materialized", "invalid_state_transition");
        ensure(
          canonicalEqual(existing.materialization, input.materialization),
          "operation_conflict",
        );
      }
      if (requestedState !== "prepared") {
        ensure(existing !== null, "invalid_state_transition");
      }

      const record = makeRecord(
        {
          ...input,
          materialization:
            requestedState === "prepared" ? null : input.materialization,
        },
        requestedState,
      );
      const published = await writeCanonicalRecord({
        authority,
        expectedIdentity: visible.identity,
        expectedRecord: existing,
        faults: this.#faults,
        inspectTemporaryRecordPath: this.#inspectTemporaryRecord,
        lock,
        record,
        syncDirectory: this.#syncDirectory,
        trustRenameOutcome: this.#trustRenameOutcome,
      });
      return {
        commitState: "committed",
        value: frozenOutcome(published.record, false),
        verification: {
          identity: published.identity,
          operationId: input.operationId,
          record: published.record,
        },
      };
    });
  }
}
