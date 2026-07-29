import { types as utilTypes } from "node:util";

import {
  PostgresSerializableStore,
} from "./postgres-serializable-store.mjs";
import {
  assertSessionManifest,
  assertSessionStorageRef,
  assertStorageBackendCapabilities,
} from "./session-storage-contracts.mjs";

export const SESSION_AUTHORITY_DOCUMENT_VERSION = 1;

const UUID_PATTERN =
  /^[0-9a-f]{8}-[0-9a-f]{4}-[1-8][0-9a-f]{3}-[89ab][0-9a-f]{3}-[0-9a-f]{12}$/u;
const REVISION_PATTERN = /^(?:0|[1-9][0-9]{0,18})$/u;
const MAX_POSTGRES_BIGINT = 9_223_372_036_854_775_807n;
const DOCUMENT_KEYS = Object.freeze([
  "activeOperation",
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
]);
const ROW_KEYS = Object.freeze([
  "created_at",
  "document",
  "revision",
  "session_id",
  "updated_at",
]);
const ERROR_MESSAGES = Object.freeze({
  invalid_authority_options: "PostgreSQL session authority options are invalid",
  invalid_session_read: "Session read request is invalid",
  invalid_session_registration: "Session registration request is invalid",
  session_identity_conflict:
    "Session ID is already bound to a different canonical document",
  session_not_found: "Session is not registered",
  session_state_invalid: "Stored session authority state is invalid",
});

const BigIntConstructor = BigInt;
const ArrayConstructor = Array;
const arrayEveryIntrinsic = Array.prototype.every;
const arrayIncludesIntrinsic = Array.prototype.includes;
const arrayIsArray = Array.isArray;
const dateGetTimeIntrinsic = Date.prototype.getTime;
const datePrototype = Date.prototype;
const dateToISOStringIntrinsic = Date.prototype.toISOString;
const jsonStringifyIntrinsic = JSON.stringify;
const isProxyValue = utilTypes.isProxy;
const numberIsFinite = Number.isFinite;
const objectCreate = Object.create;
const objectFreeze = Object.freeze;
const objectGetOwnPropertyDescriptor = Object.getOwnPropertyDescriptor;
const objectGetPrototypeOf = Object.getPrototypeOf;
const objectHasOwn = Object.hasOwn;
const objectIsFrozen = Object.isFrozen;
const objectPrototype = Object.prototype;
const objectSetPrototypeOf = Object.setPrototypeOf;
const reflectApply = Reflect.apply;
const reflectOwnKeys = Reflect.ownKeys;
const regexpExecIntrinsic = RegExp.prototype.exec;
const runSerializableIntrinsic =
  PostgresSerializableStore.prototype.runSerializable;

const INSERT_SESSION_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    "INSERT INTO session_authority.sessions",
    "(session_id, revision, document, created_at, updated_at)",
    "VALUES ($1::uuid, 0, $2::jsonb, $3::timestamptz, $3::timestamptz)",
    "ON CONFLICT (session_id) DO NOTHING",
    "RETURNING session_id, revision, document, created_at, updated_at",
  ].join(" "),
});
const READ_SESSION_QUERY = Object.freeze({
  queryMode: "extended",
  text: [
    "SELECT session_id, revision, document, created_at, updated_at",
    "FROM session_authority.sessions",
    "WHERE session_id = $1::uuid",
  ].join(" "),
});
const READ_SESSION_FOR_UPDATE_QUERY = Object.freeze({
  queryMode: "extended",
  text: `${READ_SESSION_QUERY.text} FOR UPDATE`,
});

export class PostgresSessionAuthorityError extends Error {
  constructor(code) {
    if (!objectHasOwn(ERROR_MESSAGES, code)) {
      throw new TypeError("unsupported PostgreSQL session authority error");
    }
    super(ERROR_MESSAGES[code]);
    this.name = "PostgresSessionAuthorityError";
    this.code = code;
    this.retryable = false;
    objectFreeze(this);
  }
}

function fail(code) {
  throw new PostgresSessionAuthorityError(code);
}

function ensure(condition, code) {
  if (!condition) fail(code);
}

function regexpTest(pattern, value) {
  return reflectApply(regexpExecIntrinsic, pattern, [value]) !== null;
}

function arrayEvery(value, callback) {
  return reflectApply(arrayEveryIntrinsic, value, [callback]);
}

function ownDataValue(value, key, code) {
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
  return descriptor.value;
}

function exactPlainObject(value, keys, code) {
  if (
    value === null ||
    typeof value !== "object" ||
    arrayIsArray(value) ||
    isProxyValue(value)
  ) {
    fail(code);
  }
  let actual;
  let prototype;
  try {
    prototype = objectGetPrototypeOf(value);
    actual = reflectOwnKeys(value);
  } catch {
    fail(code);
  }
  ensure(prototype === objectPrototype || prototype === null, code);
  ensure(
    actual.length === keys.length &&
      arrayEvery(
        actual,
        (key) =>
          typeof key === "string" &&
          reflectApply(arrayIncludesIntrinsic, keys, [key]),
    ),
    code,
  );
  const normalized = objectCreate(null);
  for (let index = 0; index < actual.length; index += 1) {
    const key = actual[index];
    normalized[key] = ownDataValue(value, key, code);
  }
  return normalized;
}

function canonicalSessionId(value, code) {
  ensure(typeof value === "string" && regexpTest(UUID_PATTERN, value), code);
  return value;
}

function canonicalDocument(value, code) {
  const document = exactPlainObject(value, DOCUMENT_KEYS, code);
  ensure(
    document.documentVersion === SESSION_AUTHORITY_DOCUMENT_VERSION &&
      document.lifecycle === "DETACHED" &&
      document.writerEpoch === "0" &&
      document.lease === null &&
      document.attachment === null &&
      document.activeOperation === null &&
      document.recovery === null &&
      document.launch === null,
    code,
  );
  let manifest;
  let storageRef;
  let backendCapabilities;
  try {
    manifest = assertSessionManifest(document.manifest);
    storageRef = assertSessionStorageRef(document.storageRef);
    backendCapabilities = assertStorageBackendCapabilities(
      document.backendCapabilities,
    );
  } catch {
    fail(code);
  }
  ensure(manifest.sessionId === storageRef.sessionId, code);
  return assembleCanonicalDocument({
    backendCapabilities,
    manifest,
    storageRef,
  });
}

function registrationDocument(options) {
  const normalized = exactPlainObject(
    options,
    ["backendCapabilities", "manifest", "storageRef"],
    "invalid_session_registration",
  );
  let manifest;
  let storageRef;
  let backendCapabilities;
  try {
    manifest = assertSessionManifest(normalized.manifest);
    storageRef = assertSessionStorageRef(normalized.storageRef);
    backendCapabilities = assertStorageBackendCapabilities(
      normalized.backendCapabilities,
    );
  } catch {
    fail("invalid_session_registration");
  }
  ensure(
    manifest.sessionId === storageRef.sessionId,
    "invalid_session_registration",
  );
  return assembleCanonicalDocument({
    backendCapabilities,
    manifest,
    storageRef,
  });
}

function assembleCanonicalDocument({
  backendCapabilities,
  manifest,
  storageRef,
}) {
  return deepFreeze({
    documentVersion: SESSION_AUTHORITY_DOCUMENT_VERSION,
    manifest: {
      schemaVersion: manifest.schemaVersion,
      sessionId: manifest.sessionId,
      codex: {
        rootThreadId: manifest.codex.rootThreadId,
        sessionId: manifest.codex.sessionId,
        ephemeral: manifest.codex.ephemeral,
        historyMode: manifest.codex.historyMode,
      },
      runtime: {
        imageDigest: manifest.runtime.imageDigest,
        imageMediaType: manifest.runtime.imageMediaType,
        platform: manifest.runtime.platform,
        codexVersion: manifest.runtime.codexVersion,
        codexSandbox: manifest.runtime.codexSandbox,
      },
      layoutVersion: manifest.layoutVersion,
      authMode: manifest.authMode,
      agents: {
        defaultMaxSubagents: manifest.agents.defaultMaxSubagents,
        maxSubagents: manifest.agents.maxSubagents,
        maxDepth: manifest.agents.maxDepth,
      },
    },
    storageRef: {
      contractVersion: storageRef.contractVersion,
      backendId: storageRef.backendId,
      storageId: storageRef.storageId,
      sessionId: storageRef.sessionId,
    },
    backendCapabilities: {
      atomicPointInTimeCheckpoint:
        backendCapabilities.atomicPointInTimeCheckpoint,
      exclusiveWriterAttachment:
        backendCapabilities.exclusiveWriterAttachment,
      fencing: backendCapabilities.fencing,
      normalDirectoryAttachment:
        backendCapabilities.normalDirectoryAttachment,
    },
    lifecycle: "DETACHED",
    writerEpoch: "0",
    lease: null,
    attachment: null,
    activeOperation: null,
    recovery: null,
    launch: null,
  });
}

function deepFreeze(value) {
  if (
    value !== null &&
    typeof value === "object" &&
    !objectIsFrozen(value)
  ) {
    const keys = reflectOwnKeys(value);
    for (let index = 0; index < keys.length; index += 1) {
      const descriptor = objectGetOwnPropertyDescriptor(value, keys[index]);
      if (descriptor && objectHasOwn(descriptor, "value")) {
        deepFreeze(descriptor.value);
      }
    }
    objectFreeze(value);
  }
  return value;
}

function nullPrototypeJsonDataTree(value) {
  if (value === null || typeof value !== "object") return value;
  const keys = reflectOwnKeys(value);
  let copy;
  if (arrayIsArray(value)) {
    copy = new ArrayConstructor(value.length);
    objectSetPrototypeOf(copy, null);
  } else {
    copy = objectCreate(null);
  }
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
    if (key === "length") continue;
    const descriptor = objectGetOwnPropertyDescriptor(value, key);
    if (descriptor && objectHasOwn(descriptor, "value")) {
      copy[key] = nullPrototypeJsonDataTree(descriptor.value);
    }
  }
  return copy;
}

function canonicalSerialize(document) {
  return reflectApply(jsonStringifyIntrinsic, JSON, [
    nullPrototypeJsonDataTree(document),
  ]);
}

function canonicalRevision(value) {
  ensure(
    typeof value === "string" && regexpTest(REVISION_PATTERN, value),
    "session_state_invalid",
  );
  let revision;
  try {
    revision = BigIntConstructor(value);
  } catch {
    fail("session_state_invalid");
  }
  ensure(revision <= MAX_POSTGRES_BIGINT, "session_state_invalid");
  return value;
}

function canonicalTimestamp(value) {
  ensure(
    value !== null &&
      typeof value === "object" &&
      !isProxyValue(value) &&
      objectGetPrototypeOf(value) === datePrototype,
    "session_state_invalid",
  );
  const milliseconds = reflectApply(dateGetTimeIntrinsic, value, []);
  ensure(numberIsFinite(milliseconds), "session_state_invalid");
  return reflectApply(dateToISOStringIntrinsic, value, []);
}

function rowsFromResult(result) {
  ensure(
    result !== null &&
      typeof result === "object" &&
      !isProxyValue(result),
    "session_state_invalid",
  );
  const rows = ownDataValue(result, "rows", "session_state_invalid");
  ensure(
    arrayIsArray(rows) &&
      !isProxyValue(rows) &&
      (rows.length === 0 || rows.length === 1),
    "session_state_invalid",
  );
  for (let index = 0; index < rows.length; index += 1) {
    ownDataValue(rows, String(index), "session_state_invalid");
  }
  return rows;
}

function snapshotFromRow(row, expectedSessionId) {
  const normalized = exactPlainObject(
    row,
    ROW_KEYS,
    "session_state_invalid",
  );
  const sessionId = canonicalSessionId(
    normalized.session_id,
    "session_state_invalid",
  );
  ensure(sessionId === expectedSessionId, "session_state_invalid");
  const revision = canonicalRevision(normalized.revision);
  ensure(revision === "0", "session_state_invalid");
  const document = canonicalDocument(
    normalized.document,
    "session_state_invalid",
  );
  ensure(document.manifest.sessionId === sessionId, "session_state_invalid");
  const createdAt = canonicalTimestamp(normalized.created_at);
  const updatedAt = canonicalTimestamp(normalized.updated_at);
  ensure(createdAt === updatedAt, "session_state_invalid");
  return deepFreeze({
    sessionId,
    revision,
    document,
    createdAt,
    updatedAt,
  });
}

function runSerializable(store, callback) {
  return reflectApply(runSerializableIntrinsic, store, [callback]);
}

export class PostgresSessionAuthority {
  #store;

  constructor(options) {
    const normalized = exactPlainObject(
      options,
      ["store"],
      "invalid_authority_options",
    );
    let prototype;
    let ownKeys;
    try {
      prototype = objectGetPrototypeOf(normalized.store);
      ownKeys = reflectOwnKeys(normalized.store);
    } catch {
      fail("invalid_authority_options");
    }
    ensure(
      normalized.store !== null &&
        typeof normalized.store === "object" &&
        !isProxyValue(normalized.store) &&
        prototype === PostgresSerializableStore.prototype &&
        ownKeys.length === 0 &&
        objectIsFrozen(normalized.store),
      "invalid_authority_options",
    );
    this.#store = normalized.store;
    objectFreeze(this);
  }

  async registerSession(options) {
    const document = registrationDocument(options);
    const sessionId = document.manifest.sessionId;
    const serializedDocument = canonicalSerialize(document);
    return runSerializable(this.#store, async (transaction) => {
      const inserted = rowsFromResult(
        await transaction.query(INSERT_SESSION_QUERY.text, [
          sessionId,
          serializedDocument,
          transaction.now,
        ]),
      );
      if (inserted.length === 1) {
        const snapshot = snapshotFromRow(inserted[0], sessionId);
        ensure(
          snapshot.revision === "0" &&
            snapshot.createdAt === transaction.now &&
            snapshot.updatedAt === transaction.now &&
            canonicalSerialize(snapshot.document) === serializedDocument,
          "session_state_invalid",
        );
        return snapshot;
      }
      const existing = rowsFromResult(
        await transaction.query(READ_SESSION_FOR_UPDATE_QUERY.text, [
          sessionId,
        ]),
      );
      ensure(existing.length === 1, "session_state_invalid");
      const snapshot = snapshotFromRow(existing[0], sessionId);
      ensure(
        canonicalSerialize(snapshot.document) === serializedDocument,
        "session_identity_conflict",
      );
      return snapshot;
    });
  }

  async readSession(options) {
    const normalized = exactPlainObject(
      options,
      ["sessionId"],
      "invalid_session_read",
    );
    const sessionId = canonicalSessionId(
      normalized.sessionId,
      "invalid_session_read",
    );
    return runSerializable(this.#store, async (transaction) => {
      const rows = rowsFromResult(
        await transaction.query(READ_SESSION_QUERY.text, [sessionId]),
      );
      ensure(rows.length === 1, "session_not_found");
      return snapshotFromRow(rows[0], sessionId);
    });
  }
}
