import { createHash, randomUUID as systemRandomUUID } from "node:crypto";

const AUTH_PAYLOAD_SCHEMA_VERSION = 1;
const DEFAULT_MIN_TOKEN_TTL_SECONDS = 120;
const GENERATION_PATTERN = /^(?:0|[1-9][0-9]{0,19})$/u;
const MAX_GENERATION = 18_446_744_073_709_551_615n;
const MAX_SHARED_REFRESH_RECHECKS = 8;
const MAX_STORAGE_ONLY_REBASES = 8;
const OPAQUE_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u;
const SHA256_PATTERN = /^[a-f0-9]{64}$/u;
const SINGLEFLIGHT = new Map();
const READY_CREDENTIAL_KEYS = [
  "accessToken",
  "accountId",
  "authJson",
  "expiresAt",
  "planType",
  "userId",
];
const SAFE_REASONS = new Set([
  "access_token_unchanged",
  "account_identity_changed",
  "adapter_post_dispatch_uncertain",
  "invalid_grant",
  "invalid_refresh_candidate",
  "refresh_token_reused",
  "refresh_in_progress",
  "remote_reauth_required",
  "token_ttl_insufficient",
  "user_identity_changed",
]);
const INVALID_STORE_CODES = new Set([
  "auth_state_integrity_failed",
  "authority_binding_mismatch",
  "commit_id_conflict",
  "commit_verification_failed",
  "generation_exhausted",
  "invalid_auth_state",
  "invalid_authority_id",
  "invalid_commit_id",
  "invalid_generation",
  "invalid_key_id",
  "invalid_key_material",
  "invalid_key_provider",
  "invalid_lock_provider",
  "invalid_payload",
  "invalid_random_source",
  "invalid_store_directory",
  "unsupported_auth_state",
]);
const ERROR_DEFINITIONS = Object.freeze({
  cas_conflict: { message: "Authentication state changed concurrently", retryable: false },
  invalid_credential: { message: "Authentication credential is invalid", retryable: false },
  invalid_grant: { message: "Authentication requires interactive login", retryable: false },
  invalid_request: { message: "Authentication broker request is invalid", retryable: false },
  invalid_store_snapshot: {
    message: "Authentication store returned invalid state",
    retryable: false,
  },
  reauth_required: { message: "Authentication requires interactive login", retryable: false },
  recovery_required: {
    message: "Authentication refresh requires operator recovery",
    retryable: false,
  },
  refresh_failed: { message: "Authentication refresh failed before commit", retryable: true },
  refresh_outcome_uncertain: {
    message: "Authentication refresh outcome is uncertain",
    retryable: false,
  },
  refresh_token_reused: {
    message: "Authentication requires interactive login",
    retryable: false,
  },
  store_unavailable: { message: "Authentication store is unavailable", retryable: true },
  token_ttl_insufficient: {
    message: "Authentication token lifetime is insufficient",
    retryable: false,
  },
  uninitialized: { message: "Authentication broker is not initialized", retryable: false },
});

function isPlainObject(value) {
  return (
    value !== null &&
    typeof value === "object" &&
    !Array.isArray(value) &&
    [Object.prototype, null].includes(Object.getPrototypeOf(value))
  );
}

function ownDataProperty(value, key) {
  try {
    if (value === null || !["function", "object"].includes(typeof value)) return undefined;
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    return descriptor && Object.hasOwn(descriptor, "value") ? descriptor.value : undefined;
  } catch {
    return undefined;
  }
}

function assertExactDataObject(value, keys, code = "invalid_credential") {
  if (!isPlainObject(value)) throw new AuthBrokerError(code);
  const actual = Reflect.ownKeys(value);
  if (
    actual.length !== keys.length ||
    !actual.every((key) => typeof key === "string" && keys.includes(key))
  ) {
    throw new AuthBrokerError(code);
  }
  for (const key of actual) {
    const descriptor = Object.getOwnPropertyDescriptor(value, key);
    if (!(descriptor?.enumerable === true && Object.hasOwn(descriptor, "value"))) {
      throw new AuthBrokerError(code);
    }
  }
}

function safeReason(value, fallback) {
  return SAFE_REASONS.has(value) ? value : fallback;
}

export class AuthBrokerError extends Error {
  constructor(code, { generation, reason, status } = {}) {
    const definition = ERROR_DEFINITIONS[code] ?? ERROR_DEFINITIONS.refresh_failed;
    const safeCode = Object.hasOwn(ERROR_DEFINITIONS, code) ? code : "refresh_failed";
    super(definition.message);
    this.name = "AuthBrokerError";
    this.code = safeCode;
    this.retryable = definition.retryable;
    if (isCanonicalGeneration(generation)) this.generation = generation;
    if (["ready", "reauth-required", "recovery-required"].includes(status)) {
      this.status = status;
    }
    if (SAFE_REASONS.has(reason)) this.reason = reason;
    Object.freeze(this);
  }
}

export function authBrokerErrorMetadata(error) {
  let isBrokerError = false;
  try {
    isBrokerError = error instanceof AuthBrokerError;
  } catch {
    // Hostile proxy objects are normalized to fixed public metadata.
  }
  if (!isBrokerError) {
    return { code: "refresh_failed", retryable: false };
  }
  const rawCode = ownDataProperty(error, "code");
  const code = Object.hasOwn(ERROR_DEFINITIONS, rawCode) ? rawCode : "refresh_failed";
  const metadata = { code, retryable: ERROR_DEFINITIONS[code].retryable };
  const generation = ownDataProperty(error, "generation");
  const status = ownDataProperty(error, "status");
  const reason = ownDataProperty(error, "reason");
  if (isCanonicalGeneration(generation)) {
    metadata.generation = generation;
  }
  if (["ready", "reauth-required", "recovery-required"].includes(status)) {
    metadata.status = status;
  }
  if (SAFE_REASONS.has(reason)) metadata.reason = reason;
  return metadata;
}

function parseCanonicalTimestamp(value) {
  if (typeof value !== "string") throw new AuthBrokerError("invalid_credential");
  const milliseconds = Date.parse(value);
  if (!Number.isFinite(milliseconds) || new Date(milliseconds).toISOString() !== value) {
    throw new AuthBrokerError("invalid_credential");
  }
  return milliseconds;
}

function decodeJwtPayload(token) {
  if (typeof token !== "string") throw new AuthBrokerError("invalid_credential");
  const parts = token.split(".");
  if (parts.length < 2) throw new AuthBrokerError("invalid_credential");
  try {
    const payload = JSON.parse(Buffer.from(parts[1], "base64url").toString("utf8"));
    if (!isPlainObject(payload)) throw new AuthBrokerError("invalid_credential");
    return payload;
  } catch {
    throw new AuthBrokerError("invalid_credential");
  }
}

function authClaims(payload) {
  const claims = payload["https://api.openai.com/auth"];
  return isPlainObject(claims) ? claims : {};
}

function matchingClaimValues(values, expected) {
  const present = values.filter((value) => value !== null && value !== undefined);
  return present.length > 0 && present.every((value) => value === expected);
}

function parseAuthJson(value, credential) {
  if (typeof value !== "string" || value.length === 0) {
    throw new AuthBrokerError("invalid_credential");
  }
  let parsed;
  try {
    parsed = JSON.parse(value);
  } catch {
    throw new AuthBrokerError("invalid_credential");
  }
  if (!isPlainObject(parsed) || !isPlainObject(parsed.tokens) || parsed.auth_mode !== "chatgpt") {
    throw new AuthBrokerError("invalid_credential");
  }
  if (
    parsed.tokens.access_token !== credential.accessToken ||
    typeof parsed.tokens.refresh_token !== "string" ||
    parsed.tokens.refresh_token.length === 0 ||
    typeof parsed.tokens.id_token !== "string" ||
    parsed.tokens.id_token.length === 0
  ) {
    throw new AuthBrokerError("invalid_credential");
  }
  const accessPayload = decodeJwtPayload(parsed.tokens.access_token);
  const idPayload = decodeJwtPayload(parsed.tokens.id_token);
  const accessAuth = authClaims(accessPayload);
  const idAuth = authClaims(idPayload);
  if (
    typeof accessAuth.chatgpt_account_id !== "string" ||
    accessAuth.chatgpt_account_id.length === 0 ||
    typeof accessAuth.chatgpt_user_id !== "string" ||
    accessAuth.chatgpt_user_id.length === 0 ||
    !matchingClaimValues(
      [parsed.tokens.account_id, accessAuth.chatgpt_account_id, idAuth.chatgpt_account_id],
      credential.accountId,
    ) ||
    !matchingClaimValues(
      [accessAuth.chatgpt_user_id, idAuth.chatgpt_user_id],
      credential.userId,
    )
  ) {
    throw new AuthBrokerError("invalid_credential");
  }
  const planClaims = [accessAuth.chatgpt_plan_type, idAuth.chatgpt_plan_type].filter(
    (planType) => planType !== null && planType !== undefined,
  );
  if (
    (credential.planType === null && planClaims.length !== 0) ||
    (credential.planType !== null &&
      (planClaims.length === 0 || planClaims.some((planType) => planType !== credential.planType)))
  ) {
    throw new AuthBrokerError("invalid_credential");
  }
  if (typeof accessPayload.exp !== "number" || !Number.isFinite(accessPayload.exp)) {
    throw new AuthBrokerError("invalid_credential");
  }
  const expiresAtMilliseconds = accessPayload.exp * 1000;
  const expiresAtDate = new Date(expiresAtMilliseconds);
  if (
    !Number.isFinite(expiresAtDate.getTime()) ||
    expiresAtDate.toISOString() !== credential.expiresAt
  ) {
    throw new AuthBrokerError("invalid_credential");
  }
  return parsed;
}

function validateReadyCredential(value) {
  assertExactDataObject(value, READY_CREDENTIAL_KEYS);
  for (const key of ["accessToken", "accountId", "userId"]) {
    if (typeof value[key] !== "string" || value[key].length === 0) {
      throw new AuthBrokerError("invalid_credential");
    }
  }
  if (!(value.planType === null || (typeof value.planType === "string" && value.planType.length > 0))) {
    throw new AuthBrokerError("invalid_credential");
  }
  parseCanonicalTimestamp(value.expiresAt);
  parseAuthJson(value.authJson, value);
  return {
    accessToken: value.accessToken,
    accountId: value.accountId,
    authJson: value.authJson,
    expiresAt: value.expiresAt,
    planType: value.planType,
    userId: value.userId,
  };
}

function credentialFields(value) {
  return {
    accessToken: value.accessToken,
    accountId: value.accountId,
    authJson: value.authJson,
    expiresAt: value.expiresAt,
    planType: value.planType,
    userId: value.userId,
  };
}

function readyPayload(credential) {
  return {
    schemaVersion: AUTH_PAYLOAD_SCHEMA_VERSION,
    status: "ready",
    ...validateReadyCredential(credential),
  };
}

function blockedPayload(status, reason) {
  const fallback =
    status === "reauth-required" ? "remote_reauth_required" : "adapter_post_dispatch_uncertain";
  const normalizedReason = safeReason(reason, fallback);
  return {
    schemaVersion: AUTH_PAYLOAD_SCHEMA_VERSION,
    status,
    reason: normalizedReason === "refresh_in_progress" ? fallback : normalizedReason,
  };
}

function accessTokenHash(accessToken) {
  return createHash("sha256").update(accessToken, "utf8").digest("hex");
}

function refreshTokenValue(credential) {
  return parseAuthJson(credential.authJson, credential).tokens.refresh_token;
}

function refreshTokenHash(credential) {
  return createHash("sha256").update(refreshTokenValue(credential), "utf8").digest("hex");
}

function refreshReservationPayload(credential, reservationId) {
  if (typeof reservationId !== "string" || !OPAQUE_ID_PATTERN.test(reservationId)) {
    throw new AuthBrokerError("store_unavailable");
  }
  return {
    schemaVersion: AUTH_PAYLOAD_SCHEMA_VERSION,
    status: "recovery-required",
    reason: "refresh_in_progress",
    reservationId,
    sourceAccessTokenHash: accessTokenHash(credential.accessToken),
    sourceRefreshTokenHash: refreshTokenHash(credential),
  };
}

function validateStoredPayload(payload) {
  if (!isPlainObject(payload) || payload.schemaVersion !== AUTH_PAYLOAD_SCHEMA_VERSION) {
    throw new AuthBrokerError("invalid_store_snapshot");
  }
  if (payload.status === "ready") {
    assertExactDataObject(
      payload,
      ["schemaVersion", "status", ...READY_CREDENTIAL_KEYS],
      "invalid_store_snapshot",
    );
    let credential;
    try {
      credential = validateReadyCredential(credentialFields(payload));
    } catch {
      throw new AuthBrokerError("invalid_store_snapshot");
    }
    return { schemaVersion: payload.schemaVersion, status: payload.status, ...credential };
  }
  if (["reauth-required", "recovery-required"].includes(payload.status)) {
    const isRefreshReservation =
      payload.status === "recovery-required" && payload.reason === "refresh_in_progress";
    assertExactDataObject(
      payload,
      isRefreshReservation
        ? [
            "reason",
            "reservationId",
            "schemaVersion",
            "sourceAccessTokenHash",
            "sourceRefreshTokenHash",
            "status",
          ]
        : ["reason", "schemaVersion", "status"],
      "invalid_store_snapshot",
    );
    if (!SAFE_REASONS.has(payload.reason)) {
      throw new AuthBrokerError("invalid_store_snapshot");
    }
    const blocked = {
      schemaVersion: payload.schemaVersion,
      status: payload.status,
      reason: payload.reason,
    };
    if (isRefreshReservation) {
      if (
        typeof payload.reservationId !== "string" ||
        !OPAQUE_ID_PATTERN.test(payload.reservationId) ||
        typeof payload.sourceAccessTokenHash !== "string" ||
        !SHA256_PATTERN.test(payload.sourceAccessTokenHash) ||
        typeof payload.sourceRefreshTokenHash !== "string" ||
        !SHA256_PATTERN.test(payload.sourceRefreshTokenHash)
      ) {
        throw new AuthBrokerError("invalid_store_snapshot");
      }
      blocked.reservationId = payload.reservationId;
      blocked.sourceAccessTokenHash = payload.sourceAccessTokenHash;
      blocked.sourceRefreshTokenHash = payload.sourceRefreshTokenHash;
    }
    return blocked;
  }
  throw new AuthBrokerError("invalid_store_snapshot");
}

function isCanonicalGeneration(value) {
  if (typeof value !== "string" || !GENERATION_PATTERN.test(value)) return false;
  return BigInt(value) <= MAX_GENERATION;
}

function validateGeneration(value) {
  if (!isCanonicalGeneration(value)) {
    throw new AuthBrokerError("invalid_store_snapshot");
  }
  return value;
}

function generationValue(value) {
  return BigInt(validateGeneration(value));
}

function serializeStoredPayload(payload) {
  return JSON.stringify(validateStoredPayload(payload));
}

function parseStoredPayload(rawPayload) {
  if (typeof rawPayload !== "string") {
    throw new AuthBrokerError("invalid_store_snapshot");
  }
  let parsed;
  try {
    parsed = JSON.parse(rawPayload);
  } catch {
    throw new AuthBrokerError("invalid_store_snapshot");
  }
  const canonical = validateStoredPayload(parsed);
  if (JSON.stringify(canonical) !== rawPayload) {
    throw new AuthBrokerError("invalid_store_snapshot");
  }
  return canonical;
}

function tokenTtlSeconds(payload, now) {
  return (parseCanonicalTimestamp(payload.expiresAt) - now()) / 1000;
}

function sensitiveGrant(generation, payload) {
  const grant = {
    expiresAt: payload.expiresAt,
    generation,
    planType: payload.planType,
  };
  for (const key of ["accessToken", "accountId"]) {
    Object.defineProperty(grant, key, {
      enumerable: false,
      value: payload[key],
    });
  }
  return Object.freeze(grant);
}

function validateCasResult(result, { commitId, expectedGeneration, payload }) {
  if (!isPlainObject(result)) throw new AuthBrokerError("invalid_store_snapshot");
  const generationDescriptor = Object.getOwnPropertyDescriptor(result, "generation");
  const commitIdDescriptor = Object.getOwnPropertyDescriptor(result, "commitId");
  const payloadDescriptor = Object.getOwnPropertyDescriptor(result, "payload");
  const replayedDescriptor = Object.getOwnPropertyDescriptor(result, "replayed");
  if (
    !Object.hasOwn(generationDescriptor ?? {}, "value") ||
    !Object.hasOwn(commitIdDescriptor ?? {}, "value") ||
    !Object.hasOwn(payloadDescriptor ?? {}, "value") ||
    !Object.hasOwn(replayedDescriptor ?? {}, "value") ||
    payloadDescriptor.enumerable !== false
  ) {
    throw new AuthBrokerError("invalid_store_snapshot");
  }
  const generation = validateGeneration(generationDescriptor.value);
  if (
    generationValue(generation) !== generationValue(expectedGeneration) + 1n ||
    commitIdDescriptor.value !== commitId ||
    payloadDescriptor.value !== payload ||
    typeof replayedDescriptor.value !== "boolean"
  ) {
    throw new AuthBrokerError("invalid_store_snapshot");
  }
  return generation;
}

function adapterFailureKind(error) {
  const code = ownDataProperty(error, "code");
  const postDispatch = ownDataProperty(error, "postDispatch");
  const preDispatch = ownDataProperty(error, "preDispatch");
  const reason = ownDataProperty(error, "reason");
  if (["reauth_required", "refresh_token_reused", "invalid_grant"].includes(code)) {
    return {
      code: "reauth_required",
      reason: safeReason(code, "remote_reauth_required"),
      status: "reauth-required",
    };
  }
  if (
    postDispatch === true ||
    ["recovery_required", "refresh_outcome_uncertain"].includes(code)
  ) {
    return {
      code: "recovery_required",
      reason: safeReason(reason, "adapter_post_dispatch_uncertain"),
      status: "recovery-required",
    };
  }
  if (preDispatch === true) {
    return { code: "refresh_failed", preDispatch: true };
  }
  return null;
}

function normalizedMinTtl(value, fallback) {
  const ttl = value ?? fallback;
  if (!(Number.isFinite(ttl) && ttl >= 0)) throw new AuthBrokerError("invalid_request");
  return ttl;
}

function callerMinTtl(value, floor) {
  return Math.max(normalizedMinTtl(value, floor), floor);
}

function sharedConfigurationMatches(entry, configuration) {
  return (
    entry.minTokenTtlSeconds === configuration.minTokenTtlSeconds &&
    entry.refreshAdapterIdentity === configuration.refreshAdapterIdentity
  );
}

function joinSharedRefresh(key, configuration) {
  const existing = SINGLEFLIGHT.get(key);
  if (!existing) return null;
  if (!sharedConfigurationMatches(existing, configuration)) {
    throw new AuthBrokerError("invalid_request");
  }
  return existing.task;
}

function sharedRefresh(key, configuration, operation) {
  const existing = joinSharedRefresh(key, configuration);
  if (existing) return existing;
  const task = Promise.resolve().then(operation);
  const entry = { ...configuration, task };
  SINGLEFLIGHT.set(key, entry);
  task.then(
    () => {
      if (SINGLEFLIGHT.get(key) === entry) SINGLEFLIGHT.delete(key);
    },
    () => {
      if (SINGLEFLIGHT.get(key) === entry) SINGLEFLIGHT.delete(key);
    },
  );
  return task;
}

function storeReadError(error) {
  const code = ownDataProperty(error, "code");
  if (["lock_release_failed", "recovery_required"].includes(code)) {
    return new AuthBrokerError("recovery_required");
  }
  if (INVALID_STORE_CODES.has(code)) return new AuthBrokerError("invalid_store_snapshot");
  return new AuthBrokerError("store_unavailable");
}

function storeCommitMayHaveSucceeded(error) {
  const code = ownDataProperty(error, "code");
  const commitState = ownDataProperty(error, "commitState");
  return (
    code === "commit_outcome_uncertain" ||
    commitState === "committed" ||
    commitState === "uncertain"
  );
}

export class AuthBroker {
  #workerAccessToken;
  #workerAccountId;
  #workerGeneration;

  constructor({
    minTokenTtlSeconds = DEFAULT_MIN_TOKEN_TTL_SECONDS,
    now = Date.now,
    randomUUID = systemRandomUUID,
    refreshAdapter,
    store,
  }) {
    if (
      !store ||
      typeof store.read !== "function" ||
      typeof store.compareAndSwap !== "function" ||
      store.coordinationKey === undefined ||
      store.coordinationKey === null
    ) {
      throw new AuthBrokerError("invalid_request");
    }
    const refreshAdapterIdentity = refreshAdapter;
    const adapter =
      typeof refreshAdapter === "function" ? refreshAdapter : refreshAdapter?.refresh?.bind(refreshAdapter);
    if (typeof adapter !== "function" || typeof now !== "function" || typeof randomUUID !== "function") {
      throw new AuthBrokerError("invalid_request");
    }
    this.store = store;
    this.coordinationKey = store.coordinationKey;
    this.refreshAdapter = adapter;
    this.refreshAdapterIdentity = refreshAdapterIdentity;
    this.minTokenTtlSeconds = normalizedMinTtl(minTokenTtlSeconds, DEFAULT_MIN_TOKEN_TTL_SECONDS);
    this.now = now;
    this.randomUUID = randomUUID;
    this.#workerAccessToken = null;
    this.#workerAccountId = null;
    this.#workerGeneration = null;
  }

  async #readCanonical() {
    let record;
    try {
      record = await this.store.read();
    } catch (error) {
      throw storeReadError(error);
    }
    if (record === null) return { generation: "0", payload: null };
    if (!isPlainObject(record)) {
      throw new AuthBrokerError("invalid_store_snapshot");
    }
    const generationDescriptor = Object.getOwnPropertyDescriptor(record, "generation");
    const commitIdDescriptor = Object.getOwnPropertyDescriptor(record, "commitId");
    const payloadDescriptor = Object.getOwnPropertyDescriptor(record, "payload");
    if (
      !Object.hasOwn(generationDescriptor ?? {}, "value") ||
      !Object.hasOwn(commitIdDescriptor ?? {}, "value") ||
      !Object.hasOwn(payloadDescriptor ?? {}, "value") ||
      payloadDescriptor.enumerable !== false ||
      typeof commitIdDescriptor.value !== "string" ||
      commitIdDescriptor.value.length === 0
    ) {
      throw new AuthBrokerError("invalid_store_snapshot");
    }
    const generation = validateGeneration(generationDescriptor.value);
    const rawPayload = payloadDescriptor.value;
    if (generation === "0") throw new AuthBrokerError("invalid_store_snapshot");
    return {
      commitId: commitIdDescriptor.value,
      generation,
      payload: parseStoredPayload(rawPayload),
      rawPayload,
    };
  }

  async #compareAndRead(
    expectedGeneration,
    payload,
    { commitId: requestedCommitId, postDispatch = false } = {},
  ) {
    let commitId = requestedCommitId;
    if (commitId === undefined) {
      try {
        commitId = this.randomUUID();
      } catch {
        throw new AuthBrokerError(
          postDispatch ? "refresh_outcome_uncertain" : "store_unavailable",
        );
      }
    }
    const serializedPayload = serializeStoredPayload(payload);
    const request = { commitId, expectedGeneration, payload: serializedPayload };
    let result;
    let mayHaveCommitted = false;
    for (let attempt = 0; attempt < 2; attempt += 1) {
      try {
        result = await this.store.compareAndSwap(request);
        break;
      } catch (error) {
        const code = ownDataProperty(error, "code");
        mayHaveCommitted ||= storeCommitMayHaveSucceeded(error);
        if (code === "generation_conflict") {
          if (!mayHaveCommitted) {
            throw new AuthBrokerError("cas_conflict", { generation: expectedGeneration });
          }
          break;
        }
        if (["lock_release_failed", "recovery_required"].includes(code)) {
          throw new AuthBrokerError("recovery_required");
        }
        if (INVALID_STORE_CODES.has(code)) {
          throw new AuthBrokerError("invalid_store_snapshot");
        }
        if (attempt === 1) {
          if (mayHaveCommitted) break;
          if (postDispatch) throw new AuthBrokerError("refresh_outcome_uncertain");
          throw new AuthBrokerError("store_unavailable");
        }
      }
    }
    let committedGeneration = null;
    if (result !== undefined) {
      committedGeneration = validateCasResult(result, {
        commitId,
        expectedGeneration,
        payload: serializedPayload,
      });
    }
    let canonical;
    try {
      canonical = await this.#readCanonical();
    } catch (error) {
      if (result !== undefined || mayHaveCommitted) {
        throw new AuthBrokerError("refresh_outcome_uncertain");
      }
      throw error;
    }
    if (
      result !== undefined &&
      canonical.payload !== null &&
      generationValue(canonical.generation) > generationValue(committedGeneration) &&
      canonical.rawPayload === serializedPayload
    ) {
      return canonical;
    }
    if (
      canonical.payload === null ||
      canonical.generation !== (committedGeneration ?? (generationValue(expectedGeneration) + 1n).toString()) ||
      canonical.commitId !== commitId ||
      canonical.rawPayload !== serializedPayload
    ) {
      if (mayHaveCommitted) throw new AuthBrokerError("refresh_outcome_uncertain");
      throw new AuthBrokerError("cas_conflict", { generation: canonical.generation });
    }
    return canonical;
  }

  #blockedError(canonical) {
    const code = canonical.payload.status === "reauth-required" ? "reauth_required" : "recovery_required";
    return new AuthBrokerError(code, {
      generation: canonical.generation,
      reason: canonical.payload.reason,
      status: canonical.payload.status,
    });
  }

  #grantFromCanonical(canonical) {
    if (canonical.payload === null) {
      throw new AuthBrokerError("uninitialized", { generation: canonical.generation });
    }
    if (canonical.payload.status !== "ready") throw this.#blockedError(canonical);
    return sensitiveGrant(canonical.generation, canonical.payload);
  }

  async installCredential(credential) {
    const before = await this.#readCanonical();
    if (
      before.payload?.status === "recovery-required" &&
      before.payload.reason === "refresh_in_progress"
    ) {
      throw this.#blockedError(before);
    }
    const canonical = await this.#compareAndRead(before.generation, readyPayload(credential));
    if (canonical.payload.status !== "ready") {
      throw new AuthBrokerError("invalid_store_snapshot");
    }
    return this.#publicSnapshot(canonical);
  }

  async recoverRefreshReservation(request) {
    assertExactDataObject(
      request,
      ["credential", "expectedGeneration", "reservationId"],
      "invalid_request",
    );
    if (
      !isCanonicalGeneration(request.expectedGeneration) ||
      typeof request.reservationId !== "string" ||
      !OPAQUE_ID_PATTERN.test(request.reservationId)
    ) {
      throw new AuthBrokerError("invalid_request");
    }
    const replacement = readyPayload(request.credential);
    const before = await this.#readCanonical();
    if (
      before.generation !== request.expectedGeneration ||
      before.payload?.status !== "recovery-required" ||
      before.payload.reason !== "refresh_in_progress" ||
      before.payload.reservationId !== request.reservationId
    ) {
      throw new AuthBrokerError("invalid_request");
    }
    if (accessTokenHash(replacement.accessToken) === before.payload.sourceAccessTokenHash) {
      throw new AuthBrokerError("invalid_credential", { reason: "access_token_unchanged" });
    }
    if (refreshTokenHash(replacement) === before.payload.sourceRefreshTokenHash) {
      throw new AuthBrokerError("invalid_credential", { reason: "refresh_token_reused" });
    }
    const canonical = await this.#compareAndRead(before.generation, replacement);
    return this.#publicSnapshot(canonical);
  }

  #publicSnapshot(canonical) {
    if (canonical.payload === null) {
      return Object.freeze({ generation: canonical.generation, status: "uninitialized" });
    }
    const snapshot = {
      generation: canonical.generation,
      schemaVersion: canonical.payload.schemaVersion,
      status: canonical.payload.status,
    };
    if (canonical.payload.status === "ready") snapshot.expiresAt = canonical.payload.expiresAt;
    else {
      snapshot.reason = canonical.payload.reason;
      if (canonical.payload.reason === "refresh_in_progress") {
        snapshot.reservationId = canonical.payload.reservationId;
      }
    }
    return Object.freeze(snapshot);
  }

  async snapshot() {
    return this.#publicSnapshot(await this.#readCanonical());
  }

  async getGrant({ minTtlSeconds } = {}) {
    const ttl = callerMinTtl(minTtlSeconds, this.minTokenTtlSeconds);
    let canonical = await this.#readCanonical();
    if (
      canonical.payload?.status === "recovery-required" &&
      canonical.payload.reason === "refresh_in_progress"
    ) {
      const joined = await this.#joinActiveRefresh(ttl);
      if (joined !== null) return joined;
      canonical = await this.#readCanonical();
    }
    if (canonical.payload === null || canonical.payload.status !== "ready") {
      return this.#grantFromCanonical(canonical);
    }
    if (tokenTtlSeconds(canonical.payload, this.now) >= ttl) {
      return this.#grantFromCanonical(canonical);
    }
    return this.#refreshShared(ttl, {
      expectedAccessToken: canonical.payload.accessToken,
      expectedAccountId: canonical.payload.accountId,
      expectedGeneration: canonical.generation,
    });
  }

  async refreshGrant({ minTtlSeconds } = {}) {
    const ttl = callerMinTtl(minTtlSeconds, this.minTokenTtlSeconds);
    let canonical = await this.#readCanonical();
    if (
      canonical.payload?.status === "recovery-required" &&
      canonical.payload.reason === "refresh_in_progress"
    ) {
      const reservation = canonical.payload;
      const joined = await this.#joinActiveRefresh(ttl);
      if (joined !== null) return joined;
      canonical = await this.#readCanonical();
      if (
        canonical.payload?.status === "ready" &&
        accessTokenHash(canonical.payload.accessToken) !== reservation.sourceAccessTokenHash
      ) {
        const concurrentGrant = this.#grantFromCanonical(canonical);
        if (tokenTtlSeconds(concurrentGrant, this.now) < ttl) {
          throw new AuthBrokerError("token_ttl_insufficient", {
            generation: concurrentGrant.generation,
            reason: "token_ttl_insufficient",
          });
        }
        return concurrentGrant;
      }
    }
    const current = this.#grantFromCanonical(canonical);
    return this.#refreshShared(ttl, {
      expectedAccessToken: current.accessToken,
      expectedAccountId: current.accountId,
      expectedGeneration: current.generation,
    });
  }

  async #refreshShared(
    minTtlSeconds,
    {
      expectedAccessToken = null,
      expectedAccountId = null,
      expectedGeneration = null,
    } = {},
  ) {
    for (let attempt = 0; attempt < MAX_SHARED_REFRESH_RECHECKS; attempt += 1) {
      const grant = await sharedRefresh(
        this.coordinationKey,
        {
          minTokenTtlSeconds: this.minTokenTtlSeconds,
          refreshAdapterIdentity: this.refreshAdapterIdentity,
        },
        () => this.#refreshOnce({ expectedAccessToken, expectedAccountId, expectedGeneration }),
      );
      if (expectedAccountId !== null && grant.accountId !== expectedAccountId) {
        throw new AuthBrokerError("invalid_request");
      }
      if (expectedAccessToken !== null && grant.accessToken === expectedAccessToken) {
        continue;
      }
      if (tokenTtlSeconds(grant, this.now) < minTtlSeconds) {
        throw new AuthBrokerError("token_ttl_insufficient", {
          generation: grant.generation,
          reason: "token_ttl_insufficient",
        });
      }
      return grant;
    }
    throw new AuthBrokerError("refresh_failed");
  }

  async #joinActiveRefresh(minTtlSeconds, expectedAccountId = null) {
    const task = joinSharedRefresh(this.coordinationKey, {
      minTokenTtlSeconds: this.minTokenTtlSeconds,
      refreshAdapterIdentity: this.refreshAdapterIdentity,
    });
    if (task === null) return null;
    const grant = await task;
    if (expectedAccountId !== null && grant.accountId !== expectedAccountId) {
      throw new AuthBrokerError("invalid_request");
    }
    if (tokenTtlSeconds(grant, this.now) < minTtlSeconds) {
      throw new AuthBrokerError("token_ttl_insufficient", {
        generation: grant.generation,
        reason: "token_ttl_insufficient",
      });
    }
    return grant;
  }

  async #reserveRefresh(before, reservationId) {
    const reservationPayload = refreshReservationPayload(before.payload, reservationId);
    const reservationRawPayload = serializeStoredPayload(reservationPayload);
    let current = before;
    for (let attempt = 0; attempt < MAX_STORAGE_ONLY_REBASES; attempt += 1) {
      let commitId;
      try {
        commitId = this.randomUUID();
      } catch {
        throw new AuthBrokerError("store_unavailable");
      }
      try {
        return await this.#compareAndRead(
          current.generation,
          reservationPayload,
          { commitId },
        );
      } catch (error) {
        if (
          !(error instanceof AuthBrokerError) ||
          !["cas_conflict", "refresh_outcome_uncertain"].includes(error.code)
        ) {
          throw error;
        }
        const latest = await this.#readCanonical();
        if (latest.commitId === commitId && latest.rawPayload === reservationRawPayload) {
          return latest;
        }
        if (latest.rawPayload !== before.rawPayload) throw error;
        current = latest;
      }
    }
    throw new AuthBrokerError("cas_conflict", { generation: current.generation });
  }

  async #commitAfterDispatch(before, payload) {
    const desiredRawPayload = serializeStoredPayload(payload);
    let current = before;
    for (let attempt = 0; attempt < MAX_STORAGE_ONLY_REBASES; attempt += 1) {
      try {
        return await this.#compareAndRead(
          current.generation,
          payload,
          { postDispatch: true },
        );
      } catch (error) {
        if (
          !(error instanceof AuthBrokerError) ||
          !["cas_conflict", "refresh_outcome_uncertain"].includes(error.code)
        ) {
          throw error;
        }
        let latest;
        try {
          latest = await this.#readCanonical();
        } catch {
          throw new AuthBrokerError("refresh_outcome_uncertain");
        }
        if (latest.rawPayload === desiredRawPayload) return latest;
        if (latest.rawPayload !== before.rawPayload) throw error;
        current = latest;
      }
    }
    throw new AuthBrokerError("refresh_outcome_uncertain");
  }

  async #transitionBlocked(before, status, reason) {
    let canonical;
    try {
      canonical = await this.#commitAfterDispatch(before, blockedPayload(status, reason));
    } catch (error) {
      if (error instanceof AuthBrokerError) throw error;
      throw new AuthBrokerError("store_unavailable");
    }
    if (canonical.payload.status !== status) {
      throw new AuthBrokerError("invalid_store_snapshot");
    }
    const code = status === "reauth-required" ? "reauth_required" : "recovery_required";
    throw new AuthBrokerError(code, {
      generation: canonical.generation,
      reason: canonical.payload.reason,
      status,
    });
  }

  #validateRefreshCandidate(previous, candidate, minTtlSeconds) {
    let credential;
    try {
      credential = validateReadyCredential(candidate);
    } catch {
      throw new AuthBrokerError("invalid_credential", {
        reason: "invalid_refresh_candidate",
      });
    }
    if (credential.accountId !== previous.accountId) {
      throw new AuthBrokerError("invalid_credential", { reason: "account_identity_changed" });
    }
    if (credential.userId !== previous.userId) {
      throw new AuthBrokerError("invalid_credential", { reason: "user_identity_changed" });
    }
    if (credential.accessToken === previous.accessToken) {
      throw new AuthBrokerError("invalid_credential", { reason: "access_token_unchanged" });
    }
    if (
      refreshTokenValue(credential) === refreshTokenValue(previous)
    ) {
      throw new AuthBrokerError("invalid_credential", { reason: "refresh_token_reused" });
    }
    if (tokenTtlSeconds(credential, this.now) < minTtlSeconds) {
      throw new AuthBrokerError("invalid_credential", { reason: "token_ttl_insufficient" });
    }
    return credential;
  }

  async #refreshOnce({ expectedAccessToken, expectedAccountId, expectedGeneration }) {
    let before = await this.#readCanonical();
    if (before.payload === null || before.payload.status !== "ready") {
      return this.#grantFromCanonical(before);
    }
    if (expectedAccountId !== null && before.payload.accountId !== expectedAccountId) {
      throw new AuthBrokerError("invalid_request");
    }
    if (expectedGeneration !== null) {
      if (typeof expectedAccessToken !== "string" || expectedAccessToken.length === 0) {
        throw new AuthBrokerError("invalid_request");
      }
      if (before.generation === expectedGeneration && before.payload.accessToken !== expectedAccessToken) {
        throw new AuthBrokerError("invalid_store_snapshot");
      }
      if (
        before.generation !== expectedGeneration &&
        before.payload.accessToken !== expectedAccessToken
      ) {
        return this.#grantFromCanonical(before);
      }
    }
    let attemptId;
    try {
      attemptId = this.randomUUID();
    } catch {
      throw new AuthBrokerError("store_unavailable");
    }
    const reservation = await this.#reserveRefresh(before, attemptId);
    let candidate;
    try {
      candidate = await this.refreshAdapter({
        credential: credentialFields(before.payload),
        expectedGeneration: reservation.generation,
        attemptId,
      });
    } catch (error) {
      const failure = adapterFailureKind(error);
      if (failure?.preDispatch) {
        await this.#commitAfterDispatch(
          reservation,
          readyPayload(credentialFields(before.payload)),
        );
        throw new AuthBrokerError("refresh_failed");
      }
      if (failure) {
        return this.#transitionBlocked(reservation, failure.status, failure.reason);
      }
      return this.#transitionBlocked(
        reservation,
        "recovery-required",
        "adapter_post_dispatch_uncertain",
      );
    }

    let credential;
    try {
      credential = this.#validateRefreshCandidate(
        before.payload,
        candidate,
        this.minTokenTtlSeconds,
      );
    } catch (error) {
      return this.#transitionBlocked(
        reservation,
        "recovery-required",
        safeReason(error.reason, "invalid_refresh_candidate"),
      );
    }
    const canonical = await this.#commitAfterDispatch(reservation, readyPayload(credential));
    return this.#grantFromCanonical(canonical);
  }

  async workerLoginParams(options) {
    const grant = await this.getGrant(options);
    this.#workerAccessToken = grant.accessToken;
    this.#workerAccountId = grant.accountId;
    this.#workerGeneration = grant.generation;
    return {
      accessToken: grant.accessToken,
      chatgptAccountId: grant.accountId,
      chatgptPlanType: grant.planType,
      type: "chatgptAuthTokens",
    };
  }

  async handleWorkerRefresh(params) {
    assertExactDataObject(params, ["previousAccountId", "reason"], "invalid_request");
    if (
      params.reason !== "unauthorized" ||
      typeof params.previousAccountId !== "string" ||
      params.previousAccountId.length === 0 ||
      this.#workerAccountId === null ||
      params.previousAccountId !== this.#workerAccountId
    ) {
      throw new AuthBrokerError("invalid_request");
    }
    let before = await this.#readCanonical();
    if (
      before.payload?.status === "recovery-required" &&
      before.payload.reason === "refresh_in_progress"
    ) {
      if (this.#workerGeneration === null || this.#workerAccessToken === null) {
        throw new AuthBrokerError("invalid_request");
      }
      const joined = await this.#joinActiveRefresh(
        this.minTokenTtlSeconds,
        params.previousAccountId,
      );
      if (joined !== null) {
        if (joined.accountId !== this.#workerAccountId) {
          throw new AuthBrokerError("invalid_request");
        }
        this.#workerAccessToken = joined.accessToken;
        this.#workerAccountId = joined.accountId;
        this.#workerGeneration = joined.generation;
        return {
          accessToken: joined.accessToken,
          chatgptAccountId: joined.accountId,
          chatgptPlanType: joined.planType,
        };
      }
      before = await this.#readCanonical();
    }
    const current = this.#grantFromCanonical(before);
    if (params.previousAccountId !== current.accountId) {
      throw new AuthBrokerError("invalid_request");
    }
    if (this.#workerGeneration === null || this.#workerAccessToken === null) {
      throw new AuthBrokerError("invalid_request");
    }
    const grant = await this.#refreshShared(this.minTokenTtlSeconds, {
      expectedAccessToken: this.#workerAccessToken,
      expectedAccountId: params.previousAccountId,
      expectedGeneration: this.#workerGeneration,
    });
    if (grant.accountId !== params.previousAccountId) {
      throw new AuthBrokerError("invalid_request");
    }
    this.#workerAccessToken = grant.accessToken;
    this.#workerAccountId = grant.accountId;
    this.#workerGeneration = grant.generation;
    return {
      accessToken: grant.accessToken,
      chatgptAccountId: grant.accountId,
      chatgptPlanType: grant.planType,
    };
  }
}
