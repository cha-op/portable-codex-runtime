import { Hash, createHash } from "node:crypto";
import { types as utilTypes } from "node:util";

import {
  STORAGE_CONTRACT_VERSION,
  STORAGE_FORCE_FENCE_RECONCILIATION_CONTRACT_VERSION,
  assertLeaseGrant,
  assertSessionAttachment,
  assertStorageBackendCapabilities,
  assertStorageForceFenceReconciliationResult,
  assertStorageForceFenceRequest,
  assertStorageForceFenceResult,
} from "./session-storage-contracts.mjs";
import {
  PODMAN_WRITER_VERIFIED_STOP_FENCE_CONTRACT_VERSION,
  getPodmanWriterVerifiedStopFenceController,
} from "./podman-writer-supervisor.mjs";
import { assertPodmanWriterSupervisorStateRecord } from "./podman-writer-supervisor-state.mjs";

const { isGeneratorFunction, isGeneratorObject, isPromise, isProxy } = utilTypes;
const arrayIncludesIntrinsic = Array.prototype.includes;
const arrayIsArrayIntrinsic = Array.isArray;
const createHashIntrinsic = createHash;
const hashDigestIntrinsic = Hash.prototype.digest;
const hashUpdateIntrinsic = Hash.prototype.update;
const jsonStringifyIntrinsic = JSON.stringify;
const JsonObject = JSON;
const objectCreateIntrinsic = Object.create;
const objectFreezeIntrinsic = Object.freeze;
const objectGetOwnPropertyDescriptorIntrinsic = Object.getOwnPropertyDescriptor;
const objectGetPrototypeOfIntrinsic = Object.getPrototypeOf;
const objectHasOwnIntrinsic = Object.hasOwn;
const objectIsIntrinsic = Object.is;
const objectPrototype = Object.prototype;
const PromiseConstructor = Promise;
const reflectApplyIntrinsic = Reflect.apply;
const reflectOwnKeysIntrinsic = Reflect.ownKeys;
const regexpExecIntrinsic = RegExp.prototype.exec;
const stringSliceIntrinsic = String.prototype.slice;
const TypeErrorConstructor = TypeError;

export const PODMAN_EXT4_VERIFIED_STOP_FENCE_PROVIDER_CONTRACT_VERSION = 1;

const MAX_BACKEND_PROTOTYPE_DEPTH = 64;
const FULL_CONTAINER_ID_PATTERN = /^[0-9a-f]{64}$/u;
const OPAQUE_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u;
const STATE_OWNER_ID_PATTERN = /^state-owner:[0-9a-f]{64}$/u;
const PROCESS_INCARNATION_ID_PATTERN = /^podman-process:[0-9a-f]{64}$/u;
const WRITER_INCARNATION_ID_PATTERN = /^podman-writer:[0-9a-f]{64}$/u;
const BASE_METHOD_KEYS = objectFreezeIntrinsic([
  "captureCheckpoint",
  "destroySession",
  "detachAttachment",
  "forceFence",
  "prepareWritableAttachment",
  "provisionSession",
  "restoreCheckpoint",
]);
const CAPABILITY_KEYS = objectFreezeIntrinsic([
  "atomicPointInTimeCheckpoint",
  "exclusiveWriterAttachment",
  "fencing",
  "normalDirectoryAttachment",
]);
const OPTION_KEYS = objectFreezeIntrinsic([
  "baseBackend",
  "resolveFenceBinding",
  "supervisor",
  "supervisorStateCollector",
]);
const RESOLUTION_KEYS = objectFreezeIntrinsic(["binding", "signal"]);
const BINDING_KEYS = objectFreezeIntrinsic([
  "contractVersion",
  "launch",
  "request",
  "result",
  "stateOwnerId",
]);
const WRITER_REQUEST_KEYS = objectFreezeIntrinsic([
  "attachment",
  "contractVersion",
  "fencingEpoch",
  "generation",
  "lease",
  "measuredImage",
  "supervisor",
]);
const SUPERVISOR_IDENTITY_KEYS = objectFreezeIntrinsic([
  "contractVersion",
  "supervisorId",
]);
const LAUNCH_POINTER_KEYS = objectFreezeIntrinsic([
  "attachmentId",
  "attachmentSha256",
  "contractVersion",
  "fencingEpoch",
  "generation",
  "launchAttemptId",
  "launchResultSha256",
  "leaseId",
  "leaseSha256",
  "measuredImageSha256",
  "processIncarnationId",
  "startedAt",
  "supervisorId",
  "supervisorProofId",
  "writerIncarnationId",
]);
const TERMINAL_RESULT_KEYS = objectFreezeIntrinsic([
  "evidence",
  "outcome",
  "resultVersion",
]);
const TERMINAL_EVIDENCE_KEYS = objectFreezeIntrinsic([
  "contractVersion",
  "launchAttemptId",
  "processIncarnationId",
  "proofId",
  "status",
  "supervisorId",
  "writerIncarnationId",
]);

const ERROR_MESSAGES = objectFreezeIntrinsic({
  invalid_podman_ext4_verified_stop_fence_options:
    "Podman ext4 verified-stop fence options are invalid",
  invalid_podman_ext4_verified_stop_fence_request:
    "Podman ext4 verified-stop fence request is invalid",
  podman_ext4_verified_stop_fence_outcome_uncertain:
    "Podman ext4 verified-stop fence outcome is uncertain",
});

export class PodmanExt4VerifiedStopFenceProviderError extends Error {
  constructor(code) {
    if (typeof code !== "string" || !objectHasOwnIntrinsic(ERROR_MESSAGES, code)) {
      throw new TypeErrorConstructor(
        "unsupported Podman ext4 verified-stop fence error code",
      );
    }
    super(ERROR_MESSAGES[code]);
    this.name = "PodmanExt4VerifiedStopFenceProviderError";
    this.code = code;
    this.retryable = false;
    objectFreezeIntrinsic(this);
  }
}

function fail(code) {
  throw new PodmanExt4VerifiedStopFenceProviderError(code);
}

function ensure(condition, code) {
  if (!condition) fail(code);
}

function callIntrinsic(intrinsic, receiver, args) {
  return reflectApplyIntrinsic(intrinsic, receiver, args);
}

function regexpTest(pattern, value) {
  return callIntrinsic(regexpExecIntrinsic, pattern, [value]) !== null;
}

function exactFrozenRecord(values) {
  const result = objectCreateIntrinsic(null);
  const keys = reflectOwnKeysIntrinsic(values);
  for (let index = 0; index < keys.length; index += 1) {
    const key = keys[index];
    result[key] = values[key];
  }
  return objectFreezeIntrinsic(result);
}

function inspectExactDataObject(value, keys, code) {
  ensure(
    value !== null &&
      typeof value === "object" &&
      !arrayIsArrayIntrinsic(value) &&
      !isProxy(value),
    code,
  );
  let prototype;
  let actual;
  try {
    prototype = objectGetPrototypeOfIntrinsic(value);
    actual = reflectOwnKeysIntrinsic(value);
  } catch {
    fail(code);
  }
  ensure(
    (prototype === objectPrototype || prototype === null) &&
      actual.length === keys.length,
    code,
  );
  const result = objectCreateIntrinsic(null);
  for (let index = 0; index < actual.length; index += 1) {
    const key = actual[index];
    ensure(
      typeof key === "string" &&
        callIntrinsic(arrayIncludesIntrinsic, keys, [key]) &&
        !objectHasOwnIntrinsic(result, key),
      code,
    );
    let descriptor;
    try {
      descriptor = objectGetOwnPropertyDescriptorIntrinsic(value, key);
    } catch {
      fail(code);
    }
    ensure(
      descriptor?.enumerable === true &&
        objectHasOwnIntrinsic(descriptor, "value"),
      code,
    );
    result[key] = descriptor.value;
  }
  return result;
}

function validatePrototypeChain(value, code) {
  ensure(
    value !== null &&
      typeof value === "object" &&
      !arrayIsArrayIntrinsic(value) &&
      !isProxy(value),
    code,
  );
  let cursor = value;
  for (let depth = 0; depth < MAX_BACKEND_PROTOTYPE_DEPTH; depth += 1) {
    ensure(!isProxy(cursor), code);
    let next;
    try {
      next = objectGetPrototypeOfIntrinsic(cursor);
    } catch {
      fail(code);
    }
    if (cursor === objectPrototype) {
      ensure(next === null, code);
      return;
    }
    ensure(!(depth > 0 && next === null), code);
    if (next === null) return;
    cursor = next;
  }
  fail(code);
}

function dataValueFromChain(value, key, code) {
  let cursor = value;
  for (let depth = 0; depth < MAX_BACKEND_PROTOTYPE_DEPTH; depth += 1) {
    ensure(cursor !== objectPrototype && !isProxy(cursor), code);
    let descriptor;
    let next;
    try {
      descriptor = objectGetOwnPropertyDescriptorIntrinsic(cursor, key);
      next = objectGetPrototypeOfIntrinsic(cursor);
    } catch {
      fail(code);
    }
    if (descriptor !== undefined) {
      ensure(objectHasOwnIntrinsic(descriptor, "value"), code);
      return descriptor.value;
    }
    if (next === null) break;
    cursor = next;
  }
  fail(code);
}

function trustedFunction(value, code) {
  ensure(
    typeof value === "function" && !isProxy(value) && !isGeneratorFunction(value),
    code,
  );
  return value;
}

function isSafeNativePromise(value) {
  if (!isPromise(value) || isProxy(value) || isGeneratorObject(value)) return false;
  let cursor = value;
  for (let depth = 0; cursor !== null && depth < 8; depth += 1) {
    if (isProxy(cursor)) return false;
    let descriptor;
    try {
      descriptor = objectGetOwnPropertyDescriptorIntrinsic(cursor, "constructor");
    } catch {
      return false;
    }
    if (descriptor !== undefined) {
      return (
        objectHasOwnIntrinsic(descriptor, "value") &&
        descriptor.value === PromiseConstructor
      );
    }
    try {
      cursor = objectGetPrototypeOfIntrinsic(cursor);
    } catch {
      return false;
    }
  }
  return false;
}

function captureBaseBackend(value, code) {
  validatePrototypeChain(value, code);
  const backendId = dataValueFromChain(value, "backendId", code);
  const contractVersion = dataValueFromChain(value, "contractVersion", code);
  ensure(
    contractVersion === STORAGE_CONTRACT_VERSION &&
      typeof backendId === "string" &&
      regexpTest(OPAQUE_ID_PATTERN, backendId),
    code,
  );
  const capabilitiesValue = dataValueFromChain(value, "capabilities", code);
  let capabilities;
  try {
    capabilities = assertStorageBackendCapabilities(capabilitiesValue);
  } catch {
    fail(code);
  }
  const methods = objectCreateIntrinsic(null);
  for (let index = 0; index < BASE_METHOD_KEYS.length; index += 1) {
    const name = BASE_METHOD_KEYS[index];
    methods[name] = trustedFunction(dataValueFromChain(value, name, code), code);
  }
  return exactFrozenRecord({
    backend: value,
    backendId,
    capabilities,
    contractVersion,
    methods: objectFreezeIntrinsic(methods),
  });
}

function normalizeRequest(value, backendId) {
  let request;
  try {
    request = assertStorageForceFenceRequest(value);
  } catch {
    fail("invalid_podman_ext4_verified_stop_fence_request");
  }
  ensure(
    request.backendId === backendId,
    "invalid_podman_ext4_verified_stop_fence_request",
  );
  return request;
}

function normalizeBinding(value) {
  const code = "invalid_podman_ext4_verified_stop_fence_request";
  const binding = inspectExactDataObject(value, BINDING_KEYS, code);
  const request = inspectExactDataObject(
    binding.request,
    WRITER_REQUEST_KEYS,
    code,
  );
  const supervisor = inspectExactDataObject(
    request.supervisor,
    SUPERVISOR_IDENTITY_KEYS,
    code,
  );
  const launch = inspectExactDataObject(
    binding.launch,
    LAUNCH_POINTER_KEYS,
    code,
  );
  const result = inspectExactDataObject(
    binding.result,
    TERMINAL_RESULT_KEYS,
    code,
  );
  const evidence = inspectExactDataObject(
    result.evidence,
    TERMINAL_EVIDENCE_KEYS,
    code,
  );
  let attachment;
  let lease;
  try {
    attachment = assertSessionAttachment(request.attachment);
    lease = assertLeaseGrant(request.lease);
  } catch {
    fail(code);
  }
  const containerId = typeof launch.processIncarnationId === "string"
    ? callIntrinsic(stringSliceIntrinsic, launch.processIncarnationId, [
        "podman-process:".length,
      ])
    : null;
  ensure(
    binding.contractVersion ===
        PODMAN_EXT4_VERIFIED_STOP_FENCE_PROVIDER_CONTRACT_VERSION &&
      typeof binding.stateOwnerId === "string" &&
      regexpTest(STATE_OWNER_ID_PATTERN, binding.stateOwnerId) &&
      request.contractVersion === 1 &&
      request.fencingEpoch === lease.fencingEpoch &&
      supervisor.contractVersion === 1 &&
      typeof supervisor.supervisorId === "string" &&
      regexpTest(OPAQUE_ID_PATTERN, supervisor.supervisorId) &&
      launch.contractVersion === 1 &&
      typeof launch.launchAttemptId === "string" &&
      regexpTest(OPAQUE_ID_PATTERN, launch.launchAttemptId) &&
      typeof launch.processIncarnationId === "string" &&
      regexpTest(PROCESS_INCARNATION_ID_PATTERN, launch.processIncarnationId) &&
      typeof containerId === "string" &&
      regexpTest(FULL_CONTAINER_ID_PATTERN, containerId) &&
      typeof launch.writerIncarnationId === "string" &&
      regexpTest(WRITER_INCARNATION_ID_PATTERN, launch.writerIncarnationId) &&
      launch.attachmentId === attachment.attachmentId &&
      launch.fencingEpoch === lease.fencingEpoch &&
      launch.leaseId === lease.leaseId &&
      launch.supervisorId === supervisor.supervisorId &&
      result.outcome === "writer-launch-started" &&
      result.resultVersion === 1 &&
      evidence.contractVersion === 1 &&
      evidence.launchAttemptId === launch.launchAttemptId &&
      evidence.processIncarnationId === launch.processIncarnationId &&
      evidence.status === "started" &&
      evidence.supervisorId === supervisor.supervisorId &&
      evidence.writerIncarnationId === launch.writerIncarnationId,
    code,
  );
  const controllerBinding = exactFrozenRecord({
    contractVersion: binding.contractVersion,
    launch: exactFrozenRecord(launch),
    request: exactFrozenRecord({
      ...request,
      attachment,
      lease,
      supervisor: exactFrozenRecord(supervisor),
    }),
    result: exactFrozenRecord({
      ...result,
      evidence: exactFrozenRecord(evidence),
    }),
    stateOwnerId: binding.stateOwnerId,
  });
  return exactFrozenRecord({
    attachment,
    containerId,
    controllerBinding,
    contractVersion: binding.contractVersion,
    launchAttemptId: launch.launchAttemptId,
    lease,
    processIncarnationId: launch.processIncarnationId,
    stateOwnerId: binding.stateOwnerId,
    supervisorId: supervisor.supervisorId,
    writerIncarnationId: launch.writerIncarnationId,
  });
}

export function assertPodmanExt4VerifiedStopFenceBinding(value) {
  return normalizeBinding(value).controllerBinding;
}

function normalizeResolution(value) {
  const code = "invalid_podman_ext4_verified_stop_fence_request";
  const resolution = inspectExactDataObject(value, RESOLUTION_KEYS, code);
  return exactFrozenRecord({
    binding: normalizeBinding(resolution.binding),
    signal: resolution.signal,
  });
}

function requestMatchesBinding(request, binding, controller) {
  return (
    binding.supervisorId === controller.supervisorId &&
    binding.stateOwnerId === controller.stateOwnerId &&
    binding.attachment.backendId === request.backendId &&
    binding.attachment.storageId === request.storageId &&
    binding.attachment.sessionId === request.sessionId &&
    binding.attachment.attachmentId === request.target.attachmentId &&
    binding.attachment.fencingEpoch === request.revokedFence.fencingEpoch &&
    binding.attachment.holderId === request.revokedFence.holderId &&
    binding.attachment.leaseId === request.revokedFence.leaseId &&
    binding.lease.sessionId === request.sessionId &&
    binding.lease.fencingEpoch === request.revokedFence.fencingEpoch &&
    binding.lease.holderId === request.revokedFence.holderId &&
    binding.lease.leaseId === request.revokedFence.leaseId
  );
}

function terminalRecordMatches(recordValue, request, binding) {
  let record;
  try {
    record = assertPodmanWriterSupervisorStateRecord(recordValue);
  } catch {
    return null;
  }
  return record.status === "stopped" &&
      record.revision === 4 &&
      record.stopOperationId === request.operationId &&
      record.launchAttemptId === binding.launchAttemptId &&
      record.containerId === binding.containerId &&
      record.processIncarnationId === binding.processIncarnationId &&
      record.writerIncarnationId === binding.writerIncarnationId
    ? record
    : null;
}

function proofId(request, binding, terminalRecord) {
  const requestJson = callIntrinsic(jsonStringifyIntrinsic, JsonObject, [{
    backendId: request.backendId,
    contractVersion: request.contractVersion,
    fencingEpoch: request.fencingEpoch,
    operationId: request.operationId,
    revokedFence: {
      fencingEpoch: request.revokedFence.fencingEpoch,
      holderId: request.revokedFence.holderId,
      leaseId: request.revokedFence.leaseId,
    },
    sessionId: request.sessionId,
    storageId: request.storageId,
    target: {
      attachmentId: request.target.attachmentId,
      kind: request.target.kind,
    },
  }]);
  const terminalIdentityJson = callIntrinsic(jsonStringifyIntrinsic, JsonObject, [{
    containerId: terminalRecord.containerId,
    containerName: terminalRecord.containerName,
    contractVersion: terminalRecord.contractVersion,
    launchAttemptId: terminalRecord.launchAttemptId,
    processIncarnationId: terminalRecord.processIncarnationId,
    proofId: terminalRecord.proofId,
    requestSha256: terminalRecord.requestSha256,
    revision: terminalRecord.revision,
    stateOwnerId: binding.stateOwnerId,
    status: terminalRecord.status,
    stopOperationId: terminalRecord.stopOperationId,
    stopProofId: terminalRecord.stopProofId,
    supervisorId: binding.supervisorId,
    writerIncarnationId: terminalRecord.writerIncarnationId,
  }]);
  const hash = createHashIntrinsic("sha256");
  callIntrinsic(hashUpdateIntrinsic, hash, [
    "portable-codex-runtime:podman-ext4-verified-stop-fence:v1\0",
    "utf8",
  ]);
  callIntrinsic(hashUpdateIntrinsic, hash, [requestJson, "utf8"]);
  callIntrinsic(hashUpdateIntrinsic, hash, ["\0", "utf8"]);
  callIntrinsic(hashUpdateIntrinsic, hash, [terminalIdentityJson, "utf8"]);
  return `podman-ext4-fence:${callIntrinsic(hashDigestIntrinsic, hash, ["hex"])}`;
}

function committedResult(request, binding, terminalRecord) {
  return assertStorageForceFenceResult(
    exactFrozenRecord({
      backendId: request.backendId,
      contractVersion: STORAGE_CONTRACT_VERSION,
      fencingEpoch: request.fencingEpoch,
      operationId: request.operationId,
      proofId: proofId(request, binding, terminalRecord),
      revokedFence: request.revokedFence,
      sessionId: request.sessionId,
      status: "fenced",
      storageId: request.storageId,
      target: request.target,
    }),
    { request },
  );
}

function unknownReconciliation(request) {
  return assertStorageForceFenceReconciliationResult(
    exactFrozenRecord({
      contractVersion: STORAGE_FORCE_FENCE_RECONCILIATION_CONTRACT_VERSION,
      outcome: "unknown",
      result: null,
    }),
    { request },
  );
}

/**
 * Adds a dormant, host-local verified-stop fence to one trusted ext4 backend.
 * The protected property is that the exact (supervisor, state owner, launch,
 * container, process incarnation, writer incarnation) tuple reached the rev4
 * stopped tombstone and that both its anchored name and full-ID inventories
 * are empty after removal. The proof makes no statement about remote writers,
 * block-device isolation, controller/drive caches, or cache loss.
 */
export function createPodmanExt4VerifiedStopFenceProvider(options) {
  const optionCode = "invalid_podman_ext4_verified_stop_fence_options";
  const values = inspectExactDataObject(options, OPTION_KEYS, optionCode);
  const base = captureBaseBackend(values.baseBackend, optionCode);
  ensure(base.capabilities.fencing === "manual", optionCode);
  const resolveFenceBinding = trustedFunction(
    values.resolveFenceBinding,
    optionCode,
  );
  let controller;
  try {
    controller = getPodmanWriterVerifiedStopFenceController(
      values.supervisor,
      values.supervisorStateCollector,
    );
  } catch {
    fail(optionCode);
  }
  ensure(
    controller.contractVersion ===
      PODMAN_WRITER_VERIFIED_STOP_FENCE_CONTRACT_VERSION,
    optionCode,
  );

  const capabilitiesValue = objectCreateIntrinsic(null);
  for (let index = 0; index < CAPABILITY_KEYS.length; index += 1) {
    const key = CAPABILITY_KEYS[index];
    capabilitiesValue[key] =
      key === "fencing" ? "verified-detach" : base.capabilities[key];
  }
  const capabilities = objectFreezeIntrinsic(capabilitiesValue);
  try {
    assertStorageBackendCapabilities(capabilities);
  } catch {
    fail(optionCode);
  }

  let provider;
  const delegated = (name) => {
    const operation = base.methods[name];
    const callback = function podmanExt4VerifiedStopLifecycleMethod(...args) {
      if (!objectIsIntrinsic(this, provider)) {
        throw new TypeErrorConstructor(
          "Invalid Podman ext4 verified-stop fence provider receiver",
        );
      }
      return callIntrinsic(operation, base.backend, args);
    };
    return objectFreezeIntrinsic(callback);
  };

  async function resolve(request) {
    let pending;
    try {
      pending = callIntrinsic(resolveFenceBinding, undefined, [request]);
    } catch {
      fail("podman_ext4_verified_stop_fence_outcome_uncertain");
    }
    ensure(
      isSafeNativePromise(pending),
      "podman_ext4_verified_stop_fence_outcome_uncertain",
    );
    let resolution;
    try {
      resolution = normalizeResolution(await pending);
    } catch {
      fail("podman_ext4_verified_stop_fence_outcome_uncertain");
    }
    ensure(
      requestMatchesBinding(request, resolution.binding, controller),
      "podman_ext4_verified_stop_fence_outcome_uncertain",
    );
    return resolution;
  }

  const forceFence = objectFreezeIntrinsic(async function forceFence(value) {
    ensure(
      arguments.length === 1 && objectIsIntrinsic(this, provider),
      "invalid_podman_ext4_verified_stop_fence_request",
    );
    const request = normalizeRequest(value, base.backendId);
    const resolution = await resolve(request);
    let stopped;
    try {
      stopped = await callIntrinsic(
        controller.dispatchVerifiedStopFence,
        controller,
        [exactFrozenRecord({
          binding: resolution.binding.controllerBinding,
          contractVersion: PODMAN_WRITER_VERIFIED_STOP_FENCE_CONTRACT_VERSION,
          signal: resolution.signal,
          stopOperationId: request.operationId,
        })],
      );
    } catch {
      fail("podman_ext4_verified_stop_fence_outcome_uncertain");
    }
    const terminalRecord = terminalRecordMatches(
      stopped?.terminalRecord,
      request,
      resolution.binding,
    );
    ensure(
      stopped?.contractVersion ===
          PODMAN_WRITER_VERIFIED_STOP_FENCE_CONTRACT_VERSION &&
        stopped?.outcome === "stopped" &&
        terminalRecord !== null,
      "podman_ext4_verified_stop_fence_outcome_uncertain",
    );
    return committedResult(request, resolution.binding, terminalRecord);
  });

  const reconcileForceFence = objectFreezeIntrinsic(
    async function reconcileForceFence(value) {
      ensure(
        arguments.length === 1 && objectIsIntrinsic(this, provider),
        "invalid_podman_ext4_verified_stop_fence_request",
      );
      const request = normalizeRequest(value, base.backendId);
      let resolution;
      try {
        resolution = await resolve(request);
      } catch {
        return unknownReconciliation(request);
      }
      let stopped;
      try {
        stopped = await callIntrinsic(
          controller.reconcileVerifiedStopFence,
          controller,
          [exactFrozenRecord({
            binding: resolution.binding.controllerBinding,
            contractVersion: PODMAN_WRITER_VERIFIED_STOP_FENCE_CONTRACT_VERSION,
            signal: resolution.signal,
            stopOperationId: request.operationId,
          })],
        );
      } catch {
        return unknownReconciliation(request);
      }
      if (
        stopped?.contractVersion !==
            PODMAN_WRITER_VERIFIED_STOP_FENCE_CONTRACT_VERSION ||
        stopped?.outcome !== "stopped"
      ) {
        return unknownReconciliation(request);
      }
      const terminalRecord = terminalRecordMatches(
        stopped.terminalRecord,
        request,
        resolution.binding,
      );
      if (terminalRecord === null) return unknownReconciliation(request);
      return assertStorageForceFenceReconciliationResult(
        exactFrozenRecord({
          contractVersion: STORAGE_FORCE_FENCE_RECONCILIATION_CONTRACT_VERSION,
          outcome: "committed",
          result: committedResult(request, resolution.binding, terminalRecord),
        }),
        { request },
      );
    },
  );

  provider = objectCreateIntrinsic(null);
  provider.backendId = base.backendId;
  provider.capabilities = capabilities;
  provider.contractVersion = base.contractVersion;
  provider.forceFence = forceFence;
  provider.forceFenceReconciliationContractVersion =
    STORAGE_FORCE_FENCE_RECONCILIATION_CONTRACT_VERSION;
  provider.reconcileForceFence = reconcileForceFence;
  for (let index = 0; index < BASE_METHOD_KEYS.length; index += 1) {
    const name = BASE_METHOD_KEYS[index];
    if (name !== "forceFence") provider[name] = delegated(name);
  }
  return objectFreezeIntrinsic(provider);
}

objectFreezeIntrinsic(PodmanExt4VerifiedStopFenceProviderError.prototype);
objectFreezeIntrinsic(PodmanExt4VerifiedStopFenceProviderError);
objectFreezeIntrinsic(assertPodmanExt4VerifiedStopFenceBinding);
objectFreezeIntrinsic(createPodmanExt4VerifiedStopFenceProvider);
