import { Buffer } from "node:buffer";
import { createHash } from "node:crypto";
import { createReadStream } from "node:fs";
import { isAbsolute, parse, resolve } from "node:path";
import { types as utilTypes } from "node:util";

import {
  ATOMIC_CRASH_CAPTURE_CONTRACT_VERSION,
  STORAGE_CONTRACT_VERSION,
  assertAtomicCrashCaptureRequest,
  assertAtomicCrashCaptureResult,
  assertStorageBackendCapabilities,
} from "./session-storage-contracts.mjs";

const arrayIncludesIntrinsic = Array.prototype.includes;
const arrayEveryIntrinsic = Array.prototype.every;
const arrayFilterIntrinsic = Array.prototype.filter;
const arrayIsArrayIntrinsic = Array.isArray;
const arrayPushIntrinsic = Array.prototype.push;
const arraySliceIntrinsic = Array.prototype.slice;
const bigIntToStringIntrinsic = BigInt.prototype.toString;
const bufferByteLengthIntrinsic = Buffer.byteLength;
const bufferIsBufferIntrinsic = Buffer.isBuffer;
const bufferToStringIntrinsic = Buffer.prototype.toString;
const hashDigestIntrinsic = Object.getPrototypeOf(createHash("sha256")).digest;
const hashUpdateIntrinsic = Object.getPrototypeOf(createHash("sha256")).update;
const objectCreateIntrinsic = Object.create;
const objectFreezeIntrinsic = Object.freeze;
const objectGetOwnPropertyDescriptorIntrinsic = Object.getOwnPropertyDescriptor;
const objectGetPrototypeOfIntrinsic = Object.getPrototypeOf;
const objectHasOwnIntrinsic = Object.hasOwn;
const objectIsIntrinsic = Object.is;
const objectPrototype = Object.prototype;
const pathIsAbsoluteIntrinsic = isAbsolute;
const pathParseIntrinsic = parse;
const pathResolveIntrinsic = resolve;
const PromiseConstructor = Promise;
const reflectApplyIntrinsic = Reflect.apply;
const reflectOwnKeysIntrinsic = Reflect.ownKeys;
const regexpExecIntrinsic = RegExp.prototype.exec;
const stringEndsWithIntrinsic = String.prototype.endsWith;
const stringIncludesIntrinsic = String.prototype.includes;
const stringJoinIntrinsic = Array.prototype.join;
const stringSliceIntrinsic = String.prototype.slice;
const stringSplitIntrinsic = String.prototype.split;
const stringTrimIntrinsic = String.prototype.trim;
const TypeErrorConstructor = TypeError;
const {
  isGeneratorFunction: isGeneratorFunctionValue,
  isGeneratorObject: isGeneratorObjectValue,
  isPromise: isPromiseValue,
  isProxy: isProxyValue,
} = utilTypes;

export const LVM_ATOMIC_CRASH_CAPTURE_PROVIDER_CONTRACT_VERSION = 1;
export const LVM_ATOMIC_CRASH_CAPTURE_DRIVER_CONTRACT_VERSION = 1;
export const LVM_ATOMIC_CRASH_CAPTURE_BINDING_KIND =
  "lvm-classic-snapshot-v1";

const UINT64_MAX = 18_446_744_073_709_551_615n;
const MAX_BACKEND_PROTOTYPE_DEPTH = 64;
const MAX_COMMAND_OUTPUT_BYTES = 1024 * 1024;
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
const PROVIDER_OPTION_KEYS = objectFreezeIntrinsic([
  "authorityConsumer",
  "baseBackend",
  "catalogue",
  "driver",
]);
const CATALOGUE_METHOD_KEYS = objectFreezeIntrinsic([
  "claimStarting",
  "commitResult",
  "markUncertain",
  "readCommitted",
]);
const DRIVER_METHOD_KEYS = objectFreezeIntrinsic([
  "captureSnapshot",
  "resolveProviderBinding",
  "verifySnapshot",
]);
const PROVIDER_BINDING_KEYS = objectFreezeIntrinsic([
  "bindingKind",
  "contractVersion",
  "originLvUuid",
  "snapshotName",
  "snapshotSizeBytes",
  "snapshotTag",
]);
const DRIVER_OPTION_KEYS = objectFreezeIntrinsic([
  "blockdevExecutable",
  "commandRunner",
  "createSnapshotReadStream",
  "dmsetupExecutable",
  "lvcreateExecutable",
  "lvsExecutable",
  "resolveOrigin",
]);
const ORIGIN_RESOLUTION_KEYS = objectFreezeIntrinsic([
  "originLvUuid",
  "snapshotSizeBytes",
]);
const LVS_ROW_KEYS = objectFreezeIntrinsic([
  "lv_attr",
  "lv_dm_path",
  "lv_name",
  "lv_path",
  "lv_size",
  "lv_tags",
  "lv_uuid",
  "origin_uuid",
  "snap_percent",
]);
const DEFAULT_EXECUTABLES = objectFreezeIntrinsic({
  blockdev: "/usr/sbin/blockdev",
  dmsetup: "/usr/sbin/dmsetup",
  lvcreate: "/usr/sbin/lvcreate",
  lvs: "/usr/sbin/lvs",
});
const LVM_UUID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9-]{5,127}$/u;
const LVM_NAME_PATTERN = /^[A-Za-z0-9][A-Za-z0-9+_.-]{0,126}$/u;
const LVM_TAG_PATTERN = /^[A-Za-z0-9][A-Za-z0-9+_.-]{0,126}$/u;
const POSITIVE_DECIMAL_PATTERN = /^[1-9][0-9]*$/u;
const SHA256_PATTERN = /^[0-9a-f]{64}$/u;
const DM_UUID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9+_.:-]{0,255}$/u;
const MAJOR_MINOR_PATTERN = /^(?:0|[1-9][0-9]*):(?:0|[1-9][0-9]*)$/u;
const DECIMAL_PERCENT_PATTERN = /^(?:0|[1-9][0-9]{0,2})(?:\.[0-9]+)?$/u;

const ERROR_MESSAGES = objectFreezeIntrinsic({
  invalid_lvm_atomic_crash_capture_options:
    "LVM atomic crash-capture options are invalid",
  invalid_lvm_atomic_crash_capture_request:
    "LVM atomic crash-capture request is invalid",
  lvm_atomic_crash_capture_outcome_uncertain:
    "LVM atomic crash-capture outcome is uncertain",
});

export class LvmAtomicCrashCaptureProviderError extends Error {
  constructor(code) {
    if (typeof code !== "string" || !objectHasOwnIntrinsic(ERROR_MESSAGES, code)) {
      throw new TypeErrorConstructor(
        "unsupported LVM atomic crash-capture error code",
      );
    }
    super(ERROR_MESSAGES[code]);
    this.name = "LvmAtomicCrashCaptureProviderError";
    this.code = code;
    this.retryable = false;
    objectFreezeIntrinsic(this);
  }
}

function fail(code) {
  throw new LvmAtomicCrashCaptureProviderError(code);
}

function ensure(condition, code) {
  if (!condition) fail(code);
}

function callIntrinsic(intrinsic, receiver, args) {
  return reflectApplyIntrinsic(intrinsic, receiver, args);
}

function arrayIncludes(values, candidate) {
  return callIntrinsic(arrayIncludesIntrinsic, values, [candidate]);
}

function arrayEvery(values, callback) {
  return callIntrinsic(arrayEveryIntrinsic, values, [callback]);
}

function arrayFilter(values, callback) {
  return callIntrinsic(arrayFilterIntrinsic, values, [callback]);
}

function arrayPush(values, candidate) {
  return callIntrinsic(arrayPushIntrinsic, values, [candidate]);
}

function stringSlice(value, start, end) {
  return callIntrinsic(stringSliceIntrinsic, value, [start, end]);
}

function regexpTest(pattern, value) {
  return callIntrinsic(regexpExecIntrinsic, pattern, [value]) !== null;
}

function stringTrim(value) {
  return callIntrinsic(stringTrimIntrinsic, value, []);
}

function stringIncludes(value, candidate) {
  return callIntrinsic(stringIncludesIntrinsic, value, [candidate]);
}

function stringEndsWith(value, suffix) {
  return callIntrinsic(stringEndsWithIntrinsic, value, [suffix]);
}

function stripHyphens(value) {
  return callIntrinsic(
    stringJoinIntrinsic,
    callIntrinsic(stringSplitIntrinsic, value, ["-"]),
    [""],
  );
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
      !isProxyValue(value),
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
    ensure(typeof key === "string" && arrayIncludes(keys, key), code);
    let descriptor;
    try {
      descriptor = objectGetOwnPropertyDescriptorIntrinsic(value, key);
    } catch {
      fail(code);
    }
    ensure(
      descriptor?.enumerable === true &&
        objectHasOwnIntrinsic(descriptor, "value") &&
        !objectHasOwnIntrinsic(result, key),
      code,
    );
    result[key] = descriptor.value;
  }
  return result;
}

function inspectExactDataObjectVariant(value, variants, code) {
  ensure(
    value !== null &&
      typeof value === "object" &&
      !arrayIsArrayIntrinsic(value) &&
      !isProxyValue(value),
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
  ensure(prototype === objectPrototype || prototype === null, code);
  let selected = null;
  for (let index = 0; index < variants.length; index += 1) {
    const keys = variants[index];
    if (
      actual.length === keys.length &&
      arrayEvery(
        actual,
        (key) => typeof key === "string" && arrayIncludes(keys, key),
      )
    ) {
      selected = keys;
      break;
    }
  }
  ensure(selected !== null, code);
  return inspectExactDataObject(value, selected, code);
}

function validatePrototypeChain(value, code) {
  ensure(
    value !== null &&
      typeof value === "object" &&
      !arrayIsArrayIntrinsic(value) &&
      !isProxyValue(value),
    code,
  );
  let cursor = value;
  for (let depth = 0; depth < MAX_BACKEND_PROTOTYPE_DEPTH; depth += 1) {
    ensure(!isProxyValue(cursor), code);
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
    ensure(cursor !== objectPrototype && !isProxyValue(cursor), code);
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
    typeof value === "function" &&
      !isProxyValue(value) &&
      !isGeneratorFunctionValue(value),
    code,
  );
  return value;
}

function captureSurface(value, version, methods, code) {
  validatePrototypeChain(value, code);
  ensure(dataValueFromChain(value, "contractVersion", code) === version, code);
  const captured = objectCreateIntrinsic(null);
  for (let index = 0; index < methods.length; index += 1) {
    const method = methods[index];
    captured[method] = trustedFunction(
      dataValueFromChain(value, method, code),
      code,
    );
  }
  return objectFreezeIntrinsic(captured);
}

function isSafeNativePromise(value) {
  if (
    !isPromiseValue(value) ||
    isProxyValue(value) ||
    isGeneratorObjectValue(value)
  ) {
    return false;
  }
  let cursor = value;
  for (let depth = 0; cursor !== null && depth < 8; depth += 1) {
    if (isProxyValue(cursor)) return false;
    let descriptor;
    try {
      descriptor = objectGetOwnPropertyDescriptorIntrinsic(
        cursor,
        "constructor",
      );
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

async function observePromiseRejection(promise) {
  try {
    await promise;
  } catch {
    // The owning operation reports only its fixed uncertainty outcome.
  }
}

function invokeNativePromise(receiver, method, input, code) {
  let pending;
  try {
    pending = callIntrinsic(method, receiver, [input]);
  } catch {
    fail(code);
  }
  ensure(isSafeNativePromise(pending), code);
  return pending;
}

function positiveUint64Decimal(value, code) {
  ensure(
    typeof value === "string" && regexpTest(POSITIVE_DECIMAL_PATTERN, value),
    code,
  );
  let parsed;
  try {
    parsed = BigInt(value);
  } catch {
    fail(code);
  }
  ensure(parsed > 0n && parsed <= UINT64_MAX, code);
  return value;
}

function opaqueLvmUuid(value, code) {
  ensure(typeof value === "string" && regexpTest(LVM_UUID_PATTERN, value), code);
  return value;
}

export function assertLvmAtomicCrashCaptureProviderBinding(value) {
  const code = "invalid_lvm_atomic_crash_capture_request";
  const binding = inspectExactDataObject(value, PROVIDER_BINDING_KEYS, code);
  ensure(
    binding.bindingKind === LVM_ATOMIC_CRASH_CAPTURE_BINDING_KIND &&
      binding.contractVersion ===
        LVM_ATOMIC_CRASH_CAPTURE_PROVIDER_CONTRACT_VERSION &&
      typeof binding.snapshotName === "string" &&
      regexpTest(LVM_NAME_PATTERN, binding.snapshotName) &&
      typeof binding.snapshotTag === "string" &&
      regexpTest(LVM_TAG_PATTERN, binding.snapshotTag),
    code,
  );
  return exactFrozenRecord({
    bindingKind: binding.bindingKind,
    contractVersion: binding.contractVersion,
    originLvUuid: opaqueLvmUuid(binding.originLvUuid, code),
    snapshotName: binding.snapshotName,
    snapshotSizeBytes: positiveUint64Decimal(
      binding.snapshotSizeBytes,
      code,
    ),
    snapshotTag: binding.snapshotTag,
  });
}

function sameProviderBinding(left, right) {
  return (
    left.bindingKind === right.bindingKind &&
    left.contractVersion === right.contractVersion &&
    left.originLvUuid === right.originLvUuid &&
    left.snapshotName === right.snapshotName &&
    left.snapshotSizeBytes === right.snapshotSizeBytes &&
    left.snapshotTag === right.snapshotTag
  );
}

function normalizeRequest(value) {
  try {
    return assertAtomicCrashCaptureRequest(value);
  } catch {
    fail("invalid_lvm_atomic_crash_capture_request");
  }
}

function normalizeCaptureInput(value) {
  const code = "invalid_lvm_atomic_crash_capture_request";
  const input = inspectExactDataObject(
    value,
    objectFreezeIntrinsic(["captureAuthority", "request"]),
    code,
  );
  ensure(
    input.captureAuthority !== null &&
      typeof input.captureAuthority === "object" &&
      !arrayIsArrayIntrinsic(input.captureAuthority) &&
      !isProxyValue(input.captureAuthority),
    code,
  );
  return exactFrozenRecord({
    captureAuthority: input.captureAuthority,
    request: normalizeRequest(input.request),
  });
}

function normalizeCatalogueClaim(value, request) {
  const code = "lvm_atomic_crash_capture_outcome_uncertain";
  const record = inspectExactDataObjectVariant(
    value,
    [
      objectFreezeIntrinsic(["dispatchClaim", "outcome"]),
      objectFreezeIntrinsic(["outcome"]),
      objectFreezeIntrinsic([
        "outcome",
        "providerBinding",
        "result",
      ]),
    ],
    code,
  );
  if (record.outcome === "dispatch") {
    ensure(
      record.dispatchClaim !== null &&
        typeof record.dispatchClaim === "object" &&
        !isProxyValue(record.dispatchClaim),
      code,
    );
    return exactFrozenRecord({
      dispatchClaim: record.dispatchClaim,
      outcome: "dispatch",
    });
  }
  if (record.outcome === "unknown") {
    return exactFrozenRecord({ dispatchClaim: null, outcome: "unknown" });
  }
  ensure(record.outcome === "committed", code);
  let result;
  try {
    result = assertAtomicCrashCaptureResult(record.result, { request });
  } catch {
    fail(code);
  }
  return exactFrozenRecord({
    dispatchClaim: null,
    outcome: "committed",
    providerBinding: assertLvmAtomicCrashCaptureProviderBinding(
      record.providerBinding,
    ),
    result,
  });
}

function normalizeCommittedRead(value, request) {
  const code = "lvm_atomic_crash_capture_outcome_uncertain";
  const record = inspectExactDataObjectVariant(
    value,
    [
      objectFreezeIntrinsic(["outcome"]),
      objectFreezeIntrinsic(["outcome", "providerBinding", "result"]),
    ],
    code,
  );
  if (record.outcome === "unknown") {
    return exactFrozenRecord({ outcome: "unknown" });
  }
  ensure(record.outcome === "committed", code);
  let result;
  try {
    result = assertAtomicCrashCaptureResult(record.result, { request });
  } catch {
    fail(code);
  }
  return exactFrozenRecord({
    outcome: "committed",
    providerBinding: assertLvmAtomicCrashCaptureProviderBinding(
      record.providerBinding,
    ),
    result,
  });
}

function normalizeCommit(value, request, providerBinding, result) {
  const code = "lvm_atomic_crash_capture_outcome_uncertain";
  const record = inspectExactDataObject(
    value,
    objectFreezeIntrinsic(["outcome", "providerBinding", "result"]),
    code,
  );
  ensure(
    record.outcome === "committed",
    code,
  );
  const committedBinding = assertLvmAtomicCrashCaptureProviderBinding(
    record.providerBinding,
  );
  ensure(sameProviderBinding(providerBinding, committedBinding), code);
  try {
    return assertAtomicCrashCaptureResult(record.result, {
      previousResult: result,
      request,
    });
  } catch {
    fail(code);
  }
}

function captureBaseBackend(value) {
  const code = "invalid_lvm_atomic_crash_capture_options";
  validatePrototypeChain(value, code);
  const contractVersion = dataValueFromChain(value, "contractVersion", code);
  ensure(contractVersion === STORAGE_CONTRACT_VERSION, code);
  const backendId = dataValueFromChain(value, "backendId", code);
  ensure(
    typeof backendId === "string" &&
      backendId.length >= 1 &&
      backendId.length <= 128 &&
      regexpTest(/^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u, backendId),
    code,
  );
  let capabilities;
  try {
    capabilities = assertStorageBackendCapabilities(
      dataValueFromChain(value, "capabilities", code),
    );
  } catch {
    fail(code);
  }
  const methods = objectCreateIntrinsic(null);
  for (let index = 0; index < BASE_METHOD_KEYS.length; index += 1) {
    const name = BASE_METHOD_KEYS[index];
    methods[name] = trustedFunction(dataValueFromChain(value, name, code), code);
  }
  return objectFreezeIntrinsic({
    backend: value,
    backendId,
    capabilities,
    contractVersion,
    methods: objectFreezeIntrinsic(methods),
  });
}

async function runAuthorityConsumer(
  authorityConsumer,
  captureAuthority,
  request,
  driver,
  driverMethods,
  providerBinding,
) {
  const code = "lvm_atomic_crash_capture_outcome_uncertain";
  let callbackCalls = 0;
  let callbackCompleted = false;
  let callbackResult;
  let open = true;
  const runCapture = async (...args) => {
    callbackCalls += 1;
    ensure(open && callbackCalls === 1 && args.length === 0, code);
    const pending = invokeNativePromise(
      driver,
      driverMethods.captureSnapshot,
      exactFrozenRecord({ providerBinding, request }),
      code,
    );
    void observePromiseRejection(pending);
    const raw = await pending;
    ensure(open && callbackCalls === 1, code);
    try {
      callbackResult = assertAtomicCrashCaptureResult(raw, { request });
    } catch {
      fail(code);
    }
    callbackCompleted = true;
    return callbackResult;
  };
  objectFreezeIntrinsic(runCapture);
  const admission = exactFrozenRecord({ captureAuthority, request });
  let pending;
  try {
    pending = callIntrinsic(authorityConsumer, undefined, [
      admission,
      runCapture,
    ]);
  } catch {
    open = false;
    fail(code);
  }
  ensure(isSafeNativePromise(pending), code);
  void observePromiseRejection(pending);
  try {
    const result = await pending;
    open = false;
    ensure(
      callbackCalls === 1 &&
        callbackCompleted &&
        objectIsIntrinsic(result, callbackResult),
      code,
    );
    return callbackResult;
  } catch {
    open = false;
    fail(code);
  }
}

async function bestEffortMarkUncertain(
  catalogue,
  catalogueMethods,
  dispatchClaim,
) {
  try {
    const pending = invokeNativePromise(
      catalogue,
      catalogueMethods.markUncertain,
      exactFrozenRecord({ dispatchClaim }),
      "lvm_atomic_crash_capture_outcome_uncertain",
    );
    void observePromiseRejection(pending);
    const raw = await pending;
    const record = inspectExactDataObject(
      raw,
      objectFreezeIntrinsic(["outcome"]),
      "lvm_atomic_crash_capture_outcome_uncertain",
    );
    ensure(
      record.outcome === "uncertain",
      "lvm_atomic_crash_capture_outcome_uncertain",
    );
  } catch {
    // The original dispatch is already terminally uncertain.
  }
}

async function resolveBinding(driver, driverMethods, request) {
  const code = "lvm_atomic_crash_capture_outcome_uncertain";
  const pending = invokeNativePromise(
    driver,
    driverMethods.resolveProviderBinding,
    exactFrozenRecord({ request }),
    code,
  );
  void observePromiseRejection(pending);
  try {
    return assertLvmAtomicCrashCaptureProviderBinding(await pending);
  } catch {
    fail(code);
  }
}

async function physicallyVerify(
  driver,
  driverMethods,
  providerBinding,
  result,
) {
  const code = "lvm_atomic_crash_capture_outcome_uncertain";
  let pending;
  try {
    pending = invokeNativePromise(
      driver,
      driverMethods.verifySnapshot,
      exactFrozenRecord({ providerBinding, result }),
      code,
    );
  } catch {
    return false;
  }
  void observePromiseRejection(pending);
  try {
    return (await pending) === true;
  } catch {
    return false;
  }
}

/**
 * Adds a dormant atomic crash-capture extension to one trusted lifecycle
 * backend. Nothing in this module wires that extension into public capture.
 */
export function createLvmAtomicCrashCaptureProvider(options) {
  const code = "invalid_lvm_atomic_crash_capture_options";
  const values = inspectExactDataObject(options, PROVIDER_OPTION_KEYS, code);
  const authorityConsumer = trustedFunction(values.authorityConsumer, code);
  const base = captureBaseBackend(values.baseBackend);
  const catalogueMethods = captureSurface(
    values.catalogue,
    LVM_ATOMIC_CRASH_CAPTURE_PROVIDER_CONTRACT_VERSION,
    CATALOGUE_METHOD_KEYS,
    code,
  );
  const driverMethods = captureSurface(
    values.driver,
    LVM_ATOMIC_CRASH_CAPTURE_DRIVER_CONTRACT_VERSION,
    DRIVER_METHOD_KEYS,
    code,
  );
  const catalogue = values.catalogue;
  const driver = values.driver;

  const capabilities = objectCreateIntrinsic(null);
  for (let index = 0; index < CAPABILITY_KEYS.length; index += 1) {
    const key = CAPABILITY_KEYS[index];
    capabilities[key] =
      key === "atomicPointInTimeCheckpoint" ? true : base.capabilities[key];
  }
  objectFreezeIntrinsic(capabilities);

  let provider;
  const delegated = (name) => {
    const operation = base.methods[name];
    const callback = function lvmAtomicCrashCaptureLifecycleMethod(...args) {
      if (!objectIsIntrinsic(this, provider)) {
        throw new TypeErrorConstructor(
          "Invalid LVM atomic crash-capture provider receiver",
        );
      }
      return callIntrinsic(operation, base.backend, args);
    };
    return objectFreezeIntrinsic(callback);
  };

  const captureAtomicCrashCheckpoint = async function captureAtomicCrashCheckpoint(
    input,
  ) {
    ensure(
      objectIsIntrinsic(this, provider),
      "invalid_lvm_atomic_crash_capture_request",
    );
    const normalized = normalizeCaptureInput(input);
    ensure(
      normalized.request.storageRef.backendId === base.backendId,
      "invalid_lvm_atomic_crash_capture_request",
    );
    const providerBinding = await resolveBinding(
      driver,
      driverMethods,
      normalized.request,
    );
    const claimPending = invokeNativePromise(
      catalogue,
      catalogueMethods.claimStarting,
      exactFrozenRecord({ providerBinding, request: normalized.request }),
      "lvm_atomic_crash_capture_outcome_uncertain",
    );
    void observePromiseRejection(claimPending);
    let claim;
    try {
      claim = normalizeCatalogueClaim(await claimPending, normalized.request);
    } catch {
      fail("lvm_atomic_crash_capture_outcome_uncertain");
    }
    if (claim.outcome === "unknown") {
      fail("lvm_atomic_crash_capture_outcome_uncertain");
    }
    if (claim.outcome === "committed") {
      ensure(
        sameProviderBinding(providerBinding, claim.providerBinding),
        "lvm_atomic_crash_capture_outcome_uncertain",
      );
      ensure(
        await physicallyVerify(
          driver,
          driverMethods,
          claim.providerBinding,
          claim.result,
        ),
        "lvm_atomic_crash_capture_outcome_uncertain",
      );
      return claim.result;
    }

    let result;
    try {
      result = await runAuthorityConsumer(
        authorityConsumer,
        normalized.captureAuthority,
        normalized.request,
        driver,
        driverMethods,
        providerBinding,
      );
    } catch {
      await bestEffortMarkUncertain(
        catalogue,
        catalogueMethods,
        claim.dispatchClaim,
      );
      fail("lvm_atomic_crash_capture_outcome_uncertain");
    }

    // From this point onward a failed acknowledgement may hide a committed
    // catalogue row. Never rewrite it to uncertain and never redispatch.
    const commitPending = invokeNativePromise(
      catalogue,
      catalogueMethods.commitResult,
      exactFrozenRecord({ dispatchClaim: claim.dispatchClaim, result }),
      "lvm_atomic_crash_capture_outcome_uncertain",
    );
    void observePromiseRejection(commitPending);
    try {
      return normalizeCommit(
        await commitPending,
        normalized.request,
        providerBinding,
        result,
      );
    } catch {
      fail("lvm_atomic_crash_capture_outcome_uncertain");
    }
  };
  objectFreezeIntrinsic(captureAtomicCrashCheckpoint);

  const verifyCommittedAtomicCrashCheckpoint = async function verifyCommitted(
    request,
  ) {
    ensure(
      objectIsIntrinsic(this, provider),
      "invalid_lvm_atomic_crash_capture_request",
    );
    const normalized = normalizeRequest(request);
    ensure(
      normalized.storageRef.backendId === base.backendId,
      "invalid_lvm_atomic_crash_capture_request",
    );
    const unknown = exactFrozenRecord({
      contractVersion: ATOMIC_CRASH_CAPTURE_CONTRACT_VERSION,
      outcome: "unknown",
      result: null,
    });
    let pending;
    try {
      pending = invokeNativePromise(
        catalogue,
        catalogueMethods.readCommitted,
        exactFrozenRecord({ request: normalized }),
        "lvm_atomic_crash_capture_outcome_uncertain",
      );
    } catch {
      return unknown;
    }
    void observePromiseRejection(pending);
    let committed;
    try {
      committed = normalizeCommittedRead(await pending, normalized);
    } catch {
      return unknown;
    }
    if (committed.outcome === "unknown") return unknown;
    if (
      !(await physicallyVerify(
        driver,
        driverMethods,
        committed.providerBinding,
        committed.result,
      ))
    ) {
      return unknown;
    }
    return exactFrozenRecord({
      contractVersion: ATOMIC_CRASH_CAPTURE_CONTRACT_VERSION,
      outcome: "committed",
      result: committed.result,
    });
  };
  objectFreezeIntrinsic(verifyCommittedAtomicCrashCheckpoint);

  provider = objectCreateIntrinsic(null);
  provider.atomicCrashCaptureContractVersion =
    ATOMIC_CRASH_CAPTURE_CONTRACT_VERSION;
  provider.backendId = base.backendId;
  provider.capabilities = capabilities;
  provider.captureAtomicCrashCheckpoint = captureAtomicCrashCheckpoint;
  provider.contractVersion = base.contractVersion;
  provider.verifyCommittedAtomicCrashCheckpoint =
    verifyCommittedAtomicCrashCheckpoint;
  for (let index = 0; index < BASE_METHOD_KEYS.length; index += 1) {
    const name = BASE_METHOD_KEYS[index];
    provider[name] = delegated(name);
  }
  return objectFreezeIntrinsic(provider);
}

function executablePath(value, code) {
  ensure(
    typeof value === "string" &&
      value.length > 1 &&
      value.length <= 4095 &&
      pathIsAbsoluteIntrinsic(value) &&
      pathResolveIntrinsic(value) === value &&
      value !== pathParseIntrinsic(value).root &&
      !stringIncludes(value, "\u0000"),
    code,
  );
  return value;
}

function commandStdout(value, code) {
  const completion = inspectExactDataObject(
    value,
    objectFreezeIntrinsic(["stderr", "stdout"]),
    code,
  );
  ensure(
    typeof completion.stderr === "string" ||
      bufferIsBufferIntrinsic(completion.stderr),
    code,
  );
  let stdout;
  if (typeof completion.stdout === "string") {
    stdout = completion.stdout;
  } else {
    ensure(bufferIsBufferIntrinsic(completion.stdout), code);
    stdout = callIntrinsic(bufferToStringIntrinsic, completion.stdout, ["utf8"]);
  }
  ensure(
    bufferByteLengthIntrinsic(stdout, "utf8") <= MAX_COMMAND_OUTPUT_BYTES,
    code,
  );
  return stdout;
}

function driverOptions(value) {
  const code = "invalid_lvm_atomic_crash_capture_options";
  ensure(
    value !== null &&
      typeof value === "object" &&
      !arrayIsArrayIntrinsic(value) &&
      !isProxyValue(value),
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
  ensure(prototype === objectPrototype || prototype === null, code);
  const result = objectCreateIntrinsic(null);
  for (let index = 0; index < actual.length; index += 1) {
    const key = actual[index];
    ensure(
      typeof key === "string" &&
        arrayIncludes(DRIVER_OPTION_KEYS, key) &&
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
  ensure(
    objectHasOwnIntrinsic(result, "commandRunner") &&
      objectHasOwnIntrinsic(result, "resolveOrigin"),
    code,
  );
  return result;
}

function deterministicBinding(request, origin) {
  const hash = createHash("sha256");
  const components = [
    request.storageRef.backendId,
    request.storageRef.storageId,
    request.storageRef.sessionId,
    request.captureAttemptId,
    request.checkpoint.checkpointId,
    request.checkpoint.artifactId,
    request.mutationRequest.operationId,
    request.checkpoint.sourceFencingEpoch,
    origin.originLvUuid,
    origin.snapshotSizeBytes,
  ];
  for (let index = 0; index < components.length; index += 1) {
    const component = components[index];
    callIntrinsic(hashUpdateIntrinsic, hash, [
      `${bufferByteLengthIntrinsic(component, "utf8")}:`,
      "utf8",
    ]);
    callIntrinsic(hashUpdateIntrinsic, hash, [component, "utf8"]);
  }
  const digest = callIntrinsic(hashDigestIntrinsic, hash, ["hex"]);
  return assertLvmAtomicCrashCaptureProviderBinding({
    bindingKind: LVM_ATOMIC_CRASH_CAPTURE_BINDING_KIND,
    contractVersion: LVM_ATOMIC_CRASH_CAPTURE_PROVIDER_CONTRACT_VERSION,
    originLvUuid: origin.originLvUuid,
    snapshotName: `pcr-${stringSlice(digest, 0, 48)}`,
    snapshotSizeBytes: origin.snapshotSizeBytes,
    snapshotTag: `pcr.atomic.${stringSlice(digest, 0, 48)}`,
  });
}

function parseOriginResolution(value) {
  const code = "lvm_atomic_crash_capture_outcome_uncertain";
  const origin = inspectExactDataObject(value, ORIGIN_RESOLUTION_KEYS, code);
  return exactFrozenRecord({
    originLvUuid: opaqueLvmUuid(origin.originLvUuid, code),
    snapshotSizeBytes: positiveUint64Decimal(
      origin.snapshotSizeBytes,
      code,
    ),
  });
}

function parseLvsRows(stdout) {
  const code = "lvm_atomic_crash_capture_outcome_uncertain";
  let parsed;
  try {
    parsed = JSON.parse(stdout);
  } catch {
    fail(code);
  }
  const top = inspectExactDataObject(
    parsed,
    objectFreezeIntrinsic(["report"]),
    code,
  );
  ensure(arrayIsArrayIntrinsic(top.report) && top.report.length === 1, code);
  const report = inspectExactDataObject(
    top.report[0],
    objectFreezeIntrinsic(["lv"]),
    code,
  );
  ensure(arrayIsArrayIntrinsic(report.lv), code);
  const rows = [];
  for (let index = 0; index < report.lv.length; index += 1) {
    const row = inspectExactDataObject(report.lv[index], LVS_ROW_KEYS, code);
    const normalized = objectCreateIntrinsic(null);
    for (let keyIndex = 0; keyIndex < LVS_ROW_KEYS.length; keyIndex += 1) {
      const key = LVS_ROW_KEYS[keyIndex];
      ensure(typeof row[key] === "string", code);
      normalized[key] = stringTrim(row[key]);
    }
    arrayPush(rows, exactFrozenRecord(normalized));
  }
  return objectFreezeIntrinsic(rows);
}

function parseLvmSize(value, code) {
  const canonical = stringTrim(value);
  return positiveUint64Decimal(canonical, code);
}

function validateDevicePath(value, code) {
  ensure(
    typeof value === "string" &&
      value.length > 5 &&
      value.length <= 4095 &&
      pathIsAbsoluteIntrinsic(value) &&
      pathResolveIntrinsic(value) === value &&
      value !== pathParseIntrinsic(value).root &&
      !stringIncludes(value, "\u0000"),
    code,
  );
  return value;
}

function lvmTags(value) {
  const parts = callIntrinsic(stringSplitIntrinsic, value, [","]);
  const tags = [];
  for (let index = 0; index < parts.length; index += 1) {
    const tag = stringTrim(parts[index]);
    if (tag.length > 0) arrayPush(tags, tag);
  }
  return tags;
}

function parsePercentBelowFull(value, code) {
  const canonical = stringTrim(value);
  ensure(regexpTest(DECIMAL_PERCENT_PATTERN, canonical), code);
  const numeric = Number(canonical);
  ensure(Number.isFinite(numeric) && numeric >= 0 && numeric < 100, code);
  return canonical;
}

/**
 * Creates the fixed-command LVM classic-snapshot driver. The caller injects
 * the command runner and origin resolver; all executable paths are absolute,
 * all argv vectors are exact, and no shell or ambient PATH is used.
 */
export function createLvmAtomicCrashCaptureDriver(options) {
  const optionCode = "invalid_lvm_atomic_crash_capture_options";
  const values = driverOptions(options);
  const commandRunner = trustedFunction(values.commandRunner, optionCode);
  const resolveOrigin = trustedFunction(values.resolveOrigin, optionCode);
  const openSnapshot =
    values.createSnapshotReadStream === undefined
      ? createReadStream
      : trustedFunction(values.createSnapshotReadStream, optionCode);
  const executables = objectFreezeIntrinsic({
    blockdev: executablePath(
      values.blockdevExecutable ?? DEFAULT_EXECUTABLES.blockdev,
      optionCode,
    ),
    dmsetup: executablePath(
      values.dmsetupExecutable ?? DEFAULT_EXECUTABLES.dmsetup,
      optionCode,
    ),
    lvcreate: executablePath(
      values.lvcreateExecutable ?? DEFAULT_EXECUTABLES.lvcreate,
      optionCode,
    ),
    lvs: executablePath(
      values.lvsExecutable ?? DEFAULT_EXECUTABLES.lvs,
      optionCode,
    ),
  });

  const runCommand = async (executable, arguments_) => {
    const args = objectFreezeIntrinsic(
      callIntrinsic(arraySliceIntrinsic, arguments_, []),
    );
    const commandOptions = exactFrozenRecord({
      maxBuffer: MAX_COMMAND_OUTPUT_BYTES,
    });
    let pending;
    try {
      pending = callIntrinsic(commandRunner, undefined, [
        executable,
        args,
        commandOptions,
      ]);
    } catch {
      fail("lvm_atomic_crash_capture_outcome_uncertain");
    }
    ensure(
      isSafeNativePromise(pending),
      "lvm_atomic_crash_capture_outcome_uncertain",
    );
    void observePromiseRejection(pending);
    try {
      return commandStdout(
        await pending,
        "lvm_atomic_crash_capture_outcome_uncertain",
      );
    } catch {
      fail("lvm_atomic_crash_capture_outcome_uncertain");
    }
  };

  const readLvs = async (selector) => {
    ensure(
      typeof selector === "string" &&
        selector.length <= 256 &&
        !stringIncludes(selector, "\u0000"),
      "lvm_atomic_crash_capture_outcome_uncertain",
    );
    const stdout = await runCommand(executables.lvs, [
      "--reportformat",
      "json",
      "--units",
      "b",
      "--nosuffix",
      "--options",
      "lv_uuid,origin_uuid,lv_name,lv_path,lv_size,lv_attr,snap_percent,lv_tags,lv_dm_path",
      "--select",
      selector,
    ]);
    return parseLvsRows(stdout);
  };

  const readDmIdentity = async (devicePath, lvUuid) => {
    const stdout = await runCommand(executables.dmsetup, [
      "info",
      "--columns",
      "--noheadings",
      "--separator",
      ":",
      "-o",
      "uuid,major,minor",
      "--",
      devicePath,
    ]);
    const line = stringTrim(stdout);
    const rawPieces = callIntrinsic(stringSplitIntrinsic, line, [":"]);
    const pieces = [];
    for (let index = 0; index < rawPieces.length; index += 1) {
      arrayPush(pieces, stringTrim(rawPieces[index]));
    }
    ensure(
      pieces.length === 3 &&
        regexpTest(DM_UUID_PATTERN, pieces[0]) &&
        regexpTest(/^(?:0|[1-9][0-9]*)$/u, pieces[1]) &&
        regexpTest(/^(?:0|[1-9][0-9]*)$/u, pieces[2]) &&
        stringEndsWith(pieces[0], stripHyphens(lvUuid)),
      "lvm_atomic_crash_capture_outcome_uncertain",
    );
    return exactFrozenRecord({
      dmUuid: pieces[0],
      majorMinor: `${pieces[1]}:${pieces[2]}`,
    });
  };

  const observeSnapshot = async (providerBinding, objectId = null) => {
    const selector =
      objectId === null
        ? `lv_name=${providerBinding.snapshotName}`
        : `lv_uuid=${opaqueLvmUuid(
            objectId,
            "lvm_atomic_crash_capture_outcome_uncertain",
          )}`;
    const rows = await readLvs(selector);
    const matches = arrayFilter(
      rows,
      (row) =>
        row.origin_uuid === providerBinding.originLvUuid &&
        row.lv_name === providerBinding.snapshotName &&
        arrayIncludes(lvmTags(row.lv_tags), providerBinding.snapshotTag),
    );
    ensure(
      matches.length === 1,
      "lvm_atomic_crash_capture_outcome_uncertain",
    );
    const row = matches[0];
    const lvUuid = opaqueLvmUuid(
      row.lv_uuid,
      "lvm_atomic_crash_capture_outcome_uncertain",
    );
    if (objectId !== null) {
      ensure(
        lvUuid === objectId,
        "lvm_atomic_crash_capture_outcome_uncertain",
      );
    }
    const lvAttr = stringTrim(row.lv_attr);
    ensure(
      lvAttr.length >= 5 &&
        lvAttr[0] === "s" &&
        (lvAttr[1] === "r" || lvAttr[1] === "R") &&
        lvAttr[4] === "a",
      "lvm_atomic_crash_capture_outcome_uncertain",
    );
    const cowPercent = parsePercentBelowFull(
      row.snap_percent,
      "lvm_atomic_crash_capture_outcome_uncertain",
    );
    const devicePath = validateDevicePath(
      stringTrim(row.lv_dm_path),
      "lvm_atomic_crash_capture_outcome_uncertain",
    );
    const lvPath = validateDevicePath(
      stringTrim(row.lv_path),
      "lvm_atomic_crash_capture_outcome_uncertain",
    );
    const cowAllocationBytes = parseLvmSize(
      row.lv_size,
      "lvm_atomic_crash_capture_outcome_uncertain",
    );
    const [readOnlyOutput, sizeOutput, dmIdentity] = await Promise.all([
      runCommand(executables.blockdev, ["--getro", devicePath]),
      runCommand(executables.blockdev, ["--getsize64", devicePath]),
      readDmIdentity(devicePath, lvUuid),
    ]);
    const byteLength = positiveUint64Decimal(
      stringTrim(sizeOutput),
      "lvm_atomic_crash_capture_outcome_uncertain",
    );
    ensure(
      stringTrim(readOnlyOutput) === "1" &&
        cowAllocationBytes === providerBinding.snapshotSizeBytes &&
        regexpTest(MAJOR_MINOR_PATTERN, dmIdentity.majorMinor),
      "lvm_atomic_crash_capture_outcome_uncertain",
    );
    return exactFrozenRecord({
      byteLength,
      cowAllocationBytes,
      cowPercent,
      devicePath,
      dmUuid: dmIdentity.dmUuid,
      lvPath,
      lvUuid,
      majorMinor: dmIdentity.majorMinor,
      originLvUuid: row.origin_uuid,
      readOnly: true,
      snapshotName: row.lv_name,
      snapshotTag: providerBinding.snapshotTag,
    });
  };

  const observeOrigin = async (originLvUuid) => {
    const rows = await readLvs(`lv_uuid=${originLvUuid}`);
    ensure(
      rows.length === 1 && rows[0].lv_uuid === originLvUuid,
      "lvm_atomic_crash_capture_outcome_uncertain",
    );
    return exactFrozenRecord({
      lvPath: validateDevicePath(
        stringTrim(rows[0].lv_path),
        "lvm_atomic_crash_capture_outcome_uncertain",
      ),
      lvUuid: originLvUuid,
    });
  };

  const digestSnapshot = async (observation) => {
    const hash = createHash("sha256");
    let bytes = 0n;
    let stream;
    try {
      stream = callIntrinsic(openSnapshot, undefined, [
        observation.devicePath,
      ]);
      ensure(
        stream !== null &&
          typeof stream === "object" &&
          !isProxyValue(stream),
        "lvm_atomic_crash_capture_outcome_uncertain",
      );
      for await (const chunk of stream) {
        ensure(
          bufferIsBufferIntrinsic(chunk) || chunk instanceof Uint8Array,
          "lvm_atomic_crash_capture_outcome_uncertain",
        );
        bytes += BigInt(chunk.byteLength);
        ensure(
          bytes <= UINT64_MAX,
          "lvm_atomic_crash_capture_outcome_uncertain",
        );
        callIntrinsic(hashUpdateIntrinsic, hash, [chunk]);
      }
    } catch {
      fail("lvm_atomic_crash_capture_outcome_uncertain");
    }
    ensure(
      callIntrinsic(bigIntToStringIntrinsic, bytes, []) ===
        observation.byteLength,
      "lvm_atomic_crash_capture_outcome_uncertain",
    );
    const contentSha256 = callIntrinsic(hashDigestIntrinsic, hash, ["hex"]);
    ensure(
      regexpTest(SHA256_PATTERN, contentSha256),
      "lvm_atomic_crash_capture_outcome_uncertain",
    );
    return contentSha256;
  };

  const stableDigestObservation = async (providerBinding, objectId = null) => {
    const before = await observeSnapshot(providerBinding, objectId);
    const contentSha256 = await digestSnapshot(before);
    const after = await observeSnapshot(providerBinding, before.lvUuid);
    ensure(
      before.lvUuid === after.lvUuid &&
        before.originLvUuid === after.originLvUuid &&
        before.snapshotName === after.snapshotName &&
        before.snapshotTag === after.snapshotTag &&
      before.byteLength === after.byteLength &&
        before.cowAllocationBytes === after.cowAllocationBytes &&
        before.dmUuid === after.dmUuid &&
        before.majorMinor === after.majorMinor &&
        before.readOnly === true &&
        after.readOnly === true,
      "lvm_atomic_crash_capture_outcome_uncertain",
    );
    return exactFrozenRecord({ ...after, contentSha256 });
  };

  let driver;
  const resolveProviderBinding = async function resolveProviderBinding(input) {
    ensure(
      objectIsIntrinsic(this, driver),
      "lvm_atomic_crash_capture_outcome_uncertain",
    );
    const record = inspectExactDataObject(
      input,
      objectFreezeIntrinsic(["request"]),
      "lvm_atomic_crash_capture_outcome_uncertain",
    );
    const request = normalizeRequest(record.request);
    let raw;
    try {
      raw = callIntrinsic(resolveOrigin, undefined, [
        exactFrozenRecord({ request }),
      ]);
      if (isPromiseValue(raw)) {
        ensure(
          isSafeNativePromise(raw),
          "lvm_atomic_crash_capture_outcome_uncertain",
        );
        void observePromiseRejection(raw);
        raw = await raw;
      } else {
        ensure(
          !isGeneratorObjectValue(raw),
          "lvm_atomic_crash_capture_outcome_uncertain",
        );
      }
    } catch {
      fail("lvm_atomic_crash_capture_outcome_uncertain");
    }
    return deterministicBinding(request, parseOriginResolution(raw));
  };
  objectFreezeIntrinsic(resolveProviderBinding);

  const captureSnapshot = async function captureSnapshot(input) {
    ensure(
      objectIsIntrinsic(this, driver),
      "lvm_atomic_crash_capture_outcome_uncertain",
    );
    const record = inspectExactDataObject(
      input,
      objectFreezeIntrinsic(["providerBinding", "request"]),
      "lvm_atomic_crash_capture_outcome_uncertain",
    );
    const request = normalizeRequest(record.request);
    const providerBinding = assertLvmAtomicCrashCaptureProviderBinding(
      record.providerBinding,
    );
    const origin = await observeOrigin(providerBinding.originLvUuid);
    // Once lvcreate has been invoked, every failure is outcome-uncertain and
    // this driver deliberately performs no speculative snapshot deletion.
    await runCommand(executables.lvcreate, [
      "--snapshot",
      "--name",
      providerBinding.snapshotName,
      "--size",
      `${providerBinding.snapshotSizeBytes}B`,
      "--addtag",
      providerBinding.snapshotTag,
      "--permission",
      "r",
      origin.lvPath,
    ]);
    const observation = await stableDigestObservation(providerBinding);
    const proofHash = createHash("sha256");
    callIntrinsic(hashUpdateIntrinsic, proofHash, [
      `${request.captureAttemptId}\u0000${observation.lvUuid}\u0000${observation.contentSha256}`,
      "utf8",
    ]);
    const proofDigest = callIntrinsic(hashDigestIntrinsic, proofHash, ["hex"]);
    return assertAtomicCrashCaptureResult(
      {
        artifact: {
          byteLength: observation.byteLength,
          contentSha256: observation.contentSha256,
          objectId: observation.lvUuid,
          objectIdentityScheme: "lvm-lv-uuid-v1",
          readOnly: true,
        },
        artifactId: request.checkpoint.artifactId,
        backendId: request.storageRef.backendId,
        captureAttemptId: request.captureAttemptId,
        checkpointId: request.checkpoint.checkpointId,
        contractVersion: ATOMIC_CRASH_CAPTURE_CONTRACT_VERSION,
        operationId: request.mutationRequest.operationId,
        proofId: `lvm-proof-${stringSlice(proofDigest, 0, 48)}`,
        sessionId: request.storageRef.sessionId,
        sourceFencingEpoch: request.checkpoint.sourceFencingEpoch,
        status: "committed",
        storageId: request.storageRef.storageId,
      },
      { request },
    );
  };
  objectFreezeIntrinsic(captureSnapshot);

  const verifySnapshot = async function verifySnapshot(input) {
    if (!objectIsIntrinsic(this, driver)) return false;
    try {
      const record = inspectExactDataObject(
        input,
        objectFreezeIntrinsic(["providerBinding", "result"]),
        "lvm_atomic_crash_capture_outcome_uncertain",
      );
      const providerBinding = assertLvmAtomicCrashCaptureProviderBinding(
        record.providerBinding,
      );
      const result = record.result;
      ensure(
        result !== null &&
          typeof result === "object" &&
          !isProxyValue(result) &&
          result.artifact?.objectIdentityScheme === "lvm-lv-uuid-v1",
        "lvm_atomic_crash_capture_outcome_uncertain",
      );
      const observation = await stableDigestObservation(
        providerBinding,
        result.artifact.objectId,
      );
      return (
        observation.lvUuid === result.artifact.objectId &&
        observation.byteLength === result.artifact.byteLength &&
        observation.contentSha256 === result.artifact.contentSha256 &&
        result.artifact.readOnly === true
      );
    } catch {
      return false;
    }
  };
  objectFreezeIntrinsic(verifySnapshot);

  driver = objectCreateIntrinsic(null);
  driver.captureSnapshot = captureSnapshot;
  driver.contractVersion = LVM_ATOMIC_CRASH_CAPTURE_DRIVER_CONTRACT_VERSION;
  driver.resolveProviderBinding = resolveProviderBinding;
  driver.verifySnapshot = verifySnapshot;
  return objectFreezeIntrinsic(driver);
}
