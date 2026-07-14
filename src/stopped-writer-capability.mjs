import { types as utilTypes } from "node:util";

import {
  assertLeaseGrant,
  assertSessionAttachment,
  compareFencingEpochs,
} from "./session-storage-contracts.mjs";

const arrayEveryIntrinsic = Array.prototype.every;
const arrayIncludesIntrinsic = Array.prototype.includes;
const arrayIsArray = Array.isArray;
const mapGetIntrinsic = Map.prototype.get;
const mapSetIntrinsic = Map.prototype.set;
const MapConstructor = Map;
const objectCreate = Object.create;
const objectDefineProperty = Object.defineProperty;
const objectFreeze = Object.freeze;
const objectGetOwnPropertyDescriptor = Object.getOwnPropertyDescriptor;
const objectGetPrototypeOf = Object.getPrototypeOf;
const objectHasOwn = Object.hasOwn;
const objectKeys = Object.keys;
const objectPrototype = Object.prototype;
const reflectApply = Reflect.apply;
const reflectOwnKeys = Reflect.ownKeys;
const regexpTestIntrinsic = RegExp.prototype.test;
const {
  isGeneratorFunction: isGeneratorFunctionValue,
  isGeneratorObject: isGeneratorObjectValue,
  isProxy: isProxyValue,
} = utilTypes;
const weakMapGetIntrinsic = WeakMap.prototype.get;
const weakMapSetIntrinsic = WeakMap.prototype.set;
const WeakMapConstructor = WeakMap;
const weakSetAddIntrinsic = WeakSet.prototype.add;
const weakSetHasIntrinsic = WeakSet.prototype.has;
const WeakSetConstructor = WeakSet;

function callIntrinsic(intrinsic, receiver, args) {
  return reflectApply(intrinsic, receiver, args);
}

function arrayEvery(value, callback) {
  return callIntrinsic(arrayEveryIntrinsic, value, [callback]);
}

function arrayIncludes(value, candidate) {
  return callIntrinsic(arrayIncludesIntrinsic, value, [candidate]);
}

function mapGet(value, key) {
  return callIntrinsic(mapGetIntrinsic, value, [key]);
}

function mapSet(value, key, entry) {
  return callIntrinsic(mapSetIntrinsic, value, [key, entry]);
}

function regexpTest(value, candidate) {
  return callIntrinsic(regexpTestIntrinsic, value, [candidate]);
}

function weakMapGet(value, key) {
  return callIntrinsic(weakMapGetIntrinsic, value, [key]);
}

function weakMapSet(value, key, entry) {
  return callIntrinsic(weakMapSetIntrinsic, value, [key, entry]);
}

function weakSetAdd(value, entry) {
  return callIntrinsic(weakSetAddIntrinsic, value, [entry]);
}

function weakSetHas(value, entry) {
  return callIntrinsic(weakSetHasIntrinsic, value, [entry]);
}

const OPAQUE_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u;
const FENCE_KEYS = objectFreeze([
  "contractVersion",
  "fencingEpoch",
  "holderId",
  "leaseId",
  "sessionId",
]);

const ERROR_MESSAGES = objectFreeze({
  invalid_stopped_writer_request: "Stopped-writer request is invalid",
  snapshot_outcome_uncertain: "Snapshot callback outcome is uncertain",
  stopped_writer_capability_rejected: "Stopped-writer capability was rejected",
  writer_state_conflict: "Writer incarnation state conflicts with the request",
  writer_stop_outcome_uncertain: "Writer stop outcome is uncertain",
});
const INTERNAL_ERRORS = new WeakSetConstructor();

export const STOPPED_WRITER_STOP_CONFIRMED = objectFreeze(objectCreate(null));

export class StoppedWriterCapabilityError extends Error {
  constructor(code) {
    if (!objectHasOwn(ERROR_MESSAGES, code)) {
      throw new TypeError("unsupported stopped-writer capability error code");
    }
    const message = ERROR_MESSAGES[code];
    super(message);
    objectDefineProperty(this, "name", {
      configurable: true,
      enumerable: true,
      value: "StoppedWriterCapabilityError",
      writable: true,
    });
    objectDefineProperty(this, "code", {
      configurable: true,
      enumerable: true,
      value: code,
      writable: true,
    });
    objectDefineProperty(this, "retryable", {
      configurable: true,
      enumerable: true,
      value: false,
      writable: true,
    });
    objectDefineProperty(this, "stack", {
      configurable: false,
      enumerable: false,
      value: `StoppedWriterCapabilityError: ${message}`,
      writable: false,
    });
    objectFreeze(this);
  }
}

function fail(code) {
  throw makeError(code);
}

function makeError(code) {
  const error = new StoppedWriterCapabilityError(code);
  weakSetAdd(INTERNAL_ERRORS, error);
  return error;
}

function ensure(condition, code) {
  if (!condition) fail(code);
}

function assertExactOptions(value, keys) {
  if (
    isProxyValue(value) ||
    value === null ||
    typeof value !== "object" ||
    arrayIsArray(value)
  ) {
    fail("invalid_stopped_writer_request");
  }

  let prototype;
  let actual;
  try {
    prototype = objectGetPrototypeOf(value);
    actual = reflectOwnKeys(value);
  } catch {
    fail("invalid_stopped_writer_request");
  }
  ensure(
    prototype === objectPrototype || prototype === null,
    "invalid_stopped_writer_request",
  );
  ensure(
    actual.length === keys.length &&
      arrayEvery(
        actual,
        (key) => typeof key === "string" && arrayIncludes(keys, key),
      ),
    "invalid_stopped_writer_request",
  );

  const normalized = objectCreate(null);
  for (const key of actual) {
    let descriptor;
    try {
      descriptor = objectGetOwnPropertyDescriptor(value, key);
    } catch {
      fail("invalid_stopped_writer_request");
    }
    ensure(
      descriptor?.enumerable === true && objectHasOwn(descriptor, "value"),
      "invalid_stopped_writer_request",
    );
    normalized[key] = descriptor.value;
  }
  return normalized;
}

function assertOpaqueId(value) {
  ensure(
    typeof value === "string" && regexpTest(OPAQUE_ID_PATTERN, value),
    "invalid_stopped_writer_request",
  );
  return value;
}

function assertTrustedCallback(value) {
  ensure(
    typeof value === "function" &&
      !isProxyValue(value) &&
      !isGeneratorFunctionValue(value),
    "invalid_stopped_writer_request",
  );
  return value;
}

function normalizeBinding(attachmentValue, leaseValue) {
  let attachment;
  let lease;
  try {
    attachment = assertSessionAttachment(attachmentValue);
    lease = assertLeaseGrant(leaseValue);
  } catch {
    fail("invalid_stopped_writer_request");
  }
  ensure(
    attachment.sessionId === lease.sessionId &&
      attachment.leaseId === lease.leaseId &&
      attachment.holderId === lease.holderId &&
      attachment.fencingEpoch === lease.fencingEpoch,
    "invalid_stopped_writer_request",
  );
  const writerFence = objectFreeze({
    contractVersion: lease.contractVersion,
    fencingEpoch: lease.fencingEpoch,
    holderId: lease.holderId,
    leaseId: lease.leaseId,
    sessionId: lease.sessionId,
  });
  return { attachment, writerFence };
}

function sameFlatRecord(left, right) {
  const leftKeys = objectKeys(left);
  const rightKeys = objectKeys(right);
  return (
    leftKeys.length === rightKeys.length &&
    arrayEvery(
      leftKeys,
      (key) => objectHasOwn(right, key) && left[key] === right[key],
    )
  );
}

function sameFence(left, right) {
  return arrayEvery(FENCE_KEYS, (key) => left[key] === right[key]);
}

function isSafeAsyncReturnValue(value) {
  if (
    value === null ||
    (typeof value !== "object" && typeof value !== "function")
  ) {
    return true;
  }

  let current = value;
  while (current !== null) {
    if (isProxyValue(current)) return false;
    let descriptor;
    try {
      descriptor = objectGetOwnPropertyDescriptor(current, "then");
    } catch {
      return false;
    }
    if (descriptor !== undefined) {
      return (
        objectHasOwn(descriptor, "value") &&
        typeof descriptor.value !== "function"
      );
    }
    try {
      current = objectGetPrototypeOf(current);
    } catch {
      return false;
    }
  }
  return true;
}

function makeOpaqueHandle() {
  return objectFreeze(objectCreate(null));
}

function findSlot(slots, attachment) {
  const backendSlots = mapGet(slots, attachment.sessionId);
  if (backendSlots === undefined) return undefined;
  const storageSlots = mapGet(backendSlots, attachment.backendId);
  if (storageSlots === undefined) return undefined;
  return mapGet(storageSlots, attachment.storageId);
}

function createSlot(slots, attachment) {
  let backendSlots = mapGet(slots, attachment.sessionId);
  if (backendSlots === undefined) {
    backendSlots = new MapConstructor();
    mapSet(slots, attachment.sessionId, backendSlots);
  }
  let storageSlots = mapGet(backendSlots, attachment.backendId);
  if (storageSlots === undefined) {
    storageSlots = new MapConstructor();
    mapSet(backendSlots, attachment.backendId, storageSlots);
  }
  const slot = {
    current: null,
    lastFencingEpoch: undefined,
  };
  mapSet(storageSlots, attachment.storageId, slot);
  return slot;
}

function frozenCallbackBinding(record) {
  return objectFreeze({
    attachment: record.attachment,
    processIncarnationId: record.processIncarnationId,
    stopOperationId: record.stopOperationId,
    writerFence: record.writerFence,
    writerIncarnationId: record.writerIncarnationId,
  });
}

export class StoppedWriterCapabilityCoordinator {
  #capabilities = new WeakMapConstructor();

  #slots = new MapConstructor();

  #writers = new WeakMapConstructor();

  constructor(...args) {
    ensure(args.length === 0, "invalid_stopped_writer_request");
    objectFreeze(this);
  }

  registerWriter(options) {
    const {
      attachment: attachmentValue,
      canonicalLease,
      processIncarnationId: processValue,
      stopWriter: stopValue,
      writerIncarnationId: writerValue,
    } = assertExactOptions(options, [
      "attachment",
      "canonicalLease",
      "processIncarnationId",
      "stopWriter",
      "writerIncarnationId",
    ]);
    const processIncarnationId = assertOpaqueId(processValue);
    const writerIncarnationId = assertOpaqueId(writerValue);
    const stopWriter = assertTrustedCallback(stopValue);
    const { attachment, writerFence } = normalizeBinding(
      attachmentValue,
      canonicalLease,
    );
    const existing = findSlot(this.#slots, attachment);
    if (existing?.current !== null && existing?.current !== undefined) {
      fail("writer_state_conflict");
    }
    if (existing?.lastFencingEpoch !== undefined) {
      let comparison;
      try {
        comparison = compareFencingEpochs(
          writerFence.fencingEpoch,
          existing.lastFencingEpoch,
        );
      } catch {
        fail("invalid_stopped_writer_request");
      }
      ensure(comparison > 0, "writer_state_conflict");
    }

    const writer = makeOpaqueHandle();
    const slot = existing ?? createSlot(this.#slots, attachment);
    const record = {
      attachment,
      capabilityRecord: null,
      handle: writer,
      processIncarnationId,
      revocationRequested: false,
      slot,
      state: "running",
      stopEstablished: false,
      stopOperationId: null,
      stopWriter,
      writerFence,
      writerIncarnationId,
    };
    weakMapSet(this.#writers, writer, record);
    slot.current = record;
    return writer;
  }

  async stopAndIssueCapability(options) {
    const {
      processIncarnationId,
      stopOperationId: stopValue,
      writer,
      writerIncarnationId,
    } = assertExactOptions(options, [
      "processIncarnationId",
      "stopOperationId",
      "writer",
      "writerIncarnationId",
    ]);
    const record = this.#authenticateWriter(
      writer,
      processIncarnationId,
      writerIncarnationId,
    );
    const stopOperationId = assertOpaqueId(stopValue);
    ensure(record.state === "running", "writer_state_conflict");

    record.state = "stopping";
    record.stopOperationId = stopOperationId;
    const binding = frozenCallbackBinding(record);
    try {
      const stopResult = await reflectApply(record.stopWriter, undefined, [binding]);
      ensure(
        stopResult === STOPPED_WRITER_STOP_CONFIRMED,
        "writer_stop_outcome_uncertain",
      );
    } catch {
      if (record.state === "stopping") record.state = "stop-uncertain";
      record.stopEstablished = false;
      throw makeError("writer_stop_outcome_uncertain");
    }

    record.stopEstablished = true;
    ensure(record.state === "stopping", "writer_state_conflict");
    const capability = makeOpaqueHandle();
    const capabilityRecord = {
      handle: capability,
      state: "issued",
      writerRecord: record,
    };
    record.capabilityRecord = capabilityRecord;
    record.state = "issued";
    weakMapSet(this.#capabilities, capability, capabilityRecord);
    return capability;
  }

  async consumeCapability(options) {
    const normalized = assertExactOptions(options, [
      "attachment",
      "canonicalLease",
      "capability",
      "processIncarnationId",
      "runSnapshot",
      "stopOperationId",
      "writer",
      "writerIncarnationId",
    ]);
    const capabilityRecord = weakMapGet(
      this.#capabilities,
      normalized.capability,
    );
    if (
      capabilityRecord === undefined ||
      capabilityRecord.state !== "issued"
    ) {
      fail("stopped_writer_capability_rejected");
    }
    const record = capabilityRecord.writerRecord;

    try {
      ensure(
        normalized.writer === record.handle &&
          weakMapGet(this.#writers, normalized.writer) === record,
        "stopped_writer_capability_rejected",
      );
      ensure(
        assertOpaqueId(normalized.processIncarnationId) ===
          record.processIncarnationId &&
          assertOpaqueId(normalized.writerIncarnationId) ===
            record.writerIncarnationId &&
          assertOpaqueId(normalized.stopOperationId) === record.stopOperationId,
        "stopped_writer_capability_rejected",
      );
      const runSnapshot = assertTrustedCallback(normalized.runSnapshot);
      const presented = normalizeBinding(
        normalized.attachment,
        normalized.canonicalLease,
      );
      ensure(
        sameFlatRecord(presented.attachment, record.attachment) &&
          sameFence(presented.writerFence, record.writerFence),
        "stopped_writer_capability_rejected",
      );
      ensure(record.state === "issued", "stopped_writer_capability_rejected");

      capabilityRecord.state = "consuming";
      record.state = "consuming";
      let result;
      try {
        result = await reflectApply(runSnapshot, undefined, [
          frozenCallbackBinding(record),
        ]);
      } catch {
        capabilityRecord.state = "consumed";
        record.state = "snapshot-uncertain";
        throw makeError("snapshot_outcome_uncertain");
      }
      if (
        record.revocationRequested ||
        isProxyValue(result) ||
        isGeneratorObjectValue(result) ||
        !isSafeAsyncReturnValue(result)
      ) {
        capabilityRecord.state = "consumed";
        record.state = "snapshot-uncertain";
        throw makeError("snapshot_outcome_uncertain");
      }
      capabilityRecord.state = "consumed";
      record.state = "consumed";
      return result;
    } catch (error) {
      if (
        weakSetHas(INTERNAL_ERRORS, error) &&
        error.code === "snapshot_outcome_uncertain"
      ) {
        throw error;
      }
      if (capabilityRecord.state === "issued") {
        capabilityRecord.state = "revoked";
        record.state = "revoked";
      }
      throw makeError("stopped_writer_capability_rejected");
    }
  }

  revokeWriter(options) {
    const { processIncarnationId, writer, writerIncarnationId } =
      assertExactOptions(options, [
        "processIncarnationId",
        "writer",
        "writerIncarnationId",
      ]);
    const record = this.#authenticateWriter(
      writer,
      processIncarnationId,
      writerIncarnationId,
    );
    if (record.state === "consuming") {
      record.revocationRequested = true;
      return;
    }
    ensure(
      arrayIncludes(
        ["running", "stopping", "issued", "stop-uncertain"],
        record.state,
      ),
      "writer_state_conflict",
    );
    if (record.capabilityRecord?.state === "issued") {
      record.capabilityRecord.state = "revoked";
    }
    record.state = "revoked";
  }

  retireWriter(options) {
    const { processIncarnationId, writer, writerIncarnationId } =
      assertExactOptions(options, [
        "processIncarnationId",
        "writer",
        "writerIncarnationId",
      ]);
    const record = this.#authenticateWriter(
      writer,
      processIncarnationId,
      writerIncarnationId,
    );
    ensure(
      record.state === "consumed" ||
        (record.state === "revoked" && record.stopEstablished),
      "writer_state_conflict",
    );
    const { slot } = record;
    ensure(slot?.current === record, "writer_state_conflict");
    record.state = "retired";
    slot.current = null;
    slot.lastFencingEpoch = record.writerFence.fencingEpoch;
  }

  #authenticateWriter(writer, processValue, writerValue) {
    if (
      writer === null ||
      typeof writer !== "object" ||
      arrayIsArray(writer) ||
      isProxyValue(writer)
    ) {
      fail("writer_state_conflict");
    }
    const record = weakMapGet(this.#writers, writer);
    ensure(record !== undefined, "writer_state_conflict");
    let processIncarnationId;
    let writerIncarnationId;
    try {
      processIncarnationId = assertOpaqueId(processValue);
      writerIncarnationId = assertOpaqueId(writerValue);
    } catch {
      fail("writer_state_conflict");
    }
    ensure(
      processIncarnationId === record.processIncarnationId &&
        writerIncarnationId === record.writerIncarnationId,
      "writer_state_conflict",
    );
    return record;
  }
}

objectFreeze(StoppedWriterCapabilityCoordinator.prototype);
objectFreeze(StoppedWriterCapabilityCoordinator);
objectFreeze(StoppedWriterCapabilityError.prototype);
objectFreeze(StoppedWriterCapabilityError);
