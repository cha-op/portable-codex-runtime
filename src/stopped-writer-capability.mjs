import { types as utilTypes } from "node:util";

import {
  assertLeaseGrant,
  assertSessionAttachment,
  compareFencingEpochs,
} from "./session-storage-contracts.mjs";

const OPAQUE_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u;
const FENCE_KEYS = Object.freeze([
  "contractVersion",
  "fencingEpoch",
  "holderId",
  "leaseId",
  "sessionId",
]);

const ERROR_MESSAGES = Object.freeze({
  invalid_stopped_writer_request: "Stopped-writer request is invalid",
  snapshot_outcome_uncertain: "Snapshot callback outcome is uncertain",
  stopped_writer_capability_rejected: "Stopped-writer capability was rejected",
  writer_state_conflict: "Writer incarnation state conflicts with the request",
  writer_stop_outcome_uncertain: "Writer stop outcome is uncertain",
});
const INTERNAL_ERRORS = new WeakSet();

export const STOPPED_WRITER_STOP_CONFIRMED = Object.freeze(Object.create(null));

export class StoppedWriterCapabilityError extends Error {
  constructor(code) {
    if (!Object.hasOwn(ERROR_MESSAGES, code)) {
      throw new TypeError("unsupported stopped-writer capability error code");
    }
    super(ERROR_MESSAGES[code]);
    this.name = "StoppedWriterCapabilityError";
    this.code = code;
    this.retryable = false;
    Object.defineProperty(this, "stack", {
      configurable: false,
      enumerable: false,
      value: `${this.name}: ${this.message}`,
      writable: false,
    });
    Object.freeze(this);
  }
}

function fail(code) {
  throw makeError(code);
}

function makeError(code) {
  const error = new StoppedWriterCapabilityError(code);
  INTERNAL_ERRORS.add(error);
  return error;
}

function ensure(condition, code) {
  if (!condition) fail(code);
}

function assertExactOptions(value, keys) {
  if (
    utilTypes.isProxy(value) ||
    value === null ||
    typeof value !== "object" ||
    Array.isArray(value)
  ) {
    fail("invalid_stopped_writer_request");
  }

  let prototype;
  let actual;
  try {
    prototype = Object.getPrototypeOf(value);
    actual = Reflect.ownKeys(value);
  } catch {
    fail("invalid_stopped_writer_request");
  }
  ensure(
    [Object.prototype, null].includes(prototype),
    "invalid_stopped_writer_request",
  );
  ensure(
    actual.length === keys.length &&
      actual.every((key) => typeof key === "string" && keys.includes(key)),
    "invalid_stopped_writer_request",
  );

  const normalized = Object.create(null);
  for (const key of actual) {
    let descriptor;
    try {
      descriptor = Object.getOwnPropertyDescriptor(value, key);
    } catch {
      fail("invalid_stopped_writer_request");
    }
    ensure(
      descriptor?.enumerable === true && Object.hasOwn(descriptor, "value"),
      "invalid_stopped_writer_request",
    );
    normalized[key] = descriptor.value;
  }
  return normalized;
}

function assertOpaqueId(value) {
  ensure(
    typeof value === "string" && OPAQUE_ID_PATTERN.test(value),
    "invalid_stopped_writer_request",
  );
  return value;
}

function assertTrustedCallback(value) {
  ensure(
    typeof value === "function" &&
      !utilTypes.isProxy(value) &&
      !utilTypes.isGeneratorFunction(value),
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
  const writerFence = Object.freeze({
    contractVersion: lease.contractVersion,
    fencingEpoch: lease.fencingEpoch,
    holderId: lease.holderId,
    leaseId: lease.leaseId,
    sessionId: lease.sessionId,
  });
  return { attachment, writerFence };
}

function sameFlatRecord(left, right) {
  const leftKeys = Object.keys(left);
  const rightKeys = Object.keys(right);
  return (
    leftKeys.length === rightKeys.length &&
    leftKeys.every((key) => Object.hasOwn(right, key) && left[key] === right[key])
  );
}

function sameFence(left, right) {
  return FENCE_KEYS.every((key) => left[key] === right[key]);
}

function makeOpaqueHandle() {
  return Object.freeze(Object.create(null));
}

function slotKey(attachment) {
  return JSON.stringify([
    attachment.sessionId,
    attachment.backendId,
    attachment.storageId,
  ]);
}

function frozenCallbackBinding(record) {
  return Object.freeze({
    attachment: record.attachment,
    processIncarnationId: record.processIncarnationId,
    stopOperationId: record.stopOperationId,
    writerFence: record.writerFence,
    writerIncarnationId: record.writerIncarnationId,
  });
}

export class StoppedWriterCapabilityCoordinator {
  #capabilities = new WeakMap();

  #slots = new Map();

  #writers = new WeakMap();

  constructor(...args) {
    ensure(args.length === 0, "invalid_stopped_writer_request");
    Object.freeze(this);
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
    const key = slotKey(attachment);
    const existing = this.#slots.get(key);
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
    const record = {
      attachment,
      capabilityRecord: null,
      handle: writer,
      processIncarnationId,
      revocationRequested: false,
      slotKey: key,
      state: "running",
      stopEstablished: false,
      stopOperationId: null,
      stopWriter,
      writerFence,
      writerIncarnationId,
    };
    this.#writers.set(writer, record);
    this.#slots.set(key, {
      current: record,
      lastFencingEpoch: existing?.lastFencingEpoch,
    });
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
      const stopResult = await Reflect.apply(record.stopWriter, undefined, [binding]);
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
    this.#capabilities.set(capability, capabilityRecord);
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
    const capabilityRecord = this.#capabilities.get(normalized.capability);
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
          this.#writers.get(normalized.writer) === record,
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
        result = await Reflect.apply(runSnapshot, undefined, [
          frozenCallbackBinding(record),
        ]);
      } catch {
        capabilityRecord.state = "consumed";
        record.state = "snapshot-uncertain";
        throw makeError("snapshot_outcome_uncertain");
      }
      if (
        record.revocationRequested ||
        utilTypes.isProxy(result) ||
        utilTypes.isGeneratorObject(result)
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
        INTERNAL_ERRORS.has(error) &&
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
      ["running", "stopping", "issued", "stop-uncertain"].includes(
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
    const slot = this.#slots.get(record.slotKey);
    ensure(slot?.current === record, "writer_state_conflict");
    record.state = "retired";
    slot.current = null;
    slot.lastFencingEpoch = record.writerFence.fencingEpoch;
  }

  #authenticateWriter(writer, processValue, writerValue) {
    if (
      writer === null ||
      typeof writer !== "object" ||
      Array.isArray(writer) ||
      utilTypes.isProxy(writer)
    ) {
      fail("writer_state_conflict");
    }
    const record = this.#writers.get(writer);
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

Object.freeze(StoppedWriterCapabilityCoordinator.prototype);
Object.freeze(StoppedWriterCapabilityCoordinator);
Object.freeze(StoppedWriterCapabilityError.prototype);
Object.freeze(StoppedWriterCapabilityError);
