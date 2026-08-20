import assert from "node:assert/strict";
import { Buffer } from "node:buffer";
import { createHash } from "node:crypto";
import { EventEmitter } from "node:events";
import test from "node:test";

import {
  FILESYSTEM_IMAGE_PROVIDER_STATE_HEAD_CONTRACT_VERSION,
  FILESYSTEM_IMAGE_PROVIDER_STATE_V2_HEAD_CONTRACT_VERSION,
  filesystemImageProviderStateHeadChecksum,
} from "../src/filesystem-image-provider-state.mjs";
import {
  POSTGRES_FILESYSTEM_IMAGE_PROVIDER_STATE_AUTHORITY_CONTRACT_VERSION,
  POSTGRES_FILESYSTEM_IMAGE_PROVIDER_STATE_ADOPTION_AUTHORITY_CONTRACT_VERSION,
  POSTGRES_FILESYSTEM_IMAGE_PROVIDER_STATE_RUNTIME_AUTHORITY_CONTRACT_VERSION,
  PostgresFilesystemImageProviderStateAuthorityError,
  createPostgresFilesystemImageProviderStateAdoptionAuthority,
  createPostgresFilesystemImageProviderStateAuthority,
  createPostgresFilesystemImageProviderStateRuntimeAuthority,
} from "../src/postgres-filesystem-image-provider-state-authority.mjs";
import {
  PostgresSerializableStore,
  PostgresSerializableStoreError,
} from "../src/postgres-serializable-store.mjs";

const GENESIS = Object.freeze({
  contractVersion: FILESYSTEM_IMAGE_PROVIDER_STATE_V2_HEAD_CONTRACT_VERSION,
  anchorRevision: "0",
  generation: "0",
  stateRevision: "0",
  baseHeadChecksum: null,
  checkpointStateRevision: "0",
  checkpointFrameCount: 0,
  checkpointChecksum: null,
  checkpointBytes: 0,
  frameCount: 0,
  lastChecksum: null,
  ledgerBytes: 0,
});
const V3_GENESIS = Object.freeze({
  ...GENESIS,
  contractVersion: FILESYSTEM_IMAGE_PROVIDER_STATE_HEAD_CONTRACT_VERSION,
});
const RECORD_DOMAIN = Buffer.from(
  "portable-codex/filesystem-image-provider-state/operation-record/v1\0",
  "utf8",
);

function successor(value) {
  return (BigInt(value) + 1n).toString();
}

function appendHead(expectedHead, frameChecksum, ledgerBytes) {
  return {
    contractVersion: expectedHead.contractVersion,
    anchorRevision: successor(expectedHead.anchorRevision),
    generation: expectedHead.generation,
    stateRevision: successor(expectedHead.stateRevision),
    baseHeadChecksum: expectedHead.baseHeadChecksum,
    checkpointStateRevision: expectedHead.checkpointStateRevision,
    checkpointFrameCount: expectedHead.checkpointFrameCount,
    checkpointChecksum: expectedHead.checkpointChecksum,
    checkpointBytes: expectedHead.checkpointBytes,
    frameCount: expectedHead.frameCount + 1,
    lastChecksum: frameChecksum,
    ledgerBytes,
  };
}

function rotationHead(expectedHead) {
  const checkpointChecksum = "d".repeat(64);
  return {
    contractVersion: expectedHead.contractVersion,
    anchorRevision: successor(expectedHead.anchorRevision),
    generation: successor(expectedHead.generation),
    stateRevision: expectedHead.stateRevision,
    baseHeadChecksum: filesystemImageProviderStateHeadChecksum(expectedHead),
    checkpointStateRevision: expectedHead.stateRevision,
    checkpointFrameCount: 2,
    checkpointChecksum,
    checkpointBytes: 768,
    frameCount: 0,
    lastChecksum: checkpointChecksum,
    ledgerBytes: 0,
  };
}

function adoptionHead(expectedHead) {
  const checkpointChecksum = "e".repeat(64);
  return {
    contractVersion: FILESYSTEM_IMAGE_PROVIDER_STATE_HEAD_CONTRACT_VERSION,
    anchorRevision: successor(expectedHead.anchorRevision),
    generation: successor(expectedHead.generation),
    stateRevision: expectedHead.stateRevision,
    baseHeadChecksum: filesystemImageProviderStateHeadChecksum(expectedHead),
    checkpointStateRevision: expectedHead.stateRevision,
    checkpointFrameCount: 3,
    checkpointChecksum,
    checkpointBytes: 896,
    frameCount: 0,
    lastChecksum: checkpointChecksum,
    ledgerBytes: 0,
  };
}

function preparedRecord({
  checksum = "a".repeat(64),
  kind = "provision",
  operationId = "operation-1",
  revision = "1",
  storageId = "storage-1",
  storageStateBefore = null,
} = {}) {
  return {
    kind,
    operationId,
    preparedChecksum: checksum,
    preparedStateRevision: revision,
    request: { storageId },
    state: "prepared",
    storageId,
    storageStateBefore,
  };
}

function provisionedStorage(storageId = "storage-1") {
  return {
    storageId,
    sessionId: "session-1",
    backendId: "backend-ext4",
    filesystemId: "filesystem-1",
    imagePath: `/var/lib/portable-codex/${storageId}.img`,
    lifecycle: "provisioned",
    revision: "1",
    writerEpoch: "0",
    writerAuthority: null,
    mount: {
      mountPath: `/var/lib/portable-codex/${storageId}`,
      imageIdentity: {
        filesystemId: "filesystem-1",
        objectIdentityScheme: "linux-dev-inode",
        objectId: "1:2",
      },
      rootIdentity: {
        filesystemId: "filesystem-1",
        objectIdentityScheme: "linux-dev-inode",
        objectId: "1:3",
      },
    },
    publicationControlIdentity: {
      filesystemId: "filesystem-1",
      objectIdentityScheme: "linux-dev-inode",
      objectId: "1:4",
    },
    dataRoot: null,
    attachment: null,
  };
}

function committedRecord(prepared, revision = "2") {
  return {
    ...prepared,
    state: "committed",
    committedStateRevision: revision,
    expectedStorage: null,
    result: { status: "created" },
    storageState: provisionedStorage(prepared.storageId),
  };
}

function canonicalJson(value) {
  if (value === null || typeof value !== "object") {
    return JSON.stringify(value);
  }
  if (Array.isArray(value)) {
    return `[${value.map(canonicalJson).join(",")}]`;
  }
  return `{${Object.keys(value)
    .sort()
    .map((key) => `${JSON.stringify(key)}:${canonicalJson(value[key])}`)
    .join(",")}}`;
}

function recordBytesAndDigest(record) {
  const bytes = Buffer.from(canonicalJson(record), "utf8");
  const sha256 = createHash("sha256")
    .update(RECORD_DOMAIN)
    .update(bytes)
    .digest("hex");
  return { bytes, sha256 };
}

function storedOperationRow(record, committedChecksum) {
  const prepared = {
    kind: record.kind,
    operationId: record.operationId,
    preparedChecksum: record.preparedChecksum,
    preparedStateRevision: record.preparedStateRevision,
    request: record.request,
    state: "prepared",
    storageId: record.storageId,
    storageStateBefore: record.storageStateBefore,
  };
  const preparedMaterial = recordBytesAndDigest(prepared);
  const committedMaterial = recordBytesAndDigest(record);
  return {
    provider_id: "filesystem-image-ext4",
    anchor_id: "host-primary",
    operation_id: record.operationId,
    record_contract_version: 1,
    state: "committed",
    kind: record.kind,
    storage_id: record.storageId,
    prepared_state_revision: record.preparedStateRevision,
    prepared_checksum: record.preparedChecksum,
    prepared_record_bytes: preparedMaterial.bytes,
    prepared_record_sha256: preparedMaterial.sha256,
    committed_state_revision: record.committedStateRevision,
    committed_checksum_provenance: "indexed-frame-v1",
    committed_checksum: committedChecksum,
    committed_record_bytes: committedMaterial.bytes,
    committed_record_sha256: committedMaterial.sha256,
    adoption_id: null,
  };
}

function completeProvisionAdoptionFixture(operationCount, payload = null) {
  const fixture = createFixture();
  const operations = [];
  const storages = [];
  for (let index = 0; index < operationCount; index += 1) {
    const ordinal = String(index + 1).padStart(3, "0");
    const storageId = `storage-${ordinal}`;
    const prepared = {
      ...preparedRecord({
        operationId: `operation-${ordinal}`,
        revision: String(index * 2 + 1),
        storageId,
      }),
      request:
        payload === null ? { storageId } : { payload, storageId },
    };
    const committed = {
      ...committedRecord(prepared, String(index * 2 + 2)),
      result:
        payload === null
          ? { status: "created" }
          : { payload, status: "created" },
    };
    operations.push(committed);
    storages.push({
      currentAttachmentOriginOperationId: null,
      storage: committed.storageState,
    });
  }
  const eventCount = operationCount * 2;
  const expectedHead = {
    ...GENESIS,
    anchorRevision: String(eventCount),
    stateRevision: String(eventCount),
    frameCount: eventCount,
    lastChecksum: "c".repeat(64),
    ledgerBytes: eventCount,
  };
  return {
    ...fixture,
    expectedHead,
    operations,
    request: {
      expectedHead,
      nextHead: adoptionHead(expectedHead),
      operations,
      storages,
    },
  };
}

function identityKey(providerId, anchorId) {
  return `${providerId}\0${anchorId}`;
}

function operationKey(providerId, anchorId, operationId) {
  return `${providerId}\0${anchorId}\0${operationId}`;
}

function copyHeadRow(row) {
  return { ...row };
}

function headRow(
  head,
  {
    adoptionId = null,
    adoptionXid = null,
    operationIndexStateRevision = head.stateRevision,
  } = {},
) {
  return {
    provider_id: "filesystem-image-ext4",
    anchor_id: "host-primary",
    contract_version: head.contractVersion,
    anchor_revision: head.anchorRevision,
    generation: head.generation,
    state_revision: head.stateRevision,
    base_head_checksum: head.baseHeadChecksum,
    checkpoint_state_revision: head.checkpointStateRevision,
    checkpoint_frame_count: String(head.checkpointFrameCount),
    checkpoint_checksum: head.checkpointChecksum,
    checkpoint_bytes: String(head.checkpointBytes),
    frame_count: String(head.frameCount),
    last_checksum: head.lastChecksum,
    ledger_bytes: String(head.ledgerBytes),
    operation_index_state_revision: operationIndexStateRevision,
    operation_index_adoption_id: adoptionId,
    operation_index_adoption_xid: adoptionXid,
  };
}

function copyOperationRow(row) {
  return {
    ...row,
    prepared_record_bytes: Buffer.from(row.prepared_record_bytes),
    committed_record_bytes:
      row.committed_record_bytes === null
        ? null
        : Buffer.from(row.committed_record_bytes),
  };
}

function cloneMap(source, copy) {
  return new Map([...source].map(([key, value]) => [key, copy(value)]));
}

function result(command, rows = []) {
  return { command, rowCount: rows.length, rows };
}

class FakeAuthorityDatabase {
  constructor() {
    this.heads = new Map();
    this.operations = new Map();
    this.failCommitOnce = false;
    this.failCommitBeforeDurabilityOnce = false;
    this.afterCommitOnce = null;
    this.forceHeadCasMissOnce = false;
    this.forceOperationMutationMissOnce = false;
    this.operationReadOverride = null;
    this.queries = [];
    this.releaseCalls = [];
  }

  createPool() {
    const database = this;
    return {
      async connect() {
        return new FakeAuthorityClient(database);
      },
    };
  }
}

class FakeAuthorityClient {
  constructor(database) {
    this.connection = new EventEmitter();
    this.database = database;
    this.heads = null;
    this.operations = null;
  }

  async query(...args) {
    const text = typeof args[0] === "string" ? args[0] : args[0].text;
    const values = typeof args[0] === "string" ? args[1] : args[0].values;
    this.database.queries.push([text, values]);

    if (text === "DISCARD ALL") return result("DISCARD");
    if (text.startsWith("BEGIN ")) {
      this.heads = cloneMap(this.database.heads, copyHeadRow);
      this.operations = cloneMap(this.database.operations, copyOperationRow);
      return result("BEGIN");
    }
    if (
      text.startsWith(
        "SELECT pg_catalog.transaction_timestamp() AS transaction_timestamp",
      )
    ) {
      return result("SELECT", [
        {
          transaction_timestamp: "2026-08-20T12:00:00.000Z",
          transaction_id: "1",
        },
      ]);
    }
    if (
      text ===
      "SELECT pg_catalog.pg_current_xact_id()::pg_catalog.text AS transaction_id"
    ) {
      return result("SELECT", [{ transaction_id: "1" }]);
    }
    if (text === "SET LOCAL synchronous_commit = on") return result("SET");
    if (text === "ROLLBACK") {
      this.heads = null;
      this.operations = null;
      return result("ROLLBACK");
    }
    if (text === "COMMIT") {
      if (this.database.failCommitBeforeDurabilityOnce) {
        this.database.failCommitBeforeDurabilityOnce = false;
        throw new Error("commit outcome unavailable before durability");
      }
      this.database.heads = this.heads;
      this.database.operations = this.operations;
      if (this.database.afterCommitOnce !== null) {
        const mutate = this.database.afterCommitOnce;
        this.database.afterCommitOnce = null;
        mutate(this.database);
      }
      if (this.database.failCommitOnce) {
        this.database.failCommitOnce = false;
        throw new Error("commit acknowledgement lost");
      }
      return result("COMMIT");
    }
    if (text.includes("session_authority.filesystem_image_provider_heads")) {
      return this.#headQuery(text, values);
    }
    if (
      text.includes("session_authority.filesystem_image_provider_operations")
    ) {
      return this.#operationQuery(text, values);
    }
    throw new Error(`unexpected fake query: ${text}`);
  }

  #headQuery(text, values) {
    const identity = identityKey(values[0], values[1]);
    if (text.startsWith("SELECT ")) {
      const stored = this.heads.get(identity);
      return result(
        "SELECT",
        stored === undefined ? [] : [copyHeadRow(stored)],
      );
    }
    if (this.database.forceHeadCasMissOnce) {
      this.database.forceHeadCasMissOnce = false;
      return result(text.startsWith("INSERT ") ? "INSERT" : "UPDATE");
    }
    if (text.startsWith("INSERT ")) {
      if (this.heads.has(identity)) return result("INSERT");
      const stored = {
        provider_id: values[0],
        anchor_id: values[1],
        contract_version: values[2],
        anchor_revision: values[3],
        generation: values[4],
        state_revision: values[5],
        base_head_checksum: values[6],
        checkpoint_state_revision: values[7],
        checkpoint_frame_count: values[8],
        checkpoint_checksum: values[9],
        checkpoint_bytes: values[10],
        frame_count: values[11],
        last_checksum: values[12],
        ledger_bytes: values[13],
        operation_index_state_revision: values[14],
        operation_index_adoption_id: null,
        operation_index_adoption_xid: null,
      };
      this.heads.set(identity, stored);
      return result("INSERT", [copyHeadRow(stored)]);
    }
    if (text.includes("operation_index_adoption_id = $16")) {
      const stored = this.heads.get(identity);
      if (
        stored === undefined ||
        stored.contract_version !== values[16] ||
        stored.anchor_revision !== values[17] ||
        stored.generation !== values[18] ||
        stored.state_revision !== values[19] ||
        stored.base_head_checksum !== values[20] ||
        stored.checkpoint_state_revision !== values[21] ||
        stored.checkpoint_frame_count !== values[22] ||
        stored.checkpoint_checksum !== values[23] ||
        stored.checkpoint_bytes !== values[24] ||
        stored.frame_count !== values[25] ||
        stored.last_checksum !== values[26] ||
        stored.ledger_bytes !== values[27] ||
        stored.operation_index_state_revision !== values[28] ||
        stored.operation_index_adoption_id !== null ||
        stored.operation_index_adoption_xid !== null
      ) {
        return result("UPDATE");
      }
      const updated = {
        provider_id: values[0],
        anchor_id: values[1],
        contract_version: values[2],
        anchor_revision: values[3],
        generation: values[4],
        state_revision: values[5],
        base_head_checksum: values[6],
        checkpoint_state_revision: values[7],
        checkpoint_frame_count: values[8],
        checkpoint_checksum: values[9],
        checkpoint_bytes: values[10],
        frame_count: values[11],
        last_checksum: values[12],
        ledger_bytes: values[13],
        operation_index_state_revision: values[14],
        operation_index_adoption_id: values[15],
        operation_index_adoption_xid: "1",
      };
      this.heads.set(identity, updated);
      return result("UPDATE", [copyHeadRow(updated)]);
    }
    const stored = this.heads.get(identity);
    if (
      stored === undefined ||
      stored.contract_version !== values[15] ||
      stored.anchor_revision !== values[16] ||
      stored.generation !== values[17] ||
      stored.state_revision !== values[18] ||
      stored.base_head_checksum !== values[19] ||
      stored.checkpoint_state_revision !== values[20] ||
      stored.checkpoint_frame_count !== values[21] ||
      stored.checkpoint_checksum !== values[22] ||
      stored.checkpoint_bytes !== values[23] ||
      stored.frame_count !== values[24] ||
      stored.last_checksum !== values[25] ||
      stored.ledger_bytes !== values[26] ||
      stored.operation_index_state_revision !== values[18]
    ) {
      return result("UPDATE");
    }
    const updated = {
      provider_id: values[0],
      anchor_id: values[1],
      contract_version: values[2],
      anchor_revision: values[3],
      generation: values[4],
      state_revision: values[5],
      base_head_checksum: values[6],
      checkpoint_state_revision: values[7],
      checkpoint_frame_count: values[8],
      checkpoint_checksum: values[9],
      checkpoint_bytes: values[10],
      frame_count: values[11],
      last_checksum: values[12],
      ledger_bytes: values[13],
      operation_index_state_revision: values[14],
      operation_index_adoption_id: stored.operation_index_adoption_id,
      operation_index_adoption_xid: stored.operation_index_adoption_xid,
    };
    this.heads.set(identity, updated);
    return result("UPDATE", [copyHeadRow(updated)]);
  }

  #operationQuery(text, values) {
    if (text.startsWith("SELECT ")) {
      if (this.database.operationReadOverride !== null) {
        const overridden = this.database.operationReadOverride;
        this.database.operationReadOverride = null;
        return overridden;
      }
      if (text.includes("operation_id = $3")) {
        const stored = this.operations.get(
          operationKey(values[0], values[1], values[2]),
        );
        return result(
          "SELECT",
          stored === undefined ? [] : [copyOperationRow(stored)],
        );
      }
      if (text.includes("storage_id = $3")) {
        const rows = [...this.operations.values()]
          .filter(
            (row) =>
              row.provider_id === values[0] &&
              row.anchor_id === values[1] &&
              row.storage_id === values[2] &&
              row.state === "committed",
          )
          .sort((left, right) => {
            const revisionOrder =
              BigInt(right.committed_state_revision) -
              BigInt(left.committed_state_revision);
            if (revisionOrder < 0n) return -1;
            if (revisionOrder > 0n) return 1;
            return left.operation_id < right.operation_id
              ? 1
              : left.operation_id > right.operation_id
                ? -1
                : 0;
          })
          .slice(0, 1)
          .map(copyOperationRow);
        return result("SELECT", rows);
      }
      const after = text.includes("operation_id COLLATE") && values.length === 4
        ? values[2]
        : text.includes("storage_id COLLATE") && values.length === 4
          ? values[2]
          : null;
      const maximumRows = Number(values[values.length - 1]);
      const rows = [...this.operations.values()]
        .filter(
          (row) =>
            row.provider_id === values[0] &&
            row.anchor_id === values[1] &&
            (!text.includes("state = 'prepared'") || row.state === "prepared") &&
            (after === null ||
              (text.includes("storage_id COLLATE")
                ? row.storage_id > after
                : row.operation_id > after)),
        )
        .sort((left, right) =>
          (text.includes("storage_id COLLATE")
            ? left.storage_id
            : left.operation_id) <
          (text.includes("storage_id COLLATE")
            ? right.storage_id
            : right.operation_id)
            ? -1
            : (text.includes("storage_id COLLATE")
                  ? left.storage_id
                  : left.operation_id) >
                (text.includes("storage_id COLLATE")
                  ? right.storage_id
                  : right.operation_id)
              ? 1
              : 0,
        )
        .slice(0, maximumRows)
        .map(copyOperationRow);
      return result("SELECT", rows);
    }
    if (this.database.forceOperationMutationMissOnce) {
      this.database.forceOperationMutationMissOnce = false;
      return result(text.startsWith("INSERT ") ? "INSERT" : "UPDATE");
    }
    if (text.startsWith("INSERT ")) {
      const adopted = text.includes("committed_record_sha256, adoption_id)");
      const width = adopted ? 17 : 11;
      assert.equal(values.length % width, 0);
      const rows = [];
      for (let offset = 0; offset < values.length; offset += width) {
        const key = operationKey(
          values[offset],
          values[offset + 1],
          values[offset + 2],
        );
        if (this.operations.has(key)) continue;
        const stored = {
          provider_id: values[offset],
          anchor_id: values[offset + 1],
          operation_id: values[offset + 2],
          record_contract_version: values[offset + 3],
          state: values[offset + 4],
          kind: values[offset + 5],
          storage_id: values[offset + 6],
          prepared_state_revision: values[offset + 7],
          prepared_checksum: values[offset + 8],
          prepared_record_bytes: Buffer.from(values[offset + 9], "hex"),
          prepared_record_sha256: values[offset + 10],
          committed_state_revision: adopted ? values[offset + 11] : null,
          committed_checksum_provenance: adopted
            ? values[offset + 12]
            : null,
          committed_checksum: adopted ? values[offset + 13] : null,
          committed_record_bytes:
            adopted && values[offset + 14] !== null
              ? Buffer.from(values[offset + 14], "hex")
              : null,
          committed_record_sha256: adopted ? values[offset + 15] : null,
          adoption_id: adopted ? values[offset + 16] : null,
        };
        this.operations.set(key, stored);
        rows.push(copyOperationRow(stored));
      }
      return result("INSERT", rows);
    }
    const key = operationKey(values[0], values[1], values[2]);
    const stored = this.operations.get(key);
    if (
      stored === undefined ||
      stored.record_contract_version !== values[3] ||
      stored.state !== values[4] ||
      stored.kind !== values[5] ||
      stored.storage_id !== values[6] ||
      stored.prepared_state_revision !== values[7] ||
      stored.prepared_checksum !== values[8] ||
      !stored.prepared_record_bytes.equals(Buffer.from(values[9], "hex")) ||
      stored.prepared_record_sha256 !== values[10] ||
      stored.committed_state_revision !== null ||
      stored.committed_checksum_provenance !== null ||
      stored.committed_checksum !== null ||
      stored.committed_record_bytes !== null ||
      stored.committed_record_sha256 !== null
    ) {
      return result("UPDATE");
    }
    const updated = {
      ...stored,
      state: values[11],
      committed_state_revision: values[12],
      committed_checksum_provenance: values[13],
      committed_checksum: values[14],
      committed_record_bytes: Buffer.from(values[15], "hex"),
      committed_record_sha256: values[16],
      adoption_id: stored.adoption_id,
    };
    this.operations.set(key, updated);
    return result("UPDATE", [copyOperationRow(updated)]);
  }

  async release(...args) {
    this.database.releaseCalls.push(args);
  }
}

function createFixture() {
  const database = new FakeAuthorityDatabase();
  const store = new PostgresSerializableStore({
    dedicatedPool: database.createPool(),
    maxTransactionAttempts: 1,
  });
  const createAuthority = (overrides = {}) =>
    createPostgresFilesystemImageProviderStateAuthority({
      store,
      providerId: "filesystem-image-ext4",
      anchorId: "host-primary",
      ...overrides,
    });
  const createRuntimeAuthority = (overrides = {}) =>
    createPostgresFilesystemImageProviderStateRuntimeAuthority({
      store,
      providerId: "filesystem-image-ext4",
      anchorId: "host-primary",
      ...overrides,
    });
  const createAdoptionAuthority = (overrides = {}) =>
    createPostgresFilesystemImageProviderStateAdoptionAuthority({
      store,
      providerId: "filesystem-image-ext4",
      anchorId: "host-primary",
      ...overrides,
    });
  return {
    adoptionAuthority: createAdoptionAuthority(),
    authority: createAuthority(),
    createAdoptionAuthority,
    createAuthority,
    createRuntimeAuthority,
    database,
    runtimeAuthority: createRuntimeAuthority(),
    store,
  };
}

function authorityError(code) {
  return (error) =>
    error instanceof PostgresFilesystemImageProviderStateAuthorityError &&
    error.code === code &&
    error.retryable === false &&
    Object.isFrozen(error);
}

async function appendPrepared(
  authority,
  expectedHead,
  {
    checksum = "a".repeat(64),
    ledgerBytes = expectedHead.ledgerBytes + 128,
    operationId = "operation-1",
    storageId = "storage-1",
  } = {},
) {
  const nextHead = appendHead(expectedHead, checksum, ledgerBytes);
  const record = preparedRecord({
    checksum,
    operationId,
    revision: nextHead.stateRevision,
    storageId,
  });
  const advanced = await authority.compareAndAdvance({
    expectedHead,
    nextHead,
    transition: {
      contractVersion: 1,
      type: "append-prepared-v1",
      frameChecksum: checksum,
      record,
    },
  });
  return { advanced, nextHead, record };
}

async function appendCommitted(
  authority,
  expectedHead,
  prepared,
  {
    checksum = "b".repeat(64),
    ledgerBytes = expectedHead.ledgerBytes + 192,
  } = {},
) {
  const nextHead = appendHead(expectedHead, checksum, ledgerBytes);
  const record = committedRecord(prepared, nextHead.stateRevision);
  const advanced = await authority.compareAndAdvance({
    expectedHead,
    nextHead,
    transition: {
      contractVersion: 1,
      type: "append-committed-v1",
      frameChecksum: checksum,
      record,
    },
  });
  return { advanced, nextHead, record };
}

async function fixtureWithCommittedProvision() {
  const fixture = createFixture();
  const prepared = await appendPrepared(fixture.authority, GENESIS);
  const committed = await appendCommitted(
    fixture.authority,
    prepared.nextHead,
    prepared.record,
  );
  assert.equal(committed.advanced, true);
  return { ...fixture, committed, prepared };
}

test("exposes a frozen exact receiver-independent authority surface", async () => {
  const { authority, createAuthority, database, store } = createFixture();
  assert.equal(
    POSTGRES_FILESYSTEM_IMAGE_PROVIDER_STATE_AUTHORITY_CONTRACT_VERSION,
    1,
  );
  assert.deepEqual(Reflect.ownKeys(authority), [
    "contractVersion",
    "readHead",
    "readOperation",
    "readOperationsPage",
    "compareAndAdvance",
  ]);
  assert.equal(authority.contractVersion, 1);
  assert.equal(Object.isFrozen(authority), true);
  for (const method of [
    authority.readHead,
    authority.readOperation,
    authority.readOperationsPage,
    authority.compareAndAdvance,
  ]) {
    assert.equal(Object.isFrozen(method), true);
  }

  const promise = Reflect.apply(authority.readHead, { hostile: true }, []);
  assert.equal(Object.getPrototypeOf(promise), Promise.prototype);
  assert.deepEqual(await promise, GENESIS);
  assert.equal(Object.isFrozen(await authority.readHead()), true);
  assert.equal(
    await authority.readOperation({
      expectedHead: GENESIS,
      operationId: "operation-missing",
    }),
    null,
  );
  assert.deepEqual(
    await authority.readOperationsPage({
      afterOperationId: null,
      expectedHead: GENESIS,
      limit: 1,
    }),
    { operations: [], nextAfterOperationId: null },
  );
  assert.equal(database.operations.size, 0);
  assert.equal(
    database.queries.some(
      ([text]) => text.includes("filesystem_image_provider_operations"),
    ),
    false,
  );

  assert.throws(
    () =>
      createPostgresFilesystemImageProviderStateAuthority({
        store,
        providerId: "filesystem-image-ext4",
        anchorId: "host-primary",
        extra: true,
      }),
    authorityError(
      "invalid_postgres_filesystem_image_provider_state_authority_options",
    ),
  );
  assert.throws(
    () => createAuthority({ providerId: "bad/id" }),
    authorityError(
      "invalid_postgres_filesystem_image_provider_state_authority_options",
    ),
  );
});

test("exposes distinct frozen runtime and adoption capabilities", async () => {
  const { adoptionAuthority, runtimeAuthority } = createFixture();
  assert.equal(
    POSTGRES_FILESYSTEM_IMAGE_PROVIDER_STATE_RUNTIME_AUTHORITY_CONTRACT_VERSION,
    2,
  );
  assert.deepEqual(Reflect.ownKeys(runtimeAuthority), [
    "contractVersion",
    "readHead",
    "readOperation",
    "readOperationsPage",
    "readPreparedOperationsPage",
    "compareAndAdvance",
  ]);
  assert.equal(runtimeAuthority.contractVersion, 2);
  assert.equal(Object.isFrozen(runtimeAuthority), true);
  assert.deepEqual(await runtimeAuthority.readHead(), V3_GENESIS);
  assert.deepEqual(
    await runtimeAuthority.readPreparedOperationsPage({
      afterStorageId: null,
      expectedHead: V3_GENESIS,
      limit: 4,
    }),
    { operations: [], nextAfterStorageId: null },
  );

  assert.equal(
    POSTGRES_FILESYSTEM_IMAGE_PROVIDER_STATE_ADOPTION_AUTHORITY_CONTRACT_VERSION,
    1,
  );
  assert.deepEqual(Reflect.ownKeys(adoptionAuthority), [
    "contractVersion",
    "compareAndAdopt",
  ]);
  assert.equal(adoptionAuthority.contractVersion, 1);
  assert.equal(Object.isFrozen(adoptionAuthority), true);
  assert.equal(Object.isFrozen(adoptionAuthority.compareAndAdopt), true);
});

test("legacy authority rejects stored v3 heads while runtime dual-reads v2 and v3", async () => {
  const { authority, database, runtimeAuthority } = createFixture();
  const v2Head = appendHead(GENESIS, "a".repeat(64), 128);
  database.heads.set(
    identityKey("filesystem-image-ext4", "host-primary"),
    headRow(v2Head),
  );
  assert.deepEqual(await authority.readHead(), v2Head);
  assert.deepEqual(await runtimeAuthority.readHead(), v2Head);

  const v3Head = appendHead(V3_GENESIS, "b".repeat(64), 128);
  database.heads.set(
    identityKey("filesystem-image-ext4", "host-primary"),
    headRow(v3Head),
  );
  await assert.rejects(
    authority.readHead(),
    authorityError(
      "postgres_filesystem_image_provider_state_authority_state_invalid",
    ),
  );
  assert.deepEqual(await runtimeAuthority.readHead(), v3Head);
});

test("runtime authority pages prepared operations by C storage id", async () => {
  const { runtimeAuthority } = createFixture();
  let head = V3_GENESIS;
  for (const [index, [operationId, storageId]] of [
    ["operation-c", "storage-c"],
    ["operation-a", "storage-a"],
    ["operation-b", "storage-b"],
  ].entries()) {
    const appended = await appendPrepared(runtimeAuthority, head, {
      checksum: String.fromCharCode(97 + index).repeat(64),
      ledgerBytes: 128 * (index + 1),
      operationId,
      storageId,
    });
    head = appended.nextHead;
  }
  const first = await runtimeAuthority.readPreparedOperationsPage({
    afterStorageId: null,
    expectedHead: head,
    limit: 2,
  });
  assert.deepEqual(
    first.operations.map(({ storageId }) => storageId),
    ["storage-a", "storage-b"],
  );
  assert.equal(first.nextAfterStorageId, "storage-b");
  const second = await runtimeAuthority.readPreparedOperationsPage({
    afterStorageId: first.nextAfterStorageId,
    expectedHead: head,
    limit: 2,
  });
  assert.deepEqual(
    second.operations.map(({ storageId }) => storageId),
    ["storage-c"],
  );
  assert.equal(second.nextAfterStorageId, null);
});

function legacyAdoptionFixture() {
  const fixture = createFixture();
  const preparedHead = appendHead(GENESIS, "a".repeat(64), 128);
  const expectedHead = appendHead(preparedHead, "b".repeat(64), 320);
  const prepared = preparedRecord();
  const committed = committedRecord(prepared, expectedHead.stateRevision);
  const nextHead = adoptionHead(expectedHead);
  fixture.database.heads.set(
    identityKey("filesystem-image-ext4", "host-primary"),
    headRow(expectedHead, { operationIndexStateRevision: null }),
  );
  const request = {
    expectedHead,
    nextHead,
    operations: [committed],
    storages: [
      {
        currentAttachmentOriginOperationId: null,
        storage: committed.storageState,
      },
    ],
  };
  return { ...fixture, committed, expectedHead, nextHead, request };
}

function preparedBatchAdoptionFixture(operationCount = 65) {
  const fixture = createFixture();
  const operations = Array.from({ length: operationCount }, (_, index) => {
    const ordinal = String(index + 1).padStart(3, "0");
    return preparedRecord({
      operationId: `operation-${ordinal}`,
      revision: String(index + 1),
      storageId: `storage-${ordinal}`,
    });
  });
  const expectedHead = {
    ...GENESIS,
    anchorRevision: String(operationCount),
    stateRevision: String(operationCount),
    frameCount: operationCount,
    lastChecksum: "a".repeat(64),
    ledgerBytes: operationCount,
  };
  const nextHead = {
    ...adoptionHead(expectedHead),
    checkpointFrameCount: operationCount + 2,
  };
  fixture.database.heads.set(
    identityKey("filesystem-image-ext4", "host-primary"),
    headRow(expectedHead, { operationIndexStateRevision: null }),
  );
  return {
    ...fixture,
    operations,
    request: { expectedHead, nextHead, operations, storages: [] },
  };
}

test("adoption rejects active and revoked Proxy arrays without traps or SQL", async () => {
  const fixture = legacyAdoptionFixture();
  let trapCalls = 0;
  const operations = new Proxy(fixture.request.operations, {
    getPrototypeOf() {
      trapCalls += 1;
      return Array.prototype;
    },
  });
  await assert.rejects(
    fixture.adoptionAuthority.compareAndAdopt({
      ...fixture.request,
      operations,
    }),
    authorityError(
      "invalid_postgres_filesystem_image_provider_state_authority_request",
    ),
  );
  assert.equal(trapCalls, 0);

  const revokedStorages = Proxy.revocable(fixture.request.storages, {});
  revokedStorages.revoke();
  await assert.rejects(
    fixture.adoptionAuthority.compareAndAdopt({
      ...fixture.request,
      storages: revokedStorages.proxy,
    }),
    authorityError(
      "invalid_postgres_filesystem_image_provider_state_authority_request",
    ),
  );
  assert.equal(fixture.database.queries.length, 0);
});

test("adoption bounds one shared canonical-material budget before SQL", async () => {
  const fixture = completeProvisionAdoptionFixture(
    32,
    "x".repeat(700 * 1024),
  );
  await assert.rejects(
    fixture.adoptionAuthority.compareAndAdopt(fixture.request),
    authorityError(
      "invalid_postgres_filesystem_image_provider_state_authority_request",
    ),
  );
  assert.equal(fixture.database.queries.length, 0);
});

test("adoption bounds canonical material while rereading all database rows", async () => {
  const fixture = completeProvisionAdoptionFixture(32);
  fixture.database.heads.set(
    identityKey("filesystem-image-ext4", "host-primary"),
    headRow(fixture.expectedHead, { operationIndexStateRevision: null }),
  );
  const payload = "y".repeat(700 * 1024);
  for (let index = 0; index < fixture.operations.length; index += 1) {
    const original = fixture.operations[index];
    const storedRecord = {
      ...original,
      request: { payload, storageId: original.storageId },
      result: { payload, status: "created" },
    };
    fixture.database.operations.set(
      operationKey(
        "filesystem-image-ext4",
        "host-primary",
        storedRecord.operationId,
      ),
      storedOperationRow(storedRecord, fixture.expectedHead.lastChecksum),
    );
  }
  await assert.rejects(
    fixture.adoptionAuthority.compareAndAdopt(fixture.request),
    authorityError(
      "postgres_filesystem_image_provider_state_authority_state_invalid",
    ),
  );
  assert.notEqual(fixture.database.queries.length, 0);
});

test("atomically adopts complete legacy history and is idempotent", async () => {
  const fixture = legacyAdoptionFixture();
  assert.equal(
    await fixture.adoptionAuthority.compareAndAdopt(fixture.request),
    true,
  );
  const storedHead = fixture.database.heads.get(
    identityKey("filesystem-image-ext4", "host-primary"),
  );
  assert.equal(storedHead.contract_version, 3);
  assert.equal(storedHead.operation_index_state_revision, "2");
  assert.equal(
    storedHead.operation_index_adoption_id,
    "c1eadcd18d4000dc400405bb4b9fc0aeb2cbab4b1578b75bb9fcd841ed91c598",
  );
  assert.equal(storedHead.operation_index_adoption_xid, "1");
  const storedOperation = fixture.database.operations.get(
    operationKey("filesystem-image-ext4", "host-primary", "operation-1"),
  );
  assert.equal(
    storedOperation.committed_checksum_provenance,
    "unavailable-adopted-v2",
  );
  assert.equal(storedOperation.committed_checksum, null);
  assert.equal(
    storedOperation.adoption_id,
    storedHead.operation_index_adoption_id,
  );
  assert.equal(
    await fixture.adoptionAuthority.compareAndAdopt(fixture.request),
    true,
  );
  assert.deepEqual(
    await fixture.runtimeAuthority.readOperation({
      expectedHead: fixture.nextHead,
      operationId: "operation-1",
    }),
    fixture.committed,
  );
});

test(
  "adoption idempotency does not consult a poisoned Array iterator",
  { concurrency: false },
  async () => {
    const fixture = legacyAdoptionFixture();
    assert.equal(
      await fixture.adoptionAuthority.compareAndAdopt(fixture.request),
      true,
    );
    const descriptor = Object.getOwnPropertyDescriptor(
      Array.prototype,
      Symbol.iterator,
    );
    assert.equal(typeof descriptor?.value, "function");
    let poisonCalls = 0;
    Object.defineProperty(Array.prototype, Symbol.iterator, {
      ...descriptor,
      value() {
        if (
          this.length === 2 &&
          this[0] === "legacy" &&
          this[1] === "indexed"
        ) {
          poisonCalls += 1;
          throw new Error("poisoned adoption mode iterator");
        }
        return Reflect.apply(descriptor.value, this, []);
      },
    });
    try {
      assert.equal(
        await fixture.adoptionAuthority.compareAndAdopt(fixture.request),
        true,
      );
    } finally {
      Object.defineProperty(Array.prototype, Symbol.iterator, descriptor);
    }
    assert.equal(poisonCalls, 0);
  },
);

test("adoption imports fixed-size batches and rejects a cross-batch revision hole before SQL", async () => {
  const fixture = preparedBatchAdoptionFixture();
  assert.equal(
    await fixture.adoptionAuthority.compareAndAdopt(fixture.request),
    true,
  );
  const insertQueries = fixture.database.queries.filter(
    ([text]) =>
      text.startsWith(
        "INSERT INTO session_authority.filesystem_image_provider_operations",
      ) && text.includes("committed_record_sha256, adoption_id)"),
  );
  const insertParameterCounts = insertQueries.map(
    ([, values]) => values.length,
  );
  assert.deepEqual(insertParameterCounts, [64 * 17, 17]);
  for (const [text, values] of insertQueries) {
    const placeholders = Array.from(
      text.matchAll(/\$(\d+)/gu),
      (match) => Number(match[1]),
    );
    const placeholderCounts = new Map();
    for (const placeholder of placeholders) {
      placeholderCounts.set(
        placeholder,
        (placeholderCounts.get(placeholder) ?? 0) + 1,
      );
    }
    assert.deepEqual(
      Array.from(placeholderCounts),
      Array.from({ length: values.length }, (_, index) => [
        index + 1,
        index % 17 === 14 ? 2 : 1,
      ]),
    );
  }
  assert.equal(fixture.database.operations.size, 65);

  const invalid = preparedBatchAdoptionFixture();
  const operations = invalid.operations.map((record, index) =>
    index === 63 ? { ...record, preparedStateRevision: "66" } : record,
  );
  await assert.rejects(
    invalid.adoptionAuthority.compareAndAdopt({
      ...invalid.request,
      operations,
    }),
    authorityError(
      "invalid_postgres_filesystem_image_provider_state_authority_request",
    ),
  );
  assert.equal(invalid.database.queries.length, 0);
});

test("adopts an already indexed v2 source without rewriting rows", async () => {
  const fixture = createFixture();
  const prepared = await appendPrepared(fixture.authority, GENESIS);
  const committed = await appendCommitted(
    fixture.authority,
    prepared.nextHead,
    prepared.record,
  );
  const before = copyOperationRow(
    fixture.database.operations.get(
      operationKey("filesystem-image-ext4", "host-primary", "operation-1"),
    ),
  );
  const nextHead = adoptionHead(committed.nextHead);
  assert.equal(
    await fixture.adoptionAuthority.compareAndAdopt({
      expectedHead: committed.nextHead,
      nextHead,
      operations: [committed.record],
      storages: [
        {
          currentAttachmentOriginOperationId: null,
          storage: committed.record.storageState,
        },
      ],
    }),
    true,
  );
  assert.deepEqual(
    fixture.database.operations.get(
      operationKey("filesystem-image-ext4", "host-primary", "operation-1"),
    ),
    before,
  );
});

test("adoption rejects incomplete replay before SQL and returns false when stale", async () => {
  const fixture = legacyAdoptionFixture();
  await assert.rejects(
    fixture.adoptionAuthority.compareAndAdopt({
      ...fixture.request,
      operations: [
        {
          ...fixture.committed,
          committedStateRevision: "3",
        },
      ],
    }),
    authorityError(
      "invalid_postgres_filesystem_image_provider_state_authority_request",
    ),
  );
  assert.equal(fixture.database.queries.length, 0);
  fixture.database.heads.set(
    identityKey("filesystem-image-ext4", "host-primary"),
    headRow({
      ...fixture.expectedHead,
      lastChecksum: "f".repeat(64),
    }),
  );
  assert.equal(
    await fixture.adoptionAuthority.compareAndAdopt(fixture.request),
    false,
  );
});

test("adoption rejects a stored-genesis transition before SQL", async () => {
  const fixture = createFixture();
  await assert.rejects(
    fixture.adoptionAuthority.compareAndAdopt({
      expectedHead: GENESIS,
      nextHead: adoptionHead(GENESIS),
      operations: [],
      storages: [],
    }),
    authorityError(
      "invalid_postgres_filesystem_image_provider_state_authority_request",
    ),
  );
  assert.equal(fixture.database.queries.length, 0);
});

test("adoption resolves a lost commit acknowledgement by exact readback", async () => {
  const fixture = legacyAdoptionFixture();
  fixture.database.failCommitOnce = true;
  assert.equal(
    await fixture.adoptionAuthority.compareAndAdopt(fixture.request),
    true,
  );
  assert.deepEqual(await fixture.runtimeAuthority.readHead(), fixture.nextHead);
});

test("adoption resolves an uncertain unchanged source as stale", async () => {
  const fixture = legacyAdoptionFixture();
  fixture.database.failCommitBeforeDurabilityOnce = true;
  assert.equal(
    await fixture.adoptionAuthority.compareAndAdopt(fixture.request),
    false,
  );
  assert.deepEqual(
    fixture.database.heads.get(
      identityKey("filesystem-image-ext4", "host-primary"),
    ),
    headRow(fixture.expectedHead, { operationIndexStateRevision: null }),
  );
  assert.equal(fixture.database.operations.size, 0);
});

test("adoption readback rejects a manifest and provenance mode mismatch", async () => {
  const fixture = legacyAdoptionFixture();
  assert.equal(
    await fixture.adoptionAuthority.compareAndAdopt(fixture.request),
    true,
  );
  const row = fixture.database.operations.get(
    operationKey("filesystem-image-ext4", "host-primary", "operation-1"),
  );
  row.adoption_id = null;
  row.committed_checksum_provenance = "indexed-frame-v1";
  row.committed_checksum = "c".repeat(64);
  await assert.rejects(
    fixture.adoptionAuthority.compareAndAdopt(fixture.request),
    authorityError(
      "postgres_filesystem_image_provider_state_authority_state_invalid",
    ),
  );
});

test("adoption ACK readback classifies a cross-mode target as uncertain", async () => {
  const fixture = legacyAdoptionFixture();
  fixture.database.afterCommitOnce = (database) => {
    const row = database.operations.get(
      operationKey("filesystem-image-ext4", "host-primary", "operation-1"),
    );
    row.adoption_id = null;
    row.committed_checksum_provenance = "indexed-frame-v1";
    row.committed_checksum = "c".repeat(64);
  };
  fixture.database.failCommitOnce = true;
  await assert.rejects(
    fixture.adoptionAuthority.compareAndAdopt(fixture.request),
    authorityError(
      "postgres_filesystem_image_provider_state_adoption_commit_outcome_uncertain",
    ),
  );
});

test("adoption ACK readback rejects a post-cut head mutation", async () => {
  const fixture = legacyAdoptionFixture();
  fixture.database.afterCommitOnce = (database) => {
    const row = database.heads.get(
      identityKey("filesystem-image-ext4", "host-primary"),
    );
    row.checkpoint_checksum = "f".repeat(64);
    row.last_checksum = row.checkpoint_checksum;
  };
  fixture.database.failCommitOnce = true;
  await assert.rejects(
    fixture.adoptionAuthority.compareAndAdopt(fixture.request),
    authorityError(
      "postgres_filesystem_image_provider_state_adoption_commit_outcome_uncertain",
    ),
  );
});

test("rejects a forged non-provision storage lineage at complete genesis without durable mutation", async () => {
  const { authority, database } = createFixture();
  const checksum = "a".repeat(64);
  const nextHead = appendHead(GENESIS, checksum, 128);
  const record = preparedRecord({
    kind: "attach",
    storageStateBefore: provisionedStorage(),
  });

  await assert.rejects(
    authority.compareAndAdvance({
      expectedHead: GENESIS,
      nextHead,
      transition: {
        contractVersion: 1,
        type: "append-prepared-v1",
        frameChecksum: checksum,
        record,
      },
    }),
    authorityError(
      "postgres_filesystem_image_provider_state_authority_state_invalid",
    ),
  );
  assert.equal(database.heads.size, 0);
  assert.equal(database.operations.size, 0);

  const headMutationIndex = database.queries.findIndex(
    ([text]) =>
      text.startsWith(
        "INSERT INTO session_authority.filesystem_image_provider_heads",
      ),
  );
  const lineageReadIndex = database.queries.findIndex(
    ([text]) =>
      text.includes("filesystem_image_provider_operations") &&
      text.includes("storage_id = $3"),
  );
  assert.ok(headMutationIndex >= 0);
  assert.ok(lineageReadIndex > headMutationIndex);
  assert.equal(
    database.queries.some(
      ([text]) =>
        text.startsWith(
          "INSERT INTO session_authority.filesystem_image_provider_operations",
        ),
    ),
    false,
  );
});

test("atomically appends prepared and committed rows with canonical bytes and the fixed digest vector", async () => {
  const { authority, database } = createFixture();
  const prepared = await appendPrepared(authority, GENESIS);
  assert.equal(prepared.advanced, true);
  const storedPrepared = database.operations.get(
    operationKey("filesystem-image-ext4", "host-primary", "operation-1"),
  );
  assert.equal(storedPrepared.state, "prepared");
  assert.equal(storedPrepared.prepared_checksum, "a".repeat(64));
  assert.equal(
    database.heads.get(
      identityKey("filesystem-image-ext4", "host-primary"),
    ).operation_index_state_revision,
    prepared.nextHead.stateRevision,
  );
  assert.equal(
    storedPrepared.prepared_record_bytes.toString("utf8"),
    '{"kind":"provision","operationId":"operation-1",' +
      '"preparedChecksum":"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa' +
      'aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa","preparedStateRevision":"1",' +
      '"request":{"storageId":"storage-1"},"state":"prepared",' +
      '"storageId":"storage-1","storageStateBefore":null}',
  );
  const independentDigest = createHash("sha256")
    .update(RECORD_DOMAIN)
    .update(storedPrepared.prepared_record_bytes)
    .digest("hex");
  assert.equal(storedPrepared.prepared_record_sha256, independentDigest);
  assert.equal(
    storedPrepared.prepared_record_sha256,
    "2f05f6a2c37e68833a3c2ff6ebeb74b194361ba91d4b2e20da6e5b8907e84be8",
  );

  const observedPrepared = await authority.readOperation({
    expectedHead: prepared.nextHead,
    operationId: "operation-1",
  });
  assert.deepEqual(observedPrepared, prepared.record);
  assert.equal(Object.isFrozen(observedPrepared), true);

  const committedChecksum = "b".repeat(64);
  const committedHead = appendHead(
    prepared.nextHead,
    committedChecksum,
    320,
  );
  const committed = committedRecord(
    prepared.record,
    committedHead.stateRevision,
  );
  assert.equal(
    await authority.compareAndAdvance({
      expectedHead: prepared.nextHead,
      nextHead: committedHead,
      transition: {
        contractVersion: 1,
        type: "append-committed-v1",
        frameChecksum: committedChecksum,
        record: committed,
      },
    }),
    true,
  );
  const storedCommitted = database.operations.get(
    operationKey("filesystem-image-ext4", "host-primary", "operation-1"),
  );
  assert.equal(storedCommitted.state, "committed");
  assert.equal(storedCommitted.committed_state_revision, "2");
  assert.equal(
    storedCommitted.committed_checksum_provenance,
    "indexed-frame-v1",
  );
  assert.equal(storedCommitted.committed_checksum, committedChecksum);
  assert.equal(
    database.heads.get(
      identityKey("filesystem-image-ext4", "host-primary"),
    ).operation_index_state_revision,
    committedHead.stateRevision,
  );
  assert.equal(
    storedCommitted.committed_record_sha256,
    createHash("sha256")
      .update(RECORD_DOMAIN)
      .update(storedCommitted.committed_record_bytes)
      .digest("hex"),
  );
  assert.deepEqual(
    await authority.readOperation({
      expectedHead: committedHead,
      operationId: "operation-1",
    }),
    committed,
  );

  const insertQuery = database.queries.find(
    ([text]) =>
      text.startsWith("INSERT INTO") &&
      text.includes("filesystem_image_provider_operations"),
  );
  assert.match(insertQuery[0], /pg_catalog\.decode\(\$10, 'hex'\)/u);
  assert.equal(typeof insertQuery[1][9], "string");
  assert.equal(
    insertQuery[1][9],
    storedPrepared.prepared_record_bytes.toString("hex"),
  );
  assert.deepEqual(insertQuery[1].slice(0, 9), [
    "filesystem-image-ext4",
    "host-primary",
    "operation-1",
    1,
    "prepared",
    "provision",
    "storage-1",
    "1",
    "a".repeat(64),
  ]);
  const updateQuery = database.queries.find(
    ([text]) =>
      text.startsWith("UPDATE session_authority.filesystem_image_provider_operations"),
  );
  assert.match(updateQuery[0], /committed_checksum_provenance = \$14/u);
  assert.match(updateQuery[0], /committed_checksum = \$15/u);
  assert.equal(updateQuery[1][13], "indexed-frame-v1");
  assert.equal(updateQuery[1][14], committedChecksum);

  const insertHeadQuery = database.queries.find(
    ([text]) =>
      text.startsWith(
        "INSERT INTO session_authority.filesystem_image_provider_heads",
      ),
  );
  const readHeadQuery = database.queries.find(
    ([text]) =>
      text.startsWith("SELECT ") &&
      text.includes("filesystem_image_provider_heads"),
  );
  assert.match(
    readHeadQuery[0],
    /operation_index_state_revision::pg_catalog\.text AS operation_index_state_revision/u,
  );
  assert.match(
    insertHeadQuery[0],
    /ledger_bytes, operation_index_state_revision\) VALUES[\s\S]+\$15::pg_catalog\.numeric\)/u,
  );
  assert.equal(insertHeadQuery[1][14], prepared.nextHead.stateRevision);
  const updateHeadQuery = database.queries.find(
    ([text]) =>
      text.startsWith(
        "UPDATE session_authority.filesystem_image_provider_heads",
      ),
  );
  assert.match(
    updateHeadQuery[0],
    /operation_index_state_revision = \$15::pg_catalog\.numeric[\s\S]+state_revision = \$19::pg_catalog\.numeric[\s\S]+operation_index_state_revision = \$19::pg_catalog\.numeric/u,
  );
  assert.equal(updateHeadQuery[1][14], committedHead.stateRevision);
  assert.equal(updateHeadQuery[1][18], prepared.nextHead.stateRevision);

  const lineageReadQueries = database.queries.filter(
    ([text]) =>
      text.includes("filesystem_image_provider_operations") &&
      text.includes("storage_id = $3"),
  );
  assert.equal(lineageReadQueries.length, 2);
  const lineageReadQuery = lineageReadQueries[0];
  assert.match(
    lineageReadQuery[0],
    /WHERE provider_id = \$1 AND anchor_id = \$2 AND storage_id = \$3 AND state = 'committed' ORDER BY committed_state_revision DESC, operation_id COLLATE pg_catalog\."C" DESC LIMIT 1/u,
  );
  assert.deepEqual(lineageReadQuery[1], [
    "filesystem-image-ext4",
    "host-primary",
    "storage-1",
  ]);
  assert.ok(
    database.queries.indexOf(insertHeadQuery) <
      database.queries.indexOf(lineageReadQueries[0]),
  );
  assert.ok(
    database.queries.indexOf(lineageReadQueries[0]) <
      database.queries.indexOf(insertQuery),
  );
  assert.ok(
    database.queries.indexOf(updateHeadQuery) <
      database.queries.indexOf(lineageReadQueries[1]),
  );
  assert.ok(
    database.queries.indexOf(lineageReadQueries[1]) <
      database.queries.indexOf(updateQuery),
  );
});

test("rolls back a repeated null-before provision after the storage has committed", async () => {
  const { authority, committed, database } =
    await fixtureWithCommittedProvision();
  const beforeHead = copyHeadRow(
    database.heads.get(identityKey("filesystem-image-ext4", "host-primary")),
  );
  const beforeOperations = cloneMap(database.operations, copyOperationRow);
  const checksum = "c".repeat(64);
  const nextHead = appendHead(committed.nextHead, checksum, 512);
  const record = preparedRecord({
    checksum,
    operationId: "operation-2",
    revision: nextHead.stateRevision,
  });

  await assert.rejects(
    authority.compareAndAdvance({
      expectedHead: committed.nextHead,
      nextHead,
      transition: {
        contractVersion: 1,
        type: "append-prepared-v1",
        frameChecksum: checksum,
        record,
      },
    }),
    authorityError(
      "postgres_filesystem_image_provider_state_authority_state_invalid",
    ),
  );
  assert.deepEqual(
    database.heads.get(identityKey("filesystem-image-ext4", "host-primary")),
    beforeHead,
  );
  assert.deepEqual(database.operations, beforeOperations);
});

test("rejects stale storage revision and physical identity lineage after the head CAS", async (t) => {
  for (const [name, staleBefore] of [
    [
      "revision",
      {
        ...provisionedStorage(),
        revision: "2",
      },
    ],
    [
      "physical identity",
      {
        ...provisionedStorage(),
        mount: {
          ...provisionedStorage().mount,
          rootIdentity: {
            ...provisionedStorage().mount.rootIdentity,
            objectId: "1:30",
          },
        },
      },
    ],
  ]) {
    await t.test(name, async () => {
      const { authority, committed, database } =
        await fixtureWithCommittedProvision();
      const beforeHead = copyHeadRow(
        database.heads.get(
          identityKey("filesystem-image-ext4", "host-primary"),
        ),
      );
      const checksum = "c".repeat(64);
      const nextHead = appendHead(committed.nextHead, checksum, 512);
      const record = preparedRecord({
        checksum,
        kind: "checkpoint",
        operationId: `operation-stale-${name.replace(" ", "-")}`,
        revision: nextHead.stateRevision,
        storageStateBefore: staleBefore,
      });

      await assert.rejects(
        authority.compareAndAdvance({
          expectedHead: committed.nextHead,
          nextHead,
          transition: {
            contractVersion: 1,
            type: "append-prepared-v1",
            frameChecksum: checksum,
            record,
          },
        }),
        authorityError(
          "postgres_filesystem_image_provider_state_authority_state_invalid",
        ),
      );
      assert.deepEqual(
        database.heads.get(
          identityKey("filesystem-image-ext4", "host-primary"),
        ),
        beforeHead,
      );
      assert.equal(database.operations.size, 1);
    });
  }
});

test("isolates latest committed storage lineage by storage id", async () => {
  const { authority, committed, database } =
    await fixtureWithCommittedProvision();
  const checksum = "c".repeat(64);
  const nextHead = appendHead(committed.nextHead, checksum, 512);
  const record = preparedRecord({
    checksum,
    operationId: "operation-storage-2",
    revision: nextHead.stateRevision,
    storageId: "storage-2",
  });

  assert.equal(
    await authority.compareAndAdvance({
      expectedHead: committed.nextHead,
      nextHead,
      transition: {
        contractVersion: 1,
        type: "append-prepared-v1",
        frameChecksum: checksum,
        record,
      },
    }),
    true,
  );
  assert.equal(database.operations.size, 2);
  const lineageReads = database.queries.filter(
    ([text]) =>
      text.includes("filesystem_image_provider_operations") &&
      text.includes("storage_id = $3"),
  );
  assert.deepEqual(lineageReads.at(-1)[1], [
    "filesystem-image-ext4",
    "host-primary",
    "storage-2",
  ]);
});

test("fails closed and rolls back the head when the latest committed storage row is corrupt", async () => {
  const { authority, committed, database } =
    await fixtureWithCommittedProvision();
  const beforeHead = copyHeadRow(
    database.heads.get(identityKey("filesystem-image-ext4", "host-primary")),
  );
  database.operations.get(
    operationKey("filesystem-image-ext4", "host-primary", "operation-1"),
  ).committed_record_sha256 = "f".repeat(64);
  const checksum = "c".repeat(64);
  const nextHead = appendHead(committed.nextHead, checksum, 512);
  const record = preparedRecord({
    checksum,
    kind: "checkpoint",
    operationId: "operation-2",
    revision: nextHead.stateRevision,
    storageStateBefore: provisionedStorage(),
  });

  await assert.rejects(
    authority.compareAndAdvance({
      expectedHead: committed.nextHead,
      nextHead,
      transition: {
        contractVersion: 1,
        type: "append-prepared-v1",
        frameChecksum: checksum,
        record,
      },
    }),
    authorityError(
      "postgres_filesystem_image_provider_state_authority_state_invalid",
    ),
  );
  assert.deepEqual(
    database.heads.get(identityKey("filesystem-image-ext4", "host-primary")),
    beforeHead,
  );
  assert.equal(database.operations.size, 1);
  assert.equal(
    database.operations.has(
      operationKey("filesystem-image-ext4", "host-primary", "operation-2"),
    ),
    false,
  );
});

test("fails closed on a non-genesis head whose operation index marker is missing or behind", async (t) => {
  for (const [name, marker] of [
    ["missing", null],
    ["behind", "0"],
  ]) {
    await t.test(name, async () => {
      const { authority, database } = createFixture();
      const prepared = await appendPrepared(authority, GENESIS);
      const headKey = identityKey(
        "filesystem-image-ext4",
        "host-primary",
      );
      database.heads.get(headKey).operation_index_state_revision = marker;
      const beforeHead = copyHeadRow(database.heads.get(headKey));
      const beforeOperations = cloneMap(database.operations, copyOperationRow);
      const beforeQueries = database.queries.length;

      await assert.rejects(
        authority.readOperation({
          expectedHead: prepared.nextHead,
          operationId: "operation-1",
        }),
        authorityError(
          "postgres_filesystem_image_provider_state_authority_state_invalid",
        ),
      );
      await assert.rejects(
        authority.readOperationsPage({
          afterOperationId: null,
          expectedHead: prepared.nextHead,
          limit: 1,
        }),
        authorityError(
          "postgres_filesystem_image_provider_state_authority_state_invalid",
        ),
      );

      const checksum = "b".repeat(64);
      const nextHead = appendHead(prepared.nextHead, checksum, 320);
      await assert.rejects(
        authority.compareAndAdvance({
          expectedHead: prepared.nextHead,
          nextHead,
          transition: {
            contractVersion: 1,
            type: "append-committed-v1",
            frameChecksum: checksum,
            record: committedRecord(
              prepared.record,
              nextHead.stateRevision,
            ),
          },
        }),
        authorityError(
          "postgres_filesystem_image_provider_state_authority_state_invalid",
        ),
      );
      assert.deepEqual(database.heads.get(headKey), beforeHead);
      assert.deepEqual(database.operations, beforeOperations);
      assert.equal(
        database.queries.slice(beforeQueries).some(
          ([text]) =>
            text.includes("filesystem_image_provider_operations") ||
            text.startsWith(
              "UPDATE session_authority.filesystem_image_provider_heads",
            ),
        ),
        false,
      );

      const staleChecksum = "c".repeat(64);
      assert.equal(
        await authority.compareAndAdvance({
          expectedHead: GENESIS,
          nextHead: appendHead(GENESIS, staleChecksum, 128),
          transition: {
            contractVersion: 1,
            type: "append-prepared-v1",
            frameChecksum: staleChecksum,
            record: preparedRecord({
              checksum: staleChecksum,
              operationId: "operation-stale-head",
              storageId: "storage-2",
            }),
          },
        }),
        false,
      );
      assert.deepEqual(database.heads.get(headKey), beforeHead);
      assert.deepEqual(database.operations, beforeOperations);
    });
  }
});

test("rotates the head without mutating permanent operations", async () => {
  const { authority, database } = createFixture();
  const prepared = await appendPrepared(authority, GENESIS);
  const before = cloneMap(database.operations, copyOperationRow);
  const nextHead = rotationHead(prepared.nextHead);
  assert.equal(
    await authority.compareAndAdvance({
      expectedHead: prepared.nextHead,
      nextHead,
      transition: { contractVersion: 1, type: "rotate-v1" },
    }),
    true,
  );
  assert.deepEqual(database.operations, before);
  assert.equal(
    database.heads.get(
      identityKey("filesystem-image-ext4", "host-primary"),
    ).operation_index_state_revision,
    prepared.nextHead.stateRevision,
  );
  assert.deepEqual(await authority.readHead(), nextHead);
  assert.deepEqual(
    await authority.readOperation({
      expectedHead: nextHead,
      operationId: "operation-1",
    }),
    prepared.record,
  );
});

test("quarantines unavailable adopted-v2 checksums under every v2 head", async () => {
  const { authority, database } = createFixture();
  const prepared = await appendPrepared(authority, GENESIS);
  const committedChecksum = "b".repeat(64);
  const committedHead = appendHead(prepared.nextHead, committedChecksum, 320);
  const committed = committedRecord(
    prepared.record,
    committedHead.stateRevision,
  );
  assert.equal(
    await authority.compareAndAdvance({
      expectedHead: prepared.nextHead,
      nextHead: committedHead,
      transition: {
        contractVersion: 1,
        type: "append-committed-v1",
        frameChecksum: committedChecksum,
        record: committed,
      },
    }),
    true,
  );
  const key = operationKey(
    "filesystem-image-ext4",
    "host-primary",
    "operation-1",
  );
  const adopted = database.operations.get(key);
  adopted.committed_checksum_provenance = "unavailable-adopted-v2";
  adopted.committed_checksum = null;
  await assert.rejects(
    authority.readOperation({
      expectedHead: committedHead,
      operationId: "operation-1",
    }),
    authorityError(
      "postgres_filesystem_image_provider_state_authority_state_invalid",
    ),
  );
  const rotatedHead = rotationHead(committedHead);
  assert.equal(
    await authority.compareAndAdvance({
      expectedHead: committedHead,
      nextHead: rotatedHead,
      transition: { contractVersion: 1, type: "rotate-v1" },
    }),
    true,
  );
  await assert.rejects(
    authority.readOperation({
      expectedHead: rotatedHead,
      operationId: "operation-1",
    }),
    authorityError(
      "postgres_filesystem_image_provider_state_authority_state_invalid",
    ),
  );
});

test("accepts a legal v2 operation record larger than one canonical object", async () => {
  const { authority, database } = createFixture();
  const requestPayload = "r".repeat(700 * 1024);
  const resultPayload = "s".repeat(700 * 1024);
  const preparedChecksum = "a".repeat(64);
  const preparedHead = appendHead(GENESIS, preparedChecksum, 800 * 1024);
  const prepared = {
    ...preparedRecord({ checksum: preparedChecksum }),
    request: { payload: requestPayload, storageId: "storage-1" },
  };
  assert.equal(
    await authority.compareAndAdvance({
      expectedHead: GENESIS,
      nextHead: preparedHead,
      transition: {
        contractVersion: 1,
        type: "append-prepared-v1",
        frameChecksum: preparedChecksum,
        record: prepared,
      },
    }),
    true,
  );
  const committedChecksum = "b".repeat(64);
  const committedHead = appendHead(
    preparedHead,
    committedChecksum,
    2 * 1024 * 1024,
  );
  const committed = {
    ...committedRecord(prepared, committedHead.stateRevision),
    result: { payload: resultPayload, status: "created" },
  };
  assert.equal(
    await authority.compareAndAdvance({
      expectedHead: preparedHead,
      nextHead: committedHead,
      transition: {
        contractVersion: 1,
        type: "append-committed-v1",
        frameChecksum: committedChecksum,
        record: committed,
      },
    }),
    true,
  );
  const stored = database.operations.get(
    operationKey("filesystem-image-ext4", "host-primary", "operation-1"),
  );
  assert.ok(stored.committed_record_bytes.length > 768 * 1024);
  assert.ok(stored.committed_record_bytes.length <= 4 * 1024 * 1024);
  assert.deepEqual(
    await authority.readOperation({
      expectedHead: committedHead,
      operationId: "operation-1",
    }),
    committed,
  );
  assert.deepEqual(
    await authority.readOperationsPage({
      afterOperationId: null,
      expectedHead: committedHead,
      limit: 1,
    }),
    { operations: [committed], nextAfterOperationId: null },
  );
});

test("returns false on exact head CAS loss before any operation mutation", async () => {
  const { authority, database } = createFixture();
  database.forceHeadCasMissOnce = true;
  const checksum = "a".repeat(64);
  const nextHead = appendHead(GENESIS, checksum, 128);
  assert.equal(
    await authority.compareAndAdvance({
      expectedHead: GENESIS,
      nextHead,
      transition: {
        contractVersion: 1,
        type: "append-prepared-v1",
        frameChecksum: checksum,
        record: preparedRecord(),
      },
    }),
    false,
  );
  assert.equal(database.operations.size, 0);
  assert.equal(
    database.queries.some(
      ([text]) => text.includes("filesystem_image_provider_operations"),
    ),
    false,
  );
  assert.deepEqual(await authority.readHead(), GENESIS);
});

test("rolls back a successful head and marker CAS when the exact operation mutation mismatches", async () => {
  const { authority, database } = createFixture();
  const prepared = await appendPrepared(authority, GENESIS);
  const beforeHead = copyHeadRow(
    database.heads.get(identityKey("filesystem-image-ext4", "host-primary")),
  );
  const beforeOperation = copyOperationRow(
    database.operations.get(
      operationKey("filesystem-image-ext4", "host-primary", "operation-1"),
    ),
  );
  assert.equal(
    beforeHead.operation_index_state_revision,
    prepared.nextHead.stateRevision,
  );
  const checksum = "b".repeat(64);
  const nextHead = appendHead(prepared.nextHead, checksum, 320);
  database.forceOperationMutationMissOnce = true;
  await assert.rejects(
    authority.compareAndAdvance({
      expectedHead: prepared.nextHead,
      nextHead,
      transition: {
        contractVersion: 1,
        type: "append-committed-v1",
        frameChecksum: checksum,
        record: committedRecord(prepared.record, nextHead.stateRevision),
      },
    }),
    authorityError(
      "postgres_filesystem_image_provider_state_authority_state_invalid",
    ),
  );
  assert.deepEqual(
    database.heads.get(identityKey("filesystem-image-ext4", "host-primary")),
    beforeHead,
  );
  assert.deepEqual(
    database.operations.get(
      operationKey("filesystem-image-ext4", "host-primary", "operation-1"),
    ),
    beforeOperation,
  );
});

test("propagates uncertain commit acknowledgement and supports exact-head readback", async () => {
  const { authority, database } = createFixture();
  const checksum = "a".repeat(64);
  const nextHead = appendHead(GENESIS, checksum, 128);
  const record = preparedRecord();
  database.failCommitOnce = true;
  await assert.rejects(
    authority.compareAndAdvance({
      expectedHead: GENESIS,
      nextHead,
      transition: {
        contractVersion: 1,
        type: "append-prepared-v1",
        frameChecksum: checksum,
        record,
      },
    }),
    (error) =>
      error instanceof PostgresSerializableStoreError &&
      error.code === "transaction_commit_outcome_uncertain" &&
      error.commitState === "uncertain",
  );
  assert.deepEqual(await authority.readHead(), nextHead);
  assert.deepEqual(
    await authority.readOperation({
      expectedHead: nextHead,
      operationId: "operation-1",
    }),
    record,
  );
});

test("pages permanent history in C operation-id order with limit plus one", async () => {
  const { authority, database } = createFixture();
  let head = GENESIS;
  for (const [index, operationId] of ["operation-c", "operation-a", "operation-b"].entries()) {
    const appended = await appendPrepared(authority, head, {
      checksum: String.fromCharCode(97 + index).repeat(64),
      ledgerBytes: 128 * (index + 1),
      operationId,
      storageId: `storage-${index + 1}`,
    });
    head = appended.nextHead;
  }
  const firstPage = await authority.readOperationsPage({
    afterOperationId: null,
    expectedHead: head,
    limit: 2,
  });
  assert.deepEqual(
    firstPage.operations.map((record) => record.operationId),
    ["operation-a", "operation-b"],
  );
  assert.equal(firstPage.nextAfterOperationId, "operation-b");
  assert.equal(Object.isFrozen(firstPage), true);
  assert.equal(Object.isFrozen(firstPage.operations), true);
  assert.equal(Object.isFrozen(firstPage.operations[0]), true);

  const secondPage = await authority.readOperationsPage({
    afterOperationId: firstPage.nextAfterOperationId,
    expectedHead: head,
    limit: 2,
  });
  assert.deepEqual(
    secondPage.operations.map((record) => record.operationId),
    ["operation-c"],
  );
  assert.equal(secondPage.nextAfterOperationId, null);
  const pageQuery = database.queries.find(
    ([text, values]) =>
      text.includes("filesystem_image_provider_operations") &&
      text.includes("ORDER BY operation_id") &&
      values.length === 3,
  );
  assert.match(pageQuery[0], /COLLATE pg_catalog\."C"/u);
  assert.deepEqual(pageQuery[1], [
    "filesystem-image-ext4",
    "host-primary",
    "3",
  ]);
});

test("fails closed on expected-head conflict and validates input before SQL", async () => {
  const { authority, database } = createFixture();
  const prepared = await appendPrepared(authority, GENESIS);
  await assert.rejects(
    authority.readOperation({
      expectedHead: GENESIS,
      operationId: "operation-1",
    }),
    authorityError(
      "postgres_filesystem_image_provider_state_authority_state_invalid",
    ),
  );

  const beforeQueries = database.queries.length;
  const nextHead = appendHead(prepared.nextHead, "b".repeat(64), 320);
  await assert.rejects(
    authority.compareAndAdvance({
      expectedHead: prepared.nextHead,
      nextHead,
      transition: {
        contractVersion: 1,
        type: "append-committed-v1",
        frameChecksum: "c".repeat(64),
        record: committedRecord(prepared.record, nextHead.stateRevision),
      },
    }),
    authorityError(
      "invalid_postgres_filesystem_image_provider_state_authority_request",
    ),
  );
  assert.equal(database.queries.length, beforeQueries);
  await assert.rejects(
    authority.readOperationsPage({
      afterOperationId: null,
      expectedHead: prepared.nextHead,
      limit: 5,
    }),
    authorityError(
      "invalid_postgres_filesystem_image_provider_state_authority_request",
    ),
  );
  assert.equal(database.queries.length, beforeQueries);
});

test("rejects hostile request objects and noncanonical operation records", async () => {
  const { authority, database, store } = createFixture();
  const revokedOptions = Proxy.revocable(
    {
      store,
      providerId: "filesystem-image-ext4",
      anchorId: "host-primary",
    },
    {},
  );
  revokedOptions.revoke();
  assert.throws(
    () =>
      createPostgresFilesystemImageProviderStateAuthority(
        revokedOptions.proxy,
      ),
    authorityError(
      "invalid_postgres_filesystem_image_provider_state_authority_options",
    ),
  );
  const revokedReadRequest = Proxy.revocable(
    { expectedHead: GENESIS, operationId: "operation-1" },
    {},
  );
  revokedReadRequest.revoke();
  await assert.rejects(
    authority.readOperation(revokedReadRequest.proxy),
    authorityError(
      "invalid_postgres_filesystem_image_provider_state_authority_request",
    ),
  );
  await assert.rejects(
    authority.readOperation(
      new Proxy(
        { expectedHead: GENESIS, operationId: "operation-1" },
        {},
      ),
    ),
    authorityError(
      "invalid_postgres_filesystem_image_provider_state_authority_request",
    ),
  );
  const accessor = {
    expectedHead: GENESIS,
    get operationId() {
      throw new Error("must not execute");
    },
  };
  await assert.rejects(
    authority.readOperation(accessor),
    authorityError(
      "invalid_postgres_filesystem_image_provider_state_authority_request",
    ),
  );
  const checksum = "a".repeat(64);
  const nextHead = appendHead(GENESIS, checksum, 128);
  const revokedRecord = Proxy.revocable(preparedRecord({ checksum }), {});
  revokedRecord.revoke();
  await assert.rejects(
    authority.compareAndAdvance({
      expectedHead: GENESIS,
      nextHead,
      transition: {
        contractVersion: 1,
        type: "append-prepared-v1",
        frameChecksum: checksum,
        record: revokedRecord.proxy,
      },
    }),
    authorityError(
      "invalid_postgres_filesystem_image_provider_state_authority_request",
    ),
  );
  const record = preparedRecord();
  Object.defineProperty(record, "request", {
    enumerable: true,
    get() {
      throw new Error("must not execute");
    },
  });
  await assert.rejects(
    authority.compareAndAdvance({
      expectedHead: GENESIS,
      nextHead,
      transition: {
        contractVersion: 1,
        type: "append-prepared-v1",
        frameChecksum: checksum,
        record,
      },
    }),
    authorityError(
      "invalid_postgres_filesystem_image_provider_state_authority_request",
    ),
  );
  assert.equal(database.queries.length, 0);
});

test("rejects oversized uint64 text before adoption materialization", async () => {
  const fixture = legacyAdoptionFixture();
  await assert.rejects(
    fixture.adoptionAuthority.compareAndAdopt({
      ...fixture.request,
      operations: [
        {
          ...fixture.committed,
          preparedStateRevision: "1".repeat(100_000),
        },
      ],
    }),
    authorityError(
      "invalid_postgres_filesystem_image_provider_state_authority_request",
    ),
  );
  assert.equal(fixture.database.queries.length, 0);
});

test("rejects oversized stored numeric text before parsing", async () => {
  const fixture = createFixture();
  const stored = headRow(appendHead(V3_GENESIS, "a".repeat(64), 128));
  stored.checkpoint_frame_count = "9".repeat(100_000);
  fixture.database.heads.set(
    identityKey("filesystem-image-ext4", "host-primary"),
    stored,
  );
  await assert.rejects(
    fixture.runtimeAuthority.readHead(),
    authorityError(
      "postgres_filesystem_image_provider_state_authority_state_invalid",
    ),
  );
});

test("rejects non-object operation payload roots before SQL", async () => {
  const { authority, database } = createFixture();
  const preparedChecksum = "a".repeat(64);
  const preparedHead = appendHead(GENESIS, preparedChecksum, 128);
  for (const request of [[], "primitive-request", null]) {
    await assert.rejects(
      authority.compareAndAdvance({
        expectedHead: GENESIS,
        nextHead: preparedHead,
        transition: {
          contractVersion: 1,
          type: "append-prepared-v1",
          frameChecksum: preparedChecksum,
          record: {
            ...preparedRecord({ checksum: preparedChecksum }),
            request,
          },
        },
      }),
      authorityError(
        "invalid_postgres_filesystem_image_provider_state_authority_request",
      ),
    );
  }

  const prepared = preparedRecord({ checksum: preparedChecksum });
  const committedChecksum = "b".repeat(64);
  const committedHead = appendHead(preparedHead, committedChecksum, 320);
  for (const result of [null, []]) {
    await assert.rejects(
      authority.compareAndAdvance({
        expectedHead: preparedHead,
        nextHead: committedHead,
        transition: {
          contractVersion: 1,
          type: "append-committed-v1",
          frameChecksum: committedChecksum,
          record: {
            ...committedRecord(prepared, committedHead.stateRevision),
            result,
          },
        },
      }),
      authorityError(
        "invalid_postgres_filesystem_image_provider_state_authority_request",
      ),
    );
  }
  assert.equal(database.queries.length, 0);
  assert.equal(database.heads.size, 0);
  assert.equal(database.operations.size, 0);
});

test(
  "bounds dense array precursors before own-key enumeration",
  { concurrency: false },
  async () => {
    const ownKeysDescriptor = Object.getOwnPropertyDescriptor(Reflect, "ownKeys");
    assert.equal(typeof ownKeysDescriptor?.value, "function");
    const hugeDenseArray = new Array(1_000_000).fill(null);
    let hugeArrayEnumerations = 0;
    Object.defineProperty(Reflect, "ownKeys", {
      ...ownKeysDescriptor,
      value(value) {
        if (value === hugeDenseArray) {
          hugeArrayEnumerations += 1;
          throw new Error("array own-key enumeration must not start");
        }
        return Reflect.apply(ownKeysDescriptor.value, this, [value]);
      },
    });
    try {
      const authorityModule = await import(
        "../src/postgres-filesystem-image-provider-state-authority.mjs?dense-array-bound-test"
      );
      const database = new FakeAuthorityDatabase();
      const store = new PostgresSerializableStore({
        dedicatedPool: database.createPool(),
        maxTransactionAttempts: 1,
      });
      const authority =
        authorityModule.createPostgresFilesystemImageProviderStateAuthority({
          store,
          providerId: "filesystem-image-ext4",
          anchorId: "host-primary",
        });
      const checksum = "a".repeat(64);
      const nextHead = appendHead(GENESIS, checksum, 128);
      await assert.rejects(
        authority.compareAndAdvance({
          expectedHead: GENESIS,
          nextHead,
          transition: {
            contractVersion: 1,
            type: "append-prepared-v1",
            frameChecksum: checksum,
            record: {
              ...preparedRecord({ checksum }),
              request: { payload: hugeDenseArray },
            },
          },
        }),
        (error) =>
          error instanceof
            authorityModule.PostgresFilesystemImageProviderStateAuthorityError &&
          error.code ===
            "invalid_postgres_filesystem_image_provider_state_authority_request",
      );
      assert.equal(database.queries.length, 0);
    } finally {
      Object.defineProperty(Reflect, "ownKeys", ownKeysDescriptor);
    }
    assert.equal(hugeArrayEnumerations, 0);
  },
);

test(
  "bounds plain object precursors immediately after own-key enumeration",
  { concurrency: false },
  async () => {
    const ownKeysDescriptor = Object.getOwnPropertyDescriptor(Reflect, "ownKeys");
    assert.equal(typeof ownKeysDescriptor?.value, "function");
    const methodNames = ["every", "slice", "sort"];
    const methodDescriptors = methodNames.map((name) =>
      Object.getOwnPropertyDescriptor(Array.prototype, name),
    );
    for (const descriptor of methodDescriptors) {
      assert.equal(typeof descriptor?.value, "function");
    }
    const oversizedPlainObject = {};
    for (let index = 0; index < 20_000; index += 1) {
      oversizedPlainObject[`field-${index}`] = null;
    }
    let oversizedKeys = null;
    let oversizedObjectEnumerations = 0;
    const downstreamLargeArrayCalls = [];
    Object.defineProperty(Reflect, "ownKeys", {
      ...ownKeysDescriptor,
      value(value) {
        const keys = Reflect.apply(ownKeysDescriptor.value, this, [value]);
        if (value === oversizedPlainObject) {
          oversizedKeys = keys;
          oversizedObjectEnumerations += 1;
        }
        return keys;
      },
    });
    for (let index = 0; index < methodNames.length; index += 1) {
      const name = methodNames[index];
      const descriptor = methodDescriptors[index];
      Object.defineProperty(Array.prototype, name, {
        ...descriptor,
        value(...args) {
          if (this === oversizedKeys) {
            downstreamLargeArrayCalls.push(name);
            throw new Error(`oversized key array must not call ${name}`);
          }
          return Reflect.apply(descriptor.value, this, args);
        },
      });
    }
    try {
      const authorityModule = await import(
        "../src/postgres-filesystem-image-provider-state-authority.mjs?plain-object-bound-test"
      );
      const database = new FakeAuthorityDatabase();
      const store = new PostgresSerializableStore({
        dedicatedPool: database.createPool(),
        maxTransactionAttempts: 1,
      });
      const authority =
        authorityModule.createPostgresFilesystemImageProviderStateAuthority({
          store,
          providerId: "filesystem-image-ext4",
          anchorId: "host-primary",
        });
      const checksum = "a".repeat(64);
      const nextHead = appendHead(GENESIS, checksum, 128);
      await assert.rejects(
        authority.compareAndAdvance({
          expectedHead: GENESIS,
          nextHead,
          transition: {
            contractVersion: 1,
            type: "append-prepared-v1",
            frameChecksum: checksum,
            record: {
              ...preparedRecord({ checksum }),
              request: oversizedPlainObject,
            },
          },
        }),
        (error) =>
          error instanceof
            authorityModule.PostgresFilesystemImageProviderStateAuthorityError &&
          error.code ===
            "invalid_postgres_filesystem_image_provider_state_authority_request",
      );
      assert.equal(database.queries.length, 0);
    } finally {
      for (let index = 0; index < methodNames.length; index += 1) {
        Object.defineProperty(
          Array.prototype,
          methodNames[index],
          methodDescriptors[index],
        );
      }
      Object.defineProperty(Reflect, "ownKeys", ownKeysDescriptor);
    }
    assert.equal(oversizedObjectEnumerations, 1);
    assert.deepEqual(downstreamLargeArrayCalls, []);
  },
);

test(
  "preflights canonical key bytes before copying or sorting plain objects",
  { concurrency: false },
  async () => {
    const ownKeysDescriptor = Object.getOwnPropertyDescriptor(Reflect, "ownKeys");
    assert.equal(typeof ownKeysDescriptor?.value, "function");
    const methodNames = ["slice", "sort"];
    const methodDescriptors = methodNames.map((name) =>
      Object.getOwnPropertyDescriptor(Array.prototype, name),
    );
    for (const descriptor of methodDescriptors) {
      assert.equal(typeof descriptor?.value, "function");
    }
    const sharedPrefix = "k".repeat(400 * 1024);
    const overBudgetKeysObject = {
      [`${sharedPrefix}-a`]: null,
      [`${sharedPrefix}-b`]: null,
    };
    const nonPlainObject = Object.create({ inherited: true });
    let overBudgetKeys = null;
    let nonPlainObjectEnumerations = 0;
    const keyArrayCopyOrSortCalls = [];
    Object.defineProperty(Reflect, "ownKeys", {
      ...ownKeysDescriptor,
      value(value) {
        if (value === nonPlainObject) nonPlainObjectEnumerations += 1;
        const keys = Reflect.apply(ownKeysDescriptor.value, this, [value]);
        if (value === overBudgetKeysObject) overBudgetKeys = keys;
        return keys;
      },
    });
    for (let index = 0; index < methodNames.length; index += 1) {
      const name = methodNames[index];
      const descriptor = methodDescriptors[index];
      Object.defineProperty(Array.prototype, name, {
        ...descriptor,
        value(...args) {
          if (this === overBudgetKeys) {
            keyArrayCopyOrSortCalls.push(name);
            throw new Error(`over-budget key array must not call ${name}`);
          }
          return Reflect.apply(descriptor.value, this, args);
        },
      });
    }
    try {
      const authorityModule = await import(
        "../src/postgres-filesystem-image-provider-state-authority.mjs?plain-object-key-byte-bound-test"
      );
      const database = new FakeAuthorityDatabase();
      const store = new PostgresSerializableStore({
        dedicatedPool: database.createPool(),
        maxTransactionAttempts: 1,
      });
      const authority =
        authorityModule.createPostgresFilesystemImageProviderStateAuthority({
          store,
          providerId: "filesystem-image-ext4",
          anchorId: "host-primary",
        });
      const checksum = "a".repeat(64);
      const nextHead = appendHead(GENESIS, checksum, 128);
      const rejectRequest = async (request) => {
        await assert.rejects(
          authority.compareAndAdvance({
            expectedHead: GENESIS,
            nextHead,
            transition: {
              contractVersion: 1,
              type: "append-prepared-v1",
              frameChecksum: checksum,
              record: {
                ...preparedRecord({ checksum }),
                request,
              },
            },
          }),
          (error) =>
            error instanceof
              authorityModule.PostgresFilesystemImageProviderStateAuthorityError &&
            error.code ===
              "invalid_postgres_filesystem_image_provider_state_authority_request",
        );
      };
      await rejectRequest(nonPlainObject);
      await rejectRequest(overBudgetKeysObject);
      assert.equal(database.queries.length, 0);
    } finally {
      for (let index = 0; index < methodNames.length; index += 1) {
        Object.defineProperty(
          Array.prototype,
          methodNames[index],
          methodDescriptors[index],
        );
      }
      Object.defineProperty(Reflect, "ownKeys", ownKeysDescriptor);
    }
    assert.equal(nonPlainObjectEnumerations, 0);
    assert.deepEqual(keyArrayCopyOrSortCalls, []);
  },
);

test("uses captured validation and canonicalization intrinsics", { concurrency: false }, async () => {
  const { authority } = createFixture();
  const targets = [
    [Array, "isArray"],
    [JSON, "stringify"],
    [Object, "freeze"],
    [Object, "getOwnPropertyDescriptor"],
    [Object, "getPrototypeOf"],
    [Object, "hasOwn"],
    [RegExp.prototype, "test"],
  ];
  const descriptors = targets.map(([target, key]) =>
    Object.getOwnPropertyDescriptor(target, key),
  );
  const poisonCalls = [];
  try {
    for (let index = 0; index < targets.length; index += 1) {
      const [target, key] = targets[index];
      Object.defineProperty(target, key, {
        ...descriptors[index],
        value() {
          poisonCalls.push(key);
          throw new Error(`poisoned intrinsic: ${key}`);
        },
      });
    }
    await appendPrepared(authority, GENESIS);
  } finally {
    for (let index = 0; index < targets.length; index += 1) {
      const [target, key] = targets[index];
      Object.defineProperty(target, key, descriptors[index]);
    }
  }
  assert.deepEqual(poisonCalls, []);
});

test("rejects checkpoint-incompatible operation semantics before SQL", async () => {
  const { authority, database } = createFixture();
  const firstChecksum = "a".repeat(64);
  const firstHead = appendHead(GENESIS, firstChecksum, 128);
  const secondChecksum = "b".repeat(64);
  const secondHead = appendHead(firstHead, secondChecksum, 320);
  const invalidPrepared = {
    ...preparedRecord(),
    kind: "attach",
  };
  await assert.rejects(
    authority.compareAndAdvance({
      expectedHead: GENESIS,
      nextHead: firstHead,
      transition: {
        contractVersion: 1,
        type: "append-prepared-v1",
        frameChecksum: firstChecksum,
        record: invalidPrepared,
      },
    }),
    authorityError(
      "invalid_postgres_filesystem_image_provider_state_authority_request",
    ),
  );

  const invalidExpectedStorage = {
    ...committedRecord(preparedRecord(), "2"),
    expectedStorage: { lifecycle: "provisioned", revision: "1" },
  };
  await assert.rejects(
    authority.compareAndAdvance({
      expectedHead: firstHead,
      nextHead: secondHead,
      transition: {
        contractVersion: 1,
        type: "append-committed-v1",
        frameChecksum: secondChecksum,
        record: invalidExpectedStorage,
      },
    }),
    authorityError(
      "invalid_postgres_filesystem_image_provider_state_authority_request",
    ),
  );

  const invalidStorageTransition = {
    ...committedRecord(preparedRecord(), "2"),
    storageState: {
      ...provisionedStorage(),
      revision: "2",
    },
  };
  await assert.rejects(
    authority.compareAndAdvance({
      expectedHead: firstHead,
      nextHead: secondHead,
      transition: {
        contractVersion: 1,
        type: "append-committed-v1",
        frameChecksum: secondChecksum,
        record: invalidStorageTransition,
      },
    }),
    authorityError(
      "invalid_postgres_filesystem_image_provider_state_authority_request",
    ),
  );

  const wrongStorageBefore = {
    ...preparedRecord(),
    kind: "checkpoint",
    storageStateBefore: provisionedStorage("storage-other"),
  };
  await assert.rejects(
    authority.compareAndAdvance({
      expectedHead: GENESIS,
      nextHead: firstHead,
      transition: {
        contractVersion: 1,
        type: "append-prepared-v1",
        frameChecksum: firstChecksum,
        record: wrongStorageBefore,
      },
    }),
    authorityError(
      "invalid_postgres_filesystem_image_provider_state_authority_request",
    ),
  );
  assert.equal(database.queries.length, 0);
});

test("rejects corrupt canonical bytes, digests, and relational metadata on read", async (t) => {
  async function fixtureWithPrepared() {
    const fixture = createFixture();
    const appended = await appendPrepared(fixture.authority, GENESIS);
    const key = operationKey(
      "filesystem-image-ext4",
      "host-primary",
      "operation-1",
    );
    return { ...fixture, appended, key };
  }

  async function fixtureWithCommitted() {
    const fixture = await fixtureWithPrepared();
    const checksum = "b".repeat(64);
    const nextHead = appendHead(fixture.appended.nextHead, checksum, 320);
    const record = committedRecord(
      fixture.appended.record,
      nextHead.stateRevision,
    );
    assert.equal(
      await fixture.authority.compareAndAdvance({
        expectedHead: fixture.appended.nextHead,
        nextHead,
        transition: {
          contractVersion: 1,
          type: "append-committed-v1",
          frameChecksum: checksum,
          record,
        },
      }),
      true,
    );
    return { ...fixture, nextHead, record };
  }

  await t.test("invalid UTF-8 canonical bytes", async () => {
    const { authority, database, appended, key } = await fixtureWithPrepared();
    const row = database.operations.get(key);
    row.prepared_record_bytes = Buffer.from([0xff]);
    await assert.rejects(
      authority.readOperation({
        expectedHead: appended.nextHead,
        operationId: "operation-1",
      }),
      authorityError(
        "postgres_filesystem_image_provider_state_authority_state_invalid",
      ),
    );
  });

  await t.test("record digest mismatch", async () => {
    const { authority, database, appended, key } = await fixtureWithPrepared();
    database.operations.get(key).prepared_record_sha256 = "f".repeat(64);
    await assert.rejects(
      authority.readOperation({
        expectedHead: appended.nextHead,
        operationId: "operation-1",
      }),
      authorityError(
        "postgres_filesystem_image_provider_state_authority_state_invalid",
      ),
    );
  });

  await t.test("row metadata mismatch", async () => {
    const { authority, database, appended, key } = await fixtureWithPrepared();
    database.operations.get(key).storage_id = "different-storage";
    await assert.rejects(
      authority.readOperation({
        expectedHead: appended.nextHead,
        operationId: "operation-1",
      }),
      authorityError(
        "postgres_filesystem_image_provider_state_authority_state_invalid",
      ),
    );
  });

  await t.test("prepared row with committed checksum provenance", async () => {
    const { authority, database, appended, key } = await fixtureWithPrepared();
    database.operations.get(key).committed_checksum_provenance =
      "indexed-frame-v1";
    await assert.rejects(
      authority.readOperation({
        expectedHead: appended.nextHead,
        operationId: "operation-1",
      }),
      authorityError(
        "postgres_filesystem_image_provider_state_authority_state_invalid",
      ),
    );
  });

  for (const [name, provenance, checksum] of [
    ["indexed provenance without checksum", "indexed-frame-v1", null],
    [
      "unavailable provenance with checksum",
      "unavailable-adopted-v2",
      "b".repeat(64),
    ],
    ["unknown checksum provenance", "unknown-v1", "b".repeat(64)],
  ]) {
    await t.test(name, async () => {
      const { authority, database, key, nextHead } =
        await fixtureWithCommitted();
      const row = database.operations.get(key);
      row.committed_checksum_provenance = provenance;
      row.committed_checksum = checksum;
      await assert.rejects(
        authority.readOperation({
          expectedHead: nextHead,
          operationId: "operation-1",
        }),
        authorityError(
          "postgres_filesystem_image_provider_state_authority_state_invalid",
        ),
      );
    });
  }

  await t.test("hostile row object", async () => {
    const { authority, database, appended, key } = await fixtureWithPrepared();
    database.operationReadOverride = result("SELECT", [
      new Proxy(copyOperationRow(database.operations.get(key)), {}),
    ]);
    await assert.rejects(
      authority.readOperation({
        expectedHead: appended.nextHead,
        operationId: "operation-1",
      }),
      authorityError(
        "postgres_filesystem_image_provider_state_authority_state_invalid",
      ),
    );
  });
});

test("same operation replay is a state-invalid rollback, not an implicit success", async () => {
  const { authority, database } = createFixture();
  const first = await appendPrepared(authority, GENESIS);
  const checksum = "b".repeat(64);
  const nextHead = appendHead(first.nextHead, checksum, 320);
  const duplicate = preparedRecord({
    checksum,
    revision: nextHead.stateRevision,
  });
  await assert.rejects(
    authority.compareAndAdvance({
      expectedHead: first.nextHead,
      nextHead,
      transition: {
        contractVersion: 1,
        type: "append-prepared-v1",
        frameChecksum: checksum,
        record: duplicate,
      },
    }),
    authorityError(
      "postgres_filesystem_image_provider_state_authority_state_invalid",
    ),
  );
  assert.deepEqual(await authority.readHead(), first.nextHead);
  assert.equal(database.operations.size, 1);
});
