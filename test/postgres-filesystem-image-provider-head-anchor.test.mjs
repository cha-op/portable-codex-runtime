import assert from "node:assert/strict";
import { EventEmitter } from "node:events";
import test from "node:test";

import {
  FILESYSTEM_IMAGE_PROVIDER_STATE_HEAD_CONTRACT_VERSION,
} from "../src/filesystem-image-provider-state.mjs";
import {
  PostgresFilesystemImageProviderHeadAnchorError,
  createPostgresFilesystemImageProviderHeadAnchor,
} from "../src/postgres-filesystem-image-provider-head-anchor.mjs";
import {
  PostgresSerializableStore,
  PostgresSerializableStoreError,
  isPostgresSerializableStore,
} from "../src/postgres-serializable-store.mjs";

const GENESIS = Object.freeze({
  contractVersion: FILESYSTEM_IMAGE_PROVIDER_STATE_HEAD_CONTRACT_VERSION,
  sequence: 0,
  lastChecksum: null,
  ledgerBytes: 0,
});

function head(sequence, checksumCharacter, ledgerBytes) {
  return {
    contractVersion: FILESYSTEM_IMAGE_PROVIDER_STATE_HEAD_CONTRACT_VERSION,
    sequence,
    lastChecksum: checksumCharacter.repeat(64),
    ledgerBytes,
  };
}

function key(providerId, anchorId) {
  return `${providerId}\0${anchorId}`;
}

function copyRow(row) {
  return {
    provider_id: row.provider_id,
    anchor_id: row.anchor_id,
    contract_version: row.contract_version,
    sequence: row.sequence,
    last_checksum: row.last_checksum,
    ledger_bytes: row.ledger_bytes,
  };
}

function result(command, rows = []) {
  return { command, rowCount: rows.length, rows };
}

class FakeHeadDatabase {
  constructor() {
    this.heads = new Map();
    this.failCommitOnce = false;
    this.failHeadQueryOnce = false;
    this.headQueryResultOverride = null;
    this.malformedReadRow = null;
    this.queries = [];
    this.releaseCalls = [];
  }

  createPool() {
    const database = this;
    return {
      async connect() {
        return new FakeHeadClient(database);
      },
    };
  }
}

class FakeHeadClient {
  constructor(database) {
    this.connection = new EventEmitter();
    this.database = database;
  }

  async query(...args) {
    const text = typeof args[0] === "string" ? args[0] : args[0].text;
    const values = typeof args[0] === "string" ? args[1] : args[0].values;
    this.database.queries.push([text, values]);

    if (text === "DISCARD ALL") return result("DISCARD");
    if (text.startsWith("BEGIN ")) return result("BEGIN");
    if (
      text.startsWith(
        "SELECT pg_catalog.transaction_timestamp() AS transaction_timestamp",
      )
    ) {
      return result("SELECT", [
        {
          transaction_timestamp: "2026-08-14T12:00:00.000Z",
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
    if (text === "ROLLBACK") return result("ROLLBACK");
    if (text === "COMMIT") {
      if (this.database.failCommitOnce) {
        this.database.failCommitOnce = false;
        throw new Error("commit acknowledgement lost");
      }
      return result("COMMIT");
    }

    if (text.includes("session_authority.filesystem_image_provider_heads")) {
      if (this.database.failHeadQueryOnce) {
        this.database.failHeadQueryOnce = false;
        throw new Error("head query transport failed");
      }
      if (this.database.headQueryResultOverride !== null) {
        const overridden = this.database.headQueryResultOverride;
        this.database.headQueryResultOverride = null;
        return overridden;
      }
      const providerId = values[0];
      const anchorId = values[1];
      const identity = key(providerId, anchorId);
      if (text.startsWith("SELECT ")) {
        if (this.database.malformedReadRow !== null) {
          return result("SELECT", [this.database.malformedReadRow]);
        }
        const stored = this.database.heads.get(identity);
        return result("SELECT", stored === undefined ? [] : [copyRow(stored)]);
      }
      if (text.startsWith("INSERT ")) {
        if (this.database.heads.has(identity)) return result("INSERT");
        const stored = {
          provider_id: providerId,
          anchor_id: anchorId,
          contract_version: values[2],
          sequence: values[3],
          last_checksum: values[4],
          ledger_bytes: values[5],
        };
        this.database.heads.set(identity, stored);
        return result("INSERT", [copyRow(stored)]);
      }
      if (text.startsWith("UPDATE ")) {
        const stored = this.database.heads.get(identity);
        if (
          stored === undefined ||
          stored.contract_version !== values[6] ||
          stored.sequence !== values[7] ||
          stored.last_checksum !== values[8] ||
          stored.ledger_bytes !== values[9]
        ) {
          return result("UPDATE");
        }
        const updated = {
          provider_id: providerId,
          anchor_id: anchorId,
          contract_version: values[2],
          sequence: values[3],
          last_checksum: values[4],
          ledger_bytes: values[5],
        };
        this.database.heads.set(identity, updated);
        return result("UPDATE", [copyRow(updated)]);
      }
    }
    throw new Error(`unexpected fake query: ${text}`);
  }

  async release(...args) {
    this.database.releaseCalls.push(args);
  }
}

function createFixture() {
  const database = new FakeHeadDatabase();
  const store = new PostgresSerializableStore({
    dedicatedPool: database.createPool(),
    maxTransactionAttempts: 1,
  });
  const createAnchor = (overrides = {}) =>
    createPostgresFilesystemImageProviderHeadAnchor({
      store,
      providerId: "filesystem-image-ext4",
      anchorId: "host-primary",
      ...overrides,
    });
  return { anchor: createAnchor(), createAnchor, database, store };
}

function anchorError(code) {
  return (error) =>
    error instanceof PostgresFilesystemImageProviderHeadAnchorError &&
    error.code === code &&
    error.retryable === false &&
    Object.isFrozen(error);
}

test("reads canonical genesis and exposes an exact receiver-safe native-Promise surface", async () => {
  const { anchor, database } = createFixture();
  assert.equal(database.queries.length, 0);
  assert.deepEqual(Reflect.ownKeys(anchor), ["readHead", "compareAndAdvance"]);
  assert.equal(Object.isFrozen(anchor), true);
  assert.equal(Object.isFrozen(anchor.readHead), true);
  assert.equal(Object.isFrozen(anchor.compareAndAdvance), true);

  const promise = Reflect.apply(anchor.readHead, { untrusted: true }, []);
  assert.equal(Object.getPrototypeOf(promise), Promise.prototype);
  const observed = await promise;
  assert.deepEqual(observed, GENESIS);
  assert.equal(Object.isFrozen(observed), true);
});

test("inserts genesis, reads across adapter restart, and advances by exact CAS", async () => {
  const { anchor, createAnchor, database } = createFixture();
  const first = head(1, "a", 512);
  assert.equal(
    await anchor.compareAndAdvance({ expectedHead: GENESIS, nextHead: first }),
    true,
  );
  const insertQuery = database.queries.find(([text]) =>
    text.startsWith("INSERT "),
  );
  assert.match(insertQuery[0], /ON CONFLICT \(provider_id, anchor_id\) DO NOTHING/u);
  assert.deepEqual(insertQuery[1], [
    "filesystem-image-ext4",
    "host-primary",
    1,
    1,
    "a".repeat(64),
    "512",
  ]);
  const observedFirst = await anchor.readHead();
  assert.deepEqual(observedFirst, first);
  assert.notEqual(observedFirst, first);
  assert.equal(Object.isFrozen(observedFirst), true);
  assert.deepEqual(await createAnchor().readHead(), first);

  const staleNext = head(2, "b", 1024);
  assert.equal(
    await anchor.compareAndAdvance({
      expectedHead: { ...first, lastChecksum: "f".repeat(64) },
      nextHead: staleNext,
    }),
    false,
  );
  assert.deepEqual(await anchor.readHead(), first);

  assert.equal(
    await anchor.compareAndAdvance({ expectedHead: first, nextHead: staleNext }),
    true,
  );
  const updateQueries = database.queries.filter(([text]) =>
    text.startsWith("UPDATE "),
  );
  assert.equal(updateQueries.length, 2);
  assert.match(
    updateQueries[0][0],
    /contract_version = \$7 AND sequence = \$8 AND last_checksum IS NOT DISTINCT FROM \$9 AND ledger_bytes = \$10::pg_catalog\.int8/u,
  );
  assert.deepEqual(updateQueries.map(([, values]) => values), [
    [
      "filesystem-image-ext4",
      "host-primary",
      1,
      2,
      "b".repeat(64),
      "1024",
      1,
      1,
      "f".repeat(64),
      "512",
    ],
    [
      "filesystem-image-ext4",
      "host-primary",
      1,
      2,
      "b".repeat(64),
      "1024",
      1,
      1,
      "a".repeat(64),
      "512",
    ],
  ]);
  assert.deepEqual(await createAnchor().readHead(), staleNext);

  assert.equal(
    await anchor.compareAndAdvance({
      expectedHead: GENESIS,
      nextHead: head(1, "c", 900),
    }),
    false,
  );
});

test("rejects non-consecutive, non-growing, and checksum-free advances before SQL", async () => {
  const { anchor, database } = createFixture();
  const before = database.queries.length;
  await assert.rejects(
    anchor.compareAndAdvance({
      expectedHead: GENESIS,
      nextHead: head(2, "a", 512),
    }),
    anchorError(
      "invalid_postgres_filesystem_image_provider_head_anchor_request",
    ),
  );
  await assert.rejects(
    anchor.compareAndAdvance({
      expectedHead: head(1, "a", 512),
      nextHead: head(2, "b", 512),
    }),
    anchorError(
      "invalid_postgres_filesystem_image_provider_head_anchor_request",
    ),
  );
  await assert.rejects(
    anchor.compareAndAdvance({
      expectedHead: GENESIS,
      nextHead: {
        contractVersion: FILESYSTEM_IMAGE_PROVIDER_STATE_HEAD_CONTRACT_VERSION,
        sequence: 1,
        lastChecksum: null,
        ledgerBytes: 512,
      },
    }),
    anchorError(
      "invalid_postgres_filesystem_image_provider_head_anchor_request",
    ),
  );
  assert.equal(database.queries.length, before);
});

test("concurrent advances from one exact head have one CAS winner", async () => {
  const { anchor, createAnchor } = createFixture();
  const first = head(1, "a", 512);
  const candidates = [head(2, "b", 1024), head(2, "c", 1536)];
  assert.equal(
    await anchor.compareAndAdvance({ expectedHead: GENESIS, nextHead: first }),
    true,
  );

  const outcomes = await Promise.all(
    candidates.map((nextHead) =>
      createAnchor().compareAndAdvance({
        expectedHead: first,
        nextHead,
      }),
    ),
  );
  assert.deepEqual([...outcomes].sort(), [false, true]);
  assert.deepEqual(
    await anchor.readHead(),
    candidates[outcomes.indexOf(true)],
  );
});

test("requires the store brand and rejects proxy or accessor options without execution", () => {
  const { store } = createFixture();
  assert.equal(isPostgresSerializableStore(store), true);
  const forged = Object.freeze(
    Object.create(PostgresSerializableStore.prototype),
  );
  assert.equal(isPostgresSerializableStore(forged), false);
  assert.throws(
    () =>
      createPostgresFilesystemImageProviderHeadAnchor({
        store: forged,
        providerId: "filesystem-image-ext4",
        anchorId: "host-primary",
      }),
    anchorError(
      "invalid_postgres_filesystem_image_provider_head_anchor_options",
    ),
  );

  let getterCalls = 0;
  const accessorOptions = {
    store,
    anchorId: "host-primary",
  };
  Object.defineProperty(accessorOptions, "providerId", {
    enumerable: true,
    get() {
      getterCalls += 1;
      return "filesystem-image-ext4";
    },
  });
  assert.throws(
    () => createPostgresFilesystemImageProviderHeadAnchor(accessorOptions),
    anchorError(
      "invalid_postgres_filesystem_image_provider_head_anchor_options",
    ),
  );
  assert.equal(getterCalls, 0);

  let proxyTrapCalls = 0;
  const proxyOptions = new Proxy(
    {
      store,
      providerId: "filesystem-image-ext4",
      anchorId: "host-primary",
    },
    {
      ownKeys() {
        proxyTrapCalls += 1;
        throw new Error("must not inspect proxy options");
      },
    },
  );
  assert.throws(
    () => createPostgresFilesystemImageProviderHeadAnchor(proxyOptions),
    anchorError(
      "invalid_postgres_filesystem_image_provider_head_anchor_options",
    ),
  );
  assert.equal(proxyTrapCalls, 0);

  for (const invalidIdentity of [
    { providerId: "../filesystem-image-ext4", anchorId: "host-primary" },
    { providerId: "filesystem-image-ext4", anchorId: "" },
    { providerId: "filesystem-image-ext4", anchorId: "a".repeat(129) },
  ]) {
    assert.throws(
      () =>
        createPostgresFilesystemImageProviderHeadAnchor({
          store,
          ...invalidIdentity,
        }),
      anchorError(
        "invalid_postgres_filesystem_image_provider_head_anchor_options",
      ),
    );
  }
  assert.throws(
    () =>
      createPostgresFilesystemImageProviderHeadAnchor({
        store,
        providerId: "filesystem-image-ext4",
        anchorId: "host-primary",
        unexpected: true,
      }),
    anchorError(
      "invalid_postgres_filesystem_image_provider_head_anchor_options",
    ),
  );
});

test("rejects proxy or accessor advance requests without executing traps", async () => {
  const { anchor } = createFixture();
  let getterCalls = 0;
  const accessorRequest = { nextHead: head(1, "a", 512) };
  Object.defineProperty(accessorRequest, "expectedHead", {
    enumerable: true,
    get() {
      getterCalls += 1;
      return GENESIS;
    },
  });
  const accessorPromise = Reflect.apply(
    anchor.compareAndAdvance,
    { untrusted: true },
    [accessorRequest],
  );
  assert.equal(Object.getPrototypeOf(accessorPromise), Promise.prototype);
  await assert.rejects(
    accessorPromise,
    anchorError(
      "invalid_postgres_filesystem_image_provider_head_anchor_request",
    ),
  );
  assert.equal(getterCalls, 0);

  let proxyTrapCalls = 0;
  const proxyRequest = new Proxy(
    { expectedHead: GENESIS, nextHead: head(1, "a", 512) },
    {
      ownKeys() {
        proxyTrapCalls += 1;
        throw new Error("must not inspect proxy request");
      },
    },
  );
  await assert.rejects(
    anchor.compareAndAdvance(proxyRequest),
    anchorError(
      "invalid_postgres_filesystem_image_provider_head_anchor_request",
    ),
  );
  assert.equal(proxyTrapCalls, 0);
});

test("malformed durable rows reject without exposing database contents", async () => {
  const { anchor, database } = createFixture();
  database.malformedReadRow = {
    provider_id: "filesystem-image-ext4",
    anchor_id: "host-primary",
    contract_version: 1,
    sequence: 1,
    last_checksum: "a".repeat(64),
    ledger_bytes: String(64 * 1024 * 1024 + 1),
  };
  await assert.rejects(
    anchor.readHead(),
    anchorError(
      "postgres_filesystem_image_provider_head_anchor_state_invalid",
    ),
  );

  database.malformedReadRow = {
    provider_id: "filesystem-image-ext4",
    anchor_id: "host-primary",
    contract_version: 1,
    sequence: 0,
    last_checksum: null,
    ledger_bytes: "0",
  };
  await assert.rejects(
    anchor.readHead(),
    anchorError(
      "postgres_filesystem_image_provider_head_anchor_state_invalid",
    ),
  );
});

test("rejects write results that do not report exactly one canonical row", async () => {
  const { anchor, database } = createFixture();
  const first = head(1, "a", 512);
  const stored = {
    provider_id: "filesystem-image-ext4",
    anchor_id: "host-primary",
    contract_version: 1,
    sequence: 1,
    last_checksum: "a".repeat(64),
    ledger_bytes: "512",
  };
  database.headQueryResultOverride = {
    command: "INSERT",
    rowCount: 2,
    rows: [stored, stored],
  };
  await assert.rejects(
    anchor.compareAndAdvance({ expectedHead: GENESIS, nextHead: first }),
    anchorError(
      "postgres_filesystem_image_provider_head_anchor_state_invalid",
    ),
  );

  database.headQueryResultOverride = result("INSERT", [
    { ...stored, last_checksum: "b".repeat(64) },
  ]);
  await assert.rejects(
    anchor.compareAndAdvance({ expectedHead: GENESIS, nextHead: first }),
    anchorError(
      "postgres_filesystem_image_provider_head_anchor_state_invalid",
    ),
  );
});

test("store query and COMMIT uncertainty reject through the adapter unchanged", async (t) => {
  await t.test("query failure", async () => {
    const { anchor, database } = createFixture();
    database.failHeadQueryOnce = true;
    await assert.rejects(anchor.readHead(), (error) => {
      assert.equal(error instanceof PostgresSerializableStoreError, true);
      assert.equal(error.code, "transaction_boundary_lost");
      assert.equal(error.commitState, "uncertain");
      return true;
    });
  });

  await t.test("commit acknowledgement loss", async () => {
    const { anchor, database } = createFixture();
    database.failCommitOnce = true;
    await assert.rejects(
      anchor.compareAndAdvance({
        expectedHead: GENESIS,
        nextHead: head(1, "a", 512),
      }),
      (error) => {
        assert.equal(error instanceof PostgresSerializableStoreError, true);
        assert.equal(error.code, "transaction_commit_outcome_uncertain");
        assert.equal(error.commitState, "uncertain");
        return true;
      },
    );
  });
});
