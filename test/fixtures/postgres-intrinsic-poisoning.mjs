import assert from "node:assert/strict";
import { Hash } from "node:crypto";
import { EventEmitter } from "node:events";

import { DatabaseError } from "pg";

import {
  PostgresSerializableStore,
} from "../../src/postgres-serializable-store.mjs";

const scenario = process.argv[2];
const scenarios = new Set([
  "array-includes",
  "database-error-brand",
  "hash-prototype",
  "object-command",
  "promise-prototype",
  "set-has",
  "weak-map-get",
]);
if (scenario !== undefined && !scenarios.has(scenario)) {
  throw new Error("unsupported intrinsic-poisoning scenario");
}

const localTimeout = new Error("local COMMIT response timeout");
const localSqlStateShape = Object.assign(
  new Error("local SQLSTATE-shaped COMMIT error"),
  { code: "40001", name: "error" },
);
const uncertainQueryError = new DatabaseError(
  "query response was lost",
  1,
  "error",
);
uncertainQueryError.code = "08006";
uncertainQueryError.severity = "ERROR";
const hashDigestDescriptor = Object.getOwnPropertyDescriptor(
  Hash.prototype,
  "digest",
);
const hashUpdateDescriptor = Object.getOwnPropertyDescriptor(
  Hash.prototype,
  "update",
);

class FixtureClient {
  constructor() {
    this.connection = new EventEmitter();
    this.queries = [];
    this.releaseCause = undefined;
  }

  async query(input) {
    const text = typeof input === "string" ? input : input.text;
    this.queries.push(text);
    if (text === "DISCARD ALL") return { command: "DISCARD" };
    if (scenario === "hash-prototype") {
      if (text === "BEGIN") return {};
      if (text === "SELECT pg_advisory_xact_lock($1::bigint)") {
        return {};
      }
      if (
        text === "CREATE SCHEMA IF NOT EXISTS session_authority" ||
        text.startsWith(
          "CREATE TABLE IF NOT EXISTS session_authority.schema_migrations",
        )
      ) {
        return {};
      }
      if (
        text.startsWith(
          "SELECT version, checksum FROM session_authority.schema_migrations",
        )
      ) {
        return {
          rows: [
            {
              checksum: "0".repeat(64),
              version: 1,
            },
          ],
        };
      }
      if (text === "COMMIT") return { command: "COMMIT" };
    }
    if (text === "BEGIN ISOLATION LEVEL SERIALIZABLE READ WRITE") return {};
    if (text.startsWith("SELECT transaction_timestamp()")) {
      return {
        rows: [
          {
            transaction_id: "1",
            transaction_timestamp: "2026-07-23T10:11:12.000Z",
          },
        ],
      };
    }
    if (text === "SELECT pg_current_xact_id()::text AS transaction_id") {
      return { rows: [{ transaction_id: "1" }] };
    }
    if (text === "SELECT fail") {
      this.connection.emit("errorMessage", uncertainQueryError);
      throw uncertainQueryError;
    }
    if (text === "COMMIT") {
      if (scenario === "database-error-brand") {
        this.connection.emit("errorMessage", localSqlStateShape);
        throw localSqlStateShape;
      }
      if (scenario === "object-command") return {};
      throw localTimeout;
    }
    if (text === "ROLLBACK") return { command: "ROLLBACK" };
    throw new Error(`unexpected fixture query: ${text}`);
  }

  async release(cause) {
    this.releaseCause = cause;
  }
}

class FixturePool {
  constructor(client) {
    this.client = client;
    this.connectCalls = 0;
  }

  async connect() {
    this.connectCalls += 1;
    if (this.connectCalls !== 1) {
      throw new Error("intrinsic poisoning triggered a retry");
    }
    return this.client;
  }
}

if (scenario !== undefined) {
  const client = new FixtureClient();
  const pool = new FixturePool(client);
  const store = new PostgresSerializableStore({
    dedicatedPool: pool,
    maxTransactionAttempts: 3,
  });
  let callbacks = 0;
  let caught;
  let restore;

  if (scenario === "hash-prototype") {
    let poisonedDigestCalls = 0;
    let poisonedUpdateCalls = 0;
    Object.defineProperty(Hash.prototype, "update", {
      ...hashUpdateDescriptor,
      value() {
        poisonedUpdateCalls += 1;
        return this;
      },
    });
    Object.defineProperty(Hash.prototype, "digest", {
      ...hashDigestDescriptor,
      value(encoding) {
        poisonedDigestCalls += 1;
        assert.equal(encoding, "hex");
        return "0".repeat(64);
      },
    });
    try {
      await store.migrate();
    } catch (error) {
      caught = error;
    } finally {
      Object.defineProperty(
        Hash.prototype,
        "digest",
        hashDigestDescriptor,
      );
      Object.defineProperty(
        Hash.prototype,
        "update",
        hashUpdateDescriptor,
      );
    }

    assert.notEqual(caught, undefined);
    assert.equal(caught.code, "migration_checksum_mismatch");
    assert.equal(caught.commitState, "not-committed");
    assert.equal(poisonedDigestCalls, 0);
    assert.equal(poisonedUpdateCalls, 0);
    assert.equal(pool.connectCalls, 1);
    assert.equal(client.releaseCause, undefined);
    assert.deepEqual(client.queries, [
      "DISCARD ALL",
      "BEGIN",
      "SELECT pg_advisory_xact_lock($1::bigint)",
      "CREATE SCHEMA IF NOT EXISTS session_authority",
      "CREATE TABLE IF NOT EXISTS session_authority.schema_migrations ( version integer PRIMARY KEY CHECK (version > 0), checksum character(64) NOT NULL CHECK (checksum ~ '^[0-9a-f]{64}$'), applied_at timestamp with time zone NOT NULL )",
      "SELECT version, checksum FROM session_authority.schema_migrations ORDER BY version",
      "ROLLBACK",
      "DISCARD ALL",
    ]);
    process.exit(0);
  }

  try {
    await store.runSerializable(async (transaction) => {
      callbacks += 1;
      if (scenario === "weak-map-get") {
        const descriptor = Object.getOwnPropertyDescriptor(
          WeakMap.prototype,
          "get",
        );
        Object.defineProperty(WeakMap.prototype, "get", {
          ...descriptor,
          value(key) {
            if (key === localTimeout) return "40001";
            return Reflect.apply(descriptor.value, this, [key]);
          },
        });
        restore = () => {
          Object.defineProperty(WeakMap.prototype, "get", descriptor);
        };
      } else if (scenario === "set-has") {
        const descriptor = Object.getOwnPropertyDescriptor(
          Set.prototype,
          "has",
        );
        Object.defineProperty(Set.prototype, "has", {
          ...descriptor,
          value(candidate) {
            if (candidate === undefined) return true;
            return Reflect.apply(descriptor.value, this, [candidate]);
          },
        });
        restore = () => {
          Object.defineProperty(Set.prototype, "has", descriptor);
        };
      } else if (scenario === "database-error-brand") {
        const descriptor = Object.getOwnPropertyDescriptor(
          DatabaseError,
          Symbol.hasInstance,
        );
        Object.defineProperty(DatabaseError, Symbol.hasInstance, {
          configurable: true,
          value: () => true,
        });
        restore = () => {
          if (descriptor === undefined) {
            delete DatabaseError[Symbol.hasInstance];
          } else {
            Object.defineProperty(
              DatabaseError,
              Symbol.hasInstance,
              descriptor,
            );
          }
        };
      } else if (scenario === "object-command") {
        const descriptor = Object.getOwnPropertyDescriptor(
          Object.prototype,
          "command",
        );
        Object.defineProperty(Object.prototype, "command", {
          configurable: true,
          value: "COMMIT",
          writable: true,
        });
        restore = () => {
          if (descriptor === undefined) {
            delete Object.prototype.command;
          } else {
            Object.defineProperty(
              Object.prototype,
              "command",
              descriptor,
            );
          }
        };
      } else if (scenario === "promise-prototype") {
        const constructorDescriptor = Object.getOwnPropertyDescriptor(
          Promise.prototype,
          "constructor",
        );
        const thenDescriptor = Object.getOwnPropertyDescriptor(
          Promise.prototype,
          "then",
        );
        const originalThen = thenDescriptor.value;
        Object.defineProperty(Promise.prototype, "constructor", {
          configurable: constructorDescriptor.configurable,
          enumerable: constructorDescriptor.enumerable,
          get() {
            throw new Error("poisoned Promise constructor getter");
          },
        });
        Object.defineProperty(Promise.prototype, "then", {
          ...thenDescriptor,
          value(onFulfilled, onRejected) {
            Reflect.apply(originalThen, this, [
              () => undefined,
              () => undefined,
            ]);
            return onFulfilled({ command: "COMMIT" });
          },
        });
        restore = () => {
          Object.defineProperty(
            Promise.prototype,
            "constructor",
            constructorDescriptor,
          );
          Object.defineProperty(
            Promise.prototype,
            "then",
            thenDescriptor,
          );
        };
      } else {
        const descriptor = Object.getOwnPropertyDescriptor(
          Array.prototype,
          "includes",
        );
        Object.defineProperty(Array.prototype, "includes", {
          ...descriptor,
          value(candidate, fromIndex) {
            if (candidate === "08") return false;
            return Reflect.apply(descriptor.value, this, [
              candidate,
              fromIndex,
            ]);
          },
        });
        restore = () => {
          Object.defineProperty(
            Array.prototype,
            "includes",
            descriptor,
          );
        };
        try {
          await transaction.query("SELECT fail");
        } catch {
          // The callback deliberately suppresses the query rejection.
        }
      }
      return "must-not-commit";
    });
  } catch (error) {
    caught = error;
  } finally {
    restore?.();
  }

  assert.notEqual(caught, undefined);
  assert.equal(callbacks, 1);
  assert.equal(pool.connectCalls, 1);
  if (scenario === "promise-prototype") {
    assert.equal(caught.message, "poisoned Promise constructor getter");
    assert.equal(client.releaseCause, undefined);
    assert.deepEqual(client.queries, [
      "DISCARD ALL",
      "BEGIN ISOLATION LEVEL SERIALIZABLE READ WRITE",
      "SELECT transaction_timestamp() AS transaction_timestamp, pg_current_xact_id()::text AS transaction_id",
      "ROLLBACK",
      "DISCARD ALL",
    ]);
  } else if (scenario === "array-includes") {
    assert.equal(caught.commitState, "uncertain");
    assert.equal(caught.code, "transaction_boundary_lost");
    assert.equal(client.releaseCause?.message, "transaction boundary lost");
    assert.deepEqual(client.queries, [
      "DISCARD ALL",
      "BEGIN ISOLATION LEVEL SERIALIZABLE READ WRITE",
      "SELECT transaction_timestamp() AS transaction_timestamp, pg_current_xact_id()::text AS transaction_id",
      "SELECT fail",
      "ROLLBACK",
    ]);
  } else {
    assert.equal(caught.commitState, "uncertain");
    assert.equal(caught.code, "transaction_commit_outcome_uncertain");
    if (scenario === "database-error-brand") {
      assert.equal(client.releaseCause, localSqlStateShape);
    } else if (scenario === "object-command") {
      assert.equal(
        client.releaseCause?.message,
        "invalid COMMIT acknowledgement",
      );
    } else {
      assert.equal(client.releaseCause, localTimeout);
    }
    assert.deepEqual(client.queries, [
      "DISCARD ALL",
      "BEGIN ISOLATION LEVEL SERIALIZABLE READ WRITE",
      "SELECT transaction_timestamp() AS transaction_timestamp, pg_current_xact_id()::text AS transaction_id",
      "SELECT pg_current_xact_id()::text AS transaction_id",
      "COMMIT",
      "ROLLBACK",
    ]);
  }
}
