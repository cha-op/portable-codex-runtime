import { EventEmitter } from "node:events";

import {
  PostgresSerializableStore,
  PostgresSerializableStoreError,
} from "../../src/postgres-serializable-store.mjs";

const scenario = process.argv[2];
const scenarios = new Set([
  "inactive",
  "boundary-escape",
  "invalid-signature",
  "invalid-values",
  "prototype-index-trap",
  "terminal-error",
]);

if (scenario !== undefined && !scenarios.has(scenario)) {
  throw new Error("unsupported fire-and-forget rejection scenario");
}

class FixtureClient {
  constructor() {
    this.connection = new EventEmitter();
  }

  async query(input) {
    const text = typeof input === "string" ? input : input.text;
    if (text === "DISCARD ALL") return { command: "DISCARD" };
    if (text === "BEGIN ISOLATION LEVEL SERIALIZABLE READ WRITE") {
      return {};
    }
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
    if (text === "ROLLBACK") return { command: "ROLLBACK" };
    if (text === "COMMIT") return { command: "COMMIT" };
    if (
      scenario === "prototype-index-trap" &&
      ["/", "-", "/*", ";"].includes(text)
    ) {
      return {};
    }
    throw new Error(`unexpected fixture query: ${text}`);
  }

  async release() {}
}

class FixturePool {
  constructor(client) {
    this.client = client;
  }

  async connect() {
    return this.client;
  }
}

function assertStoreError(error, code) {
  if (
    !(error instanceof PostgresSerializableStoreError) ||
    error.code !== code
  ) {
    throw error;
  }
}

if (scenario !== undefined) {
  const client = new FixtureClient();
  const store = new PostgresSerializableStore({
    dedicatedPool: new FixturePool(client),
  });

  if (scenario === "prototype-index-trap") {
    const properties = ["1", "2", "19"];
    const descriptors = new Map(
      properties.map((property) => [
        property,
        Object.getOwnPropertyDescriptor(String.prototype, property),
      ]),
    );
    for (const property of properties) {
      Object.defineProperty(String.prototype, property, {
        configurable: true,
        get() {
          throw new Error("String prototype index trap was invoked");
        },
      });
    }
    try {
      for (const text of ["/", "-", "/*", ";"]) {
        await store.runSerializable((transaction) =>
          transaction.query(text),
        );
      }
      try {
        await store.runSerializable((transaction) =>
          transaction.query("PREPARE TRANSACTION"),
        );
        throw new Error("transaction unexpectedly committed");
      } catch (error) {
        assertStoreError(error, "transaction_query_invalid");
      }
    } finally {
      for (const property of properties) {
        const descriptor = descriptors.get(property);
        if (descriptor === undefined) {
          delete String.prototype[property];
        } else {
          Object.defineProperty(String.prototype, property, descriptor);
        }
      }
    }
  } else if (scenario === "inactive") {
    let retainedTransaction;
    await store.runSerializable((transaction) => {
      retainedTransaction = transaction;
      return "committed";
    });
    void retainedTransaction.query("SELECT 1");
  } else {
    try {
      await store.runSerializable(async (transaction) => {
        if (scenario === "invalid-signature") {
          void transaction.query({ text: "SELECT 1" });
        } else if (scenario === "boundary-escape") {
          void transaction.query(
            "/* boundary */ PREPARE TRANSACTION 'portable-codex-runtime'",
          );
        } else if (scenario === "invalid-values") {
          void transaction.query("SELECT 1", {});
        } else {
          try {
            await transaction.query({ text: "SELECT 1" });
          } catch {
            // The first rejection is deliberately observed.
          }
          void transaction.query("SELECT 1");
        }
        return "must-not-commit";
      });
      throw new Error("transaction unexpectedly committed");
    } catch (error) {
      assertStoreError(error, "transaction_query_invalid");
    }
  }

  await new Promise((resolve) => setImmediate(resolve));
}
