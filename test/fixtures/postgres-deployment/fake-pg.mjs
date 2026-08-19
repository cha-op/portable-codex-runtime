import { EventEmitter } from "node:events";

const PromiseConstructor = Promise;
const promiseThenIntrinsic = Promise.prototype.then;
const reflectApply = Reflect.apply;

const ROLES = Object.freeze([
  "authority",
  "operation",
  "foregroundLifecycle",
  "recoveryLifecycle",
]);
const rawQueryResults = new WeakSet();
const LANES = Object.freeze([
  "generation",
  "activation",
  "launch-attempt",
  "current-launch",
  "supervisor-state-gc",
]);

const state = {
  constructorFailureAt: null,
  cursors: new Map(),
  databaseName: "portable_codex_runtime",
  databaseUser: "portable_codex_runtime",
  backendPids: new Map(),
  endFailures: new Set(),
  endHolds: new Map(),
  endNonPromiseResults: new Set(),
  endSyncThrows: new Set(),
  endOrder: [],
  events: [],
  inRecovery: false,
  invalidConnectAccessorReads: 0,
  invalidConnectAt: null,
  invalidConnectKind: null,
  invalidClientQueryAccessorReads: 0,
  invalidClientQueryKinds: new Map(),
  lockHolders: new Map(),
  pools: [],
  recoveryScopeId: "deployment-recovery-001",
  serverVersionNum: "130000",
  synchronousConnectErrors: new Set(),
  topologyHolds: new Map(),
  topologyResults: new Map(),
  transactionReadOnly: "off",
  tryLockOverrides: new Map(),
  unlockOverrides: new Map(),
};

export class DatabaseError extends Error {}

function textOf(query) {
  return typeof query === "string" ? query : query?.text;
}

function valuesOf(query, values) {
  return typeof query === "string" ? values ?? [] : query?.values ?? [];
}

function resetState(overrides = {}) {
  state.constructorFailureAt = overrides.constructorFailureAt ?? null;
  state.cursors = new Map();
  state.databaseName =
    overrides.databaseName ?? "portable_codex_runtime";
  state.databaseUser =
    overrides.databaseUser ?? "portable_codex_runtime";
  state.backendPids = new Map(Object.entries(overrides.backendPids ?? {}));
  state.endFailures = new Set(overrides.endFailures ?? []);
  state.endHolds = new Map();
  state.endNonPromiseResults = new Set(
    overrides.endNonPromiseResults ?? [],
  );
  state.endSyncThrows = new Set(overrides.endSyncThrows ?? []);
  state.endOrder = [];
  state.events = [];
  state.inRecovery = overrides.inRecovery ?? false;
  state.invalidConnectAccessorReads = 0;
  state.invalidConnectAt = overrides.invalidConnectAt ?? null;
  state.invalidConnectKind = overrides.invalidConnectKind ?? null;
  state.invalidClientQueryAccessorReads = 0;
  state.invalidClientQueryKinds = new Map(
    Object.entries(overrides.invalidClientQueryKinds ?? {}),
  );
  state.lockHolders = new Map();
  state.pools = [];
  state.recoveryScopeId =
    overrides.recoveryScopeId ?? "deployment-recovery-001";
  state.serverVersionNum = overrides.serverVersionNum ?? "130000";
  state.synchronousConnectErrors = new Set(
    overrides.synchronousConnectErrors ?? [],
  );
  state.topologyHolds = new Map();
  state.topologyResults = new Map(
    Object.entries(overrides.topologyResults ?? {}),
  );
  state.transactionReadOnly = overrides.transactionReadOnly ?? "off";
  state.tryLockOverrides = new Map(
    Object.entries(overrides.tryLockOverrides ?? {}),
  );
  state.unlockOverrides = new Map(
    Object.entries(overrides.unlockOverrides ?? {}),
  );
}

export function configureFakePg(overrides) {
  resetState(overrides);
}

export function fakePgState() {
  return state;
}

export function isFakePgQueryResult(value) {
  return (
    value !== null &&
    typeof value === "object" &&
    rawQueryResults.has(value)
  );
}

export function holdFakePoolEnd(role) {
  let resolve;
  const promise = new Promise((resolvePromise) => {
    resolve = resolvePromise;
  });
  state.endHolds.set(role, { promise, resolve });
  return Object.freeze({ resolve });
}

export function holdFakeTopology(role) {
  let resolve;
  const promise = new Promise((resolvePromise) => {
    resolve = resolvePromise;
  });
  state.topologyHolds.set(role, { promise, resolve });
  return Object.freeze({ resolve });
}

function snapshotPoolOptions(options) {
  const snapshot = Object.create(null);
  for (const key of Reflect.ownKeys(options)) {
    const descriptor = Object.getOwnPropertyDescriptor(options, key);
    if (
      typeof key === "string" &&
      descriptor?.enumerable === true &&
      Object.hasOwn(descriptor, "value")
    ) {
      snapshot[key] = descriptor.value;
    }
  }
  if (
    snapshot.ssl !== null &&
    typeof snapshot.ssl === "object" &&
    !Array.isArray(snapshot.ssl)
  ) {
    snapshot.ssl = Object.assign(Object.create(null), snapshot.ssl);
  }
  return Object.freeze(snapshot);
}

export class Client extends EventEmitter {
  constructor(pool) {
    super();
    this.connection = new EventEmitter();
    this.doneCalls = [];
    this.pool = pool;
    this.probeLockKeys = [];
    this.releaseCalls = [];
    this.shared = false;
    this.exclusive = false;
    this.transactionLane = null;
  }

  query(query, values, callback) {
    if (
      typeof query === "object" &&
      query !== null &&
      typeof query.callback !== "function"
    ) {
      // Match pg@8.22 normalizeQueryConfig(): a positional callback is
      // assigned back onto an object config before Query construction.
      if (typeof values === "function") query.callback = values;
      if (typeof callback === "function") query.callback = callback;
    }
    if (typeof query === "object" && typeof query?.callback === "function") {
      this.#queryCallback(query);
      return undefined;
    }
    const positionalCallback =
      typeof values === "function"
        ? values
        : typeof callback === "function"
          ? callback
          : null;
    const result = this.#queryResult(
      textOf(query),
      valuesOf(query, typeof values === "function" ? undefined : values),
    );
    if (result instanceof PromiseConstructor) {
      Object.defineProperty(result, "constructor", {
        configurable: false,
        enumerable: false,
        value: PromiseConstructor,
        writable: false,
      });
      const pending = reflectApply(promiseThenIntrinsic, result, [
        (resolved) => markRawQueryResult(resolved),
      ]);
      if (positionalCallback === null) return pending;
      Object.defineProperty(pending, "constructor", {
        configurable: false,
        enumerable: false,
        value: PromiseConstructor,
        writable: false,
      });
      reflectApply(promiseThenIntrinsic, pending, [
        (resolved) => positionalCallback(null, resolved),
        (error) => positionalCallback(error),
      ]);
      return undefined;
    }
    const marked = markRawQueryResult(result);
    if (positionalCallback === null) return PromiseConstructor.resolve(marked);
    positionalCallback(null, marked);
    return undefined;
  }

  #queryCallback(query) {
    const { callback, text } = query;
    state.events.push(["query", this.pool.role, text]);
    if (text === "DISCARD ALL") {
      this.shared = false;
      this.exclusive = false;
      callback(null, { command: "DISCARD", rows: [] });
      return;
    }
    if (text.includes("pg_try_advisory_lock_shared")) {
      this.shared = true;
      callback(null, {
        command: "SELECT",
        rows: [{ acquired: true, backend_pid: this.pool.backendPid }],
      });
      return;
    }
    if (text.includes("pg_try_advisory_lock")) {
      if (!Array.isArray(query.values) || query.values.length !== 2) {
        this.exclusive = true;
        callback(null, {
          command: "SELECT",
          rows: [{ acquired: true, backend_pid: this.pool.backendPid }],
        });
        return;
      }
      assertInt32LockValues(query.values);
      this.probeLockKeys.push([...query.values]);
      const key = `${query.values[0]}:${query.values[1]}`;
      const holder = state.lockHolders.get(key);
      const acquired = holder === undefined || holder === this.pool;
      if (acquired) state.lockHolders.set(key, this.pool);
      this.exclusive = acquired;
      callback(null, {
        command: "SELECT",
        rows: [{ acquired, backend_pid: this.pool.backendPid }],
      });
      return;
    }
    if (text.includes("FROM pg_catalog.pg_locks")) {
      callback(null, {
        command: "SELECT",
        rows: [
          {
            backend_pid: this.pool.backendPid,
            lock_held: text.includes("ShareLock")
              ? this.shared
              : this.exclusive,
          },
        ],
      });
      return;
    }
    if (text.includes("pg_advisory_unlock_shared")) {
      const unlocked = this.shared;
      this.shared = false;
      callback(null, {
        command: "SELECT",
        rows: [{ backend_pid: this.pool.backendPid, unlocked }],
      });
      return;
    }
    if (text.includes("pg_advisory_unlock")) {
      if (!Array.isArray(query.values) || query.values.length !== 2) {
        const unlocked = this.exclusive;
        this.exclusive = false;
        callback(null, {
          command: "SELECT",
          rows: [{ backend_pid: this.pool.backendPid, unlocked }],
        });
        return;
      }
      assertInt32LockValues(query.values);
      const key = `${query.values[0]}:${query.values[1]}`;
      const unlocked = state.lockHolders.get(key) === this.pool;
      if (unlocked) state.lockHolders.delete(key);
      this.exclusive = false;
      callback(null, {
        command: "SELECT",
        rows: [{ backend_pid: this.pool.backendPid, unlocked }],
      });
      return;
    }
    let result;
    try {
      result = this.#queryResult(text, query.values, false);
    } catch (error) {
      callback(error);
      return;
    }
    if (result instanceof PromiseConstructor) {
      reflectApply(promiseThenIntrinsic, result, [
        (resolved) => callback(null, markRawQueryResult(resolved)),
        (error) => callback(error),
      ]);
      return;
    }
    callback(null, markRawQueryResult(result));
  }

  #queryResult(text, values, recordEvent = true) {
    if (recordEvent) state.events.push(["query", this.pool.role, text]);
    if (
      text.includes("current_database()") &&
      text.includes("database_user") &&
      text.includes("server_version_num") &&
      text.includes("pg_is_in_recovery()")
    ) {
      const hold = state.topologyHolds.get(this.pool.role);
      const topologyResult = () => {
        if (state.topologyResults.has(this.pool.role)) {
          return state.topologyResults.get(this.pool.role);
        }
        return {
          command: "SELECT",
          rows: [
            {
              backend_pid: this.pool.backendPid,
              database_name: state.databaseName,
              database_user: state.databaseUser,
              in_recovery: state.inRecovery,
              server_version_num: state.serverVersionNum,
              transaction_read_only: state.transactionReadOnly,
            },
          ],
        };
      };
      if (hold !== undefined) {
        return reflectApply(promiseThenIntrinsic, hold.promise, [
          topologyResult,
        ]);
      }
      return topologyResult();
    }
    if (text.includes("pg_try_advisory_lock")) {
      assertInt32LockValues(values);
      this.probeLockKeys.push([...values]);
      const key = `${values[0]}:${values[1]}`;
      const holder = state.lockHolders.get(key);
      const acquired = state.tryLockOverrides.has(this.pool.role)
        ? state.tryLockOverrides.get(this.pool.role)
        : holder === undefined || holder === this.pool;
      if (acquired) state.lockHolders.set(key, this.pool);
      return {
        command: "SELECT",
        rows: [{ acquired }],
      };
    }
    if (text.includes("pg_advisory_unlock")) {
      assertInt32LockValues(values);
      const key = `${values[0]}:${values[1]}`;
      const unlocked = state.unlockOverrides.has(this.pool.role)
        ? state.unlockOverrides.get(this.pool.role)
        : state.lockHolders.get(key) === this.pool;
      if (unlocked) state.lockHolders.delete(key);
      return { command: "SELECT", rows: [{ unlocked }] };
    }
    if (text === "DISCARD ALL") return { command: "DISCARD", rows: [] };
    if (
      text === "BEGIN" ||
      text === "BEGIN ISOLATION LEVEL SERIALIZABLE READ WRITE"
    ) {
      return { command: "BEGIN" };
    }
    if (text === "ROLLBACK") {
      this.transactionLane = null;
      return { command: "ROLLBACK" };
    }
    if (text === "COMMIT") {
      this.transactionLane = null;
      return { command: "COMMIT" };
    }
    if (
      text === "SET LOCAL search_path = pg_catalog" ||
      text === "SET LOCAL synchronous_commit = on"
    ) {
      return { command: "SET" };
    }
    if (text.includes("pg_advisory_xact_lock")) {
      return { command: "SELECT", rows: [{}] };
    }
    if (text === "CREATE SCHEMA IF NOT EXISTS session_authority") {
      return { command: "CREATE" };
    }
    if (
      text.includes(
        "CREATE TABLE IF NOT EXISTS session_authority.schema_migrations",
      )
    ) {
      return { command: "CREATE" };
    }
    if (
      text ===
      "SELECT version, checksum FROM session_authority.schema_migrations ORDER BY version"
    ) {
      return { command: "SELECT", rows: [] };
    }
    if (text.startsWith("INSERT INTO session_authority.schema_migrations")) {
      return { command: "INSERT", rowCount: 1, rows: [] };
    }
    if (text.includes("transaction_timestamp() AS transaction_timestamp")) {
      return {
        command: "SELECT",
        rows: [
          {
            transaction_id: "100",
            transaction_timestamp: new Date("2026-08-12T00:00:00.000Z"),
          },
        ],
      };
    }
    if (text.includes("pg_current_xact_id()::pg_catalog.text")) {
      return { command: "SELECT", rows: [{ transaction_id: "100" }] };
    }
    if (
      text.startsWith(
        "INSERT INTO session_authority.restore_recovery_cursors",
      )
    ) {
      const lane = values[1];
      this.transactionLane = lane;
      if (!state.cursors.has(lane)) {
        state.cursors.set(lane, {
          after_session_id: null,
          cycle: "0",
          lane,
          last_request_sha256: null,
          last_transition_id: null,
          recovery_scope_id: state.recoveryScopeId,
          revision: "0",
          updated_at: new Date("2026-08-12T00:00:00.000Z"),
        });
      }
      return { command: "INSERT", rowCount: 1, rows: [] };
    }
    if (text.includes("FROM session_authority.restore_recovery_cursors")) {
      const lane = values[1] ?? this.transactionLane;
      const row = state.cursors.get(lane);
      return { command: "SELECT", rows: row === undefined ? [] : [{ ...row }] };
    }
    if (text.startsWith("UPDATE session_authority.restore_recovery_cursors")) {
      const lane = values[1];
      const row = {
        after_session_id: values[2],
        cycle: values[3],
        lane,
        last_request_sha256: values[6],
        last_transition_id: values[5],
        recovery_scope_id: values[0],
        revision: values[4],
        updated_at: new Date(values[7]),
      };
      state.cursors.set(lane, row);
      return { command: "UPDATE", rowCount: 1, rows: [{ ...row }] };
    }
    if (text.startsWith("SELECT") && text.includes("session_authority.")) {
      return { command: "SELECT", rows: [] };
    }
    // Migration bodies are executed as one query and have no row contract.
    if (!text.startsWith("SELECT")) return { command: "OK", rows: [] };
    throw new Error(`unexpected promise query: ${text}`);
  }

  release(...args) {
    this.releaseCalls.push(args);
    state.events.push(["release", this.pool.role, args.length]);
  }
}

function markRawQueryResult(value) {
  if (Array.isArray(value)) {
    rawQueryResults.add(value);
    for (let index = 0; index < value.length; index += 1) {
      markRawQueryResult(value[index]);
    }
    return value;
  }
  if (value !== null && typeof value === "object") {
    if (
      Object.hasOwn(value, "command") &&
      !Object.hasOwn(value, "rows")
    ) {
      const normalized = { ...value, rows: [] };
      rawQueryResults.add(normalized);
      rawQueryResults.add(normalized.rows);
      return normalized;
    }
    rawQueryResults.add(value);
  }
  return value;
}

function assertInt32LockValues(values) {
  if (
    !Array.isArray(values) ||
    values.length !== 2 ||
    !values.every(
      (value) =>
        Number.isSafeInteger(value) &&
        value >= -2_147_483_648 &&
        value <= 2_147_483_647,
    )
  ) {
    throw new Error("deployment probe must use two opaque int32 lock keys");
  }
}

function createInvalidConnectPool(pool, kind) {
  const invalidPool = Object.create(null);
  for (const key of [
    "backendPid",
    "calls",
    "clients",
    "options",
    "role",
  ]) {
    Object.defineProperty(invalidPool, key, {
      configurable: false,
      enumerable: true,
      value: pool[key],
      writable: false,
    });
  }
  Object.defineProperty(invalidPool, "end", {
    configurable: false,
    enumerable: true,
    value: Pool.prototype.end,
    writable: false,
  });
  Object.defineProperty(invalidPool, "controlledConnectFailure", {
    configurable: false,
    enumerable: false,
    value: kind,
    writable: false,
  });
  if (kind === "accessor") {
    Object.defineProperty(invalidPool, "connect", {
      configurable: false,
      enumerable: true,
      get() {
        state.invalidConnectAccessorReads += 1;
        return Pool.prototype.connect;
      },
    });
  } else if (kind === "invalid") {
    Object.defineProperty(invalidPool, "connect", {
      configurable: false,
      enumerable: true,
      value: null,
      writable: false,
    });
  } else if (kind !== "missing") {
    throw new Error(`unknown invalid connect kind: ${kind}`);
  }
  return invalidPool;
}

function createInvalidQueryClient(pool, kind) {
  const client = new EventEmitter();
  Object.defineProperties(client, {
    doneCalls: {
      configurable: false,
      enumerable: true,
      value: [],
      writable: false,
    },
    pool: {
      configurable: false,
      enumerable: true,
      value: pool,
      writable: false,
    },
    releaseCalls: {
      configurable: false,
      enumerable: true,
      value: [],
      writable: false,
    },
    release: {
      configurable: false,
      enumerable: true,
      value(...args) {
        this.releaseCalls.push(args);
        state.events.push(["release", this.pool.role, args.length]);
      },
      writable: false,
    },
  });
  if (kind === "accessor") {
    Object.defineProperty(client, "query", {
      configurable: false,
      enumerable: true,
      get() {
        state.invalidClientQueryAccessorReads += 1;
        return FakeClient.prototype.query;
      },
    });
  } else if (kind !== "missing") {
    throw new Error(`unknown invalid client query kind: ${kind}`);
  }
  return client;
}

export class Pool extends EventEmitter {
  constructor(options) {
    super();
    const position = state.pools.length + 1;
    if (state.constructorFailureAt === position) {
      state.events.push(["construct-failure", position]);
      throw new Error(`controlled Pool constructor failure ${position}`);
    }
    this.backendPid = 40_000 + position;
    this.calls = { connect: 0, end: 0 };
    this.clients = [];
    this.options = snapshotPoolOptions(options);
    this.role = ROLES[(position - 1) % ROLES.length];
    if (state.backendPids.has(this.role)) {
      this.backendPid = state.backendPids.get(this.role);
    }
    const returnedPool =
      state.invalidConnectAt === position
        ? createInvalidConnectPool(this, state.invalidConnectKind)
        : this;
    state.pools.push(returnedPool);
    state.events.push(["construct", this.role]);
    if (returnedPool !== this) return returnedPool;
  }

  connect(callback) {
    this.calls.connect += 1;
    state.events.push(["connect", this.role]);
    if (this.controlledConnectFailure !== undefined) {
      throw new Error(
        `controlled ${this.role} ${this.controlledConnectFailure} connect failure`,
      );
    }
    if (state.synchronousConnectErrors.has(this.role)) {
      this.emit(
        "error",
        new Error(`controlled ${this.role} synchronous connect error`),
      );
    }
    const invalidQueryKind = state.invalidClientQueryKinds.get(this.role);
    const client =
      invalidQueryKind === undefined
        ? new Client(this)
        : createInvalidQueryClient(this, invalidQueryKind);
    this.clients.push(client);
    this.emit("connect", client);
    if (typeof callback === "function") {
      callback(null, client, (...args) => {
        client.doneCalls.push(args);
        state.events.push(["done", this.role, args.length]);
      });
      return undefined;
    }
    return Promise.resolve(client);
  }

  end(callback) {
    this.calls.end += 1;
    state.endOrder.push(this.role);
    state.events.push(["end", this.role]);
    if (state.endSyncThrows.has(this.role)) {
      throw new Error(`controlled ${this.role} synchronous end failure`);
    }
    if (state.endNonPromiseResults.has(this.role)) {
      callback(new Error(`controlled ${this.role} callback end failure`));
      return 17;
    }
    if (state.endFailures.has(this.role)) {
      callback(new Error(`controlled ${this.role} end failure`));
      return undefined;
    }
    if (state.endHolds.has(this.role)) {
      void state.endHolds.get(this.role).promise.then(() => callback(null));
      return undefined;
    }
    callback(null);
    return undefined;
  }

  emitIdleError() {
    this.emit("error", new Error(`controlled ${this.role} idle error`));
  }
}

resetState();

// Keep this list live so accidental lane-name drift is visible to the fixture.
void LANES;
