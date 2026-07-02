import assert from "node:assert/strict";
import { createHash, createSecretKey } from "node:crypto";
import { chmod, mkdir, mkdtemp, readFile, rename, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";

import {
  AuthBroker,
  AuthBrokerError,
  authBrokerErrorMetadata,
} from "../src/auth-broker.mjs";
import { EncryptedAuthStateStore } from "../src/encrypted-auth-state-store.mjs";

const NOW = Date.parse("2026-07-03T00:00:00.000Z");
const ACCOUNT_ID = "account-1";
const OPAQUE_ID_PATTERN = /^[A-Za-z0-9][A-Za-z0-9._:-]{0,127}$/u;
const USER_ID = "user-1";

function isoAfter(seconds) {
  return new Date(NOW + seconds * 1000).toISOString();
}

function encodeJwt(payload) {
  return `${Buffer.from("{}").toString("base64url")}.${Buffer.from(JSON.stringify(payload)).toString("base64url")}.signature`;
}

function makeAccessToken({
  accountId = ACCOUNT_ID,
  expiresAt = isoAfter(3600),
  expirationSeconds = Date.parse(expiresAt) / 1000,
  marker = "access-1",
  planType = "enterprise",
  userId = USER_ID,
} = {}) {
  return encodeJwt({
    exp: expirationSeconds,
    jti: marker,
    "https://api.openai.com/auth": {
      chatgpt_account_id: accountId,
      chatgpt_plan_type: planType,
      chatgpt_user_id: userId,
    },
  });
}

function makeIdToken({
  accountId = ACCOUNT_ID,
  planType = "enterprise",
  userId = USER_ID,
} = {}) {
  return encodeJwt({
    "https://api.openai.com/auth": {
      chatgpt_account_id: accountId,
      chatgpt_plan_type: planType,
      chatgpt_user_id: userId,
    },
  });
}

function makeAuthJson({
  accessToken,
  accountId = ACCOUNT_ID,
  idToken = makeIdToken({ accountId }),
  refreshToken = "refresh-1",
}) {
  return JSON.stringify({
    auth_mode: "chatgpt",
    tokens: {
      access_token: accessToken,
      account_id: accountId,
      id_token: idToken,
      refresh_token: refreshToken,
    },
  });
}

function makeCredential(overrides = {}) {
  const accountId = overrides.accountId ?? ACCOUNT_ID;
  const expiresAt = overrides.expiresAt ?? isoAfter(3600);
  const planType = overrides.planType === undefined ? "enterprise" : overrides.planType;
  const userId = overrides.userId ?? USER_ID;
  const accessToken =
    overrides.accessToken ??
    makeAccessToken({
      accountId,
      expiresAt,
      marker: overrides.marker ?? "access-1",
      planType,
      userId,
    });
  const idToken =
    overrides.idToken ?? makeIdToken({ accountId, planType, userId });
  return {
    accessToken,
    accountId,
    authJson:
      overrides.authJson ??
      makeAuthJson({
        accessToken,
        accountId,
        idToken,
        refreshToken: overrides.refreshToken ?? "refresh-1",
      }),
    expiresAt,
    planType,
    userId,
  };
}

function refreshedCredential(overrides = {}) {
  return makeCredential({
    marker: "access-2",
    refreshToken: "refresh-2",
    ...overrides,
  });
}

function storedReadyPayload(credential) {
  return JSON.stringify({ schemaVersion: 1, status: "ready", ...credential });
}

function storedReservationPayload({
  accessToken = ACCESS_TOKEN_1,
  refreshToken = "refresh-1",
  reservationId = "stale-reservation-owner",
} = {}) {
  return JSON.stringify({
    schemaVersion: 1,
    status: "recovery-required",
    reason: "refresh_in_progress",
    reservationId,
    sourceAccessTokenHash: createHash("sha256").update(accessToken, "utf8").digest("hex"),
    sourceRefreshTokenHash: createHash("sha256").update(refreshToken, "utf8").digest("hex"),
  });
}

const ACCESS_TOKEN_1 = makeCredential().accessToken;
const ACCESS_TOKEN_2 = refreshedCredential().accessToken;
const ACCESS_TOKEN_3 = refreshedCredential({ marker: "access-3" }).accessToken;

class FakeStore {
  constructor({ coordinationKey = Symbol("auth-store") } = {}) {
    this.coordinationKey = coordinationKey;
    this.generation = "0";
    this.baseGeneration = null;
    this.commitId = null;
    this.payload = null;
    this.casCalls = [];
    this.afterCommit = null;
    this.afterRead = null;
    this.failAfterCommit = null;
  }

  async read() {
    if (this.payload === null) return null;
    const record = { commitId: this.commitId, generation: this.generation };
    Object.defineProperty(record, "payload", {
      enumerable: false,
      value: this.payload,
    });
    await this.afterRead?.(record);
    return Object.freeze(record);
  }

  async compareAndSwap({ expectedGeneration, commitId, payload }) {
    this.casCalls.push({ commitId, expectedGeneration });
    if (this.commitId === commitId) {
      if (this.baseGeneration !== expectedGeneration || this.payload !== payload) {
        throw Object.assign(new Error("commit replay conflict"), {
          code: "generation_conflict",
          retryable: true,
        });
      }
      return this.#result(commitId, payload, true, this.generation);
    }
    if (expectedGeneration !== this.generation) {
      throw Object.assign(new Error("generation conflict"), {
        code: "generation_conflict",
        retryable: true,
      });
    }
    assert.equal(typeof payload, "string");
    assert.equal(JSON.stringify(JSON.parse(payload)), payload);
    const committedGeneration = (BigInt(this.generation) + 1n).toString();
    this.baseGeneration = this.generation;
    this.commitId = commitId;
    this.payload = payload;
    this.generation = committedGeneration;
    await this.afterCommit?.({ commitId, payload });
    if (this.failAfterCommit) {
      const failure = this.failAfterCommit;
      this.failAfterCommit = null;
      throw failure;
    }
    return this.#result(commitId, payload, false, committedGeneration);
  }

  #result(commitId, payload, replayed, generation) {
    const result = {
      generation,
      commitId,
      keyId: "fake-key",
      replayed,
    };
    Object.defineProperty(result, "payload", {
      enumerable: false,
      value: payload,
    });
    return Object.freeze(result);
  }
}

function fakeRecord({ commitId, generation, payload }) {
  const record = { commitId, generation };
  Object.defineProperty(record, "payload", { enumerable: false, value: payload });
  return Object.freeze(record);
}

let brokerInstanceSequence = 0;

function makeBroker({
  minTokenTtlSeconds = 120,
  now = () => NOW,
  randomUUID,
  refreshAdapter = async () => refreshedCredential(),
  store = new FakeStore(),
} = {}) {
  const instanceId = ++brokerInstanceSequence;
  let sequence = 0;
  return new AuthBroker({
    minTokenTtlSeconds,
    now,
    randomUUID: randomUUID ?? (() => `uuid-${instanceId}-${++sequence}`),
    refreshAdapter,
    store,
  });
}

function simpleLockProvider() {
  return async () => ({
    async assertHeld() {},
    async release() {},
    async renameWhileHeld(source, destination) {
      await rename(source, destination);
    },
  });
}

async function createEncryptedStoreFixture(t) {
  const root = await mkdtemp(join(tmpdir(), "auth-broker-integration-"));
  const directory = join(root, "authority");
  await mkdir(directory, { mode: 0o700 });
  await chmod(directory, 0o700);
  t.after(() => rm(root, { force: true, recursive: true }));
  const key = createSecretKey(Buffer.alloc(32, 7));
  const keyProvider = {
    async activeKey() {
      return { keyId: "broker-key-001", key };
    },
    async getKey(keyId) {
      return keyId === "broker-key-001" ? key : undefined;
    },
  };
  const createStore = (overrides = {}) =>
    new EncryptedAuthStateStore({
      acquireLock: simpleLockProvider(),
      authorityId: "broker-authority-001",
      directory,
      keyProvider,
      ...overrides,
    });
  return { createStore, directory };
}

async function waitFor(predicate) {
  for (let attempt = 0; attempt < 100; attempt += 1) {
    if (predicate()) return;
    await new Promise((resolve) => setImmediate(resolve));
  }
  assert.fail("Timed out waiting for condition");
}

async function expectBrokerError(operation, code, expected = {}) {
  let caught;
  try {
    await operation();
  } catch (error) {
    caught = error;
  }
  assert.ok(caught instanceof AuthBrokerError);
  assert.equal(caught.code, code);
  assert.deepEqual(authBrokerErrorMetadata(caught), {
    code,
    retryable: caught.retryable,
    ...expected,
  });
  return caught;
}

function assertFencedBlockedSnapshot(
  snapshot,
  { generation, reason, reservationId, status },
) {
  const actualReservationId = snapshot.reservationId;
  assert.match(actualReservationId, OPAQUE_ID_PATTERN);
  if (reservationId !== undefined) assert.equal(actualReservationId, reservationId);
  assert.deepEqual(snapshot, {
    generation,
    reason,
    reservationId: actualReservationId,
    schemaVersion: 1,
    status,
  });
  return actualReservationId;
}

function assertReservationSnapshot(snapshot, { generation, reservationId } = {}) {
  return assertFencedBlockedSnapshot(snapshot, {
    generation,
    reason: "refresh_in_progress",
    reservationId,
    status: "recovery-required",
  });
}

test("TTL hit returns a non-serializable secret grant without refreshing", async () => {
  const store = new FakeStore();
  let refreshCalls = 0;
  const broker = makeBroker({
    refreshAdapter: async () => {
      refreshCalls += 1;
      return refreshedCredential();
    },
    store,
  });

  assert.deepEqual(await broker.snapshot(), {
    generation: "0",
    status: "uninitialized",
  });
  assert.deepEqual(await broker.installCredential(makeCredential()), {
    expiresAt: isoAfter(3600),
    generation: "1",
    schemaVersion: 1,
    status: "ready",
  });
  const grant = await broker.getGrant();

  assert.equal(refreshCalls, 0);
  assert.equal(grant.generation, "1");
  assert.equal(grant.accessToken, ACCESS_TOKEN_1);
  assert.equal(grant.accountId, ACCOUNT_ID);
  assert.equal(Object.hasOwn(grant, "authJson"), false);
  assert.equal(Object.hasOwn(grant, "userId"), false);
  assert.equal(JSON.stringify(grant), JSON.stringify({
    expiresAt: isoAfter(3600),
    generation: "1",
    planType: "enterprise",
  }));
  assert.deepEqual(await broker.snapshot(), {
    expiresAt: isoAfter(3600),
    generation: "1",
    schemaVersion: 1,
    status: "ready",
  });
});

test("caller TTL cannot lower the broker safety floor", async () => {
  const store = new FakeStore();
  let refreshCalls = 0;
  const broker = makeBroker({
    refreshAdapter: async () => {
      refreshCalls += 1;
      return refreshedCredential({ expiresAt: isoAfter(600) });
    },
    store,
  });
  await broker.installCredential(makeCredential({ expiresAt: isoAfter(30) }));

  const grant = await broker.getGrant({ minTtlSeconds: 0 });

  assert.equal(refreshCalls, 1);
  assert.equal(grant.generation, "3");
  assert.notEqual(grant.accessToken, ACCESS_TOKEN_1);
  assert.equal(grant.expiresAt, isoAfter(600));
});

test("invalid clock readings fail closed before returning credentials", async (t) => {
  for (const [name, now] of [
    ["NaN", () => Number.NaN],
    ["negative infinity", () => Number.NEGATIVE_INFINITY],
    [
      "throwing clock",
      () => {
        throw new Error("private clock failure");
      },
    ],
  ]) {
    await t.test(name, async () => {
      const store = new FakeStore();
      await makeBroker({ store }).installCredential(makeCredential());
      let refreshCalls = 0;
      const broker = makeBroker({
        now,
        refreshAdapter: async () => {
          refreshCalls += 1;
          return refreshedCredential();
        },
        store,
      });

      await expectBrokerError(() => broker.getGrant({ minTtlSeconds: 0 }), "invalid_request");
      assert.equal(refreshCalls, 0);
    });
  }
});

test("an invalid clock blocks forced and worker refresh before reservation", async () => {
  const store = new FakeStore({ coordinationKey: "invalid-clock-refresh" });
  let clock = NOW;
  let refreshCalls = 0;
  const broker = makeBroker({
    now: () => clock,
    refreshAdapter: async () => {
      refreshCalls += 1;
      return refreshedCredential();
    },
    store,
  });
  await broker.installCredential(makeCredential());
  await broker.workerLoginParams();
  clock = Number.NaN;

  await expectBrokerError(() => broker.refreshGrant(), "invalid_request");
  await expectBrokerError(
    () =>
      broker.handleWorkerRefresh({
        previousAccountId: ACCOUNT_ID,
        reason: "unauthorized",
      }),
    "invalid_request",
  );
  assert.equal(refreshCalls, 0);
  assert.deepEqual(await makeBroker({ store }).snapshot(), {
    expiresAt: isoAfter(3600),
    generation: "1",
    schemaVersion: 1,
    status: "ready",
  });
});

test("a new broker instance reads the durable generation from the store", async () => {
  const store = new FakeStore();
  await makeBroker({ store }).installCredential(makeCredential());

  const restarted = makeBroker({
    refreshAdapter: async () => assert.fail("restart should not refresh a valid token"),
    store,
  });
  const grant = await restarted.getGrant();

  assert.equal(grant.generation, "1");
  assert.equal(grant.accessToken, ACCESS_TOKEN_1);
  assert.deepEqual(await restarted.snapshot(), {
    expiresAt: isoAfter(3600),
    generation: "1",
    schemaVersion: 1,
    status: "ready",
  });
});

test("credential metadata must match auth mode and JWT claims", async (t) => {
  const base = makeCredential();
  const cases = [
    {
      name: "auth mode",
      credential: {
        ...base,
        authJson: JSON.stringify({
          ...JSON.parse(base.authJson),
          auth_mode: "api_key",
        }),
      },
    },
    {
      name: "access account identity",
      credential: {
        ...base,
        accessToken: makeAccessToken({ accountId: "account-2" }),
        authJson: makeAuthJson({
          accessToken: makeAccessToken({ accountId: "account-2" }),
          accountId: ACCOUNT_ID,
        }),
      },
    },
    {
      name: "access user identity",
      credential: {
        ...base,
        accessToken: makeAccessToken({ userId: "user-2" }),
        authJson: makeAuthJson({
          accessToken: makeAccessToken({ userId: "user-2" }),
          accountId: ACCOUNT_ID,
        }),
      },
    },
    {
      name: "access expiration",
      credential: {
        ...base,
        accessToken: makeAccessToken({ expiresAt: isoAfter(7200) }),
        authJson: makeAuthJson({
          accessToken: makeAccessToken({ expiresAt: isoAfter(7200) }),
          accountId: ACCOUNT_ID,
        }),
      },
    },
    {
      name: "access expiration outside the Date range",
      credential: {
        ...base,
        accessToken: makeAccessToken({ expirationSeconds: Number.MAX_VALUE }),
        authJson: makeAuthJson({
          accessToken: makeAccessToken({ expirationSeconds: Number.MAX_VALUE }),
          accountId: ACCOUNT_ID,
        }),
      },
    },
    {
      name: "plan type",
      credential: {
        ...base,
        accessToken: makeAccessToken({ planType: "business" }),
        authJson: makeAuthJson({
          accessToken: makeAccessToken({ planType: "business" }),
          accountId: ACCOUNT_ID,
        }),
      },
    },
    {
      name: "id token account identity",
      credential: {
        ...base,
        authJson: makeAuthJson({
          accessToken: base.accessToken,
          accountId: ACCOUNT_ID,
          idToken: makeIdToken({ accountId: "account-2" }),
        }),
      },
    },
  ];

  for (const scenario of cases) {
    await t.test(scenario.name, async () => {
      await expectBrokerError(
        () => makeBroker().installCredential(scenario.credential),
        "invalid_credential",
      );
    });
  }
});

test("store recovery and integrity failures are not reported as retryable outages", async (t) => {
  const cases = [
    { code: "recovery_required", expected: "recovery_required" },
    { code: "lock_release_failed", expected: "recovery_required" },
    { code: "invalid_auth_state", expected: "invalid_store_snapshot" },
    { code: "key_unavailable", expected: "store_unavailable" },
  ];

  for (const scenario of cases) {
    await t.test(scenario.code, async () => {
      const store = new FakeStore();
      store.read = async () => {
        throw Object.assign(new Error("private store detail"), { code: scenario.code });
      };
      await expectBrokerError(() => makeBroker({ store }).snapshot(), scenario.expected);
    });
  }
});

test("lock release failure stops mutation retry and requires recovery", async () => {
  const store = new FakeStore();
  let casCalls = 0;
  store.compareAndSwap = async () => {
    casCalls += 1;
    throw Object.assign(new Error("synthetic lock release failure"), {
      code: "lock_release_failed",
      commitState: "not-committed",
      retryable: false,
    });
  };
  const broker = makeBroker({ store });

  await expectBrokerError(() => broker.installCredential(makeCredential()), "recovery_required");
  assert.equal(casCalls, 1);
});

test("canonical payload parsing rejects whitespace, reordered fields, and duplicate keys", async (t) => {
  const source = new FakeStore();
  await makeBroker({ store: source }).installCredential(makeCredential());
  const parsed = JSON.parse(source.payload);
  const cases = [
    { name: "trailing whitespace", payload: `${source.payload}\n` },
    {
      name: "reordered fields",
      payload: JSON.stringify({ status: parsed.status, schemaVersion: parsed.schemaVersion, ...parsed }),
    },
    {
      name: "duplicate key",
      payload: source.payload.replace(
        '{"schemaVersion":1,',
        '{"schemaVersion":1,"schemaVersion":1,',
      ),
    },
  ];

  for (const scenario of cases) {
    await t.test(scenario.name, async () => {
      const store = new FakeStore();
      store.generation = "1";
      store.payload = scenario.payload;
      await expectBrokerError(() => makeBroker({ store }).snapshot(), "invalid_store_snapshot");
    });
  }
});

test("store generations must be canonical uint64 decimal strings", async (t) => {
  const source = new FakeStore();
  await makeBroker({ store: source }).installCredential(makeCredential());

  for (const generation of [1, "01", "18446744073709551616"]) {
    await t.test(String(generation), async () => {
      const store = new FakeStore();
      store.generation = generation;
      store.payload = source.payload;
      await expectBrokerError(() => makeBroker({ store }).snapshot(), "invalid_store_snapshot");
    });
  }
});

test("CAS result generation must equal expected generation plus one", async () => {
  const store = new FakeStore();
  const compareAndSwap = store.compareAndSwap.bind(store);
  store.compareAndSwap = async (request) => {
    const valid = await compareAndSwap(request);
    const malformed = { ...valid, generation: "2" };
    Object.defineProperty(malformed, "payload", {
      enumerable: false,
      value: valid.payload,
    });
    return Object.freeze(malformed);
  };

  await expectBrokerError(
    () => makeBroker({ store }).installCredential(makeCredential()),
    "invalid_store_snapshot",
  );
});

test("uint64 string generations remain exact above Number.MAX_SAFE_INTEGER", async () => {
  const store = new FakeStore();
  const broker = makeBroker({ store });
  await broker.installCredential(makeCredential());
  store.generation = "9007199254740993";

  assert.equal((await broker.snapshot()).generation, "9007199254740993");
  const grant = await broker.refreshGrant();
  assert.equal(grant.generation, "9007199254740995");
  assert.equal(store.casCalls.at(-1).expectedGeneration, "9007199254740994");
});

test("real encrypted store persists broker generations across restart without plaintext tokens", async (t) => {
  const { createStore, directory } = await createEncryptedStoreFixture(t);
  const initial = makeCredential({
    marker: "access-secret-install-sentinel",
    refreshToken: "refresh-secret-install-sentinel",
  });
  const refreshed = makeCredential({
    marker: "access-secret-refresh-sentinel",
    refreshToken: "refresh-secret-refresh-sentinel",
  });
  const first = makeBroker({ store: createStore() });

  assert.equal((await first.installCredential(initial)).generation, "1");

  let refreshCalls = 0;
  const restarted = makeBroker({
    refreshAdapter: async () => {
      refreshCalls += 1;
      return refreshed;
    },
    store: createStore(),
  });
  const ttlGrant = await restarted.getGrant();
  assert.equal(ttlGrant.generation, "1");
  assert.equal(ttlGrant.accessToken, initial.accessToken);
  assert.equal(refreshCalls, 0);

  const refreshedGrant = await restarted.refreshGrant();
  assert.equal(refreshedGrant.generation, "3");
  assert.equal(refreshedGrant.accessToken, refreshed.accessToken);
  assert.equal(refreshCalls, 1);

  const finalStore = createStore();
  const finalBroker = makeBroker({
    refreshAdapter: async () => assert.fail("durable refreshed token should satisfy TTL"),
    store: finalStore,
  });
  const durableGrant = await finalBroker.getGrant();
  assert.equal(durableGrant.generation, "3");
  assert.equal(durableGrant.accessToken, refreshed.accessToken);
  assert.equal((await finalStore.read()).generation, "3");

  const encrypted = await readFile(join(directory, "auth-state.enc"), "utf8");
  for (const secret of [
    initial.accessToken,
    "refresh-secret-install-sentinel",
    refreshed.accessToken,
    "refresh-secret-refresh-sentinel",
  ]) {
    assert.equal(encrypted.includes(secret), false);
  }
});

test("real encrypted store replays a refresh after post-rename acknowledgement loss", async (t) => {
  const { createStore } = await createEncryptedStoreFixture(t);
  let failNextDirectorySync = false;
  const store = createStore({
    async syncDirectory({ handle }) {
      await handle.sync();
      if (failNextDirectorySync) {
        failNextDirectorySync = false;
        throw new Error("simulated acknowledgement loss");
      }
    },
  });
  let refreshCalls = 0;
  const broker = makeBroker({
    refreshAdapter: async () => {
      refreshCalls += 1;
      failNextDirectorySync = true;
      return refreshedCredential();
    },
    store,
  });
  await broker.installCredential(makeCredential());

  const grant = await broker.refreshGrant();

  assert.equal(grant.generation, "3");
  assert.equal(grant.accessToken, ACCESS_TOKEN_2);
  assert.equal(refreshCalls, 1);
  assert.equal((await store.read()).generation, "3");
});

test("real encrypted store never publishes a refresh before directory sync is proven", async (t) => {
  const { createStore } = await createEncryptedStoreFixture(t);
  let allowDirectorySync = true;
  const store = createStore({
    async syncDirectory({ handle }) {
      if (!allowDirectorySync) throw new Error("simulated pre-sync failure");
      await handle.sync();
    },
  });
  let refreshCalls = 0;
  const broker = makeBroker({
    refreshAdapter: async () => {
      refreshCalls += 1;
      allowDirectorySync = false;
      return refreshedCredential();
    },
    store,
  });
  await broker.installCredential(makeCredential());

  await expectBrokerError(() => broker.refreshGrant(), "refresh_outcome_uncertain");
  assert.equal(refreshCalls, 1);
  await expectBrokerError(
    () => makeBroker({ store }).getGrant({ minTtlSeconds: 0 }),
    "store_unavailable",
  );

  allowDirectorySync = true;
  const proven = await makeBroker({
    refreshAdapter: async () => assert.fail("directory proof must not redispatch OAuth"),
    store,
  }).getGrant({ minTtlSeconds: 0 });
  assert.equal(proven.generation, "3");
  assert.equal(proven.accessToken, ACCESS_TOKEN_2);
  assert.equal(refreshCalls, 1);
});

test("refresh rebases across a concurrent storage-only key rotation", async (t) => {
  const { createStore } = await createEncryptedStoreFixture(t);
  const store = createStore();
  let refreshCalls = 0;
  const broker = makeBroker({
    refreshAdapter: async ({ expectedGeneration }) => {
      refreshCalls += 1;
      await store.rotateEncryption({
        expectedGeneration,
        commitId: "rotation-during-refresh",
      });
      return refreshedCredential();
    },
    store,
  });
  await broker.installCredential(makeCredential());

  const grant = await broker.refreshGrant();

  assert.equal(grant.generation, "4");
  assert.equal(grant.accessToken, ACCESS_TOKEN_2);
  assert.equal(refreshCalls, 1);
  assert.equal((await store.read()).generation, "4");
});

test("reservation ownership survives a post-CAS storage-only rotation", async () => {
  const store = new FakeStore();
  let refreshCalls = 0;
  const broker = makeBroker({
    refreshAdapter: async () => {
      refreshCalls += 1;
      return refreshedCredential();
    },
    store,
  });
  await broker.installCredential(makeCredential());
  store.afterCommit = async ({ payload }) => {
    const parsed = JSON.parse(payload);
    if (parsed.reason !== "refresh_in_progress") return;
    store.baseGeneration = store.generation;
    store.generation = (BigInt(store.generation) + 1n).toString();
    store.commitId = "rotation-after-reservation";
    store.afterCommit = null;
  };

  const grant = await broker.refreshGrant();

  assert.equal(grant.accessToken, ACCESS_TOKEN_2);
  assert.equal(grant.generation, "4");
  assert.equal(refreshCalls, 1);
});

test("reservation reread preserves recovery and integrity error classes", async (t) => {
  const cases = [
    { code: "lock_release_failed", expected: "recovery_required" },
    { code: "invalid_auth_state", expected: "invalid_store_snapshot" },
  ];

  for (const scenario of cases) {
    await t.test(scenario.code, async () => {
      const store = new FakeStore();
      const broker = makeBroker({ store });
      await broker.installCredential(makeCredential());
      const read = store.read.bind(store);
      let refreshReads = 0;
      let reservationCasCalls = 0;
      store.read = async () => {
        refreshReads += 1;
        if (refreshReads === 3) {
          throw Object.assign(new Error("reservation reread failed"), {
            code: scenario.code,
          });
        }
        return read();
      };
      store.compareAndSwap = async () => {
        reservationCasCalls += 1;
        throw Object.assign(new Error("synthetic reservation conflict"), {
          code: "generation_conflict",
          retryable: true,
        });
      };

      await expectBrokerError(() => broker.refreshGrant(), scenario.expected);
      assert.equal(reservationCasCalls, 1);
      assert.equal(refreshReads, 3);
    });
  }
});

test("foreign reservation in an ABA sequence never grants dispatch ownership", async () => {
  const store = new FakeStore();
  let refreshCalls = 0;
  const broker = makeBroker({
    refreshAdapter: async () => {
      refreshCalls += 1;
      return refreshedCredential();
    },
    store,
  });
  await broker.installCredential(makeCredential());
  store.afterCommit = async ({ payload }) => {
    const parsed = JSON.parse(payload);
    if (parsed.reason !== "refresh_in_progress") return;
    store.baseGeneration = (BigInt(store.generation) + 1n).toString();
    store.generation = (BigInt(store.generation) + 2n).toString();
    store.commitId = "foreign-reservation-after-reinstall";
    store.payload = storedReservationPayload({ reservationId: "foreign-reservation-owner" });
    store.afterCommit = null;
  };

  await expectBrokerError(() => broker.refreshGrant(), "cas_conflict", {
    generation: "4",
  });
  assert.equal(refreshCalls, 0);
  assertReservationSnapshot(await broker.snapshot(), {
    generation: "4",
    reservationId: "foreign-reservation-owner",
  });
});

test("ordinary credential installation cannot take over an active reservation", async () => {
  const store = new FakeStore({ coordinationKey: "active-reservation-install" });
  let adapterStarted = false;
  let releaseAdapter;
  const adapterGate = new Promise((resolve) => {
    releaseAdapter = resolve;
  });
  const broker = makeBroker({
    refreshAdapter: async () => {
      adapterStarted = true;
      await adapterGate;
      return refreshedCredential();
    },
    store,
  });
  await broker.installCredential(makeCredential());

  const pending = broker.refreshGrant();
  await waitFor(() => adapterStarted);
  await expectBrokerError(
    () => makeBroker({ store }).installCredential(refreshedCredential()),
    "recovery_required",
    {
      generation: "2",
      reason: "refresh_in_progress",
      status: "recovery-required",
    },
  );
  releaseAdapter();

  assert.equal((await pending).generation, "3");
});

test("explicit fenced recovery requires the exact reservation identity", async () => {
  const store = new FakeStore();
  const broker = makeBroker({ store });
  await broker.installCredential(makeCredential());
  const reservationId = "crashed-reservation-owner";
  store.baseGeneration = "1";
  store.commitId = "crashed-reservation-commit";
  store.generation = "2";
  store.payload = storedReservationPayload({ reservationId });
  const recovered = makeCredential({
    marker: "access-recovered",
    refreshToken: "refresh-recovered",
  });

  await expectBrokerError(
    () =>
      broker.recoverRefreshReservation({
        credential: recovered,
        expectedGeneration: "2",
        reservationId: "wrong-reservation-owner",
      }),
    "invalid_request",
  );
  const recoverySnapshot = await broker.snapshot();
  assert.equal(
    assertReservationSnapshot(recoverySnapshot, { generation: "2", reservationId }),
    reservationId,
  );
  await expectBrokerError(
    () =>
      broker.recoverRefreshReservation({
        credential: makeCredential({ refreshToken: "refresh-recovered" }),
        expectedGeneration: recoverySnapshot.generation,
        reservationId: recoverySnapshot.reservationId,
      }),
    "invalid_credential",
    { reason: "access_token_unchanged" },
  );
  await expectBrokerError(
    () =>
      broker.recoverRefreshReservation({
        credential: refreshedCredential({ refreshToken: "refresh-1" }),
        expectedGeneration: recoverySnapshot.generation,
        reservationId: recoverySnapshot.reservationId,
      }),
    "invalid_credential",
    { reason: "refresh_token_reused" },
  );
  assert.deepEqual(
    await broker.recoverRefreshReservation({
      credential: recovered,
      expectedGeneration: recoverySnapshot.generation,
      reservationId: recoverySnapshot.reservationId,
    }),
    {
      expiresAt: isoAfter(3600),
      generation: "3",
      schemaVersion: 1,
      status: "ready",
    },
  );
  assert.equal((await broker.getGrant()).accessToken, recovered.accessToken);
});

test("twenty concurrent callers share one refresh", async () => {
  const store = new FakeStore();
  let refreshCalls = 0;
  let releaseRefresh;
  const refreshGate = new Promise((resolve) => {
    releaseRefresh = resolve;
  });
  const broker = makeBroker({
    refreshAdapter: async () => {
      refreshCalls += 1;
      await refreshGate;
      return refreshedCredential();
    },
    store,
  });
  await broker.installCredential(makeCredential({ expiresAt: isoAfter(30) }));

  const pending = Array.from({ length: 20 }, () => broker.getGrant());
  await waitFor(() => refreshCalls === 1);
  releaseRefresh();
  const grants = await Promise.all(pending);

  assert.equal(refreshCalls, 1);
  assert.equal(store.generation, "3");
  assert.deepEqual(
    grants.map(({ accessToken, generation }) => ({ accessToken, generation })),
    Array.from({ length: 20 }, () => ({ accessToken: ACCESS_TOKEN_2, generation: "3" })),
  );
});

test("two broker objects with the same coordination key coalesce refresh", async () => {
  const store = new FakeStore({ coordinationKey: "shared-store" });
  let refreshCalls = 0;
  let releaseRefresh;
  const refreshGate = new Promise((resolve) => {
    releaseRefresh = resolve;
  });
  const adapter = async () => {
    refreshCalls += 1;
    await refreshGate;
    return refreshedCredential();
  };
  const first = makeBroker({ refreshAdapter: adapter, store });
  const second = makeBroker({ refreshAdapter: adapter, store });
  await first.installCredential(makeCredential());

  const pending = [first.refreshGrant(), second.refreshGrant()];
  await waitFor(() => refreshCalls === 1);
  releaseRefresh();
  const grants = await Promise.all(pending);

  assert.equal(refreshCalls, 1);
  assert.deepEqual(grants.map((grant) => grant.generation), ["3", "3"]);
  assert.deepEqual(grants.map((grant) => grant.accessToken), [ACCESS_TOKEN_2, ACCESS_TOKEN_2]);
});

test("caller TTL is checked after shared refresh without durably blocking the authority", async () => {
  const store = new FakeStore();
  let refreshCalls = 0;
  let releaseRefresh;
  const refreshGate = new Promise((resolve) => {
    releaseRefresh = resolve;
  });
  const broker = makeBroker({
    refreshAdapter: async () => {
      refreshCalls += 1;
      await refreshGate;
      return refreshedCredential({ expiresAt: isoAfter(180) });
    },
    store,
  });
  await broker.installCredential(makeCredential({ expiresAt: isoAfter(30) }));

  const lowerTtl = broker.getGrant({ minTtlSeconds: 60 });
  await waitFor(() => refreshCalls === 1);
  const higherTtl = broker.getGrant({ minTtlSeconds: 300 });
  releaseRefresh();

  assert.equal((await lowerTtl).generation, "3");
  await expectBrokerError(() => higherTtl, "token_ttl_insufficient", {
    generation: "3",
    reason: "token_ttl_insufficient",
  });
  assert.equal(refreshCalls, 1);
  assert.deepEqual(await broker.snapshot(), {
    expiresAt: isoAfter(180),
    generation: "3",
    schemaVersion: 1,
    status: "ready",
  });
});

test("concurrent brokers with incompatible refresh configuration fail closed", async () => {
  const store = new FakeStore({ coordinationKey: "configuration-mismatch" });
  let refreshCalls = 0;
  let releaseRefresh;
  const refreshGate = new Promise((resolve) => {
    releaseRefresh = resolve;
  });
  const adapter = async () => {
    refreshCalls += 1;
    await refreshGate;
    return refreshedCredential({ expiresAt: isoAfter(600) });
  };
  const first = makeBroker({ minTokenTtlSeconds: 120, refreshAdapter: adapter, store });
  const second = makeBroker({ minTokenTtlSeconds: 300, refreshAdapter: adapter, store });
  await first.installCredential(makeCredential());

  const pending = first.refreshGrant();
  await waitFor(() => refreshCalls === 1);
  await expectBrokerError(() => second.refreshGrant(), "invalid_request");
  releaseRefresh();
  assert.equal((await pending).generation, "3");
  assert.equal(refreshCalls, 1);
});

test("invalid refresh candidates durably require recovery", async (t) => {
  const cases = [
    {
      name: "account identity changes",
      candidate: refreshedCredential({ accountId: "account-2" }),
      reason: "account_identity_changed",
    },
    {
      name: "user identity changes",
      candidate: refreshedCredential({ userId: "user-2" }),
      reason: "user_identity_changed",
    },
    {
      name: "access token is unchanged",
      candidate: makeCredential({ refreshToken: "refresh-2" }),
      reason: "access_token_unchanged",
    },
    {
      name: "refresh token is unchanged",
      candidate: refreshedCredential({ refreshToken: "refresh-1" }),
      reason: "refresh_token_reused",
    },
    {
      name: "token lifetime is too short",
      candidate: refreshedCredential({ expiresAt: isoAfter(30) }),
      reason: "token_ttl_insufficient",
    },
  ];

  for (const scenario of cases) {
    await t.test(scenario.name, async () => {
      const store = new FakeStore();
      const broker = makeBroker({
        refreshAdapter: async () => scenario.candidate,
        store,
      });
      await broker.installCredential(makeCredential());

      await expectBrokerError(() => broker.refreshGrant(), "recovery_required", {
        generation: "3",
        reason: scenario.reason,
        status: "recovery-required",
      });
      assertFencedBlockedSnapshot(await broker.snapshot(), {
        generation: "3",
        reason: scenario.reason,
        status: "recovery-required",
      });
      await expectBrokerError(() => makeBroker({ store }).getGrant(), "recovery_required", {
        generation: "3",
        reason: scenario.reason,
        status: "recovery-required",
      });
    });
  }
});

test("structured refresh failures durably block future grants", async (t) => {
  const cases = [
    {
      code: "invalid_grant",
      expectedCode: "reauth_required",
      name: "reauthentication required",
      reason: "invalid_grant",
      status: "reauth-required",
    },
    {
      code: "refresh_token_reused",
      expectedCode: "reauth_required",
      name: "refresh token reused",
      reason: "refresh_token_reused",
      status: "reauth-required",
    },
    {
      code: "refresh_outcome_uncertain",
      expectedCode: "recovery_required",
      name: "post-dispatch result uncertain",
      postDispatch: true,
      reason: "adapter_post_dispatch_uncertain",
      status: "recovery-required",
    },
    {
      code: "refresh_outcome_uncertain",
      expectedCode: "recovery_required",
      name: "reservation-only reason is normalized",
      postDispatch: true,
      adapterReason: "refresh_in_progress",
      reason: "adapter_post_dispatch_uncertain",
      status: "recovery-required",
    },
  ];

  for (const scenario of cases) {
    await t.test(scenario.name, async () => {
      const store = new FakeStore();
      const broker = makeBroker({
        refreshAdapter: async () => {
          throw Object.assign(new Error("adapter details must remain private"), {
            code: scenario.code,
            postDispatch: scenario.postDispatch,
            reason: scenario.adapterReason ?? scenario.reason,
          });
        },
        store,
      });
      await broker.installCredential(makeCredential());

      await expectBrokerError(() => broker.refreshGrant(), scenario.expectedCode, {
        generation: "3",
        reason: scenario.reason,
        status: scenario.status,
      });
      const restarted = makeBroker({ store });
      await expectBrokerError(() => restarted.getGrant(), scenario.expectedCode, {
        generation: "3",
        reason: scenario.reason,
        status: scenario.status,
      });
      assertFencedBlockedSnapshot(await restarted.snapshot(), {
        generation: "3",
        reason: scenario.reason,
        status: scenario.status,
      });
    });
  }
});

test("post-dispatch blocks retain the recovery fence", async () => {
  const store = new FakeStore();
  const broker = makeBroker({
    refreshAdapter: async () => {
      throw Object.assign(new Error("uncertain post-dispatch result"), {
        postDispatch: true,
      });
    },
    store,
  });
  const original = makeCredential();
  await broker.installCredential(original);
  await expectBrokerError(() => broker.refreshGrant(), "recovery_required", {
    generation: "3",
    reason: "adapter_post_dispatch_uncertain",
    status: "recovery-required",
  });
  const snapshot = await broker.snapshot();
  const reservationId = assertFencedBlockedSnapshot(snapshot, {
    generation: "3",
    reason: "adapter_post_dispatch_uncertain",
    status: "recovery-required",
  });

  await expectBrokerError(() => broker.installCredential(original), "recovery_required", {
    generation: "3",
    reason: "adapter_post_dispatch_uncertain",
    status: "recovery-required",
  });
  await expectBrokerError(
    () =>
      broker.recoverRefreshReservation({
        credential: original,
        expectedGeneration: snapshot.generation,
        reservationId,
      }),
    "invalid_credential",
    { reason: "access_token_unchanged" },
  );
  const recovered = makeCredential({
    marker: "access-recovered-after-block",
    refreshToken: "refresh-recovered-after-block",
  });
  assert.deepEqual(
    await broker.recoverRefreshReservation({
      credential: recovered,
      expectedGeneration: snapshot.generation,
      reservationId,
    }),
    {
      expiresAt: isoAfter(3600),
      generation: "4",
      schemaVersion: 1,
      status: "ready",
    },
  );
});

test("permanent refresh failure rebases its block across storage-only rotation", async () => {
  const store = new FakeStore();
  const broker = makeBroker({
    refreshAdapter: async () => {
      store.baseGeneration = store.generation;
      store.generation = (BigInt(store.generation) + 1n).toString();
      store.commitId = "rotation-before-reauth-block";
      throw Object.assign(new Error("structured invalid grant"), {
        code: "invalid_grant",
      });
    },
    store,
  });
  await broker.installCredential(makeCredential());

  await expectBrokerError(() => broker.refreshGrant(), "reauth_required", {
    generation: "4",
    reason: "invalid_grant",
    status: "reauth-required",
  });
  assertFencedBlockedSnapshot(await broker.snapshot(), {
    generation: "4",
    reason: "invalid_grant",
    status: "reauth-required",
  });
});

test("CAS conflict never returns an uncommitted refreshed token", async () => {
  const store = new FakeStore();
  const broker = makeBroker({
    refreshAdapter: async () => {
      const competing = makeCredential({
        accountId: "account-2",
        marker: "access-competing-before-cas",
        userId: "user-2",
      });
      store.payload = storedReadyPayload(competing);
      store.baseGeneration = store.generation;
      store.generation = (BigInt(store.generation) + 1n).toString();
      store.commitId = "competing-before-cas";
      return refreshedCredential();
    },
    store,
  });
  await broker.installCredential(makeCredential());

  await expectBrokerError(() => broker.refreshGrant(), "cas_conflict", {
    generation: "2",
  });
  assert.equal(store.generation, "3");
  const canonical = await broker.getGrant({ minTtlSeconds: 0 });
  assert.equal(canonical.accountId, "account-2");
  assert.equal(canonical.generation, "3");
});

test("uncertain CAS acknowledgement replays the same commit without another refresh", async () => {
  const store = new FakeStore();
  let refreshCalls = 0;
  const failure = Object.assign(new Error("rename acknowledgement lost"), {
    code: "commit_outcome_uncertain",
    commitState: "uncertain",
    retryable: false,
  });
  const broker = makeBroker({
    refreshAdapter: async () => {
      refreshCalls += 1;
      store.failAfterCommit = failure;
      return refreshedCredential();
    },
    store,
  });
  await broker.installCredential(makeCredential());

  const grant = await broker.refreshGrant();

  assert.equal(grant.generation, "3");
  assert.equal(grant.accessToken, ACCESS_TOKEN_2);
  assert.equal(refreshCalls, 1);
  assert.equal(store.casCalls.length, 4);
  assert.equal(store.casCalls.at(-2).commitId, store.casCalls.at(-1).commitId);
});

test("post-dispatch commit failure leaves a durable refresh reservation", async () => {
  const store = new FakeStore();
  const broker = makeBroker({
    refreshAdapter: async () => {
      store.compareAndSwap = async () => {
        throw Object.assign(new Error("persistent store outage"), {
          code: "key_unavailable",
          retryable: true,
        });
      };
      return refreshedCredential();
    },
    store,
  });
  await broker.installCredential(makeCredential());

  await expectBrokerError(() => broker.refreshGrant(), "refresh_outcome_uncertain");
  assertReservationSnapshot(await broker.snapshot(), { generation: "2" });
  await expectBrokerError(() => makeBroker({ store }).getGrant(), "recovery_required", {
    generation: "2",
    reason: "refresh_in_progress",
    status: "recovery-required",
  });
});

test("post-dispatch commit ID failure is non-retryable and preserves reservation", async () => {
  const store = new FakeStore();
  let uuidCalls = 0;
  const broker = makeBroker({
    randomUUID: () => {
      uuidCalls += 1;
      if (uuidCalls >= 4) throw new Error("UUID source failed");
      return `controlled-uuid-${uuidCalls}`;
    },
    store,
  });
  await broker.installCredential(makeCredential());

  await expectBrokerError(() => broker.refreshGrant(), "refresh_outcome_uncertain");
  assertReservationSnapshot(await broker.snapshot(), { generation: "2" });
});

test("unreconciled uncertain CAS never returns a refreshed credential", async () => {
  const store = new FakeStore();
  const failure = Object.assign(new Error("rename acknowledgement lost"), {
    code: "commit_outcome_uncertain",
    commitState: "uncertain",
    retryable: false,
  });
  const broker = makeBroker({
    refreshAdapter: async () => {
      store.afterCommit = async () => {
        const competing = makeCredential({ marker: "access-competing" });
        store.payload = storedReadyPayload(competing);
        store.baseGeneration = store.generation;
        store.generation = (BigInt(store.generation) + 1n).toString();
        store.commitId = "competing-commit";
        store.afterCommit = null;
      };
      store.failAfterCommit = failure;
      return refreshedCredential();
    },
    store,
  });
  await broker.installCredential(makeCredential());

  await expectBrokerError(() => broker.refreshGrant(), "refresh_outcome_uncertain");
  assert.equal(store.generation, "4");
});

test("post-CAS reread rejects a later commit instead of returning another account", async () => {
  const store = new FakeStore();
  const broker = makeBroker({
    refreshAdapter: async () => {
      store.afterCommit = async () => {
        const competing = makeCredential({
          accountId: "account-2",
          marker: "access-competing",
          refreshToken: "refresh-competing",
          userId: "user-2",
        });
        store.payload = storedReadyPayload(competing);
        store.baseGeneration = store.generation;
        store.generation = (BigInt(store.generation) + 1n).toString();
        store.commitId = "competing-commit";
        store.afterCommit = null;
      };
      return refreshedCredential();
    },
    store,
  });
  await broker.installCredential(makeCredential());

  await expectBrokerError(() => broker.refreshGrant(), "cas_conflict", {
    generation: "4",
  });
  const canonical = await broker.getGrant({ minTtlSeconds: 0 });
  assert.equal(canonical.accountId, "account-2");
  assert.equal(canonical.generation, "4");
});

test("post-CAS reread accepts a storage-only rotation of the committed payload", async () => {
  const store = new FakeStore();
  const broker = makeBroker({
    refreshAdapter: async () => {
      store.afterCommit = async () => {
        store.baseGeneration = store.generation;
        store.generation = (BigInt(store.generation) + 1n).toString();
        store.commitId = "rotation-after-refresh";
        store.afterCommit = null;
      };
      return refreshedCredential();
    },
    store,
  });
  await broker.installCredential(makeCredential());

  const grant = await broker.refreshGrant();

  assert.equal(grant.accessToken, ACCESS_TOKEN_2);
  assert.equal(grant.generation, "4");
});

test("worker login and unauthorized refresh use exact app-server payloads", async () => {
  const store = new FakeStore();
  let refreshCalls = 0;
  let adapterInput;
  const broker = makeBroker({
    refreshAdapter: async (input) => {
      refreshCalls += 1;
      adapterInput = input;
      return refreshedCredential();
    },
    store,
  });
  await broker.installCredential(makeCredential());

  assert.deepEqual(await broker.workerLoginParams(), {
    accessToken: ACCESS_TOKEN_1,
    chatgptAccountId: ACCOUNT_ID,
    chatgptPlanType: "enterprise",
    type: "chatgptAuthTokens",
  });
  store.toJSON = () => ({});
  assert.equal(Object.hasOwn(broker, "workerAccessToken"), false);
  assert.equal(JSON.stringify(broker).includes(ACCESS_TOKEN_1), false);
  assert.equal(refreshCalls, 0);

  for (const params of [
    null,
    { previousAccountId: ACCOUNT_ID, reason: "expired" },
    { previousAccountId: "account-2", reason: "unauthorized" },
    { extra: true, previousAccountId: ACCOUNT_ID, reason: "unauthorized" },
  ]) {
    await expectBrokerError(() => broker.handleWorkerRefresh(params), "invalid_request");
  }
  assert.equal(refreshCalls, 0);

  assert.deepEqual(
    await broker.handleWorkerRefresh({
      previousAccountId: ACCOUNT_ID,
      reason: "unauthorized",
    }),
    {
      accessToken: ACCESS_TOKEN_2,
      chatgptAccountId: ACCOUNT_ID,
      chatgptPlanType: "enterprise",
    },
  );
  assert.equal(refreshCalls, 1);
  assert.deepEqual(Object.keys(adapterInput).sort(), [
    "attemptId",
    "credential",
    "expectedGeneration",
  ]);
  assert.deepEqual(Object.keys(adapterInput.credential).sort(), [
    "accessToken",
    "accountId",
    "authJson",
    "expiresAt",
    "planType",
    "userId",
  ]);
  assert.equal(adapterInput.expectedGeneration, "2");
});

test("worker refresh callbacks stay bound to the account actually issued", async () => {
  const store = new FakeStore({ coordinationKey: "worker-account-binding" });
  let refreshCalls = 0;
  const workerBroker = makeBroker({
    refreshAdapter: async () => {
      refreshCalls += 1;
      return refreshedCredential();
    },
    store,
  });
  await workerBroker.installCredential(makeCredential());
  await workerBroker.workerLoginParams();
  const replacement = makeCredential({
    accountId: "account-2",
    marker: "access-account-2",
    refreshToken: "refresh-account-2",
    userId: "user-2",
  });
  await makeBroker({ store }).installCredential(replacement);

  await expectBrokerError(
    () =>
      workerBroker.handleWorkerRefresh({
        previousAccountId: "account-2",
        reason: "unauthorized",
      }),
    "invalid_request",
  );
  assert.equal(refreshCalls, 0);
  assert.equal((await makeBroker({ store }).getGrant()).accountId, "account-2");
});

test("worker refresh callbacks stay bound to the user actually issued", async () => {
  const store = new FakeStore({ coordinationKey: "worker-user-binding" });
  let refreshCalls = 0;
  const workerBroker = makeBroker({
    refreshAdapter: async () => {
      refreshCalls += 1;
      return refreshedCredential();
    },
    store,
  });
  await workerBroker.installCredential(makeCredential());
  await workerBroker.workerLoginParams();
  const replacement = makeCredential({
    marker: "access-user-2",
    refreshToken: "refresh-user-2",
    userId: "user-2",
  });
  await makeBroker({ store }).installCredential(replacement);

  await expectBrokerError(
    () =>
      workerBroker.handleWorkerRefresh({
        previousAccountId: ACCOUNT_ID,
        reason: "unauthorized",
      }),
    "invalid_request",
  );
  assert.equal(refreshCalls, 0);
  assert.equal(
    JSON.parse((await store.read()).payload).userId,
    "user-2",
  );
});

test("a stale worker callback receives the newer generation without rotating again", async () => {
  const store = new FakeStore({ coordinationKey: "stale-worker" });
  let staleWorkerRefreshCalls = 0;
  let currentRefreshCalls = 0;
  const staleWorkerBroker = makeBroker({
    refreshAdapter: async () => {
      staleWorkerRefreshCalls += 1;
      return refreshedCredential({ marker: "unexpected-refresh" });
    },
    store,
  });
  const currentBroker = makeBroker({
    refreshAdapter: async () => {
      currentRefreshCalls += 1;
      return refreshedCredential();
    },
    store,
  });
  await staleWorkerBroker.installCredential(makeCredential());
  await staleWorkerBroker.workerLoginParams();
  assert.equal((await currentBroker.refreshGrant()).generation, "3");

  const response = await staleWorkerBroker.handleWorkerRefresh({
    previousAccountId: ACCOUNT_ID,
    reason: "unauthorized",
  });

  assert.deepEqual(response, {
    accessToken: ACCESS_TOKEN_2,
    chatgptAccountId: ACCOUNT_ID,
    chatgptPlanType: "enterprise",
  });
  assert.equal(currentRefreshCalls, 1);
  assert.equal(staleWorkerRefreshCalls, 0);
});

test("a storage-only key rotation does not suppress a worker refresh", async (t) => {
  const { createStore } = await createEncryptedStoreFixture(t);
  const store = createStore();
  let refreshCalls = 0;
  const broker = makeBroker({
    refreshAdapter: async () => {
      refreshCalls += 1;
      return refreshedCredential();
    },
    store,
  });
  await broker.installCredential(makeCredential());
  await broker.workerLoginParams();
  await store.rotateEncryption({
    expectedGeneration: "1",
    commitId: "rotation-before-worker-callback",
  });

  const response = await broker.handleWorkerRefresh({
    previousAccountId: ACCOUNT_ID,
    reason: "unauthorized",
  });

  assert.deepEqual(response, {
    accessToken: ACCESS_TOKEN_2,
    chatgptAccountId: ACCOUNT_ID,
    chatgptPlanType: "enterprise",
  });
  assert.equal(refreshCalls, 1);
  assert.equal((await store.read()).generation, "4");
});

test("same-token credential reinstall does not suppress a worker refresh", async () => {
  const store = new FakeStore({ coordinationKey: "same-token-reinstall" });
  let adapterInput;
  const broker = makeBroker({
    refreshAdapter: async (input) => {
      adapterInput = input;
      return refreshedCredential();
    },
    store,
  });
  await broker.installCredential(makeCredential());
  await broker.workerLoginParams();
  await makeBroker({ store }).installCredential(
    makeCredential({ refreshToken: "refresh-reinstalled" }),
  );

  const response = await broker.handleWorkerRefresh({
    previousAccountId: ACCOUNT_ID,
    reason: "unauthorized",
  });

  assert.equal(response.accessToken, ACCESS_TOKEN_2);
  assert.equal(
    JSON.parse(adapterInput.credential.authJson).tokens.refresh_token,
    "refresh-reinstalled",
  );
  assert.equal(store.generation, "4");
});

test("worker generation is rechecked after entering shared refresh", async () => {
  const store = new FakeStore({ coordinationKey: "worker-generation-race" });
  let staleWorkerRefreshCalls = 0;
  let currentRefreshCalls = 0;
  const staleWorkerBroker = makeBroker({
    refreshAdapter: async () => {
      staleWorkerRefreshCalls += 1;
      return refreshedCredential({ marker: "unexpected-refresh" });
    },
    store,
  });
  const currentBroker = makeBroker({
    refreshAdapter: async () => {
      currentRefreshCalls += 1;
      return refreshedCredential();
    },
    store,
  });
  await staleWorkerBroker.installCredential(makeCredential());
  await staleWorkerBroker.workerLoginParams();
  store.afterRead = async () => {
    store.afterRead = null;
    await currentBroker.refreshGrant();
  };

  const response = await staleWorkerBroker.handleWorkerRefresh({
    previousAccountId: ACCOUNT_ID,
    reason: "unauthorized",
  });

  assert.deepEqual(response, {
    accessToken: ACCESS_TOKEN_2,
    chatgptAccountId: ACCOUNT_ID,
    chatgptPlanType: "enterprise",
  });
  assert.equal(currentRefreshCalls, 1);
  assert.equal(staleWorkerRefreshCalls, 0);
  assert.equal(store.generation, "3");
});

test("current worker retries after joining a stale worker no-op", async () => {
  const store = new FakeStore({ coordinationKey: "stale-current-worker-collision" });
  let workerRefreshCalls = 0;
  const workerAdapter = async () => {
    workerRefreshCalls += 1;
    return refreshedCredential({ marker: "access-3", refreshToken: "refresh-3" });
  };
  const staleWorker = makeBroker({ refreshAdapter: workerAdapter, store });
  const currentBroker = makeBroker({
    refreshAdapter: async () => refreshedCredential(),
    store,
  });
  const currentWorker = makeBroker({ refreshAdapter: workerAdapter, store });
  await staleWorker.installCredential(makeCredential());
  await staleWorker.workerLoginParams();
  assert.equal((await currentBroker.refreshGrant()).generation, "3");
  await currentWorker.workerLoginParams();

  let readCount = 0;
  let staleRefreshReadEntered = false;
  let releaseStaleRefreshRead;
  const staleRefreshReadGate = new Promise((resolve) => {
    releaseStaleRefreshRead = resolve;
  });
  store.afterRead = async () => {
    readCount += 1;
    if (readCount === 2) {
      staleRefreshReadEntered = true;
      await staleRefreshReadGate;
    }
  };

  const staleResponse = staleWorker.handleWorkerRefresh({
    previousAccountId: ACCOUNT_ID,
    reason: "unauthorized",
  });
  await waitFor(() => staleRefreshReadEntered);
  const currentResponse = currentWorker.handleWorkerRefresh({
    previousAccountId: ACCOUNT_ID,
    reason: "unauthorized",
  });
  releaseStaleRefreshRead();

  assert.deepEqual(await staleResponse, {
    accessToken: ACCESS_TOKEN_2,
    chatgptAccountId: ACCOUNT_ID,
    chatgptPlanType: "enterprise",
  });
  assert.deepEqual(await currentResponse, {
    accessToken: ACCESS_TOKEN_3,
    chatgptAccountId: ACCOUNT_ID,
    chatgptPlanType: "enterprise",
  });
  assert.equal(workerRefreshCalls, 1);
  assert.equal(store.generation, "5");
});

test("late callers reread after an inactive stale refresh reservation", async (t) => {
  const reservation = storedReservationPayload();

  await t.test("grant caller", async () => {
    const store = new FakeStore();
    const broker = makeBroker({
      refreshAdapter: async () => assert.fail("stale reservation must not refresh"),
      store,
    });
    await broker.installCredential(makeCredential());
    store.baseGeneration = "2";
    store.commitId = "ready-after-stale-reservation";
    store.generation = "3";
    let staleRead = true;
    const read = store.read.bind(store);
    store.read = async () => {
      if (staleRead) {
        staleRead = false;
        return fakeRecord({
          commitId: "stale-reservation",
          generation: "2",
          payload: reservation,
        });
      }
      return read();
    };

    const grant = await broker.getGrant();

    assert.equal(grant.accessToken, ACCESS_TOKEN_1);
    assert.equal(grant.generation, "3");
  });

  await t.test("forced refresh caller", async () => {
    const store = new FakeStore();
    let refreshCalls = 0;
    const broker = makeBroker({
      refreshAdapter: async () => {
        refreshCalls += 1;
        return refreshedCredential({ marker: "unexpected-refresh" });
      },
      store,
    });
    await broker.installCredential(makeCredential());
    store.baseGeneration = "2";
    store.commitId = "ready-after-stale-reservation";
    store.generation = "3";
    store.payload = storedReadyPayload(refreshedCredential());
    let staleRead = true;
    const read = store.read.bind(store);
    store.read = async () => {
      if (staleRead) {
        staleRead = false;
        return fakeRecord({
          commitId: "stale-reservation",
          generation: "2",
          payload: reservation,
        });
      }
      return read();
    };

    const grant = await broker.refreshGrant();

    assert.equal(grant.accessToken, ACCESS_TOKEN_2);
    assert.equal(grant.generation, "3");
    assert.equal(refreshCalls, 0);
  });

  await t.test("worker callback", async () => {
    const store = new FakeStore();
    let refreshCalls = 0;
    const broker = makeBroker({
      refreshAdapter: async () => {
        refreshCalls += 1;
        return refreshedCredential({ marker: "unexpected-refresh" });
      },
      store,
    });
    await broker.installCredential(makeCredential());
    await broker.workerLoginParams();
    store.baseGeneration = "2";
    store.commitId = "ready-after-stale-reservation";
    store.generation = "3";
    store.payload = storedReadyPayload(refreshedCredential());
    let staleRead = true;
    const read = store.read.bind(store);
    store.read = async () => {
      if (staleRead) {
        staleRead = false;
        return fakeRecord({
          commitId: "stale-reservation",
          generation: "2",
          payload: reservation,
        });
      }
      return read();
    };

    const response = await broker.handleWorkerRefresh({
      previousAccountId: ACCOUNT_ID,
      reason: "unauthorized",
    });

    assert.equal(response.accessToken, ACCESS_TOKEN_2);
    assert.equal(refreshCalls, 0);
  });
});

test("hostile external error accessors cannot bypass broker error redaction", async (t) => {
  const secret = "hostile-error-secret";
  const hostile = {};
  for (const key of ["code", "commitState", "postDispatch", "reason"]) {
    Object.defineProperty(hostile, key, {
      get() {
        throw new Error(`${secret}-${key}`);
      },
    });
  }

  await t.test("store read", async () => {
    const store = new FakeStore();
    store.read = async () => {
      throw hostile;
    };
    const error = await expectBrokerError(
      () => makeBroker({ store }).snapshot(),
      "store_unavailable",
    );
    assert.equal(String(error).includes(secret), false);
  });

  await t.test("store commit after refresh", async () => {
    const store = new FakeStore();
    const broker = makeBroker({
      refreshAdapter: async () => {
        store.compareAndSwap = async () => {
          throw hostile;
        };
        return refreshedCredential();
      },
      store,
    });
    await broker.installCredential(makeCredential());
    const error = await expectBrokerError(
      () => broker.refreshGrant(),
      "refresh_outcome_uncertain",
    );
    assert.equal(String(error).includes(secret), false);
  });

  await t.test("refresh adapter", async () => {
    const store = new FakeStore();
    const broker = makeBroker({
      refreshAdapter: async () => {
        throw hostile;
      },
      store,
    });
    await broker.installCredential(makeCredential());
    const error = await expectBrokerError(() => broker.refreshGrant(), "recovery_required", {
      generation: "3",
      reason: "adapter_post_dispatch_uncertain",
      status: "recovery-required",
    });
    assert.equal(String(error).includes(secret), false);
  });
});

test("trusted pre-dispatch adapter failure restores ready state", async () => {
  const store = new FakeStore();
  const broker = makeBroker({
    refreshAdapter: async () => {
      throw Object.assign(new Error("safe local setup failure"), {
        preDispatch: true,
      });
    },
    store,
  });
  await broker.installCredential(makeCredential());

  await expectBrokerError(() => broker.refreshGrant(), "refresh_failed");
  assert.deepEqual(await broker.snapshot(), {
    expiresAt: isoAfter(3600),
    generation: "3",
    schemaVersion: 1,
    status: "ready",
  });
  const grant = await broker.getGrant({ minTtlSeconds: 0 });
  assert.equal(grant.accessToken, ACCESS_TOKEN_1);
});

test("contradictory dispatch markers fail closed", async () => {
  const store = new FakeStore();
  const broker = makeBroker({
    refreshAdapter: async () => {
      throw Object.assign(new Error("contradictory adapter result"), {
        postDispatch: true,
        preDispatch: true,
      });
    },
    store,
  });
  await broker.installCredential(makeCredential());

  await expectBrokerError(() => broker.refreshGrant(), "recovery_required", {
    generation: "3",
    reason: "adapter_post_dispatch_uncertain",
    status: "recovery-required",
  });
});

test("pre-dispatch restore failure preserves the durable reservation", async () => {
  const store = new FakeStore();
  const broker = makeBroker({
    refreshAdapter: async () => {
      store.compareAndSwap = async () => {
        throw Object.assign(new Error("restore store unavailable"), {
          code: "key_unavailable",
          retryable: true,
        });
      };
      throw Object.assign(new Error("safe local setup failure"), {
        preDispatch: true,
      });
    },
    store,
  });
  await broker.installCredential(makeCredential());

  await expectBrokerError(() => broker.refreshGrant(), "refresh_outcome_uncertain");
  assertReservationSnapshot(await broker.snapshot(), { generation: "2" });
});

test("public errors and metadata redact adapter and credential secrets", async () => {
  const secret = "never-print-this-refresh-token";
  const store = new FakeStore();
  const broker = makeBroker({
    refreshAdapter: async () => {
      throw new Error(`upstream included ${secret}`);
    },
    store,
  });
  await broker.installCredential(makeCredential({ refreshToken: secret }));

  const error = await expectBrokerError(() => broker.refreshGrant(), "recovery_required", {
    generation: "3",
    reason: "adapter_post_dispatch_uncertain",
    status: "recovery-required",
  });
  const publicForms = [
    String(error),
    JSON.stringify(error),
    JSON.stringify(authBrokerErrorMetadata(error)),
    JSON.stringify(await broker.snapshot()),
  ];
  for (const publicForm of publicForms) assert.equal(publicForm.includes(secret), false);
  assert.equal(Object.hasOwn(error, "cause"), false);
  assert.deepEqual(authBrokerErrorMetadata(new Error(secret)), {
    code: "refresh_failed",
    retryable: false,
  });
});
